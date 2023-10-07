use std::{
    net::SocketAddr,
    sync::{Arc, Mutex}, collections::HashSet,
};
use automerge::AutoCommit;

use rand::{rngs::StdRng, SeedableRng};
use foca::{Foca, Notification, PostcardCodec, Timer};
use tokio::{net::UdpSocket, sync::mpsc::{self, Sender}};
use log::{info, error, trace};
use bytes::{BufMut, Bytes, BytesMut};

use super::{core::{AccumulatingRuntime, FocaRuntimeConfig}, broadcast::{Tag, GossipMessage, craft_broadcast}};
use super::types::ID;
use super::members::Members;
use super::broadcast::Handler;

use crate::swim::core::MyDataHandler;

enum Input<T> {
    Event(Timer<T>),
    Data(Bytes),
    Announce(T),
}
#[derive(Debug)]
pub enum FocaCommand {
    SendBroadcast((Tag, GossipMessage)),
    HandleTimer(Timer<ID>),
    HandleData(Bytes),
    Announce(ID),
}

pub async fn setup_foca(runtime_config: FocaRuntimeConfig, state:Arc<Mutex<AutoCommit>>) -> Result<Sender<FocaCommand>, anyhow::Error> {
    let rng = StdRng::from_entropy();
    let data_handler = Box::new(MyDataHandler::new(&runtime_config.data_dir, state));
    let broadcast_handler = Handler::new(HashSet::new(), data_handler);
    let identity = runtime_config.identity;
    let announce_to = runtime_config.announce_to;

    let mut foca = Foca::with_custom_broadcast(identity.clone(),
    runtime_config.foca_config.clone(),
    rng, PostcardCodec,
    broadcast_handler);

    let socket = Arc::new(UdpSocket::bind(runtime_config.bind_addr).await?);

        // We'll create a task responsible to sending data through the
    // socket.
    // These are what we use to communicate with it
    let (tx_send_data, mut rx_send_data) = mpsc::channel::<(SocketAddr, Bytes)>(100);
    // The socket writing task
    let write_socket = Arc::clone(&socket);
    tokio::spawn(async move {
        while let Some((dst, data)) = rx_send_data.recv().await {
            // A more reasonable implementation would do some more stuff
            // here before sending, like:
            //  * zlib or something else to compress the data
            //  * encryption (shared key, AES most likely)
            //  * an envelope with tag+version+checksum to allow
            //    protocol evolution
            let _ignored_send_result = write_socket.send_to(&data, &dst).await;
        }
    });

    // And communicating via channels
    let (tx_foca, mut rx_foca) = mpsc::channel(100);
    // Another alternative would be putting a Lock around Foca, but
    // yours truly likes to hide behind (the lock inside) channels
    // instead.
    let mut runtime:AccumulatingRuntime<ID> = AccumulatingRuntime::new();
    let mut members = Members::new();
    members.add_member(identity);
    let tx_foca_copy = tx_foca.clone();

    let (foca_command_sender, mut foca_command_receiver) = mpsc::channel::<FocaCommand>(100);

    tokio::spawn(async move {

        while let Some(foca_event) = foca_command_receiver.recv().await {
            match foca_event {
                FocaCommand::SendBroadcast((tag, message)) => {    
                    let broadcast = craft_broadcast(tag, message);
                    let _ignore_result = foca.add_broadcast(broadcast.as_ref());
                },
                FocaCommand::HandleTimer(timer) => {
                    let _ignore_result = foca.handle_timer(timer, &mut runtime);
                },
                FocaCommand::HandleData(data) => {
                    let _ignore_result = foca.handle_data(&data, &mut runtime);
                },
                FocaCommand::Announce(destination) => {
                    let _ignore_result = foca.announce(destination, &mut runtime);
                },
            }

            // First we submit everything that needs to go to the network
            while let Some((dst, data)) = runtime.to_send.pop() {
                // ToSocketAddrs would be the fancy thing to use here
                let _ignored_send_result = tx_send_data.send((dst.addr, data)).await;
            }
            // Then schedule what needs to be scheduled
            while let Some((delay, event)) = runtime.to_schedule.pop() {
                let own_input_handle = tx_foca_copy.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    let _ignored_send_error = own_input_handle.send(Input::Event(event)).await;
                });
            }
            // And finally react to notifications.
            //
            // Here we could do smarter things to keep other actors in
            // the system up-to-date with the cluster state.
            // We could, for example:
            //
            //  * Have a broadcast channel where we submit the MemberUp
            //    and MemberDown notifications to everyone and each one
            //    keeps a lock-free version of the list
            //
            //  * Update a shared/locked Vec that every consumer has
            //    read access
            //
            // But since this is an agent, we simply write to a file
            // so other proccesses periodically open()/read()/close()
            // to figure out the cluster members.
            let mut active_list_has_changed = false;
            while let Some(notification) = runtime.notifications.pop() {
                match notification {
                    Notification::MemberUp(id) => {
                        info!("member with id {:?} up", id);
                        active_list_has_changed |= members.add_member(id);
                    },
                    Notification::MemberDown(id) => {
                        info!("member with id {:?} down", id);
                        active_list_has_changed |= members.remove_member(id);
                    },
                    Notification::Idle => {
                        info!("cluster empty");
                    },
                    other => {
                        info!("unhandled notification {:?}", other);
                    }
                }
            }

            if active_list_has_changed {
                info!("New members list: {:?}", members);
            }
        }
    });

    let foca_command_sender_clone = foca_command_sender.clone();
    tokio::spawn(async move {
        while let Some(input) = rx_foca.recv().await {

            let result = match input {
                Input::Event(timer) => foca_command_sender_clone.send(FocaCommand::HandleTimer(timer)).await,
                Input::Data(data) => foca_command_sender_clone.send(FocaCommand::HandleData(data)).await,
                Input::Announce(destination) => foca_command_sender_clone.send(FocaCommand::Announce(destination)).await,
            };

            // Every public foca result yields `()` on success, so there's
            // nothing to do with Ok
            if let Err(error) = result {
                // And we'd decide what to do with each error, but Foca
                // is pretty tolerant so we just log them and pretend
                // all is fine
                error!("Ignored Error: {}", error);
            }
        }
    });

    // Foca is running, we can tell it to announce to our target
    if let Some(dst) = announce_to {
        let _ignored_send_error = tx_foca.send(Input::Announce(dst)).await;
    }

    tokio::spawn(async move {
        let buf_len = runtime_config.foca_config.max_packet_size.get();
        let mut recv_buf = vec![0u8; buf_len];
        // And finally, we receive forever
        let mut databuf = BytesMut::new();
        loop {
            match socket.recv_from(&mut recv_buf).await {
                Ok((len, _from_addr)) => {
                // Accordinly, we would undo everything that's done prior to
                // sending: decompress, decrypt, remove the envelope
                databuf.put_slice(&recv_buf[..len]);
                let data_to_send = databuf.split().freeze();
                trace!("Data to send: {:?}", data_to_send);
                // And simply forward it to foca
                let _ignored_send_error = tx_foca.send(Input::Data(data_to_send)).await;
                },
                Err(e) => error!("got an error receiving: {}", e),
            }
        }
    });

    Ok(foca_command_sender)
}
