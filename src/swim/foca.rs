use std::{
    net::SocketAddr, str::FromStr,
    sync::Arc, collections::HashSet, num::NonZeroU8, path::PathBuf, error::Error,
};
use clap::{arg, Command, value_parser, builder::{NonEmptyStringValueParser, BoolValueParser, OsStr}};
use rand::{rngs::StdRng, SeedableRng};
use foca::{Config, Foca, Notification, PostcardCodec, Timer};
use tokio::{net::UdpSocket, sync::mpsc::{self, Sender, Receiver}};
use log::{info, error, trace};
use bytes::{BufMut, Bytes, BytesMut};

use super::core::{AccumulatingRuntime, FocaRuntimeConfig};
use super::types::ID;
use super::members::Members;
use super::broadcast::{Handler, Broadcast};

use crate::swim::core::MyDataHandler;


pub struct FocaRuntime {
    foca: Foca<ID, PostcardCodec, StdRng, Handler>,
    socket: Arc<UdpSocket>,
    runtime_config: FocaRuntimeConfig,
    tx_foca: Sender<Input<ID>>,
    rx_foca: Receiver<Input<ID>>,
}

// We'll also launch a task to manage Foca. Since there are timers
// involved, one simple way to do it is unifying the input:
enum Input<T> {
    Event(Timer<T>),
    Data(Bytes),
    Announce(T),
}

impl FocaRuntime {

    pub async fn new<'a>(runtime_config: FocaRuntimeConfig) -> Result<FocaRuntime, anyhow::Error> {
        let rng = StdRng::from_entropy();
        let data_handler = Box::new(MyDataHandler::new(&runtime_config.data_dir));
        let broadcast_handler = Handler::new(HashSet::new(), data_handler);

        let foca = Foca::with_custom_broadcast(runtime_config.identity.clone(),
        runtime_config.foca_config.clone(),
        rng, PostcardCodec,
        broadcast_handler);

        let socket = Arc::new(UdpSocket::bind(runtime_config.bind_addr).await?);
        let (tx_foca, rx_foca) = mpsc::channel(100);
        Ok(FocaRuntime {
            foca: foca,
            socket: socket,
            runtime_config: runtime_config,
            tx_foca: tx_foca,
            rx_foca: rx_foca,
        })
    }

    pub fn broadcast(&mut self, broadcast_msg: Broadcast) {
        match self.foca.add_broadcast(broadcast_msg.as_ref()) {
            Ok(_) => info!("Added broadcast"),
            Err(e) => error!("Could not add broadcast: {}", e),
        }
    }

    pub async fn init(&mut self) {
        // We'll create a task responsible to sending data through the
        // socket.
        // These are what we use to communicate with it
        let (tx_send_data, mut rx_send_data) = mpsc::channel::<(SocketAddr, Bytes)>(100);
        // The socket writing task
        let write_socket = Arc::clone(&self.socket);
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
        // let (tx_foca, mut rx_foca) = mpsc::channel(100);
        // Another alternative would be putting a Lock around Foca, but
        // yours truly likes to hide behind (the lock inside) channels
        // instead.
        let mut runtime = AccumulatingRuntime::new();
        let mut members = Members::new();
        members.add_member(self.runtime_config.identity.clone());
        let tx_foca_copy = self.tx_foca.clone();
        while let Some(input) = self.rx_foca.recv().await {
            debug_assert_eq!(0, runtime.backlog());

            let result = match input {
                Input::Event(timer) => self.foca.handle_timer(timer, &mut runtime),
                Input::Data(data) => self.foca.handle_data(&data, &mut runtime),
                Input::Announce(dst) => self.foca.announce(dst, &mut runtime),
            };

            // Every public foca result yields `()` on success, so there's
            // nothing to do with Ok
            if let Err(error) = result {
                // And we'd decide what to do with each error, but Foca
                // is pretty tolerant so we just log them and pretend
                // all is fine
                error!("Ignored Error: {}", error);
            }

            // Now we react to what happened.
            // This is how we enable async: buffer one single interaction
            // and then drain the runtime.

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
    }

    pub async fn listen(&self) {
        let buf_len = self.runtime_config.foca_config.max_packet_size.get();
        let mut recv_buf = vec![0u8; buf_len];
        // Foca is running, we can tell it to announce to our target
        if let Some(dst) = self.runtime_config.announce_to.clone() {
            let _ignored_send_error = self.tx_foca.send(Input::Announce(dst)).await;
        }

        // And finally, we receive forever
        let mut databuf = BytesMut::new();
        loop {
            match self.socket.recv_from(&mut recv_buf).await {
                Ok((len, _from_addr)) => {
                // Accordinly, we would undo everything that's done prior to
                // sending: decompress, decrypt, remove the envelope
                databuf.put_slice(&recv_buf[..len]);
                let data_to_send = databuf.split().freeze();
                trace!("Data to send: {:?}", data_to_send);
                // And simply forward it to foca
                let _ignored_send_error = self.tx_foca.send(Input::Data(data_to_send)).await;
                },
                Err(e) => error!("got an error receiving: {}", e),
            }
        }
    }
}
