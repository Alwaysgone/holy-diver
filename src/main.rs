mod swim;

use std::{
    net::SocketAddr, str::FromStr,
    sync::Arc, collections::HashSet, num::NonZeroU8, path::PathBuf,
};
use clap::{arg, Command, value_parser, builder::{NonEmptyStringValueParser, BoolValueParser, OsStr}};
use rand::{rngs::StdRng, SeedableRng};
use foca::{Config, Foca, Notification, PostcardCodec, Timer};
use tokio::{net::UdpSocket, sync::mpsc};
use log::{info, error, trace};
use bytes::{BufMut, Bytes, BytesMut};
use dotenv::dotenv;

use swim::core::{AccumulatingRuntime};
use swim::types::ID;
use swim::members::Members;
use swim::broadcast::{Handler, MessageType, MessageType::FullSync, GossipMessage, Tag::SyncOperation};

use automerge::transaction::Transactable;
use automerge::AutomergeError;
use automerge::ObjType;
use automerge::{Automerge, ROOT};
use uuid::Uuid;

fn cli() -> Command {
    Command::new("holy-diver")
        .about("You expected SWIM but it was me DIO!")
        .arg_required_else_help(false)
        // .arg(Arg::new("bind-address")
    // .help("Socket address to bind to. Example: 127.0.0.1:8080""))
        .args(&[
            // Arg::new("bind-address")
            // .long("bind-address")
            // .help("Socket address to bind to. Example: 127.0.0.1:8080")
            // .value_parser(NonEmptyStringValueParser::new())
            // ,
        arg!(--"bind-address" <BIND_ADDRESS> "Socket address to bind to. Example: 127.0.0.1:8080")
        .value_parser(NonEmptyStringValueParser::new())
        .id("bind-address"),
        arg!(identity: -i --identity <IDENTITY> "The address cluster members will use to talk to you. Defaults to bind-address")
        .value_parser(NonEmptyStringValueParser::new()),
        arg!(-a --"announce-to" <ANNOUNCE_TO> "Address to another holy-diver instance to join with")
        .value_parser(NonEmptyStringValueParser::new())
        .id("announce-to"),
        arg!(-d --"data-dir" <DATA_DIR> "Name of the file that will contain all active members")
        .value_parser(value_parser!(PathBuf))
        .default_value(OsStr::from("./data"))
        .id("data-dir"),
        arg!(-b --broadcast <BROADCAST> "Flag that indicates whether a broadcast should be sent on startup or not")
        .value_parser(BoolValueParser::new())
        .id("broadcast"),
        ])
        
}

fn handle_message(msg_type:MessageType, msg_payload:Vec<u8>) {
    info!("Received message of type {:?}: {:?}", msg_type, msg_payload);
    match msg_type {
        FullSync => {
            match Automerge::load(&msg_payload) {
                Ok(doc) => info!("Received document: {:?}", doc),
                Err(e) => error!("Could not parse FullSync message: {}", e),
            }
        },
        other => {
            info!("Handling of message type {:?} currently not implemented", other);
        }
    }
}

fn get_broadcast_data() -> Vec<u8> {
    let mut data = Automerge::new();
    let _heads = data.get_heads();
    data.transact::<_,_,AutomergeError>(|tx| {
        let memos = tx.put_object(ROOT, "memos", ObjType::Map).unwrap();
        let memo1 = tx.put(&memos, "Memo1", "Do the thing").unwrap();
        let memo2 = tx.put(&memos, "Memo2", "Add automerge support").unwrap();
        Ok((memo1, memo2))
    })
    .unwrap()
    .result;
    data.save()
    // let v = vec!(1, 2);
    // v
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), anyhow::Error> {
    dotenv().ok();
    env_logger::init();
    let matches = cli().get_matches();

    info!("Starting with matches: {:?}", matches);

    let rng = StdRng::from_entropy();
    let config = {
        let mut c = Config::simple();
        // With this setting you can suspend (^Z) one process,
        // wait for it the member to be declared down then resume
        // it (fg) and foca should recover by itself
        c.notify_down_members = true;
        // limits the 
        c.max_transmissions = NonZeroU8::new(2).unwrap();
        c
    };

    let buf_len = config.max_packet_size.get();
    let mut recv_buf = vec![0u8; buf_len];

    let bind_addr = matches.get_one::<String>("bind-address")
    .map(|ba| SocketAddr::from_str(ba.as_str()).expect(&format!("could not parse binding address as SocketAddr '{}'", ba)))
    .unwrap_or(SocketAddr::from_str("127.0.0.1:9000").unwrap());
    info!("Binding to {}", bind_addr);

    let identity = matches.get_one::<String>("identity")
    .map(|id| SocketAddr::from_str(id.as_str()).expect(&format!("could not parse identity as SocketAddr '{}'", id)))
    .map(|id| ID::new(id))
    .unwrap_or(ID::new(bind_addr));
    info!("Using identity {}", bind_addr); 

    let announce_to = matches.get_one::<String>("announce-to")
    .map(|a| SocketAddr::from_str(a.as_str()).expect(&format!("could not parse announce-to as SocketAddr '{}'", a)))
    .map(|a| ID::new(a));
    if announce_to.is_some() {
        info!("Announcing to {:?}", announce_to.clone().unwrap());
    } else {
        info!("Starting up as single swimmer");
    }

    let data_dir = matches.get_one::<PathBuf>("data-dir")
    .expect("clap should have provided a default value for data-dir");
    info!("Using {} as data dir", data_dir.display());

    let should_broadcast = matches.get_one::<bool>("broadcast")
    .unwrap_or(&false)
    .to_owned();

    let mut broadcast_handler = Handler::new(HashSet::new(), handle_message);
    let broadcast_data = get_broadcast_data();
    let broadcast_msg = broadcast_handler.craft_broadcast(SyncOperation {
        operation_id: Uuid::new_v4()
    }, GossipMessage::new(FullSync, broadcast_data));

    let mut foca = Foca::with_custom_broadcast(identity.clone(), config, rng, PostcardCodec, broadcast_handler);
    
    if should_broadcast {
        match foca.add_broadcast(broadcast_msg.as_ref()) {
            Ok(_) => info!("Added broadcast"),
            Err(e) => error!("Could not add broadcast: {}", e),
        }
    } else {
        info!("Not broadcasting");
    }

    let socket = Arc::new(UdpSocket::bind(bind_addr).await?);

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

    // We'll also launch a task to manage Foca. Since there are timers
    // involved, one simple way to do it is unifying the input:
    enum Input<T> {
        Event(Timer<T>),
        Data(Bytes),
        Announce(T),
    }
    // And communicating via channels
    let (tx_foca, mut rx_foca) = mpsc::channel(100);
    // Another alternative would be putting a Lock around Foca, but
    // yours truly likes to hide behind (the lock inside) channels
    // instead.
    let mut runtime = AccumulatingRuntime::new();
    let mut members = Members::new();
    members.add_member(identity);
    let tx_foca_copy = tx_foca.clone();
    tokio::spawn(async move {
        while let Some(input) = rx_foca.recv().await {
            debug_assert_eq!(0, runtime.backlog());

            let result = match input {
                Input::Event(timer) => foca.handle_timer(timer, &mut runtime),
                Input::Data(data) => foca.handle_data(&data, &mut runtime),
                Input::Announce(dst) => foca.announce(dst, &mut runtime),
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
    });

    // Foca is running, we can tell it to announce to our target
    if let Some(dst) = announce_to {
        let _ignored_send_error = tx_foca.send(Input::Announce(dst)).await;
    }

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
}