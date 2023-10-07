use std::{
    time::Duration, path::PathBuf, io::{BufReader, Read, Write}, fs::{File, self}, net::SocketAddr, sync::{Mutex, Arc}, cell::RefCell
};
use automerge::{Automerge, ActorId, AutoCommit, transaction::Transactable, ObjType, ROOT};
use bytes::{BufMut, Bytes, BytesMut};
use foca::{Identity, Notification, Runtime, Timer, Config};
use log::{info, error, trace};
use super::{broadcast::{MessageType, MessageType::FullSync, DataHandler}, types::ID};

pub struct AccumulatingRuntime<T> {
    pub to_send: Vec<(T, Bytes)>,
    pub to_schedule: Vec<(Duration, Timer<T>)>,
    pub notifications: Vec<Notification<T>>,
}

impl<T: Identity> Runtime<T> for AccumulatingRuntime<T> {
    // Notice that we'll interact to these via pop(), so we're taking
    // them in reverse order of when it happened.
    // That's perfectly fine, the order of items from a single interaction
    // is irrelevant. A "nicer" implementation could use VecDeque or
    // react directly here instead of accumulating.

    fn notify(&mut self, notification: Notification<T>) {
        self.notifications.push(notification);
    }

    fn send_to(&mut self, to: T, data: &[u8]) {
        let mut packet = BytesMut::new();
        packet.put_slice(data);
        let packet_to_send = packet.freeze();
        trace!("Packet to send: {:?}", packet_to_send);
        self.to_send.push((to, packet_to_send));
    }

    fn submit_after(&mut self, event: Timer<T>, after: Duration) {
        // We could spawn+sleep here
        self.to_schedule.push((after, event));
    }
}

impl<T> AccumulatingRuntime<T> {
    pub fn new() -> Self {
        Self {
            to_send: Vec::new(),
            to_schedule: Vec::new(),
            notifications: Vec::new(),
        }
    }

    pub fn backlog(&self) -> usize {
        self.to_send.len() + self.to_schedule.len() + self.notifications.len()
    }
}

pub struct MyDataHandler {
    data:Arc<Mutex<AutoCommit>>,
    data_path:PathBuf,
}

pub fn read_state_from_disk(data_dir:&PathBuf) -> AutoCommit {
    let automerge_doc_path = data_dir.join("automerge.dat");
    let automerge_doc;
    if automerge_doc_path.exists() {
        let mut read_buffer = Vec::new();    
        automerge_doc = match File::open(automerge_doc_path.clone())
        .map(|f| BufReader::new(f))
        .map(|mut r| r.read_to_end(&mut read_buffer)) {
            Ok(_) => {
                match AutoCommit::load(&read_buffer) {
                    Ok(doc) => {
                        info!("Loaded state from {}", automerge_doc_path.display());
                        doc
                    },
                    Err(e) => {
                        error!("Could not load state from {}: {}", automerge_doc_path.display(), e);
                        get_initial_state()
                    }
                }
            },
            Err(e) => {
                error!("Could not read file at {}: {}", automerge_doc_path.display(), e);
                get_initial_state()
            },
        };
    } else {
        info!("No state found at {}, creating initial state ...", automerge_doc_path.display());
        automerge_doc = get_initial_state();
    }
    automerge_doc
}

impl DataHandler for MyDataHandler {

    fn handle_message(&mut self, msg_type:MessageType, msg_payload:Vec<u8>) {
        info!("Received message of type {:?}: {:?}", msg_type, msg_payload);
        match msg_type {
            FullSync => {
                match AutoCommit::load(&msg_payload) {
                    Ok(doc) => {
                        info!("Received document: {:?}", doc);
                        self.merge(doc);
                    },
                    Err(e) => error!("Could not parse FullSync message: {}", e),
                }
            },
            other => {
                info!("Handling of message type {:?} currently not implemented", other);
            }
        }
    }

    fn get_state(&mut self) -> Vec<u8> {
        self.data.lock().unwrap().save()
    }
}

impl MyDataHandler {
    pub fn new(data_dir:&PathBuf, intial_state:Arc<Mutex<AutoCommit>>) -> Self {
        MyDataHandler {
            data: intial_state,
            data_path: data_dir.to_owned(),
        }
    }

    // fn store(&mut self) {
    //     let automerge_doc_path = self.data_path.join("automerge.dat");

    //     info!("Storing to {} ...", automerge_doc_path.display());
    //     let mut file = fs::OpenOptions::new()
    //     .write(true)
    //     .truncate(true)
    //     .create(true)
    //     .open(automerge_doc_path.clone())
    //     .unwrap();

    //     match file.write_all(&self.data.lock().unwrap().save()) {
    //         Ok(_) => {
    //             info!("Wrote current state to {}", automerge_doc_path.display());
    //         },
    //         Err(e) => {
    //             error!("Could not write current state to {}: {}", automerge_doc_path.display(), e);
    //         }
    //     }        
    // }

    fn store_data(mut data:AutoCommit, data_path:&PathBuf) {
        let mut file = fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(data_path.clone())
        .unwrap();

        match file.write_all(&data.save()) {
            Ok(_) => {
                info!("Wrote current state to {}", data_path.display());
            },
            Err(e) => {
                error!("Could not write current state to {}: {}", data_path.display(), e);
            }
        }   
    }

    fn merge(&mut self, mut other:AutoCommit) {
        let automerge_doc_path = self.data_path.join("automerge.dat");
        let mut data = self.data.lock().unwrap();
        match data.merge(&mut other) {
            Ok(cs) => {
                info!("Merged {} changes into local state", cs.len());
                Self::store_data(data.to_owned(), &automerge_doc_path);
            },
            Err(e) => {
                error!("Could not merge changes into local state: {}", e);
            },
        }
    }
}

fn get_initial_state() -> AutoCommit {
    let mut state = AutoCommit::new()
    .with_actor(ActorId::from("default".as_bytes()));
    state.put_object(ROOT, "values", ObjType::Map).unwrap();
    state
}

pub struct FocaRuntimeConfig {
    pub identity: ID,
    pub data_dir: PathBuf,
    pub bind_addr: SocketAddr,
    pub announce_to: Option<ID>,
    pub foca_config: Config
}

// pub trait HolyDiverController {
//     fn get_field(&self, field_name: String) -> Result<String, anyhow::Error>;

//     fn set_field(&mut self, field_name: String, field_value: String) -> Result<(), anyhow::Error>;
// }