use std::{
    time::Duration, path::PathBuf, io::{BufReader, Read, Write}, fs::{File, self}, net::SocketAddr, sync::{Mutex, Arc}
};
use automerge::{ActorId, AutoCommit, transaction::Transactable, ObjType, ROOT, ReadDoc};
use bytes::{BufMut, Bytes, BytesMut};
use foca::{Identity, Notification, Runtime, Timer, Config};
use log::{info, error, trace};
use tokio::sync::mpsc::Sender;
use uuid::Uuid;
use super::{broadcast::{MessageType, MessageType::FullSync, DataHandler, GossipMessage, Tag::SyncOperation}, types::ID, foca::FocaCommand};
use anyhow::Result;

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
}

pub struct HolyDiverDataHandler {
    data: Mutex<AutoCommit>,
    data_path: PathBuf,
}

pub fn read_state_from_disk(data_dir: &PathBuf, identity: ID) -> AutoCommit {
    let automerge_doc_path = data_dir.join("automerge.dat");
    let automerge_doc;
    if automerge_doc_path.exists() {
        let mut read_buffer = Vec::new();    
        automerge_doc = match File::open(automerge_doc_path.clone())
        .map(BufReader::new)
        .map(|mut r| r.read_to_end(&mut read_buffer)) {
            Ok(_) => {
                match AutoCommit::load(&read_buffer) {
                    Ok(doc) => {
                        info!("Loaded state from {}", automerge_doc_path.display());
                        doc
                    },
                    Err(e) => {
                        error!("Could not load state from {}: {}", automerge_doc_path.display(), e);
                        get_initial_state(identity)
                    }
                }
            },
            Err(e) => {
                error!("Could not read file at {}: {}", automerge_doc_path.display(), e);
                get_initial_state(identity)
            },
        };
    } else {
        info!("No state found at {}, creating initial state ...", automerge_doc_path.display());
        automerge_doc = get_initial_state(identity);
    }
    automerge_doc
}

impl DataHandler for HolyDiverDataHandler {

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

impl HolyDiverDataHandler {
    pub fn new(data_dir: &PathBuf, identity: ID) -> Self {
        let initial_state = Mutex::from(read_state_from_disk(data_dir, identity));
        HolyDiverDataHandler {
            data: initial_state,
            data_path: data_dir.to_owned(),
        }
    }

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
        let automerge_doc_path = Self::get_state_path(&self.data_path);
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

    pub fn get_field(&self, field_name: String) -> Option<String> {
        let state = self.data.lock().unwrap();
        let values = match state.get(ROOT, "values").unwrap() {
            Some((automerge::Value::Object(ObjType::Map), values)) => values,
            _ => panic!("a map with name values is expected in the ROOT of the AutoMerge document"),
        };
        state.get(&values, field_name).unwrap()
            .map(|(v,_)| v)
            .map(|v| v.to_string())
    }

    pub async fn set_field(&mut self, field_name: String, field_value: String) -> Result<()> {
        let mut state = self.data.lock().unwrap();
        let values = match state.get(ROOT, "values").unwrap() {
            Some((automerge::Value::Object(ObjType::Map), values)) => values,
            _ => panic!("a map with name values is expected in the ROOT of the AutoMerge document"),
        };
        state.put(&values, field_name, field_value)?;
        let automerge_doc_path = Self::get_state_path(&self.data_path);
        Self::store_data(state.to_owned(), &automerge_doc_path);
        Ok(())
    }

    fn get_state_path(data_path: &PathBuf) -> PathBuf {
        data_path.join("automerge.dat")
    }
}

fn get_initial_state(identity: ID) -> AutoCommit {
    let mut state = AutoCommit::new()
    .with_actor(ActorId::from(format!("{:?}", identity).as_bytes()));
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

pub struct HolyDiverController {
    pub foca_command_sender: Sender<FocaCommand>,
    pub data_handler: Arc<Mutex<HolyDiverDataHandler>>,
}

impl HolyDiverController {
    pub fn get_field(&self, field_name: String) -> Option<String> {
        self.data_handler.lock().unwrap().get_field(field_name)
    }

    pub async fn set_field(&mut self, field_name: String, field_value: String) -> Result<()> {
        let mut handler = self.data_handler.lock().unwrap();
        handler.set_field(field_name, field_value).await?;
        // broadcasting the change so that all nodes get this update
        self.foca_command_sender.send(FocaCommand::SendBroadcast((SyncOperation {
            operation_id: Uuid::new_v4()
        }, GossipMessage::new(FullSync, handler.get_state())))).await?;
        Ok(())
    }
}