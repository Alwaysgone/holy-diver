use std::{
    collections::HashSet,
    net::SocketAddr,
    time::SystemTime,
};
use bincode::Options;
use bytes::{Bytes, BytesMut, BufMut,};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use log::{debug, info};
use chrono::NaiveDateTime;

use foca::{BroadcastHandler, Invalidates};

// Broadcasts here will always have the following shape:
//
// 0. Tag describing the payload
// 1. Payload (e.g. GossipMessage)
//

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub enum Tag {
    // We can propagate general-purpose operations. Foca shouldn't
    // care about what's inside the payload, just wether this
    // has been acted on already or not.
    // Side-effects, conflict resolution and whatnot are not its
    // responsibilities, so messages like these aren't invalidated
    // at all: everything NEW it receives will be broadcast.
    SyncOperation {
        operation_id: Uuid,
        // Depending on the nature of the operations, this could
        // use more metadata.
        // E.g.: members may receive operations out of order;
        // If the storage doesn't handle that correctly you'll
        // need to do it yourself
    },

    StartupMessage {
        startup_time: NaiveDateTime,
        node_id: Uuid,
    },

    // For scenarios where the interactions are very clear, we can
    // be smarter with what we decide to broadcast.
    // E.g.: In our cluster we expect nodes to broadcast their
    // configuration when they join and when it changes, so for
    // any key `node`, there's only one writer (assuming no byzantine
    // behaviour): we can simply use last-write wins
    NodeConfig {
        node: SocketAddr,
        // XXX SystemTime does NOT guarantee monotonic growth.
        //     It's good enough for an example, but it's an outage
        //     waiting to happen. Use something you have better
        //     control of.
        version: SystemTime,
    },
}

#[derive(Debug, Clone)]
pub struct Broadcast {
    pub tag: Tag,
    pub data: Bytes,
}

impl Invalidates for Broadcast {
    fn invalidates(&self, other: &Self) -> bool {
        match (self.tag, other.tag) {
            (Tag::SyncOperation {
                operation_id: self_operation_id
            },
            Tag::SyncOperation {
                operation_id: other_operation_id
            }) => self_operation_id.eq(&other_operation_id),
            _ => false
        }
    }
}

impl AsRef<[u8]> for Broadcast {
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GossipMessage {
    message_type: MessageType,
    message_payload: Vec<u8>,
}

impl GossipMessage {
    pub fn new(message_type: MessageType, message_payload: Vec<u8>) -> Self {
        GossipMessage {
            message_type,
            message_payload
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub enum MessageType {
    FullSync,
    IncSync,
}

pub struct Handler<'a> {
    seen_op_ids: HashSet<Uuid>,
    data_handler: Box<dyn DataHandler + Send + Sync + 'a>,
}

pub trait DataHandler {
    fn handle_message(&mut self, msg_type:MessageType, data:Vec<u8>);

    fn get_state(&mut self) -> Vec<u8>;
}

impl Handler<'_> {
    pub fn new(
        seen_op_ids: HashSet<Uuid>,
        data_handler: Box<dyn DataHandler + Send + Sync>,) -> Self {
        Self {
            seen_op_ids,
            data_handler,
        }
    }

    pub fn craft_broadcast(&mut self, tag: Tag, item: GossipMessage) -> Broadcast {
        let mut writer = BytesMut::new().writer();
        let opts = bincode::DefaultOptions::new();
        opts.serialize_into(&mut writer, &tag).expect("error handling");
        opts.serialize_into(&mut writer, &item).expect("error handling");
        Broadcast {
            tag: tag,
            data: writer.into_inner().freeze()
        }
    }
}

impl<T> BroadcastHandler<T> for Handler<'_> {
    type Broadcast = Broadcast;
    type Error = String;

    fn receive_item(
        &mut self,
        data: impl bytes::Buf,
        _sender: Option<&T>,
    ) -> Result<Option<Self::Broadcast>, Self::Error> {
        info!("Receiving item ...");
        let opts = bincode::DefaultOptions::new();
        let mut reader = data.reader();

        let tag: Tag = opts.deserialize_from(&mut reader).unwrap();

        match tag {
            Tag::SyncOperation {
                operation_id
            } => {
                if self.seen_op_ids.contains(&operation_id) {
                    info!("Got already seen broadcast with id {}", &operation_id);
                    // necessary to advance the reader cursor and not start reading a new broadcast from this partially read one
                    // at the next invocation of receive_item
                    let _msg: GossipMessage = opts.deserialize_from(&mut reader).expect("error handling");
                    // We've seen this data before, nothing to do
                    return Ok(None);
                }
                info!("Got new broadcast with id {}", &operation_id);
                self.seen_op_ids.insert(operation_id);

                let msg: GossipMessage = opts.deserialize_from(&mut reader).expect("error handling");

                // let op: Operation = opts.deserialize_from(&mut reader).expect("error handling");
                {
                    // This is where foca stops caring
                    // If it were me, I'd stuff the bytes as-is into a channel
                    // and have a separate task/thread consuming it.
                    self.data_handler.handle_message(msg.message_type, msg.message_payload.clone());
                }

                // This WAS new information, so we signal it to foca
                debug!("Crafting broadcast with msg {:?}", msg);
                let broadcast = self.craft_broadcast(tag, msg);
                Ok(Some(broadcast))
            },
            Tag::StartupMessage {
                startup_time: _,
                node_id: _,
            } => {
                //TODO check if node_id and startup_time combo was already seen and if not send full state up date message
                let current_state = self.data_handler.get_state();
                let broadcast = self.craft_broadcast(Tag::SyncOperation {
                    operation_id: Uuid::new_v4()
                }, GossipMessage::new(MessageType::FullSync, current_state));
                Ok(Some(broadcast))
            },
          _ => Ok(None)

        }
    }
}
