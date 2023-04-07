use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    time::SystemTime,
};

use bincode::Options;
use bytes::{Bytes, BytesMut, BufMut,};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use log::info;

use foca::{BroadcastHandler, Invalidates};

// Broadcasts here will always have the following shape:
//
// 0. u16 length prefix
// 1. Tag describing the payload
// 2. Payload
//

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub enum Tag {
    // We can propagate general-purpose operations. Foca shouldn't
    // care about what's inside the payload, just wether this
    // has been acted on already or not.
    // Side-effects, conflict resolution and whatnot are not its
    // responsibilities, so messages like these aren't invalidated
    // at all: everything NEW it receives will be broadcast.
    Operation {
        operation_id: Uuid,
        // Depending on the nature of the opeartions, this could
        // use more metadata.
        // E.g.: members may receive operations out of order;
        // If the storage doesn't handle that correctly you'll
        // need to do it yourself
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

impl Invalidates for Broadcast {
    // I think this is where confusion happens: It's about invalidating
    // items ALREADY in the broadcast buffer, i.e.: foca uses this
    // to manage its broadcast buffer so it can stop talking about unecessary
    // (invalidated) data.
    fn invalidates(&self, other: &Self) -> bool {
        match (self.tag, other.tag) {
            // The only time we care about invalidation is when we have
            // a new nodeconfig for a node and are already broadcasting
            // a config about this same node. We need to decide which
            // to keep.
            (
                Tag::NodeConfig {
                    node: self_node,
                    version: self_version,
                },
                Tag::NodeConfig {
                    node: other_node,
                    version: other_version,
                },
            ) if self_node == other_node => self_version > other_version,
            // Any other case we'll keep broadcasting until it gets sent
            // `Config::max_transmissions` times (or gets invalidated)
            _ => false,
        }
    }
}

impl AsRef<[u8]> for Broadcast {
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
}

// XXX Use actually useful types
type Operation = GossipMessage;
type NodeConfig = String;

pub struct Handler {
    seen_op_ids: HashSet<Uuid>,
    node_config: HashMap<SocketAddr, (SystemTime, NodeConfig)>,
    message_handler: fn(MessageType, Vec<u8>),
}

impl Handler {
    pub fn new(
        seen_op_ids: HashSet<Uuid>,
        node_config: HashMap<SocketAddr, (SystemTime, NodeConfig)>,
        message_handler: fn(MessageType, Vec<u8>)) -> Self {
        Handler {
            seen_op_ids,
            node_config,
            message_handler
        }
    }

    pub fn craft_broadcast<T: Serialize>(&mut self, tag: Tag, item: T) -> Broadcast {
        let mut crafted = BytesMut::new();
        let mut writer = BytesMut::new().writer();

        let opts = bincode::DefaultOptions::new();
        opts.serialize_into(&mut writer, &tag)
            .expect("error handling");
        opts.serialize_into(&mut writer, &item)
            .expect("error handling");

        let content = writer.into_inner();
        let final_len = content.len() as u16;
        crafted.put_u16(final_len);
        crafted.extend(content);

        // Notice that `tag` here is already inside `data`,
        // we keep a copy outside to make it easier when implementing
        // `Invalidates`
        let broadcast = Broadcast {
            tag,
            data: crafted.freeze(),
        };
        info!("Crafted broadcast: {:?}", broadcast.clone());
        broadcast
    }
}

impl<T> BroadcastHandler<T> for Handler {
    type Broadcast = Broadcast;
    type Error = String;

    fn receive_item(
        &mut self,
        data: impl bytes::Buf,
    ) -> Result<Option<Self::Broadcast>, Self::Error> {
        info!("Receiving item ...");
        let raw_data = data.chunk();
        info!("Raw data: {:?}", raw_data);
        //FIXME how to handle this seemingly foca internal message
        if *raw_data == [0, 2, 1, 2] {
            info!("Found foca internal message, ignoring it");
            // return Ok(None);
            return Err(String::from("cannot process foca specific message"));
        }
        // Broadcast payload is u16-length prefixed
        if data.remaining() < 2 {
            return Err(String::from("Not enough bytes"));
        }

        let mut cursor = data;
        let len = cursor.get_u16();
        if cursor.remaining() < usize::from(len) {
            return Err(String::from("Malformed packet"));
        }

        // And a tag/header that tells us what the remaining
        // bytes actually are. We leave the blob untouched until
        // we decide wether we care about it.
        let opts = bincode::DefaultOptions::new();
        let mut reader = cursor.reader();
        let tag: Tag = opts.deserialize_from(&mut reader).unwrap();

        // Now `reader` points at the actual useful data in
        // the buffer, immediatelly after the tag. We can finally
        // make a decision
        match tag {
            Tag::Operation { operation_id} => {
                if self.seen_op_ids.contains(&operation_id) {
                    // We've seen this data before, nothing to do
                    info!("Got already seen broadcast");
                    return Ok(None);
                }

                self.seen_op_ids.insert(operation_id);

                let op: Operation = opts.deserialize_from(&mut reader).expect("error handling");
                {
                    // This is where foca stops caring
                    // If it were me, I'd stuff the bytes as-is into a channel
                    // and have a separate task/thread consuming it.
                    (self.message_handler)(op.message_type, op.message_payload.clone());
                }

                // This WAS new information, so we signal it to foca
                let broadcast = self.craft_broadcast(Tag::Operation {
                    operation_id: operation_id
                }, op);
                Ok(Some(broadcast))
            }
            Tag::NodeConfig { node, version } => {
                if let Some((current_version, _)) = self.node_config.get(&node) {
                    if &version > current_version {
                        let conf: NodeConfig =
                            opts.deserialize_from(&mut reader).expect("error handling");
                        Ok(Some(self.craft_broadcast(tag, conf)))
                    } else {
                        Ok(None)
                    }
                } else {
                    let conf: NodeConfig =
                        opts.deserialize_from(&mut reader).expect("error handling");
                    Ok(Some(self.craft_broadcast(tag, conf)))
                }
            }
        }
    }
}
