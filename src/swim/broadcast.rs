use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    time::SystemTime, io::Read, str::FromStr,
};
use std::io::Write;
use bincode::{Options, de};
use bytes::{Bytes, BytesMut, BufMut,};
use serde::{Deserialize, Serialize, ser::{SerializeStruct, self}, de::Visitor};
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawBroadcast {
    pub id: Uuid,
    pub data: Vec<u8>,
}

// impl Serialize for RawBroadcast {

//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: serde::Serializer {
//         // let mut broadcast = serializer.serialize_struct("Rawbroadcast", 2)?;
//         // broadcast.serialize_field("id", &self.id)?;
        
//         // broadcast.serialize_field("data", &self.data)?;
//         serializer.serialize_str(&self.id.to_string())?;
//         serializer.serialize_bytes(&self.data)
//         // serializer.
//         // Ok(serializer);
//         // broadcast.end()
//     }
// }

// struct UuidRawBroadcastVisitor {

// }

// impl<'de> Visitor<'de> for UuidRawBroadcastVisitor {
//     type Value = Uuid;

//     fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
//         write!(formatter, "a uuidv4 string")
//     }

//     fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
//         where
//             E: serde::de::Error, {
//         let uuid = Uuid::from_str(&v).unwrap();
//         Ok(uuid)
//     }
// }

// struct BytesRawBroadcastVisitor {

// }

// impl<'de> Visitor<'de> for BytesRawBroadcastVisitor {
//     type Value = Vec<u8>;

//     fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
//         write!(formatter, "a uuidv4 string")
//     }

//     fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
//         where
//             E: serde::de::Error, {
//         Ok(v.into())
//     }
// }

// impl<'de> Deserialize<'de> for RawBroadcast {
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//     where
//         D: serde::Deserializer<'de> {
//             let uuid = deserializer.deserialize_string(UuidRawBroadcastVisitor{})?;
//             let data = deserializer.deserialize_byte_buf(BytesRawBroadcastVisitor{})?;
//             Ok(RawBroadcast {
//                 id: uuid,
//                 data: data,
//             })
//     }
// }

impl Invalidates for RawBroadcast {
    // I think this is where confusion happens: It's about invalidating
    // items ALREADY in the broadcast buffer, i.e.: foca uses this
    // to manage its broadcast buffer so it can stop talking about unecessary
    // (invalidated) data.
    fn invalidates(&self, _other: &Self) -> bool {
        false
    }
}

impl AsRef<[u8]> for RawBroadcast {
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
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
    seen_data: Vec<Bytes>,
    node_config: HashMap<SocketAddr, (SystemTime, NodeConfig)>,
    message_handler: fn(MessageType, Bytes),
}

impl Handler {
    pub fn new(
        seen_op_ids: HashSet<Uuid>,
        seen_data: Vec<Bytes>,
        node_config: HashMap<SocketAddr, (SystemTime, NodeConfig)>,
        message_handler: fn(MessageType, Bytes)) -> Self {
        Self {
            seen_op_ids,
            seen_data,
            node_config,
            message_handler
        }
    }

    pub fn craft_broadcast3(&mut self, broadcast:RawBroadcast) -> Bytes {
        let mut writer = BytesMut::new().writer();

        let opts = bincode::DefaultOptions::new();
        opts.serialize_into(&mut writer, &broadcast)
            .expect("error handling");
        writer.into_inner().freeze()
    }

    pub fn craft_broadcast2(&mut self, tag: Tag, item: &Vec<u8>) -> Broadcast {
        let mut broadcast_data = BytesMut::new();
        let mut writer = BytesMut::new().writer();
        let opts = bincode::DefaultOptions::new();
        opts.serialize_into(&mut writer, &tag)
            .expect("error handling");
        writer.write(item)
        .expect("error handling");

        let content = writer.into_inner();
        let final_len = content.len() as u16;
        broadcast_data.put_u16(final_len);
        broadcast_data.extend(content);

        let broadcast = Broadcast {
            tag,
            data: broadcast_data.freeze(),
        };
        info!("Crafted broadcast2: {:?}", broadcast.clone());
        broadcast
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
    type Broadcast = RawBroadcast;
    type Error = String;

    fn receive_item(
        &mut self,
        data: impl bytes::Buf,
    ) -> Result<Option<Self::Broadcast>, Self::Error> {
        info!("Receiving item ...");
        let opts = bincode::DefaultOptions::new();
        let mut reader = data.reader();
        // let id:Uuid = opts.deserialize_from(&mut reader).unwrap();
        // let content:Vec<u8> = opts.deserialize_from(&mut reader).unwrap();
        let broadcast:RawBroadcast = opts.deserialize_from(&mut reader).unwrap();
        // let broadcast = RawBroadcast {
        //     id: id,
        //     data: content,
        // };
        info!("Received RawBroadcast: {:?}", broadcast.clone());
        // let mut read_data:Vec<u8> = vec![];
        // reader.read_to_end(&mut read_data)
        // .expect("could not read payload");
        // let bytes = Bytes::from(read_data);
        // info!("Raw data: {:?}", bytes);
        if self.seen_op_ids.contains(&broadcast.id) {
            info!("Got already seen broadcast");
            return Ok(None);
        }
        self.seen_op_ids.insert(broadcast.id.clone());
        (self.message_handler)(MessageType::FullSync, Bytes::from(broadcast.data.clone()));
        info!("Received new data, broadcasting it ...");
        Ok(Some(broadcast))
        // let raw_data = data.chunk();
        // info!("Raw data: {:?}", raw_data);
        // // Broadcast payload is u16-length prefixed
        // if data.remaining() < 2 {
        //     return Err(String::from("Not enough bytes"));
        // }

        // let mut cursor = data;
        // let len = cursor.get_u16();
        // if cursor.remaining() < usize::from(len) {
        //     return Err(String::from("Malformed packet"));
        // }

        // // And a tag/header that tells us what the remaining
        // // bytes actually are. We leave the blob untouched until
        // // we decide wether we care about it.
        // let opts = bincode::DefaultOptions::new();
        // let mut reader = cursor.reader();
        // let tag: Tag = opts.deserialize_from(&mut reader).unwrap();

        // // Now `reader` points at the actual useful data in
        // // the buffer, immediatelly after the tag. We can finally
        // // make a decision
        // match tag {
        //     Tag::Operation { operation_id} => {
        //         if self.seen_op_ids.contains(&operation_id) {
        //             // We've seen this data before, nothing to do
        //             info!("Got already seen broadcast");
        //             return Ok(None);
        //         }

        //         self.seen_op_ids.insert(operation_id.clone());
        //         let mut data:Vec<u8> = vec![];
        //         reader.read_to_end(&mut data)
        //         .expect("could not read payload");
        //         // let op: Operation = opts.deserialize_from(&mut reader).expect("error handling");
        //         {
        //             // This is where foca stops caring
        //             // If it were me, I'd stuff the bytes as-is into a channel
        //             // and have a separate task/thread consuming it.
        //             (self.message_handler)(MessageType::FullSync, data.clone());
        //         }

        //         // This WAS new information, so we signal it to foca
        //         let broadcast = self.craft_broadcast2(tag, &data);
        //         // GossipMessage::new(MessageType::FullSync, vec!(1, 2))
        //         Ok(Some(broadcast))
        //     }
        //     Tag::NodeConfig { node, version } => {
        //         if let Some((current_version, _)) = self.node_config.get(&node) {
        //             if &version > current_version {
        //                 let conf: NodeConfig =
        //                     opts.deserialize_from(&mut reader).expect("error handling");
        //                 Ok(Some(self.craft_broadcast(tag, conf)))
        //             } else {
        //                 Ok(None)
        //             }
        //         } else {
        //             let conf: NodeConfig =
        //                 opts.deserialize_from(&mut reader).expect("error handling");
        //             Ok(Some(self.craft_broadcast(tag, conf)))
        //         }
        //     }
        // }
    }
}
