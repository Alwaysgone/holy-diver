use std::net::SocketAddr;
use serde::{Serialize, Deserialize};
use foca::Identity;
use rand;

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ID {
    pub addr: SocketAddr,
    // An extra field to allow fast rejoin
    pub bump: u16,
}


// We implement a custom, simpler Debug format just to make the tracing
// output cuter
impl std::fmt::Debug for ID {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("ID")
            .field(&self.addr)
            .field(&self.bump)
            .finish()
    }
}

impl ID {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            bump: rand::random(),
        }
    }
}

impl Identity for ID {
    // Since a client outside the cluster will not be aware of our
    // `bump` field, we implement the optional trait method
    // `has_same_prefix` to allow anyone that knows our `addr`
    // to join our cluster.
    fn has_same_prefix(&self, other: &Self) -> bool {
        self.addr.eq(&other.addr)
    }

    // And by implementing `renew` we enable automatic rejoining:
    // when another member declares us as down, Foca immediatelly
    // switches to this new identity and rejoins the cluster for us
    fn renew(&self) -> Option<Self> {
        Some(Self {
            addr: self.addr,
            bump: self.bump.wrapping_add(1),
        })
    }
}
