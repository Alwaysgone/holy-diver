use std::{
    collections::HashMap, net::SocketAddr
};

use crate::swim::types::ID;

pub struct Members(HashMap<SocketAddr, u8>);

impl Members {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    // A result of `true` means that the effective list of
    // cluster member addresses has changed
    pub fn add_member(&mut self, member: ID) -> bool {
        // Notice how we don't care at all about the `bump` part.
        // It's only useful for Foca.
        let counter = self.0.entry(member.addr).or_insert(0);

        *counter += 1;

        counter == &1
    }

    // A result of `true` means that the effective list of
    // cluster member addresses has changed
    pub fn remove_member(&mut self, member: ID) -> bool {
        let effectivelly_down = if let Some(counter) = self.0.get_mut(&member.addr) {
            *counter -= 1;

            counter == &0
        } else {
            // Shouldn't happen
            false
        };

        if effectivelly_down {
            self.0.remove(&member.addr);
        }

        effectivelly_down
    }

    pub fn addrs(&self) -> impl Iterator<Item = &SocketAddr> {
        self.0.keys()
    }
}