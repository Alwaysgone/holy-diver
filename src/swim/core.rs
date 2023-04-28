use std::{
    time::Duration
};
use bytes::{BufMut, Bytes, BytesMut};
use foca::{Identity, Notification, Runtime, Timer};
use log::trace;

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