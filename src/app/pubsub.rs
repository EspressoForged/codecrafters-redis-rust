use crate::app::protocol::RespValue;
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::{HashSet};
use tokio::sync::mpsc;
use tracing::debug;

type ClientId = u64;
type SubscriberMap = DashMap<ClientId, mpsc::Sender<RespValue>>;

#[derive(Debug, Default)]
pub struct PubSubHub {
    /// Maps a channel name to a map of subscribers.
    /// The inner map maps a unique client ID to their sender channel.
    channels: DashMap<Bytes, SubscriberMap>,
}

impl PubSubHub {
    pub fn new() -> Self {
        Self::default()
    }

    /// Subscribes a client to a channel.
    pub fn subscribe(&self, channel: Bytes, client_id: ClientId, sender: mpsc::Sender<RespValue>) {
        self.channels
            .entry(channel)
            .or_default()
            .insert(client_id, sender);
    }

    /// Unsubscribes a client from a specific channel.
    pub fn unsubscribe(&self, channel: &Bytes, client_id: ClientId) {
        if let Some(channel_subs) = self.channels.get_mut(channel) {
            channel_subs.remove(&client_id);
            if channel_subs.is_empty() {
                // Clean up the channel entry if no subscribers are left.
                drop(channel_subs); // Release lock before removing
                self.channels.remove(channel);
            }
        }
    }
    
    /// Unsubscribes a client from all channels they are part of.
    pub fn unsubscribe_all(&self, client_id: ClientId, subscriptions: &HashSet<Bytes>) {
         for channel in subscriptions {
              self.unsubscribe(channel, client_id);
         }
    }

    /// Publishes a message to all subscribers of a channel.
    /// Returns the number of clients that received the message.
    pub fn publish(&self, channel: Bytes, message: Bytes) -> usize {
        let Some(subscribers) = self.channels.get(&channel) else {
            return 0;
        };

        let message_payload = RespValue::Array(vec![
            RespValue::BulkString(Bytes::from_static(b"message")),
            RespValue::BulkString(channel),
            RespValue::BulkString(message),
        ]);
        
        let mut recipient_count = 0;
        
        // We iterate over subscribers, cloning their senders. If a send fails,
        // it means the client has disconnected, but we don't handle cleanup here
        // for simplicity. The client's main task handles full cleanup on disconnect.
        for entry in subscribers.iter() {
            let sender = entry.value();
            if sender.try_send(message_payload.clone()).is_ok() {
                recipient_count += 1;
            }
        }
        
        debug!("Published message to {recipient_count} subscribers.");
        recipient_count
    }
}