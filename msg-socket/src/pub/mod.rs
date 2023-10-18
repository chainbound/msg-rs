use bytes::Bytes;
use msg_wire::pubsub;
use thiserror::Error;

mod driver;
mod socket;
mod stats;
mod trie;
pub use socket::*;
use stats::SocketStats;

#[derive(Debug, Error)]
pub enum PubError {
    #[error("IO error: {0:?}")]
    Io(#[from] std::io::Error),
    #[error("Wire protocol error: {0:?}")]
    Wire(#[from] msg_wire::reqrep::Error),
    #[error("Authentication error: {0}")]
    Auth(String),
    #[error("Socket closed")]
    SocketClosed,
    #[error("Topic already exists")]
    TopicExists,
    #[error("Unknown topic: {0}")]
    UnknownTopic(String),
    #[error("Topic closed")]
    TopicClosed,
    #[error("Transport error: {0:?}")]
    Transport(#[from] Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug)]
pub struct PubOptions {
    pub max_connections: Option<usize>,
    pub session_buffer_size: usize,
    /// The interval at which each session should be flushed. If this is `None`,
    /// the session will be flushed on every publish, which can add a lot of overhead.
    pub flush_interval: Option<std::time::Duration>,
}

impl Default for PubOptions {
    fn default() -> Self {
        Self {
            max_connections: None,
            session_buffer_size: 1024,
            flush_interval: Some(std::time::Duration::from_micros(100)),
        }
    }
}

/// TODO: Add more options.
// pub struct TopicOptions {
//     max_subscribers: Option<usize>,
//     /// If true, the publisher will track the last sequence number for each subscriber.
//     track_subscribers: bool,
//     queue_size: usize,
// }

// impl Default for TopicOptions {
//     fn default() -> Self {
//         Self {
//             max_subscribers: None,
//             track_subscribers: false,
//             queue_size: 1024,
//         }
//     }
// }

/// A message received from a publisher.
/// Includes the source, topic, and payload.
#[derive(Debug, Clone)]
pub struct PubMessage {
    /// The topic of the message.
    topic: String,
    /// The message payload.
    payload: Bytes,
}

#[allow(unused)]
impl PubMessage {
    pub fn new(topic: String, payload: Bytes) -> Self {
        Self { topic, payload }
    }

    #[inline]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    #[inline]
    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    #[inline]
    pub fn into_payload(self) -> Bytes {
        self.payload
    }

    #[inline]
    pub fn into_wire(self, seq: u32) -> pubsub::Message {
        pubsub::Message::new(seq, Bytes::from(self.topic), self.payload)
    }
}

/// The publisher socket state, shared between the backend task and the socket.
#[derive(Debug, Default)]
pub(crate) struct SocketState {
    pub(crate) stats: SocketStats,
}

// pub(crate) enum Command {
//     Publish {
//         topic: String,
//         message: Bytes,
//     },
//     RegisterTopic {
//         /// The topic to register.
//         topic: String,
//         /// The options for the topic.
//         options: TopicOptions,
//     },
//     Shutdown,
// }

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::StreamExt;
    use msg_transport::{Tcp, TcpOptions};

    use crate::SubSocket;

    use super::*;

    #[tokio::test]
    async fn pubsub_simple() {
        let _ = tracing_subscriber::fmt::try_init();

        let mut pub_socket = PubSocket::new(Tcp::new());
        let mut sub_socket = SubSocket::new(Tcp::new_with_options(
            TcpOptions::default().with_blocking_connect(),
        ));

        pub_socket.bind("0.0.0.0:0").await.unwrap();
        let addr = pub_socket.local_addr().unwrap();

        sub_socket.connect(&addr.to_string()).await.unwrap();
        sub_socket.subscribe("HELLO".to_string()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        pub_socket
            .publish("HELLO".to_string(), Bytes::from("WORLD"))
            .await
            .unwrap();

        let msg = sub_socket.next().await.unwrap();
        tracing::info!("Received message: {:?}", msg);
        assert_eq!("HELLO", msg.topic());
        assert_eq!("WORLD", msg.payload());
    }
}
