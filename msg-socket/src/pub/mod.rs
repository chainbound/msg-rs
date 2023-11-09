use bytes::Bytes;
use msg_wire::pubsub;
use thiserror::Error;

mod driver;
mod session;
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
    /// The maximum number of concurrent clients.
    max_clients: Option<usize>,
    /// The maximum number of outgoing messages that can be buffered per session.
    session_buffer_size: usize,
    /// The interval at which each session should be flushed. If this is `None`,
    /// the session will be flushed on every publish, which can add a lot of overhead.
    flush_interval: Option<std::time::Duration>,
    /// The maximum number of bytes that can be buffered in the session before being flushed.
    /// This internally sets [`Framed::set_backpressure_boundary`](tokio_util::codec::Framed).
    backpressure_boundary: usize,
}

impl Default for PubOptions {
    fn default() -> Self {
        Self {
            max_clients: None,
            session_buffer_size: 1024,
            flush_interval: Some(std::time::Duration::from_micros(50)),
            backpressure_boundary: 8192,
        }
    }
}

impl PubOptions {
    /// Sets the maximum number of concurrent clients.
    pub fn max_clients(mut self, max_clients: usize) -> Self {
        self.max_clients = Some(max_clients);
        self
    }

    /// Sets the session channel buffer size. This is the amount of messages that can be buffered
    /// per session before messages start being dropped.
    pub fn session_buffer_size(mut self, session_buffer_size: usize) -> Self {
        self.session_buffer_size = session_buffer_size;
        self
    }

    /// Sets the maximum number of bytes that can be buffered in the session before being flushed.
    /// This internally sets [`Framed::set_backpressure_boundary`](tokio_util::codec::Framed).
    pub fn backpressure_boundary(mut self, backpressure_boundary: usize) -> Self {
        self.backpressure_boundary = backpressure_boundary;
        self
    }

    /// Sets the interval at which each session should be flushed. If this is `None`,
    /// the session will be flushed on every publish, which can add a lot of overhead.
    pub fn flush_interval(mut self, flush_interval: std::time::Duration) -> Self {
        self.flush_interval = Some(flush_interval);
        self
    }
}

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
            .publish("HELLO".to_string(), "WORLD".into())
            .await
            .unwrap();

        let msg = sub_socket.next().await.unwrap();
        tracing::info!("Received message: {:?}", msg);
        assert_eq!("HELLO", msg.topic());
        assert_eq!("WORLD", msg.payload());
    }

    #[tokio::test]
    async fn pubsub_many() {
        let _ = tracing_subscriber::fmt::try_init();

        let mut pub_socket = PubSocket::new(Tcp::new());
        let mut sub1 = SubSocket::new(Tcp::new_with_options(
            TcpOptions::default().with_blocking_connect(),
        ));

        let mut sub2 = SubSocket::new(Tcp::new_with_options(
            TcpOptions::default().with_blocking_connect(),
        ));

        pub_socket.bind("0.0.0.0:0").await.unwrap();
        let addr = pub_socket.local_addr().unwrap();

        sub1.connect(&addr.to_string()).await.unwrap();
        sub2.connect(&addr.to_string()).await.unwrap();
        sub1.subscribe("HELLO".to_string()).await.unwrap();
        sub2.subscribe("HELLO".to_string()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        pub_socket
            .publish("HELLO".to_string(), Bytes::from("WORLD"))
            .await
            .unwrap();

        let msg = sub1.next().await.unwrap();
        tracing::info!("Received message: {:?}", msg);
        assert_eq!("HELLO", msg.topic());
        assert_eq!("WORLD", msg.payload());

        let msg = sub2.next().await.unwrap();
        tracing::info!("Received message: {:?}", msg);
        assert_eq!("HELLO", msg.topic());
        assert_eq!("WORLD", msg.payload());
    }

    #[tokio::test]
    async fn pubsub_durable() {
        let _ = tracing_subscriber::fmt::try_init();

        let mut pub_socket = PubSocket::new(Tcp::new());
        // Don't enable blocking connect
        let mut sub_socket = SubSocket::new(Tcp::new_with_options(TcpOptions::default()));

        // Try to connect and subscribe before the publisher is up
        sub_socket.connect("0.0.0.0:6662").await.unwrap();
        sub_socket.subscribe("HELLO".to_string()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1000)).await;

        pub_socket.bind("0.0.0.0:6662").await.unwrap();
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

    #[tokio::test]
    async fn pubsub_max_clients() {
        let _ = tracing_subscriber::fmt::try_init();

        let mut pub_socket =
            PubSocket::with_options(Tcp::new(), PubOptions::default().max_clients(1));

        pub_socket.bind("0.0.0.0:0").await.unwrap();

        let mut sub1 = SubSocket::new(Tcp::new_with_options(
            TcpOptions::default().with_blocking_connect(),
        ));

        let mut sub2 = SubSocket::new(Tcp::new_with_options(
            TcpOptions::default().with_blocking_connect(),
        ));

        let addr = pub_socket.local_addr().unwrap();

        sub1.connect(&addr.to_string()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(pub_socket.stats().active_clients(), 1);
        sub2.connect(&addr.to_string()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(pub_socket.stats().active_clients(), 1);
    }
}
