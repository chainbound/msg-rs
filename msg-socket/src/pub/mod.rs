use std::{io, time::Duration};

use bytes::Bytes;
use msg_common::constants::KiB;
use thiserror::Error;

mod driver;

mod session;

mod socket;
pub use socket::*;

mod stats;
use crate::{Profile, stats::SocketStats};
use stats::PubStats;

mod trie;

use msg_wire::{
    compression::{CompressionType, Compressor},
    pubsub,
};

/// The default high water mark for the socket.
const DEFAULT_HWM: usize = 1024;

#[derive(Debug, Error)]
pub enum PubError {
    #[error("IO error: {0:?}")]
    Io(#[from] io::Error),
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
    #[error("Could not connect to any valid endpoints")]
    NoValidEndpoints,
}

#[derive(Debug)]
pub struct PubOptions {
    /// The maximum number of concurrent clients.
    max_clients: Option<usize>,
    /// The maximum number of outgoing messages that can be buffered per session.
    high_water_mark: usize,
    /// The size of the write buffer in bytes.
    pub write_buffer_size: usize,
    /// The linger duration for the write buffer (how long to wait before flushing).
    pub write_buffer_linger: Option<Duration>,
    /// Minimum payload size in bytes for compression to be used. If the payload is smaller than
    /// this threshold, it will not be compressed.
    min_compress_size: usize,
}

impl PubOptions {
    /// Creates new options based on the given profile.
    pub fn new(profile: Profile) -> Self {
        match profile {
            Profile::Balanced => Self::balanced(),
            Profile::Latency => Self::low_latency(),
            Profile::Throughput => Self::high_throughput(),
        }
    }
}

impl PubOptions {
    /// Creates options optimized for low latency.
    pub fn low_latency() -> Self {
        Self {
            write_buffer_size: 8 * KiB as usize,
            write_buffer_linger: Some(Duration::from_micros(50)),
            ..Default::default()
        }
    }

    /// Creates options optimized for high throughput.
    pub fn high_throughput() -> Self {
        Self {
            write_buffer_size: 256 * KiB as usize,
            write_buffer_linger: Some(Duration::from_micros(200)),
            ..Default::default()
        }
    }

    /// Creates options optimized for a balanced trade-off between latency and throughput.
    pub fn balanced() -> Self {
        Self {
            write_buffer_size: 32 * KiB as usize,
            write_buffer_linger: Some(Duration::from_micros(100)),
            ..Default::default()
        }
    }
}

impl PubOptions {
    /// Sets the maximum number of concurrent clients.
    pub fn with_max_clients(mut self, max_clients: usize) -> Self {
        self.max_clients = Some(max_clients);
        self
    }

    /// Sets the high-water mark per session. This is the amount of messages that can be buffered
    /// per session before messages start being dropped.
    pub fn with_high_water_mark(mut self, hwm: usize) -> Self {
        self.high_water_mark = hwm;
        self
    }

    /// Sets the minimum payload size in bytes for compression to be used. If the payload is smaller
    /// than this threshold, it will not be compressed.
    pub fn with_min_compress_size(mut self, min_compress_size: usize) -> Self {
        self.min_compress_size = min_compress_size;
        self
    }

    /// Sets the size (max capacity) of the write buffer in bytes.
    /// When the buffer is full, it will be flushed to the underlying transport.
    ///
    /// Default: 8KiB
    pub fn with_write_buffer_size(mut self, size: usize) -> Self {
        self.write_buffer_size = size;
        self
    }

    /// Sets the linger duration for the write buffer. If `None`, the write buffer will only be
    /// flushed when the buffer is full.
    ///
    /// Default: 100µs
    pub fn with_write_buffer_linger(mut self, duration: Option<Duration>) -> Self {
        self.write_buffer_linger = duration;
        self
    }
}

impl Default for PubOptions {
    fn default() -> Self {
        Self {
            max_clients: None,
            high_water_mark: DEFAULT_HWM,
            min_compress_size: 8192,
            write_buffer_size: 8192,
            write_buffer_linger: Some(Duration::from_micros(100)),
        }
    }
}

/// A message received from a publisher.
/// Includes the source, topic, and payload.
#[derive(Debug, Clone)]
pub struct PubMessage {
    /// The compression type used for the message payload.
    compression_type: CompressionType,
    /// The topic of the message.
    topic: String,
    /// The message payload.
    payload: Bytes,
}

#[allow(unused)]
impl PubMessage {
    pub fn new(topic: String, payload: Bytes) -> Self {
        Self {
            // Initialize the compression type to None.
            // The actual compression type will be set in the `compress` method.
            compression_type: CompressionType::None,
            topic,
            payload,
        }
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
        pubsub::Message::new(
            seq,
            Bytes::from(self.topic),
            self.payload,
            self.compression_type as u8,
        )
    }

    #[inline]
    pub fn compress(&mut self, compressor: &dyn Compressor) -> Result<(), io::Error> {
        self.payload = compressor.compress(&self.payload)?;
        self.compression_type = compressor.compression_type();

        Ok(())
    }
}

/// The publisher socket state, shared between the backend task and the socket.
#[derive(Debug, Default)]
pub(crate) struct SocketState {
    pub(crate) stats: SocketStats<PubStats>,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::StreamExt;
    use msg_transport::{quic::Quic, tcp::Tcp};
    use msg_wire::compression::GzipCompressor;
    use tracing::info;

    use crate::{Authenticator, SubOptions, SubSocket};

    use super::*;

    struct Auth;

    impl Authenticator for Auth {
        fn authenticate(&self, id: &Bytes) -> bool {
            info!("Auth request from: {:?}", id);
            true
        }
    }

    #[tokio::test]
    async fn pubsub_simple() {
        let _ = tracing_subscriber::fmt::try_init();

        let mut pub_socket = PubSocket::new(Tcp::default());

        let mut sub_socket = SubSocket::with_options(Tcp::default(), SubOptions::default());

        pub_socket.bind("0.0.0.0:0").await.unwrap();
        let addr = pub_socket.local_addr().unwrap();

        sub_socket.connect(addr).await.unwrap();
        sub_socket.subscribe("HELLO".to_string()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        pub_socket.publish("HELLO".to_string(), "WORLD".into()).await.unwrap();

        let msg = sub_socket.next().await.unwrap();
        info!("Received message: {:?}", msg);
        assert_eq!("HELLO", msg.topic());
        assert_eq!("WORLD", msg.payload());
    }

    #[tokio::test]
    async fn pubsub_auth_tcp() {
        let _ = tracing_subscriber::fmt::try_init();

        let mut pub_socket = PubSocket::new(Tcp::default()).with_auth(Auth);

        let mut sub_socket = SubSocket::with_options(
            Tcp::default(),
            SubOptions::default().with_auth_token(Bytes::from("client1")),
        );

        pub_socket.bind("0.0.0.0:0").await.unwrap();
        let addr = pub_socket.local_addr().unwrap();

        sub_socket.connect(addr).await.unwrap();
        sub_socket.subscribe("HELLO".to_string()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        pub_socket.publish("HELLO".to_string(), "WORLD".into()).await.unwrap();

        let msg = sub_socket.next().await.unwrap();
        info!("Received message: {:?}", msg);
        assert_eq!("HELLO", msg.topic());
        assert_eq!("WORLD", msg.payload());
    }

    #[tokio::test]
    async fn pubsub_auth_quic() {
        let _ = tracing_subscriber::fmt::try_init();

        let mut pub_socket = PubSocket::new(Quic::default()).with_auth(Auth);

        let mut sub_socket = SubSocket::with_options(
            Quic::default(),
            SubOptions::default().with_auth_token(Bytes::from("client1")),
        );

        pub_socket.bind("0.0.0.0:0").await.unwrap();
        let addr = pub_socket.local_addr().unwrap();

        sub_socket.connect(addr).await.unwrap();
        sub_socket.subscribe("HELLO".to_string()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        pub_socket.publish("HELLO".to_string(), "WORLD".into()).await.unwrap();

        let msg = sub_socket.next().await.unwrap();
        info!("Received message: {:?}", msg);
        assert_eq!("HELLO", msg.topic());
        assert_eq!("WORLD", msg.payload());
    }

    #[tokio::test]
    async fn pubsub_many() {
        let _ = tracing_subscriber::fmt::try_init();

        let mut pub_socket = PubSocket::new(Tcp::default());

        let mut sub1 = SubSocket::new(Tcp::default());

        let mut sub2 = SubSocket::new(Tcp::default());

        pub_socket.bind("0.0.0.0:0").await.unwrap();
        let addr = pub_socket.local_addr().unwrap();

        sub1.connect(addr).await.unwrap();
        sub2.connect(addr).await.unwrap();
        sub1.subscribe("HELLO".to_string()).await.unwrap();
        sub2.subscribe("HELLO".to_string()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        pub_socket.publish("HELLO".to_string(), Bytes::from("WORLD")).await.unwrap();

        let msg = sub1.next().await.unwrap();
        info!("Received message: {:?}", msg);
        assert_eq!("HELLO", msg.topic());
        assert_eq!("WORLD", msg.payload());

        let msg = sub2.next().await.unwrap();
        info!("Received message: {:?}", msg);
        assert_eq!("HELLO", msg.topic());
        assert_eq!("WORLD", msg.payload());
    }

    #[tokio::test]
    async fn pubsub_many_compressed() {
        let _ = tracing_subscriber::fmt::try_init();

        let mut pub_socket = PubSocket::new(Tcp::default()).with_compressor(GzipCompressor::new(6));

        let mut sub1 = SubSocket::new(Tcp::default());

        let mut sub2 = SubSocket::new(Tcp::default());

        pub_socket.bind("0.0.0.0:0").await.unwrap();
        let addr = pub_socket.local_addr().unwrap();

        sub1.connect(addr).await.unwrap();
        sub2.connect(addr).await.unwrap();
        sub1.subscribe("HELLO".to_string()).await.unwrap();
        sub2.subscribe("HELLO".to_string()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let original_msg = Bytes::from(
            "WOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOORLD",
        );

        pub_socket.publish("HELLO".to_string(), original_msg.clone()).await.unwrap();

        let msg = sub1.next().await.unwrap();
        info!("Received message: {:?}", msg);
        assert_eq!("HELLO", msg.topic());
        assert_eq!(original_msg, msg.payload());

        let msg = sub2.next().await.unwrap();
        info!("Received message: {:?}", msg);
        assert_eq!("HELLO", msg.topic());
        assert_eq!(original_msg, msg.payload());
    }

    #[tokio::test]
    async fn pubsub_durable_tcp() {
        let _ = tracing_subscriber::fmt::try_init();

        let mut pub_socket = PubSocket::new(Tcp::default());

        let mut sub_socket = SubSocket::new(Tcp::default());

        // Try to connect and subscribe before the publisher is up
        sub_socket.connect("0.0.0.0:6662").await.unwrap();
        sub_socket.subscribe("HELLO".to_string()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;

        pub_socket.bind("0.0.0.0:6662").await.unwrap();
        tokio::time::sleep(Duration::from_millis(2000)).await;

        pub_socket.publish("HELLO".to_string(), Bytes::from("WORLD")).await.unwrap();

        let msg = sub_socket.next().await.unwrap();
        info!("Received message: {:?}", msg);
        assert_eq!("HELLO", msg.topic());
        assert_eq!("WORLD", msg.payload());
    }

    #[tokio::test]
    async fn pubsub_durable_quic() {
        let _ = tracing_subscriber::fmt::try_init();

        let mut pub_socket = PubSocket::new(Quic::default());

        let mut sub_socket = SubSocket::new(Quic::default());

        // Try to connect and subscribe before the publisher is up
        sub_socket.connect("0.0.0.0:6662").await.unwrap();
        sub_socket.subscribe("HELLO".to_string()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1000)).await;

        pub_socket.bind("0.0.0.0:6662").await.unwrap();
        tokio::time::sleep(Duration::from_millis(2000)).await;

        pub_socket.publish("HELLO".to_string(), Bytes::from("WORLD")).await.unwrap();

        let msg = sub_socket.next().await.unwrap();
        info!("Received message: {:?}", msg);
        assert_eq!("HELLO", msg.topic());
        assert_eq!("WORLD", msg.payload());
    }

    #[tokio::test]
    async fn pubsub_max_clients() {
        let _ = tracing_subscriber::fmt::try_init();

        let mut pub_socket =
            PubSocket::with_options(Tcp::default(), PubOptions::default().with_max_clients(1));

        pub_socket.bind("0.0.0.0:0").await.unwrap();

        let mut sub1 = SubSocket::with_options(Tcp::default(), SubOptions::default());

        let mut sub2 = SubSocket::with_options(Tcp::default(), SubOptions::default());

        let addr = pub_socket.local_addr().unwrap();

        sub1.connect(addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(pub_socket.stats().active_clients(), 1);
        sub2.connect(addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(pub_socket.stats().active_clients(), 1);
    }
}
