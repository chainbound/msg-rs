use bytes::Bytes;
use core::fmt;
use msg_wire::pubsub;
use std::net::SocketAddr;
use thiserror::Error;

mod driver;
mod socket;
mod stats;
mod stream;

use driver::SubDriver;
pub use socket::*;
use stats::SocketStats;

const DEFAULT_BUFFER_SIZE: usize = 1024;

#[derive(Debug, Error)]
pub enum SubError {
    #[error("IO error: {0:?}")]
    Io(#[from] std::io::Error),
    #[error("Authentication error: {0:?}")]
    Auth(String),
    #[error("Wire protocol error: {0:?}")]
    Wire(#[from] pubsub::Error),
    #[error("Socket closed")]
    SocketClosed,
    #[error("Command channel full")]
    ChannelFull,
    #[error("Transport error: {0:?}")]
    Transport(#[from] Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug)]
enum Command {
    /// Subscribe to a topic.
    Subscribe { topic: String },
    /// Unsubscribe from a topic.
    Unsubscribe { topic: String },
    /// Connect to a publisher socket.
    Connect { endpoint: SocketAddr },
    /// Disconnect from a publisher socket.
    Disconnect { endpoint: SocketAddr },
    /// Shut down the driver.
    Shutdown,
}

#[derive(Debug, Clone)]
pub struct SubOptions<T: Clone> {
    /// The maximum amount of incoming messages that will be buffered before being dropped due to
    /// a slow consumer.
    ingress_buffer_size: usize,
    /// The read buffer size for each session.
    read_buffer_size: usize,
    connect_options: T,
}

impl<T: Clone> SubOptions<T> {
    /// Sets the ingress buffer size. This is the maximum amount of incoming messages that will be buffered.
    /// If the consumer cannot keep up with the incoming messages, messages will start being dropped.
    pub fn ingress_buffer_size(mut self, ingress_buffer_size: usize) -> Self {
        self.ingress_buffer_size = ingress_buffer_size;
        self
    }

    /// Sets the read buffer size. This sets the size of the read buffer for each session.
    pub fn read_buffer_size(mut self, read_buffer_size: usize) -> Self {
        self.read_buffer_size = read_buffer_size;
        self
    }

    /// Sets the connect options for the underlying transport.
    pub fn connect_options(mut self, connect_options: T) -> Self {
        self.connect_options = connect_options;
        self
    }
}

impl<T: Default + Clone> Default for SubOptions<T> {
    fn default() -> Self {
        Self {
            ingress_buffer_size: DEFAULT_BUFFER_SIZE,
            read_buffer_size: 8192,
            connect_options: T::default(),
        }
    }
}

/// A message received from a publisher.
/// Includes the source, topic, and payload.
#[derive(Clone)]
pub struct PubMessage {
    /// The source address of the publisher. We need this because
    /// a subscriber can connect to multiple publishers.
    source: SocketAddr,
    /// The topic of the message.
    topic: String,
    /// The message payload.
    payload: Bytes,
}

impl fmt::Debug for PubMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PubMessage")
            .field("source", &self.source)
            .field("topic", &self.topic)
            .field("payload_size", &self.payload.len())
            .finish()
    }
}

impl PubMessage {
    pub fn new(source: SocketAddr, topic: String, payload: Bytes) -> Self {
        Self {
            source,
            topic,
            payload,
        }
    }

    #[inline]
    pub fn source(&self) -> SocketAddr {
        self.source
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
}

/// The request socket state, shared between the backend task and the socket.
#[derive(Debug, Default)]
pub(crate) struct SocketState {
    pub(crate) stats: SocketStats,
}

#[cfg(test)]
mod tests {
    use msg_transport::Tcp;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    use tokio_stream::StreamExt;
    use tracing::Instrument;

    use super::*;

    async fn spawn_listener() -> SocketAddr {
        let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();

        let addr = listener.local_addr().unwrap();

        tokio::spawn(
            async move {
                let (mut socket, _) = listener.accept().await.unwrap();

                let mut buf = [0u8; 1024];
                let b = socket.read(&mut buf).await.unwrap();
                let read = &buf[..b];

                tracing::info!("Received bytes: {:?}", read);
                socket.write_all(read).await.unwrap();
                socket.flush().await.unwrap();
            }
            .instrument(tracing::info_span!("listener")),
        );

        addr
    }

    #[tokio::test]
    async fn test_sub() {
        let _ = tracing_subscriber::fmt::try_init();
        let mut socket = socket::SubSocket::<Tcp>::new();

        let addr = spawn_listener().await;
        socket.connect(addr).await.unwrap();
        socket.subscribe("HELLO".to_string()).await.unwrap();

        let mirror = socket.next().await.unwrap();
        assert_eq!("MSG.SUB.HELLO", mirror.topic);
    }
}
