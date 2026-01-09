use std::{fmt, time::Duration};

use bytes::Bytes;
use thiserror::Error;

mod driver;
use driver::SubDriver;

mod session;

mod socket;
pub use socket::*;

mod stats;

use crate::stats::SocketStats;
use stats::SubStats;

mod stream;

use msg_transport::Address;
use msg_wire::pubsub;

use crate::DEFAULT_BUFFER_SIZE;

#[derive(Debug, Error)]
pub enum SubError {
    #[error("IO error: {0:?}")]
    Io(#[from] std::io::Error),
    #[error("Authentication error: {0}")]
    Auth(String),
    #[error("Wire protocol error: {0:?}")]
    Wire(#[from] pubsub::Error),
    #[error("Socket closed")]
    SocketClosed,
    #[error("Command channel full")]
    ChannelFull,
    #[error("Could not find any valid endpoints")]
    NoValidEndpoints,
    #[error("Reserved topic 'MSG' cannot be used")]
    ReservedTopic,
}

#[derive(Debug)]
enum Command<A: Address> {
    /// Subscribe to a topic.
    Subscribe { topic: String },
    /// Unsubscribe from a topic.
    Unsubscribe { topic: String },
    /// Connect to a publisher socket.
    Connect { endpoint: A },
    /// Disconnect from a publisher socket.
    Disconnect { endpoint: A },
    /// Shut down the driver.
    Shutdown,
}

#[derive(Debug, Clone)]
pub struct SubOptions {
    /// Optional authentication token.
    auth_token: Option<Bytes>,
    /// The maximum amount of incoming messages that will be buffered before being dropped due to
    /// a slow consumer.
    ingress_buffer_size: usize,
    /// The read buffer size for each session.
    read_buffer_size: usize,
    /// The initial backoff for reconnecting to a publisher.
    initial_backoff: Duration,
    /// The maximum number of retry attempts. If `None`, the connection will retry indefinitely.
    retry_attempts: Option<usize>,
}

impl SubOptions {
    /// Sets the authentication token for this socket. This will activate the authentication layer
    /// and send the token to the publisher.
    pub fn with_auth_token(mut self, auth_token: Bytes) -> Self {
        self.auth_token = Some(auth_token);
        self
    }

    /// Sets the ingress buffer size. This is the maximum amount of incoming messages that will be
    /// buffered. If the consumer cannot keep up with the incoming messages, messages will start
    /// being dropped.
    pub fn with_ingress_buffer_size(mut self, ingress_buffer_size: usize) -> Self {
        self.ingress_buffer_size = ingress_buffer_size;
        self
    }

    /// Sets the read buffer size. This sets the size of the read buffer for each session.
    pub fn with_read_buffer_size(mut self, read_buffer_size: usize) -> Self {
        self.read_buffer_size = read_buffer_size;
        self
    }

    /// Set the initial backoff for reconnecting to a publisher.
    pub fn with_initial_backoff(mut self, initial_backoff: Duration) -> Self {
        self.initial_backoff = initial_backoff;
        self
    }

    /// Sets the maximum number of retry attempts. If `None`, the connection will retry
    /// indefinitely.
    pub fn with_retry_attempts(mut self, retry_attempts: usize) -> Self {
        self.retry_attempts = Some(retry_attempts);
        self
    }
}

impl Default for SubOptions {
    fn default() -> Self {
        Self {
            auth_token: None,
            ingress_buffer_size: DEFAULT_BUFFER_SIZE,
            read_buffer_size: 8192,
            initial_backoff: Duration::from_millis(100),
            retry_attempts: Some(24),
        }
    }
}

/// A message received from a publisher.
/// Includes the source, topic, and payload.
#[derive(Clone)]
pub struct PubMessage<A: Address> {
    /// The source address of the publisher. We need this because
    /// a subscriber can connect to multiple publishers.
    source: A,
    /// The topic of the message.
    topic: String,
    /// The message payload.
    payload: Bytes,
}

impl<A: Address> fmt::Debug for PubMessage<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PubMessage")
            .field("source", &self.source)
            .field("topic", &self.topic)
            .field("payload_size", &self.payload.len())
            .finish()
    }
}

impl<A: Address> PubMessage<A> {
    pub fn new(source: A, topic: String, payload: Bytes) -> Self {
        Self { source, topic, payload }
    }

    #[inline]
    pub fn source(&self) -> &A {
        &self.source
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

/// The subscriber socket state, shared between the backend task and the socket frontend.
#[derive(Debug)]
pub(crate) struct SocketState<A: Address> {
    pub(crate) stats: SocketStats<SubStats<A>>,
}

impl<A: Address> Default for SocketState<A> {
    fn default() -> Self {
        Self { stats: SocketStats::default() }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use msg_transport::tcp::Tcp;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    use tokio_stream::StreamExt;
    use tracing::{Instrument, info, info_span};

    use super::*;

    async fn spawn_listener() -> SocketAddr {
        let listener = TcpListener::bind("[::]:0").await.unwrap();

        let addr = listener.local_addr().unwrap();

        tokio::spawn(
            async move {
                let (mut socket, _) = listener.accept().await.unwrap();

                let mut buf = [0u8; 1024];
                let b = socket.read(&mut buf).await.unwrap();
                let read = &buf[..b];

                info!("Received bytes: {:?}", read);
                socket.write_all(read).await.unwrap();
                socket.flush().await.unwrap();
            }
            .instrument(info_span!("listener")),
        );

        addr
    }

    #[tokio::test]
    async fn test_sub() {
        let _ = tracing_subscriber::fmt::try_init();
        let mut socket = socket::SubSocket::new(Tcp::default());

        let addr = spawn_listener().await;
        socket.connect(addr).await.unwrap();
        socket.subscribe("HELLO".to_string()).await.unwrap();

        let mirror = socket.next().await.unwrap();
        assert_eq!("MSG.SUB.HELLO", mirror.topic);
    }
}
