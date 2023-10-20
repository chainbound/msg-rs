use bytes::Bytes;
use futures::Stream;
use msg_wire::pubsub;
use std::{
    collections::HashSet,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use thiserror::Error;
use tokio::{sync::mpsc, task::JoinSet};
use tokio_stream::StreamMap;

use msg_transport::ClientTransport;

use self::driver::SubDriver;

mod driver;
mod stream;

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
pub struct SubOptions {
    pub auth_token: Option<Bytes>,
    pub timeout: std::time::Duration,
    pub ingress_buffer_size: usize,
}

impl SubOptions {
    /// Sets the authentication token for the socket.
    pub fn with_token(mut self, auth_token: Bytes) -> Self {
        self.auth_token = Some(auth_token);
        self
    }
}

impl Default for SubOptions {
    fn default() -> Self {
        Self {
            ingress_buffer_size: DEFAULT_BUFFER_SIZE,
            auth_token: None,
            timeout: std::time::Duration::from_secs(5),
        }
    }
}

/// A message received from a publisher.
/// Includes the source, topic, and payload.
#[derive(Debug, Clone)]
pub struct PubMessage {
    /// The source address of the publisher. We need this because
    /// a subscriber can connect to multiple publishers.
    source: SocketAddr,
    /// The topic of the message.
    topic: String,
    /// The message payload.
    payload: Bytes,
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

pub struct SubSocket<T: ClientTransport> {
    /// Command channel to the socket driver.
    to_driver: mpsc::Sender<Command>,
    /// Receiver channel from the socket driver.
    from_driver: mpsc::Receiver<PubMessage>,
    /// Options for the socket. These are shared with the backend task.
    #[allow(unused)]
    options: Arc<SubOptions>,
    /// The pending driver.
    driver: Option<SubDriver<T>>,
    // / Socket state. This is shared with the backend task.
    // state: Arc<SocketState>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> SubSocket<T>
where
    T: ClientTransport + Send + Sync + 'static,
{
    pub fn new(transport: T) -> Self {
        Self::with_options(transport, SubOptions::default())
    }

    pub fn with_options(transport: T, options: SubOptions) -> Self {
        let (to_driver, from_socket) = mpsc::channel(DEFAULT_BUFFER_SIZE);
        let (to_socket, from_driver) = mpsc::channel(options.ingress_buffer_size);

        let options = Arc::new(options);

        let driver = SubDriver {
            options: Arc::clone(&options),
            transport: Arc::new(transport),
            from_socket,
            to_socket,
            connection_tasks: JoinSet::new(),
            publishers: StreamMap::with_capacity(24),
            subscribed_topics: HashSet::with_capacity(32),
        };

        Self {
            to_driver,
            from_driver,
            driver: Some(driver),
            options,
            _marker: std::marker::PhantomData,
        }
    }

    /// Asynchronously connects to the endpoint.
    pub async fn connect(&mut self, endpoint: &str) -> Result<(), SubError> {
        self.ensure_active_driver();
        let endpoint: SocketAddr = endpoint.parse().unwrap();

        self.send_command(Command::Connect { endpoint }).await?;

        Ok(())
    }

    /// Immediately send a connect command to the driver.
    pub fn try_connect(&mut self, endpoint: &str) -> Result<(), SubError> {
        self.ensure_active_driver();
        let endpoint: SocketAddr = endpoint.parse().unwrap();

        self.try_send_command(Command::Connect { endpoint })?;

        Ok(())
    }

    /// Asynchronously disconnects from the endpoint.
    pub async fn disconnect(&mut self, endpoint: &str) -> Result<(), SubError> {
        self.ensure_active_driver();
        let endpoint: SocketAddr = endpoint.parse().unwrap();

        self.send_command(Command::Disconnect { endpoint }).await?;

        Ok(())
    }

    /// Immediately send a disconnect command to the driver.
    pub fn try_disconnect(&mut self, endpoint: &str) -> Result<(), SubError> {
        self.ensure_active_driver();
        let endpoint: SocketAddr = endpoint.parse().unwrap();

        self.try_send_command(Command::Disconnect { endpoint })?;

        Ok(())
    }

    /// Subscribes to the given topic. This will subscribe to all connected publishers.
    /// If the topic does not exist on a publisher, this will not return any data.
    /// Any publishers that are connected after this call will also be subscribed to.
    pub async fn subscribe(&mut self, topic: String) -> Result<(), SubError> {
        self.ensure_active_driver();
        assert!(!topic.starts_with("MSG"), "MSG is a reserved topic");
        self.send_command(Command::Subscribe { topic }).await?;

        Ok(())
    }

    /// Immediately send a subscribe command to the driver.
    pub fn try_subscribe(&mut self, topic: String) -> Result<(), SubError> {
        self.ensure_active_driver();
        assert!(!topic.starts_with("MSG"), "MSG is a reserved topic");
        self.try_send_command(Command::Subscribe { topic })?;

        Ok(())
    }

    /// Unsubscribe from the given topic. This will unsubscribe from all connected publishers.
    pub async fn unsubscribe(&mut self, topic: String) -> Result<(), SubError> {
        self.ensure_active_driver();
        self.send_command(Command::Unsubscribe { topic }).await?;

        Ok(())
    }

    /// Immediately send an unsubscribe command to the driver.
    pub fn try_unsubscribe(&mut self, topic: String) -> Result<(), SubError> {
        self.ensure_active_driver();
        self.try_send_command(Command::Unsubscribe { topic })?;

        Ok(())
    }

    /// Sends a command to the driver, returning [`SubError::SocketClosed`] if the
    /// driver has been dropped.
    async fn send_command(&self, command: Command) -> Result<(), SubError> {
        self.to_driver
            .send(command)
            .await
            .map_err(|_| SubError::SocketClosed)?;

        Ok(())
    }

    fn try_send_command(&self, command: Command) -> Result<(), SubError> {
        use mpsc::error::TrySendError::*;
        self.to_driver.try_send(command).map_err(|e| match e {
            Full(_) => SubError::ChannelFull,
            Closed(_) => SubError::SocketClosed,
        })?;
        Ok(())
    }

    /// Ensures that the driver task is running. This function will be called on every command,
    /// which might be overkill, but it keeps the interface simple and is not in the hot path.
    fn ensure_active_driver(&mut self) {
        if let Some(driver) = self.driver.take() {
            tokio::spawn(driver);
        }
    }
}

impl<T: ClientTransport> Drop for SubSocket<T> {
    fn drop(&mut self) {
        // Try to tell the driver to gracefully shut down.
        let _ = self.to_driver.try_send(Command::Shutdown);
    }
}

impl<T: ClientTransport + Unpin> Stream for SubSocket<T> {
    type Item = PubMessage;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.from_driver.poll_recv(cx)
    }
}

// The request socket state, shared between the backend task and the socket.
// #[derive(Debug, Default)]
// pub(crate) struct SocketState {
//     pub(crate) stats: SocketStats,
// }

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
        let mut socket = SubSocket::new(Tcp::new());

        let addr = spawn_listener().await;
        socket.connect(&addr.to_string()).await.unwrap();
        socket.subscribe("HELLO".to_string()).await.unwrap();

        let mirror = socket.next().await.unwrap();
        assert_eq!("MSG.SUB.HELLO", mirror.topic);
    }
}
