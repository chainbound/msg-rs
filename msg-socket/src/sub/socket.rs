use futures::Stream;
use rustc_hash::FxHashMap;
use std::{
    collections::HashSet,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::{
    net::{lookup_host, ToSocketAddrs},
    sync::mpsc,
};

use msg_common::task::JoinMap;
use msg_transport::Transport;

use super::{
    Command, PubMessage, SocketState, SocketStats, SubDriver, SubError, SubOptions,
    DEFAULT_BUFFER_SIZE,
};

pub struct SubSocket<T: Transport> {
    /// Command channel to the socket driver.
    to_driver: mpsc::Sender<Command>,
    /// Receiver channel from the socket driver.
    from_driver: mpsc::Receiver<PubMessage>,
    /// Options for the socket. These are shared with the backend task.
    #[allow(unused)]
    options: Arc<SubOptions>,
    /// The pending driver.
    driver: Option<SubDriver<T>>,
    /// Socket state. This is shared with the socket frontend.
    state: Arc<SocketState>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> SubSocket<T>
where
    T: Transport + Send + Sync + Unpin + 'static,
{
    #[allow(clippy::new_without_default)]
    pub fn new(transport: T) -> Self {
        Self::with_options(transport, SubOptions::default())
    }

    pub fn with_options(transport: T, options: SubOptions) -> Self {
        let (to_driver, from_socket) = mpsc::channel(DEFAULT_BUFFER_SIZE);
        let (to_socket, from_driver) = mpsc::channel(options.ingress_buffer_size);

        let options = Arc::new(options);

        let state = Arc::new(SocketState::default());

        let mut publishers = FxHashMap::default();
        publishers.reserve(32);

        let driver = SubDriver {
            options: Arc::clone(&options),
            transport,
            from_socket,
            to_socket,
            connection_tasks: JoinMap::new(),
            publishers,
            subscribed_topics: HashSet::with_capacity(32),
            state: Arc::clone(&state),
        };

        Self {
            to_driver,
            from_driver,
            driver: Some(driver),
            options,
            state,
            _marker: std::marker::PhantomData,
        }
    }

    /// Asynchronously connects to the endpoint.
    pub async fn connect<A: ToSocketAddrs>(&mut self, addr: A) -> Result<(), SubError> {
        self.ensure_active_driver();
        let mut addrs = lookup_host(addr).await?;
        let endpoint = addrs.next().ok_or(SubError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "could not find any valid address",
        )))?;

        self.send_command(Command::Connect { endpoint }).await?;

        Ok(())
    }

    /// Immediately send a connect command to the driver.
    pub fn try_connect(&mut self, endpoint: &str) -> Result<(), SubError> {
        self.ensure_active_driver();
        let endpoint: SocketAddr = endpoint.parse().map_err(|_| {
            SubError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "could not find any valid address",
            ))
        })?;

        self.try_send_command(Command::Connect { endpoint })?;

        Ok(())
    }

    /// Asynchronously disconnects from the endpoint.
    pub async fn disconnect(&mut self, endpoint: &str) -> Result<(), SubError> {
        self.ensure_active_driver();
        let endpoint: SocketAddr = endpoint.parse().map_err(|_| {
            SubError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "could not find any valid address",
            ))
        })?;

        self.send_command(Command::Disconnect { endpoint }).await?;

        Ok(())
    }

    /// Immediately send a disconnect command to the driver.
    pub fn try_disconnect(&mut self, endpoint: &str) -> Result<(), SubError> {
        self.ensure_active_driver();
        let endpoint: SocketAddr = endpoint.parse().map_err(|_| {
            SubError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "could not find any valid address",
            ))
        })?;

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

    pub fn stats(&self) -> &SocketStats {
        &self.state.stats
    }
}

impl<T: Transport> Drop for SubSocket<T> {
    fn drop(&mut self) {
        // Try to tell the driver to gracefully shut down.
        let _ = self.to_driver.try_send(Command::Shutdown);
    }
}

impl<T: Transport + Unpin> Stream for SubSocket<T> {
    type Item = PubMessage;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.from_driver.poll_recv(cx)
    }
}
