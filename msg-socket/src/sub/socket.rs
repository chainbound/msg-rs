use std::{
    collections::HashSet,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::Stream;
use rustc_hash::FxHashMap;
use tokio::{
    net::{lookup_host, ToSocketAddrs},
    sync::mpsc,
};

use msg_common::JoinMap;
use msg_transport::{Address, Transport};

use super::{
    Command, PubMessage, SocketState, SocketStats, SubDriver, SubError, SubOptions,
    DEFAULT_BUFFER_SIZE,
};

/// A subscriber socket. This socket implements [`Stream`] and yields incoming [`PubMessage`]s.
pub struct SubSocket<T: Transport<A>, A: Address> {
    /// Command channel to the socket driver.
    to_driver: mpsc::Sender<Command<A>>,
    /// Receiver channel from the socket driver.
    from_driver: mpsc::Receiver<PubMessage<A>>,
    /// Options for the socket. These are shared with the backend task.
    #[allow(unused)]
    options: Arc<SubOptions>,
    /// The pending driver.
    driver: Option<SubDriver<T, A>>,
    /// Socket state. This is shared with the socket frontend.
    state: Arc<SocketState<A>>,
    /// Marker for the transport type.
    _marker: std::marker::PhantomData<T>,
}

impl<T> SubSocket<T, SocketAddr>
where
    T: Transport<SocketAddr> + Send + Sync + Unpin + 'static,
{
    /// Connects to the given endpoint asynchronously.
    pub async fn connect(&mut self, endpoint: impl ToSocketAddrs) -> Result<(), SubError> {
        let mut addrs = lookup_host(endpoint).await?;
        let mut endpoint = addrs.next().ok_or(SubError::NoValidEndpoints)?;

        // Some transport implementations (e.g. Quinn) can't dial an unspecified
        // IP address, so replace it with localhost.
        if endpoint.ip().is_unspecified() {
            // TODO: support IPv6
            endpoint.set_ip(IpAddr::V4(Ipv4Addr::LOCALHOST));
        }

        self.connect_inner(endpoint).await
    }

    /// Attempts to connect to the given endpoint immediately.
    pub fn try_connect(&mut self, endpoint: impl Into<String>) -> Result<(), SubError> {
        let addr = endpoint.into();
        let mut endpoint: SocketAddr = addr.parse().map_err(|_| SubError::NoValidEndpoints)?;

        // Some transport implementations (e.g. Quinn) can't dial an unspecified
        // IP address, so replace it with localhost.
        if endpoint.ip().is_unspecified() {
            // TODO: support IPv6
            endpoint.set_ip(IpAddr::V4(Ipv4Addr::LOCALHOST));
        }

        self.try_connect_inner(endpoint)
    }

    /// Disconnects from the given endpoint asynchronously.
    pub async fn disconnect(&mut self, endpoint: impl ToSocketAddrs) -> Result<(), SubError> {
        let mut addrs = lookup_host(endpoint).await?;
        let endpoint = addrs.next().ok_or(SubError::NoValidEndpoints)?;

        self.disconnect_inner(endpoint).await
    }

    /// Attempts to disconnect from the given endpoint immediately.
    pub fn try_disconnect(&mut self, endpoint: impl Into<String>) -> Result<(), SubError> {
        let endpoint = endpoint.into();
        let endpoint: SocketAddr = endpoint.parse().map_err(|_| SubError::NoValidEndpoints)?;

        self.try_disconnect_inner(endpoint)
    }
}

impl<T> SubSocket<T, PathBuf>
where
    T: Transport<PathBuf> + Send + Sync + Unpin + 'static,
{
    /// Connects to the given path asynchronously.
    pub async fn connect_path(&mut self, path: impl Into<PathBuf>) -> Result<(), SubError> {
        self.connect_inner(path.into()).await
    }

    /// Attempts to connect to the given path immediately.
    pub fn try_connect_path(&mut self, path: impl Into<PathBuf>) -> Result<(), SubError> {
        self.try_connect_inner(path.into())
    }

    /// Disconnects from the given path asynchronously.
    pub async fn disconnect_path(&mut self, path: impl Into<PathBuf>) -> Result<(), SubError> {
        self.disconnect_inner(path.into()).await
    }

    /// Attempts to disconnect from the given path immediately.
    pub fn try_disconnect_path(&mut self, path: impl Into<PathBuf>) -> Result<(), SubError> {
        self.try_disconnect_inner(path.into())
    }
}

impl<T, A> SubSocket<T, A>
where
    T: Transport<A> + Send + Sync + Unpin + 'static,
    A: Address,
{
    /// Creates a new subscriber socket with the default [`SubOptions`].
    pub fn new(transport: T) -> Self {
        Self::with_options(transport, SubOptions::default())
    }

    /// Creates a new subscriber socket with the given transport and options.
    pub fn with_options(transport: T, options: SubOptions) -> Self {
        let (to_driver, from_socket) = mpsc::channel(DEFAULT_BUFFER_SIZE);
        let (to_socket, from_driver) = mpsc::channel(options.ingress_buffer_size);

        let options = Arc::new(options);

        let state = Arc::new(SocketState::new());

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
    pub async fn connect_inner(&mut self, endpoint: A) -> Result<(), SubError> {
        self.ensure_active_driver();
        self.send_command(Command::Connect { endpoint }).await?;
        Ok(())
    }

    /// Immediately send a connect command to the driver.
    pub fn try_connect_inner(&mut self, endpoint: A) -> Result<(), SubError> {
        self.ensure_active_driver();
        self.try_send_command(Command::Connect { endpoint })?;
        Ok(())
    }

    /// Asynchronously disconnects from the endpoint.
    pub async fn disconnect_inner(&mut self, endpoint: A) -> Result<(), SubError> {
        self.ensure_active_driver();
        self.send_command(Command::Disconnect { endpoint }).await?;
        Ok(())
    }

    /// Immediately send a disconnect command to the driver.
    pub fn try_disconnect_inner(&mut self, endpoint: A) -> Result<(), SubError> {
        self.ensure_active_driver();
        self.try_send_command(Command::Disconnect { endpoint })?;
        Ok(())
    }

    /// Subscribes to the given topic. This will subscribe to all connected publishers.
    /// If the topic does not exist on a publisher, this will not return any data.
    /// Any publishers that are connected after this call will also be subscribed to.
    pub async fn subscribe(&mut self, topic: impl Into<String>) -> Result<(), SubError> {
        self.ensure_active_driver();

        let topic = topic.into();
        if topic.starts_with("MSG") {
            return Err(SubError::ReservedTopic);
        }

        self.send_command(Command::Subscribe { topic }).await?;

        Ok(())
    }

    /// Immediately send a subscribe command to the driver.
    pub fn try_subscribe(&mut self, topic: impl Into<String>) -> Result<(), SubError> {
        self.ensure_active_driver();

        let topic = topic.into();
        if topic.starts_with("MSG") {
            return Err(SubError::ReservedTopic);
        }

        self.try_send_command(Command::Subscribe { topic })?;

        Ok(())
    }

    /// Unsubscribe from the given topic. This will unsubscribe from all connected publishers.
    pub async fn unsubscribe(&mut self, topic: impl Into<String>) -> Result<(), SubError> {
        self.ensure_active_driver();

        let topic = topic.into();
        if topic.starts_with("MSG") {
            return Err(SubError::ReservedTopic);
        }

        self.send_command(Command::Unsubscribe { topic }).await?;

        Ok(())
    }

    /// Immediately send an unsubscribe command to the driver.
    pub fn try_unsubscribe(&mut self, topic: impl Into<String>) -> Result<(), SubError> {
        self.ensure_active_driver();

        let topic = topic.into();
        if topic.starts_with("MSG") {
            return Err(SubError::ReservedTopic);
        }

        self.try_send_command(Command::Unsubscribe { topic })?;

        Ok(())
    }

    /// Sends a command to the driver, returning [`SubError::SocketClosed`] if the
    /// driver has been dropped.
    async fn send_command(&self, command: Command<A>) -> Result<(), SubError> {
        self.to_driver.send(command).await.map_err(|_| SubError::SocketClosed)?;

        Ok(())
    }

    fn try_send_command(&self, command: Command<A>) -> Result<(), SubError> {
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

    pub fn stats(&self) -> &SocketStats<A> {
        &self.state.stats
    }
}

impl<T: Transport<A>, A: Address> Drop for SubSocket<T, A> {
    fn drop(&mut self) {
        // Try to tell the driver to gracefully shut down.
        let _ = self.to_driver.try_send(Command::Shutdown);
    }
}

impl<T: Transport<A> + Unpin, A: Address> Stream for SubSocket<T, A> {
    type Item = PubMessage<A>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.from_driver.poll_recv(cx)
    }
}
