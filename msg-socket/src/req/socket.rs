use bytes::Bytes;
use rustc_hash::FxHashMap;
use std::{marker::PhantomData, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    net::{lookup_host, ToSocketAddrs},
    sync::{mpsc, oneshot},
};

use msg_transport::{Address, Transport};
use msg_wire::compression::Compressor;

use super::{Command, ReqError, ReqOptions, DEFAULT_BUFFER_SIZE};
use crate::{
    req::{driver::ReqDriver, stats::ReqStats, SocketState},
    ConnectionState, ExponentialBackoff, ReqMessage,
};

/// The request socket.
pub struct ReqSocket<T: Transport<A>, A: Address> {
    /// Command channel to the backend task.
    to_driver: Option<mpsc::Sender<Command>>,
    /// The socket transport.
    transport: Option<T>,
    /// Options for the socket. These are shared with the backend task.
    options: Arc<ReqOptions>,
    /// Socket state. This is shared with the backend task.
    state: Arc<SocketState>,
    /// Optional message compressor. This is shared with the backend task.
    // NOTE: for now we're using dynamic dispatch, since using generics here
    // complicates the API a lot. We can always change this later for perf reasons.
    compressor: Option<Arc<dyn Compressor>>,
    /// Marker for the address type.
    _marker: PhantomData<A>,
}

impl<T> ReqSocket<T, SocketAddr>
where
    T: Transport<SocketAddr> + Send + Sync + Unpin + 'static,
{
    /// Connects to the target address with the default options.
    pub async fn connect(&mut self, addr: impl ToSocketAddrs) -> Result<(), ReqError> {
        let mut addrs = lookup_host(addr).await?;
        let endpoint = addrs.next().ok_or(ReqError::NoValidEndpoints)?;

        self.try_connect(endpoint).await
    }
}

impl<T> ReqSocket<T, PathBuf>
where
    T: Transport<PathBuf> + Send + Sync + Unpin + 'static,
{
    /// Connects to the target path with the default options.
    pub async fn connect(&mut self, addr: impl Into<PathBuf>) -> Result<(), ReqError> {
        self.try_connect(addr.into().clone()).await
    }
}

impl<T, A> ReqSocket<T, A>
where
    T: Transport<A> + Send + Sync + Unpin + 'static,
    A: Address,
{
    pub fn new(transport: T) -> Self {
        Self::with_options(transport, ReqOptions::default())
    }

    pub fn with_options(transport: T, options: ReqOptions) -> Self {
        Self {
            to_driver: None,
            transport: Some(transport),
            options: Arc::new(options),
            state: Arc::new(SocketState::default()),
            compressor: None,
            _marker: PhantomData,
        }
    }

    /// Sets the message compressor for this socket.
    pub fn with_compressor<C: Compressor + 'static>(mut self, compressor: C) -> Self {
        self.compressor = Some(Arc::new(compressor));
        self
    }

    pub fn stats(&self) -> &ReqStats {
        &self.state.stats.specific
    }

    pub async fn request(&self, message: Bytes) -> Result<Bytes, ReqError> {
        let (response_tx, response_rx) = oneshot::channel();

        let msg = ReqMessage::new(message);

        self.to_driver
            .as_ref()
            .ok_or(ReqError::SocketClosed)?
            .send(Command::Send { message: msg, response: response_tx })
            .await
            .map_err(|_| ReqError::SocketClosed)?;

        response_rx.await.map_err(|_| ReqError::SocketClosed)?
    }

    /// Tries to connect to the target endpoint with the default options.
    /// A ReqSocket can only be connected to a single address.
    pub async fn try_connect(&mut self, endpoint: A) -> Result<(), ReqError> {
        // Initialize communication channels
        let (to_driver, from_socket) = mpsc::channel(DEFAULT_BUFFER_SIZE);

        let transport = self.transport.take().expect("Transport has been moved already");

        // We initialize the connection as inactive, and let it be activated
        // by the backend task as soon as the driver is spawned.
        let conn_state = ConnectionState::Inactive {
            addr: endpoint.clone(),
            backoff: ExponentialBackoff::new(Duration::from_millis(20), 16),
        };

        let timeout_check_interval = tokio::time::interval(self.options.timeout / 10);

        let flush_interval = self.options.flush_interval.map(tokio::time::interval);

        // TODO: we should limit the amount of active outgoing requests, and that should be the
        // capacity. If we do this, we'll never have to re-allocate.
        let pending_requests = FxHashMap::default();

        // Create the socket backend
        let driver: ReqDriver<T, A> = ReqDriver {
            addr: endpoint,
            options: Arc::clone(&self.options),
            socket_state: Arc::clone(&self.state),
            id_counter: 0,
            from_socket,
            transport,
            conn_state,
            pending_requests,
            timeout_check_interval,
            flush_interval,
            should_flush: false,
            conn_task: None,
            egress_queue: Default::default(),
            compressor: self.compressor.clone(),
        };

        // Spawn the backend task
        tokio::spawn(driver);

        self.to_driver = Some(to_driver);

        Ok(())
    }
}
