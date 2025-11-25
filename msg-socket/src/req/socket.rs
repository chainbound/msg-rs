use arc_swap::Guard;
use bytes::Bytes;
use msg_common::{IdBase58, span::WithSpan};
use rustc_hash::FxHashMap;
use std::{marker::PhantomData, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    net::{ToSocketAddrs, lookup_host},
    sync::{mpsc, oneshot},
};
use tokio_util::codec::Framed;

use msg_transport::{Address, MeteredIo, Transport};
use msg_wire::{compression::Compressor, reqrep};

use super::{Command, DEFAULT_BUFFER_SIZE, ReqError, ReqOptions};
use crate::{
    ConnectionState, ExponentialBackoff, ReqMessage, SendCommand,
    req::driver::ConnectionCtl,
    req::{SocketState, driver::ReqDriver, stats::ReqStats},
    stats::SocketStats,
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
    state: SocketState<T::Stats>,
    /// Optional message compressor. This is shared with the backend task.
    // NOTE: for now we're using dynamic dispatch, since using generics here
    // complicates the API a lot. We can always change this later for perf reasons.
    compressor: Option<Arc<dyn Compressor>>,
    /// Marker for the address type.
    _marker: PhantomData<A>,
}

impl<T> ReqSocket<T, SocketAddr>
where
    T: Transport<SocketAddr>,
{
    /// Connects to the target address with the default options.
    pub async fn connect(&mut self, addr: impl ToSocketAddrs) -> Result<(), ReqError> {
        let mut addrs = lookup_host(addr).await?;
        let endpoint = addrs.next().ok_or(ReqError::NoValidEndpoints)?;

        self.try_connect(endpoint).await
    }

    /// Starts connecting to a resolved socket address. This is essentially a [`Self::connect`]
    /// variant that doesn't error or block due to DNS resolution and blocking connect.
    pub fn connect_sync(&mut self, addr: SocketAddr) {
        // TODO: Don't panic, return error
        let transport = self.transport.take().expect("Transport has been moved already");
        // We initialize the connection as inactive, and let it be activated
        // by the backend task as soon as the driver is spawned.
        let conn_state = ConnectionState::Inactive {
            addr,
            backoff: ExponentialBackoff::new(Duration::from_millis(20), 16),
        };

        self.spawn_driver(addr, transport, conn_state)
    }
}

impl<T> ReqSocket<T, PathBuf>
where
    T: Transport<PathBuf>,
{
    /// Connects to the target path with the default options.
    pub async fn connect(&mut self, addr: impl Into<PathBuf>) -> Result<(), ReqError> {
        self.try_connect(addr.into().clone()).await
    }
}

impl<T, A> ReqSocket<T, A>
where
    T: Transport<A>,
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
            state: SocketState::default(),
            compressor: None,
            _marker: PhantomData,
        }
    }

    /// Sets the message compressor for this socket.
    pub fn with_compressor<C: Compressor + 'static>(mut self, compressor: C) -> Self {
        self.compressor = Some(Arc::new(compressor));
        self
    }

    /// Returns the socket stats.
    pub fn stats(&self) -> &SocketStats<ReqStats> {
        &self.state.stats
    }

    /// Get the latest transport-level stats snapshot.
    pub fn transport_stats(&self) -> Guard<Arc<T::Stats>> {
        self.state.transport_stats.load()
    }

    pub async fn request(&self, message: Bytes) -> Result<Bytes, ReqError> {
        let (response_tx, response_rx) = oneshot::channel();

        let msg = ReqMessage::new(message);

        self.to_driver
            .as_ref()
            .ok_or(ReqError::SocketClosed)?
            .send(Command::Send(SendCommand::new(WithSpan::current(msg), response_tx)))
            .await
            .map_err(|_| ReqError::SocketClosed)?;

        response_rx.await.map_err(|_| ReqError::SocketClosed)?
    }

    /// Tries to connect to the target endpoint with the default options.
    /// A ReqSocket can only be connected to a single address.
    pub async fn try_connect(&mut self, endpoint: A) -> Result<(), ReqError> {
        // TODO: Don't panic, return error
        let mut transport = self.transport.take().expect("transport has been moved already");

        let conn_state = if self.options.blocking_connect {
            let io = transport
                .connect(endpoint.clone())
                .await
                .map_err(|e| ReqError::Connect(Box::new(e)))?;

            let metered = MeteredIo::new(io, Arc::clone(&self.state.transport_stats));
            let mut framed = Framed::new(metered, reqrep::Codec::new());
            framed.set_backpressure_boundary(self.options.backpressure_boundary);

            ConnectionState::Active { channel: framed }
        } else {
            // We initialize the connection as inactive, and let it be activated
            // by the backend task as soon as the driver is spawned.
            ConnectionState::Inactive {
                addr: endpoint.clone(),
                backoff: ExponentialBackoff::new(Duration::from_millis(20), 16),
            }
        };

        self.spawn_driver(endpoint, transport, conn_state);

        Ok(())
    }

    /// Internal method to initialize and spawn the driver.
    fn spawn_driver(
        &mut self,
        endpoint: A,
        transport: T,
        conn_state: ConnectionCtl<T::Io, T::Stats, A>,
    ) {
        // Initialize communication channels
        let (to_driver, from_socket) = mpsc::channel(DEFAULT_BUFFER_SIZE);

        let timeout_check_interval = tokio::time::interval(self.options.timeout / 10);
        let flush_interval = self.options.flush_interval.map(tokio::time::interval);

        // TODO: we should limit the amount of active outgoing requests, and that should be the
        // capacity. If we do this, we'll never have to re-allocate.
        let pending_requests = FxHashMap::default();

        let id = IdBase58::new();
        let span = tracing::info_span!(parent: None, "req_driver", %id, addr = ?endpoint);

        // Create the socket backend
        let driver: ReqDriver<T, A> = ReqDriver {
            addr: endpoint,
            options: Arc::clone(&self.options),
            socket_state: self.state.clone(),
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
            id,
            span,
        };

        // Spawn the backend task
        tokio::spawn(driver);

        self.to_driver = Some(to_driver);
    }
}
