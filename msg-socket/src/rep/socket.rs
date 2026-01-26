use std::{
    net::SocketAddr,
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::{Stream, stream::FuturesUnordered};
use tokio::{
    net::{ToSocketAddrs, lookup_host},
    sync::mpsc,
    task::{JoinHandle, JoinSet},
};
use tokio_stream::StreamMap;
use tracing::{debug, warn};

use crate::{
    ConnectionHook, ConnectionHookErased, DEFAULT_QUEUE_SIZE, RepOptions, Request,
    rep::{RepError, SocketState, driver::RepDriver},
};

use msg_transport::{Address, Transport};
use msg_wire::compression::Compressor;

use super::stats::RepStats;

/// A reply socket. This socket implements [`Stream`] and yields incoming [`Request`]s.
pub struct RepSocket<T: Transport<A>, A: Address> {
    /// The reply socket options, shared with the driver.
    options: Arc<RepOptions>,
    /// The reply socket state, shared with the driver.
    state: Arc<SocketState>,
    /// Receiver from the socket driver.
    from_driver: Option<mpsc::Receiver<Request<A>>>,
    /// The transport used by this socket. This value is temporary and will be moved
    /// to the driver task once the socket is bound.
    transport: Option<T>,
    /// Optional connection hook.
    hook: Option<Arc<dyn ConnectionHookErased<T::Io>>>,
    /// The local address this socket is bound to.
    local_addr: Option<A>,
    /// Optional message compressor.
    compressor: Option<Arc<dyn Compressor>>,
    /// A sender channel for [`Transport::Control`] changes.
    control_tx: Option<mpsc::Sender<T::Control>>,

    /// Internal task representing a running [`RepDriver`].
    _driver_task: Option<JoinHandle<Result<(), RepError>>>,
}

impl<T> RepSocket<T, SocketAddr>
where
    T: Transport<SocketAddr>,
{
    /// Binds the socket to the given socket address.
    pub async fn bind(&mut self, addr: impl ToSocketAddrs) -> Result<(), RepError> {
        let addrs = lookup_host(addr).await?;
        self.try_bind(addrs.collect()).await
    }
}

impl<T> RepSocket<T, PathBuf>
where
    T: Transport<PathBuf>,
{
    /// Binds the socket to the given path.
    pub async fn bind(&mut self, path: impl Into<PathBuf>) -> Result<(), RepError> {
        let addr = path.into().clone();
        self.try_bind(vec![addr]).await
    }
}

impl<T, A> RepSocket<T, A>
where
    T: Transport<A>,
    A: Address,
{
    /// Creates a new reply socket with the default [`RepOptions`].
    pub fn new(transport: T) -> Self {
        Self::with_options(transport, RepOptions::balanced())
    }

    /// Sets the options for this socket.
    pub fn with_options(transport: T, options: RepOptions) -> Self {
        Self {
            from_driver: None,
            local_addr: None,
            transport: Some(transport),
            options: Arc::new(options),
            state: Arc::new(SocketState::default()),
            hook: None,
            compressor: None,
            control_tx: None,
            _driver_task: None,
        }
    }

    /// Sets the message compressor for this socket.
    pub fn with_compressor<C: Compressor + 'static>(mut self, compressor: C) -> Self {
        self.compressor = Some(Arc::new(compressor));
        self
    }

    /// Sets the connection hook for this socket.
    ///
    /// The hook is called when a new connection is accepted, before the connection
    /// is used for request/reply communication.
    pub fn with_connection_hook<H>(mut self, hook: H) -> Self
    where
        H: ConnectionHook<T::Io>,
    {
        self.hook = Some(Arc::new(hook));
        self
    }

    /// Binds the socket to the given address. This spawns the socket driver task.
    pub async fn try_bind(&mut self, addresses: Vec<A>) -> Result<(), RepError> {
        let (to_socket, from_backend) = mpsc::channel(DEFAULT_QUEUE_SIZE);
        let (control_tx, control_rx) = mpsc::channel(DEFAULT_QUEUE_SIZE);

        let mut transport = self.transport.take().expect("transport has been moved already");

        for addr in addresses {
            match transport.bind(addr.clone()).await {
                Ok(_) => break,
                Err(e) => {
                    warn!(?e, ?addr, "failed to bind");
                    continue;
                }
            }
        }

        let Some(local_addr) = transport.local_addr() else {
            return Err(RepError::NoValidEndpoints);
        };

        let span = tracing::info_span!(parent: None, "rep_driver", ?local_addr);

        span.in_scope(|| {
            debug!("listening");
        });

        let backend = RepDriver {
            transport,
            options: Arc::clone(&self.options),
            state: Arc::clone(&self.state),
            peer_states: StreamMap::with_capacity(self.options.max_clients.unwrap_or(64)),
            to_socket,
            hook: self.hook.take(),
            hook_tasks: JoinSet::new(),
            compressor: self.compressor.take(),
            conn_tasks: FuturesUnordered::new(),
            control_rx,
            span,
        };

        self._driver_task = Some(tokio::spawn(backend));
        self.local_addr = Some(local_addr);
        self.from_driver = Some(from_backend);
        self.control_tx = Some(control_tx);

        Ok(())
    }

    /// Returns the statistics for this socket.
    pub fn stats(&self) -> &RepStats {
        &self.state.stats.specific
    }

    /// Returns the local address this socket is bound to. `None` if the socket is not bound.
    pub fn local_addr(&self) -> Option<&A> {
        self.local_addr.as_ref()
    }

    /// Returns the next request from the socket using an unpinned interface.
    pub fn poll_next_unpin(&mut self, cx: &mut Context<'_>) -> Poll<Option<Request<A>>> {
        Pin::new(self).poll_next(cx)
    }

    /// Issue a [`Transport::Control`] change to the underlying transport.
    pub async fn control(
        &mut self,
        control: T::Control,
    ) -> Result<(), mpsc::error::SendError<T::Control>> {
        let Some(tx) = self.control_tx.as_mut() else {
            tracing::warn!("calling control on a non-bound socket, this is a no-op");
            return Ok(());
        };
        tx.send(control).await
    }
}

impl<T, A> Stream for RepSocket<T, A>
where
    T: Transport<A>,
    A: Address,
{
    type Item = Request<A>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().from_driver.as_mut().expect("Inactive socket").poll_recv(cx)
    }
}
