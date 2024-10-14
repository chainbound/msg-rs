use std::{
    net::SocketAddr,
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::{stream::FuturesUnordered, Stream};
use tokio::{
    net::{lookup_host, ToSocketAddrs},
    sync::mpsc,
    task::JoinSet,
};
use tokio_stream::StreamMap;
use tracing::{debug, warn};

use crate::{
    rep::{driver::RepDriver, RepError, SocketState, SocketStats},
    Authenticator, RepOptions, Request, DEFAULT_BUFFER_SIZE,
};

use msg_transport::{Address, Transport};
use msg_wire::compression::Compressor;

/// A reply socket. This socket implements [`Stream`] and yields incoming [`Request`]s.
#[derive(Default)]
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
    /// Optional connection authenticator.
    auth: Option<Arc<dyn Authenticator>>,
    /// The local address this socket is bound to.
    local_addr: Option<A>,
    /// Optional message compressor.
    compressor: Option<Arc<dyn Compressor>>,
}

impl<T> RepSocket<T, SocketAddr>
where
    T: Transport<SocketAddr> + Send + Unpin + 'static,
{
    /// Binds the socket to the given socket address.
    pub async fn bind(&mut self, addr: impl ToSocketAddrs) -> Result<(), RepError> {
        let addrs = lookup_host(addr).await?;
        self.try_bind(addrs.collect()).await
    }
}

impl<T> RepSocket<T, PathBuf>
where
    T: Transport<PathBuf> + Send + Unpin + 'static,
{
    /// Binds the socket to the given path.
    pub async fn bind(&mut self, path: impl Into<PathBuf>) -> Result<(), RepError> {
        let addr = path.into().clone();
        self.try_bind(vec![addr]).await
    }
}

impl<T, A> RepSocket<T, A>
where
    T: Transport<A> + Send + Unpin + 'static,
    A: Address,
{
    /// Creates a new reply socket with the default [`RepOptions`].
    pub fn new(transport: T) -> Self {
        Self::with_options(transport, RepOptions::default())
    }

    /// Sets the options for this socket.
    pub fn with_options(transport: T, options: RepOptions) -> Self {
        Self {
            from_driver: None,
            local_addr: None,
            transport: Some(transport),
            options: Arc::new(options),
            state: Arc::new(SocketState::default()),
            auth: None,
            compressor: None,
        }
    }

    /// Sets the connection authenticator for this socket.
    pub fn with_auth<O: Authenticator>(mut self, authenticator: O) -> Self {
        self.auth = Some(Arc::new(authenticator));
        self
    }

    /// Sets the message compressor for this socket.
    pub fn with_compressor<C: Compressor + 'static>(mut self, compressor: C) -> Self {
        self.compressor = Some(Arc::new(compressor));
        self
    }

    /// Binds the socket to the given address. This spawns the socket driver task.
    pub async fn try_bind(&mut self, addresses: Vec<A>) -> Result<(), RepError> {
        let (to_socket, from_backend) = mpsc::channel(DEFAULT_BUFFER_SIZE);

        let mut transport = self.transport.take().expect("Transport has been moved already");

        for addr in addresses {
            match transport.bind(addr.clone()).await {
                Ok(_) => break,
                Err(e) => {
                    warn!(err = ?e, "Failed to bind to {:?}, trying next address", addr);
                    continue;
                }
            }
        }

        let Some(local_addr) = transport.local_addr() else {
            return Err(RepError::NoValidEndpoints);
        };

        debug!("Listening on {:?}", local_addr);

        let backend = RepDriver {
            transport,
            options: Arc::clone(&self.options),
            state: Arc::clone(&self.state),
            peer_states: StreamMap::with_capacity(self.options.max_clients.unwrap_or(64)),
            to_socket,
            auth: self.auth.take(),
            auth_tasks: JoinSet::new(),
            conn_tasks: FuturesUnordered::new(),
            compressor: self.compressor.take(),
        };

        tokio::spawn(backend);

        self.local_addr = Some(local_addr);
        self.from_driver = Some(from_backend);

        Ok(())
    }

    /// Returns the statistics for this socket.
    pub fn stats(&self) -> &SocketStats {
        &self.state.stats
    }

    /// Returns the local address this socket is bound to. `None` if the socket is not bound.
    pub fn local_addr(&self) -> Option<&A> {
        self.local_addr.as_ref()
    }
}

impl<T: Transport<A> + Unpin, A: Address> Stream for RepSocket<T, A> {
    type Item = Request<A>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().from_driver.as_mut().expect("Inactive socket").poll_recv(cx)
    }
}
