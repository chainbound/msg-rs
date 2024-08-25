use std::{
    io,
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
    rep::{driver::RepDriver, DEFAULT_BUFFER_SIZE},
    rep::{SocketState, SocketStats},
    Authenticator, PubError, RepOptions, Request,
};

use msg_transport::Transport;
use msg_wire::compression::Compressor;

/// A reply socket. This socket implements [`Stream`] and yields incoming [`Request`]s.
#[derive(Default)]
pub struct RepSocket<T: Transport> {
    /// The reply socket options, shared with the driver.
    options: Arc<RepOptions>,
    /// The reply socket state, shared with the driver.
    state: Arc<SocketState>,
    /// Receiver from the socket driver.
    from_driver: Option<mpsc::Receiver<Request<T::Addr>>>,
    /// The transport used by this socket. This value is temporary and will be moved
    /// to the driver task once the socket is bound.
    transport: Option<T>,
    /// Optional connection authenticator.
    auth: Option<Arc<dyn Authenticator>>,
    /// The local address this socket is bound to.
    local_addr: Option<T::Addr>,
    /// Optional message compressor.
    compressor: Option<Arc<dyn Compressor>>,
}

impl<T> RepSocket<T>
where
    T: Transport<Addr = SocketAddr> + Send + Unpin + 'static,
{
    /// Binds the socket to the given socket addres
    ///
    /// This method is only available for transports that support [`SocketAddr`] as address type,
    /// like [`Tcp`](msg_transport::tcp::Tcp) and [`Quic`](msg_transport::quic::Quic).
    pub async fn bind_socket(&mut self, addr: impl ToSocketAddrs) -> Result<(), PubError> {
        let addrs = lookup_host(addr).await?;
        self.try_bind(addrs.collect()).await
    }
}

impl<T> RepSocket<T>
where
    T: Transport<Addr = PathBuf> + Send + Unpin + 'static,
{
    /// Binds the socket to the given path.
    ///
    /// This method is only available for transports that support [`PathBuf`] as address type,
    /// like [`Ipc`](msg_transport::ipc::Ipc).
    pub async fn bind_path(&mut self, path: impl Into<PathBuf>) -> Result<(), PubError> {
        let addr = path.into().clone();
        self.try_bind(vec![addr]).await
    }
}

impl<T> RepSocket<T>
where
    T: Transport + Send + Unpin + 'static,
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
    pub fn with_auth<A: Authenticator>(mut self, authenticator: A) -> Self {
        self.auth = Some(Arc::new(authenticator));
        self
    }

    /// Sets the message compressor for this socket.
    pub fn with_compressor<C: Compressor + 'static>(mut self, compressor: C) -> Self {
        self.compressor = Some(Arc::new(compressor));
        self
    }

    /// Binds the socket to the given address. This spawns the socket driver task.
    pub async fn try_bind(&mut self, addresses: Vec<T::Addr>) -> Result<(), PubError> {
        let (to_socket, from_backend) = mpsc::channel(DEFAULT_BUFFER_SIZE);

        let mut transport = self
            .transport
            .take()
            .expect("Transport has been moved already");

        for addr in addresses {
            match transport.bind(addr.clone()).await {
                Ok(_) => break,
                Err(e) => {
                    warn!("Failed to bind to {:?}, trying next address: {}", addr, e);
                    continue;
                }
            }
        }

        let Some(local_addr) = transport.local_addr() else {
            return Err(PubError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "could not bind to any valid address",
            )));
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

    pub fn stats(&self) -> &SocketStats {
        &self.state.stats
    }

    /// Returns the local address this socket is bound to. `None` if the socket is not bound.
    pub fn local_addr(&self) -> Option<&T::Addr> {
        self.local_addr.as_ref()
    }
}

impl<T: Transport + Unpin> Stream for RepSocket<T> {
    type Item = Request<T::Addr>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut()
            .from_driver
            .as_mut()
            .expect("Inactive socket")
            .poll_recv(cx)
    }
}
