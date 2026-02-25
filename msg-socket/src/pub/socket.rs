use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use arc_swap::Guard;
use bytes::Bytes;
use futures::stream::FuturesUnordered;
use tokio::{
    net::{ToSocketAddrs, lookup_host},
    sync::broadcast,
    task::JoinSet,
};
use tracing::{debug, trace, warn};

use super::{PubError, PubMessage, PubOptions, SocketState, driver::PubDriver, stats::PubStats};
use crate::{ConnectionHook, ConnectionHookErased};

use msg_transport::{Address, Transport};
use msg_wire::compression::Compressor;

/// A publisher socket. This is thread-safe and can be cloned.
///
/// Publisher sockets are used to publish messages under certain topics to multiple subscribers.
///
/// ## Session
/// Per subscriber, the socket maintains a session. The session
/// manages the underlying connection and all of its state, such as the topic subscriptions. It also
/// manages a queue of messages to be transmitted on the connection.
#[derive(Clone)]
pub struct PubSocket<T: Transport<A>, A: Address> {
    /// The reply socket options, shared with the driver.
    options: Arc<PubOptions>,
    /// The reply socket state, shared with the driver.
    state: Arc<SocketState<T::Stats>>,
    /// The transport used by this socket. This value is temporary and will be moved
    /// to the driver task once the socket is bound.
    transport: Option<T>,
    /// The broadcast channel to all active
    /// [`SubscriberSession`](super::session::SubscriberSession)s.
    to_sessions_bcast: Option<broadcast::Sender<PubMessage>>,
    /// Optional connection hook.
    hook: Option<Arc<dyn ConnectionHookErased<T::Io>>>,
    /// Optional message compressor.
    // NOTE: for now we're using dynamic dispatch, since using generics here
    // complicates the API a lot. We can always change this later for perf reasons.
    compressor: Option<Arc<dyn Compressor>>,
    /// The local address this socket is bound to.
    local_addr: Option<A>,
}

impl<T> PubSocket<T, SocketAddr>
where
    T: Transport<SocketAddr>,
{
    /// Binds the socket to the given socket address.
    ///
    /// This method is only available for transports that support [`SocketAddr`] as address type,
    /// like [`Tcp`](msg_transport::tcp::Tcp) and [`Quic`](msg_transport::quic::Quic).
    pub async fn bind(&mut self, addr: impl ToSocketAddrs) -> Result<(), PubError> {
        let addrs = lookup_host(addr).await?;
        self.try_bind(addrs.collect()).await
    }
}

impl<T> PubSocket<T, PathBuf>
where
    T: Transport<PathBuf>,
{
    /// Binds the socket to the given path.
    ///
    /// This method is only available for transports that support [`PathBuf`] as address type,
    /// like [`Ipc`](msg_transport::ipc::Ipc).
    pub async fn bind(&mut self, path: impl Into<PathBuf>) -> Result<(), PubError> {
        self.try_bind(vec![path.into()]).await
    }
}

impl<T, A> PubSocket<T, A>
where
    T: Transport<A>,
    A: Address,
{
    /// Creates a new reply socket with the default [`PubOptions`].
    pub fn new(transport: T) -> Self {
        Self::with_options(transport, PubOptions::default())
    }

    /// Creates a new publisher socket with the given transport and options.
    pub fn with_options(transport: T, options: PubOptions) -> Self {
        Self {
            local_addr: None,
            to_sessions_bcast: None,
            options: Arc::new(options),
            transport: Some(transport),
            state: Arc::new(SocketState::<T::Stats>::default()),
            hook: None,
            compressor: None,
        }
    }

    /// Sets the message compressor for this socket.
    pub fn with_compressor<C: Compressor + 'static>(mut self, compressor: C) -> Self {
        self.compressor = Some(Arc::new(compressor));
        self
    }

    /// Sets the connection hook for this socket.
    ///
    /// The connection hook is called when a new connection is accepted, before the connection
    /// is used for pub/sub communication.
    ///
    /// # Panics
    ///
    /// Panics if the socket has already been bound (driver started).
    pub fn with_connection_hook<H>(mut self, hook: H) -> Self
    where
        H: ConnectionHook<T::Io>,
    {
        assert!(self.transport.is_some(), "cannot set connection hook after socket has been bound");
        self.hook = Some(Arc::new(hook));
        self
    }

    /// Binds the socket to the given addresses in order until one succeeds.
    ///
    /// This also spawns the socket driver task.
    pub async fn try_bind(&mut self, addresses: Vec<A>) -> Result<(), PubError> {
        let (to_sessions_bcast, from_socket_bcast) =
            broadcast::channel(self.options.high_water_mark);

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
            return Err(PubError::NoValidEndpoints);
        };

        debug!("Listening on {:?}", local_addr);

        let backend = PubDriver {
            id_counter: 0,
            transport,
            options: Arc::clone(&self.options),
            state: Arc::clone(&self.state),
            hook: self.hook.take(),
            hook_tasks: JoinSet::new(),
            conn_tasks: FuturesUnordered::new(),
            from_socket_bcast,
        };

        tokio::spawn(backend);

        self.local_addr = Some(local_addr);
        self.to_sessions_bcast = Some(to_sessions_bcast);

        Ok(())
    }

    /// Publishes a message to the given topic. If the topic doesn't exist, this is a no-op.
    pub async fn publish(&self, topic: impl Into<String>, message: Bytes) -> Result<(), PubError> {
        let mut msg = PubMessage::new(topic.into(), message);

        // We compress here since that way we only have to do it once.
        // Compression is only done if the message is larger than the
        // configured minimum payload size.
        let len_before = msg.payload().len();
        if len_before > self.options.min_compress_size &&
            let Some(ref compressor) = self.compressor
        {
            msg.compress(compressor.as_ref())?;
            trace!("Compressed message from {} to {} bytes", len_before, msg.payload().len());
        }

        // Broadcast the message directly to all active sessions.
        if self.to_sessions_bcast.as_ref().ok_or(PubError::SocketClosed)?.send(msg).is_err() {
            debug!("No active subscriber sessions");
        }

        Ok(())
    }

    /// Publishes a message to the given topic, compressing the payload if a compressor is set.
    /// If the topic doesn't exist, this is a no-op.
    pub fn try_publish(&self, topic: String, message: Bytes) -> Result<(), PubError> {
        let mut msg = PubMessage::new(topic, message);

        // We compress here since that way we only have to do it once.
        if let Some(ref compressor) = self.compressor {
            let len_before = msg.payload().len();

            // For relatively small messages, this takes <100us
            msg.compress(compressor.as_ref())?;

            debug!("Compressed message from {} to {} bytes", len_before, msg.payload().len(),);
        }

        // Broadcast the message directly to all active sessions.
        if self.to_sessions_bcast.as_ref().ok_or(PubError::SocketClosed)?.send(msg).is_err() {
            debug!("No active subscriber sessions");
        }

        Ok(())
    }

    pub fn stats(&self) -> &PubStats {
        &self.state.stats.specific
    }

    /// Get the latest transport-level stats snapshot.
    pub fn transport_stats(&self) -> Guard<Arc<T::Stats>> {
        self.state.transport_stats.load()
    }

    /// Returns the local address this socket is bound to. `None` if the socket is not bound.
    pub fn local_addr(&self) -> Option<&A> {
        self.local_addr.as_ref()
    }
}
