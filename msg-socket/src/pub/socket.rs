use bytes::Bytes;
use futures::stream::FuturesUnordered;
use std::{io, net::SocketAddr, sync::Arc};
use tokio::{
    net::{lookup_host, ToSocketAddrs},
    sync::broadcast,
    task::JoinSet,
};
use tracing::{debug, trace};

use super::{driver::PubDriver, stats::SocketStats, PubError, PubMessage, PubOptions, SocketState};
use crate::Authenticator;
use msg_transport::Transport;
use msg_wire::compression::Compressor;

/// A publisher socket. This is thread-safe and can be cloned.
#[derive(Clone, Default)]
pub struct PubSocket<T: Transport> {
    /// The reply socket options, shared with the driver.
    options: Arc<PubOptions>,
    /// The reply socket state, shared with the driver.
    state: Arc<SocketState>,
    /// The transport used by this socket. This value is temporary and will be moved
    /// to the driver task once the socket is bound.
    transport: Option<T>,
    /// The broadcast channel to all active [`SubscriberSession`](super::session::SubscriberSession)s.
    to_sessions_bcast: Option<broadcast::Sender<PubMessage>>,
    /// Optional connection authenticator.
    auth: Option<Arc<dyn Authenticator>>,
    /// Optional message compressor.
    // NOTE: for now we're using dynamic dispatch, since using generics here
    // complicates the API a lot. We can always change this later for perf reasons.
    compressor: Option<Arc<dyn Compressor>>,
    /// The local address this socket is bound to.
    local_addr: Option<SocketAddr>,
}

impl<T> PubSocket<T>
where
    T: Transport + Send + Unpin + 'static,
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
    pub async fn bind<A: ToSocketAddrs>(&mut self, addr: A) -> Result<(), PubError> {
        let (to_sessions_bcast, from_socket_bcast) =
            broadcast::channel(self.options.session_buffer_size);

        let mut transport = self
            .transport
            .take()
            .expect("Transport has been moved already");

        let addrs = lookup_host(addr).await?;

        for addr in addrs {
            match transport.bind(addr).await {
                Ok(_) => break,
                Err(e) => {
                    tracing::warn!("Failed to bind to {}, trying next address: {}", addr, e);
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

        tracing::debug!("Listening on {}", local_addr);

        let backend = PubDriver {
            id_counter: 0,
            transport,
            options: Arc::clone(&self.options),
            state: Arc::clone(&self.state),
            auth: self.auth.take(),
            auth_tasks: JoinSet::new(),
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
        let topic = topic.into();
        let mut msg = PubMessage::new(topic, message);

        // We compress here since that way we only have to do it once.
        // Compression is only done if the message is larger than the
        // configured minimum payload size.
        let len_before = msg.payload().len();
        if len_before > self.options.min_compress_size {
            if let Some(ref compressor) = self.compressor {
                msg.compress(compressor.as_ref())?;

                trace!(
                    "Compressed message from {} to {} bytes",
                    len_before,
                    msg.payload().len(),
                );
            }
        }

        // Broadcast the message directly to all active sessions.
        if self
            .to_sessions_bcast
            .as_ref()
            .ok_or(PubError::SocketClosed)?
            .send(msg)
            .is_err()
        {
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

            debug!(
                "Compressed message from {} to {} bytes",
                len_before,
                msg.payload().len(),
            );
        }

        // Broadcast the message directly to all active sessions.
        if self
            .to_sessions_bcast
            .as_ref()
            .ok_or(PubError::SocketClosed)?
            .send(msg)
            .is_err()
        {
            debug!("No active subscriber sessions");
        }

        Ok(())
    }

    pub fn stats(&self) -> &SocketStats {
        &self.state.stats
    }

    /// Returns the local address this socket is bound to. `None` if the socket is not bound.
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.local_addr
    }
}
