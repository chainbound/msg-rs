use bytes::Bytes;
use std::{net::SocketAddr, sync::Arc};
use tokio::{sync::broadcast, task::JoinSet};
use tracing::debug;

use super::{driver::PubDriver, stats::SocketStats, PubError, PubMessage, PubOptions, SocketState};
use crate::Authenticator;
use msg_transport::ServerTransport;
use msg_wire::compression::Compressor;

/// A publisher socket. This is thread-safe and can be cloned.
#[derive(Clone, Default)]
pub struct PubSocket<T: ServerTransport> {
    /// The reply socket options, shared with the driver.
    options: Arc<PubOptions<T::BindOptions>>,
    /// The reply socket state, shared with the driver.
    state: Arc<SocketState>,
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

impl<T: ServerTransport> PubSocket<T> {
    /// Creates a new reply socket with the default [`PubOptions`].
    pub fn new() -> Self {
        Self::with_options(PubOptions::default())
    }

    /// Creates a new publisher socket with the given transport and options.
    pub fn with_options(options: PubOptions<T::BindOptions>) -> Self {
        Self {
            local_addr: None,
            to_sessions_bcast: None,
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
    pub async fn bind(&mut self, addr: SocketAddr) -> Result<(), PubError> {
        // let (to_driver, from_socket) = mpsc::channel(DEFAULT_BUFFER_SIZE);
        let (to_sessions_bcast, from_socket_bcast) =
            broadcast::channel(self.options.session_buffer_size);

        let transport = T::bind_with_options(addr, &self.options.bind_options)
            .await
            .map_err(|e| PubError::Transport(Box::new(e)))?;

        let local_addr = transport
            .local_addr()
            .map_err(|e| PubError::Transport(Box::new(e)))?;

        tracing::debug!("Listening on {}", local_addr);

        let backend = PubDriver {
            id_counter: 0,
            transport,
            options: Arc::clone(&self.options),
            state: Arc::clone(&self.state),
            auth: self.auth.take(),
            auth_tasks: JoinSet::new(),
            from_socket_bcast,
        };

        tokio::spawn(backend);

        self.local_addr = Some(local_addr);
        // self.to_driver = Some(to_driver);
        self.to_sessions_bcast = Some(to_sessions_bcast);

        Ok(())
    }

    /// Publishes a message to the given topic. If the topic doesn't exist, this is a no-op.
    pub async fn publish(&self, topic: String, message: Bytes) -> Result<(), PubError> {
        let mut msg = PubMessage::new(topic, message);

        // We compress here since that way we only have to do it once.
        // Compression is only done if the message is larger than the
        // configured minimum payload size.
        let len_before = msg.payload().len();
        if len_before > self.options.min_compress_size {
            if let Some(ref compressor) = self.compressor {
                msg.compress(compressor.as_ref())?;

                debug!(
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
