use bytes::Bytes;
use std::{net::SocketAddr, sync::Arc};
use tokio::{sync::broadcast, task::JoinSet};
use tracing::debug;

use super::{driver::PubDriver, PubError, PubMessage, PubOptions, DEFAULT_BUFFER_SIZE};
use crate::Authenticator;
use msg_transport::ServerTransport;

/// A publisher socket. This is thread-safe and can be cloned.
#[derive(Clone)]
pub struct PubSocket<T: ServerTransport> {
    /// The reply socket options, shared with the driver.
    options: Arc<PubOptions>,
    // The reply socket state, shared with the driver.
    // state: Arc<SocketState>,
    // to_driver: Option<mpsc::Sender<Command>>,
    /// The broadcast channel to all active [`SubscriberSession`](super::driver::SubscriberSession)s.
    to_sessions_bcast: Option<broadcast::Sender<PubMessage>>,
    /// The optional transport. This is taken when the socket is bound.
    transport: Option<T>,
    /// Optional connection authenticator.
    auth: Option<Arc<dyn Authenticator>>,
    /// The local address this socket is bound to.
    local_addr: Option<SocketAddr>,
}

impl<T: ServerTransport> PubSocket<T> {
    /// Creates a new reply socket with the default [`PubOptions`].
    pub fn new(transport: T) -> Self {
        Self {
            transport: Some(transport),
            local_addr: None,
            // to_driver: None,
            to_sessions_bcast: None,
            options: Arc::new(PubOptions::default()),
            // state: Arc::new(SocketState::default()),
            auth: None,
        }
    }

    /// Sets the options for this socket.
    pub fn with_options(mut self, options: PubOptions) -> Self {
        self.options = Arc::new(options);
        self
    }

    /// Sets the connection authenticator for this socket.
    pub fn with_auth<A: Authenticator>(mut self, authenticator: A) -> Self {
        self.auth = Some(Arc::new(authenticator));
        self
    }

    /// Binds the socket to the given address. This spawns the socket driver task.
    pub async fn bind(&mut self, addr: &str) -> Result<(), PubError> {
        // Take the transport here, so we can move it into the backend task
        let mut transport = self.transport.take().unwrap();

        // let (to_driver, from_socket) = mpsc::channel(DEFAULT_BUFFER_SIZE);
        let (to_sessions_bcast, from_socket_bcast) = broadcast::channel(DEFAULT_BUFFER_SIZE);

        transport
            .bind(addr)
            .await
            .map_err(|e| PubError::Transport(Box::new(e)))?;

        let local_addr = transport
            .local_addr()
            .map_err(|e| PubError::Transport(Box::new(e)))?;

        tracing::debug!("Listening on {}", local_addr);

        let backend = PubDriver {
            id_counter: 0,
            transport,
            // state: Arc::clone(&self.state),
            // from_socket,
            auth: self.auth.take(),
            auth_tasks: JoinSet::new(),
            from_socket_bcast,
            // from_sessions: Default::default(),
        };

        tokio::spawn(backend);

        self.local_addr = Some(local_addr);
        // self.to_driver = Some(to_driver);
        self.to_sessions_bcast = Some(to_sessions_bcast);

        Ok(())
    }

    /// Publishes a message to the given topic. If the topic doesn't exist, this is a no-op.
    pub async fn publish(&self, topic: String, message: Bytes) -> Result<(), PubError> {
        let msg = PubMessage::new(topic, message);

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

    /// Registers the given topic with the options. If the topic already exists, this is a no-op.
    // pub async fn register_topic(
    //     &mut self,
    //     topic: String,
    //     options: TopicOptions,
    // ) -> Result<(), PubError> {
    //     self.send_command(Command::RegisterTopic {
    //         topic: topic.clone(),
    //         options,
    //     })
    //     .await?;

    //     Ok(())
    // }

    // pub fn stats(&self) -> &SocketStats {
    //     &self.state.stats
    // }

    /// Returns the local address this socket is bound to. `None` if the socket is not bound.
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.local_addr
    }

    // / Sends a command to the driver, returning [`PubError::SocketClosed`] if the
    // / driver has been dropped / hasn't been spawned yet.
    // #[inline]
    // async fn send_command(&self, command: Command) -> Result<(), PubError> {
    //     self.to_driver
    //         .as_ref()
    //         .ok_or(PubError::SocketClosed)?
    //         .send(command)
    //         .await
    //         .map_err(|_| PubError::SocketClosed)?;

    //     Ok(())
    // }
}