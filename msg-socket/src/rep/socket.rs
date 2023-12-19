use futures::Stream;
use std::{
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::{sync::mpsc, task::JoinSet};
use tokio_stream::StreamMap;

use crate::{
    rep::{driver::RepDriver, DEFAULT_BUFFER_SIZE},
    rep::{SocketState, SocketStats},
    Authenticator, PubError, RepOptions, Request,
};
use msg_transport::ServerTransport;

/// A reply socket. This socket implements [`Stream`] and yields incoming [`Request`]s.
#[derive(Default)]
pub struct RepSocket<T: ServerTransport> {
    /// The reply socket options, shared with the driver.
    options: Arc<RepOptions<T::BindOptions>>,
    /// The reply socket state, shared with the driver.
    state: Arc<SocketState>,
    /// Receiver from the socket driver.
    from_driver: Option<mpsc::Receiver<Request>>,
    /// Optional connection authenticator.
    auth: Option<Arc<dyn Authenticator>>,
    /// The local address this socket is bound to.
    local_addr: Option<SocketAddr>,
}

impl<T: ServerTransport> RepSocket<T> {
    /// Creates a new reply socket with the default [`RepOptions`].
    pub fn new() -> Self {
        Self::with_options(RepOptions::default())
    }

    /// Sets the options for this socket.
    pub fn with_options(options: RepOptions<T::BindOptions>) -> Self {
        Self {
            from_driver: None,
            local_addr: None,
            options: Arc::new(options),
            state: Arc::new(SocketState::default()),
            auth: None,
        }
    }

    /// Sets the connection authenticator for this socket.
    pub fn with_auth<A: Authenticator>(mut self, authenticator: A) -> Self {
        self.auth = Some(Arc::new(authenticator));
        self
    }

    /// Binds the socket to the given address. This spawns the socket driver task.
    pub async fn bind(&mut self, addr: SocketAddr) -> Result<(), PubError> {
        let (to_socket, from_backend) = mpsc::channel(DEFAULT_BUFFER_SIZE);

        let transport = T::bind_with_options(addr, &self.options.bind_options)
            .await
            .map_err(|e| PubError::Transport(Box::new(e)))?;

        let local_addr = transport
            .local_addr()
            .map_err(|e| PubError::Transport(Box::new(e)))?;

        tracing::debug!("Listening on {}", local_addr);

        let backend = RepDriver {
            transport,
            options: Arc::clone(&self.options),
            state: Arc::clone(&self.state),
            peer_states: StreamMap::with_capacity(self.options.max_clients.unwrap_or(64)),
            to_socket,
            auth: self.auth.take(),
            auth_tasks: JoinSet::new(),
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
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.local_addr
    }
}

impl<T: ServerTransport> Stream for RepSocket<T> {
    type Item = Request;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.from_driver
            .as_mut()
            .expect("Inactive socket")
            .poll_recv(cx)
    }
}
