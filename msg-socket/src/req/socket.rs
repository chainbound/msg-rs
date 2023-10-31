use std::{collections::VecDeque, sync::Arc};
use std::time::Instant;
use bytes::Bytes;
use msg_transport::ClientTransport;
use msg_wire::reqrep;
use rustc_hash::FxHashMap;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;

use crate::{req::stats::SocketStats, req::SocketState};

use super::{Command, ReqDriver, ReqError, ReqOptions, DEFAULT_BUFFER_SIZE};

#[derive(Debug, Clone)]
pub struct ReqSocket<T: ClientTransport> {
    /// Command channel to the backend task.
    to_driver: Option<mpsc::Sender<Command>>,
    /// The underlying transport.
    transport: T,
    /// Options for the socket. These are shared with the backend task.
    options: Arc<ReqOptions>,
    /// Socket state. This is shared with the backend task.
    state: Arc<SocketState>,
}

impl<T: ClientTransport> ReqSocket<T> {
    pub fn new(transport: T) -> Self {
        Self::with_options(transport, ReqOptions::default())
    }

    pub fn with_options(transport: T, options: ReqOptions) -> Self {
        Self {
            to_driver: None,
            transport,
            options: Arc::new(options),
            state: Arc::new(SocketState::default()),
        }
    }

    pub fn stats(&self) -> &SocketStats {
        &self.state.stats
    }

    pub async fn request(&self, message: Bytes) -> Result<Bytes, ReqError> {
        let (response_tx, response_rx) = oneshot::channel();

        self.to_driver
            .as_ref()
            .ok_or(ReqError::SocketClosed)?
            .send(Command::Send {
                message,
                response: response_tx,
            })
            .await
            .map_err(|_| ReqError::SocketClosed)?;

        response_rx.await.map_err(|_| ReqError::SocketClosed)?
    }

    /// Connects to the target with the default options.
    pub async fn connect(&mut self, endpoint: &str) -> Result<(), ReqError> {
        // Initialize communication channels
        let (to_driver, from_socket) = mpsc::channel(DEFAULT_BUFFER_SIZE);

        // TODO: return error
        let endpoint = endpoint.parse().unwrap();

        tracing::debug!("Connected to {}", endpoint);

        let stream = self
            .transport
            .connect_with_auth(endpoint, self.options.auth_token.clone())
            .await
            .map_err(|e| ReqError::Transport(Box::new(e)))?;

        // Create the socket backend
        let driver = ReqDriver {
            options: Arc::clone(&self.options),
            id_counter: 0,
            from_socket,
            conn: Framed::new(stream, reqrep::Codec::new()),
            egress_queue: VecDeque::new(),
            // TODO: we should limit the amount of active outgoing requests, and that should be the capacity.
            // If we do this, we'll never have to re-allocate.
            pending_requests: FxHashMap::default(),
            socket_state: Arc::clone(&self.state),
            last_timeout_check: Instant::now(),
        };

        // Spawn the backend task
        tokio::spawn(driver);

        self.to_driver = Some(to_driver);

        Ok(())
    }
}
