use std::{collections::VecDeque, sync::Arc};

use bytes::Bytes;
use msg_transport::ClientTransport;
use msg_wire::reqrep;
use rustc_hash::FxHashMap;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;

use super::{Command, ReqBackend, ReqError, ReqOptions, DEFAULT_BUFFER_SIZE};

pub struct ReqSocket<T: ClientTransport> {
    /// Command channel to the backend task.
    to_backend: Option<mpsc::Sender<Command>>,
    /// The underlying transport.
    transport: T,
    /// Options for the socket. These are shared with the backend task.
    options: Arc<ReqOptions>,
}

impl<T: ClientTransport> ReqSocket<T> {
    pub fn new(transport: T) -> Self {
        Self::new_with_options(transport, ReqOptions::default())
    }

    pub fn new_with_options(transport: T, options: ReqOptions) -> Self {
        Self {
            to_backend: None,
            transport,
            options: Arc::new(options),
        }
    }
}

impl<T: ClientTransport> ReqSocket<T> {
    pub async fn request(&self, message: Bytes) -> Result<Bytes, ReqError> {
        let (response_tx, response_rx) = oneshot::channel();

        self.to_backend
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
}

impl<T: ClientTransport> ReqSocket<T> {
    /// Connects to the target with the default options.
    pub async fn connect(&mut self, target: &str) -> Result<(), ReqError> {
        // Initialize communication channels
        let (to_backend, from_socket) = mpsc::channel(DEFAULT_BUFFER_SIZE);

        // TODO: parse target string to get transport protocol, for now just assume TCP

        // TODO: exponential backoff, should be handled in the `Durable` versions of our transports
        let stream = if self.options.retry_on_initial_failure {
            let mut attempts = 0;
            loop {
                match self.transport.connect(target).await {
                    Ok(stream) => break stream,
                    Err(e) => {
                        attempts += 1;
                        tracing::debug!(
                            "Failed to connect to target, retrying: {} (attempt {})",
                            e,
                            attempts
                        );

                        if let Some(max_attempts) = self.options.retry_attempts {
                            if attempts >= max_attempts {
                                return Err(ReqError::Transport(Box::new(e)));
                            }
                        }

                        tokio::time::sleep(self.options.backoff_duration).await;
                    }
                }
            }
        } else {
            self.transport
                .connect(target)
                .await
                .map_err(|e| ReqError::Transport(Box::new(e)))?
        };

        tracing::debug!("Connected to {}", target);

        // Create the socket backend
        let backend = ReqBackend {
            options: Arc::clone(&self.options),
            id_counter: 0,
            from_socket,
            conn: Framed::new(stream, reqrep::Codec::new()),
            egress_queue: VecDeque::new(),
            // TODO: we should limit the amount of active outgoing requests, and that should be the capacity.
            // If we do this, we'll never have to re-allocate.
            active_requests: FxHashMap::default(),
        };

        // Spawn the backend task
        tokio::spawn(backend);

        self.to_backend = Some(to_backend);

        Ok(())
    }
}
