use std::{collections::VecDeque, sync::Arc};

use bytes::Bytes;
use futures::SinkExt;
use msg_transport::ClientTransport;
use msg_wire::{auth, reqrep};
use rustc_hash::FxHashMap;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;

use crate::req::stats::SocketStats;

use super::{Command, ReqDriver, ReqError, ReqOptions, DEFAULT_BUFFER_SIZE};

#[derive(Debug, Clone)]
pub struct ReqSocket<T: ClientTransport> {
    /// Command channel to the backend task.
    to_driver: Option<mpsc::Sender<Command>>,
    /// The underlying transport.
    transport: T,
    /// Options for the socket. These are shared with the backend task.
    options: Arc<ReqOptions>,
    /// Socket statistics. These are shared with the backend task.
    stats: Arc<SocketStats>,
}

impl<T: ClientTransport> ReqSocket<T> {
    pub fn new(transport: T) -> Self {
        Self::new_with_options(transport, ReqOptions::default())
    }

    pub fn new_with_options(transport: T, options: ReqOptions) -> Self {
        Self {
            to_driver: None,
            transport,
            options: Arc::new(options),
            stats: Arc::new(SocketStats::default()),
        }
    }

    pub fn stats(&self) -> &SocketStats {
        &self.stats
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

    /// Authenticates the client to the server.
    async fn authenticate(&self, id: Bytes, stream: T::Io) -> Result<T::Io, ReqError> {
        let mut conn = Framed::new(stream, auth::Codec::new_client());

        tracing::debug!("Sending auth message: {:?}", id);
        // Send the authentication message
        let auth_msg = auth::Message::Auth(id);
        conn.send(auth_msg).await?;
        conn.flush().await?;

        tracing::debug!("Waiting for ACK");
        // Wait for the response
        let ack = conn
            .next()
            .await
            .ok_or(ReqError::SocketClosed)?
            .map_err(|e| ReqError::Auth(e.to_string()))?;

        if matches!(ack, auth::Message::Ack) {
            Ok(conn.into_inner())
        } else {
            Err(ReqError::Auth("Invalid ACK".to_string()))
        }
    }

    /// Connects to the target with the default options.
    pub async fn connect(&mut self, target: &str) -> Result<(), ReqError> {
        // Initialize communication channels
        let (to_driver, from_socket) = mpsc::channel(DEFAULT_BUFFER_SIZE);

        // TODO: parse target string to get transport protocol, for now just assume TCP

        // TODO: exponential backoff, should be handled in the `Durable` versions of our transports
        let mut stream = if self.options.retry_on_initial_failure {
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

        if let Some(ref id) = self.options.client_id {
            stream = self.authenticate(id.clone(), stream).await?;
        }

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
            stats: Arc::clone(&self.stats),
        };

        // Spawn the backend task
        tokio::spawn(driver);

        self.to_driver = Some(to_driver);

        Ok(())
    }
}
