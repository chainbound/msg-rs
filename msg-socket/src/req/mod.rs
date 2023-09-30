use std::{
    collections::VecDeque,
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use futures::{Future, SinkExt, StreamExt};
use rustc_hash::FxHashMap;
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc, oneshot},
};
use tokio_util::codec::Framed;

use msg_transport::ClientTransport;
use msg_wire::reqrep;

const DEFAULT_BUFFER_SIZE: usize = 1024;

#[derive(Debug, Error)]
pub enum ReqError {
    #[error("IO error: {0:?}")]
    Io(#[from] std::io::Error),
    #[error("Wire protocol error: {0:?}")]
    Wire(#[from] reqrep::Error),
    #[error("Socket closed")]
    SocketClosed,
    #[error("Transport error: {0:?}")]
    Transport(#[from] Box<dyn std::error::Error + Send + Sync>),
}

pub enum Command {
    Send {
        message: Bytes,
        response: oneshot::Sender<Result<Bytes, ReqError>>,
    },
}

pub struct ReqOptions {
    pub timeout: std::time::Duration,
    pub retry_on_initial_failure: bool,
    pub backoff_duration: std::time::Duration,
    pub retry_attempts: Option<usize>,
    pub set_nodelay: bool,
}

impl Default for ReqOptions {
    fn default() -> Self {
        Self {
            timeout: std::time::Duration::from_secs(5),
            retry_on_initial_failure: true,
            backoff_duration: Duration::from_millis(200),
            retry_attempts: None,
            set_nodelay: true,
        }
    }
}

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

pub struct ReqBackend<T: AsyncRead + AsyncWrite> {
    options: Arc<ReqOptions>,
    id_counter: u32,
    from_socket: mpsc::Receiver<Command>,
    conn: Framed<T, reqrep::Codec>,
    egress_queue: VecDeque<reqrep::Message>,
    /// The currently active request, if any. Uses [`FxHashMap`] for performance.
    active_requests: FxHashMap<u32, oneshot::Sender<Result<Bytes, ReqError>>>,
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
            active_requests: FxHashMap::with_capacity_and_hasher(64, Default::default()),
        };

        // Spawn the backend task
        tokio::spawn(backend);

        self.to_backend = Some(to_backend);

        Ok(())
    }
}

impl<T: AsyncRead + AsyncWrite> ReqBackend<T> {
    fn new_message(&mut self, payload: Bytes) -> reqrep::Message {
        let id = self.id_counter;
        // Wrap add here to avoid overflow
        self.id_counter = id.wrapping_add(1);

        reqrep::Message::new(id, payload)
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> Future for ReqBackend<T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            let _ = this.conn.poll_flush_unpin(cx);

            // Check for incoming messages from the socket
            match this.conn.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(msg))) => {
                    if let Some(response) = this.active_requests.remove(&msg.id()) {
                        let _ = response.send(Ok(msg.into_payload()));
                    }

                    continue;
                }
                Poll::Ready(Some(Err(e))) => {
                    // TODO: this should contain the header ID so we can remove the request from the map
                    tracing::error!("Failed to read message from socket: {:?}", e);
                    continue;
                }
                Poll::Ready(None) => {
                    tracing::debug!("Socket closed, shutting down backend");
                    return Poll::Ready(());
                }
                Poll::Pending => {}
            }

            if this.conn.poll_ready_unpin(cx).is_ready() {
                // Drain the egress queue
                if let Some(msg) = this.egress_queue.pop_front() {
                    // Generate the new message
                    tracing::debug!("Sending msg {}", msg.id());
                    match this.conn.start_send_unpin(msg) {
                        Ok(_) => {
                            // We might be able to send more queued messages
                            continue;
                        }
                        Err(e) => {
                            tracing::error!("Failed to send message to socket: {:?}", e);
                            return Poll::Ready(());
                        }
                    }
                }
            }

            // Check for outgoing messages from the socket handle
            match this.from_socket.poll_recv(cx) {
                Poll::Ready(Some(Command::Send { message, response })) => {
                    // Queue the message for sending
                    let msg = this.new_message(message);
                    let id = msg.id();
                    this.egress_queue.push_back(msg);
                    this.active_requests.insert(id, response);

                    continue;
                }
                Poll::Ready(None) => {
                    tracing::debug!(
                        "Socket dropped, shutting down backend and flushing connection"
                    );
                    let _ = ready!(this.conn.poll_close_unpin(cx));
                    return Poll::Ready(());
                }
                Poll::Pending => {}
            }

            return Poll::Pending;
        }
    }
}
