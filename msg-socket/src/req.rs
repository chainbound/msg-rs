use std::{
    collections::VecDeque,
    pin::Pin,
    task::{ready, Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use futures::{Future, SinkExt, StreamExt};
use rustc_hash::FxHashMap;
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    sync::{mpsc, oneshot},
};
use tokio_util::codec::Framed;

const DEFAULT_BUFFER_SIZE: usize = 1024;

#[derive(Debug, Error)]
pub enum ReqError {
    #[error("IO error: {0:?}")]
    Io(#[from] std::io::Error),
    #[error("Wire protocol error: {0:?}")]
    Wire(#[from] msg_wire::reqrep::Error),
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

pub struct ReqSocket {
    to_backend: mpsc::Sender<Command>,
}

impl ReqSocket {
    pub async fn request(&self, message: Bytes) -> Result<Bytes, ReqError> {
        let (response_tx, response_rx) = oneshot::channel();

        // TODO: error handling
        self.to_backend
            .send(Command::Send {
                message,
                response: response_tx,
            })
            .await
            .unwrap();

        response_rx.await.unwrap()
    }
}

pub trait Transport: AsyncRead + AsyncWrite + Unpin + Sync + Send {}

pub struct ReqBackend<T: AsyncRead + AsyncWrite> {
    id_counter: u32,
    from_socket: mpsc::Receiver<Command>,
    conn: Framed<T, msg_wire::reqrep::Codec>,
    egress_queue: VecDeque<msg_wire::reqrep::Message>,
    /// The currently active request, if any. Uses [`FxHashMap`] for performance.
    active_requests: FxHashMap<u32, oneshot::Sender<Result<Bytes, ReqError>>>,
}

impl ReqSocket {
    /// Connects to the target with the default options.
    pub async fn connect(target: &str) -> Result<Self, ReqError> {
        Self::connect_with_options(target, ReqOptions::default()).await
    }

    pub async fn connect_with_options(target: &str, options: ReqOptions) -> Result<Self, ReqError> {
        // Initialize communication channels
        let (to_backend, from_socket) = mpsc::channel(DEFAULT_BUFFER_SIZE);

        // TODO: parse target string to get transport protocol, for now just assume TCP

        // TODO: exponential backoff, should be handled in the `Durable` versions of our transports
        let stream = if options.retry_on_initial_failure {
            let mut attempts = 0;
            loop {
                match TcpStream::connect(target).await {
                    Ok(stream) => break stream,
                    Err(e) => {
                        attempts += 1;
                        tracing::debug!(
                            "Failed to connect to target, retrying: {} (attempt {})",
                            e,
                            attempts
                        );

                        if let Some(max_attempts) = options.retry_attempts {
                            if attempts >= max_attempts {
                                return Err(e.into());
                            }
                        }

                        tokio::time::sleep(options.backoff_duration).await;
                    }
                }
            }
        } else {
            TcpStream::connect(target).await?
        };

        stream.set_nodelay(options.set_nodelay)?;
        tracing::debug!("Connected to {}", target);

        // Create the socket backend
        let backend = ReqBackend {
            id_counter: 0,
            from_socket,
            conn: Framed::new(stream, msg_wire::reqrep::Codec::new()),
            egress_queue: VecDeque::new(),
            // TODO: we should limit the amount of active outgoing requests, and that should be the capacity.
            // If we do this, we'll never have to re-allocate.
            active_requests: FxHashMap::with_capacity_and_hasher(64, Default::default()),
        };

        // Spawn the backend task
        tokio::spawn(backend);

        Ok(Self { to_backend })
    }
}

impl<T: AsyncRead + AsyncWrite> ReqBackend<T> {
    fn new_message(&mut self, payload: Bytes) -> msg_wire::reqrep::Message {
        let id = self.id_counter;
        // Wrap add here to avoid overflow
        self.id_counter = id.wrapping_add(1);

        msg_wire::reqrep::Message::new(id, payload)
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
