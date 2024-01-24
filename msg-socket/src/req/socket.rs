use bytes::Bytes;
use futures::SinkExt;
use msg_wire::compression::Compressor;
use rustc_hash::FxHashMap;
use std::{collections::VecDeque, io, sync::Arc, time::Duration};
use tokio::net::{lookup_host, ToSocketAddrs};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;

use msg_transport::Transport;
use msg_wire::{auth, reqrep};

use super::{Command, ReqDriver, ReqError, ReqOptions, DEFAULT_BUFFER_SIZE};
use crate::backoff::ExponentialBackoff;
use crate::ReqMessage;
use crate::{req::stats::SocketStats, req::SocketState};

/// The request socket.
pub struct ReqSocket<T: Transport> {
    /// Command channel to the backend task.
    to_driver: Option<mpsc::Sender<Command>>,
    /// The socket transport.
    transport: T,
    /// Options for the socket. These are shared with the backend task.
    options: Arc<ReqOptions>,
    /// Socket state. This is shared with the backend task.
    state: Arc<SocketState>,
    /// Optional message compressor. This is shared with the backend task.
    // NOTE: for now we're using dynamic dispatch, since using generics here
    // complicates the API a lot. We can always change this later for perf reasons.
    compressor: Option<Arc<dyn Compressor>>,
}

impl<T> ReqSocket<T>
where
    T: Transport + Send + Sync + Unpin + 'static,
{
    pub fn new(transport: T) -> Self {
        Self::with_options(transport, ReqOptions::default())
    }

    pub fn with_options(transport: T, options: ReqOptions) -> Self {
        Self {
            to_driver: None,
            transport,
            options: Arc::new(options),
            state: Arc::new(SocketState::default()),
            compressor: None,
        }
    }

    /// Sets the message compressor for this socket.
    pub fn with_compressor<C: Compressor + 'static>(mut self, compressor: C) -> Self {
        self.compressor = Some(Arc::new(compressor));
        self
    }

    pub fn stats(&self) -> &SocketStats {
        &self.state.stats
    }

    pub async fn request(&self, message: Bytes) -> Result<Bytes, ReqError> {
        let (response_tx, response_rx) = oneshot::channel();

        let msg = ReqMessage::new(message);

        self.to_driver
            .as_ref()
            .ok_or(ReqError::SocketClosed)?
            .send(Command::Send {
                message: msg,
                response: response_tx,
            })
            .await
            .map_err(|_| ReqError::SocketClosed)?;

        response_rx.await.map_err(|_| ReqError::SocketClosed)?
    }

    /// Connects to the target with the default options. WARN: this will wait until the connection can be established.
    pub async fn connect<A: ToSocketAddrs>(&mut self, addr: A) -> Result<(), ReqError> {
        let mut addrs = lookup_host(addr).await?;
        let endpoint = addrs.next().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "could not find any valid address",
            )
        })?;

        // Initialize communication channels
        let (to_driver, from_socket) = mpsc::channel(DEFAULT_BUFFER_SIZE);

        tracing::info!("Connecting to {}", endpoint);

        let mut backoff = ExponentialBackoff::new(Duration::from_millis(20), 16);

        let mut stream = loop {
            if let Some(duration) = backoff.next().await {
                match self
                    .transport
                    .connect(endpoint)
                    .await
                    .map_err(|e| ReqError::Transport(Box::new(e)))
                {
                    Ok(stream) => break stream,
                    Err(e) => {
                        tracing::warn!(backoff = ?duration, "Failed to connect to {}: {}, retrying...", endpoint, e);
                    }
                }
            } else {
                return Err(ReqError::Transport(Box::new(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Connection timed out",
                ))));
            }
        };

        if let Some(token) = self.options.auth_token.clone() {
            self.authenticate_stream(&mut stream, token).await?;
        }

        let mut framed = Framed::new(stream, reqrep::Codec::new());
        framed.set_backpressure_boundary(self.options.backpressure_boundary);

        // Create the socket backend
        let driver: ReqDriver<T> = ReqDriver {
            options: Arc::clone(&self.options),
            id_counter: 0,
            from_socket,
            conn: framed,
            egress_queue: VecDeque::new(),
            // TODO: we should limit the amount of active outgoing requests, and that should be the capacity.
            // If we do this, we'll never have to re-allocate.
            pending_requests: FxHashMap::default(),
            socket_state: Arc::clone(&self.state),
            timeout_check_interval: tokio::time::interval(Duration::from_millis(
                self.options.timeout.as_millis() as u64 / 10,
            )),
            flush_interval: self.options.flush_interval.map(tokio::time::interval),
            should_flush: false,
            compressor: self.compressor.clone(),
        };

        // Spawn the backend task
        tokio::spawn(driver);

        self.to_driver = Some(to_driver);

        Ok(())
    }

    async fn authenticate_stream(&self, io: &mut T::Io, token: Bytes) -> Result<(), ReqError> {
        let mut conn = Framed::new(io, auth::Codec::new_client());

        tracing::debug!("Sending auth message: {:?}", token);
        // Send the authentication message
        conn.send(auth::Message::Auth(token)).await?;
        conn.flush().await?;

        tracing::debug!("Waiting for ACK from server...");

        // Wait for the response
        let ack = conn
            .next()
            .await
            .ok_or(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Connection closed",
            ))?
            .map_err(|e| io::Error::new(io::ErrorKind::PermissionDenied, e))?;

        if matches!(ack, auth::Message::Ack) {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "Publisher denied connection",
            )
            .into())
        }
    }
}
