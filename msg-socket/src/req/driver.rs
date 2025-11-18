use std::{
    collections::VecDeque,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
    time::{Duration, Instant},
};

use bytes::Bytes;
use futures::{Future, FutureExt, SinkExt, StreamExt};
use rustc_hash::FxHashMap;
use tokio::{
    sync::{mpsc, oneshot},
    time::Interval,
};
use tokio_util::codec::Framed;
use tracing::{debug, error, trace};

use super::{Command, ReqError, ReqOptions};
use crate::{ConnectionState, ExponentialBackoff, req::SocketState};

use msg_transport::{Address, MeteredIo, PeerAddress, Transport};
use msg_wire::{
    auth,
    compression::{Compressor, try_decompress_payload},
    reqrep,
};

/// A connection task that connects to a server and returns the underlying IO object.
type ConnectionTask<Io, Err> = Pin<Box<dyn Future<Output = Result<Io, Err>> + Send>>;

/// A connection controller that manages the connection to a server with an exponential backoff.
type ConnectionCtl<Io, S, A> =
    ConnectionState<Framed<MeteredIo<Io, S, A>, reqrep::Codec>, ExponentialBackoff, A>;

/// The request socket driver. Endless future that drives
/// the socket forward.
pub(crate) struct ReqDriver<T: Transport<A>, A: Address> {
    /// Options shared with the socket.
    pub(crate) options: Arc<ReqOptions>,
    /// State shared with the socket.
    pub(crate) socket_state: SocketState<T::Stats>,
    /// ID counter for outgoing requests.
    pub(crate) id_counter: u32,
    /// Commands from the socket.
    pub(crate) from_socket: mpsc::Receiver<Command>,
    /// The transport for this socket.
    pub(crate) transport: T,
    /// The address of the server.
    pub(crate) addr: A,
    /// The connection task which handles the connection to the server.
    pub(crate) conn_task: Option<ConnectionTask<T::Io, T::Error>>,
    /// The transport controller, wrapped in a [`ConnectionState`] for backoff.
    /// The [`Framed`] object can send and receive messages from the socket.
    pub(crate) conn_state: ConnectionCtl<T::Io, T::Stats, A>,
    /// The outgoing message queue.
    pub(crate) egress_queue: VecDeque<reqrep::Message>,
    /// The currently pending requests, if any. Uses [`FxHashMap`] for performance.
    pub(crate) pending_requests: FxHashMap<u32, PendingRequest>,
    /// Interval for checking for request timeouts.
    pub(crate) timeout_check_interval: Interval,
    /// Interval for flushing the connection. This is secondary to `should_flush`.
    pub(crate) flush_interval: Option<tokio::time::Interval>,
    /// Whether or not the connection should be flushed
    pub(crate) should_flush: bool,
    /// Optional message compressor. This is shared with the socket to keep
    /// the API consistent with other socket types (e.g. `PubSocket`)
    pub(crate) compressor: Option<Arc<dyn Compressor>>,
}

/// A pending request that is waiting for a response.
pub(crate) struct PendingRequest {
    /// The timestamp when the request was sent.
    start: Instant,
    /// The response sender.
    sender: oneshot::Sender<Result<Bytes, ReqError>>,
}

impl<T, A> ReqDriver<T, A>
where
    T: Transport<A> + Send + Sync + 'static,
    A: Address,
{
    /// Start the connection task to the server, handling authentication if necessary.
    /// The result will be polled by the driver and re-tried according to the backoff policy.
    fn try_connect(&mut self, addr: A) {
        trace!("Trying to connect to {:?}", addr);

        let connect = self.transport.connect(addr.clone());
        let token = self.options.auth_token.clone();

        self.conn_task = Some(Box::pin(async move {
            let mut io = match connect.await {
                Ok(io) => io,
                Err(e) => {
                    error!(err = ?e, "Failed to connect to {:?}", addr);
                    return Err(e);
                }
            };

            // Perform the authentication handshake
            if let Some(token) = token {
                let mut conn = Framed::new(&mut io, auth::Codec::new_client());

                debug!("Sending auth message: {:?}", token);
                conn.send(auth::Message::Auth(token)).await?;
                conn.flush().await?;

                debug!("Waiting for ACK from server...");

                // Wait for the response
                match conn.next().await {
                    Some(res) => match res {
                        Ok(auth::Message::Ack) => {
                            debug!("Connected to {:?}", addr);
                            Ok(io)
                        }
                        Ok(msg) => {
                            error!(?msg, "Unexpected auth ACK result");
                            Err(io::Error::new(io::ErrorKind::PermissionDenied, "rejected").into())
                        }
                        Err(e) => Err(io::Error::new(io::ErrorKind::PermissionDenied, e).into()),
                    },
                    None => {
                        error!("Connection closed while waiting for ACK");
                        Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Connection closed")
                            .into())
                    }
                }
            } else {
                debug!("Connected to {:?}", addr);
                Ok(io)
            }
        }));
    }

    /// Handle an incoming message from the connection.
    fn on_message(&mut self, msg: reqrep::Message) {
        if let Some(pending) = self.pending_requests.remove(&msg.id()) {
            let rtt = pending.start.elapsed().as_micros() as usize;

            let size = msg.size();
            let compression_type = msg.header().compression_type();
            let mut payload = msg.into_payload();

            // decompress the response
            match try_decompress_payload(compression_type, payload) {
                Ok(decompressed) => payload = decompressed,
                Err(e) => {
                    error!(err = ?e, "Failed to decompress response payload");
                    let _ = pending.sender.send(Err(ReqError::Wire(reqrep::Error::Decompression)));
                    return;
                }
            }

            let _ = pending.sender.send(Ok(payload));

            // Update stats
            self.socket_state.stats.specific.update_rtt(rtt);
            self.socket_state.stats.specific.increment_rx(size);
        }
    }

    /// Handle an incoming command from the socket frontend.
    fn on_command(&mut self, cmd: Command) {
        match cmd {
            Command::Send { mut message, response } => {
                let start = std::time::Instant::now();

                // Compress the message if it's larger than the minimum size
                let len_before = message.payload().len();
                if len_before > self.options.min_compress_size {
                    if let Some(ref compressor) = self.compressor {
                        if let Err(e) = message.compress(compressor.as_ref()) {
                            error!(err = ?e, "Failed to compress message");
                        }

                        debug!(
                            "Compressed message from {} to {} bytes",
                            len_before,
                            message.payload().len()
                        );
                    }
                }

                let msg = message.into_wire(self.id_counter);
                let msg_id = msg.id();
                self.id_counter = self.id_counter.wrapping_add(1);
                self.egress_queue.push_back(msg);
                self.pending_requests.insert(msg_id, PendingRequest { start, sender: response });
            }
        }
    }

    /// Check for request timeouts and notify the sender if any requests have timed out.
    /// This is done periodically by the driver.
    fn check_timeouts(&mut self) {
        let now = Instant::now();

        let timed_out_ids = self
            .pending_requests
            .iter()
            .filter_map(|(&id, request)| {
                if now.duration_since(request.start) > self.options.timeout {
                    Some(id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        for id in timed_out_ids {
            if let Some(pending_request) = self.pending_requests.remove(&id) {
                let _ = pending_request.sender.send(Err(ReqError::Timeout));
            }
        }
    }

    /// Check if the connection should be flushed.
    #[inline]
    fn should_flush(&mut self, cx: &mut Context<'_>) -> bool {
        if self.should_flush {
            if let Some(interval) = self.flush_interval.as_mut() {
                interval.poll_tick(cx).is_ready()
            } else {
                true
            }
        } else {
            // If we shouldn't flush, reset the interval so we don't get woken up
            // every time the interval expires
            if let Some(interval) = self.flush_interval.as_mut() {
                interval.reset()
            }

            false
        }
    }

    /// Reset the connection state to inactive, so that it will be re-tried.
    /// This is done when the connection is closed or an error occurs.
    #[inline]
    fn reset_connection(&mut self) {
        self.conn_state = ConnectionState::Inactive {
            addr: self.addr.clone(),
            backoff: ExponentialBackoff::new(Duration::from_millis(20), 16),
        };
    }
}

impl<T, A> Future for ReqDriver<T, A>
where
    T: Transport<A> + Unpin + Send + Sync + 'static,
    A: Address,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            // Try to flush pending messages
            if this.should_flush(cx) {
                if let ConnectionState::Active { ref mut channel } = this.conn_state {
                    if let Poll::Ready(Ok(_)) = channel.poll_flush_unpin(cx) {
                        this.should_flush = false;
                    }
                }
            }

            // Poll the active connection task, if any
            if let Some(ref mut conn_task) = this.conn_task {
                if let Poll::Ready(result) = conn_task.poll_unpin(cx) {
                    // As soon as the connection task finishes, set it to `None`.
                    // - If it was successful, set the connection to active
                    // - If it failed, it will be re-tried until the backoff limit is reached.
                    this.conn_task = None;

                    if let Ok(io) = result {
                        tracing::debug!(target = ?io.peer_addr(), "new connection");

                        let tx = this.socket_state.transport.0.clone();
                        let metered = MeteredIo::new(io, tx);

                        let mut framed = Framed::new(metered, reqrep::Codec::new());
                        framed.set_backpressure_boundary(this.options.backpressure_boundary);
                        this.conn_state = ConnectionState::Active { channel: framed };
                    }
                }
            }

            // If the connection is inactive, try to connect to the server
            // or poll the backoff timer if we're already trying to connect.
            if let ConnectionState::Inactive { ref mut backoff, ref addr } = this.conn_state {
                if let Poll::Ready(item) = backoff.poll_next_unpin(cx) {
                    if let Some(duration) = item {
                        if this.conn_task.is_none() {
                            debug!(backoff = ?duration, "Retrying connection to {:?}", addr);
                            this.try_connect(addr.clone());
                        } else {
                            debug!(backoff = ?duration, "Not retrying connection to {:?} as there is already a connection task", addr);
                        }
                    } else {
                        error!(
                            "Exceeded maximum number of retries for {:?}, terminating connection",
                            addr
                        );

                        return Poll::Ready(());
                    }
                }

                return Poll::Pending;
            }

            // If there is no active connection, continue polling the backoff
            let ConnectionState::Active { ref mut channel } = this.conn_state else {
                continue;
            };

            // Check for incoming messages from the socket
            match channel.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(msg))) => {
                    this.on_message(msg);

                    continue;
                }
                Poll::Ready(Some(Err(err))) => {
                    if let reqrep::Error::Io(e) = err {
                        error!(err = ?e, "Socket wire error");
                    }

                    // set the connection to inactive, so that it will be re-tried
                    this.reset_connection();

                    continue;
                }
                Poll::Ready(None) => {
                    debug!("Connection to {:?} closed, shutting down driver", this.addr);

                    return Poll::Ready(());
                }
                Poll::Pending => {}
            }

            // Check for outgoing messages to the socket
            if channel.poll_ready_unpin(cx).is_ready() {
                // Drain the egress queue
                if let Some(msg) = this.egress_queue.pop_front() {
                    // Generate the new message
                    let size = msg.size();
                    debug!("Sending msg {}", msg.id());
                    match channel.start_send_unpin(msg) {
                        Ok(_) => {
                            this.socket_state.stats.specific.increment_tx(size);
                            this.should_flush = true;
                        }
                        Err(e) => {
                            error!(err = ?e, "Failed to send message to socket");

                            // set the connection to inactive, so that it will be re-tried
                            this.reset_connection();
                        }
                    }

                    continue;
                }
            }

            // Check for request timeouts
            while this.timeout_check_interval.poll_tick(cx).is_ready() {
                this.check_timeouts();
            }

            // Check for outgoing messages from the socket handle
            match this.from_socket.poll_recv(cx) {
                Poll::Ready(Some(cmd)) => {
                    this.on_command(cmd);

                    continue;
                }
                Poll::Ready(None) => {
                    debug!("Socket dropped, shutting down backend and flushing connection");

                    if let ConnectionState::Active { ref mut channel } = this.conn_state {
                        let _ = ready!(channel.poll_close_unpin(cx));
                    };

                    return Poll::Ready(());
                }
                Poll::Pending => {}
            }

            return Poll::Pending;
        }
    }
}
