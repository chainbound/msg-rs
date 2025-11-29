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
use msg_common::{
    bufwriter::{BufWriter, Strategy},
    span::{EnterSpan as _, SpanExt as _, WithSpan},
};
use rustc_hash::FxHashMap;
use tokio::{
    io::{ReadHalf, WriteHalf},
    sync::{mpsc, oneshot},
    time::Interval,
};
use tokio_util::codec::{Framed, FramedRead};
use tracing::Instrument;

use super::{ReqError, ReqOptions};
use crate::{
    ConnectionState, ExponentialBackoff, SendCommand, req::SocketState, state::SplitConnectionState,
};

use msg_transport::{Address, MeteredIo, Transport};
use msg_wire::{
    auth::{self},
    compression::{Compressor, try_decompress_payload},
    reqrep,
};

/// A connection task that connects to a server and returns the underlying IO object.
type ConnectionTask<Io, Err> = Pin<Box<dyn Future<Output = Result<Io, Err>> + Send>>;

/// A connection controller that manages the connection to a server with an exponential backoff.
///
/// # Usage of Framed
/// [`Framed`] is used for encoding and decoding messages ("frames").
/// Usually, [`Framed`] has its own internal buffering mechanism, that's respected
/// when calling `poll_ready` and configured by [`Framed::set_backpressure_boundary`].
///
/// However, we don't use `poll_ready` here, and instead we flush every time we write a message to
/// the framed buffer.
pub(crate) type ConnectionCtl<Io, S, A> = SplitConnectionState<
    FramedRead<ReadHalf<MeteredIo<Io, S, A>>, reqrep::Codec>,
    BufWriter<WriteHalf<MeteredIo<Io, S, A>>, reqrep::Message>,
    ExponentialBackoff,
    A,
>;

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
    pub(crate) from_socket: mpsc::Receiver<SendCommand>,
    /// The transport for this socket.
    pub(crate) transport: T,
    /// The address of the server.
    pub(crate) addr: A,
    /// The connection task which handles the connection to the server.
    pub(crate) conn_task: Option<WithSpan<ConnectionTask<T::Io, T::Error>>>,
    /// The transport controller, wrapped in a [`ConnectionState`] for backoff.
    /// The [`Framed`] object can send and receive messages from the socket.
    pub(crate) conn_state: ConnectionCtl<T::Io, T::Stats, A>,
    /// The outgoing message queue.
    pub(crate) egress_queue: VecDeque<WithSpan<reqrep::Message>>,
    /// The currently pending requests waiting for a response.
    pub(crate) pending_requests: FxHashMap<u32, WithSpan<PendingRequest>>,
    /// Interval for checking for request timeouts.
    pub(crate) timeout_check_interval: Interval,
    /// Optional message compressor. This is shared with the socket to keep
    /// the API consistent with other socket types (e.g. `PubSocket`)
    pub(crate) compressor: Option<Arc<dyn Compressor>>,

    /// An unique ID for this driver.
    pub(crate) id: usize,
    /// A span to use for general purpose notifications, not tied to a specific path.
    pub(crate) span: tracing::Span,
}

/// A pending request that is waiting for a response from the peer.
pub(crate) struct PendingRequest {
    /// The timestamp when the request was sent.
    start: Instant,
    /// The sender to send the peer response back to the user.
    sender: oneshot::Sender<Result<Bytes, ReqError>>,
}

/// Perform the authentication handshake with the server.
#[tracing::instrument(skip_all, "auth", fields(token = ?token))]
async fn authentication_handshake<T, A>(mut io: T::Io, token: Bytes) -> Result<T::Io, T::Error>
where
    T: Transport<A>,
    A: Address,
{
    let mut conn = Framed::new(&mut io, auth::Codec::new_client());

    conn.send(auth::Message::Auth(token)).await?;
    tracing::debug!("sent auth, waiting ack from server");

    // Wait for the response
    let Some(res) = conn.next().await else {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "connection closed").into());
    };

    match res {
        Ok(auth::Message::Ack) => {
            tracing::debug!("received ack");
            Ok(io)
        }
        Ok(msg) => {
            tracing::error!(?msg, "unexpected ack result");
            Err(io::Error::new(io::ErrorKind::PermissionDenied, "rejected").into())
        }
        Err(e) => Err(io::Error::new(io::ErrorKind::PermissionDenied, e).into()),
    }
}

impl<T, A> ReqDriver<T, A>
where
    T: Transport<A>,
    A: Address,
{
    /// Start the connection task to the server, handling authentication if necessary.
    /// The result will be polled by the driver and re-tried according to the backoff policy.
    fn try_connect(&mut self, addr: A) {
        let connect = self.transport.connect(addr.clone());
        let token = self.options.auth_token.clone();

        let task = async move {
            let io = connect.await?;

            let Some(token) = token else {
                return Ok(io);
            };

            authentication_handshake::<T, A>(io, token).await
        }
        .in_current_span();

        // FIX: coercion to BoxFuture for [`SpanExt::with_current_span`]
        self.conn_task = Some(WithSpan::current(Box::pin(task)));
    }

    /// Handle an incoming message from the connection.
    fn on_message(&mut self, msg: reqrep::Message) {
        let Some(pending) = self.pending_requests.remove(&msg.id()).enter() else {
            tracing::warn!(parent: &self.span, msg_id = msg.id(), "received response for unknown request id");
            return;
        };

        let rtt = pending.start.elapsed();
        tracing::debug!(msg_id = msg.id(), ?rtt, "received response");

        let size = msg.size();
        let compression_type = msg.header().compression_type();
        let mut payload = msg.into_payload();

        // decompress the response
        match try_decompress_payload(compression_type, payload) {
            Ok(decompressed) => payload = decompressed,
            Err(e) => {
                tracing::error!(?e, "failed to decompress response payload");
                let _ =
                    pending.inner.sender.send(Err(ReqError::Wire(reqrep::Error::Decompression)));
                return;
            }
        }

        if pending.inner.sender.send(Ok(payload)).is_err() {
            tracing::error!("failed to send peer response back, dropped receiver");
        }

        // Update stats
        self.socket_state.stats.specific.update_rtt(rtt.as_micros() as usize);
        self.socket_state.stats.specific.increment_rx(size);
    }

    /// Handle an incoming command from the socket frontend.
    fn on_send(&mut self, cmd: SendCommand) {
        let SendCommand { mut message, response } = cmd;
        let start = std::time::Instant::now();

        // We want ot inherit the span from the socket frontend
        let span =
            tracing::info_span!(parent: &message.span, "send", driver_id = format!("req-{}", self.id)).entered();

        // Compress the message if it's larger than the minimum size
        let size_before = message.payload().len();
        if size_before > self.options.min_compress_size {
            if let Some(ref compressor) = self.compressor {
                let start = Instant::now();
                if let Err(e) = message.compress(compressor.as_ref()) {
                    tracing::error!(?e, "failed to compress message");
                }

                tracing::debug!(
                    size_before,
                    size_after = message.payload().len(),
                    elapsed = ?start.elapsed(),
                    "compressed message",
                );
            }
        }

        let msg = message.inner.into_wire(self.id_counter);
        let msg_id = msg.id();
        self.id_counter = self.id_counter.wrapping_add(1);
        self.egress_queue.push_back(msg.with_span(span.clone()));
        self.pending_requests
            .insert(msg_id, PendingRequest { start, sender: response }.with_span(span));
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
                let _ = pending_request.into_inner().sender.send(Err(ReqError::Timeout));
            }
        }
    }

    /// Reset the connection state to inactive, so that it will be re-tried.
    /// This is done when the connection is closed or an error occurs.
    #[inline]
    fn reset_connection(&mut self) {
        self.conn_state = SplitConnectionState::Inactive {
            addr: self.addr.clone(),
            backoff: ExponentialBackoff::new(Duration::from_millis(20), 16),
        };
    }
}

impl<T, A> Future for ReqDriver<T, A>
where
    T: Transport<A>,
    A: Address,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let span = this.span.clone();

        loop {
            // TODO: Group connection management together in a function or at a different level of
            // abstraction.

            // Poll the active connection task, if any
            if let Some(ref mut conn_task) = this.conn_task {
                if let Poll::Ready(result) = conn_task.poll_unpin(cx).enter() {
                    // As soon as the connection task finishes, set it to `None`.
                    // - If it was successful, set the connection to active
                    // - If it failed, it will be re-tried until the backoff limit is reached.
                    this.conn_task = None;

                    match result.inner {
                        Ok(io) => {
                            tracing::info!("connected");

                            let metered =
                                MeteredIo::new(io, Arc::clone(&this.socket_state.transport_stats));

                            let (read, write) = tokio::io::split(metered);

                            let reader = FramedRead::new(read, reqrep::Codec::new());
                            // TODO: Config
                            let writer = BufWriter::with_strategy(
                                write,
                                this.options.buffer_strategy.clone(),
                            );

                            this.conn_state = SplitConnectionState::Active { reader, writer };
                        }
                        Err(e) => {
                            tracing::error!(?e, "failed to connect");
                        }
                    }
                }
            }

            // If the connection is inactive, try to connect to the server or poll the backoff
            // timer if we're already trying to connect.
            if let SplitConnectionState::Inactive { ref mut backoff, ref addr } = this.conn_state {
                let Poll::Ready(item) = backoff.poll_next_unpin(cx) else { return Poll::Pending };

                let _span = tracing::info_span!(parent: &this.span, "connect").entered();

                if let Some(duration) = item {
                    if this.conn_task.is_none() {
                        tracing::debug!(backoff = ?duration, "trying connection");
                        this.try_connect(addr.clone());
                    } else {
                        tracing::debug!(backoff = ?duration, "not retrying as there is already a connection task");
                    }
                } else {
                    tracing::error!("exceeded maximum number of retries, terminating connection");

                    return Poll::Ready(());
                }
            }

            // If there is no active connection, continue polling the backoff
            let SplitConnectionState::Active { ref mut reader, ref mut writer } = this.conn_state
            else {
                continue;
            };

            // Check for incoming messages from the socket
            match reader.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(msg))) => {
                    this.on_message(msg);

                    continue;
                }
                Poll::Ready(Some(Err(e))) => {
                    let _g = span.enter();
                    tracing::warn!(
                        ?e,
                        "failed to read from connection, resetting connection state"
                    );

                    // set the connection to inactive, so that it will be re-tried
                    this.reset_connection();

                    continue;
                }
                Poll::Ready(None) => {
                    let _g = span.enter();
                    tracing::warn!("connection closed, resetting connection state");

                    // set the connection to inactive, so that it will be re-tried
                    this.reset_connection();

                    continue;
                }
                Poll::Pending => {}
            }

            // NOTE: We try to drain the egress queue first (the `continue`), writing everything to
            // the `Framed` internal buffer. When all messages are written, we move on to flushing
            // the connection in the block below. We DO NOT rely on the `Framed` internal
            // backpressure boundary, because we do not call `poll_ready`.
            if writer.poll_ready_unpin(cx).is_ready() {
                if let Some(msg) = this.egress_queue.pop_front().enter() {
                    // Generate the new message
                    let size = msg.size();
                    tracing::debug!("Sending msg {}", msg.id());
                    // Write the message to the buffer.
                    match writer.start_send_unpin(msg.inner) {
                        Ok(_) => {
                            this.socket_state.stats.specific.increment_tx(size);
                        }
                        Err(e) => {
                            tracing::error!(err = ?e, "Failed to send message to socket");

                            // set the connection to inactive, so that it will be re-tried
                            this.reset_connection();
                        }
                    }

                    // We might be able to write more queued messages to the buffer.
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
                    this.on_send(cmd);

                    continue;
                }
                Poll::Ready(None) => {
                    tracing::debug!(
                        "socket dropped, shutting down backend and flushing connection"
                    );

                    if let SplitConnectionState::Active { ref mut writer, .. } = this.conn_state {
                        let _ = ready!(writer.poll_close_unpin(cx));
                    };

                    return Poll::Ready(());
                }
                Poll::Pending => {}
            }

            return Poll::Pending;
        }
    }
}
