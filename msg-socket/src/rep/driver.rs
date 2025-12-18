use std::{
    collections::VecDeque,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
};

use bytes::Bytes;
use futures::{Future, FutureExt, SinkExt, Stream, StreamExt, stream::FuturesUnordered};
use msg_common::span::{EnterSpan, SpanExt as _, WithSpan};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{
        mpsc,
        oneshot::{self, error::RecvError},
    },
    task::JoinSet,
    time::Interval,
};
use tokio_stream::{StreamMap, StreamNotifyClose};
use tokio_util::codec::Framed;
use tracing::{debug, error, info, trace, warn};

use crate::{AuthResult, Authenticator, RepOptions, Request, rep::SocketState};

use msg_transport::{Address, PeerAddress, Transport};
use msg_wire::{
    auth,
    compression::{Compressor, try_decompress_payload},
    reqrep,
};

use super::RepError;

/// The bytes of a "PING" message. They can be used to be matched on an incoming request and send a
/// response immediately, like an healthcheck.
const PING: &[u8; 4] = b"PING";
/// The bytes of a "PONG" response. See [`PING`].
const PONG: &[u8; 4] = b"PONG";

/// An object that represents a connected peer and associated state.
///
/// # Usage of Framed
/// [`Framed`] is used for encoding and decoding messages ("frames").
/// Usually, [`Framed`] has its own internal buffering mechanism, that's respected
/// when calling `poll_ready` and configured by [`Framed::set_backpressure_boundary`].
///
/// However, we don't use `poll_ready` here, and instead we flush whenever the write buffer contains
/// data (i.e., every time we write a message to it).
pub(crate) struct PeerState<T: AsyncRead + AsyncWrite, A: Address> {
    pending_requests: FuturesUnordered<WithSpan<PendingRequest>>,
    conn: Framed<T, reqrep::Codec>,
    /// The timer for the write buffer linger.
    linger_timer: Option<Interval>,
    write_buffer_size: usize,
    /// The address of the peer.
    addr: A,
    egress_queue: VecDeque<WithSpan<reqrep::Message>>,
    state: Arc<SocketState>,
    /// The optional message compressor.
    compressor: Option<Arc<dyn Compressor>>,
    span: tracing::Span,
}

#[allow(clippy::type_complexity)]
pub(crate) struct RepDriver<T: Transport<A>, A: Address> {
    /// The server transport used to accept incoming connections.
    pub(crate) transport: T,
    /// The reply socket state, shared with the socket front-end.
    pub(crate) state: Arc<SocketState>,
    #[allow(unused)]
    /// Options shared with socket.
    pub(crate) options: Arc<RepOptions>,
    /// [`StreamMap`] of connected peers. The key is the peer's address.
    pub(crate) peer_states: StreamMap<A, StreamNotifyClose<PeerState<T::Io, A>>>,
    /// Sender to the socket front-end. Used to notify the socket of incoming requests.
    pub(crate) to_socket: mpsc::Sender<Request<A>>,
    /// Optional connection authenticator.
    pub(crate) auth: Option<Arc<dyn Authenticator>>,
    /// Optional message compressor. This is shared with the socket to keep
    /// the API consistent with other socket types (e.g. `PubSocket`)
    pub(crate) compressor: Option<Arc<dyn Compressor>>,
    /// A set of pending incoming connections, represented by [`Transport::Accept`].
    pub(crate) conn_tasks: FuturesUnordered<WithSpan<T::Accept>>,
    /// A joinset of authentication tasks.
    pub(crate) auth_tasks: JoinSet<WithSpan<Result<AuthResult<T::Io, A>, RepError>>>,

    /// A span to use for general purpose notifications, not tied to a specific path.
    pub(crate) span: tracing::Span,
}

impl<T, A> Future for RepDriver<T, A>
where
    T: Transport<A>,
    A: Address,
{
    type Output = Result<(), RepError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            if let Poll::Ready(Some((peer, maybe_result))) = this.peer_states.poll_next_unpin(cx) {
                let Some(result) = maybe_result.enter() else {
                    debug!(?peer, "peer disconnected");
                    this.state.stats.specific.decrement_active_clients();
                    continue;
                };

                match result.inner {
                    Ok(mut request) => {
                        debug!("received request");

                        let size = request.msg().len();

                        match try_decompress_payload(request.compression_type, request.msg) {
                            Ok(decompressed) => request.msg = decompressed,
                            Err(e) => {
                                debug!(?e, "failed to decompress message");
                                continue;
                            }
                        }

                        this.state.stats.specific.increment_rx(size);
                        if let Err(e) = this.to_socket.try_send(request) {
                            error!(?e, ?peer, "failed to send to socket, dropping request");
                        };
                    }
                    Err(e) => {
                        if e.is_connection_reset() {
                            trace!(?peer, "connection reset")
                        } else {
                            error!(?e, ?peer, "failed to receive message from peer");
                        }
                    }
                }

                continue;
            }

            if let Poll::Ready(Some(Ok(auth))) = this.auth_tasks.poll_join_next(cx).enter() {
                match auth.inner {
                    Ok(auth) => {
                        // Run custom authenticator
                        info!(id = ?auth.id, "passed");

                        let conn = Framed::new(auth.stream, reqrep::Codec::new());

                        let span = tracing::info_span!(parent: this.span.clone(), "peer", addr = ?auth.addr);

                        let linger_timer = this.options.write_buffer_linger.map(|duration| {
                            let mut timer = tokio::time::interval(duration);
                            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                            timer
                        });

                        this.peer_states.insert(
                            auth.addr.clone(),
                            StreamNotifyClose::new(PeerState {
                                span,
                                pending_requests: FuturesUnordered::new(),
                                conn,
                                linger_timer,
                                write_buffer_size: this.options.write_buffer_size,
                                addr: auth.addr,
                                egress_queue: VecDeque::with_capacity(128),
                                state: Arc::clone(&this.state),
                                compressor: this.compressor.clone(),
                            }),
                        );
                    }
                    Err(e) => {
                        debug!(?e, "failed to authenticate peer");
                        this.state.stats.specific.decrement_active_clients();
                    }
                }

                continue;
            }

            if let Poll::Ready(Some(conn)) = this.conn_tasks.poll_next_unpin(cx).enter() {
                match conn.inner {
                    Ok(io) => {
                        if let Err(e) = this.on_accepted_connection(io) {
                            error!(?e, "failed to handle accepted connection");
                            this.state.stats.specific.decrement_active_clients();
                        }
                    }
                    Err(e) => {
                        debug!(?e, "failed to accept incoming connection");

                        // Active clients have already been incremented in the initial call to
                        // `poll_accept`, so we need to decrement them here.
                        this.state.stats.specific.decrement_active_clients();
                    }
                }

                continue;
            }

            // Finally, poll the transport for new incoming connection futures and push them to the
            // incoming connection tasks.
            if let Poll::Ready(accept) = Pin::new(&mut this.transport).poll_accept(cx) {
                let span = this.span.clone().entered();

                if let Some(max) = this.options.max_clients {
                    if this.state.stats.specific.active_clients() >= max {
                        warn!(
                            limit = max,
                            "max connections reached, rejecting new incoming connection",
                        );

                        continue;
                    }
                }

                // Increment the active clients counter. If the authentication fails, this counter
                // will be decremented.
                this.state.stats.specific.increment_active_clients();

                this.conn_tasks.push(accept.with_span(span));

                continue;
            }

            return Poll::Pending;
        }
    }
}

impl<T, A> RepDriver<T, A>
where
    T: Transport<A>,
    A: Address,
{
    /// Handles an accepted connection. If this returns an error, the active connections counter
    /// should be decremented.
    fn on_accepted_connection(&mut self, io: T::Io) -> Result<(), io::Error> {
        let addr = io.peer_addr()?;
        info!(?addr, "new connection");

        let linger_timer = self.options.write_buffer_linger.map(|duration| {
            let mut timer = tokio::time::interval(duration);
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            timer
        });

        let Some(ref auth) = self.auth else {
            self.peer_states.insert(
                addr.clone(),
                StreamNotifyClose::new(PeerState {
                    span: tracing::info_span!("peer", ?addr),
                    pending_requests: FuturesUnordered::new(),
                    conn: Framed::new(io, reqrep::Codec::new()),
                    linger_timer,
                    write_buffer_size: self.options.write_buffer_size,
                    addr,
                    egress_queue: VecDeque::with_capacity(128),
                    state: Arc::clone(&self.state),
                    compressor: self.compressor.clone(),
                }),
            );

            return Ok(());
        };

        // start the authentication process
        let authenticator = Arc::clone(auth);
        let span = tracing::info_span!("auth", ?addr);

        let fut = async move {
            let mut conn = Framed::new(io, auth::Codec::new_server());

            // Wait for the response
            let token = conn
                .next()
                .await
                .ok_or(RepError::SocketClosed)?
                .map_err(|e| RepError::Auth(e.to_string()))?;

            debug!(?token, "auth received");

            let auth::Message::Auth(id) = token else {
                conn.send(auth::Message::Reject).await?;
                conn.close().await?;
                return Err(RepError::Auth("invalid auth message".to_string()));
            };

            // If authentication fails, send a reject message and close the connection
            if !authenticator.authenticate(&id) {
                conn.send(auth::Message::Reject).await?;
                conn.close().await?;
                return Err(RepError::Auth("authentication failed".to_string()));
            }

            // Send ack
            conn.send(auth::Message::Ack).await?;

            Ok(AuthResult { id, addr, stream: conn.into_inner() })
        };

        self.auth_tasks.spawn(fut.with_span(span));

        Ok(())
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin, A: Address> PeerState<T, A> {
    /// Prepares for shutting down by sending and flushing all messages in [`Self::egress_queue`].
    /// When [`Poll::Ready`] is returned, the connection with this peer can be shutdown.
    ///
    /// TODO: there might be some [`Self::pending_requests`] yet to processed. TBD how to handle
    /// them, for now they're dropped.
    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let messages = std::mem::take(&mut self.egress_queue);
        let buffer_size = self.conn.write_buffer().len();
        if messages.is_empty() && buffer_size == 0 {
            debug!("flushed everything, closing connection");
            return Poll::Ready(());
        }

        debug!(messages = ?messages.len(), write_buffer_size = ?buffer_size, "found data to send");

        for msg in messages {
            if let Err(e) = self.conn.start_send_unpin(msg.inner) {
                error!(?e, "failed to send final messages to socket, closing");
                return Poll::Ready(());
            }
        }

        if let Err(e) = ready!(self.conn.poll_flush_unpin(cx)) {
            error!(?e, "failed to flush on shutdown, giving up");
        }

        Poll::Ready(())
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin, A: Address + Unpin> Stream for PeerState<T, A> {
    type Item = WithSpan<Result<Request<A>, RepError>>;

    /// Advances the state of the peer. Tries to poll all possible sources of progress equally.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            let mut progress = false;
            if let Some(msg) = this.egress_queue.pop_front().enter() {
                let msg_len = msg.size();
                match this.conn.start_send_unpin(msg.inner) {
                    Ok(_) => {
                        this.state.stats.specific.increment_tx(msg_len);

                        // We might be able to send more queued messages
                        progress = true;
                    }
                    Err(e) => {
                        this.state.stats.specific.increment_failed_requests();
                        error!(err = ?e, peer = ?this.addr, "failed to send message to socket, closing...");
                        // End this stream as we can't send any more messages
                        return Poll::Ready(None);
                    }
                }
            }

            // Try to flush the connection if any data was written to the buffer.
            if this.conn.write_buffer().len() >= this.write_buffer_size {
                if let Poll::Ready(Err(e)) = this.conn.poll_flush_unpin(cx) {
                    error!(err = ?e, peer = ?this.addr, "failed to flush connection, closing...");
                    return Poll::Ready(None);
                }

                if let Some(ref mut linger_timer) = this.linger_timer {
                    // Reset the linger timer.
                    linger_timer.reset();
                }
            }

            if let Some(ref mut linger_timer) = this.linger_timer {
                if !this.conn.write_buffer().is_empty() && linger_timer.poll_tick(cx).is_ready() {
                    if let Poll::Ready(Err(e)) = this.conn.poll_flush_unpin(cx) {
                        error!(err = ?e, peer = ?this.addr, "failed to flush connection, closing...");
                        return Poll::Ready(None);
                    }
                }
            }

            // Then, try to drain the egress queue.
            if this.conn.poll_ready_unpin(cx).is_ready() {
                if let Some(msg) = this.egress_queue.pop_front().enter() {
                    let msg_len = msg.size();

                    debug!(msg_id = msg.id(), "sending response");
                    match this.conn.start_send_unpin(msg.inner) {
                        Ok(_) => {
                            this.state.stats.specific.increment_tx(msg_len);

                            // We might be able to send more queued messages
                            continue;
                        }
                        Err(e) => {
                            this.state.stats.specific.increment_failed_requests();
                            error!(?e, "failed to send message to socket");
                            // End this stream as we can't send any more messages
                            return Poll::Ready(None);
                        }
                    }
                }
            }

            // Then we check for completed requests, and push them onto the egress queue.
            if let Poll::Ready(Some(result)) = this.pending_requests.poll_next_unpin(cx).enter() {
                match result.inner {
                    Err(_) => tracing::error!("response channel closed unexpectedly"),
                    Ok(Response { msg_id, mut response }) => {
                        let mut compression_type = 0;
                        let len_before = response.len();
                        if let Some(ref compressor) = this.compressor {
                            match compressor.compress(&response) {
                                Ok(compressed) => {
                                    response = compressed;
                                    compression_type = compressor.compression_type() as u8;
                                }
                                Err(e) => {
                                    error!(?e, "failed to compress message");
                                    continue;
                                }
                            }

                            debug!(
                                msg_id,
                                len_before,
                                len_after = response.len(),
                                "compressed message"
                            )
                        }

                        debug!(msg_id, "received response to send");

                        let msg = reqrep::Message::new(msg_id, compression_type, response);
                        this.egress_queue.push_back(msg.with_span(result.span));

                        continue;
                    }
                }
            }

            // Finally we accept incoming requests from the peer.
            {
                let _g = this.span.clone().entered();
                match this.conn.poll_next_unpin(cx) {
                    Poll::Ready(Some(result)) => {
                        let span = tracing::info_span!("request").entered();

                        trace!(?result, "received message");
                        let msg = match result {
                            Ok(m) => m,
                            Err(e) => {
                                return Poll::Ready(Some(Err(e.into()).with_span(span.clone())));
                            }
                        };

                        // NOTE: for this special message type, send back immediately a response.
                        if msg.payload().as_ref() == PING {
                            debug!("received ping healthcheck, responding pong");

                            let msg = reqrep::Message::new(0, 0, PONG.as_ref().into());
                            if let Err(e) = this.conn.start_send_unpin(msg) {
                                error!(?e, "failed to send pong response");
                            }
                            continue;
                        }

                        let (tx, rx) = oneshot::channel();

                        // Add the pending request to the list
                        this.pending_requests.push(
                            PendingRequest { msg_id: msg.id(), response: rx }
                                .with_span(span.clone()),
                        );

                        let request = Request {
                            source: this.addr.clone(),
                            response: tx,
                            compression_type: msg.header().compression_type(),
                            msg: msg.into_payload(),
                        };

                        return Poll::Ready(Some(Ok(request).with_span(span)));
                    }
                    Poll::Ready(None) => {
                        debug!("framed closed, sending and flushing leftover data if any");

                        if this.poll_shutdown(cx).is_ready() {
                            return Poll::Ready(None);
                        }
                    }
                    Poll::Pending => {}
                }
            }

            if progress {
                continue;
            }

            return Poll::Pending;
        }
    }
}

struct PendingRequest {
    msg_id: u32,
    response: oneshot::Receiver<Bytes>,
}

/// A response to a [`PendingRequest`].
struct Response {
    msg_id: u32,
    response: Bytes,
}

impl Future for PendingRequest {
    type Output = Result<Response, RecvError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.response.poll_unpin(cx) {
            Poll::Ready(Ok(response)) => {
                Poll::Ready(Ok(Response { msg_id: self.msg_id, response }))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}
