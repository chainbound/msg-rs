use std::{
    collections::VecDeque,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::{Future, FutureExt, SinkExt, Stream, StreamExt, stream::FuturesUnordered};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc, oneshot},
    task::JoinSet,
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

pub(crate) struct PeerState<T: AsyncRead + AsyncWrite, A: Address> {
    pending_requests: FuturesUnordered<PendingRequest>,
    conn: Framed<T, reqrep::Codec>,
    addr: A,
    egress_queue: VecDeque<reqrep::Message>,
    state: Arc<SocketState>,
    should_flush: bool,
    compressor: Option<Arc<dyn Compressor>>,
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
    pub(super) conn_tasks: FuturesUnordered<T::Accept>,
    /// A joinset of authentication tasks.
    pub(crate) auth_tasks: JoinSet<Result<AuthResult<T::Io, A>, RepError>>,
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
            if let Poll::Ready(Some((peer, msg))) = this.peer_states.poll_next_unpin(cx) {
                match msg {
                    Some(Ok(mut request)) => {
                        debug!("Received request from peer {:?}", peer);

                        let size = request.msg().len();

                        // decompress the payload
                        match try_decompress_payload(request.compression_type, request.msg) {
                            Ok(decompressed) => request.msg = decompressed,
                            Err(e) => {
                                error!(err = ?e, "Failed to decompress message");
                                continue;
                            }
                        }

                        this.state.stats.specific.increment_rx(size);
                        let _ = this.to_socket.try_send(request);
                    }
                    Some(Err(e)) => {
                        error!(err = ?e, "Error receiving message from peer {:?}", peer);
                    }
                    None => {
                        warn!("Peer {:?} disconnected", peer);
                        this.state.stats.specific.decrement_active_clients();
                    }
                }

                continue;
            }

            if let Poll::Ready(Some(Ok(auth))) = this.auth_tasks.poll_join_next(cx) {
                match auth {
                    Ok(auth) => {
                        // Run custom authenticator
                        info!("Authentication passed for {:?} ({:?})", auth.id, auth.addr);

                        let mut conn = Framed::new(auth.stream, reqrep::Codec::new());
                        conn.set_backpressure_boundary(this.options.backpressure_boundary);

                        this.peer_states.insert(
                            auth.addr.clone(),
                            StreamNotifyClose::new(PeerState {
                                pending_requests: FuturesUnordered::new(),
                                conn,
                                addr: auth.addr,
                                egress_queue: VecDeque::with_capacity(128),
                                state: Arc::clone(&this.state),
                                should_flush: false,
                                compressor: this.compressor.clone(),
                            }),
                        );
                    }
                    Err(e) => {
                        error!(err = ?e, "Error authenticating client");
                        this.state.stats.specific.decrement_active_clients();
                    }
                }

                continue;
            }

            if let Poll::Ready(Some(incoming)) = this.conn_tasks.poll_next_unpin(cx) {
                match incoming {
                    Ok(io) => {
                        if let Err(e) = this.on_incoming(io) {
                            error!(err = ?e, "Error accepting incoming connection");
                            this.state.stats.specific.decrement_active_clients();
                        }
                    }
                    Err(e) => {
                        error!(err = ?e, "Error accepting incoming connection");

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
                if let Some(max) = this.options.max_clients {
                    if this.state.stats.specific.active_clients() >= max {
                        warn!(
                            "Max connections reached ({}), rejecting new incoming connection",
                            max
                        );

                        continue;
                    }
                }

                // Increment the active clients counter. If the authentication fails, this counter
                // will be decremented.
                this.state.stats.specific.increment_active_clients();

                this.conn_tasks.push(accept);

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
    /// Handles an incoming connection. If this returns an error, the active connections counter
    /// should be decremented.
    fn on_incoming(&mut self, io: T::Io) -> Result<(), io::Error> {
        let addr = io.peer_addr()?;

        info!("New connection from {:?}", addr);

        // If authentication is enabled, start the authentication process
        if let Some(ref auth) = self.auth {
            let authenticator = Arc::clone(auth);
            debug!("New connection from {:?}, authenticating", addr);
            self.auth_tasks.spawn(async move {
                let mut conn = Framed::new(io, auth::Codec::new_server());

                debug!("Waiting for auth");

                // Wait for the response
                let auth = conn
                    .next()
                    .await
                    .ok_or(RepError::SocketClosed)?
                    .map_err(|e| RepError::Auth(e.to_string()))?;

                debug!("Auth received: {:?}", auth);

                let auth::Message::Auth(id) = auth else {
                    conn.send(auth::Message::Reject).await?;
                    conn.flush().await?;
                    conn.close().await?;
                    return Err(RepError::Auth("Invalid auth message".to_string()));
                };

                // If authentication fails, send a reject message and close the connection
                if !authenticator.authenticate(&id) {
                    conn.send(auth::Message::Reject).await?;
                    conn.flush().await?;
                    conn.close().await?;
                    return Err(RepError::Auth("Authentication failed".to_string()));
                }

                // Send ack
                conn.send(auth::Message::Ack).await?;
                conn.flush().await?;

                Ok(AuthResult { id, addr, stream: conn.into_inner() })
            });
        } else {
            self.peer_states.insert(
                addr.clone(),
                StreamNotifyClose::new(PeerState {
                    pending_requests: FuturesUnordered::new(),
                    conn: Framed::new(io, reqrep::Codec::new()),
                    addr,
                    egress_queue: VecDeque::with_capacity(128),
                    state: Arc::clone(&self.state),
                    should_flush: false,
                    compressor: self.compressor.clone(),
                }),
            );
        }

        Ok(())
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin, A: Address + Unpin> Stream for PeerState<T, A> {
    type Item = Result<Request<A>, RepError>;

    /// Advances the state of the peer.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            // Flush any messages on the outgoing buffer
            if this.should_flush {
                if let Poll::Ready(Ok(_)) = this.conn.poll_flush_unpin(cx) {
                    this.should_flush = false;
                }
            }

            // Then, try to drain the egress queue.
            if this.conn.poll_ready_unpin(cx).is_ready() {
                if let Some(msg) = this.egress_queue.pop_front() {
                    let msg_len = msg.size();
                    match this.conn.start_send_unpin(msg) {
                        Ok(_) => {
                            this.state.stats.specific.increment_tx(msg_len);
                            this.should_flush = true;

                            // We might be able to send more queued messages
                            continue;
                        }
                        Err(e) => {
                            this.state.stats.specific.increment_failed_requests();
                            error!(err = ?e, "Failed to send message to socket");
                            // End this stream as we can't send any more messages
                            return Poll::Ready(None);
                        }
                    }
                }
            }

            // Then we check for completed requests, and push them onto the egress queue.
            if let Poll::Ready(Some(Some((id, mut payload)))) =
                this.pending_requests.poll_next_unpin(cx)
            {
                let mut compression_type = 0;
                let len_before = payload.len();
                if let Some(ref compressor) = this.compressor {
                    match compressor.compress(&payload) {
                        Ok(compressed) => {
                            payload = compressed;
                            compression_type = compressor.compression_type() as u8;
                        }
                        Err(e) => {
                            error!(err = ?e, "Failed to compress message");
                            continue;
                        }
                    }

                    debug!(
                        "Compressed message {} from {} to {} bytes",
                        id,
                        len_before,
                        payload.len()
                    )
                }

                let msg = reqrep::Message::new(id, compression_type, payload);
                this.egress_queue.push_back(msg);

                continue;
            }

            // Finally we accept incoming requests from the peer.
            match this.conn.poll_next_unpin(cx) {
                Poll::Ready(Some(result)) => {
                    trace!("Received message from peer {:?}: {:?}", this.addr, result);
                    let msg = result?;

                    let (tx, rx) = oneshot::channel();

                    // Add the pending request to the list
                    this.pending_requests.push(PendingRequest { msg_id: msg.id(), response: rx });

                    let request = Request {
                        source: this.addr.clone(),
                        response: tx,
                        compression_type: msg.header().compression_type(),
                        msg: msg.into_payload(),
                    };

                    return Poll::Ready(Some(Ok(request)));
                }
                Poll::Ready(None) => {
                    error!("Framed closed unexpectedly (peer {:?})", this.addr);
                    return Poll::Ready(None);
                }
                Poll::Pending => {}
            }

            return Poll::Pending;
        }
    }
}

struct PendingRequest {
    msg_id: u32,
    response: oneshot::Receiver<Bytes>,
}

impl Future for PendingRequest {
    type Output = Option<(u32, Bytes)>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.response.poll_unpin(cx) {
            Poll::Ready(Ok(response)) => Poll::Ready(Some((self.msg_id, response))),
            Poll::Ready(Err(_)) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
