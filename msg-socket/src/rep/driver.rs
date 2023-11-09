use bytes::Bytes;
use futures::{stream::FuturesUnordered, Future, FutureExt, SinkExt, Stream, StreamExt};
use std::{
    collections::VecDeque,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc, oneshot},
    task::JoinSet,
};
use tokio_stream::{StreamMap, StreamNotifyClose};
use tokio_util::codec::Framed;

use crate::{rep::SocketState, AuthResult, Authenticator, PubError, RepOptions, Request};
use msg_transport::ServerTransport;
use msg_wire::{auth, reqrep};

pub(crate) struct PeerState<T: AsyncRead + AsyncWrite> {
    pending_requests: FuturesUnordered<PendingRequest>,
    conn: Framed<T, reqrep::Codec>,
    addr: SocketAddr,
    egress_queue: VecDeque<reqrep::Message>,
    state: Arc<SocketState>,
    should_flush: bool,
}

pub(crate) struct RepDriver<T: ServerTransport> {
    /// The server transport used to accept incoming connections.
    pub(crate) transport: T,
    /// The reply socket state, shared with the socket front-end.
    pub(crate) state: Arc<SocketState>,
    #[allow(unused)]
    /// Options shared with socket.
    pub(crate) options: Arc<RepOptions>,
    /// [`StreamMap`] of connected peers. The key is the peer's address.
    /// Note that when the [`PeerState`] stream ends, it will be silently removed
    /// from this map.
    pub(crate) peer_states: StreamMap<SocketAddr, StreamNotifyClose<PeerState<T::Io>>>,
    /// Sender to the socket front-end. Used to notify the socket of incoming requests.
    pub(crate) to_socket: mpsc::Sender<Request>,
    /// Optional connection authenticator.
    pub(crate) auth: Option<Arc<dyn Authenticator>>,
    /// A joinset of authentication tasks.
    pub(crate) auth_tasks: JoinSet<Result<AuthResult<T::Io>, PubError>>,
}

impl<T: ServerTransport> Future for RepDriver<T> {
    type Output = Result<(), PubError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            if let Poll::Ready(Some((peer, msg))) = this.peer_states.poll_next_unpin(cx) {
                match msg {
                    Some(Ok(request)) => {
                        tracing::debug!("Received request from peer {}", peer);
                        this.state.stats.increment_rx(request.msg().len());
                        let _ = this.to_socket.try_send(request);
                    }
                    Some(Err(e)) => {
                        tracing::error!("Error receiving message from peer {}: {:?}", peer, e);
                    }
                    None => {
                        tracing::debug!("Peer {} disconnected", peer);
                        this.state.stats.decrement_active_clients();
                    }
                }

                continue;
            }

            if let Poll::Ready(Some(Ok(auth))) = this.auth_tasks.poll_join_next(cx) {
                match auth {
                    Ok(auth) => {
                        // Run custom authenticator
                        tracing::debug!("Authentication passed for {:?} ({})", auth.id, auth.addr);
                        this.state.stats.increment_active_clients();

                        this.peer_states.insert(
                            auth.addr,
                            StreamNotifyClose::new(PeerState {
                                pending_requests: FuturesUnordered::new(),
                                conn: Framed::new(auth.stream, reqrep::Codec::new()),
                                addr: auth.addr,
                                // TODO: pre-allocate according to some options
                                egress_queue: VecDeque::with_capacity(64),
                                state: Arc::clone(&this.state),
                                should_flush: false,
                            }),
                        );
                    }
                    Err(e) => {
                        tracing::error!("Error authenticating client: {:?}", e);
                    }
                }

                continue;
            }

            // Poll the transport for new incoming connections
            match this.transport.poll_accept(cx) {
                Poll::Ready(Ok((stream, addr))) => {
                    if let Some(max) = this.options.max_connections {
                        if this.peer_states.len() >= max {
                            tracing::warn!(
                                "Max connections reached ({}), rejecting connection from {}",
                                max,
                                addr
                            );

                            continue;
                        }
                    }

                    // If authentication is enabled, start the authentication process
                    if let Some(ref auth) = this.auth {
                        let authenticator = Arc::clone(auth);
                        tracing::debug!("New connection from {}, authenticating", addr);
                        this.auth_tasks.spawn(async move {
                            let mut conn = Framed::new(stream, auth::Codec::new_server());

                            tracing::debug!("Waiting for auth");
                            // Wait for the response
                            let auth = conn
                                .next()
                                .await
                                .ok_or(PubError::SocketClosed)?
                                .map_err(|e| PubError::Auth(e.to_string()))?;

                            tracing::debug!("Auth received: {:?}", auth);

                            let auth::Message::Auth(id) = auth else {
                                conn.send(auth::Message::Reject).await?;
                                conn.flush().await?;
                                conn.close().await?;
                                return Err(PubError::Auth("Invalid auth message".to_string()));
                            };

                            // If authentication fails, send a reject message and close the connection
                            if !authenticator.authenticate(&id) {
                                conn.send(auth::Message::Reject).await?;
                                conn.flush().await?;
                                conn.close().await?;
                                return Err(PubError::Auth("Authentication failed".to_string()));
                            }

                            // Send ack
                            conn.send(auth::Message::Ack).await?;
                            conn.flush().await?;

                            Ok(AuthResult {
                                id,
                                addr,
                                stream: conn.into_inner(),
                            })
                        });
                    } else {
                        this.state.stats.increment_active_clients();
                        this.peer_states.insert(
                            addr,
                            StreamNotifyClose::new(PeerState {
                                pending_requests: FuturesUnordered::new(),
                                conn: Framed::new(stream, reqrep::Codec::new()),
                                addr,
                                // TODO: pre-allocate according to some options
                                egress_queue: VecDeque::with_capacity(64),
                                state: Arc::clone(&this.state),
                                should_flush: false,
                            }),
                        );

                        tracing::debug!("New connection from {}", addr);
                    }

                    continue;
                }
                Poll::Ready(Err(e)) => {
                    // Errors here are usually about `WouldBlock`
                    tracing::error!("Error accepting connection: {:?}", e);

                    continue;
                }
                Poll::Pending => {}
            }

            return Poll::Pending;
        }
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> Stream for PeerState<T> {
    type Item = Result<Request, PubError>;

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
                            this.state.stats.increment_tx(msg_len);
                            this.should_flush = true;

                            // We might be able to send more queued messages
                            continue;
                        }
                        Err(e) => {
                            this.state.stats.increment_failed_requests();
                            tracing::error!("Failed to send message to socket: {:?}", e);
                            // End this stream as we can't send any more messages
                            return Poll::Ready(None);
                        }
                    }
                }
            }

            // Then we check for completed requests, and push them onto the egress queue.
            if let Poll::Ready(Some(Some((id, payload)))) =
                this.pending_requests.poll_next_unpin(cx)
            {
                let msg = reqrep::Message::new(id, payload);
                this.egress_queue.push_back(msg);

                continue;
            }

            // Finally we accept incoming requests from the peer.
            match this.conn.poll_next_unpin(cx) {
                Poll::Ready(Some(result)) => {
                    tracing::trace!("Received message from peer {}: {:?}", this.addr, result);
                    let msg = result?;
                    let msg_id = msg.id();

                    let (tx, rx) = oneshot::channel();

                    // Add the pending request to the list
                    this.pending_requests.push(PendingRequest {
                        msg_id,
                        response: rx,
                    });

                    let request = Request {
                        source: this.addr,
                        response: tx,
                        msg: msg.into_payload(),
                    };

                    return Poll::Ready(Some(Ok(request)));
                }
                Poll::Ready(None) => {
                    tracing::debug!("Connection closed");
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
