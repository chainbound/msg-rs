use std::{
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::{stream::FuturesUnordered, Future, SinkExt, StreamExt};
use tokio::{sync::broadcast, task::JoinSet};
use tokio_util::codec::Framed;
use tracing::{debug, error, info, warn};

use super::{
    session::SubscriberSession, trie::PrefixTrie, PubError, PubMessage, PubOptions, SocketState,
};
use crate::{AuthResult, Authenticator};
use msg_transport::{Address, PeerAddress, Transport};
use msg_wire::{auth, pubsub};

#[allow(clippy::type_complexity)]
pub(crate) struct PubDriver<T: Transport<A>, A: Address> {
    /// Session ID counter.
    pub(super) id_counter: u32,
    /// The server transport used to accept incoming connections.
    pub(super) transport: T,
    /// The publisher options (shared with the socket)
    pub(super) options: Arc<PubOptions>,
    /// The publisher socket state, shared with the socket front-end.
    pub(crate) state: Arc<SocketState>,
    /// Optional connection authenticator.
    pub(super) auth: Option<Arc<dyn Authenticator>>,
    /// A set of pending incoming connections, represented by [`Transport::Accept`].
    pub(super) conn_tasks: FuturesUnordered<T::Accept>,
    /// A joinset of authentication tasks.
    pub(super) auth_tasks: JoinSet<Result<AuthResult<T::Io, A>, PubError>>,
    /// The receiver end of the message broadcast channel. The sender half is stored by [`PubSocket`](super::PubSocket).
    pub(super) from_socket_bcast: broadcast::Receiver<PubMessage>,
}

impl<T, A> Future for PubDriver<T, A>
where
    T: Transport<A> + Unpin + 'static,
    A: Address,
{
    type Output = Result<(), PubError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            // First, poll the joinset of authentication tasks. If a new connection has been handled we spawn a new session for it.
            if let Poll::Ready(Some(Ok(auth))) = this.auth_tasks.poll_join_next(cx) {
                match auth {
                    Ok(auth) => {
                        // Run custom authenticator
                        debug!("Authentication passed for {:?} ({:?})", auth.id, auth.addr);

                        let mut framed = Framed::new(auth.stream, pubsub::Codec::new());
                        framed.set_backpressure_boundary(this.options.backpressure_boundary);

                        let session = SubscriberSession {
                            seq: 0,
                            session_id: this.id_counter,
                            from_socket_bcast: this.from_socket_bcast.resubscribe().into(),
                            state: Arc::clone(&this.state),
                            pending_egress: None,
                            conn: framed,
                            topic_filter: PrefixTrie::new(),
                            should_flush: false,
                            flush_interval: this.options.flush_interval.map(tokio::time::interval),
                        };

                        tokio::spawn(session);

                        this.id_counter = this.id_counter.wrapping_add(1);
                    }
                    Err(e) => {
                        error!("Error authenticating client: {:?}", e);
                        this.state.stats.decrement_active_clients();
                    }
                }

                continue;
            }

            // Then poll the incoming connection tasks. If a new connection has been accepted, spawn a new authentication task for it.
            if let Poll::Ready(Some(incoming)) = this.conn_tasks.poll_next_unpin(cx) {
                match incoming {
                    Ok(io) => {
                        if let Err(e) = this.on_incoming(io) {
                            error!("Error accepting incoming connection: {:?}", e);
                            this.state.stats.decrement_active_clients();
                        }
                    }
                    Err(e) => {
                        error!("Error accepting incoming connection: {:?}", e);

                        // Active clients have already been incremented in the initial call to `poll_accept`,
                        // so we need to decrement them here.
                        this.state.stats.decrement_active_clients();
                    }
                }

                continue;
            }

            // Finally, poll the transport for new incoming connection futures and push them to the incoming connection tasks.
            if let Poll::Ready(accept) = Pin::new(&mut this.transport).poll_accept(cx) {
                if let Some(max) = this.options.max_clients {
                    if this.state.stats.active_clients() >= max {
                        warn!(
                            "Max connections reached ({}), rejecting new incoming connection",
                            max
                        );

                        continue;
                    }
                }

                // Increment the active clients counter. If the authentication fails, this counter
                // will be decremented.
                this.state.stats.increment_active_clients();

                this.conn_tasks.push(accept);

                continue;
            }

            return Poll::Pending;
        }
    }
}

impl<T, A> PubDriver<T, A>
where
    T: Transport<A> + Unpin + 'static,
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
                    .ok_or(PubError::SocketClosed)?
                    .map_err(|e| PubError::Auth(e.to_string()))?;

                debug!("Auth received: {:?}", auth);

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
            let mut framed = Framed::new(io, pubsub::Codec::new());
            framed.set_backpressure_boundary(self.options.backpressure_boundary);

            let session = SubscriberSession {
                seq: 0,
                session_id: self.id_counter,
                from_socket_bcast: self.from_socket_bcast.resubscribe().into(),
                state: Arc::clone(&self.state),
                pending_egress: None,
                conn: framed,
                topic_filter: PrefixTrie::new(),
                should_flush: false,
                flush_interval: self.options.flush_interval.map(tokio::time::interval),
            };

            tokio::spawn(session);

            self.id_counter = self.id_counter.wrapping_add(1);
            debug!(
                "New connection from {:?}, session ID {}",
                addr, self.id_counter
            );
        }

        Ok(())
    }
}
