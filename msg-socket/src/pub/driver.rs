use futures::{Future, SinkExt, StreamExt};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::{sync::broadcast, task::JoinSet};
use tokio_util::codec::Framed;
use tracing::{debug, error};

use super::{
    session::SubscriberSession, trie::PrefixTrie, PubError, PubMessage, PubOptions, SocketState,
};
use crate::{AuthResult, Authenticator};
use msg_transport::ServerTransport;
use msg_wire::{auth, pubsub};

pub(crate) struct PubDriver<T: ServerTransport> {
    /// Session ID counter.
    pub(super) id_counter: u32,
    /// The server transport used to accept incoming connections.
    pub(super) transport: T,
    /// The publisher options (shared with the socket)
    pub(super) options: Arc<PubOptions>,
    /// The publisher socket state, shared with the socket front-end.
    pub(crate) state: Arc<SocketState>,
    /// Receiver from the socket front-end.
    // pub(super) from_socket: mpsc::Receiver<Command>,
    /// Optional connection authenticator.
    pub(super) auth: Option<Arc<dyn Authenticator>>,
    /// A joinset of authentication tasks.
    pub(super) auth_tasks: JoinSet<Result<AuthResult<T::Io>, PubError>>,
    /// Stream map of session receivers, keyed by session ID. This is used to listen to
    /// requests from the session like subscribe, unsubscribe, and to be notified when the
    /// session is closed (`StreamNotifyClose`).
    // pub(super) from_sessions: StreamMap<u32, StreamNotifyClose<ReceiverStream<SessionRequest>>>,
    /// The receiver end of the message broadcast channel. The sender half is stored by [`PubSocket`](super::PubSocket).
    pub(super) from_socket_bcast: broadcast::Receiver<PubMessage>,
}

impl<T: ServerTransport> Future for PubDriver<T> {
    type Output = Result<(), PubError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            if let Poll::Ready(Some(Ok(auth))) = this.auth_tasks.poll_join_next(cx) {
                match auth {
                    Ok(auth) => {
                        // Run custom authenticator
                        debug!("Authentication passed for {:?} ({})", auth.id, auth.addr);
                        // this.state.stats.increment_active_clients();

                        // Default backpressury boundary of 8192 bytes, should we add config option?
                        let framed = Framed::new(auth.stream, pubsub::Codec::new());

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

                        this.state.stats.increment_active_clients();
                        this.id_counter = this.id_counter.wrapping_add(1);
                    }
                    Err(e) => {
                        error!("Error authenticating client: {:?}", e);
                    }
                }

                continue;
            }

            // Poll the transport for new incoming connections
            match this.transport.poll_accept(cx) {
                Poll::Ready(Ok((stream, addr))) => {
                    // If authentication is enabled, start the authentication process
                    if let Some(ref auth) = this.auth {
                        let authenticator = Arc::clone(auth);
                        debug!("New connection from {}, authenticating", addr);
                        this.auth_tasks.spawn(async move {
                            let mut conn = Framed::new(stream, auth::Codec::new_server());

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
                        // this.state.stats.increment_active_clients();
                        let session = SubscriberSession {
                            seq: 0,
                            session_id: this.id_counter,
                            from_socket_bcast: this.from_socket_bcast.resubscribe().into(),
                            state: Arc::clone(&this.state),
                            pending_egress: None,
                            conn: Framed::new(stream, pubsub::Codec::new()),
                            topic_filter: PrefixTrie::new(),
                            should_flush: false,
                            flush_interval: this.options.flush_interval.map(tokio::time::interval),
                        };

                        tokio::spawn(session);

                        this.state.stats.increment_active_clients();
                        this.id_counter = this.id_counter.wrapping_add(1);

                        debug!("New connection from {}", addr);
                    }

                    continue;
                }
                Poll::Ready(Err(e)) => {
                    // Errors here are usually about `WouldBlock`
                    error!("Error accepting connection: {:?}", e);

                    continue;
                }
                Poll::Pending => {}
            }

            return Poll::Pending;
        }
    }
}
