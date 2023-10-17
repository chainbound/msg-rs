use futures::{Future, SinkExt, StreamExt};
use std::{
    borrow::Cow,
    collections::VecDeque,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::broadcast,
    task::JoinSet,
};
use tokio_stream::wrappers::BroadcastStream;
use tokio_util::codec::Framed;
use tracing::{debug, error, trace, warn};

use crate::{AuthResult, Authenticator};
use msg_transport::ServerTransport;
use msg_wire::{auth, pubsub};

use super::{trie::PrefixTrie, PubError, PubMessage};

pub(crate) struct PubDriver<T: ServerTransport> {
    /// Session ID counter.
    pub(super) id_counter: u32,
    /// The server transport used to accept incoming connections.
    pub(super) transport: T,
    /// The reply socket state, shared with the socket front-end.
    // pub(crate) state: Arc<SocketState>,
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

pub(super) struct SubscriberSession<Io> {
    /// The sequence number of this session.
    seq: u32,
    /// The ID of this session.
    session_id: u32,
    /// Messages from the socket.
    from_socket_bcast: BroadcastStream<PubMessage>,
    /// Messages queued to be sent on the connection
    egress_queue: VecDeque<pubsub::Message>,
    /// Session request sender.
    // to_driver: mpsc::Sender<SessionRequest>,
    /// The framed connection.
    conn: Framed<Io, pubsub::Codec>,
    /// The topic filter (a prefix trie that works with strings)
    topic_filter: PrefixTrie,
}

impl<Io: AsyncRead + AsyncWrite + Unpin> SubscriberSession<Io> {
    #[inline]
    fn on_outgoing(&mut self, msg: PubMessage) {
        // Check if the message matches the topic filter
        if self.topic_filter.contains(msg.topic()) {
            trace!(
                topic = msg.topic(),
                "Message matches topic filter, adding to egress queue"
            );

            // Generate the wire message and increment the sequence number
            self.egress_queue.push_back(msg.into_wire(self.seq));
            self.seq = self.seq.wrapping_add(1);
        }
    }

    #[inline]
    fn on_incoming(&mut self, msg: pubsub::Message) {
        // The only incoming messages we should have are control messages.
        match msg_to_control(&msg) {
            ControlMsg::Subscribe(topic) => {
                debug!("Subscribing to topic {:?}", topic);
                self.topic_filter.insert(&topic)
            }
            ControlMsg::Unsubscribe(topic) => {
                debug!("Unsubscribing from topic {:?}", topic);
                self.topic_filter.remove(&topic)
            }
            ControlMsg::Close => todo!(),
        }
    }
}

enum ControlMsg<'a> {
    /// Subscribe to a topic.
    Subscribe(Cow<'a, str>),
    /// Unsubscribe from a topic.
    Unsubscribe(Cow<'a, str>),
    /// Close the session.
    Close,
}

/// Converts the message to a control message. If the message is not a control message,
/// the session is closed.
#[inline]
fn msg_to_control(msg: &pubsub::Message) -> ControlMsg {
    if msg.payload_size() == 0 {
        if msg.topic().starts_with(b"MSG.SUB.") {
            let topic = msg.topic().strip_prefix(b"MSG.SUB.").unwrap();
            ControlMsg::Subscribe(String::from_utf8_lossy(topic))
        } else if msg.topic().starts_with(b"MSG.UNSUB.") {
            let topic = msg.topic().strip_prefix(b"MSG.UNSUB.").unwrap();
            ControlMsg::Unsubscribe(String::from_utf8_lossy(topic))
        } else {
            ControlMsg::Close
        }
    } else {
        tracing::warn!(
            "Unkown control message topic, closing session: {:?}",
            msg.topic()
        );
        ControlMsg::Close
    }
}

impl<Io: AsyncRead + AsyncWrite + Unpin> Future for SubscriberSession<Io> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            // TODO: this might add a lot of overhead from syscalls
            let _ = this.conn.poll_flush_unpin(cx);

            // Then, try to drain the egress queue.
            if this.conn.poll_ready_unpin(cx).is_ready() {
                if let Some(msg) = this.egress_queue.pop_front() {
                    let _msg_len = msg.size();

                    match this.conn.start_send_unpin(msg) {
                        Ok(_) => {
                            // this.state.stats.increment_tx(msg_len);
                            // We might be able to send more queued messages
                            continue;
                        }
                        Err(e) => {
                            tracing::error!("Failed to send message to socket: {:?}", e);
                            // End this stream as we can't send any more messages
                            return Poll::Ready(());
                        }
                    }
                }
            }

            // Poll outgoing messages
            if let Poll::Ready(item) = this.from_socket_bcast.poll_next_unpin(cx) {
                match item {
                    Some(Ok(msg)) => {
                        this.on_outgoing(msg);
                        continue;
                    }
                    Some(Err(e)) => {
                        warn!(
                            session_id = this.session_id,
                            "Receiver lagging behind: {:?}", e
                        );
                        continue;
                    }
                    None => {
                        debug!("Socket closed, shutting down session {}", this.session_id);
                        return Poll::Ready(());
                    }
                }
            }

            // Handle incoming messages from the socket
            if let Poll::Ready(item) = this.conn.poll_next_unpin(cx) {
                match item {
                    Some(Ok(msg)) => {
                        debug!("Incoming message: {:?}", msg);
                        this.on_incoming(msg);
                        continue;
                    }
                    Some(Err(e)) => {
                        error!(
                            session_id = this.session_id,
                            "Error reading from socket: {:?}", e
                        );
                        return Poll::Ready(());
                    }
                    None => {
                        warn!(
                            "Connection closed, shutting down session {}",
                            this.session_id
                        );
                        return Poll::Ready(());
                    }
                }
            }

            return Poll::Pending;
        }
    }
}

impl<T: ServerTransport> Future for PubDriver<T> {
    type Output = Result<(), PubError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            // match this.from_socket.poll_recv(cx) {
            //     Poll::Ready(Some(cmd)) => {
            //         this.on_command(cmd);
            //         continue;
            //     }
            //     Poll::Ready(None) => {
            //         debug!("Socket closed");
            //         return Poll::Ready(Ok(()));
            //     }
            //     Poll::Pending => {}
            // }

            if let Poll::Ready(Some(Ok(auth))) = this.auth_tasks.poll_join_next(cx) {
                match auth {
                    Ok(auth) => {
                        // Run custom authenticator
                        tracing::debug!("Authentication passed for {:?} ({})", auth.id, auth.addr);
                        // this.state.stats.increment_active_clients();

                        let session = SubscriberSession {
                            seq: 0,
                            session_id: this.id_counter,
                            from_socket_bcast: this.from_socket_bcast.resubscribe().into(),
                            egress_queue: VecDeque::with_capacity(128),
                            conn: Framed::new(auth.stream, pubsub::Codec::new()),
                            topic_filter: PrefixTrie::new(),
                        };

                        tokio::spawn(session);

                        this.id_counter = this.id_counter.wrapping_add(1);
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
                        // this.state.stats.increment_active_clients();
                        let session = SubscriberSession {
                            seq: 0,
                            session_id: this.id_counter,
                            from_socket_bcast: this.from_socket_bcast.resubscribe().into(),
                            egress_queue: VecDeque::with_capacity(128),
                            conn: Framed::new(stream, pubsub::Codec::new()),
                            topic_filter: PrefixTrie::new(),
                        };

                        tokio::spawn(session);

                        this.id_counter = this.id_counter.wrapping_add(1);

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
