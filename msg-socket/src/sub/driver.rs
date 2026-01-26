use std::{
    collections::HashSet,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::{Future, StreamExt};
use rustc_hash::FxHashMap;
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio_util::codec::Framed;
use tracing::{debug, error, info, warn};

use super::{
    Command, PubMessage, SocketState, SubOptions,
    session::{PublisherSession, SessionCommand},
    stream::{PublisherStream, TopicMessage},
};
use crate::{ConnectionHookErased, ConnectionState, ExponentialBackoff, HookError};

use msg_common::{Channel, JoinMap, channel};
use msg_transport::{Address, Transport};
use msg_wire::{compression::try_decompress_payload, pubsub};

/// Publisher channel type, used to send messages to the publisher session
/// and receive messages to forward to the socket frontend.
type PubChannel = Channel<SessionCommand, TopicMessage>;

pub(crate) struct SubDriver<T: Transport<A>, A: Address> {
    /// Options shared with the socket.
    pub(super) options: Arc<SubOptions>,
    /// The transport for this socket.
    pub(super) transport: T,
    /// Commands from the socket.
    pub(super) from_socket: mpsc::Receiver<Command<A>>,
    /// Messages to the socket.
    pub(super) to_socket: mpsc::Sender<PubMessage<A>>,
    /// A joinset of connection tasks.
    pub(super) connection_tasks: JoinMap<A, Result<T::Io, HookError>>,
    /// The set of subscribed topics.
    pub(super) subscribed_topics: HashSet<String>,
    /// All publisher sessions for this subscriber socket, keyed by address.
    pub(super) publishers: FxHashMap<A, ConnectionState<PubChannel, ExponentialBackoff, A>>,
    /// Socket state. This is shared with the backend task. Contains the unified stats struct.
    pub(super) state: Arc<SocketState<A>>,
    /// Optional connection hook.
    pub(super) hook: Option<Arc<dyn ConnectionHookErased<T::Io>>>,
}

impl<T, A> Future for SubDriver<T, A>
where
    T: Transport<A>,
    A: Address,
{
    type Output = ();

    /// This poll implementation prioritizes incoming messages over commands.
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            // First, poll all the publishers to handle incoming messages.
            if this.poll_publishers(cx).is_ready() {
                continue;
            }

            // Then, poll the socket for new commands.
            if let Poll::Ready(Some(cmd)) = this.from_socket.poll_recv(cx) {
                // MODIFIED: Access stats via state.stats.specific
                this.state.stats.specific.increment_commands_received();
                // Process the command
                this.on_command(cmd);

                continue;
            }

            // Finally, poll the connection tasks for new connections.
            if let Poll::Ready(Some(Ok((addr, result)))) = this.connection_tasks.poll_join_next(cx)
            {
                match result {
                    Ok(io) => {
                        this.on_connection(addr, io);
                    }
                    Err(e) => {
                        error!(err = ?e, ?addr, "Error connecting to publisher");
                    }
                }

                continue;
            }

            return Poll::Pending;
        }
    }
}

impl<T, A> SubDriver<T, A>
where
    T: Transport<A>,
    A: Address,
{
    /// De-activates a publisher by setting it to [`ConnectionState::Inactive`].
    /// This will initialize the backoff stream.
    fn reset_publisher(&mut self, addr: A) {
        debug!("Resetting publisher at {addr:?}");
        self.publishers.insert(
            addr.clone(),
            ConnectionState::Inactive {
                addr,
                backoff: ExponentialBackoff::new(
                    self.options.initial_backoff,
                    self.options.retry_attempts,
                ),
            },
        );
    }

    /// Returns true if we're already connected to the given publisher address.
    fn is_connected(&self, addr: &A) -> bool {
        if self.publishers.get(addr).is_some_and(|s| s.is_active()) {
            return true;
        }

        false
    }

    fn is_known(&self, addr: &A) -> bool {
        self.publishers.contains_key(addr)
    }

    /// Subscribes to a topic on all publishers.
    fn subscribe(&mut self, topic: String) {
        let mut inactive = Vec::new();

        if self.subscribed_topics.insert(topic.clone()) {
            // Subscribe to the topic on all publishers

            for (addr, publisher_state) in self.publishers.iter_mut() {
                if let ConnectionState::Active { channel } = publisher_state {
                    // If the channel is closed on the other side, deactivate the publisher
                    if let Err(TrySendError::Closed(_)) =
                        channel.try_send(SessionCommand::Subscribe(topic.clone()))
                    {
                        warn!(publisher = ?addr, "Error trying to subscribe to topic {topic}: publisher channel closed");
                        inactive.push(addr.clone());
                    }
                }
            }

            // Remove all inactive publishers
            for addr in inactive {
                // Move publisher to inactive state
                self.reset_publisher(addr);
            }

            info!(
                topic = topic.as_str(),
                n_publishers = self.publishers.len(),
                "Subscribed to topic"
            );
        } else {
            debug!(topic = topic.as_str(), "Already subscribed to topic");
        }
    }

    /// Unsubscribes from a topic on all publishers.
    fn unsubscribe(&mut self, topic: String) {
        let mut inactive = Vec::new();

        if self.subscribed_topics.remove(&topic) {
            // Unsubscribe from the topic on all publishers
            for (addr, publisher_state) in self.publishers.iter_mut() {
                if let ConnectionState::Active { channel } = publisher_state {
                    // If the channel is closed on the other side, deactivate the publisher
                    if let Err(TrySendError::Closed(_)) =
                        channel.try_send(SessionCommand::Unsubscribe(topic.clone()))
                    {
                        warn!(publisher = ?addr, "Error trying to unsubscribe from topic {topic}: publisher channel closed");
                        inactive.push(addr.clone());
                    }
                }
            }

            // Remove all inactive publishers
            for addr in inactive {
                // Move publisher to inactive state
                self.reset_publisher(addr);
            }

            info!(
                topic = topic.as_str(),
                n_publishers = self.publishers.len(),
                "Unsubscribed from topic"
            );
        } else {
            debug!(topic = topic.as_str(), "Not subscribed to topic");
        }
    }

    fn on_command(&mut self, cmd: Command<A>) {
        debug!("Received command: {:?}", cmd);
        match cmd {
            Command::Subscribe { topic } => {
                self.subscribe(topic);
            }
            Command::Unsubscribe { topic } => {
                self.unsubscribe(topic);
            }
            Command::Connect { endpoint } => {
                if self.is_known(&endpoint) {
                    debug!(?endpoint, "Publisher already known, ignoring connect command");
                    return;
                }

                self.connect(endpoint.clone());

                // Also set the publisher to the disconnected state. This will make sure that if the
                // initial connection attempt fails, it will be retried in `poll_publishers`.
                self.reset_publisher(endpoint);
            }
            Command::Disconnect { endpoint } => {
                if self.publishers.remove(&endpoint).is_some() {
                    debug!(?endpoint, "Disconnected from publisher");
                    self.state.stats.specific.remove_session(&endpoint);
                } else {
                    debug!(?endpoint, "Not connected to publisher");
                };
            }
            Command::Shutdown => {
                // TODO: graceful shutdown?
                debug!("shutting down");
            }
        }
    }

    fn connect(&mut self, addr: A) {
        let connect = self.transport.connect(addr.clone());
        let hook = self.hook.clone();

        self.connection_tasks.spawn(addr.clone(), async move {
            let io = match connect.await {
                Ok(io) => io,
                Err(e) => {
                    return (addr, Err(HookError::message(e.to_string())));
                }
            };

            // Run the connection hook if configured
            let io = if let Some(hook) = hook {
                match hook.on_connection(io).await {
                    Ok(io) => io,
                    Err(e) => return (addr, Err(e)),
                }
            } else {
                io
            };

            (addr, Ok(io))
        });
    }

    fn on_connection(&mut self, addr: A, io: T::Io) {
        if self.is_connected(&addr) {
            // We're already connected to this publisher
            warn!(?addr, "Already connected to publisher");
            return;
        }

        debug!("Connection to {:?} established, spawning session", addr);

        let framed = Framed::with_capacity(io, pubsub::Codec::new(), self.options.read_buffer_size);

        let (driver_channel, mut publisher_channel) = channel(1024, 64);

        let publisher_session =
            PublisherSession::new(addr.clone(), PublisherStream::from(framed), driver_channel);

        // Get the shared session stats.
        let session_stats = publisher_session.stats();

        // Spawn the publisher session
        tokio::spawn(publisher_session);

        for topic in self.subscribed_topics.iter() {
            if publisher_channel.try_send(SessionCommand::Subscribe(topic.clone())).is_err() {
                error!(publisher = ?addr, "Error trying to subscribe to topic {topic} on startup: publisher channel closed / full");
            }
        }

        self.publishers
            .insert(addr.clone(), ConnectionState::Active { channel: publisher_channel });

        self.state.stats.specific.insert_session(addr, session_stats);
    }

    /// Polls all the publisher channels for new messages. On new messages, forwards them to the
    /// socket. If a publisher channel is closed, it will be removed from the list of
    /// publishers.
    ///
    /// Returns `Poll::Ready` if any progress was made and this method should be called again.
    /// Returns `Poll::Pending` if no progress was made.
    fn poll_publishers(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let mut progress = false;

        // These should be fine as Vec::new() does not allocate
        let mut inactive = Vec::new();
        let mut to_retry = Vec::new();
        let mut to_terminate = Vec::new();

        for (addr, state) in self.publishers.iter_mut() {
            match state {
                ConnectionState::Active { channel } => {
                    match channel.poll_recv(cx) {
                        Poll::Ready(Some(mut msg)) => {
                            match try_decompress_payload(msg.compression_type, msg.payload) {
                                Ok(decompressed) => msg.payload = decompressed,
                                Err(e) => {
                                    error!(err = ?e, "Failed to decompress message");
                                    continue;
                                }
                            };

                            let msg_to_send = PubMessage::new(addr.clone(), msg.topic, msg.payload);
                            debug!(source = ?msg_to_send.source, ?msg_to_send, "New message");

                            match self.to_socket.try_send(msg_to_send) {
                                Ok(_) => {
                                    // Successfully sent to socket frontend
                                    self.state.stats.specific.increment_messages_received();
                                }
                                Err(TrySendError::Full(msg_back)) => {
                                    // Failed due to full buffer
                                    self.state.stats.specific.increment_dropped_messages();
                                    error!(
                                        topic = msg_back.topic,
                                        "Slow subscriber socket, dropping message"
                                    );
                                }
                                Err(TrySendError::Closed(_)) => {
                                    error!(
                                        "SubSocket frontend channel closed unexpectedly while driver is active."
                                    );
                                    // Consider shutting down or marking as inactive?
                                    // For now, just log. No counter increment.
                                }
                            }

                            progress = true;
                        }
                        Poll::Ready(None) => {
                            error!(source = ?addr, "Publisher stream closed, removing channel");
                            inactive.push(addr.clone());

                            progress = true;
                        }
                        Poll::Pending => {}
                    }
                }
                ConnectionState::Inactive { addr, backoff } => {
                    // Poll the backoff stream
                    if let Poll::Ready(item) = backoff.poll_next_unpin(cx) {
                        if let Some(duration) = item {
                            progress = true;

                            // Only retry if there are no active connection tasks
                            if !self.connection_tasks.contains_key(addr) {
                                debug!(backoff = ?duration, "Retrying connection to {:?}", addr);
                                to_retry.push(addr.clone());
                            } else {
                                debug!(backoff = ?duration, "Not retrying connection to {:?} as there is already a connection task", addr);
                            }
                        } else {
                            error!(
                                "Exceeded maximum number of retries for {:?}, terminating connection",
                                addr
                            );
                            to_terminate.push(addr.clone());
                        }
                    }
                }
            }
        }

        // Activate retries
        for addr in to_retry {
            self.connect(addr);
        }

        // Queue retries for all the inactive publishers.
        for addr in inactive {
            self.reset_publisher(addr);
        }

        // Terminate publishers that are unreachable.
        for addr in to_terminate {
            self.publishers.remove(&addr);
        }

        if progress { Poll::Ready(()) } else { Poll::Pending }
    }
}
