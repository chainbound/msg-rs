use futures::{Future, SinkExt, StreamExt};
use rustc_hash::FxHashMap;
use std::{
    collections::HashSet,
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio_util::codec::Framed;
use tracing::{debug, error, info, warn};

use crate::connection::{ConnectionState, ExponentialBackoff};

use super::session::SessionCommand;
use super::{
    session::PublisherSession,
    stream::{PublisherStream, TopicMessage},
    Command, PubMessage, SocketState, SubOptions,
};

use msg_common::{channel, task::JoinMap, Channel};
use msg_transport::Transport;
use msg_wire::{auth, compression::try_decompress_payload, pubsub};

/// Publisher channel type, used to send messages to the publisher session
/// and receive messages to forward to the socket frontend.
type PubChannel = Channel<SessionCommand, TopicMessage>;

pub(crate) struct SubDriver<T: Transport> {
    /// Options shared with the socket.
    pub(super) options: Arc<SubOptions>,
    /// The transport for this socket.
    pub(super) transport: T,
    /// Commands from the socket.
    pub(super) from_socket: mpsc::Receiver<Command>,
    /// Messages to the socket.
    pub(super) to_socket: mpsc::Sender<PubMessage>,
    /// A joinset of authentication tasks.
    pub(super) connection_tasks: JoinMap<SocketAddr, Result<T::Io, T::Error>>,
    /// The set of subscribed topics.
    pub(super) subscribed_topics: HashSet<String>,
    /// All publisher sessions for this subscriber socket, keyed by address.
    pub(super) publishers: FxHashMap<SocketAddr, ConnectionState<PubChannel, ExponentialBackoff>>,
    /// Socket state. This is shared with the backend task.
    pub(super) state: Arc<SocketState>,
}

impl<T> Future for SubDriver<T>
where
    T: Transport + Send + Sync + Unpin + 'static,
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
                        error!(%addr, "Error connecting to publisher: {:?}", e);
                    }
                }

                continue;
            }

            return Poll::Pending;
        }
    }
}

impl<T> SubDriver<T>
where
    T: Transport + Send + Sync + 'static,
{
    /// De-activates a publisher by setting it to [`ConnectionState::Inactive`].
    /// This will initialize the backoff stream.
    fn reset_publisher(&mut self, addr: SocketAddr) {
        tracing::debug!("Resetting publisher at {addr:?}");
        self.publishers.insert(
            addr,
            ConnectionState::Inactive {
                addr,
                backoff: ExponentialBackoff::new(self.options.initial_backoff, 16),
            },
        );
    }

    /// Returns true if we're already connected to the given publisher address.
    fn is_connected(&self, addr: &SocketAddr) -> bool {
        if self.publishers.get(addr).is_some_and(|s| s.is_active()) {
            return true;
        }

        false
    }

    fn is_known(&self, addr: &SocketAddr) -> bool {
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
                        warn!(publisher = %addr, "Error trying to subscribe to topic {topic}: publisher channel closed");
                        inactive.push(*addr);
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
                        warn!(publisher = %addr, "Error trying to unsubscribe from topic {topic}: publisher channel closed");
                        inactive.push(*addr);
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

    fn on_command(&mut self, cmd: Command) {
        debug!("Received command: {:?}", cmd);
        match cmd {
            Command::Subscribe { topic } => {
                self.subscribe(topic);
            }
            Command::Unsubscribe { topic } => {
                self.unsubscribe(topic);
            }
            Command::Connect { mut endpoint } => {
                // Some transport implementations (e.g. Quinn) can't dial an unspecified IP address, so replace
                // it with localhost.
                if endpoint.ip().is_unspecified() {
                    // TODO: support IPv6
                    endpoint.set_ip(IpAddr::V4(Ipv4Addr::LOCALHOST));
                }

                if self.is_known(&endpoint) {
                    debug!(%endpoint, "Publisher already known, ignoring connect command");
                    return;
                }

                self.connect(endpoint);

                // Also set the publisher to the disconnected state. This will make sure that if the
                // initial connection attempt fails, it will be retried in `poll_publishers`.
                self.reset_publisher(endpoint);
            }
            Command::Disconnect { endpoint } => {
                if self.publishers.remove(&endpoint).is_some() {
                    debug!(%endpoint, "Disconnected from publisher");
                    self.state.stats.remove(&endpoint);
                } else {
                    debug!(%endpoint, "Not connected to publisher");
                };
            }
            Command::Shutdown => {
                // TODO: graceful shutdown?
                tracing::debug!("shutting down");
            }
        }
    }

    fn connect(&mut self, addr: SocketAddr) {
        let connect = self.transport.connect(addr);
        let token = self.options.auth_token.clone();

        self.connection_tasks.spawn(addr, async move {
            let io = match connect.await {
                Ok(io) => io,
                Err(e) => {
                    return (addr, Err(e));
                }
            };

            if let Some(token) = token {
                let mut conn = Framed::new(io, auth::Codec::new_client());

                tracing::debug!("Sending auth message: {:?}", token);
                // Send the authentication message
                if let Err(e) = conn.send(auth::Message::Auth(token)).await {
                    return (addr, Err(e.into()));
                }

                if let Err(e) = conn.flush().await {
                    return (addr, Err(e.into()));
                }

                tracing::debug!("Waiting for ACK from server...");

                // Wait for the response
                let ack = match conn.next().await {
                    Some(Ok(ack)) => ack,
                    Some(Err(e)) => {
                        return (
                            addr,
                            Err(io::Error::new(io::ErrorKind::PermissionDenied, e).into()),
                        )
                    }
                    None => {
                        return (
                            addr,
                            Err(
                                io::Error::new(io::ErrorKind::UnexpectedEof, "Connection closed")
                                    .into(),
                            ),
                        )
                    }
                };

                if matches!(ack, auth::Message::Ack) {
                    (addr, Ok(conn.into_inner()))
                } else {
                    (
                        addr,
                        Err(io::Error::new(
                            io::ErrorKind::PermissionDenied,
                            "Publisher denied connection",
                        )
                        .into()),
                    )
                }
            } else {
                (addr, Ok(io))
            }
        });
    }

    fn on_connection(&mut self, addr: SocketAddr, io: T::Io) {
        if self.is_connected(&addr) {
            // We're already connected to this publisher
            warn!(%addr, "Already connected to publisher");
            return;
        }

        // This should spawn a new task tied to this connection, and
        debug!("Connection to {} established, spawning session", addr);

        let framed = Framed::with_capacity(io, pubsub::Codec::new(), self.options.read_buffer_size);

        let (driver_channel, mut publisher_channel) = channel(1024, 64);

        let publisher_session =
            PublisherSession::new(addr, PublisherStream::from(framed), driver_channel);

        // Get the shared session stats.
        let session_stats = publisher_session.stats();

        // Spawn the publisher session
        tokio::spawn(publisher_session);

        for topic in self.subscribed_topics.iter() {
            if publisher_channel
                .try_send(SessionCommand::Subscribe(topic.clone()))
                .is_err()
            {
                error!(publisher = %addr, "Error trying to subscribe to topic {topic} on startup: publisher channel closed / full");
            }
        }

        self.publishers.insert(
            addr,
            ConnectionState::Active {
                channel: publisher_channel,
            },
        );

        self.state.stats.insert(addr, session_stats);
    }

    /// Polls all the publisher channels for new messages. On new messages, forwards them to the socket.
    /// If a publisher channel is closed, it will be removed from the list of publishers.
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
                                    tracing::error!("Failed to decompress message: {:?}", e);
                                    continue;
                                }
                            };

                            let msg = PubMessage::new(*addr, msg.topic, msg.payload);

                            debug!(source = %msg.source, "New message: {:?}", msg);
                            // TODO: queuing
                            if let Err(TrySendError::Full(msg)) = self.to_socket.try_send(msg) {
                                error!(
                                    topic = msg.topic,
                                    "Slow subscriber socket, dropping message"
                                );
                            }

                            progress = true;
                        }
                        Poll::Ready(None) => {
                            error!(source = %addr, "Publisher stream closed, removing channel");
                            inactive.push(*addr);

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
                                tracing::debug!(backoff = ?duration, "Retrying connection to {:?}", addr);
                                to_retry.push(*addr);
                            } else {
                                tracing::debug!(backoff = ?duration, "Not retrying connection to {:?} as there is already a connection task", addr);
                            }
                        } else {
                            error!("Exceeded maximum number of retries for {:?}, terminating connection", addr);
                            to_terminate.push(*addr);
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

        if progress {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}
