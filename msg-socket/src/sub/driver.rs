use futures::Future;
use rustc_hash::FxHashMap;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::net::{IpAddr, Ipv4Addr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc::error::TrySendError;
use tokio::{sync::mpsc, task::JoinSet};
use tokio_util::codec::Framed;
use tracing::{debug, error, info, warn};

use super::session::SessionCommand;
use super::{
    session::PublisherSession,
    stream::{PublisherStream, TopicMessage},
    Command, PubMessage, SocketState, SubOptions,
};

use msg_common::{channel, Channel};
use msg_transport::Transport;
use msg_wire::pubsub;

type ConnectionResult<Io, E> = Result<(SocketAddr, Io), E>;

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
    pub(super) connection_tasks: JoinSet<ConnectionResult<T::Io, T::Error>>,
    /// The set of subscribed topics.
    pub(super) subscribed_topics: HashSet<String>,
    /// All active publisher sessions for this subscriber socket.
    pub(super) publishers: FxHashMap<SocketAddr, Channel<SessionCommand, TopicMessage>>,
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

            if let Poll::Ready(Some(cmd)) = this.from_socket.poll_recv(cx) {
                this.on_command(cmd);

                continue;
            }

            if let Poll::Ready(Some(Ok(result))) = this.connection_tasks.poll_join_next(cx) {
                match result {
                    Ok((addr, io)) => {
                        this.on_connection(addr, io);
                    }
                    Err(e) => {
                        error!("Error connecting to publisher: {:?}", e);
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
    /// Subscribes to a topic on all publishers.
    fn subscribe(&mut self, topic: String) {
        let mut inactive = Vec::new();

        if self.subscribed_topics.insert(topic.clone()) {
            // Subscribe to the topic on all publishers

            for (addr, publisher_channel) in self.publishers.iter_mut() {
                // If the channel is closed on the other side, remove it from the list of publishers.
                if let Err(TrySendError::Closed(_)) =
                    publisher_channel.try_send(SessionCommand::Subscribe(topic.clone()))
                {
                    warn!(publisher = %addr, "Error trying to subscribe to topic {topic}: publisher channel closed");
                    inactive.push(*addr);
                }
            }

            // Remove all inactive publishers
            for addr in inactive {
                self.publishers.remove(&addr);
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
            for (addr, publisher_channel) in self.publishers.iter_mut() {
                // If the channel is closed on the other side, remove it from the list of publishers.
                if let Err(TrySendError::Closed(_)) =
                    publisher_channel.try_send(SessionCommand::Unsubscribe(topic.clone()))
                {
                    warn!(publisher = %addr, "Error trying to unsubscribe from topic {topic}: publisher channel closed");
                    inactive.push(*addr);
                }
            }

            // Remove all inactive publishers
            for addr in inactive {
                self.publishers.remove(&addr);
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

                let connect = self.transport.connect(endpoint);

                self.connection_tasks.spawn(async move {
                    let io = connect.await?;

                    Ok((endpoint, io))
                });
            }
            Command::Disconnect { endpoint } => {
                if self.publishers.remove(&endpoint).is_some() {
                    debug!(endpoint = %endpoint, "Disconnected from publisher");
                    self.state.stats.remove(&endpoint);
                } else {
                    debug!(endpoint = %endpoint, "Not connected to publisher");
                };
            }
            Command::Shutdown => {
                // TODO: graceful shutdown?
                tracing::debug!("shutting down");
            }
        }
    }

    fn on_connection(&mut self, addr: SocketAddr, io: T::Io) {
        // This should spawn a new task tied to this connection, and
        debug!("Connection to {} established, spawning session", addr);

        let framed = Framed::with_capacity(io, pubsub::Codec::new(), self.options.read_buffer_size);

        let (driver_channel, mut publisher_channel) = channel(1024, 64);

        let publisher_session =
            PublisherSession::new(addr, PublisherStream::new(framed), driver_channel);

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

        self.publishers.insert(addr, publisher_channel);
        self.state.stats.insert(addr, session_stats);
    }

    /// Polls all the publisher channels for new messages. On new messages, forwards them to the socket.
    /// If a publisher channel is closed, it will be removed from the list of publishers.
    ///
    /// Returns `Poll::Ready` if any progress was made and this method should be called again.
    /// Returns `Poll::Pending` if no progress was made.
    fn poll_publishers(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let mut progress = false;

        let mut inactive = Vec::new();

        for (addr, rx) in self.publishers.iter_mut() {
            match rx.poll_recv(cx) {
                Poll::Ready(Some(mut msg)) => {
                    match msg.try_decompress() {
                        None => { /* No decompression necessary */ }
                        Some(Ok(decompressed)) => msg.payload = decompressed,
                        Some(Err(e)) => {
                            error!(
                                topic = msg.topic.as_str(),
                                "Failed to decompress message payload: {:?}", e
                            );

                            continue;
                        }
                    }

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

        for addr in inactive {
            self.publishers.remove(&addr);
        }

        if progress {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}
