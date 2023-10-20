use bytes::Bytes;
use futures::{Future, Stream, StreamExt};
use std::collections::{HashSet, VecDeque};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::error::TrySendError;
use tokio::{sync::mpsc, task::JoinSet};
use tokio_stream::StreamMap;
use tokio_util::codec::Framed;
use tracing::{debug, error};

use super::stream::TopicMessage;
use super::{stream::PublisherStream, Command, PubMessage, SubOptions};
use msg_transport::ClientTransport;
use msg_wire::pubsub;

type ConnectionResult<Io, E> = Result<(SocketAddr, Io), E>;

pub(crate) struct SubDriver<T: ClientTransport> {
    /// Options shared with the socket.
    pub(super) options: Arc<SubOptions>,
    /// The transport for this socket driver.
    pub(super) transport: Arc<T>,
    /// Commands from the socket.
    pub(super) from_socket: mpsc::Receiver<Command>,
    /// Messages to the socket.
    pub(super) to_socket: mpsc::Sender<PubMessage>,
    /// A joinset of authentication tasks.
    pub(super) connection_tasks: JoinSet<ConnectionResult<T::Io, T::Error>>,
    /// The set of subscribed topics.
    pub(super) subscribed_topics: HashSet<String>,
    /// All active publisher sessions for this subscriber socket.
    pub(super) publishers: StreamMap<SocketAddr, PublisherSession<T::Io>>,
}

impl<T> Future for SubDriver<T>
where
    T: ClientTransport + Send + Sync + 'static,
{
    type Output = ();

    /// This poll implementation prioritizes incoming messages over commands.
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            if let Poll::Ready(Some((addr, result))) = this.publishers.poll_next_unpin(cx) {
                match result {
                    Ok(msg) => {
                        this.on_message(PubMessage::new(addr, msg.topic, msg.payload));
                    }
                    Err(e) => {
                        error!(source = %addr, "Error receiving message from publisher: {:?}", e);
                    }
                }

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
    T: ClientTransport + Send + Sync + 'static,
{
    fn on_command(&mut self, cmd: Command) {
        debug!("Received command: {:?}", cmd);
        match cmd {
            Command::Subscribe { topic } => {
                if !self.subscribed_topics.contains(&topic) {
                    self.subscribed_topics.insert(topic.clone());
                    // Subscribe to the topic on all publishers
                    for session in self.publishers.values_mut() {
                        session.subscribe(topic.clone());
                    }
                } else {
                    debug!(topic = topic.as_str(), "Already subscribed to topic");
                }
            }
            Command::Unsubscribe { topic } => {
                if self.subscribed_topics.remove(&topic) {
                    for session in self.publishers.values_mut() {
                        session.unsubscribe(topic.clone());
                    }
                } else {
                    debug!(topic = topic.as_str(), "Not subscribed to topic");
                }
            }
            Command::Connect { endpoint } => {
                let id = self.options.auth_token.clone();
                let transport = Arc::clone(&self.transport);

                // NOTE: don't know if this is gonna work
                self.connection_tasks.spawn(async move {
                    let io = transport.connect_with_auth(endpoint, id).await?;

                    Ok((endpoint, io))
                });
            }
            Command::Disconnect { endpoint } => {
                if self.publishers.remove(&endpoint).is_some() {
                    debug!(endpoint = %endpoint, "Disconnected from publisher");
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

    fn on_message(&self, msg: PubMessage) {
        debug!(source = %msg.source, "New message: {:?}", msg);
        // TODO: queuing
        if let Err(TrySendError::Full(msg)) = self.to_socket.try_send(msg) {
            error!(topic = msg.topic, "Slow subsriber socket, dropping message");
        }
    }

    fn on_connection(&mut self, addr: SocketAddr, io: T::Io) {
        // This should spawn a new task tied to this connection, and
        debug!("Connection to {} established, spawning session", addr);
        let framed = Framed::new(io, pubsub::Codec::new());
        let mut publisher_session = PublisherSession::new(addr, PublisherStream::new(framed));

        for topic in self.subscribed_topics.iter() {
            publisher_session.subscribe(topic.clone());
        }

        self.publishers.insert(addr, publisher_session);
    }
}

/// Manages the state of a single publisher, represented as a [`Stream`].
#[must_use = "streams do nothing unless polled"]
pub(super) struct PublisherSession<Io> {
    /// The addr of the publisher
    addr: SocketAddr,
    /// The egress queue (for subscribe / unsubscribe messages)
    egress: VecDeque<pubsub::Message>,
    /// The inner stream
    stream: PublisherStream<Io>,
}

impl<Io: AsyncRead + AsyncWrite + Send + Unpin> PublisherSession<Io> {
    fn new(addr: SocketAddr, stream: PublisherStream<Io>) -> Self {
        Self {
            addr,
            stream,
            egress: VecDeque::with_capacity(4),
        }
    }

    /// Queues a subscribe message for this publisher.
    /// On the next poll, the message will be attempted to be sent.
    fn subscribe(&mut self, topic: String) {
        self.egress
            .push_back(pubsub::Message::new_sub(Bytes::from(topic)));
    }

    /// Queues an unsubscribe message for this publisher.
    /// On the next poll, the message will be attempted to be sent.
    fn unsubscribe(&mut self, topic: String) {
        self.egress
            .push_back(pubsub::Message::new_unsub(Bytes::from(topic)));
    }
}

impl<Io: AsyncRead + AsyncWrite + Unpin> Stream for PublisherSession<Io> {
    type Item = Result<TopicMessage, pubsub::Error>;

    /// This poll implementation prioritizes incoming messages over outgoing messages.
    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            match this.stream.poll_next_unpin(cx) {
                Poll::Ready(Some(result)) => {
                    return Poll::Ready(Some(result));
                }
                Poll::Ready(None) => {
                    error!(addr = %this.addr, "Publisher stream closed");
                    return Poll::Ready(None);
                }
                Poll::Pending => {}
            }

            let mut progress = false;
            while let Some(msg) = this.egress.pop_front() {
                // TODO(perf): do we need to clone the message here?
                if this.stream.poll_send(cx, msg.clone()).is_ready() {
                    progress = true;
                    debug!("Queued message for sending: {:?}", msg);
                } else {
                    this.egress.push_back(msg);
                    break;
                }
            }

            if progress {
                continue;
            }

            return Poll::Pending;
        }
    }
}
