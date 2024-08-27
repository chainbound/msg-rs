use std::{
    collections::VecDeque,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::{Future, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{debug, error, warn};

use msg_common::{unix_micros, Channel};
use msg_transport::Address;
use msg_wire::pubsub;

use super::{
    stats::SessionStats,
    stream::{PublisherStream, TopicMessage},
};

pub(super) enum SessionCommand {
    Subscribe(String),
    Unsubscribe(String),
}

/// Manages the state of a single publisher, represented as a [`Future`].
#[must_use = "This future must be spawned"]
pub(super) struct PublisherSession<Io, A: Address> {
    /// The addr of the publisher
    addr: A,
    /// The egress queue (for subscribe / unsubscribe messages)
    egress: VecDeque<pubsub::Message>,
    /// The inner stream
    stream: PublisherStream<Io>,
    /// The session stats
    stats: Arc<SessionStats>,
    /// Channel for bi-directional communication with the driver. Sends new messages from the associated
    /// publisher and receives subscribe / unsubscribe commands.
    driver_channel: Channel<TopicMessage, SessionCommand>,
}

impl<Io: AsyncRead + AsyncWrite + Unpin, A: Address> PublisherSession<Io, A> {
    pub(super) fn new(
        addr: A,
        stream: PublisherStream<Io>,
        channel: Channel<TopicMessage, SessionCommand>,
    ) -> Self {
        Self {
            addr,
            stream,
            egress: VecDeque::with_capacity(4),
            stats: Arc::new(SessionStats::default()),
            driver_channel: channel,
        }
    }

    /// Returns a reference to the session stats.
    pub(super) fn stats(&self) -> Arc<SessionStats> {
        Arc::clone(&self.stats)
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

    /// Handles incoming messages. On a successful message, the session stats are updated and the message
    /// is forwarded to the driver.
    fn on_incoming(&mut self, incoming: Result<TopicMessage, pubsub::Error>) {
        match incoming {
            Ok(msg) => {
                let now = unix_micros();

                self.stats.increment_rx(msg.payload.len());
                self.stats.update_latency(now.saturating_sub(msg.timestamp));

                if self.driver_channel.try_send(msg).is_err() {
                    warn!(addr = ?self.addr, "Failed to send message to driver");
                }
            }
            Err(e) => {
                error!(addr = ?self.addr, "Error receiving message: {:?}", e);
            }
        }
    }

    fn on_command(&mut self, cmd: SessionCommand) {
        match cmd {
            SessionCommand::Subscribe(topic) => self.subscribe(topic),
            SessionCommand::Unsubscribe(topic) => self.unsubscribe(topic),
        }
    }
}

impl<Io: AsyncRead + AsyncWrite + Unpin, A: Address + Unpin> Future for PublisherSession<Io, A> {
    type Output = ();

    /// This poll implementation prioritizes incoming messages over outgoing messages.
    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            match this.stream.poll_next_unpin(cx) {
                Poll::Ready(Some(result)) => {
                    // Update session stats

                    this.on_incoming(result);
                    continue;
                }
                Poll::Ready(None) => {
                    error!(addr = ?this.addr, "Publisher stream closed");
                    return Poll::Ready(());
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

            if let Poll::Ready(item) = this.driver_channel.poll_recv(cx) {
                match item {
                    Some(cmd) => {
                        this.on_command(cmd);
                        continue;
                    }
                    None => {
                        warn!(addr = ?this.addr, "Driver channel closed, shutting down session");
                        return Poll::Ready(());
                    }
                }
            }

            return Poll::Pending;
        }
    }
}
