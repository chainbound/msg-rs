use std::{
    borrow::Cow,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::{Future, SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_stream::wrappers::BroadcastStream;
use tokio_util::codec::Framed;
use tracing::{debug, error, trace, warn};

use super::{PubMessage, trie::PrefixTrie};
use msg_wire::pubsub;

use super::stats::PubStats;
use crate::stats::SocketStats;

/// A subscriber session. This struct represents a single subscriber session, which is a
/// connection to a subscriber. This struct is responsible for handling incoming and outgoing
/// messages, as well as managing the connection state.
pub(super) struct SubscriberSession<Io> {
    /// The sequence number of this session.
    pub(super) seq: u32,
    /// The ID of this session.
    pub(super) session_id: u32,
    /// Messages from the socket.
    pub(super) from_socket_bcast: BroadcastStream<PubMessage>,
    /// Messages queued to be sent on the connection
    pub(super) pending_egress: Option<pubsub::Message>,
    /// The socket stats.
    pub(super) stats: Arc<SocketStats<PubStats>>,
    /// The framed connection.
    pub(super) conn: Framed<Io, pubsub::Codec>,
    /// The topic filter (a prefix trie that works with strings)
    pub(super) topic_filter: PrefixTrie,
    /// The timer for the write buffer linger.
    pub(super) linger_timer: Option<tokio::time::Interval>,
    /// The size of the write buffer.
    pub(super) write_buffer_size: usize,
}

impl<Io: AsyncRead + AsyncWrite + Unpin> SubscriberSession<Io> {
    /// Handles outgoing messages to the socket.
    #[inline]
    fn on_outgoing(&mut self, msg: PubMessage) {
        // Check if the message matches the topic filter
        if self.topic_filter.contains(msg.topic()) {
            trace!(topic = msg.topic(), "Message matches topic filter, adding to egress queue");

            // Generate the wire message and increment the sequence number
            self.pending_egress = Some(msg.into_wire(self.seq));
            self.seq = self.seq.wrapping_add(1);
        } else {
            trace!(topic = msg.topic(), "Message does not match topic filter, discarding");
        }
    }

    /// Handles incoming messages from the socket.
    #[inline]
    fn on_incoming(&mut self, msg: pubsub::Message) {
        // The only incoming messages we should have are control messages.
        match msg_to_control(&msg) {
            ControlMsg::Subscribe(topic) => {
                debug!("Subscribing to topic {}", topic);
                self.topic_filter.insert(&topic)
            }
            ControlMsg::Unsubscribe(topic) => {
                debug!("Unsubscribing from topic {}", topic);
                self.topic_filter.remove(&topic)
            }
            ControlMsg::Close => {
                debug!("Closing session after receiving close message {}", self.session_id);
            }
        }
    }
}

impl<Io> Drop for SubscriberSession<Io> {
    fn drop(&mut self) {
        self.stats.specific.decrement_active_clients();
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
fn msg_to_control(msg: &pubsub::Message) -> ControlMsg<'_> {
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
        warn!(
            "Unkown control message topic, closing session: {}",
            String::from_utf8_lossy(msg.topic())
        );
        ControlMsg::Close
    }
}

impl<Io: AsyncRead + AsyncWrite + Unpin> Future for SubscriberSession<Io> {
    type Output = ();

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            // NOTE: We try to drain the queue first, writing everything to
            // the `Framed` internal buffer. When all messages are written, we move on to flushing
            // the connection in the block below. We DO NOT rely on the `Framed` internal
            // backpressure boundary, because we do not call `poll_ready`.
            if let Some(msg) = this.pending_egress.take() {
                debug!(?msg, "Sending message");
                let msg_len = msg.size();

                match this.conn.start_send_unpin(msg) {
                    Ok(_) => {
                        this.stats.specific.increment_tx(msg_len);
                    }
                    Err(e) => {
                        error!(err = ?e, "Failed to send message to socket");
                        let _ = this.conn.poll_close_unpin(cx);
                        // End this stream as we can't send any more messages
                        return Poll::Ready(());
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
                        warn!(err = ?e, session_id = this.session_id, "Receiver lagging behind");
                        continue;
                    }
                    None => {
                        debug!("Socket closed, shutting down session {}", this.session_id);
                        let _ = this.conn.poll_close_unpin(cx);
                        return Poll::Ready(());
                    }
                }
            }

            // Check flushing conditions
            if this.conn.write_buffer().len() >= this.write_buffer_size {
                if let Poll::Ready(Err(e)) = this.conn.poll_flush_unpin(cx) {
                    tracing::error!(err = ?e, session_id = this.seq, "Failed to flush connection, shutting down session");
                    let _ = this.conn.poll_close_unpin(cx);
                    // End this stream as we can't send any more messages
                    return Poll::Ready(());
                }

                if let Some(ref mut linger_timer) = this.linger_timer {
                    // Reset the linger timer.
                    linger_timer.reset();
                }
            }

            if let Some(ref mut linger_timer) = this.linger_timer &&
                !this.conn.write_buffer().is_empty() &&
                linger_timer.poll_tick(cx).is_ready() &&
                let Poll::Ready(Err(e)) = this.conn.poll_flush_unpin(cx)
            {
                tracing::error!(err = ?e, "Failed to flush connection");
                let _ = this.conn.poll_close_unpin(cx);
                // End this stream as we can't send any more messages
                return Poll::Ready(());
            }

            // Handle incoming messages from the socket
            if let Poll::Ready(item) = this.conn.poll_next_unpin(cx) {
                match item {
                    Some(Ok(msg)) => {
                        debug!(?msg, "Incoming message");
                        this.on_incoming(msg);
                        continue;
                    }
                    Some(Err(e)) => {
                        error!(err = ?e, session_id = this.session_id, "Error reading from socket");
                        let _ = this.conn.poll_close_unpin(cx);
                        return Poll::Ready(());
                    }
                    None => {
                        warn!("Connection closed, shutting down session {}", this.session_id);
                        return Poll::Ready(());
                    }
                }
            }

            return Poll::Pending;
        }
    }
}
