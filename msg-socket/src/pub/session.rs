use futures::{Future, SinkExt, StreamExt};
use std::{
    borrow::Cow,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_stream::wrappers::BroadcastStream;
use tokio_util::codec::Framed;
use tracing::{debug, error, trace, warn};

use super::{trie::PrefixTrie, PubMessage, SocketState};
use msg_wire::pubsub;

pub(super) struct SubscriberSession<Io> {
    /// The sequence number of this session.
    pub(super) seq: u32,
    /// The ID of this session.
    pub(super) session_id: u32,
    /// Messages from the socket.
    pub(super) from_socket_bcast: BroadcastStream<PubMessage>,
    /// Messages queued to be sent on the connection
    pub(super) pending_egress: Option<pubsub::Message>,
    /// Session request sender.
    // to_driver: mpsc::Sender<SessionRequest>,
    pub(super) state: Arc<SocketState>,
    /// The framed connection.
    pub(super) conn: Framed<Io, pubsub::Codec>,
    /// The topic filter (a prefix trie that works with strings)
    pub(super) topic_filter: PrefixTrie,
    pub(super) flush_interval: Option<tokio::time::Interval>,
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
            self.pending_egress = Some(msg.into_wire(self.seq));
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
            ControlMsg::Close => {
                debug!(
                    "Closing session after receiving close message {}",
                    self.session_id
                );
            }
        }
    }
}

impl<Io> Drop for SubscriberSession<Io> {
    fn drop(&mut self) {
        self.state.stats.decrement_active_clients();
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
            if let Some(interval) = this.flush_interval.as_mut() {
                if interval.poll_tick(cx).is_ready() {
                    let _ = this.conn.poll_flush_unpin(cx);
                }
            } else {
                let _ = this.conn.poll_flush_unpin(cx);
            }

            // Then, try to drain the egress queue.
            if this.conn.poll_ready_unpin(cx).is_ready() {
                if let Some(msg) = this.pending_egress.take() {
                    tracing::debug!("Sending message: {:?}", msg);
                    let msg_len = msg.size();

                    match this.conn.start_send_unpin(msg) {
                        Ok(_) => {
                            this.state.stats.increment_tx(msg_len);
                            // We might be able to send more queued messages
                            continue;
                        }
                        Err(e) => {
                            tracing::error!("Failed to send message to socket: {:?}", e);
                            let _ = this.conn.poll_close_unpin(cx);
                            // End this stream as we can't send any more messages
                            return Poll::Ready(());
                        }
                    }
                }
            } else {
                return Poll::Pending;
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
                        let _ = this.conn.poll_close_unpin(cx);
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
                        let _ = this.conn.poll_close_unpin(cx);
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
