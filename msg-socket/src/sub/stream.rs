use std::{
    pin::Pin,
    task::{ready, Context, Poll},
};

use bytes::Bytes;
use futures::{SinkExt, Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;
use tracing::{debug, trace};

use super::SubError;

use msg_wire::pubsub;

/// Wraps a framed connection to a publisher and exposes all the PUBSUB specific methods.
pub(super) struct PublisherStream<Io> {
    conn: Framed<Io, pubsub::Codec>,
    flush: bool,
}

impl<Io: AsyncRead + AsyncWrite + Unpin> PublisherStream<Io> {
    /// Queues a message to be sent to the publisher. If the connection
    /// is ready, this will register the waker and flush on the next poll.
    pub fn poll_send(
        &mut self,
        cx: &mut Context<'_>,
        msg: pubsub::Message,
    ) -> Poll<Result<(), SubError>> {
        ready!(self.conn.poll_ready_unpin(cx))?;

        debug!("Sending message to topic: {:?}", msg.topic());

        self.conn.start_send_unpin(msg)?;

        // Make sure the connection gets flushed on next poll
        self.flush = true;

        // Make sure we're woken up to flush the connection
        cx.waker().wake_by_ref();

        Poll::Ready(Ok(()))
    }
}

impl<Io: AsyncRead + AsyncWrite + Unpin> From<Framed<Io, pubsub::Codec>> for PublisherStream<Io> {
    fn from(conn: Framed<Io, pubsub::Codec>) -> Self {
        Self { conn, flush: false }
    }
}

/// A message received from a stream.
pub(super) struct TopicMessage {
    pub timestamp: u64,
    pub compression_type: u8,
    pub topic: String,
    pub payload: Bytes,
}

impl<Io: AsyncRead + AsyncWrite + Unpin> Stream for PublisherStream<Io> {
    type Item = Result<TopicMessage, pubsub::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // We set flush to false only when flush returns ready (i.e. the buffer is fully flushed)
        if this.flush && this.conn.poll_flush_unpin(cx).is_ready() {
            trace!("Flushed connection");
            this.flush = false
        }

        if let Some(result) = ready!(this.conn.poll_next_unpin(cx)) {
            return Poll::Ready(Some(result.map(|msg| {
                let timestamp = msg.timestamp();
                let compression_type = msg.compression_type();
                let (topic, payload) = msg.into_parts();

                // TODO: this will allocate. Can we just return the `Cow`?
                let topic = String::from_utf8_lossy(&topic).to_string();
                TopicMessage { compression_type, timestamp, topic, payload }
            })));
        }

        Poll::Pending
    }
}
