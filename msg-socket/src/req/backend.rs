use bytes::Bytes;
use futures::{Future, SinkExt, StreamExt};
use msg_wire::reqrep;
use rustc_hash::FxHashMap;
use std::{
    collections::VecDeque,
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc, oneshot},
};
use tokio_util::codec::Framed;

use super::{Command, ReqError, ReqOptions};

pub(crate) struct ReqBackend<T: AsyncRead + AsyncWrite> {
    pub(crate) options: Arc<ReqOptions>,
    pub(crate) id_counter: u32,
    pub(crate) from_socket: mpsc::Receiver<Command>,
    pub(crate) conn: Framed<T, reqrep::Codec>,
    pub(crate) egress_queue: VecDeque<reqrep::Message>,
    /// The currently active request, if any. Uses [`FxHashMap`] for performance.
    pub(crate) active_requests: FxHashMap<u32, oneshot::Sender<Result<Bytes, ReqError>>>,
}

impl<T: AsyncRead + AsyncWrite> ReqBackend<T> {
    fn new_message(&mut self, payload: Bytes) -> reqrep::Message {
        let id = self.id_counter;
        // Wrap add here to avoid overflow
        self.id_counter = id.wrapping_add(1);

        reqrep::Message::new(id, payload)
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> Future for ReqBackend<T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            let _ = this.conn.poll_flush_unpin(cx);

            // Check for incoming messages from the socket
            match this.conn.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(msg))) => {
                    if let Some(response) = this.active_requests.remove(&msg.id()) {
                        let _ = response.send(Ok(msg.into_payload()));
                    }

                    continue;
                }
                Poll::Ready(Some(Err(e))) => {
                    // TODO: this should contain the header ID so we can remove the request from the map
                    tracing::error!("Failed to read message from socket: {:?}", e);
                    continue;
                }
                Poll::Ready(None) => {
                    tracing::debug!("Socket closed, shutting down backend");
                    return Poll::Ready(());
                }
                Poll::Pending => {}
            }

            if this.conn.poll_ready_unpin(cx).is_ready() {
                // Drain the egress queue
                if let Some(msg) = this.egress_queue.pop_front() {
                    // Generate the new message
                    tracing::debug!("Sending msg {}", msg.id());
                    match this.conn.start_send_unpin(msg) {
                        Ok(_) => {
                            // We might be able to send more queued messages
                            continue;
                        }
                        Err(e) => {
                            tracing::error!("Failed to send message to socket: {:?}", e);
                            return Poll::Ready(());
                        }
                    }
                }
            }

            // Check for outgoing messages from the socket handle
            match this.from_socket.poll_recv(cx) {
                Poll::Ready(Some(Command::Send { message, response })) => {
                    // Queue the message for sending
                    let msg = this.new_message(message);
                    let id = msg.id();
                    this.egress_queue.push_back(msg);
                    this.active_requests.insert(id, response);

                    continue;
                }
                Poll::Ready(None) => {
                    tracing::debug!(
                        "Socket dropped, shutting down backend and flushing connection"
                    );
                    let _ = ready!(this.conn.poll_close_unpin(cx));
                    return Poll::Ready(());
                }
                Poll::Pending => {}
            }

            return Poll::Pending;
        }
    }
}
