use bytes::Bytes;
use futures::{Future, SinkExt, StreamExt};
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

use crate::SocketState;

use super::{Command, ReqError, ReqOptions};
use msg_wire::reqrep;

/// The request socket driver. Endless future that drives
/// the the socket forward.
pub(crate) struct ReqDriver<T: AsyncRead + AsyncWrite> {
    /// Options shared with the socket.
    #[allow(unused)]
    pub(crate) options: Arc<ReqOptions>,
    /// State shared with the socket.
    pub(crate) socket_state: Arc<SocketState>,
    /// ID counter for outgoing requests.
    pub(crate) id_counter: u32,
    /// Commands from the socket.
    pub(crate) from_socket: mpsc::Receiver<Command>,
    /// The actual [`Framed`] connection with the `Req`-specific codec.
    pub(crate) conn: Framed<T, reqrep::Codec>,
    /// The outgoing message queue.
    pub(crate) egress_queue: VecDeque<reqrep::Message>,
    /// The currently pending requests, if any. Uses [`FxHashMap`] for performance.
    pub(crate) pending_requests: FxHashMap<u32, PendingRequest>,
}

pub(crate) struct PendingRequest {
    start: std::time::Instant,
    sender: oneshot::Sender<Result<Bytes, ReqError>>,
}

impl<T: AsyncRead + AsyncWrite> ReqDriver<T> {
    fn new_message(&mut self, payload: Bytes) -> reqrep::Message {
        let id = self.id_counter;
        // Wrap add here to avoid overflow
        self.id_counter = id.wrapping_add(1);

        reqrep::Message::new(id, payload)
    }

    fn on_message(&mut self, msg: reqrep::Message) {
        if let Some(pending) = self.pending_requests.remove(&msg.id()) {
            let rtt = pending.start.elapsed().as_micros() as usize;
            let size = msg.size();
            let _ = pending.sender.send(Ok(msg.into_payload()));

            // Update stats
            self.socket_state.stats.update_rtt(rtt);
            self.socket_state.stats.increment_rx(size);
        }
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> Future for ReqDriver<T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            let _ = this.conn.poll_flush_unpin(cx);

            // Check for incoming messages from the socket
            match this.conn.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(msg))) => {
                    this.on_message(msg);

                    continue;
                }
                Poll::Ready(Some(Err(e))) => {
                    if let reqrep::Error::Io(e) = e {
                        if e.kind() == std::io::ErrorKind::Other {
                            tracing::error!("Other error: {:?}", e);
                            return Poll::Ready(());
                        }
                    }
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
                    let size = msg.size();
                    tracing::debug!("Sending msg {}", msg.id());
                    match this.conn.start_send_unpin(msg) {
                        Ok(_) => {
                            this.socket_state.stats.increment_tx(size);
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
                    let start = std::time::Instant::now();
                    let msg = this.new_message(message);
                    let id = msg.id();
                    this.egress_queue.push_back(msg);
                    this.pending_requests.insert(
                        id,
                        PendingRequest {
                            start,
                            sender: response,
                        },
                    );

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
