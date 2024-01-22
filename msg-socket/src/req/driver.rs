use bytes::Bytes;
use futures::{Future, SinkExt, StreamExt};
use msg_transport::Transport;
use rustc_hash::FxHashMap;
use std::{
    collections::VecDeque,
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;

use crate::req::SocketState;

use super::{Command, ReqError, ReqOptions};
use msg_wire::reqrep;
use std::time::Instant;
use tokio::time::Interval;

/// The request socket driver. Endless future that drives
/// the the socket forward.
pub(crate) struct ReqDriver<T: Transport> {
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
    pub(crate) conn: Framed<T::Io, reqrep::Codec>,
    /// The outgoing message queue.
    pub(crate) egress_queue: VecDeque<reqrep::Message>,
    /// The currently pending requests, if any. Uses [`FxHashMap`] for performance.
    pub(crate) pending_requests: FxHashMap<u32, PendingRequest>,
    /// Interval for checking for request timeouts.
    pub(crate) timeout_check_interval: Interval,
    /// Interval for flushing the connection. This is secondary to `should_flush`.
    pub(crate) flush_interval: Option<tokio::time::Interval>,
    /// Whether or not the connection should be flushed
    pub(crate) should_flush: bool,
}

/// A pending request that is waiting for a response.
pub(crate) struct PendingRequest {
    /// The timestamp when the request was sent.
    start: Instant,
    /// The response sender.
    sender: oneshot::Sender<Result<Bytes, ReqError>>,
}

impl<T: Transport> ReqDriver<T> {
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

    fn check_timeouts(&mut self) {
        let now = Instant::now();
        let timed_out_ids: Vec<u32> = self
            .pending_requests
            .iter()
            .filter_map(|(&id, request)| {
                if now.duration_since(request.start) > self.options.timeout {
                    Some(id)
                } else {
                    None
                }
            })
            .collect();

        for id in timed_out_ids {
            if let Some(pending_request) = self.pending_requests.remove(&id) {
                let _ = pending_request.sender.send(Err(ReqError::Timeout));
            }
        }
    }

    #[inline]
    fn should_flush(&mut self, cx: &mut Context<'_>) -> bool {
        if self.should_flush {
            if let Some(interval) = self.flush_interval.as_mut() {
                interval.poll_tick(cx).is_ready()
            } else {
                true
            }
        } else {
            // If we shouldn't flush, reset the interval so we don't get woken up
            // every time the interval expires
            if let Some(interval) = self.flush_interval.as_mut() {
                interval.reset()
            }

            false
        }
    }
}

impl<T> Future for ReqDriver<T>
where
    T: Transport + Unpin,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            if this.should_flush(cx) {
                if let Poll::Ready(Ok(_)) = this.conn.poll_flush_unpin(cx) {
                    this.should_flush = false;
                }
            }

            // Check for incoming messages from the socket
            match this.conn.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(msg))) => {
                    this.on_message(msg);

                    continue;
                }
                Poll::Ready(Some(Err(e))) => {
                    if let reqrep::Error::Io(e) = e {
                        tracing::error!("Socket error: {:?}", e);
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

            // Check for request timeouts
            while this.timeout_check_interval.poll_tick(cx).is_ready() {
                this.check_timeouts();
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

                            this.should_flush = true;
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
                    let msg = message.into_wire(this.id_counter);
                    let msg_id = msg.id();
                    this.id_counter = this.id_counter.wrapping_add(1);
                    this.egress_queue.push_back(msg);
                    this.pending_requests.insert(
                        msg_id,
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
