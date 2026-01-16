use std::{
    collections::VecDeque,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
    time::Instant,
};

use bytes::Bytes;
use futures::{Future, SinkExt, StreamExt};
use msg_common::span::{EnterSpan as _, SpanExt as _, WithSpan};
use msg_transport::{Address, Transport};
use msg_wire::{
    compression::{Compressor, try_decompress_payload},
    reqrep,
};
use rustc_hash::FxHashMap;
use tokio::{
    sync::{mpsc, oneshot},
    time::Interval,
};

use super::{ReqError, ReqOptions};
use crate::{
    SendCommand,
    req::{
        SocketState,
        conn_manager::{ConnectionController, ConnectionManager},
    },
};

/// The request socket driver. Endless future that drives
/// the socket forward.
pub(crate) struct ReqDriver<T, A, S>
where
    T: Transport<A>,
    A: Address,
{
    /// Options shared with the socket.
    pub(crate) options: Arc<ReqOptions>,
    /// State shared with the socket.
    pub(crate) socket_state: SocketState<T::Stats>,
    /// ID counter for outgoing requests.
    pub(crate) id_counter: u32,
    /// Commands from the socket.
    pub(crate) from_socket: mpsc::Receiver<SendCommand>,
    /// Connection manager that handles connection lifecycle.
    pub(crate) conn_manager: ConnectionManager<T, A, S, reqrep::Codec>,
    /// The timer for the write buffer linger.
    pub(crate) linger_timer: Option<Interval>,
    /// The outgoing message queue.
    pub(crate) egress_queue: VecDeque<WithSpan<reqrep::Message>>,
    /// The currently pending requests waiting for a response.
    pub(crate) pending_requests: FxHashMap<u32, WithSpan<PendingRequest>>,
    /// Interval for checking for request timeouts.
    pub(crate) timeout_check_interval: Interval,
    /// Optional message compressor. This is shared with the socket to keep
    /// the API consistent with other socket types (e.g. `PubSocket`)
    pub(crate) compressor: Option<Arc<dyn Compressor>>,

    /// An unique ID for this driver.
    pub(crate) id: usize,
    /// A span to use for general purpose notifications, not tied to a specific path.
    pub(crate) span: tracing::Span,
}

/// A pending request that is waiting for a response from the peer.
pub(crate) struct PendingRequest {
    /// The timestamp when the request was sent.
    start: Instant,
    /// The sender to send the peer response back to the user.
    sender: oneshot::Sender<Result<Bytes, ReqError>>,
}

impl<T, A, S> ReqDriver<T, A, S>
where
    T: Transport<A>,
    A: Address,
{
    /// Handle an incoming message from the connection.
    fn on_message(&mut self, msg: reqrep::Message) {
        let Some(pending) = self.pending_requests.remove(&msg.id()).enter() else {
            tracing::warn!(parent: &self.span, msg_id = msg.id(), "received response for unknown request id");
            return;
        };

        let rtt = pending.start.elapsed();
        tracing::debug!(msg_id = msg.id(), ?rtt, "received response");

        let size = msg.size();
        let compression_type = msg.header().compression_type();
        let mut payload = msg.into_payload();

        // decompress the response
        match try_decompress_payload(compression_type, payload) {
            Ok(decompressed) => payload = decompressed,
            Err(e) => {
                tracing::error!(?e, "failed to decompress response payload");
                let _ =
                    pending.inner.sender.send(Err(ReqError::Wire(reqrep::Error::Decompression)));
                return;
            }
        }

        if pending.inner.sender.send(Ok(payload)).is_err() {
            tracing::error!("failed to send peer response back, dropped receiver");
        }

        // Update stats
        self.socket_state.stats.specific.update_rtt(rtt.as_micros() as usize);
        self.socket_state.stats.specific.increment_rx(size);
    }

    /// Handle an incoming command from the socket frontend.
    fn on_send(&mut self, cmd: SendCommand) {
        let SendCommand { mut message, response } = cmd;
        let start = Instant::now();

        // We want ot inherit the span from the socket frontend
        let span =
            tracing::info_span!(parent: &message.span, "send", driver_id = format!("req-{}", self.id)).entered();

        // Compress the message if it's larger than the minimum size
        let size_before = message.payload().len();
        if size_before > self.options.min_compress_size {
            if let Some(ref compressor) = self.compressor {
                let start = Instant::now();
                if let Err(e) = message.compress(compressor.as_ref()) {
                    tracing::error!(?e, "failed to compress message");
                }

                tracing::debug!(
                    size_before,
                    size_after = message.payload().len(),
                    elapsed = ?start.elapsed(),
                    "compressed message",
                );
            }
        }

        let msg = message.inner.into_wire(self.id_counter);
        let msg_id = msg.id();
        self.id_counter = self.id_counter.wrapping_add(1);
        self.egress_queue.push_back(msg.with_span(span.clone()));
        self.pending_requests
            .insert(msg_id, PendingRequest { start, sender: response }.with_span(span));
    }

    /// Check for request timeouts and notify the sender if any requests have timed out.
    /// This is done periodically by the driver.
    fn check_timeouts(&mut self) {
        let now = Instant::now();

        let timed_out_ids = self
            .pending_requests
            .iter()
            .filter_map(|(&id, request)| {
                if now.duration_since(request.start) > self.options.timeout {
                    Some(id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        for id in timed_out_ids {
            if let Some(pending_request) = self.pending_requests.remove(&id) {
                let _ = pending_request.into_inner().sender.send(Err(ReqError::Timeout));
            }
        }
    }
}

impl<T, A, S> Future for ReqDriver<T, A, S>
where
    T: Transport<A>,
    A: Address,
    S: ConnectionController<T, A, reqrep::Codec> + Unpin,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let span = this.span.clone();

        loop {
            // Handle connection management: connection task, backoff, and retry logic
            let channel = match this.conn_manager.poll(cx) {
                Poll::Ready(Some(channel)) => channel,
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => return Poll::Pending,
            };

            // Check for incoming messages from the socket
            match channel.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(msg))) => {
                    this.on_message(msg);

                    continue;
                }
                Poll::Ready(Some(Err(e))) => {
                    let _g = span.enter();
                    tracing::warn!(
                        ?e,
                        "failed to read from connection, resetting connection state"
                    );

                    // set the connection to inactive, so that it will be re-tried
                    this.conn_manager.reset_connection();

                    continue;
                }
                Poll::Ready(None) => {
                    let _g = span.enter();
                    tracing::warn!("connection closed, resetting connection state");

                    // set the connection to inactive, so that it will be re-tried
                    this.conn_manager.reset_connection();

                    continue;
                }
                Poll::Pending => {}
            }

            // NOTE: We try to drain the egress queue first (the `continue`), writing everything to
            // the `Framed` internal buffer. When all messages are written, we move on to flushing
            // the connection in the block below. We DO NOT rely on the `Framed` internal
            // backpressure boundary, because we do not call `poll_ready`.
            if let Some(msg) = this.egress_queue.pop_front().enter() {
                // Generate the new message
                let size = msg.size();
                tracing::debug!("Sending msg {}", msg.id());
                // Write the message to the buffer.
                match channel.start_send_unpin(msg.inner) {
                    Ok(_) => {
                        this.socket_state.stats.specific.increment_tx(size);
                    }
                    Err(e) => {
                        tracing::error!(err = ?e, "Failed to send message to socket");

                        // set the connection to inactive, so that it will be re-tried
                        this.conn_manager.reset_connection();
                    }
                }

                // We might be able to write more queued messages to the buffer.
                continue;
            }

            if channel.write_buffer().len() >= this.options.write_buffer_size {
                if let Poll::Ready(Err(e)) = channel.poll_flush_unpin(cx) {
                    tracing::error!(err = ?e, "Failed to flush connection");
                    this.conn_manager.reset_connection();
                    continue;
                }

                if let Some(ref mut linger_timer) = this.linger_timer {
                    // Reset the linger timer.
                    linger_timer.reset();
                }
            }

            if let Some(ref mut linger_timer) = this.linger_timer {
                if !channel.write_buffer().is_empty() && linger_timer.poll_tick(cx).is_ready() {
                    if let Poll::Ready(Err(e)) = channel.poll_flush_unpin(cx) {
                        tracing::error!(err = ?e, "Failed to flush connection");
                        this.conn_manager.reset_connection();
                    }
                }
            }

            // Check for request timeouts
            while this.timeout_check_interval.poll_tick(cx).is_ready() {
                this.check_timeouts();
            }

            // Check for outgoing messages from the socket handle
            match this.from_socket.poll_recv(cx) {
                Poll::Ready(Some(cmd)) => {
                    this.on_send(cmd);

                    continue;
                }
                Poll::Ready(None) => {
                    tracing::debug!(
                        "socket dropped, shutting down backend and flushing connection"
                    );

                    if let Some(channel) = this.conn_manager.active_connection() {
                        let _ = ready!(channel.poll_close_unpin(cx));
                    }

                    return Poll::Ready(());
                }
                Poll::Pending => {}
            }

            return Poll::Pending;
        }
    }
}
