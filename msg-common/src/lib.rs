use std::{
    task::{Context, Poll},
    time::SystemTime,
};

use tokio::sync::mpsc::{
    self,
    error::{SendError, TryRecvError, TrySendError},
    Receiver, Sender,
};

/// Returns the current UNIX timestamp in microseconds.
#[inline]
pub fn unix_micros() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

/// A bounded, bi-directional channel for sending and receiving messages.
/// Relies on Tokio's [`mpsc`] channel.
pub struct Channel<S, R> {
    tx: Sender<S>,
    rx: Receiver<R>,
}

/// Creates a new channel with the given buffer size. This will return a tuple of
/// 2 [`Channel`]s, both of which can be used to send and receive messages.
///
/// It works with 2 generic types, `S` and `R`, which represent the types of
/// messages that can be sent and received, respectively. The first channel in
/// the tuple can be used to send messages of type `S` and receive messages of
/// type `R`. The second channel can be used to send messages of type `R` and
/// receive messages of type `S`.
pub fn channel<S, R>(tx_buffer: usize, rx_buffer: usize) -> (Channel<S, R>, Channel<R, S>) {
    let (tx1, rx1) = mpsc::channel(tx_buffer);
    let (tx2, rx2) = mpsc::channel(rx_buffer);

    (Channel { tx: tx1, rx: rx2 }, Channel { tx: tx2, rx: rx1 })
}

impl<S, R> Channel<S, R> {
    /// Sends a value, waiting until there is capacity.
    ///
    /// A successful send occurs when it is determined that the other end of the
    /// channel has not hung up already. An unsuccessful send would be one where
    /// the corresponding receiver has already been closed. Note that a return
    /// value of `Err` means that the data will never be received, but a return
    /// value of `Ok` does not mean that the data will be received. It is
    /// possible for the corresponding receiver to hang up immediately after
    /// this function returns `Ok`.
    pub async fn send(&mut self, msg: S) -> Result<(), SendError<S>> {
        self.tx.send(msg).await
    }

    /// Attempts to immediately send a message on this [`Sender`]
    ///
    /// This method differs from [`send`] by returning immediately if the channel's
    /// buffer is full or no receiver is waiting to acquire some data. Compared
    /// with [`send`], this function has two failure cases instead of one (one for
    /// disconnection, one for a full buffer).
    pub fn try_send(&mut self, msg: S) -> Result<(), TrySendError<S>> {
        self.tx.try_send(msg)
    }

    /// Receives the next value for this receiver.
    ///
    /// This method returns `None` if the channel has been closed and there are
    /// no remaining messages in the channel's buffer. This indicates that no
    /// further values can ever be received from this `Receiver`. The channel is
    /// closed when all senders have been dropped, or when [`close`] is called.
    ///
    /// If there are no messages in the channel's buffer, but the channel has
    /// not yet been closed, this method will sleep until a message is sent or
    /// the channel is closed.  Note that if [`close`] is called, but there are
    /// still outstanding [`Permits`] from before it was closed, the channel is
    /// not considered closed by `recv` until the permits are released.
    pub async fn recv(&mut self) -> Option<R> {
        self.rx.recv().await
    }

    /// Tries to receive the next value for this receiver.
    ///
    /// This method returns the [`Empty`](TryRecvError::Empty) error if the channel is currently
    /// empty, but there are still outstanding [senders] or [permits].
    ///
    /// This method returns the [`Disconnected`](TryRecvError::Disconnected) error if the channel is
    /// currently empty, and there are no outstanding [senders] or [permits].
    ///
    /// Unlike the [`poll_recv`](Self::poll_recv) method, this method will never return an
    /// [`Empty`](TryRecvError::Empty) error spuriously.
    pub fn try_recv(&mut self) -> Result<R, TryRecvError> {
        self.rx.try_recv()
    }

    /// Polls to receive the next message on this channel.
    ///
    /// This method returns:
    ///
    ///  * `Poll::Pending` if no messages are available but the channel is not
    ///    closed, or if a spurious failure happens.
    ///  * `Poll::Ready(Some(message))` if a message is available.
    ///  * `Poll::Ready(None)` if the channel has been closed and all messages
    ///    sent before it was closed have been received.
    ///
    /// When the method returns `Poll::Pending`, the `Waker` in the provided
    /// `Context` is scheduled to receive a wakeup when a message is sent on any
    /// receiver, or when the channel is closed.  Note that on multiple calls to
    /// `poll_recv`, only the `Waker` from the `Context` passed to the most
    /// recent call is scheduled to receive a wakeup.
    ///
    /// If this method returns `Poll::Pending` due to a spurious failure, then
    /// the `Waker` will be notified when the situation causing the spurious
    /// failure has been resolved. Note that receiving such a wakeup does not
    /// guarantee that the next call will succeed — it could fail with another
    /// spurious failure.
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<R>> {
        self.rx.poll_recv(cx)
    }
}
