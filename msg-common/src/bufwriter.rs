use std::{
    collections::VecDeque,
    io::IoSlice,
    pin::Pin,
    task::{Context, Poll, ready},
    time::Instant,
};

use futures::Sink;
use tokio::io::AsyncWrite;
use tokio_util::bytes::{Buf, BytesMut};

use crate::constants::KiB;

/// The maximum number of slices to use in a vectored write. Beyond this, we'll have diminishing
/// returns.
/// TODO: Fine tune this.
const IOV_MAX: usize = 128;

/// The threshold at which we'll coalesce chunks into a single buffer before calling
/// `write` to minimize syscalls. This does an extra allocation and copy.
/// TODO: Fine tune this.
// const COALESCE_CHUNKS_THRESHOLD: usize = 8;

/// The target size for a single `write` or `writev` to the underlying IO.
/// TODO: Fine tune this.
const WRITE_TARGET_SIZE: usize = 128 * KiB as usize;

/// The buffer flushing strategy.
#[derive(Debug, Clone)]
pub enum Strategy {
    /// Buffer up to the given capacity.
    Capacity(usize),
    /// Buffer up to the given duration has elapsed (since the first item was written to the current
    /// buffer).
    Duration(std::time::Duration),
    /// Buffer until we're either at capacity or the given duration has elapsed.
    Either(usize, std::time::Duration),
    /// Don't buffer, flush immediately after every write.
    Immediate,
}

impl Strategy {
    fn capacity(&self) -> Option<usize> {
        match self {
            Self::Capacity(capacity) => Some(*capacity),
            Self::Either(capacity, _) => Some(*capacity),
            _ => None,
        }
    }

    fn duration(&self) -> Option<std::time::Duration> {
        match self {
            Self::Duration(duration) => Some(*duration),
            Self::Either(_, duration) => Some(*duration),
            _ => None,
        }
    }

    fn is_immediate(&self) -> bool {
        matches!(self, Self::Immediate)
    }
}

/// A zero-copy (optionally buffered) writer for items that implement [`Buf`].
///
/// # Implementation
/// It does this by buffering items as is, with the constraint that the items must implement
/// [`Buf`]. This is different from writers like [`tokio_util::codec::Framed`] which use a
/// contiguous buffer of bytes (e.g. `BytesMut`) on which messages must be encoded into before being
/// written, which requires copying bytes.
///
/// When data is flushed according to the [`FlushTrigger`], items are converted into a vector of
/// byte slices and written to the underlying writer using vectored writes (`writev` on Unix
/// systems). This is more efficient than having to copy the bytes into a contiguous buffer (i.e.,
/// header + payload) before writing.
pub struct BufWriter<Io, T>
where
    Io: AsyncWrite,
{
    inner: Io,
    /// The buffer of data to write.
    buf: VecDeque<T>,
    /// The time of the first write to the current buffer.
    first_write: Option<Instant>,
    /// The trigger for flushing the buffer.
    trigger: Strategy,
    /// The buffer to coalesce chunks into before writing (only used for non-vectored writes).
    write_buf: BytesMut,
}

impl<Io, T> BufWriter<Io, T>
where
    Io: AsyncWrite + Unpin,
    T: Buf + Unpin,
{
    /// Create a new buffered writer with the default trigger (flush after every write).
    pub fn new(inner: Io) -> Self {
        Self {
            inner,
            buf: VecDeque::new(),
            trigger: Strategy::Immediate,
            first_write: None,
            write_buf: BytesMut::new(),
        }
    }

    /// Create a new buffered writer with the given buffering strategy.
    pub fn with_strategy(inner: Io, trigger: Strategy) -> Self {
        // Pre-allocate the buffer if a capacity is specified.
        let buf = trigger.capacity().map(|cap| VecDeque::with_capacity(cap)).unwrap_or_default();

        Self { inner, buf, trigger, first_write: None, write_buf: BytesMut::new() }
    }

    #[inline]
    fn should_flush(&self) -> bool {
        match self.trigger {
            Strategy::Capacity(capacity) => self.buf.len() >= capacity,
            Strategy::Duration(duration) => {
                if let Some(first_write) = self.first_write {
                    first_write.elapsed() >= duration
                } else {
                    false
                }
            }
            Strategy::Either(capacity, duration) => {
                self.buf.len() >= capacity ||
                    (self.first_write.is_some() &&
                        self.first_write.unwrap().elapsed() >= duration)
            }
            Strategy::Immediate => true,
        }
    }

    fn write(&mut self, cx: &mut Context<'_>) -> Poll<Result<usize, std::io::Error>> {
        let mut n = 0;

        // Handle vectored writes.
        if self.inner.is_write_vectored() {
            // Drain the buffer
            while !self.buf.is_empty() {
                let mut slices = [IoSlice::new(&[]); IOV_MAX];
                let mut chunks_len = 0;
                let mut bytes_len = 0;

                // Try filling up the slices array with chunks from the buffer.
                for item in self.buf.iter() {
                    // Write the next chunk
                    let cnt = item.chunks_vectored(&mut slices[chunks_len..]);
                    chunks_len += cnt;
                    // Since cursor hasn't been advanced, we can use `remaining` to get the size of
                    // the chunk.
                    bytes_len += item.remaining();

                    if chunks_len >= IOV_MAX || bytes_len >= WRITE_TARGET_SIZE {
                        // We've reached the maximum number of slices.
                        break;
                    }
                }

                // NOTE: `write_vectored` may do a partial write, which we need to account for
                // below. The actual number of bytes written is returned by the
                // function.
                tracing::trace!(len = chunks_len, "writing chunks to underlying IO");
                n = ready!(
                    Pin::new(&mut self.inner).poll_write_vectored(cx, &slices[..chunks_len])
                )?;

                if n == 0 {
                    break;
                }

                // Clear out buffer, handle partial writes.
                let mut remaining = n;
                while remaining > 0 {
                    // Get the next item that was written to the underlying IO.
                    let item = self.buf.front_mut().expect("buffer is not empty");
                    let item_rem = item.remaining();

                    if remaining >= item_rem {
                        // The item is fully written, remove it from the buffer.
                        self.buf.pop_front();
                        remaining -= item_rem;
                    } else {
                        // The item is not fully written, advance the cursor.
                        item.advance(remaining);
                        break;
                    }
                }
            }
        } else {
            while !self.buf.is_empty() || self.write_buf.has_remaining() {
                // Drain the buffer and add chunks to the write buffer until we reach the target
                // size for a single write.
                while let Some(mut item) = self.buf.pop_front() &&
                    self.write_buf.len() < WRITE_TARGET_SIZE
                {
                    while item.has_remaining() {
                        let chunk = item.chunk();
                        self.write_buf.extend_from_slice(chunk);
                        item.advance(chunk.len());
                    }
                }

                // Write the coalesced buffer to the underlying IO.
                while self.write_buf.has_remaining() {
                    n = ready!(Pin::new(&mut self.inner).poll_write(cx, &self.write_buf))?;
                    if n == 0 {
                        return Poll::Ready(Ok(n));
                    }

                    self.write_buf.advance(n);
                }

                // Clear the write buffer if it's empty.
                if !self.write_buf.has_remaining() {
                    self.write_buf.clear();
                }
            }
        }

        Poll::Ready(Ok(n))
    }
}

impl<Io, T> Sink<T> for BufWriter<Io, T>
where
    Io: AsyncWrite + Unpin,
    T: Buf + Unpin,
{
    type Error = std::io::Error;

    /// Poll the sink to check if it is ready to accept the next item. This will attempt to flush
    /// the buffer depending on the trigger, in which case it may return `Pending`.
    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.should_flush() {
            // Proceed with flushing the buffer.
            self.as_mut().poll_flush(cx)
        } else {
            // Ready to accept the next item.
            Poll::Ready(Ok(()))
        }
    }

    /// Queues the item for writing.
    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        let this = self.get_mut();
        // Just put the item in the buffer.
        this.buf.push_back(item);

        // Set first write time if unset.
        if this.first_write.is_none() {
            this.first_write = Some(Instant::now());
        }

        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();

        tracing::trace!(len = this.buf.len(), "flushing BufWriter");

        let n = ready!(this.write(cx))?;
        if n == 0 && !this.buf.is_empty() {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "failed to write to underlying IO",
            )));
        }

        this.first_write = None;

        ready!(Pin::new(&mut this.inner).poll_flush(cx))?;

        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        ready!(self.as_mut().poll_flush(cx))?;
        ready!(Pin::new(&mut self.inner).poll_shutdown(cx))?;

        Poll::Ready(Ok(()))
    }
}
