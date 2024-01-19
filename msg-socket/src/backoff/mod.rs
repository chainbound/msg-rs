use futures::{FutureExt, Stream};
use std::{pin::Pin, task::Poll, time::Duration};
use tokio::time::sleep;

/// A stream that yields exponentially increasing backoff durations.
pub struct ExponentialBackoff {
    /// Current number of retries.
    retry_count: usize,
    /// Maximum number of retries before closing the stream.
    max_retries: usize,
    /// The current backoff duration.
    backoff: Duration,
    /// The current backoff timeout, if any.
    /// We need the timeout to be pinned (`Sleep` is not `Unpin`)
    timeout: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl ExponentialBackoff {
    pub fn new(initial: Duration, max_retries: usize) -> Self {
        Self {
            retry_count: 0,
            max_retries,
            backoff: initial,
            timeout: None,
        }
    }

    /// (Re)-set the timeout to the current backoff duration.
    fn reset_timeout(&mut self) {
        self.timeout = Some(Box::pin(sleep(self.backoff)));
    }
}

impl Stream for ExponentialBackoff {
    type Item = Duration;

    /// Polls the exponential backoff stream. Returns `Poll::Ready` with the current backoff
    /// duration if the backoff timeout has elapsed, otherwise returns `Poll::Pending`.
    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            if let Some(ref mut timeout) = this.timeout {
                if timeout.poll_unpin(cx).is_ready() {
                    // Timeout has elapsed, so reset the timeout and double the backoff
                    this.backoff *= 2;
                    this.retry_count += 1;

                    // Close the stream
                    if this.retry_count >= this.max_retries {
                        return Poll::Ready(None);
                    }

                    this.reset_timeout();

                    // Wake up the task to poll the timeout again
                    cx.waker().wake_by_ref();

                    // Return the current backoff duration
                    return Poll::Ready(Some(this.backoff));
                } else {
                    // Timeout has not elapsed, so return pending
                    return Poll::Pending;
                }
            } else {
                // Set initial timeout
                this.reset_timeout();
            }
        }
    }
}
