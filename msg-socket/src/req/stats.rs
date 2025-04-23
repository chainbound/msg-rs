use std::sync::atomic::{AtomicUsize, Ordering};

/// Statistics for a request socket. These are shared between the backend task
/// and the socket.
#[derive(Debug, Default)]
pub struct ReqStats {
    /// Total bytes sent
    bytes_tx: AtomicUsize,
    /// Total bytes received
    bytes_rx: AtomicUsize,
    /// The cumulative average round-trip time in microseconds.
    rtt: AtomicUsize,
    /// Index used to calculate rtt
    rtt_idx: AtomicUsize,
}

impl ReqStats {
    #[inline]
    /// Atomically updates the RTT according to the CA formula:
    /// CA = (rtt + n * prev_ca) / (n + 1)
    pub(crate) fn update_rtt(&self, rtt_us: usize) {
        // Wraps around on overflow, which is what we need
        let idx = self.rtt_idx.fetch_add(1, Ordering::Relaxed);
        let prev = self.rtt.load(Ordering::Relaxed);

        let new = (rtt_us + idx * prev) / (idx + 1);
        self.rtt.store(new, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn increment_tx(&self, bytes: usize) {
        self.bytes_tx.fetch_add(bytes, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn increment_rx(&self, bytes: usize) {
        self.bytes_rx.fetch_add(bytes, Ordering::Relaxed);
    }

    #[inline]
    pub fn rtt(&self) -> usize {
        self.rtt.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn bytes_tx(&self) -> usize {
        self.bytes_tx.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn bytes_rx(&self) -> usize {
        self.bytes_rx.load(Ordering::Relaxed)
    }
}
