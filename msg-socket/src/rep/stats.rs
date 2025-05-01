use std::sync::atomic::{AtomicUsize, Ordering};

/// Statistics for a reply socket.
/// These are shared between the driver task and the socket.
#[derive(Debug, Default)]
pub struct RepStats {
    /// Total bytes sent
    bytes_tx: AtomicUsize,
    /// Total bytes received
    bytes_rx: AtomicUsize,
    /// Total number of active request clients
    active_clients: AtomicUsize,
    /// Total number of failed requests
    failed_requests: AtomicUsize,
}

impl RepStats {
    #[inline]
    pub(crate) fn increment_tx(&self, bytes: usize) {
        self.bytes_tx.fetch_add(bytes, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn increment_rx(&self, bytes: usize) {
        self.bytes_rx.fetch_add(bytes, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn increment_active_clients(&self) {
        self.active_clients.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn decrement_active_clients(&self) {
        self.active_clients.fetch_sub(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn increment_failed_requests(&self) {
        self.failed_requests.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn bytes_tx(&self) -> usize {
        self.bytes_tx.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn bytes_rx(&self) -> usize {
        self.bytes_rx.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn active_clients(&self) -> usize {
        self.active_clients.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn failed_requests(&self) -> usize {
        self.failed_requests.load(Ordering::Relaxed)
    }
}
