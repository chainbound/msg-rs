use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Default)]
pub struct PubStats {
    /// Total bytes sent
    bytes_tx: AtomicUsize,
    /// Total number of active request clients
    active_clients: AtomicUsize,
}

impl PubStats {
    #[inline]
    pub fn bytes_tx(&self) -> usize {
        self.bytes_tx.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn active_clients(&self) -> usize {
        self.active_clients.load(Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn increment_tx(&self, bytes: usize) {
        self.bytes_tx.fetch_add(bytes, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn increment_active_clients(&self) {
        self.active_clients.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn decrement_active_clients(&self) {
        self.active_clients.fetch_sub(1, Ordering::Relaxed);
    }
}
