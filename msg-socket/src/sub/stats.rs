use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};

use msg_transport::Address;
use parking_lot::RwLock;

/// Statistics for a reply socket. These are shared between the driver task
/// and the socket.
#[derive(Debug, Default)]
pub struct SocketStats<A: Address> {
    /// Individual session stats for each publisher
    session_stats: RwLock<HashMap<A, Arc<SessionStats>>>,
}

impl<A: Address> SocketStats<A> {
    pub fn new() -> Self {
        Self {
            session_stats: RwLock::new(HashMap::new()),
        }
    }
}

impl<A: Address> SocketStats<A> {
    #[inline]
    pub(crate) fn insert(&self, addr: A, stats: Arc<SessionStats>) {
        self.session_stats.write().insert(addr, stats);
    }

    #[inline]
    pub(crate) fn remove(&self, addr: &A) {
        self.session_stats.write().remove(addr);
    }

    #[inline]
    pub fn bytes_rx(&self, session_addr: &A) -> Option<usize> {
        self.session_stats
            .read()
            .get(session_addr)
            .map(|stats| stats.bytes_rx())
    }

    /// Returns the average latency in microseconds for the given session.
    #[inline]
    pub fn avg_latency(&self, session_addr: &A) -> Option<u64> {
        self.session_stats
            .read()
            .get(session_addr)
            .map(|stats| stats.avg_latency())
    }
}

#[derive(Debug, Default)]
pub struct SessionStats {
    /// Total bytes received
    bytes_rx: AtomicUsize,
    /// The cumulative average latency
    latency: AtomicU64,
    /// Index used to calculate CA
    latency_idx: AtomicU64,
}

impl SessionStats {
    #[inline]
    pub(crate) fn increment_rx(&self, bytes: usize) {
        self.bytes_rx.fetch_add(bytes, Ordering::Relaxed);
    }

    #[inline]
    /// Atomically updates the RTT according to the CA formula:
    /// CA = (rtt + n * prev_ca) / (n + 1)
    pub(crate) fn update_latency(&self, latency_us: u64) {
        // Wraps around on overflow, which is what we need
        let idx = self.latency_idx.fetch_add(1, Ordering::Relaxed);
        let prev = self.latency.load(Ordering::Relaxed);

        let new = (latency_us + idx * prev) / (idx + 1);
        self.latency.store(new, Ordering::Relaxed);
    }

    #[inline]
    pub fn bytes_rx(&self) -> usize {
        self.bytes_rx.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn avg_latency(&self) -> u64 {
        self.latency.load(Ordering::Relaxed)
    }
}
