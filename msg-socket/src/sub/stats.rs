use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};

use msg_transport::Address;
use parking_lot::RwLock;

#[derive(Debug)]
pub struct SubStats<A: Address> {
    /// Individual session stats for each publisher, keyed by Address.
    session_stats: RwLock<HashMap<A, Arc<SessionStats>>>,
    /// Total number of messages dropped due to full ingress buffer between the driver and the
    /// socket frontend.
    dropped_messages_total: AtomicUsize,
    /// Total number of messages successfully received by the socket frontend from the driver.
    messages_received_total: AtomicUsize,
    /// Total number of commands received from the socket frontend by the driver (eg. Subscribe,
    /// Connect).
    commands_received_total: AtomicUsize,
}

impl<A: Address> Default for SubStats<A> {
    fn default() -> Self {
        Self {
            session_stats: RwLock::new(HashMap::new()),
            dropped_messages_total: AtomicUsize::new(0),
            messages_received_total: AtomicUsize::new(0),
            commands_received_total: AtomicUsize::new(0),
        }
    }
}

impl<A: Address> SubStats<A> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the total number of messages dropped due to the ingress buffer being full.
    pub fn dropped_messages_total(&self) -> usize {
        self.dropped_messages_total.load(Ordering::Relaxed)
    }

    /// Returns the total number of messages successfully received by the socket frontend.
    pub fn messages_received_total(&self) -> usize {
        self.messages_received_total.load(Ordering::Relaxed)
    }

    /// Returns the total number of commands processed by the socket driver.
    pub fn commands_received_total(&self) -> usize {
        self.commands_received_total.load(Ordering::Relaxed)
    }

    /// Returns the total bytes received for a specific publisher session, if tracked.
    #[inline]
    pub fn session_bytes_rx(&self, session_addr: &A) -> Option<usize> {
        self.session_stats.read().get(session_addr).map(|stats| stats.bytes_rx())
    }

    /// Returns the average latency in microseconds for a specific publisher session, if tracked.
    #[inline]
    pub fn session_avg_latency(&self, session_addr: &A) -> Option<u64> {
        self.session_stats.read().get(session_addr).map(|stats| stats.avg_latency())
    }

    /// Inserts stats for a new publisher session. (Used by SubDriver)
    #[inline]
    pub(crate) fn insert_session(&self, addr: A, stats: Arc<SessionStats>) {
        self.session_stats.write().insert(addr, stats);
    }

    /// Removes stats for a publisher session. (Used by SubDriver)
    #[inline]
    pub(crate) fn remove_session(&self, addr: &A) {
        self.session_stats.write().remove(addr);
    }

    /// Increments the dropped messages counter. (Used by SubDriver)
    #[inline]
    pub(crate) fn increment_dropped_messages(&self) {
        self.dropped_messages_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Increments the received messages counter. (Used by SubDriver)
    #[inline]
    pub(crate) fn increment_messages_received(&self) {
        self.messages_received_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Increments the received commands counter. (Used by SubDriver)
    #[inline]
    pub(crate) fn increment_commands_received(&self) {
        self.commands_received_total.fetch_add(1, Ordering::Relaxed);
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
