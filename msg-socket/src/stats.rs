use std::fmt::Debug;

/// A unified, generic structure to hold statistics for different socket types.
///
/// It contains fields common to multiple socket types (if any identified later)
/// and a specific stats struct `S` tailored to the particular socket implementation
/// (e.g., `SubStats`, `PubStats`).
#[derive(Debug)]
pub struct SocketStats<S> {
    // --- Common fields ---
    // We'll leave this empty for now. We can add common fields like
    // bytes_tx, bytes_rx later if a clear pattern emerges across Pub/Sub/Rep/Req.
    // pub(crate) bytes_tx: AtomicUsize,
    // pub(crate) bytes_rx: AtomicUsize,

    // --- Specific fields ---
    /// Holds the statistics specific to the socket type `S`.
    pub(crate) specific: S,
}

impl<S: Default> Default for SocketStats<S> {
    fn default() -> Self {
        Self {
            // common fields initialized here if added later...
            specific: S::default(),
        }
    }
}

// We can potentially add methods here later to access common fields if they exist.
// impl<S> SocketStats<S> {
//     pub fn bytes_tx(&self) -> usize { self.bytes_tx.load(Ordering::Relaxed) }
//     pub fn bytes_rx(&self) -> usize { self.bytes_rx.load(Ordering::Relaxed) }
// }
