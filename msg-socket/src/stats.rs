use std::fmt::Debug;

use derive_more::Deref;

/// Statistics for a socket.
#[derive(Debug, Deref)]
pub struct SocketStats<S> {
    /// Socket-specific stats.
    pub(crate) specific: S,
}

impl<S: Default> Default for SocketStats<S> {
    fn default() -> Self {
        Self { specific: S::default() }
    }
}
