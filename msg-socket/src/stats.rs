use std::fmt::Debug;

/// Statistics for a socket.
#[derive(Debug)]
pub struct SocketStats<S> {
    /// Socket-specific stats.
    pub(crate) specific: S,
}

impl<S: Default> Default for SocketStats<S> {
    fn default() -> Self {
        Self { specific: S::default() }
    }
}

impl<S> std::ops::Deref for SocketStats<S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        &self.specific
    }
}
