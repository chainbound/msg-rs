use std::net::SocketAddr;

use super::Backoff;

/// Abstraction to represent the state of a connection.
///
/// * `C` is the channel type, which is used to send and receive generic messages.
/// * `B` is the backoff type, used to control the backoff state for inactive connections.
pub enum ConnectionState<C, B> {
    Active {
        /// Channel to control the underlying connection. This is used to send
        /// and receive any kind of message in any direction.
        channel: C,
    },
    Inactive {
        addr: SocketAddr,
        /// The current backoff state for inactive connections.
        backoff: B,
    },
}

impl<C, B: Backoff> ConnectionState<C, B> {
    /// Returns `true` if the connection is active.
    #[allow(unused)]
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Active { .. })
    }

    /// Returns `true` if the connection is inactive.
    #[allow(unused)]
    pub fn is_inactive(&self) -> bool {
        matches!(self, Self::Inactive { .. })
    }
}
