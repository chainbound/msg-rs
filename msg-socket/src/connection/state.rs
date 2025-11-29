/// Abstraction to represent the state of a connection.
///
/// * `C` is the channel type, which is used to send and receive generic messages.
/// * `B` is the backoff type, used to control the backoff state for inactive connections.
/// * `A` is the address type, used to represent the address of the connection.
pub enum ConnectionState<C, B, A> {
    Active {
        /// Channel to control the underlying connection. This is used to send
        /// and receive any kind of message in any direction.
        channel: C,
    },
    Inactive {
        addr: A,
        /// The current backoff state for inactive connections.
        backoff: B,
    },
}

impl<C, B, A> ConnectionState<C, B, A> {
    /// Returns `true` if the connection is active.
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Active { .. })
    }

    /// Returns `true` if the connection is inactive.
    pub fn is_inactive(&self) -> bool {
        matches!(self, Self::Inactive { .. })
    }
}

pub enum SplitConnectionState<R, W, B, A> {
    Active { reader: R, writer: W },
    Inactive { addr: A, backoff: B },
}

impl<R, W, B, A> SplitConnectionState<R, W, B, A> {
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Active { .. })
    }

    pub fn is_inactive(&self) -> bool {
        matches!(self, Self::Inactive { .. })
    }
}
