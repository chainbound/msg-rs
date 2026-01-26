//! Connection hooks for customizing connection setup.
//!
//! The [`ConnectionHook`] trait provides a way to intercept connections during setup,
//! enabling custom authentication, handshakes, or protocol negotiations.
//!
//! # Built-in Hooks
//!
//! The [`token`] module provides ready-to-use token-based authentication hooks:
//! - [`token::ServerHook`] - Server-side hook that validates client tokens
//! - [`token::ClientHook`] - Client-side hook that sends a token to the server
//!
//! # Custom Hooks
//!
//! Implement [`ConnectionHook`] for custom authentication or protocol negotiation:
//!
//! ```rust,ignore
//! use msg_socket::ConnectionHook;
//! use std::io;
//! use tokio::io::{AsyncRead, AsyncWrite, AsyncReadExt, AsyncWriteExt};
//!
//! struct MyAuth;
//!
//! impl<Io> ConnectionHook<Io> for MyAuth
//! where
//!     Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
//! {
//!     async fn on_connection(&self, mut io: Io) -> Result<Io, HookError> {
//!         let mut buf = [0u8; 32];
//!         io.read_exact(&mut buf).await?;
//!         if &buf == b"expected_token_value_32_bytes!!!" {
//!             io.write_all(b"OK").await?;
//!             Ok(io)
//!         } else {
//!             Err(HookError::custom("invalid token"))
//!         }
//!     }
//! }
//! ```

use std::{error::Error as StdError, fmt, future::Future, io, pin::Pin, sync::Arc};

use tokio::io::{AsyncRead, AsyncWrite};

pub mod token;

/// Error type for connection hooks.
///
/// This enum provides two variants:
/// - `Io` for standard I/O errors
/// - `Custom` for hook-specific errors (type-erased)
#[derive(Debug)]
pub enum HookError {
    /// An I/O error occurred.
    Io(io::Error),
    /// A custom hook-specific error.
    Custom(Box<dyn StdError + Send + Sync + 'static>),
}

impl HookError {
    /// Creates a custom error from any error type.
    pub fn custom<E>(error: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::Custom(Box::new(error))
    }

    /// Creates a custom error from a string message.
    pub fn message(msg: impl Into<String>) -> Self {
        Self::Io(io::Error::other(msg.into()))
    }
}

impl fmt::Display for HookError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {e}"),
            Self::Custom(e) => write!(f, "Hook error: {e}"),
        }
    }
}

impl StdError for HookError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Custom(e) => Some(e.as_ref()),
        }
    }
}

impl From<io::Error> for HookError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Hook executed during connection setup.
///
/// For server sockets (Rep, Pub): called when a connection is accepted.
/// For client sockets (Req, Sub): called after connecting.
///
/// The hook receives the raw IO stream and has full control over the handshake protocol.
pub trait ConnectionHook<Io>: Send + Sync + 'static
where
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    /// Called when a connection is established.
    ///
    /// # Arguments
    /// * `io` - The raw IO stream for this connection
    ///
    /// # Returns
    /// The IO stream on success (potentially wrapped/transformed), or an error to reject
    /// the connection.
    fn on_connection(&self, io: Io) -> impl Future<Output = Result<Io, HookError>> + Send;
}

// ============================================================================
// Type-erased hook for internal use
// ============================================================================

/// Type-erased connection hook for internal use.
///
/// This trait allows storing hooks with different concrete types behind a single
/// `Arc<dyn ConnectionHookErased<Io>>`.
pub(crate) trait ConnectionHookErased<Io>: Send + Sync + 'static
where
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    fn on_connection(
        self: Arc<Self>,
        io: Io,
    ) -> Pin<Box<dyn Future<Output = Result<Io, HookError>> + Send + 'static>>;
}

impl<T, Io> ConnectionHookErased<Io> for T
where
    T: ConnectionHook<Io>,
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    fn on_connection(
        self: Arc<Self>,
        io: Io,
    ) -> Pin<Box<dyn Future<Output = Result<Io, HookError>> + Send + 'static>> {
        Box::pin(async move { ConnectionHook::on_connection(&*self, io).await })
    }
}

// ============================================================================
// Hook result type for driver tasks
// ============================================================================

/// The result of running a connection hook.
///
/// Contains the processed IO stream and associated address.
pub(crate) struct HookResult<Io, A> {
    pub(crate) stream: Io,
    pub(crate) addr: A,
}
