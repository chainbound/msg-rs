//! Connection hooks for customizing connection establishment.
//!
//! Connection hooks are attached when establishing connections and allow custom
//! authentication, handshakes, or protocol negotiations. The [`ConnectionHook`] trait
//! is called during connection setup, before the connection is used for messaging.
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
//! ```no_run
//! use msg_socket::hooks::{ConnectionHook, Error, HookResult};
//! use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
//!
//! struct MyAuth;
//!
//! #[derive(Debug, thiserror::Error)]
//! enum MyAuthError {
//!     #[error("invalid token")]
//!     InvalidToken,
//! }
//!
//! impl<Io> ConnectionHook<Io> for MyAuth
//! where
//!     Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
//! {
//!     type Error = MyAuthError;
//!
//!     async fn on_connection(&self, mut io: Io) -> HookResult<Io, Self::Error> {
//!         let mut buf = [0u8; 32];
//!         io.read_exact(&mut buf).await?;
//!         if &buf == b"expected_token_value_32_bytes!!!" {
//!             io.write_all(b"OK").await?;
//!             Ok(io)
//!         } else {
//!             Err(Error::hook(MyAuthError::InvalidToken))
//!         }
//!     }
//! }
//! ```
//!
//! # Future Extensions
//!
//! TODO: Additional hooks may be added for different parts of the connection lifecycle
//! (e.g., disconnection, reconnection, periodic health checks).

use std::{error::Error as StdError, future::Future, io, pin::Pin, sync::Arc};

use tokio::io::{AsyncRead, AsyncWrite};

pub mod token;

/// Error type for connection hooks.
///
/// Distinguishes between I/O errors and hook-specific errors.
#[derive(Debug, thiserror::Error)]
pub enum Error<E> {
    /// An I/O error occurred.
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    /// A hook-specific error.
    #[error("Hook error: {0}")]
    Hook(#[source] E),
}

impl<E> Error<E> {
    /// Create a hook error from a hook-specific error.
    pub fn hook(err: E) -> Self {
        Error::Hook(err)
    }
}

/// Result type for connection hooks.
///
/// This is intentionally named `HookResult` (not `Result`) to make it clear this is not
/// `std::result::Result`. A `HookResult` can be:
/// - `Ok(io)` - success, returns the IO stream
/// - `Err(Error::Io(..))` - an I/O error occurred
/// - `Err(Error::Hook(..))` - a hook-specific error occurred
pub type HookResult<T, E> = std::result::Result<T, Error<E>>;

/// Type-erased hook result used internally by drivers.
pub(crate) type ErasedHookResult<T> = HookResult<T, Box<dyn StdError + Send + Sync>>;

/// Connection hook executed during connection establishment.
///
/// For server sockets (Rep, Pub): called when a connection is accepted.
/// For client sockets (Req, Sub): called after connecting.
///
/// The connection hook receives the raw IO stream and has full control over the handshake protocol.
pub trait ConnectionHook<Io>: Send + Sync + 'static
where
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    /// The hook-specific error type.
    type Error: StdError + Send + Sync + 'static;

    /// Called when a connection is established.
    ///
    /// # Arguments
    /// * `io` - The raw IO stream for this connection
    ///
    /// # Returns
    /// - `Ok(io)` - The IO stream on success (potentially wrapped/transformed)
    /// - `Err(Error::Io(..))` - An I/O error occurred
    /// - `Err(Error::Hook(Self::Error))` - A hook-specific error to reject the connection
    fn on_connection(&self, io: Io) -> impl Future<Output = HookResult<Io, Self::Error>> + Send;
}

// ============================================================================
// Type-erased connection hook for internal use
// ============================================================================

/// Type-erased connection hook for internal use.
///
/// This trait allows storing connection hooks with different concrete types behind a single
/// `Arc<dyn ConnectionHookErased<Io>>`. The hook error type is erased to `Box<dyn Error>`.
pub(crate) trait ConnectionHookErased<Io>: Send + Sync + 'static
where
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    fn on_connection(
        self: Arc<Self>,
        io: Io,
    ) -> Pin<Box<dyn Future<Output = ErasedHookResult<Io>> + Send + 'static>>;
}

impl<T, Io> ConnectionHookErased<Io> for T
where
    T: ConnectionHook<Io>,
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    fn on_connection(
        self: Arc<Self>,
        io: Io,
    ) -> Pin<Box<dyn Future<Output = ErasedHookResult<Io>> + Send + 'static>> {
        Box::pin(async move {
            ConnectionHook::on_connection(&*self, io).await.map_err(|e| match e {
                Error::Io(io_err) => Error::Io(io_err),
                Error::Hook(hook_err) => {
                    Error::Hook(Box::new(hook_err) as Box<dyn StdError + Send + Sync>)
                }
            })
        })
    }
}
