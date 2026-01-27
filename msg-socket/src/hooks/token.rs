//! Token-based authentication connection hooks.
//!
//! This module provides ready-to-use connection hooks for simple token-based authentication:
//!
//! - [`ServerHook`] - Server-side connection hook that validates client tokens
//! - [`ClientHook`] - Client-side connection hook that sends a token to the server
//!
//! # Example
//!
//! ```no_run
//! use bytes::Bytes;
//! use msg_socket::{
//!     RepSocket, ReqSocket,
//!     hooks::token::{ClientHook, ServerHook},
//! };
//! use msg_transport::tcp::Tcp;
//!
//! // Server side - validates incoming tokens
//! let rep = RepSocket::new(Tcp::default()).with_connection_hook(ServerHook::new(|token| {
//!     // Custom validation logic
//!     token == b"secret"
//! }));
//!
//! // Client side - sends token on connect
//! let req =
//!     ReqSocket::new(Tcp::default()).with_connection_hook(ClientHook::new(Bytes::from("secret")));
//! ```

use std::io;

use bytes::Bytes;
use futures::SinkExt;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;

use crate::hooks::{ConnectionHook, Error, HookResult};
use msg_wire::auth;

/// Error type for server-side token authentication.
#[derive(Debug, thiserror::Error)]
pub enum ServerHookError {
    /// The client's token was rejected by the validator.
    #[error("authentication rejected")]
    Rejected,
    /// The connection was closed before authentication completed.
    #[error("connection closed")]
    ConnectionClosed,
    /// Expected an auth message but received something else.
    #[error("expected auth message")]
    ExpectedAuthMessage,
}

/// Error type for client-side token authentication.
#[derive(Debug, thiserror::Error)]
pub enum ClientHookError {
    /// The server denied the authentication.
    #[error("authentication denied")]
    Denied,
    /// The connection was closed before authentication completed.
    #[error("connection closed")]
    ConnectionClosed,
}

/// Server-side authentication connection hook that validates incoming client tokens.
///
/// When a client connects, this connection hook:
/// 1. Waits for the client to send an auth token
/// 2. Validates the token using the provided validator function
/// 3. Sends an ACK on success, or rejects the connection on failure
///
/// # Example
///
/// ```no_run
/// use msg_socket::hooks::token::ServerHook;
///
/// // Accept all tokens
/// let hook = ServerHook::accept_all();
///
/// // Custom validation
/// let hook = ServerHook::new(|token| token == b"my_secret_token");
/// ```
pub struct ServerHook<F> {
    validator: F,
}

impl ServerHook<fn(&Bytes) -> bool> {
    /// Creates a server hook that accepts all tokens.
    pub fn accept_all() -> Self {
        Self { validator: |_| true }
    }
}

impl<F> ServerHook<F>
where
    F: Fn(&Bytes) -> bool + Send + Sync + 'static,
{
    /// Creates a new server hook with the given validator function.
    ///
    /// The validator receives the client's token and returns `true` to accept
    /// the connection or `false` to reject it.
    pub fn new(validator: F) -> Self {
        Self { validator }
    }
}

impl<Io, F> ConnectionHook<Io> for ServerHook<F>
where
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    F: Fn(&Bytes) -> bool + Send + Sync + 'static,
{
    type Error = ServerHookError;

    async fn on_connection(&self, io: Io) -> HookResult<Io, Self::Error> {
        let mut conn = Framed::new(io, auth::Codec::new_server());

        // Wait for client authentication message
        let msg = conn
            .next()
            .await
            .ok_or(Error::hook(ServerHookError::ConnectionClosed))?
            .map_err(|e| io::Error::other(e.to_string()))?;

        let auth::Message::Auth(token) = msg else {
            return Err(Error::hook(ServerHookError::ExpectedAuthMessage));
        };

        // Validate the token
        if !(self.validator)(&token) {
            conn.send(auth::Message::Reject).await?;
            return Err(Error::hook(ServerHookError::Rejected));
        }

        // Send acknowledgment
        conn.send(auth::Message::Ack).await?;

        Ok(conn.into_inner())
    }
}

/// Client-side authentication connection hook that sends a token to the server.
///
/// When connecting to a server, this connection hook:
/// 1. Sends the configured token to the server
/// 2. Waits for the server's ACK response
/// 3. Returns an error if the server rejects the token
///
/// # Example
///
/// ```no_run
/// use bytes::Bytes;
/// use msg_socket::hooks::token::ClientHook;
///
/// let hook = ClientHook::new(Bytes::from("my_secret_token"));
/// ```
pub struct ClientHook {
    token: Bytes,
}

impl ClientHook {
    /// Creates a new client hook with the given authentication token.
    pub fn new(token: Bytes) -> Self {
        Self { token }
    }
}

impl<Io> ConnectionHook<Io> for ClientHook
where
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Error = ClientHookError;

    async fn on_connection(&self, io: Io) -> HookResult<Io, Self::Error> {
        let mut conn = Framed::new(io, auth::Codec::new_client());

        // Send authentication token
        conn.send(auth::Message::Auth(self.token.clone())).await?;

        conn.flush().await?;

        // Wait for server acknowledgment
        let ack = conn
            .next()
            .await
            .ok_or(Error::hook(ClientHookError::ConnectionClosed))?
            .map_err(|e| io::Error::other(e.to_string()))?;

        if !matches!(ack, auth::Message::Ack) {
            return Err(Error::hook(ClientHookError::Denied));
        }

        Ok(conn.into_inner())
    }
}
