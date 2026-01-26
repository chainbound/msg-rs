//! Token-based authentication hooks.
//!
//! This module provides ready-to-use connection hooks for simple token-based authentication:
//!
//! - [`ServerHook`] - Server-side hook that validates client tokens
//! - [`ClientHook`] - Client-side hook that sends a token to the server
//!
//! # Example
//!
//! ```rust,ignore
//! use msg_socket::{RepSocket, ReqSocket, tcp::Tcp};
//! use msg_socket::hooks::token::{ServerHook, ClientHook};
//! use bytes::Bytes;
//!
//! // Server side - validates incoming tokens
//! let rep = RepSocket::new(Tcp::default())
//!     .with_connection_hook(ServerHook::new(|token| {
//!         // Custom validation logic
//!         token == b"secret"
//!     }));
//!
//! // Client side - sends token on connect
//! let req = ReqSocket::new(Tcp::default())
//!     .with_connection_hook(ClientHook::new(Bytes::from("secret")));
//! ```

use bytes::Bytes;
use futures::SinkExt;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;

use crate::hooks::{ConnectionHook, HookError};
use msg_wire::auth;

/// Server-side authentication hook that validates incoming client tokens.
///
/// When a client connects, this hook:
/// 1. Waits for the client to send an auth token
/// 2. Validates the token using the provided validator function
/// 3. Sends an ACK on success, or rejects the connection on failure
///
/// # Example
///
/// ```rust,ignore
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
    async fn on_connection(&self, io: Io) -> Result<Io, HookError> {
        let mut conn = Framed::new(io, auth::Codec::new_server());

        // Wait for client authentication message
        let msg = conn
            .next()
            .await
            .ok_or_else(|| HookError::message("connection closed"))?
            .map_err(HookError::custom)?;

        let auth::Message::Auth(token) = msg else {
            return Err(HookError::message("expected auth message"));
        };

        // Validate the token
        if !(self.validator)(&token) {
            conn.send(auth::Message::Reject).await?;
            return Err(HookError::message("authentication rejected"));
        }

        // Send acknowledgment
        conn.send(auth::Message::Ack).await?;

        Ok(conn.into_inner())
    }
}

/// Client-side authentication hook that sends a token to the server.
///
/// When connecting to a server, this hook:
/// 1. Sends the configured token to the server
/// 2. Waits for the server's ACK response
/// 3. Returns an error if the server rejects the token
///
/// # Example
///
/// ```rust,ignore
/// use msg_socket::hooks::token::ClientHook;
/// use bytes::Bytes;
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
    async fn on_connection(&self, io: Io) -> Result<Io, HookError> {
        let mut conn = Framed::new(io, auth::Codec::new_client());

        // Send authentication token
        conn.send(auth::Message::Auth(self.token.clone())).await?;

        conn.flush().await?;

        // Wait for server acknowledgment
        let ack = conn
            .next()
            .await
            .ok_or_else(|| HookError::message("connection closed"))?
            .map_err(HookError::custom)?;

        if !matches!(ack, auth::Message::Ack) {
            return Err(HookError::message("authentication denied"));
        }

        Ok(conn.into_inner())
    }
}
