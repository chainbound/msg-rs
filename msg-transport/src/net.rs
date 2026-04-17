//! Network type aliases for feature-gated turmoil integration.
//!
//! When the `turmoil` feature is enabled, this module exports types from
//! `turmoil::net` that mirror tokio's networking types. This allows
//! applications to use simulated networking without any code changes.
//!
//! # Usage
//! Instead of using `tokio::net::TcpListener` or `tokio::net::TcpStream` directly,
//! use the types exported from this module:
//!
//! ```rust
//! use msg_transport::net::{TcpListener, TcpStream};
//! ```
//!
//! # Note on QUIC
//! The QUIC transport uses the [`quinn`] crate which requires real UDP sockets.
//! When using turmoil, only TCP transport participates in the simulation.
//! QUIC connections will use real networking even with the turmoil feature enabled.

#[cfg(feature = "turmoil")]
pub use turmoil::net::{TcpListener, TcpStream};

#[cfg(not(feature = "turmoil"))]
pub use tokio::net::{TcpListener, TcpStream};
