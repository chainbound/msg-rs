//! # MSG Sockets
//!
//! This crate exposes a set of socket types that can be used to facilitate multiple communication
//! patterns, such as:
//! - Request-Reply
//! - Publish-Subscribe
//!
//! Sockets are the main entrypoint in this library and facilitate all connectivity, like binding or
//! connecting, and sending and receiving messages over connections.
#![doc(issue_tracker_base_url = "https://github.com/chainbound/msg-rs/issues/")]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite};

use msg_transport::Address;

pub mod stats;

#[path = "pub/mod.rs"]
mod pubs;
pub use pubs::{PubError, PubOptions, PubSocket};

mod rep;
pub use rep::*;

mod req;
pub use req::*;

mod sub;
pub use sub::*;

mod connection;
pub use connection::*;

/// The default buffer size for a socket.
pub const DEFAULT_BUFFER_SIZE: usize = 8192;

/// The default queue size for a channel.
pub const DEFAULT_QUEUE_SIZE: usize = 8192;

/// A request Identifier.
pub struct RequestId(u32);

impl RequestId {
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    pub fn id(&self) -> u32 {
        self.0
    }

    pub fn increment(&mut self) {
        self.0 = self.0.wrapping_add(1);
    }
}

/// An interface for authenticating clients, given their ID.
pub trait Authenticator: Send + Sync + Unpin + 'static {
    fn authenticate(&self, id: &Bytes) -> bool;
}

/// The result of an authentication attempt.
pub(crate) struct AuthResult<S: AsyncRead + AsyncWrite, A: Address> {
    id: Bytes,
    addr: A,
    stream: S,
}

/// The performance profile to tune socket options for.
#[derive(Debug, Clone, Default, Copy, PartialEq, Eq)]
pub enum Profile {
    /// Optimize for a balanced trade-off between latency and throughput.
    #[default]
    Balanced,
    /// Optimize for low latency.
    Latency,
    /// Optimize for high throughput.
    Throughput,
}
