use bytes::Bytes;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::oneshot;

use msg_wire::reqrep;

mod driver;
mod socket;
mod stats;
use driver::*;
pub use socket::*;

use self::stats::SocketStats;

const DEFAULT_BUFFER_SIZE: usize = 1024;

#[derive(Debug, Error)]
pub enum ReqError {
    #[error("IO error: {0:?}")]
    Io(#[from] std::io::Error),
    #[error("Authentication error: {0:?}")]
    Auth(String),
    #[error("Wire protocol error: {0:?}")]
    Wire(#[from] reqrep::Error),
    #[error("Socket closed")]
    SocketClosed,
    #[error("Transport error: {0:?}")]
    Transport(#[from] Box<dyn std::error::Error + Send + Sync>),
    #[error("Request timed out")]
    Timeout,
}

pub enum Command {
    Send {
        message: Bytes,
        response: oneshot::Sender<Result<Bytes, ReqError>>,
        expiration: Instant,
    },
}

#[derive(Debug, Clone)]
pub struct ReqOptions {
    pub auth_token: Option<Bytes>,
    pub timeout: std::time::Duration,
    pub retry_on_initial_failure: bool,
    pub backoff_duration: std::time::Duration,
    pub retry_attempts: Option<usize>,
    pub set_nodelay: bool,
}

impl ReqOptions {
    /// Sets the authentication token for the socket.
    pub fn with_token(mut self, auth_token: Bytes) -> Self {
        self.auth_token = Some(auth_token);
        self
    }
}

impl Default for ReqOptions {
    fn default() -> Self {
        Self {
            auth_token: None,
            timeout: std::time::Duration::from_secs(5),
            retry_on_initial_failure: true,
            backoff_duration: Duration::from_millis(200),
            retry_attempts: None,
            set_nodelay: true,
        }
    }
}

/// The request socket state, shared between the backend task and the socket.
#[derive(Debug, Default)]
pub(crate) struct SocketState {
    pub(crate) stats: SocketStats,
}
