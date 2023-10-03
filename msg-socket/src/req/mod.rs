use bytes::Bytes;
use parking_lot::RwLock;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::oneshot;

use msg_wire::reqrep;

mod driver;
mod socket;
mod stats;
// pub(crate) use backend::*;
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
}

pub enum Command {
    Send {
        message: Bytes,
        response: oneshot::Sender<Result<Bytes, ReqError>>,
    },
}

#[derive(Debug, Clone)]
pub struct ReqOptions {
    pub client_id: Option<Bytes>,
    pub timeout: std::time::Duration,
    pub retry_on_initial_failure: bool,
    pub backoff_duration: std::time::Duration,
    pub retry_attempts: Option<usize>,
    pub set_nodelay: bool,
}

impl ReqOptions {
    pub fn with_id(mut self, client_id: Bytes) -> Self {
        self.client_id = Some(client_id);
        self
    }
}

impl Default for ReqOptions {
    fn default() -> Self {
        Self {
            client_id: None,
            timeout: std::time::Duration::from_secs(5),
            retry_on_initial_failure: true,
            backoff_duration: Duration::from_millis(200),
            retry_attempts: None,
            set_nodelay: true,
        }
    }
}

/// The request socket state, shared between the backend task and the socket.
#[derive(Debug)]
pub(crate) struct SocketState {
    pub(crate) stats: SocketStats,
    pub(crate) connection_state: RwLock<ConnectionState>,
}

/// Current connection state of the socket.
#[derive(Debug)]
pub(crate) enum ConnectionState {
    /// The socket is connected to the server.
    Connected,
    /// The socket is disconnected, and the backend task is trying to reconnect
    /// for the `n`th time.
    Disconnected(usize),
    /// The socket has exhausted the number of reconnection attempts or encountered
    /// a fatal error.
    Fatal(ReqError),
}

impl Default for SocketState {
    fn default() -> Self {
        Self {
            stats: SocketStats::default(),
            connection_state: RwLock::new(ConnectionState::Disconnected(0)),
        }
    }
}
