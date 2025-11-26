use arc_swap::ArcSwap;
use bytes::Bytes;
use std::{sync::Arc, time::Duration};
use thiserror::Error;
use tokio::sync::oneshot;

use msg_wire::{
    compression::{CompressionType, Compressor},
    reqrep,
};

mod driver;
mod socket;
mod stats;
pub use socket::*;

use crate::stats::SocketStats;
use stats::ReqStats;

/// The default buffer size for the socket.
const DEFAULT_BUFFER_SIZE: usize = 1024;

/// Errors that can occur when using a request socket.
#[derive(Debug, Error)]
pub enum ReqError {
    #[error("IO error: {0:?}")]
    Io(#[from] std::io::Error),
    #[error("Wire protocol error: {0:?}")]
    Wire(#[from] reqrep::Error),
    #[error("Authentication error: {0}")]
    Auth(String),
    #[error("Socket closed")]
    SocketClosed,
    #[error("Request timed out")]
    Timeout,
    #[error("Could not connect to any valid endpoints")]
    NoValidEndpoints,
    #[error("Failed to connect to the target endpoint: {0:?}")]
    Connect(Box<dyn std::error::Error + Send + Sync>),
}

/// Commands that can be sent to the request socket driver.
pub enum Command {
    /// Send a request message and wait for a response.
    Send { message: ReqMessage, response: oneshot::Sender<Result<Bytes, ReqError>> },
}

/// The request socket options.
#[derive(Debug, Clone)]
pub struct ReqOptions {
    /// Optional authentication token.
    auth_token: Option<Bytes>,
    /// Timeout duration for requests.
    timeout: std::time::Duration,
    /// Wether to block on initial connection to the target.
    blocking_connect: bool,
    /// The backoff duration for the underlying transport on reconnections.
    backoff_duration: std::time::Duration,
    /// The maximum number of retry attempts. If `None`, the connection will retry indefinitely.
    retry_attempts: Option<usize>,
    /// Minimum payload size in bytes for compression to be used. If the payload is smaller than
    /// this threshold, it will not be compressed.
    min_compress_size: usize,
}

impl ReqOptions {
    /// Sets the authentication token for the socket.
    pub fn auth_token(mut self, auth_token: Bytes) -> Self {
        self.auth_token = Some(auth_token);
        self
    }

    /// Sets the timeout for the socket.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Enables blocking initial connections to the target.
    pub fn blocking_connect(mut self) -> Self {
        self.blocking_connect = true;
        self
    }

    /// Sets the backoff duration for the socket.
    pub fn backoff_duration(mut self, backoff_duration: Duration) -> Self {
        self.backoff_duration = backoff_duration;
        self
    }

    /// Sets the maximum number of retry attempts. If `None`, all connections will be retried
    /// indefinitely.
    pub fn retry_attempts(mut self, retry_attempts: usize) -> Self {
        self.retry_attempts = Some(retry_attempts);
        self
    }

    /// Sets the minimum payload size in bytes for compression to be used. If the payload is smaller
    /// than this threshold, it will not be compressed.
    pub fn min_compress_size(mut self, min_compress_size: usize) -> Self {
        self.min_compress_size = min_compress_size;
        self
    }
}

impl Default for ReqOptions {
    fn default() -> Self {
        Self {
            auth_token: None,
            timeout: std::time::Duration::from_secs(5),
            blocking_connect: false,
            backoff_duration: Duration::from_millis(200),
            retry_attempts: None,
            min_compress_size: 8192,
        }
    }
}

/// A message sent from a [`ReqSocket`] to the backend task.
#[derive(Debug, Clone)]
pub struct ReqMessage {
    compression_type: CompressionType,
    payload: Bytes,
}

impl ReqMessage {
    pub fn new(payload: Bytes) -> Self {
        Self {
            // Initialize the compression type to None.
            // The actual compression type will be set in the `compress` method.
            compression_type: CompressionType::None,
            payload,
        }
    }

    #[inline]
    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    #[inline]
    pub fn into_payload(self) -> Bytes {
        self.payload
    }

    #[inline]
    pub fn into_wire(self, id: u32) -> reqrep::Message {
        reqrep::Message::new(id, self.compression_type as u8, self.payload)
    }

    #[inline]
    pub fn compress(&mut self, compressor: &dyn Compressor) -> Result<(), ReqError> {
        self.payload = compressor.compress(&self.payload)?;
        self.compression_type = compressor.compression_type();

        Ok(())
    }
}

/// The request socket state, shared between the backend task and the socket.
/// Generic over the transport-level stats type.
#[derive(Debug, Default)]
pub(crate) struct SocketState<S: Default> {
    /// The socket stats.
    pub(crate) stats: Arc<SocketStats<ReqStats>>,
    /// The transport-level stats. We wrap the inner stats in an `Arc`
    /// for cheap clone on read.
    pub(crate) transport_stats: Arc<ArcSwap<S>>,
}

// Manual clone implementation needed here because `S` is not `Clone`.
impl<S: Default> Clone for SocketState<S> {
    fn clone(&self) -> Self {
        Self { stats: Arc::clone(&self.stats), transport_stats: self.transport_stats.clone() }
    }
}
