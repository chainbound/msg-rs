use std::{
    sync::{Arc, atomic::AtomicUsize},
    time::Duration,
};

use arc_swap::ArcSwap;
use bytes::Bytes;
use thiserror::Error;
use tokio::sync::oneshot;

use msg_common::{constants::KiB, span::WithSpan};
use msg_wire::{
    compression::{CompressionType, Compressor},
    reqrep,
};

mod conn_manager;
mod driver;
mod socket;
mod stats;
pub use socket::*;

use crate::{Profile, stats::SocketStats};
use stats::ReqStats;

use crate::DEFAULT_BUFFER_SIZE;

pub(crate) static DRIVER_ID: AtomicUsize = AtomicUsize::new(0);

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
    #[error("High-water mark reached")]
    HighWaterMarkReached,
}

/// A command to send a request message and wait for a response.
#[derive(Debug)]
pub struct SendCommand {
    /// The request message to send.
    pub message: WithSpan<ReqMessage>,
    /// The channel to send the peer's response back.
    pub response: oneshot::Sender<Result<Bytes, ReqError>>,
}

impl SendCommand {
    /// Creates a new send command.
    pub fn new(
        message: WithSpan<ReqMessage>,
        response: oneshot::Sender<Result<Bytes, ReqError>>,
    ) -> Self {
        Self { message, response }
    }
}

/// Options for the connection manager.
#[derive(Debug, Clone)]
pub struct ConnOptions {
    /// Optional authentication token.
    pub auth_token: Option<Bytes>,
    /// The backoff duration for the underlying transport on reconnections.
    pub backoff_duration: Duration,
    /// The maximum number of retry attempts. If `None`, the connection will retry indefinitely.
    pub retry_attempts: Option<usize>,
}

impl Default for ConnOptions {
    fn default() -> Self {
        Self {
            auth_token: None,
            // These values give a good default for most use cases.
            //
            // * formula: w_i = w_0 * 2^i
            // * w_0 = 200ms, i = 0..9
            // * worst-case total wait: sum(w_i) = 200ms * (2^9 - 1) = 102.2s
            backoff_duration: Duration::from_millis(200),
            retry_attempts: Some(9),
        }
    }
}

/// The request socket options.
#[derive(Debug, Clone)]
pub struct ReqOptions {
    /// Options for the connection manager.
    pub conn: ConnOptions,
    /// Timeout duration for requests.
    pub timeout: Duration,
    /// Wether to block on initial connection to the target.
    pub blocking_connect: bool,
    /// Minimum payload size in bytes for compression to be used.
    /// If the payload is smaller than this threshold, it will not be compressed.
    pub min_compress_size: usize,
    /// The size of the write buffer in bytes.
    pub write_buffer_size: usize,
    /// The linger duration for the write buffer (how long to wait before flushing).
    pub write_buffer_linger: Option<Duration>,
    /// High-water mark for pending requests. When this limit is reached, new requests
    /// will return [`ReqError::HighWaterMarkReached`].
    /// If `None`, there is no limit (unbounded).
    pub pending_requests_hwm: Option<usize>,
}

impl ReqOptions {
    /// Creates new options based on the given profile.
    pub fn new(profile: Profile) -> Self {
        match profile {
            Profile::Latency => Self::low_latency(),
            Profile::Throughput => Self::high_throughput(),
            Profile::Balanced => Self::balanced(),
        }
    }

    /// Creates options optimized for low latency.
    pub fn low_latency() -> Self {
        Self {
            write_buffer_size: 8 * KiB as usize,
            write_buffer_linger: Some(Duration::from_micros(50)),
            ..Default::default()
        }
    }

    /// Creates options optimized for high throughput.
    pub fn high_throughput() -> Self {
        Self {
            write_buffer_size: 256 * KiB as usize,
            write_buffer_linger: Some(Duration::from_micros(200)),
            ..Default::default()
        }
    }

    /// Creates options optimized for a balanced trade-off between latency and throughput.
    pub fn balanced() -> Self {
        Self {
            write_buffer_size: 32 * KiB as usize,
            write_buffer_linger: Some(Duration::from_micros(100)),
            ..Default::default()
        }
    }
}

impl ReqOptions {
    /// Sets the authentication token for the socket.
    pub fn with_auth_token(mut self, auth_token: Bytes) -> Self {
        self.conn.auth_token = Some(auth_token);
        self
    }

    /// Sets the timeout for the socket.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Enables blocking initial connections to the target.
    pub fn with_blocking_connect(mut self) -> Self {
        self.blocking_connect = true;
        self
    }

    /// Sets the backoff duration for the socket.
    pub fn with_backoff_duration(mut self, backoff_duration: Duration) -> Self {
        self.conn.backoff_duration = backoff_duration;
        self
    }

    /// Sets the maximum number of retry attempts.
    ///
    /// If `None`, all connections will be retried indefinitely.
    pub fn with_retry_attempts(mut self, retry_attempts: usize) -> Self {
        self.conn.retry_attempts = Some(retry_attempts);
        self
    }

    /// Sets the minimum payload size in bytes for compression to be used.
    ///
    /// If the payload is smaller than this threshold, it will not be compressed.
    pub fn with_min_compress_size(mut self, min_compress_size: usize) -> Self {
        self.min_compress_size = min_compress_size;
        self
    }

    /// Sets the size (max capacity) of the write buffer in bytes.
    /// When the buffer is full, it will be flushed to the underlying transport.
    ///
    /// Default: 8KiB
    pub fn with_write_buffer_size(mut self, size: usize) -> Self {
        self.write_buffer_size = size;
        self
    }

    /// Sets the linger duration for the write buffer. If `None`, the write buffer will only be
    /// flushed when the buffer is full.
    ///
    /// Default: 100µs
    pub fn with_write_buffer_linger(mut self, duration: Option<Duration>) -> Self {
        self.write_buffer_linger = duration;
        self
    }

    /// Sets the high-water mark for pending requests. When this limit is reached, new requests
    /// will return [`ReqError::HighWaterMarkReached`].
    ///
    /// If `None`, there is no limit (unbounded).
    ///
    /// Default: `None`
    pub fn with_pending_requests_hwm(mut self, hwm: usize) -> Self {
        self.pending_requests_hwm = Some(hwm);
        self
    }
}

impl Default for ReqOptions {
    fn default() -> Self {
        Self {
            conn: ConnOptions::default(),
            timeout: Duration::from_secs(5),
            blocking_connect: false,
            min_compress_size: 8192,
            write_buffer_size: 8192,
            write_buffer_linger: Some(Duration::from_micros(100)),
            pending_requests_hwm: None,
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
