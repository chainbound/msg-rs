use bytes::Bytes;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::oneshot;

use msg_wire::{
    compression::{CompressionType, Compressor},
    reqrep,
};

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
    },
}

#[derive(Debug, Clone)]
pub struct ReqOptions {
    auth_token: Option<Bytes>,
    /// Timeout duration for requests.
    timeout: std::time::Duration,
    /// Wether to block on initial connection to the target.
    blocking_connect: bool,
    /// The backoff duration for the underlying transport on reconnections.
    backoff_duration: std::time::Duration,
    /// The interval that the request connection should be flushed.
    /// Default is `None`, and the connection is flushed after every send.
    flush_interval: Option<std::time::Duration>,
    /// The maximum number of bytes that can be buffered in the session before being flushed.
    /// This internally sets [`Framed::set_backpressure_boundary`](tokio_util::codec::Framed).
    backpressure_boundary: usize,
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

    /// Sets the flush interval for the socket. A higher flush interval will result in higher throughput,
    /// but at the cost of higher latency. Note that this behaviour can be completely useless if the
    /// `backpressure_boundary` is set too low (which will trigger a flush before the interval is reached).
    pub fn flush_interval(mut self, flush_interval: Duration) -> Self {
        self.flush_interval = Some(flush_interval);
        self
    }

    /// Sets the backpressure boundary for the socket. This is the maximum number of bytes that can be buffered
    /// in the session before being flushed. This internally sets [`Framed::set_backpressure_boundary`](tokio_util::codec::Framed).
    pub fn backpressure_boundary(mut self, backpressure_boundary: usize) -> Self {
        self.backpressure_boundary = backpressure_boundary;
        self
    }

    /// Sets the maximum number of retry attempts. If `None`, all connections will be retried indefinitely.
    pub fn retry_attempts(mut self, retry_attempts: usize) -> Self {
        self.retry_attempts = Some(retry_attempts);
        self
    }

    /// Sets the minimum payload size in bytes for compression to be used. If the payload is smaller than
    /// this threshold, it will not be compressed.
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
            blocking_connect: true,
            backoff_duration: Duration::from_millis(200),
            flush_interval: None,
            backpressure_boundary: 8192,
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

#[allow(unused)]
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
#[derive(Debug, Default)]
pub(crate) struct SocketState {
    pub(crate) stats: SocketStats,
}
