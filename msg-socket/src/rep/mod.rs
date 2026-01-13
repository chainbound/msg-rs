use std::{io, time::Duration};

use bytes::Bytes;
use msg_common::constants::KiB;
use msg_transport::Address;
use thiserror::Error;
use tokio::sync::oneshot;

mod driver;
mod socket;
mod stats;
use crate::{Profile, stats::SocketStats};
pub use socket::*;
use stats::RepStats;

const DEFAULT_MIN_COMPRESS_SIZE: usize = 8192;

/// Errors that can occur when using a reply socket.
#[derive(Debug, Error)]
pub enum RepError {
    #[error("IO error: {0:?}")]
    Io(#[from] std::io::Error),
    #[error("Wire protocol error: {0:?}")]
    Wire(#[from] msg_wire::reqrep::Error),
    #[error("Authentication error: {0}")]
    Auth(String),
    #[error("Socket closed")]
    SocketClosed,
    #[error("Could not connect to any valid endpoints")]
    NoValidEndpoints,
}

impl RepError {
    pub fn is_connection_reset(&self) -> bool {
        match self {
            Self::Io(e) | Self::Wire(msg_wire::reqrep::Error::Io(e)) => {
                e.kind() == io::ErrorKind::ConnectionReset
            }
            _ => false,
        }
    }
}

/// The reply socket options.
pub struct RepOptions {
    /// The maximum number of concurrent clients.
    pub(crate) max_clients: Option<usize>,
    pub(crate) min_compress_size: usize,
    pub(crate) write_buffer_size: usize,
    pub(crate) write_buffer_linger: Option<Duration>,
    /// High-water mark for pending responses per peer. When this limit is reached,
    /// new requests will not be read from the underlying connection until pending
    /// responses are fulfilled. If `None`, there is no limit (unbounded).
    pub(crate) pending_responses_hwm: Option<usize>,
}

impl Default for RepOptions {
    fn default() -> Self {
        Self {
            max_clients: None,
            min_compress_size: DEFAULT_MIN_COMPRESS_SIZE,
            write_buffer_size: 8192,
            write_buffer_linger: Some(Duration::from_micros(100)),
            pending_responses_hwm: None,
        }
    }
}

impl RepOptions {
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

impl RepOptions {
    /// Sets the number of maximum concurrent clients.
    pub fn with_max_clients(mut self, max_clients: usize) -> Self {
        self.max_clients = Some(max_clients);
        self
    }

    /// Sets the minimum payload size for compression.
    /// If the payload is smaller than this value, it will not be compressed.
    pub fn with_min_compress_size(mut self, min_compress_size: usize) -> Self {
        self.min_compress_size = min_compress_size;
        self
    }

    /// Sets the size (max capacity) of the write buffer in bytes. When the buffer is full, it will
    /// be flushed to the underlying transport.
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

    /// Sets the high-water mark for pending responses per peer. When this limit is reached,
    /// new requests will not be read from the underlying connection until pending
    /// responses are fulfilled. If `None`, there is no limit (unbounded).
    ///
    /// Default: `None`
    pub fn with_pending_responses_hwm(mut self, hwm: usize) -> Self {
        self.pending_responses_hwm = Some(hwm);
        self
    }
}

/// The request socket state, shared between the backend task and the socket.
#[derive(Debug, Default)]
pub(crate) struct SocketState {
    pub(crate) stats: SocketStats<RepStats>,
}

/// A request received by the socket.
pub struct Request<A: Address> {
    /// The source address of the request.
    source: A,
    /// The compression type used for the request payload
    compression_type: u8,
    /// The oneshot channel to respond to the request.
    response: oneshot::Sender<Bytes>,
    /// The message payload.
    msg: Bytes,
}

impl<A: Address> Request<A> {
    /// Returns the source address of the request.
    pub fn source(&self) -> &A {
        &self.source
    }

    /// Returns a reference to the message.
    pub fn msg(&self) -> &Bytes {
        &self.msg
    }

    /// Responds to the request.
    pub fn respond(self, response: Bytes) -> Result<(), RepError> {
        self.response.send(response).map_err(|_| RepError::SocketClosed)
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, time::Duration};

    use futures::StreamExt;
    use msg_transport::tcp::Tcp;
    use msg_wire::compression::{GzipCompressor, SnappyCompressor};
    use rand::Rng;
    use tracing::{debug, info};

    use crate::{Authenticator, ReqOptions, req::ReqSocket};

    use super::*;

    fn localhost() -> SocketAddr {
        "127.0.0.1:0".parse().unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn reqrep_simple() {
        let _ = tracing_subscriber::fmt::try_init();
        let mut rep = RepSocket::new(Tcp::default());
        rep.bind(localhost()).await.unwrap();

        let mut req = ReqSocket::new(Tcp::default());
        req.connect(rep.local_addr().unwrap()).await.unwrap();

        tokio::spawn(async move {
            loop {
                let req = rep.next().await.unwrap();

                req.respond(Bytes::from("hello")).unwrap();
            }
        });

        let n_reqs = 1000;
        let mut rng = rand::rng();
        let msg_vec: Vec<Bytes> = (0..n_reqs)
            .map(|_| {
                let mut vec = vec![0u8; 512];
                rng.fill(&mut vec[..]);
                Bytes::from(vec)
            })
            .collect();

        let start = std::time::Instant::now();
        for msg in msg_vec {
            let _res = req.request(msg).await.unwrap();
            // println!("Response: {:?} {:?}", _res, req_start.elapsed());
        }
        let elapsed = start.elapsed();
        info!("{} reqs in {:?}", n_reqs, elapsed);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn reqrep_durable() {
        let _ = tracing_subscriber::fmt::try_init();
        let random_port = rand::rng().random_range(10000..65535);
        let addr = format!("0.0.0.0:{random_port}");

        // Initialize the request socket (client side) with a transport
        let mut req = ReqSocket::new(Tcp::default());
        // Try to connect even through the server isn't up yet
        let endpoint = addr.clone();
        let connection_attempt = tokio::spawn(async move {
            req.connect(endpoint).await.unwrap();

            req
        });

        // Wait a moment to start the server
        tokio::time::sleep(Duration::from_millis(500)).await;
        let mut rep = RepSocket::new(Tcp::default());
        rep.bind(addr).await.unwrap();

        let req = connection_attempt.await.unwrap();

        tokio::spawn(async move {
            // Receive the request and respond with "world"
            // RepSocket implements `Stream`
            let req = rep.next().await.unwrap();
            println!("Message: {:?}", req.msg());

            req.respond(Bytes::from("world")).unwrap();
        });

        let _ = req.request(Bytes::from("hello")).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn reqrep_auth() {
        struct Auth;

        impl Authenticator for Auth {
            fn authenticate(&self, _id: &Bytes) -> bool {
                info!("{:?}", _id);
                true
            }
        }

        let _ = tracing_subscriber::fmt::try_init();
        let mut rep = RepSocket::new(Tcp::default()).with_auth(Auth);
        rep.bind(localhost()).await.unwrap();

        // Initialize socket with a client ID. This will implicitly enable authentication.
        let mut req = ReqSocket::with_options(
            Tcp::default(),
            ReqOptions::default().with_auth_token(Bytes::from("REQ")),
        );

        req.connect(rep.local_addr().unwrap()).await.unwrap();

        info!("Connected to rep");

        tokio::spawn(async move {
            loop {
                let req = rep.next().await.unwrap();
                debug!("Received request");

                req.respond(Bytes::from("hello")).unwrap();
            }
        });

        let n_reqs = 1000;
        let mut rng = rand::rng();
        let msg_vec: Vec<Bytes> = (0..n_reqs)
            .map(|_| {
                let mut vec = vec![0u8; 512];
                rng.fill(&mut vec[..]);
                Bytes::from(vec)
            })
            .collect();

        let start = std::time::Instant::now();
        for msg in msg_vec {
            let _res = req.request(msg).await.unwrap();
        }
        let elapsed = start.elapsed();
        info!("{} reqs in {:?}", n_reqs, elapsed);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn rep_max_connections() {
        let _ = tracing_subscriber::fmt::try_init();
        let mut rep =
            RepSocket::with_options(Tcp::default(), RepOptions::default().with_max_clients(1));
        rep.bind("127.0.0.1:0").await.unwrap();
        let addr = rep.local_addr().unwrap();

        let mut req1 = ReqSocket::new(Tcp::default());
        req1.connect(addr).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(rep.stats().active_clients(), 1);

        let mut req2 = ReqSocket::new(Tcp::default());
        req2.connect(addr).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(rep.stats().active_clients(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_basic_reqrep_with_compression() {
        let mut rep = RepSocket::with_options(
            Tcp::default(),
            RepOptions::default().with_min_compress_size(0),
        )
        .with_compressor(SnappyCompressor);

        rep.bind("0.0.0.0:4445").await.unwrap();

        let mut req = ReqSocket::with_options(
            Tcp::default(),
            ReqOptions::default().with_min_compress_size(0),
        )
        .with_compressor(GzipCompressor::new(6));

        req.connect("0.0.0.0:4445").await.unwrap();

        tokio::spawn(async move {
            let req = rep.next().await.unwrap();

            assert_eq!(req.msg(), &Bytes::from("hello"));
            req.respond(Bytes::from("world")).unwrap();
        });

        let res: Bytes = req.request(Bytes::from("hello")).await.unwrap();
        assert_eq!(res, Bytes::from("world"));
    }
}
