use bytes::Bytes;
use msg_transport::Address;
use thiserror::Error;
use tokio::sync::oneshot;

mod driver;
mod socket;
mod stats;
use crate::stats::SocketStats;
pub use socket::*;
use stats::RepStats;

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

/// The reply socket options.
pub struct RepOptions {
    /// The maximum number of concurrent clients.
    max_clients: Option<usize>,
    min_compress_size: usize,
}

impl Default for RepOptions {
    fn default() -> Self {
        Self { max_clients: None, min_compress_size: 8192 }
    }
}

impl RepOptions {
    /// Sets the number of maximum concurrent clients.
    pub fn max_clients(mut self, max_clients: usize) -> Self {
        self.max_clients = Some(max_clients);
        self
    }

    /// Sets the minimum payload size for compression.
    /// If the payload is smaller than this value, it will not be compressed.
    pub fn min_compress_size(mut self, min_compress_size: usize) -> Self {
        self.min_compress_size = min_compress_size;
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

    use crate::{req::ReqSocket, Authenticator, ReqOptions};

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
        let mut rng = rand::thread_rng();
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
        let random_port = rand::random::<u16>() + 10000;
        let addr = format!("0.0.0.0:{}", random_port);

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
            ReqOptions::default().auth_token(Bytes::from("REQ")),
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
        let mut rng = rand::thread_rng();
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
        let mut rep = RepSocket::with_options(Tcp::default(), RepOptions::default().max_clients(1));
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
        let mut rep =
            RepSocket::with_options(Tcp::default(), RepOptions::default().min_compress_size(0))
                .with_compressor(SnappyCompressor);

        rep.bind("0.0.0.0:4445").await.unwrap();

        let mut req =
            ReqSocket::with_options(Tcp::default(), ReqOptions::default().min_compress_size(0))
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
