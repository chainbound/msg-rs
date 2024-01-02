use bytes::Bytes;
use std::net::SocketAddr;
use thiserror::Error;
use tokio::sync::oneshot;

mod driver;
mod socket;
mod stats;
pub use socket::*;
use stats::SocketStats;

const DEFAULT_BUFFER_SIZE: usize = 1024;

#[derive(Debug, Error)]
pub enum PubError {
    #[error("IO error: {0:?}")]
    Io(#[from] std::io::Error),
    #[error("Wire protocol error: {0:?}")]
    Wire(#[from] msg_wire::reqrep::Error),
    #[error("Authentication error: {0}")]
    Auth(String),
    #[error("Socket closed")]
    SocketClosed,
    #[error("Transport error: {0:?}")]
    Transport(#[from] Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Default)]
pub struct RepOptions {
    /// The maximum number of concurrent clients.
    max_clients: Option<usize>,
}

impl RepOptions {
    /// Sets the number of maximum concurrent clients.
    pub fn max_clients(mut self, max_clients: usize) -> Self {
        self.max_clients = Some(max_clients);
        self
    }
}

/// The request socket state, shared between the backend task and the socket.
#[derive(Debug, Default)]
pub(crate) struct SocketState {
    pub(crate) stats: SocketStats,
}

/// A request received by the socket. It contains the source address, the message,
/// and a oneshot channel to respond to the request.
pub struct Request {
    source: SocketAddr,
    response: oneshot::Sender<Bytes>,
    msg: Bytes,
}

impl Request {
    /// Returns the source address of the request.
    pub fn source(&self) -> SocketAddr {
        self.source
    }

    /// Returns a reference to the message.
    pub fn msg(&self) -> &Bytes {
        &self.msg
    }

    /// Responds to the request.
    pub fn respond(self, response: Bytes) -> Result<(), PubError> {
        self.response
            .send(response)
            .map_err(|_| PubError::SocketClosed)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::StreamExt;
    use msg_transport::tcp::{self, Tcp};
    use rand::Rng;

    use crate::{req::ReqSocket, Authenticator};

    use super::*;

    fn localhost() -> SocketAddr {
        "127.0.0.1:0".parse().unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_req_rep() {
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
        tracing::info!("{} reqs in {:?}", n_reqs, elapsed);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_req_rep_durable() {
        let _ = tracing_subscriber::fmt::try_init();
        let random_port = rand::random::<u16>() + 10000;
        let addr = format!("0.0.0.0:{}", random_port);

        // Initialize the request socket (client side) with a transport
        let mut req = ReqSocket::new(Tcp::default());
        // Try to connect even through the server isn't up yet
        req.connect(addr.parse().unwrap()).await.unwrap();

        // Wait a moment to start the server
        tokio::time::sleep(Duration::from_secs(1)).await;
        let mut rep = RepSocket::new(Tcp::default());
        rep.bind(addr.parse().unwrap()).await.unwrap();

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
    async fn test_req_rep_auth() {
        struct Auth;

        impl Authenticator for Auth {
            fn authenticate(&self, _id: &Bytes) -> bool {
                tracing::info!("{:?}", _id);
                true
            }
        }

        let _ = tracing_subscriber::fmt::try_init();
        let mut rep = RepSocket::new(Tcp::default()).with_auth(Auth);
        rep.bind(localhost()).await.unwrap();

        // Initialize socket with a client ID. This will implicitly enable authentication.
        let mut req = ReqSocket::new(Tcp::new(
            tcp::Config::default().auth_token(Bytes::from("REQ")),
        ));

        req.connect(rep.local_addr().unwrap()).await.unwrap();

        tracing::info!("Connected to rep");

        tokio::spawn(async move {
            loop {
                let req = rep.next().await.unwrap();
                tracing::debug!("Received request");

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
        tracing::info!("{} reqs in {:?}", n_reqs, elapsed);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_rep_max_connections() {
        let _ = tracing_subscriber::fmt::try_init();
        let mut rep = RepSocket::with_options(Tcp::default(), RepOptions::default().max_clients(1));
        rep.bind("127.0.0.1:0".parse().unwrap()).await.unwrap();

        let mut req1 = ReqSocket::new(Tcp::default());
        req1.connect(rep.local_addr().unwrap()).await.unwrap();

        let mut req2 = ReqSocket::new(Tcp::default());
        req2.connect(rep.local_addr().unwrap()).await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(rep.stats().active_clients(), 1);
    }
}
