use bytes::Bytes;
use msg_transport::ClientTransport;
use msg_wire::reqrep;
use rustc_hash::FxHashMap;
use std::{collections::VecDeque, sync::Arc};
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::{req::stats::SocketStats, req::SocketState};

use super::{Command, ReqDriver, ReqError, ReqOptions, DEFAULT_BUFFER_SIZE};

#[derive(Debug, Clone)]
pub struct ReqSocket<T: ClientTransport> {
    /// Command channel to the backend task.
    to_driver: Option<mpsc::Sender<Command>>,
    /// The underlying transport.
    transport: T,
    /// Options for the socket. These are shared with the backend task.
    options: Arc<ReqOptions>,
    /// Socket state. This is shared with the backend task.
    state: Arc<SocketState>,
    /// The currently active requests.
    active_requests: Arc<AtomicUsize>,
}

impl<T: ClientTransport> ReqSocket<T> {
    pub fn new(transport: T) -> Self {
        Self::with_options(transport, ReqOptions::default())
    }

    pub fn with_options(transport: T, options: ReqOptions) -> Self {
        Self {
            to_driver: None,
            transport,
            options: Arc::new(options),
            state: Arc::new(SocketState::default()),
            active_requests: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn stats(&self) -> &SocketStats {
        &self.state.stats
    }

    pub async fn request(&self, message: Bytes) -> Result<Bytes, ReqError> {
        println!("Active requests: {}", self.active_requests.load(Ordering::Relaxed));

        if self.active_requests.load(Ordering::Relaxed) >= self.options.max_pending_requests {
            return Err(ReqError::TooManyRequests);
        } 

        let (response_tx, response_rx) = oneshot::channel();

        self.to_driver
            .as_ref()
            .ok_or(ReqError::SocketClosed)?
            .send(Command::Send {
                message,
                response: response_tx,
            })
            .await
            .map_err(|_| ReqError::SocketClosed)?;

            println!("Active requests after sending: {}", self.active_requests.load(Ordering::Relaxed));

            response_rx.await.map_err(|_| ReqError::SocketClosed)?
    }

    /// Connects to the target with the default options.
    pub async fn connect(&mut self, endpoint: &str) -> Result<(), ReqError> {
        // Initialize communication channels
        let (to_driver, from_socket) = mpsc::channel(DEFAULT_BUFFER_SIZE);
        let active_requests = self.active_requests.clone();

        let endpoint = endpoint.parse().map_err(|_| ReqError::InvalidEndpoint(endpoint.to_string()))?;

        tracing::debug!("Connected to {}", endpoint);

        let stream = self
            .transport
            .connect_with_auth(endpoint, self.options.auth_token.clone())
            .await
            .map_err(|e| ReqError::Transport(Box::new(e)))?;

        // Create the socket backend
        let mut pending_requests = FxHashMap::default();
        pending_requests.reserve(self.options.max_pending_requests);
        let driver = ReqDriver {
            options: Arc::clone(&self.options),
            id_counter: 0,
            from_socket,
            conn: Framed::new(stream, reqrep::Codec::new()),
            egress_queue: VecDeque::with_capacity(self.options.max_pending_requests),
            pending_requests,
            socket_state: Arc::clone(&self.state),
            timeout_check_interval: tokio::time::interval(self.options.timeout / 10),
            active_requests,
        };

        // Spawn the backend task
        tokio::spawn(driver);

        self.to_driver = Some(to_driver);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use msg_transport::Tcp;
    use std::net::SocketAddr;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn spawn_listener(sleep_duration: Duration) -> SocketAddr {
        let listener = TcpListener::bind("0.0.0.0:0").await.expect("Failed to bind listener");
    
        let addr = listener.local_addr().expect("Failed to get local address");
    
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((mut socket, _)) => {
                        tokio::spawn(async move {
                            let mut buf = [0u8; 1024];
                            match socket.read(&mut buf).await {
                                Ok(b) => {
                                    let read = &buf[..b];
                                    tokio::time::sleep(sleep_duration).await;
                                    if socket.write_all(read).await.is_ok() {
                                        tracing::info!("Sent bytes: {:?}", read);
                                    }
                                }
                                Err(e) => tracing::error!("Failed to read from socket: {:?}", e),
                            }
                        });
                    }
                    Err(e) => tracing::error!("Failed to accept connection: {:?}", e),
                }
            }
        });
    
        addr
    }
    
    #[tokio::test]
    async fn test_req_socket_happy_path() {
        let _ = tracing_subscriber::fmt::try_init();

        let addr = spawn_listener(Duration::from_secs(0)).await;

        let mut socket = ReqSocket::with_options(
            Tcp::new(),
            ReqOptions {
                auth_token: None,
                timeout: Duration::from_secs(1),
                retry_on_initial_failure: true,
                backoff_duration: Duration::from_secs(1),
                retry_attempts: Some(3),
                set_nodelay: true,
                max_pending_requests: 1,
            },
        );

        let addr_str = addr.to_string();
        let connect_result = socket.connect(&addr_str).await;
        assert!(
            connect_result.is_ok(),
            "Failed to connect: {:?}",
            connect_result.err()
        );

        let request = Bytes::from_static(b"test request");
        let response = socket.request(request.clone()).await;

        assert!(response.is_ok(), "Request failed: {:?}", response.err());
        assert_eq!(
            response.unwrap(),
            request,
            "Response does not match request"
        );
    }

    #[tokio::test]
    async fn test_req_socket_timeout() {
        let _ = tracing_subscriber::fmt::try_init();

        let addr = spawn_listener(Duration::from_secs(3)).await;

        let mut socket = ReqSocket::with_options(
            Tcp::new(),
            ReqOptions {
                auth_token: None,
                timeout: Duration::from_secs(1),
                retry_on_initial_failure: true,
                backoff_duration: Duration::from_secs(0),
                retry_attempts: None,
                set_nodelay: true,
                max_pending_requests: 100,
            },
        );

        let addr_str = addr.to_string();
        let connect_result = socket.connect(&addr_str).await;
        assert!(
            connect_result.is_ok(),
            "Failed to connect: {:?}",
            connect_result.err()
        );

        let request = Bytes::from_static(b"test request");
        let response = socket.request(request.clone()).await;

        assert!(
            response.is_err(),
            "Request succeeded when it should have timed out: {:?}",
            response.ok()
        );
    }

    #[tokio::test]
    async fn test_req_socket_too_many_requests() {
        use crate::RepSocket;
        use tokio_stream::StreamExt;

        let _ = tracing_subscriber::fmt::try_init();
    
        let mut rep = RepSocket::new(Tcp::new());
        rep.bind("0.0.0.0:0").await.unwrap();
        
        let addr = rep.local_addr().unwrap();
        
        tokio::spawn(async move {
            while let Some(request) = rep.next().await {
                tokio::time::sleep(Duration::from_millis(100)).await;
                request.respond(Bytes::from("test response")).unwrap();
            }
        });

        let max_pending_requests = 10;
        let mut socket = ReqSocket::with_options(
            Tcp::new(),
            ReqOptions {
                auth_token: None,
                timeout: Duration::from_secs(1),
                retry_on_initial_failure: true,
                backoff_duration: Duration::from_secs(1),
                retry_attempts: Some(3),
                set_nodelay: true,
                max_pending_requests,
            },
        );
    
        let addr_str = addr.to_string();
        let connect_result = socket.connect(&addr_str).await;
        assert!(
            connect_result.is_ok(),
            "Failed to connect: {:?}",
            connect_result.err()
        );
    
        let request = Bytes::from_static(b"test request");
        let mut futures = Vec::new();
    
        for i in 0..=max_pending_requests {
            let delay: u64 = (i * 10).try_into().unwrap();
            tokio::time::sleep(Duration::from_millis(delay)).await;
            let future = socket.request(request.clone());
            futures.push(future);
        }

        let results = futures::future::join_all(futures).await;
    
        for result in results {
            if let Err(e) = result {
                assert!(matches!(e, ReqError::TooManyRequests), "Unexpected error: {:?}", e);
            } else {
                panic!("Request succeeded when it should have failed");
            }
        }
    } 
}
