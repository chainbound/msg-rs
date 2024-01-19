use bytes::Bytes;
use rustc_hash::FxHashMap;
use std::net::SocketAddr;
use std::time::Duration;
use std::{collections::VecDeque, sync::Arc};
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;

use msg_transport::Transport;
use msg_wire::reqrep;

use super::{Command, ReqDriver, ReqError, ReqOptions, DEFAULT_BUFFER_SIZE};
use crate::{req::stats::SocketStats, req::SocketState};

#[derive(Debug)]
pub struct ReqSocket<T: Transport> {
    /// Command channel to the backend task.
    to_driver: Option<mpsc::Sender<Command>>,
    /// The socket transport.
    transport: T,
    /// Options for the socket. These are shared with the backend task.
    options: Arc<ReqOptions>,
    /// Socket state. This is shared with the backend task.
    state: Arc<SocketState>,
}

impl<T> ReqSocket<T>
where
    T: Transport + Send + Sync + Unpin + 'static,
{
    pub fn new(transport: T) -> Self {
        Self::with_options(transport, ReqOptions::default())
    }

    pub fn with_options(transport: T, options: ReqOptions) -> Self {
        Self {
            to_driver: None,
            transport,
            options: Arc::new(options),
            state: Arc::new(SocketState::default()),
        }
    }

    pub fn stats(&self) -> &SocketStats {
        &self.state.stats
    }

    pub async fn request(&self, message: Bytes) -> Result<Bytes, ReqError> {
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

        response_rx.await.map_err(|_| ReqError::SocketClosed)?
    }

    /// Connects to the target with the default options.
    pub async fn connect(&mut self, endpoint: SocketAddr) -> Result<(), ReqError> {
        // Initialize communication channels
        let (to_driver, from_socket) = mpsc::channel(DEFAULT_BUFFER_SIZE);

        tracing::info!("Connecting to {}", endpoint);

        let stream = self
            .transport
            .connect(endpoint)
            .await
            .map_err(|e| ReqError::Transport(Box::new(e)))?;

        let mut framed = Framed::new(stream, reqrep::Codec::new());
        framed.set_backpressure_boundary(self.options.backpressure_boundary);

        // Create the socket backend
        let driver: ReqDriver<T> = ReqDriver {
            options: Arc::clone(&self.options),
            id_counter: 0,
            from_socket,
            conn: framed,
            egress_queue: VecDeque::new(),
            // TODO: we should limit the amount of active outgoing requests, and that should be the capacity.
            // If we do this, we'll never have to re-allocate.
            pending_requests: FxHashMap::default(),
            socket_state: Arc::clone(&self.state),
            timeout_check_interval: tokio::time::interval(Duration::from_millis(
                self.options.timeout.as_millis() as u64 / 10,
            )),
            flush_interval: self.options.flush_interval.map(tokio::time::interval),
            should_flush: false,
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
    use msg_transport::tcp::Tcp;
    use std::net::SocketAddr;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tracing::Instrument;

    async fn spawn_listener(sleep_duration: Duration) -> SocketAddr {
        let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();

        let addr = listener.local_addr().unwrap();

        tokio::spawn(
            async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                tracing::info!("Accepted connection");

                let mut buf = [0u8; 1024];
                let b = socket.read(&mut buf).await.unwrap();
                let read = &buf[..b];

                tokio::time::sleep(sleep_duration).await;

                socket.write_all(read).await.unwrap();
                tracing::info!("Sent bytes: {:?}", read);

                socket.flush().await.unwrap();
            }
            .instrument(tracing::info_span!("listener")),
        );

        addr
    }

    #[tokio::test]
    async fn test_req_socket_happy_path() {
        let _ = tracing_subscriber::fmt::try_init();

        let addr = spawn_listener(Duration::from_secs(0)).await;

        let mut socket = ReqSocket::with_options(
            Tcp::default(),
            ReqOptions {
                auth_token: None,
                timeout: Duration::from_secs(1),
                blocking_connect: true,
                backoff_duration: Duration::from_secs(1),
                flush_interval: None,
                backpressure_boundary: 8192,
                retry_attempts: Some(3),
            },
        );

        let connect_result = socket.connect(addr).await;
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
            Tcp::default(),
            ReqOptions {
                auth_token: None,
                timeout: Duration::from_secs(1),
                blocking_connect: true,
                backoff_duration: Duration::from_millis(200),
                backpressure_boundary: 8192,
                flush_interval: None,
                retry_attempts: None,
            },
        );

        let connect_result = socket.connect(addr).await;
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
}
