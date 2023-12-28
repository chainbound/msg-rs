use std::{
    io,
    net::{SocketAddr, UdpSocket},
    pin::Pin,
    sync::Arc,
    task::{ready, Poll},
};

use bytes::Bytes;
use futures::future::BoxFuture;
use quinn::{self, ClientConfig, Endpoint};
use thiserror::Error;
use tokio::sync::mpsc::{self, Receiver};
use tracing::error;

use crate::Transport;

mod config;
mod stream;
mod tls;

use stream::QuicStream;
use tls::unsafe_tls_config;

/// Options for outgoing connections.
/// By default, unsafe TLS configuration is used (no server verification, no client authentication).
// TODO: additional configuration options related to transport etc
#[derive(Debug, Clone)]
pub struct QuicConnectOptions {
    pub blocking_connect: bool,
    /// Optional authentication message.
    pub auth_token: Option<Bytes>,
    pub client_config: quinn::ClientConfig,
    pub local_addr: SocketAddr,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub endpoint_config: quinn::EndpointConfig,
    pub client_config: quinn::ClientConfig,
    pub server_config: quinn::ServerConfig,
}

impl Default for QuicConnectOptions {
    fn default() -> Self {
        Self {
            blocking_connect: false,
            auth_token: None,
            local_addr: SocketAddr::from(([0, 0, 0, 0], 0)),
            client_config: ClientConfig::new(Arc::new(unsafe_tls_config())),
        }
    }
}

impl QuicConnectOptions {
    /// Sets the auth token for this connection.
    pub fn auth_token(mut self, auth: Bytes) -> Self {
        self.auth_token = Some(auth);
        self
    }

    /// Connect synchronously.
    pub fn blocking_connect(mut self) -> Self {
        self.blocking_connect = true;
        self
    }

    /// Sets the local address for this connection.
    pub fn local_addr(mut self, addr: SocketAddr) -> Self {
        self.local_addr = addr;
        self
    }
}

/// A QUIC transport that implements both [`ClientTransport`] and [`ServerTransport`].
pub struct Quic {
    config: Config,
    endpoint: Option<quinn::Endpoint>,

    /// A receiver for incoming connections waiting to be handled.
    incoming: Option<Receiver<Result<quinn::Connecting, Error>>>,
}

/// A QUIC error.
#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Connect(#[from] quinn::ConnectError),
    #[error(transparent)]
    Connection(#[from] quinn::ConnectionError),
    #[error("Endpoint closed")]
    ClosedEndpoint,
}

impl Quic {
    /// Creates a new QUIC transport.
    pub fn new(config: Config) -> Self {
        Self {
            config,
            endpoint: None,
            incoming: None,
        }
    }

    fn new_endpoint(
        &self,
        addr: Option<SocketAddr>,
        server_config: Option<quinn::ServerConfig>,
    ) -> Result<Endpoint, Error> {
        let socket = UdpSocket::bind(addr.unwrap_or(SocketAddr::from(([0, 0, 0, 0], 0))))?;

        let endpoint = quinn::Endpoint::new(
            self.config.endpoint_config.clone(),
            server_config,
            socket,
            Arc::new(quinn::TokioRuntime),
        )?;

        Ok(endpoint)
    }
}

#[async_trait::async_trait]
impl Transport for Quic {
    type Output = QuicStream;

    type Error = Error;

    type Connect = BoxFuture<'static, Result<Self::Output, Self::Error>>;
    type Accept = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    /// Binds a QUIC endpoint to the given address.
    async fn bind(&mut self, addr: SocketAddr) -> Result<(), Self::Error> {
        let endpoint = Endpoint::server(self.config.server_config.clone(), addr)?;

        self.endpoint = Some(endpoint);

        Ok(())
    }

    /// Connects to the given address, returning a future representing a pending outbound connection.
    /// If the endpoint is not bound, it will be bound to the default address.
    fn connect(&mut self, addr: SocketAddr) -> Result<Self::Connect, Self::Error> {
        // If we have an endpoint, use it. Otherwise, create a new one.
        let endpoint = if let Some(endpoint) = self.endpoint.clone() {
            endpoint
        } else {
            let endpoint = self.new_endpoint(None, None)?;
            self.endpoint = Some(endpoint.clone());

            endpoint
        };

        let client_config = self.config.client_config.clone();

        Ok(Box::pin(async move {
            // This `"l"` seems necessary because an empty string is an invalid domain
            // name. While we don't use domain names, the underlying rustls library
            // is based upon the assumption that we do.
            let connection = endpoint
                .connect_with(client_config, addr, "l")?
                .await
                .map_err(Error::from)?;

            // Open a bi-directional stream and return it. We'll think about multiplexing per topic later.
            connection
                .open_bi()
                .await
                .map(|(send, recv)| QuicStream { send, recv })
                .map_err(Error::from)
        }))
    }

    fn poll_accept(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<Self::Accept, Self::Error>> {
        let this = self.get_mut();

        loop {
            if let Some(ref mut incoming) = this.incoming {
                match ready!(incoming.poll_recv(cx)) {
                    Some(Ok(connecting)) => {
                        // Return a future that resolves to the output.
                        return Poll::Ready(Ok(Box::pin(async move {
                            let connection = connecting.await.map_err(Error::from)?;

                            // Accept a bi-directional stream and return it. We'll think about multiplexing per topic later.
                            connection
                                .open_bi()
                                .await
                                .map(|(send, recv)| QuicStream { send, recv })
                                .map_err(Error::from)
                        })));
                    }
                    Some(Err(e)) => {
                        return Poll::Ready(Err(e));
                    }
                    None => {
                        unreachable!("Incoming channel closed")
                    }
                }
            } else {
                // Check if there's an endpoint bound.
                let Some(endpoint) = this.endpoint.clone() else {
                    return Poll::Ready(Err(Error::ClosedEndpoint));
                };

                let (tx, rx) = mpsc::channel(32);

                this.incoming = Some(rx);

                // Spawn a task to accept incoming connections.
                tokio::spawn(async move {
                    loop {
                        let connection_result =
                            endpoint.accept().await.ok_or(Error::ClosedEndpoint);

                        if tx.send(connection_result).await.is_err() {
                            error!("Failed to notify new incoming connection, channel closed. Shutting down task.");
                            break;
                        };
                    }
                });

                // Continue here to make sure we poll the incoming channel.
                continue;
            }
        }
    }
}
