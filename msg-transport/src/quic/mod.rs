use std::{
    io,
    net::{SocketAddr, UdpSocket},
    pin::Pin,
    sync::Arc,
    task::{Poll, ready},
};

use futures::future::BoxFuture;
use thiserror::Error;
use tokio::sync::mpsc::{self, Receiver};
use tracing::{debug, error};

use crate::{Acceptor, Transport, TransportExt};

use msg_common::{SocketAddrExt, async_error};

mod config;
mod stream;
mod tls;

pub use config::{Config, ConfigBuilder};
use stream::QuicStream;

pub(crate) const ALPN_PROTOCOL: &[u8] = b"msg";

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

/// A QUIC implementation built with [quinn] that implements the [`Transport`] and [`TransportExt`]
/// traits.
///
/// # Note on multiplexing
/// This implementation does not yet support multiplexing. This means that each connection only
/// supports a single bi-directional stream, which is returned as the I/O object when connecting or
/// accepting.
///
/// In a future release, we will add support for multiplexing, which will allow multiple streams per
/// connection based on socket requirements / semantics.
#[derive(Debug, Default)]
pub struct Quic {
    config: Config,
    endpoint: Option<quinn::Endpoint>,

    /// A receiver for incoming connections waiting to be handled.
    incoming: Option<Receiver<Result<quinn::Incoming, Error>>>,
}

impl Quic {
    /// Creates a new QUIC transport with the given configuration.
    pub fn new(config: Config) -> Self {
        Self { config, endpoint: None, incoming: None }
    }

    /// Creates a new [`quinn::Endpoint`] with the given configuration and a Tokio runtime. If no
    /// `addr` is given, the endpoint will be bound to the default address.
    fn new_endpoint(
        &self,
        addr: SocketAddr,
        server_config: Option<quinn::ServerConfig>,
    ) -> Result<quinn::Endpoint, Error> {
        let socket = UdpSocket::bind(addr)?;

        let endpoint = quinn::Endpoint::new(
            self.config.endpoint_config.clone(),
            server_config,
            socket,
            Arc::new(quinn::TokioRuntime),
        )?;

        Ok(endpoint)
    }
}

impl TryFrom<&QuicStream> for quinn::ConnectionStats {
    type Error = std::io::Error;

    fn try_from(stream: &QuicStream) -> Result<Self, Self::Error> {
        Ok(stream.conn.stats())
    }
}

#[async_trait::async_trait]
impl Transport<SocketAddr> for Quic {
    type Stats = quinn::ConnectionStats;
    type Io = QuicStream;

    type Control = ();

    type Error = Error;

    type Connect = BoxFuture<'static, Result<Self::Io, Self::Error>>;
    type Accept = BoxFuture<'static, Result<Self::Io, Self::Error>>;

    /// Returns the local address this transport is bound to (if it is bound).
    fn local_addr(&self) -> Option<SocketAddr> {
        self.endpoint.as_ref().and_then(|e| e.local_addr().ok())
    }

    /// Binds a QUIC endpoint to the given address.
    async fn bind(&mut self, addr: SocketAddr) -> Result<(), Self::Error> {
        let endpoint = quinn::Endpoint::server(self.config.server_config.clone(), addr)?;

        self.endpoint = Some(endpoint);

        Ok(())
    }

    /// Connects to the given address, returning a future representing a pending outbound
    /// connection. If the endpoint is not bound, it will be bound to the default address.
    fn connect(&mut self, addr: SocketAddr) -> Self::Connect {
        // If we have an endpoint, use it. Otherwise, create a new one.
        let endpoint = if let Some(endpoint) = self.endpoint.clone() {
            endpoint
        } else {
            let Ok(mut endpoint) = self.new_endpoint(addr.as_unspecified(), None) else {
                return async_error(Error::ClosedEndpoint);
            };

            endpoint.set_default_client_config(self.config.client_config.clone());

            self.endpoint = Some(endpoint.clone());

            endpoint
        };

        Box::pin(async move {
            debug!(target = %addr, "Initiating connection");

            // This `"l"` seems necessary because an empty string is an invalid domain
            // name. While we don't use domain names, the underlying rustls library
            // is based upon the assumption that we do.
            let connection = endpoint.connect(addr, "l")?.await?;

            debug!(target = %addr, "Connected, opening stream...");

            // Open a bi-directional stream and return it. We'll think about multiplexing per topic
            // later.
            connection
                .open_bi()
                .await
                .map(|(send, recv)| QuicStream { peer: addr, send, recv, conn: connection })
                .map_err(Error::from)
        })
    }

    /// Poll for pending incoming connections.
    fn poll_accept(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Accept> {
        let this = self.get_mut();

        loop {
            if let Some(ref mut incoming) = this.incoming {
                // Incoming channel and task are spawned, so we can poll it.
                match ready!(incoming.poll_recv(cx)) {
                    Some(Ok(incoming)) => {
                        let peer = incoming.remote_address();

                        debug!("New incoming connection from {}", peer);

                        // Return a future that resolves to the output.
                        return Poll::Ready(Box::pin(async move {
                            debug!(client = %peer, "Accepting connection...");
                            let connection = incoming.accept()?.await?;
                            debug!(
                                "Accepted connection from {}, opening stream",
                                connection.remote_address()
                            );

                            // Accept a bi-directional stream and return it. We'll think about
                            // multiplexing per topic later.
                            connection
                                .accept_bi()
                                .await
                                .map(|(send, recv)| QuicStream {
                                    peer,
                                    send,
                                    recv,
                                    conn: connection,
                                })
                                .map_err(Error::from)
                        }));
                    }
                    Some(Err(e)) => {
                        return Poll::Ready(async_error(e));
                    }
                    None => {
                        unreachable!("Incoming channel closed")
                    }
                }
            } else {
                // We need to set the incoming channel and spawn a task to accept incoming
                // connections on the endpoint.

                // Check if there's an endpoint bound.
                let Some(endpoint) = this.endpoint.clone() else {
                    return Poll::Ready(async_error(Error::ClosedEndpoint));
                };

                let (tx, rx) = mpsc::channel(32);

                this.incoming = Some(rx);

                // Spawn a task to accept incoming connections.
                tokio::spawn(async move {
                    loop {
                        let connection_result =
                            endpoint.accept().await.ok_or(Error::ClosedEndpoint);

                        if tx.send(connection_result).await.is_err() {
                            error!(
                                "Failed to notify new incoming connection, channel closed. Shutting down task."
                            );
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

impl TransportExt<SocketAddr> for Quic {
    fn accept(&mut self) -> crate::Acceptor<'_, Self, SocketAddr>
    where
        Self: Sized + Unpin,
    {
        Acceptor::new(self)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::TransportExt;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        sync::oneshot,
    };
    use tracing::info;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_quic_connection_simple() {
        let _ = tracing_subscriber::fmt::try_init();

        let config = Config::default();

        let mut server = Quic::new(config.clone());
        server.bind(SocketAddr::from(([127, 0, 0, 1], 0))).await.unwrap();

        let server_addr = server.local_addr().unwrap();
        info!("Server bound on {:?}", server_addr);

        let (tx, rx) = oneshot::channel();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;

            let mut stream = server.accept().await.unwrap();

            info!("Accepted connection");

            let mut dst = [0u8; 5];

            stream.read_exact(&mut dst).await.unwrap();

            stream.shutdown().await.unwrap();
            tx.send(dst).unwrap();
        });

        let mut client = Quic::new(config);

        let mut stream = client.connect(server_addr).await.unwrap();
        info!("Connected to remote");

        let item = b"Hello";
        stream.write_all(item).await.unwrap();
        stream.flush().await.unwrap();

        info!("Wrote to remote");
        let rcv = rx.await.unwrap();

        assert_eq!(rcv, *item);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_quic_connection_late_bind() {
        let _ = tracing_subscriber::fmt::try_init();

        let config = Config::default();

        let addr = SocketAddr::from(([127, 0, 0, 1], 9971));

        let mut server = Quic::new(config.clone());

        let mut client = Quic::new(config);

        tokio::spawn(async move {
            let _stream = client.connect(addr).await.unwrap();
            info!("Connected to remote");
        });

        tokio::time::sleep(Duration::from_secs(17)).await;
        server.bind(addr).await.unwrap();

        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}
