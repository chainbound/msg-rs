use core::fmt;
use derive_more::{Deref, DerefMut, From};
use futures::future::BoxFuture;
use openssl::ssl::{Ssl, SslAcceptor, SslConnector, SslMethod};
use std::{
    io,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::net::{TcpListener, TcpStream};
use tokio_openssl::SslStream;
use tracing::debug;

use msg_common::async_error;

use crate::{Acceptor, PeerAddress, Transport, TransportExt};

pub mod config;

/// An invalid operation, due to using as a server or viceversa.
#[derive(Debug, Clone, Copy, thiserror::Error)]
pub enum InvalidOperation {
    #[error("tried to bind as a client")]
    BindAsClient,
    #[error("tried to connect as a server")]
    ConnectAsServer,
    #[error("tried to accept a connection as a client")]
    AcceptAsClient,
}

/// Error that can occur using the TCP transport with TLS.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),
    #[error("i/o error kind: {0}")]
    IoKind(io::ErrorKind),
    /// OpenSSL library error.
    #[error("openssl error: {0}")]
    OpenSsl(#[from] openssl::error::ErrorStack),
    /// SSL connection error.
    #[error("ssl error: {0}")]
    Ssl(#[from] openssl::ssl::Error),
    #[error("invalid configuration: {0}")]
    InvalidOperation(#[from] InvalidOperation),
}

/// A TCP-TLS client.
#[derive(Debug, Clone, From, Deref, DerefMut)]
pub struct Client(config::Client);

/// A TCP-TLS server.
pub struct Server {
    /// The underlying TCP listener.
    listener: Option<TcpListener>,
    /// The OpenSSL acceptor for TLS handshake requests.
    acceptor: Arc<SslAcceptor>,
}

impl fmt::Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server")
            .field("listener", &self.listener)
            .field("acceptor", &"SslAcceptor")
            .finish()
    }
}

/// A TCP transport implementation with TLS support using OpenSSL.
#[derive(Debug, From)]
pub enum TcpTls {
    Client(Client),
    Server(Server),
}

impl TcpTls {
    /// Create a new instance of the transport with client configuration.
    pub fn new_client(config: config::Client) -> Self {
        TcpTls::Client(config.into())
    }

    /// Create a new instance of the transport with server configuration.
    pub fn new_server(config: config::Server) -> Self {
        TcpTls::Server(Server { listener: None, acceptor: Arc::new(config.ssl_acceptor) })
    }

    pub fn as_client(&self) -> Option<&Client> {
        match self {
            Self::Client(c) => Some(c),
            _ => None,
        }
    }

    pub fn as_client_mut(&mut self) -> Option<&mut Client> {
        match self {
            Self::Client(c) => Some(c),
            _ => None,
        }
    }

    pub fn as_server(&self) -> Option<&Server> {
        match self {
            Self::Server(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_server_mut(&mut self) -> Option<&mut Server> {
        match self {
            Self::Server(s) => Some(s),
            _ => None,
        }
    }
}

/// A wrapper around a Tokio TCP stream with TLS support using OpenSSL.
#[derive(Debug, From, Deref, DerefMut)]
pub struct TcpTlsStream(SslStream<TcpStream>);

impl tokio::io::AsyncRead for TcpTlsStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(self.get_mut().get_mut()).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for TcpTlsStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(self.get_mut().get_mut()).poll_write(cx, buf)
    }

    fn poll_flush(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.get_mut().get_mut()).poll_flush(cx)
    }

    fn poll_shutdown(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut()).poll_shutdown(cx)
    }
}

impl PeerAddress<SocketAddr> for TcpTlsStream {
    fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.get_ref().peer_addr()
    }
}

#[async_trait::async_trait]
impl Transport<SocketAddr> for TcpTls {
    type Io = TcpTlsStream;

    type Error = Error;

    type Connect = BoxFuture<'static, Result<Self::Io, Self::Error>>;
    type Accept = BoxFuture<'static, Result<Self::Io, Self::Error>>;

    fn local_addr(&self) -> Option<SocketAddr> {
        self.as_server()?.listener.as_ref()?.local_addr().ok()
    }

    async fn bind(&mut self, addr: SocketAddr) -> Result<(), Self::Error> {
        let TcpTls::Server(server) = self else {
            return Err(InvalidOperation::BindAsClient.into());
        };

        let listener = TcpListener::bind(addr).await?;
        server.listener = Some(listener);

        Ok(())
    }

    fn connect(&mut self, addr: SocketAddr) -> Self::Connect {
        let config = match self {
            TcpTls::Client(client) => client.0.clone(),
            _ => {
                return async_error(Error::InvalidOperation(InvalidOperation::ConnectAsServer));
            }
        };

        Box::pin(async move {
            // 1. Try to configure the TLS connector
            let connector = if let Some(connector) = config.ssl_connector {
                connector
            } else {
                SslConnector::builder(SslMethod::tls())?.build()
            };
            let tls_session_state = connector.configure()?.into_ssl(&config.domain)?;

            // 2. Establish the TCP connection
            let stream = TcpStream::connect(addr).await?;
            stream.set_nodelay(true)?;

            // 3. Perform the TLS handshake
            let mut stream = SslStream::new(tls_session_state, stream)?;
            let s = Pin::new(&mut stream);
            s.connect().await?;

            Ok(stream.into())
        })
    }

    fn poll_accept(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Accept> {
        let TcpTls::Server(server) = self.get_mut() else {
            return Poll::Ready(async_error(InvalidOperation::AcceptAsClient.into()));
        };

        let Some(ref listener) = server.listener else {
            return Poll::Ready(async_error(Error::IoKind(io::ErrorKind::NotConnected)));
        };
        let tls_acceptor = server.acceptor.clone();

        match listener.poll_accept(cx) {
            Poll::Ready(Ok((io, addr))) => {
                debug!("accepted connection from {}", addr);

                Poll::Ready(Box::pin(async move {
                    io.set_nodelay(true)?;

                    let tls_session_state = Ssl::new(tls_acceptor.context())?;
                    let mut stream = SslStream::new(tls_session_state, io)?;
                    Pin::new(&mut stream).accept().await?;

                    Ok(stream.into())
                }))
            }
            Poll::Ready(Err(e)) => Poll::Ready(async_error(e.into())),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[async_trait::async_trait]
impl TransportExt<SocketAddr> for TcpTls {
    fn accept(&mut self) -> Acceptor<'_, Self, SocketAddr>
    where
        Self: Sized + Unpin,
    {
        Acceptor::new(self)
    }
}

#[cfg(test)]
mod tests {
    use tokio::net::lookup_host;

    use crate::{
        Transport,
        tcp_tls::{TcpTls, config},
    };

    #[tokio::test]
    async fn connecting_with_tls_works() {
        let server_name = "www.rust-lang.org";
        let server_name_with_port = format!("{}:443", server_name);

        let config = config::Client::new(server_name.to_string());
        let mut tcp_tls = TcpTls::new_client(config);

        let socket_address = lookup_host(server_name_with_port)
            .await
            .expect("lookup failed")
            .next()
            .expect("no address found");

        tcp_tls.connect(socket_address).await.expect("to connect successfully");
    }
}
