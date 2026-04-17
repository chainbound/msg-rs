use arc_swap::ArcSwap;
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
use tokio_openssl::SslStream;
use tracing::debug;

use msg_common::async_error;

#[cfg(feature = "turmoil")]
use crate::SyncBoxFuture;
use crate::{
    Acceptor, PeerAddress, Transport, TransportExt,
    net::{TcpListener, TcpStream},
    tcp::TcpStats,
};

pub mod config;

/// An invalid operation, due to using a client as a server or viceversa.
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
#[derive(Debug, Clone, Deref, DerefMut)]
pub struct Client(config::Client);

impl Client {
    pub fn new(config: config::Client) -> Self {
        Self(config)
    }
}

/// A TCP-TLS server.
pub struct Server {
    /// The underlying TCP listener.
    ///
    /// Under turmoil the listener is wrapped in an [`Arc`] so that the in-progress
    /// accept future stored in [`Server::accept_fut`] can hold a strong reference
    /// for its `'static` lifetime. Both references are owned by this struct, so
    /// dropping the server drops the listener synchronously, which under turmoil
    /// unbinds the port from the simulated host immediately.
    #[cfg(not(feature = "turmoil"))]
    listener: Option<TcpListener>,
    #[cfg(feature = "turmoil")]
    listener: Option<Arc<TcpListener>>,
    /// For turmoil: the in-progress raw-TCP accept future, if any.
    ///
    /// `turmoil::net::TcpListener` only exposes `async fn accept(&self)`, so we
    /// drive it by holding a pinned future here and polling it from `poll_accept`.
    /// This mirrors the tokio `poll_accept` back-pressure behavior exactly; the
    /// TLS handshake itself runs in the transport accept future returned afterwards.
    #[cfg(feature = "turmoil")]
    accept_fut: Option<SyncBoxFuture<'static, io::Result<(TcpStream, SocketAddr)>>>,
    /// The OpenSSL acceptor for TLS handshake requests.
    acceptor: ArcSwap<SslAcceptor>,
}

impl Server {
    pub fn new(acceptor: Arc<SslAcceptor>) -> Self {
        Self {
            listener: None,
            #[cfg(feature = "turmoil")]
            accept_fut: None,
            acceptor: ArcSwap::new(acceptor),
        }
    }

    pub fn swap_acceptor(&mut self, acceptor: Arc<SslAcceptor>) {
        self.acceptor.swap(acceptor);
    }
}

impl fmt::Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server")
            .field("listener", &self.listener.as_ref().map(|_| "<TcpListener>"))
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
        TcpTls::Client(Client::new(config))
    }

    /// Create a new instance of the transport with server configuration.
    pub fn new_server(config: config::Server) -> Self {
        TcpTls::Server(Server::new(config.ssl_acceptor))
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

impl TcpTlsStream {
    /// Get a mutable reference to the underlying SSL stream.
    pub fn inner(&mut self) -> &mut SslStream<TcpStream> {
        &mut self.0
    }
}

impl tokio::io::AsyncRead for TcpTlsStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let stream = self.get_mut().inner();

        Pin::new(stream).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for TcpTlsStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let stream = self.get_mut().inner();

        Pin::new(stream).poll_write(cx, buf)
    }

    fn poll_flush(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let stream = self.get_mut().inner();

        Pin::new(stream).poll_flush(cx)
    }

    fn poll_shutdown(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let stream = self.get_mut().inner();

        Pin::new(stream).poll_shutdown(cx)
    }
}

impl PeerAddress<SocketAddr> for TcpTlsStream {
    fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.get_ref().peer_addr()
    }
}

// Under turmoil there are no OS-level TCP counters to gather, so defer to the
// infallible `From<&TcpStream>` impl that returns default stats. Keeping the
// impls mutually exclusive avoids overlapping the blanket `TryFrom` that
// `From` yields with a second concrete `TryFrom` impl.
#[cfg(feature = "turmoil")]
impl From<&TcpTlsStream> for TcpStats {
    fn from(stream: &TcpTlsStream) -> Self {
        TcpStats::from(stream.get_ref())
    }
}

#[cfg(not(feature = "turmoil"))]
impl TryFrom<&TcpTlsStream> for TcpStats {
    type Error = std::io::Error;

    fn try_from(stream: &TcpTlsStream) -> Result<Self, Self::Error> {
        TcpStats::try_from(stream.get_ref())
    }
}

/// [`Transport::Control`] plane messages for this transport.
#[derive(Clone)]
pub enum Control {
    /// Allow to swap the currently used TLS acceptor with the provided one,
    /// keeping existing connections. One reason for using this could be to extend the current list
    /// of root certificates.
    SwapAcceptor(Arc<SslAcceptor>),
}

#[async_trait::async_trait]
impl Transport<SocketAddr> for TcpTls {
    type Stats = TcpStats;
    type Io = TcpTlsStream;

    type Error = Error;
    type Control = Control;

    type Connect = BoxFuture<'static, Result<Self::Io, Self::Error>>;
    type Accept = BoxFuture<'static, Result<Self::Io, Self::Error>>;

    fn local_addr(&self) -> Option<SocketAddr> {
        self.as_server()?.listener.as_ref()?.local_addr().ok()
    }

    async fn bind(&mut self, addr: SocketAddr) -> Result<(), Self::Error> {
        let TcpTls::Server(server) = self else {
            return Err(InvalidOperation::BindAsClient.into());
        };

        // Bind first, then commit, so that a failed bind leaves any previously
        // bound listener intact.
        let listener = TcpListener::bind(addr).await?;

        #[cfg(feature = "turmoil")]
        {
            // Drop the in-progress accept future before replacing the listener so
            // the old listener's last `Arc` is released in order, triggering its
            // synchronous `Drop` which unbinds the port from turmoil's host.
            server.accept_fut = None;
            server.listener = Some(Arc::new(listener));
        }
        #[cfg(not(feature = "turmoil"))]
        {
            server.listener = Some(listener);
        }

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

            // 2. Establish the TCP connection. `set_nodelay` is skipped under turmoil since its
            //    `TcpStream` stub returns Ok without effect, and tokio's real TCP call is
            //    unnecessary in the simulator (Nagle doesn't apply).
            let stream = TcpStream::connect(addr).await?;
            #[cfg(not(feature = "turmoil"))]
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

        let tls_acceptor = server.acceptor.load_full();

        // Shared TLS-handshake tail: wrap a raw accepted `TcpStream` in an
        // `SslStream` and run `accept()`. Returning an `async move` keeps the
        // `Self::Accept` future identical between the tokio and turmoil paths.
        let handshake = move |io: TcpStream| -> Self::Accept {
            Box::pin(async move {
                #[cfg(not(feature = "turmoil"))]
                io.set_nodelay(true)?;

                let tls_session_state = Ssl::new(tls_acceptor.context())?;
                let mut stream = SslStream::new(tls_session_state, io)?;
                Pin::new(&mut stream).accept().await?;

                Ok(stream.into())
            })
        };

        #[cfg(not(feature = "turmoil"))]
        {
            let Some(ref listener) = server.listener else {
                return Poll::Ready(async_error(Error::IoKind(io::ErrorKind::NotConnected)));
            };

            match listener.poll_accept(cx) {
                Poll::Ready(Ok((io, addr))) => {
                    debug!(%addr, "accepted connection");
                    Poll::Ready(handshake(io))
                }
                Poll::Ready(Err(e)) => Poll::Ready(async_error(e.into())),
                Poll::Pending => Poll::Pending,
            }
        }

        #[cfg(feature = "turmoil")]
        {
            let Some(listener) = server.listener.as_ref().cloned() else {
                return Poll::Ready(async_error(Error::IoKind(io::ErrorKind::NotConnected)));
            };

            // Lazily build a `'static` accept future that owns its own strong
            // reference to the listener, and keep it alive across pending polls.
            let fut = server
                .accept_fut
                .get_or_insert_with(|| Box::pin(async move { listener.accept().await }));

            match fut.as_mut().poll(cx) {
                Poll::Ready(Ok((io, addr))) => {
                    server.accept_fut = None;
                    debug!(%addr, "accepted connection");
                    Poll::Ready(handshake(io))
                }
                Poll::Ready(Err(e)) => {
                    server.accept_fut = None;
                    Poll::Ready(async_error(e.into()))
                }
                Poll::Pending => Poll::Pending,
            }
        }
    }

    fn on_control(&mut self, control: Self::Control) {
        match control {
            Self::Control::SwapAcceptor(acceptor) => {
                let Some(server) = self.as_server_mut() else {
                    tracing::warn!("called swap acceptor on a client instance, this is a no-op");
                    return;
                };
                server.swap_acceptor(acceptor);
            }
        }
    }
}

impl TransportExt<SocketAddr> for TcpTls {
    fn accept(&mut self) -> Acceptor<'_, Self, SocketAddr>
    where
        Self: Sized + Unpin,
    {
        Acceptor::new(self)
    }
}

// Reaches out to the public internet, which is not routable under turmoil's
// simulated topology. Only build this test in the real-network configuration.
#[cfg(all(test, not(feature = "turmoil")))]
mod tests {
    use tokio::net::lookup_host;

    use crate::{
        Transport,
        tcp_tls::{TcpTls, config},
    };

    #[tokio::test]
    async fn connecting_with_tls_works() {
        let server_name = "www.rust-lang.org";
        let server_name_with_port = format!("{server_name}:443");

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
