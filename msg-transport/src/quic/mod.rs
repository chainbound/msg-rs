use std::{io, net::SocketAddr, pin::Pin, sync::Arc};

use bytes::Bytes;
use futures::FutureExt;
use quinn::{self, ClientConfig, Endpoint};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{ClientTransport, ServerTransport};

use self::tls::unsafe_tls_config;

mod config;
mod tls;

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

/// The state of the accept loop. We need this to poll through the various
/// futures involved in accepting a connection in `poll_accept`.
enum AcceptState<'a> {
    /// Waiting for new incoming connections
    Waiting,
    /// A new incoming connection is being accepted
    Accepting(quinn::Accept<'a>),
    /// A new incoming connection is being established
    Connecting(quinn::Connecting),
}

/// A QUIC transport that implements both [`ClientTransport`] and [`ServerTransport`].
pub struct Quic<'a> {
    endpoint: quinn::Endpoint,
    accept_state: AcceptState<'a>,
}

/// A bi-directional QUIC stream that implements [`AsyncRead`] + [`AsyncWrite`].
pub struct QuicStream {
    send: quinn::SendStream,
    recv: quinn::RecvStream,
}

impl AsyncRead for QuicStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        AsyncRead::poll_read(Pin::new(&mut self.get_mut().recv), cx, buf)
    }
}

impl AsyncWrite for QuicStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        AsyncWrite::poll_write(Pin::new(&mut self.get_mut().send), cx, buf)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        AsyncWrite::poll_flush(Pin::new(&mut self.get_mut().send), cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        AsyncWrite::poll_shutdown(Pin::new(&mut self.get_mut().send), cx)
    }
}

/// Client-side QUIC errors.
#[derive(Debug, Error)]
pub enum ClientError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Connect(#[from] quinn::ConnectError),
    #[error(transparent)]
    Connection(#[from] quinn::ConnectionError),
}

#[async_trait::async_trait]
impl ClientTransport for Quic<'_> {
    type Io = QuicStream;
    type Error = ClientError;
    type ConnectOptions = QuicConnectOptions;

    async fn connect_with_options(
        addr: SocketAddr,
        options: Self::ConnectOptions,
    ) -> Result<Self::Io, Self::Error> {
        let mut endpoint = Endpoint::client(options.local_addr)?;
        endpoint.set_default_client_config(options.client_config);

        let conn = endpoint.connect(addr, "")?.await?;

        // Open a bi-directional stream and return it. We'll think about multiplexing per topic later.
        Ok(conn
            .open_bi()
            .await
            .map(|(send, recv)| QuicStream { send, recv })?)
    }
}

/// Server-side QUIC errors.
#[derive(Debug, Error)]
pub enum ServerError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Connection(#[from] quinn::ConnectionError),
    #[error("Endpoint closed")]
    ClosedEndpoint,
}

#[derive(Default)]
pub struct BindOptions {
    pub server_config: quinn::ServerConfig,
}

impl std::marker::Unpin for Quic<'_> {}

#[async_trait::async_trait]
impl ServerTransport for Quic<'static> {
    type Io = QuicStream;
    type Error = ServerError;
    type BindOptions = BindOptions;

    async fn bind_with_options(
        addr: SocketAddr,
        options: &Self::BindOptions,
    ) -> Result<Self, Self::Error> {
        let mut endpoint = Endpoint::server(options.server_config, addr)?;

        Ok(Self {
            endpoint,
            accept_state: AcceptState::Waiting,
        })
    }

    fn local_addr(&self) -> Result<SocketAddr, Self::Error> {
        Ok(self.endpoint.local_addr()?)
    }

    async fn accept(&self) -> Result<(Self::Io, SocketAddr), Self::Error> {
        let conn = self
            .endpoint
            .accept()
            .await
            .ok_or(ServerError::ClosedEndpoint)?;

        let connection = conn.await?;

        // Accept a bi-directional stream and return it. We'll think about multiplexing per topic later.
        Ok(connection
            .accept_bi()
            .await
            .map(|(send, recv)| (QuicStream { send, recv }, connection.remote_address()))?)
    }

    fn poll_accept(
        &self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(Self::Io, SocketAddr), Self::Error>> {
        loop {
            match self.accept_state {
                AcceptState::Waiting => {
                    let accept = self.endpoint.accept();
                    self.accept_state = AcceptState::Accepting(accept);
                    continue;
                }
                AcceptState::Accepting(accept) => {
                    accept.poll_unpin(cx);

                    self.accept_state = AcceptState::Connecting(connecting);
                    continue;
                }
                AcceptState::Connecting(_) => todo!(),
            }
        }
    }
}
