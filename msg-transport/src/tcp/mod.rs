use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::{
    net::SocketAddr,
    task::{Context, Poll},
};
use tokio::net::{TcpSocket, TcpStream};
use tokio_util::codec::Framed;

use msg_wire::auth;

use crate::AuthLayer;
use crate::{
    durable::{DurableSession, Layer, PendingIo},
    ClientTransport, ServerTransport,
};

/// Options for connecting over a TCP transport.
#[derive(Debug, Clone)]
pub struct TcpConnectOptions {
    /// Sets the TCP_NODELAY option on the socket.
    pub set_nodelay: bool,
    /// If true, the connection will be established synchronously.
    pub blocking_connect: bool,
    /// Optional authentication message.
    pub auth_token: Option<Bytes>,
}

impl Default for TcpConnectOptions {
    fn default() -> Self {
        Self {
            set_nodelay: true,
            blocking_connect: false,
            auth_token: None,
        }
    }
}

impl TcpConnectOptions {
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
}

#[derive(Debug, Default)]
pub struct Tcp {
    listener: Option<tokio::net::TcpListener>,
}

impl Tcp {
    pub fn new() -> Self {
        Self { listener: None }
    }
}

impl Layer<TcpStream> for AuthLayer {
    fn process(&mut self, io: TcpStream) -> PendingIo<TcpStream> {
        let id = self.id.clone();
        Box::pin(async move {
            let mut conn = Framed::new(io, auth::Codec::new_client());

            tracing::debug!("Sending auth message: {:?}", id);
            // Send the authentication message
            conn.send(auth::Message::Auth(id)).await?;
            conn.flush().await?;

            tracing::debug!("Waiting for ACK from server...");

            // Wait for the response
            let ack = conn
                .next()
                .await
                .ok_or(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Connection closed",
                ))?
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::PermissionDenied, e))?;

            conn.close().await?;

            if matches!(ack, auth::Message::Ack) {
                Ok(conn.into_inner())
            } else {
                Err(std::io::ErrorKind::PermissionDenied.into())
            }
        })
    }
}

#[async_trait::async_trait]
impl ClientTransport for Tcp {
    type Io = DurableSession<TcpStream>;
    type Error = std::io::Error;
    type ConnectOptions = TcpConnectOptions;

    async fn connect_with_options(
        addr: SocketAddr,
        options: TcpConnectOptions,
    ) -> Result<Self::Io, Self::Error> {
        let mut session = if let Some(ref id) = options.auth_token {
            let layer = AuthLayer { id: id.clone() };

            Self::Io::new(addr).with_layer(layer)
        } else {
            Self::Io::new(addr)
        };

        if options.blocking_connect {
            session.blocking_connect().await?;
        } else {
            session.connect().await;
        }

        Ok(session)
    }
}

#[async_trait::async_trait]
impl ServerTransport for Tcp {
    type Io = TcpStream;
    type Error = std::io::Error;
    type BindOptions = ();

    async fn bind_with_options(
        addr: SocketAddr,
        _options: Self::BindOptions,
    ) -> Result<Self, Self::Error> {
        let socket = TcpSocket::new_v4()?;
        socket.set_nodelay(true)?;
        socket.bind(addr)?;

        let listener = socket.listen(128)?;
        Ok(Self {
            listener: Some(listener),
        })
    }

    async fn accept(&self) -> Result<(Self::Io, SocketAddr), Self::Error> {
        self.listener.as_ref().unwrap().accept().await
    }

    fn local_addr(&self) -> Result<SocketAddr, Self::Error> {
        self.listener.as_ref().unwrap().local_addr()
    }

    fn poll_accept(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(Self::Io, SocketAddr), Self::Error>> {
        self.listener.as_ref().unwrap().poll_accept(cx)
    }
}

// #[async_trait::async_trait]
