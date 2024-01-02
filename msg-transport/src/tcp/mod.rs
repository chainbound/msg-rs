use bytes::Bytes;
use futures::{future::BoxFuture, SinkExt, StreamExt};
use msg_common::async_error;
use std::{
    io,
    net::SocketAddr,
    task::{Context, Poll},
};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

use msg_wire::auth;

use crate::{
    durable::{DurableSession, Layer, PendingIo},
    Acceptor, TransportExt,
};
use crate::{AuthLayer, Transport};

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
pub struct Config {
    /// If true, the connection will be established synchronously.
    pub blocking_connect: bool,
    /// Optional authentication message.
    pub auth_token: Option<Bytes>,
}

impl Config {
    /// Sets the auth token for this connection.
    pub fn auth_token(mut self, auth: Bytes) -> Self {
        self.auth_token = Some(auth);
        self
    }

    /// Connect synchronously.
    pub fn blocking_connect(mut self, b: bool) -> Self {
        self.blocking_connect = b;
        self
    }
}

#[derive(Debug, Default)]
pub struct Tcp {
    config: Config,
    listener: Option<tokio::net::TcpListener>,
}

impl Tcp {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            listener: None,
        }
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
impl Transport for Tcp {
    type Io = DurableSession<TcpStream>;

    type Error = io::Error;

    type Connect = BoxFuture<'static, Result<Self::Io, Self::Error>>;
    type Accept = BoxFuture<'static, Result<Self::Io, Self::Error>>;

    async fn bind(&mut self, addr: SocketAddr) -> Result<(), Self::Error> {
        let listener = TcpListener::bind(addr).await?;

        self.listener = Some(listener);

        Ok(())
    }

    fn connect(&mut self, addr: SocketAddr) -> Self::Connect {
        let mut session = if let Some(ref id) = self.config.auth_token {
            let layer = AuthLayer { id: id.clone() };

            Self::Io::new(addr).with_layer(layer)
        } else {
            Self::Io::new(addr)
        };

        let blocking_connect = self.config.blocking_connect;

        Box::pin(async move {
            if blocking_connect {
                session.blocking_connect().await?;
            } else {
                session.connect().await;
            }

            Ok(session)
        })
    }

    fn poll_accept(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Accept> {
        let this = self.get_mut();

        let Some(ref listener) = this.listener else {
            return Poll::Ready(async_error(io::ErrorKind::NotConnected.into()));
        };

        match listener.poll_accept(cx) {
            Poll::Ready(Ok((io, addr))) => {
                tracing::debug!("Accepted connection from {}", addr);

                Poll::Ready(Box::pin(async move { Ok(DurableSession::from(io)) }))
            }
            Poll::Ready(Err(e)) => Poll::Ready(async_error(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[async_trait::async_trait]
impl TransportExt for Tcp {
    fn local_addr(&self) -> Option<SocketAddr> {
        self.listener.as_ref().and_then(|l| l.local_addr().ok())
    }

    fn accept(&mut self) -> Acceptor<'_, Self>
    where
        Self: Sized + Unpin,
    {
        Acceptor::new(self)
    }
}
