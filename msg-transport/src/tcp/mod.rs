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

#[derive(Debug, Default)]
pub struct TcpOptions {
    pub blocking_connect: bool,
}

impl TcpOptions {
    pub fn with_blocking_connect(mut self) -> Self {
        self.blocking_connect = true;
        self
    }
}

pub struct TcpConnectOptions {
    pub set_nodelay: bool,
    pub auth: Option<Bytes>,
}

impl Default for TcpConnectOptions {
    fn default() -> Self {
        Self {
            set_nodelay: true,
            auth: None,
        }
    }
}

impl TcpConnectOptions {
    pub fn with_auth(mut self, auth: Bytes) -> Self {
        self.auth = Some(auth);
        self
    }
}

#[derive(Debug, Default)]
pub struct Tcp {
    listener: Option<tokio::net::TcpListener>,
    options: TcpOptions,
}

impl Tcp {
    pub fn new() -> Self {
        Self::new_with_options(TcpOptions::default())
    }

    pub fn new_with_options(options: TcpOptions) -> Self {
        Self {
            listener: None,
            options,
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

    async fn connect_with_auth(
        &self,
        addr: SocketAddr,
        auth: Option<Bytes>,
    ) -> Result<Self::Io, Self::Error> {
        let mut session = if let Some(ref id) = auth {
            let layer = AuthLayer { id: id.clone() };

            Self::Io::new(addr).with_layer(layer)
        } else {
            Self::Io::new(addr)
        };

        if self.options.blocking_connect {
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

    async fn bind(&mut self, addr: &str) -> Result<(), Self::Error> {
        let socket = TcpSocket::new_v4()?;
        socket.set_nodelay(true)?;
        socket.bind(addr.parse().unwrap())?;

        let listener = socket.listen(128)?;
        self.listener = Some(listener);
        Ok(())
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
