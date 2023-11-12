use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use msg_wire::auth;
use std::{
    net::SocketAddr,
    task::{Context, Poll},
};
use tokio_util::codec::Framed;

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

#[derive(Default)]
pub struct Tcp {
    #[cfg(feature = "turmoil")]
    listener: Option<SimulatedTcpListener>,
    #[cfg(not(feature = "turmoil"))]
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

pub struct AuthLayer {
    id: Bytes,
}

#[cfg(not(feature = "turmoil"))]
impl Layer<tokio::net::TcpStream> for AuthLayer {
    fn process(&mut self, io: tokio::net::TcpStream) -> PendingIo<tokio::net::TcpStream> {
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

#[cfg(feature = "turmoil")]
impl Layer<turmoil::net::TcpStream> for AuthLayer {
    fn process(&mut self, io: turmoil::net::TcpStream) -> PendingIo<turmoil::net::TcpStream> {
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

#[cfg(not(feature = "turmoil"))]
#[async_trait::async_trait]
impl ClientTransport for Tcp {
    type Io = DurableSession<tokio::net::TcpStream>;
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

#[cfg(not(feature = "turmoil"))]
#[async_trait::async_trait]
impl ServerTransport for Tcp {
    type Io = tokio::net::TcpStream;
    type Error = std::io::Error;

    async fn bind(&mut self, addr: &str) -> Result<(), Self::Error> {
        let socket = tokio::net::TcpSocket::new_v4()?;
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

/// Wrapper for a Turmoil TcpListener.
///
/// This internal type is only required to port the turmoil API to MSG.
#[cfg(feature = "turmoil")]
pub struct SimulatedTcpListener(turmoil::net::TcpListener);

#[cfg(feature = "turmoil")]
impl SimulatedTcpListener {
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.0.local_addr()
    }

    pub async fn accept(&self) -> std::io::Result<(turmoil::net::TcpStream, SocketAddr)> {
        self.0.accept().await
    }

    pub fn poll_accept(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<(turmoil::net::TcpStream, SocketAddr)>> {
        // TODO: implement poll_accept for turmoil tcp listener.
        // this method doesn't actually poll, it's just a placeholder.
        println!("poll_accept called");
        futures::FutureExt::poll_unpin(&mut Box::pin(async move { self.accept().await }), cx)
    }
}

#[cfg(feature = "turmoil")]
#[async_trait::async_trait]
impl ClientTransport for Tcp {
    type Io = DurableSession<turmoil::net::TcpStream>;
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

#[cfg(feature = "turmoil")]
#[async_trait::async_trait]
impl ServerTransport for Tcp {
    type Io = turmoil::net::TcpStream;
    type Error = std::io::Error;

    async fn bind(&mut self, addr: &str) -> Result<(), Self::Error> {
        println!("enter");
        let listener = turmoil::net::TcpListener::bind(addr).await?;
        println!("got");
        self.listener = Some(SimulatedTcpListener(listener));
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
