use std::{
    net::SocketAddr,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpSocket, TcpStream},
};

mod durable;

#[async_trait::async_trait]
pub trait ClientTransport {
    type Io: AsyncRead + AsyncWrite + Unpin + Send + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    async fn connect(&self, addr: &str) -> Result<Self::Io, Self::Error>;
}

#[async_trait::async_trait]
pub trait ServerTransport: Unpin + Send + Sync + 'static {
    type Io: AsyncRead + AsyncWrite + Unpin + Send + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    fn local_addr(&self) -> Result<SocketAddr, Self::Error>;

    async fn bind(&mut self, addr: &str) -> Result<(), Self::Error>;
    async fn accept(&self) -> Result<(Self::Io, SocketAddr), Self::Error>;

    #[allow(clippy::type_complexity)]
    fn poll_accept(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(Self::Io, SocketAddr), Self::Error>>;
}

#[derive(Debug, Default)]
pub struct TcpOptions {
    pub set_nodelay: bool,
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

#[async_trait::async_trait]
impl ClientTransport for Tcp {
    type Io = TcpStream;
    type Error = std::io::Error;

    async fn connect(&self, addr: &str) -> Result<Self::Io, Self::Error> {
        let stream = Self::Io::connect(addr).await?;
        stream.set_nodelay(self.options.set_nodelay)?;
        Ok(stream)
    }
}

#[async_trait::async_trait]
impl ServerTransport for Tcp {
    type Io = TcpStream;
    type Error = std::io::Error;

    async fn bind(&mut self, addr: &str) -> Result<(), Self::Error> {
        let socket = TcpSocket::new_v4()?;
        socket.set_nodelay(self.options.set_nodelay)?;
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
