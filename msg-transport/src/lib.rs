use bytes::Bytes;
use std::{
    net::SocketAddr,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite};

pub mod durable;
mod tcp;
pub use tcp::*;

#[async_trait::async_trait]
pub trait ClientTransport {
    type Io: AsyncRead + AsyncWrite + Unpin + Send + 'static;
    type Error: std::error::Error + Send + Sync + 'static;
    type ConnectOptions: Default + Clone + Send + Sync + 'static;

    async fn connect_with_options(
        addr: SocketAddr,
        options: Self::ConnectOptions,
    ) -> Result<Self::Io, Self::Error>;
}

#[async_trait::async_trait]
pub trait ServerTransport: Unpin + Send + Sync + 'static {
    type Io: AsyncRead + AsyncWrite + Unpin + Send + 'static;
    type Error: std::error::Error + Send + Sync + 'static;
    type BindOptions: Default + Send + Sync + 'static;

    async fn bind_with_options(
        addr: SocketAddr,
        options: &Self::BindOptions,
    ) -> Result<Self, Self::Error>
    where
        Self: Sized;

    fn local_addr(&self) -> Result<SocketAddr, Self::Error>;

    async fn accept(&self) -> Result<(Self::Io, SocketAddr), Self::Error>;

    #[allow(clippy::type_complexity)]
    fn poll_accept(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(Self::Io, SocketAddr), Self::Error>>;
}

pub struct AuthLayer {
    id: Bytes,
}
