use bytes::Bytes;
use futures::{Future, FutureExt};
use std::{
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite};

pub mod durable;
pub mod quic;
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
        options: Self::BindOptions,
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

/// A transport provides connection-oriented communication between two peers through
/// ordered and reliable streams of bytes.
///
/// It provides an interface to manage both inbound and outbound connections.
#[async_trait::async_trait]
pub trait Transport {
    /// The result of a successful connection.
    ///
    /// The output type is transport-specific, and can be a handle to directly write to the
    /// connection, or it can be a substream multiplexer in the case of stream protocols.
    type Output: AsyncRead + AsyncWrite;

    /// An error that occurred when setting up the connection.
    type Error: std::error::Error;

    /// A pending [`Transport::Output`] for an outbound connection,
    /// obtained when calling [`Transport::connect`].
    type Connect: Future<Output = Result<Self::Output, Self::Error>>;

    /// A pending [`Transport::Output`] for an inbound connection,
    /// obtained when calling [`Transport::poll_accept`].
    type Accept: Future<Output = Result<Self::Output, Self::Error>> + Unpin;

    /// Binds to the given address.
    async fn bind(&mut self, addr: SocketAddr) -> Result<(), Self::Error>;

    /// Connects to the given address, returning a future representing a pending outbound connection.
    fn connect(&mut self, addr: SocketAddr) -> Self::Connect;

    /// Poll for incoming connections. If an inbound connection is received, a future representing
    /// a pending inbound connection is returned. The future will resolve to [`Transport::Output`].
    fn poll_accept(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Accept>;
}

pub trait TransportExt: Transport {
    /// Returns the local address this transport is bound to (if it is bound).
    fn local_addr(&self) -> Option<SocketAddr>;

    /// Async-friendly interface for accepting inbound connections.
    fn accept(&mut self) -> Acceptor<'_, Self>
    where
        Self: Sized + Unpin,
    {
        Acceptor::new(self)
    }
}

pub struct Acceptor<'a, T> {
    inner: &'a mut T,
}

impl<'a, T> Acceptor<'a, T> {
    fn new(inner: &'a mut T) -> Self {
        Self { inner }
    }
}

impl<'a, T> Future for Acceptor<'a, T>
where
    T: Transport + Unpin,
{
    type Output = Result<T::Output, T::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut *self.get_mut().inner).poll_accept(cx) {
            Poll::Ready(mut accept) => match accept.poll_unpin(cx) {
                Poll::Ready(Ok(output)) => Poll::Ready(Ok(output)),
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                Poll::Pending => Poll::Pending,
            },
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct AuthLayer {
    id: Bytes,
}
