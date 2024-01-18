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
pub mod tcp;

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
    type Io: AsyncRead + AsyncWrite + PeerAddress + Send + Unpin;

    /// An error that occurred when setting up the connection.
    type Error: std::error::Error + Send + Sync;

    /// A pending [`Transport::Output`] for an outbound connection,
    /// obtained when calling [`Transport::connect`].
    type Connect: Future<Output = Result<Self::Io, Self::Error>> + Send;

    /// A pending [`Transport::Output`] for an inbound connection,
    /// obtained when calling [`Transport::poll_accept`].
    type Accept: Future<Output = Result<Self::Io, Self::Error>> + Send + Unpin;

    /// Returns the local address this transport is bound to (if it is bound).
    fn local_addr(&self) -> Option<SocketAddr>;

    /// Binds to the given address.
    async fn bind(&mut self, addr: SocketAddr) -> Result<(), Self::Error>;

    /// Connects to the given address, returning a future representing a pending outbound connection.
    fn connect(&mut self, addr: SocketAddr) -> Self::Connect;

    /// Poll for incoming connections. If an inbound connection is received, a future representing
    /// a pending inbound connection is returned. The future will resolve to [`Transport::Output`].
    fn poll_accept(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Accept>;
}

pub trait TransportExt: Transport {
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
    type Output = Result<T::Io, T::Error>;

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

/// Trait for connection types that can return their peer address.
pub trait PeerAddress {
    fn peer_addr(&self) -> Result<SocketAddr, std::io::Error>;
}

pub struct AuthLayer {
    id: Bytes,
}
