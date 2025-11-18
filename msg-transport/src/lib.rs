#![doc(issue_tracker_base_url = "https://github.com/chainbound/msg-rs/issues/")]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

use std::{
    fmt::Debug,
    hash::Hash,
    io,
    marker::PhantomData,
    net::SocketAddr,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, RwLock},
    task::{Context, Poll},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use futures::{Future, FutureExt};
use tokio::io::{AsyncRead, AsyncWrite};

pub mod ipc;
#[cfg(feature = "quic")]
pub mod quic;
pub mod tcp;

/// A trait for address types that can be used by any transport.
pub trait Address: Clone + Debug + Send + Sync + Unpin + Hash + Eq + 'static {}

/// IP address types, used for TCP and QUIC transports.
impl Address for SocketAddr {}

/// File system path, used for IPC transport.
impl Address for PathBuf {}

/// A wrapper around an `Io` object that records and provides transport-specific metrics.
///
/// # Reasoning
/// One reason of using a wrapper around the IO object here is that it metrics should be on a
/// per-connection (or stream) basis. This is a clean way to achieve this without polluting the
/// [`Transport`] trait or any higher-level users of the transport.
pub struct MeteredIo<Io, M, A>
where
    Io: AsyncRead + AsyncWrite + PeerAddress<A>,
    A: Address,
{
    /// The inner IO object.
    inner: Io,
    /// The shareable metrics for the inner IO object.
    metrics: Arc<RwLock<M>>,
    /// The next time the metrics should be refreshed.
    next_refresh: Instant,
    /// The interval at which the metrics should be refreshed.
    refresh_interval: Duration,

    _marker: PhantomData<A>,
}

impl<Io, M, A> std::ops::Deref for MeteredIo<Io, M, A>
where
    Io: AsyncRead + AsyncWrite + PeerAddress<A>,
    A: Address,
{
    type Target = Io;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<Io, M, A> std::ops::DerefMut for MeteredIo<Io, M, A>
where
    Io: AsyncRead + AsyncWrite + PeerAddress<A>,
    A: Address,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<Io, M, A> PeerAddress<A> for MeteredIo<Io, M, A>
where
    Io: AsyncRead + AsyncWrite + PeerAddress<A>,
    A: Address,
{
    fn peer_addr(&self) -> Result<A, io::Error> {
        self.inner.peer_addr()
    }
}

impl<Io, M, A> MeteredIo<Io, M, A>
where
    Io: AsyncRead + AsyncWrite + PeerAddress<A>,
    A: Address,
    M: Default,
{
    /// Creates a new `MeteredIo` wrapper around the given `Io` object, and initializes default
    /// metrics.
    pub fn new(inner: Io) -> Self {
        Self {
            inner,
            metrics: Default::default(),
            _marker: PhantomData,
            next_refresh: Instant::now(),
            refresh_interval: Duration::from_secs(2),
        }
    }

    /// Returns a shared reference to the metrics.
    pub fn metrics(&self) -> Arc<RwLock<M>> {
        Arc::clone(&self.metrics)
    }
}

/// A transport provides connection-oriented communication between two peers through
/// ordered and reliable streams of bytes.
///
/// It provides an interface to manage both inbound and outbound connections.
#[async_trait]
pub trait Transport<A: Address> {
    /// The result of a successful connection.
    ///
    /// The output type is transport-specific, and can be a handle to directly write to the
    /// connection, or it can be a substream multiplexer in the case of stream protocols.
    type Io: AsyncRead + AsyncWrite + PeerAddress<A> + Send + Unpin;

    /// An error that occurred when setting up the connection.
    type Error: std::error::Error + From<io::Error> + Send + Sync;

    /// A pending output for an outbound connection, obtained when calling [`Transport::connect`].
    type Connect: Future<Output = Result<Self::Io, Self::Error>> + Send;

    /// A pending output for an inbound connection, obtained when calling
    /// [`Transport::poll_accept`].
    type Accept: Future<Output = Result<Self::Io, Self::Error>> + Send + Unpin;

    /// Returns the local address this transport is bound to (if it is bound).
    fn local_addr(&self) -> Option<A>;

    /// Binds to the given address.
    async fn bind(&mut self, addr: A) -> Result<(), Self::Error>;

    /// Connects to the given address, returning a future representing a
    /// pending outbound connection.
    fn connect(&mut self, addr: A) -> Self::Connect;

    /// Poll for incoming connections. If an inbound connection is received, a future representing
    /// a pending inbound connection is returned. The future will resolve to [`Transport::Accept`].
    fn poll_accept(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Accept>;
}

pub trait TransportExt<A: Address>: Transport<A> {
    /// Async-friendly interface for accepting inbound connections.
    fn accept(&mut self) -> Acceptor<'_, Self, A>
    where
        Self: Sized + Unpin,
    {
        Acceptor::new(self)
    }
}

/// An `await`-friendly interface for accepting inbound connections.
///
/// This struct is used to accept inbound connections from a transport. It is
/// created using the [`TransportExt::accept`] method.
pub struct Acceptor<'a, T, A>
where
    T: Transport<A>,
    A: Address,
{
    inner: &'a mut T,
    /// The pending [`Transport::Accept`] future.
    pending: Option<T::Accept>,
    _marker: PhantomData<A>,
}

impl<'a, T, A> Acceptor<'a, T, A>
where
    T: Transport<A>,
    A: Address,
{
    fn new(inner: &'a mut T) -> Self {
        Self { inner, pending: None, _marker: PhantomData }
    }
}

impl<T, A> Future for Acceptor<'_, T, A>
where
    T: Transport<A> + Unpin,
    A: Address,
{
    type Output = Result<T::Io, T::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            // If there's a pending accept future, poll it to completion
            if let Some(pending) = this.pending.as_mut() {
                match pending.poll_unpin(cx) {
                    Poll::Ready(res) => {
                        this.pending = None;
                        return Poll::Ready(res);
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            // Otherwise, poll the transport for a new accept future
            match Pin::new(&mut *this.inner).poll_accept(cx) {
                Poll::Ready(accept) => {
                    this.pending = Some(accept);
                    continue;
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// Trait for connection types that can return their peer address.
pub trait PeerAddress<A: Address> {
    fn peer_addr(&self) -> Result<A, io::Error>;
}
