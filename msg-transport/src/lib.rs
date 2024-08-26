#![doc(issue_tracker_base_url = "https://github.com/chainbound/msg-rs/issues/")]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

use futures::{Future, FutureExt};
use msg_common::io_error;
use std::{
    fmt::Debug,
    hash::Hash,
    io,
    marker::PhantomData,
    net::{SocketAddr, ToSocketAddrs},
    path::PathBuf,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite};

pub mod ipc;
pub mod quic;
pub mod tcp;

/// A trait for address types that can be used by any transport.
pub trait Address: Clone + Debug + Send + Sync + Unpin + Hash + Eq + 'static {}

/// IP address types, used for TCP and QUIC transports.
impl Address for SocketAddr {}

/// File system path, used for IPC transport.
impl Address for PathBuf {}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AddressType {
    SocketAddr(SocketAddr),
    PathBuf(PathBuf),
}

impl From<SocketAddr> for AddressType {
    fn from(addr: SocketAddr) -> Self {
        AddressType::SocketAddr(addr)
    }
}

impl From<PathBuf> for AddressType {
    fn from(path: PathBuf) -> Self {
        AddressType::PathBuf(path)
    }
}

impl TryFrom<&str> for AddressType {
    type Error = io::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let s = value.to_string();
        if s.contains(':') {
            // try to parse as socket address
            let addr = s
                .parse::<SocketAddr>()
                .map_err(|_| io_error("invalid socket address"))?;

            Ok(AddressType::SocketAddr(addr))
        } else {
            // try to parse as path
            let path = PathBuf::from(s);
            Ok(AddressType::PathBuf(path))
        }
    }
}

impl ToSocketAddrs for AddressType {
    type Iter = std::vec::IntoIter<SocketAddr>;

    fn to_socket_addrs(&self) -> io::Result<Self::Iter> {
        match self {
            AddressType::SocketAddr(addr) => Ok(vec![*addr].into_iter()),
            AddressType::PathBuf(_) => Err(io_error("path is not a valid socket address")),
        }
    }
}

impl From<AddressType> for PathBuf {
    fn from(val: AddressType) -> Self {
        match val {
            AddressType::SocketAddr(_) => panic!("socket address is not a valid path"),
            AddressType::PathBuf(path) => path,
        }
    }
}

/// A transport provides connection-oriented communication between two peers through
/// ordered and reliable streams of bytes.
///
/// It provides an interface to manage both inbound and outbound connections.
#[async_trait::async_trait]
pub trait Transport<A: Address> {
    // /// The generic address type used by this transport
    // type Addr: Address;

    /// The result of a successful connection.
    ///
    /// The output type is transport-specific, and can be a handle to directly write to the
    /// connection, or it can be a substream multiplexer in the case of stream protocols.
    type Io: AsyncRead + AsyncWrite + PeerAddress<A> + Send + Unpin;

    /// An error that occurred when setting up the connection.
    type Error: std::error::Error + From<io::Error> + Send + Sync;

    /// A pending [`Transport::Output`] for an outbound connection,
    /// obtained when calling [`Transport::connect`].
    type Connect: Future<Output = Result<Self::Io, Self::Error>> + Send;

    /// A pending [`Transport::Output`] for an inbound connection,
    /// obtained when calling [`Transport::poll_accept`].
    type Accept: Future<Output = Result<Self::Io, Self::Error>> + Send + Unpin;

    /// Returns the local address this transport is bound to (if it is bound).
    fn local_addr(&self) -> Option<A>;

    /// Binds to the given address.
    async fn bind(&mut self, addr: A) -> Result<(), Self::Error>;

    /// Connects to the given address, returning a future representing a pending outbound connection.
    fn connect(&mut self, addr: A) -> Self::Connect;

    /// Poll for incoming connections. If an inbound connection is received, a future representing
    /// a pending inbound connection is returned. The future will resolve to [`Transport::Output`].
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

pub struct Acceptor<'a, T, A> {
    inner: &'a mut T,
    _marker: PhantomData<A>,
}

impl<'a, T, A> Acceptor<'a, T, A> {
    fn new(inner: &'a mut T) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }
}

impl<'a, T, A> Future for Acceptor<'a, T, A>
where
    T: Transport<A> + Unpin,
    A: Address,
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
pub trait PeerAddress<A: Address> {
    fn peer_addr(&self) -> Result<A, io::Error>;
}
