use futures::future::BoxFuture;
#[cfg(feature = "turmoil")]
use std::sync::Arc;
use std::{
    io,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};
use tracing::debug;

use msg_common::async_error;

#[cfg(feature = "turmoil")]
use crate::SyncBoxFuture;
use crate::net::{TcpListener, TcpStream};
use crate::{Acceptor, PeerAddress, Transport, TransportExt};

mod stats;
pub use stats::TcpStats;

#[derive(Debug, Default)]
pub struct Config;

/// TCP transport implementation.
///
/// When the `turmoil` feature is enabled, this transport uses turmoil's simulated
/// networking types instead of real TCP sockets. This allows for deterministic testing
/// of distributed systems.
#[derive(Default)]
pub struct Tcp {
    #[allow(unused)]
    config: Config,
    /// The bound listener.
    ///
    /// Under turmoil the listener is wrapped in an [`Arc`] so that the in-progress
    /// accept future created in [`Tcp::poll_accept`] can hold a strong reference for
    /// its `'static` lifetime. Both references are owned by this struct, so dropping
    /// `Tcp` drops the listener synchronously, which in turmoil unbinds the port
    /// from the simulated host immediately.
    #[cfg(not(feature = "turmoil"))]
    listener: Option<TcpListener>,
    #[cfg(feature = "turmoil")]
    listener: Option<Arc<TcpListener>>,
    /// For turmoil: the in-progress accept future, if any.
    ///
    /// `turmoil::net::TcpListener` only exposes an `async fn accept(&self)`, so we
    /// drive it by holding a pinned future here and polling it from `poll_accept`.
    /// Back-pressure is therefore provided by turmoil's listener queue directly,
    /// the same way `tokio::net::TcpListener::poll_accept` works in the default
    /// build. There's no intermediate mpsc buffer that can fill up and stall the
    /// listener.
    #[cfg(feature = "turmoil")]
    accept_fut: Option<SyncBoxFuture<'static, io::Result<(TcpStream, SocketAddr)>>>,
}

impl std::fmt::Debug for Tcp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tcp")
            .field("config", &self.config)
            .field("listener", &self.listener.as_ref().map(|_| "<TcpListener>"))
            .finish()
    }
}

impl Tcp {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            listener: None,
            #[cfg(feature = "turmoil")]
            accept_fut: None,
        }
    }
}

impl PeerAddress<SocketAddr> for TcpStream {
    fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.peer_addr()
    }
}

#[async_trait::async_trait]
impl Transport<SocketAddr> for Tcp {
    type Stats = TcpStats;
    type Io = TcpStream;

    type Control = ();

    type Error = io::Error;

    type Connect = BoxFuture<'static, Result<Self::Io, Self::Error>>;
    type Accept = BoxFuture<'static, Result<Self::Io, Self::Error>>;

    fn local_addr(&self) -> Option<SocketAddr> {
        self.listener.as_ref().and_then(|l| l.local_addr().ok())
    }

    async fn bind(&mut self, addr: SocketAddr) -> Result<(), Self::Error> {
        // Bind first, then commit. A failed bind must leave the transport in its
        // previous state, matching the non-turmoil path where the old listener is
        // never disturbed unless a replacement is ready to take over.
        let listener = TcpListener::bind(addr).await?;

        #[cfg(feature = "turmoil")]
        {
            // Drop the in-progress accept future before replacing the listener so
            // that the old listener's last `Arc` is released in order, triggering
            // its synchronous `Drop` which unbinds the port from turmoil's host.
            // Because we always bind to a fresh address (the simulated host cannot
            // hold two listeners on the same port), the new listener is already
            // installed on its own port and will not conflict with the release.
            self.accept_fut = None;
            self.listener = Some(Arc::new(listener));
        }

        #[cfg(not(feature = "turmoil"))]
        {
            self.listener = Some(listener);
        }

        Ok(())
    }

    fn connect(&mut self, addr: SocketAddr) -> Self::Connect {
        Box::pin(async move {
            let stream = TcpStream::connect(addr).await?;
            #[cfg(not(feature = "turmoil"))]
            stream.set_nodelay(true)?;

            Ok(stream)
        })
    }

    fn poll_accept(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Accept> {
        let this = self.get_mut();

        #[cfg(not(feature = "turmoil"))]
        {
            let Some(ref listener) = this.listener else {
                return Poll::Ready(async_error(io::ErrorKind::NotConnected.into()));
            };

            match listener.poll_accept(cx) {
                Poll::Ready(Ok((io, addr))) => {
                    debug!(%addr, "accepted connection");

                    Poll::Ready(Box::pin(async move {
                        io.set_nodelay(true)?;
                        Ok(io)
                    }))
                }
                Poll::Ready(Err(e)) => Poll::Ready(async_error(e)),
                Poll::Pending => Poll::Pending,
            }
        }

        #[cfg(feature = "turmoil")]
        {
            let Some(listener) = this.listener.as_ref().cloned() else {
                return Poll::Ready(async_error(io::ErrorKind::NotConnected.into()));
            };

            // Lazily build a `'static` accept future that owns its own strong
            // reference to the listener, and keep it alive across pending polls.
            let fut = this
                .accept_fut
                .get_or_insert_with(|| Box::pin(async move { listener.accept().await }));

            match fut.as_mut().poll(cx) {
                Poll::Ready(Ok((io, addr))) => {
                    this.accept_fut = None;
                    debug!(%addr, "accepted connection");
                    Poll::Ready(Box::pin(async move { Ok(io) }))
                }
                Poll::Ready(Err(e)) => {
                    this.accept_fut = None;
                    Poll::Ready(async_error(e))
                }
                Poll::Pending => Poll::Pending,
            }
        }
    }
}

impl TransportExt<SocketAddr> for Tcp {
    fn accept(&mut self) -> Acceptor<'_, Self, SocketAddr>
    where
        Self: Sized + Unpin,
    {
        Acceptor::new(self)
    }
}
