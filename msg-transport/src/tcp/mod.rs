use futures::future::BoxFuture;
use std::{
    io,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};
use tracing::debug;

use msg_common::async_error;

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
    /// The bound listener. Only used when turmoil is disabled; under turmoil the
    /// listener is owned by the background accept task (see [`Tcp::bind`]).
    #[cfg(not(feature = "turmoil"))]
    listener: Option<TcpListener>,
    /// For turmoil: the bound address, captured before the listener is moved into the
    /// accept task.
    #[cfg(feature = "turmoil")]
    turmoil_local_addr: Option<SocketAddr>,
    /// For turmoil: channel receiver for accepted connections.
    #[cfg(feature = "turmoil")]
    accept_rx: Option<tokio::sync::mpsc::Receiver<io::Result<(TcpStream, SocketAddr)>>>,
}

impl std::fmt::Debug for Tcp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut dbg = f.debug_struct("Tcp");
        dbg.field("config", &self.config);

        #[cfg(not(feature = "turmoil"))]
        dbg.field("listener", &self.listener.as_ref().map(|_| "<TcpListener>"));

        #[cfg(feature = "turmoil")]
        dbg.field("local_addr", &self.turmoil_local_addr);

        dbg.finish()
    }
}

impl Tcp {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            #[cfg(not(feature = "turmoil"))]
            listener: None,
            #[cfg(feature = "turmoil")]
            turmoil_local_addr: None,
            #[cfg(feature = "turmoil")]
            accept_rx: None,
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
        #[cfg(feature = "turmoil")]
        {
            self.turmoil_local_addr
        }

        #[cfg(not(feature = "turmoil"))]
        {
            self.listener.as_ref().and_then(|l| l.local_addr().ok())
        }
    }

    async fn bind(&mut self, addr: SocketAddr) -> Result<(), Self::Error> {
        let listener = TcpListener::bind(addr).await?;

        #[cfg(feature = "turmoil")]
        {
            // Turmoil's TcpListener cannot be polled and is not Clone, so we move it
            // into a background task that forwards accepted connections over a channel.
            // Capture the bound address up front because the listener is no longer
            // reachable from `self` afterwards.
            self.turmoil_local_addr = Some(listener.local_addr()?);

            let (tx, rx) = tokio::sync::mpsc::channel(32);
            self.accept_rx = Some(rx);

            tokio::spawn(async move {
                loop {
                    let result = listener.accept().await;
                    if tx.send(result).await.is_err() {
                        // Channel closed, shut down the accept task
                        break;
                    }
                }
            });
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
            let Some(ref mut rx) = this.accept_rx else {
                return Poll::Ready(async_error(io::ErrorKind::NotConnected.into()));
            };

            // `poll_recv` registers the current waker with the channel, so the task is
            // notified exactly when a connection arrives or the sender is dropped.
            match rx.poll_recv(cx) {
                Poll::Ready(Some(Ok((io, addr)))) => {
                    debug!(%addr, "accepted connection");
                    Poll::Ready(Box::pin(async move { Ok(io) }))
                }
                Poll::Ready(Some(Err(e))) => Poll::Ready(async_error(e)),
                Poll::Ready(None) => {
                    Poll::Ready(async_error(io::ErrorKind::BrokenPipe.into()))
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
