use futures::future::BoxFuture;
use std::{
    io,
    net::SocketAddr,
    task::{Context, Poll},
};
use tokio::net::{TcpListener, TcpStream};

use msg_common::async_error;

use crate::{Acceptor, PeerAddress, Transport, TransportExt};

#[derive(Debug, Default)]
pub struct Config;

#[derive(Debug, Default)]
pub struct Tcp {
    #[allow(unused)]
    config: Config,
    listener: Option<tokio::net::TcpListener>,
}

impl Tcp {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            listener: None,
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
    type Io = TcpStream;

    type Error = io::Error;

    type Connect = BoxFuture<'static, Result<Self::Io, Self::Error>>;
    type Accept = BoxFuture<'static, Result<Self::Io, Self::Error>>;

    fn local_addr(&self) -> Option<SocketAddr> {
        self.listener.as_ref().and_then(|l| l.local_addr().ok())
    }

    async fn bind(&mut self, addr: SocketAddr) -> Result<(), Self::Error> {
        let listener = TcpListener::bind(addr).await?;

        self.listener = Some(listener);

        Ok(())
    }

    fn connect(&mut self, addr: SocketAddr) -> Self::Connect {
        Box::pin(async move {
            let stream = TcpStream::connect(addr).await?;
            stream.set_nodelay(true)?;

            Ok(stream)
        })
    }

    fn poll_accept(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Accept> {
        let this = self.get_mut();

        let Some(ref listener) = this.listener else {
            return Poll::Ready(async_error(io::ErrorKind::NotConnected.into()));
        };

        match listener.poll_accept(cx) {
            Poll::Ready(Ok((io, addr))) => {
                tracing::debug!("Accepted connection from {}", addr);

                Poll::Ready(Box::pin(async move {
                    io.set_nodelay(true)?;
                    Ok(io)
                }))
            }
            Poll::Ready(Err(e)) => Poll::Ready(async_error(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[async_trait::async_trait]
impl TransportExt<SocketAddr> for Tcp {
    fn accept(&mut self) -> Acceptor<'_, Self, SocketAddr>
    where
        Self: Sized + Unpin,
    {
        Acceptor::new(self)
    }
}
