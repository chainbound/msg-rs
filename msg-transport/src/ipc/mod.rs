use std::{
    io,
    path::PathBuf,
    pin::Pin,
    task::{Context, Poll},
};

use async_trait::async_trait;
use futures::future::BoxFuture;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{UnixListener, UnixStream},
};
use tracing::debug;

use crate::{Acceptor, PeerAddress, Transport, TransportExt};

use msg_common::async_error;

#[derive(Debug, Default)]
pub struct Config;

/// An IPC (Inter-Process Communication) implementation using Unix domain sockets.
///
/// This struct represents the IPC transport, which allows communication between processes
/// on the same machine using Unix domain sockets.
///
/// # Features
/// - Asynchronous communication using Tokio's runtime
/// - Supports both connection-oriented (stream) and connectionless (datagram) sockets
/// - Implements standard transport traits for easy integration with other components
///
/// Note: This implementation is specific to Unix-like operating systems and is not tested
/// on Windows or other non-Unix platforms.
#[derive(Debug, Default)]
pub struct Ipc {
    #[allow(unused)]
    config: Config,
    listener: Option<UnixListener>,
    path: Option<PathBuf>,
}

impl Ipc {
    pub fn new(config: Config) -> Self {
        Self { config, listener: None, path: None }
    }
}

pub struct IpcStream {
    peer: PathBuf,
    stream: UnixStream,
}

impl IpcStream {
    pub async fn connect(peer: PathBuf) -> io::Result<Self> {
        let stream = UnixStream::connect(&peer).await?;
        Ok(Self { peer, stream })
    }
}

impl AsyncRead for IpcStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for IpcStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}

impl PeerAddress<PathBuf> for IpcStream {
    fn peer_addr(&self) -> Result<PathBuf, io::Error> {
        Ok(self.peer.clone())
    }
}

impl TryFrom<&IpcStream> for () {
    type Error = std::io::Error;

    fn try_from(stream: &IpcStream) -> Result<Self, Self::Error> {
        Ok(())
    }
}

#[async_trait]
impl Transport<PathBuf> for Ipc {
    // TODO: Implement stats for IPC
    type Stats = ();
    type Io = IpcStream;

    type Error = io::Error;

    type Connect = BoxFuture<'static, Result<Self::Io, Self::Error>>;
    type Accept = BoxFuture<'static, Result<Self::Io, Self::Error>>;

    fn local_addr(&self) -> Option<PathBuf> {
        self.path.clone()
    }

    async fn bind(&mut self, addr: PathBuf) -> Result<(), Self::Error> {
        if addr.exists() {
            debug!("Socket file already exists. Attempting to remove.");
            if let Err(e) = std::fs::remove_file(&addr) {
                return Err(io::Error::other(format!(
                    "Failed to remove existing socket file, {e:?}"
                )));
            }
        }

        let listener = UnixListener::bind(&addr)?;
        self.listener = Some(listener);
        self.path = Some(addr);
        Ok(())
    }

    fn connect(&mut self, addr: PathBuf) -> Self::Connect {
        Box::pin(async move { IpcStream::connect(addr).await })
    }

    fn poll_accept(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Accept> {
        let this = self.get_mut();

        let Some(ref listener) = this.listener else {
            return Poll::Ready(async_error(io::ErrorKind::NotConnected.into()));
        };

        match listener.poll_accept(cx) {
            Poll::Ready(Ok((io, _addr))) => {
                debug!("accepted IPC connection");
                let stream = IpcStream {
                    // We expect the path to be the same socket as the listener
                    peer: this.path.clone().expect("listener not bound"),
                    stream: io,
                };
                Poll::Ready(Box::pin(async move { Ok(stream) }))
            }
            Poll::Ready(Err(e)) => Poll::Ready(async_error(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[async_trait]
impl TransportExt<PathBuf> for Ipc {
    fn accept(&mut self) -> Acceptor<'_, Self, PathBuf>
    where
        Self: Sized + Unpin,
    {
        Acceptor::new(self)
    }
}
