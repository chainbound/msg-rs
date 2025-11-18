use std::{net::SocketAddr, pin::Pin, time::Instant};

use tokio::io::{AsyncRead, AsyncWrite};

use crate::{MeteredIo, PeerAddress};

/// A bi-directional QUIC stream that implements [`AsyncRead`] + [`AsyncWrite`].
pub struct QuicStream {
    pub(super) peer: SocketAddr,
    pub(super) send: quinn::SendStream,
    pub(super) recv: quinn::RecvStream,
    pub(super) conn: quinn::Connection,
}

impl AsyncRead for QuicStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        AsyncRead::poll_read(Pin::new(&mut self.get_mut().recv), cx, buf)
    }
}

impl AsyncWrite for QuicStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        AsyncWrite::poll_write(Pin::new(&mut self.get_mut().send), cx, buf)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        AsyncWrite::poll_flush(Pin::new(&mut self.get_mut().send), cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        AsyncWrite::poll_shutdown(Pin::new(&mut self.get_mut().send), cx)
    }
}

impl PeerAddress<SocketAddr> for QuicStream {
    fn peer_addr(&self) -> Result<SocketAddr, std::io::Error> {
        Ok(self.peer)
    }
}
