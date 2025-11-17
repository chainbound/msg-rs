use std::{
    net::SocketAddr,
    os::fd::AsRawFd,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};

use crate::MeteredIo;

impl MeteredIo<TcpStream, TcpMetrics, SocketAddr> {
    #[inline]
    fn maybe_refresh(&mut self) {
        let now = Instant::now();
        if self.next_refresh <= now {
            match TcpMetrics::gather(&self.inner) {
                Ok(metrics) => *self.metrics.write().unwrap() = metrics,
                Err(e) => tracing::error!(err = ?e, "failed to gather TCP metrics"),
            }

            self.next_refresh = now + self.refresh_interval;
        }
    }
}

impl AsyncRead for MeteredIo<TcpStream, TcpMetrics, SocketAddr> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();

        this.maybe_refresh();

        Pin::new(&mut this.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for MeteredIo<TcpStream, TcpMetrics, SocketAddr> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();

        this.maybe_refresh();

        Pin::new(&mut this.inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();

        this.maybe_refresh();

        Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();

        this.maybe_refresh();

        Pin::new(&mut this.inner).poll_shutdown(cx)
    }
}

#[derive(Debug, Default)]
pub struct TcpMetrics {
    /// The congestion window in bytes.
    pub cwnd: u32,
    /// The peer's advertised receive window in bytes.
    pub peer_rwnd: u32,
    /// The most recent RTT sample.
    pub last_rtt: Duration,
    /// The smoothed round-trip time.
    pub smoothed_rtt: Duration,
    /// The round-trip time variance.
    pub rtt_var: Duration,
    /// Total bytes sent on the socket.
    pub tx_bytes: u64,
    /// Total bytes received on the socket.
    pub rx_bytes: u64,
    /// Total sender retransmitted bytes on the socket.
    pub retransmitted_bytes: u64,
    /// Total sender retransmitted packets on the socket.
    pub retransmitted_packets: u64,
    /// The current retransmission timeout.
    pub rto: Duration,
}

impl TcpMetrics {
    /// Gathers stats from the given TCP socket file descriptor, sourced from the OS with
    /// [`libc::getsockopt`].
    pub fn gather(stream: &TcpStream) -> Result<Self, std::io::Error> {
        let mut info = unsafe { std::mem::zeroed::<libc::tcp_connection_info>() };
        let mut len = std::mem::size_of::<libc::tcp_connection_info>() as libc::socklen_t;

        let rc = unsafe {
            libc::getsockopt(
                stream.as_raw_fd(),
                libc::IPPROTO_TCP,
                libc::TCP_CONNECTION_INFO,
                &mut info as *mut _ as *mut _,
                &mut len,
            )
        };

        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }

        Ok(info.into())
    }
}

#[cfg(target_os = "macos")]
impl From<libc::tcp_connection_info> for TcpMetrics {
    /// Converts a [`libc::tcp_connection_info`] into [`TcpMetrics`].
    fn from(info: libc::tcp_connection_info) -> Self {
        // Window sizes
        let cwnd = info.tcpi_snd_cwnd;
        let peer_rwnd = info.tcpi_rcv_wnd;

        // RTT
        let last_rtt = Duration::from_millis(info.tcpi_rttcur as u64);
        let smoothed_rtt = Duration::from_millis(info.tcpi_srtt as u64);
        let rtt_var = Duration::from_millis(info.tcpi_rttvar as u64);

        // Volumes
        let tx_bytes = info.tcpi_txbytes;
        let rx_bytes = info.tcpi_rxbytes;

        // Retransmissions
        let retransmitted_bytes = info.tcpi_txretransmitbytes;
        let retransmitted_packets = info.tcpi_rxretransmitpackets;
        let rto = Duration::from_millis(info.tcpi_rto as u64);

        Self {
            cwnd,
            peer_rwnd,
            last_rtt,
            smoothed_rtt,
            rtt_var,
            tx_bytes,
            rx_bytes,
            retransmitted_bytes,
            retransmitted_packets,
            rto,
        }
    }
}
