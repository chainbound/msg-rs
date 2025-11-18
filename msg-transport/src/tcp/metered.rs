use std::{os::fd::AsRawFd, time::Duration};

use tokio::net::TcpStream;

#[derive(Debug, Default)]
pub struct TcpStats {
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

impl TryFrom<&TcpStream> for TcpStats {
    type Error = std::io::Error;

    /// Gathers stats from the given TCP socket file descriptor, sourced from the OS with
    /// [`libc::getsockopt`].
    fn try_from(stream: &TcpStream) -> Result<Self, Self::Error> {
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
impl From<libc::tcp_connection_info> for TcpStats {
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
