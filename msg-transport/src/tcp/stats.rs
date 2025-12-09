use std::{os::fd::AsRawFd, time::Duration};

use tokio::net::TcpStream;

#[derive(Debug, Clone, Default)]
pub struct TcpStats {
    /// The congestion window in bytes.
    pub congestion_window: u32,
    /// Our receive window in bytes.
    pub receive_window: u32,
    /// Our send window (= the peer's advertised receive window) in bytes.
    #[cfg(target_os = "macos")]
    pub send_window: u32,
    /// The most recent RTT sample.
    #[cfg(target_os = "macos")]
    pub last_rtt: Duration,
    /// The smoothed round-trip time.
    pub smoothed_rtt: Duration,
    /// The round-trip time variance.
    pub rtt_variance: Duration,
    /// Total bytes sent on the socket.
    #[cfg(target_os = "macos")]
    pub tx_bytes: u64,
    /// Total bytes received on the socket.
    #[cfg(target_os = "macos")]
    pub rx_bytes: u64,
    /// Total sender retransmitted bytes on the socket.
    pub retransmitted_bytes: u64,
    /// Total sender retransmitted packets on the socket.
    pub retransmitted_packets: u64,
    /// The current retransmission timeout.
    pub retransmission_timeout: Duration,
}

#[cfg(target_os = "macos")]
impl TryFrom<&TcpStream> for TcpStats {
    type Error = std::io::Error;

    /// Gathers stats from the given TCP socket file descriptor, sourced from the OS with
    /// [`libc::getsockopt`].
    fn try_from(stream: &TcpStream) -> Result<Self, Self::Error> {
        let info = getsockopt::<libc::tcp_connection_info>(stream, libc::TCP_CONNECTION_INFO)?;

        Ok(info.into())
    }
}

#[cfg(target_os = "macos")]
impl From<libc::tcp_connection_info> for TcpStats {
    /// Converts a [`libc::tcp_connection_info`] into [`TcpStats`].
    fn from(info: libc::tcp_connection_info) -> Self {
        // Window sizes
        let congestion_window = info.tcpi_snd_cwnd;
        let receive_window = info.tcpi_rcv_wnd;
        let send_window = info.tcpi_snd_wnd;

        // RTT
        let last_rtt = Duration::from_millis(info.tcpi_rttcur as u64);
        let smoothed_rtt = Duration::from_millis(info.tcpi_srtt as u64);
        let rtt_variance = Duration::from_millis(info.tcpi_rttvar as u64);

        // Volumes
        let tx_bytes = info.tcpi_txbytes;
        let rx_bytes = info.tcpi_rxbytes;

        // Retransmissions
        let retransmitted_bytes = info.tcpi_txretransmitbytes;
        let retransmitted_packets = info.tcpi_rxretransmitpackets;
        let retransmission_timeout = Duration::from_millis(info.tcpi_rto as u64);

        Self {
            congestion_window,
            receive_window,
            send_window,
            last_rtt,
            smoothed_rtt,
            rtt_variance,
            tx_bytes,
            rx_bytes,
            retransmitted_bytes,
            retransmitted_packets,
            retransmission_timeout,
        }
    }
}

#[cfg(target_os = "linux")]
impl TryFrom<&TcpStream> for TcpStats {
    type Error = std::io::Error;

    /// Gathers stats from the given TCP socket file descriptor, sourced from the OS with
    /// [`libc::getsockopt`].
    fn try_from(stream: &TcpStream) -> Result<Self, Self::Error> {
        let info = getsockopt::<libc::tcp_info>(stream, libc::TCP_INFO)?;

        Ok(info.into())
    }
}

#[cfg(target_os = "linux")]
impl From<libc::tcp_info> for TcpStats {
    /// Converts a [`libc::tcp_info`] into [`TcpStats`].
    fn from(info: libc::tcp_info) -> Self {
        // On Linux, tcpi_snd_cwnd is in segments; convert to bytes using snd_mss.
        let congestion_window = info.tcpi_snd_cwnd.saturating_mul(info.tcpi_snd_mss);
        // Local advertised receive window (bytes).
        let receive_window = info.tcpi_rcv_space;

        // RTT fields are reported in microseconds.
        let smoothed_rtt = Duration::from_micros(info.tcpi_rtt as u64);
        let rtt_variance = Duration::from_micros(info.tcpi_rttvar as u64);

        // Retransmissions
        let retransmitted_packets = info.tcpi_total_retrans as u64;
        let retransmitted_bytes = retransmitted_packets.saturating_mul(info.tcpi_snd_mss as u64);
        // RTO is in microseconds.
        let retransmission_timeout = Duration::from_micros(info.tcpi_rto as u64);

        Self {
            congestion_window,
            receive_window,
            smoothed_rtt,
            rtt_variance,
            retransmitted_bytes,
            retransmitted_packets,
            retransmission_timeout,
        }
    }
}

/// Helper function to get a socket option from a TCP stream.
fn getsockopt<T>(stream: &TcpStream, option: libc::c_int) -> std::io::Result<T> {
    let mut info = unsafe { std::mem::zeroed::<T>() };
    let mut len = std::mem::size_of::<T>() as libc::socklen_t;
    let dst = &mut info as *mut _ as *mut _;

    let result =
        unsafe { libc::getsockopt(stream.as_raw_fd(), libc::IPPROTO_TCP, option, dst, &mut len) };

    if result != 0 {
        return Err(std::io::Error::last_os_error());
    }

    Ok(info)
}
