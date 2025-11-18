use std::{os::fd::AsRawFd, time::Duration};

use tokio::net::TcpStream;

#[derive(Debug, Default)]
pub struct TcpStats {
    /// The congestion window in bytes.
    pub cwnd: u32,
    /// Our receive window in bytes.
    pub rwnd: u32,
    /// Our send window (= the peer's advertised receive window) in bytes.
    pub snd_wnd: u32,
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

#[cfg(target_os = "macos")]
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
    /// Converts a [`libc::tcp_connection_info`] into [`TcpStats`].
    fn from(info: libc::tcp_connection_info) -> Self {
        // Window sizes
        let cwnd = info.tcpi_snd_cwnd;
        let rwnd = info.tcpi_rcv_wnd;
        let snd_wnd = info.tcpi_snd_wnd;

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
            rwnd,
            snd_wnd,
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

#[cfg(target_os = "linux")]
impl TryFrom<&TcpStream> for TcpStats {
    type Error = std::io::Error;

    /// Gathers stats from the given TCP socket file descriptor, sourced from the OS with
    /// [`libc::getsockopt`].
    fn try_from(stream: &TcpStream) -> Result<Self, Self::Error> {
        let mut info = unsafe { std::mem::zeroed::<libc::tcp_info>() };
        let mut len = std::mem::size_of::<libc::tcp_info>() as libc::socklen_t;

        let rc = unsafe {
            libc::getsockopt(
                stream.as_raw_fd(),
                libc::IPPROTO_TCP,
                libc::TCP_INFO,
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

#[cfg(target_os = "linux")]
impl From<libc::tcp_info> for TcpStats {
    /// Converts a [`libc::tcp_info`] into [`TcpStats`].
    fn from(info: libc::tcp_info) -> Self {
        // On Linux, tcpi_snd_cwnd is in segments; convert to bytes using snd_mss.
        let cwnd = info.tcpi_snd_cwnd.saturating_mul(info.tcpi_snd_mss);
        // Local advertised receive window (bytes).
        let rwnd = info.tcpi_rcv_space;

        // RTT fields are reported in microseconds.
        let last_rtt = Duration::from_micros(info.tcpi_rtt as u64);
        let smoothed_rtt = Duration::from_micros(info.tcpi_rtt as u64);
        let rtt_var = Duration::from_micros(info.tcpi_rttvar as u64);

        // Volumes: approximate using segment counts * MSS.
        let tx_bytes = (info.tcpi_segs_out as u64).saturating_mul(info.tcpi_snd_mss as u64);
        let rx_bytes = (info.tcpi_segs_in as u64).saturating_mul(info.tcpi_rcv_mss as u64);

        // Retransmissions
        let retransmitted_packets = info.tcpi_total_retrans as u64;
        let retransmitted_bytes = retransmitted_packets.saturating_mul(info.tcpi_snd_mss as u64);
        // RTO is in microseconds.
        let rto = Duration::from_micros(info.tcpi_rto as u64);

        Self {
            cwnd,
            rwnd,
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
