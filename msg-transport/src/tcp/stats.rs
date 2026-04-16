use std::time::Duration;

/// TCP connection statistics.
///
/// When using simulated networking (e.g., with the `turmoil` feature), these stats
/// may be unavailable or return default values since there's no real OS-level TCP socket.
#[derive(Debug, Default)]
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

/// Real TCP stats implementation using OS socket options.
/// This is always compiled for tokio's TcpStream (used by tcp-tls and normal tcp).
#[cfg(any(not(feature = "turmoil"), feature = "tcp-tls"))]
mod os_stats {
    use super::*;
    use std::os::fd::AsRawFd;

    /// Helper function to get a socket option from a TCP stream.
    fn getsockopt<T>(stream: &impl AsRawFd, option: libc::c_int) -> std::io::Result<T> {
        let mut info = unsafe { std::mem::zeroed::<T>() };
        let mut len = std::mem::size_of::<T>() as libc::socklen_t;
        let dst = &mut info as *mut _ as *mut _;

        let result = unsafe {
            libc::getsockopt(stream.as_raw_fd(), libc::IPPROTO_TCP, option, dst, &mut len)
        };

        if result != 0 {
            return Err(std::io::Error::last_os_error());
        }

        Ok(info)
    }

    /// Implement stats for tokio's TcpStream.
    #[cfg(target_os = "macos")]
    impl TryFrom<&tokio::net::TcpStream> for TcpStats {
        type Error = std::io::Error;

        fn try_from(stream: &tokio::net::TcpStream) -> Result<Self, Self::Error> {
            let info = getsockopt::<libc::tcp_connection_info>(stream, libc::TCP_CONNECTION_INFO)?;
            Ok(info.into())
        }
    }

    #[cfg(target_os = "macos")]
    impl From<libc::tcp_connection_info> for TcpStats {
        fn from(info: libc::tcp_connection_info) -> Self {
            let congestion_window = info.tcpi_snd_cwnd;
            let receive_window = info.tcpi_rcv_wnd;
            let send_window = info.tcpi_snd_wnd;
            let last_rtt = Duration::from_millis(info.tcpi_rttcur as u64);
            let smoothed_rtt = Duration::from_millis(info.tcpi_srtt as u64);
            let rtt_variance = Duration::from_millis(info.tcpi_rttvar as u64);
            let tx_bytes = info.tcpi_txbytes;
            let rx_bytes = info.tcpi_rxbytes;
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
    impl TryFrom<&tokio::net::TcpStream> for TcpStats {
        type Error = std::io::Error;

        fn try_from(stream: &tokio::net::TcpStream) -> Result<Self, Self::Error> {
            let info = getsockopt::<libc::tcp_info>(stream, libc::TCP_INFO)?;
            Ok(info.into())
        }
    }

    #[cfg(target_os = "linux")]
    impl From<libc::tcp_info> for TcpStats {
        fn from(info: libc::tcp_info) -> Self {
            let congestion_window = info.tcpi_snd_cwnd.saturating_mul(info.tcpi_snd_mss);
            let receive_window = info.tcpi_rcv_space;
            let smoothed_rtt = Duration::from_micros(info.tcpi_rtt as u64);
            let rtt_variance = Duration::from_micros(info.tcpi_rttvar as u64);
            let retransmitted_packets = info.tcpi_total_retrans as u64;
            let retransmitted_bytes = retransmitted_packets.saturating_mul(info.tcpi_snd_mss as u64);
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
}

// Turmoil simulated stats: OS-level TCP counters don't exist for simulated sockets,
// so we return the default (zeroed) stats. Surfacing an error here would cause
// `MeteredIo::maybe_refresh` to log at error level on every refresh of every active
// connection, which is expected behavior in simulation rather than a failure.
//
// Without the turmoil feature, `crate::net::TcpStream` is `tokio::net::TcpStream`,
// and the impl in `os_stats` already satisfies `Transport::Stats`.
#[cfg(feature = "turmoil")]
mod turmoil_stats {
    use super::*;
    use crate::net::TcpStream;

    // `From` yields `TryFrom` through the standard library's blanket impl (with
    // `Error = Infallible`), which is enough to satisfy `Transport::Stats`.
    impl From<&TcpStream> for TcpStats {
        fn from(_stream: &TcpStream) -> Self {
            TcpStats::default()
        }
    }
}
