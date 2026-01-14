//! Ergonomic wrappers for Linux network sysctl parameters.
//!
//! This module provides type-safe access to `/proc/sys/net/*` parameters, which control
//! kernel networking behavior. These parameters are **per-namespace**, meaning each
//! network namespace can have independent configuration - essential for network simulation.
//!
//! # Protocol Selection
//!
//! Many parameters support both IPv4 and IPv6. Use the [`Protocol`] enum to specify:
//!
//! ```no_run
//! use msg_sim::sysctl::{self, Icmp, Protocol};
//!
//! // Read ICMPv4 setting
//! let v4 = sysctl::read(Icmp::EchoIgnoreAll, Protocol::V4)?;
//!
//! // Read ICMPv6 setting
//! let v6 = sysctl::read(Icmp::EchoIgnoreAll, Protocol::V6)?;
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! # Supported Parameter Categories
//!
//! - [`Tcp`] - TCP parameters (shared between IPv4/IPv6 in Linux)
//! - [`Udp`] - UDP parameters (shared between IPv4/IPv6 in Linux)
//! - [`Icmp`] - ICMP parameters (separate paths for IPv4/IPv6)
//! - [`Ip`] - General IP parameters (separate paths for IPv4/IPv6)
//!
//! # Usage
//!
//! ```no_run
//! use msg_sim::sysctl::{self, Tcp, Icmp, Ip, Protocol};
//!
//! // TCP parameters are shared - protocol argument is accepted but ignored
//! let value = sysctl::read(Tcp::SlowStartAfterIdle, Protocol::V4)?;
//! sysctl::write(Tcp::CongestionControl, Protocol::V4, "cubic")?;
//!
//! // ICMP has separate IPv4/IPv6 paths
//! sysctl::write(Icmp::EchoIgnoreAll, Protocol::V4, "1")?;
//! sysctl::write(Icmp::EchoIgnoreAll, Protocol::V6, "1")?;
//!
//! // IP settings are protocol-specific
//! let forwarding = sysctl::read(Ip::Forwarding, Protocol::V6)?;
//!
//! // For parameters not covered by this module, use direct fs access:
//! // std::fs::write("/proc/sys/net/ipv4/some_param", "value")?;
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! # Namespace Isolation
//!
//! When running inside a network namespace (via [`NetworkNamespace`](crate::namespace::NetworkNamespace)),
//! these sysctls affect only that namespace. The kernel dispatches reads/writes to the
//! calling process's namespace-specific `struct netns_ipv4` data.
//!
//! # Note on TCP/UDP and Protocol
//!
//! In Linux, TCP and UDP parameters under `/proc/sys/net/ipv4/tcp_*` and `udp_*` affect
//! **both** IPv4 and IPv6 connections. The kernel shares the TCP/UDP stack between
//! protocols. The `Protocol` argument is accepted for API consistency but doesn't
//! change the path for these parameters.

use std::io;

/// IP protocol version for sysctl paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Protocol {
    /// IPv4 (`/proc/sys/net/ipv4/...`)
    #[default]
    V4,
    /// IPv6 (`/proc/sys/net/ipv6/...`)
    V6,
}

/// Trait for sysctl parameters that can be read/written.
///
/// The `path` method takes a [`Protocol`] to support parameters that have
/// different paths for IPv4 and IPv6.
pub trait SysctlParam {
    /// Returns the full path to the sysctl file for the given protocol.
    fn path(&self, protocol: Protocol) -> &'static str;
}

/// Read a sysctl parameter value.
///
/// Returns the parameter value as a trimmed string.
///
/// # Example
///
/// ```no_run
/// use msg_sim::sysctl::{self, Tcp, Icmp, Protocol};
///
/// // TCP (protocol doesn't affect path)
/// let congestion = sysctl::read(Tcp::CongestionControl, Protocol::V4)?;
///
/// // ICMP (protocol selects IPv4 or IPv6 path)
/// let ignore_v4 = sysctl::read(Icmp::EchoIgnoreAll, Protocol::V4)?;
/// let ignore_v6 = sysctl::read(Icmp::EchoIgnoreAll, Protocol::V6)?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub fn read<P: SysctlParam>(param: P, protocol: Protocol) -> io::Result<String> {
    std::fs::read_to_string(param.path(protocol)).map(|s| s.trim().to_string())
}

/// Write a sysctl parameter value.
///
/// # Example
///
/// ```no_run
/// use msg_sim::sysctl::{self, Tcp, Icmp, Protocol};
///
/// // Disable slow start after idle (TCP is shared between protocols)
/// sysctl::write(Tcp::SlowStartAfterIdle, Protocol::V4, "0")?;
///
/// // Ignore ICMP echo for both protocols
/// sysctl::write(Icmp::EchoIgnoreAll, Protocol::V4, "1")?;
/// sysctl::write(Icmp::EchoIgnoreAll, Protocol::V6, "1")?;
/// # Ok::<(), std::io::Error>(())
/// ```
///
/// # Errors
///
/// Returns an error if:
/// - The parameter doesn't exist (wrong kernel version)
/// - Permission denied (not running as root)
/// - Invalid value for the parameter
pub fn write<P: SysctlParam>(param: P, protocol: Protocol, value: &str) -> io::Result<()> {
    std::fs::write(param.path(protocol), value)
}

// ============================================================================
// TCP Parameters
// ============================================================================

/// TCP sysctl parameters under `/proc/sys/net/ipv4/tcp_*`.
///
/// **Note:** In Linux, TCP parameters are shared between IPv4 and IPv6.
/// The protocol argument is accepted for API consistency but doesn't change
/// the path—all TCP sysctls are under `/proc/sys/net/ipv4/tcp_*`.
///
/// These parameters control TCP behavior and are commonly tuned for:
/// - High-bandwidth networks (buffer sizes)
/// - High-latency networks (congestion control, RTT settings)
/// - Testing specific TCP behaviors (slow start, SACK, timestamps)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Tcp {
    // ===== Buffer Sizes =====
    /// Receive buffer sizes: "min default max" (bytes).
    ///
    /// Controls the advertised receive window. Larger values allow higher throughput
    /// on high-bandwidth-delay-product paths.
    ///
    /// Default: "4096 131072 6291456" (4KB min, 128KB default, 6MB max)
    Rmem,

    /// Send buffer sizes: "min default max" (bytes).
    ///
    /// Controls how much unsent data can queue. Affects sender throughput.
    ///
    /// Default: "4096 16384 4194304" (4KB min, 16KB default, 4MB max)
    Wmem,

    /// Enable automatic receive buffer tuning (0/1).
    ///
    /// When enabled, kernel auto-tunes buffers based on path characteristics.
    /// Disable for deterministic simulation behavior.
    ///
    /// Default: 1 (enabled)
    ModerateRcvbuf,

    // ===== Congestion Control =====
    /// Default congestion control algorithm name.
    ///
    /// Common values: "cubic", "reno", "bbr" (if available)
    ///
    /// Default: "cubic" (most systems)
    CongestionControl,

    /// Pacing rate ratio during slow start (percentage).
    ///
    /// Rate = cwnd * mss / srtt * (ratio / 100)
    ///
    /// Default: 200 (2x calculated rate)
    PacingSsRatio,

    /// Pacing rate ratio during congestion avoidance (percentage).
    ///
    /// Default: 120 (1.2x calculated rate)
    PacingCaRatio,

    // ===== Timing and RTT =====
    /// Minimum retransmission timeout in microseconds.
    ///
    /// Floor for RTO calculation. Lower values = faster retransmission.
    ///
    /// Default: 200000 (200ms)
    RtoMinUs,

    /// Maximum retransmission timeout in milliseconds.
    ///
    /// Ceiling for RTO calculation.
    ///
    /// Default: 120000 (120 seconds)
    RtoMaxMs,

    /// Window length for min RTT tracking (seconds).
    ///
    /// Shorter = faster adaptation, longer = more stable.
    ///
    /// Default: 300
    MinRttWlen,

    /// Time before sending keepalive probes (seconds).
    ///
    /// Default: 7200 (2 hours)
    KeepaliveTime,

    /// Interval between keepalive probes (seconds).
    ///
    /// Default: 75
    KeepaliveIntvl,

    /// Number of keepalive probes before declaring connection dead.
    ///
    /// Default: 9
    KeepaliveProbes,

    /// Time to hold FIN_WAIT_2 state (seconds).
    ///
    /// Default: 60
    FinTimeout,

    // ===== Window and Scaling =====
    /// Enable RFC1323 window scaling (0/1).
    ///
    /// Required for windows > 64KB.
    ///
    /// Default: 1 (enabled)
    WindowScaling,

    /// Enable RFC1323 timestamps (0/1/2).
    ///
    /// 0 = disabled, 1 = enabled with random offset, 2 = enabled without offset
    ///
    /// Default: 1
    Timestamps,

    /// Enable selective acknowledgments (0/1).
    ///
    /// Improves loss recovery efficiency.
    ///
    /// Default: 1 (enabled)
    Sack,

    /// Enable duplicate SACK (0/1).
    ///
    /// Helps detect unnecessary retransmissions.
    ///
    /// Default: 1 (enabled)
    Dsack,

    // ===== Retransmission and Loss Recovery =====
    /// Number of SYN retries for active opens.
    ///
    /// Default: 6 (~67 seconds with exponential backoff)
    SynRetries,

    /// Number of SYNACK retries for passive opens.
    ///
    /// Default: 5 (~31 seconds)
    SynackRetries,

    /// Number of retries before reporting error to network layer.
    ///
    /// Default: 3
    Retries1,

    /// Number of retries before killing connection.
    ///
    /// Default: 15 (~924 seconds total)
    Retries2,

    /// Initial packet reordering tolerance.
    ///
    /// Default: 3
    Reordering,

    /// Maximum packet reordering tolerance.
    ///
    /// Default: 300
    MaxReordering,

    /// Loss recovery mode bitmap (RACK settings).
    ///
    /// 0x1 = RACK, 0x2 = static reordering window, 0x4 = disable dupACK threshold
    ///
    /// Default: 1
    Recovery,

    /// Forward RTO Recovery (0/1).
    ///
    /// Enhanced recovery for RTT fluctuations.
    ///
    /// Default: 1 (enabled when SACK enabled)
    Frto,

    // ===== Idle and Startup =====
    /// Reset cwnd after idle period (0/1).
    ///
    /// RFC2861 behavior. Set to 0 for persistent congestion window.
    ///
    /// Default: 1 (enabled)
    SlowStartAfterIdle,

    // ===== MTU Discovery =====
    /// Path MTU Discovery mode (0/1/2).
    ///
    /// 0 = disabled, 1 = adaptive (on ICMP black hole), 2 = always
    ///
    /// Default: 0 (disabled)
    MtuProbing,

    /// Initial MSS for MTU probing (bytes).
    ///
    /// Default: 1024
    BaseMss,

    /// Minimum MSS (bytes).
    ///
    /// Default: 48
    MinSndMss,

    // ===== ECN =====
    /// Explicit Congestion Notification mode (0/1/2).
    ///
    /// 0 = disabled, 1 = fully enabled, 2 = respond but don't request
    ///
    /// Default: 2
    Ecn,

    /// ECN fallback on misbehavior (0/1).
    ///
    /// Default: 1 (enabled)
    EcnFallback,

    // ===== Miscellaneous =====
    /// Enable SYN cookies (0/1/2).
    ///
    /// 0 = disabled, 1 = automatic under pressure, 2 = always
    ///
    /// Default: 1
    Syncookies,

    /// TCP Small Queue limit per socket (bytes).
    ///
    /// Limits queued bytes to reduce buffer bloat.
    ///
    /// Default: 131072 (128KB)
    LimitOutputBytes,

    /// TCP Fast Open mode.
    ///
    /// 0x1 = client, 0x2 = server, 0x4 = server without cookie, etc.
    ///
    /// Default: 1 (client only)
    Fastopen,

    /// Abort on listen backlog overflow (0/1).
    ///
    /// When enabled, sends RST instead of silently dropping.
    ///
    /// Default: 0
    AbortOnOverflow,
}

impl SysctlParam for Tcp {
    /// Returns the TCP sysctl path.
    ///
    /// Note: TCP parameters are shared between IPv4 and IPv6 in Linux,
    /// so the protocol argument doesn't affect the returned path.
    fn path(&self, _protocol: Protocol) -> &'static str {
        match self {
            // Buffer sizes
            Self::Rmem => "/proc/sys/net/ipv4/tcp_rmem",
            Self::Wmem => "/proc/sys/net/ipv4/tcp_wmem",
            Self::ModerateRcvbuf => "/proc/sys/net/ipv4/tcp_moderate_rcvbuf",
            // Congestion control
            Self::CongestionControl => "/proc/sys/net/ipv4/tcp_congestion_control",
            Self::PacingSsRatio => "/proc/sys/net/ipv4/tcp_pacing_ss_ratio",
            Self::PacingCaRatio => "/proc/sys/net/ipv4/tcp_pacing_ca_ratio",
            // Timing
            Self::RtoMinUs => "/proc/sys/net/ipv4/tcp_rto_min_us",
            Self::RtoMaxMs => "/proc/sys/net/ipv4/tcp_rto_max_ms",
            Self::MinRttWlen => "/proc/sys/net/ipv4/tcp_min_rtt_wlen",
            Self::KeepaliveTime => "/proc/sys/net/ipv4/tcp_keepalive_time",
            Self::KeepaliveIntvl => "/proc/sys/net/ipv4/tcp_keepalive_intvl",
            Self::KeepaliveProbes => "/proc/sys/net/ipv4/tcp_keepalive_probes",
            Self::FinTimeout => "/proc/sys/net/ipv4/tcp_fin_timeout",
            // Window/scaling
            Self::WindowScaling => "/proc/sys/net/ipv4/tcp_window_scaling",
            Self::Timestamps => "/proc/sys/net/ipv4/tcp_timestamps",
            Self::Sack => "/proc/sys/net/ipv4/tcp_sack",
            Self::Dsack => "/proc/sys/net/ipv4/tcp_dsack",
            // Retransmission
            Self::SynRetries => "/proc/sys/net/ipv4/tcp_syn_retries",
            Self::SynackRetries => "/proc/sys/net/ipv4/tcp_synack_retries",
            Self::Retries1 => "/proc/sys/net/ipv4/tcp_retries1",
            Self::Retries2 => "/proc/sys/net/ipv4/tcp_retries2",
            Self::Reordering => "/proc/sys/net/ipv4/tcp_reordering",
            Self::MaxReordering => "/proc/sys/net/ipv4/tcp_max_reordering",
            Self::Recovery => "/proc/sys/net/ipv4/tcp_recovery",
            Self::Frto => "/proc/sys/net/ipv4/tcp_frto",
            // Idle/startup
            Self::SlowStartAfterIdle => "/proc/sys/net/ipv4/tcp_slow_start_after_idle",
            // MTU
            Self::MtuProbing => "/proc/sys/net/ipv4/tcp_mtu_probing",
            Self::BaseMss => "/proc/sys/net/ipv4/tcp_base_mss",
            Self::MinSndMss => "/proc/sys/net/ipv4/tcp_min_snd_mss",
            // ECN
            Self::Ecn => "/proc/sys/net/ipv4/tcp_ecn",
            Self::EcnFallback => "/proc/sys/net/ipv4/tcp_ecn_fallback",
            // Misc
            Self::Syncookies => "/proc/sys/net/ipv4/tcp_syncookies",
            Self::LimitOutputBytes => "/proc/sys/net/ipv4/tcp_limit_output_bytes",
            Self::Fastopen => "/proc/sys/net/ipv4/tcp_fastopen",
            Self::AbortOnOverflow => "/proc/sys/net/ipv4/tcp_abort_on_overflow",
        }
    }
}

// ============================================================================
// UDP Parameters
// ============================================================================

/// UDP sysctl parameters under `/proc/sys/net/ipv4/udp_*`.
///
/// **Note:** Like TCP, UDP parameters are shared between IPv4 and IPv6 in Linux.
/// The protocol argument doesn't change the path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Udp {
    /// Minimum receive buffer size (bytes).
    ///
    /// Default: PAGE_SIZE (typically 4096)
    RmemMin,

    /// Minimum send buffer size (bytes).
    ///
    /// Default: PAGE_SIZE (typically 4096)
    WmemMin,
}

impl SysctlParam for Udp {
    fn path(&self, _protocol: Protocol) -> &'static str {
        match self {
            Self::RmemMin => "/proc/sys/net/ipv4/udp_rmem_min",
            Self::WmemMin => "/proc/sys/net/ipv4/udp_wmem_min",
        }
    }
}

// ============================================================================
// ICMP Parameters
// ============================================================================

/// ICMP sysctl parameters.
///
/// Unlike TCP/UDP, ICMP has **separate paths** for IPv4 and IPv6:
/// - IPv4: `/proc/sys/net/ipv4/icmp_*`
/// - IPv6: `/proc/sys/net/ipv6/icmp/*`
///
/// Use [`Protocol::V4`] or [`Protocol::V6`] to select the appropriate path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Icmp {
    /// Ignore all ICMP/ICMPv6 echo requests (0/1).
    ///
    /// Default: 0 (respond to pings)
    EchoIgnoreAll,

    /// Ignore broadcast ICMP echo requests (0/1). **IPv4 only.**
    ///
    /// For IPv6, this returns the same path as `EchoIgnoreMulticast`.
    ///
    /// Default: 1 (ignore broadcast pings)
    EchoIgnoreBroadcasts,

    /// Ignore multicast ICMP echo requests (0/1). **IPv6 only.**
    ///
    /// For IPv4, this returns the same path as `EchoIgnoreBroadcasts`.
    ///
    /// Default: 0
    EchoIgnoreMulticast,

    /// Ignore anycast ICMPv6 echo requests (0/1). **IPv6 only.**
    ///
    /// For IPv4, returns the broadcast path as fallback.
    ///
    /// Default: 0
    EchoIgnoreAnycast,

    /// Rate limit for ICMP messages (milliseconds).
    ///
    /// Default: 1000
    Ratelimit,

    /// Maximum ICMP messages per second. **IPv4 only.**
    ///
    /// For IPv6, returns the ratelimit path.
    ///
    /// Default: 1000
    MsgsPerSec,

    /// ICMP message burst size. **IPv4 only.**
    ///
    /// For IPv6, returns the ratelimit path.
    ///
    /// Default: 50
    MsgsBurst,

    /// Ignore bogus ICMP error responses (0/1). **IPv4 only.**
    ///
    /// For IPv6, returns the echo_ignore_all path.
    ///
    /// Default: 1 (ignore)
    IgnoreBogusErrorResponses,
}

impl SysctlParam for Icmp {
    fn path(&self, protocol: Protocol) -> &'static str {
        match (self, protocol) {
            // EchoIgnoreAll - both protocols
            (Self::EchoIgnoreAll, Protocol::V4) => "/proc/sys/net/ipv4/icmp_echo_ignore_all",
            (Self::EchoIgnoreAll, Protocol::V6) => "/proc/sys/net/ipv6/icmp/echo_ignore_all",

            // EchoIgnoreBroadcasts (V4) / EchoIgnoreMulticast (V6)
            (Self::EchoIgnoreBroadcasts, Protocol::V4) => {
                "/proc/sys/net/ipv4/icmp_echo_ignore_broadcasts"
            }
            (Self::EchoIgnoreBroadcasts, Protocol::V6) => {
                "/proc/sys/net/ipv6/icmp/echo_ignore_multicast"
            }

            (Self::EchoIgnoreMulticast, Protocol::V4) => {
                "/proc/sys/net/ipv4/icmp_echo_ignore_broadcasts"
            }
            (Self::EchoIgnoreMulticast, Protocol::V6) => {
                "/proc/sys/net/ipv6/icmp/echo_ignore_multicast"
            }

            // EchoIgnoreAnycast - V6 only, fallback to broadcasts for V4
            (Self::EchoIgnoreAnycast, Protocol::V4) => {
                "/proc/sys/net/ipv4/icmp_echo_ignore_broadcasts"
            }
            (Self::EchoIgnoreAnycast, Protocol::V6) => {
                "/proc/sys/net/ipv6/icmp/echo_ignore_anycast"
            }

            // Ratelimit - both protocols
            (Self::Ratelimit, Protocol::V4) => "/proc/sys/net/ipv4/icmp_ratelimit",
            (Self::Ratelimit, Protocol::V6) => "/proc/sys/net/ipv6/icmp/ratelimit",

            // MsgsPerSec - V4 only, fallback to ratelimit for V6
            (Self::MsgsPerSec, Protocol::V4) => "/proc/sys/net/ipv4/icmp_msgs_per_sec",
            (Self::MsgsPerSec, Protocol::V6) => "/proc/sys/net/ipv6/icmp/ratelimit",

            // MsgsBurst - V4 only, fallback to ratelimit for V6
            (Self::MsgsBurst, Protocol::V4) => "/proc/sys/net/ipv4/icmp_msgs_burst",
            (Self::MsgsBurst, Protocol::V6) => "/proc/sys/net/ipv6/icmp/ratelimit",

            // IgnoreBogusErrorResponses - V4 only
            (Self::IgnoreBogusErrorResponses, Protocol::V4) => {
                "/proc/sys/net/ipv4/icmp_ignore_bogus_error_responses"
            }
            (Self::IgnoreBogusErrorResponses, Protocol::V6) => {
                "/proc/sys/net/ipv6/icmp/echo_ignore_all"
            }
        }
    }
}

// ============================================================================
// General IP Parameters
// ============================================================================

/// General IP sysctl parameters.
///
/// These have **separate paths** for IPv4 and IPv6:
/// - IPv4: `/proc/sys/net/ipv4/*`
/// - IPv6: `/proc/sys/net/ipv6/conf/all/*` or `/proc/sys/net/ipv6/conf/default/*`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Ip {
    /// Enable IP forwarding (0/1).
    ///
    /// - IPv4: `/proc/sys/net/ipv4/ip_forward`
    /// - IPv6: `/proc/sys/net/ipv6/conf/all/forwarding`
    ///
    /// Default: 0 (disabled)
    Forwarding,

    /// Default TTL (IPv4) / Hop Limit (IPv6) for outgoing packets.
    ///
    /// - IPv4: `/proc/sys/net/ipv4/ip_default_ttl`
    /// - IPv6: `/proc/sys/net/ipv6/conf/default/hop_limit`
    ///
    /// Default: 64
    DefaultTtl,

    /// Disable the protocol on all interfaces (0/1). **IPv6 only.**
    ///
    /// For IPv4, returns the forwarding path (no equivalent).
    ///
    /// Default: 0
    Disable,

    /// Local port range for outgoing connections: "low high". **IPv4 only.**
    ///
    /// For IPv6, returns the same IPv4 path (shared).
    ///
    /// Default: "32768 60999"
    LocalPortRange,

    /// Accept ICMP redirects (0/1).
    ///
    /// - IPv4: via `/proc/sys/net/ipv4/conf/all/accept_redirects`
    /// - IPv6: `/proc/sys/net/ipv6/conf/all/accept_redirects`
    ///
    /// Default: varies
    AcceptRedirects,

    /// Accept source routing (0/1).
    ///
    /// - IPv4: via `/proc/sys/net/ipv4/conf/all/accept_source_route`
    /// - IPv6: `/proc/sys/net/ipv6/conf/all/accept_source_route`
    ///
    /// Default: 0
    AcceptSourceRoute,

    /// Accept Router Advertisements (0/1/2). **IPv6 only.**
    ///
    /// 0 = disabled, 1 = enabled if forwarding disabled, 2 = always
    ///
    /// For IPv4, returns forwarding path (no equivalent).
    ///
    /// Default: 1
    AcceptRa,

    /// Allow binding to non-local addresses (0/1).
    ///
    /// Default: 0
    NonlocalBind,

    /// Fragment reassembly high threshold (bytes).
    ///
    /// - IPv4: `/proc/sys/net/ipv4/ipfrag_high_thresh`
    /// - IPv6: `/proc/sys/net/ipv6/ip6frag_high_thresh`
    FragHighThresh,

    /// Fragment reassembly low threshold (bytes).
    ///
    /// - IPv4: `/proc/sys/net/ipv4/ipfrag_low_thresh`
    /// - IPv6: `/proc/sys/net/ipv6/ip6frag_low_thresh`
    FragLowThresh,

    /// Time to keep fragments in memory (seconds).
    ///
    /// - IPv4: `/proc/sys/net/ipv4/ipfrag_time`
    /// - IPv6: `/proc/sys/net/ipv6/ip6frag_time`
    FragTime,
}

impl SysctlParam for Ip {
    fn path(&self, protocol: Protocol) -> &'static str {
        match (self, protocol) {
            (Self::Forwarding, Protocol::V4) => "/proc/sys/net/ipv4/ip_forward",
            (Self::Forwarding, Protocol::V6) => "/proc/sys/net/ipv6/conf/all/forwarding",

            (Self::DefaultTtl, Protocol::V4) => "/proc/sys/net/ipv4/ip_default_ttl",
            (Self::DefaultTtl, Protocol::V6) => "/proc/sys/net/ipv6/conf/default/hop_limit",

            (Self::Disable, Protocol::V4) => "/proc/sys/net/ipv4/ip_forward", // no equivalent
            (Self::Disable, Protocol::V6) => "/proc/sys/net/ipv6/conf/all/disable_ipv6",

            (Self::LocalPortRange, Protocol::V4) => "/proc/sys/net/ipv4/ip_local_port_range",
            (Self::LocalPortRange, Protocol::V6) => "/proc/sys/net/ipv4/ip_local_port_range", // shared

            (Self::AcceptRedirects, Protocol::V4) => "/proc/sys/net/ipv4/conf/all/accept_redirects",
            (Self::AcceptRedirects, Protocol::V6) => "/proc/sys/net/ipv6/conf/all/accept_redirects",

            (Self::AcceptSourceRoute, Protocol::V4) => {
                "/proc/sys/net/ipv4/conf/all/accept_source_route"
            }
            (Self::AcceptSourceRoute, Protocol::V6) => {
                "/proc/sys/net/ipv6/conf/all/accept_source_route"
            }

            (Self::AcceptRa, Protocol::V4) => "/proc/sys/net/ipv4/ip_forward", // no equivalent
            (Self::AcceptRa, Protocol::V6) => "/proc/sys/net/ipv6/conf/all/accept_ra",

            (Self::NonlocalBind, Protocol::V4) => "/proc/sys/net/ipv4/ip_nonlocal_bind",
            (Self::NonlocalBind, Protocol::V6) => "/proc/sys/net/ipv6/ip_nonlocal_bind",

            (Self::FragHighThresh, Protocol::V4) => "/proc/sys/net/ipv4/ipfrag_high_thresh",
            (Self::FragHighThresh, Protocol::V6) => "/proc/sys/net/ipv6/ip6frag_high_thresh",

            (Self::FragLowThresh, Protocol::V4) => "/proc/sys/net/ipv4/ipfrag_low_thresh",
            (Self::FragLowThresh, Protocol::V6) => "/proc/sys/net/ipv6/ip6frag_low_thresh",

            (Self::FragTime, Protocol::V4) => "/proc/sys/net/ipv4/ipfrag_time",
            (Self::FragTime, Protocol::V6) => "/proc/sys/net/ipv6/ip6frag_time",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn tcp_paths_are_correct() {
        assert_eq!(
            Tcp::SlowStartAfterIdle.path(Protocol::V4),
            "/proc/sys/net/ipv4/tcp_slow_start_after_idle"
        );
        assert_eq!(
            Tcp::CongestionControl.path(Protocol::V4),
            "/proc/sys/net/ipv4/tcp_congestion_control"
        );
        // TCP paths are same for V4 and V6
        assert_eq!(
            Tcp::SlowStartAfterIdle.path(Protocol::V6),
            "/proc/sys/net/ipv4/tcp_slow_start_after_idle"
        );
    }

    #[test]
    fn icmp_paths_differ_by_protocol() {
        assert_eq!(
            Icmp::EchoIgnoreAll.path(Protocol::V4),
            "/proc/sys/net/ipv4/icmp_echo_ignore_all"
        );
        assert_eq!(
            Icmp::EchoIgnoreAll.path(Protocol::V6),
            "/proc/sys/net/ipv6/icmp/echo_ignore_all"
        );
        assert_eq!(Icmp::Ratelimit.path(Protocol::V4), "/proc/sys/net/ipv4/icmp_ratelimit");
        assert_eq!(Icmp::Ratelimit.path(Protocol::V6), "/proc/sys/net/ipv6/icmp/ratelimit");
    }

    #[test]
    fn ip_paths_differ_by_protocol() {
        assert_eq!(Ip::Forwarding.path(Protocol::V4), "/proc/sys/net/ipv4/ip_forward");
        assert_eq!(Ip::Forwarding.path(Protocol::V6), "/proc/sys/net/ipv6/conf/all/forwarding");
        assert_eq!(Ip::DefaultTtl.path(Protocol::V4), "/proc/sys/net/ipv4/ip_default_ttl");
        assert_eq!(Ip::DefaultTtl.path(Protocol::V6), "/proc/sys/net/ipv6/conf/default/hop_limit");
    }

    #[test]
    fn tcp_sysctl_paths_exist() {
        let tcp_params = [
            Tcp::Rmem,
            Tcp::Wmem,
            Tcp::ModerateRcvbuf,
            Tcp::CongestionControl,
            Tcp::PacingSsRatio,
            Tcp::PacingCaRatio,
            Tcp::KeepaliveTime,
            Tcp::KeepaliveIntvl,
            Tcp::KeepaliveProbes,
            Tcp::FinTimeout,
            Tcp::WindowScaling,
            Tcp::Timestamps,
            Tcp::Sack,
            Tcp::Dsack,
            Tcp::SynRetries,
            Tcp::SynackRetries,
            Tcp::Retries1,
            Tcp::Retries2,
            Tcp::Reordering,
            Tcp::MaxReordering,
            Tcp::Recovery,
            Tcp::Frto,
            Tcp::SlowStartAfterIdle,
            Tcp::MtuProbing,
            Tcp::BaseMss,
            Tcp::MinSndMss,
            Tcp::Ecn,
            Tcp::EcnFallback,
            Tcp::Syncookies,
            Tcp::LimitOutputBytes,
            Tcp::Fastopen,
            Tcp::AbortOnOverflow,
        ];

        for param in tcp_params {
            assert!(
                Path::new(param.path(Protocol::V4)).exists(),
                "TCP sysctl path should exist: {}",
                param.path(Protocol::V4)
            );
        }
    }

    #[test]
    fn udp_sysctl_paths_exist() {
        let udp_params = [Udp::RmemMin, Udp::WmemMin];

        for param in udp_params {
            assert!(
                Path::new(param.path(Protocol::V4)).exists(),
                "UDP sysctl path should exist: {}",
                param.path(Protocol::V4)
            );
        }
    }

    #[test]
    fn icmp_v4_sysctl_paths_exist() {
        let icmp_params = [
            Icmp::EchoIgnoreAll,
            Icmp::EchoIgnoreBroadcasts,
            Icmp::Ratelimit,
            Icmp::MsgsPerSec,
            Icmp::MsgsBurst,
            Icmp::IgnoreBogusErrorResponses,
        ];

        for param in icmp_params {
            assert!(
                Path::new(param.path(Protocol::V4)).exists(),
                "ICMP v4 sysctl path should exist: {}",
                param.path(Protocol::V4)
            );
        }
    }

    #[test]
    fn icmp_v6_sysctl_paths_exist() {
        let icmp_params = [
            Icmp::EchoIgnoreAll,
            Icmp::EchoIgnoreMulticast,
            Icmp::EchoIgnoreAnycast,
            Icmp::Ratelimit,
        ];

        for param in icmp_params {
            assert!(
                Path::new(param.path(Protocol::V6)).exists(),
                "ICMP v6 sysctl path should exist: {}",
                param.path(Protocol::V6)
            );
        }
    }

    #[test]
    fn ip_v4_sysctl_paths_exist() {
        let ip_params = [Ip::Forwarding, Ip::DefaultTtl, Ip::LocalPortRange, Ip::NonlocalBind];

        for param in ip_params {
            assert!(
                Path::new(param.path(Protocol::V4)).exists(),
                "IP v4 sysctl path should exist: {}",
                param.path(Protocol::V4)
            );
        }
    }

    #[test]
    fn ip_v6_sysctl_paths_exist() {
        let ip_params = [
            Ip::Forwarding,
            Ip::DefaultTtl,
            Ip::Disable,
            Ip::AcceptRedirects,
            Ip::AcceptSourceRoute,
            Ip::AcceptRa,
            Ip::NonlocalBind,
            Ip::FragHighThresh,
            Ip::FragLowThresh,
            Ip::FragTime,
        ];

        for param in ip_params {
            assert!(
                Path::new(param.path(Protocol::V6)).exists(),
                "IP v6 sysctl path should exist: {}",
                param.path(Protocol::V6)
            );
        }
    }
}
