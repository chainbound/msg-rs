//! # Traffic Control Utilities
//!
//! This module provides utilities for creating Linux traffic control (tc) requests using
//! rtnetlink. It enables network emulation by configuring queue disciplines (qdiscs) that
//! can introduce latency, packet loss, bandwidth limits, and other impairments.
//!
//! ## Architecture Overview
//!
//! We use a hierarchical qdisc structure to enable per-destination impairments. This means
//! that peer A can have different network conditions when talking to peer B vs peer C.
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────┐
//! │                        HTB Root Qdisc (1:0)                                 │
//! │                                                                             │
//! │   The Hierarchical Token Bucket (HTB) qdisc serves as our root scheduler.  │
//! │   It allows us to create an arbitrary number of classes, one per           │
//! │   destination peer. Each class can have its own chain of qdiscs.           │
//! └─────────────────────────────────────────────────────────────────────────────┘
//!                                    │
//!            ┌───────────────────────┼───────────────────────┐
//!            │                       │                       │
//!            ▼                       ▼                       ▼
//! ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
//! │  Class 1:1       │    │  Class 1:11      │    │  Class 1:12      │
//! │  (default)       │    │  (dest=peer 1)   │    │  (dest=peer 2)   │
//! │                  │    │                  │    │                  │
//! │  Unimpaired      │    │  Impaired path   │    │  Impaired path   │
//! │  traffic         │    │  to peer 1       │    │  to peer 2       │
//! └──────────────────┘    └──────────────────┘    └──────────────────┘
//!                                    │                       │
//!                                    ▼                       ▼
//!                         ┌──────────────────┐    ┌──────────────────┐
//!                         │  TBF (11:1)      │    │  TBF (12:1)      │
//!                         │  (optional)      │    │  (optional)      │
//!                         │                  │    │                  │
//!                         │  Rate limiting   │    │  Rate limiting   │
//!                         │  if bandwidth    │    │  if bandwidth    │
//!                         │  is configured   │    │  is configured   │
//!                         └──────────────────┘    └──────────────────┘
//!                                    │                       │
//!                                    ▼                       ▼
//!                         ┌──────────────────┐    ┌──────────────────┐
//!                         │  Netem (11:0)    │    │  Netem (12:0)    │
//!                         │                  │    │                  │
//!                         │  Latency, loss,  │    │  Different       │
//!                         │  jitter, etc.    │    │  impairments     │
//!                         └──────────────────┘    └──────────────────┘
//! ```
//!
//! ## Handle Numbering Scheme
//!
//! TC handles are 32-bit values split into major:minor (16:16 bits). Our scheme:
//!
//! | Component        | Handle          | Example (peer_id=2) |
//! |------------------|-----------------|---------------------|
//! | HTB root         | `1:0`           | `1:0`               |
//! | Default class    | `1:1`           | `1:1`               |
//! | Per-dest class   | `1:(10+id)`     | `1:12`              |
//! | TBF qdisc        | `(10+id):0`     | `12:0`              |
//! | Netem qdisc      | `(20+id):0`     | `22:0`              |
//!
//! ## Packet Flow
//!
//! 1. Packet enters HTB root qdisc
//! 2. Flower filter examines destination IP
//! 3. If destination matches a configured peer → route to that peer's class
//! 4. Otherwise → route to default class (1:1, no impairment)
//! 5. In the peer's class: TBF applies rate limiting (if configured)
//! 6. Then netem applies delay, loss, jitter, etc.

use std::io::{self, Read as _};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::LazyLock;

use nix::libc::TCA_OPTIONS;
use rtnetlink::packet_core::{DefaultNla, NLA_HEADER_SIZE, NLM_F_REPLACE, NetlinkMessage};
use rtnetlink::packet_route::tc::TcFilterFlowerOption;
use rtnetlink::packet_route::{
    RouteNetlinkMessage,
    tc::{TcAttribute, TcHandle, TcMessage, TcOption},
};

use rtnetlink::packet_core::{NLM_F_ACK, NLM_F_CREATE, NLM_F_EXCL, NLM_F_REQUEST};

// -------------------------------------------------------------------------------------
// Netlink Attribute Type Constants
// -------------------------------------------------------------------------------------

/// EtherType for IPv4 packets (0x0800).
const ETH_P_IP: u16 = nix::libc::ETH_P_IP as u16;

/// EtherType for IPv6 packets (0x86DD).
const ETH_P_IPV6: u16 = nix::libc::ETH_P_IPV6 as u16;

// HTB-specific TCA_OPTIONS sub-attributes (from linux/pkt_sched.h)
/// HTB class parameters attribute type.
const TCA_HTB_PARMS: u16 = 1;
/// HTB qdisc init/global options attribute type.
const TCA_HTB_INIT: u16 = 2;

// TBF-specific TCA_OPTIONS sub-attributes (from linux/pkt_sched.h)
/// TBF parameters attribute type.
const TCA_TBF_PARMS: u16 = 1;
/// TBF rate table attribute type.
const TCA_TBF_RTAB: u16 = 2;
/// TBF burst size in bytes (required by modern kernels).
const TCA_TBF_BURST: u16 = 6;

/// Path to the kernel's packet scheduler timing information.
///
/// This file exposes how to convert between scheduler ticks and microseconds,
/// which is essential for configuring latency and jitter values correctly.
pub const PSCHED_PATH: &str = "/proc/net/psched";

/// The offset added to peer IDs to compute HTB class minor numbers.
///
/// For peer ID `N`, the class minor is `CLASS_MINOR_OFFSET + N`.
/// This keeps class 1:1 reserved as the default (unimpaired) class.
pub const CLASS_MINOR_OFFSET: u32 = 10;

/// The default HTB rate for classes, in bytes per second.
///
/// We set this very high (10 Gbit/s) because actual rate limiting is done by TBF.
/// HTB classes require a rate, but we use it only for traffic classification.
const HTB_DEFAULT_RATE_BYTES: u64 = 10_000_000_000 / 8; // 10 Gbit/s in bytes

/// The default HTB ceiling (max burst rate), same as the rate.
const HTB_DEFAULT_CEIL_BYTES: u64 = HTB_DEFAULT_RATE_BYTES;

/// Initialize the packet scheduler time base by reading `/proc/net/psched`.
///
/// The Linux kernel's traffic control subsystem uses its own time units ("ticks").
/// This function reads the conversion factor from `/proc/net/psched`, which contains
/// four hex values. The first two represent the ratio of ticks to microseconds.
///
/// # How it works
///
/// The file format is: `t2us us2t clock resolution`
///
/// - `t2us`: ticks to microseconds numerator
/// - `us2t`: ticks to microseconds denominator
/// - The ratio `t2us / us2t` gives us ticks per microsecond
///
/// # Returns
///
/// The number of ticks per microsecond, used by [`usec_to_ticks`].
///
/// # Reference
///
/// Adapted from `iproute2/tc/tc_core.c`.
pub fn tc_core_init() -> io::Result<f64> {
    let mut file = std::fs::File::open(PSCHED_PATH)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;

    let mut iter = contents.split_whitespace();

    let t2us = u32::from_str_radix(iter.next().expect("t2us"), 16).expect("t2us hex");
    let us2t = u32::from_str_radix(iter.next().expect("us2t"), 16).expect("us2t hex");

    // Number of ticks within a microsecond.
    let tick_in_usec = t2us as f64 / us2t as f64;

    tracing::debug!(t2us, us2t, tick_in_usec, "read {PSCHED_PATH}");

    Ok(tick_in_usec)
}

/// Cached value of ticks per microsecond, initialized lazily on first use.
///
/// This avoids repeatedly reading `/proc/net/psched` for every qdisc configuration.
pub static TICK_IN_USEC: LazyLock<f64> =
    LazyLock::new(|| tc_core_init().expect("to read /proc/net/psched"));

/// Convert microseconds to kernel packet scheduler ticks.
///
/// The kernel's netem qdisc expects latency and jitter in ticks, not microseconds.
/// This function performs the conversion using the system's tick rate.
///
/// # Arguments
///
/// * `delay_usec` - The delay in microseconds
/// * `tick_in_usec` - The conversion factor (ticks per microsecond)
///
/// # Returns
///
/// The equivalent value in packet scheduler ticks.
pub fn usec_to_ticks(delay_usec: u32, tick_in_usec: f64) -> u32 {
    (delay_usec as f64 / tick_in_usec).round() as u32
}

/// Convert a time value in microseconds to kernel ticks, using the cached tick rate.
fn usec_to_ticks_cached(delay_usec: u32) -> u32 {
    (delay_usec as f64 * *TICK_IN_USEC) as u32
}

// -------------------------------------------------------------------------------------
// Link Impairment Configuration
// -------------------------------------------------------------------------------------

/// Configuration for network impairments to apply to a link.
///
/// This struct represents all the ways you can degrade a network link for testing
/// purposes. Each field maps to a feature of Linux's `netem` qdisc or `tbf` qdisc.
///
/// # Example
///
/// ```ignore
/// use msg_sim::tc::LinkImpairment;
///
/// // Simulate a lossy, high-latency satellite link with limited bandwidth
/// let satellite_link = LinkImpairment {
///     latency: 300_000,           // 300ms one-way delay
///     jitter: 50_000,             // ±50ms variation
///     loss: 1.0,                  // 1% packet loss
///     bandwidth_mbit: Some(10.0), // 10 Mbit/s bandwidth cap
///     ..Default::default()
/// };
///
/// // Simulate a local network with occasional issues
/// let flaky_lan = LinkImpairment {
///     latency: 1_000,             // 1ms base latency
///     duplicate: 0.1,             // 0.1% duplicate packets
///     ..Default::default()
/// };
/// ```
///
/// # Bandwidth Limiting
///
/// When `bandwidth_mbit` is set, a Token Bucket Filter (TBF) qdisc is inserted
/// before netem in the qdisc chain. TBF works by:
///
/// 1. Tokens accumulate at `bandwidth_mbit` rate
/// 2. Each byte transmitted consumes one token
/// 3. Burst allows temporary excess up to `burst_kib` bytes
/// 4. When tokens are exhausted, packets queue (up to `tbf_limit_bytes`)
/// 5. If the queue overflows, packets are dropped
///
/// The hierarchy becomes: `HTB class → TBF → netem` instead of `HTB class → netem`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LinkImpairment {
    // ---------------------------------------------------------------------------------
    // Netem parameters (delay, loss, reordering)
    // ---------------------------------------------------------------------------------
    /// Base latency to add to all packets, in microseconds.
    ///
    /// This simulates propagation delay. A value of 1_000_000 means 1 second delay.
    /// Combined with `jitter`, this models real-world variable latency.
    pub latency: u32,

    /// Maximum number of packets that can be queued in the netem qdisc.
    ///
    /// When this limit is reached, additional packets are dropped. Default is 1000,
    /// which matches netem's default. Increase for high-bandwidth, high-latency links
    /// to avoid artificial drops.
    pub netem_limit: u32,

    /// Packet loss percentage (0.0 to 100.0).
    ///
    /// Packets are randomly dropped with this probability. A value of 5.0 means
    /// approximately 5% of packets will be lost.
    pub loss: f64,

    /// Packet reordering gap.
    ///
    /// When non-zero, enables packet reordering. The gap specifies how many packets
    /// can be reordered. For example, gap=5 means every 5th packet is sent immediately
    /// while others are delayed, causing reordering.
    pub gap: u32,

    /// Packet duplication percentage (0.0 to 100.0).
    ///
    /// Packets are randomly duplicated with this probability. A value of 1.0 means
    /// approximately 1% of packets will be sent twice.
    pub duplicate: f64,

    /// Random jitter added to latency, in microseconds.
    ///
    /// The actual delay for each packet is `latency ± jitter` (uniform distribution).
    /// This models the variable queuing delays seen in real networks.
    pub jitter: u32,

    // ---------------------------------------------------------------------------------
    // TBF parameters (bandwidth limiting)
    // ---------------------------------------------------------------------------------
    /// Maximum bandwidth in megabits per second (Mbit/s).
    ///
    /// When set, a Token Bucket Filter (TBF) qdisc is added to enforce this rate limit.
    /// For example, `Some(100.0)` limits the link to 100 Mbit/s.
    ///
    /// When `None`, no bandwidth limiting is applied (unlimited speed).
    pub bandwidth_mbit: Option<f64>,

    /// Burst size in kibibytes (KiB).
    ///
    /// The maximum amount of data that can be sent at once before rate limiting kicks in.
    /// This allows short bursts above the rate limit, which is important for bursty
    /// traffic patterns like HTTP requests.
    ///
    /// When `None`, a sensible default is computed:
    /// `max(bandwidth_bytes_per_sec / 8, 15_000)` (either 1/8 second of traffic or
    /// 10 MTU-sized packets, whichever is larger).
    pub burst_kib: Option<u32>,

    /// Maximum bytes that can be queued waiting for tokens in TBF.
    ///
    /// When the token bucket is empty, packets queue here. If this limit is exceeded,
    /// packets are dropped. This simulates buffer exhaustion under sustained overload.
    ///
    /// When `None`, defaults to `burst_bytes * 2` (allowing one burst in-flight
    /// while another queues).
    pub tbf_limit_bytes: Option<u32>,
}

impl Default for LinkImpairment {
    fn default() -> Self {
        Self {
            // Netem defaults: no impairment
            latency: 0,
            netem_limit: 1_000, // netem's default queue size
            loss: 0.0,
            gap: 0,
            duplicate: 0.0,
            jitter: 0,

            // TBF defaults: no bandwidth limiting
            bandwidth_mbit: None,
            burst_kib: None,
            tbf_limit_bytes: None,
        }
    }
}

impl LinkImpairment {
    /// Returns `true` if bandwidth limiting is configured.
    ///
    /// When this returns `true`, a TBF qdisc will be created in the qdisc chain.
    pub fn has_bandwidth_limit(&self) -> bool {
        self.bandwidth_mbit.is_some()
    }

    /// Compute the effective burst size in bytes.
    ///
    /// If `burst_kib` is set, converts it to bytes.
    /// Otherwise, computes a sensible default based on bandwidth.
    pub fn effective_burst_bytes(&self) -> u32 {
        if let Some(burst_kib) = self.burst_kib {
            // User specified burst in KiB, convert to bytes
            burst_kib * 1024
        } else if let Some(bandwidth_mbit) = self.bandwidth_mbit {
            // Compute default: max(bandwidth/8 seconds, 10 MTU packets)
            let bandwidth_bytes_per_sec = (bandwidth_mbit * 1_000_000.0 / 8.0) as u32;
            let one_eighth_second = bandwidth_bytes_per_sec / 8;
            let ten_packets = 1500 * 10; // 10 MTU-sized packets
            std::cmp::max(one_eighth_second, ten_packets)
        } else {
            // No bandwidth limit, burst is irrelevant
            0
        }
    }

    /// Compute the effective TBF queue limit in bytes.
    ///
    /// If `tbf_limit_bytes` is set, uses that value.
    /// Otherwise, defaults to `burst_bytes * 2`.
    pub fn effective_tbf_limit_bytes(&self) -> u32 {
        self.tbf_limit_bytes.unwrap_or_else(|| self.effective_burst_bytes() * 2)
    }

    /// Compute the bandwidth rate in bytes per second.
    ///
    /// Returns `None` if no bandwidth limit is configured.
    pub fn bandwidth_bytes_per_sec(&self) -> Option<u64> {
        self.bandwidth_mbit.map(|mbit| (mbit * 1_000_000.0 / 8.0) as u64)
    }
}

// -------------------------------------------------------------------------------------
// Netem Kernel Structure
// -------------------------------------------------------------------------------------

/// The kernel's `tc_netem_qopt` structure for netem configuration.
///
/// This struct must match the exact memory layout expected by the kernel.
/// Field order matters! The kernel expects these fields in this specific sequence.
///
/// # Kernel Definition
///
/// From `<linux/pkt_sched.h>`:
///
/// ```c
/// struct tc_netem_qopt {
///     __u32 latency;    /* Delay in scheduler ticks */
///     __u32 limit;      /* Queue size limit in packets */
///     __u32 loss;       /* Loss probability (0 to 2^32-1) */
///     __u32 gap;        /* Reordering gap */
///     __u32 duplicate;  /* Duplication probability */
///     __u32 jitter;     /* Jitter in scheduler ticks */
/// };
/// ```
///
/// # Probability Encoding
///
/// Loss and duplicate percentages are encoded as fractions of `u32::MAX`.
/// For example, 50% loss = `0.5 * u32::MAX ≈ 2147483648`.
#[derive(Debug, Clone, Copy)]
pub struct NetemQopt {
    /// Latency in packet scheduler ticks (not microseconds!).
    pub latency: u32,
    /// Maximum packets in queue.
    pub limit: u32,
    /// Loss probability, scaled to u32 range.
    pub loss: u32,
    /// Reordering gap.
    pub gap: u32,
    /// Duplication probability, scaled to u32 range.
    pub duplicate: u32,
    /// Jitter in packet scheduler ticks.
    pub jitter: u32,
}

impl NetemQopt {
    /// Convert a percentage (0-100) to the kernel's probability representation.
    ///
    /// The kernel expects probabilities as values from 0 to `u32::MAX`, where
    /// `u32::MAX` represents 100% probability.
    pub fn u32_probability(percent: f64) -> u32 {
        (percent / 100.0 * u32::MAX as f64) as u32
    }

    /// Serialize this structure to bytes for the netlink message.
    ///
    /// The bytes are in native endian order, matching the kernel's expectation
    /// for structures passed via netlink.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut vec = Vec::with_capacity(4 * 6);
        vec.extend_from_slice(&self.latency.to_ne_bytes());
        vec.extend_from_slice(&self.limit.to_ne_bytes());
        vec.extend_from_slice(&self.loss.to_ne_bytes());
        vec.extend_from_slice(&self.gap.to_ne_bytes());
        vec.extend_from_slice(&self.duplicate.to_ne_bytes());
        vec.extend_from_slice(&self.jitter.to_ne_bytes());
        vec
    }
}

impl From<&LinkImpairment> for NetemQopt {
    /// Convert user-friendly [`LinkImpairment`] to kernel-friendly [`NetemQopt`].
    ///
    /// This performs:
    /// - Microseconds → scheduler ticks conversion for latency and jitter
    /// - Percentage → u32 probability conversion for loss and duplicate
    fn from(value: &LinkImpairment) -> Self {
        Self {
            latency: usec_to_ticks_cached(value.latency),
            limit: value.netem_limit,
            loss: Self::u32_probability(value.loss),
            gap: value.gap,
            duplicate: Self::u32_probability(value.duplicate),
            jitter: usec_to_ticks_cached(value.jitter),
        }
    }
}

// For backwards compatibility, also support conversion from owned LinkImpairment
impl From<LinkImpairment> for NetemQopt {
    fn from(value: LinkImpairment) -> Self {
        Self::from(&value)
    }
}

/// The kernel's `tc_tbf_qopt` structure for Token Bucket Filter configuration.
///
/// TBF implements a simple rate limiter using the token bucket algorithm:
/// - Tokens accumulate at a fixed rate (the bandwidth limit)
/// - Each byte transmitted consumes one token
/// - Burst allows accumulating tokens up to a limit
/// - When tokens run out, packets wait or are dropped
///
/// # Kernel Definition
///
/// From `<linux/pkt_sched.h>`:
///
/// ```c
/// struct tc_tbf_qopt {
///     struct tc_ratespec rate;     /* Rate limit */
///     struct tc_ratespec peakrate; /* Peak rate (optional) */
///     __u32 limit;                 /* Max bytes in queue */
///     __u32 buffer;                /* Burst size in ticks */
///     __u32 mtu;                   /* Max transmission unit */
/// };
/// ```
#[derive(Debug, Clone, Copy)]
pub struct TbfQopt {
    /// The rate specification (bandwidth limit).
    pub rate: TcRateSpec,
    /// Peak rate specification (optional, set to zero for unused).
    pub peakrate: TcRateSpec,
    /// Maximum bytes that can queue waiting for tokens.
    pub limit: u32,
    /// Burst size, expressed in scheduler ticks.
    ///
    /// This is computed as: `burst_bytes * tick_in_usec / rate_bytes_per_usec`
    pub buffer: u32,
    /// Maximum transmission unit (typically 1500 for Ethernet).
    pub mtu: u32,
    /// Burst size in bytes (for TCA_TBF_BURST attribute).
    ///
    /// Modern kernels require this attribute in addition to the buffer field.
    pub burst_bytes: u32,
}

/// The kernel's `tc_ratespec` structure for rate specification.
///
/// # Kernel Definition
///
/// From `<linux/pkt_sched.h>`:
///
/// ```c
/// struct tc_ratespec {
///     unsigned char cell_log;    /* Cell size log2 */
///     __u8 linklayer;            /* Link layer type */
///     unsigned short overhead;   /* Link layer overhead */
///     short cell_align;          /* Cell alignment */
///     unsigned short mpu;        /* Minimum packet unit */
///     __u32 rate;                /* Rate in bytes/sec */
/// };
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct TcRateSpec {
    /// Log2 of the cell size. Set to 0 for byte-level granularity.
    pub cell_log: u8,
    /// Link layer type (0 = unaware, 1 = ethernet, 2 = ATM).
    pub linklayer: u8,
    /// Overhead added by link layer framing (bytes).
    pub overhead: u16,
    /// Cell alignment for ATM.
    pub cell_align: i16,
    /// Minimum packet unit (minimum bytes charged per packet).
    pub mpu: u16,
    /// Rate in bytes per second.
    pub rate: u32,
}

impl TcRateSpec {
    /// Create a rate spec for the given rate in bytes per second.
    ///
    /// This computes an appropriate `cell_log` value based on the MTU.
    /// The cell_log determines the granularity of the rate table.
    pub fn new(rate_bytes_per_sec: u32, mtu: u32) -> Self {
        // Compute cell_log: the smallest value such that (mtu >> cell_log) <= 255
        // This ensures we can represent all packet sizes in the 256-entry rate table.
        let cell_log = Self::compute_cell_log(mtu);

        Self {
            rate: rate_bytes_per_sec,
            // TC_LINKLAYER_UNAWARE (0) is iproute2's default. When set to 0, the kernel
            // uses the rate table (TCA_TBF_RTAB) directly for timing calculations.
            linklayer: 0,
            cell_log,
            cell_align: -1, // standard value from iproute2
            ..Default::default()
        }
    }

    /// Compute the cell_log value for a given MTU.
    ///
    /// The rate table has 256 entries. Entry `i` represents the time to transmit
    /// `(i + 1) << cell_log` bytes. We need `mtu >> cell_log <= 255` so that
    /// the MTU fits in the table.
    fn compute_cell_log(mtu: u32) -> u8 {
        let mut cell_log = 0u8;
        while (mtu >> cell_log) > 255 {
            cell_log += 1;
        }
        cell_log
    }

    /// Compute the rate table for this rate spec.
    ///
    /// The rate table is a 256-entry array where entry `i` contains the time
    /// (in scheduler ticks) to transmit `(i + 1) << cell_log` bytes at this rate.
    ///
    /// # Returns
    ///
    /// A 1024-byte vector (256 × 4-byte u32 entries) representing the rate table.
    pub fn compute_rate_table(&self) -> Vec<u8> {
        let tick_in_usec = *TICK_IN_USEC;
        let mut rtab = Vec::with_capacity(256 * 4);

        for i in 0u32..256 {
            // Size in bytes for this table entry
            let size = (i + 1) << self.cell_log;

            // Time to transmit this many bytes at the configured rate (in microseconds)
            // time_usec = size / rate * 1_000_000 = size * 1_000_000 / rate
            let time_usec = if self.rate > 0 {
                (size as f64 * 1_000_000.0 / self.rate as f64) as u32
            } else {
                0
            };

            // Convert to scheduler ticks
            let time_ticks = (time_usec as f64 * tick_in_usec) as u32;

            rtab.extend_from_slice(&time_ticks.to_ne_bytes());
        }

        rtab
    }

    /// Serialize to bytes in kernel format.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut vec = Vec::with_capacity(12);
        vec.push(self.cell_log);
        vec.push(self.linklayer);
        vec.extend_from_slice(&self.overhead.to_ne_bytes());
        vec.extend_from_slice(&self.cell_align.to_ne_bytes());
        vec.extend_from_slice(&self.mpu.to_ne_bytes());
        vec.extend_from_slice(&self.rate.to_ne_bytes());
        vec
    }
}

impl TbfQopt {
    /// Serialize this structure to bytes for the netlink message.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut vec = Vec::with_capacity(12 + 12 + 4 + 4 + 4); // 2 ratespecs + 3 u32s
        vec.extend_from_slice(&self.rate.to_bytes());
        vec.extend_from_slice(&self.peakrate.to_bytes());
        vec.extend_from_slice(&self.limit.to_ne_bytes());
        vec.extend_from_slice(&self.buffer.to_ne_bytes());
        vec.extend_from_slice(&self.mtu.to_ne_bytes());
        vec
    }
}

impl TbfQopt {
    /// Create a TBF configuration from a [`LinkImpairment`].
    ///
    /// This computes all the derived values needed for TBF:
    /// - Rate from `bandwidth_mbit`
    /// - Buffer (burst) in scheduler ticks
    /// - Queue limit
    ///
    /// # Panics
    ///
    /// Panics if `bandwidth_mbit` is `None`. Check [`LinkImpairment::has_bandwidth_limit`]
    /// before calling this.
    pub fn from_impairment(impairment: &LinkImpairment) -> Self {
        // FIXME: avoid this expect.
        let bandwidth_mbit =
            impairment.bandwidth_mbit.expect("TbfQopt requires bandwidth to be set");

        // Convert Mbit/s to bytes/sec
        let rate_bytes_per_sec = (bandwidth_mbit * 1_000_000.0 / 8.0) as u32;

        // Get burst in bytes
        let burst_bytes = impairment.effective_burst_bytes();

        // Convert burst from bytes to scheduler ticks.
        //
        // The formula is: buffer_ticks = burst_bytes / rate_bytes_per_tick
        // Where: rate_bytes_per_tick = rate_bytes_per_sec / ticks_per_sec
        //        ticks_per_sec = 1_000_000 * TICK_IN_USEC
        //
        // Simplified: buffer_ticks = burst_bytes * TICK_IN_USEC * 1_000_000 / rate_bytes_per_sec
        //
        // However, iproute2 uses a simpler formula that works in practice:
        // buffer_ticks = burst_bytes * TICK_IN_USEC / (rate_bytes_per_sec / 1_000_000)
        //              = burst_bytes * TICK_IN_USEC * 1_000_000 / rate_bytes_per_sec
        let tick_in_usec = *TICK_IN_USEC;
        let buffer_ticks =
            (burst_bytes as f64 * tick_in_usec * 1_000_000.0 / rate_bytes_per_sec as f64) as u32;

        // Queue limit in bytes
        let limit_bytes = impairment.effective_tbf_limit_bytes();

        let mtu = 1500u32; // Standard Ethernet MTU

        Self {
            rate: TcRateSpec::new(rate_bytes_per_sec, mtu),
            peakrate: TcRateSpec::default(), // No peak rate limiting
            limit: limit_bytes,
            buffer: buffer_ticks,
            mtu,
            burst_bytes,
        }
    }

    /// Get the rate table for this TBF configuration.
    ///
    /// The rate table is required by the kernel to properly compute
    /// transmission times for different packet sizes.
    pub fn rate_table(&self) -> Vec<u8> {
        self.rate.compute_rate_table()
    }
}

/// Global HTB qdisc options.
///
/// # Kernel Definition
///
/// From `<linux/pkt_sched.h>`:
///
/// ```c
/// struct tc_htb_glob {
///     __u32 version;      /* Must be 3 */
///     __u32 rate2quantum; /* Bytes to quantum conversion */
///     __u32 defcls;       /* Default class for unclassified traffic */
///     __u32 debug;        /* Debug flags */
///     __u32 direct_pkts;  /* Stats: packets sent directly */
/// };
/// ```
#[derive(Debug, Clone, Copy)]
pub struct HtbGlobOpt {
    /// HTB version (must be 3).
    pub version: u32,
    /// Conversion factor from rate to quantum.
    pub rate2quantum: u32,
    /// Default class ID for unclassified traffic.
    pub defcls: u32,
    /// Debug flags (usually 0).
    pub debug: u32,
    /// Direct packets counter (read-only, set to 0).
    pub direct_pkts: u32,
}

impl Default for HtbGlobOpt {
    fn default() -> Self {
        Self {
            version: 3,       // Required by kernel
            rate2quantum: 10, // Default conversion factor
            defcls: 1,        // Default to class 1:1
            debug: 0,
            direct_pkts: 0,
        }
    }
}

impl HtbGlobOpt {
    /// Serialize to bytes in kernel format.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut vec = Vec::with_capacity(size_of::<Self>());
        vec.extend_from_slice(&self.version.to_ne_bytes());
        vec.extend_from_slice(&self.rate2quantum.to_ne_bytes());
        vec.extend_from_slice(&self.defcls.to_ne_bytes());
        vec.extend_from_slice(&self.debug.to_ne_bytes());
        vec.extend_from_slice(&self.direct_pkts.to_ne_bytes());
        vec
    }
}

/// HTB class options.
///
/// # Kernel Definition
///
/// From `<linux/pkt_sched.h>`:
///
/// ```c
/// struct tc_htb_opt {
///     struct tc_ratespec rate;     /* Guaranteed rate */
///     struct tc_ratespec ceil;     /* Maximum rate (ceiling) */
///     __u32 buffer;                /* Token buffer size */
///     __u32 cbuffer;               /* Ceil token buffer size */
///     __u32 quantum;               /* Bytes per round in scheduler */
///     __u32 level;                 /* Class level (0 for leaves) */
///     __u32 prio;                  /* Priority */
/// };
/// ```
#[derive(Debug, Clone, Copy)]
pub struct HtbClassOpt {
    /// Guaranteed rate for this class.
    pub rate: TcRateSpec,
    /// Ceiling (maximum) rate this class can use.
    pub ceil: TcRateSpec,
    /// Token buffer size in ticks.
    pub buffer: u32,
    /// Ceiling token buffer size in ticks.
    pub cbuffer: u32,
    /// Quantum for round-robin scheduling.
    pub quantum: u32,
    /// Class level in hierarchy (0 for leaf classes).
    pub level: u32,
    /// Priority (lower = higher priority).
    pub prio: u32,
}

impl Default for HtbClassOpt {
    fn default() -> Self {
        // Compute buffer sizes for the default rate.
        // These represent how much data can burst at the rate/ceil.
        let tick_in_usec = *TICK_IN_USEC;
        let rate_bytes = HTB_DEFAULT_RATE_BYTES as u32;
        let mtu = 1500u32; // Standard Ethernet MTU

        // Buffer for 1ms worth of data at the rate
        let buffer_bytes = rate_bytes / 1000;
        let buffer_ticks =
            (buffer_bytes as f64 * tick_in_usec * 1_000_000.0 / rate_bytes as f64) as u32;

        Self {
            rate: TcRateSpec::new(rate_bytes, mtu),
            ceil: TcRateSpec::new(HTB_DEFAULT_CEIL_BYTES as u32, mtu),
            buffer: buffer_ticks.max(1),
            cbuffer: buffer_ticks.max(1),
            quantum: 60000, // Default quantum
            level: 0,       // Leaf class
            prio: 0,        // Highest priority
        }
    }
}

impl HtbClassOpt {
    /// Serialize to bytes in kernel format.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut vec = Vec::with_capacity(12 + 12 + 4 * 5); // 2 ratespecs + 5 u32s
        vec.extend_from_slice(&self.rate.to_bytes());
        vec.extend_from_slice(&self.ceil.to_bytes());
        vec.extend_from_slice(&self.buffer.to_ne_bytes());
        vec.extend_from_slice(&self.cbuffer.to_ne_bytes());
        vec.extend_from_slice(&self.quantum.to_ne_bytes());
        vec.extend_from_slice(&self.level.to_ne_bytes());
        vec.extend_from_slice(&self.prio.to_ne_bytes());
        vec
    }
}

// -------------------------------------------------------------------------------------
// Common Request Infrastructure
// -------------------------------------------------------------------------------------

/// Common fields shared by all qdisc/class/filter requests.
///
/// This struct captures the basic addressing information needed to target
/// a specific qdisc, class, or filter in the traffic control hierarchy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QdiscRequestInner {
    /// The network interface index (from `if_nametoindex`).
    pub interface_index: i32,
    /// The parent handle (where this qdisc/class attaches).
    pub parent: TcHandle,
    /// This qdisc/class's own handle.
    pub handle: TcHandle,
}

impl QdiscRequestInner {
    /// Create a new request for the given interface, defaulting to root parent.
    pub fn new(index: i32) -> Self {
        Self { interface_index: index, parent: TcHandle::ROOT, handle: TcHandle::default() }
    }

    /// Set the parent handle.
    pub fn with_parent(mut self, parent: TcHandle) -> Self {
        self.parent = parent;
        self
    }

    /// Set this qdisc/class's handle.
    pub fn with_handle(mut self, handle: TcHandle) -> Self {
        self.handle = handle;
        self
    }
}

// -------------------------------------------------------------------------------------
// IP Address Mask Utilities
// -------------------------------------------------------------------------------------

/// Compute an IPv4 netmask from a prefix length.
///
/// # Example
///
/// ```
/// assert_eq!(ipv4_mask(24), Ipv4Addr::new(255, 255, 255, 0));
/// assert_eq!(ipv4_mask(32), Ipv4Addr::new(255, 255, 255, 255));
/// ```
fn ipv4_mask(prefix_len: u8) -> Ipv4Addr {
    if prefix_len == 0 {
        return Ipv4Addr::new(0, 0, 0, 0);
    }
    let mask_u32 = u32::MAX << (32 - prefix_len);
    Ipv4Addr::from(mask_u32)
}

/// Compute an IPv6 netmask from a prefix length.
fn ipv6_mask(prefix_len: u8) -> Ipv6Addr {
    if prefix_len == 0 {
        return Ipv6Addr::from(0u128);
    }
    let mask_u128 = u128::MAX << (128 - prefix_len);
    Ipv6Addr::from(mask_u128)
}

// -------------------------------------------------------------------------------------
// Nested NLA Helper
// -------------------------------------------------------------------------------------

/// Build a nested NLA (Netlink Attribute) containing sub-attributes.
///
/// Netlink uses a TLV (Type-Length-Value) format. For qdisc options like HTB and TBF,
/// the `TCA_OPTIONS` attribute contains nested sub-attributes, each with their own type.
///
/// # NLA Header Format
///
/// ```text
/// ┌─────────────────────────────────────────┐
/// │  Length (2 bytes) │  Type (2 bytes)     │  <- NLA header (4 bytes)
/// ├─────────────────────────────────────────┤
/// │  Value (variable length, padded to 4)   │
/// └─────────────────────────────────────────┘
/// ```
///
/// # Arguments
///
/// * `nla_type` - The attribute type (e.g., `TCA_HTB_INIT`)
/// * `value` - The raw bytes of the attribute value
///
/// # Returns
///
/// A byte vector containing the complete NLA (header + value + padding).
fn build_nla(nla_type: u16, value: &[u8]) -> Vec<u8> {
    // NLA length includes the 4-byte header
    let nla_len = NLA_HEADER_SIZE + value.len();
    // Pad to 4-byte alignment
    let padded_len = (nla_len + 3) & !3;

    let mut buf = vec![0u8; padded_len];

    // Write length (2 bytes, native endian)
    buf[0..2].copy_from_slice(&(nla_len as u16).to_ne_bytes());
    // Write type (2 bytes, native endian)
    buf[2..4].copy_from_slice(&nla_type.to_ne_bytes());
    // Write value
    buf[NLA_HEADER_SIZE..NLA_HEADER_SIZE + value.len()].copy_from_slice(value);
    // Padding bytes are already zero from vec initialization

    buf
}

/// Build a nested TCA_OPTIONS attribute containing one or more sub-attributes.
///
/// This wraps sub-attributes in a `TCA_OPTIONS` container, which is what the kernel
/// expects for qdisc-specific configuration.
///
/// # Arguments
///
/// * `sub_attrs` - One or more NLA sub-attributes (already formatted with headers)
///
/// # Returns
///
/// A `DefaultNla` that can be added to a `TcMessage`.
fn build_nested_options(sub_attrs: Vec<u8>) -> DefaultNla {
    DefaultNla::new(TCA_OPTIONS, sub_attrs)
}

// -------------------------------------------------------------------------------------
// HTB Qdisc Request Builder
// -------------------------------------------------------------------------------------

/// Builder for creating an HTB root qdisc.
///
/// HTB (Hierarchical Token Bucket) is our root qdisc, chosen because it allows
/// an unlimited number of classes to be created dynamically. Each class can
/// have its own qdisc chain (TBF → netem) for per-destination impairments.
///
/// # Example
///
/// ```ignore
/// let request = QdiscHtbRequest::new(QdiscRequestInner::new(if_index))
///     .with_default_class(1)  // Unclassified traffic goes to class 1:1
///     .build();
/// ```
#[derive(Debug, Clone)]
pub struct QdiscHtbRequest {
    pub inner: QdiscRequestInner,
    pub options: HtbGlobOpt,
}

impl QdiscHtbRequest {
    /// Create a new HTB qdisc request for the given interface.
    ///
    /// The qdisc will be created at the root with handle 1:0.
    pub fn new(inner: QdiscRequestInner) -> Self {
        Self { inner, options: HtbGlobOpt::default() }
    }

    /// Set the default class for unclassified traffic.
    ///
    /// Traffic that doesn't match any filter will be sent to this class.
    /// The value is the minor number of the class (e.g., 1 for class 1:1).
    pub fn with_default_class(mut self, class_minor: u32) -> Self {
        self.options.defcls = class_minor;
        self
    }

    /// Build the netlink message to create this HTB qdisc.
    pub fn build(self) -> NetlinkMessage<RouteNetlinkMessage> {
        let mut tc_message = TcMessage::with_index(self.inner.interface_index);
        tc_message.header.parent = TcHandle::ROOT;
        tc_message.header.handle = TcHandle::from(0x0001_0000); // 1:0

        tc_message.attributes.push(TcAttribute::Kind("htb".to_string()));

        // HTB qdisc options must be wrapped in TCA_HTB_INIT inside TCA_OPTIONS.
        // The kernel expects: TCA_OPTIONS { TCA_HTB_INIT { tc_htb_glob } }
        let htb_init_nla = build_nla(TCA_HTB_INIT, &self.options.to_bytes());
        tc_message.attributes.push(TcAttribute::Other(build_nested_options(htb_init_nla)));

        let mut nl_req = NetlinkMessage::from(RouteNetlinkMessage::NewQueueDiscipline(tc_message));
        // NLM_F_REPLACE allows updating an existing qdisc
        nl_req.header.flags = NLM_F_CREATE | NLM_F_REPLACE | NLM_F_REQUEST | NLM_F_ACK;

        nl_req
    }
}

// -------------------------------------------------------------------------------------
// HTB Class Request Builder
// -------------------------------------------------------------------------------------

/// Builder for creating an HTB class.
///
/// Each destination peer gets its own HTB class, which serves as the attachment
/// point for the TBF and netem qdiscs that implement the actual impairments.
///
/// # Handle Scheme
///
/// For a destination peer with ID `N`:
/// - Class handle: `1:(10 + N)` (e.g., peer 2 → class 1:12)
///
/// # Example
///
/// ```ignore
/// // Create class for traffic to peer 2
/// let class_minor = CLASS_MINOR_OFFSET + 2; // 12
/// let request = HtbClassRequest::new(
///     QdiscRequestInner::new(if_index)
///         .with_parent(TcHandle::from(0x0001_0000))  // Parent is HTB root (1:0)
///         .with_handle(TcHandle::from((1 << 16) | class_minor as u32)), // 1:12
/// ).build();
/// ```
#[derive(Debug, Clone)]
pub struct HtbClassRequest {
    pub inner: QdiscRequestInner,
    pub options: HtbClassOpt,
    /// If true, replace an existing class instead of failing if it exists.
    pub replace: bool,
}

impl HtbClassRequest {
    /// Create a new HTB class request.
    pub fn new(inner: QdiscRequestInner) -> Self {
        Self { inner, options: HtbClassOpt::default(), replace: false }
    }

    /// Set whether to replace an existing class.
    ///
    /// When `true`, uses `NLM_F_REPLACE` to update an existing class.
    /// When `false` (default), uses `NLM_F_EXCL` to fail if the class exists.
    pub fn with_replace(mut self, replace: bool) -> Self {
        self.replace = replace;
        self
    }

    /// Build the netlink message to create this HTB class.
    pub fn build(self) -> NetlinkMessage<RouteNetlinkMessage> {
        let mut tc_message = TcMessage::with_index(self.inner.interface_index);
        tc_message.header.parent = self.inner.parent;
        tc_message.header.handle = self.inner.handle;

        tc_message.attributes.push(TcAttribute::Kind("htb".to_string()));

        // HTB class options must be wrapped in TCA_HTB_PARMS inside TCA_OPTIONS.
        // The kernel expects: TCA_OPTIONS { TCA_HTB_PARMS { tc_htb_opt } }
        let htb_parms_nla = build_nla(TCA_HTB_PARMS, &self.options.to_bytes());
        tc_message.attributes.push(TcAttribute::Other(build_nested_options(htb_parms_nla)));

        let mut nl_req = NetlinkMessage::from(RouteNetlinkMessage::NewTrafficClass(tc_message));
        nl_req.header.flags = if self.replace {
            NLM_F_CREATE | NLM_F_REPLACE | NLM_F_REQUEST | NLM_F_ACK
        } else {
            NLM_F_CREATE | NLM_F_EXCL | NLM_F_REQUEST | NLM_F_ACK
        };

        nl_req
    }
}

// -------------------------------------------------------------------------------------
// TBF Qdisc Request Builder
// -------------------------------------------------------------------------------------

/// Builder for creating a TBF (Token Bucket Filter) qdisc.
///
/// TBF provides rate limiting by implementing a token bucket algorithm.
/// It's inserted between the HTB class and netem when bandwidth limiting
/// is configured.
///
/// # How Token Bucket Works
///
/// 1. Tokens accumulate at the configured rate (e.g., 10 Mbit/s)
/// 2. The bucket can hold up to `burst` tokens
/// 3. Each byte transmitted consumes one token
/// 4. If tokens are available, packets transmit immediately
/// 5. If tokens are exhausted, packets queue (up to `limit` bytes)
/// 6. If the queue is full, packets are dropped
///
/// # Handle Scheme
///
/// For destination peer ID `N`:
/// - TBF handle: `(10 + N):1` (e.g., peer 2 → handle 12:1)
/// - Parent: HTB class `1:(10 + N)`
///
/// # Example
///
/// ```ignore
/// let impairment = LinkImpairment {
///     bandwidth_mbit: Some(100.0),  // 100 Mbit/s
///     burst_kib: Some(64),          // 64 KiB burst
///     ..Default::default()
/// };
///
/// let request = QdiscTbfRequest::new(
///     QdiscRequestInner::new(if_index)
///         .with_parent(TcHandle::from(0x0001_000C))  // Parent class 1:12
///         .with_handle(TcHandle::from(0x000C_0001)), // Handle 12:1
///     &impairment,
/// ).build();
/// ```
#[derive(Debug, Clone)]
pub struct QdiscTbfRequest {
    pub inner: QdiscRequestInner,
    pub options: TbfQopt,
    /// If true, replace an existing qdisc instead of failing if it exists.
    pub replace: bool,
}

impl QdiscTbfRequest {
    /// Create a new TBF qdisc request from a [`LinkImpairment`].
    ///
    /// # Panics
    ///
    /// Panics if `impairment.bandwidth_mbit` is `None`.
    pub fn new(inner: QdiscRequestInner, impairment: &LinkImpairment) -> Self {
        Self { inner, options: TbfQopt::from_impairment(impairment), replace: false }
    }

    /// Set whether to replace an existing qdisc.
    ///
    /// When `true`, uses `NLM_F_REPLACE` to update an existing qdisc.
    /// When `false` (default), uses `NLM_F_EXCL` to fail if the qdisc exists.
    pub fn with_replace(mut self, replace: bool) -> Self {
        self.replace = replace;
        self
    }

    /// Build the netlink message to create this TBF qdisc.
    pub fn build(self) -> NetlinkMessage<RouteNetlinkMessage> {
        let mut tc_message = TcMessage::with_index(self.inner.interface_index);
        tc_message.header.parent = self.inner.parent;
        tc_message.header.handle = self.inner.handle;

        tc_message.attributes.push(TcAttribute::Kind("tbf".to_string()));

        // TBF options must be wrapped in TCA_OPTIONS containing:
        // - TCA_TBF_PARMS: the tc_tbf_qopt structure
        // - TCA_TBF_RTAB: the rate table (256 x u32 entries)
        // - TCA_TBF_BURST: burst size in bytes (required by modern kernels)
        //
        // The rate table is required by the kernel to compute transmission times
        // for different packet sizes. Without it, the kernel returns EINVAL.
        let tbf_parms_nla = build_nla(TCA_TBF_PARMS, &self.options.to_bytes());
        let rate_table = self.options.rate_table();
        let tbf_rtab_nla = build_nla(TCA_TBF_RTAB, &rate_table);
        let tbf_burst_nla = build_nla(TCA_TBF_BURST, &self.options.burst_bytes.to_ne_bytes());

        // Combine all NLAs into a single TCA_OPTIONS
        let mut combined_nlas = tbf_parms_nla;
        combined_nlas.extend(tbf_rtab_nla);
        combined_nlas.extend(tbf_burst_nla);
        tc_message.attributes.push(TcAttribute::Other(build_nested_options(combined_nlas)));

        let mut nl_req = NetlinkMessage::from(RouteNetlinkMessage::NewQueueDiscipline(tc_message));
        nl_req.header.flags = if self.replace {
            NLM_F_CREATE | NLM_F_REPLACE | NLM_F_REQUEST | NLM_F_ACK
        } else {
            NLM_F_CREATE | NLM_F_EXCL | NLM_F_REQUEST | NLM_F_ACK
        };

        tracing::debug!(?nl_req, "sending tbf request");

        nl_req
    }
}

// -------------------------------------------------------------------------------------
// Netem Qdisc Request Builder
// -------------------------------------------------------------------------------------

/// Builder for creating a netem (Network Emulator) qdisc.
///
/// Netem is the workhorse of network simulation, providing:
/// - Latency (fixed delay)
/// - Jitter (random delay variation)
/// - Packet loss (random drops)
/// - Packet duplication
/// - Packet reordering
///
/// # Handle Scheme
///
/// For destination peer ID `N`:
/// - Netem handle: `(10 + N):0` (e.g., peer 2 → handle 12:0)
/// - Parent: TBF `(10 + N):1` if bandwidth limited, else HTB class `1:(10 + N)`
///
/// # Example
///
/// ```ignore
/// let impairment = LinkImpairment {
///     latency: 100_000,   // 100ms
///     jitter: 10_000,     // ±10ms
///     loss: 1.0,          // 1% packet loss
///     ..Default::default()
/// };
///
/// let request = QdiscNetemRequest::new(
///     QdiscRequestInner::new(if_index)
///         .with_parent(TcHandle::from(0x000C_0001))  // Parent TBF 12:1
///         .with_handle(TcHandle::from(0x000C_0000)), // Handle 12:0
///     &impairment,
/// ).build();
/// ```
#[derive(Debug)]
pub struct QdiscNetemRequest {
    pub inner: QdiscRequestInner,
    pub options: NetemQopt,
    /// If true, replace an existing qdisc instead of failing if it exists.
    pub replace: bool,
}

impl QdiscNetemRequest {
    /// Create a new netem qdisc request.
    pub fn new(inner: QdiscRequestInner, options: NetemQopt) -> Self {
        Self { inner, options, replace: false }
    }

    /// Create from a [`LinkImpairment`], converting to kernel format.
    pub fn from_impairment(inner: QdiscRequestInner, impairment: &LinkImpairment) -> Self {
        Self { inner, options: NetemQopt::from(impairment), replace: false }
    }

    /// Set whether to replace an existing qdisc.
    ///
    /// When `true`, uses `NLM_F_REPLACE` to update an existing qdisc.
    /// When `false` (default), uses `NLM_F_EXCL` to fail if the qdisc exists.
    pub fn with_replace(mut self, replace: bool) -> Self {
        self.replace = replace;
        self
    }

    /// Build the netlink message to create this netem qdisc.
    pub fn build(self) -> NetlinkMessage<RouteNetlinkMessage> {
        let mut tc_message = TcMessage::with_index(self.inner.interface_index);
        tc_message.header.parent = self.inner.parent;
        tc_message.header.handle = self.inner.handle;

        tc_message.attributes.push(TcAttribute::Kind("netem".to_string()));
        tc_message
            .attributes
            .push(TcAttribute::Other(DefaultNla::new(TCA_OPTIONS, self.options.to_bytes())));

        let mut nl_req = NetlinkMessage::from(RouteNetlinkMessage::NewQueueDiscipline(tc_message));
        nl_req.header.flags = if self.replace {
            NLM_F_CREATE | NLM_F_REPLACE | NLM_F_REQUEST | NLM_F_ACK
        } else {
            NLM_F_CREATE | NLM_F_EXCL | NLM_F_REQUEST | NLM_F_ACK
        };

        nl_req
    }
}

// -------------------------------------------------------------------------------------
// Flower Filter Request Builder
// -------------------------------------------------------------------------------------

/// Builder for creating a flower filter.
///
/// Flower filters classify packets based on various criteria. We use them to
/// match packets by destination IP address and route them to the appropriate
/// HTB class for impairment.
///
/// # How Classification Works
///
/// 1. Packet enters HTB root qdisc
/// 2. Flower filter examines destination IP
/// 3. If IP matches → packet goes to the specified class (e.g., 1:12)
/// 4. If no match → packet goes to default class (1:1)
///
/// # Example
///
/// ```ignore
/// // Route traffic to 10.0.0.2 into class 1:12
/// let request = FlowerFilterRequest::new(
///     QdiscRequestInner::new(if_index)
///         .with_parent(TcHandle::from(0x0001_0000)), // Attach to HTB root
///     IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
/// )
/// .with_class_id(0x0001_000C)  // Route to class 1:12
/// .build();
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlowerFilterRequest {
    pub inner: QdiscRequestInner,
    /// The destination IP address to match.
    pub destination: IpAddr,
    /// The netmask prefix length (e.g., 32 for exact match).
    pub mask: u8,
    /// The class ID to route matching traffic to.
    pub class_id: u32,
}

impl FlowerFilterRequest {
    /// Create a new flower filter for the given destination IP.
    ///
    /// By default, uses /32 (exact match) for IPv4 or /128 for IPv6.
    pub fn new(inner: QdiscRequestInner, destination: IpAddr) -> Self {
        let default_mask = match destination {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        };
        Self {
            inner,
            destination,
            mask: default_mask,
            // Default class ID will be set by caller
            class_id: 0,
        }
    }

    /// Set the netmask prefix length.
    ///
    /// Use this to match a range of IPs (e.g., /24 for a subnet).
    pub fn with_prefix(mut self, prefix: u8) -> Self {
        self.mask = prefix;
        self
    }

    /// Set the class ID to route matching traffic to.
    ///
    /// The class ID is a 32-bit value combining major:minor (e.g., 0x0001_000C for 1:12).
    pub fn with_class_id(mut self, class_id: u32) -> Self {
        self.class_id = class_id;
        self
    }

    /// Build the netlink message to create this flower filter.
    pub fn build(self) -> NetlinkMessage<RouteNetlinkMessage> {
        // Determine EtherType and build IP-specific match options
        let (proto_ethertype, match_opts): (u16, Vec<TcOption>) = match self.destination {
            IpAddr::V4(v4) => {
                let mask = ipv4_mask(self.mask);
                (
                    ETH_P_IP,
                    vec![
                        TcOption::Flower(TcFilterFlowerOption::Ipv4Dst(v4)),
                        TcOption::Flower(TcFilterFlowerOption::Ipv4DstMask(mask)),
                    ],
                )
            }
            IpAddr::V6(v6) => {
                let mask = ipv6_mask(self.mask);
                (
                    ETH_P_IPV6,
                    vec![
                        TcOption::Flower(TcFilterFlowerOption::Ipv6Dst(v6)),
                        TcOption::Flower(TcFilterFlowerOption::Ipv6DstMask(mask)),
                    ],
                )
            }
        };

        let mut tc_msg = TcMessage::with_index(self.inner.interface_index);
        tc_msg.header.parent = self.inner.parent;
        // Let kernel auto-assign filter handle
        tc_msg.header.handle = TcHandle::from(0u32);
        // Protocol in network byte order in the info field
        tc_msg.header.info = proto_ethertype.to_be() as u32;

        tc_msg.attributes.push(TcAttribute::Kind("flower".to_string()));

        // Build flower options:
        // - ClassId: where to send matching packets
        // - Flags: usually 0
        // - EthType: what protocol we're matching
        // - IP destination + mask: the actual match criteria
        let opts: Vec<TcOption> = [
            vec![
                TcOption::Flower(TcFilterFlowerOption::ClassId(self.class_id)),
                TcOption::Flower(TcFilterFlowerOption::Flags(0)),
                TcOption::Flower(TcFilterFlowerOption::EthType(proto_ethertype)),
            ],
            match_opts,
        ]
        .concat();

        tc_msg.attributes.push(TcAttribute::Options(opts));

        let mut nl_req = NetlinkMessage::from(RouteNetlinkMessage::NewTrafficFilter(tc_msg));
        nl_req.header.flags = NLM_F_REQUEST | NLM_F_ACK | NLM_F_CREATE | NLM_F_EXCL;

        nl_req
    }
}

/// Compute the HTB class handle for a destination peer.
///
/// # Handle Format
///
/// Returns `1:(10 + peer_id)` as a 32-bit handle.
///
/// # Example
///
/// ```
/// assert_eq!(htb_class_handle(2), 0x0001_000C); // 1:12
/// ```
pub fn htb_class_handle(dest_peer_id: usize) -> u32 {
    let minor = CLASS_MINOR_OFFSET + dest_peer_id as u32;
    (1 << 16) | minor
}

/// Compute the TBF qdisc handle for a destination peer.
///
/// # Handle Format
///
/// Returns `(10 + peer_id):0` as a 32-bit handle.
/// Qdisc handles must have minor=0 (kernel rejects non-zero minor).
///
/// # Example
///
/// ```
/// assert_eq!(tbf_handle(2), 0x000C_0000); // 12:0
/// ```
pub fn tbf_handle(dest_peer_id: usize) -> u32 {
    let major = CLASS_MINOR_OFFSET + dest_peer_id as u32;
    major << 16 // minor must be 0 for qdiscs
}

/// Offset for netem qdisc major numbers (separate from TBF to avoid collisions).
pub const NETEM_MAJOR_OFFSET: u32 = 20;

/// Compute the netem qdisc handle for a destination peer.
///
/// # Handle Format
///
/// Returns `(20 + peer_id):0` as a 32-bit handle.
/// Uses a different major offset than TBF to avoid handle collisions.
/// Qdisc handles must have minor=0 (kernel rejects non-zero minor).
///
/// # Example
///
/// ```
/// assert_eq!(netem_handle(2), 0x0016_0000); // 22:0
/// ```
pub fn netem_handle(dest_peer_id: usize) -> u32 {
    let major = NETEM_MAJOR_OFFSET + dest_peer_id as u32;
    major << 16 // minor must be 0 for qdiscs
}
