//! Token Bucket Filter (TBF) qdisc support.
//!
//! TBF provides rate limiting by implementing a token bucket algorithm.
//! It's inserted between the DRR class and netem when bandwidth limiting is configured.

use rtnetlink::packet_core::{
    NLM_F_ACK, NLM_F_CREATE, NLM_F_EXCL, NLM_F_REPLACE, NLM_F_REQUEST, NetlinkMessage,
};
use rtnetlink::packet_route::{
    RouteNetlinkMessage,
    tc::{TcAttribute, TcMessage},
};

use super::core::{MTU_ETHERNET, TICK_IN_USEC};
use super::handle::QdiscRequestInner;
use super::impairment::LinkImpairment;
use super::nla::{build_nested_options, build_nla};

/// Default rate table for TBF (256 × 4-byte zero entries = 1024 bytes).
///
/// Modern kernels don't use the rate table for actual rate calculations. Instead, they use
/// precomputed `mult`/`shift` values in `psched_ratecfg` (see `psched_l2t_ns()` in the kernel).
///
/// The rate table is only used for **linklayer auto-detection** when `linklayer == 0`
/// (TC_LINKLAYER_UNAWARE). The kernel's `__detect_linklayer()` function checks:
/// 1. If `rate > 100 Mbit/s` or `rtab[0] == 0` → returns `TC_LINKLAYER_ETHERNET`
/// 2. Otherwise, looks for ATM-specific patterns in the table
///
/// Since `rtab[0] == 0` in this default table, the kernel immediately classifies the link
/// as Ethernet without further inspection. This is safe and avoids unnecessary per-rate
/// table computation.
const DEFAULT_RATE_TABLE: [u8; 1024] = [0u8; 1024];

// TBF-specific TCA_OPTIONS sub-attributes (from linux/pkt_sched.h)
/// TBF parameters attribute type.
const TCA_TBF_PARMS: u16 = 1;
/// TBF rate table attribute type.
const TCA_TBF_RTAB: u16 = 2;
/// TBF burst size in bytes (required by modern kernels).
const TCA_TBF_BURST: u16 = 6;

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
    /// Peak rate specification (optional, set to zero/[`Default::default`] for unused).
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
/// # Rate Table: Historical Context
///
/// Historically, the kernel used a **rate table** to look up transmission times for packets.
/// However, modern kernels (3.x+) use precomputed `mult`/`shift` reciprocals in `psched_ratecfg`
/// for actual rate calculations (see `psched_l2t_ns()` in `include/net/sch_generic.h`).
///
/// The rate table is now only used for **linklayer auto-detection** via `__detect_linklayer()`:
/// - If `rate > 100 Mbit/s` or `rtab[0] == 0` → Ethernet (no table inspection)
/// - Otherwise, checks for ATM-specific patterns (equal times for different cell-boundary sizes)
///
/// We use a zeroed rate table which guarantees Ethernet detection via the
/// `rtab[0] == 0` fast path.
///
/// # Linklayer Modes
///
/// - `linklayer = 0` (TC_LINKLAYER_UNAWARE): Kernel auto-detects using the rate table.
///   We use this with a zeroed table to let the kernel handle detection robustly.
/// - `linklayer = 1` (TC_LINKLAYER_ETHERNET): Explicit Ethernet, skips detection.
/// - `linklayer = 2` (TC_LINKLAYER_ATM): Explicit ATM with 48-byte cell accounting.
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
    /// Log2 of the cell size (historical, kept for kernel struct compatibility).
    ///
    /// This field is part of the kernel's `tc_ratespec` structure but is not used
    /// for rate calculations in modern kernels. We set it based on MTU for compatibility.
    pub cell_log: u8,
    /// Link layer type (0 = unaware, 1 = ethernet, 2 = ATM).
    ///
    /// When 0 (unaware), the kernel auto-detects using the rate table.
    /// We use this to let the kernel handle detection robustly.
    pub linklayer: u8,
    /// Overhead added by link layer framing (bytes).
    pub overhead: u16,
    /// Cell alignment for ATM networks (typically -1 for Ethernet).
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
        let cell_log = Self::compute_cell_log(mtu);

        Self {
            rate: rate_bytes_per_sec,
            // TC_LINKLAYER_UNAWARE (0) is iproute2's default. When set to 0, the kernel
            // uses the rate table (TCA_TBF_RTAB) directly for timing calculations.
            linklayer: 0,
            cell_log,
            cell_align: -1, // Standard value from iproute2
            ..Default::default()
        }
    }

    /// Compute the cell_log value for a given MTU.
    ///
    /// This is kept for kernel struct compatibility. The value ensures `mtu >> cell_log <= 255`.
    /// For Ethernet (MTU 1500), this returns 3 because `1500 >> 3 = 187 < 256`.
    pub fn compute_cell_log(mtu: u32) -> u8 {
        let mut cell_log = 0u8;
        while (mtu >> cell_log) > 255 {
            cell_log += 1;
        }
        cell_log
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

    /// Create a TBF configuration from a [`LinkImpairment`].
    ///
    /// This computes all the derived values needed for TBF:
    /// - Rate from `bandwidth_mbit_s`
    /// - Buffer (burst) in scheduler ticks
    /// - Queue limit
    ///
    /// Returns `None` if `bandwidth_mbit_s` is not set.
    pub fn try_from_impairment(impairment: &LinkImpairment) -> Option<Self> {
        let bandwidth_mbit = impairment.bandwidth_mbit_s?;

        // Convert Mbit/s to bytes/sec
        let rate_bytes_per_sec = (bandwidth_mbit * 1_000_000.0 / 8.0) as u32;

        // Get burst in bytes
        let burst_bytes = impairment.effective_burst_bytes();

        // Convert burst from bytes to scheduler ticks.
        //
        // The formula is: buffer_ticks = burst_bytes / rate_bytes_per_tick
        // Where:
        //  * rate_bytes_per_tick = rate_bytes_per_sec / ticks_per_sec,
        //  * ticks_per_sec = 1_000_000 * TICK_IN_USEC.
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

        Some(Self {
            rate: TcRateSpec::new(rate_bytes_per_sec, MTU_ETHERNET),
            peakrate: TcRateSpec::default(), // No peak rate limiting
            limit: limit_bytes,
            buffer: buffer_ticks,
            mtu: MTU_ETHERNET,
            burst_bytes,
        })
    }
}

/// Builder for creating a TBF (Token Bucket Filter) qdisc.
///
/// TBF provides rate limiting by implementing a token bucket algorithm.
/// It's inserted between the DRR class and netem when bandwidth limiting
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
/// - TBF handle: `(10 + N):0` (e.g., peer 2 → handle 12:0)
/// - Parent: DRR class `1:(10 + N)`
///
/// # Example
///
/// ```
/// use linkem::tc::impairment::LinkImpairment;
/// use linkem::tc::handle::QdiscRequestInner;
/// use linkem::tc::tbf::QdiscTbfRequest;
/// use rtnetlink::packet_route::tc::TcHandle;
///
/// let if_index = 1; // Network interface index
/// let impairment = LinkImpairment {
///     bandwidth_mbit_s: Some(100.0),  // 100 Mbit/s
///     burst_kib: Some(64),            // 64 KiB burst
///     ..Default::default()
/// };
///
/// let request = QdiscTbfRequest::try_new(
///     QdiscRequestInner::new(if_index)
///         .with_parent(TcHandle::from(0x0001_000C))  // Parent class 1:12
///         .with_handle(TcHandle::from(0x000C_0000)), // Handle 12:0
///     &impairment,
/// ).expect("bandwidth_mbit_s is set").build();
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
    /// Returns `None` if `impairment.bandwidth_mbit_s` is not set.
    pub fn try_new(inner: QdiscRequestInner, impairment: &LinkImpairment) -> Option<Self> {
        Some(Self { inner, options: TbfQopt::try_from_impairment(impairment)?, replace: false })
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
        // We use DEFAULT_RATE_TABLE (all zeros) because modern kernels don't use the rate
        // table for actual rate calculations—they use precomputed mult/shift values instead.
        // The zeroed table triggers the kernel's `rtab[0] == 0` fast path in __detect_linklayer(),
        // which returns TC_LINKLAYER_ETHERNET immediately. See DEFAULT_RATE_TABLE docs.
        let tbf_parms_nla = build_nla(TCA_TBF_PARMS, &self.options.to_bytes());
        let tbf_rtab_nla = build_nla(TCA_TBF_RTAB, &DEFAULT_RATE_TABLE);
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
