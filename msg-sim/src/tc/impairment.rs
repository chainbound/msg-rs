//! Link impairment configuration.
//!
//! This module defines the user-facing configuration for network impairments
//! that can be applied to links between peers.

use super::core::MTU_ETHERNET;

/// Configuration for network impairments to apply to a link.
///
/// This struct represents all the ways you can degrade a network link for testing
/// purposes. Each field maps to a feature of Linux's `netem` qdisc or `tbf` qdisc.
///
/// # Example
///
/// ```
/// use msg_sim::tc::impairment::LinkImpairment;
///
/// // Using method chaining (recommended)
/// let satellite_link = LinkImpairment::default()
///     .with_latency_ms(300)       // 300ms one-way delay
///     .with_jitter_ms(50)         // ±50ms variation
///     .with_loss(1.0)             // 1% packet loss
///     .with_bandwidth_mbit_s(10.0); // 10 Mbit/s bandwidth cap
///
/// // Using struct literal syntax
/// let flaky_lan = LinkImpairment {
///     latency: 1_000,             // 1ms base latency
///     duplicate: 0.1,             // 0.1% duplicate packets
///     ..Default::default()
/// };
/// ```
///
/// # Bandwidth Limiting
///
/// When `bandwidth_mbit_s` is set, a Token Bucket Filter (TBF) qdisc is inserted
/// before netem in the qdisc chain. TBF works by:
///
/// 1. Tokens accumulate at `bandwidth_mbit_s` rate
/// 2. Each byte transmitted consumes one token
/// 3. Burst allows temporary excess up to `burst_kib` bytes
/// 4. When tokens are exhausted, packets queue (up to `tbf_limit_bytes`)
/// 5. If the queue overflows, packets are dropped
///
/// The hierarchy becomes: `DRR class -> TBF -> netem` instead of `DRR class -> netem`.
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
    ///
    /// # Linux Kernel Limitation
    ///
    /// **Important:** Once a netem qdisc with `duplicate > 0` exists on a network
    /// interface, the Linux kernel prevents creating additional netem qdiscs on
    /// that same interface. This means you can only use packet duplication on
    /// **at most one outgoing link per peer**.
    ///
    /// For example, if peer A has links to peers B, C, and D:
    /// - You CAN set `duplicate > 0` on the A→B link
    /// - You CANNOT also set impairments on A→C or A→D (even without duplicate)
    ///
    /// If you need multiple outgoing links from the same peer, either:
    /// - Use `duplicate` on only one of them, OR
    /// - Don't use `duplicate` at all on links from that peer
    ///
    /// This is enforced by the [`check_netem_in_tree()`][kernel] function in the
    /// Linux kernel (`net/sched/sch_netem.c`), which returns:
    /// > "netem: cannot mix duplicating netems with other netems in tree"
    ///
    /// [kernel]: https://github.com/torvalds/linux/blob/master/net/sched/sch_netem.c
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
    pub bandwidth_mbit_s: Option<f64>,

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

    /// Maximum queuing latency in milliseconds for the TBF queue.
    ///
    /// This controls how long packets can wait in the TBF queue before being dropped.
    /// It's used to calculate the queue size limit: `limit = rate x latency + burst`.
    ///
    /// This models **congestion-induced delay**, which is different from the `latency`
    /// field (netem delay):
    /// - Netem `latency`: constant delay added to every packet (propagation delay)
    /// - TBF `tbf_queue_latency_ms`: variable delay from 0 up to this value depending
    ///   on queue fill level, after which packets are dropped (congestion queuing)
    ///
    /// For example, with `bandwidth_mbit_s = 10.0` and `tbf_queue_latency_ms = 100`:
    /// - Queue limit = (10 Mbit/s x 100ms) + burst ~= 125KB + burst
    /// - Under light load: packets pass through with ~0ms queue delay
    /// - Under heavy load: queue fills, delay approaches 100ms, then drops occur
    ///
    /// When `None`, defaults to 200ms.
    pub tbf_queue_latency_ms: Option<u32>,
}

impl Default for LinkImpairment {
    fn default() -> Self {
        Self {
            // Netem defaults: no impairment
            latency: 0,
            netem_limit: 1_000,
            loss: 0.0,
            gap: 0,
            duplicate: 0.0,
            jitter: 0,

            // TBF defaults: no bandwidth limiting
            bandwidth_mbit_s: None,
            burst_kib: None,
            tbf_queue_latency_ms: None,
        }
    }
}

impl LinkImpairment {
    /// Sets the base latency in microseconds.
    pub fn with_latency(mut self, microseconds: u32) -> Self {
        self.latency = microseconds;
        self
    }

    /// Sets the base latency in milliseconds.
    pub fn with_latency_ms(mut self, milliseconds: u32) -> Self {
        self.latency = milliseconds * 1_000;
        self
    }

    /// Sets the jitter (latency variation) in microseconds.
    pub fn with_jitter(mut self, microseconds: u32) -> Self {
        self.jitter = microseconds;
        self
    }

    /// Sets the jitter (latency variation) in milliseconds.
    pub fn with_jitter_ms(mut self, milliseconds: u32) -> Self {
        self.jitter = milliseconds * 1_000;
        self
    }

    /// Sets the packet loss percentage (0.0 to 100.0).
    pub fn with_loss(mut self, percent: f64) -> Self {
        self.loss = percent;
        self
    }

    /// Sets the packet duplication percentage (0.0 to 100.0).
    ///
    /// See [`LinkImpairment::duplicate`] for important kernel limitations.
    pub fn with_duplicate(mut self, percent: f64) -> Self {
        self.duplicate = percent;
        self
    }

    /// Sets the packet reordering gap.
    pub fn with_gap(mut self, gap: u32) -> Self {
        self.gap = gap;
        self
    }

    /// Sets the maximum packets in the netem queue.
    pub fn with_netem_limit(mut self, limit: u32) -> Self {
        self.netem_limit = limit;
        self
    }

    /// Sets the bandwidth limit in megabits per second.
    pub fn with_bandwidth_mbit_s(mut self, mbit_s: f64) -> Self {
        self.bandwidth_mbit_s = Some(mbit_s);
        self
    }

    /// Sets the TBF burst size in kibibytes.
    pub fn with_burst_kib(mut self, kib: u32) -> Self {
        self.burst_kib = Some(kib);
        self
    }

    /// Sets the TBF queue latency in milliseconds.
    pub fn with_tbf_queue_latency_ms(mut self, ms: u32) -> Self {
        self.tbf_queue_latency_ms = Some(ms);
        self
    }

    /// Returns `true` if bandwidth limiting is configured.
    ///
    /// When this returns `true`, a TBF qdisc will be created in the qdisc chain.
    pub fn has_bandwidth_limit(&self) -> bool {
        self.bandwidth_mbit_s.is_some()
    }

    /// Compute the effective burst size in bytes.
    ///
    /// If `burst_kib` is set, converts it to bytes.
    /// Otherwise, computes a sensible default based on bandwidth.
    pub fn effective_burst_bytes(&self) -> u32 {
        if let Some(burst_kib) = self.burst_kib {
            burst_kib * 1024
        } else if let Some(bandwidth_mbit) = self.bandwidth_mbit_s {
            // Compute default: max(bandwidth/8 seconds, 10 MTU packets)
            let bandwidth_bytes_per_sec = (bandwidth_mbit * 1_000_000.0 / 8.0) as u32;
            let one_eighth_second = bandwidth_bytes_per_sec / 8;
            let ten_packets = MTU_ETHERNET * 10; // 10 MTU-sized packets
            std::cmp::max(one_eighth_second, ten_packets)
        } else {
            // No bandwidth limit, burst is irrelevant
            0
        }
    }

    /// Compute the effective TBF queue limit in bytes.
    ///
    /// Uses the formula: `limit = rate x queue_latency + burst`, from iproute2.
    ///
    /// This determines how many bytes can queue in TBF before drops occur.
    /// The queue latency defaults to 200ms if not specified.
    pub fn effective_tbf_limit_bytes(&self) -> u32 {
        let queue_latency_ms = self.tbf_queue_latency_ms.unwrap_or(200);
        let burst_bytes = self.effective_burst_bytes();

        if let Some(bandwidth_mbit) = self.bandwidth_mbit_s {
            let rate_bytes_per_sec = (bandwidth_mbit * 1_000_000.0 / 8.0) as u32;
            let rate_bytes_per_ms = rate_bytes_per_sec / 1000;
            rate_bytes_per_ms * queue_latency_ms + burst_bytes
        } else {
            // No bandwidth limit, return a reasonable default
            burst_bytes
        }
    }

    /// Compute the bandwidth rate in bytes per second.
    ///
    /// Returns `None` if no bandwidth limit is configured.
    pub fn bandwidth_bytes_per_sec(&self) -> Option<u64> {
        self.bandwidth_mbit_s.map(|mbit| (mbit * 1_000_000.0 / 8.0) as u64)
    }
}
