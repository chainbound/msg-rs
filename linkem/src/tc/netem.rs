//! Netem (Network Emulator) qdisc support.
//!
//! Netem is the workhorse of network simulation, providing latency, jitter,
//! packet loss, duplication, and reordering.

use nix::libc::TCA_OPTIONS;
use rtnetlink::packet_core::{DefaultNla, NetlinkMessage, NLM_F_ACK, NLM_F_CREATE, NLM_F_EXCL, NLM_F_REPLACE, NLM_F_REQUEST};
use rtnetlink::packet_route::{RouteNetlinkMessage, tc::{TcAttribute, TcMessage}};

use super::core::usec_to_ticks;
use super::handle::QdiscRequestInner;
use super::impairment::LinkImpairment;

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
    /// The kernel expects probabilities as values from 0 to [`u32::MAX`], where
    /// [`u32::MAX`] represents 100% probability.
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
    fn from(value: &LinkImpairment) -> Self {
        Self {
            latency: usec_to_ticks(value.latency),
            limit: value.netem_limit,
            loss: Self::u32_probability(value.loss),
            gap: value.gap,
            duplicate: Self::u32_probability(value.duplicate),
            jitter: usec_to_ticks(value.jitter),
        }
    }
}

// For backwards compatibility, also support conversion from owned LinkImpairment
impl From<LinkImpairment> for NetemQopt {
    fn from(value: LinkImpairment) -> Self {
        Self::from(&value)
    }
}

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
/// - Netem handle: `(20 + N):0` (e.g., peer 2 → handle 22:0)
/// - Parent: TBF `(10 + N):0` if bandwidth limited, else HTB class `1:(10 + N)`
///
/// # Example
///
/// ```
/// use linkem::tc::impairment::LinkImpairment;
/// use linkem::tc::netem::QdiscNetemRequest;
/// use linkem::tc::handle::QdiscRequestInner;
/// use rtnetlink::packet_route::tc::TcHandle;
///
/// let if_index = 1; // Network interface index
/// let impairment = LinkImpairment {
///     latency: 100_000,   // 100ms
///     jitter: 10_000,     // ±10ms
///     loss: 1.0,          // 1% packet loss
///     ..Default::default()
/// };
///
/// let request = QdiscNetemRequest::from_impairment(
///     QdiscRequestInner::new(if_index)
///         .with_parent(TcHandle::from(0x000C_0000))  // Parent TBF 12:0
///         .with_handle(TcHandle::from(0x0016_0000)), // Handle 22:0
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
