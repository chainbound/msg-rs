//! HTB (Hierarchical Token Bucket) qdisc and class support.
//!
//! HTB replaces DRR as the root classifier because it correctly handles
//! non-work-conserving child qdiscs (like netem). When netem returns NULL
//! during dequeue (delay hasn't elapsed), HTB moves to the next class via
//! `htb_next_rb_node()`, preventing head-of-line blocking that occurs with DRR.

use rtnetlink::packet_core::{
    NLM_F_ACK, NLM_F_CREATE, NLM_F_EXCL, NLM_F_REPLACE, NLM_F_REQUEST, NetlinkMessage,
};
use rtnetlink::packet_route::{
    RouteNetlinkMessage,
    tc::{TcAttribute, TcHandle, TcMessage},
};

use super::core::TICK_IN_USEC;
use super::handle::QdiscRequestInner;
use super::nla::{build_nested_options, build_nla};
use super::tbf::TcRateSpec;

// HTB-specific TCA_OPTIONS sub-attributes (from linux/pkt_sched.h)
/// HTB class parameters attribute type.
const TCA_HTB_PARMS: u16 = 1;
/// HTB qdisc initialization attribute type.
const TCA_HTB_INIT: u16 = 2;
/// HTB ceil rate table attribute type.
const TCA_HTB_CTAB: u16 = 3;
/// HTB rate table attribute type.
const TCA_HTB_RTAB: u16 = 4;

/// HTB protocol version (current kernel version).
const HTB_VERSION: u32 = 3;

/// Default rate-to-quantum conversion factor.
///
/// Controls how HTB converts a class's rate to its quantum for deficit
/// round-robin among same-priority classes. The kernel computes:
/// `quantum = rate / rate2quantum`.
const HTB_RATE2QUANTUM: u32 = 10;

/// Default class minor number for unclassified traffic.
///
/// HTB's `defcls` parameter routes packets that don't match any filter
/// to this class (1:1), which has no impairments.
const HTB_DEFAULT_CLASS: u32 = 1;

/// Effectively unlimited rate in bytes per second (~10 Gbit/s).
///
/// Since we use HTB purely for classification (not rate limiting),
/// we set rate and ceil to ~10 Gbit/s which is effectively unlimited on veth.
/// This value (1,250,000,000) fits in u32, so `TCA_HTB_RATE64` is not needed.
const HTB_UNLIMITED_RATE_BPS: u32 = 1_250_000_000;

/// Default burst size in bytes (1 MiB).
///
/// Used for buffer/cbuffer tick calculations. Since we're not actually
/// rate-limiting at the HTB level, this just needs to be large enough
/// to avoid any token bucket starvation.
const HTB_DEFAULT_BURST_BYTES: u32 = 1024 * 1024;

/// Default rate table (256 x 4-byte zero entries = 1024 bytes).
///
/// Modern kernels compute rates internally. The zeroed table triggers
/// the kernel's `rtab[0] == 0` fast path in `__detect_linklayer()`,
/// returning `TC_LINKLAYER_ETHERNET` immediately.
const DEFAULT_RATE_TABLE: [u8; 1024] = [0u8; 1024];

/// The kernel's `tc_htb_glob` structure for HTB qdisc initialization.
///
/// Passed inside `TCA_OPTIONS` -> `TCA_HTB_INIT` when creating the root qdisc.
///
/// # Kernel Definition
///
/// From `<linux/pkt_sched.h>`:
///
/// ```c
/// struct tc_htb_glob {
///     __u32 version;        /* HTB version */
///     __u32 rate2quantum;   /* Rate-to-quantum conversion */
///     __u32 defcls;         /* Default class minor number */
///     __u32 debug;          /* Debug flags */
///     __u32 direct_pkts;    /* Stats: packets sent directly (read-only) */
/// };
/// ```
#[derive(Debug, Clone, Copy)]
struct HtbGlob {
    version: u32,
    rate2quantum: u32,
    defcls: u32,
    debug: u32,
    direct_pkts: u32,
}

impl HtbGlob {
    fn as_bytes(self) -> Vec<u8> {
        let mut vec = Vec::with_capacity(20);
        vec.extend_from_slice(&self.version.to_ne_bytes());
        vec.extend_from_slice(&self.rate2quantum.to_ne_bytes());
        vec.extend_from_slice(&self.defcls.to_ne_bytes());
        vec.extend_from_slice(&self.debug.to_ne_bytes());
        vec.extend_from_slice(&self.direct_pkts.to_ne_bytes());
        vec
    }
}

impl Default for HtbGlob {
    fn default() -> Self {
        Self {
            version: HTB_VERSION,
            rate2quantum: HTB_RATE2QUANTUM,
            defcls: HTB_DEFAULT_CLASS,
            debug: 0,
            direct_pkts: 0,
        }
    }
}

/// The kernel's `tc_htb_opt` structure for HTB class configuration.
///
/// # Kernel Definition
///
/// From `<linux/pkt_sched.h>`:
///
/// ```c
/// struct tc_htb_opt {
///     struct tc_ratespec rate;   /* Guaranteed rate */
///     struct tc_ratespec ceil;   /* Ceiling rate */
///     __u32 buffer;              /* Burst size in ticks */
///     __u32 cbuffer;             /* Ceil burst size in ticks */
///     __u32 quantum;             /* Quantum for deficit round-robin (0 = auto) */
///     __u32 level;               /* Class level (0 = leaf) */
///     __u32 prio;                /* Priority (0 = highest) */
/// };
/// ```
#[derive(Debug, Clone, Copy)]
struct HtbOpt {
    rate: TcRateSpec,
    ceil: TcRateSpec,
    buffer: u32,
    cbuffer: u32,
    quantum: u32,
    level: u32,
    prio: u32,
}

impl HtbOpt {
    fn as_bytes(self) -> Vec<u8> {
        let mut vec = Vec::with_capacity(44); // 2x12 + 5x4
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

/// Compute buffer ticks for HTB's effectively unlimited rate.
///
/// Uses the iproute2 formula:
/// `buffer_ticks = burst_bytes * tick_in_usec * 1_000_000 / rate_bytes_per_sec`
fn compute_buffer_ticks() -> u32 {
    let tick_in_usec = *TICK_IN_USEC;
    (HTB_DEFAULT_BURST_BYTES as f64 * tick_in_usec * 1_000_000.0
        / HTB_UNLIMITED_RATE_BPS as f64) as u32
}

/// Builder for creating an HTB (Hierarchical Token Bucket) root qdisc.
///
/// HTB is our root qdisc, chosen because it correctly handles non-work-conserving
/// child qdiscs (like netem with delay). When netem returns NULL during dequeue,
/// HTB's `htb_dequeue_tree()` moves to the next class, preventing head-of-line
/// blocking.
///
/// The `defcls=1` parameter routes unclassified traffic to class 1:1 (no impairments).
#[derive(Debug, Clone)]
pub struct QdiscHtbRequest {
    pub inner: QdiscRequestInner,
}

impl QdiscHtbRequest {
    /// Create a new HTB qdisc request for the given interface.
    ///
    /// The qdisc will be created at the root with handle 1:0.
    pub fn new(inner: QdiscRequestInner) -> Self {
        Self { inner }
    }

    /// Build the netlink message to create this HTB qdisc.
    ///
    /// HTB requires a `TCA_HTB_INIT` attribute containing the `tc_htb_glob` structure
    /// with version, rate2quantum, and default class configuration.
    pub fn build(self) -> NetlinkMessage<RouteNetlinkMessage> {
        let mut tc_message = TcMessage::with_index(self.inner.interface_index);
        tc_message.header.parent = TcHandle::ROOT;
        tc_message.header.handle = TcHandle::from(0x0001_0000); // 1:0

        tc_message.attributes.push(TcAttribute::Kind("htb".to_string()));

        // HTB init options: tc_htb_glob wrapped in TCA_OPTIONS -> TCA_HTB_INIT
        let glob = HtbGlob::default();
        let init_nla = build_nla(TCA_HTB_INIT, &glob.as_bytes());
        tc_message.attributes.push(TcAttribute::Other(build_nested_options(init_nla)));

        let mut nl_req = NetlinkMessage::from(RouteNetlinkMessage::NewQueueDiscipline(tc_message));
        // NLM_F_REPLACE allows updating an existing qdisc
        nl_req.header.flags = NLM_F_CREATE | NLM_F_REPLACE | NLM_F_REQUEST | NLM_F_ACK;

        nl_req
    }
}

/// Builder for creating an HTB class.
///
/// Each destination peer gets its own HTB class with effectively unlimited
/// rate and ceil (~10 Gbit/s). The class serves as the attachment point for
/// TBF and netem qdiscs that implement actual impairments.
///
/// With `prio=0` for all classes, HTB uses deficit-based round-robin among them.
///
/// # Handle Scheme
///
/// For a destination peer with ID `N`:
/// - Class handle: `1:(10 + N)` (e.g., peer 2 -> class 1:12)
///
/// # Example
///
/// ```no_run
/// use linkem::tc::handle::{ID_OFFSET, QdiscRequestInner};
/// use linkem::tc::htb::HtbClassRequest;
/// use rtnetlink::packet_route::tc::TcHandle;
///
/// let if_index = 1; // Network interface index
/// // Create class for traffic to peer 2
/// let class_minor = ID_OFFSET + 2; // 12
/// let request = HtbClassRequest::new(
///     QdiscRequestInner::new(if_index)
///         .with_parent(TcHandle::from(0x0001_0000))  // Parent is HTB root (1:0)
///         .with_handle(TcHandle::from((1 << 16) | class_minor)), // 1:12
/// ).build();
/// ```
#[derive(Debug, Clone)]
pub struct HtbClassRequest {
    pub inner: QdiscRequestInner,
    /// If true, replace an existing class instead of failing if it exists.
    pub replace: bool,
}

impl HtbClassRequest {
    /// Create a new HTB class request with unlimited rate/ceil.
    pub fn new(inner: QdiscRequestInner) -> Self {
        Self { inner, replace: false }
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

        let buffer_ticks = compute_buffer_ticks();

        let rate_spec = TcRateSpec {
            rate: HTB_UNLIMITED_RATE_BPS,
            linklayer: 1, // TC_LINKLAYER_ETHERNET
            cell_align: -1,
            ..Default::default()
        };

        let opt = HtbOpt {
            rate: rate_spec,
            ceil: rate_spec,
            buffer: buffer_ticks,
            cbuffer: buffer_ticks,
            quantum: 0, // Let kernel compute from rate
            level: 0,   // Leaf class
            prio: 0,    // All classes same priority
        };

        // HTB class options: TCA_HTB_PARMS + TCA_HTB_RTAB + TCA_HTB_CTAB in TCA_OPTIONS
        let parms_nla = build_nla(TCA_HTB_PARMS, &opt.as_bytes());
        let rtab_nla = build_nla(TCA_HTB_RTAB, &DEFAULT_RATE_TABLE);
        let ctab_nla = build_nla(TCA_HTB_CTAB, &DEFAULT_RATE_TABLE);

        let mut combined = parms_nla;
        combined.extend(rtab_nla);
        combined.extend(ctab_nla);
        tc_message.attributes.push(TcAttribute::Other(build_nested_options(combined)));

        let mut nl_req = NetlinkMessage::from(RouteNetlinkMessage::NewTrafficClass(tc_message));
        nl_req.header.flags = if self.replace {
            NLM_F_CREATE | NLM_F_REPLACE | NLM_F_REQUEST | NLM_F_ACK
        } else {
            NLM_F_CREATE | NLM_F_EXCL | NLM_F_REQUEST | NLM_F_ACK
        };

        nl_req
    }
}
