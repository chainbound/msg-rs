//! DRR (Deficit Round Robin) qdisc and class support.

use rtnetlink::packet_core::{
    NLM_F_ACK, NLM_F_CREATE, NLM_F_EXCL, NLM_F_REPLACE, NLM_F_REQUEST, NetlinkMessage,
};
use rtnetlink::packet_route::{
    RouteNetlinkMessage,
    tc::{TcAttribute, TcHandle, TcMessage},
};

use super::handle::QdiscRequestInner;
use super::nla::{build_nested_options, build_nla};

// DRR-specific TCA_OPTIONS sub-attributes (from linux/pkt_sched.h)
/// DRR class quantum attribute type.
const TCA_DRR_QUANTUM: u16 = 1;

/// The default quantum for DRR classes, in bytes.
///
/// With a large quantum (4GiB of [`u32::MAX`]), DRR effectively becomes a pure packet router—when
/// a class's turn comes, it drains its queue entirely before moving to the next class. This is
/// desirable because we use DRR only for classification; actual rate limiting is done by TBF.
///
/// The quantum must be at least as large as the maximum packet size (MTU) to ensure packets
/// can always be dequeued. 1MB is large enough to handle any reasonable packet while still
/// being well within safe bounds.
pub const DRR_DEFAULT_QUANTUM: u32 = u32::MAX; // 4GiB

/// Builder for creating a DRR (Deficit Round Robin) root qdisc.
///
/// DRR is our root qdisc, chosen because it allows an unlimited number of classes
/// to be created dynamically with minimal overhead. Each class can have its own
/// qdisc chain (TBF → netem) for per-destination impairments.
///
/// Unlike HTB, DRR doesn't impose bandwidth shaping semantics at the root level.
/// With a large quantum, it acts as a pure packet classifier/router.
#[derive(Debug, Clone)]
pub struct QdiscDrrRequest {
    pub inner: QdiscRequestInner,
}

impl QdiscDrrRequest {
    /// Create a new DRR qdisc request for the given interface.
    ///
    /// The qdisc will be created at the root with handle 1:0.
    pub fn new(inner: QdiscRequestInner) -> Self {
        Self { inner }
    }

    /// Build the netlink message to create this DRR qdisc.
    ///
    /// DRR qdiscs are simple—they require no special options at creation time.
    /// The quantum is specified per-class, not at the qdisc level.
    pub fn build(self) -> NetlinkMessage<RouteNetlinkMessage> {
        let mut tc_message = TcMessage::with_index(self.inner.interface_index);
        tc_message.header.parent = TcHandle::ROOT;
        tc_message.header.handle = TcHandle::from(0x0001_0000); // 1:0

        tc_message.attributes.push(TcAttribute::Kind("drr".to_string()));

        let mut nl_req = NetlinkMessage::from(RouteNetlinkMessage::NewQueueDiscipline(tc_message));
        // NLM_F_REPLACE allows updating an existing qdisc
        nl_req.header.flags = NLM_F_CREATE | NLM_F_REPLACE | NLM_F_REQUEST | NLM_F_ACK;

        nl_req
    }
}

/// Builder for creating a DRR class.
///
/// Each destination peer gets its own DRR class, which serves as the attachment
/// point for the TBF and netem qdiscs that implement the actual impairments.
///
/// DRR classes are simple—they only have a `quantum` parameter that controls how
/// many bytes can be sent per scheduling round. With a large quantum (default 1MB),
/// the class effectively drains its entire queue each time it's scheduled, making
/// DRR act as a pure classifier rather than a fair scheduler.
///
/// # Handle Scheme
///
/// For a destination peer with ID `N`:
/// - Class handle: `1:(10 + N)` (e.g., peer 2 → class 1:12)
///
/// # Example
///
/// ```
/// use msg_sim::tc::handle::{ID_OFFSET, QdiscRequestInner};
/// use msg_sim::tc::drr::DrrClassRequest;
/// use rtnetlink::packet_route::tc::TcHandle;
///
/// let if_index = 1; // Network interface index
/// // Create class for traffic to peer 2
/// let class_minor = ID_OFFSET + 2; // 12
/// let request = DrrClassRequest::new(
///     QdiscRequestInner::new(if_index)
///         .with_parent(TcHandle::from(0x0001_0000))  // Parent is DRR root (1:0)
///         .with_handle(TcHandle::from((1 << 16) | class_minor)), // 1:12
/// ).build();
/// ```
#[derive(Debug, Clone)]
pub struct DrrClassRequest {
    pub inner: QdiscRequestInner,
    /// The quantum for this class in bytes.
    ///
    /// This determines how many bytes can be sent per scheduling round.
    /// With a large value (default [`DRR_DEFAULT_QUANTUM`]), the class
    /// drains its entire queue each time it's scheduled.
    pub quantum: u32,
    /// If true, replace an existing class instead of failing if it exists.
    pub replace: bool,
}

impl DrrClassRequest {
    /// Create a new DRR class request with default quantum.
    pub fn new(inner: QdiscRequestInner) -> Self {
        Self { inner, quantum: DRR_DEFAULT_QUANTUM, replace: false }
    }

    /// Set a custom quantum for this class.
    ///
    /// The quantum determines how many bytes can be sent per scheduling round.
    /// Must be at least as large as the maximum packet size (MTU).
    pub fn with_quantum(mut self, quantum: u32) -> Self {
        self.quantum = quantum;
        self
    }

    /// Set whether to replace an existing class.
    ///
    /// When `true`, uses `NLM_F_REPLACE` to update an existing class.
    /// When `false` (default), uses `NLM_F_EXCL` to fail if the class exists.
    pub fn with_replace(mut self, replace: bool) -> Self {
        self.replace = replace;
        self
    }

    /// Build the netlink message to create this DRR class.
    pub fn build(self) -> NetlinkMessage<RouteNetlinkMessage> {
        let mut tc_message = TcMessage::with_index(self.inner.interface_index);
        tc_message.header.parent = self.inner.parent;
        tc_message.header.handle = self.inner.handle;

        tc_message.attributes.push(TcAttribute::Kind("drr".to_string()));

        // DRR class options: just TCA_DRR_QUANTUM wrapped in TCA_OPTIONS
        let quantum_nla = build_nla(TCA_DRR_QUANTUM, &self.quantum.to_ne_bytes());
        tc_message.attributes.push(TcAttribute::Other(build_nested_options(quantum_nla)));

        let mut nl_req = NetlinkMessage::from(RouteNetlinkMessage::NewTrafficClass(tc_message));
        nl_req.header.flags = if self.replace {
            NLM_F_CREATE | NLM_F_REPLACE | NLM_F_REQUEST | NLM_F_ACK
        } else {
            NLM_F_CREATE | NLM_F_EXCL | NLM_F_REQUEST | NLM_F_ACK
        };

        nl_req
    }
}
