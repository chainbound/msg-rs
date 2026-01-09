//! TC filter support (flower and u32 filters).
//!
//! Filters classify packets and route them to appropriate classes.
//! We use flower filters for destination IP matching and u32 for catch-all.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use rtnetlink::packet_core::{NLM_F_ACK, NLM_F_CREATE, NLM_F_EXCL, NLM_F_REQUEST, NetlinkMessage};
use rtnetlink::packet_route::tc::TcFilterFlowerOption;
use rtnetlink::packet_route::{
    RouteNetlinkMessage,
    tc::{TcAttribute, TcHandle, TcMessage, TcOption},
};

use super::handle::QdiscRequestInner;
use super::nla::{build_nested_options, build_nla};

/// EtherType for IPv4 packets (0x0800).
const ETH_P_IP: u16 = nix::libc::ETH_P_IP as u16;

/// EtherType for IPv6 packets (0x86DD).
const ETH_P_IPV6: u16 = nix::libc::ETH_P_IPV6 as u16;

/// Compute an IPv4 netmask from a prefix length.
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

/// Builder for creating a flower filter.
///
/// Flower filters classify packets based on various criteria. We use them to
/// match packets by destination IP address and route them to the appropriate
/// DRR class for impairment.
///
/// # How Classification Works
///
/// 1. Packet enters DRR root qdisc
/// 2. Flower filter examines destination IP
/// 3. If IP matches → packet goes to the specified class (e.g., 1:12)
/// 4. If no match → packet goes to default class (1:1)
///
/// # Example
///
/// ```
/// use std::net::{IpAddr, Ipv4Addr};
/// use msg_sim::tc::filter::FlowerFilterRequest;
/// use msg_sim::tc::handle::QdiscRequestInner;
/// use rtnetlink::packet_route::tc::TcHandle;
///
/// let if_index = 1; // Network interface index
/// // Route traffic to 10.0.0.2 into class 1:12
/// let request = FlowerFilterRequest::new(
///     QdiscRequestInner::new(if_index)
///         .with_parent(TcHandle::from(0x0001_0000)), // Attach to DRR root
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

// -------------------------------------------------------------------------------------
// U32 Catch-all Filter
// -------------------------------------------------------------------------------------

// U32 filter TCA_OPTIONS sub-attributes (from linux/pkt_cls.h)
/// U32 class ID attribute type.
const TCA_U32_CLASSID: u16 = 1;
/// U32 selector attribute type.
const TCA_U32_SEL: u16 = 5;

// U32 filter flags (from linux/pkt_cls.h)
/// Marks a u32 filter as terminal - required for the filter to return a classification result.
const TC_U32_TERMINAL: u8 = 1;

/// The kernel's `tc_u32_sel` structure for u32 filter selection.
///
/// This is a simplified version that only supports a single "match all" key.
/// The full structure supports multiple keys and more complex matching.
#[derive(Debug, Clone, Copy, Default)]
struct TcU32Sel {
    /// Flags (usually 0).
    flags: u8,
    /// Offset shift (usually 0).
    offshift: u8,
    /// Number of keys (1 for our match-all case).
    nkeys: u8,
    /// Offset mask (usually 0).
    offmask: u16,
    /// Fixed offset (usually 0).
    off: u16,
    /// Offset from end (usually 0).
    offoff: i16,
    /// Minimum header length (usually 0).
    hoff: i16,
    /// Hash mask (usually 0).
    hmask: u32,
    // Followed by nkeys * tc_u32_key structures
}

/// The kernel's `tc_u32_key` structure for u32 matching.
#[derive(Debug, Clone, Copy, Default)]
struct TcU32Key {
    /// Mask to apply before comparison.
    mask: u32,
    /// Value to compare against.
    val: u32,
    /// Offset in packet to start comparison.
    off: i32,
    /// Offset mask (usually 0).
    offmask: i32,
}

/// Builder for creating a u32 catch-all filter.
///
/// The u32 filter with `match u32 0 0` matches all packets. This is used as a catch-all
/// to route unclassified traffic to the default class.
///
/// # Why This Is Needed
///
/// DRR (unlike HTB) doesn't have a built-in default class mechanism. Packets that
/// don't match any filter are dropped. To handle unclassified traffic (like ARP
/// packets, which don't have IP headers), we need a catch-all filter with a low
/// priority (high number) that catches everything and sends it to class 1:1.
///
/// # Why u32 Instead of matchall
///
/// The `matchall` filter requires the `cls_matchall` kernel module which may not be
/// available on all systems. The `u32` filter is more universally supported and can
/// achieve the same effect with `match u32 0 0` (which always matches).
///
/// # Priority System
///
/// TC filters are checked in order of priority (lower number = checked first).
/// - Specific destination filters: priority 49152 (default for flower)
/// - Catchall filter: priority 65535 (lowest priority, checked last)
///
/// # Example
///
/// ```
/// use msg_sim::tc::filter::U32CatchallFilterRequest;
/// use msg_sim::tc::handle::QdiscRequestInner;
/// use rtnetlink::packet_route::tc::TcHandle;
///
/// let if_index = 1; // Network interface index
/// // Create a catch-all filter that sends unmatched traffic to class 1:1
/// let request = U32CatchallFilterRequest::new(
///     QdiscRequestInner::new(if_index)
///         .with_parent(TcHandle::from(0x0001_0000)), // Attach to DRR root
/// )
/// .with_class_id(0x0001_0001)  // Route to class 1:1
/// .build();
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct U32CatchallFilterRequest {
    pub inner: QdiscRequestInner,
    /// The class ID to route all traffic to.
    pub class_id: u32,
    /// Filter priority (higher number = checked later). Default is 65535 (lowest).
    pub priority: u16,
}

impl U32CatchallFilterRequest {
    /// Create a new u32 catch-all filter with default priority (65535).
    pub fn new(inner: QdiscRequestInner) -> Self {
        Self {
            inner,
            class_id: 0,
            priority: 65535, // Lowest priority = checked last
        }
    }

    /// Set the class ID to route all traffic to.
    pub fn with_class_id(mut self, class_id: u32) -> Self {
        self.class_id = class_id;
        self
    }

    /// Set the filter priority.
    ///
    /// Lower numbers are checked first. Use 65535 (default) for catch-all behavior.
    pub fn with_priority(mut self, priority: u16) -> Self {
        self.priority = priority;
        self
    }

    /// Build the netlink message to create this u32 catch-all filter.
    pub fn build(self) -> NetlinkMessage<RouteNetlinkMessage> {
        let mut tc_msg = TcMessage::with_index(self.inner.interface_index);
        tc_msg.header.parent = self.inner.parent;
        tc_msg.header.handle = TcHandle::from(0u32);
        // The info field contains the priority (in upper 16 bits) and protocol (in lower 16 bits)
        // ETH_P_ALL (0x0003) matches all protocols
        let eth_p_all: u16 = 0x0003;
        tc_msg.header.info = ((self.priority as u32) << 16) | (eth_p_all.to_be() as u32);

        tc_msg.attributes.push(TcAttribute::Kind("u32".to_string()));

        // Build the u32 selector with a single "match all" key (mask=0, val=0 always matches)
        let sel = TcU32Sel {
            flags: TC_U32_TERMINAL, // Must be terminal to return classification result
            nkeys: 1,               // One key
            ..Default::default()
        };

        // Mask of 0 means "don't care".
        // Value doesn't matter when mask is 0
        let key = TcU32Key::default();

        // Serialize selector + key
        let mut sel_bytes = Vec::with_capacity(size_of::<TcU32Sel>() + size_of::<TcU32Key>());
        sel_bytes.push(sel.flags);
        sel_bytes.push(sel.offshift);
        sel_bytes.push(sel.nkeys);
        sel_bytes.push(0); // padding
        sel_bytes.extend_from_slice(&sel.offmask.to_ne_bytes());
        sel_bytes.extend_from_slice(&sel.off.to_ne_bytes());
        sel_bytes.extend_from_slice(&sel.offoff.to_ne_bytes());
        sel_bytes.extend_from_slice(&sel.hoff.to_ne_bytes());
        sel_bytes.extend_from_slice(&sel.hmask.to_ne_bytes());
        // Add the key
        sel_bytes.extend_from_slice(&key.mask.to_ne_bytes());
        sel_bytes.extend_from_slice(&key.val.to_ne_bytes());
        sel_bytes.extend_from_slice(&key.off.to_ne_bytes());
        sel_bytes.extend_from_slice(&key.offmask.to_ne_bytes());

        // Build TCA_OPTIONS containing TCA_U32_CLASSID and TCA_U32_SEL
        let classid_nla = build_nla(TCA_U32_CLASSID, &self.class_id.to_ne_bytes());
        let sel_nla = build_nla(TCA_U32_SEL, &sel_bytes);

        let mut options = classid_nla;
        options.extend(sel_nla);
        tc_msg.attributes.push(TcAttribute::Other(build_nested_options(options)));

        let mut nl_req = NetlinkMessage::from(RouteNetlinkMessage::NewTrafficFilter(tc_msg));
        nl_req.header.flags = NLM_F_REQUEST | NLM_F_ACK | NLM_F_CREATE | NLM_F_EXCL;

        nl_req
    }
}
