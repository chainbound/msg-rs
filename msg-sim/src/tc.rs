use std::io::{self, Read as _};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::LazyLock;

use nix::libc::TCA_OPTIONS;
use rtnetlink::packet_core::{DefaultNla, NLM_F_REPLACE, NetlinkMessage};
use rtnetlink::packet_route::tc::TcFilterFlowerOption;
use rtnetlink::packet_route::{
    RouteNetlinkMessage,
    tc::{TcAttribute, TcHandle, TcMessage, TcOption},
};

use rtnetlink::packet_core::{NLM_F_ACK, NLM_F_CREATE, NLM_F_EXCL, NLM_F_REQUEST};

const ETH_P_IP: u16 = nix::libc::ETH_P_IP as u16;
const ETH_P_IPV6: u16 = nix::libc::ETH_P_IPV6 as u16;

pub const DEFAULT_PRIORITY_BANDS: u32 = 3;
pub const DEFAULT_PRIORITY_MAP: [u8; 16] = [0, 1, 2, 2, 1, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1];

pub const PSCHED_PATH: &str = "/proc/net/psched";

/// Adapted from "iproute2/tc/tc_core.c".
///
/// It reads the file `/proc/net/psched`, which exposes the packet scheduler time base used by the
/// kernel traffic control subsystem. It expresses how to convert between: scheduler ticks and
/// microseconds.
///
/// The file returns four quantities, and the first two packet scheduler’s time scaling factor
/// (ticks per microsecond) expressed as numerator and denominator.
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

pub static TICK_IN_USEC: LazyLock<f64> =
    LazyLock::new(|| tc_core_init().expect("to read /proc/net/psched"));

pub fn usec_to_ticks(delay_usec: u32, tick_in_usec: f64) -> u32 {
    (delay_usec as f64 / tick_in_usec).round() as u32
}

/// The impairments that can be applied to a network link.
///
/// Each field corresponds to a feature supported by Linux `netem`.
///
/// TODO: add support for bandwidth, burst and custom buffer sizes.
///
/// For netem compatibility, it is important the fields are in this exact order.
///
/// From <linux/pkt_sched.h>:
///
/// ```c
/// struct tc_netem_qopt {
///     __u32 latency; /* Expressed in packet scheduler ticks */
///     __u32 limit;
///     __u32 loss;
///     __u32 gap;
///     __u32 duplicate;
///     __u32 jitter; /* Expressed in packet scheduler ticks */
/// };
/// ```
#[derive(Debug, Clone, Copy)]
pub struct LinkImpairment {
    /// Latency to introduce, in microseconds. When processed, it is converted into appropriate
    /// packet scheduler ticks.
    pub latency: u32,
    /// fifo limit (packets).
    pub limit: u32,
    /// Packet loss.
    pub loss: u32,
    /// Re-ordering gap.
    pub gap: u32,
    /// Random packet duplication.
    pub duplicate: u32,
    /// Random jitter, in microseconds. When processed, it is converted into appropriate
    /// packet scheduler ticks.
    pub jitter: u32,
}

impl Default for LinkImpairment {
    fn default() -> Self {
        Self {
            latency: 0,
            limit: 1_000, // netem default limit
            loss: 0,
            gap: 0,
            duplicate: 0,
            jitter: 0,
        }
    }
}

impl LinkImpairment {
    pub fn loss_probability(percent: f64) -> u32 {
        (percent / 100.0 * u32::MAX as f64) as u32
    }

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

fn ipv4_mask(prefix_len: u8) -> Ipv4Addr {
    let mask_u32 = u32::MAX << (32 - prefix_len);
    Ipv4Addr::from(mask_u32)
}

fn ipv6_mask(prefix_len: u8) -> Ipv6Addr {
    let mask_u128 = u128::MAX << (128 - prefix_len);
    Ipv6Addr::from(mask_u128)
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QdiscRequestInner {
    pub interface_index: i32,
    pub parent: TcHandle,
    pub handle: TcHandle,
}
impl QdiscRequestInner {
    pub fn new(index: i32) -> Self {
        Self { interface_index: index, parent: TcHandle::ROOT, handle: TcHandle::default() }
    }

    pub fn with_parent(mut self, parent: TcHandle) -> Self {
        self.parent = parent;
        self
    }

    pub fn with_handle(mut self, handle: TcHandle) -> Self {
        self.handle = handle;
        self
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QdiscPrioRequest {
    pub inner: QdiscRequestInner,
    pub bands: u32,
    pub priomap: [u8; 16],
}

impl QdiscPrioRequest {
    pub fn new(inner: QdiscRequestInner) -> Self {
        Self { inner, bands: DEFAULT_PRIORITY_BANDS, priomap: DEFAULT_PRIORITY_MAP }
    }

    pub fn with_bands(mut self, bands: u32) -> Self {
        self.bands = bands;
        self
    }

    pub fn with_priomap(mut self, priomap: [u8; 16]) -> Self {
        self.priomap = priomap;
        self
    }

    pub fn build(self) -> NetlinkMessage<RouteNetlinkMessage> {
        let mut tc_message = TcMessage::with_index(self.inner.interface_index);
        tc_message.header.parent = TcHandle::ROOT;
        tc_message.header.handle = TcHandle::from(0x0001_0000);

        let mut qopt = Vec::new();
        qopt.extend_from_slice(&DEFAULT_PRIORITY_BANDS.to_ne_bytes());
        qopt.extend_from_slice(&DEFAULT_PRIORITY_MAP);

        tc_message.attributes.push(TcAttribute::Kind("prio".to_string()));
        tc_message.attributes.push(TcAttribute::Other(DefaultNla::new(TCA_OPTIONS, qopt)));

        let mut nl_req = NetlinkMessage::from(RouteNetlinkMessage::NewQueueDiscipline(tc_message));
        nl_req.header.flags = NLM_F_CREATE | NLM_F_REPLACE | NLM_F_REQUEST | NLM_F_ACK;

        nl_req
    }
}

pub struct QDiscNetemRequest {
    pub inner: QdiscRequestInner,
    pub impairment: LinkImpairment,
}

impl QDiscNetemRequest {
    pub fn new(inner: QdiscRequestInner, impairment: LinkImpairment) -> Self {
        Self { inner, impairment }
    }

    pub fn build(mut self) -> NetlinkMessage<RouteNetlinkMessage> {
        let mut tc_message = TcMessage::with_index(self.inner.interface_index);
        tc_message.header.parent = self.inner.parent;
        tc_message.header.handle = self.inner.handle;

        self.impairment.latency = (self.impairment.latency as f64 * *TICK_IN_USEC) as u32;
        self.impairment.jitter = (self.impairment.jitter as f64 * *TICK_IN_USEC) as u32;

        tc_message.attributes.push(TcAttribute::Kind("netem".to_string()));
        tc_message
            .attributes
            .push(TcAttribute::Other(DefaultNla::new(TCA_OPTIONS, self.impairment.to_bytes())));

        let mut nl_req = NetlinkMessage::from(RouteNetlinkMessage::NewQueueDiscipline(tc_message));
        nl_req.header.flags = NLM_F_CREATE | NLM_F_EXCL | NLM_F_REQUEST | NLM_F_ACK;

        nl_req
    }
}

/// Creates an RTM_NEWTFILTER request that classifies packets by destination IP
/// (IPv4 or IPv6) into `classid` (e.g. 1:3 under a prio qdisc).
///
/// - `ifindex`: interface index of dev (ns1-hub)
/// - `parent`: the qdisc handle you attach the filter to (e.g. 1:0 => 0x0001_0000)
/// - `pref`: filter preference/priority (like `prio`/`pref` in tc); 0 is fine if you
///   don't care about ordering.
/// - `dst`: the destination IP to match
/// - `prefix_len`: /32, /128, etc.
/// - `classid`: the class you want to direct into (e.g. 1:3 => 0x0001_0003)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlowerFilterRequest {
    pub inner: QdiscRequestInner,
    pub destination: IpAddr,
    pub prefix: u8,
    pub class_id: u32,
}

impl FlowerFilterRequest {
    pub fn new(inner: QdiscRequestInner, destination: IpAddr) -> Self {
        Self {
            class_id: (inner.parent.major as u32) << 16 | 0x0003,
            inner,
            destination,
            prefix: 32,
            // Priority band 3.
        }
    }

    pub fn with_prefix(mut self, prefix: u8) -> Self {
        self.prefix = prefix;
        self
    }

    pub fn with_class_id(mut self, class_id: u32) -> Self {
        self.class_id = class_id;
        self
    }

    pub fn build(self) -> NetlinkMessage<RouteNetlinkMessage> {
        // Pick protocol and build the family-specific flower keys.
        let (proto_ethertype, match_opts): (u16, Vec<TcOption>) = match self.destination {
            IpAddr::V4(v4) => {
                let mask = ipv4_mask(self.prefix);
                (
                    ETH_P_IP,
                    vec![
                        TcOption::Flower(TcFilterFlowerOption::Ipv4Dst(v4)),
                        TcOption::Flower(TcFilterFlowerOption::Ipv4DstMask(mask)),
                    ],
                )
            }
            IpAddr::V6(v6) => {
                let mask = ipv6_mask(self.prefix);
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
        // kernel can auto-assign if you don't care
        tc_msg.header.handle = TcHandle::from(0u32);
        tc_msg.header.info = proto_ethertype.to_be() as u32;

        tc_msg.attributes.push(TcAttribute::Kind("flower".to_string()));

        // Build TCA_OPTIONS (flower options)
        //
        // iproute2 always includes:
        // - CLASSID (flowid)
        // - FLAGS (often 0)
        // - KEY_ETH_TYPE (derived from protocol)
        // plus the actual match keys (IPv4/IPv6 dst + mask).
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
