use std::time::Duration;

use futures::StreamExt as _;
use rtnetlink::{
    Handle,
    packet_core::{NLM_F_ACK, NLM_F_CREATE, NLM_F_EXCL, NLM_F_REQUEST, NetlinkMessage},
    packet_route::{
        RouteNetlinkMessage,
        tc::{TcAttribute, TcMessage},
    },
    try_nl,
};

/// The impairments that can be applied to a network link.
///
/// Each field corresponds to a feature supported by Linux `tc`.
/// None = the impairment is not applied.
#[derive(Debug, Clone, Default)]
pub struct LinkImpairment {
    /// Latency to introduce (applied by the netem qdisc).
    pub latency: Option<Duration>,
    /// Maximum bandwidth in kilobits per second (applied by TBF).
    pub bandwidth_kbps: Option<u64>,
    /// Maximum allowed burst size in kilobits.
    pub burst_kbit: Option<u64>,
    /// Maximum buffer (queue) size in bytes.
    pub buffer_size_bytes: Option<u64>,
    /// Probability of packet loss, in percent (applied by netem).
    pub packet_loss_rate_percent: Option<f64>,
}

impl LinkImpairment {
    pub fn with_latency(mut self, duration: Duration) -> Self {
        self.latency = Some(duration);
        self
    }

    pub fn with_bandwidth_kbps(mut self, kbps: u64) -> Self {
        self.bandwidth_kbps = Some(kbps);
        self
    }

    pub fn with_burst_kbit(mut self, burst: u64) -> Self {
        self.burst_kbit = Some(burst);
        self
    }

    pub fn with_buffer_size_bytes(mut self, bytes: u64) -> Self {
        self.buffer_size_bytes = Some(bytes);
        self
    }

    pub fn with_packet_loss_rate_percent(mut self, pct: f64) -> Self {
        self.packet_loss_rate_percent = Some(pct);
        self
    }
}

impl LinkImpairment {
    /// TODO: add tbf support for burst_kbit, buffer_size and bandwidth_kbps
    pub fn to_tc_commands(&self, iface: &str) -> Vec<String> {
        let mut cmds = Vec::new();

        // 1. Construct root netem parameters (delay, loss, etc.)
        let mut netem = Vec::new();

        if let Some(lat) = self.latency {
            netem.push(format!("delay {}ms", lat.as_millis()));
        }
        if let Some(loss) = self.packet_loss_rate_percent {
            netem.push(format!("loss {}%", loss));
        }

        // 2. Install root netem
        cmds.push(format!(
            "tc qdisc replace dev {iface} root handle 10: netem {}",
            netem.join(" ")
        ));

        cmds
    }
}

fn netem_loss_probability(percent: f64) -> u32 {
    ((percent / 100.0) * (u32::MAX as f64)) as u32
}

// async fn setup_tc_with_netem(handle: &Handle, ifindex: i32) -> Result<(), rtnetlink::Error> {
//     // === Build TcMessage ===
//     let mut message = TcMessage::with_index(ifindex);
//
//     // parent 1:3
//     message.header.parent = 0x0001_0003u32.into();
//
//     // handle 30:
//     message.header.handle = 0x001e_0000u32.into();
//
//     // tc qdisc kind
//     message.attributes.push(TcAttribute::Kind("netem".to_string()));
//
//     // === Build TCA_OPTIONS ===
//     let loss = netem_loss_probability(40.0);
//
//     let options = vec![TcAttribute::NetemLoss(loss)];
//
//     message.attributes.push(TcAttribute::Options(options));
//
//     // === Wrap into netlink message ===
//     let mut req = NetlinkMessage::from(RouteNetlinkMessage::NewQueueDiscipline(message));
//
//     req.header.flags = NLM_F_REQUEST | NLM_F_ACK | NLM_F_EXCL | NLM_F_CREATE;
//
//     // === Send ===
//     let mut response = handle.request(req)?;
//
//     while let Some(msg) = response.try_next().await? {
//         // ACK / ERROR handled here
//         msg.payload.inner_error().map_err(|e| rtnetlink::Error::NetlinkError(e))?;
//     }
//
//     Ok(())
// }
