use std::time::Duration;

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
