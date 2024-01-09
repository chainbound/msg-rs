use std::{collections::HashMap, net::IpAddr, time::Duration};

use protocol::Protocol;

mod cmd;
mod protocol;

#[cfg(target_os = "macos")]
mod dummynet;

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;
const GIB: u64 = 1024 * MIB;

/// A type alias for a network device.
pub struct Endpoint {
    device: String,
}

#[derive(Debug)]
pub struct SimConfig {
    latency: Duration,
    target_bw: u64,
    default_bw: u64,
    /// The packet loss rate in percent.
    packet_loss_rate: f64,
    protocols: Vec<Protocol>,
}

#[derive(Default)]
pub struct Simulator {
    /// A map of active simulations.
    active_sims: HashMap<IpAddr, Simulation>,
}

impl Simulator {
    pub fn new() -> Self {
        Self {
            active_sims: HashMap::new(),
        }
    }

    /// Starts a new simulation on the given device according to the config.
    pub fn start(&mut self, endpoint: IpAddr, config: SimConfig) {
        let mut simulation = Simulation { endpoint, config };

        simulation.start();

        self.active_sims.insert(endpoint, simulation);
    }

    /// Stops the simulation on the given device.
    pub fn stop(&mut self, device: IpAddr) {
        // This will drop the simulation, which will kill the process.
        self.active_sims.remove(&device);
    }
}

/// An active simulation.
struct Simulation {
    endpoint: IpAddr,
    config: SimConfig,
}

impl Simulation {
    /// Starts the simulation.
    #[cfg(target_os = "linux")]
    fn start(&mut self) {}

    #[cfg(target_os = "macos")]
    fn start(&mut self) {}
}

impl Drop for Simulation {
    #[cfg(target_os = "linux")]
    fn drop(&mut self) {}

    #[cfg(target_os = "macos")]
    fn drop(&mut self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
}
