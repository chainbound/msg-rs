use std::{collections::HashMap, time::Duration};

use protocol::Protocol;

mod cmd;
mod protocol;

/// A type alias for a network device.
pub type Device = String;

#[derive(Debug)]
pub struct SimConfig {
    latency: Duration,
    target_bw: u64,
    default_bw: u64,
    packet_loss: f64,
    protocols: Vec<Protocol>,
}

#[derive(Default)]
pub struct Simulator {
    /// A map of active simulations.
    active_sims: HashMap<Device, Simulation>,
}

impl Simulator {
    pub fn new() -> Self {
        Self {
            active_sims: HashMap::new(),
        }
    }

    /// Starts a new simulation on the given device according to the config.
    pub fn start(&mut self, device: Device, config: SimConfig) {
        let mut simulation = Simulation {
            device: device.clone(),
            config,
        };

        simulation.start();

        self.active_sims.insert(device, simulation);
    }

    /// Stops the simulation on the given device.
    pub fn stop(&mut self, device: Device) {
        // This will drop the simulation, which will kill the process.
        self.active_sims.remove(&device);
    }
}

/// An active simulation.
struct Simulation {
    device: Device,
    config: SimConfig,
}

impl Simulation {
    /// Starts the simulation.
    fn start(&mut self) {
        // TODO: start the simulation by executing the right command
        #[cfg(target_os = "linux")]
        {}

        #[cfg(target_os = "macos")]
        {}
    }
}

impl Drop for Simulation {
    fn drop(&mut self) {
        // TODO: kill the simulation by executing the right command
        #[cfg(target_os = "linux")]
        {}

        #[cfg(target_os = "macos")]
        {}
    }
}
