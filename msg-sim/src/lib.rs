use std::{collections::HashMap, io, net::IpAddr, time::Duration};

pub use protocol::Protocol;

mod protocol;

#[cfg(target_os = "macos")]
pub mod dummynet;
#[cfg(target_os = "macos")]
use dummynet::{PacketFilter, Pipe};

#[derive(Debug)]
#[allow(unused)]
pub struct SimulationConfig {
    /// The latency of the connection.
    pub latency: Option<Duration>,
    /// The bandwidth in Kbps.
    pub bw: Option<u64>,
    /// The packet loss rate in percent.
    pub plr: Option<f64>,
    /// The supported protocols.
    pub protocols: Vec<Protocol>,
}

#[derive(Default)]
pub struct Simulator {
    /// A map of active simulations.
    active_sims: HashMap<IpAddr, Simulation>,
    /// Simulation ID counter.
    sim_id: usize,
}

impl Simulator {
    pub fn new() -> Self {
        Self {
            active_sims: HashMap::new(),
            sim_id: 1,
        }
    }

    /// Starts a new simulation on the given endpoint according to the config.
    pub fn start(&mut self, endpoint: IpAddr, config: SimulationConfig) -> io::Result<usize> {
        let id = self.sim_id;

        let mut simulation = Simulation::new(id, endpoint, config);

        simulation.start()?;

        self.sim_id += 1;

        self.active_sims.insert(endpoint, simulation);

        Ok(id)
    }

    /// Stops the simulation on the given device.
    pub fn stop(&mut self, device: IpAddr) {
        // This will drop the simulation, which will kill the process.
        self.active_sims.remove(&device);
    }
}

/// An active simulation.
#[allow(unused)]
struct Simulation {
    id: usize,
    endpoint: IpAddr,
    config: SimulationConfig,

    #[cfg(target_os = "macos")]
    active_pf: Option<PacketFilter>,
}

impl Simulation {
    fn new(id: usize, endpoint: IpAddr, config: SimulationConfig) -> Self {
        Self {
            id,
            endpoint,
            config,
            #[cfg(target_os = "macos")]
            active_pf: None,
        }
    }

    /// Starts the simulation.
    #[cfg(target_os = "linux")]
    fn start(&mut self) -> io::Result<()> {
        Ok(())
    }

    #[cfg(target_os = "macos")]
    fn start(&mut self) -> io::Result<()> {
        // Create a dummynet pipe
        let mut pipe = Pipe::new(self.id);

        // Configure the pipe according to the simulation config.
        if let Some(latency) = self.config.latency {
            pipe = pipe.delay(latency.as_millis());
        }

        if let Some(bw) = self.config.bw {
            pipe = pipe.bandwidth(bw);
        }

        if let Some(plr) = self.config.plr {
            pipe = pipe.plr(plr);
        }

        let mut pf = PacketFilter::new(pipe)
            .anchor(format!("msg-sim-{}", self.id))
            .endpoint(self.endpoint);

        if !self.config.protocols.is_empty() {
            pf = pf.protocols(self.config.protocols.clone());
        }

        pf.enable()?;

        self.active_pf = Some(pf);

        Ok(())
    }
}

impl Drop for Simulation {
    #[cfg(target_os = "linux")]
    fn drop(&mut self) {}

    #[cfg(target_os = "macos")]
    fn drop(&mut self) {
        if let Some(pf) = self.active_pf.take() {
            pf.destroy().unwrap();
        }
    }
}
