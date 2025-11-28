#![doc(issue_tracker_base_url = "https://github.com/chainbound/msg-rs/issues/")]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

use std::{collections::HashMap, io, net::IpAddr, time::Duration};

mod protocol;
pub use protocol::Protocol;

pub mod namespace;
use std::process::Command;

pub mod assert;
pub mod ip_tc;

#[derive(Debug)]
pub struct SimulationConfig {
    /// The latency of the connection.
    pub latency: Option<Duration>,
    /// The bandwidth in Kbps.
    pub bw: Option<u64>,
    /// The maximum burst size in kbit.
    pub burst: Option<u64>,
    /// The buffer size in bytes.
    pub limit: Option<u64>,
    /// The packet loss rate in percent.
    pub plr: Option<f64>,
    /// The supported protocols.
    pub protocols: Vec<Protocol>,
}

#[derive(Default)]
pub struct Simulator {
    /// A map of active simulations.
    pub active_sims: HashMap<IpAddr, Simulation>,
    /// Simulation ID counter.
    pub active_sim_count: u8,
    /// A unique simulator identifier.
    pub id: u8,
}

impl Simulator {
    pub fn new(id: u8) -> Self {
        Self { active_sims: HashMap::new(), active_sim_count: 1, id }
    }

    /// Starts a new simulation on the given endpoint according to the config.
    ///
    /// ### Linux
    /// The simulation is done using network namespaces where multiple
    /// IP addresses are needed. Make sure that the endpoint IP address is not alreay in use,
    /// and that the "next one" is available.
    ///
    /// #### Example
    /// If `endpoint` is 192.168.1.1, then both 192.168.1.1 and 192.168.1.2 will be used
    pub fn start(&mut self, endpoint: IpAddr, config: SimulationConfig) -> io::Result<u8> {
        let id = self.active_sim_count;

        let mut simulation = Simulation::new(id, self.id, endpoint, config);

        simulation.start()?;

        self.active_sim_count += 1;

        self.active_sims.insert(endpoint, simulation);

        Ok(id)
    }

    /// Stops the simulation on the given device.
    pub fn stop(&mut self, device: IpAddr) {
        // This will drop the simulation, which will kill the process.
        self.active_sims.remove(&device);
    }
}

/// An active simulation spawned by the simulator.
pub struct Simulation {
    pub id: u8,
    pub simulator_id: u8,
    pub endpoint: IpAddr,
    pub config: SimulationConfig,
}

impl Simulation {
    fn new(id: u8, simulator_id: u8, endpoint: IpAddr, config: SimulationConfig) -> Self {
        Self { id, simulator_id, endpoint, config }
    }
}

impl Simulation {
    /// Get the namespace name used for the simulation.
    #[inline]
    pub fn namespace_name(&self) -> String {
        format!("msg-{}-{}", self.simulator_id, self.id)
    }

    /// Get the host veth device used name for the simulation.
    #[inline]
    pub fn veth_host_name(&self) -> String {
        format!("vh-msg-{}-{}", self.simulator_id, self.id)
    }

    /// Get the namespaced veth device name used for the simulation.
    #[inline]
    pub fn veth_namespace_name(&self) -> String {
        format!("vn-msg-{}-{}", self.simulator_id, self.id)
    }

    /// Starts the simulation.
    fn start(&mut self) -> io::Result<()> {
        // Create network namespace

        let network_namespace = self.namespace_name();
        let veth_host = self.veth_host_name();
        let veth_namespace = self.veth_namespace_name();
        let ip_namespace = format!("{}/24", self.endpoint);

        ip_tc::create_namespace(&network_namespace)?;
        ip_tc::create_veth_pair(&veth_host, &veth_namespace)?;
        ip_tc::move_device_to_namespace(&veth_namespace, &network_namespace)?;

        let ip_host = ip_tc::gen_host_ip_address(&self.endpoint);

        ip_tc::add_ip_addr_to_device(&veth_host, &ip_host, None)?;
        ip_tc::spin_up_device(&veth_host, None)?;

        ip_tc::add_ip_addr_to_device(&veth_namespace, &ip_namespace, Some(&network_namespace))?;
        ip_tc::spin_up_device(&veth_namespace, Some(&network_namespace))?;
        ip_tc::spin_up_device("lo", Some(&network_namespace))?;

        let delay = format!("{}ms", self.config.latency.unwrap_or(Duration::new(0, 0)).as_millis());

        let loss = format!("{}%", self.config.plr.unwrap_or(0_f64));

        // Add delay to the host veth device to match MacOS symmetric behaviour
        //
        // The behaviour is specified on the top-level ("root"),
        // with a custom handle for identification
        let mut args = vec!["root", "handle", "1:", "netem"];
        if self.config.latency.is_some() {
            args.push("delay");
            args.push(&delay);
        }
        ip_tc::add_network_emulation_parameters(&veth_host, args, None)?;

        // Add network emulation parameters (delay, loss) on namespaced veth device
        let mut args = vec!["root", "handle", "1:", "netem"];
        if self.config.latency.is_some() {
            args.push("delay");
            args.push(&delay);
        }
        if (self.config.plr).is_some() {
            args.push("loss");
            args.push(&loss);
        }
        ip_tc::add_network_emulation_parameters(&veth_namespace, args, Some(&network_namespace))?;

        // Add bandwidth paramteres on namespaced veth device
        //
        // The behaviour is specified on top of the root queue discipline,
        // as parent. It uses "Hierarchical Token Bucket" (HBT) discipline
        if let Some(bandwidth) = self.config.bw {
            let bandwidth = format!("{}kbit", bandwidth);
            let burst = format!("{}kbit", self.config.burst.unwrap_or(32));
            let limit = format!("{}", self.config.limit.unwrap_or(10_000));

            let args = vec![
                "parent", "1:", "handle", "2:", "tbf", "rate", &bandwidth, "burst", &burst,
                "limit", &limit,
            ];

            ip_tc::add_network_emulation_parameters(&veth_host, args.clone(), None)?;
            ip_tc::add_network_emulation_parameters(
                &veth_namespace,
                args,
                Some(&network_namespace),
            )?;
        }
        Ok(())
    }
}

impl Drop for Simulation {
    fn drop(&mut self) {
        let veth_host = self.veth_host_name();
        let network_namespace = self.namespace_name();

        // The only thing we have to do in the host to delete the veth device
        let _ = Command::new("sudo").args(["ip", "link", "del", &veth_host]).status();
        // Deleting the network namespace where the simulated endpoint lives
        // drops everything in there
        let _ = Command::new("sudo").args(["ip", "netns", "del", &network_namespace]).status();
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn start_simulation() {
        use std::{
            net::{IpAddr, Ipv4Addr},
            time::Duration,
        };

        use super::*;

        let mut simulator = Simulator::new(1);

        let addr = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1));
        let config = SimulationConfig {
            latency: Some(Duration::from_millis(2000)),
            bw: Some(1_000),
            burst: Some(32),
            limit: None,
            plr: Some(30_f64),
            protocols: vec![Protocol::Tcp],
        };

        let res = simulator.start(addr, config);
        if let Err(e) = &res {
            eprintln!("{}", e);
        }
        assert!(res.is_ok());
    }
}
