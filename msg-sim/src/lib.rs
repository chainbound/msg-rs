pub use protocol::Protocol;
use std::{collections::HashMap, io, net::IpAddr, process::Command, time::Duration};

mod protocol;

#[cfg(target_os = "macos")]
pub mod dummynet;
#[cfg(target_os = "macos")]
use dummynet::{PacketFilter, Pipe};

pub mod utils;
use utils::assert_status;

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

        let simulation = Simulation::new(id, endpoint, config);

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
    fn start(&self) -> io::Result<()> {
        // Create network namespace
        let network_namespace = format!("msg-sim-{}", self.id);

        let status = Command::new("sudo")
            .args(["ip", "netns", "add", &network_namespace])
            .status()?;

        assert_status(status, "Failed to create namespace")?;

        // Create Virtual Ethernet (veth) devices and link them
        //
        // Note: device name length can be max 15 chars long
        let veth_host = format!("vh-msg-sim-{}", self.id);
        let veth_namespace = format!("vn-msg-sim-{}", self.id);
        let status = Command::new("sudo")
            .args([
                "ip",
                "link",
                "add",
                &veth_host,
                "type",
                "veth",
                "peer",
                "name",
                &veth_namespace,
            ])
            .status()?;

        assert_status(status, "Failed to veth devices")?;

        // Move veth namespace device to its namespace
        let status = Command::new("sudo")
            .args([
                "ip",
                "link",
                "set",
                &veth_namespace,
                "netns",
                &network_namespace,
            ])
            .status()?;

        assert_status(status, "Failed move veth device to network namespace")?;

        let ip_namespace = format!("{}/24", self.endpoint);

        let mut ip_host: Vec<u64> = self
            .endpoint
            .to_string()
            .split('.')
            .map(|octect| octect.parse::<u64>().unwrap())
            .collect();
        ip_host[3] += 1;
        let ip_host = format!(
            "{}.{}.{}.{}/24",
            ip_host[0], ip_host[1], ip_host[2], ip_host[3]
        );

        // Associate IP address to host veth device and spin it up
        let status = Command::new("sudo")
            .args(["ip", "addr", "add", &ip_host, "dev", &veth_host])
            .status()?;
        assert_status(status, "Failed to associate IP address to host veth device")?;
        let status = Command::new("sudo")
            .args(["ip", "link", "set", &veth_host, "up"])
            .status()?;
        assert_status(status, "Failed to set up the host veth device")?;

        // sudo ip netns exec msg-sim-1 ip addr add 192.168.1.1 dev vn-msg-sim-1

        // Associate IP address to namespaced veth device and spin it up
        let status = Command::new("sudo")
            .args([
                "ip",
                "netns",
                "exec",
                &network_namespace,
                "ip",
                "addr",
                "add",
                &ip_namespace,
                "dev",
                &veth_namespace,
            ])
            .status()?;
        assert_status(
            status,
            "Failed to associate IP address to namespaced veth device",
        )?;
        let status = Command::new("sudo")
            .args([
                "ip",
                "netns",
                "exec",
                &network_namespace,
                "ip",
                "link",
                "set",
                &veth_namespace,
                "up",
            ])
            .status()?;
        assert_status(status, "Failed to set up the namespaced veth device")?;

        // Add network emulation parameters on namespaced veth device
        let mut args = vec![
            "ip",
            "netns",
            "exec",
            &network_namespace,
            "tc",
            "qdisc",
            "add",
            "dev",
            &veth_namespace,
            "root",
            "netem",
        ];

        let delay = if let Some(delay) = self.config.latency {
            format!("{}ms", delay.as_millis())
        } else {
            "0ms".to_string()
        };

        if self.config.latency.is_some() {
            args.push("delay");
            args.push(&delay);
        }

        let loss = if let Some(loss) = self.config.plr {
            format!("{}%", loss)
        } else {
            "0%".to_string()
        };

        if (self.config.plr).is_some() {
            args.push("loss");
            args.push(&loss);
        }

        let status = Command::new("sudo").args(args).status()?;

        assert_status(status, "Failed to set network emulation paramteres")?;

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

#[cfg(test)]
mod test {
    use std::{
        net::{IpAddr, Ipv4Addr},
        time::Duration,
    };

    use crate::{Protocol, Simulation, SimulationConfig};

    #[test]
    fn start_on_linux() {
        let config = SimulationConfig {
            latency: Some(Duration::new(3, 0)),
            bw: Some(1_000),
            plr: Some(50_f64),
            protocols: vec![Protocol::TCP],
        };
        let simulation = Simulation::new(1, IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), config);

        let res = simulation.start();
        println!("res = {:?}", res);
    }
}
