pub use protocol::Protocol;
use std::{collections::HashMap, io, net::IpAddr, process::Command, time::Duration};
mod protocol;

#[cfg(target_os = "macos")]
pub mod dummynet;
#[cfg(target_os = "macos")]
use dummynet::{PacketFilter, Pipe};

#[cfg(target_os = "linux")]
pub mod namespace;

pub mod assert;
use assert::assert_status;

#[derive(Debug)]
#[allow(unused)]
pub struct SimulationConfig {
    /// The latency of the connection.
    pub latency: Option<Duration>,
    /// The bandwidth in Kbps.
    pub bw: Option<u64>,
    /// The maximum burst size in kbit.
    #[cfg(target_os = "linux")]
    pub burst: Option<u64>,
    /// The buffer size in bytes.
    #[cfg(target_os = "linux")]
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
        Self {
            active_sims: HashMap::new(),
            active_sim_count: 1,
            id,
        }
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

        let simulation = Simulation::new(id, self.id, endpoint, config);

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
#[allow(unused)]
pub struct Simulation {
    pub id: u8,
    pub simulator_id: u8,
    pub endpoint: IpAddr,
    pub config: SimulationConfig,

    #[cfg(target_os = "macos")]
    active_pf: Option<PacketFilter>,
}

impl Simulation {
    fn new(id: u8, simulator_id: u8, endpoint: IpAddr, config: SimulationConfig) -> Self {
        Self {
            id,
            simulator_id,
            endpoint,
            config,
            #[cfg(target_os = "macos")]
            active_pf: None,
        }
    }

    #[inline]
    #[cfg(target_os = "linux")]
    /// Get the namespace name used for the simulation.
    pub fn namespace_name(&self) -> String {
        format!("msg-{}-{}", self.simulator_id, self.id)
    }

    #[inline]
    #[cfg(target_os = "linux")]
    /// Get the host veth device used name for the simulation.
    pub fn veth_host_name(&self) -> String {
        format!("vh-msg-{}-{}", self.simulator_id, self.id)
    }

    #[inline]
    #[cfg(target_os = "linux")]
    /// Get the namespaced veth device name used for the simulation.
    pub fn veth_namespace_name(&self) -> String {
        format!("vn-msg-{}-{}", self.simulator_id, self.id)
    }

    /// Starts the simulation.
    #[cfg(target_os = "linux")]
    fn start(&self) -> io::Result<()> {
        // Create network namespace
        let network_namespace = self.namespace_name();

        let status = Command::new("sudo")
            .args(["ip", "netns", "add", &network_namespace])
            .status()?;

        assert_status(status, "Failed to create namespace")?;

        // Create Virtual Ethernet (veth) devices and link them
        //
        // Note: device name length can be max 15 chars long
        let veth_host = self.veth_host_name();
        let veth_namespace = self.veth_namespace_name();
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

        assert_status(status, "Failed add veth devices")?;

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

        // Spin up also the loopback interface on namespaced environment
        let status = Command::new("sudo")
            .args([
                "ip",
                "netns",
                "exec",
                &network_namespace,
                "ip",
                "link",
                "set",
                "lo",
                "up",
            ])
            .status()?;
        assert_status(status, "Failed to set up the namespaced loopback device")?;

        // Add network emulation parameters (delay, loss) on namespaced veth device
        //
        // The behaviour is specified on the top-level ("root"),
        // with a custom handle for identification
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
            "handle",
            "1:",
            "netem",
        ];

        let delay = format!(
            "{}ms",
            self.config
                .latency
                .unwrap_or(Duration::new(0, 0))
                .as_millis()
        );

        if self.config.latency.is_some() {
            args.push("delay");
            args.push(&delay);
        }

        let loss = format!("{}%", self.config.plr.unwrap_or(0_f64));

        if (self.config.plr).is_some() {
            args.push("loss");
            args.push(&loss);
        }

        let status = Command::new("sudo").args(args).status()?;

        assert_status(
            status,
            "Failed to set delay and loss network emulation parameters",
        )?;

        // Add bandwidth paramteres on namespaced veth device
        //
        // The behaviour is specified on top of the root queue discipline,
        // as parent. It uses "Hierarchical Token Bucket" (HBT) discipline
        if let Some(bandwidth) = self.config.bw {
            let bandwidth = format!("{}kbit", bandwidth);
            let burst = format!("{}kbit", self.config.burst.unwrap_or(32));
            let limit = format!("{}", self.config.limit.unwrap_or(10_000));

            let status = Command::new("sudo")
                .args([
                    "ip",
                    "netns",
                    "exec",
                    &network_namespace,
                    "tc",
                    "qdisc",
                    "add",
                    "dev",
                    &veth_namespace,
                    "parent",
                    "1:",
                    "handle",
                    "2:",
                    "tbf",
                    "rate",
                    &bandwidth,
                    "burst",
                    &burst,
                    "limit",
                    &limit,
                ])
                .status()?;

            assert_status(status, "Failed to set bandwidth parameter")?;
        }

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
    fn drop(&mut self) {
        let veth_host = self.veth_host_name();
        // Deleting the network namespace where the simulated endpoint lives
        // drops everything in there
        let network_namespace = self.namespace_name();

        let _ = Command::new("sudo")
            .args(["ip", "link", "del", &veth_host])
            .status();
        let _ = Command::new("sudo")
            .args(["ip", "netns", "del", &network_namespace])
            .status();
    }

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

    use super::*;

    #[cfg(target_os = "linux")]
    #[test]
    fn start_simulation() {
        let mut simulator = Simulator::new(1);

        let addr = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1));
        let config = SimulationConfig {
            latency: Some(Duration::from_millis(2000)),
            bw: Some(1_000),
            burst: Some(32),
            limit: None,
            plr: Some(30_f64),
            protocols: vec![Protocol::TCP],
        };

        let res = simulator.start(addr, config);
        assert!(res.is_ok());
    }
}
