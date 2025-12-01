#![doc(issue_tracker_base_url = "https://github.com/chainbound/msg-rs/issues/")]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

//! Man(4) veth is a short introduction on the topic.

use std::{io, net::IpAddr, time::Duration};

mod protocol;
pub use protocol::Protocol;

use crate::{
    ip::{NetworkDevice, NetworkNamespace, Subnet},
    network::{NetworkGraph, PeerId, PeerMap},
};

pub mod namespace;

pub mod assert;
pub mod ip;
pub mod ip_tc;
pub mod network;

/// The impairments that can be applied to a network link.
#[derive(Debug, Clone)]
pub struct LinkImpairment {
    /// The latency of outgoing packets.
    pub latency: Option<Duration>,
    /// The maximum bandwidth in Kbps.
    pub bandwidth_kbps: Option<u64>,
    /// The maximum burst size in kbit.
    pub burst_kbit: Option<u64>,
    /// The buffer size in bytes.
    pub buffer_size_bytes: Option<u64>,
    /// The packet loss rate in percent.
    pub packet_loss_rate_percent: Option<f64>,
    /// The supported protocols.
    pub protocols: Vec<Protocol>,
}

/// A simulation represents the [`LinkImpairment`]s, applied to a
/// certain link with a given IP address.
#[derive(Debug, Clone)]
pub struct Simulation {
    /// An unique identifier for this simulation.
    pub id: u8,
    /// The idenfiter of the [`Simulator`] which created this simulation.
    pub simulator_id: u8,
    /// The IP address of the link for which some [`LinkImpairment`] have been applied.
    pub endpoint: IpAddr,
    /// The impairments applied to the link.
    pub impairments: LinkImpairment,
}

impl Simulation {
    fn new(id: u8, simulator_id: u8, endpoint: IpAddr, config: LinkImpairment) -> Self {
        Self { id, simulator_id, endpoint, impairments: config }
    }
}

impl Simulation {
    /// Get the namespace name associated to the link, based on the [`Self::simulator_id`] and
    /// [`Self::id`].
    #[inline]
    pub fn namespace_name(&self) -> String {
        format!("msg-{}-{}", self.simulator_id, self.id)
    }

    /// Get the name of the Virtual Ethernet device used within the namespace, based on the
    /// [`Self::simulator_id`] and [`Self::id`].
    #[inline]
    pub fn veth_name(&self) -> String {
        // NOTE: a network device name can be at most 15 chars long. Given a [`Self::simulator_id`]
        // and [`Self::id`] size of one byte, this name will always fit the size constraints.
        format!("vn-msg-{}-{}", self.simulator_id, self.id)
    }

    // Starts the simulation.
    // fn start(&mut self) -> io::Result<()> {
    //     // Create network namespace
    //
    //     let network_namespace = self.namespace_name();
    //     let veth_namespace = self.veth_name();
    //     let ip_namespace = format!("{}/24", self.endpoint);
    //
    //     ip_tc::create_namespace(&network_namespace)?;
    //     ip_tc::create_veth_pair(&veth_host, &veth_namespace)?;
    //     ip_tc::move_device_to_namespace(&veth_namespace, &network_namespace)?;
    //
    //     let ip_host = ip_tc::gen_host_ip_address(&self.endpoint);
    //
    //     ip_tc::add_ip_addr_to_device(&veth_host, &ip_host, None)?;
    //     ip_tc::spin_up_device(&veth_host, None)?;
    //
    //     ip_tc::add_ip_addr_to_device(&veth_namespace, &ip_namespace, Some(&network_namespace))?;
    //     ip_tc::spin_up_device(&veth_namespace, Some(&network_namespace))?;
    //     ip_tc::spin_up_device("lo", Some(&network_namespace))?;
    //
    //     let delay =
    //         format!("{}ms", self.impairments.latency.unwrap_or(Duration::new(0, 0)).as_millis());
    //
    //     let loss = format!("{}%", self.impairments.packet_loss_rate_percent.unwrap_or(0_f64));
    //
    //     // Add delay to the host veth device to match MacOS symmetric behaviour
    //     //
    //     // The behaviour is specified on the top-level ("root"),
    //     // with a custom handle for identification
    //     let mut args = vec!["root", "handle", "1:", "netem"];
    //     if self.impairments.latency.is_some() {
    //         args.push("delay");
    //         args.push(&delay);
    //     }
    //     ip_tc::add_network_emulation_parameters(&veth_host, args, None)?;
    //
    //     // Add network emulation parameters (delay, loss) on namespaced veth device
    //     let mut args = vec!["root", "handle", "1:", "netem"];
    //     if self.impairments.latency.is_some() {
    //         args.push("delay");
    //         args.push(&delay);
    //     }
    //     if (self.impairments.packet_loss_rate_percent).is_some() {
    //         args.push("loss");
    //         args.push(&loss);
    //     }
    //     ip_tc::add_network_emulation_parameters(&veth_namespace, args, Some(&network_namespace))?;
    //
    //     // Add bandwidth paramteres on namespaced veth device
    //     //
    //     // The behaviour is specified on top of the root queue discipline,
    //     // as parent. It uses "Hierarchical Token Bucket" (HBT) discipline
    //     if let Some(bandwidth) = self.impairments.bandwidth_kbps {
    //         let bandwidth = format!("{}kbit", bandwidth);
    //         let burst = format!("{}kbit", self.impairments.burst_kbit.unwrap_or(32));
    //         let limit = format!("{}", self.impairments.buffer_size_bytes.unwrap_or(10_000));
    //
    //         let args = vec![
    //             "parent", "1:", "handle", "2:", "tbf", "rate", &bandwidth, "burst", &burst,
    //             "limit", &limit,
    //         ];
    //
    //         ip_tc::add_network_emulation_parameters(&veth_host, args.clone(), None)?;
    //         ip_tc::add_network_emulation_parameters(
    //             &veth_namespace,
    //             args,
    //             Some(&network_namespace),
    //         )?;
    //     }
    //     Ok(())
    // }
}

impl Drop for Simulation {
    fn drop(&mut self) {
        // let veth_host = self.veth_host_name();
        // let network_namespace = self.namespace_name();
        //
        // // The only thing we have to do in the host to delete the veth device
        // let _ = Command::new("sudo").args(["ip", "link", "del", &veth_host]).status();
        // // Deleting the network namespace where the simulated endpoint lives
        // // drops everything in there
        // let _ = Command::new("sudo").args(["ip", "netns", "del", &network_namespace]).status();
    }
}

#[derive(Debug)]
pub struct Simulator {
    pub network: NetworkGraph,
}

impl Simulator {
    pub fn new(subnet: Subnet) -> Self {
        Self { network: NetworkGraph { peers: PeerMap::new(), subnet } }
    }

    pub fn add_peers(&mut self, peer_1: PeerId, peer_2: PeerId) {
        if let Err(e) = self.network.add_peers(peer_1, peer_2) {
            tracing::error!(?e, ?peer_1, ?peer_2, "failed to add peers");
        }
    }
}

pub trait VethLink {
    fn link(self, veth1: NetworkDevice, veth2: NetworkDevice) -> io::Result<()>;
}

impl VethLink for (&mut NetworkNamespace, &mut NetworkNamespace) {
    fn link(self, veth1: NetworkDevice, veth2: NetworkDevice) -> io::Result<()> {
        ip::create_veth_pair(self.0, self.1, veth1, veth2)
    }
}

//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
