use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    fmt::Display,
    io,
    net::{IpAddr, Ipv4Addr},
};

use crate::{
    command,
    ip::{self, IpAddrExt as _, MSG_SIM_NAMESPACE_PREFIX, NetworkDevice, Subnet},
    namespace::{self, NetworkNamespace},
};

pub type PeerId = u8;

pub trait PeerIdExt: Display + Copy {
    /// Get the network namespace name derived by the provided IP address.
    /// NOTE: a namespace name can be at most 255 bytes long.
    fn namespace_name(self) -> String {
        format!("{MSG_SIM_NAMESPACE_PREFIX}-{self}")
    }

    fn veth_name(other: Self) -> String {
        format!("veth-{other}")
    }

    fn veth_address(self, subnet: Subnet, other: Self) -> IpAddr;
}

impl PeerIdExt for PeerId {
    fn veth_address(self, subnet: Subnet, other: Self) -> IpAddr {
        let octects = subnet.address.to_ipv6_mapped().octets();

        Ipv4Addr::new(octects[12], octects[13], self, other).into()
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Link(PeerId, PeerId);

impl Link {
    #[inline]
    pub fn new(a: PeerId, b: PeerId) -> Self {
        Link(a, b)
    }
}

pub type Links = HashSet<Link>;

pub type PeerMap = HashMap<PeerId, Peer>;

#[derive(Debug, Clone)]
pub struct Peer {
    pub id: PeerId,
    pub namespace: NetworkNamespace,
    pub peers: PeerMap,
}

impl Peer {
    pub fn new(id: PeerId, namespace: NetworkNamespace) -> Self {
        Self { id, namespace, peers: PeerMap::new() }
    }
}

pub trait PeerConnect {
    fn connect(&mut self, other: &mut Self, subnet: Subnet) -> command::Result<()>;
}

impl PeerConnect for Peer {
    fn connect(&mut self, other: &mut Self, subnet: Subnet) -> command::Result<()> {
        // Create veth devices
        let veth1 = NetworkDevice::new_veth(
            self.id.veth_address(subnet, other.id),
            PeerId::veth_name(other.id),
        );
        let veth2 = NetworkDevice::new_veth(
            other.id.veth_address(subnet, self.id),
            PeerId::veth_name(self.id),
        );

        ip::create_veth_pair(&mut self.namespace, &mut other.namespace, veth1, veth2)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum NetworkGraphError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("command error: {0}")]
    Command(#[from] command::Error),
}

#[derive(Debug, Clone)]
pub struct NetworkGraph {
    pub peers: PeerMap,
    pub links: Links,
    pub subnet: Subnet,
}

impl NetworkGraph {
    pub fn add_peers(
        &mut self,
        peer_1_id: PeerId,
        peer_2_id: PeerId,
    ) -> Result<(), NetworkGraphError> {
        if self.links.contains(&Link::new(peer_1_id, peer_2_id)) {
            return Ok(());
        }

        if let Entry::Vacant(v) = self.peers.entry(peer_1_id) {
            let ns = namespace::create(&peer_1_id.namespace_name())?;
            v.insert(Peer::new(peer_1_id, ns));
        }

        if let Entry::Vacant(v) = self.peers.entry(peer_2_id) {
            let ns = namespace::create(&peer_2_id.namespace_name())?;
            v.insert(Peer::new(peer_2_id, ns));
        }

        let [Some(peer_1), Some(peer_2)] = self.peers.get_disjoint_mut([&peer_1_id, &peer_2_id])
        else {
            unreachable!("checked and inserted");
        };

        peer_1.connect(peer_2, self.subnet)?;

        peer_1.namespace.loopback_up()?;
        peer_2.namespace.loopback_up()?;

        self.links.insert(Link::new(peer_1_id, peer_2_id));
        self.links.insert(Link::new(peer_2_id, peer_1_id));

        Ok(())
    }
}

#[cfg(test)]
mod msg_sim_network {
    use crate::{Simulator, Subnet};
    use std::net::Ipv4Addr;

    #[test]
    fn add_peer_works() {
        let _ = tracing_subscriber::fmt::try_init();
        let subnet = Subnet::new(Ipv4Addr::new(10, 0, 0, 0).into(), 24);
        let mut simulator = Simulator::new(subnet);

        let peer_1_id = 1;
        let peer_2_id = 2;
        let peer_3_id = 3;

        let result = simulator.network.add_peers(peer_1_id, peer_2_id);
        assert!(result.is_ok(), "failed: {result:?}");

        let result = simulator.network.add_peers(peer_1_id, peer_3_id);
        assert!(result.is_ok(), "failed: {result:?}");
    }
}
