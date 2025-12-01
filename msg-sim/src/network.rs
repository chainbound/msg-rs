use std::{
    collections::HashMap,
    fmt::Display,
    io,
    net::{IpAddr, Ipv4Addr},
    sync::{Arc, RwLock},
};

use crate::{
    VethLink as _,
    ip::{self, IpAddrExt as _, MSG_SIM_NAMESPACE_PREFIX, NetworkDevice, NetworkNamespace, Subnet},
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

pub trait PeerConnect {
    fn connect(self);
}

pub type PeerMap = HashMap<PeerId, Arc<RwLock<Peer>>>;

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

impl PeerConnect for (Arc<RwLock<Peer>>, Arc<RwLock<Peer>>) {
    fn connect(self) {
        let (peer_1, peer_2) = self;

        peer_1
            .write()
            .expect("not poisoned")
            .peers
            .insert(peer_2.read().expect("not poisoned").id, peer_2.clone());

        peer_2
            .write()
            .expect("not poisoned")
            .peers
            .insert(peer_1.read().expect("not poisoned").id, peer_1.clone());
    }
}

#[derive(Debug, thiserror::Error)]
pub enum NetworkGraphError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

#[derive(Debug, Clone)]
pub struct NetworkGraph {
    pub peers: PeerMap,
    pub subnet: Subnet,
}

impl NetworkGraph {
    pub fn add_peers(
        &mut self,
        peer_1_id: PeerId,
        peer_2_id: PeerId,
    ) -> Result<(), NetworkGraphError> {
        let peer_1 = if let Some(peer_1) = self.peers.get(&peer_1_id) {
            peer_1.clone()
        } else {
            let ns1 = ip::create_network_namespace(&peer_1_id.namespace_name())?;
            let p = Arc::new(RwLock::new(Peer::new(peer_1_id, ns1)));
            self.peers.insert(peer_1_id, p.clone());
            p
        };

        let peer_2 = if let Some(peer_2) = self.peers.get(&peer_2_id) {
            peer_2.clone()
        } else {
            let ns2 = ip::create_network_namespace(&peer_2_id.namespace_name())?;
            let p = Arc::new(RwLock::new(Peer::new(peer_2_id, ns2)));
            self.peers.insert(peer_2_id, p.clone());
            p
        };

        let veth1 = NetworkDevice::new_veth(
            peer_1_id.veth_address(self.subnet, peer_2_id),
            PeerId::veth_name(peer_2_id),
        );
        let veth2 = NetworkDevice::new_veth(
            peer_2_id.veth_address(self.subnet, peer_1_id),
            PeerId::veth_name(peer_1_id),
        );

        (&mut peer_1.write().unwrap().namespace, &mut peer_2.write().unwrap().namespace)
            .link(veth1, veth2)?;

        (peer_1, peer_2).connect();

        Ok(())
    }
}

#[cfg(test)]
mod msg_sim {
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
