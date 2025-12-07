use std::{
    collections::{HashMap, HashSet},
    fmt::{Debug, Display},
    io,
    net::{IpAddr, Ipv4Addr},
    sync::atomic::{AtomicU8, Ordering},
};

use derive_more::Deref;

use crate::{
    command,
    ip::{
        self, IpAddrExt as _, MSG_SIM_NAMESPACE_PREFIX, NetworkDevice, NetworkDeviceType, Subnet,
    },
    namespace::{self, NetworkNamespace},
    tc::LinkImpairment,
};

static PEER_ID_NEXT: AtomicU8 = AtomicU8::new(0);

/// TODO: support larger sizes, current means we can have at most 256 participants in the network.
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

    pub fn reversed(self) -> Self {
        let mut copy = self;
        copy.0 = self.1;
        copy.1 = self.0;
        copy
    }
}

impl Display for Link {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}, {})", self.0, self.1)
    }
}

#[derive(Debug, Clone, Default, Deref)]
pub struct Links(HashSet<Link>);

impl Links {
    pub fn insert(&mut self, link: Link) {
        self.0.insert(link);
        self.0.insert(link.reversed());
    }
}

pub type PeerMap = HashMap<PeerId, Peer>;

#[derive(Debug, Clone)]
pub struct Peer {
    pub id: PeerId,
    pub namespace: NetworkNamespace,
}

impl Peer {
    pub fn new(id: PeerId, namespace: NetworkNamespace) -> Self {
        Self { id, namespace }
    }
}

pub trait PeerConnect {
    fn connect(&mut self, other: &mut Self, subnet: Subnet) -> command::Result<()>;
}

impl PeerConnect for Peer {
    fn connect(&mut self, other: &mut Self, subnet: Subnet) -> command::Result<()> {
        // Create veth devices
        let veth1 = NetworkDevice::new_veth(self.id.veth_address(subnet, other.id), other.id);
        let veth2 = NetworkDevice::new_veth(other.id.veth_address(subnet, self.id), self.id);

        ip::create_veth_pair(&mut self.namespace, &mut other.namespace, veth1, veth2)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("command error: {0}")]
    Command(#[from] command::Error),
    #[error("link not found: {0}")]
    LinkNotFound(Link),
    #[error("peer not found: {0}")]
    PeerNotFound(PeerId),
    #[error("too many peers")]
    TooManyPeers,
}

pub type Result<T> = std::result::Result<T, Error>;

/// The [`Network`] represents a global view of all linked devices running a network emulation.
#[derive(Debug, Clone)]
pub struct Network {
    peers: PeerMap,
    links: Links,
    subnet: Subnet,
}

impl Network {
    /// Creates a new network, where participants will be added using IP addresses from the
    /// provided subnet.
    pub fn new(subnet: Subnet) -> Self {
        Self { peers: PeerMap::default(), links: Links::default(), subnet }
    }

    /// Adds a peer to the network.
    pub fn add_peer(&mut self) -> Result<PeerId> {
        if PEER_ID_NEXT.load(Ordering::Relaxed) == PeerId::MAX {
            return Err(Error::TooManyPeers);
        }

        let peer_id = PEER_ID_NEXT.fetch_add(1, Ordering::Relaxed);
        let ns = namespace::create(&peer_id.namespace_name())?;
        ns.loopback_up()?;

        let peer = Peer::new(peer_id, ns);
        self.peers.insert(peer_id, peer);

        Ok(peer_id)
    }

    pub fn connect_to_all(&mut self, peer_id: PeerId) -> Result<()> {
        if !self.peers.contains_key(&peer_id) {
            return Err(Error::PeerNotFound(peer_id));
        };

        let other_ids = self.peers.keys().cloned().filter(|k| *k != peer_id).collect::<Vec<_>>();
        for other_id in other_ids {
            self.connect_peers(peer_id, other_id)?;
        }

        Ok(())
    }

    pub fn connect_peers(&mut self, peer_1_id: PeerId, peer_2_id: PeerId) -> Result<()> {
        if peer_1_id == peer_2_id {
            return Ok(());
        }

        if self.links.contains(&Link::new(peer_1_id, peer_2_id)) {
            return Ok(());
        }

        let [Some(peer_1), Some(peer_2)] = self.peers.get_disjoint_mut([&peer_1_id, &peer_2_id])
        else {
            unreachable!("link but not peers")
        };

        peer_1.connect(peer_2, self.subnet)?;

        self.links.insert(Link::new(peer_1_id, peer_2_id));

        tracing::debug!(peers = ?self.peers, "added peers");

        Ok(())
    }

    /// Apply a [`LinkImpairment`] to the given [`Link`]. In particular, it generates the `tc`
    /// commands to be applied to the Virtual Ethernet device used by the first end of the link,
    /// which is [`Link::0`].
    pub fn apply_impairment(&mut self, link: Link, impairment: LinkImpairment) -> Result<()> {
        if !self.links.contains(&link) {
            return Err(Error::LinkNotFound(link));
        }

        let _span = tracing::debug_span!("apply_impairment", ?link, ?impairment);

        let peer = self.peers.get(&link.0).expect("to find peer, please report bug");
        let veth = peer
            .namespace
            .devices
            .get(&NetworkDeviceType::Veth(link.1))
            .expect("to find veth device, please report bug");

        let tc_commands = impairment.to_tc_commands(&veth.variant.to_string());
        let prefix = peer.namespace.prefix_command();

        for tc_cmd in tc_commands {
            command::Runner::by_str(&format!("{} {}", prefix, tc_cmd))?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod msg_sim_network {
    use crate::{
        Subnet,
        network::{Link, Network},
        tc::LinkImpairment,
    };
    use std::net::Ipv4Addr;

    #[test]
    fn add_peer_works() {
        let _ = tracing_subscriber::fmt::try_init();
        let subnet = Subnet::new(Ipv4Addr::new(11, 0, 0, 0).into(), 16);
        let mut network = Network::new(subnet);

        let peer_1_id = network.add_peer().unwrap();
        let peer_2_id = network.add_peer().unwrap();
        let _peer_3_id = network.add_peer().unwrap();

        let result = network.connect_to_all(peer_1_id);
        assert!(result.is_ok(), "failed: {result:?}");

        let impairment = LinkImpairment::default().with_packet_loss_rate_percent(50.0);

        let result = network.apply_impairment(Link::new(peer_1_id, peer_2_id), impairment);
        assert!(result.is_ok(), "failed: {result:?}");
    }
}
