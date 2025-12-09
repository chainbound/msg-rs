use std::{
    collections::{HashMap, HashSet},
    fmt::{Debug, Display},
    io,
    net::{IpAddr, Ipv4Addr},
    sync::atomic::{AtomicU8, Ordering},
};

use derive_more::Deref;
use rtnetlink::{LinkBridge, LinkVeth};
use tokio::task::JoinHandle;

use crate::{
    command,
    ip::{IpAddrExt as _, MSG_SIM_LINK_PREFIX, MSG_SIM_NAMESPACE_PREFIX, Subnet},
    namespace::{self, NetworkNamespace},
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

    // fn set_namespace(self) -> std::result::Result<(), Box<dyn std::error::Error>> {
    //     let name = self.namespace_name();
    //
    //     let path = format!("/run/netns/{name}\0");
    //
    //     unsafe {
    //         let fd = libc::open(path.as_bytes().as_ptr(), libc::O_RDONLY);
    //         if fd < 0 {}
    //
    //         let result = libc::setns(fd, libc::CLONE_NEWNET);
    //         if result == -1 {}
    //     }
    //
    //     Ok(())
    // }

    /// TODO: add docs.
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

#[derive(Debug)]
pub struct Peer {
    pub id: PeerId,
    pub namespace: NetworkNamespace,
}

impl Peer {
    pub fn new(id: PeerId, namespace: NetworkNamespace) -> Self {
        Self { id, namespace }
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
    #[error("rtnetlink error: {0}")]
    RtNetlink(#[from] rtnetlink::Error),
    #[error("network namespace error: {0}")]
    Namespace(#[from] namespace::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

/// The [`Network`] represents a global view of all linked devices running a network emulation.
///
/// A network is composed by peers, which are invidual nodes. Nodes can be connected to each other
/// in order to commmunicate and send messages. This is done via Virtual Ethernet devices and L2
/// bridges.
///
/// Each peer has a dedicated network namespace for complete isolation, allowing maximum
/// customizability for each of them, without interfering with the host namespace.
///
/// When a peer is added into the network, the following devices are created inside a namespace
/// `ns0`:
///
/// 1. A bridge device, e.g. `br1`
/// 2. A Veth link `veth1 <-> veth1-br`, where `veth1-br` is attached to `br1`.
///
/// When two peers are being connected, an additional link `veth1-2-br <-> veth2-1-br` in namespace
/// `ns1`, `ns2` respectively, is created. Those devices are attached to the respective switches.
#[derive(Debug)]
pub struct Network {
    peers: PeerMap,
    links: Links,
    subnet: Subnet,

    network_hub_namespace: NetworkNamespace,
    rtnetlink_handle: rtnetlink::Handle,
    _rtnetlink_socket_task: JoinHandle<()>,
}

impl Network {
    pub async fn new(subnet: Subnet) -> Result<Self> {
        let (connection, handle, _) = rtnetlink::new_connection()?;
        let _task = tokio::spawn(connection);

        let namespace_hub = NetworkNamespace::new(Self::hub_namespace_name()).await?;
        let fd = namespace_hub.fd();

        let network = Self {
            peers: PeerMap::default(),
            links: Links::default(),
            subnet,
            network_hub_namespace: namespace_hub,
            rtnetlink_handle: handle,
            _rtnetlink_socket_task: _task,
        };

        network
            .rtnetlink_handle
            .link()
            .add(LinkBridge::new("br0").up().setns_by_fd(fd).build())
            .execute()
            .await?;

        Ok(network)
    }

    fn hub_namespace_name() -> String {
        format!("{MSG_SIM_NAMESPACE_PREFIX}-hub")
    }

    /// Adds a peer to the network.
    pub async fn add_peer(&mut self) -> Result<PeerId> {
        let peer_id = PEER_ID_NEXT.load(Ordering::Relaxed);

        let namespace = format!("{MSG_SIM_NAMESPACE_PREFIX}-{peer_id}");
        let ns = NetworkNamespace::new(namespace.clone()).await?;

        let veth_name = format!("{MSG_SIM_LINK_PREFIX}-veth{peer_id}");
        let veth_br_name = format!("{MSG_SIM_LINK_PREFIX}-veth{peer_id}-br");

        let link = || LinkVeth::new(&veth_name, &veth_br_name);
        let link_reversed = || LinkVeth::new(&veth_br_name, &veth_name);

        let _span =
            tracing::debug_span!("add_peer", ?peer_id, %namespace, %veth_name, %veth_br_name)
                .entered();

        self.rtnetlink_handle
            .link()
            .add(link().build())
            .execute()
            .await
            .inspect_err(|e| tracing::debug!(?e, "failed to add link"))?;

        // NOTE: I have to specify link unfortunately and not the single device, however the first
        // end of the link is what matters;
        self.rtnetlink_handle
            .link()
            .set(link().up().setns_by_fd(ns.fd()).build())
            .execute()
            .await
            .inspect_err(|e| tracing::debug!(?e, "failed to set device up in namespace"))?;

        // TODO: use the syscall `if_nametoindex` to get the bridge interface index.
        // For now, we simply assume it's the first interface created on the network namespace and
        // as such should have interface index 5.
        let bridge_index = 5;
        self.rtnetlink_handle
            .link()
            .set(
                link_reversed()
                    .up()
                    .setns_by_fd(self.network_hub_namespace.fd())
                    .controller(bridge_index)
                    .build(),
            )
            .execute()
            .await
            .inspect_err(|e| {
                tracing::debug!(?e, "failed to set link end up in namespace with bridge controller")
            })?;

        // FIXME: add address.
        // let address =
        //     IpAddr::from_bits(self.subnet.address.to_bits().saturating_add(peer_id.into()));
        // self.rtnetlink_handle.address().add(0, address, self.subnet.mask).execute().await?;

        let peer = Peer::new(peer_id, ns);
        self.peers.insert(peer_id, peer);
        PEER_ID_NEXT.store(peer_id + 1, Ordering::Relaxed);

        Ok(peer_id)
    }

    // /// Apply a [`LinkImpairment`] to the given [`Link`]. In particular, it generates the `tc`
    // /// commands to be applied to the Virtual Ethernet device used by the first end of the link,
    // /// which is [`Link::0`].
    // pub fn apply_impairment(&mut self, link: Link, impairment: LinkImpairment) -> Result<()> {
    //     if !self.links.contains(&link) {
    //         return Err(Error::LinkNotFound(link));
    //     }
    //
    //     let _span = tracing::debug_span!("apply_impairment", ?link, ?impairment);
    //
    //     let peer = self.peers.get(&link.0).expect("to find peer, please report bug");
    //     let veth = self
    //         .peers
    //         .get(&link.0)
    //         .and_then(|peer| {
    //             peer.namespace
    //                 .devices
    //                 .iter()
    //                 .filter_map(|d| match d {
    //                     NetworkDevice::Veth(v) => Some(v),
    //                     _ => None,
    //                 })
    //                 .find(|v| v.id() == link.1)
    //         })
    //         .expect("to find veth device, please report bug");
    //
    //     let tc_commands = impairment.to_tc_commands(&veth.to_string());
    //     let prefix = peer.namespace.prefix_command();
    //
    //     for tc_cmd in tc_commands {
    //         command::Runner::by_str(&format!("{} {}", prefix, tc_cmd))?;
    //     }
    //
    //     Ok(())
    // }
}

#[cfg(test)]
mod msg_sim_network {
    use std::net::Ipv4Addr;

    use crate::{ip::Subnet, network::Network};

    #[tokio::test(flavor = "multi_thread")]
    async fn create_network_works() {
        let _ = tracing_subscriber::fmt::try_init();
        let subnet = Subnet::new(Ipv4Addr::new(11, 0, 0, 0).into(), 16);
        let _network = Network::new(subnet).await.unwrap();

        let path = format!("/run/netns/{}", Network::hub_namespace_name());
        let exists = std::fs::exists(path.clone()).unwrap();

        assert!(exists, "netns file doesn't exists at path {path}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn add_peer_works() {
        let _ = tracing_subscriber::fmt::try_init();
        let subnet = Subnet::new(Ipv4Addr::new(11, 0, 0, 0).into(), 16);
        let mut network = Network::new(subnet).await.unwrap();

        let _peer_id = network.add_peer().await.unwrap();
    }

    // #[test]
    // fn add_peer_works() {
    //     let _ = tracing_subscriber::fmt::try_init();
    //     let subnet = Subnet::new(Ipv4Addr::new(11, 0, 0, 0).into(), 16);
    //     let mut network = Network::new(subnet);
    //
    //     let peer_1_id = network.add_peer().unwrap();
    //     let peer_2_id = network.add_peer().unwrap();
    //     let _peer_3_id = network.add_peer().unwrap();
    //
    //     let result = network.connect_to_all(peer_1_id);
    //     assert!(result.is_ok(), "failed: {result:?}");
    //
    //     let impairment = LinkImpairment::default().with_packet_loss_rate_percent(50.0);
    //
    //     let result = network.apply_impairment(Link::new(peer_1_id, peer_2_id), impairment);
    //     assert!(result.is_ok(), "failed: {result:?}");
    //
    //     peer_1_id.run_in_namespace(async {
    //         // How to use the same veth to communicate with more peers? Otherwise it means I have
    //         // to bind so many times and it's not possible for certain applications.
    //         let listener = TcpListener::bind(addr)
    //     });
    // }
}
