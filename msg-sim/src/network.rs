use std::{
    any::Any,
    collections::HashMap,
    fmt::{Debug, Display},
    io,
    net::IpAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use futures::StreamExt as _;
use rtnetlink::{LinkBridge, LinkUnspec, LinkVeth, packet_route::tc::TcHandle};
use tokio::{
    sync::{
        mpsc,
        oneshot::{self},
    },
    task::JoinHandle,
};
use tracing::Instrument as _;

use crate::{
    dynch::DynFuture,
    ip::{IpAddrExt as _, MSG_SIM_LINK_PREFIX, MSG_SIM_NAMESPACE_PREFIX, Subnet},
    namespace::{self, NetworkNamespace},
    tc::{
        FlowerFilterRequest, LinkImpairment, QDiscNetemRequest, QdiscPrioRequest, QdiscRequestInner,
    },
    wrappers,
};

use rtnetlink::packet_core::NetlinkPayload;

static PEER_ID_NEXT: AtomicUsize = AtomicUsize::new(1);

pub type PeerId = usize;

pub trait PeerIdExt: Display + Copy {
    /// Get the network namespace name derived by the provided IP address.
    /// NOTE: a namespace name can be at most 255 bytes long.
    fn namespace_name(self) -> String {
        format!("{MSG_SIM_NAMESPACE_PREFIX}-{self}")
    }

    /// TODO: add docs.
    fn veth_address(self, subnet: Subnet) -> IpAddr;

    fn veth_name(self) -> String {
        format!("{MSG_SIM_LINK_PREFIX}{self}")
    }

    fn veth_br_name(self) -> String {
        format!("{}-br", self.veth_name())
    }
}

impl PeerIdExt for PeerId {
    fn veth_address(self, subnet: Subnet) -> IpAddr {
        IpAddr::from_bits(subnet.network_address.to_bits().saturating_add(self as u128))
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

pub type PeerMap = HashMap<PeerId, Peer<PeerContext>>;

#[derive(Debug)]
pub struct Peer<Ctx = ()> {
    pub id: PeerId,
    pub namespace: NetworkNamespace<Ctx>,
}

impl Peer {
    pub fn new<Ctx>(id: PeerId, namespace: NetworkNamespace<Ctx>) -> Peer<Ctx> {
        Peer { id, namespace }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("peer not found: {0}")]
    PeerNotFound(PeerId),
    #[error("too many peers")]
    TooManyPeers,
    #[error("rtnetlink error: {0}")]
    RtNetlink(#[from] rtnetlink::Error),
    #[error("network namespace error: {0}")]
    Namespace(#[from] namespace::Error),
    #[error("tokio join error: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("thread join error: {0:?}")]
    Thread(Box<dyn std::any::Any + Send + 'static>),
    #[error("task in namespace failed: {0}")]
    NamespaceTask(#[from] namespace::TaskError<rtnetlink::Error>),
    #[error("failed to send task: {0}")]
    SendError(#[from] mpsc::error::SendError<()>),
    #[error("failed to receive task result: {0}")]
    RecvError(#[from] oneshot::error::RecvError),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct PeerContext {
    handle: rtnetlink::Handle,
    _connection_task: tokio::task::JoinHandle<()>,

    subnet: Subnet,
    peer_id: usize,
}

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
    subnet: Subnet,

    network_hub_namespace: NetworkNamespace<PeerContext>,

    /// A [`rtnetlink::Handle`] bound to _host namespace_.
    rtnetlink_handle: rtnetlink::Handle,
    _rtnetlink_socket_task: JoinHandle<()>,
}

impl Network {
    const BRIDGE_NAME: &str = "msg-sim-br0";

    pub async fn new(subnet: Subnet) -> Result<Self> {
        let (connection, handle, _) = rtnetlink::new_connection()?;
        let _task = tokio::spawn(connection);

        let make_ctx = move || {
            let (handle, _connection_task) = rtnetlink::new_connection()
                .map(|(connection, handle, _)| (handle, tokio::task::spawn(connection)))
                .unwrap();

            PeerContext { handle, subnet, peer_id: 0, _connection_task }
        };

        let namespace_hub = NetworkNamespace::new(Self::hub_namespace_name(), make_ctx).await?;
        let fd = namespace_hub.fd();

        let network = Self {
            peers: PeerMap::default(),
            subnet,
            network_hub_namespace: namespace_hub,
            rtnetlink_handle: handle,
            _rtnetlink_socket_task: _task,
        };

        network
            .rtnetlink_handle
            .link()
            .add(LinkBridge::new(Self::BRIDGE_NAME).up().setns_by_fd(fd).build())
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
        let namespace_name = peer_id.namespace_name();
        let veth_name = Arc::new(peer_id.veth_name());
        let veth_br_name = Arc::new(peer_id.veth_br_name());

        let _span =
            tracing::debug_span!("add_peer", ?peer_id, %namespace_name, %veth_name, %veth_br_name)
                .entered();

        let subnet = self.subnet;

        let make_ctx = move || {
            let (handle, _connection_task) = rtnetlink::new_connection()
                .map(|(connection, handle, _)| (handle, tokio::task::spawn(connection)))
                .unwrap();

            PeerContext { handle, peer_id, subnet, _connection_task }
        };

        let network_namespace = NetworkNamespace::new(namespace_name.clone(), make_ctx).await?;

        // 1. Add link.
        self.rtnetlink_handle
            .link()
            .add(LinkVeth::new(&veth_name, &veth_br_name).build())
            .execute()
            .await
            .inspect_err(|e| tracing::debug!(?e, "failed to add link"))?;

        // 2. Move devices to corresponding namespaces.
        self.rtnetlink_handle
            .link()
            .set(LinkUnspec::new_with_name(&veth_name).setns_by_fd(network_namespace.fd()).build())
            .execute()
            .await
            .inspect_err(|e| tracing::debug!(?e, "failed to set device in namespace"))?;
        self.rtnetlink_handle
            .link()
            .set(
                LinkUnspec::new_with_name(&veth_br_name)
                    .setns_by_fd(self.network_hub_namespace.fd())
                    .build(),
            )
            .execute()
            .await
            .inspect_err(|e| tracing::debug!(?e, "failed to set link end in hub namespace"))?;

        // 3. Bring first device up, and assign IP address. Lastly, bring loopback up.
        let v = veth_name.clone();

        network_namespace
            .task_sender
            .submit(|ctx| {
                Box::pin(async move {
                    let address = ctx.peer_id.veth_address(ctx.subnet);
                    let mask = ctx.subnet.netmask;
                    tracing::debug!(?address, ?mask, dev = ?v, "adding address to device");
                    let index = wrappers::if_nametoindex(&v).expect("to find device").get();

                    ctx.handle
                        .link()
                        .set(LinkUnspec::new_with_name(&veth_name).up().build())
                        .execute()
                        .await?;
                    ctx.handle.address().add(index, address, mask).execute().await?;
                    ctx.handle
                        .link()
                        .set(LinkUnspec::new_with_name("lo").up().build())
                        .execute()
                        .await
                })
            })
            .await?
            .receive()
            .await??;

        // 4. Set second device controlled by hub and up.
        self.network_hub_namespace
            .task_sender
            .submit(|ctx| {
                Box::pin(async move {
                    let index =
                        wrappers::if_nametoindex(Self::BRIDGE_NAME).expect("to find bridge").get();

                    ctx.handle
                        .link()
                        .set(LinkUnspec::new_with_name(&veth_br_name).controller(index).build())
                        .execute()
                        .await?;
                    ctx.handle
                        .link()
                        .set(LinkUnspec::new_with_name(&veth_br_name).up().build())
                        .execute()
                        .await
                })
            })
            .await?
            .receive()
            .await??;

        let peer = Peer::new(peer_id, network_namespace);
        self.peers.insert(peer_id, peer);
        PEER_ID_NEXT.store(peer_id + 1, Ordering::Relaxed);

        Ok(peer_id)
    }

    pub async fn run_in_namespace<T, F>(
        &self,
        peer_id: PeerId,
        fut: F,
    ) -> Result<impl Future<Output = std::result::Result<T, oneshot::error::RecvError>>>
    where
        T: Any + Send + 'static,
        // The caller provides a function that can borrow `ctx` for some `'a`,
        // and returns a future that may also live for `'a`.
        F: for<'a> FnOnce(&'a mut PeerContext) -> DynFuture<'a, T> + Send + 'static,
    {
        let Some(peer) = self.peers.get(&peer_id) else {
            return Err(Error::PeerNotFound(peer_id));
        };

        let rx = peer.namespace.task_sender.submit(fut).await?.receive();
        Ok(rx)
    }

    /// Apply a [`LinkImpairment`] to the given [`Link`]. In particular, it generates the `tc`
    /// commands to be applied to the Virtual Ethernet device used by the first end of the link,
    /// which is [`Link::0`].
    pub async fn apply_impairment(&mut self, link: Link, impairment: LinkImpairment) -> Result<()> {
        let (p1, p2) = match self.peers.get_disjoint_mut([&link.0, &link.1]) {
            [Some(p1), Some(p2)] => (p1, p2),
            [None, Some(_)] => return Err(Error::PeerNotFound(link.0)),
            [Some(_), None] => return Err(Error::PeerNotFound(link.1)),
            [None, None] => return Err(Error::PeerNotFound(link.0)),
        };

        let p2_id = p2.id;

        p1.namespace
            .task_sender
            .submit(move |ctx| {
                let span = tracing::debug_span!("apply_impairment", ?link, ?impairment).entered();
                {
                    Box::pin(
                        async move {
                            let index = wrappers::if_nametoindex(&ctx.peer_id.veth_name())
                                .expect("to find dev")
                                .get() as i32;

                            let prio_request = QdiscPrioRequest::new(
                                QdiscRequestInner::new(index)
                                    .with_handle(TcHandle::from(0x0001_0000)),
                            )
                            .build();

                            let mut res = ctx.handle.request(prio_request)?;
                            while let Some(res) = res.next().await {
                                if let NetlinkPayload::Error(e) = res.payload {
                                    tracing::debug!(?e, "failed to create prio qdisc");
                                    return Err(rtnetlink::Error::NetlinkError(e));
                                }
                            }

                            let netem_request = QDiscNetemRequest::new(
                                QdiscRequestInner::new(index)
                                    .with_parent(TcHandle::from(0x0001_0003))
                                    .with_handle(TcHandle::from(0x0003_0000)),
                                impairment,
                            )
                            .build();

                            let mut res = ctx.handle.request(netem_request)?;
                            while let Some(res) = res.next().await {
                                tracing::debug!(?res, "received res");
                                if let NetlinkPayload::Error(e) = res.payload {
                                    tracing::debug!(?e, "failed to add netem");
                                    return Err(rtnetlink::Error::NetlinkError(e));
                                }
                            }

                            let request = FlowerFilterRequest::new(
                                QdiscRequestInner::new(index)
                                    .with_parent(TcHandle::from(0x0001_0000)),
                                p2_id.veth_address(ctx.subnet),
                            )
                            .build();

                            let mut res = ctx.handle.request(request)?;
                            while let Some(msg) = res.next().await {
                                if let NetlinkPayload::Error(e) = msg.payload {
                                    tracing::debug!(?e, "failed to add filter");
                                    return Err(rtnetlink::Error::NetlinkError(e));
                                }
                            }

                            Ok(())
                        }
                        .instrument(span.clone()),
                    )
                }
            })
            .await?
            .receive()
            .await??;

        Ok(())
    }
}

#[cfg(test)]
mod msg_sim_network {
    use std::net::{Ipv4Addr, SocketAddr};

    use futures::StreamExt;
    use msg_socket::{RepSocket, ReqSocket};
    use msg_transport::tcp::Tcp;

    use crate::{
        ip::Subnet,
        network::{Link, Network, PeerIdExt},
        tc::LinkImpairment,
    };

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
        let subnet = Subnet::new(Ipv4Addr::new(12, 0, 0, 0).into(), 16);
        let mut network = Network::new(subnet).await.unwrap();

        let _peer_id = network.add_peer().await.unwrap();
        let _peer_id = network.add_peer().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn simulate_reqrep_works() {
        let _ = tracing_subscriber::fmt::try_init();

        let subnet = Subnet::new(Ipv4Addr::new(13, 0, 0, 0).into(), 16);
        let mut network = Network::new(subnet).await.unwrap();

        let peer_1 = network.add_peer().await.unwrap();
        let peer_2 = network.add_peer().await.unwrap();

        let address_2 = peer_2.veth_address(subnet);
        let port_2 = 12345;

        let task1 = network
            .run_in_namespace(peer_2, move |_| {
                Box::pin(async move {
                    let mut rep_socket = RepSocket::new(Tcp::default());
                    rep_socket.bind(SocketAddr::new(address_2, port_2)).await.unwrap();

                    if let Some(request) = rep_socket.next().await {
                        let msg = request.msg().clone();
                        request.respond(msg).unwrap();
                    }
                })
            })
            .await
            .unwrap();

        let task2 = network
            .run_in_namespace(peer_1, move |_| {
                Box::pin(async move {
                    let mut req_socket = ReqSocket::new(Tcp::default());

                    req_socket.connect_sync(SocketAddr::new(address_2, port_2));
                    let response = req_socket.request("hello".into()).await.unwrap();
                    assert_eq!(response, "hello");
                })
            })
            .await
            .unwrap();

        tokio::try_join!(task1, task2).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn apply_impairment_works() {
        let _ = tracing_subscriber::fmt::try_init();
        let subnet = Subnet::new(Ipv4Addr::new(12, 0, 0, 0).into(), 16);
        let mut network = Network::new(subnet).await.unwrap();

        let peer_1 = network.add_peer().await.unwrap();
        let peer_2 = network.add_peer().await.unwrap();
        let _peer_3 = network.add_peer().await.unwrap();

        let impairment = LinkImpairment {
            loss: LinkImpairment::loss_probability(50.0),
            jitter: 100_000,
            latency: 1_000_000,
            duplicate: LinkImpairment::loss_probability(50.0),
            ..Default::default()
        };
        network.apply_impairment(Link::new(peer_1, peer_2), impairment).await.unwrap();
    }
}
