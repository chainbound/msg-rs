//! # Network Emulation Infrastructure
//!
//! This module provides the core infrastructure for creating isolated network environments
//! with configurable impairments. It enables testing distributed systems under various
//! network conditions like latency, packet loss, and bandwidth constraints.
//!
//! ## Architecture
//!
//! The network simulation uses Linux network namespaces and virtual ethernet devices:
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────┐
//! │                           Hub Namespace (msg-sim-hub)                       │
//! │                                                                             │
//! │   ┌─────────────────────────────────────────────────────────────────────┐   │
//! │   │                      Bridge (msg-sim-br0)                           │   │
//! │   │                                                                     │   │
//! │   │   Acts as a virtual switch connecting all peer veth endpoints       │   │
//! │   └─────────────────────────────────────────────────────────────────────┘   │
//! │          │                    │                    │                        │
//! │    msg-veth1-br         msg-veth2-br         msg-veth3-br                   │
//! └──────────┼────────────────────┼────────────────────┼────────────────────────┘
//!            │                    │                    │
//!   ═══════════════      ═══════════════      ═══════════════
//!      veth pair            veth pair            veth pair
//!   ═══════════════      ═══════════════      ═══════════════
//!            │                    │                    │
//! ┌──────────┼─────────┐ ┌────────┼─────────┐ ┌────────┼─────────┐
//! │    msg-veth1       │ │  msg-veth2       │ │  msg-veth3       │
//! │                    │ │                  │ │                  │
//! │  Peer 1 Namespace  │ │ Peer 2 Namespace │ │ Peer 3 Namespace │
//! │  (msg-sim-1)       │ │ (msg-sim-2)      │ │ (msg-sim-3)      │
//! │                    │ │                  │ │                  │
//! │  IP: 10.0.0.1      │ │ IP: 10.0.0.2     │ │ IP: 10.0.0.3     │
//! │                    │ │                  │ │                  │
//! │  ┌──────────────┐  │ │ ┌──────────────┐ │ │ ┌──────────────┐ │
//! │  │ TC Hierarchy │  │ │ │ TC Hierarchy │ │ │ │ TC Hierarchy │ │
//! │  │ (per-dest    │  │ │ │ (per-dest    │ │ │ │ (per-dest    │ │
//! │  │ impairments) │  │ │ │ impairments) │ │ │ │ impairments) │ │
//! │  └──────────────┘  │ │ └──────────────┘ │ │ └──────────────┘ │
//! └────────────────────┘ └──────────────────┘ └──────────────────┘
//! ```
//!
//! ## Per-Destination Impairments
//!
//! Each peer can have different network conditions when communicating with different
//! destinations. For example, peer 1 might have:
//! - 10ms latency + 100 Mbit/s to peer 2
//! - 200ms latency + 5% loss to peer 3
//!
//! This is achieved using a DRR (Deficit Round Robin) qdisc with per-destination
//! classes. See the [`crate::tc`] module for details on the qdisc hierarchy.

use std::{
    collections::{HashMap, HashSet},
    fmt::{Debug, Display},
    io,
    net::IpAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use rtnetlink::{LinkBridge, LinkUnspec, LinkVeth};
use tokio::{
    sync::{
        mpsc,
        oneshot::{self},
    },
    task::JoinHandle,
};
use tracing::Instrument as _;

use crate::tc::requests::configure_drr_class;
use crate::tc::requests::configure_tbf;
use crate::tc::requests::{configure_flower_filter, configure_netem, install_drr_root};
use crate::{
    dynch::DynFuture,
    ip::{IpAddrExt as _, Subnet},
    namespace::{self, NetworkNamespace},
    tc::impairment::LinkImpairment,
    wrappers,
};

/// Global counter for generating unique peer IDs.
///
/// Peer IDs start at 1 (not 0) to ensure valid IP addresses when added to the subnet.
static PEER_ID_NEXT: AtomicUsize = AtomicUsize::new(1);

/// The type used to identify peers within the network.
///
/// Peer IDs are monotonically increasing integers starting from 1.
pub type PeerId = usize;

/// Prefix for all network namespace names created by this crate.
pub const MSG_SIM_NAMESPACE_PREFIX: &str = "msg-sim";

/// Prefix for all virtual ethernet device names created by this crate.
pub const MSG_SIM_LINK_PREFIX: &str = "msg-veth";

/// Extension trait for peer IDs providing namespace and device naming utilities.
pub trait PeerIdExt: Display + Copy {
    /// Get the network namespace name for this peer.
    fn namespace_name(self) -> String {
        format!("{MSG_SIM_NAMESPACE_PREFIX}-{self}")
    }

    /// Compute the IP address for this peer's veth device within the given subnet.
    fn veth_address(self, subnet: Subnet) -> IpAddr;

    /// Get the name of the veth device inside the peer's namespace.
    fn veth_name(self) -> String {
        format!("{MSG_SIM_LINK_PREFIX}{self}")
    }

    /// Get the name of the veth device endpoint attached to the hub bridge.
    fn veth_br_name(self) -> String {
        format!("{}-br", self.veth_name())
    }
}

impl PeerIdExt for PeerId {
    fn veth_address(self, subnet: Subnet) -> IpAddr {
        IpAddr::from_bits(subnet.network_address.to_bits().saturating_add(self as u128))
    }
}

/// A directed link between two peers.
///
/// `Link(A, B)` represents traffic flowing from peer A to peer B. Links are used to specify
/// [`LinkImpairment`]s. Impairments applied to this link affect only A→B traffic, not B→A.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Link(pub PeerId, pub PeerId);

impl Link {
    /// Create a new directed link from source to destination.
    #[inline]
    pub fn new(source: impl Into<PeerId>, destination: impl Into<PeerId>) -> Self {
        Link(source.into(), destination.into())
    }

    /// Get the source peer (traffic originates here).
    #[inline]
    pub fn source(&self) -> PeerId {
        self.0
    }

    /// Get the destination peer (traffic flows to here).
    #[inline]
    pub fn destination(&self) -> PeerId {
        self.1
    }
}

impl Display for Link {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({} → {})", self.0, self.1)
    }
}

/// Tracks the traffic control state for a single peer's veth device.
///
/// This struct keeps track of what qdisc hierarchy has been set up on the peer's
/// outgoing interface, enabling us to add/remove per-destination impairments.
#[derive(Debug, Default)]
struct PeerTcState {
    /// Whether the drr root qdisc has been installed on this peer's veth.
    ///
    /// The drr qdisc is installed lazily on first `apply_impairment()` call.
    drr_installed: bool,

    /// Set of destination peer IDs that have impairments configured.
    ///
    /// For each destination in this set, we have created:
    /// - An drr class (for traffic classification)
    /// - Optionally a TBF qdisc (for bandwidth limiting)
    /// - A netem qdisc (for delay, loss, etc.)
    /// - A flower filter (to match destination IP)
    configured_destinations: HashSet<PeerId>,
}

impl PeerTcState {
    /// Check if impairments are configured for traffic to the given destination.
    fn has_impairment_to(&self, dest: PeerId) -> bool {
        self.configured_destinations.contains(&dest)
    }

    /// Record that impairments have been configured for traffic to the given destination.
    fn mark_configured(&mut self, dest: PeerId) {
        self.configured_destinations.insert(dest);
    }
}

/// Map from peer ID to peer instance.
pub type PeerMap = HashMap<PeerId, Peer<PeerContext>>;

/// Map from peer ID to traffic control state.
type TcStateMap = HashMap<PeerId, PeerTcState>;

/// A peer in the simulated network.
///
/// Each peer runs in its own network namespace with an isolated network stack.
#[derive(Debug)]
pub struct Peer<Ctx = ()> {
    /// Unique identifier for this peer.
    pub id: PeerId,
    /// The network namespace this peer runs in.
    pub namespace: NetworkNamespace<Ctx>,
}

impl Peer {
    /// Create a new peer with the given ID and namespace.
    pub fn new<Ctx>(id: PeerId, namespace: NetworkNamespace<Ctx>) -> Peer<Ctx> {
        Peer { id, namespace }
    }
}

pub(crate) type RuntimeMakerFn = Box<dyn FnOnce() -> tokio::runtime::Runtime + Send>;

/// Common context provided to all namespaces.
///
/// This context gives access to rtnetlink for network configuration.
#[derive(Debug)]
pub struct CommonContext {
    /// Handle for sending rtnetlink messages within this namespace.
    handle: rtnetlink::Handle,
    /// Background task processing rtnetlink responses.
    _connection_task: tokio::task::JoinHandle<()>,
}

pub struct HubOptions {
    make_runtime: RuntimeMakerFn,
}

impl Default for HubOptions {
    fn default() -> Self {
        Self {
            make_runtime: Box::new(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .expect("to create runtime")
            }),
        }
    }
}

/// Context provided to tasks running within a peer's namespace.
///
/// This context gives access to rtnetlink for network configuration
/// and metadata about the peer's position in the network.
#[derive(Debug)]
pub struct PeerContext {
    /// Handle for sending rtnetlink messages within this namespace.
    pub handle: rtnetlink::Handle,
    /// Background task processing rtnetlink responses.
    _connection_task: tokio::task::JoinHandle<()>,
    /// The subnet this network uses.
    pub subnet: Subnet,
    /// This peer's ID.
    pub peer_id: PeerId,
}

/// Options for configuring a peer.
pub struct PeerOptions {
    make_runtime: RuntimeMakerFn,
}

impl Default for PeerOptions {
    fn default() -> Self {
        Self {
            make_runtime: Box::new(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .expect("to create runtime")
            }),
        }
    }
}

impl PeerOptions {
    /// Create new peer options with a custom runtime factory.
    pub fn with_runtime(
        make_runtime: impl FnOnce() -> tokio::runtime::Runtime + Send + 'static,
    ) -> Self {
        Self { make_runtime: Box::new(make_runtime) }
    }
}

// -------------------------------------------------------------------------------------
// Error Handling
// -------------------------------------------------------------------------------------

/// Errors that can occur during network simulation.
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

/// Result type for network operations.
pub type Result<T> = std::result::Result<T, Error>;

// -------------------------------------------------------------------------------------
// Network Implementation
// -------------------------------------------------------------------------------------

/// A simulated network with configurable topology and impairments.
///
/// The `Network` struct manages:
/// - A central hub namespace with a bridge device
/// - Multiple peer namespaces connected via virtual ethernet pairs
/// - Per-link network impairments (latency, loss, bandwidth limits)
///
/// # Example
///
/// ```no_run
/// use msg_sim::network::{Network, Link, HubOptions, PeerOptions};
/// use msg_sim::tc::impairment::LinkImpairment;
/// use msg_sim::ip::Subnet;
/// use std::net::Ipv4Addr;
///
/// #[tokio::main]
/// async fn main() {
///     // Create a network with a /16 subnet
///     let subnet = Subnet::new(Ipv4Addr::new(10, 0, 0, 0).into(), 16);
///     let mut network = Network::new(subnet, HubOptions::default()).await.unwrap();
///
///     // Add some peers
///     let peer_1 = network.add_peer(PeerOptions::default()).await.unwrap();
///     let peer_2 = network.add_peer(PeerOptions::default()).await.unwrap();
///     let peer_3 = network.add_peer(PeerOptions::default()).await.unwrap();
///
///     // Configure different impairments for different paths
///     network.apply_impairment(
///         Link::new(peer_1, peer_2),
///         LinkImpairment {
///             latency: 10_000,           // 10ms to peer 2
///             bandwidth_mbit_s: Some(100.0), // 100 Mbit/s
///             ..Default::default()
///         },
///     ).await.unwrap();
///
///     network.apply_impairment(
///         Link::new(peer_1, peer_3),
///         LinkImpairment {
///             latency: 200_000,          // 200ms to peer 3
///             loss: 5.0,                 // 5% packet loss
///             ..Default::default()
///         },
///     ).await.unwrap();
///
///     // Run tasks in peer namespaces
///     network.run_in_namespace(peer_1, |ctx| {
///         Box::pin(async move {
///             // Network code here sees the configured impairments
///         })
///     }).await.unwrap();
/// }
/// ```
#[derive(Debug)]
pub struct Network {
    /// All peers in this network, keyed by peer ID.
    peers: PeerMap,

    /// Traffic control state for each peer's veth device.
    tc_state: TcStateMap,

    /// The IP subnet used by this network.
    subnet: Subnet,

    /// The hub namespace containing the bridge device.
    network_hub_namespace: NetworkNamespace<CommonContext>,

    /// Rtnetlink handle bound to the host namespace.
    ///
    /// Used for creating veth pairs and moving devices between namespaces.
    rtnetlink_handle: rtnetlink::Handle,

    /// Background task for the host rtnetlink connection.
    _rtnetlink_socket_task: JoinHandle<()>,
}

impl Network {
    /// Name of the bridge device in the hub namespace.
    const BRIDGE_NAME: &str = "msg-sim-br0";

    /// Create a new simulated network with the given IP subnet.
    ///
    /// This creates:
    /// 1. A hub network namespace (`msg-sim-hub`)
    /// 2. A bridge device (`msg-sim-br0`) in the hub namespace
    ///
    /// Peers can then be added with [`add_peer`](Self::add_peer).
    pub async fn new(subnet: Subnet, options: HubOptions) -> Result<Self> {
        // Create rtnetlink connection in the host namespace.
        // This is used for creating veth pairs and moving devices.
        let (connection, handle, _) = rtnetlink::new_connection()?;
        let _task = tokio::spawn(connection);

        // Factory function for creating namespace contexts.
        // Each namespace needs its own rtnetlink connection.
        let make_ctx = move || {
            let (handle, _connection_task) = rtnetlink::new_connection()
                .map(|(connection, handle, _)| (handle, tokio::task::spawn(connection)))
                .unwrap();

            CommonContext { handle, _connection_task }
        };

        // Create the hub namespace that will host the bridge.
        let namespace_hub =
            NetworkNamespace::new(Self::hub_namespace_name(), options.make_runtime, make_ctx)
                .await?;
        let fd = namespace_hub.fd();

        let network = Self {
            peers: PeerMap::default(),
            tc_state: TcStateMap::default(),
            subnet,
            network_hub_namespace: namespace_hub,
            rtnetlink_handle: handle,
            _rtnetlink_socket_task: _task,
        };

        // Create the bridge device in the hub namespace.
        // All peer veth endpoints will attach to this bridge.
        network
            .rtnetlink_handle
            .link()
            .add(LinkBridge::new(Self::BRIDGE_NAME).up().setns_by_fd(fd).build())
            .execute()
            .await?;

        Ok(network)
    }

    /// Get the name of the hub namespace.
    fn hub_namespace_name() -> String {
        format!("{MSG_SIM_NAMESPACE_PREFIX}-hub")
    }

    /// Add a new peer to the network.
    ///
    /// This creates:
    /// 1. A new network namespace for the peer
    /// 2. A veth pair connecting the peer to the hub bridge
    /// 3. IP address assignment based on the subnet and peer ID
    pub async fn add_peer(&mut self, options: PeerOptions) -> Result<PeerId> {
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
                .expect("to create rtnetlink socket");

            PeerContext { handle, _connection_task, subnet, peer_id }
        };

        let network_namespace =
            NetworkNamespace::new(namespace_name.clone(), options.make_runtime, make_ctx).await?;

        // Step 1: Create the veth pair in the host namespace.
        // One end (veth_name) will go to the peer, the other (veth_br_name) to the bridge.
        self.rtnetlink_handle
            .link()
            .add(LinkVeth::new(&veth_name, &veth_br_name).build())
            .execute()
            .await
            .inspect_err(|e| tracing::debug!(?e, "failed to add link"))?;

        // Step 2: Move veth endpoints to their respective namespaces.
        // The peer's veth goes into the peer's namespace.
        self.rtnetlink_handle
            .link()
            .set(LinkUnspec::new_with_name(&veth_name).setns_by_fd(network_namespace.fd()).build())
            .execute()
            .await
            .inspect_err(|e| tracing::debug!(?e, "failed to set device in namespace"))?;

        // The bridge endpoint goes into the hub namespace.
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

        // Step 3: Configure the peer's veth interface.
        // Bring it up, assign IP address, and bring up loopback.
        let v = veth_name.clone();

        network_namespace
            .task_sender
            .submit(|ctx: &mut PeerContext| {
                Box::pin(async move {
                    let address = ctx.peer_id.veth_address(ctx.subnet);
                    let mask = ctx.subnet.netmask;
                    tracing::debug!(?address, ?mask, dev = ?v, "adding address to device");
                    let index = wrappers::if_nametoindex(&v).expect("to find device").get();

                    // Bring the veth interface up
                    ctx.handle
                        .link()
                        .set(LinkUnspec::new_with_name(&veth_name).up().build())
                        .execute()
                        .await?;

                    // Assign the IP address
                    ctx.handle.address().add(index, address, mask).execute().await?;

                    // Bring loopback up (needed for localhost communication)
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

        // Step 4: Attach the bridge endpoint to the hub's bridge device.
        self.network_hub_namespace
            .task_sender
            .submit(|ctx| {
                Box::pin(async move {
                    let index =
                        wrappers::if_nametoindex(Self::BRIDGE_NAME).expect("to find bridge").get();

                    // Set the bridge as the controller for this veth endpoint
                    ctx.handle
                        .link()
                        .set(LinkUnspec::new_with_name(&veth_br_name).controller(index).build())
                        .execute()
                        .await?;

                    // Bring the bridge endpoint up
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

        // Record the new peer
        let peer = Peer::new(peer_id, network_namespace);
        self.peers.insert(peer_id, peer);
        self.tc_state.insert(peer_id, PeerTcState::default());
        PEER_ID_NEXT.store(peer_id + 1, Ordering::Relaxed);

        Ok(peer_id)
    }

    /// Run a task in a peer's network namespace.
    ///
    /// The provided closure receives a mutable reference to the namespace's context,
    /// which can be used for rtnetlink operations. The task runs in the peer's
    /// isolated network environment, seeing only that peer's network configuration.
    ///
    /// # Arguments
    ///
    /// * `peer_id` - The peer whose namespace to run in
    /// * `fut` - A closure that returns a future to execute
    ///
    /// # Returns
    ///
    /// A future that resolves to the task's result.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use msg_sim::ip::Subnet;
    /// use msg_sim::network::{Network, HubOptions, PeerOptions};
    /// use std::net::Ipv4Addr;
    /// use tokio::net::TcpListener;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let subnet = Subnet::new(Ipv4Addr::new(12, 0, 0, 0).into(), 16);
    ///     let mut network = Network::new(subnet, HubOptions::default()).await.unwrap();
    ///
    ///     let peer_id = network.add_peer(PeerOptions::default()).await.unwrap();
    ///     network
    ///         .run_in_namespace(peer_id, |_ctx| {
    ///             Box::pin(async move {
    ///                 // This code runs in peer's network namespace
    ///                 let socket = TcpListener::bind("0.0.0.0:8080").await?;
    ///                 // ... handle connections
    ///                 Ok::<_, std::io::Error>(())
    ///             })
    ///         })
    ///         .await.unwrap()
    ///         .await.unwrap();
    /// }
    /// ```
    pub async fn run_in_namespace<T, F>(
        &self,
        peer_id: PeerId,
        fut: F,
    ) -> Result<impl Future<Output = std::result::Result<T, oneshot::error::RecvError>>>
    where
        T: Send + 'static,
        F: for<'a> FnOnce(&'a mut PeerContext) -> DynFuture<'a, T> + Send + 'static,
    {
        let Some(peer) = self.peers.get(&peer_id) else {
            return Err(Error::PeerNotFound(peer_id));
        };

        let rx = peer.namespace.task_sender.submit(fut).await?.receive();
        Ok(rx)
    }

    /// Apply network impairments to a directed link between two peers.
    ///
    /// This configures traffic control on the source peer's veth interface to
    /// impair traffic destined for the destination peer. The impairments only
    /// affect traffic in one direction (source → destination).
    ///
    /// # Traffic Control Hierarchy
    ///
    /// On first call for a peer, this installs an drr root qdisc. Then for each
    /// destination, it creates:
    ///
    /// ```text
    /// drr root (1:0)
    ///   └── drr class (1:10+X) for destination peer X
    ///         └── TBF (10+X:1) [if bandwidth limiting enabled]
    ///               └── netem (10+X:0) [delay, loss, jitter]
    ///
    /// Flower filter: dst IP = peer X -> route to class 1:10+X
    /// ```
    ///
    /// # Replacing Impairments
    ///
    /// If impairments already exist for the given link, they will be replaced with the new
    /// configuration. This allows dynamically changing network conditions during a test.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use msg_sim::{
    ///     ip::Subnet,
    ///     network::{Link, Network, HubOptions, PeerOptions},
    ///     tc::impairment::LinkImpairment
    /// };
    ///
    /// use std::net::Ipv4Addr;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let subnet = Subnet::new(Ipv4Addr::new(12, 0, 0, 0).into(), 16);
    ///     let mut network = Network::new(subnet, HubOptions::default()).await.unwrap();
    ///
    ///     let peer_1 = network.add_peer(PeerOptions::default()).await.unwrap();
    ///     let peer_2 = network.add_peer(PeerOptions::default()).await.unwrap();
    ///
    ///     // Simulate a slow, lossy link from peer 1 to peer 2
    ///     network.apply_impairment(
    ///         Link::new(peer_1, peer_2),
    ///         LinkImpairment {
    ///             latency: 100_000,            // 100ms delay
    ///             jitter: 20_000,              // ±20ms variation
    ///             loss: 2.0,                   // 2% packet loss
    ///             bandwidth_mbit_s: Some(10.0),  // 10 Mbit/s cap
    ///             ..Default::default()
    ///         },
    ///     ).await.unwrap();
    /// }
    /// ```
    pub async fn apply_impairment(&mut self, link: Link, impairment: LinkImpairment) -> Result<()> {
        // Get references to both peers, ensuring they exist.
        let (src_peer, dst_peer) =
            match self.peers.get_disjoint_mut([&link.source(), &link.destination()]) {
                [Some(p1), Some(p2)] => (p1, p2),
                [None, Some(_)] => return Err(Error::PeerNotFound(link.source())),
                [Some(_), None] => return Err(Error::PeerNotFound(link.destination())),
                [None, None] => return Err(Error::PeerNotFound(link.source())),
            };

        let tc_state = self.tc_state.entry(link.source()).or_default();

        // Check if impairments already exist for this destination.
        // If so, we'll replace them instead of creating new ones.
        let is_replacement = tc_state.has_impairment_to(link.destination());

        let dst_peer_id = dst_peer.id;
        let drr_already_installed = tc_state.drr_installed;
        let subnet = self.subnet;

        // Execute the TC configuration in the source peer's namespace.
        src_peer
            .namespace
            .task_sender
            .submit(move |ctx: &mut PeerContext| {
                let span = tracing::debug_span!(
                    "apply_impairment",
                    link = %link,
                    ?impairment,
                )
                .entered();

                Box::pin(
                    async move {
                        // Get the interface index for the peer's veth device.
                        let if_index = wrappers::if_nametoindex(&ctx.peer_id.veth_name())
                            .expect("to find dev")
                            .get() as i32;

                        // Step 1: Install DRR root qdisc if not already present.
                        if !drr_already_installed {
                            install_drr_root(&mut ctx.handle, if_index).await?;
                        }

                        // Step 2: Create or replace DRR class for this destination.
                        configure_drr_class(&mut ctx.handle, if_index, dst_peer_id, is_replacement)
                            .await?;

                        // Step 3: Create or replace TBF qdisc if bandwidth limiting is enabled.
                        let netem_parent = configure_tbf(
                            &mut ctx.handle,
                            if_index,
                            dst_peer_id,
                            &impairment,
                            is_replacement,
                        )
                        .await?;

                        // Step 4: Create or replace netem qdisc.
                        configure_netem(
                            &mut ctx.handle,
                            if_index,
                            dst_peer_id,
                            netem_parent,
                            &impairment,
                            is_replacement,
                        )
                        .await?;

                        // Step 5: Create flower filter (only on first configuration).
                        if !is_replacement {
                            let dst_ip = dst_peer_id.veth_address(subnet);
                            configure_flower_filter(&mut ctx.handle, if_index, dst_peer_id, dst_ip)
                                .await?;
                        }

                        tracing::debug!(is_replacement, "impairment configuration complete");
                        Ok::<_, rtnetlink::Error>(())
                    }
                    .instrument(span.clone()),
                )
            })
            .await?
            .receive()
            .await??;

        // Update state tracking after successful configuration.
        let tc_state = self.tc_state.get_mut(&link.source()).unwrap();
        tc_state.drr_installed = true;
        tc_state.mark_configured(link.destination());

        Ok(())
    }
}

#[cfg(test)]
mod msg_sim_network {
    use std::{
        net::{Ipv4Addr, SocketAddr},
        time::{Duration, Instant},
    };

    use futures::StreamExt;
    use msg_socket::{RepSocket, ReqSocket};
    use msg_transport::tcp::Tcp;

    use crate::{
        ip::Subnet,
        network::{HubOptions, Link, Network, PeerIdExt, PeerOptions},
        tc::impairment::LinkImpairment,
    };

    /// Test that network creation works and creates the hub namespace.
    #[tokio::test(flavor = "multi_thread")]
    async fn create_network_works() {
        let _ = tracing_subscriber::fmt::try_init();
        let subnet = Subnet::new(Ipv4Addr::new(11, 0, 0, 0).into(), 16);
        let _network = Network::new(subnet, HubOptions::default()).await.unwrap();

        let path = format!("/run/netns/{}", Network::hub_namespace_name());
        let exists = std::fs::exists(path.clone()).unwrap();

        assert!(exists, "netns file doesn't exists at path {path}");
    }

    /// Test that multiple peers can be added to the network.
    #[tokio::test(flavor = "multi_thread")]
    async fn add_peer_works() {
        let _ = tracing_subscriber::fmt::try_init();
        let subnet = Subnet::new(Ipv4Addr::new(12, 0, 0, 0).into(), 16);
        let mut network = Network::new(subnet, HubOptions::default()).await.unwrap();

        let _peer_id = network.add_peer(PeerOptions::default()).await.unwrap();
        let _peer_id = network.add_peer(PeerOptions::default()).await.unwrap();
    }

    /// Test basic request/reply communication between two peers.
    #[tokio::test(flavor = "multi_thread")]
    async fn simulate_reqrep_works() {
        let _ = tracing_subscriber::fmt::try_init();

        let subnet = Subnet::new(Ipv4Addr::new(13, 0, 0, 0).into(), 16);
        let mut network = Network::new(subnet, HubOptions::default()).await.unwrap();

        let peer_1 = network.add_peer(PeerOptions::default()).await.unwrap();
        let peer_2 = network.add_peer(PeerOptions::default()).await.unwrap();

        let address_2 = peer_2.veth_address(subnet);
        let port_2 = 12345;

        // Peer 2 runs a reply socket (server)
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

        // Peer 1 runs a request socket (client)
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

    /// Test that applying impairments works (basic smoke test).
    #[tokio::test(flavor = "multi_thread")]
    async fn apply_impairment_works() {
        let _ = tracing_subscriber::fmt::try_init();
        let subnet = Subnet::new(Ipv4Addr::new(12, 0, 0, 0).into(), 16);
        let mut network = Network::new(subnet, HubOptions::default()).await.unwrap();

        let peer_1 = network.add_peer(PeerOptions::default()).await.unwrap();
        let peer_2 = network.add_peer(PeerOptions::default()).await.unwrap();
        let _peer_3 = network.add_peer(PeerOptions::default()).await.unwrap();

        let impairment = LinkImpairment {
            loss: 50.0,
            jitter: 100_000,
            latency: 1_000_000,
            duplicate: 50.0,
            ..Default::default()
        };
        network.apply_impairment(Link::new(peer_1, peer_2), impairment).await.unwrap();
    }

    /// Test that netem delay actually affects message timing.
    #[tokio::test(flavor = "multi_thread")]
    async fn simulate_reqrep_netem_delay_works() {
        let _ = tracing_subscriber::fmt::try_init();

        let subnet = Subnet::new(Ipv4Addr::new(14, 0, 0, 0).into(), 16);
        let mut network = Network::new(subnet, HubOptions::default()).await.unwrap();

        let peer_1 = network.add_peer(PeerOptions::default()).await.unwrap();
        let peer_2 = network.add_peer(PeerOptions::default()).await.unwrap();

        // Apply 1 second latency from peer 1 to peer 2
        let sec_in_us = 1_000_000;
        let impairment = LinkImpairment { latency: sec_in_us, ..Default::default() };
        network.apply_impairment(Link::new(peer_1, peer_2), impairment).await.unwrap();

        let address_2 = peer_2.veth_address(subnet);
        let port_2 = 12345;

        let task1 = network
            .run_in_namespace(peer_2, move |_ctx| {
                Box::pin(async move {
                    let mut rep_socket = RepSocket::new(Tcp::default());
                    rep_socket.bind(SocketAddr::new(address_2, port_2)).await.unwrap();

                    // Given the 1s delay on peer1→peer2 link, a 500ms timeout should fail
                    tokio::time::timeout(Duration::from_micros((sec_in_us / 2).into()), async {
                        if let Some(request) = rep_socket.next().await {
                            let msg = request.msg().clone();
                            request.respond(msg).unwrap();
                        }
                    })
                    .await
                    .unwrap_err();

                    // But waiting longer should succeed
                    if let Some(request) = rep_socket.next().await {
                        let msg = request.msg().clone();
                        request.respond(msg).unwrap();
                    }
                })
            })
            .await
            .unwrap();

        let task2 = network
            .run_in_namespace(peer_1, move |_ctx| {
                Box::pin(async move {
                    let mut req_socket = ReqSocket::new(Tcp::default());

                    req_socket.connect_sync(SocketAddr::new(address_2, port_2));
                    req_socket.request("hello".into()).await.unwrap();
                })
            })
            .await
            .unwrap();

        tokio::try_join!(task1, task2).unwrap();
    }

    /// Test per-destination impairments: different delays to different peers.
    #[tokio::test(flavor = "multi_thread")]
    async fn per_destination_impairments_work() {
        let _ = tracing_subscriber::fmt::try_init();

        let subnet = Subnet::new(Ipv4Addr::new(15, 0, 0, 0).into(), 16);
        let mut network = Network::new(subnet, HubOptions::default()).await.unwrap();

        let peer_1 = network.add_peer(PeerOptions::default()).await.unwrap();
        let peer_2 = network.add_peer(PeerOptions::default()).await.unwrap();
        let peer_3 = network.add_peer(PeerOptions::default()).await.unwrap();

        // Peer 1 → Peer 2: 100ms latency
        let fast_latency_us = 100_000;
        network
            .apply_impairment(
                Link::new(peer_1, peer_2),
                LinkImpairment { latency: fast_latency_us, ..Default::default() },
            )
            .await
            .unwrap();

        // Peer 1 → Peer 3: 500ms latency
        let slow_latency_us = 500_000;
        network
            .apply_impairment(
                Link::new(peer_1, peer_3),
                LinkImpairment { latency: slow_latency_us, ..Default::default() },
            )
            .await
            .unwrap();

        let address_2 = peer_2.veth_address(subnet);
        let address_3 = peer_3.veth_address(subnet);
        let port = 12345;

        // Start servers on peer 2 and peer 3
        let server_2 = network
            .run_in_namespace(peer_2, move |_| {
                Box::pin(async move {
                    let mut rep_socket = RepSocket::new(Tcp::default());
                    rep_socket.bind(SocketAddr::new(address_2, port)).await.unwrap();

                    if let Some(request) = rep_socket.next().await {
                        request.respond("peer2".into()).unwrap();
                    }
                })
            })
            .await
            .unwrap();

        let server_3 = network
            .run_in_namespace(peer_3, move |_| {
                Box::pin(async move {
                    let mut rep_socket = RepSocket::new(Tcp::default());
                    rep_socket.bind(SocketAddr::new(address_3, port)).await.unwrap();

                    if let Some(request) = rep_socket.next().await {
                        request.respond("peer3".into()).unwrap();
                    }
                })
            })
            .await
            .unwrap();

        // Client on peer 1 measures RTT to both peers
        let client = network
            .run_in_namespace(peer_1, move |_| {
                Box::pin(async move {
                    let mut req_socket_2 = ReqSocket::new(Tcp::default());
                    let mut req_socket_3 = ReqSocket::new(Tcp::default());

                    req_socket_2.connect_sync(SocketAddr::new(address_2, port));
                    req_socket_3.connect_sync(SocketAddr::new(address_3, port));

                    // Wait for both TCP connections to be established before starting
                    // measurements. Peer 3 has 500ms one-way latency, so TCP handshake takes ~1s.
                    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

                    // Measure RTT to peer 2 (should be ~100ms for one-way latency)
                    let start = Instant::now();
                    let resp = req_socket_2.request("ping".into()).await.unwrap();
                    let rtt_2 = start.elapsed();
                    assert_eq!(resp.as_ref(), b"peer2");

                    // Measure RTT to peer 3 (should be ~500ms for one-way latency)
                    let start = Instant::now();
                    let resp = req_socket_3.request("ping".into()).await.unwrap();
                    let rtt_3 = start.elapsed();
                    assert_eq!(resp.as_ref(), b"peer3");

                    // Peer 3 should take significantly longer than peer 2
                    // We use a generous margin because timing can vary
                    tracing::info!(?rtt_2, ?rtt_3, "measured RTTs");
                    assert!(
                        rtt_3 > rtt_2 * 2,
                        "RTT to peer 3 ({:?}) should be at least 2x RTT to peer 2 ({:?})",
                        rtt_3,
                        rtt_2
                    );
                })
            })
            .await
            .unwrap();

        tokio::try_join!(server_2, server_3, client).unwrap();
    }

    /// Test bandwidth limiting with TBF qdisc.
    #[tokio::test(flavor = "multi_thread")]
    async fn bandwidth_limiting_works() {
        let _ = tracing_subscriber::fmt::try_init();

        let subnet = Subnet::new(Ipv4Addr::new(16, 0, 0, 0).into(), 16);
        let mut network = Network::new(subnet, HubOptions::default()).await.unwrap();

        let peer_1 = network.add_peer(PeerOptions::default()).await.unwrap();
        let peer_2 = network.add_peer(PeerOptions::default()).await.unwrap();

        // Apply bandwidth limit: 1 Mbit/s from peer 1 to peer 2
        let bandwidth_mbit = 1.0;
        network
            .apply_impairment(
                Link::new(peer_1, peer_2),
                LinkImpairment { bandwidth_mbit_s: Some(bandwidth_mbit), ..Default::default() },
            )
            .await
            .unwrap();

        let address_2 = peer_2.veth_address(subnet);
        let port = 12346;

        // Send a known amount of data and measure transfer time
        let data_size = 125_000; // 125 KB = 1 Mbit (should take ~1 second at 1 Mbit/s)

        let server = network
            .run_in_namespace(peer_2, move |_| {
                Box::pin(async move {
                    let mut rep_socket = RepSocket::new(Tcp::default());
                    rep_socket.bind(SocketAddr::new(address_2, port)).await.unwrap();

                    if let Some(request) = rep_socket.next().await {
                        // Echo back a smaller response
                        request.respond("ok".into()).unwrap();
                    }
                })
            })
            .await
            .unwrap();

        let client = network
            .run_in_namespace(peer_1, move |_| {
                Box::pin(async move {
                    let mut req_socket = ReqSocket::new(Tcp::default());
                    req_socket.connect_sync(SocketAddr::new(address_2, port));

                    // Send large payload
                    let payload = vec![0u8; data_size];
                    let start = Instant::now();
                    let _resp = req_socket.request(payload.into()).await.unwrap();
                    let elapsed = start.elapsed();

                    // At 1 Mbit/s, 125KB should take about 1 second
                    // We allow some margin for protocol overhead and timing variance
                    tracing::info!(?elapsed, data_size, "transfer completed");

                    // Transfer should take at least 500ms (half the theoretical time)
                    // This verifies bandwidth limiting is actually happening
                    assert!(
                        elapsed > Duration::from_millis(500),
                        "Transfer of {} bytes took only {:?}, expected > 500ms at 1 Mbit/s",
                        data_size,
                        elapsed
                    );
                })
            })
            .await
            .unwrap();

        tokio::try_join!(server, client).unwrap();
    }

    /// Test combined bandwidth limiting and latency.
    #[tokio::test(flavor = "multi_thread")]
    async fn bandwidth_and_latency_combined() {
        let _ = tracing_subscriber::fmt::try_init();

        let subnet = Subnet::new(Ipv4Addr::new(17, 0, 0, 0).into(), 16);
        let mut network = Network::new(subnet, HubOptions::default()).await.unwrap();

        let peer_1 = network.add_peer(PeerOptions::default()).await.unwrap();
        let peer_2 = network.add_peer(PeerOptions::default()).await.unwrap();

        // Apply both bandwidth limit and latency
        let latency_us = 100_000; // 100ms
        let bandwidth_mbit = 10.0; // 10 Mbit/s
        network
            .apply_impairment(
                Link::new(peer_1, peer_2),
                LinkImpairment {
                    latency: latency_us,
                    bandwidth_mbit_s: Some(bandwidth_mbit),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let address_2 = peer_2.veth_address(subnet);
        let port = 12347;

        let server = network
            .run_in_namespace(peer_2, move |_| {
                Box::pin(async move {
                    let mut rep_socket = RepSocket::new(Tcp::default());
                    rep_socket.bind(SocketAddr::new(address_2, port)).await.unwrap();

                    if let Some(request) = rep_socket.next().await {
                        request.respond("pong".into()).unwrap();
                    }
                })
            })
            .await
            .unwrap();

        let client = network
            .run_in_namespace(peer_1, move |_| {
                Box::pin(async move {
                    let mut req_socket = ReqSocket::new(Tcp::default());
                    req_socket.connect_sync(SocketAddr::new(address_2, port));

                    // Small message to primarily measure latency
                    let start = Instant::now();
                    let resp = req_socket.request("ping".into()).await.unwrap();
                    let elapsed = start.elapsed();

                    assert_eq!(resp.as_ref(), b"pong");

                    // RTT should be at least 200ms (100ms each way)
                    tracing::info!(?elapsed, "ping-pong completed");
                    assert!(
                        elapsed > Duration::from_millis(150),
                        "RTT {:?} should be > 150ms with 100ms one-way latency",
                        elapsed
                    );
                })
            })
            .await
            .unwrap();

        tokio::try_join!(server, client).unwrap();
    }

    /// Test that applying impairments to the same link replaces the previous config.
    #[tokio::test(flavor = "multi_thread")]
    async fn impairment_replacement_works() {
        let _ = tracing_subscriber::fmt::try_init();

        let subnet = Subnet::new(Ipv4Addr::new(18, 0, 0, 0).into(), 16);
        let mut network = Network::new(subnet, HubOptions::default()).await.unwrap();

        let peer_1 = network.add_peer(PeerOptions::default()).await.unwrap();
        let peer_2 = network.add_peer(PeerOptions::default()).await.unwrap();

        // First application: 100ms latency
        let impairment_1 = LinkImpairment { latency: 100_000, ..Default::default() };
        network.apply_impairment(Link::new(peer_1, peer_2), impairment_1).await.unwrap();

        // Second application: replace with 200ms latency (should succeed, not fail)
        let impairment_2 = LinkImpairment { latency: 200_000, ..Default::default() };
        network
            .apply_impairment(Link::new(peer_1, peer_2), impairment_2)
            .await
            .expect("Replacement should succeed");

        // Third application: replace with bandwidth limiting added
        let impairment_3 =
            LinkImpairment { latency: 50_000, bandwidth_mbit_s: Some(10.0), ..Default::default() };
        network
            .apply_impairment(Link::new(peer_1, peer_2), impairment_3)
            .await
            .expect("Replacement with bandwidth should succeed");
    }

    /// Test applying impairments in both directions of a link.
    #[tokio::test(flavor = "multi_thread")]
    async fn bidirectional_impairments() {
        let _ = tracing_subscriber::fmt::try_init();

        let subnet = Subnet::new(Ipv4Addr::new(19, 0, 0, 0).into(), 16);
        let mut network = Network::new(subnet, HubOptions::default()).await.unwrap();

        let peer_1 = network.add_peer(PeerOptions::default()).await.unwrap();
        let peer_2 = network.add_peer(PeerOptions::default()).await.unwrap();

        // Different impairments in each direction
        let impairment_1_to_2 = LinkImpairment { latency: 50_000, ..Default::default() };
        let impairment_2_to_1 =
            LinkImpairment { latency: 200_000, loss: 5.0, ..Default::default() };

        // Both should succeed (different links)
        network.apply_impairment(Link::new(peer_1, peer_2), impairment_1_to_2).await.unwrap();

        network.apply_impairment(Link::new(peer_2, peer_1), impairment_2_to_1).await.unwrap();

        // Verify both impairments are tracked
        // (The actual effect would be tested by sending traffic both ways)
    }

    /// Test custom burst size configuration.
    #[tokio::test(flavor = "multi_thread")]
    async fn custom_burst_size() {
        let _ = tracing_subscriber::fmt::try_init();

        let subnet = Subnet::new(Ipv4Addr::new(20, 0, 0, 0).into(), 16);
        let mut network = Network::new(subnet, HubOptions::default()).await.unwrap();

        let peer_1 = network.add_peer(PeerOptions::default()).await.unwrap();
        let peer_2 = network.add_peer(PeerOptions::default()).await.unwrap();

        // Apply bandwidth limit with custom burst
        network
            .apply_impairment(
                Link::new(peer_1, peer_2),
                LinkImpairment {
                    bandwidth_mbit_s: Some(10.0),
                    burst_kib: Some(64), // 64 KiB burst
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // If we got here without error, the custom burst was accepted
    }

    /// Documents Linux kernel limitation: once a netem qdisc with `duplicate > 0` exists
    /// on an interface, no additional netem qdiscs can be created on that interface.
    ///
    /// This test verifies the limitation exists (it expects the second netem to fail).
    /// The kernel logs "netem: change failed" when this happens.
    ///
    /// Workaround: Only use `duplicate` on at most one outgoing link per peer.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "flaky, depends on kernel version"]
    async fn netem_duplicate_prevents_additional_netem_qdiscs() {
        let _ = tracing_subscriber::fmt::try_init();

        let subnet = Subnet::new(Ipv4Addr::new(24, 0, 0, 0).into(), 16);
        let mut network = Network::new(subnet, HubOptions::default()).await.unwrap();

        let peer_1 = network.add_peer(PeerOptions::default()).await.unwrap();
        let peer_2 = network.add_peer(PeerOptions::default()).await.unwrap();
        let peer_3 = network.add_peer(PeerOptions::default()).await.unwrap();

        // First netem WITH duplicate - works
        let with_dup = LinkImpairment { latency: 20_000, duplicate: 0.02, ..Default::default() };
        network.apply_impairment(Link::new(peer_1, peer_2), with_dup).await.unwrap();

        // Second netem (even WITHOUT duplicate) - fails due to kernel limitation
        let no_dup = LinkImpairment {
            latency: 10_000,
            // NO duplicate
            ..Default::default()
        };
        let result = network.apply_impairment(Link::new(peer_1, peer_3), no_dup).await;

        // This fails because the first netem has duplicate > 0
        assert!(
            result.is_err(),
            "Expected failure: kernel prevents additional netem qdiscs when one has duplicate > 0"
        );
    }

    /// Test that 100% packet duplication causes messages to be received twice.
    #[tokio::test(flavor = "multi_thread")]
    async fn packet_duplication_works() {
        let _ = tracing_subscriber::fmt::try_init();

        let subnet = Subnet::new(Ipv4Addr::new(21, 0, 0, 0).into(), 16);
        let mut network = Network::new(subnet, HubOptions::default()).await.unwrap();

        let peer_1 = network.add_peer(PeerOptions::default()).await.unwrap();
        let peer_2 = network.add_peer(PeerOptions::default()).await.unwrap();

        // Apply 100% packet duplication from peer 1 to peer 2
        // This should cause every packet to be sent twice
        let impairment = LinkImpairment { duplicate: 100.0, ..Default::default() };
        network.apply_impairment(Link::new(peer_1, peer_2), impairment).await.unwrap();

        let address_2 = peer_2.veth_address(subnet);
        let port = 9999;

        // Receiver: count how many times we receive the message
        let receiver = network
            .run_in_namespace(peer_2, move |_| {
                Box::pin(async move {
                    let sock = tokio::net::UdpSocket::bind(SocketAddr::new(address_2, port))
                        .await
                        .unwrap();

                    let mut received_count = 0;
                    let mut buf = [0u8; 64];

                    // Try to receive multiple times with a timeout
                    // With 100% duplication, we expect to receive the same packet twice
                    loop {
                        match tokio::time::timeout(Duration::from_millis(500), sock.recv(&mut buf))
                            .await
                        {
                            Ok(Ok(n)) => {
                                let msg = &buf[..n];
                                tracing::info!(?msg, received_count, "received packet");
                                assert_eq!(msg, b"ping", "unexpected message content");
                                received_count += 1;
                            }
                            Ok(Err(e)) => panic!("recv error: {}", e),
                            Err(_) => break, // Timeout, no more packets
                        }
                    }

                    received_count
                })
            })
            .await
            .unwrap();

        // Give the receiver time to bind
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Sender: send a single UDP packet
        let sender = network
            .run_in_namespace(peer_1, move |_| {
                Box::pin(async move {
                    let sock = tokio::net::UdpSocket::bind("0.0.0.0:0").await.unwrap();
                    sock.send_to(b"ping", SocketAddr::new(address_2, port)).await.unwrap();
                    tracing::info!("sent single packet");
                })
            })
            .await
            .unwrap();

        // Wait for sender to complete
        sender.await.unwrap();

        // Wait for receiver to finish (after timeout)
        let received_count = receiver.await.unwrap();

        // With 100% duplication, we should receive exactly 2 copies of the packet
        assert_eq!(
            received_count, 2,
            "Expected 2 packets (original + duplicate) but received {}",
            received_count
        );
    }
}
