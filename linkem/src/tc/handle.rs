//! TC handle computation and common request infrastructure.
//!
//! TC handles are 32-bit values split into major:minor (16:16 bits). This module
//! provides utilities for computing handles according to our numbering scheme.

use rtnetlink::packet_route::tc::TcHandle;

/// The offset added to peer IDs to compute DRR class minor numbers.
///
/// For peer ID `N`, the class minor/major (depending on the qdisc level) is `ID_OFFSET + N`. This
/// keeps class 1:1 reserved as the default (unimpaired) class.
pub const ID_OFFSET: u32 = 10;

/// Offset for netem qdisc major numbers (separate from TBF to avoid collisions).
pub const NETEM_MAJOR_OFFSET: u32 = 20;

/// Common fields shared by all qdisc/class/filter requests.
///
/// This struct captures the basic addressing information needed to target
/// a specific qdisc, class, or filter in the traffic control hierarchy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QdiscRequestInner {
    /// The network interface index (from `if_nametoindex`).
    pub interface_index: i32,
    /// The parent handle (where this qdisc/class attaches).
    pub parent: TcHandle,
    /// This qdisc/class's own handle.
    pub handle: TcHandle,
}

impl QdiscRequestInner {
    /// Create a new request for the given interface, defaulting to root parent.
    pub fn new(index: i32) -> Self {
        Self { interface_index: index, parent: TcHandle::ROOT, handle: TcHandle::default() }
    }

    /// Set the parent handle.
    pub fn with_parent(mut self, parent: TcHandle) -> Self {
        self.parent = parent;
        self
    }

    /// Set this qdisc/class's handle.
    pub fn with_handle(mut self, handle: TcHandle) -> Self {
        self.handle = handle;
        self
    }
}

/// Compute the DRR class handle for a destination peer.
///
/// # Handle Format
///
/// Returns `1:(10 + peer_id)` as a 32-bit handle.
///
/// # Example
///
/// ```
/// use linkem::tc::handle::drr_class_handle;
/// assert_eq!(drr_class_handle(2), 0x0001_000C); // 1:12
/// ```
pub fn drr_class_handle(dest_peer_id: usize) -> u32 {
    let minor = ID_OFFSET + dest_peer_id as u32;
    (1 << 16) | minor
}

/// Compute the TBF qdisc handle for a destination peer.
///
/// # Handle Format
///
/// Returns `(10 + peer_id):0` as a 32-bit handle.
/// Qdisc handles must have minor=0 (kernel rejects non-zero minor).
///
/// # Example
///
/// ```
/// use linkem::tc::handle::tbf_handle;
/// assert_eq!(tbf_handle(2), 0x000C_0000); // 12:0
/// ```
pub fn tbf_handle(dest_peer_id: usize) -> u32 {
    let major = ID_OFFSET + dest_peer_id as u32;
    major << 16 // minor must be 0 for qdiscs
}

/// Compute the netem qdisc handle for a destination peer.
///
/// # Handle Format
///
/// Returns `(20 + peer_id):0` as a 32-bit handle.
/// Uses a different major offset than TBF to avoid handle collisions.
/// Qdisc handles must have minor=0 (kernel rejects non-zero minor).
///
/// # Example
///
/// ```
/// use linkem::tc::handle::netem_handle;
/// assert_eq!(netem_handle(2), 0x0016_0000); // 22:0
/// ```
pub fn netem_handle(dest_peer_id: usize) -> u32 {
    let major = NETEM_MAJOR_OFFSET + dest_peer_id as u32;
    major << 16 // minor must be 0 for qdiscs
}
