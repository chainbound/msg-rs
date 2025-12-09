use std::{
    fmt::{self, Display},
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
};

use derive_more::{Deref, DerefMut, From};

use crate::network::PeerId;

/// A prefix to use to name all network namespaces created by this crate.
pub const MSG_SIM_NAMESPACE_PREFIX: &str = "msg-sim";

/// A prefix to use to name all links created by this crate.
pub const MSG_SIM_LINK_PREFIX: &str = "msg";

pub trait IpAddrExt {
    fn to_ipv6_mapped(&self) -> Ipv6Addr;

    fn to_bits(&self) -> u128;

    fn from_bits(bits: u128) -> Self;
}

impl IpAddrExt for IpAddr {
    fn to_ipv6_mapped(&self) -> Ipv6Addr {
        match self {
            IpAddr::V4(v4) => v4.to_ipv6_mapped(),
            IpAddr::V6(v6) => *v6,
        }
    }

    fn to_bits(&self) -> u128 {
        match self {
            Self::V4(v4) => v4.to_bits().into(),
            Self::V6(v6) => v6.to_bits(),
        }
    }

    fn from_bits(bits: u128) -> Self {
        if bits < u32::MAX as u128 {
            Ipv4Addr::from_bits(bits as u32).into()
        } else {
            Ipv6Addr::from_bits(bits).into()
        }
    }
}

/// A subnet, composed of a base IP address and a netmask.
#[derive(Debug, Clone, Copy)]
pub struct Subnet {
    pub address: IpAddr,
    pub mask: u8,
}

impl Subnet {
    pub fn new(address: IpAddr, mask: u8) -> Self {
        Self { address, mask }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NetworkDeviceInner {
    address: IpAddr,
}

impl NetworkDeviceInner {
    pub fn new(address: IpAddr) -> Self {
        Self { address }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deref, DerefMut)]
pub struct Loopback(NetworkDeviceInner);

impl Default for Loopback {
    fn default() -> Self {
        Self(NetworkDeviceInner { address: IpAddr::V4(Ipv4Addr::LOCALHOST) })
    }
}

impl Loopback {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Display for Loopback {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "lo")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deref)]
pub struct Veth {
    #[deref]
    inner: NetworkDeviceInner,
    id: PeerId,
}

impl Veth {
    pub fn new(address: IpAddr, id: PeerId) -> Self {
        Self { inner: NetworkDeviceInner::new(address), id }
    }

    pub fn id(&self) -> PeerId {
        self.id
    }
}

impl Display for Veth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "veth-{}", self.id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, From)]
pub enum NetworkDevice {
    Loopback(Loopback),
    Veth(Veth),
}

impl NetworkDevice {
    pub fn new_loopback() -> Self {
        Self::Loopback(Loopback(NetworkDeviceInner { address: IpAddr::V4(Ipv4Addr::LOCALHOST) }))
    }

    pub fn new_veth(address: IpAddr, peer_id: PeerId) -> Self {
        Self::Veth(Veth::new(address, peer_id))
    }
}

impl Display for NetworkDevice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Loopback(_) => {
                write!(f, "lo")
            }
            Self::Veth(Veth { id, .. }) => {
                write!(f, "veth-{}", id)
            }
        }
    }
}

// /// Create Virtual Ethernet (veth) devices and link them
// ///
// /// Note: device name length can be max 15 chars long
// pub fn create_veth_pair(
//     ns1: &mut NetworkNamespace,
//     ns2: &mut NetworkNamespace,
//     veth1: Veth,
//     veth2: Veth,
// ) -> command::Result<()> {
//     // 1. Create veth devices in the appropriate namespaces.
//     command::Runner::by_str(&format!(
//         "sudo ip link add {} netns {} type veth peer name {} netns {}",
//         &veth1, &ns1.name, &veth2, &ns2.name,
//     ))?;
//
//     // 2. Add IP address and Point-to-Point mask to veth devices.
//     command::Runner::by_str(&format!(
//         "{} ip addr add {}/32 dev {}",
//         ns1.prefix_command(),
//         &veth1.address,
//         &veth1
//     ))?;
//     command::Runner::by_str(&format!(
//         "{} ip addr add {}/32 dev {}",
//         ns2.prefix_command(),
//         &veth2.address,
//         &veth2
//     ))?;
//
//     // 3. Turn on the devices.
//     command::Runner::by_str(&format!("{} ip link set dev {} up", ns1.prefix_command(), &veth1))?;
//     command::Runner::by_str(&format!("{} ip link set dev {} up", ns2.prefix_command(), &veth2))?;
//
//     // 4. Add explicit routing for the peers.
//     command::Runner::by_str(&format!(
//         "{} ip route add {} dev {}",
//         ns1.prefix_command(),
//         &veth2.address,
//         &veth1
//     ))?;
//     command::Runner::by_str(&format!(
//         "{} ip route add {} dev {}",
//         ns2.prefix_command(),
//         &veth1.address,
//         &veth2
//     ))?;
//
//     ns1.devices.push(veth1.into());
//     ns2.devices.push(veth2.into());
//
//     Ok(())
// }
