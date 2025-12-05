use std::{
    fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    str::FromStr,
};

use crate::{
    command::{self},
    namespace::NetworkNamespace,
};

pub const MSG_SIM_NAMESPACE_PREFIX: &str = "msg-sim";

pub trait IpAddrExt {
    fn to_ipv6_mapped(&self) -> Ipv6Addr;
}

impl IpAddrExt for IpAddr {
    fn to_ipv6_mapped(&self) -> Ipv6Addr {
        match self {
            IpAddr::V4(v4) => v4.to_ipv6_mapped(),
            IpAddr::V6(v6) => *v6,
        }
    }
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NetworkDeviceType {
    Loopback,
    Veth(u8),
}

impl fmt::Display for NetworkDeviceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Loopback => write!(f, "lo"),
            Self::Veth(num) => write!(f, "veth-{num}"),
        }
    }
}

impl FromStr for NetworkDeviceType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "lo" {
            return Ok(NetworkDeviceType::Loopback);
        }

        // Must start with "veth-"
        const PREFIX: &str = "veth-";
        if let Some(num_str) = s.strip_prefix(PREFIX) {
            // Parse the number
            let num =
                num_str.parse::<u8>().map_err(|_| format!("invalid veth index: {num_str}"))?;
            return Ok(NetworkDeviceType::Veth(num));
        }

        Err(format!("unknown network device type: {s}"))
    }
}

#[derive(Debug, Clone)]
pub struct NetworkDevice {
    pub address: IpAddr,
    pub variant: NetworkDeviceType,
}

impl NetworkDevice {
    pub fn new(address: IpAddr, variant: NetworkDeviceType) -> Self {
        Self { address, variant }
    }
    pub fn new_loopback() -> Self {
        Self::new(IpAddr::V4(Ipv4Addr::LOCALHOST), NetworkDeviceType::Loopback)
    }

    pub fn new_veth(address: IpAddr, num: u8) -> Self {
        Self::new(address, NetworkDeviceType::Veth(num))
    }
}

/// Create Virtual Ethernet (veth) devices and link them
///
/// Note: device name length can be max 15 chars long
pub fn create_veth_pair(
    ns1: &mut NetworkNamespace,
    ns2: &mut NetworkNamespace,
    veth1: NetworkDevice,
    veth2: NetworkDevice,
) -> command::Result<()> {
    // 1. Create veth devices in the appropriate namespaces.
    command::Runner::by_str(&format!(
        "sudo ip link add {} netns {} type veth peer name {} netns {}",
        &veth1.variant, &ns1.name, &veth2.variant, &ns2.name,
    ))?;

    // 2. Add IP address and Point-to-Point mask to veth devices.
    command::Runner::by_str(&format!(
        "{} ip addr add {}/32 dev {}",
        ns1.prefix_command(),
        &veth1.address,
        &veth1.variant
    ))?;
    command::Runner::by_str(&format!(
        "{} ip addr add {}/32 dev {}",
        ns2.prefix_command(),
        &veth2.address,
        &veth2.variant
    ))?;

    // 3. Turn on the devices.
    command::Runner::by_str(&format!(
        "{} ip link set dev {} up",
        ns1.prefix_command(),
        &veth1.variant
    ))?;
    command::Runner::by_str(&format!(
        "{} ip link set dev {} up",
        ns2.prefix_command(),
        &veth2.variant
    ))?;

    // 4. Add explicit routing for the peers.
    command::Runner::by_str(&format!(
        "{} ip route add {} dev {}",
        ns1.prefix_command(),
        &veth2.address,
        &veth1.variant
    ))?;
    command::Runner::by_str(&format!(
        "{} ip route add {} dev {}",
        ns2.prefix_command(),
        &veth1.address,
        &veth2.variant
    ))?;

    ns1.devices.insert(veth1.variant, veth1);
    ns2.devices.insert(veth2.variant, veth2);

    Ok(())
}
