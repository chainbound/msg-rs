use std::{
    collections::HashMap,
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    process::{Command, Stdio},
};

use crate::{
    command::{self, Runner},
    namespace::NetworkNamespace,
};

pub const MSG_SIM_NAMESPACE_PREFIX: &str = "msg-sim-ns";

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

#[derive(Debug, Clone, Copy)]
pub enum NetworkDeviceType {
    Loopback,
    Veth,
}

#[derive(Debug, Clone)]
pub struct NetworkDevice {
    pub address: IpAddr,
    pub name: String,
    pub variant: NetworkDeviceType,
}

impl NetworkDevice {
    pub fn new_loopback() -> Self {
        Self::new(IpAddr::V4(Ipv4Addr::LOCALHOST), "lo", NetworkDeviceType::Loopback)
    }

    pub fn new_veth(address: IpAddr, name: impl Into<String>) -> Self {
        Self::new(address, name, NetworkDeviceType::Veth)
    }

    pub fn new(address: IpAddr, name: impl Into<String>, variant: NetworkDeviceType) -> Self {
        Self { address, name: name.into(), variant }
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
    command::Runner::by_str(&format!(
        "sudo ip link add {} netns {} type veth peer name {} netns {}",
        &veth1.name, &ns1.name, &veth2.name, &ns2.name,
    ))?;
    command::Runner::by_str(&format!(
        "{} ip addr add {} dev {}",
        ns1.prefix_command(),
        &veth1.address,
        &veth1.name
    ))?;
    command::Runner::by_str(&format!(
        "{} ip addr add {} dev {}",
        ns2.prefix_command(),
        &veth2.address,
        &veth2.name
    ))?;
    command::Runner::by_str(&format!(
        "{} ip link set dev {} up",
        ns1.prefix_command(),
        &veth1.name
    ))?;
    command::Runner::by_str(&format!(
        "{} ip link set dev {} up",
        ns2.prefix_command(),
        &veth2.name
    ))?;

    ns1.devices.insert(veth1.name.clone(), veth1);
    ns2.devices.insert(veth2.name.clone(), veth2);

    Ok(())
}
