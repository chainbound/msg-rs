use std::{
    collections::HashMap,
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    process::{Command, Stdio},
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

#[derive(Debug, Clone)]
pub struct NetworkNamespace {
    pub name: String,
    pub devices: HashMap<String, NetworkDevice>,
}

impl NetworkNamespace {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into(), devices: HashMap::new() }
    }
}

impl Drop for NetworkNamespace {
    fn drop(&mut self) {
        let mut cmd = Command::new("sudo");
        cmd.args(["ip", "netns", "delete", &self.name]).stderr(Stdio::piped());

        tracing::debug!(?cmd, network = self.name, "dropping network namespace");

        let result = cmd.spawn().and_then(|s| s.wait_with_output()).and_then(|output| {
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                let error = format!("failed to veth pairi: {stderr}");
                return Err(io::Error::other(error));
            }
            Ok(())
        });

        if let Err(e) = result {
            tracing::error!(?e, "failed to delete network namespace");
        }
    }
}

/// Create the network namespace with the provided name.
pub fn create_network_namespace(name: &str) -> io::Result<NetworkNamespace> {
    let mut cmd = Command::new("sudo");
    cmd.args(["ip", "netns", "add", name]);
    tracing::debug!(?name, ?cmd, "creating namespace");

    let output = cmd.stderr(Stdio::piped()).spawn()?.wait_with_output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let error = format!("failed to create namespace: {}", stderr);
        return Err(io::Error::other(error));
    }

    Ok(NetworkNamespace::new(name.to_owned()))
}

/// Create Virtual Ethernet (veth) devices and link them
///
/// Note: device name length can be max 15 chars long
pub fn create_veth_pair(
    ns1: &mut NetworkNamespace,
    ns2: &mut NetworkNamespace,
    veth1: NetworkDevice,
    veth2: NetworkDevice,
) -> io::Result<()> {
    let mut cmd = Command::new("sudo");
    cmd.args([
        "ip",
        "link",
        "add",
        &veth1.name,
        "netns",
        &ns1.name,
        "type",
        "veth",
        "peer",
        "name",
        &veth2.name,
        "netns",
        &ns2.name,
    ])
    .stderr(Stdio::piped());

    tracing::debug!(?cmd);

    let output = cmd.spawn()?.wait_with_output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let error = format!("failed to veth pairi: {stderr}");
        return Err(io::Error::other(error));
    }

    ns1.devices.insert(veth1.name.clone(), veth1);
    ns1.devices.insert(veth2.name.clone(), veth2);

    Ok(())
}
