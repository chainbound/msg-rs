use std::{io, net::IpAddr, process::Command};

use crate::assert::assert_status;

/// Take the arguments to run a command with `Command`
/// and add the prefix to run it in the namespace environment
#[inline]
fn add_namespace_prefix<'a>(namespace: &'a str, args: Vec<&'a str>) -> Vec<&'a str> {
    let mut prefix_args = vec!["ip", "netns", "exec", namespace];
    prefix_args.extend(args);
    prefix_args
}

#[inline]
pub fn create_namespace(name: &str) -> io::Result<()> {
    let status = Command::new("sudo")
        .args(["ip", "netns", "add", &name])
        .status()?;

    assert_status(status, format!("Failed to create namespace {}", name))
}

/// Create Virtual Ethernet (veth) devices and link them
///
/// Note: device name length can be max 15 chars long
#[inline]
pub fn create_veth_pair(name1: &str, name2: &str) -> io::Result<()> {
    let status = Command::new("sudo")
        .args([
            "ip", "link", "add", &name1, "type", "veth", "peer", "name", &name2,
        ])
        .status()?;
    assert_status(
        status,
        format!("Failed to create veth pair {}-{}", name1, name2),
    )
}

#[inline]
pub fn move_device_to_namespace(name: &str, namespace: &str) -> io::Result<()> {
    let status = Command::new("sudo")
        .args(["ip", "link", "set", &name, "netns", &namespace])
        .status()?;
    assert_status(
        status,
        format!("Failed to move device {} to namespace {}", name, namespace),
    )
}

/// Generate a host IPv4 address from the namespace IPv4 address.
/// This is done by incrementing the last octet of the IP by 1.
#[inline]
pub fn gen_host_ip_address(namespace_ip_addr: &IpAddr) -> String {
    let mut ip_host: Vec<u64> = namespace_ip_addr
        .to_string()
        .split('.')
        .map(|octect| octect.parse::<u64>().unwrap())
        .collect();
    ip_host[3] += 1;
    let ip_host = format!(
        "{}.{}.{}.{}/24",
        ip_host[0], ip_host[1], ip_host[2], ip_host[3]
    );
    ip_host
}

/// add IP address to device
#[inline]
pub fn add_ip_addr_to_device(
    device: &str,
    ip_addr: &str,
    namespace: Option<&str>,
) -> io::Result<()> {
    let mut args = vec!["ip", "addr", "add", ip_addr, "dev", device];
    if let Some(namespace) = namespace {
        args = add_namespace_prefix(namespace, args)
    };
    let status = Command::new("sudo").args(args).status()?;
    assert_status(
        status,
        format!("Failed to add IP address {} to device {}", ip_addr, device),
    )
}

#[inline]
pub fn spin_up_device(name: &str, namespace: Option<&str>) -> io::Result<()> {
    let mut args = vec!["ip", "link", "set", "dev", name, "up"];
    if let Some(namespace) = namespace {
        args = add_namespace_prefix(namespace, args)
    };
    let status = Command::new("sudo").args(args).status()?;
    assert_status(status, format!("Failed to spin up device {}", name))
}

/// Add the provided network emulation parameters for the device
///
/// These parameters are appended to the following command: `tc qdisc add dev <device_name>`
#[inline]
pub fn add_network_emulation_parameters(
    device_name: &str,
    parameters: Vec<&str>,
    namespace: Option<&str>,
) -> io::Result<()> {
    let mut tc_command = vec!["tc", "qdisc", "add", "dev", &device_name];
    if let Some(namespace) = namespace {
        tc_command = add_namespace_prefix(namespace, tc_command)
    };

    let err_message = format!(
        "Failed to add network emulation parameters {:?} to device {}",
        &parameters, device_name
    );

    tc_command.extend(parameters);
    let status = Command::new("sudo").args(tc_command).status()?;
    assert_status(status, err_message)
}
