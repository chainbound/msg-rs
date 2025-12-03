use std::{io, net::IpAddr, process::Command};

use crate::{command, namespace};

/// Add the provided network emulation parameters for the device
///
/// These parameters are appended to the following command: `tc qdisc add dev <device_name>`
#[inline]
pub fn add_network_emulation_parameters(
    namespace: &str,
    device_name: &str,
    parameters: Vec<&str>,
) -> command::Result<command::Output> {
    let parameters = parameters.join(" ");
    command::Runner::by_str(&format!(
        "{} tc qdisc add dev {} {}",
        namespace::prefix_command(namespace),
        device_name,
        parameters
    ))
}
