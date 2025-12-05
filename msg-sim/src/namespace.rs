use std::collections::HashMap;

use crate::command;
use crate::ip::{NetworkDevice, NetworkDeviceType};

// Run the code in the provided function `f` in the network namespace `namespace`
// pub async fn run_on_namespace<T>(
//     namespace: &str,
//     f: impl FnOnce() -> Pin<Box<dyn Future<Output = io::Result<T>> + Send>>,
// ) -> io::Result<T> {
//     let namespace_path = format!("/var/run/netns/{}", namespace);
//
//     let ns_fd = File::open(namespace_path)?;
//     let host_ns_fd = File::open("/proc/1/ns/net")?;
//
//     // Use setns to switch to the network namespace
//     setns(ns_fd, CloneFlags::CLONE_NEWNET)?;
//     let res = f().await?;
//     // Go back to the host network environment after calling `f`
//     setns(host_ns_fd, CloneFlags::CLONE_NEWNET)?;
//
//     Ok(res)
// }

/// Return a `ip netns exec <namespace>` string used to prefix other commands.
pub fn prefix_command(name: &str) -> String {
    format!("sudo ip netns exec {}", name)
}

#[derive(Debug, Clone)]
pub struct NetworkNamespace {
    pub name: String,
    pub devices: HashMap<NetworkDeviceType, NetworkDevice>,
}

impl NetworkNamespace {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into(), devices: HashMap::new() }
    }

    /// Return a `ip netns exec <namespace>` string used to prefix other commands.
    pub fn prefix_command(&self) -> String {
        prefix_command(&self.name)
    }

    pub fn loopback_up(&self) -> command::Result<command::Output> {
        command::Runner::by_str(&format!("{} ip link set dev lo up", self.prefix_command()))
    }
}

impl Drop for NetworkNamespace {
    fn drop(&mut self) {
        let result = command::Runner::by_str(&format!("sudo ip netns delete {}", self.name));

        if let Err(e) = result {
            tracing::error!(?e, "failed to delete network namespace");
        }
    }
}

/// Create the network namespace with the provided name.
pub fn create(name: &str) -> command::Result<NetworkNamespace> {
    command::Runner::by_str(&format!("sudo ip netns add {}", name))?;

    Ok(NetworkNamespace::new(name.to_owned()))
}
