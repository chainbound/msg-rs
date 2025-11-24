use std::io;
use std::pin::Pin;
use std::{fs::File, future::Future};

use nix::sched::{setns, CloneFlags};

/// Run the code in the provided function `f` in the network namespace `namespace`
pub async fn run_on_namespace<T>(
    namespace: &str,
    f: impl FnOnce() -> Pin<Box<dyn Future<Output = io::Result<T>> + Send>>,
) -> io::Result<T> {
    let namespace_path = format!("/var/run/netns/{}", namespace);

    let ns_fd = File::open(namespace_path)?;
    let host_ns_fd = File::open("/proc/1/ns/net")?;

    // Use setns to switch to the network namespace
    setns(ns_fd, CloneFlags::CLONE_NEWNET)?;
    let res = f().await?;
    // Go back to the host network environment after calling `f`
    setns(host_ns_fd, CloneFlags::CLONE_NEWNET)?;

    Ok(res)
}
