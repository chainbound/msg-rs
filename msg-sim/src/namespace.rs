use std::io;
use std::os::fd::{AsFd, AsRawFd};
use std::path::{Path, PathBuf};

use crate::ip::NetworkDevice;

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

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("rtnetlink error: {0}")]
    RtNetlink(#[from] rtnetlink::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct NetworkNamespace {
    pub name: String,
    pub file: tokio::fs::File,
    pub devices: Vec<NetworkDevice>,
}

impl NetworkNamespace {
    pub fn path(name: &str) -> PathBuf {
        Path::new("/run").join("netns").join(name)
    }

    pub async fn new(name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        rtnetlink::NetworkNamespace::add(name.clone()).await?;
        let file = tokio::fs::File::open(Self::path(&name)).await?;

        Ok(Self { name, devices: Vec::new(), file })
    }

    pub fn fd(&self) -> i32 {
        self.file.as_fd().as_raw_fd()
    }
}

impl Drop for NetworkNamespace {
    fn drop(&mut self) {
        let namespace = self.name.clone();

        let task = async {
            if let Err(e) = rtnetlink::NetworkNamespace::del(namespace.clone()).await {
                tracing::error!(?e, ?namespace, "failed to delete network namespace");
            }
        };

        // If we are *inside* a Tokio runtime, we must use `block_in_place`
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| {
                let handle = tokio::runtime::Handle::current();

                handle.block_on(task)
            });
            return;
        }
        // If we are NOT inside a runtime, we can safely create one
        let rt =
            tokio::runtime::Runtime::new().expect("failed to build temporary runtime for cleanup");

        rt.block_on(task)
    }
}
