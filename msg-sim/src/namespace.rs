use std::any::Any;
use std::io;
use std::os::fd::{AsFd, AsRawFd};
use std::path::{Path, PathBuf};
use std::pin::Pin;

use nix::sched::CloneFlags;
use tokio::sync::{mpsc, oneshot};

use crate::TryClone;
use crate::ip::NetworkDevice;
use crate::task::{DynRequest, DynRequestSender};

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
    #[error("nix error: {0}")]
    Nix(#[from] nix::Error),
    #[error("thread error: {0}")]
    Thread(Box<dyn std::error::Error + Send + 'static>),
}

#[derive(Debug, thiserror::Error)]
pub enum TaskError<E> {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("nix error: {0}")]
    Nix(#[from] nix::Error),
    #[error("task error: {0}")]
    Task(E),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct NetworkNamespace {
    pub name: String,
    pub file: std::fs::File,
    pub devices: Vec<NetworkDevice>,
}

impl NetworkNamespace {
    pub fn path(name: &str) -> PathBuf {
        Path::new("/run").join("netns").join(name)
    }

    pub async fn new(name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        rtnetlink::NetworkNamespace::add(name.clone()).await?;
        // NOTE: for internal handling, it is more useful to have a synchronous version of it.
        let file = tokio::fs::File::open(Self::path(&name)).await?.into_std().await;

        Ok(Self { name, devices: Vec::new(), file })
    }

    /// Runs the provided future in this network namespace. Running code on a different namespace
    /// requires spawning a dedicated OS thread, which will create its own asynchronous runtime.
    pub fn run<T, E, F>(
        file: std::fs::File,
        task: F,
    ) -> std::thread::JoinHandle<std::result::Result<T, TaskError<E>>>
    where
        T: Send + 'static,
        E: std::error::Error + Send + 'static,
        F: Future<Output = std::result::Result<T, E>> + Send + 'static,
    {
        std::thread::spawn(move || {
            let flags = CloneFlags::from_bits_truncate(nix::libc::CLONE_NEWNET);
            let fd = file.as_fd();

            tracing::debug!(?fd, "settings namespace for thread");

            nix::sched::setns(fd, flags)?;

            // TODO: separate this, meaning spawn a thread and then expose channel for this.
            // Question: is it possible to await between different runtimes? I don't think so. That
            // is, sending a message on one runtime and wait in another for receiving it.

            let rt = tokio::runtime::Builder::new_current_thread().build()?;
            rt.block_on(task).map_err(TaskError::Task)
        })
    }

    pub fn spawn(self) -> (std::thread::JoinHandle<Result<()>>, DynRequestSender) {
        let (tx, mut rx) = mpsc::channel(8);

        let handle = std::thread::spawn(move || {
            let flags = CloneFlags::from_bits_truncate(nix::libc::CLONE_NEWNET);
            let fd = self.file.as_fd();
            let name = &self.name;

            tracing::debug!(?fd, namespace = name, "settings namespace for thread");

            nix::sched::setns(fd, flags)?;

            // TODO: separate this, meaning spawn a thread and then expose channel for this.
            // Question: is it possible to await between different runtimes? I don't think so. That
            // is, sending a message on one runtime and wait in another for receiving it.

            let rt = tokio::runtime::Builder::new_current_thread().build()?;
            rt.block_on(async move {
                while let Some(DynRequest { task, tx }) = rx.recv().await {
                    let _span = tracing::info_span!("namespace job", ?fd, name).entered();

                    let res = task.await;
                    if tx.send(res).is_err() {
                        tracing::error!("failed to send back task response, rx dropped");
                    }
                }
            });

            Ok(())
        });

        (handle, DynRequestSender::new(tx))
    }

    pub fn fd(&self) -> i32 {
        self.file.as_fd().as_raw_fd()
    }
}

impl TryClone for NetworkNamespace {
    type Error = io::Error;

    fn try_clone(&self) -> std::result::Result<Self, Self::Error> {
        let file = self.file.try_clone()?;

        Ok(Self { name: self.name.clone(), file, devices: self.devices.clone() })
    }
}

impl Drop for NetworkNamespace {
    fn drop(&mut self) {
        // let namespace = self.name.clone();
        //
        // let task = async {
        //     if let Err(e) = rtnetlink::NetworkNamespace::del(namespace.clone()).await {
        //         tracing::error!(?e, ?namespace, "failed to delete network namespace");
        //     }
        // };
        //
        // // If we are *inside* a Tokio runtime, we must use `block_in_place`
        // if tokio::runtime::Handle::try_current().is_ok() {
        //     tokio::task::block_in_place(|| {
        //         let handle = tokio::runtime::Handle::current();
        //
        //         handle.block_on(task)
        //     });
        //     return;
        // }
        // // If we are NOT inside a runtime, we can safely create one
        // let rt =
        //     tokio::runtime::Runtime::new().expect("failed to build temporary runtime for cleanup");
        //
        // rt.block_on(task)
    }
}
