use std::io;
use std::os::fd::{AsFd, AsRawFd};
use std::path::{Path, PathBuf};

use nix::sched::CloneFlags;
use tokio::sync::{mpsc, oneshot};

use crate::ip::NetworkDevice;
use crate::namespace::helpers::current_netns;
use crate::task::{DynRequest, DynRequestSender};
use crate::{TryClone, wrappers};

mod helpers {
    use std::{
        fs, io,
        num::NonZeroU32,
        os::fd::{AsRawFd as _, BorrowedFd},
        path::Path,
    };

    use nix::sched::CloneFlags;

    pub fn if_nametoindex(name: &str) -> Option<NonZeroU32> {
        let index = unsafe { nix::libc::if_nametoindex(name.as_ptr()) };
        NonZeroU32::new(index)
    }

    /// Kernel-stable network namespace identity
    #[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
    pub struct NetnsId {
        pub ino: u64,
    }

    impl NetnsId {
        fn from_metadata(meta: &fs::Metadata) -> Self {
            use std::os::unix::fs::MetadataExt;
            NetnsId { ino: meta.ino() }
        }
    }

    /// Get current thread network namespace identity
    pub fn current_netns() -> io::Result<NetnsId> {
        let thread_id = unsafe { nix::libc::gettid() };
        let path = format!("/proc/self/task/{thread_id}/ns/net");
        let meta = fs::metadata(&path)?;
        let ns = NetnsId::from_metadata(&meta);

        tracing::debug!(ino = ns.ino, ?path, "current netns");

        Ok(ns)
    }

    /// Get network namespace identity from an FD
    pub fn netns_from_fd(fd: BorrowedFd<'_>) -> io::Result<NetnsId> {
        let path = format!("/proc/self/fd/{}", fd.as_raw_fd());
        let meta = fs::metadata(&path)?;
        let ns = NetnsId::from_metadata(&meta);

        tracing::debug!(
            ino = ns.ino,
            fd = fd.as_raw_fd(),
            path = %path,
            "netns from fd"
        );

        Ok(ns)
    }

    /// Switch network namespace and verify correctness
    pub fn setns_verified(fd: BorrowedFd<'_>) -> io::Result<(NetnsId, NetnsId)> {
        let _span = tracing::debug_span!("setns_verified", fd = fd.as_raw_fd()).entered();

        let before = current_netns()?;
        let target = netns_from_fd(fd)?;
        tracing::debug!(before_ino = before.ino, target_ino = target.ino, "setns target");

        nix::sched::setns(fd, CloneFlags::CLONE_NEWNET)?;

        let after = current_netns()?;
        tracing::debug!(after_ino = after.ino, "after setns");

        if after != target {
            return Err(io::Error::other(format!(
                "setns verification failed: after={after:?}, target={target:?}"
            )));
        }

        if before == after {
            tracing::debug!("setns was a no-op (already in target namespace)");
        } else {
            tracing::debug!("namespace switch successful");
        }

        Ok((before, after))
    }

    pub struct NetnsGuard {
        expected: NetnsId,
    }

    impl NetnsGuard {
        pub fn new(expected: NetnsId) -> io::Result<Self> {
            let current = current_netns()?;
            if current != expected {
                return Err(io::Error::other("thread not in expected netns"));
            }
            Ok(Self { expected })
        }
    }

    impl Drop for NetnsGuard {
        fn drop(&mut self) {
            if let Ok(current) = current_netns() {
                debug_assert_eq!(
                    current, self.expected,
                    "network namespace changed while guard was alive"
                );
            }
        }
    }
}

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
    Thread(String),
    #[error("failed to send task: {0}")]
    SendError(#[from] mpsc::error::SendError<DynRequest>),
    #[error("failed to receive task result: {0}")]
    RecvError(#[from] oneshot::error::RecvError),
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
struct NetworkNamespaceInner {
    name: String,
    file: std::fs::File,
    devices: Vec<NetworkDevice>,
}

impl TryClone for NetworkNamespaceInner {
    type Error = io::Error;

    fn try_clone(&self) -> std::result::Result<Self, Self::Error> {
        let file = self.file.try_clone()?;

        Ok(Self { name: self.name.clone(), file, devices: self.devices.clone() })
    }
}

impl NetworkNamespaceInner {
    ///
    /// It will create a dedicated OS thread set with this network namespace, by calling
    /// `setns(2)`, which is thread-local.
    pub fn spawn(self) -> (std::thread::JoinHandle<Result<()>>, DynRequestSender) {
        let (tx, mut rx) = mpsc::channel(8);

        let handle = std::thread::spawn(move || {
            // If the namespace thread panics, tasks will hang forever
            //
            // send() may succeed but never be processed
            //
            // NOTE: I don't know if this is true.
            let result = std::panic::catch_unwind(|| {
                let fd = self.file.as_fd();
                let name = &self.name;
                let _span = tracing::info_span!("spawn_namespace", ?name).entered();

                let (_before, after) = helpers::setns_verified(fd)?;
                let _guard = helpers::NetnsGuard::new(after)?;

                let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
                tracing::info!("starting runtime");

                drop(_span);

                rt.block_on(async move {
                    while let Some(DynRequest { task, tx }) = rx.recv().await {
                        debug_assert_eq!(
                            after,
                            current_netns().expect("to check current namespace")
                        );
                        let _span = tracing::info_span!("namespace_job", ?fd, name).entered();

                        let res = task.await;
                        if tx.send(res).is_err() {
                            tracing::error!("failed to send back task response, rx dropped");
                        }
                    }
                });
                Ok(())
            });

            match result {
                Ok(r) => r,
                Err(panic) => {
                    tracing::error!("namespace thread panicked: {:?}", panic);
                    Err(Error::Thread("namespace thread panicked".to_owned()))
                }
            }
        });

        (handle, DynRequestSender::new(tx))
    }
}

#[derive(Debug)]
pub struct NetworkNamespace {
    inner: NetworkNamespaceInner,
    pub rtnetlink_handle: rtnetlink::Handle,
    pub task_sender: DynRequestSender,

    pub _receiver_task: std::thread::JoinHandle<Result<()>>,
    pub _connection_task: tokio::task::JoinHandle<()>,
}

impl NetworkNamespace {
    pub fn path(name: &str) -> PathBuf {
        Path::new("/run").join("netns").join(name)
    }

    pub async fn new(name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        rtnetlink::NetworkNamespace::add(name.clone()).await?;
        let path = Self::path(&name);

        // NOTE: for internal handling, it is more useful to have a synchronous version of it.
        let file = tokio::fs::File::open(path).await?.into_std().await;

        let inner = NetworkNamespaceInner { name, file, devices: Vec::new() };
        let (_receiver_task, task_sender) = inner.try_clone()?.spawn();

        let (rtnetlink_handle, _connection_task) = task_sender
            .submit(async move {
                rtnetlink::new_connection()
                    .map(|(connection, handle, _)| (handle, tokio::task::spawn(connection)))
            })
            .await?
            .receive()
            .await??;

        Ok(Self { inner, task_sender, _receiver_task, rtnetlink_handle, _connection_task })
    }

    // /// Runs the provided future in this network namespace. Running code on a different namespace
    // /// requires spawning a dedicated OS thread, which will create its own asynchronous runtime.
    // pub fn run<T, E, F>(
    //     file: std::fs::File,
    //     task: F,
    // ) -> std::thread::JoinHandle<std::result::Result<T, TaskError<E>>>
    // where
    //     T: Send + 'static,
    //     E: std::error::Error + Send + 'static,
    //     F: Future<Output = std::result::Result<T, E>> + Send + 'static,
    // {
    //     std::thread::spawn(move || {
    //         let flags = CloneFlags::from_bits_truncate(nix::libc::CLONE_NEWNET);
    //         let fd = file.as_fd();
    //
    //         tracing::debug!(?fd, "settings namespace for thread");
    //
    //         nix::sched::setns(fd, flags)?;
    //
    //         // TODO: separate this, meaning spawn a thread and then expose channel for this.
    //         // Question: is it possible to await between different runtimes? I don't think so. That
    //         // is, sending a message on one runtime and wait in another for receiving it.
    //
    //         let rt = tokio::runtime::Builder::new_current_thread().build()?;
    //         rt.block_on(task).map_err(TaskError::Task)
    //     })
    // }

    pub fn fd(&self) -> i32 {
        self.inner.file.as_fd().as_raw_fd()
    }
}

impl Drop for NetworkNamespace {
    fn drop(&mut self) {
        let namespace = self.inner.name.clone();

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
