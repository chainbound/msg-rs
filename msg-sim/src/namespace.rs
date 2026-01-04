//! Utilies and abstractions for dealing with network namespaces.

use std::io;
use std::os::fd::{AsFd, AsRawFd};
use std::path::{Path, PathBuf};

use tokio::sync::oneshot;

use crate::dynch;
use crate::dynch::DynRequestSender;
use crate::namespace::helpers::current_netns;

/// Helpers for managing namespaces.
mod helpers {
    use std::{
        fs, io,
        os::fd::{AsRawFd as _, BorrowedFd},
    };

    use nix::sched::CloneFlags;

    /// Kernel-stable network namespace identity
    #[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
    pub struct NetnsId {
        pub inode: u64,
    }

    impl NetnsId {
        fn from_metadata(meta: &fs::Metadata) -> Self {
            use std::os::unix::fs::MetadataExt;
            NetnsId { inode: meta.ino() }
        }
    }

    /// Get current thread network namespace identity
    pub fn current_netns() -> io::Result<NetnsId> {
        let thread_id = unsafe { nix::libc::gettid() };
        let path = format!("/proc/self/task/{thread_id}/ns/net");
        let meta = fs::metadata(&path)?;
        let ns = NetnsId::from_metadata(&meta);

        tracing::debug!(ino = ns.inode, ?path, "current netns");

        Ok(ns)
    }

    /// Get network namespace identity from an FD
    pub fn netns_from_fd(fd: BorrowedFd<'_>) -> io::Result<NetnsId> {
        let path = format!("/proc/self/fd/{}", fd.as_raw_fd());
        let meta = fs::metadata(&path)?;
        let ns = NetnsId::from_metadata(&meta);

        tracing::debug!(
            ino = ns.inode,
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
        tracing::debug!(before_ino = before.inode, target_ino = target.inode, "setns target");

        nix::sched::setns(fd, CloneFlags::CLONE_NEWNET)?;

        let after = current_netns()?;
        tracing::debug!(after_ino = after.inode, "after setns");

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

    /// A guard to check whether the thread remains on the same network namespace when this is
    /// dropped.
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
    #[error("failed to send task, channel closed")]
    ChannelClosed,
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
pub struct NetworkNamespaceInner {
    pub name: String,
    pub file: std::fs::File,
}

trait TryClone: Sized {
    type Error;

    fn try_clone(&self) -> std::result::Result<Self, Self::Error>;
}

impl TryClone for NetworkNamespaceInner {
    type Error = io::Error;

    fn try_clone(&self) -> std::result::Result<Self, Self::Error> {
        let file = self.file.try_clone()?;

        Ok(Self { name: self.name.clone(), file })
    }
}

impl NetworkNamespaceInner {
    /// Spawn a network namespace actor able to ingest and process requests in the configured
    /// namespace.
    ///
    /// It will create a dedicated OS thread set with this network namespace, by calling
    /// `setns(2)`, which is thread-local.
    pub fn spawn<Ctx: 'static>(
        self,
        make_ctx: impl FnOnce() -> Ctx + Send + 'static,
    ) -> (std::thread::JoinHandle<Result<()>>, DynRequestSender<Ctx>) {
        let (tx, mut rx) = dynch::channel(8);

        let handle = std::thread::spawn(move || {
            let fd = self.file.as_fd();
            let name = &self.name;
            let _span = tracing::info_span!("spawn_namespace", ?name).entered();

            let (_before, after) = helpers::setns_verified(fd)?;
            let _guard = helpers::NetnsGuard::new(after)?;

            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;

            tracing::debug!("started runtime");
            drop(_span);

            rt.block_on(async move {
                let mut ctx = make_ctx();

                while let Some(req) = rx.recv().await {
                    let (task, tx) = req.into_parts();
                    let _span = tracing::info_span!("namespace_job", ?fd, name).entered();
                    debug_assert_eq!(after, current_netns().expect("to check current namespace"));

                    let res = task(&mut ctx).await;
                    if tx.send(res).is_err() {
                        tracing::error!("failed to send back task response, rx dropped");
                    }
                }
            });

            Ok(())
        });

        (handle, tx)
    }
}

/// An anctor backed by a certain network namespace.
#[derive(Debug)]
pub struct NetworkNamespace<Ctx = ()> {
    /// The inner network namespace data.
    pub inner: NetworkNamespaceInner,
    /// The channel to send requests.
    pub task_sender: DynRequestSender<Ctx>,
    /// An handle to the underlying receiver task.
    pub _receiver_task: std::thread::JoinHandle<Result<()>>,
}

impl NetworkNamespace {
    pub async fn new<Ctx: 'static>(
        name: impl Into<String>,
        make_ctx: impl FnOnce() -> Ctx + Send + 'static,
    ) -> Result<NetworkNamespace<Ctx>> {
        let name = name.into();
        rtnetlink::NetworkNamespace::add(name.clone()).await?;
        let path = Self::path(&name);

        // NOTE: for internal handling, it is more useful to have a synchronous version of it.
        let file = tokio::fs::File::open(path).await?.into_std().await;

        let inner = NetworkNamespaceInner { name, file };
        let (_receiver_task, task_sender) = inner.try_clone()?.spawn(make_ctx);

        Ok(NetworkNamespace::<Ctx> { inner, task_sender, _receiver_task })
    }
}

impl<Ctx> NetworkNamespace<Ctx> {
    pub fn path(name: &str) -> PathBuf {
        Path::new("/run").join("netns").join(name)
    }

    pub fn fd(&self) -> i32 {
        self.inner.file.as_fd().as_raw_fd()
    }
}

impl<Ctx> Drop for NetworkNamespace<Ctx> {
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
