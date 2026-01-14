//! Utilies and abstractions for dealing with network namespaces.

use std::io;
use std::os::fd::{AsFd, AsRawFd};
use std::path::{Path, PathBuf};

use tokio::sync::oneshot;

use crate::dynch::{DynCh, DynRequestSender};
use crate::namespace::helpers::current_netns;

/// Base directory for named network namespaces.
///
/// On Linux, `ip netns add <name>` creates a bind mount at `/run/netns/<name>` pointing to
/// a network namespace. This allows the namespace to persist even when no processes are
/// using it. The `iproute2` tools and `rtnetlink` use this convention.
///
/// See: `man ip-netns(8)`
const NETNS_RUN_DIR: &str = "/run/netns";

/// Procfs base path for accessing file descriptors.
///
/// `/proc/self/fd/<n>` is a symlink to the file that file descriptor `n` points to.
/// For namespace file descriptors, following this symlink (via `stat()`) gives us the
/// inode number which uniquely identifies the namespace within the kernel.
///
/// See: `man proc(5)`, section `/proc/[pid]/fd/`
const PROC_SELF_FD: &str = "/proc/self/fd";

/// Procfs base path for accessing per-thread information.
///
/// `/proc/self/task/<tid>/ns/net` points to the network namespace of thread `tid`.
/// Each thread can be in a different network namespace (set via `setns(2)`), so we
/// must use the thread-specific path rather than `/proc/self/ns/net` which only
/// reflects the main thread's namespace.
///
/// See: `man proc(5)`, section `/proc/[pid]/task/[tid]/ns/`
const PROC_SELF_TASK: &str = "/proc/self/task";

/// Helpers for managing namespaces.
pub(crate) mod helpers {
    use std::{
        fs, io,
        os::fd::{AsRawFd as _, BorrowedFd},
    };

    use nix::mount::{MsFlags, mount};
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
        let path = format!("{}/{thread_id}/ns/net", super::PROC_SELF_TASK);
        let meta = fs::metadata(&path)?;
        let ns = NetnsId::from_metadata(&meta);

        tracing::debug!(ino = ns.inode, ?path, "current netns");

        Ok(ns)
    }

    /// Get network namespace identity from an FD
    pub fn netns_from_fd(fd: BorrowedFd<'_>) -> io::Result<NetnsId> {
        let path = format!("{}/{}", super::PROC_SELF_FD, fd.as_raw_fd());
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

    /// Create a new mount namespace and remount /proc.
    ///
    /// This is necessary to access namespace-specific sysctls at `/proc/sys/net/*`.
    /// Without a fresh /proc mount, the process would see the host's /proc regardless
    /// of which network namespace it's in.
    ///
    /// # How it works
    ///
    /// 1. `unshare(CLONE_NEWNS)` creates a new mount namespace for this thread
    /// 2. `mount("proc", "/proc", "proc", ...)` remounts /proc in the new namespace
    ///
    /// After this, `/proc/sys/net/ipv4/*` will show this network namespace's values.
    pub fn setup_mount_namespace() -> io::Result<()> {
        // Create a new mount namespace
        nix::sched::unshare(CloneFlags::CLONE_NEWNS)
            .map_err(|e| io::Error::other(format!("unshare(CLONE_NEWNS) failed: {}", e)))?;

        tracing::debug!("created new mount namespace");

        // Remount /proc to see this network namespace's view
        mount(Some("proc"), "/proc", Some("proc"), MsFlags::empty(), None::<&str>)
            .map_err(|e| io::Error::other(format!("mount proc failed: {}", e)))?;

        tracing::debug!("remounted /proc for namespace-specific view");

        Ok(())
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
        let (tx, mut rx) = DynCh::<Ctx>::channel(8);

        let handle = std::thread::spawn(move || {
            let fd = self.file.as_fd();
            let name = &self.name;
            let _span = tracing::info_span!("spawn_namespace", ?name).entered();

            let (_before, after) = helpers::setns_verified(fd)?;
            let _guard = helpers::NetnsGuard::new(after)?;

            // Create mount namespace and remount /proc for namespace-specific sysctl access
            helpers::setup_mount_namespace()?;

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
    /// Returns the filesystem path for a named network namespace.
    ///
    /// Named network namespaces are stored in `/run/netns/`.
    pub fn path(name: &str) -> PathBuf {
        Path::new(NETNS_RUN_DIR).join(name)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dynch::DynFuture;
    use crate::sysctl::{self, Protocol, Tcp};

    #[tokio::test(flavor = "multi_thread")]
    async fn mount_namespace_isolates_proc() {
        // Create two namespaces
        let ns1 = NetworkNamespace::new("test-ns-mount-1", || ()).await.unwrap();
        let ns2 = NetworkNamespace::new("test-ns-mount-2", || ()).await.unwrap();

        // Verify /proc is mounted in ns1 by checking /proc/self/ns/net exists
        let proc_mounted_ns1: bool = ns1
            .task_sender
            .submit(|_: &mut ()| -> DynFuture<'_, bool> {
                Box::pin(async { std::path::Path::new("/proc/self/ns/net").exists() })
            })
            .await
            .unwrap()
            .receive()
            .await
            .unwrap();
        assert!(proc_mounted_ns1, "/proc should be mounted in namespace 1");

        // Verify /proc is mounted in ns2
        let proc_mounted_ns2: bool = ns2
            .task_sender
            .submit(|_: &mut ()| -> DynFuture<'_, bool> {
                Box::pin(async { std::path::Path::new("/proc/self/ns/net").exists() })
            })
            .await
            .unwrap()
            .receive()
            .await
            .unwrap();
        assert!(proc_mounted_ns2, "/proc should be mounted in namespace 2");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sysctl_values_are_namespace_specific() {
        // Create two namespaces
        let ns1 = NetworkNamespace::new("test-ns-sysctl-1", || ()).await.unwrap();
        let ns2 = NetworkNamespace::new("test-ns-sysctl-2", || ()).await.unwrap();

        // Set different values in each namespace using the sysctl module
        let write_result_ns1: std::io::Result<()> = ns1
            .task_sender
            .submit(|_: &mut ()| -> DynFuture<'_, std::io::Result<()>> {
                Box::pin(async { sysctl::write(Tcp::SlowStartAfterIdle, Protocol::V4, "0") })
            })
            .await
            .unwrap()
            .receive()
            .await
            .unwrap();
        write_result_ns1.expect("should write sysctl in ns1");

        let write_result_ns2: std::io::Result<()> = ns2
            .task_sender
            .submit(|_: &mut ()| -> DynFuture<'_, std::io::Result<()>> {
                Box::pin(async { sysctl::write(Tcp::SlowStartAfterIdle, Protocol::V4, "1") })
            })
            .await
            .unwrap()
            .receive()
            .await
            .unwrap();
        write_result_ns2.expect("should write sysctl in ns2");

        // Read back values and verify they're different
        let value_ns1: String = ns1
            .task_sender
            .submit(|_: &mut ()| -> DynFuture<'_, String> {
                Box::pin(async {
                    sysctl::read(Tcp::SlowStartAfterIdle, Protocol::V4)
                        .unwrap_or_else(|_| "error".to_string())
                })
            })
            .await
            .unwrap()
            .receive()
            .await
            .unwrap();

        let value_ns2: String = ns2
            .task_sender
            .submit(|_: &mut ()| -> DynFuture<'_, String> {
                Box::pin(async {
                    sysctl::read(Tcp::SlowStartAfterIdle, Protocol::V4)
                        .unwrap_or_else(|_| "error".to_string())
                })
            })
            .await
            .unwrap()
            .receive()
            .await
            .unwrap();

        assert_eq!(value_ns1, "0", "ns1 should have tcp_slow_start_after_idle=0");
        assert_eq!(value_ns2, "1", "ns2 should have tcp_slow_start_after_idle=1");
        assert_ne!(
            value_ns1, value_ns2,
            "sysctls should be isolated between namespaces"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn namespace_has_isolated_network_identity() {
        // Create a namespace
        let ns = NetworkNamespace::new("test-ns-identity", || ()).await.unwrap();

        // Get the network namespace inode from inside the namespace
        let ns_inode_inside: u64 = ns
            .task_sender
            .submit(|_: &mut ()| -> DynFuture<'_, u64> {
                Box::pin(async {
                    helpers::current_netns().map(|id| id.inode).unwrap_or(0)
                })
            })
            .await
            .unwrap()
            .receive()
            .await
            .unwrap();

        // Get the host namespace inode (from outside)
        let host_inode = helpers::current_netns().map(|id| id.inode).unwrap_or(0);

        assert_ne!(ns_inode_inside, 0, "should get valid inode inside namespace");
        assert_ne!(host_inode, 0, "should get valid host inode");
        assert_ne!(
            ns_inode_inside, host_inode,
            "namespace inode should differ from host"
        );
    }
}
