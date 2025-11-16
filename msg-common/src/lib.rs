//! Common utilities and types for msg-rs.

#![doc(issue_tracker_base_url = "https://github.com/chainbound/msg-rs/issues/")]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

use std::{
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    time::SystemTime,
};

use futures::future::BoxFuture;

mod channel;
pub use channel::{Channel, channel};

mod task;
pub use task::JoinMap;

/// Returns the current UNIX timestamp in microseconds.
#[inline]
pub fn unix_micros() -> u64 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros() as u64
}

/// Wraps the given error in a boxed future.
pub fn async_error<E: std::error::Error + Send + 'static, T>(
    e: E,
) -> BoxFuture<'static, Result<T, E>> {
    Box::pin(async move { Err(e) })
}

#[allow(non_upper_case_globals)]
pub mod constants {
    pub const KiB: u32 = 1024;
    pub const MiB: u32 = 1024 * KiB;
    pub const GiB: u32 = 1024 * MiB;
}

/// Extension trait for `SocketAddr`.
pub trait SocketAddrExt: Sized {
    /// Returns the unspecified IPv4 socket address, bound to port 0.
    fn unspecified_v4() -> Self;

    /// Returns the unspecified IPv6 socket address, bound to port 0.
    fn unspecified_v6() -> Self;

    /// Returns the unspecified socket address bound to port 0 of the same protocol version of
    /// the provided address.
    fn unspecified_compatible_with(other: &Self) -> Self;
}

impl SocketAddrExt for SocketAddr {
    fn unspecified_v4() -> Self {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0))
    }

    fn unspecified_v6() -> Self {
        SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0))
    }

    fn unspecified_compatible_with(other: &Self) -> Self {
        match other {
            SocketAddr::V4(_) => Self::unspecified_v4(),
            SocketAddr::V6(_) => Self::unspecified_v6(),
        }
    }
}
