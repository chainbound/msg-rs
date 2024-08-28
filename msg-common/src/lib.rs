//! Common utilities and types for msg-rs.

#![doc(issue_tracker_base_url = "https://github.com/chainbound/msg-rs/issues/")]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

use std::time::SystemTime;

use futures::future::BoxFuture;

mod channel;
pub use channel::{channel, Channel};

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
