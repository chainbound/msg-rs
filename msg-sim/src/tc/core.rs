//! Core time conversion utilities for traffic control.
//!
//! The Linux kernel's traffic control subsystem uses its own time units ("ticks").
//! This module provides utilities to convert between human-readable time values
//! and kernel ticks.

use std::io::{self, Read as _};
use std::sync::LazyLock;

/// Path to the kernel's packet scheduler timing information.
///
/// This file exposes how to convert between scheduler ticks and microseconds,
/// which is essential for configuring latency and jitter values correctly.
pub const PSCHED_PATH: &str = "/proc/net/psched";

/// Standard Ethernet MTU in bytes.
pub const MTU_ETHERNET: u32 = 1_500;

/// Initialize the packet scheduler time base by reading `/proc/net/psched`.
///
/// The Linux kernel's traffic control subsystem uses its own time units ("ticks").
/// This function reads the conversion factor from `/proc/net/psched`, which contains
/// four hex values. The first two represent the ratio of ticks to microseconds.
///
/// # How it works
///
/// The file format is: `t2us us2t clock resolution`
///
/// - `t2us`: ticks to microseconds numerator
/// - `us2t`: ticks to microseconds denominator
/// - The ratio `t2us / us2t` gives us ticks per microsecond
///
/// # Returns
///
/// The number of ticks per microsecond, used by [`usec_to_ticks`].
///
/// # Reference
///
/// Adapted from `iproute2/tc/tc_core.c`.
pub fn tc_core_init() -> io::Result<f64> {
    let mut file = std::fs::File::open(PSCHED_PATH)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;

    let mut iter = contents.split_whitespace();

    let t2us = u32::from_str_radix(iter.next().expect("t2us"), 16).expect("t2us hex");
    let us2t = u32::from_str_radix(iter.next().expect("us2t"), 16).expect("us2t hex");

    // Number of ticks within a microsecond.
    let tick_in_usec = t2us as f64 / us2t as f64;

    tracing::debug!(t2us, us2t, tick_in_usec, "read {PSCHED_PATH}");

    Ok(tick_in_usec)
}

/// Cached value of ticks per microsecond, initialized lazily on first use.
///
/// This avoids repeatedly reading `/proc/net/psched` for every qdisc configuration.
pub static TICK_IN_USEC: LazyLock<f64> =
    LazyLock::new(|| tc_core_init().expect("to read /proc/net/psched"));

/// Convert microseconds to kernel packet scheduler ticks.
///
/// The kernel's netem qdisc expects latency and jitter in ticks, not microseconds.
/// This function performs the conversion using the system's tick rate.
pub fn usec_to_ticks(delay_usec: u32) -> u32 {
    (delay_usec as f64 * *TICK_IN_USEC) as u32
}
