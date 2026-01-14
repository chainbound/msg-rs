//! TCP Parameter Tuning per Namespace
//!
//! Demonstrates that each network namespace has its own isolated TCP sysctl parameters.
//! This allows configuring different TCP behavior for different peers in a simulation.
//!
//! # How Namespace Isolation Works
//!
//! Linux network namespaces provide complete isolation of the networking stack, including
//! TCP/IP configuration. When you read `/proc/sys/net/ipv4/tcp_rmem`, you're not reading
//! a regular file -- `/proc` is a virtual filesystem where the kernel generates content on
//! demand based on the calling process's context.
//!
//! The kernel maintains a `struct netns_ipv4` for each network namespace containing ~77
//! TCP-specific parameters. When a process reads a TCP sysctl:
//!
//! ```text
//! Process reads /proc/sys/net/ipv4/tcp_rmem
//!                       │
//!                       ▼
//!        ┌──────────────────────────────┐
//!        │  Kernel looks up caller's    │
//!        │  network namespace via       │
//!        │  current->nsproxy->net_ns    │
//!        └──────────────────────────────┘
//!                       │
//!                       ▼
//!        ┌──────────────────────────────┐
//!        │  Returns data from that      │
//!        │  namespace's netns_ipv4:     │
//!        │  net->ipv4.sysctl_tcp_rmem   │
//!        └──────────────────────────────┘
//! ```
//!
//! This means the same path (`/proc/sys/net/ipv4/tcp_rmem`) returns different values
//! depending on which namespace the reading process belongs to. The files are not
//! symlinks—they're virtual entries that dispatch to namespace-specific data.
//!
//! # Mount Namespace Requirement
//!
//! To access namespace-specific sysctls via `/proc/sys/net/*`, you need BOTH:
//! 1. A network namespace (for isolated TCP parameters)
//! 2. A mount namespace with `/proc` remounted (to see the namespace's view)
//!
//! `msg-sim` handles this automatically: when spawning a namespace thread, it creates
//! a new mount namespace and remounts `/proc` so that `/proc/sys/net/*` reflects that
//! namespace's TCP configuration.
//!
//! # Per-Namespace vs Global Parameters
//!
//! Most TCP parameters are per-namespace:
//! - `tcp_rmem`, `tcp_wmem` (buffer sizes)
//! - `tcp_congestion_control` (congestion algorithm)
//! - `tcp_slow_start_after_idle`
//! - `tcp_sack`, `tcp_timestamps`, `tcp_window_scaling`
//! - And ~70 more...
//!
//! A few are global (read-only in child namespaces):
//! - `tcp_mem` (system-wide memory pressure thresholds)
//! - `tcp_max_orphans` (system-wide orphan limit)
//!
//! # Using the sysctl Module
//!
//! The `msg_sim::sysctl` module provides type-safe access to common parameters:
//!
//! ```no_run
//! use msg_sim::sysctl::{self, Tcp};
//!
//! // Read a parameter
//! let value = sysctl::read(Tcp::SlowStartAfterIdle)?;
//!
//! // Write a parameter
//! sysctl::write(Tcp::CongestionControl, "cubic")?;
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! For parameters not covered by the module, you can always use direct filesystem access:
//!
//! ```no_run
//! // Direct access for less common parameters
//! std::fs::write("/proc/sys/net/ipv4/tcp_plb_enabled", "1")?;
//! std::fs::write("/proc/sys/net/ipv4/tcp_comp_sack_nr", "0")?;
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! # Running
//!
//! ```bash
//! sudo HOME=$HOME $(which cargo) run --example tcp_tuning -p msg-sim
//! ```

use std::net::{IpAddr, Ipv4Addr};

use msg_sim::{
    ip::Subnet,
    network::Network,
    sysctl::{self, Protocol, Tcp},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== TCP Parameter Namespace Isolation Demo ===\n");

    // Show host namespace configuration first
    // Note: TCP parameters are shared between IPv4/IPv6, so Protocol::V4 is used by convention
    println!("Host namespace TCP settings:");
    println!(
        "  tcp_congestion_control: {}",
        sysctl::read(Tcp::CongestionControl, Protocol::V4)?
    );
    println!("  tcp_rmem: {}", sysctl::read(Tcp::Rmem, Protocol::V4)?);
    println!(
        "  tcp_slow_start_after_idle: {}",
        sysctl::read(Tcp::SlowStartAfterIdle, Protocol::V4)?
    );
    println!();

    // Create network with two peers
    let subnet = Subnet::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 0)), 24);
    let mut network = Network::new(subnet).await?;

    let peer1 = network.add_peer().await?;
    let peer2 = network.add_peer().await?;

    println!("Created network with 2 peers (peer {}, peer {})", peer1, peer2);
    println!();

    // Configure peer1 namespace: disable slow start after idle
    println!("Configuring peer {} namespace...", peer1);
    network
        .run_in_namespace(peer1, |_| {
            Box::pin(async {
                sysctl::write(Tcp::SlowStartAfterIdle, Protocol::V4, "0")?;
                println!("  Set tcp_slow_start_after_idle = 0");
                Ok::<_, std::io::Error>(())
            })
        })
        .await?
        .await??;

    // Configure peer2 namespace: enable slow start after idle (default)
    println!("Configuring peer {} namespace...", peer2);
    network
        .run_in_namespace(peer2, |_| {
            Box::pin(async {
                sysctl::write(Tcp::SlowStartAfterIdle, Protocol::V4, "1")?;
                println!("  Set tcp_slow_start_after_idle = 1");

                // Example: For parameters not in the sysctl module, use direct fs access:
                // std::fs::write("/proc/sys/net/ipv4/tcp_plb_enabled", "1")?;

                Ok::<_, std::io::Error>(())
            })
        })
        .await?
        .await??;
    println!();

    // Verify isolation by reading back from each namespace
    println!("=== Verifying Namespace Isolation ===\n");
    println!("Reading tcp_slow_start_after_idle from each namespace:\n");

    let peer1_value = network
        .run_in_namespace(peer1, |_| {
            Box::pin(async { sysctl::read(Tcp::SlowStartAfterIdle, Protocol::V4) })
        })
        .await?
        .await??;

    let peer2_value = network
        .run_in_namespace(peer2, |_| {
            Box::pin(async { sysctl::read(Tcp::SlowStartAfterIdle, Protocol::V4) })
        })
        .await?
        .await??;

    let host_value = sysctl::read(Tcp::SlowStartAfterIdle, Protocol::V4)?;

    println!("  Peer {} namespace: {}", peer1, peer1_value);
    println!("  Peer {} namespace: {}", peer2, peer2_value);
    println!("  Host namespace:    {}", host_value);
    println!();

    if peer1_value != peer2_value {
        println!("Success! Each namespace has independent TCP configuration.");
        println!();
        println!("  The same path /proc/sys/net/ipv4/tcp_slow_start_after_idle");
        println!("  returns different values depending on the caller's namespace.");
        println!();
        println!("  This works because:");
        println!("  1. Each peer runs in a separate network namespace");
        println!("  2. msg-sim creates a mount namespace and remounts /proc");
        println!("  3. The kernel returns namespace-specific sysctl values");
    } else {
        println!("Unexpected: namespaces have the same value.");
        println!("  This might indicate a kernel or namespace setup issue.");
    }

    println!("\n=== Done ===\n");
    Ok(())
}
