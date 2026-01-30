//! Multi-Region Network Simulation Example
//!
//! Simulates 4 peers in different geographic regions with realistic network impairments.
//!
//! # Topology
//!
//! ```text
//!     ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
//!     │ EU-West  │     │ US-East-1│     │ US-East-2│     │  Tokyo   │
//!     │ 10.0.0.1 │     │ 10.0.0.2 │     │ 10.0.0.3 │     │ 10.0.0.4 │
//!     └────┬─────┘     └────┬─────┘     └────┬─────┘     └────┬─────┘
//!          └────────────────┴────────────────┴────────────────┘
//!                              Hub Bridge
//! ```
//!
//! # Running
//!
//! ```bash
//! sudo HOME=$HOME RUST_LOG=info $(which cargo) run --example multi_region -p msg-sim
//! ```

#[cfg(not(target_os = "linux"))]
fn main() {}

#[cfg(target_os = "linux")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        time::{Duration, Instant},
    };

    use msg_sim::{
        ip::Subnet,
        network::{Link, Network, PeerIdExt},
    };
    use tracing_subscriber::EnvFilter;

    // Peer IDs (assigned sequentially by add_peer)
    const EU_WEST: usize = 1;
    const US_EAST_1: usize = 2;
    const US_EAST_2: usize = 3;
    const TOKYO: usize = 4;

    fn region_name(id: usize) -> &'static str {
        match id {
            EU_WEST => "EU-West",
            US_EAST_1 => "US-East-1",
            US_EAST_2 => "US-East-2",
            TOKYO => "Tokyo",
            _ => "Unknown",
        }
    }

    /// Network impairment profiles based on typical cloud latencies.
    mod profiles {
        use msg_sim::tc::impairment::LinkImpairment;

        // US-East-1 <-> US-East-2: Same region, very fast
        pub fn same_region() -> LinkImpairment {
            LinkImpairment {
                latency: 1_000, // 1ms
                jitter: 500,    // 0.5ms
                loss: 0.01,     // 0.01%
                ..Default::default()
            }
        }

        // EU <-> US: Transatlantic
        pub fn eu_to_us() -> LinkImpairment {
            LinkImpairment {
                latency: 40_000, // 40ms one-way
                jitter: 5_000,   // 5ms
                loss: 0.1,       // 0.1%
                bandwidth_mbit_s: Some(1000.0),
                ..Default::default()
            }
        }

        // US <-> Tokyo: Transpacific
        pub fn us_to_tokyo() -> LinkImpairment {
            LinkImpairment {
                latency: 80_000, // 80ms one-way
                jitter: 10_000,  // 10ms
                loss: 0.2,       // 0.2%
                bandwidth_mbit_s: Some(500.0),
                ..Default::default()
            }
        }

        // EU <-> Tokyo: Longest route
        pub fn eu_to_tokyo() -> LinkImpairment {
            LinkImpairment {
                latency: 120_000, // 120ms one-way
                jitter: 15_000,   // 15ms
                loss: 0.5,        // 0.5%
                bandwidth_mbit_s: Some(300.0),
                ..Default::default()
            }
        }
    }

    /// Simple UDP ping between two peers.
    async fn ping(
        network: &Network,
        src: usize,
        dst: usize,
        subnet: Subnet,
    ) -> Result<Duration, String> {
        let dst_addr = SocketAddr::new(dst.veth_address(subnet), 9999);

        // Start listener
        let listener = network
            .run_in_namespace(dst, |_| {
                Box::pin(async {
                    let sock = tokio::net::UdpSocket::bind("0.0.0.0:9999").await?;
                    let mut buf = [0u8; 32];
                    let (n, addr) = sock.recv_from(&mut buf).await?;
                    sock.send_to(&buf[..n], addr).await?;
                    Ok::<_, std::io::Error>(())
                })
            })
            .await
            .map_err(|e| e.to_string())?;

        tokio::time::sleep(Duration::from_millis(10)).await;

        // Send ping
        let rtt = network
            .run_in_namespace(src, move |_| {
                Box::pin(async move {
                    let sock = tokio::net::UdpSocket::bind("0.0.0.0:0").await?;
                    let start = Instant::now();
                    sock.send_to(b"ping", dst_addr).await?;
                    let mut buf = [0u8; 32];
                    tokio::time::timeout(Duration::from_secs(5), sock.recv(&mut buf))
                        .await
                        .map_err(|_| {
                            std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout")
                        })??;
                    Ok::<_, std::io::Error>(start.elapsed())
                })
            })
            .await
            .map_err(|e| e.to_string())?
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?;

        let _ = listener.await;
        Ok(rtt)
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    println!("\n=== Multi-Region Network Simulation ===\n");

    // Create network
    let subnet = Subnet::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 0)), 24);
    let mut network = Network::new(subnet).await?;

    // Add peers
    let eu = network.add_peer().await?;
    let us1 = network.add_peer().await?;
    let us2 = network.add_peer().await?;
    let tokyo = network.add_peer().await?;

    println!("Peers:");
    for id in [eu, us1, us2, tokyo] {
        println!("  {} @ {}", region_name(id), id.veth_address(subnet));
    }
    println!();

    // Configure impairments
    println!("Configuring network impairments...");

    // US-East-1 <-> US-East-2 (same region)
    let same = profiles::same_region();
    network.apply_impairment(Link::new(us1, us2), same).await?;
    network.apply_impairment(Link::new(us2, us1), same).await?;
    println!(
        "  US-East-1 <-> US-East-2: {}ms ±{}ms (same region)",
        same.latency / 1000,
        same.jitter / 1000
    );

    // EU <-> US
    let eu_us = profiles::eu_to_us();
    for us in [us1, us2] {
        network.apply_impairment(Link::new(eu, us), eu_us).await?;
        network.apply_impairment(Link::new(us, eu), eu_us).await?;
    }
    println!(
        "  EU <-> US: {}ms ±{}ms, {} Mbit/s",
        eu_us.latency / 1000,
        eu_us.jitter / 1000,
        eu_us.bandwidth_mbit_s.unwrap() as u32
    );

    // US <-> Tokyo
    let us_tokyo = profiles::us_to_tokyo();
    for us in [us1, us2] {
        network.apply_impairment(Link::new(us, tokyo), us_tokyo).await?;
        network.apply_impairment(Link::new(tokyo, us), us_tokyo).await?;
    }
    println!(
        "  US <-> Tokyo: {}ms ±{}ms, {} Mbit/s",
        us_tokyo.latency / 1000,
        us_tokyo.jitter / 1000,
        us_tokyo.bandwidth_mbit_s.unwrap() as u32
    );

    // EU <-> Tokyo
    let eu_tokyo = profiles::eu_to_tokyo();
    network.apply_impairment(Link::new(eu, tokyo), eu_tokyo).await?;
    network.apply_impairment(Link::new(tokyo, eu), eu_tokyo).await?;
    println!(
        "  EU <-> Tokyo: {}ms ±{}ms, {:.1}% loss, {} Mbit/s",
        eu_tokyo.latency / 1000,
        eu_tokyo.jitter / 1000,
        eu_tokyo.loss,
        eu_tokyo.bandwidth_mbit_s.unwrap() as u32
    );

    println!("\n=== Ping Tests ===\n");

    // Test connectivity
    let tests = [
        (us1, us2, "US-East-1 -> US-East-2"),
        (eu, us1, "EU -> US-East-1"),
        (us1, tokyo, "US-East-1 -> Tokyo"),
        (eu, tokyo, "EU -> Tokyo"),
    ];

    for (src, dst, label) in tests {
        match ping(&network, src, dst, subnet).await {
            Ok(rtt) => println!("  {}: {:.1}ms", label, rtt.as_secs_f64() * 1000.0),
            Err(e) => println!("  {}: failed ({})", label, e),
        }
    }

    println!("\n=== Done ===\n");
    Ok(())
}
