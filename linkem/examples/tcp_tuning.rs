//! TCP Parameter Tuning per Namespace
//!
//! Shows that each network namespace has isolated TCP sysctl parameters.
//!
//! ```bash
//! sudo HOME=$HOME RUST_LOG=info $(which cargo) run --example tcp_tuning -p linkem
//! ```

#[cfg(not(target_os = "linux"))]
fn main() {}

#[cfg(target_os = "linux")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::net::{IpAddr, Ipv4Addr};

    use linkem::{ip::Subnet, network::Network};
    use tracing_subscriber::EnvFilter;

    const TCP_RMEM: &str = "/proc/sys/net/ipv4/tcp_rmem";
    const TCP_WMEM: &str = "/proc/sys/net/ipv4/tcp_wmem";

    fn read_sysctl(path: &str) -> String {
        std::fs::read_to_string(path).unwrap().trim().to_string()
    }

    fn write_sysctl(path: &str, value: &str) {
        std::fs::write(path, value).unwrap();
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    println!("\n=== TCP Tuning Demo ===\n");
    println!("Host TCP buffers:");
    println!("  tcp_rmem: {}", read_sysctl(TCP_RMEM));
    println!("  tcp_wmem: {}", read_sysctl(TCP_WMEM));

    // Create network with one peer
    let subnet = Subnet::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 0)), 24);
    let mut network = Network::new(subnet).await?;
    let peer = network.add_peer().await?;

    // Tune TCP buffers in peer's namespace (min, default, max)
    let tuned_buffers = "4096 1048576 16777216"; // 4KB / 1MB / 16MB

    network
        .run_in_namespace(peer, |_| {
            Box::pin(async {
                write_sysctl(TCP_RMEM, tuned_buffers);
                write_sysctl(TCP_WMEM, tuned_buffers);
            })
        })
        .await?
        .await?;

    // Read back to verify
    let peer_rmem = network
        .run_in_namespace(peer, |_| Box::pin(async { read_sysctl(TCP_RMEM) }))
        .await?
        .await?;

    println!("\nPeer {} TCP buffers (tuned):", peer);
    println!("  tcp_rmem: {}", peer_rmem);

    println!("\nHost TCP buffers (unchanged):");
    println!("  tcp_rmem: {}", read_sysctl(TCP_RMEM));

    println!("\n=== Done ===\n");
    Ok(())
}
