//! Bandwidth-Delay Product (BDP) and TCP Throughput
//!
//! Shows how TCP window scaling and buffer tuning affect throughput on high-latency links.
//!
//! TCP throughput = min(rwnd, cwnd) / RTT
//!   - rwnd (receive window) is limited by tcp_rmem
//!   - Window scaling allows rwnd > 64KB (required for high-BDP links)
//!
//! ```bash
//! sudo HOME=$HOME $(which cargo) run --example bdp_throughput -p msg-sim
//! ```

#[cfg(not(target_os = "linux"))]
fn main() {}

#[cfg(target_os = "linux")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        time::Instant,
    };

    use futures::StreamExt;
    use msg_sim::{
        ip::Subnet,
        network::{Link, Network, PeerIdExt, PeerOptions},
        tc::impairment::LinkImpairment,
    };
    use msg_socket::{RepSocket, ReqSocket};
    use msg_transport::tcp::Tcp;
    use tracing_subscriber::EnvFilter;

    const BANDWIDTH_MBIT: f64 = 10.0;
    const MSG_SIZE: usize = 256 * 1024; // 256 KB per message
    const NUM_MESSAGES: usize = 20; // Send multiple to let cwnd grow
    const LATENCY_MS: u32 = 20; // 20ms one-way = 40ms RTT

    const TCP_RMEM: &str = "/proc/sys/net/ipv4/tcp_rmem";
    const TCP_WINDOW_SCALING: &str = "/proc/sys/net/ipv4/tcp_window_scaling";

    /// Transfer multiple messages and measure throughput
    async fn transfer(network: &Network, sender: usize, receiver: usize, addr: SocketAddr) -> f64 {
        let server = network
            .run_in_namespace(receiver, move |_| {
                Box::pin(async move {
                    let mut rep = RepSocket::new(Tcp::default());
                    rep.bind(addr).await.unwrap();
                    for _ in 0..NUM_MESSAGES {
                        if let Some(req) = rep.next().await {
                            req.respond("ok".into()).unwrap();
                        }
                    }
                })
            })
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let client = network
            .run_in_namespace(sender, move |_| {
                Box::pin(async move {
                    let mut req = ReqSocket::new(Tcp::default());
                    req.connect_sync(addr);
                    let payload = vec![0u8; MSG_SIZE];
                    let start = Instant::now();
                    for _ in 0..NUM_MESSAGES {
                        req.request(payload.clone().into()).await.unwrap();
                    }
                    start.elapsed()
                })
            })
            .await
            .unwrap();

        let (elapsed, _) = tokio::try_join!(client, server).unwrap();
        println!("Transfer elapsed: {elapsed:?}");

        let total_bytes = MSG_SIZE * NUM_MESSAGES;
        (total_bytes as f64 * 8.0) / (elapsed.as_secs_f64() * 1_000_000.0)
    }

    tracing_subscriber::fmt().with_env_filter(EnvFilter::from_default_env()).init();

    let rtt_ms = LATENCY_MS * 2;
    let bdp_kb = (BANDWIDTH_MBIT * 1000.0 / 8.0) * (rtt_ms as f64 / 1000.0);

    println!("\n=== BDP Throughput Demo ===\n");
    let total_mb = (MSG_SIZE * NUM_MESSAGES) / (1024 * 1024);
    println!("Link: {} Mbit/s, {} ms RTT, BDP = {:.0} KB", BANDWIDTH_MBIT, rtt_ms, bdp_kb);
    println!("Transfer: {} messages × {} KB = {} MB\n", NUM_MESSAGES, MSG_SIZE / 1024, total_mb);

    let subnet = Subnet::new(IpAddr::V4(Ipv4Addr::new(10, 100, 0, 0)), 16);
    let mut network = Network::new(subnet).await?;
    let sender = network.add_peer(PeerOptions::default()).await?;
    let receiver = network.add_peer(PeerOptions::default()).await?;

    let impairment =
        LinkImpairment::default().with_latency_ms(LATENCY_MS).with_bandwidth_mbit_s(BANDWIDTH_MBIT);
    network.apply_impairment(Link::new(sender, receiver), impairment).await?;
    network.apply_impairment(Link::new(receiver, sender), impairment).await?;

    let receiver_ip = receiver.veth_address(subnet);

    // Test 1: Window scaling OFF, small buffers
    network
        .run_in_namespace(receiver, |_| {
            Box::pin(async {
                std::fs::write(TCP_WINDOW_SCALING, "0").unwrap();
                std::fs::write(TCP_RMEM, "4096 16384 65535").unwrap(); // max 64KB (no scaling)
            })
        })
        .await?
        .await?;

    println!("Test 1: Window scaling OFF, max rwnd = 64 KB");
    let tp1 = transfer(&network, sender, receiver, SocketAddr::new(receiver_ip, 9001)).await;
    println!("  Throughput: {:.1} Mbit/s ({:.0}%)\n", tp1, tp1 / BANDWIDTH_MBIT * 100.0);

    // Test 2: Window scaling ON, large buffers
    network
        .run_in_namespace(receiver, |_| {
            Box::pin(async {
                std::fs::write(TCP_WINDOW_SCALING, "1").unwrap();
                std::fs::write(TCP_RMEM, "4096 262144 4194304").unwrap(); // max 4MB
            })
        })
        .await?
        .await?;

    println!("Test 2: Window scaling ON, max rwnd = 4 MB");
    let tp2 = transfer(&network, sender, receiver, SocketAddr::new(receiver_ip, 9002)).await;
    println!("  Throughput: {:.1} Mbit/s ({:.0}%)\n", tp2, tp2 / BANDWIDTH_MBIT * 100.0);

    if tp2 > tp1 * 1.1 {
        println!(
            "Window scaling + larger buffers improved throughput by {:.0}%!",
            (tp2 - tp1) / tp1 * 100.0
        );
    }

    println!("\n=== Done ===\n");
    Ok(())
}
