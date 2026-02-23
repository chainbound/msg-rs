//! Raw TCP Fan-out Latency Probe - Netem Only (No DRR)
//!
//! Diagnostic variant of `raw_tcp_fanout_probe` that applies netem directly on
//! the hub's bridge ports instead of using the DRR-based `apply_impairment()`.
//! This eliminates DRR from the qdisc hierarchy, isolating whether the
//! DRR + netem interaction causes the latency inflation seen in the original.
//!
//! # Architecture difference
//!
//! Original (`apply_impairment`):
//!
//! ```text
//!   sender veth -> [DRR root -> per-dest class -> netem] -> bridge -> receiver
//! ```
//!
//! This variant:
//!
//! ```text
//!   sender veth -> bridge -> [netem root on bridge port] -> receiver veth
//! ```
//!
//! Each receiver's bridge port (`msg-vethN-br` in the hub namespace) gets its
//! own standalone netem as root qdisc. No classification needed - each bridge
//! port only carries traffic for one destination.
//!
//! # Running
//!
//! ```bash
//! sudo HOME=$HOME RUST_LOG=info $(which cargo) run --example raw_tcp_fanout_netem_only -p linkem
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

    use linkem::{
        ip::Subnet,
        network::{NAMESPACE_PREFIX, Network, PeerIdExt},
        tc::impairment::LinkImpairment,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tracing_subscriber::EnvFilter;

    struct Stats {
        name: &'static str,
        count: u64,
        total: Duration,
        min: Duration,
        max: Duration,
    }

    fn capture_command(command: &str, args: &[&str]) -> String {
        let rendered = format!("{} {}", command, args.join(" "));
        match std::process::Command::new(command).args(args).output() {
            Ok(output) => {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let stderr = String::from_utf8_lossy(&output.stderr);
                if output.status.success() {
                    let trimmed = stdout.trim_end();
                    if trimmed.is_empty() {
                        format!("$ {rendered}\n(no output)")
                    } else {
                        format!("$ {rendered}\n{trimmed}")
                    }
                } else {
                    format!(
                        "$ {rendered}\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
                        output.status,
                        stdout.trim_end(),
                        stderr.trim_end(),
                    )
                }
            }
            Err(e) => format!("$ {rendered}\nfailed to execute: {e}"),
        }
    }

    /// Apply netem as root qdisc on a bridge port in the hub namespace.
    ///
    /// This bypasses the DRR hierarchy entirely. Each bridge port only carries
    /// traffic to one specific peer, so no per-destination classification is needed.
    fn apply_netem_on_bridge_port(
        hub_namespace: &str,
        bridge_port: &str,
        impairment: &LinkImpairment,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let delay = format!("{}us", impairment.latency);
        let jitter = format!("{}us", impairment.jitter);

        let mut args = vec![
            "netns",
            "exec",
            hub_namespace,
            "tc",
            "qdisc",
            "add",
            "dev",
            bridge_port,
            "root",
            "netem",
            "delay",
            &delay,
            &jitter,
        ];

        let loss_str;
        if impairment.loss > 0.0 {
            loss_str = format!("{}%", impairment.loss);
            args.push("loss");
            args.push(&loss_str);
        }

        let output = std::process::Command::new("ip").args(&args).output()?;

        if !output.status.success() {
            return Err(std::io::Error::other(format!(
                "tc netem on {bridge_port} failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ))
            .into());
        }

        Ok(())
    }

    async fn spawn_receiver(
        network: &Network,
        recv_id: usize,
        sender_addr: IpAddr,
        port: u16,
        total_messages: u64,
        epoch: Instant,
        name: &'static str,
    ) -> Result<
        impl std::future::Future<Output = Result<Stats, tokio::sync::oneshot::error::RecvError>>,
        linkem::network::Error,
    > {
        network
            .run_in_namespace(recv_id, move |_| {
                Box::pin(async move {
                    let dest = SocketAddr::new(sender_addr, port);

                    let mut stream = loop {
                        match tokio::net::TcpStream::connect(dest).await {
                            Ok(s) => break s,
                            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
                        }
                    };

                    tracing::info!(name, %dest, "connected");

                    let mut buf = [0u8; 16];
                    let mut count = 0u64;
                    let mut total_delay = Duration::ZERO;
                    let mut min_delay = Duration::MAX;
                    let mut max_delay = Duration::ZERO;

                    loop {
                        match tokio::time::timeout(
                            Duration::from_secs(5),
                            stream.read_exact(&mut buf),
                        )
                        .await
                        {
                            Ok(Ok(_)) => {
                                let now = epoch.elapsed();
                                let secs = u64::from_be_bytes(buf[..8].try_into().unwrap());
                                let nanos = u64::from_be_bytes(buf[8..16].try_into().unwrap());
                                let sent_at = Duration::new(secs, nanos as u32);

                                let delay = now.saturating_sub(sent_at);
                                count += 1;
                                total_delay += delay;
                                if delay < min_delay {
                                    min_delay = delay;
                                }
                                if delay > max_delay {
                                    max_delay = delay;
                                }

                                tracing::info!(
                                    name,
                                    seq = count,
                                    delay_ms = format_args!("{:.2}", delay.as_secs_f64() * 1000.0),
                                    "received"
                                );

                                if count == total_messages {
                                    break;
                                }
                            }
                            Ok(Err(e)) => {
                                tracing::error!(name, %e, "read error");
                                break;
                            }
                            Err(_) => break,
                        }
                    }

                    Stats { name, count, total: total_delay, min: min_delay, max: max_delay }
                })
            })
            .await
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // --- Configuration (same as original) ---
    const MESSAGES_PER_SECOND: u64 = 1000;
    const TOTAL_MESSAGES: u64 = 1000;
    const NUM_RECEIVERS: usize = 3;
    const PORT: u16 = 9900;

    let impairments: [(&str, LinkImpairment); 3] = [
        (
            "local",
            LinkImpairment {
                latency: 1_000, // 1ms
                jitter: 500,    // 0.5ms
                ..Default::default()
            },
        ),
        (
            "regional",
            LinkImpairment {
                latency: 50_000, // 50ms
                jitter: 10_000,  // 10ms
                ..Default::default()
            },
        ),
        (
            "overseas",
            LinkImpairment {
                latency: 120_000, // 120ms
                jitter: 20_000,   // 20ms
                loss: 0.5,        // 0.5% loss
                ..Default::default()
            },
        ),
    ];

    println!("\n=== Raw TCP Fan-out Latency Probe - NETEM ONLY (no DRR) ===\n");
    println!("  Impairments applied as standalone netem on hub bridge ports.");
    println!("  No DRR classifier in the qdisc hierarchy.\n");
    println!("  Rate:  {} msg/s", MESSAGES_PER_SECOND);
    println!("  Count: {} messages\n", TOTAL_MESSAGES);
    println!("  Receivers:");
    for (name, imp) in &impairments {
        println!(
            "    {:<10} {}ms latency, {}ms jitter, {:.1}% loss",
            name,
            imp.latency / 1000,
            imp.jitter / 1000,
            imp.loss
        );
    }
    println!();

    // --- Network setup ---
    let subnet = Subnet::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 0)), 24);
    let mut network = Network::new(subnet).await?;

    let sender_id = network.add_peer().await?;
    let sender_addr = sender_id.veth_address(subnet);
    println!("  Sender: peer {} @ {}", sender_id, sender_addr);

    let hub_namespace = format!("{NAMESPACE_PREFIX}-hub");

    // Add peers WITHOUT apply_impairment. Instead, apply netem directly on each
    // receiver's bridge port in the hub namespace.
    let mut recv_ids: Vec<(usize, &str)> = Vec::new();
    for (name, imp) in &impairments {
        let recv_id = network.add_peer().await?;

        let bridge_port = recv_id.veth_br_name();
        apply_netem_on_bridge_port(&hub_namespace, &bridge_port, imp)?;

        println!(
            "  Recv [{}]: peer {} @ {}  (netem on {})",
            name,
            recv_id,
            recv_id.veth_address(subnet),
            bridge_port,
        );
        recv_ids.push((recv_id, name));
    }

    // Disable TSO/GSO/GRO on all peer veth devices.
    let all_peer_ids: Vec<usize> =
        std::iter::once(sender_id).chain(recv_ids.iter().map(|(id, _)| *id)).collect();

    for peer_id in &all_peer_ids {
        network
            .run_in_namespace(*peer_id, |ctx| {
                let veth = ctx.peer_id.veth_name();
                Box::pin(async move {
                    let output = std::process::Command::new("ethtool")
                        .args(["-K", &veth, "tso", "off", "gso", "off", "gro", "off"])
                        .output()
                        .expect("ethtool -K failed");

                    assert!(
                        output.status.success(),
                        "failed to disable offloads on {veth}: {}",
                        String::from_utf8_lossy(&output.stderr)
                    );
                })
            })
            .await?
            .await?;
    }

    // Disable offloads on hub-side veth endpoints and bridge.
    for peer_id in &all_peer_ids {
        let hub_veth = peer_id.veth_br_name();
        let output = std::process::Command::new("ip")
            .args([
                "netns",
                "exec",
                &hub_namespace,
                "ethtool",
                "-K",
                &hub_veth,
                "tso",
                "off",
                "gso",
                "off",
                "gro",
                "off",
            ])
            .output()?;

        if !output.status.success() {
            return Err(std::io::Error::other(format!(
                "failed to disable hub offloads on {hub_veth}: {}",
                String::from_utf8_lossy(&output.stderr)
            ))
            .into());
        }
    }

    let _ = std::process::Command::new("ip")
        .args([
            "netns",
            "exec",
            &hub_namespace,
            "ethtool",
            "-K",
            "linkem-br0",
            "tso",
            "off",
            "gso",
            "off",
            "gro",
            "off",
        ])
        .output();

    println!("  Disabled offloads: tso/gso/gro on peer + hub veth devices");
    println!();

    // Show the netem qdiscs we configured in the hub.
    println!("  Hub bridge port qdiscs:");
    for (recv_id, name) in &recv_ids {
        let bridge_port = recv_id.veth_br_name();
        let tc_out = capture_command(
            "ip",
            &["netns", "exec", &hub_namespace, "tc", "qdisc", "show", "dev", &bridge_port],
        );
        println!("    [{name}] {bridge_port}:");
        for line in tc_out.lines().skip(1) {
            println!("      {line}");
        }
    }
    println!();

    let epoch = Instant::now();

    // --- Sender (identical to original) ---
    let sender = network
        .run_in_namespace(sender_id, move |_| {
            Box::pin(async move {
                let listener = tokio::net::TcpListener::bind(SocketAddr::new(sender_addr, PORT))
                    .await
                    .unwrap();

                tracing::info!("sender listening, waiting for {} receivers", NUM_RECEIVERS);

                let mut streams = Vec::with_capacity(NUM_RECEIVERS);
                for i in 0..NUM_RECEIVERS {
                    let (stream, peer) = listener.accept().await.unwrap();
                    stream.set_nodelay(true).unwrap();
                    tracing::info!(peer = %peer, "receiver {} connected", i + 1);
                    streams.push(stream);
                }

                tracing::info!("all receivers connected, starting broadcast");

                let interval = Duration::from_micros(1_000_000 / MESSAGES_PER_SECOND);

                for seq in 1..=TOTAL_MESSAGES {
                    let now = epoch.elapsed();
                    let mut payload = [0u8; 16];
                    payload[..8].copy_from_slice(&now.as_secs().to_be_bytes());
                    payload[8..16].copy_from_slice(&(now.subsec_nanos() as u64).to_be_bytes());

                    for stream in &mut streams {
                        if let Err(e) = stream.write_all(&payload).await {
                            tracing::warn!(seq, %e, "write error");
                        }
                    }

                    tracing::info!(seq, "broadcast");

                    if seq < TOTAL_MESSAGES {
                        tokio::time::sleep(interval).await;
                    }
                }

                for mut stream in streams {
                    let _ = stream.shutdown().await;
                }
            })
        })
        .await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // --- Receivers (identical to original) ---
    let (recv_1_id, recv_1_name) = recv_ids[0];
    let (recv_2_id, recv_2_name) = recv_ids[1];
    let (recv_3_id, recv_3_name) = recv_ids[2];

    let recv_1 =
        spawn_receiver(&network, recv_1_id, sender_addr, PORT, TOTAL_MESSAGES, epoch, recv_1_name)
            .await?;
    let recv_2 =
        spawn_receiver(&network, recv_2_id, sender_addr, PORT, TOTAL_MESSAGES, epoch, recv_2_name)
            .await?;
    let recv_3 =
        spawn_receiver(&network, recv_3_id, sender_addr, PORT, TOTAL_MESSAGES, epoch, recv_3_name)
            .await?;

    sender.await?;
    let (r1, r2, r3) = tokio::join!(recv_1, recv_2, recv_3);

    // Capture hub-side diagnostics (netem stats on bridge ports).
    println!("\n=== Hub Bridge Port Diagnostics ===\n");
    for (recv_id, name) in &recv_ids {
        let bridge_port = recv_id.veth_br_name();
        let tc_out = capture_command(
            "ip",
            &["netns", "exec", &hub_namespace, "tc", "-s", "qdisc", "show", "dev", &bridge_port],
        );
        println!("  [{name}] {bridge_port}:\n{tc_out}\n");
    }

    // Also capture sender-side diagnostics (should be default qdisc, no DRR).
    let (sender_veth, sender_tc) = network
        .run_in_namespace(sender_id, |ctx| {
            let veth = ctx.peer_id.veth_name();
            Box::pin(async move {
                let tc = capture_command("tc", &["-s", "qdisc", "show", "dev", &veth]);
                (veth, tc)
            })
        })
        .await?
        .await?;

    println!("  Sender ({sender_veth}):\n{sender_tc}\n");

    // --- Summary ---
    println!("=== Results ===\n");
    for stats in [r1?, r2?, r3?] {
        println!("  [{}]", stats.name);
        println!("    Received: {}/{} messages", stats.count, TOTAL_MESSAGES);
        if stats.count > 0 {
            let avg = stats.total / stats.count as u32;
            println!(
                "    Latency:  avg {:.2}ms, min {:.2}ms, max {:.2}ms",
                avg.as_secs_f64() * 1000.0,
                stats.min.as_secs_f64() * 1000.0,
                stats.max.as_secs_f64() * 1000.0
            );
        }
    }
    println!();

    Ok(())
}
