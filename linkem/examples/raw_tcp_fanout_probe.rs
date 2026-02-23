//! Raw TCP Fan-out Latency Probe
//!
//! Creates four network namespaces: one sender and three receivers, each with
//! different network impairments. The sender accepts raw TCP connections from
//! all receivers, then broadcasts fixed-size timestamped messages at a fixed rate.
//! Each receiver logs the one-way delay for every message.
//!
//! This is like `pubsub_latency_probe` but uses raw `TcpListener`/`TcpStream`
//! instead of the msg-rs Pub/Sub sockets.
//!
//! # Running
//!
//! ```bash
//! sudo HOME=$HOME RUST_LOG=info $(which cargo) run --example raw_tcp_fanout_probe -p linkem
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
        network::{Link, Network, PeerIdExt},
        tc::impairment::LinkImpairment,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tracing_subscriber::EnvFilter;

    /// Latency stats collected by each receiver.
    struct Stats {
        name: &'static str,
        count: u64,
        total: Duration,
        min: Duration,
        max: Duration,
    }

    /// Spawn a receiver in the given namespace. It connects to the sender via raw TCP,
    /// reads fixed 16-byte timestamp payloads, and measures one-way delay.
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

                    // Retry connecting until the sender is listening
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
                            Err(_) => break, // timeout — sender is done
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

    // --- Configuration ---
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

    println!("\n=== Raw TCP Fan-out Latency Probe (1 sender, 3 receivers) ===\n");
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

    let mut recv_ids: Vec<(usize, &str)> = Vec::new();
    for (name, imp) in &impairments {
        let recv_id = network.add_peer().await?;
        network.apply_impairment(Link::new(sender_id, recv_id), *imp).await?;
        println!("  Recv [{}]: peer {} @ {}", name, recv_id, recv_id.veth_address(subnet));
        recv_ids.push((recv_id, name));
    }
    println!();

    let epoch = Instant::now();

    // --- Sender ---
    // Binds a TCP listener, waits for all receivers to connect, then broadcasts
    // timestamped 16-byte payloads to each connection.
    let sender = network
        .run_in_namespace(sender_id, move |_| {
            Box::pin(async move {
                let listener = tokio::net::TcpListener::bind(SocketAddr::new(sender_addr, PORT))
                    .await
                    .unwrap();

                tracing::info!("sender listening, waiting for {} receivers", NUM_RECEIVERS);

                // Accept all receiver connections
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

                // Shut down writes so receivers see EOF
                for mut stream in streams {
                    let _ = stream.shutdown().await;
                }
            })
        })
        .await?;

    // Small delay so the sender can bind before receivers connect
    tokio::time::sleep(Duration::from_millis(100)).await;

    // --- Receivers ---
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

    // Wait for sender to finish, then collect receiver results
    sender.await?;
    let (r1, r2, r3) = tokio::join!(recv_1, recv_2, recv_3);

    // --- Summary ---
    println!("\n=== Results ===\n");
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
