//! Pub/Sub Latency Probe Example
//!
//! Creates four network namespaces: one publisher and three subscribers, each with
//! different network impairments. The publisher sends timestamped messages at a fixed
//! rate on a topic, and each subscriber logs the one-way delay for every message.
//!
//! # Running
//!
//! ```bash
//! sudo HOME=$HOME RUST_LOG=info $(which cargo) run --example pubsub_latency_probe -p linkem
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

    use bytes::Bytes;
    use futures::StreamExt;
    use linkem::{
        ip::Subnet,
        network::{Link, Network, PeerIdExt},
        tc::impairment::LinkImpairment,
    };
    use msg_socket::{PubSocket, SubSocket};
    use msg_transport::tcp::Tcp;
    use tracing_subscriber::EnvFilter;

    /// Latency stats collected by each subscriber.
    struct Stats {
        name: &'static str,
        count: u64,
        total: Duration,
        min: Duration,
        max: Duration,
    }

    /// Spawn a subscriber in the given namespace. Returns a future that resolves to [`Stats`].
    async fn spawn_subscriber(
        network: &Network,
        sub_id: usize,
        pub_addr: IpAddr,
        port: u16,
        topic: &'static str,
        total_messages: u64,
        epoch: Instant,
        name: &'static str,
    ) -> Result<
        impl std::future::Future<Output = Result<Stats, tokio::sync::oneshot::error::RecvError>>,
        linkem::network::Error,
    > {
        network
            .run_in_namespace(sub_id, move |_| {
                Box::pin(async move {
                    let mut sub_socket = SubSocket::new(Tcp::default());
                    sub_socket.connect(SocketAddr::new(pub_addr, port)).await.unwrap();
                    sub_socket.subscribe(topic).await.unwrap();

                    let mut count = 0u64;
                    let mut total_delay = Duration::ZERO;
                    let mut min_delay = Duration::MAX;
                    let mut max_delay = Duration::ZERO;

                    loop {
                        match tokio::time::timeout(Duration::from_secs(5), sub_socket.next()).await
                        {
                            Ok(Some(msg)) => {
                                let now = epoch.elapsed();
                                let payload = msg.payload();

                                if payload.len() != 16 {
                                    continue;
                                }

                                let secs = u64::from_be_bytes(payload[..8].try_into().unwrap());
                                let nanos = u64::from_be_bytes(payload[8..16].try_into().unwrap());
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
                            Ok(None) => break,
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

    // --- Configuration ---
    const MESSAGES_PER_SECOND: u64 = 1000;
    const TOTAL_MESSAGES: u64 = 1000;
    const PORT: u16 = 9900;
    const TOPIC: &str = "latency";

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

    println!("\n=== Pub/Sub Latency Probe (1 pub, 3 subs) ===\n");
    println!("  Rate:  {} msg/s", MESSAGES_PER_SECOND);
    println!("  Count: {} messages\n", TOTAL_MESSAGES);
    println!("  Subscribers:");
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

    let pub_id = network.add_peer().await?;
    let pub_addr = pub_id.veth_address(subnet);
    println!("  Publisher: peer {} @ {}", pub_id, pub_addr);

    let mut sub_ids: Vec<(usize, &str)> = Vec::new();
    for (name, imp) in &impairments {
        let sub_id = network.add_peer().await?;
        network.apply_impairment(Link::new(pub_id, sub_id), *imp).await?;
        println!("  Sub [{}]: peer {} @ {}", name, sub_id, sub_id.veth_address(subnet));
        sub_ids.push((sub_id, name));
    }
    println!();

    let epoch = Instant::now();

    // --- Publisher ---
    let publisher = network
        .run_in_namespace(pub_id, move |_| {
            Box::pin(async move {
                let mut pub_socket = PubSocket::new(Tcp::default());
                pub_socket.bind(SocketAddr::new(pub_addr, PORT)).await.unwrap();

                // Wait for all subscribers to connect and subscribe
                tokio::time::sleep(Duration::from_secs(1)).await;

                let interval = Duration::from_micros(1_000_000 / MESSAGES_PER_SECOND);

                for seq in 1..=TOTAL_MESSAGES {
                    let now = epoch.elapsed();
                    let mut payload = [0u8; 16];
                    payload[..8].copy_from_slice(&now.as_secs().to_be_bytes());
                    payload[8..16].copy_from_slice(&(now.subsec_nanos() as u64).to_be_bytes());

                    pub_socket.publish(TOPIC, Bytes::from(payload.to_vec())).await.unwrap();
                    tracing::info!(seq, "published");

                    if seq < TOTAL_MESSAGES {
                        tokio::time::sleep(interval).await;
                    }
                }

                // Give subscribers time to drain
                tokio::time::sleep(Duration::from_secs(5)).await;
            })
        })
        .await?;

    // Small delay before subscribers connect
    tokio::time::sleep(Duration::from_millis(100)).await;

    // --- Subscribers ---
    let (sub_1_id, sub_1_name) = sub_ids[0];
    let (sub_2_id, sub_2_name) = sub_ids[1];
    let (sub_3_id, sub_3_name) = sub_ids[2];

    let sub_1 = spawn_subscriber(
        &network,
        sub_1_id,
        pub_addr,
        PORT,
        TOPIC,
        TOTAL_MESSAGES,
        epoch,
        sub_1_name,
    )
    .await?;
    let sub_2 = spawn_subscriber(
        &network,
        sub_2_id,
        pub_addr,
        PORT,
        TOPIC,
        TOTAL_MESSAGES,
        epoch,
        sub_2_name,
    )
    .await?;
    let sub_3 = spawn_subscriber(
        &network,
        sub_3_id,
        pub_addr,
        PORT,
        TOPIC,
        TOTAL_MESSAGES,
        epoch,
        sub_3_name,
    )
    .await?;

    // Wait for publisher to finish, then collect subscriber results
    publisher.await?;
    let (r1, r2, r3) = tokio::join!(sub_1, sub_2, sub_3);

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
