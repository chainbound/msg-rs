//! Latency Probe Example
//!
//! Creates two network namespaces with configurable impairments. A sender sends
//! messages at a fixed rate, each stamped with the time of creation. The receiver
//! logs the one-way delay for every message it receives.
//!
//! # Running
//!
//! ```bash
//! sudo HOME=$HOME RUST_LOG=info $(which cargo) run --example latency_probe -p linkem
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
    use tracing_subscriber::EnvFilter;

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // --- Configuration ---
    const MESSAGES_PER_SECOND: u64 = 1000;
    const TOTAL_MESSAGES: u64 = 1000;
    const PORT: u16 = 9900;

    let impairment = LinkImpairment {
        latency: 50_000, // 50ms one-way
        jitter: 10_000,  // 10ms jitter
        ..Default::default()
    };

    println!("\n=== Latency Probe ===\n");
    println!("  Rate:    {} msg/s", MESSAGES_PER_SECOND);
    println!("  Count:   {} messages", TOTAL_MESSAGES);
    println!(
        "  Netem:   {}ms latency, {}ms jitter\n",
        impairment.latency / 1000,
        impairment.jitter / 1000
    );

    // --- Network setup ---
    let subnet = Subnet::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 0)), 24);
    let mut network = Network::new(subnet).await?;

    let sender_id = network.add_peer().await?;
    let receiver_id = network.add_peer().await?;

    let receiver_addr = receiver_id.veth_address(subnet);

    // Apply impairment on the sender → receiver link
    network.apply_impairment(Link::new(sender_id, receiver_id), impairment).await?;

    println!("  Sender:   peer {} @ {}", sender_id, sender_id.veth_address(subnet));
    println!("  Receiver: peer {} @ {}\n", receiver_id, receiver_addr);

    // --- Receiver ---
    // Listens for UDP datagrams. Each datagram contains 16 bytes: the sender's
    // Instant encoded as two u64s (secs + nanos since an arbitrary epoch).
    // We share `Instant::now()` as our epoch between namespaces via the closure.
    let epoch = Instant::now();

    let receiver = network
        .run_in_namespace(receiver_id, move |_| {
            Box::pin(async move {
                let sock = tokio::net::UdpSocket::bind(SocketAddr::new(receiver_addr, PORT))
                    .await
                    .unwrap();

                let mut buf = [0u8; 16];
                let mut count = 0u64;
                let mut total_delay = Duration::ZERO;
                let mut min_delay = Duration::MAX;
                let mut max_delay = Duration::ZERO;

                loop {
                    match tokio::time::timeout(Duration::from_secs(5), sock.recv(&mut buf)).await {
                        Ok(Ok(16)) => {
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
                                seq = count,
                                delay_ms = format_args!("{:.2}", delay.as_secs_f64() * 1000.0),
                                "received"
                            );

                            if count == TOTAL_MESSAGES {
                                break;
                            }
                        }
                        Ok(Ok(_)) => continue, // unexpected size, skip
                        Ok(Err(e)) => {
                            tracing::error!(%e, "recv error");
                            break;
                        }
                        Err(_) => break, // timeout — sender is done
                    }
                }

                (count, total_delay, min_delay, max_delay)
            })
        })
        .await?;

    // Give receiver time to bind
    tokio::time::sleep(Duration::from_millis(50)).await;

    // --- Sender ---
    let interval = Duration::from_micros(1_000_000 / MESSAGES_PER_SECOND);

    let sender = network
        .run_in_namespace(sender_id, move |_| {
            Box::pin(async move {
                let sock = tokio::net::UdpSocket::bind("0.0.0.0:0").await.unwrap();
                let dest = SocketAddr::new(receiver_addr, PORT);

                for seq in 1..=TOTAL_MESSAGES {
                    let now = epoch.elapsed();
                    let mut payload = [0u8; 16];
                    payload[..8].copy_from_slice(&now.as_secs().to_be_bytes());
                    payload[8..16].copy_from_slice(&(now.subsec_nanos() as u64).to_be_bytes());

                    sock.send_to(&payload, dest).await.unwrap();
                    tracing::info!(seq, "sent");

                    if seq < TOTAL_MESSAGES {
                        tokio::time::sleep(interval).await;
                    }
                }
            })
        })
        .await?;

    // Wait for both to finish
    sender.await?;
    let (count, total_delay, min_delay, max_delay) = receiver.await?;

    // --- Summary ---
    println!("\n=== Results ===\n");
    println!("  Received: {}/{} messages", count, TOTAL_MESSAGES);
    if count > 0 {
        let avg = total_delay / count as u32;
        println!(
            "  Latency:  avg {:.2}ms, min {:.2}ms, max {:.2}ms",
            avg.as_secs_f64() * 1000.0,
            min_delay.as_secs_f64() * 1000.0,
            max_delay.as_secs_f64() * 1000.0
        );
    }
    println!();

    Ok(())
}
