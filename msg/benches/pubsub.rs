use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::StreamExt;
use rand::Rng;

use msg_socket::{PubOptions, PubSocket, SubOptions, SubSocket};
use msg_transport::{Tcp, TcpOptions};

const N_REQS: usize = 10_000;
const REPEAT_TIMES: usize = 10;
const MSG_SIZE: usize = 512;

/// Benchmark the throughput of a single request/response socket pair over localhost
fn main() {
    let _ = tracing_subscriber::fmt::try_init();

    // Using jemalloc improves performance by ~10%
    #[cfg(all(not(windows), not(target_env = "musl")))]
    #[global_allocator]
    static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

    divan::main();
}

#[divan::bench_group(sample_count = REPEAT_TIMES as u32)]
mod pubsub {
    use super::*;

    #[divan::bench]
    fn pubsub_single_thread(bencher: divan::Bencher) {
        // create a multi-threaded tokio runtime
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        pubsub_with_runtime(bencher, rt);
    }

    #[divan::bench]
    fn pubsub_multi_thread(bencher: divan::Bencher) {
        // create a multi-threaded tokio runtime
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        pubsub_with_runtime(bencher, rt);
    }

    fn pubsub_with_runtime(bencher: divan::Bencher, rt: tokio::runtime::Runtime) {
        let mut pub_socket = PubSocket::with_options(
            Tcp::new(),
            PubOptions {
                session_buffer_size: N_REQS,
                ..Default::default()
            },
        );

        let mut sub = SubSocket::with_options(
            Tcp::new_with_options(TcpOptions::default().with_blocking_connect()),
            SubOptions {
                ingress_buffer_size: N_REQS,
                ..Default::default()
            },
        );

        // Prepare the messages to send
        let mut rng = rand::thread_rng();
        let msg_vec: Vec<Bytes> = (0..N_REQS)
            .map(|_| {
                let mut vec = vec![0u8; MSG_SIZE];
                rng.fill(&mut vec[..]);
                Bytes::from(vec)
            })
            .collect();

        // let mut results = Vec::with_capacity(REPEAT_TIMES);

        // Set up the socket connections
        rt.block_on(async {
            pub_socket.bind("127.0.0.1:0").await.unwrap();

            sub.connect(&pub_socket.local_addr().unwrap().to_string())
                .await
                .unwrap();

            sub.subscribe("HELLO".to_string()).await.unwrap();
        });

        std::thread::sleep(Duration::from_millis(50));
        tracing::info!("Starting benchmark");

        bencher.bench_local(|| {
            let msg = msg_vec.clone();
            rt.block_on(async {
                let send = async {
                    tokio::time::sleep(Duration::from_millis(10)).await;

                    let start = Instant::now();
                    for msg in msg {
                        pub_socket.publish("HELLO".to_string(), msg).await.unwrap();
                    }

                    start
                };

                let recv = async {
                    let mut rx = 0;
                    while let Some(_msg) = sub.next().await {
                        rx += 1;
                        if rx + 1 == N_REQS {
                            break;
                        }

                        // tracing::info!("Received: {:?}", msg)
                    }

                    Instant::now()
                };

                let (send_start, recv_end) = tokio::join!(send, recv);

                let elapsed = recv_end.duration_since(send_start);
                let avg_throughput = N_REQS as f64 / elapsed.as_secs_f64();
                let mbps = avg_throughput * MSG_SIZE as f64 / 1_000_000.0;
                tracing::info!(
                    "Throughput: {:.0} msgs/s {:.0} MB/s, Avg time: {:.2} ms",
                    avg_throughput,
                    mbps,
                    elapsed.as_millis()
                );
            })
        });
    }
}
