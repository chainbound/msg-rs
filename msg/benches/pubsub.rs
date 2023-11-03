use bytes::Bytes;
use futures::StreamExt;
use rand::Rng;
use std::time::Instant;

use msg_socket::{PubOptions, PubSocket, SubOptions, SubSocket};
use msg_transport::{Tcp, TcpOptions};

const N_REQS: usize = 100_000;
const MSG_SIZE: usize = 512;

/// Runs various benchmarks for the `PubSocket` and `SubSocket`.
fn main() {
    let _ = tracing_subscriber::fmt::try_init();

    // Using jemalloc improves performance by ~10%
    #[cfg(all(not(windows), not(target_env = "musl")))]
    #[global_allocator]
    static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

    divan::main();
}

#[divan::bench_group(sample_count = 20)]
mod pubsub {
    use std::time::Duration;

    use divan::counter::{BytesCount, ItemsCount};
    use tracing::Instrument;

    use super::*;

    /// Last run: 60.01 ms, 853.1 MB/s, 1.666 Mitem/s
    #[divan::bench()]
    fn pubsub_single_thread_tcp(bencher: divan::Bencher) {
        // create a current-threaded tokio runtime
        let rt = tokio::runtime::Builder::new_current_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();

        pubsub_with_runtime(bencher, rt);
    }

    /// Last run:
    /// Median: 42.83ms, 1.195 GB/s, 2.334 Mitem/s
    #[divan::bench]
    fn pubsub_multi_thread_tcp(bencher: divan::Bencher) {
        // create a multi-threaded tokio runtime
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();

        pubsub_with_runtime(bencher, rt);
    }

    /// Last run:
    /// Median: 49.86 ms, 1.026 GB/s, 2.005 Mitem/s
    #[divan::bench]
    fn pubsub_2_subscribers(bencher: divan::Bencher) {
        // create a multi-threaded tokio runtime
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();

        let mut pub_socket = PubSocket::with_options(
            Tcp::new(),
            PubOptions {
                session_buffer_size: N_REQS,
                flush_interval: Some(Duration::from_micros(100)),
                backpressure_boundary: 1024 * 64,
                ..Default::default()
            },
        );

        let mut sub1 = SubSocket::with_options(
            Tcp::new_with_options(TcpOptions::default().with_blocking_connect()),
            SubOptions {
                ingress_buffer_size: N_REQS,
                ..Default::default()
            },
        );

        let mut sub2 = SubSocket::with_options(
            Tcp::new_with_options(TcpOptions::default().with_blocking_connect()),
            SubOptions {
                ingress_buffer_size: N_REQS,
                ..Default::default()
            },
        );

        // Set up the socket connections
        rt.block_on(async {
            pub_socket.bind("127.0.0.1:0").await.unwrap();

            sub1.connect(&pub_socket.local_addr().unwrap().to_string())
                .await
                .unwrap();

            sub1.subscribe("HELLO".to_string()).await.unwrap();

            sub2.connect(&pub_socket.local_addr().unwrap().to_string())
                .await
                .unwrap();

            sub2.subscribe("HELLO".to_string()).await.unwrap();

            // Give some time for the background connection process to run
            tokio::time::sleep(Duration::from_millis(10)).await;
        });

        bencher
            .counter(ItemsCount::new(N_REQS as u64))
            .counter(BytesCount::new((N_REQS * MSG_SIZE) as u64))
            .with_inputs(|| -> Vec<Bytes> {
                let mut rng = rand::thread_rng();
                (0..N_REQS)
                    .map(|_| {
                        let mut vec = vec![0u8; MSG_SIZE];
                        rng.fill(&mut vec[..]);
                        Bytes::from(vec)
                    })
                    .collect()
            })
            .bench_local_values(|msg_vec: Vec<Bytes>| {
                rt.block_on(async {
                    let send = async {
                        let start = Instant::now();
                        // tokio::time::sleep(Duration::from_micros(5)).await;
                        for msg in msg_vec {
                            pub_socket.publish("HELLO".to_string(), msg).await.unwrap();
                        }

                        start
                    };

                    let recv1 = async {
                        let mut rx = 0;
                        while let Some(_msg) = sub1.next().await {
                            rx += 1;
                            if rx + 1 == N_REQS {
                                break;
                            }
                        }
                    };

                    let recv2 = async {
                        let mut rx = 0;
                        while let Some(_msg) = sub2.next().await {
                            rx += 1;
                            if rx + 1 == N_REQS {
                                break;
                            }
                        }
                    };

                    let (send_start, _, _) = tokio::join!(send, recv1, recv2);
                    let recv_end = Instant::now();

                    let elapsed = recv_end.duration_since(send_start);
                    let avg_throughput = N_REQS as f64 / elapsed.as_secs_f64();
                    let mbps = avg_throughput * MSG_SIZE as f64 / 1_000_000.0;
                    tracing::debug!(
                        "Throughput: {:.0} msgs/s {:.0} MB/s, Avg time: {:.2} ms",
                        avg_throughput,
                        mbps,
                        elapsed.as_millis()
                    );
                })
            });
    }

    /// Last run:
    /// Median: 49.86 ms, 1.026 GB/s, 2.005 Mitem/s
    #[divan::bench]
    fn pubsub_3_publishers(bencher: divan::Bencher) {
        const N_PUBLISHERS: usize = 3;

        // create a multi-threaded tokio runtime
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();

        let mut pub1 = PubSocket::with_options(
            Tcp::new(),
            PubOptions {
                session_buffer_size: N_REQS,
                flush_interval: Some(Duration::from_micros(100)),
                backpressure_boundary: 1024 * 64,
                ..Default::default()
            },
        );

        let mut pub2 = PubSocket::with_options(
            Tcp::new(),
            PubOptions {
                session_buffer_size: N_REQS,
                flush_interval: Some(Duration::from_micros(100)),
                backpressure_boundary: 1024 * 64,
                ..Default::default()
            },
        );

        let mut pub3 = PubSocket::with_options(
            Tcp::new(),
            PubOptions {
                session_buffer_size: N_REQS,
                flush_interval: Some(Duration::from_micros(100)),
                backpressure_boundary: 1024 * 64,
                ..Default::default()
            },
        );

        let mut sub = SubSocket::with_options(
            Tcp::new_with_options(TcpOptions::default().with_blocking_connect()),
            SubOptions {
                ingress_buffer_size: N_REQS * N_PUBLISHERS,
                read_buffer_size: 1024 * 64,
                ..Default::default()
            },
        );

        // Set up the socket connections
        rt.block_on(async {
            pub1.bind("127.0.0.1:0").await.unwrap();
            pub2.bind("127.0.0.1:0").await.unwrap();
            pub3.bind("127.0.0.1:0").await.unwrap();

            sub.connect(&pub1.local_addr().unwrap().to_string())
                .await
                .unwrap();

            sub.connect(&pub2.local_addr().unwrap().to_string())
                .await
                .unwrap();

            sub.connect(&pub3.local_addr().unwrap().to_string())
                .await
                .unwrap();

            sub.subscribe("HELLO".to_string()).await.unwrap();

            // Give some time for the background connection process to run
            tokio::time::sleep(Duration::from_millis(10)).await;
        });

        bencher
            .counter(ItemsCount::new(N_REQS as u64))
            .counter(BytesCount::new((N_REQS * MSG_SIZE) as u64))
            .with_inputs(|| -> Vec<Bytes> {
                let mut rng = rand::thread_rng();
                (0..N_REQS)
                    .map(|_| {
                        let mut vec = vec![0u8; MSG_SIZE];
                        rng.fill(&mut vec[..]);
                        Bytes::from(vec)
                    })
                    .collect()
            })
            .bench_local_values(|msg_vec: Vec<Bytes>| {
                rt.block_on(async {
                    let send1 = async {
                        let start = Instant::now();
                        for msg in &msg_vec {
                            pub1.publish("HELLO".to_string(), msg.clone())
                                .await
                                .unwrap();
                        }

                        tracing::info!("Done sending in {:?}", start.elapsed());
                        start
                    }
                    .instrument(tracing::info_span!("pub1"));

                    let send2 = async {
                        let start = Instant::now();
                        for msg in &msg_vec {
                            pub2.publish("HELLO".to_string(), msg.clone())
                                .await
                                .unwrap();
                        }

                        tracing::info!("Done sending in {:?}", start.elapsed());
                        start
                    }
                    .instrument(tracing::info_span!("pub2"));

                    let send3 = async {
                        let start = Instant::now();
                        for msg in &msg_vec {
                            pub3.publish("HELLO".to_string(), msg.clone())
                                .await
                                .unwrap();
                        }

                        tracing::info!("Done sending in {:?}", start.elapsed());
                        start
                    }
                    .instrument(tracing::info_span!("pub3"));

                    let recv = async {
                        let start = Instant::now();
                        let mut rx = 0;
                        while let Some(_msg) = sub.next().await {
                            rx += 1;
                            if rx + 1 == N_REQS * N_PUBLISHERS {
                                break;
                            }
                        }

                        tracing::info!("Done in {:?}", start.elapsed());
                    }
                    .instrument(tracing::info_span!("sub"));

                    let (send_start, _, _, _) = tokio::join!(send1, send2, send3, recv);
                    let recv_end = Instant::now();

                    let elapsed = recv_end.duration_since(send_start);
                    let avg_throughput = N_REQS as f64 / elapsed.as_secs_f64();
                    let mbps = avg_throughput * MSG_SIZE as f64 / 1_000_000.0;
                    tracing::debug!(
                        "Throughput: {:.0} msgs/s {:.0} MB/s, Avg time: {:.2} ms",
                        avg_throughput,
                        mbps,
                        elapsed.as_millis()
                    );
                })
            });
    }

    fn pubsub_with_runtime(bencher: divan::Bencher, rt: tokio::runtime::Runtime) {
        let buffer_size = 1024 * 64;

        let mut pub_socket = PubSocket::with_options(
            Tcp::new(),
            PubOptions {
                flush_interval: Some(Duration::from_micros(100)),
                backpressure_boundary: buffer_size,
                session_buffer_size: N_REQS,
                ..Default::default()
            },
        );

        let mut sub = SubSocket::with_options(
            Tcp::new_with_options(TcpOptions::default().with_blocking_connect()),
            SubOptions {
                read_buffer_size: buffer_size,
                ingress_buffer_size: N_REQS,
                ..Default::default()
            },
        );

        // Set up the socket connections
        rt.block_on(async {
            pub_socket.bind("127.0.0.1:0").await.unwrap();

            let addr = pub_socket.local_addr().unwrap();
            sub.connect(&addr.to_string()).await.unwrap();

            sub.subscribe("HELLO".to_string()).await.unwrap();

            // Give some time for the background connection process to run
            tokio::time::sleep(Duration::from_millis(10)).await;
        });

        bencher
            .counter(ItemsCount::new(N_REQS as u64))
            .counter(BytesCount::new((N_REQS * MSG_SIZE) as u64))
            .with_inputs(|| -> Vec<Bytes> {
                let mut rng = rand::thread_rng();
                (0..N_REQS)
                    .map(|_| {
                        let mut vec = vec![0u8; MSG_SIZE];
                        rng.fill(&mut vec[..]);
                        Bytes::from(vec)
                    })
                    .collect()
            })
            .bench_local_values(|msg_vec: Vec<Bytes>| {
                rt.block_on(async {
                    let send = async {
                        let start = Instant::now();
                        for msg in msg_vec {
                            pub_socket.publish("HELLO".to_string(), msg).await.unwrap();
                        }

                        start
                    }
                    .instrument(tracing::info_span!("publisher"));

                    let recv = async {
                        let mut rx = 0;
                        while let Some(_msg) = sub.next().await {
                            rx += 1;
                            if rx + 1 == N_REQS {
                                break;
                            }
                        }

                        Instant::now()
                    }
                    .instrument(tracing::info_span!("subscriber"));

                    let (send_start, recv_end) = tokio::join!(send, recv);

                    let elapsed = recv_end.duration_since(send_start);
                    let avg_throughput = N_REQS as f64 / elapsed.as_secs_f64();
                    let mbps = avg_throughput * MSG_SIZE as f64 / 1_000_000.0;
                    tracing::debug!(
                        "Throughput: {:.0} msgs/s {:.0} MB/s, Avg time: {:.2} ms",
                        avg_throughput,
                        mbps,
                        elapsed.as_millis()
                    );
                })
            });
    }
}
