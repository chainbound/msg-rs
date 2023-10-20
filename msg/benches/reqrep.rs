use bytes::Bytes;
use futures::StreamExt;
use rand::Rng;

use msg_socket::{RepSocket, ReqOptions, ReqSocket};
use msg_transport::{Tcp, TcpOptions};

const N_REQS: usize = 100_000;
const PAR_FACTOR: usize = 64;
const MSG_SIZE: usize = 512;

/// Benchmark the throughput of request/response socket pairs over localhost
fn main() {
    let _ = tracing_subscriber::fmt::try_init();

    // Using jemalloc improves performance by ~10%
    #[cfg(all(not(windows), not(target_env = "musl")))]
    #[global_allocator]
    static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

    divan::main();
}

#[divan::bench_group(sample_count = 16)]
mod reqrep {

    use std::time::{Duration, Instant};

    use divan::counter::{BytesCount, ItemsCount};

    use super::*;

    #[divan::bench()]
    fn reqrep_single_thread_tcp(bencher: divan::Bencher) {
        // create a multi-threaded tokio runtime
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        reqrep_with_runtime(bencher, rt);
    }

    #[divan::bench()]
    fn reqrep_multi_thread_tcp(bencher: divan::Bencher) {
        // create a multi-threaded tokio runtime
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        reqrep_with_runtime(bencher, rt);
    }

    #[divan::bench]
    fn reqrep_2_request(bencher: divan::Bencher) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        let mut rep_socket = RepSocket::new(Tcp::new());
        let mut req1 = ReqSocket::with_options(
            Tcp::new_with_options(TcpOptions::default().with_blocking_connect()),
            ReqOptions::default(),
        );
        let mut req2 = ReqSocket::with_options(
            Tcp::new_with_options(TcpOptions::default().with_blocking_connect()),
            ReqOptions::default(),
        );

        // setup the socket connections
        rt.block_on(async {
            rep_socket.bind("127.0.0.1:0").await.unwrap();

            req1.connect(&rep_socket.local_addr().unwrap().to_string())
                .await
                .unwrap();

            req2.connect(&rep_socket.local_addr().unwrap().to_string())
                .await
                .unwrap();

            tokio::spawn(async move {
                // Receive the request and respond with "world"
                // RepSocket implements `Stream`
                rep_socket
                    .map(|req| async move {
                        req.respond(Bytes::from("hello")).unwrap();
                    })
                    .buffer_unordered(PAR_FACTOR)
                    .for_each(|_| async {})
                    .await;
            });

            tokio::time::sleep(Duration::from_millis(10)).await;
        });

        bencher
            .counter(ItemsCount::new(N_REQS as u64))
            .counter(BytesCount::new((N_REQS * MSG_SIZE) as u64))
            .with_inputs(|| -> Vec<Bytes> {
                // Prepare the messages to send
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
                    let start = Instant::now();
                    let send1 = tokio_stream::iter(msg_vec.clone())
                        .map(|msg| req1.request(msg))
                        .buffer_unordered(PAR_FACTOR)
                        .for_each(|_| async {});

                    let send2 = tokio_stream::iter(msg_vec)
                        .map(|msg| req1.request(msg))
                        .buffer_unordered(PAR_FACTOR)
                        .for_each(|_| async {});

                    tokio::join!(send1, send2);
                    let elapsed = start.elapsed();
                    let avg_throughput = N_REQS as f64 / elapsed.as_secs_f64();
                    let mbps = avg_throughput * MSG_SIZE as f64 / 1_000_000.0;
                    tracing::debug!(
                        "Throughput: {:.0} msgs/s {:.0} MB/s, Avg time: {:.2} ms",
                        avg_throughput,
                        mbps,
                        elapsed.as_millis()
                    );
                });
            });
    }

    fn reqrep_with_runtime(bencher: divan::Bencher, rt: tokio::runtime::Runtime) {
        //Note: Configure Tcp transport when #9 is merged.
        let mut req_socket = ReqSocket::with_options(
            Tcp::new_with_options(TcpOptions::default().with_blocking_connect()),
            ReqOptions::default(),
        );
        let mut rep_socket = RepSocket::new(Tcp::new());

        // setup the socket connections
        rt.block_on(async {
            rep_socket.bind("127.0.0.1:0").await.unwrap();

            req_socket
                .connect(&rep_socket.local_addr().unwrap().to_string())
                .await
                .unwrap();

            tokio::spawn(async move {
                // Receive the request and respond with "world"
                // RepSocket implements `Stream`
                rep_socket
                    .map(|req| async move {
                        req.respond(Bytes::from("hello")).unwrap();
                    })
                    .buffer_unordered(PAR_FACTOR)
                    .for_each(|_| async {})
                    .await;
            });

            tokio::time::sleep(Duration::from_millis(10)).await;
        });

        bencher
            .counter(ItemsCount::new(N_REQS as u64))
            .counter(BytesCount::new((N_REQS * MSG_SIZE) as u64))
            .with_inputs(|| -> Vec<Bytes> {
                // Prepare the messages to send
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
                    let start = Instant::now();
                    tokio_stream::iter(msg_vec)
                        .map(|msg| req_socket.request(msg))
                        .buffer_unordered(PAR_FACTOR)
                        .for_each(|_| async {})
                        .await;

                    let elapsed = start.elapsed();
                    let avg_throughput = N_REQS as f64 / elapsed.as_secs_f64();
                    let mbps = avg_throughput * MSG_SIZE as f64 / 1_000_000.0;
                    tracing::debug!(
                        "Throughput: {:.0} msgs/s {:.0} MB/s, Avg time: {:.2} ms",
                        avg_throughput,
                        mbps,
                        elapsed.as_millis()
                    );
                });
            });
    }
}
