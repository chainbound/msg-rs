use bytes::Bytes;
use futures::StreamExt;
use rand::Rng;

use msg_socket::{RepSocket, ReqSocket};
use msg_transport::{Tcp, TcpOptions};

const N_REQS: usize = 100000;
const PAR_FACTOR: usize = 64;
const REPEAT_TIMES: usize = 10;

/// Benchmark the throughput of a single request/response socket pair over localhost
fn main() {
    let _ = tracing_subscriber::fmt::try_init();

    // create a multi-threaded tokio runtime
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut rep = RepSocket::new(Tcp::new());
    let mut req = ReqSocket::new(Tcp::new_with_options(
        TcpOptions::default().with_blocking_connect(),
    ));

    // setup the socket connections
    rt.block_on(async {
        rep.bind("127.0.0.1:0").await.unwrap();

        req.connect(&rep.local_addr().unwrap().to_string())
            .await
            .unwrap();

        tokio::spawn(async move {
            rep.map(|req| async move {
                req.respond(Bytes::from("hello")).unwrap();
            })
            .buffer_unordered(PAR_FACTOR)
            .for_each(|_| async {})
            .await;
        });
    });

    // Prepare the messages to send
    let mut rng = rand::thread_rng();
    let msg_vec: Vec<Bytes> = (0..N_REQS)
        .map(|_| {
            let mut vec = vec![0u8; 512];
            rng.fill(&mut vec[..]);
            Bytes::from(vec)
        })
        .collect();

    let mut results = Vec::with_capacity(REPEAT_TIMES);
    for _ in 0..REPEAT_TIMES {
        rt.block_on(async {
            let vec = msg_vec.clone();
            let start = std::time::Instant::now();

            tokio_stream::iter(vec)
                .map(|msg| req.request(msg))
                .buffer_unordered(PAR_FACTOR)
                .for_each(|_| async {})
                .await;

            let elapsed = start.elapsed();

            results.push(elapsed);
        });
    }

    // print the results
    let total_time: std::time::Duration = results.iter().sum();
    let avg_time = total_time / (REPEAT_TIMES as u32);
    let avg_throughput = N_REQS as f64 / avg_time.as_secs_f64();

    tracing::info!(
        "Runs: {}, Avg throughput: {:.0} req/s, Avg time: {:.2} ms",
        REPEAT_TIMES,
        avg_throughput,
        avg_time.as_millis()
    );
}
