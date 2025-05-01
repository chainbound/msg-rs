use bytes::Bytes;
use criterion::{
    criterion_group, criterion_main, measurement::WallTime, BenchmarkGroup, BenchmarkId, Criterion,
    Throughput,
};
use futures::StreamExt;
use msg::{ipc::Ipc, Address};
use pprof::criterion::{Output, PProfProfiler};
use rand::Rng;
use std::{
    env::temp_dir,
    time::{Duration, Instant},
};
use tokio::runtime::Runtime;

use msg_socket::{PubOptions, PubSocket, SubOptions, SubSocket};
use msg_transport::{quic::Quic, tcp::Tcp, Transport};

const N_REQS: usize = 10_000;
const MSG_SIZE: usize = 512;

// Using jemalloc improves performance by ~10%
#[cfg(all(not(windows), not(target_env = "musl")))]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

struct PairBenchmark<T: Transport<A>, A: Address> {
    rt: Runtime,
    publisher: PubSocket<T, A>,
    subscriber: SubSocket<T, A>,

    n_reqs: usize,
    msg_sizes: Vec<usize>,
}

impl<T: Transport<A> + Send + Sync + Unpin + 'static, A: Address> PairBenchmark<T, A> {
    /// Sets up the publisher and subscriber sockets.
    fn init(&mut self, addr: A) {
        // Set up the socket connections
        self.rt.block_on(async {
            self.publisher.try_bind(vec![addr]).await.unwrap();

            let addr = self.publisher.local_addr().unwrap();
            self.subscriber.connect_inner(addr.clone()).await.unwrap();

            self.subscriber.subscribe("HELLO".to_string()).await.unwrap();

            // Give some time for the background connection process to run
            tokio::time::sleep(Duration::from_millis(10)).await;
        });
    }

    fn bench_bytes_throughput(&mut self, mut group: BenchmarkGroup<'_, WallTime>) {
        for size in &self.msg_sizes {
            let messages = generate_messages(self.n_reqs, *size);

            group.throughput(Throughput::Bytes(*size as u64 * self.n_reqs as u64));
            group.bench_function(BenchmarkId::from_parameter(size), |b| {
                b.iter(|| {
                    self.rt.block_on(async {
                        let send = async {
                            let start = Instant::now();
                            for msg in &messages {
                                self.publisher
                                    .publish("HELLO".to_string(), msg.to_owned())
                                    .await
                                    .unwrap();
                            }

                            start
                        };

                        let recv = async {
                            let mut rx = 0;
                            while let Some(_msg) = self.subscriber.next().await {
                                rx += 1;
                                if rx + 1 == self.n_reqs {
                                    break;
                                }
                            }
                        };

                        tokio::join!(send, recv);
                    });
                });
            });
        }

        group.finish();
    }

    fn bench_message_throughput(&mut self, mut group: BenchmarkGroup<'_, WallTime>) {
        for size in &self.msg_sizes {
            let messages = generate_messages(self.n_reqs, *size);

            group.throughput(Throughput::Elements(self.n_reqs as u64));
            group.bench_function(BenchmarkId::from_parameter(size), |b| {
                b.iter(|| {
                    self.rt.block_on(async {
                        let send = async {
                            let start = Instant::now();
                            for msg in &messages {
                                self.publisher
                                    .publish("HELLO".to_string(), msg.to_owned())
                                    .await
                                    .unwrap();
                            }

                            start
                        };

                        let recv = async {
                            let mut rx = 0;
                            while let Some(_msg) = self.subscriber.next().await {
                                rx += 1;
                                if rx + 1 == self.n_reqs {
                                    break;
                                }
                            }
                        };

                        tokio::join!(send, recv);
                    });
                });
            });
        }

        group.finish();
    }
}

fn generate_messages(n_reqs: usize, msg_size: usize) -> Vec<Bytes> {
    let mut rng = rand::thread_rng();
    (0..n_reqs)
        .map(|_| {
            let mut vec = vec![0u8; msg_size];
            rng.fill(&mut vec[..]);
            Bytes::from(vec)
        })
        .collect()
}

fn pubsub_single_thread_tcp(c: &mut Criterion) {
    let _ = tracing_subscriber::fmt::try_init();

    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    let buffer_size = 1024 * 64;

    let publisher = PubSocket::with_options(
        Tcp::default(),
        PubOptions::default()
            .flush_interval(Duration::from_micros(100))
            .backpressure_boundary(buffer_size)
            .session_buffer_size(N_REQS * 2),
    );

    let subscriber = SubSocket::with_options(
        Tcp::default(),
        SubOptions::default().read_buffer_size(buffer_size).ingress_buffer_size(N_REQS * 2),
    );

    let mut bench = PairBenchmark {
        rt,
        publisher,
        subscriber,
        n_reqs: N_REQS,
        msg_sizes: vec![MSG_SIZE, MSG_SIZE * 8, MSG_SIZE * 64, MSG_SIZE * 128],
    };

    bench.init("127.0.0.1:0".parse().unwrap());

    let mut group = c.benchmark_group("pubsub_single_thread_tcp_bytes");
    group.sample_size(10);
    bench.bench_bytes_throughput(group);

    let mut group = c.benchmark_group("pubsub_single_thread_tcp_msgs");
    group.sample_size(10);
    bench.bench_message_throughput(group);
}

fn pubsub_multi_thread_tcp(c: &mut Criterion) {
    let _ = tracing_subscriber::fmt::try_init();

    let rt =
        tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();

    let buffer_size = 1024 * 64;

    let publisher = PubSocket::with_options(
        Tcp::default(),
        PubOptions::default()
            .flush_interval(Duration::from_micros(100))
            .backpressure_boundary(buffer_size)
            .session_buffer_size(N_REQS * 2),
    );

    let subscriber = SubSocket::with_options(
        Tcp::default(),
        SubOptions::default().read_buffer_size(buffer_size).ingress_buffer_size(N_REQS * 2),
    );

    let mut bench = PairBenchmark {
        rt,
        publisher,
        subscriber,
        n_reqs: N_REQS,
        msg_sizes: vec![MSG_SIZE, MSG_SIZE * 8, MSG_SIZE * 64, MSG_SIZE * 128],
    };

    bench.init("127.0.0.1:0".parse().unwrap());

    let mut group = c.benchmark_group("pubsub_multi_thread_tcp_bytes");
    group.sample_size(10);
    bench.bench_bytes_throughput(group);

    let mut group = c.benchmark_group("pubsub_multi_thread_tcp_msgs");
    group.sample_size(10);
    bench.bench_message_throughput(group);
}

fn pubsub_single_thread_quic(c: &mut Criterion) {
    let _ = tracing_subscriber::fmt::try_init();

    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    let buffer_size = 1024 * 64;

    let publisher = PubSocket::with_options(
        Quic::default(),
        PubOptions::default()
            .flush_interval(Duration::from_micros(100))
            .backpressure_boundary(buffer_size)
            .session_buffer_size(N_REQS * 2),
    );

    let subscriber = SubSocket::with_options(
        Quic::default(),
        SubOptions::default().read_buffer_size(buffer_size).ingress_buffer_size(N_REQS * 2),
    );

    let mut bench = PairBenchmark {
        rt,
        publisher,
        subscriber,
        n_reqs: N_REQS,
        msg_sizes: vec![MSG_SIZE, MSG_SIZE * 8, MSG_SIZE * 64, MSG_SIZE * 128],
    };

    bench.init("127.0.0.1:0".parse().unwrap());

    let mut group = c.benchmark_group("pubsub_single_thread_quic_bytes");
    group.sample_size(10);
    bench.bench_bytes_throughput(group);

    let mut group = c.benchmark_group("pubsub_single_thread_quic_msgs");
    group.sample_size(10);
    bench.bench_message_throughput(group);
}

fn pubsub_multi_thread_quic(c: &mut Criterion) {
    let _ = tracing_subscriber::fmt::try_init();

    let rt =
        tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();

    let buffer_size = 1024 * 64;

    let publisher = PubSocket::with_options(
        Quic::default(),
        PubOptions::default()
            .flush_interval(Duration::from_micros(100))
            .backpressure_boundary(buffer_size)
            .session_buffer_size(N_REQS * 2),
    );

    let subscriber = SubSocket::with_options(
        Quic::default(),
        SubOptions::default().read_buffer_size(buffer_size).ingress_buffer_size(N_REQS * 2),
    );

    let mut bench = PairBenchmark {
        rt,
        publisher,
        subscriber,
        n_reqs: N_REQS,
        msg_sizes: vec![MSG_SIZE, MSG_SIZE * 8, MSG_SIZE * 64, MSG_SIZE * 128],
    };

    bench.init("127.0.0.1:0".parse().unwrap());

    let mut group = c.benchmark_group("pubsub_multi_thread_quic_bytes");
    group.sample_size(10);
    bench.bench_bytes_throughput(group);

    let mut group = c.benchmark_group("pubsub_multi_thread_quic_msgs");
    group.sample_size(10);
    bench.bench_message_throughput(group);
}

fn pubsub_single_thread_ipc(c: &mut Criterion) {
    let _ = tracing_subscriber::fmt::try_init();

    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    let buffer_size = 1024 * 64;

    let publisher = PubSocket::with_options(
        Ipc::default(),
        PubOptions::default()
            .flush_interval(Duration::from_micros(100))
            .backpressure_boundary(buffer_size)
            .session_buffer_size(N_REQS * 2),
    );

    let subscriber = SubSocket::with_options(
        Ipc::default(),
        SubOptions::default().read_buffer_size(buffer_size).ingress_buffer_size(N_REQS * 2),
    );

    let mut bench = PairBenchmark {
        rt,
        publisher,
        subscriber,
        n_reqs: N_REQS,
        msg_sizes: vec![MSG_SIZE, MSG_SIZE * 8, MSG_SIZE * 64, MSG_SIZE * 128],
    };

    bench.init(temp_dir().join("msg-bench-pubsub-ipc.sock"));

    let mut group = c.benchmark_group("pubsub_single_thread_ipc_bytes");
    group.sample_size(10);
    bench.bench_bytes_throughput(group);

    let mut group = c.benchmark_group("pubsub_single_thread_ipc_msgs");
    group.sample_size(10);
    bench.bench_message_throughput(group);
}

fn pubsub_multi_thread_ipc(c: &mut Criterion) {
    let _ = tracing_subscriber::fmt::try_init();

    let rt =
        tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();

    let buffer_size = 1024 * 64;

    let publisher = PubSocket::with_options(
        Ipc::default(),
        PubOptions::default()
            .flush_interval(Duration::from_micros(100))
            .backpressure_boundary(buffer_size)
            .session_buffer_size(N_REQS * 2),
    );

    let subscriber = SubSocket::with_options(
        Ipc::default(),
        SubOptions::default().read_buffer_size(buffer_size).ingress_buffer_size(N_REQS * 2),
    );

    let mut bench = PairBenchmark {
        rt,
        publisher,
        subscriber,
        n_reqs: N_REQS,
        msg_sizes: vec![MSG_SIZE, MSG_SIZE * 8, MSG_SIZE * 64, MSG_SIZE * 128],
    };

    bench.init(temp_dir().join("msg-bench-pubsub-ipc-multi.sock"));

    let mut group = c.benchmark_group("pubsub_multi_thread_ipc_bytes");
    group.sample_size(10);
    bench.bench_bytes_throughput(group);

    let mut group = c.benchmark_group("pubsub_multi_thread_ipc_msgs");
    group.sample_size(10);
    bench.bench_message_throughput(group);
}

criterion_group! {
    name = benches;
    config = Criterion::default().warm_up_time(Duration::from_secs(1)).with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = pubsub_single_thread_tcp, pubsub_multi_thread_tcp, pubsub_single_thread_quic,
              pubsub_multi_thread_quic, pubsub_single_thread_ipc, pubsub_multi_thread_ipc
}

// Runs various benchmarks for the `PubSocket` and `SubSocket`.
criterion_main!(benches);
