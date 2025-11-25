use std::{env::temp_dir, time::Duration};

use bytes::Bytes;
use criterion::{
    BenchmarkGroup, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
    measurement::WallTime,
};
use futures::StreamExt;
use pprof::criterion::Output;
use rand::Rng;

use msg::{Address, Transport, ipc::Ipc};
use msg_socket::{RepSocket, ReqOptions, ReqSocket};
use msg_transport::tcp::Tcp;
use tokio::runtime::Runtime;

const N_REQS: usize = 10_000;
const PAR_FACTOR: usize = 64;
const MSG_SIZE: usize = 512;

// Using jemalloc improves performance by ~10%
#[cfg(all(not(windows), not(target_env = "musl")))]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

struct PairBenchmark<T: Transport<A>, A: Address> {
    rt: Runtime,
    req: ReqSocket<T, A>,
    rep: Option<RepSocket<T, A>>,

    n_reqs: usize,
    msg_sizes: Vec<usize>,
}

impl<T: Transport<A>, A: Address> PairBenchmark<T, A> {
    fn init(&mut self, addr: A) {
        let mut rep = self.rep.take().unwrap();
        // setup the socket connections
        self.rt.block_on(async {
            rep.try_bind(vec![addr]).await.unwrap();

            self.req.try_connect(rep.local_addr().unwrap().clone()).await.unwrap();

            tokio::spawn(async move {
                rep.map(|req| async move {
                    let msg = req.msg().clone();
                    req.respond(msg).unwrap();
                })
                .buffer_unordered(PAR_FACTOR)
                .for_each(|_| async {})
                .await;
            });

            tokio::time::sleep(Duration::from_millis(10)).await;
        });
    }

    fn bench_request_throughput(&mut self, mut group: BenchmarkGroup<'_, WallTime>) {
        for size in &self.msg_sizes {
            let requests = generate_requests(self.n_reqs, *size);

            group.throughput(Throughput::Bytes(*size as u64 * self.n_reqs as u64));
            group.bench_function(BenchmarkId::from_parameter(size), |b| {
                b.iter(|| {
                    let requests = requests.clone();
                    self.rt.block_on(async {
                        tokio_stream::iter(requests)
                            .map(|msg| self.req.request(msg))
                            .buffer_unordered(PAR_FACTOR)
                            .for_each(|_| async {})
                            .await;
                    });
                });
            });
        }

        group.finish();
    }

    fn bench_rps(&mut self, mut group: BenchmarkGroup<'_, WallTime>) {
        for size in &self.msg_sizes {
            let requests = generate_requests(self.n_reqs, *size);

            group.throughput(Throughput::Elements(self.n_reqs as u64));
            group.bench_function(BenchmarkId::from_parameter(size), |b| {
                b.iter(|| {
                    let requests = requests.clone();
                    self.rt.block_on(async {
                        tokio_stream::iter(requests)
                            .map(|msg| self.req.request(msg))
                            .buffer_unordered(PAR_FACTOR)
                            .for_each(|_| async {})
                            .await;
                    });
                });
            });
        }

        group.finish();
    }
}

fn generate_requests(n_reqs: usize, msg_size: usize) -> Vec<Bytes> {
    let mut rng = rand::thread_rng();
    (0..n_reqs)
        .map(|_| {
            let mut vec = vec![0u8; msg_size];
            rng.fill(&mut vec[..]);
            Bytes::from(vec)
        })
        .collect()
}

fn reqrep_single_thread_tcp(c: &mut Criterion) {
    let _ = tracing_subscriber::fmt::try_init();

    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    let req = ReqSocket::with_options(Tcp::default(), ReqOptions::default());

    let rep = RepSocket::new(Tcp::default());

    let mut bench = PairBenchmark {
        rt,
        req,
        rep: Some(rep),
        n_reqs: N_REQS,
        msg_sizes: vec![MSG_SIZE, MSG_SIZE * 8, MSG_SIZE * 64, MSG_SIZE * 128],
    };

    bench.init("127.0.0.1:0".parse().unwrap());
    let mut group = c.benchmark_group("reqrep_single_thread_tcp_bytes");
    group.sample_size(10);
    bench.bench_request_throughput(group);

    let mut group = c.benchmark_group("reqrep_single_thread_tcp_rps");
    group.sample_size(10);
    bench.bench_rps(group);
}

fn reqrep_multi_thread_tcp(c: &mut Criterion) {
    let _ = tracing_subscriber::fmt::try_init();

    let rt =
        tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();

    let req =
        ReqSocket::with_options(Tcp::default(), ReqOptions::default().write_buffer(1024 * 16));

    let rep = RepSocket::new(Tcp::default());

    let mut bench = PairBenchmark {
        rt,
        req,
        rep: Some(rep),
        n_reqs: N_REQS,
        msg_sizes: vec![MSG_SIZE, MSG_SIZE * 8, MSG_SIZE * 64, MSG_SIZE * 128],
    };

    bench.init("127.0.0.1:0".parse().unwrap());
    let mut group = c.benchmark_group("reqrep_multi_thread_tcp_bytes");
    group.sample_size(10);
    bench.bench_request_throughput(group);

    let mut group = c.benchmark_group("reqrep_multi_thread_tcp_rps");
    group.sample_size(10);
    bench.bench_rps(group);
}

fn reqrep_single_thread_ipc(c: &mut Criterion) {
    let _ = tracing_subscriber::fmt::try_init();

    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    let req = ReqSocket::new(Ipc::default());
    let rep = RepSocket::new(Ipc::default());

    let mut bench = PairBenchmark {
        rt,
        req,
        rep: Some(rep),
        n_reqs: N_REQS,
        msg_sizes: vec![MSG_SIZE, MSG_SIZE * 8, MSG_SIZE * 64, MSG_SIZE * 128],
    };

    bench.init(temp_dir().join("msg-bench-reqrep-ipc.sock"));
    let mut group = c.benchmark_group("reqrep_single_thread_ipc_bytes");
    group.sample_size(10);
    bench.bench_request_throughput(group);

    let mut group = c.benchmark_group("reqrep_single_thread_ipc_rps");
    group.sample_size(10);
    bench.bench_rps(group);
}

fn reqrep_multi_thread_ipc(c: &mut Criterion) {
    let _ = tracing_subscriber::fmt::try_init();

    let rt =
        tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();

    let req = ReqSocket::new(Ipc::default());
    let rep = RepSocket::new(Ipc::default());

    let mut bench = PairBenchmark {
        rt,
        req,
        rep: Some(rep),
        n_reqs: N_REQS,
        msg_sizes: vec![MSG_SIZE, MSG_SIZE * 8, MSG_SIZE * 64, MSG_SIZE * 128],
    };

    bench.init(temp_dir().join("msg-bench-reqrep-ipc-multi.sock"));
    let mut group = c.benchmark_group("reqrep_multi_thread_ipc_bytes");
    group.sample_size(10);
    bench.bench_request_throughput(group);

    let mut group = c.benchmark_group("reqrep_multi_thread_ipc_rps");
    group.sample_size(10);
    bench.bench_rps(group);
}

criterion_group! {
    name = benches;
    config = Criterion::default().warm_up_time(Duration::from_secs(1)).with_profiler(pprof::criterion::PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = reqrep_single_thread_tcp, reqrep_multi_thread_tcp, reqrep_single_thread_ipc, reqrep_multi_thread_ipc
}

// Runs various benchmarks for the `ReqSocket` and `RepSocket`.
criterion_main!(benches);
