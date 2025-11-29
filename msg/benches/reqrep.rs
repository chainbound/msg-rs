use std::{env::temp_dir, time::Duration};

use bytes::Bytes;
use criterion::{
    BenchmarkGroup, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
    measurement::WallTime,
};
use futures::StreamExt;
use pprof::criterion::Output;
use rand::Rng;

use msg::{
    Address, Transport,
    ipc::Ipc,
    tcp_tls::{TcpTls, config},
};
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
                    req.respond(Bytes::from_static(b"hello")).unwrap();
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
    let mut rng = rand::rng();
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
    let mut group = c.benchmark_group("reqrep_multi_thread_tcp_bytes");
    group.sample_size(10);
    bench.bench_request_throughput(group);

    let mut group = c.benchmark_group("reqrep_multi_thread_tcp_rps");
    group.sample_size(10);
    bench.bench_rps(group);
}

fn reqrep_multi_thread_tls(c: &mut Criterion) {
    let _ = tracing_subscriber::fmt::try_init();

    let rt =
        tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();

    let req = ReqSocket::with_options(
        TcpTls::new_client(
            config::Client::new("localhost".to_string())
                .with_ssl_connector(helpers::default_connector_builder().build()),
        ),
        ReqOptions::default(),
    );

    let rep = RepSocket::new(TcpTls::new_server(config::Server::new(
        helpers::default_acceptor_builder().build(),
    )));

    let mut bench = PairBenchmark {
        rt,
        req,
        rep: Some(rep),
        n_reqs: N_REQS,
        msg_sizes: vec![MSG_SIZE, MSG_SIZE * 8, MSG_SIZE * 64, MSG_SIZE * 128],
    };

    bench.init("127.0.0.1:0".parse().unwrap());
    let mut group = c.benchmark_group("reqrep_multi_thread_tls_bytes");
    group.sample_size(10);
    bench.bench_request_throughput(group);

    let mut group = c.benchmark_group("reqrep_multi_thread_tls_rps");
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
    targets = reqrep_single_thread_tcp, reqrep_multi_thread_tcp, reqrep_single_thread_ipc, reqrep_multi_thread_ipc, reqrep_multi_thread_tls
}

// Runs various benchmarks for the `ReqSocket` and `RepSocket`.
criterion_main!(benches);

mod helpers {
    use std::{path::PathBuf, str::FromStr as _};

    use openssl::ssl::{
        SslAcceptor, SslAcceptorBuilder, SslConnector, SslConnectorBuilder, SslFiletype, SslMethod,
    };

    /// Creates a default SSL acceptor builder for testing, with a trusted CA.
    pub fn default_acceptor_builder() -> SslAcceptorBuilder {
        let certificate_path =
            PathBuf::from_str("../testdata/certificates/server-cert.pem").unwrap();
        let private_key_path =
            PathBuf::from_str("../testdata/certificates/server-key.pem").unwrap();
        let ca_certificate_path =
            PathBuf::from_str("../testdata/certificates/ca-cert.pem").unwrap();

        assert!(certificate_path.exists(), "Certificate file does not exist");
        assert!(private_key_path.exists(), "Private key file does not exist");
        assert!(ca_certificate_path.exists(), "CA Certificate file does not exist");

        let mut acceptor_builder = SslAcceptor::mozilla_modern(SslMethod::tls()).unwrap();
        acceptor_builder.set_certificate_file(certificate_path, SslFiletype::PEM).unwrap();
        acceptor_builder.set_private_key_file(private_key_path, SslFiletype::PEM).unwrap();
        acceptor_builder.set_ca_file(ca_certificate_path).unwrap();
        acceptor_builder.set_cipher_list("ECDHE-RSA-AES128-GCM-SHA256").unwrap();
        acceptor_builder
    }

    /// Creates a default SSL connector builder for testing, with a trusted CA.
    /// It also has client certificate and private key set for mTLS testing.
    pub fn default_connector_builder() -> SslConnectorBuilder {
        let certificate_path =
            PathBuf::from_str("../testdata/certificates/client-cert.pem").unwrap();
        let private_key_path =
            PathBuf::from_str("../testdata/certificates/client-key.pem").unwrap();
        let ca_certificate_path =
            PathBuf::from_str("../testdata/certificates/ca-cert.pem").unwrap();

        assert!(certificate_path.exists(), "Certificate file does not exist");
        assert!(private_key_path.exists(), "Private key file does not exist");
        assert!(ca_certificate_path.exists(), "CA Certificate file does not exist");

        let mut connector_builder = SslConnector::builder(SslMethod::tls()).unwrap();
        connector_builder
            .set_ciphersuites(
                "TLS_AES_128_GCM_SHA256:TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256",
            )
            .unwrap();
        connector_builder.set_certificate_file(certificate_path, SslFiletype::PEM).unwrap();
        connector_builder.set_private_key_file(private_key_path, SslFiletype::PEM).unwrap();
        connector_builder.set_ca_file(ca_certificate_path).unwrap();

        connector_builder
    }
}
