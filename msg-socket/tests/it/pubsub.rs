use bytes::Bytes;
use msg_sim::{namespace::run_on_namespace, test_ip_addr, Protocol, SimulationConfig, Simulator};
use rand::Rng;
use std::{
    collections::HashSet,
    net::{IpAddr, SocketAddr},
    time::Duration,
};
use tokio::{sync::mpsc, task::JoinSet};
use tokio_stream::StreamExt;

use msg_socket::{PubSocket, SubSocket};
use msg_transport::{quic::Quic, tcp::Tcp, Transport};

const TOPIC: &str = "test";

fn init_simulation(addr: IpAddr, config: SimulationConfig) -> Simulator {
    let mut simulator = Simulator::new();
    simulator.start(addr, config).unwrap();

    simulator
}

/// Single publisher, single subscriber
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pubsub_channel() {
    let _ = tracing_subscriber::fmt::try_init();

    let addr = test_ip_addr();

    let mut simulator = init_simulation(
        addr,
        SimulationConfig {
            latency: Some(Duration::from_millis(2000)),
            bw: None,
            burst: None,
            limit: None,
            plr: Some(20_f64),
            protocols: vec![Protocol::UDP, Protocol::TCP],
        },
    );

    let result = pubsub_channel_transport(build_tcp, addr).await;

    assert!(result.is_ok());

    let result = pubsub_channel_transport(build_quic, addr).await;

    assert!(result.is_ok());

    simulator.stop(addr);
}

async fn pubsub_channel_transport<F: Fn() -> T, T: Transport + Send + Sync + Unpin + 'static>(
    new_transport: F,
    addr: IpAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut publisher = PubSocket::new(new_transport());
    let socket_addr = SocketAddr::new(addr, 0);

    #[cfg(target_os = "linux")]
    let publisher = {
        run_on_namespace("msg-sim-1", || {
            Box::pin(async move {
                let _ = publisher.bind(socket_addr).await;
                Ok(publisher)
            })
        })
        .await?
    };
    #[cfg(target_os = "macos")]
    publisher.bind(test_socket_addr).await?;

    let mut subscriber = SubSocket::new(new_transport());
    subscriber.connect(publisher.local_addr().unwrap()).await?;
    subscriber.subscribe(TOPIC).await?;

    inject_delay(400).await;

    // Spawn a task to keep sending messages until the subscriber receives one (after connection process)
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
            publisher
                .publish(TOPIC, Bytes::from("WORLD"))
                .await
                .unwrap();
        }
    });

    let msg = subscriber.next().await.unwrap();
    tracing::info!("Received message: {:?}", msg);
    assert_eq!(TOPIC, msg.topic());
    assert_eq!("WORLD", msg.payload());

    Ok(())
}

/// Single publisher, multiple subscribers
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pubsub_fan_out() {
    let _ = tracing_subscriber::fmt::try_init();

    let addr = test_ip_addr();

    let mut simulator = init_simulation(
        addr,
        SimulationConfig {
            latency: Some(Duration::from_millis(150)),
            bw: None,
            burst: None,
            limit: None,
            plr: None,
            protocols: vec![Protocol::UDP, Protocol::TCP],
        },
    );

    let result = pubsub_fan_out_transport(build_tcp, addr, 20).await;

    assert!(result.is_ok());

    let result = pubsub_fan_out_transport(build_quic, addr, 20).await;

    assert!(result.is_ok());

    simulator.stop(addr);
}

async fn pubsub_fan_out_transport<
    F: Fn() -> T + Send + 'static + Copy,
    T: Transport + Send + Sync + Unpin + 'static,
>(
    new_transport: F,
    addr: IpAddr,
    subscibers: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut publisher = PubSocket::new(new_transport());

    let mut sub_tasks = JoinSet::new();

    let socket_addr = SocketAddr::new(addr, 0);

    #[cfg(target_os = "linux")]
    let publisher = {
        run_on_namespace("msg-sim-1", || {
            Box::pin(async move {
                let _ = publisher.bind(socket_addr).await;
                Ok(publisher)
            })
        })
        .await?
    };
    #[cfg(target_os = "macos")]
    publisher.bind(test_socket_addr).await?;

    inject_delay(400).await;

    let sub_addr = publisher.local_addr().unwrap();

    for i in 0..subscibers {
        sub_tasks.spawn(async move {
            let mut subscriber = SubSocket::new(new_transport());
            inject_delay((100 * (i + 1)) as u64).await;

            subscriber.connect(sub_addr).await.unwrap();
            inject_delay((1000 / (i + 1)) as u64).await;
            subscriber.subscribe(TOPIC).await.unwrap();

            let msg = subscriber.next().await.unwrap();
            tracing::info!("Received message: {:?}", msg);
            assert_eq!(TOPIC, msg.topic());
            assert_eq!("WORLD", msg.payload());
        });
    }

    // Spawn a task to keep sending messages until the subscriber receives one (after connection process)
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
            publisher
                .publish(TOPIC, Bytes::from("WORLD"))
                .await
                .unwrap();
        }
    });

    for _ in 0..subscibers {
        sub_tasks.join_next().await.unwrap().unwrap();
    }

    Ok(())
}

/// Multiple publishers, single subscriber
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pubsub_fan_in() {
    let _ = tracing_subscriber::fmt::try_init();

    let addr = test_ip_addr();

    let mut simulator = init_simulation(
        addr,
        SimulationConfig {
            latency: Some(Duration::from_millis(150)),
            bw: None,
            burst: None,
            limit: None,
            plr: None,
            protocols: vec![Protocol::UDP, Protocol::TCP],
        },
    );

    let result = pubsub_fan_in_transport(build_tcp, addr, 5).await;

    assert!(result.is_ok());

    let result = pubsub_fan_in_transport(build_quic, addr, 5).await;

    assert!(result.is_ok());

    simulator.stop(addr);
}

async fn pubsub_fan_in_transport<
    F: Fn() -> T + Send + 'static + Copy,
    T: Transport + Send + Sync + Unpin + 'static,
>(
    new_transport: F,
    addr: IpAddr,
    publishers: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut sub_tasks = JoinSet::new();

    let (tx, mut rx) = mpsc::channel(publishers);

    for i in 0..publishers {
        let tx = tx.clone();
        sub_tasks.spawn(async move {
            let mut publisher = PubSocket::new(new_transport());
            inject_delay((100 * (i + 1)) as u64).await;

            let socket_addr = SocketAddr::new(addr, 0);

            #[cfg(target_os = "linux")]
            let publisher = {
                run_on_namespace("msg-sim-1", || {
                    Box::pin(async move {
                        let _ = publisher.bind(socket_addr).await;
                        Ok(publisher)
                    })
                })
                .await
                .unwrap()
            };
            #[cfg(target_os = "macos")]
            publisher.bind(test_socket_addr).await?;

            let addr = publisher.local_addr().unwrap();
            tx.send(addr).await.unwrap();

            // Spawn a task to keep sending messages until the subscriber receives one (after connection process)
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    publisher
                        .publish(TOPIC, Bytes::from("WORLD"))
                        .await
                        .unwrap();
                }
            });
        });
    }

    drop(tx);

    let mut subscriber = SubSocket::new(new_transport());

    let mut addrs = HashSet::with_capacity(publishers);

    while let Some(addr) = rx.recv().await {
        addrs.insert(addr);
    }

    for addr in &addrs {
        inject_delay(500).await;
        subscriber.connect(addr).await.unwrap();
        subscriber.subscribe(TOPIC).await.unwrap();
    }

    loop {
        if addrs.is_empty() {
            break;
        }

        let msg = subscriber.next().await.unwrap();
        tracing::info!("Received message: {:?}", msg);
        assert_eq!(TOPIC, msg.topic());
        assert_eq!("WORLD", msg.payload());

        addrs.remove(&msg.source());
    }

    for _ in 0..publishers {
        sub_tasks.join_next().await.unwrap().unwrap();
    }

    Ok(())
}

fn build_tcp() -> Tcp {
    Tcp::default()
}

fn build_quic() -> Quic {
    Quic::default()
}

fn random_delay(upper_ms: u64) -> Duration {
    let mut rng = rand::thread_rng();
    let delay_ms = rng.gen_range(0..upper_ms);
    Duration::from_millis(delay_ms)
}

async fn inject_delay(upper_ms: u64) {
    tokio::time::sleep(random_delay(upper_ms)).await;
}
