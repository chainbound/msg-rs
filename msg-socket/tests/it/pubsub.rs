use bytes::Bytes;
use msg_sim::{namespace::run_on_namespace, Protocol, Simulation, SimulationConfig, Simulator};
use rand::Rng;
use std::{
    collections::HashSet,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};
use tokio::{sync::mpsc, task::JoinSet};
use tokio_stream::StreamExt;

use msg_socket::{PubSocket, SubSocket};
use msg_transport::{quic::Quic, tcp::Tcp, Transport};

const TOPIC: &str = "test";

/// Single publisher, single subscriber
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pubsub_channel() {
    let _ = tracing_subscriber::fmt::try_init();

    #[cfg(target_os = "linux")]
    let addr = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1));
    #[cfg(target_os = "macos")]
    let addr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

    let mut simulator = Simulator::new(1);
    let result = simulator.start(
        addr,
        SimulationConfig {
            latency: Some(Duration::from_millis(500)),
            bw: None,
            #[cfg(target_os = "linux")]
            burst: None,
            #[cfg(target_os = "linux")]
            limit: None,
            plr: Some(20_f64),
            protocols: vec![Protocol::UDP, Protocol::TCP],
        },
    );

    assert!(result.is_ok());

    let simulation = simulator.active_sims.get(&addr).unwrap();

    let result = pubsub_channel_transport(build_tcp, simulation).await;

    assert!(result.is_ok());

    let result = pubsub_channel_transport(build_quic, simulation).await;

    assert!(result.is_ok());

    simulator.stop(addr);
}

async fn pubsub_channel_transport<F: Fn() -> T, T: Transport + Send + Sync + Unpin + 'static>(
    new_transport: F,
    simulation: &Simulation,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut publisher = PubSocket::new(new_transport());
    let socket_addr = SocketAddr::new(simulation.endpoint, 0);

    #[cfg(target_os = "linux")]
    let publisher = {
        run_on_namespace(&simulation.namespace_name(), || {
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

    #[cfg(target_os = "linux")]
    let addr = IpAddr::V4(Ipv4Addr::new(192, 168, 2, 1));
    #[cfg(target_os = "macos")]
    let addr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

    let mut simulator = Simulator::new(2);
    let result = simulator.start(
        addr,
        SimulationConfig {
            latency: Some(Duration::from_millis(500)),
            bw: None,
            #[cfg(target_os = "linux")]
            burst: None,
            #[cfg(target_os = "linux")]
            limit: None,
            plr: Some(20_f64),
            protocols: vec![Protocol::UDP, Protocol::TCP],
        },
    );

    assert!(result.is_ok());

    let simulation = simulator.active_sims.get(&addr).unwrap();

    let result = pubsub_fan_out_transport(build_tcp, simulation, 20).await;

    assert!(result.is_ok());

    let result = pubsub_fan_out_transport(build_quic, simulation, 20).await;

    assert!(result.is_ok());

    simulator.stop(addr);
}

async fn pubsub_fan_out_transport<
    F: Fn() -> T + Send + 'static + Copy,
    T: Transport + Send + Sync + Unpin + 'static,
>(
    new_transport: F,
    simulation: &Simulation,
    subscibers: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut publisher = PubSocket::new(new_transport());

    let mut sub_tasks = JoinSet::new();

    let socket_addr = SocketAddr::new(simulation.endpoint, 0);

    #[cfg(target_os = "linux")]
    let publisher = {
        run_on_namespace(&simulation.namespace_name(), || {
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

    #[cfg(target_os = "linux")]
    let addr = IpAddr::V4(Ipv4Addr::new(192, 168, 3, 1));
    #[cfg(target_os = "macos")]
    let addr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

    let mut simulator = Simulator::new(3);
    let result = simulator.start(
        addr,
        SimulationConfig {
            latency: Some(Duration::from_millis(500)),
            bw: None,
            #[cfg(target_os = "linux")]
            burst: None,
            #[cfg(target_os = "linux")]
            limit: None,
            plr: Some(20_f64),
            protocols: vec![Protocol::UDP, Protocol::TCP],
        },
    );

    assert!(result.is_ok());

    let simulation = simulator.active_sims.get(&addr).unwrap();

    let result = pubsub_fan_in_transport(build_tcp, simulation, 5).await;

    assert!(result.is_ok());

    let result = pubsub_fan_in_transport(build_quic, simulation, 5).await;

    assert!(result.is_ok());

    simulator.stop(addr);
}

async fn pubsub_fan_in_transport<
    F: Fn() -> T + Send + 'static + Copy,
    T: Transport + Send + Sync + Unpin + 'static,
>(
    new_transport: F,
    simulation: &Simulation,
    publishers: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut sub_tasks = JoinSet::new();

    let (tx, mut rx) = mpsc::channel(publishers);

    let namespace = simulation.namespace_name();
    let socket_addr = SocketAddr::new(simulation.endpoint, 0);

    for i in 0..publishers {
        let tx = tx.clone();
        let namespace = namespace.clone();
        sub_tasks.spawn(async move {
            let mut publisher = PubSocket::new(new_transport());
            inject_delay((100 * (i + 1)) as u64).await;

            #[cfg(target_os = "linux")]
            let publisher = {
                run_on_namespace(&namespace, || {
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
