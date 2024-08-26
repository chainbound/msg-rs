use bytes::Bytes;
use msg_sim::{Protocol, SimulationConfig, Simulator};
use rand::Rng;
use std::{collections::HashSet, net::IpAddr, time::Duration};
use tokio::{sync::mpsc, task::JoinSet};
use tokio_stream::StreamExt;

use msg_socket::{PubSocket, SubSocket};
use msg_transport::{quic::Quic, tcp::Tcp, Address, Transport};

const TOPIC: &str = "test";

fn init_simulation(addr: IpAddr, config: SimulationConfig) -> Simulator {
    let mut simulator = Simulator::new();
    simulator.start(addr, config).unwrap();

    simulator
}

/// Single publisher, single subscriber
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn pubsub_channel() {
    let _ = tracing_subscriber::fmt::try_init();

    let addr = "127.0.0.1".parse().unwrap();

    let mut simulator = init_simulation(
        addr,
        SimulationConfig {
            latency: Some(Duration::from_millis(50)),
            bw: None,
            plr: None,
            protocols: vec![Protocol::UDP, Protocol::TCP],
        },
    );

    let result = pubsub_channel_transport(build_tcp, "127.0.0.1:9879".parse().unwrap()).await;

    assert!(result.is_ok());

    let result = pubsub_channel_transport(build_quic, "127.0.0.1:9879".parse().unwrap()).await;

    assert!(result.is_ok());

    simulator.stop(addr);
}

async fn pubsub_channel_transport<F, T, A>(
    new_transport: F,
    addr: A,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: Fn() -> T,
    T: Transport<A> + Send + Sync + Unpin + 'static,
    A: Address,
{
    let mut publisher = PubSocket::new(new_transport());

    let mut subscriber = SubSocket::new(new_transport());
    subscriber.connect_inner(addr.clone()).await?;
    subscriber.subscribe(TOPIC).await?;

    inject_delay(400).await;

    publisher.try_bind(vec![addr]).await?;

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
#[ignore]
async fn pubsub_fan_out() {
    let _ = tracing_subscriber::fmt::try_init();

    let addr = "127.0.0.1".parse().unwrap();

    let mut simulator = init_simulation(
        addr,
        SimulationConfig {
            latency: Some(Duration::from_millis(150)),
            bw: None,
            plr: None,
            protocols: vec![Protocol::UDP, Protocol::TCP],
        },
    );

    let result = pubsub_fan_out_transport(build_tcp, 10, "127.0.0.1:9880".parse().unwrap()).await;

    assert!(result.is_ok());

    let result = pubsub_fan_out_transport(build_quic, 10, "127.0.0.1:9880".parse().unwrap()).await;

    assert!(result.is_ok());

    simulator.stop(addr);
}

async fn pubsub_fan_out_transport<
    F: Fn() -> T + Send + 'static + Copy,
    T: Transport<A> + Send + Sync + Unpin + 'static,
    A: Address,
>(
    new_transport: F,
    subscibers: usize,
    addr: A,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut publisher = PubSocket::new(new_transport());

    let mut sub_tasks = JoinSet::new();

    for i in 0..subscibers {
        let cloned = addr.clone();
        sub_tasks.spawn(async move {
            let mut subscriber = SubSocket::new(new_transport());
            inject_delay((100 * (i + 1)) as u64).await;

            subscriber.connect_inner(cloned).await.unwrap();
            inject_delay((1000 / (i + 1)) as u64).await;
            subscriber.subscribe(TOPIC).await.unwrap();

            let msg = subscriber.next().await.unwrap();
            tracing::info!("Received message: {:?}", msg);
            assert_eq!(TOPIC, msg.topic());
            assert_eq!("WORLD", msg.payload());
        });
    }

    inject_delay(400).await;

    publisher.try_bind(vec![addr]).await?;

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
#[ignore]
async fn pubsub_fan_in() {
    let _ = tracing_subscriber::fmt::try_init();

    let addr = "127.0.0.1".parse().unwrap();

    let mut simulator = init_simulation(
        addr,
        SimulationConfig {
            latency: Some(Duration::from_millis(150)),
            bw: None,
            plr: None,
            protocols: vec![Protocol::UDP, Protocol::TCP],
        },
    );

    let result = pubsub_fan_in_transport(build_tcp, 20, "127.0.0.1:9881".parse().unwrap()).await;

    assert!(result.is_ok());

    let result = pubsub_fan_in_transport(build_quic, 20, "127.0.0.1:9881".parse().unwrap()).await;

    assert!(result.is_ok());

    simulator.stop(addr);
}

async fn pubsub_fan_in_transport<
    F: Fn() -> T + Send + 'static + Copy,
    T: Transport<A> + Send + Sync + Unpin + 'static,
    A: Address,
>(
    new_transport: F,
    publishers: usize,
    addr: A,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut sub_tasks = JoinSet::new();

    let (tx, mut rx) = mpsc::channel(publishers);

    for i in 0..publishers {
        let tx = tx.clone();
        let addr = addr.clone();
        sub_tasks.spawn(async move {
            let mut publisher = PubSocket::new(new_transport());
            inject_delay((100 * (i + 1)) as u64).await;

            publisher.try_bind(vec![addr]).await.unwrap();

            let local_addr = publisher.local_addr().unwrap().clone();
            tx.send(local_addr).await.unwrap();

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

    for addr in addrs.clone() {
        inject_delay(500).await;
        subscriber.connect_inner(addr.clone()).await.unwrap();
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

        addrs.remove(msg.source());
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
