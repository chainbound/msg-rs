use bytes::Bytes;
use futures::StreamExt;
use msg_transport::{Transport, quic::Quic};
use std::time::{Duration, Instant};
use tracing::info;

use libmsg::{Address, PubOptions, PubSocket, SubOptions, SubSocket, tcp::Tcp};

#[tokio::main]
async fn main() {
    let _ = tracing_subscriber::fmt::try_init();

    run_tcp().await;
    run_quic().await;
}

async fn run_tcp() {
    // Configure the publisher socket with options
    let mut pub_socket = PubSocket::with_options(
        Tcp::default(),
        PubOptions::default()
            .with_write_buffer_size(8192)
            .with_high_water_mark(1024)
            .with_write_buffer_linger(Some(Duration::from_micros(100))),
    );

    // Configure the subscribers with options
    let mut sub1 = SubSocket::with_options(
        Tcp::default(),
        SubOptions::default().with_ingress_queue_size(1024),
    );

    tracing::info!("Setting up the sockets...");
    pub_socket.bind("127.0.0.1:0").await.unwrap();
    let pub_addr = pub_socket.local_addr().unwrap();

    info!("Publisher listening on: {}", pub_addr);

    sub1.connect(pub_addr).await.unwrap();

    sub1.subscribe("HELLO_TOPIC".to_string()).await.unwrap();
    info!("Subscriber 1 connected and subscribed to HELLO_TOPIC");

    tokio::time::sleep(Duration::from_millis(1000)).await;

    run_transfer("TCP", &mut pub_socket, &mut sub1).await;
}

async fn run_quic() {
    // Configure the publisher socket with options
    let mut pub_socket = PubSocket::with_options(
        Quic::default(),
        PubOptions::default()
            .with_write_buffer_size(8192)
            .with_high_water_mark(1024)
            .with_write_buffer_linger(Some(Duration::from_micros(100))),
    );

    // Configure the subscribers with options
    let mut sub1 = SubSocket::with_options(
        // TCP transport with blocking connect, usually connection happens in the background.
        Quic::default(),
        SubOptions::default().with_ingress_queue_size(1024),
    );

    tracing::info!("Setting up the sockets...");
    pub_socket.bind("127.0.0.1:0").await.unwrap();
    let pub_addr = pub_socket.local_addr().unwrap();

    info!("Publisher listening on: {}", pub_addr);

    sub1.connect(pub_addr).await.unwrap();

    sub1.subscribe("HELLO_TOPIC".to_string()).await.unwrap();
    info!("Subscriber 1 connected and subscribed to HELLO_TOPIC");

    tokio::time::sleep(Duration::from_millis(1000)).await;

    run_transfer("QUIC", &mut pub_socket, &mut sub1).await;
}

async fn run_transfer<T: Transport<A>, A: Address>(
    transport: &str,
    pub_socket: &mut PubSocket<T, A>,
    sub_socket: &mut SubSocket<T, A>,
) {
    let data = Bytes::from(
        std::fs::read("./testdata/mainnetCapellaBlock7928030.ssz")
            .expect("failed to read test file"),
    );

    for _ in 0..100 {
        let start = Instant::now();
        pub_socket.publish("HELLO_TOPIC".to_string(), data.clone()).await.unwrap();

        let recv = sub_socket.next().await.unwrap();
        let elapsed = start.elapsed();
        info!("{} transfer took {:?}", transport, elapsed);
        assert_eq!(recv.into_payload(), data);

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
