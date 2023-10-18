use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::StreamExt;
use msg_socket::{PubOptions, PubSocket, SubOptions, SubSocket};
use msg_transport::{Tcp, TcpOptions};

#[tokio::main]
async fn main() {
    // Configure the publisher socket with options
    let mut pub_socket = PubSocket::with_options(
        Tcp::new(),
        PubOptions {
            session_buffer_size: 1024,
            flush_interval: Some(Duration::from_micros(100)),
            max_connections: None,
        },
    );

    // Configure the subscribers with options
    let mut sub1 = SubSocket::with_options(
        // TCP transport with blocking connect, usually connection happens in the background.
        Tcp::new_with_options(TcpOptions::default().with_blocking_connect()),
        SubOptions {
            ingress_buffer_size: 1024,
            ..Default::default()
        },
    );

    let mut sub2 = SubSocket::with_options(
        Tcp::new_with_options(TcpOptions::default().with_blocking_connect()),
        SubOptions {
            ingress_buffer_size: 1024,
            ..Default::default()
        },
    );

    tracing::info!("Setting up the sockets...");
    pub_socket.bind("127.0.0.1:0").await.unwrap();
    let pub_addr = pub_socket.local_addr().unwrap().to_string();
    tracing::info!("Publisher listening on: {}", pub_addr);

    sub1.connect(&pub_addr).await.unwrap();

    sub1.subscribe("HELLO_TOPIC".to_string()).await.unwrap();
    tracing::info!("Subscriber 1 connected and subscribed to HELLO_TOPIC");

    sub2.connect(&pub_addr).await.unwrap();

    sub2.subscribe("HELLO_TOPIC".to_string()).await.unwrap();
    tracing::info!("Subscriber 2 connected and subscribed to HELLO_TOPIC");

    // PROBLEM: I think the problem here stems from the fact that downstream we have a future
    // that is not sync. Probably the connection / auth future. This works without the call
    // to `unsubscribe`, but fails with it. The problem stems from sub socket containing a reference to
    // the transport, which is not sync.
    tokio::spawn(async move {
        loop {
            let recv = sub1.next().await.unwrap();
            if bytes_to_string(recv.into_payload()).contains("10") {
                tracing::info!("Received message 10, unsubscribing...");
                sub1.unsubscribe("HELLO_TOPIC".to_string()).await.unwrap();
            }
        }
    });

    tokio::spawn(async move {
        loop {
            let recv = sub2.next().await.unwrap();
        }
    });

    for i in 0..1000 {
        pub_socket
            .publish("HELLO_TOPIC".to_string(), format!("Message {i}").into())
            .await
            .unwrap();
    }
}

fn bytes_to_string(b: Bytes) -> String {
    String::from_utf8(b.to_vec()).unwrap()
}
