use bytes::Bytes;
use futures::StreamExt;
use nix::sched::{setns, CloneFlags};
use std::{
    fs::File,
    process::{self, Command},
    thread,
    time::Duration,
};
use tokio::time::timeout;
use tracing::Instrument;

use msg::{tcp::Tcp, PubOptions, PubSocket, SubOptions, SubSocket};

#[tokio::main]
async fn main() {
    let _ = tracing_subscriber::fmt::try_init();
    // Configure the publisher socket with options
    let mut pub_socket = PubSocket::with_options(
        Tcp::default(),
        PubOptions::default()
            .backpressure_boundary(8192)
            .session_buffer_size(1024)
            .flush_interval(Duration::from_micros(100)),
    );

    // Configure the subscribers with options
    let mut sub1 = SubSocket::with_options(
        Tcp::default(),
        SubOptions::default().ingress_buffer_size(1024),
    );

    let mut sub2 = SubSocket::with_options(
        // TCP transport with blocking connect, usually connection happens in the background.
        Tcp::default(),
        SubOptions::default().ingress_buffer_size(1024),
    );

    let ns_fd = File::open("/var/run/netns/msg-sim-1").unwrap();

    println!("current thread id {:?}", thread::current().id());

    if setns(ns_fd, CloneFlags::CLONE_NEWNET).is_ok() {
        println!("Done changing network namespace");
        let _ = Command::new("sudo")
            .args(["ls", "-l", &format!("/proc/{}/ns/net", process::id())])
            .status();
    }

    tracing::info!("Setting up the sockets...");

    println!("current thread id {:?}", thread::current().id());
    let _ = Command::new("sudo")
        .args(["ls", "-l", &format!("/proc/{}/ns/net", process::id())])
        .status();
    pub_socket.bind("192.168.1.1:0").await.unwrap();

    let pub_addr = pub_socket.local_addr().unwrap();

    let host_ns_fd = File::open("/proc/1/ns/net");
    if setns(host_ns_fd.unwrap(), CloneFlags::CLONE_NEWNET).is_ok() {
        println!("done changing back to host namespace")
    }

    tracing::info!("Publisher listening on: {}", pub_addr);

    sub1.connect(pub_addr).await.unwrap();

    sub1.subscribe("HELLO_TOPIC".to_string()).await.unwrap();
    tracing::info!("Subscriber 1 connected and subscribed to HELLO_TOPIC");

    sub2.connect(pub_addr).await.unwrap();

    sub2.subscribe("HELLO_TOPIC".to_string()).await.unwrap();
    tracing::info!("Subscriber 2 connected and subscribed to HELLO_TOPIC");

    let t1 = tokio::spawn(
        async move {
            loop {
                // Wait for a message to arrive, or timeout after 2 seconds. If the unsubscription was succesful,
                // we should time out after the 10th message.
                let Ok(Some(recv)) = timeout(Duration::from_millis(20000), sub1.next()).await
                else {
                    tracing::warn!("Timeout waiting for message, stopping sub1");
                    break;
                };

                let string = bytes_to_string(recv.clone().into_payload());
                tracing::info!("Received message: {}", string);
                if string.contains("10") {
                    tracing::warn!("Received message 10, unsubscribing...");
                    sub1.unsubscribe("HELLO_TOPIC".to_string()).await.unwrap();
                }
            }
        }
        .instrument(tracing::info_span!("sub1")),
    );

    let t2 = tokio::spawn(
        async move {
            loop {
                let Ok(Some(recv)) = timeout(Duration::from_millis(10000), sub2.next()).await
                else {
                    tracing::warn!("Timeout waiting for message, stopping sub2");
                    break;
                };
                let string = bytes_to_string(recv.clone().into_payload());
                tracing::info!("Received message: {}", string);
            }
        }
        .instrument(tracing::info_span!("sub2")),
    );

    for i in 0..20 {
        tokio::time::sleep(Duration::from_millis(300)).await;
        pub_socket
            .publish("HELLO_TOPIC".to_string(), format!("Message {i}").into())
            .await
            .unwrap();
    }

    let _ = tokio::join!(t1, t2);
}

fn bytes_to_string(b: Bytes) -> String {
    String::from_utf8(b.to_vec()).unwrap()
}
