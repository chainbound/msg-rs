use bytes::Bytes;
use futures::StreamExt;
use msg_socket::SubOptions;
use std::time::Duration;
use tokio::time::timeout;
use tracing::{Instrument, info, info_span, warn};

use msg::{Authenticator, PubSocket, SubSocket, tcp::Tcp};

#[derive(Default)]
struct Auth;

impl Authenticator for Auth {
    fn authenticate(&self, id: &Bytes) -> bool {
        info!("Auth request from: {:?}", id);
        if id.as_ref() == b"client1" {
            info!("Client authenticated: {:?}", id);
            true
        } else {
            warn!("Unknown client: {:?}", id);
            false
        }
    }
}

#[tokio::main]
async fn main() {
    let _ = tracing_subscriber::fmt::try_init();
    // Configure the publisher socket with options
    let mut pub_socket = PubSocket::new(Tcp::default())
        // Enable the authenticator
        .with_auth(Auth);

    // Configure the subscribers with options
    let mut sub1 = SubSocket::with_options(
        Tcp::default(),
        // Set the authentication token
        SubOptions::default().with_auth_token(Bytes::from("client1")),
    );

    let mut sub2 = SubSocket::with_options(
        Tcp::default(),
        // Set the authentication token
        SubOptions::default().with_auth_token(Bytes::from("client2")),
    );

    tracing::info!("Setting up the sockets...");
    pub_socket.bind("127.0.0.1:0").await.unwrap();

    let pub_addr = pub_socket.local_addr().unwrap();
    info!("Publisher listening on: {}", pub_addr);

    sub1.connect(pub_addr).await.unwrap();

    sub1.subscribe("HELLO_TOPIC".to_string()).await.unwrap();
    info!("Subscriber 1 connected and subscribed to HELLO_TOPIC");

    sub2.connect(pub_addr).await.unwrap();

    sub2.subscribe("HELLO_TOPIC".to_string()).await.unwrap();
    info!("Subscriber 2 connected and subscribed to HELLO_TOPIC");

    let t1 = tokio::spawn(
        async move {
            loop {
                // Wait for a message to arrive, or timeout after 2 seconds. If the unsubscription
                // was succesful, we should time out after the 10th message.
                let Ok(Some(recv)) = timeout(Duration::from_millis(2000), sub1.next()).await else {
                    warn!("Timeout waiting for message, stopping sub1");
                    break;
                };

                let string = bytes_to_string(recv.clone().into_payload());
                info!("Received message: {}", string);
                if string.contains("10") {
                    warn!("Received message 10, unsubscribing...");
                    sub1.unsubscribe("HELLO_TOPIC".to_string()).await.unwrap();
                }
            }
        }
        .instrument(info_span!("sub1")),
    );

    let t2 = tokio::spawn(
        async move {
            loop {
                let Ok(Some(recv)) = timeout(Duration::from_millis(1000), sub2.next()).await else {
                    warn!("Timeout waiting for message, stopping sub2");
                    break;
                };
                let string = bytes_to_string(recv.clone().into_payload());
                info!("Received message: {}", string);
            }
        }
        .instrument(info_span!("sub2")),
    );

    for i in 0..20 {
        tokio::time::sleep(Duration::from_millis(300)).await;
        pub_socket.publish("HELLO_TOPIC".to_string(), format!("Message {i}").into()).await.unwrap();
    }

    let _ = tokio::join!(t1, t2);
}

fn bytes_to_string(b: Bytes) -> String {
    String::from_utf8(b.to_vec()).unwrap()
}
