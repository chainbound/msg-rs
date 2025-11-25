use std::time::Duration;

use bytes::Bytes;
use tokio::sync::oneshot;
use tokio_stream::StreamExt;

use msg::{Authenticator, RepSocket, ReqOptions, ReqSocket, tcp::Tcp};
use tracing::{Instrument, error, info, info_span, instrument, warn};

#[derive(Default)]
struct Auth;

impl Authenticator for Auth {
    fn authenticate(&self, id: &Bytes) -> bool {
        info!("Auth request from: {:?}, authentication passed.", id);
        // Custom authentication logic
        true
    }
}

#[instrument(name = "RepSocket")]
async fn start_rep() {
    // Initialize the reply socket (server side) with a transport
    // and an authenticator.
    let mut rep = RepSocket::new(Tcp::default()).with_auth(Auth);
    while rep.bind("0.0.0.0:4444").await.is_err() {
        rep = RepSocket::new(Tcp::default()).with_auth(Auth);
        warn!("Failed to bind rep socket, retrying...");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // Receive the request and respond with "world"
    // RepSocket implements `Stream`
    let mut n_reqs = 0;
    loop {
        let req = rep.next().await.unwrap();
        n_reqs += 1;
        info!("Message: {:?}", req.msg());

        let msg = String::from_utf8_lossy(req.msg()).to_string();
        let msg_id = msg.split_whitespace().nth(1).unwrap().parse::<i32>().unwrap();

        if n_reqs == 5 {
            warn!(
                "RepSocket received the 5th request, dropping the request to trigger a timeout..."
            );

            continue;
        }

        let response = format!("PONG {msg_id}");
        req.respond(Bytes::from(response)).unwrap();
    }
}

#[tokio::main]
async fn main() {
    let _ = tracing_subscriber::fmt::try_init();

    // Initialize the request socket (client side) with a transport
    // and an identifier. This will implicitly turn on client authentication.
    let mut req = ReqSocket::with_options(
        Tcp::default(),
        ReqOptions::default().with_timeout(Duration::from_secs(4)).with_auth_token(Bytes::from("client1")),
    );

    let (tx, rx) = oneshot::channel();

    tokio::spawn(
        async move {
            tracing::info!("Trying to connect to rep socket... This will start the connection process in the background, it won't immediately connect.");
            req.connect("0.0.0.0:4444").await.unwrap();

            for i in 0..10 {
                info!("Sending request {i}...");
                if i == 0 {
                    warn!("At this point the RepSocket is not running yet, so the request will block while \
                    the ReqSocket continues to establish a connection. The RepSocket will be started in 3 seconds.");
                }

                let msg = format!("PING {i}");

                let res = loop {
                    match req.request(Bytes::from(msg.clone())).await {
                        Ok(res) => break res,
                        Err(e) => {
                            error!(err = ?e, "Request failed, retrying...");
                            tokio::time::sleep(Duration::from_millis(1000)).await;
                        }
                    }
                };

                info!("Response: {:?}", res);
                tokio::time::sleep(Duration::from_millis(1000)).await;
            }

            tx.send(true).unwrap();
        }
        .instrument(info_span!("ReqSocket")),
    );

    tokio::time::sleep(Duration::from_secs(3)).await;

    info!("==========================");
    info!("Starting the RepSocket now");
    info!("==========================");

    tokio::spawn(start_rep());

    // Wait for the client to finish
    rx.await.unwrap();
    info!("DONE. Sent all 10 PINGS and received 10 PONGS.");
}
