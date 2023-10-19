use std::time::Duration;

use bytes::Bytes;
use tokio_stream::StreamExt;

use msg::{Authenticator, RepSocket, ReqOptions, ReqSocket, Tcp};

#[derive(Default)]
struct Auth;

impl Authenticator for Auth {
    fn authenticate(&self, id: &Bytes) -> bool {
        tracing::info!("Auth request from: {:?}", id);
        // Custom authentication logic
        true
    }
}

async fn start_rep() {
    // Initialize the reply socket (server side) with a transport
    // and an authenticator.
    let mut rep = RepSocket::new(Tcp::new()).with_auth(Auth::default());
    rep.bind("0.0.0.0:4444").await.unwrap();

    // Receive the request and respond with "world"
    // RepSocket implements `Stream`
    let req = rep.next().await.unwrap();
    tracing::info!("Message: {:?}", req.msg());

    req.respond(Bytes::from("world")).unwrap();
    tracing::warn!("Killing rep socket");
}

#[tokio::main]
async fn main() {
    let _ = tracing_subscriber::fmt::try_init();

    // Initialize the request socket (client side) with a transport
    // and an identifier. This will implicitly turn on client authentication.
    let mut req = ReqSocket::with_options(
        Tcp::new(),
        ReqOptions::default().with_token(Bytes::from("client1")),
    );

    tracing::info!("Trying to connect to rep socket...");
    req.connect("0.0.0.0:4444").await.unwrap();
    tokio::spawn(async move {
        tracing::info!("Sending request...");
        let res: Bytes = req.request(Bytes::from("hello")).await.unwrap();

        tracing::info!("Response: {:?}", res);
    });

    tokio::time::sleep(Duration::from_secs(2)).await;
    tracing::info!("Starting rep socket");
    tokio::spawn(start_rep());

    tokio::time::sleep(Duration::from_secs(2)).await;
}
