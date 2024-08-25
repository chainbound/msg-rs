use std::env::temp_dir;

use bytes::Bytes;
use tokio_stream::StreamExt;

use msg::{ipc::Ipc, RepSocket, ReqSocket};

#[tokio::main]
async fn main() {
    let _ = tracing_subscriber::fmt::try_init();

    // Initialize the reply socket (server side) with a transport
    let mut rep = RepSocket::new(Ipc::default());

    // use a temporary file as the socket path
    let path = temp_dir().join("test.sock");
    rep.bind_path(path.clone()).await.unwrap();
    println!("Listening on {:?}", rep.local_addr().unwrap());

    // Initialize the request socket (client side) with a transport
    let mut req = ReqSocket::new(Ipc::default());
    req.connect_path(path).await.unwrap();

    tokio::spawn(async move {
        // Receive the request and respond with "world"
        // RepSocket implements `Stream`
        let req = rep.next().await.unwrap();
        println!("Message: {:?}", req.msg());

        req.respond(Bytes::from("world")).unwrap();
    });

    let res: Bytes = req.request(Bytes::from("helloooo!")).await.unwrap();
    println!("Response: {:?}", res);

    // Access the socket statistics
    let stats = req.stats();
    println!(
        "Sent: {}B, Received: {}B | time: {}μs",
        stats.bytes_tx(),
        stats.bytes_rx(),
        stats.rtt()
    );
}
