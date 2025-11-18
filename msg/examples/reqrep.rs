use bytes::Bytes;
use tokio_stream::StreamExt;

use msg::{RepSocket, ReqSocket, tcp::Tcp};

#[tokio::main]
async fn main() {
    // Initialize the reply socket (server side) with a transport
    let mut rep = RepSocket::new(Tcp::default());
    rep.bind("0.0.0.0:4444").await.unwrap();

    // Initialize the request socket (client side) with a transport
    let mut req = ReqSocket::new(Tcp::default());
    req.connect("0.0.0.0:4444").await.unwrap();

    tokio::spawn(async move {
        // Receive the request and respond with "world"
        // RepSocket implements `Stream`
        let req = rep.next().await.unwrap();
        println!("Message: {:?}", req.msg());

        req.respond(Bytes::from("world")).unwrap();
    });

    let res: Bytes = req.request(Bytes::from("helloooo!")).await.unwrap();
    println!("Response: {res:?}");

    // Access the socket statistics
    let stats = req.stats();
    println!(
        "Sent: {}B, Received: {}B | time: {}μs",
        stats.bytes_tx(),
        stats.bytes_rx(),
        stats.rtt()
    );

    let transport_stats = req.transport_stats();
    println!("Transport stats: {:?}", transport_stats);
}
