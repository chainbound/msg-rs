use bytes::Bytes;
use msg_socket::ReqOptions;
use msg_wire::compression::GzipCompressor;
use tokio_stream::StreamExt;

use msg::{tcp::Tcp, RepSocket, ReqSocket};

#[tokio::main]
async fn main() {
    // Initialize the reply socket (server side) with a transport
    let mut rep = RepSocket::new(Tcp::default());
    rep.bind("0.0.0.0:4444").await.unwrap();

    // Initialize the request socket (client side) with a transport
    let mut req =
        ReqSocket::with_options(Tcp::default(), ReqOptions::default().min_compress_size(0))
            // Enable Gzip compression (compression level 6)
            .with_compressor(GzipCompressor::new(6));

    req.connect("0.0.0.0:4444").await.unwrap();

    tokio::spawn(async move {
        // Receive the request and respond with "world"
        // RepSocket implements `Stream`
        let req = rep.next().await.unwrap();
        println!("Message: {:?}", req.msg());

        req.respond(Bytes::from("world")).unwrap();
    });

    let res: Bytes = req.request(Bytes::from("hello")).await.unwrap();
    println!("Response: {:?}", res);
}
