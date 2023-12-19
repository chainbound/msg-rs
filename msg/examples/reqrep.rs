use bytes::Bytes;
use tokio_stream::StreamExt;

use msg::{RepSocket, ReqSocket, Tcp};

#[tokio::main]
async fn main() {
    // Initialize the reply socket (server side) with a transport
    let mut rep = RepSocket::<Tcp>::new();
    rep.bind("0.0.0.0:4444".parse().unwrap()).await.unwrap();

    // Initialize the request socket (client side) with a transport
    let mut req = ReqSocket::<Tcp>::new();
    req.connect("0.0.0.0:4444".parse().unwrap()).await.unwrap();

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
