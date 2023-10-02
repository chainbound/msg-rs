use bytes::Bytes;
use tokio_stream::StreamExt;

use msg::{Authenticator, RepSocket, ReqOptions, ReqSocket, Tcp};

#[derive(Default)]
struct Auth;

impl Authenticator for Auth {
    fn authenticate(&self, id: &Bytes) -> bool {
        println!("Auth request from: {:?}", id);
        // Custom authentication logic
        true
    }
}

#[tokio::main]
async fn main() {
    // Initialize the reply socket (server side) with a transport
    // and an authenticator.
    let mut rep = RepSocket::new(Tcp::new()).with_auth(Auth::default());
    rep.bind("0.0.0.0:4444").await.unwrap();

    // Initialize the request socket (client side) with a transport
    // and an identifier. This will implicitly turn on client authentication.
    let mut req = ReqSocket::new_with_options(
        Tcp::new(),
        ReqOptions::default().with_client_id(Bytes::from("client1")),
    );

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
