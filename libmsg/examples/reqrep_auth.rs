//! Request-Reply with token-based authentication example.
//!
//! This example demonstrates using the built-in token authentication connection hooks.
//! For custom authentication logic, implement the `ConnectionHook` trait directly.

use bytes::Bytes;
use tokio_stream::StreamExt;

use msg::{RepSocket, ReqSocket, hooks, tcp::Tcp};

#[tokio::main]
async fn main() {
    // Initialize the reply socket (server side) with a token validation connection hook.
    // The ServerHook accepts all tokens in this example; use a custom validator for real apps.
    let mut rep =
        RepSocket::new(Tcp::default()).with_connection_hook(hooks::token::ServerHook::accept_all());
    rep.bind("0.0.0.0:4444").await.unwrap();

    // Initialize the request socket (client side) with a token connection hook
    let mut req = ReqSocket::new(Tcp::default())
        .with_connection_hook(hooks::token::ClientHook::new(Bytes::from("REQ")));

    req.connect("0.0.0.0:4444").await.unwrap();

    tokio::spawn(async move {
        // Receive the request and respond with "world"
        // RepSocket implements `Stream`
        let req = rep.next().await.unwrap();
        println!("Message: {:?}", req.msg());

        req.respond(Bytes::from("world")).unwrap();
    });

    let res: Bytes = req.request(Bytes::from("hello")).await.unwrap();
    println!("Response: {res:?}");
}
