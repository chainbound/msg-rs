# `msg-rs`
> A flexible and lightweight messaging library for distributed systems built with Rust and Tokio.

## Overview
`msg-rs` is a messaging library that was inspired by projects like [ZeroMQ](https://zeromq.org/) and [Nanomsg](https://nanomsg.org/).
It was built because we needed a Rust-native messaging library like those above.

## Features
- [ ] Multiple socket types
    - [x] Request/Reply
    - [ ] Channel
    - [ ] Publish/Subscribe
    - [ ] Push/Pull
    - [ ] Survey/Respond
- [ ] Stats (RTT, throughput, packet drops etc.)
- [ ] Durable transports (built-in retries and reconnections)
- [ ] Queuing
- [ ] Pluggable transport layer (TCP, UDP, QUIC etc.)
- [ ] Simulation modes with [Turmoil](https://github.com/tokio-rs/turmoil)

## Socket Types
### Request/Reply
Example:
```rust
use bytes::Bytes;
use tokio_stream::StreamExt;

use msg::{RepSocket, ReqSocket, Tcp};

#[tokio::main]
async fn main() {
    // Initialize the reply socket (server side) with a transport
    let mut rep = RepSocket::new(Tcp::new());
    rep.bind("0.0.0.0:4444").await.unwrap();

    // Initialize the request socket (client side) with a transport
    let mut req = ReqSocket::new(Tcp::new());
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
```