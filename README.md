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
- [ ] Durable transports (built-in retries and reconnections)
- [ ] Queuing
- [ ] Pluggable transport layer (TCP, UDP, QUIC etc.)

## Socket Types
### Request/Reply
Example:
```rust
use msg::*;
use bytes::Bytes;

#[tokio::main]
async fn main() {
    // Initialize the reply socket (server side)
    let rep = RepSocket::bind("0.0.0.0:4444").await.unwrap();

    // Initialize the request socket (client side)
    let req = ReqSocket::connect("0.0.0.0:4444").await.unwrap();

    tokio::spawn(async move {
        // Receive the request and respond with "world"
        let req = rep.recv().await.unwrap();
        println!("Message: {:?}", req.msg());

        req.respond(Bytes::from("world")).unwrap();
    });

    let res: Bytes = req.request(Bytes::from("hello")).await.unwrap();
    println!("Response: {:?}", res);
}