# `msg-rs`

[![CI](https://github.com/chainbound/msg-rs/actions/workflows/ci.yml/badge.svg)][gh-ci]
[![License](https://img.shields.io/badge/License-MIT-orange.svg)][mit-license]

**A flexible and lightweight messaging library for distributed systems built with Rust and Tokio.**

## Overview

`msg-rs` is a messaging library that was inspired by projects like [ZeroMQ](https://zeromq.org/) and [Nanomsg](https://nanomsg.org/).
It was built because we needed a Rust-native messaging library like those above.

> MSG is still in ALPHA and is not ready for production use.

## Features

- [ ] Multiple socket types
  - [x] Request/Reply
  - [x] Publish/Subscribe
  - [ ] Channel
  - [ ] Push/Pull
  - [ ] Survey/Respond
- [ ] Stats (RTT, throughput, packet drops etc.)
- [x] Request/Reply basic stats
- [ ] Queuing
- [ ] Pluggable transport layer
  - [x] TCP
  - [ ] TLS
  - [ ] IPC
  - [ ] UDP
  - [ ] Inproc
- [x] Durable IO abstraction (built-in retries and reconnections)
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

### Publish/Subscribe

```rust
use bytes::Bytes;
use tokio_stream::StreamExt;

use msg::{PubSocket, SubSocket, Tcp};

#[tokio::main]
async fn main() {
    // Initialize the publisher socket (server side) with a transport
    let mut pub_socket = PubSocket::new(Tcp::new());
    pub_socket.bind("0.0.0.0:4444").await.unwrap();

    // Initialize the subscriber socket (client side) with a transport
    let mut sub_socket = SubSocket::new(Tcp::new());
    sub_socket.connect("0.0.0.0:4444").await.unwrap();

    let topic = "some_interesting_topic".to_string();

    // Subscribe to a topic
    sub_socket.subscribe(topic.clone()).await.unwrap();

    tokio::spawn(async move {
        // Values are `bytes::Bytes`
        pub_socket.publish(topic, Bytes::from("hello_world")).await.unwrap();
    });

    let msg = sub_socket.next().await.unwrap();
    println!("Received message: {:?}", msg);
}
```

## MSRV

The minimum supported Rust version is 1.70.

## Contributions & Bug Reports

If you are interested in contributing or have found a bug, please check out the [contributing guide][contributing].
Please report any bugs or doubts you encounter by [opening a Github issue][new-issue].

Additionally, you can reach out to us on [Discord][discord] if you have any questions or just want to chat.

## License

This project is licensed under the [MIT license][mit-license].

<!-- Links -->

[gh-ci]: https://github.com/chainbound/msg-rs/actions/workflows/ci.yml
[discord]: https://discord.gg/nhWcSWYpm9
[new-issue]: https://github.com/chainbound/msg-rs/issues/new
[mit-license]: https://github.com/chainbound/msg-rs/blob/main/LICENSE
[contributing]: https://github.com/chainbound/msg-rs/blob/main/CONTRIBUTING.md
