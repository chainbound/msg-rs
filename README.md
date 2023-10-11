# `msg-rs`

[![CI](https://github.com/chainbound/msg-rs/actions/workflows/ci.yml/badge.svg)][gh-ci]
[![License](https://img.shields.io/badge/License-MIT-orange.svg)][mit-license]

[gh-ci]: https://github.com/chainbound/msg-rs/actions/workflows/ci.yml
[mit-license]: https://github.com/chainbound/msg-rs/blob/main/LICENSE

**A flexible and lightweight messaging library for distributed systems built with Rust and Tokio.**

## Overview

`msg-rs` is a messaging library that was inspired by projects like [ZeroMQ](https://zeromq.org/) and [Nanomsg](https://nanomsg.org/).
It was built because we needed a Rust-native messaging library like those above.

> **Warning**
> This project is still in early development and is not ready for production use.

## Features

- [ ] Multiple socket types
  - [x] Request/Reply
  - [ ] Publish/Subscribe
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
## MSRV
The minimum supported Rust version is 1.70.

## Contributions & Bug Reports

Please report any bugs or issues you encounter by [opening a github issue](https://github.com/chainbound/msg-rs/issues/new).

Pull requests are welcome! If you would like to contribute, please open an issue first to discuss the change you would like to make.

## License

This project is licensed under the [MIT license](https://github.com/chainbound/msg-rs/blob/main/LICENSE).
