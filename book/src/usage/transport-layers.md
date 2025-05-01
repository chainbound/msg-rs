# Transport layers

MSG-RS supports multiple transport layers. The transport layer is the one that
handles the actual sending and receiving of messages. The following transport
layers are supported:

- [TCP](#tcp)
- [QUIC](#quic)
- [IPC](#ipc)

<!--
- [Inproc](#inproc)
- [UDP](#udp)
- [TLS](#tls)
  -->

## TCP

### Why choose TCP?

The TCP transport layer is ideal for scenarios where reliable, ordered, and error-checked
delivery of a stream of data is crucial. It ensures that data is delivered in the order it
was sent and retransmits lost packets. This makes TCP suitable for applications where data
integrity and accuracy are more important than speed.

TCP is especially useful if messages are going to be sent over public internet links, where
the quality of the connection cannot be guaranteed and a significant portion of packets may
be lost or corrupted.

### How to use TCP

In MSG, here is how you can setup any socket type with the TCP transport:

```rust
use msg::{RepSocket, ReqSocket, tcp::Tcp};

#[tokio::main]
async fn main() {
    // Initialize the reply socket (server side) with default TCP
    let mut rep = RepSocket::new(Tcp::default());
    // Bind the socket to the address. This will start listening for incoming connections.
    // This method does DNS resolution internally, so you can use hostnames here.
    rep.bind("0.0.0.0:4444").await.unwrap();

    // Initialize the request socket (client side) with default TCP
    let mut req = ReqSocket::new(Tcp::default());
    // Connect the socket to the address. This will initiate a connection to the server.
    // This method does DNS resolution internally, so you can use hostnames here.
    req.connect("0.0.0.0:4444").await.unwrap();

    // ...
}
```

## QUIC

### Why choose QUIC?

QUIC is a new transport layer protocol that is built on top of UDP. It is designed to provide the same
reliability & security guarantees as TCP + TLS, while solving some of the issues that it has, like

- **Head-of-line blocking**: If a packet is lost, all subsequent packets are held up until the lost packet
  is retransmitted. This can be a problem especially when multiplexing multiple streams over a single
  connection because it can cause a single slow stream to block all other streams.
- **Slow connection setup**: TCP + TLS requires 2-3 round trips to establish a connection, which can be
  slow on high latency networks.
- **No support for multiplexing**: TCP does not support multiplexing multiple streams over a single connection.
  This means that if you want to send multiple streams of data over a single connection, you have to
  implement your own multiplexing layer on top of TCP, which can run into issues like head-of-line
  blocking that we've seen above.

### QUIC in MSG

The MSG QUIC implementation is based on [quinn](https://github.com/quinn-rs/quinn). It relies on self-signed
certificates and does not verify server certificates. Also, due to how our `Transport` abstraction works, we
don't support QUIC connections with multiple streams. This means that the `Quic` transport implementation will
do all its work over a single, bi-directional stream for now.

### How to use QUIC

In MSG, here is how you can setup any socket type with the QUIC transport:

```rust
use msg::{RepSocket, ReqSocket, Quic};

#[tokio::main]
async fn main() {
    // Initialize the reply socket (server side) with default QUIC
    let mut rep = RepSocket::new(Quic::default());
    // Bind the socket to the address. This will start listening for incoming connections.
    // This method does DNS resolution internally, so you can use hostnames here.
    rep.bind("0.0.0.0:4444").await.unwrap();

    // Initialize the request socket (client side) with default QUIC
    let mut req = ReqSocket::new(Quic::default());
    // Connect the socket to the address. This will initiate a connection to the server.
    // This method does DNS resolution internally, so you can use hostnames here.
    req.connect("0.0.0.0:4444").await.unwrap();

    // ...
}
```

## IPC

More precisely, MSG-RS supports [Unix Domain Sockets (UDS)][uds] for IPC.

### Why choose IPC?

IPC is a transport layer that allows for communication between processes on the same machine.
The main difference between IPC and other transport layers is that IPC sockets use the filesystem
as the address namespace.

IPC is useful when you want to avoid the overhead of network sockets and want to have a low-latency
communication link between processes on the same machine, all while being able to use the same API
as the other transport layers that MSG-RS supports.

Due to its simplicity, IPC is typically faster than TCP and QUIC, but the exact performance improvements
also depend on the throughput of the underlying UDS implementation. We only recommend using IPC when you
know that the performance benefits outweigh the overhead of using a network socket.

### How to use IPC

In MSG, here is how you can setup any socket type with the IPC transport:

```rust
use msg::{RepSocket, ReqSocket, Ipc};

#[tokio::main]
async fn main() {
    // Initialize the reply socket (server side) with default IPC
    let mut rep = RepSocket::new(Ipc::default());
    // Bind the socket to the address. This will start listening for incoming connections.
    // You can use any path that is valid for a Unix Domain Socket.
    rep.bind("/tmp/msg.sock").await.unwrap();

    // Initialize the request socket (client side) with default IPC
    let mut req = ReqSocket::new(Ipc::default());
    // Connect the socket to the address. This will initiate a connection to the server.
    req.connect("/tmp/msg.sock").await.unwrap();

    // ...
}
```

[uds]: https://en.wikipedia.org/wiki/Unix_domain_socket

{{#include ../links.md}}
