# Transport layers

MSG-RS supports multiple transport layers. The transport layer is the one that
handles the actual sending and receiving of messages. The following transport
layers are supported:

- [TCP](#tcp)
<!--
- [IPC](#ipc)
- [Inproc](#inproc)
- [UDP](#udp)
- [TLS](#tls)
- [QUIC](#quic)
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
use msg::{RepSocket, ReqSocket, Tcp};

#[tokio::main]
async fn main() {
    // Initialize the reply socket (server side) with TCP
    let mut rep = RepSocket::new(Tcp::new());
    rep.bind("0.0.0.0:4444").await.unwrap();

    // Initialize the request socket (client side) with TCP
    let mut req = ReqSocket::new(Tcp::new());
    req.connect("0.0.0.0:4444").await.unwrap();

    // ...
}
```

{{#include ../links.md}}
