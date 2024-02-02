# Message compression

Sometimes, you may want to compress messages before sending them over the network.
MSG-RS supports message compression out of the box, and it is very easy to use.

Compression is most useful in scenarios where you are sending large messages over the network.
It can also help reduce the amount of bandwidth used by your application.

In MSG, compression is handled by the socket type:

- [Request/Response](#requestresponse)
- [Publish/Subscribe](#publishsubscribe)

---

## Request/Response

You can also find a complete example in [msg/examples/reqrep_compression.rs][examples].

```rust
use msg::{compression::GzipCompressor, ReqSocket, RepSocket, Tcp};

#[tokio::main]
async fn main() {
    // Initialize the reply socket (server side) with a transport
    let mut rep = RepSocket::new(Tcp::default())
            // Enable Gzip compression (compression level 6).
            .with_compressor(GzipCompressor::new(6));

    rep.bind("0.0.0.0:4444").await.unwrap();

    // Initialize the request socket (client side) with a transport
    let mut req = ReqSocket::new(Tcp::default())
            // Enable Gzip compression (compression level 6).
            // The request and response sockets *don't have to*
            // use the same compression algorithm or level.
            .with_compressor(GzipCompressor::new(6));

    req.connect("0.0.0.0:4444").await.unwrap();

    // ...
}
```

## Publish/Subscribe

You can also find a complete example in [msg/examples/pubsub_compression.rs][examples].

```rust
use msg::{compression::GzipCompressor, PubSocket, SubSocket, Tcp};

#[tokio::main]
async fn main() {
    // Configure the publisher socket with options
    let mut pub_socket = PubSocket::new(Tcp::new())
        // Enable Gzip compression (compression level 6)
        .with_compressor(GzipCompressor::new(6));

    // Configure the subscribers with options
    let mut sub_socket = SubSocket::new(Tcp::default());

    // ...
}
```

By looking at this example, you might be wondering: "how does the subscriber know that the
publisher is compressing messages, if the subscriber is not configured with Gzip compression?"

The answer is that in MSG, compression is defined by the publisher for each message that is sent.
In practice, each message contains info in its `Header` that tells the subscriber whether the message
payload is compressed - and if so, which compression algorithm was used. The subscriber then uses this
info to decompress the message payload before making it available to the user.

All of this is done automatically by MSG and it works out of the box
with the default compression methods:

- Gzip
- Zstd
- Snappy
- LZ4

If you wish to use a custom compression algorithm, this is not exposed with a public API yet.
If you need this, please [open an issue][new-issue] on Github and we will prioritize it!

{{#include ../links.md}}
