# Socket types

MSG-RS supports the following socket types:

- [Request/Reply](#requestreply)
- [Publish/Subscribe](#publishsubscribe)
<!--
- [Push/Pull](#pushpull)
- [Channel](#channel)
- [Survey/Respond](#surveyrespond)
  -->

## Request/Reply

The request/reply socket type is used for sending a request to a server and receiving a response.

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
        // (RepSocket implements `Stream`)
        let req = rep.next().await.unwrap();
        println!("Message: {:?}", req.msg());

        req.respond(Bytes::from("world")).unwrap();
    });

    let res: Bytes = req.request(Bytes::from("hello")).await.unwrap();
    println!("Response: {:?}", res);
}
```

## Publish/Subscribe

The publish/subscribe socket type is used for sending a message to multiple subscribers.
It works by defining topics over which messages can be sent and received. Subscribers can
subscribe to one or more topics, and will receive all messages sent to those topics by
publishers.

Example:

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

{{#include ../links.md}}
