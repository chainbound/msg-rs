# Authentication

Authentication is the process of verifying the identity of a user or process
before allowing access to resources.

In MSG, authentication is handled by users of the library through the
`Authenticator` trait. This trait is implemented by the user and passed to the
socket type when it is created.

Here is how the `Authenticator` trait is defined:

```rust
// msg-socket/src/lib.rs
pub trait Authenticator: Send + Sync + Unpin + 'static {
    fn authenticate(&self, id: &Bytes) -> bool;
}
```

The `authenticate` method is called by the library whenever a new connection
is established. The `id` parameter is the identity of the connecting peer.

**Note: the Authenticator is used by the server-side socket only!**

Here is an example of how you can add an authenticator to a
client-server application:

```rust
// Define some custom authentication logic
#[derive(Default)]
struct Auth;

impl Authenticator for Auth {
    fn authenticate(&self, id: &Bytes) -> bool {
        println!("Auth request from: {:?}", id);
        // Custom authentication logic goes here
        // ...
        true
    }
}

#[tokio::main]
async fn main() {
    // Initialize the reply socket (server side) with a transport
    // and an authenticator that we just implemented:
    let mut rep = RepSocket::new(Tcp::new()).with_auth(Auth);
    rep.bind("0.0.0.0:4444").await.unwrap();

    // Initialize the request socket (client side) with a transport
    // and an identifier. This will implicitly turn on client authentication.
    // The identifier will be sent to the server when the connection is established.
    let mut req = ReqSocket::with_options(
        Tcp::new(),
        ReqOptions::default().auth_token(Bytes::from("client1")),
    );
}
```

{{#include ../links.md}}
