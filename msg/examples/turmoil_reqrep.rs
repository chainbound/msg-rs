use bytes::Bytes;
use tokio_stream::StreamExt;

use msg::{RepSocket, ReqSocket, Tcp};

fn main() -> turmoil::Result {
    // Build the simulation runtime
    let mut sim = turmoil::Builder::new().build();

    sim.host("server", || async move {
        // Initialize the reply socket (server side) with a transport
        println!("1");
        let mut rep = RepSocket::new(Tcp::new());
        println!("2");
        rep.bind("0.0.0.0:4444").await?;
        println!("3");

        tokio::spawn(async move {
            println!("6");
            // Receive the request and respond with "world"
            // RepSocket implements `Stream`
            let req = rep.next().await.unwrap();
            println!("Message: {:?}", req.msg());

            req.respond(Bytes::from("world")).unwrap();
            println!("7");
        });

        Ok(())
    });

    sim.client("client", async move {
        // Initialize the request socket (client side) with a transport
        let mut req = ReqSocket::new(Tcp::new());
        println!("4");
        req.connect("0.0.0.0:4444").await?;
        println!("5");

        let res: Bytes = req.request(Bytes::from("hello")).await?;
        println!("Response: {:?}", res);
        Ok(())
    });

    sim.run()
}
