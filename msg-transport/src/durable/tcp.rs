use std::{io, pin::Pin};

use futures::{Future, FutureExt};
use tokio::net::{TcpStream, ToSocketAddrs};

use super::io::{DurableIo, UnderlyingIo};

impl<A> UnderlyingIo<A> for TcpStream
where
    A: ToSocketAddrs + Sync + Send + Clone + Unpin + 'static,
{
    fn establish(addr: A) -> Pin<Box<dyn Future<Output = io::Result<Self>> + Send>> {
        async {
            let stream = TcpStream::connect(addr).await?;
            stream.set_nodelay(true)?;
            Ok(stream)
        }
        .boxed()
    }
}

pub type DurableTcpStream<A> = DurableIo<TcpStream, A>;
