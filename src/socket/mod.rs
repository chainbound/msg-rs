use bytes::Bytes;
use futures::{stream, SinkExt, StreamExt};
use std::{error::Error, net::SocketAddr};
use tokio::{
    io::{self, AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tokio_util::codec::{BytesCodec, Framed};

mod req;

#[async_trait::async_trait]
pub trait RequestSocket {
    type Error: Error;

    async fn connect(&mut self, target: &str) -> Result<(), Self::Error>;
    async fn request(&mut self, message: Bytes) -> Result<Bytes, Self::Error>;

    #[cfg(feature = "bare")]
    async fn send(&mut self, message: Bytes) -> Result<(), Self::Error>;
}

pub trait Transport: AsyncRead + AsyncWrite + Unpin {}

impl Transport for TcpStream {}

// pub struct ReqSocket<T>
// where
//     T: Transport,
// {
//     framed: Framed<T, BytesCodec>,
// }

// impl<T> ReqSocket<T>
// where
//     T: Transport,
// {
//     pub fn new(transport: T) -> Self {
//         Self {
//             framed: Framed::new(transport, BytesCodec::new()),
//         }
//     }
// }

// #[async_trait::async_trait]
// impl<T> RequestSocket for ReqSocket<T>
// where
//     T: Transport + Unpin + Send + Sync,
// {
//     type Error = T::Error;

//     async fn connect(&mut self, target: &str) -> Result<(), Self::Error> {
//         self.framed.connect(target).await.unwrap();
//         Ok(())
//     }

//     async fn request(&mut self, message: Bytes) -> Result<Bytes, Self::Error> {
//         self.framed.send(message).await.unwrap();
//         // self.framed.read_to_string(dst)
//         Ok(Bytes::default())
//     }
// }

pub trait ClientSocket {}

pub trait ServerSocket {
    fn new(listener: TcpListener) -> Self;
}

/// Request socket pattern
pub struct Req {
    framed: Option<Framed<Box<dyn Transport>, BytesCodec>>,
}

pub struct Rep {
    listener: Option<TcpListener>,
    rx: Option<mpsc::Receiver<(SocketAddr, Bytes)>>,
}

impl ClientSocket for Req {}

pub struct Socket<S> {
    framed: Option<Framed<Box<dyn Transport>, BytesCodec>>,
    pattern: Option<S>,
}

impl Socket<Req> {
    pub fn new() -> Req {
        Req { framed: None }
    }
}

impl Req {
    pub async fn connect(&mut self, target: &str) -> Result<(), io::Error> {
        let tcp = TcpStream::connect(target).await?;
        tcp.set_nodelay(true)?;
        self.framed = Some(Framed::new(Box::new(tcp), BytesCodec::new()));
        Ok(())
    }

    pub async fn send(&mut self, message: Bytes) -> Result<(), io::Error> {
        if let Some(framed) = &mut self.framed {
            framed.send(message).await?;
        }

        Ok(())
    }

    pub async fn recv(&mut self) -> Result<Bytes, io::Error> {
        let framed = self.framed.as_mut().expect("Socket not connected");
        let bytes = framed.next().await.unwrap().unwrap();
        Ok(bytes.freeze())
    }
}

impl Socket<Rep> {
    pub fn new() -> Rep {
        Rep {
            listener: None,
            rx: None,
        }
    }
}

impl Rep {
    pub async fn bind(&mut self, addr: SocketAddr) -> Result<(), io::Error> {
        let listener = TcpListener::bind(addr).await?;
        let (tx, rx) = mpsc::channel(1024);

        tokio::spawn(async move {
            loop {
                let (socket, peer_addr) = listener.accept().await.unwrap();
                let mut framed = Framed::new(Box::new(socket), BytesCodec::new());

                let tx = tx.clone();
                tokio::spawn(async move {
                    while let Some(bytes) = framed.next().await {
                        let bytes = bytes.unwrap();
                        tx.send((peer_addr, bytes.freeze())).await.unwrap();
                    }
                });
            }
        });

        self.rx = Some(rx);
        Ok(())
    }

    pub async fn recv(&mut self) -> Result<(SocketAddr, Bytes), io::Error> {
        let rx = self.rx.as_mut().expect("Socket not bound");

        let (peer_addr, bytes) = rx.recv().await.unwrap();
        Ok((peer_addr, bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn req_rep() {
        let mut server_socket = Socket::<Rep>::new();
        server_socket
            .bind("127.0.0.1:4444".parse().unwrap())
            .await
            .unwrap();

        let mut req_socket = Socket::<Req>::new();
        req_socket.connect("127.0.0.1:4444").await.unwrap();
        req_socket.send(Bytes::from("Hello")).await.unwrap();
        let response = req_socket.recv().await.unwrap();
        println!("Response: {:?}", response);
    }
}
