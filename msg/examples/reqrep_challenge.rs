//! Request-Reply with challenge-response authentication (insecure demo).
//!
//! Protocol:
//! 1. Server sends a random 32-byte challenge
//! 2. Client computes a signature using a shared secret and sends it back
//! 3. Server verifies the signature and accepts or rejects

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_stream::StreamExt;

use msg::{RepSocket, ReqSocket, hooks::{ConnectionHook, Error, HookResult}, tcp::Tcp};

const SECRET: [u8; 32] = *b"this_is_a_32_byte_secret_key!!!!";

/// Simple XOR-based MAC
fn compute_mac(secret: &[u8; 32], challenge: &[u8; 32]) -> [u8; 32] {
    let mut mac = [0u8; 32];
    for i in 0..32 {
        mac[i] = secret[i] ^ challenge[i] ^ secret[(i + 1) % 32];
    }
    mac
}

struct ChallengeServerHook {
    secret: [u8; 32],
}

impl ChallengeServerHook {
    fn new(secret: [u8; 32]) -> Self {
        Self { secret }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("client authentication failed")]
struct ServerAuthError;

impl<Io> ConnectionHook<Io> for ChallengeServerHook
where
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Error = ServerAuthError;

    async fn on_connection(&self, mut io: Io) -> HookResult<Io, Self::Error> {
        let challenge: [u8; 32] = rand::random();
        io.write_all(&challenge).await?;

        let mut response = [0u8; 32];
        io.read_exact(&mut response).await?;

        let expected = compute_mac(&self.secret, &challenge);
        if response != expected {
            io.write_all(&[0]).await?;
            return Err(Error::hook(ServerAuthError));
        }

        io.write_all(&[1]).await?;
        Ok(io)
    }
}

struct ChallengeClientHook {
    secret: [u8; 32],
}

impl ChallengeClientHook {
    fn new(secret: [u8; 32]) -> Self {
        Self { secret }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("server rejected authentication")]
struct ClientAuthError;

impl<Io> ConnectionHook<Io> for ChallengeClientHook
where
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Error = ClientAuthError;

    async fn on_connection(&self, mut io: Io) -> HookResult<Io, Self::Error> {
        let mut challenge = [0u8; 32];
        io.read_exact(&mut challenge).await?;

        let response = compute_mac(&self.secret, &challenge);
        io.write_all(&response).await?;

        let mut verdict = [0u8; 1];
        io.read_exact(&mut verdict).await?;

        if verdict[0] != 1 {
            return Err(Error::hook(ClientAuthError));
        }

        Ok(io)
    }
}

#[tokio::main]
async fn main() {
    let mut rep =
        RepSocket::new(Tcp::default()).with_connection_hook(ChallengeServerHook::new(SECRET));
    rep.bind("0.0.0.0:4445").await.unwrap();

    let mut req =
        ReqSocket::new(Tcp::default()).with_connection_hook(ChallengeClientHook::new(SECRET));
    req.connect("0.0.0.0:4445").await.unwrap();

    tokio::spawn(async move {
        let req = rep.next().await.unwrap();
        println!("Server received: {:?}", req.msg());
        req.respond(Bytes::from("authenticated response")).unwrap();
    });

    let res: Bytes = req.request(Bytes::from("hello")).await.unwrap();
    println!("Client received: {res:?}");
}
