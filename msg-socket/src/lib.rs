use std::net::SocketAddr;
use tokio::io::{AsyncRead, AsyncWrite};

mod rep;
mod req;
mod sub;

use bytes::Bytes;
pub use rep::*;
pub use req::*;
pub use sub::*;

pub struct RequestId(u32);

impl RequestId {
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    pub fn id(&self) -> u32 {
        self.0
    }

    pub fn increment(&mut self) {
        self.0 = self.0.wrapping_add(1);
    }
}

pub trait Authenticator: Send + Sync + Unpin + 'static {
    fn authenticate(&self, id: &Bytes) -> bool;
}

pub(crate) struct AuthResult<S: AsyncRead + AsyncWrite> {
    id: Bytes,
    addr: SocketAddr,
    stream: S,
}
