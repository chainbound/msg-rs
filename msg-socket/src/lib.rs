#![doc(issue_tracker_base_url = "https://github.com/chainbound/msg-rs/issues/")]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

use msg_transport::Address;
use tokio::io::{AsyncRead, AsyncWrite};

#[path = "pub/mod.rs"]
mod pubs;
mod rep;
mod req;
mod sub;

mod connection;
pub use connection::*;

use bytes::Bytes;
pub use pubs::{PubError, PubOptions, PubSocket};
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

pub(crate) struct AuthResult<S: AsyncRead + AsyncWrite, A: Address> {
    id: Bytes,
    addr: A,
    stream: S,
}
