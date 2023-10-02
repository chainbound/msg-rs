use std::{
    collections::VecDeque,
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use futures::{Future, SinkExt, StreamExt};
use rustc_hash::FxHashMap;
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc, oneshot},
};
use tokio_util::codec::Framed;

use msg_wire::reqrep;

mod driver;
mod socket;
mod stats;
// pub(crate) use backend::*;
use driver::*;
pub use socket::*;

const DEFAULT_BUFFER_SIZE: usize = 1024;

#[derive(Debug, Error)]
pub enum ReqError {
    #[error("IO error: {0:?}")]
    Io(#[from] std::io::Error),
    #[error("Wire protocol error: {0:?}")]
    Wire(#[from] reqrep::Error),
    #[error("Socket closed")]
    SocketClosed,
    #[error("Transport error: {0:?}")]
    Transport(#[from] Box<dyn std::error::Error + Send + Sync>),
}

pub enum Command {
    Send {
        message: Bytes,
        response: oneshot::Sender<Result<Bytes, ReqError>>,
    },
}

pub struct ReqOptions {
    pub client_id: Option<Bytes>,
    pub timeout: std::time::Duration,
    pub retry_on_initial_failure: bool,
    pub backoff_duration: std::time::Duration,
    pub retry_attempts: Option<usize>,
    pub set_nodelay: bool,
}

impl Default for ReqOptions {
    fn default() -> Self {
        Self {
            client_id: None,
            timeout: std::time::Duration::from_secs(5),
            retry_on_initial_failure: true,
            backoff_duration: Duration::from_millis(200),
            retry_attempts: None,
            set_nodelay: true,
        }
    }
}