use std::{
    collections::VecDeque,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::{Future, FutureExt, SinkExt, Stream, StreamExt};
use msg_wire::reqrep;
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpSocket, TcpStream},
    sync::{mpsc, oneshot},
    task::JoinSet,
};
use tokio_stream::StreamMap;
use tokio_util::codec::Framed;

const DEFAULT_BUFFER_SIZE: usize = 1024;

/// A reply socket. This socket can bind multiple times.
pub struct RepSocket {
    from_backend: mpsc::Receiver<Request>,
    local_addr: SocketAddr,
}

impl RepSocket {
    pub async fn recv(&mut self) -> Result<Request, RepError> {
        self.from_backend.recv().await.ok_or(RepError::SocketClosed)
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

#[derive(Debug, Error)]
pub enum RepError {
    #[error("IO error: {0:?}")]
    Io(#[from] std::io::Error),
    #[error("Wire protocol error: {0:?}")]
    Wire(#[from] msg_wire::reqrep::Error),
    #[error("Socket closed")]
    SocketClosed,
}

pub struct RepOptions {
    pub set_nodelay: bool,
    pub max_connections: Option<usize>,
}

impl Default for RepOptions {
    fn default() -> Self {
        Self {
            set_nodelay: true,
            max_connections: None,
        }
    }
}

impl RepSocket {
    pub async fn bind(addr: &str) -> Result<Self, RepError> {
        Self::bind_with_options(addr, RepOptions::default()).await
    }

    /// TODO: should we allow multiple binds?
    pub async fn bind_with_options(addr: &str, options: RepOptions) -> Result<Self, RepError> {
        let (to_socket, from_backend) = mpsc::channel(DEFAULT_BUFFER_SIZE);
        let socket = TcpSocket::new_v4()?;
        socket.set_nodelay(options.set_nodelay)?;
        socket.bind(addr.parse().unwrap())?;

        let listener = socket.listen(128)?;
        tracing::debug!("Listening on {}", addr);
        let local_addr = listener.local_addr()?;

        let backend = RepBackend {
            listener,
            peer_states: StreamMap::with_capacity(128),
            to_socket,
        };

        tokio::spawn(backend);

        Ok(Self {
            local_addr,
            from_backend,
        })
    }
}

struct PendingRequest {
    id: u32,
    response: oneshot::Receiver<Bytes>,
}

impl Future for PendingRequest {
    type Output = Option<(u32, Bytes)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        match this.response.poll_unpin(cx) {
            Poll::Ready(Ok(response)) => Poll::Ready(Some((this.id, response))),
            Poll::Ready(Err(_)) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct Request {
    source: SocketAddr,
    response: oneshot::Sender<Bytes>,
    msg: Bytes,
}

impl Request {
    pub fn source(&self) -> SocketAddr {
        self.source
    }

    pub fn msg(&self) -> &Bytes {
        &self.msg
    }

    pub fn respond(self, response: Bytes) -> Result<(), RepError> {
        self.response
            .send(response)
            .map_err(|_| RepError::SocketClosed)
    }
}

struct PeerState<T: AsyncRead + AsyncWrite> {
    pending_requests: JoinSet<Option<(u32, Bytes)>>,
    conn: Framed<T, reqrep::Codec>,
    addr: SocketAddr,
    egress_queue: VecDeque<reqrep::Message>,
}

struct RepBackend {
    listener: TcpListener,
    /// [`StreamMap`] of connected peers. The key is the peer's address.
    /// Note that when the [`PeerState`] stream ends, it will be silently removed
    /// from this map.
    peer_states: StreamMap<SocketAddr, PeerState<TcpStream>>,
    to_socket: mpsc::Sender<Request>,
}

impl Future for RepBackend {
    type Output = Result<(), RepError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            tracing::trace!("Polling peer states");
            if let Poll::Ready(Some((peer, msg))) = this.peer_states.poll_next_unpin(cx) {
                match msg {
                    Ok(request) => {
                        tracing::debug!("Received message from peer {}", peer);
                        let _ = this.to_socket.try_send(request);
                    }

                    Err(e) => {
                        tracing::error!("Error receiving message from peer {}: {:?}", peer, e);
                    }
                }

                continue;
            }

            match this.listener.poll_accept(cx) {
                Poll::Ready(Ok((stream, addr))) => {
                    this.peer_states.insert(
                        addr,
                        PeerState {
                            addr,
                            pending_requests: JoinSet::new(),
                            conn: Framed::new(stream, reqrep::Codec::new()),
                            egress_queue: VecDeque::new(),
                        },
                    );
                    tracing::debug!("New connection from {}, inserted into PeerStates", addr);

                    continue;
                }
                Poll::Ready(Err(e)) => {
                    // Errors here are usually about `WouldBlock`
                    tracing::error!("Error accepting connection: {:?}", e);

                    continue;
                }
                Poll::Pending => {}
            }

            return Poll::Pending;
        }
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> Stream for PeerState<T> {
    type Item = Result<Request, RepError>;

    /// Advances the state of the peer.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        tracing::trace!("Polling PeerState");
        let this = self.get_mut();

        loop {
            let _ = this.conn.poll_flush_unpin(cx);

            if this.conn.poll_ready_unpin(cx).is_ready() {
                if let Some(msg) = this.egress_queue.pop_front() {
                    match this.conn.start_send_unpin(msg) {
                        Ok(_) => {
                            // We might be able to send more queued messages
                            continue;
                        }
                        Err(e) => {
                            tracing::error!("Failed to send message to socket: {:?}", e);
                            // End this stream as we can't send any more messages
                            return Poll::Ready(None);
                        }
                    }
                }
            }

            // First, try to drain the egress queue.
            // First check for completed requests
            match this.pending_requests.poll_join_next(cx) {
                Poll::Ready(Some(Ok(Some((id, payload))))) => {
                    let msg = reqrep::Message::new(id, payload);
                    this.egress_queue.push_back(msg);

                    continue;
                }
                Poll::Ready(Some(Err(e))) => {
                    tracing::error!("Error receiving response: {:?}", e);
                    continue;
                }
                _ => {}
            }

            tracing::trace!("Polling connection");
            match this.conn.poll_next_unpin(cx) {
                Poll::Ready(Some(result)) => {
                    tracing::trace!("Received message from peer {}: {:?}", this.addr, result);
                    let msg = result?;
                    let (tx, rx) = oneshot::channel();

                    // Spawn the pending request future, which will resolve once the response is sent.
                    this.pending_requests.spawn(PendingRequest {
                        id: msg.id(),
                        response: rx,
                    });

                    let request = Request {
                        source: this.addr,
                        response: tx,
                        msg: msg.into_payload(),
                    };

                    return Poll::Ready(Some(Ok(request)));
                }
                Poll::Ready(None) => {
                    tracing::debug!("Connection closed");
                    return Poll::Ready(None);
                }
                Poll::Pending => {}
            }

            return Poll::Pending;
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use crate::req::ReqSocket;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_req_rep() {
        tracing_subscriber::fmt::init();
        let mut sock = RepSocket::bind("127.0.0.1:0").await.unwrap();

        let req = ReqSocket::connect(&sock.local_addr().to_string())
            .await
            .unwrap();

        tokio::spawn(async move {
            loop {
                let req = sock.recv().await.unwrap();

                req.respond(Bytes::from("hello")).unwrap();
            }
        });

        let n_reqs = 100000;
        let mut rng = rand::thread_rng();
        let msg_vec: Vec<Bytes> = (0..n_reqs)
            .map(|_| {
                let mut vec = vec![0u8; 512];
                rng.fill(&mut vec[..]);
                Bytes::from(vec)
            })
            .collect();

        let start = std::time::Instant::now();
        for msg in msg_vec {
            let _res = req.request(msg).await.unwrap();
            // println!("Response: {:?} {:?}", _res, req_start.elapsed());
        }
        let elapsed = start.elapsed();
        tracing::info!("{} reqs in {:?}", n_reqs, elapsed);
    }
}
