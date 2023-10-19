use std::{
    io::{self, IoSlice},
    net::SocketAddr,
    pin::Pin,
    task::{ready, Context, Poll},
    time::Duration,
};

use futures::Future;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
};
use tracing::{debug, error};

pub type PendingIo<Io> = Pin<Box<dyn Future<Output = io::Result<Io>> + Send + Sync>>;

/// A layer can be applied to pre-process a newly established IO object. If you need
/// multiple layers, use a single top-level layer that contains and calls the other layers.
pub trait Layer<Io: AsyncRead + AsyncWrite>: Send + Sync + 'static {
    /// The processing method. This method is called with the IO object that
    /// should be processed, and returns a future that resolves to a processing error
    /// or the processed IO object.
    fn process(&mut self, io: Io) -> PendingIo<Io>;
}

struct ReconnectStatus<Io> {
    /// The number of reconnect attempts that have been made.
    attempts: u32,
    current_attempt: Option<PendingIo<Io>>,
}

impl<Io> ReconnectStatus<Io> {
    /// Polls the current reconnect attempt. If the attempt is successful, this will return Poll::Ready with
    /// the established IO object, and set the current attempt to `None`.
    /// If the attempt fails, this will return Poll::Ready with the error. The caller should make sure
    /// a new reconnect attempt is scheduled before polling again.
    /// Poll::Pending is returned if the current attempt is still in progress.
    #[inline]
    fn poll_reconnect(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<Io>> {
        if let Some(attempt) = &mut self.current_attempt {
            match attempt.as_mut().poll(cx) {
                Poll::Ready(Ok(io)) => {
                    self.current_attempt = None;
                    Poll::Ready(Ok(io))
                }
                Poll::Ready(Err(e)) => {
                    self.current_attempt = None;
                    Poll::Ready(Err(e))
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            unreachable!()
        }
    }
}

/// Trait that should be implemented for an [AsyncRead] and/or [AsyncWrite]
/// item to enable it to work with the [DurableIo] struct.
pub trait UnderlyingIo: Sized + Unpin {
    /// The creation function is used by StubbornIo in order to establish both the initial IO connection
    /// in addition to performing reconnects.
    fn establish(endpoint: SocketAddr) -> PendingIo<Self>;

    /// When IO items experience an [io::Error](io::Error) during operation, it does not necessarily mean
    /// it is a disconnect/termination (ex: WouldBlock). This trait provides sensible defaults to classify
    /// which errors are considered "disconnects", but this can be overridden based on the user's needs.
    #[inline]
    fn is_disconnect_error(&self, err: &io::Error) -> bool {
        use std::io::ErrorKind::*;

        matches!(
            err.kind(),
            NotFound
                | PermissionDenied
                | ConnectionRefused
                | ConnectionReset
                | ConnectionAborted
                | NotConnected
                | AddrInUse
                | AddrNotAvailable
                | BrokenPipe
                | AlreadyExists
        )
    }

    /// If the underlying IO item implements AsyncRead, this method allows the user to specify
    /// if a technically successful read actually means that the connect is closed.
    /// For example, tokio's TcpStream successfully performs a read of 0 bytes when closed.
    #[inline]
    fn is_final_read(&self, bytes_read: usize) -> bool {
        // definitely true for tcp, perhaps true for other io as well,
        // indicative of EOF hit
        bytes_read == 0
    }
}

impl UnderlyingIo for TcpStream {
    fn establish(addr: SocketAddr) -> PendingIo<TcpStream> {
        Box::pin(async move {
            let stream = TcpStream::connect(addr).await?;
            stream.set_nodelay(true)?;
            Ok(stream)
        })
    }
}

impl<Io> DurableSession<Io>
where
    Io: UnderlyingIo + AsyncRead + AsyncWrite + 'static,
{
    pub fn new(endpoint: SocketAddr) -> Self {
        Self {
            state: SessionState::Disconnected(ReconnectStatus {
                attempts: 0,
                current_attempt: None,
            }),
            endpoint,
            layer_stack: None,
        }
    }

    /// Adds a layer to the session. The layer will be applied to all established or re-established
    /// sessions.
    pub fn with_layer(mut self, layer: impl Layer<Io> + Send) -> Self {
        self.layer_stack = Some(Box::new(layer));
        self
    }

    /// Asynchronously connects to the remote endpoint.
    pub async fn connect(&mut self) {
        let attempt = Io::establish(self.endpoint);

        self.state = SessionState::Disconnected(ReconnectStatus {
            attempts: 0,
            current_attempt: Some(attempt),
        });
    }

    /// Tries to establish a session with the remote endpoint immediately. Returns an error if
    /// the connection could not be established.
    pub async fn blocking_connect(&mut self) -> Result<(), io::Error> {
        let io = Io::establish(self.endpoint).await?;
        if let Some(layer) = &mut self.layer_stack {
            let io = layer.process(io).await?;
            self.state = SessionState::Connected(io);
        } else {
            self.state = SessionState::Connected(io);
        }

        Ok(())
    }

    fn on_disconnect(mut self: Pin<&mut Self>, cx: &mut Context<'_>) {
        // We copy here because we can't do it after borrowing self in the match below
        let endpoint = self.endpoint;

        match &mut self.state {
            SessionState::Connected(_) => {
                error!("Session was disconnected from {}", self.endpoint);
                self.state = SessionState::Disconnected(ReconnectStatus {
                    attempts: 0,
                    current_attempt: Some(Io::establish(self.endpoint)),
                });
            }
            SessionState::Disconnected(reconnect_status) => {
                debug!(
                    attempts = reconnect_status.attempts,
                    "Reconnect failed, retrying..."
                );

                reconnect_status.attempts += 1;

                // Start and set the new reconnect attempt
                let attempt = Box::pin(async move {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    Io::establish(endpoint).await
                });

                reconnect_status.current_attempt = Some(attempt);
            }
            SessionState::Processing(_) => {
                error!("Session was disconnected from {}", self.endpoint);
                self.state = SessionState::Disconnected(ReconnectStatus {
                    attempts: 0,
                    current_attempt: Some(Io::establish(self.endpoint)),
                });
            }
            SessionState::Terminated(_) => {
                unreachable!("Session was already terminated")
            }
        }

        // Register the waker to make sure the reconnect attempt is polled
        cx.waker().wake_by_ref();
    }
}

enum SessionState<Io> {
    /// The session is connected and ready to read and write.
    Connected(Io),
    /// The session is disconnected and waiting to be reconnected.
    Disconnected(ReconnectStatus<Io>),
    /// The session is currently processing a layer.
    Processing(PendingIo<Io>),
    /// The session is terminated and will not be tried again.
    Terminated(io::Error),
}

/// DurableSession is the AsyncRead + AsyncWrite implementation to be used in application Framed stacks.
pub struct DurableSession<Io: UnderlyingIo> {
    /// The session state.
    state: SessionState<Io>,
    /// The address of the remote endpoint.
    endpoint: SocketAddr,
    /// Optional layer stack. If this is `None`, newly connected (or reconnected) sessions will
    /// be passed through without processing.
    layer_stack: Option<Box<dyn Layer<Io> + Send>>,
}

impl<Io> AsyncRead for DurableSession<Io>
where
    Io: UnderlyingIo + AsyncRead + AsyncWrite + 'static,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut self.state {
            SessionState::Disconnected(reconnect_status) => {
                match ready!(reconnect_status.poll_reconnect(cx)) {
                    Ok(io) => {
                        if let Some(layer) = &mut self.layer_stack {
                            self.state = SessionState::Processing(layer.process(io));
                            // Register the waker to make sure the layer is polled
                            cx.waker().wake_by_ref();
                            Poll::Pending
                        } else {
                            self.state = SessionState::Connected(io);
                            self.poll_read(cx, buf)
                        }
                    }
                    Err(e) => {
                        debug!("Failed to reconnect to {}: {}", self.endpoint, e);
                        self.on_disconnect(cx);
                        Poll::Pending
                    }
                }
            }
            SessionState::Processing(fut) => match ready!(fut.as_mut().poll(cx)) {
                Ok(io) => {
                    debug!("Finished processing layer");
                    self.state = SessionState::Connected(io);
                    self.poll_read(cx, buf)
                }
                Err(e) => {
                    error!("Processing layer failed: {:?}", e);
                    use std::io::ErrorKind::*;

                    // If this is any of the non-fatal errors, we try to reconnect
                    if matches!(
                        e.kind(),
                        NotFound
                            | PermissionDenied
                            | ConnectionRefused
                            | ConnectionReset
                            | ConnectionAborted
                            | NotConnected
                            | AddrInUse
                            | AddrNotAvailable
                            | BrokenPipe
                            | AlreadyExists
                    ) {
                        self.on_disconnect(cx);
                        Poll::Pending
                    } else {
                        self.state = SessionState::Terminated(e);
                        Poll::Pending
                    }
                }
            },
            SessionState::Connected(io) => {
                let pre_len = buf.filled().len();
                let poll = AsyncRead::poll_read(Pin::new(io), cx, buf);
                let post_len = buf.filled().len();
                let bytes_read = post_len - pre_len;

                let disconnected = match poll {
                    Poll::Ready(Ok(())) => io.is_final_read(bytes_read),
                    Poll::Ready(Err(ref e)) => io.is_disconnect_error(e),
                    Poll::Pending => false,
                };

                if disconnected {
                    self.on_disconnect(cx);
                    Poll::Pending
                } else {
                    poll
                }
            }
            SessionState::Terminated(e) => Poll::Ready(Err(e.kind().into())),
        }
    }
}

impl<Io> AsyncWrite for DurableSession<Io>
where
    Io: UnderlyingIo + AsyncRead + AsyncWrite + 'static,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut self.state {
            SessionState::Disconnected(reconnect_status) => {
                debug!("Disconnected, trying to reconnect");
                match ready!(reconnect_status.poll_reconnect(cx)) {
                    Ok(io) => {
                        if let Some(layer) = &mut self.layer_stack {
                            self.state = SessionState::Processing(layer.process(io));
                            // Register the waker to make sure the layer is polled
                            cx.waker().wake_by_ref();
                            Poll::Pending
                        } else {
                            debug!("Reconnected again");
                            self.state = SessionState::Connected(io);
                            self.poll_write(cx, buf)
                        }
                    }
                    Err(e) => {
                        debug!("Failed to reconnect to {}: {}", self.endpoint, e);
                        self.on_disconnect(cx);
                        Poll::Pending
                    }
                }
            }
            SessionState::Processing(fut) => match ready!(fut.as_mut().poll(cx)) {
                Ok(io) => {
                    debug!("Finished processing layer");
                    self.state = SessionState::Connected(io);
                    self.poll_write(cx, buf)
                }
                Err(e) => {
                    error!("Processing layer failed: {:?}", e);
                    use std::io::ErrorKind::*;

                    // If this is any of the non-fatal errors, we try to reconnect
                    if matches!(
                        e.kind(),
                        NotFound
                            | PermissionDenied
                            | ConnectionRefused
                            | ConnectionReset
                            | ConnectionAborted
                            | NotConnected
                            | AddrInUse
                            | AddrNotAvailable
                            | BrokenPipe
                            | AlreadyExists
                    ) {
                        self.on_disconnect(cx);
                        Poll::Pending
                    } else {
                        self.state = SessionState::Terminated(e);
                        Poll::Pending
                    }
                }
            },
            SessionState::Connected(io) => {
                let poll = AsyncWrite::poll_write(Pin::new(io), cx, buf);

                let disconnected = match poll {
                    Poll::Ready(Err(ref e)) => io.is_disconnect_error(e),
                    _ => false,
                };

                if disconnected {
                    self.on_disconnect(cx);
                    Poll::Pending
                } else {
                    poll
                }
            }
            SessionState::Terminated(e) => Poll::Ready(Err(e.kind().into())),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.state {
            SessionState::Disconnected(reconnect_status) => {
                match ready!(reconnect_status.poll_reconnect(cx)) {
                    Ok(io) => {
                        if let Some(layer) = &mut self.layer_stack {
                            self.state = SessionState::Processing(layer.process(io));
                            // Register the waker to make sure the layer is polled
                            cx.waker().wake_by_ref();
                            Poll::Pending
                        } else {
                            self.state = SessionState::Connected(io);
                            self.poll_flush(cx)
                        }
                    }
                    Err(e) => {
                        debug!("Failed to reconnect to {}: {}", self.endpoint, e);
                        self.on_disconnect(cx);
                        Poll::Pending
                    }
                }
            }
            SessionState::Processing(fut) => match ready!(fut.as_mut().poll(cx)) {
                Ok(io) => {
                    debug!("Finished processing layer");
                    self.state = SessionState::Connected(io);
                    self.poll_flush(cx)
                }
                Err(e) => {
                    error!("Processing layer failed: {:?}", e);
                    use std::io::ErrorKind::*;

                    // If this is any of the non-fatal errors, we try to reconnect
                    if matches!(
                        e.kind(),
                        NotFound
                            | PermissionDenied
                            | ConnectionRefused
                            | ConnectionReset
                            | ConnectionAborted
                            | NotConnected
                            | AddrInUse
                            | AddrNotAvailable
                            | BrokenPipe
                            | AlreadyExists
                    ) {
                        self.on_disconnect(cx);
                        Poll::Pending
                    } else {
                        self.state = SessionState::Terminated(e);
                        Poll::Pending
                    }
                }
            },
            SessionState::Connected(io) => {
                let poll = AsyncWrite::poll_flush(Pin::new(io), cx);

                let disconnected = match poll {
                    Poll::Ready(Err(ref e)) => io.is_disconnect_error(e),
                    _ => false,
                };

                if disconnected {
                    self.on_disconnect(cx);
                    Poll::Pending
                } else {
                    poll
                }
            }
            SessionState::Terminated(e) => Poll::Ready(Err(e.kind().into())),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.state {
            SessionState::Disconnected(reconnect_status) => {
                reconnect_status.current_attempt.take();
                Poll::Ready(Ok(()))
            }
            SessionState::Processing(_) => Poll::Ready(Ok(())),
            SessionState::Connected(io) => AsyncWrite::poll_shutdown(Pin::new(io), cx),
            SessionState::Terminated(e) => Poll::Ready(Err(e.kind().into())),
        }
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match &mut self.state {
            SessionState::Disconnected(reconnect_status) => {
                match ready!(reconnect_status.poll_reconnect(cx)) {
                    Ok(io) => {
                        if let Some(layer) = &mut self.layer_stack {
                            self.state = SessionState::Processing(layer.process(io));
                            // Register the waker to make sure the layer is polled
                            cx.waker().wake_by_ref();
                            Poll::Pending
                        } else {
                            self.state = SessionState::Connected(io);
                            self.poll_write_vectored(cx, bufs)
                        }
                    }
                    Err(e) => {
                        debug!("Failed to reconnect to {}: {}", self.endpoint, e);
                        self.on_disconnect(cx);
                        Poll::Pending
                    }
                }
            }
            SessionState::Processing(fut) => match ready!(fut.as_mut().poll(cx)) {
                Ok(io) => {
                    debug!("Finished processing layer");
                    self.state = SessionState::Connected(io);
                    self.poll_write_vectored(cx, bufs)
                }
                Err(e) => {
                    error!("Processing layer failed: {:?}", e);
                    use std::io::ErrorKind::*;

                    // If this is any of the non-fatal errors, we try to reconnect
                    if matches!(
                        e.kind(),
                        NotFound
                            | PermissionDenied
                            | ConnectionRefused
                            | ConnectionReset
                            | ConnectionAborted
                            | NotConnected
                            | AddrInUse
                            | AddrNotAvailable
                            | BrokenPipe
                            | AlreadyExists
                    ) {
                        self.on_disconnect(cx);
                        Poll::Pending
                    } else {
                        self.state = SessionState::Terminated(e);
                        Poll::Pending
                    }
                }
            },
            SessionState::Connected(io) => {
                let poll = AsyncWrite::poll_write_vectored(Pin::new(io), cx, bufs);

                let disconnected = match poll {
                    Poll::Ready(Err(ref e)) => io.is_disconnect_error(e),
                    _ => false,
                };

                if disconnected {
                    self.on_disconnect(cx);
                    Poll::Pending
                } else {
                    poll
                }
            }
            SessionState::Terminated(e) => Poll::Ready(Err(e.kind().into())),
        }
    }

    fn is_write_vectored(&self) -> bool {
        if let SessionState::Connected(io) = &self.state {
            return io.is_write_vectored();
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicI32, Ordering},
        Arc,
    };

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    use tracing::Instrument;

    use super::*;

    #[tokio::test]
    async fn session_connect() {
        let _ = tracing_subscriber::fmt::try_init();
        let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();

        let addr = listener.local_addr().unwrap();

        let mut session = DurableSession::<TcpStream>::new(addr);
        session.blocking_connect().await.unwrap();

        let last_rx = Arc::new(AtomicI32::new(0));

        let cloned_rx = Arc::clone(&last_rx);
        tokio::spawn(
            async move {
                let (mut socket, addr) = listener.accept().await.unwrap();
                tracing::info!("Accepted connection from {}", addr);

                let mut buf = [0u8; 1024];
                let _ = socket.read(&mut buf).await.unwrap();
                loop {
                    let recv = socket.read_i32().await.unwrap();
                    if recv == 3 {
                        tracing::info!("Breaking off client connection");
                        drop(socket);
                        break;
                    }
                    tracing::info!("Received {}", recv);
                }

                // let listener = TcpListener::bind(addr).await.unwrap();
                let (mut socket, addr) = listener.accept().await.unwrap();
                tracing::info!("Accepted connection from {}", addr);

                let mut buf = [0u8; 1024];
                let _ = socket.read(&mut buf).await.unwrap();
                loop {
                    let recv = socket.read_i32().await.unwrap();
                    cloned_rx.store(recv, Ordering::Relaxed);
                    tracing::info!("Received {}", recv);
                }
            }
            .instrument(tracing::info_span!("server")),
        );

        for i in 0..=10 {
            session.write_i32(i).await.unwrap();
            tracing::info!("Sent {}", i);
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        assert_eq!(
            last_rx.load(Ordering::Relaxed),
            10,
            "Last received value should be 10"
        );
    }

    #[tokio::test]
    async fn session_with_layer() {
        struct TestLayer;

        impl Layer<TcpStream> for TestLayer {
            fn process(&mut self, io: TcpStream) -> PendingIo<TcpStream> {
                Box::pin(async move {
                    let mut io = io;
                    io.write_i32(10).await.unwrap();
                    tracing::debug!("Wrote initial in layer");
                    Ok(io)
                })
            }
        }

        let _ = tracing_subscriber::fmt::try_init();
        let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();

        let addr = listener.local_addr().unwrap();

        let mut session = DurableSession::<TcpStream>::new(addr).with_layer(TestLayer);
        session.blocking_connect().await.unwrap();

        let (mut socket, addr) = listener.accept().await.unwrap();
        tracing::info!("Accepted connection from {}", addr);

        tokio::spawn(
            async move {
                let mut i = 0;
                loop {
                    session.write_i32(i).await.unwrap();
                    tracing::info!("Sent {}", i);
                    i += 1;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
            .instrument(tracing::info_span!("client")),
        );

        loop {
            let recv = socket.read_i32().await.unwrap();
            if recv == 3 {
                tracing::info!("Breaking off client connection");
                drop(socket);
                break;
            }
            tracing::info!("Received {}", recv);
        }
    }
}
