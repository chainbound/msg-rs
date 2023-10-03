use std::{
    io::{self, IoSlice},
    ops::{Deref, DerefMut},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures::{Future, FutureExt};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tracing::{debug, error, info};

use super::config::ReconnectOptions;

/// Trait that should be implemented for an [AsyncRead] and/or [AsyncWrite]
/// item to enable it to work with the [DurableIo] struct.
pub trait UnderlyingIo<C>: Sized + Unpin
where
    C: Clone + Send + Unpin,
{
    /// The creation function is used by StubbornIo in order to establish both the initial IO connection
    /// in addition to performing reconnects.
    fn establish(ctor_arg: C) -> Pin<Box<dyn Future<Output = io::Result<Self>> + Send>>;

    /// When IO items experience an [io::Error](io::Error) during operation, it does not necessarily mean
    /// it is a disconnect/termination (ex: WouldBlock). This trait provides sensible defaults to classify
    /// which errors are considered "disconnects", but this can be overridden based on the user's needs.
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
    fn is_final_read(&self, bytes_read: usize) -> bool {
        // definitely true for tcp, perhaps true for other io as well,
        // indicative of EOF hit
        bytes_read == 0
    }
}

struct AttemptsTracker {
    attempt_num: usize,
    retries_remaining: Box<dyn Iterator<Item = Duration> + Send>,
}

struct ReconnectStatus<T, C> {
    attempts_tracker: AttemptsTracker,
    reconnect_attempt: Pin<Box<dyn Future<Output = io::Result<T>> + Send>>,
    _phantom_data: std::marker::PhantomData<C>,
}

impl<T, C> ReconnectStatus<T, C>
where
    T: UnderlyingIo<C>,
    C: Clone + Send + Unpin + 'static,
{
    pub fn new(options: &ReconnectOptions) -> Self {
        ReconnectStatus {
            attempts_tracker: AttemptsTracker {
                attempt_num: 0,
                retries_remaining: (options.retries_to_attempt_fn)(),
            },
            reconnect_attempt: Box::pin(async { unreachable!("Not going to happen") }),
            _phantom_data: std::marker::PhantomData,
        }
    }
}

/// The StubbornIo is a wrapper over a tokio AsyncRead/AsyncWrite item that will automatically
/// invoke the [UnderlyingIo::establish] upon initialization and when a reconnect is needed.
/// Because it implements deref, you are able to invoke all of the original methods on the wrapped IO.
pub struct DurableIo<T, C> {
    status: Status<T, C>,
    underlying_io: Option<T>,
    options: ReconnectOptions,
    ctor_arg: C,
}

enum Status<T, C> {
    Connected,
    Disconnected(ReconnectStatus<T, C>),
    FailedAndExhausted, // the way one feels after programming in dynamically typed languages
}

fn exhausted_err<T>() -> Poll<io::Result<T>> {
    let io_err = io::Error::new(
        io::ErrorKind::NotConnected,
        "Disconnected. Connection attempts have been exhausted.",
    );
    Poll::Ready(Err(io_err))
}

impl<T, C> Deref for DurableIo<T, C> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.underlying_io.as_ref().unwrap()
    }
}

impl<T, C> DerefMut for DurableIo<T, C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.underlying_io.as_mut().unwrap()
    }
}

impl<T, C> DurableIo<T, C>
where
    T: UnderlyingIo<C>,
    C: Clone + Send + Unpin + 'static,
{
    /// Connects or creates a handle to the UnderlyingIo item,
    /// using the default reconnect options.
    pub async fn connect(ctor_arg: C) -> io::Result<Self> {
        let options = ReconnectOptions::new();
        Self::connect_with_options(ctor_arg, options).await
    }

    /// Connects or creates a handle to the UnderlyingIo item, with the given options.
    pub async fn connect_with_options(ctor_arg: C, options: ReconnectOptions) -> io::Result<Self> {
        let (status, tcp) = match T::establish(ctor_arg.clone()).await {
            Ok(tcp) => {
                info!("Initial connection succeeded.");
                (options.on_connect_callback)();
                (Status::Connected, Some(tcp))
            }
            Err(e) => {
                error!("Initial connection failed due to: {:?}.", e);
                (options.on_connect_fail_callback)();

                if options.with_block {
                    debug!("with_block set, attempting to reconnect now");
                    let mut result = Err(e);

                    for (i, duration) in (options.retries_to_attempt_fn)().enumerate() {
                        let reconnect_num = i + 1;

                        info!(
                            "Will re-perform initial connect attempt #{} in {:?}.",
                            reconnect_num, duration
                        );

                        tokio::time::sleep(duration).await;

                        info!("Attempting reconnect #{} now.", reconnect_num);

                        match T::establish(ctor_arg.clone()).await {
                            Ok(tcp) => {
                                result = Ok(tcp);
                                (options.on_connect_callback)();
                                info!("Initial connection successfully established.");
                                break;
                            }
                            Err(e) => {
                                (options.on_connect_fail_callback)();
                                result = Err(e);
                            }
                        }
                    }

                    match result {
                        Ok(tcp) => (Status::Connected, Some(tcp)),
                        Err(e) => {
                            error!("No more re-connect retries remaining. Never able to establish initial connection.");
                            return Err(e);
                        }
                    }
                } else {
                    let mut reconnect_status = ReconnectStatus::new(&options);
                    let ctor_arg = ctor_arg.clone();
                    reconnect_status.reconnect_attempt =
                        async move { T::establish(ctor_arg).await }.boxed();
                    (Status::Disconnected(reconnect_status), None)
                }
            }
        };

        Ok(DurableIo {
            status,
            ctor_arg,
            underlying_io: tcp,
            options,
        })
    }

    fn on_disconnect(mut self: Pin<&mut Self>, cx: &mut Context) {
        debug!("On disconnect called");
        match &mut self.status {
            // initial disconnect
            Status::Connected => {
                error!("Disconnect occurred");
                (self.options.on_disconnect_callback)();
                self.status = Status::Disconnected(ReconnectStatus::new(&self.options));
            }
            Status::Disconnected(_) => {
                (self.options.on_connect_fail_callback)();
            }
            Status::FailedAndExhausted => {
                unreachable!("on_disconnect will not occur for already exhausted state.")
            }
        };

        let ctor_arg = self.ctor_arg.clone();

        // this is ensured to be true now
        if let Status::Disconnected(reconnect_status) = &mut self.status {
            let next_duration = match reconnect_status.attempts_tracker.retries_remaining.next() {
                Some(duration) => duration,
                None => {
                    error!("No more re-connect retries remaining. Giving up.");
                    self.status = Status::FailedAndExhausted;
                    cx.waker().wake_by_ref();
                    return;
                }
            };

            let future_instant = tokio::time::sleep(next_duration);

            reconnect_status.attempts_tracker.attempt_num += 1;
            let cur_num = reconnect_status.attempts_tracker.attempt_num;

            let reconnect_attempt = async move {
                future_instant.await;
                info!("Attempting reconnect #{} now.", cur_num);
                T::establish(ctor_arg).await
            };

            reconnect_status.reconnect_attempt = Box::pin(reconnect_attempt);

            info!(
                "Will perform reconnect attempt #{} in {:?}.",
                reconnect_status.attempts_tracker.attempt_num, next_duration
            );

            cx.waker().wake_by_ref();
        }
    }

    fn poll_disconnect(mut self: Pin<&mut Self>, cx: &mut Context) {
        debug!("poll_disconnect called");
        let (attempt, attempt_num) = match &mut self.status {
            Status::Connected => unreachable!(),
            Status::Disconnected(ref mut status) => (
                Pin::new(&mut status.reconnect_attempt),
                status.attempts_tracker.attempt_num,
            ),
            Status::FailedAndExhausted => unreachable!(),
        };

        match attempt.poll(cx) {
            Poll::Ready(Ok(underlying_io)) => {
                info!("Connection re-established");
                cx.waker().wake_by_ref();
                self.status = Status::Connected;
                (self.options.on_connect_callback)();
                self.underlying_io = Some(underlying_io);
            }
            Poll::Ready(Err(err)) => {
                error!("Connection attempt #{} failed: {:?}", attempt_num, err);
                self.on_disconnect(cx);
            }
            Poll::Pending => {}
        }
    }

    fn is_read_disconnect_detected(
        &self,
        poll_result: &Poll<io::Result<()>>,
        bytes_read: usize,
    ) -> bool {
        match poll_result {
            Poll::Ready(Ok(())) if self.is_final_read(bytes_read) => true,
            Poll::Ready(Err(err)) => self.is_disconnect_error(err),
            _ => false,
        }
    }

    fn is_write_disconnect_detected<X>(&self, poll_result: &Poll<io::Result<X>>) -> bool {
        match poll_result {
            Poll::Ready(Err(err)) => self.is_disconnect_error(err),
            _ => false,
        }
    }
}

impl<T, C> AsyncRead for DurableIo<T, C>
where
    T: UnderlyingIo<C> + AsyncRead,
    C: Clone + Send + Unpin + 'static,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut self.status {
            Status::Connected => {
                let pre_len = buf.filled().len();
                let poll = AsyncRead::poll_read(
                    Pin::new(&mut self.underlying_io.as_mut().unwrap()),
                    cx,
                    buf,
                );
                let post_len = buf.filled().len();
                let bytes_read = post_len - pre_len;
                if self.is_read_disconnect_detected(&poll, bytes_read) {
                    self.on_disconnect(cx);
                    Poll::Pending
                } else {
                    poll
                }
            }
            Status::Disconnected(_) => {
                self.poll_disconnect(cx);
                Poll::Pending
            }
            Status::FailedAndExhausted => exhausted_err(),
        }
    }
}

impl<T, C> AsyncWrite for DurableIo<T, C>
where
    T: UnderlyingIo<C> + AsyncWrite,
    C: Clone + Send + Unpin + 'static,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut self.status {
            Status::Connected => {
                let poll = AsyncWrite::poll_write(
                    Pin::new(&mut self.underlying_io.as_mut().unwrap()),
                    cx,
                    buf,
                );

                if self.is_write_disconnect_detected(&poll) {
                    self.on_disconnect(cx);
                    Poll::Pending
                } else {
                    poll
                }
            }
            Status::Disconnected(_) => {
                self.poll_disconnect(cx);
                Poll::Pending
            }
            Status::FailedAndExhausted => exhausted_err(),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.status {
            Status::Connected => {
                let poll =
                    AsyncWrite::poll_flush(Pin::new(&mut self.underlying_io.as_mut().unwrap()), cx);

                if self.is_write_disconnect_detected(&poll) {
                    self.on_disconnect(cx);
                    Poll::Pending
                } else {
                    poll
                }
            }
            Status::Disconnected(_) => {
                self.poll_disconnect(cx);
                Poll::Pending
            }
            Status::FailedAndExhausted => exhausted_err(),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.status {
            Status::Connected => {
                let poll = AsyncWrite::poll_shutdown(
                    Pin::new(&mut self.underlying_io.as_mut().unwrap()),
                    cx,
                );
                if poll.is_ready() {
                    // if completed, we are disconnected whether error or not
                    self.on_disconnect(cx);
                }

                poll
            }
            Status::Disconnected(_) => Poll::Pending,
            Status::FailedAndExhausted => exhausted_err(),
        }
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match &mut self.status {
            Status::Connected => {
                let poll = AsyncWrite::poll_write_vectored(
                    Pin::new(&mut self.underlying_io.as_mut().unwrap()),
                    cx,
                    bufs,
                );

                if self.is_write_disconnect_detected(&poll) {
                    self.on_disconnect(cx);
                    Poll::Pending
                } else {
                    poll
                }
            }
            Status::Disconnected(_) => {
                self.poll_disconnect(cx);
                Poll::Pending
            }
            Status::FailedAndExhausted => exhausted_err(),
        }
    }

    fn is_write_vectored(&self) -> bool {
        if let Some(underlying_io) = &self.underlying_io {
            underlying_io.is_write_vectored()
        } else {
            false
        }
    }
}
