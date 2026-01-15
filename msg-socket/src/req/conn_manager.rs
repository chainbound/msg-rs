use std::{
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arc_swap::ArcSwap;
use bytes::Bytes;
use futures::{Future, FutureExt, SinkExt, StreamExt};
use msg_common::span::{EnterSpan as _, WithSpan};
use tokio_util::codec::Framed;
use tracing::Instrument;

use crate::{ClientOptions, ConnectionState, ExponentialBackoff};

use msg_transport::{Address, MeteredIo, Transport};
use msg_wire::{auth, reqrep};

/// A connection task that connects to a server and returns the underlying IO object.
type ConnTask<Io, Err> = Pin<Box<dyn Future<Output = Result<Io, Err>> + Send>>;

/// A connection from the transport to a server.
///
/// # Usage of Framed
/// [`Framed`] is used for encoding and decoding messages ("frames").
/// Usually, [`Framed`] has its own internal buffering mechanism, that's respected
/// when calling `poll_ready` and configured by [`Framed::set_backpressure_boundary`].
///
/// However, we don't use `poll_ready` here, and instead we flush every time we write a message to
/// the framed buffer.
pub(crate) type Conn<Io, S, A> = Framed<MeteredIo<Io, S, A>, reqrep::Codec>;

/// A connection controller that manages the connection to a server with an exponential backoff.
pub(crate) type ConnCtl<Io, S, A> = ConnectionState<Conn<Io, S, A>, ExponentialBackoff, A>;

/// A connection manager for managing client OR server connections.
/// The type parameter `S` contains the connection state, including its "side" (client / server).
pub(crate) struct ConnectionManager<T, A, S>
where
    T: Transport<A>,
    A: Address,
{
    /// The connection state, including its "side" (client / server).
    state: S,
    /// The transport used for the connection.
    transport: T,
    /// Transport stats for metering IO.
    transport_stats: Arc<ArcSwap<T::Stats>>,

    /// Connection manager tracing span.
    span: tracing::Span,
}

impl<T, A, S> ConnectionManager<T, A, S>
where
    T: Transport<A>,
    A: Address,
{
    /// Set the connection manager tracing span.
    pub(crate) fn with_span(mut self, span: tracing::Span) -> Self {
        self.span = span;
        self
    }
}

/// A client connection to a remote server.
pub(crate) struct ClientConnection<T, A>
where
    T: Transport<A>,
    A: Address,
{
    /// Options for the connection manager.
    options: ClientOptions,
    /// The address of the remote.
    addr: A,
    /// The connection task which handles the connection to the server.
    conn_task: Option<WithSpan<ConnTask<T::Io, T::Error>>>,
    /// The transport controller, wrapped in a [`ConnectionState`] for backoff.
    /// The [`Framed`] object can send and receive messages from the socket.
    conn_ctl: ConnCtl<T::Io, T::Stats, A>,
}

impl<T, A> ClientConnection<T, A>
where
    T: Transport<A>,
    A: Address,
{
    /// Reset the connection state to inactive, so that it will be re-tried.
    ///
    /// This is done when the connection is closed or an error occurs.
    #[inline]
    pub(crate) fn reset_connection(&mut self) {
        self.conn_ctl = ConnectionState::Inactive {
            addr: self.addr.clone(),
            backoff: ExponentialBackoff::from(&self.options),
        };
    }

    /// Returns a mutable reference to the connection channel if it is active.
    #[inline]
    pub fn active_connection(&mut self) -> Option<&mut Conn<T::Io, T::Stats, A>> {
        if let ConnectionState::Active { ref mut channel } = self.conn_ctl {
            Some(channel)
        } else {
            None
        }
    }
}

/// A local server connection. Manages the connection lifecycle:
/// - Accepting incoming connections.
/// - Handling established connections.
pub(crate) struct ServerConnection<T, A>
where
    T: Transport<A>,
    A: Address,
{
    /// The local address.
    addr: A,
    /// The accept task which handles accepting an incoming connection.
    accept_task: Option<WithSpan<T::Accept>>,
    /// The inbound connection.
    conn: Conn<T::Io, T::Stats, A>,
}

impl<T, A> ConnectionManager<T, A, ClientConnection<T, A>>
where
    T: Transport<A>,
    A: Address,
{
    pub(crate) fn new(
        options: ClientOptions,
        transport: T,
        addr: A,
        conn_ctl: ConnCtl<T::Io, T::Stats, A>,
        transport_stats: Arc<ArcSwap<T::Stats>>,
        span: tracing::Span,
    ) -> Self {
        let conn = ClientConnection { options, addr, conn_task: None, conn_ctl };

        Self { state: conn, transport, transport_stats, span }
    }

    /// Start the connection task to the server, handling authentication if necessary.
    /// The result will be polled by the driver and re-tried according to the backoff policy.
    fn try_connect(&mut self) {
        let connect = self.transport.connect(self.state.addr.clone());
        let token = self.state.options.auth_token.clone();

        let task = async move {
            let io = connect.await?;

            let Some(token) = token else {
                return Ok(io);
            };

            authentication_handshake::<T, A>(io, token).await
        }
        .in_current_span();

        // FIX: coercion to BoxFuture for [`SpanExt::with_current_span`]
        self.state.conn_task = Some(WithSpan::current(Box::pin(task)));
    }

    /// Reset the connection state to inactive, so that it will be re-tried.
    ///
    /// This is done when the connection is closed or an error occurs.
    #[inline]
    pub(crate) fn reset_connection(&mut self) {
        self.state.reset_connection();
    }

    /// Poll connection management logic: connection task, backoff, and retry logic.
    /// Loops until the connection is active, then returns a mutable reference to the channel.
    ///
    /// Note: this is not a `Future` impl because we want to return a reference; doing it in
    /// a `Future` would require lifetime headaches or unsafe code.
    ///
    /// Returns:
    /// * `Poll::Ready(Some(&mut channel))` if the connection is active
    /// * `Poll::Ready(None)` if we should terminate (max retries exceeded)
    /// * `Poll::Pending` if we need to wait for backoff
    #[allow(clippy::type_complexity)]
    pub(crate) fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<&mut Conn<T::Io, T::Stats, A>>> {
        loop {
            // Poll the active connection task, if any
            if let Some(ref mut conn_task) = self.state.conn_task {
                if let Poll::Ready(result) = conn_task.poll_unpin(cx).enter() {
                    // As soon as the connection task finishes, set it to `None`.
                    // - If it was successful, set the connection to active
                    // - If it failed, it will be re-tried until the backoff limit is reached.
                    self.state.conn_task = None;

                    match result.inner {
                        Ok(io) => {
                            tracing::info!("connected");

                            let metered = MeteredIo::new(io, self.transport_stats.clone());
                            let framed = Framed::new(metered, reqrep::Codec::new());
                            self.state.conn_ctl = ConnectionState::Active { channel: framed };
                        }
                        Err(e) => {
                            tracing::error!(?e, "failed to connect");
                        }
                    }
                }
            }

            // If the connection is inactive, try to connect to the server or poll the backoff
            // timer if we're already trying to connect.
            if let ConnectionState::Inactive { backoff, .. } = &mut self.state.conn_ctl {
                let Poll::Ready(item) = backoff.poll_next_unpin(cx) else {
                    return Poll::Pending;
                };

                let _span = tracing::info_span!(parent: &self.span, "connect").entered();

                if let Some(duration) = item {
                    if self.state.conn_task.is_none() {
                        tracing::debug!(backoff = ?duration, "trying connection");
                        self.try_connect();
                    } else {
                        tracing::debug!(
                            backoff = ?duration,
                            "not retrying as there is already a connection task"
                        );
                    }
                } else {
                    tracing::error!("exceeded maximum number of retries, terminating connection");
                    return Poll::Ready(None);
                }
            }

            if let ConnectionState::Active { ref mut channel } = self.state.conn_ctl {
                return Poll::Ready(Some(channel));
            }
        }
    }
}

pub struct ServerOptions {}

impl<T, A> ConnectionManager<T, A, ServerConnection<T, A>>
where
    T: Transport<A>,
    A: Address,
{
    pub(crate) fn new(
        options: ServerOptions,
        transport: T,
        addr: A,
        conn: Conn<T::Io, T::Stats, A>,
        transport_stats: Arc<ArcSwap<T::Stats>>,
        span: tracing::Span,
    ) -> Self {
        let conn = ServerConnection { addr, accept_task: None, conn };

        Self { state: conn, transport, transport_stats, span }
    }
}

/// Manages the connection lifecycle: connecting, reconnecting, and maintaining the connection.
pub(crate) struct ConnManager<T: Transport<A>, A: Address> {
    /// Options for the connection manager.
    options: ClientOptions,
    /// The connection task which handles the connection to the server.
    conn_task: Option<WithSpan<ConnTask<T::Io, T::Error>>>,
    /// The transport controller, wrapped in a [`ConnectionState`] for backoff.
    /// The [`Framed`] object can send and receive messages from the socket.
    conn_ctl: ConnCtl<T::Io, T::Stats, A>,
    /// The transport for this socket.
    transport: T,
    /// The address of the server.
    addr: A,
    /// Transport stats for metering IO.
    transport_stats: Arc<arc_swap::ArcSwap<T::Stats>>,

    /// A span to use for connection-related logging.
    span: tracing::Span,
}

/// Perform the authentication handshake with the server.
#[tracing::instrument(skip_all, "auth", fields(token = ?token))]
async fn authentication_handshake<T, A>(mut io: T::Io, token: Bytes) -> Result<T::Io, T::Error>
where
    T: Transport<A>,
    A: Address,
{
    let mut conn = Framed::new(&mut io, auth::Codec::new_client());

    conn.send(auth::Message::Auth(token)).await?;
    tracing::debug!("sent auth, waiting ack from server");

    // Wait for the response
    let Some(res) = conn.next().await else {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "connection closed").into());
    };

    match res {
        Ok(auth::Message::Ack) => {
            tracing::debug!("received ack");
            Ok(io)
        }
        Ok(msg) => {
            tracing::error!(?msg, "unexpected ack result");
            Err(io::Error::new(io::ErrorKind::PermissionDenied, "rejected").into())
        }
        Err(e) => Err(io::Error::new(io::ErrorKind::PermissionDenied, e).into()),
    }
}

impl<T, A> ConnManager<T, A>
where
    T: Transport<A>,
    A: Address,
{
    pub(crate) fn new(
        options: ClientOptions,
        transport: T,
        addr: A,
        conn_ctl: ConnCtl<T::Io, T::Stats, A>,
        transport_stats: Arc<arc_swap::ArcSwap<T::Stats>>,
        span: tracing::Span,
    ) -> Self {
        Self { options, conn_task: None, conn_ctl, transport, addr, transport_stats, span }
    }

    /// Start the connection task to the server, handling authentication if necessary.
    /// The result will be polled by the driver and re-tried according to the backoff policy.
    fn try_connect(&mut self) {
        let connect = self.transport.connect(self.addr.clone());
        let token = self.options.auth_token.clone();

        let task = async move {
            let io = connect.await?;

            let Some(token) = token else {
                return Ok(io);
            };

            authentication_handshake::<T, A>(io, token).await
        }
        .in_current_span();

        // FIX: coercion to BoxFuture for [`SpanExt::with_current_span`]
        self.conn_task = Some(WithSpan::current(Box::pin(task)));
    }

    /// Reset the connection state to inactive, so that it will be re-tried.
    ///
    /// This is done when the connection is closed or an error occurs.
    #[inline]
    pub(crate) fn reset_connection(&mut self) {
        self.conn_ctl = ConnectionState::Inactive {
            addr: self.addr.clone(),
            backoff: ExponentialBackoff::from(&self.options),
        };
    }

    /// Returns a mutable reference to the connection channel if it is active.
    #[inline]
    pub(crate) fn active_connection(&mut self) -> Option<&mut Conn<T::Io, T::Stats, A>> {
        if let ConnectionState::Active { ref mut channel } = self.conn_ctl {
            Some(channel)
        } else {
            None
        }
    }

    /// Poll connection management logic: connection task, backoff, and retry logic.
    /// Loops until the connection is active, then returns a mutable reference to the channel.
    ///
    /// Note: this is not a `Future` impl because we want to return a reference; doing it in
    /// a `Future` would require lifetime headaches or unsafe code.
    ///
    /// Returns:
    /// * `Poll::Ready(Some(&mut channel))` if the connection is active
    /// * `Poll::Ready(None)` if we should terminate (max retries exceeded)
    /// * `Poll::Pending` if we need to wait for backoff
    #[allow(clippy::type_complexity)]
    pub(crate) fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<&mut Conn<T::Io, T::Stats, A>>> {
        loop {
            // Poll the active connection task, if any
            if let Some(ref mut conn_task) = self.conn_task {
                if let Poll::Ready(result) = conn_task.poll_unpin(cx).enter() {
                    // As soon as the connection task finishes, set it to `None`.
                    // - If it was successful, set the connection to active
                    // - If it failed, it will be re-tried until the backoff limit is reached.
                    self.conn_task = None;

                    match result.inner {
                        Ok(io) => {
                            tracing::info!("connected");

                            let metered = MeteredIo::new(io, self.transport_stats.clone());
                            let framed = Framed::new(metered, reqrep::Codec::new());
                            self.conn_ctl = ConnectionState::Active { channel: framed };
                        }
                        Err(e) => {
                            tracing::error!(?e, "failed to connect");
                        }
                    }
                }
            }

            // If the connection is inactive, try to connect to the server or poll the backoff
            // timer if we're already trying to connect.
            if let ConnectionState::Inactive { backoff, .. } = &mut self.conn_ctl {
                let Poll::Ready(item) = backoff.poll_next_unpin(cx) else {
                    return Poll::Pending;
                };

                let _span = tracing::info_span!(parent: &self.span, "connect").entered();

                if let Some(duration) = item {
                    if self.conn_task.is_none() {
                        tracing::debug!(backoff = ?duration, "trying connection");
                        self.try_connect();
                    } else {
                        tracing::debug!(
                            backoff = ?duration,
                            "not retrying as there is already a connection task"
                        );
                    }
                } else {
                    tracing::error!("exceeded maximum number of retries, terminating connection");
                    return Poll::Ready(None);
                }
            }

            if let ConnectionState::Active { ref mut channel } = self.conn_ctl {
                return Poll::Ready(Some(channel));
            }
        }
    }
}
