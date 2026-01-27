use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::{Future, StreamExt, stream::FuturesUnordered};
use msg_common::span::{EnterSpan as _, SpanExt as _, WithSpan};
use tokio::{sync::broadcast, task::JoinSet};
use tokio_util::codec::Framed;
use tracing::{debug, error, info, warn};

use super::{
    PubError, PubMessage, PubOptions, SocketState, session::SubscriberSession, trie::PrefixTrie,
};
use crate::{ConnectionHookErased, hooks};
use msg_transport::{Address, PeerAddress, Transport};
use msg_wire::pubsub;

/// The driver for the publisher socket. This is responsible for accepting incoming connections,
/// running connection hooks, and spawning new [`SubscriberSession`]s for each connection.
#[allow(clippy::type_complexity)]
pub(crate) struct PubDriver<T: Transport<A>, A: Address> {
    /// Session ID counter.
    pub(super) id_counter: u32,
    /// The server transport used to accept incoming connections.
    pub(super) transport: T,
    /// The publisher options (shared with the socket)
    pub(super) options: Arc<PubOptions>,
    /// The publisher socket state, shared with the socket front-end.
    pub(crate) state: Arc<SocketState>,
    /// Optional connection hook.
    pub(super) hook: Option<Arc<dyn ConnectionHookErased<T::Io>>>,
    /// A set of pending incoming connections, represented by [`Transport::Accept`].
    pub(super) conn_tasks: FuturesUnordered<T::Accept>,
    /// A joinset of connection hook tasks.
    pub(super) hook_tasks: JoinSet<WithSpan<hooks::ErasedHookResult<(T::Io, A)>>>,
    /// The receiver end of the message broadcast channel. The sender half is stored by
    /// [`PubSocket`](super::PubSocket).
    pub(super) from_socket_bcast: broadcast::Receiver<PubMessage>,
}

impl<T, A> Future for PubDriver<T, A>
where
    T: Transport<A>,
    A: Address,
{
    type Output = Result<(), PubError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            // First, poll the joinset of hook tasks. If a new connection has been handled
            // we spawn a new session for it.
            if let Poll::Ready(Some(Ok(hook_result))) = this.hook_tasks.poll_join_next(cx).enter() {
                match hook_result.inner {
                    Ok((stream, _addr)) => {
                        info!("connection hook passed");

                        let framed = Framed::new(stream, pubsub::Codec::new());

                        let session = SubscriberSession {
                            seq: 0,
                            session_id: this.id_counter,
                            from_socket_bcast: this.from_socket_bcast.resubscribe().into(),
                            state: Arc::clone(&this.state),
                            pending_egress: None,
                            conn: framed,
                            topic_filter: PrefixTrie::new(),
                            linger_timer: this
                                .options
                                .write_buffer_linger
                                .map(tokio::time::interval),
                            write_buffer_size: this.options.write_buffer_size,
                        };

                        tokio::spawn(session);

                        this.id_counter = this.id_counter.wrapping_add(1);
                    }
                    Err(e) => {
                        error!(err = ?e, "Error in connection hook");
                        this.state.stats.specific.decrement_active_clients();
                    }
                }

                continue;
            }

            // Then poll the incoming connection tasks. If a new connection has been accepted, spawn
            // a new hook task for it (or directly spawn a session if no hook is configured).
            if let Poll::Ready(Some(incoming)) = this.conn_tasks.poll_next_unpin(cx) {
                match incoming {
                    Ok(io) => {
                        if let Err(e) = this.on_incoming(io) {
                            error!(err = ?e, "Error accepting incoming connection");
                            this.state.stats.specific.decrement_active_clients();
                        }
                    }
                    Err(e) => {
                        error!(err = ?e, "Error accepting incoming connection");

                        // Active clients have already been incremented in the initial call to
                        // `poll_accept`, so we need to decrement them here.
                        this.state.stats.specific.decrement_active_clients();
                    }
                }

                continue;
            }

            // Finally, poll the transport for new incoming connection futures and push them to the
            // incoming connection tasks.
            if let Poll::Ready(accept) = Pin::new(&mut this.transport).poll_accept(cx) {
                if let Some(max) = this.options.max_clients {
                    if this.state.stats.specific.active_clients() >= max {
                        warn!("Max connections reached ({}), rejecting incoming connection", max);
                        continue;
                    }
                }

                // Increment the active clients counter. If the hook fails,
                // this counter will be decremented.
                this.state.stats.specific.increment_active_clients();

                this.conn_tasks.push(accept);

                continue;
            }

            return Poll::Pending;
        }
    }
}

impl<T, A> PubDriver<T, A>
where
    T: Transport<A>,
    A: Address,
{
    /// Handles an incoming connection. If this returns an error, the active connections counter
    /// should be decremented.
    fn on_incoming(&mut self, io: T::Io) -> Result<(), std::io::Error> {
        let addr = io.peer_addr()?;

        info!("New connection from {:?}", addr);

        // If a connection hook is configured, run it
        if let Some(ref hook) = self.hook {
            let hook = Arc::clone(hook);
            let span = tracing::info_span!("connection_hook", ?addr);

            let fut = async move {
                let stream = hook.on_connection(io).await?;
                Ok((stream, addr))
            };

            self.hook_tasks.spawn(fut.with_span(span));
        } else {
            let framed = Framed::new(io, pubsub::Codec::new());

            let session = SubscriberSession {
                seq: 0,
                session_id: self.id_counter,
                from_socket_bcast: self.from_socket_bcast.resubscribe().into(),
                state: Arc::clone(&self.state),
                pending_egress: None,
                conn: framed,
                topic_filter: PrefixTrie::new(),
                linger_timer: self.options.write_buffer_linger.map(tokio::time::interval),
                write_buffer_size: self.options.write_buffer_size,
            };

            tokio::spawn(session);

            self.id_counter = self.id_counter.wrapping_add(1);
            debug!("New connection from {:?}, session ID {}", addr, self.id_counter);
        }

        Ok(())
    }
}
