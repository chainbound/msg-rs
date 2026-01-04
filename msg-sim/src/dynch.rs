//! This module provides the building blocks for creating an actor able to process dynamically
//! typed requests, while maintaing type-safety and returning the appropriately typed responses.
//!
//! # Rationale
//!
//! In the actor model, an actor is a long lived components that communicates with other services
//! by sharing messages. Those messages are usually typed, meaning an actor know upront which type
//! of requests they will receive. While this covers the majority of use-cases, there might be some
//! where this is not applicable, and workarounds result in inconvenient solutions compared to
//! paying a little cost of a virtual table lookup for dynamic dispatching.
//!
//! A class of such examples is one where an actor is tied to some non-cheapy-clonable resources. In this
//! case, it is not possible to provide multiple actors for each type of message we want to
//! communicate in our application by assumption.
//! A member of such class, that motivated this code from first principles, is an actor backed by a
//! OS thread that runs in a isolated Linux network namespace. This primitive allows to interact
//! with many Linux namespace within the same binary, which is the raison d'etre of this create.
//! Trying to clone this actor for each message type means spwaning another OS thread and, in case
//! there is necessity of asynchronous code within the actor logic, an additional asynchronous
//! runtime.

use tokio::sync::{mpsc, oneshot};

use std::{any::Any, future::Future, marker::PhantomData, pin::Pin};

/// A syntax-sugar alias for a trait object which is `Any + Send + 'static`.
type AnySendStatic = dyn Any + Send + 'static;

/// Alias for a [`Future`] trait object that can be [`Send`].
///
/// Note: it is lifetime-parameterized so it can borrow from the provided context.
pub type DynFuture<'a, T = Box<AnySendStatic>> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A boxed function that, given a mutable reference to `Ctx`, produces a future.
///
/// The `for<'a>` makes this callable with *any* borrow lifetime of `&'a mut Ctx`,
/// and the returned future is allowed to live for that same `'a`.
pub type DynTask<Ctx, T = Box<AnySendStatic>> =
    Box<dyn for<'a> FnOnce(&'a mut Ctx) -> DynFuture<'a, T> + Send + 'static>;

/// A dynamically typed request that can be created and sent when calling
/// [`DynRequestSender::submit`]. This is intended to ensure the correct type downcasting of the
/// result.
pub struct DynRequest<Ctx> {
    /// The function that will be invoked by the receiver with access to `Ctx`,
    /// producing the future to execute.
    task: DynTask<Ctx>,
    /// The channel to send back the result of running `task(ctx).await`.
    tx: oneshot::Sender<Box<AnySendStatic>>,
}

impl<Ctx> From<DynRequest<Ctx>> for (DynTask<Ctx>, oneshot::Sender<Box<AnySendStatic>>) {
    fn from(value: DynRequest<Ctx>) -> Self {
        (value.task, value.tx)
    }
}

impl<Ctx> DynRequest<Ctx> {
    /// Break the request into its task and the response channel.
    pub fn into_parts(self) -> (DynTask<Ctx>, oneshot::Sender<Box<AnySendStatic>>) {
        self.into()
    }
}

/// The response received after awaiting a [`DynRequestSender::submit`] request.
/// Internally, it holds a marker to the type needed for safe type downcasting.
pub struct DynRequestResponse<T: 'static> {
    /// The channel to receive the response once it has been processed.
    rx: oneshot::Receiver<Box<AnySendStatic>>,
    /// The marker of the type to downcast the response to.
    _marker: PhantomData<T>,
}

impl<T> DynRequestResponse<T> {
    /// Await to receive a response after having [`DynRequestSender::submit`]ted a request.
    ///
    /// Internally, it automatically downcasts the response to the type inferred when creating the
    /// request task.
    pub async fn receive(self) -> Result<T, oneshot::error::RecvError> {
        let to_cast = self.rx.await?;
        let value = *to_cast.downcast::<T>().expect("same type");
        Ok(value)
    }
}

/// The sender of [`DynRequest`].
#[derive(Debug, Clone)]
pub struct DynRequestSender<Ctx> {
    tx: mpsc::Sender<DynRequest<Ctx>>,
}

impl<Ctx> DynRequestSender<Ctx> {
    fn new(tx: mpsc::Sender<DynRequest<Ctx>>) -> Self {
        Self { tx }
    }

    /// Create a [`DynRequest`] to be sent, by providing a function which receives `&mut Ctx`
    /// and returns a future.
    ///
    /// # Example
    ///
    /// ```
    /// use msg_sim::dynch;
    ///
    /// #[derive(Default)]
    /// struct Ctx {
    ///     counter: usize,
    /// }
    ///
    /// let (tx, mut rx) = dynch::channel::<Ctx>(8);
    ///
    /// tokio::spawn(async move {
    ///     let mut ctx = Ctx::default();
    ///
    ///     while let Some(req) = rx.recv().await {
    ///         let (task, tx) = req.into_parts();
    ///         // Execute with access to actor-owned context:
    ///         let value = task(&mut ctx).await; // type-erased
    ///         let _ = tx.send(value);
    ///     }
    /// });
    ///
    /// let res = tx
    ///     .submit(|ctx| Box::pin(async move {
    ///         ctx.counter += 1;
    ///         ctx.counter
    ///     }))
    ///     .await?
    ///     .receive()
    ///     .await?;
    ///
    /// assert_eq!(res, 1);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn submit<T, F>(
        &self,
        f: F,
    ) -> std::result::Result<DynRequestResponse<T>, mpsc::error::SendError<()>>
    where
        T: Any + Send + 'static,
        // The caller provides a function that can borrow `ctx` for some `'a`,
        // and returns a future that may also live for `'a`.
        F: for<'a> FnOnce(&'a mut Ctx) -> DynFuture<'a, T> + Send + 'static,
    {
        // Type-erase the result into Box<dyn Any + Send>.
        let task: DynTask<Ctx> = Box::new(move |ctx: &mut Ctx| {
            let fut = f(ctx);
            Box::pin(async move { Box::new(fut.await) as Box<AnySendStatic> })
        });

        let (tx, rx) = oneshot::channel();
        let request = DynRequest { task, tx };
        self.tx.send(request).await.map_err(|_| mpsc::error::SendError(()))?;

        Ok(DynRequestResponse { rx, _marker: PhantomData })
    }
}

pub fn channel<Ctx>(buffer: usize) -> (DynRequestSender<Ctx>, mpsc::Receiver<DynRequest<Ctx>>) {
    let (tx, rx) = mpsc::channel(buffer);
    (DynRequestSender::new(tx), rx)
}
