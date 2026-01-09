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
//!
//! # Cast Abstraction
//!
//! The channel is generic over a [`Cast`] strategy that determines how values are type-erased
//! and recovered. This abstraction enables different communication scenarios:
//!
//! - [`AnyCast`]: In-process communication using `dyn Any` (default, zero serialization overhead)
//! - Future implementations could support cross-process communication via serialization

use tokio::sync::{mpsc, oneshot};

use std::{any::Any, future::Future, marker::PhantomData, pin::Pin};

/// Trait for type erasure and reconstruction strategies.
///
/// This trait abstracts the mechanism used to erase and recover types when sending
/// messages through the dynamic channel. Different implementations enable different
/// communication scenarios.
///
/// # Safety Contract
///
/// Implementations must guarantee that `recover::<T>` correctly reconstructs a value
/// that was erased with `erase::<T>`. Calling `recover` with a different type than
/// was used for `erase` may panic or return incorrect data.
///
/// # Example
///
/// See [`AnyCast`].
/// ```
pub trait Cast: Send + 'static {
    /// The type-erased representation used for transport.
    type Erased: Send + 'static;

    /// Erase the type of a value into the transport representation.
    fn erase<T: Send + 'static>(value: T) -> Self::Erased;

    /// Recover the original type from the erased representation.
    ///
    /// # Panics
    ///
    /// Panics if the type `T` doesn't match the type that was originally erased.
    fn recover<T: Send + 'static>(erased: Self::Erased) -> T;
}

/// In-process type erasure using `dyn Any`.
///
/// This is the default and most efficient strategy for same-process communication.
/// It uses Rust's [`Any`] trait for dynamic typing with zero serialization overhead.
///
/// # Example
///
/// ```
/// use msg_sim::dynch::{Cast, AnyCast};
///
/// let original: i32 = 42;
/// let erased = AnyCast::erase(original);
/// let recovered: i32 = AnyCast::recover(erased);
/// assert_eq!(recovered, 42);
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct AnyCast;

impl Cast for AnyCast {
    type Erased = Box<dyn Any + Send + 'static>;

    fn erase<T: Send + 'static>(value: T) -> Self::Erased {
        Box::new(value)
    }

    fn recover<T: Send + 'static>(erased: Self::Erased) -> T {
        *erased.downcast::<T>().expect("type mismatch in AnyCast::recover")
    }
}

/// Alias for a [`Future`] trait object that can be [`Send`].
///
/// Note: it is lifetime-parameterized so it can borrow from the provided context.
pub type DynFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A boxed function that, given a mutable reference to `Ctx`, produces a future.
///
/// The `for<'a>` makes this callable with *any* borrow lifetime of `&'a mut Ctx`,
/// and the returned future is allowed to live for that same `'a`.
pub type DynTask<Ctx, T> =
    Box<dyn for<'a> FnOnce(&'a mut Ctx) -> DynFuture<'a, T> + Send + 'static>;

/// A dynamically typed request that can be created and sent when calling
/// [`DynRequestSender::submit`]. This is intended to ensure the correct type downcasting of the
/// result.
///
/// The `C` type parameter determines the type erasure strategy (defaults to [`AnyCast`]).
pub struct DynRequest<Ctx, C: Cast = AnyCast> {
    /// The function that will be invoked by the receiver with access to `Ctx`,
    /// producing the future to execute.
    task: DynTask<Ctx, C::Erased>,
    /// The channel to send back the result of running `task(ctx).await`.
    tx: oneshot::Sender<C::Erased>,
}

impl<Ctx, C: Cast> From<DynRequest<Ctx, C>>
    for (DynTask<Ctx, C::Erased>, oneshot::Sender<C::Erased>)
{
    fn from(value: DynRequest<Ctx, C>) -> Self {
        (value.task, value.tx)
    }
}

impl<Ctx, C: Cast> DynRequest<Ctx, C> {
    /// Break the request into its task and the response channel.
    pub fn into_parts(self) -> (DynTask<Ctx, C::Erased>, oneshot::Sender<C::Erased>) {
        self.into()
    }
}

/// The response received after awaiting a [`DynRequestSender::submit`] request.
/// Internally, it holds a marker to the type needed for safe type downcasting.
///
/// The `C` type parameter determines the type erasure strategy (defaults to [`AnyCast`]).
pub struct DynRequestResponse<T: 'static, C: Cast = AnyCast> {
    /// The channel to receive the response once it has been processed.
    rx: oneshot::Receiver<C::Erased>,
    /// The marker of the type to downcast the response to.
    _marker: PhantomData<(T, C)>,
}

impl<T: Send + 'static, C: Cast> DynRequestResponse<T, C> {
    /// Await to receive a response after having [`DynRequestSender::submit`]ted a request.
    ///
    /// Internally, it automatically recovers the response to the type inferred when creating the
    /// request task using the [`Cast`] strategy.
    pub async fn receive(self) -> Result<T, oneshot::error::RecvError> {
        let erased = self.rx.await?;
        let value = C::recover::<T>(erased);
        Ok(value)
    }
}

/// The sender of [`DynRequest`].
///
/// The `C` type parameter determines the type erasure strategy (defaults to [`AnyCast`]).
#[derive(Debug)]
pub struct DynRequestSender<Ctx, C: Cast = AnyCast> {
    tx: mpsc::Sender<DynRequest<Ctx, C>>,
}

impl<Ctx, C: Cast> Clone for DynRequestSender<Ctx, C> {
    fn clone(&self) -> Self {
        Self { tx: self.tx.clone() }
    }
}

impl<Ctx, C: Cast> DynRequestSender<Ctx, C> {
    fn new(tx: mpsc::Sender<DynRequest<Ctx, C>>) -> Self {
        Self { tx }
    }

    /// Create a [`DynRequest`] to be sent, by providing a function which receives `&mut Ctx`
    /// and returns a future.
    ///
    /// # Example
    ///
    /// ```
    /// use msg_sim::dynch::DynCh;
    ///
    /// #[derive(Default)]
    /// struct Ctx {
    ///     counter: usize,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let (tx, mut rx) = DynCh::<Ctx>::channel(8);
    ///
    ///     tokio::spawn(async move {
    ///         let mut ctx = Ctx::default();
    ///
    ///         while let Some(req) = rx.recv().await {
    ///             let (task, tx) = req.into_parts();
    ///             // Execute with access to actor-owned context:
    ///             let value = task(&mut ctx).await; // type-erased
    ///             let _ = tx.send(value);
    ///         }
    ///     });
    ///
    ///     let res = tx
    ///         .submit(|ctx| Box::pin(async move {
    ///             ctx.counter += 1;
    ///             ctx.counter
    ///         }))
    ///         .await.unwrap()
    ///         .receive()
    ///         .await.unwrap();
    ///
    ///     assert_eq!(res, 1);
    /// }
    /// ```
    pub async fn submit<T, F>(
        &self,
        f: F,
    ) -> std::result::Result<DynRequestResponse<T, C>, mpsc::error::SendError<()>>
    where
        T: Send + 'static,
        // The caller provides a function that can borrow `ctx` for some `'a`,
        // and returns a future that may also live for `'a`.
        F: for<'a> FnOnce(&'a mut Ctx) -> DynFuture<'a, T> + Send + 'static,
    {
        // Type-erase the result using the Cast strategy.
        let task: DynTask<Ctx, C::Erased> = Box::new(move |ctx: &mut Ctx| {
            let fut = f(ctx);
            Box::pin(async move { C::erase(fut.await) })
        });

        let (tx, rx) = oneshot::channel();
        let request = DynRequest { task, tx };
        self.tx.send(request).await.map_err(|_| mpsc::error::SendError(()))?;

        Ok(DynRequestResponse { rx, _marker: PhantomData })
    }
}

/// A factory type for creating dynamic channels with specific type parameters.
///
/// # Type Parameters
///
/// - `Ctx`: The context type that will be available to tasks executed in the channel.
/// - `C`: The [`Cast`] strategy for type erasure (defaults to [`AnyCast`]).
///
/// # Examples
///
/// Using the default [`AnyCast`] strategy (most common):
///
/// ```
/// use msg_sim::dynch::DynCh;
///
/// struct MyContext {
///     data: Vec<u8>,
/// }
///
/// // Create a channel with MyContext, using default AnyCast
/// let (tx, rx) = DynCh::<MyContext>::channel(8);
/// ```
pub struct DynCh<Ctx = (), C: Cast = AnyCast> {
    _marker_ctx: PhantomData<Ctx>,
    _marker_cast: PhantomData<C>,
}

impl<Ctx, C: Cast> DynCh<Ctx, C> {
    /// Create a new dynamic channel with the specified buffer size.
    ///
    /// Returns a sender/receiver pair where:
    /// - [`DynRequestSender`]: Used to submit tasks that will execute with access to `Ctx`
    /// - [`mpsc::Receiver`]: Used by the actor to receive and execute tasks
    ///
    /// # Arguments
    ///
    /// * `buffer` - The capacity of the channel's internal buffer
    ///
    /// # Example
    ///
    /// ```
    /// use msg_sim::dynch::DynCh;
    ///
    /// #[derive(Default)]
    /// struct Ctx {
    ///     counter: usize,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let (tx, mut rx) = DynCh::<Ctx>::channel(8);
    ///
    ///     tokio::spawn(async move {
    ///         let mut ctx = Ctx::default();
    ///
    ///         while let Some(req) = rx.recv().await {
    ///             let (task, tx) = req.into_parts();
    ///             let value = task(&mut ctx).await;
    ///             let _ = tx.send(value);
    ///         }
    ///     });
    ///
    ///     let res = tx
    ///         .submit(|ctx| Box::pin(async move {
    ///             ctx.counter += 1;
    ///             ctx.counter
    ///         }))
    ///         .await.unwrap()
    ///         .receive()
    ///         .await.unwrap();
    ///
    ///     assert_eq!(res, 1);
    /// }
    /// ```
    pub fn channel(
        buffer: usize,
    ) -> (DynRequestSender<Ctx, C>, mpsc::Receiver<DynRequest<Ctx, C>>) {
        let (tx, rx) = mpsc::channel(buffer);
        (DynRequestSender::new(tx), rx)
    }
}
