//! This module provides the building blocks for creating an actor able to process dynamically
//! typed requests.
//!
//! # Rationale
//!
//! In the actor model, an actor is a long lived components that communicates with other services
//! by sharing messages. Those messages are usually typed, meaning an actor know upront which type
//! of requests they will receive. While this covers the majority of use-cases, there might be some
//! where this is not applicable, and workarounds result in inconvenient solutions compared to
//! paying a little cost of a virtual table lookup for dynamic dispatching.
//!
//! A class of such examples is one where an actor is tied to some non-clonable resources. In this
//! case, it is not possible to provide multiple actors for each type of message we want to
//! communicate in our application by assumption.
//! A member of such class, that motivated this code from first principles, is an actor backed by a
//! OS thread that runs in a isolated Linux network namespace. This primitive allows to interact
//! with many Linux namespace within the same binary, which is the raison d'etre of this create.
//! Trying to clone this actor for each message type means spwaning another OS thread and, in case
//! there is necessity of asynchronous code within the actor logic, an additional asynchronous
//! runtime.

use tokio::sync::{mpsc, oneshot};

use std::{any::Any, marker::PhantomData, pin::Pin};

/// A syntax-sugar alias for a trait object which is `Any + Send + 'static`.
type AnySendStatic = dyn Any + Send + 'static;

/// Alias for a [`Future`] trait object that can be [`Send`].
pub type DynFuture<T = Box<AnySendStatic>> = Pin<Box<dyn Future<Output = T> + Send>>;

/// A dynamically typed request that can be created and sent when calling
/// [`DynRequestSender::submit`]. This is inteded to ensure the correct type downcasting of the
/// result.
pub struct DynRequest {
    /// The [`Future`] task underlying this request.
    task: DynFuture,
    /// The channel to send back the result of [`Self::task`].
    tx: oneshot::Sender<Box<AnySendStatic>>,
}

impl From<DynRequest> for (DynFuture, oneshot::Sender<Box<AnySendStatic>>) {
    fn from(value: DynRequest) -> Self {
        (value.task, value.tx)
    }
}

impl DynRequest {
    /// Break the request into its task and the response channel.
    pub fn into_parts(self) -> (DynFuture, oneshot::Sender<Box<AnySendStatic>>) {
        self.into()
    }
}

/// The response received after awaiting a [`DynRequestSender::submit`] request.
/// Internally, it holds a market to the type needed for safe type downcasting.
pub struct DynRequestResponse<T: 'static> {
    /// The channel to received the response once it has been successfully been processed by the
    /// consumer of this library.
    rx: oneshot::Receiver<Box<AnySendStatic>>,
    /// The marker of the type to downcast the response to.
    _marker: PhantomData<T>,
}

impl<T> DynRequestResponse<T> {
    /// Await to receive a response after having [`DynRequestSender::submit`]ted a request.
    ///
    /// Internally, it automatically downcast the response to the type inferred when creating the
    /// request [`Future`] task.
    pub async fn receive(self) -> Result<T, oneshot::error::RecvError> {
        let to_cast = self.rx.await?;
        let value = *to_cast.downcast::<T>().expect("same type");

        Ok(value)
    }
}

/// The sender of [`DynRequest`], that will be created internally when calling
/// [`DynRequestSender::submit`].
#[derive(Debug, Clone)]
pub struct DynRequestSender {
    tx: mpsc::Sender<DynRequest>,
}

impl DynRequestSender {
    fn new(tx: mpsc::Sender<DynRequest>) -> Self {
        Self { tx }
    }
}

impl DynRequestSender {
    /// Create a [`DynRequest`] to be sent, by providing a future.
    ///
    /// # Example
    ///
    /// ```
    /// use msg_sim::dynch::self;
    ///
    /// let (tx, rx) = dynch::channel(8);
    ///
    /// tokio::spawn(async move {
    ///     while let Some(req) = rx.recv().await {
    ///         let (fut, tx) = req.into_parts();
    ///         // Here, value is type-erased.
    ///         let value = fut.await;
    ///         tx.send(value);
    ///     }
    /// });
    ///
    /// let res = tx.send(async move { "hello" }).await?.receive().await?;
    /// assert_eq!(res, "hello");
    /// ```
    pub async fn submit<T: Any + Send + 'static, F: Future<Output = T> + Send + 'static>(
        &self,
        fut: F,
    ) -> std::result::Result<DynRequestResponse<T>, mpsc::error::SendError<DynRequest>> {
        let task = Box::pin(async move { Box::new(fut.await) as Box<AnySendStatic> });

        let (tx, rx) = oneshot::channel();
        let request = DynRequest { task, tx };
        self.tx.send(request).await?;

        Ok(DynRequestResponse { rx, _marker: PhantomData })
    }
}

pub fn channel(buffer: usize) -> (DynRequestSender, mpsc::Receiver<DynRequest>) {
    let (tx, rx) = mpsc::channel(buffer);
    (DynRequestSender::new(tx), rx)
}
