use std::{any::Any, marker::PhantomData, pin::Pin};

use tokio::sync::{mpsc, oneshot};

// TODO: docs, and add context regarding network namespaces.

pub type DynFuture<T = Box<dyn Any + Send + 'static>> = Pin<Box<dyn Future<Output = T> + Send>>;

pub struct DynRequest {
    pub task: DynFuture,
    pub tx: oneshot::Sender<Box<dyn Any + Send + 'static>>,
}

pub struct DynRequestSender {
    tx: mpsc::Sender<DynRequest>,
}

impl DynRequestSender {
    pub fn new(tx: mpsc::Sender<DynRequest>) -> Self {
        Self { tx }
    }
}

pub struct DynRequestResponse<T: 'static> {
    rx: oneshot::Receiver<Box<dyn Any + Send + 'static>>,
    _marker: PhantomData<T>,
}

impl<T> DynRequestResponse<T> {
    pub async fn receive(self) -> Result<T, oneshot::error::RecvError> {
        let to_cast = self.rx.await?;
        let value = *to_cast.downcast::<T>().expect("same type");

        Ok(value)
    }
}

impl DynRequestSender {
    pub async fn submit<T: Any + Send + 'static, F: Future<Output = T> + Send + 'static>(
        &self,
        fut: F,
    ) -> std::result::Result<DynRequestResponse<T>, mpsc::error::SendError<DynRequest>> {
        let task = Box::pin(async move { Box::new(fut.await) as Box<dyn Any + Send + 'static> });

        let (tx, rx) = oneshot::channel();
        let request = DynRequest { task, tx };
        self.tx.send(request).await?;

        Ok(DynRequestResponse { rx, _marker: PhantomData })
    }
}
