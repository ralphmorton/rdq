use crate::queue::backend::{Backend, DropOptions, DroppedItem};
use crate::queue::error::Error;

#[derive(Clone)]
pub struct Queue<I, B: Backend<I>> {
    i: std::marker::PhantomData<I>,
    backend: B
}

impl<I, B: Backend<I>> Queue<I, B> {
    pub fn new(backend: B) -> Self {
        Self {
            i: std::marker::PhantomData::default(),
            backend
        }
    }

    pub async fn enqueue(
        &mut self,
        item: &I
    ) -> Result<(), Error> {
        self.backend.enqueue(item).await
    }

    pub async fn dequeue(
        &mut self,
        n: usize,
        timeout: Option<std::time::Duration>
    ) -> Result<Vec<I>, Error> {
        self.backend.dequeue(n, timeout).await
    }

    pub async fn ack(
        &mut self,
        items: &Vec<&I>
    ) -> Result<(), Error> {
        self.backend.ack(items).await
    }

    pub async fn drop_items(
        &mut self,
        options: &DropOptions
    ) -> Result<Vec<DroppedItem>, Error> {
        self.backend.drop_items(options).await
    }
}
