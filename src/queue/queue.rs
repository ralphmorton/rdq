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

    pub fn enqueue(
        &self,
        item: &I
    ) -> Result<(), Error> {
        self.backend.enqueue(item)
    }

    pub fn dequeue(
        &mut self,
        n: usize,
        timeout: std::time::Duration
    ) -> Result<Vec<I>, Error> {
        self.backend.dequeue(n, timeout)
    }

    pub fn ack(
        &self,
        items: &Vec<&I>
    ) -> Result<(), Error> {
        self.backend.ack(items)
    }

    pub fn drop_items(
        &self,
        options: &DropOptions
    ) -> Result<Vec<DroppedItem>, Error> {
        self.backend.drop_items(options)
    }
}
