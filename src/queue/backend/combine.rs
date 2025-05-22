use crate::queue::backend::{Backend, DropOptions, DroppedItem};
use crate::queue::error::Error;
use crate::queue::item::Item;

pub struct Combine<I1: Item, I2: Item, B1: Backend<I1>, B2: Backend<I2>> {
    i1: std::marker::PhantomData<I1>,
    i2: std::marker::PhantomData<I2>,
    backend1: B1,
    backend2: B2,
    dequeue_stage: DequeueStage
}

enum DequeueStage {
    Backend1,
    Backend2
}

pub enum Either<A, B> {
    Left(A),
    Right(B)
}

impl<I1: Item, I2: Item, B1: Backend<I1>, B2: Backend<I2>> Combine<I1, I2, B1, B2> {
    pub fn new(backend1: B1, backend2: B2) -> Self {
        Self {
            i1: std::marker::PhantomData::default(),
            i2: std::marker::PhantomData::default(),
            backend1,
            backend2,
            dequeue_stage: DequeueStage::Backend1
        }
    }
}

impl<I1: Item, I2: Item, B1: Backend<I1>, B2: Backend<I2>> Backend<Either<I1, I2>> for Combine<I1, I2, B1, B2> {
    fn enqueue(
        &self,
        item: &Either<I1, I2>
    ) -> Result<(), Error> {
        match item {
            Either::Left(i) => self.backend1.enqueue(i),
            Either::Right(i) => self.backend2.enqueue(i)
        }
    }

    fn dequeue(
        &mut self,
        n: usize,
        timeout: std::time::Duration
    ) -> Result<Vec<Either<I1, I2>>, Error> {
        let res = match self.dequeue_stage {
            DequeueStage::Backend1 => {
                self.backend1
                    .dequeue(n, timeout)?
                    .into_iter()
                    .map(Either::left)
                    .collect()
            },
            DequeueStage::Backend2 => {
                self.backend2
                    .dequeue(n, timeout)?
                    .into_iter()
                    .map(Either::right)
                    .collect()
            }
        };

        self.dequeue_stage = self.dequeue_stage.next();
        Ok(res)
    }

    fn ack(&self, items: &Vec<&Either<I1, I2>>) -> Result<(), Error> {
        let i1 = items.into_iter().filter_map(|i| Either::as_left(*i)).collect();
        let i2 = items.into_iter().filter_map(|i| Either::as_right(*i)).collect();

        self.backend1.ack(&i1)?;
        self.backend2.ack(&i2)?;

        Ok(())
    }

    fn drop_items(
        &self,
        options: &DropOptions
    ) -> Result<Vec<DroppedItem>, Error> {
        let d1 = self.backend1.drop_items(options)?;
        let mut d2 = self.backend2.drop_items(options)?;

        let mut dropped = d1;
        dropped.append(&mut d2);

        Ok(dropped)
    }
}

impl<A, B> Either<A, B> {
    pub fn left(a: A) -> Self {
        Self::Left(a)
    }

    pub fn right(b: B) -> Self {
        Self::Right(b)
    }

    pub fn as_left(&self) -> Option<&A> {
        match self {
            Self::Left(i) => Some(i),
            Self::Right(_) => None
        }
    }

    pub fn as_right(&self) -> Option<&B> {
        match self {
            Self::Left(_) => None,
            Self::Right(i) => Some(i)
        }
    }
}

impl DequeueStage {
    fn next(&self) -> Self {
        match self {
            Self::Backend1 => Self::Backend2,
            Self::Backend2 => Self::Backend1
        }
    }
}
