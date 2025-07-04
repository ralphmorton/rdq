use std::time::Duration;

use crate::queue::backend::{Backend, DropOptions, DroppedItem};
use crate::queue::error::Error;
use crate::queue::item::Item;

#[derive(Clone)]
pub struct Combine<I1: Item, I2: Item, B1: Backend<I1>, B2: Backend<I2>> {
    i1: std::marker::PhantomData<I1>,
    i2: std::marker::PhantomData<I2>,
    backend1: B1,
    backend2: B2,
    dequeue_strategy: DequeueStrategy,
    dequeue_stage: DequeueStage
}

#[derive(Clone)]
enum DequeueStage {
    Backend1,
    Backend2
}

#[derive(Clone)]
pub enum DequeueStrategy {
    RoundRobin,
    Precedence
}

#[derive(Clone, Debug, PartialEq)]
pub enum Either<A, B> {
    Left(A),
    Right(B)
}

impl<I1: Item, I2: Item, B1: Backend<I1>, B2: Backend<I2>> Combine<I1, I2, B1, B2> {
    pub fn new(
        backend1: B1,
        backend2: B2,
        dequeue_strategy: DequeueStrategy
    ) -> Self {
        Self {
            i1: std::marker::PhantomData::default(),
            i2: std::marker::PhantomData::default(),
            backend1,
            backend2,
            dequeue_strategy,
            dequeue_stage: DequeueStage::Backend1
        }
    }

    async fn dequeue_round_robin(
        &mut self,
        n: usize,
        timeout: Option<Duration>
    ) -> Result<Vec<Either<I1, I2>>, Error> {
        let res = match self.dequeue_stage {
            DequeueStage::Backend1 => {
                self.backend1
                    .dequeue(n, timeout)
                    .await?
                    .into_iter()
                    .map(Either::left)
                    .collect()
            },
            DequeueStage::Backend2 => {
                self.backend2
                    .dequeue(n, timeout)
                    .await?
                    .into_iter()
                    .map(Either::right)
                    .collect()
            }
        };

        self.dequeue_stage = self.dequeue_stage.next();
        Ok(res)
    }

    async fn dequeue_precedence(
        &mut self,
        n: usize,
        timeout: Option<Duration>
    ) -> Result<Vec<Either<I1, I2>>, Error> {
        let items : Vec<Either<I1, I2>> = self.backend1
            .dequeue(n, None)
            .await?
            .into_iter()
            .map(Either::left)
            .collect();

        if !items.is_empty() {
            return Ok(items)
        }

        let items : Vec<Either<I1, I2>> = self.backend2
            .dequeue(n, None)
            .await?
            .into_iter()
            .map(Either::right)
            .collect();

        if !items.is_empty() {
            return Ok(items)
        }

        let items : Vec<Either<I1, I2>> = self.backend1
            .dequeue(n, timeout)
            .await?
            .into_iter()
            .map(Either::left)
            .collect();

        Ok(items)
    }
}

#[async_trait::async_trait]
impl<
    I1: Item + Send + Sync,
    I2: Item + Send + Sync,
    B1: Backend<I1> + Send + Sync,
    B2: Backend<I2> + Send + Sync
> Backend<Either<I1, I2>> for Combine<I1, I2, B1, B2> {
    async fn enqueue(
        &mut self,
        item: &Either<I1, I2>
    ) -> Result<(), Error> {
        match item {
            Either::Left(i) => self.backend1.enqueue(i).await,
            Either::Right(i) => self.backend2.enqueue(i).await
        }
    }

    async fn dequeue(
        &mut self,
        n: usize,
        timeout: Option<Duration>
    ) -> Result<Vec<Either<I1, I2>>, Error> {
        match self.dequeue_strategy {
            DequeueStrategy::RoundRobin => self.dequeue_round_robin(n, timeout).await,
            DequeueStrategy::Precedence => self.dequeue_precedence(n, timeout).await
        }
    }

    async fn ack(&mut self, items: &Vec<&Either<I1, I2>>) -> Result<(), Error> {
        let i1 = items.into_iter().filter_map(|i| Either::as_left(*i)).collect();
        let i2 = items.into_iter().filter_map(|i| Either::as_right(*i)).collect();

        self.backend1.ack(&i1).await?;
        self.backend2.ack(&i2).await?;

        Ok(())
    }

    async fn drop_items(
        &mut self,
        options: &DropOptions
    ) -> Result<Vec<DroppedItem>, Error> {
        let d1 = self.backend1.drop_items(options).await?;
        let mut d2 = self.backend2.drop_items(options).await?;

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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use crate::queue::{Backend, DroppedItem, Error, Item};

    #[derive(Clone)]
    struct TestBackend<I: Item + Clone> {
        enqueued: Arc<Mutex<std::collections::VecDeque<I>>>,
        acked: Arc<Mutex<Vec<I>>>
    }

    impl<I: Item + Clone> TestBackend<I> {
        fn new() -> Self {
            Self {
                enqueued: Arc::new(Mutex::new(std::collections::VecDeque::new())),
                acked: Arc::new(Mutex::new(vec![]))
            }
        }

        fn get_enqueued(&self) -> std::collections::VecDeque<I> {
            self.enqueued.lock().unwrap().clone()
        }

        fn get_acked(&self) -> Vec<I> {
            self.acked.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl<I: Item + Clone + Send + Sync> Backend<I> for TestBackend<I> {
        async fn enqueue(
            &mut self,
            item: &I
        ) -> Result<(), Error> {
            self.enqueued
                .lock()
                .unwrap()
                .push_back(item.clone());

            Ok(())
        }

        async fn dequeue(
            &mut self,
            n: usize,
            _timeout: Option<std::time::Duration>
        ) -> Result<Vec<I>, Error> {
            let mut res = vec![];

            for _ in 0..n {
                if let Some(item) = self.enqueued.lock().unwrap().pop_front() {
                    res.push(item);
                }
            }

            Ok(res)
        }

        async fn ack(
            &mut self,
            items: &Vec<&I>
        ) -> Result<(), Error> {
            let mut items = items.iter().map(|i| (*i).clone()).collect();

            self.acked.lock().unwrap().append(&mut items);
            Ok(())
        }

        async fn drop_items(
            &mut self,
            _options: &crate::queue::backend::DropOptions
        ) -> Result<Vec<DroppedItem>, Error> {
            Ok(vec![])
        }
    }

    mod round_robin {
        use super::*;
        use crate::queue::JsonItem;
        use crate::queue::backend::combine::{Combine, DequeueStrategy, Either};

        #[tokio::test]
        async fn enqueues_into_correct_backend() {
            let b1 : TestBackend<JsonItem<i32>> = TestBackend::new();
            let b2 : TestBackend<JsonItem<i32>> = TestBackend::new();
            let mut c = Combine::new(b1.clone(), b2.clone(), DequeueStrategy::RoundRobin);
            c.enqueue(&Either::Left(JsonItem::new(42))).await.unwrap();

            let enqueued_b1 : Vec<JsonItem<i32>> = b1.get_enqueued().into_iter().collect();
            let expected_b1 = vec![JsonItem::new(42)];
            assert_eq!(enqueued_b1, expected_b1);

            let enqueued_b2 : Vec<JsonItem<i32>> = b2.get_enqueued().into_iter().collect();
            let expected_b2 = vec![];
            assert_eq!(enqueued_b2, expected_b2);

            let b1 : TestBackend<JsonItem<i32>> = TestBackend::new();
            let b2 : TestBackend<JsonItem<i32>> = TestBackend::new();
            let mut c = Combine::new(b1.clone(), b2.clone(), DequeueStrategy::RoundRobin);
            c.enqueue(&Either::Right(JsonItem::new(42))).await.unwrap();

            let enqueued_b1 : Vec<JsonItem<i32>> = b1.get_enqueued().into_iter().collect();
            let expected_b1 = vec![];
            assert_eq!(enqueued_b1, expected_b1);

            let enqueued_b2 : Vec<JsonItem<i32>> = b2.get_enqueued().into_iter().collect();
            let expected_b2 = vec![JsonItem::new(42)];
            assert_eq!(enqueued_b2, expected_b2);
        }

        #[tokio::test]
        async fn dequeues_round_robin() {
            let b1 : TestBackend<JsonItem<i32>> = TestBackend::new();
            let b2 : TestBackend<JsonItem<i32>> = TestBackend::new();
            let mut c = Combine::new(b1.clone(), b2.clone(), DequeueStrategy::RoundRobin);

            c.enqueue(&Either::Left(JsonItem::new(1))).await.unwrap();
            c.enqueue(&Either::Right(JsonItem::new(2))).await.unwrap();
            c.enqueue(&Either::Left(JsonItem::new(3))).await.unwrap();
            c.enqueue(&Either::Right(JsonItem::new(4))).await.unwrap();
            c.enqueue(&Either::Left(JsonItem::new(5))).await.unwrap();

            let enqueued_b1 : Vec<JsonItem<i32>> = b1.get_enqueued().into_iter().collect();
            let expected_b1 = vec![
                JsonItem::new(1),
                JsonItem::new(3),
                JsonItem::new(5)
            ];
            assert_eq!(enqueued_b1, expected_b1);

            let enqueued_b2 : Vec<JsonItem<i32>> = b2.get_enqueued().into_iter().collect();
            let expected_b2 = vec![
                JsonItem::new(2),
                JsonItem::new(4)
            ];
            assert_eq!(enqueued_b2, expected_b2);

            let dequeued = c.dequeue(2, None).await.unwrap();
            let expected = vec![
                Either::Left(JsonItem::new(1)),
                Either::Left(JsonItem::new(3))
            ];
            assert_eq!(dequeued, expected);

            let dequeued = c.dequeue(2, None).await.unwrap();
            let expected = vec![
                Either::Right(JsonItem::new(2)),
                Either::Right(JsonItem::new(4))
            ];
            assert_eq!(dequeued, expected);

            let dequeued = c.dequeue(2, None).await.unwrap();
            let expected = vec![
                Either::Left(JsonItem::new(5))
            ];
            assert_eq!(dequeued, expected);

            let dequeued = c.dequeue(2, None).await.unwrap();
            let expected = vec![];
            assert_eq!(dequeued, expected);
        }

        #[tokio::test]
        async fn acks_into_correct_backend() {
            let b1 : TestBackend<JsonItem<i32>> = TestBackend::new();
            let b2 : TestBackend<JsonItem<i32>> = TestBackend::new();
            let mut c = Combine::new(b1.clone(), b2.clone(), DequeueStrategy::RoundRobin);

            let acked_b1 = b1.get_acked();
            let acked_b2 = b2.get_acked();
            assert_eq!(acked_b1, vec![]);
            assert_eq!(acked_b2, vec![]);

            let ack_b1 = vec![JsonItem::new(1), JsonItem::new(2)];
            let ack_b1 : Vec<Either<JsonItem<i32>, JsonItem<i32>>> = ack_b1
                .into_iter()
                .map(|i| Either::left(i.clone()))
                .collect();

            c.ack(&ack_b1.iter().collect()).await.unwrap();

            let acked_b1 = b1.get_acked();
            let acked_b2 = b2.get_acked();
            let expected_b1 = vec![JsonItem::new(1), JsonItem::new(2)];
            assert_eq!(acked_b1, expected_b1);
            assert_eq!(acked_b2, vec![]);

            let ack_b2 = vec![JsonItem::new(3), JsonItem::new(4)];
            let ack_b2 : Vec<Either<JsonItem<i32>, JsonItem<i32>>> = ack_b2
                .into_iter()
                .map(|i| Either::right(i.clone()))
                .collect();

            c.ack(&ack_b2.iter().collect()).await.unwrap();

            let acked_b1 = b1.get_acked();
            let acked_b2 = b2.get_acked();
            let expected_b1 = vec![JsonItem::new(1), JsonItem::new(2)];
            let expected_b2 = vec![JsonItem::new(3), JsonItem::new(4)];
            assert_eq!(acked_b1, expected_b1);
            assert_eq!(acked_b2, expected_b2);
        }
    }

    mod precedence {
        use super::*;
        use crate::queue::JsonItem;
        use crate::queue::backend::combine::{Combine, DequeueStrategy, Either};

        #[tokio::test]
        async fn enqueues_into_correct_backend() {
            let b1 : TestBackend<JsonItem<i32>> = TestBackend::new();
            let b2 : TestBackend<JsonItem<i32>> = TestBackend::new();
            let mut c = Combine::new(b1.clone(), b2.clone(), DequeueStrategy::Precedence);
            c.enqueue(&Either::Left(JsonItem::new(42))).await.unwrap();

            let enqueued_b1 : Vec<JsonItem<i32>> = b1.get_enqueued().into_iter().collect();
            let expected_b1 = vec![JsonItem::new(42)];
            assert_eq!(enqueued_b1, expected_b1);

            let enqueued_b2 : Vec<JsonItem<i32>> = b2.get_enqueued().into_iter().collect();
            let expected_b2 = vec![];
            assert_eq!(enqueued_b2, expected_b2);

            let b1 : TestBackend<JsonItem<i32>> = TestBackend::new();
            let b2 : TestBackend<JsonItem<i32>> = TestBackend::new();
            let mut c = Combine::new(b1.clone(), b2.clone(), DequeueStrategy::Precedence);
            c.enqueue(&Either::Right(JsonItem::new(42))).await.unwrap();

            let enqueued_b1 : Vec<JsonItem<i32>> = b1.get_enqueued().into_iter().collect();
            let expected_b1 = vec![];
            assert_eq!(enqueued_b1, expected_b1);

            let enqueued_b2 : Vec<JsonItem<i32>> = b2.get_enqueued().into_iter().collect();
            let expected_b2 = vec![JsonItem::new(42)];
            assert_eq!(enqueued_b2, expected_b2);
        }

        #[tokio::test]
        async fn dequeues_by_precedence() {
            let b1 : TestBackend<JsonItem<i32>> = TestBackend::new();
            let b2 : TestBackend<JsonItem<i32>> = TestBackend::new();
            let mut c = Combine::new(b1.clone(), b2.clone(), DequeueStrategy::Precedence);

            c.enqueue(&Either::Left(JsonItem::new(1))).await.unwrap();
            c.enqueue(&Either::Right(JsonItem::new(2))).await.unwrap();
            c.enqueue(&Either::Left(JsonItem::new(3))).await.unwrap();
            c.enqueue(&Either::Right(JsonItem::new(4))).await.unwrap();
            c.enqueue(&Either::Left(JsonItem::new(5))).await.unwrap();

            let enqueued_b1 : Vec<JsonItem<i32>> = b1.get_enqueued().into_iter().collect();
            let expected_b1 = vec![
                JsonItem::new(1),
                JsonItem::new(3),
                JsonItem::new(5)
            ];
            assert_eq!(enqueued_b1, expected_b1);

            let enqueued_b2 : Vec<JsonItem<i32>> = b2.get_enqueued().into_iter().collect();
            let expected_b2 = vec![
                JsonItem::new(2),
                JsonItem::new(4)
            ];
            assert_eq!(enqueued_b2, expected_b2);

            let dequeued = c.dequeue(2, None).await.unwrap();
            let expected = vec![
                Either::Left(JsonItem::new(1)),
                Either::Left(JsonItem::new(3))
            ];
            assert_eq!(dequeued, expected);

            let dequeued = c.dequeue(2, None).await.unwrap();
            let expected = vec![
                Either::Left(JsonItem::new(5))
            ];
            assert_eq!(dequeued, expected);

            let dequeued = c.dequeue(2, None).await.unwrap();
            let expected = vec![
                Either::Right(JsonItem::new(2)),
                Either::Right(JsonItem::new(4))
            ];
            assert_eq!(dequeued, expected);

            let dequeued = c.dequeue(2, None).await.unwrap();
            let expected = vec![];
            assert_eq!(dequeued, expected);
        }

        #[tokio::test]
        async fn acks_into_correct_backend() {
            let b1 : TestBackend<JsonItem<i32>> = TestBackend::new();
            let b2 : TestBackend<JsonItem<i32>> = TestBackend::new();
            let mut c = Combine::new(b1.clone(), b2.clone(), DequeueStrategy::Precedence);

            let acked_b1 = b1.get_acked();
            let acked_b2 = b2.get_acked();
            assert_eq!(acked_b1, vec![]);
            assert_eq!(acked_b2, vec![]);

            let ack_b1 = vec![JsonItem::new(1), JsonItem::new(2)];
            let ack_b1 : Vec<Either<JsonItem<i32>, JsonItem<i32>>> = ack_b1
                .into_iter()
                .map(|i| Either::left(i.clone()))
                .collect();

            c.ack(&ack_b1.iter().collect()).await.unwrap();

            let acked_b1 = b1.get_acked();
            let acked_b2 = b2.get_acked();
            let expected_b1 = vec![JsonItem::new(1), JsonItem::new(2)];
            assert_eq!(acked_b1, expected_b1);
            assert_eq!(acked_b2, vec![]);

            let ack_b2 = vec![JsonItem::new(3), JsonItem::new(4)];
            let ack_b2 : Vec<Either<JsonItem<i32>, JsonItem<i32>>> = ack_b2
                .into_iter()
                .map(|i| Either::right(i.clone()))
                .collect();

            c.ack(&ack_b2.iter().collect()).await.unwrap();

            let acked_b1 = b1.get_acked();
            let acked_b2 = b2.get_acked();
            let expected_b1 = vec![JsonItem::new(1), JsonItem::new(2)];
            let expected_b2 = vec![JsonItem::new(3), JsonItem::new(4)];
            assert_eq!(acked_b1, expected_b1);
            assert_eq!(acked_b2, expected_b2);
        }
    }
}
