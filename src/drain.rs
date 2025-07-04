use tokio::time::{Duration, Instant};

use crate::queue::backend::{self, Backend};
use crate::queue::queue::Queue;

pub struct Drain<I: Send + Clone + Sync, S: Sink<I> + Clone, B: Backend<I> + Send + Clone> {
    queue: Queue<I, B>,
    sink: S,
    num_workers: usize,
    ack_interval: Duration,
    drop_options: Option<DropOptions>
}

pub struct DropOptions {
    pub drop_interval: Duration,
    pub min_idle_time: Duration,
    pub max_deliveries: u64,
    pub batch_size: u64
}

#[async_trait::async_trait]
pub trait Sink<I: Send + Sync> {
    async fn process(&self, item: &I) -> bool;
}

impl<
    I: Clone + Send + Sync + 'static,
    S: Sink<I> + Clone + Send + Sync + 'static,
    B: Backend<I> + Send + Clone + 'static
> Drain<I, S, B> {
    pub fn new(
        queue: Queue<I, B>,
        sink: S,
        num_workers: usize,
        ack_interval: Duration,
        drop_options: Option<DropOptions>
    ) -> Self {
        Self {
            queue,
            sink,
            num_workers,
            ack_interval,
            drop_options
        }
    }

    // Spawns an ack task, and `num_workers` tasks to drain
    // the queue, and begins to drain the queue in batches
    // of `num_workers` items.
    pub async fn run(
        &mut self,
        dequeue_timeout: Duration
    ) {
        let (tx_ack, rx_ack) = tokio::sync::mpsc::channel::<I>(self.num_workers);

        tokio::select! {
            _ = self.ack(rx_ack) => {
                ()
            }
            _ = self.drain(tx_ack, dequeue_timeout) => {
                ()
            }
        }
    }

    async fn ack(
        &self,
        rx_ack: tokio::sync::mpsc::Receiver<I>
    ) {
        let mut rx_ack = rx_ack;
        let mut queue = self.queue.clone();
        let ack_interval = self.ack_interval;

        loop {
            let mut items = vec![];
            while let Ok(i) = rx_ack.try_recv() {
                items.push(i);
            }

            queue.ack(&items.iter().collect()).await.unwrap();

            tokio::time::sleep(ack_interval).await;
        }
    }

    async fn drain(
        &self,
        tx_ack: tokio::sync::mpsc::Sender<I>,
        dequeue_timeout: Duration
    ) {
        let (tx_process, rx_process) = tokio::sync::mpsc::channel::<I>(self.num_workers);
        let rx_process = std::sync::Arc::new(tokio::sync::Mutex::new(rx_process));
        let mut queue = self.queue.clone();

        for _ in 0..self.num_workers {
            let sink = self.sink.clone();
            let rx_process = rx_process.clone();
            let tx_ack = tx_ack.clone();

            tokio::spawn(async move {
                loop {
                    if let Some(i) = rx_process.lock().await.recv().await {
                        let ack = sink.process(&i).await;
                        if ack {
                            tx_ack.send(i).await.unwrap();
                        }
                    }
                }
            });
        }

        let mut drop_timer = Instant::now();
        loop {
            if let Some(options) = &self.drop_options {
                let drop_options = backend::DropOptions {
                    min_idle_time: options.min_idle_time,
                    max_deliveries: options.max_deliveries,
                    count: options.batch_size
                };

                if drop_timer.elapsed() > options.drop_interval {
                    drop_timer = Instant::now();
                    queue.drop_items(&drop_options).await.unwrap();
                }
            }

            let items : Vec<I> = queue.dequeue(self.num_workers, Some(dequeue_timeout)).await.unwrap();
            for item in items.into_iter() {
                tx_process.send(item).await.unwrap();
            }
        }
    }
}
