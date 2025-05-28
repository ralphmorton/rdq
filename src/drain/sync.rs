use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::queue::backend::{self, Backend};
use crate::queue::item::Item;
use crate::queue::queue::Queue;

pub struct Drain<I: Item + Send + Clone, B: Backend<I> + Send + Clone> {
    queue: Queue<I, B>,
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

pub trait Sink<I: Item> {
    type InitArgs;

    fn init(args: Self::InitArgs) -> Self;
    fn process(&mut self, item: &I);
}

impl<I: Item + Clone + Send + 'static, B: Backend<I> + Send + Clone + 'static> Drain<I, B> {
    pub fn new(
        queue: Queue<I, B>,
        num_workers: usize,
        ack_interval: Duration,
        drop_options: Option<DropOptions>
    ) -> Self {
        Self {
            queue,
            num_workers,
            ack_interval,
            drop_options
        }
    }

    // Spawns an ack thread, and `num_workers` worker threads
    // to drain the queue, and begins to drain the queue in
    // batches of `num_workers` items.
    pub fn run<A: Clone + Send + 'static, S: Sink<I, InitArgs = A>>(
        &mut self,
        sink_args: A,
        dequeue_timeout: Duration
    ) -> ! {
        let (tx_ack, rx_ack) = mpsc::channel::<I>();
        let (tx_process, rx_process) = mpsc::sync_channel::<I>(self.num_workers);
        let rx_event = Arc::new(Mutex::new(rx_process));

        self.spawn_ack(rx_ack);

        for _ in 0..self.num_workers {
            let sink_args = sink_args.clone();
            let rx_event = rx_event.clone();
            let tx_ack = tx_ack.clone();

            std::thread::spawn(move || {
                let mut sink = S::init(sink_args);

                loop {
                    let i = rx_event.lock().unwrap().recv().unwrap();
                    sink.process(&i);
                    tx_ack.send(i).unwrap();
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
                    self.queue.drop_items(&drop_options).unwrap();
                }
            }

            let items : Vec<I> = self.queue.dequeue(self.num_workers, dequeue_timeout).unwrap();
            items.into_iter().for_each(|i| tx_process.send(i).unwrap());
        }
    }

    fn spawn_ack(
        &self,
        rx_ack: mpsc::Receiver<I>
    ) {
        let queue = self.queue.clone();
        let ack_interval = self.ack_interval;

        std::thread::spawn(move || {
            loop {
                let mut items = vec![];
                while let Ok(i) = rx_ack.try_recv() {
                    items.push(i);
                }

                queue.ack(&items.iter().collect()).unwrap();

                std::thread::sleep(ack_interval);
            }
        });
    }
}
