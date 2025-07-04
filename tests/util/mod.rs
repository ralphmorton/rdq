use rand::Rng;
use rdq::queue::{Backend, Queue, Item};
use rdq::queue::backend::stream::{AutoclaimOptions, Stream};

const REDIS_CONNECTION_STRING : &'static str = "redis://localhost:6378";

pub async fn create_stream_queue<I: Item + Send + Sync>(
    autoclaim_options: Option<AutoclaimOptions>
) -> Queue<I, Stream<I>> {
    let stream_name = rand::rng()
        .sample_iter(rand::distr::Alphanumeric)
        .take(32)
        .map(char::from)
        .collect();

    let queue_name = format!("q-{}", &stream_name);

    let stream = Stream::build(
        REDIS_CONNECTION_STRING,
        stream_name,
        queue_name,
        "consumer".to_string(),
        autoclaim_options
    ).await.unwrap();

    Queue::new(stream)
}

pub async fn enqueue_all<I: Item + Send + Sync, B: Backend<I>>(
    queue: &mut Queue<I, B>,
    items: Vec<I>
) {
    for item in items.iter() {
        queue.enqueue(&item).await.unwrap();
    }
}
