use rdq::queue::backend::stream::{AutoclaimOptions, Stream, StreamBuilder};
use rdq::queue::{Backend, Item, Queue};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::Redis;

pub async fn with_stream<
    I: Item + Send + Sync,
    F: Fn(Queue<I, Stream<I>>) -> Fut,
    Fut: Future<Output = ()>,
>(
    autoclaim_options: Option<AutoclaimOptions>,
    f: F,
) {
    let rd = Redis::default().with_tag("alpine").start().await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;
    let rd_port = rd.get_host_port_ipv4(6379).await.unwrap();
    let rd_url = format!("redis://127.0.0.1:{rd_port}");

    let mut builder = StreamBuilder::new(rd_url, "s", "q");
    if let Some(options) = autoclaim_options {
        builder = builder.autoclaim_options(options);
    }

    let stream = builder.build().await.unwrap();
    let queue = Queue::new(stream);

    f(queue).await;
}

pub async fn enqueue_all<I: Item + Send + Sync, B: Backend<I>>(
    queue: &mut Queue<I, B>,
    items: Vec<I>,
) {
    for item in items.iter() {
        queue.enqueue(&item).await.unwrap();
    }
}
