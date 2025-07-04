pub mod combine;
pub mod stream;

use crate::queue::error::Error;

#[async_trait::async_trait]
pub trait Backend<I> {
    async fn enqueue(&mut self, item: &I) -> Result<(), Error>;
    async fn dequeue(&mut self, n: usize, timeout: Option<std::time::Duration>) -> Result<Vec<I>, Error>;
    async fn ack(&mut self, items: &Vec<&I>) -> Result<(), Error>;
    async fn drop_items(&mut self, options: &DropOptions) -> Result<Vec<DroppedItem>, Error>;
}

#[derive(Clone)]
#[allow(dead_code)]
pub struct DropOptions {
    pub min_idle_time: std::time::Duration,
    pub max_deliveries: u64,
    pub count: u64
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct DroppedItem {
    pub id: String,
    pub idle: u64,
    pub deliveries: u64
}
