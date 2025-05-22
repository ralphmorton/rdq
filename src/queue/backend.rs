pub mod combine;
pub mod stream;

use crate::queue::error::Error;

pub trait Backend<I> {
    fn enqueue(&self, item: &I) -> Result<(), Error>;
    fn dequeue(&mut self, n: usize, timeout: std::time::Duration) -> Result<Vec<I>, Error>;
    fn ack(&self, items: &Vec<&I>) -> Result<(), Error>;
    fn drop_items(&self, options: &DropOptions) -> Result<Vec<DroppedItem>, Error>;
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
