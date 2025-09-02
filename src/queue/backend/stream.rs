use redis::AsyncCommands;

use crate::queue::backend::{Backend, DropOptions, DroppedItem};
use crate::queue::error::Error;
use crate::queue::item::Item;

#[derive(Clone)]
pub struct Stream<I: Item> {
    i: std::marker::PhantomData<I>,
    redis: redis::aio::ConnectionManager,
    stream_key: String,
    queue_name: String,
    consumer: String,
    autoclaim_options: Option<AutoclaimOptions>,
    dequeue_stage: DequeueStage,
}

pub struct StreamBuilder {
    redis_connection_string: String,
    stream_key: String,
    queue_name: String,
    consumer: String,
    autoclaim_options: Option<AutoclaimOptions>,
}

#[derive(Clone)]
pub struct AutoclaimOptions {
    pub frequency: usize,
    pub min_idle_time: std::time::Duration,
}

#[derive(Clone)]
enum DequeueStage {
    Autoclaim { next_stream_id: String },
    Read { next_autoclaim: Option<usize> },
}

impl StreamBuilder {
    /// Initialize a stream builder, with a random consumer (V4 UUID).
    pub fn new(
        redis_connection_string: impl Into<String>,
        stream_key: impl Into<String>,
        queue_name: impl Into<String>,
    ) -> Self {
        Self {
            redis_connection_string: redis_connection_string.into(),
            stream_key: stream_key.into(),
            queue_name: queue_name.into(),
            consumer: uuid::Uuid::new_v4().to_string(),
            autoclaim_options: None,
        }
    }

    pub fn consumer(mut self, consumer: impl Into<String>) -> Self {
        self.consumer = consumer.into();
        self
    }

    pub fn autoclaim_options(mut self, options: AutoclaimOptions) -> Self {
        self.autoclaim_options = Some(options);
        self
    }

    pub async fn build<I: Item>(self) -> Result<Stream<I>, Error> {
        Stream::new(
            &self.redis_connection_string,
            self.stream_key,
            self.queue_name,
            self.consumer,
            self.autoclaim_options,
        )
        .await
    }
}

impl<I: Item> Stream<I> {
    async fn new(
        redis_connection_string: &str,
        stream_key: String,
        queue_name: String,
        consumer: String,
        autoclaim_options: Option<AutoclaimOptions>,
    ) -> Result<Self, Error> {
        let redis = redis::Client::open(redis_connection_string)?;
        let mut redis = redis::aio::ConnectionManager::new(redis).await?;

        let queue_group_exists = if redis.exists(&stream_key).await? {
            let existing_groups: redis::streams::StreamInfoGroupsReply =
                redis.xinfo_groups(&stream_key).await?;
            existing_groups
                .groups
                .iter()
                .find(|g| g.name == queue_name)
                .is_some()
        } else {
            false
        };

        if !queue_group_exists {
            let _: () = redis
                .xgroup_create_mkstream(&stream_key, &queue_name, "$")
                .await?;
        }

        let next_autoclaim = autoclaim_options.clone().map(|o| o.frequency);

        let instance = Self {
            i: std::marker::PhantomData::default(),
            redis,
            stream_key,
            queue_name,
            consumer,
            autoclaim_options,
            dequeue_stage: DequeueStage::Read { next_autoclaim },
        };

        Ok(instance)
    }

    async fn read(
        &mut self,
        n: usize,
        timeout: Option<std::time::Duration>,
        next_autoclaim: &Option<usize>,
    ) -> Result<Vec<I>, Error> {
        let mut opts = redis::streams::StreamReadOptions::default()
            .group(&self.queue_name, &self.consumer)
            .count(n);

        if let Some(timeout) = timeout {
            opts = opts.block(timeout.as_millis() as usize);
        }

        let res: redis::streams::StreamReadReply = self
            .redis
            .xread_options(&[&self.stream_key], &[">"], &opts)
            .await?;

        if let Some(next_autoclaim) = next_autoclaim {
            self.dequeue_stage = if *next_autoclaim <= 1 {
                DequeueStage::Autoclaim {
                    next_stream_id: "0-0".to_string(),
                }
            } else {
                DequeueStage::Read {
                    next_autoclaim: Some(next_autoclaim - 1),
                }
            }
        }

        if res.keys.is_empty() {
            return Ok(vec![]);
        }

        let items = res.keys[0]
            .ids
            .iter()
            .map(|i| I::from_stream(&i).ok_or_else(|| Error::ParseError(i.clone())))
            .collect::<Result<Vec<I>, Error>>()?;

        Ok(items)
    }

    async fn autoclaim(&mut self, n: usize, next_stream_id: &str) -> Result<Vec<I>, Error> {
        let opts = redis::streams::StreamAutoClaimOptions::default().count(n);

        let res: redis::streams::StreamAutoClaimReply = self
            .redis
            .xautoclaim_options(
                &self.stream_key,
                &self.queue_name,
                &self.consumer,
                self.autoclaim_options
                    .as_ref()
                    .map(|o| o.min_idle_time.as_millis() as usize)
                    .unwrap(),
                next_stream_id,
                opts,
            )
            .await?;

        let items = res
            .claimed
            .into_iter()
            .map(|i| I::from_stream(&i).ok_or_else(|| Error::ParseError(i.clone())))
            .collect::<Result<Vec<I>, Error>>()?;

        self.dequeue_stage = if res.next_stream_id == "0-0" {
            DequeueStage::Read {
                next_autoclaim: self.autoclaim_options.as_ref().map(|o| o.frequency),
            }
        } else {
            DequeueStage::Autoclaim {
                next_stream_id: res.next_stream_id,
            }
        };

        Ok(items)
    }
}

#[async_trait::async_trait]
impl<I: Item + Send + Sync> Backend<I> for Stream<I> {
    async fn enqueue(&mut self, item: &I) -> Result<(), crate::queue::error::Error> {
        let item = item.to_stream();
        let _: () = self.redis.xadd(&self.stream_key, "*", &item).await?;

        Ok(())
    }

    async fn dequeue(
        &mut self,
        n: usize,
        timeout: Option<std::time::Duration>,
    ) -> Result<Vec<I>, crate::queue::error::Error> {
        match self.dequeue_stage.clone() {
            DequeueStage::Read { next_autoclaim } => self.read(n, timeout, &next_autoclaim).await,
            DequeueStage::Autoclaim { next_stream_id } => self.autoclaim(n, &next_stream_id).await,
        }
    }

    async fn ack(&mut self, items: &Vec<&I>) -> Result<(), crate::queue::error::Error> {
        if items.is_empty() {
            return Ok(());
        }

        let ids: Vec<&str> = items.iter().filter_map(|i| i.id()).collect();
        let _: () = self
            .redis
            .xack(&self.stream_key, &self.queue_name, &ids)
            .await?;

        Ok(())
    }

    async fn drop_items(
        &mut self,
        options: &DropOptions,
    ) -> Result<Vec<super::DroppedItem>, crate::queue::error::Error> {
        let min_idle_time = options.min_idle_time.as_millis() as u64;

        let pending: Vec<(String, String, u64, u64)> = redis::cmd("XPENDING")
            .arg(&self.stream_key)
            .arg(&self.queue_name)
            .arg("-")
            .arg("+")
            .arg(options.count)
            .query_async(&mut self.redis)
            .await?;

        let drop = pending
            .into_iter()
            .filter(|(_, _, idle, deliveries)| {
                *idle > min_idle_time && *deliveries >= options.max_deliveries
            })
            .map(|(id, _, idle, deliveries)| DroppedItem {
                id,
                idle,
                deliveries,
            })
            .collect::<Vec<DroppedItem>>();

        if !drop.is_empty() {
            let drop_ids: Vec<&str> = drop.iter().map(|d| d.id.as_str()).collect();
            let _: () = self
                .redis
                .xack(&self.stream_key, &self.queue_name, &drop_ids)
                .await?;
        }

        Ok(drop)
    }
}
