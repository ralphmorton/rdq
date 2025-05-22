use redis::Commands;

use crate::queue::backend::{Backend, DropOptions, DroppedItem};
use crate::queue::error::Error;
use crate::queue::item::Item;

#[derive(Clone)]
pub struct Stream<I: Item> {
    i: std::marker::PhantomData<I>,
    redis: r2d2::Pool<redis::Client>,
    stream_key: String,
    queue_name:  String,
    consumer: String,
    autoclaim_options: Option<AutoclaimOptions>,
    dequeue_stage: DequeueStage
}

#[derive(Clone)]
pub struct AutoclaimOptions {
    pub frequency: usize,
    pub min_idle_time: std::time::Duration
}

#[derive(Clone)]
enum DequeueStage {
    Autoclaim { next_stream_id: String },
    Read { next_autoclaim: Option<usize> }
}

impl<I: Item> Stream<I> {
    pub fn build(
        redis_connection_string: &str,
        stream_key: String,
        queue_name: String,
        consumer: String,
        autoclaim_options: Option<AutoclaimOptions>
    ) -> Result<Self, Error> {
        let redis = redis::Client::open(redis_connection_string)?;
        let redis = r2d2::Pool::builder().build(redis)?;

        let mut conn = redis.get().unwrap();

        let queue_group_exists = if conn.exists(&stream_key)? {
            let existing_groups : redis::streams::StreamInfoGroupsReply = conn.xinfo_groups(&stream_key)?;
            existing_groups.groups.iter().find(|g| g.name == queue_name).is_some()
        } else {
            false
        };

        if !queue_group_exists {
            let _ : () = conn.xgroup_create_mkstream(&stream_key, &queue_name, "$")?;
        }

        let next_autoclaim = autoclaim_options.clone().map(|o| o.frequency);

        let instance = Self {
            i: std::marker::PhantomData::default(),
            redis,
            stream_key,
            queue_name,
            consumer,
            autoclaim_options,
            dequeue_stage: DequeueStage::Read { next_autoclaim }
        };

        Ok(instance)
    }

    fn read(
        &mut self,
        n: usize,
        timeout: usize,
        next_autoclaim: &Option<usize>
    ) -> Result<Vec<I>, Error> {
        let mut conn = self.redis.get()?;

        let opts = redis::streams::StreamReadOptions::default()
            .group(&self.queue_name, &self.consumer)
            .count(n)
            .block(timeout);

        let res : redis::streams::StreamReadReply = conn.xread_options(&[&self.stream_key], &[">"], &opts)?;

        if let Some(next_autoclaim) = next_autoclaim {
            self.dequeue_stage = if *next_autoclaim <= 1 {
                DequeueStage::Autoclaim { next_stream_id: "0-0".to_string() }
            } else {
                DequeueStage::Read { next_autoclaim: Some(next_autoclaim - 1) }
            }
        }

        if res.keys.is_empty() {
            return Ok(vec![])
        }

        let items = res.keys[0]
            .ids
            .iter()
            .map(|i| I::from_stream(&i).ok_or_else(|| Error::ParseError(i.clone())))
            .collect::<Result<Vec<I>, Error>>()?;

        Ok(items)
    }

    fn autoclaim(
        &mut self,
        n: usize,
        next_stream_id: &str
    ) -> Result<Vec<I>, Error> {
        let mut conn = self.redis.get()?;

        let opts = redis::streams::StreamAutoClaimOptions::default()
            .count(n);

        let res : redis::streams::StreamAutoClaimReply = conn.xautoclaim_options(
            &self.stream_key,
            &self.queue_name,
            &self.consumer,
            self.autoclaim_options.as_ref().map(|o| o.min_idle_time.as_millis() as usize).unwrap(),
            next_stream_id,
            opts
        )?;

        let items = res.claimed
            .into_iter()
            .map(|i| I::from_stream(&i).ok_or_else(|| Error::ParseError(i.clone())))
            .collect::<Result<Vec<I>, Error>>()?;

        self.dequeue_stage = if res.next_stream_id == "0-0" {
            DequeueStage::Read {
                next_autoclaim: self.autoclaim_options.as_ref().map(|o| o.frequency)
            }
        } else {
            DequeueStage::Autoclaim { next_stream_id: res.next_stream_id }
        };

        Ok(items)
    }
}

impl<I: Item> Backend<I> for Stream<I> {
    fn enqueue(
        &self,
        item: &I
    ) -> Result<(), crate::queue::error::Error> {
        let mut conn = self.redis.get()?;

        let item = item.to_stream();
        let _ : () = conn.xadd(&self.stream_key, "*", &item)?;

        Ok(())
    }

    fn dequeue(
        &mut self,
        n: usize,
        timeout: std::time::Duration
    ) -> Result<Vec<I>, crate::queue::error::Error> {
        match self.dequeue_stage.clone() {
            DequeueStage::Read { next_autoclaim } => {
                self.read(n, timeout.as_millis() as usize, &next_autoclaim)
            },
            DequeueStage::Autoclaim { next_stream_id } => {
                self.autoclaim(n, &next_stream_id)
            }
        }
    }

    fn ack(
        &self,
        items: &Vec<&I>
    ) -> Result<(), crate::queue::error::Error> {
        if items.is_empty() {
            return Ok(())
        }

        let mut conn = self.redis.get()?;

        let ids: Vec<&str> = items.iter().filter_map(|i| i.id()).collect();
        let _ : () = conn.xack(&self.stream_key, &self.queue_name, &ids)?;

        Ok(())
    }

    fn drop_items(
        &self,
        options: &DropOptions
    ) -> Result<Vec<super::DroppedItem>, crate::queue::error::Error> {
        let mut conn = self.redis.get()?;

        let min_idle_time = options.min_idle_time.as_millis() as u64;

        let pending : Vec<(String, String, u64, u64)> = redis::cmd("XPENDING")
            .arg(&self.stream_key)
            .arg(&self.queue_name)
            .arg("-")
            .arg("+")
            .arg(options.count)
            .query(&mut conn)?;


        let drop = pending
            .into_iter()
            .filter(|(_, _, idle, deliveries)| *idle > min_idle_time && *deliveries > options.max_deliveries)
            .map(|(id, _, idle, deliveries)| DroppedItem { id, idle, deliveries })
            .collect::<Vec<DroppedItem>>();

        if !drop.is_empty() {
            let drop_ids : Vec<&str> = drop.iter().map(|d| d.id.as_str()).collect();
            let _ : () = conn.xack(&self.stream_key, &self.queue_name, &drop_ids)?;
        }

        Ok(drop)
    }
}
