mod util;

use rdq::queue::{DropOptions, JsonItem};
use rdq::queue::stream::AutoclaimOptions;

#[test]
fn enqueue_dequeue() {
    let timeout = std::time::Duration::from_millis(1);
    let mut queue = util::create_stream_queue::<JsonItem<i32>>(None);

    queue.enqueue(&JsonItem::new(123)).unwrap();
    let dequeued : Vec<i32> = queue.dequeue(1, timeout).unwrap().into_iter().map(|i| i.item).collect();
    assert_eq!(dequeued, vec![123]);
}

#[test]
fn batch_dequeue() {
    let timeout = std::time::Duration::from_millis(1);
    let mut queue = util::create_stream_queue::<JsonItem<i32>>(None);

    util::enqueue_all(
        &queue,
        vec![
            JsonItem::new(1),
            JsonItem::new(2),
            JsonItem::new(3),
            JsonItem::new(4),
            JsonItem::new(5)
        ]
    );

    let dequeued : Vec<i32> = queue.dequeue(2, timeout).unwrap().into_iter().map(|i| i.item).collect();
    assert_eq!(dequeued, vec![1, 2]);

    let dequeued : Vec<i32> = queue.dequeue(2, timeout).unwrap().into_iter().map(|i| i.item).collect();
    assert_eq!(dequeued, vec![3, 4]);

    let dequeued : Vec<i32> = queue.dequeue(2, timeout).unwrap().into_iter().map(|i| i.item).collect();
    assert_eq!(dequeued, vec![5]);

    let dequeued : Vec<i32> = queue.dequeue(2, timeout).unwrap().into_iter().map(|i| i.item).collect();
    assert_eq!(dequeued.is_empty(), true);
}

#[test]
fn autoclaim_frequency() {
    let timeout = std::time::Duration::from_millis(1);

    let autoclaim_options = AutoclaimOptions {
        frequency: 2,
        min_idle_time: std::time::Duration::from_millis(0)
    };

    let mut queue = util::create_stream_queue::<JsonItem<i32>>(Some(autoclaim_options));

    util::enqueue_all(
        &queue,
        vec![
            JsonItem::new(1),
            JsonItem::new(2),
            JsonItem::new(3),
            JsonItem::new(4),
            JsonItem::new(5)
        ]
    );

    // Dequeued but not acked, will be autoclaimed
    let dequeued : Vec<i32> = queue.dequeue(2, timeout).unwrap().into_iter().map(|i| i.item).collect();
    assert_eq!(dequeued, vec![1, 2]);

    let dequeued = queue.dequeue(3, timeout).unwrap();
    queue.ack(&dequeued.iter().collect()).unwrap();
    let dequeued : Vec<i32> = dequeued.into_iter().map(|i| i.item).collect();
    assert_eq!(dequeued, vec![3, 4, 5]);

    std::thread::sleep(std::time::Duration::from_millis(10));

    let dequeued : Vec<i32> = queue.dequeue(3, timeout).unwrap().into_iter().map(|i| i.item).collect();
    assert_eq!(dequeued, vec![1, 2]);
}

#[test]
fn autoclaim_idle_time() {
    let timeout = std::time::Duration::from_millis(1);

    let autoclaim_options = AutoclaimOptions {
        frequency: 4,
        min_idle_time: std::time::Duration::from_millis(100)
    };

    let mut queue = util::create_stream_queue::<JsonItem<i32>>(Some(autoclaim_options));

    util::enqueue_all(
        &queue,
        vec![
            JsonItem::new(1),
            JsonItem::new(2),
            JsonItem::new(3),
            JsonItem::new(4),
            JsonItem::new(5)
        ]
    );

    // Dequeued but not acked, will be autoclaimed
    // Dequeue #1 is a read
    let dequeued : Vec<i32> = queue.dequeue(2, timeout).unwrap().into_iter().map(|i| i.item).collect();
    assert_eq!(dequeued, vec![1, 2]);

    // Dequeue #2 is a read
    let dequeued = queue.dequeue(3, timeout).unwrap();
    queue.ack(&dequeued.iter().collect()).unwrap();
    let dequeued : Vec<i32> = dequeued.into_iter().map(|i| i.item).collect();
    assert_eq!(dequeued, vec![3, 4, 5]);

    std::thread::sleep(std::time::Duration::from_millis(10));

    // Dequeue #3 is a read
    let dequeued : Vec<i32> = queue.dequeue(3, timeout).unwrap().into_iter().map(|i| i.item).collect();
    assert_eq!(dequeued.is_empty(), true);

    std::thread::sleep(std::time::Duration::from_millis(100));

    // Dequeue #4 is a read
    let dequeued : Vec<i32> = queue.dequeue(3, timeout).unwrap().into_iter().map(|i| i.item).collect();
    assert_eq!(dequeued.is_empty(), true);

    // Dequeue #5 is an autoclaim
    let dequeued : Vec<i32> = queue.dequeue(3, timeout).unwrap().into_iter().map(|i| i.item).collect();
    assert_eq!(dequeued, vec![1, 2]);
}

#[test]
fn drop_items() {
    let timeout = std::time::Duration::from_millis(1);
    let mut queue = util::create_stream_queue::<JsonItem<i32>>(None);

    util::enqueue_all(
        &queue,
        vec![
            JsonItem::new(1),
            JsonItem::new(2),
            JsonItem::new(3),
            JsonItem::new(4),
            JsonItem::new(5)
        ]
    );

    // Dequeued but not acked, will be dropped
    let dequeued = queue.dequeue(3, timeout).unwrap();
    let dequeued_items : Vec<i32> = dequeued.iter().map(|i| i.item.clone()).collect();
    assert_eq!(dequeued_items, vec![1, 2, 3]);

    // Dequeued and acked, not in the stream at drop time
    let dequeued2 = queue.dequeue(2, timeout).unwrap();
    queue.ack(&dequeued2.iter().collect()).unwrap();
    let dequeued2 : Vec<i32> = dequeued2.into_iter().map(|i| i.item).collect();
    assert_eq!(dequeued2, vec![4, 5]);

    let drop_options = DropOptions {
        min_idle_time: std::time::Duration::from_millis(50),
        max_deliveries: 1,
        count: 2
    };

    // Nothing will be dropped because no pending items exceed min idle time
    let dropped = queue.drop_items(&drop_options).unwrap();
    assert_eq!(dropped.is_empty(), true);

    std::thread::sleep(std::time::Duration::from_millis(100));

    // First two enqueued items will be dropped
    let dropped = queue.drop_items(&drop_options).unwrap();
    let dropped_ids : Vec<String> = dropped.into_iter().map(|i| i.id).collect();
    let dequeued_ids : Vec<String> = dequeued.iter().take(2).map(|i| i.id.clone().unwrap()).collect();
    assert_eq!(dropped_ids, dequeued_ids);

    let drop_options = DropOptions {
        min_idle_time: std::time::Duration::from_millis(50),
        max_deliveries: 2,
        count: 2
    };

    // Nothing will be dropped because nothing is at max deliveries
    let dropped = queue.drop_items(&drop_options).unwrap();
    assert_eq!(dropped.is_empty(), true);

    let drop_options = DropOptions {
        min_idle_time: std::time::Duration::from_millis(50),
        max_deliveries: 1,
        count: 2
    };

    // Third enqueued item will be dropped because it is at max deliveries
    let dropped = queue.drop_items(&drop_options).unwrap();
    let dropped_ids : Vec<String> = dropped.into_iter().map(|i| i.id).collect();
    let dequeued_ids : Vec<String> = dequeued.iter().skip(2).map(|i| i.id.clone().unwrap()).collect();
     assert_eq!(dropped_ids, dequeued_ids);
}
