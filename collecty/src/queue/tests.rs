use std::io::{Seek, SeekFrom, Write};

use super::*;
use crate::test_support::Scratch;

fn record(body: &[u8]) -> Record {
    Record {
        frame: body.to_vec(),
        plain_len: body.len() as u32,
    }
}

fn open(scratch: &Scratch, limits: QueueLimits) -> Queue {
    Queue::open(scratch.path(), limits).expect("a queue")
}

fn drain(queue: &Queue) -> Vec<Vec<u8>> {
    let mut records = Vec::new();
    while let Some(batch) = queue.read_batch(usize::MAX, usize::MAX).expect("a batch") {
        for record in &batch.records {
            records.push(batch.frames[record.span.clone()].to_vec());
        }
        let count = batch.len() as u64;
        queue.commit(batch.end, count).expect("a commit");
    }
    records
}

#[test]
fn a_batch_returns_the_appended_frames_in_order() {
    let scratch = Scratch::new("order");
    let queue = open(&scratch, QueueLimits::default());

    for index in 0..5u8 {
        queue.append(&record(&[index; 40])).expect("an append");
    }

    let batch = queue
        .read_batch(usize::MAX, usize::MAX)
        .expect("a batch")
        .expect("records");
    assert_eq!(batch.len(), 5);
    for (index, record) in batch.records.iter().enumerate() {
        assert_eq!(batch.frames[record.span.clone()], [index as u8; 40]);
    }
}

#[test]
fn a_committed_batch_is_not_returned_again() {
    let scratch = Scratch::new("commit");
    let queue = open(&scratch, QueueLimits::default());
    queue.append(&record(b"only")).expect("an append");

    assert_eq!(drain(&queue), vec![b"only".to_vec()]);
    assert!(
        queue
            .read_batch(usize::MAX, usize::MAX)
            .expect("a batch")
            .is_none()
    );
    assert!(!queue.has_records());
}

#[test]
fn a_batch_stops_at_the_uncompressed_ceiling_but_never_returns_nothing() {
    let scratch = Scratch::new("ceiling");
    let queue = open(&scratch, QueueLimits::default());
    for _ in 0..4 {
        queue
            .append(&Record {
                frame: vec![7u8; 10],
                plain_len: 100,
            })
            .expect("an append");
    }

    let batch = queue
        .read_batch(250, usize::MAX)
        .expect("a batch")
        .expect("records");
    assert_eq!(batch.len(), 2);
    assert_eq!(batch.plain_bytes, 200);
    queue.commit(batch.end, 2).expect("a commit");

    let batch = queue
        .read_batch(1, usize::MAX)
        .expect("a batch")
        .expect("records");
    assert_eq!(batch.len(), 1);
    assert_eq!(batch.plain_bytes, 100);
}

#[test]
fn a_batch_stops_at_the_record_ceiling() {
    let scratch = Scratch::new("records");
    let queue = open(&scratch, QueueLimits::default());
    for _ in 0..10 {
        queue.append(&record(b"line")).expect("an append");
    }

    let batch = queue
        .read_batch(usize::MAX, 3)
        .expect("a batch")
        .expect("records");
    assert_eq!(batch.len(), 3);
}

#[test]
fn a_batch_crosses_a_segment_boundary() {
    let scratch = Scratch::new("boundary");
    let queue = open(
        &scratch,
        QueueLimits {
            max_bytes: 1 << 20,
            max_segment_bytes: 64,
        },
    );
    for index in 0..6u8 {
        queue.append(&record(&[index; 30])).expect("an append");
    }

    assert!(queue.stats().segments > 1);
    let frames = drain(&queue);
    assert_eq!(frames.len(), 6);
    for (index, frame) in frames.iter().enumerate() {
        assert_eq!(frame.as_slice(), [index as u8; 30]);
    }
}

#[test]
fn a_reopened_queue_keeps_its_records_and_its_cursor() {
    let scratch = Scratch::new("reopen");
    {
        let queue = open(&scratch, QueueLimits::default());
        queue.append(&record(b"first")).expect("an append");
        queue.append(&record(b"second")).expect("an append");
        let batch = queue
            .read_batch(usize::MAX, 1)
            .expect("a batch")
            .expect("records");
        queue.commit(batch.end, 1).expect("a commit");
        queue.sync().expect("a sync");
    }

    let queue = open(&scratch, QueueLimits::default());
    assert_eq!(drain(&queue), vec![b"second".to_vec()]);
}

#[test]
fn a_torn_tail_is_truncated_when_the_queue_reopens() {
    let scratch = Scratch::new("torn");
    {
        let queue = open(&scratch, QueueLimits::default());
        queue.append(&record(b"whole record")).expect("an append");
        queue.sync().expect("a sync");
    }

    let segment = scratch.path().join(format!("{:020}.seg", 0));
    let before = std::fs::metadata(&segment).expect("a segment").len();
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&segment)
        .expect("the segment");
    file.write_all(&[9u8; 7]).expect("a torn write");
    drop(file);

    let queue = open(&scratch, QueueLimits::default());
    assert_eq!(drain(&queue), vec![b"whole record".to_vec()]);
    assert_eq!(
        std::fs::metadata(&segment).expect("a segment").len(),
        before
    );
}

#[test]
fn a_corrupt_record_ends_the_segment_rather_than_being_returned() {
    let scratch = Scratch::new("corrupt");
    {
        let queue = open(&scratch, QueueLimits::default());
        queue.append(&record(b"good one")).expect("an append");
        queue.append(&record(b"bad one!")).expect("an append");
        queue.append(&record(b"after it")).expect("an append");
        queue.sync().expect("a sync");
    }

    let segment = scratch.path().join(format!("{:020}.seg", 0));
    let second_frame_at = (RECORD_HEADER_BYTES + 8 + RECORD_HEADER_BYTES) as u64;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(&segment)
        .expect("the segment");
    file.seek(SeekFrom::Start(second_frame_at)).expect("a seek");
    file.write_all(b"X").expect("a flipped byte");
    drop(file);

    let queue = open(&scratch, QueueLimits::default());
    assert_eq!(drain(&queue), vec![b"good one".to_vec()]);
}

#[test]
fn the_oldest_segment_is_dropped_when_the_queue_is_full() {
    let scratch = Scratch::new("drop");
    let queue = open(
        &scratch,
        QueueLimits {
            max_bytes: 200,
            max_segment_bytes: 60,
        },
    );
    for index in 0..12u8 {
        queue.append(&record(&[index; 30])).expect("an append");
    }

    let stats = queue.stats();
    assert!(stats.dropped_segments > 0);
    assert!(stats.dropped_bytes > 0);
    assert!(stats.queued_bytes <= 200);

    let frames = drain(&queue);
    assert!(!frames.is_empty());
    assert_eq!(frames.last().expect("a frame").as_slice(), [11u8; 30]);
}

#[test]
fn a_record_larger_than_the_whole_queue_is_refused() {
    let scratch = Scratch::new("oversized");
    let queue = open(
        &scratch,
        QueueLimits {
            max_bytes: 64,
            max_segment_bytes: 64,
        },
    );
    let error = queue.append(&record(&[0u8; 100])).expect_err("a refusal");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
fn a_corrupt_cursor_replays_from_the_oldest_record() {
    let scratch = Scratch::new("cursor");
    {
        let queue = open(&scratch, QueueLimits::default());
        queue.append(&record(b"first")).expect("an append");
        queue.append(&record(b"second")).expect("an append");
        let batch = queue
            .read_batch(usize::MAX, usize::MAX)
            .expect("a batch")
            .expect("records");
        queue.commit(batch.end, 2).expect("a commit");
        queue.sync().expect("a sync");
    }

    std::fs::write(scratch.path().join("cursor"), b"junk").expect("a corrupt cursor");

    let queue = open(&scratch, QueueLimits::default());
    assert_eq!(drain(&queue), vec![b"first".to_vec(), b"second".to_vec()]);
}

#[tokio::test]
async fn a_waiter_wakes_on_the_next_append() {
    let scratch = Scratch::new("wait");
    let queue = std::sync::Arc::new(open(&scratch, QueueLimits::default()));
    let waiter = queue.clone();
    let handle = tokio::spawn(async move { waiter.wait_for_records().await });

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    queue.append(&record(b"late")).expect("an append");

    tokio::time::timeout(std::time::Duration::from_secs(1), handle)
        .await
        .expect("the waiter woke")
        .expect("the task finished");
}

#[test]
fn the_backlog_counts_what_is_unsent_rather_than_what_is_on_disk() {
    let scratch = Scratch::new("backlog");
    let queue = open(&scratch, QueueLimits::default());
    for index in 0..4u8 {
        queue.append(&record(&[index; 40])).expect("an append");
    }

    let full = queue.stats();
    assert_eq!(full.backlog_bytes, full.queued_bytes);

    let batch = queue
        .read_batch(usize::MAX, 2)
        .expect("a batch")
        .expect("records");
    queue.commit(batch.end, 2).expect("a commit");

    let half = queue.stats();
    assert_eq!(half.queued_bytes, full.queued_bytes);
    assert_eq!(half.backlog_bytes, full.backlog_bytes / 2);

    drain(&queue);
    let empty = queue.stats();
    assert_eq!(empty.backlog_bytes, 0);
    assert_eq!(empty.queued_bytes, full.queued_bytes);
}
