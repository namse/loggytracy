use std::io::{Seek, SeekFrom, Write};

use super::*;
use crate::test_support::Scratch;

fn record(body: &[u8]) -> Record {
    Record {
        frame: body.to_vec(),
    }
}

fn open(scratch: &Scratch, limits: QueueLimits) -> Queue {
    Queue::open(scratch.path(), limits).expect("a queue")
}

/// Segments close on age as well as size, and the sender is what asks. Tests
/// that want a closed segment ask the same way, with an age of nothing.
fn eager() -> QueueLimits {
    QueueLimits {
        max_segment_age: Duration::from_nanos(1),
        ..QueueLimits::default()
    }
}

fn path_of(scratch: &Scratch, seq: u64) -> std::path::PathBuf {
    scratch.path().join(format!("{seq:020}.seg"))
}

/// Every record collecty still holds, taken segment by segment the way the
/// sender takes them.
fn drain(queue: &Queue) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    queue.seal_if_due().expect("a seal");
    while let Some(seq) = queue.oldest_sealed() {
        let sealed = queue.read_segment(seq).expect("a segment");
        let mut at = 0;
        // The wire carries frames back to back with nothing between them, so a
        // test that knows their lengths is what tells them apart again.
        while at < sealed.frames.len() {
            frames.push(sealed.frames[at..].to_vec());
            at = sealed.frames.len();
        }
        queue.commit(seq, sealed.records).expect("a commit");
        queue.seal_if_due().expect("a seal");
    }
    frames
}

#[test]
fn a_closed_segment_carries_its_records_frames_concatenated() {
    let scratch = Scratch::new("frames");
    let queue = open(&scratch, eager());
    for index in 0..5u8 {
        queue.append(&record(&[index; 40])).expect("an append");
    }

    queue.seal_if_due().expect("a seal");
    let seq = queue.oldest_sealed().expect("a closed segment");
    let sealed = queue.read_segment(seq).expect("a segment");

    assert_eq!(sealed.records, 5);
    assert_eq!(sealed.frames.len(), 5 * 40);
    for index in 0..5usize {
        assert_eq!(&sealed.frames[index * 40..(index + 1) * 40], &[index as u8; 40]);
    }
}

/// The open segment is being appended to, so it is never offered. This is what
/// removes the one place a reader and a writer shared a file.
#[test]
fn the_open_segment_is_not_offered() {
    let scratch = Scratch::new("open");
    let queue = open(&scratch, QueueLimits::default());
    queue.append(&record(b"still being written")).expect("an append");

    assert!(queue.oldest_sealed().is_none());
    assert!(!queue.has_sealed());
}

#[test]
fn an_open_segment_closes_once_it_is_old_enough() {
    let scratch = Scratch::new("age");
    let queue = open(
        &scratch,
        QueueLimits {
            max_segment_age: Duration::from_millis(30),
            ..QueueLimits::default()
        },
    );
    queue.append(&record(b"waiting")).expect("an append");

    queue.seal_if_due().expect("a seal");
    assert!(queue.oldest_sealed().is_none(), "not old enough yet");

    std::thread::sleep(Duration::from_millis(40));
    queue.seal_if_due().expect("a seal");
    assert_eq!(queue.oldest_sealed(), Some(1));
}

/// An empty queue must not roll segments forever while nothing arrives.
#[test]
fn an_empty_segment_is_never_closed() {
    let scratch = Scratch::new("idle");
    let queue = open(&scratch, eager());

    for _ in 0..5 {
        queue.seal_if_due().expect("a seal");
    }

    assert_eq!(queue.stats().segments, 1);
    assert!(queue.oldest_sealed().is_none());
}

#[test]
fn an_answered_segment_is_removed_and_not_offered_again() {
    let scratch = Scratch::new("commit");
    let queue = open(&scratch, eager());
    queue.append(&record(b"first")).expect("an append");
    queue.seal_if_due().expect("a seal");
    queue.append(&record(b"second")).expect("an append");
    queue.seal_if_due().expect("a seal");

    let seq = queue.oldest_sealed().expect("a closed segment");
    assert_eq!(seq, 1);
    queue.commit(seq, 1).expect("a commit");

    assert!(!path_of(&scratch, 1).exists(), "the file is unlinked");
    assert_eq!(queue.oldest_sealed(), Some(2));
}

#[test]
fn a_reopened_queue_keeps_what_signy_has_not_answered_for() {
    let scratch = Scratch::new("reopen");
    let sender = {
        let queue = open(&scratch, eager());
        queue.append(&record(b"answered")).expect("an append");
        queue.seal_if_due().expect("a seal");
        queue.append(&record(b"still owed")).expect("an append");
        queue.seal_if_due().expect("a seal");
        queue.commit(1, 1).expect("a commit");
        queue.sync().expect("a sync");
        queue.sender_id()
    };

    let queue = open(&scratch, eager());
    assert_eq!(queue.sender_id(), sender, "the id outlives the process");
    assert_eq!(drain(&queue), vec![b"still owed".to_vec()]);
}

/// Nothing is written down about how far signy has got: an answered segment is
/// unlinked, so what is on disk is what is still owed. A crash between the
/// answer and the unlink leaves the file behind, and it is simply offered
/// again — signy answers that one without reading it.
#[test]
fn a_segment_left_behind_by_a_crash_is_offered_again() {
    let scratch = Scratch::new("stale");
    {
        let queue = open(&scratch, eager());
        queue.append(&record(b"answered")).expect("an append");
        queue.seal_if_due().expect("a seal");
        queue.append(&record(b"owed")).expect("an append");
        queue.sync().expect("a sync");
    }
    assert!(path_of(&scratch, 1).exists());

    let queue = open(&scratch, eager());
    assert_eq!(queue.oldest_sealed(), Some(1));
    assert_eq!(drain(&queue), vec![b"answered".to_vec(), b"owed".to_vec()]);
}

#[test]
fn a_torn_tail_is_truncated_when_the_queue_reopens() {
    let scratch = Scratch::new("torn");
    {
        let queue = open(&scratch, QueueLimits::default());
        queue.append(&record(b"whole record")).expect("an append");
        queue.sync().expect("a sync");
    }

    let segment = path_of(&scratch, 1);
    let before = std::fs::metadata(&segment).expect("a segment").len();
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&segment)
        .expect("the segment");
    file.write_all(&[9u8; 7]).expect("a torn write");
    drop(file);

    let queue = open(&scratch, eager());
    assert_eq!(
        std::fs::metadata(&segment).expect("a segment").len(),
        before,
        "the torn bytes are gone before anything reads the file"
    );
    assert_eq!(drain(&queue), vec![b"whole record".to_vec()]);
}

#[test]
fn a_corrupt_record_ends_the_segment_rather_than_being_sent() {
    let scratch = Scratch::new("corrupt");
    {
        let queue = open(&scratch, QueueLimits::default());
        queue.append(&record(b"good one")).expect("an append");
        queue.append(&record(b"bad one!")).expect("an append");
        queue.append(&record(b"after it")).expect("an append");
        queue.sync().expect("a sync");
    }

    let segment = path_of(&scratch, 1);
    let second_frame_at = (RECORD_HEADER_BYTES + 8 + RECORD_HEADER_BYTES) as u64;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(&segment)
        .expect("the segment");
    file.seek(SeekFrom::Start(second_frame_at)).expect("a seek");
    file.write_all(b"X").expect("a flipped byte");
    drop(file);

    let queue = open(&scratch, eager());
    queue.seal_if_due().expect("a seal");
    let seq = queue.oldest_sealed().expect("a closed segment");
    let sealed = queue.read_segment(seq).expect("a segment");
    assert_eq!(sealed.records, 1);
    assert_eq!(sealed.frames, b"good one".to_vec());
}

#[test]
fn the_oldest_segment_is_dropped_when_the_queue_is_full() {
    let scratch = Scratch::new("drop");
    let queue = open(
        &scratch,
        QueueLimits {
            max_bytes: 200,
            max_segment_bytes: 60,
            ..eager()
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
            ..QueueLimits::default()
        },
    );
    let error = queue.append(&record(&[0u8; 100])).expect_err("a refusal");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}

/// A name that cannot be read is replaced rather than repaired. Keeping it
/// while the segments went back to one would have signy skip every segment
/// under the high-water mark it still held for that name.
#[test]
fn an_unreadable_identity_is_replaced() {
    let scratch = Scratch::new("identity");
    let before = {
        let queue = open(&scratch, eager());
        queue.append(&record(b"first")).expect("an append");
        queue.seal_if_due().expect("a seal");
        queue.commit(1, 1).expect("a commit");
        queue.sync().expect("a sync");
        queue.sender_id()
    };

    std::fs::write(scratch.path().join("identity"), b"junk").expect("a damaged identity");

    let queue = open(&scratch, eager());
    assert_ne!(queue.sender_id(), before);
}

#[tokio::test]
async fn a_waiter_wakes_when_a_segment_closes() {
    let scratch = Scratch::new("wait");
    let queue = Arc::new(open(&scratch, eager()));
    let waiting = queue.clone();
    let waiter = tokio::spawn(async move { waiting.wait_for_sealed().await });

    tokio::time::sleep(Duration::from_millis(20)).await;
    queue.append(&record(b"arrived")).expect("an append");
    queue.seal_if_due().expect("a seal");

    tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("the waiter woke")
        .expect("the task finished");
}

/// What the queue occupies is what it still owes: an answered segment is gone
/// from disk, so there is no second number to keep.
#[test]
fn an_answered_segment_leaves_the_queue_smaller() {
    let scratch = Scratch::new("backlog");
    let queue = open(&scratch, eager());
    queue.append(&record(&[1u8; 100])).expect("an append");
    queue.seal_if_due().expect("a seal");
    queue.append(&record(&[2u8; 100])).expect("an append");
    queue.seal_if_due().expect("a seal");

    let before = queue.stats().queued_bytes;
    queue.commit(1, 1).expect("a commit");

    assert!(queue.stats().queued_bytes < before);
}

use std::sync::Arc;
