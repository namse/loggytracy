use std::io::Write;
use std::sync::Arc;

use super::*;
use crate::signal::Signal;
use crate::test_support::Scratch;
use crate::wire::{self, ZSTD_LEVEL};

fn record(body: &[u8]) -> Record {
    Record {
        plain: wire::frame_record(Signal::Logs, body),
    }
}

fn open(scratch: &Scratch, limits: QueueLimits) -> Queue {
    Queue::open(scratch.path(), limits, ZSTD_LEVEL).expect("a queue")
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

/// A closed segment's records, as signy would read them: one zstd stream, and
/// behind it the records back to back.
fn records_of(sealed: &SealedSegment) -> Vec<Vec<u8>> {
    let plain = wire::decompress(&sealed.body, sealed.body.len() * 8).expect("a stream");
    wire::split_records(&plain)
        .expect("framed records")
        .into_iter()
        .map(|(_, payload)| payload.to_vec())
        .collect()
}

/// Every record collecty still holds, taken segment by segment the way the
/// sender takes them.
fn drain(queue: &Queue) -> Vec<Vec<u8>> {
    let mut records = Vec::new();
    queue.seal_if_due().expect("a seal");
    while let Some(seq) = queue.oldest_sealed() {
        let sealed = queue.read_segment(seq).expect("a segment");
        records.extend(records_of(&sealed));
        queue.commit(seq).expect("a commit");
        queue.seal_if_due().expect("a seal");
    }
    records
}

/// Bytes with nothing in them to compress, for tests about what a segment
/// occupies rather than about what it saves.
fn noise(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 24) as u8
        })
        .collect()
}

fn lines(count: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|index| format!("GET /v1/checkout 200 in 31ms request_id=req-{index:08}").into_bytes())
        .collect()
}

#[test]
fn a_closed_segment_is_one_stream_over_every_record_it_took() {
    let scratch = Scratch::new("stream");
    let queue = open(&scratch, eager());
    let bodies = lines(5);
    for body in &bodies {
        queue.append(&record(body)).expect("an append");
    }

    queue.seal_if_due().expect("a seal");
    let seq = queue.oldest_sealed().expect("a closed segment");
    let sealed = queue.read_segment(seq).expect("a segment");

    assert_eq!(records_of(&sealed), bodies);
    assert_eq!(
        sealed.body,
        std::fs::read(path_of(&scratch, seq)).expect("the file"),
        "the file is the body, byte for byte"
    );
}

/// The whole point of one stream a segment: the second record costs almost
/// nothing once the first has taught the compressor what the data looks like.
#[test]
fn a_segment_compresses_across_its_records() {
    let scratch = Scratch::new("ratio");
    let queue = open(&scratch, eager());
    let bodies = lines(200);
    for body in &bodies {
        queue.append(&record(body)).expect("an append");
    }

    queue.seal_if_due().expect("a seal");
    let seq = queue.oldest_sealed().expect("a closed segment");
    let together = queue.read_segment(seq).expect("a segment").body.len();

    let apart: usize = bodies
        .iter()
        .map(|body| {
            zstd::bulk::compress(&wire::frame_record(Signal::Logs, body), ZSTD_LEVEL)
                .expect("a frame")
                .len()
        })
        .sum();

    assert!(
        together * 4 < apart,
        "one stream is {together} bytes against {apart} for a frame a record"
    );
}

/// The open segment is being appended to, so it is never offered. This is what
/// removes the one place a reader and a writer shared a file.
#[test]
fn the_open_segment_is_not_offered() {
    let scratch = Scratch::new("open");
    let queue = open(&scratch, QueueLimits::default());
    queue
        .append(&record(b"still being written"))
        .expect("an append");

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

/// An empty queue must not roll segments forever while nothing arrives. The
/// file being empty is not the test: a segment that has taken records can
/// still be an empty file while the encoder holds them.
#[test]
fn an_empty_segment_is_never_closed() {
    let scratch = Scratch::new("idle");
    let queue = open(&scratch, eager());

    for _ in 0..5 {
        queue.seal_if_due().expect("a seal");
    }

    assert_eq!(queue.stats().segments, 1);
    assert!(queue.oldest_sealed().is_none());

    queue.append(&record(b"arrived")).expect("an append");
    queue.seal_if_due().expect("a seal");
    assert_eq!(queue.oldest_sealed(), Some(1));
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
    queue.commit(seq).expect("a commit");

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
        queue.commit(1).expect("a commit");
        queue.sender_id()
    };

    let queue = open(&scratch, eager());
    assert_eq!(queue.sender_id(), sender, "the id outlives the process");
    assert_eq!(drain(&queue), vec![b"still owed".to_vec()]);
}

/// A stream cannot be resumed, so what a previous process left open is closed
/// as it is and this one starts a segment of its own.
#[test]
fn a_reopened_queue_closes_the_old_segment_and_opens_a_new_one() {
    let scratch = Scratch::new("resume");
    {
        let queue = open(&scratch, QueueLimits::default());
        queue
            .append(&record(b"before the restart"))
            .expect("an append");
        queue.seal().expect("a seal on the way out");
    }

    let queue = open(&scratch, eager());
    assert_eq!(queue.oldest_sealed(), Some(1), "the old one is sendable");
    queue.append(&record(b"after it")).expect("an append");
    assert_eq!(queue.oldest_sealed(), Some(1), "and not appended to");
    assert_eq!(queue.stats().segments, 2);

    assert_eq!(
        drain(&queue),
        vec![b"before the restart".to_vec(), b"after it".to_vec()]
    );
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
    }
    assert!(path_of(&scratch, 1).exists());

    let queue = open(&scratch, eager());
    assert_eq!(queue.oldest_sealed(), Some(1));
    assert_eq!(drain(&queue), vec![b"answered".to_vec()]);
}

/// A segment closes and syncs in one step, so a process that dies without
/// closing loses whatever the encoder was still holding — and keeps every
/// record that had already reached the file.
#[test]
fn a_crash_keeps_the_records_that_reached_the_file() {
    let scratch = Scratch::new("crash");
    // Past the encoder's block size, so some of it is written and some is not.
    let bodies = lines(4000);
    {
        let queue = open(&scratch, QueueLimits::default());
        for body in &bodies {
            queue.append(&record(body)).expect("an append");
        }
    }

    let queue = open(&scratch, eager());
    let recovered = drain(&queue);

    assert!(!recovered.is_empty(), "what reached the file is kept");
    assert!(
        recovered.len() < bodies.len(),
        "and what the encoder still held is not"
    );
    assert_eq!(recovered, bodies[..recovered.len()]);
}

/// The stream ends wherever the crash left it, which is not a record boundary.
/// Recovery cuts back to one, because a segment ending mid-record is one signy
/// refuses whole.
#[test]
fn a_torn_tail_is_cut_back_to_the_last_whole_record() {
    let scratch = Scratch::new("torn");
    let bodies = lines(4000);
    {
        let queue = open(&scratch, QueueLimits::default());
        for body in &bodies {
            queue.append(&record(body)).expect("an append");
        }
    }

    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(path_of(&scratch, 1))
        .expect("the segment");
    file.write_all(&[9u8; 7]).expect("a torn write");
    drop(file);

    let queue = open(&scratch, eager());
    let recovered = drain(&queue);

    assert!(!recovered.is_empty());
    assert_eq!(recovered, bodies[..recovered.len()]);
}

/// Closing a recovered segment rewrites it, so what the sender picks up is a
/// stream that ends properly rather than one signy would refuse.
#[test]
fn a_recovered_segment_is_closed_before_it_is_offered() {
    let scratch = Scratch::new("recovered");
    {
        let queue = open(&scratch, QueueLimits::default());
        for body in lines(4000) {
            queue.append(&record(&body)).expect("an append");
        }
    }

    let queue = open(&scratch, eager());
    let sealed = queue.read_segment(1).expect("a segment");
    wire::decompress(&sealed.body, sealed.body.len() * 8).expect("a stream that ends properly");
}

/// A segment that holds nothing readable leaves nothing behind.
#[test]
fn a_segment_that_decompresses_to_nothing_is_dropped() {
    let scratch = Scratch::new("garbage");
    {
        let queue = open(&scratch, QueueLimits::default());
        queue
            .append(&record(b"never reached the file"))
            .expect("an append");
    }
    assert_eq!(
        std::fs::metadata(path_of(&scratch, 1))
            .expect("a segment")
            .len(),
        0,
        "the encoder held all of it"
    );

    // The number is not reused. signy holds a high-water mark under this
    // sender's name, and numbering that went backwards would have it skip
    // every segment under that mark as one it already stored.
    let queue = open(&scratch, eager());
    assert!(drain(&queue).is_empty());
    queue.append(&record(b"after it")).expect("an append");
    queue.seal_if_due().expect("a seal");
    assert_eq!(queue.oldest_sealed(), Some(2));
}

#[test]
fn the_oldest_segment_is_dropped_when_the_queue_is_full() {
    let scratch = Scratch::new("drop");
    let queue = open(
        &scratch,
        QueueLimits {
            max_bytes: 4096,
            max_segment_bytes: 1024,
            ..eager()
        },
    );
    // A segment a record, so the cap has whole segments to unlink: what the
    // encoder still holds is not on disk and cannot be counted or dropped.
    // Bodies zstd cannot shrink, so the segments are the size they look.
    let bodies: Vec<Vec<u8>> = (0..40).map(|index| noise(index, 400)).collect();
    for body in &bodies {
        queue.append(&record(body)).expect("an append");
        queue.seal_if_due().expect("a seal");
    }

    let stats = queue.stats();
    assert!(stats.dropped_segments > 0);
    assert!(stats.dropped_bytes > 0);
    assert!(stats.queued_bytes <= 4096);

    let kept = drain(&queue);
    assert!(!kept.is_empty());
    assert_eq!(kept.last().expect("a record"), bodies.last().expect("one"));
}

/// Against the plain length, which is all that is known before the record has
/// been compressed.
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
        queue.commit(1).expect("a commit");
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
    queue.commit(1).expect("a commit");

    assert!(queue.stats().queued_bytes < before);
}

/// Plain bytes in, compressed bytes on disk. The two together are the ratio a
/// host is achieving, which is the reason they are counted differently.
#[test]
fn appended_bytes_are_what_arrived_and_queued_bytes_are_what_is_kept() {
    let scratch = Scratch::new("counts");
    let queue = open(&scratch, eager());
    let bodies = lines(200);
    for body in &bodies {
        queue.append(&record(body)).expect("an append");
    }
    queue.seal_if_due().expect("a seal");

    let stats = queue.stats();
    let plain: u64 = bodies
        .iter()
        .map(|body| (wire::RECORD_HEADER_BYTES + body.len()) as u64)
        .sum();
    assert_eq!(stats.appended_records, bodies.len() as u64);
    assert_eq!(stats.appended_bytes, plain);
    assert!(stats.queued_bytes < plain);
}
