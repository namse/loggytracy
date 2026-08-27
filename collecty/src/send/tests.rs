use std::net::SocketAddr;

use bytes::Bytes;
use parking_lot::Mutex;

use super::*;
use crate::queue::{Queue, QueueLimits, Record};
use crate::test_support::Scratch;

struct Scripted {
    respond: Box<dyn Fn(&Bytes) -> Outcome + Send + Sync>,
    segments: Mutex<Vec<u64>>,
    bytes: Mutex<Vec<usize>>,
}

impl Scripted {
    fn new(respond: impl Fn(&Bytes) -> Outcome + Send + Sync + 'static) -> Arc<Scripted> {
        Arc::new(Scripted {
            respond: Box::new(respond),
            segments: Mutex::new(Vec::new()),
            bytes: Mutex::new(Vec::new()),
        })
    }

    fn segments(&self) -> Vec<u64> {
        self.segments.lock().clone()
    }

    fn bytes(&self) -> Vec<usize> {
        self.bytes.lock().clone()
    }
}

impl Transport for Scripted {
    fn deliver<'a>(&'a self, shipment: Shipment) -> DeliverFuture<'a> {
        Box::pin(async move {
            self.segments.lock().push(shipment.segment);
            self.bytes.lock().push(shipment.frames.len());
            (self.respond)(&shipment.frames)
        })
    }
}

fn shipment(frames: &'static [u8]) -> Shipment {
    Shipment {
        frames: Bytes::from_static(frames),
        sender: crate::queue::SenderId::generate().expect("an id"),
        segment: 1,
    }
}

/// The sender closes the open segment itself, so a queue under test closes on
/// the first ask.
fn eager() -> QueueLimits {
    QueueLimits {
        max_segment_age: Duration::from_nanos(1),
        ..QueueLimits::default()
    }
}

/// One segment per record, so a test can count segments and records together.
fn queue_with(scratch: &Scratch, bodies: &[Vec<u8>]) -> Arc<Queue> {
    let queue = Arc::new(Queue::open(scratch.path(), eager()).expect("a queue"));
    for body in bodies {
        queue
            .append(&Record {
                frame: body.clone(),
                plain_len: body.len() as u32,
            })
            .expect("an append");
        queue.seal_if_due().expect("a seal");
    }
    queue
}

fn frames(count: usize) -> Vec<Vec<u8>> {
    (0..count).map(|index| vec![index as u8; 32]).collect()
}

async fn deliver_all<T: Transport>(sender: &Sender<T>, queue: &Queue) {
    let (_tx, mut rx) = watch::channel(false);
    queue.seal_if_due().expect("a seal");
    while let Some(seq) = queue.oldest_sealed() {
        let segment = queue.read_segment(seq).expect("a segment");
        sender.deliver(segment, &mut rx).await;
        queue.seal_if_due().expect("a seal");
    }
}

#[tokio::test]
async fn a_delivered_segment_advances_the_cursor_and_unlinks_the_file() {
    let scratch = Scratch::new("send-ok");
    let queue = queue_with(&scratch, &frames(3));
    let transport = Scripted::new(|_| Outcome::Accepted(0));
    let sender = Sender::new(queue.clone(), transport.clone(), SenderConfig::default());

    deliver_all(&sender, &queue).await;

    assert_eq!(transport.segments(), vec![1, 2, 3]);
    assert_eq!(sender.stats().sent_records.load(Ordering::Relaxed), 3);
    assert_eq!(sender.stats().sent_segments.load(Ordering::Relaxed), 3);
    assert!(!queue.has_sealed());
}

/// A segment is sent from its first record every time. Nothing is remembered
/// about how far into one an earlier attempt reached — signy remembers that.
#[tokio::test]
async fn a_retried_segment_is_sent_whole_again() {
    let scratch = Scratch::new("send-retry");
    let queue = queue_with(&scratch, &frames(1));
    let attempts = Arc::new(Mutex::new(0usize));
    let counter = attempts.clone();
    let transport = Scripted::new(move |_| {
        let mut seen = counter.lock();
        *seen += 1;
        if *seen < 3 {
            Outcome::Retry("signy is not up".to_string())
        } else {
            Outcome::Accepted(0)
        }
    });
    let sender = Sender::new(queue.clone(), transport.clone(), SenderConfig::default());

    tokio::time::pause();
    deliver_all(&sender, &queue).await;

    assert_eq!(transport.segments(), vec![1, 1, 1]);
    assert_eq!(transport.bytes(), vec![32, 32, 32], "the same bytes each time");
    assert_eq!(sender.stats().retries.load(Ordering::Relaxed), 2);
    assert!(!queue.has_sealed());
}

/// Halving is gone with the batch. signy drops a record it cannot decode on
/// its own side, so a refusal that reaches here is the segment's shape and no
/// amount of splitting fixes it.
#[tokio::test]
async fn a_refused_segment_is_dropped_whole_and_counted() {
    let scratch = Scratch::new("send-refused");
    let queue = queue_with(&scratch, &frames(2));
    let transport = Scripted::new(|frames: &Bytes| {
        if frames[0] == 0 {
            Outcome::Refused("signy cannot read this".to_string())
        } else {
            Outcome::Accepted(0)
        }
    });
    let sender = Sender::new(queue.clone(), transport.clone(), SenderConfig::default());

    deliver_all(&sender, &queue).await;

    assert_eq!(transport.segments(), vec![1, 2]);
    assert_eq!(sender.stats().refused_records.load(Ordering::Relaxed), 1);
    assert_eq!(sender.stats().sent_records.load(Ordering::Relaxed), 1);
    assert!(!queue.has_sealed());
}

#[tokio::test(start_paused = true)]
async fn shutdown_stops_a_retry_loop_without_advancing_the_cursor() {
    let scratch = Scratch::new("send-stop");
    let queue = queue_with(&scratch, &frames(1));
    let transport = Scripted::new(|_| Outcome::Retry("signy is down".to_string()));
    let sender = Sender::new(queue.clone(), transport.clone(), SenderConfig::default());

    let (tx, mut rx) = watch::channel(false);
    let seq = queue.oldest_sealed().expect("a closed segment");
    let segment = queue.read_segment(seq).expect("a segment");
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let _ = tx.send(true);
    });
    sender.deliver(segment, &mut rx).await;

    assert_eq!(queue.oldest_sealed(), Some(1), "the segment is still owed");
    assert_eq!(sender.stats().sent_records.load(Ordering::Relaxed), 0);
}

#[tokio::test(start_paused = true)]
async fn the_run_loop_closes_the_open_segment_and_ships_it() {
    let scratch = Scratch::new("send-run");
    let queue = Arc::new(Queue::open(scratch.path(), eager()).expect("a queue"));
    for body in frames(3) {
        queue
            .append(&Record {
                plain_len: body.len() as u32,
                frame: body,
            })
            .expect("an append");
    }
    let transport = Scripted::new(|_| Outcome::Accepted(0));
    let sender = Sender::new(queue.clone(), transport.clone(), SenderConfig::default());

    let (tx, rx) = watch::channel(false);
    let stop = async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let _ = tx.send(true);
    };
    tokio::join!(sender.run(rx), stop);

    assert_eq!(sender.stats().sent_records.load(Ordering::Relaxed), 3);
    assert!(!queue.has_sealed());
}

/// signy can be ahead: it answered an attempt collecty never heard. Everything
/// up to what it names goes at once.
#[tokio::test]
async fn an_answer_beyond_the_segment_clears_everything_under_it() {
    let scratch = Scratch::new("send-ahead");
    let queue = queue_with(&scratch, &frames(3));
    let transport = Scripted::new(|_| Outcome::Accepted(3));
    let sender = Sender::new(queue.clone(), transport.clone(), SenderConfig::default());

    deliver_all(&sender, &queue).await;

    assert_eq!(transport.segments(), vec![1], "the rest were already stored");
    assert!(!queue.has_sealed(), "segments two and three went with the answer");
}

type SeenRequest = (String, String, String, String);

async fn fake_signy(status: http::StatusCode, seen: Arc<Mutex<Vec<SeenRequest>>>) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a bound port");
    let address = listener.local_addr().expect("an address");

    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let seen = seen.clone();
            tokio::spawn(async move {
                let service = hyper::service::service_fn(
                    move |request: http::Request<hyper::body::Incoming>| {
                        let seen = seen.clone();
                        async move {
                            let header = |name: &str| {
                                request
                                    .headers()
                                    .get(name)
                                    .and_then(|value| value.to_str().ok())
                                    .unwrap_or("")
                                    .to_string()
                            };
                            seen.lock().push((
                                request.uri().path().to_string(),
                                header("content-encoding"),
                                header(super::transport::SENDER_HEADER),
                                header(super::transport::SEGMENT_HEADER),
                            ));
                            http::Response::builder()
                                .status(status)
                                .body(http_body_util::Full::new(Bytes::from_static(
                                    br#"{"stored":42}"#,
                                )))
                        }
                    },
                );
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                    .await;
            });
        }
    });

    address
}

#[tokio::test]
async fn a_success_over_http_carries_the_encoding_and_who_sent_which_segment() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let address = fake_signy(http::StatusCode::OK, seen.clone()).await;
    let transport = HttpTransport::new(format!("http://{address}"), Duration::from_secs(5));
    let mut shipment = shipment(b"frames");
    shipment.segment = 7;
    let sender = shipment.sender.to_string();

    let outcome = transport.deliver(shipment).await;

    assert_eq!(
        outcome,
        Outcome::Accepted(42),
        "the answer says how far this sender got"
    );
    assert_eq!(
        seen.lock().as_slice(),
        [(
            "/signy/api/v1/collect".to_string(),
            "zstd".to_string(),
            sender,
            "7".to_string()
        )]
    );
}

#[tokio::test]
async fn a_bad_request_refuses_the_payload_and_a_server_error_asks_for_a_retry() {
    let refusing = fake_signy(
        http::StatusCode::BAD_REQUEST,
        Arc::new(Mutex::new(Vec::new())),
    )
    .await;
    let transport = HttpTransport::new(format!("http://{refusing}"), Duration::from_secs(5));
    let outcome = transport.deliver(shipment(b"frames")).await;
    assert!(matches!(outcome, Outcome::Refused(_)), "{outcome:?}");

    let unavailable = fake_signy(
        http::StatusCode::SERVICE_UNAVAILABLE,
        Arc::new(Mutex::new(Vec::new())),
    )
    .await;
    let transport = HttpTransport::new(format!("http://{unavailable}"), Duration::from_secs(5));
    let outcome = transport.deliver(shipment(b"frames")).await;
    assert!(matches!(outcome, Outcome::Retry(_)), "{outcome:?}");
}

#[tokio::test]
async fn a_refused_connection_asks_for_a_retry_rather_than_dropping() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a bound port");
    let address = listener.local_addr().expect("an address");
    drop(listener);

    let transport = HttpTransport::new(format!("http://{address}"), Duration::from_secs(5));
    let outcome = transport.deliver(shipment(b"frames")).await;

    assert!(matches!(outcome, Outcome::Retry(_)), "{outcome:?}");
}
