use std::net::SocketAddr;

use bytes::Bytes;
use parking_lot::Mutex;

use super::*;
use crate::queue::{Queue, QueueLimits, Record};
use crate::test_support::Scratch;

const POISON: u8 = 0xFF;

struct Scripted {
    respond: Box<dyn Fn(&Bytes) -> Outcome + Send + Sync>,
    calls: Mutex<Vec<usize>>,
    sequences: Mutex<Vec<u64>>,
}

impl Scripted {
    fn new(respond: impl Fn(&Bytes) -> Outcome + Send + Sync + 'static) -> Arc<Scripted> {
        Arc::new(Scripted {
            respond: Box::new(respond),
            calls: Mutex::new(Vec::new()),
            sequences: Mutex::new(Vec::new()),
        })
    }

    fn calls(&self) -> Vec<usize> {
        self.calls.lock().clone()
    }

    fn sequences(&self) -> Vec<u64> {
        self.sequences.lock().clone()
    }
}

impl Transport for Scripted {
    fn deliver<'a>(&'a self, shipment: Shipment) -> DeliverFuture<'a> {
        Box::pin(async move {
            self.calls.lock().push(shipment.frames.len());
            self.sequences.lock().push(shipment.start_sequence);
            (self.respond)(&shipment.frames)
        })
    }
}

fn shipment(frames: &'static [u8], plain_bytes: usize) -> Shipment {
    Shipment {
        frames: Bytes::from_static(frames),
        plain_bytes,
        sender: crate::queue::SenderId::generate().expect("an id"),
        start_sequence: 1,
    }
}

fn queue_with(scratch: &Scratch, bodies: &[Vec<u8>]) -> Arc<Queue> {
    let queue = Arc::new(Queue::open(scratch.path(), QueueLimits::default()).expect("a queue"));
    for body in bodies {
        queue
            .append(&Record {
                frame: body.clone(),
                plain_len: body.len() as u32,
            })
            .expect("an append");
    }
    queue
}

fn frames(count: usize, poison_at: Option<usize>) -> Vec<Vec<u8>> {
    (0..count)
        .map(|index| {
            let fill = if Some(index) == poison_at {
                POISON
            } else {
                index as u8
            };
            vec![fill; 32]
        })
        .collect()
}

async fn deliver_all<T: Transport>(sender: &Sender<T>, queue: &Queue) {
    let (_tx, mut rx) = watch::channel(false);
    while let Some(batch) = queue.read_batch(usize::MAX, usize::MAX).expect("a batch") {
        sender.deliver(batch, &mut rx).await;
    }
}

#[tokio::test]
async fn a_delivered_batch_advances_the_cursor_in_one_call() {
    let scratch = Scratch::new("send-ok");
    let queue = queue_with(&scratch, &frames(4, None));
    let transport = Scripted::new(|_| Outcome::Accepted);
    let sender = Sender::new(queue.clone(), transport.clone(), SenderConfig::default());

    deliver_all(&sender, &queue).await;

    assert_eq!(transport.calls().len(), 1);
    assert_eq!(sender.stats().sent_records.load(Ordering::Relaxed), 4);
    assert_eq!(sender.stats().sent_batches.load(Ordering::Relaxed), 1);
    assert!(!queue.has_records());
}

#[tokio::test(start_paused = true)]
async fn a_retryable_answer_is_retried_until_it_is_accepted() {
    let scratch = Scratch::new("send-retry");
    let queue = queue_with(&scratch, &frames(2, None));
    let attempts = Arc::new(Mutex::new(0usize));
    let counter = attempts.clone();
    let transport = Scripted::new(move |_| {
        let mut seen = counter.lock();
        *seen += 1;
        if *seen < 3 {
            Outcome::Retry("signy is not up".to_string())
        } else {
            Outcome::Accepted
        }
    });
    let sender = Sender::new(queue.clone(), transport.clone(), SenderConfig::default());

    deliver_all(&sender, &queue).await;

    assert_eq!(transport.calls().len(), 3);
    assert_eq!(sender.stats().retries.load(Ordering::Relaxed), 2);
    assert_eq!(sender.stats().sent_records.load(Ordering::Relaxed), 2);
    assert!(!queue.has_records());
}

#[tokio::test]
async fn a_refused_batch_is_halved_until_the_one_bad_record_is_dropped() {
    let scratch = Scratch::new("send-poison");
    let queue = queue_with(&scratch, &frames(8, Some(5)));
    let transport = Scripted::new(|frames: &Bytes| {
        if frames.contains(&POISON) {
            Outcome::Refused("signy cannot decode this".to_string())
        } else {
            Outcome::Accepted
        }
    });
    let sender = Sender::new(queue.clone(), transport.clone(), SenderConfig::default());

    deliver_all(&sender, &queue).await;

    assert_eq!(sender.stats().sent_records.load(Ordering::Relaxed), 7);
    assert_eq!(sender.stats().refused_records.load(Ordering::Relaxed), 1);
    let halving: Vec<usize> = [8, 4, 4, 2, 1, 3, 1, 2]
        .iter()
        .map(|records| records * 32)
        .collect();
    assert_eq!(transport.calls(), halving);
    // Every attempt names the first record it carries, halved slices
    // included, so signy can number what it reads without the body saying so.
    assert_eq!(transport.sequences(), vec![1, 1, 5, 5, 5, 6, 6, 7]);
    assert!(!queue.has_records());
}

#[tokio::test(start_paused = true)]
async fn shutdown_stops_a_retry_loop_without_advancing_the_cursor() {
    let scratch = Scratch::new("send-stop");
    let queue = queue_with(&scratch, &frames(3, None));
    let transport = Scripted::new(|_| Outcome::Retry("signy is down".to_string()));
    let sender = Sender::new(queue.clone(), transport.clone(), SenderConfig::default());
    let before = queue.cursor();

    let (tx, mut rx) = watch::channel(false);
    let batch = queue
        .read_batch(usize::MAX, usize::MAX)
        .expect("a batch")
        .expect("records");
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let _ = tx.send(true);
    });
    sender.deliver(batch, &mut rx).await;

    assert_eq!(queue.cursor(), before);
    assert_eq!(sender.stats().sent_records.load(Ordering::Relaxed), 0);
    assert!(queue.has_records());
}

#[tokio::test(start_paused = true)]
async fn the_run_loop_delivers_what_arrives_and_stops_on_shutdown() {
    let scratch = Scratch::new("send-run");
    let queue = queue_with(&scratch, &frames(3, None));
    let transport = Scripted::new(|_| Outcome::Accepted);
    let sender = Sender::new(queue.clone(), transport.clone(), SenderConfig::default());

    let (tx, rx) = watch::channel(false);
    let stop = async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let _ = tx.send(true);
    };
    tokio::join!(sender.run(rx), stop);

    assert_eq!(sender.stats().sent_records.load(Ordering::Relaxed), 3);
    assert!(!queue.has_records());
}

type SeenRequest = (String, String, String, String, String);

async fn fake_signy(
    status: http::StatusCode,
    seen: Arc<Mutex<Vec<SeenRequest>>>,
) -> SocketAddr {
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
                                header(super::transport::UNCOMPRESSED_BYTES_HEADER),
                                header(super::transport::SENDER_HEADER),
                                header(super::transport::START_SEQUENCE_HEADER),
                            ));
                            http::Response::builder()
                                .status(status)
                                .body(http_body_util::Full::new(Bytes::from_static(b"refused")))
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
async fn a_success_over_http_carries_the_encoding_the_size_and_who_sent_it() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let address = fake_signy(http::StatusCode::NO_CONTENT, seen.clone()).await;
    let transport = HttpTransport::new(format!("http://{address}"), Duration::from_secs(5));
    let mut shipment = shipment(b"frames", 4096);
    shipment.start_sequence = 91;
    let sender = shipment.sender.to_string();

    let outcome = transport.deliver(shipment).await;

    assert_eq!(outcome, Outcome::Accepted);
    assert_eq!(
        seen.lock().as_slice(),
        [(
            "/signy/api/v1/collect".to_string(),
            "zstd".to_string(),
            "4096".to_string(),
            sender,
            "91".to_string()
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
    let outcome = transport.deliver(shipment(b"frames", 1)).await;
    assert!(matches!(outcome, Outcome::Refused(_)), "{outcome:?}");

    let unavailable = fake_signy(
        http::StatusCode::SERVICE_UNAVAILABLE,
        Arc::new(Mutex::new(Vec::new())),
    )
    .await;
    let transport = HttpTransport::new(format!("http://{unavailable}"), Duration::from_secs(5));
    let outcome = transport.deliver(shipment(b"frames", 1)).await;
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
    let outcome = transport.deliver(shipment(b"frames", 1)).await;

    assert!(matches!(outcome, Outcome::Retry(_)), "{outcome:?}");
}
