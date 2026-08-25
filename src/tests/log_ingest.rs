    use super::*;
    use crate::config::Config;
    use crate::journal;
    use crate::memtable::MemTable;
    use crate::tenant::test_tenant;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::resource::v1::Resource;

    fn fixture(config: Config) -> (Arc<MemTable>, LogIngestService) {
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let memtable = Arc::new(MemTable::new());
        let journal = Arc::new(journal::Journal::spawn(&config, memtable.clone()).unwrap());
        let ingest_gate = IngestGate::for_test(&journal, &config);
        let service = LogIngestService::new(
            journal,
            Arc::new(crate::shutdown::ShutdownState::new()),
            Arc::new(config.clone()),
            ingest_gate,
            crate::tenant_quota::TenantQuota::for_test(&config),
            Arc::new(crate::tenant_policy::TenantPolicy::disabled()),
            crate::clock::Clock::system(),
        );
        (memtable, service)
    }

    fn config(label: &str) -> Config {
        Config {
            data_dir: std::env::temp_dir()
                .join(format!("loggytracy-otlp-logs-{label}-{}", uuid::Uuid::new_v4())),
            ..Config::default()
        }
    }

    fn tenant_request(request: ExportLogsServiceRequest) -> Request<ExportLogsServiceRequest> {
        Request::from_parts(
            crate::tenant::test_tenant_metadata(),
            tonic::Extensions::default(),
            request,
        )
    }

    fn now_ns() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }

    fn request(records: Vec<LogRecord>) -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("checkout".to_string())),
                        }),
                        ..Default::default()
                    }],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: records,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    fn record(body: &str) -> LogRecord {
        LogRecord {
            time_unix_nano: now_ns(),
            body: Some(AnyValue {
                value: Some(any_value::Value::StringValue(body.to_string())),
            }),
            ..Default::default()
        }
    }

    /// The gap this closes: `ARCHITECTURE.md` listed OTLP as an ingest
    /// protocol while only the trace service was registered, so a collector
    /// exporting logs got `UNIMPLEMENTED`. An accepted export has to be
    /// queryable through the same path a Loki push is.
    #[tokio::test]
    async fn an_exported_record_is_queryable_like_a_pushed_one() {
        let (memtable, service) = fixture(config("accepted"));

        service
            .export(tenant_request(request(vec![record("order placed")])))
            .await
            .expect("the export is accepted");

        let results = memtable.query(&test_tenant(), &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 10, true);
        let entries: Vec<&str> = results
            .iter()
            .flat_map(|stream| stream.entries.iter())
            .map(|entry| entry.line.as_str())
            .collect();
        assert_eq!(entries, vec!["order placed"]);
        assert!(
            results[0]
                .entries[0]
                .structured_metadata
                .contains(&("service_name".to_string(), "checkout".to_string())),
            "the resource attribute rides in the entry's own metadata"
        );
    }

    /// A record arriving over gRPC is not exempt from the bounds the Loki path
    /// applies. They exist to keep a label set from becoming a cardinality
    /// problem and a timestamp from landing in a partition retention has
    /// already swept, and neither depends on the protocol.
    #[tokio::test]
    async fn the_same_input_limits_apply_as_on_the_loki_path() {
        let (memtable, service) = fixture(Config {
            max_line_bytes: 32,
            ..config("limits")
        });

        let status = service
            .export(tenant_request(request(vec![record(&"x".repeat(64))])))
            .await
            .expect_err("an oversized line is refused");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);

        let (memtable_window, service_window) = fixture(Config {
            max_timestamp_age: Some(std::time::Duration::from_secs(60)),
            ..config("window")
        });
        let mut ancient = record("from last year");
        ancient.time_unix_nano = 1;
        let status = service_window
            .export(tenant_request(request(vec![ancient])))
            .await
            .expect_err("a timestamp outside the window is refused");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);

        for table in [&memtable, &memtable_window] {
            assert!(
                table
                    .query(&test_tenant(), &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 10, true)
                    .is_empty(),
                "a refused export must not have been written"
            );
        }
    }

    /// A drained server refuses before the request is normalized, with the
    /// code OTLP exporters back off on.
    #[tokio::test]
    async fn a_drained_export_is_refused_before_it_is_normalized() {
        let config = config("drain");
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let memtable = Arc::new(MemTable::new());
        let journal = Arc::new(journal::Journal::spawn(&config, memtable.clone()).unwrap());
        let shutdown = Arc::new(crate::shutdown::ShutdownState::new());
        shutdown.begin_drain();
        let service = LogIngestService::new(
            journal.clone(),
            shutdown,
            Arc::new(config.clone()),
            IngestGate::for_test(&journal, &config),
            crate::tenant_quota::TenantQuota::for_test(&config),
            Arc::new(crate::tenant_policy::TenantPolicy::disabled()),
            crate::clock::Clock::system(),
        );
        let status = service
            .export(tenant_request(request(vec![record("while draining")])))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unavailable);

        assert!(
            memtable
                .query(&test_tenant(), &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 10, true)
                .is_empty(),
            "a refused export must not have been written"
        );
    }

    /// Backpressure is a state, not a latch — ported from the Loki push tests
    /// when that ingest was removed, because the coverage was about the gate
    /// and not the wire. A full memtable refuses with the code exporters back
    /// off on, and a drained one accepts again.
    #[tokio::test]
    async fn an_export_is_refused_while_the_memtable_is_over_its_limit_and_recovers() {
        let config = Config {
            flush_max_bytes: 1,
            max_memtable_bytes: Some(1),
            ..config("memtable_backpressure")
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let memtable = Arc::new(MemTable::new());
        let journal = Arc::new(journal::Journal::spawn(&config, memtable.clone()).unwrap());
        let ingest_gate = IngestGate::for_test(&journal, &config);
        let service = LogIngestService::new(
            journal.clone(),
            Arc::new(crate::shutdown::ShutdownState::new()),
            Arc::new(config.clone()),
            ingest_gate,
            crate::tenant_quota::TenantQuota::for_test(&config),
            Arc::new(crate::tenant_policy::TenantPolicy::disabled()),
            crate::clock::Clock::system(),
        );

        service
            .export(tenant_request(request(vec![record("first line")])))
            .await
            .expect("the first export is under the limit");
        let refused = service
            .export(tenant_request(request(vec![record("second line")])))
            .await
            .expect_err("a full memtable must be refused");
        assert_eq!(refused.code(), tonic::Code::ResourceExhausted);
        assert_eq!(
            refused
                .get_details_retry_info()
                .and_then(|info| info.retry_delay),
            Some(config.backpressure_retry_after),
            "backpressure must tell a gRPC client when to come back, or the \
specification tells it to drop the batch"
        );

        let checkpoint = journal.checkpoint().await.unwrap();
        memtable.commit_flush();
        journal.set_checkpoint(checkpoint.offset).unwrap();
        service
            .export(tenant_request(request(vec![record("third line")])))
            .await
            .expect("a drained memtable accepts writes again");
    }

    /// The WAL-backlog half of the same gate, same porting rationale.
    #[tokio::test]
    async fn an_export_is_refused_while_the_wal_backlog_is_over_its_limit_and_recovers() {
        let config = Config {
            max_memtable_bytes: None,
            max_wal_backlog_bytes: Some(1),
            ..config("wal_backpressure")
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let memtable = Arc::new(MemTable::new());
        let journal = Arc::new(journal::Journal::spawn(&config, memtable.clone()).unwrap());
        let service = LogIngestService::new(
            journal.clone(),
            Arc::new(crate::shutdown::ShutdownState::new()),
            Arc::new(config.clone()),
            IngestGate::for_test(&journal, &config),
            crate::tenant_quota::TenantQuota::for_test(&config),
            Arc::new(crate::tenant_policy::TenantPolicy::disabled()),
            crate::clock::Clock::system(),
        );

        service
            .export(tenant_request(request(vec![record("first line")])))
            .await
            .expect("an empty WAL accepts the first export");
        let refused = service
            .export(tenant_request(request(vec![record("second line")])))
            .await
            .expect_err("an unretired WAL backlog must be refused");
        assert_eq!(refused.code(), tonic::Code::ResourceExhausted);

        let checkpoint = journal.checkpoint().await.unwrap();
        memtable.commit_flush();
        journal.set_checkpoint(checkpoint.offset).unwrap();
        service
            .export(tenant_request(request(vec![record("third line")])))
            .await
            .expect("a retired backlog accepts writes again");
    }

    /// `time_unix_nano` is a u64 and the storage timestamp is an i64; the
    /// value in between must be a refusal, not a wraparound.
    #[tokio::test]
    async fn a_timestamp_past_i64_nanoseconds_is_refused() {
        let (memtable, service) = fixture(config("timestamp_overflow"));
        let mut too_far = record("must be rejected");
        too_far.time_unix_nano = u64::MAX;

        let status = service
            .export(tenant_request(request(vec![too_far])))
            .await
            .expect_err("an out-of-range timestamp must fail");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(memtable.is_empty());
    }

    /// With the window disabled, a historical import is accepted however old.
    #[tokio::test]
    async fn a_disabled_timestamp_window_accepts_a_backfill() {
        let (memtable, service) = fixture(Config {
            max_timestamp_age: None,
            max_timestamp_skew: None,
            ..config("window_off")
        });
        let mut old = record("historical import");
        old.time_unix_nano = 1_000_000;

        service
            .export(tenant_request(request(vec![old])))
            .await
            .expect("a disabled window must accept any in-range timestamp");
        assert!(!memtable.is_empty());
    }

    /// The journal keeps one encoding for a log record whatever protocol it
    /// arrived on. If the OTLP path wrote something replay could not decode,
    /// the WAL would be unreadable after a crash — and it is the WAL that makes
    /// an acknowledged write durable.
    #[tokio::test]
    async fn an_exported_record_survives_a_replay_of_the_journal() {
        let config = config("replay");
        let (_memtable, service) = fixture(config.clone());
        service
            .export(tenant_request(request(vec![record("survives a crash")])))
            .await
            .unwrap();
        drop(service);

        let replayed = Arc::new(MemTable::new());
        journal::replay_with_traces(
            &config.data_dir.join("journal.wal"),
            &config.data_dir.join("journal.ckpt"),
            &replayed,
            &crate::trace::TraceMemTable::new())
        .expect("the WAL replays");
        let results = replayed.query(&test_tenant(), &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 10, true);
        let lines: Vec<&str> = results
            .iter()
            .flat_map(|stream| stream.entries.iter())
            .map(|entry| entry.line.as_str())
            .collect();
        assert_eq!(lines, vec!["survives a crash"]);
        assert!(
            results[0]
                .entries[0]
                .structured_metadata
                .contains(&("service_name".to_string(), "checkout".to_string())),
            "replay reconstructs the attributes, not just the line"
        );
    }

    /// The two transports carry one instruction, not two compatible ones.
    ///
    /// This is the property the mapping exists for. HTTP renders a refusal's
    /// `retry_after` as `Retry-After`; gRPC renders the same field as
    /// `RetryInfo`, and the OTLP specification makes that attachment the whole
    /// difference between "hold this and come back" and "drop it" — a
    /// `RESOURCE_EXHAUSTED` without one is retryable only if the server signals
    /// recovery is possible, and a bare code signals nothing. So the test drives
    /// one error down both renderings and compares the number, rather than
    /// asserting each side in isolation and leaving the two free to drift.
    #[test]
    fn a_throttled_push_names_the_same_delay_on_both_transports() {
        use axum::http::header;
        use axum::response::IntoResponse;

        for retry_after in [
            std::time::Duration::from_secs(1),
            // Sub-second and fractional: the header cannot express either, so
            // both sides round up together. Truncating instead would send the
            // client back before the server's own arithmetic says it may.
            std::time::Duration::from_millis(300),
            std::time::Duration::from_millis(1700),
        ] {
            let http = IngestError {
                status: StatusCode::TOO_MANY_REQUESTS,
                message: "flush is not keeping up".to_string(),
                retry_after: Some(retry_after),
            }
            .into_response();
            let header_seconds: u64 = http
                .headers()
                .get(header::RETRY_AFTER)
                .expect("a throttled HTTP push carries Retry-After")
                .to_str()
                .unwrap()
                .parse()
                .unwrap();

            let status = ingest_error_to_status(IngestError {
                status: StatusCode::TOO_MANY_REQUESTS,
                message: "flush is not keeping up".to_string(),
                retry_after: Some(retry_after),
            });
            let grpc_delay = status
                .get_details_retry_info()
                .expect("a throttled gRPC push carries RetryInfo")
                .retry_delay
                .expect("with a delay in it");

            assert_eq!(
                grpc_delay,
                std::time::Duration::from_secs(header_seconds),
                "{retry_after:?} became {header_seconds}s over HTTP and \
{grpc_delay:?} over gRPC"
            );
            assert!(header_seconds >= 1, "Retry-After: 0 reads as retry now");
        }
    }

    /// And the attachment stays on the retryable path only.
    ///
    /// A limit violation is permanent for that batch — retrying produces the
    /// identical refusal forever — so it answers `INVALID_ARGUMENT` and must not
    /// acquire a "come back later" on the way past. The two codes were one
    /// before `7538367`; this pins the split from the other side.
    #[test]
    fn a_permanent_refusal_carries_no_invitation_to_retry() {
        for status in [StatusCode::PAYLOAD_TOO_LARGE, StatusCode::BAD_REQUEST] {
            let refused = ingest_error_to_status(IngestError {
                status,
                message: "OTLP request is too large".to_string(),
                // Set deliberately: even if a producer attaches one, a
                // non-retryable code must not carry it.
                retry_after: Some(std::time::Duration::from_secs(1)),
            });
            assert_eq!(refused.code(), tonic::Code::InvalidArgument);
            assert!(
                refused.get_details_retry_info().is_none(),
                "a non-retryable refusal must not tell a collector to come back"
            );
        }
    }
