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
            crate::clock::Clock::system(),
            Arc::new(crate::part_registry::PartRegistry::new()),
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

        let results = memtable.query(&test_tenant(), &[], &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 10, true);
        let entries: Vec<&str> = results
            .iter()
            .flat_map(|stream| stream.entries.iter())
            .map(|entry| entry.line.as_str())
            .collect();
        assert_eq!(entries, vec!["order placed"]);
        assert_eq!(results[0].labels["service_name"], "checkout");
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
                    .query(&test_tenant(), &[], &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 10, true)
                    .is_empty(),
                "a refused export must not have been written"
            );
        }
    }

    /// Backpressure and the tenant rate are decided before the request is
    /// normalized, and they answer with the code OTLP exporters back off on.
    #[tokio::test]
    async fn a_drained_or_over_quota_export_is_refused_before_it_is_normalized() {
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
            crate::clock::Clock::system(),
            Arc::new(crate::part_registry::PartRegistry::new()),
        );
        let status = service
            .export(tenant_request(request(vec![record("while draining")])))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unavailable);

        let quota_config = Config {
            default_tenant_ingest_bytes_per_second: Some(1),
            tenant_ingest_burst: std::time::Duration::from_nanos(1),
            max_push_bytes: 1,
            ..config.clone()
        };
        let quota_service = LogIngestService::new(
            journal.clone(),
            Arc::new(crate::shutdown::ShutdownState::new()),
            Arc::new(quota_config.clone()),
            IngestGate::for_test(&journal, &quota_config),
            crate::tenant_quota::TenantQuota::for_test(&quota_config),
            crate::clock::Clock::system(),
            Arc::new(crate::part_registry::PartRegistry::new()),
        );
        // The first export drains the one-byte bucket, the second finds it empty.
        let _ = quota_service
            .export(tenant_request(request(vec![record("first")])))
            .await;
        let status = quota_service
            .export(tenant_request(request(vec![record("second")])))
            .await
            .expect_err("the tenant is over its rate");
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);

        assert!(
            memtable
                .query(&test_tenant(), &[], &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 10, true)
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
            crate::clock::Clock::system(),
            Arc::new(crate::part_registry::PartRegistry::new()),
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
            crate::clock::Clock::system(),
            Arc::new(crate::part_registry::PartRegistry::new()),
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

    /// The label-count bound applies to what promotion produces: three
    /// promoted resource attributes against a limit of two is a refusal, and
    /// nothing may have been written.
    #[tokio::test]
    async fn an_export_whose_promoted_labels_exceed_the_limit_is_refused() {
        let (memtable, service) = fixture(Config {
            max_label_names_per_stream: 2,
            ..config("label_count")
        });
        let mut export = request(vec![record("too many labels")]);
        let resource = export.resource_logs[0].resource.as_mut().unwrap();
        for name in ["deployment.environment", "cloud.region"] {
            resource.attributes.push(KeyValue {
                key: name.to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("x".to_string())),
                }),
                ..Default::default()
            });
        }

        let status = service
            .export(tenant_request(export))
            .await
            .expect_err("three promoted labels against a limit of two");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("3 labels"), "{}", status.message());
        assert!(
            memtable
                .query(
                    &test_tenant(),
                    &[],
                    &[],
                    crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX),
                    10,
                    true
                )
                .is_empty()
        );
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
        let results = replayed.query(&test_tenant(), &[], &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 10, true);
        let lines: Vec<&str> = results
            .iter()
            .flat_map(|stream| stream.entries.iter())
            .map(|entry| entry.line.as_str())
            .collect();
        assert_eq!(lines, vec!["survives a crash"]);
        assert_eq!(
            results[0].labels["service_name"], "checkout",
            "replay reconstructs the stream, not just the line"
        );
    }
