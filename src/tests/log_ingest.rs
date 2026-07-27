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

        let results = memtable.query(&test_tenant(), &[], &[], i64::MIN, i64::MAX, 10, true);
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
                    .query(&test_tenant(), &[], &[], i64::MIN, i64::MAX, 10, true)
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
                .query(&test_tenant(), &[], &[], i64::MIN, i64::MAX, 10, true)
                .is_empty(),
            "a refused export must not have been written"
        );
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
            &crate::trace::TraceMemTable::new(),
            &config.default_tenant,
        )
        .expect("the WAL replays");
        let results = replayed.query(&test_tenant(), &[], &[], i64::MIN, i64::MAX, 10, true);
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
