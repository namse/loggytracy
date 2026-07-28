    use super::*;
    use crate::tenant::test_tenant;
    use crate::config::Config;
    use crate::journal::Journal;
    use crate::memtable::{LogEntry, MemTable};
    use crate::part::{self, Row};
    use crate::part_registry::PartRegistry;
    use tower::ServiceExt;

    fn temp_dir() -> std::path::PathBuf {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "loggytracy-query-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn test_state(
        data_dir: &std::path::Path,
        memtable: Arc<MemTable>,
        parts: Arc<PartRegistry>,
        remote_cache: Option<Arc<crate::object_storage::RemoteCache>>,
    ) -> Arc<AppState> {
        let config = Config {
            data_dir: data_dir.to_path_buf(),
            ..Config::default()
        };
        let trace_parts = Arc::new(crate::trace_registry::TraceRegistry::new(
            parts.operation_lock(),
        ));
        crate::test_support::state(
            config.clone(),
            memtable.clone(),
            Arc::new(Journal::spawn(&config, memtable).unwrap()),
            parts,
            trace_parts,
            remote_cache,
        )
    }

    #[test]
    fn extracts_repeated_match_params() {
        let raw =
            Some("match%5B%5D=%7Bapp%3D%22a%22%7D&match%5B%5D=%7Bapp%3D%22b%22%7D".to_string());
        let v = extract_match_params(&raw);
        assert_eq!(v, vec![r#"{app="a"}"#, r#"{app="b"}"#]);
    }

    #[test]
    fn extracts_single_match_param() {
        let raw = Some("match%5B%5D=%7Bjob%3D%22x%22%7D".to_string());
        let v = extract_match_params(&raw);
        assert_eq!(v, vec![r#"{job="x"}"#]);
    }

    #[test]
    fn empty_when_no_match() {
        assert!(extract_match_params(&None).is_empty());
        assert!(extract_match_params(&Some("foo=bar".to_string())).is_empty());
    }

    #[tokio::test]
    async fn http_query_range_decodes_url_encoded_logql() {
        let data_dir = temp_dir();
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let memtable = Arc::new(MemTable::new());
        memtable.insert(
            test_tenant(),
            labels,
            vec![LogEntry {
                timestamp_ns: 5_000_000_000,
                line: "hello from http".to_string(),
                structured_metadata: Vec::new(),
            }],
        );
        let state = test_state(&data_dir, memtable, Arc::new(PartRegistry::new()), None);
        let request = axum::http::Request::builder()
            .uri("/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22hello%22&start=4&end=6&limit=10&direction=forward")
            .header(crate::tenant::TENANT_HEADER, test_tenant().as_str())
            .body(axum::body::Body::empty())
            .unwrap();

        let response = crate::build_router(state).oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "success");
        assert_eq!(json["data"]["resultType"], "streams");
        assert_eq!(json["data"]["result"][0]["values"][0][1], "hello from http");
        assert_eq!(json["data"]["stats"]["summary"]["totalLinesProcessed"], 1);
    }

    #[tokio::test]
    async fn omitted_metric_start_scans_the_first_lookback_window() {
        let data_dir = temp_dir();
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let memtable = Arc::new(MemTable::new());
        memtable.insert(
            test_tenant(),
            labels,
            vec![LogEntry {
                timestamp_ns: 20_000_000_000,
                line: "inside first lookback".to_string(),
                structured_metadata: Vec::new(),
            }],
        );
        let state = test_state(&data_dir, memtable, Arc::new(PartRegistry::new()), None);

        let response = query_range(
            State(state),
            crate::tenant::test_tenant_headers(),
            Query(QueryRangeParams {
                query: "count_over_time({}[60s])".to_string(),
                start: None,
                end: Some("100".to_string()),
                limit: None,
                direction: None,
                step: Some("60".to_string()),
            }),
        )
        .await
        .unwrap()
        .0;

        assert_eq!(response.data.result[0]["values"][0][0], 40);
        assert_eq!(response.data.result[0]["values"][0][1], "1");
    }

    #[tokio::test]
    async fn query_scan_stats_count_rows_before_applying_output_limit() {
        let data_dir = temp_dir();
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let memtable = Arc::new(MemTable::new());
        memtable.insert(
            test_tenant(),
            labels,
            (0..3)
                .map(|timestamp_ns| LogEntry {
                    timestamp_ns,
                    line: format!("line-{timestamp_ns}"),
                    structured_metadata: Vec::new(),
                })
                .collect(),
        );
        let state = test_state(&data_dir, memtable, Arc::new(PartRegistry::new()), None);
        let parsed = logql::parse("{}").unwrap();
        let execution =
            run_unified_query_with_stats(
                state.clone(),
                test_tenant(),
                parsed.clone(),
                0,
                2,
                1,
                true,
                None,
            )
                .await
                .unwrap();
        assert_eq!(execution.results[0].entries.len(), 1);
        assert_eq!(execution.scanned_rows, 3);

        let error = match unified_query_with_stats_cancellable(
            &state,
            &test_tenant(),
            &parsed,
            0,
            2,
            1,
            true,
            Some(2),
            None,
        ) {
            Ok(_) => panic!("scan budget should reject the third physical row"),
            Err(error) => error,
        };
        assert!(error.contains("2 scanned rows"));
    }

    #[test]
    fn parse_time_ns_unix_seconds() {
        assert_eq!(parse_time_ns("0").unwrap(), 0);
        assert_eq!(
            parse_time_ns("1700000000").unwrap(),
            1_700_000_000_000_000_000
        );
    }

    #[test]
    fn parse_time_ns_negative_unix_seconds() {
        assert_eq!(
            parse_time_ns("-1700000000").unwrap(),
            -1_700_000_000_000_000_000
        );
    }

    #[test]
    fn parse_time_ns_unix_nanos() {
        assert_eq!(
            parse_time_ns("1700000000000000000").unwrap(),
            1_700_000_000_000_000_000
        );
    }

    #[test]
    fn parse_time_ns_unix_millis_and_micros() {
        assert_eq!(
            parse_time_ns("1700000000000").unwrap(),
            1_700_000_000_000_000_000
        );
        assert_eq!(
            parse_time_ns("1700000000000000").unwrap(),
            1_700_000_000_000_000_000
        );
    }

    #[test]
    fn parse_time_ns_rfc3339() {
        let ns = parse_time_ns("2023-11-14T22:13:20Z").unwrap();
        assert_eq!(ns, 1_700_000_000_000_000_000);
    }

    #[test]
    fn parse_time_ns_rfc3339_with_nanos() {
        let ns = parse_time_ns("2023-11-14T22:13:20.123456789Z").unwrap();
        assert_eq!(ns, 1_700_000_000_123_456_789);
    }

    #[test]
    fn parse_time_ns_invalid() {
        assert!(parse_time_ns("not-a-time").is_err());
        assert!(parse_time_ns("").is_err());
    }

    #[test]
    fn parse_time_ns_accepts_exact_decimal_unix_seconds() {
        assert_eq!(
            parse_time_ns("1700000000.000000001").unwrap(),
            1_700_000_000_000_000_001
        );
        assert_eq!(parse_time_ns("1.5e0").unwrap(), 1_500_000_000);
        assert!(parse_time_ns("1.0000000001").is_err());
    }

    #[test]
    fn metric_timestamps_keep_nanosecond_precision_in_json() {
        assert_eq!(
            timestamp_seconds_json(1_700_000_000_000_000_001).to_string(),
            "1700000000.000000001"
        );
        assert_eq!(timestamp_seconds_json(-1).to_string(), "-0.000000001");
    }

    #[test]
    fn query_input_limits_and_direction_are_validated() {
        assert_eq!(parse_limit(None, MAX_LOG_LIMIT).unwrap(), 100);
        assert_eq!(
            parse_limit(Some(MAX_LOG_LIMIT), MAX_LOG_LIMIT).unwrap(),
            MAX_LOG_LIMIT
        );
        assert!(parse_limit(Some(MAX_LOG_LIMIT + 1), MAX_LOG_LIMIT).is_err());
        assert!(parse_direction(&Some("sideways".to_string())).is_err());
        assert!(parse_direction(&Some("FORWARD".to_string())).unwrap());
        assert!(!parse_direction(&Some("backward".to_string())).unwrap());
    }

    #[tokio::test]
    async fn distinct_stream_count_deduplicates_memtable_and_parts() {
        let data_dir = temp_dir();
        let config = Config {
            data_dir: data_dir.clone(),
            ..Config::default()
        };
        let labels: Labels = [("app".to_string(), "same-stream".to_string())]
            .into_iter()
            .collect();
        let memtable = Arc::new(MemTable::new());
        memtable.insert(
            test_tenant(),
            labels.clone(),
            vec![LogEntry {
                timestamp_ns: 2,
                line: "in memory".to_string(),
                structured_metadata: Vec::new(),
            }],
        );
        let parts_root = data_dir.join("parts");
        let parts = Arc::new(PartRegistry::new());
        parts
            .register(
                part::flush_rows(
                    vec![Row {
                        tenant: test_tenant(),
                        timestamp_ns: 1,
                        labels,
                        line: "on disk".to_string(),
                        structured_metadata: Vec::new(),
                    }],
                    &parts_root,
                    config.row_group_size,
                )
                .unwrap(),
            )
            .unwrap();
        let journal = Arc::new(Journal::spawn(&config, memtable.clone()).unwrap());
        let trace_parts = Arc::new(crate::trace_registry::TraceRegistry::new(
            parts.operation_lock(),
        ));
        let state = crate::test_support::state(
            config,
            memtable,
            journal,
            parts,
            trace_parts,
            None,
        );

        assert_eq!(distinct_stream_count(&state, &test_tenant(), crate::part::MetadataWindow::unbounded()), 1);
    }

    #[tokio::test]
    async fn readiness_reflects_background_worker_health() {
        let data_dir = temp_dir();
        let config = Config {
            data_dir,
            ..Config::default()
        };
        let memtable = Arc::new(MemTable::new());
        let state = crate::test_support::state(
            config.clone(),
            memtable.clone(),
            Arc::new(Journal::spawn(&config, memtable).unwrap()),
            Arc::new(PartRegistry::new()),
            Arc::new(crate::trace_registry::TraceRegistry::standalone()),
            None,
        );

        assert_eq!(ready(State(state.clone())).await.unwrap(), "ready");

        state.flush_healthy.store(false, Ordering::Release);
        state.merge_healthy.store(false, Ordering::Release);
        state.otlp_healthy.store(false, Ordering::Release);
        let error = ready(State(state)).await.unwrap_err();
        assert_eq!(error.0, StatusCode::SERVICE_UNAVAILABLE);
        assert!(error.1.contains("flush worker"));
        assert!(error.1.contains("merge worker"));
        assert!(error.1.contains("OTLP gRPC server"));
    }

    /// Readiness follows a sustained object-store outage, not a single failed
    /// request.
    ///
    /// One failure used to mark the store unhealthy, and `/ready` reads that.
    /// Measured under a 3% injected write-error rate — which the engine
    /// survives with no ingest errors and no lost data — the flag flipped
    /// 14-17 times a minute and read false 34-59% of the time. An orchestrator
    /// watching it pulls the instance out of service over an error rate that
    /// cost nothing.
    #[tokio::test]
    async fn readiness_ignores_an_isolated_object_store_failure() {
        let data_dir = temp_dir();
        let config = Config {
            data_dir: data_dir.clone(),
            ..Config::default()
        };
        let memtable = Arc::new(MemTable::new());
        let remote = Arc::new(crate::object_storage::RemoteCache::new(
            Arc::new(crate::object_storage::ObjectStorage::in_memory()),
            data_dir.join("parts"),
        ));
        remote.mark_cache_healthy();
        let state = crate::test_support::state(
            config.clone(),
            memtable.clone(),
            Arc::new(Journal::spawn(&config, memtable).unwrap()),
            Arc::new(PartRegistry::new()),
            Arc::new(crate::trace_registry::TraceRegistry::standalone()),
            Some(remote.clone()),
        );

        remote.record_remote_failure();
        assert!(
            remote.is_remote_healthy(),
            "one failed request is not an outage"
        );
        assert!(ready(State(state.clone())).await.is_ok());

        // A success between failures is what distinguishes a flaky request
        // from a store that is gone, so it resets the count.
        remote.record_remote_failure();
        remote.record_remote_success();
        remote.record_remote_failure();
        remote.record_remote_failure();
        assert!(remote.is_remote_healthy(), "{}", remote.consecutive_remote_failures());
        assert!(ready(State(state.clone())).await.is_ok());

        // Failing without a success in between does mean the store is gone.
        remote.record_remote_failure();
        assert!(!remote.is_remote_healthy());
        assert_eq!(
            ready(State(state.clone())).await.unwrap_err().0,
            StatusCode::SERVICE_UNAVAILABLE
        );

        // And one success is enough to come back, because the evidence that
        // the store works is the store working.
        remote.record_remote_success();
        assert!(ready(State(state)).await.is_ok());
    }

    #[tokio::test]
    async fn readiness_reflects_remote_storage_health() {
        let data_dir = temp_dir();
        let config = Config {
            data_dir: data_dir.clone(),
            ..Config::default()
        };
        let memtable = Arc::new(MemTable::new());
        let remote = Arc::new(crate::object_storage::RemoteCache::new(
            Arc::new(crate::object_storage::ObjectStorage::in_memory()),
            data_dir.join("parts"),
        ));
        remote.mark_unhealthy();
        remote.mark_cache_healthy();
        let state = crate::test_support::state(
            config.clone(),
            memtable.clone(),
            Arc::new(Journal::spawn(&config, memtable).unwrap()),
            Arc::new(PartRegistry::new()),
            Arc::new(crate::trace_registry::TraceRegistry::standalone()),
            Some(remote.clone()),
        );

        let error = ready(State(state)).await.unwrap_err();
        assert_eq!(error.0, StatusCode::SERVICE_UNAVAILABLE);
        assert!(error.1.contains("object store"));

        remote.record_remote_success();
        remote.mark_cache_unhealthy();
        let memtable = Arc::new(MemTable::new());
        let state = crate::test_support::state(
            config.clone(),
            memtable.clone(),
            Arc::new(Journal::spawn(&config, memtable).unwrap()),
            Arc::new(PartRegistry::new()),
            Arc::new(crate::trace_registry::TraceRegistry::standalone()),
            Some(remote),
        );
        let error = ready(State(state)).await.unwrap_err();
        assert!(error.1.contains("local cache"));
    }

    #[tokio::test]
    async fn query_restores_evicted_part_from_object_store() {
        let data_dir = temp_dir();
        let config = Config {
            data_dir: data_dir.clone(),
            ..Config::default()
        };
        let parts_root = data_dir.join("parts");
        let storage = Arc::new(crate::object_storage::ObjectStorage::in_memory());
        let labels: Labels = [("app".to_string(), "remote".to_string())]
            .into_iter()
            .collect();
        let local_parts = part::flush_rows(
            vec![Row {
                tenant: test_tenant(),
                timestamp_ns: 1_700_000_000_000_000_000,
                labels,
                line: "restored after eviction".to_string(),
                structured_metadata: Vec::new(),
            }],
            &parts_root,
            config.row_group_size,
        )
        .unwrap();
        let other_labels: Labels = [("app".to_string(), "other".to_string())]
            .into_iter()
            .collect();
        let other_parts = part::flush_rows(
            vec![Row {
                tenant: test_tenant(),
                timestamp_ns: 1_700_000_000_000_000_001,
                labels: other_labels,
                line: "must remain remote".to_string(),
                structured_metadata: Vec::new(),
            }],
            &parts_root,
            config.row_group_size,
        )
        .unwrap();
        let other_data_path = other_parts[0].data_path();
        let mut published_parts = local_parts.clone();
        published_parts.extend(other_parts);
        storage.publish(&published_parts, &[]).await.unwrap();
        let parts = Arc::new(PartRegistry::load_from_disk(&parts_root).unwrap());
        storage
            .evict_cache(&parts_root, 0, &parts.part_dirs())
            .unwrap();
        assert!(parts.has_missing_cache_files());

        let memtable = Arc::new(MemTable::new());
        let trace_parts = Arc::new(crate::trace_registry::TraceRegistry::new(
            parts.operation_lock(),
        ));
        let state = crate::test_support::state(
            config.clone(),
            memtable.clone(),
            Arc::new(Journal::spawn(&config, memtable).unwrap()),
            parts,
            trace_parts,
            Some(Arc::new(crate::object_storage::RemoteCache::new(
                storage, parts_root,
            ))),
        );
        let parsed = logql::parse(r#"{app="remote"}"#).unwrap();
        let result = run_unified_query(state, test_tenant(), parsed, i64::MIN, i64::MAX, 10, true)
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].entries[0].line, "restored after eviction");
        assert!(
            !other_data_path.exists(),
            "query restored an unrelated part"
        );
    }

    #[tokio::test]
    async fn query_replans_restore_after_registry_changes_in_lock_gap() {
        let data_dir = temp_dir();
        let config = Config {
            data_dir: data_dir.clone(),
            ..Config::default()
        };
        let parts_root = data_dir.join("parts");
        let storage = Arc::new(crate::object_storage::ObjectStorage::in_memory());
        let labels: Labels = [("app".to_string(), "remote".to_string())]
            .into_iter()
            .collect();
        let old = part::flush_rows(
            vec![Row {
                tenant: test_tenant(),
                timestamp_ns: 1,
                labels: labels.clone(),
                line: "old generation".to_string(),
                structured_metadata: Vec::new(),
            }],
            &parts_root,
            config.row_group_size,
        )
        .unwrap();
        storage.publish(&old, &[]).await.unwrap();
        let parts = Arc::new(PartRegistry::load_from_disk(&parts_root).unwrap());

        let new = part::flush_rows(
            vec![Row {
                tenant: test_tenant(),
                timestamp_ns: 2,
                labels,
                line: "new generation".to_string(),
                structured_metadata: Vec::new(),
            }],
            &parts_root,
            config.row_group_size,
        )
        .unwrap();
        let manifest = storage
            .publish(&new, &[old[0].meta.id.clone()])
            .await
            .unwrap();
        let eligible = vec![old[0].dir.clone(), new[0].dir.clone()];
        storage.evict_cache(&parts_root, 0, &eligible).unwrap();

        let memtable = Arc::new(MemTable::new());
        let trace_parts = Arc::new(crate::trace_registry::TraceRegistry::new(
            parts.operation_lock(),
        ));
        let state = crate::test_support::state(
            config.clone(),
            memtable.clone(),
            Arc::new(Journal::spawn(&config, memtable).unwrap()),
            parts.clone(),
            trace_parts,
            Some(Arc::new(crate::object_storage::RemoteCache::new(
                storage,
                parts_root.clone(),
            ))),
        );
        let parsed = logql::parse(r#"{app="remote"}"#).unwrap();
        let guard = pin_query_parts_with_gap_hook(&state, &test_tenant(), &parsed, i64::MIN, i64::MAX, || {
            parts.reload_from_manifest(&parts_root, &manifest)
        })
        .await
        .unwrap();
        let result = unified_query(&state, &test_tenant(), &parsed, i64::MIN, i64::MAX, 10, true).unwrap();
        drop(guard);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].entries[0].line, "new generation");
        assert!(new[0].data_path().exists());
    }

    #[tokio::test]
    async fn structured_filters_match_identically_in_memtable_and_parts() {
        let data_dir = temp_dir();
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let line = r#"{"code":503,"elapsed":"250ms","json":"ok","level":"error"}"#;
        let metadata = vec![
            ("trace_id".to_string(), "abc".to_string()),
            ("logfmt".to_string(), "value".to_string()),
        ];
        let memtable = Arc::new(MemTable::new());
        memtable.insert(
            test_tenant(),
            labels.clone(),
            vec![LogEntry {
                timestamp_ns: 10,
                line: line.to_string(),
                structured_metadata: metadata.clone(),
            }],
        );
        let parts = Arc::new(PartRegistry::new());
        parts
            .register(
                part::flush_rows(
                    vec![Row {
                        tenant: test_tenant(),
                        timestamp_ns: 20,
                        labels,
                        line: line.to_string(),
                        structured_metadata: metadata,
                    }],
                    &data_dir.join("parts"),
                    1,
                )
                .unwrap(),
            )
            .unwrap();
        let state = test_state(&data_dir, memtable, parts, None);
        let parsed = logql::parse(
            r#"{app="api"} | app="api" | trace_id="abc" | json | level=~"err.*" | code>=500 | elapsed<1s"#,
        )
        .unwrap();

        let result = run_unified_query(state.clone(), test_tenant(), parsed, 0, 30, 10, true)
            .await
            .unwrap();
        let timestamps: Vec<_> = result[0]
            .entries
            .iter()
            .map(|entry| entry.timestamp_ns)
            .collect();
        assert_eq!(timestamps, vec![10, 20]);

        for query in [r#"{} | json | json="ok""#, r#"{} | logfmt="value""#] {
            let result =
                run_unified_query(state.clone(), test_tenant(), logql::parse(query).unwrap(), 0, 30, 10, true)
                    .await
                    .unwrap();
            let timestamps: Vec<_> = result[0]
                .entries
                .iter()
                .map(|entry| entry.timestamp_ns)
                .collect();
            assert_eq!(timestamps, vec![10, 20]);
        }
    }

    #[tokio::test]
    async fn synthesized_extracted_field_never_false_negative_prunes_parts() {
        let data_dir = temp_dir();
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let metadata = vec![
            ("foo".to_string(), "x".to_string()),
            ("foo_extracted".to_string(), "y".to_string()),
        ];
        let memtable = Arc::new(MemTable::new());
        memtable.insert(
            test_tenant(),
            labels.clone(),
            vec![LogEntry {
                timestamp_ns: 10,
                line: r#"{"foo":"z"}"#.to_string(),
                structured_metadata: metadata.clone(),
            }],
        );
        let parts = Arc::new(PartRegistry::new());
        parts
            .register(
                part::flush_rows(
                    vec![Row {
                        tenant: test_tenant(),
                        timestamp_ns: 20,
                        labels,
                        line: r#"{"foo":"z"}"#.to_string(),
                        structured_metadata: metadata,
                    }],
                    &data_dir.join("parts"),
                    1,
                )
                .unwrap(),
            )
            .unwrap();
        let state = test_state(&data_dir, memtable, parts, None);
        let parsed = logql::parse(r#"{} | json | foo_extracted_2="z""#).unwrap();

        let result = run_unified_query(state, test_tenant(), parsed, 0, 30, 10, true)
            .await
            .unwrap();
        let timestamps: Vec<_> = result[0]
            .entries
            .iter()
            .map(|entry| entry.timestamp_ns)
            .collect();
        assert_eq!(timestamps, vec![10, 20]);
    }

    #[test]
    fn metric_windows_are_left_open_and_aggregations_recompute_each_step() {
        let a: Labels = [("app".to_string(), "a".to_string())].into_iter().collect();
        let b: Labels = [("app".to_string(), "b".to_string())].into_iter().collect();
        let entries = vec![
            (
                a.clone(),
                LogEntry {
                    timestamp_ns: 5_000_000_000,
                    line: "boundary".into(),
                    structured_metadata: vec![],
                },
            ),
            (
                a.clone(),
                LogEntry {
                    timestamp_ns: 6_000_000_000,
                    line: "a1".into(),
                    structured_metadata: vec![],
                },
            ),
            (
                a.clone(),
                LogEntry {
                    timestamp_ns: 10_000_000_000,
                    line: "a2".into(),
                    structured_metadata: vec![],
                },
            ),
            (
                b.clone(),
                LogEntry {
                    timestamp_ns: 8_000_000_000,
                    line: "b1".into(),
                    structured_metadata: vec![],
                },
            ),
            (
                b.clone(),
                LogEntry {
                    timestamp_ns: 13_000_000_000,
                    line: "b2".into(),
                    structured_metadata: vec![],
                },
            ),
            (
                b.clone(),
                LogEntry {
                    timestamp_ns: 14_000_000_000,
                    line: "b3".into(),
                    structured_metadata: vec![],
                },
            ),
        ];
        let logql::QueryExpr::Metric(expr) =
            logql::parse_expr(r#"topk(1, sum by (app) (count_over_time({}[5s])))"#).unwrap()
        else {
            panic!("expected metric")
        };

        let at_ten = evaluate_metric_at(&expr, &entries, 10_000_000_000);
        assert_eq!(at_ten, vec![(a, 2.0)], "entry at t-range must be excluded");
        let at_fifteen = evaluate_metric_at(&expr, &entries, 15_000_000_000);
        assert_eq!(
            at_fifteen,
            vec![(b, 2.0)],
            "topk must be recomputed per step"
        );

        for (name, expected) in [("sum", 3.0), ("avg", 1.5), ("min", 1.0), ("max", 2.0)] {
            let logql::QueryExpr::Metric(aggregate) =
                logql::parse_expr(&format!("{name}(count_over_time({{}}[5s]))")).unwrap()
            else {
                panic!("expected metric")
            };
            assert_eq!(
                evaluate_metric_at(&aggregate, &entries, 10_000_000_000),
                vec![(Labels::new(), expected)]
            );
        }
    }

    #[test]
    fn metric_evaluation_stops_when_timeout_cancellation_is_requested() {
        let expr = match logql::parse_expr("count_over_time({}[1m])") {
            Ok(logql::QueryExpr::Metric(expr)) => expr,
            _ => panic!("expected metric expression"),
        };
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let entries = vec![(
            labels,
            LogEntry {
                timestamp_ns: 1,
                line: "line".to_string(),
                structured_metadata: Vec::new(),
            },
        )];
        let cancelled = AtomicBool::new(true);
        let result = evaluate_metric_stream(&expr, &entries, &[1], Some(&cancelled));
        assert_eq!(result.unwrap_err(), "metric query timed out");
    }

    #[tokio::test]
    async fn metric_window_includes_i64_min_when_mathematical_start_underflows() {
        let data_dir = temp_dir();
        let labels: Labels = [("app".to_string(), "oldest".to_string())]
            .into_iter()
            .collect();
        let parts = Arc::new(PartRegistry::new());
        parts
            .register(
                part::flush_rows(
                    vec![Row {
                        tenant: test_tenant(),
                        timestamp_ns: i64::MIN,
                        labels: labels.clone(),
                        line: "oldest".to_string(),
                        structured_metadata: vec![],
                    }],
                    &data_dir.join("parts"),
                    1,
                )
                .unwrap(),
            )
            .unwrap();
        let state = test_state(&data_dir, Arc::new(MemTable::new()), parts, None);
        let logql::QueryExpr::Metric(expr) = logql::parse_expr("count_over_time({}[5ns])").unwrap()
        else {
            panic!("expected metric")
        };

        let result = run_metric_query(state, test_tenant(), expr, vec![i64::MIN + 1])
            .await
            .unwrap();
        assert_eq!(result[0].labels, labels);
        assert_eq!(result[0].samples, vec![(i64::MIN + 1, 1.0)]);
    }

    #[tokio::test]
    async fn metric_query_ignores_log_limit_and_returns_matrix_or_vector() {
        let data_dir = temp_dir();
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let memtable = Arc::new(MemTable::new());
        memtable.insert(
            test_tenant(),
            labels,
            (1..=3)
                .map(|second| LogEntry {
                    timestamp_ns: second * 1_000_000_000,
                    line: "xx".to_string(),
                    structured_metadata: vec![],
                })
                .collect(),
        );
        let state = test_state(&data_dir, memtable, Arc::new(PartRegistry::new()), None);

        let range = query_range(
            State(state.clone()),
            crate::tenant::test_tenant_headers(),
            Query(QueryRangeParams {
                query: "bytes_over_time({}[5s])".to_string(),
                start: Some("5".to_string()),
                end: Some("5".to_string()),
                limit: Some(1),
                direction: None,
                step: Some("1".to_string()),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(range.data.result_type, "matrix");
        assert_eq!(range.data.result[0]["values"][0][1], "6");

        let instant = query(
            State(state),
            crate::tenant::test_tenant_headers(),
            Query(QueryParams {
                query: "count_over_time({}[5s])".to_string(),
                time: Some("5".to_string()),
                limit: Some(1),
                direction: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(instant.data.result_type, "vector");
        assert_eq!(instant.data.result[0]["value"][1], "3");
    }

    #[tokio::test]
    async fn metric_grouping_uses_extracted_fields_and_parser_errors() {
        let data_dir = temp_dir();
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let memtable = Arc::new(MemTable::new());
        memtable.insert(
            test_tenant(),
            labels,
            vec![
                LogEntry {
                    timestamp_ns: 6_000_000_000,
                    line: r#"{"level":"info","app":"line"}"#.to_string(),
                    structured_metadata: vec![],
                },
                LogEntry {
                    timestamp_ns: 7_000_000_000,
                    line: r#"{"level":"error"}"#.to_string(),
                    structured_metadata: vec![],
                },
                LogEntry {
                    timestamp_ns: 8_000_000_000,
                    line: "not json".to_string(),
                    structured_metadata: vec![],
                },
            ],
        );
        let state = test_state(&data_dir, memtable, Arc::new(PartRegistry::new()), None);

        let logql::QueryExpr::Metric(expr) =
            logql::parse_expr(r#"sum(count_over_time({app="api"} | json [5s])) by (level)"#)
                .unwrap()
        else {
            panic!("expected metric")
        };
        let result = run_metric_query(state.clone(), test_tenant(), expr, vec![10_000_000_000])
            .await
            .unwrap();
        let levels: Vec<_> = result
            .iter()
            .filter_map(|series| series.labels.get("level"))
            .cloned()
            .collect();
        assert_eq!(levels, vec!["error".to_string(), "info".to_string()]);

        let logql::QueryExpr::Metric(expr) = logql::parse_expr(
            r#"sum by (app_extracted) (count_over_time({app="api"} | json | app="api" [5s]))"#,
        )
        .unwrap() else {
            panic!("expected metric")
        };
        let result = run_metric_query(state.clone(), test_tenant(), expr, vec![10_000_000_000])
            .await
            .unwrap();
        assert!(
            result
                .iter()
                .any(|series| { series.labels.get("app_extracted") == Some(&"line".to_string()) })
        );

        let logql::QueryExpr::Metric(expr) =
            logql::parse_expr(r#"sum by (__error__) (count_over_time({app="api"} | json [5s]))"#)
                .unwrap()
        else {
            panic!("expected metric")
        };
        let result = run_metric_query(state, test_tenant(), expr, vec![10_000_000_000])
            .await
            .unwrap();
        assert!(result.iter().any(|series| {
            series.labels.get(logql::PARSER_ERROR_FIELD) == Some(&"JSONParserErr".to_string())
                && series.samples == vec![(10_000_000_000, 1.0)]
        }));
    }

    #[tokio::test]
    async fn metric_lookback_restores_only_exact_field_candidates() {
        let data_dir = temp_dir();
        let parts_root = data_dir.join("parts");
        let storage = Arc::new(crate::object_storage::ObjectStorage::in_memory());
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let wanted = part::flush_rows(
            vec![Row {
                tenant: test_tenant(),
                timestamp_ns: 6_000_000_000,
                labels: labels.clone(),
                line: "wanted".to_string(),
                structured_metadata: vec![("tenant".to_string(), "one".to_string())],
            }],
            &parts_root,
            1,
        )
        .unwrap();
        let unwanted = part::flush_rows(
            vec![Row {
                tenant: test_tenant(),
                timestamp_ns: 7_000_000_000,
                labels,
                line: "unwanted".to_string(),
                structured_metadata: vec![("tenant".to_string(), "two".to_string())],
            }],
            &parts_root,
            1,
        )
        .unwrap();
        let unwanted_path = unwanted[0].data_path();
        let mut published = wanted.clone();
        published.extend(unwanted.clone());
        storage.publish(&published, &[]).await.unwrap();
        let parts = Arc::new(PartRegistry::load_from_disk(&parts_root).unwrap());
        storage
            .evict_cache(&parts_root, 0, &parts.part_dirs())
            .unwrap();
        let state = test_state(
            &data_dir,
            Arc::new(MemTable::new()),
            parts,
            Some(Arc::new(crate::object_storage::RemoteCache::new(
                storage, parts_root,
            ))),
        );
        let logql::QueryExpr::Metric(expr) =
            logql::parse_expr(r#"count_over_time({} | tenant="one" [5s])"#).unwrap()
        else {
            panic!("expected metric")
        };

        let result = run_metric_query(state, test_tenant(), expr, vec![10_000_000_000])
            .await
            .unwrap();
        assert_eq!(result[0].samples, vec![(10_000_000_000, 1.0)]);
        assert!(
            wanted[0].data_path().exists(),
            "lookback part was not restored"
        );
        assert!(
            !unwanted_path.exists(),
            "exact field pruning should happen before remote restoration"
        );
    }

    #[tokio::test]
    async fn synthesized_extracted_field_restores_an_evicted_part_conservatively() {
        let data_dir = temp_dir();
        let parts_root = data_dir.join("parts");
        let storage = Arc::new(crate::object_storage::ObjectStorage::in_memory());
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let flushed = part::flush_rows(
            vec![Row {
                tenant: test_tenant(),
                timestamp_ns: 10,
                labels,
                line: r#"{"foo":"z"}"#.to_string(),
                structured_metadata: vec![
                    ("foo".to_string(), "x".to_string()),
                    ("foo_extracted".to_string(), "y".to_string()),
                ],
            }],
            &parts_root,
            1,
        )
        .unwrap();
        storage.publish(&flushed, &[]).await.unwrap();
        let parts = Arc::new(PartRegistry::load_from_disk(&parts_root).unwrap());
        storage
            .evict_cache(&parts_root, 0, &parts.part_dirs())
            .unwrap();
        assert!(!flushed[0].data_path().exists());
        let state = test_state(
            &data_dir,
            Arc::new(MemTable::new()),
            parts,
            Some(Arc::new(crate::object_storage::RemoteCache::new(
                storage, parts_root,
            ))),
        );
        let parsed = logql::parse(r#"{} | json | foo_extracted_2="z""#).unwrap();

        let result = run_unified_query(state, test_tenant(), parsed, 0, 20, 10, true)
            .await
            .unwrap();
        assert_eq!(result[0].entries[0].line, r#"{"foo":"z"}"#);
        assert!(flushed[0].data_path().exists());
    }

    #[tokio::test]
    async fn invalid_metric_step_and_range_are_client_errors() {
        let data_dir = temp_dir();
        let state = test_state(
            &data_dir,
            Arc::new(MemTable::new()),
            Arc::new(PartRegistry::new()),
            None,
        );
        let bad_step = query_range(
            State(state.clone()),
            crate::tenant::test_tenant_headers(),
            Query(QueryRangeParams {
                query: "rate({}[5m])".to_string(),
                start: Some("5".to_string()),
                end: Some("10".to_string()),
                limit: None,
                direction: None,
                step: Some("0".to_string()),
            }),
        )
        .await;
        let Err(error) = bad_step else {
            panic!("zero step must fail")
        };
        assert_eq!(error.0, StatusCode::BAD_REQUEST);

        let bad_range = query_range(
            State(state),
            crate::tenant::test_tenant_headers(),
            Query(QueryRangeParams {
                query: "rate({}[5m])".to_string(),
                start: Some("10".to_string()),
                end: Some("5".to_string()),
                limit: None,
                direction: None,
                step: Some("0".to_string()),
            }),
        )
        .await;
        let Err(error) = bad_range else {
            panic!("invalid query range must fail")
        };
        assert_eq!(error.0, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn metric_step_accepts_duration_decimal_and_scientific_seconds() {
        assert_eq!(parse_step_ns(Some("250ms")).unwrap(), 250_000_000);
        assert_eq!(parse_step_ns(Some("0.25")).unwrap(), 250_000_000);
        assert_eq!(parse_step_ns(Some("2.5e-1")).unwrap(), 250_000_000);
        assert!(parse_step_ns(Some("NaN")).is_err());
        assert!(parse_step_ns(Some("inf")).is_err());
    }

    #[test]
    fn metric_evaluation_points_are_bounded() {
        assert!(evaluation_times(0, MAX_METRIC_EVALUATION_POINTS as i64, 1).is_err());
        assert!(evaluation_times(0, (MAX_METRIC_EVALUATION_POINTS - 1) as i64, 1).is_ok());
    }

    fn state_with_config(
        config: Config,
        memtable: Arc<MemTable>,
        parts: Arc<PartRegistry>,
    ) -> Arc<AppState> {
        let trace_parts = Arc::new(crate::trace_registry::TraceRegistry::new(
            parts.operation_lock(),
        ));
        crate::test_support::state(
            config.clone(),
            memtable.clone(),
            Arc::new(Journal::spawn(&config, memtable).unwrap()),
            parts,
            trace_parts,
            None,
        )
    }

    fn state_with(
        data_dir: &std::path::Path,
        memtable: Arc<MemTable>,
        parts: Arc<PartRegistry>,
    ) -> Arc<AppState> {
        state_with_config(
            Config {
                data_dir: data_dir.to_path_buf(),
                ..Config::default()
            },
            memtable,
            parts,
        )
    }

    fn tenant_policy_state(
        data_dir: &std::path::Path,
        memtable: Arc<MemTable>,
        parts: Arc<PartRegistry>,
        tenant_policy: Arc<crate::tenant_policy::TenantPolicy>,
    ) -> Arc<AppState> {
        let config = Config {
            data_dir: data_dir.to_path_buf(),
            ..Config::default()
        };
        let trace_parts = Arc::new(crate::trace_registry::TraceRegistry::new(
            parts.operation_lock(),
        ));
        crate::test_support::state_with_tenant_policy(
            config.clone(),
            memtable.clone(),
            Arc::new(Journal::spawn(&config, memtable).unwrap()),
            parts,
            trace_parts,
            None,
            tenant_policy,
        )
    }

    async fn lines_in_last_day(state: Arc<AppState>) -> Vec<String> {
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        let response = query_range(
            State(state),
            crate::tenant::test_tenant_headers(),
            Query(QueryRangeParams {
                query: "{app=\"api\"}".to_string(),
                start: Some((now_ns - 86_400_000_000_000).to_string()),
                end: Some(now_ns.to_string()),
                limit: Some(100),
                direction: Some("forward".to_string()),
                step: None,
            }),
        )
        .await
        .unwrap()
        .0;
        response
            .data
            .result
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|stream| stream["values"].as_array().unwrap().clone())
            .map(|value| value[1].as_str().unwrap().to_string())
            .collect()
    }

    /// A downgrade takes effect at query time within one poll, long before the
    /// bytes are reclaimed.
    #[tokio::test]
    async fn a_downgrade_hides_data_before_the_bytes_are_gone() {
        let data_dir = temp_dir();
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        let one_hour_ago = now_ns - 3_600_000_000_000;
        let memtable = Arc::new(MemTable::new());
        memtable.insert(
            test_tenant(),
            [("app".to_string(), "api".to_string())]
                .into_iter()
                .collect(),
            vec![LogEntry {
                timestamp_ns: one_hour_ago,
                line: "an hour old".to_string(),
                structured_metadata: Vec::new(),
            }],
        );
        let policy = Arc::new(crate::tenant_policy::TenantPolicy::enabled_for_test());
        let state = tenant_policy_state(
            &data_dir,
            memtable,
            Arc::new(PartRegistry::new()),
            policy.clone(),
        );

        // No snapshot yet: reads fail open, so the control plane is never on
        // the query hot path.
        assert_eq!(lines_in_last_day(state.clone()).await.len(), 1);

        policy.install_for_test(
            [(
                test_tenant(),
                crate::tenant_policy::TenantRetention::Finite(std::time::Duration::from_secs(60)),
            )]
            .into_iter()
            .collect(),
        );
        assert!(lines_in_last_day(state.clone()).await.is_empty());

        // An upgrade brings back everything still on disk.
        policy.install_for_test(
            [(
                test_tenant(),
                crate::tenant_policy::TenantRetention::Finite(std::time::Duration::from_secs(
                    7 * 24 * 60 * 60,
                )),
            )]
            .into_iter()
            .collect(),
        );
        assert_eq!(lines_in_last_day(state).await.len(), 1);
    }

    /// A cumulative latency sum cannot produce a quantile, and every target in
    /// the plan documents is a p95 or p99. The histogram has to be in the shape
    /// `histogram_quantile` reads: cumulative in `le`, with a `+Inf` bucket
    /// that equals `_count`.
    #[tokio::test]
    async fn the_latency_histogram_is_shaped_for_histogram_quantile() {
        let data_dir = temp_dir();
        let state = state_with(
            &data_dir,
            Arc::new(MemTable::new()),
            Arc::new(PartRegistry::new()),
        );
        for millis in [0, 3, 40, 900] {
            state
                .metrics
                .query_latency
                .observe(std::time::Duration::from_millis(millis));
        }

        let rendered = metrics(State(state)).await;
        let bucket = |bound: &str| -> u64 {
            let needle = format!("loggytracy_query_latency_ms_bucket{{le=\"{bound}\"}} ");
            rendered
                .lines()
                .find_map(|line| line.strip_prefix(&needle))
                .unwrap_or_else(|| panic!("no bucket {bound} in:\n{rendered}"))
                .trim()
                .parse()
                .unwrap()
        };

        assert_eq!(bucket("1"), 1, "only the 0 ms observation is <= 1 ms");
        assert_eq!(bucket("5"), 2);
        assert_eq!(bucket("50"), 3);
        assert_eq!(bucket("1000"), 4);
        assert_eq!(bucket("+Inf"), 4);

        // Monotonic in `le`, which is what makes the series a valid histogram.
        let mut previous = 0;
        for bound in ["1", "5", "10", "25", "50", "100", "250", "500", "1000", "+Inf"] {
            let current = bucket(bound);
            assert!(current >= previous, "bucket {bound} went backwards");
            previous = current;
        }
        assert!(rendered.contains("loggytracy_query_latency_ms_count 4"));
        assert!(rendered.contains("# TYPE loggytracy_query_latency_ms histogram"));
        assert!(rendered.contains("loggytracy_build_info{"));
    }

    /// The scrape renders whatever the retention worker last published. It
    /// deliberately does not recompute the number: that walk is
    /// `unknown_tenant_count`'s, and its own coverage lives with it.
    #[tokio::test]
    async fn the_unknown_tenant_gauge_renders_the_published_value() {
        let data_dir = temp_dir();
        let policy = Arc::new(crate::tenant_policy::TenantPolicy::enabled_for_test());
        let state = tenant_policy_state(
            &data_dir,
            Arc::new(MemTable::new()),
            Arc::new(PartRegistry::new()),
            policy,
        );
        state
            .metrics
            .unknown_tenants
            .store(3, std::sync::atomic::Ordering::Relaxed);

        let rendered = metrics(State(state)).await;
        assert!(
            rendered.contains("loggytracy_tenant_policy_unknown_tenants 3\n"),
            "{rendered}"
        );
    }

    /// Grafana sends `start`/`end` on every label call. Answering from the
    /// whole history both returns labels that do not exist in the requested
    /// range and reads every part to find them.
    #[tokio::test]
    async fn metadata_endpoints_honour_the_requested_time_range() {
        let data_dir = temp_dir();
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        let hour_ns = 3_600_000_000_000i64;
        let memtable = Arc::new(MemTable::new());
        for (app, timestamp_ns) in [("recent", now_ns - hour_ns), ("ancient", now_ns - 48 * hour_ns)]
        {
            memtable.insert(
                test_tenant(),
                [("app".to_string(), app.to_string())].into_iter().collect(),
                vec![LogEntry {
                    timestamp_ns,
                    line: format!("{app} line"),
                    structured_metadata: Vec::new(),
                }],
            );
        }
        let state = state_with(&data_dir, memtable, Arc::new(PartRegistry::new()));

        let window = |start_ns: i64| crate::query::MetadataParams {
            start: Some(start_ns.to_string()),
            end: Some(now_ns.to_string()),
        };
        let values_in = async |params| {
            label_values(
                State(state.clone()),
                crate::tenant::test_tenant_headers(),
                Path("app".to_string()),
                Query(params),
            )
            .await
            .unwrap()
            .0
            .data
        };

        assert_eq!(
            values_in(window(now_ns - 2 * hour_ns)).await,
            vec!["recent".to_string()],
            "a value outside the range must not be offered"
        );
        let mut both = values_in(window(now_ns - 72 * hour_ns)).await;
        both.sort();
        assert_eq!(both, vec!["ancient".to_string(), "recent".to_string()]);

        // A range entirely in the future is an empty answer, not an error:
        // a dashboard asks this whenever its window outruns the data.
        let empty = crate::query::MetadataParams {
            start: Some((now_ns + hour_ns).to_string()),
            end: Some((now_ns + 2 * hour_ns).to_string()),
        };
        assert!(values_in(empty).await.is_empty());
    }

    /// Every `match[]` is another full pass, so the count is a multiplier the
    /// client picks. Left uncapped it was the cheapest way to make the server
    /// do unbounded work.
    #[tokio::test]
    async fn series_refuses_more_matchers_than_the_limit() {
        let data_dir = temp_dir();
        let config = crate::config::Config {
            data_dir: data_dir.clone(),
            max_series_matchers: 2,
            ..crate::config::Config::default()
        };
        config.validate().unwrap();
        let state = state_with_config(config, Arc::new(MemTable::new()), Arc::new(PartRegistry::new()));

        let one = "match%5B%5D=%7B%7D";
        let accepted = series(
            State(state.clone()),
            crate::tenant::test_tenant_headers(),
            RawQuery(Some([one, one].join("&"))),
        )
        .await;
        assert!(accepted.is_ok());

        let refused = match series(
            State(state),
            crate::tenant::test_tenant_headers(),
            RawQuery(Some([one, one, one].join("&"))),
        )
        .await
        {
            Err(error) => error,
            Ok(_) => panic!("more matchers than the limit must be refused"),
        };
        assert_eq!(refused.0, StatusCode::BAD_REQUEST);
        assert!(refused.1.contains("match[]"), "{}", refused.1);
    }

    /// A tenant the control plane never mentioned reads its full history.
    #[tokio::test]
    async fn an_unknown_tenant_is_never_clamped() {
        let data_dir = temp_dir();
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        let memtable = Arc::new(MemTable::new());
        memtable.insert(
            test_tenant(),
            [("app".to_string(), "api".to_string())]
                .into_iter()
                .collect(),
            vec![LogEntry {
                timestamp_ns: now_ns - 3_600_000_000_000,
                line: "an hour old".to_string(),
                structured_metadata: Vec::new(),
            }],
        );
        let policy = Arc::new(crate::tenant_policy::TenantPolicy::enabled_for_test());
        policy.install_for_test(
            [(
                crate::tenant::TenantId::parse("someone-else").unwrap(),
                crate::tenant_policy::TenantRetention::Finite(std::time::Duration::from_secs(1)),
            )]
            .into_iter()
            .collect(),
        );
        let state =
            tenant_policy_state(&data_dir, memtable, Arc::new(PartRegistry::new()), policy);

        assert_eq!(lines_in_last_day(state).await.len(), 1);
    }

    /// A metric query's first evaluation point looks back past the requested
    /// start, so the scan start is raised too.
    #[tokio::test]
    async fn a_metric_lookback_cannot_reach_below_the_retention_floor() {
        let data_dir = temp_dir();
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        let memtable = Arc::new(MemTable::new());
        memtable.insert(
            test_tenant(),
            [("app".to_string(), "api".to_string())]
                .into_iter()
                .collect(),
            vec![LogEntry {
                // Inside the 1h lookback of the evaluation point, but outside
                // the 60s retention floor.
                timestamp_ns: now_ns - 1_800_000_000_000,
                line: "half an hour old".to_string(),
                structured_metadata: Vec::new(),
            }],
        );
        let policy = Arc::new(crate::tenant_policy::TenantPolicy::enabled_for_test());
        policy.install_for_test(
            [(
                test_tenant(),
                crate::tenant_policy::TenantRetention::Finite(std::time::Duration::from_secs(60)),
            )]
            .into_iter()
            .collect(),
        );
        let state =
            tenant_policy_state(&data_dir, memtable, Arc::new(PartRegistry::new()), policy);

        let response = query_range(
            State(state),
            crate::tenant::test_tenant_headers(),
            Query(QueryRangeParams {
                query: "count_over_time({app=\"api\"}[1h])".to_string(),
                start: Some(now_ns.to_string()),
                end: Some(now_ns.to_string()),
                limit: None,
                direction: None,
                step: Some("60".to_string()),
            }),
        )
        .await
        .unwrap()
        .0;

        assert!(
            response.data.result.as_array().unwrap().is_empty(),
            "an expired row must not reach the metric evaluator through the lookback window"
        );
    }

    /// `labels`, `label_values`, `series` and `index_stats` have no range of
    /// their own, so they inherit the clamp directly. Memtable entries are
    /// filtered per entry; parts are pruned per part, which is the finest
    /// granularity the stream index supports.
    #[tokio::test]
    async fn label_and_stats_endpoints_inherit_the_retention_clamp() {
        let data_dir = temp_dir();
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        let memtable = Arc::new(MemTable::new());
        memtable.insert(
            test_tenant(),
            [("app".to_string(), "expired".to_string())]
                .into_iter()
                .collect(),
            vec![LogEntry {
                timestamp_ns: now_ns - 3_600_000_000_000,
                line: "an hour old".to_string(),
                structured_metadata: Vec::new(),
            }],
        );
        memtable.insert(
            test_tenant(),
            [("app".to_string(), "fresh".to_string())]
                .into_iter()
                .collect(),
            vec![LogEntry {
                timestamp_ns: now_ns,
                line: "just now".to_string(),
                structured_metadata: Vec::new(),
            }],
        );
        let policy = Arc::new(crate::tenant_policy::TenantPolicy::enabled_for_test());
        policy.install_for_test(
            [(
                test_tenant(),
                crate::tenant_policy::TenantRetention::Finite(std::time::Duration::from_secs(60)),
            )]
            .into_iter()
            .collect(),
        );
        let state = tenant_policy_state(
            &data_dir,
            memtable,
            Arc::new(PartRegistry::new()),
            policy.clone(),
        );

        let values = label_values(
            State(state.clone()),
            crate::tenant::test_tenant_headers(),
            Path("app".to_string()),
            Query(Default::default()),
        )
        .await
        .unwrap()
        .0
        .data;
        assert_eq!(values, vec!["fresh".to_string()]);

        let stats = index_stats(
            State(state.clone()),
            crate::tenant::test_tenant_headers(),
            Query(Default::default()),
        )
            .await
            .unwrap()
            .0;
        assert_eq!(stats["data"]["entries"], 1);
        assert_eq!(stats["data"]["streams"], 1);

        let matched = series(
            State(state),
            crate::tenant::test_tenant_headers(),
            RawQuery(Some("match%5B%5D=%7B%7D".to_string())),
        )
        .await
        .unwrap()
        .0
        .data;
        assert_eq!(matched.len(), 1);
        assert_eq!(matched[0]["app"], "fresh");
    }

    fn tail_labels() -> Labels {
        [("app".to_string(), "tailed".to_string())]
            .into_iter()
            .collect()
    }

    fn tail_entry(timestamp_ns: i64, line: &str) -> LogEntry {
        LogEntry {
            timestamp_ns,
            line: line.to_string(),
            structured_metadata: vec![],
        }
    }

    fn tail_query() -> logql::LogQuery {
        match logql::parse_expr(r#"{app="tailed"}"#).unwrap() {
            logql::QueryExpr::Logs(query) => query,
            logql::QueryExpr::Metric(_) => unreachable!("the fixture is a log selector"),
        }
    }

    /// A tail must send each line exactly once. A cursor alone cannot do that:
    /// advancing past the newest timestamp drops every other entry sharing that
    /// nanosecond, and stopping on it resends them. Lines written as one batch
    /// routinely share a timestamp, so this is the ordinary case, not an edge.
    #[tokio::test]
    async fn a_tail_sends_each_line_once_even_when_timestamps_collide() {
        let dir = temp_dir();
        let memtable = Arc::new(MemTable::new());
        let state = test_state(&dir, memtable.clone(), Arc::new(PartRegistry::new()), None);

        let collision_ns = 1_700_000_000_000_000_000;
        memtable.insert(
            test_tenant(),
            tail_labels(),
            vec![
                tail_entry(collision_ns, "first"),
                tail_entry(collision_ns, "second"),
            ],
        );

        let mut cursor = TailCursor::new(collision_ns - 1);
        let query = tail_query();
        let first = tail_poll(&state, &test_tenant(), &query, &mut cursor, collision_ns, 100)
            .await
            .expect("the first poll delivers both lines");
        let lines = tail_lines(&first);
        assert_eq!(lines, vec!["first", "second"]);

        // Nothing new: the same window must not resend what it already sent.
        assert!(
            tail_poll(&state, &test_tenant(), &query, &mut cursor, collision_ns, 100)
                .await
                .is_none(),
            "a repeated poll over the same window has nothing to send"
        );

        // A third line at the very same nanosecond still has to arrive.
        memtable.insert(
            test_tenant(),
            tail_labels(),
            vec![tail_entry(collision_ns, "third")],
        );
        let third = tail_poll(&state, &test_tenant(), &query, &mut cursor, collision_ns, 100)
            .await
            .expect("a late arrival at the same timestamp is not lost");
        assert_eq!(tail_lines(&third), vec!["third"]);
    }

    /// A burst larger than one poll's limit is left for the next poll, not
    /// skipped. The tail falls behind rather than losing lines, which is what
    /// makes reporting an empty `dropped_entries` true rather than a stub.
    #[tokio::test]
    async fn a_burst_larger_than_the_limit_is_delivered_across_polls() {
        let dir = temp_dir();
        let memtable = Arc::new(MemTable::new());
        let state = test_state(&dir, memtable.clone(), Arc::new(PartRegistry::new()), None);

        let base_ns = 1_700_000_000_000_000_000;
        let entries: Vec<LogEntry> = (0..5)
            .map(|index| tail_entry(base_ns + index, &format!("line-{index}")))
            .collect();
        memtable.insert(test_tenant(), tail_labels(), entries);

        let mut cursor = TailCursor::new(base_ns - 1);
        let query = tail_query();
        let end_ns = base_ns + 10;

        let first = tail_poll(&state, &test_tenant(), &query, &mut cursor, end_ns, 2)
            .await
            .expect("the oldest two arrive first");
        assert_eq!(tail_lines(&first), vec!["line-0", "line-1"]);
        assert_eq!(first["dropped_entries"].as_array().unwrap().len(), 0);

        let second = tail_poll(&state, &test_tenant(), &query, &mut cursor, end_ns, 2)
            .await
            .expect("the next poll continues rather than skipping ahead");
        assert_eq!(tail_lines(&second), vec!["line-2", "line-3"]);

        let third = tail_poll(&state, &test_tenant(), &query, &mut cursor, end_ns, 2)
            .await
            .expect("and drains the rest");
        assert_eq!(tail_lines(&third), vec!["line-4"]);
    }

    fn tail_lines(payload: &serde_json::Value) -> Vec<String> {
        payload["streams"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|stream| stream["values"].as_array().unwrap().iter())
            .map(|value| value[1].as_str().unwrap().to_string())
            .collect()
    }

    /// Perform a real WebSocket handshake against a real listener and return
    /// the status line.
    ///
    /// `oneshot` cannot reach this handler: the upgrade extractor needs the
    /// `OnUpgrade` extension that only a live hyper connection attaches, so a
    /// hand-built request is rejected before any of the handler's own
    /// validation runs. A test built that way would pass without proving
    /// anything about the endpoint.
    async fn tail_handshake_status(state: Arc<AppState>, query: &str) -> u16 {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, crate::build_router(state)).await.ok();
        });

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let request = format!(
            "GET /loki/api/v1/tail?query={query} HTTP/1.1\r\n\
Host: localhost\r\n\
{}: {}\r\n\
Connection: Upgrade\r\n\
Upgrade: websocket\r\n\
Sec-WebSocket-Version: 13\r\n\
Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n",
            crate::tenant::TENANT_HEADER,
            test_tenant().as_str(),
        );
        stream.write_all(request.as_bytes()).await.unwrap();

        let mut response = vec![0u8; 256];
        let read = stream.read(&mut response).await.unwrap();
        let status_line = String::from_utf8_lossy(&response[..read]);
        let status: u16 = status_line
            .split_whitespace()
            .nth(1)
            .expect("a status line")
            .parse()
            .expect("a status code");
        drop(stream);
        server.abort();
        status
    }

    /// Everything a tail can reject is rejected before the upgrade. A client
    /// that gets a 101 followed by an immediate close cannot tell a bad query
    /// from a server fault.
    #[tokio::test]
    async fn a_tail_rejects_what_it_cannot_follow_before_upgrading() {
        let dir = temp_dir();
        let memtable = Arc::new(MemTable::new());
        let state = test_state(&dir, memtable, Arc::new(PartRegistry::new()), None);

        assert_eq!(
            tail_handshake_status(state.clone(), "%7Bapp%3D").await,
            400,
            "an unparseable selector"
        );
        assert_eq!(
            tail_handshake_status(
                state.clone(),
                "rate(%7Bapp%3D%22tailed%22%7D%5B5m%5D)"
            )
            .await,
            400,
            "a metric expression has no stream to follow"
        );
        assert_eq!(
            tail_handshake_status(state, "%7Bapp%3D%22tailed%22%7D").await,
            101,
            "and a followable query upgrades"
        );
    }

    /// A tail is a poll loop with an open socket, so the count of them is
    /// bounded and the refusal happens where the client can read it.
    #[tokio::test]
    async fn a_tail_beyond_the_connection_limit_is_refused_with_429() {
        let dir = temp_dir();
        let memtable = Arc::new(MemTable::new());
        let state = test_state(&dir, memtable, Arc::new(PartRegistry::new()), None);

        let held: Vec<_> = (0..state.config.max_concurrent_tails)
            .map(|_| {
                Arc::clone(&state.tail_semaphore)
                    .try_acquire_owned()
                    .expect("the limit is not reached yet")
            })
            .collect();

        assert_eq!(
            tail_handshake_status(state.clone(), "%7Bapp%3D%22tailed%22%7D").await,
            429
        );

        // And it is the limit doing it, not the endpoint: the same request
        // upgrades once a slot frees.
        drop(held);
        assert_eq!(
            tail_handshake_status(state, "%7Bapp%3D%22tailed%22%7D").await,
            101
        );
    }

    async fn get_json(state: &Arc<AppState>, uri: &str) -> (StatusCode, serde_json::Value) {
        let request = axum::http::Request::builder()
            .uri(uri)
            .header(crate::tenant::TENANT_HEADER, test_tenant().as_str())
            .body(axum::body::Body::empty())
            .unwrap();
        let response = crate::build_router(state.clone())
            .oneshot(request)
            .await
            .unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        (
            status,
            serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null),
        )
    }

    fn volume_state() -> (Arc<AppState>, i64) {
        let dir = temp_dir();
        let memtable = Arc::new(MemTable::new());
        let state = test_state(&dir, memtable.clone(), Arc::new(PartRegistry::new()), None);
        let base_ns = 1_700_000_000_000_000_000;
        for (app, line) in [("a", "aaaa"), ("a", "bbbbbb"), ("b", "cc")] {
            memtable.insert(
                test_tenant(),
                [("app".to_string(), app.to_string())]
                    .into_iter()
                    .collect::<Labels>(),
                vec![LogEntry {
                    timestamp_ns: base_ns,
                    line: line.to_string(),
                    structured_metadata: vec![],
                }],
            );
        }
        (state, base_ns)
    }

    /// Volume is `bytes_over_time` under a different name, so it is answered by
    /// the metric evaluator rather than by a scan of its own. That is what puts
    /// it inside the same scan budgets, retention clamp and tenant scope.
    #[tokio::test]
    async fn index_volume_reports_bytes_per_stream() {
        let (state, base_ns) = volume_state();

        let (status, body) = get_json(
            &state,
            &format!(
                "/loki/api/v1/index/volume?query=%7B%7D&start={}&end={}",
                base_ns - 1_000_000_000,
                base_ns + 1_000_000_000
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["data"]["resultType"], "vector");

        let mut by_app: BTreeMap<String, f64> = BTreeMap::new();
        for series in body["data"]["result"].as_array().unwrap() {
            by_app.insert(
                series["metric"]["app"].as_str().unwrap().to_string(),
                series["value"][1].as_str().unwrap().parse().unwrap(),
            );
        }
        // "aaaa" + "bbbbbb" against "cc".
        assert_eq!(by_app["a"], 10.0);
        assert_eq!(by_app["b"], 2.0);
    }

    /// The histogram above Explore's results is the ranged form, which has to
    /// come back as a matrix for Grafana to plot it — one point per step that
    /// has data, so the bars line up with when the lines were written.
    #[tokio::test]
    async fn index_volume_range_returns_a_matrix() {
        let dir = temp_dir();
        let memtable = Arc::new(MemTable::new());
        let state = test_state(&dir, memtable.clone(), Arc::new(PartRegistry::new()), None);
        let base_ns = 1_700_000_000_000_000_000;
        for offset in 0..3i64 {
            memtable.insert(
                test_tenant(),
                [("app".to_string(), "a".to_string())]
                    .into_iter()
                    .collect::<Labels>(),
                vec![LogEntry {
                    timestamp_ns: base_ns + offset * 1_000_000_000,
                    line: "aaaa".to_string(),
                    structured_metadata: vec![],
                }],
            );
        }

        let (status, body) = get_json(
            &state,
            &format!(
                "/loki/api/v1/index/volume_range?query=%7Bapp%3D%22a%22%7D&start={}&end={}&step=1s",
                base_ns,
                base_ns + 2_000_000_000
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["data"]["resultType"], "matrix");
        let series = body["data"]["result"].as_array().unwrap();
        assert_eq!(series.len(), 1, "the selector narrowed it to one stream");
        assert_eq!(series[0]["metric"]["app"], "a");
        let values = series[0]["values"].as_array().unwrap();
        assert_eq!(
            values.len(),
            3,
            "one bucket per second, each holding its own line: {}",
            series[0]
        );
        for value in values {
            assert_eq!(value[1], "4", "each bucket sees exactly one four-byte line");
        }
    }

    /// `targetLabels` aggregates, which is how Explore asks for volume by one
    /// dimension rather than per stream.
    #[tokio::test]
    async fn index_volume_aggregates_by_target_labels() {
        let (state, base_ns) = volume_state();
        let (status, body) = get_json(
            &state,
            &format!(
                "/loki/api/v1/index/volume?query=%7B%7D&start={}&end={}&targetLabels=app&limit=1",
                base_ns - 1_000_000_000,
                base_ns + 1_000_000_000
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let series = body["data"]["result"].as_array().unwrap();
        assert_eq!(series.len(), 1, "limit caps the series returned");
        assert_eq!(
            series[0]["metric"]["app"], "a",
            "and it keeps the largest, not an arbitrary one"
        );
    }

    /// The filter sidebar in Grafana 11+ reads this. Cardinality is what makes
    /// it useful, and it comes from the same sources `labels` answers from.
    #[tokio::test]
    async fn detected_labels_reports_cardinality_per_label() {
        let (state, _base_ns) = volume_state();
        let (status, body) = get_json(&state, "/loki/api/v1/detected_labels").await;
        assert_eq!(status, StatusCode::OK);
        let labels = body["detectedLabels"].as_array().unwrap();
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0]["label"], "app");
        assert_eq!(labels[0]["cardinality"], 2);
    }

    /// Detected fields come from structured metadata, with a type guessed from
    /// the values actually seen. The guess is a hint, so values that disagree
    /// fall back to string rather than to the first one that parsed.
    #[tokio::test]
    async fn detected_fields_reports_structured_metadata_with_a_type() {
        let dir = temp_dir();
        let memtable = Arc::new(MemTable::new());
        let state = test_state(&dir, memtable.clone(), Arc::new(PartRegistry::new()), None);
        memtable.insert(
            test_tenant(),
            [("app".to_string(), "fields".to_string())]
                .into_iter()
                .collect::<Labels>(),
            vec![
                LogEntry {
                    timestamp_ns: 1_700_000_000_000_000_000,
                    line: "one".to_string(),
                    structured_metadata: vec![
                        ("status".to_string(), "200".to_string()),
                        ("trace_id".to_string(), "abc".to_string()),
                    ],
                },
                LogEntry {
                    timestamp_ns: 1_700_000_001_000_000_000,
                    line: "two".to_string(),
                    structured_metadata: vec![
                        ("status".to_string(), "500".to_string()),
                        ("trace_id".to_string(), "def".to_string()),
                    ],
                },
            ],
        );

        let (status, body) = get_json(&state, "/loki/api/v1/detected_fields").await;
        assert_eq!(status, StatusCode::OK);
        let fields: BTreeMap<String, serde_json::Value> = body["fields"]
            .as_array()
            .unwrap()
            .iter()
            .map(|field| (field["label"].as_str().unwrap().to_string(), field.clone()))
            .collect();
        assert_eq!(fields["status"]["type"], "int");
        assert_eq!(fields["status"]["cardinality"], 2);
        assert_eq!(fields["trace_id"]["type"], "string");
    }

    /// The format button validates. It deliberately does not rewrite: an
    /// invalid query is a 400 with the parse error, and a valid one comes back
    /// as sent rather than as something the client did not write.
    #[tokio::test]
    async fn format_query_validates_without_rewriting() {
        let dir = temp_dir();
        let memtable = Arc::new(MemTable::new());
        let state = test_state(&dir, memtable, Arc::new(PartRegistry::new()), None);

        let (status, body) =
            get_json(&state, "/loki/api/v1/format_query?query=%20%7Bapp%3D%22a%22%7D%20").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["data"], r#"{app="a"}"#);

        let (status, _) = get_json(&state, "/loki/api/v1/format_query?query=%7Bapp%3D").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    /// The quota reaches the HTTP query path, and it answers 429 rather than
    /// 500 — the difference between "come back later" and "this is broken".
    #[tokio::test]
    async fn a_tenant_over_its_query_quota_is_refused_with_429() {
        let dir = temp_dir();
        let memtable = Arc::new(MemTable::new());
        let config = Config {
            data_dir: dir.clone(),
            default_tenant_query_scan_bytes_per_second: Some(1),
            tenant_ingest_burst: std::time::Duration::from_nanos(1),
            max_push_bytes: 1,
            ..Config::default()
        };
        // Backed by a part, not the memtable: the scan budget is charged on
        // bytes read out of parts, which is where a query's real cost is.
        let parts = Arc::new(PartRegistry::new());
        parts
            .register(
                part::flush_rows(
                    vec![Row {
                        tenant: test_tenant(),
                        timestamp_ns: 1_700_000_000_000_000_000,
                        labels: [("app".to_string(), "quota".to_string())]
                            .into_iter()
                            .collect::<Labels>(),
                        line: "a line long enough to cost some scan budget".to_string(),
                        structured_metadata: Vec::new(),
                    }],
                    &dir.join("parts"),
                    config.row_group_size,
                )
                .unwrap(),
            )
            .unwrap();
        let trace_parts = Arc::new(crate::trace_registry::TraceRegistry::standalone());
        let state = crate::test_support::state(
            config.clone(),
            memtable.clone(),
            Arc::new(Journal::spawn(&config, memtable).unwrap()),
            parts,
            trace_parts,
            None,
        );

        let uri = "/loki/api/v1/query_range?query=%7Bapp%3D%22quota%22%7D";
        let (first, _) = get_json(&state, uri).await;
        assert_eq!(first, StatusCode::OK, "the first query runs and pays after");

        let (second, body) = get_json(&state, uri).await;
        assert_eq!(
            second,
            StatusCode::TOO_MANY_REQUESTS,
            "the next query pays for the last one: {body}"
        );
        assert_eq!(
            state
                .metrics
                .query_quota_rejected
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "counted apart from queries this instance failed to answer"
        );
        assert_eq!(
            state
                .metrics
                .query_errors
                .load(std::sync::atomic::Ordering::Relaxed),
            0,
            "a refused query is not a failed one"
        );
    }

    fn unwrapped_entries(values: &[(i64, &str)]) -> Vec<(Labels, LogEntry)> {
        let labels: Labels = [("app".to_string(), "u".to_string())].into_iter().collect();
        values
            .iter()
            .map(|(timestamp_ns, latency)| {
                (
                    labels.clone(),
                    LogEntry {
                        timestamp_ns: *timestamp_ns,
                        line: format!("latency={latency}"),
                        // The pipeline leaves its fields here, which is where
                        // the unwrap reads them from.
                        structured_metadata: vec![("latency".to_string(), latency.to_string())],
                    },
                )
            })
            .collect()
    }

    #[test]
    fn the_value_functions_aggregate_unwrapped_samples() {
        let entries = unwrapped_entries(&[
            (1_000_000_000, "10"),
            (2_000_000_000, "20"),
            (3_000_000_000, "60"),
        ]);
        for (query, expected) in [
            (r#"sum_over_time({app="u"} | unwrap latency [10s])"#, 90.0),
            (r#"avg_over_time({app="u"} | unwrap latency [10s])"#, 30.0),
            (r#"min_over_time({app="u"} | unwrap latency [10s])"#, 10.0),
            (r#"max_over_time({app="u"} | unwrap latency [10s])"#, 60.0),
        ] {
            let logql::QueryExpr::Metric(expr) = logql::parse_expr(query).unwrap() else {
                panic!("expected metric")
            };
            let values = evaluate_metric_at(&expr, &entries, 5_000_000_000);
            assert_eq!(values.len(), 1, "{query}");
            assert!(
                (values[0].1 - expected).abs() < 1e-9,
                "{query}: {} != {expected}",
                values[0].1
            );
        }
    }

    /// Interpolated between the two nearest ranks, as Prometheus and Loki
    /// report it. Picking the nearest sample instead would make a p99 over a
    /// handful of points wrong in a different way for every window size.
    #[test]
    fn quantile_over_time_interpolates_between_ranks() {
        let entries = unwrapped_entries(&[
            (1_000_000_000, "0"),
            (2_000_000_000, "10"),
            (3_000_000_000, "20"),
            (4_000_000_000, "30"),
        ]);
        let logql::QueryExpr::Metric(expr) =
            logql::parse_expr(r#"quantile_over_time(0.5, {app="u"} | unwrap latency [10s])"#)
                .unwrap()
        else {
            panic!("expected metric")
        };
        let values = evaluate_metric_at(&expr, &entries, 5_000_000_000);
        assert_eq!(values.len(), 1);
        assert!(
            (values[0].1 - 15.0).abs() < 1e-9,
            "median of 0,10,20,30 is 15, got {}",
            values[0].1
        );

        let logql::QueryExpr::Metric(p100) =
            logql::parse_expr(r#"quantile_over_time(1, {app="u"} | unwrap latency [10s])"#).unwrap()
        else {
            panic!("expected metric")
        };
        assert_eq!(evaluate_metric_at(&p100, &entries, 5_000_000_000)[0].1, 30.0);
    }

    /// An entry whose unwrap field does not convert is dropped from the sample
    /// set. Counting it as zero would drag an average toward a value nothing
    /// measured.
    #[test]
    fn unconvertible_samples_are_dropped_not_zeroed() {
        let mut entries = unwrapped_entries(&[(1_000_000_000, "10"), (2_000_000_000, "20")]);
        let labels = entries[0].0.clone();
        entries.push((
            labels,
            LogEntry {
                timestamp_ns: 3_000_000_000,
                line: "latency=slow".into(),
                structured_metadata: vec![("latency".to_string(), "slow".to_string())],
            },
        ));
        let logql::QueryExpr::Metric(expr) =
            logql::parse_expr(r#"avg_over_time({app="u"} | unwrap latency [10s])"#).unwrap()
        else {
            panic!("expected metric")
        };
        let values = evaluate_metric_at(&expr, &entries, 5_000_000_000);
        assert!(
            (values[0].1 - 15.0).abs() < 1e-9,
            "average of 10 and 20 is 15, not 10: {}",
            values[0].1
        );
    }

    /// The window still slides. A value function is not exempt from the range
    /// bound just because it aggregates values rather than counts.
    #[test]
    fn a_value_function_respects_the_range_window() {
        let entries = unwrapped_entries(&[(1_000_000_000, "10"), (9_000_000_000, "90")]);
        let logql::QueryExpr::Metric(expr) =
            logql::parse_expr(r#"sum_over_time({app="u"} | unwrap latency [5s])"#).unwrap()
        else {
            panic!("expected metric")
        };
        assert_eq!(evaluate_metric_at(&expr, &entries, 10_000_000_000)[0].1, 90.0);
    }

    /// An offset shifts the window back without moving the point it is
    /// reported at. That is what makes `rate(…[5m] offset 1h)` plottable
    /// against an un-offset series on the same axis.
    #[tokio::test]
    async fn an_offset_shifts_the_window_not_the_evaluation_point() {
        let entries = unwrapped_entries(&[(1_000_000_000, "10"), (61_000_000_000, "99")]);
        let logql::QueryExpr::Metric(offset) =
            logql::parse_expr(r#"sum_over_time({app="u"} | unwrap latency [5s] offset 60s)"#)
                .unwrap()
        else {
            panic!("expected metric")
        };
        // At t=62s the un-offset window holds the second sample; offset by 60s
        // it holds the first.
        let values = evaluate_metric_at(&offset, &entries, 62_000_000_000);
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].1, 10.0, "the offset window sees the older sample");

        let logql::QueryExpr::Metric(plain) =
            logql::parse_expr(r#"sum_over_time({app="u"} | unwrap latency [5s])"#).unwrap()
        else {
            panic!("expected metric")
        };
        assert_eq!(
            evaluate_metric_at(&plain, &entries, 62_000_000_000)[0].1,
            99.0
        );
    }

    /// A subquery evaluates its inner expression on its own step grid inside
    /// the outer window, and aggregates those samples. Evaluating the inner
    /// expression once at the outer point instead would make
    /// `max_over_time(rate(…)[1h:1m])` the rate at one instant rather than the
    /// largest of sixty.
    #[tokio::test]
    async fn a_subquery_aggregates_the_inner_expression_over_its_own_steps() {
        let labels: Labels = [("app".to_string(), "s".to_string())].into_iter().collect();
        // Three entries inside the second step, one inside the tenth. A
        // one-second count_over_time therefore peaks at 3.
        let mut entries: Vec<(Labels, LogEntry)> = Vec::new();
        for offset in [500_000_000, 600_000_000, 700_000_000] {
            entries.push((
                labels.clone(),
                LogEntry {
                    timestamp_ns: 1_000_000_000 + offset,
                    line: "burst".into(),
                    structured_metadata: vec![],
                },
            ));
        }
        entries.push((
            labels.clone(),
            LogEntry {
                timestamp_ns: 9_500_000_000,
                line: "tail".into(),
                structured_metadata: vec![],
            },
        ));

        let logql::QueryExpr::Metric(peak) =
            logql::parse_expr(r#"max_over_time(count_over_time({app="s"}[1s])[12s:1s])"#).unwrap()
        else {
            panic!("expected metric")
        };
        let values = evaluate_metric_stream(&peak, &entries, &[12_000_000_000], None).unwrap();
        let series = values.values().next().expect("one series");
        assert_eq!(series[0].1, 3.0, "the busiest inner step is the maximum");

        let logql::QueryExpr::Metric(minimum) =
            logql::parse_expr(r#"min_over_time(count_over_time({app="s"}[1s])[12s:1s])"#).unwrap()
        else {
            panic!("expected metric")
        };
        let values = evaluate_metric_stream(&minimum, &entries, &[12_000_000_000], None).unwrap();
        assert_eq!(
            values.values().next().unwrap()[0].1,
            1.0,
            "steps with no entries produce no sample, so the minimum is over the steps that did"
        );
    }
