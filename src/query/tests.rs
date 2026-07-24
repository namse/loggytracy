    use super::*;
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
            run_unified_query_with_stats(state.clone(), parsed.clone(), 0, 2, 1, true, None)
                .await
                .unwrap();
        assert_eq!(execution.results[0].entries.len(), 1);
        assert_eq!(execution.scanned_rows, 3);

        let error = match unified_query_with_stats_cancellable(
            &state,
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

        assert_eq!(distinct_stream_count(&state), 1);
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
        remote.mark_remote_unhealthy();
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

        remote.mark_remote_healthy();
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
            .evict_cache(&parts_root, 0, &parts.part_ids())
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
        let result = run_unified_query(state, parsed, i64::MIN, i64::MAX, 10, true)
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
        let eligible = [old[0].meta.id.clone(), new[0].meta.id.clone()]
            .into_iter()
            .collect();
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
        let guard = pin_query_parts_with_gap_hook(&state, &parsed, i64::MIN, i64::MAX, || {
            parts.reload_from_manifest(&parts_root, &manifest)
        })
        .await
        .unwrap();
        let result = unified_query(&state, &parsed, i64::MIN, i64::MAX, 10, true).unwrap();
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

        let result = run_unified_query(state.clone(), parsed, 0, 30, 10, true)
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
                run_unified_query(state.clone(), logql::parse(query).unwrap(), 0, 30, 10, true)
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

        let result = run_unified_query(state, parsed, 0, 30, 10, true)
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

        let result = run_metric_query(state, expr, vec![i64::MIN + 1])
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
        let result = run_metric_query(state.clone(), expr, vec![10_000_000_000])
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
        let result = run_metric_query(state.clone(), expr, vec![10_000_000_000])
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
        let result = run_metric_query(state, expr, vec![10_000_000_000])
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
            .evict_cache(&parts_root, 0, &parts.part_ids())
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

        let result = run_metric_query(state, expr, vec![10_000_000_000])
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
            .evict_cache(&parts_root, 0, &parts.part_ids())
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

        let result = run_unified_query(state, parsed, 0, 20, 10, true)
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
