    use super::*;
    use crate::tenant::test_tenant;
    use crate::config::Config;
    use crate::journal::Journal;
    use crate::memtable::{LogEntry, MemTable};
    use crate::part::{self, Row};
    use crate::part_registry::PartRegistry;
    use tower::ServiceExt;

    /// The stream era attached these pairs as labels; storage now keeps them
    /// as each row's own attributes, which is what selectors match against.
    fn rows_with_attributes(attributes: &Labels, mut rows: Vec<Row>) -> Vec<Row> {
        for row in &mut rows {
            row.structured_metadata
                .extend(attributes.iter().map(|(k, v)| (k.clone(), v.clone())));
            crate::memtable::canonicalize_structured_metadata(&mut row.structured_metadata);
        }
        rows
    }

    /// The same fold for buffered entries.
    fn with_attributes(attributes: Labels, mut entries: Vec<LogEntry>) -> Vec<LogEntry> {
        for entry in &mut entries {
            entry
                .structured_metadata
                .extend(attributes.iter().map(|(k, v)| (k.clone(), v.clone())));
            crate::memtable::canonicalize_structured_metadata(&mut entry.structured_metadata);
        }
        entries
    }

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
        memtable.insert(test_tenant(), with_attributes(labels.clone(), vec![LogEntry {
                timestamp_ns: 5_000_000_000,
                line: "hello from http".to_string(),
                structured_metadata: Vec::new(),
            }]));
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

    /// The window boundary at every level at once: a row on `start` and a row
    /// on `end` in the memtable, the same pair in a part, and one `query_range`
    /// over exactly that window.
    ///
    /// This is the test that was missing. The memtable scan, the part-level
    /// prune, the row-group prune and the row-level reject each spelled the
    /// comparison out for itself, so `end` could be — and was — inclusive on the
    /// log path while Loki's `query_range` is `[start, end)`; nothing caught it
    /// because no test put a row exactly on a boundary. Asserting the two
    /// sources together is what makes a single site drifting back a failure.
    #[tokio::test]
    async fn query_range_includes_start_and_excludes_end_in_the_memtable_and_in_parts() {
        const START_NS: i64 = 1_700_000_000_000_000_000;
        const END_NS: i64 = 1_700_000_001_000_000_000;

        let data_dir = temp_dir();
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let memtable = Arc::new(MemTable::new());
        memtable.insert(test_tenant(), with_attributes(labels.clone(), vec![
                LogEntry {
                    timestamp_ns: START_NS,
                    line: "memtable on start".to_string(),
                    structured_metadata: Vec::new(),
                },
                LogEntry {
                    timestamp_ns: END_NS,
                    line: "memtable on end".to_string(),
                    structured_metadata: Vec::new(),
                },
            ]));
        let parts = Arc::new(PartRegistry::new());
        parts
            .register(
                part::flush_rows(
                    [(START_NS, "part on start"), (END_NS, "part on end")]
                        .into_iter()
                        .map(|(timestamp_ns, line)| Row {
                            tenant: test_tenant(),
                            timestamp_ns,
                            line: line.to_string(),
                            structured_metadata: vec![(
                                "app".to_string(),
                                "api".to_string(),
                            )],
                        })
                        .collect(),
                    &data_dir.join("parts"),
                    // One row per row group, so the row-group prune has to make
                    // the same call the row-level test does.
                    1,
                )
                .unwrap(),
            )
            .unwrap();
        let state = test_state(&data_dir, memtable, parts, None);

        let response = query_range(
            State(state.clone()),
            crate::tenant::test_tenant_headers(),
            Query(QueryRangeParams {
                query: r#"{app="api"}"#.to_string(),
                start: Some(START_NS.to_string()),
                end: Some(END_NS.to_string()),
                limit: Some(100),
                direction: Some("forward".to_string()),
                step: None,
            }),
        )
        .await
        .unwrap()
        .0;
        let mut lines: Vec<String> = response
            .data
            .result
            .to_value()
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|stream| stream["values"].as_array().unwrap().clone())
            .map(|value| value[1].as_str().unwrap().to_string())
            .collect();
        lines.sort();
        assert_eq!(lines, vec!["memtable on start", "part on start"]);

        // The same window one nanosecond wider does return the rows on `end`,
        // so what is being asserted above is the boundary and not a lost row.
        let response = query_range(
            State(state.clone()),
            crate::tenant::test_tenant_headers(),
            Query(QueryRangeParams {
                query: r#"{app="api"}"#.to_string(),
                start: Some(START_NS.to_string()),
                end: Some((END_NS + 1).to_string()),
                limit: Some(100),
                direction: Some("forward".to_string()),
                step: None,
            }),
        )
        .await
        .unwrap()
        .0;
        let lines: usize = response
            .data
            .result
            .to_value()
            .as_array()
            .unwrap()
            .iter()
            .map(|stream| stream["values"].as_array().unwrap().len())
            .sum();
        assert_eq!(lines, 4);

        // The unified scan decides nothing itself: it answers whatever range it
        // is handed, which is what lets one caller own the contract.
        let parsed = logql::parse(r#"{app="api"}"#).unwrap();
        let rows = |range| {
            unified_query(&state, &test_tenant(), &parsed, range, 100, true)
                .unwrap()
                .iter()
                .map(|stream| stream.entries.len())
                .sum::<usize>()
        };
        assert_eq!(rows(part::QueryTimeRange::half_open(START_NS, END_NS)), 2);
        assert_eq!(rows(part::QueryTimeRange::closed(START_NS, END_NS)), 4);
        assert_eq!(rows(part::QueryTimeRange::half_open(END_NS, END_NS)), 0);
    }

    /// A metric scan's `end` is its last evaluation point, not a bound the
    /// client asked to exclude, and the range evaluator's own window closes on
    /// it. So the row that lands exactly there is still counted — the log
    /// window's exclusive `end` must not reach this path.
    #[tokio::test]
    async fn a_metric_query_still_counts_the_row_on_its_last_evaluation_point() {
        const AT_NS: i64 = 1_700_000_001_000_000_000;

        let data_dir = temp_dir();
        let memtable = Arc::new(MemTable::new());
        memtable.insert(
            test_tenant(),
            vec![LogEntry {
                timestamp_ns: AT_NS,
                line: "on the last evaluation point".to_string(),
                structured_metadata: vec![("app".to_string(), "api".to_string())],
            }],
        );
        let state = test_state(&data_dir, memtable, Arc::new(PartRegistry::new()), None);

        let response = query_range(
            State(state),
            crate::tenant::test_tenant_headers(),
            Query(QueryRangeParams {
                query: r#"count_over_time({app="api"}[1s])"#.to_string(),
                start: Some((AT_NS - 1_000_000_000).to_string()),
                end: Some(AT_NS.to_string()),
                limit: None,
                direction: None,
                step: Some("1".to_string()),
            }),
        )
        .await
        .unwrap()
        .0;
        let result = response.data.result.to_value();
        let samples = result[0]["values"].as_array().unwrap();
        let last = samples.last().unwrap();
        assert_eq!(last[0], AT_NS / 1_000_000_000);
        assert_eq!(last[1], "1");
    }

    #[tokio::test]
    async fn omitted_metric_start_scans_the_first_lookback_window() {
        let data_dir = temp_dir();
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let memtable = Arc::new(MemTable::new());
        memtable.insert(test_tenant(), with_attributes(labels.clone(), vec![LogEntry {
                timestamp_ns: 20_000_000_000,
                line: "inside first lookback".to_string(),
                structured_metadata: Vec::new(),
            }]));
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

        assert_eq!(response.data.result.to_value()[0]["values"][0][0], 40);
        assert_eq!(response.data.result.to_value()[0]["values"][0][1], "1");
    }

    /// The output limit bounds the physical scan.
    ///
    /// This test used to assert the opposite, under the name
    /// `query_scan_stats_count_rows_before_applying_output_limit`: three rows
    /// were read to answer a `limit=1`, because the limit could only truncate
    /// the result after the scan had produced it. That is
    /// `docs/VISION.md` III — "the worst violation is the most common query" —
    /// and what the bounded sink removes. The scan budget is still a budget on
    /// rows *read*, so it is asserted with a limit loose enough not to bound
    /// them.
    #[tokio::test]
    async fn the_output_limit_bounds_the_physical_scan() {
        let data_dir = temp_dir();
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let memtable = Arc::new(MemTable::new());
        memtable.insert(test_tenant(), with_attributes(labels.clone(), (0..3)
                .map(|timestamp_ns| LogEntry {
                    timestamp_ns,
                    line: format!("line-{timestamp_ns}"),
                    structured_metadata: Vec::new(),
                })
                .collect()));
        let state = test_state(&data_dir, memtable, Arc::new(PartRegistry::new()), None);
        let parsed = logql::parse("{}").unwrap();
        for (forward, wanted_line) in [(true, "line-0"), (false, "line-2")] {
            let execution = run_unified_query_with_stats(
                state.clone(),
                test_tenant(),
                parsed.clone(),
                crate::part::QueryTimeRange::closed(0, 2),
                1,
                forward,
                None,
                crate::metrics::QueryEndpoint::Query,
            )
            .await
            .unwrap();
            assert_eq!(execution.results[0].entries.len(), 1);
            assert_eq!(execution.results[0].entries[0].line, wanted_line);
            assert_eq!(
                execution.scanned_rows, 1,
                "one row of answer must cost one row of scan"
            );
        }

        let error = match unified_query_with_stats_cancellable(
            &state,
            &test_tenant(),
            &parsed,
            crate::part::QueryTimeRange::closed(0, 2),
            10,
            true,
            Some(2),
            None,
        ) {
            Ok(_) => panic!("scan budget should reject the third physical row"),
            Err(error) => error,
        };
        assert!(error.contains("2 scanned rows"));
    }

    /// The bed the early-termination tests share: rows in both the memtable and
    /// several parts, out of order, with timestamps spread far wider than any
    /// limit those tests ask for.
    fn early_termination_state(data_dir: &std::path::Path) -> Arc<AppState> {
        let attributes = vec![("app".to_string(), "api".to_string())];
        let line = |timestamp_ns: i64| {
            format!(r#"{{"status":"{}","seq":{timestamp_ns}}}"#, 500 - (timestamp_ns % 3))
        };
        let memtable = Arc::new(MemTable::new());
        // Newest, and out of order.
        memtable.insert(
            test_tenant(),
            [93i64, 90, 92, 91]
                .into_iter()
                .map(|timestamp_ns| LogEntry {
                    timestamp_ns,
                    line: line(timestamp_ns),
                    structured_metadata: attributes.clone(),
                })
                .collect(),
        );
        let parts = Arc::new(PartRegistry::new());
        // Three parts whose windows do not overlap, so the frontier can reject a
        // whole part from its metadata, in row groups of three so it can also
        // reject a row group inside the part it does open. Adjacent rather than
        // separated, so "the answer is contiguous" is a statement the boundary
        // test can make.
        for base in [0i64, 30, 60] {
            parts
                .register(
                    part::flush_rows(
                        (0..30)
                            .rev()
                            .map(|offset| Row {
                                tenant: test_tenant(),
                                timestamp_ns: base + offset,
                                line: line(base + offset),
                                structured_metadata: attributes.clone(),
                            })
                            .collect(),
                        &data_dir.join("parts"),
                        3,
                    )
                    .unwrap(),
                )
                .unwrap();
        }
        test_state(data_dir, memtable, parts, None)
    }

    fn flatten(results: &[StreamResult]) -> Vec<(i64, String)> {
        let mut rows: Vec<(i64, String)> = results
            .iter()
            .flat_map(|stream| {
                stream
                    .entries
                    .iter()
                    .map(|entry| (entry.timestamp_ns, entry.line.clone()))
            })
            .collect();
        rows.sort_by_key(|(timestamp_ns, _)| *timestamp_ns);
        rows
    }

    /// A limit changes how many rows a query answers with, never which ones.
    ///
    /// Stated as an identity rather than as a fixture, because that is what
    /// makes early termination a performance change: `limit = n` has to return
    /// the first `n` rows of the answer the same query gives when nothing is
    /// truncated. Checked in both directions and through a `| json | field=`
    /// pipeline, which is the shape whose limit used to be `usize::MAX`.
    #[tokio::test]
    async fn a_limited_query_returns_a_prefix_of_the_unlimited_answer() {
        let data_dir = temp_dir();
        let state = early_termination_state(&data_dir);
        let range = crate::part::QueryTimeRange::closed(0, 1_000);
        for query in ["{app=\"api\"}", "{app=\"api\"} | json | status=\"500\""] {
            let parsed = logql::parse(query).unwrap();
            for forward in [true, false] {
                let unlimited = flatten(
                    &unified_query(&state, &test_tenant(), &parsed, range, 1_000, forward).unwrap(),
                );
                assert!(
                    unlimited.len() > 20,
                    "{query} must match more than any limit below asks for"
                );
                for limit in [1usize, 2, 7, 20] {
                    let limited =
                        unified_query(&state, &test_tenant(), &parsed, range, limit, forward)
                            .unwrap();
                    let rows = flatten(&limited);
                    assert_eq!(
                        rows.len(),
                        limit,
                        "{query} at limit {limit} ({}) must return exactly the limit",
                        if forward { "forward" } else { "backward" }
                    );
                    // Forwards the answer starts at the oldest row, backwards at
                    // the newest, so the prefix is taken from the matching end.
                    let wanted: Vec<(i64, String)> = if forward {
                        unlimited[..limit].to_vec()
                    } else {
                        unlimited[unlimited.len() - limit..].to_vec()
                    };
                    assert_eq!(
                        rows, wanted,
                        "{query} at limit {limit} returned different rows, not fewer"
                    );
                }
            }
        }
    }

    /// The limit is honoured exactly on the boundary where the parts stop and the
    /// memtable starts, and on the boundary between two parts.
    ///
    /// Those are the two places a bounded scan decides to stop reading, so an
    /// off-by-one there returns 99 rows or 101 rather than an obviously wrong
    /// answer.
    #[tokio::test]
    async fn a_limit_is_honoured_exactly_at_a_source_boundary() {
        let data_dir = temp_dir();
        let state = early_termination_state(&data_dir);
        let range = crate::part::QueryTimeRange::closed(0, 1_000);
        let parsed = logql::parse("{app=\"api\"}").unwrap();
        // 4 memtable rows and 30 rows in the newest part: 4 is the memtable
        // exactly, 5 is one row past it, 34 is the newest part exactly, and 35
        // is one row into the part before it.
        for limit in [3usize, 4, 5, 33, 34, 35] {
            let rows = flatten(
                &unified_query(&state, &test_tenant(), &parsed, range, limit, false).unwrap(),
            );
            assert_eq!(rows.len(), limit, "backward limit {limit}");
            let newest = rows.last().unwrap().0;
            assert_eq!(newest, 93, "backward always answers from the newest row");
            let oldest = rows.first().unwrap().0;
            assert_eq!(
                oldest,
                94 - limit as i64,
                "the rows are contiguous from the newest downwards at limit {limit}"
            );
        }
    }

    /// Early termination must not reach a row the scan budget already refused.
    /// The budget counts rows read, so a limit that stops the scan early makes a
    /// query that used to be refused succeed — and one whose pipeline discards
    /// most rows still has to be refused.
    #[tokio::test]
    async fn the_scan_budget_still_refuses_a_query_the_limit_cannot_bound() {
        let data_dir = temp_dir();
        let state = early_termination_state(&data_dir);
        let range = crate::part::QueryTimeRange::closed(0, 1_000);
        // Matches nothing, so no limit can ever be reached and the scan runs
        // until the budget stops it. A regex rather than an equality, because an
        // equality on an indexed field is answered by the exact-field bloom and
        // never reaches a row at all — which is the pruning working, not the
        // budget.
        let parsed = logql::parse("{app=\"api\"} | json | status=~\"418\"").unwrap();
        let error = match unified_query_with_stats(
            &state,
            &test_tenant(),
            &parsed,
            range,
            10,
            false,
            Some(20),
        ) {
            Ok(execution) => panic!(
                "a query that survives nothing must exhaust the budget, read {} rows",
                execution.scanned_rows
            ),
            Err(error) => error,
        };
        assert!(error.contains("20 scanned rows"), "{error}");
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
        let local_parts = part::flush_rows(rows_with_attributes(&labels, vec![Row {
                tenant: test_tenant(),
                timestamp_ns: 1_700_000_000_000_000_000,
                line: "restored after eviction".to_string(),
                structured_metadata: Vec::new(),
            }]),
            &parts_root,
            config.row_group_size)
        .unwrap();
        let other_labels: Labels = [("app".to_string(), "other".to_string())]
            .into_iter()
            .collect();
        let other_parts = part::flush_rows(rows_with_attributes(&other_labels, vec![Row {
                tenant: test_tenant(),
                timestamp_ns: 1_700_000_000_000_000_001,
                line: "must remain remote".to_string(),
                structured_metadata: Vec::new(),
            }]),
            &parts_root,
            config.row_group_size)
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
        let result = run_unified_query(state, test_tenant(), parsed, crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 10, true)
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
        let old = part::flush_rows(rows_with_attributes(&labels, vec![Row {
                tenant: test_tenant(),
                timestamp_ns: 1,
                line: "old generation".to_string(),
                structured_metadata: Vec::new(),
            }]),
            &parts_root,
            config.row_group_size)
        .unwrap();
        storage.publish(&old, &[]).await.unwrap();
        let parts = Arc::new(PartRegistry::load_from_disk(&parts_root).unwrap());

        let new = part::flush_rows(rows_with_attributes(&labels, vec![Row {
                tenant: test_tenant(),
                timestamp_ns: 2,
                line: "new generation".to_string(),
                structured_metadata: Vec::new(),
            }]),
            &parts_root,
            config.row_group_size)
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
        let guard = pin_query_parts_with_gap_hook(&state, &test_tenant(), &parsed, crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), || {
            parts.reload_from_manifest(&parts_root, &manifest)
        })
        .await
        .unwrap();
        let result = unified_query(&state, &test_tenant(), &parsed, crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 10, true).unwrap();
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
        // Canonical (key-sorted) order, because this fixture hands a `Row`
        // straight to the part writer, bypassing the memtable door that
        // canonicalizes real ingest.
        let metadata = vec![
            ("logfmt".to_string(), "value".to_string()),
            ("trace_id".to_string(), "abc".to_string()),
        ];
        let memtable = Arc::new(MemTable::new());
        memtable.insert(test_tenant(), with_attributes(labels.clone(), vec![LogEntry {
                timestamp_ns: 10,
                line: line.to_string(),
                structured_metadata: metadata.clone(),
            }]));
        let parts = Arc::new(PartRegistry::new());
        parts
            .register(
                part::flush_rows(rows_with_attributes(&labels, vec![Row {
                        tenant: test_tenant(),
                        timestamp_ns: 20,
                        line: line.to_string(),
                        structured_metadata: metadata,
                    }]),
                    &data_dir.join("parts"),
                    1)
                .unwrap(),
            )
            .unwrap();
        let state = test_state(&data_dir, memtable, parts, None);
        let parsed = logql::parse(
            r#"{app="api"} | app="api" | trace_id="abc" | json | level=~"err.*" | code>=500 | elapsed<1s"#,
        )
        .unwrap();

        let result = run_unified_query(state.clone(), test_tenant(), parsed, crate::part::QueryTimeRange::closed(0, 30), 10, true)
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
                run_unified_query(state.clone(), test_tenant(), logql::parse(query).unwrap(), crate::part::QueryTimeRange::closed(0, 30), 10, true)
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

        #[test]
    fn metric_windows_are_left_open_and_aggregations_recompute_each_step() {
        let a: SharedLabels = SharedLabels::new([("app".to_string(), "a".to_string())].into_iter().collect());
        let b: SharedLabels = SharedLabels::new([("app".to_string(), "b".to_string())].into_iter().collect());
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
                vec![(SharedLabels::new(Labels::new()), expected)]
            );
        }
    }

    #[test]
    fn metric_evaluation_stops_when_timeout_cancellation_is_requested() {
        let expr = match logql::parse_expr("count_over_time({}[1m])") {
            Ok(logql::QueryExpr::Metric(expr)) => expr,
            _ => panic!("expected metric expression"),
        };
        let labels: SharedLabels =
            SharedLabels::new([("app".to_string(), "api".to_string())].into_iter().collect());
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
                part::flush_rows(rows_with_attributes(&labels, vec![Row {
                        tenant: test_tenant(),
                        timestamp_ns: i64::MIN,
                        line: "oldest".to_string(),
                        structured_metadata: vec![],
                    }]),
                    &data_dir.join("parts"),
                    1)
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
        assert!(result[0].labels.is_empty(), "an ungrouped series carries no labels");
        assert_eq!(result[0].samples, vec![(i64::MIN + 1, 1.0)]);
    }

    #[tokio::test]
    async fn metric_query_ignores_log_limit_and_returns_matrix_or_vector() {
        let data_dir = temp_dir();
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let memtable = Arc::new(MemTable::new());
        memtable.insert(test_tenant(), with_attributes(labels.clone(), (1..=3)
                .map(|second| LogEntry {
                    timestamp_ns: second * 1_000_000_000,
                    line: "xx".to_string(),
                    structured_metadata: vec![],
                })
                .collect()));
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
        assert_eq!(range.data.result.to_value()[0]["values"][0][1], "6");

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
        assert_eq!(instant.data.result.to_value()[0]["value"][1], "3");
    }

    /// `| trace_id="x"` with no parser stage — the shape the claim rests on —
    /// takes the two-pass path in a part: the narrow `_sm:trace_id` column
    /// selects the rows before anything wide is decoded. The memtable cannot
    /// do that, so memtable-against-part equality is the late-materialization
    /// on/off check, covering a hit, a miss, and a predicate the pass must
    /// refuse (a stream-label name, answered by the label instead).
    #[tokio::test]
    async fn a_metadata_filter_answers_identically_through_the_two_pass_scan() {
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let entries: Vec<LogEntry> = (0..50i64)
            .map(|i| LogEntry {
                timestamp_ns: 1_000 + i,
                line: format!("line-{i}"),
                structured_metadata: vec![("trace_id".to_string(), format!("t{}", i % 10))],
            })
            .collect();
        let in_memtable = {
            let data_dir = temp_dir();
            let memtable = Arc::new(MemTable::new());
            memtable.insert(test_tenant(), with_attributes(labels.clone(), entries.clone()));
            test_state(&data_dir, memtable, Arc::new(PartRegistry::new()), None)
        };
        let in_parts = {
            let data_dir = temp_dir();
            let rows: Vec<Row> = entries
                .iter()
                .map(|entry| Row {
                    tenant: test_tenant(),
                    timestamp_ns: entry.timestamp_ns,
                    line: entry.line.clone(),
                    structured_metadata: entry.structured_metadata.clone(),
                })
                .collect();
            let parts = Arc::new(PartRegistry::new());
            parts
                .register(part::flush_rows(rows_with_attributes(&labels, rows), &data_dir.join("parts"), 16).unwrap())
                .unwrap();
            test_state(&data_dir, Arc::new(MemTable::new()), parts, None)
        };

        for expression in [
            r#"{app="api"} | trace_id="t3""#,
            r#"{app="api"} | trace_id="absent""#,
            r#"{app="api"} | app="api" | trace_id="t3""#,
        ] {
            let parsed = logql::parse(expression).unwrap();
            let flat = |results: Vec<StreamResult>| -> Vec<(i64, String)> {
                let mut rows: Vec<(i64, String)> = results
                    .into_iter()
                    .flat_map(|stream| {
                        stream
                            .entries
                            .into_iter()
                            .map(|entry| (entry.timestamp_ns, entry.line))
                    })
                    .collect();
                rows.sort();
                rows
            };
            let range = crate::part::QueryTimeRange::closed(0, 10_000);
            let from_memtable = flat(
                run_unified_query(
                    in_memtable.clone(),
                    test_tenant(),
                    parsed.clone(),
                    range,
                    1000,
                    false,
                )
                .await
                .unwrap(),
            );
            let from_parts = flat(
                run_unified_query(in_parts.clone(), test_tenant(), parsed, range, 1000, false)
                    .await
                    .unwrap(),
            );
            assert_eq!(
                from_memtable, from_parts,
                "the two-pass scan must not change the answer to {expression}"
            );
            if expression.contains("t3") {
                assert_eq!(from_parts.len(), 5, "the hit case must actually hit");
            }
        }
    }

    /// The counting fast path against the general evaluator, on the same
    /// state: `sum(...)` takes the sink accumulator and `sum by (app) (...)`
    /// takes the general path, and over a single `app` value their samples
    /// must be identical numbers under different labels.
    #[tokio::test]
    async fn the_counting_fast_path_matches_the_general_evaluator() {
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let memtable = Arc::new(MemTable::new());
        memtable.insert(test_tenant(), with_attributes(labels.clone(), (0..50i64)
                .map(|i| LogEntry {
                    timestamp_ns: 1_000_000_000 + i * 137_000_000,
                    line: format!("line-{i}"),
                    structured_metadata: vec![],
                })
                .collect()));
        let data_dir = temp_dir();
        let state = test_state(&data_dir, memtable, Arc::new(PartRegistry::new()), None);
        for function in ["rate", "count_over_time", "bytes_over_time"] {
            let parse = |text: &str| match logql::parse_expr(text).unwrap() {
                logql::QueryExpr::Metric(expr) => expr,
                _ => panic!("expected metric"),
            };
            let times = vec![3_000_000_000, 5_000_000_000, 9_000_000_000];
            let fast = run_metric_query(
                state.clone(),
                test_tenant(),
                parse(&format!(r#"sum({function}({{app="api"}}[2s]))"#)),
                times.clone(),
            )
            .await
            .unwrap();
            let general = run_metric_query(
                state.clone(),
                test_tenant(),
                parse(&format!(r#"sum by (app) ({function}({{app="api"}}[2s]))"#)),
                times,
            )
            .await
            .unwrap();
            assert_eq!(fast.len(), 1, "{function}");
            assert_eq!(
                fast[0].samples, general[0].samples,
                "fast and general paths must agree on {function}"
            );
        }
    }

    /// The selector matches the row's own attributes: an attribute that says
    /// something else keeps the row out of the answer, and the value the row
    /// pushed is the one the response shows.
    #[tokio::test]
    async fn the_selector_matches_the_rows_own_attributes() {
        let memtable = Arc::new(MemTable::new());
        memtable.insert(
            test_tenant(),
            vec![LogEntry {
                timestamp_ns: 10,
                line: "line".to_string(),
                structured_metadata: vec![
                    ("app".to_string(), "smuggled".to_string()),
                    ("trace_id".to_string(), "t1".to_string()),
                ],
            }],
        );
        let data_dir = temp_dir();
        let state = test_state(&data_dir, memtable, Arc::new(PartRegistry::new()), None);

        let refused = run_unified_query(
            state.clone(),
            test_tenant(),
            logql::parse(r#"{app="api"}"#).unwrap(),
            crate::part::QueryTimeRange::closed(0, 20),
            10,
            false,
        )
        .await
        .unwrap();
        assert!(refused.is_empty() || refused.iter().all(|s| s.entries.is_empty()));

        let results = run_unified_query(
            state,
            test_tenant(),
            logql::parse(r#"{app="smuggled"}"#).unwrap(),
            crate::part::QueryTimeRange::closed(0, 20),
            10,
            false,
        )
        .await
        .unwrap();
        let data = build_stream_data(results, false);
        assert_eq!(data[0].stream.get("app"), Some("smuggled"));
        assert_eq!(data[0].stream.get("trace_id"), Some("t1"));
    }

    /// `| json | field="x"` — `json_field_rare`'s shape — now takes the
    /// two-pass path too: the `_pf:` column is what `| json` would extract,
    /// and metadata still wins where both exist. Memtable-against-parts is the
    /// on/off pair; the cases are a line-value hit, a miss, and the shadowing
    /// row whose metadata must silence its line.
    #[tokio::test]
    async fn a_json_extraction_filter_answers_identically_through_the_two_pass_scan() {
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let mut entries: Vec<LogEntry> = (0..40i64)
            .map(|i| LogEntry {
                timestamp_ns: 1_000 + i,
                line: format!(r#"{{"trace_id":"t{}","status":200}}"#, i % 8),
                structured_metadata: vec![],
            })
            .collect();
        // The shadowing row: the line says t3, the pushed metadata says
        // other — Loki's rule discards the extraction, so `trace_id="t3"`
        // must not return this row and `trace_id="other"` must.
        entries.push(LogEntry {
            timestamp_ns: 2_000,
            line: r#"{"trace_id":"t3"}"#.to_string(),
            structured_metadata: vec![("trace_id".to_string(), "other".to_string())],
        });
        // A non-JSON row: extraction fails, the filter sees nothing.
        entries.push(LogEntry {
            timestamp_ns: 2_001,
            line: "trace_id=t3 not json".to_string(),
            structured_metadata: vec![],
        });

        let in_memtable = {
            let data_dir = temp_dir();
            let memtable = Arc::new(MemTable::new());
            memtable.insert(test_tenant(), with_attributes(labels.clone(), entries.clone()));
            test_state(&data_dir, memtable, Arc::new(PartRegistry::new()), None)
        };
        let in_parts = {
            let data_dir = temp_dir();
            let rows: Vec<Row> = entries
                .iter()
                .map(|entry| Row {
                    tenant: test_tenant(),
                    timestamp_ns: entry.timestamp_ns,
                    line: entry.line.clone(),
                    structured_metadata: entry.structured_metadata.clone(),
                })
                .collect();
            let parts = Arc::new(PartRegistry::new());
            parts
                .register(
                    part::flush_rows(
                        rows_with_attributes(&labels, rows),
                        &data_dir.join("parts"),
                        16,
                    )
                    .unwrap(),
                )
                .unwrap();
            test_state(&data_dir, Arc::new(MemTable::new()), parts, None)
        };

        for (expression, expected_rows) in [
            (r#"{app="api"} | json | trace_id="t3""#, 5usize),
            (r#"{app="api"} | json | trace_id="absent""#, 0),
            (r#"{app="api"} | json | trace_id="other""#, 1),
        ] {
            let parsed = logql::parse(expression).unwrap();
            let flat = |results: Vec<StreamResult>| -> Vec<(i64, String)> {
                let mut rows: Vec<(i64, String)> = results
                    .into_iter()
                    .flat_map(|stream| {
                        stream
                            .entries
                            .into_iter()
                            .map(|entry| (entry.timestamp_ns, entry.line))
                    })
                    .collect();
                rows.sort();
                rows
            };
            let range = crate::part::QueryTimeRange::closed(0, 10_000);
            let from_memtable = flat(
                run_unified_query(
                    in_memtable.clone(),
                    test_tenant(),
                    parsed.clone(),
                    range,
                    1000,
                    false,
                )
                .await
                .unwrap(),
            );
            let from_parts = flat(
                run_unified_query(in_parts.clone(), test_tenant(), parsed, range, 1000, false)
                    .await
                    .unwrap(),
            );
            assert_eq!(
                from_memtable, from_parts,
                "the two-pass json path must not change the answer to {expression}"
            );
            assert_eq!(from_parts.len(), expected_rows, "{expression}");
        }
    }

    /// The metric path projects columns — only the timestamp, the labels and
    /// the fields the expression names come off disk. The memtable cannot
    /// project, so the same rows in the memtable and in a part are a
    /// projection-on/projection-off pair, and every shape the projection
    /// analysis distinguishes must answer identically on both: bare
    /// aggregation (nothing named), grouping over a metadata field (that field
    /// named), unwrap (that field named), and a parser stage (bails to every
    /// column).
    #[tokio::test]
    async fn metric_answers_are_identical_with_and_without_projection() {
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let entries: Vec<LogEntry> = (0..40i64)
            .map(|i| LogEntry {
                timestamp_ns: 1_000_000_000 + i * 200_000_000,
                line: format!(r#"{{"level":"{}","status":200}}"#, ["info", "error"][(i % 2) as usize]),
                structured_metadata: vec![
                    ("duration_ms".to_string(), format!("{}", 10 + i)),
                    ("trace_id".to_string(), format!("t{}", i % 4)),
                ],
            })
            .collect();

        let in_memtable = {
            let data_dir = temp_dir();
            let memtable = Arc::new(MemTable::new());
            memtable.insert(test_tenant(), with_attributes(labels.clone(), entries.clone()));
            test_state(&data_dir, memtable, Arc::new(PartRegistry::new()), None)
        };
        let in_parts = {
            let data_dir = temp_dir();
            let rows: Vec<Row> = entries
                .iter()
                .map(|entry| Row {
                    tenant: test_tenant(),
                    timestamp_ns: entry.timestamp_ns,
                    line: entry.line.clone(),
                    structured_metadata: entry.structured_metadata.clone(),
                })
                .collect();
            let parts = Arc::new(PartRegistry::new());
            parts
                .register(part::flush_rows(rows_with_attributes(&labels, rows), &data_dir.join("parts"), 8).unwrap())
                .unwrap();
            test_state(&data_dir, Arc::new(MemTable::new()), parts, None)
        };

        for expression in [
            r#"sum(rate({app="api"}[5s]))"#,
            r#"sum by (trace_id) (count_over_time({app="api"}[5s]))"#,
            r#"sum without (trace_id) (count_over_time({app="api"}[5s]))"#,
            r#"avg_over_time({app="api"} | unwrap duration_ms [5s])"#,
            r#"sum(bytes_over_time({app="api"}[5s]))"#,
            r#"sum by (level) (count_over_time({app="api"} | json [5s]))"#,
            r#"sum(count_over_time({app="api"} | trace_id="t1" [5s]))"#,
        ] {
            let parse = |text: &str| -> logql::MetricExpr {
                match logql::parse_expr(text).unwrap() {
                    logql::QueryExpr::Metric(expr) => expr,
                    _ => panic!("expected metric"),
                }
            };
            let times = vec![5_000_000_000, 9_000_000_000];
            let from_memtable = run_metric_query(
                in_memtable.clone(),
                test_tenant(),
                parse(expression),
                times.clone(),
            )
            .await
            .unwrap();
            let from_parts =
                run_metric_query(in_parts.clone(), test_tenant(), parse(expression), times)
                    .await
                    .unwrap();
            let flat = |series: &[MetricSeries]| -> Vec<String> {
                series
                    .iter()
                    .map(|series| format!("{:?} {:?}", series.labels, series.samples))
                    .collect()
            };
            assert_eq!(
                flat(&from_memtable),
                flat(&from_parts),
                "projection must not change the answer to {expression}"
            );
        }
    }

    /// A metric query is no longer refused for materializing rows it never keeps.
    ///
    /// The scan used to hand the evaluator every matching line and the evaluator
    /// reduced them to a `(timestamp, value)` per series one step later, so the
    /// ceiling was on an intermediate. The same rows that make a *log* query
    /// legitimately too large to materialize cost a metric query sixteen bytes
    /// an event and none of the text.
    #[tokio::test]
    async fn a_metric_query_folds_rows_a_log_query_could_not_materialize() {
        const ROWS: usize = 512;

        let data_dir = temp_dir();
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let memtable = Arc::new(MemTable::new());
        memtable.insert(test_tenant(), with_attributes(labels.clone(), (0..ROWS)
                .map(|index| LogEntry {
                    timestamp_ns: 1_000_000_000 + index as i64 * 1_000_000,
                    line: "x".repeat(1024),
                    structured_metadata: vec![],
                })
                .collect()));

        // Well under the half-megabyte of lines, and well over the eight
        // kilobytes of samples they fold down to.
        let state = state_with_config(
            Config {
                data_dir: data_dir.clone(),
                max_query_memory_bytes: 64 * 1024,
                ..Config::default()
            },
            memtable,
            Arc::new(PartRegistry::new()),
        );

        let logql::QueryExpr::Metric(expr) =
            logql::parse_expr(r#"count_over_time({app="api"}[1m])"#).unwrap()
        else {
            panic!("expected metric")
        };
        let series = run_metric_query(state.clone(), test_tenant(), expr, vec![2_000_000_000])
            .await
            .unwrap();
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].samples[0].1, ROWS as f64);

        // The same rows asked for as lines are still refused, and that refusal
        // is the honest one: a client asking for them would receive every byte.
        let error = run_unified_query_with_stats(
            state,
            test_tenant(),
            logql::parse(r#"{app="api"}"#).unwrap(),
            crate::part::QueryTimeRange::closed(0, 3_000_000_000),
            ROWS,
            true,
            None,
            crate::metrics::QueryEndpoint::Query,
        )
        .await
        .err()
        .expect("the log query must not fit in the same budget");
        assert!(
            error.contains("materialized bytes"),
            "expected a memory refusal for the log query, got: {error}"
        );
    }

    #[tokio::test]
    async fn metric_grouping_uses_extracted_fields_and_parser_errors() {
        let data_dir = temp_dir();
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let memtable = Arc::new(MemTable::new());
        memtable.insert(test_tenant(), with_attributes(labels.clone(), vec![
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
            ]));
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

        // An extraction that collides with pushed metadata is discarded —
        // the pushed attribute is authoritative — so grouping by the name
        // reads the pushed value, never the line's.
        let logql::QueryExpr::Metric(expr) = logql::parse_expr(
            r#"sum by (app) (count_over_time({app="api"} | json [5s]))"#,
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
                .all(|series| series.labels.get("app") == Some(&"api".to_string())),
            "the pushed attribute wins over the line's extraction"
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
        let wanted = part::flush_rows(rows_with_attributes(&labels, vec![Row {
                tenant: test_tenant(),
                timestamp_ns: 6_000_000_000,
                line: "wanted".to_string(),
                structured_metadata: vec![("tenant".to_string(), "one".to_string())],
            }]),
            &parts_root,
            1)
        .unwrap();
        let unwanted = part::flush_rows(rows_with_attributes(&labels, vec![Row {
                tenant: test_tenant(),
                timestamp_ns: 7_000_000_000,
                line: "unwanted".to_string(),
                structured_metadata: vec![("tenant".to_string(), "two".to_string())],
            }]),
            &parts_root,
            1)
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
            .to_value()
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
            vec![LogEntry {
                timestamp_ns: one_hour_ago,
                line: "an hour old".to_string(),
                structured_metadata: vec![("app".to_string(), "api".to_string())],
            }],
        );
        let policy = Arc::new(crate::tenant_policy::TenantPolicy::enabled_for_test());
        let state = tenant_policy_state(
            &data_dir,
            memtable,
            Arc::new(PartRegistry::new()),
            policy.clone(),
        );

        // Nothing pushed yet: the pushed policies are the tenant registry, so
        // the tenant is not served until the control plane onboards it.
        let now_ns_query = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        let refused = match query_range(
            State(state.clone()),
            crate::tenant::test_tenant_headers(),
            Query(QueryRangeParams {
                query: "{app=\"api\"}".to_string(),
                start: Some((now_ns_query - 86_400_000_000_000).to_string()),
                end: Some(now_ns_query.to_string()),
                limit: Some(100),
                direction: Some("forward".to_string()),
                step: None,
            }),
        )
        .await
        {
            Err(error) => error,
            Ok(_) => panic!("a tenant nothing was pushed for must be refused"),
        };
        assert_eq!(refused.0, StatusCode::FORBIDDEN);

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
            state.metrics.observe_query(
                crate::metrics::QueryEndpoint::QueryRange,
                std::time::Duration::from_millis(millis),
            );
        }
        // One observation on a second endpoint, so the assertions below prove
        // the split is real rather than one histogram wearing a label.
        state.metrics.observe_query(
            crate::metrics::QueryEndpoint::Volume,
            std::time::Duration::from_millis(900),
        );

        let rendered = metrics(State(state)).await;
        let bucket = |bound: &str| -> u64 {
            let needle = format!(
                "loggytracy_query_latency_ms_bucket{{endpoint=\"query_range\",le=\"{bound}\"}} "
            );
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
        assert!(
            rendered.contains("loggytracy_query_latency_ms_count{endpoint=\"volume\"} 1"),
            "volume's own series must carry only volume's observation:\n{rendered}"
        );
        assert!(
            rendered.contains("loggytracy_query_latency_ms_count{endpoint=\"query_range\"} 4"),
            "and query_range's must carry only its own:\n{rendered}"
        );
        assert!(
            rendered.contains("loggytracy_query_latency_ms_count{endpoint=\"tail\"} 0"),
            "an endpoint nothing reached is still exported, so a dashboard shows a gap rather than a missing series:\n{rendered}"
        );
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
        // The unlabeled series is gone on purpose: an aggregate that is also a
        // labeled series double-counts under `sum`, and the whole-path number
        // is `sum by (le)` across the endpoints.
        assert!(!rendered.contains("loggytracy_query_latency_ms_count 4"));
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
            memtable.insert(test_tenant(), with_attributes([("app".to_string(), app.to_string())].into_iter().collect(), vec![LogEntry {
                    timestamp_ns,
                    line: format!("{app} line"),
                    structured_metadata: Vec::new(),
                }]));
        }
        let state = state_with(&data_dir, memtable, Arc::new(PartRegistry::new()));

        let window = |start_ns: i64| crate::query::MetadataParams {
            start: Some(start_ns.to_string()),
            end: Some(now_ns.to_string()),
            query: None,
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
            query: None,
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

    /// With per-tenant policy enabled, the pushed policies are the tenant
    /// registry: a tenant the control plane never mentioned is refused at
    /// query time rather than served its full history.
    #[tokio::test]
    async fn a_tenant_without_a_pushed_policy_is_refused_at_query_time() {
        let data_dir = temp_dir();
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        let memtable = Arc::new(MemTable::new());
        memtable.insert(
            test_tenant(),
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

        let refused = match query_range(
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
        {
            Err(error) => error,
            Ok(_) => panic!("a tenant nothing was pushed for must be refused"),
        };
        assert_eq!(refused.0, StatusCode::FORBIDDEN);
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
            response.data.result.to_value().as_array().unwrap().is_empty(),
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
            vec![LogEntry {
                timestamp_ns: now_ns - 3_600_000_000_000,
                line: "an hour old".to_string(),
                structured_metadata: vec![("app".to_string(), "stale".to_string())],
            }],
        );
        memtable.insert(
            test_tenant(),
            vec![LogEntry {
                timestamp_ns: now_ns,
                line: "just now".to_string(),
                structured_metadata: vec![("app".to_string(), "fresh".to_string())],
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
        assert_eq!(stats["data"]["streams"], 0, "streams are gone from the model");

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
        memtable.insert(test_tenant(), with_attributes(tail_labels(), vec![
                tail_entry(collision_ns, "first"),
                tail_entry(collision_ns, "second"),
            ]));

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
        memtable.insert(test_tenant(), with_attributes(tail_labels(), vec![tail_entry(collision_ns, "third")]));
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
        memtable.insert(test_tenant(), with_attributes(tail_labels(), entries));

        let mut cursor = TailCursor::new(base_ns - 1);
        let query = tail_query();
        let end_ns = base_ns + 10;

        let first = tail_poll(&state, &test_tenant(), &query, &mut cursor, end_ns, 2)
            .await
            .expect("the oldest two arrive first");
        assert_eq!(tail_lines(&first), vec!["line-0", "line-1"]);

        let second = tail_poll(&state, &test_tenant(), &query, &mut cursor, end_ns, 2)
            .await
            .expect("the next poll continues rather than skipping ahead");
        assert_eq!(tail_lines(&second), vec!["line-2", "line-3"]);

        let third = tail_poll(&state, &test_tenant(), &query, &mut cursor, end_ns, 2)
            .await
            .expect("and drains the rest");
        assert_eq!(tail_lines(&third), vec!["line-4"]);
    }

    fn tail_lines(fresh: &[StreamResult]) -> Vec<String> {
        let mut rows: Vec<(i64, String)> = fresh
            .iter()
            .flat_map(|stream| {
                stream
                    .entries
                    .iter()
                    .map(|entry| (entry.timestamp_ns, entry.line.clone()))
            })
            .collect();
        rows.sort_by_key(|(timestamp_ns, _)| *timestamp_ns);
        rows.into_iter().map(|(_, line)| line).collect()
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
            memtable.insert(test_tenant(), with_attributes([("app".to_string(), app.to_string())]
                    .into_iter()
                    .collect::<Labels>(), vec![LogEntry {
                    timestamp_ns: base_ns,
                    line: line.to_string(),
                    structured_metadata: vec![],
                }]));
        }
        (state, base_ns)
    }

    /// Volume is `bytes_over_time` under a different name, so it is answered by
    /// the metric evaluator rather than by a scan of its own. That is what puts
    /// it inside the same scan budgets, retention clamp and tenant scope. With
    /// no stream concept, no `targetLabels` means one total series; the
    /// breakdown lives in `index_volume_aggregates_by_target_labels`.
    #[tokio::test]
    async fn index_volume_reports_total_bytes() {
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

        let series = body["data"]["result"].as_array().unwrap();
        assert_eq!(series.len(), 1, "one ungrouped series");
        // "aaaa" + "bbbbbb" + "cc".
        let total: f64 = series[0]["value"][1].as_str().unwrap().parse().unwrap();
        assert_eq!(total, 12.0);
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
            memtable.insert(test_tenant(), with_attributes([("app".to_string(), "a".to_string())]
                    .into_iter()
                    .collect::<Labels>(), vec![LogEntry {
                    timestamp_ns: base_ns + offset * 1_000_000_000,
                    line: "aaaa".to_string(),
                    structured_metadata: vec![],
                }]));
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
        assert_eq!(series.len(), 1, "the selector narrowed it to one series");
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
        memtable.insert(test_tenant(), with_attributes([("app".to_string(), "fields".to_string())]
                .into_iter()
                .collect::<Labels>(), vec![
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
            ]));

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
            max_concurrent_queries_per_tenant: 1,
            ..Config::default()
        };
        let parts = Arc::new(PartRegistry::new());
        let trace_parts = Arc::new(crate::trace_registry::TraceRegistry::standalone());
        let state = crate::test_support::state(
            config.clone(),
            memtable.clone(),
            Arc::new(Journal::spawn(&config, memtable).unwrap()),
            parts,
            trace_parts,
            None,
        );

        // The tenant's one slot is taken by a query still in flight.
        let slot = state.tenant_quota.begin_query(&test_tenant()).unwrap();

        let uri = "/loki/api/v1/query_range?query=%7Bapp%3D%22quota%22%7D";
        let (refused, body) = get_json(&state, uri).await;
        assert_eq!(
            refused,
            StatusCode::TOO_MANY_REQUESTS,
            "a tenant at its concurrency limit is told to come back: {body}"
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

        drop(slot);
        let (allowed, _) = get_json(&state, uri).await;
        assert_eq!(allowed, StatusCode::OK, "the freed slot admits the next query");
    }

    fn unwrapped_entries(values: &[(i64, &str)]) -> Vec<(SharedLabels, LogEntry)> {
        let labels: SharedLabels =
            SharedLabels::new([("app".to_string(), "u".to_string())].into_iter().collect());
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
        let labels: SharedLabels =
            SharedLabels::new([("app".to_string(), "s".to_string())].into_iter().collect());
        // Three entries inside the second step, one inside the tenth. A
        // one-second count_over_time therefore peaks at 3.
        let mut entries: Vec<(SharedLabels, LogEntry)> = Vec::new();
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

    /// Tokens carrying a digit are masked before anything is compared, so a
    /// thousand distinct request ids are one pattern rather than a thousand.
    #[test]
    fn digits_are_variable_on_their_face() {
        assert_eq!(
            tokenize("GET /users/4821 took 13ms"),
            vec!["GET", "<_>", "took", "<_>"]
        );
    }

    /// A template that has degraded to wildcards must not absorb every line of
    /// its length: with nothing pinned there is nothing to have matched.
    #[test]
    fn similarity_ignores_the_positions_a_template_gave_up_on() {
        let template = vec!["<_>".to_string(), "<_>".to_string()];
        let tokens = vec!["anything".to_string(), "at all".to_string()];
        assert_eq!(similarity(&template, &tokens), 0.0);

        let template = vec!["level".to_string(), "<_>".to_string(), "msg".to_string()];
        let matching = vec!["level".to_string(), "x".to_string(), "msg".to_string()];
        assert_eq!(similarity(&template, &matching), 1.0);
        let half = vec!["level".to_string(), "x".to_string(), "other".to_string()];
        assert_eq!(similarity(&template, &half), 0.5);
        // Different lengths are different patterns, whatever they share.
        assert_eq!(similarity(&template, &matching[..2]), 0.0);
    }

    /// The whole point of the endpoint: many lines, few templates, and the
    /// varying field shown as a wildcard rather than as a hundred patterns.
    #[tokio::test]
    async fn patterns_collapse_varying_fields_and_keep_distinct_messages_apart() {
        let dir = temp_dir();
        let memtable = Arc::new(MemTable::new());
        let state = test_state(&dir, memtable.clone(), Arc::new(PartRegistry::new()), None);
        let base_ns = 1_700_000_000_000_000_000i64;
        let mut entries = Vec::new();
        for user_index in 0..20i64 {
            entries.push(LogEntry {
                timestamp_ns: base_ns + user_index * 1_000_000_000,
                line: format!("user {user_index} logged in from host alpha"),
                structured_metadata: vec![],
            });
        }
        for failure_index in 0..5i64 {
            entries.push(LogEntry {
                timestamp_ns: base_ns + failure_index * 1_000_000_000,
                line: "could not reach the payment service".to_string(),
                structured_metadata: vec![],
            });
        }
        memtable.insert(test_tenant(), with_attributes([("app".to_string(), "patterns".to_string())]
                .into_iter()
                .collect::<Labels>(), entries));

        let (status, body) = get_json(
            &state,
            "/loki/api/v1/patterns?query=%7Bapp%3D%22patterns%22%7D",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let found = body["data"].as_array().unwrap();
        let templates: Vec<&str> = found
            .iter()
            .map(|pattern| pattern["pattern"].as_str().unwrap())
            .collect();
        assert_eq!(
            templates,
            vec![
                "user <_> logged in from host alpha",
                "could not reach the payment service"
            ],
            "twenty logins are one pattern, and the failures are not folded into it"
        );
        assert_eq!(body["sampledLines"], 25);

        let login_samples = found[0]["samples"].as_array().unwrap();
        let counted: u64 = login_samples
            .iter()
            .map(|sample| sample[1].as_u64().unwrap())
            .sum();
        assert_eq!(counted, 20, "every line it matched is in a bucket");
    }

    /// An unparseable selector is the caller's error, and a window that
    /// retention has already emptied is an empty answer rather than one.
    #[tokio::test]
    async fn patterns_refuses_a_bad_query_and_answers_an_empty_window() {
        let dir = temp_dir();
        let memtable = Arc::new(MemTable::new());
        let state = test_state(&dir, memtable, Arc::new(PartRegistry::new()), None);
        let (status, _) = get_json(&state, "/loki/api/v1/patterns?query=%7Bnot+a+selector").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        let (status, body) = get_json(
            &state,
            "/loki/api/v1/patterns?query=%7Bapp%3D%22none%22%7D&start=1700000000&end=1700000060",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["data"].as_array().unwrap().is_empty());
    }

    async fn send(
        state: &Arc<AppState>,
        method: &str,
        uri: &str,
    ) -> (StatusCode, serde_json::Value) {
        let request = axum::http::Request::builder()
            .method(method)
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

    fn delete_state() -> (Arc<AppState>, i64) {
        let dir = temp_dir();
        let memtable = Arc::new(MemTable::new());
        let state = test_state(&dir, memtable.clone(), Arc::new(PartRegistry::new()), None);
        let base_ns = 1_700_000_000_000_000_000i64;
        for (app, line) in [("keep", "kept line"), ("drop", "secret line")] {
            memtable.insert(test_tenant(), with_attributes([("app".to_string(), app.to_string())]
                    .into_iter()
                    .collect::<Labels>(), vec![LogEntry {
                    timestamp_ns: base_ns,
                    line: line.to_string(),
                    structured_metadata: vec![],
                }]));
        }
        (state, base_ns)
    }

    async fn lines_for(state: &Arc<AppState>, selector: &str) -> Vec<String> {
        let (status, body) = get_json(
            state,
            &format!(
                "/loki/api/v1/query_range?query={}&start=1699999999&end=1700000001&limit=100",
                urlencoding(selector)
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        body["data"]["result"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|stream| stream["values"].as_array().unwrap().clone())
            .map(|value| value[1].as_str().unwrap().to_string())
            .collect()
    }

    fn urlencoding(raw: &str) -> String {
        raw.bytes()
            .map(|byte| match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    (byte as char).to_string()
                }
                _ => format!("%{byte:02X}"),
            })
            .collect()
    }

    /// The promise of the endpoint: the lines stop being readable when the
    /// request is accepted, not when a rewrite eventually runs. Only the
    /// selected stream goes.
    #[tokio::test]
    async fn an_accepted_deletion_hides_its_lines_immediately() {
        let (state, base_ns) = delete_state();
        assert_eq!(lines_for(&state, r#"{app=~".+"}"#).await.len(), 2);

        let (status, _) = send(
            &state,
            "POST",
            &format!(
                "/loki/api/v1/delete?query={}&start={}&end={}",
                urlencoding(r#"{app="drop"}"#),
                (base_ns - 1_000_000_000) / 1_000_000_000,
                (base_ns + 1_000_000_000) / 1_000_000_000
            ),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        assert_eq!(
            lines_for(&state, r#"{app=~".+"}"#).await,
            vec!["kept line".to_string()],
            "the deleted stream is gone and the other one is untouched"
        );
    }

    /// A request outside the window the lines fall in deletes nothing. Getting
    /// this wrong deletes more than was asked for, which is unrecoverable.
    #[tokio::test]
    async fn a_deletion_outside_the_window_removes_nothing() {
        let (state, base_ns) = delete_state();
        let (status, _) = send(
            &state,
            "POST",
            &format!(
                "/loki/api/v1/delete?query={}&start={}&end={}",
                urlencoding(r#"{app="drop"}"#),
                (base_ns / 1_000_000_000) + 10,
                (base_ns / 1_000_000_000) + 20
            ),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert_eq!(lines_for(&state, r#"{app=~".+"}"#).await.len(), 2);
    }

    /// Listing reports what was asked for, and cancelling puts the lines back —
    /// which is only honest while the request has not been applied.
    #[tokio::test]
    async fn a_cancelled_request_makes_its_lines_readable_again() {
        let (state, base_ns) = delete_state();
        let window = format!(
            "start={}&end={}",
            (base_ns - 1_000_000_000) / 1_000_000_000,
            (base_ns + 1_000_000_000) / 1_000_000_000
        );
        send(
            &state,
            "POST",
            &format!(
                "/loki/api/v1/delete?query={}&{window}",
                urlencoding(r#"{app="drop"}"#)
            ),
        )
        .await;

        let (status, body) = send(&state, "GET", "/loki/api/v1/delete").await;
        assert_eq!(status, StatusCode::OK);
        let listed = body.as_array().unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0]["status"], "received");
        assert_eq!(listed[0]["query"], r#"{app="drop"}"#);
        let request_id = listed[0]["request_id"].as_str().unwrap().to_string();

        let (status, _) = send(
            &state,
            "DELETE",
            &format!("/loki/api/v1/delete?request_id={request_id}"),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert_eq!(lines_for(&state, r#"{app=~".+"}"#).await.len(), 2);

        let (_, body) = send(&state, "GET", "/loki/api/v1/delete").await;
        assert!(body.as_array().unwrap().is_empty());

        let (status, _) = send(
            &state,
            "DELETE",
            "/loki/api/v1/delete?request_id=never-existed",
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    /// A selector this cannot honour consistently is refused rather than
    /// accepted and half-applied. A pipeline stage names a value derived from
    /// the line, so what it deletes would change whenever the parser does.
    #[tokio::test]
    async fn a_deletion_query_must_be_a_selector() {
        let (state, base_ns) = delete_state();
        let window = format!(
            "start={}&end={}",
            base_ns / 1_000_000_000,
            base_ns / 1_000_000_000 + 1
        );
        for query in [
            r#"{app="drop"} | json | status="500""#,
            r#"rate({app="drop"}[1m])"#,
            r#"{}"#,
        ] {
            let (status, _) = send(
                &state,
                "POST",
                &format!(
                    "/loki/api/v1/delete?query={}&{window}",
                    urlencoding(query)
                ),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{query}");
        }

        // A window that ends before it starts deletes an empty set, which is
        // far more likely to be a mistake than an intent.
        let (status, _) = send(
            &state,
            "POST",
            &format!(
                "/loki/api/v1/delete?query={}&start=1700000010&end=1700000000",
                urlencoding(r#"{app="drop"}"#)
            ),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    fn stream_result(labels: &[(&str, &str)], mut entries: Vec<LogEntry>) -> StreamResult {
        for entry in &mut entries {
            entry
                .structured_metadata
                .extend(labels.iter().map(|(k, v)| (k.to_string(), v.to_string())));
            crate::memtable::canonicalize_structured_metadata(&mut entry.structured_metadata);
        }
        StreamResult {
            labels: SharedLabels::default(),
            entries,
        }
    }

    fn log_entry(timestamp_ns: i64, line: &str, metadata: &[(&str, &str)]) -> LogEntry {
        LogEntry {
            timestamp_ns,
            line: line.to_string(),
            structured_metadata: metadata
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect(),
        }
    }

    /// Loki's log response has no structured-metadata slot in the `values`
    /// tuple: measured against `grafana/loki:3.3.2`, a stream pushed with
    /// `trace_id` metadata answers `{app="probe"}` with `trace_id` among the
    /// `stream` labels and a two-element tuple. The three-element form is its
    /// opt-in `categorize-labels` encoding. loggytracy returned the triple
    /// unconditionally (`todo.md`, "Open correctness defects").
    #[test]
    fn a_log_response_promotes_pushed_metadata_into_the_stream_labels() {
        let data = build_stream_data(
            vec![stream_result(
                &[("app", "api")],
                vec![log_entry(
                    20,
                    "alpha",
                    &[("trace_id", "t1"), ("pod_ip", "10.0.0.1")],
                )],
            )],
            false,
        );
        assert_eq!(data.len(), 1);
        assert_eq!(
            data[0].stream.to_map(),
            [
                ("app", "api"),
                ("trace_id", "t1"),
                ("pod_ip", "10.0.0.1"),
            ]
            .into_iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect::<std::collections::BTreeMap<_, _>>()
        );
        assert_eq!(
            data[0].values,
            vec![("20".to_string(), "alpha".to_string())],
            "a values tuple carries the timestamp and the line and nothing else"
        );
    }

    /// The same slot, so the same fix: the fields `| json` extracted are stream
    /// labels on Loki. Driven through the pipeline rather than by handing
    /// `build_stream_data` a synthetic entry, because the extraction landing in
    /// the query-local `structured_metadata` is the internal representation the
    /// fix had to keep working.
    #[tokio::test]
    async fn a_log_response_promotes_json_extracted_fields_into_the_stream_labels() {
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let memtable = Arc::new(MemTable::new());
        memtable.insert(test_tenant(), with_attributes(labels.clone(), vec![log_entry(
                10,
                r#"{"level":"error","status":"500"}"#,
                &[("trace_id", "t1")],
            )]));
        let data_dir = temp_dir();
        let state = test_state(&data_dir, memtable, Arc::new(PartRegistry::new()), None);
        let parsed = logql::parse(r#"{app="api"} | json"#).unwrap();
        let results = run_unified_query(
            state,
            test_tenant(),
            parsed,
            crate::part::QueryTimeRange::closed(0, 20),
            10,
            false,
        )
        .await
        .unwrap();

        let data = build_stream_data(results, false);
        assert_eq!(data.len(), 1);
        let map = data[0].stream.to_map();
        let mut names: Vec<&str> = map.keys().map(String::as_str).collect();
        names.sort_unstable();
        assert_eq!(names, vec!["app", "level", "status", "trace_id"]);
        assert_eq!(data[0].stream.get("level"), Some("error"));
        assert_eq!(data[0].values[0], ("10".to_string(), "{\"level\":\"error\",\"status\":\"500\"}".to_string()));
    }

    /// The comparison corpus names its JSON free-text key `_msg` so that
    /// VictoriaLogs keeps the text as its message. On this side the name must
    /// stay an ordinary extracted field: reserved names are enforced for
    /// stream labels at ingest, not for what `| json` extracts, so `_msg`
    /// must extract, filter and promote like any other field.
    #[tokio::test]
    async fn a_json_field_named_msg_extracts_and_filters_like_any_other() {
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let memtable = Arc::new(MemTable::new());
        memtable.insert(test_tenant(), with_attributes(labels.clone(), vec![
                log_entry(10, r#"{"level":"error","_msg":"boom"}"#, &[]),
                log_entry(11, r#"{"level":"info","_msg":"fine"}"#, &[]),
            ]));
        let data_dir = temp_dir();
        let state = test_state(&data_dir, memtable, Arc::new(PartRegistry::new()), None);
        let parsed = logql::parse(r#"{app="api"} | json | _msg="boom""#).unwrap();
        let results = run_unified_query(
            state,
            test_tenant(),
            parsed,
            crate::part::QueryTimeRange::closed(0, 20),
            10,
            false,
        )
        .await
        .unwrap();

        let data = build_stream_data(results, false);
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].stream.get("_msg"), Some("boom"));
        assert_eq!(data[0].values.len(), 1);
    }

    /// An extraction whose name is a pushed metadata key is dropped rather than
    /// renamed, so `trace_id_extracted` — a name Loki's response never contained
    /// — must not appear. Loki 3.3.2 answers `| json | trace_id="<the JSON
    /// value>"` with nothing and renders `{{.trace_id}}` as the metadata value.
    #[tokio::test]
    async fn an_extraction_shadowed_by_pushed_metadata_never_reaches_the_response() {
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let memtable = Arc::new(MemTable::new());
        memtable.insert(test_tenant(), with_attributes(labels.clone(), vec![log_entry(
                10,
                r#"{"trace_id":"from-the-line","level":"error"}"#,
                &[("trace_id", "from-the-push")],
            )]));
        let data_dir = temp_dir();
        let state = test_state(&data_dir, memtable, Arc::new(PartRegistry::new()), None);
        let results = run_unified_query(
            state,
            test_tenant(),
            logql::parse(r#"{app="api"} | json"#).unwrap(),
            crate::part::QueryTimeRange::closed(0, 20),
            10,
            false,
        )
        .await
        .unwrap();

        let data = build_stream_data(results, false);
        assert_eq!(data[0].stream.get("trace_id"), Some("from-the-push"));
        assert!(data[0].stream.get("trace_id_extracted").is_none());
    }

    /// Rows whose promoted label sets differ are different streams, which is how
    /// Loki answers a stream with per-entry metadata, and rows whose label sets
    /// agree are one stream even when the scan produced them separately — with
    /// the requested direction preserved across the merge.
    #[test]
    fn promotion_regroups_rows_by_their_whole_label_set_and_keeps_the_direction() {
        let data = build_stream_data(
            vec![
                stream_result(
                    &[("app", "api")],
                    vec![
                        log_entry(30, "c", &[("trace_id", "t1")]),
                        log_entry(10, "a", &[("trace_id", "t2")]),
                    ],
                ),
                stream_result(
                    &[("app", "api")],
                    vec![log_entry(20, "b", &[("trace_id", "t1")])],
                ),
            ],
            false,
        );
        assert_eq!(data.len(), 2, "one stream per distinct promoted label set");
        let t1 = data
            .iter()
            .find(|stream| stream.stream.get("trace_id") == Some("t1"))
            .expect("t1 stream");
        assert_eq!(
            t1.values,
            vec![("30".to_string(), "c".to_string()), ("20".to_string(), "b".to_string())],
            "backward: descending time even though the two rows came from two inputs"
        );
        let forward = build_stream_data(
            vec![stream_result(
                &[("app", "api")],
                vec![log_entry(30, "c", &[]), log_entry(10, "a", &[])],
            )],
            true,
        );
        assert_eq!(
            forward[0].values,
            vec![
                ("10".to_string(), "a".to_string()),
                ("30".to_string(), "c".to_string())
            ]
        );
    }

    /// `label_format` over a stream label has to reach the response, because the
    /// row's label set genuinely changed: Loki 3.3.2 answers
    /// `{app="probe"} | label_format level="rewritten"` with `level="rewritten"`
    /// rather than with the stored value. `line_format` rewrites the line and
    /// leaves the label set alone.
    #[tokio::test]
    async fn label_format_over_a_stream_label_reaches_the_response() {
        let labels: Labels = [
            ("app".to_string(), "api".to_string()),
            ("level".to_string(), "stored".to_string()),
        ]
        .into_iter()
        .collect();
        let memtable = Arc::new(MemTable::new());
        memtable.insert(test_tenant(), with_attributes(labels.clone(), vec![log_entry(10, r#"{"status":"500"}"#, &[])]));
        let data_dir = temp_dir();
        let state = test_state(&data_dir, memtable, Arc::new(PartRegistry::new()), None);
        let results = run_unified_query(
            state,
            test_tenant(),
            logql::parse(r#"{app="api"} | json | label_format level="rewritten" | line_format "{{.status}}""#)
                .unwrap(),
            crate::part::QueryTimeRange::closed(0, 20),
            10,
            false,
        )
        .await
        .unwrap();

        let data = build_stream_data(results, false);
        assert_eq!(data[0].stream.get("level"), Some("rewritten"));
        assert_eq!(data[0].stream.get("status"), Some("500"));
        assert_eq!(data[0].values, vec![("10".to_string(), "500".to_string())]);
    }

/// Every refusal this classifier can see, and the one that used to be a fault.
///
/// The scan path reports `String`, so this function is the only thing standing
/// between a working limit and a `500`. An exhausted query memory pool reached
/// clients as `INTERNAL_SERVER_ERROR` until 2026-08-13 — measured once in a
/// 45-minute run at the capacity ceiling — which pages an operator for a
/// refusal and tells a client library that backing off is pointless. The other
/// arms are pinned beside it because the distinctions are the point: a broad
/// query is permanently wrong (`400`), a busy instance is temporarily unable
/// (`429`), and only an unrecognized error is a fault.
#[test]
fn a_refusal_is_never_reported_as_a_server_fault() {
    use crate::query_memory::EXHAUSTED_PREFIX;

    assert_eq!(
        metric_error_status(&format!("{EXHAUSTED_PREFIX} 322122547 bytes is exhausted")),
        StatusCode::TOO_MANY_REQUESTS,
        "an instance out of query memory is busy, not broken"
    );
    assert_eq!(
        metric_error_status(&format!("{TENANT_QUOTA_PREFIX}scan rate exceeded")),
        StatusCode::TOO_MANY_REQUESTS
    );
    assert_eq!(
        metric_error_status("query exceeds the maximum of 1000000 scanned rows"),
        StatusCode::BAD_REQUEST,
        "a query too broad to answer is permanently too broad"
    );
    assert_eq!(
        metric_error_status("query timed out"),
        StatusCode::GATEWAY_TIMEOUT
    );
    assert_eq!(
        metric_error_status("part reader returned garbage"),
        StatusCode::INTERNAL_SERVER_ERROR,
        "and something nobody recognizes is still a fault"
    );
}

/// The runbook decides what to alert on; `deploy/alerts.yml` is that decision
/// in the form a Prometheus can load. Two files holding one set of decisions
/// drift, and the drift is silent in the worst direction: the operator is
/// paged by neither because they believe the other has it. So the tie is a
/// test rather than a convention.
///
/// Three ways it can fail, and each is a real defect rather than tidiness. A
/// row added to the table with no rule is an alert someone decided to have and
/// nobody will receive. A rule naming a metric this engine does not export is a
/// rule that can never fire — a renamed metric leaves exactly this shape. And a
/// rule on a signal the table never listed is a page whose meaning is written
/// down nowhere.
#[test]
fn every_alert_signal_in_the_runbook_has_a_rule() {
    let runbook = include_str!("../../docs/RUNBOOK.md");
    let rules = include_str!("../../deploy/alerts.yml");
    let exported = include_str!("handlers.rs");

    // The signals are the first column of the "What to alert on" table, one
    // backticked metric per row.
    let table = runbook
        .split_once("## What to alert on")
        .expect("the runbook must still have the table these rules come from")
        .1;
    let table = table.split("\n## ").next().unwrap();
    let signals: Vec<&str> = table
        .lines()
        .filter_map(|line| line.strip_prefix("| `loggytracy_"))
        .filter_map(|rest| rest.split_once('`').map(|(name, _)| name))
        .collect();
    // A row's condition and meaning name metrics too — "increasing while
    // `flush_success_total` is flat" is half of an expression, and the memory
    // row sends the reader to `loggytracy_query_latency_ms`. Those are
    // explained by the table even though they are not its signals, so a rule
    // may reach for them.
    let mentioned: Vec<&str> = table
        .match_indices('`')
        .filter_map(|(at, _)| table[at + 1..].split_once('`').map(|(name, _)| name))
        .map(|name| name.trim_start_matches("loggytracy_"))
        .collect();
    assert!(
        signals.len() > 10,
        "the table parsed as {} rows, which means its shape changed, not that it shrank",
        signals.len()
    );

    let missing: Vec<&&str> = signals
        .iter()
        .filter(|name| !rules.contains(&format!("loggytracy_{name}")))
        .collect();
    assert!(
        missing.is_empty(),
        "these signals are in the runbook's table but no rule in deploy/alerts.yml uses them: {missing:#?}"
    );

    // Every metric a rule names must be one this engine actually renders, and
    // must be a signal the table explains. Helper metrics a rule divides or
    // guards by are still the first: an expression cannot evaluate against a
    // series nobody exports.
    let mut rest = rules;
    let mut unexported: Vec<String> = Vec::new();
    let mut unexplained: Vec<String> = Vec::new();
    while let Some(start) = rest.find("loggytracy_") {
        rest = &rest[start..];
        let end = rest
            .find(|c: char| !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'))
            .unwrap_or(rest.len());
        let name = rest[..end].to_string();
        rest = &rest[end..];
        let bare = name.trim_start_matches("loggytracy_").to_string();
        if !exported.contains(&name) && !unexported.contains(&name) {
            unexported.push(name.clone());
        }
        // A histogram is exported under its base name and queried by its
        // derived series, so the table naming the base is enough.
        let explained = signals
            .iter()
            .chain(mentioned.iter())
            .any(|known| bare == *known || bare.starts_with(known));
        // Guards and denominators are not signals; they are named here so the
        // list of what a rule may reach for stays visible.
        const HELPERS: [&str; 3] = [
            "loggytracy_part_count",
            "loggytracy_tenant_policy_known_tenants",
            "loggytracy_drain_in_progress",
        ];
        if !explained && !HELPERS.contains(&name.as_str()) && !unexplained.contains(&name) {
            unexplained.push(name);
        }
    }
    assert!(
        unexported.is_empty(),
        "deploy/alerts.yml names metrics this engine does not export: {unexported:#?}"
    );
    assert!(
        unexplained.is_empty(),
        "deploy/alerts.yml alerts on signals the runbook's table does not explain: {unexplained:#?}"
    );
}

/// Loki filters `label/{name}/values` by a stream selector, and a dropdown
/// built without it offers values from streams the user is not looking at —
/// clicking one returns nothing, which reads as data loss rather than as a
/// wrong dropdown.
#[tokio::test]
async fn label_values_are_filtered_by_the_query_selector() {
    let data_dir = temp_dir();
    let memtable = Arc::new(MemTable::new());
    for (app, env) in [("api", "prod"), ("api", "staging"), ("worker", "prod")] {
        memtable.insert(test_tenant(), with_attributes([
                ("app".to_string(), app.to_string()),
                ("env".to_string(), env.to_string()),
            ]
            .into_iter()
            .collect(), vec![LogEntry {
                timestamp_ns: 1,
                line: format!("{app} in {env}"),
                structured_metadata: Vec::new(),
            }]));
    }
    let state = state_with(&data_dir, memtable, Arc::new(PartRegistry::new()));

    let values_for = async |query: Option<&str>| {
        label_values(
            State(state.clone()),
            crate::tenant::test_tenant_headers(),
            Path("env".to_string()),
            Query(crate::query::MetadataParams {
                start: None,
                end: None,
                query: query.map(str::to_string),
            }),
        )
        .await
    };

    let mut unfiltered = values_for(None).await.unwrap().0.data;
    unfiltered.sort();
    assert_eq!(
        unfiltered,
        vec!["prod".to_string(), "staging".to_string()],
        "with no selector every value the tenant holds is offered"
    );

    assert_eq!(
        values_for(Some("{app=\"worker\"}")).await.unwrap().0.data,
        vec!["prod".to_string()],
        "worker only runs in prod, so staging must not be offered for it"
    );

    // An empty selector is not a filter that matches nothing.
    let mut blank = values_for(Some("  ")).await.unwrap().0.data;
    blank.sort();
    assert_eq!(blank, unfiltered);

    // A line filter cannot narrow stream labels, so it is refused rather than
    // dropped: answering as though it were absent returns a superset while
    // looking like it applied.
    let Err((status, message)) = values_for(Some("{app=\"api\"} |= \"boom\"")).await else {
        panic!("a line filter must be refused");
    };
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(message.contains("line filter"), "{message}");
}

// --- The first-party API: parameter grammar (`params.rs`) ---

#[test]
fn attr_operator_scan_takes_the_longest_match_at_the_first_operator() {
    let params = parse_filter_params("attr=level=error", 0, LOGS_PARAMS).unwrap();
    let matcher = &params.query.matchers[0];
    assert_eq!(matcher.name, "level");
    assert_eq!(matcher.op, logql::MatcherOp::Eq);
    assert_eq!(matcher.value, "error");

    let params = parse_filter_params("attr=level!%3Ddebug", 0, LOGS_PARAMS).unwrap();
    assert_eq!(params.query.matchers[0].op, logql::MatcherOp::Neq);
    assert_eq!(params.query.matchers[0].value, "debug");

    let params = parse_filter_params("attr=path%3D~/api/.*", 0, LOGS_PARAMS).unwrap();
    assert_eq!(params.query.matchers[0].op, logql::MatcherOp::Re);

    let params = parse_filter_params("attr=host!~db-.*", 0, LOGS_PARAMS).unwrap();
    assert_eq!(params.query.matchers[0].op, logql::MatcherOp::NRe);

    // The value keeps any later operator characters verbatim.
    let params = parse_filter_params("attr=formula=a%3Db", 0, LOGS_PARAMS).unwrap();
    assert_eq!(params.query.matchers[0].name, "formula");
    assert_eq!(params.query.matchers[0].value, "a=b");
}

#[test]
fn attr_without_an_operator_teaches_the_form() {
    let error = parse_filter_params("attr=level", 0, LOGS_PARAMS).unwrap_err();
    assert!(error.contains("has no operator"), "{error}");
    assert!(error.contains("attr=key=value"), "{error}");

    let error = parse_filter_params("attr=%3Derror", 0, LOGS_PARAMS).unwrap_err();
    assert!(error.contains("empty key"), "{error}");
}

#[test]
fn unknown_parameters_name_themselves_and_the_accepted_set() {
    let error = parse_filter_params("atr=level=error", 0, LOGS_PARAMS).unwrap_err();
    assert!(error.contains("unknown parameter 'atr'"), "{error}");
    assert!(error.contains("attr"), "{error}");
    assert!(error.contains("docs/QUERY_API.md"), "{error}");
}

#[test]
fn relative_times_need_a_unit_and_negative_epochs_stay_epochs() {
    let now_ns = 1_700_000_000_000_000_000;
    assert_eq!(
        parse_time_or_relative_ns("-1h", now_ns).unwrap(),
        now_ns - 3_600_000_000_000
    );
    assert_eq!(
        parse_time_or_relative_ns("-90s", now_ns).unwrap(),
        now_ns - 90_000_000_000
    );
    assert_eq!(
        parse_time_or_relative_ns("-5", now_ns).unwrap(),
        -5_000_000_000
    );
    let error = parse_time_or_relative_ns("-1hh", now_ns).unwrap_err();
    assert!(error.contains("invalid relative time"), "{error}");
}

#[test]
fn parse_json_recompiles_attr_filters_into_field_stages() {
    let params =
        parse_filter_params("parse=json&attr=status=500&attr=path%3D~/api/.*", 0, LOGS_PARAMS)
            .unwrap();
    assert!(params.query.matchers.is_empty());
    assert!(matches!(
        params.query.stages[0],
        logql::PipelineStage::Json
    ));
    let logql::PipelineStage::Field(filter) = &params.query.stages[1] else {
        panic!("attr must become a field filter after parse=");
    };
    assert_eq!(filter.name, "status");
    assert_eq!(filter.op, logql::FieldOp::Eq);
    assert!(matches!(
        &params.query.stages[2],
        logql::PipelineStage::Field(filter) if filter.op == logql::FieldOp::Regex
    ));

    let error = parse_filter_params("parse=json&parse=json", 0, LOGS_PARAMS).unwrap_err();
    assert!(error.contains("more than once"), "{error}");
    let error = parse_filter_params("parse=regex", 0, LOGS_PARAMS).unwrap_err();
    assert!(error.contains("expected json or logfmt"), "{error}");
}

#[test]
fn scalar_parameters_refuse_duplicates() {
    let error = parse_filter_params("start=1&start=2", 0, LOGS_PARAMS).unwrap_err();
    assert!(error.contains("'start' was given more than once"), "{error}");
}

// --- The first-party API: `GET /loggytracy/api/v1/logs` ---

async fn first_party_logs(
    state: Arc<AppState>,
    query_string: &str,
) -> (StatusCode, axum::http::HeaderMap, String) {
    let request = axum::http::Request::builder()
        .uri(format!("/loggytracy/api/v1/logs?{query_string}"))
        .header(crate::tenant::TENANT_HEADER, test_tenant().as_str())
        .body(axum::body::Body::empty())
        .unwrap();
    let response = crate::build_router(state).oneshot(request).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    (status, headers, String::from_utf8(body.to_vec()).unwrap())
}

fn ndjson_rows(body: &str) -> Vec<serde_json::Value> {
    body.lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[tokio::test]
async fn first_party_logs_answers_ndjson_with_string_timestamps() {
    let data_dir = temp_dir();
    let labels: Labels = [("app".to_string(), "api".to_string())]
        .into_iter()
        .collect();
    let memtable = Arc::new(MemTable::new());
    memtable.insert(
        test_tenant(),
        with_attributes(
            labels,
            vec![
                LogEntry {
                    timestamp_ns: 5_000_000_000,
                    line: "hello timeout".to_string(),
                    structured_metadata: Vec::new(),
                },
                LogEntry {
                    timestamp_ns: 6_000_000_000,
                    line: "unrelated".to_string(),
                    structured_metadata: Vec::new(),
                },
            ],
        ),
    );
    let state = test_state(&data_dir, memtable, Arc::new(PartRegistry::new()), None);

    let (status, headers, body) =
        first_party_logs(state, "start=4&end=7&attr=app=api&contains=timeout").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers[axum::http::header::CONTENT_TYPE],
        "application/x-ndjson"
    );
    assert!(headers.contains_key(SCANNED_ROWS_HEADER));
    let rows = ndjson_rows(&body);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["line"], "hello timeout");
    assert_eq!(rows[0]["timestamp"], "5000000000");
    assert!(rows[0]["timestamp"].is_string());
    assert_eq!(rows[0]["attributes"]["app"], "api");
}

#[tokio::test]
async fn first_party_logs_default_direction_is_backward_and_limit_applies() {
    let data_dir = temp_dir();
    let memtable = Arc::new(MemTable::new());
    memtable.insert(
        test_tenant(),
        (1..=5)
            .map(|at| LogEntry {
                timestamp_ns: at * 1_000_000_000,
                line: format!("row {at}"),
                structured_metadata: vec![("app".to_string(), "api".to_string())],
            })
            .collect(),
    );
    let state = test_state(&data_dir, memtable, Arc::new(PartRegistry::new()), None);

    let (status, _, body) = first_party_logs(state.clone(), "start=0&end=10&limit=2").await;
    assert_eq!(status, StatusCode::OK);
    let rows = ndjson_rows(&body);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["line"], "row 5");
    assert_eq!(rows[1]["line"], "row 4");

    let (_, _, body) =
        first_party_logs(state, "start=0&end=10&limit=2&direction=forward").await;
    let rows = ndjson_rows(&body);
    assert_eq!(rows[0]["line"], "row 1");
    assert_eq!(rows[1]["line"], "row 2");
}

#[tokio::test]
async fn first_party_logs_selects_without_any_filter() {
    let data_dir = temp_dir();
    let memtable = Arc::new(MemTable::new());
    memtable.insert(
        test_tenant(),
        vec![LogEntry {
            timestamp_ns: 5_000_000_000,
            line: "bare row".to_string(),
            structured_metadata: Vec::new(),
        }],
    );
    let state = test_state(&data_dir, memtable, Arc::new(PartRegistry::new()), None);

    let (status, _, body) = first_party_logs(state, "start=4&end=6").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ndjson_rows(&body).len(), 1);
}

#[tokio::test]
async fn first_party_logs_parse_json_filters_on_extracted_fields() {
    let data_dir = temp_dir();
    let memtable = Arc::new(MemTable::new());
    memtable.insert(
        test_tenant(),
        vec![
            LogEntry {
                timestamp_ns: 5_000_000_000,
                line: r#"{"status":"500","path":"/api/x"}"#.to_string(),
                structured_metadata: vec![("app".to_string(), "api".to_string())],
            },
            LogEntry {
                timestamp_ns: 6_000_000_000,
                line: r#"{"status":"200","path":"/api/y"}"#.to_string(),
                structured_metadata: vec![("app".to_string(), "api".to_string())],
            },
        ],
    );
    let state = test_state(&data_dir, memtable, Arc::new(PartRegistry::new()), None);

    let (status, _, body) = first_party_logs(
        state,
        "start=4&end=7&parse=json&attr=app=api&attr=status=500",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let rows = ndjson_rows(&body);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["line"], r#"{"status":"500","path":"/api/x"}"#);
}

#[tokio::test]
async fn first_party_logs_refuses_unknown_parameters_with_a_teaching_error() {
    let data_dir = temp_dir();
    let state = test_state(
        &data_dir,
        Arc::new(MemTable::new()),
        Arc::new(PartRegistry::new()),
        None,
    );

    let (status, _, body) = first_party_logs(state, "atr=app=api").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let error: serde_json::Value = serde_json::from_str(&body).unwrap();
    let message = error["error"].as_str().unwrap();
    assert!(message.contains("unknown parameter 'atr'"), "{message}");
    assert!(message.contains("docs/QUERY_API.md"), "{message}");
}

#[tokio::test]
async fn unmatched_routes_answer_with_the_first_party_surface() {
    let data_dir = temp_dir();
    let state = test_state(
        &data_dir,
        Arc::new(MemTable::new()),
        Arc::new(PartRegistry::new()),
        None,
    );
    let request = axum::http::Request::builder()
        .uri("/loggytracy/api/v1/nope")
        .body(axum::body::Body::empty())
        .unwrap();
    let response = crate::build_router(state).oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let error: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let message = error["error"].as_str().unwrap();
    assert!(message.contains("/loggytracy/api/v1/logs"), "{message}");
}

// --- The first-party API: `GET /loggytracy/api/v1/logs/histogram` ---

async fn first_party_histogram(
    state: Arc<AppState>,
    query_string: &str,
) -> (StatusCode, String) {
    let request = axum::http::Request::builder()
        .uri(format!("/loggytracy/api/v1/logs/histogram?{query_string}"))
        .header(crate::tenant::TENANT_HEADER, test_tenant().as_str())
        .body(axum::body::Body::empty())
        .unwrap();
    let response = crate::build_router(state).oneshot(request).await.unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    (status, String::from_utf8(body.to_vec()).unwrap())
}

/// The boundary test the plan calls the likeliest silent bug: a row exactly on
/// a bucket boundary belongs to the bucket it starts, `[start, end)` at every
/// bucket, including the partial first and last.
#[tokio::test]
async fn histogram_buckets_are_half_open_and_epoch_aligned() {
    const BUCKET_NS: i64 = 10_000_000_000;
    let data_dir = temp_dir();
    let memtable = Arc::new(MemTable::new());
    memtable.insert(
        test_tenant(),
        [
            BUCKET_NS,                  // exactly on a bucket start
            BUCKET_NS + 1,              // inside the same bucket
            2 * BUCKET_NS - 1,          // last instant of the same bucket
            2 * BUCKET_NS,              // exactly on the next bucket start
        ]
        .into_iter()
        .map(|timestamp_ns| LogEntry {
            timestamp_ns,
            line: "row".to_string(),
            structured_metadata: Vec::new(),
        })
        .collect(),
    );
    let state = test_state(&data_dir, memtable, Arc::new(PartRegistry::new()), None);

    // The query range is not bucket-aligned; the buckets must be.
    let (status, body) = first_party_histogram(
        state,
        "start=9.999999997&end=20.000000003&bucket=10s",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let rows = ndjson_rows(&body);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["bucket_start"], (0).to_string());
    assert_eq!(rows[0]["bucket_end"], BUCKET_NS.to_string());
    assert_eq!(rows[0]["count"], 0);
    assert_eq!(rows[1]["bucket_start"], BUCKET_NS.to_string());
    assert_eq!(rows[1]["count"], 3);
    assert_eq!(rows[2]["bucket_start"], (2 * BUCKET_NS).to_string());
    assert_eq!(rows[2]["count"], 1);
}

/// The partial first and last buckets count only rows inside the query range.
#[tokio::test]
async fn histogram_clips_partial_buckets_to_the_range() {
    const BUCKET_NS: i64 = 10_000_000_000;
    let data_dir = temp_dir();
    let memtable = Arc::new(MemTable::new());
    memtable.insert(
        test_tenant(),
        [
            BUCKET_NS + 2, // in the first bucket, before start
            BUCKET_NS + 5, // in range
            BUCKET_NS + 7, // on end: excluded
            BUCKET_NS + 8, // after end, same bucket
        ]
        .into_iter()
        .map(|timestamp_ns| LogEntry {
            timestamp_ns,
            line: "row".to_string(),
            structured_metadata: Vec::new(),
        })
        .collect(),
    );
    let state = test_state(&data_dir, memtable, Arc::new(PartRegistry::new()), None);

    let (status, body) = first_party_histogram(
        state,
        "start=10.000000004&end=10.000000007&bucket=10s",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let rows = ndjson_rows(&body);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["count"], 1);
}

/// The histogram and the search endpoint must agree on what the filters
/// select: `sum(count)` equals the row count `/logs` returns.
#[tokio::test]
async fn histogram_counts_agree_with_the_search_endpoint() {
    let data_dir = temp_dir();
    let memtable = Arc::new(MemTable::new());
    memtable.insert(
        test_tenant(),
        (1..=20)
            .map(|at| LogEntry {
                timestamp_ns: at * 1_000_000_000,
                line: if at % 2 == 0 {
                    format!("timeout {at}")
                } else {
                    format!("fine {at}")
                },
                structured_metadata: vec![("app".to_string(), "api".to_string())],
            })
            .collect(),
    );
    let state = test_state(&data_dir, memtable, Arc::new(PartRegistry::new()), None);

    let filters = "start=0&end=30&attr=app=api&contains=timeout";
    let (_, _, search_body) =
        first_party_logs(state.clone(), &format!("{filters}&limit=1000")).await;
    let (status, histogram_body) =
        first_party_histogram(state, &format!("{filters}&bucket=5s")).await;
    assert_eq!(status, StatusCode::OK, "{histogram_body}");
    let total: u64 = ndjson_rows(&histogram_body)
        .iter()
        .map(|bucket| bucket["count"].as_u64().unwrap())
        .sum();
    assert_eq!(total, ndjson_rows(&search_body).len() as u64);
    assert_eq!(total, 10);
}

#[tokio::test]
async fn histogram_refuses_a_bucket_count_over_the_cap() {
    let data_dir = temp_dir();
    let state = test_state(
        &data_dir,
        Arc::new(MemTable::new()),
        Arc::new(PartRegistry::new()),
        None,
    );
    let (status, body) =
        first_party_histogram(state, "start=0&end=100000&bucket=1s").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let error: serde_json::Value = serde_json::from_str(&body).unwrap();
    let message = error["error"].as_str().unwrap();
    assert!(message.contains("buckets"), "{message}");
    assert!(message.contains("widen bucket="), "{message}");
}

#[tokio::test]
async fn histogram_rejects_search_only_parameters() {
    let data_dir = temp_dir();
    let state = test_state(
        &data_dir,
        Arc::new(MemTable::new()),
        Arc::new(PartRegistry::new()),
        None,
    );
    let (status, body) = first_party_histogram(state, "start=0&end=10&limit=5").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("unknown parameter 'limit'"), "{body}");
}

// --- The first-party API: attribute autocomplete ---

async fn first_party_get(state: Arc<AppState>, path_and_query: &str) -> (StatusCode, String) {
    let request = axum::http::Request::builder()
        .uri(path_and_query)
        .header(crate::tenant::TENANT_HEADER, test_tenant().as_str())
        .body(axum::body::Body::empty())
        .unwrap();
    let response = crate::build_router(state).oneshot(request).await.unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    (status, String::from_utf8(body.to_vec()).unwrap())
}

#[tokio::test]
async fn attribute_keys_come_from_the_memtable_and_the_parts() {
    let data_dir = temp_dir();
    let memtable = Arc::new(MemTable::new());
    memtable.insert(
        test_tenant(),
        vec![LogEntry {
            timestamp_ns: 5_000_000_000,
            line: "row".to_string(),
            structured_metadata: vec![
                ("app".to_string(), "api".to_string()),
                ("level".to_string(), "error".to_string()),
            ],
        }],
    );
    let parts = Arc::new(PartRegistry::new());
    parts
        .register(
            part::flush_rows(
                vec![Row {
                    tenant: test_tenant(),
                    timestamp_ns: 6_000_000_000,
                    line: "part row".to_string(),
                    structured_metadata: vec![("host".to_string(), "db-1".to_string())],
                }],
                &data_dir.join("parts"),
                8,
            )
            .unwrap(),
        )
        .unwrap();
    let state = test_state(&data_dir, memtable, parts, None);

    let (status, body) =
        first_party_get(state, "/loggytracy/api/v1/logs/attributes?start=4&end=7").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let keys: Vec<String> = ndjson_rows(&body)
        .iter()
        .map(|row| row["key"].as_str().unwrap().to_string())
        .collect();
    assert!(keys.contains(&"app".to_string()), "{keys:?}");
    assert!(keys.contains(&"level".to_string()), "{keys:?}");
    assert!(keys.contains(&"host".to_string()), "{keys:?}");
}

#[tokio::test]
async fn attribute_values_narrow_by_attr_filters() {
    let data_dir = temp_dir();
    let memtable = Arc::new(MemTable::new());
    memtable.insert(
        test_tenant(),
        vec![
            LogEntry {
                timestamp_ns: 5_000_000_000,
                line: "row".to_string(),
                structured_metadata: vec![
                    ("app".to_string(), "api".to_string()),
                    ("level".to_string(), "error".to_string()),
                ],
            },
            LogEntry {
                timestamp_ns: 6_000_000_000,
                line: "row".to_string(),
                structured_metadata: vec![
                    ("app".to_string(), "worker".to_string()),
                    ("level".to_string(), "info".to_string()),
                ],
            },
        ],
    );
    let state = test_state(&data_dir, memtable, Arc::new(PartRegistry::new()), None);

    let (status, body) = first_party_get(
        state.clone(),
        "/loggytracy/api/v1/logs/attributes/level/values?start=4&end=7",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let values: Vec<String> = ndjson_rows(&body)
        .iter()
        .map(|row| row["value"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(values, vec!["error".to_string(), "info".to_string()]);

    let (status, body) = first_party_get(
        state,
        "/loggytracy/api/v1/logs/attributes/level/values?start=4&end=7&attr=app=api",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let values: Vec<String> = ndjson_rows(&body)
        .iter()
        .map(|row| row["value"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(values, vec!["error".to_string()]);
}

#[tokio::test]
async fn attribute_values_refuse_line_filters_by_the_unknown_parameter_rule() {
    let data_dir = temp_dir();
    let state = test_state(
        &data_dir,
        Arc::new(MemTable::new()),
        Arc::new(PartRegistry::new()),
        None,
    );
    let (status, body) = first_party_get(
        state,
        "/loggytracy/api/v1/logs/attributes/level/values?contains=x",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("unknown parameter 'contains'"), "{body}");
}

// --- The first-party API: `GET /loggytracy/api/v1/logs/tail` ---

#[tokio::test]
async fn first_party_tail_streams_the_backlog_as_ndjson_rows() {
    use futures_util::StreamExt;

    let data_dir = temp_dir();
    let memtable = Arc::new(MemTable::new());
    let now_ns = 1_700_000_000_000_000_000;
    memtable.insert(
        test_tenant(),
        vec![
            LogEntry {
                timestamp_ns: now_ns,
                line: "tailed line".to_string(),
                structured_metadata: vec![("app".to_string(), "api".to_string())],
            },
            LogEntry {
                timestamp_ns: now_ns + 1,
                line: "second line".to_string(),
                structured_metadata: vec![("app".to_string(), "api".to_string())],
            },
        ],
    );
    let state = test_state(&data_dir, memtable, Arc::new(PartRegistry::new()), None);

    let request = axum::http::Request::builder()
        .uri(format!(
            "/loggytracy/api/v1/logs/tail?start={now_ns}&attr=app=api"
        ))
        .header(crate::tenant::TENANT_HEADER, test_tenant().as_str())
        .body(axum::body::Body::empty())
        .unwrap();
    let response = crate::build_router(state).oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[axum::http::header::CONTENT_TYPE],
        "application/x-ndjson"
    );
    let mut body = response.into_body().into_data_stream();
    let chunk = tokio::time::timeout(Duration::from_secs(5), body.next())
        .await
        .expect("the first poll is immediate")
        .expect("the stream is open")
        .expect("the chunk is data");
    let text = String::from_utf8(chunk.to_vec()).unwrap();
    let rows = ndjson_rows(&text);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["line"], "tailed line");
    assert_eq!(rows[0]["attributes"]["app"], "api");
    assert_eq!(rows[1]["line"], "second line");
}

#[tokio::test]
async fn first_party_tail_ends_cleanly_on_drain() {
    let data_dir = temp_dir();
    let state = test_state(
        &data_dir,
        Arc::new(MemTable::new()),
        Arc::new(PartRegistry::new()),
        None,
    );
    state.shutdown.begin_drain();

    let request = axum::http::Request::builder()
        .uri("/loggytracy/api/v1/logs/tail")
        .header(crate::tenant::TENANT_HEADER, test_tenant().as_str())
        .body(axum::body::Body::empty())
        .unwrap();
    let response = crate::build_router(state).oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = tokio::time::timeout(
        Duration::from_secs(5),
        axum::body::to_bytes(response.into_body(), 1024 * 1024),
    )
    .await
    .expect("a draining tail closes its stream")
    .unwrap();
    assert!(body.is_empty());
}

#[tokio::test]
async fn first_party_tail_refuses_what_a_tail_cannot_honour() {
    let data_dir = temp_dir();
    let state = test_state(
        &data_dir,
        Arc::new(MemTable::new()),
        Arc::new(PartRegistry::new()),
        None,
    );

    let (status, body) =
        first_party_get(state.clone(), "/loggytracy/api/v1/logs/tail?direction=forward").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("unknown parameter 'direction'"), "{body}");

    let held: Vec<_> = (0..state.config.max_concurrent_tails)
        .map(|_| {
            Arc::clone(&state.tail_semaphore)
                .try_acquire_owned()
                .expect("the limit is not reached yet")
        })
        .collect();
    let (status, body) = first_party_get(state, "/loggytracy/api/v1/logs/tail").await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert!(body.contains("live tail connections"), "{body}");
    drop(held);
}
