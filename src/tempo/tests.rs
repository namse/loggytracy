    use super::*;
    use crate::tenant::test_tenant;
    use crate::config::Config;
    use crate::journal::Journal;
    use crate::memtable::MemTable;
    use crate::part_registry::PartRegistry;
    use crate::tenant_policy::{TenantPolicy, TenantRetention};
    use crate::trace::{TraceMemTable, normalize_request};
    use crate::trace_registry::TraceRegistry;
    use axum::extract::Path;
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

    fn test_state() -> Arc<AppState> {
        let config = Config {
            data_dir: std::env::temp_dir()
                .join(format!("loggytracy-tempo-{}", uuid::Uuid::new_v4())),
            ..Config::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let trace_memtable = Arc::new(TraceMemTable::new());
        let memtable = Arc::new(MemTable::new());
        let journal = Arc::new(
            Journal::spawn_with_traces(&config, memtable.clone(), trace_memtable.clone())
                .unwrap(),
        );
        let parts = Arc::new(PartRegistry::new());
        let trace_parts = Arc::new(TraceRegistry::new(parts.operation_lock()));
        trace_memtable.insert(
            normalize_request(&test_tenant(), ExportTraceServiceRequest {
                resource_spans: vec![ResourceSpans {
                    resource: None,
                    scope_spans: vec![ScopeSpans {
                        scope: None,
                        spans: vec![Span {
                            trace_id: vec![1; 16],
                            span_id: vec![2; 8],
                            start_time_unix_nano: 100,
                            end_time_unix_nano: 250,
                            name: "GET_items".to_string(),
                            ..Default::default()
                        }],
                        schema_url: String::new(),
                    }],
                    schema_url: String::new(),
                }],
            })
            .unwrap(),
        );
        crate::test_support::state(config, memtable, journal, parts, trace_parts, None)
    }

    #[tokio::test]
    async fn trace_by_id_returns_tempo_batches() {
        let response = trace_by_id(State(test_state()), crate::tenant::test_tenant_headers(), Path("01".repeat(16)))
            .await
            .unwrap()
            .0;
        assert!(
            response
                .get("batches")
                .and_then(|value| value.as_array())
                .is_some()
        );
        assert_eq!(
            response["batches"][0]["instrumentationLibrarySpans"][0]["spans"][0]["name"],
            "GET_items"
        );
    }

    #[tokio::test]
    async fn trace_search_returns_trace_summary_and_rejects_bad_ids() {
        let response = search(
            State(test_state()),
            crate::tenant::test_tenant_headers(),
            Query(SearchParams {
                tags: Some("name=GET_items".to_string()),
                start: None,
                end: None,
                limit: Some(10),
                min_duration: None,
                max_duration: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(response["traces"].as_array().unwrap().len(), 1);

        let error = trace_by_id(State(test_state()), crate::tenant::test_tenant_headers(), Path("bad".to_string()))
            .await
            .unwrap_err();
        assert_eq!(error.0, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn trace_search_filters_on_child_but_summarizes_the_full_trace() {
        let state = test_state();
        state.journal.trace_memtable().insert(
            normalize_request(&test_tenant(), ExportTraceServiceRequest {
                resource_spans: vec![ResourceSpans {
                    resource: None,
                    scope_spans: vec![ScopeSpans {
                        scope: None,
                        spans: vec![Span {
                            trace_id: vec![1; 16],
                            span_id: vec![3; 8],
                            parent_span_id: vec![2; 8],
                            start_time_unix_nano: 250,
                            end_time_unix_nano: 650,
                            name: "child".to_string(),
                            attributes: vec![KeyValue {
                                key: "http.route".to_string(),
                                value: Some(AnyValue {
                                    value: Some(any_value::Value::StringValue(
                                        "/items".to_string(),
                                    )),
                                }),
                                key_strindex: 0,
                            }],
                            ..Default::default()
                        }],
                        schema_url: String::new(),
                    }],
                    schema_url: String::new(),
                }],
            })
            .unwrap(),
        );

        let response = search(
            State(state),
            crate::tenant::test_tenant_headers(),
            Query(SearchParams {
                tags: Some("http.route=/items".to_string()),
                start: None,
                end: None,
                limit: Some(10),
                min_duration: Some("500ns".to_string()),
                max_duration: None,
            }),
        )
        .await
        .unwrap()
        .0;
        let trace = &response["traces"][0];

        assert_eq!(trace["rootTraceName"], "GET_items");
        assert_eq!(trace["startTimeUnixNano"], "100");
        assert!((trace["durationMs"].as_f64().unwrap() - 0.00055).abs() < f64::EPSILON);
    }

    const HOUR_NS: i64 = 60 * 60 * 1_000_000_000;

    fn span_at(trace_id: &str, span_id: &str, start_time_ns: i64, attribute: &str) -> TraceSpan {
        TraceSpan {
            tenant: test_tenant(),
            trace_id: trace_id.to_string(),
            span_id: span_id.to_string(),
            start_time_ns,
            end_time_ns: start_time_ns + 1_000,
            span: Span {
                name: format!("span-{span_id}"),
                attributes: vec![KeyValue {
                    key: attribute.to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(format!("{attribute}-value"))),
                    }),
                    key_strindex: 0,
                }],
                ..Default::default()
            },
            resource: None,
            resource_schema_url: String::new(),
            scope: None,
            scope_schema_url: String::new(),
        }
    }

    /// Three traces around a retention floor of one hour: one entirely below
    /// it, one straddling it, and one entirely above it. Timestamps are
    /// relative to now because the floor is `now - retention`.
    fn retention_state(policy_entries: &[(&str, TenantRetention)]) -> (Arc<AppState>, i64) {
        let now_ns = crate::tenant_policy::now_ns();
        let config = Config {
            data_dir: std::env::temp_dir()
                .join(format!("loggytracy-tempo-floor-{}", uuid::Uuid::new_v4())),
            ..Config::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let trace_memtable = Arc::new(TraceMemTable::new());
        let memtable = Arc::new(MemTable::new());
        let journal = Arc::new(
            Journal::spawn_with_traces(&config, memtable.clone(), trace_memtable.clone()).unwrap(),
        );
        trace_memtable.insert(vec![
            span_at(&"aa".repeat(16), "old-only", now_ns - 2 * HOUR_NS, "old.attribute"),
            span_at(&"bb".repeat(16), "straddle-old", now_ns - 2 * HOUR_NS, "old.attribute"),
            span_at(&"bb".repeat(16), "straddle-new", now_ns - 60_000_000_000, "fresh.attribute"),
            span_at(&"cc".repeat(16), "fresh-only", now_ns - 60_000_000_000, "fresh.attribute"),
        ]);
        let policy = Arc::new(TenantPolicy::enabled_for_test());
        policy.install_for_test(
            policy_entries
                .iter()
                .map(|(name, retention)| {
                    (
                        crate::tenant::TenantId::parse(name).expect("valid tenant id"),
                        *retention,
                    )
                })
                .collect(),
        );
        let state = crate::test_support::state_with_tenant_policy(
            config,
            memtable,
            journal,
            Arc::new(PartRegistry::new()),
            Arc::new(TraceRegistry::new(PartRegistry::new().operation_lock())),
            None,
            policy,
        );
        (state, now_ns)
    }

    fn one_hour_floor() -> (Arc<AppState>, i64) {
        retention_state(&[(
            test_tenant().as_str(),
            TenantRetention::Finite(std::time::Duration::from_secs(60 * 60)),
        )])
    }

    /// A trace lookup carries no range, so the floor is applied span by span:
    /// a wholly expired trace is a 404 and a straddling one keeps only the
    /// spans that are still retained.
    #[tokio::test]
    async fn trace_by_id_drops_spans_below_the_retention_floor() {
        let (state, _now_ns) = one_hour_floor();

        let error = trace_by_id(
            State(state.clone()),
            crate::tenant::test_tenant_headers(),
            Path("aa".repeat(16)),
        )
        .await
        .unwrap_err();
        assert_eq!(error.0, StatusCode::NOT_FOUND);

        let response = trace_by_id(
            State(state),
            crate::tenant::test_tenant_headers(),
            Path("bb".repeat(16)),
        )
        .await
        .unwrap()
        .0;
        let spans = response["batches"][0]["instrumentationLibrarySpans"][0]["spans"]
            .as_array()
            .expect("the retained span survives")
            .clone();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0]["name"], "span-straddle-new");
    }

    /// Search clamps the requested window instead of filtering spans, and a
    /// trace is placed by its own earliest span. A trace that began below the
    /// floor therefore leaves the results whole.
    #[tokio::test]
    async fn trace_search_clamps_its_window_to_the_retention_floor() {
        let (state, _now_ns) = one_hour_floor();

        let response = search(
            State(state),
            crate::tenant::test_tenant_headers(),
            Query(SearchParams {
                tags: None,
                start: None,
                end: None,
                limit: Some(10),
                min_duration: None,
                max_duration: None,
            }),
        )
        .await
        .unwrap()
        .0;

        let ids: Vec<&str> = response["traces"]
            .as_array()
            .unwrap()
            .iter()
            .map(|trace| trace["traceID"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["cc".repeat(16)]);
    }

    /// A window that is entirely below the floor collapses to nothing. A
    /// retention downgrade must not turn a previously valid query into a 400.
    #[tokio::test]
    async fn a_fully_clamped_search_window_returns_empty_rather_than_an_error() {
        let (state, now_ns) = one_hour_floor();

        let response = search(
            State(state),
            crate::tenant::test_tenant_headers(),
            Query(SearchParams {
                tags: None,
                start: Some((now_ns - 4 * HOUR_NS).to_string()),
                end: Some((now_ns - 3 * HOUR_NS).to_string()),
                limit: Some(10),
                min_duration: None,
                max_duration: None,
            }),
        )
        .await
        .unwrap()
        .0;

        assert!(response["traces"].as_array().unwrap().is_empty());
    }

    /// The tag endpoints now take Grafana's range, so a dropdown answers for
    /// the window on screen instead of for the whole history — and, because the
    /// range also narrows the pin set, without restoring every part to do it.
    #[tokio::test]
    async fn the_tag_endpoints_answer_for_the_requested_window() {
        let (state, now_ns) = one_hour_floor();

        let in_window = search_tags(
            State(state.clone()),
            crate::tenant::test_tenant_headers(),
            Query(TagParams {
                start: Some((now_ns - 30 * 60 * 1_000_000_000).to_string()),
                end: Some(now_ns.to_string()),
                ..TagParams::default()
            }),
        )
        .await
        .unwrap()
        .0;
        let names: Vec<&str> = in_window["tags"]
            .as_array()
            .unwrap()
            .iter()
            .map(|tag| tag.as_str().unwrap())
            .collect();
        assert!(names.contains(&"fresh.attribute"), "{names:?}");

        // A window that ends before anything was written has nothing to say,
        // and says it without reading a part.
        let empty = search_tags(
            State(state.clone()),
            crate::tenant::test_tenant_headers(),
            Query(TagParams {
                start: Some((now_ns - 4 * HOUR_NS).to_string()),
                end: Some((now_ns - 3 * HOUR_NS).to_string()),
                ..TagParams::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(
            empty["tags"].as_array().unwrap().is_empty(),
            "a window below the retention floor holds no tags: {empty:?}"
        );
        // The intrinsics are a fixed list, not data, so they stay.
        assert_eq!(empty["scopes"][2]["tags"][0], "duration");

        let values = search_tag_values(
            State(state),
            crate::tenant::test_tenant_headers(),
            Path("fresh.attribute".to_string()),
            Query(TagParams {
                start: Some((now_ns - 4 * HOUR_NS).to_string()),
                end: Some((now_ns - 3 * HOUR_NS).to_string()),
                ..TagParams::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(values["values"].as_array().unwrap().is_empty());
    }

    /// The tag endpoints drop expired spans before collecting names and
    /// values, whether or not the client sent a range.
    #[tokio::test]
    async fn the_tag_endpoints_ignore_spans_below_the_retention_floor() {
        let (state, _now_ns) = one_hour_floor();

        let tags = search_tags(
            State(state.clone()),
            crate::tenant::test_tenant_headers(),
            Query(TagParams::default()),
        )
            .await
            .unwrap()
            .0;
        let names: Vec<&str> = tags["tags"]
            .as_array()
            .unwrap()
            .iter()
            .map(|tag| tag.as_str().unwrap())
            .collect();
        assert!(names.contains(&"fresh.attribute"), "{names:?}");
        assert!(!names.contains(&"old.attribute"), "{names:?}");

        let expired = search_tag_values(
            State(state.clone()),
            crate::tenant::test_tenant_headers(),
            Path("old.attribute".to_string()),
            Query(TagParams::default()),
        )
        .await
        .unwrap()
        .0;
        assert!(expired["values"].as_array().unwrap().is_empty());

        let retained = search_tag_values(
            State(state),
            crate::tenant::test_tenant_headers(),
            Path("fresh.attribute".to_string()),
            Query(TagParams::default()),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(
            retained["values"].as_array().unwrap(),
            &vec![serde_json::json!("fresh.attribute-value")]
        );
    }

    /// A tenant the control plane has never mentioned is never clamped, on any
    /// of the four trace read paths.
    #[tokio::test]
    async fn an_unknown_tenant_keeps_every_span_on_every_trace_path() {
        let (state, _now_ns) = retention_state(&[(
            "someone-else",
            TenantRetention::Finite(std::time::Duration::from_secs(1)),
        )]);

        let response = trace_by_id(
            State(state.clone()),
            crate::tenant::test_tenant_headers(),
            Path("aa".repeat(16)),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(
            response["batches"][0]["instrumentationLibrarySpans"][0]["spans"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        let found = search(
            State(state.clone()),
            crate::tenant::test_tenant_headers(),
            Query(SearchParams {
                tags: None,
                start: None,
                end: None,
                limit: Some(10),
                min_duration: None,
                max_duration: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(found["traces"].as_array().unwrap().len(), 3);

        let tags = search_tags(
            State(state.clone()),
            crate::tenant::test_tenant_headers(),
            Query(TagParams::default()),
        )
            .await
            .unwrap()
            .0;
        assert!(
            tags["tags"]
                .as_array()
                .unwrap()
                .iter()
                .any(|tag| tag == "old.attribute")
        );

        let values = search_tag_values(
            State(state),
            crate::tenant::test_tenant_headers(),
            Path("old.attribute".to_string()),
            Query(TagParams::default()),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(values["values"].as_array().unwrap().len(), 1);
    }
