    use super::*;
    use crate::tenant::test_tenant;
    use crate::config::Config;
    use crate::journal::Journal;
    use crate::memtable::MemTable;
    use crate::part_registry::PartRegistry;
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
