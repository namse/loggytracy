    use super::*;
    use crate::config::Config;
    use crate::journal::Journal;
    use crate::memtable::MemTable;
    use crate::part_registry::PartRegistry;
    use crate::tenant::test_tenant;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use tower::ServiceExt;

    fn fixture() -> (Arc<MemTable>, Arc<AppState>) {
        fixture_with(|_| {})
    }

    fn fixture_with(edit: impl FnOnce(&mut Config)) -> (Arc<MemTable>, Arc<AppState>) {
        let mut config = Config {
            data_dir: std::env::temp_dir()
                .join(format!("signy-otlp-http-{}", uuid::Uuid::new_v4())),
            ..Config::default()
        };
        edit(&mut config);
        let config = config;
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let memtable = Arc::new(MemTable::new());
        let journal = Arc::new(Journal::spawn(&config, memtable.clone()).unwrap());
        let parts = Arc::new(PartRegistry::new());
        let trace_parts = Arc::new(crate::trace_registry::TraceRegistry::new(
            parts.operation_lock(),
        ));
        let state = crate::test_support::state(config, memtable.clone(), journal, parts, trace_parts, None);
        (memtable, state)
    }

    fn log_request(body: &str) -> ExportLogsServiceRequest {
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
                    log_records: vec![LogRecord {
                        time_unix_nano: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_nanos() as u64,
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(body.to_string())),
                        }),
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    async fn post(
        state: &Arc<AppState>,
        uri: &str,
        content_type: &str,
        body: Vec<u8>,
    ) -> (StatusCode, Vec<u8>, String) {
        let request = axum::http::Request::builder()
            .method("POST")
            .uri(uri)
            .header(header::CONTENT_TYPE, content_type)
            .header(crate::tenant::TENANT_HEADER, test_tenant().as_str())
            .body(axum::body::Body::from(body))
            .unwrap();
        let response = crate::build_router(state.clone())
            .oneshot(request)
            .await
            .unwrap();
        let status = response.status();
        let response_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .map(|value| value.to_str().unwrap().to_string())
            .unwrap_or_default();
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        (status, bytes.to_vec(), response_type)
    }

    fn lines(memtable: &MemTable) -> Vec<String> {
        memtable
            .query(&test_tenant(), &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 10, true)
            .iter()
            .flat_map(|stream| stream.entries.iter())
            .map(|entry| entry.line.clone())
            .collect()
    }

    /// The response must be decodable by the client that sent the request. A
    /// collector that posted JSON cannot read a protobuf `ExportLogsServiceResponse`
    /// back, and the specification requires a body of that type either way.
    #[tokio::test]
    async fn an_export_is_answered_in_the_encoding_it_arrived_in() {
        let (memtable, state) = fixture();

        let (status, body, response_type) = post(
            &state,
            "/v1/logs",
            "application/x-protobuf",
            log_request("over protobuf").encode_to_vec(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(response_type, "application/x-protobuf");
        ExportLogsServiceResponse::decode(body.as_slice()).expect("a protobuf response");

        let (status, body, response_type) = post(
            &state,
            "/v1/logs",
            "application/json",
            serde_json::to_vec(&log_request("over json")).unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(response_type, "application/json");
        serde_json::from_slice::<serde_json::Value>(&body).expect("a JSON response");

        let mut delivered = lines(&memtable);
        delivered.sort();
        assert_eq!(delivered, vec!["over json", "over protobuf"]);
    }

    /// The in-flight bound must not be able to wedge a server that is idle.
    ///
    /// One byte is the tightest ceiling the knob accepts, and every legal push
    /// is larger than it. If the middleware refused on arithmetic alone this
    /// server would answer 429 forever with nothing in flight, which is why
    /// `IngestGate::admit_body` always admits into an empty server — this is
    /// that rule reached through the router, layer order and all.
    #[tokio::test]
    async fn the_tightest_inflight_ceiling_still_serves_a_lone_push() {
        let (memtable, state) = fixture_with(|config| {
            config.max_inflight_push_bytes = Some(1);
        });
        let (status, _, _) = post(
            &state,
            "/v1/logs",
            "application/x-protobuf",
            log_request("admitted into an empty server").encode_to_vec(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(lines(&memtable), vec!["admitted into an empty server"]);
        // And the charge was released rather than held by the answered request.
        assert_eq!(state.ingest_gate.inflight_body_bytes(), 0);
    }

    /// The charge is the header, and it is released on the way out.
    ///
    /// A body that never arrives at a handler still has to be accounted for
    /// while it is in the heap, so the accounting lives in the middleware; what
    /// this pins is that it does not *leak* — a refused encoding returns early,
    /// through a different path than the success case, and must still give the
    /// bytes back.
    #[tokio::test]
    async fn a_refused_push_releases_its_inflight_charge() {
        let (_, state) = fixture();
        let (status, _, _) = post(
            &state,
            "/v1/logs",
            "application/octet-stream",
            log_request("never decoded").encode_to_vec(),
        )
        .await;
        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert_eq!(state.ingest_gate.inflight_body_bytes(), 0);
    }

    /// A charset parameter is legal on `Content-Type` and collectors send one.
    /// Comparing the raw header would reject those with a 415.
    #[tokio::test]
    async fn a_content_type_with_parameters_is_still_recognized() {
        let (memtable, state) = fixture();
        let (status, _, _) = post(
            &state,
            "/v1/logs",
            "application/json; charset=utf-8",
            serde_json::to_vec(&log_request("with a charset")).unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(lines(&memtable), vec!["with a charset"]);
    }

    #[tokio::test]
    async fn an_unsupported_encoding_is_refused_without_writing() {
        let (memtable, state) = fixture();
        let (status, _, _) = post(
            &state,
            "/v1/logs",
            "application/xml",
            log_request("never stored").encode_to_vec(),
        )
        .await;
        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);

        // A body that does not decode is the client's error, not a write.
        let (status, _, _) = post(
            &state,
            "/v1/logs",
            "application/x-protobuf",
            b"not a protobuf message at all".to_vec(),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(lines(&memtable).is_empty());
    }

    /// 403 and not 400: the request is well formed and there is nothing the
    /// client can change about it. With per-tenant policy enabled, the pushed
    /// policies are the tenant registry — a tenant nothing was pushed for is
    /// not served, on this transport like any other.
    #[tokio::test]
    async fn an_export_from_a_tenant_without_a_pushed_policy_is_refused() {
        let config = Config {
            data_dir: std::env::temp_dir()
                .join(format!("signy-otlp-http-allow-{}", uuid::Uuid::new_v4())),
            ..Config::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let memtable = Arc::new(MemTable::new());
        let journal = Arc::new(Journal::spawn(&config, memtable.clone()).unwrap());
        let parts = Arc::new(PartRegistry::new());
        let trace_parts = Arc::new(crate::trace_registry::TraceRegistry::new(
            parts.operation_lock(),
        ));
        let tenant_policy = Arc::new(crate::tenant_policy::TenantPolicy::enabled_with_clock(
            crate::clock::Clock::system(),
        ));
        tenant_policy
            .push(&test_tenant(), "30d", None)
            .await
            .expect("the test tenant is onboarded by pushing a policy");
        let state = crate::test_support::state_with_tenant_policy(
            config,
            memtable.clone(),
            journal,
            parts,
            trace_parts,
            None,
            tenant_policy,
        );

        let body = log_request("accepted").encode_to_vec();
        let (status, _, _) = post(&state, "/v1/logs", "application/x-protobuf", body).await;
        assert_eq!(status, StatusCode::OK, "a listed tenant is accepted");

        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/v1/logs")
            .header(header::CONTENT_TYPE, "application/x-protobuf")
            .header(crate::tenant::TENANT_HEADER, "stranger")
            .body(axum::body::Body::from(
                log_request("refused").encode_to_vec(),
            ))
            .unwrap();
        let response = crate::build_router(state.clone())
            .oneshot(request)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert!(
            memtable
                .query(&crate::tenant::TenantId::parse("stranger").unwrap(), &[],
                    crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX),
                    10,
                    true
                )
                .is_empty(),
            "a refused tenant must not have been written"
        );
    }

    #[tokio::test]
    async fn a_trace_export_over_http_reaches_the_trace_memtable() {
        let (_memtable, state) = fixture();
        let request = ExportTraceServiceRequest {
            resource_spans: vec![opentelemetry_proto::tonic::trace::v1::ResourceSpans {
                resource: None,
                scope_spans: vec![opentelemetry_proto::tonic::trace::v1::ScopeSpans {
                    scope: None,
                    spans: vec![opentelemetry_proto::tonic::trace::v1::Span {
                        trace_id: vec![7; 16],
                        span_id: vec![9; 8],
                        start_time_unix_nano: 10,
                        end_time_unix_nano: 20,
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let (status, _, _) = post(
            &state,
            "/v1/traces",
            "application/x-protobuf",
            request.encode_to_vec(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            state
                .journal
                .trace_memtable()
                .snapshot_limited(&test_tenant(), 10)
                .unwrap()
                .len(),
            1
        );
    }

    fn span_request(mark: u8) -> ExportTraceServiceRequest {
        ExportTraceServiceRequest {
            resource_spans: vec![opentelemetry_proto::tonic::trace::v1::ResourceSpans {
                resource: None,
                scope_spans: vec![opentelemetry_proto::tonic::trace::v1::ScopeSpans {
                    scope: None,
                    spans: vec![opentelemetry_proto::tonic::trace::v1::Span {
                        trace_id: vec![mark; 16],
                        span_id: vec![mark; 8],
                        start_time_unix_nano: 10,
                        end_time_unix_nano: 20,
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    fn spans(state: &Arc<AppState>) -> usize {
        state
            .journal
            .trace_memtable()
            .snapshot_limited(&test_tenant(), 10)
            .unwrap()
            .len()
    }

    const LOGS_TAG: u8 = 1;
    const TRACES_TAG: u8 = 2;

    /// The batch collecty ships: each payload behind its own record header,
    /// each record its own zstd frame, all of it concatenated.
    fn zstd_frames(records: &[(u8, Vec<u8>)]) -> Vec<u8> {
        let mut frames = Vec::new();
        for (tag, payload) in records {
            let mut plain = Vec::with_capacity(5 + payload.len());
            plain.push(*tag);
            plain.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            plain.extend_from_slice(payload);
            frames.extend_from_slice(&zstd::bulk::compress(&plain, 3).unwrap());
        }
        frames
    }

    async fn post_collected(
        state: &Arc<AppState>,
        encoding: &str,
        frames: Vec<u8>,
    ) -> (StatusCode, String) {
        post_collected_as(state, test_tenant().as_str(), encoding, frames).await
    }

    async fn post_collected_as(
        state: &Arc<AppState>,
        tenant: &str,
        encoding: &str,
        frames: Vec<u8>,
    ) -> (StatusCode, String) {
        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/signy/api/v1/collect")
            .header(header::CONTENT_TYPE, "application/x-protobuf")
            .header(header::CONTENT_ENCODING, encoding)
            .header(crate::tenant::TENANT_HEADER, tenant)
            .body(axum::body::Body::from(frames))
            .unwrap();
        let response = crate::build_router(state.clone())
            .oneshot(request)
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        (status, String::from_utf8_lossy(&bytes).to_string())
    }

    #[tokio::test]
    async fn a_collected_batch_of_separate_exports_lands_as_every_line_it_carried() {
        let (memtable, state) = fixture();
        let records: Vec<(u8, Vec<u8>)> = ["first", "second", "third"]
            .iter()
            .map(|line| (LOGS_TAG, log_request(line).encode_to_vec()))
            .collect();
        let frames = zstd_frames(&records);

        let (status, body) = post_collected(&state, "zstd", frames).await;

        assert_eq!(status, StatusCode::OK, "{body}");
        let mut lines = lines(&memtable);
        lines.sort();
        assert_eq!(lines, vec!["first", "second", "third"]);
    }

    #[tokio::test]
    async fn a_collected_batch_of_spans_reaches_the_trace_memtable() {
        let (_memtable, state) = fixture();
        let records = vec![
            (TRACES_TAG, span_request(1).encode_to_vec()),
            (TRACES_TAG, span_request(2).encode_to_vec()),
        ];
        let frames = zstd_frames(&records);

        let (status, body) = post_collected(&state, "zstd", frames).await;

        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            state
                .journal
                .trace_memtable()
                .snapshot_limited(&test_tenant(), 10)
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn a_collected_batch_that_is_not_zstd_is_refused_without_writing() {
        let (memtable, state) = fixture();
        let body = log_request("never stored").encode_to_vec();

        let (status, _) = post_collected(&state, "gzip", body).await;

        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert!(lines(&memtable).is_empty());
    }

    /// The batch itself has no ceiling any more, so the one left is a record's
    /// own payload, and it is refused on what the header claims rather than
    /// after the bytes have been waited for.
    #[tokio::test]
    async fn a_record_claiming_more_than_one_export_is_refused_before_it_arrives() {
        let (memtable, state) = fixture();
        let mut plain = vec![LOGS_TAG];
        plain.extend_from_slice(
            &((crate::otlp_http::MAX_COLLECT_RECORD_BYTES + 1) as u32).to_le_bytes(),
        );
        plain.extend_from_slice(b"and nothing like that much behind it");
        let frames = zstd::bulk::compress(&plain, 3).unwrap();

        let (status, body) = post_collected(&state, "zstd", frames).await;

        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
        assert!(lines(&memtable).is_empty());
    }

    /// A batch larger than anything the old ceiling would have taken, landing
    /// whole. Nothing here holds the batch, so its size is collecty's business.
    #[tokio::test]
    async fn a_batch_past_the_old_ceiling_lands_whole() {
        let (memtable, state) = fixture();
        let filler = "x".repeat(64 * 1024);
        let records: Vec<(u8, Vec<u8>)> = (0..400)
            .map(|index| {
                (
                    LOGS_TAG,
                    log_request(&format!("{index} {filler}")).encode_to_vec(),
                )
            })
            .collect();
        let plain_bytes: usize = records
            .iter()
            .map(|(_, payload)| 5 + payload.len())
            .sum();
        assert!(plain_bytes > crate::trace_ingest::MAX_OTLP_REQUEST_BYTES);

        let (status, body) = post_collected(&state, "zstd", zstd_frames(&records)).await;

        assert_eq!(status, StatusCode::OK, "{body}");
        let stored: usize = memtable
            .query(
                &test_tenant(),
                &[],
                crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX),
                1000,
                true,
            )
            .iter()
            .map(|stream| stream.entries.len())
            .sum();
        assert_eq!(stored, 400);
    }

    #[tokio::test]
    async fn one_batch_of_mixed_signals_lands_in_every_store_it_names() {
        let (memtable, state) = fixture();
        let records = vec![
            (LOGS_TAG, log_request("first").encode_to_vec()),
            (TRACES_TAG, span_request(7).encode_to_vec()),
            (LOGS_TAG, log_request("second").encode_to_vec()),
        ];
        let frames = zstd_frames(&records);

        let (status, body) = post_collected(&state, "zstd", frames).await;

        assert_eq!(status, StatusCode::OK, "{body}");
        let mut lines = lines(&memtable);
        lines.sort();
        assert_eq!(lines, vec!["first", "second"]);
        assert_eq!(spans(&state), 1);
    }

    #[tokio::test]
    async fn a_signal_that_will_never_decode_is_dropped_and_the_rest_of_the_batch_lands() {
        let (memtable, state) = fixture();
        let poison = vec![0xFFu8; 16];
        let records = vec![
            (LOGS_TAG, poison.clone()),
            (TRACES_TAG, span_request(9).encode_to_vec()),
        ];
        let frames = zstd_frames(&records);

        let (status, body) = post_collected(&state, "zstd", frames).await;

        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(lines(&memtable).is_empty());
        assert_eq!(spans(&state), 1);
        assert_eq!(
            state
                .metrics
                .collect_dropped_records
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(
            state
                .metrics
                .collect_dropped_bytes
                .load(std::sync::atomic::Ordering::Relaxed),
            poison.len() as u64
        );
    }

    #[tokio::test]
    async fn a_batch_whose_framing_does_not_add_up_is_refused() {
        let (memtable, state) = fixture();
        let mut plain = vec![LOGS_TAG];
        plain.extend_from_slice(&64u32.to_le_bytes());
        plain.extend_from_slice(b"eight...");
        let frames = zstd::bulk::compress(&plain, 3).unwrap();

        let (status, body) = post_collected(&state, "zstd", frames).await;

        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(body.contains("claims 64 bytes"), "{body}");
        assert!(lines(&memtable).is_empty());
    }

    #[tokio::test]
    async fn a_record_under_a_tag_signy_does_not_know_is_refused() {
        let (_memtable, state) = fixture();
        let mut plain = vec![9u8];
        plain.extend_from_slice(&0u32.to_le_bytes());
        let frames = zstd::bulk::compress(&plain, 3).unwrap();

        let (status, body) = post_collected(&state, "zstd", frames).await;

        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(body.contains("not a signal tag"), "{body}");
    }

    /// A tenant nothing was pushed for is a policy mistake, and the batch
    /// carrying it is still dropped: the collector has one queue for the whole
    /// machine, so holding it would stop every other application on the host
    /// behind one misconfigured process.
    #[tokio::test]
    async fn a_tenant_this_instance_does_not_serve_is_dropped_rather_than_held() {
        let config = Config {
            data_dir: std::env::temp_dir()
                .join(format!("signy-otlp-http-collect-{}", uuid::Uuid::new_v4())),
            ..Config::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let memtable = Arc::new(MemTable::new());
        let journal = Arc::new(Journal::spawn(&config, memtable.clone()).unwrap());
        let parts = Arc::new(PartRegistry::new());
        let trace_parts = Arc::new(crate::trace_registry::TraceRegistry::new(
            parts.operation_lock(),
        ));
        let tenant_policy = Arc::new(crate::tenant_policy::TenantPolicy::enabled_with_clock(
            crate::clock::Clock::system(),
        ));
        tenant_policy
            .push(&test_tenant(), "30d", None)
            .await
            .expect("the test tenant is onboarded by pushing a policy");
        let state = crate::test_support::state_with_tenant_policy(
            config,
            memtable.clone(),
            journal,
            parts,
            trace_parts,
            None,
            tenant_policy,
        );

        let records = vec![(LOGS_TAG, log_request("held, not lost").encode_to_vec())];
        let frames = zstd_frames(&records);

        let (status, body) = post_collected_as(&state, "stranger", "zstd", frames).await;

        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(lines(&memtable).is_empty());
        assert_eq!(
            state
                .metrics
                .collect_dropped_records
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }
