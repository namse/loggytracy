    use super::*;
    use crate::config::Config;
    use crate::journal::Journal;
    use crate::memtable::MemTable;
    use crate::object_storage::ObjectStorage;
    use crate::part_registry::PartRegistry;
    use crate::tenant_policy::TenantPolicy;
    use tower::ServiceExt;

    const RETENTION_URI: &str = "/loggytracy/api/v1/admin/tenants/acme/retention";
    const TOKEN: &str = "control-plane-secret";

    fn temp_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "loggytracy-admin-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn tenant(raw: &str) -> TenantId {
        TenantId::parse(raw).expect("valid tenant id")
    }

    fn state_with(policy: Arc<TenantPolicy>, token: Option<&str>) -> Arc<AppState> {
        let config = Config {
            data_dir: temp_dir(),
            tenant_policy_token: token.map(str::to_string),
            retention_period: None,
            ..Config::default()
        };
        let memtable = Arc::new(MemTable::new());
        let journal = Arc::new(Journal::spawn(&config, memtable.clone()).unwrap());
        let parts = Arc::new(PartRegistry::new());
        let trace_parts = Arc::new(crate::trace_registry::TraceRegistry::new(
            parts.operation_lock(),
        ));
        crate::test_support::state_with_tenant_policy(
            config,
            memtable,
            journal,
            parts,
            trace_parts,
            None,
            policy,
        )
    }

    async fn call(
        state: &Arc<AppState>,
        method: &str,
        uri: &str,
        token: Option<&str>,
        body: &str,
    ) -> (StatusCode, String) {
        let mut request = axum::http::Request::builder().method(method).uri(uri);
        if let Some(token) = token {
            request = request.header(axum::http::header::AUTHORIZATION, format!("Bearer {token}"));
        }
        let request = request
            .body(axum::body::Body::from(body.to_string()))
            .unwrap();
        let response = crate::build_router(state.clone())
            .oneshot(request)
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        (status, String::from_utf8_lossy(&bytes).to_string())
    }

    #[tokio::test]
    async fn a_push_is_stored_then_readable_and_deletable() {
        let storage = Arc::new(ObjectStorage::in_memory());
        let policy = Arc::new(TenantPolicy::for_test_with_store(storage.clone()));
        let state = state_with(policy.clone(), Some(TOKEN));

        let (status, _) = call(&state, "GET", RETENTION_URI, Some(TOKEN), "").await;
        assert_eq!(status, StatusCode::NOT_FOUND, "no policy pushed yet");

        let (status, body) = call(
            &state,
            "PUT",
            RETENTION_URI,
            Some(TOKEN),
            r#"{"retention":"30d"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["tenant"], "acme");
        assert_eq!(json["retention"], "30d");
        assert!(json["updated_at"].as_str().is_some());

        // A 200 is a promise that the policy survives a restart.
        assert_eq!(storage.load_tenant_policies().await.unwrap().len(), 1);
        assert!(policy.query_floor_ns(&tenant("acme")).is_some());

        let (status, body) = call(&state, "GET", RETENTION_URI, Some(TOKEN), "").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&body).unwrap()["retention"],
            "30d"
        );

        let (status, _) = call(&state, "DELETE", RETENTION_URI, Some(TOKEN), "").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            policy.query_floor_ns(&tenant("acme")),
            None,
            "DELETE returns the tenant to unknown, which keeps its data"
        );
        assert!(storage.load_tenant_policies().await.unwrap().is_empty());
    }

    /// The ingest rate rides on the same push as retention, survives a
    /// restart with it, and is reported as sent. The last part matters most:
    /// a body without `ingest_rate` clears the rate rather than keeping it,
    /// because the body is the policy and not a patch of it.
    #[tokio::test]
    async fn an_ingest_rate_rides_the_same_push_and_a_body_without_one_clears_it() {
        let storage = Arc::new(ObjectStorage::in_memory());
        let policy = Arc::new(TenantPolicy::for_test_with_store(storage.clone()));
        let state = state_with(policy.clone(), Some(TOKEN));

        let (status, body) = call(
            &state,
            "PUT",
            RETENTION_URI,
            Some(TOKEN),
            r#"{"retention":"30d","ingest_rate":"4MiB/s"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["ingest_rate"], "4MiB/s");
        assert_eq!(
            policy.ingest_rate(&tenant("acme")),
            Some(crate::tenant_policy::TenantIngestRate::BytesPerSecond(
                4 * 1024 * 1024
            ))
        );

        // Durable, and readable back through a restart.
        let reload_config = Config {
            tenant_policy_token: Some(TOKEN.to_string()),
            retention_period: None,
            ..Config::default()
        };
        let reloaded = TenantPolicy::load(&reload_config, Some(storage.clone()))
            .await
            .unwrap();
        assert_eq!(
            reloaded.ingest_rate(&tenant("acme")),
            Some(crate::tenant_policy::TenantIngestRate::BytesPerSecond(
                4 * 1024 * 1024
            )),
            "a rate that does not survive a restart is not a policy"
        );

        let (status, body) = call(
            &state,
            "PUT",
            RETENTION_URI,
            Some(TOKEN),
            r#"{"retention":"30d"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            serde_json::from_str::<serde_json::Value>(&body)
                .unwrap()
                .get("ingest_rate")
                .is_none()
        );
        assert_eq!(
            policy.ingest_rate(&tenant("acme")),
            None,
            "omitting the field clears it; the body is the whole policy"
        );

        // A value that cannot be parsed stores nothing at all, retention
        // included: a partially applied push is worse than a rejected one.
        let (status, _) = call(
            &state,
            "PUT",
            RETENTION_URI,
            Some(TOKEN),
            r#"{"retention":"7d","ingest_rate":"quickly"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            policy.view(&tenant("acme")).unwrap().retention,
            "30d",
            "the rejected push must not have changed retention either"
        );
    }

    #[tokio::test]
    async fn a_failing_store_returns_503_and_applies_nothing() {
        let policy = Arc::new(TenantPolicy::for_test_with_store(Arc::new(
            ObjectStorage::in_memory_with_failing_writes(),
        )));
        let state = state_with(policy.clone(), Some(TOKEN));

        let (status, _) = call(
            &state,
            "PUT",
            RETENTION_URI,
            Some(TOKEN),
            r#"{"retention":"30d"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            policy.query_floor_ns(&tenant("acme")),
            None,
            "a policy that is not durable is not applied either"
        );
        assert_eq!(
            policy
                .metrics
                .push_persist_errors
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(
            policy
                .metrics
                .push_accepted
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn an_unauthenticated_or_wrong_token_push_changes_nothing() {
        let storage = Arc::new(ObjectStorage::in_memory());
        let policy = Arc::new(TenantPolicy::for_test_with_store(storage.clone()));
        let state = state_with(policy.clone(), Some(TOKEN));

        for token in [None, Some("wrong"), Some("control-plane-secre")] {
            let (status, _) =
                call(&state, "PUT", RETENTION_URI, token, r#"{"retention":"0"}"#).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{token:?}");
        }
        assert!(storage.load_tenant_policies().await.unwrap().is_empty());
        assert_eq!(policy.query_floor_ns(&tenant("acme")), None);
    }

    #[tokio::test]
    async fn a_malformed_tenant_or_value_is_a_400_and_stores_nothing() {
        let storage = Arc::new(ObjectStorage::in_memory());
        let policy = Arc::new(TenantPolicy::for_test_with_store(storage.clone()));
        let state = state_with(policy, Some(TOKEN));

        let (status, _) = call(
            &state,
            "PUT",
            "/loggytracy/api/v1/admin/tenants/..%2Fetc/retention",
            Some(TOKEN),
            r#"{"retention":"30d"}"#,
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "a tenant id is never propagated into a path"
        );

        for body in [r#"{"retention":"soon"}"#, r#"{"retention":7}"#, "{}", "not json"] {
            let (status, _) = call(&state, "PUT", RETENTION_URI, Some(TOKEN), body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        }
        assert!(storage.load_tenant_policies().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn the_admin_routes_do_not_exist_without_a_token() {
        let state = state_with(Arc::new(TenantPolicy::disabled()), None);
        for method in ["PUT", "GET", "DELETE"] {
            let (status, _) = call(
                &state,
                method,
                RETENTION_URI,
                Some(TOKEN),
                r#"{"retention":"30d"}"#,
            )
            .await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{method}");
        }
    }

    /// A pushed value is applied exactly as sent. The instance has no setting
    /// that caps or rewrites it, so what a `GET` reports and what the query
    /// floor enforces can never disagree.
    #[tokio::test]
    async fn a_pushed_value_is_reported_and_enforced_as_sent() {
        let policy = Arc::new(TenantPolicy::for_test_with_store(Arc::new(
            ObjectStorage::in_memory(),
        )));
        let state = state_with(policy.clone(), Some(TOKEN));

        let (status, body) = call(
            &state,
            "PUT",
            RETENTION_URI,
            Some(TOKEN),
            r#"{"retention":"30d"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&body).unwrap()["retention"],
            "30d"
        );
        let floor_ns = policy.query_floor_ns(&tenant("acme")).unwrap();
        let thirty_days_ns = 30 * 24 * 60 * 60 * 1_000_000_000i64;
        let elapsed = crate::tenant_policy::now_ns() - floor_ns;
        assert!(
            (elapsed - thirty_days_ns).abs() < 1_000_000_000,
            "the pushed thirty days is what the query floor enforces"
        );
    }

    /// Asking for preservation must never cost data that staying silent would
    /// have kept: an explicit `infinite` and a tenant the control plane has
    /// never mentioned both leave queries unclamped.
    #[tokio::test]
    async fn an_explicit_infinite_keeps_as_much_as_never_being_pushed() {
        let policy = Arc::new(TenantPolicy::for_test_with_store(Arc::new(
            ObjectStorage::in_memory(),
        )));
        let state = state_with(policy.clone(), Some(TOKEN));

        let (status, _) = call(
            &state,
            "PUT",
            RETENTION_URI,
            Some(TOKEN),
            r#"{"retention":"infinite"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(policy.query_floor_ns(&tenant("acme")), None);
        assert_eq!(policy.query_floor_ns(&tenant("never-pushed")), None);
    }

    /// `/metrics` is one positional `format!` over roughly forty-five counters,
    /// so a name and a value can drift apart without the compiler noticing. Give
    /// three of them distinct values and check each lands on its own line.
    #[tokio::test]
    async fn every_metric_name_carries_its_own_value() {
        let policy = Arc::new(TenantPolicy::for_test_with_store(Arc::new(
            ObjectStorage::in_memory(),
        )));
        let state = state_with(policy.clone(), Some(TOKEN));
        use std::sync::atomic::Ordering;
        state.metrics.merge_errors.store(11, Ordering::Relaxed);
        state
            .metrics
            .merge_inputs_changed
            .store(22, Ordering::Relaxed);
        state.metrics.retention_success.store(33, Ordering::Relaxed);
        policy.record_unauthorized();

        let body = crate::query::metrics(axum::extract::State(state.clone())).await;
        for (name, value) in [
            ("loggytracy_merge_errors_total", "11"),
            ("loggytracy_merge_inputs_changed_total", "22"),
            ("loggytracy_retention_success_total", "33"),
            ("loggytracy_tenant_policy_admin_unauthorized_total", "1"),
        ] {
            assert!(
                body.lines().any(|line| line == format!("{name} {value}")),
                "{name} should report {value}:\n{body}"
            );
        }
    }


    /// Per-tenant numbers live here, not as labels on `/metrics`.
    ///
    /// That endpoint is unauthenticated and process-wide by design, and a label
    /// per tenant would multiply every series by the tenant count — on a
    /// workload whose whole point is many small tenants, that is the
    /// cardinality problem this engine bounds everywhere else. The reader that
    /// needs per-tenant numbers is the control plane, which is already
    /// authenticated here.
    #[tokio::test]
    async fn per_tenant_usage_is_authenticated_and_scoped_to_one_tenant() {
        let storage = Arc::new(ObjectStorage::in_memory());
        let policy = Arc::new(TenantPolicy::for_test_with_store(storage));
        let state = state_with(policy, Some(TOKEN));
        let uri = "/loggytracy/api/v1/admin/tenants/acme/usage";

        let (status, _) = call(&state, "GET", uri, None, "").await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "usage names a tenant, so it is not public"
        );

        state.memtable.insert(
            tenant("acme"),
            [("app".to_string(), "usage".to_string())]
                .into_iter()
                .collect(),
            vec![crate::memtable::LogEntry {
                timestamp_ns: 1_700_000_000_000_000_000,
                line: "counted".to_string(),
                structured_metadata: Vec::new(),
            }],
        );

        let (status, body) = call(&state, "GET", uri, Some(TOKEN), "").await;
        assert_eq!(status, StatusCode::OK);
        let usage: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(usage["tenant"], "acme");
        assert_eq!(usage["streams"], 1);
        assert_eq!(usage["entries"], 1);

        // A different tenant sees its own zeroes, not acme's numbers.
        let (status, body) = call(
            &state,
            "GET",
            "/loggytracy/api/v1/admin/tenants/other/usage",
            Some(TOKEN),
            "",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let other: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(other["streams"], 0);
        assert_eq!(other["entries"], 0);
    }

    /// `/metrics` stays free of tenant labels, which is the decision the usage
    /// endpoint exists to preserve rather than an oversight.
    #[tokio::test]
    async fn the_operator_scrape_carries_no_tenant_labels() {
        let storage = Arc::new(ObjectStorage::in_memory());
        let policy = Arc::new(TenantPolicy::for_test_with_store(storage));
        let state = state_with(policy, Some(TOKEN));
        state.memtable.insert(
            tenant("acme"),
            [("app".to_string(), "scrape".to_string())]
                .into_iter()
                .collect(),
            vec![crate::memtable::LogEntry {
                timestamp_ns: 1,
                line: "x".to_string(),
                structured_metadata: Vec::new(),
            }],
        );

        let rendered = crate::query::metrics(axum::extract::State(state)).await;
        assert!(
            !rendered.contains("acme"),
            "a tenant id in the operator scrape is a series per tenant"
        );
        assert!(rendered.contains("loggytracy_stream_limit_rejected_total"));
    }
