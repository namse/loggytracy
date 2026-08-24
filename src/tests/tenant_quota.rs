    use super::*;
    use crate::clock::Clock;
    use crate::tenant::TenantId;

    fn tenant(name: &str) -> TenantId {
        TenantId::parse(name).unwrap()
    }

    fn quota(config: Config, policy: Arc<TenantPolicy>) -> TenantQuota {
        quota_over(
            config,
            policy,
            Arc::new(crate::part_registry::PartRegistry::new()),
        )
    }

    fn quota_over(
        config: Config,
        policy: Arc<TenantPolicy>,
        parts: Arc<crate::part_registry::PartRegistry>,
    ) -> TenantQuota {
        TenantQuota::new(
            Arc::new(config),
            Arc::new(RuntimeMetrics::new()),
            policy,
            parts,
            Arc::new(crate::trace_registry::TraceRegistry::standalone()),
        )
    }

    /// A registry holding one part with rows for each named tenant, so a
    /// storage limit has something real to measure.
    fn registry_holding(tenants: &[&str]) -> Arc<crate::part_registry::PartRegistry> {
        use crate::memtable::Labels;
        use crate::part::Row;
        use std::collections::BTreeMap;

        let root = std::env::temp_dir().join(format!(
            "loggytracy-storage-quota-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let mut labels: Labels = BTreeMap::new();
        labels.insert("app".to_string(), "test".to_string());
        let rows: Vec<Row> = tenants
            .iter()
            .flat_map(|name| {
                let labels = std::sync::Arc::new(labels.clone());
                (0..64).map(move |index| Row {
                    tenant: TenantId::parse(name).unwrap(),
                    timestamp_ns: index,
                    labels: labels.clone(),
                    line: format!("line {index}"),
                    structured_metadata: vec![],
                })
            })
            .collect();
        let registry = Arc::new(crate::part_registry::PartRegistry::new());
        registry
            .register(crate::part::flush_rows(rows, &root, 16).unwrap())
            .unwrap();
        for name in tenants {
            assert!(registry.tenant_stored_bytes(&TenantId::parse(name).unwrap()) > 0);
        }
        registry
    }

    /// The limit is a stock, not a flow: being under it admits regardless of
    /// how fast the tenant is writing, and being at it refuses regardless of
    /// how slowly.
    #[test]
    fn a_tenant_at_its_storage_limit_is_refused_and_told_to_wait() {
        let parts = registry_holding(&["acme"]);
        let stored = parts.tenant_stored_bytes(&tenant("acme"));
        let under = Config {
            default_tenant_max_stored_bytes: Some(stored + 1),
            ..Config::default()
        };
        quota_over(under, Arc::new(TenantPolicy::disabled()), parts.clone())
            .admit_storage(&tenant("acme"))
            .expect("a tenant below its limit still writes");

        let at = Config {
            default_tenant_max_stored_bytes: Some(stored),
            ..Config::default()
        };
        let error = quota_over(at, Arc::new(TenantPolicy::disabled()), parts)
            .admit_storage(&tenant("acme"))
            .expect_err("a tenant at its limit is refused");
        assert_eq!(error.status, axum::http::StatusCode::TOO_MANY_REQUESTS);
        assert!(
            error.retry_after.is_some_and(|wait| wait.as_secs() >= 60),
            "a storage refusal clears when retention runs, not in a second"
        );
        assert!(error.message.contains("at its limit"), "{}", error.message);
    }

    /// One tenant's storage says nothing about its neighbour's, even though
    /// they share the object.
    #[test]
    fn a_storage_limit_does_not_reach_the_tenant_beside_it() {
        let parts = registry_holding(&["acme"]);
        let stored = parts.tenant_stored_bytes(&tenant("acme"));
        let config = Config {
            default_tenant_max_stored_bytes: Some(stored),
            ..Config::default()
        };
        let quota = quota_over(config, Arc::new(TenantPolicy::disabled()), parts);
        quota
            .admit_storage(&tenant("acme"))
            .expect_err("the tenant holding the part is at its limit");
        quota
            .admit_storage(&tenant("globex"))
            .expect("a tenant holding nothing is not");
    }

    /// The pushed limit wins over the configured default — a free-tier default
    /// is what a tenant gets until a plan is sold to it.
    #[tokio::test]
    async fn a_pushed_storage_limit_overrides_the_free_tier_default() {
        let parts = registry_holding(&["acme", "globex"]);
        let stored = parts.tenant_stored_bytes(&tenant("acme"));
        let clock = Clock::fixed(0);
        let policy = Arc::new(TenantPolicy::enabled_with_clock(clock));
        policy
            .push(&tenant("acme"), "7d", None, Some(&format!("{}", stored * 4)))
            .await
            .unwrap();
        let config = Config {
            default_tenant_max_stored_bytes: Some(1),
            ..Config::default()
        };
        let quota = quota_over(config, policy, parts);
        quota
            .admit_storage(&tenant("acme"))
            .expect("the pushed limit is four times what the tenant holds");
        quota
            .admit_storage(&tenant("globex"))
            .expect_err("a tenant with nothing pushed keeps the free-tier default");
    }

    /// A tenant issuing many concurrent scans would otherwise hold every permit
    /// of the shared query semaphore and queue everyone else behind it.
    #[test]
    fn one_tenant_cannot_hold_every_query_slot() {
        let config = Config {
            max_concurrent_queries_per_tenant: 2,
            ..Config::default()
        };
        let quota = Arc::new(quota(config, Arc::new(TenantPolicy::disabled())));
        let loud = tenant("loud");

        let first = quota.begin_query(&loud).unwrap();
        let second = quota.begin_query(&loud).unwrap();
        let refused = quota
            .begin_query(&loud)
            .expect_err("a third concurrent query is over the limit");
        assert_eq!(refused.status, StatusCode::TOO_MANY_REQUESTS);

        // The limit is per tenant, not global.
        let quiet = quota
            .begin_query(&tenant("quiet"))
            .expect("another tenant has its own slots");

        // Slots are released by dropping, including on a cancelled query.
        drop(first);
        quota
            .begin_query(&loud)
            .expect("finishing a query frees its slot");
        drop(second);
        drop(quiet);
    }

    /// A tenant at its stream limit keeps writing to the streams it has. The
    /// limit exists to stop a client that mints label values, and cutting off
    /// its existing streams would punish the wrong thing — the tenant's real
    /// logs would stop while the runaway labels were the problem.
    #[test]
    fn the_stream_limit_refuses_new_streams_and_keeps_existing_ones() {
        use crate::memtable::{Labels, LogEntry, MemTable};
        use crate::part_registry::PartRegistry;

        fn labels(app: &str) -> Labels {
            [("app".to_string(), app.to_string())].into_iter().collect()
        }
        fn entry() -> Vec<LogEntry> {
            vec![LogEntry {
                timestamp_ns: 1,
                line: "x".to_string(),
                structured_metadata: vec![],
            }]
        }

        let config = Config {
            default_tenant_max_streams: Some(2),
            ..Config::default()
        };
        let quota = quota(config, Arc::new(TenantPolicy::disabled()));
        let parts = PartRegistry::new();
        let memtable = MemTable::new();
        let acme = tenant("acme");

        quota
            .admit_stream(&acme, &labels("one"), &parts, &memtable)
            .unwrap();
        memtable.insert(acme.clone(), labels("one"), entry());
        quota
            .admit_stream(&acme, &labels("two"), &parts, &memtable)
            .unwrap();
        memtable.insert(acme.clone(), labels("two"), entry());

        let refused = quota
            .admit_stream(&acme, &labels("three"), &parts, &memtable)
            .expect_err("a third stream is over the limit");
        assert_eq!(refused.status, StatusCode::BAD_REQUEST);

        // The streams it already has keep working.
        quota
            .admit_stream(&acme, &labels("one"), &parts, &memtable)
            .expect("an existing stream is never refused");

        // And the limit is per tenant.
        quota
            .admit_stream(&tenant("other"), &labels("three"), &parts, &memtable)
            .expect("another tenant has its own budget");
    }

    /// Parts and buffers are unioned, not summed. A stream that has been
    /// flushed lives in both for as long as the buffer holds it, and summing
    /// would count it twice — charging a tenant for streams it does not have.
    #[tokio::test]
    async fn a_flushed_stream_is_not_counted_twice() {
        use crate::memtable::{Labels, LogEntry, MemTable};
        use crate::part_registry::PartRegistry;

        let dir = std::env::temp_dir().join(format!("loggytracy-streams-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let config = Config {
            default_tenant_max_streams: Some(2),
            ..Config::default()
        };
        let quota = quota(config.clone(), Arc::new(TenantPolicy::disabled()));
        let parts = PartRegistry::new();
        let memtable = MemTable::new();
        let acme = tenant("acme");

        let stream: Labels = [("app".to_string(), "flushed".to_string())]
            .into_iter()
            .collect();
        // The same stream in both places, which is what a flush in progress
        // looks like.
        parts
            .register(
                crate::part::flush_rows(
                    vec![crate::part::Row {
                        tenant: acme.clone(),
                        timestamp_ns: 1,
                        labels: std::sync::Arc::new(stream.clone()),
                        line: "on disk".to_string(),
                        structured_metadata: vec![],
                    }],
                    &dir,
                    config.row_group_size,
                )
                .unwrap(),
            )
            .unwrap();
        memtable.insert(
            acme.clone(),
            stream.clone(),
            vec![LogEntry {
                timestamp_ns: 2,
                line: "buffered".to_string(),
                structured_metadata: vec![],
            }],
        );

        assert_eq!(parts.tenant_stream_count(&acme), 1);
        // One stream held, so a second is still allowed. Summing the two
        // sources would have counted two and refused it.
        quota
            .admit_stream(
                &acme,
                &[("app".to_string(), "new".to_string())]
                    .into_iter()
                    .collect(),
                &parts,
                &memtable,
            )
            .expect("the flushed stream must be counted once");
    }
