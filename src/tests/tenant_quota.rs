    use super::*;
    use crate::tenant::TenantId;
    use crate::tenant_policy::parse_ingest_rate;

    fn tenant(name: &str) -> TenantId {
        TenantId::parse(name).unwrap()
    }

    fn quota(config: Config, clock: Arc<Clock>, policy: Arc<TenantPolicy>) -> TenantQuota {
        TenantQuota::new(
            Arc::new(config),
            clock,
            Arc::new(RuntimeMetrics::new()),
            policy,
        )
    }

    /// A rate limiter that never refills is a permanent outage for the tenant,
    /// so this checks both halves: that it catches and that it lets go.
    #[test]
    fn a_tenant_over_its_rate_is_refused_and_recovers_when_the_budget_refills() {
        let clock = Clock::fixed(0);
        let config = Config {
            default_tenant_ingest_bytes_per_second: Some(1_000),
            tenant_ingest_burst: Duration::from_secs(1),
            max_push_bytes: 100,
            ..Config::default()
        };
        let quota = quota(config, clock.clone(), Arc::new(TenantPolicy::disabled()));
        let acme = tenant("acme");

        // One second of budget is banked at the start.
        quota.check(&acme, 1_000).expect("the first burst fits");
        let error = quota
            .check(&acme, 1_000)
            .expect_err("the bucket is empty now");
        assert_eq!(error.status, StatusCode::TOO_MANY_REQUESTS);
        assert!(
            error.retry_after.is_some(),
            "a throttled client needs to be told how long to wait"
        );

        clock.advance(Duration::from_millis(500));
        quota
            .check(&acme, 500)
            .expect("half a second refills half the rate");
        clock.advance(Duration::from_millis(1_500));
        quota
            .check(&acme, 1_000)
            .expect("the bucket refills to capacity and no further");
    }

    /// One tenant emptying its bucket must not touch another's. This is the
    /// whole point of the limit: the shared part layout means a neighbour's
    /// burst is otherwise taken out of everyone's flush capacity.
    #[test]
    fn one_tenants_burst_does_not_spend_another_tenants_budget() {
        let clock = Clock::fixed(0);
        let config = Config {
            default_tenant_ingest_bytes_per_second: Some(1_000),
            tenant_ingest_burst: Duration::from_secs(1),
            max_push_bytes: 100,
            ..Config::default()
        };
        let quota = quota(config, clock, Arc::new(TenantPolicy::disabled()));

        quota.check(&tenant("loud"), 1_000).unwrap();
        quota.check(&tenant("loud"), 1).unwrap_err();
        quota
            .check(&tenant("quiet"), 1_000)
            .expect("a quiet tenant still has its own full budget");
    }

    /// The capacity floor. A rate below one request's worth would otherwise
    /// reject that request forever however long the client waited, which is
    /// the latching failure the backpressure gate is built to avoid.
    #[test]
    fn a_request_at_the_body_limit_always_eventually_fits() {
        let clock = Clock::fixed(0);
        let config = Config {
            // Far below `max_push_bytes`: without the floor the bucket could
            // never hold one legal body.
            default_tenant_ingest_bytes_per_second: Some(10),
            tenant_ingest_burst: Duration::from_secs(1),
            max_push_bytes: 1_000_000,
            ..Config::default()
        };
        let quota = quota(config, clock, Arc::new(TenantPolicy::disabled()));
        quota
            .check(&tenant("acme"), 1_000_000)
            .expect("a body at the configured maximum fits the bucket");
    }

    /// No configured default and no pushed policy is the pre-quota behaviour,
    /// which every existing deployment is running.
    #[test]
    fn without_a_default_or_a_policy_nothing_is_limited() {
        let config = Config {
            default_tenant_ingest_bytes_per_second: None,
            ..Config::default()
        };
        let quota = quota(config, Clock::fixed(0), Arc::new(TenantPolicy::disabled()));
        for _ in 0..100 {
            quota.check(&tenant("acme"), u32::MAX as u64).unwrap();
        }
    }

    /// The pushed rate wins over the configured default, which is the point of
    /// putting it in the policy: plans differ per tenant and this instance's
    /// environment cannot express that.
    #[tokio::test]
    async fn a_pushed_rate_overrides_the_configured_default() {
        let clock = Clock::fixed(0);
        let policy = Arc::new(TenantPolicy::enabled_with_clock(clock.clone()));
        policy
            .push(&tenant("premium"), "7d", Some("1MiB/s"), None, None)
            .await
            .unwrap();
        policy
            .push(&tenant("blocked"), "7d", Some("0"), None, None)
            .await
            .unwrap();
        let config = Config {
            default_tenant_ingest_bytes_per_second: Some(10),
            tenant_ingest_burst: Duration::from_secs(1),
            max_push_bytes: 10,
            ..Config::default()
        };
        let quota = quota(config, clock, policy);

        quota
            .check(&tenant("premium"), 1024 * 1024)
            .expect("the pushed rate is a megabyte per second");
        quota
            .check(&tenant("blocked"), 1)
            .expect_err("rate 0 means the tenant may not write at all");
        quota
            .check(&tenant("unknown"), 100)
            .expect_err("a tenant with no pushed rate falls back to the default");
    }

    #[test]
    fn ingest_rates_parse_the_way_they_are_written() {
        assert_eq!(
            parse_ingest_rate("unlimited").unwrap(),
            TenantIngestRate::Unlimited
        );
        assert_eq!(
            parse_ingest_rate("0").unwrap(),
            TenantIngestRate::BytesPerSecond(0)
        );
        assert_eq!(
            parse_ingest_rate("512KiB/s").unwrap(),
            TenantIngestRate::BytesPerSecond(512 * 1024)
        );
        assert_eq!(
            parse_ingest_rate(" 4MiB ").unwrap(),
            TenantIngestRate::BytesPerSecond(4 * 1024 * 1024)
        );
        assert_eq!(
            parse_ingest_rate("2048").unwrap(),
            TenantIngestRate::BytesPerSecond(2048)
        );
        assert!(parse_ingest_rate("").is_err());
        assert!(parse_ingest_rate("fast").is_err());
        assert!(parse_ingest_rate("-1").is_err());
    }

    /// Buckets are keyed by a value that arrives in a request header, so the
    /// map has to shed entries it no longer needs.
    #[test]
    fn refilled_buckets_are_dropped_so_the_map_does_not_grow_forever() {
        let clock = Clock::fixed(0);
        let config = Config {
            default_tenant_ingest_bytes_per_second: Some(1_000_000),
            tenant_ingest_burst: Duration::from_secs(1),
            max_push_bytes: 100,
            ..Config::default()
        };
        let quota = quota(config, clock.clone(), Arc::new(TenantPolicy::disabled()));

        for index in 0..SWEEP_EVERY {
            quota.check(&tenant(&format!("t{index}")), 1).unwrap();
        }
        // Every bucket above spent one byte of a megabyte, so a second later
        // all of them are back at capacity and say nothing. The sweep runs on
        // a check count, so drive it to the next one.
        clock.advance(Duration::from_secs(1));
        for _ in 0..SWEEP_EVERY {
            quota.check(&tenant("last"), 1).unwrap();
        }
        assert!(
            quota.buckets.lock().unwrap().len() < SWEEP_EVERY as usize,
            "idle buckets that have refilled must not be kept"
        );
    }

    /// Reading and writing are separate resources. A shared bucket would let a
    /// tenant's queries decide whether its writes are accepted, which is a
    /// coupling nobody asked for and nobody could reason about.
    #[test]
    fn the_read_budget_and_the_write_budget_are_not_the_same_budget() {
        let clock = Clock::fixed(0);
        let config = Config {
            default_tenant_ingest_bytes_per_second: Some(1_000),
            default_tenant_query_scan_bytes_per_second: Some(1_000),
            tenant_ingest_burst: Duration::from_secs(1),
            max_push_bytes: 100,
            ..Config::default()
        };
        let quota = Arc::new(quota(
            config,
            clock,
            Arc::new(TenantPolicy::disabled()),
        ));
        let acme = tenant("acme");

        // Drain the read budget entirely.
        quota.charge_scan(&acme, 100_000);
        quota
            .begin_query(&acme)
            .expect_err("the read budget is spent");

        // Writing is unaffected.
        quota
            .check(&acme, 1_000)
            .expect("a spent read budget must not refuse a write");
    }

    /// The scan charge lands after the query, so an overrun is bounded at one
    /// query rather than prevented. That is the deliberate trade: the cost of a
    /// query is not knowable before running it.
    #[test]
    fn a_query_that_overruns_is_refused_on_the_next_one_and_recovers() {
        let clock = Clock::fixed(0);
        let config = Config {
            default_tenant_query_scan_bytes_per_second: Some(1_000),
            tenant_ingest_burst: Duration::from_secs(1),
            max_push_bytes: 1,
            ..Config::default()
        };
        let quota = Arc::new(quota(
            config,
            clock.clone(),
            Arc::new(TenantPolicy::disabled()),
        ));
        let acme = tenant("acme");

        let slot = quota.begin_query(&acme).expect("the first query is allowed");
        drop(slot);
        quota.charge_scan(&acme, 10_000);

        quota
            .begin_query(&acme)
            .expect_err("the next query pays for the last one");

        // And the budget refills, so this is throttling rather than a ban.
        clock.advance(Duration::from_secs(60));
        quota
            .begin_query(&acme)
            .expect("the budget refills over time");
    }

    /// A tenant issuing many concurrent scans would otherwise hold every permit
    /// of the shared query semaphore and queue everyone else behind it.
    #[test]
    fn one_tenant_cannot_hold_every_query_slot() {
        let config = Config {
            max_concurrent_queries_per_tenant: 2,
            ..Config::default()
        };
        let quota = Arc::new(quota(
            config,
            Clock::fixed(0),
            Arc::new(TenantPolicy::disabled()),
        ));
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

    /// Rate 0 on the read side is a suspended account: it still owns its data
    /// and must not be able to read it.
    #[tokio::test]
    async fn a_query_rate_of_zero_refuses_every_query() {
        let clock = Clock::fixed(0);
        let policy = Arc::new(TenantPolicy::enabled_with_clock(clock.clone()));
        policy
            .push(&tenant("suspended"), "7d", None, Some("0"), None)
            .await
            .unwrap();
        policy
            .push(&tenant("active"), "7d", None, Some("1MiB/s"), None)
            .await
            .unwrap();
        let quota = Arc::new(quota(Config::default(), clock, policy));

        quota.begin_query(&tenant("suspended")).unwrap_err();
        quota.begin_query(&tenant("active")).unwrap();
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
        let quota = quota(config, Clock::fixed(0), Arc::new(TenantPolicy::disabled()));
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
        let quota = quota(config.clone(), Clock::fixed(0), Arc::new(TenantPolicy::disabled()));
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
                        labels: stream.clone(),
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
