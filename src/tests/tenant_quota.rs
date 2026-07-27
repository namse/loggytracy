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
            .push(&tenant("premium"), "7d", Some("1MiB/s"))
            .await
            .unwrap();
        policy
            .push(&tenant("blocked"), "7d", Some("0"))
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
