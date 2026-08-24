/// `patterns` — the templates the lines in a window collapse to, counted over
/// time. Grafana's Explore draws its "patterns" tab from this.
///
/// This is a deliberate subset of what Loki does. Loki mines patterns
/// continuously at ingest and keeps them in a persistent drain tree, so its
/// answer covers every line ever written. Doing that here would mean a second
/// index maintained on the write path, and the write path is the thing this
/// engine spends its budget protecting. So the patterns are mined at read time
/// from a bounded sample of the window, which makes the answer a description of
/// that sample rather than of the whole window — stated in the response so a
/// reader is not misled about which one they have.
const PATTERN_SAMPLE: usize = 5_000;
/// Distinct templates kept. Beyond this the mining itself is the cost, and a
/// list this long has stopped being a summary.
const MAX_PATTERNS: usize = 100;
/// Fraction of positions two lines must agree on to be the same pattern.
///
/// Low enough that one variable field does not fork a template, high enough
/// that unrelated messages of equal length do not merge into a row of
/// wildcards.
const PATTERN_SIMILARITY: f64 = 0.6;
const PATTERN_WILDCARD: &str = "<_>";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PatternsParams {
    pub query: String,
    pub start: Option<String>,
    pub end: Option<String>,
    pub step: Option<String>,
}

pub async fn patterns(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<PatternsParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let tenant = crate::tenant::from_headers(&headers, &state.config, &state.tenant_policy)
        .map_err(crate::tenant::TenantError::into_http)?;
    let metadata_params = crate::query::MetadataParams {
        start: params.start.clone(),
        end: params.end.clone(),
        query: None,
    };
    let Some(guard) = MetadataGuard::acquire(&state, &tenant, &metadata_params).await? else {
        return Ok(Json(serde_json::json!({ "status": "success", "data": [] })));
    };
    let query = match logql::parse_expr(&params.query).map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            format!("LogQL parse error: {error}"),
        )
    })? {
        logql::QueryExpr::Logs(logs) => logs,
        logql::QueryExpr::Metric(expr) => expr.log_query().clone(),
    };
    let step_ns = pattern_step_ns(params.step.as_deref(), &guard.window)?;

    let execution = run_unified_query_with_stats(
        state.clone(),
        tenant,
        query,
        // The same window a `query_range` over the same picker range would
        // scan: this samples the rows in the window, so a second row window for
        // one Grafana time range is exactly the inconsistency that hid the
        // inclusive-`end` defect.
        crate::part::QueryTimeRange::half_open(guard.window.start_ns, guard.window.end_ns),
        PATTERN_SAMPLE,
        false,
        Some(PATTERN_SAMPLE),
        crate::metrics::QueryEndpoint::Patterns,
    )
    .await
    .map_err(|error| (metric_error_status(&error), error))?;

    let mut miner = PatternMiner::default();
    let mut sampled = 0usize;
    for stream in &execution.results {
        for entry in &stream.entries {
            guard.check_deadline()?;
            miner.observe(&entry.line, entry.timestamp_ns, step_ns);
            sampled += 1;
        }
    }

    Ok(Json(serde_json::json!({
        "status": "success",
        "data": miner.into_response(),
        // Named so it cannot be mistaken for a count of the window. A caller
        // that wants the true totals has `count_over_time` for that.
        "sampledLines": sampled,
        "sampleLimit": PATTERN_SAMPLE,
    })))
}

/// Buckets are seconds in the response, so the step is rounded to one.
fn pattern_step_ns(
    step: Option<&str>,
    window: &crate::part::MetadataWindow,
) -> Result<i64, (StatusCode, String)> {
    let requested = match step {
        Some(raw) => parse_step_ns(Some(raw))
            .map_err(|error| (StatusCode::BAD_REQUEST, format!("invalid step: {error}")))?,
        // Loki's own default shape: about sixty buckets across the window. A
        // request that named neither end asks about an unbounded one, where
        // sixty buckets is not a meaningful thing to want; the floor below
        // decides it instead.
        None => window.end_ns.saturating_sub(window.start_ns) / 60,
    };
    Ok(requested.max(1_000_000_000))
}

#[derive(Default)]
struct PatternMiner {
    clusters: Vec<PatternCluster>,
}

struct PatternCluster {
    template: Vec<String>,
    count: u64,
    /// Bucket start in seconds to lines seen in it.
    samples: BTreeMap<i64, u64>,
}

impl PatternMiner {
    fn observe(&mut self, line: &str, timestamp_ns: i64, step_ns: i64) {
        let tokens = tokenize(line);
        if tokens.is_empty() {
            return;
        }
        let bucket = timestamp_ns.div_euclid(step_ns) * step_ns / 1_000_000_000;
        if let Some(cluster) = self
            .clusters
            .iter_mut()
            .find(|cluster| similarity(&cluster.template, &tokens) >= PATTERN_SIMILARITY)
        {
            for (position, token) in cluster.template.iter_mut().zip(&tokens) {
                if position != token {
                    *position = PATTERN_WILDCARD.to_string();
                }
            }
            cluster.count += 1;
            *cluster.samples.entry(bucket).or_default() += 1;
            return;
        }
        if self.clusters.len() >= MAX_PATTERNS {
            return;
        }
        self.clusters.push(PatternCluster {
            template: tokens,
            count: 1,
            samples: BTreeMap::from([(bucket, 1)]),
        });
    }

    fn into_response(mut self) -> Vec<serde_json::Value> {
        self.clusters
            .sort_by(|left, right| right.count.cmp(&left.count).then(left.template.cmp(&right.template)));
        self.clusters
            .into_iter()
            .map(|cluster| {
                serde_json::json!({
                    "pattern": cluster.template.join(" "),
                    "samples": cluster
                        .samples
                        .into_iter()
                        .map(|(second, count)| serde_json::json!([second, count]))
                        .collect::<Vec<_>>(),
                })
            })
            .collect()
    }
}

/// Splits on whitespace and masks the tokens that are variable on their face.
///
/// A token containing a digit is the cheap and durable form of "this is an id,
/// a duration, an address or a timestamp". Doing it before clustering rather
/// than relying on the clustering to discover it is what stops a thousand
/// request ids from becoming a thousand patterns.
fn tokenize(line: &str) -> Vec<String> {
    line.split_whitespace()
        .map(|token| {
            if token.chars().any(|character| character.is_ascii_digit()) {
                PATTERN_WILDCARD.to_string()
            } else {
                token.to_string()
            }
        })
        .collect()
}

/// Agreement over positions, counting only the ones the template still pins.
///
/// Wildcards are excluded from both sides of the fraction. Counting them as
/// matches would make a template that has degraded to mostly wildcards absorb
/// everything of that length.
fn similarity(template: &[String], tokens: &[String]) -> f64 {
    if template.len() != tokens.len() {
        return 0.0;
    }
    let fixed = template
        .iter()
        .filter(|token| *token != PATTERN_WILDCARD)
        .count();
    if fixed == 0 {
        return 0.0;
    }
    let agreeing = template
        .iter()
        .zip(tokens)
        .filter(|(left, right)| *left != PATTERN_WILDCARD && left == right)
        .count();
    agreeing as f64 / fixed as f64
}
