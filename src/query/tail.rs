/// Live tail over a WebSocket, the endpoint Grafana's Explore "Live" toggle
/// opens.
///
/// There is no push path from ingest to a reader here, and there deliberately
/// is not one: a notification fan-out from the memtable would put a per-reader
/// cost on the write path, which is the one path this engine protects. Tail
/// polls the ordinary query path instead, so it sees exactly what a
/// `query_range` would and inherits every limit, the retention clamp and the
/// tenant isolation without a second implementation of any of them.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TailParams {
    pub query: String,
    /// Seconds to hold an entry back before sending it, so writers whose
    /// clocks trail the server's are not permanently cut off. Loki bounds this
    /// at 5 seconds and so does this.
    #[serde(default, rename = "delay_for")]
    pub delay_for: Option<u64>,
    pub limit: Option<usize>,
    pub start: Option<String>,
}

const MAX_TAIL_DELAY_SECONDS: u64 = 5;
const DEFAULT_TAIL_LIMIT: usize = 100;

pub async fn tail(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<TailParams>,
    upgrade: axum::extract::ws::WebSocketUpgrade,
) -> Result<axum::response::Response, (StatusCode, String)> {
    let tenant = crate::tenant::from_headers(&headers, &state.config)
        .map_err(crate::tenant::TenantError::into_http)?;
    // Everything that can be rejected is rejected before the upgrade. A client
    // that gets a 101 and then an immediate close cannot tell a bad query from
    // a server fault, so a bad query has to fail as an HTTP status.
    let parsed = logql::parse_expr(&params.query)
        .map_err(|error| (StatusCode::BAD_REQUEST, format!("LogQL parse error: {error}")))?;
    let logql::QueryExpr::Logs(query) = parsed else {
        return Err((
            StatusCode::BAD_REQUEST,
            "tail supports log queries only; metric expressions have no stream to follow"
                .to_string(),
        ));
    };
    let delay = Duration::from_secs(
        params
            .delay_for
            .unwrap_or(0)
            .min(MAX_TAIL_DELAY_SECONDS),
    );
    let limit = parse_limit(
        params.limit.or(Some(DEFAULT_TAIL_LIMIT)),
        state.config.max_log_limit.min(MAX_LOG_LIMIT),
    )
    .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    let start_ns = match params.start.as_deref() {
        Some(raw) => parse_time_ns(raw).map_err(|error| (StatusCode::BAD_REQUEST, error))?,
        None => state.clock.now_ns(),
    };

    // Bounded before the upgrade for the same reason ingest is: a socket held
    // open is a poll loop scheduled forever, and the scan semaphore it borrows
    // per poll is shared with ordinary queries.
    let permit = Arc::clone(&state.tail_semaphore)
        .try_acquire_owned()
        .map_err(|_| {
            (
                StatusCode::TOO_MANY_REQUESTS,
                format!(
                    "too many live tail connections; the limit is {}",
                    state.config.max_concurrent_tails
                ),
            )
        })?;

    Ok(upgrade.on_upgrade(move |socket| async move {
        let _permit = permit;
        run_tail(state, tenant, query, socket, start_ns, delay, limit).await;
    }))
}

/// What has already been sent, so a repeated poll does not resend it.
///
/// A cursor alone cannot do this. Advancing it past the newest timestamp drops
/// any entry that shares that nanosecond, and leaving it on the timestamp
/// resends them; both are wrong and both are common, because a batch of lines
/// written together often carries one timestamp. So the cursor stays on the
/// newest timestamp seen and the entries at exactly that timestamp are
/// remembered until it moves.
struct TailCursor {
    since_ns: i64,
    sent_at_since: HashSet<u64>,
}

impl TailCursor {
    fn new(since_ns: i64) -> Self {
        Self {
            since_ns,
            sent_at_since: HashSet::new(),
        }
    }

    fn is_new(&self, labels: &Labels, entry: &LogEntry) -> bool {
        entry.timestamp_ns > self.since_ns
            || (entry.timestamp_ns == self.since_ns
                && !self.sent_at_since.contains(&entry_key(labels, entry)))
    }

    fn record(&mut self, labels: &Labels, entry: &LogEntry) {
        if entry.timestamp_ns > self.since_ns {
            self.since_ns = entry.timestamp_ns;
            self.sent_at_since.clear();
        }
        if entry.timestamp_ns == self.since_ns {
            self.sent_at_since.insert(entry_key(labels, entry));
        }
    }
}

fn entry_key(labels: &Labels, entry: &LogEntry) -> u64 {
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for (name, value) in labels {
        name.hash(&mut hasher);
        value.hash(&mut hasher);
    }
    entry.line.hash(&mut hasher);
    entry.structured_metadata.hash(&mut hasher);
    hasher.finish()
}

async fn run_tail(
    state: Arc<AppState>,
    tenant: TenantId,
    query: logql::LogQuery,
    mut socket: axum::extract::ws::WebSocket,
    start_ns: i64,
    delay: Duration,
    limit: usize,
) {
    use axum::extract::ws::Message;

    let mut cursor = TailCursor::new(start_ns.saturating_sub(1));
    let mut ticker = tokio::time::interval(state.config.tail_poll_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut drain = state.shutdown.subscribe();

    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            // A drain is a planned shutdown; closing the socket lets the client
            // reconnect to whatever replaces this instance rather than sit on a
            // connection that will never produce another line.
            _ = crate::shutdown::wait_for_drain(&mut drain) => return,
            message = socket.recv() => {
                // The client only ever closes. Anything else is still a signal
                // that it is gone once `recv` yields `None`.
                match message {
                    Some(Ok(_)) => continue,
                    Some(Err(_)) | None => return,
                }
            }
        }

        let end_ns = state.clock.now_ns().saturating_sub(duration_to_i64_ns(delay));
        let Some(payload) =
            tail_poll(&state, &tenant, &query, &mut cursor, end_ns, limit).await
        else {
            continue;
        };
        if socket
            .send(Message::Text(payload.to_string().into()))
            .await
            .is_err()
        {
            return;
        }
    }
}

/// One poll: everything the tail sends, decided without a socket in sight.
///
/// `None` means there is nothing to send — no new entries, a window retention
/// has emptied, or a scan that failed. A failing poll is not a failing
/// connection: the usual cause is a scan budget hit by a burst, and the next
/// poll covers a smaller window. Closing on it would make a transient overrun
/// look like a broken endpoint.
async fn tail_poll(
    state: &Arc<AppState>,
    tenant: &TenantId,
    query: &logql::LogQuery,
    cursor: &mut TailCursor,
    end_ns: i64,
    limit: usize,
) -> Option<serde_json::Value> {
    if end_ns < cursor.since_ns {
        return None;
    }
    // The retention floor is re-read every poll rather than resolved once: a
    // tail can outlive the policy it started under, and a downgrade has to take
    // effect on a live connection too.
    let scan_start = clamp_to_retention(cursor.since_ns, state.tenant_policy.query_floor_ns(tenant));
    if scan_start > end_ns {
        return None;
    }

    // The window starts *on* the cursor, so the entries already sent at that
    // timestamp come back and are filtered out below. Asking for them on top of
    // the limit keeps it bounding new lines delivered rather than rows read —
    // otherwise every poll after a timestamp collision would deliver fewer
    // lines than asked for, and a busy stream would never catch up.
    let scan_limit = limit
        .saturating_add(cursor.sent_at_since.len())
        .min(state.config.max_log_limit.min(MAX_LOG_LIMIT));

    let execution = run_unified_query_with_stats(
        state.clone(),
        tenant.clone(),
        query.clone(),
        // Closed: `end_ns` is this poll's "now minus delay", not a bound the
        // client asked to exclude. Excluding it would hold the newest line back
        // a poll, and forever if the clock does not move.
        crate::part::QueryTimeRange::closed(scan_start, end_ns),
        scan_limit,
        true,
        Some(state.config.max_query_scan_rows.min(MAX_LOG_SCAN_ROWS)),
    )
    .await
    .ok()?;

    let mut fresh: Vec<StreamResult> = Vec::new();
    for stream in execution.results {
        let entries: Vec<LogEntry> = stream
            .entries
            .into_iter()
            .filter(|entry| cursor.is_new(&stream.labels, entry))
            .collect();
        if entries.is_empty() {
            continue;
        }
        for entry in &entries {
            cursor.record(&stream.labels, entry);
        }
        fresh.push(StreamResult {
            labels: stream.labels,
            entries,
        });
    }
    if fresh.is_empty() {
        return None;
    }

    // `dropped_entries` is always empty, and that is a claim rather than a
    // stub. Each poll asks for the *oldest* entries after the cursor, and the
    // cursor advances only over what was sent, so a burst larger than `limit`
    // is left for the next poll rather than skipped. A tail that cannot keep
    // up falls behind — visibly, in the timestamps it delivers — instead of
    // losing lines, which is the failure an operator can actually act on.
    Some(serde_json::json!({
        "streams": build_stream_data(fresh),
        "dropped_entries": [],
    }))
}
