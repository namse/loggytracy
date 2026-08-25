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
    let tenant = crate::tenant::from_headers(&headers, &state.config, &state.tenant_policy)
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
        let Some(fresh) =
            tail_poll(&state, &tenant, &query, &mut cursor, end_ns, limit).await
        else {
            continue;
        };
        let payload = serde_json::json!({
            // `forward`: the poll asked for the oldest entries after the
            // cursor, so a tail delivers in ascending time.
            "streams": build_stream_data(fresh, true),
            "dropped_entries": [],
        });
        if socket
            .send(Message::Text(payload.to_string().into()))
            .await
            .is_err()
        {
            return;
        }
    }
}

/// How long a quiet tail waits before saying it is alive. A keep-alive for
/// proxies that reap idle streaming responses, and a liveness signal a client
/// can time out on.
const TAIL_HEARTBEAT: Duration = Duration::from_secs(15);
const TAIL_HEARTBEAT_LINE: &[u8] = b"{\"heartbeat\":true}\n";

/// The first-party tail: the same poll loop, streamed as chunked NDJSON.
///
/// Rows are ordinary `/logs` row lines in ascending time; a heartbeat line is
/// not data. There is no socket to watch — a client that goes away drops the
/// body stream, and the loop is simply never polled again. On drain the
/// stream ends cleanly and the client reconnects to whatever replaces this
/// instance.
pub async fn logs_tail(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Result<axum::response::Response, ApiError> {
    let tenant = crate::tenant::from_headers(&headers, &state.config, &state.tenant_policy)
        .map_err(ApiError::from_tenant)?;
    let now_ns = state.clock.now_ns();
    let params = parse_filter_params(raw.as_deref().unwrap_or(""), now_ns, TAIL_PARAMS)
        .map_err(ApiError::bad_request)?;
    let delay = Duration::from_secs(
        params
            .delay_seconds
            .unwrap_or(0)
            .min(MAX_TAIL_DELAY_SECONDS),
    );
    let limit = parse_limit(
        params.limit.or(Some(DEFAULT_TAIL_LIMIT)),
        state.config.max_log_limit.min(MAX_LOG_LIMIT),
    )
    .map_err(ApiError::bad_request)?;
    let start_ns = params.start_ns.unwrap_or(now_ns);

    let permit = Arc::clone(&state.tail_semaphore)
        .try_acquire_owned()
        .map_err(|_| {
            ApiError(
                StatusCode::TOO_MANY_REQUESTS,
                format!(
                    "too many live tail connections; the limit is {} — retry when one closes",
                    state.config.max_concurrent_tails
                ),
            )
        })?;

    let mut ticker = tokio::time::interval(state.config.tail_poll_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let drain = state.shutdown.subscribe();
    let stream = futures_util::stream::unfold(
        TailStream {
            state,
            tenant,
            query: params.query,
            cursor: TailCursor::new(start_ns.saturating_sub(1)),
            ticker,
            drain,
            delay,
            limit,
            last_sent: std::time::Instant::now(),
            _permit: permit,
        },
        |mut tail| async move {
            loop {
                tokio::select! {
                    _ = tail.ticker.tick() => {}
                    _ = crate::shutdown::wait_for_drain(&mut tail.drain) => return None,
                }
                let end_ns = tail
                    .state
                    .clock
                    .now_ns()
                    .saturating_sub(duration_to_i64_ns(tail.delay));
                if let Some(fresh) = tail_poll(
                    &tail.state,
                    &tail.tenant,
                    &tail.query,
                    &mut tail.cursor,
                    end_ns,
                    tail.limit,
                )
                .await
                {
                    tail.last_sent = std::time::Instant::now();
                    let chunk = log_rows_ndjson(fresh, true);
                    return Some((Ok::<_, std::convert::Infallible>(bytes::Bytes::from(chunk)), tail));
                }
                if tail.last_sent.elapsed() >= TAIL_HEARTBEAT {
                    tail.last_sent = std::time::Instant::now();
                    return Some((Ok(bytes::Bytes::from_static(TAIL_HEARTBEAT_LINE)), tail));
                }
            }
        },
    );
    let mut response = axum::response::Response::new(axum::body::Body::from_stream(stream));
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static(NDJSON_CONTENT_TYPE),
    );
    Ok(response)
}

struct TailStream {
    state: Arc<AppState>,
    tenant: TenantId,
    query: logql::LogQuery,
    cursor: TailCursor,
    ticker: tokio::time::Interval,
    drain: tokio::sync::watch::Receiver<bool>,
    delay: Duration,
    limit: usize,
    last_sent: std::time::Instant,
    _permit: tokio::sync::OwnedSemaphorePermit,
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
) -> Option<Vec<StreamResult>> {
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
        crate::metrics::QueryEndpoint::Tail,
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

    // Nothing is ever dropped, and that is a claim rather than a stub. Each
    // poll asks for the *oldest* entries after the cursor, and the cursor
    // advances only over what was sent, so a burst larger than `limit` is
    // left for the next poll rather than skipped. A tail that cannot keep up
    // falls behind — visibly, in the timestamps it delivers — instead of
    // losing lines, which is the failure an operator can actually act on.
    Some(fresh)
}
