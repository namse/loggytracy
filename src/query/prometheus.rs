/// The operator endpoints: readiness and the Prometheus text scrape.
pub async fn ready(
    State(state): State<Arc<AppState>>,
) -> Result<&'static str, (StatusCode, String)> {
    if state.shutdown.is_fenced() {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "fenced by a newer writer; this instance no longer owns the object-store prefix"
                .to_string(),
        ));
    }
    if state.shutdown.is_draining() {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            format!(
                "draining for shutdown: force_flush_complete={}, pending_flush_bytes={}",
                state.shutdown.is_flush_complete(),
                state.shutdown.pending_flush_bytes(),
            ),
        ));
    }
    let mut unavailable = Vec::new();
    if !state.journal.is_healthy() {
        unavailable.push("journal writer");
    }
    if !state.flush_healthy.load(Ordering::Acquire) {
        unavailable.push("flush worker");
    }
    if !state.merge_healthy.load(Ordering::Acquire) {
        unavailable.push("merge worker");
    }
    if !state.retention_healthy.load(Ordering::Acquire) {
        unavailable.push("retention worker");
    }
    if !state.otlp_healthy.load(Ordering::Acquire) {
        unavailable.push("OTLP gRPC server");
    }
    if let Some(cache) = &state.remote_cache {
        if !cache.is_remote_healthy() {
            unavailable.push("object store");
        }
        if !cache.is_cache_healthy() {
            unavailable.push("local cache");
        }
    }

    if unavailable.is_empty() {
        Ok("ready")
    } else {
        Err((
            StatusCode::SERVICE_UNAVAILABLE,
            format!("{} unavailable", unavailable.join(", ")),
        ))
    }
}


pub async fn metrics(State(state): State<Arc<AppState>>) -> String {
    let mem = state.memtable.global_stats();
    let disk = state.parts.global_stats();
    let remote_healthy = state
        .remote_cache
        .as_ref()
        .is_none_or(|cache| cache.is_remote_healthy());
    let cache_healthy = state
        .remote_cache
        .as_ref()
        .is_none_or(|cache| cache.is_cache_healthy());
    let wal_backlog_bytes = state.journal.wal_backlog_bytes();
    // Both of these are published by the workers that already hold the
    // snapshots they describe. A scrape must not be able to ask for a walk of
    // every part's tenant index.
    let merge_debt_parts = m_merge_debt(&state);
    // Read rather than computed: the registry maintains these as its set
    // changes, so they are current whether or not any worker has ticked.
    let layout = state.parts.layout_totals();
    let policy = tenant_policy_gauges(&state);
    let m = &state.metrics;
    let mut body = format!(
        "# TYPE loggytracy_memtable_entries gauge\n\
loggytracy_memtable_entries {}\n\
# TYPE loggytracy_memtable_bytes gauge\n\
loggytracy_memtable_bytes {}\n\
# TYPE loggytracy_part_entries gauge\n\
loggytracy_part_entries {}\n\
# TYPE loggytracy_part_bytes gauge\n\
loggytracy_part_bytes {}\n\
# TYPE loggytracy_part_count gauge\n\
loggytracy_part_count {}\n\
# TYPE loggytracy_trace_part_count gauge\n\
loggytracy_trace_part_count {}\n\
# TYPE loggytracy_remote_healthy gauge\n\
loggytracy_remote_healthy {}\n\
# HELP loggytracy_remote_consecutive_failures Object-store failures since the last success. The health flag hides these below its threshold, so this is where a degrading store shows before it is declared down.\n\
# TYPE loggytracy_remote_consecutive_failures gauge\n\
loggytracy_remote_consecutive_failures {}\n\
# TYPE loggytracy_cache_healthy gauge\n\
loggytracy_cache_healthy {}\n\
# TYPE loggytracy_wal_backlog_bytes gauge\n\
loggytracy_wal_backlog_bytes {}\n\
# HELP loggytracy_inflight_push_bytes Request bodies admitted and not yet answered, bounded by max_inflight_push_bytes. Counted at admission because a body is already resident by the time a handler sees it.\n\
# TYPE loggytracy_inflight_push_bytes gauge\n\
loggytracy_inflight_push_bytes {}\n\
# HELP loggytracy_data_dir_free_bytes Free space on the filesystem holding the data directory, as the unprivileged user sees it. Ingest is refused below LOGGYTRACY_MIN_FREE_DISK_BYTES.\n\
# TYPE loggytracy_data_dir_free_bytes gauge\n\
loggytracy_data_dir_free_bytes {}\n\
# HELP loggytracy_data_dir_total_bytes Size of that filesystem, so the gauge above can be read as a fraction without knowing the volume.\n\
# TYPE loggytracy_data_dir_total_bytes gauge\n\
loggytracy_data_dir_total_bytes {}\n\
# TYPE loggytracy_merge_debt_parts gauge\n\
loggytracy_merge_debt_parts {}\n\
# HELP loggytracy_part_tenant_segments (tenant, part) pairs. The shared-part layout spends a row group, two blooms and a metadata segment per pair.\n\
# TYPE loggytracy_part_tenant_segments gauge\n\
loggytracy_part_tenant_segments {}\n\
# HELP loggytracy_part_sidecar_resident_bytes Bloom and stream-index bytes resident for open parts. The bloom half is bounded by sidecar_cache_max_bytes and evicted LRU; the stream-index half stays resident per part.\n\
# TYPE loggytracy_part_sidecar_resident_bytes gauge\n\
loggytracy_part_sidecar_resident_bytes {}\n\
# HELP loggytracy_row_group_cache_bytes Decoded row groups held for reuse across scans, bounded by row_group_cache_max_bytes.\n\
# TYPE loggytracy_row_group_cache_bytes gauge\n\
loggytracy_row_group_cache_bytes {}\n\
# HELP loggytracy_part_meta_bytes Total meta.json across parts, which startup parses before serving.\n\
# TYPE loggytracy_part_meta_bytes gauge\n\
loggytracy_part_meta_bytes {}\n\
# TYPE loggytracy_ingest_requests_total counter\n\
loggytracy_ingest_requests_total {}\n\
# TYPE loggytracy_ingest_errors_total counter\n\
loggytracy_ingest_errors_total {}\n\
# TYPE loggytracy_ingest_throttled_total counter\n\
loggytracy_ingest_throttled_total {}\n\
# HELP loggytracy_query_quota_rejected_total Queries refused by the tenant's own concurrency limit, as opposed to queries this instance failed to answer.\n\
# TYPE loggytracy_query_quota_rejected_total counter\n\
loggytracy_query_quota_rejected_total {}\n\
# HELP loggytracy_storage_limit_rejected_total Writes refused because the tenant already stores what its plan sells. Unlike the rate rejections this one clears only when retention retires parts.\n\
# TYPE loggytracy_storage_limit_rejected_total counter\n\
loggytracy_storage_limit_rejected_total {}\n\
# HELP loggytracy_wal_replayed_records Records this process replayed from the WAL at startup. Non-zero means the previous run did not shut down cleanly.\n\
# TYPE loggytracy_wal_replayed_records gauge\n\
loggytracy_wal_replayed_records {}\n\
# HELP loggytracy_wal_replayed_entries Log entries in those records — the upper bound on how many lines this restart may have duplicated.\n\
# TYPE loggytracy_wal_replayed_entries gauge\n\
loggytracy_wal_replayed_entries {}\n\
# TYPE loggytracy_memtable_buffered_bytes gauge\n\
loggytracy_memtable_buffered_bytes {}\n\
# TYPE loggytracy_flush_success_total counter\n\
loggytracy_flush_success_total {}\n\
# TYPE loggytracy_flush_errors_total counter\n\
loggytracy_flush_errors_total {}\n\
# TYPE loggytracy_merge_success_total counter\n\
loggytracy_merge_success_total {}\n\
# TYPE loggytracy_merge_errors_total counter\n\
loggytracy_merge_errors_total {}\n\
# TYPE loggytracy_merge_inputs_changed_total counter\n\
loggytracy_merge_inputs_changed_total {}\n\
# TYPE loggytracy_retention_success_total counter\n\
loggytracy_retention_success_total {}\n\
# TYPE loggytracy_retention_errors_total counter\n\
loggytracy_retention_errors_total {}\n\
# TYPE loggytracy_retention_expired_rows_dropped_total counter\n\
loggytracy_retention_expired_rows_dropped_total {}\n\
# TYPE loggytracy_retention_parts_rewritten_total counter\n\
loggytracy_retention_parts_rewritten_total {}\n\
# TYPE loggytracy_retention_rewrite_skipped_total counter\n\
loggytracy_retention_rewrite_skipped_total {}\n\
# TYPE loggytracy_tenant_policy_push_accepted_total counter\n\
loggytracy_tenant_policy_push_accepted_total {}\n\
# TYPE loggytracy_tenant_policy_push_rejected_total counter\n\
loggytracy_tenant_policy_push_rejected_total {}\n\
# TYPE loggytracy_tenant_policy_push_persist_errors_total counter\n\
loggytracy_tenant_policy_push_persist_errors_total {}\n\
# TYPE loggytracy_tenant_policy_known_tenants gauge\n\
loggytracy_tenant_policy_known_tenants {}\n\
# TYPE loggytracy_tenant_policy_infinite_tenants gauge\n\
loggytracy_tenant_policy_infinite_tenants {}\n\
# TYPE loggytracy_tenant_policy_unknown_tenants gauge\n\
loggytracy_tenant_policy_unknown_tenants {}\n\
# TYPE loggytracy_tenant_policy_last_push_age_seconds gauge\n\
loggytracy_tenant_policy_last_push_age_seconds {}\n\
# TYPE loggytracy_query_success_total counter\n\
loggytracy_query_success_total {}\n\
# TYPE loggytracy_query_errors_total counter\n\
loggytracy_query_errors_total {}\n\
# TYPE loggytracy_query_scanned_rows_total counter\n\
loggytracy_query_scanned_rows_total {}\n\
# TYPE loggytracy_query_scanned_bytes_total counter\n\
loggytracy_query_scanned_bytes_total {}\n\
# TYPE loggytracy_query_latency_ns_total counter\n\
loggytracy_query_latency_ns_total {}\n\
# HELP loggytracy_query_scans_in_flight Scans holding a scheduler permit right now, out of max_concurrent_query_scans.\n\
# TYPE loggytracy_query_scans_in_flight gauge\n\
loggytracy_query_scans_in_flight {}\n\
# HELP loggytracy_query_scans_in_flight_peak High-water mark of that since start. The memory budget's largest term is max_concurrent_query_scans x max_query_memory_bytes, and this is how far into it a run actually reached — a sampled gauge cannot see a burst that fills the scheduler and drains between two scrapes.\n\
# TYPE loggytracy_query_scans_in_flight_peak gauge\n\
loggytracy_query_scans_in_flight_peak {}\n\
# HELP loggytracy_query_scans_queued_total Scans that found every slot taken and waited. Nonzero is proof the concurrency limit bound, which the peak alone only suggests.\n\
# TYPE loggytracy_query_scans_queued_total counter\n\
loggytracy_query_scans_queued_total {}\n\
# TYPE loggytracy_query_scan_queue_wait_ns_total counter\n\
loggytracy_query_scan_queue_wait_ns_total {}\n\
# TYPE loggytracy_remote_restore_success_total counter\n\
loggytracy_remote_restore_success_total {}\n\
# TYPE loggytracy_remote_restore_errors_total counter\n\
loggytracy_remote_restore_errors_total {}\n\
# TYPE loggytracy_remote_restore_latency_ns_total counter\n\
loggytracy_remote_restore_latency_ns_total {}\n\
# TYPE loggytracy_cache_evictions_total counter\n\
loggytracy_cache_evictions_total {}\n\
# TYPE loggytracy_drain_in_progress gauge\n\
loggytracy_drain_in_progress {}\n\
# TYPE loggytracy_pending_flush_bytes gauge\n\
loggytracy_pending_flush_bytes {}\n\
# TYPE loggytracy_force_flush_complete gauge\n\
loggytracy_force_flush_complete {}\n\
# HELP loggytracy_build_info Build identity, always 1. Join on it to attribute a series to a revision.\n\
# TYPE loggytracy_build_info gauge\n\
loggytracy_build_info{{version=\"{}\",revision=\"{}\"}} 1\n\
# HELP loggytracy_query_latency_ms Query latency by the endpoint the query arrived at. The cumulative _ns_total counters only ever yielded a mean; every target is written as p95/p99, so use histogram_quantile on this, and sum by (le) across endpoints for the whole read path.\n\
# TYPE loggytracy_query_latency_ms histogram\n\
{}\
# HELP loggytracy_remote_restore_latency_ms Object-store restore latency, the cost of a cache miss.\n\
# TYPE loggytracy_remote_restore_latency_ms histogram\n\
{}",
        mem.entries,
        mem.bytes,
        disk.entries,
        disk.bytes,
        state.parts.part_count(),
        state.trace_parts.part_count(),
        remote_healthy as u8,
        state
            .remote_cache
            .as_ref()
            .map(|cache| cache.consecutive_remote_failures())
            .unwrap_or(0),
        cache_healthy as u8,
        wal_backlog_bytes,
        state.ingest_gate.inflight_body_bytes(),
        state.disk.free_bytes(),
        state.disk.total_bytes(),
        merge_debt_parts,
        layout.tenant_segments,
        layout
            .sidecar_resident_bytes
            .saturating_add(crate::part::bloom_cache_bytes()),
        crate::part::row_group_cache_bytes(),
        layout.meta_bytes,
        m.ingest_requests.load(Ordering::Relaxed),
        m.ingest_errors.load(Ordering::Relaxed),
        m.ingest_throttled.load(Ordering::Relaxed),
        m.query_quota_rejected.load(Ordering::Relaxed),
        m.storage_limit_rejected.load(Ordering::Relaxed),
        m.wal_replayed_records.load(Ordering::Relaxed),
        m.wal_replayed_entries.load(Ordering::Relaxed),
        state.ingest_gate.buffered_bytes(),
        m.flush_success.load(Ordering::Relaxed),
        m.flush_errors.load(Ordering::Relaxed),
        m.merge_success.load(Ordering::Relaxed),
        m.merge_errors.load(Ordering::Relaxed),
        m.merge_inputs_changed.load(Ordering::Relaxed),
        m.retention_success.load(Ordering::Relaxed),
        m.retention_errors.load(Ordering::Relaxed),
        m.retention_expired_rows_dropped.load(Ordering::Relaxed),
        m.retention_parts_rewritten.load(Ordering::Relaxed),
        m.retention_rewrite_skipped.load(Ordering::Relaxed),
        state
            .tenant_policy
            .metrics
            .push_accepted
            .load(Ordering::Relaxed),
        state
            .tenant_policy
            .metrics
            .push_rejected
            .load(Ordering::Relaxed),
        state
            .tenant_policy
            .metrics
            .push_persist_errors
            .load(Ordering::Relaxed),
        policy.known_tenants,
        policy.infinite_tenants,
        policy.unknown_tenants,
        policy.last_push_age_seconds,
        m.query_success.load(Ordering::Relaxed),
        m.query_errors.load(Ordering::Relaxed),
        m.query_scanned_rows.load(Ordering::Relaxed),
        m.query_scanned_bytes.load(Ordering::Relaxed),
        m.query_latency_ns.load(Ordering::Relaxed),
        m.query_scans_in_flight.load(Ordering::Relaxed),
        m.query_scans_in_flight_peak.load(Ordering::Relaxed),
        m.query_scans_queued.load(Ordering::Relaxed),
        m.query_scan_queue_wait_ns.load(Ordering::Relaxed),
        m.remote_restore_success.load(Ordering::Relaxed),
        m.remote_restore_errors.load(Ordering::Relaxed),
        m.remote_restore_latency_ns.load(Ordering::Relaxed),
        m.cache_evictions.load(Ordering::Relaxed),
        state.shutdown.is_draining() as u8,
        state.shutdown.pending_flush_bytes(),
        state.shutdown.is_flush_complete() as u8,
        env!("CARGO_PKG_VERSION"),
        build_revision(),
        crate::metrics::QueryEndpoint::ALL
            .iter()
            .map(|endpoint| {
                m.query_latency[*endpoint as usize].render_labeled(
                    "loggytracy_query_latency_ms",
                    &format!("endpoint=\"{}\"", endpoint.label()),
                )
            })
            .collect::<String>(),
        m.remote_restore_latency
            .render("loggytracy_remote_restore_latency_ms"),
    );
    body.push_str(&object_store_operation_metrics(&state));
    body.push_str(&restore_economics_metrics());
    body.push_str(&delete_request_metrics(&state));
    body.push_str(&journal_writer_metrics(&state));
    body.push_str(&series_ladder_metrics(&state));
    body.push_str(&crate::memprof::render());
    body
}

/// Where an accepted push's server-side time went.
///
/// Every push in the process is written by one task, so these four phases are
/// the whole of it and they are additive: queue, write, fsync, insert. The
/// question they exist to answer is which of them the push tail is made of —
/// a p50 of 12 ms beside a p95 that moves between 40 and 106 ms with nothing
/// but the client's connection count (`todo.md`, 2026-08-12) is a queue, and
/// until these there was no number in the process that could say so.
/// The M14 degradation ladder's observability: every rung moves one of these,
/// and the comparison bed's churn table is built from them.
fn series_ladder_metrics(state: &AppState) -> String {
    use std::sync::atomic::Ordering;
    let series = state.journal.series_memtable();
    let counters = series.counters();
    format!(
        "# HELP loggytracy_active_series Live metric series index entries across tenants. \
Bounded per tenant by LOGGYTRACY_MAX_ACTIVE_SERIES.\n\
# TYPE loggytracy_active_series gauge\n\
loggytracy_active_series {}\n\
# TYPE loggytracy_series_created_total counter\n\
loggytracy_series_created_total {}\n\
# HELP loggytracy_series_evicted_idle_total Series whose index state left at the idle \
horizon (LOGGYTRACY_METRIC_SERIES_IDLE_TIMEOUT); their history stays in parts.\n\
# TYPE loggytracy_series_evicted_idle_total counter\n\
loggytracy_series_evicted_idle_total {}\n\
# HELP loggytracy_series_rejected_total New series refused at the max_active_series \
boundary. Known series are never refused by this rung.\n\
# TYPE loggytracy_series_rejected_total counter\n\
loggytracy_series_rejected_total {}\n\
# TYPE loggytracy_metric_datapoints_rejected_total counter\n\
loggytracy_metric_datapoints_rejected_total {}\n\
# TYPE loggytracy_metric_samples_rejected_total counter\n\
loggytracy_metric_samples_rejected_total {}\n\
# TYPE loggytracy_series_memtable_bytes gauge\n\
loggytracy_series_memtable_bytes {}\n",
        counters.active_series.load(Ordering::Relaxed),
        counters.series_created_total.load(Ordering::Relaxed),
        counters.series_evicted_idle_total.load(Ordering::Relaxed),
        counters.series_rejected_total.load(Ordering::Relaxed),
        counters
            .metric_datapoints_rejected_total
            .load(Ordering::Relaxed),
        counters
            .metric_samples_rejected_total
            .load(Ordering::Relaxed),
        series.approximate_size(),
    )
}

fn journal_writer_metrics(state: &AppState) -> String {
    let metrics = state.journal.metrics();
    let mut out = String::new();
    out.push_str(
        "# HELP loggytracy_journal_batches_total Batches the writer task wrote, one fsync each.\n\
# TYPE loggytracy_journal_batches_total counter\n",
    );
    out.push_str(&format!(
        "loggytracy_journal_batches_total {}\n",
        metrics.batches.load(Ordering::Relaxed)
    ));
    out.push_str(
        "# HELP loggytracy_journal_batched_records_total Appends carried by those batches. Divided by the batches, the number of pushes sharing each fsync.\n\
# TYPE loggytracy_journal_batched_records_total counter\n",
    );
    out.push_str(&format!(
        "loggytracy_journal_batched_records_total {}\n",
        metrics.batched_records.load(Ordering::Relaxed)
    ));
    out.push_str(
        &metrics
            .append_queue_wait
            .render("loggytracy_journal_append_queue_wait_ms"),
    );
    out.push_str(&metrics.batch_write.render("loggytracy_journal_write_ms"));
    out.push_str(&metrics.batch_fsync.render("loggytracy_journal_fsync_ms"));
    out.push_str(&metrics.batch_insert.render("loggytracy_journal_insert_ms"));
    out.push_str(
        &metrics
            .checkpoint
            .render("loggytracy_journal_checkpoint_ms"),
    );
    out.push_str(&flush_phase_metrics(state));
    out.push_str(
        "# HELP loggytracy_query_memory_exhausted_total Queries refused because this instance's query memory pool had no room. Distinct from the tenant read quota, which says a tenant asked for more than it was sold, and from a scan-limit refusal, which says the query was too broad: this one says the instance ran out of room for work it was willing to do, and is the read side's counterpart to ingest_throttled.\n\
# TYPE loggytracy_query_memory_exhausted_total counter\n",
    );
    out.push_str(&format!(
        "loggytracy_query_memory_exhausted_total {}\n",
        state.query_memory_pool.exhausted()
    ));
    out
}

/// Where a flush pass's time goes.
///
/// The companion to the journal writer's phases, and the more consequential of
/// the two: the rate ladder of 2026-08-13 put this engine's capacity ceiling
/// here rather than in the WAL. A pass that takes longer than the memtable
/// takes to refill *is* the ceiling, and before these the only evidence of it
/// reaching one was a `429` arriving at a client.
fn flush_phase_metrics(state: &AppState) -> String {
    let flush = &state.metrics.flush;
    let mut out = String::new();
    out.push_str(
        "# HELP loggytracy_flush_rows_total Rows written into parts by the flush loop.\n\
# TYPE loggytracy_flush_rows_total counter\n",
    );
    out.push_str(&format!(
        "loggytracy_flush_rows_total {}\n",
        flush.rows.load(Ordering::Relaxed)
    ));
    out.push_str(
        "# HELP loggytracy_flush_parts_total Parts those passes produced. Divided by the rows, the part size the chunker is choosing.\n\
# TYPE loggytracy_flush_parts_total counter\n",
    );
    out.push_str(&format!(
        "loggytracy_flush_parts_total {}\n",
        flush.parts.load(Ordering::Relaxed)
    ));
    out.push_str(
        &flush
            .checkpoint_wait
            .render("loggytracy_flush_checkpoint_wait_ms"),
    );
    out.push_str(&flush.build.render("loggytracy_flush_build_ms"));
    out.push_str(&flush.open.render("loggytracy_flush_open_ms"));
    out.push_str(&flush.visibility.render("loggytracy_flush_visibility_ms"));
    out.push_str(&flush.advance_checkpoint.render("loggytracy_flush_advance_ms"));
    // The build phase's own four, counted per part rather than per pass because
    // a flush cuts its snapshot into chunks. Only the flush path observes them;
    // a merge rewrite runs the same code and is deliberately excluded.
    let build = &crate::part::FLUSH_BUILD;
    out.push_str(&build.sort.render("loggytracy_flush_build_sort_ms"));
    out.push_str(&build.parse.render("loggytracy_flush_build_parse_ms"));
    out.push_str(&build.write.render("loggytracy_flush_build_write_ms"));
    out.push_str(&build.parquet.render("loggytracy_flush_build_parquet_ms"));
    out.push_str(&build.index.render("loggytracy_flush_build_index_ms"));
    out.push_str(&build.meta.render("loggytracy_flush_build_meta_ms"));
    out.push_str(&build.commit.render("loggytracy_flush_build_commit_ms"));
    out
}

/// Deletion is the one operation here that destroys data on request, so how
/// many were accepted, how many were refused, and how many rows are being
/// hidden are all things an operator has to be able to see without asking a
/// tenant.
fn delete_request_metrics(state: &AppState) -> String {
    let metrics = &state.delete_requests.metrics;
    format!(
        "# TYPE loggytracy_delete_requests_accepted_total counter\n\
loggytracy_delete_requests_accepted_total {}\n\
# HELP loggytracy_delete_requests_rejected_total Submissions refused for exceeding the per-tenant limit. Each outstanding request is a predicate every scan for that tenant evaluates per row.\n\
# TYPE loggytracy_delete_requests_rejected_total counter\n\
loggytracy_delete_requests_rejected_total {}\n\
# TYPE loggytracy_delete_requests_cancelled_total counter\n\
loggytracy_delete_requests_cancelled_total {}\n\
# HELP loggytracy_delete_hidden_rows_total Rows a scan dropped because a deletion request covered them. Stops growing for a request once a rewrite has removed its bytes.\n\
# TYPE loggytracy_delete_hidden_rows_total counter\n\
loggytracy_delete_hidden_rows_total {}\n",
        metrics.accepted.load(Ordering::Relaxed),
        metrics.rejected.load(Ordering::Relaxed),
        metrics.cancelled.load(Ordering::Relaxed),
        metrics.hidden_rows.load(Ordering::Relaxed),
    )
}

/// The two numbers that decide the sign of "add Parquet range reads": what a
/// selective download would cost in requests, and what the whole-object
/// download earns by leaving a reusable copy behind. See
/// [`crate::restore_meter`] for why those two and not the byte total.
fn restore_economics_metrics() -> String {
    let meter = crate::restore_meter::global().snapshot();
    format!(
        "# HELP loggytracy_query_part_scans_total Query scans that read a part body. A rewrite is excluded: it reads what it was told to.\n\
# TYPE loggytracy_query_part_scans_total counter\n\
loggytracy_query_part_scans_total {}\n\
# HELP loggytracy_query_row_groups_total Row groups in the parts those scans read, by how far selection narrowed them. `present` is the whole part a restore downloads, `tenant` is the querying tenant's segment, `selected` is what the scan read.\n\
# TYPE loggytracy_query_row_groups_total counter\n\
loggytracy_query_row_groups_total{{stage=\"present\"}} {}\n\
loggytracy_query_row_groups_total{{stage=\"tenant\"}} {}\n\
loggytracy_query_row_groups_total{{stage=\"selected\"}} {}\n\
# HELP loggytracy_query_selected_runs_total Contiguous runs among the selected row groups. Column chunks of a row group are contiguous and the log path projects every column, so a run is one byte range: this plus one footer read is what a selective download would issue where a whole restore issues one GET.\n\
# TYPE loggytracy_query_selected_runs_total counter\n\
loggytracy_query_selected_runs_total {}\n\
# HELP loggytracy_restore_first_scan_total The same three numbers over the first scan of each restored body alone. That scan is the query the download was issued for, so its selection is the one a selective download would have applied; the aggregates above mix it with scans of bodies that were never downloaded.\n\
# TYPE loggytracy_restore_first_scan_total counter\n\
loggytracy_restore_first_scan_total{{stage=\"parts\"}} {}\n\
loggytracy_restore_first_scan_total{{stage=\"present\"}} {}\n\
loggytracy_restore_first_scan_total{{stage=\"selected\"}} {}\n\
loggytracy_restore_first_scan_total{{stage=\"runs\"}} {}\n\
# HELP loggytracy_restored_body_scans_total Query scans served by a body that was downloaded whole after eviction and is still on disk. Divided by the restore count, this is how much later work one over-fetch prepaid.\n\
# TYPE loggytracy_restored_body_scans_total counter\n\
loggytracy_restored_body_scans_total {}\n\
# HELP loggytracy_restored_bodies_total Bodies restored, and how many of them eviction has since taken. A restore still resident has not finished earning.\n\
# TYPE loggytracy_restored_bodies_total counter\n\
loggytracy_restored_bodies_total{{state=\"restored\"}} {}\n\
loggytracy_restored_bodies_total{{state=\"retired\"}} {}\n\
# HELP loggytracy_restored_tenant_slices_total Distinct (restored body, querying tenant) pairs. A whole restore costs one GET however many tenants read it; a selective download serves one slice, so this is how many it would have taken.\n\
# TYPE loggytracy_restored_tenant_slices_total counter\n\
loggytracy_restored_tenant_slices_total {}\n",
        meter.part_scans,
        meter.row_groups_present,
        meter.row_groups_tenant,
        meter.row_groups_selected,
        meter.selected_runs,
        meter.first_scan_parts,
        meter.first_scan_row_groups_present,
        meter.first_scan_row_groups_selected,
        meter.first_scan_runs,
        meter.restored_scans,
        meter.restores,
        meter.restored_retired,
        meter.restored_tenant_slices,
    )
}

/// The cost model of this design is operation counts, not bytes. R2 bills per
/// request, and the whole shared-part layout exists because per-tenant objects
/// multiplied that count. These are the numbers to divide by flush, merge and
/// retention cycles to get the per-cycle cost, and they measure the same
/// locally as they do against a paid backend.
fn object_store_operation_metrics(state: &AppState) -> String {
    let Some(counts) = state
        .remote_cache
        .as_ref()
        .map(|cache| cache.storage.operation_counts())
    else {
        return String::new();
    };
    format!(
        "# HELP loggytracy_object_store_operations_total Object-store requests issued, by kind. Which kinds are billed how is the backend's policy; how many of each this engine issues is not.\n\
# TYPE loggytracy_object_store_operations_total counter\n\
loggytracy_object_store_operations_total{{kind=\"put\"}} {}\n\
loggytracy_object_store_operations_total{{kind=\"put_multipart\"}} {}\n\
loggytracy_object_store_operations_total{{kind=\"get\"}} {}\n\
loggytracy_object_store_operations_total{{kind=\"delete\"}} {}\n\
loggytracy_object_store_operations_total{{kind=\"list\"}} {}\n\
loggytracy_object_store_operations_total{{kind=\"copy\"}} {}\n\
# HELP loggytracy_object_store_listed_objects_total Objects the listings returned. A backend pages a listing, so its request count follows from this and the page size rather than from the list count.\n\
# TYPE loggytracy_object_store_listed_objects_total counter\n\
loggytracy_object_store_listed_objects_total {}\n\
# HELP loggytracy_object_store_ranged_gets_total GETs that asked for a byte range rather than a whole object. Zero means every restore moves the whole part, including the rows belonging to other tenants of a shared part.\n\
# TYPE loggytracy_object_store_ranged_gets_total counter\n\
loggytracy_object_store_ranged_gets_total {}\n\
# HELP loggytracy_object_store_bytes_total Bytes moved to and from the object store. Read bytes are what the responses agreed to return, not what a caller consumed.\n\
# TYPE loggytracy_object_store_bytes_total counter\n\
loggytracy_object_store_bytes_total{{direction=\"get\"}} {}\n\
loggytracy_object_store_bytes_total{{direction=\"put\"}} {}\n\
# HELP loggytracy_object_store_bytes_by_kind_total The same bytes split by what was read or written. A part restore and a manifest rewrite are both bytes and only one of them is a part; the totals above cannot tell them apart.\n\
# TYPE loggytracy_object_store_bytes_by_kind_total counter\n\
loggytracy_object_store_bytes_by_kind_total{{direction=\"get\",kind=\"manifest\"}} {}\n\
loggytracy_object_store_bytes_by_kind_total{{direction=\"get\",kind=\"part\"}} {}\n\
loggytracy_object_store_bytes_by_kind_total{{direction=\"get\",kind=\"trace_part\"}} {}\n\
loggytracy_object_store_bytes_by_kind_total{{direction=\"get\",kind=\"other\"}} {}\n\
loggytracy_object_store_bytes_by_kind_total{{direction=\"put\",kind=\"manifest\"}} {}\n\
loggytracy_object_store_bytes_by_kind_total{{direction=\"put\",kind=\"part\"}} {}\n\
loggytracy_object_store_bytes_by_kind_total{{direction=\"put\",kind=\"trace_part\"}} {}\n\
loggytracy_object_store_bytes_by_kind_total{{direction=\"put\",kind=\"other\"}} {}\n",
        counts.puts,
        counts.multipart_puts,
        counts.gets,
        counts.deletes,
        counts.lists,
        counts.copies,
        counts.listed_objects,
        counts.ranged_gets,
        counts.get_bytes,
        counts.put_bytes,
        counts.get_bytes_by_kind.manifest,
        counts.get_bytes_by_kind.part,
        counts.get_bytes_by_kind.trace_part,
        counts.get_bytes_by_kind.other,
        counts.put_bytes_by_kind.manifest,
        counts.put_bytes_by_kind.part,
        counts.put_bytes_by_kind.trace_part,
        counts.put_bytes_by_kind.other,
    )
}

/// The revision this binary was built from, or `unknown` when the build did
/// not supply one. Without it a scraped series cannot be attributed to code,
/// which is the first question asked when two deployments behave differently.
pub fn build_revision() -> &'static str {
    option_env!("LOGGYTRACY_BUILD_REVISION").unwrap_or("unknown")
}

fn m_merge_debt(state: &AppState) -> u64 {
    state
        .metrics
        .merge_debt_parts
        .load(std::sync::atomic::Ordering::Relaxed)
}

struct TenantPolicyGauges {
    known_tenants: usize,
    infinite_tenants: usize,
    unknown_tenants: u64,
    last_push_age_seconds: u64,
}

/// The policy map is small and in memory, so its two counts are computed here.
/// The unknown-tenant count is not: it walks every part's tenant index, so the
/// retention worker publishes it and this reads what that worker last saw.
fn tenant_policy_gauges(state: &AppState) -> TenantPolicyGauges {
    let Some(snapshot) = state.tenant_policy.snapshot() else {
        return TenantPolicyGauges {
            known_tenants: 0,
            infinite_tenants: 0,
            unknown_tenants: 0,
            last_push_age_seconds: 0,
        };
    };
    TenantPolicyGauges {
        known_tenants: snapshot.tenant_count(),
        infinite_tenants: snapshot.infinite_tenant_count(),
        unknown_tenants: state
            .metrics
            .unknown_tenants
            .load(std::sync::atomic::Ordering::Relaxed),
        last_push_age_seconds: snapshot
            .newest_push_age(state.clock.now())
            .as_secs(),
    }
}

