//! The one place a log query meets its rows.
//!
//! Lifted out of `query/execution.rs` because nothing under `benches/` could
//! reach it there. The shape [`docs/COMPARISON.md`](../docs/COMPARISON.md)
//! measured this engine losing on — `| json | field=` with a small limit over a
//! window far larger than the limit — is a property of the *executor* rather
//! than of any single part scan, so it was the one hot path with no bench.
//! `benches/query.rs` measures this module.
//!
//! What it owns: the memtable and the parts are read through one funnel, in one
//! direction, with one limit, and the deletion mask is applied in one place.
//! `query/execution.rs` supplies the mask and the stats; everything else about
//! how a log query is answered is here.

use std::sync::atomic::{AtomicBool, Ordering};

use crate::logql::LogQuery;
use crate::memtable::{Labels, LogEntry, MemTable, SharedLabels, StreamResult};
use crate::part::{ExactFieldPruning, QueryTimeRange};
use crate::part_registry::PartRegistry;
use crate::tenant::TenantId;

/// Whether a row is hidden from this tenant by a delete request.
///
/// A predicate rather than the mask itself: the mask lives behind the delete
/// API and carries a metric this module has no business bumping, and the only
/// thing a scan needs to know is yes or no.
pub type HiddenRow<'a> = &'a dyn Fn(&Labels, &LogEntry) -> bool;

pub struct LogScanResult {
    pub results: Vec<StreamResult>,
    pub scanned_rows: u64,
    pub scanned_bytes: u64,
}

/// One log query, with every bound that applies to it.
///
/// A struct rather than a fifteen-argument function: every caller sets the
/// first five and a different subset of the rest, and the arguments were
/// already threaded through four wrappers that existed only to default them.
pub struct LogScan<'a> {
    tenant: &'a TenantId,
    query: &'a LogQuery,
    range: QueryTimeRange,
    limit: usize,
    forward: bool,
    /// Rows the scan may read before it must refuse, rather than rows it may
    /// return. Exceeding it is an error and not a truncation, because a
    /// silently short answer to a range query reads as an absence of logs.
    scan_budget: Option<usize>,
    max_scan_bytes: Option<u64>,
    max_memory_bytes: Option<u64>,
    cancellation: Option<&'a AtomicBool>,
    hidden: Option<HiddenRow<'a>>,
}

impl<'a> LogScan<'a> {
    pub fn new(
        tenant: &'a TenantId,
        query: &'a LogQuery,
        range: QueryTimeRange,
        limit: usize,
        forward: bool,
    ) -> Self {
        Self {
            tenant,
            query,
            range,
            limit,
            forward,
            scan_budget: None,
            max_scan_bytes: None,
            max_memory_bytes: None,
            cancellation: None,
            hidden: None,
        }
    }

    pub fn scan_budget(mut self, rows: Option<usize>) -> Self {
        self.scan_budget = rows;
        self
    }

    pub fn max_scan_bytes(mut self, bytes: Option<u64>) -> Self {
        self.max_scan_bytes = bytes;
        self
    }

    pub fn max_memory_bytes(mut self, bytes: Option<u64>) -> Self {
        self.max_memory_bytes = bytes;
        self
    }

    pub fn cancellation(mut self, cancellation: Option<&'a AtomicBool>) -> Self {
        self.cancellation = cancellation;
        self
    }

    pub fn hidden(mut self, hidden: HiddenRow<'a>) -> Self {
        self.hidden = Some(hidden);
        self
    }

    fn cancelled(&self) -> bool {
        self.cancellation
            .is_some_and(|flag| flag.load(Ordering::Acquire))
    }

    pub fn run(&self, memtable: &MemTable, parts: &PartRegistry) -> Result<LogScanResult, String> {
        let mut all: Vec<(SharedLabels, LogEntry)> = Vec::new();
        let mut scanned_rows = 0u64;
        let mut scanned_bytes = 0u64;
        let mut materialized_memory_bytes = 0u64;

        // Pipeline predicates run after storage scans. Do not let the API log
        // limit truncate raw rows before a json/logfmt/field stage has
        // evaluated.
        let normal_scan_limit = if self.query.stages.len() == self.query.line_filters.len() {
            self.limit
        } else {
            usize::MAX
        };
        let scan_limit = self.scan_budget.map(|budget| budget.saturating_add(1));

        let memtable_result = memtable.query_with_scan_limit(
            self.tenant,
            &self.query.matchers,
            &self.query.line_filters,
            self.range,
            normal_scan_limit,
            self.forward,
            scan_limit,
            self.cancellation,
        );
        scanned_rows = scanned_rows.saturating_add(memtable_result.scanned_rows as u64);
        for sr in memtable_result.results {
            for mut e in sr.entries {
                if self.cancelled() {
                    return Err("query timed out".to_string());
                }
                // Before the pipeline runs: a delete selector matches the line
                // as it was written, and `line_format` would have rewritten it.
                if self.hidden.is_some_and(|hidden| hidden(&sr.labels, &e)) {
                    continue;
                }
                if self.query.process_entry_with_labels_cancellable(
                    &sr.labels,
                    &mut e,
                    self.cancellation,
                )? {
                    materialized_memory_bytes = materialized_memory_bytes
                        .checked_add(estimated_log_entry_memory_bytes(&sr.labels, &e))
                        .ok_or_else(|| {
                            "query materialized memory accounting overflowed".to_string()
                        })?;
                    self.check_memory(materialized_memory_bytes)?;
                    all.push((sr.labels.clone(), e));
                }
            }
        }

        if self.cancelled() {
            return Err("query timed out".to_string());
        }
        self.check_scan_budget(scanned_rows)?;

        let exact_fields = self.query.exact_field_predicates();
        let part_scan_limit = scan_limit.map(|budget| budget.saturating_sub(scanned_rows as usize));
        let part_scan_bytes_limit = self
            .max_scan_bytes
            .map(|budget| budget.saturating_sub(scanned_bytes));
        let part_result = parts.query_with_exact_field_pruning_and_scan_limits(
            self.tenant,
            &self.query.matchers,
            ExactFieldPruning::new(&self.query.line_filters, &exact_fields),
            self.range,
            normal_scan_limit,
            self.forward,
            part_scan_limit,
            part_scan_bytes_limit,
            self.cancellation,
        )?;
        scanned_rows = scanned_rows.saturating_add(part_result.scanned_rows as u64);
        scanned_bytes = scanned_bytes.saturating_add(part_result.scanned_bytes);
        for sr in part_result.results {
            for mut e in sr.entries {
                if self.cancelled() {
                    return Err("query timed out".to_string());
                }
                if self.hidden.is_some_and(|hidden| hidden(&sr.labels, &e)) {
                    continue;
                }
                if self.query.process_entry_with_labels_cancellable(
                    &sr.labels,
                    &mut e,
                    self.cancellation,
                )? {
                    materialized_memory_bytes = materialized_memory_bytes
                        .checked_add(estimated_log_entry_memory_bytes(&sr.labels, &e))
                        .ok_or_else(|| {
                            "query materialized memory accounting overflowed".to_string()
                        })?;
                    self.check_memory(materialized_memory_bytes)?;
                    all.push((sr.labels.clone(), e));
                }
            }
        }

        if self.cancelled() {
            return Err("query timed out".to_string());
        }
        self.check_scan_budget(scanned_rows)?;
        if let Some(budget) = self.max_scan_bytes
            && scanned_bytes > budget
        {
            return Err(format!(
                "query exceeds the maximum of {budget} scanned bytes"
            ));
        }

        if self.forward {
            all.sort_by_key(|e| e.1.timestamp_ns);
        } else {
            all.sort_by_key(|e| std::cmp::Reverse(e.1.timestamp_ns));
        }
        all.truncate(self.limit);

        Ok(LogScanResult {
            results: crate::part::group_by_labels(all),
            scanned_rows,
            scanned_bytes,
        })
    }

    fn check_scan_budget(&self, scanned_rows: u64) -> Result<(), String> {
        if let Some(budget) = self.scan_budget
            && scanned_rows > budget as u64
        {
            return Err(format!(
                "query exceeds the maximum of {budget} scanned rows"
            ));
        }
        Ok(())
    }

    fn check_memory(&self, materialized_memory_bytes: u64) -> Result<(), String> {
        if let Some(max) = self.max_memory_bytes
            && materialized_memory_bytes > max
        {
            return Err(format!(
                "query exceeds the maximum of {max} materialized bytes"
            ));
        }
        Ok(())
    }
}

/// What one returned row is charged against `max_query_memory_bytes`.
///
/// Still charges every row for the label bytes it now shares through
/// `SharedLabels`, so it over-counts by the rows-per-stream factor. That makes
/// the ceiling stricter than the memory really held, which is the safe
/// direction; correcting it loosens a limit and belongs with the rest of the
/// metering (`todo.md`, M10 honest metering).
pub(crate) fn estimated_log_entry_memory_bytes(labels: &Labels, entry: &LogEntry) -> u64 {
    let labels = labels
        .iter()
        .map(|(name, value)| name.len().saturating_add(value.len()))
        .sum::<usize>();
    let metadata = entry
        .structured_metadata
        .iter()
        .map(|(name, value)| name.len().saturating_add(value.len()))
        .sum::<usize>();
    labels
        .saturating_add(metadata)
        .saturating_add(entry.line.len())
        .saturating_add(std::mem::size_of::<LogEntry>()) as u64
}
