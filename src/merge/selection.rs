/// Number of parts currently eligible for but not yet merged: the point-in-time
/// merge backlog. Runs the scheduler's own `select_groups`, so the count covers
/// both kinds of pending work — small parts accumulating into an ordinary group,
/// and parts a retention rewrite has made eligible on their own. Counting only
/// the first left retention-driven backlog invisible to operators, with
/// `retention_rewrite_skipped` reporting only the rewrites that had already
/// failed.
pub fn merge_debt_part_count(
    registry: &PartRegistry,
    config: &Config,
    cutoffs: Option<&Cutoffs>,
) -> usize {
    let readers = registry.snapshot();
    if readers.is_empty() {
        return 0;
    }
    let mut by_partition: HashMap<String, Vec<Arc<PartReader>>> = HashMap::new();
    for reader in readers {
        by_partition
            .entry(reader.meta().partition.clone())
            .or_default()
            .push(reader);
    }
    let mut debt = 0usize;
    for (_partition, mut parts) in by_partition {
        parts.sort_by_key(|reader| reader.meta().row_count);
        for group in select_groups(&parts, config, cutoffs) {
            debt += group.parts.len();
        }
    }
    debt
}

/// One group of merge inputs and the reason it was selected.
struct MergeGroup {
    parts: Vec<Arc<PartReader>>,
    /// The group is below `merge_min_part_count`, so nothing but retention
    /// would have picked it. Its only product is reclaimed bytes, which makes
    /// a failure to process it a missed optimization rather than a merge that
    /// did not happen.
    retention_only: bool,
}

/// Merge groups for one partition, including the ones retention needs.
///
/// An ordinary group must reach `merge_min_part_count` to be worth the write.
/// A part whose expired share reaches `retention_rewrite_threshold` is worth
/// it on its own, so it is admitted as a group of one — even when it is too
/// large for `group_for_merge` to consider at all.
///
/// A tenant at zero retention is a deleted tenant, and its rows are reclaimed
/// regardless of the threshold: otherwise "deletion" would leave rows in a
/// large part indefinitely, which is not a thing the word can mean.
fn select_groups(
    parts: &[Arc<PartReader>],
    config: &Config,
    cutoffs: Option<&Cutoffs>,
) -> Vec<MergeGroup> {
    let min_part_count = config.merge_min_part_count.max(2);
    let needs_rewrite = |reader: &Arc<PartReader>| {
        cutoffs.is_some_and(|cutoffs| {
            cutoffs.holds_zero_retention_rows(reader.meta())
                || cutoffs.expired_log_row_fraction(reader.meta())
                    >= config.retention_rewrite_threshold
        })
    };

    let mut selected: Vec<MergeGroup> = Vec::new();
    let mut grouped: std::collections::HashSet<String> = std::collections::HashSet::new();
    for group in group_for_merge(parts, config) {
        if group.len() < min_part_count && !group.iter().any(needs_rewrite) {
            continue;
        }
        for reader in &group {
            grouped.insert(reader.meta().id.clone());
        }
        selected.push(MergeGroup {
            retention_only: group.len() < min_part_count,
            parts: group,
        });
    }
    for reader in parts {
        if needs_rewrite(reader) && !grouped.contains(&reader.meta().id) {
            selected.push(MergeGroup {
                parts: vec![reader.clone()],
                retention_only: true,
            });
        }
    }
    selected
}

fn group_for_merge(parts: &[Arc<PartReader>], config: &Config) -> Vec<Vec<Arc<PartReader>>> {
    let mut groups: Vec<Vec<Arc<PartReader>>> = Vec::new();
    let mut current: Vec<Arc<PartReader>> = Vec::new();
    let mut current_rows: u64 = 0;
    let mut current_bytes: u64 = 0;
    // A merge must always make progress. Treat the target row count as a soft
    // limit until enough parts have accumulated; otherwise, for example, four
    // 300k-row parts with a 1M target are split into groups of 3 and 1 and are
    // skipped forever by merge_min_part_count=4.
    let min_part_count = config.merge_min_part_count.max(2);
    for r in parts {
        if r.meta().row_count >= config.merge_max_part_rows {
            // 이미 큰 part는 merge 그룹에 넣지 않음
            continue;
        }

        let next_rows = current_rows.saturating_add(r.meta().row_count);
        let next_bytes = current_bytes.saturating_add(estimated_part_bytes(r));
        if current.len() >= min_part_count && next_bytes > config.merge_max_input_bytes {
            groups.push(std::mem::take(&mut current));
            current_rows = 0;
            current_bytes = 0;
        }
        if !current.is_empty() && next_rows > config.merge_max_part_rows {
            // The target is soft, but the maximum is a hard output bound.
            // Finalize the current group before adding a part that would make
            // the replacement oversized. merge_once will skip it if it does
            // not contain enough inputs to make progress.
            groups.push(std::mem::take(&mut current));
            current_rows = 0;
            current_bytes = 0;
        }

        current_rows = current_rows.saturating_add(r.meta().row_count);
        current_bytes = current_bytes.saturating_add(estimated_part_bytes(r));
        current.push(r.clone());
        if current.len() >= min_part_count && current_rows >= config.merge_target_part_rows {
            groups.push(std::mem::take(&mut current));
            current_rows = 0;
            current_bytes = 0;
        }
    }
    if !current.is_empty() {
        groups.push(current);
    }
    groups
}

/// What reading this part will materialize, straight from `meta.json`.
///
/// Replaces a `stat` of the compressed file: that number was both the wrong
/// unit for the budgets it was compared against and a syscall per part on
/// every `/metrics` scrape.
fn estimated_part_bytes(reader: &PartReader) -> u64 {
    reader.meta().materialized_bytes
}

#[cfg(test)]
fn read_all_rows(readers: &[Arc<PartReader>]) -> Result<Vec<part::Row>, String> {
    read_all_rows_with_limit(readers, u64::MAX)
}

/// What one group's rewrite produced.
pub struct GroupRewrite {
    pub new_parts: Vec<part::Part>,
    pub dropped_rows: usize,
    pub kept_rows: usize,
}

/// Read a group, drop what has expired, and write the survivors.
///
/// Reading and writing are interleaved rather than staged, because the whole
/// point of splitting an oversized group is to keep peak memory bounded:
/// holding every batch until the end would cost exactly what materializing the
/// group at once costs.
///
/// However many output parts this produces, they all carry the same merge
/// tombstone naming `old_dirs`, so the commit that follows still replaces the
/// inputs in one transaction.
pub fn rewrite_group(
    readers: &[Arc<PartReader>],
    cutoffs: Option<&Cutoffs>,
    parts_root: &Path,
    row_group_size: usize,
    max_memory_bytes: u64,
    old_dirs: &[PathBuf],
) -> Result<GroupRewrite, String> {
    let mut rewrite = GroupRewrite {
        new_parts: Vec::new(),
        dropped_rows: 0,
        kept_rows: 0,
    };
    let result = read_in_batches(readers, max_memory_bytes, &mut |mut rows| {
        let before = rows.len();
        if let Some(cutoffs) = cutoffs {
            rows.retain(|row| !cutoffs.is_expired(&row.tenant, row.timestamp_ns));
        }
        rewrite.dropped_rows += before - rows.len();
        if rows.is_empty() {
            return Ok(());
        }
        rewrite.kept_rows += rows.len();
        let written =
            part::flush_rows_with_merge_tombstone(rows, parts_root, row_group_size, old_dirs)
                .map_err(|error| error.to_string())?;
        rewrite.new_parts.extend(written);
        Ok(())
    });
    if let Err(error) = result {
        // A later batch failed after earlier ones were already on disk. They
        // are unreferenced by any manifest, so removing them here keeps the
        // failure from leaving parts that only tombstone recovery would clean.
        let written: Vec<PathBuf> = rewrite.new_parts.iter().map(|new| new.dir.clone()).collect();
        if let Err(cleanup_error) = part::remove_part_dirs(&written) {
            tracing::warn!(%cleanup_error, "failed to remove partial merge output");
        }
        return Err(error);
    }
    Ok(rewrite)
}

/// Hand the group's rows to `sink` in pieces that each fit the budget.
///
/// A group of several parts halves until its pieces fit; a single part that
/// still does not fit is read a row-group window at a time. Without this a
/// part larger than `merge_max_memory_bytes` could never be rewritten, and for
/// a tenant at zero retention that means its rows are never actually deleted —
/// only hidden by the query clamp, which is not what deletion means.
fn read_in_batches(
    readers: &[Arc<PartReader>],
    max_memory_bytes: u64,
    sink: &mut impl FnMut(Vec<part::Row>) -> Result<(), String>,
) -> Result<(), String> {
    let error = match read_all_rows_with_limit(readers, max_memory_bytes) {
        Ok(rows) => return sink(rows),
        Err(error) => error,
    };
    if readers.len() > 1 {
        let (left, right) = readers.split_at(readers.len() / 2);
        read_in_batches(left, max_memory_bytes, sink)?;
        return read_in_batches(right, max_memory_bytes, sink);
    }
    let reader = &readers[0];
    if reader.row_group_count() <= 1 {
        // One row group is the part's indivisible unit. `row_group_size` caps
        // how many rows it holds, so reaching here means a single row group
        // exceeds the whole merge budget — a configuration problem the split
        // cannot solve, and the error says so rather than looping.
        return Err(error);
    }
    for window in row_group_windows(reader, max_memory_bytes) {
        sink(reader.read_rows_in_row_groups(window, Some(max_memory_bytes))?)?;
    }
    Ok(())
}

/// Row-group windows sized so each one is expected to fit the memory budget.
///
/// Sized from the part's own average row width rather than a fixed count: a
/// part of wide rows needs smaller windows than one of narrow rows for the same
/// budget. Always at least one row group, so the walk terminates.
fn row_group_windows(reader: &PartReader, max_memory_bytes: u64) -> Vec<std::ops::Range<u32>> {
    let row_group_count = reader.row_group_count();
    let meta = reader.meta();
    let rows_per_group = (meta.row_count / row_group_count.max(1) as u64).max(1);
    let bytes_per_row = meta
        .materialized_bytes
        .checked_div(meta.row_count.max(1))
        .unwrap_or(0)
        .max(1);
    let bytes_per_group = rows_per_group.saturating_mul(bytes_per_row).max(1);
    let groups_per_window = (max_memory_bytes / bytes_per_group).clamp(1, row_group_count as u64);

    let mut windows = Vec::new();
    let mut start = 0u32;
    while start < row_group_count {
        let end = start
            .saturating_add(groups_per_window as u32)
            .min(row_group_count);
        windows.push(start..end);
        start = end;
    }
    windows
}

fn read_all_rows_with_limit(
    readers: &[Arc<PartReader>],
    max_memory_bytes: u64,
) -> Result<Vec<part::Row>, String> {
    let mut rows: Vec<part::Row> = Vec::new();
    let mut estimated_memory = 0u64;
    for reader in readers {
        let remaining_memory = max_memory_bytes.saturating_sub(estimated_memory);
        // Merge rewrites the whole shared part, so it reads every tenant's
        // rows. `read_all_rows` walks the tenant index rather than bypassing
        // it, so each row still arrives tagged with its own tenant.
        for row in reader.read_all_rows(Some(remaining_memory))? {
            let row_memory = row.materialized_bytes();
            estimated_memory = estimated_memory
                .checked_add(row_memory)
                .ok_or_else(|| "merge memory accounting overflowed".to_string())?;
            if estimated_memory > max_memory_bytes {
                return Err(format!(
                    "merge exceeds the maximum of {max_memory_bytes} materialized bytes"
                ));
            }
            rows.push(row);
        }
    }
    Ok(rows)
}
