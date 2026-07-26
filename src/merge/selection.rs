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

fn estimated_part_bytes(reader: &PartReader) -> u64 {
    std::fs::metadata(reader.part().data_path())
        .map(|metadata| metadata.len())
        .unwrap_or_else(|_| reader.meta().row_count.saturating_mul(128))
}

#[cfg(test)]
fn read_all_rows(readers: &[Arc<PartReader>]) -> Result<Vec<part::Row>, String> {
    read_all_rows_with_limit(readers, u64::MAX)
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
            let row_memory = row
                .labels
                .iter()
                .map(|(name, value)| name.len().saturating_add(value.len()))
                .sum::<usize>()
                .saturating_add(row.line.len())
                .saturating_add(
                    row.structured_metadata
                        .iter()
                        .map(|(name, value)| name.len().saturating_add(value.len()))
                        .sum::<usize>(),
                )
                .saturating_add(std::mem::size_of::<part::Row>()) as u64;
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
