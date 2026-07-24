use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::logql::{LabelMatcher, LineFilter};

pub type Labels = BTreeMap<String, String>;

#[derive(Clone)]
pub struct LogEntry {
    pub timestamp_ns: i64,
    pub line: String,
    pub structured_metadata: Vec<(String, String)>,
}

pub struct StreamResult {
    pub labels: Labels,
    pub entries: Vec<LogEntry>,
}

pub struct QueryResult {
    pub results: Vec<StreamResult>,
    pub scanned_rows: usize,
}

pub struct MemTable {
    inner: RwLock<HashMap<Labels, Vec<LogEntry>>>,
    flushing: RwLock<Option<HashMap<Labels, Vec<LogEntry>>>>,
}

#[allow(clippy::too_many_arguments)]
fn scan_memtable_stream(
    labels: &Labels,
    entries: &[LogEntry],
    line_filters: &[LineFilter],
    start_ns: i64,
    end_ns: i64,
    limit: usize,
    forward: bool,
    scan_limit: Option<usize>,
    cancellation: Option<&AtomicBool>,
    scanned_rows: &mut usize,
    grouped: &mut BTreeMap<Labels, Vec<LogEntry>>,
    scan_stopped: &mut bool,
) {
    let mut ordered: Vec<&LogEntry> = entries.iter().collect();
    ordered.sort_unstable_by_key(|entry| entry.timestamp_ns);
    if !forward {
        ordered.reverse();
    }
    let mut matched = 0usize;
    for entry in ordered {
        if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
            *scan_stopped = true;
            break;
        }
        *scanned_rows = scanned_rows.saturating_add(1);
        if entry.timestamp_ns >= start_ns
            && entry.timestamp_ns <= end_ns
            && line_filters
                .iter()
                .all(|filter| filter.matches(&entry.line))
        {
            grouped
                .entry(labels.clone())
                .or_default()
                .push(entry.clone());
            matched += 1;
            // Each stream contributes at most `limit` rows to the global top
            // or bottom `limit`, so older rows from this stream cannot affect
            // the final result once its own candidate set is full.
            if !forward && matched >= limit {
                break;
            }
        }
        if scan_limit.is_some_and(|limit| *scanned_rows >= limit) {
            *scan_stopped = true;
            break;
        }
    }
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            flushing: RwLock::new(None),
        }
    }

    pub fn insert(&self, labels: Labels, entries: Vec<LogEntry>) {
        let mut inner = self.inner.write().unwrap();
        let stream = inner.entry(labels).or_default();
        stream.extend(entries);
    }

    pub fn begin_flush(&self) -> HashMap<Labels, Vec<LogEntry>> {
        let mut inner = self.inner.write().unwrap();
        let mut flushing = self.flushing.write().unwrap();
        let mut snapshot = std::mem::take(&mut *inner);
        if let Some(previous_snapshot) = flushing.take() {
            for (labels, entries) in previous_snapshot {
                snapshot.entry(labels).or_default().extend(entries);
            }
        }
        *flushing = Some(snapshot.clone());
        snapshot
    }

    pub fn commit_flush(&self) {
        let mut flushing = self.flushing.write().unwrap();
        *flushing = None;
    }

    pub fn abort_flush(&self, snapshot: HashMap<Labels, Vec<LogEntry>>) {
        let mut inner = self.inner.write().unwrap();
        for (labels, entries) in snapshot {
            let stream = inner.entry(labels).or_default();
            stream.extend(entries);
        }
        let mut flushing = self.flushing.write().unwrap();
        *flushing = None;
    }

    pub fn is_empty(&self) -> bool {
        let inner = self.inner.read().unwrap();
        if !inner.is_empty() {
            return false;
        }
        let flushing = self.flushing.read().unwrap();
        flushing.as_ref().map(|m| m.is_empty()).unwrap_or(true)
    }

    pub fn approximate_size(&self) -> usize {
        let inner = self.inner.read().unwrap();
        let mut bytes = 0usize;
        for (labels, entries) in inner.iter() {
            for (k, v) in labels {
                bytes += k.len() + v.len();
            }
            for e in entries {
                bytes += e.line.len();
                for (k, v) in &e.structured_metadata {
                    bytes += k.len() + v.len();
                }
            }
        }
        // Keep the lock order aligned with begin_flush/abort_flush. The
        // size is therefore computed from one consistent pair of buffers.
        let flushing = self.flushing.read().unwrap();
        if let Some(f) = flushing.as_ref() {
            for (labels, entries) in f {
                for (k, v) in labels {
                    bytes += k.len() + v.len();
                }
                for e in entries {
                    bytes += e.line.len();
                    for (k, v) in &e.structured_metadata {
                        bytes += k.len() + v.len();
                    }
                }
            }
        }
        drop(flushing);
        drop(inner);
        bytes
    }

    pub fn query(
        &self,
        matchers: &[LabelMatcher],
        line_filters: &[LineFilter],
        start_ns: i64,
        end_ns: i64,
        limit: usize,
        forward: bool,
    ) -> Vec<StreamResult> {
        self.query_with_scan_limit(
            matchers,
            line_filters,
            start_ns,
            end_ns,
            limit,
            forward,
            None,
            None,
        )
        .results
    }

    #[allow(clippy::too_many_arguments)]
    pub fn query_with_scan_limit(
        &self,
        matchers: &[LabelMatcher],
        line_filters: &[LineFilter],
        start_ns: i64,
        end_ns: i64,
        limit: usize,
        forward: bool,
        scan_limit: Option<usize>,
        cancellation: Option<&AtomicBool>,
    ) -> QueryResult {
        if limit == 0 {
            return QueryResult {
                results: Vec::new(),
                scanned_rows: 0,
            };
        }
        let mut grouped: BTreeMap<Labels, Vec<LogEntry>> = BTreeMap::new();
        let mut scanned_rows = 0usize;
        let mut scan_stopped = false;

        // begin_flush and abort_flush acquire inner before flushing. Holding
        // both read guards in that order prevents observing the same entry in
        // both buffers if a flush starts between the two reads.
        let inner = self.inner.read().unwrap();
        for (labels, entries) in inner.iter() {
            if !matchers.iter().all(|m| m.matches(labels)) {
                continue;
            }
            scan_memtable_stream(
                labels,
                entries,
                line_filters,
                start_ns,
                end_ns,
                limit,
                forward,
                scan_limit,
                cancellation,
                &mut scanned_rows,
                &mut grouped,
                &mut scan_stopped,
            );
            if scan_stopped {
                break;
            }
        }
        let flushing = self.flushing.read().unwrap();
        if !scan_stopped && let Some(f) = flushing.as_ref() {
            for (labels, entries) in f {
                if !matchers.iter().all(|m| m.matches(labels)) {
                    continue;
                }
                scan_memtable_stream(
                    labels,
                    entries,
                    line_filters,
                    start_ns,
                    end_ns,
                    limit,
                    forward,
                    scan_limit,
                    cancellation,
                    &mut scanned_rows,
                    &mut grouped,
                    &mut scan_stopped,
                );
                if scan_stopped {
                    break;
                }
            }
        }
        drop(flushing);
        drop(inner);

        let mut all_entries: Vec<(Labels, LogEntry)> = grouped
            .into_iter()
            .flat_map(|(labels, entries)| {
                entries
                    .into_iter()
                    .map(move |entry| (labels.clone(), entry))
            })
            .collect();
        if forward {
            all_entries.sort_by_key(|e| e.1.timestamp_ns);
        } else {
            all_entries.sort_by_key(|e| std::cmp::Reverse(e.1.timestamp_ns));
        }
        all_entries.truncate(limit);
        let mut result_groups: BTreeMap<Labels, Vec<LogEntry>> = BTreeMap::new();
        for (labels, entry) in all_entries {
            result_groups.entry(labels).or_default().push(entry);
        }
        let results = result_groups
            .into_iter()
            .map(|(labels, entries)| StreamResult { labels, entries })
            .collect();

        QueryResult {
            results,
            scanned_rows,
        }
    }

    pub fn label_names(&self) -> Vec<String> {
        let inner = self.inner.read().unwrap();
        let mut names = BTreeSet::new();
        for labels in inner.keys() {
            for k in labels.keys() {
                names.insert(k.clone());
            }
        }
        drop(inner);
        let flushing = self.flushing.read().unwrap();
        if let Some(f) = flushing.as_ref() {
            for labels in f.keys() {
                for k in labels.keys() {
                    names.insert(k.clone());
                }
            }
        }
        names.into_iter().collect()
    }

    pub fn label_values(&self, name: &str) -> Vec<String> {
        let inner = self.inner.read().unwrap();
        let mut values = BTreeSet::new();
        for labels in inner.keys() {
            if let Some(v) = labels.get(name) {
                values.insert(v.clone());
            }
        }
        drop(inner);
        let flushing = self.flushing.read().unwrap();
        if let Some(f) = flushing.as_ref() {
            for labels in f.keys() {
                if let Some(v) = labels.get(name) {
                    values.insert(v.clone());
                }
            }
        }
        values.into_iter().collect()
    }

    pub fn series(&self, matchers: &[LabelMatcher]) -> Vec<Labels> {
        let inner = self.inner.read().unwrap();
        let mut result: Vec<Labels> = inner
            .keys()
            .filter(|labels| matchers.iter().all(|m| m.matches(labels)))
            .cloned()
            .collect();
        drop(inner);
        let flushing = self.flushing.read().unwrap();
        if let Some(f) = flushing.as_ref() {
            for labels in f.keys() {
                if matchers.iter().all(|m| m.matches(labels)) {
                    result.push(labels.clone());
                }
            }
        }
        result.sort();
        result.dedup();
        result
    }

    pub fn stats(&self) -> IndexStats {
        let inner = self.inner.read().unwrap();
        let mut stream_set: BTreeSet<Labels> = BTreeSet::new();
        let mut entries = 0usize;
        let mut bytes = 0u64;
        for (labels, stream) in inner.iter() {
            stream_set.insert(labels.clone());
            entries += stream.len();
            for e in stream {
                bytes += e.line.len() as u64;
            }
        }
        let flushing = self.flushing.read().unwrap();
        if let Some(f) = flushing.as_ref() {
            for (labels, stream) in f.iter() {
                stream_set.insert(labels.clone());
                entries += stream.len();
                for e in stream {
                    bytes += e.line.len() as u64;
                }
            }
        }
        drop(flushing);
        drop(inner);
        IndexStats {
            streams: stream_set.len(),
            entries,
            bytes,
        }
    }
}

pub struct IndexStats {
    pub streams: usize,
    pub entries: usize,
    pub bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_entry(line: &str, ts_ns: i64) -> LogEntry {
        LogEntry {
            timestamp_ns: ts_ns,
            line: line.to_string(),
            structured_metadata: vec![],
        }
    }

    fn sample_labels() -> Labels {
        std::iter::once(("app".to_string(), "test".to_string())).collect()
    }

    #[test]
    fn flushing_buffer_visible_during_flush() {
        // begin_flush는 inner를 비우고 flushing 버퍼로 옮긴다.
        // unified_query는 flushing 버퍼까지 조회하므로 flush 진행 중에도
        // 데이터는 사라지지 않는다 (이슈 #2 복구).
        let mt = MemTable::new();
        mt.insert(sample_labels(), vec![sample_entry("hello", 100)]);

        let snapshot = mt.begin_flush();
        assert_eq!(snapshot.len(), 1);

        let results = mt.query(&[], &[], i64::MIN, i64::MAX, 100, true);
        let total: usize = results.iter().map(|s| s.entries.len()).sum();
        assert_eq!(
            total, 1,
            "flushing buffer should remain visible during flush"
        );

        mt.commit_flush();
        let results2 = mt.query(&[], &[], i64::MIN, i64::MAX, 100, true);
        let total2: usize = results2.iter().map(|s| s.entries.len()).sum();
        assert_eq!(
            total2, 0,
            "after commit_flush, no data should remain visible"
        );
    }

    #[test]
    fn abort_flush_restores_to_inner() {
        let mt = MemTable::new();
        mt.insert(sample_labels(), vec![sample_entry("hello", 100)]);

        let snapshot = mt.begin_flush();
        mt.abort_flush(snapshot);

        let results = mt.query(&[], &[], i64::MIN, i64::MAX, 100, true);
        let total: usize = results.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 1, "abort_flush should restore data to inner");
        assert!(!mt.is_empty());
    }

    #[test]
    fn begin_flush_keeps_query_consistent_with_concurrent_insert() {
        // flush 진행 중 새로 들어온 데이터도 보여야 한다.
        let mt = MemTable::new();
        mt.insert(sample_labels(), vec![sample_entry("first", 100)]);

        let _snapshot = mt.begin_flush();
        // flush 진행 중 새 데이터 수신
        mt.insert(sample_labels(), vec![sample_entry("second", 200)]);

        let results = mt.query(&[], &[], i64::MIN, i64::MAX, 100, true);
        let total: usize = results.iter().map(|s| s.entries.len()).sum();
        assert_eq!(
            total, 2,
            "both flushing buffer and inner should be visible concurrently"
        );
    }

    #[test]
    fn stats_does_not_count_same_stream_twice_during_flush() {
        let mt = MemTable::new();
        mt.insert(sample_labels(), vec![sample_entry("first", 100)]);
        let _snapshot = mt.begin_flush();
        mt.insert(sample_labels(), vec![sample_entry("second", 200)]);

        let stats = mt.stats();
        assert_eq!(stats.streams, 1);
        assert_eq!(stats.entries, 2);
    }

    #[test]
    fn begin_flush_preserves_previous_uncommitted_snapshot() {
        let memtable = MemTable::new();
        memtable.insert(sample_labels(), vec![sample_entry("first", 100)]);

        let _first_snapshot = memtable.begin_flush();
        memtable.insert(sample_labels(), vec![sample_entry("second", 200)]);

        let second_snapshot = memtable.begin_flush();
        let snapshot_entries: usize = second_snapshot.values().map(Vec::len).sum();
        assert_eq!(snapshot_entries, 2);

        memtable.abort_flush(second_snapshot);
        let results = memtable.query(&[], &[], i64::MIN, i64::MAX, 100, true);
        let total_entries: usize = results.iter().map(|stream| stream.entries.len()).sum();
        assert_eq!(total_entries, 2);
    }

    #[test]
    fn query_includes_the_end_timestamp() {
        let memtable = MemTable::new();
        memtable.insert(
            sample_labels(),
            vec![sample_entry("at the inclusive end", i64::MAX)],
        );

        let results = memtable.query(&[], &[], i64::MAX, i64::MAX, 100, true);
        let total_entries: usize = results.iter().map(|stream| stream.entries.len()).sum();
        assert_eq!(total_entries, 1);
    }
}
