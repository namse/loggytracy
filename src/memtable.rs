use std::collections::{BTreeMap, HashMap};
use std::sync::RwLock;

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

pub struct MemTable {
    inner: RwLock<HashMap<Labels, Vec<LogEntry>>>,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    pub fn insert(&self, labels: Labels, entries: Vec<LogEntry>) {
        let mut inner = self.inner.write().unwrap();
        let stream = inner.entry(labels).or_default();
        stream.extend(entries);
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
        let inner = self.inner.read().unwrap();

        let mut all_entries: Vec<(Labels, LogEntry)> = Vec::new();

        for (labels, entries) in inner.iter() {
            if !matchers.iter().all(|m| m.matches(labels)) {
                continue;
            }

            for e in entries {
                if e.timestamp_ns >= start_ns
                    && e.timestamp_ns < end_ns
                    && line_filters.iter().all(|f| f.matches(&e.line))
                {
                    all_entries.push((labels.clone(), e.clone()));
                }
            }
        }

        if forward {
            all_entries.sort_by_key(|e| e.1.timestamp_ns);
        } else {
            all_entries.sort_by_key(|e| std::cmp::Reverse(e.1.timestamp_ns));
        }

        all_entries.truncate(limit);

        let mut results: Vec<StreamResult> = Vec::new();
        for (labels, entry) in all_entries {
            if let Some(result) = results.iter_mut().find(|r| r.labels == labels) {
                result.entries.push(entry);
            } else {
                results.push(StreamResult {
                    labels,
                    entries: vec![entry],
                });
            }
        }

        results
    }

    pub fn label_names(&self) -> Vec<String> {
        let inner = self.inner.read().unwrap();
        let mut names = std::collections::BTreeSet::new();
        for labels in inner.keys() {
            for k in labels.keys() {
                names.insert(k.clone());
            }
        }
        names.into_iter().collect()
    }

    pub fn label_values(&self, name: &str) -> Vec<String> {
        let inner = self.inner.read().unwrap();
        let mut values = std::collections::BTreeSet::new();
        for labels in inner.keys() {
            if let Some(v) = labels.get(name) {
                values.insert(v.clone());
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
        result.sort();
        result
    }

    pub fn stats(&self) -> IndexStats {
        let inner = self.inner.read().unwrap();
        let mut entries = 0usize;
        let mut bytes = 0u64;
        for stream in inner.values() {
            entries += stream.len();
            for e in stream {
                bytes += e.line.len() as u64;
            }
        }
        IndexStats {
            streams: inner.len(),
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
