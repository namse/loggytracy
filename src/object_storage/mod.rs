use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutMode, PutOptions, UpdateVersion};
use serde::{Deserialize, Serialize};

use crate::part::{self, BLOOM_FILE, DATA_FILE, META_FILE, Part, STREAM_INDEX_FILE};
use crate::trace_part::{
    TRACE_BLOOM_FILE, TRACE_DATA_FILE, TRACE_META_FILE, TracePart, TracePartReader,
    discover_trace_parts,
};

const MANIFEST_FILE: &str = "manifest.json";
const UPLOAD_MARKER_FILE: &str = ".object-store-uploading";
const PART_FILES: [&str; 4] = [DATA_FILE, BLOOM_FILE, STREAM_INDEX_FILE, META_FILE];
const CATALOG_FILES: [&str; 3] = [BLOOM_FILE, STREAM_INDEX_FILE, META_FILE];
const MANIFEST_FORMAT_VERSION: u32 = 1;
const TRACE_MANIFEST_FILE: &str = "trace-manifest.json";
const TRACE_PART_FILES: [&str; 3] = [TRACE_DATA_FILE, TRACE_BLOOM_FILE, TRACE_META_FILE];
const TRACE_CATALOG_FILES: [&str; 2] = [TRACE_BLOOM_FILE, TRACE_META_FILE];
const MAX_CAS_ATTEMPTS: usize = 16;
const FLUSH_TRANSACTION_FILE: &str = "flush.txn";

fn normalized_object_store_options<I>(variables: I) -> Vec<(String, String)>
where
    I: IntoIterator<Item = (String, String)>,
{
    let variables: Vec<_> = variables.into_iter().collect();
    let mut options = BTreeMap::new();

    // object_store's config-key parser expects lowercase names even though
    // process environment variables conventionally use uppercase names.
    for (key, value) in &variables {
        if key.starts_with("AWS_") {
            options.insert(key.to_ascii_lowercase(), value.clone());
        }
    }
    // Explicit OBJECT_STORE_* values override the corresponding AWS value.
    // Both OBJECT_STORE_ENDPOINT and OBJECT_STORE_AWS_ENDPOINT are accepted.
    for (key, value) in &variables {
        if let Some(key) = key.strip_prefix("OBJECT_STORE_") {
            options.insert(key.to_ascii_lowercase(), value.clone());
        }
    }
    options.into_iter().collect()
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ManifestPart {
    pub id: String,
    pub partition: String,
}

impl From<&Part> for ManifestPart {
    fn from(part: &Part) -> Self {
        Self {
            id: part.meta.id.clone(),
            partition: part.meta.partition.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub format_version: u32,
    pub generation: u64,
    pub parts: Vec<ManifestPart>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TraceManifestPart {
    pub id: String,
    pub partition: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TraceManifest {
    pub format_version: u32,
    pub generation: u64,
    pub parts: Vec<TraceManifestPart>,
}

/// Durable intent for the cross-domain flush boundary. The journal
/// checkpoint is the commit record: before it advances, startup removes both
/// manifest additions; after it advances, startup only clears this intent.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct FlushTransaction {
    pub offset: u64,
    pub log_parts: Vec<ManifestPart>,
    pub trace_parts: Vec<TraceManifestPart>,
}

impl Default for TraceManifest {
    fn default() -> Self {
        Self {
            format_version: MANIFEST_FORMAT_VERSION,
            generation: 0,
            parts: Vec::new(),
        }
    }
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            format_version: MANIFEST_FORMAT_VERSION,
            generation: 0,
            parts: Vec::new(),
        }
    }
}

struct LoadedManifest {
    manifest: Manifest,
    version: Option<UpdateVersion>,
}

struct LocalMergeGroup {
    old_ids: Vec<String>,
    added: Vec<Part>,
}

mod fault_store;

include!("paths.rs");
include!("catalog.rs");
include!("object_io.rs");
include!("cache.rs");
include!("recovery.rs");

#[cfg(test)]
mod tests {
    include!("tests.rs");
}
