use std::path::PathBuf;
use std::time::Duration;

#[derive(Clone)]
pub struct Config {
    pub listen_addr: String,
    pub otlp_grpc_addr: String,
    pub data_dir: PathBuf,
    pub max_batch_bytes: usize,
    pub max_batch_ms: u64,
    pub flush_max_bytes: u64,
    pub flush_max_interval: Duration,
    pub flush_check_interval: Duration,
    pub row_group_size: usize,
    pub merge_min_part_count: usize,
    pub merge_target_part_rows: u64,
    pub merge_max_part_rows: u64,
    pub merge_interval: Duration,
    /// Object-store URL, for example `s3://bucket/loggytracy` or
    /// `file:///var/lib/loggytracy-remote`. When unset, the engine keeps the
    /// M1 local-only behaviour.
    pub object_store_url: Option<String>,
    pub cache_max_bytes: u64,
    pub cache_eviction_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:3100".to_string(),
            otlp_grpc_addr: "0.0.0.0:4317".to_string(),
            data_dir: PathBuf::from("./data"),
            max_batch_bytes: 1024 * 1024,
            max_batch_ms: 200,
            flush_max_bytes: 1024 * 1024,
            flush_max_interval: Duration::from_secs(5),
            flush_check_interval: Duration::from_millis(500),
            row_group_size: 8192,
            merge_min_part_count: 4,
            merge_target_part_rows: 1_000_000,
            merge_max_part_rows: 4_000_000,
            merge_interval: Duration::from_secs(30),
            object_store_url: None,
            cache_max_bytes: 10 * 1024 * 1024 * 1024,
            cache_eviction_interval: Duration::from_secs(30),
        }
    }
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let object_store_url = std::env::var("LOGGYTRACY_OBJECT_STORE_URL")
            .ok()
            .filter(|value| !value.trim().is_empty());
        let otlp_grpc_addr = std::env::var("LOGGYTRACY_OTLP_GRPC_ADDR")
            .unwrap_or_else(|_| Self::default().otlp_grpc_addr);
        let cache_max_bytes = match std::env::var("LOGGYTRACY_CACHE_MAX_BYTES") {
            Ok(value) => value.parse().map_err(|error| {
                format!("invalid LOGGYTRACY_CACHE_MAX_BYTES {value:?}: {error}")
            })?,
            Err(_) => Self::default().cache_max_bytes,
        };
        Ok(Self {
            otlp_grpc_addr,
            object_store_url,
            cache_max_bytes,
            ..Self::default()
        })
    }
}
