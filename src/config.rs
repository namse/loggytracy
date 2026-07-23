use std::path::PathBuf;
use std::time::Duration;

#[derive(Clone)]
pub struct Config {
    pub listen_addr: String,
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
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:3100".to_string(),
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
        }
    }
}
