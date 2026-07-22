use std::path::PathBuf;

pub struct Config {
    pub listen_addr: String,
    pub data_dir: PathBuf,
    pub max_batch_bytes: usize,
    pub max_batch_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:3100".to_string(),
            data_dir: PathBuf::from("./data"),
            max_batch_bytes: 1024 * 1024,
            max_batch_ms: 200,
        }
    }
}
