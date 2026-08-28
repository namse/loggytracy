use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use crate::queue::QueueLimits;
use crate::receive::{DEFAULT_LISTEN_ADDR, DEFAULT_MAX_INFLIGHT_BYTES, DEFAULT_MAX_REQUEST_BYTES};
use crate::send::SenderConfig;

#[derive(Clone, Debug)]
pub struct Config {
    pub listen_addr: SocketAddr,
    pub data_dir: PathBuf,
    pub signy_url: String,
    pub max_request_bytes: usize,
    pub max_inflight_bytes: usize,
    pub queue: QueueLimits,
    pub sender: SenderConfig,
    pub send_timeout: Duration,
    pub report_interval: Duration,
    pub zstd_level: i32,
    pub log_json: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            listen_addr: DEFAULT_LISTEN_ADDR
                .parse()
                .expect("the default listen address parses"),
            data_dir: PathBuf::from("/var/lib/collecty"),
            signy_url: "http://127.0.0.1:3100".to_string(),
            max_request_bytes: DEFAULT_MAX_REQUEST_BYTES,
            max_inflight_bytes: DEFAULT_MAX_INFLIGHT_BYTES,
            queue: QueueLimits::default(),
            sender: SenderConfig::default(),
            send_timeout: Duration::from_secs(30),
            report_interval: Duration::from_secs(60),
            zstd_level: crate::wire::ZSTD_LEVEL,
            log_json: false,
        }
    }
}

impl Config {
    pub fn from_env() -> Result<Config, String> {
        let defaults = Config::default();
        let config = Config {
            listen_addr: socket_addr("COLLECTY_LISTEN_ADDR", defaults.listen_addr)?,
            data_dir: path("COLLECTY_DATA_DIR", defaults.data_dir),
            signy_url: string("COLLECTY_SIGNY_URL", defaults.signy_url),
            max_request_bytes: bytes("COLLECTY_MAX_REQUEST_BYTES", defaults.max_request_bytes)?,
            max_inflight_bytes: bytes("COLLECTY_MAX_INFLIGHT_BYTES", defaults.max_inflight_bytes)?,
            queue: QueueLimits {
                max_bytes: bytes64("COLLECTY_QUEUE_MAX_BYTES", defaults.queue.max_bytes)?,
                max_segment_bytes: bytes64(
                    "COLLECTY_QUEUE_SEGMENT_BYTES",
                    defaults.queue.max_segment_bytes,
                )?,
                max_segment_age: duration(
                    "COLLECTY_SEGMENT_MAX_AGE",
                    defaults.queue.max_segment_age,
                )?,
            },
            sender: SenderConfig {
                retry_initial: duration("COLLECTY_RETRY_INITIAL", defaults.sender.retry_initial)?,
                retry_max: duration("COLLECTY_RETRY_MAX", defaults.sender.retry_max)?,
            },
            send_timeout: duration("COLLECTY_SEND_TIMEOUT", defaults.send_timeout)?,
            report_interval: duration("COLLECTY_REPORT_INTERVAL", defaults.report_interval)?,
            zstd_level: level("COLLECTY_ZSTD_LEVEL", defaults.zstd_level)?,
            log_json: matches!(
                string("COLLECTY_LOG_FORMAT", "text".to_string()).as_str(),
                "json"
            ),
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), String> {
        if self.max_request_bytes > u32::MAX as usize {
            return Err(format!(
                "COLLECTY_MAX_REQUEST_BYTES is {} and cannot exceed {}",
                self.max_request_bytes,
                u32::MAX
            ));
        }
        if self.max_inflight_bytes < self.max_request_bytes {
            return Err(format!(
                "COLLECTY_MAX_INFLIGHT_BYTES ({}) is below COLLECTY_MAX_REQUEST_BYTES ({}), \
which would refuse every large export forever",
                self.max_inflight_bytes, self.max_request_bytes
            ));
        }
        if self.queue.max_bytes < self.queue.max_segment_bytes {
            return Err(format!(
                "COLLECTY_QUEUE_MAX_BYTES ({}) is below COLLECTY_QUEUE_SEGMENT_BYTES ({})",
                self.queue.max_bytes, self.queue.max_segment_bytes
            ));
        }
        if (self.max_request_bytes as u64) >= self.queue.max_bytes {
            return Err(format!(
                "COLLECTY_QUEUE_MAX_BYTES ({}) leaves no room for one \
COLLECTY_MAX_REQUEST_BYTES ({}) export",
                self.queue.max_bytes, self.max_request_bytes
            ));
        }
        if self.queue.max_segment_age.is_zero() {
            return Err("COLLECTY_SEGMENT_MAX_AGE must be positive".to_string());
        }
        if !(1..=22).contains(&self.zstd_level) {
            return Err(format!(
                "COLLECTY_ZSTD_LEVEL is {} and must be between 1 and 22",
                self.zstd_level
            ));
        }
        Ok(())
    }

    pub fn queue_dir(&self) -> PathBuf {
        self.data_dir.join("queue")
    }
}

fn string(name: &str, fallback: String) -> String {
    std::env::var(name).unwrap_or(fallback)
}

fn path(name: &str, fallback: PathBuf) -> PathBuf {
    std::env::var(name).map(PathBuf::from).unwrap_or(fallback)
}

fn socket_addr(name: &str, fallback: SocketAddr) -> Result<SocketAddr, String> {
    match std::env::var(name) {
        Err(_) => Ok(fallback),
        Ok(value) => value
            .parse()
            .map_err(|error| format!("invalid {name} {value:?}: {error}")),
    }
}

fn level(name: &str, fallback: i32) -> Result<i32, String> {
    match std::env::var(name) {
        Err(_) => Ok(fallback),
        Ok(value) => value
            .parse::<i32>()
            .map_err(|error| format!("invalid {name} {value:?}: {error}")),
    }
}

fn bytes(name: &str, fallback: usize) -> Result<usize, String> {
    Ok(bytes64(name, fallback as u64)? as usize)
}

fn bytes64(name: &str, fallback: u64) -> Result<u64, String> {
    match std::env::var(name) {
        Err(_) => Ok(fallback),
        Ok(value) => {
            let parsed = parse_bytes(&value)
                .ok_or_else(|| format!("invalid {name} {value:?}: expected 512, 64MiB or 1GiB"))?;
            if parsed == 0 {
                return Err(format!("{name} must be positive"));
            }
            Ok(parsed)
        }
    }
}

pub fn parse_bytes(value: &str) -> Option<u64> {
    let value = value.trim();
    let (digits, scale) = match value {
        _ if value.ends_with("KiB") => (&value[..value.len() - 3], 1024),
        _ if value.ends_with("MiB") => (&value[..value.len() - 3], 1024 * 1024),
        _ if value.ends_with("GiB") => (&value[..value.len() - 3], 1024 * 1024 * 1024),
        _ => (value, 1),
    };
    digits
        .trim()
        .parse::<u64>()
        .ok()
        .and_then(|number| number.checked_mul(scale))
}

fn duration(name: &str, fallback: Duration) -> Result<Duration, String> {
    match std::env::var(name) {
        Err(_) => Ok(fallback),
        Ok(value) => parse_duration(&value)
            .ok_or_else(|| format!("invalid {name} {value:?}: expected 500ms, 30s or 5m")),
    }
}

pub fn parse_duration(value: &str) -> Option<Duration> {
    let value = value.trim();
    let (digits, unit) = match value {
        _ if value.ends_with("ms") => (&value[..value.len() - 2], 1),
        _ if value.ends_with('s') => (&value[..value.len() - 1], 1000),
        _ if value.ends_with('m') => (&value[..value.len() - 1], 60 * 1000),
        _ if value.ends_with('h') => (&value[..value.len() - 1], 60 * 60 * 1000),
        _ => return None,
    };
    digits
        .trim()
        .parse::<u64>()
        .ok()
        .and_then(|number| number.checked_mul(unit))
        .map(Duration::from_millis)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_sizes_accept_a_binary_suffix_and_reject_anything_else() {
        assert_eq!(parse_bytes("512"), Some(512));
        assert_eq!(parse_bytes("64KiB"), Some(64 * 1024));
        assert_eq!(parse_bytes("8 MiB"), Some(8 * 1024 * 1024));
        assert_eq!(parse_bytes("1GiB"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_bytes("1GB"), None);
        assert_eq!(parse_bytes("many"), None);
    }

    #[test]
    fn durations_need_a_unit() {
        assert_eq!(parse_duration("500ms"), Some(Duration::from_millis(500)));
        assert_eq!(parse_duration("30s"), Some(Duration::from_secs(30)));
        assert_eq!(parse_duration("5m"), Some(Duration::from_secs(300)));
        assert_eq!(parse_duration("2h"), Some(Duration::from_secs(7200)));
        assert_eq!(parse_duration("30"), None);
    }

    #[test]
    fn an_inflight_ceiling_below_the_request_ceiling_is_refused() {
        let config = Config {
            max_inflight_bytes: 1024,
            max_request_bytes: 4096,
            ..Config::default()
        };
        let error = config.validate().expect_err("a refusal");
        assert!(error.contains("COLLECTY_MAX_INFLIGHT_BYTES"), "{error}");
    }

    #[test]
    fn a_queue_that_cannot_hold_one_export_is_refused() {
        let config = Config {
            queue: QueueLimits {
                max_bytes: 4096,
                max_segment_bytes: 4096,
                ..QueueLimits::default()
            },
            max_request_bytes: 8192,
            max_inflight_bytes: 8192,
            ..Config::default()
        };
        let error = config.validate().expect_err("a refusal");
        assert!(error.contains("COLLECTY_QUEUE_MAX_BYTES"), "{error}");
    }

    #[test]
    fn the_defaults_are_consistent() {
        Config::default().validate().expect("consistent defaults");
    }
}
