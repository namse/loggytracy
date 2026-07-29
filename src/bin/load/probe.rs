//! What the harness reads about the server rather than from it: resident
//! memory out of `/proc`, and the Prometheus scrape.

use std::collections::HashMap;

/// Resident memory of the **server**.
///
/// `VmHWM` is the kernel's own high-water mark, so a peak between two samples
/// cannot be missed. What this replaces polled `ps` every tenth push and took
/// `unwrap_or(0)` on failure (`bin/load.rs:850-861` before the rewrite), so a
/// server whose peak fell between two polls, or whose `ps` call failed, was
/// recorded as having used less memory than it did — and before that the poll
/// read `std::process::id()`, the harness's own RSS.
pub struct Memory {
    pub vm_rss_bytes: u64,
    pub vm_hwm_bytes: u64,
}

pub fn read_memory(pid: u32) -> Result<Memory, String> {
    let path = format!("/proc/{pid}/status");
    let text = std::fs::read_to_string(&path).map_err(|error| format!("{path}: {error}"))?;
    let field = |name: &str| -> Option<u64> {
        text.lines()
            .find_map(|line| line.strip_prefix(name))
            .and_then(|value| {
                value
                    .trim()
                    .trim_end_matches("kB")
                    .trim()
                    .parse::<u64>()
                    .ok()
            })
            .and_then(|kilobytes| kilobytes.checked_mul(1024))
    };
    let vm_rss_bytes = field("VmRSS:").ok_or_else(|| format!("{path} has no VmRSS"))?;
    let vm_hwm_bytes = field("VmHWM:").ok_or_else(|| format!("{path} has no VmHWM"))?;
    Ok(Memory {
        vm_rss_bytes,
        vm_hwm_bytes,
    })
}

pub type Metrics = HashMap<String, f64>;

/// Prometheus text into name -> value, where the name includes the label set
/// so `..._total{kind="put"}` is its own series.
pub fn parse_metrics(body: &[u8]) -> Metrics {
    let mut map = Metrics::new();
    let Ok(text) = std::str::from_utf8(body) else {
        return map;
    };
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split_whitespace();
        if let (Some(name), Some(value)) = (parts.next(), parts.next())
            && let Ok(value) = value.parse::<f64>()
        {
            map.insert(name.to_string(), value);
        }
    }
    map
}

pub fn counter_delta(start: &Metrics, end: &Metrics, name: &str) -> u64 {
    let start_value = start.get(name).copied().unwrap_or(0.0);
    let end_value = end.get(name).copied().unwrap_or(0.0);
    (end_value - start_value).max(0.0) as u64
}

pub fn object_store_op_delta(start: &Metrics, end: &Metrics, kind: &str) -> u64 {
    counter_delta(
        start,
        end,
        &format!("loggytracy_object_store_operations_total{{kind=\"{kind}\"}}"),
    )
}

pub fn gauge(metrics: &Metrics, name: &str) -> u64 {
    metrics.get(name).copied().unwrap_or(0.0) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_high_water_mark_of_a_live_process() {
        let memory = read_memory(std::process::id()).expect("this process has a /proc status");
        assert!(memory.vm_rss_bytes > 0);
        assert!(memory.vm_hwm_bytes >= memory.vm_rss_bytes);
    }

    /// The failure that must not be silently zero: an unreadable process.
    #[test]
    fn a_process_that_does_not_exist_is_an_error_not_a_zero() {
        assert!(read_memory(u32::MAX).is_err());
    }

    #[test]
    fn keeps_the_label_set_in_the_series_name() {
        let metrics = parse_metrics(
            b"# TYPE x counter\nloggytracy_object_store_operations_total{kind=\"put\"} 12\n\
loggytracy_wal_backlog_bytes 4096\n",
        );
        assert_eq!(gauge(&metrics, "loggytracy_wal_backlog_bytes"), 4096);
        assert_eq!(object_store_op_delta(&Metrics::new(), &metrics, "put"), 12);
    }
}
