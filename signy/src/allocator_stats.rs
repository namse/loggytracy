//! What the process is holding, as the kernel and the allocator each see it.
//!
//! [`crate::memprof`] answers "which part of the engine owns these bytes", and
//! only in the instrumented build, whose wrapper allocates through the system
//! allocator. The shipped binary allocates through mimalloc (`src/main.rs`),
//! and about that build this process could publish nothing at all: a run that
//! died at its limit left behind one number, the cgroup's anonymous total,
//! which is live bytes and retained bytes added together with no way to tell
//! them apart. Those are different problems with different fixes, and "it used
//! 2 GiB" does not say which one happened.
//!
//! Two sources, kept apart because they answer different questions:
//!
//! * **The kernel**, through `/proc/self`. `rss` is what the cgroup limit is
//!   actually spent on and what an OOM kill is decided on; `peak_rss` is the
//!   kernel's own high-water mark, so a peak between two scrapes is not lost.
//!   Published by every build, because it is a fact about the process rather
//!   than about an allocator.
//! * **mimalloc**, through `mi_process_info`. `committed` is what it has asked
//!   the operating system for and not handed back. Published only when
//!   mimalloc is the allocator, which is exactly when `memprof` is off:
//!   publishing it from the instrumented build would attribute glibc's
//!   behavior to mimalloc's counters, and the two must never be read off the
//!   same run.
//!
//! **What is deliberately not here, and why.** mimalloc's in-use bytes
//! (`malloc_normal`, `malloc_requested`) are maintained only when the C
//! library is compiled with `MI_STAT > 0`, and a release build compiles them
//! out — so the production binary can say what it holds from the operating
//! system but not what of that is in use. `committed` and `rss` both count
//! retained free memory, so neither alone separates live from retained. Until
//! that gap is closed, the live side has to come from the engine's own gauges
//! (memtable, sidecars, part metadata, caches) and the difference against
//! `committed` is what the allocator is sitting on. `mi_stats_get_json` would
//! add `reserved` and `purged` — how much has ever been given back — and is
//! the cheap next step if that question comes up.
//!
//! One trap, found by reading mimalloc's source rather than its header, and
//! recorded so nobody re-derives it: **`mi_process_info` does not fill
//! `current_rss` on Linux.** It seeds that field from the commit counter and
//! then lets the platform overwrite it, and the Linux implementation sets only
//! `utime`, `stime`, `page_faults` and `peak_rss` (from `ru_maxrss`). A gauge
//! named `rss` fed from that call would report committed bytes under the name
//! of resident ones. That is why the resident numbers below come from `/proc`.

/// What the process holds, at one instant.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Snapshot {
    /// Resident bytes, from the kernel. `None` where `/proc` cannot be read.
    pub rss_bytes: Option<u64>,
    /// The kernel's own high-water mark for the above.
    pub peak_rss_bytes: Option<u64>,
    /// Bytes mimalloc holds from the operating system. `None` in a build whose
    /// allocator is not mimalloc.
    pub committed_bytes: Option<u64>,
    pub peak_committed_bytes: Option<u64>,
    /// Major faults — the ones that went to disk, so they say reclaim took
    /// back a page that was wanted again.
    pub major_page_faults: Option<u64>,
}

/// Read both sources.
pub fn snapshot() -> Snapshot {
    let (rss_bytes, peak_rss_bytes) = kernel_resident();
    let allocator = self::allocator::committed();
    Snapshot {
        rss_bytes,
        peak_rss_bytes,
        committed_bytes: allocator.map(|(committed, _, _)| committed),
        peak_committed_bytes: allocator.map(|(_, peak, _)| peak),
        major_page_faults: allocator.map(|(_, _, faults)| faults),
    }
}

pub fn render() -> String {
    let stats = snapshot();
    let mut out = String::new();
    if let Some(rss) = stats.rss_bytes {
        out.push_str(&format!(
            "# HELP signy_process_rss_bytes Resident bytes of this process, from the kernel. \
This is what a memory limit is spent on and what an OOM kill is decided on.\n\
# TYPE signy_process_rss_bytes gauge\n\
signy_process_rss_bytes {rss}\n"
        ));
    }
    if let Some(peak) = stats.peak_rss_bytes {
        out.push_str(&format!(
            "# HELP signy_process_peak_rss_bytes The kernel's own high-water mark for the above, \
so a peak between two scrapes is not lost.\n\
# TYPE signy_process_peak_rss_bytes gauge\n\
signy_process_peak_rss_bytes {peak}\n"
        ));
    }
    if let Some(committed) = stats.committed_bytes {
        out.push_str(&format!(
            "# HELP signy_allocator_committed_bytes Bytes mimalloc holds from the operating \
system. Subtract what the engine's own gauges account for and the remainder is what the allocator \
is sitting on -- which is what kills a process whose residents are all flat. Absent in builds \
whose allocator is not mimalloc.\n\
# TYPE signy_allocator_committed_bytes gauge\n\
signy_allocator_committed_bytes {committed}\n"
        ));
    }
    if let Some(peak) = stats.peak_committed_bytes {
        out.push_str(&format!(
            "# TYPE signy_allocator_peak_committed_bytes gauge\n\
signy_allocator_peak_committed_bytes {peak}\n"
        ));
    }
    if let Some(faults) = stats.major_page_faults {
        out.push_str(&format!(
            "# HELP signy_process_major_page_faults_total Faults that went to disk: reclaim took \
back a page this process wanted again.\n\
# TYPE signy_process_major_page_faults_total counter\n\
signy_process_major_page_faults_total {faults}\n"
        ));
    }
    out
}

/// Resident and peak-resident bytes from `/proc/self/status`.
///
/// `VmRSS` and `VmHWM` rather than `statm`, because the peak is only in
/// `status` and reading one file for both keeps them from disagreeing about
/// the instant they describe.
fn kernel_resident() -> (Option<u64>, Option<u64>) {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return (None, None);
    };
    let field = |name: &str| {
        status
            .lines()
            .find_map(|line| {
                line.strip_prefix(name)?
                    .split_whitespace()
                    .next()?
                    .parse::<u64>()
                    .ok()
            })
            .map(|kib| kib * 1024)
    };
    (field("VmRSS:"), field("VmHWM:"))
}

#[cfg(not(feature = "memprof"))]
mod allocator {
    /// `(committed, peak_committed, major_page_faults)`.
    pub fn committed() -> Option<(u64, u64, u64)> {
        let mut elapsed_msecs = 0usize;
        let mut user_msecs = 0usize;
        let mut system_msecs = 0usize;
        let mut current_rss = 0usize;
        let mut peak_rss = 0usize;
        let mut current_commit = 0usize;
        let mut peak_commit = 0usize;
        let mut page_faults = 0usize;
        // SAFETY: every argument is a live, initialized `usize` this call only
        // writes to. `mi_process_info` reads counters and allocates nothing.
        unsafe {
            libmimalloc_sys::mi_process_info(
                &mut elapsed_msecs,
                &mut user_msecs,
                &mut system_msecs,
                &mut current_rss,
                &mut peak_rss,
                &mut current_commit,
                &mut peak_commit,
                &mut page_faults,
            );
        }
        // `current_rss` and `peak_rss` are deliberately dropped on the floor:
        // see this module's header for what Linux does and does not fill in.
        Some((
            current_commit as u64,
            peak_commit as u64,
            page_faults as u64,
        ))
    }
}

#[cfg(feature = "memprof")]
mod allocator {
    /// Nothing: this build's allocator is not mimalloc, and mimalloc's
    /// counters would describe a heap nothing in the process allocates from.
    pub fn committed() -> Option<(u64, u64, u64)> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_running_process_reports_a_resident_size_and_a_peak_at_least_as_large() {
        let stats = snapshot();
        let rss = stats.rss_bytes.expect("/proc/self/status carries VmRSS");
        let peak = stats
            .peak_rss_bytes
            .expect("/proc/self/status carries VmHWM");
        assert!(rss > 0, "{stats:?}");
        assert!(peak >= rss, "{stats:?}");
        let body = render();
        assert!(body.contains("signy_process_rss_bytes "), "{body}");
    }

    /// The allocator half is published by exactly one build, and it is the one
    /// that ships.
    #[test]
    fn only_the_production_build_publishes_the_allocator_gauges() {
        let stats = snapshot();
        let body = render();
        if cfg!(feature = "memprof") {
            assert!(stats.committed_bytes.is_none(), "{stats:?}");
            assert!(!body.contains("signy_allocator_committed_bytes"), "{body}");
        } else {
            assert!(stats.committed_bytes.is_some(), "{stats:?}");
            assert!(body.contains("signy_allocator_committed_bytes "), "{body}");
        }
    }
}
