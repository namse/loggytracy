//! glibc allocator tuning, applied before the runtime spawns a thread.
//!
//! `docs/MEMORY_ATTRIBUTION.md` measured the gap this closes: at the OOM the
//! kernel decided on, 44–69% of the cgroup's anonymous memory was
//! already-freed heap glibc had not returned, and the `trimmed` variant —
//! `MALLOC_ARENA_MAX=1` with a fixed 128 KiB trim threshold — took anon/live
//! from 2.5–4.1 down to 1.34 on the same workload. Setting it here instead of
//! asking every operator to export two environment variables makes the
//! measured configuration the default one.
//!
//! This must run before any other thread exists: an arena is created the
//! first time a thread contends for one, and `M_ARENA_MAX` only bounds the
//! arenas not yet created. `main` calls this before building the tokio
//! runtime.
//!
//! `LOGGYTRACY_MALLOC_TUNING=off` restores glibc's defaults — the knob an
//! A/B measurement or an unexpected throughput regression reaches for.

/// One arena: every thread shares it, so freed memory consolidates where
/// `M_TRIM_THRESHOLD` can actually return it. The measured cost on this
/// engine's ingest path was within noise (the hot allocation sites are
/// batch-scoped, not per-row), but it is a cost class worth re-measuring on
/// any workload change — which is why the override exists.
const ARENA_MAX: libc_shim::c_int = 1;

/// Fixed 128 KiB, matching the measured `MALLOC_TRIM_THRESHOLD_=131072`.
/// Setting it also disables glibc's dynamic threshold, which only ever
/// ratchets upward and is how the high-water mark became permanent.
const TRIM_THRESHOLD: libc_shim::c_int = 131072;

mod libc_shim {
    #![allow(non_camel_case_types)]
    pub type c_int = i32;
    // glibc `malloc.h` values; stable ABI.
    pub const M_TRIM_THRESHOLD: c_int = -1;
    pub const M_ARENA_MAX: c_int = -8;
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    unsafe extern "C" {
        pub fn mallopt(param: c_int, value: c_int) -> c_int;
    }
}

/// Apply the tuning unless `LOGGYTRACY_MALLOC_TUNING=off`.
///
/// Returns whether it was applied, purely so startup can log the fact — an
/// operator comparing memory numbers across builds needs to know which
/// allocator behavior each one ran with.
pub fn apply_from_env() -> bool {
    if std::env::var("LOGGYTRACY_MALLOC_TUNING")
        .map(|value| value.eq_ignore_ascii_case("off"))
        .unwrap_or(false)
    {
        return false;
    }
    apply()
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn apply() -> bool {
    // SAFETY: mallopt only writes allocator parameters; called before any
    // other thread exists.
    unsafe {
        libc_shim::mallopt(libc_shim::M_ARENA_MAX, ARENA_MAX);
        libc_shim::mallopt(libc_shim::M_TRIM_THRESHOLD, TRIM_THRESHOLD);
    }
    true
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn apply() -> bool {
    false
}
