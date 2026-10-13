//! A system-wide monotonic clock, comparable ACROSS processes on one host.
//!
//! `std::time::Instant` is opaque and its zero is per-process, so it cannot
//! timestamp a value emitted in one process and received in another. The (A)
//! cross-process arm needs a shared zero: `CLOCK_MONOTONIC` is system-wide
//! (since boot) and identical in every process on the host (on macOS it is
//! mach-based and also system-wide), so `emit_ns` stamped in the provider
//! process and `recv_ns` stamped in the bench process are directly subtractable.
//!
//! BOTH arms call this one function → the SAME clock id → the subtraction is
//! valid. Uses the safe `rustix` wrapper so the crate keeps `#![forbid(unsafe_code)]`.
//!
//! The cross-process latency bench (the only caller) runs on unix, where
//! `CLOCK_MONOTONIC` is system-wide. The clock API is unix-only, so on non-unix
//! targets (Windows, which the workspace must still compile + clippy clean) we fall
//! back to a process-relative monotonic clock. That fallback is single-process only —
//! it exists so the workspace builds on Windows, not to run the cross-process bench
//! there.

/// Nanoseconds on the system monotonic clock. On unix, comparable across processes on
/// the same host (system-wide `CLOCK_MONOTONIC`); on other targets, process-relative
/// monotonic only (the cross-process bench is unix-only).
#[cfg(unix)]
pub fn monotonic_ns() -> u64 {
    use rustix::time::{clock_gettime, ClockId};
    let ts = clock_gettime(ClockId::Monotonic);
    (ts.tv_sec as u64)
        .wrapping_mul(1_000_000_000)
        .wrapping_add(ts.tv_nsec as u64)
}

/// Process-relative monotonic fallback for non-unix targets (e.g. Windows). Anchored
/// at first call via a monotonic `Instant`, so it is single-process only — the
/// cross-process (A) bench that needs a system-wide zero runs on unix. This exists so
/// the workspace compiles + clippy-clean on Windows.
#[cfg(not(unix))]
pub fn monotonic_ns() -> u64 {
    use std::sync::OnceLock;
    use std::time::Instant;
    static ANCHOR: OnceLock<Instant> = OnceLock::new();
    ANCHOR.get_or_init(Instant::now).elapsed().as_nanos() as u64
}
