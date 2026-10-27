//! Fsync accounting for the writer sync deadline.
//!
//! A healthy writer syncs every appended record within D (10 s), at any append
//! rate, and a dropped handle syncs dirty data before the writer exits. The
//! loop issues its `Wal::sync` calls from four paths — an explicit barrier,
//! the fired deadline, a clean shutdown, and a channel disconnect — and every
//! call is counted here by the path that issued it, so health can report the
//! cost of each. Counts are process totals in plain atomics: in production the
//! fsync path never takes a lock, and health never scans per-writer rows.
//! (Test builds additionally record fence-keyed counts under a mutex, so one
//! test's assertions cannot observe another test's writer.)
//!
//! The monotonic source lives here too. Its scripted overrides exist only in
//! test builds and are keyed by the writer's fence, so one test's fake clock
//! never disturbs another writer's real deadline. Production pays nothing for
//! the override machinery: it is compiled out, and the loop reads no clock
//! while clean.

#[cfg(test)]
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(test)]
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

/// D, the writer sync deadline: dirty data waits at most this long for a
/// barrier before the loop syncs it on its own.
pub const SYNC_DEADLINE: Duration = Duration::from_secs(10);

/// The same deadline in milliseconds, the unit of the monotonic source below.
pub const SYNC_DEADLINE_MS: u64 = 10_000;

/// Why the writer loop issued a `Wal::sync`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FsyncCause {
    /// An explicit `sync()` barrier from the owner.
    Barrier,
    /// The sync deadline fired with dirty data and no barrier arrived first.
    Deadline,
    /// The final sync of a clean `Shutdown`.
    Shutdown,
    /// The final sync after the handle was dropped without shutting down.
    Disconnect,
}

/// Cumulative per-cause fsync totals.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DurabilitySnapshot {
    pub barrier: u64,
    pub deadline: u64,
    pub shutdown: u64,
    pub disconnect: u64,
}

impl DurabilitySnapshot {
    /// The total fsyncs across every cause.
    pub fn total(self) -> u64 {
        self.barrier + self.deadline + self.shutdown + self.disconnect
    }

    /// The total for one cause.
    pub fn get(self, cause: FsyncCause) -> u64 {
        match cause {
            FsyncCause::Barrier => self.barrier,
            FsyncCause::Deadline => self.deadline,
            FsyncCause::Shutdown => self.shutdown,
            FsyncCause::Disconnect => self.disconnect,
        }
    }
}

fn cause_index(cause: FsyncCause) -> usize {
    match cause {
        FsyncCause::Barrier => 0,
        FsyncCause::Deadline => 1,
        FsyncCause::Shutdown => 2,
        FsyncCause::Disconnect => 3,
    }
}

/// Process-wide fsync totals by cause. Plain atomics: production recording
/// never blocks, and readers never contend with the fsync path. Test builds
/// additionally update fence-keyed rows under a mutex (see below).
static TOTALS: [AtomicU64; 4] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];

/// Record one `Wal::sync` the writer loop issued, attributed to the path that
/// issued it. Counts calls the loop actually made: a poisoned or clean path
/// that skips the call counts nothing.
pub fn record_fsync(cause: FsyncCause, fence: u64) {
    TOTALS[cause_index(cause)].fetch_add(1, Ordering::Relaxed);
    #[cfg(test)]
    record_fence_fsync(cause, fence);
    #[cfg(not(test))]
    let _ = fence;
}

/// Cumulative totals across every writer, for health.
pub fn snapshot() -> DurabilitySnapshot {
    DurabilitySnapshot {
        barrier: TOTALS[0].load(Ordering::Relaxed),
        deadline: TOTALS[1].load(Ordering::Relaxed),
        shutdown: TOTALS[2].load(Ordering::Relaxed),
        disconnect: TOTALS[3].load(Ordering::Relaxed),
    }
}

#[cfg(test)]
fn fence_counts() -> &'static Mutex<HashMap<u64, [u64; 4]>> {
    static COUNTS: OnceLock<Mutex<HashMap<u64, [u64; 4]>>> = OnceLock::new();
    COUNTS.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
fn snapshot_row(entry: Option<&[u64; 4]>) -> DurabilitySnapshot {
    match entry {
        Some(entry) => DurabilitySnapshot {
            barrier: entry[0],
            deadline: entry[1],
            shutdown: entry[2],
            disconnect: entry[3],
        },
        None => DurabilitySnapshot::default(),
    }
}

/// Per-writer totals, for tests only: concurrent writers in other tests also
/// record process totals, so only a fence-keyed row isolates one test's
/// writer. Production keeps no per-writer rows.
#[cfg(test)]
fn record_fence_fsync(cause: FsyncCause, fence: u64) {
    let mut counts = fence_counts().lock().unwrap();
    counts.entry(fence).or_default()[cause_index(cause)] += 1;
}

/// Totals for one writer's fence. Test-only, for the reason above.
#[cfg(test)]
pub fn counts_for_fence(fence: u64) -> DurabilitySnapshot {
    let counts = fence_counts().lock().unwrap();
    snapshot_row(counts.get(&fence))
}

/// A fence-keyed test override: a fake monotonic now, a replacement deadline,
/// or both. `None` in either field keeps the real value for that part.
#[cfg(test)]
struct FenceOverride {
    now_ms: Option<u64>,
    deadline_ms: Option<u64>,
}

#[cfg(test)]
fn overrides() -> &'static Mutex<HashMap<u64, FenceOverride>> {
    static OVERRIDES: OnceLock<Mutex<HashMap<u64, FenceOverride>>> = OnceLock::new();
    OVERRIDES.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
fn override_now_ms_for(fence: u64) -> Option<u64> {
    overrides()
        .lock()
        .unwrap()
        .get(&fence)
        .and_then(|over| over.now_ms)
}

#[cfg(test)]
fn override_deadline_ms_for(fence: u64) -> Option<u64> {
    overrides()
        .lock()
        .unwrap()
        .get(&fence)
        .and_then(|over| over.deadline_ms)
}

/// This process's monotonic now, in milliseconds. Never the wall clock and
/// never a record stamp: only deadline arithmetic reads it.
pub(crate) fn monotonic_now_ms_for(fence: u64) -> u64 {
    #[cfg(test)]
    if let Some(now) = override_now_ms_for(fence) {
        return now;
    }
    #[cfg(not(test))]
    let _ = fence;
    boot().elapsed().as_millis().try_into().unwrap_or(u64::MAX)
}

/// The sync deadline in milliseconds for one writer: D unless a test
/// replaced it for that fence.
pub(crate) fn deadline_ms_for(fence: u64) -> u64 {
    #[cfg(test)]
    if let Some(deadline) = override_deadline_ms_for(fence) {
        return deadline;
    }
    #[cfg(not(test))]
    let _ = fence;
    SYNC_DEADLINE_MS
}

fn boot() -> Instant {
    static BOOT: OnceLock<Instant> = OnceLock::new();
    *BOOT.get_or_init(Instant::now)
}

#[cfg(test)]
fn blank_override_for_test() -> FenceOverride {
    FenceOverride {
        now_ms: None,
        deadline_ms: None,
    }
}

/// Pin one writer's monotonic now to a scripted value. Only that fence reads
/// it; every other writer keeps its real deadline.
#[cfg(test)]
pub(crate) fn set_fake_monotonic_ms_for_test(fence: u64, now_ms: u64) {
    let mut overrides = overrides().lock().unwrap();
    overrides
        .entry(fence)
        .or_insert_with(blank_override_for_test)
        .now_ms = Some(now_ms);
}

/// Move one writer's fake clock forward without touching its deadline.
#[cfg(test)]
pub(crate) fn advance_fake_monotonic_ms_for_test(fence: u64, delta_ms: u64) {
    let mut overrides = overrides().lock().unwrap();
    let entry = overrides
        .entry(fence)
        .or_insert_with(blank_override_for_test);
    let now = entry.now_ms.unwrap_or(0);
    entry.now_ms = Some(now.saturating_add(delta_ms));
}

/// Replace one writer's deadline, so a test can watch a real timeout fire
/// without waiting out the full D.
#[cfg(test)]
pub(crate) fn set_deadline_ms_for_test(fence: u64, deadline_ms: u64) {
    let mut overrides = overrides().lock().unwrap();
    overrides
        .entry(fence)
        .or_insert_with(blank_override_for_test)
        .deadline_ms = Some(deadline_ms);
}

/// Drop every override for one fence, handing the writer back to the real clock.
#[cfg(test)]
pub(crate) fn clear_fence_overrides_for_test(fence: u64) {
    overrides().lock().unwrap().remove(&fence);
}
