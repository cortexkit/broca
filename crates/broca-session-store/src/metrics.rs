//! Process-lifetime timings for the new session store and its run loop, not durability inputs.
//!
//! Snapshots read relaxed atomics independently: a phase finishing during a health read can be
//! partially reflected in that snapshot. Durations round up to one microsecond so fast completed
//! work remains distinguishable from a phase that has never run.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use serde::Serialize;

use crate::session::Sealed;

#[derive(Clone, Copy)]
pub enum Phase {
    Open,
    Append,
    Sync,
    Seal,
    MediaWait,
    /// Run-loop preparation through the final CallOptions handoff, excluding media waits.
    /// Provider rendering and transport are not included.
    RequestPrepare,
    MediaFold,
    Read,
    EntriesPage,
    Collection,
    CollectionSweep,
}

struct Spec {
    phase: Phase,
    name: &'static str,
    slow_us: Option<u64>,
}

// Append and media wait have counters but no slow-line policy. Page reads and sweep passes use
// the same budgets as their corresponding read and per-session collection work.
const PHASE_THRESHOLDS: [Spec; 11] = [
    Spec {
        phase: Phase::Open,
        name: "open",
        slow_us: Some(500_000),
    },
    Spec {
        phase: Phase::Append,
        name: "append",
        slow_us: None,
    },
    Spec {
        phase: Phase::Sync,
        name: "sync",
        slow_us: Some(100_000),
    },
    Spec {
        phase: Phase::Seal,
        name: "seal",
        slow_us: Some(500_000),
    },
    Spec {
        phase: Phase::MediaWait,
        name: "media_wait",
        slow_us: None,
    },
    Spec {
        phase: Phase::RequestPrepare,
        name: "request_prepare",
        slow_us: Some(50_000),
    },
    Spec {
        phase: Phase::MediaFold,
        name: "media_fold",
        slow_us: Some(1_000_000),
    },
    Spec {
        phase: Phase::Read,
        name: "read",
        slow_us: Some(200_000),
    },
    Spec {
        phase: Phase::EntriesPage,
        name: "entries_page",
        slow_us: Some(200_000),
    },
    Spec {
        phase: Phase::Collection,
        name: "collection",
        slow_us: Some(1_000_000),
    },
    Spec {
        phase: Phase::CollectionSweep,
        name: "collection_sweep",
        slow_us: Some(1_000_000),
    },
];

#[derive(Default)]
struct Counter {
    count: AtomicU64,
    total_us: AtomicU64,
    max_us: AtomicU64,
    last_us: AtomicU64,
}

impl Counter {
    const fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            total_us: AtomicU64::new(0),
            max_us: AtomicU64::new(0),
            last_us: AtomicU64::new(0),
        }
    }
}

static COUNTERS: [Counter; PHASE_THRESHOLDS.len()] =
    [const { Counter::new() }; PHASE_THRESHOLDS.len()];

#[derive(Default, Serialize)]
pub struct SealTotals {
    pub transcript_records: u64,
    pub ledger_records: u64,
    pub kept_records: u64,
    pub transcript_bytes: u64,
    pub ledger_bytes: u64,
    pub index_entries: u64,
    pub merged_entries: u64,
    pub forced_merges: u64,
}

static SEAL_TOTALS: [AtomicU64; 8] = [const { AtomicU64::new(0) }; 8];

/// Retains the work statistics of a successfully committed seal, including index merge work.
pub fn sealed(sealed: &Sealed) {
    for (counter, value) in SEAL_TOTALS.iter().zip([
        sealed.transcript_records as u64,
        sealed.ledger_records as u64,
        sealed.kept_records as u64,
        sealed.transcript_bytes,
        sealed.ledger_bytes,
        sealed.index_entries as u64,
        sealed.merged_entries,
        u64::from(sealed.forced_merges),
    ]) {
        counter.fetch_add(value, Relaxed);
    }
}

#[derive(Serialize)]
pub struct PhaseSnapshot {
    pub count: u64,
    pub total_us: u64,
    pub max_us: u64,
    pub last_us: u64,
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub sealed: Option<SealTotals>,
}

pub fn snapshot() -> BTreeMap<&'static str, PhaseSnapshot> {
    PHASE_THRESHOLDS
        .iter()
        .map(|spec| {
            let counter = &COUNTERS[spec.phase as usize];
            let sealed = matches!(spec.phase, Phase::Seal).then(|| SealTotals {
                transcript_records: SEAL_TOTALS[0].load(Relaxed),
                ledger_records: SEAL_TOTALS[1].load(Relaxed),
                kept_records: SEAL_TOTALS[2].load(Relaxed),
                transcript_bytes: SEAL_TOTALS[3].load(Relaxed),
                ledger_bytes: SEAL_TOTALS[4].load(Relaxed),
                index_entries: SEAL_TOTALS[5].load(Relaxed),
                merged_entries: SEAL_TOTALS[6].load(Relaxed),
                forced_merges: SEAL_TOTALS[7].load(Relaxed),
            });
            (
                spec.name,
                PhaseSnapshot {
                    count: counter.count.load(Relaxed),
                    total_us: counter.total_us.load(Relaxed),
                    max_us: counter.max_us.load(Relaxed),
                    last_us: counter.last_us.load(Relaxed),
                    sealed,
                },
            )
        })
        .collect()
}

pub struct Timer {
    phase: Phase,
    started: Option<Instant>,
    elapsed: Duration,
}

/// Finishes on every exit, including errors and cancelled futures. Address construction is lazy
/// so normal phases pay only for the monotonic clock and relaxed atomic updates.
pub struct ScopedTimer<F: FnOnce() -> Option<String>> {
    timer: Option<Timer>,
    session: Option<F>,
}

impl<F: FnOnce() -> Option<String>> ScopedTimer<F> {
    pub fn pause(&mut self) {
        self.timer.as_mut().unwrap().pause();
    }

    pub fn resume(&mut self) {
        self.timer.as_mut().unwrap().resume();
    }
}

impl<F: FnOnce() -> Option<String>> Drop for ScopedTimer<F> {
    fn drop(&mut self) {
        self.timer
            .take()
            .unwrap()
            .finish_with(self.session.take().unwrap());
    }
}

impl Phase {
    pub fn start(self) -> Timer {
        Timer {
            phase: self,
            started: Some(Instant::now()),
            elapsed: Duration::ZERO,
        }
    }
}

impl Timer {
    pub fn with_session<F: FnOnce() -> Option<String>>(self, session: F) -> ScopedTimer<F> {
        ScopedTimer {
            timer: Some(self),
            session: Some(session),
        }
    }

    /// Excludes a separately measured wait from this phase's time.
    pub fn pause(&mut self) {
        if let Some(started) = self.started.take() {
            self.elapsed += started.elapsed();
        }
    }

    pub fn resume(&mut self) {
        self.started = Some(Instant::now());
    }

    /// `session` is the already-hashed storage address. A sweep has no single session.
    pub fn finish(self, session: Option<&str>) {
        self.finish_with(|| session.map(str::to_owned));
    }

    /// Defers address hashing/allocation until a slow phase actually needs a log line.
    pub fn finish_with(self, session: impl FnOnce() -> Option<String>) {
        let elapsed = self.elapsed + self.started.map_or(Duration::ZERO, |start| start.elapsed());
        let elapsed_us = elapsed.as_micros().clamp(1, u128::from(u64::MAX)) as u64;
        let counter = &COUNTERS[self.phase as usize];
        counter.total_us.fetch_add(elapsed_us, Relaxed);
        counter.max_us.fetch_max(elapsed_us, Relaxed);
        counter.last_us.store(elapsed_us, Relaxed);
        counter.count.fetch_add(1, Relaxed);
        let spec = &PHASE_THRESHOLDS[self.phase as usize];
        if spec
            .slow_us
            .is_some_and(|limit| elapsed > Duration::from_micros(limit))
            && tracing::enabled!(target: "session_storage", tracing::Level::WARN)
        {
            tracing::warn!(target: "session_storage", session = session().as_deref(), phase = spec.name,
                elapsed_us, elapsed_ms = elapsed_us / 1000, "slow session storage phase");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_forced_slow_phase_emits_one_warn_and_normal_phases_stay_silent() {
        let capture = broca_test_support::LogCapture::new();
        capture.in_scope(|| {
            // Only this test can replace the monotonic start; shipping callers always use now.
            let mut slow = Phase::Read.start();
            slow.started = Some(Instant::now() - Duration::from_millis(201));
            slow.finish(Some("f65431d12b98e076"));
            Phase::Read
                .start()
                .finish_with(|| panic!("a normal phase must not build log fields"));
        });
        let events = capture.events();
        assert_eq!(events.len(), 1, "{events:?}");
        let warning = &events[0];
        assert_eq!(warning.level, tracing::Level::WARN);
        assert_eq!(warning.target, "session_storage");
        assert_eq!(warning.fields["phase"], "read");
        assert_eq!(warning.fields["session"], "f65431d12b98e076");
        assert!(warning.fields["elapsed_us"].parse::<u64>().unwrap() >= 201_000);
        assert!(warning.fields["elapsed_ms"].parse::<u64>().unwrap() >= 201);
        assert_eq!(snapshot()["read"].count, 2);
    }
}
