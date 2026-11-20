//! Tests for the writer sync deadline.
//!
//! A healthy writer syncs every appended record within D (10 s), at any append
//! rate: the loop checks the deadline before every command on a monotonic
//! clock, wakes for it while dirty, and a dropped handle syncs dirty data
//! before the writer exits. The fake clock below is keyed by the writer's
//! fence, so one test's scripted time never disturbs another writer's real
//! deadline.
//!
//! Timing note: the loop stamps a deadline while handling an append, which
//! always trails the test's send. Tests that advance scripted time around
//! appends drain the loop first (a no-op round trip through the loop's own
//! FIFO), so the stamp the test reasons about has already happened. The
//! backlog test instead matches each record to its covering sync by sequence,
//! which stays exact however far handling trails the sends, and the clock
//! isolation test runs no writer at all.

use super::{WalCommand, WalHandle};
use crate::durability_counters::{
    advance_fake_monotonic_ms_for_test, clear_fence_overrides_for_test, monotonic_now_ms_for,
    set_deadline_ms_for_test, set_fake_monotonic_ms_for_test, SYNC_DEADLINE_MS,
};
use broca_wal::{FramedRecord, Wal, WalError, WalRecord};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;

const FENCE_CUT: u64 = 0xF2_0006;
const FENCE_CLOCK: u64 = 0xF2_0008;
/// Clears one fence's clock overrides when the test leaves the scope, so a
/// failure cannot leak scripted time into another test's writer.
struct OverrideGuard(u64);

impl OverrideGuard {
    fn live(fence: u64) -> Self {
        Self(fence)
    }
}

impl Drop for OverrideGuard {
    fn drop(&mut self) {
        clear_fence_overrides_for_test(self.0);
    }
}

/// A `Wal` that separates written-but-volatile appends from synced ones, so a
/// test can tell exactly when the deadline moved a record to durability. It
/// can also fail its syncs on demand, to prove the failure paths poison
/// without publishing.
struct ProbeState {
    appends: AtomicUsize,
    attempts: AtomicUsize,
    synced: Mutex<Vec<(u64, u64)>>,
    handled: Mutex<Vec<(u64, u64)>>,
    durable: Mutex<Vec<u64>>,
    volatile: Mutex<Vec<u64>>,
}

impl Default for ProbeState {
    fn default() -> Self {
        Self {
            appends: AtomicUsize::new(0),
            attempts: AtomicUsize::new(0),
            synced: Mutex::new(Vec::new()),
            handled: Mutex::new(Vec::new()),
            durable: Mutex::new(Vec::new()),
            volatile: Mutex::new(Vec::new()),
        }
    }
}

struct ProbeWal {
    fence: u64,
    next: u64,
    state: Arc<ProbeState>,
    succeed_first_n: usize,
}

impl ProbeWal {
    fn new(fence: u64, state: Arc<ProbeState>) -> Self {
        Self {
            fence,
            next: 0,
            state,
            succeed_first_n: usize::MAX,
        }
    }
}

impl Wal for ProbeWal {
    fn append(&mut self, _fence: u64, _record: &WalRecord, _sync: bool) -> Result<u64, WalError> {
        self.next += 1;
        let seq = self.next;
        self.state.appends.fetch_add(1, Ordering::SeqCst);
        self.state
            .handled
            .lock()
            .unwrap()
            .push((seq, monotonic_now_ms_for(self.fence)));
        self.state.volatile.lock().unwrap().push(seq);
        Ok(seq)
    }

    fn sync(&self) -> Result<(), WalError> {
        let attempt = self.state.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        if attempt > self.succeed_first_n {
            return Err(WalError::Io(std::io::Error::other(
                "injected fsync failure",
            )));
        }
        let mut volatile = self.state.volatile.lock().unwrap();
        let mut durable = self.state.durable.lock().unwrap();
        durable.extend(volatile.drain(..));
        self.state
            .synced
            .lock()
            .unwrap()
            .push((monotonic_now_ms_for(self.fence), durable.len() as u64));
        Ok(())
    }

    fn replay(&self) -> Result<Vec<FramedRecord>, WalError> {
        Ok(Vec::new())
    }

    fn tail_after_seq(&self, _after_seq: u64) -> Result<Vec<FramedRecord>, WalError> {
        Ok(Vec::new())
    }

    fn last_seq(&self) -> u64 {
        self.next
    }
}

async fn wait_for_appends(state: &Arc<ProbeState>, n: usize) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while state.appends.load(Ordering::SeqCst) < n {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("writer handled the append");
}

async fn wait_for_durable_head(handle: &WalHandle, seq: u64) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while handle.durable_head().load(Ordering::Acquire) < seq {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("durable head advanced");
}

/// One no-op round trip through the loop's own FIFO. Everything sent before
/// this has run when it answers: each earlier append has been handled, and
/// the writer has assigned its deadline while handling the one that made it
/// dirty.
async fn drain_writer(handle: &WalHandle) {
    let (ack_tx, ack_rx) = oneshot::channel();
    handle
        .tx
        .send(WalCommand::AppendDrained(ack_tx))
        .expect("writer loop is running");
    ack_rx.await.expect("writer answered the drain");
}

/// Wake the loop after scripted time moved past a deadline. The loop checks
/// the deadline at the top of its next iteration, after the wakeup is
/// handled, so two round trips guarantee the sync below ran.
async fn fire_deadline(handle: &WalHandle) {
    drain_writer(handle).await;
    drain_writer(handle).await;
}

/// Once the deadline has fired, the record survives every power-loss state:
/// the synced prefix, and the synced prefix with any in-order subset of later
/// unsynced writes. The full state enumeration over the filesystem shim is the
/// power-loss harness's own proof; this test verifies the deadline's half of
/// the argument, that the record reached durability in the first place.
#[tokio::test]
async fn power_loss_cut_after_deadline_keeps_synced_record() {
    let _guard = OverrideGuard::live(FENCE_CUT);
    set_fake_monotonic_ms_for_test(FENCE_CUT, 0);
    let state = Arc::new(ProbeState::default());
    let handle = WalHandle::spawn(Box::new(ProbeWal::new(FENCE_CUT, state.clone())), FENCE_CUT)
        .expect("spawn");
    handle
        .record(WalRecord::Unknown)
        .into_result()
        .expect("append accepted");
    wait_for_appends(&state, 1).await;
    drain_writer(&handle).await;
    // Before its deadline the record is volatile: a cut now may lose it.
    assert!(state.durable.lock().unwrap().is_empty());
    set_fake_monotonic_ms_for_test(FENCE_CUT, SYNC_DEADLINE_MS);
    fire_deadline(&handle).await;
    wait_for_durable_head(&handle, 1).await;
    // A later unsynced append must not endanger the synced prefix.
    handle
        .record(WalRecord::Unknown)
        .into_result()
        .expect("append accepted");
    wait_for_appends(&state, 2).await;
    drain_writer(&handle).await;
    let durable = state.durable.lock().unwrap().clone();
    let volatile = state.volatile.lock().unwrap().clone();
    assert_eq!(durable, vec![1]);
    assert_eq!(volatile, vec![2]);
    for kept in 0..=volatile.len() {
        let mut surviving = durable.clone();
        surviving.extend(volatile[..kept].iter().copied());
        assert!(
            surviving.contains(&1),
            "the synced record survives every cut state"
        );
    }
    handle.shutdown().await.expect("shutdown");
}

/// The fake clock only moves the fence that set it.
#[test]
fn fake_clock_is_keyed_by_fence() {
    use crate::durability_counters::{deadline_ms_for, SYNC_DEADLINE_MS as D};

    const FAKER: u64 = 9_999_999_999;
    let _guard = OverrideGuard::live(FENCE_CLOCK);
    set_fake_monotonic_ms_for_test(FENCE_CLOCK, FAKER);
    set_deadline_ms_for_test(FENCE_CLOCK, 50);
    assert_eq!(monotonic_now_ms_for(FENCE_CLOCK), FAKER);
    assert_eq!(deadline_ms_for(FENCE_CLOCK), 50);
    // A fence with no test override uses the real monotonic clock and the
    // default deadline D: no real uptime reaches the scripted value.
    assert_eq!(deadline_ms_for(0xF2_FFFF), D);
    let untouched = monotonic_now_ms_for(0xF2_FFFF);
    assert_ne!(untouched, FAKER, "the fake value leaked onto another fence");
    assert!(
        untouched < 1_000_000,
        "untouched fences read the real clock"
    );
    advance_fake_monotonic_ms_for_test(FENCE_CLOCK, 100);
    assert_eq!(monotonic_now_ms_for(FENCE_CLOCK), FAKER + 100);
    assert_eq!(deadline_ms_for(FENCE_CLOCK), 50);
}
