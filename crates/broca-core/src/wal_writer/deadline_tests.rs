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

use super::{writer_loop, RecordObserver, WalCommand, WalHandle};
use crate::durability_counters::{
    advance_fake_monotonic_ms_for_test, clear_fence_overrides_for_test, counts_for_fence,
    monotonic_now_ms_for, set_deadline_ms_for_test, set_fake_monotonic_ms_for_test, FsyncCause,
    SYNC_DEADLINE_MS,
};
use broca_wal::{FramedRecord, Wal, WalError, WalLineageId, WalRecord};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{broadcast, oneshot};

const FENCE_ISOLATED: u64 = 0xF2_0001;
const FENCE_IDLE: u64 = 0xF2_0002;
const FENCE_BACKLOG: u64 = 0xF2_0003;
const FENCE_BARRIER: u64 = 0xF2_0004;
const FENCE_RAW_DISCONNECT: u64 = 0xF2_0005;
const FENCE_CUT: u64 = 0xF2_0006;
const FENCE_TIMEOUT: u64 = 0xF2_0007;
const FENCE_CLOCK: u64 = 0xF2_0008;
const FENCE_NONSLIDE: u64 = 0xF2_0009;
const FENCE_FAIL_DEADLINE: u64 = 0xF2_000A;
const FENCE_FAIL_DISCONNECT: u64 = 0xF2_000B;
const FENCE_SHUTDOWN_COUNT: u64 = 0xF2_000C;
const FENCE_DROP: u64 = 0xF2_000D;

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

    fn failing_after(fence: u64, state: Arc<ProbeState>, succeed_first_n: usize) -> Self {
        Self {
            fence,
            next: 0,
            state,
            succeed_first_n,
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

/// A single append with no barrier behind it is synced once its deadline fires.
#[tokio::test]
async fn deadline_syncs_isolated_append() {
    let _guard = OverrideGuard::live(FENCE_ISOLATED);
    set_fake_monotonic_ms_for_test(FENCE_ISOLATED, 0);
    let state = Arc::new(ProbeState::default());
    let handle = WalHandle::spawn(
        Box::new(ProbeWal::new(FENCE_ISOLATED, state.clone())),
        FENCE_ISOLATED,
    )
    .expect("spawn");
    let base_attempts = state.attempts.load(Ordering::SeqCst);
    handle
        .record(WalRecord::Unknown)
        .into_result()
        .expect("append accepted");
    wait_for_appends(&state, 1).await;
    drain_writer(&handle).await;
    // Nothing else arrives, so only the before-command check can sync.
    set_fake_monotonic_ms_for_test(FENCE_ISOLATED, SYNC_DEADLINE_MS);
    fire_deadline(&handle).await;
    assert_eq!(
        state.attempts.load(Ordering::SeqCst),
        base_attempts + 1,
        "the fired deadline synced exactly once"
    );
    assert_eq!(handle.durable_head().load(Ordering::Acquire), 1);
    assert_eq!(
        state.synced.lock().unwrap().last(),
        Some(&(SYNC_DEADLINE_MS, 1)),
        "the sync ran at the deadline, not before it"
    );
    let counts = counts_for_fence(FENCE_ISOLATED);
    assert_eq!(counts.get(FsyncCause::Deadline), 1);
    assert_eq!(counts.get(FsyncCause::Barrier), 0);
    handle.shutdown().await.expect("shutdown");
}

/// A writer with nothing dirty issues no fsync no matter how far the clock runs.
#[tokio::test]
async fn idle_writer_never_syncs() {
    let _guard = OverrideGuard::live(FENCE_IDLE);
    set_fake_monotonic_ms_for_test(FENCE_IDLE, 0);
    let state = Arc::new(ProbeState::default());
    let handle = WalHandle::spawn(
        Box::new(ProbeWal::new(FENCE_IDLE, state.clone())),
        FENCE_IDLE,
    )
    .expect("spawn");
    let base_attempts = state.attempts.load(Ordering::SeqCst);
    // Run far past several deadlines, waking the loop each time so the
    // deadline check itself runs and must find nothing dirty.
    for step in 1..=5 {
        set_fake_monotonic_ms_for_test(FENCE_IDLE, step * SYNC_DEADLINE_MS);
        drain_writer(&handle).await;
    }
    assert_eq!(
        state.attempts.load(Ordering::SeqCst),
        base_attempts,
        "an idle writer issues no fsync"
    );
    assert_eq!(handle.durable_head().load(Ordering::Acquire), 0);
    assert_eq!(counts_for_fence(FENCE_IDLE).total(), 0);
    handle.shutdown().await.expect("shutdown");
}

/// Later appends must not slide the deadline: an append at 0 and another just
/// before D are both durable after a single sync at D.
#[tokio::test]
async fn deadline_does_not_slide_with_later_appends() {
    let _guard = OverrideGuard::live(FENCE_NONSLIDE);
    set_fake_monotonic_ms_for_test(FENCE_NONSLIDE, 0);
    let state = Arc::new(ProbeState::default());
    let handle = WalHandle::spawn(
        Box::new(ProbeWal::new(FENCE_NONSLIDE, state.clone())),
        FENCE_NONSLIDE,
    )
    .expect("spawn");
    let base_attempts = state.attempts.load(Ordering::SeqCst);
    handle
        .record(WalRecord::Unknown)
        .into_result()
        .expect("append accepted");
    wait_for_appends(&state, 1).await;
    drain_writer(&handle).await;
    set_fake_monotonic_ms_for_test(FENCE_NONSLIDE, SYNC_DEADLINE_MS - 1);
    handle
        .record(WalRecord::Unknown)
        .into_result()
        .expect("append accepted");
    wait_for_appends(&state, 2).await;
    drain_writer(&handle).await;
    // A sliding deadline would wait for almost another D; the original one
    // fires now and covers both records in one sync.
    set_fake_monotonic_ms_for_test(FENCE_NONSLIDE, SYNC_DEADLINE_MS);
    fire_deadline(&handle).await;
    wait_for_durable_head(&handle, 2).await;
    assert_eq!(
        state.attempts.load(Ordering::SeqCst),
        base_attempts + 1,
        "one deadline sync covered both appends"
    );
    assert_eq!(*state.durable.lock().unwrap(), vec![1, 2]);
    assert_eq!(
        counts_for_fence(FENCE_NONSLIDE).get(FsyncCause::Deadline),
        1
    );
    handle.shutdown().await.expect("shutdown");
}

/// Across 120 simulated seconds with a never-empty queue and no barrier, every
/// record is synced within D plus one feeder step. Coverage is matched by
/// sequence — the first sync covering each record — rather than by timestamp.
#[tokio::test]
async fn deadline_holds_under_sustained_backlog() {
    let _guard = OverrideGuard::live(FENCE_BACKLOG);
    set_fake_monotonic_ms_for_test(FENCE_BACKLOG, 0);
    let state = Arc::new(ProbeState::default());
    let handle = WalHandle::spawn(
        Box::new(ProbeWal::new(FENCE_BACKLOG, state.clone())),
        FENCE_BACKLOG,
    )
    .expect("spawn");
    // One feeder step of scripted time: enough appends to keep the loop's queue
    // from draining, then a real breath so the loop runs before time advances.
    const STEP_MS: u64 = 100;
    const STEPS: u64 = 1200;
    const BATCH: usize = 50;
    for step in 0..STEPS {
        set_fake_monotonic_ms_for_test(FENCE_BACKLOG, step * STEP_MS);
        for _ in 0..BATCH {
            handle
                .record(WalRecord::Unknown)
                .into_result()
                .expect("append accepted");
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let total = STEPS as usize * BATCH;
    wait_for_appends(&state, total).await;
    // The run's first barrier flushes the tail; it must also be its last word
    // on durability, because the deadline covered everything before it.
    let head = handle.sync().await.expect("final barrier");
    assert_eq!(head, total as u64);
    {
        let handled = state.handled.lock().unwrap();
        let synced = state.synced.lock().unwrap();
        assert_eq!(handled.len(), total);
        for (seq, at) in handled.iter() {
            let cover = synced.partition_point(|(_, covered)| *covered < *seq);
            assert!(
                cover < synced.len(),
                "seq {seq} handled at {at} was never covered by a sync"
            );
            assert!(
                synced[cover].0 - at <= SYNC_DEADLINE_MS + 1_000,
                "seq {seq} handled at {at} covered at {}, past D plus one step",
                synced[cover].0
            );
        }
    }
    let counts = counts_for_fence(FENCE_BACKLOG);
    assert!(
        counts.get(FsyncCause::Deadline) >= 8,
        "the backlog synced on its deadline throughout: {counts:?}"
    );
    assert_eq!(
        counts.get(FsyncCause::Barrier),
        1,
        "no barrier ran during the backlog"
    );
    handle.shutdown().await.expect("shutdown");
}

/// An explicit barrier still costs exactly one fsync and returns promptly: the
/// deadline machinery adds no work to the latency it never asked for.
#[tokio::test]
async fn barrier_latency_unchanged() {
    let _guard = OverrideGuard::live(FENCE_BARRIER);
    let state = Arc::new(ProbeState::default());
    let handle = WalHandle::spawn(
        Box::new(ProbeWal::new(FENCE_BARRIER, state.clone())),
        FENCE_BARRIER,
    )
    .expect("spawn");
    handle
        .record(WalRecord::Unknown)
        .into_result()
        .expect("append accepted");
    let start = std::time::Instant::now();
    let head = handle.sync().await.expect("barrier");
    let elapsed = start.elapsed();
    assert_eq!(head, 1);
    assert!(
        elapsed < Duration::from_secs(5),
        "a barrier returns promptly without deadline interference: {elapsed:?}"
    );
    // Recovery plus the one barrier: no deadline path ran.
    assert_eq!(state.attempts.load(Ordering::SeqCst), 2);
    let counts = counts_for_fence(FENCE_BARRIER);
    assert_eq!(counts.get(FsyncCause::Barrier), 1);
    assert_eq!(counts.get(FsyncCause::Deadline), 0);
    handle.shutdown().await.expect("shutdown");
}

/// A clean shutdown's final sync counts as a shutdown, not a barrier.
#[tokio::test]
async fn dirty_shutdown_counts_shutdown_cause() {
    let _guard = OverrideGuard::live(FENCE_SHUTDOWN_COUNT);
    let state = Arc::new(ProbeState::default());
    let handle = WalHandle::spawn(
        Box::new(ProbeWal::new(FENCE_SHUTDOWN_COUNT, state.clone())),
        FENCE_SHUTDOWN_COUNT,
    )
    .expect("spawn");
    handle
        .record(WalRecord::Unknown)
        .into_result()
        .expect("append accepted");
    wait_for_appends(&state, 1).await;
    handle.shutdown().await.expect("shutdown");
    assert_eq!(state.attempts.load(Ordering::SeqCst), 2);
    let counts = counts_for_fence(FENCE_SHUTDOWN_COUNT);
    assert_eq!(counts.get(FsyncCause::Shutdown), 1);
    assert_eq!(counts.get(FsyncCause::Barrier), 0);
    assert_eq!(counts.get(FsyncCause::Deadline), 0);
}

/// A failed deadline sync poisons the writer without publishing anything, and
/// later wakeups never retry the doomed sync.
#[tokio::test]
async fn failed_deadline_sync_poisons_without_publishing_or_retrying() {
    let _guard = OverrideGuard::live(FENCE_FAIL_DEADLINE);
    set_fake_monotonic_ms_for_test(FENCE_FAIL_DEADLINE, 0);
    let state = Arc::new(ProbeState::default());
    let (tx, mut rx) = broadcast::channel(8);
    let observer = RecordObserver::new(tx, Arc::new(AtomicUsize::new(1)));
    let handle = WalHandle::spawn_observed(
        Box::new(ProbeWal::failing_after(
            FENCE_FAIL_DEADLINE,
            state.clone(),
            1,
        )),
        FENCE_FAIL_DEADLINE,
        Some(observer),
    )
    .expect("spawn");
    // The recovery proof is the one sync allowed to succeed.
    handle
        .record(WalRecord::Unknown)
        .into_result()
        .expect("append accepted");
    wait_for_appends(&state, 1).await;
    drain_writer(&handle).await;
    assert!(
        matches!(rx.try_recv(), Err(broadcast::error::TryRecvError::Empty)),
        "an unsynced append stays invisible to subscribers"
    );
    set_fake_monotonic_ms_for_test(FENCE_FAIL_DEADLINE, SYNC_DEADLINE_MS);
    fire_deadline(&handle).await;
    assert_eq!(
        state.attempts.load(Ordering::SeqCst),
        2,
        "the deadline attempted its one sync"
    );
    assert!(
        handle.poison_error().is_some(),
        "the failed sync poisoned the writer"
    );
    assert_eq!(
        handle.durable_head().load(Ordering::Acquire),
        0,
        "the failed sync published nothing"
    );
    assert!(
        matches!(rx.try_recv(), Err(broadcast::error::TryRecvError::Empty)),
        "the failed sync delivered nothing to subscribers"
    );
    assert!(state.durable.lock().unwrap().is_empty());
    // Wakeups after the failure must not retry: the writer idles until close.
    set_fake_monotonic_ms_for_test(FENCE_FAIL_DEADLINE, 3 * SYNC_DEADLINE_MS);
    fire_deadline(&handle).await;
    assert_eq!(
        state.attempts.load(Ordering::SeqCst),
        2,
        "no wakeup retried the poisoned sync"
    );
    assert_eq!(
        counts_for_fence(FENCE_FAIL_DEADLINE).get(FsyncCause::Deadline),
        1
    );
    let _ = handle.shutdown().await;
}

/// A raw channel disconnect — the loop's receiver going away with no Shutdown
/// command — syncs dirty data before the writer exits. `WalHandle::drop`
/// always sends Shutdown, so only a test driving the loop directly sees this
/// path.
#[test]
fn raw_disconnect_without_shutdown_syncs_dirty_writer() {
    let _guard = OverrideGuard::live(FENCE_RAW_DISCONNECT);
    let state = Arc::new(ProbeState::default());
    let (tx, rx) = std::sync::mpsc::channel();
    let poisoned = Arc::new(Mutex::new(None));
    let durable_head = Arc::new(AtomicU64::new(0));
    let lineage: Arc<Mutex<Option<WalLineageId>>> = Arc::new(Mutex::new(None));
    let wal = Box::new(ProbeWal::new(FENCE_RAW_DISCONNECT, state.clone()));
    let writer = std::thread::spawn(move || {
        writer_loop(
            wal,
            FENCE_RAW_DISCONNECT,
            rx,
            None,
            poisoned,
            durable_head,
            lineage,
        );
    });
    tx.send(WalCommand::Append {
        seq: 1,
        record: Box::new(WalRecord::Unknown),
        frame: None,
    })
    .expect("writer loop is running");
    // Wait until the append is on the writer: the point is a dirty disconnect,
    // not a race with an empty queue.
    let give_up = std::time::Instant::now() + Duration::from_secs(10);
    while state.appends.load(Ordering::SeqCst) < 1 {
        assert!(
            std::time::Instant::now() < give_up,
            "writer never handled the append"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    drop(tx);
    writer.join().expect("writer thread joined");
    assert_eq!(
        state.attempts.load(Ordering::SeqCst),
        1,
        "the disconnect synced the dirty writer"
    );
    assert_eq!(*state.durable.lock().unwrap(), vec![1]);
    assert_eq!(
        counts_for_fence(FENCE_RAW_DISCONNECT).get(FsyncCause::Disconnect),
        1
    );
}

/// A failed disconnect sync still poisons and exits, publishing nothing.
#[test]
fn failed_disconnect_sync_poisons_without_publishing() {
    let _guard = OverrideGuard::live(FENCE_FAIL_DISCONNECT);
    let state = Arc::new(ProbeState::default());
    let (tx, rx) = std::sync::mpsc::channel();
    let poisoned = Arc::new(Mutex::new(None));
    let poisoned_for_assert = poisoned.clone();
    let durable_head = Arc::new(AtomicU64::new(0));
    let head_for_assert = durable_head.clone();
    let lineage: Arc<Mutex<Option<WalLineageId>>> = Arc::new(Mutex::new(None));
    let wal = Box::new(ProbeWal::failing_after(
        FENCE_FAIL_DISCONNECT,
        state.clone(),
        0,
    ));
    let writer = std::thread::spawn(move || {
        writer_loop(
            wal,
            FENCE_FAIL_DISCONNECT,
            rx,
            None,
            poisoned,
            durable_head,
            lineage,
        );
    });
    tx.send(WalCommand::Append {
        seq: 1,
        record: Box::new(WalRecord::Unknown),
        frame: None,
    })
    .expect("writer loop is running");
    let give_up = std::time::Instant::now() + Duration::from_secs(10);
    while state.appends.load(Ordering::SeqCst) < 1 {
        assert!(
            std::time::Instant::now() < give_up,
            "writer never handled the append"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    drop(tx);
    writer.join().expect("writer thread joined");
    assert_eq!(
        state.attempts.load(Ordering::SeqCst),
        1,
        "the disconnect attempted its one sync"
    );
    assert!(
        poisoned_for_assert.lock().unwrap().is_some(),
        "the failed sync poisoned the writer"
    );
    assert_eq!(head_for_assert.load(Ordering::Acquire), 0);
    assert!(state.durable.lock().unwrap().is_empty());
    assert_eq!(
        counts_for_fence(FENCE_FAIL_DISCONNECT).get(FsyncCause::Disconnect),
        1
    );
}

/// Dropping a dirty `WalHandle` runs the shutdown path: the final sync lands
/// before the writer exits.
#[tokio::test]
async fn dropped_handle_shuts_down_and_syncs_dirty_data() {
    let _guard = OverrideGuard::live(FENCE_DROP);
    let state = Arc::new(ProbeState::default());
    let handle = WalHandle::spawn(
        Box::new(ProbeWal::new(FENCE_DROP, state.clone())),
        FENCE_DROP,
    )
    .expect("spawn");
    let head = handle.durable_head();
    handle
        .record(WalRecord::Unknown)
        .into_result()
        .expect("append accepted");
    wait_for_appends(&state, 1).await;
    drop(handle);
    assert_eq!(
        head.load(Ordering::Acquire),
        1,
        "dropping the handle synced its dirty data"
    );
    assert_eq!(state.attempts.load(Ordering::SeqCst), 2);
    assert_eq!(*state.durable.lock().unwrap(), vec![1]);
    assert_eq!(counts_for_fence(FENCE_DROP).get(FsyncCause::Shutdown), 1);
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

/// With no command arriving at all, the bounded wait itself fires and syncs.
#[tokio::test]
async fn deadline_timeout_syncs_without_wakeup() {
    let _guard = OverrideGuard::live(FENCE_TIMEOUT);
    set_deadline_ms_for_test(FENCE_TIMEOUT, 150);
    let state = Arc::new(ProbeState::default());
    let handle = WalHandle::spawn(
        Box::new(ProbeWal::new(FENCE_TIMEOUT, state.clone())),
        FENCE_TIMEOUT,
    )
    .expect("spawn");
    handle
        .record(WalRecord::Unknown)
        .into_result()
        .expect("append accepted");
    // Nothing else is sent: the wait bounded by the deadline must sync alone.
    wait_for_durable_head(&handle, 1).await;
    assert_eq!(state.attempts.load(Ordering::SeqCst), 2);
    let counts = counts_for_fence(FENCE_TIMEOUT);
    assert_eq!(counts.get(FsyncCause::Deadline), 1);
    assert_eq!(counts.get(FsyncCause::Barrier), 0);
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
