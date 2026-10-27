//! Tests for the writer sync deadline.
//!
//! A healthy writer syncs every appended record within D (10 s), at any append
//! rate: the loop checks the deadline before every command on a monotonic
//! clock, wakes for it while dirty, and a dropped handle syncs dirty data
//! before the writer exits. The fake clock below is keyed by the writer's
//! fence, so one test's scripted time never disturbs another writer's real
//! deadline.

use super::{writer_loop, WalCommand, WalHandle};
use crate::durability_counters::{
    advance_fake_monotonic_ms_for_test, clear_fence_overrides_for_test, counts_for_fence,
    monotonic_now_ms_for, set_deadline_ms_for_test, set_fake_monotonic_ms_for_test, FsyncCause,
    SYNC_DEADLINE_MS,
};
use broca_wal::{FramedRecord, Wal, WalError, WalRecord};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;

const FENCE_ISOLATED: u64 = 0xF2_0001;
const FENCE_IDLE: u64 = 0xF2_0002;
const FENCE_BACKLOG: u64 = 0xF2_0003;
const FENCE_BARRIER: u64 = 0xF2_0004;
const FENCE_DROPPED: u64 = 0xF2_0005;
const FENCE_CUT: u64 = 0xF2_0006;
const FENCE_TIMEOUT: u64 = 0xF2_0007;

/// A `Wal` that separates written-but-volatile appends from synced ones, so a
/// test can tell exactly when the deadline moved a record to durability.
#[derive(Default)]
struct ProbeState {
    appends: AtomicUsize,
    syncs: AtomicUsize,
    sync_fake_times: Mutex<Vec<u64>>,
    handled: Mutex<Vec<(u64, u64)>>,
    durable: Mutex<Vec<u64>>,
    volatile: Mutex<Vec<u64>>,
}

struct ProbeWal {
    fence: u64,
    next: u64,
    state: Arc<ProbeState>,
}

impl ProbeWal {
    fn new(fence: u64, state: Arc<ProbeState>) -> Self {
        Self {
            fence,
            next: 0,
            state,
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
        self.state.syncs.fetch_add(1, Ordering::SeqCst);
        self.state
            .sync_fake_times
            .lock()
            .unwrap()
            .push(monotonic_now_ms_for(self.fence));
        let mut volatile = self.state.volatile.lock().unwrap();
        let mut durable = self.state.durable.lock().unwrap();
        durable.extend(volatile.drain(..));
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

/// Wake the loop with its test-only no-op. The deadline check before that
/// command is what the isolated test exercises.
async fn ping_writer(handle: &WalHandle) {
    let (ack_tx, ack_rx) = oneshot::channel();
    handle
        .tx
        .send(WalCommand::AppendDrained(ack_tx))
        .expect("writer loop is running");
    ack_rx.await.expect("writer answered the drain");
}

/// A single append with no barrier behind it is synced once its deadline fires.
#[tokio::test]
async fn deadline_syncs_isolated_append() {
    set_fake_monotonic_ms_for_test(FENCE_ISOLATED, 0);
    let state = Arc::new(ProbeState::default());
    let handle = WalHandle::spawn(
        Box::new(ProbeWal::new(FENCE_ISOLATED, state.clone())),
        FENCE_ISOLATED,
    )
    .expect("spawn");
    let base_syncs = state.syncs.load(Ordering::SeqCst);
    handle
        .record(WalRecord::Unknown)
        .into_result()
        .expect("append accepted");
    wait_for_appends(&state, 1).await;
    // Move the fake clock to the deadline and wake the loop. Nothing else
    // arrives, so only the before-command check can sync.
    set_fake_monotonic_ms_for_test(FENCE_ISOLATED, SYNC_DEADLINE_MS);
    ping_writer(&handle).await;
    assert_eq!(
        state.syncs.load(Ordering::SeqCst),
        base_syncs + 1,
        "the fired deadline synced exactly once"
    );
    assert_eq!(handle.durable_head().load(Ordering::Acquire), 1);
    assert_eq!(
        state.sync_fake_times.lock().unwrap().last(),
        Some(&SYNC_DEADLINE_MS),
        "the sync ran at the deadline, not before it"
    );
    let counts = counts_for_fence(FENCE_ISOLATED);
    assert_eq!(counts.get(FsyncCause::Deadline), 1);
    assert_eq!(counts.get(FsyncCause::Barrier), 0);
    clear_fence_overrides_for_test(FENCE_ISOLATED);
    handle.shutdown().await.expect("shutdown");
}

/// A writer with nothing dirty issues no fsync no matter how far the clock runs.
#[tokio::test]
async fn idle_writer_never_syncs() {
    set_fake_monotonic_ms_for_test(FENCE_IDLE, 0);
    let state = Arc::new(ProbeState::default());
    let handle = WalHandle::spawn(
        Box::new(ProbeWal::new(FENCE_IDLE, state.clone())),
        FENCE_IDLE,
    )
    .expect("spawn");
    let base_syncs = state.syncs.load(Ordering::SeqCst);
    // Run far past several deadlines, waking the loop each time so the
    // deadline check itself runs and must find nothing dirty.
    for step in 1..=5 {
        set_fake_monotonic_ms_for_test(FENCE_IDLE, step * SYNC_DEADLINE_MS);
        ping_writer(&handle).await;
    }
    assert_eq!(
        state.syncs.load(Ordering::SeqCst),
        base_syncs,
        "an idle writer issues no fsync"
    );
    assert_eq!(handle.durable_head().load(Ordering::Acquire), 0);
    assert_eq!(counts_for_fence(FENCE_IDLE).total(), 0);
    clear_fence_overrides_for_test(FENCE_IDLE);
    handle.shutdown().await.expect("shutdown");
}

/// Across 120 simulated seconds with a never-empty queue and no barrier, every
/// record is synced within D plus one feeder step.
#[tokio::test]
async fn deadline_holds_under_sustained_backlog() {
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
        let syncs = state.sync_fake_times.lock().unwrap();
        assert_eq!(handled.len(), total);
        for (seq, at) in handled.iter() {
            let cover = syncs.partition_point(|ts| *ts < *at);
            assert!(
                cover < syncs.len(),
                "seq {seq} appended at {at} was never synced"
            );
            assert!(
                syncs[cover] - at <= SYNC_DEADLINE_MS + 1_000,
                "seq {seq} appended at {at} synced at {}, past D plus one step",
                syncs[cover]
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
    clear_fence_overrides_for_test(FENCE_BACKLOG);
    handle.shutdown().await.expect("shutdown");
}

/// An explicit barrier still costs exactly one fsync and returns promptly: the
/// deadline machinery adds no work to the latency it never asked for.
#[tokio::test]
async fn barrier_latency_unchanged() {
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
    assert_eq!(state.syncs.load(Ordering::SeqCst), 2);
    let counts = counts_for_fence(FENCE_BARRIER);
    assert_eq!(counts.get(FsyncCause::Barrier), 1);
    assert_eq!(counts.get(FsyncCause::Deadline), 0);
    handle.shutdown().await.expect("shutdown");
}

/// Dropping the handle while dirty syncs before the writer exits. The loop used
/// to leave on a disconnect with no sync; this test drives the loop directly
/// so the disconnect is real rather than a shutdown in disguise.
#[test]
fn dropped_handle_syncs_dirty_writer() {
    let state = Arc::new(ProbeState::default());
    let (tx, rx) = std::sync::mpsc::channel();
    let poisoned = Arc::new(Mutex::new(None));
    let durable_head = Arc::new(AtomicU64::new(0));
    let lineage = Arc::new(Mutex::new(None));
    let wal = Box::new(ProbeWal::new(FENCE_DROPPED, state.clone()));
    let writer = std::thread::spawn(move || {
        writer_loop(
            wal,
            FENCE_DROPPED,
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
        state.syncs.load(Ordering::SeqCst),
        1,
        "the disconnect synced the dirty writer"
    );
    assert_eq!(*state.durable.lock().unwrap(), vec![1]);
    assert_eq!(
        counts_for_fence(FENCE_DROPPED).get(FsyncCause::Disconnect),
        1
    );
}

/// Once the deadline has fired, the record survives every power-loss state:
///
/// the synced prefix, and the synced prefix with any in-order subset of later
/// unsynced writes. The full state enumeration over the filesystem shim is the
/// power-loss harness's own proof; this test verifies the deadline's half of
/// the argument, that the record reached durability in the first place.
#[tokio::test]
async fn power_loss_cut_after_deadline_keeps_synced_record() {
    set_fake_monotonic_ms_for_test(FENCE_CUT, 0);
    let state = Arc::new(ProbeState::default());
    let handle = WalHandle::spawn(Box::new(ProbeWal::new(FENCE_CUT, state.clone())), FENCE_CUT)
        .expect("spawn");
    handle
        .record(WalRecord::Unknown)
        .into_result()
        .expect("append accepted");
    wait_for_appends(&state, 1).await;
    // Before its deadline the record is volatile: a cut now may lose it.
    assert!(state.durable.lock().unwrap().is_empty());
    set_fake_monotonic_ms_for_test(FENCE_CUT, SYNC_DEADLINE_MS);
    ping_writer(&handle).await;
    wait_for_durable_head(&handle, 1).await;
    // A later unsynced append must not endanger the synced prefix.
    handle
        .record(WalRecord::Unknown)
        .into_result()
        .expect("append accepted");
    wait_for_appends(&state, 2).await;
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
    clear_fence_overrides_for_test(FENCE_CUT);
    handle.shutdown().await.expect("shutdown");
}

/// With no command arriving at all, the bounded wait itself fires and syncs.
#[tokio::test]
async fn deadline_timeout_syncs_without_wakeup() {
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
    assert_eq!(state.syncs.load(Ordering::SeqCst), 2);
    let counts = counts_for_fence(FENCE_TIMEOUT);
    assert_eq!(counts.get(FsyncCause::Deadline), 1);
    assert_eq!(counts.get(FsyncCause::Barrier), 0);
    clear_fence_overrides_for_test(FENCE_TIMEOUT);
    handle.shutdown().await.expect("shutdown");
}

/// The fake clock only moves the fence that set it.
#[test]
fn fake_clock_is_keyed_by_fence() {
    use crate::durability_counters::{deadline_ms_for, SYNC_DEADLINE_MS as D};

    set_fake_monotonic_ms_for_test(FENCE_CUT, 1234);
    set_deadline_ms_for_test(FENCE_CUT, 50);
    assert_eq!(monotonic_now_ms_for(FENCE_CUT), 1234);
    assert_eq!(deadline_ms_for(FENCE_CUT), 50);
    // A writer that registered nothing keeps the real clock and the real D.
    assert_eq!(deadline_ms_for(0xF2_FFFF), D);
    let real = monotonic_now_ms_for(0xF2_FFFF);
    assert!(real < 1_000_000, "untouched fences read the real clock");
    advance_fake_monotonic_ms_for_test(FENCE_CUT, 100);
    assert_eq!(monotonic_now_ms_for(FENCE_CUT), 1334);
    clear_fence_overrides_for_test(FENCE_CUT);
    assert_eq!(deadline_ms_for(FENCE_CUT), D);
}
