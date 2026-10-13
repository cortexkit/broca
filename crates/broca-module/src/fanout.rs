//! Control-stream fan-out + the attach barrier.
//!
//! subc is 1:1, so `broca` owns fan-out to multiple subscribers of one session's run.
//! The control stream is RELIABLE (durable + replayable), so its source of truth is the
//! WAL, never the in-process ring:
//!
//! - When at least one subscriber is active, or a subscriber is in the middle of attaching,
//!   the session writer forwards each appended record to a [`RecordObserver`]; otherwise it
//!   skips that extra work. This module turns those observed records into cursor-tagged
//!   [`ControlUnit`] values and broadcasts them to live subscribers, and WAL replay uses the
//!   same conversion so live and replayed control messages have the same format.
//! - Each subscriber has its own task draining the ring into the subscriber's sink. A slow
//!   subscriber lags the broadcast → it DETACHES (and resubscribes from its durable cursor,
//!   replaying the projection) — never blocking the run or co-subscribers, never silently
//!   dropping a control unit (the WAL is the source of truth).
//!
//! To avoid gaps and duplicates, attach in this order: register the live receiver, enable
//! live forwarding, sync the WAL to get the durable head, recover projection state through that
//! durable point, then drain the live receiver while skipping anything already covered. Historical
//! subscriptions recover state while replaying their requested units. Future-only subscriptions
//! restore a persisted state and project only its validated WAL suffix. Records written before
//! forwarding was enabled are covered by that durable recovery, and later records are already
//! waiting in the receiver, so each record is delivered once.
//!
//! NO-WRITER-CLONE INVARIANT: the record observer lives at the WRITER (the writer task
//! publishes), and the small register+sync step of the barrier runs in the ACTOR (which
//! owns the writer). A subscriber TASK holds only the ring receiver + a read-only WAL store
//! handle — NEVER the writer `Arc`. That keeps the actor the sole writer owner at close
//! (the `Arc::into_inner` sole-owner assert is the tripwire).

use broca_diagnostics::{spawn_blocking, JobKind};

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use broca_core::{
    advance_control_projection_state, project_control_with_state, ControlProjectionState, Cursor,
    SessionStore, WalStore,
};
use broca_protocol::caller::ControlUnit;
use broca_protocol::BindIdentity;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

/// One cursor-tagged control unit on the stream. The bounded Tokio broadcast channel shares this
/// tuple behind an [`Arc`] so each receiver clones a pointer, not the owned message/tool payload.
pub type ControlEvent = (Cursor, ControlUnit);

/// What a subscriber receives on the unified session stream. Per the module surface, one
/// `session.subscribe` carries BOTH the reliable+replayable control stream and the
/// live+lossy display stream.
///
/// `Control` is a cursor-tagged control unit (reliable; replayed on attach, deduped).
/// Both event variants share their owned payload so fan-out only clones an [`Arc`] per subscriber.
/// `Display` is a live display event (lossy; live-only, never replayed).
/// `Detached` means the subscriber fell behind the bounded control broadcast channel and must
/// resubscribe from its last cursor. A client never consumes a post-gap control event as contiguous.
/// Display lag is signalled in-band by a `DisplayGap` event, not a detach.
#[derive(Debug, Clone)]
pub enum Delivery {
    Expired {
        expired_at_ms: u64,
    },
    Control(Arc<ControlEvent>),
    Display(Arc<broca_protocol::display::DisplayEvent>),
    /// The subscriber could not replay the durable prefix; the serve adapter emits a typed
    /// stream Error frame instead of misreporting a clean empty stream.
    Error(String),
    Detached,
}

/// Where a subscriber attaches from.
#[derive(Debug, Clone, Copy)]
pub enum FromCursor {
    /// From the very start of the lineage (seq 1).
    Start,
    /// Strictly after this durable cursor (a resume from the client's last-processed
    /// position — at-least-once from here; re-delivery of `<= After` is the client's to
    /// ignore).
    After(Cursor),
}

impl FromCursor {
    /// Should an event at `cursor` be delivered for this attach point?
    fn includes(&self, cursor: Cursor) -> bool {
        match self {
            FromCursor::Start => true,
            FromCursor::After(c) => cursor > *c,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum AttachFrom {
    Cursor(FromCursor),
    Live,
}

/// The attach-barrier decision for a unit arriving on the LIVE ring (step 2 of the
/// subscriber task): deliver it ONLY if it's past the barrier head `h` AND past the
/// client's attach point. A unit with `cursor.wal_seq <= h` was already delivered by the
/// replay (step 1), so it's the DEDUP skip — without it, a record fsynced before `h` but
/// ring-published after the subscriber registered would be delivered twice. Pure so the
/// dedup is unit-testable in isolation (the integration stress proves it fires under real
/// concurrency; this proves the decision itself is correct).
fn deliver_from_ring(cursor: Cursor, h: u64, from: FromCursor) -> bool {
    cursor.wal_seq > h && from.includes(cursor)
}

/// How many live control events the ring buffers before a slow subscriber lags + detaches.
const RING_CAPACITY: usize = 1024;
/// The writer-to-projector queue is bounded independently from the subscriber ring. If the
/// projector falls behind, its receiver reports lag and detaches subscribers so they recover
/// through the existing WAL replay path.
const OBSERVER_RING_CAPACITY: usize = 1024;

/// How many live DISPLAY events the display ring buffers. Display is LOSSY by design — a
/// slow display subscriber drops frames (a `DisplayGap` marks the gap), it never detaches
/// or backpressures the run. Smaller than the control ring: display is a render
/// convenience, not durable.
const DISPLAY_RING_CAPACITY: usize = 256;

/// The fan-out: a control ring (fed by the projector from the writer's record observer)
/// and a display ring (fed by the run's live `out` display events). Owned by the actor;
/// subscribers attach via the actor's Subscribe command.
pub struct Fanout<W: WalStore> {
    approvals: Arc<crate::approval_state::ApprovalState>,
    ring: broadcast::Sender<Arc<ControlEvent>>,
    /// Signals that the bounded writer→projector queue dropped records. Registered as part of
    /// the attach barrier so an attaching subscriber cannot miss a detach signal.
    detach_ring: broadcast::Sender<()>,
    /// The LOSSY display ring. The actor forwards the run's live display events here; it is
    /// NEVER fed control (control is sourced only from the WAL projection, so it can't
    /// double-publish). Live-only, never replayed — a fresh display subscriber gets events
    /// from attach onward, with a `DisplayGap` on lag.
    display_ring: broadcast::Sender<Arc<broca_protocol::display::DisplayEvent>>,
    wal_store: Arc<W>,
    session: BindIdentity,
    /// Tracks both active subscribers and attaches that have registered a receiver but have
    /// not finished their replay barrier yet. Keeping the writer-side observer gate enabled
    /// until the spawned subscriber task exits prevents a concurrent last-detach from turning
    /// live observation off too early.
    gate: SubscriberGate,
    /// Seeds the live projector with the exact state produced while an attaching subscriber
    /// projects its WAL replay, plus that replay's durable head. The small state handoff keeps
    /// live projection continuous without sending or re-projecting the full WAL prefix.
    projection_seed: mpsc::UnboundedSender<ProjectionSeedMessage>,
    /// Live subscriber tasks: cancelled + joined at close (BEFORE the lease drops), so no
    /// subscriber route outlives the actor.
    subscribers: Vec<SubscriberTask>,
    /// Cancelled at close to stop the projector task.
    projector_cancel: CancellationToken,
    projector: Option<tokio::task::JoinHandle<()>>,
}

struct SubscriberTask {
    sink: mpsc::Sender<Delivery>,
    cancel: CancellationToken,
    handle: tokio::task::JoinHandle<()>,
}

struct ProjectionSeed {
    head: u64,
    state: ControlProjectionState,
    /// True only for the attach that changed the observer gate from empty to live.
    gate_reopen: bool,
    /// Identifies the 0 -> 1 gate transition that produced this seed.
    gate_tenure: u64,
}

enum ProjectionSeedMessage {
    State(ProjectionSeed),
    /// The gate-owning attach could not replay the WAL. A concurrent attach's deferred seed may
    /// take ownership so one failed subscriber cannot stall every other subscriber in the tenure.
    GateOwnerFailed {
        gate_tenure: u64,
    },
}

/// The gate is a bare subscriber COUNT and the writer observes `count > 0` directly.
/// A separate enabled-boolean protocol had a last-detach/attach race: the detaching
/// subscriber could observe itself last, then a new attach increments and enables,
/// then the stale detach's disable lands — gate off with a live subscriber attached
/// (a silent control-stream gap). A single counter cannot express that state: the
/// attach's increment makes the writer's `> 0` check true, and no delayed store can
/// undo an increment it never saw.
#[derive(Clone)]
struct SubscriberGate {
    slots: Arc<AtomicUsize>,
    /// Set before the first observer slot is published after an empty tenure. The projector
    /// consumes this signal before handling either the new seed or any newly observed record.
    reseed_pending: Arc<AtomicBool>,
    /// Monotonic identity for each transition from no observer slots to at least one.
    tenure: Arc<AtomicU64>,
    /// Serializes slot transitions so a new tenure is published before its first observer slot.
    transition: Arc<Mutex<()>>,
}

pub(crate) struct SubscriberSlot {
    gate: SubscriberGate,
    gate_reopen: bool,
    gate_tenure: u64,
    gate_owner_failure: Option<mpsc::UnboundedSender<ProjectionSeedMessage>>,
}

impl SubscriberGate {
    fn new() -> Self {
        Self {
            slots: Arc::new(AtomicUsize::new(0)),
            reseed_pending: Arc::new(AtomicBool::new(false)),
            tenure: Arc::new(AtomicU64::new(0)),
            transition: Arc::new(Mutex::new(())),
        }
    }

    fn observer_slots(&self) -> Arc<AtomicUsize> {
        self.slots.clone()
    }

    fn reseed_pending(&self) -> Arc<AtomicBool> {
        self.reseed_pending.clone()
    }

    fn tenure(&self) -> Arc<AtomicU64> {
        self.tenure.clone()
    }

    fn begin_attach(&self) -> SubscriberSlot {
        let _transition = self
            .transition
            .lock()
            .expect("subscriber gate lock poisoned");
        let gate_reopen = self.slots.load(Ordering::Acquire) == 0;
        let gate_tenure = if gate_reopen {
            // Publish both the reseed request and its identity BEFORE making the observer gate
            // nonzero. A record from the new tenure can therefore only be projected after the
            // matching seed handshake begins.
            self.reseed_pending.store(true, Ordering::Release);
            // An explicit compare-exchange loop rather than `fetch_update`, which Rust 1.99
            // deprecates in favour of `try_update`, a method older toolchains don't have.
            let mut tenure = self.tenure.load(Ordering::Acquire);
            loop {
                let next = tenure
                    .checked_add(1)
                    .expect("subscriber gate tenure overflowed");
                match self.tenure.compare_exchange_weak(
                    tenure,
                    next,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break next,
                    Err(actual) => tenure = actual,
                }
            }
        } else {
            self.tenure.load(Ordering::Acquire)
        };
        let previous = self.slots.fetch_add(1, Ordering::AcqRel);
        debug_assert_eq!(previous == 0, gate_reopen);
        SubscriberSlot {
            gate: self.clone(),
            gate_reopen,
            gate_tenure,
            gate_owner_failure: None,
        }
    }

    fn release_slot(&self) {
        let _transition = self
            .transition
            .lock()
            .expect("subscriber gate lock poisoned");
        let prev = self.slots.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(prev > 0, "subscriber gate released without a slot");
    }
}

impl SubscriberSlot {
    fn arm_gate_owner_failure(&mut self, sender: mpsc::UnboundedSender<ProjectionSeedMessage>) {
        if self.gate_reopen {
            self.gate_owner_failure = Some(sender);
        }
    }

    fn projection_seed_handed_off(&mut self) {
        self.gate_owner_failure = None;
    }

    fn report_gate_owner_failure(&mut self) {
        if let Some(sender) = self.gate_owner_failure.take() {
            let _ = sender.send(ProjectionSeedMessage::GateOwnerFailed {
                gate_tenure: self.gate_tenure,
            });
        }
    }
}

impl Drop for SubscriberSlot {
    fn drop(&mut self) {
        // The attach may fail after it publishes the gate but before its subscriber task starts.
        // Report an unconsumed gate reopen so a concurrent seed can finish the handshake.
        self.report_gate_owner_failure();
        self.gate.release_slot();
    }
}

impl<W: WalStore + 'static> Fanout<W> {
    /// Build the fan-out for a session: returns the fan-out and the [`RecordObserver`] to
    /// pass to the session writer (`WalHandle::spawn_observed`). The projector task drains
    /// the observer, projects each record, and broadcasts the units on the ring.
    pub fn new(wal_store: Arc<W>, session: BindIdentity) -> (Self, broca_core::RecordObserver) {
        let approvals = Arc::new(crate::approval_state::ApprovalState::default());
        let (ring, _) = broadcast::channel(RING_CAPACITY);
        let (display_ring, _) = broadcast::channel(DISPLAY_RING_CAPACITY);
        let (detach_ring, _) = broadcast::channel(1);
        let (obs_tx, obs_rx) = broadcast::channel(OBSERVER_RING_CAPACITY);
        let (projection_seed, seed_rx) = mpsc::unbounded_channel();
        let gate = SubscriberGate::new();
        let projector_cancel = CancellationToken::new();
        let projector = tokio::spawn(projector_loop(
            obs_rx,
            seed_rx,
            ring.clone(),
            detach_ring.clone(),
            gate.reseed_pending(),
            gate.tenure(),
            projector_cancel.clone(),
        ));
        let tapped = approvals.clone();
        let observer = broca_core::RecordObserver::new(obs_tx, gate.observer_slots())
            .with_durable_tap(
                Arc::new(move |seq, fence, lineage, record| {
                    tapped.observe(seq, fence, lineage, record)
                }),
                approvals.healthy.clone(),
            );
        (
            Self {
                approvals,
                ring,
                detach_ring,
                display_ring,
                wal_store,
                session,
                gate: gate.clone(),
                projection_seed,
                subscribers: Vec::new(),
                projector_cancel,
                projector: Some(projector),
            },
            observer,
        )
    }

    pub(crate) fn approval_state(&self) -> Arc<crate::approval_state::ApprovalState> {
        self.approvals.clone()
    }

    /// A cloneable sender into the lossy display ring — handed to the run's `out`-drain
    /// task so it can forward display events (broadcast::send is synchronous + non-blocking,
    /// so the drain never stalls; a full ring drops the oldest, a lagging subscriber gets a
    /// `DisplayGap`). It only ever carries display (control is projection-sourced), so no
    /// double-publish.
    pub fn display_sender(&self) -> broadcast::Sender<Arc<broca_protocol::display::DisplayEvent>> {
        self.display_ring.clone()
    }

    /// Whether an attached subscriber or an in-progress attach still owns a fan-out slot.
    /// The actor checks this before claiming idle close so an observer keeps the session open.
    pub(crate) fn has_live_subscribers(&self) -> bool {
        self.gate.slots.load(Ordering::Acquire) > 0
    }

    /// Prepare an attach in this order: first subscribe to the live control stream, then
    /// enable live forwarding before the actor calls `writer.sync()`, while keeping a
    /// subscriber slot reserved until the new task starts. Records written before the sync
    /// are recovered from the WAL, and records written after the gate opens are already
    /// queued on the live receiver, so nothing is lost or duplicated. The reserved slot keeps
    /// a concurrently exiting last subscriber from turning the gate off in the middle of this
    /// hand-off.
    pub(crate) fn prepare_attach(
        &self,
    ) -> (
        SubscriberSlot,
        broadcast::Receiver<Arc<ControlEvent>>,
        broadcast::Receiver<()>,
    ) {
        let rx = self.ring.subscribe();
        let detach_rx = self.detach_ring.subscribe();
        let mut slot = self.gate.begin_attach();
        slot.arm_gate_owner_failure(self.projection_seed.clone());
        (slot, rx, detach_rx)
    }

    /// Spawn a subscriber task with an already-registered CONTROL ring receiver, the attach
    /// reservation that kept the gate enabled through `writer.sync()`, and the durable head
    /// `H` returned by that sync. The task also subscribes the DISPLAY ring (live-only, no
    /// barrier because display has no durable replay) and forwards both on the unified
    /// `sink`. The actor only performed the small receiver-registration + sync step; the
    /// slower WAL replay happens in this task so a subscriber's catch-up work never blocks
    /// the actor.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spawn_subscriber(
        &mut self,
        slot: SubscriberSlot,
        from: AttachFrom,
        h: u64,
        rx: broadcast::Receiver<Arc<ControlEvent>>,
        detach_rx: broadcast::Receiver<()>,
        sink: mpsc::Sender<Delivery>,
        materialized_store: Option<Arc<dyn SessionStore>>,
    ) {
        self.reap_finished();
        let display_rx = self.display_ring.subscribe();
        let cancel = CancellationToken::new();
        let handle = tokio::spawn(subscriber_task(
            slot,
            from,
            h,
            rx,
            detach_rx,
            display_rx,
            sink.clone(),
            self.wal_store.clone(),
            self.session.clone(),
            self.projection_seed.clone(),
            cancel.clone(),
            materialized_store,
        ));
        self.subscribers.push(SubscriberTask {
            sink,
            cancel,
            handle,
        });
    }

    /// Drop completed subscriber handles so long-lived actors do not retain one task entry per
    /// disconnected client. Called on every attach and idle tick; unfinished tasks remain owned
    /// for cancel-and-join during close.
    pub(crate) fn reap_finished(&mut self) {
        self.subscribers.retain(|sub| !sub.handle.is_finished());
    }

    #[cfg(test)]
    fn subscriber_count(&self) -> usize {
        self.subscribers.len()
    }

    /// CANCEL + AWAIT-JOIN every subscriber task and the projector — the close teardown's
    /// subscriber step, run BEFORE the writer shuts down + the lease drops. So once close
    /// completes, no subscriber route + no projector is live on this actor (the lease-vs-
    /// subscriber window the teardown order closes). Cancel-then-join (not drop), because
    /// dropping a JoinHandle does NOT cancel the task.
    pub(crate) async fn expire(&mut self, expired_at_ms: u64) {
        for sub in &self.subscribers {
            sub.cancel.cancel();
        }
        for sub in self.subscribers.drain(..) {
            let _ = sub.handle.await;
            // A slow route cannot hold the lease. Its content-free terminal may wait off-actor.
            tokio::spawn(async move {
                let _ = sub.sink.send(Delivery::Expired { expired_at_ms }).await;
            });
        }
        self.projector_cancel.cancel();
        if let Some(projector) = self.projector.take() {
            let _ = projector.await;
        }
    }

    pub async fn teardown(&mut self) {
        for sub in &self.subscribers {
            sub.cancel.cancel();
        }
        for sub in self.subscribers.drain(..) {
            let _ = sub.handle.await;
        }
        self.projector_cancel.cancel();
        if let Some(p) = self.projector.take() {
            let _ = p.await;
        }
    }
}

/// Enter the existing replay-seed handshake after the observer has skipped a record range.
/// Both bounded observer lag and the zero-subscriber optimization use this single reset path.
fn reset_projection_handshake(
    projection_state: &mut ControlProjectionState,
    seeded: &mut bool,
    buffered: &mut Vec<broca_wal::FramedRecord>,
) {
    *projection_state = ControlProjectionState::default();
    *seeded = false;
    buffered.clear();
}

/// Drains the writer's record observer, projects each record, and broadcasts the units.
/// Records arrive in durable FIFO sequence order. If observation has a gap, the task enters
/// the replay-seed handshake and buffers new records until an attaching subscriber restores state.
///
/// The projector starts unseeded. The first attaching subscriber sends the state produced by
/// projecting its durable replay, so live projection continues from the exact same history without
/// projecting that history twice. Records observed during the attach barrier are buffered until
/// the seed arrives. The same handshake is reused after bounded observer lag and after the
/// zero-subscriber observer gate reopens, because both cases can hide state-changing records.
///
/// INVARIANT (memory safety): this task MUST never await a subscriber. Both the
/// writer-to-projector queue and subscriber ring are bounded broadcast channels whose sends are
/// synchronous and non-blocking. Lag detaches subscribers, which recover through WAL replay;
/// backpressure must never reach the fsync writer.
async fn projector_loop(
    mut obs_rx: broadcast::Receiver<broca_wal::FramedRecord>,
    mut seed_rx: mpsc::UnboundedReceiver<ProjectionSeedMessage>,
    ring: broadcast::Sender<Arc<ControlEvent>>,
    detach_ring: broadcast::Sender<()>,
    reseed_pending: Arc<AtomicBool>,
    gate_tenure: Arc<AtomicU64>,
    cancel: CancellationToken,
) {
    let mut projection_state = broca_core::ControlProjectionState::default();
    let mut seeded = false;
    let mut gate_seed_required: Option<u64> = None;
    let mut deferred_gate_seed: Option<ProjectionSeed> = None;
    let mut covered_head = 0_u64;
    let mut buffered: Vec<broca_wal::FramedRecord> = Vec::new();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            seed = seed_rx.recv() => {
                let Some(message) = seed else { return };
                if reseed_pending.swap(false, Ordering::AcqRel) {
                    reset_projection_handshake(
                        &mut projection_state,
                        &mut seeded,
                        &mut buffered,
                    );
                    deferred_gate_seed = None;
                    // Concurrent attaches can finish replay out of order. Only the slot that
                    // reopened this exact gate tenure owns the reset, so its earlier barrier head
                    // is installed before any later attach can move the handoff forward.
                    gate_seed_required = Some(gate_tenure.load(Ordering::Acquire));
                }
                let seed = match message {
                    ProjectionSeedMessage::State(seed) => {
                        if seed.gate_tenure != gate_tenure.load(Ordering::Acquire) {
                            // A prior 0 -> 1 transition finished replay after a newer tenure began.
                            // Its state cannot cover records written while the gate was empty.
                            continue;
                        }
                        if seeded {
                            continue;
                        }
                        if gate_seed_required.is_some() && !seed.gate_reopen {
                            // Keep the earliest concurrent barrier as a liveness fallback if the
                            // gate owner cannot read the WAL. Installing it now could skip records
                            // the owner's lower replay head still needs from the live ring.
                            if deferred_gate_seed
                                .as_ref()
                                .is_none_or(|deferred| seed.head < deferred.head)
                            {
                                deferred_gate_seed = Some(seed);
                            }
                            continue;
                        }
                        seed
                    }
                    ProjectionSeedMessage::GateOwnerFailed { gate_tenure: failed_tenure } => {
                        if gate_seed_required != Some(failed_tenure)
                            || failed_tenure != gate_tenure.load(Ordering::Acquire)
                        {
                            continue;
                        }
                        gate_seed_required = None;
                        let Some(seed) = deferred_gate_seed.take() else {
                            continue;
                        };
                        seed
                    }
                };
                if seed.head < covered_head {
                    // A seed older than an already projected record would roll episode-local
                    // state backward. This cannot happen for the gate owner because WAL heads
                    // are monotonic; detach rather than continue from an unprovable state.
                    let _ = detach_ring.send(());
                    gate_seed_required = None;
                    deferred_gate_seed = None;
                    continue;
                }

                // Equality is safe: replay and incremental projection cover the same immutable
                // WAL prefix, so replacing state at exactly `covered_head` cannot discard a
                // newer record. Buffered records are strictly newer and drain below.
                projection_state = seed.state;
                seeded = true;
                gate_seed_required = None;
                deferred_gate_seed = None;
                covered_head = covered_head.max(seed.head);
                for framed in buffered.drain(..).filter(|record| record.seq > seed.head) {
                    for event in broca_core::project_control_with_state(
                        std::slice::from_ref(&framed),
                        &mut projection_state,
                    ) {
                        let _ = ring.send(Arc::new(event));
                    }
                    covered_head = covered_head.max(framed.seq);
                }
            }
            rec = obs_rx.recv() => {
                if reseed_pending.swap(false, Ordering::AcqRel) {
                    reset_projection_handshake(
                        &mut projection_state,
                        &mut seeded,
                        &mut buffered,
                    );
                    deferred_gate_seed = None;
                    gate_seed_required = Some(gate_tenure.load(Ordering::Acquire));
                }
                match rec {
                    Ok(framed) if seeded => {
                        // The seed may win the select race against observer records already
                        // covered by its sync barrier. Do not project those records twice.
                        if framed.seq <= covered_head {
                            continue;
                        }
                        for event in broca_core::project_control_with_state(
                            std::slice::from_ref(&framed),
                            &mut projection_state,
                        ) {
                            // A send error means no live subscribers right now — fine, the WAL is
                            // the source of truth and a future attach replays from it.
                            let _ = ring.send(Arc::new(event));
                        }
                        covered_head = framed.seq;
                    }
                    Ok(framed) => buffered.push(framed),
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        // Reuse the same unseed-and-buffer handshake for both kinds of observer
                        // gap: bounded-channel lag and deliberate zero-subscriber suppression.
                        // No subscriber may consume a post-gap unit as contiguous.
                        let _ = detach_ring.send(());
                        reset_projection_handshake(
                            &mut projection_state,
                            &mut seeded,
                            &mut buffered,
                        );
                        deferred_gate_seed = None;
                        // Lag can happen while old subscriber slots are still unwinding, so the
                        // first subsequent replay seed is authoritative even if it did not cause
                        // a literal 0 -> 1 gate transition.
                        gate_seed_required = None;
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        }
    }
}

/// Restore the control projection state only when the persisted fields prove whether a model step
/// completed. A missing recorded finish reason is ambiguous: it can mean either a legacy completed
/// step or a failed retraction after an earlier step, so the caller must use full WAL replay.
fn control_projection_state_from_seed(
    seed: &broca_core::ProjectionSeed,
) -> Option<ControlProjectionState> {
    (seed.completed_steps == 0 || seed.last_step_finish_reason.is_some()).then(|| {
        ControlProjectionState::from_completed_steps_and_final_step_finish_reason(
            seed.completed_steps,
            seed.last_step_finish_reason,
            // The seed knows which run the watermark sits inside. Without it a
            // live-attached subscriber sees a terminal naming NO run while a
            // replaying one sees the same terminal naming it.
            seed.last_run_id.clone(),
        )
    })
}

/// Restore the control projection at `h` without decoding the materialized WAL prefix.
/// Every miss or inconsistency returns `None` so the caller takes the unchanged full replay path.
async fn try_live_projection_seed<W: WalStore + 'static>(
    wal_store: Arc<W>,
    session: BindIdentity,
    store: Option<Arc<dyn SessionStore>>,
    h: u64,
) -> Option<ControlProjectionState> {
    let store = store?;
    spawn_blocking(JobKind::WalRead, move || {
        let seed = store.projection_seed(&session).ok().flatten()?;
        if seed.watermark_seq > h {
            return None;
        }

        let mut state = control_projection_state_from_seed(&seed)?;
        if seed.watermark_seq == h {
            return Some(state);
        }

        let anchor = seed.anchor.as_ref()?;
        let range = wal_store
            .read_records_range_from_valid_anchor(&session, seed.watermark_seq, Some(h), anchor)
            .ok()
            .flatten()?;
        let records = match range.records() {
            broca_wal::WalRangeRecords::Complete(records)
            | broca_wal::WalRangeRecords::Partial(records)
            | broca_wal::WalRangeRecords::Unknown(records) => records,
            // Without a WAL suffix there is no proof that this seed reaches h.
            broca_wal::WalRangeRecords::Absent => return None,
        };
        if crate::registry::budget_firing_needs_full_replay(records, seed.prefix_len) {
            return None;
        }
        let expected_first = seed.watermark_seq.checked_add(1)?;
        if records.first().map(|record| record.seq) != Some(expected_first)
            || records.last().map(|record| record.seq) != Some(h)
        {
            return None;
        }
        advance_control_projection_state(records, &mut state);
        Some(state)
    })
    .await
    .ok()
    .flatten()
}

/// One subscriber's task: the SLOW part of the attach barrier (off-actor), then ongoing
/// live forwarding. Holds only the ring receiver + read-only store handles — NEVER the writer Arc
/// (the no-writer-clone invariant).
#[allow(clippy::too_many_arguments)]
async fn subscriber_task<W: WalStore + 'static>(
    slot: SubscriberSlot,
    from: AttachFrom,
    h: u64,
    mut rx: broadcast::Receiver<Arc<ControlEvent>>,
    mut detach_rx: broadcast::Receiver<()>,
    mut display_rx: broadcast::Receiver<Arc<broca_protocol::display::DisplayEvent>>,
    sink: mpsc::Sender<Delivery>,
    wal_store: Arc<W>,
    session: BindIdentity,
    projection_seed: mpsc::UnboundedSender<ProjectionSeedMessage>,
    cancel: CancellationToken,
    materialized_store: Option<Arc<dyn SessionStore>>,
) {
    let gate_reopen = slot.gate_reopen;
    let gate_tenure = slot.gate_tenure;
    let mut slot = Some(slot);

    let fast_live_state = if matches!(from, AttachFrom::Live) {
        try_live_projection_seed(wal_store.clone(), session.clone(), materialized_store, h).await
    } else {
        None
    };
    let from = if let Some(state) = fast_live_state {
        // The store seed and validated suffix cover exactly H. Register that state before awaiting
        // any sink operation; records appended after H are already buffered in the live receiver.
        if projection_seed
            .send(ProjectionSeedMessage::State(ProjectionSeed {
                head: h,
                state,
                gate_reopen,
                gate_tenure,
            }))
            .is_ok()
        {
            slot.as_mut()
                .expect("subscriber slot remains owned during seed handoff")
                .projection_seed_handed_off();
        }
        FromCursor::Start
    } else {
        // STEP 1 — replay the durable CONTROL projection for records seq <= H. read_records
        // returns committed (fsynced) records; HARD-FILTER seq <= H so a concurrent append of
        // seq > H during the read is excluded, keeping the read prefix-consistent at H.
        // (Display is live-only, so there's no display replay — a fresh subscriber sees display
        // from attach onward.)
        let records = match spawn_blocking(JobKind::WalRead, {
            let wal_store = wal_store.clone();
            let session = session.clone();
            move || wal_store.read_records(&session)
        })
        .await
        {
            Ok(Ok(recs)) => recs,
            Ok(Err(error)) => {
                slot.as_mut()
                    .expect("subscriber slot remains owned during replay")
                    .report_gate_owner_failure();
                let _ = deliver(
                    &sink,
                    &cancel,
                    Delivery::Error(format!("WAL replay failed: {error}")),
                )
                .await;
                return;
            }
            Err(error) => {
                slot.as_mut()
                    .expect("subscriber slot remains owned during replay")
                    .report_gate_owner_failure();
                let _ = deliver(
                    &sink,
                    &cancel,
                    Delivery::Error(format!("WAL replay task failed: {error}")),
                )
                .await;
                return;
            }
        };
        let durable_prefix: Vec<broca_wal::FramedRecord> =
            records.into_iter().filter(|f| f.seq <= h).collect();
        let seeded_h = durable_prefix.last().map_or(0, |record| record.seq);
        let mut replay_state = ControlProjectionState::default();
        let replay = project_control_with_state(&durable_prefix, &mut replay_state);
        // Live uses the existing strictly-after comparison once the barrier has projected H. An empty
        // lineage has no cursor to resume after, but Start is equivalent because its replay is empty.
        let from = match from {
            AttachFrom::Live => replay
                .last()
                .map_or(FromCursor::Start, |(cursor, _)| FromCursor::After(*cursor)),
            AttachFrom::Cursor(from) => from,
        };
        // Hand the projector the exact state this replay produced. Send before awaiting the subscriber
        // sink so live projection cannot remain unseeded behind a slow replay consumer.
        if projection_seed
            .send(ProjectionSeedMessage::State(ProjectionSeed {
                head: seeded_h,
                state: replay_state,
                gate_reopen,
                gate_tenure,
            }))
            .is_ok()
        {
            slot.as_mut()
                .expect("subscriber slot remains owned during seed handoff")
                .projection_seed_handed_off();
        }
        for (cursor, unit) in replay {
            if !from.includes(cursor) {
                continue; // before the client's attach point
            }
            if deliver(&sink, &cancel, Delivery::Control(Arc::new((cursor, unit))))
                .await
                .is_break()
            {
                return;
            }
        }
        from
    };

    // STEP 2 — forward the live streams: CONTROL (deduped past H) + DISPLAY (live, lossy).
    // `display_open` guards the display arm so a CLOSED display ring (returns Closed on
    // every poll) doesn't busy-loop the select — once display closes we stop polling it but
    // keep serving control until the control ring closes.
    let mut display_open = true;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = sink.closed() => return,
            detached = detach_rx.recv() => {
                match detached {
                    Ok(()) | Err(broadcast::error::RecvError::Lagged(_)) => {
                        detach_subscriber(&mut slot, &sink, &cancel).await;
                        return;
                    }
                    Err(broadcast::error::RecvError::Closed) => {}
                }
            }
            recv = rx.recv() => match recv {
                Ok(event) => {
                    // Dedup + attach-point filter: skip anything already in the replay
                    // (cursor.wal_seq <= h) or before the client's attach point.
                    if !deliver_from_ring(event.0, h, from) {
                        continue;
                    }
                    if deliver(&sink, &cancel, Delivery::Control(event))
                        .await
                        .is_break()
                    {
                        return;
                    }
                }
                // The CONTROL subscriber fell behind: the live ring overwrote events it
                // hadn't drained. DETACH (signal it, then stop) — the client resubscribes
                // from its last cursor and replays the projection. NEVER deliver a post-gap
                // CONTROL event as if it were contiguous (control is reliable).
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    detach_subscriber(&mut slot, &sink, &cancel).await;
                    return;
                }
                // The control ring closed (the projector stopped — session closing). Done.
                Err(broadcast::error::RecvError::Closed) => return,
            },
            disp = display_rx.recv(), if display_open => match disp {
                Ok(ev) => {
                    if deliver(&sink, &cancel, Delivery::Display(ev)).await.is_break() {
                        return;
                    }
                }
                // Display LAG is benign (display is lossy): emit an in-band DisplayGap
                // marker (NOT a detach — display has no durable replay to resubscribe to)
                // and keep going. The renderer learns its live text has a gap.
                Err(broadcast::error::RecvError::Lagged(dropped)) => {
                    let gap = broca_protocol::display::DisplayEvent::DisplayGap { dropped };
                    if deliver(&sink, &cancel, Delivery::Display(Arc::new(gap))).await.is_break() {
                        return;
                    }
                }
                // The display ring closed (session closing). Control may still be live;
                // stop polling display (else it returns Closed immediately, busy-looping)
                // and keep serving control until the control ring closes.
                Err(broadcast::error::RecvError::Closed) => display_open = false,
            }
        }
    }
}

/// Release a detached subscriber's gate slot before notifying its sink. The notification can
/// backpressure, but a subscriber that already lost continuity must not keep idle close pinned.
async fn detach_subscriber(
    slot: &mut Option<SubscriberSlot>,
    sink: &mpsc::Sender<Delivery>,
    cancel: &CancellationToken,
) {
    drop(slot.take());
    let _ = deliver(sink, cancel, Delivery::Detached).await;
}

/// Successful subscriber deliveries by lane, and successful wire encodes, since boot.
///
/// THE FAN-OUT PATH HAD NO COUNTERS AT ALL. I first wrote that it counted ONLY failures,
/// citing `frames_dropped_stale_epoch` and `frames_dropped_unmatched_terminal` — THAT WAS
/// WRONG. Those two live in `broca-subc`'s connection (`FRAME_DROPS`) and count INBOUND
/// frames on the client socket; they have nothing to do with subscriber fan-out. They sit
/// beside these on `health.check`, which is the whole reason I read them as this path's
/// failure counters without checking. The real state was WORSE than the one I argued
/// from: no success signal and no loss signal —
/// so a healthy reading was zero on both and said nothing about whether anything was being
/// delivered at all. A zero meaning "nothing bad happened" and a zero meaning "I cannot see
/// the thing" are indistinguishable at the surface, and only one of them is health.
///
/// It also made the path unmeasurable. A perf round measured the fan-out seam at ~420 ns per
/// subscriber delivery, payload-independent because the payload is Arc-shared, and could not
/// turn that into a deployed cost because nothing counts deliveries. `stream_chunks` is the
/// wrong denominator: one decoded SSE payload normalizes to zero or several display events.
///
/// Process-local and lifetime; they reset on restart, so diff two readings carrying the SAME
/// `launch_id` to get a rate. Relaxed ordering: these are counters, not fences, and no
/// decision reads them.
static CONTROL_DELIVERIES: AtomicU64 = AtomicU64::new(0);
static DISPLAY_DELIVERIES: AtomicU64 = AtomicU64::new(0);
static DISPLAY_GAPS: AtomicU64 = AtomicU64::new(0);

/// Control-lane units delivered to a subscriber sink since boot.
pub fn control_deliveries() -> u64 {
    CONTROL_DELIVERIES.load(Ordering::Relaxed)
}

/// Display-lane events delivered to a subscriber sink since boot.
pub fn display_deliveries() -> u64 {
    DISPLAY_DELIVERIES.load(Ordering::Relaxed)
}

/// Display-lane gap notices delivered since boot — the lossy lane's only loss signal.
///
/// Display lag is BENIGN by design (the lane is lossy and has no durable replay), so a gap is
/// not an error and must not read as one. It is also not a delivery: counting it as one made
/// `display_deliveries` rise on LOSS, which is the reading an operator would least expect.
pub fn display_gaps() -> u64 {
    DISPLAY_GAPS.load(Ordering::Relaxed)
}

/// Deliver one item to the sink, honoring cancellation. Returns `Break` if the subscriber
/// should stop (the sink closed = the client went away, or cancelled = close teardown).
async fn deliver(
    sink: &mpsc::Sender<Delivery>,
    cancel: &CancellationToken,
    item: Delivery,
) -> std::ops::ControlFlow<()> {
    // COUNT THE LANE BEFORE THE MOVE, because `item` is consumed by `send`.
    //
    // A GAP IS A LOSS NOTICE WEARING A DISPLAY DELIVERY'S ENVELOPE. The first version of this
    // match discriminated on `Delivery` alone, and the comment above it already said a Gap must
    // not count — but `DisplayGap` is a DisplayEvent PAYLOAD variant, wrapped in
    // `Delivery::Display` at the lag arm, so `display_deliveries` incremented on display LOSS.
    // The rule was stated at the right level and implemented one level too shallow.
    //
    // Detached and Error carry no unit at all, so they never count either.
    let lane = match &item {
        Delivery::Control(_) => Some(&CONTROL_DELIVERIES),
        Delivery::Display(event) => match **event {
            // Not a delivery AND not nothing: a gap is the display lane's only loss signal,
            // so it gets its own counter rather than vanishing from both readings.
            broca_protocol::display::DisplayEvent::DisplayGap { .. } => Some(&DISPLAY_GAPS),
            _ => Some(&DISPLAY_DELIVERIES),
        },
        _ => None,
    };
    tokio::select! {
        _ = cancel.cancelled() => std::ops::ControlFlow::Break(()),
        r = sink.send(item) => {
            if r.is_err() {
                std::ops::ControlFlow::Break(()) // the client dropped its receiver
            } else {
                // AFTER the send succeeds: a counter incremented before it would count
                // attempts, and the question an operator asks is whether anything ARRIVED.
                if let Some(counter) = lane {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
                std::ops::ControlFlow::Continue(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// THE DELIVERY COUNTERS MOVE ON A REAL DELIVERY, AND ONLY ON ONE.
    ///
    /// A counter that reads zero forever is worse than no counter: it answers the operator's
    /// question with "nothing is being delivered" when the truth is "I am not wired". That
    /// shipped here once already, when a projection-failure counter sat inside `#[cfg(test)]`
    /// and read permanently zero in release builds.
    ///
    /// Three arms, because the failure modes are different: a payload delivery must count, a
    /// control-plane notice carrying no unit must NOT (it would inflate the denominator), and a
    /// send to a closed sink must NOT (it never arrived). Deltas rather than absolutes, since
    /// the counters are process-global and other tests share them.
    #[tokio::test]
    async fn delivery_counters_count_arrivals_and_nothing_else() {
        let cancel = CancellationToken::new();
        let (tx, rx) = mpsc::channel::<Delivery>(8);

        let before = display_deliveries();
        let event = Arc::new(broca_protocol::display::DisplayEvent::TextDelta {
            message_id: broca_protocol::MessageId("m1".to_string()),
            tool_call_id: None,
            delta: "hi".to_string(),
        });
        assert!(matches!(
            deliver(&tx, &cancel, Delivery::Display(Arc::clone(&event))).await,
            std::ops::ControlFlow::Continue(())
        ));
        assert_eq!(
            display_deliveries() - before,
            1,
            "a delivered display event must count exactly once"
        );

        // A GAP IS THE ARM THIS TEST DID NOT HAVE, and its absence is why the defect
        // shipped: a DisplayGap rides in a Delivery::Display envelope, so a match on the
        // envelope alone counted display LOSS as a delivery. The mutation that restores
        // the envelope-only match passes every other assertion here.
        let before_gap_d = display_deliveries();
        let before_gap_g = display_gaps();
        assert!(matches!(
            deliver(
                &tx,
                &cancel,
                Delivery::Display(Arc::new(
                    broca_protocol::display::DisplayEvent::DisplayGap { dropped: 3 }
                ))
            )
            .await,
            std::ops::ControlFlow::Continue(())
        ));
        assert_eq!(
            (
                display_deliveries() - before_gap_d,
                display_gaps() - before_gap_g
            ),
            (0, 1),
            "a gap is LOSS: it must not count as a delivery, and must not vanish either"
        );

        let before_notice = display_deliveries();
        let before_control = control_deliveries();
        assert!(matches!(
            deliver(&tx, &cancel, Delivery::Detached).await,
            std::ops::ControlFlow::Continue(())
        ));
        assert_eq!(
            (
                display_deliveries() - before_notice,
                control_deliveries() - before_control
            ),
            (0, 0),
            "a Detached notice carries no unit and must not count on either lane"
        );

        // Drop the receiver: the client went away, so the send fails and nothing arrived.
        drop(rx);
        let before_closed = display_deliveries();
        assert!(matches!(
            deliver(&tx, &cancel, Delivery::Display(event)).await,
            std::ops::ControlFlow::Break(())
        ));
        assert_eq!(
            display_deliveries() - before_closed,
            0,
            "a send to a closed sink never arrived and must not count"
        );
    }

    use broca_protocol::caller::RunFinishReason;
    use broca_protocol::content::{AssistantMessage, ContentBlock, FinishReason, ToolCall};
    #[cfg(unix)]
    use broca_protocol::error::{ErrorClass, ProviderError};
    #[cfg(unix)]
    use broca_protocol::SubmissionId;
    use broca_protocol::{MessageId, RunId, StepId, ToolCallId, Usage};
    #[cfg(unix)]
    use broca_wal::QueuedAdmissionFailure;
    use broca_wal::{FramedRecord, RunConfig, RunOrigin, WalRecord};

    fn run_started(session: &BindIdentity) -> WalRecord {
        WalRecord::RunStarted {
            retention: None,
            hooks: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            marker_order_version: None,
            manifest: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            ts_ms: None,
            run_id: RunId("r1".into()),
            session: session.clone(),
            config: Box::new(RunConfig {
                model: broca_protocol::request::ModelHandle::new("fake", "fake"),
                tools: vec![].into(),
                tool_choice: Default::default(),
                generation: Default::default(),
                response_format: Default::default(),
                provider_options: serde_json::Value::Null,
                cache_tiers: serde_json::Value::Null,
                context_epoch: 0,
                renderer_version: 1,
                frozen_render_config: serde_json::json!({"test": true}),
                auth_selection: None,
                tools_were_omitted: false,
                budget: None,
                context_limit: None,
                work_class: None,
                variant: None,
                service_tier: None,
                stop_when: Vec::new(),
                context_management: None,
            }),
            input: vec![].into(),
            origin: RunOrigin::Fresh,
            submission_id: None,
        }
    }

    fn tool_step() -> WalRecord {
        WalRecord::ModelStepFinished {
            hooks: None,
            step_id: StepId(1),
            assistant_message: (AssistantMessage {
                message_id: MessageId("m1".into()),
                content: vec![ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId("c1".into()),
                    tool_name: "lookup".into(),
                    input: serde_json::json!({"query": "x"}),
                })],
            })
            .into(),
            usage: Usage::default(),
            finish_reason: Some(FinishReason::ToolCalls),
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        }
    }

    fn run_finished() -> WalRecord {
        WalRecord::RunFinished {
            ts_ms: None,
            reason: RunFinishReason::Interrupted,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        }
    }

    fn framed(seq: u64, record: WalRecord) -> FramedRecord {
        FramedRecord {
            seq,
            fence: 1,
            record,
        }
    }

    fn state_through(records: &[FramedRecord]) -> ControlProjectionState {
        let mut state = ControlProjectionState::default();
        let _ = project_control_with_state(records, &mut state);
        state
    }

    #[cfg(unix)]
    fn text_step(step_id: u64) -> WalRecord {
        WalRecord::ModelStepFinished {
            hooks: None,
            step_id: StepId(step_id),
            assistant_message: (AssistantMessage {
                message_id: MessageId(format!("m{step_id}")),
                content: vec![ContentBlock::Text {
                    text: "done".into(),
                }],
            })
            .into(),
            usage: Usage::default(),
            finish_reason: Some(FinishReason::Stop),
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        }
    }

    #[cfg(unix)]
    fn failed_retraction() -> WalRecord {
        WalRecord::Retracted {
            submission_id: SubmissionId("sub-r2".into()),
            failure: Some(QueuedAdmissionFailure {
                run_id: RunId("r2".into()),
                error: ProviderError {
                    refusal: None,
                    class: ErrorClass::Permanent,
                    message: "unknown model".into(),
                    status: None,
                    retry_after_secs: None,
                    provider_code: Some("queued_admission_failed".into()),
                    resets_at_ms: None,
                    resets_at_source: None,
                    limit_window: None,
                    account_ref: None,
                    credential_id: None,
                },
            }),
        }
    }

    #[derive(Clone)]
    struct FixedProjectionSeedStore {
        seed: Option<broca_core::ProjectionSeed>,
    }

    impl broca_core::MaterializedStore for FixedProjectionSeedStore {
        fn read_prefix_with_watermark(
            &self,
            _session: &BindIdentity,
        ) -> Result<broca_core::StorePrefix, String> {
            Err("test store exposes only a projection seed".into())
        }
    }

    impl broca_core::Persister for FixedProjectionSeedStore {
        fn projection_seed(
            &self,
            _session: &BindIdentity,
        ) -> Result<Option<broca_core::ProjectionSeed>, String> {
            Ok(self.seed.clone())
        }

        fn project(
            &self,
            _session: &BindIdentity,
            _records: &[FramedRecord],
        ) -> Result<u64, String> {
            Err("test store does not project".into())
        }
    }

    #[cfg(unix)]
    fn fixed_projection_seed(
        watermark_seq: u64,
        anchor: broca_wal::WalByteAnchor,
        prefix_len: u64,
        final_step_finish_reason: Option<FinishReason>,
    ) -> broca_core::ProjectionSeed {
        broca_core::ProjectionSeed {
            deferred_calls: None,
            watermark_seq,
            anchor: Some(anchor),
            prefix_len,
            completed_steps: 1,
            usage: Usage::default(),
            retries_used: 0,
            last_step_usage: Some(Usage::default()),
            last_step_finish_reason: final_step_finish_reason,
            trailing_origin: None,
            last_run_id: Some(RunId("r1".into())),
            admission_checkpoint: broca_core::AdmissionCheckpointV1::empty(),
        }
    }

    /// The dedup decision is correct + non-vacuous: a live unit at `wal_seq <= h` was
    /// already replayed (step 1) so it MUST be skipped (the dedup); one at `wal_seq > h`
    /// MUST be delivered. This is the no-DUP half of the attach barrier, proven on the pure
    /// decision (the free-running stress proves it fires under real concurrency).
    #[test]
    fn ring_dedup_skips_at_or_below_h_delivers_above() {
        let h = 5u64;
        // At/below H → already in the replay → SKIP (the dedup; without it = a duplicate).
        assert!(!deliver_from_ring(Cursor::new(3, 0), h, FromCursor::Start));
        assert!(!deliver_from_ring(Cursor::new(5, 0), h, FromCursor::Start));
        // A multi-unit record at exactly H (sub_index > 0) is still ≤ H → skip.
        assert!(!deliver_from_ring(Cursor::new(5, 2), h, FromCursor::Start));
        // Above H → only on the live ring → DELIVER.
        assert!(deliver_from_ring(Cursor::new(6, 0), h, FromCursor::Start));
    }

    /// The attach-point filter composes with the dedup: a `From::After(c)` subscriber gets
    /// only units strictly after BOTH `h` and its own cursor `c`.
    #[test]
    fn ring_filter_honors_attach_point() {
        let h = 5u64;
        let from = FromCursor::After(Cursor::new(7, 0));
        // > h but not past the attach point → skip.
        assert!(!deliver_from_ring(Cursor::new(6, 0), h, from));
        assert!(!deliver_from_ring(Cursor::new(7, 0), h, from));
        // past both → deliver.
        assert!(deliver_from_ring(Cursor::new(8, 0), h, from));
    }

    // File-WAL byte anchors are recorded on unix only; on other platforms the live seed
    // path declines the anchor and falls back to full replay, which the fallback tests cover.
    #[cfg(unix)]
    #[tokio::test]
    async fn live_seed_matches_full_projection_and_buffers_the_first_post_barrier_record() {
        let root = broca_test_support::scratch_root()
            .join(format!("broca-fanout-live-seed-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let wal_store = Arc::new(broca_core::FileWalStore::new(root.join("wal")));
        let session = BindIdentity::new(root.join("project"), "runner", "live-seed");
        let (mut fanout, observer) = Fanout::new(wal_store.clone(), session.clone());
        let writer = broca_core::WalHandle::spawn_observed(
            wal_store.open(&session).expect("open WAL"),
            1,
            Some(observer),
        )
        .expect("spawn writer");

        writer.record(run_started(&session));
        writer.record(tool_step());
        writer.sync().await.expect("sync projected prefix");
        let prefix = wal_store
            .read_records_range(&session, 0, Some(2), None)
            .expect("read prefix anchor");
        let anchor = prefix
            .anchor_at(2)
            .expect("file WAL returns an exact anchor");

        writer.record(WalRecord::RunConfigRefrozen {
            renderer_version: 2,
            ts_ms: None,
            frozen_render_config: serde_json::json!({"refrozen": true}),
        });
        writer.record(text_step(2));
        let h = writer.sync().await.expect("sync attach head");
        assert_eq!(h, 4);

        let seed_store = Arc::new(FixedProjectionSeedStore {
            seed: Some(fixed_projection_seed(
                2,
                anchor,
                1,
                Some(FinishReason::ToolCalls),
            )),
        });
        let fast_state = try_live_projection_seed(
            wal_store.clone(),
            session.clone(),
            Some(seed_store.clone()),
            h,
        )
        .await
        .expect("valid seed and suffix use the fast path");
        let full_records = wal_store.read_records(&session).expect("read full oracle");
        let full_state = state_through(&full_records);
        assert_eq!(
            fast_state, full_state,
            "seed plus suffix must equal full replay at H"
        );

        let next = [framed(5, run_finished())];
        let mut expected_state = full_state.clone();
        let expected = project_control_with_state(&next, &mut expected_state);
        let mut continued_state = fast_state;
        let continued = project_control_with_state(&next, &mut continued_state);
        assert_eq!(continued_state, expected_state);
        assert_eq!(
            continued, expected,
            "the next cursors and payload bytes must match"
        );

        let (slot, rx, detach_rx) = fanout.prepare_attach();
        let (sink, mut deliveries) = mpsc::channel(8);
        fanout.spawn_subscriber(
            slot,
            AttachFrom::Live,
            h,
            rx,
            detach_rx,
            sink,
            Some(seed_store),
        );
        // This append can reach the observer before the blocking seed read completes. The reseed
        // handshake must buffer it and deliver it exactly once after installing state at H.
        writer.record(run_finished());
        writer.sync().await.expect("sync post-barrier record");

        let delivery = tokio::time::timeout(std::time::Duration::from_secs(1), deliveries.recv())
            .await
            .expect("post-barrier record arrives")
            .expect("subscriber remains attached");
        assert!(matches!(
            delivery,
            Delivery::Control(event)
                if event.0 == Cursor::new(5, 0)
                    && matches!(
                        &event.1,
                        ControlUnit::RunFinished {
                            final_step_finish_reason: Some(FinishReason::Stop),
                            ..
                        }
                    )
        ));
        assert!(matches!(
            deliveries.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));

        fanout.teardown().await;
        writer.shutdown().await;
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn real_store_mid_episode_seed_matches_full_terminal_projection() {
        let root = broca_test_support::scratch_root().join(format!(
            "broca-fanout-real-store-mid-episode-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let wal_store = Arc::new(broca_core::FileWalStore::new(root.join("wal")));
        let session = BindIdentity::new(root.join("project"), "runner", "mid-episode");
        let writer = broca_core::WalHandle::spawn(wal_store.open(&session).expect("open WAL"), 1)
            .expect("spawn writer");
        writer.record(run_started(&session));
        writer.record(text_step(1));
        writer.record(run_finished());
        let h = writer.sync().await.expect("sync complete episode");
        let full_range = wal_store
            .read_records_range(&session, 0, Some(h), None)
            .expect("read complete WAL with anchors");
        let store = Arc::new(
            broca_store::SqliteStore::open(root.join("store.db")).expect("open SQLite store"),
        );
        let watermark = broca_core::Persister::project_range(store.as_ref(), &session, &full_range)
            .expect("project terminal step");
        assert_eq!(watermark, 2, "RunFinished must remain in the WAL tail");

        let seed = broca_core::Persister::projection_seed(store.as_ref(), &session)
            .expect("read persisted projection seed")
            .expect("projection seed exists");
        let suffix = wal_store
            .read_records_range_from_valid_anchor(
                &session,
                watermark,
                Some(h),
                seed.anchor
                    .as_ref()
                    .expect("projection committed an anchor"),
            )
            .expect("read strict suffix")
            .expect("anchor remains valid");
        let mut fast_state = control_projection_state_from_seed(&seed)
            .expect("recorded finish reason proves the materialized completed step");
        let fast_tail = project_control_with_state(
            match suffix.records() {
                broca_wal::WalRangeRecords::Complete(records)
                | broca_wal::WalRangeRecords::Partial(records)
                | broca_wal::WalRangeRecords::Unknown(records) => records,
                broca_wal::WalRangeRecords::Absent => &[],
            },
            &mut fast_state,
        );
        let expected_tail: Vec<_> = broca_core::project_control(match full_range.records() {
            broca_wal::WalRangeRecords::Complete(records)
            | broca_wal::WalRangeRecords::Partial(records)
            | broca_wal::WalRangeRecords::Unknown(records) => records,
            broca_wal::WalRangeRecords::Absent => &[],
        })
        .into_iter()
        .filter(|(cursor, _)| cursor.wal_seq > watermark)
        .collect();
        assert_eq!(fast_tail, expected_tail);
        assert!(matches!(
            fast_tail.as_slice(),
            [(
                _,
                ControlUnit::RunFinished {
                    final_step_finish_reason: Some(FinishReason::Stop),
                    ..
                }
            )]
        ));

        let recovered =
            try_live_projection_seed(wal_store.clone(), session.clone(), Some(store), h)
                .await
                .expect("real store seed uses the fast path");
        assert_eq!(
            recovered,
            state_through(match full_range.records() {
                broca_wal::WalRangeRecords::Complete(records)
                | broca_wal::WalRangeRecords::Partial(records)
                | broca_wal::WalRangeRecords::Unknown(records) => records,
                broca_wal::WalRangeRecords::Absent => &[],
            })
        );

        writer.shutdown().await;
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn real_store_failed_retraction_clears_terminal_finish_reason() {
        let root = broca_test_support::scratch_root().join(format!(
            "broca-fanout-real-store-retraction-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let wal_store = Arc::new(broca_core::FileWalStore::new(root.join("wal")));
        let session = BindIdentity::new(root.join("project"), "runner", "retraction");
        let writer = broca_core::WalHandle::spawn(wal_store.open(&session).expect("open WAL"), 1)
            .expect("spawn writer");
        let store = Arc::new(
            broca_store::SqliteStore::open(root.join("store.db")).expect("open SQLite store"),
        );

        writer.record(run_started(&session));
        writer.record(tool_step());
        writer.record(WalRecord::TurnFinished { step_id: StepId(1) });
        let first_h = writer.sync().await.expect("sync first projection boundary");
        let first_range = wal_store
            .read_records_range(&session, 0, Some(first_h), None)
            .expect("read first projection range");
        let first_watermark =
            broca_core::Persister::project_range(store.as_ref(), &session, &first_range)
                .expect("project first boundary");
        assert_eq!(first_watermark, 3);
        let first_seed = broca_core::Persister::projection_seed(store.as_ref(), &session)
            .expect("read first seed")
            .expect("first seed exists");
        assert_eq!(
            first_seed.last_step_finish_reason,
            Some(FinishReason::ToolCalls)
        );

        writer.record(failed_retraction());
        writer.record(WalRecord::TurnFinished { step_id: StepId(2) });
        writer.record(run_finished());
        let h = writer.sync().await.expect("sync retraction and terminal");
        let projection_tail = wal_store
            .read_records_range(
                &session,
                first_watermark,
                Some(h),
                first_seed.anchor.as_ref(),
            )
            .expect("read incremental projection range");
        let watermark =
            broca_core::Persister::project_range(store.as_ref(), &session, &projection_tail)
                .expect("project failed retraction");
        assert_eq!(watermark, 5);
        let seed = broca_core::Persister::projection_seed(store.as_ref(), &session)
            .expect("read retraction seed")
            .expect("retraction seed exists");
        assert_eq!(seed.last_step_finish_reason, None);

        let suffix = wal_store
            .read_records_range_from_valid_anchor(
                &session,
                watermark,
                Some(h),
                seed.anchor
                    .as_ref()
                    .expect("projection committed an anchor"),
            )
            .expect("read strict terminal suffix")
            .expect("anchor remains valid");
        assert!(
            control_projection_state_from_seed(&seed).is_none(),
            "a retraction after a completed step must use full projection rather than guess usage presence"
        );
        let mut suffix_state = ControlProjectionState::default();
        let fast_tail = project_control_with_state(
            match suffix.records() {
                broca_wal::WalRangeRecords::Complete(records)
                | broca_wal::WalRangeRecords::Partial(records)
                | broca_wal::WalRangeRecords::Unknown(records) => records,
                broca_wal::WalRangeRecords::Absent => &[],
            },
            &mut suffix_state,
        );
        let full_records = wal_store
            .read_records(&session)
            .expect("read full WAL oracle");
        let expected_tail: Vec<_> = broca_core::project_control(&full_records)
            .into_iter()
            .filter(|(cursor, _)| cursor.wal_seq > watermark)
            .collect();
        assert_eq!(fast_tail, expected_tail);
        assert!(matches!(
            fast_tail.as_slice(),
            [(
                _,
                ControlUnit::RunFinished {
                    final_step_finish_reason: None,
                    ..
                }
            )]
        ));

        assert!(
            try_live_projection_seed(wal_store.clone(), session.clone(), Some(store), h)
                .await
                .is_none(),
            "an ambiguous retraction seed must use full replay"
        );

        writer.shutdown().await;
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn live_attach_without_a_persisted_seed_falls_back_to_full_replay() {
        let root = broca_test_support::scratch_root()
            .join(format!("broca-fanout-live-fallback-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let wal_store = Arc::new(broca_core::FileWalStore::new(root.join("wal")));
        let session = BindIdentity::new(root.join("project"), "runner", "live-fallback");
        let (mut fanout, observer) = Fanout::new(wal_store.clone(), session.clone());
        let writer = broca_core::WalHandle::spawn_observed(
            wal_store.open(&session).expect("open WAL"),
            1,
            Some(observer),
        )
        .expect("spawn writer");
        writer.record(run_started(&session));
        writer.record(tool_step());
        let h = writer.sync().await.expect("sync attach head");

        let no_seed_store = Arc::new(FixedProjectionSeedStore { seed: None });
        let (slot, rx, detach_rx) = fanout.prepare_attach();
        let (sink, mut deliveries) = mpsc::channel(8);
        fanout.spawn_subscriber(
            slot,
            AttachFrom::Live,
            h,
            rx,
            detach_rx,
            sink,
            Some(no_seed_store),
        );
        writer.record(run_finished());
        writer.sync().await.expect("sync post-barrier terminal");

        let delivery = tokio::time::timeout(std::time::Duration::from_secs(1), deliveries.recv())
            .await
            .expect("slow-path subscriber receives the next record")
            .expect("subscriber remains attached");
        assert!(matches!(
            delivery,
            Delivery::Control(event)
                if event.0 == Cursor::new(3, 0)
                    && matches!(
                        &event.1,
                        ControlUnit::RunFinished {
                            final_step_finish_reason: Some(FinishReason::ToolCalls),
                            ..
                        }
                    )
        ));

        fanout.teardown().await;
        writer.shutdown().await;
        let _ = std::fs::remove_dir_all(root);
    }

    // File-WAL byte anchors are recorded on unix only; on other platforms the live seed
    // path declines the anchor and falls back to full replay, which the fallback tests cover.
    #[cfg(unix)]
    #[tokio::test]
    async fn live_seed_rejects_a_budget_firing_suffix_without_its_run_config() {
        let root = broca_test_support::scratch_root().join(format!(
            "broca-fanout-live-budget-fallback-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let wal_store = Arc::new(broca_core::FileWalStore::new(root.join("wal")));
        let session = BindIdentity::new(root.join("project"), "runner", "live-budget-fallback");
        let writer = broca_core::WalHandle::spawn(wal_store.open(&session).expect("open WAL"), 1)
            .expect("spawn writer");
        writer.record(run_started(&session));
        writer.record(tool_step());
        writer.sync().await.expect("sync projected prefix");
        let prefix = wal_store
            .read_records_range(&session, 0, Some(2), None)
            .expect("read prefix anchor");
        let anchor = prefix
            .anchor_at(2)
            .expect("file WAL returns an exact anchor");
        writer.record(WalRecord::RunConfigRefrozen {
            renderer_version: 2,
            ts_ms: None,
            frozen_render_config: serde_json::json!({"refrozen": true}),
        });
        let refrozen_h = writer.sync().await.expect("sync ordinary suffix");
        let ahead_seed_store = Arc::new(FixedProjectionSeedStore {
            seed: Some(fixed_projection_seed(
                refrozen_h + 1,
                anchor,
                1,
                Some(FinishReason::ToolCalls),
            )),
        });
        assert!(
            try_live_projection_seed(
                wal_store.clone(),
                session.clone(),
                Some(ahead_seed_store),
                refrozen_h,
            )
            .await
            .is_none(),
            "a projection watermark ahead of the fsynced head must select full replay"
        );

        let mut stale_anchor = anchor;
        stale_anchor.file_id ^= 1;
        let stale_seed_store = Arc::new(FixedProjectionSeedStore {
            seed: Some(fixed_projection_seed(
                2,
                stale_anchor,
                1,
                Some(FinishReason::ToolCalls),
            )),
        });
        assert!(
            try_live_projection_seed(
                wal_store.clone(),
                session.clone(),
                Some(stale_seed_store),
                refrozen_h,
            )
            .await
            .is_none(),
            "an invalid byte anchor must select the full replay safety path"
        );

        writer.record(WalRecord::BudgetFired {
            run_id: RunId("r1".into()),
            kind: broca_protocol::request::BudgetKind::Finalize,
            threshold: 1,
            ordinal: 0,
            caller_text: None,
        });
        let budget_h = writer.sync().await.expect("sync budget suffix");
        let seed_store = Arc::new(FixedProjectionSeedStore {
            seed: Some(fixed_projection_seed(
                2,
                anchor,
                1,
                Some(FinishReason::ToolCalls),
            )),
        });

        assert!(
            try_live_projection_seed(wal_store, session, Some(seed_store), budget_h)
                .await
                .is_none(),
            "BudgetFired in a materialized suffix must select the full replay safety path"
        );

        writer.shutdown().await;
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn multiple_subscribers_receive_identical_shared_live_payloads() {
        let root = broca_test_support::scratch_root().join(format!(
            "broca-fanout-shared-payloads-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let store = Arc::new(broca_core::FileWalStore::new(root.join("wal")));
        let session = BindIdentity::new(root.join("project"), "runner", "shared-payloads");
        let (mut fanout, observer) = Fanout::new(store.clone(), session.clone());
        let writer = broca_core::WalHandle::spawn_observed(
            store.open(&session).expect("open WAL"),
            1,
            Some(observer),
        )
        .expect("spawn writer");

        let (slot_a, rx_a, detach_a) = fanout.prepare_attach();
        let (slot_b, rx_b, detach_b) = fanout.prepare_attach();
        let head = writer.sync().await.expect("initial sync");
        let (sink_a, mut deliveries_a) = mpsc::channel(8);
        let (sink_b, mut deliveries_b) = mpsc::channel(8);
        fanout.spawn_subscriber(
            slot_a,
            AttachFrom::Cursor(FromCursor::Start),
            head,
            rx_a,
            detach_a,
            sink_a,
            None,
        );
        fanout.spawn_subscriber(
            slot_b,
            AttachFrom::Cursor(FromCursor::Start),
            head,
            rx_b,
            detach_b,
            sink_b,
            None,
        );

        writer.record(run_started(&session));
        writer.sync().await.expect("sync run start");
        let control_a =
            tokio::time::timeout(std::time::Duration::from_secs(1), deliveries_a.recv())
                .await
                .expect("subscriber A receives control")
                .expect("subscriber A remains attached");
        let control_b =
            tokio::time::timeout(std::time::Duration::from_secs(1), deliveries_b.recv())
                .await
                .expect("subscriber B receives control")
                .expect("subscriber B remains attached");
        match (control_a, control_b) {
            (Delivery::Control(event_a), Delivery::Control(event_b)) => {
                assert_eq!(event_a.as_ref(), event_b.as_ref());
                assert!(Arc::ptr_eq(&event_a, &event_b));
            }
            other => panic!("both subscribers must receive the same control event: {other:?}"),
        }

        let display = Arc::new(broca_protocol::display::DisplayEvent::TextDelta {
            message_id: MessageId("m1".into()),
            tool_call_id: None,
            delta: "shared delta".into(),
        });
        fanout
            .display_sender()
            .send(Arc::clone(&display))
            .expect("display ring has two receivers");
        let display_a =
            tokio::time::timeout(std::time::Duration::from_secs(1), deliveries_a.recv())
                .await
                .expect("subscriber A receives display")
                .expect("subscriber A remains attached");
        let display_b =
            tokio::time::timeout(std::time::Duration::from_secs(1), deliveries_b.recv())
                .await
                .expect("subscriber B receives display")
                .expect("subscriber B remains attached");
        match (display_a, display_b) {
            (Delivery::Display(event_a), Delivery::Display(event_b)) => {
                assert_eq!(event_a.as_ref(), event_b.as_ref());
                assert!(Arc::ptr_eq(&event_a, &event_b));
                assert!(Arc::ptr_eq(&display, &event_a));
            }
            other => panic!("both subscribers must receive the same display event: {other:?}"),
        }

        fanout.teardown().await;
        writer.shutdown().await;
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn gate_reopen_reseeds_state_before_live_terminal() {
        let root = broca_test_support::scratch_root()
            .join(format!("broca-fanout-reseed-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let store = Arc::new(broca_core::FileWalStore::new(root.join("wal")));
        let session = BindIdentity::new(root.join("project"), "runner", "reseed");
        let (mut fanout, observer) = Fanout::new(store.clone(), session.clone());
        let writer = broca_core::WalHandle::spawn_observed(
            store.open(&session).expect("open WAL"),
            1,
            Some(observer),
        )
        .expect("spawn writer");

        // Attach A and prove its seed has installed by observing one live record.
        let (slot_a, rx_a, detach_a) = fanout.prepare_attach();
        let head_a = writer.sync().await.expect("initial sync");
        let (sink_a, mut deliveries_a) = mpsc::channel(16);
        fanout.spawn_subscriber(
            slot_a,
            AttachFrom::Cursor(FromCursor::Start),
            head_a,
            rx_a,
            detach_a,
            sink_a,
            None,
        );
        writer.record(run_started(&session));
        writer.sync().await.expect("sync run start");
        let delivery = tokio::time::timeout(std::time::Duration::from_secs(1), deliveries_a.recv())
            .await
            .expect("subscriber A receives promptly")
            .expect("subscriber A remains attached");
        assert!(matches!(
            delivery,
            Delivery::Control(event)
                if matches!(&event.1, ControlUnit::RunStarted { .. })
        ));

        // Detach A, then commit the state-changing step while the observer gate is disabled.
        drop(deliveries_a);
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while fanout.has_live_subscribers() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("subscriber A releases the observer gate");
        writer.record(tool_step());
        writer.sync().await.expect("sync suppressed tool step");

        // B's replay includes the suppressed ModelStepFinished and therefore computes the
        // ToolCalls state that the live projector must adopt before projecting the terminal.
        let (slot_b, rx_b, detach_b) = fanout.prepare_attach();
        let head_b = writer.sync().await.expect("reopen sync");
        let (sink_b, mut deliveries_b) = mpsc::channel(16);
        fanout.spawn_subscriber(
            slot_b,
            AttachFrom::Cursor(FromCursor::Start),
            head_b,
            rx_b,
            detach_b,
            sink_b,
            None,
        );
        loop {
            match tokio::time::timeout(std::time::Duration::from_secs(1), deliveries_b.recv())
                .await
                .expect("subscriber B replay completes")
                .expect("subscriber B remains attached")
            {
                Delivery::Control(event)
                    if matches!(
                        &event.1,
                        ControlUnit::StepFinished {
                            finish_reason: FinishReason::ToolCalls,
                            ..
                        }
                    ) =>
                {
                    break
                }
                Delivery::Control(_) | Delivery::Display(_) => {}
                Delivery::Error(error) => panic!("replay failed: {error}"),
                Delivery::Expired { .. } => panic!("forever subscriber expired"),
                Delivery::Detached => panic!("subscriber B detached during replay"),
            }
        }

        writer.record(run_finished());
        writer.sync().await.expect("sync live terminal");
        let terminal_finish_reason = loop {
            match tokio::time::timeout(std::time::Duration::from_secs(1), deliveries_b.recv())
                .await
                .expect("live terminal arrives")
                .expect("subscriber B remains attached")
            {
                Delivery::Control(event) => {
                    if let ControlUnit::RunFinished {
                        final_step_finish_reason,
                        ..
                    } = &event.1
                    {
                        break *final_step_finish_reason;
                    }
                }
                Delivery::Display(_) => {}
                Delivery::Error(error) => panic!("live delivery failed: {error}"),
                Delivery::Expired { .. } => panic!("forever subscriber expired"),
                Delivery::Detached => panic!("subscriber B detached before terminal"),
            }
        };
        assert_eq!(
            terminal_finish_reason,
            Some(FinishReason::ToolCalls),
            "the reconnect seed must replace stale pre-gap projector state"
        );

        fanout.teardown().await;
        writer.shutdown().await;
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn dropping_unconsumed_gate_owner_reports_failure() {
        let gate = SubscriberGate::new();
        let (seed_tx, mut seed_rx) = mpsc::unbounded_channel();
        let mut owner = gate.begin_attach();
        assert!(owner.gate_reopen);
        let tenure = owner.gate_tenure;
        owner.arm_gate_owner_failure(seed_tx);

        drop(owner);

        assert_eq!(gate.slots.load(Ordering::Acquire), 0);
        assert!(matches!(
            seed_rx.try_recv(),
            Ok(ProjectionSeedMessage::GateOwnerFailed { gate_tenure })
                if gate_tenure == tenure
        ));
    }

    #[tokio::test]
    async fn stale_seed_from_prior_gate_tenure_cannot_win_reseed() {
        let gate = SubscriberGate::new();
        let first_owner = gate.begin_attach();
        let first_tenure = first_owner.gate_tenure;
        drop(first_owner);
        let second_owner = gate.begin_attach();
        let second_tenure = second_owner.gate_tenure;
        assert!(second_owner.gate_reopen);
        assert!(second_tenure > first_tenure);

        let (observer_tx, observer_rx) = broadcast::channel(4);
        let (ring, mut ring_rx) = broadcast::channel(8);
        let (detach_ring, _) = broadcast::channel(1);
        let (seed_tx, seed_rx) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(projector_loop(
            observer_rx,
            seed_rx,
            ring,
            detach_ring,
            gate.reseed_pending(),
            gate.tenure(),
            cancel.clone(),
        ));

        // The terminal arrives during the second tenure, before either replay completes. The
        // first tenure's delayed seed must not clear the pending handshake or project this record.
        observer_tx.send(framed(3, run_finished())).unwrap();
        seed_tx
            .send(ProjectionSeedMessage::State(ProjectionSeed {
                head: 2,
                state: ControlProjectionState::from_completed_steps_and_final_step_finish_reason(
                    1,
                    Some(FinishReason::ToolCalls),
                    // A hand-built seed in a test: no run to name.
                    None,
                ),
                gate_reopen: true,
                gate_tenure: first_tenure,
            }))
            .unwrap();
        seed_tx
            .send(ProjectionSeedMessage::State(ProjectionSeed {
                head: 2,
                state: ControlProjectionState::from_completed_steps_and_final_step_finish_reason(
                    1,
                    Some(FinishReason::Stop),
                    // A hand-built seed in a test: no run to name.
                    None,
                ),
                gate_reopen: true,
                gate_tenure: second_tenure,
            }))
            .unwrap();

        let terminal = tokio::time::timeout(std::time::Duration::from_secs(1), ring_rx.recv())
            .await
            .expect("current tenure seed drains the buffered terminal")
            .expect("ring remains open");
        assert!(matches!(
            terminal.as_ref(),
            (
                Cursor { wal_seq: 3, .. },
                ControlUnit::RunFinished {
                    final_step_finish_reason: Some(FinishReason::Stop),
                    ..
                }
            )
        ));
        assert!(matches!(
            ring_rx.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));

        drop(second_owner);
        cancel.cancel();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn record_between_gate_reopen_and_seed_is_buffered() {
        let session = BindIdentity::new("/tmp/project-a", "runner", "buffer-reseed");
        let prefix = vec![framed(1, run_started(&session)), framed(2, tool_step())];
        let (observer_tx, observer_rx) = broadcast::channel(4);
        let (ring, mut ring_rx) = broadcast::channel(8);
        let (detach_ring, _) = broadcast::channel(1);
        let (seed_tx, seed_rx) = mpsc::unbounded_channel();
        let reseed_pending = Arc::new(AtomicBool::new(true));
        let cancel = CancellationToken::new();
        let task = tokio::spawn(projector_loop(
            observer_rx,
            seed_rx,
            ring,
            detach_ring,
            reseed_pending,
            Arc::new(AtomicU64::new(1)),
            cancel.clone(),
        ));

        // RunFinished is newer than the attach barrier but reaches the observer before the
        // replay seed. It must wait in the existing buffer instead of using stale default state.
        observer_tx.send(framed(3, run_finished())).unwrap();
        tokio::task::yield_now().await;
        assert!(matches!(
            ring_rx.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
        seed_tx
            .send(ProjectionSeedMessage::State(ProjectionSeed {
                head: 2,
                state: state_through(&prefix),
                gate_reopen: true,
                gate_tenure: 1,
            }))
            .unwrap();

        let terminal = tokio::time::timeout(std::time::Duration::from_secs(1), ring_rx.recv())
            .await
            .expect("buffer drains after seed")
            .expect("ring remains open");
        assert!(matches!(
            terminal.as_ref(),
            (
                Cursor { wal_seq: 3, .. },
                ControlUnit::RunFinished {
                    final_step_finish_reason: Some(FinishReason::ToolCalls),
                    ..
                }
            )
        ));
        assert!(matches!(
            ring_rx.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));

        cancel.cancel();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_gate_reopen_attaches_converge_to_uninterrupted_projection() {
        let session = BindIdentity::new("/tmp/project-a", "runner", "double-reseed");
        let records = vec![
            framed(1, run_started(&session)),
            framed(2, tool_step()),
            framed(
                3,
                WalRecord::ToolResult {
                    hooks: None,
                    batch_id: broca_protocol::BatchId("b1".into()),
                    result: broca_protocol::content::ToolResult {
                        tool_call_id: ToolCallId("c1".into()),
                        output: broca_protocol::content::ToolOutput::Text {
                            text: "done".into(),
                        },
                        is_error: false,
                    }
                    .into(),
                    outcome: broca_wal::DispatchOutcome::Completed,
                    reason: None,
                    withdraw_elicitation: None,
                    provider_held: None,
                },
            ),
            framed(4, run_finished()),
        ];
        let expected = broca_core::project_control(&records);
        let gate = SubscriberGate::new();
        let owner = gate.begin_attach();
        let later = gate.begin_attach();
        assert!(owner.gate_reopen);
        assert!(!later.gate_reopen);

        let (observer_tx, observer_rx) = broadcast::channel(4);
        let (ring, mut ring_rx) = broadcast::channel(16);
        let (detach_ring, _) = broadcast::channel(1);
        let (seed_tx, seed_rx) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(projector_loop(
            observer_rx,
            seed_rx,
            ring,
            detach_ring,
            gate.reseed_pending(),
            gate.tenure(),
            cancel.clone(),
        ));

        // The later attach (H=3) finishes replay before the gate owner (H=2). Its seed must
        // not skip seq 3 for the older attach; seq 3 stays buffered until the owner installs.
        observer_tx.send(records[2].clone()).unwrap();
        seed_tx
            .send(ProjectionSeedMessage::State(ProjectionSeed {
                head: 3,
                state: state_through(&records[..3]),
                gate_reopen: later.gate_reopen,
                gate_tenure: later.gate_tenure,
            }))
            .unwrap();
        seed_tx
            .send(ProjectionSeedMessage::State(ProjectionSeed {
                head: 2,
                state: state_through(&records[..2]),
                gate_reopen: owner.gate_reopen,
                gate_tenure: owner.gate_tenure,
            }))
            .unwrap();
        observer_tx.send(records[3].clone()).unwrap();

        let mut live: Vec<Arc<ControlEvent>> = Vec::new();
        while live.last().is_none_or(|event| event.0.wal_seq < 4) {
            live.push(
                tokio::time::timeout(std::time::Duration::from_secs(1), ring_rx.recv())
                    .await
                    .expect("live continuation arrives")
                    .expect("ring remains open"),
            );
        }
        let mut owner_view = broca_core::project_control(&records[..2]);
        owner_view.extend(live.iter().map(|event| event.as_ref().clone()));
        let mut later_view = broca_core::project_control(&records[..3]);
        later_view.extend(
            live.iter()
                .filter(|event| event.0.wal_seq > 3)
                .map(|event| event.as_ref().clone()),
        );
        assert_eq!(owner_view, expected);
        assert_eq!(later_view, expected);

        drop(owner);
        drop(later);
        cancel.cancel();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn test_fanout_teardown_while_lagged() {
        use std::time::Duration;

        struct MockWalStore;
        impl WalStore for MockWalStore {
            fn open(
                &self,
                _session: &BindIdentity,
            ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
                unimplemented!()
            }
            fn read_records(
                &self,
                _session: &BindIdentity,
            ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
                Ok(Vec::new())
            }
        }

        let wal_store = Arc::new(MockWalStore);
        let session = BindIdentity::new("/tmp/project-a", "runner", "s1");
        let (mut fanout, _observer) = Fanout::new(wal_store, session);

        // Prepare attach.
        let (slot, rx, detach_rx) = fanout.prepare_attach();

        // Bounded sink channel of size 1.
        let (sink_tx, mut sink_rx) = mpsc::channel::<Delivery>(1);

        // Spawn subscriber.
        fanout.spawn_subscriber(
            slot,
            AttachFrom::Cursor(FromCursor::Start),
            0, // h
            rx,
            detach_rx,
            sink_tx,
            None,
        );

        // Send 1 event to the ring. The subscriber task receives it and delivers it (succeeds, filling the sink buffer).
        let event1 = (Cursor::new(1, 0), ControlUnit::DisplayReset);
        fanout.ring.send(Arc::new(event1)).unwrap();

        // Send 2nd event to the ring. The subscriber task receives it and blocks on deliver (since sink is full).
        let event2 = (Cursor::new(2, 0), ControlUnit::DisplayReset);
        // Give it a tiny bit of time to make sure the first event is processed.
        tokio::time::sleep(Duration::from_millis(10)).await;
        fanout.ring.send(Arc::new(event2)).unwrap();

        // Give the subscriber task a moment to block on deliver.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Overflow the control ring so the subscriber lags.
        // RING_CAPACITY is 1024, so sending 1025 events is enough.
        for i in 0..1025 {
            let event = (Cursor::new(3 + i, 0), ControlUnit::DisplayReset);
            let _ = fanout.ring.send(Arc::new(event));
        }

        // Read 1 event from sink_rx to unblock the blocked deliver.
        let _ = sink_rx.recv().await.unwrap();

        // The lag-detach must release its gate slot before trying to notify the still-full sink.
        tokio::time::timeout(Duration::from_secs(1), async {
            while fanout.has_live_subscribers() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("a lag-detached subscriber must stop pinning idle close");

        // Call teardown. It should unblock and return within a bounded time.
        let teardown_fut = fanout.teardown();
        let result = tokio::time::timeout(Duration::from_secs(1), teardown_fut).await;
        assert!(
            result.is_ok(),
            "teardown deadlocked/hung while subscriber was lagged"
        );
    }

    struct EmptyWalStore;

    impl WalStore for EmptyWalStore {
        fn open(
            &self,
            _session: &BindIdentity,
        ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
            unimplemented!()
        }

        fn read_records(
            &self,
            _session: &BindIdentity,
        ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
            Ok(Vec::new())
        }
    }

    struct FailingWalStore;

    impl WalStore for FailingWalStore {
        fn open(
            &self,
            _session: &BindIdentity,
        ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
            unimplemented!()
        }

        fn read_records(
            &self,
            _session: &BindIdentity,
        ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
            Err(broca_wal::WalError::Io(std::io::Error::other(
                "corrupted WAL",
            )))
        }
    }

    #[tokio::test]
    async fn lagged_observer_queue_detaches_live_subscribers() {
        let (observer_tx, observer_rx) = broadcast::channel(2);
        let (ring, _ring_rx) = broadcast::channel(2);
        let (detach_ring, mut detach_rx) = broadcast::channel(1);
        let cancel = CancellationToken::new();

        for seq in 1..=3 {
            observer_tx
                .send(broca_wal::FramedRecord {
                    seq,
                    fence: 1,
                    record: broca_wal::WalRecord::StepStarted {
                        step_id: broca_protocol::StepId(seq),
                    },
                })
                .unwrap();
        }

        let (_seed_tx, seed_rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(projector_loop(
            observer_rx,
            seed_rx,
            ring,
            detach_ring,
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicU64::new(1)),
            cancel.clone(),
        ));
        tokio::time::timeout(std::time::Duration::from_secs(1), detach_rx.recv())
            .await
            .expect("observer lag must be surfaced promptly")
            .expect("observer lag must detach subscribers");
        cancel.cancel();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn seeded_projector_continuation_matches_uninterrupted_projection() {
        use broca_protocol::caller::RunFinishReason;
        use broca_protocol::content::{AssistantMessage, ContentBlock, FinishReason, ToolCall};
        use broca_protocol::{MessageId, RunId, ToolCallId, Usage};
        use broca_wal::{FramedRecord, RunConfig, WalRecord};

        let records = vec![
            FramedRecord {
                seq: 1,
                fence: 1,
                record: WalRecord::RunStarted {
                    retention: None,
                    hooks: None,
                    title: None,
                    on_restart: None,
                    keep_warm: None,
                    marker_order_version: None,
                    manifest: None,
                    scope: None,
                    send_id: None,
                    request_identity_digest: None,
                    project_id: None,
                    principal: None,
                    ts_ms: None,
                    run_id: RunId("r1".into()),
                    session: BindIdentity::new("/tmp/project-a", "runner", "restart"),
                    config: Box::new(RunConfig {
                        model: broca_protocol::request::ModelHandle::new("fake", "fake"),
                        tools: vec![].into(),
                        tool_choice: Default::default(),
                        generation: Default::default(),
                        response_format: Default::default(),
                        provider_options: serde_json::Value::Null,
                        cache_tiers: serde_json::Value::Null,
                        context_epoch: 0,
                        renderer_version: 1,
                        frozen_render_config: serde_json::json!({"test": true}),
                        auth_selection: None,
                        tools_were_omitted: false,
                        budget: None,
                        context_limit: None,
                        work_class: None,
                        variant: None,
                        service_tier: None,
                        stop_when: Vec::new(),
                        context_management: None,
                    }),
                    input: vec![].into(),
                    origin: broca_wal::RunOrigin::Fresh,
                    submission_id: None,
                },
            },
            FramedRecord {
                seq: 2,
                fence: 1,
                record: WalRecord::ModelStepFinished {
                    hooks: None,
                    step_id: broca_protocol::StepId(1),
                    assistant_message: AssistantMessage {
                        message_id: MessageId("m1".into()),
                        content: vec![
                            ContentBlock::Text { text: "x".into() },
                            ContentBlock::ToolCall(ToolCall {
                                thought_signature: None,
                                tool_call_id: ToolCallId("c1".into()),
                                tool_name: "lookup".into(),
                                input: serde_json::json!({"query": "x"}),
                            }),
                            ContentBlock::ToolCall(ToolCall {
                                thought_signature: None,
                                tool_call_id: ToolCallId("c2".into()),
                                tool_name: "fetch".into(),
                                input: serde_json::json!({"id": 2}),
                            }),
                        ],
                    }
                    .into(),
                    usage: Usage::default(),
                    finish_reason: Some(FinishReason::Length),
                    retries_used: 0,
                    service_tier_used: None,
                    speed_used: None,
                },
            },
            FramedRecord {
                seq: 3,
                fence: 1,
                record: WalRecord::RunFinished {
                    ts_ms: None,
                    reason: RunFinishReason::Completed,
                    usage: Usage::default(),
                    error: None,
                    retries_used: 0,
                },
            },
        ];
        let full = broca_core::project_control(&records);
        let split = 2;
        let mut replay_state = ControlProjectionState::default();
        let replay = project_control_with_state(&records[..split], &mut replay_state);
        let expected_live = full[replay.len()..].to_vec();

        // This fixture is state-sensitive: replacing the handed-off replay state with a fresh
        // default loses the prior step's finish reason from the terminal event.
        let mut default_state = ControlProjectionState::default();
        let default_live = project_control_with_state(&records[split..], &mut default_state);
        assert_ne!(default_live, expected_live);

        let step_cursors: Vec<Cursor> = replay
            .iter()
            .filter(|(cursor, _)| cursor.wal_seq == 2)
            .map(|(cursor, _)| *cursor)
            .collect();
        assert_eq!(
            step_cursors,
            vec![
                Cursor::new(2, 0),
                Cursor::new(2, 1),
                Cursor::new(2, 2),
                Cursor::new(2, 3),
            ],
            "the replay side of the handoff preserves every multi-unit record cursor"
        );

        let (observer_tx, observer_rx) = broadcast::channel(4);
        let (ring, mut ring_rx) = broadcast::channel(4);
        let (detach_ring, _) = broadcast::channel(1);
        let cancel = CancellationToken::new();
        let (seed_tx, seed_rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(projector_loop(
            observer_rx,
            seed_rx,
            ring,
            detach_ring,
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicU64::new(1)),
            cancel.clone(),
        ));

        observer_tx.send(records[split].clone()).unwrap();
        seed_tx
            .send(ProjectionSeedMessage::State(ProjectionSeed {
                head: records[split - 1].seq,
                state: replay_state,
                gate_reopen: true,
                gate_tenure: 1,
            }))
            .unwrap();

        let mut continued = replay;
        for _ in 0..expected_live.len() {
            let event = tokio::time::timeout(std::time::Duration::from_secs(1), ring_rx.recv())
                .await
                .expect("seeded projector must continue promptly")
                .expect("live projection ring remains open");
            continued.push(event.as_ref().clone());
        }
        assert_eq!(
            continued, full,
            "replay projection followed by the handed-off state must equal uninterrupted projection"
        );

        cancel.cancel();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn replay_failure_is_delivered_as_an_explicit_error() {
        let session = BindIdentity::new("/tmp/project-a", "runner", "broken");
        let (mut fanout, _observer) = Fanout::new(Arc::new(FailingWalStore), session);
        let (slot, rx, detach_rx) = fanout.prepare_attach();
        let (sink, mut deliveries) = mpsc::channel(1);
        fanout.spawn_subscriber(
            slot,
            AttachFrom::Cursor(FromCursor::Start),
            0,
            rx,
            detach_rx,
            sink,
            None,
        );

        let delivery = tokio::time::timeout(std::time::Duration::from_secs(1), deliveries.recv())
            .await
            .expect("replay failure delivery must not hang")
            .expect("subscriber must send an explicit terminal error");
        match delivery {
            Delivery::Error(message) => assert!(message.contains("corrupted WAL")),
            other => panic!("expected replay Error delivery, got {other:?}"),
        }
        fanout.teardown().await;
    }

    #[tokio::test]
    async fn finished_subscriber_handles_are_reaped_on_the_next_tick() {
        let session = BindIdentity::new("/tmp/project-a", "runner", "reap");
        let (mut fanout, _observer) = Fanout::new(Arc::new(EmptyWalStore), session);
        let (slot, rx, detach_rx) = fanout.prepare_attach();
        let (sink, deliveries) = mpsc::channel(1);
        fanout.spawn_subscriber(
            slot,
            AttachFrom::Cursor(FromCursor::Start),
            0,
            rx,
            detach_rx,
            sink,
            None,
        );
        assert_eq!(fanout.subscriber_count(), 1);

        drop(deliveries);
        fanout
            .ring
            .send(Arc::new((Cursor::new(1, 0), ControlUnit::DisplayReset)))
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                fanout.reap_finished();
                if fanout.subscriber_count() == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the idle-tick reap must remove the disconnected subscriber handle");

        fanout.teardown().await;
    }
}
