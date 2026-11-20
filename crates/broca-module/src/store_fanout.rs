//! The `session.subscribe` stream of a session kept in the new storage.
//!
//! Its control lane keeps today's meaning: a subscriber gets every unit after its attach point
//! exactly once, first from a catch-up over the session's stored records, then from the live
//! projection, and its display lane is today's lossy one.
//!
//! - **The barrier.** The actor registers the subscriber's live receiver and takes the session's
//!   durable end H in one step between appends: its last seq, short of a batch appended without
//!   a barrier, which is published once a barrier makes it durable. A task then projects the
//!   stored records through H for a cursor attach. A live attach instead takes the open step's
//!   retained durable controls at H, before the display snapshot. Both deliver live units only past H
//!   ([`crate::fanout::deliver_from_ring`]), so a unit in both is delivered once.
//! - **Live.** The session calls [`Publisher::appended`] right after each append returns; each
//!   batch is projected once, when durable, and its units published on a bounded broadcast. A
//!   subscriber that falls behind is detached and resumes from its last cursor. The actor seeds
//!   the live projection in the background when it opens, from the open log's checkpoint through
//!   H, and again after the projection loses continuity; batches made durable meanwhile wait for
//!   it. The projection keeps its state and the cursor of the stream's last unit, which actor
//!   reads serve as `session.read`'s `head` from memory. Only the open step's visible controls are
//!   retained for live attaches; a queued attach is answered exactly when seeding reaches its H.
//! - **Cursors.** `wal_seq` is the store record's seq and `sub_index` the unit's index among that
//!   record's units. `incarnation` is the session head's incarnation in hex, as `session.read`
//!   serves it, and `digest` the first 8 bytes of HMAC-SHA256 over the record's canonical
//!   encoding, keyed from the session's data key ([`broca_session_store::keys::CursorKey`]).
//! - **Resume.** A cursor is checked against the record it names, as the session holds it now,
//!   before notice visibility filters that record's units: another incarnation, a seq past the
//!   durable end, a record a revert removed when it replaced the session's history, another
//!   digest, or a `sub_index` the record doesn't yield is refused `cursor_expired`. A
//!   position-only cursor resumes after its position unchecked, as today.
//! - **Catch-up cost.** A catch-up starts from the checkpoint of the log holding its cursor and
//!   folds forward from there, never from the session's start.
//! - **Display.** The run's provider output feeds today's [`DisplaySender`]: a current-step
//!   replay at attach, then live deltas, with gaps reported in band. The step a delta belongs to
//!   is taken from the records: a step's start resets the replay and names the message its
//!   deltas belong to.
//! - **Read access.** Access is checked for a stream's whole life: a change that takes it away
//!   ends the streams at that change (see [`StoreFanout::end_read_access`]).

use std::sync::{Arc, Mutex};

use broca_core::Cursor;
use broca_diagnostics::{spawn_blocking, JobKind};
use broca_session_store::backend::SessionBackend;
use broca_session_store::keys::CursorKey;
use broca_session_store::record::Record;
use broca_session_store::session::{Appended, LogFrom, Session, StoredRecords};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::fanout::{
    deliver, deliver_from_ring, AttachFrom, ControlEvent, Delivery, DisplaySender, FromCursor,
    StepReplay,
};
use crate::store_control::{StoreControl, StoreUnit};

/// What a store session's cursors carry besides their position.
#[derive(Clone)]
pub(crate) struct CursorIdentity {
    pub(crate) incarnation: [u8; 16],
    pub(crate) key: CursorKey,
}

impl CursorIdentity {
    pub(crate) fn of<B: SessionBackend>(session: &Session<B>) -> Self {
        Self {
            incarnation: session.head().incarnation.0,
            key: session.cursor_key(),
        }
    }

    /// The cursor of `unit`, whose record has `digest`.
    pub(crate) fn cursor(&self, unit: &StoreUnit, digest: u64) -> Cursor {
        Cursor::new(unit.seq, unit.sub_index).with_store_identity(self.incarnation, digest)
    }
}

/// One item of the live broadcast, in the order the session recorded what it stands for.
#[derive(Debug, Clone)]
pub(crate) enum LiveItem {
    AdmissionChanged,
    Unit(Arc<ControlEvent>),
    /// The live projection lost continuity: every subscriber resumes from its last cursor.
    Detach,
    /// Read access ended at this point (see [`StoreFanout::end_read_access`]).
    #[cfg_attr(not(test), allow(dead_code))]
    AccessEnded(Box<broca_core::ScopeRefusal>),
}

enum Live {
    /// Nothing seeds the live projection: before the first seeding, or after it lost
    /// continuity, until the actor seeds it again (see [`StoreFanout::ensure_seeded`]).
    Unseeded,
    /// A catch-up is projecting through its barrier; the batches made durable since wait for the
    /// state it reaches there.
    Seeding {
        generation: u64,
        waiting: Vec<(u64, Vec<Record>)>,
    },
    Seeded(Box<LiveProjection>),
}

const SEED_FAILED: &str = "the catch-up seeding the live projection failed";
type LiveReplay = Result<Vec<Arc<ControlEvent>>, String>;
type LiveAttaches = Vec<(u64, oneshot::Sender<LiveReplay>)>;

struct LiveProjection {
    control: StoreControl,
    last: Option<Cursor>,
    step: StepReplay<Arc<ControlEvent>>,
    through: u64,
}

impl LiveProjection {
    fn project(
        &mut self,
        seq: u64,
        record: &Record,
        next: Option<&Record>,
        identity: &CursorIdentity,
    ) -> Result<Vec<Arc<ControlEvent>>, String> {
        self.step.observe(record);
        let units = self.control.project(seq, record, next)?;
        let digest = identity.key.digest(record);
        let mut visible = Vec::new();
        for unit in units {
            let cursor = identity.cursor(&unit, digest);
            self.last = Some(cursor);
            if unit.visible {
                let event = Arc::new((cursor, unit.unit, unit.event));
                self.step.push(Arc::clone(&event));
                visible.push(event);
            }
        }
        self.through = seq;
        Ok(visible)
    }

    fn answer_attaches(&self, attaches: &mut LiveAttaches) {
        let mut pending = Vec::new();
        for (head, answer) in std::mem::take(attaches) {
            if head == self.through {
                let _ = answer.send(Ok(self.step.units().to_vec()));
            } else if head < self.through {
                let _ = answer.send(Err(SEED_FAILED.into()));
            } else {
                pending.push((head, answer));
            }
        }
        *attaches = pending;
    }
}

/// The live side of a store session's stream, shared by the session's append hook and the
/// catch-ups that seed it. It holds the projector's state, which the session's folds bound, and
/// the cursor of the stream's last unit, and only the open step's controls for live attaches.
pub(crate) struct Publisher {
    live: Live,
    /// Counts seedings, so a catch-up that started before the projection lost continuity can't
    /// seed the projection a later catch-up is seeding.
    seedings: u64,
    /// Batches appended without a barrier, published once a barrier makes them durable.
    unsynced: Vec<(u64, Vec<Record>)>,
    live_attaches: LiveAttaches,
    ring: broadcast::Sender<LiveItem>,
    display: DisplaySender,
    /// The id of the message the current step writes, which its display deltas name.
    display_mid: Arc<Mutex<Option<String>>>,
    identity: CursorIdentity,
}

impl Publisher {
    /// The hook the session calls right after each append returns (see
    /// `Session::observe_appends`): each batch's units are projected once and published, once the
    /// batch is durable.
    pub(crate) fn appended(&mut self, appended: Appended<'_>) {
        match appended {
            Appended::Records {
                first,
                records,
                durable,
            } => {
                self.display_context(records);
                self.unsynced.push((first, records.to_vec()));
                if durable {
                    self.publish_durable();
                }
            }
            Appended::Synced => self.publish_durable(),
            Appended::Replaced => {
                self.unsynced.clear();
                self.lose_continuity("a revert replaced the session's history");
            }
        }
    }

    /// Publishes the batches a barrier just made durable, in seq order.
    fn publish_durable(&mut self) {
        for (first, records) in std::mem::take(&mut self.unsynced) {
            match &mut self.live {
                Live::Unseeded => {}
                Live::Seeding { waiting, .. } => waiting.push((first, records)),
                Live::Seeded(projection) => {
                    match publish(
                        projection,
                        &self.identity,
                        &self.ring,
                        first,
                        &records,
                        &mut self.live_attaches,
                    ) {
                        Ok(()) => {}
                        Err(error) => self.lose_continuity(&error),
                    }
                }
            }
        }
    }

    /// Starts and ends display steps at the records that start and end model steps, as today's
    /// output drain does at a step's display context and its end.
    fn display_context(&self, records: &[Record]) {
        for record in records {
            match record {
                Record::StepStart(start) => {
                    self.display.finish_step();
                    *self.display_mid.lock().unwrap() = start.mid.clone();
                }
                Record::StepEnd(_) | Record::Pause(_) => self.display.finish_step(),
                Record::RunEnd(_) => {
                    self.display.finish_step();
                    *self.display_mid.lock().unwrap() = None;
                }
                _ => {}
            }
        }
    }

    /// Every subscriber detaches and resumes from its last cursor; the projection is seeded
    /// again by the actor's next seeding.
    fn lose_continuity(&mut self, why: &str) {
        tracing::warn!(
            target: "broca.store_fanout",
            reason = why,
            "the live projection lost continuity; subscribers resume from their cursors",
        );
        self.live = Live::Unseeded;
        for (_, answer) in std::mem::take(&mut self.live_attaches) {
            let _ = answer.send(Err(SEED_FAILED.into()));
        }
        let _ = self.ring.send(LiveItem::Detach);
    }

    /// Starts seeding the live projection from a catch-up through the barrier just taken, when
    /// nothing seeds it yet: the seeding's number, which the catch-up hands back with its state.
    fn start_seeding(&mut self) -> Option<u64> {
        if !matches!(self.live, Live::Unseeded) {
            return None;
        }
        self.seedings += 1;
        self.live = Live::Seeding {
            generation: self.seedings,
            waiting: Vec::new(),
        };
        Some(self.seedings)
    }

    /// The state the seeding catch-up reached at its head and the cursor of the last unit
    /// through it, or `None` when it failed. The batches made durable since are projected and
    /// published from it.
    fn seeded(&mut self, generation: u64, seed: Option<LiveProjection>) {
        if !matches!(&self.live, Live::Seeding { generation: seeding, .. } if *seeding == generation)
        {
            return;
        }
        let Live::Seeding { waiting, .. } = std::mem::replace(&mut self.live, Live::Unseeded)
        else {
            unreachable!("checked above");
        };
        let Some(mut projection) = seed else {
            self.lose_continuity(SEED_FAILED);
            return;
        };
        projection.answer_attaches(&mut self.live_attaches);
        for (first, records) in waiting {
            match publish(
                &mut projection,
                &self.identity,
                &self.ring,
                first,
                &records,
                &mut self.live_attaches,
            ) {
                Ok(()) => {}
                Err(error) => {
                    self.lose_continuity(&error);
                    return;
                }
            }
        }
        self.live = Live::Seeded(Box::new(projection));
    }

    /// The cursor of the stream's last unit, when the live projection is seeded: `Some(None)`
    /// for a stream with no unit yet, `None` while nothing is seeded.
    pub(crate) fn head(&self) -> Option<Option<Cursor>> {
        match &self.live {
            Live::Seeded(projection) => Some(projection.last),
            _ => None,
        }
    }

    /// The seq of the last durable record, given `last`, the seq of the last record appended:
    /// the records before the first batch still waiting for a barrier.
    fn durable_end(&self, last: u64) -> u64 {
        self.unsynced
            .first()
            .map_or(last, |(first, _)| first.saturating_sub(1))
    }
}

/// Projects each durable record once and answers live attaches at their exact barrier.
fn publish(
    projection: &mut LiveProjection,
    identity: &CursorIdentity,
    ring: &broadcast::Sender<LiveItem>,
    first: u64,
    records: &[Record],
    attaches: &mut LiveAttaches,
) -> Result<(), String> {
    for (offset, record) in records.iter().enumerate() {
        let seq = first + offset as u64;
        for event in projection.project(seq, record, records.get(offset + 1), identity)? {
            let _ = ring.send(LiveItem::Unit(event));
        }
        projection.answer_attaches(attaches);
    }
    Ok(())
}

/// The subscriptions of one store session, held by its actor.
pub(crate) struct StoreFanout<B> {
    labels: crate::store_awaits::Labels,
    publisher: Arc<Mutex<Publisher>>,
    display: DisplaySender,
    display_mid: Arc<Mutex<Option<String>>>,
    /// Handles on the session's storage that only read, one per catch-up.
    readers: Arc<dyn Fn() -> B + Send + Sync>,
    // Only expiry upgrades the weak sender, before stopping the task. Keeping a strong sender
    // here would leave a finished stream's channel open until the fanout itself was dropped.
    subscribers: Vec<(
        CancellationToken,
        JoinHandle<()>,
        mpsc::WeakSender<Delivery>,
    )>,
    /// Holds each seeding until notified, so a test can read while nothing is seeded.
    #[cfg(test)]
    seed_gate: Option<Arc<tokio::sync::Notify>>,
}

impl<B: SessionBackend + Send + Sync + 'static> StoreFanout<B> {
    /// The subscriptions of `session`, whose appends it observes from now on. A catch-up reads
    /// the session's records over a backend `readers` makes. The live projection starts seeding
    /// at once, in the background (see [`StoreFanout::ensure_seeded`]).
    pub(crate) fn new(
        session: &mut Session<B>,
        readers: Arc<dyn Fn() -> B + Send + Sync>,
        labels: crate::store_awaits::Labels,
    ) -> Self {
        let mut fanout = Self::unseeded(session, readers, labels);
        fanout.ensure_seeded(session);
        fanout
    }

    /// [`StoreFanout::new`], with each seeding held until `gate` is notified.
    #[cfg(test)]
    pub(crate) fn held(
        session: &mut Session<B>,
        readers: Arc<dyn Fn() -> B + Send + Sync>,
        gate: Arc<tokio::sync::Notify>,
        labels: crate::store_awaits::Labels,
    ) -> Self {
        let mut fanout = Self::unseeded(session, readers, labels);
        fanout.seed_gate = Some(gate);
        fanout.ensure_seeded(session);
        fanout
    }

    /// The subscriptions of `session` before anything seeds their live projection.
    fn unseeded(
        session: &mut Session<B>,
        readers: Arc<dyn Fn() -> B + Send + Sync>,
        labels: crate::store_awaits::Labels,
    ) -> Self {
        let display = DisplaySender::new(crate::fanout::DISPLAY_RING_CAPACITY);
        let display_mid = Arc::new(Mutex::new(None));
        let publisher = Arc::new(Mutex::new(Publisher {
            live: Live::Unseeded,
            seedings: 0,
            unsynced: Vec::new(),
            live_attaches: Vec::new(),
            ring: broadcast::channel(crate::fanout::RING_CAPACITY).0,
            display: display.clone(),
            display_mid: Arc::clone(&display_mid),
            identity: CursorIdentity::of(session),
        }));
        let hook = Arc::clone(&publisher);
        session.add_append_observer(Box::new(move |appended| {
            hook.lock().unwrap().appended(appended);
        }));
        Self {
            labels,
            publisher,
            display,
            display_mid,
            readers,
            subscribers: Vec::new(),
            #[cfg(test)]
            seed_gate: None,
        }
    }

    /// Seeds the live projection in the background when nothing seeds it: before the first
    /// seeding, and after it lost continuity. The seeding reads the open log over a backend of
    /// its own, from its checkpoint through the session's durable end, and never blocks the
    /// actor; batches made durable meanwhile wait for the state it reaches. Outside a runtime it
    /// does nothing; the next call inside one seeds.
    pub(crate) fn ensure_seeded(&mut self, session: &Session<B>) {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let (seeding, head, identity) = {
            let mut publisher = self.publisher.lock().unwrap();
            let head = publisher.durable_end(session.next_seq().saturating_sub(1));
            (publisher.start_seeding(), head, publisher.identity.clone())
        };
        let Some(generation) = seeding else {
            return;
        };
        let reader = session.stored_records((self.readers)());
        let labels = self.labels.clone();
        let publisher = Arc::clone(&self.publisher);
        #[cfg(test)]
        let gate = self.seed_gate.clone();
        runtime.spawn(async move {
            #[cfg(test)]
            if let Some(gate) = gate {
                gate.notified().await;
            }
            let read = tokio::runtime::Handle::current();
            let seeded = spawn_blocking(JobKind::Projection, move || {
                read.block_on(seed_with_labels(&reader, head, &identity, labels))
            })
            .await;
            let seed = match seeded {
                Ok(Ok(seed)) => Some(seed),
                Ok(Err(error)) => {
                    tracing::warn!(
                        target: "broca.store_fanout",
                        error = %error,
                        "seeding the live projection failed",
                    );
                    None
                }
                Err(error) => {
                    tracing::warn!(
                        target: "broca.store_fanout",
                        error = %error,
                        "the task seeding the live projection failed",
                    );
                    None
                }
            };
            publisher.lock().unwrap().seeded(generation, seed);
        });
    }

    /// The cursor of the stream's last unit, when the live projection is seeded.
    pub(crate) fn head(&self) -> Option<Option<Cursor>> {
        self.publisher.lock().unwrap().head()
    }

    /// Attaches a subscriber from `from`, delivering to `sink`: registers its live receiver and
    /// takes the session's durable end as its barrier, then catches it up in a task of its own.
    pub(crate) fn attach(
        &mut self,
        session: &Session<B>,
        from: AttachFrom,
        sink: mpsc::Sender<Delivery>,
    ) {
        self.subscribers.retain(|(_, task, _)| !task.is_finished());
        // The live projection a subscriber's stream continues from.
        self.ensure_seeded(session);
        let (rx, identity, head, live_replay) = {
            let mut publisher = self.publisher.lock().unwrap();
            let rx = publisher.ring.subscribe();
            let head = publisher.durable_end(session.next_seq().saturating_sub(1));
            let live_replay = if matches!(from, AttachFrom::Live) {
                let (answer, replay) = oneshot::channel();
                publisher.live_attaches.push((head, answer));
                if let Live::Seeded(projection) = &publisher.live {
                    // The publisher lock makes snapshot and ring registration one attach barrier.
                    let through = projection.through;
                    let step = projection.step.units().to_vec();
                    let (_, answer) = publisher.live_attaches.pop().unwrap();
                    let _ = answer.send(if through == head {
                        Ok(step)
                    } else {
                        Err(SEED_FAILED.into())
                    });
                }
                Some(replay)
            } else {
                None
            };
            (rx, publisher.identity.clone(), head, live_replay)
        };
        let cancel = CancellationToken::new();
        let terminal = sink.downgrade();
        let task = subscriber(Attach {
            labels: self.labels.clone(),
            from,
            head,
            rx,
            sink,
            reader: session.stored_records((self.readers)()),
            identity,
            display: self.display.clone(),
            cancel: cancel.clone(),
            live_replay,
        });
        self.subscribers
            .push((cancel, tokio::spawn(task), terminal));
    }

    /// Whether any subscriber is still attached. A subscriber's task is all there is of it: the
    /// task ends when its stream is dropped, when the stream ends it (a lost continuity, a lag,
    /// read access ending) or when this fanout is dropped. Finished tasks are let go of here, so
    /// a session held open for its subscribers keeps no entry for the ones that left.
    pub(crate) fn has_live_subscribers(&mut self) -> bool {
        self.subscribers.retain(|(_, task, _)| !task.is_finished());
        !self.subscribers.is_empty()
    }

    /// What the live side of the stream holds, estimated by walking it when asked (see
    /// `broca_session_store::held`): the projector's copies of the folds and its run facts, the
    /// open step's units, and the batches waiting for a barrier. A unit counts by its compact
    /// JSON, which stands in for its heap. The broadcast ring's retained units can't be walked:
    /// each counts only its slot.
    pub(crate) fn held_bytes(&self) -> usize {
        use broca_session_store::held::HeldBytes;
        let Ok(publisher) = self.publisher.lock() else {
            return 0;
        };
        let unit = |event: &Arc<ControlEvent>| {
            2 * std::mem::size_of::<usize>()
                + std::mem::size_of::<ControlEvent>()
                + serde_json::to_vec(&event.1).map_or(0, |bytes| bytes.len())
        };
        let live = match &publisher.live {
            Live::Unseeded => 0,
            Live::Seeding { waiting, .. } => waiting.heap_bytes(),
            Live::Seeded(projection) => {
                std::mem::size_of::<LiveProjection>()
                    + projection.control.held_bytes()
                    + projection.step.units().iter().map(unit).sum::<usize>()
            }
        };
        std::mem::size_of::<Self>()
            + std::mem::size_of::<Publisher>()
            + live
            + publisher.unsynced.heap_bytes()
            + publisher.ring.len() * std::mem::size_of::<LiveItem>()
            + self.subscribers.capacity()
                * std::mem::size_of::<(
                    CancellationToken,
                    JoinHandle<()>,
                    mpsc::WeakSender<Delivery>,
                )>()
    }

    /// The display lane's sender and the step its deltas belong to, for a run's output drain.
    pub(crate) fn display(&self) -> (DisplaySender, Arc<Mutex<Option<String>>>) {
        (self.display.clone(), Arc::clone(&self.display_mid))
    }

    /// Stops content delivery before deletion, then sends the content-free `Expired` terminal.
    /// A slow subscriber never holds the session lease.
    pub(crate) async fn expire(&mut self, expired_at_ms: u64) {
        let subscribers: Vec<_> = self
            .subscribers
            .drain(..)
            .map(|(cancel, task, sink)| (cancel, task, sink.upgrade()))
            .collect();
        for (cancel, _, _) in &subscribers {
            cancel.cancel();
        }
        for (_, task, sink) in subscribers {
            let _ = task.await;
            if let Some(sink) = sink {
                tokio::spawn(async move {
                    let _ = sink.send(Delivery::Expired { expired_at_ms }).await;
                });
            }
        }
    }

    /// Ends every stream at this point with `refusal`, the error a new subscribe would get: the
    /// hook for an accepted change that takes read access away, a refresh that drops the
    /// subscribers' principal or an adoption that changes the owner. No event recorded after it
    /// reaches them.
    ///
    /// Nothing calls it yet: this storage accepts no scope refresh, and no adoption it accepts
    /// changes the owner.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn end_read_access(&mut self, refusal: broca_core::ScopeRefusal) {
        let publisher = self.publisher.lock().unwrap();
        let _ = publisher
            .ring
            .send(LiveItem::AccessEnded(Box::new(refusal)));
    }

    /// Wake idle streams after durable policy acceptance. Each route checks its own caller,
    /// leaving subscribers whose admission is unchanged connected.
    pub(crate) fn recheck_read_access(&self) {
        let _ = self
            .publisher
            .lock()
            .unwrap()
            .ring
            .send(LiveItem::AdmissionChanged);
    }
}

impl<B> Drop for StoreFanout<B> {
    fn drop(&mut self) {
        for (_, answer) in std::mem::take(&mut self.publisher.lock().unwrap().live_attaches) {
            let _ = answer.send(Err(SEED_FAILED.into()));
        }
        for (cancel, _, _) in &self.subscribers {
            cancel.cancel();
        }
    }
}

/// Drains a store run's output into the display lane: provider deltas, each naming the message
/// its step writes. Control events come from the records, never from here.
pub(crate) fn drain_display(
    mut events: mpsc::Receiver<broca_core::OutputEvent>,
    (display, display_mid): (DisplaySender, Arc<Mutex<Option<String>>>),
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            if let broca_core::OutputEvent::Display(mut delta) = event {
                // The run names its deltas by their step's message; one it didn't name takes
                // the message the session is on.
                if delta.mid().is_none() {
                    delta.set_mid(display_mid.lock().unwrap().clone());
                }
                let _ = display.send(Arc::new(delta));
            }
        }
    })
}

/// One subscriber's attach.
struct Attach<R> {
    labels: crate::store_awaits::Labels,
    from: AttachFrom,
    /// The barrier: the session's durable end when the subscriber's receiver was registered.
    head: u64,
    rx: broadcast::Receiver<LiveItem>,
    sink: mpsc::Sender<Delivery>,
    reader: StoredRecords<R>,
    identity: CursorIdentity,
    display: DisplaySender,
    cancel: CancellationToken,
    live_replay: Option<oneshot::Receiver<LiveReplay>>,
}

/// What a catch-up delivers.
pub(crate) struct Replay {
    /// The units after the attach point that subscribers see.
    pub(crate) units: Vec<ControlEvent>,
    /// Whether a resume cursor names a unit of the record it names, as the session holds it.
    pub(crate) cursor_valid: bool,
}

/// Projects `log` through `head` for an attach from `from`, and checks a resume cursor against
/// the record it names before notice visibility filters any unit.
pub(crate) fn replay(
    log: LogFrom,
    from: AttachFrom,
    head: u64,
    identity: &CursorIdentity,
) -> Result<Replay, String> {
    replay_with_labels(log, from, head, identity, Default::default())
}

fn replay_with_labels(
    log: LogFrom,
    from: AttachFrom,
    head: u64,
    identity: &CursorIdentity,
    labels: crate::store_awaits::Labels,
) -> Result<Replay, String> {
    let LogFrom {
        checkpoint,
        records,
    } = log;
    let mut control = StoreControl::at_checkpoint(
        &checkpoint.conversation,
        &checkpoint.operational,
        checkpoint.covers_through,
    )
    .with_labels(labels);
    let delivered_from = match from {
        AttachFrom::Live => None,
        AttachFrom::Cursor(cursor) => Some(cursor),
    };
    let resume = match from {
        AttachFrom::Cursor(FromCursor::Resume { cursor, proof }) => Some((cursor, proof)),
        _ => None,
    };
    // A cursor past the durable end names no record the session holds; one whose record is gone
    // is never found.
    let mut cursor_valid = resume.is_none_or(|(cursor, _)| cursor.wal_seq <= head);
    let mut found = resume.is_none();
    let mut units = Vec::new();
    for (at, (seq, record)) in records.iter().enumerate() {
        let next = records.get(at + 1).map(|(_, next)| next);
        let projected = control.project(*seq, record, next)?;
        let named = resume.filter(|(cursor, _)| cursor.wal_seq == *seq);
        if projected.is_empty() && named.is_none() {
            continue;
        }
        let digest = identity.key.digest(record);
        if let Some((cursor, proof)) = named {
            found = true;
            // Every unit the record yields counts here, notices hidden from subscribers too.
            cursor_valid &= projected.iter().any(|unit| {
                unit.sub_index == cursor.sub_index && proof.matches(identity.cursor(unit, digest))
            });
        }
        let Some(delivered_from) = delivered_from else {
            continue;
        };
        for unit in projected {
            let cursor = identity.cursor(&unit, digest);
            if unit.visible && delivered_from.includes(cursor) {
                units.push((cursor, unit.unit, unit.event));
            }
        }
    }
    Ok(Replay {
        units,
        cursor_valid: cursor_valid && found,
    })
}

/// Where a projection reads a session's logs from: the writer's session itself, or a handle that
/// reads apart from it ([`StoredRecords`]).
#[allow(async_fn_in_trait)]
pub(crate) trait Logs {
    /// The log holding `seq`, from its checkpoint through `last`.
    async fn log_holding(&self, seq: u64, last: u64) -> Result<LogFrom, String>;
}

impl<B: SessionBackend> Logs for Session<B> {
    async fn log_holding(&self, seq: u64, last: u64) -> Result<LogFrom, String> {
        self.from_log_holding(seq, last)
            .await
            .map_err(|error| format!("the session's records can't be read: {error:?}"))
    }
}

impl<R: SessionBackend> Logs for StoredRecords<R> {
    async fn log_holding(&self, seq: u64, last: u64) -> Result<LogFrom, String> {
        self.from_log_holding(seq, last)
            .await
            .map_err(|error| format!("the session's records can't be read: {error:?}"))
    }
}

/// The open log projected once, retaining only the current step's visible controls.
fn project_log(log: LogFrom, identity: &CursorIdentity) -> Result<LiveProjection, String> {
    project_log_with_labels(log, identity, Default::default())
}

fn project_log_with_labels(
    log: LogFrom,
    identity: &CursorIdentity,
    labels: crate::store_awaits::Labels,
) -> Result<LiveProjection, String> {
    let mut projection = LiveProjection {
        control: StoreControl::at_checkpoint(
            &log.checkpoint.conversation,
            &log.checkpoint.operational,
            log.checkpoint.covers_through,
        )
        .with_labels(labels),
        last: None,
        step: StepReplay::default(),
        through: log.checkpoint.covers_through,
    };
    for (at, (seq, record)) in log.records.iter().enumerate() {
        let next = log.records.get(at + 1).map(|(_, next)| next);
        projection.project(*seq, record, next, identity)?;
    }
    Ok(projection)
}

/// The cursor of the stream's last unit through seq `through`, starting from the log holding
/// `seq`: that log's last unit, else the newest earlier log's that has one, reading one log at a
/// time. `None` when the stream has no unit through `through`.
async fn last_unit(
    logs: &impl Logs,
    mut seq: u64,
    mut through: u64,
    identity: &CursorIdentity,
) -> Result<Option<Cursor>, String> {
    loop {
        let log = logs.log_holding(seq, through).await?;
        let covers = log.checkpoint.covers_through;
        if let Some(last) = project_log(log, identity)?.last {
            return Ok(Some(last));
        }
        if covers == 0 {
            return Ok(None);
        }
        (seq, through) = (covers, covers);
    }
}

/// The live projection's seed: the open log projected from its checkpoint through `head`, the
/// session's durable end, and the cursor of the stream's last unit through it.
async fn seed(
    logs: &impl Logs,
    head: u64,
    identity: &CursorIdentity,
) -> Result<LiveProjection, String> {
    seed_with_labels(logs, head, identity, Default::default()).await
}

async fn seed_with_labels(
    logs: &impl Logs,
    head: u64,
    identity: &CursorIdentity,
    labels: crate::store_awaits::Labels,
) -> Result<LiveProjection, String> {
    let log = logs.log_holding(head + 1, head).await?;
    let covers = log.checkpoint.covers_through;
    let mut projection = project_log_with_labels(log, identity, labels)?;
    projection.last = match projection.last {
        Some(last) => Some(last),
        None if covers == 0 => None,
        // The open log has no unit yet: the last one is in sealed history.
        None => last_unit(logs, covers, covers, identity).await?,
    };
    projection.through = head;
    Ok(projection)
}

/// The cursor of the last unit of `session`'s control stream, which `session.read` serves as its
/// `head`: a subscriber resuming from it gets exactly the units recorded after what the read
/// covered. `None` when the stream has no unit yet.
pub(crate) async fn last_cursor<B: SessionBackend>(
    session: &Session<B>,
) -> Result<Option<Cursor>, String> {
    let head = session.next_seq().saturating_sub(1);
    last_unit(session, head + 1, head, &CursorIdentity::of(session)).await
}

/// Seeds a live projection from `session`'s open log, as the actor does when it opens, and
/// returns the projector state's estimated memory (see [`StoreControl::held_bytes`]) and the
/// cursor of the stream's last unit. For the store actor's read-cost measure
/// (`store_actor::read_cost`) and `session-convert subscribe-cost`; the actor seeds in its own
/// task.
#[doc(hidden)]
pub async fn measure_seed<B: SessionBackend>(
    session: &Session<B>,
) -> Result<(usize, Option<Cursor>), String> {
    let head = session.next_seq().saturating_sub(1);
    let projection = seed(session, head, &CursorIdentity::of(session)).await?;
    Ok((projection.control.held_bytes(), projection.last))
}

/// The cursor of the first unit of `session`'s open log, which a subscriber that followed the
/// session up to its last seal holds. `None` when the open log yields no unit yet. For the
/// `session-convert subscribe-cost` measure (crate `broca-session-convert`, which converts WAL
/// logs into the new storage); a subscription never calls it.
#[doc(hidden)]
pub async fn open_log_first_cursor<B: SessionBackend>(
    session: &Session<B>,
) -> Result<Option<broca_wire::CursorWire>, String> {
    let head = session.next_seq().saturating_sub(1);
    let identity = CursorIdentity::of(session);
    let log = session
        .from_log_holding(head + 1, head)
        .await
        .map_err(|error| format!("the session's records can't be read: {error:?}"))?;
    let mut control = StoreControl::at_checkpoint(
        &log.checkpoint.conversation,
        &log.checkpoint.operational,
        log.checkpoint.covers_through,
    );
    for (at, (seq, record)) in log.records.iter().enumerate() {
        let next = log.records.get(at + 1).map(|(_, next)| next);
        if let Some(unit) = control.project(*seq, record, next)?.into_iter().next() {
            let cursor = identity.cursor(&unit, identity.key.digest(record));
            return Ok(Some(cursor.into()));
        }
    }
    Ok(None)
}

/// A subscriber's catch-up from the resume cursor `cursor` through the session's last seq, as
/// its task runs it: the read from the checkpoint of the log holding the cursor, the projection
/// and the resume check. Returns how many units it delivers. For the `session-convert
/// subscribe-cost` measure (crate `broca-session-convert`); a subscription runs the same steps
/// in its own task.
#[doc(hidden)]
pub async fn catch_up_from<B: SessionBackend>(
    session: &Session<B>,
    cursor: &broca_wire::CursorWire,
) -> Result<usize, String> {
    let head = session.next_seq().saturating_sub(1);
    let proof = crate::fanout::ResumeCursorProof::new(
        cursor.incarnation.as_deref().unwrap_or_default(),
        cursor.digest.as_deref().unwrap_or_default(),
    )
    .ok_or("the cursor carries no well-formed identity")?;
    let from = AttachFrom::Cursor(FromCursor::Resume {
        cursor: Cursor::new(cursor.wal_seq, cursor.sub_index),
        proof,
    });
    let log = session
        .from_log_holding(cursor.wal_seq.min(head + 1), head)
        .await
        .map_err(|error| format!("the session's records can't be read: {error:?}"))?;
    let replayed = replay(log, from, head, &CursorIdentity::of(session))?;
    if !replayed.cursor_valid {
        return Err("cursor_expired".into());
    }
    Ok(replayed.units.len())
}

/// One subscriber: its catch-up through the barrier, then the live stream.
async fn subscriber<R: SessionBackend + Send + Sync + 'static>(attach: Attach<R>) {
    let Attach {
        labels,
        from,
        head,
        mut rx,
        sink,
        reader,
        identity,
        display,
        cancel,
        live_replay,
    } = attach;
    if let Some(replay) = live_replay {
        let replay = tokio::select! {
            _ = cancel.cancelled() => Err(SEED_FAILED.into()),
            _ = sink.closed() => return,
            replay = replay => replay.unwrap_or_else(|_| Err(SEED_FAILED.into())),
        };
        let units = match replay {
            Ok(units) => units,
            Err(error) => {
                // Closing the actor cancels subscribers too; still finish a queued attach with
                // the same error as a failed seed, without waiting on a closed or full sink.
                let _ = sink.try_send(Delivery::Error(error));
                return;
            }
        };
        for unit in units {
            if deliver(&sink, &cancel, Delivery::Control(unit))
                .await
                .is_break()
            {
                return;
            }
        }
    }
    // Cursor attaches read history; live attaches use only the projection's open-step snapshot.
    if !matches!(from, AttachFrom::Live) {
        let start = match from {
            AttachFrom::Live => head + 1,
            AttachFrom::Cursor(cursor) => cursor
                .position()
                .map_or(1, |position| position.wal_seq)
                .min(head + 1),
        };
        // The catch-up reads and projects on the blocking pool, off the runtime worker carrying
        // other sessions' streams; the read is driven there by the runtime's handle.
        let read_identity = identity.clone();
        let runtime = tokio::runtime::Handle::current();
        let replayed = match spawn_blocking(JobKind::Projection, move || {
            let log = runtime
                .block_on(reader.from_log_holding(start, head))
                .map_err(|error| format!("the session's records can't be read: {error:?}"))?;
            replay_with_labels(log, from, head, &read_identity, labels)
        })
        .await
        {
            Ok(replayed) => replayed,
            Err(error) => Err(format!("the catch-up task failed: {error}")),
        };
        let units = match replayed {
            Ok(Replay {
                cursor_valid: false,
                ..
            }) => {
                let _ = deliver(&sink, &cancel, Delivery::CursorExpired).await;
                return;
            }
            Ok(Replay { units, .. }) => units,
            Err(error) => {
                let _ = deliver(&sink, &cancel, Delivery::Error(error)).await;
                return;
            }
        };
        if matches!(from, AttachFrom::Cursor(_)) {
            for unit in units {
                if deliver(&sink, &cancel, Delivery::Control(Arc::new(unit)))
                    .await
                    .is_break()
                {
                    return;
                }
            }
        }
    }
    let from = match from {
        AttachFrom::Live => FromCursor::Start,
        AttachFrom::Cursor(cursor) => cursor,
    };
    let (mut display_rx, snapshot) = display.subscribe_snapshot();
    for event in snapshot {
        if deliver(&sink, &cancel, Delivery::Display(Arc::new(event)))
            .await
            .is_break()
        {
            return;
        }
    }
    let mut display_open = true;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = sink.closed() => return,
            item = rx.recv() => match item {
                Ok(LiveItem::Unit(event)) => {
                    // The barrier: a unit at or before the head came with the catch-up.
                    if !deliver_from_ring(event.0, head, from) {
                        continue;
                    }
                    if deliver(&sink, &cancel, Delivery::Control(event)).await.is_break() {
                        return;
                    }
                }
                Ok(LiveItem::AdmissionChanged) => {
                    if deliver(&sink, &cancel, Delivery::AdmissionChanged).await.is_break() {
                        return;
                    }
                }
                // A control unit was missed: never deliver what follows as contiguous.
                Ok(LiveItem::Detach) | Err(broadcast::error::RecvError::Lagged(_)) => {
                    let _ = deliver(&sink, &cancel, Delivery::Detached).await;
                    return;
                }
                Ok(LiveItem::AccessEnded(refusal)) => {
                    let _ = deliver(&sink, &cancel, Delivery::AccessEnded(refusal)).await;
                    return;
                }
                // The session closed.
                Err(broadcast::error::RecvError::Closed) => return,
            },
            event = display_rx.recv(), if display_open => match event {
                Ok(event) => {
                    if deliver(&sink, &cancel, Delivery::Display(event)).await.is_break() {
                        return;
                    }
                }
                // Display is lossy: a gap is reported in band, never a detach.
                Err(broadcast::error::RecvError::Lagged(dropped)) => {
                    let gap = broca_protocol::display::DisplayEvent::DisplayGap { dropped };
                    if deliver(&sink, &cancel, Delivery::Display(Arc::new(gap))).await.is_break() {
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Closed) => display_open = false,
            }
        }
    }
}

#[cfg(test)]
mod tests;
