//! The `session.subscribe` stream of a session kept in the new storage.
//!
//! Its control lane keeps today's meaning: a subscriber gets every unit after its attach point
//! exactly once, first from a catch-up over the session's stored records, then from the live
//! projection, and its display lane is today's lossy one.
//!
//! - **The barrier.** The actor registers the subscriber's live receiver and takes the session's
//!   last seq H in one step between appends. A store append is durable when it returns, so no
//!   sync is needed. A task then projects the stored records through H, and delivers live units
//!   only past H ([`crate::fanout::deliver_from_ring`]), so a unit in both is delivered once.
//! - **Live.** The session calls [`Publisher::appended`] right after each append returns; the
//!   batch is projected once and its units published on a bounded broadcast. A subscriber that
//!   falls behind is detached and resumes from its last cursor. The live projection starts from
//!   the state the first subscriber's catch-up reaches at its H; batches appended meanwhile wait
//!   for it.
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
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::fanout::{
    deliver, deliver_from_ring, AttachFrom, ControlEvent, Delivery, DisplaySender, FromCursor,
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
    Unit(Arc<ControlEvent>),
    /// The live projection lost continuity: every subscriber resumes from its last cursor.
    Detach,
    /// Read access ended at this point (see [`StoreFanout::end_read_access`]).
    #[cfg_attr(not(test), allow(dead_code))]
    AccessEnded(Box<broca_core::ScopeRefusal>),
}

enum Live {
    /// No subscriber has needed the live projection yet.
    Unseeded,
    /// A subscriber's catch-up is projecting through its barrier; the batches appended since
    /// wait for the state it reaches there.
    Seeding {
        generation: u64,
        waiting: Vec<(u64, Vec<Record>)>,
    },
    Seeded(Box<StoreControl>),
}

/// The live side of a store session's stream, shared by the session's append hook and the
/// subscribers that seed it.
pub(crate) struct Publisher {
    live: Live,
    /// Counts seedings, so a catch-up that started before the projection lost continuity can't
    /// seed the projection a later catch-up is seeding.
    seedings: u64,
    ring: broadcast::Sender<LiveItem>,
    display: DisplaySender,
    /// The id of the message the current step writes, which its display deltas name.
    display_mid: Arc<Mutex<Option<String>>>,
    identity: CursorIdentity,
}

impl Publisher {
    /// The hook the session calls right after each append returns (see
    /// `Session::observe_appends`): the batch's units are projected once and published.
    pub(crate) fn appended(&mut self, appended: Appended<'_>) {
        match appended {
            Appended::Records { first, records } => {
                self.display_context(records);
                match &mut self.live {
                    Live::Unseeded => {}
                    Live::Seeding { waiting, .. } => waiting.push((first, records.to_vec())),
                    Live::Seeded(control) => {
                        if let Err(error) =
                            publish(control, &self.identity, &self.ring, first, records)
                        {
                            self.lose_continuity(&error);
                        }
                    }
                }
            }
            Appended::Replaced => self.lose_continuity("a revert replaced the session's history"),
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

    /// Every subscriber detaches and resumes from its last cursor; the next one seeds the live
    /// projection again.
    fn lose_continuity(&mut self, why: &str) {
        tracing::warn!(
            target: "broca.store_fanout",
            reason = why,
            "the live projection lost continuity; subscribers resume from their cursors",
        );
        self.live = Live::Unseeded;
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

    /// The state the seeding catch-up reached at its head, or `None` when it failed. The
    /// batches appended since are projected and published from it.
    fn seeded(&mut self, generation: u64, control: Option<StoreControl>) {
        if !matches!(&self.live, Live::Seeding { generation: seeding, .. } if *seeding == generation)
        {
            return;
        }
        let Live::Seeding { waiting, .. } = std::mem::replace(&mut self.live, Live::Unseeded)
        else {
            unreachable!("checked above");
        };
        let Some(mut control) = control else {
            self.lose_continuity("the catch-up seeding the live projection failed");
            return;
        };
        for (first, records) in waiting {
            if let Err(error) = publish(&mut control, &self.identity, &self.ring, first, &records) {
                self.lose_continuity(&error);
                return;
            }
        }
        self.live = Live::Seeded(Box::new(control));
    }
}

/// Projects `records`, the first at seq `first`, and publishes the units subscribers see.
fn publish(
    control: &mut StoreControl,
    identity: &CursorIdentity,
    ring: &broadcast::Sender<LiveItem>,
    first: u64,
    records: &[Record],
) -> Result<(), String> {
    for (offset, record) in records.iter().enumerate() {
        let seq = first + offset as u64;
        let units = control.project(seq, record, records.get(offset + 1))?;
        if units.is_empty() {
            continue;
        }
        let digest = identity.key.digest(record);
        for unit in units.into_iter().filter(|unit| unit.visible) {
            let cursor = identity.cursor(&unit, digest);
            let _ = ring.send(LiveItem::Unit(Arc::new((cursor, unit.unit, unit.event))));
        }
    }
    Ok(())
}

/// The subscriptions of one store session, held by its actor.
pub(crate) struct StoreFanout<B> {
    publisher: Arc<Mutex<Publisher>>,
    display: DisplaySender,
    display_mid: Arc<Mutex<Option<String>>>,
    /// Handles on the session's storage that only read, one per catch-up.
    readers: Arc<dyn Fn() -> B + Send + Sync>,
    subscribers: Vec<(CancellationToken, JoinHandle<()>)>,
}

impl<B: SessionBackend + Send + Sync + 'static> StoreFanout<B> {
    /// The subscriptions of `session`, whose appends it observes from now on. A catch-up reads
    /// the session's records over a backend `readers` makes.
    pub(crate) fn new(session: &mut Session<B>, readers: Arc<dyn Fn() -> B + Send + Sync>) -> Self {
        let display = DisplaySender::new(crate::fanout::DISPLAY_RING_CAPACITY);
        let display_mid = Arc::new(Mutex::new(None));
        let publisher = Arc::new(Mutex::new(Publisher {
            live: Live::Unseeded,
            seedings: 0,
            ring: broadcast::channel(crate::fanout::RING_CAPACITY).0,
            display: display.clone(),
            display_mid: Arc::clone(&display_mid),
            identity: CursorIdentity::of(session),
        }));
        let hook = Arc::clone(&publisher);
        session.observe_appends(Box::new(move |appended| {
            hook.lock().unwrap().appended(appended);
        }));
        Self {
            publisher,
            display,
            display_mid,
            readers,
            subscribers: Vec::new(),
        }
    }

    /// Attaches a subscriber from `from`, delivering to `sink`: registers its live receiver and
    /// takes the session's last seq as its barrier, then catches it up in a task of its own.
    pub(crate) fn attach(
        &mut self,
        session: &Session<B>,
        from: AttachFrom,
        sink: mpsc::Sender<Delivery>,
    ) {
        self.subscribers.retain(|(_, task)| !task.is_finished());
        let head = session.next_seq().saturating_sub(1);
        let (rx, identity, seeding) = {
            let mut publisher = self.publisher.lock().unwrap();
            let rx = publisher.ring.subscribe();
            let seeding = publisher.start_seeding();
            (rx, publisher.identity.clone(), seeding)
        };
        let cancel = CancellationToken::new();
        let task = subscriber(Attach {
            from,
            head,
            rx,
            sink,
            reader: session.stored_records((self.readers)()),
            identity,
            seed: seeding.map(|generation| (Arc::clone(&self.publisher), generation)),
            display: self.display.clone(),
            cancel: cancel.clone(),
        });
        self.subscribers.push((cancel, tokio::spawn(task)));
    }

    /// The display lane's sender and the step its deltas belong to, for a run's output drain.
    pub(crate) fn display(&self) -> (DisplaySender, Arc<Mutex<Option<String>>>) {
        (self.display.clone(), Arc::clone(&self.display_mid))
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
}

impl<B> Drop for StoreFanout<B> {
    fn drop(&mut self) {
        for (cancel, _) in &self.subscribers {
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
                delta.set_mid(display_mid.lock().unwrap().clone());
                let _ = display.send(Arc::new(delta));
            }
        }
    })
}

/// One subscriber's attach.
struct Attach<R> {
    from: AttachFrom,
    /// The barrier: the session's last seq when the subscriber's receiver was registered.
    head: u64,
    rx: broadcast::Receiver<LiveItem>,
    sink: mpsc::Sender<Delivery>,
    reader: StoredRecords<R>,
    identity: CursorIdentity,
    /// The live projection this catch-up seeds, and the seeding's number.
    seed: Option<(Arc<Mutex<Publisher>>, u64)>,
    display: DisplaySender,
    cancel: CancellationToken,
}

/// What a catch-up delivers, and the projection's state at its end.
pub(crate) struct Replay {
    /// The units after the attach point that subscribers see.
    pub(crate) units: Vec<ControlEvent>,
    /// Whether a resume cursor names a unit of the record it names, as the session holds it.
    pub(crate) cursor_valid: bool,
    pub(crate) control: StoreControl,
}

/// Projects `log` through `head` for an attach from `from`, and checks a resume cursor against
/// the record it names before notice visibility filters any unit.
pub(crate) fn replay(
    log: LogFrom,
    from: AttachFrom,
    head: u64,
    identity: &CursorIdentity,
) -> Result<Replay, String> {
    let LogFrom {
        checkpoint,
        records,
    } = log;
    let mut control = StoreControl::at_checkpoint(
        &checkpoint.conversation,
        &checkpoint.operational,
        checkpoint.covers_through,
    );
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
        control,
    })
}

/// The cursor of the last unit of `session`'s control stream, which `session.read` serves as its
/// `head`: a subscriber resuming from it gets exactly the units recorded after what the read
/// covered. `None` when the stream has no unit yet.
pub(crate) async fn last_cursor<B: SessionBackend>(
    session: &Session<B>,
) -> Result<Option<Cursor>, String> {
    let head = session.next_seq().saturating_sub(1);
    let identity = CursorIdentity::of(session);
    // The open log's last unit, or, when the open log yields none yet, the session's.
    for start in [head + 1, 1] {
        let log = session
            .from_log_holding(start, head)
            .await
            .map_err(|error| format!("the session's records can't be read: {error:?}"))?;
        let mut control = StoreControl::at_checkpoint(
            &log.checkpoint.conversation,
            &log.checkpoint.operational,
            log.checkpoint.covers_through,
        );
        let mut last = None;
        for (at, (seq, record)) in log.records.iter().enumerate() {
            let next = log.records.get(at + 1).map(|(_, next)| next);
            if let Some(unit) = control.project(*seq, record, next)?.pop() {
                last = Some((unit, record));
            }
        }
        if let Some((unit, record)) = last {
            return Ok(Some(identity.cursor(&unit, identity.key.digest(record))));
        }
    }
    Ok(None)
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
        from,
        head,
        mut rx,
        sink,
        reader,
        identity,
        seed,
        display,
        cancel,
    } = attach;
    if !matches!(from, AttachFrom::Live) || seed.is_some() {
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
            replay(log, from, head, &read_identity)
        })
        .await
        {
            Ok(replayed) => replayed,
            Err(error) => Err(format!("the catch-up task failed: {error}")),
        };
        let replayed = match replayed {
            Ok(Replay {
                units,
                cursor_valid,
                control,
            }) => {
                if let Some((publisher, generation)) = &seed {
                    publisher.lock().unwrap().seeded(*generation, Some(control));
                }
                Ok((units, cursor_valid))
            }
            Err(error) => {
                if let Some((publisher, generation)) = &seed {
                    publisher.lock().unwrap().seeded(*generation, None);
                }
                Err(error)
            }
        };
        drop(seed);
        let units = match replayed {
            Ok((_, false)) => {
                let _ = deliver(&sink, &cancel, Delivery::CursorExpired).await;
                return;
            }
            Ok((units, true)) => units,
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
