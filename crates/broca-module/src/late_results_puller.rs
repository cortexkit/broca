//! The module's intake for provider-held calls: one puller per provider module, the
//! classification of every pulled entry, and the ack that follows.
//!
//! A held call is not re-dispatched and its result does not come back on the tool route. The
//! provider settles the call later and serves the entry only to its custodian, so broca pulls:
//!
//! - one feed per (provider module, broca's principal), owned by the module and never by a
//!   session or a scope, because the custodian is the module and every session on it shares the
//!   one cursor;
//! - one acked position per feed, and every `through` sent is a cursor the provider returned;
//! - a registration per dispatched call, written before that call's key was sent, so an entry's
//!   lineage is found from the store and never from the rebuildable run index.
//!
//! A puller runs only while the module still needs its feed: a registered call with an open held
//! record or an owed batch commit barrier, an entry no ack covers yet, or a registered WAL the
//! intake cannot read (which may hide a hold). Ordinary calls to a provider that can settle late
//! never start one.
//!
//! This module deliberately enables nothing itself. The tool plane declares the held-reply
//! capabilities on its routes, and the run holds a batch open; this module is the store, the
//! puller and the delivery hook that code is written against.

use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use broca_core::tool_plane::LateResultRegistration;
use broca_core::{FileWalStore, HeldCallFacts, WalStore};
use broca_protocol::{BindIdentity, ToolCallId};
use broca_wal::{DispatchOutcome, FramedRecord, WalRangeRecords, WalRecord};
use cortexkit_role_tool_provider::late_results::{Cursor, EntryKind, IntakeKey, LateEntry};

use crate::late_results_store::{
    Disposition, FeedPosition, LateResultsStore, RegistrationRow, RegistrationState,
};

/// How often a started feed is pulled.
pub const PULL_INTERVAL: Duration = Duration::from_secs(3);
/// The longest wait between attempts after a transport error or a refusal.
pub const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// The bind a feed's route is opened under. The custodian comes from the connection's consumer
/// identity, so this only names the route; it is fixed so one feed keeps one route open.
const FEED_HARNESS: &str = "broca";
const FEED_SESSION: &str = "late-results";

/// The provider feeds the intake pulls, reduced to what it acts on.
#[async_trait]
pub trait LateResultsFeed: Send + Sync {
    /// Pull the entries after `since` for one provider module. Fragments filled in by the
    /// callee (its cursor, whether more wait) stay as given.
    async fn pull(
        &self,
        module: &str,
        since: Option<&Cursor>,
        limit: Option<u32>,
    ) -> Result<FeedReply, FeedError>;
    /// Acknowledge one provider module's entries through a cursor it returned.
    async fn ack(&self, module: &str, through: &Cursor) -> Result<(), FeedError>;
}

/// One `late_results` reply.
#[derive(Clone, Debug, PartialEq)]
pub struct FeedReply {
    pub entries: Vec<LateEntry>,
    pub cursor: Cursor,
    pub more: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FeedError {
    /// The provider refused the cursor: it names another incarnation, or a position past that
    /// incarnation's entries. The reader re-reads from the start.
    CursorIncarnationChanged,
    /// The module no longer offers this feed, so an ack-only puller gives up on it.
    Abandoned(String),
    Retryable(String),
}

/// What became of one pulled entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Classified {
    /// Handed to the call's batch, which writes the terminal.
    Delivered,
    /// Settled without delivery: the call is already terminal, its episode is closing, it was
    /// dispatched elsewhere, its scope does not match, its kind is unknown, or no registration
    /// names its lineage. A no-op never closes an open call.
    NoOp,
    /// The lineage is registered but the call's held record is not durable yet. It pins the ack.
    Pending,
    /// Nothing can be concluded yet, so the ack stops before it and the entry is retried.
    Blocked(String),
}

impl Classified {
    pub fn settles(&self) -> bool {
        matches!(self, Classified::Delivered | Classified::NoOp)
    }
}

/// Delivers one settled entry to the call's run. The run's batch is the only writer of a held
/// call's terminal, and the entry is that terminal's content: the held call is never sent to its
/// provider again.
#[async_trait]
pub trait HeldDelivery: Send + Sync {
    /// Hand the entry to the batch of the held call `tool_call_id`. `Ok(())` means the terminal
    /// is durable.
    async fn deliver(
        &self,
        registration: &LateResultRegistration,
        tool_call_id: &ToolCallId,
        entry: &LateEntry,
    ) -> Result<(), String>;
}

/// What one tick concluded about a module's puller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StartOutcome {
    /// A puller now runs for the module.
    Started,
    /// Nothing keeps a puller for this module.
    Idle,
    /// Another puller already holds the module.
    Held,
}

struct PullerState {
    /// Entries an earlier reply left unacked because one of them could not be settled, in reply
    /// order.
    pinned: Vec<IntakeKey>,
}

/// One running feed.
pub struct Puller {
    state: Mutex<PullerState>,
    interval: Mutex<Duration>,
    stopped: Mutex<bool>,
}

impl Puller {
    fn new() -> Self {
        Self {
            state: Mutex::new(PullerState { pinned: Vec::new() }),
            interval: Mutex::new(PULL_INTERVAL),
            stopped: Mutex::new(false),
        }
    }

    /// Entries an earlier reply left unacked because one of them could not be settled.
    pub fn pinned(&self) -> Vec<IntakeKey> {
        self.state.lock().expect("puller state").pinned.clone()
    }

    fn set_pinned(&self, pinned: Vec<IntakeKey>) {
        self.state.lock().expect("puller state").pinned = pinned;
    }

    fn interval(&self) -> Duration {
        *self.interval.lock().expect("puller interval")
    }

    fn back_off(&self) {
        let next = (self.interval() * 2).min(MAX_BACKOFF);
        *self.interval.lock().expect("puller interval") = next;
    }

    fn reset_interval(&self) {
        *self.interval.lock().expect("puller interval") = PULL_INTERVAL;
    }

    /// Finish the current attempt and stop, without pulling again.
    pub fn stop(&self) {
        *self.stopped.lock().expect("puller stop") = true;
    }

    pub fn is_stopped(&self) -> bool {
        *self.stopped.lock().expect("puller stop")
    }
}

/// The intake for every provider module in this process.
pub struct LateResultsIntake {
    store: Arc<LateResultsStore>,
    wal: Arc<FileWalStore>,
    /// The module id the daemon attests for this process: the feed's custodian. Without one no
    /// plain key is sent and no feed is pulled.
    consumer_module: String,
    delivery: Arc<dyn HeldDelivery>,
    feed: Arc<dyn LateResultsFeed>,
    /// The running pullers, one per provider module. The claim lives only in this process: a
    /// puller never survives its process, so nothing durable may say one is running.
    pullers: Mutex<HashMap<String, Arc<Puller>>>,
    /// Set while the store is being rebuilt from the WALs. Until the rebuild completes, no
    /// registration can be trusted to be missing, so every entry is Blocked and nothing is acked.
    rebuilding: std::sync::atomic::AtomicBool,
}

impl LateResultsIntake {
    pub fn new(
        store: Arc<LateResultsStore>,
        wal: Arc<FileWalStore>,
        consumer_module: impl Into<String>,
        delivery: Arc<dyn HeldDelivery>,
        feed: Arc<dyn LateResultsFeed>,
    ) -> Arc<Self> {
        Arc::new(Self {
            store,
            wal,
            consumer_module: consumer_module.into(),
            delivery,
            feed,
            pullers: Mutex::new(HashMap::new()),
            rebuilding: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Mark the store as being rebuilt (or done). See [`Self::rebuild_from_wals`].
    pub fn set_rebuilding(&self, rebuilding: bool) {
        self.rebuilding
            .store(rebuilding, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn is_rebuilding(&self) -> bool {
        self.rebuilding.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn store(&self) -> &Arc<LateResultsStore> {
        &self.store
    }

    /// The custodian string the provider stamps on entries served to this process.
    pub fn custodian(&self) -> String {
        format!("reserved:{}", self.consumer_module)
    }

    /// The one bind every feed route is opened under.
    pub fn feed_identity(state_root: &Path) -> BindIdentity {
        BindIdentity::new(
            state_root.to_string_lossy().to_string(),
            FEED_HARNESS,
            FEED_SESSION,
        )
    }

    pub fn running(&self) -> Vec<String> {
        let mut modules: Vec<String> = self
            .pullers
            .lock()
            .expect("pullers")
            .keys()
            .cloned()
            .collect();
        modules.sort();
        modules
    }

    fn puller(&self, module: &str) -> Option<Arc<Puller>> {
        self.pullers.lock().expect("pullers").get(module).cloned()
    }

    /// Read one registered lineage's whole WAL without the run index.
    pub fn lineage_view(&self, registration: &LateResultRegistration) -> LineageView {
        let mut view = match self.readable(&registration.bind) {
            Some(records) => LineageView {
                facts: broca_core::held_call_facts(&records, Some(&registration.call_key)),
                closing: closing_episode(&records),
                records,
                tombstoned: false,
            },
            None => LineageView {
                // A WAL the intake cannot read proves nothing: every entry for it is deferred.
                facts: HeldCallFacts {
                    unreadable: true,
                    ..HeldCallFacts::default()
                },
                closing: false,
                records: Vec::new(),
                tombstoned: false,
            },
        };
        view.tombstoned = self.tombstoned(&registration.bind, &registration.lineage);
        view
    }

    /// The records of one registered lineage, when the intake can read them. An absent WAL
    /// proves nothing about a hold, so it is `None`, never empty.
    fn readable(&self, session: &BindIdentity) -> Option<Vec<FramedRecord>> {
        self.wal.read_records(session).ok()
    }

    /// Whether retention already tombstoned this lineage. A session deleted through the
    /// unreadable-WAL fallback can leave its live WAL behind, and then this is the only evidence
    /// that nothing of it can be delivered any more.
    fn tombstoned(&self, session: &BindIdentity, lineage: &str) -> bool {
        let directory = self
            .wal
            .ephemeral_root()
            .join(format!("{}.expired", broca_core::session_addr(session)));
        directory.join(lineage).exists()
    }

    /// Whether the module's feed is still needed, decided from the store's registrations and the
    /// WALs they name: an owed ack, a registered call whose WAL has an open held call or an owed
    /// batch commit barrier, or a registered WAL that cannot be read (which may hide a hold).
    ///
    /// The same pass prunes the registrations that are settled for good, so a registration
    /// never outlives the commit of the batch that settled its call.
    pub fn module_needs_feed(&self, module: &str) -> Result<bool, String> {
        let holds = self.module_holds_calls(module)?;
        Ok(holds || self.store.feed(module)?.is_some_and(|feed| feed.ack_owed))
    }

    /// Start the module's puller when its feed is still needed and none runs. The claim lives in
    /// this process only: a puller never outlives its process, so a crash leaves nothing that
    /// could keep the next process from starting one.
    pub fn ensure_puller(self: &Arc<Self>, module: &str) -> Result<StartOutcome, String> {
        if !self.module_needs_feed(module)? {
            return Ok(StartOutcome::Idle);
        }
        let puller = Arc::new(Puller::new());
        {
            // The claim is the insert itself, under the lock: two starts racing for one module
            // cannot both win.
            let mut pullers = self.pullers.lock().expect("pullers");
            if pullers.contains_key(module) {
                return Ok(StartOutcome::Held);
            }
            pullers.insert(module.to_owned(), Arc::clone(&puller));
        }
        self.spawn(module.to_owned(), puller);
        Ok(StartOutcome::Started)
    }

    /// Start a module's puller after boot recovery registered its calls. Same claim, same loop.
    pub fn ensure_puller_after_recovery(
        self: &Arc<Self>,
        module: &str,
    ) -> Result<StartOutcome, String> {
        self.ensure_puller(module)
    }

    /// Run the module's puller until nothing keeps it, then do one last pull and ack, release the
    /// slot and stop. It never restarts itself.
    fn spawn(self: &Arc<Self>, module: String, puller: Arc<Puller>) {
        let intake = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                if puller.is_stopped() {
                    break;
                }
                let needed = intake.module_needs_feed(&module).unwrap_or(true);
                if !needed {
                    break;
                }
                intake.tick_with(&module, &puller).await;
                if puller.is_stopped() {
                    break;
                }
                tokio::time::sleep(puller.interval()).await;
            }
            let _ = intake.final_pull(&module).await;
            intake.pullers.lock().expect("pullers").remove(&module);
        });
    }

    /// One pull cycle for a module whose puller runs.
    pub async fn tick(&self, module: &str) -> Result<(), String> {
        let Some(puller) = self.puller(module) else {
            return Ok(());
        };
        self.tick_with(module, &puller).await;
        Ok(())
    }

    async fn tick_with(&self, module: &str, puller: &Puller) {
        match self.pull_round(module, puller).await {
            Ok(()) => puller.reset_interval(),
            Err(FeedError::CursorIncarnationChanged) => {
                // The provider changed incarnation. Its `event_id`s are deterministic across
                // restarts, so the receipts absorb the re-read from the start.
                let _ = self.store.forget_cursor(module);
                puller.set_pinned(Vec::new());
                puller.reset_interval();
            }
            Err(FeedError::Abandoned(reason)) => match self.module_holds_calls(module) {
                Ok(true) => {
                    // A module with an open held call or owed barrier keeps its backoff: its
                    // calls stay paused until the feed answers again.
                    tracing::warn!(
                        target: "late_results",
                        module,
                        "late-results feed unavailable while a call is held: {reason}"
                    );
                    puller.back_off();
                }
                _ => {
                    tracing::info!(
                        target: "late_results",
                        module,
                        "late-results feed abandoned; clearing the owed ack: {reason}"
                    );
                    let _ = self.store.set_ack_owed(module, false);
                    let _ = self.store.forget_cursor(module);
                }
            },
            Err(FeedError::Retryable(reason)) => {
                // A deferred pull repeats every backoff, so it stays at INFO rather than
                // vanishing on a daemon whose log root is the default.
                tracing::info!(target: "late_results", module, "late-results pull deferred: {reason}");
                puller.back_off();
            }
        }
    }

    /// Whether the module still has a held call or an owed barrier: the condition that outranks
    /// an abandoned ack. Prunes every registration whose call is settled for good on the way.
    pub fn module_holds_calls(&self, module: &str) -> Result<bool, String> {
        let mut holds = false;
        for row in self.store.registrations_for(module)? {
            match self.readable(&row.call.bind) {
                Some(records) => {
                    let facts = broca_core::held_call_facts(&records, Some(&row.call.call_key));
                    if facts.needs_intake() {
                        holds = true;
                    } else if registration_settled(&row.call.call_key, &records, &facts) {
                        self.store.remove(module, &row.call.call_key)?;
                    }
                }
                None if self.tombstoned(&row.call.bind, &row.call.lineage) => {
                    // Retention deleted the session: nothing of it can be delivered any more.
                    self.store.remove(module, &row.call.call_key)?;
                }
                // An unreadable registered WAL may hide a hold, so the feed stays.
                None => holds = true,
            }
        }
        Ok(holds)
    }

    /// Settle the pinned entries again, then deliver everything newer. An entry the ack cannot
    /// pass pins the ack where it is and never keeps a newly published entry from being
    /// delivered: the pinned entries are read from the start of the incarnation, and pages after
    /// that reply from its cursor, so no page is read twice.
    async fn pull_round(&self, module: &str, puller: &Puller) -> Result<(), FeedError> {
        let mut read_from = self
            .store
            .feed(module)
            .map_err(FeedError::Retryable)?
            .and_then(|row| row.position);
        if !puller.pinned().is_empty() {
            let reply = self.pull(module, read_from.as_ref(), None).await?;
            read_from = Some(FeedPosition {
                provider_incarnation: reply.cursor.provider_incarnation.clone(),
                seq: reply.cursor.seq,
            });
            // While anything in the pinned reply is unsettled it is never acked: any ack
            // covering it would pass the pin.
            let (settled, still) = self.settle(module, &reply.entries).await?;
            if still.is_empty() {
                // Every entry of the reply settled, so nothing pins the ack any more. The reply
                // was read from the acked position, so its cursor covers only settled entries.
                // Without this ack the feed would stay unacked for as long as the puller lives.
                puller.set_pinned(Vec::new());
                if !reply.entries.is_empty() {
                    self.ack(module, &reply.cursor, &reply.entries).await?;
                }
                if !reply.more {
                    return Ok(());
                }
            } else {
                let mut pins = still;
                for entry in &reply.entries[settled..] {
                    let key = entry.intake_key(module);
                    if !pins.contains(&key) {
                        pins.push(key);
                    }
                }
                puller.set_pinned(pins);
                if !reply.more {
                    return Ok(());
                }
            }
        }
        loop {
            // A feed with no acked position yet is read from its start: `None` names no cursor,
            // and every cursor broca sends is one the provider returned.
            let from = read_from.clone();
            let reply = self.pull(module, from.as_ref(), None).await?;
            let next = Some(FeedPosition {
                provider_incarnation: reply.cursor.provider_incarnation.clone(),
                seq: reply.cursor.seq,
            });
            // An entry delivered on a page is durable before anything can ack that page: a
            // delivery that could not be made pins the ack here instead.
            let (settled, still) = self.settle(module, &reply.entries).await?;
            if !still.is_empty() {
                puller.set_pinned(still);
            }
            let ackable = puller.pinned().is_empty();
            if ackable && reply.entries.is_empty() {
                // Nothing after the acked position: an ack would cover no entry, and sending one
                // on every idle poll only adds a round trip to the provider. Nothing unacked
                // remains before the feed's head, so an owed ack is settled without one.
                self.store
                    .set_ack_owed(module, false)
                    .map_err(FeedError::Retryable)?;
            } else if ackable {
                self.ack(module, &reply.cursor, &reply.entries).await?;
            } else if settled > 0 {
                // The settled entries of this page form one run at its head, so a limit re-pull
                // of that run returns exactly the cursor to ack. An entry pinned before it is
                // not one of these, so the ack cannot pass it.
                let prefix: Vec<LateEntry> = reply.entries[..settled].to_vec();
                let _ = self.ack_prefix(module, from.as_ref(), &prefix).await?;
            }
            if !reply.more {
                return Ok(());
            }
            read_from = next;
        }
    }

    async fn pull(
        &self,
        module: &str,
        since: Option<&FeedPosition>,
        limit: Option<u32>,
    ) -> Result<FeedReply, FeedError> {
        let cursor = since.map(FeedPosition::cursor);
        self.feed.pull(module, cursor.as_ref(), limit).await
    }

    /// Nothing is acked before its durable write: every entry is delivered or classed first. The
    /// returned count is the settled prefix length; unsettled entries are returned as pins.
    async fn settle(
        &self,
        module: &str,
        entries: &[LateEntry],
    ) -> Result<(usize, Vec<IntakeKey>), FeedError> {
        let rows = self
            .store
            .registration_rows()
            .map_err(FeedError::Retryable)?;
        let mut views: HashMap<BindIdentity, LineageView> = HashMap::new();
        let mut settled = 0usize;
        let mut pinned = Vec::new();
        for entry in entries {
            let key = entry.intake_key(module);
            if !pinned.is_empty() {
                // An ack only advances over a settled run, so delivery stops where the run does.
                // These entries are classed afresh from the pinned position next tick.
                pinned.push(key);
                continue;
            }
            let kind = self
                .classify_one(module, entry, &rows, &mut views)
                .await
                .map_err(FeedError::Retryable)?;
            if !kind.settles() {
                pinned.push(key);
                continue;
            }
            if matches!(kind, Classified::Delivered) {
                // A delivery is this module's only settlement that needs the run, so it is
                // handed over before anything can ack the entry's position. A delivery that
                // could not be made leaves the entry exactly where it was.
                if let Err(reason) = self
                    .deliver_settled(module, std::slice::from_ref(entry), &rows)
                    .await
                {
                    // The entry is retried from its pin on a later pull. The reason names the
                    // failure only, never the entry's result, so it is safe at INFO.
                    tracing::info!(
                        target: "late_results",
                        module,
                        call_key = entry.call_key,
                        event_id = entry.event_id,
                        "late-results delivery not made; the entry stays pinned: {reason}"
                    );
                    pinned.push(key);
                    continue;
                }
            }
            settled += 1;
        }
        if settled > 0 {
            self.store
                .set_ack_owed(module, true)
                .map_err(FeedError::Retryable)?;
        }
        Ok((settled, pinned))
    }

    async fn classify_one(
        &self,
        module: &str,
        entry: &LateEntry,
        rows: &[RegistrationRow],
        views: &mut HashMap<BindIdentity, LineageView>,
    ) -> Result<Classified, String> {
        let key = entry.intake_key(module);
        if let Some(disposition) = self.store.receipt(&key)? {
            return Ok(match disposition {
                Disposition::Delivered => Classified::Delivered,
                Disposition::NoOp => Classified::NoOp,
            });
        }
        let kind = self.classify(module, entry, rows, views)?;
        match kind {
            Classified::NoOp => {
                self.store
                    .record_receipt(&key, &entry.kind, Disposition::NoOp)?;
            }
            Classified::Blocked(ref reason) => {
                tracing::info!(
                    target: "late_results",
                    module,
                    call_key = entry.call_key,
                    event_id = entry.event_id,
                    "late-results entry deferred: {reason}"
                );
            }
            Classified::Pending => {
                tracing::info!(
                    target: "late_results",
                    module,
                    call_key = entry.call_key,
                    event_id = entry.event_id,
                    "late-results entry waits for its held record"
                );
            }
            Classified::Delivered => {}
        }
        Ok(kind)
    }

    fn classify(
        &self,
        module: &str,
        entry: &LateEntry,
        rows: &[RegistrationRow],
        views: &mut HashMap<BindIdentity, LineageView>,
    ) -> Result<Classified, String> {
        if self.is_rebuilding() {
            // A registration missing from a store that is still being rebuilt proves nothing.
            return Ok(Classified::Blocked("intake_rebuilding".into()));
        }
        let custodian = self.custodian();
        if entry.custodian != custodian && entry.custodian != self.consumer_module {
            tracing::warn!(
                target: "late_results",
                module,
                call_key = entry.call_key,
                "late-results entry names another custodian"
            );
            return Ok(Classified::NoOp);
        }
        let Some(row) = rows
            .iter()
            .find(|row| row.call.module == module && row.call.call_key == entry.call_key)
        else {
            // No registration names this lineage. Every call that can still settle was
            // registered before its key was sent, so this entry can never be delivered.
            return Ok(Classified::NoOp);
        };
        if !scope_matches(&row.call, entry) {
            tracing::warn!(
                target: "late_results",
                module,
                call_key = entry.call_key,
                "late-results entry does not match its registration's scope"
            );
            return Ok(Classified::NoOp);
        }
        if !settles_a_call(entry) {
            // An unknown kind (or `progress`, which only user commands use) is recorded and
            // acked, never retried, and leaves the call open: a later entry with another
            // `event_id` is classified afresh.
            return Ok(Classified::NoOp);
        }
        if !views.contains_key(&row.call.bind) {
            let view = self.lineage_view(&row.call);
            views.insert(row.call.bind.clone(), view);
        }
        let view = views.get(&row.call.bind).expect("view was just inserted");
        if view.tombstoned {
            // Retention deleted the session; its late entries are no-ops, never Blocked.
            return Ok(Classified::NoOp);
        }
        if view.facts.unreadable {
            return Ok(Classified::Blocked("wal_unreadable".into()));
        }
        let Some(held) = view.facts.held_call(&entry.call_key) else {
            return Ok(if row.state == RegistrationState::Registered {
                // Registered before the send, but no held record is durable yet: the ack stops
                // here until the record syncs.
                Classified::Pending
            } else {
                Classified::NoOp
            });
        };
        if !held.module.is_empty() && held.module != module {
            tracing::warn!(
                target: "late_results",
                module,
                dispatched_to = held.module,
                "late-results entry names a call this module did not dispatch"
            );
            return Ok(Classified::NoOp);
        }
        if view.facts.terminal(&held.tool_call_id).is_some() {
            // The call already has its terminal: this entry was delivered before (its ack was
            // lost, or the provider restarted and re-served it), or the call was closed out. Either
            // way it is acked as a no-op and never written a second time.
            return Ok(Classified::NoOp);
        }
        if view.closing {
            return Ok(Classified::NoOp);
        }
        let raw: Vec<WalRecord> = view
            .records
            .iter()
            .map(|frame| frame.record.clone())
            .collect();
        match broca_core::replay(&raw) {
            Ok(state) => {
                if matches!(
                    state.resolution,
                    broca_core::ReplayResolution::ProviderHeld { .. }
                ) || view.facts.barrier_owed
                {
                    Ok(Classified::Delivered)
                } else {
                    Ok(Classified::Blocked("episode_unsettled".into()))
                }
            }
            Err(_) => Ok(Classified::Blocked("wal_unreadable".into())),
        }
    }

    /// Deliver the settled entries that need a delivery, then record their receipts. A delivered
    /// receipt is written only after its terminal is durable.
    async fn deliver_settled(
        &self,
        module: &str,
        entries: &[LateEntry],
        rows: &[RegistrationRow],
    ) -> Result<(), String> {
        for entry in entries {
            let key = entry.intake_key(module);
            if self.store.receipt(&key)?.is_some() {
                continue;
            }
            let Some(row) = rows
                .iter()
                .find(|row| row.call.module == module && row.call.call_key == entry.call_key)
            else {
                continue;
            };
            let held_call_id = match self.delivery_id(module, row, entry) {
                Some(id) => id,
                None => {
                    // Nothing to deliver as a tool result: settle the entry as a no-op.
                    self.store
                        .record_receipt(&key, &entry.kind, Disposition::NoOp)?;
                    continue;
                }
            };
            self.delivery
                .deliver(&row.call, &held_call_id, entry)
                .await?;
            // The batch synced the call's terminal before `deliver` returned, so the WAL now
            // answers any re-read of this entry as already settled; the receipt only spares the
            // re-read until the ack lands.
            self.store
                .record_receipt(&key, &entry.kind, Disposition::Delivered)?;
        }
        Ok(())
    }

    fn delivery_id(
        &self,
        _module: &str,
        registration: &RegistrationRow,
        entry: &LateEntry,
    ) -> Option<ToolCallId> {
        let view = self.lineage_view(&registration.call);
        view.facts
            .held_call(&entry.call_key)
            .map(|held| held.tool_call_id.clone())
    }

    /// Send one ack for a fully settled reply and store its cursor.
    async fn ack(
        &self,
        module: &str,
        cursor: &Cursor,
        entries: &[LateEntry],
    ) -> Result<(), FeedError> {
        // Every entry of the page is already durable before the ack: the puller delivers each
        // one as it settles, so the ack never stands in for a missing terminal.
        self.feed.ack(module, cursor).await?;
        self.store
            .record_ack(
                module,
                &FeedPosition {
                    provider_incarnation: cursor.provider_incarnation.clone(),
                    seq: cursor.seq,
                },
            )
            .map_err(FeedError::Retryable)?;
        let keys: Vec<IntakeKey> = entries
            .iter()
            .map(|entry| entry.intake_key(module))
            .collect();
        self.store
            .prune_receipts(&keys)
            .map_err(FeedError::Retryable)?;
        Ok(())
    }

    /// Ack the settled prefix of a page that continues past it. The prefix alone is re-pulled
    /// with a limit, so the cursor broca acks is one the provider returned for exactly those
    /// entries; on any mismatch nothing is acked this tick.
    async fn ack_prefix(
        &self,
        module: &str,
        since: Option<&FeedPosition>,
        prefix: &[LateEntry],
    ) -> Result<bool, FeedError> {
        let since = since.map(FeedPosition::cursor);
        let reply = self
            .feed
            .pull(module, since.as_ref(), Some(prefix.len() as u32))
            .await?;
        let keys: Vec<IntakeKey> = reply.entries.iter().map(|e| e.intake_key(module)).collect();
        let expected: Vec<IntakeKey> = prefix.iter().map(|e| e.intake_key(module)).collect();
        if keys != expected {
            tracing::warn!(
                target: "late_results",
                module,
                "late-results limit re-pull did not return the settled prefix; acking nothing"
            );
            return Ok(false);
        }
        let rows = self
            .store
            .registration_rows()
            .map_err(FeedError::Retryable)?;
        self.deliver_settled(module, &reply.entries, &rows)
            .await
            .map_err(FeedError::Retryable)?;
        self.feed.ack(module, &reply.cursor).await?;
        self.store
            .record_ack(
                module,
                &FeedPosition {
                    provider_incarnation: reply.cursor.provider_incarnation.clone(),
                    seq: reply.cursor.seq,
                },
            )
            .map_err(FeedError::Retryable)?;
        self.store
            .prune_receipts(&keys)
            .map_err(FeedError::Retryable)?;
        Ok(true)
    }

    /// One last pull and ack after the module stops needing its feed, so anything settled before
    /// the stop is not left owed.
    pub async fn final_pull(&self, module: &str) -> Result<(), FeedError> {
        let Some(puller) = self.puller(module) else {
            return Ok(());
        };
        let Some(feed_row) = self.store.feed(module).map_err(FeedError::Retryable)? else {
            return Ok(());
        };
        if !feed_row.ack_owed {
            return Ok(());
        }
        match self.pull_round(module, &puller).await {
            Ok(()) => Ok(()),
            Err(FeedError::CursorIncarnationChanged) => {
                self.store
                    .forget_cursor(module)
                    .map_err(FeedError::Retryable)?;
                self.pull_round(module, &puller).await
            }
            Err(error) => Err(error),
        }
    }
}

/// One registered lineage's WAL, as the intake reads it.
pub struct LineageView {
    pub facts: HeldCallFacts,
    pub records: Vec<FramedRecord>,
    /// An episode is closing when its run was canceled, or when a held call already has a
    /// close-out terminal. Recognition never reads the result's reason.
    pub closing: bool,
    pub tombstoned: bool,
}

fn closing_episode(records: &[FramedRecord]) -> bool {
    let held: Vec<ToolCallId> = records
        .iter()
        .filter_map(|frame| match &frame.record {
            WalRecord::ToolCallProviderHeld { tool_call_id, .. } => Some(tool_call_id.clone()),
            _ => None,
        })
        .collect();
    records.iter().any(|frame| match &frame.record {
        WalRecord::RunCancelRequested { .. } => true,
        WalRecord::ToolResult {
            result, outcome, ..
        } => {
            held.contains(&result.tool_call_id)
                && matches!(
                    outcome,
                    DispatchOutcome::Interrupted | DispatchOutcome::MaxSteps
                )
        }
        _ => false,
    })
}

fn scope_matches(registration: &LateResultRegistration, entry: &LateEntry) -> bool {
    match &registration.scope {
        Some(scope) => {
            scope.owner.as_owner_string() == entry.owner
                && scope.scope_ref == entry.scope_ref
                && scope.scope_epoch == entry.scope_epoch
        }
        None => true,
    }
}

/// Whether an entry of this kind settles its call: `result`, `not_started` and `expired` do.
/// `progress` (user commands only) and kinds this build does not know settle nothing.
fn settles_a_call(entry: &LateEntry) -> bool {
    matches!(
        entry.kind(),
        EntryKind::Result | EntryKind::NotStarted | EntryKind::Expired
    )
}

/// Whether a registered call is settled for good, so its registration may be pruned: the WAL
/// holds a terminal for it and no batch in the WAL still owes its commit barrier.
///
/// The call is found by its key: a held record naming it, an approved call's intent carrying
/// it, or a plain call's intent at the WAL sequence the key ends in (`<lineage>:<intent seq>`).
/// A key that names nothing in the WAL is never pruned here: the registration is written right
/// after the intent, and a read racing that write must not conclude the call is gone.
fn registration_settled(call_key: &str, records: &[FramedRecord], facts: &HeldCallFacts) -> bool {
    if facts.barrier_owed {
        return false;
    }
    let held = facts
        .held_call(call_key)
        .map(|held| held.tool_call_id.clone());
    let intent_seq = call_key
        .rsplit_once(':')
        .and_then(|(_, seq)| seq.parse::<u64>().ok());
    let call = held.or_else(|| {
        records.iter().find_map(|frame| match &frame.record {
            WalRecord::ToolDispatchIntent {
                tool_call_id,
                call_key: stored,
                ..
            } if stored.as_deref() == Some(call_key) || Some(frame.seq) == intent_seq => {
                Some(tool_call_id.clone())
            }
            _ => None,
        })
    });
    let Some(call) = call else {
        return false;
    };
    records.iter().any(|frame| match &frame.record {
        WalRecord::ToolResult { result, .. } => result.tool_call_id == call,
        WalRecord::ToolNoDispatch { tool_call_id, .. } => *tool_call_id == call,
        _ => false,
    })
}

/// How far the rebuild scanned, for the operator's log and for the measurement test.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RebuildReport {
    /// WAL files looked at.
    pub files: usize,
    /// WAL files decoded, including logs with no held reply yet.
    pub decoded: usize,
    /// Registrations written.
    pub registered: usize,
    /// WALs that could not be verified or registered; any one keeps the fence tripped.
    pub unreadable: Vec<std::path::PathBuf>,
    /// The state root's WAL feature floor shows no provider-held frame was ever written here,
    /// so no WAL was looked at.
    pub skipped_by_floor: bool,
}

/// Rebuild the intake from verified live WALs and every published archive member, never
/// from the run index. Registration precedes dispatch, so intents without a held reply must
/// also be reconstructed. Their original scope and lineage come from the log, not its path.
/// Each reconstructed feed owes an ack whose previous position is unknown.
pub fn rebuild_store_from_wals(
    store: &LateResultsStore,
    state_root: &Path,
) -> Result<RebuildReport, String> {
    rebuild_store_from_wals_with_progress(store, state_root, |_| {})
}

pub fn rebuild_store_from_wals_with_progress(
    store: &LateResultsStore,
    state_root: &Path,
    progress: impl Fn(&RebuildReport),
) -> Result<RebuildReport, String> {
    rebuild_with_publication_hook(store, state_root, progress, || {})
}

fn rebuild_with_publication_hook(
    store: &LateResultsStore,
    state_root: &Path,
    progress: impl Fn(&RebuildReport),
    after_verification: impl FnOnce(),
) -> Result<RebuildReport, String> {
    use std::io::{Read, Seek};
    let mut report = RebuildReport::default();
    // Registration can precede the held feature floor. Only absence of both kinds of
    // history permits skipping the log scan.
    if !state_root
        .join(crate::late_results_store::REGISTRATION_MARKER)
        .try_exists()
        .map_err(|error| error.to_string())?
        && !state_root
            .join(crate::late_results_store::REBUILD_REQUIRED_MARKER)
            .try_exists()
            .map_err(|error| error.to_string())?
        && broca_wal::read_floor(&state_root.join(broca_wal::FEATURE_FLOOR_FILENAME))
            .is_ok_and(|floor| !floor.contains(broca_wal::PROVIDER_HELD_FEATURE))
    {
        report.skipped_by_floor = true;
        progress(&report);
        return Ok(report);
    }
    let paths = rebuild_inventory(state_root)?;
    let mut identities = Vec::new();
    for (path, archive) in &paths {
        let scanned = (|| -> Result<(), String> {
            let mut file = std::fs::File::open(path).map_err(|error| error.to_string())?;
            let identity = input_identity(&mut file, path, *archive)?;
            identities.push((path, *archive, identity.clone()));
            if let Some(container) = &identity.archive {
                use broca_wal::archive::{ArchiveReader, ContainerState};
                if container.state == ContainerState::Unsealed {
                    // Missing seal bytes can describe either an unfinished new archive or a
                    // damaged published archive. Require a sealed archive with all members
                    // verified; neither condition permits skipping this input.
                    return Err("archive is unsealed or its published footer is truncated".into());
                }
                let mut reader = ArchiveReader::new(
                    file.try_clone().map_err(|error| error.to_string())?,
                    broca_wal::file_wal::DEFAULT_MAX_READ_BYTES,
                )
                .with_stamp(
                    broca_wal::archive::fold_timestamp(path).map_err(|error| error.to_string())?,
                );
                for expected in &container.members {
                    report.files += 1;
                    progress(&report);
                    let member = reader
                        .read_member(&expected.address)
                        .map_err(|error| error.to_string())?
                        .ok_or("enumerated archive member disappeared")?;
                    if ArchiveMemberIdentity::from(&member.info) != *expected {
                        return Err("archive member identity changed during verification".into());
                    }
                    let range = broca_wal::file_wal::read_verified_member_range(
                        &member.payload,
                        1,
                        0,
                        None,
                    )
                    .map_err(|error| error.to_string())?;
                    if !verified_eof(&range, member.payload.len() as u64, || {
                        Ok(member.payload.clone())
                    }) {
                        return Err("archive member has an unverified suffix".into());
                    }
                    rebuild_range(store, &range, Some(&expected.address), &mut report)?;
                    progress(&report);
                }
            } else {
                report.files += 1;
                progress(&report);
                file.rewind().map_err(|error| error.to_string())?;
                let mut bytes = Vec::new();
                (&mut file)
                    .take(broca_wal::file_wal::DEFAULT_MAX_READ_BYTES + 1)
                    .read_to_end(&mut bytes)
                    .map_err(|error| error.to_string())?;
                if bytes.len() as u64 > broca_wal::file_wal::DEFAULT_MAX_READ_BYTES {
                    return Err("live WAL exceeds rebuild byte limit".into());
                }
                // Decode exactly the opened file's bytes, not another lookup by pathname.
                // The constant member_identity only tags byte-anchor cache entries, which this
                // scan never stores or reuses. The WAL lineage comes from the decoded log.
                let range = broca_wal::file_wal::read_verified_member_range(&bytes, 1, 0, None)
                    .map_err(|error| error.to_string())?;
                if !verified_eof(&range, bytes.len() as u64, || Ok(bytes.clone())) {
                    return Err("WAL has an unverified suffix".into());
                }
                rebuild_range(store, &range, None, &mut report)?;
            }
            // Hash/reader/hash operations are serial: try_clone may share a file cursor.
            // Every member payload has been verified before its header digest is trusted.
            if input_identity(&mut file, path, *archive)? != identity {
                return Err("input content changed while it was being verified".into());
            }
            Ok(())
        })();
        if let Err(error) = scanned {
            tracing::warn!(target: "late_results", wal = %path.display(), "late-results rebuild incomplete: {error}");
            report.unreadable.push(path.clone());
        }
        progress(&report);
    }
    if rebuild_inventory(state_root)? != paths {
        return Err("WAL inventory changed during late-results rebuild".into());
    }
    for (path, archive, expected) in identities {
        let mut file =
            std::fs::File::open(path).map_err(|error| format!("WAL inventory changed: {error}"))?;
        if input_identity(&mut file, path, archive)? != expected {
            return Err(format!(
                "WAL inventory changed during late-results rebuild: {}",
                path.display()
            ));
        }
    }
    // Supported writers append under the session lease; published archives are immutable.
    // Folding seals/verifies its copy before retiring an unchanged source. Thus historical
    // content cannot appear after this comparison without either a retained verified copy
    // or a new registration committed to this same database before dispatch. This is not a
    // snapshot against an uncooperative writer changing/restoring bytes between checks.
    after_verification();
    progress(&report);
    Ok(report)
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ArchiveMemberIdentity {
    address: String,
    offset: u64,
    length: u64,
    digest: [u8; 32],
}

impl From<&broca_wal::archive::MemberInfo> for ArchiveMemberIdentity {
    fn from(member: &broca_wal::archive::MemberInfo) -> Self {
        Self {
            address: member.session_addr.clone(),
            offset: member.offset,
            length: member.payload_len,
            digest: member.payload_sha256,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ArchiveScanIdentity {
    state: broca_wal::archive::ContainerState,
    members: Vec<ArchiveMemberIdentity>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct InputIdentity {
    length: u64,
    digest: [u8; 32],
    archive: Option<ArchiveScanIdentity>,
}

fn input_identity(
    file: &mut std::fs::File,
    path: &Path,
    archive: bool,
) -> Result<InputIdentity, String> {
    use sha2::{Digest, Sha256};
    use std::io::{Read, Seek};
    let expected_length = file.metadata().map_err(|error| error.to_string())?.len();
    if !archive && expected_length > broca_wal::file_wal::DEFAULT_MAX_READ_BYTES {
        return Err("live WAL exceeds rebuild byte limit".into());
    }
    file.rewind().map_err(|error| error.to_string())?;
    let mut hasher = Sha256::new();
    let mut length = 0;
    let mut buffer = [0; 64 * 1024];
    while length < expected_length {
        let limit = (expected_length - length).min(buffer.len() as u64) as usize;
        let bytes = file
            .read(&mut buffer[..limit])
            .map_err(|error| error.to_string())?;
        if bytes == 0 {
            return Err("input was truncated while hashing its identity".into());
        }
        hasher.update(&buffer[..bytes]);
        length += bytes as u64;
    }
    if file.metadata().map_err(|error| error.to_string())?.len() != expected_length {
        return Err("input length changed while hashing its identity".into());
    }
    let archive = if archive {
        use broca_wal::archive::{ArchiveReader, ContainerState};
        let mut reader = ArchiveReader::new(
            file.try_clone().map_err(|error| error.to_string())?,
            broca_wal::file_wal::DEFAULT_MAX_READ_BYTES,
        )
        .with_stamp(broca_wal::archive::fold_timestamp(path).map_err(|error| error.to_string())?);
        let state = reader.state().map_err(|error| error.to_string())?;
        let members = if state == ContainerState::Sealed {
            reader
                .rescan()
                .map_err(|error| error.to_string())?
                .iter()
                .map(ArchiveMemberIdentity::from)
                .collect()
        } else {
            Vec::new()
        };
        Some(ArchiveScanIdentity { state, members })
    } else {
        None
    };
    Ok(InputIdentity {
        length,
        digest: hasher.finalize().into(),
        archive,
    })
}

fn rebuild_inventory(root: &Path) -> Result<Vec<(std::path::PathBuf, bool)>, String> {
    let mut files = Vec::new();
    for (tree, archive) in [
        ("wal", false),
        ("wal-ephemeral", false),
        ("wal-durable", false),
        ("wal-archive", true),
        ("wal-archive-durable", true),
    ] {
        let directory = root.join(tree);
        let entries = match std::fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(format!("list {}: {error}", directory.display())),
        };
        for entry in entries {
            let path = entry.map_err(|error| error.to_string())?.path();
            if path.extension().and_then(|ext| ext.to_str())
                == Some(if archive { "ark" } else { "wal" })
            {
                files.push((path, archive));
            }
        }
    }
    files.sort();
    Ok(files)
}

fn verified_eof(
    range: &broca_wal::WalRangeRead,
    length: u64,
    empty_bytes: impl FnOnce() -> std::io::Result<Vec<u8>>,
) -> bool {
    if range.complete_byte_end == Some(length) {
        return true;
    }
    let WalRangeRecords::Complete(records) = range.records() else {
        return false;
    };
    if !records.is_empty() {
        return false;
    }
    let Ok(bytes) = empty_bytes() else {
        return false;
    };
    if bytes.len() as u64 != length {
        return false;
    }
    let mut remaining = bytes.as_slice();
    while !remaining.is_empty() {
        match broca_wal::framing::decode_next(remaining) {
            broca_wal::framing::DecodeStep::Lineage { consumed, .. } => {
                remaining = &remaining[consumed..]
            }
            _ => return false,
        }
    }
    true
}

fn rebuild_range(
    store: &LateResultsStore,
    range: &broca_wal::WalRangeRead,
    address: Option<&str>,
    report: &mut RebuildReport,
) -> Result<(), String> {
    let WalRangeRecords::Complete(records) = range.records() else {
        return Err("incomplete WAL range".into());
    };
    report.decoded += 1;
    let Some(session) = records.iter().find_map(|frame| match &frame.record {
        WalRecord::RunStarted { session, .. } => Some(session.clone()),
        _ => None,
    }) else {
        return if records.is_empty() {
            Ok(())
        } else {
            Err("WAL has records but no session identity".into())
        };
    };
    if address.is_some_and(|address| address != broca_core::session_addr(&session)) || records.iter().any(|frame| matches!(&frame.record, WalRecord::RunStarted { session: other, .. } if other != &session)) {
        return Err("WAL session identity does not match its archive member or other episodes".into());
    }
    let calls = registration_candidates(records, &session, range.lineage_id)?;
    for call in &calls {
        store.ensure_feed(&call.module, true)?;
    }
    store.register_calls(&calls)?;
    report.registered += calls.len();
    Ok(())
}

/// The registrations a rebuilt store takes from one WAL: one per `ToolCallProviderHeld`, open or
/// closed.
pub fn held_registrations(
    records: &[FramedRecord],
    session: &BindIdentity,
) -> Vec<LateResultRegistration> {
    let mut calls: Vec<LateResultRegistration> = Vec::new();
    let mut scope = None;
    for frame in records {
        if let WalRecord::RunStarted {
            scope: recorded, ..
        } = &frame.record
        {
            scope = recorded.clone();
        }
        if let WalRecord::ToolCallProviderHeld {
            call_key, module, ..
        } = &frame.record
        {
            let Some((lineage, _)) = call_key.rsplit_once(':') else {
                continue;
            };
            if calls.iter().any(|call| &call.call_key == call_key) {
                continue;
            }
            calls.push(LateResultRegistration {
                lineage: lineage.to_owned(),
                call_key: call_key.clone(),
                module: module.clone(),
                bind: session.clone(),
                scope: scope.clone(),
            });
        }
    }
    calls
}

/// Intents are durable before registration, but registration itself is not a WAL event.
/// Rebuild a conservative superset, including calls whose provider never answered held.
fn registration_candidates(
    records: &[FramedRecord],
    session: &BindIdentity,
    lineage: Option<broca_wal::WalLineageId>,
) -> Result<Vec<LateResultRegistration>, String> {
    let mut calls: Vec<LateResultRegistration> = Vec::new();
    let mut scope = None;
    for frame in records {
        let (module, key) = match &frame.record {
            WalRecord::RunStarted {
                scope: recorded, ..
            } => {
                scope = recorded.clone();
                continue;
            }
            WalRecord::ToolCallProviderHeld {
                call_key, module, ..
            } => (module, call_key.clone()),
            WalRecord::ToolDispatchIntent {
                module_id: Some(module),
                call_key,
                ..
            } => {
                let key = call_key
                    .clone()
                    .or_else(|| lineage.map(|lineage| format!("{lineage}:{}", frame.seq)));
                // Before lineage metadata existed a plain dispatch could not be registered.
                let Some(key) = key else {
                    continue;
                };
                (module, key)
            }
            _ => continue,
        };
        cortexkit_role_tool_provider::call::validate_call_key(&key)
            .map_err(|error| error.to_string())?;
        let (lineage, _) = key
            .rsplit_once(':')
            .ok_or("registered call key has no lineage")?;
        let candidate = LateResultRegistration {
            lineage: lineage.into(),
            call_key: key.clone(),
            module: module.clone(),
            bind: session.clone(),
            scope: scope.clone(),
        };
        if let Some(existing) = calls.iter().find(|call| call.call_key == key) {
            if existing != &candidate {
                return Err("WAL registration key has conflicting module or scope".into());
            }
        } else {
            calls.push(candidate);
        }
    }
    Ok(calls)
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_protocol::content::{AssistantMessage, ContentBlock, ToolCall};
    use broca_protocol::{BatchId, MessageId, RunId, StepId};
    use cortexkit_role_tool_provider::late_results::kinds;
    use serde_json::{json, Value};

    /// One scripted feed: each pull answers the next reply and records what was asked.
    #[derive(Default)]
    struct ScriptedFeed {
        replies: Mutex<Vec<FeedReply>>,
        pulls: Mutex<Vec<(Option<Cursor>, Option<u32>)>>,
        acks: Mutex<Vec<Cursor>>,
        pull_error: Mutex<Option<FeedError>>,
        /// The next ack is lost on its way, as a transport failure would lose it.
        fail_ack: Mutex<bool>,
    }

    impl ScriptedFeed {
        fn fail_ack_once(&self) {
            *self.fail_ack.lock().unwrap() = true;
        }
        fn replies(&self, replies: Vec<FeedReply>) {
            *self.replies.lock().unwrap() = replies;
        }
        fn pulls(&self) -> Vec<(Option<Cursor>, Option<u32>)> {
            self.pulls.lock().unwrap().clone()
        }
        fn acks(&self) -> Vec<Cursor> {
            self.acks.lock().unwrap().clone()
        }
        fn fail_pull(&self, error: FeedError) {
            *self.pull_error.lock().unwrap() = Some(error);
        }
    }

    #[async_trait]
    impl LateResultsFeed for ScriptedFeed {
        async fn pull(
            &self,
            _module: &str,
            since: Option<&Cursor>,
            limit: Option<u32>,
        ) -> Result<FeedReply, FeedError> {
            self.pulls.lock().unwrap().push((since.cloned(), limit));
            if let Some(error) = self.pull_error.lock().unwrap().clone() {
                return Err(error);
            }
            let mut replies = self.replies.lock().unwrap();
            if replies.is_empty() {
                return Err(FeedError::Retryable("no scripted reply".into()));
            }
            Ok(replies.remove(0))
        }

        async fn ack(&self, _module: &str, through: &Cursor) -> Result<(), FeedError> {
            if std::mem::take(&mut *self.fail_ack.lock().unwrap()) {
                return Err(FeedError::Retryable("ack lost".into()));
            }
            self.acks.lock().unwrap().push(through.clone());
            Ok(())
        }
    }

    struct RecordedDelivery {
        delivered: Mutex<Vec<(String, String)>>,
        refuse: Mutex<Option<String>>,
    }

    impl Default for RecordedDelivery {
        fn default() -> Self {
            Self {
                delivered: Mutex::new(Vec::new()),
                refuse: Mutex::new(None),
            }
        }
    }

    #[async_trait]
    impl HeldDelivery for RecordedDelivery {
        async fn deliver(
            &self,
            registration: &LateResultRegistration,
            _tool_call_id: &ToolCallId,
            entry: &LateEntry,
        ) -> Result<(), String> {
            if let Some(reason) = self.refuse.lock().unwrap().clone() {
                return Err(reason);
            }
            self.delivered
                .lock()
                .unwrap()
                .push((registration.call_key.clone(), entry.event_id.clone()));
            Ok(())
        }
    }

    fn cursor(seq: u64) -> Cursor {
        Cursor {
            provider_incarnation: "inc-1".into(),
            seq,
        }
    }

    fn entry(kind: &str, call_key: &str, event: &str) -> LateEntry {
        LateEntry::new(
            kind,
            "reserved:broca",
            "ref",
            1,
            "reserved:broca",
            call_key,
            event,
            1,
        )
        .with_result(json!({"content": [{"type": "text", "text": "late"}]}))
    }

    fn reply(entries: Vec<LateEntry>, seq: u64, more: bool) -> FeedReply {
        FeedReply {
            entries,
            cursor: cursor(seq),
            more,
        }
    }

    fn session(name: &str) -> BindIdentity {
        BindIdentity::new("/project", "broca", name)
    }

    /// The records of one held call whose batch is still waiting on the provider: the run paused
    /// on it after the held record synced.
    fn held_records(call_key: &str, module: &str, bind: &BindIdentity) -> Vec<WalRecord> {
        let mut records = vec![
            WalRecord::RunStarted {
                written_at_ms: None,
                tags: None,
                session_class: None,
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
                ts_ms: Some(1),
                run_id: RunId("run".into()),
                session: bind.clone(),
                config: Box::new(min_config()),
                input: Vec::new().into(),
                origin: broca_wal::RunOrigin::Fresh,
                submission_id: None,
            },
            WalRecord::ModelStepFinished {
                fallback: Default::default(),
                written_at_ms: None,
                tags: None,
                hooks: None,
                step_id: StepId(1),
                assistant_message: Arc::new(AssistantMessage {
                    message_id: MessageId("m".into()),
                    content: vec![ContentBlock::ToolCall(ToolCall {
                        tool_call_id: ToolCallId("a".into()),
                        tool_name: "ask".into(),
                        input: json!({}),
                        thought_signature: None,
                    })],
                }),
                usage: broca_protocol::Usage::default(),
                retries_used: 0,
                finish_reason: None,
                service_tier_used: None,
                speed_used: None,
            },
            WalRecord::ToolBatchStarted {
                batch_id: BatchId("b".into()),
                step_id: StepId(1),
                planned_calls: vec![ToolCallId("a".into())],
            },
            WalRecord::ToolDispatchIntent {
                dispatched_at_ms: None,
                batch_id: BatchId("b".into()),
                tool_call_id: ToolCallId("a".into()),
                tool_name: Some("ask".into()),
                args: Arc::new(json!({})),
                module_id: Some(module.into()),
                schema_pin: None,
                hooks: None,
                approval_digest: None,
                call_key: None,
            },
            WalRecord::ToolCallProviderHeld {
                batch_id: BatchId("b".into()),
                tool_call_id: ToolCallId("a".into()),
                call_key: call_key.into(),
                module: module.into(),
                reason: "running".into(),
                detail_digest: None,
                held_at_ms: 42,
                sequential: false,
            },
            WalRecord::RunPaused {
                run_id: RunId("run".into()),
                reason: broca_wal::PauseReason::ToolHeld,
                error: None,
                ts_ms: Some(43),
            },
        ];
        // The held call's key is what the provider's entry names.
        if let WalRecord::ToolDispatchIntent { call_key: key, .. } = &mut records[3] {
            *key = Some(call_key.into());
        }
        records
    }

    fn min_config() -> broca_wal::RunConfig {
        broca_wal::RunConfig {
            turn_directives_v1: None,
            render_tags: false,
            computed_tags: None,
            model: broca_protocol::request::ModelHandle::new("fake", "fake"),
            tools: vec![].into(),
            tool_choice: Default::default(),
            generation: Default::default(),
            response_format: Default::default(),
            provider_options: serde_json::Value::Null,
            cache_tiers: serde_json::Value::Null,
            context_epoch: 0,
            renderer_version: 1,
            frozen_render_config: serde_json::Value::Null,
            auth_selection: None,
            tools_were_omitted: false,
            budget: None,
            context_limit: None,
            work_class: None,
            variant: None,
            service_tier: None,
            stop_when: Vec::new(),
            context_management: None,
        }
    }

    fn write_wal(wal: &FileWalStore, bind: &BindIdentity, records: &[WalRecord]) {
        write_wal_root(wal.root(), bind, records);
    }

    /// Write one session's records where `FileWalStore::new(wal_tree)` would look for them.
    fn write_wal_root(wal_tree: &std::path::Path, bind: &BindIdentity, records: &[WalRecord]) {
        let wal = FileWalStore::new(wal_tree);
        let path = wal.wal_path(bind);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let bytes: Vec<u8> = records
            .iter()
            .enumerate()
            .flat_map(|(index, record)| {
                broca_wal::framing::encode_payload(
                    index as u64 + 1,
                    1,
                    &serde_json::to_vec(record).unwrap(),
                )
                .unwrap()
            })
            .collect();
        std::fs::write(path, bytes).unwrap();
    }

    /// A session that registered for a feed and then finished its run before any tool call was
    /// held. Its WAL names no held call, so it keeps no puller running.
    ///
    /// The WAL is written directly: a real writer would open its own lease and writer task for a
    /// session no test drives further.
    fn quiet_bind(name: &str, wal_root: &std::path::Path) -> BindIdentity {
        let bind = BindIdentity::new("/project", "broca", name);
        let mut records = held_records("lineage:1", "tools", &bind);
        // Nothing is held: the run stopped before any tool call was dispatched.
        records.truncate(1);
        write_wal_root(wal_root, &bind, &records);
        bind
    }

    /// Register one more call with an intake a rig already holds.
    async fn register_more(store: &Arc<LateResultsStore>, calls: &[LateResultRegistration]) {
        use broca_core::tool_plane::LateResultsIntake as _;
        store.register_group(calls).await.unwrap();
    }

    fn rig_root(name: &str) -> std::path::PathBuf {
        broca_test_support::scratch_root()
            .join(format!("late-results-{name}-{}", std::process::id()))
    }

    struct Rig {
        #[allow(dead_code)]
        root: std::path::PathBuf,
        store: Arc<LateResultsStore>,
        intake: Arc<LateResultsIntake>,
        feed: Arc<ScriptedFeed>,
        delivery: Arc<RecordedDelivery>,
    }

    impl Rig {
        /// A rig whose WAL tree holds `held` (a call waiting on the provider) and whose store
        /// holds `registrations`.
        async fn new(
            name: &str,
            held: Option<(&str, &str, BindIdentity)>,
            registrations: Vec<LateResultRegistration>,
        ) -> Self {
            let root = rig_root(name);
            let _ = std::fs::remove_dir_all(&root);
            std::fs::create_dir_all(&root).unwrap();
            let wal = Arc::new(FileWalStore::new(root.join("wal")));
            if let Some((call_key, module, bind)) = held {
                write_wal(&wal, &bind, &held_records(call_key, module, &bind));
            }
            let store = Arc::new(LateResultsStore::open(&root.join("late-results.db")).unwrap());
            if !registrations.is_empty() {
                use broca_core::tool_plane::LateResultsIntake as _;
                store.register_group(&registrations).await.unwrap();
            }
            let feed = Arc::new(ScriptedFeed::default());
            let delivery = Arc::new(RecordedDelivery::default());
            let intake = LateResultsIntake::new(
                Arc::clone(&store),
                Arc::clone(&wal),
                "broca",
                Arc::clone(&delivery) as Arc<dyn HeldDelivery>,
                Arc::clone(&feed) as Arc<dyn LateResultsFeed>,
            );
            Self {
                root,
                store,
                intake,
                feed,
                delivery,
            }
        }

        fn puller(&self, module: &str) -> Arc<Puller> {
            self.intake
                .pullers
                .lock()
                .unwrap()
                .entry(module.to_owned())
                .or_insert_with(|| Arc::new(Puller::new()))
                .clone()
        }
    }

    fn registration(
        call_key: &str,
        module: &str,
        session: &BindIdentity,
    ) -> LateResultRegistration {
        LateResultRegistration {
            lineage: call_key.rsplit_once(':').unwrap().0.to_owned(),
            call_key: call_key.to_owned(),
            module: module.to_owned(),
            bind: session.clone(),
            scope: None,
        }
    }

    #[tokio::test]
    async fn an_entry_for_a_held_call_is_delivered_then_acked_through_the_providers_cursor() {
        let call_key = "lineage:4";
        let bind = session("held");
        let rig = Rig::new(
            "deliver",
            Some((call_key, "tools", bind.clone())),
            vec![registration(call_key, "tools", &bind)],
        )
        .await;
        let _puller = rig.puller("tools");
        rig.feed.replies(vec![reply(
            vec![entry(kinds::RESULT, call_key, "ev-1")],
            1,
            false,
        )]);
        rig.intake.tick("tools").await.unwrap();
        assert_eq!(
            rig.delivery.delivered.lock().unwrap().clone(),
            vec![(call_key.to_owned(), "ev-1".to_owned())]
        );
        assert_eq!(rig.feed.acks(), vec![cursor(1)]);
        assert_eq!(
            rig.store
                .feed("tools")
                .unwrap()
                .unwrap()
                .position
                .unwrap()
                .seq,
            1
        );
    }

    #[tokio::test]
    async fn nothing_is_acked_before_its_delivery_is_durable() {
        let call_key = "lineage:4";
        let bind = session("held");
        let rig = Rig::new(
            "no-early-ack",
            Some((call_key, "tools", bind.clone())),
            vec![registration(call_key, "tools", &bind)],
        )
        .await;
        let _puller = rig.puller("tools");
        *rig.delivery.refuse.lock().unwrap() = Some("session cannot be written".into());
        rig.feed.replies(vec![reply(
            vec![entry(kinds::RESULT, call_key, "ev-1")],
            1,
            false,
        )]);
        let outcome = rig.intake.tick("tools").await;
        assert_eq!(
            rig.store.feed("tools").unwrap().unwrap().position,
            None,
            "an undelivered entry leaves the feed where it was; tick said {outcome:?}"
        );
        assert!(
            rig.feed.acks().is_empty(),
            "an undelivered entry is never acked; tick said {outcome:?}"
        );
    }

    #[tokio::test]
    async fn a_pinned_entry_pins_the_ack_but_a_later_page_is_still_delivered() {
        let call_key = "lineage:4";
        let pending_key = "lineage:5";
        let bind = session("held");
        let rig = Rig::new(
            "later-page",
            Some((call_key, "tools", bind.clone())),
            vec![registration(call_key, "tools", &bind)],
        )
        .await;
        let pending = quiet_bind("late-sync", &rig_root("later-page").join("wal"));
        register_more(&rig.store, &[registration(pending_key, "tools", &pending)]).await;
        let _puller = rig.puller("tools");
        rig.feed.replies(vec![
            // Page one holds only the pending entry, so nothing on it can be acked.
            reply(vec![entry(kinds::RESULT, pending_key, "ev-1")], 1, true),
            // Page two is read past the pin and delivered.
            reply(vec![entry(kinds::RESULT, call_key, "ev-2")], 2, false),
        ]);
        rig.intake.tick("tools").await.unwrap();
        assert_eq!(
            rig.delivery.delivered.lock().unwrap().clone(),
            vec![(call_key.to_owned(), "ev-2".to_owned())],
            "a later page is delivered although the ack is pinned"
        );
        assert!(
            rig.feed.acks().is_empty(),
            "no ack passes the pending entry: {:?}",
            rig.feed.acks()
        );
        assert!(rig
            .intake
            .puller("tools")
            .unwrap()
            .pinned()
            .contains(&entry(kinds::RESULT, pending_key, "ev-1").intake_key("tools")));
    }

    #[tokio::test]
    async fn a_mismatched_limit_repull_acks_nothing() {
        let call_key = "lineage:4";
        let bind = session("held");
        let rig = Rig::new(
            "ack-mismatch",
            Some((call_key, "tools", bind.clone())),
            vec![
                registration(call_key, "tools", &bind),
                registration(
                    "lineage:9",
                    "tools",
                    &quiet_bind("unregistered", &rig_root("ack-mismatch").join("wal")),
                ),
            ],
        )
        .await;
        let _puller = rig.puller("tools");
        rig.feed.replies(vec![
            reply(
                vec![
                    entry(kinds::RESULT, call_key, "ev-1"),
                    entry(kinds::RESULT, "lineage:9", "ev-2"),
                ],
                2,
                false,
            ),
            // The re-pull answers the wrong entries.
            reply(vec![entry(kinds::RESULT, "lineage:9", "ev-2")], 2, false),
        ]);
        rig.intake.tick("tools").await.unwrap();
        assert!(
            rig.feed.acks().is_empty(),
            "a limit re-pull that answers other entries acks nothing: {:?}",
            rig.feed.acks()
        );
    }

    #[tokio::test]
    async fn an_unknown_kind_is_acked_and_leaves_the_call_open_for_a_later_entry() {
        let call_key = "lineage:4";
        let bind = session("held");
        let rig = Rig::new(
            "unknown-kind",
            Some((call_key, "tools", bind.clone())),
            vec![registration(call_key, "tools", &bind)],
        )
        .await;
        let _puller = rig.puller("tools");
        rig.feed.replies(vec![reply(
            vec![entry("telemetry_note", call_key, "ev-1")],
            1,
            false,
        )]);
        rig.intake.tick("tools").await.unwrap();
        assert_eq!(rig.feed.acks(), vec![cursor(1)]);
        assert!(rig.delivery.delivered.lock().unwrap().is_empty());
        assert!(
            rig.store.registration("tools", call_key).unwrap().is_some(),
            "an unknown kind does not settle the call"
        );
        rig.feed.replies(vec![reply(
            vec![entry(kinds::RESULT, call_key, "ev-2")],
            2,
            false,
        )]);
        rig.intake.tick("tools").await.unwrap();
        assert_eq!(
            rig.delivery.delivered.lock().unwrap().clone(),
            vec![(call_key.to_owned(), "ev-2".to_owned())]
        );
    }

    #[tokio::test]
    async fn an_entry_with_no_registration_is_acked_as_a_no_op() {
        let call_key = "lineage:4";
        let bind = session("held");
        let rig = Rig::new(
            "unregistered",
            Some((call_key, "tools", bind.clone())),
            vec![registration(call_key, "tools", &bind)],
        )
        .await;
        let _puller = rig.puller("tools");
        rig.feed.replies(vec![reply(
            vec![entry(kinds::RESULT, "lineage:9", "ev-1")],
            1,
            false,
        )]);
        rig.intake.tick("tools").await.unwrap();
        assert_eq!(rig.feed.acks(), vec![cursor(1)]);
        assert!(rig.delivery.delivered.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_entry_whose_scope_differs_from_its_registration_is_a_no_op() {
        let call_key = "lineage:4";
        let bind = session("held");
        let mut registered = registration(call_key, "tools", &bind);
        registered.scope = Some(broca_protocol::SessionScope {
            owner: broca_protocol::BindPrincipal::Reserved {
                module_id: "core".into(),
            },
            scope_ref: "ref".into(),
            scope_epoch: 1,
            flow_id: None,
            sender: None,
            agent_id: None,
            run_id: None,
        });
        let rig = Rig::new(
            "scope-mismatch",
            Some((call_key, "tools", bind)),
            vec![registered],
        )
        .await;
        let _puller = rig.puller("tools");
        rig.feed.replies(vec![reply(
            vec![entry(kinds::RESULT, call_key, "ev-1")],
            1,
            false,
        )]);
        rig.intake.tick("tools").await.unwrap();
        assert!(rig.delivery.delivered.lock().unwrap().is_empty());
        assert_eq!(rig.feed.acks(), vec![cursor(1)]);
    }

    #[tokio::test]
    async fn an_entry_for_a_call_dispatched_to_another_module_is_a_no_op() {
        let call_key = "lineage:4";
        let bind = session("held");
        // The WAL holds the call as held by `other`, while the entry arrives on `tools`' feed.
        let rig = Rig::new(
            "other-module",
            Some((call_key, "other", bind.clone())),
            vec![registration(call_key, "tools", &bind)],
        )
        .await;
        let _puller = rig.puller("tools");
        rig.feed.replies(vec![reply(
            vec![entry(kinds::RESULT, call_key, "ev-1")],
            1,
            false,
        )]);
        rig.intake.tick("tools").await.unwrap();
        assert!(rig.delivery.delivered.lock().unwrap().is_empty());
        assert_eq!(rig.feed.acks(), vec![cursor(1)]);
    }

    #[tokio::test]
    async fn an_incarnation_change_forgets_the_cursor_and_keeps_the_owed_ack() {
        let call_key = "lineage:4";
        let bind = session("held");
        let rig = Rig::new(
            "incarnation",
            Some((call_key, "tools", bind.clone())),
            vec![registration(call_key, "tools", &bind)],
        )
        .await;
        let _puller = rig.puller("tools");
        rig.store
            .record_ack(
                "tools",
                &FeedPosition {
                    provider_incarnation: "inc-1".into(),
                    seq: 9,
                },
            )
            .unwrap();
        rig.store.set_ack_owed("tools", true).unwrap();
        rig.feed.fail_pull(FeedError::CursorIncarnationChanged);
        rig.intake.tick("tools").await.unwrap();
        let row = rig.store.feed("tools").unwrap().unwrap();
        assert_eq!(row.position, None, "the refused cursor is forgotten");
        assert!(
            row.ack_owed,
            "a provider restart does not settle an owed ack"
        );
    }

    #[tokio::test]
    async fn an_ack_only_puller_clears_its_owed_ack_when_the_feed_is_abandoned() {
        let rig = Rig::new("abandoned", None, Vec::new()).await;
        rig.store.set_ack_owed("tools", true).unwrap();
        let _puller = rig.puller("tools");
        rig.feed
            .fail_pull(FeedError::Abandoned("module left the catalog".into()));
        rig.intake.tick("tools").await.unwrap();
        assert!(!rig.store.feed("tools").unwrap().unwrap().ack_owed);
    }

    #[tokio::test]
    async fn a_module_with_an_open_held_call_keeps_its_owed_ack_on_abandonment() {
        let call_key = "lineage:4";
        let bind = session("held");
        let rig = Rig::new(
            "abandoned-held",
            Some((call_key, "tools", bind.clone())),
            vec![registration(call_key, "tools", &bind)],
        )
        .await;
        rig.store.set_ack_owed("tools", true).unwrap();
        let _puller = rig.puller("tools");
        rig.feed
            .fail_pull(FeedError::Abandoned("module left the catalog".into()));
        rig.intake.tick("tools").await.unwrap();
        assert!(rig.store.feed("tools").unwrap().unwrap().ack_owed);
    }

    #[tokio::test]
    async fn an_unheld_registration_alone_never_starts_a_puller() {
        let rig = Rig::new("unheld", None, Vec::new()).await;
        let bind = quiet_bind("quiet", &rig_root("unheld").join("wal"));
        register_more(&rig.store, &[registration("lineage:1", "tools", &bind)]).await;
        assert!(!rig.intake.module_needs_feed("tools").unwrap());
        assert_eq!(
            rig.intake.ensure_puller("tools").unwrap(),
            StartOutcome::Idle
        );
        assert!(rig.intake.running().is_empty());
        assert!(rig.feed.pulls().is_empty());
    }

    #[tokio::test]
    async fn a_module_holds_one_puller_however_many_scopes_register() {
        let first = "lineage:4";
        let bind = session("held");
        let rig = Rig::new(
            "one-puller",
            Some((first, "tools", bind.clone())),
            vec![registration(first, "tools", &bind)],
        )
        .await;
        let other = quiet_bind("other-scope", &rig_root("one-puller").join("wal"));
        register_more(&rig.store, &[registration("lineage:5", "tools", &other)]).await;
        assert_eq!(
            rig.intake.ensure_puller("tools").unwrap(),
            StartOutcome::Started
        );
        assert_eq!(
            rig.intake.ensure_puller("tools").unwrap(),
            StartOutcome::Held
        );
        assert_eq!(rig.intake.running(), vec!["tools".to_owned()]);
    }

    fn framed(records: Vec<WalRecord>) -> Vec<FramedRecord> {
        records
            .into_iter()
            .enumerate()
            .map(|(index, record)| FramedRecord {
                seq: index as u64 + 1,
                fence: 1,
                record,
            })
            .collect()
    }

    /// The held records of `held_records`, followed by the call's delivered terminal and the
    /// batch's commit: the call was answered and its run went on.
    fn delivered_records(call_key: &str, module: &str, bind: &BindIdentity) -> Vec<WalRecord> {
        let mut records = held_records(call_key, module, bind);
        records.push(WalRecord::ToolResult {
            finished_at_ms: None,
            tags: None,
            hooks: None,
            batch_id: BatchId("b".into()),
            result: Arc::new(broca_protocol::ToolResult {
                tool_call_id: ToolCallId("a".into()),
                output: broca_protocol::ToolOutput::Text {
                    text: "late".into(),
                },
                is_error: false,
            }),
            outcome: DispatchOutcome::Completed,
            reason: None,
            withdraw_elicitation: None,
            provider_held: None,
        });
        records.push(WalRecord::ToolBatchFinished {
            batch_id: BatchId("b".into()),
        });
        records
    }

    #[test]
    fn a_rebuild_registers_every_held_call_from_its_wal() {
        let bind = session("held");
        let calls = held_registrations(&framed(held_records("lineage:4", "tools", &bind)), &bind);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].call_key, "lineage:4");
        assert_eq!(calls[0].module, "tools");
        assert_eq!(calls[0].bind, bind);
    }

    struct WithdrawalRoute {
        wal: Arc<FileWalStore>,
        seen: Mutex<Vec<String>>,
        mode: &'static str,
    }

    fn review_archive(root: &Path, bind: &BindIdentity, key: &str) -> std::path::PathBuf {
        let wal = FileWalStore::new(root.join("wal"));
        let mut records = held_records(key, "tools", bind);
        records.truncate(4);
        write_wal_gated(&wal, bind, &records);
        let directory = root.join("wal-archive");
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("fold-1.ark");
        let mut archive = broca_wal::archive::ArchiveWriter::create(&path)
            .unwrap()
            .without_feature_floor();
        archive
            .append(
                &broca_core::session_addr(bind),
                &std::fs::read(wal.wal_path(bind)).unwrap(),
            )
            .unwrap();
        archive.seal().unwrap();
        std::fs::remove_file(wal.wal_path(bind)).unwrap();
        path
    }

    fn review_store(root: &Path) -> LateResultsStore {
        let _ = std::fs::remove_dir_all(root);
        std::fs::create_dir_all(root).unwrap();
        std::fs::write(
            root.join(crate::late_results_store::REGISTRATION_MARKER),
            b"",
        )
        .unwrap();
        LateResultsStore::open(&root.join("late-results.db")).unwrap()
    }

    #[test]
    fn review_regression_truncated_published_archive_keeps_the_fence() {
        let root = rig_root("review-truncated-footer");
        let store = review_store(&root);
        let path = review_archive(&root, &session("archived-only"), "lineage:4");
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(file.metadata().unwrap().len() - 8).unwrap();
        file.sync_all().unwrap();
        assert_eq!(std::fs::read_dir(root.join("wal")).unwrap().count(), 0);
        let report = rebuild_store_from_wals(&store, &root).unwrap();
        if report.unreadable.is_empty() {
            store.finish_rebuild().unwrap();
        }
        assert!(
            root.join(crate::late_results_store::REBUILD_REQUIRED_MARKER)
                .exists(),
            "a missing footer cannot prove an archive was never published"
        );
        assert_eq!(report.unreadable.as_slice(), std::slice::from_ref(&path));
    }

    #[test]
    fn review_regression_same_name_archive_replacement_invalidates_inventory() {
        let root = rig_root("review-archive-replace");
        let store = review_store(&root);
        let bind = session("replaced");
        let path = review_archive(&root, &bind, "lineage:4");
        let replacement = review_archive(&root.join("replacement"), &bind, "lineage:5");
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            std::fs::metadata(&replacement).unwrap().len()
        );
        let replaced = std::cell::Cell::new(false);
        let result = rebuild_store_from_wals_with_progress(&store, &root, |report| {
            if report.registered > 0 && !replaced.replace(true) {
                std::fs::rename(&replacement, &path).unwrap();
            }
        });
        assert!(replaced.get());
        assert!(
            result.is_err(),
            "the verified old inode must not certify a same-length replacement at its pathname"
        );
    }

    #[test]
    fn rebuild_rejects_a_same_length_live_log_replacement() {
        let root = rig_root("live-content-identity");
        let store = review_store(&root);
        let wal = FileWalStore::new(root.join("wal"));
        let mut sources = Vec::new();
        for name in ["a", "b"] {
            let bind = session(name);
            let key = format!("lineage-{name}:4");
            write_wal_gated(&wal, &bind, &held_records(&key, "tools", &bind));
            sources.push((wal.wal_path(&bind), bind, key));
        }
        sources.sort_by(|left, right| left.0.cmp(&right.0));
        let (first, bind, key) = &sources[0];
        let length = std::fs::metadata(first).unwrap().len();
        let replaced = std::cell::Cell::new(false);
        let result = rebuild_store_from_wals_with_progress(&store, &root, |report| {
            if report.decoded == 1 && !replaced.replace(true) {
                write_wal_gated(
                    &wal,
                    bind,
                    &held_records(&key.replace(":4", ":5"), "tools", bind),
                );
                assert_eq!(std::fs::metadata(first).unwrap().len(), length);
            }
        });
        assert!(replaced.get());
        assert!(result.is_err());
        assert!(first.exists(), "rebuild never retires a changed source");
    }

    #[test]
    fn rebuild_final_comparison_gap_keeps_new_registered_calls() {
        let root = rig_root("rebuild-final-gap");
        let store = review_store(&root);
        let wal = FileWalStore::new(root.join("wal"));
        let old = session("old");
        write_wal_gated(&wal, &old, &held_records("old-lineage:4", "tools", &old));
        let fresh = session("fresh-after-verification");
        let fresh_records = held_records("new-lineage:4", "tools", &fresh);
        let report = rebuild_with_publication_hook(
            &store,
            &root,
            |_| {},
            || {
                // Normal dispatch syncs its intent and registers in this same database before
                // a provider can return held. A new call cannot depend solely on this scan.
                write_wal_gated(&wal, &fresh, &fresh_records[..4]);
                store
                    .register_calls(&held_registrations(&framed(fresh_records.clone()), &fresh))
                    .unwrap();
                write_wal_gated(&wal, &fresh, &fresh_records);
            },
        )
        .unwrap();
        assert!(report.unreadable.is_empty());
        assert_eq!(report.registered, 1);
        store.finish_rebuild().unwrap();
        let reopened = LateResultsStore::open(&root.join("late-results.db")).unwrap();
        let keys: Vec<_> = reopened
            .registrations()
            .unwrap()
            .into_iter()
            .map(|call| call.call_key)
            .collect();
        assert!(keys.contains(&"old-lineage:4".into()));
        assert!(keys.contains(&"new-lineage:4".into()));
        assert!(wal.wal_path(&fresh).exists());
    }

    #[test]
    fn rebuild_final_comparison_gap_keeps_a_verified_fold_copy() {
        let root = rig_root("rebuild-final-fold-gap");
        let store = review_store(&root);
        let wal = FileWalStore::new(root.join("wal"));
        let bind = session("intent-folded-after-verification");
        let mut records = held_records("fold-lineage:4", "tools", &bind);
        records.truncate(4);
        write_wal_gated(&wal, &bind, &records);
        let source = wal.wal_path(&bind);
        let bytes = std::fs::read(&source).unwrap();
        let report = rebuild_with_publication_hook(
            &store,
            &root,
            |_| {},
            || {
                // The folder's publication order preserves a verified copy before it retires
                // an unchanged source. Moving the log after comparison cannot erase its rows.
                let directory = root.join("wal-archive");
                std::fs::create_dir_all(&directory).unwrap();
                let path = directory.join("fold-1.ark");
                let mut writer = broca_wal::archive::ArchiveWriter::create(&path)
                    .unwrap()
                    .with_feature_floor(root.join(broca_wal::FEATURE_FLOOR_FILENAME));
                writer
                    .append(&broca_core::session_addr(&bind), &bytes)
                    .unwrap();
                writer.seal().unwrap();
                let mut reader = broca_wal::archive::ArchiveReader::open(
                    &path,
                    broca_wal::file_wal::DEFAULT_MAX_READ_BYTES,
                )
                .unwrap();
                assert_eq!(
                    reader
                        .read_member(&broca_core::session_addr(&bind))
                        .unwrap()
                        .unwrap()
                        .payload,
                    bytes
                );
                assert_eq!(std::fs::read(&source).unwrap(), bytes);
                std::fs::remove_file(&source).unwrap();
            },
        )
        .unwrap();
        assert!(report.unreadable.is_empty());
        store.finish_rebuild().unwrap();
        assert_eq!(store.registrations().unwrap()[0].call_key, "fold-lineage:4");
        assert_eq!(wal.read_records(&bind).unwrap().len(), records.len());
        assert!(!source.exists());
    }

    #[test]
    fn review_regression_archive_publication_changes_inventory_identity() {
        let root = rig_root("review-archive-publication");
        let store = review_store(&root);
        let bind = session("published");
        let sealed = review_archive(&root.join("staged"), &bind, "lineage:4");
        let path = root.join("wal-archive/fold-1.ark");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let unsealed = broca_wal::archive::ArchiveWriter::create(&path).unwrap();
        drop(unsealed);
        // The archive sorts before this live tree, so publication happens after the
        // scanner has observed its unsealed state, not before its first archive read.
        let wal = FileWalStore::new(root.join("wal-durable"));
        write_wal_gated(
            &wal,
            &session("live"),
            &held_records("another:4", "tools", &session("live")),
        );
        let published = std::cell::Cell::new(false);
        let result = rebuild_store_from_wals_with_progress(&store, &root, |_| {
            if !published.replace(true) {
                std::fs::rename(&sealed, &path).unwrap();
            }
        });
        assert!(published.get());
        assert!(
            result.is_err(),
            "unsealed-to-sealed publication cannot pass a stable inventory check"
        );
    }

    #[tokio::test]
    async fn review_regression_completed_sibling_is_not_withdrawn_on_recovery() {
        let root = rig_root("review-completed-sibling");
        let store = Arc::new(review_store(&root));
        let bind = session("siblings");
        let wal = Arc::new(FileWalStore::new(root.join("wal")));
        let mut records = held_records("lineage:4", "tools", &bind);
        if let WalRecord::ModelStepFinished {
            assistant_message, ..
        } = &mut records[1]
        {
            let mut message = (**assistant_message).clone();
            let ContentBlock::ToolCall(mut call) = message.content[0].clone() else {
                unreachable!()
            };
            call.tool_call_id = ToolCallId("b".into());
            message.content.push(ContentBlock::ToolCall(call));
            *assistant_message = Arc::new(message);
        }
        if let WalRecord::ToolBatchStarted { planned_calls, .. } = &mut records[2] {
            planned_calls.push(ToolCallId("b".into()));
        }
        let mut intent = records[3].clone();
        if let WalRecord::ToolDispatchIntent {
            tool_call_id,
            call_key,
            ..
        } = &mut intent
        {
            *tool_call_id = ToolCallId("b".into());
            *call_key = Some("lineage:5".into());
        }
        let mut sibling = records[4].clone();
        if let WalRecord::ToolCallProviderHeld {
            tool_call_id,
            call_key,
            ..
        } = &mut sibling
        {
            *tool_call_id = ToolCallId("b".into());
            *call_key = "lineage:5".into();
        }
        records.insert(4, intent);
        records.insert(6, sibling);
        records.push(delivered_records("lineage:4", "tools", &bind)[6].clone());
        let (closed, outcome, reason) = broca_core::resume::provider_held_close_result(
            &ToolCallId("b".into()),
            broca_protocol::RunFinishReason::Cancelled,
        );
        records.push(WalRecord::ToolResult {
            finished_at_ms: None,
            tags: None,
            hooks: None,
            batch_id: BatchId("b".into()),
            result: Arc::new(closed),
            outcome,
            reason: Some(reason.into()),
            withdraw_elicitation: None,
            provider_held: None,
        });
        records.push(WalRecord::ToolBatchFinished {
            batch_id: BatchId("b".into()),
        });
        records.push(WalRecord::TurnFinished { step_id: StepId(1) });
        records.push(WalRecord::RunFinished {
            reason: broca_protocol::RunFinishReason::Cancelled,
            interrupt_reason: None,
            ts_ms: Some(50),
            usage: Default::default(),
            error: None,
            retries_used: 0,
        });
        write_wal_gated(&wal, &bind, &records);
        store
            .register_calls(&held_registrations(&framed(records), &bind))
            .unwrap();
        let route = Arc::new(WithdrawalRoute {
            wal: wal.clone(),
            seen: Mutex::default(),
            mode: "ok",
        });
        let service = crate::held_withdrawals::HeldWithdrawals::new(store, wal, route.clone());
        service.bind_stopping_signal(tokio_util::sync::CancellationToken::new());
        service.recover().await.unwrap();
        assert_eq!(
            *route.seen.lock().unwrap(),
            ["lineage:5"],
            "A completed; only B's cancellation authorizes withdrawal"
        );
    }

    struct DrainWithdrawalRoute {
        seen: std::sync::atomic::AtomicUsize,
        stopping: tokio_util::sync::CancellationToken,
    }
    #[async_trait]
    impl crate::held_withdrawals::WithdrawTransport for DrainWithdrawalRoute {
        async fn withdraw(
            &self,
            _: &LateResultRegistration,
        ) -> (
            cortexkit_role_tool_provider::withdraw::WithdrawVerdict,
            Value,
        ) {
            self.seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.stopping.cancel();
            let error =
                serde_json::from_value(json!({"code": "target_unavailable", "message": "offline"}))
                    .unwrap();
            (
                cortexkit_role_tool_provider::withdraw::caller_verdict(Err(&error)),
                serde_json::to_value(error).unwrap(),
            )
        }
    }

    async fn review_drain(stopped_before_recovery: bool) {
        let root = rig_root(if stopped_before_recovery {
            "review-stop-before"
        } else {
            "review-stop-retry"
        });
        let store = Arc::new(review_store(&root));
        let bind = session("cancelled-before-drain");
        let wal = Arc::new(FileWalStore::new(root.join("wal")));
        let mut records = held_records("lineage:4", "tools", &bind);
        records.push(WalRecord::RunFinished {
            reason: broca_protocol::RunFinishReason::Cancelled,
            interrupt_reason: None,
            ts_ms: Some(50),
            usage: Default::default(),
            error: None,
            retries_used: 0,
        });
        write_wal_gated(&wal, &bind, &records);
        store
            .register_calls(&held_registrations(&framed(records), &bind))
            .unwrap();
        let stopping = tokio_util::sync::CancellationToken::new();
        let route = Arc::new(DrainWithdrawalRoute {
            seen: Default::default(),
            stopping: stopping.clone(),
        });
        let service = crate::held_withdrawals::HeldWithdrawals::new(store, wal, route.clone());
        service.bind_stopping_signal(stopping.clone());
        if stopped_before_recovery {
            stopping.cancel();
        }
        service.recover().await.unwrap();
        assert_eq!(
            route.seen.load(std::sync::atomic::Ordering::SeqCst),
            usize::from(!stopped_before_recovery),
            "drain must stop new withdrawal attempts, including retries"
        );
    }

    #[tokio::test]
    async fn review_regression_drain_suppresses_recovered_withdrawals() {
        review_drain(true).await;
    }
    #[tokio::test]
    async fn review_regression_drain_suppresses_withdrawal_retries() {
        review_drain(false).await;
    }

    #[test]
    fn rebuild_commit_crash_keeps_the_fence_until_verified_publication() {
        let root = rig_root("rebuild-commit-cut");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            root.join(crate::late_results_store::REGISTRATION_MARKER),
            b"",
        )
        .unwrap();
        let path = root.join("late-results.db");
        let store = LateResultsStore::open(&path).unwrap();
        let bind = session("rebuilt");
        let wal = FileWalStore::new(root.join("wal"));
        write_wal_gated(&wal, &bind, &held_records("lineage:4", "tools", &bind));
        let report = rebuild_store_from_wals(&store, &root).unwrap();
        assert!(report.unreadable.is_empty());
        assert_eq!(report.registered, 1);
        drop(store);
        // The process stops after SQLite committed, before the fence's unlink and dir sync.
        let store = LateResultsStore::open(&path).unwrap();
        assert_eq!(store.registrations().unwrap().len(), 1);
        assert!(
            crate::late_results_store::retention_block_reason(&root, "other")
                .unwrap()
                .is_some()
        );
        let report = rebuild_store_from_wals(&store, &root).unwrap();
        assert!(report.unreadable.is_empty());
        store.finish_rebuild().unwrap();
        assert!(!root
            .join(crate::late_results_store::REBUILD_REQUIRED_MARKER)
            .exists());
        assert_eq!(
            crate::late_results_store::retention_block_reason(&root, "other").unwrap(),
            None
        );
    }

    #[test]
    fn rebuild_checks_intent_only_and_torn_logs_without_a_held_feature_floor() {
        let root = rig_root("rebuild-intent");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            root.join(crate::late_results_store::REGISTRATION_MARKER),
            b"",
        )
        .unwrap();
        let store = LateResultsStore::open(&root.join("late-results.db")).unwrap();
        let bind = session("intent-only");
        let wal = FileWalStore::new(root.join("wal"));
        let mut records = held_records("lineage:4", "tools", &bind);
        records.truncate(4);
        let scope = broca_protocol::SessionScope {
            owner: broca_protocol::BindPrincipal::Reserved {
                module_id: "core".into(),
            },
            scope_ref: "scope".into(),
            scope_epoch: 2,
            flow_id: None,
            sender: None,
            agent_id: None,
            run_id: None,
        };
        if let WalRecord::RunStarted { scope: saved, .. } = &mut records[0] {
            *saved = Some(scope.clone());
        }
        write_wal_gated(&wal, &bind, &records);
        let broken = root.join("wal-ephemeral/broken.wal");
        std::fs::create_dir_all(broken.parent().unwrap()).unwrap();
        std::fs::write(&broken, b"torn").unwrap();
        let report = rebuild_store_from_wals(&store, &root).unwrap();
        assert!(!report.skipped_by_floor);
        assert_eq!(report.files, 2);
        assert_eq!(report.unreadable, [broken]);
        assert_eq!(store.registrations().unwrap()[0].scope, Some(scope));
        assert!(root
            .join(crate::late_results_store::REBUILD_REQUIRED_MARKER)
            .exists());
    }

    #[test]
    fn rebuild_verifies_every_archive_member_including_intent_only_calls() {
        let root = rig_root("rebuild-archives");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            root.join(crate::late_results_store::REGISTRATION_MARKER),
            b"",
        )
        .unwrap();
        let store = LateResultsStore::open(&root.join("late-results.db")).unwrap();
        let wal = FileWalStore::new(root.join("wal"));
        let mut expected = Vec::new();
        for (index, tree) in ["wal-archive", "wal-archive-durable"].iter().enumerate() {
            let directory = root.join(tree);
            std::fs::create_dir_all(&directory).unwrap();
            let mut archive = broca_wal::archive::ArchiveWriter::create(
                directory.join(format!("fold-{}.ark", index + 1)),
            )
            .unwrap()
            .with_feature_floor(root.join(broca_wal::FEATURE_FLOOR_FILENAME));
            for item in 0..2 {
                let name = format!("archived-{index}-{item}");
                let bind = session(&name);
                let key = format!("lineage-{index}-{item}:4");
                let mut records = held_records(&key, "tools", &bind);
                records.truncate(4);
                write_wal_gated(&wal, &bind, &records);
                archive
                    .append(
                        &broca_core::session_addr(&bind),
                        &std::fs::read(wal.wal_path(&bind)).unwrap(),
                    )
                    .unwrap();
                std::fs::remove_file(wal.wal_path(&bind)).unwrap();
                expected.push(key);
            }
            archive.seal().unwrap();
        }
        let report = rebuild_store_from_wals(&store, &root).unwrap();
        assert!(report.unreadable.is_empty(), "{report:?}");
        assert_eq!(report.files, 4);
        assert_eq!(report.decoded, 4);
        let mut keys: Vec<_> = store
            .registrations()
            .unwrap()
            .into_iter()
            .map(|call| call.call_key)
            .collect();
        keys.sort();
        expected.sort();
        assert_eq!(keys, expected);
        assert_eq!(
            std::fs::read_dir(root.join("wal")).unwrap().count(),
            0,
            "rebuild never extracts an archive into the live tree"
        );
    }

    #[test]
    fn rebuild_rejects_a_torn_published_archive_and_an_inventory_move() {
        let root = rig_root("rebuild-archive-torn");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("wal-archive")).unwrap();
        std::fs::write(
            root.join(crate::late_results_store::REGISTRATION_MARKER),
            b"",
        )
        .unwrap();
        let store = LateResultsStore::open(&root.join("late-results.db")).unwrap();
        let path = root.join("wal-archive/fold-1.ark");
        let mut archive = broca_wal::archive::ArchiveWriter::create(&path)
            .unwrap()
            .with_feature_floor(root.join(broca_wal::FEATURE_FLOOR_FILENAME));
        archive.append("0123456789abcdef", b"torn").unwrap();
        archive.seal().unwrap();
        let report = rebuild_store_from_wals(&store, &root).unwrap();
        assert_eq!(report.unreadable.as_slice(), std::slice::from_ref(&path));
        assert!(root
            .join(crate::late_results_store::REBUILD_REQUIRED_MARKER)
            .exists());
        std::fs::remove_file(path).unwrap();
        let wal = FileWalStore::new(root.join("wal"));
        let bind = session("moved");
        write_wal_gated(&wal, &bind, &held_records("lineage:4", "tools", &bind));
        let moved = std::cell::Cell::new(false);
        let error = rebuild_store_from_wals_with_progress(&store, &root, |_| {
            if !moved.replace(true) {
                std::fs::write(root.join("wal/new.wal"), b"").unwrap();
            }
        })
        .unwrap_err();
        assert!(error.contains("inventory changed"), "{error}");
    }

    #[async_trait]
    impl crate::held_withdrawals::WithdrawTransport for WithdrawalRoute {
        async fn withdraw(
            &self,
            call: &LateResultRegistration,
        ) -> (
            cortexkit_role_tool_provider::withdraw::WithdrawVerdict,
            Value,
        ) {
            use cortexkit_role_tool_provider::withdraw::caller_verdict;
            assert!(
                self.wal
                    .read_records(&call.bind)
                    .unwrap()
                    .iter()
                    .any(|frame| matches!(
                        frame.record,
                        WalRecord::RunFinished {
                            reason: broca_protocol::RunFinishReason::Cancelled,
                            ..
                        }
                    )),
                "withdraw must follow the durable cancel seal"
            );
            self.seen.lock().unwrap().push(call.call_key.clone());
            if self.mode == "offline" {
                let error = serde_json::from_value(
                    json!({"code": "target_unavailable", "message": "offline"}),
                )
                .unwrap();
                (
                    caller_verdict(Err(&error)),
                    serde_json::to_value(error).unwrap(),
                )
            } else {
                let raw = if self.mode == "refused" {
                    json!({"answer": "refused", "reason": "denied"})
                } else {
                    json!({"answer": "withdrawn"})
                };
                (caller_verdict(Ok(&raw)), raw)
            }
        }
    }

    #[tokio::test]
    async fn held_withdrawal_cancel_seal_crash_cut_recovers_once_even_after_intake_prunes() {
        use crate::held_slots::HeldWithdrawal;
        let root = rig_root("withdraw-seal-cut");
        let _ = std::fs::remove_dir_all(&root);
        let bind = session("cancel-seal-cut");
        let wal = Arc::new(FileWalStore::new(root.join("wal")));
        let mut records = held_records("lineage:4", "tools", &bind);
        write_wal_gated(&wal, &bind, &records);
        let path = root.join("late-results.db");
        let store = Arc::new(LateResultsStore::open(&path).unwrap());
        store
            .register_calls(&held_registrations(&framed(records.clone()), &bind))
            .unwrap();
        let route = Arc::new(WithdrawalRoute {
            wal: wal.clone(),
            seen: Mutex::default(),
            mode: "ok",
        });
        let service = crate::held_withdrawals::HeldWithdrawals::new(
            store.clone(),
            wal.clone(),
            route.clone(),
        );
        service.bind_stopping_signal(tokio_util::sync::CancellationToken::new());
        let reply = broca_core::tool_plane::ProviderHeldReply {
            tool_call_id: ToolCallId("a".into()),
            call_key: "lineage:4".into(),
            module: "tools".into(),
            reason: "approval".into(),
            detail_digest: None,
        };
        service.withdraw(&reply).await.unwrap();
        assert!(
            route.seen.lock().unwrap().is_empty(),
            "an open hold is not a cancel"
        );
        records.push(WalRecord::RunFinished {
            reason: broca_protocol::RunFinishReason::Cancelled,
            interrupt_reason: None,
            ts_ms: Some(50),
            usage: Default::default(),
            error: None,
            retries_used: 0,
        });
        write_wal_gated(&wal, &bind, &records);
        std::fs::OpenOptions::new()
            .write(true)
            .open(wal.wal_path(&bind))
            .unwrap()
            .sync_all()
            .unwrap();
        crate::retention::sync_dir(wal.root()).unwrap();
        // The process stops after the synced seal and before invoking the withdrawal hook.
        store.remove("tools", "lineage:4").unwrap();
        drop(service);
        drop(store);
        let store = Arc::new(LateResultsStore::open(&path).unwrap());
        store.recovery_snapshot().unwrap();
        let service = crate::held_withdrawals::HeldWithdrawals::new(store, wal, route.clone());
        service.bind_stopping_signal(tokio_util::sync::CancellationToken::new());
        service.recover().await.unwrap();
        service.withdraw(&reply).await.unwrap();
        service.recover().await.unwrap();
        assert_eq!(*route.seen.lock().unwrap(), ["lineage:4"]);
        assert_eq!(service.health()["failed"], 0);
    }

    #[tokio::test]
    async fn held_withdrawal_shutdown_and_restart_send_none() {
        let root = rig_root("withdraw-shutdown");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let bind = session("shutdown");
        let wal = Arc::new(FileWalStore::new(root.join("wal")));
        let mut records = held_records("lineage:4", "tools", &bind);
        let store = Arc::new(LateResultsStore::open(&root.join("late-results.db")).unwrap());
        store
            .register_calls(&held_registrations(&framed(records.clone()), &bind))
            .unwrap();
        let route = Arc::new(WithdrawalRoute {
            wal: wal.clone(),
            seen: Mutex::default(),
            mode: "ok",
        });
        let service =
            crate::held_withdrawals::HeldWithdrawals::new(store, wal.clone(), route.clone());
        service.bind_stopping_signal(tokio_util::sync::CancellationToken::new());
        write_wal_gated(&wal, &bind, &records);
        service.recover().await.unwrap();
        records.push(WalRecord::RunFinished {
            reason: broca_protocol::RunFinishReason::Interrupted,
            interrupt_reason: Some("runner_shutdown".into()),
            ts_ms: Some(50),
            usage: Default::default(),
            error: None,
            retries_used: 0,
        });
        write_wal_gated(&wal, &bind, &records);
        service.recover().await.unwrap();
        assert!(route.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn held_withdrawal_failures_are_bounded_durable_and_do_not_change_cancel() {
        for (mode, expected) in [("offline", 3), ("refused", 1)] {
            let root = rig_root(&format!("withdraw-{mode}"));
            let _ = std::fs::remove_dir_all(&root);
            let bind = session(mode);
            let wal = Arc::new(FileWalStore::new(root.join("wal")));
            let mut records = held_records("lineage:4", "tools", &bind);
            records.push(WalRecord::RunFinished {
                reason: broca_protocol::RunFinishReason::Cancelled,
                interrupt_reason: None,
                ts_ms: Some(50),
                usage: Default::default(),
                error: None,
                retries_used: 0,
            });
            write_wal_gated(&wal, &bind, &records);
            let path = root.join("late-results.db");
            let store = Arc::new(LateResultsStore::open(&path).unwrap());
            store
                .register_calls(&held_registrations(&framed(records), &bind))
                .unwrap();
            let before = std::fs::read(wal.wal_path(&bind)).unwrap();
            let route = Arc::new(WithdrawalRoute {
                wal: wal.clone(),
                seen: Mutex::default(),
                mode,
            });
            let service =
                crate::held_withdrawals::HeldWithdrawals::new(store, wal.clone(), route.clone());
            service.bind_stopping_signal(tokio_util::sync::CancellationToken::new());
            let slots = crate::held_slots::HeldSlots::new();
            slots.with_withdrawals(service.clone());
            broca_core::tool_plane::ProviderHeldLifecycle::withdraw_held(
                &*slots,
                &broca_core::tool_plane::ProviderHeldReply {
                    tool_call_id: ToolCallId("a".into()),
                    call_key: "lineage:4".into(),
                    module: "tools".into(),
                    reason: "approval".into(),
                    detail_digest: None,
                },
            )
            .await
            .unwrap();
            assert_eq!(service.health()["failed"], 1);
            drop(slots);
            drop(service);
            let store = Arc::new(LateResultsStore::open(&path).unwrap());
            let service =
                crate::held_withdrawals::HeldWithdrawals::new(store, wal.clone(), route.clone());
            service.bind_stopping_signal(tokio_util::sync::CancellationToken::new());
            service.recover().await.unwrap();
            assert_eq!(route.seen.lock().unwrap().len(), expected);
            assert_eq!(
                std::fs::read(wal.wal_path(&bind)).unwrap(),
                before,
                "withdraw never rewrites or reopens a cancelled run"
            );
        }
    }

    /// A store rebuilt from the WAL trees registers the held calls it finds (open or closed),
    /// marks their feeds as owing an ack, and verifies ordinary WALs as well.
    #[test]
    fn a_rebuilt_store_registers_held_calls_and_owes_their_feeds_an_ack() {
        let root = rig_root("rebuild");
        let _ = std::fs::remove_dir_all(&root);
        let wal_root = root.join("wal-root");
        let open = session("open-held");
        let closed = session("closed-held");
        let wal = FileWalStore::new(wal_root.join("wal"));
        write_wal_gated(&wal, &open, &held_records("lineage:4", "tools", &open));
        write_wal_gated(
            &wal,
            &closed,
            &delivered_records("lineage:7", "core", &closed),
        );
        quiet_bind("never-held", &wal_root.join("wal"));
        // A real writer raised the floor before the held frames reached the files.
        broca_wal::raise_floor(
            &wal_root.join(broca_wal::FEATURE_FLOOR_FILENAME),
            &[broca_wal::PROVIDER_HELD_FEATURE],
        )
        .unwrap();
        let store = LateResultsStore::open(&root.join("late-results.db")).unwrap();
        let report = rebuild_store_from_wals(&store, &wal_root).unwrap();
        assert!(!report.skipped_by_floor);
        assert_eq!(report.files, 3);
        assert_eq!(
            report.decoded, 3,
            "registration precedes a held reply, so every WAL is verified"
        );
        assert!(report.unreadable.is_empty());
        let mut keys: Vec<String> = store
            .registrations()
            .unwrap()
            .into_iter()
            .map(|call| call.call_key)
            .collect();
        keys.sort();
        assert_eq!(keys, ["lineage:4", "lineage:7"]);
        assert!(store.feed("tools").unwrap().unwrap().ack_owed);
        assert!(store.feed("core").unwrap().unwrap().ack_owed);
    }

    /// A state root whose feature floor never named `provider-held/v1` holds no held frame, so a
    /// rebuild there reads no WAL at all: a deployment that never held a call pays nothing.
    #[test]
    fn a_rebuild_reads_no_wal_when_the_floor_never_named_a_held_frame() {
        let root = rig_root("rebuild-floor");
        let _ = std::fs::remove_dir_all(&root);
        let wal_root = root.join("wal-root");
        quiet_bind("never-held", &wal_root.join("wal"));
        let store = LateResultsStore::open(&root.join("late-results.db")).unwrap();
        let report = rebuild_store_from_wals(&store, &wal_root).unwrap();
        assert!(report.skipped_by_floor);
        assert_eq!(report.files, 0);
        assert!(store.registrations().unwrap().is_empty());
    }

    /// Write one session's records as a real writer frames them, gated frames included, so a
    /// held frame includes the reader feature its decoder requires.
    fn write_wal_gated(wal: &FileWalStore, bind: &BindIdentity, records: &[WalRecord]) {
        let path = wal.wal_path(bind);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let bytes: Vec<u8> = records
            .iter()
            .enumerate()
            .flat_map(|(index, record)| {
                broca_wal::framing::encode(index as u64 + 1, 1, record).unwrap()
            })
            .collect();
        std::fs::write(path, bytes).unwrap();
    }

    /// While the store is being rebuilt, no missing registration proves anything: every entry is
    /// Blocked, nothing is delivered and nothing is acked.
    #[tokio::test]
    async fn while_the_store_is_rebuilding_nothing_is_delivered_or_acked() {
        let call_key = "lineage:4";
        let bind = session("held");
        let rig = Rig::new(
            "rebuilding",
            Some((call_key, "tools", bind.clone())),
            vec![registration(call_key, "tools", &bind)],
        )
        .await;
        rig.intake.set_rebuilding(true);
        let _puller = rig.puller("tools");
        rig.feed.replies(vec![reply(
            vec![
                entry(kinds::RESULT, call_key, "ev-1"),
                entry(kinds::RESULT, "lineage:9", "ev-2"),
            ],
            2,
            false,
        )]);
        rig.intake.tick("tools").await.unwrap();
        assert!(rig.delivery.delivered.lock().unwrap().is_empty());
        assert!(rig.feed.acks().is_empty(), "{:?}", rig.feed.acks());
    }

    /// An entry whose held call already has its terminal (the entry was delivered and its ack
    /// lost, or the provider re-served it after a restart) is acked as a no-op: the result is
    /// never handed to the run a second time.
    #[tokio::test]
    async fn an_entry_for_a_call_that_already_has_its_terminal_is_acked_and_not_delivered() {
        let call_key = "lineage:4";
        let bind = session("answered");
        let rig = Rig::new(
            "answered",
            None,
            vec![registration(call_key, "tools", &bind)],
        )
        .await;
        write_wal_root(
            &rig.root.join("wal"),
            &bind,
            &delivered_records(call_key, "tools", &bind),
        );
        let _puller = rig.puller("tools");
        rig.feed.replies(vec![reply(
            vec![entry(kinds::RESULT, call_key, "ev-1")],
            1,
            false,
        )]);
        rig.intake.tick("tools").await.unwrap();
        assert!(rig.delivery.delivered.lock().unwrap().is_empty());
        assert_eq!(rig.feed.acks(), vec![cursor(1)]);
    }

    /// The same entry pulled twice before its ack lands is handed over once: the second read
    /// finds the first delivery under its `(provider, custodian, call_key, event_id)` key.
    #[tokio::test]
    async fn the_same_entry_read_twice_is_delivered_once() {
        let call_key = "lineage:4";
        let bind = session("held");
        let rig = Rig::new(
            "dedupe",
            Some((call_key, "tools", bind.clone())),
            vec![registration(call_key, "tools", &bind)],
        )
        .await;
        let _puller = rig.puller("tools");
        // The ack of the first read is lost, so the provider serves the entry again.
        rig.feed.fail_ack_once();
        rig.feed.replies(vec![
            reply(vec![entry(kinds::RESULT, call_key, "ev-1")], 1, false),
            reply(vec![entry(kinds::RESULT, call_key, "ev-1")], 1, false),
        ]);
        let _ = rig.intake.tick("tools").await;
        rig.intake.tick("tools").await.unwrap();
        assert_eq!(
            rig.delivery.delivered.lock().unwrap().clone(),
            vec![(call_key.to_owned(), "ev-1".to_owned())]
        );
        assert_eq!(rig.feed.acks(), vec![cursor(1)]);
    }

    /// An `expired` entry is the call's terminal answer: it is handed to the run like a result,
    /// never dropped as a no-op and never left pending.
    #[tokio::test]
    async fn an_expired_entry_is_delivered_as_the_calls_terminal() {
        let call_key = "lineage:4";
        let bind = session("held");
        let rig = Rig::new(
            "expired",
            Some((call_key, "tools", bind.clone())),
            vec![registration(call_key, "tools", &bind)],
        )
        .await;
        let _puller = rig.puller("tools");
        let mut expired = entry(kinds::EXPIRED, call_key, "ev-1");
        expired.result = None;
        rig.feed.replies(vec![reply(vec![expired], 1, false)]);
        rig.intake.tick("tools").await.unwrap();
        assert_eq!(
            rig.delivery.delivered.lock().unwrap().clone(),
            vec![(call_key.to_owned(), "ev-1".to_owned())]
        );
        assert_eq!(rig.feed.acks(), vec![cursor(1)]);
    }

    /// A `progress` entry settles nothing: it is acked and the call stays open.
    #[tokio::test]
    async fn a_progress_entry_is_acked_and_settles_nothing() {
        let call_key = "lineage:4";
        let bind = session("held");
        let rig = Rig::new(
            "progress",
            Some((call_key, "tools", bind.clone())),
            vec![registration(call_key, "tools", &bind)],
        )
        .await;
        let _puller = rig.puller("tools");
        rig.feed.replies(vec![reply(
            vec![entry(kinds::PROGRESS, call_key, "ev-1")],
            1,
            false,
        )]);
        rig.intake.tick("tools").await.unwrap();
        assert!(rig.delivery.delivered.lock().unwrap().is_empty());
        assert_eq!(rig.feed.acks(), vec![cursor(1)]);
        assert!(rig.store.registration("tools", call_key).unwrap().is_some());
    }

    /// A registration whose call has its terminal and whose batch committed is pruned by the
    /// puller's pass; a call still held keeps its registration and its feed.
    #[tokio::test]
    async fn a_settled_registration_is_pruned_and_a_held_one_is_kept() {
        let held_key = "lineage:4";
        let done_key = "lineage:7";
        let held = session("held");
        let done = session("done");
        let rig = Rig::new(
            "prune",
            Some((held_key, "tools", held.clone())),
            vec![
                registration(held_key, "tools", &held),
                registration(done_key, "tools", &done),
            ],
        )
        .await;
        write_wal_root(
            &rig.root.join("wal"),
            &done,
            &delivered_records(done_key, "tools", &done),
        );
        assert!(rig.intake.module_needs_feed("tools").unwrap());
        let kept: Vec<String> = rig
            .store
            .registrations()
            .unwrap()
            .into_iter()
            .map(|call| call.call_key)
            .collect();
        assert_eq!(kept, [held_key]);
    }

    fn peak_rss_kb() -> Option<u64> {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        status
            .lines()
            .find(|line| line.starts_with("VmHWM:"))
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|kb| kb.parse().ok())
    }

    /// The rebuild's cost on a state root the size of production's (about 235,000 session WALs),
    /// one in a thousand holding a held call. Run it on purpose:
    /// `cargo test -p broca-module --lib -- --ignored rebuild_scan_cost --nocapture`.
    /// `BROCA_REBUILD_BENCH_FILES` and `BROCA_REBUILD_BENCH_BYTES` (each WAL's size) change the
    /// shape. It prints the time and the process's peak memory before and after the scan.
    #[test]
    #[ignore = "measurement on a production-sized synthetic state root; run on purpose"]
    fn rebuild_scan_cost_on_a_production_sized_state_root() {
        let files: usize = std::env::var("BROCA_REBUILD_BENCH_FILES")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(235_000);
        let bytes: usize = std::env::var("BROCA_REBUILD_BENCH_BYTES")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(4096);
        const HELD_EVERY: usize = 1000;
        let root = rig_root("rebuild-bench");
        let _ = std::fs::remove_dir_all(&root);
        let wal_root = root.join("wal-root");
        let tree = wal_root.join("wal");
        std::fs::create_dir_all(&tree).unwrap();
        let frames = |records: &[WalRecord]| -> Vec<u8> {
            records
                .iter()
                .enumerate()
                .flat_map(|(index, record)| {
                    broca_wal::framing::encode(index as u64 + 1, 1, record).unwrap()
                })
                .collect()
        };
        let step: WalRecord =
            serde_json::from_str(r#"{"type":"step_started","step_id":1}"#).unwrap();
        let started = std::time::Instant::now();
        for index in 0..files {
            let bind = session(&format!("bench-{index}"));
            let mut records = if index % HELD_EVERY == 0 {
                held_records(&format!("lineage-{index}:4"), "tools", &bind)
            } else {
                let mut records = held_records("unused:1", "tools", &bind);
                records.truncate(1);
                records
            };
            let mut encoded = frames(&records);
            while encoded.len() < bytes {
                records.push(step.clone());
                encoded = frames(&records);
            }
            std::fs::write(tree.join(format!("{index:08}.wal")), encoded).unwrap();
        }
        broca_wal::raise_floor(
            &wal_root.join(broca_wal::FEATURE_FLOOR_FILENAME),
            &[broca_wal::PROVIDER_HELD_FEATURE],
        )
        .unwrap();
        let generated_ms = started.elapsed().as_millis();
        let store = LateResultsStore::open(&root.join("late-results.db")).unwrap();
        let rss_before = peak_rss_kb();
        let started = std::time::Instant::now();
        let report = rebuild_store_from_wals(&store, &wal_root).unwrap();
        let elapsed_ms = started.elapsed().as_millis();
        let rss_after = peak_rss_kb();
        println!(
            "REBUILD-BENCH files={} bytes_per_wal={bytes} decoded={} registered={} generate_ms={generated_ms} rebuild_ms={elapsed_ms} peak_rss_kb_before={rss_before:?} peak_rss_kb_after={rss_after:?}",
            report.files, report.decoded, report.registered
        );
        assert_eq!(report.files, files);
        assert_eq!(
            report.decoded, files,
            "a complete rebuild verifies unheld logs too"
        );
        assert!(report.unreadable.is_empty());
        let _ = std::fs::remove_dir_all(&root);
    }

    /// An idle poll (a page with no entry after the acked position) sends no ack: it would
    /// cover nothing. An ack the store still owed, as after a rebuild, is settled by reaching
    /// the feed's head without one.
    #[tokio::test]
    async fn an_empty_page_sends_no_ack_and_settles_an_owed_one() {
        let call_key = "lineage:4";
        let bind = session("held");
        let rig = Rig::new(
            "empty-page",
            Some((call_key, "tools", bind.clone())),
            vec![registration(call_key, "tools", &bind)],
        )
        .await;
        rig.store.set_ack_owed("tools", true).unwrap();
        let _puller = rig.puller("tools");
        rig.feed.replies(vec![reply(Vec::new(), 0, false)]);
        rig.intake.tick("tools").await.unwrap();
        assert_eq!(rig.feed.pulls().len(), 1);
        assert!(rig.feed.acks().is_empty(), "{:?}", rig.feed.acks());
        assert!(!rig.store.feed("tools").unwrap().unwrap().ack_owed);
    }

    /// An entry whose first delivery failed is pinned. Once a later round settles it, the pin
    /// is cleared and the entry is acked, so the feed does not stay unacked for good.
    #[tokio::test]
    async fn a_pinned_entry_that_later_settles_is_acked_and_unpinned() {
        let call_key = "lineage:4";
        let bind = session("held");
        let rig = Rig::new(
            "pin-settles",
            Some((call_key, "tools", bind.clone())),
            vec![registration(call_key, "tools", &bind)],
        )
        .await;
        let puller = rig.puller("tools");
        let page = || reply(vec![entry(kinds::RESULT, call_key, "ev-1")], 1, false);
        rig.feed.replies(vec![page(), page()]);
        // The first delivery fails the way a delivery that waited too long for the run fails.
        *rig.delivery.refuse.lock().unwrap() =
            Some("the run did not sync the held call's terminal in time".into());
        rig.intake.tick("tools").await.unwrap();
        assert!(rig.feed.acks().is_empty());
        assert_eq!(
            puller.pinned(),
            vec![entry(kinds::RESULT, call_key, "ev-1").intake_key("tools")]
        );

        *rig.delivery.refuse.lock().unwrap() = None;
        rig.intake.tick("tools").await.unwrap();
        assert_eq!(
            rig.delivery.delivered.lock().unwrap().clone(),
            vec![(call_key.to_owned(), "ev-1".to_owned())]
        );
        assert_eq!(
            rig.feed.acks(),
            vec![cursor(1)],
            "the pinned entry settled, so its page is acked; pinned={:?}",
            puller.pinned()
        );
        assert!(puller.pinned().is_empty(), "{:?}", puller.pinned());
        let feed = rig.store.feed("tools").unwrap().unwrap();
        assert_eq!(feed.position.map(|position| position.seq), Some(1));
        assert!(!feed.ack_owed);
    }

    /// A provider feed that keeps what it publishes: a pull answers every entry after `since`
    /// (or after the acked position, whichever is later), and the cursor is the entry count.
    #[derive(Default)]
    struct KeepingFeed {
        entries: Mutex<Vec<LateEntry>>,
        acked: Mutex<u64>,
        acks: Mutex<Vec<(u64, tokio::time::Instant)>>,
        pulls: Mutex<Vec<(Option<u64>, tokio::time::Instant)>>,
        changed: tokio::sync::Notify,
    }

    impl KeepingFeed {
        /// Wait, on the test's clock, until `done` holds for this feed.
        async fn wait_until(&self, done: impl Fn(&Self) -> bool) {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if done(self) {
                    return;
                }
                changed.await;
            }
        }
    }

    #[async_trait]
    impl LateResultsFeed for KeepingFeed {
        async fn pull(
            &self,
            _module: &str,
            since: Option<&Cursor>,
            _limit: Option<u32>,
        ) -> Result<FeedReply, FeedError> {
            self.pulls
                .lock()
                .unwrap()
                .push((since.map(|cursor| cursor.seq), tokio::time::Instant::now()));
            let entries = self.entries.lock().unwrap().clone();
            let from = since.map_or(0, |cursor| cursor.seq);
            let start = from.max(*self.acked.lock().unwrap()) as usize;
            self.changed.notify_waiters();
            Ok(FeedReply {
                entries: entries.iter().skip(start).cloned().collect(),
                cursor: cursor(entries.len() as u64),
                more: false,
            })
        }

        async fn ack(&self, _module: &str, through: &Cursor) -> Result<(), FeedError> {
            {
                let mut acked = self.acked.lock().unwrap();
                *acked = (*acked).max(through.seq);
            }
            self.acks
                .lock()
                .unwrap()
                .push((through.seq, tokio::time::Instant::now()));
            self.changed.notify_waiters();
            Ok(())
        }
    }

    /// A session actor that loses the first resume request, as an actor that does not keep a
    /// request arriving while its run is still pausing would. Every later request resumes the
    /// paused batch, whose held call waits in its slot again.
    struct LosesFirstResume {
        slots: Arc<crate::held_slots::HeldSlots>,
        lose_next: std::sync::atomic::AtomicBool,
        resumes: Mutex<Vec<(bool, tokio::time::Instant)>>,
        taken_at: Arc<Mutex<Option<tokio::time::Instant>>>,
        call_key: String,
    }

    #[async_trait]
    impl crate::held_slots::HeldRunResumer for LosesFirstResume {
        async fn resume_for_late_result(&self, session: &BindIdentity) -> Result<(), String> {
            use broca_core::tool_plane::ProviderHeldLifecycle as _;
            let lost = self
                .lose_next
                .swap(false, std::sync::atomic::Ordering::SeqCst);
            self.resumes
                .lock()
                .unwrap()
                .push((lost, tokio::time::Instant::now()));
            if lost {
                return Ok(());
            }
            let slots = Arc::clone(&self.slots);
            let taken_at = Arc::clone(&self.taken_at);
            let bind = session.clone();
            let call_key = self.call_key.clone();
            tokio::spawn(async move {
                let ctx = broca_core::tool_plane::BatchCtx {
                    batch_id: BatchId("b".into()),
                    bind,
                    run_id: RunId("run".into()),
                    scope: None,
                    media_capture: None,
                };
                let call = broca_protocol::ToolCall {
                    tool_call_id: ToolCallId("a".into()),
                    tool_name: "ask".into(),
                    input: json!({}),
                    thought_signature: None,
                };
                let reply = broca_core::tool_plane::ProviderHeldReply {
                    tool_call_id: ToolCallId("a".into()),
                    call_key,
                    module: "tools".into(),
                    reason: "running".into(),
                    detail_digest: None,
                };
                let _ = slots.wait(&ctx, &call, &reply).await;
                *taken_at.lock().unwrap() = Some(tokio::time::Instant::now());
                slots.terminal_synced(&ctx, &call.tool_call_id).await;
            });
            Ok(())
        }
    }

    /// The whole delivery chain on a paused clock: the spawned puller loop, `HeldSlots` and
    /// `SlotDelivery`. The entry is published before the first pull, and the first resume
    /// request is lost, so the first delivery times out and pins the entry. The next pull after
    /// the timeout delivers it from the pin and acks it once; later pulls read from the acked
    /// position instead of re-reading the feed from its start.
    #[tokio::test(start_paused = true)]
    async fn a_delivery_that_times_out_is_delivered_from_its_pin_and_acked() {
        let call_key = "lineage:4";
        let bind = session("held");
        let root = rig_root("timed-out-delivery");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let wal = Arc::new(FileWalStore::new(root.join("wal")));
        write_wal(&wal, &bind, &held_records(call_key, "tools", &bind));
        let store = Arc::new(LateResultsStore::open(&root.join("late-results.db")).unwrap());
        register_more(&store, &[registration(call_key, "tools", &bind)]).await;
        let slots = crate::held_slots::HeldSlots::new();
        let taken_at = Arc::new(Mutex::new(None));
        let actor = Arc::new(LosesFirstResume {
            slots: Arc::clone(&slots),
            lose_next: std::sync::atomic::AtomicBool::new(true),
            resumes: Mutex::new(Vec::new()),
            taken_at: Arc::clone(&taken_at),
            call_key: call_key.into(),
        });
        let mapper: Arc<crate::held_slots::EntryMapper> = Arc::new(|id, _entry| {
            Some(broca_protocol::ToolResult {
                tool_call_id: id.clone(),
                output: broca_protocol::ToolOutput::Text {
                    text: "late".into(),
                },
                is_error: false,
            })
        });
        let delivery = Arc::new(crate::held_slots::SlotDelivery::new(
            Arc::clone(&slots),
            Arc::clone(&actor) as Arc<dyn crate::held_slots::HeldRunResumer>,
            mapper,
        ));
        let feed = Arc::new(KeepingFeed::default());
        feed.entries
            .lock()
            .unwrap()
            .push(entry(kinds::RESULT, call_key, "event-1"));
        let intake = LateResultsIntake::new(
            Arc::clone(&store),
            Arc::clone(&wal),
            "broca",
            delivery as Arc<dyn HeldDelivery>,
            Arc::clone(&feed) as Arc<dyn LateResultsFeed>,
        );
        let start = tokio::time::Instant::now();
        assert_eq!(
            intake.ensure_puller("tools").unwrap(),
            StartOutcome::Started
        );
        // Both waits run on the paused clock, so the bound costs no real time. It only turns
        // an ack that never comes into a failure instead of a hang.
        let acked = tokio::time::timeout(
            Duration::from_secs(600),
            feed.wait_until(|feed| !feed.acks.lock().unwrap().is_empty()),
        )
        .await;
        let since = |at: tokio::time::Instant| (at - start).as_millis();
        assert!(
            acked.is_ok(),
            "the entry is never acked; pinned={:?} feed_row={:?}",
            intake.puller("tools").map(|puller| puller.pinned()),
            store.feed("tools").unwrap()
        );
        let fallback =
            (crate::held_slots::SlotDelivery::DEFAULT_TIMEOUT + PULL_INTERVAL).as_millis();
        assert_eq!(taken_at.lock().unwrap().map(since), Some(fallback));
        let resumes: Vec<_> = actor
            .resumes
            .lock()
            .unwrap()
            .iter()
            .map(|(lost, at)| (*lost, since(*at)))
            .collect();
        assert_eq!(resumes, vec![(true, 0), (false, fallback)]);
        let acks: Vec<_> = feed
            .acks
            .lock()
            .unwrap()
            .iter()
            .map(|(seq, at)| (*seq, since(*at)))
            .collect();
        assert_eq!(acks, vec![(1, fallback)]);
        assert!(intake
            .puller("tools")
            .is_some_and(|puller| puller.pinned().is_empty()));
        let row = store.feed("tools").unwrap().unwrap();
        assert_eq!(row.position.map(|position| position.seq), Some(1));
        assert!(!row.ack_owed);

        // Two more rounds: both read from the acked position, and nothing is acked again.
        let pulls_at_ack = feed.pulls.lock().unwrap().len();
        tokio::time::timeout(
            Duration::from_secs(600),
            feed.wait_until(|feed| feed.pulls.lock().unwrap().len() >= pulls_at_ack + 2),
        )
        .await
        .expect("the puller keeps pulling while the call is registered as held");
        let later: Vec<_> = feed.pulls.lock().unwrap()[pulls_at_ack..]
            .iter()
            .map(|(since, _)| *since)
            .collect();
        assert!(later.iter().all(|since| *since == Some(1)), "{later:?}");
        assert_eq!(feed.acks.lock().unwrap().len(), 1);
        if let Some(puller) = intake.puller("tools") {
            puller.stop();
        }
    }
}
