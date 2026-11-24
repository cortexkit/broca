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
//! A puller runs only while the module still needs its feed: a registered call with an open
//! held record or an owed batch commit barrier, an entry no ack covers yet, or a registered
//! session's log the intake cannot read (which may hide a hold). Ordinary calls to a provider
//! that can settle late never start one.
//!
//! This module deliberately enables nothing itself. The tool plane declares the held-reply
//! capabilities on its routes, and the run holds a batch open; this module is the store, the
//! puller and the delivery hook that code is written against.

use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use broca_core::tool_plane::LateResultRegistration;
use broca_protocol::{BindIdentity, ToolCallId};
use cortexkit_role_tool_provider::late_results::{Cursor, EntryKind, IntakeKey, LateEntry};

use crate::late_results_store::{
    Disposition, FeedPosition, LateResultsStore, RegistrationRow, RegistrationState,
};

mod stored;
pub(crate) use stored::is_notice_send_id;
use stored::StoredLineage;
pub use stored::{
    held_call_records, notice_send_id, rebuild_store_from_sessions_with_progress, HeldCallRecords,
};

/// How often a started feed is pulled.
pub const PULL_INTERVAL: Duration = Duration::from_secs(3);
/// The longest wait between attempts after a transport error or a refusal.
pub const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// An entry that still prevents the feed's ack from advancing after this interval needs
/// operator attention, even if the provider keeps returning successful feed replies.
pub const PIN_WARN_AFTER: Duration = Duration::from_secs(5 * 60);
/// After three identical delivery failures, expose the delivery as stuck in health diagnostics.
pub const DELIVERY_STUCK_AFTER: u32 = 3;
/// Bound each reply while allowing later entries to progress behind an unresolved prefix.
pub const FEED_PAGE_SIZE: u32 = 128;
/// Limit log reads for old registrations to one small page per maintenance pass.
pub const REGISTRATION_SWEEP_LIMIT: usize = 64;
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
    /// The intent is durable in an open run, but neither terminal nor held record is synced yet.
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

    /// Deliver the entry, the terminal of a call handed back for the person's steer, to the
    /// call's session as a notice from the call's provider. `Ok` means the notice is durable in
    /// the session, or that the session refused it for good; `Err` is retried on a later pull.
    async fn notice(
        &self,
        _registration: &LateResultRegistration,
        _call: &YieldedCall,
        _entry: &LateEntry,
    ) -> Result<NoticeOutcome, String> {
        Err("this intake delivers no notices".into())
    }
}

/// A call handed back for the person's steer (llm-runner `call_yield`), whose provider's
/// terminal for it the session receives as a notice.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct YieldedCall {
    pub tool_call_id: ToolCallId,
    /// The call's tool, as the notice names it; empty when its intent didn't record one.
    pub tool_name: String,
}

/// What became of a handed-back call's notice.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NoticeOutcome {
    /// The notice is durable in the session.
    Delivered,
    /// The session refused it for a reason a retry can't change, such as a plan that doesn't
    /// name the provider as a sender. The entry is settled without delivery.
    Refused(String),
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
    ages: HashMap<IntakeKey, PinAge>,
}

struct PinAge {
    since: tokio::time::Instant,
    warned: bool,
}

/// One running feed.
pub struct Puller {
    state: Mutex<PullerState>,
    interval: Mutex<Duration>,
    stopped: Mutex<bool>,
}

impl Puller {
    /// Finish the current attempt and stop, without pulling again.
    pub fn stop(&self) {
        *self.stopped.lock().expect("puller stop") = true;
    }

    fn new() -> Self {
        Self {
            state: Mutex::new(PullerState {
                pinned: Vec::new(),
                ages: HashMap::new(),
            }),
            interval: Mutex::new(PULL_INTERVAL),
            stopped: Mutex::new(false),
        }
    }

    /// Entries an earlier reply left unacked because one of them could not be settled.
    pub fn pinned(&self) -> Vec<IntakeKey> {
        self.state.lock().expect("puller state").pinned.clone()
    }

    fn set_pinned(&self, pinned: Vec<IntakeKey>) {
        let mut state = self.state.lock().expect("puller state");
        state.ages.retain(|key, _| pinned.contains(key));
        for key in &pinned {
            state.ages.entry(key.clone()).or_insert_with(|| PinAge {
                since: tokio::time::Instant::now(),
                warned: false,
            });
        }
        state.pinned = pinned;
    }

    fn warn_overdue(&self, module: &str) {
        for (key, age) in &mut self.state.lock().expect("puller state").ages {
            if !age.warned && age.since.elapsed() >= PIN_WARN_AFTER {
                age.warned = true;
                tracing::warn!(target: "late_results", module, call_key = key.call_key,
                    event_id = key.event_id, "late-results entry has held the feed's ack for five \
                    minutes: its call has neither a terminal nor a held record yet (another call \
                    of its batch may still be running, or its held reply isn't recorded)");
            }
        }
    }

    fn overdue_pins(&self) -> usize {
        self.state
            .lock()
            .expect("puller state")
            .ages
            .values()
            .filter(|age| age.since.elapsed() >= PIN_WARN_AFTER)
            .count()
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

    pub fn is_stopped(&self) -> bool {
        *self.stopped.lock().expect("puller stop")
    }
}

/// The intake for every provider module in this process.
pub struct LateResultsIntake {
    store: Arc<LateResultsStore>,
    session_store: Mutex<Option<Arc<crate::store_open::StoreRuntime>>>,
    /// The registry whose open actors answer reads of their sessions, once installed (see
    /// [`Self::set_registry`]). A puller polls every session it holds calls for each round, and
    /// a running session moves between rounds, so reading it as stored would load the whole
    /// view again each time.
    registry: Mutex<Option<Arc<crate::LineageRegistry>>>,
    /// The module id the daemon attests for this process: the feed's custodian. Without one no
    /// plain key is sent and no feed is pulled.
    consumer_module: String,
    delivery: Arc<dyn HeldDelivery>,
    feed: Arc<dyn LateResultsFeed>,
    /// The running pullers, one per provider module. The claim lives only in this process: a
    /// puller never survives its process, so nothing durable may say one is running.
    pullers: Mutex<HashMap<String, Arc<Puller>>>,
    /// Set while the store is being rebuilt from the session logs. Until the rebuild completes, no
    /// registration can be trusted to be missing, so every entry is Blocked and nothing is acked.
    rebuilding: std::sync::atomic::AtomicBool,
    sweep_after: tokio::sync::Mutex<Option<(String, String)>>,
    delivery_failures: Mutex<HashMap<IntakeKey, DeliveryFailure>>,
}

#[derive(Clone)]
struct DeliveryFailure {
    reason: String,
    attempts: u32,
}

impl LateResultsIntake {
    pub fn new(
        store: Arc<LateResultsStore>,
        consumer_module: impl Into<String>,
        delivery: Arc<dyn HeldDelivery>,
        feed: Arc<dyn LateResultsFeed>,
    ) -> Arc<Self> {
        Arc::new(Self {
            store,
            session_store: Mutex::new(None),
            registry: Mutex::new(None),
            consumer_module: consumer_module.into(),
            delivery,
            feed,
            pullers: Mutex::new(HashMap::new()),
            rebuilding: std::sync::atomic::AtomicBool::new(false),
            sweep_after: tokio::sync::Mutex::new(None),
            delivery_failures: Mutex::new(HashMap::new()),
        })
    }

    /// Install the reader that owns session-store sessions before recovering their feeds.
    pub fn set_session_store(&self, runtime: Arc<crate::store_open::StoreRuntime>) {
        *self.session_store.lock().expect("session store") = Some(runtime);
    }

    /// Install the registry whose open actors answer the intake's reads of their sessions from
    /// memory. Until it is installed, every session is read as stored.
    pub fn set_registry(&self, registry: Arc<crate::LineageRegistry>) {
        *self.registry.lock().expect("registry") = Some(registry);
    }

    /// Read every session as stored again, as before [`Self::set_registry`].
    #[cfg(test)]
    pub(crate) fn forget_registry(&self) {
        *self.registry.lock().expect("registry") = None;
    }

    /// Mark the store as being rebuilt (or done).
    pub fn set_rebuilding(&self, rebuilding: bool) {
        self.rebuilding
            .store(rebuilding, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn is_rebuilding(&self) -> bool {
        self.rebuilding.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// The custodian string the provider stamps on entries served to this process.
    pub fn custodian(&self) -> String {
        format!("reserved:{}", self.consumer_module)
    }

    /// The dedicated maintenance bind for the custodian's global feeds, never a conversation
    /// identity: each provider's cursor and ack cover all of this custodian's sessions.
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

    /// In-memory only: health checks never open a session's log or the registration store.
    pub fn overdue_pins(&self) -> usize {
        self.pullers
            .lock()
            .expect("pullers")
            .values()
            .map(|puller| puller.overdue_pins())
            .sum()
    }

    /// Content-free, process-local evidence; a successful retry removes its finding.
    pub fn stuck_deliveries(&self) -> Vec<serde_json::Value> {
        self.delivery_failures
            .lock()
            .expect("delivery failures")
            .iter()
            .filter(|(_, failure)| failure.attempts >= DELIVERY_STUCK_AFTER)
            .map(|(key, failure)| {
                serde_json::json!({
                    "module": key.provider, "call_key": key.call_key, "event_id": key.event_id,
                    "attempts": failure.attempts,
                })
            })
            .collect()
    }

    /// Revisit old rows even for providers with no running feed. Each pass reads at most one
    /// page of registrations' session records; unreadable and unfinished calls remain for the next cycle.
    pub async fn sweep_registrations(&self) -> Result<usize, String> {
        if self.is_rebuilding() || !self.store.has_held_history() {
            return Ok(0);
        }
        let mut after = self.sweep_after.lock().await;
        let rows = self
            .store
            .registration_page(after.as_ref(), REGISTRATION_SWEEP_LIMIT)?;
        let mut views = HashMap::new();
        // Calls retired here. Those no withdrawal can be owed for (see
        // `withdrawal_never_owed`) give up their withdrawal watches with their registrations, so
        // the watches don't pile up for recovery to read at every start; the rest keep theirs.
        let mut settled = Vec::new();
        let mut spent = Vec::new();
        let mut retired = Vec::new();
        for row in &rows {
            if !views.contains_key(&row.call.bind) {
                views.insert(
                    row.call.bind.clone(),
                    self.lineage_view(&row.call, &rows).await,
                );
            }
            let view = views.get(&row.call.bind).expect("lineage view");
            match &view.source {
                Source::Stored(stored) => {
                    if let Some(reason) = stored.retirement_reason(&row.call.call_key) {
                        let key = (row.call.module.clone(), row.call.call_key.clone());
                        if stored.withdrawal_never_owed(&row.call.call_key) {
                            spent.push(key);
                        } else {
                            settled.push(key);
                        }
                        retired.push((&row.call, reason));
                    }
                }
                Source::Gone => {
                    spent.push((row.call.module.clone(), row.call.call_key.clone()));
                    retired.push((&row.call, "session_deleted"));
                }
                Source::Unknown => {}
            }
        }
        self.store.remove_calls(&settled)?;
        self.store.remove_spent_calls(&spent)?;
        for (call, reason) in retired {
            log_retirement(call, reason);
        }
        *after = if rows.len() == REGISTRATION_SWEEP_LIMIT {
            rows.last()
                .map(|row| (row.call.module.clone(), row.call.call_key.clone()))
        } else {
            None
        };
        Ok(settled.len() + spent.len())
    }

    fn puller(&self, module: &str) -> Option<Arc<Puller>> {
        self.pullers.lock().expect("pullers").get(module).cloned()
    }

    /// What the store says of the session `registration` names, read once per round for every
    /// registration of that session in `rows`.
    pub(crate) async fn lineage_view(
        &self,
        registration: &LateResultRegistration,
        rows: &[RegistrationRow],
    ) -> LineageView {
        let runtime = self.session_store.lock().expect("session store").clone();
        let registry = self.registry.lock().expect("registry").clone();
        let source = match runtime {
            // The store isn't installed yet: nothing about the session is known.
            None => Source::Unknown,
            Some(runtime) => {
                let keys = rows
                    .iter()
                    .filter(|row| row.call.bind == registration.bind)
                    .map(|row| row.call.call_key.clone())
                    .collect();
                match StoredLineage::read(&runtime, registry.as_deref(), &registration.bind, keys)
                    .await
                {
                    Some(stored) => Source::Stored(stored),
                    // A session with a registered call ran, so a store that holds no head for
                    // it deleted it: nothing of it can be delivered any more.
                    None => Source::Gone,
                }
            }
        };
        LineageView {
            source,
            delivered: HashSet::new(),
        }
    }

    /// Whether the module's feed is still needed, decided from the store's registrations and the
    /// session logs they name: an owed ack, a registered call whose session's log has an open
    /// held call or an owed batch commit barrier, or a registered session's log that cannot be
    /// read (which may hide a hold).
    ///
    /// The same pass prunes the registrations that are settled for good, so a registration
    /// never outlives the commit of the batch that settled its call.
    pub async fn module_needs_feed(&self, module: &str) -> Result<bool, String> {
        self.needs_feed_in(module, &mut HashMap::new()).await
    }

    /// [`Self::module_needs_feed`], reading each registered session into `views` unless it is
    /// there already, so the round that goes on to pull classifies from the same reads.
    async fn needs_feed_in(
        &self,
        module: &str,
        views: &mut HashMap<BindIdentity, LineageView>,
    ) -> Result<bool, String> {
        let holds = self.holds_calls_in(module, views).await?;
        Ok(holds || self.store.feed(module)?.is_some_and(|feed| feed.ack_owed))
    }

    /// Start the module's puller when its feed is still needed and none runs. The claim lives in
    /// this process only: a puller never outlives its process, so a crash leaves nothing that
    /// could keep the next process from starting one.
    pub async fn ensure_puller(self: &Arc<Self>, module: &str) -> Result<StartOutcome, String> {
        // One puller per module, whichever of a renamed module's ids its calls were made under:
        // its feed opens under the id the daemon lists now (see `LateResultsFeed::open`).
        let module = broca_protocol::canonical_module(module);
        if !self.module_needs_feed(module).await? {
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

    /// Run the module's puller until nothing keeps it, then do one last pull and ack, release the
    /// slot and stop.
    ///
    /// A hold that arrives while the puller is finishing finds its claim still in place and
    /// leaves the feed to it ([`Self::ensure_puller`] answers `Held`). So after giving the claim
    /// up the puller looks once more, and takes the claim back and goes on when the feed is
    /// needed again and no other puller has started; otherwise that hold's result would never be
    /// pulled.
    fn spawn(self: &Arc<Self>, module: String, puller: Arc<Puller>) {
        let intake = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                loop {
                    if puller.is_stopped() {
                        break;
                    }
                    if !intake.round(&module, &puller).await {
                        break;
                    }
                    if puller.is_stopped() {
                        break;
                    }
                    tokio::time::sleep(puller.interval()).await;
                }
                let _ = intake.final_pull(&module).await;
                intake.pullers.lock().expect("pullers").remove(&module);
                if puller.is_stopped() || !intake.module_needs_feed(&module).await.unwrap_or(true) {
                    break;
                }
                let mut pullers = intake.pullers.lock().expect("pullers");
                if pullers.contains_key(&module) {
                    break;
                }
                pullers.insert(module.clone(), Arc::clone(&puller));
            }
        });
    }

    /// One round of the module's running puller: pull and settle its owed pages when its feed
    /// is still needed, and say whether it was. Each registered session is read once for both:
    /// a closed session is read as stored, and one too large to keep cached would otherwise load
    /// whole twice every round.
    async fn round(&self, module: &str, puller: &Puller) -> bool {
        let mut views = HashMap::new();
        if !self.needs_feed_in(module, &mut views).await.unwrap_or(true) {
            return false;
        }
        self.tick_in(module, puller, &mut views).await;
        true
    }

    /// One pull cycle for a module whose puller runs.
    pub async fn tick(&self, module: &str) -> Result<(), String> {
        let Some(puller) = self.puller(module) else {
            return Ok(());
        };
        self.tick_in(module, &puller, &mut HashMap::new()).await;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn tick_for_test(&self, module: &str) -> Result<(), FeedError> {
        self.pull_round(module, &Puller::new(), &mut HashMap::new())
            .await
    }

    /// One round of the module's puller: the next owed pages pulled and settled, each session
    /// read into `views` unless the round already read it.
    async fn tick_in(
        &self,
        module: &str,
        puller: &Puller,
        views: &mut HashMap<BindIdentity, LineageView>,
    ) {
        match self.pull_round(module, puller, views).await {
            Ok(()) => puller.reset_interval(),
            Err(FeedError::CursorIncarnationChanged) => {
                // The provider changed incarnation. Its `event_id`s are deterministic across
                // restarts, so the receipts absorb the re-read from the start.
                let _ = self.store.forget_cursor(module);
                puller.set_pinned(Vec::new());
                puller.reset_interval();
            }
            // Read afresh: a delivery this round may have settled a call since the round read it.
            Err(FeedError::Abandoned(reason)) => match self.module_holds_calls(module).await {
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
        puller.warn_overdue(module);
    }

    /// Whether the module still has a held call or an owed barrier: the condition that outranks
    /// an abandoned ack. Prunes every registration whose call is settled for good on the way.
    pub async fn module_holds_calls(&self, module: &str) -> Result<bool, String> {
        self.holds_calls_in(module, &mut HashMap::new()).await
    }

    /// [`Self::module_holds_calls`], reading each registered session into `views` unless it is
    /// there already.
    async fn holds_calls_in(
        &self,
        module: &str,
        views: &mut HashMap<BindIdentity, LineageView>,
    ) -> Result<bool, String> {
        let mut holds = false;
        // Calls retired here, their withdrawal watches with them where none can be owed, as in
        // the registration sweep.
        let mut settled = Vec::new();
        let mut spent = Vec::new();
        let mut retired = Vec::new();
        let rows = self.store.registrations_for(module)?;
        for row in &rows {
            if !views.contains_key(&row.call.bind) {
                views.insert(
                    row.call.bind.clone(),
                    self.lineage_view(&row.call, &rows).await,
                );
            }
            let view = views.get(&row.call.bind).expect("lineage view");
            match &view.source {
                Source::Stored(stored) => {
                    if let Some(reason) = stored.retirement_reason(&row.call.call_key) {
                        let key = (module.to_owned(), row.call.call_key.clone());
                        if stored.withdrawal_never_owed(&row.call.call_key) {
                            spent.push(key);
                        } else {
                            settled.push(key);
                        }
                        retired.push((&row.call, reason));
                    } else {
                        holds |= stored.needs_intake(&row.call.call_key);
                    }
                }
                Source::Gone => {
                    spent.push((module.to_owned(), row.call.call_key.clone()));
                    retired.push((&row.call, "session_deleted"));
                }
                // Until the store answers, a registration may still hide a hold.
                Source::Unknown => holds = true,
            }
        }
        self.store.remove_calls(&settled)?;
        self.store.remove_spent_calls(&spent)?;
        for (call, reason) in retired {
            log_retirement(call, reason);
        }
        Ok(holds)
    }

    /// Settle the pinned entries again, then deliver everything newer. An entry the ack cannot
    /// pass pins the ack where it is and never keeps a newly published entry from being
    /// delivered: the pinned entries are read from the start of the incarnation, and pages after
    /// that reply from its cursor, so no page is read twice.
    async fn pull_round(
        &self,
        module: &str,
        puller: &Puller,
        views: &mut HashMap<BindIdentity, LineageView>,
    ) -> Result<(), FeedError> {
        let mut read_from = self
            .store
            .feed(module)
            .map_err(FeedError::Retryable)?
            .and_then(|row| row.position);
        if !puller.pinned().is_empty() {
            let reply = self
                .pull(module, read_from.as_ref(), Some(FEED_PAGE_SIZE))
                .await?;
            read_from = Some(FeedPosition {
                provider_incarnation: reply.cursor.provider_incarnation.clone(),
                seq: reply.cursor.seq,
            });
            // While anything in the pinned reply is unsettled it is never acked: any ack
            // covering it would pass the pin.
            let (settled, still) = self.settle(module, &reply.entries, views).await?;
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
            let reply = self
                .pull(module, from.as_ref(), Some(FEED_PAGE_SIZE))
                .await?;
            let next = Some(FeedPosition {
                provider_incarnation: reply.cursor.provider_incarnation.clone(),
                seq: reply.cursor.seq,
            });
            let pinned_before_page = !puller.pinned().is_empty();
            // An entry delivered on a page is durable before anything can ack that page: a
            // delivery that could not be made pins the ack here instead.
            let (settled, still) = self.settle(module, &reply.entries, views).await?;
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
            } else if settled > 0 && !pinned_before_page {
                // The settled entries of this page form one run at its head, so a limit re-pull
                // of that run returns exactly the cursor to ack. An entry pinned before it is
                // not one of these, so the ack cannot pass it.
                let prefix: Vec<LateEntry> = reply.entries[..settled].to_vec();
                let _ = self
                    .ack_prefix(module, from.as_ref(), &prefix, views)
                    .await?;
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
        views: &mut HashMap<BindIdentity, LineageView>,
    ) -> Result<(usize, Vec<IntakeKey>), FeedError> {
        // Only this module's registrations can match its entries, and an empty pull needs none.
        let rows = if entries.is_empty() {
            Vec::new()
        } else {
            self.store
                .registrations_for(module)
                .map_err(FeedError::Retryable)?
        };
        let mut settled = 0usize;
        let mut pinned = Vec::new();
        for entry in entries {
            let key = entry.intake_key(module);
            let kind = self
                .classify_one(module, entry, &rows, views)
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
                    .deliver_settled(module, std::slice::from_ref(entry), &rows, views)
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
                    let mut failures = self.delivery_failures.lock().expect("delivery failures");
                    let failure = failures
                        .entry(key.clone())
                        .or_insert_with(|| DeliveryFailure {
                            reason: reason.clone(),
                            attempts: 0,
                        });
                    if failure.reason != reason {
                        failure.reason = reason;
                        failure.attempts = 0;
                    }
                    failure.attempts = failure.attempts.saturating_add(1);
                    pinned.push(key);
                    continue;
                }
            }
            self.delivery_failures
                .lock()
                .expect("delivery failures")
                .remove(&key);
            // Delivery may pass a pin, but acknowledgement still covers only a settled prefix.
            if pinned.is_empty() {
                settled += 1;
            }
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
        let kind = self.classify(module, entry, rows, views).await?;
        let session_address = rows
            .iter()
            .find(|row| {
                broca_protocol::same_module(&row.call.module, module)
                    && row.call.call_key == entry.call_key
            })
            .map(|row| broca_core::session_addr(&row.call.bind));
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
                    session_address,
                    reason,
                    "late-results entry deferred: {reason}"
                );
            }
            // Logged at debug: an entry waits here on every poll while its batch runs, and a
            // five-minute wait is warned about once (`Puller::warn_overdue`).
            Classified::Pending => {
                tracing::debug!(
                    target: "late_results",
                    module,
                    call_key = entry.call_key,
                    event_id = entry.event_id,
                    session_address,
                    "late-results entry waits for its call's terminal or held record"
                );
            }
            Classified::Delivered => {}
        }
        Ok(kind)
    }

    async fn classify(
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
        let Some(row) = rows.iter().find(|row| {
            broca_protocol::same_module(&row.call.module, module)
                && row.call.call_key == entry.call_key
        }) else {
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
            let view = self.lineage_view(&row.call, rows).await;
            views.insert(row.call.bind.clone(), view);
        }
        let view = views.get(&row.call.bind).expect("view was just inserted");
        if view.delivered.contains(&entry.call_key) {
            return Ok(Classified::NoOp);
        }
        Ok(match &view.source {
            Source::Stored(stored) => stored.classify(module, entry, row),
            // The session was deleted; its late entries are no-ops, never Blocked.
            Source::Gone => Classified::NoOp,
            Source::Unknown => Classified::Blocked("session_store_unavailable".into()),
        })
    }

    /// Deliver the settled entries that need a delivery, then record their receipts. A delivered
    /// receipt is written only after its terminal is durable.
    async fn deliver_settled(
        &self,
        module: &str,
        entries: &[LateEntry],
        rows: &[RegistrationRow],
        views: &mut HashMap<BindIdentity, LineageView>,
    ) -> Result<(), String> {
        for entry in entries {
            let key = entry.intake_key(module);
            if self.store.receipt(&key)?.is_some() {
                continue;
            }
            let Some(row) = rows.iter().find(|row| {
                broca_protocol::same_module(&row.call.module, module)
                    && row.call.call_key == entry.call_key
            }) else {
                continue;
            };
            let stored = views
                .get(&row.call.bind)
                .and_then(|view| match &view.source {
                    Source::Stored(stored) => Some(stored),
                    Source::Gone | Source::Unknown => None,
                });
            if let Some((tool_call_id, tool_name)) =
                stored.and_then(|stored| stored.yielded_call(&entry.call_key))
            {
                let call = YieldedCall {
                    tool_call_id,
                    tool_name,
                };
                let disposition = match self.delivery.notice(&row.call, &call, entry).await? {
                    NoticeOutcome::Delivered => Disposition::Delivered,
                    NoticeOutcome::Refused(reason) => {
                        tracing::warn!(
                            target: "late_results",
                            module,
                            call_key = entry.call_key,
                            "a handed-back call's result could not be delivered as a notice: {reason}"
                        );
                        Disposition::NoOp
                    }
                };
                if let Some(view) = views.get_mut(&row.call.bind) {
                    view.delivered.insert(entry.call_key.clone());
                }
                self.store.record_receipt(&key, &entry.kind, disposition)?;
                continue;
            }
            let held_call_id = match stored.and_then(|stored| stored.held_call_id(&entry.call_key))
            {
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
            // Later entries in this round share the snapshot. Remember the durable terminal
            // without re-reading the log, so a second event cannot deliver the same call twice.
            if let Some(view) = views.get_mut(&row.call.bind) {
                view.delivered.insert(entry.call_key.clone());
            }
            // The batch synced the call's terminal before `deliver` returned, so the log now
            // answers any re-read of this entry as already settled; the receipt only spares the
            // re-read until the ack lands.
            self.store
                .record_receipt(&key, &entry.kind, Disposition::Delivered)?;
        }
        Ok(())
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
        views: &mut HashMap<BindIdentity, LineageView>,
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
            .registrations_for(module)
            .map_err(FeedError::Retryable)?;
        self.deliver_settled(module, &reply.entries, &rows, views)
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
        match self.pull_round(module, &puller, &mut HashMap::new()).await {
            Ok(()) => Ok(()),
            Err(FeedError::CursorIncarnationChanged) => {
                self.store
                    .forget_cursor(module)
                    .map_err(FeedError::Retryable)?;
                self.pull_round(module, &puller, &mut HashMap::new()).await
            }
            Err(error) => Err(error),
        }
    }
}

/// One registered session's durable records, as the intake reads them.
pub(crate) struct LineageView {
    source: Source,
    /// Calls this round already delivered, so a second entry for one is never delivered again.
    delivered: HashSet<String>,
}

/// Where a registered session's facts come from.
enum Source {
    Stored(StoredLineage),
    /// The store holds no session at this address any more.
    Gone,
    /// The store isn't installed yet.
    Unknown,
}

fn log_retirement(call: &LateResultRegistration, reason: &str) {
    tracing::info!(
        target: "late_results",
        module = call.module,
        call_key = call.call_key,
        session_address = broca_core::session_addr(&call.bind),
        reason,
        "retired late-results registration"
    );
}

/// Settles an entry for an open held call without writing it into the run, and says why.
/// These outcomes are rare and each one discards a provider's answer, so each is logged: a
/// silent one hid an answer dropped from a later run of a session for 2.6 days. The line names
/// the reason only, never the entry's result.
fn settled_without_delivery(module: &str, entry: &LateEntry, reason: &str) -> Classified {
    tracing::info!(
        target: "late_results",
        module,
        call_key = entry.call_key,
        event_id = entry.event_id,
        "late-results entry settled without delivery: {reason}"
    );
    Classified::NoOp
}

fn scope_matches(registration: &LateResultRegistration, entry: &LateEntry) -> bool {
    match &registration.scope {
        Some(scope) => {
            broca_protocol::same_owner_string(&scope.owner.as_owner_string(), &entry.owner)
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

/// How far the rebuild scanned, for the operator's log and for the measurement test.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RebuildReport {
    /// Log files looked at.
    pub files: usize,
    /// Log files decoded, including logs with no held reply yet.
    pub decoded: usize,
    /// Session-store inventory entries examined.
    pub sessions: usize,
    /// Registrations written.
    pub registered: usize,
    /// WALs or session heads that could not be read or registered; any one keeps the fence tripped.
    pub unreadable: Vec<std::path::PathBuf>,
    /// The state root's log feature floor shows no provider-held frame was ever written here,
    /// so no log was looked at.
    pub skipped_by_floor: bool,
}

/// The watched `keys` of `bind`'s session whose calls are settled for good: answered inline,
/// answered after a hold with their batch committed, or left behind when their run ended. No
/// withdrawal can ever be owed for them, so withdrawal recovery retires their watches. `None`
/// when the store can't read the session, so nothing is retired on a guess.
pub(crate) async fn settled_watches(
    runtime: &crate::store_open::StoreRuntime,
    bind: &BindIdentity,
    keys: &[String],
) -> Option<Vec<SettledWatch>> {
    let lineage = StoredLineage::read(runtime, None, bind, keys.to_vec()).await?;
    if matches!(lineage, StoredLineage::Unreadable) {
        return None;
    }
    Some(
        keys.iter()
            .filter(|key| lineage.retirement_reason(key).is_some())
            .map(|key| SettledWatch {
                call_key: key.clone(),
                inline: lineage.answered_inline(key),
            })
            .collect(),
    )
}

/// A watched call that needs no withdrawal.
pub(crate) struct SettledWatch {
    pub call_key: String,
    /// Answered without a hold, so no late result can ever come for it: its registration is
    /// spent too. A held call keeps its registration, since its provider may still send the
    /// result, which intake settles.
    pub inline: bool,
}

#[cfg(test)]
mod tests;
