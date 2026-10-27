//! Installing the provider-held runtime in the daemon.
//!
//! A provider that declares `late_results` may answer a tool call with a `held` marker and
//! settle it later through its `late_results` feed. The entry it settles the call with IS the
//! call's result: broca never sends a held call to its provider again. This module wires the
//! pieces that make that safe across restarts:
//!
//! - **The waiting slots** ([`HeldSlots`]): a held call's batch waits there, live or after a
//!   resume, and the run's own batch writes the delivered result at the position the
//!   synchronous reply would have taken. The batch stays the only writer.
//! - **The intake** ([`LateResultsIntake`]): one puller per provider module, pulling under
//!   broca's own principal on one unscoped route (the custodian is the carrier, contract §7),
//!   deduplicating on `(provider, custodian, call_key, event_id)`, and acking only through a
//!   cursor the provider returned, after the delivered terminal is synced.
//! - **Boot recovery** ([`HeldRuntime::recover`]): every session with a registration is opened
//!   so its open-time recovery runs, then a puller starts for every module that still needs its
//!   feed. A store that is missing or unreadable is rebuilt from the live WALs first, off the
//!   startup path; until that scan completes every pulled entry is Blocked and nothing is acked.
//!
//! The `held-reply/v1` and `content/v1` route capabilities stay UNDECLARED in release builds:
//! a held call cannot be cancelled or stopped yet, so no production provider may hold one. With
//! the capabilities undeclared, no call carries a plain key and nothing is registered, so every
//! call keeps its exact wire bytes. Debug builds may declare them for tests (see
//! [`declare_held_reply_for_tests`]).
//!
//! Known edge: a provider keeps at most 1,000 unacked results per session and drops the oldest
//! first (contract §7). A held call whose entry was dropped that way (for example while broca was
//! down long enough for more than 1,000 results to pile up) never receives an entry, and broca
//! puts no bound of its own on a hold, so the run stays paused. Cancelling or stopping the run is
//! its exit; that close-out arrives with the cancel and stop support for held calls.

use std::{
    collections::{BTreeSet, HashMap},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use broca_module::held_slots::{EntryMapper, HeldRunResumer, HeldSlots, SlotDelivery};
use broca_module::late_results_puller::{
    rebuild_store_from_wals, FeedError, FeedReply, HeldDelivery, LateResultsFeed, LateResultsIntake,
};
use broca_module::late_results_store::LateResultsStore;
use broca_protocol::BindIdentity;
use cortexkit_role_tool_provider::late_results::Cursor;

/// The intake store's file name under the state root, kept apart from `run-index.db` the way the
/// other projections are.
pub const LATE_RESULTS_DB: &str = "late-results.db";

/// How many registered sessions boot recovery opens at once.
const RECOVERY_CONCURRENCY: usize = 8;

/// How long a rebuild that met an unreadable WAL waits before scanning again.
const REBUILD_RETRY: Duration = Duration::from_secs(30);

/// Debug builds only: the environment variable that makes this daemon's tool routes declare
/// `content/v1` and `held-reply/v1`, so a test provider may hold a call. Its name is a
/// non-shipping marker: the release fence refuses any artifact that contains it.
#[cfg(debug_assertions)]
pub const DECLARE_HELD_REPLY_ENV: &str = "BROCA_TEST_DECLARE_HELD_REPLY_V1";

/// Whether this (debug) process was asked to declare the held-reply capabilities on its tool
/// routes. Release builds never declare them and do not contain this check.
#[cfg(debug_assertions)]
pub fn declare_held_reply_for_tests() -> bool {
    std::env::var_os(DECLARE_HELD_REPLY_ENV).is_some_and(|value| value == "1")
}

/// One route per provider module, opened on the consumer-identity connection.
pub struct FeedRoutes {
    conn: Arc<broca_subc::SubcConnection>,
    identity: BindIdentity,
    feeds: Mutex<HashMap<String, Arc<broca_subc::LateResultsFeed>>>,
}

impl FeedRoutes {
    /// `identity` names each feed route; the custodian itself comes from the connection's
    /// consumer identity, so every module's feed shares this one bind.
    pub fn new(conn: Arc<broca_subc::SubcConnection>, identity: BindIdentity) -> Self {
        Self {
            conn,
            identity,
            feeds: Mutex::new(HashMap::new()),
        }
    }

    async fn feed(&self, module: &str) -> Result<Arc<broca_subc::LateResultsFeed>, FeedError> {
        if let Some(feed) = self.feeds.lock().unwrap().get(module).cloned() {
            return Ok(feed);
        }
        let feed =
            broca_subc::LateResultsFeed::open(Arc::clone(&self.conn), module, &self.identity)
                .await
                .map_err(feed_error)?;
        let feed = Arc::new(feed);
        self.feeds
            .lock()
            .unwrap()
            .insert(module.to_owned(), Arc::clone(&feed));
        Ok(feed)
    }

    /// Forget a module's route after a failure, so the next pull opens a fresh one.
    fn forget(&self, module: &str) {
        self.feeds.lock().unwrap().remove(module);
    }
}

#[async_trait::async_trait]
impl LateResultsFeed for FeedRoutes {
    async fn pull(
        &self,
        module: &str,
        since: Option<&Cursor>,
        limit: Option<u32>,
    ) -> Result<FeedReply, FeedError> {
        let feed = self.feed(module).await?;
        match feed.pull(since, limit).await {
            Ok(reply) => Ok(FeedReply {
                entries: reply.entries,
                cursor: reply.cursor,
                more: reply.more,
            }),
            Err(error) => {
                if !error.is_cursor_incarnation_changed() {
                    self.forget(module);
                }
                Err(feed_error(error))
            }
        }
    }

    async fn ack(&self, module: &str, through: &Cursor) -> Result<(), FeedError> {
        let feed = self.feed(module).await?;
        feed.ack(through).await.map_err(|error| {
            self.forget(module);
            feed_error(error)
        })
    }
}

fn feed_error(error: broca_subc::LateResultsError) -> FeedError {
    if error.is_cursor_incarnation_changed() {
        return FeedError::CursorIncarnationChanged;
    }
    if error.is_permanent() {
        // An unreadable answer will not improve by waiting: the puller backs off for good and
        // an ack-only feed is dropped, instead of retrying forever.
        return FeedError::Abandoned(error.to_string());
    }
    FeedError::Retryable(error.to_string())
}

/// The installed runtime: the slots the tool plane and the run wait in, the intake that pulls,
/// and the store both read.
pub struct HeldRuntime {
    pub slots: Arc<HeldSlots>,
    pub intake: Arc<LateResultsIntake>,
    pub store: Arc<LateResultsStore>,
    /// The state root whose live WAL trees a rebuild scans.
    wal_root: PathBuf,
    /// The store was missing or unreadable when this process opened it.
    needs_rebuild: bool,
}

impl HeldRuntime {
    /// Open (or recreate) the store under `state_root` and wire the intake to the provider
    /// feeds on `conn`, delivering through `resumer`.
    pub fn build(
        state_root: &Path,
        wal: Arc<broca_core::FileWalStore>,
        consumer_module: impl Into<String>,
        feed: Arc<dyn LateResultsFeed>,
        resumer: Arc<dyn HeldRunResumer>,
    ) -> Result<Arc<Self>, String> {
        let path = state_root.join(LATE_RESULTS_DB);
        let (store, needs_rebuild) = open_or_recreate(&path)?;
        let store = Arc::new(store);
        let slots = HeldSlots::new();
        let mapper: Arc<EntryMapper> = Arc::new(broca_subc::late_entry_tool_result);
        let delivery: Arc<dyn HeldDelivery> =
            Arc::new(SlotDelivery::new(Arc::clone(&slots), resumer, mapper));
        // The live WAL trees (`wal`, `wal-ephemeral`, `wal-durable`) are siblings under the
        // state root, which is the parent of the ordinary tree the store was built on.
        let wal_root = wal
            .root()
            .parent()
            .map_or_else(|| state_root.to_path_buf(), Path::to_path_buf);
        let intake =
            LateResultsIntake::new(Arc::clone(&store), wal, consumer_module, delivery, feed);
        // Until the rebuild below completes, no missing registration proves anything.
        intake.set_rebuilding(needs_rebuild);
        // A call that is held in a live batch joins its module's puller as soon as its held
        // record is synced.
        let weak = Arc::downgrade(&intake);
        slots.on_hold(move |module| {
            if let Some(intake) = weak.upgrade() {
                if let Err(error) = intake.ensure_puller(module) {
                    tracing::warn!(target: "late_results", module, "late-results puller did not start: {error}");
                }
            }
        });
        Ok(Arc::new(Self {
            slots,
            intake,
            store,
            wal_root,
            needs_rebuild,
        }))
    }

    /// Boot recovery, run off the startup path: rebuild a lost store from the live WALs, open
    /// every registered session so its open-time recovery runs, then start a puller for every
    /// module whose feed is still needed.
    pub async fn recover(self: Arc<Self>, registry: Arc<broca_module::registry::LineageRegistry>) {
        if self.needs_rebuild {
            loop {
                let store = Arc::clone(&self.store);
                let root = self.wal_root.clone();
                let started = std::time::Instant::now();
                let report =
                    tokio::task::spawn_blocking(move || rebuild_store_from_wals(&store, &root))
                        .await
                        .map_err(|error| error.to_string())
                        .and_then(|report| report);
                match report {
                    Ok(report) if report.unreadable.is_empty() => {
                        tracing::info!(
                            target: "late_results",
                            files = report.files,
                            decoded = report.decoded,
                            registered = report.registered,
                            skipped_by_floor = report.skipped_by_floor,
                            elapsed_ms = started.elapsed().as_millis() as u64,
                            "late-results store rebuilt from the live WALs"
                        );
                        break;
                    }
                    Ok(report) => tracing::warn!(
                        target: "late_results",
                        unreadable = report.unreadable.len(),
                        "late-results rebuild met unreadable WALs; entries stay blocked until a rescan completes"
                    ),
                    Err(error) => tracing::warn!(
                        target: "late_results",
                        "late-results rebuild failed; entries stay blocked: {error}"
                    ),
                }
                tokio::time::sleep(REBUILD_RETRY).await;
            }
            self.intake.set_rebuilding(false);
        }
        let sessions: Vec<BindIdentity> = match self.store.registrations() {
            Ok(calls) => {
                let mut sessions: Vec<BindIdentity> = Vec::new();
                for call in calls {
                    if !sessions.contains(&call.bind) {
                        sessions.push(call.bind);
                    }
                }
                sessions
            }
            Err(error) => {
                tracing::warn!(target: "late_results", "late-results recovery could not read registrations: {error}");
                Vec::new()
            }
        };
        let (opened, failed) = registry
            .open_registered_sessions(sessions, RECOVERY_CONCURRENCY)
            .await;
        for (session, error) in &failed {
            tracing::warn!(target: "late_results", session = %session.session, "late-results recovery could not open a registered session: {error}");
        }
        if opened > 0 || !failed.is_empty() {
            tracing::info!(target: "late_results", opened, failed = failed.len(), "late-results recovery opened registered sessions");
        }
        for module in self.modules() {
            if let Err(error) = self.intake.ensure_puller(&module) {
                tracing::warn!(target: "late_results", module, "late-results puller did not start: {error}");
            }
        }
    }

    /// Every module the store names, by a registration or a feed row.
    fn modules(&self) -> BTreeSet<String> {
        let mut modules = BTreeSet::new();
        if let Ok(feeds) = self.store.feeds() {
            modules.extend(feeds.into_iter().map(|feed| feed.module));
        }
        if let Ok(calls) = self.store.registrations() {
            modules.extend(calls.into_iter().map(|call| call.module));
        }
        modules
    }
}

/// Open the store at `path`. A missing file, or one SQLite cannot open, is replaced by a fresh
/// store that must be rebuilt from the WALs: the second value says so. An unreadable file is
/// kept beside it for inspection rather than deleted.
fn open_or_recreate(path: &Path) -> Result<(LateResultsStore, bool), String> {
    let existed = path.is_file();
    match LateResultsStore::open(path) {
        Ok(store) if existed => Ok((store, false)),
        Ok(store) => Ok((store, true)),
        Err(error) => {
            let aside = path.with_extension(format!(
                "db.unreadable-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|elapsed| elapsed.as_millis())
                    .unwrap_or(0)
            ));
            tracing::warn!(
                target: "late_results",
                "late-results store unreadable ({error}); keeping it as {} and rebuilding",
                aside.display()
            );
            std::fs::rename(path, &aside).map_err(|error| error.to_string())?;
            Ok((LateResultsStore::open(path)?, true))
        }
    }
}

/// The resumer the daemon delivers through: the registry, which opens the session's actor. The
/// registry is built after the tool plane that needs the held runtime, so it is bound once it
/// exists; a delivery before then fails and is retried on a later pull.
#[derive(Default)]
pub struct LateBoundResumer(std::sync::OnceLock<Arc<broca_module::registry::LineageRegistry>>);

impl LateBoundResumer {
    pub fn bind(&self, registry: Arc<broca_module::registry::LineageRegistry>) {
        let _ = self.0.set(registry);
    }
}

#[async_trait::async_trait]
impl HeldRunResumer for LateBoundResumer {
    async fn resume_for_late_result(&self, session: &BindIdentity) -> Result<(), String> {
        match self.0.get() {
            Some(registry) => registry.resume_held_run(session).await,
            None => Err("the session registry is not running yet".into()),
        }
    }
}
