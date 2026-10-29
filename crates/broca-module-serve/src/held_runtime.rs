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
//!   feed. A store that is missing or unreadable is rebuilt from live and archived WALs, off the
//!   startup path; until that scan completes every pulled entry is Blocked and nothing is acked.
//!
//! The `held-reply/v1` route capability stays UNDECLARED in release builds, so no production
//! provider may hold a call. With held replies undeclared, no call carries a plain key and
//! nothing is registered. Result-only negotiation is independent. Debug builds may declare
//! held replies for tests (see [`declare_held_reply_for_tests`]).
//!
//! Known edge: a provider keeps at most 1,000 unacked results per session and drops the oldest
//! first (contract §7). A held call whose entry was dropped that way (for example while broca was
//! down long enough for more than 1,000 results to pile up) never receives an entry, and broca
//! puts no bound of its own on a hold, so the run stays paused. Cancelling or stopping the run is
//! its exit; planned daemon shutdown instead preserves the provider's obligation.

use std::{
    collections::{BTreeSet, HashMap},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use broca_module::held_slots::{EntryMapper, HeldRunResumer, HeldSlots, SlotDelivery};
use broca_module::held_withdrawals::{HeldWithdrawals, WithdrawTransport};
use broca_module::late_results_puller::{
    rebuild_store_from_wals_with_progress, FeedError, FeedReply, HeldDelivery, LateResultsFeed,
    LateResultsIntake, RebuildReport,
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

#[async_trait::async_trait]
impl WithdrawTransport for FeedRoutes {
    async fn withdraw(
        &self,
        call: &broca_core::tool_plane::LateResultRegistration,
    ) -> (
        cortexkit_role_tool_provider::withdraw::WithdrawVerdict,
        serde_json::Value,
    ) {
        broca_subc::late_results::withdraw_held_call(&self.conn, call).await
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
    withdrawals: Arc<HeldWithdrawals>,
    progress: Arc<Mutex<RebuildReport>>,
}

static HEALTH: std::sync::OnceLock<Mutex<std::sync::Weak<HeldRuntime>>> =
    std::sync::OnceLock::new();

/// Health never probes a file: boot and the rebuild worker publish the fence and progress.
pub fn health_gauges() -> serde_json::Value {
    let runtime = HEALTH
        .get()
        .and_then(|health| health.lock().ok()?.upgrade());
    runtime.map_or_else(
        || {
            health_snapshot(
                false,
                &RebuildReport::default(),
                serde_json::json!({"failed": 0, "pending": 0}),
            )
        },
        |runtime| {
            let progress = runtime.progress.lock().expect("rebuild progress");
            health_snapshot(
                runtime.intake.is_rebuilding(),
                &progress,
                runtime.withdrawals.health(),
            )
        },
    )
}

fn health_snapshot(
    rebuilding: bool,
    progress: &RebuildReport,
    withdrawals: serde_json::Value,
) -> serde_json::Value {
    serde_json::json!({
        "rebuild_required": rebuilding,
        "fence": if rebuilding { Some(broca_module::late_results_store::REBUILD_REQUIRED_MARKER) } else { None },
        "scanned": progress.files, "decoded": progress.decoded,
        "registered": progress.registered, "unreadable": progress.unreadable.len(),
        "withdrawals": withdrawals,
    })
}

impl HeldRuntime {
    /// Open (or recreate) the store under `state_root` and wire the intake to the provider
    /// feeds on `conn`, delivering through `resumer`.
    pub fn build(
        state_root: &Path,
        wal: Arc<broca_core::FileWalStore>,
        consumer_module: impl Into<String>,
        feed: Arc<dyn LateResultsFeed>,
        withdraw: Arc<dyn WithdrawTransport>,
        resumer: Arc<dyn HeldRunResumer>,
    ) -> Result<Arc<Self>, String> {
        let path = state_root.join(LATE_RESULTS_DB);
        let (store, needs_rebuild) = open_or_recreate(&path)?;
        let store = Arc::new(store);
        let slots = HeldSlots::new();
        let withdrawals = HeldWithdrawals::new(store.clone(), wal.clone(), withdraw);
        slots.with_withdrawals(withdrawals.clone());
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
            withdrawals,
            progress: Arc::new(Mutex::new(RebuildReport::default())),
        }))
    }

    pub fn publish_for_health(self: &Arc<Self>) {
        *HEALTH
            .get_or_init(Mutex::default)
            .lock()
            .expect("held runtime health") = Arc::downgrade(self);
    }

    /// Boot recovery, run off the startup path: rebuild a lost store from the live WALs, open
    /// every registered session so its open-time recovery runs, then start a puller for every
    /// module whose feed is still needed.
    pub async fn recover(self: Arc<Self>, registry: Arc<broca_module::registry::LineageRegistry>) {
        if self.needs_rebuild {
            loop {
                let store = Arc::clone(&self.store);
                let root = self.wal_root.clone();
                let progress = self.progress.clone();
                let started = std::time::Instant::now();
                let report = tokio::task::spawn_blocking(move || {
                    let report = rebuild_store_from_wals_with_progress(&store, &root, |report| {
                        *progress.lock().expect("rebuild progress") = report.clone();
                    })?;
                    if report.unreadable.is_empty() {
                        store.finish_rebuild()?;
                    }
                    Ok::<_, String>(report)
                })
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
        if let Err(error) = self.withdrawals.recover().await {
            tracing::warn!(target: "late_results", "provider-held withdrawal recovery failed: {error}");
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
    open_or_recreate_with_probe(path, |marker| marker.try_exists())
}

fn open_or_recreate_with_probe(
    path: &Path,
    probe: impl FnOnce(&Path) -> std::io::Result<bool>,
) -> Result<(LateResultsStore, bool), String> {
    let fenced = rebuild_fence(path, probe);
    let existed = path.is_file();
    match LateResultsStore::open(path) {
        Ok(store) if existed => Ok((store, fenced)),
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

fn rebuild_fence(path: &Path, probe: impl FnOnce(&Path) -> std::io::Result<bool>) -> bool {
    path.parent().is_none_or(|root| {
        probe(&root.join(broca_module::late_results_store::REBUILD_REQUIRED_MARKER)).unwrap_or(true)
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_names_the_fence_and_reports_scan_progress() {
        let report = RebuildReport {
            files: 7,
            decoded: 5,
            registered: 3,
            unreadable: vec![PathBuf::from("unreadable.wal")],
            skipped_by_floor: false,
        };
        let health = health_snapshot(
            true,
            &report,
            serde_json::json!({"failed": 1, "pending": 0}),
        );
        assert_eq!(health["fence"], "late-results-rebuild-required");
        assert_eq!(health["rebuild_required"], true);
        assert_eq!(health["scanned"], 7);
        assert_eq!(health["decoded"], 5);
        assert_eq!(health["registered"], 3);
        assert_eq!(health["unreadable"], 1);
        assert_eq!(health["withdrawals"]["failed"], 1);
        assert_eq!(
            health_snapshot(false, &report, serde_json::Value::Null)["fence"],
            serde_json::Value::Null
        );
    }

    #[test]
    fn rebuild_fence_survives_an_existing_database_restart() {
        let root = broca_test_support::scratch_root().join("held-runtime-fence-restart");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let path = root.as_path().join(LATE_RESULTS_DB);
        drop(LateResultsStore::open(&path).unwrap());
        std::fs::write(
            root.as_path()
                .join(broca_module::late_results_store::REBUILD_REQUIRED_MARKER),
            b"",
        )
        .unwrap();
        let (_, needs_rebuild) = open_or_recreate(&path).unwrap();
        assert!(
            needs_rebuild,
            "a committed database is not proof the full scan was published"
        );
    }

    #[test]
    fn unheld_existing_root_only_probes_the_fence_once_and_does_not_rebuild() {
        let root = broca_test_support::scratch_root().join("held-runtime-unheld-boot");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let path = root.as_path().join(LATE_RESULTS_DB);
        drop(LateResultsStore::open(&path).unwrap());
        let before = std::fs::read(&path).unwrap();
        let probes = std::cell::Cell::new(0);
        let (_, needs_rebuild) = open_or_recreate_with_probe(&path, |marker| {
            probes.set(probes.get() + 1);
            assert_eq!(
                marker,
                root.as_path()
                    .join(broca_module::late_results_store::REBUILD_REQUIRED_MARKER)
            );
            marker.try_exists()
        })
        .unwrap();
        assert_eq!(probes.get(), 1);
        assert!(
            !needs_rebuild,
            "boot must not scan any WAL for an existing unheld root"
        );
        assert_eq!(
            std::fs::read(&path).unwrap(),
            before,
            "boot adds no database writes"
        );
        assert!(!root
            .as_path()
            .join(broca_module::late_results_store::REBUILD_REQUIRED_MARKER)
            .exists());
        assert!(!root.as_path().join("late-results-registered").exists());
    }

    #[test]
    fn rebuild_fence_probe_error_is_not_absence() {
        assert!(rebuild_fence(Path::new("root/late-results.db"), |_| Err(
            std::io::Error::from(std::io::ErrorKind::PermissionDenied)
        )));
    }
}
