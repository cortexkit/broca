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
//!   feed. A missing or unreadable intake database is rebuilt from WALs and new-storage sessions, off the
//!   startup path; until that scan completes every pulled entry is Blocked and nothing is acked.
//!
//! Installing this runtime declares `tool-result/v1` and `held-reply/v1` on dispatch routes in
//! every build. Plain-call keys and registrations still require a provider's frozen
//! `late_results` declaration; role and plan-fetch routes do not negotiate held replies.
//!
//! Known edge: a provider keeps at most 1,000 unacked results per session and drops the oldest
//! first (contract §7). A held call whose entry was dropped that way (for example while broca was
//! down long enough for more than 1,000 results to pile up) never receives an entry, and broca
//! puts no bound of its own on a hold, so the run stays paused. Cancelling or stopping the run is
//! its exit; planned daemon shutdown instead preserves the provider's obligation.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use broca_module::held_slots::{EntryMapper, HeldRunResumer, HeldSlots, SlotDelivery};
use broca_module::held_withdrawals::{HeldWithdrawals, WithdrawTransport};
use broca_module::late_results_puller::{
    rebuild_store_from_sessions_with_progress, FeedError, FeedReply, HeldDelivery, LateResultsFeed,
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

/// How long a rebuild that met unreadable session storage waits before scanning again.
const REBUILD_RETRY: Duration = Duration::from_secs(30);

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

    /// Forget a module's route `failed` after a failure, so the next pull opens a fresh one, and
    /// close it: a refused or unreadable answer leaves the route open on the daemon. A route
    /// another caller already replaced is left to that caller.
    fn forget(&self, module: &str, failed: &Arc<broca_subc::LateResultsFeed>) {
        let removed = {
            let mut feeds = self.feeds.lock().unwrap();
            match feeds.get(module) {
                Some(cached) if Arc::ptr_eq(cached, failed) => feeds.remove(module),
                _ => None,
            }
        };
        if let Some(feed) = removed {
            tokio::spawn(async move {
                let _ = feed.close().await;
            });
        }
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
                    self.forget(module, &feed);
                }
                Err(feed_error(error))
            }
        }
    }

    async fn ack(&self, module: &str, through: &Cursor) -> Result<(), FeedError> {
        let feed = self.feed(module).await?;
        feed.ack(through).await.map_err(|error| {
            self.forget(module, &feed);
            feed_error(error)
        })
    }
}

#[async_trait::async_trait]
impl WithdrawTransport for FeedRoutes {
    async fn withdraw(
        &self,
        call: &broca_core::tool_plane::LateResultRegistration,
        session_identity: bool,
    ) -> (
        cortexkit_role_tool_provider::withdraw::WithdrawVerdict,
        serde_json::Value,
    ) {
        broca_subc::late_results::withdraw_held_call(&self.conn, call, session_identity).await
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
    /// The state root whose session storage a rebuild scans.
    state_root: PathBuf,
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
            // Same members as `HeldWithdrawals::health`, so a reader sees one shape whether or
            // not the held runtime has registered yet.
            health_snapshot(
                false,
                &RebuildReport::default(),
                serde_json::json!({
                    "failed": 0,
                    "pending": 0,
                    "scope_gone": 0,
                    "sweep_running": false,
                    "rows_remaining": 0,
                }),
                0,
                Vec::new(),
            )
        },
        |runtime| {
            let progress = runtime.progress.lock().expect("rebuild progress");
            health_snapshot(
                runtime.intake.is_rebuilding(),
                &progress,
                runtime.withdrawals.health(),
                runtime.intake.overdue_pins(),
                runtime.intake.stuck_deliveries(),
            )
        },
    )
}

fn health_snapshot(
    rebuilding: bool,
    progress: &RebuildReport,
    withdrawals: serde_json::Value,
    overdue_pins: usize,
    stuck_deliveries: Vec<serde_json::Value>,
) -> serde_json::Value {
    serde_json::json!({
        "rebuild_required": rebuilding,
        "fence": if rebuilding { Some(broca_module::late_results_store::REBUILD_REQUIRED_MARKER) } else { None },
        "scanned": progress.files, "decoded": progress.decoded,
        "registered": progress.registered, "unreadable": progress.unreadable.len(),
        "withdrawals": withdrawals,
        "overdue_pins": overdue_pins,
        "late_results_delivery_stuck": stuck_deliveries,
    })
}

impl HeldRuntime {
    /// Open (or recreate) the store under `state_root` and wire the intake to the provider
    /// feeds on `conn`, delivering through `resumer`.
    pub fn build(
        state_root: &Path,
        consumer_module: impl Into<String>,
        feed: Arc<dyn LateResultsFeed>,
        withdraw: Arc<dyn WithdrawTransport>,
        resumer: Arc<dyn HeldRunResumer>,
    ) -> Result<Arc<Self>, String> {
        let path = state_root.join(LATE_RESULTS_DB);
        let (store, needs_rebuild) = open_or_recreate(&path)?;
        let store = Arc::new(store);
        let slots = HeldSlots::new();
        let withdrawals = HeldWithdrawals::new(store.clone(), withdraw);
        slots.with_withdrawals(withdrawals.clone());
        let mapper: Arc<EntryMapper> = Arc::new(broca_subc::late_entry_tool_result);
        let delivery: Arc<dyn HeldDelivery> =
            Arc::new(SlotDelivery::new(Arc::clone(&slots), resumer, mapper));
        let intake = LateResultsIntake::new(Arc::clone(&store), consumer_module, delivery, feed);
        // Until the rebuild below completes, no missing registration proves anything.
        intake.set_rebuilding(needs_rebuild);
        // A call that is held in a live batch joins its module's puller as soon as its held
        // record is synced.
        let weak = Arc::downgrade(&intake);
        slots.on_hold(move |module| {
            if let Some(intake) = weak.upgrade() {
                let module = module.to_owned();
                tokio::spawn(async move {
                    if let Err(error) = intake.ensure_puller(&module).await {
                        tracing::warn!(target: "late_results", module, "late-results puller did not start: {error}");
                    }
                });
            }
        });
        Ok(Arc::new(Self {
            slots,
            intake,
            store,
            state_root: state_root.to_path_buf(),
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

    pub fn bind_stopping_signal(&self, stopping: tokio_util::sync::CancellationToken) {
        self.withdrawals.bind_stopping_signal(stopping);
    }

    /// Boot recovery, run off the startup path: rebuild a lost intake from both session storages, open
    /// every registered session so its open-time recovery runs, then start a puller for every
    /// module whose feed is still needed.
    pub async fn recover(self: Arc<Self>, registry: Arc<broca_module::registry::LineageRegistry>) {
        if let Some(runtime) = registry.store_runtime() {
            // Withdrawals of cancelled held calls read new-storage sessions the same way intake
            // classifies their late results.
            self.withdrawals.with_store_runtime(runtime.clone());
            self.intake.set_session_store(runtime);
        }
        let stopping = registry.stopping_signal();
        self.bind_stopping_signal(stopping.clone());
        if stopping.is_cancelled() {
            return;
        }
        if self.needs_rebuild {
            loop {
                let started = std::time::Instant::now();
                let scan = async {
                    let mut report = RebuildReport::default();
                    let runtime = registry.store_runtime();
                    if let Some(runtime) = &runtime {
                        self.intake.set_session_store(runtime.clone());
                        self.withdrawals.with_store_runtime(runtime.clone());
                    }
                    rebuild_store_from_sessions_with_progress(
                        &self.store,
                        &self.state_root,
                        runtime.as_deref(),
                        &mut report,
                        |report| {
                            *self.progress.lock().expect("rebuild progress") = report.clone();
                        },
                    )
                    .await?;
                    if stopping.is_cancelled() {
                        return Err("late-results recovery is stopping".into());
                    }
                    if report.unreadable.is_empty() {
                        self.store.finish_rebuild()?;
                    }
                    Ok::<_, String>(report)
                };
                let report = tokio::select! {
                    biased;
                    _ = stopping.cancelled() => return,
                    report = scan => report,
                };
                match report {
                    Ok(report) if report.unreadable.is_empty() => {
                        tracing::info!(
                            target: "late_results",
                            files = report.files,
                            decoded = report.decoded,
                            sessions = report.sessions,
                            registered = report.registered,
                            skipped_by_floor = report.skipped_by_floor,
                            elapsed_ms = started.elapsed().as_millis() as u64,
                            "late-results intake rebuilt from session storage"
                        );
                        break;
                    }
                    Ok(report) => tracing::warn!(
                        target: "late_results",
                        unreadable = report.unreadable.len(),
                        "late-results rebuild met unreadable session storage; entries stay blocked until a rescan completes"
                    ),
                    Err(error) => tracing::warn!(
                        target: "late_results",
                        "late-results rebuild failed; entries stay blocked: {error}"
                    ),
                }
                tokio::select! {
                    biased;
                    _ = stopping.cancelled() => return,
                    _ = tokio::time::sleep(REBUILD_RETRY) => {}
                }
            }
            self.intake.set_rebuilding(false);
        }
        let (calls, modules) = match self.store.recovery_snapshot() {
            Ok(snapshot) => snapshot,
            Err(error) => {
                tracing::warn!(target: "late_results", "late-results recovery could not read registrations/feeds: {error}");
                return;
            }
        };
        let sessions: Vec<BindIdentity> = {
            let mut sessions: Vec<BindIdentity> = Vec::new();
            for call in calls {
                if !sessions.contains(&call.bind) {
                    sessions.push(call.bind);
                }
            }
            sessions
        };
        let (opened, failed) = tokio::select! {
            biased;
            _ = stopping.cancelled() => return,
            opened = registry.open_registered_sessions(sessions, RECOVERY_CONCURRENCY) => opened,
        };
        for (session, error) in &failed {
            tracing::warn!(target: "late_results", session = %session.session, "late-results recovery could not open a registered session: {error}");
        }
        if opened > 0 || !failed.is_empty() {
            tracing::info!(target: "late_results", opened, failed = failed.len(), "late-results recovery opened registered sessions");
        }
        self.recover_feeds(modules.into_iter().collect(), stopping.clone())
            .await;
        // Old inline registrations may belong to a provider with no held calls and no puller.
        // Sweep a bounded page off the recovery path, rather than scanning every WAL at boot.
        loop {
            tokio::select! {
                biased;
                _ = stopping.cancelled() => return,
                _ = tokio::time::sleep(broca_module::late_results_puller::PULL_INTERVAL) => {}
            }
            match self.intake.sweep_registrations().await {
                Ok(settled) if settled > 0 => {
                    tracing::info!(target: "late_results", settled, "retired settled late-results registrations")
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(target: "late_results", "late-results registration sweep failed: {error}")
                }
            }
        }
    }

    async fn recover_feeds(
        &self,
        modules: Vec<String>,
        stopping: tokio_util::sync::CancellationToken,
    ) {
        for module in modules {
            if stopping.is_cancelled() {
                return;
            }
            if let Err(error) = self.intake.ensure_puller(&module).await {
                tracing::warn!(target: "late_results", module, "late-results puller did not start: {error}");
            }
        }
        // Withdrawal retries and old watching rows must never stand between a paused worker
        // and its late result. The shared stop signal cancels this independent recovery task.
        let withdrawals = self.withdrawals.clone();
        tokio::spawn(async move {
            if let Err(error) = withdrawals.recover().await {
                tracing::warn!(target: "late_results", "provider-held withdrawal recovery failed: {error}");
            }
        });
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

    /// Sent as the provider's own notice would be, inside broca: it rides the next tool result
    /// while a call is open, else it arrives as a steer marked `notice`. On an idle session it
    /// starts a run when the plan lets the provider start runs, and otherwise waits for the
    /// session's next run. One send id per call, so a redelivered entry is the same send.
    async fn deliver_late_notice(
        &self,
        session: &BindIdentity,
        notice: broca_module::held_slots::LateNotice,
    ) -> Result<broca_module::late_results_puller::NoticeOutcome, String> {
        use broca_module::late_results_puller::NoticeOutcome;
        use broca_module::ServeHandler;
        let Some(registry) = self.0.get() else {
            return Err("the session registry is not running yet".into());
        };
        let send_id = broca_module::late_results_puller::notice_send_id(&notice.call_key);
        let run_id = {
            let seed = format!("{}\u{1f}{send_id}", broca_core::session_addr(session));
            broca_protocol::RunId(format!(
                "run-notice-{}",
                broca_provider::hash::base62(broca_provider::hash::fnv1a128(&seed))
            ))
        };
        let mut refused = None;
        for modes in [
            &["tool_result", "steer", "idle_start"][..],
            &["tool_result", "steer"],
        ] {
            let send = late_notice_send(session, &run_id, &send_id, &notice, modes)?;
            match registry.send(send).await {
                Ok(_) => return Ok(NoticeOutcome::Delivered),
                Err(error) if notice_retryable(&error) => return Err(error.to_string()),
                Err(error) => refused = Some(error.to_string()),
            }
        }
        Ok(NoticeOutcome::Refused(
            refused.unwrap_or_else(|| "refused".into()),
        ))
    }
}

/// Builds the send that delivers a handed-back call's late result as a notice, tried with the
/// delivery `modes`. Its sender is the call's provider (`notice.carrier`), over the internal
/// scope. It names no model or other run setting, so the session supplies every one
/// (`InheritedParam::ALL`).
fn late_notice_send(
    session: &BindIdentity,
    run_id: &broca_protocol::RunId,
    send_id: &str,
    notice: &broca_module::held_slots::LateNotice,
    modes: &[&str],
) -> Result<broca_module::serve::SendRequest, String> {
    let mut params: broca_wire::SendParams = serde_json::from_value(serde_json::json!({
        "prompt": notice.text,
        "send_id": send_id,
        "delivery": modes,
    }))
    .map_err(|error| format!("the notice's send doesn't decode: {error}"))?;
    // An empty placeholder, as the serve adapter fills an omitted model: the session's frozen
    // model replaces it before the send is queued or admitted (`InheritedParam::Model`).
    params.model = Some(crate::wire::ModelParams {
        provider: String::new(),
        model: String::new(),
        variant: None,
    });
    let mut send =
        crate::wire::SendParamsExt::into_send_request(params, session.clone(), run_id.clone())
            .map_err(|error| format!("the notice's send is malformed: {error}"))?;
    send.request.inherit_frozen = broca_protocol::request::InheritedParam::ALL
        .iter()
        .map(|(param, _)| *param)
        .collect();
    send.request.message_marks = [("notice".to_owned(), serde_json::json!(true))]
        .into_iter()
        .collect();
    send.request.principal = Some(broca_protocol::BindPrincipal::Reserved {
        module_id: notice.carrier.clone(),
    });
    send.request.scope = broca_protocol::SendScope::Internal;
    Ok(send)
}

/// Whether a refused notice may be sent again unchanged later: the session or broca was busy,
/// not the send wrong.
fn notice_retryable(error: &broca_module::ServeError) -> bool {
    use broca_module::ServeError;
    matches!(
        error,
        ServeError::Transient(_)
            | ServeError::Open(_, Some(broca_protocol::ErrorClass::Transient))
            | ServeError::ModuleStopping(_)
            | ServeError::SessionHeld(_)
            | ServeError::SessionMoving { .. }
            | ServeError::CredentialsUnavailable
    )
}

#[cfg(test)]
#[path = "held_runtime_tests.rs"]
mod recovery_tests;

#[cfg(test)]
mod tests {
    use super::*;

    /// The notice send for a late result builds for both delivery lists broca tries. It names no
    /// model: the model and every other run setting come from the session. Its sender is the
    /// call's provider, over the internal scope.
    #[test]
    fn a_late_notice_send_builds_and_takes_every_parameter_from_the_session() {
        let session = BindIdentity::new("/p", "h", "s");
        let notice = broca_module::held_slots::LateNotice {
            carrier: "tools".into(),
            call_key: "lineage:1".into(),
            text: "the call finished".into(),
        };
        for modes in [
            &["tool_result", "steer", "idle_start"][..],
            &["tool_result", "steer"],
        ] {
            let send = late_notice_send(
                &session,
                &broca_protocol::RunId("run-notice-1".into()),
                "notice-1",
                &notice,
                modes,
            )
            .unwrap();
            assert_eq!(send.request.model, broca_protocol::ModelHandle::new("", ""));
            assert!(send
                .request
                .inherit_frozen
                .contains(&broca_protocol::request::InheritedParam::Model));
            assert_eq!(
                send.request.inherit_frozen.len(),
                broca_protocol::request::InheritedParam::ALL.len()
            );
            assert_eq!(
                send.request.message_marks["notice"],
                serde_json::json!(true)
            );
            assert_eq!(
                send.request.principal,
                Some(broca_protocol::BindPrincipal::Reserved {
                    module_id: "tools".into()
                })
            );
            assert_eq!(send.request.scope, broca_protocol::SendScope::Internal);
        }
    }

    #[test]
    fn review_regression_activation_docs_do_not_promise_exactly_once_withdrawal_packets() {
        let contract = include_str!("../../../docs/module-surface.md");
        assert!(
            !contract.contains("because a held call cannot be\ncancelled or stopped yet"),
            "withdrawal support is no longer the reason release negotiation is disabled"
        );
        assert!(contract.contains("at-most-once dispatch"));
        assert!(contract.contains("already-durably-cancelled"));
    }

    #[test]
    fn health_names_the_fence_and_reports_scan_progress() {
        let report = RebuildReport {
            files: 7,
            decoded: 5,
            sessions: 0,
            registered: 3,
            unreadable: vec![PathBuf::from("unreadable.wal")],
            skipped_by_floor: false,
        };
        let health = health_snapshot(
            true,
            &report,
            serde_json::json!({"failed": 1, "pending": 0}),
            2,
            vec![serde_json::json!({"attempts":3})],
        );
        assert_eq!(health["fence"], "late-results-rebuild-required");
        assert_eq!(health["rebuild_required"], true);
        assert_eq!(health["scanned"], 7);
        assert_eq!(health["decoded"], 5);
        assert_eq!(health["registered"], 3);
        assert_eq!(health["unreadable"], 1);
        assert_eq!(health["withdrawals"]["failed"], 1);
        assert_eq!(health["overdue_pins"], 2);
        assert_eq!(health["late_results_delivery_stuck"][0]["attempts"], 3);
        assert_eq!(
            health_snapshot(false, &report, serde_json::Value::Null, 0, Vec::new())["fence"],
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
