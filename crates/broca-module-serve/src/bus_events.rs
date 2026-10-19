//! WAL-to-outbox projection and the asynchronous ck-bus claim-check publisher.

use async_trait::async_trait;
use broca_core::{FileWalStore, WalStore};
use broca_module::{
    serve::{ServeHandler, StatusResponse},
    LineageRegistry,
};
use broca_protocol::{
    run_events::{RunEvent, RunEventDetail},
    BindIdentity,
};
use broca_store::{
    bus_events::{BusPublisher, DrainResult, Metrics},
    SqliteStore,
};
use cortexkit_bus_naming::AccountNames;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;

static METRICS: OnceLock<Arc<Metrics>> = OnceLock::new();

/// Whether this process started the publisher. With the switch off every counter reads zero,
/// which is indistinguishable from an enabled publisher that has had nothing to send, so the
/// health block states which of the two an operator is looking at.
static ENABLED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Whether the enabled publisher is actually doing its work, as opposed to merely running.
///
/// The counters in [`Metrics`] only move once events reach the outbox, so a publisher that is
/// stuck before that point reads exactly like an idle one: every number zero while runs flow.
/// These readings separate the stages. Durable barriers seen with no session projected means
/// projection is not keeping up; a pending recovery count says how much history a walk still
/// has to visit; a loop age that keeps growing means one pass of the loop is stuck.
struct Liveness {
    /// Durable WAL barriers the publisher was told about.
    hints_seen: AtomicU64,
    /// Session projections into the outbox that completed, live and recovered.
    sessions_projected: AtomicU64,
    /// Whether the machine account is known, which is what projection waits for.
    account_known: std::sync::atomic::AtomicBool,
    /// Sessions the current recovery walk has yet to visit.
    recovery_pending: AtomicU64,
    /// Recovery walks that visited every session they listed.
    recovery_walks: AtomicU64,
    /// When the loop last began a pass, on `broca_diagnostics`' monotonic clock; zero before the
    /// first pass.
    pass_started_ms: AtomicU64,
}

impl Liveness {
    const fn new() -> Self {
        Self {
            hints_seen: AtomicU64::new(0),
            sessions_projected: AtomicU64::new(0),
            account_known: std::sync::atomic::AtomicBool::new(false),
            recovery_pending: AtomicU64::new(0),
            recovery_walks: AtomicU64::new(0),
            pass_started_ms: AtomicU64::new(0),
        }
    }

    fn pass_started(&self) {
        self.pass_started_ms
            .store(broca_diagnostics::now_ms(), Ordering::Relaxed);
    }
}

static LIVENESS: Liveness = Liveness::new();

pub fn health() -> serde_json::Value {
    let mut snapshot = METRICS
        .get()
        .map(|m| m.snapshot())
        .unwrap_or_else(|| Metrics::default().snapshot());
    if let Some(fields) = snapshot.as_object_mut() {
        fields.insert(
            "enabled".into(),
            serde_json::Value::Bool(ENABLED.load(Ordering::Relaxed)),
        );
        let count = |value: &AtomicU64| serde_json::Value::from(value.load(Ordering::Relaxed));
        fields.insert("bus_hints_seen".into(), count(&LIVENESS.hints_seen));
        fields.insert(
            "bus_sessions_projected".into(),
            count(&LIVENESS.sessions_projected),
        );
        fields.insert(
            "bus_account_known".into(),
            serde_json::Value::Bool(LIVENESS.account_known.load(Ordering::Relaxed)),
        );
        fields.insert(
            "bus_recovery_pending".into(),
            count(&LIVENESS.recovery_pending),
        );
        fields.insert("bus_recovery_walks".into(), count(&LIVENESS.recovery_walks));
        // Milliseconds since the loop began its latest pass. A running loop begins one every
        // quarter second, so a value that keeps growing means a pass is stuck. Null when no
        // loop has started, which is always the case with `bus_run_events_enabled` off.
        let started = LIVENESS.pass_started_ms.load(Ordering::Relaxed);
        fields.insert(
            "bus_loop_pass_age_ms".into(),
            (started != 0)
                .then(|| broca_diagnostics::now_ms().saturating_sub(started))
                .into(),
        );
    }
    snapshot
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Config {
    pub enabled: bool,
}

impl Config {
    pub fn load_default_path() -> Result<Self, String> {
        match crate::config_home::config_file(crate::cache_routing::CONFIG_PATH_ENV, "broca.jsonc")
        {
            crate::config_home::ConfigHome::Unresolvable => Ok(Self::default()),
            crate::config_home::ConfigHome::Resolved { path, .. } => {
                match std::fs::read_to_string(&path) {
                    Ok(text) => Self::parse(&text)
                        .map_err(|e| format!("bus events config {}: {e}", path.display())),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
                    Err(e) => Err(format!("read bus events config {}: {e}", path.display())),
                }
            }
        }
    }
    fn parse(text: &str) -> Result<Self, String> {
        let text = crate::cache_routing::strip_trailing_commas(
            &crate::cache_routing::strip_jsonc_comments(text),
        );
        let config: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(&text).map_err(|e| e.to_string())?;
        match config.get("bus_run_events_enabled") {
            None => Ok(Self::default()),
            Some(value) => value
                .as_bool()
                .map(|enabled| Self { enabled })
                .ok_or_else(|| "bus_run_events_enabled must be a boolean".into()),
        }
    }
}

/// Hints contain no record bodies. Overflow and process loss both request WAL recovery.
#[derive(Default)]
pub struct Hints(Mutex<HintState>);
#[derive(Default)]
struct HintState {
    heads: DurableHeads,
    recovery: bool,
}
type DurableHeads = HashMap<BindIdentity, (u64, String)>;
impl Hints {
    pub fn observer(self: &Arc<Self>) -> Arc<broca_core::wal_writer::DurableWalObserver> {
        let hints = self.clone();
        Arc::new(move |session, seq, epoch| {
            LIVENESS.hints_seen.fetch_add(1, Ordering::Relaxed);
            let Some(epoch) = epoch else {
                return;
            };
            let mut state = hints.0.lock().unwrap();
            if state.heads.len() >= 4096 && !state.heads.contains_key(session) {
                state.recovery = true;
            } else {
                state
                    .heads
                    .insert(session.clone(), (seq, epoch.to_string()));
            }
        })
    }
    fn take(&self) -> (DurableHeads, bool) {
        let mut state = self.0.lock().unwrap();
        (
            std::mem::take(&mut state.heads),
            std::mem::take(&mut state.recovery),
        )
    }
}

/// A transport connection with its machine account and next credential refresh deadline.
pub struct BusConnection {
    pub publisher: Arc<dyn BusPublisher>,
    pub account: AccountNames,
    pub refresh_at_ms: u64,
}

#[async_trait]
pub trait BusConnector: Send + Sync {
    async fn connect(&self) -> Result<BusConnection, String>;
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

/// What one recovery visit found for a session.
#[derive(Debug, PartialEq, Eq)]
enum Visit {
    /// The session's durable records are projected through `head`. `projected` is false when
    /// the projection already stood at that head, so nothing was read again or written.
    Current { head: u64, projected: bool },
    /// No live or archived WAL, or one without a lineage identity yet: nothing to project.
    Absent,
}

/// Why a recovery visit could not finish. Either way the walk is repeated later.
#[derive(Debug)]
enum VisitError {
    /// The WAL could not be read or proven durable.
    Unreadable,
    /// The WAL was read but its events could not be projected into the outbox.
    Projection(String),
}

/// Bring one session's outbox projection up to its durable WAL head after process loss or
/// hint overflow.
///
/// The head is the last sequence decoded BEFORE the read-side flush: the flush proves every
/// frame written before it, so a frame appended after the read cannot ride on that proof.
/// `flush_for_read` returns the WAL file's identity (an inode number on Unix, always `None`
/// elsewhere), never a sequence, so its value is deliberately ignored here. A session whose
/// projection already stands at the head needs no flush: it has nothing left to publish.
fn recover_session(
    store: &SqliteStore,
    wal: &FileWalStore,
    session: &BindIdentity,
    now: u64,
) -> Result<Visit, VisitError> {
    let missing = |error: &broca_wal::WalError| match error {
        broca_wal::WalError::Io(error) => error.kind() == std::io::ErrorKind::NotFound,
        _ => false,
    };
    let candidate = match wal.read_records_range(session, 0, None, None) {
        Ok(range) => range,
        Err(error) if missing(&error) => return Ok(Visit::Absent),
        Err(_) => return Err(VisitError::Unreadable),
    };
    let epoch = candidate.lineage_id.map(|id| id.to_string());
    let records = match candidate.into_records() {
        broca_wal::WalRangeRecords::Complete(records) => records,
        broca_wal::WalRangeRecords::Absent => return Ok(Visit::Absent),
        _ => return Err(VisitError::Unreadable),
    };
    let Some(epoch) = epoch else {
        return Ok(Visit::Absent);
    };
    let head = records.last().map_or(0, |record| record.seq);
    let position = store
        .bus_event_position(session, &epoch)
        .map_err(|e| VisitError::Projection(e.to_string()))?;
    if position >= head {
        return Ok(Visit::Current {
            head,
            projected: false,
        });
    }
    match wal.flush_for_read(session) {
        Ok(_identity) => {}
        Err(error) if missing(&error) => return Ok(Visit::Absent),
        Err(_) => return Err(VisitError::Unreadable),
    }
    store
        .project_bus_events(session, &epoch, &records, now)
        .map_err(|e| VisitError::Projection(e.to_string()))?;
    Ok(Visit::Current {
        head,
        projected: true,
    })
}

/// The outcome of one bounded slice of a recovery walk.
#[derive(Debug, Default)]
struct WalkStep {
    /// Sessions whose visit failed; the walk must be repeated.
    incomplete: bool,
    /// Sessions whose events were projected into the outbox.
    projected: u64,
}

/// Longest a single pass of the publisher loop spends visiting historical sessions.
///
/// A recovery walk visits every session the module knows, and a long-lived state root holds
/// tens of thousands: each visit reads that session's whole WAL and, when anything is left to
/// project, fsyncs it. Run in one piece, the walk held the loop for many minutes after every
/// start while live runs' events waited behind it and health read all zeros. Sliced, live
/// events are projected and published between slices, and the walk finishes in the background.
const WALK_SLICE: Duration = Duration::from_millis(100);

/// Passes on which a live barrier's projection is attempted before its session is left to a
/// recovery walk.
const LIVE_PROJECTION_ATTEMPTS: u32 = 5;

/// Visit queued sessions until `budget` has passed, at least one per call so a walk always
/// advances. Returns the sessions still queued.
fn walk_step(
    store: &SqliteStore,
    wal: &FileWalStore,
    mut queue: std::collections::VecDeque<BindIdentity>,
    budget: Duration,
    now: u64,
) -> (std::collections::VecDeque<BindIdentity>, WalkStep) {
    let started = std::time::Instant::now();
    let mut step = WalkStep::default();
    while let Some(session) = queue.pop_front() {
        match recover_session(store, wal, &session, now) {
            Ok(Visit::Current {
                projected: true, ..
            }) => step.projected += 1,
            Ok(_) => {}
            Err(VisitError::Unreadable) => step.incomplete = true,
            Err(VisitError::Projection(error)) => {
                step.incomplete = true;
                store
                    .bus_metrics()
                    .bus_publish_failures
                    .fetch_add(1, Ordering::Relaxed);
                tracing::warn!(target: "bus", %error, "run event projection failed during recovery; the walk will be repeated");
            }
        }
        if started.elapsed() >= budget {
            break;
        }
    }
    (queue, step)
}

pub struct Service {
    pub config: Config,
    pub store: Arc<SqliteStore>,
    pub wal: Arc<FileWalStore>,
    pub registry: LineageRegistry,
    pub connector: Option<Arc<dyn BusConnector>>,
    pub hints: Arc<Hints>,
    pub stop: CancellationToken,
}

impl Service {
    pub fn spawn(self) -> Option<tokio::task::JoinHandle<()>> {
        let Self {
            config,
            store,
            wal,
            registry,
            connector,
            hints,
            stop,
        } = self;
        let _ = METRICS.set(store.bus_metrics_arc());
        if !config.enabled {
            return None;
        }
        ENABLED.store(true, Ordering::Relaxed);
        Some(tokio::spawn(async move {
            let mut publisher = None;
            let mut account_known = false;
            let mut refresh_at_ms = 0;
            let mut recovery = true;
            let mut recovery_at = tokio::time::Instant::now();
            let mut retry_at = tokio::time::Instant::now();
            let mut backoff = Duration::from_secs(1);
            let mut progress = ProgressThrottle::default();
            let mut progress_at = tokio::time::Instant::now();
            let mut prune_at = tokio::time::Instant::now();
            // Sessions the current recovery walk has yet to visit, and when that walk began.
            let mut walk: Option<(std::collections::VecDeque<BindIdentity>, std::time::Instant)> =
                None;
            let mut walk_incomplete = false;
            // Live barriers whose projection failed, with the attempts made so far.
            let mut retry: HashMap<BindIdentity, ((u64, String), u32)> = HashMap::new();
            let mut tick = tokio::time::interval(Duration::from_millis(250));
            loop {
                tokio::select! {_ = stop.cancelled()=>break,_ = tick.tick()=>{}}
                LIVENESS.pass_started();
                let now = now_ms();
                if publisher.is_none() && tokio::time::Instant::now() >= retry_at {
                    let result = async {
                        let connection = connector
                            .as_ref()
                            .ok_or("run event bus transport is not installed")?
                            .connect()
                            .await?;
                        let account = &connection.account;
                        for name in [
                            "run.started",
                            "run.paused",
                            "run.ended",
                            "run.steer_consumed",
                            "run.progress",
                        ] {
                            account
                                .event_subject("broca", name, 1)
                                .map_err(|_| "bus naming client refuses dotted run events")?;
                        }
                        store.enable_bus_events(account.account())?;
                        if !account_known {
                            tracing::info!(target: "bus", account = account.account(), "run events armed: the machine account is known, so durable runs are now projected and published");
                        }
                        account_known = true;
                        LIVENESS.account_known.store(true, Ordering::Relaxed);
                        refresh_at_ms = connection.refresh_at_ms;
                        Ok::<_, String>(connection.publisher)
                    }
                    .await;
                    match result {
                        // A reconnect requests no recovery walk: once the account is known every
                        // durable hint is projected even while the broker is away, so the outbox
                        // already holds everything a new connection has to drain. Only hints
                        // that arrived before the account was known are lost, and those request
                        // a walk below.
                        Ok(connected) => {
                            publisher = Some(connected);
                            backoff = Duration::from_secs(1);
                        }
                        Err(_) => {
                            store
                                .bus_metrics()
                                .bus_publish_failures
                                .fetch_add(1, Ordering::Relaxed);
                            retry_at = tokio::time::Instant::now() + backoff;
                            backoff = (backoff * 2).min(Duration::from_secs(60));
                        }
                    }
                }
                if publisher.is_some() && now >= refresh_at_ms {
                    publisher = None;
                    retry_at = tokio::time::Instant::now();
                }
                let (mut heads, overflow) = hints.take();
                recovery |= overflow;
                // Once an account is known, WAL projection continues during a broker outage.
                if account_known {
                    // Barriers whose projection failed on an earlier pass are tried again here,
                    // unless a newer barrier for the same session has replaced them.
                    let mut attempts: HashMap<BindIdentity, u32> = HashMap::new();
                    for (session, (job, tries)) in retry.drain() {
                        heads.entry(session.clone()).or_insert(job);
                        attempts.insert(session, tries);
                    }
                    // Live barriers first: a run that just committed must never wait for the
                    // historical walk below.
                    for (session, (head, epoch)) in heads {
                        let tries = attempts.get(&session).copied().unwrap_or(0) + 1;
                        let job = (session.clone(), (head, epoch.clone()));
                        let s = store.clone();
                        let w = wal.clone();
                        match broca_diagnostics::spawn_blocking(
                            broca_diagnostics::JobKind::Projection,
                            move || {
                                let after = s
                                    .bus_event_position(&session, &epoch)
                                    .map_err(|e| e.to_string())?;
                                let range = w
                                    .read_records_range(&session, after, Some(head), None)
                                    .map_err(|e| e.to_string())?;
                                if range.lineage_id.is_none_or(|id| id.to_string() != epoch) {
                                    return Err(
                                        "WAL epoch changed during run event projection".into()
                                    );
                                }
                                let broca_wal::WalRangeRecords::Complete(records) =
                                    range.into_records()
                                else {
                                    return Err("run event WAL is absent".into());
                                };
                                s.project_bus_events(&session, &epoch, &records, now)
                                    .map_err(|e| e.to_string())
                            },
                        )
                        .await
                        {
                            Ok(Ok(())) => {
                                LIVENESS.sessions_projected.fetch_add(1, Ordering::Relaxed);
                            }
                            failed => {
                                let error = match failed {
                                    Ok(Err(error)) => error,
                                    _ => "the projection task stopped".to_string(),
                                };
                                store
                                    .bus_metrics()
                                    .bus_publish_failures
                                    .fetch_add(1, Ordering::Relaxed);
                                // A failure such as a briefly locked database clears on a later
                                // pass, so the barrier is retried a few times before the session
                                // is left to a full recovery walk, which can take minutes.
                                if tries < LIVE_PROJECTION_ATTEMPTS {
                                    tracing::warn!(target: "bus", %error, attempt = tries, "run event projection failed; retrying on the next pass");
                                    retry.insert(job.0, (job.1, tries));
                                } else {
                                    recovery = true;
                                    tracing::warn!(target: "bus", %error, "run event projection failed; scheduling WAL recovery");
                                }
                            }
                        }
                    }
                    // Start a requested walk once none is in progress.
                    if walk.is_none() && recovery && tokio::time::Instant::now() >= recovery_at {
                        let s = store.clone();
                        match broca_diagnostics::spawn_blocking(
                            broca_diagnostics::JobKind::StoreRead,
                            move || s.bus_event_sessions(),
                        )
                        .await
                        {
                            Ok(Ok(sessions)) => {
                                recovery = false;
                                walk_incomplete = false;
                                LIVENESS
                                    .recovery_pending
                                    .store(sessions.len() as u64, Ordering::Relaxed);
                                walk = Some((sessions.into(), std::time::Instant::now()));
                            }
                            _ => {
                                tracing::warn!(target: "bus", "run event recovery could not list sessions; retrying in a minute");
                                recovery_at = tokio::time::Instant::now() + Duration::from_secs(60);
                            }
                        }
                    }
                    // Then one bounded slice of it.
                    if let Some((queue, started)) = walk.take() {
                        let s = store.clone();
                        let w = wal.clone();
                        match broca_diagnostics::spawn_blocking(
                            broca_diagnostics::JobKind::WalRead,
                            move || walk_step(&s, &w, queue, WALK_SLICE, now),
                        )
                        .await
                        {
                            Ok((queue, step)) => {
                                walk_incomplete |= step.incomplete;
                                LIVENESS
                                    .sessions_projected
                                    .fetch_add(step.projected, Ordering::Relaxed);
                                LIVENESS
                                    .recovery_pending
                                    .store(queue.len() as u64, Ordering::Relaxed);
                                if queue.is_empty() {
                                    LIVENESS.recovery_walks.fetch_add(1, Ordering::Relaxed);
                                    tracing::info!(target: "bus", elapsed_ms = started.elapsed().as_millis() as u64, incomplete = walk_incomplete, "run event recovery walk finished");
                                    if walk_incomplete {
                                        tracing::warn!(target: "bus", "run event recovery has unreadable WALs; healthy sessions continue");
                                        recovery = true;
                                    }
                                    recovery_at =
                                        tokio::time::Instant::now() + Duration::from_secs(60);
                                } else {
                                    walk = Some((queue, started));
                                }
                            }
                            // The slice's task died with the queue it owned: start the walk over.
                            Err(_) => {
                                LIVENESS.recovery_pending.store(0, Ordering::Relaxed);
                                recovery = true;
                                recovery_at = tokio::time::Instant::now() + Duration::from_secs(60);
                            }
                        }
                    }
                } else if !heads.is_empty() {
                    recovery = true;
                }
                if tokio::time::Instant::now() >= prune_at {
                    let _ = store.prune_bus_events(now);
                    prune_at = tokio::time::Instant::now() + Duration::from_secs(60);
                }
                if let Some(bus) = publisher.as_ref() {
                    if tokio::time::Instant::now() >= retry_at {
                        // One sequential publisher preserves each run's WAL order through retries.
                        for _ in 0..64 {
                            match store.drain_bus_event(bus.as_ref(), now).await {
                                Ok(DrainResult::Published) => {
                                    backoff = Duration::from_secs(1);
                                }
                                Ok(DrainResult::Empty) => break,
                                _ => {
                                    retry_at = tokio::time::Instant::now() + backoff;
                                    backoff = (backoff * 2).min(Duration::from_secs(60));
                                    break;
                                }
                            }
                        }
                    }
                    if tokio::time::Instant::now() < progress_at {
                        continue;
                    }
                    progress_at = tokio::time::Instant::now() + Duration::from_secs(30);
                    for session in registry.active_event_sessions() {
                        if let Ok(StatusResponse::Active { run_id, .. }) =
                            registry.status(&session).await
                        {
                            if let Ok(broca_wire::RunStatusResponse::Active {
                                step: Some(step),
                                ..
                            }) = registry
                                .run_status(&session, broca_protocol::RunId(run_id.clone()))
                                .await
                            {
                                if !progress.due(&run_id, tokio::time::Instant::now()) {
                                    continue;
                                }
                                let range = match wal.read_records_range(&session, 0, Some(0), None)
                                {
                                    Ok(r) => r,
                                    Err(_) => continue,
                                };
                                let Some(epoch) = range.lineage_id else {
                                    continue;
                                };
                                let epoch = epoch.to_string();
                                let seq = store.bus_event_position(&session, &epoch).unwrap_or(0);
                                if seq == 0 {
                                    continue;
                                }
                                let event = RunEvent {
                                    v: 1,
                                    session_ref: broca_core::wal_writer::agent_session_ref(
                                        &session,
                                    ),
                                    run_id,
                                    seq,
                                    at_ms: now,
                                    detail: RunEventDetail::Progress {
                                        step: step.step_id.0,
                                        last_content_at_ms: step.last_content_at_ms,
                                        max_content_gap_ms: step.max_content_gap_ms,
                                    },
                                };
                                if let Ok(Some(event)) =
                                    store.retain_bus_progress(&session, &epoch, &event, now)
                                {
                                    match bus.publish(&event).await {
                                        Ok(receipt) => {
                                            let _ =
                                                store.acknowledge_bus_event(&event, receipt, now);
                                        }
                                        Err(_) => {
                                            store
                                                .bus_metrics()
                                                .bus_publish_failures
                                                .fetch_add(1, Ordering::Relaxed);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    progress.prune();
                }
            }
        }))
    }
}

#[derive(Default)]
struct ProgressThrottle(HashMap<String, tokio::time::Instant>);
impl ProgressThrottle {
    fn due(&mut self, run: &str, now: tokio::time::Instant) -> bool {
        if self
            .0
            .get(run)
            .is_some_and(|last| now.duration_since(*last) < Duration::from_secs(120))
        {
            return false;
        }
        self.0.insert(run.into(), now);
        true
    }
    fn prune(&mut self) {
        let now = tokio::time::Instant::now();
        self.0
            .retain(|_, last| now.duration_since(*last) < Duration::from_secs(240));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bus_event_config_is_off_by_default_and_strict() {
        assert!(!Config::default().enabled);
        assert!(!Config::parse("{}").unwrap().enabled);
        assert!(
            !Config::parse(r#"{"bus_run_events_enabled":false}"#)
                .unwrap()
                .enabled
        );
        assert!(
            Config::parse(r#"{"bus_run_events_enabled":true}"#)
                .unwrap()
                .enabled
        );
        for value in ["1", "null", r#""true""#] {
            assert!(Config::parse(&format!(r#"{{"bus_run_events_enabled":{value}}}"#)).is_err());
        }
    }

    #[test]
    fn bus_event_consumer_declaration_tracks_the_switch() {
        use subc_protocol::manifest::ConsumerRole;
        let has_bus = |enabled| {
            crate::consumer_routes::ConsumerRoutes::default().with_bus_events(enabled).consumer_roles().iter().any(|role| matches!(role,ConsumerRole::ServiceClient {of} if of.iter().any(|id|id=="ckbus")))
        };
        assert!(!has_bus(false));
        assert!(has_bus(true));
    }

    #[test]
    fn bus_recovery_missing_session_does_not_hide_healthy_sessions() {
        use broca_core::{RunIndex, RunIndexRow, RunIndexState};
        let root = broca_test_support::scratch_root().join(format!("bus-recovery-{}", now_ms()));
        let wal = FileWalStore::new(root.join("wal"));
        let store = SqliteStore::open_in_memory().unwrap();
        let missing = BindIdentity::new("/test", "test", "missing");
        let healthy = BindIdentity::new("/test", "test", "healthy");
        for session in [&missing, &healthy] {
            store
                .index_run_active(&RunIndexRow {
                    run_id: session.session.clone(),
                    session: session.clone(),
                    submission_id: None,
                    request_identity_digest: None,
                    state: RunIndexState::Active,
                    usage: None,
                    final_step_finish_reason: None,
                    retries_used: 0,
                    title: None,
                    withdrawals_owed: false,
                })
                .unwrap();
        }
        let mut writer = wal.open(&healthy).unwrap();
        for step in 1..=2 {
            writer
                .append(
                    1,
                    &broca_wal::WalRecord::StepStarted {
                        step_id: broca_protocol::StepId(step),
                    },
                    true,
                )
                .unwrap();
        }
        drop(writer);
        let names = AccountNames::derive("box_broca_test").unwrap();
        store.enable_bus_events(names.account()).unwrap();
        let queue = store.bus_event_sessions().unwrap().into();
        let (queue, step) = walk_step(&store, &wal, queue, Duration::MAX, now_ms());
        assert!(queue.is_empty());
        assert!(!step.incomplete);
        assert_eq!(step.projected, 1);
        // The recovered head is the last durable record's sequence on every platform. It must not
        // depend on the WAL file's identity, which only Unix provides.
        let epoch = wal
            .read_records_range(&healthy, 0, None, None)
            .unwrap()
            .lineage_id
            .unwrap()
            .to_string();
        assert_eq!(store.bus_event_position(&healthy, &epoch).unwrap(), 2);
        // A second visit finds the projection current and writes nothing.
        assert_eq!(
            recover_session(&store, &wal, &healthy, now_ms()).unwrap(),
            Visit::Current {
                head: 2,
                projected: false
            }
        );
        assert_eq!(
            recover_session(&store, &wal, &missing, now_ms()).unwrap(),
            Visit::Absent
        );
    }

    /// One slice visits sessions only until its budget runs out, so the publisher loop returns
    /// to live barriers and the outbox between slices however much history the walk has left.
    #[test]
    fn bus_recovery_walk_slice_stops_at_its_budget_and_keeps_the_rest() {
        let root = broca_test_support::scratch_root().join(format!("bus-slice-{}", now_ms()));
        let wal = FileWalStore::new(root.join("wal"));
        let store = SqliteStore::open_in_memory().unwrap();
        let queue: std::collections::VecDeque<_> = (0..5)
            .map(|n| BindIdentity::new("/test", "test", format!("absent-{n}")))
            .collect();
        let (queue, step) = walk_step(&store, &wal, queue, Duration::ZERO, now_ms());
        assert_eq!(
            queue.len(),
            4,
            "a zero budget still visits exactly one session"
        );
        assert!(!step.incomplete);
        let (queue, _) = walk_step(&store, &wal, queue, Duration::MAX, now_ms());
        assert!(queue.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn bus_progress_throttle_is_per_run_at_least_two_minutes() {
        let mut throttle = ProgressThrottle::default();
        let now = tokio::time::Instant::now();
        assert!(throttle.due("a", now));
        assert!(throttle.due("b", now));
        assert!(!throttle.due("a", now + Duration::from_secs(119)));
        assert!(throttle.due("a", now + Duration::from_secs(120)));
        assert!(!throttle.due("a", now + Duration::from_secs(121)));
    }
}
