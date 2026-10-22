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
/// projection is not keeping up; a pending recovery count says how many sessions a recovery
/// walk still has to visit; a loop age that keeps growing means one pass of the loop is stuck.
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
    /// Sessions recovery walks have visited, each a read of that session's WAL.
    recovery_visits: AtomicU64,
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
            recovery_visits: AtomicU64::new(0),
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
        fields.insert(
            "bus_recovery_visits".into(),
            count(&LIVENESS.recovery_visits),
        );
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
    async fn compaction_consumer(
        &self,
    ) -> Result<Box<dyn cortexkit_bus_trait::StreamCursor>, String> {
        Err("compaction bus consumer unavailable".into())
    }
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

/// First tail window a projection reads from a WAL's end. Each retry reads four times more,
/// until the read is enough or covers the whole file.
const TAIL_WINDOW: u64 = 256 * 1024;

/// The records a projection needs and the WAL lineage they were read under.
type ProjectionRead = (Vec<broca_wal::FramedRecord>, Option<String>);

/// Why the records for a projection could not be read.
enum ReadError {
    Wal(broca_wal::WalError),
    Store(String),
}

/// The record clock of the record kinds that carry one.
fn record_ts(record: &broca_wal::WalRecord) -> Option<u64> {
    match record {
        broca_wal::WalRecord::RunStarted { ts_ms, .. }
        | broca_wal::WalRecord::Steered { ts_ms, .. }
        | broca_wal::WalRecord::RunPaused { ts_ms, .. }
        | broca_wal::WalRecord::RunFinished { ts_ms, .. } => *ts_ms,
        _ => None,
    }
}

/// Read only the end of `session`'s WAL, as much as its projection needs, and its lineage.
///
/// A session with a checkpoint for the WAL's lineage needs every record after the checkpoint.
/// One without needs the first `RunStarted` stamped during the current period, so the read
/// must reach a timestamped record from before the period; record clocks only move forward
/// within a WAL, so no later run can hide before it. The window starts at [`TAIL_WINDOW`] and
/// quadruples until that holds or the read covers the whole file, so the cost follows what is
/// new rather than how long the session's history is. `None` means the WAL does not exist.
fn read_for_projection(
    store: &SqliteStore,
    wal: &FileWalStore,
    session: &BindIdentity,
) -> Result<Option<ProjectionRead>, ReadError> {
    let checkpoint = store
        .bus_event_checkpoint(session)
        .map_err(|e| ReadError::Store(e.to_string()))?;
    let period = store.bus_event_period_started_ms();
    let mut window = TAIL_WINDOW;
    loop {
        let tail = wal
            .read_records_tail(session, window)
            .map_err(ReadError::Wal)?;
        if !tail.file_present {
            return Ok(None);
        }
        let lineage = tail.lineage_id.map(|id| id.to_string());
        let after = checkpoint.as_ref().and_then(|(epoch, seq)| {
            (epoch.is_empty() || Some(epoch) == lineage.as_ref()).then_some(*seq)
        });
        let enough = tail.from_start
            || match after {
                Some(after) => tail.records.first().is_some_and(|r| r.seq <= after + 1),
                None => tail
                    .records
                    .iter()
                    .any(|r| record_ts(&r.record).is_some_and(|ts| ts < period)),
            };
        if enough {
            let records = match after {
                Some(after) => tail.records.into_iter().filter(|r| r.seq > after).collect(),
                None => tail.records,
            };
            return Ok(Some((records, lineage)));
        }
        window = window.saturating_mul(4);
    }
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

/// Bring one open session's outbox projection up to its durable WAL head after process loss
/// or hint overflow. A session with no checkpoint starts at its head, as a live barrier would.
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
    let (records, lineage) = match read_for_projection(store, wal, session) {
        Ok(Some(read)) => read,
        Ok(None) => return Ok(Visit::Absent),
        Err(ReadError::Wal(error)) if missing(&error) => return Ok(Visit::Absent),
        Err(ReadError::Wal(_)) => return Err(VisitError::Unreadable),
        Err(ReadError::Store(error)) => return Err(VisitError::Projection(error)),
    };
    let Some(epoch) = lineage else {
        return Ok(Visit::Absent);
    };
    let position = store
        .bus_event_position(session, &epoch)
        .map_err(|e| VisitError::Projection(e.to_string()))?;
    // The read returns only records after a checkpoint, so none means the head is the
    // checkpoint itself.
    let head = records.last().map_or(position, |record| record.seq);
    // Nothing new needs no durability proof; projecting the empty remainder still settles
    // whether the session's run is open, so a session whose run finished leaves the walk.
    let current = position >= head;
    if !current {
        match wal.flush_for_read(session) {
            Ok(_identity) => {}
            Err(error) if missing(&error) => return Ok(Visit::Absent),
            Err(_) => return Err(VisitError::Unreadable),
        }
    }
    store
        .project_new_bus_events(session, &epoch, &records, now)
        .map_err(|e| VisitError::Projection(e.to_string()))?;
    Ok(Visit::Current {
        head,
        projected: !current,
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

/// Longest a single pass of the publisher loop spends on a recovery walk.
///
/// A walk visits only sessions with a run open during the current enabled period, normally a
/// handful. Each visit reads that session's whole WAL and, when anything is left to project,
/// fsyncs it, so a slice bound still keeps a slow disk or an unusually long open list from
/// holding live runs' events behind the walk: they are projected and published between slices.
const WALK_SLICE: Duration = Duration::from_millis(100);

/// Passes on which a live barrier's projection is attempted before its session is left to a
/// recovery walk.
const LIVE_PROJECTION_ATTEMPTS: u32 = 5;

/// Longest a single pass of the publisher loop spends publishing from the outbox.
const DRAIN_SLICE: Duration = Duration::from_millis(200);

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
        LIVENESS.recovery_visits.fetch_add(1, Ordering::Relaxed);
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

/// Begin or continue the enabled period, logging which. Returns whether it is in force.
fn begin_period(store: &SqliteStore) -> bool {
    match store.begin_bus_event_period(now_ms()) {
        Ok(start) if start.fresh => {
            tracing::info!(target: "bus", discarded = start.discarded, "run events on: a new period begins, so only runs written from now on become events");
            true
        }
        Ok(_) => {
            tracing::info!(target: "bus", "run events on: continuing the period an earlier process began");
            true
        }
        Err(error) => {
            tracing::warn!(target: "bus", %error, "could not begin the run event period; retrying");
            false
        }
    }
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
            // Off is recorded, so turning events on again begins a new period instead of
            // turning everything written meanwhile into events.
            if let Err(error) = store.end_bus_event_period() {
                tracing::warn!(target: "bus", %error, "could not record that run events are off");
            }
            return None;
        }
        ENABLED.store(true, Ordering::Relaxed);
        // Begin the period before this process writes any WAL, so every run it starts records
        // where its events begin. A failure is retried on each pass, and nothing is projected
        // until it succeeds.
        let mut period_ready = begin_period(&store);
        Some(tokio::spawn(async move {
            let mut publisher = None;
            let mut account_known = false;
            let mut refresh_at_ms = 0;
            let mut recovery = true;
            let mut recovery_at = tokio::time::Instant::now();
            let mut retry_at = tokio::time::Instant::now();
            let mut backoff = Duration::from_secs(1);
            // Whether the stale backlog must be replaced with skip markers before the next
            // publish: set when a connection comes up and after a failed publish, which is
            // how an outage shows while a publisher is held.
            let mut skip_due = true;
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
                            broca_store::bus_events::SKIPPED_EVENT,
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
                            skip_due = true;
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
                if !period_ready {
                    let s = store.clone();
                    period_ready = broca_diagnostics::spawn_blocking(
                        broca_diagnostics::JobKind::StoreWrite,
                        move || begin_period(&s),
                    )
                    .await
                    .unwrap_or(false);
                }
                let (mut heads, overflow) = hints.take();
                recovery |= overflow;
                // Once an account is known, WAL projection continues during a broker outage.
                if account_known && period_ready {
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
                                let (records, lineage) = match read_for_projection(&s, &w, &session)
                                {
                                    Ok(Some(read)) => read,
                                    Ok(None) => return Err("run event WAL is absent".to_string()),
                                    Err(ReadError::Wal(e)) => return Err(e.to_string()),
                                    Err(ReadError::Store(e)) => return Err(e),
                                };
                                if lineage.as_deref() != Some(epoch.as_str()) {
                                    return Err(
                                        "WAL epoch changed during run event projection".into()
                                    );
                                }
                                // Only records the barrier proved durable.
                                let records: Vec<_> =
                                    records.into_iter().filter(|r| r.seq <= head).collect();
                                s.project_new_bus_events(&session, &epoch, &records, now)
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
                            move || s.bus_event_open_sessions(),
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
                    if skip_due && tokio::time::Instant::now() >= retry_at {
                        let s = store.clone();
                        match broca_diagnostics::spawn_blocking(
                            broca_diagnostics::JobKind::StoreWrite,
                            move || s.skip_stale_bus_events(now_ms()),
                        )
                        .await
                        {
                            Ok(Ok(summary)) => {
                                skip_due = false;
                                if summary.skipped > 0 {
                                    tracing::warn!(target: "bus", skipped = summary.skipped, sessions = summary.sessions, "queued run events older than two minutes are not published; one run.skipped marker per session stands in for them");
                                }
                            }
                            failed => {
                                // Publishing now could replay stale events, so the drain waits
                                // until the backlog has been replaced.
                                let error = match failed {
                                    Ok(Err(error)) => error.to_string(),
                                    _ => "the skip task stopped".to_string(),
                                };
                                tracing::warn!(target: "bus", %error, "could not replace the stale run event backlog; publishing waits");
                                retry_at = tokio::time::Instant::now() + backoff;
                                backoff = (backoff * 2).min(Duration::from_secs(60));
                            }
                        }
                    }
                    if !skip_due && tokio::time::Instant::now() >= retry_at {
                        // One sequential publisher preserves each run's WAL order through retries.
                        // A pass drains until the outbox is empty or its budget is spent, so a
                        // backlog clears at the broker's pace rather than a fixed count per pass.
                        let drain_started = tokio::time::Instant::now();
                        while drain_started.elapsed() < DRAIN_SLICE {
                            match store.drain_bus_event(bus.as_ref(), now_ms()).await {
                                Ok(DrainResult::Published) => {
                                    backoff = Duration::from_secs(1);
                                }
                                Ok(DrainResult::Empty) => break,
                                _ => {
                                    retry_at = tokio::time::Instant::now() + backoff;
                                    backoff = (backoff * 2).min(Duration::from_secs(60));
                                    skip_due = true;
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
pub(crate) mod tests {
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
        let queue = vec![missing.clone(), healthy.clone()].into();
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

    /// A projection of a session with long history reads only the WAL's end: enough to reach
    /// a record from before the period, not the whole file.
    #[test]
    fn bus_projection_reads_only_the_tail_of_a_long_history() {
        let root = broca_test_support::scratch_root().join(format!("bus-tail-{}", now_ms()));
        let wal = FileWalStore::new(root.join("wal"));
        let store = SqliteStore::open_in_memory().unwrap();
        let session = BindIdentity::new("/test", "test", "long");
        let hour_ago = now_ms() - 60 * 60 * 1000;
        let mut writer = wal.open(&session).unwrap();
        for n in 0..1000 {
            writer
                .append(
                    1,
                    &run_started(&session, &format!("old-{n}"), hour_ago),
                    false,
                )
                .unwrap();
            writer.append(1, &run_finished(hour_ago), false).unwrap();
        }
        store.begin_bus_event_period(now_ms()).unwrap();
        writer
            .append(1, &run_started(&session, "new", now_ms()), true)
            .unwrap();
        drop(writer);
        let Ok(Some((records, lineage))) = read_for_projection(&store, &wal, &session) else {
            panic!("the WAL must be readable");
        };
        assert!(lineage.is_some());
        assert_eq!(records.last().map(|r| r.seq), Some(2001));
        assert!(
            records.len() < 1000,
            "the projection decoded {} of 2001 records instead of the tail",
            records.len()
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

    pub(crate) fn run_started(
        session: &BindIdentity,
        run_id: &str,
        ts_ms: u64,
    ) -> broca_wal::WalRecord {
        broca_wal::WalRecord::RunStarted {
            tags: None,
            run_id: broca_protocol::RunId(run_id.into()),
            session: session.clone(),
            ts_ms: Some(ts_ms),
            config: Box::new(broca_wal::RunConfig {
                turn_directives_v1: None,
                render_tags: false,
                model: broca_protocol::request::ModelHandle::new("fixture", "model"),
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
                stop_when: vec![],
                context_management: None,
            }),
            input: Arc::new(vec![]),
            origin: Default::default(),
            on_restart: None,
            keep_warm: None,
            project_id: None,
            principal: None,
            retention: None,
            session_class: None,
            submission_id: None,
            send_id: None,
            request_identity_digest: None,
            title: None,
            scope: None,
            marker_order_version: None,
            manifest: None,
            hooks: None,
        }
    }

    pub(crate) fn run_finished(ts_ms: u64) -> broca_wal::WalRecord {
        broca_wal::WalRecord::RunFinished {
            interrupt_reason: None,
            reason: broca_protocol::RunFinishReason::Completed,
            ts_ms: Some(ts_ms),
            usage: Default::default(),
            error: None,
            retries_used: 0,
        }
    }

    /// Every publication the publisher made, as `(kind, run_id)`. A `run.skipped` marker spans
    /// runs and carries no `run_id` header, so it is recorded with an empty one.
    #[derive(Default)]
    struct RecordingBus(Mutex<Vec<(String, String)>>);

    #[async_trait]
    impl BusPublisher for RecordingBus {
        async fn publish(
            &self,
            event: &broca_store::bus_events::Publication,
        ) -> Result<cortexkit_bus_trait::PublishReceipt, String> {
            let mut seen = self.0.lock().unwrap();
            seen.push((
                event.headers["kind"].clone(),
                event.headers.get("run_id").cloned().unwrap_or_default(),
            ));
            Ok(cortexkit_bus_trait::PublishReceipt::new(
                seen.len() as u64,
                false,
            ))
        }
    }

    struct Connector(Arc<RecordingBus>);

    #[async_trait]
    impl BusConnector for Connector {
        async fn connect(&self) -> Result<BusConnection, String> {
            Ok(BusConnection {
                publisher: self.0.clone(),
                account: AccountNames::derive("box_broca_test").unwrap(),
                refresh_at_ms: u64::MAX,
            })
        }
    }

    struct NoActor;

    #[async_trait]
    impl broca_module::registry::LineageOpener for NoActor {
        async fn open(
            &self,
            _key: &broca_module::LineageKey,
            _gen: u64,
            _registry: broca_module::registry::RegistryHandle,
        ) -> Result<broca_module::registry::ActorHandle, broca_module::registry::OpenError>
        {
            Err(broca_module::registry::OpenError::Open(
                "the publisher test opens no actor".into(),
            ))
        }
    }

    /// Turning events on over a store full of finished runs publishes nothing for them, even
    /// when those sessions write again, and publishes every run that starts afterwards,
    /// including a new run on one of the historical sessions.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bus_enable_publishes_only_runs_started_after_it() {
        use broca_core::{RunIndex, RunIndexRow, RunIndexState};
        let root = broca_test_support::scratch_root().join(format!("bus-enable-{}", now_ms()));
        std::fs::create_dir_all(&root).unwrap();
        let store = Arc::new(SqliteStore::open(root.join("run-index.db")).unwrap());
        let hints = Arc::new(Hints::default());
        let wal =
            Arc::new(FileWalStore::new(root.join("wal")).with_durable_observer(hints.observer()));
        wal.set_session_ref_index(store.clone());
        // History: runs that finished an hour ago, recent enough that projecting them would
        // publish rather than drop them. Their WAL barriers are left in the publisher's hints
        // for its first pass, just as a write to an old session after run events are turned on
        // would leave them.
        let hour_ago = now_ms() - 60 * 60 * 1000;
        let session = |n: usize| BindIdentity::new(&root, "runner", format!("history-{n}"));
        for n in 0..100 {
            let run_id = format!("old-{n}");
            store
                .index_run_active(&RunIndexRow {
                    run_id: run_id.clone(),
                    session: session(n),
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
            let mut writer = wal.open(&session(n)).unwrap();
            writer
                .append(1, &run_started(&session(n), &run_id, hour_ago), true)
                .unwrap();
            writer.append(1, &run_finished(hour_ago), true).unwrap();
            store
                .index_run_terminal(&run_id, RunIndexState::Completed, None, None, 0)
                .unwrap();
        }
        // The period begins strictly after the history's last state change.
        std::thread::sleep(Duration::from_millis(5));
        let bus = Arc::new(RecordingBus::default());
        let stop = CancellationToken::new();
        let _service = Service {
            config: Config { enabled: true },
            store: store.clone(),
            wal: wal.clone(),
            registry: LineageRegistry::new_with_reader(
                Arc::new(NoActor),
                wal.clone(),
                Some(store.clone()),
            ),
            connector: Some(Arc::new(Connector(bus.clone()))),
            hints: hints.clone(),
            stop: stop.clone(),
        }
        .spawn()
        .unwrap();
        // After the switch: a run on a new session and a second run on a historical one.
        let fresh = BindIdentity::new(&root, "runner", "fresh");
        for (who, run_id) in [(&fresh, "new-run"), (&session(0), "second-run")] {
            let mut writer = wal.open(who).unwrap();
            writer
                .append(1, &run_started(who, run_id, now_ms()), true)
                .unwrap();
            writer.append(1, &run_finished(now_ms()), true).unwrap();
        }
        let expected = [
            ("run.started", "new-run"),
            ("run.ended", "new-run"),
            ("run.started", "second-run"),
            ("run.ended", "second-run"),
        ];
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            let seen = bus.0.lock().unwrap().clone();
            if expected
                .iter()
                .all(|(kind, run)| seen.iter().any(|(k, r)| k == kind && r == run))
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "runs started after enabling were not all published: {seen:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // Let a few more passes run, so anything owed for history would have been published.
        tokio::time::sleep(Duration::from_millis(750)).await;
        stop.cancel();
        let seen = bus.0.lock().unwrap().clone();
        assert_eq!(
            seen.len(),
            expected.len(),
            "history must publish nothing: {seen:?}"
        );
        assert!(store.pending_bus_event().unwrap().is_none());
        assert_eq!(
            store
                .bus_metrics()
                .bus_outbox_dropped
                .load(Ordering::Relaxed),
            0
        );
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
