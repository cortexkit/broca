//! Keeps `kind: "model_step"` facts in `export_facts` current, off the request path.
//!
//! Three jobs share one background loop:
//!
//! - LIVE. Every successful WAL barrier reports its session's durable head (a hint, no record
//!   bodies). Each pass projects the hinted sessions from their step-fact checkpoint up to that
//!   head, so a committed step reaches `export_facts` within about a pass (250 ms) of becoming
//!   durable. Hints are handled before anything else in the pass.
//! - WALK. Sessions no hint reached: on the first start with this build, every session with a
//!   run in the last 30 days (the backfill); on later starts, sessions whose runs changed since
//!   the previous walk began, plus every run still active or paused, which covers a crash between
//!   a step becoming durable and its fact being written. The walk is resumable (its place is kept
//!   in `model_step_walk`) and runs in short slices between live passes.
//! - EXPIRY. A bounded sweep deletes model step facts 30 days after they finished. It never
//!   touches session or System One facts.
//!
//! Every write is a short transaction on the shared store connection, and every WAL read is a
//! tail read bounded by what the projection needs, so neither a long session nor the backfill
//! holds the connection or the runtime for long.

use broca_core::model_step_fact::MODEL_STEP_FACT_RETENTION_MS;
use broca_core::{FileWalStore, WalStore};
use broca_protocol::BindIdentity;
use broca_store::SqliteStore;
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

/// First tail window a projection reads from a WAL's end; each retry reads four times more.
const TAIL_WINDOW: u64 = 256 * 1024;

/// Longest one pass spends on the walk before returning to live hints.
const WALK_SLICE: Duration = Duration::from_millis(100);

/// A walk that resumes after a restart starts over from this long before the previous walk
/// began, so a session whose run changed while the previous walk's session list was being read
/// is still visited.
const WALK_OVERLAP_MS: u64 = 10 * 60 * 1000;

/// How often the expiry sweep runs, and how many facts one of its transactions deletes.
const EXPIRY_INTERVAL: Duration = Duration::from_secs(10 * 60);
const EXPIRY_BATCH: u64 = 500;
/// Most facts one sweep deletes before yielding to the rest of the loop.
const EXPIRY_PER_SWEEP: u64 = 20 * EXPIRY_BATCH;

/// Sessions a walk visits between saves of its place.
const SAVE_PLACE_EVERY: u64 = 64;

/// How often an unfinished walk logs its progress.
const PROGRESS_EVERY: Duration = Duration::from_secs(30);

/// Durable heads reported since the last pass, by session.
#[derive(Default)]
pub struct Hints(Mutex<HintState>);

#[derive(Default)]
struct HintState {
    heads: HashMap<BindIdentity, u64>,
    /// More sessions reported than the map holds; the dropped ones need a walk.
    overflow: bool,
}

impl Hints {
    /// The callback a [`FileWalStore`] calls after each successful barrier. It keeps only the
    /// session and its durable sequence.
    pub fn observer(self: &Arc<Self>) -> Arc<broca_core::wal_writer::DurableWalObserver> {
        let hints = self.clone();
        Arc::new(move |session, seq, _lineage| hints.note(session, seq))
    }

    pub fn note(&self, session: &BindIdentity, seq: u64) {
        let mut state = self.0.lock().unwrap();
        if state.heads.len() >= 4096 && !state.heads.contains_key(session) {
            state.overflow = true;
        } else {
            let head = state.heads.entry(session.clone()).or_default();
            *head = (*head).max(seq);
        }
    }

    fn take(&self) -> (HashMap<BindIdentity, u64>, bool) {
        let mut state = self.0.lock().unwrap();
        (
            std::mem::take(&mut state.heads),
            std::mem::take(&mut state.overflow),
        )
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

/// What projecting one session did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Visit {
    Projected(broca_store::model_step_facts::StepProjectionReport),
    /// No WAL, or a WAL without a lineage id yet: nothing to derive.
    Absent,
}

/// Project `session`'s step facts up to its durable head.
///
/// `durable_head` is the head a live barrier proved durable; records after it are not read
/// into facts. Without one (the walk), a read-side flush proves what was read durable first.
///
/// A session with a checkpoint for its WAL's lineage needs only the records after it. One
/// without needs every run that may have a step in the last 30 days, so the read reaches back
/// to a `run_started` older than that (or the WAL's start): runs in a session are sequential,
/// so every run with a step inside the window starts at or after that record.
pub fn project_session(
    store: &SqliteStore,
    wal: &FileWalStore,
    session: &BindIdentity,
    now: u64,
    durable_head: Option<u64>,
) -> Result<Visit, String> {
    let missing = |error: &broca_wal::WalError| match error {
        broca_wal::WalError::Io(error) => error.kind() == std::io::ErrorKind::NotFound,
        _ => false,
    };
    let checkpoint = store
        .model_step_checkpoint(session)
        .map_err(|e| e.to_string())?;
    let cutoff = now.saturating_sub(MODEL_STEP_FACT_RETENTION_MS);
    let mut window = TAIL_WINDOW;
    let (records, lineage) = loop {
        let tail = match wal.read_records_tail(session, window) {
            Ok(tail) => tail,
            Err(error) if missing(&error) => return Ok(Visit::Absent),
            Err(error) => return Err(error.to_string()),
        };
        if !tail.file_present {
            return Ok(Visit::Absent);
        }
        let Some(lineage) = tail.lineage_id.map(|id| id.to_string()) else {
            return Ok(Visit::Absent);
        };
        let after = checkpoint
            .as_ref()
            .filter(|checkpoint| checkpoint.lineage_id == lineage)
            .map(|checkpoint| checkpoint.seq);
        let enough = tail.from_start
            || match after {
                Some(after) => tail
                    .records
                    .first()
                    .is_none_or(|record| record.seq <= after + 1),
                None => tail.records.iter().any(|record| {
                    matches!(
                        record.record,
                        broca_wal::WalRecord::RunStarted { ts_ms, .. }
                            if ts_ms.is_none_or(|ts_ms| ts_ms < cutoff)
                    )
                }),
            };
        if enough {
            let mut records = tail.records;
            if let Some(head) = durable_head {
                records.retain(|record| record.seq <= head);
            }
            break (records, lineage);
        }
        window = window.saturating_mul(4);
    };
    let unprojected = records.last().is_some_and(|last| {
        checkpoint
            .as_ref()
            .is_none_or(|c| c.lineage_id != lineage || c.seq < last.seq)
    });
    if durable_head.is_none() && unprojected {
        match wal.flush_for_read(session) {
            Ok(_identity) => {}
            // An archived WAL has no live file to flush, and its member is sealed and durable.
            Err(error) if missing(&error) => {}
            Err(error) => return Err(error.to_string()),
        }
    }
    store
        .project_model_step_facts(session, &lineage, &records, now)
        .map(Visit::Projected)
        .map_err(|e| e.to_string())
}

/// A resumable pass over the sessions a hint may not have reached; see the module comment.
pub struct Walk {
    state: broca_store::model_step_facts::ModelStepWalk,
    queue: VecDeque<(String, BindIdentity)>,
    /// When this process began; a walk that started earlier is followed by one more.
    process_started_ms: u64,
    pub total: u64,
    pub visited: u64,
    pub written: u64,
    pub expired: u64,
    pub failed: u64,
    began: Instant,
    logged: Instant,
}

impl Walk {
    /// Resume an interrupted walk, or begin the one this start owes: the 30-day backfill on
    /// the first start with this build, otherwise one since the previous walk began.
    pub fn begin(store: &SqliteStore, now: u64) -> Result<Self, String> {
        let state = match store.model_step_walk().map_err(|e| e.to_string())? {
            Some(walk) if !walk.done => walk,
            previous => broca_store::model_step_facts::ModelStepWalk {
                since_ms: previous
                    .map(|walk| walk.started_ms.saturating_sub(WALK_OVERLAP_MS))
                    .unwrap_or(0),
                started_ms: now,
                cursor: String::new(),
                done: false,
            },
        };
        Self::start(store, state, now)
    }

    fn start(
        store: &SqliteStore,
        mut state: broca_store::model_step_facts::ModelStepWalk,
        process_started_ms: u64,
    ) -> Result<Self, String> {
        // Nothing older than 30 days is ever written, so no walk reaches further back.
        state.since_ms = state
            .since_ms
            .max(process_started_ms.saturating_sub(MODEL_STEP_FACT_RETENTION_MS));
        store
            .save_model_step_walk(&state)
            .map_err(|e| e.to_string())?;
        let queue: VecDeque<_> = store
            .model_step_walk_sessions(state.since_ms)
            .map_err(|e| e.to_string())?
            .into_iter()
            .filter(|(key, _)| key.as_str() > state.cursor.as_str())
            .collect();
        tracing::info!(
            target: "export",
            sessions = queue.len(),
            since_ms = state.since_ms,
            resumed = !state.cursor.is_empty(),
            "model step fact walk started"
        );
        Ok(Self {
            total: queue.len() as u64,
            state,
            queue,
            process_started_ms,
            visited: 0,
            written: 0,
            expired: 0,
            failed: 0,
            began: Instant::now(),
            logged: Instant::now(),
        })
    }

    pub fn is_done(&self) -> bool {
        self.queue.is_empty() && self.state.done
    }

    /// Visit sessions until `budget` has passed, at least one per call. Returns whether the
    /// walk (and any follow-up walk it owes) is finished.
    pub fn slice(
        &mut self,
        store: &SqliteStore,
        wal: &FileWalStore,
        budget: Duration,
        now: u64,
    ) -> bool {
        let started = Instant::now();
        while let Some((key, session)) = self.queue.pop_front() {
            match project_session(store, wal, &session, now, None) {
                Ok(Visit::Projected(report)) => {
                    self.written += report.written;
                    self.expired += report.expired;
                }
                Ok(Visit::Absent) => {}
                Err(error) => {
                    self.failed += 1;
                    tracing::warn!(target: "export", %error, "model step fact walk could not project a session; a later walk retries it");
                }
            }
            self.visited += 1;
            self.state.cursor = key;
            let out_of_time = started.elapsed() >= budget;
            // The place is saved every few sessions and whenever the slice yields, not after
            // each one: re-visiting a few sessions after a crash writes nothing new, while a
            // write per session was one store transaction per session on a large backfill.
            if out_of_time || self.visited.is_multiple_of(SAVE_PLACE_EVERY) {
                if let Err(error) = store.save_model_step_walk(&self.state) {
                    tracing::warn!(target: "export", %error, "could not record the model step fact walk's place");
                }
            }
            if self.logged.elapsed() >= PROGRESS_EVERY {
                self.logged = Instant::now();
                self.log("model step fact walk in progress");
            }
            if out_of_time {
                return false;
            }
        }
        if !self.state.done {
            self.state.done = true;
            if let Err(error) = store.save_model_step_walk(&self.state) {
                tracing::warn!(target: "export", %error, "could not record that the model step fact walk finished");
            }
            self.log("model step fact walk finished");
            // A walk resumed from an earlier process did not see sessions that changed before
            // its cursor while it was interrupted; one more walk since its start covers them.
            if self.state.started_ms < self.process_started_ms {
                let next = broca_store::model_step_facts::ModelStepWalk {
                    since_ms: self.state.started_ms.saturating_sub(WALK_OVERLAP_MS),
                    started_ms: now,
                    cursor: String::new(),
                    done: false,
                };
                match Self::start(store, next, self.process_started_ms) {
                    Ok(next) => {
                        *self = next;
                        return false;
                    }
                    Err(error) => {
                        tracing::warn!(target: "export", %error, "could not begin the follow-up model step fact walk")
                    }
                }
            }
        }
        true
    }

    fn log(&self, message: &'static str) {
        tracing::info!(
            target: "export",
            visited = self.visited,
            total = self.total,
            written = self.written,
            older_than_30_days = self.expired,
            failed = self.failed,
            elapsed_ms = self.began.elapsed().as_millis() as u64,
            "{message}"
        );
    }
}

/// Delete expired model step facts in bounded transactions. Returns how many were deleted.
pub fn sweep_expired(store: &SqliteStore, now: u64) -> Result<u64, String> {
    let mut deleted = 0;
    while deleted < EXPIRY_PER_SWEEP {
        let batch = store
            .expire_model_step_facts(now, EXPIRY_BATCH)
            .map_err(|e| e.to_string())?;
        deleted += batch;
        if batch < EXPIRY_BATCH {
            break;
        }
    }
    Ok(deleted)
}

pub struct Service {
    pub store: Arc<SqliteStore>,
    pub wal: Arc<FileWalStore>,
    pub hints: Arc<Hints>,
    pub stop: CancellationToken,
}

impl Service {
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        let Self {
            store,
            wal,
            hints,
            stop,
        } = self;
        tokio::spawn(async move {
            let process_started = now_ms();
            let mut walk: Option<Walk> = None;
            let mut walk_requested = true;
            let mut retry: HashMap<BindIdentity, (u64, u32)> = HashMap::new();
            let mut sweep_at = tokio::time::Instant::now();
            let mut tick = tokio::time::interval(Duration::from_millis(250));
            loop {
                tokio::select! {_ = stop.cancelled() => break, _ = tick.tick() => {}}
                let now = now_ms();
                let (mut heads, overflow) = hints.take();
                walk_requested |= overflow;
                for (session, (head, _)) in retry.iter() {
                    let entry = heads.entry(session.clone()).or_insert(*head);
                    *entry = (*entry).max(*head);
                }
                for (session, head) in heads {
                    let tries = retry.remove(&session).map_or(0, |(_, tries)| tries) + 1;
                    let (s, w, visit) = (store.clone(), wal.clone(), session.clone());
                    let result = broca_diagnostics::spawn_blocking(
                        broca_diagnostics::JobKind::Projection,
                        move || project_session(&s, &w, &visit, now, Some(head)),
                    )
                    .await
                    .unwrap_or_else(|_| Err("the projection task stopped".into()));
                    if let Err(error) = result {
                        // A briefly busy database clears on a later pass; a session that keeps
                        // failing is left to a walk, which re-reads its WAL from its checkpoint.
                        if tries < 5 {
                            retry.insert(session, (head, tries));
                        } else {
                            walk_requested = true;
                            tracing::warn!(target: "export", %error, "model step fact projection kept failing; scheduling a walk");
                        }
                    }
                }
                if walk.is_none() && walk_requested {
                    let s = store.clone();
                    let begun = broca_diagnostics::spawn_blocking(
                        broca_diagnostics::JobKind::StoreRead,
                        move || Walk::begin(&s, process_started),
                    )
                    .await
                    .unwrap_or_else(|_| Err("the walk task stopped".into()));
                    match begun {
                        Ok(begun) => {
                            walk_requested = false;
                            walk = Some(begun);
                        }
                        Err(error) => {
                            tracing::warn!(target: "export", %error, "could not begin the model step fact walk; retrying");
                        }
                    }
                }
                if let Some(mut current) = walk.take() {
                    let (s, w) = (store.clone(), wal.clone());
                    match broca_diagnostics::spawn_blocking(
                        broca_diagnostics::JobKind::Recovery,
                        move || {
                            let done = current.slice(&s, &w, WALK_SLICE, now);
                            (current, done)
                        },
                    )
                    .await
                    {
                        Ok((current, false)) => walk = Some(current),
                        Ok((_, true)) => {}
                        // The slice's task died with the walk it owned; its place is stored.
                        Err(_) => walk_requested = true,
                    }
                }
                if tokio::time::Instant::now() >= sweep_at {
                    sweep_at = tokio::time::Instant::now() + EXPIRY_INTERVAL;
                    let s = store.clone();
                    match broca_diagnostics::spawn_blocking(
                        broca_diagnostics::JobKind::StoreWrite,
                        move || sweep_expired(&s, now),
                    )
                    .await
                    .unwrap_or_else(|_| Err("the sweep task stopped".into()))
                    {
                        Ok(0) => {}
                        Ok(deleted) => {
                            tracing::info!(target: "export", deleted, "expired model step facts deleted")
                        }
                        Err(error) => {
                            tracing::warn!(target: "export", %error, "model step fact expiry failed; retrying later")
                        }
                    }
                }
            }
        })
    }
}

#[cfg(test)]
#[path = "model_step_facts_tests.rs"]
mod tests;
