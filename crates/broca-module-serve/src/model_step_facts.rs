//! Keeps `kind: "model_step"` facts in `export_facts` current, off the request path.
//!
//! Four jobs share one background loop:
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
//! - PACED PUBLISHING. Steps that finished before the writer's first start are history: the
//!   live projection and the walk both put their facts in the store's backlog, and each pass
//!   publishes what the cap allows (at most 5,000 rows in any 30 s, astrocyte's request), so
//!   live session facts are never queued behind hours of history. Live steps skip the backlog.
//! - EXPIRY. A bounded sweep deletes model step facts 30 days after they finished. It never
//!   touches session or System One facts.
//!
//! Every write is a short transaction on the shared store connection, and every WAL read is
//! either a bounded tail or a one-record-at-a-time scan, so neither a long session nor the
//! backfill holds the connection for long or grows the process with a WAL's size.

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

/// Sessions a walk visits between saves of its place (removing them from its queue).
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

fn missing(error: &broca_wal::WalError) -> bool {
    match error {
        broca_wal::WalError::Io(error) => error.kind() == std::io::ErrorKind::NotFound,
        _ => false,
    }
}

/// Project `session`'s step facts up to its durable head.
///
/// `durable_head` is the head a live barrier proved durable; records after it are not read
/// into facts. Without one (the walk), a read-side flush proves what was read durable first.
///
/// MEMORY IS BOUNDED WHATEVER THE WAL'S SIZE. A session whose checkpoint is near the WAL's end
/// (every live projection, and a walk revisiting a session it already covered) reads a tail of
/// at most [`TAIL_CAP`] bytes. Anything else (a session never projected, a replaced WAL, a
/// checkpoint far behind) is decoded from the start ONE RECORD AT A TIME and folded as it is
/// read, so only one record and one transaction's facts are held at once. Decoding a whole
/// WAL at once instead held every record of the session, content included, and on production
/// state that dominated the backfill's footprint.
pub fn project_session(
    store: &SqliteStore,
    wal: &FileWalStore,
    session: &BindIdentity,
    now: u64,
    durable_head: Option<u64>,
) -> Result<Visit, String> {
    if let Some(checkpoint) = store
        .model_step_checkpoint(session)
        .map_err(|e| e.to_string())?
    {
        if let Some(visit) = project_tail(store, wal, session, &checkpoint, now, durable_head)? {
            return Ok(visit);
        }
    }
    project_stream(store, wal, session, now, durable_head)
}

/// The largest tail a projection reads at once. Records after a checkpoint that do not fit
/// are streamed from the WAL's start instead.
const TAIL_CAP: u64 = 4 * 1024 * 1024;

/// Project the records after `checkpoint` from a bounded tail read, or `None` when they do not
/// fit in [`TAIL_CAP`] (or the WAL is not live, or its lineage changed), leaving the session to
/// [`project_stream`].
fn project_tail(
    store: &SqliteStore,
    wal: &FileWalStore,
    session: &BindIdentity,
    checkpoint: &broca_store::model_step_facts::ModelStepCheckpoint,
    now: u64,
    durable_head: Option<u64>,
) -> Result<Option<Visit>, String> {
    let mut window = TAIL_WINDOW;
    loop {
        let range = match wal.read_live_tail_bounded(session, window) {
            Ok(range) => range,
            Err(error) if missing(&error) => return Ok(None),
            Err(error) => return Err(error.to_string()),
        };
        let records = match range.as_ref().map(|range| range.records()) {
            Some(broca_wal::WalRangeRecords::Complete(records))
            | Some(broca_wal::WalRangeRecords::Partial(records))
            | Some(broca_wal::WalRangeRecords::Unknown(records)) => Some(records),
            _ => None,
        };
        if let (Some(range), Some(records)) = (range.as_ref(), records) {
            if range.lineage_id.map(|id| id.to_string()).as_deref()
                != Some(checkpoint.lineage_id.as_str())
            {
                return Ok(None);
            }
            if records
                .first()
                .is_none_or(|first| first.seq <= checkpoint.seq + 1)
            {
                let head = records.last().map(|last| last.seq);
                let records: Vec<_> = records
                    .iter()
                    .filter(|record| durable_head.is_none_or(|head| record.seq <= head))
                    .cloned()
                    .collect();
                if durable_head.is_none() && head.is_some_and(|head| head > checkpoint.seq) {
                    match wal.flush_for_read(session) {
                        Ok(_identity) => {}
                        Err(error) if missing(&error) => return Ok(None),
                        Err(error) => return Err(error.to_string()),
                    }
                }
                return store
                    .project_model_step_facts(session, &checkpoint.lineage_id, &records, now)
                    .map(|report| Some(Visit::Projected(report)))
                    .map_err(|e| e.to_string());
            }
        }
        if window >= TAIL_CAP {
            return Ok(None);
        }
        window = window.saturating_mul(4).min(TAIL_CAP);
    }
}

/// Stream `session`'s WAL from its first record through the store's step-fact writer.
fn project_stream(
    store: &SqliteStore,
    wal: &FileWalStore,
    session: &BindIdentity,
    now: u64,
    durable_head: Option<u64>,
) -> Result<Visit, String> {
    let mut writer = None;
    let mut failure: Option<String> = None;
    let mut unlabelled = false;
    let scanned = wal.scan_durable_records(session, durable_head, |frame, lineage| {
        // A WAL written before lineage ids existed gains its lineage frame only on its next
        // append by a newer build, after its old records. Those records have no lineage to file
        // facts under, so such a session yields no step facts.
        let Some(lineage) = lineage else {
            unlabelled = true;
            return false;
        };
        if writer.is_none() {
            match store.model_step_writer(session, &lineage.to_string(), now) {
                Ok(created) => writer = Some(created),
                Err(error) => {
                    failure = Some(error.to_string());
                    return false;
                }
            }
        }
        match writer.as_mut().map(|writer| writer.push(frame)) {
            Some(Err(error)) => {
                failure = Some(error.to_string());
                false
            }
            _ => true,
        }
    });
    match scanned {
        Ok(None) => return Ok(Visit::Absent),
        Ok(Some(_)) => {}
        Err(error) if missing(&error) => return Ok(Visit::Absent),
        Err(error) => return Err(error.to_string()),
    }
    if let Some(failure) = failure {
        return Err(failure);
    }
    match writer {
        Some(writer) => writer
            .finish()
            .map(Visit::Projected)
            .map_err(|e| e.to_string()),
        None if unlabelled => Ok(Visit::Absent),
        None => Ok(Visit::Projected(Default::default())),
    }
}

/// Sessions read from the walk's on-disk queue at a time.
const WALK_PAGE: u64 = 256;

/// `run_index` rows scanned per transaction while filling the walk's queue.
const WALK_FILL_ROWS: i64 = 20_000;

/// A resumable pass over the sessions a hint may not have reached; see the module comment.
///
/// The sessions to visit live in the store's `model_step_walk_queue`, filled from `run_index`
/// in bounded transactions and read a page at a time, so a walk over every session the store
/// knows holds one page in memory rather than the whole list.
pub struct Walk {
    state: broca_store::model_step_facts::ModelStepWalk,
    page: VecDeque<(String, BindIdentity)>,
    /// Visited sessions not yet removed from the queue.
    visited_keys: Vec<String>,
    /// When this process began; a walk that started earlier is followed by one more.
    process_started_ms: u64,
    pub total: u64,
    pub visited: u64,
    pub written: u64,
    /// History steps handed to the paced backlog rather than written at once.
    pub queued: u64,
    pub expired: u64,
    pub failed: u64,
    began: Instant,
    logged: Instant,
}

impl Walk {
    /// Resume an interrupted walk, or begin the one this start owes: the 30-day backfill on
    /// the first start with this build, otherwise one since the previous walk began.
    pub fn begin(store: &SqliteStore, now: u64) -> Result<Self, String> {
        match store.model_step_walk().map_err(|e| e.to_string())? {
            Some(walk) if !walk.done => Self::start(store, walk, now),
            previous => {
                let since_ms = previous
                    .map(|walk| walk.started_ms.saturating_sub(WALK_OVERLAP_MS))
                    .unwrap_or(0);
                Self::fresh(store, since_ms, now, now)
            }
        }
    }

    fn fresh(
        store: &SqliteStore,
        since_ms: u64,
        started_ms: u64,
        process_started_ms: u64,
    ) -> Result<Self, String> {
        store
            .clear_model_step_walk_queue()
            .map_err(|e| e.to_string())?;
        let state = broca_store::model_step_facts::ModelStepWalk {
            since_ms,
            started_ms,
            build_rowid: Some(0),
            done: false,
        };
        Self::start(store, state, process_started_ms)
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
        tracing::info!(
            target: "export",
            since_ms = state.since_ms,
            resumed = state.started_ms < process_started_ms,
            "model step fact walk started"
        );
        // A resumed walk whose queue is complete knows its size now; otherwise it is known once
        // the queue is filled.
        let total = if state.build_rowid.is_none() {
            store
                .model_step_walk_queue_len()
                .map_err(|e| e.to_string())?
        } else {
            0
        };
        Ok(Self {
            total,
            state,
            page: VecDeque::new(),
            visited_keys: Vec::new(),
            process_started_ms,
            visited: 0,
            written: 0,
            queued: 0,
            expired: 0,
            failed: 0,
            began: Instant::now(),
            logged: Instant::now(),
        })
    }

    pub fn is_done(&self) -> bool {
        self.state.done
    }

    /// Remove the sessions visited so far from the queue and record the walk's state.
    /// Re-visiting a session after a crash writes nothing new, so this runs every few
    /// sessions and whenever a slice yields rather than after each one.
    fn save(&mut self, store: &SqliteStore) {
        let visited = std::mem::take(&mut self.visited_keys);
        let saved = store
            .model_step_walk_visited(&visited)
            .and_then(|()| store.save_model_step_walk(&self.state));
        if let Err(error) = saved {
            tracing::warn!(target: "export", %error, "could not record the model step fact walk's place");
        }
    }

    /// Fill the queue or visit sessions until `budget` has passed, at least one step per call.
    /// Returns whether the walk (and any follow-up walk it owes) is finished.
    pub fn slice(
        &mut self,
        store: &SqliteStore,
        wal: &FileWalStore,
        budget: Duration,
        now: u64,
    ) -> bool {
        let started = Instant::now();
        loop {
            if self.state.done {
                return true;
            }
            if let Some(after) = self.state.build_rowid {
                match store.fill_model_step_walk_queue(self.state.since_ms, after, WALK_FILL_ROWS) {
                    Ok(next) => {
                        self.state.build_rowid = next;
                        if next.is_none() {
                            self.total = store.model_step_walk_queue_len().unwrap_or(0);
                            self.log("model step fact walk queued its sessions");
                        }
                        self.save(store);
                    }
                    Err(error) => {
                        tracing::warn!(target: "export", %error, "could not fill the model step fact walk's queue; retrying");
                        return false;
                    }
                }
            } else if let Some((key, session)) = self.page.pop_front() {
                match project_session(store, wal, &session, now, None) {
                    Ok(Visit::Projected(report)) => {
                        self.written += report.written;
                        self.queued += report.queued;
                        self.expired += report.expired;
                    }
                    Ok(Visit::Absent) => {}
                    Err(error) => {
                        self.failed += 1;
                        tracing::warn!(target: "export", %error, "model step fact walk could not project a session; a later walk retries it");
                    }
                }
                self.visited += 1;
                self.visited_keys.push(key);
                if self.visited.is_multiple_of(SAVE_PLACE_EVERY) {
                    self.save(store);
                }
            } else {
                // The page is used up: drop its sessions from the queue, then read the next.
                self.save(store);
                match store.model_step_walk_page(WALK_PAGE) {
                    Ok(page) if page.is_empty() => {
                        self.finish(store, now);
                        return self.state.done;
                    }
                    Ok(page) => self.page = page.into(),
                    Err(error) => {
                        tracing::warn!(target: "export", %error, "could not read the model step fact walk's queue; retrying");
                        return false;
                    }
                }
            }
            if self.logged.elapsed() >= PROGRESS_EVERY {
                self.logged = Instant::now();
                self.log("model step fact walk in progress");
            }
            if started.elapsed() >= budget {
                self.save(store);
                return false;
            }
        }
    }

    fn finish(&mut self, store: &SqliteStore, now: u64) {
        self.state.done = true;
        self.save(store);
        self.log("model step fact walk finished");
        // A walk resumed from an earlier process did not see sessions whose runs changed after
        // its queue was filled; one more walk since that walk began covers them.
        if self.state.started_ms < self.process_started_ms {
            let since_ms = self.state.started_ms.saturating_sub(WALK_OVERLAP_MS);
            match Self::fresh(store, since_ms, now, self.process_started_ms) {
                Ok(next) => *self = next,
                Err(error) => {
                    tracing::warn!(target: "export", %error, "could not begin the follow-up model step fact walk")
                }
            }
        }
    }

    fn log(&self, message: &'static str) {
        tracing::info!(
            target: "export",
            visited = self.visited,
            total = self.total,
            written = self.written,
            queued_for_paced_publishing = self.queued,
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

/// Counts what the paced publisher did and logs it every [`PROGRESS_EVERY`] while it works.
#[derive(Default)]
struct PublishProgress {
    published: u64,
    expired: u64,
    logged: Option<Instant>,
}

impl PublishProgress {
    async fn record(
        &mut self,
        store: &Arc<SqliteStore>,
        report: broca_store::model_step_facts::BacklogPublishReport,
    ) {
        self.published += report.published;
        self.expired += report.expired;
        let moved = report.published + report.expired > 0;
        if !moved || self.logged.is_some_and(|at| at.elapsed() < PROGRESS_EVERY) {
            return;
        }
        self.logged = Some(Instant::now());
        let s = store.clone();
        let remaining =
            broca_diagnostics::spawn_blocking(broca_diagnostics::JobKind::StoreRead, move || {
                s.model_step_backlog_len()
            })
            .await
            .ok()
            .and_then(Result::ok);
        tracing::info!(
            target: "export",
            published = self.published,
            older_than_30_days = self.expired,
            remaining,
            "model step backfill publishing at its capped rate"
        );
    }
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
            // Whether the history cutoff is recorded. Until it is, nothing is projected: a fact
            // written before it would skip the paced backlog.
            let mut backfill_ready = false;
            let mut publishing = PublishProgress::default();
            let mut tick = tokio::time::interval(Duration::from_millis(250));
            loop {
                tokio::select! {_ = stop.cancelled() => break, _ = tick.tick() => {}}
                let now = now_ms();
                if !backfill_ready {
                    let s = store.clone();
                    match broca_diagnostics::spawn_blocking(
                        broca_diagnostics::JobKind::StoreWrite,
                        move || s.begin_model_step_backfill(process_started),
                    )
                    .await
                    {
                        Ok(Ok(_)) => backfill_ready = true,
                        _ => {
                            tracing::warn!(target: "export", "could not record the model step backfill cutoff; retrying");
                            continue;
                        }
                    }
                }
                let (mut heads, overflow) = hints.take();
                walk_requested |= overflow;
                for (session, (head, _)) in retry.iter() {
                    let entry = heads.entry(session.clone()).or_insert(*head);
                    *entry = (*entry).max(*head);
                }
                // Live steps first, unpaced: they never wait behind the backfill below.
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
                // Then the paced backfill: publish what the cap allows now.
                let s = store.clone();
                match broca_diagnostics::spawn_blocking(
                    broca_diagnostics::JobKind::StoreWrite,
                    move || s.publish_model_step_backlog(now),
                )
                .await
                {
                    Ok(Ok(report)) => publishing.record(&store, report).await,
                    Ok(Err(error)) => {
                        tracing::warn!(target: "export", %error, "could not publish backfilled model step facts; retrying")
                    }
                    Err(_) => {}
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
