//! Writes `kind: "model_step"` facts into `export_facts`, keeps each one for 30 days, and
//! remembers how far through each session's WAL it has derived them.
//!
//! WHY A SEPARATE PER-SESSION CHECKPOINT. Session facts are written once a run has ended; step
//! facts must appear while the run is still going, within seconds of each committed step. So
//! this writer follows each WAL's durable head on its own, with its own `(lineage, seq)`
//! checkpoint and the serialized [`ModelStepFold`] beside it, independent of the transcript
//! projection's turn-boundary watermark and of the run-event outbox.
//!
//! The facts a range yields and the checkpoint that covers that range are committed in ONE
//! SQLite transaction. A crash before that commit leaves both unwritten, so the next projection
//! derives the same facts again; a crash after it leaves the checkpoint past them, so they are
//! never derived twice. `fact_id` is the second guard: re-deriving a step always hits the same
//! row and `INSERT OR IGNORE` keeps the first one.
//!
//! PACED BACKFILL. A step that finished before this writer's first start is history: its fact
//! goes into `model_step_backlog` in that same transaction instead of `export_facts`, and
//! [`SqliteStore::publish_model_step_backlog`] moves backlog rows into `export_facts` at no more
//! than [`BACKFILL_ROWS_PER_WINDOW`] per [`BACKFILL_WINDOW_MS`]. A step that finished since is
//! written straight to `export_facts`, so live steps never wait behind the backfill.
//!
//! NEVER SPEND. A step fact leaves the SQL `run_id` column EMPTY. The startup catch-up that finds
//! terminal runs missing their session facts asks whether ANY row carries the run's id, so a
//! step row filed under the run would hide a run whose billing facts were lost. The run id stays
//! inside `segment_json`. Nothing here reads, changes or deletes a session or System One fact:
//! the expiry sweep only deletes rows listed in `export_fact_expiry`, which only this writer
//! fills, and re-checks each row's `kind` before deleting it.

use broca_core::model_step_fact::{
    FoldedStep, ModelStepFold, StepFactIdentity, MODEL_STEP_FACT_RETENTION_MS,
};
use broca_protocol::BindIdentity;
use broca_wal::FramedRecord;
use rusqlite::{params, Connection, OptionalExtension};
use std::sync::atomic::Ordering;

use crate::{SqliteStore, StoreError};

/// Facts committed per transaction, so a long backfill range never holds the store's one
/// connection for more than a short insert batch at a time.
const FACTS_PER_TRANSACTION: usize = 128;

/// Records folded per transaction even when they hold no step, so a long stretch of other
/// records still advances the checkpoint in bounded pieces.
const RECORDS_PER_TRANSACTION: usize = 8192;

/// The publishing cap astrocyte asked for: at most this many backlog rows in any window of
/// [`BACKFILL_WINDOW_MS`]. Astrocyte reads `export_facts` in `export_seq` order one page per
/// cycle, so an unpaced backfill would queue live session facts (spend) behind hours of history.
pub const BACKFILL_ROWS_PER_WINDOW: u64 = 5_000;
pub const BACKFILL_WINDOW_MS: u64 = 30_000;

/// The pace: one backlog row per 6.25 ms on average, with a burst of at most
/// [`PUBLISH_BURST`] rows at once. This is the generic cell rate algorithm: a row may be
/// published at time t when t >= TAT - (BURST - 1) * INTERVAL, and each published row moves TAT
/// (the theoretical arrival time) on by one INTERVAL. Any window of length L then holds at most
/// floor((L + (BURST - 1) * INTERVAL) / INTERVAL) + 1 rows: for 30 s that is 4,800 + 128 = 4,928,
/// under the 5,000 cap, and pinned by `the_backlog_never_publishes_more_than_the_cap_in_any_window`.
pub const PUBLISH_INTERVAL_US: u64 = 6_250;
pub const PUBLISH_BURST: u64 = 128;

pub(crate) fn init_schema(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS model_step_projection (
             session    TEXT PRIMARY KEY,
             lineage_id TEXT NOT NULL,
             seq        INTEGER NOT NULL,
             fold_json  TEXT NOT NULL
         );
         -- When each model step fact may be deleted. Only model step facts get a row, so the
         -- sweep can never reach a session or System One fact through this table.
         CREATE TABLE IF NOT EXISTS export_fact_expiry (
             export_seq    INTEGER PRIMARY KEY,
             expires_at_ms INTEGER NOT NULL
         );
         CREATE INDEX IF NOT EXISTS export_fact_expiry_due
             ON export_fact_expiry(expires_at_ms);
         -- The resumable walk that derives step facts for sessions no live write reached:
         -- the 30-day backfill on first start, and recovery after a crash on later starts.
         CREATE TABLE IF NOT EXISTS model_step_walk (
             id          INTEGER PRIMARY KEY CHECK (id = 1),
             since_ms    INTEGER NOT NULL,
             started_ms  INTEGER NOT NULL,
             -- run_index rowid the queue has been filled through; NULL once it is complete.
             build_rowid INTEGER,
             done        INTEGER NOT NULL
         );
         -- The sessions the current walk has yet to visit, by their run_index `session` JSON.
         -- Kept on disk rather than in memory, because a backfill can list every session the
         -- store knows; a visited session is deleted from it.
         -- Backfilled step facts waiting to be published into export_facts at a capped rate
         -- (see `publish_model_step_backlog`), in the order they were derived.
         CREATE TABLE IF NOT EXISTS model_step_backlog (
             seq          INTEGER PRIMARY KEY,
             fact_id      TEXT NOT NULL UNIQUE,
             segment_json TEXT NOT NULL,
             basis_ms     INTEGER NOT NULL
         );
         -- One row, written once by the first start that runs the step-fact writer.
         -- `backfill_before_ms`: steps that finished before it are history and go through the
         -- backlog. `publish_tat_us`: the publishing pace's next theoretical publish time, kept
         -- here so a restart continues the pace rather than starting a new burst.
         CREATE TABLE IF NOT EXISTS model_step_meta (
             id                 INTEGER PRIMARY KEY CHECK (id = 1),
             backfill_before_ms INTEGER NOT NULL,
             publish_tat_us     INTEGER NOT NULL
         );
         CREATE TABLE IF NOT EXISTS model_step_walk_queue (
             session TEXT PRIMARY KEY
         ) WITHOUT ROWID;",
    )
}

/// How far a session's step facts have been derived.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelStepCheckpoint {
    pub lineage_id: String,
    pub seq: u64,
}

/// What one projection call did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct StepProjectionReport {
    /// New rows inserted into `export_facts`.
    pub written: u64,
    /// Steps whose `fact_id` was already stored; nothing was written for them.
    pub already_present: u64,
    /// Steps already past their 30-day lifetime, which are never written (a full re-read of an
    /// old WAL must not bring back facts the sweep deleted).
    pub expired: u64,
    /// Steps of runs installed from another machine; that machine reports them.
    pub imported: u64,
    /// History steps (finished before the first start of this writer) added to the backlog,
    /// which publishes them into `export_facts` at a capped rate.
    pub queued: u64,
    /// The checkpoint after the call; zero when nothing was projected.
    pub through_seq: u64,
}

/// What one [`SqliteStore::publish_model_step_backlog`] call did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BacklogPublishReport {
    /// Rows moved into `export_facts`.
    pub published: u64,
    /// Rows dropped because their 30 days ended while they waited.
    pub expired: u64,
}

/// How many backlog rows the pace allows at `now_us` given the stored theoretical arrival time:
/// the largest n with now >= TAT + (n - 1) * INTERVAL - (BURST - 1) * INTERVAL, at most BURST.
fn publishable_rows(tat_us: u64, now_us: u64) -> u64 {
    let tolerance = (PUBLISH_BURST - 1) * PUBLISH_INTERVAL_US;
    match (now_us + tolerance).checked_sub(tat_us) {
        Some(slack) => (slack / PUBLISH_INTERVAL_US + 1).min(PUBLISH_BURST),
        None => 0,
    }
}

/// The persisted walk state; see `model_step_walk`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelStepWalk {
    /// Sessions with a run whose state changed at or after this time are visited.
    pub since_ms: u64,
    /// When the walk began; the next walk starts from here (less a margin).
    pub started_ms: u64,
    /// The run_index rowid the walk's queue has been filled through; `None` once the queue
    /// holds every session the walk visits.
    pub build_rowid: Option<i64>,
    pub done: bool,
}

/// The expiry time of a fact whose lifetime counts from `basis_ms`.
pub fn model_step_expires_at(basis_ms: u64) -> u64 {
    basis_ms.saturating_add(MODEL_STEP_FACT_RETENTION_MS)
}

/// Whether a fact dated `basis_ms` is past its lifetime at `now_ms`. The sweep deletes at
/// exactly this boundary and the writer refuses to write at it, so the two never disagree.
pub fn model_step_expired(basis_ms: u64, now_ms: u64) -> bool {
    model_step_expires_at(basis_ms) <= now_ms
}

/// Folds one session's durable records in WAL order and writes their step facts in bounded
/// transactions, each also moving the checkpoint past the records it covers. It holds no
/// records, only the fold state and at most one transaction's facts, so a caller streaming a
/// WAL of any size through it keeps a bounded footprint.
pub struct ModelStepWriter<'a> {
    store: &'a SqliteStore,
    key: String,
    session_json: String,
    lineage_id: String,
    identity: StepFactIdentity,
    /// The stored checkpoint as last read or written; a commit refuses if it moved.
    expected: Option<ModelStepCheckpoint>,
    /// Records at or before this sequence are already covered by the checkpoint.
    after: u64,
    /// The last record folded (or `after` before the first).
    last: u64,
    fold: ModelStepFold,
    batch: Vec<FoldedStep>,
    /// Records folded since the last commit.
    pending: usize,
    now_ms: u64,
    /// See `model_step_meta.backfill_before_ms`; `None` before the writer's first start, when
    /// every fact is published at once.
    backfill_before_ms: Option<u64>,
    report: StepProjectionReport,
}

impl ModelStepWriter<'_> {
    /// The checkpoint this writer continues from: records at or before it are skipped.
    pub fn after(&self) -> u64 {
        self.after
    }

    /// Fold one durable record. Records at or before the checkpoint are skipped; a record that
    /// does not follow the previous one is refused, because the fold would silently miss steps.
    pub fn push(&mut self, frame: &FramedRecord) -> Result<(), StoreError> {
        if frame.seq <= self.after {
            return Ok(());
        }
        // Without a checkpoint (after == 0) the first record may be anywhere: a caller folds
        // from wherever its read began.
        let follows = (self.last == 0 && self.after == 0) || frame.seq == self.last + 1;
        if !follows {
            return Err(StoreError::InconsistentSnapshot(
                "model step projection range has a gap after its checkpoint",
            ));
        }
        self.batch.extend(self.fold.apply(frame, &self.identity));
        self.last = frame.seq;
        self.pending += 1;
        if self.batch.len() >= FACTS_PER_TRANSACTION || self.pending >= RECORDS_PER_TRANSACTION {
            self.commit()?;
        }
        Ok(())
    }

    /// Commit what is left and report what this writer did.
    pub fn finish(mut self) -> Result<StepProjectionReport, StoreError> {
        if self.pending > 0 {
            self.commit()?;
        }
        Ok(self.report)
    }

    fn commit(&mut self) -> Result<(), StoreError> {
        self.store.commit_model_step_batch(
            &self.key,
            &self.session_json,
            &self.lineage_id,
            self.expected.as_ref(),
            self.last,
            &self.fold,
            std::mem::take(&mut self.batch),
            self.now_ms,
            self.backfill_before_ms,
            &mut self.report,
        )?;
        self.expected = Some(ModelStepCheckpoint {
            lineage_id: self.lineage_id.clone(),
            seq: self.last,
        });
        self.report.through_seq = self.last;
        self.pending = 0;
        Ok(())
    }
}

#[cfg(test)]
thread_local! {
    /// Fails a projection transaction after its facts are inserted and before its checkpoint
    /// moves, standing in for a crash at the worst point.
    pub(crate) static MODEL_STEP_COMMIT_HOOK: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

fn int(value: u64) -> Result<i64, StoreError> {
    i64::try_from(value)
        .map_err(|_| StoreError::InconsistentSnapshot("value exceeds SQLite INTEGER"))
}

impl SqliteStore {
    pub fn model_step_checkpoint(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<ModelStepCheckpoint>, StoreError> {
        Ok(self
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT lineage_id, seq FROM model_step_projection WHERE session=?1",
                [Self::key(session)],
                |row| {
                    Ok(ModelStepCheckpoint {
                        lineage_id: row.get(0)?,
                        seq: row.get::<_, i64>(1)?.max(0) as u64,
                    })
                },
            )
            .optional()?)
    }

    /// Derive and write the step facts in `records`, a DURABLE range of `session`'s WAL with
    /// lineage `lineage_id`.
    ///
    /// When the stored checkpoint names this lineage, `records` must reach back to the record
    /// right after it; records at or before it are ignored. Without such a checkpoint (a session
    /// this writer has not seen, or a WAL that was replaced) every record is folded from an empty
    /// state, so the caller passes a range that starts early enough to hold the `run_started` of
    /// every run it wants facts for.
    pub fn project_model_step_facts(
        &self,
        session: &BindIdentity,
        lineage_id: &str,
        records: &[FramedRecord],
        now_ms: u64,
    ) -> Result<StepProjectionReport, StoreError> {
        let mut writer = self.model_step_writer(session, lineage_id, now_ms)?;
        for frame in records {
            writer.push(frame)?;
        }
        writer.finish()
    }

    /// A writer for `session`'s step facts that continues from the stored checkpoint when it
    /// names `lineage_id`, and from an empty fold otherwise. Feed it records in WAL order with
    /// [`ModelStepWriter::push`] and end with [`ModelStepWriter::finish`].
    pub fn model_step_writer(
        &self,
        session: &BindIdentity,
        lineage_id: &str,
        now_ms: u64,
    ) -> Result<ModelStepWriter<'_>, StoreError> {
        let key = Self::key(session);
        let stored: Option<(String, u64, String)> = self
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT lineage_id, seq, fold_json FROM model_step_projection WHERE session=?1",
                [&key],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get::<_, i64>(1)?.max(0) as u64,
                        row.get(2)?,
                    ))
                },
            )
            .optional()?;
        let expected = stored
            .as_ref()
            .map(|(lineage, seq, _)| ModelStepCheckpoint {
                lineage_id: lineage.clone(),
                seq: *seq,
            });
        let (after, fold) = match stored {
            Some((lineage, seq, fold_json)) if lineage == lineage_id => {
                (seq, serde_json::from_str::<ModelStepFold>(&fold_json)?)
            }
            _ => (0, ModelStepFold::default()),
        };
        let backfill_before_ms = self.model_step_backfill_before()?;
        Ok(ModelStepWriter {
            backfill_before_ms,
            store: self,
            session_json: serde_json::to_string(session)?,
            key,
            lineage_id: lineage_id.to_string(),
            identity: StepFactIdentity::new(lineage_id, session),
            expected,
            after,
            last: after,
            fold,
            batch: Vec::new(),
            pending: 0,
            now_ms,
            report: StepProjectionReport {
                through_seq: after,
                ..StepProjectionReport::default()
            },
        })
    }

    // One transaction: the facts of a folded range and the checkpoint past it.
    #[allow(clippy::too_many_arguments)]
    fn commit_model_step_batch(
        &self,
        key: &str,
        session_json: &str,
        lineage_id: &str,
        expected: Option<&ModelStepCheckpoint>,
        through_seq: u64,
        fold: &ModelStepFold,
        batch: Vec<FoldedStep>,
        now_ms: u64,
        backfill_before_ms: Option<u64>,
        report: &mut StepProjectionReport,
    ) -> Result<(), StoreError> {
        let fold_json = serde_json::to_string(fold)?;
        let rows = batch
            .into_iter()
            .map(|step| Ok((serde_json::to_string(&step.fact)?, step)))
            .collect::<Result<Vec<_>, StoreError>>()?;
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        // Another writer moved this session's checkpoint since it was read: what this call
        // folded may no longer follow it. Stop; the caller's next attempt re-reads it.
        let current: Option<ModelStepCheckpoint> = tx
            .query_row(
                "SELECT lineage_id, seq FROM model_step_projection WHERE session=?1",
                [key],
                |row| {
                    Ok(ModelStepCheckpoint {
                        lineage_id: row.get(0)?,
                        seq: row.get::<_, i64>(1)?.max(0) as u64,
                    })
                },
            )
            .optional()?;
        if current.as_ref() != expected {
            return Err(StoreError::InconsistentSnapshot(
                "model step checkpoint moved during projection",
            ));
        }
        let mut written = 0u64;
        let mut imported_run: Option<(String, bool)> = None;
        for (json, step) in rows {
            let run_id = &step.fact.run_id;
            let imported = match &imported_run {
                Some((cached, imported)) if cached == run_id => *imported,
                _ => {
                    let imported: bool = tx.query_row(
                        "SELECT EXISTS (SELECT 1 FROM export_imported_runs
                                        WHERE session=?1 AND run_id=?2)",
                        params![session_json, run_id],
                        |row| row.get(0),
                    )?;
                    imported_run = Some((run_id.clone(), imported));
                    imported
                }
            };
            if imported {
                report.imported += 1;
                continue;
            }
            let basis = step.age_basis_ms.unwrap_or(now_ms);
            if model_step_expired(basis, now_ms) {
                report.expired += 1;
                continue;
            }
            // HISTORY GOES THROUGH THE BACKLOG, whichever path derived it: a step that finished
            // before this writer's first start (or carries no time at all) is backfill, and is
            // published later at the capped rate. A step that finished since is live and is
            // written now, so it never waits behind the backfill.
            let history = backfill_before_ms
                .is_some_and(|cutoff| step.age_basis_ms.is_none_or(|basis| basis < cutoff));
            if history {
                let queued = tx.execute(
                    "INSERT OR IGNORE INTO model_step_backlog (fact_id, segment_json, basis_ms)
                     SELECT ?1, ?2, ?3
                      WHERE NOT EXISTS (SELECT 1 FROM export_facts WHERE fact_id=?1)",
                    params![step.fact.fact_id, json, int(basis)?],
                )?;
                if queued == 0 {
                    report.already_present += 1;
                } else {
                    report.queued += 1;
                }
                continue;
            }
            // The SQL run_id column stays empty: see the module comment (never spend).
            let inserted = tx.execute(
                "INSERT OR IGNORE INTO export_facts (fact_id, run_id, segment_json)
                 VALUES (?1, '', ?2)",
                params![step.fact.fact_id, json],
            )?;
            if inserted == 0 {
                report.already_present += 1;
                continue;
            }
            tx.execute(
                "INSERT INTO export_fact_expiry (export_seq, expires_at_ms) VALUES (?1, ?2)",
                params![tx.last_insert_rowid(), int(model_step_expires_at(basis))?],
            )?;
            written += 1;
        }
        #[cfg(test)]
        if MODEL_STEP_COMMIT_HOOK.with(|hook| hook.replace(false)) {
            return Err(StoreError::InconsistentSnapshot(
                "injected failure before the model step checkpoint",
            ));
        }
        tx.execute(
            "INSERT INTO model_step_projection (session, lineage_id, seq, fold_json)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(session) DO UPDATE SET
                 lineage_id=excluded.lineage_id, seq=excluded.seq, fold_json=excluded.fold_json",
            params![key, lineage_id, int(through_seq)?, fold_json],
        )?;
        let high_water: i64 = if written > 0 {
            let high_water = tx.query_row(
                "SELECT COALESCE(MAX(export_seq), 0) FROM export_facts",
                [],
                |row| row.get(0),
            )?;
            tx.execute(
                "UPDATE export_meta SET high_water=?1 WHERE id=1",
                [high_water],
            )?;
            high_water
        } else {
            0
        };
        tx.commit()?;
        drop(conn);
        if written > 0 {
            self.export_high_water
                .fetch_max(high_water.max(0) as u64, Ordering::Release);
        }
        report.written += written;
        Ok(())
    }

    /// Record, once, when this writer first started: steps that finished before `now_ms` are
    /// history from then on. Later calls keep the first value. Returns the value in force.
    pub fn begin_model_step_backfill(&self, now_ms: u64) -> Result<u64, StoreError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO model_step_meta (id, backfill_before_ms, publish_tat_us)
             VALUES (1, ?1, 0)",
            [int(now_ms)?],
        )?;
        let before: i64 = conn.query_row(
            "SELECT backfill_before_ms FROM model_step_meta WHERE id=1",
            [],
            |row| row.get(0),
        )?;
        Ok(before.max(0) as u64)
    }

    fn model_step_backfill_before(&self) -> Result<Option<u64>, StoreError> {
        Ok(self
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT backfill_before_ms FROM model_step_meta WHERE id=1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .map(|before| before.max(0) as u64))
    }

    /// Backfilled step facts not yet published.
    pub fn model_step_backlog_len(&self) -> Result<u64, StoreError> {
        let count: i64 = self.conn.lock().unwrap().query_row(
            "SELECT COUNT(*) FROM model_step_backlog",
            [],
            |row| row.get(0),
        )?;
        Ok(count.max(0) as u64)
    }

    /// Publish as many backlog rows into `export_facts` as the pace allows at `now_ms` (at most
    /// [`PUBLISH_BURST`]), oldest first, in one short transaction that also moves the stored pace.
    /// Call it often (the daemon calls it every 250 ms); a call the pace does not allow writes
    /// nothing. A row past its 30 days is dropped rather than published, and does not count
    /// against the pace.
    pub fn publish_model_step_backlog(
        &self,
        now_ms: u64,
    ) -> Result<BacklogPublishReport, StoreError> {
        let mut report = BacklogPublishReport::default();
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let Some(tat_us) = tx
            .query_row(
                "SELECT publish_tat_us FROM model_step_meta WHERE id=1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
        else {
            return Ok(report);
        };
        let tat_us = tat_us.max(0) as u64;
        let now_us = now_ms.saturating_mul(1000);
        let allowed = publishable_rows(tat_us, now_us);
        if allowed == 0 {
            return Ok(report);
        }
        let due: Vec<(i64, String, String, i64)> = tx
            .prepare(
                "SELECT seq, fact_id, segment_json, basis_ms FROM model_step_backlog
                  ORDER BY seq LIMIT ?1",
            )?
            .query_map([int(allowed)?], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })?
            .collect::<Result<_, _>>()?;
        for (seq, fact_id, json, basis_ms) in due {
            tx.execute("DELETE FROM model_step_backlog WHERE seq=?1", [seq])?;
            let basis = basis_ms.max(0) as u64;
            if model_step_expired(basis, now_ms) {
                report.expired += 1;
                continue;
            }
            // Empty SQL run_id, as for every step fact: see the module comment (never spend).
            if tx.execute(
                "INSERT OR IGNORE INTO export_facts (fact_id, run_id, segment_json)
                 VALUES (?1, '', ?2)",
                params![fact_id, json],
            )? == 0
            {
                continue;
            }
            tx.execute(
                "INSERT INTO export_fact_expiry (export_seq, expires_at_ms) VALUES (?1, ?2)",
                params![tx.last_insert_rowid(), int(model_step_expires_at(basis))?],
            )?;
            report.published += 1;
        }
        let tat_us = tat_us
            .max(now_us)
            .saturating_add(report.published.saturating_mul(PUBLISH_INTERVAL_US));
        tx.execute(
            "UPDATE model_step_meta SET publish_tat_us=?1 WHERE id=1",
            [int(tat_us)?],
        )?;
        let high_water: i64 = tx.query_row(
            "SELECT COALESCE(MAX(export_seq), 0) FROM export_facts",
            [],
            |row| row.get(0),
        )?;
        if report.published > 0 {
            tx.execute(
                "UPDATE export_meta SET high_water=?1 WHERE id=1",
                [high_water],
            )?;
        }
        tx.commit()?;
        drop(conn);
        if report.published > 0 {
            self.export_high_water
                .fetch_max(high_water.max(0) as u64, Ordering::Release);
        }
        Ok(report)
    }

    /// Delete at most `limit` model step facts whose lifetime ended at or before `now_ms`, in
    /// one short transaction. Returns how many were deleted; call again while it returns
    /// `limit`.
    ///
    /// Only rows listed in `export_fact_expiry` are candidates, and a candidate whose stored
    /// fact is not `kind: "model_step"` is left in place even then: a session fact is billing
    /// and is never deleted, whatever a bookkeeping row says.
    pub fn expire_model_step_facts(&self, now_ms: u64, limit: u64) -> Result<u64, StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let due: Vec<i64> = tx
            .prepare(
                "SELECT export_seq FROM export_fact_expiry
                  WHERE expires_at_ms <= ?1 ORDER BY expires_at_ms LIMIT ?2",
            )?
            .query_map(params![int(now_ms)?, int(limit)?], |row| row.get(0))?
            .collect::<Result<_, _>>()?;
        let mut deleted = 0;
        for export_seq in &due {
            deleted += tx.execute(
                "DELETE FROM export_facts WHERE export_seq=?1
                    AND json_extract(segment_json, '$.kind')='model_step'",
                [export_seq],
            )? as u64;
            tx.execute(
                "DELETE FROM export_fact_expiry WHERE export_seq=?1",
                [export_seq],
            )?;
        }
        tx.commit()?;
        Ok(deleted)
    }

    pub fn model_step_walk(&self) -> Result<Option<ModelStepWalk>, StoreError> {
        Ok(self
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT since_ms, started_ms, build_rowid, done FROM model_step_walk WHERE id=1",
                [],
                |row| {
                    Ok(ModelStepWalk {
                        since_ms: row.get::<_, i64>(0)?.max(0) as u64,
                        started_ms: row.get::<_, i64>(1)?.max(0) as u64,
                        build_rowid: row.get(2)?,
                        done: row.get(3)?,
                    })
                },
            )
            .optional()?)
    }

    pub fn save_model_step_walk(&self, walk: &ModelStepWalk) -> Result<(), StoreError> {
        self.conn.lock().unwrap().execute(
            "INSERT INTO model_step_walk (id, since_ms, started_ms, build_rowid, done)
             VALUES (1, ?1, ?2, ?3, ?4)
             ON CONFLICT(id) DO UPDATE SET since_ms=excluded.since_ms,
                 started_ms=excluded.started_ms, build_rowid=excluded.build_rowid,
                 done=excluded.done",
            params![
                int(walk.since_ms)?,
                int(walk.started_ms)?,
                walk.build_rowid,
                walk.done
            ],
        )?;
        Ok(())
    }

    /// Begin a new walk's queue: forget what an earlier walk left in it.
    pub fn clear_model_step_walk_queue(&self) -> Result<(), StoreError> {
        self.conn
            .lock()
            .unwrap()
            .execute("DELETE FROM model_step_walk_queue", [])?;
        Ok(())
    }

    /// Add to the walk's queue every session that has a run in `run_index` rows
    /// `(after_rowid, after_rowid + rows]` whose state changed at or after `since_ms`, or that
    /// is still active or paused (it may have committed steps just before a crash). One short
    /// transaction per call. Returns the rowid to continue after, or `None` when the table has
    /// been covered.
    pub fn fill_model_step_walk_queue(
        &self,
        since_ms: u64,
        after_rowid: i64,
        rows: i64,
    ) -> Result<Option<i64>, StoreError> {
        let conn = self.conn.lock().unwrap();
        let through = after_rowid.saturating_add(rows);
        conn.execute(
            "INSERT OR IGNORE INTO model_step_walk_queue (session)
             SELECT session FROM run_index
              WHERE rowid > ?1 AND rowid <= ?2
                AND (state_changed_ms >= ?3 OR state IN ('active','paused'))",
            params![after_rowid, through, int(since_ms)?],
        )?;
        let max: i64 =
            conn.query_row("SELECT COALESCE(MAX(rowid), 0) FROM run_index", [], |row| {
                row.get(0)
            })?;
        Ok((through < max).then_some(through))
    }

    /// How many sessions the walk's queue still holds.
    pub fn model_step_walk_queue_len(&self) -> Result<u64, StoreError> {
        let count: i64 = self.conn.lock().unwrap().query_row(
            "SELECT COUNT(*) FROM model_step_walk_queue",
            [],
            |row| row.get(0),
        )?;
        Ok(count.max(0) as u64)
    }

    /// The next `limit` sessions of the walk's queue, as `(queue key, session)` in key order.
    pub fn model_step_walk_page(
        &self,
        limit: u64,
    ) -> Result<Vec<(String, BindIdentity)>, StoreError> {
        let encoded: Vec<String> = self
            .conn
            .lock()
            .unwrap()
            .prepare("SELECT session FROM model_step_walk_queue ORDER BY session LIMIT ?1")?
            .query_map([int(limit)?], |row| row.get(0))?
            .collect::<Result<_, _>>()?;
        encoded
            .into_iter()
            .map(|json| {
                let session: BindIdentity = serde_json::from_str(&json)?;
                Ok((json, session))
            })
            .collect()
    }

    /// Remove visited sessions (by queue key) from the walk's queue, in one transaction.
    pub fn model_step_walk_visited(&self, keys: &[String]) -> Result<(), StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        for key in keys {
            tx.execute("DELETE FROM model_step_walk_queue WHERE session=?1", [key])?;
        }
        tx.commit()?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "model_step_facts_tests.rs"]
mod tests;
