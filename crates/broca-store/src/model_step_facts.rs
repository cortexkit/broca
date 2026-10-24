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
             id         INTEGER PRIMARY KEY CHECK (id = 1),
             since_ms   INTEGER NOT NULL,
             started_ms INTEGER NOT NULL,
             cursor     TEXT NOT NULL,
             done       INTEGER NOT NULL
         );",
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
    /// The checkpoint after the call; zero when nothing was projected.
    pub through_seq: u64,
}

/// The persisted walk state; see `model_step_walk`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelStepWalk {
    /// Sessions with a run whose state changed at or after this time are visited.
    pub since_ms: u64,
    /// When the walk began; the next walk starts from here (less a margin).
    pub started_ms: u64,
    /// The store key of the last session visited; the walk resumes after it.
    pub cursor: String,
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
    /// The store key `model_step_projection` and the walk cursor use for `session`.
    pub fn model_step_session_key(session: &BindIdentity) -> String {
        Self::key(session)
    }

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
        let mut expected = stored
            .as_ref()
            .map(|(lineage, seq, _)| (lineage.clone(), *seq));
        let (after, mut fold) = match stored {
            Some((lineage, seq, fold_json)) if lineage == lineage_id => {
                (seq, serde_json::from_str::<ModelStepFold>(&fold_json)?)
            }
            _ => (0, ModelStepFold::default()),
        };
        let pending: Vec<&FramedRecord> =
            records.iter().filter(|frame| frame.seq > after).collect();
        let mut report = StepProjectionReport {
            through_seq: after,
            ..StepProjectionReport::default()
        };
        if pending.is_empty() {
            return Ok(report);
        }
        let continues = after == 0 || pending[0].seq == after + 1;
        if !continues
            || pending
                .windows(2)
                .any(|pair| pair[1].seq != pair[0].seq + 1)
        {
            return Err(StoreError::InconsistentSnapshot(
                "model step projection range has a gap after its checkpoint",
            ));
        }

        let identity = StepFactIdentity::new(lineage_id, session);
        let session_json = serde_json::to_string(session)?;
        let mut batch: Vec<FoldedStep> = Vec::new();
        let mut folded = 0usize;
        for (index, frame) in pending.iter().enumerate() {
            batch.extend(fold.apply(frame, &identity));
            folded += 1;
            let last = index + 1 == pending.len();
            if last || batch.len() >= FACTS_PER_TRANSACTION || folded >= RECORDS_PER_TRANSACTION {
                self.commit_model_step_batch(
                    &key,
                    &session_json,
                    lineage_id,
                    expected.as_ref(),
                    frame.seq,
                    &fold,
                    std::mem::take(&mut batch),
                    now_ms,
                    &mut report,
                )?;
                expected = Some((lineage_id.to_string(), frame.seq));
                report.through_seq = frame.seq;
                folded = 0;
            }
        }
        Ok(report)
    }

    // One transaction: the facts of a folded range and the checkpoint past it.
    #[allow(clippy::too_many_arguments)]
    fn commit_model_step_batch(
        &self,
        key: &str,
        session_json: &str,
        lineage_id: &str,
        expected: Option<&(String, u64)>,
        through_seq: u64,
        fold: &ModelStepFold,
        batch: Vec<FoldedStep>,
        now_ms: u64,
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
        let current: Option<(String, u64)> = tx
            .query_row(
                "SELECT lineage_id, seq FROM model_step_projection WHERE session=?1",
                [key],
                |row| Ok((row.get(0)?, row.get::<_, i64>(1)?.max(0) as u64)),
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
                "SELECT since_ms, started_ms, cursor, done FROM model_step_walk WHERE id=1",
                [],
                |row| {
                    Ok(ModelStepWalk {
                        since_ms: row.get::<_, i64>(0)?.max(0) as u64,
                        started_ms: row.get::<_, i64>(1)?.max(0) as u64,
                        cursor: row.get(2)?,
                        done: row.get(3)?,
                    })
                },
            )
            .optional()?)
    }

    pub fn save_model_step_walk(&self, walk: &ModelStepWalk) -> Result<(), StoreError> {
        self.conn.lock().unwrap().execute(
            "INSERT INTO model_step_walk (id, since_ms, started_ms, cursor, done)
             VALUES (1, ?1, ?2, ?3, ?4)
             ON CONFLICT(id) DO UPDATE SET since_ms=excluded.since_ms,
                 started_ms=excluded.started_ms, cursor=excluded.cursor, done=excluded.done",
            params![
                int(walk.since_ms)?,
                int(walk.started_ms)?,
                walk.cursor,
                walk.done
            ],
        )?;
        Ok(())
    }

    /// Sessions a walk visits, as `(store key, session)` sorted by key: every session with a run
    /// whose state changed at or after `since_ms`, and every session with a run still active or
    /// paused (one that may have committed steps just before a crash).
    pub fn model_step_walk_sessions(
        &self,
        since_ms: u64,
    ) -> Result<Vec<(String, BindIdentity)>, StoreError> {
        let encoded: Vec<String> = self
            .conn
            .lock()
            .unwrap()
            .prepare(
                "SELECT DISTINCT session FROM run_index
                  WHERE state_changed_ms >= ?1 OR state IN ('active','paused')",
            )?
            .query_map([int(since_ms)?], |row| row.get(0))?
            .collect::<Result<_, _>>()?;
        let mut sessions = encoded
            .iter()
            .map(|json| {
                let session: BindIdentity = serde_json::from_str(json)?;
                Ok((Self::key(&session), session))
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        sessions.sort_by(|left, right| left.0.cmp(&right.0));
        sessions.dedup_by(|left, right| left.0 == right.0);
        Ok(sessions)
    }
}

#[cfg(test)]
#[path = "model_step_facts_tests.rs"]
mod tests;
