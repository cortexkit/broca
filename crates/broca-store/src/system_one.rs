//! The durable batch record of System One, broca's one-pass decision and extraction models.
//!
//! A model-batch call (`classify.run`, later `extract.run`) admits a batch under a caller's
//! `batch_id` and records each item's outcome before it replies, so a re-send of the same
//! batch serves the stored outcomes and asks the provider only for items still needing one.
//! This file is the authority for those batches: nothing else can rebuild it. That is why it
//! is its own SQLite file (`system-one.db`) rather than tables in `run-index.db`, which is a
//! projection of the session WAL that broca may delete and rebuild at any time. Nothing here
//! reads or writes a session, a lease or a WAL.
//!
//! Two tables:
//!
//! - `batch`: one row per admitted batch, keyed by role and `batch_id`. It holds the body
//!   identity the batch was admitted with (the role's own digest structure, as JSON), the
//!   catalog facts frozen at admission, the spend ceiling, the spend recorded so far, and the
//!   single-flight state: `running` with the owning process and its start time, or `idle`.
//! - `item_outcome`: one row per item that has a stored outcome, either the provider's answer
//!   text exactly as it came, or an item error.
//!
//! Every write is one short `IMMEDIATE` transaction on the one writer connection, which uses
//! the store's shared busy handler. The file uses SQLite's rollback journal rather than its
//! WAL mode: the store has one connection and no concurrent readers, so WAL mode would buy
//! nothing, and a rollback journal leaves no `-wal` sidecar that could be mistaken for a
//! session log.

use std::path::Path;
use std::sync::Mutex;

use rusqlite::{Connection, OptionalExtension, TransactionBehavior};

/// How long a batch and its outcomes are kept after the batch was first admitted. A re-send
/// after this is a new batch and is sent to the provider (and billed) again.
pub const RETENTION_MS: i64 = 7 * 24 * 60 * 60 * 1000;

#[derive(Debug)]
pub struct SystemOneStoreError(pub String);

impl std::fmt::Display for SystemOneStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SystemOneStoreError {}

impl From<rusqlite::Error> for SystemOneStoreError {
    fn from(error: rusqlite::Error) -> Self {
        Self(error.to_string())
    }
}

type Result<T> = std::result::Result<T, SystemOneStoreError>;

/// What a call presents when it asks to hold a batch.
#[derive(Clone, Debug)]
pub struct BatchClaim<'a> {
    /// The role the batch belongs to (`classifier`, `extractor`): batch ids are the caller's,
    /// so two roles may use the same one.
    pub role: &'a str,
    pub batch_id: &'a str,
    pub model: &'a str,
    /// The role's body identity as JSON, recorded at first admission.
    pub identity: &'a str,
    /// A digest of `identity`; equal digests mean the same body.
    pub body_digest: &'a str,
    /// The catalog facts the batch is priced and checked by, frozen at first admission.
    pub frozen_row: &'a str,
    /// The ceiling this call sent, in nanodollars (rounded down). `None` sends no ceiling.
    pub ceiling_nanos: Option<u64>,
    /// The process holding the batch while this call runs.
    pub owner: &'a str,
    pub now_ms: i64,
}

/// A batch as recorded, at the moment a call claimed it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BatchRecord {
    pub model: String,
    pub identity: String,
    pub frozen_row: String,
    /// The recorded ceiling after this call's claim: the lower of the recorded one and the
    /// call's own.
    pub ceiling_nanos: Option<u64>,
    pub spent_nanos: u64,
    pub admitted_at_ms: i64,
    pub outcomes: Vec<StoredOutcome>,
}

/// How a claim went.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Claimed {
    /// No batch had this id: it is now recorded as running, with this call's ceiling.
    Fresh,
    /// The same body was recorded before: it is now running under this call, and its ceiling
    /// is tightened to this call's when that is lower. Nothing else changed.
    Resumed(BatchRecord),
    /// Another process holds the batch. Nothing was written.
    InProgress,
    /// The id names a batch with a different body. Nothing was written; the recorded identity
    /// is returned so the caller can name the first field that differs.
    Reuse { recorded_identity: String },
}

/// One item's stored outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredOutcome {
    pub index: u64,
    pub outcome: Outcome,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The provider's answers, as the exact JSON text it sent.
    Answered(String),
    /// An item error, as its wire JSON.
    Failed(String),
}

/// What the retention sweep removed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SweepCounts {
    pub batches: usize,
    pub outcomes: usize,
}

pub struct SystemOneStore {
    conn: Mutex<Connection>,
}

fn to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn to_u64(value: Option<i64>) -> Option<u64> {
    value.map(|value| u64::try_from(value).unwrap_or(0))
}

impl SystemOneStore {
    /// Open (creating when absent) the store file, owner-only.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        #[cfg(windows)]
        super::prepare_private_sqlite(path).map_err(|e| SystemOneStoreError(e.to_string()))?;
        #[cfg(unix)]
        {
            let mut options = broca_owner_only::OpenOptions::new();
            options.write(true).create(true).truncate(false);
            options
                .open(path)
                .map_err(|e| SystemOneStoreError(format!("create {}: {e}", path.display())))?;
        }
        super::sqlite_open_observed();
        Self::from_connection(Connection::open(path)?)
    }

    pub fn open_in_memory() -> Result<Self> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(conn: Connection) -> Result<Self> {
        crate::busy::configure_writer_connection(&conn)?;
        conn.execute_batch(
            "PRAGMA journal_mode=DELETE;
             PRAGMA synchronous=FULL;
             CREATE TABLE IF NOT EXISTS batch (
                 role TEXT NOT NULL,
                 batch_id TEXT NOT NULL,
                 model TEXT NOT NULL,
                 identity TEXT NOT NULL,
                 body_digest TEXT NOT NULL,
                 frozen_row TEXT NOT NULL,
                 ceiling_nanos INTEGER,
                 spent_nanos INTEGER NOT NULL DEFAULT 0,
                 state TEXT NOT NULL CHECK (state IN ('idle', 'running')),
                 owner TEXT,
                 started_at_ms INTEGER,
                 admitted_at_ms INTEGER NOT NULL,
                 PRIMARY KEY (role, batch_id)
             );
             CREATE INDEX IF NOT EXISTS batch_admitted ON batch (admitted_at_ms);
             CREATE TABLE IF NOT EXISTS item_outcome (
                 role TEXT NOT NULL,
                 batch_id TEXT NOT NULL,
                 item_index INTEGER NOT NULL,
                 kind TEXT NOT NULL CHECK (kind IN ('answered', 'failed')),
                 body TEXT NOT NULL,
                 input_tokens INTEGER,
                 output_tokens INTEGER,
                 recorded_at_ms INTEGER NOT NULL,
                 PRIMARY KEY (role, batch_id, item_index)
             );",
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.conn
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Release every batch a previous process left `running`. Called once at startup, before
    /// any call is served: the process that held them is gone, so no call holds them now, and
    /// their unanswered items are asked again on the next re-send. Returns how many it released.
    pub fn release_abandoned(&self) -> Result<usize> {
        let conn = self.lock();
        Ok(conn.execute(
            "UPDATE batch SET state = 'idle', owner = NULL, started_at_ms = NULL
              WHERE state = 'running'",
            [],
        )?)
    }

    /// Hold a batch for one call. See [`Claimed`] for the outcomes; only `Fresh` and `Resumed`
    /// write, and each in one transaction, so the tightened ceiling is durable before the call
    /// sends anything.
    pub fn claim(&self, claim: &BatchClaim<'_>) -> Result<Claimed> {
        let mut conn = self.lock();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing = tx
            .query_row(
                "SELECT model, identity, body_digest, frozen_row, ceiling_nanos, spent_nanos,
                        state, admitted_at_ms
                   FROM batch WHERE role = ?1 AND batch_id = ?2",
                rusqlite::params![claim.role, claim.batch_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<i64>>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, i64>(7)?,
                    ))
                },
            )
            .optional()?;
        let Some((model, identity, digest, frozen_row, ceiling, spent, state, admitted)) = existing
        else {
            tx.execute(
                "INSERT INTO batch (role, batch_id, model, identity, body_digest, frozen_row,
                                    ceiling_nanos, spent_nanos, state, owner, started_at_ms,
                                    admitted_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0, 'running', ?8, ?9, ?9)",
                rusqlite::params![
                    claim.role,
                    claim.batch_id,
                    claim.model,
                    claim.identity,
                    claim.body_digest,
                    claim.frozen_row,
                    claim.ceiling_nanos.map(to_i64),
                    claim.owner,
                    claim.now_ms,
                ],
            )?;
            tx.commit()?;
            return Ok(Claimed::Fresh);
        };
        if digest != claim.body_digest {
            return Ok(Claimed::Reuse {
                recorded_identity: identity,
            });
        }
        if state == "running" {
            return Ok(Claimed::InProgress);
        }
        // A caller can tighten the ceiling and never loosen it: the recorded ceiling after
        // this call is the lower of the two.
        let recorded = to_u64(ceiling);
        let ceiling = match (recorded, claim.ceiling_nanos) {
            (Some(recorded), Some(sent)) => Some(recorded.min(sent)),
            (one, None) | (None, one) => one,
        };
        tx.execute(
            "UPDATE batch SET ceiling_nanos = ?3, state = 'running', owner = ?4,
                              started_at_ms = ?5
              WHERE role = ?1 AND batch_id = ?2",
            rusqlite::params![
                claim.role,
                claim.batch_id,
                ceiling.map(to_i64),
                claim.owner,
                claim.now_ms
            ],
        )?;
        let outcomes = read_outcomes(&tx, claim.role, claim.batch_id)?;
        tx.commit()?;
        Ok(Claimed::Resumed(BatchRecord {
            model,
            identity,
            frozen_row,
            ceiling_nanos: ceiling,
            spent_nanos: to_u64(Some(spent)).unwrap_or(0),
            admitted_at_ms: admitted,
            outcomes,
        }))
    }

    /// Record one item's outcome and add `spend_nanos` to the batch's recorded spend, in one
    /// transaction, only while `owner` still holds the batch. Returns false (and writes
    /// nothing) when it does not.
    pub fn record(
        &self,
        role: &str,
        batch_id: &str,
        owner: &str,
        outcome: &StoredOutcome,
        spend_nanos: u64,
        now_ms: i64,
    ) -> Result<bool> {
        let mut conn = self.lock();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let held = tx.execute(
            "UPDATE batch SET spent_nanos = MIN(spent_nanos + ?4, 9223372036854775807)
              WHERE role = ?1 AND batch_id = ?2 AND state = 'running' AND owner = ?3",
            rusqlite::params![role, batch_id, owner, to_i64(spend_nanos)],
        )?;
        if held == 0 {
            return Ok(false);
        }
        let (kind, body) = match &outcome.outcome {
            Outcome::Answered(text) => ("answered", text),
            Outcome::Failed(text) => ("failed", text),
        };
        tx.execute(
            "INSERT INTO item_outcome (role, batch_id, item_index, kind, body, input_tokens,
                                       output_tokens, recorded_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT (role, batch_id, item_index) DO UPDATE SET
                 kind = excluded.kind, body = excluded.body,
                 input_tokens = excluded.input_tokens, output_tokens = excluded.output_tokens,
                 recorded_at_ms = excluded.recorded_at_ms",
            rusqlite::params![
                role,
                batch_id,
                to_i64(outcome.index),
                kind,
                body,
                outcome.input_tokens.map(to_i64),
                outcome.output_tokens.map(to_i64),
                now_ms,
            ],
        )?;
        tx.commit()?;
        Ok(true)
    }

    /// End `owner`'s hold on a batch; its record stays.
    pub fn release(&self, role: &str, batch_id: &str, owner: &str) -> Result<()> {
        self.lock().execute(
            "UPDATE batch SET state = 'idle', owner = NULL, started_at_ms = NULL
              WHERE role = ?1 AND batch_id = ?2 AND owner = ?3",
            rusqlite::params![role, batch_id, owner],
        )?;
        Ok(())
    }

    /// Remove a batch this call created when the call ends up refused, so the refusal writes
    /// nothing. Only a batch `owner` holds with no stored outcome is removed.
    pub fn discard_fresh(&self, role: &str, batch_id: &str, owner: &str) -> Result<()> {
        let mut conn = self.lock();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let outcomes: i64 = tx.query_row(
            "SELECT COUNT(*) FROM item_outcome WHERE role = ?1 AND batch_id = ?2",
            rusqlite::params![role, batch_id],
            |row| row.get(0),
        )?;
        if outcomes == 0 {
            tx.execute(
                "DELETE FROM batch WHERE role = ?1 AND batch_id = ?2 AND owner = ?3",
                rusqlite::params![role, batch_id, owner],
            )?;
        } else {
            tx.execute(
                "UPDATE batch SET state = 'idle', owner = NULL, started_at_ms = NULL
                  WHERE role = ?1 AND batch_id = ?2 AND owner = ?3",
                rusqlite::params![role, batch_id, owner],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Delete every batch first admitted at or before `now_ms - RETENTION_MS`, with its
    /// outcomes. A batch a call holds right now is left for the next sweep, so a running call
    /// never loses the record it is writing to.
    pub fn sweep(&self, now_ms: i64) -> Result<SweepCounts> {
        let cutoff = now_ms.saturating_sub(RETENTION_MS);
        let mut conn = self.lock();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let outcomes = tx.execute(
            "DELETE FROM item_outcome WHERE (role, batch_id) IN
                 (SELECT role, batch_id FROM batch
                   WHERE admitted_at_ms <= ?1 AND state = 'idle')",
            [cutoff],
        )?;
        let batches = tx.execute(
            "DELETE FROM batch WHERE admitted_at_ms <= ?1 AND state = 'idle'",
            [cutoff],
        )?;
        tx.commit()?;
        Ok(SweepCounts { batches, outcomes })
    }

    /// The stored outcomes of a batch, in item order. Empty when it has none or is unknown.
    pub fn outcomes(&self, role: &str, batch_id: &str) -> Result<Vec<StoredOutcome>> {
        let conn = self.lock();
        read_outcomes(&conn, role, batch_id)
    }

    /// The recorded ceiling and spend of a batch, when it is recorded.
    pub fn ledger(&self, role: &str, batch_id: &str) -> Result<Option<(Option<u64>, u64)>> {
        let conn = self.lock();
        Ok(conn
            .query_row(
                "SELECT ceiling_nanos, spent_nanos FROM batch WHERE role = ?1 AND batch_id = ?2",
                rusqlite::params![role, batch_id],
                |row| {
                    Ok((
                        to_u64(row.get::<_, Option<i64>>(0)?),
                        to_u64(Some(row.get::<_, i64>(1)?)).unwrap_or(0),
                    ))
                },
            )
            .optional()?)
    }
}

fn read_outcomes(conn: &Connection, role: &str, batch_id: &str) -> Result<Vec<StoredOutcome>> {
    let mut statement = conn.prepare(
        "SELECT item_index, kind, body, input_tokens, output_tokens FROM item_outcome
          WHERE role = ?1 AND batch_id = ?2 ORDER BY item_index",
    )?;
    let rows = statement.query_map(rusqlite::params![role, batch_id], |row| {
        let kind: String = row.get(1)?;
        let body: String = row.get(2)?;
        Ok(StoredOutcome {
            index: to_u64(Some(row.get::<_, i64>(0)?)).unwrap_or(0),
            outcome: if kind == "answered" {
                Outcome::Answered(body)
            } else {
                Outcome::Failed(body)
            },
            input_tokens: to_u64(row.get::<_, Option<i64>>(3)?),
            output_tokens: to_u64(row.get::<_, Option<i64>>(4)?),
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

#[cfg(test)]
#[path = "system_one_tests.rs"]
mod tests;
