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

use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use rusqlite::{Connection, OptionalExtension, TransactionBehavior};
use sha2::{Digest, Sha256};

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

/// Attribution supplied by a daemon-stamped route, never by the batch body. A flow is
/// deliberately a separate identity: its owner's agent must not receive its spend.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum BatchScope {
    Flow { flow_id: String },
    Run { agent_id: String, run_id: String },
}

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
    /// The role's replay digest: classifier hashes `identity`, while `llm.classify` hashes
    /// its validated arguments, including the caller's ceiling.
    pub body_digest: &'a str,
    /// The catalog facts the batch is priced and checked by, frozen at first admission.
    pub frozen_row: &'a str,
    pub scope: &'a BatchScope,
    /// The ceiling this call sent, in nanodollars (rounded down). `None` sends no ceiling.
    pub ceiling_nanos: Option<u64>,
    /// The process holding the batch while this call runs. Use a fresh launch identity, never
    /// a PID alone or an identity reused by a later process.
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

/// A read-only snapshot of a batch and its stored outcomes. Looking it up neither claims the
/// batch nor changes its state, so a re-sent call can be compared with what was recorded
/// before anything is written.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredBatch {
    pub model: String,
    /// The scope the batch was first admitted under. `None` for a row written before batches
    /// recorded a scope; such a row matches no caller.
    pub scope: Option<BatchScope>,
    /// The role's body identity as JSON, as recorded at first admission.
    pub identity: String,
    /// The request digest recorded at first admission (the claim's `body_digest`). The
    /// `llm.classify` tool records its `args_digest` here.
    pub args_digest: String,
    /// The catalog facts frozen at first admission, as JSON.
    pub frozen_row: String,
    pub ceiling_nanos: Option<u64>,
    pub spent_nanos: u64,
    /// The time the batch's seven-day retention runs from.
    pub admitted_at_ms: i64,
    /// Whether a call holds the batch right now.
    pub running: bool,
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
    /// A retry keeps its first scope so stored outcomes and spend cannot be assigned to
    /// a different caller, even when the request body matches.
    ScopeMismatch,
}

/// One item's stored outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredOutcome {
    pub index: u64,
    pub outcome: Outcome,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
}

/// Credential and billing identity resolved for the attempt that actually answered, not a
/// later vault lookup that might select another account during outbox recovery.
pub struct FactContext {
    pub source_instance_id: String,
    pub provider: String,
    pub model: String,
    pub auth_selection: Option<String>,
    pub account_ref: Option<String>,
    pub charge_basis: broca_core::ChargeBasis,
    pub charge_basis_mapping_version: u32,
    pub charge_basis_overrides: String,
}

pub struct PaidRecord<'a> {
    pub spend_nanos: u64,
    pub now_ms: i64,
    pub fact: Option<&'a FactContext>,
}

pub struct PendingFact {
    pub fact: broca_core::system_one_fact::SystemOneFact,
    pub spend_nanos: u64,
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
    owner_dir: Option<PathBuf>,
    owner_prefix: String,
    /// Each owner keeps its OS lock until the owning store is dropped. An executing call
    /// holds the store alive; killing its process releases the lock without requiring Drop.
    owner_lifetimes: Mutex<HashMap<String, File>>,
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
        let conn = Connection::open(path)?;
        let canonical = path
            .canonicalize()
            .map_err(|error| SystemOneStoreError(error.to_string()))?;
        let name = canonical.file_name().expect("a database file has a name");
        let owner_dir = canonical
            .parent()
            .expect("a canonical path has a parent")
            .to_path_buf();
        let owner_prefix = format!("{}.owner-", name.to_string_lossy());
        Self::from_connection(conn, Some((owner_dir, owner_prefix)))
    }

    pub fn open_in_memory() -> Result<Self> {
        Self::from_connection(Connection::open_in_memory()?, None)
    }

    fn from_connection(
        conn: Connection,
        owner_location: Option<(PathBuf, String)>,
    ) -> Result<Self> {
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
              );
              CREATE TABLE IF NOT EXISTS usage_outbox (
                  fact_id TEXT PRIMARY KEY,
                  fact_json TEXT NOT NULL,
                  spend_nanos INTEGER NOT NULL
              );",
        )?;
        let has_scope = conn.prepare("SELECT scope_json FROM batch").is_ok();
        if !has_scope {
            conn.execute("ALTER TABLE batch ADD COLUMN scope_json TEXT", [])?;
        }
        let (owner_dir, owner_prefix) = match owner_location {
            Some((dir, prefix)) => (Some(dir), prefix),
            None => (None, String::new()),
        };
        Ok(Self {
            conn: Mutex::new(conn),
            owner_dir,
            owner_prefix,
            owner_lifetimes: Mutex::new(HashMap::new()),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.conn
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn owner_path(&self, owner: &str) -> Option<PathBuf> {
        self.owner_dir.as_ref().map(|dir| {
            dir.join(format!(
                "{}{:x}.lock",
                self.owner_prefix,
                Sha256::digest(owner.as_bytes())
            ))
        })
    }

    fn hold_owner_lifetime(&self, owner: &str) -> Result<()> {
        let Some(path) = self.owner_path(owner) else {
            return Ok(());
        };
        let mut held = self
            .owner_lifetimes
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if held.contains_key(owner) {
            return Ok(());
        }
        broca_owner_only::create_dir_all(path.parent().expect("an owner lock has a parent"))
            .map_err(|error| {
                SystemOneStoreError(format!("create batch-owner lock directory: {error}"))
            })?;
        let mut options = broca_owner_only::OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        let file = options
            .open(&path)
            .map_err(|error| SystemOneStoreError(format!("open batch-owner lock: {error}")))?;
        file.try_lock()
            .map_err(|error| SystemOneStoreError(format!("hold batch-owner lifetime: {error}")))?;
        held.insert(owner.into(), file);
        Ok(())
    }

    /// A pre-existing marker with an acquirable OS lock proves that its owner no longer
    /// executes. Missing/unreadable markers and failed lock attempts are unknown, never dead.
    fn dead_owner_proof(path: &Path) -> Option<File> {
        let mut options = broca_owner_only::OpenOptions::new();
        options.read(true).write(true);
        let file = options.open(path).ok()?;
        file.try_lock().ok()?;
        Some(file)
    }

    /// Release only batches whose owner-lifetime locks prove they are abandoned. A live or
    /// unidentifiable owner keeps its claim, so another process receives the transient refusal.
    pub fn release_abandoned(&self) -> Result<usize> {
        let mut conn = self.lock();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let owners = {
            let mut statement = tx.prepare(
                "SELECT DISTINCT owner FROM batch WHERE state='running' AND owner IS NOT NULL",
            )?;
            let owners = statement
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            owners
        };
        let owners = owners
            .into_iter()
            .filter_map(|owner| self.owner_path(&owner).map(|path| (path, owner)))
            .collect::<HashMap<_, _>>();
        let mut paths = owners.keys().cloned().collect::<Vec<_>>();
        // Also collect dead owners whose calls finished normally. Those markers have no
        // running row left, but must be reclaimed just like markers left by a crash.
        if let Some(entries) = self
            .owner_dir
            .as_ref()
            .and_then(|dir| std::fs::read_dir(dir).ok())
        {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let Some(digest) = name
                    .to_str()
                    .and_then(|name| name.strip_prefix(&self.owner_prefix))
                    .and_then(|name| name.strip_suffix(".lock"))
                else {
                    continue;
                };
                if digest.len() == 64
                    && digest
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                    && !owners.contains_key(&entry.path())
                {
                    paths.push(entry.path());
                }
            }
        }
        let mut proofs = Vec::new();
        let mut released = 0;
        for path in paths {
            if let Some(proof) = Self::dead_owner_proof(&path) {
                if let Some(owner) = owners.get(&path) {
                    released += tx.execute(
                        "UPDATE batch SET state='idle', owner=NULL, started_at_ms=NULL WHERE state='running' AND owner=?1",
                        [owner],
                    )?;
                }
                proofs.push((path, proof));
            }
        }
        tx.commit()?;
        for (path, proof) in proofs {
            // Close before unlinking for Windows. Launch identities are never reused, so no
            // future owner can acquire this abandoned identity between close and removal.
            drop(proof);
            match std::fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(SystemOneStoreError(format!(
                        "remove abandoned batch-owner marker: {error}"
                    )))
                }
            }
        }
        Ok(released)
    }

    /// Hold a batch for one call. See [`Claimed`] for the outcomes; only `Fresh` and `Resumed`
    /// write, and each in one transaction, so the tightened ceiling is durable before the call
    /// sends anything. A fresh batch's retention runs from the claim's `now_ms`.
    pub fn claim(&self, claim: &BatchClaim<'_>) -> Result<Claimed> {
        self.claim_admitted_at(claim, claim.now_ms)
    }

    /// [`Self::claim`], with a fresh batch's retention running from `admitted_at_ms` rather
    /// than the claim time. A call admitted earlier and executed later (one held for the
    /// person's consent, say) keeps one seven-day window for everything it stored, counted
    /// from its first admission. A batch already recorded keeps the admission time it was
    /// first recorded with.
    pub fn claim_admitted_at(
        &self,
        claim: &BatchClaim<'_>,
        admitted_at_ms: i64,
    ) -> Result<Claimed> {
        self.hold_owner_lifetime(claim.owner)?;
        let mut conn = self.lock();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing = tx
            .query_row(
                "SELECT model, identity, body_digest, frozen_row, ceiling_nanos, spent_nanos,
                        state, admitted_at_ms, scope_json
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
                        row.get::<_, Option<String>>(8)?,
                    ))
                },
            )
            .optional()?;
        let scope_json = serde_json::to_string(claim.scope)
            .map_err(|error| SystemOneStoreError(error.to_string()))?;
        let Some((model, identity, digest, frozen_row, ceiling, spent, state, admitted, scope)) =
            existing
        else {
            tx.execute(
                "INSERT INTO batch (role, batch_id, model, identity, body_digest, frozen_row,
                                    ceiling_nanos, spent_nanos, state, owner, started_at_ms,
                                     admitted_at_ms, scope_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0, 'running', ?8, ?9, ?10, ?11)",
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
                    admitted_at_ms,
                    scope_json,
                ],
            )?;
            tx.commit()?;
            return Ok(Claimed::Fresh);
        };
        // Legacy unscoped rows are refused too; adopting them would silently assign old work
        // to a new caller. Retention eventually removes them under the ordinary batch rules.
        if scope.as_deref() != Some(scope_json.as_str()) {
            return Ok(Claimed::ScopeMismatch);
        }
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
        self.record_paid(
            role,
            batch_id,
            owner,
            outcome,
            &PaidRecord {
                spend_nanos,
                now_ms,
                fact: None,
            },
        )
    }

    /// Commit the outcome and its export intent together. Publishing to run-index.db happens
    /// later, but a restart can never observe a paid outcome without its measurement.
    pub fn record_paid(
        &self,
        role: &str,
        batch_id: &str,
        owner: &str,
        outcome: &StoredOutcome,
        paid: &PaidRecord<'_>,
    ) -> Result<bool> {
        let PaidRecord {
            spend_nanos,
            now_ms,
            fact,
        } = paid;
        let mut conn = self.lock();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let held = tx.execute(
            "UPDATE batch SET spent_nanos = MIN(spent_nanos + ?4, 9223372036854775807)
              WHERE role = ?1 AND batch_id = ?2 AND state = 'running' AND owner = ?3",
            rusqlite::params![role, batch_id, owner, to_i64(*spend_nanos)],
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
        if let Some(context) = fact {
            use broca_core::system_one_fact::{SystemOneFact, SystemOneUsage};
            use sha2::{Digest, Sha256};
            let (scope, admitted): (String, i64) = tx.query_row(
                "SELECT scope_json, admitted_at_ms FROM batch WHERE role=?1 AND batch_id=?2",
                rusqlite::params![role, batch_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            let scope: BatchScope =
                serde_json::from_str(&scope).map_err(|e| SystemOneStoreError(e.to_string()))?;
            let mut identity = Sha256::new();
            for value in [
                "system_one/v1",
                &context.source_instance_id,
                role,
                batch_id,
                &admitted.to_string(),
                &outcome.index.to_string(),
            ] {
                identity.update((value.len() as u64).to_le_bytes());
                identity.update(value.as_bytes());
            }
            let (run_id, flow_id) = match scope {
                BatchScope::Run { run_id, .. } => (Some(run_id), None),
                BatchScope::Flow { flow_id } => (None, Some(flow_id)),
            };
            let fact = SystemOneFact {
                fact_id: format!("{:x}", identity.finalize()),
                batch_id: batch_id.into(),
                item_index: outcome.index,
                provider: context.provider.clone(),
                model: context.model.clone(),
                auth_selection: context.auth_selection.clone(),
                account_ref: context.account_ref.clone(),
                run_id,
                flow_id,
                charge_basis: context.charge_basis,
                charge_basis_mapping_version: context.charge_basis_mapping_version,
                charge_basis_overrides: context.charge_basis_overrides.clone(),
                usage: SystemOneUsage {
                    input_tokens: outcome.input_tokens,
                    output_tokens: outcome.output_tokens,
                },
                occurred_at_ms: u64::try_from(*now_ms).unwrap_or(0),
            };
            let json =
                serde_json::to_string(&fact).map_err(|e| SystemOneStoreError(e.to_string()))?;
            tx.execute(
                "INSERT INTO usage_outbox (fact_id, fact_json, spend_nanos) VALUES (?1, ?2, ?3)",
                rusqlite::params![fact.fact_id, json, to_i64(*spend_nanos)],
            )?;
        }
        tx.commit()?;
        Ok(true)
    }

    pub fn pending_facts(&self) -> Result<Vec<PendingFact>> {
        let conn = self.lock();
        let mut statement =
            conn.prepare("SELECT fact_json, spend_nanos FROM usage_outbox ORDER BY rowid")?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;
        rows.map(|row| {
            let (json, spend) = row?;
            Ok(PendingFact {
                fact: serde_json::from_str(&json)
                    .map_err(|e| SystemOneStoreError(e.to_string()))?,
                spend_nanos: to_u64(Some(spend)).unwrap_or(0),
            })
        })
        .collect()
    }

    pub fn acknowledge_fact(&self, fact_id: &str) -> Result<()> {
        self.lock()
            .execute("DELETE FROM usage_outbox WHERE fact_id=?1", [fact_id])?;
        Ok(())
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

    /// A batch as recorded under `role`, with its outcomes, or `None` when there is none.
    /// Reads only: the batch is not claimed and nothing is written.
    pub fn lookup(&self, role: &str, batch_id: &str) -> Result<Option<StoredBatch>> {
        let mut conn = self.lock();
        // One read transaction, so the row and its outcomes come from the same moment even
        // when another process shares the file.
        let tx = conn.transaction()?;
        let row = tx
            .query_row(
                "SELECT model, scope_json, identity, body_digest, frozen_row, ceiling_nanos,
                        spent_nanos, admitted_at_ms, state
                   FROM batch WHERE role = ?1 AND batch_id = ?2",
                rusqlite::params![role, batch_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                        row.get::<_, i64>(6)?,
                        row.get::<_, i64>(7)?,
                        row.get::<_, String>(8)?,
                    ))
                },
            )
            .optional()?;
        let Some((model, scope, identity, digest, frozen_row, ceiling, spent, admitted, state)) =
            row
        else {
            return Ok(None);
        };
        let scope = scope
            .map(|json| serde_json::from_str::<BatchScope>(&json))
            .transpose()
            .map_err(|error| SystemOneStoreError(format!("a recorded batch scope: {error}")))?;
        let outcomes = read_outcomes(&tx, role, batch_id)?;
        tx.commit()?;
        Ok(Some(StoredBatch {
            model,
            scope,
            identity,
            args_digest: digest,
            frozen_row,
            ceiling_nanos: to_u64(ceiling),
            spent_nanos: to_u64(Some(spent)).unwrap_or(0),
            admitted_at_ms: admitted,
            running: state == "running",
            outcomes,
        }))
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

/// The read-only batch lookup and the anchored admission time.
#[cfg(test)]
mod lookup_tests {
    use super::*;

    const DAY_MS: i64 = 24 * 60 * 60 * 1000;

    fn scope(run_id: &str) -> BatchScope {
        BatchScope::Run {
            agent_id: "agent".into(),
            run_id: run_id.into(),
        }
    }

    fn claim<'a>(role: &'a str, scope: &'a BatchScope, now: i64) -> BatchClaim<'a> {
        BatchClaim {
            role,
            batch_id: "d",
            model: "typesafe/jev-1.13.0",
            identity: r#"{"items":["x"]}"#,
            body_digest: "digest-1",
            frozen_row: r#"{"input_rate_nanos":42}"#,
            scope,
            ceiling_nanos: Some(7),
            owner: "p",
            now_ms: now,
        }
    }

    #[test]
    fn lookup_returns_the_recorded_batch_by_role_and_writes_nothing() {
        let store = SystemOneStore::open_in_memory().unwrap();
        assert_eq!(store.lookup("llm.classify", "d").unwrap(), None);
        let run = scope("run");
        assert_eq!(
            store.claim(&claim("llm.classify", &run, 1_000)).unwrap(),
            Claimed::Fresh
        );
        let outcome = StoredOutcome {
            index: 0,
            outcome: Outcome::Answered(r#"{"q":1}"#.into()),
            input_tokens: Some(5),
            output_tokens: None,
        };
        assert!(store
            .record("llm.classify", "d", "p", &outcome, 3, 1_001)
            .unwrap());
        let running = store.lookup("llm.classify", "d").unwrap().unwrap();
        assert!(running.running, "a held batch reads as running");
        store.release("llm.classify", "d", "p").unwrap();

        let found = store.lookup("llm.classify", "d").unwrap().unwrap();
        assert_eq!(
            found,
            StoredBatch {
                model: "typesafe/jev-1.13.0".into(),
                scope: Some(run.clone()),
                identity: r#"{"items":["x"]}"#.into(),
                args_digest: "digest-1".into(),
                frozen_row: r#"{"input_rate_nanos":42}"#.into(),
                ceiling_nanos: Some(7),
                spent_nanos: 3,
                admitted_at_ms: 1_000,
                running: false,
                outcomes: vec![outcome],
            }
        );
        assert_eq!(
            store.lookup("classifier", "d").unwrap(),
            None,
            "the same batch id under another role is another batch"
        );
        // A lookup is not a claim: the batch stays free for the next caller.
        assert_eq!(store.lookup("llm.classify", "d").unwrap(), Some(found));
        assert!(matches!(
            store.claim(&claim("llm.classify", &run, 2_000)).unwrap(),
            Claimed::Resumed(_)
        ));
    }

    #[test]
    fn an_anchored_claim_dates_retention_from_the_supplied_admission() {
        let store = SystemOneStore::open_in_memory().unwrap();
        let run = scope("run");
        let admitted = 10 * DAY_MS;
        let executed = admitted + DAY_MS;
        assert_eq!(
            store
                .claim_admitted_at(&claim("llm.classify", &run, executed), admitted)
                .unwrap(),
            Claimed::Fresh
        );
        store.release("llm.classify", "d", "p").unwrap();
        let record = store.lookup("llm.classify", "d").unwrap().unwrap();
        assert_eq!(record.admitted_at_ms, admitted);

        // A later claim with another anchor keeps the first one.
        let Claimed::Resumed(resumed) = store
            .claim_admitted_at(&claim("llm.classify", &run, executed + 1), executed + 1)
            .unwrap()
        else {
            panic!("expected the recorded batch");
        };
        assert_eq!(resumed.admitted_at_ms, admitted);
        store.release("llm.classify", "d", "p").unwrap();

        assert_eq!(store.sweep(admitted + RETENTION_MS - 1).unwrap().batches, 0);
        assert_eq!(
            store.sweep(admitted + RETENTION_MS).unwrap().batches,
            1,
            "retention runs from the supplied admission, not the later claim"
        );
        assert_eq!(store.lookup("llm.classify", "d").unwrap(), None);
    }
}

#[cfg(test)]
mod owner_lifetime_tests {
    use super::*;
    use std::process::{Child, Command, Stdio};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    fn claim_for<'a>(owner: &'a str, scope: &'a BatchScope) -> BatchClaim<'a> {
        BatchClaim {
            role: "llm.classify",
            batch_id: "live",
            model: "typesafe/jev-1.13.0",
            identity: "{}",
            body_digest: "digest",
            frozen_row: "{}",
            scope,
            ceiling_nanos: Some(1),
            owner,
            now_ms: 1,
        }
    }

    #[test]
    #[ignore = "subprocess fixture launched by owner_recovery_never_steals_a_live_process_claim"]
    fn owner_process_fixture() {
        let path =
            PathBuf::from(std::env::var_os("BROCA_BATCH_OWNER_FIXTURE").expect("fixture path"));
        let store = SystemOneStore::open(&path).unwrap();
        let owner = format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let scope = BatchScope::Run {
            agent_id: "agent".into(),
            run_id: "run".into(),
        };
        assert_eq!(
            store.claim(&claim_for(&owner, &scope)).unwrap(),
            Claimed::Fresh
        );
        std::fs::write(path.with_extension("ready"), owner).unwrap();
        loop {
            std::thread::park();
        }
    }

    struct Reap(Child);
    impl Drop for Reap {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    #[test]
    fn owner_recovery_never_steals_a_live_process_claim() {
        let root =
            broca_test_support::scratch_root().join(format!("owner-proof-{}", std::process::id()));
        broca_owner_only::create_dir_all(&root).unwrap();
        let path = root.join("system-one.db");
        let mut child = Reap(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--ignored",
                    "--exact",
                    "system_one::owner_lifetime_tests::owner_process_fixture",
                ])
                .env("BROCA_BATCH_OWNER_FIXTURE", &path)
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(15);
        while !path.with_extension("ready").exists() {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "owner fixture exited before claiming"
            );
            assert!(Instant::now() < deadline, "owner fixture never claimed");
            std::thread::sleep(Duration::from_millis(10));
        }
        let store = SystemOneStore::open(&path).unwrap();
        let scope = BatchScope::Run {
            agent_id: "agent".into(),
            run_id: "run".into(),
        };
        assert_eq!(
            store.release_abandoned().unwrap(),
            0,
            "live owner was released"
        );
        assert_eq!(
            store.claim(&claim_for("other-process", &scope)).unwrap(),
            Claimed::InProgress
        );
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        assert_eq!(
            store.release_abandoned().unwrap(),
            1,
            "dead owner was not released"
        );
        assert_eq!(
            store.release_abandoned().unwrap(),
            0,
            "dead owner was released twice"
        );
        assert!(matches!(
            store.claim(&claim_for("other-process", &scope)).unwrap(),
            Claimed::Resumed(_)
        ));
    }

    #[test]
    fn owner_markers_do_not_accumulate_across_three_restarts() {
        let root = broca_test_support::scratch_root()
            .join(format!("owner-cleanup-{}", std::process::id()));
        broca_owner_only::create_dir_all(&root).unwrap();
        let path = root.join("system-one.db");
        let unrelated = root.join("unrelated.lock");
        std::fs::write(&unrelated, b"unchanged").unwrap();
        let marker_count = || {
            std::fs::read_dir(&root)
                .unwrap()
                .filter(|entry| {
                    entry
                        .as_ref()
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .starts_with("system-one.db.owner-")
                })
                .count()
        };
        let scope = BatchScope::Run {
            agent_id: "agent".into(),
            run_id: "run".into(),
        };
        let mut abandoned = 0;
        for generation in 0..3 {
            let store = SystemOneStore::open(&path).unwrap();
            assert_eq!(store.release_abandoned().unwrap(), abandoned);
            assert_eq!(marker_count(), 0, "dead owner markers accumulated");
            let owner = format!("generation-{generation}");
            store.claim(&claim_for(&owner, &scope)).unwrap();
            assert_eq!(marker_count(), 1);
            let observer = SystemOneStore::open(&path).unwrap();
            assert_eq!(observer.release_abandoned().unwrap(), 0);
            assert_eq!(marker_count(), 1, "a live owner's marker was removed");
            if generation % 2 == 0 {
                store.release("llm.classify", "live", &owner).unwrap();
                abandoned = 0;
            } else {
                abandoned = 1;
            }
        }
        let store = SystemOneStore::open(&path).unwrap();
        assert_eq!(store.release_abandoned().unwrap(), abandoned);
        assert_eq!(marker_count(), 0);
        assert_eq!(std::fs::read(unrelated).unwrap(), b"unchanged");
    }

    #[test]
    fn an_unidentifiable_owner_is_not_assumed_dead() {
        let root = broca_test_support::scratch_root()
            .join(format!("owner-unknown-{}", std::process::id()));
        broca_owner_only::create_dir_all(&root).unwrap();
        let store = SystemOneStore::open(root.join("system-one.db")).unwrap();
        let scope = BatchScope::Run {
            agent_id: "agent".into(),
            run_id: "run".into(),
        };
        store.claim(&claim_for("known", &scope)).unwrap();
        store
            .lock()
            .execute("UPDATE batch SET owner='unknown-without-marker'", [])
            .unwrap();
        assert_eq!(store.release_abandoned().unwrap(), 0);
        assert!(
            store
                .lookup("llm.classify", "live")
                .unwrap()
                .unwrap()
                .running
        );
    }
}
