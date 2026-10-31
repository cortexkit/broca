//! The durable intake for provider-held calls: which calls are registered with which provider
//! feed, what each feed's cursor and owed ack are, and a receipt per entry already settled.
//!
//! Feed ownership is decided from this store alone, never from `run-index.db`: that is a
//! rebuildable projection, and an entry must still be classed when its lineage is not indexed.
//! Every write is one synced transaction, so a crash leaves either the whole change or none.

use std::{
    path::{Path, PathBuf},
    sync::Mutex,
};

use broca_core::tool_plane::{LateResultRegistration, LateResultsIntake};
use cortexkit_role_tool_provider::late_results::{Cursor, IntakeKey};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};

pub(crate) const REGISTRATION_MARKER: &str = "late-results-registered";
pub const REBUILD_REQUIRED_MARKER: &str = "late-results-rebuild-required";

fn sync_marker(root: &Path, name: &str) -> Result<(), String> {
    let marker = root.join(name);
    broca_store::create_private_sqlite(&marker).map_err(|error| error.to_string())?;
    std::fs::OpenOptions::new()
        .write(true)
        .open(marker)
        .and_then(|file| file.sync_all())
        .map_err(|error| error.to_string())?;
    crate::retention::sync_dir(root)
}

/// Missing projections are harmless only on roots with no evidence of prior held intake.
/// An existing but unreadable projection cannot prove that a lineage is unregistered.
pub(crate) fn retention_block_reason(
    root: &Path,
    lineage: &str,
) -> Result<Option<&'static str>, String> {
    if root
        .join(REBUILD_REQUIRED_MARKER)
        .try_exists()
        .map_err(|e| e.to_string())?
        || root
            .join("late-results.db.rebuild")
            .try_exists()
            .map_err(|e| e.to_string())?
    {
        return Ok(Some("provider_held_intake_unknown"));
    }
    let path = root.join("late-results.db");
    if !path.try_exists().map_err(|e| e.to_string())? {
        let prior_registration = root
            .join(REGISTRATION_MARKER)
            .try_exists()
            .map_err(|e| e.to_string())?;
        let floor = broca_wal::read_floor(&root.join(broca_wal::FEATURE_FLOOR_FILENAME))
            .map_err(|e| e.to_string())?;
        return Ok((prior_registration || floor.contains("provider-held/v1"))
            .then_some("provider_held_intake_unknown"));
    }
    let connection = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|error| error.to_string())?;
    let registered: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM registrations WHERE lineage = ?1)",
            [lineage],
            |row| row.get(0),
        )
        .map_err(|error| error.to_string())?;
    Ok(registered.then_some("provider_held_registered"))
}

pub(crate) fn remove_retained_lineage(root: &Path, lineage: &str) -> Result<(), String> {
    let path = root.join("late-results.db");
    if !path.try_exists().map_err(|e| e.to_string())? {
        return Ok(());
    }
    let connection =
        Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE)
            .map_err(|error| error.to_string())?;
    LateResultsStore {
        connection: Mutex::new(connection),
        path,
        held_history: std::sync::atomic::AtomicBool::new(false),
    }
    .remove_lineage(lineage)
}

/// The feed cursor as this store keeps it: the provider's incarnation plus the sequence the
/// provider said that incarnation's entries had reached.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedPosition {
    pub provider_incarnation: String,
    pub seq: u64,
}

impl FeedPosition {
    pub fn cursor(&self) -> Cursor {
        Cursor {
            provider_incarnation: self.provider_incarnation.clone(),
            seq: self.seq,
        }
    }
}

/// What the intake knows about one provider module's feed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeedRow {
    pub module: String,
    pub position: Option<FeedPosition>,
    /// The module has entries to acknowledge on this feed.
    pub ack_owed: bool,
}

/// The most an entry's disposition says about it, for the receipt record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Disposition {
    Delivered,
    NoOp,
}

impl Disposition {
    pub fn as_str(self) -> &'static str {
        match self {
            Disposition::Delivered => "delivered",
            Disposition::NoOp => "no_op",
        }
    }
}

/// Whether a registered call still needs its feed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RegistrationState {
    /// Written before the call's key was sent; the call has not been seen in its WAL yet.
    Registered,
    /// A terminal was written for the call, or its batch commit barrier or seal was recorded, so
    /// the feed no longer has to wait for it. The row is kept so a later entry is still classed.
    Settled,
}

impl RegistrationState {
    pub fn as_str(self) -> &'static str {
        match self {
            RegistrationState::Registered => "registered",
            RegistrationState::Settled => "settled",
        }
    }

    fn from_str(value: &str) -> Self {
        match value {
            "settled" => RegistrationState::Settled,
            _ => RegistrationState::Registered,
        }
    }
}

/// One registered call, as the store holds it.
#[derive(Clone, Debug, PartialEq)]
pub struct RegistrationRow {
    pub call: LateResultRegistration,
    pub state: RegistrationState,
}

pub struct LateResultsStore {
    connection: Mutex<Connection>,
    path: PathBuf,
    held_history: std::sync::atomic::AtomicBool,
}

impl LateResultsStore {
    /// Publish completeness only after all rebuilt rows have committed with synchronous FULL.
    /// A crash before the directory sync may leave the fence present, which safely repeats
    /// the rebuild; it must never publish a partial scan as complete.
    pub fn finish_rebuild(&self) -> Result<(), String> {
        self.finish_rebuild_with_sync(crate::retention::sync_dir)
    }

    fn finish_rebuild_with_sync(
        &self,
        sync: impl FnOnce(&Path) -> Result<(), String>,
    ) -> Result<(), String> {
        let root = self.path.parent().ok_or("intake store has no state root")?;
        match std::fs::remove_file(root.join(REBUILD_REQUIRED_MARKER)) {
            Ok(()) => sync(root),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => sync(root),
            Err(error) => Err(error.to_string()),
        }
    }

    /// Open the intake projection in `path`, creating it when absent.
    ///
    /// The caller owns rebuilding a missing projection before enabling feed intake: a store that
    /// was just created here holds no registration, so its answers are only safe once the WAL
    /// scan has filled it.
    pub fn open(path: &Path) -> Result<Self, String> {
        Self::open_with_wal_recovery(path, false)
    }

    /// Open the store over the file at `path`, moving any write-ahead log SQLite left beside it
    /// out of the way instead of replaying it. A store being rebuilt replays the file's own
    /// journal: the frames there are the projection's last transaction, and replaying them
    /// over the replaced file would resurrect content the rebuild exists to regenerate.
    pub fn open_with_wal_recovery(path: &Path, replay_journal: bool) -> Result<Self, String> {
        let path = path.to_path_buf();
        if !path.try_exists().map_err(|error| error.to_string())? {
            let root = path.parent().ok_or("intake store has no state root")?;
            if retention_block_reason(root, "")?.is_some() {
                // A newly created database is not a completed rebuild. Keep retention fenced
                // across its creation; only a verified full scan can discharge this marker.
                sync_marker(root, REBUILD_REQUIRED_MARKER)?;
            }
        }
        if !replay_journal {
            for suffix in ["-wal", "-shm"] {
                let mut side = path.clone().into_os_string();
                side.push(suffix);
                let side = std::path::PathBuf::from(side);
                if side.exists() {
                    std::fs::remove_file(&side).map_err(|error| error.to_string())?;
                }
            }
        }
        // The store names sessions and late results, so like every other file under the state
        // root it is owner-only: created that way here, before SQLite opens it through the
        // process umask.
        broca_store::create_private_sqlite(&path)
            .map_err(|error| format!("create {} owner-only: {error}", path.display()))?;
        #[cfg(test)]
        boot_io::record(|counts| counts.opens += 1);
        let connection = Connection::open(&path).map_err(|error| error.to_string())?;
        #[cfg(test)]
        boot_io::record(|counts| counts.schema_batches += 1);
        connection
            .execute_batch(
                "PRAGMA journal_mode = DELETE;
             PRAGMA synchronous = FULL;
             CREATE TABLE IF NOT EXISTS registrations (
                 module TEXT NOT NULL,
                 call_key TEXT NOT NULL,
                 lineage TEXT NOT NULL,
                 registration TEXT NOT NULL,
                 state TEXT NOT NULL DEFAULT 'registered',
                 PRIMARY KEY (module, call_key)
             );
             CREATE TABLE IF NOT EXISTS feeds (
                 module TEXT PRIMARY KEY,
                 provider_incarnation TEXT,
                 seq INTEGER,
                 ack_owed INTEGER NOT NULL DEFAULT 0
             );
             CREATE TABLE IF NOT EXISTS receipts (
                 provider TEXT NOT NULL,
                 custodian TEXT NOT NULL,
                 call_key TEXT NOT NULL,
                 event_id TEXT NOT NULL,
                 kind TEXT NOT NULL,
                 disposition TEXT NOT NULL,
                 PRIMARY KEY (provider, custodian, call_key, event_id)
             );",
            )
            .map_err(|error| error.to_string())?;
        let store = Self {
            connection: Mutex::new(connection),
            path,
            held_history: std::sync::atomic::AtomicBool::new(false),
        };
        // Older projections may have registrations but no withdrawal outbox yet. Backfill
        // only those rows; ordinary roots with no registration history remain write-free.
        let calls: Vec<_> = store
            .prune_tombstoned()?
            .into_iter()
            .map(|row| row.call)
            .collect();
        store.register_calls(&calls)?;
        Ok(store)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Registration and feed snapshots already needed by boot discover history without a
    /// second marker probe or schema query. Feed rows survive registration pruning.
    pub fn has_held_history(&self) -> bool {
        self.held_history.load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn recovery_snapshot(
        &self,
    ) -> Result<
        (
            Vec<LateResultRegistration>,
            std::collections::BTreeSet<String>,
        ),
        String,
    > {
        let calls = self.registrations()?;
        let mut modules: std::collections::BTreeSet<_> =
            self.feeds()?.into_iter().map(|feed| feed.module).collect();
        modules.extend(calls.iter().map(|call| call.module.clone()));
        Ok((calls, modules))
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, String> {
        self.connection.lock().map_err(|error| error.to_string())
    }

    /// Every registration, in a stable order.
    pub fn registrations(&self) -> Result<Vec<LateResultRegistration>, String> {
        Ok(self
            .registration_rows()?
            .into_iter()
            .map(|row| row.call)
            .collect())
    }

    /// Every registration with its state.
    pub fn registration_rows(&self) -> Result<Vec<RegistrationRow>, String> {
        self.prune_tombstoned()?;
        self.unpruned_registration_rows()
    }

    /// A bounded maintenance page, without probing every registration's WAL or tombstone.
    /// The cursor is process-local; deleting settled rows cannot skip a surviving key.
    pub fn registration_page(
        &self,
        after: Option<&(String, String)>,
        limit: usize,
    ) -> Result<Vec<RegistrationRow>, String> {
        let connection = self.lock()?;
        let mut statement = connection
            .prepare(
                "SELECT registration, state FROM registrations
             WHERE ?1 IS NULL OR (module, call_key) > (?1, ?2)
             ORDER BY module, call_key LIMIT ?3",
            )
            .map_err(|error| error.to_string())?;
        let rows = statement
            .query_map(
                params![
                    after.map(|key| key.0.as_str()),
                    after.map(|key| key.1.as_str()),
                    limit as u64
                ],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .map_err(|error| error.to_string())?;
        rows.map(|row| {
            let (json, state) = row.map_err(|error| error.to_string())?;
            Ok(RegistrationRow {
                call: serde_json::from_str(&json).map_err(|error| error.to_string())?,
                state: RegistrationState::from_str(&state),
            })
        })
        .collect()
    }

    fn unpruned_registration_rows(&self) -> Result<Vec<RegistrationRow>, String> {
        #[cfg(test)]
        boot_io::record(|counts| counts.reads += 1);
        let connection = self.lock()?;
        let mut statement = connection
            .prepare(
                "SELECT registration, state FROM registrations
                 ORDER BY module, lineage, call_key",
            )
            .map_err(|error| error.to_string())?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|error| error.to_string())?;
        let rows: Vec<_> = rows
            .map(|row| {
                let (json, state) = row.map_err(|error| error.to_string())?;
                Ok(RegistrationRow {
                    call: serde_json::from_str(&json).map_err(|error| error.to_string())?,
                    state: RegistrationState::from_str(&state),
                })
            })
            .collect::<Result<_, String>>()?;
        if !rows.is_empty() {
            self.held_history
                .store(true, std::sync::atomic::Ordering::Release);
        }
        Ok(rows)
    }

    pub fn registration(
        &self,
        module: &str,
        call_key: &str,
    ) -> Result<Option<RegistrationRow>, String> {
        self.prune_tombstoned()?;
        let connection = self.lock()?;
        connection
            .query_row(
                "SELECT registration, state FROM registrations WHERE module = ?1 AND call_key = ?2",
                params![module, call_key],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(|error| error.to_string())?
            .map(|(json, state)| {
                Ok::<_, String>(RegistrationRow {
                    call: serde_json::from_str(&json).map_err(|error| error.to_string())?,
                    state: RegistrationState::from_str(&state),
                })
            })
            .transpose()
    }

    /// Registrations on one provider module.
    pub fn registrations_for(&self, module: &str) -> Result<Vec<RegistrationRow>, String> {
        Ok(self
            .registration_rows()?
            .into_iter()
            .filter(|row| row.call.module == module)
            .collect())
    }

    /// Forget a registration: its call is settled for good (terminal and batch commit durable),
    /// or its lineage was deleted with its session.
    pub fn remove(&self, module: &str, call_key: &str) -> Result<(), String> {
        let connection = self.lock()?;
        connection
            .execute(
                "DELETE FROM registrations WHERE module = ?1 AND call_key = ?2",
                params![module, call_key],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    /// Every feed row, in module order.
    pub fn feeds(&self) -> Result<Vec<FeedRow>, String> {
        #[cfg(test)]
        boot_io::record(|counts| counts.reads += 1);
        let connection = self.lock()?;
        let mut statement = connection
            .prepare(
                "SELECT module, provider_incarnation, seq, ack_owed FROM feeds
                 ORDER BY module",
            )
            .map_err(|error| error.to_string())?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })
            .map_err(|error| error.to_string())?;
        let rows: Vec<_> = rows
            .map(|row| {
                let (module, incarnation, seq, ack_owed) =
                    row.map_err(|error| error.to_string())?;
                Ok(FeedRow {
                    module,
                    position: incarnation.zip(seq).map(|(provider_incarnation, seq)| {
                        FeedPosition {
                            provider_incarnation,
                            seq: seq as u64,
                        }
                    }),
                    ack_owed: ack_owed != 0,
                })
            })
            .collect::<Result<_, String>>()?;
        if !rows.is_empty() {
            self.held_history
                .store(true, std::sync::atomic::Ordering::Release);
        }
        Ok(rows)
    }

    pub fn feed(&self, module: &str) -> Result<Option<FeedRow>, String> {
        Ok(self.feeds()?.into_iter().find(|row| row.module == module))
    }

    /// Create a feed row for a module a rebuild found a held call on. An existing row keeps its
    /// cursor and ack state; only the owed ack is raised.
    pub fn ensure_feed(&self, module: &str, ack_owed: bool) -> Result<(), String> {
        let connection = self.lock()?;
        connection
            .execute(
                "INSERT INTO feeds (module, ack_owed) VALUES (?1, ?2)
                 ON CONFLICT (module) DO UPDATE SET
                     ack_owed = CASE WHEN ?2 != 0 THEN 1 ELSE feeds.ack_owed END",
                params![module, ack_owed as i64],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    /// Store the cursor an ack went through, and clear the owed ack.
    pub fn record_ack(&self, module: &str, through: &FeedPosition) -> Result<(), String> {
        let connection = self.lock()?;
        connection
            .execute(
                "INSERT INTO feeds (module, provider_incarnation, seq, ack_owed)
                 VALUES (?1, ?2, ?3, 0)
                 ON CONFLICT (module) DO UPDATE SET
                     provider_incarnation = ?2, seq = ?3, ack_owed = 0",
                params![module, through.provider_incarnation, through.seq as i64],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    /// Raise the owed ack: an entry was handed over or classified and no ack covers it yet.
    pub fn set_ack_owed(&self, module: &str, owed: bool) -> Result<(), String> {
        let connection = self.lock()?;
        connection
            .execute(
                "INSERT INTO feeds (module, ack_owed) VALUES (?1, ?2)
                 ON CONFLICT (module) DO UPDATE SET ack_owed = ?2",
                params![module, owed as i64],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    /// Forget a cursor the provider refused, keeping ack willingness: the reader re-reads from
    /// the start of the provider's new incarnation and dedupes on the receipt keys.
    pub fn forget_cursor(&self, module: &str) -> Result<(), String> {
        let connection = self.lock()?;
        connection
            .execute(
                "UPDATE feeds SET provider_incarnation = NULL, seq = NULL WHERE module = ?1",
                params![module],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    /// Whether a receipt already covers this entry.
    pub fn receipt(&self, key: &IntakeKey) -> Result<Option<Disposition>, String> {
        let connection = self.lock()?;
        let disposition = connection
            .query_row(
                "SELECT disposition FROM receipts
                 WHERE provider = ?1 AND custodian = ?2 AND call_key = ?3 AND event_id = ?4",
                params![key.provider, key.custodian, key.call_key, key.event_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| error.to_string())?;
        Ok(disposition.map(|value| {
            if value == Disposition::Delivered.as_str() {
                Disposition::Delivered
            } else {
                Disposition::NoOp
            }
        }))
    }

    /// Record that an entry is settled. A delivered receipt is only ever added: once an entry has
    /// been delivered, a later re-read must not record it as a no-op. A delivered receipt is
    /// written only after its terminal is synced, and a no-op's before the ack that covers it, so
    /// a receipt never stands in for a missing terminal.
    pub fn record_receipt(
        &self,
        key: &IntakeKey,
        kind: &str,
        disposition: Disposition,
    ) -> Result<(), String> {
        let connection = self.lock()?;
        connection
            .execute(
                "INSERT INTO receipts (provider, custodian, call_key, event_id, kind, disposition)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT (provider, custodian, call_key, event_id) DO UPDATE SET
                     kind = excluded.kind,
                     disposition = CASE
                         WHEN receipts.disposition = 'delivered' THEN 'delivered'
                         ELSE excluded.disposition
                     END",
                params![
                    key.provider,
                    key.custodian,
                    key.call_key,
                    key.event_id,
                    kind,
                    disposition.as_str()
                ],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    /// Drop the receipts an ack through `through` covers: those up to that sequence of the
    /// provider's incarnation in which they were pulled.
    pub fn prune_receipts(&self, keys: &[IntakeKey]) -> Result<(), String> {
        if keys.is_empty() {
            return Ok(());
        }
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| error.to_string())?;
        for key in keys {
            transaction
                .execute(
                    "DELETE FROM receipts
                     WHERE provider = ?1 AND custodian = ?2 AND call_key = ?3 AND event_id = ?4",
                    params![key.provider, key.custodian, key.call_key, key.event_id],
                )
                .map_err(|error| error.to_string())?;
        }
        transaction.commit().map_err(|error| error.to_string())?;
        Ok(())
    }
}

impl LateResultsStore {
    /// Write every registration of one dispatch group in one synced transaction. A repeated
    /// registration must name exactly the same lineage and scope: an issued key is never
    /// retargeted to another session.
    pub fn register_calls(&self, calls: &[LateResultRegistration]) -> Result<(), String> {
        if calls.is_empty() {
            return Ok(());
        }
        // The projection may later be deleted for rebuild. Keep durable evidence that absence
        // no longer proves this state root has never registered a provider-held-capable call.
        let root = self.path.parent().ok_or("intake store has no state root")?;
        sync_marker(root, REGISTRATION_MARKER)?;
        let mut connection = self.lock()?;
        #[cfg(test)]
        boot_io::record(|counts| counts.writes += 1);
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| error.to_string())?;
        for call in calls {
            let json = serde_json::to_string(call).map_err(|error| error.to_string())?;
            // A repeated registration must name exactly the same lineage and scope. Never
            // retarget an already-issued provider key to a different session.
            let changed = transaction.execute(
                "INSERT INTO registrations (module, call_key, lineage, registration, state) VALUES (?1, ?2, ?3, ?4, 'registered')
                 ON CONFLICT (module, call_key) DO UPDATE SET registration = excluded.registration
                 WHERE registrations.registration = excluded.registration",
                params![call.module, call.call_key, call.lineage, json],
            ).map_err(|error| error.to_string())?;
            if changed != 1 {
                return Err(
                    "late-results key already registered to another session or scope".into(),
                );
            }
            // Keep the registration independently of intake pruning: a cancel seal can be
            // durable just before a crash, before any withdrawal has been sent.
            transaction
                .execute_batch(
                    "CREATE TABLE IF NOT EXISTS held_withdrawals (
                module TEXT NOT NULL, call_key TEXT NOT NULL, registration TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'watching', attempts INTEGER NOT NULL DEFAULT 0,
                detail TEXT, PRIMARY KEY (module, call_key));",
                )
                .map_err(|error| error.to_string())?;
            let changed = transaction.execute("INSERT INTO held_withdrawals (module, call_key, registration) VALUES (?1, ?2, ?3)
                ON CONFLICT (module, call_key) DO UPDATE SET registration = excluded.registration
                WHERE held_withdrawals.registration = excluded.registration", params![call.module, call.call_key, json]).map_err(|error| error.to_string())?;
            if changed != 1 {
                return Err("withdrawal key already registered to another session or scope".into());
            }
            transaction
                .execute(
                    "INSERT INTO feeds (module, ack_owed) VALUES (?1, 0)
                     ON CONFLICT (module) DO NOTHING",
                    params![call.module],
                )
                .map_err(|error| error.to_string())?;
        }
        transaction.commit().map_err(|error| error.to_string())?;
        self.held_history
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    pub fn withdrawal_rows(&self) -> Result<Vec<(LateResultRegistration, String, u32)>, String> {
        let connection = self.lock()?;
        #[cfg(test)]
        boot_io::record(|counts| counts.reads += 1);
        let exists: bool = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='held_withdrawals')",
                [],
                |row| row.get(0),
            )
            .map_err(|error| error.to_string())?;
        if !exists {
            return Ok(Vec::new());
        }
        #[cfg(test)]
        boot_io::record(|counts| counts.reads += 1);
        let mut query = connection
            .prepare(
                "SELECT registration, state, attempts FROM held_withdrawals WHERE state != 'done'",
            )
            .map_err(|error| error.to_string())?;
        let rows = query
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, u32>(2)?,
                ))
            })
            .map_err(|error| error.to_string())?;
        rows.map(|row| {
            let (json, state, attempts) = row.map_err(|error| error.to_string())?;
            Ok((
                serde_json::from_str(&json).map_err(|error| error.to_string())?,
                state,
                attempts,
            ))
        })
        .collect()
    }

    pub fn record_withdrawal(
        &self,
        call: &LateResultRegistration,
        state: &str,
        attempts: u32,
        detail: &serde_json::Value,
    ) -> Result<(), String> {
        let changed = self.lock()?.execute("UPDATE held_withdrawals SET state=?3, attempts=?4, detail=?5 WHERE module=?1 AND call_key=?2",
            params![call.module, call.call_key, state, attempts, detail.to_string()]).map_err(|error| error.to_string())?;
        if changed != 1 {
            return Err("held withdrawal registration was removed".into());
        }
        Ok(())
    }

    /// Read one outbox key, including done rows, while a per-call active guard is held.
    pub fn withdrawal_state(
        &self,
        module: &str,
        call_key: &str,
    ) -> Result<Option<(String, u32)>, String> {
        self.lock()?
            .query_row(
                "SELECT state, attempts FROM held_withdrawals WHERE module=?1 AND call_key=?2",
                params![module, call_key],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| error.to_string())
    }

    pub fn withdrawal_call(
        &self,
        module: &str,
        call_key: &str,
    ) -> Result<Option<LateResultRegistration>, String> {
        let json: Option<String> = self.lock()?.query_row(
            "SELECT registration FROM held_withdrawals WHERE module=?1 AND call_key=?2 AND state NOT IN ('done','failed')",
            params![module, call_key], |row| row.get(0),
        ).optional().map_err(|error| error.to_string())?;
        json.map(|json| serde_json::from_str(&json).map_err(|error| error.to_string()))
            .transpose()
    }

    pub fn withdrawal_counts(&self) -> Result<(usize, usize, usize), String> {
        self.lock()?.query_row(
            "SELECT count(CASE WHEN state='watching' THEN 1 END), count(CASE WHEN state='pending' THEN 1 END), count(CASE WHEN state='failed' THEN 1 END) FROM held_withdrawals",
            [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        ).map_err(|error| error.to_string())
    }

    /// An inline terminal removes its registration and unattempted withdrawal watch in one
    /// transaction. Keep attempted withdrawals available for recovery if the process crashes
    /// before withdrawal finishes, independently of ordinary intake pruning.
    pub fn remove_inline_calls(&self, calls: &[(String, String)]) -> Result<usize, String> {
        if calls.is_empty() {
            return Ok(0);
        }
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| error.to_string())?;
        let mut removed = 0;
        for (module, key) in calls {
            transaction
                .execute(
                    "DELETE FROM registrations WHERE module=?1 AND call_key=?2",
                    params![module, key],
                )
                .map_err(|error| error.to_string())?;
            removed += transaction.execute("DELETE FROM held_withdrawals WHERE module=?1 AND call_key=?2 AND state='watching' AND attempts=0", params![module, key]).map_err(|error| error.to_string())?;
        }
        transaction.commit().map_err(|error| error.to_string())?;
        Ok(removed)
    }

    pub fn withdrawal_failure_details(&self) -> Result<Vec<serde_json::Value>, String> {
        let connection = self.lock()?;
        let mut query = connection.prepare("SELECT detail FROM held_withdrawals WHERE state='failed' ORDER BY module, call_key LIMIT 10").map_err(|error| error.to_string())?;
        let rows = query
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|error| error.to_string())?;
        rows.map(|row| {
            serde_json::from_str(&row.map_err(|error| error.to_string())?)
                .map_err(|error| error.to_string())
        })
        .collect()
    }

    /// Forget several registrations in one synced transaction. Each pair is `(module, call_key)`.
    pub fn remove_calls(&self, calls: &[(String, String)]) -> Result<(), String> {
        if calls.is_empty() {
            return Ok(());
        }
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| error.to_string())?;
        for (module, call_key) in calls {
            transaction
                .execute(
                    "DELETE FROM registrations WHERE module = ?1 AND call_key = ?2",
                    params![module, call_key],
                )
                .map_err(|error| error.to_string())?;
        }
        transaction.commit().map_err(|error| error.to_string())
    }

    /// Retention retires a whole lineage, including receipts whose registrations were already
    /// pruned. Feed positions and ack debt belong to the module, not to the deleted session.
    pub fn remove_lineage(&self, lineage: &str) -> Result<(), String> {
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| error.to_string())?;
        transaction
            .execute("DELETE FROM registrations WHERE lineage = ?1", [lineage])
            .map_err(|error| error.to_string())?;
        let has_withdrawals: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='held_withdrawals')",
                [],
                |row| row.get(0),
            )
            .map_err(|error| error.to_string())?;
        if has_withdrawals {
            transaction.execute("DELETE FROM held_withdrawals WHERE substr(call_key, 1, length(?1) + 1) = ?1 || ':'", [lineage]).map_err(|error| error.to_string())?;
        }
        transaction
            .execute(
                "DELETE FROM receipts WHERE substr(call_key, 1, length(?1) + 1) = ?1 || ':'",
                [lineage],
            )
            .map_err(|error| error.to_string())?;
        transaction.commit().map_err(|error| error.to_string())
    }

    /// A tombstone is durable before WAL removal. Reuse the surviving registration snapshot
    /// for backfill, and inspect the outbox only after an existing registration/feed read
    /// has proved history. An ordinary root performs no additional schema lookup.
    pub(crate) fn prune_tombstoned(&self) -> Result<Vec<RegistrationRow>, String> {
        let root = self.path.parent().ok_or("intake store has no state root")?;
        let wal = broca_core::FileWalStore::new(root.join("wal"));
        let mut rows = self.unpruned_registration_rows()?;
        let mut calls: Vec<_> = rows.iter().map(|row| row.call.clone()).collect();
        if self.has_held_history() {
            calls.extend(self.withdrawal_rows()?.into_iter().map(|(call, _, _)| call));
        }
        for call in calls {
            let path = wal.tombstone_path(&call.bind, &call.lineage);
            let bytes = match std::fs::read(path) {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.to_string()),
            };
            let stone: crate::retention::Tombstone =
                serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
            if stone.lineage_id != call.lineage {
                return Err("intake retention tombstone identity mismatch".into());
            }
            self.remove_lineage(&call.lineage)?;
            rows.retain(|row| row.call.lineage != call.lineage);
        }
        Ok(rows)
    }
}

#[async_trait::async_trait]
impl LateResultsIntake for LateResultsStore {
    async fn register_group(&self, calls: &[LateResultRegistration]) -> Result<(), String> {
        self.register_calls(calls)
    }

    async fn prune(&self, calls: &[(String, String)]) -> Result<(), String> {
        self.remove_calls(calls)
    }

    async fn prune_inline(&self, calls: &[(String, String)]) -> Result<(), String> {
        self.remove_inline_calls(calls).map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(key: &str) -> LateResultRegistration {
        serde_json::from_value(serde_json::json!({
            "lineage":"lineage", "call_key":key, "module":"provider",
            "bind":{"project_root":"/project", "harness":"broca", "session":"session"},
            "scope":null
        }))
        .unwrap()
    }

    fn scratch(name: &str) -> PathBuf {
        broca_test_support::scratch_root().join(name)
    }

    struct NoWithdrawal;
    #[async_trait::async_trait]
    impl crate::held_withdrawals::WithdrawTransport for NoWithdrawal {
        async fn withdraw(
            &self,
            _: &LateResultRegistration,
        ) -> (
            cortexkit_role_tool_provider::withdraw::WithdrawVerdict,
            serde_json::Value,
        ) {
            panic!("a never-held root has no withdrawal work")
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn review_regression_dormant_boot_keeps_the_sqlite_transaction_budget() {
        let root = scratch("dormant-boot-io");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("late-results.db");
        drop(LateResultsStore::open(&path).unwrap());
        boot_io::reset();
        let store = std::sync::Arc::new(LateResultsStore::open(&path).unwrap());
        let (calls, modules) = store.recovery_snapshot().unwrap();
        assert!(calls.is_empty() && modules.is_empty());
        let service = crate::held_withdrawals::HeldWithdrawals::new(
            store,
            std::sync::Arc::new(broca_core::FileWalStore::new(root.join("wal"))),
            std::sync::Arc::new(NoWithdrawal),
        );
        service.bind_stopping_signal(tokio_util::sync::CancellationToken::new());
        service.recover().await.unwrap();
        // Fixture setup is excluded. Count SQLite connection opens and each SELECT's
        // autocommit read-transaction boundary, not OS open/read/write syscalls.
        assert_eq!(
            boot_io::counts(),
            boot_io::Counts {
                opens: 1,
                reads: 4,
                writes: 0,
                schema_batches: 1
            }
        );
        assert!(!root.join(REGISTRATION_MARKER).exists());
        assert!(!root.join(REBUILD_REQUIRED_MARKER).exists());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn retained_feed_discovers_pruned_outbox_history_and_tombstones() {
        let root = scratch("pruned-outbox-history");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("late-results.db");
        let store = LateResultsStore::open(&path).unwrap();
        let deleted = call("lineage:1");
        store
            .register_calls(std::slice::from_ref(&deleted))
            .unwrap();
        store.remove("provider", &deleted.call_key).unwrap();
        drop(store);
        let store = std::sync::Arc::new(LateResultsStore::open(&path).unwrap());
        assert!(!store.has_held_history());
        let (calls, modules) = store.recovery_snapshot().unwrap();
        assert!(calls.is_empty());
        assert!(modules.contains("provider"));
        assert!(
            store.has_held_history(),
            "feed rows retain history when intake registrations are pruned"
        );
        assert_eq!(store.withdrawal_rows().unwrap().len(), 1);
        let wal = std::sync::Arc::new(broca_core::FileWalStore::new(root.join("wal")));
        let tombstone = wal.tombstone_path(&deleted.bind, &deleted.lineage);
        std::fs::create_dir_all(tombstone.parent().unwrap()).unwrap();
        std::fs::write(
            tombstone,
            serde_json::to_vec(&crate::retention::Tombstone {
                lineage_id: deleted.lineage,
                expired_at_ms: 1,
                seq: 1,
                completed_at_ms: None,
                next_tag: None,
                next_notice_seq: None,
            })
            .unwrap(),
        )
        .unwrap();
        let service = crate::held_withdrawals::HeldWithdrawals::new(
            store.clone(),
            wal,
            std::sync::Arc::new(NoWithdrawal),
        );
        service.bind_stopping_signal(tokio_util::sync::CancellationToken::new());
        service.recover().await.unwrap();
        assert!(
            store.withdrawal_rows().unwrap().is_empty(),
            "expired calls are pruned before recovery reads any WAL or sends a withdrawal"
        );
    }

    #[test]
    fn review_regression_unlink_sync_failure_retries_the_directory_barrier() {
        let root = scratch("unlink-sync-retry");
        std::fs::create_dir_all(&root).unwrap();
        let store = LateResultsStore::open(&root.join("late-results.db")).unwrap();
        std::fs::write(root.join(REBUILD_REQUIRED_MARKER), b"").unwrap();
        let calls = std::cell::Cell::new(0);
        assert!(store
            .finish_rebuild_with_sync(|_| {
                calls.set(calls.get() + 1);
                Err("injected directory sync failure".into())
            })
            .is_err());
        assert!(!root.join(REBUILD_REQUIRED_MARKER).exists());
        store
            .finish_rebuild_with_sync(|_| {
                calls.set(calls.get() + 1);
                Ok(())
            })
            .unwrap();
        assert_eq!(
            calls.get(),
            2,
            "an absent marker does not prove its unlink was synced"
        );
    }

    fn intake_key(event: &str) -> IntakeKey {
        IntakeKey {
            provider: "provider".into(),
            custodian: "reserved:broca".into(),
            call_key: "lineage:1".into(),
            event_id: event.into(),
        }
    }

    #[tokio::test]
    async fn dispatch_group_registrations_survive_reopen() {
        let path = scratch("late-results-reopen.db");
        let expected = vec![call("lineage:1"), call("lineage:2")];
        {
            let store = LateResultsStore::open(&path).unwrap();
            assert_eq!(
                store
                    .lock()
                    .unwrap()
                    .pragma_query_value(None, "synchronous", |row| row.get::<_, i64>(0))
                    .unwrap(),
                2
            );
            store.register_group(&expected).await.unwrap();
            store.register_group(&expected).await.unwrap();
        }
        let reopened = LateResultsStore::open(&path).unwrap();
        assert_eq!(reopened.registrations().unwrap(), expected);
    }

    /// The store is created owner-only, and a write leaves no file in its directory that group or
    /// world can read: the database, and any journal SQLite made for the write, carry the
    /// database's own mode.
    #[cfg(unix)]
    #[tokio::test]
    async fn the_store_and_its_journal_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = scratch("late-results-mode");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        // Under a umask that already strips group and world bits, every file lands owner-only
        // whatever this code does, so the check could not fail: refuse instead of passing.
        let ambient = dir.join("ambient-control");
        std::fs::File::create(&ambient).unwrap();
        let ambient_mode = std::fs::metadata(&ambient).unwrap().permissions().mode() & 0o777;
        std::fs::remove_file(&ambient).unwrap();
        assert_ne!(
            ambient_mode & 0o077,
            0,
            "the ambient umask already yields {ambient_mode:o}, so this test cannot tell an \
             explicit mode from the default"
        );
        let path = dir.join("late-results.db");
        let store = LateResultsStore::open(&path).unwrap();
        store.register_group(&[call("lineage:1")]).await.unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "late-results.db is {mode:o}");
        for entry in std::fs::read_dir(&dir).unwrap() {
            let entry = entry.unwrap();
            let mode = entry.metadata().unwrap().permissions().mode() & 0o777;
            assert_eq!(mode & 0o077, 0, "{} is {mode:o}", entry.path().display());
        }
    }

    #[tokio::test]
    async fn dispatch_group_is_atomic_and_cannot_retarget_an_issued_key() {
        let path = scratch("late-results-atomic.db");
        let store = LateResultsStore::open(&path).unwrap();
        store.register_group(&[call("lineage:2")]).await.unwrap();
        let mut conflicting = call("lineage:2");
        conflicting.bind.session = "another-session".into();
        assert!(store
            .register_group(&[call("lineage:1"), conflicting])
            .await
            .is_err());
        assert_eq!(store.registrations().unwrap(), vec![call("lineage:2")]);
    }

    /// A batch commit prunes its calls' registrations in one transaction; a call not named is
    /// kept.
    #[tokio::test]
    async fn a_prune_forgets_exactly_the_named_registrations() {
        let path = scratch("late-results-prune.db");
        let _ = std::fs::remove_file(&path);
        let store = LateResultsStore::open(&path).unwrap();
        store
            .register_group(&[call("lineage:1"), call("lineage:2")])
            .await
            .unwrap();
        store
            .prune(&[("provider".into(), "lineage:1".into())])
            .await
            .unwrap();
        assert_eq!(store.registrations().unwrap(), vec![call("lineage:2")]);
    }

    #[tokio::test]
    async fn inline_pruning_retires_its_watch_but_ordinary_pruning_preserves_cancellation_recovery()
    {
        let root = scratch("inline-prune");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let store = LateResultsStore::open(&root.join("late-results.db")).unwrap();
        let inline = call("lineage:1");
        let held = call("lineage:2");
        store.register_calls(&[inline, held]).unwrap();
        store
            .prune_inline(&[("provider".into(), "lineage:1".into())])
            .await
            .unwrap();
        store
            .prune(&[("provider".into(), "lineage:2".into())])
            .await
            .unwrap();
        assert!(store.registrations().unwrap().is_empty());
        assert_eq!(
            store.withdrawal_state("provider", "lineage:1").unwrap(),
            None
        );
        assert_eq!(
            store.withdrawal_state("provider", "lineage:2").unwrap(),
            Some(("watching".into(), 0))
        );
    }

    #[test]
    fn an_ack_stores_the_cursors_position_and_clears_the_owed_ack() {
        let path = scratch("late-results-ack.db");
        let store = LateResultsStore::open(&path).unwrap();
        store.ensure_feed("provider", true).unwrap();
        let raised = store.feed("provider").unwrap().unwrap();
        assert!(raised.ack_owed);
        assert_eq!(raised.position, None);
        store
            .record_ack(
                "provider",
                &FeedPosition {
                    provider_incarnation: "inc-2".into(),
                    seq: 7,
                },
            )
            .unwrap();
        let acked = store.feed("provider").unwrap().unwrap();
        assert!(!acked.ack_owed);
        let position = acked.position.clone().expect("a position was stored");
        assert_eq!(position.seq, 7);
        assert_eq!(
            position.cursor(),
            Cursor {
                provider_incarnation: "inc-2".into(),
                seq: 7
            }
        );
    }

    #[test]
    fn a_receipt_remembers_the_disposition_and_is_pruned_by_its_key() {
        let path = scratch("late-results-receipt.db");
        let store = LateResultsStore::open(&path).unwrap();
        let key = intake_key("ev-1");
        assert_eq!(store.receipt(&key).unwrap(), None);
        store
            .record_receipt(&key, "result", Disposition::Delivered)
            .unwrap();
        assert_eq!(store.receipt(&key).unwrap(), Some(Disposition::Delivered));
        // A re-read of the same entry must not turn a delivery into a no-op, because the
        // delivery already wrote its terminal.
        store
            .record_receipt(&key, "result", Disposition::NoOp)
            .unwrap();
        assert_eq!(store.receipt(&key).unwrap(), Some(Disposition::Delivered));
        store.prune_receipts(std::slice::from_ref(&key)).unwrap();
        assert_eq!(store.receipt(&key).unwrap(), None);
    }

    #[test]
    fn forgetting_a_refused_cursor_keeps_the_owed_ack() {
        let path = scratch("late-results-incarnation.db");
        let store = LateResultsStore::open(&path).unwrap();
        store
            .record_ack(
                "provider",
                &FeedPosition {
                    provider_incarnation: "inc-1".into(),
                    seq: 4,
                },
            )
            .unwrap();
        store.ensure_feed("provider", true).unwrap();
        store.forget_cursor("provider").unwrap();
        let row = store.feed("provider").unwrap().unwrap();
        assert_eq!(row.position, None);
        assert!(row.ack_owed, "an owed ack survives an incarnation change");
    }

    #[test]
    fn retention_cleanup_keeps_shared_feed_and_other_lineages() {
        let path = scratch("late-results-retention/late-results.db");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let store = LateResultsStore::open(&path).unwrap();
        let deleted = call("lineage:1");
        let mut other = call("lineage-other:1");
        other.lineage = "lineage-other".into();
        other.bind.session = "other-session".into();
        store.register_calls(&[deleted, other.clone()]).unwrap();
        let position = FeedPosition {
            provider_incarnation: "inc-1".into(),
            seq: 42,
        };
        store.record_ack("provider", &position).unwrap();
        store.set_ack_owed("provider", true).unwrap();
        let deleted_receipt = intake_key("deleted");
        let mut unregistered_receipt = intake_key("pruned");
        unregistered_receipt.call_key = "lineage:2".into();
        let mut other_receipt = intake_key("other");
        other_receipt.call_key = other.call_key.clone();
        for key in [&deleted_receipt, &unregistered_receipt, &other_receipt] {
            store
                .record_receipt(key, "result", Disposition::NoOp)
                .unwrap();
        }
        store.remove_lineage("lineage").unwrap();
        drop(store);
        let store = LateResultsStore::open(&path).unwrap();
        assert_eq!(store.registrations().unwrap(), vec![other]);
        assert_eq!(store.receipt(&deleted_receipt).unwrap(), None);
        assert_eq!(store.receipt(&unregistered_receipt).unwrap(), None);
        assert_eq!(
            store.receipt(&other_receipt).unwrap(),
            Some(Disposition::NoOp)
        );
        assert_eq!(
            store.feed("provider").unwrap(),
            Some(FeedRow {
                module: "provider".into(),
                position: Some(position),
                ack_owed: true,
            })
        );
    }

    #[test]
    fn boot_prunes_tombstoned_registrations_without_a_wal_or_actor() {
        let path = scratch("late-results-tombstone/late-results.db");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let store = LateResultsStore::open(&path).unwrap();
        let deleted = call("lineage:1");
        store
            .register_calls(std::slice::from_ref(&deleted))
            .unwrap();
        store.ensure_feed("provider", true).unwrap();
        let key = intake_key("ev-1");
        store
            .record_receipt(&key, "result", Disposition::NoOp)
            .unwrap();
        drop(store);
        let wal = broca_core::FileWalStore::new(path.parent().unwrap().join("wal"));
        let tombstone = wal.tombstone_path(&deleted.bind, &deleted.lineage);
        std::fs::create_dir_all(tombstone.parent().unwrap()).unwrap();
        std::fs::write(
            tombstone,
            serde_json::to_vec(&crate::retention::Tombstone {
                lineage_id: deleted.lineage,
                expired_at_ms: 1,
                seq: 1,
                completed_at_ms: None,
                next_tag: None,
                next_notice_seq: None,
            })
            .unwrap(),
        )
        .unwrap();
        let reopened = LateResultsStore::open(&path).unwrap();
        assert!(reopened.unpruned_registration_rows().unwrap().is_empty());
        assert!(reopened.registrations().unwrap().is_empty());
        assert_eq!(
            reopened.registration("provider", "lineage:1").unwrap(),
            None
        );
        assert_eq!(reopened.receipt(&key).unwrap(), None);
        assert!(reopened.feed("provider").unwrap().unwrap().ack_owed);
    }
}

#[cfg(test)]
mod boot_io {
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub struct Counts {
        pub opens: usize,
        pub reads: usize,
        pub writes: usize,
        pub schema_batches: usize,
    }
    thread_local! { static COUNTS: std::cell::Cell<Counts> = const { std::cell::Cell::new(Counts { opens: 0, reads: 0, writes: 0, schema_batches: 0 }) }; }
    pub fn record(update: impl FnOnce(&mut Counts)) {
        COUNTS.with(|counts| {
            let mut value = counts.get();
            update(&mut value);
            counts.set(value);
        });
    }
    pub fn reset() {
        COUNTS.with(|counts| counts.set(Counts::default()));
    }
    pub fn counts() -> Counts {
        COUNTS.with(std::cell::Cell::get)
    }
}
