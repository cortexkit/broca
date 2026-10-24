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
}

impl LateResultsStore {
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
        let connection = Connection::open(&path).map_err(|error| error.to_string())?;
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
        Ok(Self {
            connection: Mutex::new(connection),
            path,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
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
        rows.map(|row| {
            let (json, state) = row.map_err(|error| error.to_string())?;
            Ok(RegistrationRow {
                call: serde_json::from_str(&json).map_err(|error| error.to_string())?,
                state: RegistrationState::from_str(&state),
            })
        })
        .collect()
    }

    pub fn registration(
        &self,
        module: &str,
        call_key: &str,
    ) -> Result<Option<RegistrationRow>, String> {
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
        rows.map(|row| {
            let (module, incarnation, seq, ack_owed) = row.map_err(|error| error.to_string())?;
            Ok(FeedRow {
                module,
                position: incarnation
                    .zip(seq)
                    .map(|(provider_incarnation, seq)| FeedPosition {
                        provider_incarnation,
                        seq: seq as u64,
                    }),
                ack_owed: ack_owed != 0,
            })
        })
        .collect()
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
        let mut connection = self.lock()?;
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
            transaction
                .execute(
                    "INSERT INTO feeds (module, ack_owed) VALUES (?1, 0)
                     ON CONFLICT (module) DO NOTHING",
                    params![call.module],
                )
                .map_err(|error| error.to_string())?;
        }
        transaction.commit().map_err(|error| error.to_string())
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
}

#[async_trait::async_trait]
impl LateResultsIntake for LateResultsStore {
    async fn register_group(&self, calls: &[LateResultRegistration]) -> Result<(), String> {
        self.register_calls(calls)
    }

    async fn prune(&self, calls: &[(String, String)]) -> Result<(), String> {
        self.remove_calls(calls)
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
}
