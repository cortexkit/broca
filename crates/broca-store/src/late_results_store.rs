//! Durable intake registrations. Feed ownership is independent of the rebuildable run index.
//!
//! Runtime installation is intentionally left to the held-call lifecycle wiring. Merely
//! registering an ordinary call never opens a provider route or starts a puller.

use std::{path::Path, sync::Mutex};

use broca_core::tool_plane::{LateResultRegistration, LateResultsIntake};
use rusqlite::{params, Connection, TransactionBehavior};

pub struct LateResultsStore {
    connection: Mutex<Connection>,
}

impl LateResultsStore {
    /// Open a registration projection with a fully synced commit per dispatch group.
    /// The caller owns rebuilding a missing projection before enabling feed intake.
    pub fn open(path: &Path) -> Result<Self, String> {
        let connection = Connection::open(path).map_err(|error| error.to_string())?;
        crate::busy::configure_writer_connection(&connection).map_err(|error| error.to_string())?;
        connection
            .execute_batch(
                "PRAGMA journal_mode = DELETE;
             PRAGMA synchronous = FULL;
             CREATE TABLE IF NOT EXISTS registrations (
                 module TEXT NOT NULL,
                 call_key TEXT NOT NULL,
                 lineage TEXT NOT NULL,
                 registration TEXT NOT NULL,
                 PRIMARY KEY (module, call_key)
             );",
            )
            .map_err(|error| error.to_string())?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    pub fn registrations(&self) -> Result<Vec<LateResultRegistration>, String> {
        let connection = self.connection.lock().map_err(|error| error.to_string())?;
        let mut statement = connection
            .prepare("SELECT registration FROM registrations ORDER BY module, call_key")
            .map_err(|error| error.to_string())?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|error| error.to_string())?;
        rows.map(|row| {
            let json = row.map_err(|error| error.to_string())?;
            serde_json::from_str(&json).map_err(|error| error.to_string())
        })
        .collect()
    }
}

#[async_trait::async_trait]
impl LateResultsIntake for LateResultsStore {
    async fn register_group(&self, calls: &[LateResultRegistration]) -> Result<(), String> {
        if calls.is_empty() {
            return Ok(());
        }
        let mut connection = self.connection.lock().map_err(|error| error.to_string())?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| error.to_string())?;
        for call in calls {
            let json = serde_json::to_string(call).map_err(|error| error.to_string())?;
            // A repeated registration must name exactly the same lineage and scope. Never
            // retarget an already-issued provider key to a different session.
            let changed = transaction.execute(
                "INSERT INTO registrations (module, call_key, lineage, registration) VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT (module, call_key) DO UPDATE SET registration = excluded.registration
                 WHERE registrations.registration = excluded.registration",
                params![call.module, call.call_key, call.lineage, json],
            ).map_err(|error| error.to_string())?;
            if changed != 1 {
                return Err(
                    "late-results key already registered to another session or scope".into(),
                );
            }
        }
        transaction.commit().map_err(|error| error.to_string())
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

    #[tokio::test]
    async fn dispatch_group_registrations_survive_reopen() {
        let path = broca_test_support::scratch_root().join("late-results-reopen.db");
        let expected = vec![call("lineage:1"), call("lineage:2")];
        {
            let store = LateResultsStore::open(&path).unwrap();
            assert_eq!(
                store
                    .connection
                    .lock()
                    .unwrap()
                    .pragma_query_value(None, "synchronous", |row| row.get::<_, i64>(0))
                    .unwrap(),
                2
            );
            store.register_group(&expected).await.unwrap();
            store.register_group(&expected).await.unwrap();
        }
        assert_eq!(
            LateResultsStore::open(&path)
                .unwrap()
                .registrations()
                .unwrap(),
            expected
        );
    }

    #[tokio::test]
    async fn dispatch_group_is_atomic_and_cannot_retarget_an_issued_key() {
        let path = broca_test_support::scratch_root().join("late-results-atomic.db");
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
}
