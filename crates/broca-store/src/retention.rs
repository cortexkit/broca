//! Content-free retention bookkeeping. The caller derives these rows from the WAL under the
//! session lease; deadlines in this index nominate candidates, never authorize deletion.

use broca_protocol::BindIdentity;

use crate::{SqliteStore, StoreError};

pub(crate) fn init_work_schema(tx: &rusqlite::Transaction<'_>) -> Result<(), StoreError> {
    let initialized: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='retention_discovery')", [], |row| row.get(0))?;
    // Triggers cover all run-index writers, including repair and legacy admission.
    // The inventory is a candidate hint; projection still rechecks the leased WAL.
    tx.execute_batch(
        "CREATE TABLE IF NOT EXISTS retention_discovery (lineage_id TEXT PRIMARY KEY);
         CREATE TRIGGER IF NOT EXISTS retention_discovery_run_insert AFTER INSERT ON run_index
         WHEN NEW.withdrawals_owed=1 AND NEW.lineage_id IS NOT NULL BEGIN
             INSERT INTO retention_discovery SELECT NEW.lineage_id
             WHERE EXISTS(SELECT 1 FROM retention_lineage WHERE lineage_id=NEW.lineage_id AND expired_at_ms IS NULL)
             ON CONFLICT(lineage_id) DO NOTHING;
         END;
         CREATE TRIGGER IF NOT EXISTS retention_discovery_run_update AFTER UPDATE OF lineage_id, withdrawals_owed ON run_index BEGIN
             DELETE FROM retention_discovery WHERE lineage_id=OLD.lineage_id AND NOT EXISTS(
                 SELECT 1 FROM run_index WHERE lineage_id=OLD.lineage_id AND withdrawals_owed=1);
             INSERT INTO retention_discovery SELECT NEW.lineage_id
             WHERE NEW.withdrawals_owed=1 AND NEW.lineage_id IS NOT NULL AND EXISTS(
                 SELECT 1 FROM retention_lineage WHERE lineage_id=NEW.lineage_id AND expired_at_ms IS NULL)
             ON CONFLICT(lineage_id) DO NOTHING;
         END;
         CREATE TRIGGER IF NOT EXISTS retention_discovery_run_delete AFTER DELETE ON run_index BEGIN
             DELETE FROM retention_discovery WHERE lineage_id=OLD.lineage_id AND NOT EXISTS(
                 SELECT 1 FROM run_index WHERE lineage_id=OLD.lineage_id AND withdrawals_owed=1);
         END;
         CREATE TRIGGER IF NOT EXISTS retention_discovery_lineage_insert AFTER INSERT ON retention_lineage
         WHEN NEW.expired_at_ms IS NULL BEGIN
             INSERT INTO retention_discovery SELECT NEW.lineage_id WHERE EXISTS(
                 SELECT 1 FROM run_index WHERE lineage_id=NEW.lineage_id AND withdrawals_owed=1)
             ON CONFLICT(lineage_id) DO NOTHING;
         END;
         CREATE TRIGGER IF NOT EXISTS retention_discovery_lineage_update AFTER UPDATE OF expired_at_ms ON retention_lineage BEGIN
             DELETE FROM retention_discovery WHERE lineage_id=NEW.lineage_id;
             INSERT INTO retention_discovery SELECT NEW.lineage_id
             WHERE NEW.expired_at_ms IS NULL AND EXISTS(
                 SELECT 1 FROM run_index WHERE lineage_id=NEW.lineage_id AND withdrawals_owed=1)
             ON CONFLICT(lineage_id) DO NOTHING;
         END;
         CREATE TRIGGER IF NOT EXISTS retention_discovery_lineage_delete AFTER DELETE ON retention_lineage BEGIN
             DELETE FROM retention_discovery WHERE lineage_id=OLD.lineage_id;
         END;"
    )?;
    // Rebuild only on introduction of the inventory; normal opens must not scan it.
    if !initialized {
        tx.execute_batch("INSERT OR IGNORE INTO retention_discovery SELECT l.lineage_id FROM retention_lineage l
            CROSS JOIN run_index r ON r.lineage_id=l.lineage_id WHERE l.expired_at_ms IS NULL AND r.withdrawals_owed=1;")?;
    }
    Ok(())
}

// Start at retention_lineage and look up only its runs, rather than scanning all
// run_index rows when no sessions have retention enabled. Include expired lineages
// too: their sessions must remain non-exportable even after deletion.
const RETAINED_SESSIONS_SQL: &str = "SELECT DISTINCT r.session FROM retention_lineage l CROSS JOIN run_index r ON r.lineage_id=l.lineage_id";

impl SqliteStore {
    pub fn retained_sessions(&self) -> Result<Vec<BindIdentity>, StoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(RETAINED_SESSIONS_SQL)?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
    }
}

#[cfg(test)]
mod tests {
    fn cost_fixture() -> SqliteStore {
        let store = SqliteStore::open_in_memory().unwrap();
        {
            let conn = store.conn.lock().unwrap();
            let session = serde_json::to_string(&BindIdentity::new("/p", "h", "live")).unwrap();
            conn.execute(
                "INSERT INTO retention_lineage VALUES ('live', 'live-key', 1, 0, 1, 0, NULL)",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO run_index (run_id, session, state, lineage_id, state_changed_ms) VALUES ('z-live', ?1, 'completed', 'live', 7), ('a-live', ?1, 'completed', 'live', NULL)",
                [&session],
            ).unwrap();
            conn.execute(
                "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<4000)
                 INSERT INTO run_index (run_id, session, state, lineage_id, withdrawals_owed)
                 SELECT 'other-'||x, ?1, 'completed', 'other-lineage-'||x, 1 FROM n",
                [serde_json::to_string(&BindIdentity::new("/p", "h", "other")).unwrap()],
            )
            .unwrap();
        }
        store
    }

    #[test]
    fn retained_sessions_cost_is_bounded_by_retention_inventory() {
        let store = cost_fixture();
        assert_eq!(
            store.retained_sessions().unwrap(),
            vec![BindIdentity::new("/p", "h", "live")]
        );
        let conn = store.conn.lock().unwrap();
        for empty in [false, true] {
            if empty {
                conn.execute("DELETE FROM retention_lineage", []).unwrap();
            }
            let mut stmt = conn.prepare(RETAINED_SESSIONS_SQL).unwrap();
            let rows = stmt
                .query_map([], |row| row.get::<_, String>(0))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(rows.len(), usize::from(!empty));
            let steps = stmt.get_status(rusqlite::StatementStatus::VmStep);
            assert!(
                steps < 200,
                "retained sessions walked unrelated runs: {steps} VM steps"
            );
        }
    }
    use super::*;

    #[test]
    fn retention_schema_migrates_legacy_rows_idempotently() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE run_index (run_id TEXT PRIMARY KEY, session TEXT NOT NULL,
                 submission_id TEXT, state TEXT NOT NULL);
             INSERT INTO run_index VALUES ('legacy', '{}', NULL, 'completed');",
        )
        .unwrap();
        SqliteStore::init(&conn).unwrap();
        SqliteStore::init(&conn).unwrap();
        let mut stmt = conn
            .prepare("PRAGMA table_info(retention_lineage)")
            .unwrap();
        let columns = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            columns,
            [
                "lineage_id",
                "session",
                "retention_s",
                "last_activity_ms",
                "expires_at_ms",
                "wal_bytes",
                "expired_at_ms"
            ]
        );
        let old: (Option<String>, Option<u64>) = conn
            .query_row(
                "SELECT lineage_id, expired_at_ms FROM run_index WHERE run_id='legacy'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(old, (None, None));
        for column in ["lineage_id", "expired_at_ms"] {
            let nullable: i64 = conn
                .query_row(
                    "SELECT \"notnull\" FROM pragma_table_info('run_index') WHERE name=?1",
                    [column],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(nullable, 0);
        }
        let deadline_index: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE name='retention_lineage_expires_idx'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(deadline_index
            .contains("ON retention_lineage(expires_at_ms) WHERE expires_at_ms IS NOT NULL"));
        let live_index: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE name='retention_lineage_live_session_idx'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(live_index.contains("UNIQUE INDEX"));
        assert!(live_index.contains("ON retention_lineage(session) WHERE expired_at_ms IS NULL"));
    }
}

#[cfg(test)]
mod sweep_work_tests {
    use super::*;

    #[test]
    fn discovery_inventory_tracks_repair_settlement_and_expiry() {
        let store = SqliteStore::open_in_memory().unwrap();
        let key = BindIdentity::new("/p", "h", "s");
        let session = serde_json::to_string(&key).unwrap();
        let conn = store.conn.lock().unwrap();
        conn.execute("INSERT INTO run_index(run_id, session, state, lineage_id, withdrawals_owed) VALUES ('r', ?1, 'completed', 'l', 1)", [&session]).unwrap();
        conn.execute(
            "INSERT INTO retention_lineage VALUES ('l', ?1, 1, 0, 100, 20, NULL)",
            [SqliteStore::key(&key)],
        )
        .unwrap();
        let owed = || {
            conn.query_row("SELECT COUNT(*) FROM retention_discovery", [], |row| {
                row.get::<_, u64>(0)
            })
            .unwrap()
        };
        assert_eq!(owed(), 1);
        conn.execute(
            "UPDATE run_index SET withdrawals_owed=0 WHERE run_id='r'",
            [],
        )
        .unwrap();
        assert_eq!(owed(), 0);
        conn.execute(
            "UPDATE run_index SET withdrawals_owed=1 WHERE run_id='r'",
            [],
        )
        .unwrap();
        assert_eq!(owed(), 1);
        conn.execute("UPDATE retention_lineage SET expires_at_ms=NULL, expired_at_ms=100 WHERE lineage_id='l'", []).unwrap();
        assert_eq!(owed(), 0);
    }
}
