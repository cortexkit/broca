//! Content-free retention bookkeeping. The caller derives these rows from the WAL under the
//! session lease; deadlines in this index nominate candidates, never authorize deletion.

use broca_core::RunIndexRow;
use broca_protocol::BindIdentity;
use rusqlite::{params, OptionalExtension};

use crate::{bind_identity_from_store_key, SqliteStore, StoreError};

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

const RETENTION_RUN_ROWS_SQL: &str =
    "SELECT run_id, state_changed_ms FROM run_index WHERE lineage_id=?1 ORDER BY run_id";
// Start at retention_lineage and look up only its runs, rather than scanning all
// run_index rows when no sessions have retention enabled. Include expired lineages
// too: their sessions must remain non-exportable even after deletion.
const RETAINED_SESSIONS_SQL: &str = "SELECT DISTINCT r.session FROM retention_lineage l CROSS JOIN run_index r ON r.lineage_id=l.lineage_id";

/// One WAL lineage's durable retention progress. `session` is stored in meta's U+001F triple
/// form, not the JSON form used by run_index. A non-terminal lineage has no deadline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionLineageRow {
    pub lineage_id: String,
    pub session: BindIdentity,
    pub retention_s: u64,
    pub last_activity_ms: u64,
    pub expires_at_ms: Option<u64>,
    pub wal_bytes: u64,
    pub expired_at_ms: Option<u64>,
}

/// A run reconstructed from the WAL, including its durable lifecycle transition time.
/// The supplied title is deliberately ignored: retention run rows never store content.
#[derive(Debug, Clone)]
pub struct RetentionRunRow {
    pub run: RunIndexRow,
    pub state_changed_ms: Option<u64>,
}

/// Content-free lineage and expiry metadata attached to a durable run-index row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunRetention {
    pub lineage_id: Option<String>,
    pub expired_at_ms: Option<u64>,
    pub withdrawals_owed: bool,
}

fn integer(value: u64) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(StoreError::Int)
}

fn read_lineage(row: &rusqlite::Row<'_>) -> rusqlite::Result<RetentionLineageRow> {
    let key: String = row.get(1)?;
    let session = bind_identity_from_store_key(&key).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            1,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid retention session triple",
            )),
        )
    })?;
    Ok(RetentionLineageRow {
        lineage_id: row.get(0)?,
        session,
        retention_s: row.get(2)?,
        last_activity_ms: row.get(3)?,
        expires_at_ms: row.get(4)?,
        wal_bytes: row.get(5)?,
        expired_at_ms: row.get(6)?,
    })
}

impl SqliteStore {
    /// Boot-only inventory, keyed by the caller's WAL address before any file is opened.
    pub fn retention_live_inventory(&self) -> Result<Vec<RetentionLineageRow>, StoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT lineage_id, session, retention_s, last_activity_ms, expires_at_ms,
                    wal_bytes, expired_at_ms FROM retention_lineage WHERE expired_at_ms IS NULL",
        )?;
        let rows = stmt.query_map([], read_lineage)?;
        rows.collect::<Result<_, _>>().map_err(StoreError::from)
    }

    /// Record recovery work before publishing an incomplete tombstone. Clearing work is
    /// safe only after the completed tombstone and its directory are durable.
    pub fn retention_deletion_pending(
        &self,
        lineage: &str,
        session: &BindIdentity,
        pending: bool,
    ) -> Result<(), StoreError> {
        let conn = self.conn.lock().unwrap();
        if pending {
            conn.execute(
                "INSERT INTO retention_deletion(lineage_id, session) VALUES (?1, ?2)
                ON CONFLICT(lineage_id) DO UPDATE SET session=excluded.session",
                params![lineage, serde_json::to_string(session)?],
            )?;
        } else {
            conn.execute(
                "DELETE FROM retention_deletion WHERE lineage_id=?1",
                [lineage],
            )?;
        }
        Ok(())
    }

    pub fn retention_pending_deletions(&self) -> Result<Vec<BindIdentity>, StoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT DISTINCT session FROM retention_deletion")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
    }

    pub fn retention_run_rows(&self, lineage: &str) -> Result<Vec<RetentionRunRow>, String> {
        let ids: Vec<(String, Option<u64>)> = {
            let conn = self.conn.lock().unwrap();
            let mut stmt = conn
                .prepare(RETENTION_RUN_ROWS_SQL)
                .map_err(|e| e.to_string())?;
            let rows = stmt
                .query_map([lineage], |row| Ok((row.get(0)?, row.get(1)?)))
                .map_err(|e| e.to_string())?;
            rows.collect::<Result<_, _>>().map_err(|e| e.to_string())?
        };
        ids.into_iter()
            .map(|(id, state_changed_ms)| {
                let run = broca_core::RunIndex::lookup_run(self, &id)?
                    .ok_or("retention run disappeared")?;
                Ok(RetentionRunRow {
                    run,
                    state_changed_ms,
                })
            })
            .collect()
    }

    pub fn retained_sessions(&self) -> Result<Vec<BindIdentity>, StoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(RETAINED_SESSIONS_SQL)?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
    }
    /// Indexed boot-enrollment lookup. A matching `wal_bytes` lets the caller skip replay.
    pub fn lookup_live_retention_lineage(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<RetentionLineageRow>, StoreError> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT lineage_id, session, retention_s, last_activity_ms, expires_at_ms,
                    wal_bytes, expired_at_ms FROM retention_lineage
             WHERE session=?1 AND expired_at_ms IS NULL",
            [Self::key(session)],
            read_lineage,
        )
        .optional()
        .map_err(StoreError::from)
    }

    /// Lookup by lineage, including an expired lineage superseded at the same address.
    pub fn lookup_retention_lineage(
        &self,
        lineage_id: &str,
    ) -> Result<Option<RetentionLineageRow>, StoreError> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT lineage_id, session, retention_s, last_activity_ms, expires_at_ms,
                    wal_bytes, expired_at_ms FROM retention_lineage WHERE lineage_id=?1",
            [lineage_id],
            read_lineage,
        )
        .optional()
        .map_err(StoreError::from)
    }

    /// Bounded by the partial deadline index. Every candidate still needs WAL confirmation.
    pub fn retention_candidates(
        &self,
        now_ms: u64,
    ) -> Result<Vec<RetentionLineageRow>, StoreError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT lineage_id, session, retention_s, last_activity_ms, expires_at_ms,
                    wal_bytes, expired_at_ms FROM retention_lineage
             WHERE expires_at_ms IS NOT NULL AND expires_at_ms<=?1
             ORDER BY expires_at_ms, lineage_id",
        )?;
        let rows = stmt.query_map([integer(now_ms)?], read_lineage)?;
        rows.collect::<Result<_, _>>().map_err(StoreError::from)
    }

    /// Retention metadata for run-keyed reads; `None` is an unknown run, while an inner null
    /// lineage identifies a legacy or forever run. Expiry is the WAL's instant, not sweep time.
    pub fn lookup_run_retention(&self, run_id: &str) -> Result<Option<RunRetention>, StoreError> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT lineage_id, expired_at_ms, withdrawals_owed FROM run_index WHERE run_id=?1",
            [run_id],
            |row| {
                Ok(RunRetention {
                    lineage_id: row.get(0)?,
                    expired_at_ms: row.get(1)?,
                    withdrawals_owed: row.get(2)?,
                })
            },
        )
        .optional()
        .map_err(StoreError::from)
    }

    /// Atomically re-derive a lineage and every run the caller replayed. This is used for live
    /// enrollment/repair and the sweep's row update after move-aside. Missing admission rows
    /// are inserted, stale active/paused rows take the WAL state, and titles are always NULL.
    /// For deletion, supply `expired_at_ms` and no deadline; all runs must be terminal.
    /// An expired lineage cannot be revived and no row belonging to another lineage is changed.
    pub fn upsert_retention_lineage(
        &self,
        lineage: &RetentionLineageRow,
        runs: &[RetentionRunRow],
    ) -> Result<(), StoreError> {
        self.upsert_retention_lineage_with_withdrawals(lineage, runs, &[])
    }

    /// Expiry accounting and identity-only withdrawal export commit before content deletion.
    pub fn upsert_retention_lineage_with_withdrawals(
        &self,
        lineage: &RetentionLineageRow,
        runs: &[RetentionRunRow],
        questions: &[crate::retention_withdrawals::RetentionWithdrawal],
    ) -> Result<(), StoreError> {
        if lineage.retention_s == 0
            || (lineage.expired_at_ms.is_some() && lineage.expires_at_ms.is_some())
        {
            return Err(StoreError::InconsistentSnapshot("invalid retention row"));
        }
        let session_json = serde_json::to_string(&lineage.session)?;
        // Serialize before taking the connection lock. No content from RunIndexRow is serialized.
        let prepared = runs
            .iter()
            .map(|row| {
                if row.run.session != lineage.session
                    || (lineage.expired_at_ms.is_some()
                        && (!row.run.state.is_terminal() || row.run.withdrawals_owed))
                {
                    return Err(StoreError::InconsistentSnapshot(
                        "retention run has a different session or is not terminal at expiry",
                    ));
                }
                Ok((
                    row,
                    row.run
                        .usage
                        .as_ref()
                        .map(serde_json::to_string)
                        .transpose()?,
                    row.run
                        .final_step_finish_reason
                        .as_ref()
                        .map(serde_json::to_string)
                        .transpose()?,
                    row.state_changed_ms.map(integer).transpose()?,
                ))
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute("INSERT INTO agent_data_writer VALUES(1)", [])?;
        crate::session_refs::insert(
            &tx,
            &broca_core::session_addr(&lineage.session),
            &lineage.session,
        )?;
        for question in questions {
            if question.lineage_id != lineage.lineage_id || question.session != lineage.session {
                return Err(StoreError::InconsistentSnapshot(
                    "withdrawal export has a different retained identity",
                ));
            }
        }
        // Expiry and export are one transaction. Recovery must not resurrect a question
        // that the content-free worker already settled after that transaction committed.
        let exported: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM retention_lineage WHERE lineage_id=?1 AND expired_at_ms IS NOT NULL)",
            params![lineage.lineage_id], |row| row.get(0),
        )?;
        if !exported {
            crate::retention_withdrawals::write_questions(&tx, questions)?;
        }
        let changed = tx.execute(
            "INSERT INTO retention_lineage
                (lineage_id, session, retention_s, last_activity_ms, expires_at_ms,
                 wal_bytes, expired_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(lineage_id) DO UPDATE SET
                retention_s=excluded.retention_s, last_activity_ms=excluded.last_activity_ms,
                expires_at_ms=excluded.expires_at_ms, wal_bytes=excluded.wal_bytes,
                expired_at_ms=excluded.expired_at_ms
             WHERE retention_lineage.session=excluded.session
               AND (retention_lineage.expired_at_ms IS NULL
                    OR retention_lineage.expired_at_ms=excluded.expired_at_ms)",
            params![
                lineage.lineage_id,
                Self::key(&lineage.session),
                integer(lineage.retention_s)?,
                integer(lineage.last_activity_ms)?,
                lineage.expires_at_ms.map(integer).transpose()?,
                integer(lineage.wal_bytes)?,
                lineage.expired_at_ms.map(integer).transpose()?,
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::InconsistentSnapshot(
                "retention lineage changed session or was already expired",
            ));
        }
        for (row, usage_json, finish_reason_json, state_changed_ms) in prepared {
            let changed = tx.execute(
                "INSERT INTO run_index
                    (run_id, session, lineage_id, expired_at_ms, submission_id,
                     request_identity_digest, state, usage_json, final_step_finish_reason_json,
                      retries_used, state_changed_ms, title, withdrawals_owed)
                  VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, NULL, ?12)
                 ON CONFLICT(run_id) DO UPDATE SET
                    lineage_id=excluded.lineage_id, expired_at_ms=excluded.expired_at_ms,
                    submission_id=excluded.submission_id,
                    request_identity_digest=excluded.request_identity_digest,
                    state=excluded.state, usage_json=excluded.usage_json,
                    final_step_finish_reason_json=excluded.final_step_finish_reason_json,
                    retries_used=excluded.retries_used, state_changed_ms=excluded.state_changed_ms,
                     title=NULL, withdrawals_owed=excluded.withdrawals_owed
                 WHERE run_index.session=excluded.session
                   AND (run_index.lineage_id IS NULL OR run_index.lineage_id=excluded.lineage_id)",
                params![
                    row.run.run_id,
                    session_json,
                    lineage.lineage_id,
                    lineage.expired_at_ms.map(integer).transpose()?,
                    row.run.submission_id,
                    row.run.request_identity_digest,
                    row.run.state.as_str(),
                    usage_json,
                    finish_reason_json,
                    i64::from(row.run.retries_used),
                    state_changed_ms,
                    row.run.withdrawals_owed,
                ],
            )?;
            if changed != 1 {
                return Err(StoreError::InconsistentSnapshot(
                    "retention run belongs to another lineage",
                ));
            }
        }
        tx.execute("DELETE FROM agent_data_writer", [])?;
        tx.commit()?;
        Ok(())
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

    #[test]
    fn retention_run_rows_cost_is_bounded_by_lineage_runs() {
        let store = cost_fixture();
        let runs = store.retention_run_rows("live").unwrap();
        assert_eq!(
            runs.iter()
                .map(|row| row.run.run_id.as_str())
                .collect::<Vec<_>>(),
            vec!["a-live", "z-live"]
        );
        assert_eq!(
            runs.iter()
                .map(|row| row.state_changed_ms)
                .collect::<Vec<_>>(),
            vec![None, Some(7)]
        );
        let conn = store.conn.lock().unwrap();
        let mut stmt = conn.prepare(RETENTION_RUN_ROWS_SQL).unwrap();
        let rows = stmt
            .query_map(["live"], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(rows, vec!["a-live", "z-live"]);
        let steps = stmt.get_status(rusqlite::StatementStatus::VmStep);
        assert!(
            steps < 200,
            "lineage lookup walked unrelated runs: {steps} VM steps"
        );
    }

    #[test]
    fn retained_sessions_matches_old_join_including_expired_and_shared_sessions() {
        use std::collections::BTreeSet;

        let store = SqliteStore::open_in_memory().unwrap();
        let expected = ["live", "expired-only", "shared"]
            .map(|s| serde_json::to_string(&BindIdentity::new("/p", "h", s)).unwrap())
            .into_iter()
            .collect::<BTreeSet<_>>();
        let old;
        let new;
        {
            let conn = store.conn.lock().unwrap();
            conn.execute_batch(
                "INSERT INTO retention_lineage VALUES
                ('live', 'live-key', 1, 0, 1, 0, NULL),
                ('expired', 'expired-key', 1, 0, NULL, 0, 2),
                ('fresh', 'shared-key', 1, 0, 1, 0, NULL),
                ('old', 'shared-key', 1, 0, NULL, 0, 2),
                ('no-runs', 'no-runs-key', 1, 0, 1, 0, NULL)",
            )
            .unwrap();
            for (i, (session, lineage)) in [
                ("live", Some("live")),
                ("live", Some("live")),
                ("expired-only", Some("expired")),
                ("shared", Some("fresh")),
                ("shared", Some("old")),
                ("unknown", Some("not-retained")),
                ("null", None),
            ]
            .into_iter()
            .enumerate()
            {
                conn.execute("INSERT INTO run_index (run_id, session, state, lineage_id) VALUES (?1, ?2, 'completed', ?3)",
                    params![format!("run-{i}"), serde_json::to_string(&BindIdentity::new("/p", "h", session)).unwrap(), lineage]).unwrap();
            }
            let sessions = |sql| {
                conn.prepare(sql)
                    .unwrap()
                    .query_map([], |row| row.get::<_, String>(0))
                    .unwrap()
                    .collect::<Result<BTreeSet<_>, _>>()
                    .unwrap()
            };
            old = sessions("SELECT DISTINCT r.session FROM run_index r JOIN retention_lineage l ON r.lineage_id=l.lineage_id");
            new = sessions(RETAINED_SESSIONS_SQL);
        }
        assert_eq!(old, expected);
        assert_eq!(new, old);
        assert_eq!(
            store
                .retained_sessions()
                .unwrap()
                .iter()
                .map(|s| serde_json::to_string(s).unwrap())
                .collect::<BTreeSet<_>>(),
            expected
        );
    }

    #[test]
    fn retention_recovery_does_not_resurrect_settled_withdrawal_export() {
        use crate::retention_withdrawals::{RetentionQuestionId, RetentionWithdrawal};
        let store = SqliteStore::open_in_memory().unwrap();
        let row = run("owed", RunIndexState::Interrupted);
        let mut expired = lineage("a");
        expired.expires_at_ms = None;
        expired.expired_at_ms = Some(60_100);
        let question = RetentionWithdrawal {
            lineage_id: "a".into(), session: expired.session.clone(), call_key: "a:4".into(),
            scope: serde_json::from_value(serde_json::json!({"owner":{"kind":"reserved","module_id":"prefrontal-core"},"ref":"head","scope_epoch":1})).unwrap(),
            question: RetentionQuestionId::Unknown, provider_id: "prefrontal-core".into(), provider_incarnation: None, expires_at_ms: 70_000,
        };
        store
            .upsert_retention_lineage_with_withdrawals(
                &expired,
                std::slice::from_ref(&row),
                std::slice::from_ref(&question),
            )
            .unwrap();
        assert_eq!(store.pending_retention_withdrawals().unwrap().len(), 1);
        assert!(store.settle_retention_withdrawal("a", "a:4").unwrap());
        assert!(!store.settle_retention_withdrawal("a", "a:4").unwrap());
        store
            .upsert_retention_lineage_with_withdrawals(&expired, &[row], &[question])
            .unwrap();
        assert!(store.pending_retention_withdrawals().unwrap().is_empty());
    }
    use super::*;
    use broca_core::{ExportIndex, RunIndex, RunIndexState};
    use broca_protocol::{FinishReason, Usage};

    #[test]
    fn retention_terminal_upsert_preserves_withdrawal_discovery() {
        let store = SqliteStore::open_in_memory().unwrap();
        let mut owed = run("owed", RunIndexState::Interrupted);
        owed.run.withdrawals_owed = true;
        store
            .upsert_retention_lineage(&lineage("a"), &[owed])
            .unwrap();
        assert!(store.lookup_run("owed").unwrap().unwrap().withdrawals_owed);
    }

    #[test]
    fn retention_expiry_refuses_pending_withdrawals() {
        let store = SqliteStore::open_in_memory().unwrap();
        let mut owed = run("owed", RunIndexState::Interrupted);
        owed.run.withdrawals_owed = true;
        let mut expired = lineage("a");
        expired.expires_at_ms = None;
        expired.expired_at_ms = Some(60_100);
        assert!(store.upsert_retention_lineage(&expired, &[owed]).is_err());
    }

    #[test]
    fn retention_expiry_clears_actor_discovery_after_durable_rederivation() {
        let store = SqliteStore::open_in_memory().unwrap();
        let row = run("owed", RunIndexState::Interrupted);
        store
            .upsert_retention_lineage(&lineage("a"), std::slice::from_ref(&row))
            .unwrap();
        store.index_run_withdrawals_owed("owed", true).unwrap();
        let mut expired = lineage("a");
        expired.expires_at_ms = None;
        expired.expired_at_ms = Some(60_100);
        store.upsert_retention_lineage(&expired, &[row]).unwrap();
        assert!(!store.lookup_run("owed").unwrap().unwrap().withdrawals_owed);
    }

    #[test]
    fn retention_rederive_clears_discovery_when_durable_debt_is_settled() {
        let store = SqliteStore::open_in_memory().unwrap();
        let mut row = run("settled", RunIndexState::Interrupted);
        row.run.withdrawals_owed = true;
        store
            .upsert_retention_lineage(&lineage("a"), std::slice::from_ref(&row))
            .unwrap();
        row.run.withdrawals_owed = false;
        store
            .upsert_retention_lineage(&lineage("a"), &[row])
            .unwrap();
        assert!(
            !store
                .lookup_run("settled")
                .unwrap()
                .unwrap()
                .withdrawals_owed,
            "MAX stranded a flag after durable settlement"
        );
    }

    fn lineage(id: &str) -> RetentionLineageRow {
        RetentionLineageRow {
            lineage_id: id.into(),
            session: BindIdentity::new("/project", "harness", "retained"),
            retention_s: 60,
            last_activity_ms: 100,
            expires_at_ms: Some(60_100),
            wal_bytes: 1024,
            expired_at_ms: None,
        }
    }

    fn run(id: &str, state: RunIndexState) -> RetentionRunRow {
        RetentionRunRow {
            run: RunIndexRow {
                run_id: id.into(),
                withdrawals_owed: false,
                session: lineage("a").session,
                submission_id: Some(format!("submission-{id}")),
                request_identity_digest: Some(format!("digest-{id}")),
                state,
                usage: Some(Usage {
                    input_tokens: Some(42),
                    ..Usage::default()
                }),
                final_step_finish_reason: Some(FinishReason::Stop),
                retries_used: 2,
                title: Some("PRIVATE_RETENTION_TITLE_MUST_NOT_BE_STORED".into()),
            },
            state_changed_ms: Some(100),
        }
    }

    fn expired(id: &str) -> RetentionLineageRow {
        RetentionLineageRow {
            expires_at_ms: None,
            expired_at_ms: Some(60_100),
            ..lineage(id)
        }
    }

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

    #[test]
    fn upsert_reconstructs_missing_retention_runs_without_content() {
        let store = SqliteStore::open_in_memory().unwrap();
        let row = lineage("a");
        let runs = [
            run("missing-admission", RunIndexState::Completed),
            run("second-run", RunIndexState::Error),
        ];
        store.upsert_retention_lineage(&row, &runs).unwrap();
        for r in runs {
            let mut expected = r.run.clone();
            expected.title = None;
            assert_eq!(store.lookup_run(&r.run.run_id).unwrap(), Some(expected));
            assert_eq!(
                store.lookup_run_retention(&r.run.run_id).unwrap(),
                Some(RunRetention {
                    withdrawals_owed: false,
                    lineage_id: Some("a".into()),
                    expired_at_ms: None
                })
            );
        }
        assert_eq!(
            store.lookup_live_retention_lineage(&row.session).unwrap(),
            Some(row.clone())
        );
        let conn = store.conn.lock().unwrap();
        let key: String = conn
            .query_row("SELECT session FROM retention_lineage", [], |r| r.get(0))
            .unwrap();
        assert_eq!(key, "/project\u{1f}harness\u{1f}retained");
        for table in ["meta", "message"] {
            let count: i64 = conn
                .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
                .unwrap();
            assert_eq!(count, 0);
        }
    }

    #[test]
    fn expiry_replaces_stale_active_and_paused_with_wal_terminals() {
        let store = SqliteStore::open_in_memory().unwrap();
        let stale = [
            run("active", RunIndexState::Active),
            run("paused", RunIndexState::Paused),
        ];
        // Simulate crashes after terminal WAL appends but before run_index updates.
        for r in &stale {
            let mut row = r.run.clone();
            row.title = None;
            store.index_run_active(&row).unwrap();
        }
        let runs = [
            run("active", RunIndexState::Completed),
            run("paused", RunIndexState::Cancelled),
        ];
        let row = expired("a");
        store.upsert_retention_lineage(&row, &runs).unwrap();
        store.upsert_retention_lineage(&row, &runs).unwrap();
        for r in runs {
            let mut expected = r.run.clone();
            expected.title = None;
            assert_eq!(store.lookup_run(&r.run.run_id).unwrap(), Some(expected));
            assert_eq!(
                store.lookup_run_retention(&r.run.run_id).unwrap(),
                Some(RunRetention {
                    withdrawals_owed: false,
                    lineage_id: Some("a".into()),
                    expired_at_ms: Some(60_100)
                })
            );
        }
        assert_eq!(
            store.lookup_retention_lineage("a").unwrap(),
            Some(row.clone())
        );
        assert_eq!(
            store.lookup_live_retention_lineage(&row.session).unwrap(),
            None
        );
        assert!(store
            .retention_candidates(i64::MAX as u64)
            .unwrap()
            .is_empty());
        let conn = store.conn.lock().unwrap();
        let transition: u64 = conn
            .query_row(
                "SELECT state_changed_ms FROM run_index WHERE run_id='active'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(transition, 100);
    }

    #[test]
    fn retention_deadlines_and_wal_progress_follow_live_repair() {
        let store = SqliteStore::open_in_memory().unwrap();
        let mut row = lineage("a");
        row.expires_at_ms = None; // active or paused
        store.upsert_retention_lineage(&row, &[]).unwrap();
        assert!(store.retention_candidates(100_000).unwrap().is_empty());
        row.retention_s = 30;
        row.last_activity_ms = 200;
        row.expires_at_ms = Some(30_200);
        row.wal_bytes = 2048;
        store.upsert_retention_lineage(&row, &[]).unwrap();
        assert!(store.retention_candidates(30_199).unwrap().is_empty());
        assert_eq!(
            store.retention_candidates(30_200).unwrap(),
            vec![row.clone()]
        );
        assert_eq!(
            store.lookup_live_retention_lineage(&row.session).unwrap(),
            Some(row)
        );
    }

    #[test]
    fn one_live_lineage_per_address_and_old_expiry_preserves_fresh_lineage() {
        let store = SqliteStore::open_in_memory().unwrap();
        store.upsert_retention_lineage(&lineage("a"), &[]).unwrap();
        assert!(store.upsert_retention_lineage(&lineage("b"), &[]).is_err());
        let a = expired("a");
        let a_runs = [run("a-run", RunIndexState::Completed)];
        store.upsert_retention_lineage(&a, &a_runs).unwrap();
        let b = lineage("b");
        let b_runs = [run("b-run", RunIndexState::Active)];
        store.upsert_retention_lineage(&b, &b_runs).unwrap();
        // Recovery retries the old row update after the address has been reused.
        store.upsert_retention_lineage(&a, &a_runs).unwrap();
        assert_eq!(
            store.lookup_live_retention_lineage(&b.session).unwrap(),
            Some(b)
        );
        assert_eq!(
            store.lookup_run("b-run").unwrap().unwrap().state,
            RunIndexState::Active
        );
        assert_eq!(
            store.lookup_run_retention("b-run").unwrap(),
            Some(RunRetention {
                withdrawals_owed: false,
                lineage_id: Some("b".into()),
                expired_at_ms: None
            })
        );
        assert!(store
            .upsert_retention_lineage(&lineage("a"), &a_runs)
            .is_err());
    }

    #[test]
    fn row_update_rolls_back_on_run_id_owned_by_another_lineage() {
        let store = SqliteStore::open_in_memory().unwrap();
        let a = expired("a");
        store
            .upsert_retention_lineage(&a, &[run("shared-run", RunIndexState::Completed)])
            .unwrap();
        let b = lineage("b");
        assert!(store
            .upsert_retention_lineage(
                &b,
                &[
                    run("new-run", RunIndexState::Completed),
                    run("shared-run", RunIndexState::Completed),
                ]
            )
            .is_err());
        assert_eq!(store.lookup_retention_lineage("b").unwrap(), None);
        assert_eq!(store.lookup_run("new-run").unwrap(), None);
        assert_eq!(
            store.lookup_run_retention("shared-run").unwrap(),
            Some(RunRetention {
                withdrawals_owed: false,
                lineage_id: Some("a".into()),
                expired_at_ms: Some(60_100)
            })
        );
    }

    #[test]
    fn expired_row_update_rejects_nonterminal_runs_and_foreign_sessions() {
        let store = SqliteStore::open_in_memory().unwrap();
        for state in [RunIndexState::Active, RunIndexState::Paused] {
            assert!(store
                .upsert_retention_lineage(&expired("a"), &[run("bad-run", state)])
                .is_err());
        }
        let mut foreign = run("foreign", RunIndexState::Completed);
        foreign.run.session.session = "different".into();
        assert!(store
            .upsert_retention_lineage(&expired("a"), &[foreign])
            .is_err());
        assert_eq!(store.lookup_retention_lineage("a").unwrap(), None);
    }

    #[test]
    fn boot_export_catch_up_skips_expired_runs_and_keeps_stored_facts() {
        let store = SqliteStore::open_in_memory().unwrap();
        // Expired no-fact rows must not try to replay a WAL that has been deleted.
        store
            .upsert_retention_lineage(
                &expired("a"),
                &[
                    run("expired-without-facts", RunIndexState::Completed),
                    run("expired-with-facts", RunIndexState::Error),
                ],
            )
            .unwrap();
        for (id, state) in [
            ("forever", RunIndexState::Completed),
            ("active", RunIndexState::Active),
            ("paused", RunIndexState::Paused),
        ] {
            store.index_run_active(&run(id, state).run).unwrap();
        }
        let fact = "{\"immutable\": \"billing bytes\"}";
        store.conn.lock().unwrap().execute(
            "INSERT INTO export_facts(fact_id, run_id, segment_json) VALUES ('fact', 'expired-with-facts', ?1)",
            [fact],
        ).unwrap();
        store
            .upsert_retention_lineage(
                &expired("a"),
                &[
                    run("expired-without-facts", RunIndexState::Completed),
                    run("expired-with-facts", RunIndexState::Error),
                ],
            )
            .unwrap();
        let missing = store.missing_terminal_runs().unwrap();
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].run_id.0, "forever");
        let stored: String = store
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT segment_json FROM export_facts WHERE fact_id='fact'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(stored.as_bytes(), fact.as_bytes());
    }
}

#[cfg(test)]
mod sweep_work_tests {
    use super::*;

    #[test]
    fn quiet_retained_population_does_not_expand_discovery_work() {
        let store = SqliteStore::open_in_memory().unwrap();
        let conn = store.conn.lock().unwrap();
        conn.execute_batch(
            "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<5000)
            INSERT INTO retention_lineage SELECT 'l-'||x, 's-'||x, 1, 0, 9000000, 20, NULL FROM n;",
        )
        .unwrap();
        let sql = crate::retention_withdrawals::DISCOVERY_SQL;
        let mut stmt = conn.prepare(sql).unwrap();
        assert!(stmt.query([]).unwrap().next().unwrap().is_none());
        assert_eq!(stmt.get_status(rusqlite::StatementStatus::FullscanStep), 0);
        assert!(stmt.get_status(rusqlite::StatementStatus::VmStep) < 100);
    }

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
