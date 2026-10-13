//! Content-free retention bookkeeping. The caller derives these rows from the WAL under the
//! session lease; deadlines in this index nominate candidates, never authorize deletion.

use broca_core::RunIndexRow;
use broca_protocol::BindIdentity;
use rusqlite::{params, OptionalExtension};

use crate::{bind_identity_from_store_key, SqliteStore, StoreError};

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
    pub fn retention_run_rows(&self, lineage: &str) -> Result<Vec<RetentionRunRow>, String> {
        let ids: Vec<(String, Option<u64>)> = {
            let conn = self.conn.lock().unwrap();
            let mut stmt = conn.prepare("SELECT run_id, state_changed_ms FROM run_index WHERE lineage_id=?1 ORDER BY run_id").map_err(|e| e.to_string())?;
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
        let mut stmt = conn.prepare("SELECT DISTINCT r.session FROM run_index r JOIN retention_lineage l ON r.lineage_id=l.lineage_id")?;
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
        tx.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
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
