//! Content-free retention progress and withdrawal obligations. WAL data stays in the WAL.
use super::{DbError, OverhaulDb};
use crate::retention::{RetentionLineageRow, RetentionRunRow, RunRetention};
use crate::retention_withdrawals::{RetentionQuestionId, RetentionWithdrawal};
use broca_core::{RunIndexRow, RunIndexState};
use broca_protocol::BindIdentity;
use rusqlite::{params, Connection, OptionalExtension};

pub(super) fn initialize(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS retention_progress (
            lineage_id TEXT PRIMARY KEY, session TEXT NOT NULL, retention_s INTEGER NOT NULL,
            last_activity_ms INTEGER NOT NULL, expires_at_ms INTEGER, wal_bytes INTEGER NOT NULL,
            expired_at_ms INTEGER
         );
         CREATE UNIQUE INDEX IF NOT EXISTS retention_progress_live ON retention_progress(session) WHERE expired_at_ms IS NULL;
         CREATE INDEX IF NOT EXISTS retention_progress_session ON retention_progress(session,lineage_id);
         CREATE INDEX IF NOT EXISTS retention_progress_deadline ON retention_progress(expires_at_ms,lineage_id) WHERE expires_at_ms IS NOT NULL;
         CREATE TABLE IF NOT EXISTS retention_run (
            run_id TEXT PRIMARY KEY, lineage_id TEXT NOT NULL REFERENCES retention_progress(lineage_id),
            state TEXT NOT NULL, state_changed_ms INTEGER, submission_id TEXT,
            request_identity_digest TEXT, withdrawals_owed INTEGER NOT NULL
         );
         CREATE INDEX IF NOT EXISTS retention_run_lineage ON retention_run(lineage_id);
         CREATE INDEX IF NOT EXISTS retention_run_discovery ON retention_run(lineage_id) WHERE withdrawals_owed=1;
         CREATE TABLE IF NOT EXISTS retention_deletion (lineage_id TEXT PRIMARY KEY, session TEXT NOT NULL);
         CREATE INDEX IF NOT EXISTS retention_deletion_session ON retention_deletion(session,lineage_id);
         CREATE TABLE IF NOT EXISTS retention_withdrawal (
            lineage_id TEXT NOT NULL, session TEXT NOT NULL, call_key TEXT NOT NULL,
            scope_json TEXT NOT NULL, elicitation_id TEXT, provider_id TEXT NOT NULL,
            provider_incarnation TEXT, expires_at_ms TEXT NOT NULL,
            state TEXT NOT NULL CHECK(state IN ('ready','id_unknown','scope_unknown')),
            PRIMARY KEY(lineage_id,call_key)
         );
         CREATE INDEX IF NOT EXISTS retention_withdrawal_session ON retention_withdrawal(session,lineage_id);
         CREATE TABLE IF NOT EXISTS retention_withdrawal_stop (
            lineage_id TEXT NOT NULL, call_key TEXT NOT NULL, reason TEXT NOT NULL,
            PRIMARY KEY(lineage_id,call_key)
         );",
    )
}

fn invalid(error: impl std::fmt::Display) -> DbError {
    DbError::Invalid(error.to_string())
}
fn integer(value: u64) -> Result<i64, DbError> {
    i64::try_from(value).map_err(invalid)
}
fn session_key(session: &BindIdentity) -> Result<String, DbError> {
    serde_json::to_string(session).map_err(invalid)
}
fn lineage(row: &rusqlite::Row<'_>) -> rusqlite::Result<RetentionLineageRow> {
    let session: String = row.get(1)?;
    Ok(RetentionLineageRow {
        lineage_id: row.get(0)?,
        session: serde_json::from_str(&session).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Text, Box::new(e))
        })?,
        retention_s: row.get(2)?,
        last_activity_ms: row.get(3)?,
        expires_at_ms: row.get(4)?,
        wal_bytes: row.get(5)?,
        expired_at_ms: row.get(6)?,
    })
}
const COLUMNS: &str =
    "lineage_id,session,retention_s,last_activity_ms,expires_at_ms,wal_bytes,expired_at_ms";

impl OverhaulDb {
    pub fn pending_retention_withdrawals(&self) -> Result<Vec<RetentionWithdrawal>, DbError> {
        self.read_snapshot(|tx| {
            let rows = tx.prepare("SELECT lineage_id,session,call_key,scope_json,elicitation_id,provider_id,provider_incarnation,expires_at_ms,state FROM retention_withdrawal ORDER BY lineage_id,call_key")?
                .query_map([], |r| Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,String>(3)?,r.get::<_,Option<String>>(4)?,r.get::<_,String>(5)?,r.get::<_,Option<String>>(6)?,r.get::<_,String>(7)?,r.get::<_,String>(8)?)))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            rows.into_iter().map(|(lineage_id,session,call_key,scope,id,provider_id,provider_incarnation,expires,state)| {
                let question = match (id,state.as_str()) {
                    (Some(id), "ready" | "scope_unknown") => RetentionQuestionId::Known(id),
                    (None, "id_unknown" | "scope_unknown") => RetentionQuestionId::Unknown,
                    _ => return Err(invalid("invalid content-free withdrawal identity")),
                };
                let scope: Option<crate::retention_withdrawals::RetentionScopeIdentity> = serde_json::from_str(&scope).map_err(invalid)?;
                if scope.is_none() != (state == "scope_unknown") { return Err(invalid("invalid content-free scope state")); }
                Ok(RetentionWithdrawal { lineage_id,session:serde_json::from_str(&session).map_err(invalid)?,call_key,scope,question,provider_id,provider_incarnation,expires_at_ms:expires.parse().map_err(invalid)? })
            }).collect()
        })
    }
    pub fn settle_retention_withdrawal(&self, lineage: &str, key: &str) -> Result<bool, DbError> {
        self.authoritative_transaction(|tx| {
            Ok(tx.execute(
                "DELETE FROM retention_withdrawal WHERE lineage_id=?1 AND call_key=?2",
                params![lineage, key],
            )? == 1)
        })
    }
    pub fn give_up_retention_withdrawal(&self, lineage: &str, key: &str) -> Result<bool, DbError> {
        self.authoritative_transaction(|tx| {
            let changed = tx.execute("INSERT INTO retention_withdrawal_stop SELECT lineage_id,call_key,'withdrawal_retry_exhausted' FROM retention_withdrawal WHERE lineage_id=?1 AND call_key=?2 ON CONFLICT DO NOTHING", params![lineage,key])?;
            tx.execute("DELETE FROM retention_withdrawal WHERE lineage_id=?1 AND call_key=?2", params![lineage,key])?;
            Ok(changed == 1)
        })
    }
    pub fn retention_withdrawal_stop_reason(
        &self,
        lineage: &str,
        key: &str,
    ) -> Result<Option<String>, DbError> {
        self.read_snapshot(|tx| Ok(tx.query_row("SELECT reason FROM retention_withdrawal_stop WHERE lineage_id=?1 AND call_key=?2", params![lineage,key], |r| r.get(0)).optional()?))
    }
    pub fn retention_lineages_with_discovery(&self) -> Result<Vec<BindIdentity>, DbError> {
        self.read_snapshot(|tx| {
            super::recovery::ensure_complete(tx)?;
            let rows = tx.prepare("WITH owed AS (SELECT run_id FROM actor_withdrawal_hint WHERE owed=1 UNION SELECT run_id FROM withdrawal_call WHERE decided=0 AND withdrawal_terminal=1 AND acknowledged=0 UNION SELECT run_id FROM retention_run WHERE withdrawals_owed=1) SELECT DISTINCT p.session FROM owed JOIN retention_run r USING(run_id) JOIN retention_progress p USING(lineage_id) WHERE p.expired_at_ms IS NULL")?
                .query_map([], |r| r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
            rows.into_iter().map(|s| serde_json::from_str(&s).map_err(invalid)).collect()
        })
    }
    pub fn retention_live_inventory(&self) -> Result<Vec<RetentionLineageRow>, DbError> {
        self.read_snapshot(|tx| {
            let rows = tx
                .prepare(&format!(
                    "SELECT {COLUMNS} FROM retention_progress WHERE expired_at_ms IS NULL"
                ))?
                .query_map([], lineage)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
    }
    pub fn lookup_live_retention_lineage(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<RetentionLineageRow>, DbError> {
        self.read_snapshot(|tx| Ok(tx.query_row(
            &format!("SELECT {COLUMNS} FROM retention_progress WHERE session=?1 AND expired_at_ms IS NULL"),
            [session_key(session)?], lineage,
        ).optional()?))
    }
    pub fn lookup_retention_lineage(
        &self,
        id: &str,
    ) -> Result<Option<RetentionLineageRow>, DbError> {
        self.read_snapshot(|tx| {
            Ok(tx
                .query_row(
                    &format!("SELECT {COLUMNS} FROM retention_progress WHERE lineage_id=?1"),
                    [id],
                    lineage,
                )
                .optional()?)
        })
    }
    pub fn retention_candidates(&self, now: u64) -> Result<Vec<RetentionLineageRow>, DbError> {
        self.read_snapshot(|tx| {
            let rows = tx.prepare(&format!("SELECT {COLUMNS} FROM retention_progress WHERE expires_at_ms IS NOT NULL AND expires_at_ms<=?1 ORDER BY expires_at_ms,lineage_id"))?
                .query_map([integer(now)?], lineage)?.collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
    }
    pub fn retained_sessions(&self) -> Result<Vec<BindIdentity>, DbError> {
        self.read_snapshot(|tx| {
            let rows = tx
                .prepare("SELECT DISTINCT session FROM retention_progress")?
                .query_map([], |r| r.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            rows.into_iter()
                .map(|s| serde_json::from_str(&s).map_err(invalid))
                .collect()
        })
    }
    pub fn retention_deletion_pending(
        &self,
        id: &str,
        session: &BindIdentity,
        pending: bool,
    ) -> Result<(), DbError> {
        self.authoritative_transaction(|tx| {
            if pending {
                tx.execute("INSERT INTO retention_deletion VALUES(?1,?2) ON CONFLICT(lineage_id) DO UPDATE SET session=excluded.session", params![id,session_key(session)?])?;
            } else { tx.execute("DELETE FROM retention_deletion WHERE lineage_id=?1", [id])?; }
            Ok(())
        })
    }
    pub fn retention_pending_deletions(&self) -> Result<Vec<BindIdentity>, DbError> {
        self.read_snapshot(|tx| {
            let rows = tx
                .prepare("SELECT DISTINCT session FROM retention_deletion")?
                .query_map([], |r| r.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            rows.into_iter()
                .map(|s| serde_json::from_str(&s).map_err(invalid))
                .collect()
        })
    }
    pub fn lookup_run_retention(&self, id: &str) -> Result<Option<RunRetention>, DbError> {
        self.read_snapshot(|tx| {
            let retained = tx.query_row(
                "SELECT p.lineage_id,p.expired_at_ms,r.withdrawals_owed FROM retention_run r JOIN retention_progress p USING(lineage_id) WHERE r.run_id=?1", [id],
                |r| Ok(RunRetention { lineage_id:r.get(0)?, expired_at_ms:r.get(1)?, withdrawals_owed:r.get(2)? }),
            ).optional()?;
            if let Some(mut retained)=retained {
                retained.withdrawals_owed=super::recovery::owed(tx,id)?;
                return Ok(Some(retained));
            }
            let exists = tx.query_row("SELECT EXISTS(SELECT 1 FROM run WHERE run_id=?1)", [id], |r| r.get::<_, bool>(0))?;
            Ok(exists.then_some(RunRetention { lineage_id:None, expired_at_ms:None, withdrawals_owed:super::recovery::owed(tx,id)? }))
        })
    }
    /// Lifecycle witnesses contain no usage, titles or finish payloads. Accounting
    /// consumers must hydrate their WAL sources, not treat these rows as usage facts.
    pub fn retention_run_rows(&self, id: &str) -> Result<Vec<RetentionRunRow>, String> {
        self.read_snapshot(|tx| {
            let rows = tx.prepare("SELECT r.run_id,p.session,r.state,r.state_changed_ms,r.submission_id,r.request_identity_digest,r.withdrawals_owed FROM retention_run r JOIN retention_progress p USING(lineage_id) WHERE lineage_id=?1 ORDER BY run_id")?
                .query_map([id], |r| Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,Option<u64>>(3)?,r.get::<_,Option<String>>(4)?,r.get::<_,Option<String>>(5)?,r.get::<_,bool>(6)?)))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            rows.into_iter().map(|(id,session,state,at,submission_id,digest,_)| {
                let owed=super::recovery::owed(tx,&id)?;
                Ok(RetentionRunRow {
                attribution:None, state_changed_ms:at, run:RunIndexRow {
                    run_id:id, session:serde_json::from_str(&session).map_err(invalid)?,
                    state:RunIndexState::parse(&state).ok_or_else(|| invalid("unknown retained run state"))?,
                    submission_id, request_identity_digest:digest, withdrawals_owed:owed,
                    usage:None, final_step_finish_reason:None, retries_used:0, title:None,
                },
                })
            }).collect()
        }).map_err(|e| e.to_string())
    }
    pub fn stamp_retained_run_lineage(
        &self,
        id: &str,
        session: &BindIdentity,
        lineage: &str,
    ) -> Result<bool, DbError> {
        self.authoritative_transaction(|tx| Ok(tx.execute(
            "INSERT OR IGNORE INTO retention_run SELECT r.run_id,?3,r.state,NULL,r.submission_id,r.request_identity_digest,0 FROM run r JOIN session s ON s.sid=r.sid WHERE r.run_id=?1 AND s.sid=?2 AND EXISTS(SELECT 1 FROM retention_progress WHERE lineage_id=?3 AND session=?4 AND expired_at_ms IS NULL)",
            params![id,super::index::sid(session)?,lineage,session_key(session)?],
        )? == 1))
    }
    pub fn upsert_retention_lineage(
        &self,
        row: &RetentionLineageRow,
        runs: &[RetentionRunRow],
    ) -> Result<(), DbError> {
        self.upsert_retention_lineage_with_withdrawals(row, runs, &[])
    }
    pub fn upsert_retention_lineage_with_withdrawals(
        &self,
        row: &RetentionLineageRow,
        runs: &[RetentionRunRow],
        questions: &[RetentionWithdrawal],
    ) -> Result<(), DbError> {
        if row.retention_s == 0 || (row.expired_at_ms.is_some() && row.expires_at_ms.is_some()) {
            return Err(invalid("invalid retention row"));
        }
        for run in runs {
            if run.run.session != row.session
                || (row.expired_at_ms.is_some()
                    && (!run.run.state.is_terminal() || run.run.withdrawals_owed))
            {
                return Err(invalid(
                    "retention run has a different session or is not terminal at expiry",
                ));
            }
        }
        if questions
            .iter()
            .any(|q| q.lineage_id != row.lineage_id || q.session != row.session)
        {
            return Err(invalid(
                "withdrawal export has a different retained identity",
            ));
        }
        self.authoritative_transaction(|tx| {
            super::inventory::insert(tx, &broca_core::session_addr(&row.session), &row.session)?;
            let exported = tx.query_row("SELECT EXISTS(SELECT 1 FROM retention_progress WHERE lineage_id=?1 AND expired_at_ms IS NOT NULL)", [&row.lineage_id], |r| r.get::<_,bool>(0))?;
            if !exported { crate::retention_withdrawals::write_questions(tx, questions).map_err(invalid)?; }
            let changed = tx.execute(
                "INSERT INTO retention_progress VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(lineage_id) DO UPDATE SET retention_s=excluded.retention_s,last_activity_ms=excluded.last_activity_ms,expires_at_ms=excluded.expires_at_ms,wal_bytes=excluded.wal_bytes,expired_at_ms=excluded.expired_at_ms WHERE retention_progress.session=excluded.session AND (retention_progress.expired_at_ms IS NULL OR retention_progress.expired_at_ms=excluded.expired_at_ms)",
                params![row.lineage_id, session_key(&row.session)?, integer(row.retention_s)?, integer(row.last_activity_ms)?, row.expires_at_ms.map(integer).transpose()?, integer(row.wal_bytes)?, row.expired_at_ms.map(integer).transpose()?],
            )?;
            if changed != 1 { return Err(invalid("retention lineage changed session or was already expired")); }
            for run in runs {
                let changed = tx.execute(
                    "INSERT INTO retention_run VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(run_id) DO UPDATE SET state=excluded.state,state_changed_ms=excluded.state_changed_ms,submission_id=excluded.submission_id,request_identity_digest=excluded.request_identity_digest,withdrawals_owed=excluded.withdrawals_owed WHERE retention_run.lineage_id=excluded.lineage_id",
                    params![run.run.run_id,row.lineage_id,run.run.state.as_str(),run.state_changed_ms.map(integer).transpose()?,run.run.submission_id,run.run.request_identity_digest,run.run.withdrawals_owed],
                )?;
                if changed != 1 { return Err(invalid("retention run belongs to another lineage")); }
            }
            Ok(())
        })
    }
}
