//! Identity-only withdrawal discovery; the actor still confirms and settles WAL obligations.
use super::{DbError, OverhaulDb};
use broca_protocol::{BindIdentity, RunId};
use broca_wal::{FramedRecord, WalRecord};
use rusqlite::{params, Connection, OptionalExtension};

const PROJECTION_VERSION: i64 = 1;

pub(super) fn initialize(conn: &Connection) -> rusqlite::Result<()> {
    let had_fold: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='withdrawal_call')",
        [],
        |r| r.get(0),
    )?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS withdrawal_projection (
            sid TEXT PRIMARY KEY REFERENCES session(sid) ON DELETE CASCADE, version INTEGER NOT NULL
         );
         CREATE TABLE IF NOT EXISTS withdrawal_call (
            call_key TEXT NOT NULL, sid TEXT NOT NULL REFERENCES session(sid) ON DELETE CASCADE,
            run_id TEXT, batch_id TEXT NOT NULL, call_id TEXT NOT NULL,
            decided INTEGER NOT NULL, withdrawal_terminal INTEGER NOT NULL, acknowledged INTEGER NOT NULL,
            PRIMARY KEY(sid,call_key)
         );
         CREATE INDEX IF NOT EXISTS withdrawal_call_anchor ON withdrawal_call(sid,batch_id,call_id,call_key);
         CREATE INDEX IF NOT EXISTS withdrawal_call_owed ON withdrawal_call(run_id) WHERE decided=0 AND withdrawal_terminal=1 AND acknowledged=0;
         CREATE TABLE IF NOT EXISTS actor_withdrawal_hint (
            run_id TEXT PRIMARY KEY, owed INTEGER NOT NULL CHECK(owed IN (0,1))
         );
         CREATE INDEX IF NOT EXISTS actor_withdrawal_owed ON actor_withdrawal_hint(run_id) WHERE owed=1;",
    )?;
    if !had_fold {
        conn.execute("DELETE FROM withdrawal_projection", [])?;
    }
    Ok(())
}

pub(super) fn projection_current(conn: &Connection, sid: &str) -> Result<bool, DbError> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM withdrawal_projection WHERE sid=?1 AND version=?2)",
        params![sid, PROJECTION_VERSION],
        |r| r.get(0),
    )?)
}

pub(super) fn mark_projection_current(conn: &Connection, sid: &str) -> Result<(), DbError> {
    conn.execute("INSERT INTO withdrawal_projection VALUES(?1,?2) ON CONFLICT(sid) DO UPDATE SET version=excluded.version",params![sid,PROJECTION_VERSION])?;
    Ok(())
}

/// A WAL cursor authenticates bytes, not the presence of a fold added later.
/// Until every indexed session has replayed that fold, absence is unknown.
pub(super) fn ensure_complete(conn: &Connection) -> Result<(), DbError> {
    let incomplete:bool=conn.query_row("SELECT EXISTS(SELECT 1 FROM session s WHERE NOT EXISTS(SELECT 1 FROM withdrawal_projection p WHERE p.sid=s.sid AND p.version=?1))",[PROJECTION_VERSION],|r|r.get(0))?;
    if incomplete {
        return Err(DbError::Invalid(
            "withdrawal projection incomplete; reconcile the session indexes from the WAL".into(),
        ));
    }
    Ok(())
}

pub(super) fn observe(
    tx: &Connection,
    sid: &str,
    lineage: Option<&str>,
    current_run: Option<&str>,
    frame: &FramedRecord,
) -> Result<(), DbError> {
    match &frame.record {
        WalRecord::ToolCallDeferred {
            batch_id, call_id, ..
        } => {
            let lineage = lineage.ok_or_else(|| {
                DbError::Invalid("withdrawal discovery requires a WAL lineage".into())
            })?;
            tx.execute(
                "INSERT OR IGNORE INTO withdrawal_call VALUES(?1,?2,?3,?4,?5,0,0,0)",
                params![
                    format!("{lineage}:{}", frame.seq),
                    sid,
                    current_run,
                    batch_id.0,
                    call_id.0
                ],
            )?;
        }
        WalRecord::ApprovalRecorded { call_key, .. } => {
            tx.execute(
                "UPDATE withdrawal_call SET decided=1 WHERE sid=?1 AND call_key=?2",
                params![sid, call_key],
            )?;
        }
        WalRecord::QuestionWithdrawalAcknowledged { call_key, .. } => {
            tx.execute(
                "UPDATE withdrawal_call SET acknowledged=1 WHERE sid=?1 AND call_key=?2",
                params![sid, call_key],
            )?;
        }
        WalRecord::ToolResult {
            batch_id,
            result,
            outcome,
            withdraw_elicitation,
            ..
        } => {
            let key=match withdraw_elicitation {
                Some(withdrawal)=>Some(withdrawal.call_key.clone()),
                None=>tx.query_row("SELECT call_key FROM withdrawal_call WHERE sid=?1 AND batch_id=?2 AND call_id=?3 ORDER BY call_key LIMIT 1",params![sid,batch_id.0,result.tool_call_id.0],|r|r.get(0)).optional()?,
            };
            if let Some(key) = key {
                let terminal = withdraw_elicitation.is_some()
                    || (*outcome == broca_wal::DispatchOutcome::Interrupted
                        && **result
                            == broca_core::resume::not_dispatched_result(&result.tool_call_id));
                tx.execute("UPDATE withdrawal_call SET withdrawal_terminal=?3 WHERE sid=?1 AND call_key=?2",params![sid,key,terminal])?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub(super) fn owed(conn: &Connection, run: &str) -> Result<bool, DbError> {
    let sid: Option<String> = conn
        .query_row("SELECT sid FROM run WHERE run_id=?1", [run], |r| r.get(0))
        .optional()?;
    if let Some(sid) = sid {
        if !projection_current(conn, &sid)? {
            return Err(DbError::Invalid(
                "withdrawal projection incomplete; reconcile the session index from the WAL".into(),
            ));
        }
    }
    Ok(conn.query_row(
        "SELECT (EXISTS(SELECT 1 FROM actor_withdrawal_hint WHERE run_id=?1 AND owed=1)
            OR EXISTS(SELECT 1 FROM withdrawal_call WHERE run_id=?1 AND decided=0 AND withdrawal_terminal=1 AND acknowledged=0)
            OR EXISTS(SELECT 1 FROM retention_run WHERE run_id=?1 AND withdrawals_owed=1))
         AND NOT EXISTS(SELECT 1 FROM retention_run r JOIN retention_progress p USING(lineage_id) WHERE r.run_id=?1 AND p.expired_at_ms IS NOT NULL)",
        [run],|r|r.get(0),
    )?)
}

impl OverhaulDb {
    /// The actor writes its conservative hint before publishing a terminal state,
    /// and clears it only after WAL confirmation. `OverhaulDb::rebuild` and
    /// `OverhaulDb::rebuild_session` preserve this authoritative hint.
    pub fn index_run_withdrawals_owed(&self, id: &str, owed: bool) -> Result<(), String> {
        self.authoritative_transaction(|tx| {
            let exists:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM run WHERE run_id=?1)",[id],|r|r.get(0))?;
            if !exists { return Err(DbError::Invalid(format!("cannot record withdrawal debt: run '{id}' has no index row"))); }
            tx.execute("INSERT INTO actor_withdrawal_hint VALUES(?1,?2) ON CONFLICT(run_id) DO UPDATE SET owed=excluded.owed",params![id,owed])?;
            tx.execute("UPDATE retention_run SET withdrawals_owed=?2 WHERE run_id=?1",params![id,owed])?;
            Ok(())
        }).map_err(|e|e.to_string())
    }

    pub fn runs_with_withdrawals_owed(&self) -> Result<Vec<broca_core::WithdrawalRun>, String> {
        self.read_snapshot(|tx| {
            ensure_complete(tx)?;
            let ids=tx.prepare("SELECT run_id FROM actor_withdrawal_hint WHERE owed=1 UNION SELECT run_id FROM withdrawal_call WHERE decided=0 AND withdrawal_terminal=1 AND acknowledged=0 UNION SELECT run_id FROM retention_run WHERE withdrawals_owed=1 ORDER BY run_id")?
                .query_map([],|r|r.get::<_,Option<String>>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
            let mut runs=Vec::new();
            // A Prepared call before RunStarted has no run owner. Keep the call
            // in the fold for WAL confirmation, but never assign it a later run.
            for id in ids.into_iter().flatten() {
                if !owed(tx,&id)? { continue; }
                let session=tx.query_row("SELECT s.project_root,s.harness,s.session_key FROM run r JOIN session s ON s.sid=r.sid WHERE r.run_id=?1",[&id],|r|Ok(BindIdentity::new(r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?))).optional()?;
                if let Some(session)=session { runs.push(broca_core::WithdrawalRun { run_id:RunId(id),session }); }
            }
            Ok(runs)
        }).map_err(|e|e.to_string())
    }
}
