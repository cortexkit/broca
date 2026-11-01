//! Bounded, content-free diagnostic evidence derived in the publication transaction.
use super::{DbError, OverhaulDb};
use crate::diagnostics::{Evidence, RunEvidence, SCAN_LIMIT};
use broca_core::RunIndexAttribution;
use broca_wal::WalRecord;
use rusqlite::{params, Connection, OptionalExtension};

pub trait DiagnosticEvidenceReader: Send + Sync {
    fn diagnostic_evidence(&self, since: i64, now: i64) -> Result<Evidence, String>;
}

pub(super) fn initialize(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS operational_run (
            run_id TEXT PRIMARY KEY REFERENCES run(run_id) ON DELETE CASCADE,
            sid TEXT NOT NULL REFERENCES session(sid) ON DELETE CASCADE,
            state_changed_ms INTEGER, provider TEXT, credential TEXT, error_class TEXT,
            error_code TEXT, pause_reason TEXT, interrupt_reason TEXT
         );
         CREATE INDEX IF NOT EXISTS operational_run_time ON operational_run(state_changed_ms);
         CREATE INDEX IF NOT EXISTS run_state ON run(state);",
    )
}

fn cap(value: Option<String>, limit: usize) -> Option<String> {
    value.map(|value| value.chars().take(limit).collect())
}

pub(super) fn observe(
    conn: &Connection,
    current_run: Option<&str>,
    record: &WalRecord,
) -> Result<(), DbError> {
    let id = match record {
        WalRecord::RunStarted { run_id, .. } | WalRecord::RunPaused { run_id, .. } => &run_id.0,
        WalRecord::RunFinished { .. } => match current_run {
            Some(id) => id,
            None => return Ok(()),
        },
        _ => return Ok(()),
    };
    let mut attribution = conn.query_row(
        "SELECT state_changed_ms,provider,credential,error_class,error_code,pause_reason,interrupt_reason FROM operational_run WHERE run_id=?1", [id],
        |r| Ok(RunIndexAttribution { at_ms:r.get(0)?,provider_module_id:r.get(1)?,credential_id:r.get(2)?,error_class:r.get(3)?,error_code:r.get(4)?,pause_reason:r.get(5)?,interrupt_reason:r.get(6)? }),
    ).optional()?.unwrap_or_default();
    attribution.observe(record);
    if let WalRecord::RunStarted { ts_ms, .. } = record {
        attribution.at_ms = *ts_ms;
    }
    conn.execute(
        "INSERT INTO operational_run SELECT ?1,sid,?2,?3,?4,?5,?6,?7,?8 FROM run WHERE run_id=?1 ON CONFLICT(run_id) DO UPDATE SET state_changed_ms=excluded.state_changed_ms,provider=excluded.provider,credential=excluded.credential,error_class=excluded.error_class,error_code=excluded.error_code,pause_reason=excluded.pause_reason,interrupt_reason=excluded.interrupt_reason",
        params![id,attribution.at_ms.and_then(|v| i64::try_from(v).ok()),cap(attribution.provider_module_id,256),cap(attribution.credential_id,512),cap(attribution.error_class,64),cap(attribution.error_code,128),attribution.pause_reason,cap(attribution.interrupt_reason,128)],
    )?;
    Ok(())
}

impl DiagnosticEvidenceReader for crate::SqliteStore {
    fn diagnostic_evidence(&self, since: i64, now: i64) -> Result<Evidence, String> {
        crate::SqliteStore::diagnostic_evidence(self, since, now)
    }
}

impl DiagnosticEvidenceReader for OverhaulDb {
    fn diagnostic_evidence(&self, since: i64, now: i64) -> Result<Evidence, String> {
        self.read_snapshot(|tx| {
            let missing: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM run r LEFT JOIN operational_run o USING(run_id) WHERE o.run_id IS NULL AND r.state IN ('active','paused','error','interrupted','transform_unavailable'))", [], |r| r.get(0))?;
            if missing { return Err(DbError::Invalid("diagnostic attribution index is incomplete; rebuild the affected sessions".into())); }
            let read = |predicate: &str, timed: bool, live: bool| -> Result<Vec<RunEvidence>, DbError> {
                let from = if live { "run r JOIN operational_run o ON r.run_id=o.run_id" }
                    else { "operational_run o INDEXED BY operational_run_time JOIN run r ON r.run_id=o.run_id" };
                let window = if live { "1" } else if timed { "o.state_changed_ms>=?1 AND o.state_changed_ms<=?2" } else { "o.state_changed_ms IS NULL" };
                let query = format!("SELECT substr(r.run_id,1,256),r.state,o.state_changed_ms,o.provider,o.credential,o.error_class,o.error_code,o.pause_reason,o.interrupt_reason FROM {from} WHERE {predicate} AND {window} ORDER BY o.state_changed_ms DESC,o.rowid DESC LIMIT ?3");
                let rows = tx.prepare(&query)?.query_map(params![since,now,(SCAN_LIMIT+1) as i64], |r| Ok(RunEvidence {
                    run_id:r.get(0)?,state:r.get(1)?,at_ms:r.get(2)?,provider:r.get(3)?,credential:r.get(4)?,class:r.get(5)?,code:r.get(6)?,pause_reason:r.get(7)?,interrupt_reason:r.get(8)?,
                }))?.collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            };
            Ok(Evidence {
                pending_recovery: Vec::new(),
                live:read("r.state IN ('active','paused')",false,true)?,
                errors:read("r.state='error'",true,false)?,
                interruptions:read("r.state IN ('interrupted','transform_unavailable')",true,false)?,
                untimed_errors:read("r.state='error'",false,false)?,
                untimed_interruptions:read("r.state IN ('interrupted','transform_unavailable')",false,false)?,
            })
        }).map_err(|e| e.to_string())
    }
}

pub fn operational_database_name(root: &std::path::Path) -> Result<&'static str, String> {
    match std::fs::read(root.join("state-format")) {
        Ok(bytes) if bytes == b"broca-state/2" || bytes == b"broca-state/2\n" => Ok("broca.db"),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok("run-index.db"),
        Ok(_) => Err("unsupported state-format marker for operational reader".into()),
        Err(e) => Err(e.to_string()),
    }
}

pub fn open_diagnostic_reader(
    root: &std::path::Path,
    config: broca_wal::WalConfig,
) -> Result<Box<dyn DiagnosticEvidenceReader>, String> {
    if operational_database_name(root)? == "broca.db" {
        OverhaulDb::open_read_only(root)
            .map(|db| Box::new(db) as Box<dyn DiagnosticEvidenceReader>)
            .map_err(|e| e.to_string())
    } else {
        crate::SqliteStore::open_read_only(root.join("run-index.db"))
            .map(|db| Box::new(db.with_wal_config(config)) as Box<dyn DiagnosticEvidenceReader>)
            .map_err(|e| e.to_string())
    }
}
