//! Bounded scalar reads for diagnosis. These queries never join transcript tables or decode logs.
use crate::SqliteStore;
use broca_core::RunIndexAttribution;
use rusqlite::{params, Connection};

pub const SCAN_LIMIT: usize = 20_000;
const COLUMNS: &str = "substr(run_id,1,256), state, state_changed_ms, substr(provider_module_id,1,256), substr(credential_id,1,512), substr(error_class,1,64), substr(error_code,1,128), pause_reason, substr(interrupt_reason,1,128)";
pub const ERROR_PREDICATE: &str = "state='error'";
pub const INTERRUPTED_PREDICATE: &str = "state IN ('interrupted','transform_unavailable')";
pub const LIVE_PREDICATE: &str = "state IN ('active','paused')";

pub(crate) fn init_schema(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS diagnostics_errors_time ON run_index(state_changed_ms) WHERE state='error';
         CREATE INDEX IF NOT EXISTS diagnostics_interruptions_time ON run_index(state_changed_ms) WHERE state IN ('interrupted','transform_unavailable');",
    )
}

/// An update without attribution must not erase attribution recorded by an earlier update.
/// Repeating publication of the same state also preserves its original observation time.
pub(crate) fn write_lifecycle(
    conn: &Connection,
    run_id: &str,
    state: &str,
    usage: Option<&str>,
    finish_reason: Option<&str>,
    retries: u32,
    attribution: Option<&RunIndexAttribution>,
) -> rusqlite::Result<usize> {
    let empty = RunIndexAttribution::default();
    let a = attribution.unwrap_or(&empty);
    conn.execute(
        "UPDATE run_index SET state=?2, usage_json=?3,
         final_step_finish_reason_json=COALESCE(?4,final_step_finish_reason_json), retries_used=?5,
         state_changed_ms=CASE WHEN ?13 THEN ?6 WHEN state=?2 THEN COALESCE(state_changed_ms,?6) ELSE ?6 END,
         provider_module_id=COALESCE(?7,provider_module_id), credential_id=COALESCE(?8,credential_id),
         error_class=CASE WHEN state=?2 OR NOT ?14 THEN COALESCE(?9,error_class) ELSE ?9 END,
         error_code=CASE WHEN state=?2 OR NOT ?14 THEN COALESCE(?10,error_code) ELSE ?10 END,
         pause_reason=CASE WHEN state=?2 OR NOT ?14 THEN COALESCE(?11,pause_reason) ELSE ?11 END,
         interrupt_reason=CASE WHEN state=?2 OR NOT ?14 THEN COALESCE(?12,interrupt_reason) ELSE ?12 END
         WHERE run_id=?1 AND (?2!='paused' OR state IN ('active','paused'))",
        params![run_id,state,usage,finish_reason,retries,
            if attribution.is_some() { a.at_ms.and_then(|ms| i64::try_from(ms).ok()) } else { Some(SqliteStore::now_epoch_ms()) },
            a.provider_module_id,a.credential_id,a.error_class,a.error_code,a.pause_reason,a.interrupt_reason,
            a.at_ms.is_some(), attribution.is_some()],
    )
}

#[derive(Debug, Clone)]
pub struct RunEvidence {
    pub run_id: String,
    pub state: String,
    pub at_ms: Option<i64>,
    pub provider: Option<String>,
    pub credential: Option<String>,
    pub class: Option<String>,
    pub code: Option<String>,
    pub pause_reason: Option<String>,
    pub interrupt_reason: Option<String>,
}

#[derive(Default)]
pub struct Evidence {
    pub live: Vec<RunEvidence>,
    pub errors: Vec<RunEvidence>,
    pub interruptions: Vec<RunEvidence>,
    pub untimed_errors: Vec<RunEvidence>,
    pub untimed_interruptions: Vec<RunEvidence>,
}

/// The exact statements are exposed for query-plan verification against deployment copies.
pub fn queries() -> [String; 5] {
    let window = |index: &str, predicate: &str| {
        format!(
        "SELECT {COLUMNS} FROM run_index INDEXED BY {index} WHERE {predicate} AND state_changed_ms>=?1 AND state_changed_ms<=?2 ORDER BY state_changed_ms DESC,rowid DESC LIMIT ?3")
    };
    let untimed = |index: &str, predicate: &str| {
        format!(
        "SELECT {COLUMNS} FROM run_index INDEXED BY {index} WHERE {predicate} AND state_changed_ms IS NULL ORDER BY rowid DESC LIMIT ?1")
    };
    [
        format!("SELECT {COLUMNS} FROM run_index INDEXED BY bus_event_live_runs WHERE {LIVE_PREDICATE} ORDER BY state_changed_ms DESC,rowid DESC LIMIT ?1"),
        window("diagnostics_errors_time", ERROR_PREDICATE),
        window("diagnostics_interruptions_time", INTERRUPTED_PREDICATE),
        untimed("diagnostics_errors_time", ERROR_PREDICATE),
        untimed("diagnostics_interruptions_time", INTERRUPTED_PREDICATE),
    ]
}

impl SqliteStore {
    pub fn diagnostic_evidence(&self, since: i64, now: i64) -> Result<Evidence, String> {
        let mut conn = self.conn.lock().map_err(|e| e.to_string())?;
        let tx = conn.transaction().map_err(|e| e.to_string())?;
        let queries = queries();
        let read = |query: &str,
                    parameters: &[&dyn rusqlite::ToSql]|
         -> Result<Vec<RunEvidence>, String> {
            let mut stmt = tx.prepare(query).map_err(|e| e.to_string())?;
            let rows = stmt
                .query_map(parameters, |r| {
                    Ok(RunEvidence {
                        run_id: r.get(0)?,
                        state: r.get(1)?,
                        at_ms: r.get(2)?,
                        provider: r.get(3)?,
                        credential: r.get(4)?,
                        class: r.get(5)?,
                        code: r.get(6)?,
                        pause_reason: r.get(7)?,
                        interrupt_reason: r.get(8)?,
                    })
                })
                .map_err(|e| e.to_string())?;
            rows.collect::<Result<_, _>>().map_err(|e| e.to_string())
        };
        let limit = (SCAN_LIMIT + 1) as i64;
        let evidence = Evidence {
            live: read(&queries[0], &[&limit])?,
            errors: read(&queries[1], &[&since, &now, &limit])?,
            interruptions: read(&queries[2], &[&since, &now, &limit])?,
            untimed_errors: read(&queries[3], &[&limit])?,
            untimed_interruptions: read(&queries[4], &[&limit])?,
        };
        tx.commit().map_err(|e| e.to_string())?;
        Ok(evidence)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_core::{RunIndex, RunIndexState};
    use broca_protocol::{BindIdentity, ModelHandle, RunFinishReason, Usage};
    use broca_wal::{FramedRecord, WalRecord};

    #[test]
    fn diagnostics_terminal_writers_preserve_identical_attribution_in_both_orders() {
        for actor_first in [true, false] {
            let store = SqliteStore::open_in_memory().unwrap();
            store.enable_bus_events("diagnostics_test").unwrap();
            let session = BindIdentity::new("/project", "test", "diagnostics");
            let start = crate::conformance::run_started(
                1,
                "run",
                &session,
                "PRIVATE PROMPT",
                ModelHandle::new("openai", "model"),
            );
            let finish = FramedRecord {seq:2,fence:1,record:WalRecord::RunFinished {
                interrupt_reason:None,reason:RunFinishReason::Error,ts_ms:Some(2000),usage:Usage::default(),retries_used:0,
                error:Some(serde_json::from_value(serde_json::json!({"class":"permanent","message":"PRIVATE PROVIDER TEXT","provider_code":"request_invalid"})).unwrap()),
            }};
            store
                .project_bus_events(&session, "epoch", std::slice::from_ref(&start), 3000)
                .unwrap();
            let mut attribution = RunIndexAttribution::default();
            attribution.observe(&start.record);
            attribution.observe(&finish.record);
            let publish_actor = || {
                store
                    .index_run_attributed("run", RunIndexState::Error, None, None, 0, &attribution)
                    .unwrap()
            };
            let publish_projector = || {
                store
                    .project_bus_events(&session, "epoch", &[start.clone(), finish.clone()], 3000)
                    .unwrap()
            };
            if actor_first {
                publish_actor();
                publish_projector();
            } else {
                publish_projector();
                publish_actor();
            }
            store
                .index_run_terminal("run", RunIndexState::Error, None, None, 0)
                .unwrap();
            let evidence = store.diagnostic_evidence(0, 3000).unwrap();
            assert_eq!(evidence.errors.len(), 1);
            let row = &evidence.errors[0];
            assert_eq!(row.at_ms, Some(2000));
            assert_eq!(row.provider.as_deref(), Some("openai"));
            assert_eq!(row.class.as_deref(), Some("permanent"));
            assert_eq!(row.code.as_deref(), Some("request_invalid"));
        }
    }

    #[test]
    fn diagnostics_index_plans_and_250k_index_build() {
        let store = SqliteStore::open_in_memory().unwrap();
        let conn = store.conn.lock().unwrap();
        conn.execute_batch("DROP INDEX diagnostics_errors_time; DROP INDEX diagnostics_interruptions_time;
            WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<250000)
            INSERT INTO run_index(run_id,session,state,state_changed_ms)
            SELECT 'r-'||x,'{}',CASE x%4 WHEN 0 THEN 'error' WHEN 1 THEN 'interrupted' WHEN 2 THEN 'transform_unavailable' ELSE 'completed' END,x FROM n;").unwrap();
        let start = std::time::Instant::now();
        init_schema(&conn).unwrap();
        println!(
            "diagnostics partial index build, 250000 rows: {:?}",
            start.elapsed()
        );
        for (i, sql) in queries().iter().enumerate() {
            let mut statement = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
            let limit = (SCAN_LIMIT + 1) as i64;
            let parameters: Vec<&dyn rusqlite::ToSql> = if matches!(i, 1 | 2) {
                vec![&0i64, &300000i64, &limit]
            } else {
                vec![&limit]
            };
            let plans = statement
                .query_map(parameters.as_slice(), |r| r.get::<_, String>(3))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            println!("diagnostics query {i}: {}", plans.join("; "));
            assert!(
                plans
                    .iter()
                    .all(|p| p.contains("USING INDEX") && !p.contains("TEMP")),
                "{plans:?}"
            );
            if i != 0 {
                assert!(plans.iter().any(|p| p.starts_with("SEARCH")));
            }
        }
        let metadata_plan: String = conn.query_row(
            "EXPLAIN QUERY PLAN SELECT source_instance_id,source_epoch,high_water FROM export_meta WHERE id=1",
            [], |r| r.get(3),
        ).unwrap();
        println!("diagnostics read-only open: {metadata_plan}");
        assert!(metadata_plan.contains("USING INTEGER PRIMARY KEY"));
    }

    #[test]
    fn diagnostics_unreadable_index_is_not_an_empty_success() {
        let store = SqliteStore::open_in_memory().unwrap();
        store
            .conn
            .lock()
            .unwrap()
            .execute("DROP INDEX diagnostics_errors_time", [])
            .unwrap();
        assert!(store.diagnostic_evidence(0, 1000).is_err());
    }
}
