//! The run index's lifecycle write, shared by the terminal and attributed index updates.
use crate::SqliteStore;
use broca_core::RunIndexAttribution;
use rusqlite::{params, Connection};

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
