//! Durable invalidation of rebuildable views before an overhaul cut. The fence
//! uses the existing checkpoint slot; immutable export facts are never touched.

use super::*;
use broca_core::{ProjectionSeed, RunIndexState};
use broca_wal::{recovery::RecoveryCut, FramedRecord, WalRangeRead, WalRecord};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub(super) const FENCE_PREFIX: &str = broca_core::admission::WAL_RECOVERY_FENCE_PREFIX;

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Fence {
    pub(super) pending: bool,
    cut_ids: Vec<String>,
}

fn decode_fence(text: &str) -> Result<Option<Fence>, StoreError> {
    let value: serde_json::Value = match serde_json::from_str(text) {
        Ok(value) => value,
        Err(error) if text.contains("\"wal_recovery_fence\"") => return Err(error.into()),
        Err(_) => return Ok(None),
    };
    value
        .get("wal_recovery_fence")
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(Into::into)
}

fn checkpoint(tx: &Transaction<'_>, key: &str) -> Result<Option<String>, StoreError> {
    Ok(tx
        .query_row(
            "SELECT admission_checkpoint_json FROM meta WHERE session=?1",
            [key],
            |row| row.get(0),
        )
        .optional()?
        .flatten())
}

pub(super) fn read_fence(tx: &Transaction<'_>, key: &str) -> Result<Option<Fence>, StoreError> {
    checkpoint(tx, key)?
        .as_deref()
        .map(decode_fence)
        .transpose()
        .map(Option::flatten)
}

pub(super) fn pending_diagnostics(
    tx: &Transaction<'_>,
) -> Result<Vec<crate::diagnostics::PendingRecovery>, String> {
    let mut statement = tx.prepare(
        "SELECT session, admission_checkpoint_json FROM meta WHERE admission_checkpoint_json LIKE ?1"
    ).map_err(|error| error.to_string())?;
    let rows = statement
        .query_map([format!("{FENCE_PREFIX}%")], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|error| error.to_string())?;
    let mut pending = Vec::new();
    for row in rows {
        let (key, checkpoint) = row.map_err(|error| error.to_string())?;
        let (cut_ids, evidence_unreadable) = match decode_fence(&checkpoint) {
            Ok(Some(fence)) if fence.pending => (fence.cut_ids, false),
            Ok(_) => continue,
            Err(_) => (Vec::new(), true),
        };
        let session = bind_identity_from_store_key(&key)
            .ok_or_else(|| format!("invalid session identity in pending WAL recovery: {key}"))?;
        pending.push(crate::diagnostics::PendingRecovery {
            session,
            cut_ids,
            evidence_unreadable,
        });
    }
    Ok(pending)
}

fn require_full_synchronous(conn: &rusqlite::Connection) -> Result<(), StoreError> {
    // Invalidation is the barrier that permits shortening the authoritative WAL.
    // Enforce the mode on this connection before BEGIN, not on a separate index DB.
    conn.pragma_update(None, "synchronous", "FULL")?;
    let mode: i64 = conn.pragma_query_value(None, "synchronous", |row| row.get(0))?;
    if mode != 2 {
        return Err(StoreError::InconsistentSnapshot(
            "WAL recovery requires SQLite synchronous=FULL",
        ));
    }
    Ok(())
}

pub(super) fn required_cut_ids(
    text: Option<&str>,
    seed: Option<&ProjectionSeed>,
) -> Result<Vec<String>, StoreError> {
    if let Some(seed) = seed {
        return Ok(seed
            .admission_checkpoint
            .recovery_cuts
            .iter()
            .map(|cut| cut.cut_id.clone())
            .collect());
    }
    if let Some(text) = text {
        if let Some(fence) = decode_fence(text)? {
            return Ok(fence.cut_ids);
        }
        if let Some(checkpoint) = decode_admission_checkpoint(text) {
            return Ok(checkpoint
                .recovery_cuts
                .into_iter()
                .map(|cut| cut.cut_id)
                .collect());
        }
    }
    Ok(Vec::new())
}

pub(super) fn required_cut_ids_in_tx(
    tx: &Transaction<'_>,
    key: &str,
) -> Result<Vec<String>, StoreError> {
    required_cut_ids(checkpoint(tx, key)?.as_deref(), None)
}

pub(super) fn preserve_fence(
    tx: &Transaction<'_>,
    key: &str,
    fence: &Fence,
) -> Result<(), StoreError> {
    let mut value: serde_json::Value = checkpoint(tx, key)?
        .as_deref()
        .map(serde_json::from_str)
        .transpose()?
        .unwrap_or_else(|| serde_json::json!({}));
    let object = value
        .as_object_mut()
        .ok_or(StoreError::InconsistentSnapshot(
            "recovery checkpoint is not an object",
        ))?;
    object.remove("wal_recovery_fence");
    let body = serde_json::to_string(&value)?;
    let complete = decode_admission_checkpoint(&body).is_some_and(|checkpoint| {
        fence
            .cut_ids
            .iter()
            .all(|id| checkpoint.recovery_cuts.iter().any(|cut| &cut.cut_id == id))
    });
    let encoded = if !fence.pending && complete {
        body
    } else {
        let fields = &body[1..body.len() - 1];
        format!(
            "{FENCE_PREFIX}{}{}{fields}}}",
            serde_json::to_string(fence)?,
            if fields.is_empty() { "" } else { "," }
        )
    };
    tx.execute(
        "UPDATE meta SET admission_checkpoint_json=?2 WHERE session=?1",
        params![key, encoded],
    )?;
    Ok(())
}

impl SqliteStore {
    pub(crate) fn invalidate_wal_recovery_inner(
        &self,
        session: &BindIdentity,
        cut: &RecoveryCut,
    ) -> Result<(), StoreError> {
        if !self.wal_config.is_overhaul() {
            return Err(StoreError::InconsistentSnapshot(
                "overhaul WAL recovery requires a boot-configured materialized store",
            ));
        }
        let key = Self::key(session);
        let mut conn = self.conn.lock().unwrap();
        require_full_synchronous(&conn)?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (_, lineage, epoch) = current_projection_state(&tx, &key, true)?;
        let text = checkpoint(&tx, &key)?;
        let mut ids: BTreeSet<_> = required_cut_ids(text.as_deref(), None)?
            .into_iter()
            .collect();
        ids.insert(cut.cut_id.clone());
        let epoch = epoch
            .checked_add(1)
            .ok_or(StoreError::InconsistentSnapshot(
                "recovery context epoch overflow",
            ))?;
        for table in [
            "episode_span",
            "message_tag",
            "message",
            "compaction_facts",
            "retained_head",
        ] {
            tx.execute(&format!("DELETE FROM {table} WHERE session=?1"), [&key])?;
        }
        tx.execute(
            "DELETE FROM compaction_view_receipt WHERE session_ref=?1",
            [broca_core::wal_writer::agent_session_ref(session)],
        )?;
        tx.execute("DELETE FROM meta WHERE session=?1", [&key])?;
        let fence = Fence {
            pending: true,
            cut_ids: ids.into_iter().collect(),
        };
        let json = serde_json::to_string(&serde_json::json!({"wal_recovery_fence":fence}))?;
        tx.execute("INSERT INTO meta(session,watermark_seq,lineage_id,context_epoch,prefix_len,admission_checkpoint_json)
            VALUES(?1,0,?2,?3,0,?4)", params![key,lineage,epoch,json])?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn needs_wal_recovery_projection_inner(
        &self,
        session: &BindIdentity,
        cut_ids: &BTreeSet<String>,
    ) -> Result<bool, StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let text = checkpoint(&tx, &Self::key(session))?;
        if text
            .as_deref()
            .map(decode_fence)
            .transpose()?
            .flatten()
            .is_some_and(|f| f.pending)
        {
            return Ok(true);
        }
        let stored: BTreeSet<_> = required_cut_ids(text.as_deref(), None)?
            .into_iter()
            .collect();
        Ok(stored != *cut_ids)
    }

    pub(crate) fn reproject_wal_recovery_inner(
        &self,
        session: &BindIdentity,
        range: &WalRangeRead,
    ) -> Result<(), StoreError> {
        let records = match range.records() {
            broca_wal::WalRangeRecords::Complete(records)
            | broca_wal::WalRangeRecords::Partial(records)
            | broca_wal::WalRangeRecords::Unknown(records) => records,
            broca_wal::WalRangeRecords::Absent => {
                return Err(StoreError::InconsistentSnapshot(
                    "recovery projection source absent",
                ))
            }
        };
        if !broca_core::retention::starts_at_lineage_origin(records) {
            return Err(StoreError::InconsistentSnapshot(
                "recovery projection requires sequence-zero WAL",
            ));
        }
        let cuts: Vec<_> = records
            .iter()
            .filter_map(|frame| match &frame.record {
                WalRecord::RecoveryCut { recovery } => Some(recovery.as_ref()),
                _ => None,
            })
            .collect();
        if cuts.is_empty() {
            return Err(StoreError::InconsistentSnapshot(
                "recovery projection lacks its cut record",
            ));
        }
        let key = Self::key(session);
        // Late attachment of a view still fences it before rebuilding; normal
        // writer opens have already executed this before the physical cut.
        let has_fence = {
            let mut conn = self.conn.lock().unwrap();
            let tx = conn.transaction()?;
            read_fence(&tx, &key)?.is_some()
        };
        if !has_fence {
            for cut in cuts {
                self.invalidate_wal_recovery_inner(session, cut)?;
            }
        }
        let mut conn = self.conn.lock().unwrap();
        require_full_synchronous(&conn)?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let fence = read_fence(&tx, &key)?.ok_or(StoreError::InconsistentSnapshot(
            "recovery projection fence missing",
        ))?;
        let snapshot = Self::projection_snapshot_from_tx(&tx, &key, true)?;
        let plan = Self::prepare_projection(snapshot, records, Some(range))?;
        Self::apply_projection_plan(&tx, &key, plan)?;
        restore_run_rows(&tx, session, records)?;
        if broca_core::retention::is_retention_lineage(records) {
            if let Some(lineage) = range.lineage_id {
                let mut facts = broca_core::head::HeadProjection::default();
                for frame in records {
                    facts.observe(&frame.record);
                }
                super::retained_head::write(
                    &tx,
                    &key,
                    &lineage.to_string(),
                    records.last().map_or(0, |f| f.seq),
                    &facts,
                )?;
            }
        }
        preserve_fence(
            &tx,
            &key,
            &Fence {
                pending: false,
                ..fence
            },
        )?;
        tx.commit()?;
        Ok(())
    }
}

fn restore_run_rows(
    tx: &Transaction<'_>,
    session: &BindIdentity,
    records: &[FramedRecord],
) -> Result<(), StoreError> {
    let mut present = BTreeSet::new();
    let mut current_runs = BTreeSet::new();
    for frame in records {
        if let WalRecord::RunStarted { run_id, .. } = &frame.record {
            present.insert(run_id.0.clone());
            current_runs.insert(run_id.0.clone());
        }
        if let WalRecord::RecoveryCut { recovery } = &frame.record {
            current_runs.extend(recovery.runs.iter().map(|run| run.run_id.0.clone()));
            current_runs.extend(recovery.active_run.iter().map(|run| run.0.clone()));
        }
    }
    let session_json = serde_json::to_string(session)?;
    // Session addresses can acquire a fresh lineage after deletion. Only records
    // and cut evidence in this lineage may authorize rewriting a historical row.
    for run in &current_runs {
        let belongs: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM run_index WHERE run_id=?1 AND session=?2)",
            params![run, session_json],
            |row| row.get(0),
        )?;
        if !belongs {
            continue;
        }
        tx.execute(
            "UPDATE run_index SET state_changed_ms=NULL,provider_module_id=NULL,credential_id=NULL,error_class=NULL,error_code=NULL,pause_reason=NULL,interrupt_reason=NULL WHERE run_id=?1 AND session=?2",
            params![run, session_json],
        )?;
        let mut state = if present.contains(run) {
            broca_core::reconcile_run_state(
                records.iter().filter_map(|f| {
                    (!matches!(
                        f.record,
                        WalRecord::RecoveryCut { .. } | WalRecord::RecoveryCallClosed { .. }
                    ))
                    .then_some(&f.record)
                }),
                run,
            )
        } else {
            None
        };
        if let Some(state) = &mut state {
            // A cut ends unfinished work, but cannot rewrite an explicit terminal
            // that survived before the cut.
            if matches!(state.state, RunIndexState::Active | RunIndexState::Paused)
                && records.iter().any(|frame| match &frame.record {
                    WalRecord::RecoveryCut { recovery } => {
                        recovery.runs.iter().any(|owner| owner.run_id.0 == *run)
                            || recovery
                                .active_run
                                .as_ref()
                                .is_some_and(|owner| owner.0 == *run)
                    }
                    WalRecord::RecoveryCallClosed { run_id, .. } => run_id.0 == *run,
                    _ => false,
                })
            {
                state.state = RunIndexState::Interrupted;
            }
        }
        let usage = state
            .as_ref()
            .and_then(|s| s.usage.as_ref())
            .map(serde_json::to_string)
            .transpose()?;
        let finish = state
            .as_ref()
            .and_then(|s| s.final_step_finish_reason)
            .map(|r| serde_json::to_string(&r))
            .transpose()?;
        let mut attribution = broca_core::RunIndexAttribution::default();
        let mut active = false;
        for frame in records {
            if let WalRecord::RunStarted { run_id, .. } = &frame.record {
                active = run_id.0 == *run;
            }
            if active {
                attribution.observe(&frame.record);
            }
        }
        super::diagnostics::write_lifecycle(
            tx,
            run,
            state
                .as_ref()
                .map_or(RunIndexState::Interrupted, |s| s.state.clone())
                .as_str(),
            usage.as_deref(),
            finish.as_deref(),
            state.as_ref().map_or(0, |s| s.retries_used),
            Some(&attribution),
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
