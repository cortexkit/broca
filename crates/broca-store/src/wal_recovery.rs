//! Durable invalidation of rebuildable views before an overhaul cut. The fence
//! uses the existing checkpoint slot; immutable export facts are never touched.

use super::*;
use broca_core::{ProjectionSeed, RunIndexState};
use broca_wal::{recovery::RecoveryCut, FramedRecord, WalRangeRead, WalRecord};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashSet};

pub(super) const FENCE_PREFIX: &str = broca_core::admission::WAL_RECOVERY_FENCE_PREFIX;

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Fence {
    pub(super) pending: bool,
    cut_ids: Vec<String>,
    invalidated_runs: Vec<String>,
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
        let key = Self::key(session);
        let session_json = serde_json::to_string(session)?;
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (_, lineage, epoch) = current_projection_state(&tx, &key)?;
        let text = checkpoint(&tx, &key)?;
        let old = text.as_deref().map(decode_fence).transpose()?.flatten();
        let mut ids: BTreeSet<_> = required_cut_ids(text.as_deref(), None)?
            .into_iter()
            .collect();
        ids.insert(cut.cut_id.clone());
        let mut runs: BTreeSet<_> = old.into_iter().flat_map(|f| f.invalidated_runs).collect();
        {
            let mut query = tx.prepare("SELECT run_id FROM run_index WHERE session=?1")?;
            for run in query.query_map([&session_json], |row| row.get::<_, String>(0))? {
                runs.insert(run?);
            }
        }
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
        tx.execute("UPDATE run_index SET state='active',usage_json=NULL,final_step_finish_reason_json=NULL,retries_used=0,
            state_changed_ms=NULL,error_class=NULL,error_code=NULL,pause_reason=NULL,interrupt_reason=NULL
            WHERE session=?1", [&session_json])?;
        tx.execute("DELETE FROM meta WHERE session=?1", [&key])?;
        let fence = Fence {
            pending: true,
            cut_ids: ids.into_iter().collect(),
            invalidated_runs: runs.into_iter().collect(),
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
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let fence = read_fence(&tx, &key)?.ok_or(StoreError::InconsistentSnapshot(
            "recovery projection fence missing",
        ))?;
        let snapshot = Self::projection_snapshot_from_tx(&tx, &key)?;
        let plan = Self::prepare_projection(snapshot, records, Some(range))?;
        Self::apply_projection_plan(&tx, &key, plan)?;
        restore_run_rows(&tx, session, records, &fence.invalidated_runs)?;
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
                invalidated_runs: Vec::new(),
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
    runs: &[String],
) -> Result<(), StoreError> {
    let mut present = HashSet::new();
    for frame in records {
        if let WalRecord::RunStarted { run_id, .. } = &frame.record {
            present.insert(run_id.0.clone());
        }
    }
    let session_json = serde_json::to_string(session)?;
    for run in runs {
        let belongs: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM run_index WHERE run_id=?1 AND session=?2)",
            params![run, session_json],
            |row| row.get(0),
        )?;
        if !belongs {
            continue;
        }
        let state = if present.contains(run) {
            // The index reflects explicit surviving lifecycle records. Recovery
            // markers govern execution close-out, not an already recorded terminal.
            broca_core::reconcile_run_state(
                records.iter().filter_map(|f| {
                    (!matches!(f.record, WalRecord::RecoveryCut { .. })).then_some(&f.record)
                }),
                run,
            )
        } else {
            None
        };
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
