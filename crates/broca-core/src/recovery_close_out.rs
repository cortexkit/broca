//! Owner-scoped completion of attributed calls removed by WAL recovery.
//! The durable cut defines the work set; the current episode only determines
//! whether an intact assistant turn still needs ordinary pairing records.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use broca_protocol::{BatchId, RunId, StepId, ToolCallId};
use broca_wal::recovery::{RecoveryCall, RecoveryOutcome};
use broca_wal::{FramedRecord, WalRecord};

use crate::replay::{DeferredCallProjection, OpenCallState, ReplayError, ReplayResolution};

fn lost_approval_result(id: &ToolCallId) -> broca_protocol::ToolResult {
    let mut result = crate::outcome_unknown_result(id);
    if let broca_protocol::ToolOutput::Error { message, code, .. } = &mut result.output {
        *message = "outcome_unknown: WAL repair lost late approval/dispatch evidence. This call may or may not already have been approved and sent. It will not be sent again; a later approval is refused (late_approval_evidence_lost).".into();
        *code = Some(broca_wal::recovery::LATE_APPROVAL_EVIDENCE_LOST.into());
    }
    result
}

pub(crate) fn is_lost_approval_receipt(record: &WalRecord) -> bool {
    matches!(record, WalRecord::RecoveryCallClosed { result, .. }
        if matches!(&result.output, broca_protocol::ToolOutput::Error { code: Some(code), .. }
            if code == broca_wal::recovery::LATE_APPROVAL_EVIDENCE_LOST))
}

struct AttributedCall {
    sources: Vec<(String, RecoveryCall)>,
    run_id: RunId,
    call: RecoveryCall,
}

struct Terminal {
    run_id: RunId,
    call: RecoveryCall,
    cut_id: Option<String>,
    consumed: bool,
}

pub(crate) type BatchEvidence = BTreeMap<(String, String), (StepId, Vec<ToolCallId>)>;

pub(crate) fn enrich_identity(
    mut call: RecoveryCall,
    run: &RunId,
    batches: &BatchEvidence,
) -> RecoveryCall {
    if let Some(batch) = &call.batch_id {
        if let Some((step, _)) = batches.get(&(run.0.clone(), batch.0.clone())) {
            call.step_id = call.step_id.or(Some(*step));
        }
    } else if let Some(step) = call.step_id {
        let mut candidates = batches
            .iter()
            .filter(|((owner, _), (known_step, planned))| {
                owner == &run.0 && *known_step == step && planned.contains(&call.tool_call_id)
            });
        if let Some(((_, batch), _)) = candidates.next() {
            if candidates.next().is_none() {
                call.batch_id = Some(BatchId(batch.clone()));
            }
        }
    }
    call
}

pub(crate) fn merge_call(target: &mut RecoveryCall, source: &RecoveryCall) {
    target.batch_id = target.batch_id.clone().or_else(|| source.batch_id.clone());
    target.step_id = target.step_id.or(source.step_id);
    target.call_keys.extend(source.call_keys.iter().cloned());
    target.call_keys.sort();
    target.call_keys.dedup();
}

pub(crate) fn merge_effect(calls: &mut Vec<RecoveryCall>, call: RecoveryCall) {
    if let Some(existing) = calls
        .iter_mut()
        .find(|existing| existing.matches_effect(&call))
    {
        merge_call(existing, &call);
    } else {
        calls.push(call);
    }
}

/// Plan missing terminals from the complete lineage. Step/batch evidence and
/// exact physical keys distinguish repeated model-facing IDs within one owner.
/// Entries with no effect identity retain their multiplicity under their cut id.
pub fn plan(
    frames: &[FramedRecord],
    lineage: impl std::fmt::Display,
    through: u64,
) -> Result<Vec<WalRecord>, ReplayError> {
    let lineage = lineage.to_string();
    let mut raw_work = Vec::new();
    let mut seen_cuts = BTreeSet::new();
    let mut terminals = Vec::new();
    let mut batches = BatchEvidence::new();
    let mut intents: BTreeMap<(String, String, String), Vec<RecoveryCall>> = BTreeMap::new();
    let mut current: Option<RunId> = None;
    let mut generation = None;
    let mut deferred = DeferredCallProjection::new(&lineage);
    deferred.fold_frames(frames);
    for frame in frames {
        let mut ordinary = None;
        match &frame.record {
            WalRecord::RecoveryCut { recovery } => {
                generation = Some(recovery.cut_id.clone());
                if seen_cuts.insert(recovery.cut_id.clone()) {
                    for run in &recovery.runs {
                        for call in &run.calls {
                            raw_work.push(AttributedCall {
                                sources: vec![(recovery.cut_id.clone(), call.clone())],
                                run_id: run.run_id.clone(),
                                call: call.clone(),
                            });
                        }
                    }
                }
            }
            WalRecord::RunStarted { run_id, .. } => current = Some(run_id.clone()),
            WalRecord::ToolBatchStarted {
                batch_id,
                step_id,
                planned_calls,
            } => {
                if let Some(run) = &current {
                    batches.insert(
                        (run.0.clone(), batch_id.0.clone()),
                        (*step_id, planned_calls.clone()),
                    );
                }
            }
            WalRecord::ToolDispatchIntent {
                batch_id,
                tool_call_id,
                call_key,
                ..
            } => {
                if let Some(run) = &current {
                    let identity = enrich_identity(
                        RecoveryCall {
                            tool_call_id: tool_call_id.clone(),
                            batch_id: Some(batch_id.clone()),
                            step_id: None,
                            call_keys: call_key
                                .iter()
                                .cloned()
                                .chain(std::iter::once(broca_wal::recovery::physical_call_key(
                                    &lineage,
                                    frame.seq,
                                    generation.as_deref(),
                                )))
                                .collect(),
                        },
                        run,
                        &batches,
                    );
                    merge_effect(
                        intents
                            .entry((run.0.clone(), batch_id.0.clone(), tool_call_id.0.clone()))
                            .or_default(),
                        identity,
                    );
                }
            }
            WalRecord::ToolResult {
                batch_id,
                result,
                outcome,
                provider_held,
                ..
            } => {
                if let Some(run) = &current {
                    ordinary = Some((
                        run.clone(),
                        RecoveryCall {
                            tool_call_id: result.tool_call_id.clone(),
                            batch_id: Some(batch_id.clone()),
                            step_id: None,
                            call_keys: provider_held
                                .iter()
                                .map(|held| held.call_key.clone())
                                .collect(),
                        },
                        *outcome == broca_wal::DispatchOutcome::Unknown,
                    ));
                }
            }
            WalRecord::ToolNoDispatch {
                batch_id,
                tool_call_id,
                ..
            } => {
                if let Some(run) = &current {
                    ordinary = Some((
                        run.clone(),
                        RecoveryCall {
                            tool_call_id: tool_call_id.clone(),
                            batch_id: Some(batch_id.clone()),
                            step_id: None,
                            call_keys: Vec::new(),
                        },
                        false,
                    ));
                }
            }
            WalRecord::ApprovalRecorded {
                call_key,
                call_id,
                closing_result: Some(_),
                ..
            } => {
                if let Some(call) = deferred.calls.get(call_key) {
                    if let (Some(run), WalRecord::ToolCallDeferred { batch_id, .. }) =
                        (&call.run_id, call.prepared.as_ref())
                    {
                        ordinary = Some((
                            run.clone(),
                            RecoveryCall {
                                tool_call_id: call_id.clone(),
                                batch_id: Some(batch_id.clone()),
                                step_id: None,
                                call_keys: vec![call_key.clone()],
                            },
                            false,
                        ));
                    }
                }
            }
            WalRecord::RecoveryCallClosed {
                cut_id,
                run_id,
                batch_id,
                step_id,
                call_key,
                tool_call_id,
                result,
                ..
            } => {
                if result.tool_call_id != *tool_call_id {
                    return Err(ReplayError::CorruptLineage {
                        episode_run_id: run_id.clone(),
                        reason: "recovery completion result does not match its call identity"
                            .into(),
                    });
                }
                terminals.push(Terminal {
                    run_id: run_id.clone(),
                    cut_id: Some(cut_id.clone()),
                    consumed: false,
                    call: RecoveryCall {
                        tool_call_id: tool_call_id.clone(),
                        batch_id: batch_id.clone(),
                        step_id: *step_id,
                        call_keys: call_key.iter().cloned().collect(),
                    },
                });
            }
            _ => {}
        }
        if let Some((run, call, unknown)) = ordinary {
            let mut call = enrich_identity(call, &run, &batches);
            let known = call.batch_id.as_ref().and_then(|batch| {
                intents.get(&(run.0.clone(), batch.0.clone(), call.tool_call_id.0.clone()))
            });
            if let Some(known) = known.filter(|known| known.len() == 1) {
                call.call_keys.extend(known[0].call_keys.iter().cloned());
            } else if unknown && known.is_none() && call.call_keys.is_empty() {
                // A synthetic Unknown after a cut has no new dispatch intent. Its
                // exact keys are recoverable only from one unambiguous matching
                // recovery candidate.
                let mut candidates = Vec::new();
                for attributed in raw_work.iter().filter(|work| work.run_id == run) {
                    let candidate = enrich_identity(attributed.call.clone(), &run, &batches);
                    if candidate.matches_effect(&call) {
                        merge_effect(&mut candidates, candidate);
                    }
                }
                if candidates.len() == 1 {
                    call.call_keys = candidates[0].call_keys.clone();
                }
            }
            call.call_keys.sort();
            call.call_keys.dedup();
            terminals.push(Terminal {
                run_id: run,
                call,
                cut_id: None,
                consumed: false,
            });
        }
    }
    if seen_cuts.is_empty() {
        return Ok(Vec::new());
    }
    let records: Vec<_> = frames.iter().map(|frame| frame.record.clone()).collect();
    let prior = crate::replay(&records)?;
    if let (Some(run), Some(batch)) = (&prior.run_id, &prior.open_tool_batch) {
        batches
            .entry((run.0.clone(), batch.batch_id.0.clone()))
            .or_insert_with(|| {
                (
                    batch.step_id,
                    batch.calls.iter().map(|(id, _)| id.clone()).collect(),
                )
            });
    }
    let mut work: Vec<AttributedCall> = Vec::new();
    for mut attributed in raw_work {
        attributed.call = enrich_identity(attributed.call, &attributed.run_id, &batches);
        if let Some(existing) = work.iter_mut().find(|existing| {
            existing.run_id == attributed.run_id && existing.call.matches_effect(&attributed.call)
        }) {
            merge_call(&mut existing.call, &attributed.call);
            existing.sources.extend(attributed.sources);
        } else {
            work.push(attributed);
        }
    }
    let mut closing = prior.clone();
    let mut surviving = BTreeSet::new();
    let mut removed = Vec::new();
    for attributed in &work {
        let prepared = deferred.calls.iter().find(|(_, projected)| {
            if projected.run_id.as_ref() != Some(&attributed.run_id) {
                return false;
            }
            let Some(identity) = deferred.effect_identity(projected) else {
                return false;
            };
            let identity = enrich_identity(identity, &attributed.run_id, &batches);
            attributed.call.matches_effect(&identity)
        });
        if prepared.is_some_and(|(_, call)| call.late_result.is_some()) {
            continue;
        }
        let late_open = prepared.is_some_and(|(_, call)| DeferredCallProjection::late_open(call));
        let approval_evidence_lost =
            late_open && prepared.is_some_and(|(_, call)| call.late_approval.is_none());
        let mut settled = false;
        for terminal in terminals
            .iter_mut()
            .filter(|terminal| terminal.run_id == attributed.run_id)
        {
            if late_open && terminal.cut_id.is_none() {
                continue;
            }
            if attributed.call.terminal_matches(&terminal.call) {
                settled = true;
                break;
            }
            if !attributed.call.has_effect_identity()
                && !terminal.call.has_effect_identity()
                && attributed.call.tool_call_id == terminal.call.tool_call_id
                && !terminal.consumed
                && terminal
                    .cut_id
                    .as_ref()
                    .is_some_and(|cut| attributed.sources.iter().any(|(source, _)| source == cut))
            {
                terminal.consumed = true;
                settled = true;
                break;
            }
        }
        if settled {
            continue;
        }
        let in_surviving_turn = !late_open
            && !prior.run_finished_durable
            && prior.run_id.as_ref() == Some(&attributed.run_id)
            && prior.open_tool_batch.as_ref().is_some_and(|batch| {
                if !batch
                    .calls
                    .iter()
                    .any(|(id, _)| id == &attributed.call.tool_call_id)
                {
                    return false;
                }
                let known = intents.get(&(
                    attributed.run_id.0.clone(),
                    batch.batch_id.0.clone(),
                    attributed.call.tool_call_id.0.clone(),
                ));
                if known.is_some_and(|known| known.len() > 1) {
                    return false;
                }
                let cell = RecoveryCall {
                    tool_call_id: attributed.call.tool_call_id.clone(),
                    step_id: Some(batch.step_id),
                    batch_id: Some(batch.batch_id.clone()),
                    call_keys: known
                        .and_then(|known| known.first())
                        .map_or_else(Vec::new, |known| known.call_keys.clone()),
                };
                cell.matches_effect(&attributed.call)
                    && work
                        .iter()
                        .filter(|other| {
                            other.run_id == attributed.run_id && cell.matches_effect(&other.call)
                        })
                        .count()
                        == 1
            });
        if in_surviving_turn {
            surviving.insert(attributed.call.tool_call_id.0.clone());
            continue;
        }
        let key = prepared
            .map(|(key, _)| key.clone())
            .filter(|key| attributed.call.call_keys.contains(key))
            .or_else(|| attributed.call.call_keys.first().cloned());
        let source = attributed
            .sources
            .iter()
            .find(|(_, source)| {
                key.as_ref().map_or(source.call_keys.is_empty(), |key| {
                    source.call_keys.contains(key)
                }) && (source.matches_effect(&attributed.call)
                    || (!source.has_effect_identity()
                        && !attributed.call.has_effect_identity()
                        && source.tool_call_id == attributed.call.tool_call_id))
            })
            .ok_or_else(|| ReplayError::CorruptLineage {
                episode_run_id: attributed.run_id.clone(),
                reason: "recovery completion identity has no matching durable cut source".into(),
            })?;
        removed.push(WalRecord::RecoveryCallClosed {
            cut_id: source.0.clone(),
            run_id: attributed.run_id.clone(),
            batch_id: attributed.call.batch_id.clone(),
            step_id: attributed.call.step_id,
            call_key: key,
            tool_call_id: attributed.call.tool_call_id.clone(),
            outcome: RecoveryOutcome::Unknown,
            result: Arc::new(if approval_evidence_lost {
                lost_approval_result(&attributed.call.tool_call_id)
            } else {
                crate::outcome_unknown_result(&attributed.call.tool_call_id)
            }),
        });
    }
    let mut completion = Vec::new();
    let close_surviving_turn = prior.open_tool_batch.is_some()
        && !prior.run_finished_durable
        && matches!(prior.resolution, ReplayResolution::Interrupted { .. });
    if close_surviving_turn || !surviving.is_empty() {
        if let Some(batch) = &mut closing.open_tool_batch {
            let mut marked = BTreeSet::new();
            for (id, state) in &mut batch.calls {
                if surviving.contains(&id.0)
                    && *state != OpenCallState::Terminated
                    && marked.insert(id.0.clone())
                {
                    *state = OpenCallState::Dispatched;
                }
            }
        }
        if !matches!(closing.resolution, ReplayResolution::Interrupted { .. }) {
            closing.resolution = ReplayResolution::Interrupted {
                indeterminate: surviving.into_iter().map(ToolCallId).collect(),
            };
        }
        completion.push(crate::resume_started_record(&closing, through, Vec::new()));
        completion.extend(crate::close_out_open_batch(&closing, |_| {
            crate::DeferredCloseKind::NotDispatched
        }));
    }
    completion.extend(removed);
    Ok(completion)
}

/// Complete the attributed work before admitting a session or a new run. The
/// caller supplies a complete snapshot at the writer's synced head while holding
/// the session lease. No RunFinished may follow a failed capacity reservation.
pub async fn complete(
    writer: &crate::WalHandle,
    frames: &[FramedRecord],
) -> Result<bool, crate::WalWriteError> {
    let through = writer
        .durable_head()
        .load(std::sync::atomic::Ordering::Acquire);
    if frames.last().map_or(0, |frame| frame.seq) != through {
        return Err(crate::WalWriteError::Failed(
            "recovery close-out snapshot is not at the durable writer head".into(),
        ));
    }
    let lineage = writer.lineage_id().ok_or_else(|| {
        crate::WalWriteError::Failed("recovery close-out requires a durable lineage".into())
    })?;
    let completion = plan(frames, lineage, through)
        .map_err(|error| crate::WalWriteError::Failed(error.to_string()))?;
    if completion.is_empty() {
        return Ok(false);
    }
    let tags = crate::tags::lineage_tags(frames.iter().map(|frame| &frame.record));
    if !matches!(tags, crate::tags::LineageTags::Unstarted { .. }) {
        writer.seed_lineage_tags(Some(tags));
    }
    writer.record_recovery_close_out(completion)?;
    writer.sync().await?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_wal::recovery::{CutReason, RecoveryCut, RecoveryRun};

    #[test]
    fn recovery_terminal_does_not_settle_a_new_held_call_reusing_its_model_id() {
        let mut records = crate::replay::provider_held_tests::prefix(false);
        let old_key = records
            .iter()
            .find_map(|record| match record {
                WalRecord::ToolCallProviderHeld { call_key, .. } => Some(call_key.clone()),
                _ => None,
            })
            .unwrap();
        records.push(WalRecord::RecoveryCut {
            recovery: Box::new(RecoveryCut {
                cut_id: "held-cut".into(),
                sidecar_basename: "log.cut".into(),
                cut_offset: 0,
                removed_len: 1,
                removed_sha256: "a".repeat(64),
                reason: CutReason::PublishedRegionCorruption,
                active_run: Some(RunId("r".into())),
                runs: vec![RecoveryRun {
                    run_id: RunId("r".into()),
                    calls: vec![RecoveryCall {
                        tool_call_id: ToolCallId("a".into()),
                        step_id: Some(StepId(1)),
                        batch_id: Some(BatchId("b".into())),
                        call_keys: vec![old_key],
                    }],
                }],
                unowned_calls: Vec::new(),
                unattributed_tool_risk: false,
            }),
        });
        let prior = crate::replay(&records).unwrap();
        records.extend(crate::close_out_open_batch(&prior, |_| {
            crate::DeferredCloseKind::NotDispatched
        }));
        let mut finish = crate::replay::deferred_tests::finished().pop().unwrap();
        if let WalRecord::RunFinished { reason, .. } = &mut finish {
            *reason = broca_protocol::RunFinishReason::Interrupted;
        }
        records.push(finish);
        let mut second = crate::replay::provider_held_tests::prefix(false);
        for record in &mut second {
            match record {
                WalRecord::RunStarted { run_id, .. } => *run_id = RunId("second-owner".into()),
                WalRecord::ToolBatchStarted { batch_id, .. } => {
                    *batch_id = BatchId("second-batch".into())
                }
                WalRecord::ToolDispatchIntent {
                    batch_id,
                    tool_call_id,
                    call_key,
                    ..
                } => {
                    *batch_id = BatchId("second-batch".into());
                    if tool_call_id.0 == "a" {
                        *call_key = Some("second-held-key".into());
                    }
                }
                WalRecord::ToolCallProviderHeld {
                    batch_id, call_key, ..
                } => {
                    *batch_id = BatchId("second-batch".into());
                    *call_key = "second-held-key".into();
                }
                _ => {}
            }
        }
        records.extend(second);
        crate::replay(&records).expect("the new held call follows a sealed older owner");
        let frames: Vec<_> = records
            .into_iter()
            .enumerate()
            .map(|(index, record)| FramedRecord {
                seq: index as u64 + 1,
                fence: 1,
                record,
            })
            .collect();
        let facts = crate::replay::held_call_facts(&frames, Some("second-held-key"));
        assert_eq!(
            facts.terminal(&ToolCallId("a".into())),
            None,
            "the older physical call's Unknown must not close a new same-ID registration"
        );
        assert!(facts.needs_intake());
    }

    #[test]
    fn unknown_identity_entries_are_not_settled_by_another_steps_same_id() {
        let mut records = crate::replay::deferred_tests::batch(&["call_0"]);
        records.push(crate::replay::deferred_tests::terminal("call_0", None));
        records.extend(crate::replay::deferred_tests::finished());
        let unknown = RecoveryCall {
            tool_call_id: ToolCallId("call_0".into()),
            step_id: None,
            batch_id: None,
            call_keys: Vec::new(),
        };
        let marker = WalRecord::RecoveryCut {
            recovery: Box::new(RecoveryCut {
                cut_id: "unknown-cut".into(),
                sidecar_basename: "log.cut".into(),
                cut_offset: 0,
                removed_len: 1,
                removed_sha256: "a".repeat(64),
                reason: CutReason::PublishedRegionCorruption,
                active_run: None,
                runs: vec![RecoveryRun {
                    run_id: RunId("r".into()),
                    calls: vec![unknown.clone(), unknown],
                }],
                unowned_calls: Vec::new(),
                unattributed_tool_risk: false,
            }),
        };
        records.extend([marker.clone(), marker]);
        let pending = |records: &[WalRecord]| {
            let frames: Vec<_> = records
                .iter()
                .cloned()
                .enumerate()
                .map(|(index, record)| FramedRecord {
                    seq: index as u64 + 1,
                    fence: 1,
                    record,
                })
                .collect();
            plan(&frames, "lineage", frames.len() as u64).unwrap()
        };
        let completion = pending(&records);
        assert_eq!(
            completion
                .iter()
                .filter(|record| matches!(record, WalRecord::RecoveryCallClosed { .. }))
                .count(),
            2,
            "unknown identities must not merge with a completed same-id call or with each other"
        );
        for boundary in 0..=completion.len() {
            let mut prefix = records.clone();
            prefix.extend_from_slice(&completion[..boundary]);
            prefix.extend(pending(&prefix));
            assert_eq!(
                prefix
                    .iter()
                    .filter(|record| matches!(record, WalRecord::RecoveryCallClosed { .. }))
                    .count(),
                2
            );
            assert!(pending(&prefix).is_empty());
            broca_crash_harness::check_tool_pairing(&crate::replay(&prefix).unwrap().prompt)
                .unwrap();
        }
    }

    #[test]
    fn recovery_inferred_batch_matches_the_production_batch_id_generator() {
        let generated = crate::run::tool_batch_id(&RunId("r".into()), broca_protocol::StepId(1));
        assert_eq!(generated, broca_protocol::BatchId("r-b1".into()));
        let mut records = crate::replay::deferred_tests::batch(&["kept-call"]);
        records.pop();
        records.push(
            serde_json::from_value(serde_json::json!({
                "type":"recovery_cut", "cut_id":"cut-lost-start", "sidecar_basename":"log.cut",
                "cut_offset":0, "removed_len":1, "removed_sha256":"a".repeat(64),
                "reason":"published_region_corruption", "active_run":"r",
                "runs":[{"run_id":"r","calls":[{"tool_call_id":"kept-call"}]}],
                "unattributed_tool_risk":false
            }))
            .unwrap(),
        );
        let prior = crate::replay(&records).unwrap();
        let batch = prior
            .open_tool_batch
            .expect("the retained model turn infers its lost start");
        assert!(batch.needs_start);
        assert_eq!(batch.batch_id, generated);
    }

    #[test]
    fn recovery_inferred_batch_cannot_be_replaced_by_another_plan() {
        let mut records = crate::replay::deferred_tests::batch(&["kept-call"]);
        records.pop();
        records.push(WalRecord::RecoveryCut {
            recovery: Box::new(RecoveryCut {
                cut_id: "cut".into(),
                sidecar_basename: "log.cut".into(),
                cut_offset: 0,
                removed_len: 1,
                removed_sha256: "a".repeat(64),
                reason: CutReason::PublishedRegionCorruption,
                active_run: Some(RunId("r".into())),
                runs: vec![RecoveryRun {
                    run_id: RunId("r".into()),
                    calls: vec![RecoveryCall {
                        tool_call_id: ToolCallId("kept-call".into()),
                        step_id: None,
                        batch_id: None,
                        call_keys: Vec::new(),
                    }],
                }],
                unowned_calls: Vec::new(),
                unattributed_tool_risk: false,
            }),
        });
        records.push(WalRecord::ToolBatchStarted {
            batch_id: broca_protocol::BatchId("different-batch".into()),
            step_id: broca_protocol::StepId(2),
            planned_calls: vec![ToolCallId("different-call".into())],
        });
        assert!(
            matches!(
                crate::replay(&records),
                Err(ReplayError::CorruptLineage { .. })
            ),
            "a recovery tombstone must not be erased by replacing the inferred batch"
        );
    }

    #[test]
    fn recovery_receipt_closes_held_aliases_without_inventing_default_read_content() {
        let id = ToolCallId("removed-held-call".into());
        let records = vec![
            WalRecord::RecoveryCut {
                recovery: Box::new(RecoveryCut {
                    cut_id: "cut".into(),
                    sidecar_basename: "log.cut".into(),
                    cut_offset: 0,
                    removed_len: 1,
                    removed_sha256: "a".repeat(64),
                    reason: CutReason::PublishedRegionCorruption,
                    active_run: None,
                    runs: vec![RecoveryRun {
                        run_id: RunId("removed-owner".into()),
                        calls: vec![RecoveryCall {
                            tool_call_id: id.clone(),
                            step_id: None,
                            batch_id: None,
                            call_keys: vec!["intent-key".into(), "provider-key".into()],
                        }],
                    }],
                    unowned_calls: Vec::new(),
                    unattributed_tool_risk: false,
                }),
            },
            WalRecord::RecoveryCallClosed {
                cut_id: "cut".into(),
                run_id: RunId("removed-owner".into()),
                batch_id: None,
                step_id: None,
                call_key: Some("intent-key".into()),
                tool_call_id: id.clone(),
                outcome: RecoveryOutcome::Unknown,
                result: Arc::new(crate::outcome_unknown_result(&id)),
            },
        ];
        let frames: Vec<_> = records
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, record)| FramedRecord {
                seq: index as u64 + 1,
                fence: 1,
                record,
            })
            .collect();
        for key in ["intent-key", "provider-key"] {
            let facts = crate::replay::held_call_facts(&frames, Some(key));
            assert_eq!(
                facts.terminal(&id),
                Some(broca_wal::DispatchOutcome::Unknown)
            );
            assert!(!facts.needs_intake());
        }
        assert!(crate::replay::held_call_facts(&frames, None)
            .held
            .is_empty());
        let prior = crate::replay(&records).unwrap();
        assert!(prior.prompt.is_empty());
        broca_crash_harness::check_tool_pairing(&prior.prompt).unwrap();
        assert!(plan(&frames, "lineage", 2).unwrap().is_empty());
    }
}
