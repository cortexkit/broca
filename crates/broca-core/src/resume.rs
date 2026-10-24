//! What a resume writes before it does anything else.
//!
//! A resume picks up an episode that has no durable `RunFinished`: the process that ran it
//! crashed or was stopped. Before the resume calls a provider, dispatches a tool or seals the
//! episode, it writes one informational [`WalRecord::ResumeStarted`] saying what it found.
//!
//! When the episode holds an indeterminate tool call (a durable dispatch intent with no
//! result, so the call may or may not have run), the resume never sends it again and makes no
//! model call. It closes the open batch instead: a `ToolResult` with outcome `Unknown` for each
//! indeterminate call, an `Interrupted` result for each planned call that never left the
//! process, and the batch's commit markers. Those records go in one barrier with the resume
//! record, and the caller then seals the episode `Interrupted` in a barrier of its own. The
//! owner's next send renders every call of that batch paired with its result, so no request
//! ever carries a tool call with no result, and the model learns the outcome is uncertain
//! before it would repeat the call.
//!
//! Both the run loop and the module's crash-orphan pass resume through these functions, so the
//! close-out has one shape however the resume is reached.

use std::sync::Arc;

use broca_protocol::{
    content::{ToolOutput, ToolResult},
    ToolCallId,
};
use broca_wal::{DispatchOutcome, ResumeRedo, ResumeReplayed, WalRecord, WithdrawElicitation};

use crate::replay::{OpenCallState, ReplayResolution, ReplayState};

/// The text of the result a resume writes for an indeterminate call. It says plainly that no
/// result was recorded, because that is all the runner knows: the call may have run.
pub const OUTCOME_UNKNOWN_TEXT: &str = "outcome_unknown: this tool call was sent, but no result \
     was recorded before the run stopped, so it may or may not have run. It will not be sent \
     again.";

/// The text of the result a resume writes for a planned call of the same batch that never left
/// the process. It definitely did not run.
pub const NOT_DISPATCHED_TEXT: &str =
    "tool not run: the run was interrupted before this call was dispatched";

/// The result a resume records for an indeterminate call: an error whose text says the call has
/// no recorded result. Written with outcome [`DispatchOutcome::Unknown`], which every reader
/// already treats as indeterminate, so the call stays marked as possibly run.
pub fn outcome_unknown_result(tool_call_id: &ToolCallId) -> ToolResult {
    ToolResult {
        tool_call_id: tool_call_id.clone(),
        output: ToolOutput::Error {
            message: OUTCOME_UNKNOWN_TEXT.into(),
        },
        is_error: true,
    }
}

/// The result for a planned call that never left the process because the run stopped first. A
/// resume writes it for such a call of an open batch, and a run asked to stop writes it for each
/// call it no longer sends. Recorded with outcome [`DispatchOutcome::Interrupted`]: the call
/// definitely did not run and is never sent later.
pub fn not_dispatched_result(tool_call_id: &ToolCallId) -> ToolResult {
    ToolResult {
        tool_call_id: tool_call_id.clone(),
        output: ToolOutput::Error {
            message: NOT_DISPATCHED_TEXT.into(),
        },
        is_error: true,
    }
}

/// A provider-held call has a known owner, so crash close-out must not claim an unknown outcome.
pub fn provider_held_interrupted_result(tool_call_id: &ToolCallId) -> ToolResult {
    ToolResult {
        tool_call_id: tool_call_id.clone(),
        output: ToolOutput::Error {
            message: "provider_held_interrupted: the run was interrupted while the provider held the call".into(),
        },
        is_error: true,
    }
}

/// The resume record for the last episode of `prior`, resumed from the record at
/// `resumed_from_seq`, about to redo `redone`.
///
/// The indeterminate list is replay's own: every call with an intent and no result, and every
/// call already carrying an `Unknown` result, including one a crashed earlier resume wrote.
pub fn resume_started_record(
    prior: &ReplayState,
    resumed_from_seq: u64,
    redone: Vec<ResumeRedo>,
) -> WalRecord {
    let indeterminate = match &prior.resolution {
        ReplayResolution::Interrupted { indeterminate } => indeterminate.clone(),
        ReplayResolution::LineageIdle { .. }
        | ReplayResolution::Resume { .. }
        | ReplayResolution::AuthRequired { .. }
        | ReplayResolution::ProviderHeld { .. } => Vec::new(),
    };
    let tool_results = prior.open_tool_batch.as_ref().map_or(0, |batch| {
        batch
            .calls
            .iter()
            .filter(|(_, state)| *state == OpenCallState::Terminated)
            .count() as u64
    });
    WalRecord::ResumeStarted {
        episode: prior
            .run_id
            .clone()
            .unwrap_or_else(|| broca_protocol::RunId(String::new())),
        resumed_from_seq,
        replayed: ResumeReplayed {
            messages: prior.prompt.len() as u64,
            model_steps: prior.completed_steps,
            tool_results,
        },
        redone,
        indeterminate,
    }
}

/// What a resume that continues the episode will do again, read from replay's resolution: the
/// planned calls it dispatches because they never left the process, or, with no tool turn
/// pending, the model step it requests again because none of its output was durable. Empty for
/// every resolution the resume seals instead of continuing.
pub fn planned_redo(prior: &ReplayState) -> Vec<ResumeRedo> {
    match &prior.resolution {
        ReplayResolution::Resume {
            pending: Some(pending),
        } => pending
            .redispatch
            .iter()
            .map(|tool_call_id| ResumeRedo::ToolDispatch {
                tool_call_id: tool_call_id.clone(),
            })
            .collect(),
        ReplayResolution::Resume { pending: None } => vec![ResumeRedo::ModelStep {
            step_id: broca_protocol::StepId(prior.completed_steps + 1),
        }],
        ReplayResolution::LineageIdle { .. }
        | ReplayResolution::Interrupted { .. }
        | ReplayResolution::AuthRequired { .. }
        | ReplayResolution::ProviderHeld { .. } => Vec::new(),
    }
}

/// How a sealing caller closes an unanswered Prepared call. Reader builds choose the ungated
/// not-dispatched result; writer builds supply the question's durable filing identity.
pub enum DeferredCloseKind {
    NotDispatched,
    WithdrawElicitation(WithdrawElicitation),
}

/// Refuse a reader resume before it can drop a Prepared batch and enter a model step.
pub fn refuse_prepared_resume(
    pending: Option<&crate::replay::PendingTurn>,
) -> Result<(), crate::run::RunError> {
    if pending.is_some_and(|pending| !pending.deferred.is_empty() || !pending.approved.is_empty()) {
        return Err(prepared_resume_refusal());
    }
    Ok(())
}

pub(crate) fn prepared_resume_refusal() -> crate::run::RunError {
    crate::run::RunError::Config(
        "deferred_call_open: this reader cannot resume a Prepared approval call".into(),
    )
}

/// The records that close an indeterminate batch, or any batch retaining a Deferred/Approved
/// call, regardless of the resolution: one terminal per unfinished call, then commit markers.
/// Reader close-out cannot withdraw pending approval elicitations; a writer must still do so.
/// The generated records leave observation times unset until enqueued by the WAL writer,
/// rather than reconstructing them from an earlier dispatch.
///
/// A call that already has a terminal gets nothing, so a resume after a crash inside an earlier
/// resume's close-out writes only what that close-out did not make durable, and never a second
/// result for one call.
pub fn close_out_open_batch(
    prior: &ReplayState,
    mut deferred_close: impl FnMut(&ToolCallId) -> DeferredCloseKind,
) -> Vec<WalRecord> {
    let Some(batch) = &prior.open_tool_batch else {
        return Vec::new();
    };
    if !matches!(prior.resolution, ReplayResolution::Interrupted { .. })
        && !batch.calls.iter().any(|(_, state)| {
            matches!(
                state,
                OpenCallState::Deferred
                    | OpenCallState::Approved
                    | OpenCallState::ProviderHeld
                    | OpenCallState::HeldSuccessor
            )
        })
    {
        return Vec::new();
    }
    let mut records = Vec::with_capacity(batch.calls.len() + 2);
    for (tool_call_id, state) in &batch.calls {
        let withdraw_elicitation = if *state == OpenCallState::Deferred {
            match deferred_close(tool_call_id) {
                DeferredCloseKind::NotDispatched => None,
                DeferredCloseKind::WithdrawElicitation(withdrawal) => Some(withdrawal),
            }
        } else {
            None
        };
        let (result, outcome) = match state {
            OpenCallState::Terminated => continue,
            OpenCallState::Dispatched => (
                outcome_unknown_result(tool_call_id),
                DispatchOutcome::Unknown,
            ),
            OpenCallState::ProviderHeld => (
                provider_held_interrupted_result(tool_call_id),
                DispatchOutcome::Interrupted,
            ),
            OpenCallState::NotSent
            | OpenCallState::Deferred
            | OpenCallState::Approved
            | OpenCallState::HeldSuccessor => (
                not_dispatched_result(tool_call_id),
                DispatchOutcome::Interrupted,
            ),
        };
        records.push(WalRecord::ToolResult {
            finished_at_ms: None,
            tags: None,
            hooks: None,
            batch_id: batch.batch_id.clone(),
            result: Arc::new(result),
            outcome,
            reason: (*state == OpenCallState::ProviderHeld)
                .then(|| "provider_held_interrupted".into()),
            withdraw_elicitation,
            provider_held: None,
        });
    }
    records.push(WalRecord::ToolBatchFinished {
        batch_id: batch.batch_id.clone(),
    });
    records.push(WalRecord::TurnFinished {
        step_id: batch.step_id,
    });
    records
}

/// The records that keep a tool turn whose remaining calls were never dispatched, written when a
/// caller seals the episode `Interrupted` instead of resuming it.
///
/// Replay holds a model step that asked for tools out of the conversation until its batch
/// commits. A seal that wrote only `RunFinished` would therefore drop that already-written
/// assistant message from history, and dropping or rewriting a written message would need a new
/// lineage. Instead the turn is kept and completed: the batch is opened if it never was, each
/// planned call without a terminal gets [`not_dispatched_result`] with outcome
/// [`DispatchOutcome::Interrupted`], and the batch's commit markers follow. These are the records
/// a hard stop writes for calls it never dispatched: a definite "never ran" outcome, never
/// `Unknown`, so no reader reports the call as indeterminate and no later run sends it.
///
/// The caller must write these records and its terminal behind ONE durability barrier. A batch
/// committed with only synthetic results and no terminal reads to replay as a finished run, so a
/// crash between two barriers would end the episode with the wrong reason.
///
/// Empty unless replay would resume the turn by dispatching its calls (`Resume`, or a pause with
/// a pending turn). An indeterminate batch, or one holding a Prepared call, is closed by
/// [`close_out_open_batch`] instead.
pub fn close_out_pending_turn(prior: &ReplayState) -> Vec<WalRecord> {
    let pending = match &prior.resolution {
        ReplayResolution::Resume {
            pending: Some(pending),
        }
        | ReplayResolution::AuthRequired {
            pending: Some(pending),
            ..
        } => pending,
        ReplayResolution::ProviderHeld { .. }
        | ReplayResolution::Interrupted { .. }
        | ReplayResolution::LineageIdle { .. }
        | ReplayResolution::Resume { pending: None }
        | ReplayResolution::AuthRequired { pending: None, .. } => return Vec::new(),
    };
    if !pending.deferred.is_empty() || !pending.approved.is_empty() {
        return Vec::new();
    }
    let mut records = Vec::with_capacity(pending.planned.len() + 3);
    let batch_id = match &pending.batch_id {
        Some(batch_id) => batch_id.clone(),
        None => {
            // The id the run loop would have given this step's batch, so the kept turn reads
            // the same as one the loop committed itself.
            let run_id = prior.run_id.as_ref().map_or("", |run_id| run_id.0.as_str());
            let batch_id = broca_protocol::BatchId(format!("{run_id}-b{}", pending.step_id.0));
            records.push(WalRecord::ToolBatchStarted {
                batch_id: batch_id.clone(),
                step_id: pending.step_id,
                planned_calls: pending.planned.clone(),
            });
            batch_id
        }
    };
    for tool_call_id in &pending.planned {
        if pending.existing.iter().any(|(id, _)| id == tool_call_id) {
            continue;
        }
        records.push(WalRecord::ToolResult {
            finished_at_ms: None,
            tags: None,
            hooks: None,
            batch_id: batch_id.clone(),
            result: Arc::new(not_dispatched_result(tool_call_id)),
            outcome: DispatchOutcome::Interrupted,
            reason: None,
            withdraw_elicitation: None,
            provider_held: None,
        });
    }
    records.push(WalRecord::ToolBatchFinished { batch_id });
    records.push(WalRecord::TurnFinished {
        step_id: pending.step_id,
    });
    records
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replay::{
        deferred_tests::{approval, batch, finished, prepared, terminal},
        replay,
    };
    use broca_protocol::{BatchId, PauseReason, RunFinishReason, RunId};
    use broca_wal::DeferredDecision;

    #[test]
    fn seal_closes_prepared_calls_whatever_the_resolution() {
        let mut records = batch(&["deferred", "approved", "fresh", "done"]);
        records.extend([
            prepared("deferred", "b"),
            prepared("approved", "b"),
            approval("approved", "key-approved", DeferredDecision::Approved),
            terminal("done", None),
        ]);
        let prior = replay(&records).unwrap();
        let resolutions = [
            prior.resolution.clone(),
            ReplayResolution::Interrupted {
                indeterminate: vec![],
            },
            ReplayResolution::AuthRequired {
                run_id: RunId("r".into()),
                reason: PauseReason::Restart,
                error: None,
                pending: match &prior.resolution {
                    ReplayResolution::Resume { pending } => pending.clone(),
                    _ => unreachable!(),
                },
            },
            ReplayResolution::LineageIdle {
                last_reason: RunFinishReason::Interrupted,
            },
        ];
        for resolution in resolutions {
            let mut prior = prior.clone();
            prior.resolution = resolution;
            let close = close_out_open_batch(&prior, |_| DeferredCloseKind::NotDispatched);
            assert_eq!(close.len(), 5);
            for (record, id) in close.iter().take(3).zip(["deferred", "approved", "fresh"]) {
                let WalRecord::ToolResult {
                    result,
                    outcome,
                    withdraw_elicitation,
                    ..
                } = record
                else {
                    panic!("missing terminal")
                };
                assert_eq!(
                    **result,
                    not_dispatched_result(&broca_protocol::ToolCallId(id.into()))
                );
                assert_eq!(*outcome, DispatchOutcome::Interrupted);
                assert!(withdraw_elicitation.is_none());
                assert!(
                    record.required_features().is_empty(),
                    "reader close-out must stay ungated"
                );
            }
            assert!(
                matches!(&close[3], WalRecord::ToolBatchFinished { batch_id } if batch_id == &BatchId("b".into()))
            );
            assert!(
                matches!(&close[4], WalRecord::TurnFinished { step_id } if *step_id == broca_protocol::StepId(1))
            );
            let mut closed = records.clone();
            closed.extend(close);
            assert!(replay(&closed).unwrap().open_tool_batch.is_none());
        }
    }

    #[test]
    fn writer_close_kind_only_withdraws_deferred_calls_and_keeps_unknown_siblings() {
        let mut records = batch(&["deferred", "approved", "dispatched"]);
        records.extend([
            prepared("deferred", "b"),
            prepared("approved", "b"),
            approval("approved", "key-approved", DeferredDecision::Approved),
        ]);
        let mut prior = replay(&records).unwrap();
        prior.open_tool_batch.as_mut().unwrap().calls[2].1 = OpenCallState::Dispatched;
        prior.resolution = ReplayResolution::Interrupted {
            indeterminate: vec![ToolCallId("dispatched".into())],
        };
        for elicitation_id in [None, Some("question".into())] {
            let close = close_out_open_batch(&prior, |id| {
                assert_eq!(id.0, "deferred");
                DeferredCloseKind::WithdrawElicitation(WithdrawElicitation {
                    call_key: "key-deferred".into(),
                    elicitation_id: elicitation_id.clone(),
                })
            });
            assert!(
                matches!(&close[0], WalRecord::ToolResult { withdraw_elicitation: Some(w), .. } if w.elicitation_id == elicitation_id)
            );
            assert!(matches!(
                &close[1],
                WalRecord::ToolResult {
                    withdraw_elicitation: None,
                    outcome: DispatchOutcome::Interrupted,
                    ..
                }
            ));
            assert!(
                matches!(&close[2], WalRecord::ToolResult { withdraw_elicitation: None, outcome: DispatchOutcome::Unknown, result, .. }
                if **result == outcome_unknown_result(&broca_protocol::ToolCallId("dispatched".into())))
            );
        }
    }

    #[test]
    fn ordinary_resume_without_prepared_calls_does_not_close_batch() {
        let prior = replay(&batch(&["fresh"])).unwrap();
        assert!(close_out_open_batch(&prior, |_| panic!("not a deferred call")).is_empty());
        assert!(refuse_prepared_resume(None).is_ok());
        if let ReplayResolution::Resume { pending } = &prior.resolution {
            assert!(refuse_prepared_resume(pending.as_ref()).is_ok());
        }
    }

    /// A sealed tool turn keeps every written message: the step whose batch never opened gets a
    /// batch, each call a never-executed result, and replay then commits the turn to the prompt.
    #[test]
    fn pending_turn_close_out_keeps_a_step_whose_batch_never_opened() {
        let mut records = batch(&["first", "second"]);
        records.pop(); // the step is durable, its ToolBatchStarted is not
        let prior = replay(&records).unwrap();
        let close = close_out_pending_turn(&prior);
        let planned = vec![ToolCallId("first".into()), ToolCallId("second".into())];
        assert!(
            matches!(&close[0], WalRecord::ToolBatchStarted { batch_id, step_id, planned_calls }
                if batch_id == &BatchId("r-b1".into())
                    && *step_id == broca_protocol::StepId(1)
                    && planned_calls == &planned),
            "{close:#?}"
        );
        for (record, id) in close[1..3].iter().zip(&planned) {
            assert!(
                matches!(record, WalRecord::ToolResult { result, outcome: DispatchOutcome::Interrupted, batch_id, .. }
                    if **result == not_dispatched_result(id) && batch_id == &BatchId("r-b1".into())),
                "{record:#?}"
            );
        }
        assert!(matches!(&close[3], WalRecord::ToolBatchFinished { .. }));
        assert!(matches!(&close[4], WalRecord::TurnFinished { .. }));
        assert_eq!(close.len(), 5);
        let mut closed = records.clone();
        closed.extend(close);
        let after = replay(&closed).unwrap();
        assert!(after.open_tool_batch.is_none());
        assert_eq!(
            after.prompt.len(),
            prior.prompt.len() + 2,
            "the step and its results join the transcript"
        );
        assert_eq!(after.prompt[..prior.prompt.len()], prior.prompt[..]);
    }

    /// A batch that already holds a result keeps it: only the calls without one are closed, in
    /// the durable batch, and no second batch is opened.
    #[test]
    fn pending_turn_close_out_reuses_an_open_batch_and_keeps_its_results() {
        let mut records = batch(&["done", "fresh"]);
        records.push(terminal("done", None));
        let prior = replay(&records).unwrap();
        let close = close_out_pending_turn(&prior);
        assert_eq!(close.len(), 3, "{close:#?}");
        assert!(
            matches!(&close[0], WalRecord::ToolResult { result, batch_id, .. }
                if **result == not_dispatched_result(&ToolCallId("fresh".into()))
                    && batch_id == &BatchId("b".into()))
        );
        let mut closed = records.clone();
        closed.extend(close);
        assert!(replay(&closed).unwrap().open_tool_batch.is_none());
    }

    /// Only a turn replay would resume by dispatching is closed here: nothing pending, an
    /// indeterminate batch and a Prepared call are left to their own paths.
    #[test]
    fn pending_turn_close_out_leaves_other_resolutions_alone() {
        let mut idle = batch(&["done"]);
        idle.push(terminal("done", None));
        idle.extend(finished());
        assert!(close_out_pending_turn(&replay(&idle).unwrap()).is_empty());
        let mut deferred = batch(&["deferred"]);
        deferred.push(prepared("deferred", "b"));
        assert!(close_out_pending_turn(&replay(&deferred).unwrap()).is_empty());
        let mut prior = replay(&batch(&["sent"])).unwrap();
        prior.resolution = ReplayResolution::Interrupted {
            indeterminate: vec![ToolCallId("sent".into())],
        };
        assert!(close_out_pending_turn(&prior).is_empty());
    }

    #[tokio::test]
    async fn reader_core_resume_refuses_restart_paused_prepared_batch() {
        use crate::{
            AllowAll, FileLeaseStore, FileWalStore, RunRequest, StubProvider, StubToolPlane,
            WalStore,
        };
        use broca_protocol::{
            request::{GenerationConfig, ModelHandle, ToolChoice},
            BindIdentity,
        };
        use std::io::Write;

        for approved in [false, true] {
            let root = broca_test_support::scratch_root().join(format!(
                "broca-reader-refusal-{}-{approved}",
                std::process::id()
            ));
            std::fs::create_dir_all(root.join("project")).unwrap();
            let session = BindIdentity::new(root.join("project"), "runner", "approval");
            let store = FileWalStore::new(root.join("wal"));
            let mut records = batch(&["call"]);
            if let WalRecord::RunStarted {
                session: stored_session,
                ..
            } = &mut records[0]
            {
                *stored_session = session.clone();
            }
            records.push(prepared("call", "b"));
            if approved {
                records.push(approval("call", "key-call", DeferredDecision::Approved));
            }
            records.push(WalRecord::RunPaused {
                run_id: RunId("r".into()),
                ts_ms: Some(1),
                reason: PauseReason::Restart,
                error: None,
            });
            let mut wal = store.open(&session).unwrap();
            wal.append(1, &records[0], true).unwrap();
            drop(wal);
            // Exercise the reader after decoding; feature placement is separately blocked until
            // every reader slice lands, so these payload frames bypass only the feature table.
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(store.wal_path(&session))
                .unwrap();
            for (index, record) in records.iter().enumerate().skip(1) {
                file.write_all(
                    &broca_wal::framing::encode_payload(
                        (index + 1) as u64,
                        1,
                        &serde_json::to_vec(record).unwrap(),
                    )
                    .unwrap(),
                )
                .unwrap();
            }
            file.sync_all().unwrap();
            drop(file);
            let before = std::fs::read(store.wal_path(&session)).unwrap();
            let request = RunRequest {
                directives: None,
                session_class: None,
                retention: None,
                title: None,
                on_restart: None,
                keep_warm: None,
                scope: None,
                send_id: None,
                request_identity_digest: None,
                project_id: None,
                principal: None,
                run_id: RunId("r".into()),
                session: session.clone(),
                model: ModelHandle::new("stub", "m"),
                input: vec![],
                tools: vec![],
                policy_allowed: Default::default(),
                tool_choice: ToolChoice::Auto,
                generation: GenerationConfig::default(),
                cache: None,
                stop_when: vec![],
                submission_id: None,
                budget: None,
                context_limit: None,
                work_class: None,
                context_management: None,
                variant: None,
                service_tier: None,
                inherit_frozen_cache: false,
                structured_output: None,
            };
            let provider = StubProvider::new(vec![]);
            let plane = StubToolPlane::new()
                .with_tool("edit", |_| panic!("reader dispatched a Prepared call"));
            let lease = FileLeaseStore::new(root.join("lease"));
            let (out, _events) = tokio::sync::mpsc::channel(64);
            let error =
                crate::run::run_loop(request, &provider, &plane, &lease, &AllowAll, &store, &out)
                    .await
                    .unwrap_err();
            assert!(
                matches!(error, crate::run::RunError::Config(ref reason) if reason.starts_with("deferred_call_open:")),
                "unexpected refusal: {error:?}"
            );
            assert_eq!(
                std::fs::read(store.wal_path(&session)).unwrap(),
                before,
                "refusal writes no gated records or model steps"
            );
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}
