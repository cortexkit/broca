//! The records that close a run's open tool batch or unanswered tool turn when the run stops
//! without sending anything more: after a crash, a shutdown, or a cancel.

use super::*;

/// The records that close the run's open batch after an interruption or cancellation, sending nothing: an
/// `outcome_unknown` result for each call that was sent with no result recorded, a not-run
/// result for each planned call that never left, a provider-held call's close result, and the
/// batch's end. Also the held calls whose providers must be asked to withdraw once the run's end
/// is durable. Nothing without an open batch.
pub(super) fn open_batch_close_out<W: Writer>(
    writer: &W,
    run_id: &str,
    now: u64,
    reason: RunFinishReason,
) -> (Vec<Record>, Vec<ProviderHeldReply>) {
    let mut records = Vec::new();
    let mut withdrawn = Vec::new();
    if let Some(batch) = run_state(writer).open_batch.as_ref() {
        // The calls still waiting for a result, in the order the model asked for them. With
        // every call answered the turn is no longer open, and only the batch needs closing.
        let open = writer.conversation().open_tool_turn();
        for block in open.iter().flat_map(|open| open.content.iter()) {
            let broca_protocol::content::ContentBlock::ToolCall(call) = block else {
                continue;
            };
            let id = &call.tool_call_id;
            let (result, outcome, reason) = match batch.state_of(id) {
                Some(CallState::Dispatched) => (
                    error_result(id, OUTCOME_UNKNOWN),
                    CallOutcome::Unknown,
                    None,
                ),
                Some(CallState::NotSent | CallState::HeldSuccessor) => (
                    error_result(id, NOT_RUN_INTERRUPTED),
                    CallOutcome::Interrupted,
                    None,
                ),
                Some(CallState::ProviderHeld) => {
                    let (result, _, code) =
                        broca_core::tool_results::provider_held_close_result(id, reason);
                    let position = (0..batch.calls.len())
                        .find(|position| batch.id(*position) == id)
                        .expect("a held call is planned");
                    if let Some(held) = batch.held_reply(position) {
                        withdrawn.push(ProviderHeldReply {
                            tool_call_id: held.tool_call_id.clone(),
                            call_key: held.call_key.clone(),
                            module: held.module.clone(),
                            reason: held.reason.clone(),
                            detail_digest: held.detail_digest.clone(),
                        });
                    }
                    (result, CallOutcome::Interrupted, Some(code.to_string()))
                }
                Some(CallState::Terminated) | None => continue,
            };
            terminal_with_reason(
                &mut records,
                run_id,
                &batch.batch_id,
                result,
                outcome,
                reason,
                Some(now),
            );
        }
        records.push(Record::BatchEnd(BatchEnd {
            run_id: run_id.into(),
            batch_id: batch.batch_id.clone(),
        }));
    }
    (records, withdrawn)
}

/// The records that keep a tool turn whose calls were never dispatched, as
/// `broca_core::close_out_pending_turn` writes them: the step's batch started if it wasn't, a
/// never-run result for each call without one, and the batch's end. Nothing when the run's last
/// step asked for no tools.
pub(super) fn never_dispatched_close_out<W: Writer>(
    writer: &W,
    run_id: &str,
    now: u64,
) -> Vec<Record> {
    let Some(open) = writer.conversation().open_tool_turn() else {
        return Vec::new();
    };
    let planned: Vec<ToolCallId> = open
        .content
        .iter()
        .filter_map(|block| match block {
            broca_protocol::content::ContentBlock::ToolCall(call) => {
                Some(call.tool_call_id.clone())
            }
            _ => None,
        })
        .collect();
    if planned.is_empty() {
        return Vec::new();
    }
    let run = run_state(writer);
    let mut records = Vec::new();
    let batch_id = match run.open_batch.as_ref() {
        Some(batch) => batch.batch_id.clone(),
        None => {
            let batch_id = format!("{run_id}-b{}", run.completed_steps);
            records.push(Record::BatchStart(BatchStart {
                run_id: run_id.into(),
                batch_id: batch_id.clone(),
                step_id: run.completed_steps,
                planned: planned.clone(),
                mid: None,
                shown_tags: Vec::new(),
            }));
            batch_id
        }
    };
    let answered = |id: &ToolCallId| {
        run.open_batch
            .as_ref()
            .is_some_and(|batch| batch.state_of(id) == Some(CallState::Terminated))
    };
    for id in planned.iter().filter(|id| !answered(id)) {
        terminal(
            &mut records,
            run_id,
            &batch_id,
            broca_core::tool_results::not_dispatched_result(id),
            CallOutcome::Interrupted,
            now,
        );
    }
    records.push(Record::BatchEnd(BatchEnd {
        run_id: run_id.into(),
        batch_id,
    }));
    records
}

/// The result, outcome and code a held call gets when a cancel closes it.
pub(super) fn held_close_result(
    id: &ToolCallId,
    reason: RunFinishReason,
) -> (ToolResult, CallOutcome, &'static str) {
    let (result, _, code) = broca_core::tool_results::provider_held_close_result(id, reason);
    (result, CallOutcome::Interrupted, code)
}
