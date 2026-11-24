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
        for (call, position) in open
            .map(|open| turn_positions(open, Some(batch)))
            .unwrap_or_default()
        {
            let id = &call.tool_call_id;
            let (result, outcome, reason) = match position.map(|position| batch.state(position)) {
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
                    let position = position.expect("a held call is planned");
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

/// Each tool call of `turn`, in the model's order, with its position among `batch`'s planned
/// calls when the batch plans it. A model can repeat a call id, and each copy has its own state:
/// the n-th copy of an id in the turn is the batch's n-th planned copy of it, so a repeated copy
/// is never read as the first copy, which may already be answered.
pub(super) fn turn_positions<'a>(
    turn: &'a Message,
    batch: Option<&broca_session_store::fold::operations::OpenBatch>,
) -> Vec<(&'a ToolCall, Option<usize>)> {
    let mut seen: Vec<&ToolCallId> = Vec::new();
    turn.content
        .iter()
        .filter_map(|block| match block {
            broca_protocol::content::ContentBlock::ToolCall(call) => Some(call),
            _ => None,
        })
        .map(|call| {
            let id = &call.tool_call_id;
            let copy = seen.iter().filter(|seen| **seen == id).count();
            seen.push(id);
            let position = batch.and_then(|batch| {
                (0..batch.calls.len())
                    .filter(|position| batch.id(*position) == id)
                    .nth(copy)
            });
            (call, position)
        })
        .collect()
}

/// The records that keep a tool turn whose calls were never dispatched: the step's batch started
/// if it wasn't, a never-run result for each call without one, and the batch's end. Nothing when
/// the run's last step asked for no tools.
pub(super) fn never_dispatched_close_out<W: Writer>(
    writer: &W,
    run_id: &str,
    now: u64,
) -> Vec<Record> {
    let Some(open) = writer.conversation().open_tool_turn() else {
        return Vec::new();
    };
    let run = run_state(writer);
    let calls = turn_positions(open, run.open_batch.as_ref());
    let planned: Vec<ToolCallId> = calls
        .iter()
        .map(|(call, _)| call.tool_call_id.clone())
        .collect();
    if planned.is_empty() {
        return Vec::new();
    }
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
    let answered = |position: Option<usize>| {
        run.open_batch
            .as_ref()
            .zip(position)
            .is_some_and(|(batch, position)| batch.state(position) == CallState::Terminated)
    };
    for (call, _) in calls.iter().filter(|(_, position)| !answered(*position)) {
        terminal(
            &mut records,
            run_id,
            &batch_id,
            broca_core::tool_results::not_dispatched_result(&call.tool_call_id),
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

/// Ends the session's unfinished latest run `run_id` as `reason`, with `error` on its end and
/// `now_ms` as its time: the end the loop writes for a run that can't go on, durable before it
/// returns, with the messages steered or delivered since the run's last step boundary. For the
/// session's owner, when a run's loop stopped without writing an end.
///
/// A tool turn the run left open closes first, as a seal closes it ([`end_closing_turn`]); each
/// call a provider still holds is withdrawn through `held` once the end is durable.
pub async fn end_run_with_error<W: Writer>(
    writer: &mut W,
    run_id: &str,
    reason: RunFinishReason,
    error: &ProviderError,
    now_ms: u64,
    held: Option<&dyn ProviderHeldLifecycle>,
) -> Result<Finished, LoopError> {
    end_run_with_error_and_interrupt_reason(writer, run_id, reason, error, now_ms, None, held).await
}

/// [`end_run_with_error`] with the recorded interruption reason the actor's seal selected, so
/// callers read the same cause whether the loop or its actor wrote the interrupted end.
pub async fn end_run_with_error_and_interrupt_reason<W: Writer>(
    writer: &mut W,
    run_id: &str,
    reason: RunFinishReason,
    error: &ProviderError,
    now_ms: u64,
    interrupt_reason: Option<String>,
    held: Option<&dyn ProviderHeldLifecycle>,
) -> Result<Finished, LoopError> {
    let mut record = run_end(writer, run_id, reason, now_ms);
    if let Record::RunEnd(end) = &mut record {
        end.error = Some(
            serde_json::to_value(error)
                .map_err(|e| LoopError::Parts(e.to_string()))?
                .into(),
        );
        end.interrupt_reason = interrupt_reason;
    }
    end_closing_turn(writer, run_id, reason, now_ms, record, held).await?;
    Ok(finished(writer, reason))
}

/// Appends the run's `end`, sending nothing, after the records that close the tool turn the run
/// left open, as a seal closes it: the open batch's close-out (an unknown outcome for each call
/// that was sent, not run for each that wasn't, a held call's close result), or with no open
/// batch a never-run result for each call of a turn whose batch never started. Without them the
/// turn would stay open past the run's end, the next run would leave it out of its input, and a
/// message steered in before the end could not be placed inside it. Each call a provider still
/// holds is withdrawn through `held` once the end is durable.
async fn end_closing_turn<W: Writer>(
    writer: &mut W,
    run_id: &str,
    reason: RunFinishReason,
    now_ms: u64,
    end: Record,
    held: Option<&dyn ProviderHeldLifecycle>,
) -> Result<(), LoopError> {
    let (mut records, withdrawn) = open_batch_close_out(writer, run_id, now_ms, reason);
    if records.is_empty() {
        records = never_dispatched_close_out(writer, run_id, now_ms);
    }
    records.push(end);
    append_end(writer, run_id, records).await?;
    if let Some(lifecycle) = held {
        for reply in &withdrawn {
            let _ = lifecycle.withdraw_held(reply).await;
        }
    }
    Ok(())
}

/// Records a refused restart continuation and its interrupted terminal without dispatching
/// anything. Set `record_resume` when the attempt failed before recording a resume entry;
/// owners that already recorded it keep the existing entry instead of appending a duplicate.
/// A tool turn the run left open closes first, as [`end_run_with_error`] closes it.
pub async fn end_restart_resume_refused<W: Writer>(
    writer: &mut W,
    run_id: &str,
    error: &ProviderError,
    now_ms: u64,
    record_resume: bool,
    held: Option<&dyn ProviderHeldLifecycle>,
) -> Result<Finished, LoopError> {
    if record_resume {
        let resolution = run_state(writer).resolution();
        let mut batch = Some(vec![Record::Resumed(resumed_record(writer, &resolution))]);
        writer
            .append_with(
                &mut |_, _, first_seq| {
                    let mut batch = batch.take().expect("one refused resume entry");
                    name_resumed_from(&mut batch, first_seq);
                    batch
                },
                false,
            )
            .await?;
    }
    let mut record = run_end(writer, run_id, RunFinishReason::Interrupted, now_ms);
    if let Record::RunEnd(end) = &mut record {
        end.interrupt_reason = Some("runner_crash".into());
        end.error = Some(
            serde_json::to_value(error)
                .map_err(|e| LoopError::Parts(e.to_string()))?
                .into(),
        );
    }
    end_closing_turn(
        writer,
        run_id,
        RunFinishReason::Interrupted,
        now_ms,
        record,
        held,
    )
    .await?;
    Ok(finished(writer, RunFinishReason::Interrupted))
}
