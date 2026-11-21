//! How a run ends when it stops before its tool batch: a stop condition its step met, or a stop
//! or cancel that arrived between the model's answer and the batch. No call is sent.

use super::*;

/// Every call the open tool turn's assistant message asked for.
/// The stop condition the step that asked for the open tool turn's calls met, while that step's
/// batch hasn't started. The live loop closes such calls as not run right after the answer
/// (`hard_stop`), so a resume after a crash in between closes them the same way rather than
/// sending them and asking the model for a step the run had no room for.
pub(super) fn stop_before_batch<W: Writer>(
    writer: &W,
    stop_when: &[StopCondition],
) -> Option<StopCondition> {
    if run_state(writer).open_batch.is_some() {
        return None;
    }
    let open = writer.conversation().open_tool_turn()?;
    let answer = AssistantMessage {
        message_id: broca_protocol::MessageId(String::new()),
        content: open.content.clone(),
    };
    stop_satisfied(stop_when, run_state(writer).completed_steps, &answer).cloned()
}

/// Ends the run at a step that met `stop` before its batch: each of `calls` closes as not run
/// (max steps for a step limit), and the batch, the budget firings the calls made due and the
/// run's end are one barrier.
pub(super) async fn hard_stop<W: Writer, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    run_id: &str,
    step_id: u64,
    calls: &[ToolCall],
    stop: &StopCondition,
) -> Result<Finished, LoopError> {
    let (outcome, message, reason) = match stop {
        StopCondition::StepCount { .. } => (
            CallOutcome::MaxSteps,
            "tool not run: max steps reached before dispatch",
            RunFinishReason::MaxSteps,
        ),
        _ => (
            CallOutcome::Interrupted,
            "tool not run: interrupted before dispatch",
            RunFinishReason::Completed,
        ),
    };
    let batch_id = format!("{run_id}-b{step_id}");
    let mut records = vec![Record::BatchStart(BatchStart {
        run_id: run_id.into(),
        batch_id: batch_id.clone(),
        step_id,
        planned: calls.iter().map(|call| call.tool_call_id.clone()).collect(),
        mid: None,
        shown_tags: Vec::new(),
    })];
    let now = (seams.now_ms)();
    for call in calls {
        let result = ToolResult {
            tool_call_id: call.tool_call_id.clone(),
            output: ToolOutput::error(message.to_string()),
            is_error: true,
        };
        terminal(&mut records, run_id, &batch_id, result, outcome, now);
    }
    records.push(Record::BatchEnd(BatchEnd {
        run_id: run_id.into(),
        batch_id,
    }));
    // The budget firings the step's calls made due go in the same barrier, after the closed
    // batch and before the run's end.
    let end = run_end(writer, run_id, reason, now);
    let run_id = run_id.to_string();
    let mut records = Some(records);
    let mut end = Some(end);
    writer
        .append_with(
            &mut |conversation, operations, first_seq| {
                let mut batch = records.take().expect("one batch");
                let placing = closing_places(conversation);
                let firings =
                    crate::budget::due(conversation, operations, first_seq, batch.len(), placing);
                batch.extend(firings);
                batch.extend(placed_copies(operations, &run_id));
                batch.extend(end.take());
                batch
            },
            true,
        )
        .await?;
    Ok(finished(writer, reason))
}

/// The runner asked the run to stop, or the caller cancelled it, before its batch was
/// dispatched. No call is sent: each gets a result saying so, the tool plane is never told the
/// batch started, and the run ends cancelled (`cancelled`) or interrupted whatever its restart
/// policy. The closed batch and the run's end are one barrier.
pub(super) async fn stopped_batch<W: Writer, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    run_id: &str,
    step_id: u64,
    calls: &[ToolCall],
    cancelled: bool,
) -> Result<Finished, LoopError> {
    stopped_batch_in(writer, seams, run_id, step_id, calls, cancelled, true).await
}

/// [`stopped_batch`], also for a batch that already started (`start_batch` false), whose
/// start is already durable: then only `calls`, the ones never sent, are closed.
pub(super) async fn stopped_batch_in<W: Writer, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    run_id: &str,
    step_id: u64,
    calls: &[ToolCall],
    cancelled: bool,
    start_batch: bool,
) -> Result<Finished, LoopError> {
    let batch_id = match run_state(writer).open_batch.as_ref() {
        Some(open) if !start_batch => open.batch_id.clone(),
        _ => format!("{run_id}-b{step_id}"),
    };
    let now = (seams.now_ms)();
    let mut records = Vec::new();
    if start_batch {
        records.push(Record::BatchStart(BatchStart {
            run_id: run_id.into(),
            batch_id: batch_id.clone(),
            step_id,
            planned: calls.iter().map(|call| call.tool_call_id.clone()).collect(),
            mid: None,
            shown_tags: Vec::new(),
        }));
    }
    let mut seen = std::collections::HashSet::new();
    for call in calls {
        // A repeated call id gets the duplicate error, not "not run".
        if !seen.insert(call.tool_call_id.clone()) {
            terminal_with_reason(
                &mut records,
                run_id,
                &batch_id,
                error_result(&call.tool_call_id, "duplicate tool_call_id from provider"),
                CallOutcome::Completed,
                None,
                Some(now),
            );
            continue;
        }
        let result = ToolResult {
            tool_call_id: call.tool_call_id.clone(),
            output: ToolOutput::error(NOT_DISPATCHED.to_string()),
            is_error: true,
        };
        terminal(
            &mut records,
            run_id,
            &batch_id,
            result,
            CallOutcome::Interrupted,
            now,
        );
    }
    records.push(Record::BatchEnd(BatchEnd {
        run_id: run_id.into(),
        batch_id,
    }));
    // A caller's cancel ends the run cancelled; a stop ends it interrupted by the shutdown. The
    // closed batch and the run's end are durable together: a crash between them would leave a
    // run whose calls all have results, which a resume ends as completed.
    let reason = if cancelled {
        RunFinishReason::Cancelled
    } else {
        RunFinishReason::Interrupted
    };
    let mut end = run_end(writer, run_id, reason, now);
    if let Record::RunEnd(end) = &mut end {
        if !cancelled {
            end.interrupt_reason = Some("runner_shutdown".into());
        }
    }
    records.push(end);
    append_end(writer, run_id, records).await?;
    Ok(finished(writer, reason))
}
