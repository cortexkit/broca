//! Run stops derived from settled steps and tool turns, not from attempt-local state.

use super::*;
use broca_core::tool_repetition::{ConsecutiveCalls, RepetitionCheckpoint};
use broca_protocol::{content::Role, ErrorClass};

pub(super) struct CutCalls {
    pub records: Vec<Record>,
    pub uncut: Vec<ToolCall>,
    pub stop: bool,
}

impl CutCalls {
    pub(super) async fn prepare<W: Writer>(
        writer: &W,
        run_id: &str,
        step_id: u64,
        step: &Step,
        calls: &[ToolCall],
        shown_tags: &[broca_session_store::record::TagRange],
        now: u64,
    ) -> Result<Self, LoopError> {
        let stop = !calls.is_empty()
            && step.finish_reason == FinishReason::Length
            && consecutive_length(writer, 2).await?;
        let mut records = Vec::new();
        if !stop && !step.cuts.is_empty() {
            let batch_id = format!("{run_id}-b{step_id}");
            records.push(Record::BatchStart(BatchStart {
                run_id: run_id.into(),
                batch_id: batch_id.clone(),
                step_id,
                planned: calls.iter().map(|call| call.tool_call_id.clone()).collect(),
                mid: None,
                shown_tags: shown_tags.to_vec(),
            }));
            for cut in &step.cuts {
                if step.finish_reason == FinishReason::Length {
                    no_dispatch(
                        &mut records,
                        run_id,
                        &batch_id,
                        &cut.tool_call_id,
                        &NoDispatchReason::ArgumentsIncomplete {
                            tool_name: cut.tool_name.clone(),
                        },
                        None,
                        now,
                    );
                    if let Some(Record::ToolOutcome(outcome)) = records.last_mut() {
                        outcome.reason = Some("arguments_incomplete".into());
                    }
                } else {
                    // A scripted or older provider can still supply the earlier cut control
                    // on a tool-call finish. Only a length finish changes its meaning.
                    terminal_with_reason(
                        &mut records,
                        run_id,
                        &batch_id,
                        error_result(&cut.tool_call_id, &cut.error_text()),
                        CallOutcome::Completed,
                        Some("tool_args_trailing_garbage".into()),
                        Some(now),
                    );
                }
            }
        }
        let uncut = calls
            .iter()
            .filter(|call| {
                !step
                    .cuts
                    .iter()
                    .any(|cut| cut.tool_call_id == call.tool_call_id)
            })
            .cloned()
            .collect();
        Ok(Self {
            records,
            uncut,
            stop,
        })
    }
}

/// Retry and pause records do not break a streak; only a finished step can do that. A seal may
/// hide the earlier finishes, in which case we stop only once enough new finishes are recorded.
async fn consecutive_length<W: Writer>(writer: &W, count: usize) -> Result<bool, LoopError> {
    let run = run_state(writer);
    if run.last_finish_reason != Some(FinishReason::Length) || run.completed_steps < count as u64 {
        return Ok(false);
    }
    let mut records = writer.open_run_tail(&run.run_id, 0).await?;
    if records.is_empty() {
        // The run's start may be behind a seal even when its latest finishes are open.
        records = writer.open_log_records().await?;
    }
    let mut finishes = records.iter().rev().filter_map(|(_, record)| match record {
        Record::StepEnd(end) if end.run_id == run.run_id => Some(end),
        _ => None,
    });
    for offset in 0..count as u64 {
        if !finishes.next().is_some_and(|end| {
            end.step_id == run.completed_steps - offset
                && end.finish_reason == Some(FinishReason::Length)
        }) {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) async fn resumed_output_limit<W: Writer>(writer: &W) -> Result<bool, LoopError> {
    Ok(writer.conversation().open_tool_turn().is_some()
        && run_state(writer).open_batch.is_none()
        && consecutive_length(writer, 3).await?)
}

/// Parallel siblings do not wait for one another. Check the first call at a batch boundary
/// against settled results; a streak that crosses the limit inside a parallel batch is caught
/// at the next boundary instead of speculating about results still in flight.
pub(super) async fn tool_loop<W: Writer>(
    writer: &W,
    call: Option<&ToolCall>,
) -> Result<bool, LoopError> {
    let Some(candidate) =
        call.filter(|_| run_state(writer).tool_call_count >= u64::from(ConsecutiveCalls::STOP_AT))
    else {
        return Ok(false);
    };
    let conversation = writer.conversation();
    let Some(episode) = conversation
        .episodes
        .last()
        .filter(|episode| episode.run_id == run_state(writer).run_id)
    else {
        return Ok(false);
    };
    let window = conversation.window_start();
    let first = episode
        .first_model_answer
        .unwrap_or(episode.start)
        .max(window);
    let needed = ConsecutiveCalls::STOP_AT as usize - 1;
    let mut recent = Vec::with_capacity(needed);
    let mut position = conversation.placed;
    while position > first && recent.len() < needed {
        if !conversation
            .window_entry(position)
            .is_some_and(|(entry, _)| entry.role() == Role::Tool)
        {
            position -= 1;
            continue;
        }
        let assistant = writer.window_message_whole(position - 1).await?;
        if assistant.role != Role::Assistant {
            return Ok(false);
        }
        let calls: Vec<_> = assistant
            .content
            .iter()
            .filter_map(|block| match block {
                broca_protocol::ContentBlock::ToolCall(call) => Some(call),
                _ => None,
            })
            .collect();
        if calls.last().is_none_or(|call| {
            call.tool_name != candidate.tool_name || call.input != candidate.input
        }) {
            return Ok(false);
        }
        let tool = writer.window_message_whole(position).await?;
        let results: Vec<_> = tool
            .content
            .iter()
            .filter_map(|part| match part {
                broca_protocol::ContentBlock::ToolResult(result) => Some(result),
                _ => None,
            })
            .collect();
        if calls.len() != results.len() {
            return Ok(false);
        }
        for (call, result) in calls.into_iter().zip(results).rev() {
            if call.tool_name != candidate.tool_name || call.input != candidate.input {
                return Ok(false);
            }
            recent.push(result.clone());
            if recent.len() == needed {
                break;
            }
        }
        position -= 2;
    }
    let mut streak = if recent.len() < needed && episode.start < window {
        match &conversation.window_repetition {
            Some(seed) => {
                let seed: RepetitionCheckpoint =
                    serde_json::from_value(seed.clone()).map_err(|error| {
                        LoopError::Parts(format!("stored repetition counters: {error}"))
                    })?;
                ConsecutiveCalls::seeded(&seed)
            }
            None => ConsecutiveCalls::default(),
        }
    } else {
        ConsecutiveCalls::default()
    };
    for result in recent.iter().rev() {
        streak.observe_result(candidate, result);
    }
    Ok(streak.stops(candidate))
}

pub(super) async fn stop<W: Writer, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    run_id: &str,
    output_limit: bool,
) -> Result<Finished, LoopError> {
    let (code, message) = if output_limit {
        (
            "output_limit",
            "Three consecutive model steps reached the output limit.",
        )
    } else {
        ("tool_loop", "The same tool call returned the same result 19 times in a row; the next call was not run.")
    };
    let mut error = incomplete_stream();
    error.class = ErrorClass::Permanent;
    error.provider_code = Some(code.into());
    error.message = message.into();
    end_with_error(writer, seams, run_id, RunFinishReason::Error, &error).await
}
