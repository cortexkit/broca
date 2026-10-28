//! Read-only, log-ordered session diagnostics. Only selected fields leave the WAL;
//! authentication configuration and provider continuation fields are excluded.

use std::collections::HashMap;
use std::path::Path;

use broca_core::FileWalStore;
use broca_protocol::content::{ContentBlock, ToolOutput, ToolOutputPart};
use broca_protocol::BindIdentity;
use broca_wal::{FramedRecord, WalRecord};
use serde_json::{json, Value};

pub fn read(root: &Path, bind: &BindIdentity, transcript: bool) -> Result<Vec<Value>, String> {
    bind.validate_keyable().map_err(|e| e.to_string())?;
    let store = FileWalStore::new(root.join("wal"));
    let clock =
        broca_core::Clock::for_state_root(root).map_err(|_| "cannot read retention clock")?;
    let (lineage, frames) = crate::cache_dump::exportable_frames(&store, bind, clock.now_ms())?;
    if frames.is_empty() {
        return Err(format!("no WAL records for session {:?}", bind.session));
    }
    rows(
        &frames,
        lineage.map(|id| id.to_string()).as_deref(),
        transcript,
    )
}

fn name(value: &impl serde::Serialize) -> Result<String, String> {
    serde_json::to_value(value)
        .map_err(|_| "cannot encode event name".to_string())?
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| "event name is not a string".into())
}

fn output_text(output: &ToolOutput) -> String {
    match output {
        ToolOutput::Text { text } => text.clone(),
        ToolOutput::Json { value } => value.to_string(),
        ToolOutput::Parts { parts } => parts
            .iter()
            .filter_map(|part| match part {
                ToolOutputPart::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n"),
        ToolOutput::Denied { reason } => reason.clone(),
        ToolOutput::Error {
            message,
            module,
            code,
            detail,
            nothing_sent,
            retry,
        } => ToolOutput::error_model_text(
            message,
            module.as_deref(),
            code.as_deref(),
            detail.as_deref(),
            *nothing_sent,
            retry.as_deref(),
        ),
    }
}

fn exit_code(text: &str) -> Option<i64> {
    text.lines().find_map(|line| {
        line.trim()
            .strip_prefix("[exit code: ")?
            .strip_suffix(']')?
            .parse()
            .ok()
    })
}

fn bash_fields(row: &mut Value, args: &Value) {
    for key in ["command", "runon", "background", "timeout_ms", "wait"] {
        if let Some(value) = args.get(key) {
            row[key] = value.clone();
        }
    }
    // The tool surface calls this knob `timeout`; older recorded calls may already
    // use `timeout_ms`. Both are milliseconds, and neither gets a guessed default.
    if row.get("timeout_ms").is_none() {
        if let Some(value) = args.get("timeout") {
            row["timeout_ms"] = value.clone();
        }
    }
}

pub fn rows(
    frames: &[FramedRecord],
    lineage: Option<&str>,
    transcript: bool,
) -> Result<Vec<Value>, String> {
    let mut rows = Vec::new();
    let mut sequences = Vec::new();
    let mut model = None;
    let mut run = None;
    let mut previous_end: Option<(Option<u64>, Option<broca_protocol::RunId>, String)> = None;
    let mut batches = HashMap::new();
    let mut calls = HashMap::new();
    let mut planned = HashMap::new();
    for frame in frames {
        let before = rows.len();
        match &frame.record {
            WalRecord::RunStarted {
                run_id,
                config,
                ts_ms,
                send_id,
                ..
            } => {
                if let Some((from, previous_run_id, end)) = previous_end.take() {
                    rows.push(json!({"kind":"gap", "from_ms":from, "to_ms":ts_ms,
                        "duration_ms":from.zip(*ts_ms).and_then(|(a,b)| b.checked_sub(a)),
                        "previous_run_id":previous_run_id, "previous_end":end}));
                }
                model = Some(format!(
                    "{}/{}",
                    config.model.provider_module_id, config.model.model_id
                ));
                run = Some(run_id.clone());
                rows.push(json!({"kind":"run", "run_id":run_id, "model":model,
                    "variant":config.variant, "send_id":send_id, "started_ms":ts_ms}));
                planned.clear();
            }
            WalRecord::RunFinished { reason, ts_ms, .. } => {
                let end = format!("finished:{}", name(reason)?);
                rows.push(json!({"kind":"run_end", "run_id":run, "at_ms":ts_ms, "end":end}));
                previous_end = Some((*ts_ms, run.clone(), end));
            }
            WalRecord::RunPaused {
                run_id,
                reason,
                ts_ms,
                ..
            } => {
                let end = format!("paused:{}", name(reason)?);
                rows.push(json!({"kind":"run_end", "run_id":run_id, "at_ms":ts_ms, "end":end}));
                previous_end = Some((*ts_ms, Some(run_id.clone()), end));
            }
            WalRecord::ModelAttemptFinished {
                step_id,
                attempt,
                sent_at_ms,
                ts_ms,
                content_progress,
                http_status,
                transport,
                outcome,
                ..
            } => {
                rows.push(
                    json!({"kind":"model_request", "step_id":step_id, "attempt":attempt,
                    "model":model, "sent_at_ms":sent_at_ms,
                    "first_content_at_ms":content_progress.first_content_at_ms,
                    "ended_at_ms":ts_ms, "outcome":outcome, "http_status":http_status,
                    "transport":transport}),
                );
            }
            WalRecord::ModelStepFinished {
                step_id,
                finish_reason,
                written_at_ms,
                usage,
                assistant_message,
                ..
            } => {
                rows.push(json!({"kind":"model_step", "step_id":step_id,
                    "finish_reason":finish_reason, "written_at_ms":written_at_ms,
                    "input_tokens":usage.input_tokens, "cached_input_tokens":usage.cached_input_tokens,
                    "output_tokens":usage.output_tokens}));
                for block in &assistant_message.content {
                    if let ContentBlock::ToolCall(call) = block {
                        planned.insert(
                            call.tool_call_id.clone(),
                            (call.tool_name.clone(), call.input.clone()),
                        );
                    }
                }
                if transcript {
                    let bytes: usize = assistant_message
                        .content
                        .iter()
                        .map(|block| match block {
                            ContentBlock::Reasoning { text, .. } => text.len(),
                            ContentBlock::RedactedReasoning { data, .. } => data.len(),
                            _ => 0,
                        })
                        .sum();
                    rows.push(json!({"kind":"reasoning", "step_id":step_id,
                        "at_ms":written_at_ms, "bytes":bytes}));
                }
            }
            WalRecord::ToolBatchStarted {
                batch_id, step_id, ..
            } => {
                batches.insert(batch_id.clone(), *step_id);
            }
            WalRecord::ToolDispatchIntent {
                batch_id,
                tool_call_id,
                tool_name,
                module_id,
                dispatched_at_ms,
                call_key,
                args,
                ..
            } => {
                let tool_name = tool_name
                    .as_deref()
                    .or_else(|| planned.get(tool_call_id).map(|p| p.0.as_str()));
                let key = call_key
                    .clone()
                    .or_else(|| lineage.map(|id| format!("{id}:{}", frame.seq)));
                let mut row = json!({"kind":"tool_call", "step_id":batches.get(batch_id),
                    "call_key":key, "tool_call_id":tool_call_id, "tool_name":tool_name,
                    "module_id":module_id, "dispatched":true, "dispatched_at_ms":dispatched_at_ms});
                if tool_name.is_some_and(|name| name == "bash" || name.ends_with(".bash")) {
                    bash_fields(&mut row, args);
                }
                if transcript {
                    row["arguments"] = args.as_ref().clone();
                }
                calls.insert((batch_id.clone(), tool_call_id.clone()), rows.len());
                rows.push(row);
            }
            WalRecord::ToolNoDispatch {
                batch_id,
                tool_call_id,
                reason,
                dispatched_at_ms,
                ..
            } => {
                let plan = planned.get(tool_call_id);
                let mut row = json!({"kind":"tool_call", "step_id":batches.get(batch_id),
                    "call_key":lineage.map(|id| format!("{id}:{}", frame.seq)),
                    "tool_call_id":tool_call_id, "tool_name":plan.map(|p| &p.0),
                    "dispatched":false, "dispatched_at_ms":dispatched_at_ms,
                    "outcome":serde_json::to_value(reason).map_err(|_| "cannot encode no-dispatch reason")?["kind"]});
                if let Some((name, args)) = plan {
                    if transcript {
                        row["arguments"] = args.clone();
                    }
                    if name == "bash" || name.ends_with(".bash") {
                        bash_fields(&mut row, args);
                    }
                }
                rows.push(row);
            }
            WalRecord::ToolResult {
                batch_id,
                result,
                finished_at_ms,
                outcome,
                ..
            } => {
                if let Some(index) = calls.remove(&(batch_id.clone(), result.tool_call_id.clone()))
                {
                    let row = &mut rows[index];
                    let text = output_text(&result.output);
                    row["result_at_ms"] = json!(finished_at_ms);
                    row["is_error"] = json!(result.is_error);
                    row["result_bytes"] = json!(text.len());
                    row["outcome"] = json!(outcome);
                    if transcript {
                        row["result"] = json!(result);
                    }
                    if row.get("command").is_some() {
                        if let Some(code) = exit_code(&text) {
                            row["exit_code"] = json!(code);
                        }
                    }
                }
            }
            _ => {}
        }
        sequences.extend(std::iter::repeat_n(frame.seq, rows.len() - before));
    }
    let mut ordered: Vec<_> = sequences.into_iter().zip(rows).collect();
    if transcript {
        ordered.extend(transcript_rows(frames)?);
        ordered.sort_by_key(|(seq, _)| *seq);
    }
    Ok(ordered.into_iter().map(|(_, row)| row).collect())
}

fn content(blocks: &[ContentBlock]) -> Vec<Value> {
    blocks.iter().filter_map(|block| match block {
        ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } =>
            Some(json!({"type":"text", "text":text})),
        ContentBlock::ToolCall(call) => Some(json!({"type":"tool_call",
            "tool_call_id":call.tool_call_id, "tool_name":call.tool_name, "input":call.input})),
        ContentBlock::ToolResult(result) => Some(json!({"type":"tool_result",
            "tool_call_id":result.tool_call_id, "output":result.output, "is_error":result.is_error})),
        _ => None,
    }).collect()
}

fn message(
    ordinal: Option<u64>,
    at_ms: Option<u64>,
    role: Value,
    blocks: &[ContentBlock],
    source: &str,
) -> Value {
    json!({"kind":"message", "ordinal":ordinal, "mid":ordinal.map(|n| format!("m{n}")),
        "at_ms":at_ms, "role":role, "content":content(blocks), "source":source})
}

fn transcript_rows(frames: &[FramedRecord]) -> Result<Vec<(u64, Value)>, String> {
    use broca_core::placement::{AppendKind, Placement, PlacementAction};
    use broca_protocol::caller::ControlUnit;
    let readable_len = broca_core::read_session_page(
        &broca_core::StorePageSnapshot::empty(),
        frames,
        Some(0),
        Some(1),
    )
    .map_err(|_| "cannot derive session.read transcript coordinates")?
    .prompt_len;
    let times = broca_core::message_times::written_times(
        frames.iter().map(|f| &f.record),
        Placement::default(),
    )?;
    let mut placement = Placement::default();
    let mut held = Vec::new();
    let mut queued = HashMap::new();
    let mut rows = Vec::new();
    // Placement owns ordinals, including held steers and queued admissions. Keep
    // their source sequences separately so export order remains the WAL's order.
    for frame in frames {
        match &frame.record {
            WalRecord::Queued {
                submission_id,
                request,
                written_at_ms,
                ..
            } => {
                queued.insert(
                    submission_id.clone(),
                    (frame.seq, request.input.clone(), *written_at_ms),
                );
            }
            WalRecord::Steered {
                input,
                written_at_ms,
                ..
            }
            | WalRecord::SendDelivered {
                input: Some(input),
                written_at_ms,
                ..
            } => {
                held.push((frame.seq, input.clone(), *written_at_ms));
            }
            _ => {}
        }
        for action in placement.observe(&frame.record)? {
            match action {
                PlacementAction::Append {
                    first,
                    count,
                    kind: AppendKind::Input,
                } => {
                    if let WalRecord::RunStarted {
                        input,
                        submission_id,
                        ..
                    } = &frame.record
                    {
                        let queued_source = submission_id.as_ref().and_then(|id| queued.remove(id));
                        let seq = queued_source.as_ref().map_or(frame.seq, |source| source.0);
                        for (index, input) in input.iter().take(count as usize).enumerate() {
                            let ordinal = first + index as u64;
                            rows.push((
                                seq,
                                message(
                                    Some(ordinal),
                                    times.get(&ordinal).copied(),
                                    json!(input.role),
                                    &input.content,
                                    if queued_source.is_some() {
                                        "queued"
                                    } else {
                                        "user"
                                    },
                                ),
                            ));
                        }
                    }
                }
                PlacementAction::PlaceHeld { first, count } => {
                    let inputs = std::mem::take(&mut held);
                    for (index, (seq, input)) in inputs
                        .iter()
                        .flat_map(|(seq, input, _)| input.iter().map(move |input| (*seq, input)))
                        .take(count as usize)
                        .enumerate()
                    {
                        let ordinal = first + index as u64;
                        rows.push((
                            seq,
                            message(
                                Some(ordinal),
                                times.get(&ordinal).copied(),
                                json!(input.role),
                                &input.content,
                                "steer",
                            ),
                        ));
                    }
                }
                PlacementAction::Truncate { to } => rows
                    .retain(|(_, row)| row["ordinal"].as_u64().is_none_or(|ordinal| ordinal < to)),
                _ => {}
            }
        }
    }
    for (seq, inputs, at_ms) in held {
        for input in inputs.iter() {
            rows.push((
                seq,
                message(None, at_ms, json!(input.role), &input.content, "steer"),
            ));
        }
    }
    // A queued submission that has not started has no session.read coordinate yet.
    // Null is intentional: reserving an ordinal here would mislabel later steers.
    for (_, (seq, inputs, at_ms)) in queued {
        for input in inputs.iter() {
            rows.push((
                seq,
                message(None, at_ms, json!(input.role), &input.content, "queued"),
            ));
        }
    }
    let mut state = broca_core::ControlProjectionState::default();
    let source_times: HashMap<_, _> = frames
        .iter()
        .filter_map(|frame| {
            let time = match &frame.record {
                WalRecord::ModelStepFinished { written_at_ms, .. } => *written_at_ms,
                WalRecord::ToolResult { finished_at_ms, .. } => *finished_at_ms,
                WalRecord::ToolNoDispatch {
                    dispatched_at_ms, ..
                } => *dispatched_at_ms,
                WalRecord::ApprovalRecorded { closed_at_ms, .. } => *closed_at_ms,
                _ => None,
            };
            time.map(|time| (frame.seq, time))
        })
        .collect();
    for unit in broca_core::project_control_with_mids(frames, &mut state)? {
        // A live control stream reserves coordinates before session.read exposes
        // an open tool turn. Keep that reservation out of a transcript export.
        let ordinal = unit
            .message_ordinal
            .filter(|ordinal| *ordinal < readable_len);
        let at_ms = ordinal
            .and_then(|n| times.get(&n).copied())
            .or_else(|| source_times.get(&unit.cursor.wal_seq).copied());
        match unit.unit {
            ControlUnit::AssistantMessage { message: assistant } => rows.push((
                unit.cursor.wal_seq,
                message(
                    ordinal,
                    at_ms,
                    json!("assistant"),
                    &assistant.content,
                    "assistant",
                ),
            )),
            ControlUnit::ToolResult { result } => rows.push((
                unit.cursor.wal_seq,
                message(
                    ordinal,
                    at_ms,
                    json!("tool"),
                    &[ContentBlock::ToolResult(result)],
                    "tool",
                ),
            )),
            _ => {}
        }
    }
    Ok(rows)
}
