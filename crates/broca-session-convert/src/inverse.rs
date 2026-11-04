//! Converts new records back into today's record JSON. Comparing the result with the original
//! log proves a conversion lost nothing: every field of every old record was placed somewhere it
//! can be read back from.
//!
//! The forward conversion writes each old record as one ledger *anchor* (`RunStart`, `StepEnd`,
//! `ToolOutcome`, an `Event`, ...), with any transcript records it split off written just before
//! the anchor, except a run start's input entries, which follow it. So the inverse buffers
//! transcript records until the next ledger record claims them, and refuses a log where a
//! transcript record is left unclaimed or claimed by the wrong anchor.

use std::collections::{BTreeMap, HashMap};

use broca_session_store::codec::Digest;
use broca_session_store::record::{
    Assistant, Attempt, CallOutcome, Event, Input, PartKind, PartRef, Placement, QueuedSend,
    Record, ResultAppend, RunStart, SessionRefresh, StepEnd, ToolIntent, ToolOutcome, ToolReply,
};
use serde::Serialize;
use serde_json::{Map, Value};

fn json(value: impl Serialize) -> Value {
    serde_json::to_value(value).expect("record values serialize")
}

fn set<T: Serialize>(object: &mut Map<String, Value>, key: &str, value: Option<T>) {
    if let Some(value) = value {
        object.insert(key.into(), json(value));
    }
}

/// An owned payload's members, as the object a record was rebuilt on.
fn members(payload: Option<&Value>) -> Result<Map<String, Value>, String> {
    match payload {
        None => Ok(Map::new()),
        Some(Value::Object(object)) => Ok(object.clone()),
        Some(_) => Err("a carried payload is not an object".into()),
    }
}

fn hex(digest: &Digest) -> String {
    digest.0.iter().map(|b| format!("{b:02x}")).collect()
}

fn part_value(parts: &BTreeMap<Digest, Vec<u8>>, part: &PartRef) -> Result<Value, String> {
    let bytes = parts
        .get(&part.digest)
        .ok_or_else(|| format!("setup part {} is missing", hex(&part.digest)))?;
    serde_json::from_slice(bytes).map_err(|e| format!("setup part {}: {e}", hex(&part.digest)))
}

/// Puts a plan manifest's setup part back into the manifest object.
fn fill_manifest(manifest: &mut Map<String, Value>, kind: PartKind, value: Value) {
    match kind {
        PartKind::Plan => {
            manifest.insert("plan".into(), value);
        }
        PartKind::ProviderAnswer => {
            if let Some(Value::Array(fetches)) = manifest.get_mut("fetches") {
                fetches.push(value);
            } else {
                manifest.insert("fetches".into(), Value::Array(vec![value]));
            }
        }
        PartKind::Tools => {
            manifest.insert("tools".into(), value);
        }
        PartKind::SystemText => {
            manifest.insert("system_text".into(), value);
        }
        PartKind::Declarations => {
            manifest.insert("declarations".into(), value);
        }
        PartKind::ResponseFormat | PartKind::GlobalSystem => {}
    }
}

/// A manifest always lists its provider answers, even when there are none.
fn finish_manifest(mut manifest: Map<String, Value>) -> Value {
    manifest
        .entry("fetches")
        .or_insert_with(|| Value::Array(Vec::new()));
    Value::Object(manifest)
}

fn tags(entries: impl Iterator<Item = Option<Value>>) -> Option<Value> {
    let entries: Vec<Option<Value>> = entries.collect();
    entries.iter().any(Option::is_some).then(|| {
        Value::Array(
            entries
                .into_iter()
                .map(|t| t.unwrap_or(Value::Null))
                .collect(),
        )
    })
}

fn run_started(
    start: &RunStart,
    inputs: &[&Input],
    parts: &BTreeMap<Digest, Vec<u8>>,
) -> Result<Value, String> {
    let choices = members(start.run_choices.as_ref())?;
    let mut object = members(choices.get("record"))?;
    let mut config = members(choices.get("config"))?;
    let mut manifest = choices
        .get("manifest")
        .map(|m| members(Some(m)))
        .transpose()?;

    object.insert("type".into(), json("run_started"));
    object.insert("run_id".into(), json(&start.run_id));
    set(&mut object, "session_class", start.session_class.as_ref());
    set(&mut object, "submission_id", start.submission_id.as_ref());
    set(&mut object, "send_id", start.send_id.as_ref());
    set(
        &mut object,
        "request_identity_digest",
        start.request_identity_digest.as_ref(),
    );
    set(&mut object, "ts_ms", start.ts_ms);
    set(&mut object, "written_at_ms", start.written_at_ms);
    set(&mut object, "retention", start.retention_secs);
    set(&mut object, "title", start.title.as_ref());
    set(
        &mut object,
        "compaction_plan_version",
        start.compaction_plan_version,
    );
    set(&mut object, "origin", start.origin.as_ref());
    set(&mut object, "principal", start.principal.as_ref());
    set(&mut object, "scope", start.scope.as_ref());
    set(&mut object, "hooks", start.hooks.as_ref());
    set(&mut object, "keep_warm", start.keep_warm.as_ref());
    set(&mut object, "on_restart", start.on_restart.as_ref());

    config.insert("model".into(), json(&start.model));
    config.insert("renderer_version".into(), json(start.renderer_version));
    set(&mut config, "context_limit", start.context_limit);
    set(&mut config, "service_tier", start.service_tier.as_ref());
    set(
        &mut config,
        "route_decision_id",
        start.route_decision_id.as_ref(),
    );
    set(
        &mut config,
        "frozen_render_config",
        start.render_config.as_ref(),
    );
    set(&mut config, "auth_selection", start.auth_selection.as_ref());

    // The config's parts come first (its tools, then its response format, which every run
    // config has, then its global system text), then the manifest's. So a tools part before the
    // response format is the config's, and one after it is the manifest's.
    let mut past_config = false;
    for part in &start.setup {
        let value = part_value(parts, part)?;
        match part.kind {
            PartKind::Tools if !past_config => {
                config.insert("tools".into(), value);
            }
            PartKind::ResponseFormat => {
                config.insert("response_format".into(), value);
                past_config = true;
            }
            PartKind::GlobalSystem => {
                config.insert("global_system".into(), value);
            }
            kind => fill_manifest(manifest.get_or_insert_with(Map::new), kind, value),
        }
    }
    object.insert("config".into(), Value::Object(config));
    if let Some(manifest) = manifest {
        object.insert("manifest".into(), finish_manifest(manifest));
    }

    object.insert(
        "input".into(),
        Value::Array(inputs.iter().map(|i| json(&i.message)).collect()),
    );
    set(
        &mut object,
        "tags",
        tags(inputs.iter().map(|i| i.tags_v1.clone())),
    );
    Ok(Value::Object(object))
}

fn model_step_finished(assistant: &Assistant, end: &StepEnd) -> Result<Value, String> {
    let mut object = members(end.fallback.as_ref())?;
    object.insert("type".into(), json("model_step_finished"));
    object.insert("step_id".into(), json(end.step_id));
    let mut message = Map::new();
    message.insert("message_id".into(), json(&assistant.provider_message_id));
    message.insert("content".into(), json(&assistant.message.content));
    object.insert("assistant_message".into(), Value::Object(message));
    set(&mut object, "usage", assistant.usage.as_ref());
    set(&mut object, "written_at_ms", assistant.written_at_ms);
    set(&mut object, "tags", assistant.tags_v1.as_ref());
    set(&mut object, "finish_reason", end.finish_reason.as_ref());
    object.insert("retries_used".into(), json(end.retries_used));
    set(
        &mut object,
        "service_tier_used",
        end.service_tier_used.as_ref(),
    );
    set(&mut object, "speed_used", end.speed_used.as_ref());
    set(&mut object, "hooks", end.hooks.as_ref());
    Ok(Value::Object(object))
}

fn model_attempt(attempt: &Attempt) -> Result<Value, String> {
    let mut object = members(attempt.telemetry.as_ref())?;
    object.extend(members(attempt.billing.as_ref())?);
    object.insert("type".into(), json("model_attempt_finished"));
    object.insert("step_id".into(), json(attempt.step_id));
    object.insert("attempt".into(), json(attempt.attempt));
    set(&mut object, "attempt_id", attempt.attempt_id.as_ref());
    object.insert("outcome".into(), json(&attempt.outcome));
    set(&mut object, "http_status", attempt.http_status);
    object.insert("request_body_len".into(), json(attempt.request_body_len));
    object.insert(
        "request_body_sha256".into(),
        json(hex(&attempt.request_body_sha256)),
    );
    object.insert("response_body_len".into(), json(attempt.response_body_len));
    object.insert(
        "response_body_sha256".into(),
        json(hex(&attempt.response_body_sha256)),
    );
    object.insert("ts_ms".into(), json(attempt.ts_ms));
    set(&mut object, "sent_at_ms", attempt.sent_at_ms);
    set(
        &mut object,
        "first_content_at_ms",
        attempt.first_content_at_ms,
    );
    set(
        &mut object,
        "last_content_at_ms",
        attempt.last_content_at_ms,
    );
    set(&mut object, "transport", attempt.transport.as_ref());
    set(
        &mut object,
        "served_account_ref",
        attempt.served_account_ref.as_ref(),
    );
    Ok(Value::Object(object))
}

fn tool_intent(intent: &ToolIntent, calls: &HashMap<String, Value>) -> Value {
    let mut object = Map::new();
    object.insert("type".into(), json("tool_dispatch_intent"));
    object.insert("batch_id".into(), json(&intent.batch_id));
    object.insert("tool_call_id".into(), json(&intent.tool_call_id));
    // Arguments equal to the call's own input were not stored a second time.
    let args = intent
        .args
        .clone()
        .or_else(|| calls.get(&intent.tool_call_id.0).cloned())
        .unwrap_or(Value::Null);
    object.insert("args".into(), args);
    set(&mut object, "tool_name", intent.tool_name.as_ref());
    set(&mut object, "module_id", intent.module_id.as_ref());
    set(&mut object, "call_key", intent.call_key.as_ref());
    set(
        &mut object,
        "approval_digest",
        intent.approval_digest.as_ref(),
    );
    set(&mut object, "dispatched_at_ms", intent.dispatched_at_ms);
    set(&mut object, "schema_pin", intent.schema_pin.as_ref());
    set(&mut object, "hooks", intent.hooks.as_ref());
    Value::Object(object)
}

fn tool_terminal(reply: &ToolReply, outcome: &ToolOutcome) -> Result<Value, String> {
    if reply.result.tool_call_id != outcome.tool_call_id {
        return Err("a tool outcome claims another call's reply".into());
    }
    if outcome.outcome == CallOutcome::NotDispatched {
        let mut object = Map::new();
        object.insert("type".into(), json("tool_no_dispatch"));
        set(&mut object, "dispatched_at_ms", outcome.finished_at_ms);
        object.insert("batch_id".into(), json(&outcome.batch_id));
        object.insert("tool_call_id".into(), json(&outcome.tool_call_id));
        set(&mut object, "reason", outcome.detail.as_ref());
        set(&mut object, "hooks", outcome.hooks.as_ref());
        set(&mut object, "tags", reply.tags_v1.as_ref());
        return Ok(Value::Object(object));
    }
    let mut object = members(outcome.detail.as_ref())?;
    object.insert("type".into(), json("tool_result"));
    object.insert("result".into(), json(&reply.result));
    let name = match outcome.outcome {
        CallOutcome::Completed => "completed",
        CallOutcome::Unknown => "unknown",
        CallOutcome::Interrupted => "interrupted",
        CallOutcome::MaxSteps => "max_steps",
        CallOutcome::NotDispatched => unreachable!("handled above"),
    };
    object.insert("outcome".into(), json(name));
    set(&mut object, "tags", reply.tags_v1.as_ref());
    object.insert("batch_id".into(), json(&outcome.batch_id));
    set(&mut object, "finished_at_ms", outcome.finished_at_ms);
    set(&mut object, "reason", outcome.reason.as_ref());
    set(&mut object, "hooks", outcome.hooks.as_ref());
    Ok(Value::Object(object))
}

fn queued(send: &QueuedSend, parts: &BTreeMap<Digest, Vec<u8>>) -> Result<Value, String> {
    let mut object = members(send.receipt.as_ref())?;
    object.insert("type".into(), json("queued"));
    object.insert("submission_id".into(), json(&send.submission_id));
    set(&mut object, "send_id", send.send_id.as_ref());
    set(&mut object, "written_at_ms", send.written_at_ms);
    set(&mut object, "ts_ms", send.ts_ms);
    let mut request = members(send.request.as_ref())?;
    request.insert("input".into(), json(&send.input));
    for part in &send.setup {
        if part.kind == PartKind::Tools {
            request.insert("tools".into(), part_value(parts, part)?);
        }
    }
    object.insert("request".into(), Value::Object(request));
    Ok(Value::Object(object))
}

fn session_refresh(
    refresh: &SessionRefresh,
    parts: &BTreeMap<Digest, Vec<u8>>,
) -> Result<Value, String> {
    let mut object = Map::new();
    object.insert("type".into(), json("session_refresh_accepted"));
    object.insert("generation".into(), json(refresh.generation));
    object.insert("policy".into(), json(&refresh.policy));
    object.insert("run_in_progress".into(), json(refresh.run_in_progress));
    object.insert(
        "applies_from_next_run".into(),
        json(refresh.applies_from_next_run),
    );
    if refresh.manifest.is_some() || !refresh.setup.is_empty() {
        let mut manifest = members(refresh.manifest.as_ref())?;
        for part in &refresh.setup {
            fill_manifest(&mut manifest, part.kind, part_value(parts, part)?);
        }
        object.insert("manifest".into(), finish_manifest(manifest));
    }
    Ok(Value::Object(object))
}

/// An event's old record, with whatever transcript records were split off it put back.
fn event(event: &Event, claimed: &[&Record]) -> Result<Value, String> {
    let mut object = members(event.body.as_ref())?;
    object.insert("type".into(), json(&event.name));
    set(&mut object, "run_id", event.run_id.as_ref());
    let inputs: Vec<&Input> = claimed
        .iter()
        .filter_map(|r| match r {
            Record::Input(input) => Some(input),
            _ => None,
        })
        .collect();
    let replies: Vec<&ToolReply> = claimed
        .iter()
        .filter_map(|r| match r {
            Record::ToolReply(reply) => Some(reply),
            _ => None,
        })
        .collect();
    let appends: Vec<&ResultAppend> = claimed
        .iter()
        .filter_map(|r| match r {
            Record::ResultAppend(append) => Some(append),
            _ => None,
        })
        .collect();
    let expect = |ok: bool| {
        ok.then_some(())
            .ok_or_else(|| format!("{} claimed records it doesn't split into", event.name))
    };
    match event.name.as_str() {
        "steered" => {
            expect(replies.is_empty() && appends.is_empty())?;
            expect(inputs.iter().all(|i| i.placement == Placement::Steer))?;
            object.insert(
                "input".into(),
                Value::Array(inputs.iter().map(|i| json(&i.message)).collect()),
            );
            if let Some(first) = inputs.first() {
                set(&mut object, "written_at_ms", first.written_at_ms);
            }
            set(
                &mut object,
                "tags",
                tags(inputs.iter().map(|i| i.tags_v1.clone())),
            );
        }
        "budget_fired" => {
            expect(replies.is_empty() && appends.is_empty() && inputs.len() <= 1)?;
            if let Some(input) = inputs.first() {
                let text = match input.message.content.as_slice() {
                    [broca_protocol::content::ContentBlock::Text { text }] => text.clone(),
                    _ => return Err("budget nudge is not one text block".into()),
                };
                object.insert("caller_text".into(), json(text));
                set(&mut object, "tags", input.tags_v1.as_ref());
            }
        }
        "send_delivered" => {
            expect(replies.is_empty() && appends.len() <= 1)?;
            if !inputs.is_empty() {
                object.insert(
                    "input".into(),
                    Value::Array(inputs.iter().map(|i| json(&i.message)).collect()),
                );
                set(
                    &mut object,
                    "tags",
                    tags(inputs.iter().map(|i| i.tags_v1.clone())),
                );
            }
            if let Some(append) = appends.first() {
                object.insert("appended_text".into(), json(&append.text));
                object.insert("tool_call_id".into(), json(&append.tool_call_id));
            }
        }
        "recovery_call_closed" => {
            expect(inputs.is_empty() && appends.is_empty() && replies.len() == 1)?;
            let reply = replies[0];
            object.insert("result".into(), json(&reply.result));
            object.insert("run_id".into(), json(&reply.run_id));
            set(&mut object, "batch_id", reply.batch_id.as_ref());
        }
        "approval_recorded" => {
            expect(inputs.is_empty() && appends.is_empty() && replies.len() <= 1)?;
            if let Some(reply) = replies.first() {
                object.insert("closing_result".into(), json(&reply.result));
                set(&mut object, "closed_at_ms", reply.finished_at_ms);
                set(&mut object, "tags", reply.tags_v1.as_ref());
            }
        }
        _ => expect(claimed.is_empty())?,
    }
    Ok(Value::Object(object))
}

/// Today's record JSON for each old record the new records were converted from, in order.
pub fn invert(records: &[Record], parts: &BTreeMap<Digest, Vec<u8>>) -> Result<Vec<Value>, String> {
    let mut out = Vec::new();
    let mut pending: Vec<&Record> = Vec::new();
    let mut calls: HashMap<String, Value> = HashMap::new();
    let unclaimed = |pending: &[&Record], at: &str| {
        if pending.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "{} transcript record(s) unclaimed before {at}",
                pending.len()
            ))
        }
    };
    let mut index = 0;
    while index < records.len() {
        let record = &records[index];
        index += 1;
        match record {
            Record::SetupStub(_) => {}
            Record::Input(input) if input.placement == Placement::RunInput => {
                return Err("a run's input entry is not right after its run start".into());
            }
            Record::Input(_)
            | Record::Assistant(_)
            | Record::ToolReply(_)
            | Record::ResultAppend(_) => {
                if let Record::Assistant(assistant) = record {
                    calls.clear();
                    for block in &assistant.message.content {
                        if let broca_protocol::content::ContentBlock::ToolCall(call) = block {
                            calls.insert(call.tool_call_id.0.clone(), call.input.clone());
                        }
                    }
                }
                pending.push(record);
            }
            Record::Compaction(compaction) => {
                unclaimed(&pending, "a compaction boundary")?;
                let mut object = members(Some(&compaction.body))?;
                let name = match compaction.version {
                    1 => "compaction_message",
                    2 => "compaction_marker",
                    other => return Err(format!("compaction version {other}")),
                };
                object.insert("type".into(), json(name));
                out.push(Value::Object(object));
            }
            Record::RunStart(start) => {
                unclaimed(&pending, "a run start")?;
                let mut inputs = Vec::new();
                while let Some(Record::Input(input)) = records.get(index) {
                    if input.placement != Placement::RunInput || input.run_id != start.run_id {
                        break;
                    }
                    inputs.push(input);
                    index += 1;
                }
                calls.clear();
                out.push(run_started(start, &inputs, parts)?);
            }
            Record::StepEnd(end) => {
                let [Record::Assistant(assistant)] = pending.as_slice() else {
                    return Err("a step end without exactly its assistant message".into());
                };
                if assistant.step_id != end.step_id {
                    return Err("a step end claims another step's message".into());
                }
                out.push(model_step_finished(assistant, end)?);
                pending.clear();
            }
            Record::ToolOutcome(outcome) => {
                let [Record::ToolReply(reply)] = pending.as_slice() else {
                    return Err("a tool outcome without exactly its reply".into());
                };
                out.push(tool_terminal(reply, outcome)?);
                pending.clear();
            }
            Record::Event(e) => {
                out.push(event(e, &pending)?);
                pending.clear();
            }
            other => {
                unclaimed(&pending, other.name())?;
                out.push(match other {
                    Record::RunEnd(end) => {
                        let mut object = Map::new();
                        object.insert("type".into(), json("run_finished"));
                        object.insert("reason".into(), json(end.reason));
                        set(&mut object, "usage", end.usage.as_ref());
                        set(&mut object, "ts_ms", end.ts_ms);
                        object.insert("retries_used".into(), json(end.retries_used));
                        set(
                            &mut object,
                            "interrupt_reason",
                            end.interrupt_reason.as_ref(),
                        );
                        set(&mut object, "error", end.error.as_ref());
                        Value::Object(object)
                    }
                    Record::StepStart(start) => serde_json::json!({
                        "type": "step_started",
                        "step_id": start.step_id,
                    }),
                    Record::Attempt(attempt) => model_attempt(attempt)?,
                    Record::BatchStart(batch) => serde_json::json!({
                        "type": "tool_batch_started",
                        "batch_id": batch.batch_id,
                        "step_id": batch.step_id,
                        "planned_calls": batch.planned,
                    }),
                    Record::BatchEnd(batch) => serde_json::json!({
                        "type": "tool_batch_finished",
                        "batch_id": batch.batch_id,
                    }),
                    Record::ToolIntent(intent) => tool_intent(intent, &calls),
                    Record::QueuedSend(send) => queued(send, parts)?,
                    Record::SessionRefresh(refresh) => session_refresh(refresh, parts)?,
                    Record::StructuredResult(result) => serde_json::json!({
                        "type": "structured_output_result",
                        "run_id": result.run_id,
                        "result": result.result,
                    }),
                    _ => unreachable!("every other kind is matched above"),
                });
            }
        }
    }
    unclaimed(&pending, "the end of the log")?;
    Ok(out)
}

/// Today's records as the inverse should rebuild them. `turn_finished` and `visibility_late`
/// are left out, since the new format retires them. A run start's `session` and `project_id`
/// are removed: every run repeats them, and the new format stores them once per session, in the
/// session's head object. Null members are dropped.
pub fn comparable(old: &[broca_wal::FramedRecord]) -> Vec<Value> {
    old.iter()
        .filter_map(|framed| {
            let mut value = json(&framed.record);
            let object = value.as_object_mut()?;
            match object.get("type").and_then(Value::as_str) {
                Some("turn_finished" | "visibility_late") => return None,
                Some("run_started") => {
                    object.remove("session");
                    object.remove("project_id");
                }
                _ => {}
            }
            Some(without_nulls(&value))
        })
        .collect()
}

/// Drops null members at every level, since an old record reads a null member and an absent
/// one the same way, and the forward conversion doesn't keep the difference.
pub fn without_nulls(value: &Value) -> Value {
    match value {
        Value::Object(object) => Value::Object(
            object
                .iter()
                .filter(|(_, v)| !v.is_null())
                .map(|(k, v)| (k.clone(), without_nulls(v)))
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(without_nulls).collect()),
        other => other.clone(),
    }
}

/// The path of the first place two values differ, naming only keys and indexes, never values,
/// so a report can be shown without exposing session content.
pub fn first_difference(a: &Value, b: &Value, path: &str) -> Option<String> {
    match (a, b) {
        (Value::Object(x), Value::Object(y)) => {
            let mut keys: Vec<&String> = x.keys().chain(y.keys()).collect();
            keys.sort();
            keys.dedup();
            keys.into_iter()
                .find_map(|key| match (x.get(key), y.get(key)) {
                    (Some(p), Some(q)) => first_difference(p, q, &format!("{path}.{key}")),
                    (Some(_), None) => Some(format!("{path}.{key} (only in the original)")),
                    (None, Some(_)) => Some(format!("{path}.{key} (only in the rebuilt record)")),
                    (None, None) => None,
                })
        }
        (Value::Array(x), Value::Array(y)) if x.len() == y.len() => x
            .iter()
            .zip(y)
            .enumerate()
            .find_map(|(i, (p, q))| first_difference(p, q, &format!("{path}[{i}]"))),
        (Value::Array(x), Value::Array(y)) => {
            Some(format!("{path} (length {} against {})", x.len(), y.len()))
        }
        _ if a == b => None,
        _ => Some(format!("{path} (value)")),
    }
}

#[cfg(test)]
mod tests;
