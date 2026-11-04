//! Converts one of today's session logs into records of the new session storage format.
//!
//! The mapping works on each old record's JSON form, the shape it is stored in today: typed
//! fields are taken out by name, and whatever remains of an owned payload travels as JSON. Old
//! records the new format has no kind for yet are counted as *uncovered*, with their bytes, so a
//! measurement says exactly what it left out. Records the new format retires (the repeated
//! lineage frame, `turn_finished`, `visibility_late`) are counted as *dropped*.

use std::collections::{BTreeMap, HashMap};

use broca_protocol::content::{ContentBlock, FinishReason, Message, Role, ToolResult};
use broca_protocol::ids::{MessageId, ToolCallId};
use broca_protocol::request::ModelHandle;
use broca_protocol::usage::Usage;
use broca_session_store::codec::{Digest, Message as _};
use broca_session_store::record::{
    Assistant, Attempt, BatchEnd, BatchStart, CallOutcome, Input, PartKind, PartRef, Placement,
    Record, ResultAppend, RunEnd, RunStart, SetupStub, StepEnd, StepStart, ToolIntent, ToolOutcome,
    ToolReply,
};
use broca_wal::{FramedRecord, WalRecord};
use serde_json::{Map, Value};
use sha2::{Digest as _, Sha256};

pub mod measure;

/// Old records of one type that were left out, and their stored JSON size.
#[derive(Debug, Default, Clone, Copy, serde::Serialize)]
pub struct Tally {
    pub count: u64,
    pub json_bytes: u64,
}

impl Tally {
    fn add(&mut self, bytes: usize) {
        self.count += 1;
        self.json_bytes += bytes as u64;
    }
}

/// One converted log.
#[derive(Debug, Default)]
pub struct Converted {
    pub records: Vec<Record>,
    /// Every setup part this log's runs named, by digest, with its canonical bytes.
    pub parts: BTreeMap<Digest, Vec<u8>>,
    /// Bytes of setup parts as today's runs store them, summed over runs.
    pub part_bytes_per_run: u64,
    pub uncovered: BTreeMap<String, Tally>,
    pub dropped: BTreeMap<String, Tally>,
    /// The session class of the log's first run.
    pub session_class: Option<String>,
    pub runs: u64,
}

#[derive(Debug)]
pub struct ConvertError(pub String);

impl std::fmt::Display for ConvertError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

fn err(message: impl Into<String>) -> ConvertError {
    ConvertError(message.into())
}

/// Per-log state the mapping needs: which run and model records belong to, the setup now in
/// force, and the current step's tool call inputs, so a dispatch intent stores its arguments
/// only when they differ from the call's.
#[derive(Default)]
struct State {
    run_id: String,
    model: Option<ModelHandle>,
    setup: Vec<PartRef>,
    call_inputs: HashMap<String, Value>,
}

pub fn convert(old: &[FramedRecord]) -> Result<Converted, ConvertError> {
    let mut out = Converted::default();
    let mut state = State::default();
    for framed in old {
        convert_one(&framed.record, &mut state, &mut out)?;
    }
    Ok(out)
}

fn sha256(bytes: &[u8]) -> Digest {
    Digest(Sha256::digest(bytes).into())
}

fn canonical(value: &Value) -> Result<Vec<u8>, ConvertError> {
    serde_jcs::to_vec(value).map_err(|e| err(format!("canonical JSON: {e}")))
}

fn hex_digest(text: &str) -> Option<Digest> {
    if text.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, chunk) in text.as_bytes().chunks(2).enumerate() {
        let pair = std::str::from_utf8(chunk).ok()?;
        out[i] = u8::from_str_radix(pair, 16).ok()?;
    }
    Some(Digest(out))
}

fn take(object: &mut Map<String, Value>, key: &str) -> Option<Value> {
    object.remove(key).filter(|v| !v.is_null())
}

fn take_str(object: &mut Map<String, Value>, key: &str) -> Option<String> {
    match take(object, key)? {
        Value::String(s) => Some(s),
        other => Some(other.to_string()),
    }
}

fn take_u64(object: &mut Map<String, Value>, key: &str) -> Option<u64> {
    take(object, key).and_then(|v| v.as_u64())
}

fn take_as<T: serde::de::DeserializeOwned>(
    object: &mut Map<String, Value>,
    key: &str,
) -> Result<Option<T>, ConvertError> {
    take(object, key)
        .map(|v| serde_json::from_value(v).map_err(|e| err(format!("{key}: {e}"))))
        .transpose()
}

/// What's left of an object once its typed fields are taken: `None` when nothing is.
fn rest(object: Map<String, Value>) -> Option<Value> {
    (!object.is_empty()).then_some(Value::Object(object))
}

fn json_size(record: &WalRecord) -> usize {
    serde_json::to_vec(record).map(|v| v.len()).unwrap_or(0)
}

fn convert_one(
    record: &WalRecord,
    state: &mut State,
    out: &mut Converted,
) -> Result<(), ConvertError> {
    let value = serde_json::to_value(record).map_err(|e| err(format!("serialize: {e}")))?;
    let Value::Object(mut obj) = value else {
        return Err(err("record is not an object"));
    };
    let kind = take_str(&mut obj, "type").unwrap_or_default();
    let size = json_size(record);
    match kind.as_str() {
        "run_started" => run_started(obj, state, out)?,
        "model_step_finished" => model_step_finished(obj, state, out)?,
        "model_attempt_finished" => model_attempt(obj, state, out)?,
        "step_started" => {
            let step_id = take_u64(&mut obj, "step_id").unwrap_or_default();
            out.records.push(Record::StepStart(StepStart {
                run_id: state.run_id.clone(),
                step_id,
            }));
        }
        "tool_batch_started" => {
            let planned: Vec<ToolCallId> = take_as(&mut obj, "planned_calls")?.unwrap_or_default();
            out.records.push(Record::BatchStart(BatchStart {
                run_id: state.run_id.clone(),
                batch_id: take_str(&mut obj, "batch_id").unwrap_or_default(),
                step_id: take_u64(&mut obj, "step_id").unwrap_or_default(),
                planned,
            }));
        }
        "tool_batch_finished" => out.records.push(Record::BatchEnd(BatchEnd {
            run_id: state.run_id.clone(),
            batch_id: take_str(&mut obj, "batch_id").unwrap_or_default(),
        })),
        "tool_dispatch_intent" => tool_intent(obj, state, out)?,
        "tool_result" => tool_result(obj, state, out)?,
        "tool_no_dispatch" => tool_no_dispatch(record, state, out)?,
        "run_finished" => {
            let reason =
                take_as(&mut obj, "reason")?.ok_or_else(|| err("run_finished without reason"))?;
            out.records.push(Record::RunEnd(RunEnd {
                run_id: state.run_id.clone(),
                reason,
                usage: take_as::<Usage>(&mut obj, "usage")?,
                ts_ms: take_u64(&mut obj, "ts_ms"),
                retries_used: take_u64(&mut obj, "retries_used").unwrap_or_default() as u32,
                interrupt_reason: take_str(&mut obj, "interrupt_reason"),
                error: take(&mut obj, "error"),
            }));
        }
        "steered" => {
            let written = take_u64(&mut obj, "written_at_ms");
            let input: Vec<Message> = take_as(&mut obj, "input")?.unwrap_or_default();
            for message in input {
                out.records.push(Record::Input(Input {
                    run_id: state.run_id.clone(),
                    placement: Placement::Steer,
                    message,
                    written_at_ms: written,
                    submission_id: None,
                    notice: None,
                }));
            }
            // The receipt (send id, delivery, retention) belongs to the queue kinds, not yet
            // defined: count what remains of it.
            out.uncovered
                .entry("steered (receipt)".into())
                .or_default()
                .add(serde_json::to_vec(&Value::Object(obj)).map_or(0, |v| v.len()));
        }
        "send_delivered" => {
            let written = take_u64(&mut obj, "written_at_ms");
            let submission = take_str(&mut obj, "submission_id");
            let input: Vec<Message> = take_as(&mut obj, "input")?.unwrap_or_default();
            for message in input {
                out.records.push(Record::Input(Input {
                    run_id: state.run_id.clone(),
                    placement: Placement::Delivered,
                    message,
                    written_at_ms: written,
                    submission_id: submission.clone(),
                    notice: None,
                }));
            }
            if let (Some(text), Some(call)) = (
                take_str(&mut obj, "appended_text"),
                take_as::<ToolCallId>(&mut obj, "tool_call_id")?,
            ) {
                out.records.push(Record::ResultAppend(ResultAppend {
                    run_id: state.run_id.clone(),
                    tool_call_id: call,
                    text,
                    notice: None,
                    written_at_ms: written,
                }));
            }
        }
        "budget_fired" => {
            if let Some(text) = take_str(&mut obj, "caller_text") {
                out.records.push(Record::Input(Input {
                    run_id: state.run_id.clone(),
                    placement: Placement::BudgetNudge,
                    message: Message::new(Role::User, vec![ContentBlock::Text { text }]),
                    written_at_ms: None,
                    submission_id: None,
                    notice: None,
                }));
            }
            out.uncovered
                .entry("budget_fired (trigger)".into())
                .or_default()
                .add(serde_json::to_vec(&Value::Object(obj)).map_or(0, |v| v.len()));
        }
        "turn_finished" | "visibility_late" => out.dropped.entry(kind).or_default().add(size),
        _ => out.uncovered.entry(kind).or_default().add(size),
    }
    Ok(())
}

fn add_part(
    kind: PartKind,
    value: &Value,
    parts: &mut Vec<PartRef>,
    out: &mut Converted,
) -> Result<(), ConvertError> {
    let bytes = canonical(value)?;
    out.part_bytes_per_run += bytes.len() as u64;
    let digest = sha256(&bytes);
    out.parts.entry(digest).or_insert(bytes);
    let part = PartRef { kind, digest };
    if !parts.contains(&part) {
        parts.push(part);
    }
    Ok(())
}

fn run_started(
    mut obj: Map<String, Value>,
    state: &mut State,
    out: &mut Converted,
) -> Result<(), ConvertError> {
    out.runs += 1;
    let run_id = take_str(&mut obj, "run_id").unwrap_or_default();
    let session_class = take_str(&mut obj, "session_class");
    if out.session_class.is_none() {
        out.session_class = session_class.clone();
    }
    let mut config = match take(&mut obj, "config") {
        Some(Value::Object(config)) => config,
        _ => return Err(err("run_started without config")),
    };
    let model: ModelHandle =
        take_as(&mut config, "model")?.ok_or_else(|| err("run config without model"))?;

    let mut parts = Vec::new();
    if let Some(tools) = take(&mut config, "tools") {
        add_part(PartKind::Tools, &tools, &mut parts, out)?;
    }
    if let Some(format) = take(&mut config, "response_format") {
        add_part(PartKind::ResponseFormat, &format, &mut parts, out)?;
    }
    if let Some(global) = take(&mut config, "global_system") {
        add_part(PartKind::GlobalSystem, &global, &mut parts, out)?;
    }
    let mut manifest_rest = None;
    if let Some(Value::Object(mut manifest)) = take(&mut obj, "manifest") {
        if let Some(plan) = take(&mut manifest, "plan") {
            add_part(PartKind::Plan, &plan, &mut parts, out)?;
        }
        if let Some(Value::Array(fetches)) = take(&mut manifest, "fetches") {
            for fetch in fetches {
                add_part(PartKind::ProviderAnswer, &fetch, &mut parts, out)?;
            }
        }
        if let Some(tools) = take(&mut manifest, "tools") {
            add_part(PartKind::Tools, &tools, &mut parts, out)?;
        }
        if let Some(text) = take(&mut manifest, "system_text") {
            add_part(PartKind::SystemText, &text, &mut parts, out)?;
        }
        if let Some(declarations) = take(&mut manifest, "declarations") {
            add_part(PartKind::Declarations, &declarations, &mut parts, out)?;
        }
        manifest_rest = rest(manifest);
    }

    let tool_names: Vec<String> = parts
        .iter()
        .find(|p| p.kind == PartKind::Tools)
        .and_then(|p| out.parts.get(&p.digest))
        .and_then(|bytes| serde_json::from_slice::<Vec<Value>>(bytes).ok())
        .map(|tools| {
            tools
                .iter()
                .filter_map(|t| t.get("name").and_then(Value::as_str).map(str::to_owned))
                .collect()
        })
        .unwrap_or_default();

    let mut sorted = parts.clone();
    sorted.sort();
    if sorted != state.setup || state.model.as_ref() != Some(&model) {
        out.records.push(Record::SetupStub(SetupStub {
            parts: parts.clone(),
            model: model.clone(),
            tool_names,
        }));
        state.setup = sorted;
    }

    let written_at = take_u64(&mut obj, "written_at_ms");
    let submission_id = take_str(&mut obj, "submission_id");
    let input: Vec<Message> = take_as(&mut obj, "input")?.unwrap_or_default();
    let render_config = take(&mut config, "frozen_render_config");
    let auth_selection = take(&mut config, "auth_selection");
    let renderer_version = take_u64(&mut config, "renderer_version").unwrap_or_default() as u32;
    let context_limit = take_u64(&mut config, "context_limit");
    let service_tier = take_str(&mut config, "service_tier");
    let route_decision_id = take_str(&mut config, "route_decision_id");

    let mut choices = Map::new();
    if let Some(config) = rest(config) {
        choices.insert("config".into(), config);
    }
    if let Some(manifest) = manifest_rest {
        choices.insert("manifest".into(), manifest);
    }
    // The session identity lives in the head; the v1 tag ledger is measured separately.
    for key in ["session", "project_id"] {
        obj.remove(key);
    }
    if let Some(tags) = take(&mut obj, "tags") {
        out.uncovered
            .entry("run_started (v1 tags)".into())
            .or_default()
            .add(tags.to_string().len());
    }

    let start = RunStart {
        run_id: run_id.clone(),
        model: model.clone(),
        setup: parts,
        renderer_version,
        submission_id: submission_id.clone(),
        send_id: take_str(&mut obj, "send_id"),
        request_identity_digest: take_str(&mut obj, "request_identity_digest"),
        ts_ms: take_u64(&mut obj, "ts_ms"),
        written_at_ms: written_at,
        session_class,
        retention_secs: take_u64(&mut obj, "retention"),
        title: take_str(&mut obj, "title"),
        context_limit,
        service_tier,
        route_decision_id,
        compaction_plan_version: take_u64(&mut obj, "compaction_plan_version").map(|v| v as u32),
        render_config,
        auth_selection,
        run_choices: None,
        origin: take(&mut obj, "origin"),
        principal: take(&mut obj, "principal"),
        scope: take(&mut obj, "scope"),
        hooks: take(&mut obj, "hooks"),
        keep_warm: take(&mut obj, "keep_warm"),
        on_restart: take(&mut obj, "on_restart"),
    };
    if let Some(record) = rest(obj) {
        choices.insert("record".into(), record);
    }
    let start = RunStart {
        run_choices: rest(choices),
        ..start
    };
    out.records.push(Record::RunStart(Box::new(start)));
    for message in input {
        out.records.push(Record::Input(Input {
            run_id: run_id.clone(),
            placement: Placement::RunInput,
            message,
            written_at_ms: written_at,
            submission_id: submission_id.clone(),
            notice: None,
        }));
    }
    state.run_id = run_id;
    state.model = Some(model);
    state.call_inputs.clear();
    Ok(())
}

fn model_step_finished(
    mut obj: Map<String, Value>,
    state: &mut State,
    out: &mut Converted,
) -> Result<(), ConvertError> {
    let step_id = take_u64(&mut obj, "step_id").unwrap_or_default();
    let mut assistant = match take(&mut obj, "assistant_message") {
        Some(Value::Object(assistant)) => assistant,
        _ => return Err(err("model_step_finished without assistant_message")),
    };
    let message_id: MessageId =
        take_as(&mut assistant, "message_id")?.unwrap_or(MessageId(String::new()));
    let content: Vec<ContentBlock> = take_as(&mut assistant, "content")?.unwrap_or_default();
    state.call_inputs.clear();
    let mut calls = 0u32;
    for block in &content {
        if let ContentBlock::ToolCall(call) = block {
            calls += 1;
            state
                .call_inputs
                .insert(call.tool_call_id.0.clone(), call.input.clone());
        }
    }
    let usage: Option<Usage> = take_as(&mut obj, "usage")?;
    let model = state
        .model
        .clone()
        .ok_or_else(|| err("model step before any run start"))?;
    out.records.push(Record::Assistant(Assistant {
        run_id: state.run_id.clone(),
        step_id,
        provider_message_id: message_id,
        message: Message::assistant_from(content, model),
        usage,
        written_at_ms: take_u64(&mut obj, "written_at_ms"),
        last_content_at_ms: None,
    }));
    if let Some(tags) = take(&mut obj, "tags") {
        out.uncovered
            .entry("model_step_finished (v1 tags)".into())
            .or_default()
            .add(tags.to_string().len());
    }
    let finish_reason: Option<FinishReason> = take_as(&mut obj, "finish_reason")?;
    out.records.push(Record::StepEnd(StepEnd {
        run_id: state.run_id.clone(),
        step_id,
        usage,
        finish_reason,
        retries_used: take_u64(&mut obj, "retries_used").unwrap_or_default() as u32,
        service_tier_used: take_str(&mut obj, "service_tier_used"),
        speed_used: take_str(&mut obj, "speed_used"),
        call_count: calls,
        hooks: take(&mut obj, "hooks"),
        fallback: rest(obj),
    }));
    Ok(())
}

fn model_attempt(
    mut obj: Map<String, Value>,
    state: &mut State,
    out: &mut Converted,
) -> Result<(), ConvertError> {
    let mut billing = Map::new();
    for key in ["reservation", "envelope", "account_limit_disposition"] {
        if let Some(value) = take(&mut obj, key) {
            billing.insert(key.into(), value);
        }
    }
    let request_sha = take_str(&mut obj, "request_body_sha256").unwrap_or_default();
    let response_sha = take_str(&mut obj, "response_body_sha256").unwrap_or_default();
    let attempt = Attempt {
        run_id: state.run_id.clone(),
        step_id: take_u64(&mut obj, "step_id").unwrap_or_default(),
        attempt: take_u64(&mut obj, "attempt").unwrap_or_default() as u32,
        attempt_id: take_str(&mut obj, "attempt_id"),
        outcome: take_str(&mut obj, "outcome").unwrap_or_default(),
        http_status: take_u64(&mut obj, "http_status").map(|v| v as u32),
        request_body_len: take_u64(&mut obj, "request_body_len").unwrap_or_default(),
        request_body_sha256: hex_digest(&request_sha).unwrap_or(Digest([0; 32])),
        response_body_len: take_u64(&mut obj, "response_body_len").unwrap_or_default(),
        response_body_sha256: hex_digest(&response_sha).unwrap_or(Digest([0; 32])),
        ts_ms: take_u64(&mut obj, "ts_ms").unwrap_or_default(),
        sent_at_ms: take_u64(&mut obj, "sent_at_ms"),
        first_content_at_ms: take_u64(&mut obj, "first_content_at_ms"),
        last_content_at_ms: take_u64(&mut obj, "last_content_at_ms"),
        transport: take_str(&mut obj, "transport"),
        served_account_ref: take_str(&mut obj, "served_account_ref"),
        billing: rest(billing),
        telemetry: rest(obj),
    };
    out.records.push(Record::Attempt(attempt));
    Ok(())
}

fn tool_intent(
    mut obj: Map<String, Value>,
    state: &mut State,
    out: &mut Converted,
) -> Result<(), ConvertError> {
    let call: ToolCallId =
        take_as(&mut obj, "tool_call_id")?.ok_or_else(|| err("intent without tool_call_id"))?;
    let args = take(&mut obj, "args").unwrap_or(Value::Null);
    let args_digest = sha256(&canonical(&args)?);
    let same_as_call = state.call_inputs.get(&call.0) == Some(&args);
    out.records.push(Record::ToolIntent(ToolIntent {
        run_id: state.run_id.clone(),
        batch_id: take_str(&mut obj, "batch_id").unwrap_or_default(),
        tool_call_id: call,
        tool_name: take_str(&mut obj, "tool_name"),
        module_id: take_str(&mut obj, "module_id"),
        args_digest,
        args: (!same_as_call).then_some(args),
        call_key: take_str(&mut obj, "call_key"),
        approval_digest: take_str(&mut obj, "approval_digest"),
        dispatched_at_ms: take_u64(&mut obj, "dispatched_at_ms"),
        schema_pin: take(&mut obj, "schema_pin"),
        hooks: take(&mut obj, "hooks"),
    }));
    Ok(())
}

/// A tool call's terminal as today's records state it, before it splits into a transcript reply
/// and a ledger outcome.
struct Terminal {
    result: ToolResult,
    batch_id: String,
    outcome: CallOutcome,
    finished_at_ms: Option<u64>,
    reason: Option<String>,
    detail: Option<Value>,
    hooks: Option<Value>,
}

fn reply_and_outcome(terminal: Terminal, state: &State, out: &mut Converted) {
    let Terminal {
        result,
        batch_id,
        outcome,
        finished_at_ms,
        reason,
        detail,
        hooks,
    } = terminal;
    let result_digest = sha256(&result.encode());
    let tool_call_id = result.tool_call_id.clone();
    let is_error = result.is_error;
    out.records.push(Record::ToolReply(ToolReply {
        run_id: state.run_id.clone(),
        batch_id: batch_id.clone(),
        result,
        finished_at_ms,
    }));
    out.records.push(Record::ToolOutcome(ToolOutcome {
        run_id: state.run_id.clone(),
        batch_id,
        tool_call_id,
        outcome,
        result_digest,
        is_error,
        reason,
        finished_at_ms,
        detail,
        hooks,
    }));
}

fn tool_result(
    mut obj: Map<String, Value>,
    state: &mut State,
    out: &mut Converted,
) -> Result<(), ConvertError> {
    let result: ToolResult =
        take_as(&mut obj, "result")?.ok_or_else(|| err("tool_result without result"))?;
    let outcome = match take_str(&mut obj, "outcome").as_deref() {
        Some("completed") | None => CallOutcome::Completed,
        Some("unknown") => CallOutcome::Unknown,
        Some("interrupted") => CallOutcome::Interrupted,
        Some("max_steps") => CallOutcome::MaxSteps,
        Some(other) => return Err(err(format!("unknown dispatch outcome {other}"))),
    };
    if let Some(tags) = take(&mut obj, "tags") {
        out.uncovered
            .entry("tool_result (v1 tags)".into())
            .or_default()
            .add(tags.to_string().len());
    }
    let terminal = Terminal {
        result,
        batch_id: take_str(&mut obj, "batch_id").unwrap_or_default(),
        outcome,
        finished_at_ms: take_u64(&mut obj, "finished_at_ms"),
        reason: take_str(&mut obj, "reason"),
        hooks: take(&mut obj, "hooks"),
        detail: None,
    };
    reply_and_outcome(
        Terminal {
            detail: rest(obj),
            ..terminal
        },
        state,
        out,
    );
    Ok(())
}

fn tool_no_dispatch(
    record: &WalRecord,
    state: &mut State,
    out: &mut Converted,
) -> Result<(), ConvertError> {
    let WalRecord::ToolNoDispatch {
        dispatched_at_ms,
        batch_id,
        tool_call_id,
        reason,
        hooks,
        tags,
    } = record
    else {
        return Err(err("tool_no_dispatch shape"));
    };
    if let Some(tags) = tags {
        out.uncovered
            .entry("tool_no_dispatch (v1 tags)".into())
            .or_default()
            .add(serde_json::to_vec(tags).map_or(0, |v| v.len()));
    }
    reply_and_outcome(
        Terminal {
            result: broca_core::replay::no_dispatch_result(tool_call_id, reason),
            batch_id: batch_id.0.clone(),
            outcome: CallOutcome::NotDispatched,
            finished_at_ms: *dispatched_at_ms,
            reason: None,
            detail: serde_json::to_value(reason).ok(),
            hooks: hooks.as_ref().and_then(|h| serde_json::to_value(h).ok()),
        },
        state,
        out,
    );
    Ok(())
}

#[cfg(test)]
mod tests;
