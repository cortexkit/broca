//! Converts one of today's session logs into records of the new session storage format.
//!
//! The mapping works on each old record's JSON form, the shape it is stored in today. Fields are
//! taken out one at a time by name into typed fields, or carried whole inside an owned payload.
//! Nothing is dropped silently:
//! - a field left over after its record is converted is reported as *lost*, by record type and
//!   field name, and so is a value of the wrong type or a required field that is missing;
//! - old records whose fields the new format's state rebuilding (its folds, see
//!   `broca_session_store::record`) doesn't read individually yet become ledger events that
//!   carry them whole;
//! - what the new format retires on purpose (`turn_finished`, `visibility_late`, the session
//!   identity repeated in every run start, which the head holds) is counted as *dropped*.
//!
//! A clean conversion is one whose lost tally is empty.

use std::collections::{BTreeMap, HashMap};

use broca_protocol::content::{ContentBlock, FinishReason, Message, Role, ToolResult};
use broca_protocol::ids::{MessageId, ToolCallId};
use broca_protocol::request::ModelHandle;
use broca_protocol::usage::Usage;
use broca_session_store::codec::{Digest, Message as _};
use broca_session_store::fold::conversation::Conversation;
use broca_session_store::record::{
    Assistant, Attempt, BatchEnd, BatchStart, BudgetFire, BudgetFiring, CallOutcome, CancelPoint,
    CancelRequest, Compaction, Delivery, DeliveryOrdered, Event, Input, Marker, Notice, PartKind,
    PartRef, Pause, Placement, QueuedSend, Record, ResultAppend, Retraction, RunEnd, RunStart,
    SendEnded, SessionRefresh, SetupStub, Steer, StepEnd, StepStart, Stream, StructuredResult,
    ToolHeld, ToolIntent, ToolOutcome, ToolReply, View, ViewApplied,
};
use broca_wal::{FramedRecord, WalRecord};
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};
use sha2::{Digest as _, Sha256};

pub mod entry_facts;
pub mod inverse;
pub mod measure;
pub mod split;

/// A count of old records, fields or values, and their stored JSON size.
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

fn tally(map: &mut BTreeMap<String, Tally>, name: impl Into<String>, bytes: usize) {
    map.entry(name.into()).or_default().add(bytes);
}

/// One converted log.
#[derive(Debug, Default)]
pub struct Converted {
    pub records: Vec<Record>,
    /// Every setup part this log's runs named, by digest, with its canonical bytes.
    pub parts: BTreeMap<Digest, Vec<u8>>,
    /// Bytes of setup parts as today's runs store them, summed over runs.
    pub part_bytes_per_run: u64,
    pub lost: BTreeMap<String, Tally>,
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
    /// Copies of steered and delivered messages accepted but not placed yet. Today's log
    /// records them where they arrived; the conversation takes them at the next step boundary
    /// or run end, which is where the new transcript records its copies.
    held: Vec<Input>,
    /// Whether the current run was frozen with a tool-call budget. Only then does a budget
    /// firing place a message in today's conversation.
    budget_configured: bool,
    /// The firing the current run's budget had due when the budget record being converted was
    /// written. Older records stored neither which nudge fired nor its text.
    due_firing: Option<broca_core::DueFiring>,
    /// Whether the step being converted is a run's budget-finalize step.
    finalize_step: bool,
    /// For the step being converted: whether a stop condition was met, and whether a
    /// limiting one was. Today's replay works these out from the frozen stop conditions; the
    /// new ledger records them.
    stop_met: bool,
    limiting_stop_met: bool,
    /// The refusal-fallback schedule the run being converted starts with, as today's replay
    /// decides it at admission. The new run start records it.
    admitted_schedule: Option<broca_wal::RefusalFallbackSchedule>,
    /// The seq of the old record being converted.
    seq: u64,
    /// In a mode-3 log: who delivered each tool-result delivery's text, by the delivering
    /// record's seq (see `entry_facts`).
    delivery_notices: HashMap<u64, Notice>,
}

/// Writes the held copies as transcript entries at the boundary that places them.
fn place_held(state: &mut State, out: &mut Converted) {
    out.records.extend(state.held.drain(..).map(Record::Input));
}

/// Holds copies of messages accepted by the ledger record about to be written, until a boundary
/// places them. Copies never placed are dropped: they never reached the conversation, and the
/// accepting record keeps the messages.
fn hold(inputs: Vec<Input>, state: &mut State, out: &Converted) {
    let receipt = out.records.len() as u64 + 1;
    let run_id = state.run_id.clone();
    state.held.extend(inputs.into_iter().map(|input| Input {
        run_id: run_id.clone(),
        receipt_seq: Some(receipt),
        ..input
    }));
}

/// Converts one log. `lineage` is the log's lineage id: a log on the current compaction
/// contract needs it, because today's message ids carry it and a converted message keeps its id.
pub fn convert(old: &[FramedRecord], lineage: Option<&str>) -> Result<Converted, ConvertError> {
    let mut out = Converted::default();
    let mut state = State::default();
    // A mode-3 log gets an `EntryFacts` record right after each record that places (or
    // reserves) a message, with what today's projection serves about it. A fold that keeps only
    // counts tells which ordinals each converted record placed.
    let mut facts = entry_facts::legacy(old, lineage)
        .map_err(err)?
        .map(|legacy| {
            state.delivery_notices = legacy.delivery_notices.clone();
            (legacy, Conversation::new(false), 0u64)
        });
    // Today's placement and budget rules decide what older records left implicit: the firing a
    // budget record stands for, and whether a step met a stop condition. They read each record
    // before it is converted, as today's replay does.
    let mut placement = broca_core::placement::Placement::default();
    let mut budget = None;
    let mut stop_when = Vec::new();
    // Session-scoped: the schedule carries from run to run under the same model.
    let mut schedule: Option<broca_wal::RefusalFallbackSchedule> = None;
    for framed in old {
        match &framed.record {
            WalRecord::RunStarted { config, ts_ms, .. } => {
                budget = config.budget.clone();
                stop_when = config.stop_when.clone();
                state.budget_configured = budget.is_some();
                schedule = broca_core::refusal_fallback::on_episode_admitted(
                    schedule.take(),
                    &config.model,
                    *ts_ms,
                );
                state.admitted_schedule = schedule.clone();
            }
            WalRecord::ModelStepFinished { fallback, .. } => {
                if let Some(next) = &fallback.refusal_fallback {
                    schedule = Some(next.clone());
                }
            }
            WalRecord::ModelStepRefused { schedule: next, .. } => schedule = Some(next.clone()),
            WalRecord::BudgetFired { .. } => {
                state.due_firing = budget
                    .as_ref()
                    .and_then(|budget| broca_core::next_due(placement.budget(), budget));
            }
            _ => {}
        }
        // A record today's placement refuses is one today's replay refuses too; the conversion
        // still carries it, and the conversation check reports the log.
        let actions = placement.observe(&framed.record).unwrap_or_default();
        // A step taken after the budget's finalize firing enters today's conversation at once,
        // calls and all.
        state.finalize_step = actions.iter().any(|action| {
            matches!(
                action,
                broca_core::placement::PlacementAction::Append {
                    kind: broca_core::placement::AppendKind::Finalize,
                    ..
                }
            )
        });
        if let WalRecord::ModelStepFinished {
            assistant_message, ..
        } = &framed.record
        {
            let steps = placement.completed_steps();
            state.stop_met =
                broca_core::run::stop_satisfied(&stop_when, steps, assistant_message).is_some();
            state.limiting_stop_met =
                broca_core::run::limiting_stop_satisfied(&stop_when, steps, assistant_message);
        }
        state.seq = framed.seq;
        let before = out.records.len();
        convert_one(&framed.record, &mut state, &mut out)?;
        if let Some((legacy, counts, emitted)) = &mut facts {
            let mut known = *emitted;
            for record in &out.records[before..] {
                if record.stream() != Stream::Transcript {
                    continue;
                }
                counts
                    .apply(record)
                    .map_err(|e| err(format!("entry facts: conversation {e:?}")))?;
                counts.clear_window();
                let reserved = counts.open_tool_turn().is_some();
                known = known.max(counts.placed + u64::from(reserved));
            }
            for ordinal in *emitted + 1..=known {
                let (metadata, written_at, mid) =
                    legacy.rows.get(&(ordinal - 1)).ok_or_else(|| {
                        err(format!(
                            "entry facts: today's projection has no message {ordinal}"
                        ))
                    })?;
                out.records.push(Record::EntryFacts(entry_facts::record(
                    ordinal,
                    metadata,
                    *written_at,
                    mid.clone(),
                )));
            }
            *emitted = known;
        }
    }
    if let Some((legacy, _, emitted)) = &facts {
        if emitted != &(legacy.rows.len() as u64) {
            return Err(err(format!(
                "entry facts: today's projection has {} messages, the conversion placed {emitted}",
                legacy.rows.len()
            )));
        }
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

fn json_len(value: &Value) -> usize {
    serde_json::to_vec(value).map_or(0, |v| v.len())
}

/// The fields of one old record (or one object inside it), taken out one by one. Whatever is
/// still there at `finish` is reported as lost, and so are values of the wrong type and missing
/// required fields, so a misspelt field name shows up instead of reading as absent.
struct Fields {
    path: String,
    object: Map<String, Value>,
    lost: Vec<(String, usize)>,
}

impl Fields {
    fn new(path: impl Into<String>, object: Map<String, Value>) -> Self {
        Self {
            path: path.into(),
            object,
            lost: Vec::new(),
        }
    }

    fn lose(&mut self, what: String, bytes: usize) {
        self.lost.push((format!("{}.{what}", self.path), bytes));
    }

    fn has(&self, key: &str) -> bool {
        self.object.get(key).is_some_and(|v| !v.is_null())
    }

    fn take(&mut self, key: &str) -> Option<Value> {
        self.object.remove(key).filter(|v| !v.is_null())
    }
    fn u64(&mut self, key: &str) -> Option<u64> {
        let value = self.take(key)?;
        let number = value.as_u64();
        if number.is_none() {
            self.lose(format!("{key} (not an unsigned integer)"), json_len(&value));
        }
        number
    }

    fn req_u64(&mut self, key: &str) -> u64 {
        if !self.has(key) {
            self.lose(format!("{key} (missing)"), 0);
        }
        self.u64(key).unwrap_or_default()
    }

    fn req_bool(&mut self, key: &str) -> bool {
        match self.take(key) {
            Some(Value::Bool(value)) => value,
            Some(other) => {
                self.lose(format!("{key} (not a bool)"), json_len(&other));
                false
            }
            None => {
                self.lose(format!("{key} (missing)"), 0);
                false
            }
        }
    }

    fn string(&mut self, key: &str) -> Option<String> {
        match self.take(key)? {
            Value::String(text) => Some(text),
            other => {
                self.lose(format!("{key} (not a string)"), json_len(&other));
                None
            }
        }
    }

    fn req_string(&mut self, key: &str) -> String {
        if !self.has(key) {
            self.lose(format!("{key} (missing)"), 0);
        }
        self.string(key).unwrap_or_default()
    }

    fn parse<T: DeserializeOwned>(&mut self, key: &str) -> Result<Option<T>, ConvertError> {
        let path = format!("{}.{key}", self.path);
        self.take(key)
            .map(|v| serde_json::from_value(v).map_err(|e| err(format!("{path}: {e}"))))
            .transpose()
    }

    /// An object member as its own fields, to take from separately.
    fn object(&mut self, key: &str) -> Option<Fields> {
        match self.take(key)? {
            Value::Object(object) => Some(Fields::new(format!("{}.{key}", self.path), object)),
            other => {
                self.lose(format!("{key} (not an object)"), json_len(&other));
                None
            }
        }
    }

    /// Everything not yet taken, as one payload carried whole: `None` when nothing is left.
    fn rest(&mut self) -> Option<Value> {
        let object = std::mem::take(&mut self.object);
        (!object.is_empty()).then_some(Value::Object(object))
    }

    fn finish(self, out: &mut Converted) {
        for (key, value) in self.object {
            tally(
                &mut out.lost,
                format!("{}.{key}", self.path),
                json_len(&value),
            );
        }
        for (what, bytes) in self.lost {
            tally(&mut out.lost, what, bytes);
        }
    }
}

fn convert_one(
    record: &WalRecord,
    state: &mut State,
    out: &mut Converted,
) -> Result<(), ConvertError> {
    if let WalRecord::ToolNoDispatch { .. } = record {
        return tool_no_dispatch(record, state, out);
    }
    let value = serde_json::to_value(record).map_err(|e| err(format!("serialize: {e}")))?;
    let size = json_len(&value);
    let Value::Object(mut object) = value else {
        return Err(err("record is not an object"));
    };
    let kind = match object.remove("type") {
        Some(Value::String(kind)) => kind,
        _ => return Err(err("record without a type")),
    };
    let mut f = Fields::new(kind.clone(), object);
    match kind.as_str() {
        "run_started" => run_started(&mut f, state, out)?,
        "model_step_finished" => model_step_finished(&mut f, state, out)?,
        "model_attempt_finished" => model_attempt(&mut f, state, out),
        "step_started" => {
            place_held(state, out);
            out.records.push(Record::StepStart(StepStart {
                run_id: state.run_id.clone(),
                step_id: f.req_u64("step_id"),
            }));
        }
        "tool_batch_started" => {
            let planned: Vec<ToolCallId> = f.parse("planned_calls")?.unwrap_or_default();
            out.records.push(Record::BatchStart(BatchStart {
                run_id: state.run_id.clone(),
                batch_id: f.req_string("batch_id"),
                step_id: f.req_u64("step_id"),
                planned,
            }));
        }
        "tool_batch_finished" => out.records.push(Record::BatchEnd(BatchEnd {
            run_id: state.run_id.clone(),
            batch_id: f.req_string("batch_id"),
        })),
        "tool_dispatch_intent" => tool_intent(&mut f, state, out)?,
        "tool_result" => tool_result(&mut f, state, out)?,
        "run_finished" => {
            let reason = f
                .parse("reason")?
                .ok_or_else(|| err("run_finished without reason"))?;
            place_held(state, out);
            out.records.push(Record::RunEnd(RunEnd {
                run_id: state.run_id.clone(),
                reason,
                usage: f.parse::<Usage>("usage")?,
                ts_ms: f.u64("ts_ms"),
                retries_used: f.u64("retries_used").unwrap_or_default() as u32,
                interrupt_reason: f.string("interrupt_reason"),
                error: f.take("error"),
            }));
        }
        "steered" => {
            let input: Vec<Message> = f.parse("input")?.unwrap_or_default();
            let written = f.u64("written_at_ms");
            let tags = f.take("tags");
            hold(
                copies_of(&input, &tags, Placement::Steer, written, None),
                state,
                out,
            );
            // The record keeps everything it accepted: the messages, and the receipt (send id,
            // delivery, retention, sender) the queue reads.
            out.records.push(Record::Steer(Steer {
                input,
                written_at_ms: written,
                tags_v1: tags,
                receipt: f.rest(),
            }));
        }
        "send_delivered" => send_delivered(&mut f, state, out)?,
        "budget_fired" => {
            let firing = match f.req_string("kind").as_str() {
                "nudge" => BudgetFiring::Nudge,
                "finalize" => BudgetFiring::Finalize,
                other => {
                    f.lose(format!("kind (unknown: {other})"), other.len());
                    BudgetFiring::Nudge
                }
            };
            let due = state.due_firing.take();
            let fire = BudgetFire {
                run_id: f.req_string("run_id"),
                firing,
                threshold: f.req_u64("threshold"),
                ordinal: f.req_u64("ordinal"),
                caller_text: f.string("caller_text"),
                tags_v1: f.take("tags"),
                nudge_index: due
                    .as_ref()
                    .and_then(|due| due.nudge_index)
                    .map(|index| index as u64),
            };
            // The transcript gets the message the conversation took, written right after the
            // record and naming it. Older records didn't store the text, and today's
            // conversation then takes the text the run's budget had due.
            let text = fire.caller_text.clone().or_else(|| due.map(|due| due.text));
            let tags = fire.tags_v1.clone();
            out.records.push(Record::BudgetFire(fire));
            if let (Some(text), true) = (text, state.budget_configured) {
                out.records.push(Record::Input(Input {
                    run_id: state.run_id.clone(),
                    placement: Placement::BudgetNudge,
                    message: Message::new(Role::User, vec![ContentBlock::Text { text }]),
                    written_at_ms: None,
                    submission_id: None,
                    notice: None,
                    tags_v1: tags,
                    receipt_seq: Some(out.records.len() as u64),
                    pruned: Vec::new(),
                }));
            }
        }
        "run_paused" => {
            let reason = f
                .parse("reason")?
                .ok_or_else(|| err("run_paused without reason"))?;
            out.records.push(Record::Pause(Pause {
                run_id: f.req_string("run_id"),
                reason,
                ts_ms: f.u64("ts_ms"),
                error: f.take("error"),
                account_limit: f.take("account_limit"),
            }));
        }
        "tool_call_provider_held" => out.records.push(Record::ToolHeld(ToolHeld {
            batch_id: f.req_string("batch_id"),
            tool_call_id: ToolCallId(f.req_string("tool_call_id")),
            call_key: f.req_string("call_key"),
            module: f.req_string("module"),
            reason: f.req_string("reason"),
            detail_digest: f.string("detail_digest"),
            held_at_ms: f.req_u64("held_at_ms"),
            sequential: f.req_bool("sequential"),
        })),
        "send_ended" => out.records.push(Record::SendEnded(SendEnded {
            submission_id: f.req_string("submission_id"),
            reason: f.take("reason"),
        })),
        "delivery_ordered" => {
            let submission_id = f.req_string("submission_id");
            let order = f
                .take("order")
                .ok_or_else(|| err("delivery_ordered without order"))?;
            out.records.push(Record::DeliveryOrdered(DeliveryOrdered {
                submission_id,
                order,
                sent_by: f.take("sent_by"),
            }));
        }
        "retracted" => out.records.push(Record::Retraction(Retraction {
            submission_id: f.req_string("submission_id"),
            failure: f.take("failure"),
        })),
        "run_cancel_requested" => {
            let at = match f.req_string("at").as_str() {
                "now" => CancelPoint::Now,
                "step_boundary" => CancelPoint::StepBoundary,
                other => {
                    f.lose(format!("at (unknown: {other})"), other.len());
                    CancelPoint::Now
                }
            };
            out.records.push(Record::CancelRequest(CancelRequest {
                run_id: f.req_string("run_id"),
                at,
            }));
        }
        "queued" => queued(&mut f, out)?,
        "session_refresh_accepted" => {
            let mut setup = Vec::new();
            let manifest = match f.object("manifest") {
                Some(manifest) => manifest_parts(manifest, &mut setup, out)?,
                None => None,
            };
            out.records.push(Record::SessionRefresh(SessionRefresh {
                generation: f.req_u64("generation"),
                policy: f.req_string("policy"),
                run_in_progress: f.req_bool("run_in_progress"),
                applies_from_next_run: f.req_bool("applies_from_next_run"),
                setup,
                manifest,
            }));
        }
        "structured_output_result" => {
            let run_id = f.req_string("run_id");
            let result = f.take("result").unwrap_or(Value::Null);
            out.records.push(Record::StructuredResult(StructuredResult {
                run_id,
                result,
            }));
        }
        "compaction_marker" => out.records.push(Record::Marker(Marker {
            view_version: f.req_u64("view_version"),
            first_kept_mid: f.req_string("first_kept_mid"),
            first_kept_ordinal: f.req_u64("first_kept_ordinal"),
            checkpoint: f.req_u64("checkpoint"),
            harness: f.string("harness"),
            session: f.string("session"),
        })),
        "compaction_view_v2" => {
            let version = f.req_u64("version");
            let checksum = f.req_string("checksum");
            let edits = f
                .take("edits")
                .ok_or_else(|| err("compaction_view_v2 without edits"))?;
            out.records.push(Record::View(View {
                version,
                checksum,
                edits,
            }));
        }
        "compaction_view_applied_v2" => out.records.push(Record::ViewApplied(ViewApplied {
            step_id: f.req_u64("step_id"),
            version: f.req_u64("version"),
            checksum: f.req_string("checksum"),
        })),
        "compaction_message" => {
            let body = f.rest().unwrap_or(Value::Null);
            out.records
                .push(Record::Compaction(Compaction { version: 1, body }));
        }
        "recovery_call_closed" => {
            let result: ToolResult = f
                .parse("result")?
                .ok_or_else(|| err("recovery_call_closed without result"))?;
            out.records.push(Record::ToolReply(ToolReply {
                run_id: f.string("run_id").unwrap_or_else(|| state.run_id.clone()),
                batch_id: f.string("batch_id"),
                result,
                finished_at_ms: None,
                tags_v1: None,
            }));
            event(&mut f, kind.clone(), out);
        }
        "approval_recorded" => {
            if let Some(result) = f.parse::<ToolResult>("closing_result")? {
                out.records.push(Record::ToolReply(ToolReply {
                    run_id: state.run_id.clone(),
                    batch_id: None,
                    result,
                    finished_at_ms: f.u64("closed_at_ms"),
                    tags_v1: f.take("tags"),
                }));
            }
            event(&mut f, kind.clone(), out);
        }
        "turn_finished" | "visibility_late" => {
            f.rest();
            tally(&mut out.dropped, kind, size);
        }
        _ => event(&mut f, kind.clone(), out),
    }
    f.finish(out);
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
    // Kept even when an equal part is already listed: a run's config and its manifest both
    // name their tools, often identical, and the position says which member a part fills.
    parts.push(PartRef { kind, digest });
    Ok(())
}

fn tool_names(parts: &[PartRef], out: &Converted) -> Vec<String> {
    parts
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
        .unwrap_or_default()
}

/// Moves a plan manifest's shared members into setup parts and returns what remains of it.
fn manifest_parts(
    mut manifest: Fields,
    parts: &mut Vec<PartRef>,
    out: &mut Converted,
) -> Result<Option<Value>, ConvertError> {
    if let Some(plan) = manifest.take("plan") {
        add_part(PartKind::Plan, &plan, parts, out)?;
    }
    match manifest.take("fetches") {
        Some(Value::Array(fetches)) => {
            for fetch in fetches {
                add_part(PartKind::ProviderAnswer, &fetch, parts, out)?;
            }
        }
        Some(other) => manifest.lose("fetches (not an array)".into(), json_len(&other)),
        None => {}
    }
    for (key, kind) in [
        ("tools", PartKind::Tools),
        ("system_text", PartKind::SystemText),
        ("declarations", PartKind::Declarations),
    ] {
        if let Some(value) = manifest.take(key) {
            add_part(kind, &value, parts, out)?;
        }
    }
    let rest = manifest.rest();
    manifest.finish(out);
    Ok(rest)
}

/// The v1 part numbers of the `index`th message of a record whose tags list one entry per
/// message.
fn tags_at(tags: &Option<Value>, index: usize) -> Option<Value> {
    match tags {
        Some(Value::Array(per_message)) => per_message.get(index).cloned(),
        _ => None,
    }
}

/// Copies of the messages a steer or delivered send carries, to place in the transcript. The
/// record keeps its own messages, `tags` (the part numbers its run showed the model) and time;
/// each copy takes its message's entry from `tags` when `tags` holds one entry per message.
fn copies_of(
    messages: &[Message],
    tags: &Option<Value>,
    placement: Placement,
    written: Option<u64>,
    submission: Option<String>,
) -> Vec<Input> {
    let per_message = match tags {
        Some(Value::Array(list)) if list.len() == messages.len() => tags.clone(),
        _ => None,
    };
    messages
        .iter()
        .enumerate()
        .map(|(index, message)| Input {
            run_id: String::new(),
            placement,
            message: message.clone(),
            written_at_ms: written,
            submission_id: submission.clone(),
            notice: None,
            tags_v1: tags_at(&per_message, index),
            receipt_seq: None,
            pruned: Vec::new(),
        })
        .collect()
}

/// Carries the rest of an old record whole, as a ledger event under its old type name. The run
/// id is the record's own, never inferred, so the event reads back exactly as written.
fn event(f: &mut Fields, name: String, out: &mut Converted) {
    out.records.push(Record::Event(Event {
        name,
        run_id: f.string("run_id"),
        body: f.rest(),
    }));
}

fn queued(f: &mut Fields, out: &mut Converted) -> Result<(), ConvertError> {
    let submission_id = f.req_string("submission_id");
    let mut request = f
        .object("request")
        .ok_or_else(|| err("queued without request"))?;
    let input: Vec<Message> = request.parse("input")?.unwrap_or_default();
    let mut setup = Vec::new();
    if let Some(tools) = request.take("tools") {
        add_part(PartKind::Tools, &tools, &mut setup, out)?;
    }
    let request_rest = request.rest();
    request.finish(out);
    out.records.push(Record::QueuedSend(QueuedSend {
        submission_id,
        send_id: f.string("send_id"),
        input,
        setup,
        written_at_ms: f.u64("written_at_ms"),
        ts_ms: f.u64("ts_ms"),
        request: request_rest,
        delivery: f.string("delivery"),
        receipt: f.rest(),
    }));
    Ok(())
}

fn run_started(f: &mut Fields, state: &mut State, out: &mut Converted) -> Result<(), ConvertError> {
    out.runs += 1;
    let run_id = f.req_string("run_id");
    let session_class = f.string("session_class");
    if out.session_class.is_none() {
        out.session_class = session_class.clone();
    }
    let mut config = f
        .object("config")
        .ok_or_else(|| err("run_started without config"))?;
    let model: ModelHandle = config
        .parse("model")?
        .ok_or_else(|| err("run config without model"))?;

    let mut parts = Vec::new();
    for (key, kind) in [
        ("tools", PartKind::Tools),
        ("response_format", PartKind::ResponseFormat),
        ("global_system", PartKind::GlobalSystem),
    ] {
        if let Some(value) = config.take(key) {
            add_part(kind, &value, &mut parts, out)?;
        }
    }
    let manifest_rest = match f.object("manifest") {
        Some(manifest) => manifest_parts(manifest, &mut parts, out)?,
        None => None,
    };

    let mut sorted = parts.clone();
    sorted.sort();
    if sorted != state.setup || state.model.as_ref() != Some(&model) {
        out.records.push(Record::SetupStub(SetupStub {
            parts: parts.clone(),
            model: model.clone(),
            tool_names: tool_names(&parts, out),
        }));
        state.setup = sorted;
    }

    let written_at = f.u64("written_at_ms");
    let submission_id = f.string("submission_id");
    let input: Vec<Message> = f.parse("input")?.unwrap_or_default();
    // The session identity lives in the head, not in each run start.
    for key in ["session", "project_id"] {
        if let Some(value) = f.take(key) {
            tally(
                &mut out.dropped,
                format!("run_started.{key}"),
                json_len(&value),
            );
        }
    }
    let tags = f.take("tags");

    let mut start = RunStart {
        run_id: run_id.clone(),
        model: model.clone(),
        setup: parts,
        renderer_version: config.req_u64("renderer_version") as u32,
        submission_id: submission_id.clone(),
        send_id: f.string("send_id"),
        request_identity_digest: f.string("request_identity_digest"),
        ts_ms: f.u64("ts_ms"),
        written_at_ms: written_at,
        session_class,
        retention_secs: f.u64("retention"),
        title: f.string("title"),
        context_limit: config.u64("context_limit"),
        service_tier: config.string("service_tier"),
        route_decision_id: config.string("route_decision_id"),
        compaction_plan_version: f.u64("compaction_plan_version").map(|v| v as u32),
        render_config: config.take("frozen_render_config"),
        auth_selection: config.take("auth_selection"),
        run_choices: None,
        origin: f.take("origin"),
        principal: f.take("principal"),
        scope: f.take("scope"),
        hooks: f.take("hooks"),
        keep_warm: f.take("keep_warm"),
        on_restart: f.take("on_restart"),
        seeded: None,
        refusal_schedule: state
            .admitted_schedule
            .take()
            .map(|schedule| serde_json::to_value(schedule).expect("a schedule encodes")),
    };
    start.seeded = start
        .origin
        .as_ref()
        .is_some_and(|origin| origin.get("kind").and_then(Value::as_str) == Some("seeded_from"))
        .then_some(true);
    let mut choices = Map::new();
    if let Some(rest) = config.rest() {
        choices.insert("config".into(), rest);
    }
    config.finish(out);
    if let Some(rest) = manifest_rest {
        choices.insert("manifest".into(), rest);
    }
    if let Some(rest) = f.rest() {
        choices.insert("record".into(), rest);
    }
    start.run_choices = (!choices.is_empty()).then_some(Value::Object(choices));
    // Copies still held when a new run starts were never placed: today's conversation drops
    // those messages, and the records that accepted them keep them.
    state.held.clear();
    out.records.push(Record::RunStart(Box::new(start)));
    for (index, message) in input.into_iter().enumerate() {
        out.records.push(Record::Input(Input {
            run_id: run_id.clone(),
            placement: Placement::RunInput,
            message,
            written_at_ms: written_at,
            submission_id: submission_id.clone(),
            notice: None,
            tags_v1: tags_at(&tags, index),
            receipt_seq: None,
            pruned: Vec::new(),
        }));
    }
    state.run_id = run_id;
    state.model = Some(model);
    state.call_inputs.clear();
    Ok(())
}

fn model_step_finished(
    f: &mut Fields,
    state: &mut State,
    out: &mut Converted,
) -> Result<(), ConvertError> {
    let step_id = f.req_u64("step_id");
    let mut assistant = f
        .object("assistant_message")
        .ok_or_else(|| err("model_step_finished without assistant_message"))?;
    let message_id: MessageId = assistant
        .parse("message_id")?
        .unwrap_or(MessageId(String::new()));
    let content: Vec<ContentBlock> = assistant.parse("content")?.unwrap_or_default();
    assistant.finish(out);
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
    let usage: Option<Usage> = f.parse("usage")?;
    let model = state
        .model
        .clone()
        .ok_or_else(|| err("model step before any run start"))?;
    let finalize = std::mem::take(&mut state.finalize_step).then_some(true);
    out.records.push(Record::Assistant(Assistant {
        run_id: state.run_id.clone(),
        step_id,
        provider_message_id: message_id,
        message: Message::assistant_from(content, model),
        usage,
        written_at_ms: f.u64("written_at_ms"),
        last_content_at_ms: None,
        tags_v1: f.take("tags"),
        budget_finalize: finalize,
        pruned: Vec::new(),
    }));
    let finish_reason: Option<FinishReason> = f.parse("finish_reason")?;
    let retries_used = f.u64("retries_used").unwrap_or_default() as u32;
    let service_tier_used = f.string("service_tier_used");
    let speed_used = f.string("speed_used");
    let hooks = f.take("hooks");
    let fallback = f.rest();
    // A provider fallback inside the step served several model calls; `usage` holds the last
    // one's and the fallback's iterations hold each. The step's whole usage is their sum.
    let iterations: Option<Vec<broca_protocol::usage::UsageIteration>> = fallback
        .as_ref()
        .and_then(|fallback| fallback.get("usage_iterations"))
        .map(|value| serde_json::from_value(value.clone()))
        .transpose()
        .map_err(|e| err(format!("model_step_finished.usage_iterations: {e}")))?;
    let total = broca_protocol::usage::step_usage(usage.unwrap_or_default(), iterations.as_deref());
    out.records.push(Record::StepEnd(StepEnd {
        run_id: state.run_id.clone(),
        step_id,
        usage,
        finish_reason,
        retries_used,
        service_tier_used,
        speed_used,
        call_count: calls,
        hooks,
        fallback,
        budget_finalize: finalize,
        total_usage: (Some(total) != usage).then_some(total),
        stop_met: std::mem::take(&mut state.stop_met).then_some(true),
        limiting_stop_met: std::mem::take(&mut state.limiting_stop_met).then_some(true),
    }));
    Ok(())
}

fn digest_field(f: &mut Fields, key: &str) -> Digest {
    let text = f.req_string(key);
    match hex_digest(&text) {
        Some(digest) => digest,
        None => {
            f.lose(format!("{key} (not a SHA-256 hex digest)"), text.len());
            Digest([0; 32])
        }
    }
}

fn model_attempt(f: &mut Fields, state: &mut State, out: &mut Converted) {
    let mut billing = Map::new();
    for key in ["reservation", "envelope", "account_limit_disposition"] {
        if let Some(value) = f.take(key) {
            billing.insert(key.into(), value);
        }
    }
    let attempt = Attempt {
        run_id: state.run_id.clone(),
        step_id: f.req_u64("step_id"),
        attempt: f.req_u64("attempt") as u32,
        attempt_id: f.string("attempt_id"),
        outcome: f.req_string("outcome"),
        http_status: f.u64("http_status").map(|v| v as u32),
        request_body_len: f.req_u64("request_body_len"),
        request_body_sha256: digest_field(f, "request_body_sha256"),
        response_body_len: f.req_u64("response_body_len"),
        response_body_sha256: digest_field(f, "response_body_sha256"),
        ts_ms: f.req_u64("ts_ms"),
        sent_at_ms: f.u64("sent_at_ms"),
        first_content_at_ms: f.u64("first_content_at_ms"),
        last_content_at_ms: f.u64("last_content_at_ms"),
        transport: f.string("transport"),
        served_account_ref: f.string("served_account_ref"),
        billing: (!billing.is_empty()).then_some(Value::Object(billing)),
        telemetry: f.rest(),
    };
    out.records.push(Record::Attempt(attempt));
}

fn tool_intent(f: &mut Fields, state: &mut State, out: &mut Converted) -> Result<(), ConvertError> {
    let call: ToolCallId = f
        .parse("tool_call_id")?
        .ok_or_else(|| err("intent without tool_call_id"))?;
    let args = f.take("args").unwrap_or(Value::Null);
    let args_digest = sha256(&canonical(&args)?);
    let same_as_call = state.call_inputs.get(&call.0) == Some(&args);
    out.records.push(Record::ToolIntent(ToolIntent {
        run_id: state.run_id.clone(),
        batch_id: f.req_string("batch_id"),
        tool_call_id: call,
        tool_name: f.string("tool_name"),
        module_id: f.string("module_id"),
        args_digest,
        args: (!same_as_call).then_some(args),
        call_key: f.string("call_key"),
        approval_digest: f.string("approval_digest"),
        dispatched_at_ms: f.u64("dispatched_at_ms"),
        schema_pin: f.take("schema_pin"),
        hooks: f.take("hooks"),
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
    tags: Option<Value>,
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
        tags,
    } = terminal;
    let result_digest = sha256(&result.encode());
    let tool_call_id = result.tool_call_id.clone();
    let is_error = result.is_error;
    out.records.push(Record::ToolReply(ToolReply {
        run_id: state.run_id.clone(),
        batch_id: Some(batch_id.clone()),
        result,
        finished_at_ms,
        tags_v1: tags,
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

fn tool_result(f: &mut Fields, state: &mut State, out: &mut Converted) -> Result<(), ConvertError> {
    let result: ToolResult = f
        .parse("result")?
        .ok_or_else(|| err("tool_result without result"))?;
    let outcome = match f.string("outcome").as_deref() {
        Some("completed") | None => CallOutcome::Completed,
        Some("unknown") => CallOutcome::Unknown,
        Some("interrupted") => CallOutcome::Interrupted,
        Some("max_steps") => CallOutcome::MaxSteps,
        Some(other) => return Err(err(format!("unknown dispatch outcome {other}"))),
    };
    let terminal = Terminal {
        result,
        batch_id: f.req_string("batch_id"),
        outcome,
        finished_at_ms: f.u64("finished_at_ms"),
        reason: f.string("reason"),
        hooks: f.take("hooks"),
        tags: f.take("tags"),
        detail: None,
    };
    let terminal = Terminal {
        detail: f.rest(),
        ..terminal
    };
    reply_and_outcome(terminal, state, out);
    Ok(())
}

fn tool_no_dispatch(
    record: &WalRecord,
    state: &mut State,
    out: &mut Converted,
) -> Result<(), ConvertError> {
    // Destructured field by field, so a field added to the old record fails to compile here
    // instead of being dropped.
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
    reply_and_outcome(
        Terminal {
            result: broca_core::replay::no_dispatch_result(tool_call_id, reason),
            batch_id: batch_id.0.clone(),
            outcome: CallOutcome::NotDispatched,
            finished_at_ms: *dispatched_at_ms,
            reason: None,
            detail: serde_json::to_value(reason).ok(),
            hooks: hooks.as_ref().and_then(|h| serde_json::to_value(h).ok()),
            tags: tags.as_ref().and_then(|t| serde_json::to_value(t).ok()),
        },
        state,
        out,
    );
    Ok(())
}

/// A queued send placed into a running run. Its record keeps everything as written: the
/// delivery facts and the messages it delivered. The transcript gets copies: the messages where
/// the next step boundary places them, and text delivered with a tool result right away, which
/// is where the conversation takes it.
fn send_delivered(
    f: &mut Fields,
    state: &mut State,
    out: &mut Converted,
) -> Result<(), ConvertError> {
    let input: Option<Vec<Message>> = f.parse("input")?;
    let delivery = Delivery {
        submission_id: f.req_string("submission_id"),
        run_id: f.req_string("run_id"),
        mode: f.req_string("mode"),
        input_listed: input.is_some().then_some(true),
        input: input.unwrap_or_default(),
        tool_call_id: f.parse("tool_call_id")?,
        appended_text: f.string("appended_text"),
        written_at_ms: f.u64("written_at_ms"),
        tags_v1: f.take("tags"),
    };
    // A delivery always lands in the run it was recorded with, which is the current run.
    if delivery.run_id != state.run_id {
        f.lose("run_id (not the current run)".into(), 0);
    }
    let placement = match delivery.mode.as_str() {
        "steer" => Placement::Delivered,
        "turn_system" => Placement::DeliveredTurnSystem,
        // Delivered with a tool result: only `appended_text` below, no input message.
        "tool_result" if delivery.input.is_empty() => Placement::Delivered,
        other => {
            f.lose(format!("mode (with this input: {other})"), other.len());
            Placement::Delivered
        }
    };
    if let (Some(call), Some(text)) = (&delivery.tool_call_id, &delivery.appended_text) {
        out.records.push(Record::ResultAppend(ResultAppend {
            run_id: state.run_id.clone(),
            tool_call_id: call.clone(),
            text: text.clone(),
            notice: state.delivery_notices.get(&state.seq).cloned(),
            written_at_ms: delivery.written_at_ms,
        }));
    }
    let copies = copies_of(
        &delivery.input,
        &delivery.tags_v1,
        placement,
        delivery.written_at_ms,
        Some(delivery.submission_id.clone()),
    );
    hold(copies, state, out);
    out.records.push(Record::Delivery(delivery));
    Ok(())
}

#[cfg(test)]
mod tests;
