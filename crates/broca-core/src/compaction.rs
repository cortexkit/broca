//! Runner-owned compaction state and validation. Scheduling, fencing and cursor rules come
//! from the pinned role; native-message validation and durability belong to broca.

use crate::compaction_provider::{
    answer::{CompactionMessage, Range, StepAnswer},
    fence::{self, Disposition, FenceState},
    setup::{self, CallWhen},
    status::{AppliedRef, NotAppliedRef, StepStatus},
};
use broca_protocol::{ContentBlock, Message, Role, Usage};
use broca_wal::{
    CompactionCursor, CompactionDisposition, CompactionMessageKind, CompactionRange,
    CompactionStabilityRank, CompactionWait, CompactionWaitExit, OpaqueJson, RejectedCompaction,
    WalRecord,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

pub use broca_wal::MAX_REPLACEMENT_BYTES;
pub use runtime::{CompactionConfig, CompactionInbox, CompactionPlane, CompactionUnavailable};
pub(crate) mod runtime;
#[cfg(feature = "test-clock")]
pub mod test_control;

/// End-to-end route budget, including route open and retries, for Setup and step calls.
pub const COMPACTION_CALL_BUDGET_MS: u64 = 2_000;
/// A step cannot wait forever even when the provider repeatedly extends its bound.
pub const COMPACTION_STEP_WAIT_CAP_MS: u64 = 30_000;
/// Two bytes per token deliberately overcounts typical text/JSON's unmeasured input.
pub const ESTIMATE_BYTES_PER_TOKEN: u64 = 2;
/// Uncertain estimates near the window must not authorize sending an overflowing request.
pub const ESTIMATE_SAFETY_MARGIN_PERCENT: u64 = 10;

/// Absent checkpoint field means not captured; `None` means captured before Setup; `State`
/// carries everything needed to continue without consulting the provider or the old WAL.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", content = "value", rename_all = "snake_case")]
pub enum CompactionCheckpoint {
    None,
    State(Box<CompactionState>),
}
impl CompactionCheckpoint {
    /// The prefix length comes from canonical replay at this record, not from the
    /// replacement range or provider cursor (which can lag behind a capped page).
    pub fn fold_at(&mut self, record: &WalRecord, prefix_len: u64) {
        self.fold(record);
        if let Self::State(state) = self {
            state.capture_thinking_boundary(record, prefix_len);
        }
    }

    pub fn state(&self) -> Option<&CompactionState> {
        match self {
            Self::None => None,
            Self::State(state) => Some(state),
        }
    }
    pub fn fold(&mut self, record: &WalRecord) {
        if matches!(
            record,
            WalRecord::CompactionMessage {
                kind: CompactionMessageKind::Setup,
                ..
            }
        ) {
            *self = Self::State(Box::default());
        }
        if let Self::State(state) = self {
            state.fold(record);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AppliedCompaction {
    pub compaction_id: String,
    pub version: u64,
    pub range: CompactionRange,
    pub replacement: Arc<Vec<Message>>,
    pub sha256: Box<str>,
    /// Number of canonical messages present when this replacement was applied.
    /// Derived from WAL record order, not written into the WAL. Older materialized
    /// caches lack this field and must be rebuilt from canonical replay before reuse.
    #[serde(default = "unknown_thinking_boundary")]
    pub thinking_prefix_len: u64,
}

fn unknown_thinking_boundary() -> u64 {
    u64::MAX
}
impl AppliedCompaction {
    pub fn wire(&self) -> CompactionMessage {
        CompactionMessage::new(
            &self.compaction_id,
            self.version,
            Range::new(self.range.from, self.range.to),
            self.replacement
                .iter()
                .map(|m| serde_json::to_value(m).expect("native message JSON"))
                .collect(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NotAppliedCompaction {
    pub disposition: CompactionDisposition,
    pub rejected: RejectedCompaction,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenCompactionWait {
    pub request_id: String,
    pub wait: CompactionWait,
    /// Continuing exits close the wait; terminal exits remain until RunFinished seals it.
    pub terminal_exit: Option<CompactionWaitExit>,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CompactionState {
    pub history: CompactionHistory,
    pub provider: String,
    pub call_when: Option<OpaqueJson>,
    pub stability: Vec<CompactionStabilityRank>,
    pub applied: Option<AppliedCompaction>,
    pub last_not_applied: Option<NotAppliedCompaction>,
    pub cursor: Option<CompactionCursor>,
    pub newest_request_id: Option<String>,
    pub open_wait: Option<OpenCompactionWait>,
    /// An answer for an opened step, not yet consumed by a durable model completion.
    /// Recovery renders it without repeating a completed provider call.
    pub pending_answer: bool,
    pub recall_required: bool,
    /// An issued request with no durable outcome must be superseded on recovery, even
    /// when the original scheduling cause (such as a model switch) has already folded.
    pub pending_request: bool,
}
impl CompactionState {
    pub fn capture_thinking_boundary(&mut self, record: &WalRecord, prefix_len: u64) {
        if matches!(record, WalRecord::CompactionMessage { .. }) {
            if let Some(applied) = &mut self.applied {
                applied.thinking_prefix_len = prefix_len;
            }
        }
    }

    pub fn thinking_boundary_complete(&self) -> bool {
        self.applied
            .as_ref()
            .is_none_or(|applied| applied.thinking_prefix_len != u64::MAX)
    }

    pub fn fold(&mut self, record: &WalRecord) {
        self.history.fold(record);
        match record {
            WalRecord::CompactionRequest {
                provider,
                request_id,
            } => {
                self.provider.clone_from(provider);
                self.newest_request_id = Some(request_id.clone());
                self.recall_required = false;
                self.pending_request = true;
                self.pending_answer = false;
            }
            WalRecord::CompactionMessage {
                provider,
                kind,
                compaction_id,
                version,
                range,
                replacement,
                sha256,
                cursor,
                call_when,
                stability,
                ..
            } => {
                self.provider.clone_from(provider);
                if *kind == CompactionMessageKind::Setup {
                    self.call_when.clone_from(call_when);
                    self.stability = stability.clone().unwrap_or_default();
                } else {
                    self.cursor.clone_from(cursor);
                    self.pending_answer = true;
                    self.pending_request = false;
                }
                self.applied = Some(AppliedCompaction {
                    compaction_id: compaction_id.clone(),
                    version: *version,
                    range: *range,
                    replacement: Arc::clone(replacement),
                    sha256: sha256.clone().into_boxed_str(),
                    thinking_prefix_len: unknown_thinking_boundary(),
                });
                if self
                    .last_not_applied
                    .as_ref()
                    .is_some_and(|last| last.rejected.version <= *version)
                {
                    self.last_not_applied = None;
                }
            }
            WalRecord::CompactionAnswer {
                provider,
                request_id,
                cursor,
                disposition,
                rejected,
                wait,
            } => {
                self.provider.clone_from(provider);
                if !matches!(
                    disposition,
                    CompactionDisposition::Late | CompactionDisposition::SupersededRequest
                ) {
                    self.cursor = Some(cursor.clone());
                    self.pending_answer = true;
                    self.pending_request = false;
                }
                if let Some(rejected) = rejected {
                    self.last_not_applied = Some(NotAppliedCompaction {
                        disposition: *disposition,
                        rejected: rejected.clone(),
                    });
                }
                if *disposition == CompactionDisposition::Wait {
                    self.open_wait = wait.clone().map(|wait| OpenCompactionWait {
                        request_id: request_id.clone(),
                        wait,
                        terminal_exit: None,
                    });
                }
            }
            WalRecord::CompactionWait { exited, .. } => {
                if matches!(
                    exited,
                    CompactionWaitExit::Ready | CompactionWaitExit::Bound | CompactionWaitExit::Cap
                ) {
                    self.open_wait = None;
                    self.pending_answer = *exited == CompactionWaitExit::Cap;
                    self.recall_required = *exited != CompactionWaitExit::Cap;
                } else if let Some(wait) = &mut self.open_wait {
                    wait.terminal_exit = Some(*exited);
                }
            }
            WalRecord::RunFinished { .. } => {
                self.open_wait = None;
                self.pending_answer = false;
                self.pending_request = false;
            }
            WalRecord::StepStarted { .. } | WalRecord::ModelStepFinished { .. } => {
                self.pending_answer = false
            }
            _ => {}
        }
    }
    pub fn should_call(
        &self,
        model: &str,
        usage: Option<Usage>,
        window: Option<u64>,
        prefix_rebuilding: bool,
        execution_error: bool,
    ) -> bool {
        let condition = self
            .call_when
            .as_ref()
            .and_then(|json| serde_json::from_str::<CallWhen>(json.as_json_text()).ok());
        self.pending_request
            || self.recall_required
            || prefix_rebuilding
            || execution_error
            || setup::calls_every_step(condition.as_ref())
            || window.zip(usage).is_some_and(|(window, usage)| {
                window > 0
                    && usage.fill_tokens().unwrap_or(0) as f64 / window as f64
                        >= condition
                            .as_ref()
                            .expect("known condition")
                            .share_for(model)
            })
    }
    pub fn fill_status(&self, status: &mut StepStatus, raw: &[Message]) {
        status.after_ordinal = self
            .cursor
            .as_ref()
            .filter(|cursor| cursor.lineage_id == status.lineage_id)
            .and_then(|cursor| cursor.after_ordinal);
        let pending = raw
            .iter()
            .filter_map(|message| {
                let ordinal = message
                    .ordinal
                    .expect("full lineage is stamped before status assembly");
                status
                    .after_ordinal
                    .is_none_or(|after| ordinal > after)
                    .then(|| crate::compaction_provider::status::StatusMessage {
                        ordinal,
                        mid: format!("m{ordinal}"),
                        message: serde_json::to_value(message).expect("native message JSON"),
                        raw_message: None,
                    })
            })
            .collect::<Vec<_>>();
        let (count, more) = crate::compaction_provider::status::take_capped(
            &pending,
            crate::compaction_provider::status::DEFAULT_MESSAGE_CAP_BYTES,
        );
        status.messages = pending.into_iter().take(count).collect();
        status.more = more;
        status.newest = raw.last().and_then(|m| m.ordinal).map(|ordinal| {
            crate::compaction_provider::status::MessageRef {
                ordinal,
                mid: format!("m{ordinal}"),
            }
        });
        status.last_applied = self.applied.as_ref().map(|applied| AppliedRef {
            compaction_id: applied.compaction_id.clone(),
            version: applied.version,
        });
        status.last_not_applied = self
            .last_not_applied
            .as_ref()
            .filter(|last| {
                self.applied
                    .as_ref()
                    .is_none_or(|applied| last.rejected.version > applied.version)
            })
            .map(|last| {
                NotAppliedRef::new(
                    &last.rejected.compaction_id,
                    last.rejected.version,
                    disposition_name(last.disposition),
                )
            });
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CompactionHistory {
    pub previous_usage: Option<crate::compaction_provider::status::Usage>,
    /// Preserve unreported counters across checkpoints; the wire Usage cannot encode them.
    pub previous_native_usage: Option<Usage>,
    pub previous_provider_code: Option<String>,
    pub previous_model: Option<broca_protocol::ModelHandle>,
    pub current_model: Option<broca_protocol::ModelHandle>,
    pub completed_at: u64,
    pub previous_error: bool,
    pub current_request_bytes: Option<usize>,
    pub previous_request_bytes: Option<usize>,
}
impl CompactionHistory {
    pub fn fold(&mut self, record: &WalRecord) {
        match record {
            WalRecord::RunStarted { config, .. } => self.current_model = Some(config.model.clone()),
            WalRecord::ModelAttemptFinished {
                ts_ms,
                request_body_len,
                ..
            } => {
                self.completed_at = *ts_ms;
                self.current_request_bytes = usize::try_from(*request_body_len).ok();
            }
            WalRecord::ModelStepFinished {
                usage,
                finish_reason,
                ..
            } => {
                self.previous_native_usage = Some(*usage);
                self.previous_model.clone_from(&self.current_model);
                let finish_reason = finish_reason
                    .and_then(|reason| serde_json::to_value(reason).ok())
                    .and_then(|value| value.as_str().map(str::to_owned))
                    .unwrap_or_else(|| "unknown".into());
                self.previous_usage = wire_usage(*usage, self.completed_at, finish_reason);
                self.previous_provider_code = None;
                self.previous_error = false;
                self.previous_request_bytes = self.current_request_bytes;
            }
            WalRecord::RunFinished {
                reason: broca_protocol::RunFinishReason::Error,
                error: Some(error),
                ..
            } => {
                self.previous_error = true;
                self.previous_provider_code = error.provider_code.clone();
            }
            WalRecord::RunFinished {
                reason: broca_protocol::RunFinishReason::Error,
                ..
            } => self.previous_error = true,
            _ => {}
        }
    }
    pub fn usage(&self) -> Option<Usage> {
        self.previous_native_usage
    }
}

pub(crate) fn wire_usage(
    usage: Usage,
    completed_at: u64,
    finish_reason: String,
) -> Option<crate::compaction_provider::status::Usage> {
    Some(crate::compaction_provider::status::Usage {
        input: usage.input_tokens?,
        cache_read: usage.cached_input_tokens?,
        cache_write: usage.cache_write_tokens?,
        output: usage.output_tokens?,
        completed_at,
        finish_reason,
    })
}

/// Use the measured full occupancy, not just billed fresh tokens. The shared helper treats
/// absent cache splits as included in fresh input; only absent fresh input makes fill unknown.
/// A display estimate may fall back to byte conversion, but unknown measured input never
/// authorizes sending at a wait cap after a completed model step.
pub(crate) fn request_estimate(bytes: usize, history: &CompactionHistory) -> (u64, Option<u64>) {
    let measured = history.usage().and_then(|usage| usage.fill_tokens());
    // Without the old body length, the whole current body bounds bytes added since it.
    let previous = measured.map(|input| (input, history.previous_request_bytes.unwrap_or(0)));
    let estimate = estimate_tokens(bytes, previous);
    let first_request = history.previous_native_usage.is_none()
        && history.previous_usage.is_none()
        && history.current_request_bytes.is_none();
    let reliable = (first_request || measured.is_some()).then_some(estimate);
    (estimate, reliable)
}

pub fn disposition_name(disposition: CompactionDisposition) -> &'static str {
    match disposition {
        CompactionDisposition::Noop => "noop",
        CompactionDisposition::Wait => "wait",
        CompactionDisposition::Refuse => "refuse",
        CompactionDisposition::SupersededRequest => "superseded_request",
        CompactionDisposition::Late => "late",
        CompactionDisposition::RangeInverted => "range_inverted",
        CompactionDisposition::StaleVersion => "stale_version",
        CompactionDisposition::RangeBeyondNewest => "range_beyond_newest",
        CompactionDisposition::Structural => "structural",
        CompactionDisposition::FailedCall => "failed_call",
        CompactionDisposition::Timeout => "timeout",
    }
}

pub fn has_compaction(manifest: &broca_wal::PlanManifest) -> bool {
    serde_json::from_str::<Value>(manifest.plan.as_json_text())
        .ok()
        .is_some_and(|plan| {
            plan.get("compaction_item")
                .is_some_and(|item| !item.is_null())
        })
}

/// Size belongs to the message-on-its-own check, ahead of range inversion but behind fence
/// and deadline. Commons cannot validate broca's schema or byte ceiling, so we interpose
/// there using a noop probe, then delegate the remaining ordered checks to its fence.
pub fn validate_answer(
    fence: &FenceState,
    answer: &StepAnswer,
    arrived: u64,
    raw: &[Message],
) -> Result<Option<Vec<Message>>, CompactionDisposition> {
    let probe = StepAnswer::Noop {
        request_id: answer.request_id().into(),
        channel2_directive: None,
        bootstrap_complete: None,
    };
    let first = fence::dispose(fence, &probe, arrived);
    if let Disposition::NotApplied(reason) = first {
        return Err(disposition_from_name(reason.name()));
    }
    let StepAnswer::CompactionMessage { compaction, .. } = answer else {
        return Ok(None);
    };
    if serde_json::to_vec(&compaction.replacement)
        .map_or(true, |bytes| bytes.len() > MAX_REPLACEMENT_BYTES)
    {
        return Err(CompactionDisposition::Structural);
    }
    // Range inversion is the role's message-on-its-own rule. Native checks must not mask it.
    if compaction.check().is_err() {
        return Err(CompactionDisposition::RangeInverted);
    }
    let messages = native_replacement(compaction, raw)?;
    if serde_json::to_vec(&messages).map_or(true, |bytes| bytes.len() > MAX_REPLACEMENT_BYTES) {
        return Err(CompactionDisposition::Structural);
    }
    if let Disposition::NotApplied(reason) = fence::dispose(fence, answer, arrived) {
        return Err(disposition_from_name(reason.name()));
    }
    structural_conversation(compaction, &messages, raw)?;
    Ok(Some(messages))
}
fn disposition_from_name(name: &str) -> CompactionDisposition {
    match name {
        "superseded_request" => CompactionDisposition::SupersededRequest,
        "late" => CompactionDisposition::Late,
        "range_inverted" => CompactionDisposition::RangeInverted,
        "stale_version" => CompactionDisposition::StaleVersion,
        "range_beyond_newest" => CompactionDisposition::RangeBeyondNewest,
        _ => unreachable!("closed role fence reason"),
    }
}

fn keys(value: &Value, allowed: &[&str]) -> bool {
    value
        .as_object()
        .is_some_and(|object| object.keys().all(|key| allowed.contains(&key.as_str())))
}
fn native_fields(value: &Value) -> bool {
    if !keys(value, &["role", "content", "origin", "cache_prefix_blocks"]) {
        return false;
    }
    if let Some(origin) = value.get("origin").filter(|v| !v.is_null()) {
        if !keys(origin, &["provider_module_id", "model_id"]) {
            return false;
        }
    }
    value
        .get("content")
        .and_then(Value::as_array)
        .is_some_and(|blocks| {
            blocks
                .iter()
                .all(|block| match block.get("type").and_then(Value::as_str) {
                    Some("text") => keys(block, &["type", "text"]),
                    Some("reasoning") => keys(block, &["type", "text", "signature"]),
                    Some("redacted_reasoning") => keys(block, &["type", "data", "provider_format"]),
                    Some("tool_call") => keys(
                        block,
                        &[
                            "type",
                            "tool_call_id",
                            "tool_name",
                            "input",
                            "thought_signature",
                        ],
                    ),
                    Some("tool_result") => {
                        keys(block, &["type", "tool_call_id", "output", "is_error"])
                            && block.get("output").is_some_and(|output| {
                                match output.get("kind").and_then(Value::as_str) {
                                    Some("text") => keys(output, &["kind", "text"]),
                                    Some("json") => keys(output, &["kind", "value"]),
                                    Some("denied") => keys(output, &["kind", "reason"]),
                                    Some("error") => keys(output, &["kind", "message"]),
                                    _ => false,
                                }
                            })
                    }
                    _ => false,
                })
        })
}
fn encoded(message: &Message) -> Value {
    serde_json::to_value(message).expect("native JSON")
}
fn tool_ids(message: &Message) -> impl Iterator<Item = &str> {
    message.content.iter().filter_map(|block| match block {
        ContentBlock::ToolCall(call) => Some(call.tool_call_id.0.as_str()),
        ContentBlock::ToolResult(result) => Some(result.tool_call_id.0.as_str()),
        _ => None,
    })
}

/// Six broca rules: cache metadata; origin fidelity; signed reasoning fidelity; no system;
/// conversation/tool integrity; closed native fields. Opaque tool JSON remains opaque.
pub fn structural(
    compaction: &CompactionMessage,
    raw: &[Message],
) -> Result<Vec<Message>, CompactionDisposition> {
    let messages = native_replacement(compaction, raw)?;
    structural_conversation(compaction, &messages, raw)?;
    Ok(messages)
}
fn native_replacement(
    compaction: &CompactionMessage,
    raw: &[Message],
) -> Result<Vec<Message>, CompactionDisposition> {
    let bad = CompactionDisposition::Structural;
    let end = usize::try_from(compaction.range.to)
        .ok()
        .filter(|end| *end <= raw.len());
    let start = usize::try_from(compaction.range.from).ok();
    // Out-of-bounds is fenced later; don't mislabel it as structural just for indexing.
    let carried = start
        .zip(end)
        .and_then(|(start, end)| raw.get(start..end))
        .unwrap_or_default();
    let mut messages = Vec::with_capacity(compaction.replacement.len());
    for value in &compaction.replacement {
        if !native_fields(value) {
            return Err(bad);
        }
        let message: Message = serde_json::from_value(value.clone()).map_err(|_| bad)?;
        if message.cache_prefix_blocks.is_some() || message.role == Role::System {
            return Err(bad);
        }
        let unchanged = carried
            .iter()
            .any(|original| encoded(original) == encoded(&message));
        if !unchanged {
            if message.origin.is_some()
                || message.content.iter().any(|block| {
                    matches!(
                        block,
                        ContentBlock::Reasoning {
                            signature: Some(_),
                            ..
                        } | ContentBlock::RedactedReasoning { .. }
                    )
                })
            {
                return Err(bad);
            }
            // Removing an origin from otherwise unchanged content does not make it authored.
            if carried.iter().any(|original| {
                original.origin.is_some()
                    && original.role == message.role
                    && original.content == message.content
            }) {
                return Err(bad);
            }
        }
        messages.push(message);
    }
    Ok(messages)
}
fn structural_conversation(
    compaction: &CompactionMessage,
    messages: &[Message],
    raw: &[Message],
) -> Result<(), CompactionDisposition> {
    let bad = CompactionDisposition::Structural;
    let end = usize::try_from(compaction.range.to)
        .ok()
        .filter(|end| *end <= raw.len());
    if let Some(end) = end {
        // A raw call before `to` must have its result before `to`, even when a provider
        // copied the call into the replacement. Otherwise the cut separates its owner range.
        let mut pending = HashMap::<&str, usize>::new();
        for message in &raw[..end] {
            for block in &message.content {
                match block {
                    ContentBlock::ToolCall(call) => {
                        *pending.entry(call.tool_call_id.0.as_str()).or_default() += 1;
                    }
                    ContentBlock::ToolResult(result) => {
                        consume_result(&mut pending, result.tool_call_id.0.as_str());
                    }
                    _ => {}
                }
            }
        }
        if !pending.is_empty() {
            return Err(bad);
        }
        let outside: HashSet<_> = raw
            .iter()
            .enumerate()
            .filter(|(i, _)| (*i as u64) < compaction.range.from || *i >= end)
            .flat_map(|(_, m)| tool_ids(m))
            .collect();
        if messages
            .iter()
            .flat_map(tool_ids)
            .any(|id| outside.contains(id))
        {
            return Err(bad);
        }
        conversation(messages.iter().chain(&raw[end..]))?;
    } else {
        // Range is not usable yet; still validate the replacement on its own.
        conversation(messages.iter())?;
    }
    Ok(())
}
fn conversation<'a>(
    messages: impl Iterator<Item = &'a Message>,
) -> Result<(), CompactionDisposition> {
    let mut pending = HashMap::<&str, usize>::new();
    for message in messages {
        if !pending.is_empty() && message.role != Role::Tool {
            return Err(CompactionDisposition::Structural);
        }
        if message.role == Role::System {
            continue;
        }
        for block in &message.content {
            match block {
                ContentBlock::ToolCall(call) => {
                    if message.role != Role::Assistant {
                        return Err(CompactionDisposition::Structural);
                    }
                    *pending.entry(call.tool_call_id.0.as_str()).or_default() += 1;
                }
                ContentBlock::ToolResult(result) => {
                    if message.role != Role::Tool
                        || !consume_result(&mut pending, result.tool_call_id.0.as_str())
                    {
                        return Err(CompactionDisposition::Structural);
                    }
                }
                _ if message.role == Role::Tool => return Err(CompactionDisposition::Structural),
                _ => {}
            }
        }
    }
    if pending.is_empty() {
        Ok(())
    } else {
        Err(CompactionDisposition::Structural)
    }
}

fn consume_result<'a>(pending: &mut HashMap<&'a str, usize>, id: &'a str) -> bool {
    let Some(count) = pending.get_mut(id) else {
        return false;
    };
    *count -= 1;
    if *count == 0 {
        pending.remove(id);
    }
    true
}

/// Slice only the fully stamped, guidance-folded prompt. Replacement messages retain their
/// applied bytes and have no positional ordinal; raw tail ordinals are never restamped.
pub fn model_options(
    opts: &broca_protocol::CallOptions,
    state: &CompactionState,
) -> broca_protocol::CallOptions {
    let Some(applied) = &state.applied else {
        return opts.clone();
    };
    let mut prompt: Vec<_> = opts
        .prompt
        .iter()
        .filter(|m| m.role == Role::System)
        .cloned()
        .collect();
    let replacement_start = prompt.len();
    prompt.extend(applied.replacement.iter().cloned().map(|mut m| {
        m.ordinal = None;
        m
    }));
    let tail_start = prompt.len();
    prompt.extend(
        opts.prompt
            .iter()
            .filter(|m| m.role != Role::System && m.ordinal.is_some_and(|o| o >= applied.range.to))
            .cloned(),
    );
    let mut view = opts.with_prompt(prompt);
    view.declared_markers = state
        .stability
        .iter()
        .filter(|rank| (rank.index as usize) < applied.replacement.len())
        .map(|rank| broca_protocol::request::DeclaredMessageMarker {
            message_index: replacement_start + rank.index as usize,
            rank: rank.rank,
        })
        .collect();
    // The renderer's episode coordinates are indexes, unlike transcript ordinals. Map only
    // retained raw messages; replacement messages must not inherit episode decorations.
    view.render_episodes = Arc::new(broca_protocol::request::RenderHistory {
        compaction_prefix_len: Some(applied.thinking_prefix_len),
        episodes: opts
            .render_episodes
            .iter()
            .enumerate()
            .filter_map(|(index, episode)| {
                let end = opts
                    .render_episodes
                    .get(index + 1)
                    .map_or(usize::MAX, |next| next.prompt_start);
                let retained = view.prompt[tail_start..].iter().position(|m| {
                    m.ordinal
                        .is_some_and(|o| o >= episode.prompt_start as u64 && o < end as u64)
                })?;
                let first_user = episode
                    .first_user
                    .and_then(|first| {
                        view.prompt[tail_start..]
                            .iter()
                            .position(|m| m.ordinal == Some(first as u64))
                    })
                    .map(|i| tail_start + i);
                Some(broca_protocol::request::EpisodeRenderInput {
                    prompt_start: tail_start + retained,
                    first_user,
                    frozen_config: episode.frozen_config.clone(),
                })
            })
            .collect(),
    });
    view
}

pub fn estimate_tokens(rendered_bytes: usize, previous: Option<(u64, usize)>) -> u64 {
    let convert = |bytes: usize| (bytes as u64).div_ceil(ESTIMATE_BYTES_PER_TOKEN);
    previous.map_or_else(
        || convert(rendered_bytes),
        |(input, bytes)| input.saturating_add(convert(rendered_bytes.saturating_sub(bytes))),
    )
}
pub fn estimate_fits(estimate: Option<u64>, window: Option<u64>) -> bool {
    estimate.zip(window).is_some_and(|(estimate, window)| {
        u128::from(estimate) * 100
            < u128::from(window) * u128::from(100 - ESTIMATE_SAFETY_MARGIN_PERCENT)
    })
}

pub(crate) type CompactionItem = (
    String,
    Option<String>,
    Map<String, Value>,
    Map<String, Value>,
);
pub(crate) fn item(manifest: &broca_wal::PlanManifest) -> Result<Option<CompactionItem>, String> {
    let plan: Value =
        serde_json::from_str(manifest.plan.as_json_text()).map_err(|e| e.to_string())?;
    let Some(item) = plan.get("compaction_item").filter(|item| !item.is_null()) else {
        return Ok(None);
    };
    Ok(Some((
        item.get("provider")
            .and_then(Value::as_str)
            .ok_or("compaction provider missing")?
            .into(),
        item.get("preset")
            .and_then(Value::as_str)
            .map(str::to_owned),
        item.get("params")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default(),
        plan.get("composition")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default(),
    )))
}

#[cfg(test)]
mod tests;

pub(crate) fn now() -> u64 {
    crate::run::epoch_millis().unwrap_or(0)
}
