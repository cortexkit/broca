//! Runner-owned compaction state and validation.
//!
//! Compaction is asynchronous (compaction-provider/v1 0.3): Setup once per session, then the
//! provider's views, applied at step boundaries ([`view`]). Records the removed synchronous
//! client wrote (`CompactionRequest`, step `CompactionMessage`s, `CompactionAnswer`,
//! `CompactionWait`) are still folded here, so an old session replays and renders the bytes it
//! rendered before.
//!
//! Runner state lost (compaction-provider/v1 §5.3) cannot happen to broca on its own: tags,
//! the Setup answer, applied views and notices all live in the same write-ahead log as the
//! transcript, so none of them can be lost or restored separately from it. The next tag is the
//! WAL's own high-water plus one, and retention deletion carries a tag floor to the next
//! lineage. A new lineage starts with no compaction state at all: it runs Setup again,
//! renders Setup's initial message, and its notices carry no `descends_from` and no
//! `applied_view_version` until a view of that lineage applies. Its notice numbers continue
//! from the deleted lineage's, which retention deletion records on its tombstone beside the tag
//! floor. Restoring an older copy of a session's WAL by hand is not detected.

use crate::compaction_provider::message::{CompactionMessage, Range};
use broca_protocol::{ContentBlock, Message, Role, Usage};
use broca_wal::{
    CompactionMessageKind, CompactionRange, CompactionStabilityRank, CompactionStrip,
    CompactionWait, CompactionWaitExit, OpaqueJson, WalRecord,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

pub use broca_wal::MAX_REPLACEMENT_BYTES;
pub use runtime::{
    apply_idle_view, CompactionConfig, CompactionPlane, CompactionUnavailable, CompactionViews,
    ViewReadyInbox,
};
pub(crate) mod runtime;
#[cfg(feature = "test-clock")]
pub mod test_control;
pub mod view;

/// End-to-end route budget, including route open and retries, for a Setup call.
pub const COMPACTION_CALL_BUDGET_MS: u64 = 2_000;
/// Two bytes per token deliberately overcounts typical text/JSON's unmeasured input.
pub const ESTIMATE_BYTES_PER_TOKEN: u64 = 2;

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

/// A wait the removed synchronous client opened. Recovery still seals one left open by a crash,
/// without calling any provider (see `runtime::seal_recorded_wait`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenCompactionWait {
    pub request_id: String,
    pub wait: CompactionWait,
    /// Continuing exits close the wait; terminal exits remain until RunFinished seals it.
    pub terminal_exit: Option<CompactionWaitExit>,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CompactionState {
    /// The current step's wait or completed decision. Folded before any request can be sent.
    #[serde(default)]
    pub ceiling_check: Option<CeilingCheck>,
    pub history: CompactionHistory,
    pub provider: String,
    /// The `call_when` schedule a `compaction.setup` answer of the removed synchronous client
    /// froze (folded from its Setup record); no new record carries one.
    pub call_when: Option<OpaqueJson>,
    /// The stability ranks a `compaction.setup` answer of the removed synchronous client froze;
    /// the renderer still declares them on that session's applied replacement.
    pub stability: Vec<CompactionStabilityRank>,
    /// Setup's initial CompactionMessage, or the last replacement the synchronous client
    /// applied. Rendered until an asynchronous view applies.
    pub applied: Option<AppliedCompaction>,
    pub open_wait: Option<OpenCompactionWait>,
    /// Setup's ceiling policy, verbatim (`CeilingPolicy`); absent when Setup gave none.
    #[serde(default)]
    pub ceiling: Option<OpaqueJson>,
    /// The newest view applied at a step boundary; rendered instead of `applied`.
    #[serde(default)]
    pub view: Option<view::AppliedView>,
    /// Every companion strip applied in this lineage. Once signed reasoning is left out of a
    /// request it stays out, so the bytes later signatures were produced against stay put.
    #[serde(default)]
    pub strips: Vec<CompactionStrip>,
    /// The notice sequence of the newest recorded notice; 0 before the first.
    #[serde(default)]
    pub notice_seq: u64,
    /// What the newest recorded notice announced, to tell whether a boundary has news.
    #[serde(default)]
    pub announced: Option<view::Announced>,
    /// Event tokens owed to the next notice: companion edits of views applied since the
    /// newest notice. Folded from the WAL, so a crash between a view record and its notice
    /// still reports the companion edit.
    #[serde(default)]
    pub unannounced: Vec<String>,
    /// The Setup answer's tag checkpoint, for a session the provider moved from standalone
    /// Magic Context: tag numbering is anchored at its cut message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag_checkpoint: Option<broca_wal::SetupTagCheckpoint>,
}
impl CompactionState {
    /// Send admission uses the last durable request estimate plus the new native messages,
    /// as closing notices do. It does not call the compaction provider or mutate the session.
    /// A window override is checked afresh rather than reusing a previous terminal's detail.
    pub fn send_ceiling(
        &self,
        input: &[Message],
        window: Option<u64>,
        model: &broca_protocol::ModelHandle,
    ) -> Option<broca_protocol::ContextCeilingDetail> {
        if window.is_none()
            && self
                .history
                .current_model
                .as_ref()
                .is_some_and(|current| current != model)
        {
            return None;
        }
        let announced = self.announced.as_ref()?;
        let window = window.or(announced.window)?;
        let bytes: usize = input
            .iter()
            .filter(|m| m.role != Role::System)
            .map(|m| serde_json::to_vec(m).map_or(0, |b| b.len()))
            .sum();
        let size = announced
            .est_tokens?
            .saturating_add(estimate_tokens(bytes, None));
        match view::before_send(view::ceiling_policy(self).as_ref(), size, Some(window)) {
            view::BeforeSend::Send => None,
            view::BeforeSend::Refuse(detail) => Some(detail),
            view::BeforeSend::Pause => Some(broca_protocol::ContextCeilingDetail::new(
                window,
                size,
                crate::compaction_provider::ceiling::context_ceiling_causes::VIEW_TIMEOUT,
            )),
        }
    }
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

    /// The version of the view the session renders now, for notices.
    pub fn applied_view_version(&self, lineage_id: &str) -> Option<u64> {
        self.view
            .as_ref()
            .filter(|applied| applied.view.lineage_id == lineage_id)
            .map(|applied| applied.view.view_version)
    }

    pub fn fold(&mut self, record: &WalRecord) {
        self.history.fold(record);
        match record {
            WalRecord::RunStarted { .. } => self.ceiling_check = None,
            WalRecord::ContextCeilingWait {
                step_id,
                deadline_ms,
                unavailable,
            } => {
                self.ceiling_check = Some(CeilingCheck::Waiting {
                    step_id: *step_id,
                    deadline_ms: *deadline_ms,
                    unavailable: *unavailable,
                });
            }
            WalRecord::ContextCeilingDecided { step_id, detail } => {
                self.ceiling_check = Some(CeilingCheck::Decided {
                    step_id: *step_id,
                    detail: detail.clone(),
                });
            }
            WalRecord::CompactionRequest { provider, .. } => {
                self.provider.clone_from(provider);
            }
            WalRecord::CompactionMessage {
                provider,
                kind,
                compaction_id,
                version,
                range,
                replacement,
                sha256,
                call_when,
                stability,
                ceiling,
                tag_checkpoint,
                ..
            } => {
                self.provider.clone_from(provider);
                if *kind == CompactionMessageKind::Setup {
                    self.call_when.clone_from(call_when);
                    self.stability = stability.clone().unwrap_or_default();
                    self.ceiling.clone_from(ceiling);
                    self.tag_checkpoint.clone_from(tag_checkpoint);
                }
                self.applied = Some(AppliedCompaction {
                    compaction_id: compaction_id.clone(),
                    version: *version,
                    range: *range,
                    replacement: Arc::clone(replacement),
                    sha256: sha256.clone().into_boxed_str(),
                    thinking_prefix_len: unknown_thinking_boundary(),
                });
            }
            WalRecord::CompactionAnswer {
                provider,
                request_id,
                disposition,
                wait,
                ..
            } => {
                self.provider.clone_from(provider);
                if *disposition == broca_wal::CompactionDisposition::Wait {
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
                } else if let Some(wait) = &mut self.open_wait {
                    wait.terminal_exit = Some(*exited);
                }
            }
            WalRecord::CompactionViewApplied {
                provider,
                view,
                choice,
                ..
            } => {
                self.provider.clone_from(provider);
                // The writer only records a view it decoded, so a view that does not decode
                // here is a corrupt WAL. Keep rendering what was rendered before rather than
                // guess at the record's meaning.
                if let Some(applied) = view::AppliedView::decode(view, choice) {
                    if let broca_wal::CompactionViewChoice::Companion { edit, strip } = choice {
                        self.strips.push(*strip);
                        self.unannounced.push(
                            crate::compaction_provider::notice::Event::ViewCompanion {
                                view_version: applied.view.view_version,
                                edit: edit.clone(),
                            }
                            .token(),
                        );
                    }
                    self.view = Some(applied);
                }
            }
            WalRecord::CompactionNotice { seq, headers } => {
                self.notice_seq = *seq;
                self.announced = Some(view::Announced::from_headers(headers));
                self.unannounced.clear();
            }
            WalRecord::RunFinished { .. } => {
                if matches!(self.ceiling_check, Some(CeilingCheck::Waiting { .. })) {
                    self.ceiling_check = None;
                }
                self.open_wait = None;
            }
            _ => {}
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CeilingCheck {
    Waiting {
        step_id: broca_protocol::StepId,
        deadline_ms: u64,
        unavailable: Option<bool>,
    },
    Decided {
        step_id: broca_protocol::StepId,
        detail: Option<broca_protocol::ContextCeilingDetail>,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CompactionHistory {
    /// The last completed model step's measured usage.
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
            WalRecord::ModelStepFinished { usage, .. } => {
                self.previous_native_usage = Some(*usage);
                self.previous_model.clone_from(&self.current_model);
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

/// Use the measured full occupancy, not just billed fresh tokens. The shared helper treats
/// absent cache splits as included in fresh input; only absent fresh input makes fill unknown.
/// A display estimate may fall back to byte conversion, but unknown measured input never
/// authorizes sending at a wait cap after a completed model step.
pub(crate) fn request_estimate(bytes: usize, history: &CompactionHistory) -> (u64, Option<u64>) {
    let measured = history.usage().and_then(|usage| usage.fill_tokens());
    // Without the old body length, the whole current body bounds bytes added since it.
    let previous = measured.map(|input| (input, history.previous_request_bytes.unwrap_or(0)));
    let estimate = estimate_tokens(bytes, previous);
    let first_request =
        history.previous_native_usage.is_none() && history.current_request_bytes.is_none();
    let reliable = (first_request || measured.is_some()).then_some(estimate);
    (estimate, reliable)
}

pub fn has_compaction(manifest: &broca_wal::PlanManifest) -> bool {
    serde_json::from_str::<Value>(manifest.plan.as_json_text())
        .ok()
        .is_some_and(|plan| {
            plan.get("compaction_item")
                .is_some_and(|item| !item.is_null())
        })
}

/// Check Setup's initial CompactionMessage against broca's message schema and the transcript
/// it is rendered over, and return its messages. `None` when it cannot be rendered, which
/// Setup treats as a failed call (compaction-provider/v1 §4).
pub fn validate_initial(initial: &CompactionMessage, raw: &[Message]) -> Option<Vec<Message>> {
    if serde_json::to_vec(&initial.replacement)
        .map_or(true, |bytes| bytes.len() > MAX_REPLACEMENT_BYTES)
    {
        return None;
    }
    // Range inversion is the role's message-on-its-own rule.
    initial.check().ok()?;
    if initial.range.to > raw.len() as u64 {
        return None;
    }
    let messages = native_replacement(initial, raw).ok()?;
    structural_conversation(initial, &messages, raw).ok()?;
    Some(messages)
}

/// Why broca's schema or conversation rules refuse a replacement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Structural;

fn keys(value: &Value, allowed: &[&str]) -> bool {
    value
        .as_object()
        .is_some_and(|object| object.keys().all(|key| allowed.contains(&key.as_str())))
}
pub(crate) fn native_fields(value: &Value) -> bool {
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
                    Some("text_with_state") => {
                        keys(block, &["type", "text", "provider_state"])
                            && block
                                .get("provider_state")
                                .is_some_and(|state| keys(state, &["format", "data"]))
                    }
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
    #[cfg(test)]
    carried_encoding_tests::note_encoded();
    serde_json::to_value(message).expect("native JSON")
}
fn tool_ids(message: &Message) -> impl Iterator<Item = &str> {
    message.content.iter().filter_map(|block| match block {
        ContentBlock::ToolCall(call) => Some(call.tool_call_id.0.as_str()),
        ContentBlock::ToolResult(result) => Some(result.tool_call_id.0.as_str()),
        _ => None,
    })
}

fn native_replacement(
    compaction: &CompactionMessage,
    raw: &[Message],
) -> Result<Vec<Message>, Structural> {
    let bad = Structural;
    let end = usize::try_from(compaction.range.to)
        .ok()
        .filter(|end| *end <= raw.len());
    let start = usize::try_from(compaction.range.from).ok();
    // Out-of-bounds is fenced later; don't mislabel it as structural just for indexing.
    let carried = CarriedMessages::new(
        start
            .zip(end)
            .and_then(|(start, end)| raw.get(start..end))
            .unwrap_or_default(),
    );
    let mut messages = Vec::with_capacity(compaction.replacement.len());
    for value in &compaction.replacement {
        messages.push(native_message(value, &carried).ok_or(bad)?);
    }
    Ok(messages)
}

/// The transcript messages a replacement stands in for, each encoded once so that judging every
/// replacement message against them costs one encoding per message on either side rather than
/// two per comparison.
pub(crate) struct CarriedMessages<'a> {
    messages: &'a [Message],
    encoded: Vec<Value>,
}

impl<'a> CarriedMessages<'a> {
    pub(crate) fn new(messages: &'a [Message]) -> Self {
        Self {
            messages,
            encoded: messages.iter().map(encoded).collect(),
        }
    }
}

/// One provider-written message in broca's schema. A message that is not a byte-identical
/// copy of one in `carried` (the transcript messages it stands in for) is authored by the
/// provider, and may then carry no model origin, signed or encrypted reasoning, or
/// provider-signed text: those would claim the model wrote what the provider wrote.
pub(crate) fn native_message(value: &Value, carried: &CarriedMessages<'_>) -> Option<Message> {
    let message = native_schema_message(value)?;
    let candidate = encoded(&message);
    let unchanged = carried.encoded.contains(&candidate);
    if !unchanged {
        if message.origin.is_some() || message.content.iter().any(is_signed) {
            return None;
        }
        // Removing an origin from otherwise unchanged content does not make it authored.
        if carried.messages.iter().any(|original| {
            original.origin.is_some()
                && original.role == message.role
                && original.content == message.content
        }) {
            return None;
        }
    }
    Some(message)
}

/// One provider-written message in broca's schema, without judging who wrote its content: no
/// unknown members, no cache marker, and not a system message.
pub(crate) fn native_schema_message(value: &Value) -> Option<Message> {
    if !native_fields(value) {
        return None;
    }
    let message: Message = serde_json::from_value(value.clone()).ok()?;
    if message.cache_prefix_blocks.is_some() || message.role == Role::System {
        return None;
    }
    Some(message)
}

/// Signed or encrypted model output a model provider checks against the request's prefix, or
/// that only the model may author.
pub(crate) fn is_signed(block: &ContentBlock) -> bool {
    matches!(
        block,
        ContentBlock::Reasoning {
            signature: Some(_),
            ..
        } | ContentBlock::RedactedReasoning { .. }
            | ContentBlock::TextWithState { .. }
    )
}
fn structural_conversation(
    compaction: &CompactionMessage,
    messages: &[Message],
    raw: &[Message],
) -> Result<(), Structural> {
    let bad = Structural;
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
        conversation(messages.iter().chain(&raw[end..])).map_err(|_| bad)?;
    } else {
        // Range is not usable yet; still validate the replacement on its own.
        conversation(messages.iter()).map_err(|_| bad)?;
    }
    Ok(())
}

/// Why a history breaks broca's conversation rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Conversation {
    /// A tool call without its result, or a result without its call.
    ToolPairSplit,
    /// A message in a role the place it stands does not allow.
    RoleOrder,
}

pub(crate) fn conversation<'a>(
    messages: impl Iterator<Item = &'a Message>,
) -> Result<(), Conversation> {
    let mut pending = HashMap::<&str, usize>::new();
    for message in messages {
        if !pending.is_empty() && message.role != Role::Tool {
            return Err(Conversation::ToolPairSplit);
        }
        if message.role == Role::System {
            continue;
        }
        for block in &message.content {
            match block {
                ContentBlock::ToolCall(call) => {
                    if message.role != Role::Assistant {
                        return Err(Conversation::RoleOrder);
                    }
                    *pending.entry(call.tool_call_id.0.as_str()).or_default() += 1;
                }
                ContentBlock::ToolResult(result) => {
                    if message.role != Role::Tool {
                        return Err(Conversation::RoleOrder);
                    }
                    if !consume_result(&mut pending, result.tool_call_id.0.as_str()) {
                        return Err(Conversation::ToolPairSplit);
                    }
                }
                _ if message.role == Role::Tool => return Err(Conversation::RoleOrder),
                _ => {}
            }
        }
    }
    if pending.is_empty() {
        Ok(())
    } else {
        Err(Conversation::ToolPairSplit)
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
///
/// A session with an applied asynchronous view renders that view ([`view::render`]);
/// otherwise Setup's initial message, or the replacement the synchronous client last applied,
/// stands in for its range, exactly as before views existed.
pub fn model_options(
    opts: &broca_protocol::CallOptions,
    state: &CompactionState,
) -> Result<broca_protocol::CallOptions, view::ViewNotRenderable> {
    let systems: Vec<_> = opts
        .prompt
        .iter()
        .enumerate()
        .filter_map(|(i, m)| (m.role == Role::System).then_some(i))
        .collect();
    model_options_indexed(opts, state, &systems)
}

pub(crate) fn model_options_indexed(
    opts: &broca_protocol::CallOptions,
    state: &CompactionState,
    systems: &[usize],
) -> Result<broca_protocol::CallOptions, view::ViewNotRenderable> {
    if let Some(view) = view::render(opts, state, systems) {
        return view;
    }
    let Some(applied) = &state.applied else {
        return Ok(opts.clone());
    };
    let mut prompt: Vec<_> = systems.iter().map(|&i| opts.prompt[i].clone()).collect();
    let replacement_start = prompt.len();
    prompt.extend(applied.replacement.iter().cloned().map(|mut m| {
        m.ordinal = None;
        m
    }));
    let tail_start = prompt.len();
    prompt.extend(
        view::ordinal_range(&opts.prompt, applied.range.to, u64::MAX)
            .iter()
            .filter(|m| m.role != Role::System)
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
        episodes: retained_episodes(opts, &view.prompt, tail_start),
    });
    Ok(view)
}

/// Translate only episodes represented by retained transcript messages. Searching the episode
/// boundaries from each retained ordinal avoids walking episodes hidden by a compaction view.
fn retained_episodes(
    opts: &broca_protocol::CallOptions,
    prompt: &[Message],
    start: usize,
) -> Vec<broca_protocol::request::EpisodeRenderInput> {
    let mut kept = BTreeMap::new();
    let mut positions = BTreeMap::new();
    for (position, message) in prompt.iter().enumerate().skip(start) {
        let Some(ordinal) = message.ordinal else {
            continue;
        };
        positions.entry(ordinal).or_insert(position);
        let end = opts
            .render_episodes
            .partition_point(|episode| episode.prompt_start as u64 <= ordinal);
        if let Some(index) = end.checked_sub(1) {
            kept.entry(index).or_insert(position);
        }
    }
    kept.into_iter()
        .map(|(index, position)| {
            let episode = &opts.render_episodes[index];
            broca_protocol::request::EpisodeRenderInput {
                prompt_start: position,
                first_user: episode
                    .first_user
                    .and_then(|first| positions.get(&(first as u64)).copied()),
                frozen_config: episode.frozen_config.clone(),
            }
        })
        .collect()
}

/// One boundary's rendered model input and its exact wire size. Only an accepted view can
/// change this input during a ceiling wait; notices and wait records do not change it.
#[derive(Default)]
pub(crate) struct BoundaryRequest {
    options: Option<broca_protocol::CallOptions>,
    view_version: Option<u64>,
    pub(crate) bytes: usize,
}

impl BoundaryRequest {
    pub(crate) fn refresh(
        &mut self,
        opts: &broca_protocol::CallOptions,
        state: &CompactionState,
        systems: &[usize],
        render: impl FnOnce(&broca_protocol::CallOptions) -> Vec<u8>,
    ) -> Result<(), view::ViewNotRenderable> {
        let version = state.view.as_ref().map(|view| view.view.view_version);
        if self.options.is_some() && self.view_version == version {
            return Ok(());
        }
        let options = model_options_indexed(opts, state, systems)?;
        self.bytes = render(&options).len();
        self.options = Some(options);
        self.view_version = version;
        Ok(())
    }

    pub(crate) fn into_options(self) -> broca_protocol::CallOptions {
        self.options.expect("boundary request was rendered")
    }
}

pub fn estimate_tokens(rendered_bytes: usize, previous: Option<(u64, usize)>) -> u64 {
    let convert = |bytes: usize| (bytes as u64).div_ceil(ESTIMATE_BYTES_PER_TOKEN);
    previous.map_or_else(
        || convert(rendered_bytes),
        |(input, bytes)| {
            if rendered_bytes < bytes {
                // A new view can shrink the request. Scale the measured input down with its
                // rendered size instead of keeping the old request's token count as a floor.
                (u128::from(input) * rendered_bytes as u128).div_ceil(bytes as u128) as u64
            } else {
                input.saturating_add(convert(rendered_bytes.saturating_sub(bytes)))
            }
        },
    )
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

#[cfg(test)]
mod carried_encoding_tests {
    use super::*;

    thread_local! {
        /// Messages encoded to JSON values on this thread by `encoded`.
        static ENCODED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }

    pub(super) fn note_encoded() {
        ENCODED.with(|cell| cell.set(cell.get() + 1));
    }

    fn text(role: Role, text: String) -> Message {
        Message::new(role, vec![ContentBlock::Text { text }])
    }

    /// Judging a replacement against the range it replaces encodes each carried message once and
    /// each replacement message once. Encoding both sides for every comparison made a large
    /// replacement over a large range allocate a JSON value tree per pair while a step waited.
    #[test]
    fn replacement_validation_encodes_each_message_once() {
        const CARRIED: usize = 20;
        const REPLACEMENT: usize = 10;
        let raw: Vec<Message> = (0..CARRIED)
            .map(|index| {
                let role = if index % 2 == 0 {
                    Role::User
                } else {
                    Role::Assistant
                };
                text(role, format!("carried {index}"))
            })
            .collect();
        // Provider-authored summaries: none is a copy of a carried message, so each one is
        // compared against the whole range.
        let replacement: Vec<Value> = (0..REPLACEMENT)
            .map(|index| {
                serde_json::to_value(text(Role::User, format!("summary {index}"))).unwrap()
            })
            .collect();
        let message = CompactionMessage::new("c", 1, Range::new(0, CARRIED as u64), replacement);

        ENCODED.with(|cell| cell.set(0));
        let messages = native_replacement(&message, &raw).expect("summaries are valid");
        let encodings = ENCODED.with(std::cell::Cell::get);

        assert_eq!(messages.len(), REPLACEMENT);
        assert_eq!(
            encodings,
            CARRIED + REPLACEMENT,
            "each carried and each replacement message is encoded exactly once"
        );
    }

    /// A replacement message identical to a carried one is recognized as carried, so it may keep
    /// the model origin it had in the transcript.
    #[test]
    fn a_carried_copy_keeps_its_model_origin() {
        let mut carried = text(Role::Assistant, "kept".into());
        carried.origin = Some(broca_protocol::request::ModelHandle {
            provider_module_id: "provider".into(),
            model_id: "model".into(),
        });
        let raw = vec![text(Role::User, "ask".into()), carried.clone()];
        let copy = serde_json::to_value(&carried).unwrap();
        let message = CompactionMessage::new("c", 1, Range::new(0, 2), vec![copy]);

        let messages = native_replacement(&message, &raw).expect("a carried copy is valid");

        assert_eq!(messages, vec![carried]);
    }
}
