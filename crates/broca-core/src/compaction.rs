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
//! `applied_view_version` until a view of that lineage applies. Restoring an older copy of a
//! session's WAL by hand is not detected.

use crate::compaction_provider::message::{CompactionMessage, Range};
use broca_protocol::{ContentBlock, Message, Role, Usage};
use broca_wal::{
    CompactionMessageKind, CompactionRange, CompactionStabilityRank, CompactionStrip,
    CompactionWait, CompactionWaitExit, OpaqueJson, WalRecord,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

pub use broca_wal::MAX_REPLACEMENT_BYTES;
pub use runtime::{CompactionConfig, CompactionPlane, CompactionUnavailable, CompactionViews};
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
                ..
            } => {
                self.provider.clone_from(provider);
                if *kind == CompactionMessageKind::Setup {
                    self.call_when.clone_from(call_when);
                    self.stability = stability.clone().unwrap_or_default();
                    self.ceiling.clone_from(ceiling);
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
                self.open_wait = None;
            }
            _ => {}
        }
    }
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
    let carried = start
        .zip(end)
        .and_then(|(start, end)| raw.get(start..end))
        .unwrap_or_default();
    let mut messages = Vec::with_capacity(compaction.replacement.len());
    for value in &compaction.replacement {
        messages.push(native_message(value, carried).ok_or(bad)?);
    }
    Ok(messages)
}

/// One provider-written message in broca's schema. A message that is not a byte-identical
/// copy of one in `carried` (the transcript messages it stands in for) is authored by the
/// provider, and may then carry no model origin, signed or encrypted reasoning, or
/// provider-signed text: those would claim the model wrote what the provider wrote.
pub(crate) fn native_message(value: &Value, carried: &[Message]) -> Option<Message> {
    if !native_fields(value) {
        return None;
    }
    let message: Message = serde_json::from_value(value.clone()).ok()?;
    if message.cache_prefix_blocks.is_some() || message.role == Role::System {
        return None;
    }
    let unchanged = carried
        .iter()
        .any(|original| encoded(original) == encoded(&message));
    if !unchanged {
        if message.origin.is_some() || message.content.iter().any(is_signed) {
            return None;
        }
        // Removing an origin from otherwise unchanged content does not make it authored.
        if carried.iter().any(|original| {
            original.origin.is_some()
                && original.role == message.role
                && original.content == message.content
        }) {
            return None;
        }
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
) -> broca_protocol::CallOptions {
    if let Some(view) = view::render(opts, state) {
        return view;
    }
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
