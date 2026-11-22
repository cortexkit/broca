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

use broca_protocol::{ContentBlock, Message, Role, Usage};
use broca_records::{
    record::CompactionMessageKind, record::CompactionRange, record::CompactionStabilityRank,
    record::CompactionStrip, record::CompactionWait, record::CompactionWaitExit,
    record::OpaqueJson, record::WalRecord,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

pub use broca_records::record::MAX_REPLACEMENT_BYTES;
mod media;
pub use media::CapturedMedia;
mod stored;
pub use runtime::CompactionConfig;
pub use runtime::{
    CompactionPlane, CompactionUnavailable, CompactionViews, ProviderCapabilities, ViewReadyInbox,
};
pub(crate) use stored::{OriginalMessages, StoredTranscript};
pub(crate) mod runtime;
#[cfg(feature = "test-clock")]
pub mod test_control;
pub mod view;

/// End-to-end route budget, including route open and retries, for a Setup call.
pub const COMPACTION_CALL_BUDGET_MS: u64 = 2_000;

/// Absent checkpoint field means not captured; `None` and `Captures` precede Setup; `State`
/// carries everything needed to continue without consulting the provider or the old WAL.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", content = "value", rename_all = "snake_case")]
pub enum CompactionCheckpoint {
    None,
    Captures(CapturedMedia),
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
            Self::None | Self::Captures(_) => None,
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
            let captured = self.captured().cloned().unwrap_or_default();
            *self = Self::State(Box::new(CompactionState {
                captured,
                ..Default::default()
            }));
        }
        match self {
            Self::State(state) => state.fold(record),
            Self::Captures(captured) => captured.fold(record),
            Self::None => {
                if CapturedMedia::captures_any(record) {
                    let mut captured = CapturedMedia::default();
                    captured.fold(record);
                    if !captured.is_empty() {
                        *self = Self::Captures(captured);
                    }
                }
            }
        }
    }

    pub(crate) fn captured(&self) -> Option<&CapturedMedia> {
        match self {
            Self::None => None,
            Self::Captures(captured) => Some(captured),
            Self::State(state) => Some(&state.captured),
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
impl AppliedCompaction {}

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
    /// Captures survive view replacement and sparse resume; provider content never adds entries.
    /// Required on decode so older materialized states replay instead of silently losing captures.
    pub captured: CapturedMedia,
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
    pub tag_checkpoint: Option<broca_records::record::SetupTagCheckpoint>,
    /// The Setup answer's `ride_wait_ms` (compaction-provider 0.5.1 §4), frozen with the
    /// session: the deadline of every rewrite ride. Absent when the answer carried none, and
    /// the runner then uses its own short ride deadline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ride_wait_ms: Option<u64>,
    /// The feature tokens declared by this lineage's recorded Setup, never inferred on resume.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub view_features: Option<Vec<String>>,
    /// Retried before every model step until this exact published view applies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub view_due: Option<broca_records::record::SetupViewDue>,
    /// The rewrite ride the current step waits for, once its wait is reported
    /// (`CompactionRideWait`): the step and the absolute deadline. Cleared when it resolves.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ride_wait: Option<(broca_protocol::StepId, u64)>,
    /// The current run's provider-account label, from its frozen auth selection when that
    /// names a subscription account with a vault-served account id. `None` when broca has
    /// no account fact (no routing selection, or an unrouted credential). Updated when a
    /// run starts and when its auth refreezes; folded from `RunStarted` and
    /// `RunAuthRefrozen` so a restart rebuilds it from the WAL instead of memory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_account: Option<String>,
    /// The account label the newest recorded notice attributed, `None` before the first
    /// notice with a known account. Set when a notice is recorded from the run that wrote
    /// it when that run's account is known; left in place when that run's account is
    /// unknown, so a later known run compares against the last known account. Durable
    /// through the WAL (folded from notices) and checkpoints, so a restart neither rides
    /// spuriously nor misses a change.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub announced_account: Option<String>,
    /// Recovered from the frozen render bundle before each model step; not checkpointed.
    #[serde(skip)]
    pub current_service_tier: Option<String>,
}
impl CompactionState {
    pub fn capture_thinking_boundary(&mut self, record: &WalRecord, prefix_len: u64) {
        if matches!(record, WalRecord::CompactionMessage { .. }) {
            if let Some(applied) = &mut self.applied {
                applied.thinking_prefix_len = prefix_len;
            }
        }
    }

    pub fn fold(&mut self, record: &WalRecord) {
        self.captured.fold(record);
        self.history.fold(record);
        match record {
            WalRecord::RunStarted { config, .. } => {
                self.ceiling_check = None;
                self.ride_wait = None;
                // The run's frozen account, when it names a subscription account with a
                // vault-served account id. Unknown selections leave the previous known
                // account in place only by not clearing it here: an unknown run has no
                // account fact, so `current_account` becomes `None` while
                // `announced_account` keeps the last known account for the next known run
                // to compare against.
                self.current_account = config
                    .auth_selection
                    .as_ref()
                    .and_then(view::selection_account_label);
            }
            WalRecord::RunAuthRefrozen { auth_selection, .. } => {
                self.current_account = view::selection_account_label(auth_selection);
            }
            WalRecord::CompactionRideWait {
                step_id,
                deadline_ms,
            } => self.ride_wait = Some((*step_id, *deadline_ms)),
            // A wait ends when its ride resolves, and never outlives its step or its run.
            WalRecord::CompactionRideWaitEnded { .. }
            | WalRecord::ModelStepFinished { .. }
            | WalRecord::RunPaused { .. } => self.ride_wait = None,
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
                ride_wait_ms,
                view_features,
                view_due,
                ..
            } => {
                self.provider.clone_from(provider);
                if *kind == CompactionMessageKind::Setup {
                    self.call_when.clone_from(call_when);
                    self.stability = stability.clone().unwrap_or_default();
                    self.ceiling.clone_from(ceiling);
                    self.tag_checkpoint.clone_from(tag_checkpoint);
                    self.ride_wait_ms = *ride_wait_ms;
                    self.view_features.clone_from(view_features);
                    self.view_due.clone_from(view_due);
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
                if *disposition == broca_records::record::CompactionDisposition::Wait {
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
                    if self.view_due.as_ref().is_some_and(|due| {
                        due.version == applied.view.view_version
                            && due.digest == applied.view.digest
                    }) {
                        self.view_due = None;
                    }
                    if let broca_records::record::CompactionViewChoice::Companion { edit, strip } =
                        choice
                    {
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
                // The last notice's account is the run's account when it was recorded,
                // when that run's account is known. Unknown runs leave the last known
                // account in place, so a later known run compares against it rather than
                // against nothing. The first notice with a known account therefore baselines
                // without an event: with no prior notice there is no change.
                if let Some(current) = self.current_account.clone() {
                    self.announced_account = Some(current);
                }
            }
            WalRecord::RunFinished { .. } => {
                if matches!(self.ceiling_check, Some(CeilingCheck::Waiting { .. })) {
                    self.ceiling_check = None;
                }
                self.open_wait = None;
                self.ride_wait = None;
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
            WalRecord::ModelStepFinished {
                usage, fallback, ..
            } => {
                let requested = fallback
                    .route
                    .as_ref()
                    .map(|route| &route.model)
                    .or(self.current_model.as_ref());
                // Checkpoints retain one prompt measurement, not the sum of prompts
                // sent to every serving model during a server-side handoff.
                let native = requested.and_then(|requested| {
                    broca_protocol::usage::model_usage(
                        fallback
                            .served_model
                            .as_deref()
                            .unwrap_or(&requested.model_id),
                        *usage,
                        fallback.usage_iterations.as_deref(),
                    )
                    .last()
                    .map(|(model, usage)| {
                        (
                            broca_protocol::ModelHandle::new(&requested.provider_module_id, *model),
                            *usage,
                        )
                    })
                });
                self.previous_native_usage = native.as_ref().map(|(_, usage)| *usage);
                self.previous_model = native.map(|(model, _)| model);
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
}

pub fn has_compaction(manifest: &broca_records::record::PlanManifest) -> bool {
    serde_json::from_str::<Value>(manifest.plan.as_json_text())
        .ok()
        .is_some_and(|plan| {
            plan.get("compaction_item")
                .is_some_and(|item| !item.is_null())
        })
}

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
                    Some("media") => keys(
                        block,
                        &[
                            "type",
                            "kind",
                            "mime",
                            "byte_size",
                            "sha256",
                            "width",
                            "height",
                        ],
                    ),
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
                                    Some("parts") => keys(output, &["kind", "parts"]),
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
/// Every media reference must equal a reference in the carried range, including its metadata.
pub(crate) fn native_message(value: &Value, carried: &CarriedMessages<'_>) -> Option<Message> {
    let message = native_schema_message(value)?;
    if !media_refs(&message.content).all(|reference| {
        carried
            .messages
            .iter()
            .flat_map(|message| media_refs(&message.content))
            .any(|stored| stored == reference)
    }) {
        return None;
    }
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

/// References are compared as complete metadata, not just digests: a provider cannot change
/// the kind, size or dimensions of an object the session captured.
pub(crate) fn media_refs(
    blocks: &[ContentBlock],
) -> impl Iterator<Item = &broca_protocol::MediaRef> {
    use broca_protocol::{ToolOutput, ToolOutputPart};
    blocks.iter().flat_map(|block| {
        let direct = match block {
            ContentBlock::Media(media) => Some(media),
            _ => None,
        };
        let parts = match block {
            ContentBlock::ToolResult(broca_protocol::ToolResult {
                output: ToolOutput::Parts { parts },
                ..
            }) => parts.as_slice(),
            _ => &[],
        };
        direct
            .into_iter()
            .chain(parts.iter().filter_map(|part| match part {
                ToolOutputPart::Media { media } => Some(media),
                ToolOutputPart::Text { .. } => None,
            }))
    })
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
    state.captured.enforce(&mut prompt);
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
        raw_ordinals: false,
        compaction_prefix_len: Some(applied.thinking_prefix_len),
        signed_floor: opts.render_episodes.signed_floor,
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

pub type CompactionItem = (
    String,
    Option<String>,
    Map<String, Value>,
    Map<String, Value>,
);

pub fn item(
    manifest: &broca_records::record::PlanManifest,
) -> Result<Option<CompactionItem>, String> {
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

#[cfg(test)]
mod carried_encoding_tests {

    thread_local! {
        /// Messages encoded to JSON values on this thread by `encoded`.
        static ENCODED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }

    pub(super) fn note_encoded() {
        ENCODED.with(|cell| cell.set(cell.get() + 1));
    }
}
