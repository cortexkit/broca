//! The runner's side of asynchronous compaction views (compaction-provider/v1 §5 to §8):
//! deciding what to do with a fetched view, assembling the history a request sends under it,
//! the strip-or-defer rule for signed reasoning, the context ceiling, and advance notices.
//!
//! Everything here is a pure function of the session's folded [`CompactionState`] and its
//! transcript. The run loop records each decision in the log (`CompactionViewApplied`,
//! `CompactionNotice`) before anything renders or publishes it, so a resumed run renders the
//! same bytes without asking the provider again.

use broca_protocol::{tags::part_key, ContentBlock, Message, Role};
use broca_records::{record::CompactionStrip, record::CompactionViewChoice};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{is_signed, native_message, native_schema_message, CompactionState};
use {
    crate::compaction_provider::view::CompactionView, crate::compaction_provider::view::ViewProblem,
};

/// A view the session applied, and how.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AppliedView {
    pub view: CompactionView,
    pub choice: CompactionViewChoice,
}

/// What the newest recorded notice announced: enough to tell whether a later boundary has
/// anything new to say.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Announced {
    #[serde(default)]
    pub est_tokens: Option<u64>,
    #[serde(default)]
    pub window: Option<u64>,
    pub lineage_id: Option<String>,
    pub through_ordinal: Option<u64>,
    pub applied_view_version: Option<u64>,
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history_edit_cost: Option<String>,
    #[serde(default, skip_serializing_if = "hidden_service_tier")]
    pub service_tier: Option<String>,
}

fn hidden_service_tier(value: &Option<String>) -> bool {
    !broca_protocol::service_tier_enabled() || value.is_none()
}

/// The transcript messages of `prompt`: every message with an ordinal, in order, except
/// system messages, which the runner places ahead of the history itself (as it does under
/// Setup's initial message).
fn transcript(prompt: &[Message]) -> impl DoubleEndedIterator<Item = &Message> {
    prompt
        .iter()
        .filter(|message| message.ordinal.is_some() && message.role != Role::System)
}

/// What [`rendered_history`] renders from a state that holds no compaction, when the caller
/// keeps the set of references the session may render itself: the transcript messages of
/// `prompt`, with each medium outside that set replaced by its provenance marker.
pub(crate) fn uncompacted_history(
    prompt: &[Message],
    captured: impl Fn(&broca_protocol::MediaRef) -> bool,
) -> Vec<Message> {
    let mut history: Vec<Message> = transcript(prompt).cloned().collect();
    super::media::enforce_by(&mut history, captured);
    history
}

/// Locate a half-open ordinal range in the stamped, append-only prompt. Unstamped system
/// messages, when present, precede the transcript and are not part of an ordinal range.
pub(super) fn ordinal_range(prompt: &[Message], from: u64, to: u64) -> &[Message] {
    let start = prompt.partition_point(|m| m.ordinal.is_none_or(|o| o < from));
    let end = start + prompt[start..].partition_point(|m| m.ordinal.is_some_and(|o| o < to));
    &prompt[start..end]
}

/// Why a view cannot be assembled over the transcript.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssemblyProblem {
    /// Bytes that are not messages or parts in broca's schema; the view is rejected.
    Rejected(ViewProblem),
    /// Messages or parts the compaction provider wrote itself that carry signed or encrypted
    /// reasoning, provider-signed text or a model origin, which only the model may produce
    /// (a byte-identical copy of the transcript is allowed); the view is refused
    /// `provider_constraint`.
    ProviderConstraint,
    /// A renderer or negotiation refusal, named by compaction-provider/v1 §6.1.
    Refused(&'static str),
}

fn assemble_captured(
    view: &CompactionView,
    prompt: &[Message],
    captured: &super::CapturedMedia,
    features: &[String],
) -> Result<Vec<Message>, AssemblyProblem> {
    let mut history = assemble_judged(view, prompt, None, features)?;
    captured.enforce(&mut history);
    Ok(history)
}

/// [`assemble`], also judging who wrote the view's bytes when `stored` is given: the transcript
/// as stored, which is what `session.read` returns and what a provider copies from (the prompt
/// shows the same messages with runner tags and loop guidance folded into their text). A
/// replace-range message or an edited part that carries signed or encrypted reasoning,
/// provider-signed text or a model origin must be a byte-identical copy of the stored message
/// or part it stands for; otherwise only the model could have written it.
fn assemble_judged(
    view: &CompactionView,
    prompt: &[Message],
    stored: Option<super::StoredTranscript<'_>>,
    features: &[String],
) -> Result<Vec<Message>, AssemblyProblem> {
    use crate::compaction_provider::view::ViewRange;
    let stored_at = |ordinal: u64| stored.and_then(|stored| stored.at(ordinal));
    let in_range = |from: u64, to: u64| transcript(ordinal_range(prompt, from, to));
    let mut out = Vec::new();
    for range in &view.ranges {
        match range {
            ViewRange::Keep { from, to, edits } => {
                for message in in_range(*from, *to) {
                    let ordinal = message.ordinal.expect("transcript message");
                    let mine: Vec<_> = edits.iter().filter(|e| e.ordinal == ordinal).collect();
                    if mine.is_empty() {
                        out.push(message.clone());
                        continue;
                    }
                    let mut edited = message.clone();
                    edited.content.clear();
                    for (index, block) in message.content.iter().enumerate() {
                        let key = part_key(ordinal, index);
                        let selected = mine.iter().find(|edit| edit.part == key);
                        let source = stored_at(ordinal).unwrap_or(message);
                        let container = if source.role == Role::Tool {
                            broca_protocol::tags::v1::PartContainer::ToolResult
                        } else {
                            broca_protocol::tags::v1::PartContainer::Other
                        };
                        if let Some(edit) = selected {
                            edit.check_container(container)
                                .map_err(AssemblyProblem::Rejected)?;
                        }
                        match selected {
                            None => edited.content.push(block.clone()),
                            Some(edit) if edit.drop => {}
                            Some(edit) => {
                                let original = stored
                                    .is_some()
                                    .then(|| stored_at(ordinal).and_then(|m| m.content.get(index)));
                                let number = message
                                    .tags
                                    .get()
                                    .filter(|_| message.tags.is_rendered())
                                    .and_then(|tags| tags.parts.get(index).copied().flatten());
                                let rendered;
                                let bytes = if let Some(mode) = &edit.truncate {
                                    if !features.iter().any(|f| f == crate::compaction_provider::setup::VIEW_FEATURE_TRUNCATE) {
                                        return Err(AssemblyProblem::Refused("truncate_kind"));
                                    }
                                    let part = serde_json::to_value(
                                        source.content.get(index).unwrap_or(block),
                                    )
                                    .expect("stored content JSON");
                                    let part = broca_protocol::tags::v1::truncate_part(
                                        "owned-broca",
                                        &part,
                                        container,
                                        mode.clone(),
                                        number,
                                        edit.drop_attachments,
                                    )
                                    .map_err(|refusal| {
                                        AssemblyProblem::Refused(refusal.reason())
                                    })?;
                                    rendered = serde_json::to_string(&part)
                                        .expect("rendered content JSON");
                                    rendered.as_str()
                                } else {
                                    edit.bytes.as_deref().unwrap_or_default()
                                };
                                // Truncation is runner-authored by the shared renderer, which
                                // preserves signed fields and cannot introduce media references.
                                // Provider-authored bytes still need the original-part checks.
                                let authorship = if edit.truncate.is_some() {
                                    None
                                } else {
                                    original
                                };
                                let mut part = edited_part(bytes, authorship, message.role)?;
                                if number.is_some() && (edit.truncate.is_some() || features.iter().any(|f| f == crate::compaction_provider::setup::VIEW_FEATURE_TAG_BYTES_EDITS)) {
                                        let mut probe = Message::new(message.role, vec![part]);
                                        let mut tags = message.tags.get().expect("numbered part").clone();
                                        tags.parts = vec![number];
                                        // These bytes are authored by the runner or compaction
                                        // provider, not the model. Keep their inline references
                                        // while rendering the original part's own tag.
                                        if let Some(computed) = &mut tags.computed {
                                            computed.clean = false;
                                        }
                                        probe.tags = broca_protocol::tags::TagSlot::new(Some(std::sync::Arc::new(tags)));
                                        broca_protocol::tags::render_tags_into(std::slice::from_mut(&mut probe), broca_protocol::tags::TagRendering { assistant_text: true, ..Default::default() });
                                        part = probe.content.remove(0);
                                }
                                edited.content.push(part);
                            }
                        }
                    }
                    out.push(edited);
                }
            }
            ViewRange::Replace { from, to, bytes } => {
                let values: Vec<Value> = serde_json::from_str(bytes)
                    .map_err(|_| AssemblyProblem::Rejected(ViewProblem::ReplacementNotArray))?;
                let candidates: Vec<_> = values.iter().filter_map(native_schema_message).collect();
                let carried: Option<Vec<Message>> = stored.map(|stored| {
                    if !candidates.iter().any(|candidate| {
                        candidate.origin.is_some()
                            || candidate.content.iter().any(super::is_signed)
                            || stored.has_model_origin(candidate.role)
                            || super::media_refs(&candidate.content).next().is_some()
                    }) {
                        return Vec::new();
                    }
                    stored
                        .range(*from, *to)
                        // Keep possible signed copies and media sources without cloning unrelated
                        // transcript text. Media can be reused without copying the whole message.
                        .filter(|original| {
                            candidates.iter().any(|candidate| {
                                (candidate.role == original.role
                                    && candidate.content == original.content
                                    && (original.origin.is_some()
                                        || candidate.origin.is_some()
                                        || candidate.content.iter().any(super::is_signed)))
                                    || super::media_refs(&candidate.content).any(|reference| {
                                        super::media_refs(&original.content)
                                            .any(|original| original == reference)
                                    })
                            })
                        })
                        .cloned()
                        .collect()
                });
                let carried = carried
                    .as_deref()
                    .map(crate::compaction::CarriedMessages::new);
                for value in &values {
                    let message = match &carried {
                        Some(carried) => native_message(value, carried),
                        None => native_schema_message(value),
                    };
                    let mut message = message.ok_or_else(|| replacement_problem(value))?;
                    message.ordinal = None;
                    out.push(message);
                }
            }
            // A range kind this build does not know cannot reach here: views decode strictly
            // on `kind`, so such a view never decodes.
            _ => return Err(AssemblyProblem::Rejected(ViewProblem::Inverted)),
        }
    }
    let covered = view.covers_through.ordinal;
    out.extend(transcript(ordinal_range(prompt, covered.saturating_add(1), u64::MAX)).cloned());
    Ok(out)
}

/// A replacement message broca refuses: bytes that are not a message in broca's schema are
/// rejected; a message in the schema that violates provenance or authorship is a provider constraint.
fn replacement_problem(value: &Value) -> AssemblyProblem {
    match native_schema_message(value) {
        Some(message)
            if message.origin.is_some()
                || message.content.iter().any(is_signed)
                || super::media_refs(&message.content).next().is_some() =>
        {
            AssemblyProblem::ProviderConstraint
        }
        _ => AssemblyProblem::Rejected(ViewProblem::ReplacementNotArray),
    }
}

/// One edited part: `bytes` decoded as a content block of broca's schema. A block that only
/// the model may author (signed or encrypted reasoning, provider-signed text) is accepted only
/// as a byte-identical copy of the part it replaces.
/// Text may be edited freely or replace media; each reference that remains must match a
/// reference in the original stored part, never merely another capture in the session.
/// `original` is the stored part the edit replaces, when the caller judges authorship
/// (`Some(None)` when there is no such stored part); `None` to skip that judgement.
fn edited_part(
    bytes: &str,
    original: Option<Option<&ContentBlock>>,
    role: Role,
) -> Result<ContentBlock, AssemblyProblem> {
    let value: Value = serde_json::from_str(bytes)
        .map_err(|_| AssemblyProblem::Rejected(ViewProblem::EditNotJson))?;
    // Reuse the whole-message schema check on a one-part message of the same role.
    let probe = serde_json::json!({"role": role, "content": [value]});
    let block: ContentBlock = match serde_json::from_value::<Message>(probe.clone()) {
        Ok(mut message) if super::native_fields(&probe) => message.content.remove(0),
        _ => return Err(AssemblyProblem::Rejected(ViewProblem::EditNotJson)),
    };
    if let Some(original) = original {
        if is_signed(&block) && original != Some(&block) {
            return Err(AssemblyProblem::ProviderConstraint);
        }
        if !super::media_refs(std::slice::from_ref(&block)).all(|reference| {
            original.is_some_and(|original| {
                super::media_refs(std::slice::from_ref(original)).any(|stored| stored == reference)
            })
        }) {
            return Err(AssemblyProblem::ProviderConstraint);
        }
    }
    Ok(block)
}

/// Remove signed reasoning from every transcript message whose ordinal lies in one of
/// `strips`, and, when `copies`, from every message the provider wrote into the view (those
/// carry no ordinal): a copy of a signed message stands behind a changed prefix by
/// construction.
pub fn apply_strips(history: &mut [Message], strips: &[CompactionStrip], copies: bool) {
    for message in history {
        let stripped = match message.ordinal {
            None => copies,
            Some(ordinal) => strips
                .iter()
                .any(|strip| ordinal >= strip.from_ordinal && ordinal < strip.to_ordinal),
        };
        if stripped {
            message.content.retain(|block| !is_prefix_bound(block));
        }
    }
}

/// Whether the applied view went with the companion strip, which also covers the signed
/// reasoning of the messages the provider copied into it.
fn strips_copies(state: &CompactionState) -> bool {
    state
        .view
        .as_ref()
        .is_some_and(|applied| matches!(applied.choice, CompactionViewChoice::Companion { .. }))
}

/// Signed or encrypted reasoning: the blocks a model provider that binds reasoning to its
/// prefix rejects once the bytes before them change. The same blocks the renderer leaves out
/// of the prefix of a replacement the synchronous client applied.
pub fn is_prefix_bound(block: &ContentBlock) -> bool {
    matches!(
        block,
        ContentBlock::Reasoning {
            signature: Some(_),
            ..
        } | ContentBlock::RedactedReasoning { .. }
    )
}

/// The history the session renders now, before any newer view: the applied view, or else
/// Setup's initial CompactionMessage (or a replacement the synchronous client applied) in
/// place of its range, or else the transcript as written.
pub fn rendered_history(state: &CompactionState, prompt: &[Message]) -> Vec<Message> {
    if state.view_due.is_some() {
        return Vec::new();
    }
    if let Some(applied) = &state.view {
        if let Ok(mut history) = assemble_captured(
            &applied.view,
            prompt,
            &state.captured,
            state.view_features.as_deref().unwrap_or_default(),
        ) {
            apply_strips(&mut history, &state.strips, strips_copies(state));
            return history;
        }
    }
    let mut history: Vec<Message> = match &state.applied {
        Some(applied) => applied
            .replacement
            .iter()
            .cloned()
            .map(|mut message| {
                message.ordinal = None;
                message
            })
            .chain(
                transcript(prompt)
                    .filter(|m| m.ordinal.is_some_and(|o| o >= applied.range.to))
                    .cloned(),
            )
            .collect(),
        None => transcript(prompt).cloned().collect(),
    };
    state.captured.enforce(&mut history);
    history
}

/// What the runner does with a fetched view at a step boundary.
#[derive(Debug, Clone, PartialEq)]
pub enum Decision {
    /// Record it (`CompactionViewApplied`) with this choice, and render it from this step on.
    Apply(CompactionViewChoice),
    /// Hold it and keep rendering the current view; report `view_deferred:<version>:<reason>`.
    Defer(&'static str),
    /// Never render it and keep the current view; report `view_refused:<version>:<reason>`.
    Refuse(&'static str),
    /// Its coverage is not a prefix of this transcript. broca never truncates a transcript,
    /// so this is a view of another lineage, which the provider leaves behind once it reads
    /// the new one. Dropped without a report: `view_discarded` is defined only beside a
    /// `revert_below`, and broca has no revert to report.
    Discard,
    /// Not newer than the view rendered now; ignored.
    Stale,
    /// Not the view asked for, malformed, or not in broca's message schema. Not recorded and
    /// not reported; the current view stays.
    Rejected(ViewProblem),
    /// A newer version is known: announced and receipted while this one was fetched, or
    /// answered by the provider in its place. Not recorded and not reported; the caller
    /// decides the newest version instead, so one boundary rewrites the prefix only once.
    Superseded,
}

/// A recorded view that does not assemble over the prompt being rendered. It assembled when it
/// was applied, over the same prompt, so this means the prompt changed under it. Rendering
/// anything else (Setup's head, no strips) would send bytes no recorded decision covers, so it
/// is an error, never a fallback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewNotRenderable {
    pub view_version: u64,
    pub problem: AssemblyProblem,
}

impl std::fmt::Display for ViewNotRenderable {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "applied compaction view {} no longer assembles over the prompt: {:?}",
            self.view_version, self.problem
        )
    }
}

/// The context limit a model provider's refusal states, when its text states one:
/// Anthropic's `prompt is too long: N tokens > M maximum` and OpenAI's `maximum context
/// length is M tokens`. Anything else gives `None`; the limit is never guessed or taken from
/// the model catalog.
pub fn reported_limit_tokens(message: &str) -> Option<u64> {
    fn leading_number(text: &str) -> Option<u64> {
        let digits: String = text
            .trim_start()
            .chars()
            .take_while(char::is_ascii_digit)
            .collect();
        digits.parse().ok()
    }
    if let Some(rest) = message
        .find("prompt is too long:")
        .map(|at| &message[at + "prompt is too long:".len()..])
    {
        // `N tokens > M maximum`: the limit is the number after `>`.
        let limit = rest.split_once('>').map(|(_, after)| after)?;
        let number = leading_number(limit)?;
        return limit
            .trim_start()
            .trim_start_matches(|c: char| c.is_ascii_digit())
            .trim_start()
            .starts_with("maximum")
            .then_some(number);
    }
    let rest = message
        .find("maximum context length is")
        .map(|at| &message[at + "maximum context length is".len()..])?;
    let number = leading_number(rest)?;
    rest.trim_start()
        .trim_start_matches(|c: char| c.is_ascii_digit())
        .trim_start()
        .starts_with("tokens")
        .then_some(number)
}

/// The size of the request a model provider refused as too long, when its text states one:
/// Anthropic's `prompt is too long: N tokens > M maximum` and OpenAI's `your messages resulted
/// in N tokens`. Anything else gives `None`; the size is never guessed.
pub fn reported_prompt_tokens(message: &str) -> Option<u64> {
    let after = |marker: &str| {
        message
            .find(marker)
            .map(|at| message[at + marker.len()..].trim_start())
    };
    let rest = after("prompt is too long:").or_else(|| after("resulted in"))?;
    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    let number = digits.parse().ok()?;
    rest[digits.len()..]
        .trim_start()
        .starts_with("tokens")
        .then_some(number)
}

/// The opaque prompt-cache label broca sends for a provider account
/// (compaction-provider/v1 §5, §8): the first 32 lowercase hex characters (128 bits) of
/// SHA-256 over the vault-served account id's UTF-8 bytes. Hashing keeps the label opaque:
/// it never is nor contains a credential, token, email address or anything identifying the
/// person, while equal accounts give equal labels and different accounts give different
/// labels. The 32-hex form fits the contract's `[A-Za-z0-9._-]{1,64}` grammar.
///
/// Pure, so the run loop and the log fold share it: the same vault account id always maps
/// to the same label within a runner.
pub fn account_label(account_ref: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(account_ref.as_bytes());
    let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    hex[..32].to_owned()
}

/// The account label for a run's frozen auth selection, or `None` when broca has no
/// account fact and must send neither the ride cause nor the event.
///
/// Known only for subscription accounts (every method but `apikey`) whose selection carries a
/// routing decision and a vault-served account id. No routing selection (no selection at
/// all, as on the mock path; no `account_decision`, as on old records) or an unknown account
/// (an `apikey` method, an unknown method token, or no vault account id) is unknown. A
/// placeholder is never sent: unknown stays `None`.
pub fn selection_account_label(selection: &broca_records::record::AuthSelection) -> Option<String> {
    let method = match selection.auth_method.as_deref() {
        Some(method) => method,
        None => selection.credential_id.split_once(':')?.0,
    };
    if !broca_protocol::AuthMethod::from_token(method)?.is_subscription() {
        return None;
    }
    selection.account_decision.as_ref()?;
    let account_ref = selection
        .account_ref
        .as_deref()
        .filter(|value| !value.is_empty())?;
    Some(account_label(account_ref))
}

#[cfg(test)]
#[path = "view_tests.rs"]
mod tests;
