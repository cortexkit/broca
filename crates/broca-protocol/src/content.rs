//! Canonical message + content-block types. The request `Prompt` and the
//! assembled assistant message share these blocks. Mirrors the Vercel
//! `LanguageModelV4` Prompt shape, adapted to subc-routed (client-executed)
//! tools. Serialized internally-tagged (`{"type": ...}`) so the wire is
//! self-describing and easy to read.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ids::{MessageId, ToolCallId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    System,
    User,
    Assistant,
    Tool,
}

/// One message in a `Prompt` (`Vec<Message>`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub role: Role,
    pub content: Vec<ContentBlock>,
    /// Number of leading content blocks that form a caller-declared cacheable prefix.
    ///
    /// This render metadata stays beside the content it describes so queueing and replay retain
    /// the same marker boundary. Providers without intra-message cache markers ignore it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_prefix_blocks: Option<u32>,
    /// The model that PRODUCED this message — the producing `(provider, model)`, set on
    /// assistant messages, `None` for user/tool/system (and for any message whose origin
    /// is unknown). It is a per-message render INPUT, NOT request content: a renderer that
    /// echoes provider-signed state (Anthropic signed thinking) compares each assistant
    /// message's origin against the run's CURRENT model and strips signatures produced by a
    /// DIFFERENT model — echoing a cross-model signature is a hard provider error. Derived
    /// deterministically at replay from each episode's frozen run config (so it reproduces
    /// byte-identically on resume), so it never enters a request's rendered bytes and is
    /// skipped when absent (a message with no origin renders exactly as before).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<crate::request::ModelHandle>,
    /// This message's durable position in the full, UNTRIMMED lineage-ordered prompt (0 for
    /// the first message, incrementing by one per message across all episodes). A stable
    /// per-message coordinate used by an external reduction transform to key its durable
    /// per-message state — NOT request content and NOT part of the rendered bytes.
    ///
    /// It is `#[serde(skip)]`: pure in-memory position metadata, RE-DERIVED by position every
    /// time a prompt is finalized (via [`stamp_ordinals`]), never persisted and never
    /// serialized into a request. So it can never drift and never affects cache/resume
    /// byte-identity (no renderer reads it; it is absent from the canonical bytes).
    ///
    /// DURABLE ONLY UNDER AN APPEND-ONLY PROMPT: the position is stable because the lineage
    /// prompt only ever grows at the end (the WAL is append-only; an incomplete turn is held
    /// out only at the tail; a fork copies the prefix verbatim). A FUTURE feature that edits
    /// or removes a mid-lineage message (a doctor-edit of an indeterminate turn, redaction,
    /// history surgery) would shift every later position and silently mis-key the transform's
    /// durable state — so such a feature MUST tombstone-in-place (preserve positions) or fork
    /// a new lineage, NEVER splice a message out.
    #[serde(skip)]
    pub ordinal: Option<u64>,
    /// The runner tags of this message's parts (see [`crate::tags`]). In-memory
    /// only, like `ordinal`: never serialized, so it never enters a rendered request or the
    /// bytes of a stored message, and ignored by equality. Replay attaches it from the WAL
    /// records that carry the tags, and reads from the store attach it from the row beside the
    /// message.
    #[serde(skip)]
    pub tags: crate::tags::TagSlot,
}

impl Message {
    /// A message with no producing-model origin (user/tool/system input, or any message
    /// whose origin is not tracked). The common constructor for input + test messages.
    pub fn new(role: Role, content: Vec<ContentBlock>) -> Self {
        Self {
            role,
            content,
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
        }
    }

    /// An assistant message stamped with the model that produced it (the producing
    /// `(provider, model)`), so a cross-model signed-reasoning echo can be detected at
    /// render.
    pub fn assistant_from(content: Vec<ContentBlock>, origin: crate::request::ModelHandle) -> Self {
        Self {
            role: Role::Assistant,
            content,
            cache_prefix_blocks: None,
            origin: Some(origin),
            ordinal: None,
            tags: Default::default(),
        }
    }

    /// Attach (or clear) this message's runner tags. Rendering state starts over.
    pub fn set_tags(&mut self, tags: Option<std::sync::Arc<crate::tags::PartTags>>) {
        self.tags = crate::tags::TagSlot::new(tags);
    }

    /// This message's runner tags, when its session has them.
    pub fn part_tags(&self) -> Option<&crate::tags::PartTags> {
        self.tags.get()
    }

    /// The opaque per-message identity token (`m<ordinal>`) an external reduction transform
    /// keys on — a formatting of [`Self::ordinal`], so the two share one durable coordinate.
    /// `None` until the message's ordinal is stamped. The `#`-free format is deliberate: a
    /// consumer composes block ids as `<mid>#<block_index>`, so a `#` in the mid would break
    /// that separator.
    ///
    /// THIS IDENTITY IS POSITIONAL, AND ITS VALIDITY DEPENDS ON THE TRANSCRIPT BEING
    /// APPEND-ONLY. External consumers now key delta reads on it: a client asks for messages
    /// after a given mid and skips re-fetching the rest, which is only sound because the
    /// message at a given ordinal can never become a DIFFERENT message. Positional identity
    /// was rejected for other transcript sources in that contract for exactly this reason and
    /// accepted here only on the append-only property.
    ///
    /// THE CONSUMER IS NAMED, because a general warning gets read as a general warning. The
    /// fleet's prefix-claim contract (2026-09-20) sends `{heldFloorMessageId, sinceMessageId,
    /// heldVersion}` and expects one of three positive replies: matched-with-turns,
    /// matched-empty, or diverged. Broca can answer `matched` without inspecting content
    /// SOLELY because mutation is inexpressible at its durability surface -- `trait Wal` is
    /// append/sync/replay/tail_after_seq/last_seq, with no update, remove or replace verb at
    /// any layer -- so the classifier reduces to "did the lineage change". Every other harness
    /// on that contract must classify seven kinds of mutation event; broca classifies none,
    /// and that difference IS this property.
    ///
    /// So a feature that rewrites history does not merely break a local assumption. It makes
    /// broca's `matched` arm a lie on a wire another repository decodes, with no error
    /// anywhere: the phone renders a stale turn and neither side can tell.
    ///
    /// So `stamp_ordinals`' guarantee — live and replay assign identical ordinals by
    /// construction, pinned by the prefix-merge == full-replay gate — is load-bearing for an
    /// EXTERNAL contract, not only for the reduction transform it was built for.
    ///
    /// A future feature that rewrites lineage history (fork, revert, edit-in-place, compaction
    /// that rewrites the DURABLE transcript rather than only the render prompt) breaks this
    /// claim silently: mids would stay stable while the messages beneath them changed, and
    /// every delta-reading client would serve stale content with no error anywhere. Such a
    /// feature must either preserve positional immutability or replace this identity with a
    /// content-derived one — and it must say which, at that point, rather than inheriting this
    /// comment's assumption.
    ///
    /// A SECOND CONSUMER PROPERTY RESTS ON THE SAME FOUNDATION, recorded here so one future
    /// feature trips one comment and finds BOTH consequences rather than fixing the visible
    /// half. `session.read`'s `head` cursor composes across polls under an append-only
    /// transcript: an equal head means two pages compose exactly, a GREATER head means the
    /// later read saw strictly more with everything the earlier read saw still present and
    /// unchanged, and a backward or newly-absent head means the lineage was recreated. That
    /// middle rule — compose-and-larger — is only sound because an append cannot alter what
    /// preceded it.
    ///
    /// Under lineage rewriting a greater head would no longer imply the earlier page is still
    /// valid, so a consumer composing pages would silently merge two incompatible views. Same
    /// root cause as the mid break above, different symptom, and the two must be fixed
    /// together.
    pub fn mid(&self) -> Option<String> {
        self.ordinal.map(|o| format!("m{o}"))
    }
}

/// Stamp each message with its position in the prompt (0-based), the durable ordinal an
/// external reduction transform keys on. Called at every point a prompt is FINALIZED (both
/// live-loop and replay), so live and resume assign identical ordinals by construction (the
/// prompt is the same append-only list either way — proven by the prefix-merge == full-replay
/// gate). Idempotent: re-stamping a prompt yields the same ordinals.
///
/// INVARIANT — APPEND-ONLY: this is durable only because the prompt grows solely at the end.
/// Any future feature that removes/reorders a mid-lineage message must tombstone-in-place or
/// fork a new lineage, never splice (see [`Message::ordinal`]).
pub fn stamp_ordinals(prompt: &mut [Message]) {
    for (i, m) in prompt.iter_mut().enumerate() {
        // A stamped ordinal must never CHANGE (append-only): re-stamping assigns the same
        // position, so an already-stamped message keeps its value.
        debug_assert!(
            m.ordinal.is_none_or(|o| o == i as u64),
            "ordinal must be append-only stable: message at position {i} already carries \
             ordinal {:?} (a mid-lineage splice/reorder is forbidden)",
            m.ordinal,
        );
        m.ordinal = Some(i as u64);
    }
}

/// Stamp only the append-only suffix beginning at `start`, using absolute prompt positions.
///
/// The skipped prefix must already carry its final ordinals. Debug builds validate that complete
/// prefix so an invalid high-water fails before it can mis-key transform state; release builds pay
/// only for newly appended messages. Returns the number of messages stamped.
pub fn stamp_ordinals_from(prompt: &mut [Message], start: usize) -> usize {
    assert!(
        start <= prompt.len(),
        "ordinal high-water exceeds prompt length"
    );
    #[cfg(debug_assertions)]
    for (i, message) in prompt[..start].iter().enumerate() {
        debug_assert_eq!(
            message.ordinal,
            Some(i as u64),
            "ordinal prefix must be append-only stable: message at position {i} carries \
             ordinal {:?}",
            message.ordinal,
        );
    }

    let stamped = prompt.len() - start;
    for (local_index, message) in prompt[start..].iter_mut().enumerate() {
        let index = start + local_index;
        debug_assert!(
            message
                .ordinal
                .is_none_or(|ordinal| ordinal == index as u64),
            "ordinal must be append-only stable: message at position {index} already carries \
             ordinal {:?} (a mid-lineage splice/reorder is forbidden)",
            message.ordinal,
        );
        message.ordinal = Some(index as u64);
    }
    stamped
}

/// The provider request format that owns an encrypted reasoning payload.
///
/// The `provider_format` field was added after Anthropic and Gemini reasoning was already
/// persisted. An absent value therefore means the legacy Anthropic/Gemini representation.
/// Newly supported request formats set an explicit value so another format cannot emit the data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EncryptedReasoningFormat {
    OpenAiResponses,
    OpenAiResponsesChatgpt,
    /// OpenRouter's complete reasoning/reasoning_details continuation envelope.
    OpenRouter,
}

/// The wire format that owns opaque state attached to an assistant text block.
/// Similar APIs remain distinct: a signature or phase from one endpoint must not leak to another.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TextProviderFormat {
    OpenAiResponses,
    OpenAiResponsesChatgpt,
    Gemini,
    GoogleCodeAssist,
}

/// Opaque, format-owned metadata. Core and WAL retain these bytes without interpreting them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TextProviderState {
    pub format: TextProviderFormat,
    pub data: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    Text {
        text: String,
    },
    /// Assistant text whose provider state belongs to this exact item/part, even if text is empty.
    /// Kept separate from plain Text so existing text constructors and serialized bytes stay stable.
    TextWithState {
        text: String,
        provider_state: TextProviderState,
    },
    Reasoning {
        text: String,
        /// Provider-signed reasoning signature (e.g. Anthropic signed thinking),
        /// preserved verbatim for cache + replay fidelity.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        signature: Option<String>,
    },
    /// Opaque provider continuation state, which may include encrypted reasoning. `data` is:
    /// the legacy Anthropic/Gemini form stores the provider blob, while OpenAI Responses stores
    /// the complete raw reasoning-item JSON so its id, summary, ciphertext, and field order
    /// survive persistence and replay. OpenRouter stores its reasoning/reasoning_details JSON
    /// envelope, including unknown fields and encrypted entries, under its own format tag.
    ///
    /// `provider_format` prevents different request formats from mixing encrypted state. Old
    /// persisted messages omit it and retain legacy behavior. New messages set it explicitly,
    /// and request rendering accepts only its matching format and producing model.
    RedactedReasoning {
        data: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider_format: Option<EncryptedReasoningFormat>,
    },
    ToolCall(ToolCall),
    ToolResult(ToolResult),
}

/// An assembled, complete tool call (input is the full JSON args, not deltas).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolCall {
    pub tool_call_id: ToolCallId,
    pub tool_name: String,
    pub input: Value,
    /// Provider-minted signature that must accompany this function call when replayed to
    /// the same model. It is omitted for providers and model versions that do not sign calls.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thought_signature: Option<String>,
}

/// A tool result fed back to the model. Produced by the runner (via subc→AFT),
/// or a denied/error placeholder so every assistant tool-call has a matching
/// result before the next provider send (a tool_use with no matching result is a
/// provider-invalid conversation).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolResult {
    pub tool_call_id: ToolCallId,
    pub output: ToolOutput,
    #[serde(default)]
    pub is_error: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ToolOutput {
    Text {
        text: String,
    },
    Json {
        value: Value,
    },
    /// Approval denied by policy — still fed back as `tool_result{is_error}` so
    /// the assistant's tool_use has a matching result and the model can adapt.
    Denied {
        reason: String,
    },
    /// Tool execution error, fed back as conversation data so the model can adapt.
    Error {
        message: String,
    },
}

/// The assembled assistant message — text + reasoning + tool-call blocks in
/// source order. The provider module assembles this from its stream deltas; the
/// runner validates/persists it and never re-assembles deltas itself.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AssistantMessage {
    pub message_id: MessageId,
    pub content: Vec<ContentBlock>,
}

impl AssistantMessage {
    /// The tool calls (the loop's per-step dispatch batch) in assistant source
    /// order — results must be emitted back in this same order (universal rule).
    pub fn tool_calls(&self) -> impl Iterator<Item = &ToolCall> {
        self.content.iter().filter_map(|b| match b {
            ContentBlock::ToolCall(c) => Some(c),
            _ => None,
        })
    }
}

/// Unified finish reason, normalized from the provider's raw reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinishReason {
    Stop,
    Length,
    ToolCalls,
    ContentFilter,
    Error,
    Other,
}

/// Decode a step finish reason on a caller-facing surface, where a newer runner may send a
/// reason this build does not know.
///
/// An unfamiliar reason reads as [`FinishReason::Other`], the bucket this vocabulary already
/// keeps for a provider reason that matches none of the named ones, so no named reason is
/// claimed for it. Durable WAL records and provider events keep the derived, closed decoding.
pub fn deserialize_caller_finish_reason<'de, D>(deserializer: D) -> Result<FinishReason, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = crate::caller::OpenState::<FinishReason>::deserialize(deserializer)?;
    Ok(match raw {
        crate::caller::OpenState::Known(reason) => reason,
        crate::caller::OpenState::Unknown(_) => FinishReason::Other,
    })
}

/// The optional form of [`deserialize_caller_finish_reason`].
pub fn deserialize_optional_caller_finish_reason<'de, D>(
    deserializer: D,
) -> Result<Option<FinishReason>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = Option::<crate::caller::OpenState<FinishReason>>::deserialize(deserializer)?;
    Ok(raw.map(|raw| match raw {
        crate::caller::OpenState::Known(reason) => reason,
        crate::caller::OpenState::Unknown(_) => FinishReason::Other,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_provider_finish_reason_uses_the_other_fallback_only_for_callers() {
        #[derive(Deserialize)]
        struct CallerRead {
            #[serde(deserialize_with = "deserialize_caller_finish_reason")]
            finish_reason: FinishReason,
            #[serde(
                default,
                deserialize_with = "deserialize_optional_caller_finish_reason"
            )]
            final_step_finish_reason: Option<FinishReason>,
        }
        let read: CallerRead = serde_json::from_str(
            r#"{"finish_reason":"future_reason","final_step_finish_reason":"future_reason"}"#,
        )
        .unwrap();
        assert_eq!(read.finish_reason, FinishReason::Other);
        assert_eq!(read.final_step_finish_reason, Some(FinishReason::Other));
        let known: CallerRead = serde_json::from_str(r#"{"finish_reason":"tool_calls"}"#).unwrap();
        assert_eq!(known.finish_reason, FinishReason::ToolCalls);
        assert_eq!(known.final_step_finish_reason, None);
        assert_eq!(
            serde_json::to_string(&FinishReason::Other).unwrap(),
            "\"other\""
        );

        // The closed enum, which durable records use, still refuses a value it does not know.
        assert!(serde_json::from_str::<FinishReason>("\"future_reason\"").is_err());
    }

    fn text_message(role: Role, text: &str) -> Message {
        Message::new(
            role,
            vec![ContentBlock::Text {
                text: text.to_string(),
            }],
        )
    }

    fn assert_incremental_matches_full(mut prompt: Vec<Message>, start: usize, expected: usize) {
        let mut reference = prompt.clone();
        stamp_ordinals(&mut reference);
        assert_eq!(stamp_ordinals_from(&mut prompt, start), expected);
        assert_eq!(prompt, reference);
    }

    #[test]
    fn incremental_stamping_matches_full_stamping_for_every_run_entry_mode() {
        assert_incremental_matches_full(
            vec![
                text_message(Role::System, "system"),
                text_message(Role::User, "fresh input"),
            ],
            0,
            2,
        );

        let mut resumed = vec![text_message(Role::User, "prior input")];
        stamp_ordinals(&mut resumed);
        let resumed_prefix_len = resumed.len();
        resumed.push(text_message(Role::Assistant, "prior call"));
        resumed.push(text_message(Role::Tool, "resumed result"));
        assert_incremental_matches_full(resumed, resumed_prefix_len, 2);

        let mut appended = vec![
            text_message(Role::User, "prior input"),
            text_message(Role::Assistant, "prior answer"),
        ];
        stamp_ordinals(&mut appended);
        let appended_prefix_len = appended.len();
        appended.push(text_message(Role::User, "appended input"));
        assert_incremental_matches_full(appended, appended_prefix_len, 1);
    }

    #[test]
    #[should_panic(expected = "ordinal prefix must be append-only stable")]
    fn incremental_stamping_rejects_an_unstable_prefix() {
        let mut prompt = vec![
            text_message(Role::User, "prefix"),
            text_message(Role::Assistant, "tail"),
        ];
        stamp_ordinals(&mut prompt);
        prompt[0].ordinal = Some(9);
        stamp_ordinals_from(&mut prompt, 1);
    }

    #[test]
    fn incremental_tool_loop_stamps_each_new_message_once() {
        const PREFIX_LEN: usize = 128;
        const STEPS: usize = 32;

        let mut prompt: Vec<_> = (0..PREFIX_LEN)
            .map(|index| text_message(Role::User, &format!("prefix-{index}")))
            .collect();
        stamp_ordinals(&mut prompt);
        let mut stamped_len = prompt.len();
        let mut assignments = 0;

        for step in 0..STEPS {
            prompt.push(text_message(Role::Assistant, &format!("call-{step}")));
            prompt.push(text_message(Role::Tool, &format!("result-{step}")));
            assignments += stamp_ordinals_from(&mut prompt, stamped_len);
            stamped_len = prompt.len();
        }

        assert_eq!(assignments, 2 * STEPS);
        let mut reference = prompt.clone();
        stamp_ordinals(&mut reference);
        assert_eq!(prompt, reference);
    }

    #[test]
    fn legacy_message_without_cache_prefix_round_trips_identical_bytes() {
        let bytes = br#"{"role":"user","content":[{"type":"text","text":"hello"}]}"#;
        let message: Message = serde_json::from_slice(bytes).unwrap();
        assert_eq!(message.cache_prefix_blocks, None);
        assert_eq!(serde_json::to_vec(&message).unwrap(), bytes);
    }

    #[test]
    fn legacy_redacted_reasoning_defaults_to_unscoped_format() {
        let encoded = r#"{"type":"redacted_reasoning","data":"legacy-blob"}"#;
        let block: ContentBlock = serde_json::from_str(encoded).expect("legacy block decodes");
        assert_eq!(
            block,
            ContentBlock::RedactedReasoning {
                data: "legacy-blob".into(),
                provider_format: None,
            }
        );
        assert_eq!(serde_json::to_string(&block).unwrap(), encoded);
    }
}
