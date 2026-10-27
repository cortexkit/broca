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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TextProviderState {
    pub format: TextProviderFormat,
    pub data: String,
}

/// The modality of a captured, byte-backed object. The bytes live in the media store, not here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaKind {
    Image,
    Document,
    Audio,
    Video,
}

impl MediaKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Image => "image",
            Self::Document => "document",
            Self::Audio => "audio",
            Self::Video => "video",
        }
    }
}

/// Metadata over the decoded bytes of a captured object; never bytes or base64.
/// Only images have dimensions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "MediaRefFields")]
pub struct MediaRef {
    pub kind: MediaKind,
    pub mime: String,
    pub byte_size: u64,
    pub sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub width: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub height: Option<u32>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MediaRefFields {
    kind: MediaKind,
    mime: String,
    byte_size: u64,
    sha256: String,
    width: Option<u32>,
    height: Option<u32>,
}

impl TryFrom<MediaRefFields> for MediaRef {
    type Error = &'static str;

    fn try_from(fields: MediaRefFields) -> Result<Self, Self::Error> {
        if fields.kind == MediaKind::Image {
            if fields.width.is_none() || fields.height.is_none() {
                return Err("image media requires width and height");
            }
        } else if fields.width.is_some() || fields.height.is_some() {
            return Err("only image media has width and height");
        }
        Ok(Self {
            kind: fields.kind,
            mime: fields.mime,
            byte_size: fields.byte_size,
            sha256: fields.sha256,
            width: fields.width,
            height: fields.height,
        })
    }
}

/// Why a captured item is represented by text instead of provider-native media.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaMarkerReason {
    Modality,
    Carrier,
    Mime,
    Undecodable,
    TooLarge,
    RequestCap,
    Aged,
    Provenance,
    StoreFailed,
    /// Read-only fallback for a reader that did not opt into media references.
    NotRequested,
}

impl MediaMarkerReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Modality => "modality",
            Self::Carrier => "carrier",
            Self::Mime => "mime",
            Self::Undecodable => "undecodable",
            Self::TooLarge => "too_large",
            Self::RequestCap => "request_cap",
            Self::Aged => "aged",
            Self::Provenance => "provenance",
            Self::StoreFailed => "store_failed",
            Self::NotRequested => "not_requested",
        }
    }
}

pub fn media_not_shown_marker(media: &MediaRef, reason: MediaMarkerReason) -> String {
    let digest: String = media.sha256.chars().take(12).collect();
    format!(
        "[media not shown: kind={} mime={} bytes={} sha256={} reason={}]",
        media.kind.as_str(),
        media.mime,
        media.byte_size,
        digest,
        reason.as_str()
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    Media(MediaRef),
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolResult {
    pub tool_call_id: ToolCallId,
    pub output: ToolOutput,
    #[serde(default)]
    pub is_error: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ToolOutput {
    Text {
        text: String,
    },
    Json {
        value: Value,
    },
    Parts {
        parts: Vec<ToolOutputPart>,
    },
    /// Approval denied by policy — still fed back as `tool_result{is_error}` so
    /// the assistant's tool_use has a matching result and the model can adapt.
    Denied {
        reason: String,
    },
    /// Tool execution error, fed back as conversation data so the model can adapt.
    ///
    /// `message` is the stable, matchable account of the failure: exact-equality checks on it
    /// (for example the tool-plane-unavailable match) keep working. A failure the plane can
    /// attribute to a module outage additionally carries `module`/`code`/`detail`, with
    /// `nothing_sent` set, so the model (and any log reader) sees WHAT was down and that a
    /// retry is safe — without changing `message`. The extra fields are skipped on the wire
    /// when absent, so results that never had them serialize exactly as before.
    Error {
        message: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        module: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        code: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        nothing_sent: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        retry: Option<String>,
    },
}

/// The nested reference keeps a part's discriminator separate from the media modality.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ToolOutputPart {
    Text { text: String },
    Media { media: MediaRef },
}

/// The error text for a tool result the plane never sent: the string a caller matches on
/// with exact equality to detect an unavailable tool plane.
///
/// Kept as a single constant (rather than a literal at each construction site) so the
/// plane-built result and the run-built no-dispatch result cannot drift apart.
pub const TOOL_PLANE_UNAVAILABLE_MESSAGE: &str = "tool plane unavailable";

/// Guidance appended (in renderings, not in `message`) to a result the plane never sent
/// because its module was restarting or unavailable. The runner contract limits `retry` to
/// one line of at most [`MODULE_UNAVAILABLE_RETRY_MAX_BYTES`].
pub const TOOL_PLANE_UNAVAILABLE_RETRY: &str =
    "the module is restarting or unavailable; retry the call in a minute";

/// The most bytes the runner contract allows in a module-outage error's `detail`.
pub const MODULE_UNAVAILABLE_DETAIL_MAX_BYTES: usize = 1024;

/// The most bytes the runner contract allows in a module-outage error's `retry`.
pub const MODULE_UNAVAILABLE_RETRY_MAX_BYTES: usize = 256;

/// Whether `module` is a module id the runner contract accepts in an error's `module`: 1 to
/// 255 bytes of UTF-8 with no control character (spaces and non-ASCII are allowed).
fn is_contract_module_id(module: &str) -> bool {
    (1..=255).contains(&module.len()) && !module.chars().any(char::is_control)
}

/// Whether `code` is an error code the runner contract accepts: `^[a-z][a-z0-9_]{0,63}$`.
fn is_contract_error_code(code: &str) -> bool {
    let mut bytes = code.bytes();
    bytes.next().is_some_and(|first| first.is_ascii_lowercase())
        && code.len() <= 64
        && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

/// Bound a daemon refusal detail to what the runner contract allows in a module-outage
/// error's `detail`: one line of at most [`MODULE_UNAVAILABLE_DETAIL_MAX_BYTES`] bytes.
/// Every line break (CR, LF or CRLF) becomes one space, and an over-long detail is cut at
/// the last char boundary that fits, so the result is always valid UTF-8.
pub fn bound_module_unavailable_detail(detail: &str) -> String {
    let mut line = detail.replace("\r\n", " ").replace(['\r', '\n'], " ");
    if line.len() > MODULE_UNAVAILABLE_DETAIL_MAX_BYTES {
        let mut end = MODULE_UNAVAILABLE_DETAIL_MAX_BYTES;
        while !line.is_char_boundary(end) {
            end -= 1;
        }
        line.truncate(end);
    }
    line
}

impl ToolOutput {
    /// A plain error result with no module attribution, as every existing site built it.
    pub fn error(message: impl Into<String>) -> Self {
        ToolOutput::Error {
            message: message.into(),
            module: None,
            code: None,
            detail: None,
            nothing_sent: None,
            retry: None,
        }
    }

    /// An error result for a call refused because its module was restarting or
    /// unavailable, proven undelivered: `message` stays exactly
    /// [`TOOL_PLANE_UNAVAILABLE_MESSAGE`] (callers match on it), while the module, the
    /// daemon's refusal code and detail, and the retry guidance ride in sibling fields so
    /// the model — and any reader of the serialized result — sees what was down and that
    /// nothing was sent. The detail is bounded to one line of at most 1 KiB (see
    /// [`bound_module_unavailable_detail`]).
    ///
    /// Strict readers of the runner contract refuse a whole history page that holds one
    /// invalid member, and the module id and code come from the daemon, so each is checked
    /// here, where every such result is built, and left out when it does not fit: `module`
    /// must be 1 to 255 bytes with no control character, `code` must match
    /// `^[a-z][a-z0-9_]{0,63}$`, and no member is ever present but empty. The result stays a
    /// valid attributed error with fewer fields, and `message` never changes.
    pub fn module_unavailable(
        module: impl Into<String>,
        code: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        let module = Some(module.into()).filter(|module| is_contract_module_id(module));
        let code = Some(code.into()).filter(|code| is_contract_error_code(code));
        let detail = bound_module_unavailable_detail(&detail.into());
        ToolOutput::Error {
            message: TOOL_PLANE_UNAVAILABLE_MESSAGE.to_owned(),
            module,
            code,
            detail: (!detail.is_empty()).then_some(detail),
            nothing_sent: Some(true),
            retry: Some(TOOL_PLANE_UNAVAILABLE_RETRY.to_owned()),
        }
    }

    /// The text the model reads for an error result's fields: the message, plus any
    /// module-outage attribution on its own lines. A plain error renders as its message
    /// alone, exactly as before; only an attributed outage result grows the extra lines.
    /// Kept as one associated function (rather than a method matching `self`) so every
    /// provider family's encoder renders these fields byte-identically.
    pub fn error_model_text(
        message: &str,
        module: Option<&str>,
        code: Option<&str>,
        detail: Option<&str>,
        nothing_sent: Option<bool>,
        retry: Option<&str>,
    ) -> String {
        let mut text = message.to_owned();
        if let Some(module) = module {
            text.push_str(&format!("\nmodule: {module}"));
        }
        if let Some(code) = code {
            text.push_str(&format!("\ncode: {code}"));
        }
        if let Some(detail) = detail {
            text.push_str(&format!("\ndetail: {detail}"));
        }
        if nothing_sent == Some(true) {
            text.push_str("\nnothing was sent");
        }
        if let Some(retry) = retry {
            text.push_str(&format!("\nretry: {retry}"));
        }
        text
    }
}

/// Text-only rendering in source order, also used by readers that did not request media.
pub fn media_parts_text(parts: &[ToolOutputPart], reason: MediaMarkerReason) -> String {
    parts
        .iter()
        .map(|part| match part {
            ToolOutputPart::Text { text } => text.clone(),
            ToolOutputPart::Media { media } => media_not_shown_marker(media, reason),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The assembled assistant message — text + reasoning + tool-call blocks in
/// source order. The provider module assembles this from its stream deltas; the
/// runner validates/persists it and never re-assembles deltas itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

/// The start of the text a tool result shows in place of a content block broca does not carry
/// (an image, audio, a resource), so the model still learns that the tool answered with it.
const NON_TEXT_PLACEHOLDER_OPEN: &str = "[non-text tool content omitted: type=";

/// The text a tool result shows in place of a non-text content block of `content_type`, naming
/// its MIME type and the length of its encoded data when the block had them. Tool providers'
/// replies are mapped through this one spelling, which [`is_image_only_placeholder`] recognises.
pub fn non_text_tool_content_placeholder(
    content_type: &str,
    mime_type: Option<&str>,
    data_len: Option<usize>,
) -> String {
    let mime_type = mime_type
        .map(|value| format!(" mimeType={value}"))
        .unwrap_or_default();
    let data_size = data_len
        .map(|len| format!(" data≈{len} bytes"))
        .unwrap_or_default();
    format!("{NON_TEXT_PLACEHOLDER_OPEN}{content_type}{mime_type}{data_size}]")
}

/// Whether a tool result's text is nothing but placeholders for images: a tool result that
/// held only images, which the tag rules number and render as an image label.
pub fn is_image_only_placeholder(text: &str) -> bool {
    let image = format!("{NON_TEXT_PLACEHOLDER_OPEN}image");
    let mut rest = text;
    if rest.is_empty() {
        return false;
    }
    while !rest.is_empty() {
        let Some(after) = rest.strip_prefix(&image) else {
            return false;
        };
        // The type is exactly `image`, then the optional members, then the closing bracket.
        if !(after.starts_with(' ') || after.starts_with(']')) {
            return false;
        }
        let Some(close) = after.find(']') else {
            return false;
        };
        rest = &after[close + 1..];
    }
    true
}

#[cfg(test)]
mod tests {
    /// The model-facing text for a module outage names the module, the code and the
    /// guidance lines after the stable message; a plain error renders as its message
    /// alone, exactly as before.
    #[test]
    fn module_outage_error_model_text_shows_attribution() {
        use super::{ToolOutput, TOOL_PLANE_UNAVAILABLE_MESSAGE};
        let text = ToolOutput::error_model_text(
            TOOL_PLANE_UNAVAILABLE_MESSAGE,
            Some("aft"),
            Some("target_unavailable"),
            Some("supervised_not_registered"),
            Some(true),
            Some(super::TOOL_PLANE_UNAVAILABLE_RETRY),
        );
        assert!(text.starts_with("tool plane unavailable\n"), "{text}");
        for line in [
            "module: aft",
            "code: target_unavailable",
            "detail: supervised_not_registered",
            "nothing was sent",
            super::TOOL_PLANE_UNAVAILABLE_RETRY,
        ] {
            assert!(text.contains(line), "{text}");
        }
        assert_eq!(
            ToolOutput::error_model_text("boom", None, None, None, None, None),
            "boom"
        );
        // The constructor and the renderer agree field for field.
        let built = ToolOutput::module_unavailable("aft", "target_unavailable", "d");
        let ToolOutput::Error {
            message,
            module,
            code,
            detail,
            nothing_sent,
            retry,
        } = &built
        else {
            unreachable!()
        };
        assert_eq!(
            ToolOutput::error_model_text(
                message,
                module.as_deref(),
                code.as_deref(),
                detail.as_deref(),
                *nothing_sent,
                retry.as_deref(),
            ),
            text.replace("supervised_not_registered", "d")
        );
    }

    /// The runner contract caps `retry` at one line of 256 bytes; the constant the plane
    /// sends must stay inside it.
    #[test]
    fn module_unavailable_retry_is_one_short_line() {
        use super::{MODULE_UNAVAILABLE_RETRY_MAX_BYTES, TOOL_PLANE_UNAVAILABLE_RETRY};
        assert!(TOOL_PLANE_UNAVAILABLE_RETRY.len() <= MODULE_UNAVAILABLE_RETRY_MAX_BYTES);
        assert!(!TOOL_PLANE_UNAVAILABLE_RETRY.contains(['\n', '\r']));
        let super::ToolOutput::Error {
            retry,
            nothing_sent,
            ..
        } = super::ToolOutput::module_unavailable("aft", "target_unavailable", "d")
        else {
            unreachable!()
        };
        assert_eq!(retry.as_deref(), Some(TOOL_PLANE_UNAVAILABLE_RETRY));
        assert_eq!(nothing_sent, Some(true));
    }

    /// A daemon detail that spans lines and runs past 1 KiB is stored as one line cut to
    /// 1024 bytes on a char boundary: every line break becomes a space, and a multi-byte
    /// char straddling the limit is dropped whole rather than split.
    #[test]
    fn module_unavailable_detail_is_one_line_of_at_most_1_kib() {
        use super::{bound_module_unavailable_detail, MODULE_UNAVAILABLE_DETAIL_MAX_BYTES};
        // 1021 ASCII bytes once the CRLF collapses to one space, then a 4-byte char that
        // would end at byte 1025.
        let mut raw = String::from("first line\nsecond\r\nthird\rfourth ");
        while raw.len() < 1022 {
            raw.push('x');
        }
        raw.push('🦀');
        raw.push_str("\ntail that must be cut");
        let bounded = bound_module_unavailable_detail(&raw);
        assert!(
            bounded.len() <= MODULE_UNAVAILABLE_DETAIL_MAX_BYTES,
            "{}",
            bounded.len()
        );
        assert_eq!(bounded.len(), 1021, "the straddling char is dropped whole");
        assert!(!bounded.contains(['\n', '\r']), "{bounded}");
        assert!(
            bounded.starts_with("first line second third fourth "),
            "{bounded}"
        );
        assert!(!bounded.contains('🦀'));
        // The constructor applies the same bound to what it stores.
        let super::ToolOutput::Error { detail, .. } =
            super::ToolOutput::module_unavailable("aft", "target_unavailable", raw)
        else {
            unreachable!()
        };
        assert_eq!(detail.as_deref(), Some(bounded.as_str()));
        // A short single-line detail is kept exactly as given.
        assert_eq!(
            bound_module_unavailable_detail("supervised_not_registered"),
            "supervised_not_registered"
        );
    }

    /// The members of an attributed outage error, for the contract-shape tests below.
    fn outage_members(
        module: &str,
        code: &str,
        detail: &str,
    ) -> (String, Option<String>, Option<String>, Option<String>) {
        let super::ToolOutput::Error {
            message,
            module,
            code,
            detail,
            nothing_sent,
            retry,
        } = super::ToolOutput::module_unavailable(module, code, detail)
        else {
            unreachable!()
        };
        // Whatever is left out, the result stays an attributed outage.
        assert_eq!(nothing_sent, Some(true));
        assert_eq!(retry.as_deref(), Some(super::TOOL_PLANE_UNAVAILABLE_RETRY));
        (message, module, code, detail)
    }

    /// A code outside `^[a-z][a-z0-9_]{0,63}$` is left out rather than emitted; the message
    /// and the other members are unchanged.
    #[test]
    fn an_out_of_grammar_code_is_omitted() {
        for bad in [
            "Target_unavailable",
            "9lives",
            "target-unavailable",
            "",
            "a b",
        ] {
            let (message, module, code, detail) = outage_members("aft", bad, "d");
            assert_eq!(message, super::TOOL_PLANE_UNAVAILABLE_MESSAGE);
            assert_eq!(code, None, "{bad:?} must be omitted");
            assert_eq!(module.as_deref(), Some("aft"));
            assert_eq!(detail.as_deref(), Some("d"));
        }
        let too_long = format!("a{}", "b".repeat(64));
        assert_eq!(outage_members("aft", &too_long, "d").2, None);
        let longest = format!("a{}", "b".repeat(63));
        assert_eq!(outage_members("aft", &longest, "d").2, Some(longest));
        assert_eq!(
            outage_members("aft", "module_timeout2", "d").2.as_deref(),
            Some("module_timeout2")
        );
    }

    /// An empty detail is never emitted as an empty member, and nor is one that bounds to
    /// nothing.
    #[test]
    fn an_empty_detail_is_omitted() {
        let (message, module, code, detail) = outage_members("aft", "target_unavailable", "");
        assert_eq!(message, super::TOOL_PLANE_UNAVAILABLE_MESSAGE);
        assert_eq!(detail, None);
        assert_eq!(module.as_deref(), Some("aft"));
        assert_eq!(code.as_deref(), Some("target_unavailable"));
    }

    /// A module id over 255 bytes, one with a control character, and an empty one are left
    /// out; the rest of the result is unchanged.
    #[test]
    fn an_out_of_contract_module_is_omitted() {
        let long = "m".repeat(300);
        for bad in [long.as_str(), "aft\u{7}", "af\nt", ""] {
            let (message, module, code, detail) = outage_members(bad, "target_unavailable", "d");
            assert_eq!(message, super::TOOL_PLANE_UNAVAILABLE_MESSAGE);
            assert_eq!(module, None, "{bad:?} must be omitted");
            assert_eq!(code.as_deref(), Some("target_unavailable"));
            assert_eq!(detail.as_deref(), Some("d"));
        }
        let longest = "m".repeat(255);
        assert_eq!(outage_members(&longest, "x", "d").1, Some(longest));
    }

    /// A module id with a space and non-ASCII characters fits the contract and is kept.
    #[test]
    fn a_non_ascii_module_with_a_space_is_kept() {
        let id = "mödule äft";
        assert_eq!(
            outage_members(id, "target_unavailable", "d").1.as_deref(),
            Some(id)
        );
    }
    #[test]
    fn image_only_placeholders_are_recognised_and_mixed_content_is_not() {
        use super::{is_image_only_placeholder, non_text_tool_content_placeholder};
        let image = non_text_tool_content_placeholder("image", Some("image/png"), Some(8));
        assert_eq!(
            image,
            "[non-text tool content omitted: type=image mimeType=image/png data≈8 bytes]"
        );
        assert!(is_image_only_placeholder(&image));
        assert!(is_image_only_placeholder(&format!("{image}{image}")));
        assert!(is_image_only_placeholder(
            &non_text_tool_content_placeholder("image", None, None)
        ));
        assert!(!is_image_only_placeholder(&format!("before{image}")));
        assert!(!is_image_only_placeholder(&format!("{image} after")));
        assert!(!is_image_only_placeholder(
            &non_text_tool_content_placeholder("audio", Some("audio/wav"), Some(4))
        ));
        assert!(!is_image_only_placeholder(
            &non_text_tool_content_placeholder("images", None, None)
        ));
        assert!(!is_image_only_placeholder(""));
    }

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
