//! Per-family media profiles: how a provider's request format carries images, PDFs, audio and
//! video, and with which limits.
//!
//! Whether a model accepts a modality at all is the catalog's statement (`ModelSpec`'s
//! `input_modalities`). How the request format carries it is a separate question, answered here:
//! in a user message, inside a tool result, as a follow-up user message, or not at all (a text
//! marker), plus the provider's per-item, count, dimension and request-size limits. Every value
//! comes from `docs/media-families.md`, which cites the provider's primary source; a value that
//! table marks as a broca choice is a broca choice here too.
//!
//! A profile is chosen from three frozen inputs: the wire family, the provider's media quirk, and
//! the wire model id (for documented generation splits such as Gemini 3 versus earlier Gemini).
//! A family and provider combination with no entry gets the conservative profile, which refuses
//! a person's media and turns tool media into markers.
//!
//! The chosen profile is frozen into `FrozenRenderConfig::media` once at admission (and again
//! when a model switch refreezes the run config), so a later catalog or table change never moves
//! an in-flight run. A config without a profile renders exactly as before media existed.

use std::fmt;

use broca_protocol::content::{MediaKind, MediaRef};
use serde::{Deserialize, Serialize};

use crate::spec::{InputModalities, ModelSpec, ProviderSpec, WireFamilyId};
use crate::wire::FrozenRenderConfig;

/// One decimal megabyte, the unit providers mean when they write "MB".
const MB: u64 = 1_000_000;
/// One mebibyte, the unit providers mean when they write "MiB".
const MIB: u64 = 1_048_576;

/// The context window the token aging thresholds are derived from when the catalog states none,
/// or states one too small to be a real window. A broca choice: a common window size for current
/// models, so a model with an unpublished window still ages media before it crowds out text.
pub const FALLBACK_CONTEXT_TOKENS: u64 = 128_000;

/// The smallest stated context window treated as real. Below it the token thresholds could not
/// all be positive, and no model that accepts media has a window anywhere near this small.
const MIN_CONTEXT_TOKENS: u64 = 8;

/// How one kind of media reaches the model in one position of the request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaCarrier {
    /// The provider's own media shape in that position.
    Native,
    /// Tool media only: the tool result keeps a text reference and the media follows in a user
    /// message right after it. Used only where a live check showed the provider accepts it and
    /// prompt caching stays stable.
    FollowUpUserMessage,
    /// The request format cannot carry it. A person's media is refused with reason `carrier`;
    /// tool media renders as a `carrier` marker.
    Marker,
}

/// A per-item size limit, in the measure the provider documents it in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ByteLimit {
    /// At most this many decoded (raw) bytes.
    Decoded(u64),
    /// At most this many bytes once base64-encoded.
    Encoded(u64),
}

impl ByteLimit {
    /// Whether an item of `decoded_bytes` raw bytes is within this limit.
    pub fn admits(self, decoded_bytes: u64) -> bool {
        match self {
            Self::Decoded(max) => decoded_bytes <= max,
            Self::Encoded(max) => base64_encoded_len(decoded_bytes) <= max,
        }
    }
}

/// The size of `decoded_bytes` raw bytes after standard padded base64 encoding.
pub fn base64_encoded_len(decoded_bytes: u64) -> u64 {
    decoded_bytes.div_ceil(3).saturating_mul(4)
}

/// What one profile does with one kind of media.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct KindProfile {
    /// How a person's media of this kind reaches the model.
    pub user_message: MediaCarrier,
    /// How tool media of this kind reaches the model.
    pub tool_result: MediaCarrier,
    /// The provider's per-item limit. Absent when it documents none, or none broca can evaluate
    /// (the request cap still applies).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_item_bytes: Option<ByteLimit>,
    /// The most items of this kind one request may hold. Absent when the provider documents no
    /// count broca can evaluate (for example a page count, since broca does not count pages).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_count: Option<u32>,
}

impl KindProfile {
    const fn new(
        user_message: MediaCarrier,
        tool_result: MediaCarrier,
        max_item_bytes: Option<ByteLimit>,
        max_count: Option<u32>,
    ) -> Self {
        Self {
            user_message,
            tool_result,
            max_item_bytes,
            max_count,
        }
    }

    /// A kind the request format cannot carry in either position.
    const MARKER: Self = Self::new(MediaCarrier::Marker, MediaCarrier::Marker, None, None);
}

/// A maximum image side length that applies once a request holds more than a number of items.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DimensionLimit {
    /// The limit applies when the request holds more than this many counted items; zero means it
    /// always applies.
    pub above_count: u32,
    /// The longest side, in pixels, each image may have while the limit applies.
    pub max_side_px: u32,
    /// Whether documents count toward `above_count` alongside images (Claude on Bedrock).
    #[serde(default, skip_serializing_if = "is_false")]
    pub counts_documents: bool,
}

impl DimensionLimit {
    const fn new(above_count: u32, max_side_px: u32) -> Self {
        Self {
            above_count,
            max_side_px,
            counts_documents: false,
        }
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// The two aging threshold pairs. When the unprotected media still shown exceed a trigger, the
/// oldest are marked `aged` until they are at or below both targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgingThresholds {
    /// Cb: the encoded-byte total of shown media that starts aging.
    pub byte_trigger: u64,
    /// Lb: the encoded-byte total aging reduces shown media to.
    pub byte_target: u64,
    /// Ct: the estimated-token total of shown media that starts aging.
    pub token_trigger: u64,
    /// Lt: the estimated-token total aging reduces shown media to.
    pub token_target: u64,
}

impl AgingThresholds {
    /// The thresholds broca derives for a request cap and a context window. A broca choice: the
    /// byte trigger is the largest allowed (half the cap) so aging rewrites the prompt prefix as
    /// rarely as possible, while the token trigger is a quarter of the window so media never take
    /// more than half of the room text needs. Each target is half its trigger.
    pub fn derived(request_cap_bytes: u64, context_tokens: u64) -> Self {
        let byte_trigger = request_cap_bytes / 2;
        let token_trigger = context_tokens / 4;
        Self {
            byte_trigger,
            byte_target: byte_trigger / 2,
            token_trigger,
            token_target: token_trigger / 2,
        }
    }

    /// Check the rules every threshold set must satisfy: all four positive, Lb <= Cb/2,
    /// Cb <= half the request cap, Lt <= Ct/2 and Ct <= half the model context. The gaps between
    /// trigger and target bound how often aging changes the prompt prefix in a long session.
    pub fn validate(
        &self,
        request_cap_bytes: u64,
        context_tokens: u64,
    ) -> Result<(), MediaProfileError> {
        for (name, value) in [
            ("byte_trigger", self.byte_trigger),
            ("byte_target", self.byte_target),
            ("token_trigger", self.token_trigger),
            ("token_target", self.token_target),
        ] {
            if value == 0 {
                return Err(MediaProfileError::NotPositive(name));
            }
        }
        if self.byte_target > self.byte_trigger / 2 {
            return Err(MediaProfileError::ByteTargetAboveHalfTrigger);
        }
        if self.byte_trigger > request_cap_bytes / 2 {
            return Err(MediaProfileError::ByteTriggerAboveHalfCap);
        }
        if self.token_target > self.token_trigger / 2 {
            return Err(MediaProfileError::TokenTargetAboveHalfTrigger);
        }
        if self.token_trigger > context_tokens / 2 {
            return Err(MediaProfileError::TokenTriggerAboveHalfContext);
        }
        Ok(())
    }
}

/// A media profile that breaks one of the aging threshold rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaProfileError {
    /// The named threshold is zero.
    NotPositive(&'static str),
    /// Lb is above Cb/2.
    ByteTargetAboveHalfTrigger,
    /// Cb is above half the request cap.
    ByteTriggerAboveHalfCap,
    /// Lt is above Ct/2.
    TokenTargetAboveHalfTrigger,
    /// Ct is above half the model context.
    TokenTriggerAboveHalfContext,
}

impl fmt::Display for MediaProfileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotPositive(name) => write!(f, "media aging threshold {name} must be positive"),
            Self::ByteTargetAboveHalfTrigger => {
                f.write_str("media aging byte target must be at most half the byte trigger")
            }
            Self::ByteTriggerAboveHalfCap => {
                f.write_str("media aging byte trigger must be at most half the request cap")
            }
            Self::TokenTargetAboveHalfTrigger => {
                f.write_str("media aging token target must be at most half the token trigger")
            }
            Self::TokenTriggerAboveHalfContext => {
                f.write_str("media aging token trigger must be at most half the model context")
            }
        }
    }
}

impl std::error::Error for MediaProfileError {}

/// Carriers that stay off until a live check against the real provider shows they work. Each
/// flag is set in [`LIVE_CHECKED_GATES`] only after that check passes; until then the profile
/// falls back to markers (tool media) and `carrier` refusals (a person's media).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct LiveGates {
    /// The ChatGPT backend's `input_audio` part, in user messages and tool results.
    #[serde(default, skip_serializing_if = "is_false")]
    pub chatgpt_input_audio: bool,
    /// Tool media carried as a follow-up user message where the tool result cannot hold it.
    #[serde(default, skip_serializing_if = "is_false")]
    pub follow_up_user_message: bool,
    /// Media on the undocumented Code Assist endpoint, in either position.
    #[serde(default, skip_serializing_if = "is_false")]
    pub code_assist_media: bool,
}

impl LiveGates {
    /// No live check has passed.
    pub const UNSET: Self = Self {
        chatgpt_input_audio: false,
        follow_up_user_message: false,
        code_assist_media: false,
    };

    pub fn is_unset(&self) -> bool {
        *self == Self::UNSET
    }
}

/// The live checks that have passed, per profile. A profile not listed has every gate unset.
/// Adding an entry changes only runs admitted afterwards, because the gates are frozen into each
/// run's profile.
pub const LIVE_CHECKED_GATES: &[(MediaProfileId, LiveGates)] = &[];

fn live_gates(id: MediaProfileId) -> LiveGates {
    LIVE_CHECKED_GATES
        .iter()
        .find(|(profile, _)| *profile == id)
        .map_or(LiveGates::UNSET, |(_, gates)| *gates)
}

/// The provider a media profile belongs to, within a wire family several providers share. It is
/// a provider quirk like the reasoning-echo policy: carried on `Quirks`, read once when the
/// profile is frozen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaQuirk {
    /// Anthropic's own Messages API.
    Anthropic,
    /// OpenAI's own Chat Completions and Responses APIs.
    Openai,
    /// The Gemini API and Vertex AI `generateContent`.
    Google,
    Openrouter,
    Xai,
    Deepseek,
    Kimi,
    /// The Llama API (Chat Completions) and the Meta Model API (Responses).
    Meta,
}

/// The catalog provider ids each media quirk covers (`crates/broca-catalog/data/overlay.json`
/// and the models.dev snapshot). A provider not listed has no quirk, and on a family several
/// providers share that means the conservative profile.
const CATALOG_PROVIDER_MEDIA_QUIRKS: &[(&str, MediaQuirk)] = &[
    ("anthropic", MediaQuirk::Anthropic),
    ("openai", MediaQuirk::Openai),
    ("google", MediaQuirk::Google),
    ("google-vertex", MediaQuirk::Google),
    ("openrouter", MediaQuirk::Openrouter),
    ("xai", MediaQuirk::Xai),
    ("deepseek", MediaQuirk::Deepseek),
    ("moonshotai", MediaQuirk::Kimi),
    ("moonshotai-cn", MediaQuirk::Kimi),
    ("kimi-code-plan-global", MediaQuirk::Kimi),
    ("kimi-code-plan-cn", MediaQuirk::Kimi),
    ("llama", MediaQuirk::Meta),
    ("meta", MediaQuirk::Meta),
];

/// The media quirk for a catalog provider id, when the provider has a profile entry.
pub fn media_quirk_for_catalog_provider(provider_id: &str) -> Option<MediaQuirk> {
    CATALOG_PROVIDER_MEDIA_QUIRKS
        .iter()
        .find(|(id, _)| *id == provider_id)
        .map(|(_, quirk)| *quirk)
}

/// Which table entry a frozen profile came from. Recorded so a refusal or a marker can name the
/// profile that decided it, and so tests can pin selection without comparing every limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaProfileId {
    Anthropic,
    Bedrock,
    OpenaiResponses,
    OpenaiResponsesChatgpt,
    OpenaiChat,
    Openrouter,
    Xai,
    Deepseek,
    Kimi,
    /// The Llama API on `openai_chat`.
    MetaChat,
    /// The Meta Model API on `openai_responses`.
    MetaResponses,
    /// Gemini 3 and later: media inside `functionResponse.parts`.
    Gemini3,
    /// Gemini before 3: no media inside tool results.
    Gemini,
    CodeAssistGemini3,
    CodeAssistGemini,
    /// No profile entry: a person's media is refused and tool media renders as markers.
    Conservative,
}

/// A documented generation split inside one wire family, keyed by the wire model id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WireModelPattern {
    /// A Gemini model id whose major version is at least this (`gemini-3-pro`, `gemini-3.1-flash`).
    GeminiMajorAtLeast(u32),
    /// Every wire model of the family: the split is decided by a live-gated flag, not the model.
    AnyModel,
}

/// What a matching wire model changes in its family's profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GenerationSplit {
    /// Tool results carry images and PDFs in `functionResponse.parts`.
    GeminiFunctionResponseParts,
    /// The ChatGPT backend's `input_audio` part, used only once its live gate is set.
    ChatgptInputAudio,
}

/// The per-family wire-model-pattern table (`docs/media-families.md`, "Profile selection").
const WIRE_MODEL_SPLITS: &[(WireFamilyId, WireModelPattern, GenerationSplit)] = &[
    (
        WireFamilyId::Gemini,
        WireModelPattern::GeminiMajorAtLeast(3),
        GenerationSplit::GeminiFunctionResponseParts,
    ),
    (
        WireFamilyId::GoogleCodeAssist,
        WireModelPattern::GeminiMajorAtLeast(3),
        GenerationSplit::GeminiFunctionResponseParts,
    ),
    (
        WireFamilyId::OpenAiResponsesChatgpt,
        WireModelPattern::AnyModel,
        GenerationSplit::ChatgptInputAudio,
    ),
];

impl WireModelPattern {
    fn matches(self, wire_model_id: &str) -> bool {
        match self {
            Self::AnyModel => true,
            Self::GeminiMajorAtLeast(min) => gemini_major(wire_model_id).is_some_and(|m| m >= min),
        }
    }
}

/// The major version of a Gemini wire model id, read from its last path segment so a Vertex
/// resource name (`publishers/google/models/gemini-3-pro`) reads like the bare id.
fn gemini_major(wire_model_id: &str) -> Option<u32> {
    let name = wire_model_id.rsplit('/').next().unwrap_or(wire_model_id);
    let rest = name.strip_prefix("gemini-")?;
    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
}

fn has_split(family: WireFamilyId, wire_model_id: &str, split: GenerationSplit) -> bool {
    WIRE_MODEL_SPLITS
        .iter()
        .any(|(entry_family, pattern, entry_split)| {
            *entry_family == family && *entry_split == split && pattern.matches(wire_model_id)
        })
}

/// The media profile frozen into a run's render config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MediaProfile {
    /// The table entry this profile came from.
    pub profile: MediaProfileId,
    /// The catalog's modality statements for the model, as they stood at freeze. Only an
    /// explicit `true` admits a kind; false and unknown both refuse with reason `modality`.
    pub modalities: InputModalities,
    pub image: KindProfile,
    pub document: KindProfile,
    pub audio: KindProfile,
    pub video: KindProfile,
    /// Image side limits, each applying above an item count. The tightest applicable one wins.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dimension_limits: Vec<DimensionLimit>,
    /// The largest request, in encoded bytes with base64 included.
    pub request_cap_bytes: u64,
    /// The context window the token thresholds were derived from.
    pub context_tokens: u64,
    pub aging: AgingThresholds,
    /// The live-gated carriers that were enabled when this profile was frozen.
    #[serde(default, skip_serializing_if = "LiveGates::is_unset")]
    pub live_gated: LiveGates,
}

impl MediaProfile {
    /// Estimate tokens from the family facts table, never from compressed or base64 size.
    /// Missing dimensions use the fixed image budget rather than guessing a small image.
    pub fn media_tokens(&self, media: &MediaRef) -> u64 {
        if media.kind != MediaKind::Image {
            return match media.kind {
                MediaKind::Document => 30_000,
                MediaKind::Audio => 16_000,
                MediaKind::Video => 64_000,
                MediaKind::Image => unreachable!(),
            };
        }
        let dimensions = media
            .width
            .zip(media.height)
            .filter(|(w, h)| *w > 0 && *h > 0);
        match self.profile {
            MediaProfileId::Anthropic | MediaProfileId::Bedrock => dimensions
                .map_or(4_800, |(w, h)| {
                    (u64::from(w).div_ceil(28) * u64::from(h).div_ceil(28)).min(4_784)
                }),
            MediaProfileId::OpenaiResponses
            | MediaProfileId::OpenaiResponsesChatgpt
            | MediaProfileId::OpenaiChat => dimensions.map_or(4_800, |(w, h)| {
                let patches = (u64::from(w).div_ceil(32) * u64::from(h).div_ceil(32)).min(30_000);
                (patches * 6).div_ceil(5)
            }),
            MediaProfileId::Deepseek => 1_024,
            MediaProfileId::Gemini3 | MediaProfileId::CodeAssistGemini3 => 1_120,
            MediaProfileId::Gemini | MediaProfileId::CodeAssistGemini => {
                dimensions.map_or(4_800, |(w, h)| {
                    if w <= 384 && h <= 384 {
                        258
                    } else {
                        258 * u64::from(w).div_ceil(768) * u64::from(h).div_ceil(768)
                    }
                })
            }
            _ => 4_800,
        }
    }

    /// What this profile does with one kind of media.
    pub fn kind(&self, kind: MediaKind) -> &KindProfile {
        match kind {
            MediaKind::Image => &self.image,
            MediaKind::Document => &self.document,
            MediaKind::Audio => &self.audio,
            MediaKind::Video => &self.video,
        }
    }

    /// Whether the model's frozen catalog modality admits this kind: `image` gates images, `pdf`
    /// gates documents, `audio` and `video` gate themselves.
    pub fn accepts_modality(&self, kind: MediaKind) -> bool {
        let claim = match kind {
            MediaKind::Image => self.modalities.image,
            MediaKind::Document => self.modalities.pdf,
            MediaKind::Audio => self.modalities.audio,
            MediaKind::Video => self.modalities.video,
        };
        claim == Some(true)
    }

    /// The longest image side allowed in a request holding `images` images and `documents`
    /// documents, or `None` when no limit applies.
    pub fn max_image_side(&self, images: u32, documents: u32) -> Option<u32> {
        self.dimension_limits
            .iter()
            .filter(|limit| {
                let counted = if limit.counts_documents {
                    images.saturating_add(documents)
                } else {
                    images
                };
                counted > limit.above_count
            })
            .map(|limit| limit.max_side_px)
            .min()
    }

    /// Check the aging thresholds against this profile's request cap and context window.
    pub fn validate(&self) -> Result<(), MediaProfileError> {
        self.aging
            .validate(self.request_cap_bytes, self.context_tokens)
    }
}

/// Everything a table entry fixes, before the catalog's modalities and the context window are
/// added.
struct ProfileEntry {
    image: KindProfile,
    document: KindProfile,
    audio: KindProfile,
    video: KindProfile,
    dimension_limits: &'static [DimensionLimit],
    request_cap_bytes: u64,
}

/// Select the profile entry for a family, a provider media quirk and a wire model id.
fn select_profile_id(
    family: WireFamilyId,
    quirk: Option<MediaQuirk>,
    wire_model_id: &str,
) -> MediaProfileId {
    use MediaProfileId as P;
    use MediaQuirk as Q;
    let gemini3 = has_split(
        family,
        wire_model_id,
        GenerationSplit::GeminiFunctionResponseParts,
    );
    match (family, quirk) {
        (WireFamilyId::AnthropicMessages, Some(Q::Anthropic)) => P::Anthropic,
        // These three families each reach exactly one provider's endpoint, so the family alone
        // names the provider.
        (WireFamilyId::BedrockConverse, _) => P::Bedrock,
        (WireFamilyId::OpenAiResponsesChatgpt, _) => P::OpenaiResponsesChatgpt,
        (WireFamilyId::GoogleCodeAssist, _) if gemini3 => P::CodeAssistGemini3,
        (WireFamilyId::GoogleCodeAssist, _) => P::CodeAssistGemini,
        (WireFamilyId::OpenAiResponses, Some(Q::Openai)) => P::OpenaiResponses,
        (WireFamilyId::OpenAiResponses, Some(Q::Meta)) => P::MetaResponses,
        (WireFamilyId::OpenAiChat, Some(Q::Openai)) => P::OpenaiChat,
        (WireFamilyId::OpenAiChat, Some(Q::Openrouter)) => P::Openrouter,
        (WireFamilyId::OpenAiChat, Some(Q::Xai)) => P::Xai,
        (WireFamilyId::OpenAiChat, Some(Q::Deepseek)) => P::Deepseek,
        (WireFamilyId::OpenAiChat, Some(Q::Kimi)) => P::Kimi,
        (WireFamilyId::OpenAiChat, Some(Q::Meta)) => P::MetaChat,
        (WireFamilyId::Gemini, Some(Q::Google)) if gemini3 => P::Gemini3,
        (WireFamilyId::Gemini, Some(Q::Google)) => P::Gemini,
        _ => P::Conservative,
    }
}

/// Anthropic: 8000 px per side always, 2000 px once a request holds more than 20 images.
const ANTHROPIC_DIMENSIONS: &[DimensionLimit] = &[
    DimensionLimit::new(0, 8_000),
    DimensionLimit::new(20, 2_000),
];

/// Bedrock: 8000 px per side always; 2000 px above 20 image-or-document blocks. The second limit
/// is Anthropic's rule for Claude on Bedrock, applied to every Bedrock model because it is the
/// stricter one and the profile does not depend on the model.
const BEDROCK_DIMENSIONS: &[DimensionLimit] = &[
    DimensionLimit::new(0, 8_000),
    DimensionLimit {
        above_count: 20,
        max_side_px: 2_000,
        counts_documents: true,
    },
];

/// DeepSeek: 8192 px per side always; 4096 px once the request holds 15 or more images.
const DEEPSEEK_DIMENSIONS: &[DimensionLimit] = &[
    DimensionLimit::new(0, 8_192),
    DimensionLimit::new(14, 4_096),
];

/// The Anthropic image count limit: 600 per request, but 100 for models with a window of 200k
/// tokens or less. An unknown window takes the smaller limit.
fn anthropic_image_count(context_tokens: Option<u64>) -> u32 {
    match context_tokens {
        Some(window) if window > 200_000 => 600,
        _ => 100,
    }
}

/// The values each profile entry takes from `docs/media-families.md`. `gates` are the entry's
/// live-checked carriers and `context_tokens` the catalog's stated window, which only
/// Anthropic's image count depends on.
fn profile_entry(
    id: MediaProfileId,
    gates: LiveGates,
    context_tokens: Option<u64>,
) -> ProfileEntry {
    use ByteLimit::{Decoded, Encoded};
    use MediaCarrier::{FollowUpUserMessage, Marker, Native};
    use MediaProfileId as P;
    let follow_up = if gates.follow_up_user_message {
        FollowUpUserMessage
    } else {
        Marker
    };
    let gemini = |tool_parts: bool| -> ProfileEntry {
        // Gemini 3 accepts images and PDFs in `functionResponse.parts`; earlier models have no
        // media slot in a tool result. Audio and video are never accepted there.
        let tool = if tool_parts { Native } else { follow_up };
        ProfileEntry {
            image: KindProfile::new(Native, tool, Some(Decoded(7 * MB)), Some(3_000)),
            document: KindProfile::new(Native, tool, Some(Decoded(50 * MB)), None),
            audio: KindProfile::new(Native, Marker, None, Some(1)),
            video: KindProfile::new(Native, Marker, None, Some(10)),
            dimension_limits: &[],
            request_cap_bytes: 20 * MB,
        }
    };
    let code_assist = |tool_parts: bool| -> ProfileEntry {
        // The endpoint is undocumented: every media row waits for a live check, keeping the
        // Gemini limits it would then use.
        let mut entry = gemini(tool_parts);
        if !gates.code_assist_media {
            for kind in [
                &mut entry.image,
                &mut entry.document,
                &mut entry.audio,
                &mut entry.video,
            ] {
                kind.user_message = Marker;
                kind.tool_result = Marker;
            }
        }
        entry
    };
    match id {
        P::Anthropic => ProfileEntry {
            image: KindProfile::new(
                Native,
                Native,
                Some(Encoded(10 * MB)),
                Some(anthropic_image_count(context_tokens)),
            ),
            // The 600/100 limit counts PDF pages, which broca does not count.
            document: KindProfile::new(Native, Native, None, None),
            audio: KindProfile::MARKER,
            video: KindProfile::MARKER,
            dimension_limits: ANTHROPIC_DIMENSIONS,
            request_cap_bytes: 32 * MB,
        },
        P::Bedrock => ProfileEntry {
            image: KindProfile::new(Native, Native, Some(Decoded(3_750_000)), Some(20)),
            document: KindProfile::new(Native, Native, Some(Decoded(4_500_000)), Some(5)),
            // `ToolResultContentBlock` has no audio member.
            audio: KindProfile::new(Native, Marker, None, None),
            video: KindProfile::new(Native, Native, None, Some(1)),
            dimension_limits: BEDROCK_DIMENSIONS,
            request_cap_bytes: 20 * MB,
        },
        P::OpenaiResponses => ProfileEntry {
            // The image limit is a patch count after the model's resizing, not a byte size.
            image: KindProfile::new(Native, Native, None, Some(1_500)),
            // "Under 50 MB per file."
            document: KindProfile::new(Native, Native, Some(Decoded(50 * MB - 1)), None),
            audio: KindProfile::MARKER,
            video: KindProfile::MARKER,
            dimension_limits: &[],
            request_cap_bytes: 50 * MB,
        },
        P::OpenaiResponsesChatgpt => {
            let audio = if gates.chatgpt_input_audio {
                KindProfile::new(Native, Native, None, None)
            } else {
                KindProfile::MARKER
            };
            ProfileEntry {
                image: KindProfile::new(Native, Native, None, Some(1_500)),
                // The Codex client never sends `input_file`, so documents are not evidenced.
                document: KindProfile::MARKER,
                audio,
                video: KindProfile::MARKER,
                dimension_limits: &[],
                request_cap_bytes: 50 * MB,
            }
        }
        P::OpenaiChat => ProfileEntry {
            image: KindProfile::new(Native, follow_up, None, Some(1_500)),
            document: KindProfile::new(Native, follow_up, Some(Decoded(50 * MB - 1)), None),
            audio: KindProfile::new(Native, follow_up, None, None),
            video: KindProfile::MARKER,
            dimension_limits: &[],
            request_cap_bytes: 50 * MB,
        },
        P::Openrouter => ProfileEntry {
            image: KindProfile::new(Native, follow_up, None, None),
            document: KindProfile::new(Native, follow_up, None, None),
            audio: KindProfile::new(Native, follow_up, None, None),
            video: KindProfile::new(Native, follow_up, None, None),
            dimension_limits: &[],
            request_cap_bytes: 20 * MB,
        },
        P::Xai => ProfileEntry {
            image: KindProfile::new(Native, follow_up, Some(Decoded(20 * MIB)), None),
            document: KindProfile::MARKER,
            audio: KindProfile::MARKER,
            video: KindProfile::MARKER,
            dimension_limits: &[],
            request_cap_bytes: 20 * MIB,
        },
        P::Deepseek => ProfileEntry {
            image: KindProfile::new(Native, follow_up, Some(Decoded(32 * MIB)), Some(600)),
            document: KindProfile::MARKER,
            audio: KindProfile::MARKER,
            video: KindProfile::MARKER,
            dimension_limits: DEEPSEEK_DIMENSIONS,
            request_cap_bytes: 48 * MIB,
        },
        P::Kimi => ProfileEntry {
            image: KindProfile::new(Native, follow_up, None, None),
            document: KindProfile::MARKER,
            audio: KindProfile::MARKER,
            video: KindProfile::new(Native, follow_up, None, None),
            dimension_limits: &[],
            request_cap_bytes: 100 * MB,
        },
        P::MetaChat => ProfileEntry {
            image: KindProfile::new(Native, follow_up, Some(Decoded(50 * MB)), Some(50)),
            document: KindProfile::new(Native, follow_up, Some(Decoded(50 * MB)), None),
            audio: KindProfile::new(Native, follow_up, Some(Decoded(50 * MB)), None),
            video: KindProfile::new(Native, follow_up, Some(Decoded(50 * MB)), None),
            dimension_limits: &[],
            request_cap_bytes: 50 * MB,
        },
        // Meta's limits in the Responses grammar, which has no audio or video part.
        P::MetaResponses => ProfileEntry {
            image: KindProfile::new(Native, follow_up, Some(Decoded(50 * MB)), Some(50)),
            document: KindProfile::new(Native, follow_up, Some(Decoded(50 * MB)), None),
            audio: KindProfile::MARKER,
            video: KindProfile::MARKER,
            dimension_limits: &[],
            request_cap_bytes: 50 * MB,
        },
        P::Gemini3 => gemini(true),
        P::Gemini => gemini(false),
        P::CodeAssistGemini3 => code_assist(true),
        P::CodeAssistGemini => code_assist(false),
        // The smallest request cap any profile documents.
        P::Conservative => ProfileEntry {
            image: KindProfile::MARKER,
            document: KindProfile::MARKER,
            audio: KindProfile::MARKER,
            video: KindProfile::MARKER,
            dimension_limits: &[],
            request_cap_bytes: 20 * MB,
        },
    }
}

/// Build the profile for a family, a provider media quirk, a wire model id, the catalog's modality
/// statements and its stated context window, using the live checks that have passed so far.
pub fn select_media_profile(
    family: WireFamilyId,
    quirk: Option<MediaQuirk>,
    wire_model_id: &str,
    modalities: InputModalities,
    context_tokens: Option<u64>,
) -> MediaProfile {
    let id = select_profile_id(family, quirk, wire_model_id);
    build_profile(id, live_gates(id), modalities, context_tokens)
}

fn build_profile(
    id: MediaProfileId,
    gates: LiveGates,
    modalities: InputModalities,
    context_tokens: Option<u64>,
) -> MediaProfile {
    let entry = profile_entry(id, gates, context_tokens);
    let window = context_tokens
        .filter(|&window| window >= MIN_CONTEXT_TOKENS)
        .unwrap_or(FALLBACK_CONTEXT_TOKENS);
    MediaProfile {
        profile: id,
        modalities,
        image: entry.image,
        document: entry.document,
        audio: entry.audio,
        video: entry.video,
        dimension_limits: entry.dimension_limits.to_vec(),
        request_cap_bytes: entry.request_cap_bytes,
        context_tokens: window,
        aging: AgingThresholds::derived(entry.request_cap_bytes, window),
        live_gated: gates,
    }
}

/// Freeze the media profile into an admitted run's render config. This is the one call admission
/// (and a model switch's refreeze) makes, after the config's wire family and wire model id are
/// final, so an auth method that moves the run to another family (ChatGPT, Code Assist) gets that
/// family's profile.
///
/// The provider's media quirk comes from its spec when set, otherwise from its catalog provider
/// id. The model's catalog modalities and context window are read once, here; a later catalog
/// change leaves the frozen profile as it is.
pub fn freeze_media_profile(
    frozen: &mut FrozenRenderConfig,
    provider: &ProviderSpec,
    model: &ModelSpec,
) {
    let quirk = provider
        .quirks
        .media
        .or_else(|| media_quirk_for_catalog_provider(&provider.id));
    frozen.media = Some(select_media_profile(
        frozen.family_id,
        quirk,
        &frozen.wire_model_id,
        model.input_modalities,
        model.limits.context,
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::Quirks;
    use crate::spec::{AuthKind, Limits};

    const ALL_IDS: [MediaProfileId; 16] = [
        MediaProfileId::Anthropic,
        MediaProfileId::Bedrock,
        MediaProfileId::OpenaiResponses,
        MediaProfileId::OpenaiResponsesChatgpt,
        MediaProfileId::OpenaiChat,
        MediaProfileId::Openrouter,
        MediaProfileId::Xai,
        MediaProfileId::Deepseek,
        MediaProfileId::Kimi,
        MediaProfileId::MetaChat,
        MediaProfileId::MetaResponses,
        MediaProfileId::Gemini3,
        MediaProfileId::Gemini,
        MediaProfileId::CodeAssistGemini3,
        MediaProfileId::CodeAssistGemini,
        MediaProfileId::Conservative,
    ];

    fn all_kinds(profile: &MediaProfile) -> [(MediaKind, &KindProfile); 4] {
        [
            MediaKind::Image,
            MediaKind::Document,
            MediaKind::Audio,
            MediaKind::Video,
        ]
        .map(|kind| (kind, profile.kind(kind)))
    }

    fn select(family: WireFamilyId, quirk: Option<MediaQuirk>, model: &str) -> MediaProfile {
        select_media_profile(family, quirk, model, InputModalities::default(), None)
    }

    fn provider(id: &str, family: WireFamilyId, media: Option<MediaQuirk>) -> ProviderSpec {
        ProviderSpec {
            id: id.into(),
            wire_family: family,
            base_url: String::new(),
            auth_kind: AuthKind::Bearer,
            static_headers: Default::default(),
            static_options: serde_json::Value::Null,
            quirks: Quirks {
                media,
                ..Default::default()
            },
            raw: serde_json::Value::Null,
        }
    }

    fn model(id: &str, modalities: serde_json::Value, context: Option<u64>) -> ModelSpec {
        let mut model: ModelSpec = serde_json::from_value(serde_json::json!({ "id": id })).unwrap();
        model.input_modalities = InputModalities::from_value(&modalities);
        model.limits = Limits {
            context,
            ..Default::default()
        };
        model
    }

    #[test]
    fn profile_selection_follows_family_quirk_and_wire_model() {
        use MediaProfileId as P;
        use MediaQuirk as Q;
        use WireFamilyId as F;
        for (family, quirk, wire_model, expected) in [
            (
                F::AnthropicMessages,
                Some(Q::Anthropic),
                "claude-opus-5",
                P::Anthropic,
            ),
            (F::BedrockConverse, None, "anthropic.claude-5", P::Bedrock),
            (
                F::OpenAiResponses,
                Some(Q::Openai),
                "gpt-5.5",
                P::OpenaiResponses,
            ),
            (
                F::OpenAiResponses,
                Some(Q::Meta),
                "llama-5",
                P::MetaResponses,
            ),
            (
                F::OpenAiResponsesChatgpt,
                None,
                "gpt-5.5",
                P::OpenaiResponsesChatgpt,
            ),
            (F::OpenAiChat, Some(Q::Openai), "gpt-4o", P::OpenaiChat),
            (
                F::OpenAiChat,
                Some(Q::Openrouter),
                "any/model",
                P::Openrouter,
            ),
            (F::OpenAiChat, Some(Q::Xai), "grok-4", P::Xai),
            (F::OpenAiChat, Some(Q::Deepseek), "deepseek-v4", P::Deepseek),
            (F::OpenAiChat, Some(Q::Kimi), "kimi-k2.6", P::Kimi),
            (F::OpenAiChat, Some(Q::Meta), "llama-4", P::MetaChat),
            (
                F::Gemini,
                Some(Q::Google),
                "gemini-3-pro-preview",
                P::Gemini3,
            ),
            (F::Gemini, Some(Q::Google), "gemini-3.1-flash", P::Gemini3),
            (F::Gemini, Some(Q::Google), "gemini-4-ultra", P::Gemini3),
            (
                F::Gemini,
                Some(Q::Google),
                "publishers/google/models/gemini-3-pro",
                P::Gemini3,
            ),
            (F::Gemini, Some(Q::Google), "gemini-2.5-pro", P::Gemini),
            (F::Gemini, Some(Q::Google), "gemini-1.5-flash", P::Gemini),
            (F::Gemini, Some(Q::Google), "gemma-3-27b", P::Gemini),
            (
                F::GoogleCodeAssist,
                None,
                "gemini-3-pro-high",
                P::CodeAssistGemini3,
            ),
            (
                F::GoogleCodeAssist,
                None,
                "gemini-2.5-flash",
                P::CodeAssistGemini,
            ),
            // No profile entry: another provider on a shared family, or a quirk that belongs to
            // a different family.
            (F::AnthropicMessages, None, "MiniMax-M3", P::Conservative),
            (F::OpenAiChat, None, "qwen3", P::Conservative),
            (F::OpenAiResponses, Some(Q::Xai), "grok-4", P::Conservative),
            (F::Gemini, None, "gemini-3-pro", P::Conservative),
            (
                F::AnthropicMessages,
                Some(Q::Openai),
                "claude",
                P::Conservative,
            ),
        ] {
            assert_eq!(
                select(family, quirk, wire_model).profile,
                expected,
                "{family:?} {quirk:?} {wire_model}"
            );
        }
    }

    #[test]
    fn gemini_3_carries_tool_images_and_earlier_gemini_does_not() {
        let gemini3 = select(
            WireFamilyId::Gemini,
            Some(MediaQuirk::Google),
            "gemini-3-pro",
        );
        let gemini25 = select(
            WireFamilyId::Gemini,
            Some(MediaQuirk::Google),
            "gemini-2.5-pro",
        );
        for profile in [&gemini3, &gemini25] {
            for (kind, entry) in all_kinds(profile) {
                assert_eq!(entry.user_message, MediaCarrier::Native, "{kind:?}");
            }
            assert_eq!(profile.audio.tool_result, MediaCarrier::Marker);
            assert_eq!(profile.video.tool_result, MediaCarrier::Marker);
            assert_eq!(profile.request_cap_bytes, 20 * MB);
        }
        assert_eq!(gemini3.image.tool_result, MediaCarrier::Native);
        assert_eq!(gemini3.document.tool_result, MediaCarrier::Native);
        // Earlier Gemini has no media slot in `functionResponse`, and the follow-up user message
        // is not live-checked yet.
        assert_eq!(gemini25.image.tool_result, MediaCarrier::Marker);
        assert_eq!(gemini25.document.tool_result, MediaCarrier::Marker);
    }

    #[test]
    fn chatgpt_input_audio_defaults_to_unset() {
        assert!(
            !live_gates(MediaProfileId::OpenaiResponsesChatgpt).chatgpt_input_audio,
            "the ChatGPT input_audio flag is set only after a live check passes"
        );
        let profile = select(WireFamilyId::OpenAiResponsesChatgpt, None, "gpt-5.5");
        assert_eq!(profile.audio.user_message, MediaCarrier::Marker);
        assert_eq!(profile.audio.tool_result, MediaCarrier::Marker);
        assert!(profile.live_gated.is_unset());
        assert_eq!(profile.image.tool_result, MediaCarrier::Native);

        let checked = build_profile(
            MediaProfileId::OpenaiResponsesChatgpt,
            LiveGates {
                chatgpt_input_audio: true,
                ..LiveGates::UNSET
            },
            InputModalities::default(),
            None,
        );
        assert_eq!(checked.audio.user_message, MediaCarrier::Native);
        assert_eq!(checked.audio.tool_result, MediaCarrier::Native);
        assert!(checked.live_gated.chatgpt_input_audio);
    }

    #[test]
    fn live_gated_follow_up_and_code_assist_rows_stay_markers_until_checked() {
        assert!(LIVE_CHECKED_GATES.is_empty());
        let chat = select(WireFamilyId::OpenAiChat, Some(MediaQuirk::Openai), "gpt-4o");
        assert_eq!(chat.image.user_message, MediaCarrier::Native);
        assert_eq!(chat.image.tool_result, MediaCarrier::Marker);
        let checked = build_profile(
            MediaProfileId::OpenaiChat,
            LiveGates {
                follow_up_user_message: true,
                ..LiveGates::UNSET
            },
            InputModalities::default(),
            None,
        );
        assert_eq!(checked.image.tool_result, MediaCarrier::FollowUpUserMessage);
        assert_eq!(checked.video.tool_result, MediaCarrier::Marker);

        let code_assist = select(WireFamilyId::GoogleCodeAssist, None, "gemini-3-pro");
        for (kind, entry) in all_kinds(&code_assist) {
            assert_eq!(entry.user_message, MediaCarrier::Marker, "{kind:?}");
            assert_eq!(entry.tool_result, MediaCarrier::Marker, "{kind:?}");
        }
        let checked = build_profile(
            MediaProfileId::CodeAssistGemini3,
            LiveGates {
                code_assist_media: true,
                ..LiveGates::UNSET
            },
            InputModalities::default(),
            None,
        );
        assert_eq!(checked.image.tool_result, MediaCarrier::Native);
        assert_eq!(checked.video.user_message, MediaCarrier::Native);
    }

    #[test]
    fn providers_without_a_profile_get_the_conservative_profile() {
        let image_capable = serde_json::json!(["text", "image", "pdf", "audio", "video"]);
        for (provider_id, family) in [
            ("minimax", WireFamilyId::AnthropicMessages),
            ("ollama-cloud", WireFamilyId::OpenAiChat),
            ("some-new-gateway", WireFamilyId::Gemini),
        ] {
            let mut frozen = crate::families::frozen_config_for_family(family, "m");
            freeze_media_profile(
                &mut frozen,
                &provider(provider_id, family, None),
                &model("m", image_capable.clone(), Some(200_000)),
            );
            let profile = frozen.media.expect("freeze sets a profile");
            assert_eq!(
                profile.profile,
                MediaProfileId::Conservative,
                "{provider_id}"
            );
            // The catalog may say the model accepts every modality; the request format is still
            // unknown, so a person's media is refused and tool media renders as markers.
            assert!(profile.accepts_modality(MediaKind::Image));
            for (kind, entry) in all_kinds(&profile) {
                assert_eq!(
                    entry.user_message,
                    MediaCarrier::Marker,
                    "{provider_id} {kind:?}"
                );
                assert_eq!(
                    entry.tool_result,
                    MediaCarrier::Marker,
                    "{provider_id} {kind:?}"
                );
            }
        }
    }

    #[test]
    fn freeze_uses_the_spec_quirk_before_the_catalog_id_and_the_final_family() {
        let text_only = model("m", serde_json::json!(["text"]), None);
        // A spec quirk wins over the provider id's table entry.
        let mut frozen = crate::families::openai_chat_frozen_config("m");
        freeze_media_profile(
            &mut frozen,
            &provider(
                "openai",
                WireFamilyId::OpenAiChat,
                Some(MediaQuirk::Deepseek),
            ),
            &text_only,
        );
        assert_eq!(
            frozen.media.as_ref().unwrap().profile,
            MediaProfileId::Deepseek
        );
        // Without one, the catalog provider id decides.
        for (id, expected) in [
            ("moonshotai-cn", MediaProfileId::Kimi),
            ("llama", MediaProfileId::MetaChat),
            ("openrouter", MediaProfileId::Openrouter),
        ] {
            let mut frozen = crate::families::openai_chat_frozen_config("m");
            freeze_media_profile(
                &mut frozen,
                &provider(id, WireFamilyId::OpenAiChat, None),
                &text_only,
            );
            assert_eq!(frozen.media.unwrap().profile, expected, "{id}");
        }
        // An auth method that moved the run to the ChatGPT backend gets that family's profile,
        // even though the catalog provider speaks the Responses API.
        let mut frozen = crate::families::openai_responses_chatgpt_frozen_config("gpt-5.5");
        freeze_media_profile(
            &mut frozen,
            &provider("openai", WireFamilyId::OpenAiResponses, None),
            &text_only,
        );
        let profile = frozen.media.unwrap();
        assert_eq!(profile.profile, MediaProfileId::OpenaiResponsesChatgpt);
        // The modalities are the catalog's, as they stood at freeze.
        assert!(!profile.accepts_modality(MediaKind::Image));
        assert_eq!(profile.modalities.text, Some(true));
    }

    #[test]
    fn profile_values_match_the_family_facts_table() {
        let anthropic = |context| {
            select_media_profile(
                WireFamilyId::AnthropicMessages,
                Some(MediaQuirk::Anthropic),
                "claude",
                InputModalities::default(),
                context,
            )
        };
        let claude = anthropic(Some(1_000_000));
        assert_eq!(claude.request_cap_bytes, 32 * MB);
        assert_eq!(
            claude.image.max_item_bytes,
            Some(ByteLimit::Encoded(10 * MB))
        );
        assert_eq!(claude.image.max_count, Some(600));
        assert_eq!(anthropic(Some(200_000)).image.max_count, Some(100));
        assert_eq!(anthropic(None).image.max_count, Some(100));
        assert_eq!(claude.audio, KindProfile::MARKER);
        // Above 20 images every image must be at most 2000 px per side.
        assert_eq!(claude.max_image_side(20, 0), Some(8_000));
        assert_eq!(claude.max_image_side(21, 0), Some(2_000));
        assert_eq!(claude.max_image_side(20, 5), Some(8_000));

        let bedrock = select(WireFamilyId::BedrockConverse, None, "anthropic.claude");
        assert_eq!(bedrock.request_cap_bytes, 20 * MB);
        assert_eq!(bedrock.image.max_count, Some(20));
        assert_eq!(bedrock.document.max_count, Some(5));
        assert_eq!(bedrock.video.max_count, Some(1));
        assert_eq!(bedrock.audio.tool_result, MediaCarrier::Marker);
        assert_eq!(bedrock.video.tool_result, MediaCarrier::Native);
        // On Bedrock documents count toward the 20.
        assert_eq!(bedrock.max_image_side(15, 6), Some(2_000));
        assert_eq!(bedrock.max_image_side(15, 5), Some(8_000));

        let deepseek = select(WireFamilyId::OpenAiChat, Some(MediaQuirk::Deepseek), "d");
        assert_eq!(deepseek.request_cap_bytes, 48 * MIB);
        assert_eq!(deepseek.max_image_side(14, 0), Some(8_192));
        assert_eq!(deepseek.max_image_side(15, 0), Some(4_096));
        assert_eq!(
            deepseek.image.max_item_bytes,
            Some(ByteLimit::Decoded(32 * MIB))
        );

        for (family, quirk, cap) in [
            (
                WireFamilyId::OpenAiResponses,
                Some(MediaQuirk::Openai),
                50 * MB,
            ),
            (WireFamilyId::OpenAiResponsesChatgpt, None, 50 * MB),
            (WireFamilyId::OpenAiChat, Some(MediaQuirk::Openai), 50 * MB),
            (
                WireFamilyId::OpenAiChat,
                Some(MediaQuirk::Openrouter),
                20 * MB,
            ),
            (WireFamilyId::OpenAiChat, Some(MediaQuirk::Xai), 20 * MIB),
            (WireFamilyId::OpenAiChat, Some(MediaQuirk::Kimi), 100 * MB),
            (WireFamilyId::OpenAiChat, Some(MediaQuirk::Meta), 50 * MB),
            (WireFamilyId::GoogleCodeAssist, None, 20 * MB),
            (WireFamilyId::AnthropicMessages, None, 20 * MB),
        ] {
            assert_eq!(
                select(family, quirk, "m").request_cap_bytes,
                cap,
                "{family:?} {quirk:?}"
            );
        }
        let meta = select(WireFamilyId::OpenAiChat, Some(MediaQuirk::Meta), "m");
        assert_eq!(meta.image.max_count, Some(50));
        let gemini = select(
            WireFamilyId::Gemini,
            Some(MediaQuirk::Google),
            "gemini-3-pro",
        );
        assert_eq!(
            gemini.image.max_item_bytes,
            Some(ByteLimit::Decoded(7 * MB))
        );
        assert_eq!(gemini.audio.max_count, Some(1));
        assert_eq!(gemini.video.max_count, Some(10));
    }

    #[test]
    fn byte_limits_compare_in_their_documented_measure() {
        // 10 MB of base64 holds 7,500,000 raw bytes.
        assert!(ByteLimit::Encoded(10 * MB).admits(7_500_000));
        assert!(!ByteLimit::Encoded(10 * MB).admits(7_500_001));
        assert!(ByteLimit::Decoded(3_750_000).admits(3_750_000));
        assert!(!ByteLimit::Decoded(3_750_000).admits(3_750_001));
        assert_eq!(base64_encoded_len(0), 0);
        assert_eq!(base64_encoded_len(1), 4);
        assert_eq!(base64_encoded_len(3), 4);
        assert_eq!(base64_encoded_len(4), 8);
    }

    #[test]
    fn aging_thresholds_reject_each_rule_violation() {
        let cap = 20 * MB;
        let context = 200_000;
        let valid = AgingThresholds {
            byte_trigger: cap / 2,
            byte_target: cap / 4,
            token_trigger: context / 2,
            token_target: context / 4,
        };
        assert_eq!(valid.validate(cap, context), Ok(()));
        let broken = |edit: fn(&mut AgingThresholds)| {
            let mut thresholds = valid;
            edit(&mut thresholds);
            thresholds.validate(cap, context)
        };
        assert_eq!(
            broken(|t| t.byte_target = 0),
            Err(MediaProfileError::NotPositive("byte_target"))
        );
        assert_eq!(
            broken(|t| t.token_target = 0),
            Err(MediaProfileError::NotPositive("token_target"))
        );
        assert_eq!(
            broken(|t| t.byte_target = t.byte_trigger / 2 + 1),
            Err(MediaProfileError::ByteTargetAboveHalfTrigger)
        );
        assert_eq!(
            broken(|t| t.byte_trigger = 20 * MB / 2 + 1),
            Err(MediaProfileError::ByteTriggerAboveHalfCap)
        );
        assert_eq!(
            broken(|t| t.token_target = t.token_trigger / 2 + 1),
            Err(MediaProfileError::TokenTargetAboveHalfTrigger)
        );
        assert_eq!(
            broken(|t| t.token_trigger = 200_000 / 2 + 1),
            Err(MediaProfileError::TokenTriggerAboveHalfContext)
        );
    }

    #[test]
    fn every_frozen_profile_satisfies_the_aging_rules() {
        let gate_sets = [
            LiveGates::UNSET,
            LiveGates {
                chatgpt_input_audio: true,
                follow_up_user_message: true,
                code_assist_media: true,
            },
        ];
        for id in ALL_IDS {
            for gates in gate_sets {
                for context in [
                    None,
                    Some(0),
                    Some(7),
                    Some(8),
                    Some(200_000),
                    Some(2_000_000),
                ] {
                    let profile = build_profile(id, gates, InputModalities::default(), context);
                    assert_eq!(profile.validate(), Ok(()), "{id:?} {context:?}");
                    let expected_window = match context {
                        Some(window) if window >= MIN_CONTEXT_TOKENS => window,
                        _ => FALLBACK_CONTEXT_TOKENS,
                    };
                    assert_eq!(
                        profile.context_tokens, expected_window,
                        "{id:?} {context:?}"
                    );
                }
            }
        }
    }
    #[test]
    fn media_token_estimates_follow_family_rules_not_encoded_bytes() {
        let mut media = broca_protocol::MediaRef {
            kind: MediaKind::Image,
            mime: "image/png".into(),
            byte_size: 3,
            sha256: "a".repeat(64),
            width: Some(769),
            height: Some(385),
        };
        let mut profile = select(WireFamilyId::AnthropicMessages, None, "claude");
        for (id, expected) in [
            (MediaProfileId::Anthropic, 392),
            (MediaProfileId::Bedrock, 392),
            (MediaProfileId::OpenaiResponses, 390),
            (MediaProfileId::OpenaiResponsesChatgpt, 390),
            (MediaProfileId::OpenaiChat, 390),
            (MediaProfileId::Deepseek, 1_024),
            (MediaProfileId::Gemini3, 1_120),
            (MediaProfileId::CodeAssistGemini3, 1_120),
            (MediaProfileId::Gemini, 516),
            (MediaProfileId::CodeAssistGemini, 516),
            (MediaProfileId::Openrouter, 4_800),
            (MediaProfileId::Xai, 4_800),
            (MediaProfileId::Kimi, 4_800),
            (MediaProfileId::MetaChat, 4_800),
            (MediaProfileId::MetaResponses, 4_800),
            (MediaProfileId::Conservative, 4_800),
        ] {
            profile.profile = id;
            assert_eq!(profile.media_tokens(&media), expected, "{id:?}");
            media.byte_size = 30_000_000;
            assert_eq!(
                profile.media_tokens(&media),
                expected,
                "compressed size changed {id:?} tokens"
            );
            media.byte_size = 3;
        }
        profile.profile = MediaProfileId::Anthropic;
        media.width = Some(8_000);
        media.height = Some(8_000);
        assert_eq!(profile.media_tokens(&media), 4_784);
        profile.profile = MediaProfileId::OpenaiResponses;
        assert_eq!(profile.media_tokens(&media), 36_000);
        profile.profile = MediaProfileId::Gemini;
        media.width = Some(384);
        media.height = Some(384);
        assert_eq!(profile.media_tokens(&media), 258);
        for (kind, expected) in [
            (MediaKind::Document, 30_000),
            (MediaKind::Audio, 16_000),
            (MediaKind::Video, 64_000),
        ] {
            media.kind = kind;
            assert_eq!(profile.media_tokens(&media), expected);
        }
    }
}
