//! Typed within-family quirk hooks.
//!
//! Providers in the same wire family still differ in small, structural ways
//! (how reasoning is requested, how tool ids round-trip, where the system prompt
//! goes, how usage is read). Those differences are encoded as DATA — a small set
//! of typed enums selected per provider — and the family renderer matches on the
//! enum. This keeps quirks out of the renderer as branches on a provider id:
//! adding a provider is adding data, not adding `if provider == X`.
//!
//! Each enum has a safe default so a minimal `ProviderSpec` is valid.

use serde::{Deserialize, Serialize};

/// The typed quirk selections carried by a `ProviderSpec`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Quirks {
    #[serde(default)]
    pub reasoning: ReasoningPolicy,
    #[serde(default)]
    pub tool_id: ToolIdPolicy,
    #[serde(default)]
    pub system_prompt: SystemPromptPolicy,
    #[serde(default)]
    pub usage: UsagePolicy,
    #[serde(default)]
    pub token_limit_field: TokenLimitField,
    /// Which reasoning to send back on OpenAI Chat assistant messages. Selected per API and
    /// frozen when a run starts so resumed requests retain their original wire shape.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_echo: Option<ReasoningEchoPolicy>,
    /// Whether this provider rejects `content: null` on a tool-call-only assistant turn
    /// (ollama's OpenAI-compat endpoint 400s with "invalid message content type: <nil>").
    /// When set, the OpenAI-Chat render uses `"content": ""` instead of `null` on ANY
    /// text-less assistant turn -- with tool calls or without. The narrower "tool calls and
    /// no text" reading is how this was first written and it left a hole: a step can bill
    /// output tokens and assemble neither text nor a call, and that empty turn is durable, so
    /// the next episode rendered `{"role":"assistant","content":null}` and was refused 400.
    /// DEFAULT OFF: opt-in per provider via the catalog; unset leaves the byte-identical
    /// OpenAI convention (`null`).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub tool_call_content_empty_string: bool,
    /// Which provider's media profile applies on a wire family several providers share (see
    /// `crate::media_profile`). Unset falls back to the catalog provider id's entry, and a
    /// provider with neither gets the conservative profile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media: Option<crate::media_profile::MediaQuirk>,
    /// Whether the provider's API requires a `type` on every tool schema node with an `enum`
    /// (Moonshot's, which serves the Kimi routes). Frozen into
    /// `FrozenRenderConfig::moonshot_schema_types`. DEFAULT OFF.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub moonshot_schema_types: bool,
}

/// Which assistant turns carry continuation state. Neither scope trims history at a user turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningEchoTurns {
    EveryAssistant,
    ToolCalls,
}

/// The wire field also determines the representation: scalar text or opaque structured details.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningEchoFormat {
    ReasoningContent,
    OpenRouterDetails,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReasoningEchoPolicy {
    pub format: ReasoningEchoFormat,
    pub turns: ReasoningEchoTurns,
}

impl ReasoningEchoPolicy {
    pub const DEEPSEEK: Self = Self {
        format: ReasoningEchoFormat::ReasoningContent,
        turns: ReasoningEchoTurns::EveryAssistant,
    };
    pub const KIMI: Self = Self {
        format: ReasoningEchoFormat::ReasoningContent,
        turns: ReasoningEchoTurns::EveryAssistant,
    };
    pub const OPENROUTER: Self = Self {
        format: ReasoningEchoFormat::OpenRouterDetails,
        turns: ReasoningEchoTurns::EveryAssistant,
    };
}

/// Which request field carries the output-token limit, and where it sits. Real
/// cross-provider deviations, selected as data per provider and read by the family —
/// never a provider-name branch:
///  - classic `/chat/completions` (+ most OpenAI-compatible servers) take a
///    top-level `max_tokens`;
///  - OpenAI reasoning (o-series) models require top-level `max_completion_tokens`
///    and reject `max_tokens`;
///  - the OpenAI Responses API uses a top-level `max_output_tokens`;
///  - Gemini uses `maxOutputTokens` NESTED under `generationConfig`.
///
/// The OpenAI `max_output_tokens` and the Gemini `maxOutputTokens` are DISTINCT
/// variants on purpose — same English name, different wire key AND placement, so
/// they must not collapse into one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TokenLimitField {
    #[default]
    MaxTokens,
    MaxCompletionTokens,
    /// OpenAI Responses top-level `max_output_tokens`.
    MaxOutputTokens,
    /// Google Gemini `generationConfig.maxOutputTokens` (camelCase, nested).
    GeminiMaxOutputTokens,
}

impl TokenLimitField {
    /// The request body key this field uses.
    pub fn key(self) -> &'static str {
        match self {
            TokenLimitField::MaxTokens => "max_tokens",
            TokenLimitField::MaxCompletionTokens => "max_completion_tokens",
            TokenLimitField::MaxOutputTokens => "max_output_tokens",
            TokenLimitField::GeminiMaxOutputTokens => "maxOutputTokens",
        }
    }

    /// Whether the limit lives nested under `generationConfig` (Gemini) rather than
    /// at the top level.
    pub fn in_generation_config(self) -> bool {
        matches!(self, TokenLimitField::GeminiMaxOutputTokens)
    }
}

/// How a provider wants reasoning ("thinking") requested + echoed back.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum ReasoningPolicy {
    /// No reasoning support.
    #[default]
    None,
    /// Anthropic interleaved thinking. When the historical `last_turn_only` switch is on,
    /// every retained assistant reply may echo its signed thinking if the target model accepts
    /// it and the earlier request content covered by its signature is unchanged. When off, none
    /// is echoed.
    AnthropicInterleaved {
        budget_tokens: u32,
        last_turn_only: bool,
        /// Which model's signed thinking may be echoed to the request's model. Absent from
        /// configs frozen before this field existed, which read back as `SameModelOnly` and
        /// serialize without it, so those runs keep their recorded bytes on resume.
        #[serde(
            default,
            skip_serializing_if = "CrossModelThinking::is_same_model_only"
        )]
        cross_model: CrossModelThinking,
    },
    /// OpenAI Responses / o-series: a top-level `reasoning` object (e.g.
    /// `{ "effort": "medium" }`). Effort is the primary knob; optional summary is
    /// forwarded when set.
    OpenAiResponsesReasoning {
        effort: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        summary: Option<String>,
    },
}

impl Serialize for ReasoningPolicy {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        match self {
            Self::None => {
                let mut state = serializer.serialize_struct("ReasoningPolicy", 1)?;
                state.serialize_field("mode", "none")?;
                state.end()
            }
            Self::AnthropicInterleaved {
                budget_tokens,
                last_turn_only,
                cross_model,
            } => {
                let include_cross_model = !cross_model.is_same_model_only();
                let mut state = serializer
                    .serialize_struct("ReasoningPolicy", 4 + usize::from(include_cross_model))?;
                state.serialize_field("mode", "anthropic_interleaved")?;
                state.serialize_field("budget_tokens", budget_tokens)?;
                state.serialize_field("last_turn_only", last_turn_only)?;
                // Record all-assistant echo explicitly. An omitted marker means the old
                // behavior echoed only the last assistant reply's thinking.
                state.serialize_field(
                    "echo_scope",
                    broca_core::provider_errors::ANTHROPIC_ECHO_SCOPE,
                )?;
                if include_cross_model {
                    state.serialize_field("cross_model", cross_model)?;
                }
                state.end()
            }
            Self::OpenAiResponsesReasoning { effort, summary } => {
                let mut state = serializer
                    .serialize_struct("ReasoningPolicy", 2 + usize::from(summary.is_some()))?;
                state.serialize_field("mode", "open_ai_responses_reasoning")?;
                state.serialize_field("effort", effort)?;
                if let Some(summary) = summary {
                    state.serialize_field("summary", summary)?;
                }
                state.end()
            }
        }
    }
}

/// Whose signed thinking is echoed back to the request's model under Anthropic interleaved
/// thinking, when the conversation has switched models.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CrossModelThinking {
    /// Only thinking the request's own model produced. The original rule, kept for runs frozen
    /// under it. Because it is re-applied to every message on every request, a session that
    /// switches models A -> B -> A removes A's thinking from the middle of history while on B,
    /// omits an A turn that carried only thinking, and puts A's thinking back on returning to A.
    /// Anthropic treats each of those as an edit that invalidates the thinking after it.
    #[default]
    SameModelOnly,
    /// Thinking from any model served by the first-party `anthropic` provider is echoed back
    /// unchanged to any other model of that provider; the API itself drops the blocks the target
    /// model cannot read (it lists them in `input_transformations` as `thinking_dropped`) instead
    /// of rejecting the request. Measured live on claude-opus-5-5, claude-opus-5 and
    /// claude-fable-5-1. Thinking from any other provider, or from an unknown model, is still
    /// stripped, and since that decision does not depend on the request's model, a message
    /// renders the same way on every request. This is what current runs freeze.
    AnyAnthropicModel,
}

impl CrossModelThinking {
    /// Serde helper: the legacy default is left out of serialized configs.
    pub fn is_same_model_only(&self) -> bool {
        *self == CrossModelThinking::SameModelOnly
    }
}

/// How tool-call ids round-trip.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolIdPolicy {
    /// Ids pass through unchanged (the provider echoes the same id back).
    #[default]
    Passthrough,
    /// The provider doesn't echo ids; the runner maps them by index instead.
    SyntheticIndex,
}

/// Where the system prompt goes in the request.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum SystemPromptPolicy {
    /// A top-level `system` field (Anthropic).
    TopLevelField,
    /// A leading message with role `system` (OpenAI chat).
    #[default]
    SystemMessage,
    /// Google Gemini `systemInstruction: { parts: [{ text }] }` (separate from contents).
    SystemInstructionField,
    /// A required identity string that MUST lead the system content, then the
    /// caller's system messages. Some providers reject the request without it
    /// (e.g. an OAuth client identity).
    RequiredIdentityLead { identity: String },
    /// OpenAI Responses: system/developer text is merged into the top-level
    /// `instructions` string (not an `input` message item).
    InstructionsField,
    /// OpenAI Responses: the same merged text as `InstructionsField`, sent instead as one
    /// leading `developer` message in `input`. GPT-6.2 never caches top-level `instructions`,
    /// so every step would pay the whole system prompt uncached; the same text as an input
    /// message is cached like the rest of the prefix.
    DeveloperMessage,
}

/// How to read token usage off this provider's wire response into canonical usage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsagePolicy {
    #[default]
    OpenAiUsage,
    /// OpenAI-compatible usage where reasoning is reported beside completion tokens.
    OpenAiUsageReasoningSeparate,
    AnthropicUsage,
    GeminiUsageMetadata,
    BedrockUsage,
}
