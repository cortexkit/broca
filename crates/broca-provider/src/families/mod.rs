//! The wire-family implementations — one per distinct API grammar. Each renders a
//! canonical request to its grammar's exact bytes and normalizes its response
//! stream back to canonical events.

pub mod anthropic_decode;
pub mod anthropic_encode;
pub mod anthropic_messages;

pub use anthropic_messages::AnthropicMessages;

pub mod bedrock_converse;

pub use bedrock_converse::BedrockConverse;

pub mod openai_chat;

pub use openai_chat::OpenAiChat;

pub mod openai_responses;
pub mod responses_decode;

pub use openai_responses::OpenAiResponses;

pub mod gemini;

pub use gemini::Gemini;

pub mod google_codeassist;

pub use google_codeassist::GoogleCodeAssist;

pub mod openai_responses_chatgpt;

pub use openai_responses_chatgpt::OpenAiResponsesChatgpt;

use broca_protocol::{
    request::{CallOptions, GenerationConfig, ModelHandle, ToolChoice},
    Usage,
};
use serde_json::{json, Map, Value};

use crate::policy::{
    Quirks, ReasoningPolicy, SystemPromptPolicy, TokenLimitField, ToolIdPolicy, UsagePolicy,
};
use crate::spec::{AuthKind, ModelSpec, ProviderSpec, UnknownCapability, WireFamilyId};
use crate::wire::FrozenRenderConfig;

/// Whether provider-signed metadata produced by the model `origin` is safe to echo back
/// when rendering for the model `target`.
///
/// A signature is minted by ONE specific model and rejected by a different one, so
/// echoing a signed block or function call produced under model A into a request to model B
/// is a hard provider error (a 400). This family-agnostic gate covers Anthropic signed
/// thinking and Gemini function-call thought signatures; other signed metadata can reuse the
/// same rule. For Anthropic thinking this is only the legacy frozen rule: measured live,
/// Anthropic's API accepts another Anthropic model's signed thinking, and current runs use
/// [`anthropic_thinking_is_echo_safe`] instead.
///
/// Semantics:
/// - `target == None`: no run-model context is available (an isolated render or golden
///   test that doesn't thread the current model). The cross-model check is skipped and
///   the caller's positional rule stands unchanged — preserving existing render bytes.
/// - `target == Some(t)`: keep ONLY when `origin == Some(t)` (same producing model). An
///   unknown origin (`None`) or a different model fails CLOSED (strip): echoing a
///   signature we cannot prove the target minted risks the 400, while stripping only
///   loses some cache continuity. This is the strip-on-doubt rule.
pub fn signed_reasoning_is_echo_safe(
    origin: Option<&ModelHandle>,
    target: Option<&ModelHandle>,
) -> bool {
    match target {
        None => true,
        Some(t) => origin == Some(t),
    }
}

/// The catalog provider id of Anthropic's own API: the only provider whose models were measured
/// to accept one another's signed thinking.
pub const ANTHROPIC_PROVIDER_ID: &str = "anthropic";

/// Thinking signed against an earlier conversation prefix cannot be reused after
/// compaction rewrites it. Remove those blocks only from outbound request copies,
/// preserving the transcript and the same boundary on every later render.
pub(crate) fn compaction_view(
    family: WireFamilyId,
    opts: &CallOptions,
) -> std::borrow::Cow<'_, CallOptions> {
    let Some(prefix_len) = opts.render_episodes.compaction_prefix_len else {
        return std::borrow::Cow::Borrowed(opts);
    };
    if !family.thinking_is_prefix_bound() {
        return std::borrow::Cow::Borrowed(opts);
    }
    let mut view = opts.clone();
    for message in &mut view.prompt {
        if message.ordinal.is_some_and(|ordinal| ordinal >= prefix_len) {
            continue;
        }
        message.content.retain(|block| {
            !matches!(
                block,
                broca_protocol::ContentBlock::Reasoning {
                    signature: Some(_),
                    ..
                } | broca_protocol::ContentBlock::RedactedReasoning { .. }
            )
        });
    }
    std::borrow::Cow::Owned(view)
}

/// Whether signed thinking produced by the model `origin` may be echoed to the model `target`
/// under the frozen Anthropic cross-model rule.
///
/// `SameModelOnly` is [`signed_reasoning_is_echo_safe`] unchanged. `AnyAnthropicModel` also keeps
/// thinking whose origin and target are both models of the first-party `anthropic` provider:
/// the API drops what the target model cannot read rather than rejecting it. Any other origin,
/// including an unknown one, is stripped whatever the target is, so the decision for a message
/// never changes between two requests of the same session. With no target model the positional
/// rule stands alone, as it does for the legacy rule.
pub fn anthropic_thinking_is_echo_safe(
    rule: crate::policy::CrossModelThinking,
    origin: Option<&ModelHandle>,
    target: Option<&ModelHandle>,
) -> bool {
    match rule {
        crate::policy::CrossModelThinking::SameModelOnly => {
            signed_reasoning_is_echo_safe(origin, target)
        }
        crate::policy::CrossModelThinking::AnyAnthropicModel => match (origin, target) {
            (_, None) => true,
            (Some(origin), Some(target)) => {
                origin == target
                    || (origin.provider_module_id == ANTHROPIC_PROVIDER_ID
                        && target.provider_module_id == ANTHROPIC_PROVIDER_ID)
            }
            (None, Some(_)) => false,
        },
    }
}

/// Split a provider's inclusive input total into canonical fresh and cached counts.
/// Each usage object is self-contained, so deriving both values from that object keeps
/// repeated cumulative usage reports idempotent instead of subtracting cached tokens twice.
pub(crate) fn merge_inclusive_input_tokens(
    usage: &mut Usage,
    total: Option<u64>,
    cached: Option<u64>,
) {
    if let Some(total) = total {
        // No split means the reported total is the only input measurement available.
        usage.input_tokens = Some(match cached {
            Some(cached) => total.saturating_sub(cached),
            None => total,
        });
        usage.cached_input_tokens = cached;
    } else if let Some(cached) = cached {
        usage.cached_input_tokens = Some(cached);
    }
}

/// Split an inclusive input total that also contains cache-WRITE tokens into canonical
/// fresh, cache-read and cache-write counts, so all three are disjoint as `Usage` requires.
///
/// OpenAI's Responses usage counts both cache reads (`cached_tokens`) and cache writes
/// (`cache_write_tokens`) inside `input_tokens`. Measured live on 2026-09-28 against
/// `/v1/responses` with `gpt-6-luna` and the same ~3.2k-token prompt twice: the first call
/// reported `input_tokens` 3173 with `cache_write_tokens` 3170 and `total_tokens` 3178 (input
/// plus 5 output), and the second reported `input_tokens` 3173 with `cached_tokens` 3170. The
/// prompt-caching guide computes ordinary input the same way: `input_tokens - cached_tokens -
/// cache_write_tokens` (<https://platform.openai.com/docs/guides/prompt-caching>, "Monitor cache
/// performance"). Leaving the write inside fresh input would count those tokens twice.
///
/// A key the object did not carry stays absent rather than becoming a reported zero, and like
/// `merge_inclusive_input_tokens` every value is derived from this one usage object, so
/// merging a repeated cumulative report is idempotent.
pub(crate) fn merge_inclusive_input_tokens_with_writes(
    usage: &mut Usage,
    total: Option<u64>,
    cached: Option<u64>,
    written: Option<u64>,
) {
    if let Some(total) = total {
        let split_out = cached.unwrap_or(0).saturating_add(written.unwrap_or(0));
        usage.input_tokens = Some(total.saturating_sub(split_out));
        usage.cached_input_tokens = cached;
        usage.cache_write_tokens = written;
    } else {
        if let Some(cached) = cached {
            usage.cached_input_tokens = Some(cached);
        }
        if let Some(written) = written {
            usage.cache_write_tokens = Some(written);
        }
    }
}

/// Copy caller generation controls into a request object. `CallOptions.generation` is
/// persisted in the run config and rebuilt on resume before each render, so these
/// body-visible fields stay byte-identical live-vs-resume the same way `tools` and
/// `tool_choice` do.
pub(crate) fn apply_generation_controls(
    body: &mut Map<String, Value>,
    opts: &CallOptions,
    temperature_key: &str,
    top_p_key: &str,
    stop_key: Option<&str>,
) {
    if let Some(temperature) = opts.generation.temperature {
        body.insert(temperature_key.to_string(), json!(temperature));
    }
    if let Some(top_p) = opts.generation.top_p {
        body.insert(top_p_key.to_string(), json!(top_p));
    }
    if let Some(stop_key) = stop_key.filter(|_| !opts.generation.stop_sequences.is_empty()) {
        body.insert(
            stop_key.to_string(),
            json!(opts.generation.stop_sequences.clone()),
        );
    }
}

/// Explain request conflicts introduced by Anthropic OAuth's resolved manual-thinking policy.
/// API-key resolution replaces this policy with `ReasoningPolicy::None`, so the OAuth-only
/// temperature and forced-tool conflicts do not reject API-key requests.
pub fn fresh_admission_config_error(
    frozen: &FrozenRenderConfig,
    generation: &GenerationConfig,
    tool_choice: &ToolChoice,
) -> Option<String> {
    if !matches!(
        frozen.reasoning,
        ReasoningPolicy::AnthropicInterleaved { .. }
    ) {
        return None;
    }

    if let Some(temperature) = generation
        .temperature
        .filter(|temperature| *temperature != 1.0)
    {
        return Some(format!(
            "generation.temperature={temperature} conflicts with \
             reasoning.mode='anthropic_interleaved': the caller set generation.temperature, \
             while the resolved Anthropic OAuth auth method enabled thinking; Anthropic requires \
             generation.temperature=1 when thinking is enabled"
        ));
    }

    let forced_choice = match tool_choice {
        ToolChoice::Required | ToolChoice::Tool { .. } => {
            serde_json::to_string(tool_choice).expect("ToolChoice always serializes")
        }
        ToolChoice::Auto | ToolChoice::None => return None,
    };
    Some(format!(
        "tool_choice={forced_choice} conflicts with \
         reasoning.mode='anthropic_interleaved': the caller set tool_choice, while the resolved \
         Anthropic OAuth auth method enabled thinking; Anthropic rejects this combination: \
         \"Thinking may not be enabled when tool_choice forces tool use.\""
    ))
}

/// The current render ABI version for a wire family. The catalog-resolve path needs
/// this to freeze the right `renderer_version` (the hand-written per-family helpers
/// bake their own constant). An exhaustive match so a new family can't be added
/// without giving it a version here.
pub fn renderer_version_for(family: WireFamilyId) -> u32 {
    match family {
        WireFamilyId::AnthropicMessages => ANTHROPIC_RENDERER_VERSION,
        WireFamilyId::OpenAiChat => OPENAI_CHAT_RENDERER_VERSION,
        WireFamilyId::OpenAiResponses => OPENAI_RESPONSES_RENDERER_VERSION,
        WireFamilyId::Gemini => GEMINI_RENDERER_VERSION,
        WireFamilyId::BedrockConverse => BEDROCK_RENDERER_VERSION,
        WireFamilyId::GoogleCodeAssist => GOOGLE_CODEASSIST_RENDERER_VERSION,
        WireFamilyId::OpenAiResponsesChatgpt => OPENAI_RESPONSES_CHATGPT_RENDERER_VERSION,
    }
}

/// The correct within-family quirk selections for a wire family — the SINGLE source
/// of truth for "how does this family shape a request". Both the catalog ingestion
/// path (`map_provider`) and the hand-written per-family `*_frozen_config` helpers
/// source their quirks from here, so the two paths can never drift: a catalog-resolved
/// provider and a hand-built config for the same family render byte-identical request
/// shapes. Mirrors `default_auth_kind` (a per-family default keyed on the family),
/// just for the render quirks instead of the auth kind.
///
/// A specific provider can still override any of these via an overlay/spec, but a
/// provider with no overrides gets its family's correct shape — not a one-size default
/// that happens to match only OpenAI-chat.
pub fn default_quirks(family: WireFamilyId) -> Quirks {
    match family {
        WireFamilyId::AnthropicMessages => Quirks {
            reasoning: ReasoningPolicy::AnthropicInterleaved {
                budget_tokens: 1024,
                last_turn_only: true,
                echo_scope: crate::policy::ThinkingEchoScope::AllAssistantMessages,
                cross_model: crate::policy::CrossModelThinking::AnyAnthropicModel,
            },
            tool_id: ToolIdPolicy::Passthrough,
            system_prompt: SystemPromptPolicy::RequiredIdentityLead {
                identity: CLAUDE_CODE_IDENTITY.to_string(),
            },
            usage: UsagePolicy::AnthropicUsage,
            token_limit_field: TokenLimitField::MaxTokens,
            reasoning_echo: None,
            tool_call_content_empty_string: false,
        },
        WireFamilyId::OpenAiChat => Quirks {
            reasoning: ReasoningPolicy::None,
            tool_id: ToolIdPolicy::Passthrough,
            system_prompt: SystemPromptPolicy::SystemMessage,
            usage: UsagePolicy::OpenAiUsage,
            token_limit_field: TokenLimitField::MaxTokens,
            reasoning_echo: None,
            tool_call_content_empty_string: false,
        },
        WireFamilyId::OpenAiResponses => Quirks {
            reasoning: ReasoningPolicy::None,
            tool_id: ToolIdPolicy::Passthrough,
            system_prompt: SystemPromptPolicy::InstructionsField,
            usage: UsagePolicy::OpenAiUsage,
            token_limit_field: TokenLimitField::MaxOutputTokens,
            reasoning_echo: None,
            tool_call_content_empty_string: false,
        },
        WireFamilyId::Gemini => Quirks {
            reasoning: ReasoningPolicy::None,
            tool_id: ToolIdPolicy::SyntheticIndex,
            system_prompt: SystemPromptPolicy::SystemInstructionField,
            usage: UsagePolicy::GeminiUsageMetadata,
            token_limit_field: TokenLimitField::GeminiMaxOutputTokens,
            reasoning_echo: None,
            tool_call_content_empty_string: false,
        },
        WireFamilyId::BedrockConverse => Quirks {
            reasoning: ReasoningPolicy::None,
            tool_id: ToolIdPolicy::Passthrough,
            system_prompt: SystemPromptPolicy::TopLevelField,
            usage: UsagePolicy::BedrockUsage,
            token_limit_field: TokenLimitField::MaxTokens,
            reasoning_echo: None,
            tool_call_content_empty_string: false,
        },
        // The inner body is the Gemini generateContent body, so the render quirks are
        // Gemini's; the family differs only in the envelope wrap + endpoint + OAuth auth.
        WireFamilyId::GoogleCodeAssist => default_quirks(WireFamilyId::Gemini),
        // The body is the OpenAI-Responses shape, so the quirks are Responses'; the family
        // differs only in endpoint (the ChatGPT codex backend) + headers.
        WireFamilyId::OpenAiResponsesChatgpt => default_quirks(WireFamilyId::OpenAiResponses),
    }
}

/// Resolve the frozen render config from a provider + model spec (the catalog
/// path). This reads ONLY the bounded, typed config fields — never the unbounded
/// `raw` passthrough — so the frozen result is bounded and complete. It is called
/// ONCE at run start; the result is what every later render (including on resume)
/// uses, so a catalog/spec change afterward cannot move the rendered bytes.
///
/// Unknown capability flags fail closed here, not in the parser: this is the first
/// point that knows whether the run would use the flag. Today only reasoning is
/// consulted (it chooses `ReasoningPolicy`). The other four flags are carried on
/// the model spec so a later gate can admit them the same way.
pub fn resolve_frozen(
    provider: &ProviderSpec,
    model: &ModelSpec,
    max_output_tokens: u32,
    renderer_version: u32,
) -> Result<FrozenRenderConfig, UnknownCapability> {
    // The parser does not know whether this run would use reasoning. The family quirk
    // does: a non-None policy is the shape we would freeze if the model has the flag.
    // Unknown + that need is a refusal at admission, not a silent `ReasoningPolicy::None`.
    let reasoning_needed = provider.quirks.reasoning != ReasoningPolicy::None;
    let reasoning = model.capabilities.reasoning.admit(
        reasoning_needed,
        &provider.id,
        &model.id,
        "reasoning",
    )?;
    Ok(FrozenRenderConfig {
        family_id: provider.wire_family,
        renderer_version,
        wire_model_id: model.id.clone(),
        // The spec's base_url is the catalog-driven override for OpenAI-compatible
        // (and other host-configurable) providers. Empty = the family default.
        base_url: (!provider.base_url.is_empty()).then(|| provider.base_url.clone()),
        max_output_tokens,
        caller_max_output_tokens: None,
        // Provider quirks describe the family's reasoning wire shape, but the selected model
        // remains the authority on whether that shape is legal at all.
        reasoning: if reasoning {
            provider.quirks.reasoning.clone()
        } else {
            ReasoningPolicy::None
        },
        tool_id: provider.quirks.tool_id,
        system_prompt: provider.quirks.system_prompt.clone(),
        usage: provider.quirks.usage,
        cache: None,
        token_limit_field: provider.quirks.token_limit_field,
        static_options: provider.static_options.clone(),
        aws_region: match &provider.auth_kind {
            AuthKind::SigV4 { region } => Some(region.clone()),
            _ => None,
        },
        // For the Antigravity family the cloud project comes from the credential when the
        // run starts, not from the model catalog — so a config built from the catalog leaves
        // this None and the code that starts the run fills it in later.
        project_id: None,
        codeassist_identity: None,
        // The caller cache scope is supplied at admission, so the catalog-built config leaves
        // it None. ChatGPT codex falls back to the run session id there when it is absent.
        prompt_cache_key: None,
        reasoning_effort: None,
        service_tier: None,
        service_tier_request: None,
        speed: None,
        reasoning_content_required: false,
        reasoning_echo: provider.quirks.reasoning_echo,
        tool_call_content_empty_string: provider.quirks.tool_call_content_empty_string,
        variant: None,
        responses_tools_non_strict: false,
        chatgpt_effort_updates: false,
        provider_text_state: text_state_family(provider.wire_family),
        structured_output: None,
        claude_code_persona: None,
    })
}

/// The Anthropic Messages family render ABI version. Bump on ANY change to the
/// rendered request shape — a change means a resume can't reproduce the original
/// request bytes.
/// v2: render `CallOptions.generation` (`temperature`, `top_p`, `stop_sequences`).
pub const ANTHROPIC_RENDERER_VERSION: u32 = 2;

/// The required identity that must lead the Anthropic system content under the
/// Claude-subscription OAuth shape — the provider rejects the request without it.
pub const CLAUDE_CODE_IDENTITY: &str = "You are Claude Code, Anthropic's official CLI for Claude.";

/// The convenience max-output-tokens the hand-built per-family helpers default to. A
/// real run overrides this from the caller's request; this is only the helper default.
const DEFAULT_HELPER_MAX_OUTPUT_TOKENS: u32 = 2048;

/// Build a frozen render config for a family from its `default_quirks` + standard
/// helper defaults (no base_url override, 2048 max output, no static options). This is
/// the shared body of every `*_frozen_config` helper, so they stay byte-identical to
/// the catalog path (which also sources quirks from `default_quirks`). `aws_region` is
/// endpoint/signing metadata only (never rendered into the body) and is `Some` only for
/// Bedrock.
fn frozen_from_family_defaults(
    family: WireFamilyId,
    wire_model_id: impl Into<String>,
    aws_region: Option<String>,
) -> FrozenRenderConfig {
    let q = default_quirks(family);
    FrozenRenderConfig {
        family_id: family,
        renderer_version: renderer_version_for(family),
        wire_model_id: wire_model_id.into(),
        base_url: None,
        max_output_tokens: DEFAULT_HELPER_MAX_OUTPUT_TOKENS,
        caller_max_output_tokens: None,
        reasoning: q.reasoning,
        tool_id: q.tool_id,
        system_prompt: q.system_prompt,
        usage: q.usage,
        cache: None,
        token_limit_field: q.token_limit_field,
        static_options: serde_json::Value::Null,
        aws_region,
        project_id: None,
        codeassist_identity: None,
        prompt_cache_key: None,
        reasoning_effort: None,
        service_tier: None,
        service_tier_request: None,
        speed: None,
        reasoning_content_required: false,
        reasoning_echo: q.reasoning_echo,
        tool_call_content_empty_string: q.tool_call_content_empty_string,
        variant: None,
        responses_tools_non_strict: false,
        chatgpt_effort_updates: false,
        provider_text_state: text_state_family(family),
        structured_output: None,
        claude_code_persona: None,
    }
}

fn text_state_family(family: WireFamilyId) -> bool {
    matches!(
        family,
        WireFamilyId::OpenAiResponses
            | WireFamilyId::OpenAiResponsesChatgpt
            | WireFamilyId::Gemini
            | WireFamilyId::GoogleCodeAssist
    )
}

pub(crate) fn text_state_is_echo_safe(
    message: &broca_protocol::content::Message,
    target: Option<&ModelHandle>,
    format: broca_protocol::content::TextProviderFormat,
    family: WireFamilyId,
) -> bool {
    use broca_protocol::content::TextProviderFormat as F;
    message.origin.is_some()
        && message.origin.as_ref() == target
        && matches!(
            (format, family),
            (F::OpenAiResponses, WireFamilyId::OpenAiResponses)
                | (
                    F::OpenAiResponsesChatgpt,
                    WireFamilyId::OpenAiResponsesChatgpt
                )
                | (F::Gemini, WireFamilyId::Gemini)
                | (F::GoogleCodeAssist, WireFamilyId::GoogleCodeAssist)
        )
}

/// Build the frozen render config for an Anthropic Claude-subscription model. The
/// quirks (identity lead, interleaved thinking with a 1024-token budget, ...) come
/// from `default_quirks` — the same source the catalog uses — so this hand-built
/// config and a catalog-resolved one render byte-identically. 2048 max output is the
/// convenience default.
pub fn anthropic_frozen_config(wire_model_id: impl Into<String>) -> FrozenRenderConfig {
    frozen_from_family_defaults(WireFamilyId::AnthropicMessages, wire_model_id, None)
}

/// Build the frozen render config for the Anthropic api-key SHAPE — same wire family
/// (`/v1/messages`), but the QUIRKS that distinguish a plain api-key request from the
/// Claude-subscription OAuth request: no required identity lead in the body (a plain
/// top-level `system`) and no interleaved-thinking block. These are the only body-affecting
/// differences; auth (`x-api-key` vs `Bearer`) and the dropped `anthropic-beta: oauth-…`
/// header are off the rendered bytes (resolved/attached at the endpoint).
///
/// This is a QUIRKS config of the AnthropicMessages family, NOT a separate wire family — the
/// grammar is identical; only the policy selection differs (the same way OpenAI api-key stays
/// a config of OpenAiChat). The body distinctness from the OAuth shape is the L3 requirement,
/// proven by the distinctness golden in the family's tests.
///
/// COUPLING NOTE: the two anthropic shapes share the SAME family (AnthropicMessages), so the
/// family-level method↔family consistency assert can't catch a shape mismatch — there is no
/// family-level mismatch possible. The guarantee that a resume reproduces the right body is
/// therefore purely that the method-resolved quirks FROZE into the run's frozen_render_config
/// at run start (reproduced from the blob on resume, never re-resolved). Family-changing
/// methods (antigravity/chatgpt) get the family assert; quirks-only methods (anthropic
/// api-key) rely on this frozen-quirks reproduction. Both are C7-correct, different
/// mechanisms.
pub fn anthropic_apikey_frozen_config(wire_model_id: impl Into<String>) -> FrozenRenderConfig {
    let mut frozen = anthropic_frozen_config(wire_model_id);
    apply_anthropic_apikey_quirks(&mut frozen);
    frozen
}

/// Select the plain Anthropic API-key policies on an already-resolved catalog config.
///
/// Catalog resolution owns endpoint/model/static configuration, so it cannot replace that
/// config with [`anthropic_apikey_frozen_config`]. Applying the same policy source in place
/// preserves those resolved fields while freezing the method-specific request shape.
pub fn apply_anthropic_apikey_quirks(frozen: &mut FrozenRenderConfig) {
    // The api-key body differences (the OAuth shape's identity lead + interleaved thinking
    // are both absent):
    frozen.system_prompt = SystemPromptPolicy::TopLevelField;
    frozen.reasoning = ReasoningPolicy::None;
}

/// The OpenAI-Chat family render ABI version. Bump on ANY change to the rendered
/// request shape — a change means a resume can't reproduce the original bytes.
/// v2: render `CallOptions.generation` (`temperature`, `top_p`, `stop`).
/// v3: request streaming usage with `stream_options.include_usage`.
/// v4: normalize model-visible tool names at the render edge.
pub const OPENAI_CHAT_RENDERER_VERSION: u32 = 4;

/// Build the frozen render config for a standard OpenAI `/chat/completions` model:
/// system as a leading message, no thinking blocks, `max_tokens` for the limit. Quirks
/// come from `default_quirks` (the same source the catalog uses).
pub fn openai_chat_frozen_config(wire_model_id: impl Into<String>) -> FrozenRenderConfig {
    frozen_from_family_defaults(WireFamilyId::OpenAiChat, wire_model_id, None)
}

/// The Bedrock ConverseStream family render ABI version. Bump on ANY change to the
/// rendered request shape — a change means a resume can't reproduce the original
/// bytes.
/// v2: normalize model-visible tool names at the render edge.
pub const BEDROCK_RENDERER_VERSION: u32 = 2;

/// Default AWS region used by the convenience Bedrock frozen config. Real provider
/// specs resolve the region from `AuthKind::SigV4`.
pub const DEFAULT_BEDROCK_REGION: &str = "us-east-1";

/// Build the frozen render config for AWS Bedrock ConverseStream. The model id is
/// placed in the endpoint path, not in the rendered JSON body.
pub fn bedrock_frozen_config(wire_model_id: impl Into<String>) -> FrozenRenderConfig {
    bedrock_frozen_config_with_region(wire_model_id, DEFAULT_BEDROCK_REGION)
}

/// Build the frozen render config for a specific AWS Bedrock region. Region is
/// endpoint/signing metadata only; it is never rendered into the request body.
pub fn bedrock_frozen_config_with_region(
    wire_model_id: impl Into<String>,
    region: impl Into<String>,
) -> FrozenRenderConfig {
    frozen_from_family_defaults(
        WireFamilyId::BedrockConverse,
        wire_model_id,
        Some(region.into()),
    )
}

/// The OpenAI-Responses family render ABI version. Bump on ANY change to the rendered
/// request shape — a change means a resume can't reproduce the original bytes.
/// v2: dropped `strict: true` from function tools (the real API rejects strict tools
/// whose schema doesn't set additionalProperties:false + require every property).
/// v3: render `CallOptions.generation` (`temperature`, `top_p`). `/responses` has no stop field.
/// v4: reasoning-enabled requests ask for encrypted reasoning and same-model continuations echo
/// the captured reasoning item byte-verbatim. Old in-flight runs fail the renderer-version
/// compatibility check rather than silently changing continuation bytes.
/// v5: normalize model-visible tool names at the render edge.
/// v6: echo per-item phase from format-tagged text state for newly frozen runs.
pub const OPENAI_RESPONSES_RENDERER_VERSION: u32 = 6;

/// Build the frozen render config for OpenAI `/v1/responses`: system text in
/// `instructions`, `max_output_tokens`, flat function tools. Quirks come from
/// `default_quirks` (the same source the catalog uses).
pub fn openai_responses_frozen_config(wire_model_id: impl Into<String>) -> FrozenRenderConfig {
    frozen_from_family_defaults(WireFamilyId::OpenAiResponses, wire_model_id, None)
}

/// The Gemini family render ABI version. Bump on ANY change to the rendered request shape.
/// v2: render `generationConfig.temperature`, `topP`, and `stopSequences` from
/// `CallOptions.generation`.
/// v3: normalize model-visible tool names at the render edge.
/// v4: echo signed text parts, including signature-only empty text.
pub const GEMINI_RENDERER_VERSION: u32 = 4;

/// Build the frozen render config for Google AI Gemini `generateContent`. Quirks come
/// from `default_quirks` (the same source the catalog uses).
pub fn gemini_frozen_config(wire_model_id: impl Into<String>) -> FrozenRenderConfig {
    frozen_from_family_defaults(WireFamilyId::Gemini, wire_model_id, None)
}

/// Build the frozen render config for a family from its DEFAULTS plus a model id — the
/// single dispatcher from a `WireFamilyId` to its `*_frozen_config` helper. Used both by the
/// catalog's structural-lock test and by the auth-method family OVERRIDE path (when an auth
/// method like Antigravity selects a different wire family than the catalog provider's, the
/// override family's config is built from ITS OWN defaults — its endpoint, auth kind, and
/// quirks — keeping only the model id). An exhaustive match so a new family must register here.
pub fn frozen_config_for_family(
    family: WireFamilyId,
    wire_model_id: impl Into<String>,
) -> FrozenRenderConfig {
    let model = wire_model_id.into();
    match family {
        WireFamilyId::AnthropicMessages => anthropic_frozen_config(model),
        WireFamilyId::OpenAiChat => openai_chat_frozen_config(model),
        WireFamilyId::OpenAiResponses => openai_responses_frozen_config(model),
        WireFamilyId::Gemini => gemini_frozen_config(model),
        WireFamilyId::BedrockConverse => bedrock_frozen_config(model),
        WireFamilyId::GoogleCodeAssist => google_codeassist_frozen_config(model),
        WireFamilyId::OpenAiResponsesChatgpt => openai_responses_chatgpt_frozen_config(model),
    }
}

/// The Google Antigravity / Code-Assist family render ABI version. Bump on ANY change to the
/// rendered request shape (the envelope wrap or the deterministic requestId derivation).
/// v2: the shared inner Gemini request now renders generationConfig sampling controls from
/// `CallOptions.generation`.
/// v3: Antigravity's resolved Gemini 3 tiers select model-specific thinking controls and
/// output-token defaults.
/// v4: normalize model-visible tool names at the render edge.
/// v5: preserve an explicit caller output-token cap, clamped only to the Antigravity tier ceiling.
/// v6: retain signed text parts through the shared Gemini codec. Session identity is an opt-in
/// frozen field within v6: its absence keeps all earlier bytes.
pub const GOOGLE_CODEASSIST_RENDERER_VERSION: u32 = 6;

/// Build the frozen render config for Google Antigravity / Code-Assist. The inner request
/// body follows Gemini's rendering rules; the cloud project id is not known when reading the
/// catalog and is supplied when the run actually starts (from the stored credential), so this
/// builder leaves it `None`.
pub fn google_codeassist_frozen_config(wire_model_id: impl Into<String>) -> FrozenRenderConfig {
    frozen_from_family_defaults(WireFamilyId::GoogleCodeAssist, wire_model_id, None)
}

/// The OpenAI ChatGPT (Codex) family render ABI version. Bump on ANY change to the rendered
/// request shape. The body is the OpenAI-Responses shape PLUS the codex-required fields; the
/// version is independent so a Responses change doesn't silently move this family's resume
/// domain.
/// v2: added the codex-required `store:false` + the per-run `prompt_cache_key` to the body.
/// v3: strip `max_output_tokens` — the codex backend rejects its presence outright.
/// v4: the shared Responses body now renders `CallOptions.generation` temperature/top_p.
/// v5: reasoning-enabled requests ask for encrypted reasoning and same-model continuations echo
/// the captured reasoning item byte-verbatim. Old in-flight runs fail the renderer-version
/// compatibility check rather than silently changing continuation bytes.
/// v6: normalize model-visible tool names at the render edge.
/// v7: omit sampling controls that the ChatGPT codex backend rejects as unsupported.
/// v8: preserve Responses message phase in newly frozen runs.
pub const OPENAI_RESPONSES_CHATGPT_RENDERER_VERSION: u32 = 8;

/// Build the frozen render config for the OpenAI ChatGPT codex backend. The body starts from the
/// OpenAI-Responses shape and applies the codex backend's stricter field set.
pub fn openai_responses_chatgpt_frozen_config(
    wire_model_id: impl Into<String>,
) -> FrozenRenderConfig {
    frozen_from_family_defaults(WireFamilyId::OpenAiResponsesChatgpt, wire_model_id, None)
}

#[cfg(test)]
mod inclusive_usage_tests {
    use super::*;

    #[test]
    fn inclusive_usage_merge_is_idempotent_and_handles_cache_boundaries() {
        let mut usage = Usage::default();
        merge_inclusive_input_tokens(&mut usage, Some(12), Some(3));
        merge_inclusive_input_tokens(&mut usage, Some(12), Some(3));
        assert_eq!(
            (usage.input_tokens, usage.cached_input_tokens),
            (Some(9), Some(3))
        );
        assert_eq!(usage.fill_tokens(), Some(12));

        merge_inclusive_input_tokens(&mut usage, Some(12), None);
        assert_eq!(
            (usage.input_tokens, usage.cached_input_tokens),
            (Some(12), None)
        );

        merge_inclusive_input_tokens(&mut usage, Some(12), Some(12));
        assert_eq!(
            (usage.input_tokens, usage.cached_input_tokens),
            (Some(0), Some(12))
        );
    }

    #[test]
    fn inclusive_usage_merge_with_writes_splits_all_three_and_keeps_absence() {
        let mut usage = Usage::default();
        merge_inclusive_input_tokens_with_writes(&mut usage, Some(12), Some(3), Some(4));
        merge_inclusive_input_tokens_with_writes(&mut usage, Some(12), Some(3), Some(4));
        assert_eq!(
            (
                usage.input_tokens,
                usage.cached_input_tokens,
                usage.cache_write_tokens
            ),
            (Some(5), Some(3), Some(4)),
            "repeating the same report must not subtract twice"
        );

        let mut usage = Usage::default();
        merge_inclusive_input_tokens_with_writes(&mut usage, Some(12), Some(0), None);
        assert_eq!(
            (
                usage.input_tokens,
                usage.cached_input_tokens,
                usage.cache_write_tokens
            ),
            (Some(12), Some(0), None),
            "an unreported write stays absent, not zero"
        );

        let mut usage = Usage::default();
        merge_inclusive_input_tokens_with_writes(&mut usage, None, None, Some(7));
        assert_eq!(
            (usage.input_tokens, usage.cache_write_tokens),
            (None, Some(7))
        );
    }
}
