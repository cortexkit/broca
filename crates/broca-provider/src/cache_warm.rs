//! Warming a session's provider prompt cache, provider side: what a frozen config declares about
//! its cache, whether it can be warmed, and the warm request body.
//!
//! A warm re-sends a request the session already sent. Anthropic uses `max_tokens: 0` with
//! streaming removed; Responses uses `max_output_tokens: 16` with streaming unchanged.
//! A cache marker identifies the point up to which the provider caches the request. Everything
//! that selects the cached prefix (system, tools, messages, cache markers and their lifetimes,
//! thinking, effort, speed) stays byte-identical: a changed byte before the marker makes the warm
//! read a different prefix and pay to write it rather than reuse the existing cache.

use broca_protocol::{
    canonical_json_bytes,
    request::{ResponseFormat, ToolChoice},
};
use serde_json::Value;

use crate::policy::ReasoningPolicy;
use crate::spec::WireFamilyId;
use crate::wire::{CacheClass, CacheTtl, FrozenRenderConfig, MarkerSyntax};

/// Provider id for Anthropic's own API, not gateways using its request grammar. Only Anthropic
/// itself documents the zero-output request used for warming.
const FIRST_PARTY_ANTHROPIC: &str = "anthropic";

/// Why a Bedrock session cannot be warmed: its wire family has no request that generates nothing.
pub const BEDROCK_WARM_UNSUPPORTED: &str = "bedrock converse has no documented zero-output request";

/// The cache lifetimes Anthropic offers, shortest first.
const ANTHROPIC_TTL_OPTIONS_MS: [u64; 2] = [300_000, 3_600_000];

/// What a frozen config says about its prompt cache. Every member but `warm` may be unknown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheDeclaration {
    /// How long the provider keeps a cached prefix after the request that wrote or read it.
    pub ttl_ms: Option<u64>,
    /// Every lifetime the provider offers, shortest first.
    pub ttl_options_ms: Vec<u64>,
    /// Whether reading a cached prefix restarts its lifetime.
    pub read_refreshes_ttl: Option<bool>,
    /// `Ok` when the session can be warmed, else why not.
    pub warm: Result<(), String>,
    /// Where `ttl_ms` comes from: `requested` when the session's cache request named the
    /// lifetime, `provider_default` when the markers carry none and the provider's default
    /// applies.
    pub basis: Option<&'static str>,
}

/// The marker lifetime a frozen cache policy schedules by, in milliseconds. `None` for a policy
/// without markers.
pub fn marker_ttl_ms(frozen: &FrozenRenderConfig) -> Option<u64> {
    match &frozen.cache.as_ref()?.class {
        CacheClass::Markers { ttl, .. } => Some(ttl_ms(*ttl)),
        CacheClass::Hints { .. } | CacheClass::None => None,
    }
}

fn ttl_ms(ttl: CacheTtl) -> u64 {
    match ttl {
        CacheTtl::FiveMinutes => 300_000,
        CacheTtl::OneHour => 3_600_000,
    }
}

/// What a session frozen with `frozen`, served by `provider_id` and sending `tool_choice` and
/// `response_format`, declares about its prompt cache.
pub fn cache_declaration(
    provider_id: &str,
    frozen: &FrozenRenderConfig,
    tool_choice: &ToolChoice,
    response_format: &ResponseFormat,
) -> CacheDeclaration {
    let unsupported = |reason: String| CacheDeclaration {
        ttl_ms: None,
        ttl_options_ms: Vec::new(),
        read_refreshes_ttl: None,
        warm: Err(reason),
        basis: None,
    };
    // These Responses backends render prompt_cache_key even without frozen.cache. The
    // request therefore carries a cache-affinity hint regardless of the optional policy;
    // warming must follow that rendered key rather than require an explicit cache request.
    if frozen.prompt_cache_key.is_some()
        && (frozen.family_id == WireFamilyId::OpenAiResponsesChatgpt
            || (frozen.family_id == WireFamilyId::OpenAiResponses && provider_id == "openai"))
    {
        return CacheDeclaration {
            ttl_ms: Some(1_800_000),
            ttl_options_ms: vec![1_800_000],
            read_refreshes_ttl: None,
            warm: Ok(()),
            basis: None,
        };
    }
    let Some(policy) = &frozen.cache else {
        return unsupported(
            "the session froze no prompt-cache policy, so no cached prefix exists to keep warm"
                .into(),
        );
    };
    match (&policy.class, frozen.family_id) {
        (
            CacheClass::Markers {
                syntax: MarkerSyntax::AnthropicCacheControl,
                ttl,
                omit_ttl,
                ..
            },
            WireFamilyId::AnthropicMessages,
        ) => {
            let first_party = provider_id == FIRST_PARTY_ANTHROPIC;
            CacheDeclaration {
                ttl_ms: Some(ttl_ms(*ttl)),
                ttl_options_ms: if first_party {
                    ANTHROPIC_TTL_OPTIONS_MS.to_vec()
                } else {
                    vec![ttl_ms(*ttl)]
                },
                // Anthropic documents that a cache hit refreshes the entry's lifetime at no
                // extra cost; a gateway speaking its grammar promises nothing.
                read_refreshes_ttl: first_party.then_some(true),
                warm: if first_party {
                    warm_shape(frozen, tool_choice, response_format)
                } else {
                    Err(format!(
                        "provider '{provider_id}' speaks the Anthropic grammar but is not \
                         first-party Anthropic, whose zero-output request warming relies on"
                    ))
                },
                basis: Some(if *omit_ttl {
                    "provider_default"
                } else {
                    "requested"
                }),
            }
        }
        (CacheClass::Markers { ttl, omit_ttl, .. }, _) => CacheDeclaration {
            ttl_ms: Some(ttl_ms(*ttl)),
            ttl_options_ms: vec![ttl_ms(*ttl)],
            read_refreshes_ttl: None,
            warm: Err(match frozen.family_id {
                WireFamilyId::BedrockConverse => BEDROCK_WARM_UNSUPPORTED.to_string(),
                family => format!("wire family {family:?} has no zero-output request"),
            }),
            basis: Some(if *omit_ttl {
                "provider_default"
            } else {
                "requested"
            }),
        },
        (CacheClass::Hints { .. }, WireFamilyId::OpenAiResponsesChatgpt)
        | (CacheClass::Hints { .. }, WireFamilyId::OpenAiResponses)
            if frozen.family_id == WireFamilyId::OpenAiResponsesChatgpt
                || provider_id == "openai" =>
        {
            CacheDeclaration {
                ttl_ms: Some(1_800_000),
                ttl_options_ms: vec![1_800_000],
                // OpenAI does not document whether reading a cached prefix extends its lifetime,
                // so read_refreshes_ttl remains unknown.
                read_refreshes_ttl: None,
                warm: Ok(()),
                basis: None,
            }
        }
        (CacheClass::Hints { .. }, _) => unsupported(format!(
            "provider '{provider_id}' caches by prompt-cache hints, which a request without \
             output cannot refresh"
        )),
        (CacheClass::None, _) => unsupported(
            "the session froze no prompt-cache policy, so no cached prefix exists to keep warm"
                .into(),
        ),
    }
}

/// Whether the request shape a first-party Anthropic session froze can be warmed. A warm sends
/// `max_tokens: 0`. Manual thinking cannot accompany it (its `budget_tokens` must be below
/// `max_tokens`), and a forced tool choice or a structured output format demands output a
/// zero-output request cannot produce. Such a session is not warmed at all: stripping the field
/// to make the warm acceptable would change the cached prefix the warm reads.
fn warm_shape(
    frozen: &FrozenRenderConfig,
    tool_choice: &ToolChoice,
    response_format: &ResponseFormat,
) -> Result<(), String> {
    let thinking = match &frozen.variant {
        Some(crate::variant::FrozenVariant {
            request: crate::variant::VariantRequest::Anthropic { thinking, .. },
            ..
        }) => thinking.clone(),
        _ => match frozen.reasoning {
            ReasoningPolicy::AnthropicInterleaved { budget_tokens, .. } => {
                Some(serde_json::json!({ "type": "enabled", "budget_tokens": budget_tokens }))
            }
            _ => None,
        },
    };
    if thinking.as_ref().is_some_and(is_manual_thinking) {
        return Err(MANUAL_THINKING.into());
    }
    if matches!(tool_choice, ToolChoice::Required | ToolChoice::Tool { .. }) {
        return Err(FORCED_TOOL_CHOICE.into());
    }
    if !matches!(response_format, ResponseFormat::Text) {
        return Err(STRUCTURED_OUTPUT.into());
    }
    Ok(())
}

const MANUAL_THINKING: &str =
    "the session froze manual thinking (a budget_tokens thinking config), which a zero-output \
     request cannot carry";
const FORCED_TOOL_CHOICE: &str =
    "the session froze a forced tool_choice, which a zero-output request cannot carry";
const STRUCTURED_OUTPUT: &str =
    "the session froze a structured output format, which a zero-output request cannot carry";

fn is_manual_thinking(thinking: &Value) -> bool {
    thinking.get("type").and_then(Value::as_str) == Some("enabled")
        || thinking.get("budget_tokens").is_some()
}

/// The warm body for an Anthropic Messages request `base`: the same request with `max_tokens` set
/// to 0 and `stream` removed, nothing else changed. Refused, with nothing stripped, when `base`
/// carries a shape a zero-output request cannot (see [`warm_shape`]).
///
/// The body is re-emitted with the same canonical JSON encoding every family renders with, so
/// each member left alone keeps its exact bytes.
pub fn anthropic_warm_body(base: &[u8]) -> Result<Vec<u8>, String> {
    let mut body: Value = serde_json::from_slice(base)
        .map_err(|error| format!("the request to warm is not JSON: {error}"))?;
    let object = body
        .as_object_mut()
        .ok_or("the request to warm is not a JSON object")?;
    if object.get("thinking").is_some_and(is_manual_thinking) {
        return Err(MANUAL_THINKING.into());
    }
    if object
        .get("tool_choice")
        .and_then(|choice| choice.get("type"))
        .and_then(Value::as_str)
        .is_some_and(|kind| matches!(kind, "any" | "tool"))
    {
        return Err(FORCED_TOOL_CHOICE.into());
    }
    if object.contains_key("output_format")
        || object
            .get("output_config")
            .is_some_and(|config| config.get("format").is_some())
    {
        return Err(STRUCTURED_OUTPUT.into());
    }
    if !object.contains_key("max_tokens") {
        return Err("the request to warm carries no max_tokens".into());
    }
    object.insert("max_tokens".into(), Value::from(0u32));
    object.remove("stream");
    Ok(canonical_json_bytes(&body))
}

/// The warm body for Responses, including the subscription backend: cap output without
/// changing streaming, reasoning effort, or any other prefix-affecting field.
pub fn responses_warm_body(base: &[u8]) -> Result<Vec<u8>, String> {
    let mut body: Value = serde_json::from_slice(base)
        .map_err(|error| format!("the request to warm is not JSON: {error}"))?;
    let object = body
        .as_object_mut()
        .ok_or("the request to warm is not a JSON object")?;
    object.insert("max_output_tokens".into(), Value::from(16u32));
    Ok(canonical_json_bytes(&body))
}

/// Read only usage from the bounded Responses answer; generated content is discarded.
/// Both streaming terminal envelopes and non-streaming responses carry the same usage shape.
pub(crate) fn responses_response_usage(body: &[u8]) -> broca_protocol::Usage {
    let response = serde_json::from_slice::<Value>(body).ok().or_else(|| {
        let text = std::str::from_utf8(body).ok()?;
        crate::sse::parse_data_events(text)
            .ok()?
            .into_iter()
            .rev()
            .find_map(|event| {
                let event: Value = serde_json::from_str(&event).ok()?;
                match event.get("type").and_then(Value::as_str) {
                    Some("response.completed" | "response.incomplete") => {
                        event.get("response").cloned()
                    }
                    _ => None,
                }
            })
    });
    let u = response
        .and_then(|response| response.get("usage").cloned())
        .unwrap_or(Value::Null);
    let count = |key: &str| u.get(key).and_then(Value::as_u64);
    let detail = |key: &str| {
        u.get("input_tokens_details")
            .and_then(|d| d.get(key))
            .and_then(Value::as_u64)
    };
    let mut usage = broca_protocol::Usage::default();
    crate::families::merge_inclusive_input_tokens_with_writes(
        &mut usage,
        count("input_tokens"),
        detail("cached_tokens"),
        detail("cache_write_tokens"),
    );
    usage.output_tokens = count("output_tokens");
    usage.reasoning_tokens = u
        .get("output_tokens_details")
        .and_then(|d| d.get("reasoning_tokens"))
        .and_then(Value::as_u64);
    usage
}

/// The response headers a warm keeps: Anthropic's unified rate-limit report.
pub(crate) const RATELIMIT_HEADER_PREFIX: &str =
    cortexkit_anthropic_policy::RATELIMIT_UNIFIED_HEADER_PREFIX;

/// The usage a non-streaming Anthropic Messages response reports, each count absent when the
/// response carries none.
pub(crate) fn anthropic_response_usage(body: &[u8]) -> broca_protocol::Usage {
    let usage = serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|body| body.get("usage").cloned())
        .unwrap_or(Value::Null);
    let count = |key: &str| usage.get(key).and_then(Value::as_u64);
    broca_protocol::Usage {
        input_tokens: count("input_tokens"),
        cached_input_tokens: count("cache_read_input_tokens"),
        cache_write_tokens: count("cache_creation_input_tokens"),
        output_tokens: count("output_tokens"),
        ..broca_protocol::Usage::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::CachePolicy;

    fn anthropic_with(cache: Option<CacheClass>) -> FrozenRenderConfig {
        let mut frozen = crate::families::anthropic_frozen_config("claude-opus-4-8");
        frozen.cache = cache.map(|class| CachePolicy { version: 1, class });
        frozen
    }

    fn markers(ttl: CacheTtl, omit_ttl: bool) -> CacheClass {
        CacheClass::Markers {
            syntax: MarkerSyntax::AnthropicCacheControl,
            ttl,
            max_breakpoints: 4,
            lookback_blocks: 20,
            tail: None,
            omit_ttl,
        }
    }

    #[test]
    fn first_party_anthropic_declares_its_lifetimes_and_warms() {
        let mut frozen = anthropic_with(Some(markers(CacheTtl::OneHour, false)));
        frozen.reasoning = ReasoningPolicy::None;
        let declared = cache_declaration(
            "anthropic",
            &frozen,
            &ToolChoice::Auto,
            &ResponseFormat::Text,
        );
        assert_eq!(
            declared,
            CacheDeclaration {
                ttl_ms: Some(3_600_000),
                ttl_options_ms: vec![300_000, 3_600_000],
                read_refreshes_ttl: Some(true),
                warm: Ok(()),
                basis: Some("requested"),
            }
        );
    }

    #[test]
    fn a_gateway_speaking_anthropic_does_not_warm() {
        let frozen = anthropic_with(Some(markers(CacheTtl::FiveMinutes, false)));
        assert!(
            cache_declaration("minimax", &frozen, &ToolChoice::Auto, &ResponseFormat::Text)
                .warm
                .is_err()
        );
    }

    #[test]
    fn bedrock_is_declared_unsupported_with_its_reason() {
        let mut frozen = crate::families::frozen_config_for_family(
            WireFamilyId::BedrockConverse,
            "anthropic.claude-opus-4-8",
        );
        frozen.cache = Some(CachePolicy {
            version: 1,
            class: CacheClass::Markers {
                syntax: MarkerSyntax::BedrockCachePoint,
                ttl: CacheTtl::FiveMinutes,
                max_breakpoints: 4,
                lookback_blocks: 20,
                tail: None,
                omit_ttl: false,
            },
        });
        assert_eq!(
            cache_declaration(
                "amazon-bedrock",
                &frozen,
                &ToolChoice::Auto,
                &ResponseFormat::Text
            )
            .warm,
            Err(BEDROCK_WARM_UNSUPPORTED.to_string())
        );
    }

    #[test]
    fn manual_thinking_forced_tools_and_structured_output_are_unsupported() {
        let mut manual = anthropic_with(Some(markers(CacheTtl::FiveMinutes, true)));
        manual.reasoning = ReasoningPolicy::None;
        manual.variant = Some(crate::variant::FrozenVariant {
            name: "high".into(),
            request: crate::variant::VariantRequest::Anthropic {
                thinking: Some(serde_json::json!({"type": "enabled", "budget_tokens": 16000})),
                effort: None,
            },
        });
        let declared = cache_declaration(
            "anthropic",
            &manual,
            &ToolChoice::Auto,
            &ResponseFormat::Text,
        );
        assert_eq!(declared.warm, Err(MANUAL_THINKING.to_string()));
        assert_eq!(declared.basis, Some("provider_default"));

        let mut plain = anthropic_with(Some(markers(CacheTtl::FiveMinutes, true)));
        plain.reasoning = ReasoningPolicy::None;
        assert_eq!(
            cache_declaration(
                "anthropic",
                &plain,
                &ToolChoice::Required,
                &ResponseFormat::Text
            )
            .warm,
            Err(FORCED_TOOL_CHOICE.to_string())
        );
        assert_eq!(
            cache_declaration(
                "anthropic",
                &plain,
                &ToolChoice::Auto,
                &ResponseFormat::Json {
                    name: "x".into(),
                    schema: serde_json::json!({})
                }
            )
            .warm,
            Err(STRUCTURED_OUTPUT.to_string())
        );
    }

    #[test]
    fn no_policy_and_hints_are_unsupported() {
        let none = anthropic_with(None);
        assert!(
            cache_declaration("anthropic", &none, &ToolChoice::Auto, &ResponseFormat::Text)
                .warm
                .is_err()
        );
        let hints = anthropic_with(Some(CacheClass::Hints {
            cache_key: "k".into(),
            retention: Some("24h".into()),
        }));
        let declared =
            cache_declaration("openai", &hints, &ToolChoice::Auto, &ResponseFormat::Text);
        assert!(declared.warm.is_err());
        assert_eq!(declared.ttl_ms, None);
    }

    #[test]
    fn the_warm_body_refuses_shapes_it_cannot_carry_and_strips_nothing() {
        for (base, reason) in [
            (
                r#"{"max_tokens":100,"stream":true,"thinking":{"budget_tokens":10,"type":"enabled"}}"#,
                MANUAL_THINKING,
            ),
            (
                r#"{"max_tokens":100,"stream":true,"tool_choice":{"type":"any"}}"#,
                FORCED_TOOL_CHOICE,
            ),
            (
                r#"{"max_tokens":100,"stream":true,"tool_choice":{"name":"x","type":"tool"}}"#,
                FORCED_TOOL_CHOICE,
            ),
            (
                r#"{"max_tokens":100,"output_format":{"type":"json_schema"},"stream":true}"#,
                STRUCTURED_OUTPUT,
            ),
        ] {
            assert_eq!(
                anthropic_warm_body(base.as_bytes()),
                Err(reason.to_string()),
                "{base}"
            );
        }
        assert_eq!(
            anthropic_warm_body(
                br#"{"max_tokens":100,"stream":true,"thinking":{"type":"adaptive"},"tool_choice":{"type":"auto"}}"#
            )
            .unwrap(),
            br#"{"max_tokens":0,"thinking":{"type":"adaptive"},"tool_choice":{"type":"auto"}}"#
        );
    }

    #[test]
    fn responses_declarations_distinguish_first_party_and_gateways() {
        for (provider, family, warmable) in [
            ("openai", WireFamilyId::OpenAiResponses, true),
            ("openai", WireFamilyId::OpenAiResponsesChatgpt, true),
            ("gateway", WireFamilyId::OpenAiResponses, false),
            ("google", WireFamilyId::Gemini, false),
            ("google", WireFamilyId::GoogleCodeAssist, false),
        ] {
            let mut frozen = crate::families::frozen_config_for_family(family, "model");
            frozen.cache = Some(CachePolicy {
                version: 1,
                class: CacheClass::Hints {
                    cache_key: "k".into(),
                    retention: None,
                },
            });
            let declared =
                cache_declaration(provider, &frozen, &ToolChoice::Auto, &ResponseFormat::Text);
            assert_eq!(declared.warm.is_ok(), warmable, "{family:?}");
            assert_eq!(declared.ttl_ms, warmable.then_some(1_800_000));
            assert_eq!(declared.read_refreshes_ttl, None);
            assert_eq!(declared.basis, None);
            if !warmable {
                assert!(declared
                    .warm
                    .unwrap_err()
                    .contains("caches by prompt-cache hints"));
            }
        }
    }

    #[test]
    fn responses_key_without_policy_declares_hints_only_for_supported_backends() {
        for (family, provider, supported) in [
            (WireFamilyId::OpenAiResponsesChatgpt, "openai", true),
            (WireFamilyId::OpenAiResponsesChatgpt, "codex", true),
            (WireFamilyId::OpenAiResponses, "openai", true),
            (WireFamilyId::OpenAiResponses, "gateway", false),
            (WireFamilyId::OpenAiChat, "openai", false),
            (WireFamilyId::Gemini, "google", false),
        ] {
            let mut frozen = crate::families::frozen_config_for_family(family, "model");
            frozen.prompt_cache_key = Some("scope".into());
            assert!(frozen.cache.is_none());
            let declared =
                cache_declaration(provider, &frozen, &ToolChoice::Auto, &ResponseFormat::Text);
            assert_eq!(declared.warm.is_ok(), supported, "{family:?}/{provider}");
            if supported {
                assert_eq!(declared.ttl_ms, Some(1_800_000));
                assert_eq!(declared.ttl_options_ms, vec![1_800_000]);
                assert_eq!(declared.read_refreshes_ttl, None);
            }
            frozen.prompt_cache_key = None;
            assert!(
                cache_declaration(provider, &frozen, &ToolChoice::Auto, &ResponseFormat::Text)
                    .warm
                    .is_err()
            );
        }
    }

    #[test]
    fn responses_warm_changes_only_the_output_cap() {
        let base = br#"{"input":[],"model":"gpt","reasoning":{"effort":"high"},"store":false,"stream":true}"#;
        assert_eq!(responses_warm_body(base).unwrap(), br#"{"input":[],"max_output_tokens":16,"model":"gpt","reasoning":{"effort":"high"},"store":false,"stream":true}"#);
    }

    #[test]
    fn response_usage_keeps_unreported_counts_absent() {
        assert_eq!(
            anthropic_response_usage(
                br#"{"usage":{"input_tokens":3,"cache_read_input_tokens":4000}}"#
            ),
            broca_protocol::Usage {
                input_tokens: Some(3),
                cached_input_tokens: Some(4000),
                ..broca_protocol::Usage::default()
            }
        );
        assert_eq!(
            anthropic_response_usage(b"not json"),
            broca_protocol::Usage::default()
        );
    }
}
