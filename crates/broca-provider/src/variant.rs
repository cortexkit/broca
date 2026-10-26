// Adapted from OpenCode's packages/core/src/models-dev.ts,
// revision 5716f8ba60e79ec60ec485b6e5291c0b0bc1f252 (MIT). Copyright (c) 2025 opencode.
// See THIRD-PARTY-NOTICES.md for the license notice.
// broca maps settings by wire family instead of npm package and freezes them at admission.

//! Reasoning variants: a caller's `model.variant` name, resolved against the model's catalog
//! reasoning options into the request parameters its wire family sends.
//!
//! The names a model accepts are DERIVED from its models.dev `reasoning_options` exactly the way
//! OpenCode derives its per-model variants, to keep caller variant names compatible with
//! OpenCode. The reference is OpenCode's `packages/core/src/models-dev.ts` at commit
//! 5716f8ba60e79ec60ec485b6e5291c0b0bc1f252: `reasoningVariants` (lines 190-219),
//! `settingsForEffort` (221-284),
//! `budgetVariants` (286-301), `toggleVariants` (303-430) and `settingsForBudget` (432-456).
//! OpenCode keys its request settings on the provider's npm package; broca keys them on the
//! wire family, which is what decides the request grammar here. Each mapping below names the
//! OpenCode branch it follows.
//!
//! Nothing here is a per-model table: a model's variants come only from its own catalog
//! options, and the wire parameters only from its family.
//!
//! The resolved parameters are frozen into [`FrozenRenderConfig::variant`] when a run is
//! admitted, so a resume renders from the frozen value and a later catalog change cannot move
//! an admitted run's request bytes.

#[cfg(feature = "live")]
use std::collections::HashSet;
#[cfg(feature = "live")]
use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::policy::ReasoningPolicy;
use crate::spec::{ReasoningOptions, WireFamilyId};
use crate::wire::FrozenRenderConfig;

/// OpenCode's `OUTPUT_TOKEN_MAX` (models-dev.ts line 188). Budget variants stay strictly below
/// it, as OpenCode's `budgetVariants` does.
const OUTPUT_TOKEN_MAX: i64 = 32_000;

/// The reasoning variant a run was admitted with: its name and the wire parameters it resolved
/// to. Frozen in the render config; renderers read only `request`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FrozenVariant {
    pub name: String,
    pub request: VariantRequest,
}

/// The request parameters one variant adds, per wire grammar.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum VariantRequest {
    /// Anthropic Messages: the request's `thinking` object (omitted entirely when `None`) and,
    /// when set, `output_config.effort`.
    Anthropic {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thinking: Option<Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        effort: Option<String>,
    },
    /// OpenAI Responses, platform API and ChatGPT codex backend: `reasoning: {effort, summary:
    /// "auto"}` plus `include: ["reasoning.encrypted_content"]`. Applied through the
    /// `OpenAiResponsesReasoning` policy, which the Responses renderer already emits.
    OpenAiResponses { effort: String },
    /// OpenAI Chat Completions and compatible providers: top-level `reasoning_effort`.
    OpenAiChat { effort: String },
    /// Gemini `generateContent` (also Code Assist's inner request):
    /// `generationConfig.thinkingConfig`.
    Gemini { thinking_config: Value },
}

/// A model's variants in OpenCode's order, plus the option entry types that contributed
/// nothing because this build does not know them.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct DerivedVariants {
    pub variants: Vec<(String, VariantRequest)>,
    pub unknown_entry_types: Vec<String>,
}

impl DerivedVariants {
    pub fn names(&self) -> Vec<&str> {
        self.variants
            .iter()
            .map(|(name, _)| name.as_str())
            .collect()
    }
}

/// Derive a model's variants from its reasoning options, following OpenCode's
/// `reasoningVariants` (models-dev.ts 190-219):
///
/// 1. An `effort` entry wins. Its string values become variants in order, preceded by a `none`
///    from the toggle when the model also publishes a `toggle` entry.
/// 2. Otherwise a `budget_tokens` entry gives `high` and `max` budgets (plus the toggle's
///    `none`).
/// 3. Otherwise a lone `toggle` gives `none` and `thinking`.
///
/// `output_limit` is the model's catalog output limit, which bounds the budget variants.
pub fn derive_variants(
    family: WireFamilyId,
    model_id: &str,
    options: &ReasoningOptions,
    output_limit: Option<u64>,
) -> DerivedVariants {
    let ReasoningOptions::Entries(entries) = options else {
        return DerivedVariants::default();
    };
    let entry_type = |entry: &Value| entry.get("type").and_then(Value::as_str).map(str::to_owned);
    let unknown_entry_types = entries
        .iter()
        .filter_map(|entry| match entry_type(entry).as_deref() {
            Some("effort" | "toggle" | "budget_tokens") => None,
            Some(other) => Some(other.to_string()),
            None => Some(format!("<entry without a type: {entry}>")),
        })
        .collect();
    let has_type = |wanted: &str| {
        entries
            .iter()
            .find(|entry| entry_type(entry).as_deref() == Some(wanted))
    };
    let toggle = has_type("toggle").is_some();
    let toggle_off = || -> Vec<(String, VariantRequest)> {
        if toggle {
            toggle_variants(family)
                .into_iter()
                .filter(|(name, _)| name == "none")
                .collect()
        } else {
            Vec::new()
        }
    };

    let variants = if let Some(effort) = has_type("effort") {
        let off = toggle_off();
        let mut variants = off.clone();
        for value in effort
            .get("values")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            // Only a string names a variant, and not the string "null" (models-dev.ts 201-203).
            // A JSON null inside `values` (sarvam publishes [null, "low", "medium", "high"])
            // NEVER becomes a variant: it is not a setting anyone can request.
            let Some(name) = value.as_str().filter(|name| *name != "null") else {
                continue;
            };
            // The toggle already supplied `none` (models-dev.ts 204).
            if name == "none" && !off.is_empty() {
                continue;
            }
            if let Some(request) = settings_for_effort(family, model_id, name) {
                variants.push((name.to_string(), request));
            }
        }
        dedupe_like_a_js_map(variants)
    } else if let Some(budget) = has_type("budget_tokens") {
        let mut variants = toggle_off();
        variants.extend(budget_variants(family, budget, output_limit));
        variants
    } else if toggle {
        toggle_variants(family)
    } else {
        Vec::new()
    };
    DerivedVariants {
        variants,
        unknown_entry_types,
    }
}

/// OpenCode dedupes with `new Map(variants.map(v => [v.id, v]))` (models-dev.ts 209): a
/// repeated name keeps its FIRST position and takes its LAST settings.
fn dedupe_like_a_js_map(variants: Vec<(String, VariantRequest)>) -> Vec<(String, VariantRequest)> {
    let mut out: Vec<(String, VariantRequest)> = Vec::with_capacity(variants.len());
    for (name, request) in variants {
        match out.iter_mut().find(|(existing, _)| *existing == name) {
            Some(slot) => slot.1 = request,
            None => out.push((name, request)),
        }
    }
    out
}

/// OpenCode `settingsForEffort` (models-dev.ts 221-284), by wire family.
fn settings_for_effort(
    family: WireFamilyId,
    model_id: &str,
    effort: &str,
) -> Option<VariantRequest> {
    match family {
        // `@ai-sdk/anthropic` branch (models-dev.ts 223-229): models before 4.6 take only the
        // effort; later ones also take adaptive thinking. OpenCode's own Anthropic protocol
        // (packages/ai/src/protocols/anthropic-messages.ts, resolveOptions/resolveThinking)
        // sends these as `output_config: {effort}` and `thinking: {type: "adaptive", display:
        // "summarized"}`.
        WireFamilyId::AnthropicMessages => Some(VariantRequest::Anthropic {
            thinking: (!anthropic_manual_thinking(model_id)).then(adaptive_thinking),
            effort: Some(effort.to_string()),
        }),
        // `@ai-sdk/openai` branch (models-dev.ts 252-253): `{reasoningEffort, reasoningSummary:
        // "auto", include: ["reasoning.encrypted_content"]}`, which the Responses protocol sends
        // as `reasoning: {effort, summary}` and `include`.
        WireFamilyId::OpenAiResponses | WireFamilyId::OpenAiResponsesChatgpt => {
            Some(VariantRequest::OpenAiResponses {
                effort: effort.to_string(),
            })
        }
        // `@ai-sdk/openai-compatible` / `@ai-sdk/xai` / ... branch (models-dev.ts 270-283):
        // `{reasoningEffort}`, sent by OpenCode's chat protocol as `reasoning_effort`
        // (packages/ai/src/protocols/openai-chat.ts line 722).
        WireFamilyId::OpenAiChat => Some(VariantRequest::OpenAiChat {
            effort: effort.to_string(),
        }),
        // `@ai-sdk/google` branch (models-dev.ts 230-231).
        WireFamilyId::Gemini | WireFamilyId::GoogleCodeAssist => Some(VariantRequest::Gemini {
            thinking_config: json!({ "includeThoughts": true, "thinkingLevel": effort }),
        }),
        // OpenCode's Bedrock settings are AI SDK options with no request mapping in broca's
        // Converse renderer yet, so Bedrock models define no variants here.
        WireFamilyId::BedrockConverse => None,
    }
}

/// OpenCode `toggleVariants` (models-dev.ts 303-430), by wire family. Families OpenCode gives
/// no toggle branch (OpenAI, OpenAI-compatible) get none.
fn toggle_variants(family: WireFamilyId) -> Vec<(String, VariantRequest)> {
    match family {
        // `@ai-sdk/anthropic` branch (models-dev.ts 323-332).
        WireFamilyId::AnthropicMessages => vec![
            (
                "none".into(),
                VariantRequest::Anthropic {
                    thinking: Some(json!({ "type": "disabled" })),
                    effort: None,
                },
            ),
            (
                "thinking".into(),
                VariantRequest::Anthropic {
                    thinking: Some(adaptive_thinking()),
                    effort: None,
                },
            ),
        ],
        // `@ai-sdk/google` branch (models-dev.ts 333-343).
        WireFamilyId::Gemini | WireFamilyId::GoogleCodeAssist => vec![
            (
                "none".into(),
                VariantRequest::Gemini {
                    thinking_config: json!({ "includeThoughts": false, "thinkingBudget": 0 }),
                },
            ),
            (
                "thinking".into(),
                VariantRequest::Gemini {
                    thinking_config: json!({ "includeThoughts": true, "thinkingBudget": -1 }),
                },
            ),
        ],
        WireFamilyId::OpenAiChat
        | WireFamilyId::OpenAiResponses
        | WireFamilyId::OpenAiResponsesChatgpt
        | WireFamilyId::BedrockConverse => Vec::new(),
    }
}

/// OpenCode `budgetVariants` (models-dev.ts 286-301): `high` is half the maximum (at least the
/// published minimum) and `max` is the maximum, where the maximum is the smallest of the
/// published max, the model's output limit minus one, and `OUTPUT_TOKEN_MAX - 1`.
///
/// OpenCode always has an output limit; a catalog row here may not, and then only the other
/// two bounds apply.
fn budget_variants(
    family: WireFamilyId,
    entry: &Value,
    output_limit: Option<u64>,
) -> Vec<(String, VariantRequest)> {
    let published_max = entry.get("max").and_then(Value::as_i64);
    let published_min = entry.get("min").and_then(Value::as_i64).unwrap_or(0);
    let mut maximum = published_max
        .unwrap_or(OUTPUT_TOKEN_MAX - 1)
        .min(OUTPUT_TOKEN_MAX - 1);
    if let Some(limit) = output_limit.and_then(|limit| i64::try_from(limit).ok()) {
        maximum = maximum.min(limit - 1);
    }
    if maximum <= 0 {
        return Vec::new();
    }
    let high = published_min.max((maximum + 1) / 2).min(maximum);
    [("high", high), ("max", maximum)]
        .into_iter()
        .filter_map(|(name, budget)| {
            settings_for_budget(family, budget).map(|request| (name.to_string(), request))
        })
        .collect()
}

/// OpenCode `settingsForBudget` (models-dev.ts 432-456), by wire family.
fn settings_for_budget(family: WireFamilyId, budget: i64) -> Option<VariantRequest> {
    match family {
        // `@ai-sdk/anthropic` branch (models-dev.ts 434-435), sent as `thinking: {type:
        // "enabled", budget_tokens}` (anthropic-messages.ts resolveThinking).
        WireFamilyId::AnthropicMessages => Some(VariantRequest::Anthropic {
            thinking: Some(json!({ "type": "enabled", "budget_tokens": budget })),
            effort: None,
        }),
        // `@ai-sdk/google` branch (models-dev.ts 436-437).
        WireFamilyId::Gemini | WireFamilyId::GoogleCodeAssist => Some(VariantRequest::Gemini {
            thinking_config: json!({ "includeThoughts": true, "thinkingBudget": budget }),
        }),
        WireFamilyId::OpenAiChat
        | WireFamilyId::OpenAiResponses
        | WireFamilyId::OpenAiResponsesChatgpt
        | WireFamilyId::BedrockConverse => None,
    }
}

fn adaptive_thinking() -> Value {
    json!({ "type": "adaptive", "display": "summarized" })
}

/// OpenCode `anthropicManualThinking` (models-dev.ts 468-476): Claude models before 4.6 take
/// an effort but not adaptive thinking. The version is read from either spelling,
/// `claude-opus-4-5` (family first) or `claude-3-5-sonnet` (version first); a minor above 9
/// is a date stamp (`claude-opus-4-20250514`) and counts as 0. No version at all is not manual.
pub fn anthropic_manual_thinking(model_id: &str) -> bool {
    let id = model_id.to_ascii_lowercase();
    let version = family_first_version(&id).or_else(|| version_first_version(&id));
    let Some((major, minor)) = version else {
        return false;
    };
    let minor = if minor > 9 { 0 } else { minor };
    major < 4 || (major == 4 && minor < 6)
}

/// Leading ASCII digits of `s` as a number, and the rest. `None` without a digit.
fn leading_number(s: &str) -> Option<(u64, &str)> {
    let end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    if end == 0 {
        return None;
    }
    // A digit run too long for u64 saturates; it can only be a date stamp, which counts as 0.
    Some((s[..end].parse().unwrap_or(u64::MAX), &s[end..]))
}

/// `[.-]<digits>` at the start of `s`: the minor version and the rest.
fn separated_number(s: &str) -> Option<(u64, &str)> {
    let rest = s.strip_prefix('.').or_else(|| s.strip_prefix('-'))?;
    leading_number(rest)
}

/// `(opus|sonnet|haiku)-(\d+)([.-](\d+))?`, leftmost match.
fn family_first_version(id: &str) -> Option<(u64, u64)> {
    (0..id.len()).find_map(|start| {
        let rest = &id[start..];
        let after = ["opus-", "sonnet-", "haiku-"]
            .iter()
            .find_map(|family| rest.strip_prefix(family))?;
        let (major, rest) = leading_number(after)?;
        let minor = separated_number(rest).map_or(0, |(minor, _)| minor);
        Some((major, minor))
    })
}

/// `claude-(\d+)([.-](\d+))?-(opus|sonnet|haiku)`, leftmost match.
fn version_first_version(id: &str) -> Option<(u64, u64)> {
    let ends_in_family = |rest: &str| {
        ["-opus", "-sonnet", "-haiku"]
            .iter()
            .any(|family| rest.starts_with(family))
    };
    id.match_indices("claude-").find_map(|(start, _)| {
        let (major, rest) = leading_number(&id[start + "claude-".len()..])?;
        if let Some((minor, after_minor)) = separated_number(rest) {
            if ends_in_family(after_minor) {
                return Some((major, minor));
            }
        }
        ends_in_family(rest).then_some((major, 0))
    })
}

// Admission refusals belong to the live catalog resolver; the pure derivation and
// rendering paths stay independent of broca-core.
#[cfg(feature = "live")]
pub use broca_core::run::VariantRefusal;

/// Resolve a requested variant for a model, or refuse it. A name the model's derived list does
/// not contain is refused; nothing is ever downgraded to a nearby name or dropped.
#[cfg(feature = "live")]
pub fn resolve_variant(
    family: WireFamilyId,
    catalog_model: &str,
    model_id: &str,
    options: &ReasoningOptions,
    output_limit: Option<u64>,
    requested: &str,
) -> Result<FrozenVariant, VariantRefusal> {
    let derived = derive_variants(family, model_id, options, output_limit);
    for entry_type in &derived.unknown_entry_types {
        warn_unknown_entry_type_once(catalog_model, entry_type);
    }
    if let Some((name, request)) = derived.variants.iter().find(|(name, _)| name == requested) {
        return Ok(FrozenVariant {
            name: name.clone(),
            request: request.clone(),
        });
    }
    let why = match options {
        ReasoningOptions::Unstated => {
            "it defines no variants: the catalog publishes no reasoning options for it".to_string()
        }
        ReasoningOptions::NoOptions => {
            "it defines no variants: the catalog states it takes no reasoning options".to_string()
        }
        ReasoningOptions::Entries(_) if derived.variants.is_empty() => format!(
            "it defines no variants the {} wire family can send",
            family.wire_name()
        ),
        ReasoningOptions::Entries(_) => {
            format!("its variants are [{}]", derived.names().join(", "))
        }
    };
    Err(VariantRefusal {
        model: catalog_model.into(),
        variant: requested.into(),
        defined: derived.names().into_iter().map(str::to_owned).collect(),
        reason: format!("model '{catalog_model}' does not define variant '{requested}'; {why}"),
    })
}

/// An option entry type this build does not know contributes no variants. Say so once per model
/// and type per process, so a new upstream option shape is visible without flooding the log.
#[cfg(feature = "live")]
fn warn_unknown_entry_type_once(catalog_model: &str, entry_type: &str) {
    static SEEN: OnceLock<Mutex<HashSet<(String, String)>>> = OnceLock::new();
    let first = SEEN
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .insert((catalog_model.to_string(), entry_type.to_string()));
    if first {
        tracing::warn!(
            target: "catalog",
            "model '{catalog_model}' publishes a reasoning option of unknown type \
             '{entry_type}'; it contributes no variants (reported once per model and type)"
        );
    }
}

/// Freeze a resolved variant into a render config, adjusting the reasoning policy the family
/// renderer reads so request fields and history echo agree with the variant:
///
/// - Anthropic: thinking that is on (adaptive or a budget) takes the interleaved policy, which
///   also echoes the last turn's signed thinking as Anthropic requires; thinking that is off or
///   absent takes no policy, so no thinking is requested or echoed.
/// - OpenAI Responses: the effort becomes the `OpenAiResponsesReasoning` policy with summary
///   `auto`, which renders `reasoning` and `include`.
///
/// - Gemini and Code Assist: nothing to adjust. On Code Assist the caller has already set the
///   wire model id to the backend tier the variant names, and that tier's thinking settings
///   replace the variant's own `thinkingConfig` when the request is rendered.
///
/// Refuses what the provider would reject: an Anthropic budget at or above the output cap
/// (Anthropic requires `max_tokens` above `budget_tokens`).
pub fn freeze_variant(
    frozen: &mut FrozenRenderConfig,
    variant: FrozenVariant,
) -> Result<(), String> {
    match &variant.request {
        VariantRequest::Anthropic { thinking, .. } => {
            let kind = thinking
                .as_ref()
                .and_then(|thinking| thinking.get("type"))
                .and_then(Value::as_str);
            let budget = thinking
                .as_ref()
                .and_then(|thinking| thinking.get("budget_tokens"))
                .and_then(Value::as_u64);
            if let Some(budget) = budget {
                if u64::from(frozen.max_output_tokens) <= budget {
                    return Err(format!(
                        "variant '{}' asks for a thinking budget of {budget} tokens, but the \
                         request's output cap is {}; Anthropic requires the output cap to exceed \
                         the thinking budget, so raise generation.max_output_tokens",
                        variant.name, frozen.max_output_tokens
                    ));
                }
            }
            frozen.reasoning = match kind {
                Some("enabled" | "adaptive") => ReasoningPolicy::AnthropicInterleaved {
                    budget_tokens: budget
                        .and_then(|budget| u32::try_from(budget).ok())
                        .unwrap_or(match frozen.reasoning {
                            ReasoningPolicy::AnthropicInterleaved { budget_tokens, .. } => {
                                budget_tokens
                            }
                            _ => ANTHROPIC_DEFAULT_BUDGET,
                        }),
                    last_turn_only: true,
                    // A variant changes how thinking is requested, not which messages echo it,
                    // so the scope already frozen for this run carries over.
                    echo_scope: match frozen.reasoning {
                        ReasoningPolicy::AnthropicInterleaved { echo_scope, .. } => echo_scope,
                        _ => crate::policy::ThinkingEchoScope::AllAssistantMessages,
                    },
                    // Nor does it change whose signed thinking is echoed across a model switch,
                    // so the cross-model rule already frozen for this run carries over.
                    cross_model: match frozen.reasoning {
                        ReasoningPolicy::AnthropicInterleaved { cross_model, .. } => cross_model,
                        _ => crate::policy::CrossModelThinking::AnyAnthropicModel,
                    },
                },
                _ => ReasoningPolicy::None,
            };
        }
        VariantRequest::OpenAiResponses { effort } => {
            frozen.reasoning = ReasoningPolicy::OpenAiResponsesReasoning {
                effort: effort.clone(),
                summary: Some("auto".into()),
            };
        }
        VariantRequest::Gemini { .. } | VariantRequest::OpenAiChat { .. } => {}
    }
    frozen.variant = Some(variant);
    Ok(())
}

/// The thinking budget the Anthropic family's default interleaved policy carries.
const ANTHROPIC_DEFAULT_BUDGET: u32 = 1024;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_thinking_follows_opencode_version_rule() {
        for (id, manual) in [
            ("claude-opus-4-5", true),
            ("claude-opus-4-5-20251101", true),
            ("claude-sonnet-4-5", true),
            ("claude-haiku-4-5", true),
            ("claude-opus-4-20250514", true),
            // OpenCode's family-first pattern matches `sonnet-20241022` and reads the date as the
            // major version, so this old model is NOT manual there either; ported as is.
            ("claude-3-5-sonnet-20241022", false),
            ("claude-3-5-sonnet", true),
            ("claude-3-opus", true),
            ("claude-opus-4-6", false),
            ("claude-opus-4-8", false),
            ("claude-sonnet-4.6", false),
            ("claude-opus-5", false),
            ("claude-fable-5", false),
            ("anthropic/claude-opus-4-6", false),
        ] {
            assert_eq!(anthropic_manual_thinking(id), manual, "{id}");
        }
    }

    #[test]
    fn repeated_names_keep_first_position_and_last_settings() {
        let request = |effort: &str| VariantRequest::OpenAiChat {
            effort: effort.into(),
        };
        let variants = dedupe_like_a_js_map(vec![
            ("low".into(), request("low")),
            ("high".into(), request("high")),
            ("low".into(), request("override")),
        ]);
        assert_eq!(
            variants
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>(),
            ["low", "high"]
        );
        assert_eq!(variants[0].1, request("override"));
        assert_eq!(variants[1].1, request("high"));
    }
}
