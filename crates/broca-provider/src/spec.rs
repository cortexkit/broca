//! Declarative provider/model description — the DATA half of the provider
//! framework. A concrete provider is `(WireFamily, ProviderSpec)`; most providers
//! are pure config over one of the ~5 wire families, with no bespoke code.
//!
//! These types are sourced from a catalog (e.g. models.dev) plus a local overlay,
//! and the render-relevant subset is frozen into the run's WAL config so a resume
//! renders byte-identical bytes. They carry NO behavior — the wire family reads
//! them. The unbounded `raw` passthrough is INGESTION-ONLY and is never read on the
//! render/normalize path (so the frozen subset stays bounded and complete).

use std::fmt;

use serde::de::{self, Deserializer, Visitor};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;

use crate::policy::Quirks;

/// Which wire grammar a provider speaks. A closed enum: each variant maps to one
/// `WireFamily` implementation via the registry. ~5 of these cover the long tail,
/// because most providers are config over the OpenAI-Chat shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WireFamilyId {
    // The serialized strings are pinned explicitly (rather than via a derived
    // `rename_all`) so they read the conventional way — "openai_chat", not serde's
    // case-boundary split "open_ai_chat". This string is what a human writes in the
    // catalog overlay's `wire_family` field and what lands in the frozen run config,
    // so it must be the intuitive spelling.
    /// OpenAI `/chat/completions` — the big bucket + the OpenAI-compatible long tail.
    #[serde(rename = "openai_chat")]
    OpenAiChat,
    /// OpenAI `/responses`.
    #[serde(rename = "openai_responses")]
    OpenAiResponses,
    /// Anthropic `/v1/messages`.
    #[serde(rename = "anthropic_messages")]
    AnthropicMessages,
    /// Google Gemini `generateContent` (+ Vertex).
    #[serde(rename = "gemini")]
    Gemini,
    /// AWS Bedrock Converse.
    #[serde(rename = "bedrock_converse")]
    BedrockConverse,
    /// Google Antigravity / Code-Assist: the Gemini `generateContent` body wrapped in a
    /// `cloudcode-pa` envelope, project-bound, OAuth Bearer. A distinct family from `Gemini`
    /// because the request body is the envelope (not bare `generateContent`) and the response
    /// is wrapped `{response: ...}`.
    #[serde(rename = "google_codeassist")]
    GoogleCodeAssist,
    /// OpenAI ChatGPT (Codex) backend: the `/responses` body sent to the ChatGPT codex
    /// endpoint (not `api.openai.com`), authed with a ChatGPT-subscription token. The body is
    /// the OpenAI-Responses shape; the family differs in endpoint + headers only.
    #[serde(rename = "openai_responses_chatgpt")]
    OpenAiResponsesChatgpt,
}

impl WireFamilyId {
    /// Anthropic rejects thinking when the preceding conversation was rewritten.
    /// Bedrock Converse drops thinking; there is no prefix-binding evidence for
    /// the other families, so their reasoning is left unchanged.
    pub fn thinking_is_prefix_bound(self) -> bool {
        match self {
            Self::AnthropicMessages => true,
            Self::OpenAiChat
            | Self::OpenAiResponses
            | Self::Gemini
            | Self::BedrockConverse
            | Self::GoogleCodeAssist
            | Self::OpenAiResponsesChatgpt => false,
        }
    }

    /// Returns the stable wire-family name used in API contracts, matching this enum's explicit serde rename attributes.
    pub fn wire_name(&self) -> &'static str {
        match self {
            Self::OpenAiChat => "openai_chat",
            Self::OpenAiResponses => "openai_responses",
            Self::AnthropicMessages => "anthropic_messages",
            Self::Gemini => "gemini",
            Self::BedrockConverse => "bedrock_converse",
            Self::GoogleCodeAssist => "google_codeassist",
            Self::OpenAiResponsesChatgpt => "openai_responses_chatgpt",
        }
    }
}

/// How a provider authenticates. KIND only — never the credential itself. The
/// credential is supplied at runtime and attached AFTER the body is rendered, so
/// it never enters the cache/resume hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AuthKind {
    /// `Authorization: Bearer <token>`.
    Bearer,
    /// A named API-key header, e.g. `x-api-key: <key>`.
    ApiKeyHeader { header: String },
    /// OAuth bearer token (minting/refresh handled out of band).
    OAuth,
    /// AWS SigV4 request signing.
    SigV4 { region: String },
    /// No auth (e.g. a local model server).
    None,
}

/// A provider: which wire family it speaks + the request-shaping config that's
/// constant for the provider (independent of any one request).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderSpec {
    pub id: String,
    pub wire_family: WireFamilyId,
    pub base_url: String,
    pub auth_kind: AuthKind,
    /// Headers sent on every request (ordering is deterministic — a `BTreeMap`).
    #[serde(default)]
    pub static_headers: std::collections::BTreeMap<String, String>,
    /// Body options merged into every request (e.g. provider-required beta flags).
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub static_options: Value,
    /// The typed within-family quirk selections (reasoning/tool-id/etc.).
    #[serde(default)]
    pub quirks: Quirks,
    /// Catalog fields not yet modeled as typed config — preserved verbatim so a
    /// catalog schema addition never silently drops data. INGESTION-ONLY: this is
    /// NEVER read on the render/normalize path (promote a field to a typed config
    /// field if a family needs it).
    ///
    /// What keeps that promise is NOT this comment. `ProviderSpec` derives `Serialize`,
    /// so an unread value here is reachable by any future mapping step. The fence is
    /// one type further in: `FrozenRenderConfig` is the sole render input besides
    /// `CallOptions`, it is narrowed from this struct once at run start, and it
    /// deliberately excludes `raw` — which is why the durable `RunStarted` record
    /// cannot carry the passthrough even though this type can.
    ///
    /// So the edit to be careful about is not one that reads `raw` here; it is one that
    /// WIDENS `FrozenRenderConfig` to include it. That would put unbounded upstream
    /// fields into every WAL record and into the resume byte-identity domain, and
    /// nothing would fail at the moment it was written.
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub raw: Value,
}

/// One model offered by a provider.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelSpec {
    /// The wire model id (what goes in the request `model` field).
    pub id: String,
    #[serde(default)]
    pub display_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub family: Option<String>,
    #[serde(default)]
    pub capabilities: Capabilities,
    #[serde(default)]
    pub limits: Limits,
    #[serde(default)]
    pub cost: CostSchedule,
    /// The reasoning settings the model accepts, verbatim from models.dev `reasoning_options`
    /// (served by fusiform as `capability.reasoning_options`). A caller's `model.variant` is
    /// valid only if these options define it; see `crate::variant`.
    #[serde(default, skip_serializing_if = "ReasoningOptions::is_unstated")]
    pub reasoning_options: ReasoningOptions,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release_date: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    /// Local overlay fields not present in the catalog (reserved for a future
    /// routing layer; not designed yet).
    #[serde(default)]
    pub overlay: Overlay,
    /// Catalog passthrough — INGESTION-ONLY, never read on the render path.
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub raw: Value,
}

/// One catalog capability flag, keeping the three producer statements distinct.
///
/// A catalog document can omit the key, publish a boolean, or publish `null`.
/// Those are not the same claim:
/// - omitted: the document said nothing. Freeze treats this as not capable — the
///   historical default, and the only behavior the current corpus has ever seen
///   for a missing key.
/// - `true` / `false`: an explicit claim.
/// - `null`: the producer looked and does not know. The parser must not invent a
///   boolean; [`CapabilityFlag::admit`] fails closed if the run would use the flag.
///
/// `Option<bool>` cannot carry this: both a missing key and JSON `null` become
/// `None`, which is the collapse this type exists to prevent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum CapabilityFlag {
    /// The catalog omitted the key.
    #[default]
    Absent,
    /// An explicit boolean from the producer.
    Known(bool),
    /// JSON `null`: the producer published "I do not know".
    Unknown,
}

impl CapabilityFlag {
    pub fn is_absent(&self) -> bool {
        matches!(self, Self::Absent)
    }

    /// Decide whether the model has this capability for a run.
    ///
    /// `needed` is whether this run would use the capability if the model has it.
    /// Absent and explicit `false` both resolve to `false` (the historical default).
    /// Unknown is allowed only when the run would not use the flag; otherwise this
    /// refuses rather than inventing a boolean that would silently change the run.
    pub fn admit(
        self,
        needed: bool,
        provider: &str,
        model: &str,
        capability: &'static str,
    ) -> Result<bool, UnknownCapability> {
        match self {
            Self::Known(value) => Ok(value),
            Self::Absent => Ok(false),
            Self::Unknown if !needed => Ok(false),
            Self::Unknown => Err(UnknownCapability {
                provider: provider.to_string(),
                model: model.to_string(),
                capability,
            }),
        }
    }
}

impl Serialize for CapabilityFlag {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Known(value) => serializer.serialize_bool(*value),
            Self::Unknown | Self::Absent => serializer.serialize_none(),
        }
    }
}

impl<'de> Deserialize<'de> for CapabilityFlag {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct FlagVisitor;

        impl Visitor<'_> for FlagVisitor {
            type Value = CapabilityFlag;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a boolean, null, or omitted capability flag")
            }

            fn visit_bool<E: de::Error>(self, value: bool) -> Result<Self::Value, E> {
                Ok(CapabilityFlag::Known(value))
            }

            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(CapabilityFlag::Unknown)
            }

            fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(CapabilityFlag::Unknown)
            }
        }

        deserializer.deserialize_any(FlagVisitor)
    }
}

/// A model's published reasoning options, keeping the catalog's three statements apart.
///
/// - `Unstated`: the catalog served `null`, or did not serve the fact at all (a catalog from
///   before it existed). The upstream said nothing, so the model defines no variant.
/// - `NoOptions`: the catalog served `[]`, the upstream's explicit statement that the model
///   takes no reasoning options. Also no variant, but a different claim than silence.
/// - `Entries`: the published array, in order, each entry kept as served. Entries are not
///   typed here because an entry type or value this build does not understand must reach the
///   variant derivation intact, which decides what to do with it.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum ReasoningOptions {
    #[default]
    Unstated,
    NoOptions,
    Entries(std::sync::Arc<[Value]>),
}

impl ReasoningOptions {
    pub fn is_unstated(&self) -> bool {
        matches!(self, Self::Unstated)
    }

    /// Read the served JSON value. `None` is anything that is neither `null` nor an array.
    pub fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Null => Some(Self::Unstated),
            Value::Array(entries) if entries.is_empty() => Some(Self::NoOptions),
            Value::Array(entries) => Some(Self::Entries(entries.clone().into())),
            _ => None,
        }
    }

    /// The JSON value this was read from.
    pub fn to_value(&self) -> Value {
        match self {
            Self::Unstated => Value::Null,
            Self::NoOptions => Value::Array(Vec::new()),
            Self::Entries(entries) => Value::Array(entries.to_vec()),
        }
    }
}

impl Serialize for ReasoningOptions {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.to_value().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ReasoningOptions {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        Self::from_value(&value)
            .ok_or_else(|| de::Error::custom("reasoning_options must be null or an array"))
    }
}

/// Admission refused because a catalog capability the run would use is `null`.
///
/// Grep for `unknown_capability:` — the Display form names the provider, model,
/// and flag so a refusal at freeze is attributable without reading a backtrace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownCapability {
    pub provider: String,
    pub model: String,
    pub capability: &'static str,
}

impl fmt::Display for UnknownCapability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "unknown_capability: provider={} model={} capability={}",
            self.provider, self.model, self.capability
        )
    }
}

impl std::error::Error for UnknownCapability {}

/// What a model can do (mirrors the catalog's capability flags). The wire family
/// uses these to decide whether to render a thinking shape, tools, etc., and the
/// conformance harness checks that the declared flags match what the family
/// actually renders/normalizes.
///
/// Each flag is a [`CapabilityFlag`] so a producer `null` ("I do not know") stays
/// distinct from both an omitted key and an explicit `false` until freeze, where
/// the run's requirements are known.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Capabilities {
    #[serde(default, skip_serializing_if = "CapabilityFlag::is_absent")]
    pub attachment: CapabilityFlag,
    #[serde(default, skip_serializing_if = "CapabilityFlag::is_absent")]
    pub reasoning: CapabilityFlag,
    #[serde(default, skip_serializing_if = "CapabilityFlag::is_absent")]
    pub temperature: CapabilityFlag,
    #[serde(default, skip_serializing_if = "CapabilityFlag::is_absent")]
    pub tool_call: CapabilityFlag,
    #[serde(default, skip_serializing_if = "CapabilityFlag::is_absent")]
    pub interleaved: CapabilityFlag,
}

/// Token limits (mirrors the catalog).
///
/// `None` means the catalog stated no limit, which is NOT the same as zero. The upstream
/// publishes `0` for fields that do not apply -- 186 models declare `output: 0` and 124
/// declare `context: 0`. A zero here is "not applicable" wearing the costume of a
/// measurement, and a consumer that reads it as a real bound gets a silently absurd one.
///
/// TREAT ZERO AS ABSENT PER FIELD, never per row. 90 models mix a real limit with a zero
/// one -- `alibaba-token-plan/qwen-image-2.0` declares `{context: 8192, output: 0}`, and
/// eight such models sit under providers we serve. A row-level rule ("this model has no
/// token model, ignore its limits") looks tidier and is wrong for those: it would discard
/// a real context window alongside an inapplicable output cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Limits {
    /// Consumed: surfaced as the run's `context_limit`, which is the prompt-reduction
    /// transform's pressure signal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<u64>,
    /// PARSED BUT UNREAD, and carrying the same zero hazard: 5 models publish
    /// `input: 0`, all image models. See `max_output`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_input: Option<u64>,
    /// Read in exactly one place: `default_max_output_tokens` in broca-catalog's `live.rs`,
    /// which uses it as the output cap when a send sets none. It never clamps a caller's
    /// explicit cap. That reader treats zero as absent, and treats a value not strictly below
    /// `context` as a placeholder (catalogs publish output == context on over a thousand
    /// rows), falling back to a fixed default in both cases.
    ///
    /// Any new consumer needs the same care: map zero to absent before anything else, or
    /// `max_output_tokens: 0` renders for any model publishing one and no guard here objects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output: Option<u64>,
}

/// Per-token cost (mirrors the catalog). Used by cost accounting, never by render.
///
/// `None` means the catalog did not state a rate — which is NOT the same as zero
/// (free). A cost-aware router must distinguish "no published rate" from "free", so
/// every rate is optional. Beyond the base rates, models.dev also publishes reasoning
/// and audio token rates and CONTEXT-SIZE tiers (a longer prompt is billed at a
/// higher rate above a threshold); those are preserved here rather than dropped.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct CostSchedule {
    /// Base input (prompt) token rate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<f64>,
    /// Base output (completion) token rate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<f64>,
    /// Cache-read (prompt-cache hit) token rate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_read: Option<f64>,
    /// Cache-write (prompt-cache store) token rate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write: Option<f64>,
    /// Reasoning (thinking) token rate, when billed separately from output.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<f64>,
    /// Audio input token rate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_audio: Option<f64>,
    /// Audio output token rate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_audio: Option<f64>,
    /// Context-size pricing tiers, sorted ascending by `min_context`. When a prompt
    /// exceeds a tier's threshold, that tier's rates apply instead of the base rates.
    /// Empty = flat pricing.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tiers: Vec<CostTier>,
}

/// One context-size pricing tier: the rates that apply once the prompt is at least
/// `min_context` tokens. Rates are optional for the same reason as `CostSchedule`.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct CostTier {
    /// The prompt-size threshold (in tokens) at or above which this tier applies.
    pub min_context: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_read: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write: Option<f64>,
}

/// Local overlay fields (ours, not from the catalog). Reserved for a future
/// routing layer; intentionally minimal for now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Overlay {
    /// Whether the model supports structured output (the catalog doesn't model
    /// this). `None` = unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub structured_output: Option<bool>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The `WireFamilyId` wire strings are a stable contract: they're written by hand
    /// in the catalog overlay and stored in the frozen run config. Pin them so a
    /// rename can't silently shift the serialized form (which would orphan overlays
    /// and break frozen-config round-trips on resume).
    #[test]
    fn wire_family_id_serialized_strings_are_stable() {
        for (family, expected) in [
            (WireFamilyId::OpenAiChat, "openai_chat"),
            (WireFamilyId::OpenAiResponses, "openai_responses"),
            (WireFamilyId::AnthropicMessages, "anthropic_messages"),
            (WireFamilyId::Gemini, "gemini"),
            (WireFamilyId::BedrockConverse, "bedrock_converse"),
            (WireFamilyId::GoogleCodeAssist, "google_codeassist"),
            (
                WireFamilyId::OpenAiResponsesChatgpt,
                "openai_responses_chatgpt",
            ),
        ] {
            let serialized = serde_json::to_value(family).unwrap();
            assert_eq!(
                serialized,
                serde_json::Value::String(expected.to_string()),
                "serialized form of {family:?} changed"
            );
            assert_eq!(
                family.wire_name(),
                expected,
                "wire_name drifted from serde for {family:?}"
            );
            let back: WireFamilyId = serde_json::from_value(serialized).unwrap();
            assert_eq!(back, family, "round-trip failed for {family:?}");
        }
    }

    #[test]
    fn capability_flag_serde_keeps_null_distinct_from_omitted() {
        let json = serde_json::json!({
            "reasoning": null,
            "tool_call": false,
            "attachment": true
        });
        let caps: Capabilities = serde_json::from_value(json).unwrap();
        assert_eq!(caps.reasoning, CapabilityFlag::Unknown);
        assert_eq!(caps.tool_call, CapabilityFlag::Known(false));
        assert_eq!(caps.attachment, CapabilityFlag::Known(true));
        assert_eq!(caps.temperature, CapabilityFlag::Absent);
        assert_eq!(caps.interleaved, CapabilityFlag::Absent);
    }

    #[test]
    fn capability_flag_admit_fails_closed_only_when_unknown_and_needed() {
        let cases = [
            (CapabilityFlag::Absent, true, Ok(false)),
            (CapabilityFlag::Known(false), true, Ok(false)),
            (CapabilityFlag::Known(true), true, Ok(true)),
            (CapabilityFlag::Unknown, false, Ok(false)),
        ];
        for (flag, needed, expected) in cases {
            assert_eq!(
                flag.admit(needed, "prov", "model", "reasoning"),
                expected,
                "{flag:?} needed={needed}"
            );
        }
        let err = CapabilityFlag::Unknown
            .admit(true, "prov", "model", "reasoning")
            .expect_err("unknown + needed must refuse");
        assert_eq!(
            err.to_string(),
            "unknown_capability: provider=prov model=model capability=reasoning"
        );
    }
}
