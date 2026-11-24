//! The frozen plan manifest a `RunStarted` may carry: the fetch plan a session's first send
//! named, what each of its tool providers answered when fetched, and what was built from those
//! answers (the tools offered to the model and the joined system text), frozen so that every
//! later episode and every resume renders from exactly those bytes and never fetches again.
//!
//! The plan itself is stored as the opaque JSON it arrived as, never as fields of this record:
//! the plan's layout belongs to the caller that writes it, and may grow without a log change.
//!
//! Nothing here decides how a manifest is fetched, joined or checked; `broca-fetch-plan` builds
//! it at admission and the run loop records it. A reader must carry every value through
//! untouched, because the digests recorded beside them were computed over the values' bytes as
//! the providers answered them.

use std::{collections::BTreeMap, sync::Arc};

use broca_protocol::request::Tool;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// JSON that broca stores and passes on but never interprets, such as the session's fetch
/// plan. Held as its JSON text.
///
/// On disk it is a JSON STRING whose content is that JSON text, not an embedded JSON value.
/// `WalRecord` is an internally tagged enum, and serde decodes every field of such an enum
/// through an intermediate buffer that keeps numbers only as `u64`, `i64` or `f64` and strings
/// only by value. An embedded value would therefore come back with its keys re-sorted, `2.50`
/// re-spelled `2.5`, `1e3` re-spelled `1000.0` and escapes normalized, and a reader could no
/// longer reproduce the bytes a digest was computed over. A string survives that buffer exactly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OpaqueJson(String);

/// The reply declaration stored in a frozen plan. The log owns this wire shape so a
/// provider role's serde changes cannot silently change durable record bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrozenReply {
    pub max_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hold_argument: Option<FrozenHoldArgument>,
}

/// The provider's own result-format declaration for one tool, frozen with the plan. The log
/// treats it as an open string so a contract's later format is stored verbatim rather than
/// refused; only the surface that reads it decides which names it understands.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FrozenResultFormat(pub String);

/// Session-level declarations of one tool provider, frozen with the plan.
///
/// These are the baseline declarations. New-storage runs may freeze transport-only overrides
/// on their own start, without changing this plan or its schema pins. A resumed run uses its
/// recorded facts, never a later catalog, so provider changes cannot alter reply decoding in
/// flight. Everything here is optional and absent when the provider did not declare it, so a
/// manifest from a provider with no such declarations keeps its exact bytes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrozenProviderDeclarations {
    /// The provider promised call keepalives in its manifest at admission. Missing on
    /// older manifests and non-declaring providers, which retain legacy waits on resume.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub call_keepalive: Option<bool>,
    /// Per-tool result formats, by tool name, frozen only for tools that opt in. Absent means no
    /// tool of this provider declared one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_formats: Option<BTreeMap<String, FrozenResultFormat>>,
    /// Whether the provider declared the `late_results` session capability, frozen only when it
    /// answered yes. Absent means the provider cannot settle a call after its reply.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub late_results: Option<bool>,
    /// Actual tool.catalog identities frozen at admission. Old manifests and
    /// non-plan tools carry none; a later catalog must not fill the missing facts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_pins: Option<BTreeMap<String, super::DispatchSchemaPin>>,
    /// Tools whose calls broca may yield, frozen at admission. Present only when the provider
    /// declared `call-yield/v1` and `late_results`, and only for tools tagged `call.yield/v1`
    /// using `tool-result/v1`. Missing or empty means no call is yieldable.
    #[serde(default, skip_serializing_if = "no_yield_tools")]
    pub yield_tools: Option<std::collections::BTreeSet<String>>,
}

fn no_yield_tools(tools: &Option<std::collections::BTreeSet<String>>) -> bool {
    tools.as_ref().is_none_or(|tools| tools.is_empty())
}

impl FrozenProviderDeclarations {
    /// Whether nothing was declared, so the declaration writes no bytes at all.
    pub fn is_empty(&self) -> bool {
        self.call_keepalive.is_none()
            && self.result_formats.is_none()
            && self.late_results.is_none()
            && self.schema_pins.is_none()
            && no_yield_tools(&self.yield_tools)
    }
}

/// The numeric hold argument saved with a tool's reply declaration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrozenHoldArgument {
    pub name: String,
    pub default_ms: u64,
    pub grace_ms: u64,
}

impl OpaqueJson {
    /// Wrap `text`, refusing it unless it is one well-formed JSON value. Only a writer checks
    /// this; decoding a stored record never parses the text.
    pub fn from_json_text(text: impl Into<String>) -> Result<Self, serde_json::Error> {
        let text = text.into();
        serde_json::from_str::<serde::de::IgnoredAny>(&text)?;
        Ok(Self(text))
    }

    /// The JSON text exactly as it was stored.
    pub fn as_json_text(&self) -> &str {
        &self.0
    }
}

/// One reference slot a catalog tool declared, as frozen: the argument's top-level name and its
/// kind (`token`, or `source:<dialect>`). Kinds stay open strings, as the catalog sends them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrozenReferenceSlot {
    pub name: String,
    pub kind: String,
}

/// What one provider's fetch at admission came back with.
///
/// An answered fetch with no `text` (no system-text item was asked of the provider), one whose
/// `text` is present but empty (asked, and the provider answered with no text), and an absent
/// fetch (an optional item the provider did not answer, so nothing of it was joined) are three
/// different facts and stay different through a decode.
#[derive(Debug, Clone, PartialEq)]
pub enum FetchOutcome {
    /// The provider answered. Written as `catalog_digest`, `tools` and, when a system-text item
    /// was asked of it, `text`.
    Answered {
        /// The provider's digest of its answer.
        catalog_digest: String,
        /// The tools the provider answered, exactly as answered and in its order; empty for a
        /// provider that was asked only for system text.
        tools: Arc<Vec<Tool>>,
        /// The system text the provider answered, byte for byte: `Some` when a system-text item
        /// was asked of it (possibly `""`), `None` when none was asked.
        text: Option<String>,
        /// Per-tool operation permissions frozen from the catalog, including its default.
        /// Absent means permission was not frozen; it never authorizes result replacement.
        result_ops: Option<BTreeMap<String, Vec<String>>>,
        /// Reply deadlines by tool name, frozen only when the provider declares them.
        /// Absent keeps manifests from providers without deadlines byte-compatible.
        reply: Option<BTreeMap<String, FrozenReply>>,
        /// Result formats and session capabilities the provider declared, frozen only when it
        /// declared some. Absent keeps manifests from providers without declarations
        /// byte-compatible.
        declarations_frozen: Option<Box<FrozenProviderDeclarations>>,
        /// The catalog's reference slots by tool name: the top-level string arguments in which
        /// output references are read (llm-runner §9.13). Frozen only when the provider's plan
        /// item opts into output references and one of its tools declares slots; absent
        /// otherwise, which keeps every other manifest's bytes. Must-understand (feature
        /// `plan-reference-slots/v1`): a reader that dropped it would re-freeze later episodes
        /// without the slots.
        reference_slots: Option<BTreeMap<String, Vec<FrozenReferenceSlot>>>,
    },
    /// An optional tool or system-text provider that gave no usable answer, so the session was
    /// admitted without it. Written as `"absent": "<reason>"`. The reason is an open string
    /// (for example `timeout`, `unknown_module`, `plan_stale` or `refused:<code>`) that a reader keeps and
    /// never interprets.
    Absent {
        /// Why the provider did not answer.
        reason: String,
    },
    /// A fetch carrying neither `catalog_digest` nor `absent`: an outcome a newer broca release
    /// writes that this release does not know. Decoding it keeps the record readable; it is never
    /// written, because nothing of the original outcome survives to write back.
    Unknown,
}

/// One provider's final fetch outcome at admission, combining its tool and system-text items.
#[derive(Debug, Clone, PartialEq)]
pub struct ProviderFetch {
    /// The provider module that was fetched.
    pub provider: String,
    /// What the fetch came back with.
    pub outcome: FetchOutcome,
}

/// The on-disk shape of a [`ProviderFetch`], flat: an answered fetch writes `catalog_digest`,
/// `tools` and optionally `text`; an absent one writes `absent`. Field order is part of the
/// byte layout.
#[derive(Serialize)]
struct ProviderFetchOut<'a> {
    provider: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    catalog_digest: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<&'a [Tool]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    absent: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result_ops: Option<&'a BTreeMap<String, Vec<String>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reply: Option<&'a BTreeMap<String, FrozenReply>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    declarations_frozen: Option<&'a FrozenProviderDeclarations>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reference_slots: Option<&'a BTreeMap<String, Vec<FrozenReferenceSlot>>>,
}

#[derive(Deserialize)]
struct ProviderFetchIn {
    provider: String,
    #[serde(default)]
    catalog_digest: Option<String>,
    #[serde(default)]
    tools: Option<Arc<Vec<Tool>>>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    absent: Option<String>,
    #[serde(default)]
    result_ops: Option<BTreeMap<String, Vec<String>>>,
    #[serde(default)]
    reply: Option<BTreeMap<String, FrozenReply>>,
    #[serde(default)]
    declarations_frozen: Option<Box<FrozenProviderDeclarations>>,
    #[serde(default)]
    reference_slots: Option<BTreeMap<String, Vec<FrozenReferenceSlot>>>,
}

impl Serialize for ProviderFetch {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut out = ProviderFetchOut {
            provider: &self.provider,
            catalog_digest: None,
            tools: None,
            text: None,
            absent: None,
            result_ops: None,
            reply: None,
            declarations_frozen: None,
            reference_slots: None,
        };
        match &self.outcome {
            FetchOutcome::Answered {
                catalog_digest,
                tools,
                text,
                result_ops,
                reply,
                declarations_frozen,
                reference_slots,
            } => {
                out.catalog_digest = Some(catalog_digest);
                out.tools = Some(tools.as_slice());
                out.text = text.as_deref();
                out.result_ops = result_ops.as_ref();
                out.reply = reply.as_ref();
                out.declarations_frozen = declarations_frozen.as_deref();
                out.reference_slots = reference_slots.as_ref();
            }
            FetchOutcome::Absent { reason } => out.absent = Some(reason),
            FetchOutcome::Unknown => {
                return Err(serde::ser::Error::custom(
                    "a provider fetch whose outcome this build does not know cannot be written",
                ))
            }
        }
        out.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ProviderFetch {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let fetch = ProviderFetchIn::deserialize(deserializer)?;
        // A declaration is frozen only beside an answered fetch: an absent provider declared
        // nothing, so a record carrying one next to `absent` is malformed.
        if fetch.absent.is_some() && fetch.declarations_frozen.is_some() {
            return Err(serde::de::Error::custom(
                "an absent provider fetch cannot carry frozen declarations",
            ));
        }
        let outcome = match (fetch.catalog_digest, fetch.absent) {
            (Some(catalog_digest), None) => FetchOutcome::Answered {
                catalog_digest,
                tools: fetch.tools.ok_or_else(|| {
                    serde::de::Error::custom("an answered provider fetch must carry `tools`")
                })?,
                text: fetch.text,
                result_ops: fetch.result_ops,
                reply: fetch.reply,
                declarations_frozen: fetch.declarations_frozen,
                reference_slots: fetch.reference_slots,
            },
            (None, Some(reason)) => {
                if fetch.tools.is_some() || fetch.text.is_some() {
                    return Err(serde::de::Error::custom(
                        "an absent provider fetch cannot carry `tools` or `text`",
                    ));
                }
                FetchOutcome::Absent { reason }
            }
            (None, None) => FetchOutcome::Unknown,
            (Some(_), Some(_)) => {
                return Err(serde::de::Error::custom(
                    "a provider fetch cannot carry both `catalog_digest` and `absent`",
                ))
            }
        };
        Ok(ProviderFetch {
            provider: fetch.provider,
            outcome,
        })
    }
}

/// Everything a send or refresh fetched and froze: the plan as received, each provider's
/// answer, the tools offered to the model, the joined system text, and the versions that decided
/// how they were combined. Each `RunStarted` carries its own manifest so replay keeps that
/// episode's tools and text even after a session refresh or a log fold archives earlier episodes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlanManifest {
    /// The agreed stamp from the fetched system-text answers, not from the caller's plan.
    /// Older manifests did not freeze this metadata and therefore remain unstamped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rich_ui: Option<broca_protocol::RichUiStamp>,
    /// The `plan` member of the session's first `session.send`, exactly as received.
    pub plan: OpaqueJson,
    /// The plan's identity: SHA-256, as 64 lowercase hex characters, of the RFC 8785 (JCS)
    /// canonical bytes of `plan`. A later send's plan is the same plan exactly when its
    /// identity is equal.
    pub plan_identity: String,
    /// The composition digest the plan stated and the writer verified against the plan's
    /// composition, as 64 lowercase hex characters.
    pub composition_digest: String,
    /// One entry per provider fetched, sorted by provider id bytewise.
    pub fetches: Vec<ProviderFetch>,
    /// The tools offered to the model, in the order they were offered, in the same form as a
    /// run's frozen tools.
    pub tools: Arc<Vec<Tool>>,
    /// The session's system text: the answered texts joined.
    pub system_text: String,
    /// The digest of `system_text`.
    pub system_digest: String,
    /// Which joining rule produced `system_text` from the answered texts.
    pub join_version: u32,
    /// The version of each role's definition the session was admitted with, by role name.
    pub role_versions: BTreeMap<String, String>,
    /// One declaration per step-transform item, in plan order. Empty on sessions admitted
    /// without transforms, including every manifest written before declarations existed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub declarations: Vec<FrozenDeclaration>,
}

/// A provider's `transform.declare` answer frozen at admission, never fetched again on resume.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrozenDeclaration {
    pub provider: String,
    /// Stored as opaque JSON text for the same byte-preservation reason as `PlanManifest.plan`.
    pub answer: OpaqueJson,
}
