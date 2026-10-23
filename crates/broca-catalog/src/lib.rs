//! broca catalog — turns fusiform's runtime model facts plus Broca's serving overlay into
//! the provider framework's `ProviderSpec`/`ModelSpec`.
//!
//! Admission only reads an immutable in-memory snapshot. A detached poll refreshes that snapshot
//! and its disk last-known-good copy, so fusiform and filesystem latency never enter resolution.
//!
//! Fusiform owns model and provider facts, including wire packages, shapes, endpoints,
//! reasoning tier ids and retirement successors. Broca maps packages and shapes to its
//! implemented renderers. The serving overlay contains no copied model routing facts.
//! The legacy snapshot importer remains available for offline tooling. Its FULL
//! original object is preserved verbatim in `raw` (forward-compat — models.dev adds
//! fields, and an unmapped field must never be silently dropped). The renderer NEVER
//! reads `raw`; any field that can change request bytes is promoted to a typed field
//! (a test over the real snapshot enforces that every key is classified).
//!
//! Renderer implementation is Broca's decision; the upstream protocol being implemented
//! comes from the row's wire facts. See `family.rs`.
#![forbid(unsafe_code)]

pub mod family;
#[cfg(feature = "live")]
pub mod keep_warm;
/// The catalog → live provider bridge (resolve a model id + auth into a runnable provider).
/// Behind `live` because it pulls the provider's live transport.
#[cfg(feature = "live")]
pub mod live;
pub mod merge;
pub mod routing;
pub mod runtime;

pub use runtime::{
    catalog, global_catalog_cache, install_global_catalog_cache, CatalogCache, CatalogHealth,
    CatalogRefreshError, CatalogSnapshot, CatalogSource, CatalogUnavailable,
    FusiformCatalogResponse, ModelBatchRow, FUSIFORM_FACT_PREFIXES,
};

use std::collections::{BTreeMap, BTreeSet};

use broca_provider::policy::{Quirks, TokenLimitField, UsagePolicy};
use broca_provider::spec::{
    AuthKind, Capabilities, CapabilityFlag, CostSchedule, CostTier, Limits, ModelSpec, Overlay,
    ProviderSpec, ReasoningOptions, WireFamilyId,
};
use family::{resolve_provider_family, FamilyResolution, ResolutionSource};
use serde::Deserialize;
use serde_json::Value;

/// Native routing holds and renderer compatibility choices layered over Fusiform.
const OVERLAY: &str = include_str!("../data/overlay.json");

/// Where the vendored snapshot came from and when — so a stale catalog is visible
/// rather than silent.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Metadata {
    pub source_url: String,
    pub fetched_at: String,
    pub sha256: String,
    pub provider_count: u64,
    pub model_count: u64,
    #[serde(default)]
    pub note: String,
}

/// A provider we cannot safely serve yet: its grammar is native (not
/// OpenAI-compatible) and not in our hand-table, so choosing a renderer would be a
/// guess. Surfaced for review, never silently defaulted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingProvider {
    pub id: String,
    pub reason: String,
}

/// What can go wrong assembling the catalog — all structural overlay mistakes that
/// should fail loudly (a typo'd key must not silently materialize or vanish).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogError {
    BaseParse(String),
    OverlayParse(String),
    /// An overlay `patch` names a provider absent from the catalog — almost always a
    /// typo or a key left behind after a catalog refresh.
    PatchTargetMissing(String),
    /// An overlay `add` names a provider already in the catalog — it should be a
    /// `patch`, not an `add`.
    AddTargetExists(String),
    /// An overlay `remove` is not an object of provider ids to arrays of model-id strings.
    RemoveShapeInvalid {
        provider: Option<String>,
        found: String,
    },
    /// An overlay `remove` names a provider absent from the catalog.
    RemoveTargetMissing(String),
    /// An overlay `remove` names a model the provider does not carry — the model id is
    /// wrong, or upstream already dropped it and the suppression is now dead weight.
    RemoveModelMissing {
        provider: String,
        model: String,
    },
    /// Fusiform returned a model map key that is not the required `provider_id/model_id` form.
    FusiformModelIdentity(String),
    /// A required fusiform fact was absent or had a wire type Broca cannot safely interpret.
    FusiformFact {
        model: String,
        fact: String,
        reason: String,
    },
    /// Fusiform withheld or marked uncertain a requested serving fact. Loading it would turn a
    /// known-bad or non-current fact into an ordinary absence.
    FusiformQualifiedFacts {
        withheld: usize,
        uncertain: usize,
    },
}

impl std::fmt::Display for CatalogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogError::BaseParse(e) => write!(f, "vendored snapshot is not valid JSON: {e}"),
            CatalogError::OverlayParse(e) => write!(f, "overlay is not valid JSON: {e}"),
            CatalogError::PatchTargetMissing(id) => write!(
                f,
                "overlay patch targets provider '{id}' which is not in the catalog (typo, or stale after a refresh?) — use `add` to introduce a new provider"
            ),
            CatalogError::AddTargetExists(id) => write!(
                f,
                "overlay add targets provider '{id}' which already exists in the catalog — use `patch` to modify it"
            ),
            CatalogError::RemoveShapeInvalid {
                provider: Some(provider),
                found,
            } => write!(
                f,
                "overlay remove entry for provider '{provider}' must be an array of model-id strings; found {found}"
            ),
            CatalogError::RemoveShapeInvalid {
                provider: None,
                found,
            } => write!(
                f,
                "overlay remove must be an object keyed by provider id; found {found}"
            ),
            CatalogError::RemoveTargetMissing(id) => write!(
                f,
                "overlay remove targets provider '{id}' which is not in the catalog (typo, or stale after a refresh?)"
            ),
            CatalogError::RemoveModelMissing { provider, model } => write!(
                f,
                "overlay remove targets model '{model}' which provider '{provider}' does not carry — upstream may have dropped it already, in which case delete the suppression"
            ),
            CatalogError::FusiformModelIdentity(identity) => write!(
                f,
                "fusiform model identity '{identity}' is invalid: expected 'provider_id/model_id'"
            ),
            CatalogError::FusiformFact { model, fact, reason } => write!(
                f,
                "fusiform model '{model}' has unusable fact '{fact}': {reason}"
            ),
            CatalogError::FusiformQualifiedFacts {
                withheld,
                uncertain,
            } => write!(
                f,
                "fusiform catalog has {withheld} withheld and {uncertain} uncertain requested facts; retaining the prior complete catalog"
            ),
        }
    }
}

impl std::error::Error for CatalogError {}

/// The merged, parsed catalog.
pub struct Catalog {
    /// provider_id -> merged provider object (with nested `models`).
    providers: BTreeMap<String, Value>,
    /// provider_id -> (resolved wire family, the rule that chose it), for providers
    /// we can serve.
    resolved_family: BTreeMap<String, (WireFamilyId, ResolutionSource)>,
    /// Providers we cannot serve yet (native grammar, no family mapping).
    pending: Vec<PendingProvider>,
    /// provider ids introduced by the overlay `add` block (not in the snapshot).
    overlay_added: Vec<String>,
    /// Models suppressed by the overlay `remove` block, retained after their serving rows are gone.
    overlay_removed_models: BTreeMap<String, BTreeSet<String>>,
    /// Explicit upstream withdrawals retained from fusiform's `include_retired` response. These
    /// are not serveable rows, but keeping the identity distinguishes a withdrawal from a typo.
    withdrawn_models: BTreeSet<(String, String)>,
    metadata: Option<Metadata>,
}

/// One provider's resolution outcome, for the committed refresh report. The report is
/// per-provider (not an aggregate count) so a single reclassification on a catalog
/// refresh surfaces as a reviewed row — that's what makes a silent family re-route
/// impossible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolutionRow {
    pub provider_id: String,
    pub npm: Option<String>,
    /// The resolved wire family tag (e.g. "openai_chat"), or `None` if pending.
    pub resolved_family: Option<String>,
    /// Which rule resolved it ("overlay"/"provider_id"), or "pending".
    pub resolution_source: String,
    /// Why it's pending (empty when served).
    pub pending_reason: String,
}

impl Catalog {
    /// Parse + merge a base catalog and an overlay from JSON strings (used by tooling, tests,
    /// and the runtime fusiform mapper).
    ///
    /// Overlay shape: `{ "patch": { id: {..} }, "add": { id: {complete spec} },
    /// "remove": { id: [model_ids] } }`.
    /// A `patch` id MUST exist in the catalog; an `add` id MUST NOT; a `remove` id and
    /// each model under it MUST exist.
    ///
    /// `remove` exists because the snapshot is upstream data: when a provider retires a
    /// model that models.dev still lists, every refresh reinstates a row we know cannot
    /// be served, and patching cannot express a deletion (deep_merge only adds and
    /// replaces). It is deliberately model-scoped — no provider-wide removal until
    /// something actually needs one.
    pub fn from_json_str(base_json: &str, overlay_json: &str) -> Result<Self, CatalogError> {
        let base: Value =
            serde_json::from_str(base_json).map_err(|e| CatalogError::BaseParse(e.to_string()))?;
        let overlay: Value = serde_json::from_str(overlay_json)
            .map_err(|e| CatalogError::OverlayParse(e.to_string()))?;

        let base_obj = base.as_object().cloned().unwrap_or_default();
        let empty = serde_json::Map::new();
        let overlay_obj = overlay.as_object().unwrap_or(&empty);
        let patch = overlay_obj
            .get("patch")
            .and_then(Value::as_object)
            .unwrap_or(&empty);
        let add = overlay_obj
            .get("add")
            .and_then(Value::as_object)
            .unwrap_or(&empty);
        let remove = match overlay_obj.get("remove") {
            None => &empty,
            Some(Value::Object(remove)) => remove,
            Some(value) => {
                return Err(CatalogError::RemoveShapeInvalid {
                    provider: None,
                    found: json_value_kind(value).to_string(),
                });
            }
        };
        let validated_remove = remove
            .iter()
            .map(|(provider, model_ids)| {
                let model_ids =
                    model_ids
                        .as_array()
                        .ok_or_else(|| CatalogError::RemoveShapeInvalid {
                            provider: Some(provider.clone()),
                            found: json_value_kind(model_ids).to_string(),
                        })?;
                let mut validated_ids = Vec::with_capacity(model_ids.len());
                for (index, model_id) in model_ids.iter().enumerate() {
                    let model_id =
                        model_id
                            .as_str()
                            .ok_or_else(|| CatalogError::RemoveShapeInvalid {
                                provider: Some(provider.clone()),
                                found: format!(
                                    "{} at array index {index}",
                                    json_value_kind(model_id)
                                ),
                            })?;
                    validated_ids.push(model_id);
                }
                Ok((provider, validated_ids))
            })
            .collect::<Result<Vec<_>, CatalogError>>()?;

        // Validate overlay structure loudly: patch must hit an existing provider, add
        // must not collide with one.
        for id in patch.keys() {
            if !base_obj.contains_key(id) {
                return Err(CatalogError::PatchTargetMissing(id.clone()));
            }
        }
        for id in add.keys() {
            if base_obj.contains_key(id) {
                return Err(CatalogError::AddTargetExists(id.clone()));
            }
        }

        let mut providers = BTreeMap::new();
        for (id, base_entry) in &base_obj {
            let merged = match patch.get(id) {
                Some(p) => merge::deep_merge(base_entry, p),
                None => base_entry.clone(),
            };
            providers.insert(id.clone(), merged);
        }
        let mut overlay_added: Vec<String> = Vec::new();
        for (id, add_entry) in add {
            providers.insert(id.clone(), add_entry.clone());
            overlay_added.push(id.clone());
        }
        overlay_added.sort();

        // Suppress retired models after the merge, so a suppression can target a row that
        // `patch` also touches and the ordering stays obvious: merge, then delete.
        let mut overlay_removed_models: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for (provider_id, model_ids) in validated_remove {
            let entry = providers
                .get_mut(provider_id)
                .ok_or_else(|| CatalogError::RemoveTargetMissing(provider_id.clone()))?;
            let models = entry
                .get_mut("models")
                .and_then(Value::as_object_mut)
                .ok_or_else(|| CatalogError::RemoveTargetMissing(provider_id.clone()))?;
            for model_id in model_ids {
                if models.remove(model_id).is_none() {
                    return Err(CatalogError::RemoveModelMissing {
                        provider: provider_id.clone(),
                        model: model_id.to_string(),
                    });
                }
                overlay_removed_models
                    .entry(provider_id.clone())
                    .or_default()
                    .insert(model_id.to_string());
            }
        }

        // Resolve a renderer per provider; collect the ones we can't serve.
        let mut resolved_family = BTreeMap::new();
        let mut pending = Vec::new();
        for (id, entry) in &providers {
            let overlay_family = entry.get("wire_family").and_then(parse_family);
            let resolution =
                if overlay_family.is_some() || entry.get("npm").and_then(Value::as_str).is_none() {
                    resolve_provider_family(id, overlay_family)
                } else {
                    FamilyResolution::Pending {
                        reason: format!("{id}: unsupported published wire package"),
                    }
                };
            let resolution = match resolution {
                FamilyResolution::Pending { .. } => entry
                    .get("npm")
                    .and_then(Value::as_str)
                    .and_then(|npm| family::wire_family(npm, None))
                    .map(|family| FamilyResolution::Supported {
                        family,
                        source: ResolutionSource::WireFacts,
                    })
                    .or_else(|| {
                        family::credential_persona_route(id, "oauth").map(|(family, _)| {
                            FamilyResolution::Supported {
                                family,
                                source: ResolutionSource::CredentialPersona,
                            }
                        })
                    })
                    .unwrap_or(resolution),
                _ => resolution,
            };
            match resolution {
                FamilyResolution::Supported { family, source } => {
                    resolved_family.insert(id.clone(), (family, source));
                }
                FamilyResolution::Pending { reason } => {
                    pending.push(PendingProvider {
                        id: id.clone(),
                        reason,
                    });
                }
            }
        }
        pending.sort_by(|a, b| a.id.cmp(&b.id));

        Ok(Self {
            providers,
            resolved_family,
            pending,
            overlay_added,
            overlay_removed_models,
            withdrawn_models: BTreeSet::new(),
            metadata: None,
        })
    }

    /// Snapshot provenance (only present on the bundled catalog).
    pub fn metadata(&self) -> Option<&Metadata> {
        self.metadata.as_ref()
    }

    /// Providers we cannot serve yet (native grammar, no family mapping) — surface in
    /// review and in the refresh report so each gets a deliberate decision.
    pub fn pending_providers(&self) -> &[PendingProvider] {
        &self.pending
    }

    /// Provider ids the overlay introduced (not in the vendored snapshot).
    pub fn overlay_added(&self) -> &[String] {
        &self.overlay_added
    }

    /// All serveable provider ids (a renderer was resolved), sorted.
    pub fn provider_ids(&self) -> impl Iterator<Item = &str> {
        self.resolved_family.keys().map(String::as_str)
    }

    /// Resolve a serveable provider's `ProviderSpec` (without a model). `None` if the
    /// id is absent or pending (no safe renderer).
    pub fn provider(&self, provider_id: &str) -> Option<ProviderSpec> {
        let (family, _source) = *self.resolved_family.get(provider_id)?;
        let entry = self.providers.get(provider_id)?;
        Some(map_provider(
            provider_id,
            entry,
            family,
            RawPolicy::Preserve,
        ))
    }

    /// The per-provider resolution report over EVERY provider in the merged catalog
    /// (served and pending), sorted by id. This is the committed refresh artifact —
    /// CI fails if a snapshot bump changes any row without the report diff being
    /// reviewed, so a family reclassification can never merge unseen.
    pub fn resolution_report(&self) -> Vec<ResolutionRow> {
        let mut rows = Vec::new();
        for (id, entry) in &self.providers {
            let npm = entry.get("npm").and_then(Value::as_str).map(str::to_owned);
            match self.resolved_family.get(id) {
                Some((family, source)) => rows.push(ResolutionRow {
                    provider_id: id.clone(),
                    npm,
                    resolved_family: serde_json::to_value(family)
                        .ok()
                        .and_then(|v| v.as_str().map(str::to_owned)),
                    resolution_source: source.tag().to_string(),
                    pending_reason: String::new(),
                }),
                None => {
                    let reason = self
                        .pending
                        .iter()
                        .find(|p| p.id == *id)
                        .map(|p| p.reason.clone())
                        .unwrap_or_default();
                    rows.push(ResolutionRow {
                        provider_id: id.clone(),
                        npm,
                        resolved_family: None,
                        resolution_source: "pending".to_string(),
                        pending_reason: reason,
                    });
                }
            }
        }
        rows.sort_by(|a, b| a.provider_id.cmp(&b.provider_id));
        rows
    }

    /// Whether fusiform explicitly reports this model as withdrawn/retired.
    pub fn is_withdrawn(&self, provider_id: &str, model_id: &str) -> bool {
        self.withdrawn_models
            .contains(&(provider_id.to_string(), model_id.to_string()))
    }

    /// Resolve a `(ProviderSpec, ModelSpec)` serving pair. The family is resolved per
    /// `(provider, model)`: the provider default, overridden by an explicit
    /// `wire_family` on the model (e.g. OpenAI Chat vs Responses per model). The
    /// resolved family rides on the returned `ProviderSpec`, so `resolve_frozen`
    /// freezes the per-model renderer. `None` if the provider is absent/pending or the
    /// model id is unknown.
    pub fn model(&self, provider_id: &str, model_id: &str) -> Option<(ProviderSpec, ModelSpec)> {
        self.map_model_pair(provider_id, model_id, RawPolicy::Preserve)
    }

    /// Resolve the same typed serving pair as [`Self::model`] without copying the catalog's
    /// ingestion-only passthrough fields. Runtime provider construction never reads `raw`, and a
    /// provider's raw object contains its complete model map, so omitting it keeps lookup work
    /// proportional to the selected model rather than the provider's catalog breadth.
    pub fn model_for_serving(
        &self,
        provider_id: &str,
        model_id: &str,
    ) -> Option<(ProviderSpec, ModelSpec)> {
        self.map_model_pair(provider_id, model_id, RawPolicy::Skip)
    }

    fn map_model_pair(
        &self,
        provider_id: &str,
        model_id: &str,
        raw_policy: RawPolicy,
    ) -> Option<(ProviderSpec, ModelSpec)> {
        let (provider_family, _source) = *self.resolved_family.get(provider_id)?;
        let entry = self.providers.get(provider_id)?;
        let model_entry = entry.get("models").and_then(|m| m.get(model_id))?;

        // The snapshot's per-model provider object carries the same package/shape override
        // that Fusiform publishes as wire facts. An unsupported shape cannot borrow a default.
        let model_family = if let Some(family) =
            model_entry.get("wire_family").and_then(parse_family)
        {
            family
        } else if let Some(overrides) = model_entry.get("provider").filter(|value| !value.is_null())
        {
            let overrides = overrides.as_object()?;
            let npm = overrides.get("npm").filter(|value| !value.is_null());
            let shape = overrides.get("shape").filter(|value| !value.is_null());
            if npm.is_some() || shape.is_some() {
                let npm = match npm {
                    Some(value) => value.as_str()?,
                    None => entry.get("npm")?.as_str()?,
                };
                let shape = match shape {
                    Some(value) => Some(value.as_str()?),
                    None => None,
                };
                family::wire_family(npm, shape)?
            } else {
                provider_family
            }
        } else {
            provider_family
        };

        let mut provider_spec = map_provider(provider_id, entry, model_family, raw_policy);
        // The serving spec speaks for THIS model: carry the per-model family.
        provider_spec.wire_family = model_family;
        // Model-level endpoint facts take precedence over the provider's root.
        if let Some(base_url) = model_entry.get("base_url").and_then(Value::as_str) {
            provider_spec.base_url = base_url.to_string();
        } else if let Some(api) = model_entry
            .get("provider")
            .and_then(|provider| provider.get("api"))
            .and_then(Value::as_str)
        {
            provider_spec.base_url = routing::renderer_base_url(model_family, api);
        }
        Some((provider_spec, map_model(model_id, model_entry, raw_policy)))
    }
}

fn json_value_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Parse a `WireFamilyId` from a JSON string value (overlay-supplied family override).
fn parse_family(v: &Value) -> Option<WireFamilyId> {
    serde_json::from_value(v.clone()).ok()
}

/// Catalog-time provider quirks layered on top of the family's defaults. Keep this table small
/// and explicit: the OpenAI-Chat family is shared by the OpenAI-compatible long tail, so a
/// provider-only request or usage fix belongs here, never in the family default.
#[derive(Debug, Clone, Copy)]
struct ProviderQuirkOverride {
    provider_id: &'static str,
    family: WireFamilyId,
    token_limit_field: Option<TokenLimitField>,
    usage: Option<UsagePolicy>,
    tool_call_content_empty_string: bool,
}

const PROVIDER_QUIRK_OVERRIDES: &[ProviderQuirkOverride] = &[
    // OpenAI's current Chat Completions docs expose `max_completion_tokens` as the replacement
    // for deprecated `max_tokens`; keep the family default at `max_tokens` for the broader
    // OpenAI-compatible ecosystem and override only the real `openai` provider here.
    ProviderQuirkOverride {
        provider_id: "openai",
        family: WireFamilyId::OpenAiChat,
        token_limit_field: Some(TokenLimitField::MaxCompletionTokens),
        usage: None,
        tool_call_content_empty_string: false,
    },
    ProviderQuirkOverride {
        provider_id: "xai",
        family: WireFamilyId::OpenAiChat,
        token_limit_field: None,
        usage: Some(UsagePolicy::OpenAiUsageReasoningSeparate),
        tool_call_content_empty_string: false,
    },
    // Ollama Cloud (`https://ollama.com/v1`) speaks OpenAI Chat but 400s on `content: null`
    // in an assistant turn: "invalid message content type: <nil>". Opt in to `content: ""`.
    // NOT only tool-call turns -- a step that assembles neither text nor a call leaves a
    // durable empty assistant turn, and replaying it into the next episode was refused the
    // same way (5 of 5 such continuations failed in production before the render was fixed).
    ProviderQuirkOverride {
        provider_id: "ollama-cloud",
        family: WireFamilyId::OpenAiChat,
        token_limit_field: None,
        usage: None,
        tool_call_content_empty_string: true,
    },
];

/// Resolve a provider's typed quirks from the family defaults plus any catalog-time provider
/// override. `resolve_frozen` copies the selected policies into `FrozenRenderConfig` once at run
/// start, so resumed runs keep the behavior they already froze while new runs receive their
/// provider's current policies.
fn provider_quirks(provider_id: &str, family: WireFamilyId, base_url: &str) -> Quirks {
    let mut quirks = broca_provider::families::default_quirks(family);
    if let Some(override_) = PROVIDER_QUIRK_OVERRIDES
        .iter()
        .find(|override_| override_.provider_id == provider_id && override_.family == family)
    {
        if let Some(token_limit_field) = override_.token_limit_field {
            quirks.token_limit_field = token_limit_field;
        }
        if let Some(usage) = override_.usage {
            quirks.usage = usage;
        }
        if override_.tool_call_content_empty_string {
            quirks.tool_call_content_empty_string = true;
        }
    }
    if family == WireFamilyId::OpenAiChat {
        quirks.reasoning_echo = reasoning_echo_for_route(provider_id, base_url);
    }
    quirks
}

fn reasoning_echo_for_route(
    provider_id: &str,
    base_url: &str,
) -> Option<broca_provider::policy::ReasoningEchoPolicy> {
    use broca_provider::policy::ReasoningEchoPolicy;
    if is_deepseek_route(provider_id, base_url) {
        return Some(ReasoningEchoPolicy::DEEPSEEK);
    }
    let host = url_host(base_url);
    // Kimi: "retain the reasoning_content from every historical assistant message".
    // https://platform.kimi.ai/docs/api/chat (thinking.keep); no last-user cutoff.
    // Its applicability to api.kimi.ai/coding/v1 is inferred from the platform API rule;
    // the renamed-tool-id live probe validates acceptance there, not retention inside the model.
    if matches!(
        provider_id,
        "kimi-code-plan-global" | "kimi-code-plan-cn" | "moonshotai" | "moonshotai-cn"
    ) || host.as_deref().is_some_and(|h| {
        matches!(
            h,
            "api.kimi.ai" | "api.kimi.com" | "api.moonshot.ai" | "api.moonshot.cn"
        )
    }) {
        return Some(ReasoningEchoPolicy::KIMI);
    }
    if provider_id == "openrouter" || host.as_deref() == Some("openrouter.ai") {
        return Some(ReasoningEchoPolicy::OPENROUTER);
    }
    None
}

/// Whether a route is served by DeepSeek's own API: the `deepseek` provider, or any provider
/// whose base URL host is `deepseek.com` or one of its subdomains (a renamed or user-added
/// provider pointing at `https://api.deepseek.com`). DeepSeek's thinking mode needs every
/// earlier assistant turn's reasoning sent back as `reasoning_content`; without it the model
/// loses its earlier reasoning at each turn boundary, and a request whose tool-call ids DeepSeek
/// no longer recognises is refused 400. The OpenAI-Chat renderer adds that field when
/// the scalar echo policy is set. Other providers that merely serve a DeepSeek model
/// (ollama-cloud, OpenRouter, ...) are not matched: their APIs are not DeepSeek's.
fn is_deepseek_route(provider_id: &str, base_url: &str) -> bool {
    provider_id == "deepseek"
        || url_host(base_url)
            .is_some_and(|host| host == "deepseek.com" || host.ends_with(".deepseek.com"))
}

/// The lower-cased host of an `http(s)://host[:port][/path]` URL, or `None` when the string has
/// no `scheme://` prefix or an empty host. Userinfo (`user@`) is skipped, so a URL such as
/// `https://deepseek.com@evil.example` yields `evil.example`, never `deepseek.com`.
fn url_host(url: &str) -> Option<String> {
    let (_scheme, rest) = url.trim().split_once("://")?;
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let host_port = authority.rsplit('@').next().unwrap_or("");
    let host = host_port
        .split(':')
        .next()
        .unwrap_or("")
        .trim_end_matches('.');
    (!host.is_empty()).then(|| host.to_ascii_lowercase())
}

#[derive(Clone, Copy)]
enum RawPolicy {
    Preserve,
    Skip,
}

impl RawPolicy {
    fn map(self, entry: &Value) -> Value {
        match self {
            Self::Preserve => entry.clone(),
            Self::Skip => Value::Null,
        }
    }
}

/// Map a merged provider object into a typed `ProviderSpec` with an ALREADY-RESOLVED
/// wire family (the caller decides provider-default vs per-model).
fn map_provider(
    provider_id: &str,
    entry: &Value,
    family: WireFamilyId,
    raw_policy: RawPolicy,
) -> ProviderSpec {
    let base_url = entry
        .get("base_url")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            entry
                .get("api")
                .and_then(Value::as_str)
                .map(|api| routing::renderer_base_url(family, api))
        })
        .unwrap_or_default();
    ProviderSpec {
        id: provider_id.to_string(),
        wire_family: family,
        auth_kind: default_auth_kind(family),
        static_headers: Default::default(),
        static_options: provider_static_options(provider_id, family),
        // Quirks start from the family's correct request shape, then layer any provider-specific
        // override from the catalog bridge. This keeps the OpenAI provider fix out of the shared
        // OpenAI-Chat family default while leaving the hand-built family helpers unchanged.
        quirks: provider_quirks(provider_id, family, &base_url),
        // Prefer an explicit native hold, then the published API. Empty is metadata-only;
        // migrated live admission requires the row's wire.api rather than a family default.
        base_url,
        raw: raw_policy.map(entry),
    }
}

/// OpenAI, xAI and Meta request the encrypted reasoning items their stateless replay needs.
/// These options freeze with the request.
fn provider_static_options(provider_id: &str, family: WireFamilyId) -> Value {
    // Meta's Responses protocol, "Reasoning items in multi-turn input", documents the same
    // stateless encrypted replay contract: https://dev.meta.ai/docs/protocols/responses#reasoning-items
    if family == WireFamilyId::OpenAiResponses && matches!(provider_id, "openai" | "xai" | "meta") {
        serde_json::json!({"store": false, "include": ["reasoning.encrypted_content"]})
    } else {
        Value::Null
    }
}

/// Map a merged model object into a typed `ModelSpec`. Known fields map through the same path
/// whether the caller needs the ingestion-only passthrough or a serving-only view.
fn map_model(model_id: &str, entry: &Value, raw_policy: RawPolicy) -> ModelSpec {
    // A missing key, JSON false, and JSON null are three different producer statements.
    // This mapper must not decide among them: freeze is the first point that knows whether
    // the run would use the flag. `as_bool().unwrap_or(false)` collapsed null into false
    // and silently stripped reasoning (and would strip any later-gated flag the same way).
    ModelSpec {
        id: model_id.to_string(),
        display_name: entry
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        family: entry
            .get("family")
            .and_then(Value::as_str)
            .map(str::to_owned),
        capabilities: Capabilities {
            attachment: capability_flag(entry, "attachment"),
            reasoning: capability_flag(entry, "reasoning"),
            temperature: capability_flag(entry, "temperature"),
            tool_call: capability_flag(entry, "tool_call"),
            interleaved: capability_flag(entry, "interleaved"),
            // The models.dev spelling of fusiform's `capability.structured_output`, read like
            // the other snapshot capability keys.
            structured_output: capability_flag(entry, "structured_output"),
        },
        limits: map_limits(entry.get("limit")),
        cost: map_cost(entry.get("cost")),
        // models.dev `reasoning_options`, the snapshot spelling of fusiform's
        // `capability.reasoning_options`. A value that is neither null nor an array is read as
        // unstated rather than failing the whole snapshot.
        reasoning_options: entry
            .get("reasoning_options")
            .and_then(ReasoningOptions::from_value)
            .unwrap_or_default(),
        release_date: entry
            .get("release_date")
            .and_then(Value::as_str)
            .map(str::to_owned),
        status: entry
            .get("status")
            .and_then(Value::as_str)
            .map(str::to_owned),
        overlay: Overlay::default(),
        raw: raw_policy.map(entry),
    }
}

/// Read one capability key without deciding what the run should do with it.
///
/// - omitted → [`CapabilityFlag::Absent`] (freeze still defaults this to not capable)
/// - `true` / `false` → [`CapabilityFlag::Known`]
/// - `null` → [`CapabilityFlag::Unknown`] (freeze fails closed if the run needs it)
/// - any other JSON type → [`CapabilityFlag::Absent`] (models.dev publishes
///   `interleaved` as an object on some rows; that is not a boolean claim)
fn capability_flag(entry: &Value, key: &str) -> CapabilityFlag {
    match entry.get(key) {
        None => CapabilityFlag::Absent,
        Some(Value::Null) => CapabilityFlag::Unknown,
        Some(Value::Bool(value)) => CapabilityFlag::Known(*value),
        Some(_) => CapabilityFlag::Absent,
    }
}

fn map_limits(limit: Option<&Value>) -> Limits {
    Limits {
        context: limit.and_then(|l| l.get("context")).and_then(Value::as_u64),
        max_input: limit.and_then(|l| l.get("input")).and_then(Value::as_u64),
        max_output: limit.and_then(|l| l.get("output")).and_then(Value::as_u64),
    }
}

/// Map a models.dev `cost` object into a `CostSchedule`. Base rates + reasoning/audio
/// rates + context tiers are all preserved (a missing rate is `None`, NOT zero).
fn map_cost(cost: Option<&Value>) -> CostSchedule {
    let Some(cost) = cost else {
        return CostSchedule::default();
    };
    let f = |k: &str| cost.get(k).and_then(Value::as_f64);
    CostSchedule {
        input: f("input"),
        output: f("output"),
        cache_read: f("cache_read"),
        cache_write: f("cache_write"),
        reasoning: f("reasoning"),
        input_audio: f("input_audio"),
        output_audio: f("output_audio"),
        tiers: map_tiers(cost.get("tiers")),
    }
}

/// Map the `cost.tiers` array (context-size pricing tiers), sorted ascending by the
/// context threshold so a lookup can scan in order.
fn map_tiers(tiers: Option<&Value>) -> Vec<CostTier> {
    let Some(arr) = tiers.and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut out: Vec<CostTier> = arr
        .iter()
        .filter_map(|t| {
            // A tier's threshold lives at tier.tier.size with tier.tier.type == "context".
            let min_context = t
                .get("tier")
                .and_then(|ti| ti.get("size"))
                .and_then(Value::as_u64)?;
            let f = |k: &str| t.get(k).and_then(Value::as_f64);
            Some(CostTier {
                min_context,
                input: f("input"),
                output: f("output"),
                cache_read: f("cache_read"),
                cache_write: f("cache_write"),
            })
        })
        .collect();
    out.sort_by_key(|t| t.min_context);
    out
}

/// A sensible default auth KIND per wire family (overlay-correctable). KIND only —
/// never a credential; the credential is attached at runtime, after render.
fn default_auth_kind(family: WireFamilyId) -> AuthKind {
    match family {
        WireFamilyId::BedrockConverse => AuthKind::SigV4 {
            region: String::new(),
        },
        // Gemini's `generateContent` authenticates with an `x-goog-api-key` header, not
        // a Bearer token — the catalog must reflect that so a resolved Gemini provider
        // reaches a real key into the right header.
        WireFamilyId::Gemini => AuthKind::ApiKeyHeader {
            header: "x-goog-api-key".to_string(),
        },
        _ => AuthKind::Bearer,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_provider::policy::TokenLimitField;
    use serde_json::json;
    use std::sync::OnceLock;

    fn catalog() -> &'static Catalog {
        static CATALOG: OnceLock<Catalog> = OnceLock::new();
        CATALOG.get_or_init(|| {
            Catalog::from_json_str(
                include_str!("../data/models.dev.json"),
                include_str!("../data/overlay.json"),
            )
            .expect("legacy fixture catalog is well formed")
        })
    }

    fn base() -> Value {
        json!({
            "anthropic": {
                "id": "anthropic", "name": "Anthropic", "npm": "@ai-sdk/anthropic",
                "models": {
                    "claude-x": {
                        "id": "claude-x", "name": "Claude X", "family": "claude",
                        "reasoning": true, "tool_call": true, "temperature": true,
                        "limit": { "input": 190000, "context": 200000, "output": 64000 },
                        "cost": {
                            "input": 3, "output": 15, "cache_read": 0.3, "cache_write": 3.75,
                            "tiers": [
                                { "input": 6, "output": 22.5, "tier": { "size": 200000, "type": "context" } }
                            ]
                        },
                        "modalities": { "input": ["text"], "output": ["text"] }
                    }
                }
            },
            "deepseek": {
                "id": "deepseek", "name": "DeepSeek", "api": "https://api.deepseek.com",
                "npm": "@ai-sdk/openai-compatible",
                "models": {
                    "deepseek-chat": {
                        "id": "deepseek-chat", "name": "DeepSeek Chat", "tool_call": true,
                        "limit": { "context": 64000, "output": 8000 },
                        "cost": { "input": 0.27, "output": 1.1 }
                    }
                }
            },
            "mistral": {
                "id": "mistral", "name": "Mistral", "api": "https://api.mistral.ai",
                "npm": "@ai-sdk/mistral",
                "models": { "mistral-large": { "id": "mistral-large", "name": "Mistral Large" } }
            }
        })
    }

    #[test]
    fn native_provider_resolves_and_preserves_raw() {
        let c = Catalog::from_json_str(&base().to_string(), "{}").unwrap();
        let p = c.provider("anthropic").unwrap();
        assert_eq!(p.wire_family, WireFamilyId::AnthropicMessages);
        assert_eq!(
            p.raw.get("npm").and_then(|v| v.as_str()),
            Some("@ai-sdk/anthropic")
        );
    }

    #[test]
    fn published_package_resolves_metadata_without_an_overlay_pin() {
        let mut facts = base();
        let c = Catalog::from_json_str(&facts.to_string(), "{}").unwrap();
        let provider = c
            .provider("deepseek")
            .expect("published compatible package and API resolve");
        assert_eq!(provider.wire_family, WireFamilyId::OpenAiChat);
        assert_eq!(provider.base_url, "https://api.deepseek.com");
        facts["deepseek"].as_object_mut().unwrap().remove("api");
        let c = Catalog::from_json_str(&facts.to_string(), "{}").unwrap();
        assert!(
            c.provider("deepseek").unwrap().base_url.is_empty(),
            "metadata must not invent an endpoint; live admission requires wire.api"
        );
    }

    #[test]
    fn snapshot_model_provider_shape_is_consumed_without_guessing() {
        let mut base = base();
        base["deepseek"]["models"]["shape-test"] = json!({
            "provider":{"npm":"@ai-sdk/openai","api":"https://row.example/v1","shape":"completions"}
        });
        let catalog = Catalog::from_json_str(&base.to_string(), "{}").unwrap();
        let (provider, _) = catalog.model_for_serving("deepseek", "shape-test").unwrap();
        assert_eq!(provider.wire_family, WireFamilyId::OpenAiChat);
        assert_eq!(provider.base_url, "https://row.example/v1");
        base["deepseek"]["models"]["shape-test"]["provider"]
            .as_object_mut()
            .unwrap()
            .remove("npm");
        base["deepseek"]["models"]["shape-test"]["provider"]["shape"] = json!("responses");
        let catalog = Catalog::from_json_str(&base.to_string(), "{}").unwrap();
        assert_eq!(
            catalog
                .model_for_serving("deepseek", "shape-test")
                .unwrap()
                .0
                .wire_family,
            WireFamilyId::OpenAiResponses
        );
        base["deepseek"]["models"]["shape-test"]["provider"]["shape"] = json!("unsupported-shape");
        let catalog = Catalog::from_json_str(&base.to_string(), "{}").unwrap();
        assert!(catalog
            .model_for_serving("deepseek", "shape-test")
            .is_none());
    }

    #[test]
    fn overlay_pin_serves_openai_compatible_provider() {
        let overlay = json!({
            "patch": {
                "deepseek": {
                    "base_url": "https://api.deepseek.com",
                    "wire_family": "openai_chat"
                }
            }
        });
        let c = Catalog::from_json_str(&base().to_string(), &overlay.to_string()).unwrap();
        let p = c.provider("deepseek").unwrap();
        assert_eq!(p.wire_family, WireFamilyId::OpenAiChat);
        assert_eq!(p.base_url, "https://api.deepseek.com");
    }

    #[test]
    fn native_npm_without_table_entry_is_pending_not_served() {
        let c = Catalog::from_json_str(&base().to_string(), "{}").unwrap();
        // mistral has a native npm we haven't tabled → not serveable, listed pending.
        assert!(c.provider("mistral").is_none());
        assert!(c.pending_providers().iter().any(|p| p.id == "mistral"));
    }

    #[test]
    fn model_limits_map_distinct_input_context_and_output_values() {
        let c = Catalog::from_json_str(&base().to_string(), "{}").unwrap();
        let (_provider, model) = c.model("anthropic", "claude-x").unwrap();

        assert_eq!(model.limits.max_input, Some(190_000));
        assert_eq!(model.limits.context, Some(200_000));
        assert_eq!(model.limits.max_output, Some(64_000));
    }

    #[test]
    fn cost_schedule_maps_tiers_and_optional_rates() {
        let c = Catalog::from_json_str(&base().to_string(), "{}").unwrap();
        let (_p, m) = c.model("anthropic", "claude-x").unwrap();
        assert_eq!(m.cost.input, Some(3.0));
        assert_eq!(m.cost.cache_write, Some(3.75));
        assert_eq!(m.cost.reasoning, None); // not published ≠ free
        assert_eq!(m.cost.tiers.len(), 1);
        assert_eq!(m.cost.tiers[0].min_context, 200000);
        assert_eq!(m.cost.tiers[0].input, Some(6.0));
    }

    #[test]
    fn overlay_patch_deep_merges_and_can_override_family() {
        // Patch deepseek to the Responses family + correct its name.
        let overlay = json!({
            "patch": {
                "deepseek": {
                    "base_url": "https://api.deepseek.com",
                    "name": "DeepSeek (overlaid)",
                    "wire_family": "openai_responses"
                }
            }
        });
        let c = Catalog::from_json_str(&base().to_string(), &overlay.to_string()).unwrap();
        let p = c.provider("deepseek").unwrap();
        assert_eq!(p.wire_family, WireFamilyId::OpenAiResponses);
        assert_eq!(
            p.raw.get("name").and_then(|v| v.as_str()),
            Some("DeepSeek (overlaid)")
        );
        // The endpoint pin survives alongside the other patched fields.
        assert_eq!(p.base_url, "https://api.deepseek.com");
    }

    #[test]
    fn overlay_add_introduces_a_provider_and_is_flagged() {
        let overlay = json!({
            "add": {
                "localhost": {
                    "id": "localhost", "base_url": "http://localhost:8080/v1",
                    "wire_family": "openai_chat", "models": { "m": { "id": "m", "name": "M" } }
                }
            }
        });
        let c = Catalog::from_json_str(&base().to_string(), &overlay.to_string()).unwrap();
        assert_eq!(c.overlay_added(), &["localhost".to_string()]);
        let p = c.provider("localhost").unwrap();
        assert_eq!(p.wire_family, WireFamilyId::OpenAiChat);
    }

    /// The two retired ids must stay unservable and the v4 ids must serve. Until the
    /// 2026-09-18 snapshot, upstream still listed deepseek-chat/-reasoner and broca's overlay
    /// `remove` suppressed them; that snapshot withdrew both, so the suppression became dead
    /// weight and was dropped. This now witnesses the upstream state rather than the overlay,
    /// and fails the day a refresh resurrects a name the live API rejects with a 400.
    #[test]
    fn bundled_retired_deepseek_ids_are_suppressed_and_v4_still_serves() {
        let c = catalog();
        assert!(c.model_for_serving("deepseek", "deepseek-chat").is_none());
        assert!(c
            .model_for_serving("deepseek", "deepseek-reasoner")
            .is_none());
        assert!(c.model_for_serving("deepseek", "deepseek-v4-pro").is_some());
        assert!(c
            .model_for_serving("deepseek", "deepseek-v4-flash")
            .is_some());
    }

    #[test]
    fn overlay_remove_suppresses_a_retired_model() {
        // Non-vacuous: the model serves when the provider is pinned, and stops serving
        // only because of the suppression.
        let pin = json!({ "patch": { "deepseek": { "wire_family": "openai_chat" } } });
        let served = Catalog::from_json_str(&base().to_string(), &pin.to_string()).unwrap();
        assert!(served.model("deepseek", "deepseek-chat").is_some());

        let overlay = json!({
            "patch": { "deepseek": { "wire_family": "openai_chat" } },
            "remove": { "deepseek": ["deepseek-chat"] }
        });
        let c = Catalog::from_json_str(&base().to_string(), &overlay.to_string()).unwrap();
        assert!(c.model("deepseek", "deepseek-chat").is_none());
        assert!(c.model_for_serving("deepseek", "deepseek-chat").is_none());
        // The provider itself still serves — only the one model is gone.
        assert!(c.provider("deepseek").is_some());
    }

    #[test]
    fn overlay_remove_survives_a_patch_on_the_same_provider() {
        // Suppression applies after the merge, so patching the provider and removing one
        // of its models compose.
        let overlay = json!({
            "patch": { "deepseek": { "base_url": "https://api.deepseek.com/v9", "wire_family": "openai_chat" } },
            "remove": { "deepseek": ["deepseek-chat"] }
        });
        let c = Catalog::from_json_str(&base().to_string(), &overlay.to_string()).unwrap();
        assert_eq!(
            c.provider("deepseek").unwrap().base_url,
            "https://api.deepseek.com/v9"
        );
        assert!(c.model("deepseek", "deepseek-chat").is_none());
    }

    #[test]
    fn overlay_remove_of_an_unknown_provider_fails_loud() {
        let overlay = json!({ "remove": { "nope": ["m"] } });
        let err = Catalog::from_json_str(&base().to_string(), &overlay.to_string())
            .err()
            .expect("removing an unknown provider must fail");
        assert_eq!(err, CatalogError::RemoveTargetMissing("nope".to_string()));
    }

    #[test]
    fn overlay_remove_of_an_unknown_model_fails_loud() {
        // A suppression that no longer matches anything is dead weight, and silently
        // ignoring it would hide a typo that leaves a dead model served.
        let overlay = json!({ "remove": { "deepseek": ["deepseek-ghost"] } });
        let err = Catalog::from_json_str(&base().to_string(), &overlay.to_string())
            .err()
            .expect("removing an unknown model must fail");
        assert_eq!(
            err,
            CatalogError::RemoveModelMissing {
                provider: "deepseek".to_string(),
                model: "deepseek-ghost".to_string(),
            }
        );
    }

    #[test]
    fn overlay_remove_rejects_string_instead_of_model_array() {
        let overlay = json!({ "remove": { "deepseek": "deepseek-chat" } });
        let error = match Catalog::from_json_str(&base().to_string(), &overlay.to_string()) {
            Err(error) => error,
            Ok(catalog) => panic!(
                "malformed remove entry was accepted; deepseek-chat remains resolvable: {}",
                catalog.model("deepseek", "deepseek-chat").is_some()
            ),
        };
        assert_eq!(
            error,
            CatalogError::RemoveShapeInvalid {
                provider: Some("deepseek".to_string()),
                found: "string".to_string(),
            }
        );
    }

    #[test]
    fn overlay_remove_rejects_non_string_model_id() {
        let overlay = json!({ "remove": { "deepseek": ["deepseek-chat", 42] } });
        let error = Catalog::from_json_str(&base().to_string(), &overlay.to_string())
            .err()
            .expect("a remove array containing a non-string must fail");
        assert_eq!(
            error,
            CatalogError::RemoveShapeInvalid {
                provider: Some("deepseek".to_string()),
                found: "number at array index 1".to_string(),
            }
        );
    }

    #[test]
    fn overlay_remove_rejects_object_instead_of_model_array() {
        let overlay = json!({ "remove": { "deepseek": {} } });
        let error = Catalog::from_json_str(&base().to_string(), &overlay.to_string())
            .err()
            .expect("a remove provider entry must be an array");
        assert_eq!(
            error,
            CatalogError::RemoveShapeInvalid {
                provider: Some("deepseek".to_string()),
                found: "object".to_string(),
            }
        );
    }

    #[test]
    fn overlay_remove_rejects_non_object_section() {
        let overlay = json!({ "remove": ["deepseek"] });
        let error = Catalog::from_json_str(&base().to_string(), &overlay.to_string())
            .err()
            .expect("the top-level remove section must be an object");
        assert_eq!(
            error,
            CatalogError::RemoveShapeInvalid {
                provider: None,
                found: "array".to_string(),
            }
        );
    }

    #[test]
    fn per_model_family_override() {
        // A model can override the provider default family.
        let overlay = json!({
            "add": {
                "openai-ish": {
                    "id": "openai-ish", "api": "https://x/v1", "wire_family": "openai_chat",
                    "models": {
                        "chat-model": { "id": "chat-model", "name": "Chat" },
                        "resp-model": { "id": "resp-model", "name": "Resp", "wire_family": "openai_responses" }
                    }
                }
            }
        });
        let c = Catalog::from_json_str(&base().to_string(), &overlay.to_string()).unwrap();
        let (chat_p, _) = c.model("openai-ish", "chat-model").unwrap();
        let (resp_p, _) = c.model("openai-ish", "resp-model").unwrap();
        assert_eq!(chat_p.wire_family, WireFamilyId::OpenAiChat);
        assert_eq!(resp_p.wire_family, WireFamilyId::OpenAiResponses);
    }

    #[test]
    fn per_model_base_url_override() {
        let overlay = json!({
            "add": {
                "openai-ish": {
                    "id": "openai-ish", "api": "https://x/v1", "wire_family": "openai_chat",
                    "models": {
                        "chat-model": {
                            "id": "chat-model", "name": "Chat",
                            "base_url": "https://x/chat/v1"
                        }
                    }
                }
            }
        });
        let catalog = Catalog::from_json_str(&base().to_string(), &overlay.to_string()).unwrap();
        let (provider, _) = catalog.model("openai-ish", "chat-model").unwrap();
        assert_eq!(provider.base_url, "https://x/chat/v1");
    }

    #[test]
    fn overlay_patch_missing_target_errors() {
        let overlay = json!({ "patch": { "nonexistent": { "name": "x" } } });
        assert_eq!(
            Catalog::from_json_str(&base().to_string(), &overlay.to_string()).err(),
            Some(CatalogError::PatchTargetMissing("nonexistent".to_string()))
        );
    }

    #[test]
    fn overlay_add_existing_target_errors() {
        let overlay = json!({ "add": { "anthropic": { "id": "anthropic" } } });
        assert_eq!(
            Catalog::from_json_str(&base().to_string(), &overlay.to_string()).err(),
            Some(CatalogError::AddTargetExists("anthropic".to_string()))
        );
    }

    #[test]
    fn unknown_ids_return_none() {
        let c = Catalog::from_json_str(&base().to_string(), "{}").unwrap();
        assert!(c.provider("nope").is_none());
        assert!(c.model("anthropic", "nope").is_none());
        assert!(c.model("nope", "x").is_none());
    }

    /// Every key the vendored snapshot actually contains must be CLASSIFIED — either
    /// mapped to a typed field, or explicitly listed as intentionally-not-mapped. A
    /// new models.dev field then fails THIS test until someone decides whether it can
    /// change request bytes (in which case it must be promoted to a typed field — the
    /// renderer never reads `raw`). This is the guard against a byte-affecting field
    /// silently living only in `raw`.
    #[cfg(feature = "bundled-catalog")]
    #[test]
    fn every_snapshot_key_is_classified() {
        // Keys mapped into typed ProviderSpec fields.
        const PROVIDER_MAPPED: &[&str] = &["id", "models", "api", "npm"];
        // Keys intentionally NOT mapped — none can change request BODY bytes:
        //  - doc: a documentation URL;
        //  - env: the NAMES of auth env vars (auth is attached after render, off the
        //    hash domain; the auth KIND comes from the family, not this);
        //  - name: provider display name;
        const PROVIDER_IGNORED: &[&str] = &["doc", "env", "name"];

        // Keys mapped into typed ModelSpec fields.
        const MODEL_MAPPED: &[&str] = &[
            "attachment",
            "cost",
            "family",
            "id",
            "interleaved",
            "limit",
            "name",
            "reasoning",
            // Typed as `ModelSpec::reasoning_options`: a caller's reasoning variant is resolved
            // against it, so it is byte-affecting whenever a send carries a variant.
            "reasoning_options",
            "release_date",
            "status",
            "structured_output",
            "temperature",
            "tool_call",
            // Per-model package, endpoint and shape become bounded serving fields.
            "provider",
        ];
        // Keys intentionally NOT mapped. None is consumed by the renderer today:
        //  - description: human-readable model prose for display only; never read by any
        //    renderer and not byte-affecting;
        //  - knowledge/open_weights: metadata, non-byte;
        //  - last_updated: PROSE, not a change indicator. The upstream was observed moving
        //    a model's wire family while this field stayed put, so it cannot back change
        //    detection, era boundaries, or staleness;
        //  - experimental: NOT metadata — it carries literal request BODY params and
        //    HEADERS for tier variants (38 models, 42 modes; e.g. gpt-5.4 fast =
        //    `{"body":{"service_tier":"priority"}}`). It is ignored because those are
        //    RENDERER decisions and the renderer is ours: a caller's `service_tier` is
        //    rendered by broca's own family table (broca-provider's `service_tier`), never
        //    from this field. Mapping it would let an upstream edit change our request bytes;
        //  - modalities: capability metadata — NOT byte-affecting until multimodal
        //    rendering exists; promote it then;
        const MODEL_IGNORED: &[&str] = &[
            "description",
            "experimental",
            "knowledge",
            "last_updated",
            "modalities",
            "open_weights",
        ];

        let snapshot: Value =
            serde_json::from_str(include_str!("../data/models.dev.json")).unwrap();
        let providers = snapshot.as_object().expect("snapshot is an object");

        let mut unclassified_provider = std::collections::BTreeSet::new();
        let mut unclassified_model = std::collections::BTreeSet::new();
        for (_pid, prov) in providers {
            for key in prov.as_object().into_iter().flat_map(|o| o.keys()) {
                if !PROVIDER_MAPPED.contains(&key.as_str())
                    && !PROVIDER_IGNORED.contains(&key.as_str())
                {
                    unclassified_provider.insert(key.clone());
                }
            }
            let models = prov.get("models").and_then(Value::as_object);
            for (_mid, model) in models.into_iter().flatten() {
                for key in model.as_object().into_iter().flat_map(|o| o.keys()) {
                    if !MODEL_MAPPED.contains(&key.as_str())
                        && !MODEL_IGNORED.contains(&key.as_str())
                    {
                        unclassified_model.insert(key.clone());
                    }
                }
            }
        }

        assert!(
            unclassified_provider.is_empty() && unclassified_model.is_empty(),
            "unclassified catalog keys — classify as typed-mapped or explicitly-ignored \
             (a byte-affecting field MUST be promoted to a typed field; the renderer \
             never reads raw). provider: {unclassified_provider:?}, model: {unclassified_model:?}"
        );
    }

    /// Published package and endpoint facts replace duplicate overlay pins.
    #[cfg(feature = "bundled-catalog")]
    #[test]
    fn fact_served_providers_resolve_to_openai_chat_with_base_url() {
        // (provider, a real snapshot model id, expected base_url after overlay).
        let cases = [
            ("deepseek", "deepseek-v4-flash", "https://api.deepseek.com"),
            // OpenRouter's root comes directly from its published API field.
            (
                "openrouter",
                "aion-labs/aion-2.0",
                "https://openrouter.ai/api/v1",
            ),
        ];
        for (provider, model_id, want_base) in cases {
            let (spec, _model) = catalog()
                .model(provider, model_id)
                .unwrap_or_else(|| panic!("{provider}/{model_id} must resolve via the overlay"));
            assert_eq!(
                spec.wire_family,
                WireFamilyId::OpenAiChat,
                "{provider} must resolve to OpenAiChat (overlay), not Pending"
            );
            assert_eq!(
                spec.base_url, want_base,
                "{provider} base_url must be the pinned overlay host the endpoint POSTs to"
            );
        }
    }

    #[cfg(feature = "bundled-catalog")]
    #[test]
    fn opencode_go_models_use_fusiform_pi_protocol_and_endpoint() {
        let cache = crate::CatalogCache::empty();
        cache
            .install_for_test(
                serde_json::from_str(include_str!("../tests/fixtures/fusiform_opencode_go.json"))
                    .unwrap(),
                crate::CatalogSource::Fusiform,
            )
            .unwrap();
        let snapshot = cache.snapshot().unwrap();
        let cases = [
            (
                "gpt-6-luna",
                WireFamilyId::OpenAiResponses,
                "https://opencode.ai/inference/go/v1",
            ),
            (
                "gemini-4-argon-contributor",
                WireFamilyId::Gemini,
                "https://opencode.ai/inference/go/v1",
            ),
            (
                "claude-haiku-5-5",
                WireFamilyId::AnthropicMessages,
                "https://opencode.ai/inference/go",
            ),
            (
                "gpt-5.6-luna",
                WireFamilyId::OpenAiResponses,
                "https://opencode.ai/inference/go/v1",
            ),
            (
                "grok-4.7",
                WireFamilyId::OpenAiResponses,
                "https://opencode.ai/inference/go/v1",
            ),
            (
                "muse-spark-1.2-contributor",
                WireFamilyId::OpenAiResponses,
                "https://opencode.ai/inference/go/v1",
            ),
            (
                "muse-spark-1.3-contributor",
                WireFamilyId::OpenAiResponses,
                "https://opencode.ai/inference/go/v1",
            ),
            (
                "minimax-m3",
                WireFamilyId::AnthropicMessages,
                "https://opencode.ai/inference/go",
            ),
            (
                "qwen3.7-plus",
                WireFamilyId::AnthropicMessages,
                "https://opencode.ai/inference/go",
            ),
            (
                "qwen3.8-flash",
                WireFamilyId::AnthropicMessages,
                "https://opencode.ai/inference/go",
            ),
            (
                "qwen3.8-max",
                WireFamilyId::AnthropicMessages,
                "https://opencode.ai/inference/go",
            ),
            (
                "deepseek-v4.1-flash",
                WireFamilyId::OpenAiChat,
                "https://opencode.ai/inference/go/v1",
            ),
            (
                "kimi-k2.7-code",
                WireFamilyId::OpenAiChat,
                "https://opencode.ai/inference/go/v1",
            ),
        ];
        for (model_id, family, base_url) in cases {
            let route = snapshot
                .resolve_route("opencode-go", model_id)
                .unwrap_or_else(|error| panic!("{error}"));
            assert_eq!(route.0, family, "{model_id} protocol fact");
            assert_eq!(route.1, base_url, "{model_id} endpoint fact");
        }
    }

    #[cfg(feature = "bundled-catalog")]
    #[test]
    fn meta_muse_resolves_to_first_party_openai_responses() {
        let (spec, _model) = catalog()
            .model("meta", "muse-spark-1.1")
            .expect("the bundled Meta Muse model must resolve");
        assert_eq!(spec.wire_family, WireFamilyId::OpenAiResponses);
        assert_eq!(spec.base_url, "https://api.meta.ai/v1");
    }

    #[cfg(feature = "bundled-catalog")]
    #[test]
    fn serving_model_matches_full_typed_mapping_and_frozen_config() {
        use broca_provider::families::{renderer_version_for, resolve_frozen};

        let (mut full_provider, mut full_model) = catalog()
            .model("openrouter", "openai/gpt-4.1")
            .expect("overlaid openrouter/openai/gpt-4.1 must resolve from the bundled snapshot");
        let (serving_provider, serving_model) = catalog()
            .model_for_serving("openrouter", "openai/gpt-4.1")
            .expect("overlaid openrouter/openai/gpt-4.1 must resolve for serving");

        let full_frozen = resolve_frozen(
            &full_provider,
            &full_model,
            2048,
            renderer_version_for(full_provider.wire_family),
        )
        .expect("bundled catalog models publish known capability flags");
        let serving_frozen = resolve_frozen(
            &serving_provider,
            &serving_model,
            2048,
            renderer_version_for(serving_provider.wire_family),
        )
        .expect("bundled catalog models publish known capability flags");
        assert_eq!(serving_frozen, full_frozen);

        full_provider.raw = Value::Null;
        full_model.raw = Value::Null;
        assert_eq!(serving_provider, full_provider);
        assert_eq!(serving_model, full_model);
    }

    #[cfg(feature = "bundled-catalog")]
    #[test]
    fn serving_model_omits_ingestion_raw() {
        let (full_provider, full_model) = catalog()
            .model("openrouter", "openai/gpt-4.1")
            .expect("overlaid openrouter/openai/gpt-4.1 must resolve from the bundled snapshot");
        let (serving_provider, serving_model) = catalog()
            .model_for_serving("openrouter", "openai/gpt-4.1")
            .expect("overlaid openrouter/openai/gpt-4.1 must resolve for serving");

        assert!(!full_provider.raw.is_null());
        assert!(!full_model.raw.is_null());
        assert!(serving_provider.raw.is_null());
        assert!(serving_model.raw.is_null());
    }

    #[cfg(feature = "bundled-catalog")]
    fn published_chat_shape_fixture() -> Catalog {
        Catalog::from_json_str(&json!({
            "openai":{"npm":"@ai-sdk/openai","api":"https://api.openai.com/v1","models":{
                "chat-fixture":{"reasoning":false,"provider":{"shape":"completions"}},
                "second-chat-fixture":{"reasoning":false,"provider":{"shape":"completions"}}
            }},
            "deepseek":{"npm":"@ai-sdk/openai-compatible","api":"https://api.deepseek.com","models":{"compatible-fixture":{"reasoning":false}}}
        }).to_string(), "{}").unwrap()
    }

    /// Explicit published Chat shapes keep the native token-field quirk without
    /// inferring a model's protocol from its name or reasoning capability.
    #[cfg(feature = "bundled-catalog")]
    #[test]
    fn published_chat_shapes_keep_the_native_token_field_quirk_scoped() {
        let catalog = published_chat_shape_fixture();
        for model_id in ["chat-fixture", "second-chat-fixture"] {
            let (spec, _model) = catalog.model("openai", model_id).unwrap_or_else(|| {
                panic!("openai/{model_id} must resolve from its published shape")
            });
            assert_eq!(spec.wire_family, WireFamilyId::OpenAiChat);
            assert_eq!(
                spec.quirks.token_limit_field,
                TokenLimitField::MaxCompletionTokens,
                "openai/{model_id} must freeze max_completion_tokens"
            );
        }

        let (deepseek, _model) = catalog
            .model("deepseek", "compatible-fixture")
            .expect("deepseek/deepseek-v4-flash must stay on the OpenAI-compatible family default");
        assert_eq!(deepseek.wire_family, WireFamilyId::OpenAiChat);
        assert_eq!(
            deepseek.quirks.token_limit_field,
            TokenLimitField::MaxTokens
        );
    }

    /// A catalog-resolved OpenAI Chat provider must render `max_completion_tokens`, not the
    /// long-tail family default `max_tokens`.
    #[cfg(feature = "bundled-catalog")]
    #[test]
    fn published_chat_shape_renders_max_completion_tokens() {
        use broca_protocol::content::{ContentBlock, Message, Role};
        use broca_protocol::request::CallOptions;
        use broca_provider::families::{renderer_version_for, resolve_frozen};
        use broca_provider::registry::wire_family;

        let (provider_spec, model_spec) = published_chat_shape_fixture()
            .model("openai", "chat-fixture")
            .expect("the explicit Chat shape resolves");
        let family = provider_spec.wire_family;
        assert_eq!(family, WireFamilyId::OpenAiChat);

        let frozen = resolve_frozen(
            &provider_spec,
            &model_spec,
            512,
            renderer_version_for(family),
        )
        .expect("bundled catalog models publish known capability flags");
        let opts = CallOptions::new(vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: "Hello".into(),
            }],
        }]);
        let renderer = wire_family(family).expect("family is implemented");
        let rendered = String::from_utf8(renderer.render(&frozen, &opts)).unwrap();

        assert!(
            rendered.contains("\"max_completion_tokens\":512"),
            "expected max_completion_tokens in catalog-resolved OpenAI render; got {rendered}"
        );
        assert!(
            !rendered.contains("\"max_tokens\""),
            "did not expect max_tokens in catalog-resolved OpenAI render; got {rendered}"
        );
    }

    /// Regression for the fix in f984f35: `resolve_frozen` must freeze
    /// `ReasoningPolicy::None` when the selected model declares `reasoning: false`, even
    /// though the Anthropic family's quirk is the OAuth `AnthropicInterleaved` thinking
    /// shape. A non-reasoning model that received the provider-wide thinking shape was
    /// rejected by the provider.
    ///
    /// The gate is driven from SYNTHETIC specs rather than a pinned catalog model. The
    /// models.dev refresh that brought in the Claude 5 generation also removed EVERY
    /// non-reasoning Anthropic model (the bundled Anthropic set is now all `reasoning:
    /// true`), so no bundled fixture can exercise the `reasoning: false` branch anymore —
    /// and pinning a model upstream can delete is exactly the fragility that broke this
    /// test. Building the model specs directly keeps the proof non-vacuous and immune to
    /// catalog churn: the Anthropic family quirk is the interleaved policy, a `reasoning:
    /// false` model freezes `None`, and a `reasoning: true` model still freezes the family
    /// policy.
    /// The bundled snapshot must state `reasoning` on every model. Absent still defaults to
    /// not-capable at freeze (the historical behavior), so a refresh that drops the key would
    /// quietly stop a model reasoning. This reads the raw snapshot rather than the typed view
    /// so a missing key cannot hide behind `CapabilityFlag::Absent`, and it asserts a non-zero
    /// corpus first so an empty or unparsed snapshot refuses rather than passing over nothing.
    #[cfg(feature = "bundled-catalog")]
    #[test]
    fn every_bundled_model_states_its_reasoning_capability() {
        let snapshot: Value = serde_json::from_str(include_str!("../data/models.dev.json"))
            .expect("bundled snapshot parses");
        let mut checked = 0usize;
        let mut silent = Vec::new();
        for (provider_id, provider) in snapshot.as_object().expect("snapshot is an object") {
            let Some(models) = provider.get("models").and_then(Value::as_object) else {
                continue;
            };
            for (model_id, model) in models {
                checked += 1;
                if !model
                    .get("reasoning")
                    .map(Value::is_boolean)
                    .unwrap_or(false)
                {
                    silent.push(format!("{provider_id}/{model_id}"));
                }
            }
        }
        assert!(
            checked > 1000,
            "expected the bundled corpus, got {checked} models -- snapshot missing or unparsed"
        );
        assert!(
            silent.is_empty(),
            "{} model(s) omit `reasoning`, so map_model reads them as false and they will \
             silently stop reasoning: {:?}",
            silent.len(),
            &silent[..silent.len().min(5)]
        );
    }

    #[cfg(feature = "bundled-catalog")]
    #[test]
    fn bundled_model_capability_gates_anthropic_reasoning_shape() {
        use broca_protocol::content::{ContentBlock, Message, Role};
        use broca_protocol::request::CallOptions;
        use broca_provider::families::{renderer_version_for, resolve_frozen};
        use broca_provider::policy::ReasoningPolicy;
        use broca_provider::registry::wire_family;

        let request = CallOptions::new(vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: "Hello".into(),
            }],
        }]);

        // The Anthropic family's provider spec: its quirk is the OAuth interleaved thinking
        // shape. This is the policy the capability gate chooses whether to freeze.
        let provider = map_provider(
            "anthropic",
            &json!({ "id": "anthropic", "name": "Anthropic" }),
            WireFamilyId::AnthropicMessages,
            RawPolicy::Skip,
        );
        assert!(matches!(
            &provider.quirks.reasoning,
            ReasoningPolicy::AnthropicInterleaved { .. }
        ));
        let family = provider.wire_family;
        let renderer = wire_family(family).expect("Anthropic family is implemented");

        // A model that declares reasoning=false freezes ReasoningPolicy::None, NOT the
        // provider-wide interleaved shape.
        let non_reasoning = map_model(
            "synthetic-non-reasoning",
            &json!({
                "id": "synthetic-non-reasoning",
                "name": "Synthetic Non-Reasoning",
                "reasoning": false,
            }),
            RawPolicy::Skip,
        );
        assert_eq!(
            non_reasoning.capabilities.reasoning,
            CapabilityFlag::Known(false)
        );
        let frozen = resolve_frozen(
            &provider,
            &non_reasoning,
            4096,
            renderer_version_for(family),
        )
        .expect("explicit reasoning=false is a known flag");
        assert_eq!(frozen.reasoning, ReasoningPolicy::None);
        let body: Value = serde_json::from_slice(&renderer.render(&frozen, &request)).unwrap();
        assert!(
            body.get("thinking").is_none(),
            "a model that declares reasoning=false must not receive a reasoning directive"
        );

        // Contrast with the old provider-wide freeze: it sent the OAuth thinking shape even
        // though this selected model declares that reasoning is unsupported.
        let mut legacy = frozen.clone();
        legacy.reasoning = provider.quirks.reasoning.clone();
        let legacy_body: Value =
            serde_json::from_slice(&renderer.render(&legacy, &request)).unwrap();
        assert!(legacy_body.get("thinking").is_some());

        // A reasoning-capable model still freezes the provider's interleaved policy, so the
        // gate only suppresses the shape for models that declare they cannot reason.
        let capable = map_model(
            "synthetic-reasoning",
            &json!({
                "id": "synthetic-reasoning",
                "name": "Synthetic Reasoning",
                "reasoning": true,
            }),
            RawPolicy::Skip,
        );
        assert_eq!(capable.capabilities.reasoning, CapabilityFlag::Known(true));
        let capable_frozen =
            resolve_frozen(&provider, &capable, 4096, renderer_version_for(family))
                .expect("explicit reasoning=true is a known flag");
        assert_eq!(capable_frozen.reasoning, provider.quirks.reasoning);
        let capable_body: Value =
            serde_json::from_slice(&renderer.render(&capable_frozen, &request)).unwrap();
        assert!(capable_body.get("thinking").is_some());
    }

    fn model_with_capability(key: &str, value: Option<Value>) -> ModelSpec {
        let mut entry = json!({"id": "synthetic", "name": "Synthetic"});
        if let Some(value) = value {
            entry[key] = value;
        }
        map_model("synthetic", &entry, RawPolicy::Skip)
    }

    fn assert_capability_three_way(key: &'static str, read: fn(&ModelSpec) -> CapabilityFlag) {
        let absent = model_with_capability(key, None);
        let explicit_false = model_with_capability(key, Some(json!(false)));
        let unknown = model_with_capability(key, Some(Value::Null));

        assert_eq!(read(&absent), CapabilityFlag::Absent, "{key} omitted");
        assert_eq!(
            read(&explicit_false),
            CapabilityFlag::Known(false),
            "{key} false"
        );
        assert_eq!(read(&unknown), CapabilityFlag::Unknown, "{key} null");

        // Freeze-time admission: absent and false stay the historical not-capable
        // default; null refuses when the run would use the flag.
        assert_eq!(
            read(&absent).admit(true, "prov", "synthetic", key),
            Ok(false),
            "{key} omitted + needed"
        );
        assert_eq!(
            read(&explicit_false).admit(true, "prov", "synthetic", key),
            Ok(false),
            "{key} false + needed"
        );
        let err = read(&unknown)
            .admit(true, "prov", "synthetic", key)
            .expect_err("{key} null + needed must refuse");
        assert_eq!(
            err.to_string(),
            format!("unknown_capability: provider=prov model=synthetic capability={key}")
        );
        assert_eq!(
            read(&unknown).admit(false, "prov", "synthetic", key),
            Ok(false),
            "{key} null + unneeded proceeds as today"
        );
    }

    #[test]
    fn attachment_absent_false_and_null_are_distinct() {
        assert_capability_three_way("attachment", |m| m.capabilities.attachment);
    }

    #[test]
    fn reasoning_absent_false_and_null_are_distinct() {
        assert_capability_three_way("reasoning", |m| m.capabilities.reasoning);
    }

    #[test]
    fn temperature_absent_false_and_null_are_distinct() {
        assert_capability_three_way("temperature", |m| m.capabilities.temperature);
    }

    #[test]
    fn tool_call_absent_false_and_null_are_distinct() {
        assert_capability_three_way("tool_call", |m| m.capabilities.tool_call);
    }

    #[test]
    fn interleaved_absent_false_and_null_are_distinct() {
        assert_capability_three_way("interleaved", |m| m.capabilities.interleaved);
    }

    #[test]
    fn structured_output_absent_false_and_null_are_distinct() {
        assert_capability_three_way("structured_output", |m| m.capabilities.structured_output);
    }

    #[test]
    fn unknown_reasoning_fails_closed_at_freeze_when_the_family_would_use_it() {
        use broca_provider::families::{renderer_version_for, resolve_frozen};
        use broca_provider::policy::ReasoningPolicy;

        let provider = map_provider(
            "anthropic",
            &json!({ "id": "anthropic", "name": "Anthropic" }),
            WireFamilyId::AnthropicMessages,
            RawPolicy::Skip,
        );
        assert!(matches!(
            &provider.quirks.reasoning,
            ReasoningPolicy::AnthropicInterleaved { .. }
        ));
        let version = renderer_version_for(provider.wire_family);

        let absent = model_with_capability("reasoning", None);
        let frozen = resolve_frozen(&provider, &absent, 4096, version)
            .expect("omitted reasoning still defaults to not capable");
        assert_eq!(frozen.reasoning, ReasoningPolicy::None);

        let explicit_false = model_with_capability("reasoning", Some(json!(false)));
        let frozen = resolve_frozen(&provider, &explicit_false, 4096, version)
            .expect("explicit reasoning=false stays not capable");
        assert_eq!(frozen.reasoning, ReasoningPolicy::None);

        let unknown = model_with_capability("reasoning", Some(Value::Null));
        let err = resolve_frozen(&provider, &unknown, 4096, version)
            .expect_err("null reasoning must refuse when the family would request thinking");
        assert_eq!(
            err.to_string(),
            "unknown_capability: provider=anthropic model=synthetic capability=reasoning"
        );
    }

    #[test]
    fn unknown_reasoning_is_allowed_when_the_family_would_not_use_it() {
        use broca_provider::families::{renderer_version_for, resolve_frozen};
        use broca_provider::policy::ReasoningPolicy;

        let provider = map_provider(
            "openai",
            &json!({ "id": "openai", "name": "OpenAI" }),
            WireFamilyId::OpenAiChat,
            RawPolicy::Skip,
        );
        assert_eq!(provider.quirks.reasoning, ReasoningPolicy::None);

        let unknown = model_with_capability("reasoning", Some(Value::Null));
        let frozen = resolve_frozen(
            &provider,
            &unknown,
            4096,
            renderer_version_for(provider.wire_family),
        )
        .expect("null reasoning is allowed when the family would not request thinking");
        assert_eq!(frozen.reasoning, ReasoningPolicy::None);
    }

    /// Pin claude-opus-5's catalog→serving resolution so a future snapshot refresh that
    /// drops it or re-routes its family fails loudly here, not silently in the field.
    /// opus-5 is a native Anthropic model: it must resolve to the AnthropicMessages family
    /// under its own wire id, keep its 1M-context / 128K-output limits, and — being
    /// reasoning-capable — freeze the same AnthropicInterleaved policy that claude-fable-5
    /// (which runs in production today) already gets.
    #[cfg(feature = "bundled-catalog")]
    #[test]
    fn bundled_anthropic_opus_5_resolves_for_serving() {
        use broca_provider::families::{renderer_version_for, resolve_frozen};
        use broca_provider::policy::ReasoningPolicy;

        let (provider, model) = catalog()
            .model_for_serving("anthropic", "claude-opus-5")
            .expect("anthropic/claude-opus-5 must resolve for serving through the catalog");

        assert_eq!(provider.wire_family, WireFamilyId::AnthropicMessages);
        assert_eq!(
            model.id, "claude-opus-5",
            "wire model id must be the catalog id"
        );
        assert_eq!(model.limits.context, Some(1_000_000));
        assert_eq!(model.limits.max_output, Some(128_000));
        assert_eq!(model.capabilities.reasoning, CapabilityFlag::Known(true));

        let frozen = resolve_frozen(
            &provider,
            &model,
            4096,
            renderer_version_for(provider.wire_family),
        )
        .expect("bundled catalog models publish known capability flags");
        assert_eq!(frozen.wire_model_id, "claude-opus-5");
        assert!(
            matches!(
                frozen.reasoning,
                ReasoningPolicy::AnthropicInterleaved { .. }
            ),
            "reasoning-capable opus-5 must freeze the AnthropicInterleaved policy like fable-5"
        );
    }

    #[cfg(feature = "bundled-catalog")]
    #[test]
    fn bundled_openai_gpt_5_6_sol_preserves_published_limits() {
        let (_provider, model) = catalog()
            .model_for_serving("openai", "gpt-5.6-sol")
            .expect("openai/gpt-5.6-sol must resolve for serving through the catalog");

        assert_eq!(model.id, "gpt-5.6-sol");
        assert_eq!(model.limits.max_input, Some(922_000));
        assert_eq!(model.limits.context, Some(1_050_000));
        assert_eq!(model.limits.max_output, Some(128_000));
    }

    /// The structural lock against the catalog/helper render-drift class of bug.
    ///
    /// The catalog serves native families (Anthropic, Gemini, Bedrock, OpenAI
    /// Responses) as well as the OpenAI-compatible long tail. Each must render the SAME
    /// request bytes whether the config came from the catalog ingestion path
    /// (`map_provider` → `resolve_frozen`) or from the hand-written per-family
    /// `*_frozen_config` helper. They used to diverge: `map_provider` hardcoded
    /// `Quirks::default()` (the OpenAI-chat shape), so a catalog-resolved Anthropic
    /// rendered without its required identity-lead system block and was rejected, while
    /// the helper rendered it correctly. The existing render goldens only test the
    /// HELPER path, so they could not catch a catalog path that diverged from them.
    ///
    /// This test renders a fixed request through BOTH paths for EVERY family and
    /// asserts byte-identical output. If the two paths ever drift again (e.g. someone
    /// changes how `map_provider` sources quirks), this fails. It compares rendered
    /// BODY bytes — base_url/region differ between the two paths but live in the
    /// endpoint/signing layer, never in the rendered (hashed) body.
    #[test]
    fn catalog_path_renders_identically_to_per_family_helper() {
        use broca_protocol::content::{ContentBlock, Message, Role};
        use broca_protocol::request::{CallOptions, Tool, ToolChoice};
        use broca_provider::families::{
            anthropic_frozen_config, bedrock_frozen_config, gemini_frozen_config,
            google_codeassist_frozen_config, openai_chat_frozen_config,
            openai_responses_chatgpt_frozen_config, openai_responses_frozen_config,
            renderer_version_for, resolve_frozen,
        };
        use broca_provider::registry::wire_family;
        use broca_provider::wire::FrozenRenderConfig;

        // A fixed request exercising the fields families differ on: a system message
        // (placement is the field that broke Anthropic), a user message, and a tool.
        let request = || {
            let mut opts = CallOptions::new(vec![
                Message {
                    cache_prefix_blocks: None,
                    origin: None,
                    ordinal: None,
                    tags: Default::default(),
                    role: Role::System,
                    content: vec![ContentBlock::Text {
                        text: "Be concise.".into(),
                    }],
                },
                Message {
                    cache_prefix_blocks: None,
                    origin: None,
                    ordinal: None,
                    tags: Default::default(),
                    role: Role::User,
                    content: vec![ContentBlock::Text {
                        text: "What time is it in Madrid?".into(),
                    }],
                },
            ]);
            opts.tools = vec![Tool {
                name: "get_time".into(),
                description: "Get the current time in an IANA timezone.".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": { "timezone": { "type": "string" } },
                    "required": ["timezone"]
                }),
                module: None,
                execution_mode: Default::default(),
                reply: None,
            }]
            .into();
            opts.tool_choice = ToolChoice::Auto.into();
            // Populate step identity so the GoogleCodeAssist family (which requires it for
            // its deterministic requestId) renders; every other family ignores it.
            opts.step_context = Some(broca_protocol::StepContext {
                session_address: None,
                credential_id: None,
                account_ref: None,
                run_id: "fixed-run".into(),
                step_index: 1,
            });
            opts
        };

        // The hand-written helper config for a family — the known-correct reference.
        fn helper(family: WireFamilyId, model_id: &str) -> FrozenRenderConfig {
            match family {
                WireFamilyId::AnthropicMessages => anthropic_frozen_config(model_id),
                WireFamilyId::OpenAiChat => openai_chat_frozen_config(model_id),
                WireFamilyId::OpenAiResponses => openai_responses_frozen_config(model_id),
                WireFamilyId::Gemini => gemini_frozen_config(model_id),
                WireFamilyId::BedrockConverse => bedrock_frozen_config(model_id),
                WireFamilyId::GoogleCodeAssist => {
                    let mut frozen = google_codeassist_frozen_config(model_id);
                    frozen.project_id = Some("test-project".into());
                    frozen
                }
                WireFamilyId::OpenAiResponsesChatgpt => {
                    openai_responses_chatgpt_frozen_config(model_id)
                }
            }
        }

        let model_id = "test-model";
        for family in [
            WireFamilyId::AnthropicMessages,
            WireFamilyId::OpenAiChat,
            WireFamilyId::OpenAiResponses,
            WireFamilyId::Gemini,
            WireFamilyId::BedrockConverse,
            WireFamilyId::GoogleCodeAssist,
            WireFamilyId::OpenAiResponsesChatgpt,
        ] {
            // Catalog path test: a minimal provider entry (no `base_url`, so the family default
            // is used like the hand-written helper) flows through `map_provider` (sourcing quirks from
            // the family defaults) and then `resolve_frozen` with the same max-output and
            // renderer version the helper bakes in.
            let entry = json!({
                "id": "p", "name": "P",
                "models": {
                    model_id: {
                        "id": model_id,
                        "name": "Test Model",
                        // The family helper functions generate the render shape for models that
                        // support reasoning, so this fixture must declare the same capability.
                        "reasoning": true
                    }
                }
            });
            let provider_spec = map_provider("p", &entry, family, RawPolicy::Preserve);
            let model_spec = map_model(
                model_id,
                entry.pointer(&format!("/models/{model_id}")).unwrap(),
                RawPolicy::Preserve,
            );
            let mut catalog_frozen = resolve_frozen(
                &provider_spec,
                &model_spec,
                2048,
                renderer_version_for(family),
            )
            .expect("synthetic catalog fixtures publish known capability flags");
            if family == WireFamilyId::GoogleCodeAssist {
                // project_id is credential-bound rather than catalog-derived. Supply the same
                // real-looking value to both paths; rendering an absent project is invalid.
                catalog_frozen.project_id = Some("test-project".into());
            }

            let renderer = wire_family(family).expect("family is implemented");
            let helper_frozen = helper(family, model_id);
            if family == WireFamilyId::AnthropicMessages {
                assert_eq!(
                    serde_json::to_value(&catalog_frozen).unwrap(),
                    serde_json::to_value(&helper_frozen).unwrap(),
                    "a reasoning-capable Anthropic model must keep the serialized golden config"
                );
            }
            let catalog_bytes = renderer.render(&catalog_frozen, &request());
            let helper_bytes = renderer.render(&helper_frozen, &request());

            assert_eq!(
                String::from_utf8(catalog_bytes).unwrap(),
                String::from_utf8(helper_bytes).unwrap(),
                "catalog path and per-family helper render DIFFERENT bytes for {family:?} — \
                 the catalog must produce the same request shape as the known-correct helper \
                 (this is the drift the Quirks::default() bug caused for native families)"
            );
        }

        // OpenAI is the one intentional catalog-time override on top of the shared OpenAI-Chat
        // family default. Lock it to EXACTLY one config delta: the token-limit field.
        let entry = json!({
            "id": "openai", "name": "OpenAI",
            "models": { model_id: { "id": model_id, "name": "Test Model" } }
        });
        let provider_spec = map_provider(
            "openai",
            &entry,
            WireFamilyId::OpenAiChat,
            RawPolicy::Preserve,
        );
        let model_spec = map_model(
            model_id,
            entry.pointer(&format!("/models/{model_id}")).unwrap(),
            RawPolicy::Preserve,
        );
        let openai_frozen = resolve_frozen(
            &provider_spec,
            &model_spec,
            2048,
            renderer_version_for(WireFamilyId::OpenAiChat),
        )
        .expect("synthetic catalog fixtures publish known capability flags");
        let family_default = helper(WireFamilyId::OpenAiChat, model_id);
        assert_eq!(
            family_default.token_limit_field,
            TokenLimitField::MaxTokens,
            "the shared OpenAI-Chat helper must stay on the long-tail default"
        );
        assert_eq!(
            openai_frozen.token_limit_field,
            TokenLimitField::MaxCompletionTokens,
            "OpenAI's catalog override must freeze max_completion_tokens"
        );
        let mut expected_openai = family_default.clone();
        expected_openai.token_limit_field = TokenLimitField::MaxCompletionTokens;
        assert_eq!(
            openai_frozen, expected_openai,
            "OpenAI's catalog config must differ from the family default ONLY in token_limit_field"
        );
    }

    #[test]
    fn xai_usage_policy_is_provider_scoped() {
        use broca_provider::families::{renderer_version_for, resolve_frozen};

        let entry = json!({
            "id": "provider",
            "name": "Provider",
            "models": {"test-model": {"id": "test-model", "name": "Test Model"}}
        });
        let model = map_model(
            "test-model",
            entry.pointer("/models/test-model").unwrap(),
            RawPolicy::Preserve,
        );
        let frozen_usage = |provider_id| {
            let provider = map_provider(
                provider_id,
                &entry,
                WireFamilyId::OpenAiChat,
                RawPolicy::Preserve,
            );
            resolve_frozen(
                &provider,
                &model,
                2048,
                renderer_version_for(WireFamilyId::OpenAiChat),
            )
            .expect("synthetic catalog fixtures publish known capability flags")
            .usage
        };

        assert_eq!(
            frozen_usage("xai"),
            UsagePolicy::OpenAiUsageReasoningSeparate
        );
        for provider_id in ["openai", "deepseek"] {
            assert_eq!(
                frozen_usage(provider_id),
                UsagePolicy::OpenAiUsage,
                "{provider_id} must keep inclusive completion-token semantics"
            );
        }
    }

    /// ollama-cloud must opt into empty-string assistant content on tool-call turns; the
    /// OpenAI-compatible long tail must stay on the family default (false).
    #[cfg(feature = "bundled-catalog")]
    #[test]
    fn bundled_ollama_cloud_tool_call_content_quirk_but_long_tail_stays_off() {
        let (ollama, _model) = catalog()
            .model("ollama-cloud", "deepseek-v4-pro")
            .unwrap_or_else(|| {
                panic!("ollama-cloud/deepseek-v4-pro must resolve from the bundled snapshot")
            });
        assert_eq!(ollama.wire_family, WireFamilyId::OpenAiChat);
        assert!(
            ollama.quirks.tool_call_content_empty_string,
            "ollama-cloud must opt into tool_call_content_empty_string for OpenAI-compat"
        );

        let (deepseek, _model) = catalog()
            .model("deepseek", "deepseek-v4-flash")
            .expect("deepseek/deepseek-v4-flash must stay on the OpenAI-compatible family default");
        assert_eq!(deepseek.wire_family, WireFamilyId::OpenAiChat);
        assert!(
            !deepseek.quirks.tool_call_content_empty_string,
            "deepseek must not inherit ollama's content quirk"
        );
    }

    /// Direct APIs own their echo policies; serving a model with the same name is not opt-in.
    #[cfg(feature = "bundled-catalog")]
    #[test]
    fn bundled_chat_reasoning_echo_policies_are_provider_scoped() {
        let (deepseek, model) = catalog()
            .model("deepseek", "deepseek-v4-flash")
            .expect("deepseek/deepseek-v4-flash resolves from the bundled snapshot");
        assert_eq!(
            deepseek.quirks.reasoning_echo,
            Some(broca_provider::policy::ReasoningEchoPolicy::DEEPSEEK)
        );
        let frozen = broca_provider::families::resolve_frozen(&deepseek, &model, 2048, 1)
            .expect("deepseek freezes");
        assert_eq!(frozen.reasoning_echo, deepseek.quirks.reasoning_echo);

        for (provider_id, model_id) in [
            ("ollama-cloud", "deepseek-v4-pro"),
            ("openai", "gpt-4o"),
            ("xai", "grok-4"),
        ] {
            let Some((provider, _)) = catalog().model(provider_id, model_id) else {
                continue;
            };
            assert!(
                provider.quirks.reasoning_echo.is_none(),
                "{provider_id} must not echo reasoning_content"
            );
        }
        let opted_in = catalog()
            .provider_ids()
            .filter_map(|id| catalog().provider(id))
            .filter(|provider| {
                provider.quirks.reasoning_echo
                    == Some(broca_provider::policy::ReasoningEchoPolicy::DEEPSEEK)
            })
            .map(|provider| provider.id)
            .collect::<Vec<_>>();
        assert_eq!(
            opted_in,
            vec![
                "deepseek",
                "kimi-code-plan-cn",
                "kimi-code-plan-global",
                "moonshotai",
                "moonshotai-cn"
            ]
        );
    }

    #[test]
    fn kimi_and_openrouter_route_detection_is_api_scoped() {
        use broca_provider::policy::ReasoningEchoPolicy as Echo;
        for (id, url) in [
            ("kimi-code-plan-global", ""),
            ("kimi-code-plan-cn", ""),
            ("moonshotai", ""),
            ("moonshotai-cn", ""),
            ("custom", "https://api.kimi.ai/coding/v1"),
            ("custom", "https://api.kimi.com/coding/v1"),
            ("custom", "https://api.moonshot.ai/v1"),
            ("custom", "https://api.moonshot.cn/v1"),
        ] {
            assert_eq!(
                provider_quirks(id, WireFamilyId::OpenAiChat, url).reasoning_echo,
                Some(Echo::KIMI),
                "{id} {url}"
            );
        }
        for (id, url) in [
            ("openrouter", ""),
            ("custom", "https://openrouter.ai/api/v1"),
        ] {
            assert_eq!(
                provider_quirks(id, WireFamilyId::OpenAiChat, url).reasoning_echo,
                Some(Echo::OPENROUTER)
            );
        }
        for url in [
            "https://api.kimi.ai.evil.test/v1",
            "https://api.kimi.ai@evil.test/v1",
            "https://evil.test/api.moonshot.ai",
            "https://openrouter.ai.evil.test/v1",
        ] {
            assert!(reasoning_echo_for_route("custom", url).is_none(), "{url}");
        }
        assert!(provider_quirks(
            "kimi-code-plan-global",
            WireFamilyId::AnthropicMessages,
            "https://api.kimi.ai/coding"
        )
        .reasoning_echo
        .is_none());
    }

    #[test]
    fn deepseek_route_detection_matches_the_provider_or_a_deepseek_host() {
        for (provider_id, base_url) in [
            ("deepseek", ""),
            ("deepseek", "http://127.0.0.1:4010/v1"),
            ("my-deepseek", "https://api.deepseek.com"),
            ("my-deepseek", "https://API.DeepSeek.com/v1"),
            ("my-deepseek", "https://deepseek.com:443/v1?x=1"),
            ("my-deepseek", "https://api.deepseek.com./v1"),
        ] {
            assert!(
                is_deepseek_route(provider_id, base_url),
                "{provider_id} {base_url}"
            );
        }
        for (provider_id, base_url) in [
            ("openai", "https://api.openai.com/v1"),
            ("ollama-cloud", "https://ollama.com/v1"),
            ("x", "https://notdeepseek.com/v1"),
            ("x", "https://api.deepseek.com.evil.example/v1"),
            ("x", "https://deepseek.com@evil.example/v1"),
            ("x", "https://evil.example/deepseek.com"),
            ("x", "api.deepseek.com"),
            ("x", ""),
        ] {
            assert!(
                !is_deepseek_route(provider_id, base_url),
                "{provider_id} {base_url}"
            );
        }
    }

    /// The quirk is OpenAI-Chat only: no other family reads it, and setting it there would only
    /// move that family's frozen blob.
    #[test]
    fn deepseek_reasoning_echo_is_set_only_for_the_openai_chat_family() {
        assert!(provider_quirks("deepseek", WireFamilyId::OpenAiChat, "")
            .reasoning_echo
            .is_some());
        assert!(!provider_quirks(
            "deepseek",
            WireFamilyId::AnthropicMessages,
            "https://api.deepseek.com/anthropic"
        )
        .reasoning_echo
        .is_some());
        assert!(!provider_quirks(
            "openai",
            WireFamilyId::OpenAiChat,
            "https://api.openai.com/v1"
        )
        .reasoning_echo
        .is_some());
    }
}
