use broca_owner_only::OpenOptions;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Instant;

use broca_provider::spec::{
    Capabilities, CapabilityFlag, CostSchedule, CostTier, Limits, ModelSpec, Overlay, ProviderSpec,
    ReasoningOptions, WireFamilyId,
};
use serde::{Deserialize, Deserializer, Serialize};
#[cfg(test)]
use serde_json::Map;
use serde_json::Value;

use crate::family::{resolve_provider_family, FamilyResolution};
use crate::{Catalog, CatalogError, OVERLAY};

/// Catalog fields fetched from fusiform for accepting sends and maintaining prompt caches.
/// Accepting a send freezes its run configuration before any record is written.
///
/// `existence` decides whether a model can be served. `limit.context` drives prompt reduction;
/// `limit.output` supplies the default output-token cap when the send sets none (see
/// `default_max_output_tokens` in `live.rs`). The attachment, reasoning, and tool-call flags
/// populate Broca's typed capabilities. `capability.reasoning_options` lists the settings used
/// to resolve the caller's `model.variant`. Fetching the `capability.reasoning` prefix also
/// fetches `capability.reasoning_options`, but listing both keeps the options requested if the
/// flag's prefix is later removed or renamed. Fusiform serves neither temperature nor
/// interleaving fields, so requesting them would add no information.
///
/// The rate prefixes fetch base and context-bracket input, output, cache-read, and cache-write
/// prices for cache maintenance, plus input/output mode prices for accepting sends. A missing
/// mode rate is never replaced by a base rate; warming deliberately ignores mode prices.
pub const FUSIFORM_FACT_PREFIXES: &[&str] = &[
    "existence",
    "limit.context",
    "limit.output",
    "capability.attachment",
    "capability.reasoning",
    "capability.reasoning_options",
    "capability.tool_call",
    "capability.structured_output",
    "rate.input",
    "rate.output",
    "rate.cache_read",
    "rate.cache_write",
];

/// Prefix of a per-model input rate for one upstream mode, e.g. `rate.input.mode.fast`.
const MODE_RATE_INPUT_PREFIX: &str = "rate.input.mode.";
/// Prefix of a per-model output rate for one upstream mode, e.g. `rate.output.mode.fast`.
const MODE_RATE_OUTPUT_PREFIX: &str = "rate.output.mode.";

/// The successful `catalog.get` response persisted as the last-known-good document.
///
/// Extra response fields remain forward-compatible. Broca only needs the resolved instant,
/// version, source, and model fact plane; retaining the qualification arrays in the LKG keeps the
/// response reproducible without interpreting facts Broca did not request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FusiformCatalogResponse {
    pub source: String,
    pub resolved_at_ms: i64,
    pub catalog_version: i64,
    pub models: BTreeMap<String, BTreeMap<String, Value>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub withheld: Vec<Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub uncertain: Vec<Value>,
}

/// Where the currently served in-memory document was obtained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogSource {
    Fusiform,
    LastKnownGood,
}

impl CatalogSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Fusiform => "fusiform",
            Self::LastKnownGood => "lkg",
        }
    }
}

struct LoadedCatalog {
    // Arc-wrapped so an equal-version refresh can republish the existing index with fresh
    // metadata instead of rebuilding ~6,300 models to produce the same answer.
    catalog: Arc<ServingCatalog>,
    model_identities: Arc<BTreeSet<String>>,
    resolved_at_ms: i64,
    catalog_version: i64,
    source: CatalogSource,
    /// Whether any row of the source document carried `limit.output`. The equal-version fast
    /// path reuses this index on the premise that equal versions mean equal facts, which stops
    /// holding when broca starts requesting a fact it did not request before: an LKG written
    /// before `limit.output` was requested has the same version as the next poll and none of
    /// the new fact. Without this bit that poll would republish an index with no output limits,
    /// and it would stay that way until fusiform happened to mint a new version.
    carries_output_limits: bool,
    /// Whether any row of the source document carried a mode rate. Same reason as
    /// `carries_output_limits`: an LKG fetched before broca requested mode rates has the same
    /// version as the next poll and none of them, and reusing its index would keep every tier
    /// refused until fusiform happened to mint a new version.
    carries_mode_rates: bool,
    /// A poll requesting prices must replace an equal-version last-known-good catalog snapshot
    /// that was fetched without prices.
    carries_warm_rates: bool,
}

/// Whether a response's model rows carry any mode rate. Stops at the first one.
fn response_carries_mode_rates(response: &FusiformCatalogResponse) -> bool {
    response.models.values().any(|facts| {
        facts.keys().any(|key| {
            key.starts_with(MODE_RATE_INPUT_PREFIX) || key.starts_with(MODE_RATE_OUTPUT_PREFIX)
        })
    })
}

/// Whether a response's model rows carry `limit.output`. Stops at the first row that has it, so
/// on a current document this reads one or two rows rather than thousands.
fn response_carries_output_limits(response: &FusiformCatalogResponse) -> bool {
    response
        .models
        .values()
        .any(|facts| facts.contains_key("limit.output"))
}

/// The bounded provider facts retained for admission. `raw` is always null, so cloning this spec
/// for one lookup never copies another model or an ingestion-only catalog field.
struct ServingProvider {
    spec: ProviderSpec,
    models: BTreeMap<String, ServingModel>,
}

/// Construction also tracks pending providers so they remain valid overlay targets, then discards
/// them before the immutable serving index is installed.
struct DraftServingProvider {
    spec: Option<ProviderSpec>,
    models: BTreeMap<String, ServingModel>,
}

/// The exact model facts retained between polls.
///
/// Fusiform requires all three flags on a present row. They remain three-state typed values so an
/// explicit `null` still fails closed when a family would consume it.
#[derive(Clone)]
struct ServingModel {
    display_name: String,
    cost: CostSchedule,
    context_limit: Option<u64>,
    /// The model's published maximum output, `None` when fusiform serves null (its spelling of
    /// "unknown": it collapses an upstream zero to null) or omits the fact. Whether the value is
    /// usable as a default cap is decided at resolution, not here, so the index keeps what was
    /// served.
    output_limit: Option<u64>,
    attachment: CapabilityFlag,
    reasoning: CapabilityFlag,
    tool_call: CapabilityFlag,
    /// `capability.structured_output`. Unlike the three flags above, fusiform omits it on rows
    /// whose upstream does not state it, and absence reads as unknown rather than refusing the
    /// row.
    structured_output: CapabilityFlag,
    /// `capability.reasoning_options`: null, `[]` and a populated array stay distinct. A catalog
    /// from before fusiform served the fact reads as unstated, which defines no variant.
    reasoning_options: ReasoningOptions,
    /// Upstream mode names (`fast`, `priority`, ...) for which fusiform serves BOTH a priced
    /// input and a priced output rate, sorted. A mode missing either side is unpriced and is
    /// not listed; see `priced_modes_fact`.
    priced_modes: Vec<String>,
    wire_family: Option<WireFamilyId>,
}

impl Default for ServingModel {
    fn default() -> Self {
        Self {
            display_name: String::new(),
            cost: CostSchedule::default(),
            context_limit: None,
            output_limit: None,
            attachment: CapabilityFlag::Absent,
            reasoning: CapabilityFlag::Absent,
            tool_call: CapabilityFlag::Absent,
            structured_output: CapabilityFlag::Absent,
            reasoning_options: ReasoningOptions::Unstated,
            priced_modes: Vec::new(),
            wire_family: None,
        }
    }
}

impl ServingModel {
    fn from_spec(
        model: &ModelSpec,
        model_family: WireFamilyId,
        provider_family: WireFamilyId,
    ) -> Self {
        Self {
            display_name: model.display_name.clone(),
            cost: model.cost.clone(),
            context_limit: model.limits.context,
            output_limit: model.limits.max_output,
            attachment: model.capabilities.attachment,
            reasoning: model.capabilities.reasoning,
            tool_call: model.capabilities.tool_call,
            structured_output: model.capabilities.structured_output,
            reasoning_options: model.reasoning_options.clone(),
            // The legacy snapshot path carries no fusiform mode rates, so it offers no tier
            // that needs one.
            priced_modes: Vec::new(),
            wire_family: (model_family != provider_family).then_some(model_family),
        }
    }

    fn to_spec(&self, model_id: &str) -> ModelSpec {
        ModelSpec {
            id: model_id.to_string(),
            display_name: self.display_name.clone(),
            family: None,
            capabilities: Capabilities {
                attachment: self.attachment,
                reasoning: self.reasoning,
                temperature: CapabilityFlag::Absent,
                tool_call: self.tool_call,
                interleaved: CapabilityFlag::Absent,
                structured_output: self.structured_output,
            },
            limits: Limits {
                context: self.context_limit,
                max_input: None,
                max_output: self.output_limit,
            },
            cost: self.cost.clone(),
            reasoning_options: self.reasoning_options.clone(),
            release_date: None,
            status: None,
            overlay: Overlay::default(),
            raw: Value::Null,
        }
    }
}

struct ServingCatalog {
    providers: BTreeMap<String, ServingProvider>,
    overlay_removed_models: BTreeMap<String, BTreeSet<String>>,
    withdrawn_models: BTreeMap<String, BTreeSet<String>>,
}

/// An immutable admission snapshot. Taking one clones an `Arc`; it performs no file or network IO.
///
/// This deliberately exposes only serving accessors. It does not dereference to the legacy
/// generic catalog, because doing so would require retaining or reconstructing that catalog.
#[derive(Clone)]
pub struct CatalogSnapshot(Arc<LoadedCatalog>);

impl CatalogSnapshot {
    pub fn resolved_at_ms(&self) -> i64 {
        self.0.resolved_at_ms
    }

    pub fn catalog_version(&self) -> i64 {
        self.0.catalog_version
    }

    pub fn source(&self) -> CatalogSource {
        self.0.source
    }

    /// Whether two snapshots share one serving index rather than holding equal copies.
    ///
    /// Test-facing, and it asserts the thing that matters: an equal-version poll is supposed to
    /// REPUBLISH the existing index, not rebuild an identical one. Comparing contents cannot
    /// tell those apart, and the difference is the entire 2.96ms / 1.38 MB saving.
    #[doc(hidden)]
    pub fn shares_index_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0.catalog, &other.0.catalog)
    }

    /// Whether the current catalog serves any models for this provider.
    pub fn contains_provider(&self, provider_id: &str) -> bool {
        self.0
            .catalog
            .providers
            .get(provider_id)
            .is_some_and(|provider| !provider.models.is_empty())
    }

    /// Whether a provider/model pair is in the compact serving index.
    pub fn contains_model(&self, provider_id: &str, model_id: &str) -> bool {
        self.0.catalog.contains_model(provider_id, model_id)
    }

    /// Resolve the family selected for one model without materializing its full serving pair.
    pub fn wire_family(&self, provider_id: &str, model_id: &str) -> Option<WireFamilyId> {
        self.0.catalog.wire_family(provider_id, model_id)
    }

    /// Read the prompt-pressure limit retained for one model.
    pub fn context_limit(&self, provider_id: &str, model_id: &str) -> Option<u64> {
        self.0.catalog.context_limit(provider_id, model_id)
    }

    /// Read the published output maximum retained for one model (`None` = unknown).
    pub fn output_limit(&self, provider_id: &str, model_id: &str) -> Option<u64> {
        self.0.catalog.output_limit(provider_id, model_id)
    }

    /// The upstream mode names this model has a priced input AND output rate for, sorted.
    /// Empty for a model the catalog does not list.
    pub fn priced_modes(&self, provider_id: &str, model_id: &str) -> Vec<String> {
        self.0
            .catalog
            .provider_and_model(provider_id, model_id)
            .map(|(_, model)| model.priced_modes.clone())
            .unwrap_or_default()
    }

    /// Current base/context prices for cache maintenance. Modes are intentionally excluded,
    /// and an absent rate stays absent so a maintenance decision can refuse unknown pricing.
    pub fn warm_cost_schedule(&self, provider_id: &str, model_id: &str) -> Option<CostSchedule> {
        self.0
            .catalog
            .provider_and_model(provider_id, model_id)
            .map(|(_, model)| model.cost.clone())
    }

    /// Materialize the bounded typed pair consumed by admission and freeze.
    pub fn model_for_serving(
        &self,
        provider_id: &str,
        model_id: &str,
    ) -> Option<(ProviderSpec, ModelSpec)> {
        self.0.catalog.model_for_serving(provider_id, model_id)
    }

    /// Whether fusiform explicitly reports this identity as withdrawn.
    pub fn is_withdrawn(&self, provider_id: &str, model_id: &str) -> bool {
        self.0.catalog.is_withdrawn(provider_id, model_id)
    }

    /// Whether broca's overlay explicitly removes this identity from the serving catalog.
    pub fn is_overlay_removed(&self, provider_id: &str, model_id: &str) -> bool {
        self.0.catalog.is_overlay_removed(provider_id, model_id)
    }
}

impl ServingCatalog {
    /// Consume fusiform's identity map exactly once. The overlay is already typed and is applied
    /// while compact rows are assembled; no generic provider/model tree is ever created.
    fn from_fusiform(
        models: &BTreeMap<String, BTreeMap<String, Value>>,
    ) -> Result<(Self, BTreeSet<String>), CatalogError> {
        Self::from_fusiform_with_overlay(models, RuntimeOverlay::parse()?)
    }

    /// The overlay as a parameter, so a test can hand in a `remove` for a model its fixture
    /// lists. Production always passes the compiled overlay; the seam exists because that
    /// overlay carried a real suppression (deepseek-chat) until upstream withdrew the id on
    /// 2026-09-18, and a test of how removal is RECORDED must not depend on a vendor keeping
    /// a dead row around to suppress.
    fn from_fusiform_with_overlay(
        models: &BTreeMap<String, BTreeMap<String, Value>>,
        overlay: RuntimeOverlay,
    ) -> Result<(Self, BTreeSet<String>), CatalogError> {
        let mut providers: BTreeMap<String, DraftServingProvider> = BTreeMap::new();
        let mut overlay_removed_models: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        let mut withdrawn_models: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        let mut model_identities = BTreeSet::new();

        for (identity, facts) in models {
            let (provider_id, model_id) = identity
                .split_once('/')
                .filter(|(provider, model)| !provider.is_empty() && !model.is_empty())
                .ok_or_else(|| CatalogError::FusiformModelIdentity(identity.clone()))?;
            let provider_id = provider_id.to_string();
            let model_id = model_id.to_string();

            match facts.get("existence") {
                Some(Value::String(value)) if value == "present" => {
                    // Creating the provider before applying a model removal matches the old base:
                    // a provider remains a valid overlay patch target even if removal empties it.
                    let provider = providers.entry(provider_id.clone()).or_insert_with(|| {
                        let provider_overlay = overlay.patch.get(&provider_id);
                        let overlay_family =
                            provider_overlay.and_then(RuntimeProviderOverlay::family);
                        let family = match resolve_provider_family(&provider_id, overlay_family) {
                            FamilyResolution::Supported { family, .. } => Some(family),
                            FamilyResolution::Pending { .. } => None,
                        };
                        RuntimeProviderOverlay::serving_provider(
                            provider_overlay,
                            &provider_id,
                            family,
                        )
                    });

                    if overlay.removes(&provider_id, &model_id) {
                        overlay_removed_models
                            .entry(provider_id.clone())
                            .or_default()
                            .insert(model_id.clone());
                    } else {
                        let mut model = ServingModel {
                            display_name: String::new(),
                            cost: warm_cost_fact(facts),
                            context_limit: context_fact(identity, facts)?,
                            output_limit: output_fact(identity, facts)?,
                            attachment: capability_fact(identity, facts, "capability.attachment")?,
                            reasoning: capability_fact(identity, facts, "capability.reasoning")?,
                            tool_call: capability_fact(identity, facts, "capability.tool_call")?,
                            structured_output: optional_capability_fact(
                                identity,
                                facts,
                                "capability.structured_output",
                            )?,
                            reasoning_options: reasoning_options_fact(identity, facts)?,
                            priced_modes: priced_modes_fact(identity, facts)?,
                            wire_family: None,
                        };
                        if let Some(patch) = overlay.model_patch(&provider_id, &model_id) {
                            patch.apply(&mut model);
                        }
                        provider.models.insert(model_id.clone(), model);
                    }
                }
                Some(Value::String(value))
                    if value == "absent" || value == "retired" || value == "withdrawn" =>
                {
                    withdrawn_models
                        .entry(provider_id.clone())
                        .or_default()
                        .insert(model_id.clone());
                }
                Some(Value::String(value)) => {
                    return Err(CatalogError::FusiformFact {
                        model: identity.clone(),
                        fact: "existence".to_string(),
                        reason: format!("unknown existence state '{value}'"),
                    });
                }
                Some(value) => {
                    return Err(CatalogError::FusiformFact {
                        model: identity.clone(),
                        fact: "existence".to_string(),
                        reason: format!("expected string, found {}", value_kind(value)),
                    });
                }
                None => {
                    return Err(CatalogError::FusiformFact {
                        model: identity.clone(),
                        fact: "existence".to_string(),
                        reason: "fact is absent".to_string(),
                    });
                }
            }
            model_identities.insert(identity.clone());
        }

        // Patch model additions after the fusiform pass so map order cannot change whether an
        // overlay-only model exists. Removed base models stay removed even if the same patch names
        // them, matching the old merge-then-remove order.
        for (provider_id, patch) in &overlay.patch {
            let Some(provider) = providers.get_mut(provider_id) else {
                continue;
            };
            for (model_id, model_patch) in &patch.models {
                if overlay.removes(provider_id, model_id) {
                    overlay_removed_models
                        .entry(provider_id.clone())
                        .or_default()
                        .insert(model_id.clone());
                    continue;
                }
                if provider.models.contains_key(model_id) {
                    continue;
                }
                let mut model = ServingModel::default();
                model_patch.apply(&mut model);
                provider.models.insert(model_id.clone(), model);
            }
        }

        // An added provider must not collide with the present fusiform base. Additions are not
        // subject to `remove`: the historical runtime filtered removes against the base first.
        for (provider_id, addition) in &overlay.add {
            if providers.contains_key(provider_id) {
                return Err(CatalogError::AddTargetExists(provider_id.clone()));
            }
            let family = match resolve_provider_family(provider_id, addition.family()) {
                FamilyResolution::Supported { family, .. } => Some(family),
                FamilyResolution::Pending { .. } => None,
            };
            let mut provider =
                RuntimeProviderOverlay::serving_provider(Some(addition), provider_id, family);
            for (model_id, model_patch) in &addition.models {
                let mut model = ServingModel::default();
                model_patch.apply(&mut model);
                provider.models.insert(model_id.clone(), model);
            }
            providers.insert(provider_id.clone(), provider);
        }

        // Pending providers are represented during construction only so overlay target validation
        // keeps the old semantics. They cannot serve and are not retained in the snapshot.
        providers.retain(|_, provider| provider.spec.is_some());
        overlay_removed_models.retain(|provider_id, _| providers.contains_key(provider_id));
        let providers = providers
            .into_iter()
            .map(|(id, provider)| {
                (
                    id,
                    ServingProvider {
                        spec: provider.spec.expect("pending providers were removed"),
                        models: provider.models,
                    },
                )
            })
            .collect();

        Ok((
            Self {
                providers,
                overlay_removed_models,
                withdrawn_models,
            },
            model_identities,
        ))
    }

    fn from_catalog(catalog: &Catalog) -> Self {
        let mut providers = BTreeMap::new();
        for (provider_id, (provider_family, _)) in &catalog.resolved_family {
            let Some(entry) = catalog.providers.get(provider_id) else {
                continue;
            };
            let Some(model_entries) = entry.get("models").and_then(Value::as_object) else {
                continue;
            };
            let mut spec = catalog
                .provider(provider_id)
                .expect("resolved catalog provider has a provider spec");
            spec.raw = Value::Null;
            let mut models = BTreeMap::new();
            for model_id in model_entries.keys() {
                let Some((model_provider, model)) =
                    catalog.model_for_serving(provider_id, model_id)
                else {
                    continue;
                };
                models.insert(
                    model_id.clone(),
                    ServingModel::from_spec(&model, model_provider.wire_family, *provider_family),
                );
            }
            providers.insert(provider_id.clone(), ServingProvider { spec, models });
        }

        let mut overlay_removed_models = catalog.overlay_removed_models.clone();
        overlay_removed_models.retain(|provider_id, _| providers.contains_key(provider_id));
        let mut withdrawn_models: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for (provider_id, model_id) in &catalog.withdrawn_models {
            withdrawn_models
                .entry(provider_id.clone())
                .or_default()
                .insert(model_id.clone());
        }
        Self {
            providers,
            overlay_removed_models,
            withdrawn_models,
        }
    }

    fn provider_and_model(
        &self,
        provider_id: &str,
        model_id: &str,
    ) -> Option<(&ServingProvider, &ServingModel)> {
        let provider = self.providers.get(provider_id)?;
        Some((provider, provider.models.get(model_id)?))
    }

    fn contains_model(&self, provider_id: &str, model_id: &str) -> bool {
        self.provider_and_model(provider_id, model_id).is_some()
    }

    fn wire_family(&self, provider_id: &str, model_id: &str) -> Option<WireFamilyId> {
        let (provider, model) = self.provider_and_model(provider_id, model_id)?;
        Some(model.wire_family.unwrap_or(provider.spec.wire_family))
    }

    fn context_limit(&self, provider_id: &str, model_id: &str) -> Option<u64> {
        self.provider_and_model(provider_id, model_id)
            .and_then(|(_, model)| model.context_limit)
    }

    fn output_limit(&self, provider_id: &str, model_id: &str) -> Option<u64> {
        self.provider_and_model(provider_id, model_id)
            .and_then(|(_, model)| model.output_limit)
    }

    fn model_for_serving(
        &self,
        provider_id: &str,
        model_id: &str,
    ) -> Option<(ProviderSpec, ModelSpec)> {
        let (provider, model) = self.provider_and_model(provider_id, model_id)?;
        let family = model.wire_family.unwrap_or(provider.spec.wire_family);
        let provider_spec = if family == provider.spec.wire_family {
            provider.spec.clone()
        } else {
            serving_provider_spec(provider_id, family, provider.spec.base_url.clone())
        };
        Some((provider_spec, model.to_spec(model_id)))
    }

    fn is_withdrawn(&self, provider_id: &str, model_id: &str) -> bool {
        self.withdrawn_models
            .get(provider_id)
            .is_some_and(|models| models.contains(model_id))
    }

    fn is_overlay_removed(&self, provider_id: &str, model_id: &str) -> bool {
        self.overlay_removed_models
            .get(provider_id)
            .is_some_and(|models| models.contains(model_id))
    }
}

fn serving_provider_spec(
    provider_id: &str,
    family: WireFamilyId,
    base_url: String,
) -> ProviderSpec {
    ProviderSpec {
        id: provider_id.to_string(),
        wire_family: family,
        auth_kind: crate::default_auth_kind(family),
        static_headers: Default::default(),
        static_options: crate::provider_static_options(provider_id, family),
        quirks: crate::provider_quirks(provider_id, family, &base_url),
        base_url,
        raw: Value::Null,
    }
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeOverlay {
    #[serde(default)]
    patch: BTreeMap<String, RuntimeProviderOverlay>,
    #[serde(default)]
    add: BTreeMap<String, RuntimeProviderOverlay>,
    #[serde(default)]
    remove: BTreeMap<String, Vec<String>>,
}

impl RuntimeOverlay {
    fn parse() -> Result<Self, CatalogError> {
        serde_json::from_str(OVERLAY).map_err(|error| CatalogError::OverlayParse(error.to_string()))
    }

    fn removes(&self, provider_id: &str, model_id: &str) -> bool {
        self.remove
            .get(provider_id)
            .is_some_and(|models| models.iter().any(|candidate| candidate == model_id))
    }

    fn model_patch(&self, provider_id: &str, model_id: &str) -> Option<&RuntimeModelOverlay> {
        self.patch
            .get(provider_id)
            .and_then(|provider| provider.models.get(model_id))
    }
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeProviderOverlay {
    #[serde(default, rename = "base_url_source")]
    _base_url_source: Option<String>,
    #[serde(default, rename = "wire_family_source")]
    _wire_family_source: Option<String>,
    #[serde(default, rename = "wire_family_reason")]
    _wire_family_reason: Option<String>,
    #[serde(default)]
    base_url: Patch<Option<String>>,
    #[serde(default)]
    wire_family: Patch<Option<WireFamilyId>>,
    #[serde(default)]
    models: BTreeMap<String, RuntimeModelOverlay>,
}

impl RuntimeProviderOverlay {
    fn family(&self) -> Option<WireFamilyId> {
        match self.wire_family {
            Patch::Set(family) => family,
            Patch::Missing => None,
        }
    }

    fn base_url(&self) -> String {
        match &self.base_url {
            Patch::Set(Some(base_url)) => base_url.clone(),
            Patch::Missing | Patch::Set(None) => String::new(),
        }
    }

    fn serving_provider(
        overlay: Option<&Self>,
        provider_id: &str,
        family: Option<WireFamilyId>,
    ) -> DraftServingProvider {
        DraftServingProvider {
            spec: family.map(|family| {
                serving_provider_spec(
                    provider_id,
                    family,
                    overlay.map(Self::base_url).unwrap_or_default(),
                )
            }),
            models: BTreeMap::new(),
        }
    }
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeModelOverlay {
    #[serde(default, rename = "id")]
    _id: Option<String>,
    #[serde(default)]
    name: Patch<Option<String>>,
    #[serde(default)]
    cost: Patch<Option<RuntimeCostOverlay>>,
    #[serde(default)]
    attachment: Patch<CapabilityFlag>,
    #[serde(default)]
    reasoning: Patch<CapabilityFlag>,
    #[serde(default)]
    tool_call: Patch<CapabilityFlag>,
    /// The overlay spells this capability as its other capability patches are spelled, the
    /// models.dev way. It sets `capability.structured_output` for a model the overlay adds or
    /// must correct; fusiform's fact is used for every other row.
    #[serde(default)]
    structured_output: Patch<CapabilityFlag>,
    #[serde(default)]
    wire_family: Patch<Option<WireFamilyId>>,
    #[serde(default)]
    limit: Patch<Option<RuntimeLimitOverlay>>,
}

impl RuntimeModelOverlay {
    fn apply(&self, model: &mut ServingModel) {
        if let Patch::Set(name) = &self.name {
            model.display_name = name.clone().unwrap_or_default();
        }
        if let Patch::Set(cost) = &self.cost {
            let value = cost.as_ref().map(|cost| serde_json::json!(cost));
            model.cost = crate::map_cost(value.as_ref());
        }
        if let Patch::Set(value) = self.attachment {
            model.attachment = value;
        }
        if let Patch::Set(value) = self.reasoning {
            model.reasoning = value;
        }
        if let Patch::Set(value) = self.tool_call {
            model.tool_call = value;
        }
        if let Patch::Set(value) = self.structured_output {
            model.structured_output = value;
        }
        if let Patch::Set(value) = self.wire_family {
            model.wire_family = value;
        }
        match &self.limit {
            Patch::Set(None) => {
                model.context_limit = None;
                model.output_limit = None;
            }
            Patch::Set(Some(limit)) => {
                if let Patch::Set(context) = limit.context {
                    model.context_limit = context;
                }
                if let Patch::Set(output) = limit.output {
                    model.output_limit = output;
                }
            }
            Patch::Missing => {}
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RuntimeCostOverlay {
    input: Option<f64>,
    output: Option<f64>,
    cache_read: Option<f64>,
    cache_write: Option<f64>,
    reasoning: Option<f64>,
    input_audio: Option<f64>,
    output_audio: Option<f64>,
    tiers: Option<Vec<RuntimeCostTierOverlay>>,
    // The shipped overlay also records the same context rates under this legacy label.
    // Only the explicit tiers array determines the serving schedule.
    #[serde(default, rename = "context_over_200k", skip_serializing)]
    _context_over_200k: Option<Value>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RuntimeCostTierOverlay {
    input: Option<f64>,
    output: Option<f64>,
    cache_read: Option<f64>,
    cache_write: Option<f64>,
    tier: RuntimeCostTierThreshold,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RuntimeCostTierThreshold {
    size: u64,
    #[serde(rename = "type")]
    kind: String,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeLimitOverlay {
    #[serde(default)]
    context: Patch<Option<u64>>,
    #[serde(default)]
    output: Patch<Option<u64>>,
}

#[derive(Default)]
enum Patch<T> {
    #[default]
    Missing,
    Set(T),
}

impl<'de, T> Deserialize<'de> for Patch<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        T::deserialize(deserializer).map(Self::Set)
    }
}

/// Read one capability fact, distinguishing "not stated" from "missing".
///
/// An explicit null is the producer saying the upstream does not state this, which is a fact
/// about knowledge. An absent key is a fact that should have been there and was not, and it is
/// an error rather than a default: defaulting to false would silently stop a capable model
/// reasoning, and nothing downstream would report anything -- the run would simply be worse.
///
/// KNOW THE BLAST RADIUS BEFORE RELAXING THIS. The error propagates out of `from_fusiform`, so
/// ONE malformed model refuses the WHOLE catalog -- currently about 6,300 of them. The daemon
/// then keeps serving its last-known-good, which is complete and correct as of the last accepted
/// poll, so nothing that worked stops working; what stops is new models appearing. Since the
/// refusal repeats while the upstream serves that row, `catalog_refusals` on `health.check`
/// surfaces it within the hour.
///
/// That all-or-nothing shape is deliberate. Dropping just the bad model would keep the catalog
/// fresh, but a systematic upstream fault would then shrink the serving set quietly while every
/// reading looked healthy -- trading a loud whole-catalog stall for a silent partial one, which
/// is the worse direction and the same size-as-proxy-for-content failure in a new place.
fn capability_fact(
    identity: &str,
    facts: &BTreeMap<String, Value>,
    fact: &'static str,
) -> Result<CapabilityFlag, CatalogError> {
    match facts.get(fact) {
        Some(Value::Bool(value)) => Ok(CapabilityFlag::Known(*value)),
        Some(Value::Null) => Ok(CapabilityFlag::Unknown),
        Some(value) => Err(CatalogError::FusiformFact {
            model: identity.to_string(),
            fact: fact.to_string(),
            reason: format!("expected boolean or null, found {}", value_kind(value)),
        }),
        None => Err(CatalogError::FusiformFact {
            model: identity.to_string(),
            fact: fact.to_string(),
            reason: "fact is absent".to_string(),
        }),
    }
}

/// A capability fact fusiform may omit: absence reads as [`CapabilityFlag::Absent`] (the
/// catalog does not say), never as a refusal of the row. A present value must still be a
/// boolean or null.
fn optional_capability_fact(
    identity: &str,
    facts: &BTreeMap<String, Value>,
    fact: &'static str,
) -> Result<CapabilityFlag, CatalogError> {
    if facts.contains_key(fact) {
        capability_fact(identity, facts, fact)
    } else {
        Ok(CapabilityFlag::Absent)
    }
}

/// `capability.reasoning_options`, served verbatim from models.dev. Unlike the capability flags
/// this fact is optional: a fusiform from before it existed does not serve it, and that reads
/// as unstated (no variants) rather than refusing the whole catalog.
fn reasoning_options_fact(
    identity: &str,
    facts: &BTreeMap<String, Value>,
) -> Result<ReasoningOptions, CatalogError> {
    match facts.get("capability.reasoning_options") {
        None => Ok(ReasoningOptions::Unstated),
        Some(value) => {
            ReasoningOptions::from_value(value).ok_or_else(|| CatalogError::FusiformFact {
                model: identity.to_string(),
                fact: "capability.reasoning_options".to_string(),
                reason: format!("expected array or null, found {}", value_kind(value)),
            })
        }
    }
}

/// The modes this row has a priced input AND output rate for, from `rate.input.mode.<mode>`
/// and `rate.output.mode.<mode>`.
///
/// An absent mode rate means UNPRICED. It is never filled from the base `rate.input` /
/// `rate.output`: a tier is priced by its own rate (fast mode is about 2x the base), so borrowing
/// the base would under-price every step served at it. The reverse also holds: a row may carry
/// mode rates with no base rate, and its modes are priced all the same.
///
/// A rate counts as priced when its `state` is `priced` or `stated_zero` (the source states the
/// price is zero). `unpriced`, a null, or any state this build does not know is unpriced, so an
/// unfamiliar value refuses a tier rather than offering one routing cannot price. A value that
/// is neither null nor an object is malformed and refuses the catalog, like every other fact.
fn priced_modes_fact(
    identity: &str,
    facts: &BTreeMap<String, Value>,
) -> Result<Vec<String>, CatalogError> {
    let rate_is_priced = |key: &str| -> Result<bool, CatalogError> {
        match facts.get(key) {
            None | Some(Value::Null) => Ok(false),
            Some(Value::Object(rate)) => Ok(matches!(
                rate.get("state").and_then(Value::as_str),
                Some("priced" | "stated_zero")
            )),
            Some(value) => Err(CatalogError::FusiformFact {
                model: identity.to_string(),
                fact: key.to_string(),
                reason: format!("expected rate object or null, found {}", value_kind(value)),
            }),
        }
    };
    let modes: BTreeSet<&str> = facts
        .keys()
        .filter_map(|key| {
            key.strip_prefix(MODE_RATE_INPUT_PREFIX)
                .or_else(|| key.strip_prefix(MODE_RATE_OUTPUT_PREFIX))
        })
        .filter(|mode| !mode.is_empty())
        .collect();
    let mut priced = Vec::new();
    for mode in modes {
        let input = rate_is_priced(&format!("{MODE_RATE_INPUT_PREFIX}{mode}"))?;
        let output = rate_is_priced(&format!("{MODE_RATE_OUTPUT_PREFIX}{mode}"))?;
        if input && output {
            priced.push(mode.to_string());
        }
    }
    Ok(priced)
}

fn context_fact(
    identity: &str,
    facts: &BTreeMap<String, Value>,
) -> Result<Option<u64>, CatalogError> {
    match facts.get("limit.context") {
        Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_u64()
            .map(Some)
            .ok_or_else(|| CatalogError::FusiformFact {
                model: identity.to_string(),
                fact: "limit.context".to_string(),
                reason: format!(
                    "expected unsigned integer or null, found {}",
                    value_kind(value)
                ),
            }),
        None => Err(CatalogError::FusiformFact {
            model: identity.to_string(),
            fact: "limit.context".to_string(),
            reason: "fact is absent".to_string(),
        }),
    }
}

/// Read `limit.output`. Null and absent both mean "unknown" and decode to `None`, never to a
/// number: fusiform collapses an upstream zero to null, so a synthesised value here would be a
/// cap nobody published.
///
/// Absent is tolerated, unlike `limit.context`, because this fact only chooses a default and has
/// a documented fallback; an LKG written before broca requested the fact carries none, and it
/// must keep loading rather than refuse the whole catalog. A present value of the wrong type is
/// still an error, for the same all-or-nothing reason as the capability facts.
fn output_fact(
    identity: &str,
    facts: &BTreeMap<String, Value>,
) -> Result<Option<u64>, CatalogError> {
    match facts.get("limit.output") {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_u64()
            .map(Some)
            .ok_or_else(|| CatalogError::FusiformFact {
                model: identity.to_string(),
                fact: "limit.output".to_string(),
                reason: format!(
                    "expected unsigned integer or null, found {}",
                    value_kind(value)
                ),
            }),
    }
}

/// A typed admission refusal used only when neither memory nor the configured LKG has a catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogUnavailable {
    lkg_path: Option<PathBuf>,
}

impl CatalogUnavailable {
    pub fn lkg_path(&self) -> Option<&Path> {
        self.lkg_path.as_deref()
    }
}

impl std::fmt::Display for CatalogUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.lkg_path {
            Some(path) => write!(
                f,
                "model catalog unavailable: no in-memory fusiform catalog and no last-known-good catalog at {}",
                path.display()
            ),
            None => write!(
                f,
                "model catalog unavailable: no in-memory fusiform catalog and no last-known-good catalog is configured"
            ),
        }
    }
}

impl std::error::Error for CatalogUnavailable {}

#[derive(Debug)]
pub enum CatalogRefreshError {
    Catalog(CatalogError),
    Serialize(String),
    Persist {
        path: PathBuf,
        message: String,
    },
    /// `include_retired` makes legitimate withdrawals remain explicit rows, so identities that
    /// vanish indicate an incomplete/degraded response rather than a withdrawal.
    ///
    /// Carries the response's `resolved_at_ms` because that field is the DISCRIMINATOR between
    /// the two causes, and a refusal naming only the missing count cannot tell them apart:
    /// an instant near now means the producer's own current view shrank (a restore, or a
    /// degraded response), while an instant in the past means this refresh asked for a
    /// historical snapshot and got a smaller identity set legitimately.
    ///
    /// The distinction decides who acts. Only one of them is broca's to fix.
    Incomplete {
        missing_count: usize,
        sample: String,
        resolved_at_ms: i64,
    },
    /// A response older than the one already held. Refusing is not an error condition: the
    /// response was valid when it was issued, it is just not news.
    Stale {
        installed: i64,
        received: i64,
    },
}

impl std::fmt::Display for CatalogRefreshError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Catalog(error) => write!(f, "assemble fusiform catalog: {error}"),
            Self::Serialize(error) => write!(f, "serialize catalog LKG: {error}"),
            Self::Stale {
                installed,
                received,
            } => write!(
                f,
                "fusiform response is older than the installed catalog \
                 (installed version {installed}, received {received})"
            ),
            Self::Persist { path, message } => {
                write!(f, "persist catalog LKG {}: {message}", path.display())
            }
            Self::Incomplete {
                missing_count,
                sample,
                resolved_at_ms,
            } => write!(
                f,
                "fusiform catalog response omitted {missing_count} previously known model identities \
                 (for example '{sample}'); retaining the prior complete catalog. \
                 The response resolved to {resolved_at_ms} -- if that instant is near now the \
                 producer's current view shrank, and if it is in the past this refresh asked for a \
                 historical snapshot"
            ),
        }
    }
}

impl std::error::Error for CatalogRefreshError {}

impl From<CatalogError> for CatalogRefreshError {
    fn from(value: CatalogError) -> Self {
        Self::Catalog(value)
    }
}

/// In-memory catalog state plus its durable last-known-good path.
///
/// Disk is read once by [`Self::open`] and written only by a successful background refresh.
/// [`Self::snapshot`] only takes a read lock and clones an `Arc`, which keeps admission independent
/// of fusiform and the filesystem.
pub struct CatalogCache {
    current: RwLock<Option<Arc<LoadedCatalog>>>,
    lkg_path: Option<PathBuf>,
    lkg_load_error: Option<String>,
    /// How many refreshes in a row have been refused, and why the last one was.
    ///
    /// A refusal is not necessarily transient. The completeness guard compares against the
    /// identities THIS CACHE HOLDS and a refused refresh does not install, so the comparison
    /// basis never advances and nothing about a later poll can differ -- the refusal repeats
    /// for as long as the producer serves the same content. Pinned by
    /// `a_newer_response_with_fewer_identities_is_refused_permanently`.
    ///
    /// Without this, that state is invisible where it matters: `catalog_age_ms` rises either
    /// way, so a poll that missed once and a poll that will never succeed again are the same
    /// reading with a different number.
    consecutive_refusals: RwLock<(u32, Option<String>)>,
    /// Refusals seen since this process started, which the streak above deliberately forgets.
    ///
    /// The streak is a CURRENT-STATE gauge: it resets on the first success, so it can only be
    /// read while the condition holds. The conditions it exists to catch -- a producer restore,
    /// a degraded response -- are rare AND self-clearing, so the likely reading is at 09:00
    /// after a 02:00 event that healed by 04:00, and the streak says zero with nothing to
    /// suggest anything happened.
    ///
    /// A total makes the pair discriminating: streak zero with a non-zero total is "this
    /// occurred and recovered", which is a different fact from "this has never occurred" and
    /// the one that decides whether to go looking at the producer's poll history.
    total_refusals: RwLock<u64>,
}

impl CatalogCache {
    /// Open a cache and eagerly load its LKG before admission can begin. A missing or malformed LKG
    /// does not prevent boot: the first successful poll can repair it, while admission receives a
    /// typed absence until then.
    pub fn open(lkg_path: impl Into<PathBuf>) -> Self {
        let lkg_path = lkg_path.into();
        let (current, lkg_load_error) = match load_lkg(&lkg_path) {
            Ok(Some(response)) => match loaded(&response, CatalogSource::LastKnownGood) {
                Ok(catalog) => (Some(Arc::new(catalog)), None),
                Err(error) => (None, Some(error.to_string())),
            },
            Ok(None) => (None, None),
            Err(error) => (None, Some(error)),
        };
        Self {
            current: RwLock::new(current),
            lkg_path: Some(lkg_path),
            lkg_load_error,
            consecutive_refusals: RwLock::new((0, None)),
            total_refusals: RwLock::new(0),
        }
    }

    /// An unconfigured empty cache, primarily useful for typed absence checks and lightweight
    /// adapters which do not run the real module startup path.
    pub fn empty() -> Self {
        Self {
            current: RwLock::new(None),
            lkg_path: None,
            lkg_load_error: None,
            consecutive_refusals: RwLock::new((0, None)),
            total_refusals: RwLock::new(0),
        }
    }

    pub fn lkg_load_error(&self) -> Option<&str> {
        self.lkg_load_error.as_deref()
    }

    /// Read the current immutable document without waiting, retrying, or performing IO.
    pub fn snapshot(&self) -> Result<CatalogSnapshot, CatalogUnavailable> {
        self.current
            .read()
            .expect("catalog cache lock poisoned")
            .as_ref()
            .cloned()
            .map(CatalogSnapshot)
            .ok_or_else(|| CatalogUnavailable {
                lkg_path: self.lkg_path.clone(),
            })
    }

    /// Replace the in-memory document after parsing it and atomically persist the exact fusiform
    /// response as the LKG. A persistence failure leaves the prior document in place, because a
    /// refresh that cannot survive restart is not yet last-known-good.
    pub fn refresh(&self, response: FusiformCatalogResponse) -> Result<(), CatalogRefreshError> {
        self.refresh_with_poll_summary(response, None)
    }

    /// Refresh the cache after a complete poll, carrying the poll start time and page count so the
    /// success record is emitted beside the refusal-counter update below.
    pub fn refresh_polled(
        &self,
        response: FusiformCatalogResponse,
        page_count: usize,
        poll_started: Instant,
    ) -> Result<(), CatalogRefreshError> {
        self.refresh_with_poll_summary(response, Some((page_count, poll_started)))
    }

    fn refresh_with_poll_summary(
        &self,
        response: FusiformCatalogResponse,
        poll_summary: Option<(usize, Instant)>,
    ) -> Result<(), CatalogRefreshError> {
        let catalog_version = response.catalog_version;
        let resolved_at_ms = response.resolved_at_ms;
        let model_count = response.models.len();
        let outcome = self.refresh_inner(response);
        // Recorded here rather than at each return so a future arm cannot forget it: the whole
        // point is distinguishing a poll that missed once from one that will never succeed.
        let mut refusals = self
            .consecutive_refusals
            .write()
            .expect("catalog refusal lock poisoned");
        match &outcome {
            Ok(()) => *refusals = (0, None),
            Err(error) => {
                *refusals = (refusals.0.saturating_add(1), Some(error.to_string()));
                let mut total = self
                    .total_refusals
                    .write()
                    .expect("catalog refusal lock poisoned");
                *total = total.saturating_add(1);
            }
        }
        drop(refusals);
        if outcome.is_ok() {
            if let Some((page_count, poll_started)) = poll_summary {
                let elapsed_ms =
                    u64::try_from(poll_started.elapsed().as_millis()).unwrap_or(u64::MAX);
                tracing::info!(
                    target: "catalog",
                    "fusiform catalog refresh succeeded pages={} catalog_version={} resolved_at_ms={} models={} elapsed_ms={}",
                    page_count,
                    catalog_version,
                    resolved_at_ms,
                    model_count,
                    elapsed_ms,
                );
            }
        }
        outcome
    }

    /// A poll that never REACHED the cache still failed to refresh the catalog.
    ///
    /// The counters below were written only inside `refresh`, so a transport failure --
    /// the poll dying before it has a document to offer -- left them at zero. On
    /// 2026-09-14 a stale-route storm kept this process on LKG for four hours while
    /// `catalog_refusals` and `catalog_refusals_total` both read 0 and only
    /// `catalog_age_ms` moved. A health surface that returns a REASSURING value through
    /// the exact failure it exists to report is worse than one that reports nothing.
    ///
    /// Deliberately the SAME counters rather than a parallel pair: the operator question
    /// is "is my catalog refreshing", and both causes answer no. The reason string is
    /// what distinguishes them, and the existing degrade rule then covers both.
    pub fn note_poll_failure(&self, reason: String) {
        let mut refusals = self
            .consecutive_refusals
            .write()
            .expect("catalog refusal lock poisoned");
        *refusals = (refusals.0.saturating_add(1), Some(reason));
        let mut total = self
            .total_refusals
            .write()
            .expect("catalog refusal lock poisoned");
        *total = total.saturating_add(1);
    }

    /// How many refreshes in a row have been refused, with the most recent reason.
    ///
    /// Zero is the healthy reading and is reported as such rather than omitted: a field that
    /// vanishes when things are fine cannot distinguish "refreshing cleanly" from "not
    /// instrumented".
    /// Refusals seen since process start, including ones that have since cleared.
    ///
    /// Read alongside the streak: zero streak with a non-zero total is "this happened and
    /// recovered", which the streak alone cannot say.
    pub fn total_refusals(&self) -> u64 {
        *self
            .total_refusals
            .read()
            .expect("catalog refusal lock poisoned")
    }

    pub fn consecutive_refusals(&self) -> (u32, Option<String>) {
        self.consecutive_refusals
            .read()
            .expect("catalog refusal lock poisoned")
            .clone()
    }

    fn refresh_inner(&self, response: FusiformCatalogResponse) -> Result<(), CatalogRefreshError> {
        // WHEN THIS CAN ACTUALLY FIRE, per fusiform's serve predicates rather than observation:
        // `uncertain` needs an era with `boundary_at_ms > at`, and `withheld` needs a correction
        // window with `affected_until_ms >= at`. A boundary is stamped when the poll that detected
        // the change ran, so it is always in the past, and `plan_correction` refuses unless the
        // era in force begins at or after the window ends. Broca's poller omits at_ms, so it always
        // asks `at = now` and both predicates are false by construction.
        //
        // TWO EXCEPTIONS, and the second is why this guard is not decoration. A read landing on the
        // exact millisecond where a correction's `affected_until` equals the current era's boundary
        // is a real but negligible coincidence. A BACKWARD CLOCK STEP is not: both predicates
        // compare stored timestamps against the system clock, so an NTP correction or a VM restore
        // moving time backward puts `now` before boundaries already written and re-arms both arms
        // for the length of the skew. Fusiform derives catalog_version as max(now_ms, current + 1)
        // precisely because backward steps happen here -- which keeps VERSIONS monotonic, so the
        // equal-version fast path below keeps firing and would route a qualified response straight
        // through the exit that used to have no guard.
        //
        // The invariant this defends is not "reject bad input". It is that REFRESH AND `open` MUST
        // AGREE ON WHAT A VALID CATALOG IS: `loaded` rejects qualified facts on every LKG load, so
        // any path here that persists one writes a file the next restart cannot read, and a
        // degraded poll becomes an empty catalog with every model-backed send failing.
        if !response.withheld.is_empty() || !response.uncertain.is_empty() {
            return Err(CatalogError::FusiformQualifiedFacts {
                withheld: response.withheld.len(),
                uncertain: response.uncertain.len(),
            }
            .into());
        }

        let installed = self
            .current
            .read()
            .expect("catalog cache lock poisoned")
            .as_ref()
            .cloned();

        // Decide on the version BEFORE materializing. Rebuilding the compact index for all
        // ~6,300 models costs 2.96ms and 1.38 MB of transient allocation (measured, release,
        // 100 fresh processes); comparing two integers costs 84ns. Building first and then
        // discovering the response is stale or unchanged pays the former to learn the latter.
        //
        // Ordering, not logic: the guards below are the same ones, moved ahead of the work they
        // gate. I put them after materialization when I added the version check, because that
        // is where the identity check already sat.
        if let Some(current) = &installed {
            if response.catalog_version < current.catalog_version {
                return Err(CatalogRefreshError::Stale {
                    installed: current.catalog_version,
                    received: response.catalog_version,
                });
            }
            if response.catalog_version == current.catalog_version {
                // Same version, so the facts cannot have changed and the existing index is
                // still correct — but only if the response is COMPLETE. A truncated or
                // degraded response can carry the version of the document it was truncated
                // from, and this path persists the LKG, so accepting one here would write a
                // short catalog to disk for the next restart to load. The in-memory index
                // would survive (it is reused, not rebuilt from the response) and the damage
                // would appear only after a restart, which is the worst time to find it.
                //
                // Checked against the response's own keys rather than a built index: the
                // identity set is just the map keys, so this costs a set difference and none
                // of the 2.96ms materialization.
                let missing: Vec<&String> = current
                    .model_identities
                    .iter()
                    .filter(|identity| !response.models.contains_key(*identity))
                    .collect();
                if let Some(sample) = missing.first() {
                    return Err(CatalogRefreshError::Incomplete {
                        missing_count: missing.len(),
                        sample: (*sample).clone(),
                        resolved_at_ms: response.resolved_at_ms,
                    });
                }
                // `loaded` rejects responses with withheld or uncertain facts. Keep that check
                // ahead of the version fast path: persisting a rejected response would leave an
                // LKG that `open` refuses after restart, even while the in-memory catalog keeps
                // serving its last accepted snapshot.
                if response_carries_output_limits(&response) != current.carries_output_limits
                    || response_carries_mode_rates(&response) != current.carries_mode_rates
                    || response_carries_warm_rates(&response) != current.carries_warm_rates
                {
                    let rebuilt = Arc::new(loaded(&response, CatalogSource::Fusiform)?);
                    if let Some(path) = &self.lkg_path {
                        persist_lkg(path, &response)?;
                    }
                    *self.current.write().expect("catalog cache lock poisoned") = Some(rebuilt);
                    return Ok(());
                }

                // Persist even though no fact changed: the LKG carries resolved_at_ms, and a
                // restart should recover the freshest proof that fusiform answered.
                if let Some(path) = &self.lkg_path {
                    persist_lkg(path, &response)?;
                }
                let refreshed = Arc::new(LoadedCatalog {
                    catalog: Arc::clone(&current.catalog),
                    model_identities: Arc::clone(&current.model_identities),
                    resolved_at_ms: response.resolved_at_ms,
                    catalog_version: current.catalog_version,
                    source: CatalogSource::Fusiform,
                    carries_output_limits: current.carries_output_limits,
                    carries_mode_rates: current.carries_mode_rates,
                    carries_warm_rates: current.carries_warm_rates,
                });
                *self.current.write().expect("catalog cache lock poisoned") = Some(refreshed);
                return Ok(());
            }
        }

        let next = Arc::new(loaded(&response, CatalogSource::Fusiform)?);
        if let Some(current) = installed {
            // Why the version guard above exists at all: `catalog_version` is the only field
            // fusiform documents as safe to hold as a high-water mark, derived as
            // max(now_ms, current + 1), so it is monotonic even across a restore of their
            // database, which `era_count` and `model_count` are not.
            //
            // The identity check below cannot stand in for it. With include_retired every model
            // stays present as a row, so a snapshot that merely disagrees about whether a model
            // EXISTS has the same identity set — an older response can flip a withdrawn model
            // back to present and pass, putting a model the newer catalog withdrew back on the
            // admission path.
            //
            // Reachable without any fusiform-side fault: the daemon and broca-run share one LKG
            // path, and persist_lkg is atomic per write but has no cross-process ordering. Two
            // pollers minutes apart both write and the last one wins, so a CLI invocation that
            // polled earlier can overwrite the daemon's newer snapshot on disk.
            let missing = current
                .model_identities
                .difference(&next.model_identities)
                .collect::<Vec<_>>();
            if let Some(sample) = missing.first() {
                return Err(CatalogRefreshError::Incomplete {
                    missing_count: missing.len(),
                    sample: (*sample).clone(),
                    resolved_at_ms: response.resolved_at_ms,
                });
            }
        }
        if let Some(path) = &self.lkg_path {
            persist_lkg(path, &response)?;
        }
        *self.current.write().expect("catalog cache lock poisoned") = Some(next);
        Ok(())
    }

    /// Install a document without touching disk. This is intended for focused tests that prove the
    /// admission path remains independent from a concurrently slow or failed poll.
    #[doc(hidden)]
    pub fn install_for_test(
        &self,
        response: FusiformCatalogResponse,
        source: CatalogSource,
    ) -> Result<(), CatalogRefreshError> {
        let next = Arc::new(loaded(&response, source)?);
        *self.current.write().expect("catalog cache lock poisoned") = Some(next);
        Ok(())
    }

    /// Install an already assembled catalog for tests which exercise legacy snapshot fixtures.
    #[doc(hidden)]
    pub fn install_parsed_for_test(
        &self,
        catalog: &Catalog,
        resolved_at_ms: i64,
        source: CatalogSource,
    ) {
        let next = Arc::new(LoadedCatalog {
            catalog: Arc::new(ServingCatalog::from_catalog(catalog)),
            model_identities: Arc::new(BTreeSet::new()),
            resolved_at_ms,
            catalog_version: 0,
            source,
            carries_output_limits: true,
            carries_mode_rates: false,
            carries_warm_rates: true,
        });
        *self.current.write().expect("catalog cache lock poisoned") = Some(next);
    }

    pub fn health(&self, now_ms: i64) -> Result<CatalogHealth, CatalogUnavailable> {
        let snapshot = self.snapshot()?;
        let (refusals, reason) = self.consecutive_refusals();
        Ok(CatalogHealth {
            age_ms: now_ms.saturating_sub(snapshot.resolved_at_ms()).max(0),
            source: snapshot.source(),
            consecutive_refusals: refusals,
            refusal_reason: reason,
            total_refusals: self.total_refusals(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogHealth {
    pub age_ms: i64,
    pub source: CatalogSource,
    /// Refreshes refused in a row, and why the last one was.
    ///
    /// `age_ms` alone cannot carry this: it rises whether a poll missed once or will never
    /// succeed again, so thirty minutes and three weeks are the same reading with a different
    /// number. A non-zero count with a stable reason is the second state.
    pub consecutive_refusals: u32,
    pub refusal_reason: Option<String>,
    /// Refusals since process start, including cleared ones. Zero streak with a non-zero total
    /// is "this occurred and recovered" -- a different fact from "this has never occurred".
    pub total_refusals: u64,
}

fn loaded(
    response: &FusiformCatalogResponse,
    source: CatalogSource,
) -> Result<LoadedCatalog, CatalogError> {
    if !response.withheld.is_empty() || !response.uncertain.is_empty() {
        return Err(CatalogError::FusiformQualifiedFacts {
            withheld: response.withheld.len(),
            uncertain: response.uncertain.len(),
        });
    }
    let (catalog, model_identities) = ServingCatalog::from_fusiform(&response.models)?;
    Ok(LoadedCatalog {
        catalog: Arc::new(catalog),
        model_identities: Arc::new(model_identities),
        resolved_at_ms: response.resolved_at_ms,
        catalog_version: response.catalog_version,
        source,
        carries_output_limits: response_carries_output_limits(response),
        carries_mode_rates: response_carries_mode_rates(response),
        carries_warm_rates: response_carries_warm_rates(response),
    })
}

#[cfg(test)]
fn fusiform_models_to_base(
    response: &FusiformCatalogResponse,
) -> Result<(Value, BTreeSet<(String, String)>), CatalogError> {
    let mut providers = Map::new();
    let mut withdrawn = BTreeSet::new();
    for (identity, facts) in &response.models {
        let (provider_id, model_id) = identity
            .split_once('/')
            .filter(|(provider, model)| !provider.is_empty() && !model.is_empty())
            .ok_or_else(|| CatalogError::FusiformModelIdentity(identity.clone()))?;
        // Fusiform's existence vocabulary is a closed two-state value: "present" or
        // "absent" (`Existence` in fusiform-store/src/ingest.rs). "Absent" is deliberately
        // not "deleted" or "unavailable" -- those would be claims about the provider, and
        // fusiform only observed that a row stopped appearing upstream.
        //
        // "retired" and "withdrawn" are accepted alongside it because they are what an
        // earlier reading of the contract expected, and treating an unrecognised state as
        // fatal is the right default for a fact that decides whether a model serves.
        match facts.get("existence") {
            Some(Value::String(value)) if value == "present" => {}
            Some(Value::String(value))
                if value == "absent" || value == "retired" || value == "withdrawn" =>
            {
                withdrawn.insert((provider_id.to_string(), model_id.to_string()));
                continue;
            }
            Some(Value::String(value)) => {
                return Err(CatalogError::FusiformFact {
                    model: identity.clone(),
                    fact: "existence".to_string(),
                    reason: format!("unknown existence state '{value}'"),
                });
            }
            Some(value) => {
                return Err(CatalogError::FusiformFact {
                    model: identity.clone(),
                    fact: "existence".to_string(),
                    reason: format!("expected string, found {}", value_kind(value)),
                });
            }
            None => {
                return Err(CatalogError::FusiformFact {
                    model: identity.clone(),
                    fact: "existence".to_string(),
                    reason: "fact is absent".to_string(),
                });
            }
        }

        let provider = providers
            .entry(provider_id.to_string())
            .or_insert_with(|| serde_json::json!({ "id": provider_id, "models": {} }));
        let models = provider
            .get_mut("models")
            .and_then(Value::as_object_mut)
            .expect("new provider has an object model map");
        let mut model = Map::new();
        model.insert("id".to_string(), Value::String(model_id.to_string()));

        for (fact, field) in [
            ("capability.attachment", "attachment"),
            ("capability.reasoning", "reasoning"),
            ("capability.tool_call", "tool_call"),
        ] {
            let value = facts.get(fact).ok_or_else(|| CatalogError::FusiformFact {
                model: identity.clone(),
                fact: fact.to_string(),
                reason: "fact is absent".to_string(),
            })?;
            if !value.is_boolean() && !value.is_null() {
                return Err(CatalogError::FusiformFact {
                    model: identity.clone(),
                    fact: fact.to_string(),
                    reason: format!("expected boolean or null, found {}", value_kind(value)),
                });
            }
            model.insert(field.to_string(), value.clone());
        }
        // Optional, unlike the flags above: absent means the catalog does not say.
        if let Some(value) = facts.get("capability.structured_output") {
            model.insert("structured_output".to_string(), value.clone());
        }
        let context = facts
            .get("limit.context")
            .ok_or_else(|| CatalogError::FusiformFact {
                model: identity.clone(),
                fact: "limit.context".to_string(),
                reason: "fact is absent".to_string(),
            })?;
        if context.as_u64().is_none() && !context.is_null() {
            return Err(CatalogError::FusiformFact {
                model: identity.clone(),
                fact: "limit.context".to_string(),
                reason: format!(
                    "expected unsigned integer or null, found {}",
                    value_kind(context)
                ),
            });
        }
        let mut limit = serde_json::json!({ "context": context });
        if let Some(output) = facts.get("limit.output").filter(|value| !value.is_null()) {
            limit["output"] = output.clone();
        }
        model.insert("limit".to_string(), limit);
        models.insert(model_id.to_string(), Value::Object(model));
    }
    Ok((Value::Object(providers), withdrawn))
}

/// Drop dynamic overlay operations whose serveable targets are already absent upstream. Retired
/// identities remain separately recorded, so a historical `remove` may already be satisfied, and
/// a provider with no present models has nothing for a pin-only `patch` to modify. Colliding `add`
/// entries remain errors because silently changing an authored provider into a patch would alter
/// ownership of its serving facts.
#[cfg(test)]
fn runtime_overlay(base: &Value) -> Result<Value, CatalogError> {
    let mut overlay: Value = serde_json::from_str(OVERLAY)
        .map_err(|error| CatalogError::OverlayParse(error.to_string()))?;
    let base = base.as_object().expect("fusiform base is an object");
    let Some(overlay_object) = overlay.as_object_mut() else {
        return Ok(overlay);
    };
    if let Some(patch) = overlay_object
        .get_mut("patch")
        .and_then(Value::as_object_mut)
    {
        patch.retain(|provider, _| base.contains_key(provider));
    }
    if let Some(remove) = overlay_object
        .get_mut("remove")
        .and_then(Value::as_object_mut)
    {
        remove.retain(|provider, models| {
            let Some(base_models) = base
                .get(provider)
                .and_then(|entry| entry.get("models"))
                .and_then(Value::as_object)
            else {
                return false;
            };
            if let Some(models) = models.as_array_mut() {
                models.retain(|model| {
                    model
                        .as_str()
                        .is_some_and(|model| base_models.contains_key(model))
                });
                !models.is_empty()
            } else {
                true
            }
        });
    }
    Ok(overlay)
}

#[cfg(test)]
fn legacy_catalog(response: &FusiformCatalogResponse) -> Result<Catalog, CatalogError> {
    let (base, withdrawn_models) = fusiform_models_to_base(response)?;
    let overlay = runtime_overlay(&base)?;
    let mut catalog = Catalog::from_json_str(&base.to_string(), &overlay.to_string())?;
    catalog.withdrawn_models = withdrawn_models;
    catalog.metadata = None;
    Ok(catalog)
}

fn load_lkg(path: &Path) -> Result<Option<FusiformCatalogResponse>, String> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("read {}: {error}", path.display())),
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| format!("decode {}: {error}", path.display()))
}

fn persist_lkg(path: &Path, response: &FusiformCatalogResponse) -> Result<(), CatalogRefreshError> {
    let bytes = serde_json::to_vec(response)
        .map_err(|error| CatalogRefreshError::Serialize(error.to_string()))?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let create_parent = broca_owner_only::create_dir_all(parent);
    create_parent.map_err(|error| CatalogRefreshError::Persist {
        path: path.to_path_buf(),
        message: error.to_string(),
    })?;
    let temp = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("catalog-lkg"),
        std::process::id()
    ));
    let write_result = (|| {
        let mut options = OpenOptions::new();
        options.create(true).truncate(true).write(true);
        let mut file = options.open(&temp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temp, path)
    })();
    if let Err(error) = write_result {
        let _ = fs::remove_file(&temp);
        return Err(CatalogRefreshError::Persist {
            path: path.to_path_buf(),
            message: error.to_string(),
        });
    }
    Ok(())
}

fn value_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

static GLOBAL_CACHE: OnceLock<RwLock<Arc<CatalogCache>>> = OnceLock::new();

fn global_slot() -> &'static RwLock<Arc<CatalogCache>> {
    GLOBAL_CACHE.get_or_init(|| RwLock::new(Arc::new(CatalogCache::empty())))
}

/// Replace the process catalog cache during module startup, after its LKG has been loaded.
pub fn install_global_catalog_cache(cache: Arc<CatalogCache>) {
    *global_slot().write().expect("global catalog lock poisoned") = cache;
}

pub fn global_catalog_cache() -> Arc<CatalogCache> {
    global_slot()
        .read()
        .expect("global catalog lock poisoned")
        .clone()
}

/// Read the process catalog snapshot. This is an `Arc` clone and never performs IO.
pub fn catalog() -> Result<CatalogSnapshot, CatalogUnavailable> {
    global_catalog_cache().snapshot()
}

fn response_carries_warm_rates(response: &FusiformCatalogResponse) -> bool {
    response.models.values().any(|facts| {
        [
            "rate.input",
            "rate.output",
            "rate.cache_read",
            "rate.cache_write",
        ]
        .iter()
        .any(|base| {
            facts.contains_key(*base)
                || facts
                    .keys()
                    .any(|key| key.starts_with(&format!("{base}.above_context.")))
        })
    })
}

/// Rates are independent optional facts. An unfamiliar state, currency, or malformed amount
/// makes that price unavailable for maintenance, without preventing ordinary model admission.
fn warm_rate(value: Option<&Value>) -> Option<f64> {
    let rate = value?.as_object()?;
    if rate.get("currency")?.as_str()? != "USD"
        || !matches!(rate.get("state")?.as_str()?, "priced" | "stated_zero")
    {
        return None;
    }
    let units = rate.get("units")?.as_u64()?;
    let exponent = rate.get("exponent")?.as_u64()?;
    if exponent > 18 {
        return None;
    }
    if rate.get("state")?.as_str()? == "stated_zero" && units != 0 {
        return None;
    }
    Some(units as f64 / 10f64.powi(exponent as i32))
}

fn warm_cost_fact(facts: &BTreeMap<String, Value>) -> CostSchedule {
    let bases = [
        "rate.input",
        "rate.output",
        "rate.cache_read",
        "rate.cache_write",
    ];
    let thresholds: BTreeSet<u64> = facts
        .keys()
        .filter_map(|key| {
            bases
                .iter()
                .find_map(|base| key.strip_prefix(&format!("{base}.above_context.")))?
                .parse::<u64>()
                .ok()?
                .checked_add(1)
        })
        .collect();
    // Fusiform says above_context, while CostTier says at least min_context. Advancing the
    // threshold by one preserves the base price at the published boundary exactly.
    let tiers = thresholds
        .into_iter()
        .map(|min_context| {
            let threshold = min_context - 1;
            let rate = |base| warm_rate(facts.get(&format!("{base}.above_context.{threshold}")));
            CostTier {
                min_context,
                input: rate(bases[0]),
                output: rate(bases[1]),
                cache_read: rate(bases[2]),
                cache_write: rate(bases[3]),
            }
        })
        .collect();
    CostSchedule {
        input: warm_rate(facts.get(bases[0])),
        output: warm_rate(facts.get(bases[1])),
        cache_read: warm_rate(facts.get(bases[2])),
        cache_write: warm_rate(facts.get(bases[3])),
        tiers,
        ..CostSchedule::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_provider::families::{renderer_version_for, resolve_frozen};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn response(resolved_at_ms: i64) -> FusiformCatalogResponse {
        FusiformCatalogResponse {
            source: "models.dev".into(),
            resolved_at_ms,
            catalog_version: 9,
            models: BTreeMap::from([
                (
                    "anthropic/claude-sonnet-4-5".into(),
                    BTreeMap::from([
                        ("existence".into(), serde_json::json!("present")),
                        ("limit.context".into(), serde_json::json!(200000)),
                        ("capability.attachment".into(), serde_json::Value::Null),
                        ("capability.reasoning".into(), serde_json::json!(true)),
                        ("capability.tool_call".into(), serde_json::json!(true)),
                    ]),
                ),
                (
                    "anthropic/claude-retired".into(),
                    BTreeMap::from([("existence".into(), serde_json::json!("retired"))]),
                ),
            ]),
            withheld: Vec::new(),
            uncertain: Vec::new(),
        }
    }

    /// Fusiform's `capability.structured_output` maps like the other capability facts, except
    /// that an omitted fact reads as the catalog not saying rather than refusing the row.
    #[test]
    fn structured_output_fact_maps_to_a_three_state_capability() {
        for (fact, expected) in [
            (Some(serde_json::json!(true)), CapabilityFlag::Known(true)),
            (Some(serde_json::json!(false)), CapabilityFlag::Known(false)),
            (Some(serde_json::Value::Null), CapabilityFlag::Unknown),
            (None, CapabilityFlag::Absent),
        ] {
            let mut response = response(10);
            let facts = response
                .models
                .get_mut("anthropic/claude-sonnet-4-5")
                .unwrap();
            if let Some(fact) = fact.clone() {
                facts.insert("capability.structured_output".into(), fact);
            }
            let (catalog, _) = ServingCatalog::from_fusiform_with_overlay(
                &response.models,
                RuntimeOverlay::default(),
            )
            .unwrap();
            let (_, model) = catalog
                .model_for_serving("anthropic", "claude-sonnet-4-5")
                .unwrap();
            assert_eq!(model.capabilities.structured_output, expected, "{fact:?}");
        }
        let mut response = response(10);
        response
            .models
            .get_mut("anthropic/claude-sonnet-4-5")
            .unwrap()
            .insert(
                "capability.structured_output".into(),
                serde_json::json!("yes"),
            );
        assert!(ServingCatalog::from_fusiform_with_overlay(
            &response.models,
            RuntimeOverlay::default()
        )
        .is_err());
        assert!(FUSIFORM_FACT_PREFIXES.contains(&"capability.structured_output"));
    }

    #[test]
    fn legacy_overridden_member_is_ignored() {
        let mut legacy = serde_json::to_value(response(100)).expect("serialize response");
        legacy.as_object_mut().expect("response object").insert(
            "overridden".into(),
            serde_json::json!([{
                "provider_id": "anthropic",
                "model_id": "claude-sonnet-4-5",
                "fact_key": "limit.context",
                "upstream_value": "1000000",
                "served_value": "200000",
                "authority": "https://example.invalid"
            }]),
        );

        let decoded: FusiformCatalogResponse =
            serde_json::from_value(legacy).expect("legacy catalog fields are ignored");
        assert_eq!(decoded.catalog_version, 9);
        assert_eq!(decoded.models.len(), 2);
    }

    #[test]
    fn compact_index_retains_the_cause_of_an_overlay_removed_identity() {
        let response: FusiformCatalogResponse = serde_json::from_str(include_str!(
            "../tests/fixtures/fusiform_catalog_get_live.json"
        ))
        .expect("captured live fusiform response must deserialize");
        // The captured fixture (2026-08-13) still lists deepseek-chat; a test-local remove gives
        // the RECORDING of a removal a subject without the production overlay having to carry a
        // suppression for a row upstream no longer serves.
        let overlay: RuntimeOverlay = serde_json::from_str(
            r#"{ "patch": { "deepseek": { "wire_family": "openai_chat" } },
                 "remove": { "deepseek": ["deepseek-chat"] } }"#,
        )
        .expect("test overlay parses");
        let (catalog, _) = ServingCatalog::from_fusiform_with_overlay(&response.models, overlay)
            .expect("compact serving index materializes");

        assert!(
            catalog
                .model_for_serving("deepseek", "deepseek-chat")
                .is_none(),
            "the overlay-removed row must remain absent from the serving index"
        );
        assert!(catalog.is_overlay_removed("deepseek", "deepseek-chat"));
        assert!(!catalog.is_overlay_removed("deepseek", "no-such-model"));
        assert!(!catalog.is_withdrawn("deepseek", "deepseek-chat"));
    }

    #[test]
    fn compact_index_freezes_every_live_fixture_model_like_the_legacy_catalog() {
        let response: FusiformCatalogResponse = serde_json::from_str(include_str!(
            "../tests/fixtures/fusiform_catalog_get_live.json"
        ))
        .expect("captured live fusiform response must deserialize");
        assert_eq!(
            response.models.len(),
            6_328,
            "the all-model gate must not silently shrink with its fixture"
        );

        let legacy = legacy_catalog(&response).expect("legacy catalog materializes");
        let compact =
            loaded(&response, CatalogSource::Fusiform).expect("compact serving index materializes");
        let freeze = |pair: Option<(ProviderSpec, ModelSpec)>| {
            pair.map(|(provider, model)| {
                resolve_frozen(
                    &provider,
                    &model,
                    4_096,
                    renderer_version_for(provider.wire_family),
                )
                .map_err(|error| error.to_string())
            })
        };
        let consumed_model_facts = |pair: &Option<(ProviderSpec, ModelSpec)>| {
            pair.as_ref().map(|(_, model)| {
                (
                    model.limits.context,
                    model.limits.max_output,
                    model.capabilities.attachment,
                    model.capabilities.reasoning,
                    model.capabilities.tool_call,
                )
            })
        };

        let mut compared = 0usize;
        for identity in response.models.keys() {
            let (provider_id, model_id) = identity
                .split_once('/')
                .expect("fixture identities were validated by both materializers");
            let old_pair = legacy.model_for_serving(provider_id, model_id);
            let new_pair = compact.catalog.model_for_serving(provider_id, model_id);
            assert_eq!(
                consumed_model_facts(&new_pair),
                consumed_model_facts(&old_pair),
                "consumed model facts changed for {identity}"
            );
            assert_eq!(
                freeze(new_pair),
                freeze(old_pair),
                "frozen resolution changed for {identity}"
            );
            compared += 1;
        }
        assert_eq!(compared, 6_328, "the gate must compare every fixture row");

        // This model is introduced by the provider overlay rather than fusiform, so it is not one
        // of the 6,328 response rows above. It still serves and must retain the legacy answer.
        let overlay_old = legacy.model_for_serving("xai", "grok-composer-2.5-fast");
        let overlay_new = compact
            .catalog
            .model_for_serving("xai", "grok-composer-2.5-fast");
        assert!(overlay_old.is_some(), "overlay-only oracle model is absent");
        assert_eq!(
            consumed_model_facts(&overlay_new),
            consumed_model_facts(&overlay_old),
            "consumed facts changed for the overlay-only model"
        );
        assert_eq!(
            freeze(overlay_new),
            freeze(overlay_old),
            "frozen resolution changed for the overlay-only model"
        );
    }

    /// A model missing a capability fact fails the whole load rather than defaulting.
    ///
    /// FUSI relies on this in their protocol contract, so it is a promise to another repository
    /// rather than an internal preference. Their earlier claim cited a test over the VENDORED
    /// snapshot, which today's cutover took off the serving path -- nobody edited it, the road
    /// moved, and the claim became irrelevant rather than false. This asserts the OUTCOME on the
    /// path the data actually takes, which no cutover can retire without breaking it.
    ///
    /// Fail-closed is the point. Defaulting an absent capability to false would silently stop a
    /// capable model reasoning, and nothing downstream would report anything: the run would
    /// simply be worse. Refusing the load is loud, and since fb1356a it is also visible as
    /// `catalog_refusals` rather than reading as ordinary staleness.
    #[test]
    fn a_model_missing_a_capability_fact_fails_the_load_rather_than_defaulting() {
        for fact in [
            "capability.reasoning",
            "capability.attachment",
            "capability.tool_call",
            "limit.context",
        ] {
            let mut response = response(100);
            let model = response
                .models
                .get_mut("anthropic/claude-sonnet-4-5")
                .expect("fixture model");
            model.remove(fact);

            let cache = CatalogCache::empty();
            let error = cache
                .refresh(response)
                .expect_err("a model missing a served fact must not load");
            let text = error.to_string();
            assert!(
                text.contains(fact) && text.contains("claude-sonnet-4-5"),
                "the refusal must name the fact and the model so it can be acted on, got: {text}"
            );
            assert!(
                cache.snapshot().is_err(),
                "a refused load must leave nothing installed for {fact}"
            );
        }

        // An EXPLICIT null is different and must load: it is the producer saying "the upstream
        // does not state this", which is a fact about knowledge rather than a missing field.
        // Collapsing the two would discard the distinction CapabilityFlag exists to carry.
        let mut response = response(100);
        response
            .models
            .get_mut("anthropic/claude-sonnet-4-5")
            .expect("fixture model")
            .insert("capability.reasoning".into(), serde_json::Value::Null);
        let cache = CatalogCache::empty();
        cache
            .refresh(response)
            .expect("an explicit null is a stated unknown, not an absent fact");
    }

    /// The refusal names the instant the response resolved to, because that is what says WHO
    /// should act.
    ///
    /// A shrunken identity set has two causes that a missing-count alone cannot separate: the
    /// producer's current view shrank (their problem), or this refresh asked for a historical
    /// snapshot and legitimately got fewer models (ours). FUSI first told me the two responses
    /// were byte-identical and later corrected it from their own source -- `resolved_at_ms` is
    /// `at.unwrap_or(now)`, so it differs, and the discriminator was on the wire from the first
    /// response. Nothing said to read it.
    ///
    /// That is the detectability-versus-detection gap in its cheapest form: no new field, no
    /// producer change, one value carried into the message a human actually reads.
    #[test]
    fn an_incomplete_refusal_names_the_instant_the_response_resolved_to() {
        let cache = CatalogCache::empty();
        cache.refresh(response(100)).expect("first install");

        // A historical read: fewer identities, resolved to a past instant.
        let mut historical = response(200);
        historical.resolved_at_ms = 1_700_000_000_000;
        historical.catalog_version += 1;
        historical.models.remove("anthropic/claude-retired");
        let text = cache
            .refresh(historical)
            .expect_err("a shrunken response is refused")
            .to_string();
        assert!(
            text.contains("1700000000000"),
            "the refusal must carry the resolved instant so a reader can tell which cause \
             they are looking at, got: {text}"
        );
        assert!(
            text.contains("claude-retired"),
            "and it must still name what went missing, got: {text}"
        );
    }

    /// An unchanged equal-version poll still reuses the index, so r115's saving is intact.
    ///
    /// The equal-version fast path must keep reusing the index on unchanged polls. Rebuilding it
    /// every 30 minutes would undo the measured 2.96ms / 1.38 MB saving, so this test pins reuse.
    #[test]
    fn an_unchanged_equal_version_poll_reuses_the_existing_index() {
        let cache = CatalogCache::empty();
        let first = response(100);
        let version = first.catalog_version;
        cache.refresh(first).expect("install");
        let before = cache.snapshot().expect("installed");

        let mut again = response(200);
        again.catalog_version = version;
        cache.refresh(again).expect("same-version refresh");
        let after = cache.snapshot().expect("installed");

        assert!(
            before.shares_index_with(&after),
            "an unchanged equal-version poll must republish the SAME index rather than rebuild it"
        );
        assert_eq!(
            after.resolved_at_ms(),
            200,
            "freshness still advances on a reused index"
        );
    }

    /// An equal-version poll that is the first to carry `limit.output` rebuilds the index.
    ///
    /// The LKG on disk was written before broca requested the fact, so the next poll arrives at
    /// the same catalog version with the fact now present. Reusing the index there would serve
    /// no output limits until fusiform happened to mint a new version.
    #[test]
    fn an_equal_version_poll_that_newly_carries_output_limits_rebuilds_the_index() {
        let cache = CatalogCache::empty();
        let first = response(100);
        let version = first.catalog_version;
        cache.refresh(first).expect("install");
        let before = cache.snapshot().expect("installed");
        assert_eq!(before.output_limit("anthropic", "claude-sonnet-4-5"), None);

        let mut upgraded = response(200);
        upgraded.catalog_version = version;
        upgraded
            .models
            .get_mut("anthropic/claude-sonnet-4-5")
            .expect("fixture row")
            .insert("limit.output".into(), serde_json::json!(64000));
        cache.refresh(upgraded).expect("same-version refresh");
        let after = cache.snapshot().expect("installed");
        assert!(!before.shares_index_with(&after));
        assert_eq!(
            after.output_limit("anthropic", "claude-sonnet-4-5"),
            Some(64000)
        );
    }

    /// An equal-version poll that is the first to carry mode rates rebuilds the index, for the
    /// same reason as `limit.output` above: otherwise every tier stays refused until fusiform
    /// mints a new version.
    #[test]
    fn an_equal_version_poll_that_newly_carries_mode_rates_rebuilds_the_index() {
        let cache = CatalogCache::empty();
        let first = response(100);
        let version = first.catalog_version;
        cache.refresh(first).expect("install");
        let before = cache.snapshot().expect("installed");
        assert!(before
            .priced_modes("anthropic", "claude-sonnet-4-5")
            .is_empty());

        let mut upgraded = response(200);
        upgraded.catalog_version = version;
        let row = upgraded
            .models
            .get_mut("anthropic/claude-sonnet-4-5")
            .expect("fixture row");
        row.insert("rate.input.mode.fast".into(), priced_rate(6_000_000_000));
        row.insert("rate.output.mode.fast".into(), priced_rate(30_000_000_000));
        cache.refresh(upgraded).expect("same-version refresh");
        let after = cache.snapshot().expect("installed");
        assert!(!before.shares_index_with(&after));
        assert_eq!(
            after.priced_modes("anthropic", "claude-sonnet-4-5"),
            vec!["fast".to_string()]
        );
    }

    #[test]
    fn compiled_overlay_only_model_retains_authored_warm_prices() {
        let mut response = response(10);
        let facts = response.models["anthropic/claude-sonnet-4-5"].clone();
        response.models.insert("xai/fixture-base".into(), facts);
        assert!(!response.models.contains_key("xai/grok-composer-2.5-fast"));
        let cache = CatalogCache::empty();
        cache.refresh(response).expect("shipped overlay must load");
        let snapshot = cache.snapshot().unwrap();
        let cost = snapshot
            .warm_cost_schedule("xai", "grok-composer-2.5-fast")
            .unwrap();
        assert_eq!(cost.input, Some(0.5));
        assert_eq!(cost.output, Some(2.5));
        assert_eq!(cost.cache_read, Some(0.2));
        assert_eq!(cost.cache_write, None);
        let (_, model) = snapshot
            .model_for_serving("xai", "grok-composer-2.5-fast")
            .unwrap();
        assert_eq!(model.cost, cost);
        assert_eq!(model.display_name, "Grok Composer 2.5 Fast");
    }

    #[test]
    fn existing_model_overlay_replaces_prices_and_preserves_name_and_structured_output() {
        let mut response = response(10);
        let facts = response
            .models
            .get_mut("anthropic/claude-sonnet-4-5")
            .unwrap();
        facts.insert("rate.input".into(), priced_rate(9_000_000));
        let overlay: RuntimeOverlay = serde_json::from_str(r#"{"patch":{"anthropic":{
            "base_url_source":"authored", "wire_family_source":"authored", "wire_family_reason":"first party",
            "models":{"claude-sonnet-4-5":{
                "id":"claude-sonnet-4-5", "name":"Authored name", "structured_output":true,
                "cost":{"input":1.5,"output":7.5,"cache_read":0.15,"cache_write":0,
                    "reasoning":3,"input_audio":4,"output_audio":5,
                    "tiers":[{"tier":{"type":"context","size":200000},"input":2,"output":10,"cache_read":0.2}]}
            }}}}}"#).unwrap();
        let (catalog, _) =
            ServingCatalog::from_fusiform_with_overlay(&response.models, overlay).unwrap();
        let cost = &catalog.providers["anthropic"].models["claude-sonnet-4-5"].cost;
        assert_eq!(cost.input, Some(1.5));
        assert_eq!(cost.output, Some(7.5));
        assert_eq!(cost.cache_read, Some(0.15));
        assert_eq!(cost.cache_write, Some(0.0));
        assert_eq!(cost.reasoning, Some(3.0));
        assert_eq!(cost.input_audio, Some(4.0));
        assert_eq!(cost.output_audio, Some(5.0));
        assert_eq!(cost.tiers.len(), 1);
        assert_eq!(cost.tiers[0].min_context, 200000);
        assert_eq!(cost.tiers[0].input, Some(2.0));
        assert_eq!(cost.tiers[0].output, Some(10.0));
        assert_eq!(cost.tiers[0].cache_read, Some(0.2));
        assert_eq!(cost.tiers[0].cache_write, None);
        let (_, model) = catalog
            .model_for_serving("anthropic", "claude-sonnet-4-5")
            .unwrap();
        assert_eq!(model.cost, *cost);
        assert_eq!(model.display_name, "Authored name");
        assert_eq!(
            model.capabilities.structured_output,
            CapabilityFlag::Known(true)
        );
    }

    #[test]
    fn runtime_overlay_refuses_unknown_keys() {
        for overlay in [
            r#"{"patch":{"anthropic":{"models":{"m":{"coast":{"input":1}}}}}}"#,
            r#"{"patch":{"anthropic":{"base_urll":"https://example.test"}}}"#,
            r#"{"patchh":{}}"#,
            r#"{"patch":{"anthropic":{"models":{"m":{"limit":{"outpt":1}}}}}}"#,
            r#"{"patch":{"anthropic":{"models":{"m":{"cost":{"inpt":1}}}}}}"#,
            r#"{"patch":{"anthropic":{"models":{"m":{"cost":{"tiers":[{"inputt":1,"tier":{"size":1,"type":"context"}}]}}}}}}"#,
            r#"{"patch":{"anthropic":{"models":{"m":{"cost":{"tiers":[{"tier":{"size":1,"type":"context","typo":true}}]}}}}}}"#,
        ] {
            let error = serde_json::from_str::<RuntimeOverlay>(overlay)
                .err()
                .expect("unknown overlay field must be refused");
            assert!(error.to_string().contains("unknown field"), "{error}");
        }
    }

    #[test]
    fn warm_prices_retain_vendored_base_context_facts_and_ignore_modes() {
        let response: FusiformCatalogResponse = serde_json::from_str(include_str!(
            "../tests/fixtures/fusiform_service_tier_modes.json"
        ))
        .unwrap();
        let cache = CatalogCache::empty();
        cache.refresh(response).unwrap();
        let snapshot = cache.snapshot().unwrap();
        let prices = snapshot
            .warm_cost_schedule("openai", "gpt-5.4-mini")
            .unwrap();
        assert_eq!(prices.input, Some(0.2));
        assert_eq!(prices.cache_read, Some(0.02));
        assert_eq!(prices.cache_write, Some(0.25));
        assert_eq!(prices.output, Some(1.2));
        assert_eq!(prices.tiers.len(), 1);
        assert_eq!(prices.tiers[0].min_context, 272_001);
        assert_eq!(prices.tiers[0].input, Some(0.4));
        assert_eq!(prices.tiers[0].cache_read, Some(0.04));
        assert_eq!(prices.tiers[0].cache_write, Some(0.5));
        assert!(snapshot.warm_cost_schedule("missing", "m").is_none());
    }

    #[test]
    fn an_equal_version_poll_that_newly_carries_warm_prices_rebuilds_the_index() {
        let cache = CatalogCache::empty();
        cache.refresh(response(100)).unwrap();
        let before = cache.snapshot().unwrap();
        assert_eq!(
            before
                .warm_cost_schedule("anthropic", "claude-sonnet-4-5")
                .unwrap()
                .input,
            None
        );
        let mut upgraded = response(200);
        upgraded
            .models
            .get_mut("anthropic/claude-sonnet-4-5")
            .unwrap()
            .insert("rate.input".into(), priced_rate(3_000_000_000));
        cache.refresh(upgraded).unwrap();
        let after = cache.snapshot().unwrap();
        assert!(!before.shares_index_with(&after));
        assert_eq!(
            after
                .warm_cost_schedule("anthropic", "claude-sonnet-4-5")
                .unwrap()
                .input,
            Some(3.0)
        );
        assert_eq!(
            before
                .warm_cost_schedule("anthropic", "claude-sonnet-4-5")
                .unwrap()
                .input,
            None
        );
    }

    #[test]
    fn warm_rate_distinguishes_missing_unpriced_and_stated_zero() {
        assert_eq!(warm_rate(None), None);
        for value in [
            Value::Null,
            serde_json::json!(2),
            serde_json::json!({"state":"unpriced"}),
            serde_json::json!({"state":"unknown","currency":"USD","exponent":9,"units":0}),
            serde_json::json!({"state":"priced","currency":"EUR","exponent":9,"units":1}),
            serde_json::json!({"state":"stated_zero","currency":"USD","exponent":9,"units":1}),
        ] {
            assert_eq!(warm_rate(Some(&value)), None, "{value}");
        }
        assert_eq!(
            warm_rate(Some(
                &serde_json::json!({"state":"stated_zero","currency":"USD","exponent":9,"units":0})
            )),
            Some(0.0)
        );
        assert_eq!(
            warm_rate(Some(
                &serde_json::json!({"state":"priced","currency":"USD","exponent":6,"units":3000000})
            )),
            Some(3.0)
        );
        let facts = BTreeMap::from([
            ("rate.input.mode.fast".into(), priced_rate(9_000_000_000)),
            ("rate.cache_read.mode.fast".into(), priced_rate(450_000_000)),
        ]);
        assert_eq!(warm_cost_fact(&facts), CostSchedule::default());
    }

    fn priced_rate(units: u64) -> Value {
        serde_json::json!({"currency": "USD", "exponent": 9, "state": "priced", "units": units})
    }

    /// A mode is priced only when BOTH of its own rates are. The base rate never fills a missing
    /// side, an unpriced or unknown state is unpriced, a zero the source states is a price, and a
    /// row with mode rates but no base rate still prices its modes.
    #[test]
    fn a_mode_is_priced_only_by_its_own_input_and_output_rates() {
        let facts = |pairs: &[(&str, Value)]| -> BTreeMap<String, Value> {
            pairs
                .iter()
                .map(|(key, value)| (key.to_string(), value.clone()))
                .collect()
        };
        let base = [
            ("rate.input", priced_rate(1_000_000_000)),
            ("rate.output", priced_rate(8_000_000_000)),
        ];
        let cases: Vec<(&str, BTreeMap<String, Value>, Vec<&str>)> = vec![
            (
                "both sides priced",
                facts(&[
                    base[0].clone(),
                    base[1].clone(),
                    ("rate.input.mode.fast", priced_rate(2)),
                    ("rate.output.mode.fast", priced_rate(16)),
                ]),
                vec!["fast"],
            ),
            (
                "input side absent, base rates present",
                facts(&[
                    base[0].clone(),
                    base[1].clone(),
                    ("rate.output.mode.fast", priced_rate(16)),
                ]),
                vec![],
            ),
            (
                "output side absent, base rates present",
                facts(&[
                    base[0].clone(),
                    base[1].clone(),
                    ("rate.input.mode.priority", priced_rate(2)),
                ]),
                vec![],
            ),
            (
                "mode rates with no base rate",
                facts(&[
                    ("rate.input.mode.fast", priced_rate(2)),
                    ("rate.output.mode.fast", priced_rate(16)),
                ]),
                vec!["fast"],
            ),
            (
                "unpriced, null and unknown states",
                facts(&[
                    (
                        "rate.input.mode.pro",
                        serde_json::json!({"state": "unpriced"}),
                    ),
                    (
                        "rate.output.mode.pro",
                        serde_json::json!({"state": "unpriced"}),
                    ),
                    ("rate.input.mode.flex", Value::Null),
                    ("rate.output.mode.flex", priced_rate(1)),
                    (
                        "rate.input.mode.odd",
                        serde_json::json!({"state": "estimated"}),
                    ),
                    ("rate.output.mode.odd", priced_rate(1)),
                ]),
                vec![],
            ),
            (
                "a stated zero is a price",
                facts(&[
                    (
                        "rate.input.mode.priority",
                        serde_json::json!({"state": "stated_zero"}),
                    ),
                    ("rate.output.mode.priority", priced_rate(1)),
                ]),
                vec!["priority"],
            ),
        ];
        for (name, facts, expected) in cases {
            assert_eq!(
                priced_modes_fact("p/m", &facts).expect("well-formed"),
                expected,
                "{name}"
            );
        }
        let malformed = facts(&[("rate.input.mode.fast", serde_json::json!(2.0))]);
        assert!(priced_modes_fact("p/m", &malformed).is_err());
    }

    /// `limit.output: null` is fusiform's "unknown" and must decode to `None`, never a zero cap;
    /// a number decodes as served; a pre-change row without the fact still loads, as `None`.
    #[test]
    fn a_served_null_output_limit_decodes_to_none_not_zero() {
        let mut response = response(100);
        let row = response
            .models
            .get("anthropic/claude-sonnet-4-5")
            .expect("fixture row")
            .clone();
        let mut null_row = row.clone();
        null_row.insert("limit.output".into(), Value::Null);
        let mut known_row = row.clone();
        known_row.insert("limit.output".into(), serde_json::json!(64000));
        response
            .models
            .insert("anthropic/null-output".into(), null_row);
        response
            .models
            .insert("anthropic/known-output".into(), known_row);
        // Through the wire decode, not just the typed map, so the JSON null is the input.
        let response: FusiformCatalogResponse =
            serde_json::from_value(serde_json::to_value(&response).expect("serialize"))
                .expect("decode");
        let loaded = loaded(&response, CatalogSource::Fusiform).expect("loads");
        let model = |id: &str| {
            loaded
                .catalog
                .model_for_serving("anthropic", id)
                .expect("served")
                .1
        };
        assert_eq!(model("null-output").limits.max_output, None);
        assert_eq!(model("known-output").limits.max_output, Some(64000));
        assert_eq!(model("claude-sonnet-4-5").limits.max_output, None);

        let mut malformed = response.clone();
        malformed
            .models
            .get_mut("anthropic/known-output")
            .expect("row")
            .insert("limit.output".into(), serde_json::json!("64000"));
        assert!(loaded_err_names_fact(&malformed, "limit.output"));
    }

    fn loaded_err_names_fact(response: &FusiformCatalogResponse, fact: &str) -> bool {
        matches!(
            loaded(response, CatalogSource::Fusiform),
            Err(CatalogError::FusiformFact { fact: found, .. }) if found == fact
        )
    }

    /// The poll's prefix list must request `limit.output`, or no output limit ever arrives and
    /// every omitted cap silently takes the fallback. This applies fusiform's prefix filter to a
    /// row carrying more facts than broca needs, then loads what would have been served.
    #[test]
    fn the_requested_fact_prefixes_deliver_the_output_limit() {
        let mut full = response(100);
        let row = full
            .models
            .get_mut("anthropic/claude-sonnet-4-5")
            .expect("fixture row");
        row.insert("limit.output".into(), serde_json::json!(64000));
        row.insert("rate.input".into(), serde_json::json!(3.0));
        for facts in full.models.values_mut() {
            facts.retain(|fact, _| {
                FUSIFORM_FACT_PREFIXES
                    .iter()
                    .any(|prefix| fact.starts_with(prefix))
            });
        }
        let served = loaded(&full, CatalogSource::Fusiform).expect("filtered response loads");
        assert_eq!(
            served
                .catalog
                .output_limit("anthropic", "claude-sonnet-4-5"),
            Some(64000),
            "limit.output must survive the prefix filter the poll asks fusiform to apply"
        );
    }

    /// An equal-version qualified response cannot replace either the serving state or its LKG.
    ///
    /// The in-memory reuse path used to persist qualified responses without materializing them, so
    /// the running process looked fresh while its next restart rejected the newly written LKG. A
    /// run of these refusals must also remain visible through both health counters.
    #[test]
    fn an_equal_version_qualified_response_leaves_memory_and_lkg_unchanged() {
        let path = temp_lkg("equal-version-qualified");
        let cache = CatalogCache::open(&path);
        cache.refresh(response(100)).expect("first install");
        let installed = cache.snapshot().expect("installed");
        let lkg_before = fs::read(&path).expect("read clean LKG");

        let mut qualified = response(500);
        qualified.catalog_version = installed.catalog_version();
        qualified
            .withheld
            .push(serde_json::json!({"fact": "limit.context"}));

        for expected in 1..=3u32 {
            let error = cache
                .refresh(qualified.clone())
                .expect_err("qualified response must be refused at an equal version");
            assert!(
                matches!(
                    error,
                    CatalogRefreshError::Catalog(CatalogError::FusiformQualifiedFacts {
                        withheld: 1,
                        uncertain: 0,
                    })
                ),
                "expected FusiformQualifiedFacts, got {error}"
            );

            let health = cache.health(500).expect("health remains available");
            assert_eq!(
                health.consecutive_refusals, expected,
                "qualified refusals must degrade health as a streak"
            );
            assert_eq!(
                health.total_refusals,
                u64::from(expected),
                "each qualified refusal must increment the lifetime counter exactly once"
            );
            assert!(
                health
                    .refusal_reason
                    .is_some_and(|reason| reason.contains("1 withheld and 0 uncertain")),
                "health must retain the qualification reason"
            );
        }

        let after = cache.snapshot().expect("prior catalog remains installed");
        assert!(
            installed.shares_index_with(&after),
            "refusal must keep serving the prior in-memory index"
        );
        assert_eq!(after.resolved_at_ms(), 100);
        assert!(after
            .model_for_serving("anthropic", "claude-sonnet-4-5")
            .is_some());
        assert_eq!(
            fs::read(&path).expect("read LKG after refusal"),
            lkg_before,
            "a refused response must not change the on-disk LKG"
        );

        let restarted = CatalogCache::open(&path);
        let recovered = restarted
            .snapshot()
            .expect("the unchanged LKG remains loadable after restart");
        assert_eq!(recovered.source(), CatalogSource::LastKnownGood);
        assert_eq!(recovered.resolved_at_ms(), 100);
        assert!(recovered
            .model_for_serving("anthropic", "claude-sonnet-4-5")
            .is_some());

        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    /// A cleared refusal stays visible in the total, because the streak forgets it.
    ///
    /// FUSI's point that this gauge is the only instrument either of us has pointed at a real
    /// producer restore is what forced this: an observer of a rare event is useless if reading
    /// it requires being present while the event holds. Both conditions that trip these guards
    /// are rare AND self-clearing, so the streak's most likely value during an investigation is
    /// zero even when something happened hours earlier.
    #[test]
    fn a_recovered_refusal_is_still_visible_in_the_total() {
        let cache = CatalogCache::empty();
        cache.refresh(response(100)).expect("first install");
        assert_eq!(cache.total_refusals(), 0, "a clean cache has seen nothing");

        let mut short = response(200);
        short.catalog_version += 1;
        short.models.remove("anthropic/claude-retired");
        for _ in 0..3 {
            let _ = cache.refresh(short.clone()).expect_err("refused");
        }
        assert_eq!(cache.consecutive_refusals().0, 3);
        assert_eq!(cache.total_refusals(), 3);

        // The producer recovers. The streak forgets, which is correct for a current-state gauge
        // -- and is exactly why the total must not.
        cache
            .refresh(response(300))
            .expect("a complete response is accepted");
        assert_eq!(
            cache.consecutive_refusals(),
            (0, None),
            "the streak reports the CURRENT state"
        );
        assert_eq!(
            cache.total_refusals(),
            3,
            "the total must survive recovery, or the event leaves no trace at all"
        );
    }

    /// The stale refusal SELF-HEALS and the completeness refusal does not, and a content-derived
    /// producer version heals into the second rather than escaping it.
    ///
    /// FUSI asked whether the stale guard is permanent the way the completeness guard is. It is
    /// not, and the asymmetry is worth stating because it decides a design on their side: both
    /// guards compare against a basis this cache freezes on refusal, but the PRODUCER's side of
    /// the comparison moves in one case and not the other. Their version climbs with wall clock,
    /// so a held high-water is eventually passed. Their identity set stays shrunk until the
    /// missing models are re-observed, which for a model retired before the restore point may be
    /// never.
    ///
    /// So a version derived from content does not convert a permanent refusal into a bounded
    /// one. It inserts a bounded stale phase in FRONT of the same permanent completeness
    /// refusal. This test walks that exact sequence.
    #[test]
    fn a_rewound_version_heals_into_the_completeness_guard_rather_than_past_it() {
        let cache = CatalogCache::empty();
        cache.refresh(response(100)).expect("first install");
        let held = cache.snapshot().expect("installed").catalog_version();

        // A restore under a content-derived version: the value rewinds to the restored
        // generation's newest era, and the store carries fewer identities.
        let mut restored = response(200);
        restored.catalog_version = held - 1_000;
        restored.models.remove("anthropic/claude-retired");
        let error = cache
            .refresh(restored.clone())
            .expect_err("rewound version is refused");
        assert!(
            matches!(error, CatalogRefreshError::Stale { .. }),
            "a rewound version must refuse on STALENESS, got {error}"
        );

        // The producer then writes an era at wall-clock, so the version climbs past the held
        // high-water. This is the self-heal: the stale gate stops refusing.
        let mut climbed = restored.clone();
        climbed.catalog_version = held + 1_000;
        let error = cache
            .refresh(climbed)
            .expect_err("the identity set is still short");
        assert!(
            matches!(error, CatalogRefreshError::Incomplete { .. }),
            "past the stale gate it must land on COMPLETENESS, got {error}"
        );

        // And that one does not heal: the basis never advances, so every later poll carrying the
        // restored store's smaller set refuses identically however high the version climbs.
        for step in 1..=3 {
            let mut later = restored.clone();
            later.catalog_version = held + 1_000 + step * 1_000;
            assert!(
                matches!(
                    cache.refresh(later).expect_err("still short"),
                    CatalogRefreshError::Incomplete { .. }
                ),
                "completeness refusal must persist at step {step}"
            );
        }
        assert_eq!(
            cache.snapshot().expect("still installed").catalog_version(),
            held,
            "no refused refresh may advance the installed version"
        );
    }

    /// A permanent refusal is visible as a rising count, and one success clears it.
    ///
    /// Freshness cannot carry this: `catalog_age_ms` rises identically whether a poll missed
    /// once or will never succeed again. The count is what separates them, so it has to survive
    /// across polls and reset only on an accepted refresh.
    #[test]
    fn consecutive_refusals_accumulate_and_reset_on_the_first_success() {
        let cache = CatalogCache::empty();
        cache.refresh(response(100)).expect("first install");
        assert_eq!(
            cache.consecutive_refusals().0,
            0,
            "a healthy cache reports zero rather than nothing"
        );

        let mut restored = response(200);
        restored.catalog_version = cache.snapshot().unwrap().catalog_version() + 5_000;
        restored.models.remove("anthropic/claude-retired");
        for expected in 1..=3u32 {
            let _ = cache.refresh(restored.clone()).expect_err("refused");
            let (count, reason) = cache.consecutive_refusals();
            assert_eq!(count, expected, "refusals must accumulate across polls");
            assert!(
                reason.is_some_and(|text| text.contains("claude-retired")),
                "the reason must name what is missing, so a reader can act on it"
            );
        }

        // A producer that recovers clears the state: the count reports the CURRENT streak, not a
        // lifetime total, because the question it answers is "is the catalog frozen right now".
        cache
            .refresh(response(300))
            .expect("a complete response is accepted again");
        assert_eq!(
            cache.consecutive_refusals(),
            (0, None),
            "one success must clear both the count and the stale reason"
        );
    }

    /// A newer-version response carrying FEWER identities is refused, and the refusal is
    /// PERMANENT rather than one poll.
    ///
    /// This is the interaction FUSI walked from their side and I could not have seen from mine.
    /// Their catalog version is derived as max(now_ms, current + 1), so it is monotonic even
    /// across a restore of their database. A restore therefore serves a version that is HIGHER
    /// than anything held here while the CONTENT is older and the identity set is smaller --
    /// the one combination that passes a version check and lands in a content check.
    ///
    /// The completeness guard then compares against the identities THIS CACHE HOLDS, not against
    /// the previous response, and a failed refresh does not install. So the held set never
    /// shrinks, every subsequent poll compares against the same larger set, and the refusal
    /// repeats for as long as fusiform serves the restored store.
    ///
    /// Refusing is still correct: a legitimately smaller set and a truncated response are
    /// indistinguishable by identity count, and serving a catalog that lost models is worse.
    /// What this test pins is the PERMANENCE, because that is what decides whether the state
    /// needs to be visible rather than merely logged.
    #[test]
    fn a_newer_response_with_fewer_identities_is_refused_permanently() {
        let cache = CatalogCache::empty();
        cache.refresh(response(100)).expect("first install");
        let held = cache.snapshot().expect("installed").catalog_version();

        // A restore: higher version, smaller identity set.
        let mut restored = response(200);
        restored.catalog_version = held + 5_000;
        restored.models.remove("anthropic/claude-retired");

        for attempt in 1..=3 {
            let error = cache
                .refresh(restored.clone())
                .expect_err("a response missing held identities must be refused");
            assert!(
                matches!(
                    error,
                    CatalogRefreshError::Incomplete {
                        missing_count: 1,
                        ..
                    }
                ),
                "attempt {attempt} expected Incomplete, got {error}"
            );
        }

        // The held document is untouched, which is why the refusal repeats: the comparison
        // basis never advances, so nothing about a later poll can differ.
        let snapshot = cache.snapshot().expect("still installed");
        assert_eq!(
            snapshot.catalog_version(),
            held,
            "a refused refresh must not advance the installed version"
        );
        assert_eq!(
            snapshot.resolved_at_ms(),
            100,
            "a refused refresh must not advance freshness either"
        );
    }

    /// An equal-version response reuses the index instead of rebuilding it, and must still
    /// refuse an incomplete one.
    ///
    /// Reuse is the whole point of the fast path, so the risk it introduces is that a truncated
    /// response carrying the version of the document it was truncated from gets written to the
    /// LKG while the in-memory index survives untouched. That damage is invisible until a
    /// restart loads the short catalog.
    #[test]
    fn an_equal_version_response_is_reused_but_an_incomplete_one_is_still_refused() {
        let dir = temp_lkg("equal-version-reuse");
        let cache = CatalogCache::open(dir.join("lkg.json"));
        cache.refresh(response(100)).expect("first install");
        let installed = cache.snapshot().expect("installed");

        // Same version, complete: accepted, and resolved_at_ms moves so catalog_age_ms is fresh.
        let mut same = response(500);
        same.catalog_version = installed.catalog_version();
        cache.refresh(same).expect("an equal version is not stale");
        let refreshed = cache.snapshot().expect("still installed");
        assert_eq!(refreshed.catalog_version(), installed.catalog_version());
        assert_eq!(
            refreshed.resolved_at_ms(),
            500,
            "the reuse path must still publish the new resolved_at_ms"
        );

        // Same version, truncated: refused, and the complete catalog stays installed.
        let mut truncated = response(900);
        truncated.catalog_version = installed.catalog_version();
        truncated.models.remove("anthropic/claude-retired");
        let error = cache
            .refresh(truncated)
            .expect_err("an incomplete response must be refused even at the same version");
        assert!(
            matches!(
                error,
                CatalogRefreshError::Incomplete {
                    missing_count: 1,
                    ..
                }
            ),
            "expected Incomplete, got {error}"
        );
        assert_eq!(
            cache.snapshot().expect("unchanged").resolved_at_ms(),
            500,
            "the refusal must leave the previously installed document in place"
        );
    }

    /// An older response must not replace a newer one, and the identity check cannot catch it.
    ///
    /// Both snapshots carry the SAME identity set -- which is the normal case, because
    /// include_retired keeps withdrawn models as rows -- and disagree only about whether one of
    /// them exists. So the older response passes the identity guard and would install, putting a
    /// model the newer catalog withdrew back on the admission path.
    #[test]
    fn refresh_refuses_a_response_older_than_the_installed_catalog() {
        let cache = CatalogCache::empty();

        let mut newer = response(1_000);
        newer.catalog_version = 20;
        newer
            .models
            .get_mut("anthropic/claude-sonnet-4-5")
            .expect("fixture model")
            .insert("existence".into(), serde_json::json!("absent"));
        cache.refresh(newer).expect("the newer response installs");
        assert!(
            cache
                .snapshot()
                .expect("installed")
                .catalog_version()
                .eq(&20),
            "the newer response did not install, so the rest of this test proves nothing"
        );

        // Older, complete, and it disagrees: it still calls the model present.
        let mut older = response(2_000);
        older.catalog_version = 19;
        let error = cache
            .refresh(older)
            .expect_err("an older catalog version must be refused");
        match error {
            CatalogRefreshError::Stale {
                installed,
                received,
            } => assert_eq!((installed, received), (20, 19)),
            other => panic!("expected Stale, got {other}"),
        }
        assert_eq!(
            cache.snapshot().expect("still installed").catalog_version(),
            20,
            "the refusal must leave the newer catalog in place"
        );
    }

    /// Equal versions still install: a re-poll at the same version is the same document, and
    /// installing it is what moves catalog_age_ms without changing a fact.
    #[test]
    fn refresh_accepts_a_response_at_the_same_version() {
        let cache = CatalogCache::empty();
        cache.refresh(response(1_000)).expect("first install");
        cache
            .refresh(response(2_000))
            .expect("an equal version is not stale");
    }

    fn temp_lkg(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        broca_test_support::scratch_root()
            .join(format!(
                "broca-catalog-{label}-{}-{nonce}",
                std::process::id()
            ))
            .join("fusiform-lkg.json")
    }

    #[test]
    fn lkg_survives_a_new_cache_after_memory_is_dropped() {
        let path = temp_lkg("restart");
        {
            let cache = CatalogCache::open(&path);
            cache.refresh(response(1234)).unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                assert_eq!(
                    fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                    0o600
                );
                assert_eq!(
                    fs::metadata(path.parent().unwrap())
                        .unwrap()
                        .permissions()
                        .mode()
                        & 0o777,
                    0o700
                );
            }
            assert_eq!(cache.snapshot().unwrap().source(), CatalogSource::Fusiform);
        }

        let restarted = CatalogCache::open(&path);
        let snapshot = restarted
            .snapshot()
            .expect("restart loads the persisted LKG");
        assert_eq!(snapshot.source(), CatalogSource::LastKnownGood);
        assert_eq!(snapshot.resolved_at_ms(), 1234);
        assert_eq!(snapshot.catalog_version(), 9);
        assert!(snapshot
            .model_for_serving("anthropic", "claude-sonnet-4-5")
            .is_some());
        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn a_response_that_drops_known_identities_cannot_replace_the_complete_catalog() {
        let cache = CatalogCache::empty();
        cache.refresh(response(100)).unwrap();
        let mut partial = response(200);
        partial.models.remove("anthropic/claude-retired");

        let error = cache
            .refresh(partial)
            .expect_err("a smaller response is not a complete refresh");
        assert!(matches!(
            error,
            CatalogRefreshError::Incomplete {
                missing_count: 1,
                ..
            }
        ));
        let snapshot = cache.snapshot().unwrap();
        assert_eq!(snapshot.resolved_at_ms(), 100);
        assert!(snapshot.is_withdrawn("anthropic", "claude-retired"));
    }

    #[test]
    fn total_absence_is_a_typed_error_naming_memory_and_lkg() {
        let path = temp_lkg("absent");
        let cache = CatalogCache::open(&path);
        let error = match cache.snapshot() {
            Ok(_) => panic!("empty cache unexpectedly resolved"),
            Err(error) => error,
        };
        assert_eq!(error.lkg_path(), Some(path.as_path()));
        let text = error.to_string();
        assert!(text.contains("no in-memory fusiform catalog"));
        assert!(text.contains("no last-known-good catalog"));
        assert!(text.contains(path.to_string_lossy().as_ref()));
    }

    #[test]
    fn fusiform_response_maps_present_models_and_preserves_unknown_and_retired_states() {
        let cache = CatalogCache::empty();
        cache
            .install_for_test(response(100), CatalogSource::Fusiform)
            .unwrap();
        let (_, model) = cache
            .snapshot()
            .unwrap()
            .model_for_serving("anthropic", "claude-sonnet-4-5")
            .expect("mapped model");
        assert_eq!(model.capabilities.attachment, CapabilityFlag::Unknown);
        assert_eq!(model.capabilities.reasoning, CapabilityFlag::Known(true));
        assert_eq!(model.capabilities.tool_call, CapabilityFlag::Known(true));
        assert_eq!(model.limits.context, Some(200000));
        assert!(cache
            .snapshot()
            .unwrap()
            .is_withdrawn("anthropic", "claude-retired"));
    }

    /// `capability.reasoning_options` as fusiform serves it (the row values are copied from
    /// fusiform's served fixture at bc2fe0b): null, `[]` and a populated array stay three
    /// distinct states, an array keeps its entries in order, and an older catalog that does not
    /// serve the fact reads as unstated rather than refusing to load.
    #[test]
    fn served_reasoning_options_keep_null_empty_and_populated_distinct() {
        let mut served = response(100);
        let row = |options: Option<serde_json::Value>| {
            let mut facts = BTreeMap::from([
                ("existence".into(), serde_json::json!("present")),
                ("limit.context".into(), serde_json::json!(200000)),
                ("capability.attachment".into(), serde_json::json!(true)),
                ("capability.reasoning".into(), serde_json::json!(true)),
                ("capability.tool_call".into(), serde_json::json!(true)),
            ]);
            if let Some(options) = options {
                facts.insert("capability.reasoning_options".into(), options);
            }
            facts
        };
        let populated = serde_json::json!([
            {"type": "toggle"},
            {"type": "effort", "values": ["low", "medium", "high"]},
            {"max": 127999, "min": 1024, "type": "budget_tokens"}
        ]);
        for (id, options) in [
            ("anthropic/null-options", Some(serde_json::Value::Null)),
            ("anthropic/empty-options", Some(serde_json::json!([]))),
            ("anthropic/populated-options", Some(populated.clone())),
            ("anthropic/unserved-options", None),
        ] {
            served.models.insert(id.into(), row(options));
        }
        let cache = CatalogCache::empty();
        cache
            .install_for_test(served, CatalogSource::Fusiform)
            .unwrap();
        let snapshot = cache.snapshot().unwrap();
        let options = |model: &str| {
            snapshot
                .model_for_serving("anthropic", model)
                .expect("mapped model")
                .1
                .reasoning_options
        };
        assert_eq!(options("null-options"), ReasoningOptions::Unstated);
        assert_eq!(options("empty-options"), ReasoningOptions::NoOptions);
        assert_eq!(options("populated-options").to_value(), populated);
        assert_eq!(options("unserved-options"), ReasoningOptions::Unstated);
    }

    #[test]
    fn stale_health_reports_age_and_lkg_source_without_a_degradation_state() {
        let cache = CatalogCache::empty();
        cache
            .install_for_test(response(100), CatalogSource::LastKnownGood)
            .unwrap();
        let health = cache.health(5100).unwrap();
        assert_eq!(health.age_ms, 5000);
        assert_eq!(health.source, CatalogSource::LastKnownGood);
    }
}

#[cfg(all(test, windows))]
mod windows_acl_tests {
    use super::*;
    use broca_owner_only::test_support::{
        assert_protected_private as assert_private, observe, BroadParent,
    };
    #[test]
    fn windows_catalog_temporary_and_replacement_are_private() {
        let fixture = BroadParent::new("catalog");
        let path = fixture.path.join("catalog/lkg.json");
        let response = FusiformCatalogResponse {
            source: "fixture".into(),
            resolved_at_ms: 1,
            catalog_version: 1,
            models: BTreeMap::new(),
            withheld: vec![],
            uncertain: vec![],
        };
        let seen = std::rc::Rc::new(std::cell::Cell::new(0));
        let output = seen.clone();
        observe(
            move |path| {
                assert_private(path);
                output.set(output.get() + 1);
            },
            || {
                persist_lkg(&path, &response).unwrap();
                persist_lkg(&path, &response).unwrap();
            },
        );
        assert_eq!(
            seen.get(),
            2,
            "both catalog temporary files must be inspected before rename"
        );
        assert_private(&path);
        assert_private(&fixture.path.join("catalog"));
    }
}
