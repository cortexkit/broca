use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Instant;

use broca_provider::spec::{
    Capabilities, CapabilityFlag, CostSchedule, CostTier, InputModalities, Limits, ModelSpec,
    Overlay, ProviderSpec, ReasoningOptions, WireFamilyId,
};
use serde::{Deserialize, Deserializer, Serialize};
#[cfg(test)]
use serde_json::Map;
use serde_json::Value;

use crate::family::{resolve_provider_family, FamilyResolution};
#[cfg(any(test, feature = "test-support"))]
use crate::Catalog;
use crate::{CatalogError, OVERLAY};

mod lkg;
mod reasoning_facts;
mod temperature_facts;
use lkg::{load_lkg, persist_bootable_lkg};
use reasoning_facts::{reasoning_default_effort_fact, reasoning_fact, reasoning_options_fact};
use temperature_facts::temperature_facts;

// Snapshot replacement bypasses refresh validation and must not be available to shipping code.
#[cfg(all(feature = "test-support", not(debug_assertions)))]
compile_error!("catalog test-support cannot be enabled in a release build");

/// Catalog fields fetched from fusiform for accepting sends and maintaining prompt caches.
/// Accepting a send freezes its run configuration before any record is written.
///
/// `existence` decides whether a model can be served. `limit.context` drives prompt reduction;
/// `limit.output` supplies the default output-token cap when the send sets none (see
/// `default_max_output_tokens` in `live.rs`). The attachment, reasoning, and tool-call flags
/// populate Broca's typed capabilities. `capability.reasoning_options` lists the settings used
/// to resolve the caller's `model.variant`. Fetching the `capability.reasoning` prefix also
/// fetches `capability.reasoning_options`, but listing both keeps the options requested if the
/// flag's prefix is later removed or renamed. Temperature support and numeric bounds are
/// admission inputs; their effective setting is recorded in the run's generation config.
///
/// The rate prefixes fetch base and context-bracket input, output, cache-read, and cache-write
/// prices for cache maintenance, plus input/output mode prices for accepting sends. A missing
/// mode rate is never replaced by a base rate; warming deliberately ignores mode prices.
///
/// `model.kind` says what a model does when fusiform states it (`chat`, or a decision or
/// extraction model such as Cloudflare's System One rows). `session.send` serves chat only, so
/// it refuses a model whose stated kind is anything else; an absent kind is not stated, never a
/// claim of chat, and admits as before.
///
/// `capability.server_side_fallback` is the one capability fusiform serves for a routing
/// feature rather than a model property: the runtime may hand a refused request to another
/// model itself. It sits under `capability.` because fusiform publishes it as a model
/// capability, and it is requested by its full name because broca requests exact capability
/// keys.
pub const FUSIFORM_FACT_PREFIXES: &[&str] = &[
    "existence",
    "model.kind",
    "model.name",
    "provider.",
    "limit.context",
    "limit.output",
    "limit.temperature_min",
    "limit.temperature_max",
    "limit.images_per_request",
    "limit.image_long_edge_px",
    "capability.attachment",
    "capability.input_modalities",
    "capability.mid_conversation_system",
    "capability.effort_change_cache",
    "capability.reasoning",
    "capability.reasoning_options",
    "capability.server_side_fallback",
    "capability.tool_call",
    "capability.temperature",
    "capability.structured_output",
    "rate.input",
    "rate.output",
    "rate.cache_read",
    "rate.cache_write",
    "wire.npm",
    "wire.api",
    "wire.shape",
    "route.reasoning_wire_ids",
    "route.reasoning_default",
    "route.mode_wire_ids",
    "successor",
];

/// Prefix of a per-model input rate for one upstream mode, e.g. `rate.input.mode.fast`.
const MODE_RATE_INPUT_PREFIX: &str = "rate.input.mode.";
/// Prefix of a per-model output rate for one upstream mode, e.g. `rate.output.mode.fast`.
const MODE_RATE_OUTPUT_PREFIX: &str = "rate.output.mode.";
/// The `model.kind` value of a chat model, the only kind `session.send` admits.
pub(crate) const MODEL_KIND_CHAT: &str = "chat";

/// The catalog fact saying the runtime accepts a request that asks it to hand a refusal to
/// another model itself. A named constant because the rest of the work keys on it: the prefix
/// list above, the row reader, the `carries_…` scan and the tests.
pub(crate) const SERVER_SIDE_FALLBACK_FACT: &str = "capability.server_side_fallback";
pub(crate) const MID_CONVERSATION_SYSTEM_FACT: &str = "capability.mid_conversation_system";
/// The catalog fact saying a change of the model's reasoning effort between requests keeps the
/// provider's cached prefix, when sent in the provider's cache-keeping form (llm-runner §9.3.6).
pub(crate) const EFFORT_CHANGE_CACHE_FACT: &str = "capability.effort_change_cache";

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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub wire_corrections: Vec<Value>,
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
    carries_temperature_facts: [bool; 3],
    carries_routing_facts: bool,
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
    /// An equal-version poll must replace an older LKG that did not request image counts.
    carries_image_counts: bool,
    /// An equal-version poll must replace an older LKG that did not request pixel bounds.
    carries_image_dimensions: bool,
    /// Whether any row of the source document carried a mode rate. Same reason as
    /// `carries_output_limits`: an LKG fetched before broca requested mode rates has the same
    /// version as the next poll and none of them, and reusing its index would keep every tier
    /// refused until fusiform happened to mint a new version.
    carries_mode_rates: bool,
    /// A poll requesting prices must replace an equal-version last-known-good catalog snapshot
    /// that was fetched without prices.
    carries_warm_rates: bool,
    /// Whether any row of the source document carried `model.kind`. Same reason as
    /// `carries_output_limits`: an LKG fetched before broca requested the kind has the same
    /// version as the next poll and no kinds, and reusing its index would keep admitting a
    /// non-chat model to `session.send` until fusiform happened to mint a new version.
    carries_model_kinds: bool,
    /// An equal-version poll can add modalities that an older prefix list did not request.
    carries_input_modalities: bool,
    /// A same-version poll can add names omitted by the previous prefix list.
    carries_display_names: bool,
    /// Whether any row of the source document carried `capability.server_side_fallback`. Same
    /// reason as `carries_output_limits`: fusiform serves the fact on current reads, so a
    /// last-known-good catalog written before broca requested it has the same version as the
    /// next poll and none of the fact. Reusing its index would leave every row reading "no
    /// server-side fallback" until fusiform happened to mint a new version.
    carries_server_side_fallback: bool,
    /// Refresh equal-version last-known-good data fetched before `mid_conversation_system` was requested.
    carries_mid_conversation_system: bool,
    /// Refresh equal-version last-known-good data fetched before `effort_change_cache` was
    /// requested, so its rows don't keep reading "no fact" until fusiform mints a new version.
    carries_effort_change_cache: bool,
    /// Present rows this catalog left out or kept from the catalog it replaced, because their
    /// facts could not be used. Shared with an equal-version republish, which reuses the index
    /// these describe.
    excluded_rows: Arc<[RowExclusion]>,
}

const TEMPERATURE_FACTS: [&str; 3] = [
    "capability.temperature",
    "limit.temperature_min",
    "limit.temperature_max",
];

fn response_carries_temperature_facts(response: &FusiformCatalogResponse) -> [bool; 3] {
    TEMPERATURE_FACTS.map(|key| {
        response
            .models
            .values()
            .any(|facts| facts.contains_key(key))
    })
}

/// Whether a response's model rows carry `model.kind`. Stops at the first row that has it.
fn response_carries_model_kinds(response: &FusiformCatalogResponse) -> bool {
    response
        .models
        .values()
        .any(|facts| facts.contains_key("model.kind"))
}

fn response_carries_input_modalities(response: &FusiformCatalogResponse) -> bool {
    response
        .models
        .values()
        .any(|facts| facts.contains_key("capability.input_modalities"))
}

fn response_carries_display_names(response: &FusiformCatalogResponse) -> bool {
    response
        .models
        .values()
        .any(|facts| facts.contains_key("model.name") || facts.contains_key("provider.name"))
}

fn response_carries_server_side_fallback(response: &FusiformCatalogResponse) -> bool {
    response
        .models
        .values()
        .any(|facts| facts.contains_key(SERVER_SIDE_FALLBACK_FACT))
}

fn response_carries_mid_conversation_system(response: &FusiformCatalogResponse) -> bool {
    response
        .models
        .values()
        .any(|facts| facts.contains_key(MID_CONVERSATION_SYSTEM_FACT))
}

fn response_carries_effort_change_cache(response: &FusiformCatalogResponse) -> bool {
    response
        .models
        .values()
        .any(|facts| facts.contains_key(EFFORT_CHANGE_CACHE_FACT))
}

fn response_carries_routing_facts(response: &FusiformCatalogResponse) -> bool {
    response.models.values().any(|facts| {
        facts
            .keys()
            .any(|key| key.starts_with("wire.") || key.starts_with("route."))
    })
}

impl LoadedCatalog {
    /// One WARN per excluded row. Called each time a catalog is installed, so a row that stays
    /// malformed is reported on every refresh rather than once and then forgotten.
    fn warn_excluded_rows(&self) {
        for row in self.excluded_rows.iter() {
            row.warn();
        }
    }
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

fn response_carries_image_counts(response: &FusiformCatalogResponse) -> bool {
    response
        .models
        .values()
        .any(|facts| facts.contains_key("limit.images_per_request"))
}

fn response_carries_image_dimensions(response: &FusiformCatalogResponse) -> bool {
    response.models.values().any(|facts| {
        facts.contains_key("limit.image_long_edge_px")
            || facts
                .keys()
                .any(|key| key.starts_with("limit.image_long_edge_px.above_images."))
    })
}

/// The bounded provider facts retained for admission. `raw` is always null, so cloning this spec
/// for one lookup never copies another model or an ingestion-only catalog field.
struct ServingProvider {
    spec: Option<ProviderSpec>,
    models: BTreeMap<String, ServingModel>,
}

/// Construction tracks whether a provider has an implemented renderer. Pending providers retain
/// bounded model metadata, so an unsupported chat route cannot hide a stated non-chat kind.
struct DraftServingProvider {
    spec: Option<ProviderSpec>,
    models: BTreeMap<String, ServingModel>,
}

/// The exact model facts retained between polls.
///
/// The capability flags remain three-state typed values, so an explicit `null` still fails
/// closed when a family would consume it, and a flag fusiform omits reads as Absent (see
/// `optional_capability_fact`).
#[derive(Clone)]
struct ServingModel {
    temperature: broca_core::provider::TemperaturePolicy,
    display_name: String,
    base_url: Option<String>,
    cost: CostSchedule,
    context_limit: Option<u64>,
    /// The model's published maximum output, `None` when fusiform serves null (its spelling of
    /// "unknown": it collapses an upstream zero to null) or omits the fact. Whether the value is
    /// usable as a default cap is decided at resolution, not here, so the index keeps what was
    /// served.
    output_limit: Option<u64>,
    images_per_request: Option<u32>,
    image_long_edge_px: Option<u32>,
    image_long_edge_px_above_images: BTreeMap<u32, u32>,
    attachment: CapabilityFlag,
    input_modalities: InputModalities,
    reasoning: CapabilityFlag,
    tool_call: CapabilityFlag,
    /// `capability.structured_output`. Like the three flags above, fusiform omits it on rows
    /// whose upstream does not state it, and absence reads as unknown rather than excluding the
    /// row.
    structured_output: CapabilityFlag,
    /// `capability.reasoning_options`: null, `[]` and a populated array stay distinct. A catalog
    /// from before fusiform served the fact reads as unstated, which defines no variant.
    reasoning_options: ReasoningOptions,
    /// `capability.reasoning_default`, when it is an effort its own options offer.
    reasoning_default_effort: Option<String>,
    /// `capability.server_side_fallback`: whether the runtime accepts a request that asks it to
    /// hand a refusal to another model itself. Absent and null both read as `None`, which callers
    /// treat as false: the fact is served on current reads only, so an older catalog or a
    /// point-in-time read carries no answer, never a claim. A value that is not a boolean is
    /// malformed; it is reported and reads as `None`, because an optional routing feature must
    /// not take a model offline.
    server_side_fallback: Option<bool>,
    mid_conversation_system: Option<bool>,
    /// `capability.effort_change_cache`: whether an effort change keeps the cached prefix. Absent,
    /// null or not a boolean reads as `None`, which the run counts as `false` (it rides).
    effort_change_cache: Option<bool>,
    /// Upstream mode names (`fast`, `priority`, ...) for which fusiform serves BOTH a priced
    /// input and a priced output rate, sorted. A mode missing either side is unpriced and is
    /// not listed; see `priced_modes_fact`.
    priced_modes: Vec<String>,
    /// `route.mode_wire_ids`: the model id this row's route serves an upstream mode at, by mode
    /// name, for a vendor that offers a mode as a model of its own (Kimi's `-highspeed` twin is
    /// `fast`). Empty when the row names none; see `mode_wire_ids_fact`.
    mode_wire_ids: BTreeMap<String, String>,
    wire_family: Option<WireFamilyId>,
    /// `model.kind` as served: `Some(kind)` when fusiform states one, `None` when it is absent
    /// or null. `None` means the catalog does not say, never that the model is a chat model.
    kind: Option<String>,
}

impl Default for ServingModel {
    fn default() -> Self {
        Self {
            temperature: Default::default(),
            display_name: String::new(),
            base_url: None,
            cost: CostSchedule::default(),
            context_limit: None,
            output_limit: None,
            images_per_request: None,
            image_long_edge_px: None,
            image_long_edge_px_above_images: BTreeMap::new(),
            attachment: CapabilityFlag::Absent,
            input_modalities: InputModalities::default(),
            reasoning: CapabilityFlag::Absent,
            tool_call: CapabilityFlag::Absent,
            structured_output: CapabilityFlag::Absent,
            reasoning_options: ReasoningOptions::Unstated,
            reasoning_default_effort: None,
            server_side_fallback: None,
            mid_conversation_system: None,
            effort_change_cache: None,
            priced_modes: Vec::new(),
            mode_wire_ids: BTreeMap::new(),
            wire_family: None,
            kind: None,
        }
    }
}

impl ServingModel {
    #[cfg(any(test, feature = "test-support"))]
    fn from_spec(
        model: &ModelSpec,
        model_family: WireFamilyId,
        provider_family: WireFamilyId,
        base_url: Option<String>,
    ) -> Self {
        Self {
            temperature: broca_core::provider::TemperaturePolicy {
                supported: match model.capabilities.temperature {
                    CapabilityFlag::Known(value) => Some(value),
                    _ => None,
                },
                min: None,
                max: None,
            },
            display_name: model.display_name.clone(),
            base_url,
            cost: model.cost.clone(),
            context_limit: model.limits.context,
            output_limit: model.limits.max_output,
            images_per_request: model.limits.images_per_request,
            image_long_edge_px: model.limits.image_long_edge_px,
            image_long_edge_px_above_images: model.limits.image_long_edge_px_above_images.clone(),
            attachment: model.capabilities.attachment,
            input_modalities: model.input_modalities,
            reasoning: model.capabilities.reasoning,
            tool_call: model.capabilities.tool_call,
            structured_output: model.capabilities.structured_output,
            reasoning_options: model.reasoning_options.clone(),
            reasoning_default_effort: model.reasoning_default_effort.clone(),
            server_side_fallback: model.server_side_fallback,
            mid_conversation_system: model.mid_conversation_system,
            effort_change_cache: model.effort_change_cache,
            // The legacy snapshot path carries no fusiform mode rates, so it offers no tier
            // that needs one.
            priced_modes: Vec::new(),
            mode_wire_ids: BTreeMap::new(),
            wire_family: (model_family != provider_family).then_some(model_family),
            // The vendored models.dev snapshot has no `model.kind`.
            kind: None,
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
                temperature: self
                    .temperature
                    .supported
                    .map_or(CapabilityFlag::Absent, CapabilityFlag::Known),
                tool_call: self.tool_call,
                interleaved: CapabilityFlag::Absent,
                structured_output: self.structured_output,
            },
            input_modalities: self.input_modalities,
            limits: Limits {
                context: self.context_limit,
                max_input: None,
                max_output: self.output_limit,
                images_per_request: self.images_per_request,
                image_long_edge_px: self.image_long_edge_px,
                image_long_edge_px_above_images: self.image_long_edge_px_above_images.clone(),
            },
            cost: self.cost.clone(),
            reasoning_options: self.reasoning_options.clone(),
            reasoning_default_effort: self.reasoning_default_effort.clone(),
            server_side_fallback: self.server_side_fallback,
            mid_conversation_system: self.mid_conversation_system,
            effort_change_cache: self.effort_change_cache,
            release_date: None,
            status: None,
            overlay: Overlay::default(),
            raw: Value::Null,
        }
    }
}

struct ServingCatalog {
    routing: BTreeMap<String, crate::routing::RoutingFacts>,
    providers: BTreeMap<String, ServingProvider>,
    overlay_removed_models: BTreeMap<String, BTreeSet<String>>,
    withdrawn_models: BTreeMap<String, BTreeSet<String>>,
    /// Every present row's model-batch facts, by catalog id, whether or not broca resolves a
    /// chat wire family for its provider. System One (decision and extraction models) is not
    /// served through a wire family, so a row such as `typesafe/jev-1.13.0`, which the chat
    /// serving index leaves out, still needs its kind, price and limits read from here.
    batch_rows: BTreeMap<String, ModelBatchRow>,
}

/// The facts a model-batch role (`classifier/v1`, `extractor/v1`) reads from one catalog row.
/// Absent is never zero: a fact the catalog does not state stays `None`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelBatchRow {
    /// Human display facts only. Missing or blank names never fall back to identifiers.
    pub model_name: Option<String>,
    pub provider_name: Option<String>,
    /// `model.kind` as served (`decision`, `extraction`, `chat`, ...); `None` when the row
    /// states no kind.
    pub kind: Option<String>,
    /// `limit.context`, the exact context window, when stated.
    pub context_tokens: Option<u64>,
    /// `capability.attachment` stated true.
    pub attachment: bool,
    /// `rate.input` in nanodollars per million tokens: `Some(0)` for a stated zero, `None`
    /// when the row does not price input.
    pub input_rate_nanos: Option<i64>,
    /// `rate.output` in nanodollars per million tokens, with the same rules as
    /// `input_rate_nanos`.
    pub output_rate_nanos: Option<i64>,
}

/// A fusiform rate as integer nanodollars per million tokens, for spend arithmetic that must
/// not pass through binary floating point. A rate finer than a nanodollar rounds up, so a
/// spend bound built on it never understates. `None` for an unpriced or unreadable rate.
fn batch_rate_nanos(value: Option<&Value>) -> Option<i64> {
    let rate = value?.as_object()?;
    if rate.get("state").and_then(Value::as_str) == Some("stated_zero")
        && !rate.contains_key("units")
        && !rate.contains_key("exponent")
    {
        return Some(0);
    }
    if rate.get("currency")?.as_str()? != "USD"
        || !matches!(rate.get("state")?.as_str()?, "priced" | "stated_zero")
    {
        return None;
    }
    let units = rate.get("units")?.as_u64()?;
    let exponent = rate.get("exponent")?.as_u64()?;
    if exponent > 18 || (rate.get("state")?.as_str()? == "stated_zero" && units != 0) {
        return None;
    }
    let nanos = if exponent <= 9 {
        u128::from(units) * 10u128.pow(9 - exponent as u32)
    } else {
        u128::from(units).div_ceil(10u128.pow(exponent as u32 - 9))
    };
    i64::try_from(nanos).ok()
}

fn batch_row_of(model: &ServingModel, facts: &BTreeMap<String, Value>) -> ModelBatchRow {
    ModelBatchRow {
        model_name: display_name_fact(facts, "model.name"),
        provider_name: display_name_fact(facts, "provider.name"),
        kind: model.kind.clone(),
        context_tokens: model.context_limit,
        attachment: model.attachment == CapabilityFlag::Known(true),
        input_rate_nanos: batch_rate_nanos(facts.get("rate.input")),
        output_rate_nanos: batch_rate_nanos(facts.get("rate.output")),
    }
}

fn display_name_fact(facts: &BTreeMap<String, Value>, key: &str) -> Option<String> {
    facts
        .get(key)?
        .as_str()
        .filter(|name| !name.trim().is_empty())
        .map(str::to_owned)
}

/// What one assembly of fusiform's model map produced.
struct AssembledCatalog {
    catalog: ServingCatalog,
    /// Every identity the response listed, including excluded rows.
    model_identities: BTreeSet<String>,
    /// Present rows whose facts could not be used, in identity order.
    excluded_rows: Vec<RowExclusion>,
}

/// A present fusiform row left out of the catalog because one of its facts could not be used.
///
/// Fusiform serves thousands of rows, a few of them hand-curated with only the facts their
/// vendor states. Refusing the whole catalog over one such row froze every other model at the
/// last accepted poll, three times. So a bad row is excluded on its own, and it is made visible
/// instead of silent: one WARN per excluded row on every refresh that installs the catalog, and
/// a count with the latest reason on `health.check` (`catalog_rows_excluded`). A systematic
/// upstream fault that spoils many rows therefore shows up as a large count rather than as a
/// quietly shrinking catalog.
///
/// A row the replaced catalog served keeps its earlier model, so an identity that was working
/// does not stop working because its latest facts are malformed. Only a row never seen before
/// (or one seen first after a restart, which loads the on-disk copy with no earlier catalog to
/// borrow from) goes missing.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RowExclusion {
    /// The fusiform identity, `provider_id/model_id`.
    model: String,
    /// The fact that could not be used, or `identity` when the key itself is malformed.
    fact: String,
    reason: String,
    /// Whether the replaced catalog's model for this identity is still being served.
    kept_previous: bool,
}

impl RowExclusion {
    fn from_error(identity: &str, error: CatalogError, kept_previous: bool) -> Self {
        let (fact, reason) = match error {
            CatalogError::FusiformFact { fact, reason, .. } => (fact, reason),
            other => (String::new(), other.to_string()),
        };
        Self {
            model: identity.to_string(),
            fact,
            reason,
            kept_previous,
        }
    }

    /// The one-line form used for the health reason.
    fn describe(&self) -> String {
        format!(
            "fusiform model '{}' has unusable fact '{}': {}; {}",
            self.model,
            self.fact,
            self.reason,
            if self.kept_previous {
                "serving its last-known-good model"
            } else {
                "not served"
            }
        )
    }

    fn warn(&self) {
        tracing::warn!(
            target: "catalog",
            model = %self.model,
            fact = %self.fact,
            reason = %self.reason,
            last_known_good_kept = self.kept_previous,
            "fusiform catalog row excluded: {}",
            self.describe(),
        );
    }
}

/// An immutable admission snapshot. Taking one clones an `Arc`; it performs no file or network IO.
///
/// This deliberately exposes only serving accessors. It does not dereference to the legacy
/// generic catalog, because doing so would require retaining or reconstructing that catalog.
#[derive(Clone)]
pub struct CatalogSnapshot(Arc<LoadedCatalog>);

impl CatalogSnapshot {
    pub(crate) fn temperature_policy(
        &self,
        provider: &str,
        model: &str,
    ) -> broca_core::provider::TemperaturePolicy {
        self.0
            .catalog
            .provider_and_model(provider, model)
            .map(|(_, model)| model.temperature)
            .unwrap_or_default()
    }
    pub fn routing_facts(
        &self,
        provider: &str,
        model: &str,
    ) -> Option<&crate::routing::RoutingFacts> {
        self.0.catalog.routing.get(&format!("{provider}/{model}"))
    }

    /// Admission refuses an unknown endpoint or grammar rather than borrowing another model's.
    pub fn resolve_route(
        &self,
        provider: &str,
        model: &str,
    ) -> Result<(WireFamilyId, String), String> {
        let (family, api) = self
            .routing_facts(provider, model)
            .ok_or_else(|| format!("catalog model '{provider}/{model}': missing routing facts"))?
            .route()
            .map_err(|reason| format!("catalog model '{provider}/{model}': {reason}"))?;
        Ok((self.wire_family(provider, model).unwrap_or(family), api))
    }

    pub fn successor(&self, provider: &str, model: &str) -> Option<&str> {
        self.routing_facts(provider, model)?.successor.as_deref()
    }

    /// Historical tier spellings are recognized only by a published backend id, not a suffix guess.
    pub fn code_assist_route(
        &self,
        provider: &str,
        model: &str,
        level: Option<&str>,
    ) -> Result<(String, String), String> {
        if let Some(facts) = self.routing_facts(provider, model) {
            if !self.contains_model(provider, model) {
                return Err(format!("catalog model '{provider}/{model}': no served row"));
            }
            return facts
                .tier(level)
                .map(|wire| (model.to_owned(), wire))
                .map_err(|reason| format!("catalog model '{provider}/{model}': {reason}"));
        }
        let prefix = format!("{provider}/");
        let wire = model.strip_prefix("antigravity-").unwrap_or(model);
        for (id, facts) in &self.0.catalog.routing {
            if let Some(alias) = id
                .strip_prefix(&prefix)
                .filter(|alias| self.contains_model(provider, alias))
            {
                if facts
                    .reasoning_wire_ids
                    .as_ref()
                    .is_some_and(|tiers| tiers.values().any(|id| id == wire))
                {
                    return Ok((
                        alias.to_string(),
                        level.map_or_else(
                            || Ok(wire.to_string()),
                            |level| facts.tier(Some(level)),
                        )?,
                    ));
                }
            }
        }
        Err(format!(
            "catalog model '{provider}/{model}': {}",
            crate::routing::MISSING_CODE_ASSIST_TIERS
        ))
    }
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
            .is_some_and(|provider| provider.spec.is_some() && !provider.models.is_empty())
    }

    /// The model-batch facts of the row `<provider_id>/<model_id>`, whether or not the chat
    /// serving index serves it. `None` when the catalog lists no present, readable row for it.
    pub fn model_batch_row(&self, catalog_id: &str) -> Option<ModelBatchRow> {
        self.0.catalog.batch_rows.get(catalog_id).cloned()
    }

    /// Every row whose stated `model.kind` is `kind`, by catalog id in sorted order.
    pub fn model_batch_rows_of_kind(&self, kind: &str) -> Vec<(String, ModelBatchRow)> {
        self.0
            .catalog
            .batch_rows
            .iter()
            .filter(|(_, row)| row.kind.as_deref() == Some(kind))
            .map(|(id, row)| (id.clone(), row.clone()))
            .collect()
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

    /// The model ids this model's route serves its modes at (`route.mode_wire_ids`), by mode
    /// name. Empty when the row names none, or the catalog does not list the model.
    pub fn mode_wire_ids(&self, provider_id: &str, model_id: &str) -> BTreeMap<String, String> {
        self.0
            .catalog
            .provider_and_model(provider_id, model_id)
            .map(|(_, model)| model.mode_wire_ids.clone())
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

    /// Whether the catalog serves `provider_id` itself: it has a wire family and rows for it. A
    /// provider it doesn't serve may still be reachable by borrowing another provider's rows.
    pub fn serves_provider(&self, provider_id: &str) -> bool {
        self.0.catalog.providers.contains_key(provider_id)
    }

    /// Resolve a model using a previously recorded family instead of the current overlay pin.
    pub(crate) fn model_for_serving_with_family(
        &self,
        provider_id: &str,
        model_id: &str,
        family: WireFamilyId,
    ) -> Option<(ProviderSpec, ModelSpec)> {
        self.0
            .catalog
            .model_for_serving_with_family(provider_id, model_id, family)
    }

    /// `capability.server_side_fallback` for this model on its resolved row: `Some(true)` or
    /// `Some(false)` when the catalog states it, `None` when it states nothing (an older
    /// catalog, a point-in-time read, or a value that was malformed). A caller reads `None` as
    /// false: the request field it gates is never sent without a stated `true`.
    pub fn server_side_fallback(&self, provider_id: &str, model_id: &str) -> Option<bool> {
        self.0
            .catalog
            .provider_and_model(provider_id, model_id)
            .and_then(|(_, model)| model.server_side_fallback)
    }

    /// The model's `model.kind`: `Some(kind)` when the catalog states one, `None` when it does
    /// not or the model is not in the serving index. `None` is "not stated"; a caller must not
    /// read it as `chat`.
    pub fn model_kind(&self, provider_id: &str, model_id: &str) -> Option<String> {
        self.0
            .catalog
            .provider_and_model(provider_id, model_id)
            .and_then(|(_, model)| model.kind.clone())
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
    ///
    /// `previous` is the catalog this one replaces, if any. A present row whose facts cannot be
    /// assembled keeps serving the model `previous` served for that identity; see
    /// [`RowExclusion`].
    fn from_fusiform(
        models: &BTreeMap<String, BTreeMap<String, Value>>,
        previous: Option<&ServingCatalog>,
    ) -> Result<AssembledCatalog, CatalogError> {
        Self::assemble_with_overlay(models, RuntimeOverlay::parse()?, previous)
    }

    /// [`Self::assemble_with_overlay`] with no previous catalog, for tests that only need the
    /// index and the identity set.
    #[cfg(test)]
    fn from_fusiform_with_overlay(
        models: &BTreeMap<String, BTreeMap<String, Value>>,
        overlay: RuntimeOverlay,
    ) -> Result<(Self, BTreeSet<String>), CatalogError> {
        Self::assemble_with_overlay(models, overlay, None)
            .map(|assembled| (assembled.catalog, assembled.model_identities))
    }

    /// The overlay as a parameter, so a test can hand in a `remove` for a model its fixture
    /// lists. Production always passes the compiled overlay; the seam exists because that
    /// overlay carried a real suppression (deepseek-chat) until upstream withdrew the id on
    /// 2026-09-18, and a test of how removal is RECORDED must not depend on a vendor keeping
    /// a dead row around to suppress.
    ///
    /// One bad row is isolated, not fatal. A present row whose identity does not split or
    /// whose facts cannot be read is left out of this catalog (or keeps the model `previous`
    /// served for it) and is recorded in `excluded_rows`; every other row still assembles.
    /// The row's identity is still recorded as known, so the vanished-identity guard in
    /// `refresh_inner` does not mistake an excluded row for a truncated response.
    ///
    /// What stays fatal is a problem with the document rather than with one row: an
    /// `existence` value this build does not understand (that vocabulary decides whether a
    /// model serves at all, so an unknown state means the protocol moved under us) and an
    /// overlay `add` colliding with a fusiform provider. Qualified facts, truncation and
    /// staleness are refused by `loaded` and `refresh_inner` before or after this runs.
    fn assemble_with_overlay(
        models: &BTreeMap<String, BTreeMap<String, Value>>,
        overlay: RuntimeOverlay,
        previous: Option<&ServingCatalog>,
    ) -> Result<AssembledCatalog, CatalogError> {
        let mut providers: BTreeMap<String, DraftServingProvider> = BTreeMap::new();
        let mut overlay_removed_models: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        let mut withdrawn_models: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        let mut model_identities = BTreeSet::new();
        let mut excluded_rows = Vec::new();
        let mut batch_rows = BTreeMap::new();
        let routing: BTreeMap<_, _> = models
            .iter()
            .map(|(id, facts)| (id.clone(), crate::routing::RoutingFacts::decode(facts)))
            .collect();

        for (identity, facts) in models {
            let Some((provider_id, model_id)) = identity
                .split_once('/')
                .filter(|(provider, model)| !provider.is_empty() && !model.is_empty())
            else {
                // No provider/model pair means no slot to serve it in and no previous model
                // to keep, so the row is simply left out.
                excluded_rows.push(RowExclusion {
                    model: identity.clone(),
                    fact: "identity".to_string(),
                    reason: CatalogError::FusiformModelIdentity(identity.clone()).to_string(),
                    kept_previous: false,
                });
                model_identities.insert(identity.clone());
                continue;
            };
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
                        let legacy_family =
                            match resolve_provider_family(&provider_id, overlay_family) {
                                FamilyResolution::Supported { family, .. } => Some(family),
                                FamilyResolution::Pending { .. } => None,
                            };
                        let family = routing[identity]
                            .npm
                            .as_ref()
                            .and_then(|npm| {
                                crate::family::wire_family(
                                    &npm.value,
                                    routing[identity]
                                        .shape
                                        .as_ref()
                                        .map(|shape| shape.value.as_str()),
                                )
                            })
                            .or_else(|| {
                                crate::family::credential_persona_route(&provider_id, "oauth")
                                    .map(|route| route.0)
                            })
                            .or(legacy_family);
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
                        match serving_model_from_facts(identity, facts) {
                            Ok(mut model) => {
                                batch_rows.insert(identity.clone(), batch_row_of(&model, facts));
                                if let Some(patch) = overlay.model_patch(&provider_id, &model_id) {
                                    patch.apply(&mut model);
                                }
                                if let Ok((family, api)) = routing[identity].route() {
                                    model.wire_family = Some(family);
                                    if let Some(override_family) = overlay
                                        .model_patch(&provider_id, &model_id)
                                        .and_then(|patch| match patch.wire_family {
                                            Patch::Set(family) => family,
                                            Patch::Missing => None,
                                        })
                                    {
                                        model.wire_family = Some(override_family);
                                    }
                                    model.base_url = Some(api.clone());
                                    if provider.spec.is_none() {
                                        provider.spec =
                                            Some(serving_provider_spec(&provider_id, family, api));
                                    }
                                }
                                provider.models.insert(model_id.clone(), model);
                            }
                            Err(error) => {
                                // The previous model already had the overlay applied when it
                                // was assembled, so it is reinstated as it was served.
                                let kept = previous
                                    .and_then(|catalog| {
                                        catalog.provider_and_model(&provider_id, &model_id)
                                    })
                                    .map(|(_, model)| model.clone());
                                excluded_rows.push(RowExclusion::from_error(
                                    identity,
                                    error,
                                    kept.is_some(),
                                ));
                                if let Some(model) = kept {
                                    provider.models.insert(model_id.clone(), model);
                                }
                            }
                        }
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
                // Renderer choices describe an existing row; they are not evidence that a
                // missing or retired model exists in the upstream catalog.
                if model_patch.is_renderer_pin_only() {
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

        // A missing renderer must not hide kind, limits or prices from admission. Keep the
        // bounded metadata for pending providers, but never materialize a serving spec for them.
        let providers = providers
            .into_iter()
            .map(|(id, provider)| {
                (
                    id,
                    ServingProvider {
                        spec: provider.spec,
                        models: provider.models,
                    },
                )
            })
            .collect();

        Ok(AssembledCatalog {
            catalog: Self {
                routing,
                providers,
                overlay_removed_models,
                withdrawn_models,
                batch_rows,
            },
            model_identities,
            excluded_rows,
        })
    }

    #[cfg(any(test, feature = "test-support"))]
    fn from_catalog(catalog: &Catalog) -> Self {
        let mut providers = BTreeMap::new();
        let mut routing = BTreeMap::new();
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
                if let Some(raw) = model_entries[model_id]
                    .as_object()
                    .filter(|raw| raw.contains_key("route.reasoning_wire_ids"))
                {
                    let facts = raw
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect();
                    routing.insert(
                        format!("{provider_id}/{model_id}"),
                        crate::routing::RoutingFacts::decode(&facts),
                    );
                }
                let Some((model_provider, model)) =
                    catalog.model_for_serving(provider_id, model_id)
                else {
                    continue;
                };
                let model_base_url = (model_provider.base_url != spec.base_url)
                    .then(|| model_provider.base_url.clone());
                models.insert(
                    model_id.clone(),
                    ServingModel::from_spec(
                        &model,
                        model_provider.wire_family,
                        *provider_family,
                        model_base_url,
                    ),
                );
            }
            providers.insert(
                provider_id.clone(),
                ServingProvider {
                    spec: Some(spec),
                    models,
                },
            );
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
            routing,
            providers,
            overlay_removed_models,
            withdrawn_models,
            // The vendored models.dev snapshot states no `model.kind`, so it lists no
            // model-batch row.
            batch_rows: BTreeMap::new(),
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
        self.provider_and_model(provider_id, model_id)
            .is_some_and(|(provider, _)| provider.spec.is_some())
    }

    fn wire_family(&self, provider_id: &str, model_id: &str) -> Option<WireFamilyId> {
        let (provider, model) = self.provider_and_model(provider_id, model_id)?;
        model
            .wire_family
            .or_else(|| provider.spec.as_ref().map(|spec| spec.wire_family))
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
        let spec = provider.spec.as_ref()?;
        let family = model.wire_family.unwrap_or(spec.wire_family);
        let mut provider_spec = if family == spec.wire_family {
            spec.clone()
        } else {
            serving_provider_spec(provider_id, family, spec.base_url.clone())
        };
        if let Some(base_url) = &model.base_url {
            provider_spec.base_url.clone_from(base_url);
        }
        Some((provider_spec, model.to_spec(model_id)))
    }

    fn model_for_serving_with_family(
        &self,
        provider_id: &str,
        model_id: &str,
        family: WireFamilyId,
    ) -> Option<(ProviderSpec, ModelSpec)> {
        let (provider, model) = self.provider_and_model(provider_id, model_id)?;
        let spec = provider.spec.as_ref()?;
        let base_url = model
            .base_url
            .clone()
            .unwrap_or_else(|| spec.base_url.clone());
        Some((
            serving_provider_spec(provider_id, family, base_url),
            model.to_spec(model_id),
        ))
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

pub(crate) fn serving_provider_spec(
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
    #[serde(default, rename = "_comment")]
    _comment: Option<String>,
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
    #[serde(default, rename = "_comment")]
    _comment: Option<String>,
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
    base_url: Patch<Option<String>>,
    #[serde(default)]
    limit: Patch<Option<RuntimeLimitOverlay>>,
}

impl RuntimeModelOverlay {
    fn is_renderer_pin_only(&self) -> bool {
        matches!(self.name, Patch::Missing)
            && matches!(self.cost, Patch::Missing)
            && matches!(self.attachment, Patch::Missing)
            && matches!(self.reasoning, Patch::Missing)
            && matches!(self.tool_call, Patch::Missing)
            && matches!(self.structured_output, Patch::Missing)
            && matches!(self.limit, Patch::Missing)
    }

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
        if let Patch::Set(value) = &self.base_url {
            model.base_url.clone_from(value);
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

/// Assemble one present row's serving model from its facts, before any overlay patch.
///
/// An error here excludes this row only (see [`RowExclusion`]); it never refuses the catalog.
/// Which absent facts are tolerated, and why, is documented on each reader below.
fn serving_model_from_facts(
    identity: &str,
    facts: &BTreeMap<String, Value>,
) -> Result<ServingModel, CatalogError> {
    Ok(ServingModel {
        temperature: temperature_facts(identity, facts),
        display_name: display_name_fact(facts, "model.name").unwrap_or_default(),
        base_url: None,
        cost: warm_cost_fact(facts),
        context_limit: context_fact(identity, facts)?,
        output_limit: output_fact(identity, facts)?,
        images_per_request: images_per_request_fact(identity, facts)?,
        image_long_edge_px: positive_u32_fact(identity, facts, "limit.image_long_edge_px")?,
        image_long_edge_px_above_images: image_long_edge_conditionals_fact(identity, facts)?,
        attachment: optional_capability_fact(identity, facts, "capability.attachment")?,
        input_modalities: facts
            .get("capability.input_modalities")
            .map(InputModalities::from_value)
            .unwrap_or_default(),
        reasoning: reasoning_fact(identity, facts)?,
        tool_call: optional_capability_fact(identity, facts, "capability.tool_call")?,
        structured_output: optional_capability_fact(
            identity,
            facts,
            "capability.structured_output",
        )?,
        reasoning_options: reasoning_options_fact(identity, facts)?,
        reasoning_default_effort: reasoning_default_effort_fact(identity, facts),
        server_side_fallback: server_side_fallback_fact(identity, facts),
        mid_conversation_system: facts
            .get(MID_CONVERSATION_SYSTEM_FACT)
            .and_then(Value::as_bool),
        effort_change_cache: facts.get(EFFORT_CHANGE_CACHE_FACT).and_then(Value::as_bool),
        priced_modes: priced_modes_fact(identity, facts)?,
        mode_wire_ids: mode_wire_ids_fact(identity, facts),
        wire_family: None,
        kind: model_kind_fact(identity, facts)?,
    })
}

/// Read one capability fact that is present: a boolean is known, an explicit null is the
/// producer saying the upstream does not state it (`Unknown`), and any other value is
/// malformed. A malformed value excludes the row rather than guessing.
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
/// catalog does not say), never as an exclusion of the row. A present value must still be a
/// boolean or null.
///
/// Fusiform omits a flag when the vendor does not state it; hand-curated decision and
/// extraction rows (jev, clef, clairic) carry no reasoning or tool-call flag at all. Absent is
/// safe for the three flags read here because nothing refuses or changes a send on them:
///
/// - `capability.structured_output`: `broca_provider::structured_output::decide` treats Absent
///   like null (`catalog_unknown`): the schema is sent unconstrained and the runner validates
///   the answer when the run ends. The send is not refused.
/// - `capability.tool_call`: no admission or render path reads it. A send carrying tools is
///   rendered with its tools whatever the flag says, and the provider accepts or rejects them,
///   so a row with unstated tool support behaves exactly like one with unstated structured
///   output: sent, not refused.
/// - `capability.attachment`: likewise carried on the model spec and read by no admission or
///   render path.
///
/// `capability.reasoning` is NOT read this way; see [`reasoning_fact`].
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

/// `capability.server_side_fallback`: `Some(true)` or `Some(false)` when fusiform states one,
/// `None` when it is absent or null. A value of any other type is malformed, and this reader is
/// deliberately the one place a malformed row fact does not exclude its row.
///
/// Every other reader here fails closed: a fact it cannot read means the row's route or spec
/// cannot be trusted. This fact is different because nothing about the row depends on it. It
/// only asks for an extra request field that the runtime may answer with a handoff, so a value
/// broca cannot read means "do not ask", which is the same as false. Excluding the row instead
/// would take a model offline over one optional feature; the WARN names the row so the producer
/// of the bad value can find it.
fn server_side_fallback_fact(identity: &str, facts: &BTreeMap<String, Value>) -> Option<bool> {
    match facts.get(SERVER_SIDE_FALLBACK_FACT) {
        Some(Value::Bool(value)) => Some(*value),
        Some(Value::Null) | None => None,
        Some(value) => {
            tracing::warn!(
                target: "catalog",
                model = identity,
                fact = SERVER_SIDE_FALLBACK_FACT,
                value = %value,
                "a catalog fact has an unexpected value; it is read as unstated and the row \
                 still serves"
            );
            None
        }
    }
}

/// `route.mode_wire_ids`, an object from mode name to model id. A value that is not such an
/// object is reported and read as naming none, like every optional routing fact: it must not
/// take the row offline, and a caller asking for the mode is then refused as unmapped.
fn mode_wire_ids_fact(identity: &str, facts: &BTreeMap<String, Value>) -> BTreeMap<String, String> {
    let fact = "route.mode_wire_ids";
    let parsed = match facts.get(fact) {
        None | Some(Value::Null) => Some(BTreeMap::new()),
        Some(Value::Object(ids)) => ids
            .iter()
            .map(|(mode, id)| {
                Some((
                    mode.clone(),
                    id.as_str().filter(|id| !id.is_empty())?.to_owned(),
                ))
            })
            .collect(),
        Some(_) => None,
    };
    parsed.unwrap_or_else(|| {
        tracing::warn!(
            target: "catalog",
            model = identity,
            fact,
            "a catalog fact has an unexpected value; it is read as unstated and the row still \
             serves"
        );
        BTreeMap::new()
    })
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
/// is neither null nor an object is malformed and excludes the row, like every other fact.
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

/// Read `model.kind`. Absent and null both mean the catalog does not state a kind and decode to
/// `None`; that is never read as `chat`. A present value of another type is malformed and
/// excludes this row only (keeping its last-known-good model if one was served), like every
/// other fact.
///
/// The kind changes no other fact's reading: a decision or extraction row that omits the chat
/// facts decodes them as unstated exactly like any other row (see `optional_capability_fact`,
/// `reasoning_fact` and `context_fact`).
fn model_kind_fact(
    identity: &str,
    facts: &BTreeMap<String, Value>,
) -> Result<Option<String>, CatalogError> {
    match facts.get("model.kind") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(kind)) => Ok(Some(kind.clone())),
        Some(value) => Err(CatalogError::FusiformFact {
            model: identity.to_string(),
            fact: "model.kind".to_string(),
            reason: format!("expected string or null, found {}", value_kind(value)),
        }),
    }
}

/// Read `limit.context`. Null and absent both mean the window is unknown and decode to `None`;
/// a present value that is not an unsigned integer excludes the row.
///
/// Absent is read like null because `None` already means "unknown" to every consumer, never
/// "unlimited" or zero (about 120 live rows serve an explicit null today):
///
/// - Admission (`context_limit_for_episode` in broca-module-serve) freezes the caller's
///   `context_limit`, else this value, else `None`; `None` stays unknown in the run config.
/// - The default output cap (`default_max_output_tokens` in `live.rs`) uses the output limit
///   as served, or the fixed fallback, when the context is unknown. Nothing is sized from a
///   window that was never published.
/// - The pre-send ceiling check (`compaction::view::before_send` in broca-core) cannot compare
///   against an unknown window and sends; if the provider then refuses for length, that refusal
///   starts the ceiling ride, carrying the limit the provider stated. Compaction notices and the
///   setup record omit the window rather than reporting one.
///
/// Jev, for one, states no exact context size, so it has no `limit.context`.
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
        None => Ok(None),
    }
}

/// Read `limit.output`. Null and absent both mean "unknown" and decode to `None`, never to a
/// number: fusiform collapses an upstream zero to null, so a synthesised value here would be a
/// cap nobody published.
///
/// Absent is tolerated because this fact only chooses a default and has a documented fallback;
/// an LKG written before broca requested the fact carries none, and it must keep loading. A
/// present value of the wrong type is still an error, which excludes the row.
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

/// Read a positive image count that fits the media profile's counter. Absent or null is unknown;
/// malformed bounds exclude the row rather than silently falling back to a different limit.
fn images_per_request_fact(
    identity: &str,
    facts: &BTreeMap<String, Value>,
) -> Result<Option<u32>, CatalogError> {
    positive_u32_fact(identity, facts, "limit.images_per_request")
}

fn positive_u32_fact(
    identity: &str,
    facts: &BTreeMap<String, Value>,
    key: &str,
) -> Result<Option<u32>, CatalogError> {
    match facts.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_u64()
            .and_then(|count| u32::try_from(count).ok())
            .filter(|&count| count > 0)
            .map(Some)
            .ok_or_else(|| CatalogError::FusiformFact {
                model: identity.to_string(),
                fact: key.to_string(),
                reason: format!(
                    "expected positive 32-bit unsigned integer or null, found {}",
                    value_kind(value)
                ),
            }),
    }
}

/// Conditional bounds count image and document blocks across the request, including history.
/// Validate even without a base bound so malformed facts follow the other limit readers.
fn image_long_edge_conditionals_fact(
    identity: &str,
    facts: &BTreeMap<String, Value>,
) -> Result<BTreeMap<u32, u32>, CatalogError> {
    let mut limits = BTreeMap::new();
    for key in facts.keys() {
        let Some(suffix) = key.strip_prefix("limit.image_long_edge_px.above_images.") else {
            continue;
        };
        let above_count = suffix
            .parse::<u32>()
            .map_err(|_| CatalogError::FusiformFact {
                model: identity.to_string(),
                fact: key.clone(),
                reason: "expected 32-bit unsigned integer image count suffix".to_string(),
            })?;
        if let Some(side) = positive_u32_fact(identity, facts, key)? {
            limits.insert(above_count, side);
        }
    }
    Ok(limits)
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
            Ok(Some(response)) => match loaded(&response, CatalogSource::LastKnownGood, None) {
                Ok(catalog) => {
                    // Boot has no earlier catalog, so an unusable row here is simply excluded.
                    catalog.warn_excluded_rows();
                    (Some(Arc::new(catalog)), None)
                }
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
        for correction in &response.wire_corrections {
            tracing::info!(target: "catalog", correction = %correction, "fusiform wire correction");
        }
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

    fn refresh_inner(
        &self,
        mut response: FusiformCatalogResponse,
    ) -> Result<(), CatalogRefreshError> {
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
                // Equal versions cannot withdraw a published name. Preserve those facts in
                // the persisted response too, so a nameless poll cannot erase them on restart.
                if current.carries_display_names {
                    for (identity, facts) in &mut response.models {
                        if let Some(row) = current.catalog.batch_rows.get(identity) {
                            for (key, cached) in [
                                ("model.name", &row.model_name),
                                ("provider.name", &row.provider_name),
                            ] {
                                if display_name_fact(facts, key).is_none() {
                                    if let Some(name) = cached {
                                        facts.insert(key.into(), Value::String(name.clone()));
                                    }
                                }
                            }
                        }
                    }
                }
                // Same version, so the facts of the rows the index already holds are taken as
                // unchanged and the existing index is reused — but only if the response is
                // COMPLETE. A truncated or
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
                // An equal version can still GAIN rows. Fusiform compiles some model data into
                // its own build, and a deploy that adds such a row does not move
                // catalog_version. Reusing the index there would leave the new model unknown, so
                // every send naming it would be refused for lacking a row until some unrelated
                // change minted a new version. Taken like a newly carried fact: rebuild and
                // install. Decided on the identity set alone, like the completeness check above,
                // so an unchanged poll still pays no materialization.
                let gains_rows = response
                    .models
                    .keys()
                    .any(|identity| !current.model_identities.contains(identity));
                // `loaded` rejects responses with withheld or uncertain facts. Keep that check
                // ahead of the version fast path: persisting a rejected response would leave an
                // LKG that `open` refuses after restart, even while the in-memory catalog keeps
                // serving its last accepted snapshot.
                if gains_rows
                    || response_carries_output_limits(&response) != current.carries_output_limits
                    || response_carries_image_counts(&response) != current.carries_image_counts
                    || response_carries_image_dimensions(&response)
                        != current.carries_image_dimensions
                    || response_carries_mode_rates(&response) != current.carries_mode_rates
                    || response_carries_warm_rates(&response) != current.carries_warm_rates
                    || response_carries_model_kinds(&response) != current.carries_model_kinds
                    || response_carries_input_modalities(&response)
                        != current.carries_input_modalities
                    || response_carries_routing_facts(&response) != current.carries_routing_facts
                    || response_carries_display_names(&response) != current.carries_display_names
                    || response_carries_server_side_fallback(&response)
                        != current.carries_server_side_fallback
                    || response_carries_mid_conversation_system(&response)
                        != current.carries_mid_conversation_system
                    || response_carries_effort_change_cache(&response)
                        != current.carries_effort_change_cache
                    || response_carries_temperature_facts(&response)
                        != current.carries_temperature_facts
                {
                    let rebuilt = Arc::new(loaded(
                        &response,
                        CatalogSource::Fusiform,
                        Some(&current.catalog),
                    )?);
                    if let Some(path) = &self.lkg_path {
                        persist_bootable_lkg(path, &response, &rebuilt.excluded_rows)?;
                    }
                    self.install(rebuilt);
                    return Ok(());
                }

                // Persist even though no fact changed: the LKG carries resolved_at_ms, and a
                // restart should recover the freshest proof that fusiform answered.
                if let Some(path) = &self.lkg_path {
                    persist_bootable_lkg(path, &response, &current.excluded_rows)?;
                }
                let refreshed = Arc::new(LoadedCatalog {
                    carries_routing_facts: current.carries_routing_facts,
                    catalog: Arc::clone(&current.catalog),
                    model_identities: Arc::clone(&current.model_identities),
                    resolved_at_ms: response.resolved_at_ms,
                    catalog_version: current.catalog_version,
                    source: CatalogSource::Fusiform,
                    carries_output_limits: current.carries_output_limits,
                    carries_image_counts: current.carries_image_counts,
                    carries_image_dimensions: current.carries_image_dimensions,
                    carries_mode_rates: current.carries_mode_rates,
                    carries_warm_rates: current.carries_warm_rates,
                    carries_model_kinds: current.carries_model_kinds,
                    carries_input_modalities: current.carries_input_modalities,
                    carries_display_names: current.carries_display_names,
                    carries_server_side_fallback: current.carries_server_side_fallback,
                    carries_mid_conversation_system: current.carries_mid_conversation_system,
                    carries_effort_change_cache: current.carries_effort_change_cache,
                    carries_temperature_facts: current.carries_temperature_facts,
                    excluded_rows: Arc::clone(&current.excluded_rows),
                });
                self.install(refreshed);
                return Ok(());
            }
        }

        // The catalog being replaced, so a row that served before and now arrives malformed
        // keeps its earlier model instead of disappearing.
        let next = Arc::new(loaded(
            &response,
            CatalogSource::Fusiform,
            installed.as_ref().map(|current| current.catalog.as_ref()),
        )?);
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
            // Reachable without any fusiform-side fault: pollers sharing a catalog snapshot
            // path have no cross-process ordering, even though persist_lkg is atomic per write.
            // An older poll can finish writing last and overwrite a newer snapshot on disk.
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
            persist_bootable_lkg(path, &response, &next.excluded_rows)?;
        }
        self.install(next);
        Ok(())
    }

    /// Publish an accepted catalog and report its excluded rows.
    fn install(&self, next: Arc<LoadedCatalog>) {
        next.warn_excluded_rows();
        *self.current.write().expect("catalog cache lock poisoned") = Some(next);
    }

    /// Install a document without touching disk. This is intended for focused tests that prove the
    /// admission path remains independent from a concurrently slow or failed poll.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-support"))]
    pub fn install_for_test(
        &self,
        response: FusiformCatalogResponse,
        source: CatalogSource,
    ) -> Result<(), CatalogRefreshError> {
        let next = Arc::new(loaded(&response, source, None)?);
        *self.current.write().expect("catalog cache lock poisoned") = Some(next);
        Ok(())
    }

    /// Install an already assembled catalog for tests which exercise legacy snapshot fixtures.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-support"))]
    pub fn install_parsed_for_test(
        &self,
        catalog: &Catalog,
        resolved_at_ms: i64,
        source: CatalogSource,
    ) {
        let next = Arc::new(LoadedCatalog {
            carries_routing_facts: false,
            catalog: Arc::new(ServingCatalog::from_catalog(catalog)),
            model_identities: Arc::new(BTreeSet::new()),
            resolved_at_ms,
            catalog_version: 0,
            source,
            carries_output_limits: true,
            carries_image_counts: true,
            carries_image_dimensions: false,
            carries_mode_rates: false,
            carries_warm_rates: true,
            // The vendored snapshot serves no `model.kind`.
            carries_model_kinds: false,
            carries_input_modalities: false,
            carries_display_names: false,
            // The vendored snapshot serves no `capability.server_side_fallback`.
            carries_server_side_fallback: false,
            carries_mid_conversation_system: false,
            carries_effort_change_cache: false,
            carries_temperature_facts: [false; 3],
            excluded_rows: Arc::from([]),
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
            rows_excluded: snapshot.0.excluded_rows.len(),
            rows_excluded_reason: snapshot.0.excluded_rows.last().map(RowExclusion::describe),
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
    /// Present rows the served catalog left out or kept at their previous model because their
    /// facts could not be used. Not a refusal: the catalog around them refreshed. Zero is the
    /// healthy reading.
    pub rows_excluded: usize,
    /// The last excluded row in identity order, naming the model, the fact and the reason, and
    /// whether its last-known-good model is still served. `None` when nothing is excluded.
    pub rows_excluded_reason: Option<String>,
}

/// Assemble a response into a serving catalog. `previous` is the catalog being replaced (the
/// installed one on refresh, `None` on boot or in tests); rows that cannot be assembled keep
/// the model it served for them.
fn loaded(
    response: &FusiformCatalogResponse,
    source: CatalogSource,
    previous: Option<&ServingCatalog>,
) -> Result<LoadedCatalog, CatalogError> {
    if !response.withheld.is_empty() || !response.uncertain.is_empty() {
        return Err(CatalogError::FusiformQualifiedFacts {
            withheld: response.withheld.len(),
            uncertain: response.uncertain.len(),
        });
    }
    let assembled = ServingCatalog::from_fusiform(&response.models, previous)?;
    Ok(LoadedCatalog {
        carries_routing_facts: response_carries_routing_facts(response),
        catalog: Arc::new(assembled.catalog),
        model_identities: Arc::new(assembled.model_identities),
        resolved_at_ms: response.resolved_at_ms,
        catalog_version: response.catalog_version,
        source,
        carries_output_limits: response_carries_output_limits(response),
        carries_image_counts: response_carries_image_counts(response),
        carries_image_dimensions: response_carries_image_dimensions(response),
        carries_mode_rates: response_carries_mode_rates(response),
        carries_warm_rates: response_carries_warm_rates(response),
        carries_model_kinds: response_carries_model_kinds(response),
        carries_input_modalities: response_carries_input_modalities(response),
        carries_display_names: response_carries_display_names(response),
        carries_server_side_fallback: response_carries_server_side_fallback(response),
        carries_mid_conversation_system: response_carries_mid_conversation_system(response),
        carries_effort_change_cache: response_carries_effort_change_cache(response),
        carries_temperature_facts: response_carries_temperature_facts(response),
        excluded_rows: assembled.excluded_rows.into(),
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

/// The process-wide catalog slot. Every test that installs its own catalog takes this gate for
/// as long as it resolves, because the slot itself is shared by the whole test binary: without
/// it, two tests' catalogs can replace each other between an install and the lookup it was
/// meant to answer. Production installs once from the fusiform poll and needs no gate.
pub fn test_catalog_lock() -> &'static std::sync::Mutex<()> {
    static GATE: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    GATE.get_or_init(|| std::sync::Mutex::new(()))
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
///
/// `stated_zero` with no amount (`{"state":"stated_zero"}`, how fusiform serves "output tokens
/// are free") is a known price of zero, not a missing one. With an amount it must still be a
/// USD zero, so a stated zero carrying non-zero units stays unavailable.
fn warm_rate(value: Option<&Value>) -> Option<f64> {
    let rate = value?.as_object()?;
    if rate.get("state").and_then(Value::as_str) == Some("stated_zero")
        && !rate.contains_key("units")
        && !rate.contains_key("exponent")
    {
        return Some(0.0);
    }
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
mod tests;

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
            wire_corrections: vec![],
        };
        let seen = std::rc::Rc::new(std::cell::Cell::new(0));
        let output = seen.clone();
        observe(
            move |path| {
                assert_private(path);
                output.set(output.get() + 1);
            },
            || {
                super::lkg::persist_lkg(&path, &response).unwrap();
                super::lkg::persist_lkg(&path, &response).unwrap();
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
