// The ChatGPT header construction and account-claim extraction follow OpenCode's codex.ts
// (MIT). Copyright (c) 2025 opencode. See THIRD-PARTY-NOTICES.md for the license notice.
// broca adds vault credential resolution, frozen session scope, and non-empty claim checks.

//! The catalog → live provider bridge: resolve a model id + auth into a runnable
//! `Box<dyn Provider>`. Shared by every harness that builds a live provider (the
//! `broca-run` CLI and the `broca-module-serve` module binary) so the resolution logic
//! (catalog lookup, base_url override, and auth shaping)
//! lives in ONE place — a harness can't drift from another on how a model resolves.
//!
//! Behind the `live` feature (it pulls the provider's live transport).

use std::sync::Arc;

pub use broca_provider::credential_refresh::SharedSecretRefresher;
use broca_provider::credential_refresh::{CredentialRefresher, ShapedSecretRefresher};
use broca_provider::families::google_codeassist::tier_thinking_config;
use broca_provider::families::{
    apply_anthropic_apikey_quirks, frozen_config_for_family, openai_chat_frozen_config,
    renderer_version_for, resolve_frozen,
};
pub use broca_provider::policy::{CrossModelThinking, ThinkingEchoScope};
use broca_provider::policy::{ReasoningPolicy, SystemPromptPolicy};
pub use broca_provider::service_tier::ServiceTier;
use broca_provider::spec::{AuthKind, CapabilityFlag, ReasoningOptions};
pub use broca_provider::variant::FrozenVariant;
use broca_provider::variant::{freeze_variant, resolve_variant};
use broca_provider::{live_provider_with_clock_and_refresher, ProviderHttpClient};
// Re-exported so a consumer (the module executor) can name the resolved family for the
// (provider, auth-method) consistency guard without depending on broca-provider directly.
pub use broca_provider::spec::WireFamilyId;
use broca_provider::wire::AuthHandle;
pub use broca_provider::wire::{
    CacheClass, CachePolicy, CacheTail, CacheTtl, ClaudeCodePersona, FrozenRenderConfig,
    MarkerSyntax,
};

/// How a provider authenticates for a run. The stable token is also the first segment of the
/// credential id used by the vault-backed module.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AuthMethod {
    /// A static API key. Anthropic uses this method to select API-key-specific body policies.
    ApiKey,
    /// Provider OAuth, including Anthropic's Claude-subscription request shape.
    Oauth,
    /// Google Antigravity / Code-Assist OAuth.
    Antigravity,
    /// OpenAI ChatGPT-subscription OAuth.
    Chatgpt,
}

impl AuthMethod {
    /// The stable credential-id token for this method.
    pub fn as_token(self) -> &'static str {
        match self {
            Self::ApiKey => "apikey",
            Self::Oauth => "oauth",
            Self::Antigravity => "antigravity",
            Self::Chatgpt => "chatgpt",
        }
    }

    /// Parse the first credential-id segment.
    pub fn from_token(token: &str) -> Option<Self> {
        match token {
            "apikey" => Some(Self::ApiKey),
            "oauth" => Some(Self::Oauth),
            "antigravity" => Some(Self::Antigravity),
            "chatgpt" => Some(Self::Chatgpt),
            _ => None,
        }
    }
}

/// User-facing Antigravity routes that select a different backend tier on the wire. This mirrors
/// `packages/core/src/model-registry.ts` at `GEMINI_35_FLASH_ROUTES`/`RESOLVER_ALIASES`; the
/// corresponding output limits come from `packages/opencode/src/plugin/request.ts` at
/// `getAgyMaxOutputTokens`. Mapping here, before freeze, means the frozen `wire_model_id` already
/// holds the tier id and resumes reproduce the same rendered bytes without consulting this table.
const ANTIGRAVITY_TIER_ALIASES: &[(&str, &str)] = &[
    ("gemini-3.5-flash", "gemini-3.5-flash-low"),
    ("gemini-3.5-flash-high", "gemini-3-flash-agent"),
    ("gemini-3.5-flash-low", "gemini-3.5-flash-extra-low"),
    // Live tier wire ids pass through unchanged; bare slugs select their default tier.
    ("gemini-3.8-flash", "gemini-3.8-flash-medium"),
    ("gemini-4-argon", "gemini-4-argon-medium"),
    ("gemini-3-flash", "gemini-3-flash-medium"),
    ("gemini-3-pro", "gemini-3-pro-low"),
];

/// Routes Google withdrew from Antigravity's daily endpoint, paired with a live replacement.
/// AGAUTH's two `fetchAvailableModels` captures just before 2026-10-14T13:11Z, using clients
/// 1.3.3 and 1.3.5, returned the same 29 routes. Compared with the 2026-10-12 map, these tier
/// ids disappeared; their bare aliases must not reach the backend either. Update this list only
/// from a fresh capture, not guesses from a 404 (which can also mean a stale client version).
const RETIRED_ANTIGRAVITY_ROUTES: &[(&str, &str)] = &[
    ("gemini-3.7-flash", "google/antigravity-gemini-3.8-flash"),
    (
        "gemini-3.7-flash-low",
        "google/antigravity-gemini-3.8-flash",
    ),
    (
        "gemini-3.7-flash-medium",
        "google/antigravity-gemini-3.8-flash",
    ),
    (
        "gemini-3.7-flash-high",
        "google/antigravity-gemini-3.8-flash",
    ),
    ("gemini-3.6-flash", "google/antigravity-gemini-3.8-flash"),
    (
        "gemini-3.6-flash-low",
        "google/antigravity-gemini-3.8-flash",
    ),
    (
        "gemini-3.6-flash-medium",
        "google/antigravity-gemini-3.8-flash",
    ),
    (
        "gemini-3.6-flash-high",
        "google/antigravity-gemini-3.8-flash",
    ),
    ("gemini-3.1-pro", "google/antigravity-gemini-4-argon"),
    ("gemini-3.1-pro-low", "google/antigravity-gemini-4-argon"),
    ("gemini-3.1-pro-high", "google/antigravity-gemini-4-argon"),
    ("gemini-pro-agent", "google/antigravity-gemini-4-argon"),
    // The Fusiform model-catalog repository's retirement commit 24ba360 records that both
    // Google's served-model list and the native Antigravity plugin dropped this Claude route.
    // No live Antigravity replacement has been verified.
    ("claude-sonnet-4-6-thinking", "anthropic/claude-sonnet-5-5"),
];

fn retired_antigravity_replacement(model_id: &str) -> Option<&'static str> {
    let stripped = model_id.strip_prefix("antigravity-").unwrap_or(model_id);
    RETIRED_ANTIGRAVITY_ROUTES
        .iter()
        .find_map(|(route, replacement)| (*route == stripped).then_some(*replacement))
}

/// Strip the harness-only route prefix, then resolve any user-facing Antigravity tier alias.
///
/// THE FALLBACK PASSES THE STRIPPED NAME THROUGH UNCHANGED, which is deliberate but sharp on
/// both edges. Non-Gemini Antigravity models use plain backend names without tier aliases, so
/// passthrough remains necessary. Known withdrawals are refused before reaching this conversion.
///
/// A GEMINI model with no entry can still become an opaque provider 404 because bare slugs do not
/// necessarily name backend entities. This table records measured Antigravity routes rather than
/// deriving them from the public catalog.
///
/// A daily 404 is not proof that an alias or model is absent because daily also gates Gemini
/// models on the native client version. Before changing this table, verify the content User-Agent;
/// `google_codeassist.rs` documents a controlled probe where changing only that version toggled
/// the same request between 404 and 200.
fn google_codeassist_wire_model_id(model_id: &str) -> &str {
    let stripped = model_id.strip_prefix("antigravity-").unwrap_or(model_id);
    ANTIGRAVITY_TIER_ALIASES
        .iter()
        .find_map(|(alias, wire_id)| (*alias == stripped).then_some(*wire_id))
        .unwrap_or(stripped)
}

/// The thinking-level names a Code Assist tier suffix can spell. Only used to list the tiers a
/// route has when a variant is refused; whether a name IS a tier is decided by
/// [`google_codeassist_tier_wire_id`].
const CODE_ASSIST_TIER_LEVELS: [&str; 4] = ["minimal", "low", "medium", "high"];

/// The requested id with the harness route prefix and any tier suffix removed: the model whose
/// tiers a reasoning variant chooses between (`antigravity-gemini-3.8-flash-high` ->
/// `gemini-3.8-flash`).
fn google_codeassist_tier_base(model_id: &str) -> &str {
    let stripped = model_id.strip_prefix("antigravity-").unwrap_or(model_id);
    CODE_ASSIST_TIER_LEVELS
        .iter()
        .find_map(|level| {
            stripped
                .strip_suffix(level)
                .and_then(|prefix| prefix.strip_suffix('-'))
        })
        .unwrap_or(stripped)
}

/// The backend model id Code Assist sends when `level` is requested as a reasoning variant, or
/// `None` when the family has no tier of that name for the requested model.
///
/// There is no separate variant table: the id is the one the caller would get by spelling the
/// tier in the model id (`<base>-<level>`, through the same [`ANTIGRAVITY_TIER_ALIASES`]), and
/// it counts as a tier only if the family's own tier table gives it thinking settings
/// ([`tier_thinking_config`]). A request with no variant keeps the default tier the bare id is
/// aliased to.
fn google_codeassist_tier_wire_id(model_id: &str, level: &str) -> Option<String> {
    let candidate = format!("{}-{level}", google_codeassist_tier_base(model_id));
    let wire_id = google_codeassist_wire_model_id(&candidate);
    tier_thinking_config(wire_id)
        .is_some()
        .then(|| wire_id.to_string())
}

/// The tier names the Code Assist family can render for the requested model, in level order.
fn google_codeassist_tiers(model_id: &str) -> Vec<&'static str> {
    CODE_ASSIST_TIER_LEVELS
        .into_iter()
        .filter(|level| google_codeassist_tier_wire_id(model_id, level).is_some())
        .collect()
}

/// Extra resolution inputs an auth method contributes: its identity, any wire-family override,
/// and method-specific metadata such as the Antigravity cloud project. The method identity is
/// required even when the family stays unchanged because Anthropic API key and OAuth use distinct
/// policies within the same `AnthropicMessages` family.
#[derive(Debug, Clone, Default)]
pub struct ResolveOptions<'a> {
    /// The selected credential method. Most providers use it only for credential lookup;
    /// Anthropic also uses it to freeze the API-key or OAuth request policies.
    pub auth_method: Option<AuthMethod>,
    /// The wire family this run's auth method selects, overriding the catalog provider's.
    /// `None` ⇒ use the catalog family (header-only methods don't change the wire grammar).
    pub family_override: Option<WireFamilyId>,
    /// The Antigravity cloud project to freeze as a body render input. `None` for every
    /// other family.
    pub project_id: Option<&'a str>,
    /// The run's session id — used as the ChatGPT codex `session-id` header (a stable
    /// per-session correlation id the backend may thread on). Only consulted for the
    /// `OpenAiResponsesChatgpt` family; ignored otherwise. `None` ⇒ a fixed fallback.
    pub session_id: Option<&'a str>,
    /// The vault-served account id for a ChatGPT credential — the authoritative source
    /// for the `ChatGPT-Account-Id` header (the vault parses it at refresh time, so it
    /// tracks re-logins). `None` falls back to decoding the token's JWT claims locally.
    pub account_id: Option<&'a str>,
    /// Prompt-cache policy resolved by the module before freeze. `None` preserves the
    /// historical frozen-config shape and rendered bytes.
    pub cache_policy: Option<CachePolicy>,
    /// An optional caller-selected cache scope, frozen independently from role and TTL policy.
    pub prompt_cache_key: Option<&'a str>,
    /// The caller's requested reasoning variant (`model.variant`) for a fresh episode. It is
    /// resolved against the model's catalog reasoning options and refused when the model does
    /// not define it. A resume passes the frozen variant through `EpisodeRenderConfig` instead.
    pub variant: Option<&'a str>,
    /// The caller's requested service tier for a fresh episode. Admitted only when the model's
    /// provider family has bytes for it and the catalog prices the matching mode; otherwise
    /// refused, naming the offered tiers. A resume passes the frozen tier through
    /// `EpisodeRenderConfig` instead.
    pub service_tier: Option<ServiceTier>,
    /// Where a fresh token for the run's own credential comes from when the provider rejects
    /// the served one with a 401 mid-run or its token is about to expire. Used only with
    /// [`AuthMode::Secret`]; ChatGPT rebuilds its complete account and session header set.
    /// `None` keeps a 401 ending the step.
    pub secret_refresher: Option<SharedSecretRefresher>,
    /// Account identity frozen at run admission (including when resolving a resume).
    pub frozen_account_ref: Option<&'a str>,
    /// The module clock shared with every live provider for this run.
    pub clock: Option<&'a broca_core::clock::Clock>,
    /// The structured output a fresh episode asks for. Decided against the model's catalog
    /// support, its wire family and the family's subset profile, then frozen into the render
    /// config. A resume passes the frozen decision through `EpisodeRenderConfig` instead.
    pub structured_output: Option<&'a broca_protocol::structured_output::StructuredOutput>,
    /// The Claude Code persona a fresh episode freezes when it resolves to the Anthropic OAuth
    /// (identity-lead) shape: the identity values its requests carry. `None` admits the episode
    /// without a persona, the shape OAuth runs had before it existed. A resume passes the
    /// recorded persona through `EpisodeRenderConfig` instead.
    pub claude_code_persona: Option<&'a ClaudeCodePersona>,
}

/// The Claude Code persona an episode renders with, as the code starting it knows it.
#[derive(Clone, Copy, Default, Debug, PartialEq)]
pub enum EpisodePersona<'a> {
    /// A new episode: it takes `ResolveOptions::claude_code_persona`.
    #[default]
    Fresh,
    /// A resumed episode: exactly what its frozen render config recorded, including `None`
    /// for an episode admitted before the persona existed, which keeps its old bytes.
    Recorded(Option<&'a ClaudeCodePersona>),
}

/// How to obtain the runtime credential for the resolved provider.
pub enum AuthMode {
    /// An explicit bearer token (wins over everything).
    Bearer(String),
    /// A pre-fetched vault secret, shaped into the provider's declared header. For example,
    /// Gemini uses `x-goog-api-key`, not Bearer. The caller owns the async vault fetch;
    /// this synchronous resolver only shapes the supplied secret.
    Secret(String),
    /// No auth header (the mock path — aimock needs none).
    None,
}

/// A resolved, runnable provider plus the secret-free descriptors a harness's run request
/// and greppable/structured output need.
pub struct ProviderResolution {
    pub provider: Box<dyn broca_core::provider::Provider>,
    /// The effective authentication method supplied by the caller.
    pub auth_method: Option<AuthMethod>,
    /// The resolved wire family as a typed value — used to assert (provider, auth-method)
    /// consistency at run start (a wire-changing method's credential MUST match the family
    /// it implies, or a frozen mismatch would render a token through the wrong endpoint).
    pub family_id: WireFamilyId,
    /// The wire family name (e.g. "OpenAiChat") for an observability marker.
    pub family: String,
    /// The base URL the provider POSTs to (resolved or overridden).
    pub base_url: String,
    /// The wire model id actually sent (the model part of a catalog id, or the verbatim
    /// model under a base_url override).
    pub wire_model: String,
    /// A short, SECRET-FREE description of where auth came from.
    pub auth_desc: String,
    /// The provider tag recorded in the run's `ModelHandle` (catalog provider id, or
    /// "openai-compatible" for the base_url override path).
    pub provider_tag: String,
    /// The model's context-window limit (tokens) from the catalog, when known. Threaded to
    /// a prompt-reduction transform as its pressure ground truth. `None` on the base_url
    /// override path (the endpoint is opaque, no catalog model spec) and on the
    /// family-override path (built from family defaults, not a catalog model).
    pub context_limit: Option<u64>,
}

/// Resolution failures retain unsupported variants for admission-time wire errors.
#[derive(Debug, Clone)]
pub enum ResolutionError {
    Config(String),
    VariantUnsupported(broca_provider::variant::VariantRefusal),
}
impl From<String> for ResolutionError {
    fn from(value: String) -> Self {
        Self::Config(value)
    }
}
impl From<&str> for ResolutionError {
    fn from(value: &str) -> Self {
        Self::Config(value.into())
    }
}
impl std::fmt::Display for ResolutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(s) => f.write_str(s),
            Self::VariantUnsupported(e) => e.fmt(f),
        }
    }
}
impl std::error::Error for ResolutionError {}

/// Refuse a catalog model whose stated `model.kind` is not `chat`.
///
/// A decision or extraction model (for example Cloudflare's `@cf/cloudflare/clef`) answers a
/// typed schema, not a conversation, so `session.send` must not render a chat request to it.
/// The refusal is a [`ResolutionError::Config`], the same permanent pre-admission refusal a
/// withdrawn or unknown model gets: it surfaces as `open_failed` with class `permanent`, and
/// nothing durable is written. A model with no stated kind admits exactly as before; absence is
/// not a claim that the model is not chat.
pub fn refuse_non_chat_model(
    catalog: &crate::CatalogSnapshot,
    provider_id: &str,
    model_id: &str,
) -> Result<(), ResolutionError> {
    match catalog.model_kind(provider_id, model_id) {
        Some(kind) if kind != crate::runtime::MODEL_KIND_CHAT => Err(format!(
            "catalog model '{provider_id}/{model_id}' is a {kind} model (model.kind \
             \"{kind}\"), not a chat model; session.send serves only chat models"
        )
        .into()),
        _ => Ok(()),
    }
}
impl From<ResolutionError> for broca_core::RunError {
    fn from(value: ResolutionError) -> Self {
        match value {
            ResolutionError::Config(s) => Self::Config(s),
            ResolutionError::VariantUnsupported(e) => Self::VariantUnsupported(e),
        }
    }
}

/// Resolve a live provider from a model id + an optional base_url override + auth.
///
/// Precedence (so the mock/openai-compatible path is explicit and the catalog is only
/// consulted when needed):
///   1. `base_url` set → OpenAI-Chat at that URL, `model` sent VERBATIM, the catalog is NOT
///      consulted (the mock / any-openai-compatible-endpoint path).
///   2. no `base_url`  → `model` is a catalog id `<provider>/<model>`, resolved to a wire
///      family + base_url + auth kind from the bundled catalog.
///
/// `resolve_provider_with_catalog_base_url` deliberately breaks rule 1 for callers that need
/// a known model to keep its native family while still being sent elsewhere. That entry point
/// is the exception; this one is not, and a caller reaching an openai-compatible endpoint with
/// an id that happens to match a catalog row must keep getting OpenAI-Chat here.
pub fn resolve_provider(
    model: &str,
    base_url: Option<&str>,
    max_tokens: u32,
    auth: &AuthMode,
    opts: &ResolveOptions,
) -> Result<ProviderResolution, ResolutionError> {
    resolve_provider_inner(
        model,
        base_url,
        Some(max_tokens),
        ResolveContext {
            auth,
            opts,
            http_client: None,
            chatgpt_pause_only: false,
            catalog_base_url: None,
            episode_render: EpisodeRenderConfig::default(),
        },
    )
}

/// Resolve a live provider over a caller-owned HTTP client.
///
/// Long-lived harnesses use this entry point so providers created for separate runs retain one
/// connection pool. Resolution, rendering, endpoints, and request-local auth are otherwise
/// identical to [`resolve_provider`]. `max_tokens: None` preserves caller omission so a wire family
/// may select its own default; `Some` is an explicit cap.
pub fn resolve_provider_with_client(
    model: &str,
    base_url: Option<&str>,
    max_tokens: Option<u32>,
    auth: &AuthMode,
    opts: &ResolveOptions,
    http_client: ProviderHttpClient,
) -> Result<ProviderResolution, ResolutionError> {
    resolve_provider_inner(
        model,
        base_url,
        max_tokens,
        ResolveContext {
            auth,
            opts,
            http_client: Some(&http_client),
            chatgpt_pause_only: false,
            catalog_base_url: None,
            episode_render: EpisodeRenderConfig::default(),
        },
    )
}

/// Resolve a catalog-selected family while sending it to a shared base URL.
///
/// The ordinary `base_url` override intentionally selects OpenAI-Chat and leaves the model
/// opaque. The module's aimock path needs different behavior: a known catalog model keeps its
/// native wire family, while every family appends its own endpoint path to the same mock host.
/// Unknown models retain the historical OpenAI-Chat override behavior.
///
/// `restored_max_tokens` is the cap an existing episode froze at start, passed on resume so an
/// omitted cap is not re-derived from a catalog that moved; see
/// [`EpisodeRenderConfig::max_output_tokens`]. `None` for a fresh episode.
pub fn resolve_provider_with_catalog_base_url(
    model: &str,
    base_url: &str,
    max_tokens: Option<u32>,
    restored_max_tokens: Option<u32>,
    auth: &AuthMode,
    opts: &ResolveOptions,
    http_client: ProviderHttpClient,
) -> Result<ProviderResolution, ResolutionError> {
    resolve_catalog_episode_at_base_url(
        model,
        base_url,
        max_tokens,
        auth,
        opts,
        http_client,
        EpisodeRenderConfig {
            max_output_tokens: restored_max_tokens,
            ..Default::default()
        },
    )
}

/// Resolve a shared-host episode while retaining all admission-time render choices on resume.
pub fn resolve_catalog_episode_at_base_url(
    model: &str,
    base_url: &str,
    max_tokens: Option<u32>,
    auth: &AuthMode,
    opts: &ResolveOptions,
    http_client: ProviderHttpClient,
    render: EpisodeRenderConfig<'_>,
) -> Result<ProviderResolution, ResolutionError> {
    let use_catalog = model
        .split_once('/')
        .is_some_and(|(provider_id, model_id)| {
            crate::catalog()
                .ok()
                .is_some_and(|catalog| catalog.contains_model(provider_id, model_id))
        });
    resolve_provider_inner(
        model,
        if use_catalog { None } else { Some(base_url) },
        max_tokens,
        ResolveContext {
            auth,
            opts,
            http_client: Some(&http_client),
            chatgpt_pause_only: false,
            catalog_base_url: use_catalog.then_some(base_url),
            episode_render: render,
        },
    )
}

/// Construct the ChatGPT family while an at-start auth pause is already decided.
///
/// The returned provider intentionally has no auth headers. It may only be handed to the core's
/// pause-at-start path, which durably writes `RunStarted` and `RunPaused` without rendering or
/// calling the provider. A repaired resume must use [`resolve_provider_with_client`] so normal
/// token and account validation builds the real headers before the first call.
///
/// `restored_max_tokens` carries an existing episode's frozen cap on resume, as for
/// [`resolve_provider_with_catalog_base_url`].
pub fn resolve_chatgpt_pause_provider_with_client(
    model: &str,
    max_tokens: Option<u32>,
    restored_max_tokens: Option<u32>,
    opts: &ResolveOptions,
    http_client: ProviderHttpClient,
) -> Result<ProviderResolution, ResolutionError> {
    resolve_chatgpt_pause_episode_with_client(
        model,
        max_tokens,
        opts,
        http_client,
        EpisodeRenderConfig {
            max_output_tokens: restored_max_tokens,
            ..Default::default()
        },
    )
}

/// Freeze the complete episode render choices even when admission pauses before the first call.
pub fn resolve_chatgpt_pause_episode_with_client(
    model: &str,
    max_tokens: Option<u32>,
    opts: &ResolveOptions,
    http_client: ProviderHttpClient,
    render: EpisodeRenderConfig<'_>,
) -> Result<ProviderResolution, ResolutionError> {
    if opts.family_override != Some(WireFamilyId::OpenAiResponsesChatgpt) {
        return Err("pause-only provider construction requires the ChatGPT wire family".into());
    }
    resolve_provider_inner(
        model,
        None,
        max_tokens,
        ResolveContext {
            auth: &AuthMode::None,
            opts,
            http_client: Some(&http_client),
            chatgpt_pause_only: true,
            catalog_base_url: None,
            episode_render: render,
        },
    )
}

/// A live provider over an episode's RECORDED render config, holding a freshly read credential.
///
/// Unlike resolving the episode again, nothing about the config is re-derived from the catalog,
/// so the provider renders exactly the bytes the recorded config pins: what re-sending a recorded
/// request (a prompt-cache warm) needs. The catalog is read only for how the provider takes its
/// credential; a model it no longer lists takes the family's default.
// The credential, its method and both account references are separate inputs on purpose: the
// caller resolved them, and grouping them would only rename the same values.
#[allow(clippy::too_many_arguments)]
pub fn provider_for_recorded_config(
    provider_id: &str,
    model_id: &str,
    frozen: FrozenRenderConfig,
    auth: &AuthMode,
    auth_method: Option<AuthMethod>,
    frozen_account_ref: Option<&str>,
    served_account_ref: Option<&str>,
    http_client: ProviderHttpClient,
    clock: &broca_core::clock::Clock,
) -> Result<Box<dyn broca_core::provider::Provider>, String> {
    let auth_kind = crate::catalog()
        .ok()
        .and_then(|catalog| catalog.model_for_serving(provider_id, model_id))
        .filter(|(provider_spec, _)| provider_spec.wire_family == frozen.family_id)
        .map(|(provider_spec, _)| provider_spec.auth_kind.clone())
        .unwrap_or_else(|| crate::default_auth_kind(frozen.family_id));
    let opts = ResolveOptions {
        auth_method,
        account_id: served_account_ref,
        clock: Some(clock),
        ..Default::default()
    };
    let resolved_auth = resolve_family_auth(auth, &frozen, &auth_kind, provider_id, &opts)?;
    live_provider_with_clock_and_refresher(
        Some(http_client),
        frozen,
        resolved_auth.handle,
        resolved_auth.refresher,
        frozen_account_ref.map(str::to_owned),
        served_account_ref.map(str::to_owned),
        clock.clone(),
    )
    .map_err(|error| error.to_string())
}

/// Render inputs for a catalog episode. Unlike the OpenAI-compatible URL override,
/// these inputs never bypass catalog identity, family, quirks or auth resolution.
#[derive(Clone, Copy, Default)]
pub struct EpisodeRenderConfig<'a> {
    /// Preserve a recorded OpenCode Go protocol when the model's current per-model pin changes.
    /// `None` means a fresh run should use today's catalog selection.
    pub wire_family: Option<WireFamilyId>,
    /// OpenCode Go's session header is derived from the bind identity on every admission and
    /// resume. It is transport metadata, so it does not enter the frozen request-body blob.
    pub opencode_go_session_id: Option<&'a str>,
    /// Restore the recorded body compatibility choice on resume; a fresh Go episode opts in.
    pub opencode_go_reasoning_signature: Option<bool>,
    /// Restore Go's model-specific Qwen and DeepSeek Chat Completions quirks on resume.
    pub opencode_go_qwen_thinking: Option<bool>,
    pub opencode_go_deepseek_thinking: Option<bool>,
    /// On resume, restore whether OpenCode Go's Kimi K2.6 route rejects 24-hour prompt-cache hints.
    pub opencode_go_long_cache_retention_unsupported: Option<bool>,
    /// Freshly derived or restored Code Assist identity. If the episode's frozen config
    /// recorded no identity, keep it absent so a resume reproduces the original bytes.
    pub codeassist_identity: Option<&'a broca_provider::wire::CodeAssistIdentity>,
    pub base_url: Option<&'a str>,
    /// The output cap recorded when this episode started, fed back on resume.
    ///
    /// Both a default cap and an explicit request/catalog minimum are frozen admission
    /// inputs. Restore this value unconditionally, even if the resuming send repeats an
    /// explicit cap: re-clamping against a changed catalog would alter the episode's
    /// render config and make the drift guard refuse an otherwise unchanged resume.
    pub max_output_tokens: Option<u32>,
    /// The variant this episode froze at start, fed back on resume and applied as recorded.
    /// It is never re-derived from the catalog, so a catalog change since admission cannot move
    /// a resumed run's request bytes or refuse its resume.
    pub variant: Option<&'a FrozenVariant>,
    /// The service tier this episode froze at start, fed back on resume and applied as recorded,
    /// without re-checking the catalog's rates: a rate withdrawn since admission must not move a
    /// resumed run's request bytes or refuse its resume.
    pub service_tier: Option<ServiceTier>,
    /// The Anthropic thinking-echo scope this episode froze at start, fed back on resume. The
    /// family default moved from echoing only the last assistant message to echoing the whole
    /// current tool loop; an episode admitted under the old default must keep rendering the
    /// old bytes, and re-deriving the default would also make the frozen-config drift guard
    /// refuse its resume. Ignored unless the resolved config uses Anthropic interleaved
    /// thinking.
    pub thinking_echo_scope: Option<ThinkingEchoScope>,
    /// The Anthropic cross-model thinking rule this episode froze at start, fed back on resume.
    /// The family default moved from echoing signed thinking only to the model that produced it
    /// to echoing it across Anthropic's own models; an episode admitted under the old default
    /// must keep rendering the old bytes, and re-deriving the default would also make the
    /// frozen-config drift guard refuse its resume. Ignored unless the resolved config uses
    /// Anthropic interleaved thinking.
    pub thinking_cross_model: Option<CrossModelThinking>,
    /// The Responses tool strictness choice restored from the episode on resume. `None`
    /// admits a new episode with explicit non-strict tools; `Some(false)` preserves the
    /// omitted field used by episodes recorded before this choice existed.
    pub responses_tools_non_strict: Option<bool>,
    /// Restore the recorded Codex effort-update choice on resume. `None` enables input
    /// configuration updates for a newly admitted episode; `Some(false)` keeps old bytes.
    pub chatgpt_effort_updates: Option<bool>,
    /// Admission's profile, or its exact recorded presence or absence on resume.
    pub chatgpt_wire: Option<&'a broca_provider::chatgpt_wire::ChatgptWire>,
    /// Restore the recorded `reasoning_content_required` choice on resume (OpenAI-Chat only).
    /// `None`, a newly admitted episode, leaves this legacy flag off and uses the echo policy.
    /// A DeepSeek episode admitted before DeepSeek was opted in recorded no field,
    /// which reads back as `Some(false)`: it resumes with the bytes it was sending, and the
    /// frozen-config drift guard does not refuse it for a catalog quirk that moved since.
    pub reasoning_content_required: Option<bool>,
    /// A resume supplies `Some(recorded_policy)`, including `Some(None)` for pre-policy runs.
    /// New admissions use `None` to select today's catalog policy.
    pub reasoning_echo: Option<Option<broca_provider::policy::ReasoningEchoPolicy>>,
    /// Restore the recorded `provider_text_state` choice on resume. `None`, a newly admitted
    /// episode, keeps what the family resolved (on for the Responses and Gemini families). An
    /// episode admitted before the flag existed recorded no field, which reads back as
    /// `Some(false)`: it resumes without echoing message phases or text signatures, which is
    /// what its renderer version sent, instead of being re-resolved into the new shape.
    pub provider_text_state: Option<bool>,
    /// The structured-output decision this episode froze at start, fed back on resume and
    /// applied as recorded. It is never re-decided from the catalog, so a catalog change since
    /// admission cannot move a resumed run's request bytes or its reported enforcement.
    pub structured_output: Option<&'a broca_protocol::structured_output::FrozenStructuredOutput>,
    /// The Claude Code persona: fresh for a new episode, as recorded for a resume. A recorded
    /// absence is kept, so an OAuth episode admitted before the persona existed resumes with
    /// its old bytes rather than being re-admitted into the new shape.
    pub claude_code_persona: EpisodePersona<'a>,
}

/// The output cap used when a send sets none and the catalog cannot supply a usable one.
///
/// 32,000 is OpenCode's `OUTPUT_TOKEN_MAX`, chosen so a run with no stated cap behaves the way
/// the same model does in OpenCode. It is a decision, not a measurement: large enough that a
/// reasoning model's thinking does not exhaust it mid tool call (the previous flat 4,096
/// did exactly that), and small enough to stay well inside every context window in real use.
pub const FALLBACK_MAX_OUTPUT_TOKENS: u32 = 32_000;

/// The largest catalog `limit.output` used as a default without a caller asking for it.
///
/// Above this, a published limit is capped here rather than trusted. Every model broca served
/// in the 30 days before this change publishes 131,072 or less, except deepseek-v4-flash on
/// the DeepSeek API (393,216) and deepseek-v4.1-flash on ollama-cloud (384,000). Neither value
/// has been confirmed against the provider, and fusiform reports that output corrections it
/// holds for ollama-cloud (an unconfirmed 65,536 wall on deepseek-v4-flash) are not served.
/// A default asking a provider for more than it allows fails every send for that model, so an
/// unverified large value is bounded instead. It bounds only the DEFAULT: an explicit caller
/// value is sent as given. Raise it when a model above it is confirmed to accept its limit.
pub const MAX_DEFAULT_OUTPUT_TOKENS: u32 = 131_072;

/// The output cap for a send that set none, from the model's catalog limits.
///
/// The model's own `limit.output` is used when it is known AND strictly below the same model's
/// `limit.context`. A published output limit that is not below the context window is a
/// placeholder, not a cap: over a thousand live rows publish output equal to or greater than
/// context (grok-4.6 at 500,000/500,000, an image model with 32,768 context and 64,000 output),
/// and requesting it would reserve the whole window, or more, for output. Those are treated
/// exactly like an unknown limit. When the context window itself is unknown there is nothing to
/// compare against, so the output limit is used as served.
///
/// Zero is treated as an unknown output limit too. A limit that does not fit the wire's
/// u32 is not a real cap either and falls back. The fallback stays strictly below a known
/// positive context window; an unknown or zero context does not constrain it.
pub fn default_max_output_tokens(output_limit: Option<u64>, context_limit: Option<u64>) -> u32 {
    output_limit
        .filter(|&output| output > 0)
        .filter(|&output| context_limit.is_none_or(|context| output < context))
        .and_then(|output| u32::try_from(output).ok())
        .map(|output| output.min(MAX_DEFAULT_OUTPUT_TOKENS))
        .unwrap_or_else(|| {
            context_limit
                .filter(|&context| context > 0)
                .map_or(FALLBACK_MAX_OUTPUT_TOKENS, |context| {
                    u64::from(FALLBACK_MAX_OUTPUT_TOKENS).min(context - 1) as u32
                })
        })
}

impl EpisodeRenderConfig<'_> {
    fn apply(self, mut config: FrozenRenderConfig) -> FrozenRenderConfig {
        if let Some(session_id) = self.opencode_go_session_id {
            config.opencode_go_session_id = Some(session_id.to_string());
            if config.family_id == WireFamilyId::OpenAiChat {
                config.opencode_go_reasoning_signature =
                    self.opencode_go_reasoning_signature.unwrap_or(true);
            }
        }
        if let Some(recorded) = self.opencode_go_reasoning_signature {
            config.opencode_go_reasoning_signature = recorded;
        }
        if let Some(recorded) = self.opencode_go_qwen_thinking {
            config.opencode_go_qwen_thinking = recorded;
        }
        if let Some(recorded) = self.opencode_go_deepseek_thinking {
            config.opencode_go_deepseek_thinking = recorded;
        }
        if let Some(recorded) = self.opencode_go_long_cache_retention_unsupported {
            config.opencode_go_long_cache_retention_unsupported = recorded;
        }
        if config.family_id == WireFamilyId::GoogleCodeAssist {
            config.codeassist_identity = self.codeassist_identity.cloned();
        }
        if let Some(base_url) = self.base_url {
            config.base_url = Some(base_url.to_string());
        }
        if let (Some(restored), ReasoningPolicy::AnthropicInterleaved { echo_scope, .. }) =
            (self.thinking_echo_scope, &mut config.reasoning)
        {
            *echo_scope = restored;
        }
        if let (Some(restored), ReasoningPolicy::AnthropicInterleaved { cross_model, .. }) =
            (self.thinking_cross_model, &mut config.reasoning)
        {
            *cross_model = restored;
        }
        if matches!(
            config.family_id,
            WireFamilyId::OpenAiResponses | WireFamilyId::OpenAiResponsesChatgpt
        ) {
            config.responses_tools_non_strict = self.responses_tools_non_strict.unwrap_or(true);
        }
        if config.family_id == WireFamilyId::OpenAiResponsesChatgpt {
            config.chatgpt_effort_updates = self.chatgpt_effort_updates.unwrap_or(true);
            config.chatgpt_wire = self.chatgpt_wire.cloned();
        }
        if let (Some(recorded), WireFamilyId::OpenAiChat) =
            (self.reasoning_content_required, config.family_id)
        {
            config.reasoning_content_required = recorded;
        }
        if let (Some(recorded), WireFamilyId::OpenAiChat) = (self.reasoning_echo, config.family_id)
        {
            config.reasoning_echo = recorded;
        }
        if let Some(recorded) = self.provider_text_state {
            config.provider_text_state = recorded;
        }
        config
    }
}

/// Resolve catalog identity and auth normally, changing only the episode's endpoint.
/// On resume, callers supply the recorded host; all other catalog fields are resolved
/// afresh so core can still refuse non-host frozen-config drift.
pub fn resolve_catalog_episode_with_client(
    model: &str,
    max_tokens: Option<u32>,
    auth: &AuthMode,
    opts: &ResolveOptions,
    http_client: ProviderHttpClient,
    render: EpisodeRenderConfig<'_>,
) -> Result<ProviderResolution, ResolutionError> {
    resolve_provider_inner(
        model,
        None,
        max_tokens,
        ResolveContext {
            auth,
            opts,
            http_client: Some(&http_client),
            chatgpt_pause_only: false,
            catalog_base_url: None,
            episode_render: render,
        },
    )
}

fn catalog_override_base_url(base_url: &str, family: WireFamilyId) -> String {
    // Gemini appends `/models/...`; its normal default base already carries this `/v1beta`
    // prefix, so a bare shared mock host needs the same prefix before the family path.
    let base_url = base_url.trim_end_matches('/');
    if family == WireFamilyId::Gemini && !base_url.ends_with("/v1beta") {
        format!("{base_url}/v1beta")
    } else {
        base_url.to_string()
    }
}

struct ResolveContext<'a> {
    auth: &'a AuthMode,
    opts: &'a ResolveOptions<'a>,
    http_client: Option<&'a ProviderHttpClient>,
    chatgpt_pause_only: bool,
    catalog_base_url: Option<&'a str>,
    episode_render: EpisodeRenderConfig<'a>,
}

fn resolve_provider_inner(
    model: &str,
    base_url: Option<&str>,
    caller_max_tokens: Option<u32>,
    context: ResolveContext<'_>,
) -> Result<ProviderResolution, ResolutionError> {
    let ResolveContext {
        auth,
        opts,
        http_client,
        chatgpt_pause_only,
        catalog_base_url,
        episode_render,
    } = context;
    // Resume restores admission's cap, regardless of a repeated explicit request or a
    // changed catalog maximum. Only a new episode computes the request/catalog minimum.
    let restored_max_tokens = episode_render.max_output_tokens;
    let max_tokens_for = |output_limit: Option<u64>, context_limit: Option<u64>| {
        restored_max_tokens.unwrap_or_else(|| {
            caller_max_tokens.map_or_else(
                || default_max_output_tokens(output_limit, context_limit),
                |requested| {
                    output_limit
                        .filter(|limit| *limit > 0)
                        .map_or(requested, |limit| u64::from(requested).min(limit) as u32)
                },
            )
        })
    };

    if let Some(base_url) = base_url.filter(|s| !s.is_empty()) {
        // No catalog model on this path: the endpoint is opaque, so the default is the fallback.
        let max_tokens = max_tokens_for(None, None);
        // Mode 1: explicit base_url override — OpenAI-Chat, model id verbatim.
        let mut frozen = openai_chat_frozen_config(model);
        frozen.base_url = Some(base_url.to_string());
        // Explicit endpoints use the catalog's host-based echo selection too; a resume
        // restores its recorded policy (or absence) in EpisodeRenderConfig::apply.
        frozen.reasoning_content_required =
            episode_render.reasoning_content_required.unwrap_or(false);
        frozen.reasoning_echo = crate::reasoning_echo_for_route("", base_url);
        frozen.max_output_tokens = max_tokens;
        frozen.cache = opts.cache_policy.clone();
        frozen.prompt_cache_key = opts
            .prompt_cache_key
            .map(broca_provider::hash::clamp_prompt_cache_key);
        let (handle, auth_desc, auth_method) = resolve_auth(
            auth,
            &AuthKind::Bearer,
            "openai-compatible",
            opts.auth_method,
        )?;
        // The override endpoint has no catalog row, so it has no reasoning options either.
        apply_variant(
            &mut frozen,
            episode_render.variant,
            opts.variant,
            model,
            model,
            None,
            None,
        )?;
        // The override endpoint is opaque, so broca has no tier bytes for it: any requested
        // tier is refused as unmapped.
        apply_service_tier(
            &mut frozen,
            episode_render.service_tier,
            opts.service_tier,
            "openai-compatible",
            model,
            &[],
        )?;
        // No catalog row either: support is unknown, and the family is OpenAI Chat.
        apply_structured_output(
            &mut frozen,
            episode_render.structured_output,
            opts.structured_output,
            "openai-compatible",
            CapabilityFlag::Absent,
        )?;
        let refresher = secret_refresher_for(
            auth,
            opts,
            &AuthKind::Bearer,
            "openai-compatible",
            opts.auth_method,
        );
        let provider = resolved_live_provider(http_client, frozen, handle, refresher, opts)?;
        return Ok(ProviderResolution {
            provider,
            auth_method,
            family_id: WireFamilyId::OpenAiChat,
            family: format!("{:?}", WireFamilyId::OpenAiChat),
            base_url: base_url.to_string(),
            wire_model: model.to_string(),
            auth_desc,
            provider_tag: "openai-compatible".to_string(),
            // The override endpoint is opaque (no catalog model spec) — the transform, if
            // wired, falls back to its own persisted limit.
            context_limit: None,
        });
    }

    // Mode 2: catalog resolution from `<provider>/<model>`.
    let (provider_id, model_id) = model.split_once('/').ok_or_else(|| {
        format!(
            "model '{model}' is not a catalog id: expected '<provider>/<model>' (e.g. \
             anthropic/claude-opus-4-0), or pass a base_url to use an openai-compatible \
             endpoint directly"
        )
    })?;

    if opts.family_override == Some(WireFamilyId::GoogleCodeAssist) {
        if let Some(replacement) = retired_antigravity_replacement(model_id) {
            let advice = if replacement.starts_with("google/antigravity-") {
                " (medium)"
            } else {
                " through the Anthropic provider"
            };
            return Err(format!(
                "model '{model}' is unservable: Google withdrew this Antigravity route; \
                 use '{replacement}'{advice} instead"
            )
            .into());
        }
    }

    // Taking the snapshot is an Arc clone over memory. It never opens fusiform or reads the LKG.
    // Without a catalog snapshot, provider resolution fails as configuration rather than
    // reporting an unsupported variant for a model whose options we cannot inspect.
    let catalog = crate::catalog().map_err(|error| error.to_string())?;
    let episode_render = EpisodeRenderConfig {
        wire_family: episode_render.wire_family,
        opencode_go_session_id: episode_render.opencode_go_session_id,
        opencode_go_reasoning_signature: episode_render.opencode_go_reasoning_signature,
        opencode_go_qwen_thinking: episode_render.opencode_go_qwen_thinking,
        opencode_go_deepseek_thinking: episode_render.opencode_go_deepseek_thinking,
        opencode_go_long_cache_retention_unsupported: episode_render
            .opencode_go_long_cache_retention_unsupported,
        codeassist_identity: episode_render.codeassist_identity,
        base_url: episode_render
            .base_url
            .filter(|_| catalog.contains_provider(provider_id)),
        max_output_tokens: episode_render.max_output_tokens,
        variant: episode_render.variant,
        service_tier: episode_render.service_tier,
        thinking_echo_scope: episode_render.thinking_echo_scope,
        thinking_cross_model: episode_render.thinking_cross_model,
        responses_tools_non_strict: episode_render.responses_tools_non_strict,
        chatgpt_effort_updates: episode_render.chatgpt_effort_updates,
        chatgpt_wire: episode_render.chatgpt_wire,
        reasoning_content_required: episode_render.reasoning_content_required,
        reasoning_echo: episode_render.reasoning_echo,
        provider_text_state: episode_render.provider_text_state,
        structured_output: episode_render.structured_output,
        claude_code_persona: episode_render.claude_code_persona,
    };
    if catalog.is_withdrawn(provider_id, model_id) {
        return Err(format!(
            "catalog model '{provider_id}/{model_id}' was withdrawn upstream; choose a different model for this episode"
        ).into());
    }
    refuse_non_chat_model(&catalog, provider_id, model_id)?;

    // Mode 2b: an AUTH METHOD selected a wire family that OVERRIDES the catalog provider's
    // (Google Antigravity → google-codeassist, OpenAI ChatGPT → openai-responses-chatgpt).
    // The override family brings its OWN endpoint, auth kind, and quirks — the catalog's
    // family/base_url/auth for this provider don't apply — so build from the override
    // family's defaults, keeping only the wire model id. `family_for_method` (in the
    // executor) is the single authority that produces `family_override`; this just renders
    // the family it names, so a mismatched (family, credential) pair is impossible by
    // construction (the run-start consistency guard remains as defense-in-depth).
    if let Some(family) = opts.family_override {
        // Code Assist tier aliases are not backend model ids. Resolve them only at this
        // boundary: run metadata keeps the canonical id, while the frozen render config carries
        // the stable wire model. Every other override family sends the id verbatim.
        let wire_model_id = match family {
            WireFamilyId::GoogleCodeAssist => google_codeassist_wire_model_id(model_id),
            _ => model_id,
        };
        // A ChatGPT id resolves only if the catalog lists it exactly, the same rule as every
        // catalog path. There are no tier suffixes: `-fast` and `-pro` ids are taken literally.
        // Without this, an id no catalog knows (a retired `-fast` or `-pro` moniker, a typo)
        // went straight to the backend and failed there as an opaque provider error.
        if family == WireFamilyId::OpenAiResponsesChatgpt
            && !catalog.contains_model(provider_id, model_id)
        {
            return Err(format!(
                "catalog has no served model '{provider_id}/{model_id}' for ChatGPT auth (a \
                 ChatGPT id must name a catalog model exactly; for priority processing send \
                 service_tier: \"priority\" rather than a '-fast' suffix, and for maximum \
                 reasoning send model.variant rather than a '-pro' suffix)"
            )
            .into());
        }
        let mut frozen = frozen_config_for_family(family, wire_model_id);
        // The override family owns the endpoint, but output capacity belongs to the model, so
        // the catalog answers it where it lists this provider/model. A tier alias the catalog
        // does not list gets the fallback rather than a guess about its base model's limit.
        frozen.max_output_tokens = max_tokens_for(
            catalog.output_limit(provider_id, model_id),
            catalog.context_limit(provider_id, model_id),
        );
        if let Some(base_url) = catalog_base_url {
            frozen.base_url = Some(catalog_override_base_url(base_url, family));
        }
        if family == WireFamilyId::GoogleCodeAssist {
            frozen.caller_max_output_tokens = caller_max_tokens;
        }
        frozen.cache = opts.cache_policy.clone();
        // The cloud project (Antigravity) is a BODY render input frozen at run start; it
        // reproduces on resume via the persisted frozen-config blob, like every render field.
        frozen.project_id = opts.project_id.map(str::to_string);
        // A caller-selected hint scope replaces codex's session-derived fallback. The resolved
        // value is frozen into the body and reused for the off-body affinity header, so both
        // names stay identical across every step and a resume.
        let codex_cache_key = if family == WireFamilyId::OpenAiResponsesChatgpt {
            opts.prompt_cache_key
                .map(broca_provider::hash::clamp_prompt_cache_key)
                .or_else(|| {
                    opts.session_id
                        .map(broca_provider::hash::clamp_prompt_cache_key)
                })
        } else {
            opts.prompt_cache_key
                .map(broca_provider::hash::clamp_prompt_cache_key)
        };
        frozen.prompt_cache_key = codex_cache_key;
        let FamilyAuth {
            handle,
            auth_desc,
            auth_method,
            refresher,
        } = if chatgpt_pause_only && family == WireFamilyId::OpenAiResponsesChatgpt {
            FamilyAuth {
                handle: AuthHandle::None,
                auth_desc: format!("chatgpt-pause-only({provider_id})"),
                auth_method: opts.auth_method,
                refresher: None,
            }
        } else {
            let auth_kind = crate::default_auth_kind(family);
            resolve_family_auth(auth, &frozen, &auth_kind, provider_id, opts)?
        };
        // The variant is resolved against the requested id's catalog row. On Code Assist that
        // is the id before tier-alias resolution, since the catalog does not list the aliases.
        let options_spec = catalog
            .model_for_serving(provider_id, model_id)
            .map(|(_, spec)| spec);
        apply_variant(
            &mut frozen,
            episode_render.variant,
            opts.variant,
            &format!("{provider_id}/{model_id}"),
            model_id,
            options_spec.as_ref().map(|spec| &spec.reasoning_options),
            catalog.output_limit(provider_id, model_id),
        )?;
        // Priced against the same catalog row the variant resolves against.
        apply_service_tier(
            &mut frozen,
            episode_render.service_tier,
            opts.service_tier,
            provider_id,
            &format!("{provider_id}/{model_id}"),
            &catalog.priced_modes(provider_id, model_id),
        )?;
        // Support is read from the same catalog row the variant resolves against.
        apply_structured_output(
            &mut frozen,
            episode_render.structured_output,
            opts.structured_output,
            provider_id,
            options_spec
                .as_ref()
                .map_or(CapabilityFlag::Absent, |spec| {
                    spec.capabilities.structured_output
                }),
        )?;
        let frozen = episode_render.apply(frozen);
        let resolved_base_url = frozen
            .base_url
            .clone()
            .unwrap_or_else(|| "<family default>".to_string());
        // Read back from the frozen config: on Code Assist a variant may have moved the wire id
        // to another tier, and this reports the id actually sent.
        let wire_model = frozen.wire_model_id.clone();
        let provider = resolved_live_provider(http_client, frozen, handle, refresher, opts)?;
        return Ok(ProviderResolution {
            provider,
            auth_method,
            family_id: family,
            family: format!("{family:?}"),
            base_url: resolved_base_url,
            wire_model,
            auth_desc,
            provider_tag: provider_id.to_string(),
            // The override family supplies the endpoint and quirks, but the context window is a
            // property of the MODEL, so the catalog still answers it when it lists this model.
            // Worth the lookup: a consumer that reads this as a capacity cannot tell an unknown
            // limit from a full one, and reporting nothing for a model the catalog describes
            // would understate a large window as an absent one.
            //
            // A tier-aliased id the catalog does not list stays None. Resolving it to the base
            // model's window would be a guess about whether the tier shares that window, and an
            // absent limit is the honest answer where a wrong one is unrecoverable.
            context_limit: catalog.context_limit(provider_id, model_id),
        });
    }
    let recorded_opencode_go_family = if provider_id == "opencode-go" {
        episode_render.wire_family
    } else {
        None
    };
    let serving_pair = match recorded_opencode_go_family {
        Some(family) => catalog.model_for_serving_with_family(provider_id, model_id, family),
        None => catalog.model_for_serving(provider_id, model_id),
    };
    let (mut provider_spec, model_spec) = serving_pair
        .ok_or_else(|| {
            if catalog.is_withdrawn(provider_id, model_id) {
                format!(
                    "catalog model '{provider_id}/{model_id}' was withdrawn upstream; choose a different model for this episode"
                )
            } else if catalog.is_overlay_removed(provider_id, model_id) {
                format!(
                    "catalog model '{provider_id}/{model_id}' is retired by broca's own overlay \
                     (crates/broca-catalog/data/overlay.json `remove`), not missing upstream"
                )
            } else {
                format!(
                    "catalog has no served model '{provider_id}/{model_id}' (unknown provider/model, \
                     or the provider is pending — not yet catalog-served; pass a base_url to reach it \
                     as an openai-compatible endpoint)"
                )
            }
        })?;

    if provider_id == "opencode-go" {
        if let Some(base_url) = episode_render.base_url {
            provider_spec.base_url = base_url.to_string();
        }
        if episode_render.opencode_go_session_id.is_none() {
            return Err("OpenCode Go needs a session identity when resolving a new run".into());
        }
    }

    let family = provider_spec.wire_family;
    let max_tokens = max_tokens_for(model_spec.limits.max_output, model_spec.limits.context);
    let mut frozen: FrozenRenderConfig = resolve_frozen(
        &provider_spec,
        &model_spec,
        max_tokens,
        renderer_version_for(family),
    )
    .map_err(|error| error.to_string())?;
    if let Some(base_url) = catalog_base_url {
        frozen.base_url = Some(catalog_override_base_url(base_url, family));
    }
    let auth_kind = provider_route_auth_kind(provider_id, family, &provider_spec.auth_kind);
    let (handle, auth_desc, auth_method) =
        resolve_auth(auth, &auth_kind, provider_id, opts.auth_method)?;
    apply_auth_method_quirks(provider_id, auth_method, &mut frozen);
    frozen.max_output_tokens = max_tokens;
    if family == WireFamilyId::GoogleCodeAssist {
        frozen.caller_max_output_tokens = caller_max_tokens;
    }
    frozen.cache = opts.cache_policy.clone();
    frozen.prompt_cache_key = opts
        .prompt_cache_key
        .map(broca_provider::hash::clamp_prompt_cache_key);
    apply_variant(
        &mut frozen,
        episode_render.variant,
        opts.variant,
        &format!("{provider_id}/{model_id}"),
        model_id,
        Some(&model_spec.reasoning_options),
        model_spec.limits.max_output,
    )?;
    apply_service_tier(
        &mut frozen,
        episode_render.service_tier,
        opts.service_tier,
        provider_id,
        &format!("{provider_id}/{model_id}"),
        &catalog.priced_modes(provider_id, model_id),
    )?;
    apply_structured_output(
        &mut frozen,
        episode_render.structured_output,
        opts.structured_output,
        provider_id,
        model_spec.capabilities.structured_output,
    )?;
    apply_claude_code_persona(
        &mut frozen,
        episode_render.claude_code_persona,
        opts.claude_code_persona,
    );
    if provider_id == "opencode-go" && family == WireFamilyId::OpenAiChat {
        // Pi serves these Qwen ids through Chat Completions despite their Anthropic models.dev
        // metadata and uses the Qwen thinking switch; the frozen flag preserves resumed bytes.
        if episode_render.opencode_go_qwen_thinking.is_none()
            && matches!(model_id, "qwen3.5-plus" | "qwen3.6-plus")
        {
            frozen.opencode_go_qwen_thinking = true;
        }
        if episode_render.opencode_go_deepseek_thinking.is_none()
            && (model_id.starts_with("deepseek-v4") || model_id == "kimi-k2.6")
        {
            // Pi's OpenCode Go compat sends the DeepSeek-style `thinking` field on these models.
            frozen.opencode_go_deepseek_thinking = true;
        }
        if episode_render.reasoning_echo.is_none() && model_id.starts_with("deepseek-v4") {
            // DeepSeek V4 also requires each prior assistant turn's reasoning_content.
            frozen.reasoning_echo = Some(broca_provider::policy::ReasoningEchoPolicy::DEEPSEEK);
        }
        if episode_render
            .opencode_go_long_cache_retention_unsupported
            .is_none()
            && model_id == "kimi-k2.6"
        {
            frozen.opencode_go_long_cache_retention_unsupported = true;
        }
    }
    let frozen = episode_render.apply(frozen);
    let resolved_base_url = frozen
        .base_url
        .clone()
        .unwrap_or_else(|| "<family default>".to_string());

    let refresher = secret_refresher_for(auth, opts, &auth_kind, provider_id, auth_method);
    let provider = resolved_live_provider(http_client, frozen, handle, refresher, opts)?;
    Ok(ProviderResolution {
        provider,
        auth_method,
        family_id: family,
        family: format!("{family:?}"),
        base_url: resolved_base_url,
        wire_model: model_id.to_string(),
        auth_desc,
        provider_tag: provider_id.to_string(),
        // The catalog model's context window (the transform's pressure ground truth).
        context_limit: model_spec.limits.context,
    })
}

/// Freeze the episode's Claude Code persona into its render config. Only the Anthropic OAuth
/// shape (a required identity lead) speaks as Claude Code; every other config is left without
/// one. A resume restores what it recorded, a fresh episode takes the persona offered for it.
fn apply_claude_code_persona(
    frozen: &mut FrozenRenderConfig,
    episode: EpisodePersona<'_>,
    fresh: Option<&ClaudeCodePersona>,
) {
    let identity_lead = frozen.family_id == WireFamilyId::AnthropicMessages
        && matches!(
            frozen.system_prompt,
            SystemPromptPolicy::RequiredIdentityLead { .. }
        );
    frozen.claude_code_persona = match episode {
        _ if !identity_lead => None,
        EpisodePersona::Fresh => fresh.cloned(),
        EpisodePersona::Recorded(recorded) => recorded.cloned(),
    };
}

/// Freeze the episode's structured-output decision into its render config. A resume applies
/// the decision it recorded; a fresh episode decides from the model's catalog support
/// (`capability.structured_output`), its wire family and the family's subset profile. Runs
/// without structured output are left untouched, so their frozen config is byte-identical.
fn apply_structured_output(
    frozen: &mut FrozenRenderConfig,
    restored: Option<&broca_protocol::structured_output::FrozenStructuredOutput>,
    requested: Option<&broca_protocol::structured_output::StructuredOutput>,
    provider_id: &str,
    catalog: CapabilityFlag,
) -> Result<(), ResolutionError> {
    frozen.structured_output = match (restored, requested) {
        (Some(restored), _) => Some(restored.clone()),
        (None, Some(requested)) => Some(
            broca_provider::structured_output::decide(
                frozen.family_id,
                provider_id,
                frozen.base_url.as_deref(),
                catalog,
                requested,
            )
            .map_err(|field| format!("structured output refused at {field}"))?,
        ),
        (None, None) => None,
    };
    if restored.is_none() {
        if let Some(so) = frozen.structured_output.as_mut() {
            broca_provider::structured_output::apply_grammar_memo(&frozen.wire_model_id, so);
        }
    }
    Ok(())
}

/// Freeze the episode's reasoning variant into its render config.
///
/// A resumed episode re-applies exactly the variant it froze, without consulting the catalog.
/// A fresh one resolves the caller's requested name against this model's reasoning options and
/// is refused, with the model's variant list, when the model does not define it.
///
/// `options` is `None` when the catalog has no row for the model. `model_id` is the requested
/// model id without its provider; Google Code Assist derives the backend tier from it.
///
/// Google Code Assist renders a variant differently from every other family: which levels the
/// model accepts still comes from its catalog row, but a level is sent by switching the backend
/// model id to that level's tier (`high` on `antigravity-gemini-3.8-flash` sends
/// `gemini-3.8-flash-high` with that tier's thinking settings). See [`apply_code_assist_variant`].
fn apply_variant(
    frozen: &mut FrozenRenderConfig,
    restored: Option<&FrozenVariant>,
    requested: Option<&str>,
    catalog_model: &str,
    model_id: &str,
    options: Option<&ReasoningOptions>,
    output_limit: Option<u64>,
) -> Result<(), ResolutionError> {
    let code_assist = frozen.family_id == WireFamilyId::GoogleCodeAssist;
    if let Some(restored) = restored {
        // The tier is recomputed from the frozen name and this build's constant tier table, never
        // from the catalog, so a row withdrawn since admission cannot move the resumed request.
        // A frozen Code Assist variant with no tier of its name (frozen by an older build on an
        // id without tiers) keeps rendering exactly as it was frozen.
        if code_assist {
            if let Some(tier) = google_codeassist_tier_wire_id(model_id, &restored.name) {
                frozen.wire_model_id = tier;
            }
        }
        return freeze_variant(frozen, restored.clone()).map_err(ResolutionError::Config);
    }
    let Some(requested) = requested else {
        return Ok(());
    };
    if code_assist {
        return apply_code_assist_variant(
            frozen,
            requested,
            catalog_model,
            model_id,
            options,
            output_limit,
        );
    }
    let variant = resolve_variant(
        frozen.family_id,
        catalog_model,
        &frozen.wire_model_id,
        options.unwrap_or(&ReasoningOptions::Unstated),
        output_limit,
        requested,
    )
    .map_err(ResolutionError::VariantUnsupported)?;
    freeze_variant(frozen, variant).map_err(ResolutionError::Config)
}

/// Freeze the episode's service tier into its render config.
///
/// A resumed episode re-applies exactly the tier it froze, from broca's family table alone, never
/// consulting the catalog. A fresh one is admitted only when the provider family has bytes for
/// the tier and the catalog prices the mode it corresponds to (`priced_modes`); otherwise it is
/// refused, naming the reason and the tiers that are offered. It is never downgraded silently.
fn apply_service_tier(
    frozen: &mut FrozenRenderConfig,
    restored: Option<ServiceTier>,
    requested: Option<ServiceTier>,
    provider_id: &str,
    catalog_model: &str,
    priced_modes: &[String],
) -> Result<(), String> {
    if let Some(restored) = restored {
        return broca_provider::service_tier::freeze_service_tier(frozen, provider_id, restored);
    }
    let Some(requested) = requested else {
        return Ok(());
    };
    let family = frozen.family_id;
    if let Err(reason) =
        broca_provider::service_tier::tier_offered(provider_id, family, requested, priced_modes)
    {
        return Err(broca_provider::service_tier::refusal_message(
            catalog_model,
            provider_id,
            family,
            requested,
            &reason,
            priced_modes,
        ));
    }
    broca_provider::service_tier::freeze_service_tier(frozen, provider_id, requested)
}

/// Admit a fresh variant on Google Code Assist, where a thinking level is rendered as a backend
/// tier id rather than as request parameters.
///
/// - No catalog row for the id: refused because its variants cannot be known.
/// - A name the row does not define: refused, as on every family.
/// - A defined name with a tier of that name for the model: applied by switching the wire model
///   id to that tier, whose thinking settings the renderer then sends.
/// - A defined name with no such tier: refused, naming the tiers the route does have.
/// - A model the family has no tiers for at all (`antigravity-claude-sonnet-4-6-thinking`,
///   served through an alias row): refused because this route cannot send any level for it.
fn apply_code_assist_variant(
    frozen: &mut FrozenRenderConfig,
    requested: &str,
    catalog_model: &str,
    model_id: &str,
    options: Option<&ReasoningOptions>,
    output_limit: Option<u64>,
) -> Result<(), ResolutionError> {
    let Some(options) = options else {
        return Err(ResolutionError::VariantUnsupported(
            broca_provider::variant::VariantRefusal {
                model: catalog_model.into(),
                variant: requested.into(),
                defined: Vec::new(),
                reason: format!(
                    "model '{catalog_model}' cannot use variant '{requested}': the catalog has no \
                     row for this model, so its variants cannot be known"
                ),
            },
        ));
    };
    let variant = resolve_variant(
        frozen.family_id,
        catalog_model,
        &frozen.wire_model_id,
        options,
        output_limit,
        requested,
    )
    .map_err(ResolutionError::VariantUnsupported)?;
    let tiers = google_codeassist_tiers(model_id);
    if tiers.is_empty() {
        return Err(ResolutionError::VariantUnsupported(
            broca_provider::variant::VariantRefusal {
                model: catalog_model.into(),
                variant: requested.into(),
                defined: broca_provider::variant::derive_variants(
                    frozen.family_id,
                    &frozen.wire_model_id,
                    options,
                    output_limit,
                )
                .names()
                .into_iter()
                .map(str::to_owned)
                .collect(),
                reason: format!(
                    "model '{catalog_model}' cannot use variant '{requested}': the Google Code \
                     Assist route sends a reasoning level as a backend tier id and has no tiers \
                     for '{}'",
                    frozen.wire_model_id
                ),
            },
        ));
    }
    let Some(tier) = google_codeassist_tier_wire_id(model_id, &variant.name) else {
        return Err(ResolutionError::VariantUnsupported(
            broca_provider::variant::VariantRefusal {
                model: catalog_model.into(),
                variant: requested.into(),
                defined: broca_provider::variant::derive_variants(
                    frozen.family_id,
                    &frozen.wire_model_id,
                    options,
                    output_limit,
                )
                .names()
                .into_iter()
                .map(str::to_owned)
                .collect(),
                reason: format!(
            "model '{catalog_model}' defines variant '{requested}', but the Google Code Assist \
             route cannot render it: this route sends a reasoning level as a backend tier, and \
             its tiers for '{}' are [{}]. This is a limit of the route, not of the catalog; \
             request one of those tiers or send no variant",
            google_codeassist_tier_base(model_id),
            tiers.join(", ")
        ),
            },
        ));
    };
    frozen.wire_model_id = tier;
    freeze_variant(frozen, variant).map_err(ResolutionError::Config)
}

struct FamilyAuth {
    handle: AuthHandle,
    auth_desc: String,
    auth_method: Option<AuthMethod>,
    refresher: Option<Arc<dyn CredentialRefresher>>,
}

/// Steps and recorded warms must present the same off-body identity. The Codex backend keys
/// prompt-cache affinity on `session-id`, which carries the frozen prompt-cache scope: a warm
/// with identical body bytes but no `session-id` is still answered, yet reads only a shared
/// head of the prefix or nothing. Account and client identity headers are sent too, so a warm
/// is indistinguishable from the step it re-sends.
fn resolve_family_auth(
    auth: &AuthMode,
    frozen: &FrozenRenderConfig,
    auth_kind: &AuthKind,
    provider_id: &str,
    opts: &ResolveOptions<'_>,
) -> Result<FamilyAuth, String> {
    if frozen.family_id == WireFamilyId::OpenAiResponsesChatgpt {
        let cache_scope_key = frozen.prompt_cache_key.as_deref();
        let (handle, auth_desc) =
            chatgpt_codex_auth(auth, provider_id, cache_scope_key, opts.account_id)?;
        let refresher = chatgpt_secret_refresher_for(auth, opts, cache_scope_key);
        Ok(FamilyAuth {
            handle,
            auth_desc,
            auth_method: opts.auth_method,
            refresher,
        })
    } else {
        let auth_kind = provider_route_auth_kind(provider_id, frozen.family_id, auth_kind);
        let (handle, auth_desc, auth_method) =
            resolve_auth(auth, &auth_kind, provider_id, opts.auth_method)?;
        let refresher = secret_refresher_for(auth, opts, &auth_kind, provider_id, auth_method);
        Ok(FamilyAuth {
            handle,
            auth_desc,
            auth_method,
            refresher,
        })
    }
}

fn provider_route_auth_kind(
    provider_id: &str,
    family: WireFamilyId,
    default: &AuthKind,
) -> AuthKind {
    if provider_id == "opencode-go" && family == WireFamilyId::AnthropicMessages {
        AuthKind::ApiKeyHeader {
            header: "x-api-key".to_string(),
        }
    } else {
        default.clone()
    }
}

fn resolved_live_provider(
    http_client: Option<&ProviderHttpClient>,
    frozen: FrozenRenderConfig,
    auth: AuthHandle,
    refresher: Option<Arc<dyn CredentialRefresher>>,
    opts: &ResolveOptions<'_>,
) -> Result<Box<dyn broca_core::provider::Provider>, String> {
    live_provider_with_clock_and_refresher(
        http_client.cloned(),
        frozen,
        auth,
        refresher,
        opts.frozen_account_ref.map(str::to_owned),
        opts.account_id.map(str::to_owned),
        opts.clock.cloned().unwrap_or_default(),
    )
    .map_err(|error| error.to_string())
}

/// The in-step 401 recovery for a vault secret: each refreshed secret is shaped by the same
/// rule `resolve_auth` applied to the admission-time one, so it lands in the same header.
/// `None` for every other auth source (an explicit bearer cannot be
/// force-refreshed) and when the caller supplied no refresher.
fn secret_refresher_for(
    auth: &AuthMode,
    opts: &ResolveOptions,
    auth_kind: &AuthKind,
    auth_provider: &str,
    method: Option<AuthMethod>,
) -> Option<Arc<dyn CredentialRefresher>> {
    if !matches!(auth, AuthMode::Secret(_)) {
        return None;
    }
    let source = opts.secret_refresher.as_ref()?.0.clone();
    let kind = auth_kind_for_method(auth_provider, method, auth_kind);
    Some(Arc::new(ShapedSecretRefresher::new(
        source,
        move |secret| shape_secret(secret, &kind),
    )))
}

/// Shape a raw secret string into the provider's declared auth header. A provider with an
/// `ApiKeyHeader` auth kind (e.g. Gemini's `x-goog-api-key`) receives the secret in THAT
/// header; everything else reaches it as a `Bearer` token. (SigV4 isn't a
/// credential-from-a-string; Bedrock via the catalog isn't wired through this path.) This
/// is the single shaping rule shared by every credential SOURCE — a vault token and an
/// OpenCode token both land in the right header by construction, so e.g. a Gemini token is
/// never mis-sent as a Bearer.
fn shape_secret(secret: String, auth_kind: &AuthKind) -> AuthHandle {
    match auth_kind {
        AuthKind::ApiKeyHeader { header } => AuthHandle::ApiKeyHeader {
            header: header.clone(),
            value: secret,
        },
        _ => AuthHandle::Bearer(secret),
    }
}

/// Shape a caller-supplied credential and retain its resolved authentication method.
fn resolve_auth(
    auth: &AuthMode,
    auth_kind: &AuthKind,
    auth_provider: &str,
    method: Option<AuthMethod>,
) -> Result<(AuthHandle, String, Option<AuthMethod>), String> {
    match auth {
        AuthMode::Bearer(t) if !t.is_empty() => Ok((
            AuthHandle::Bearer(t.clone()),
            "bearer(explicit)".to_string(),
            method,
        )),
        AuthMode::Bearer(_) | AuthMode::None => Ok((AuthHandle::None, "none".to_string(), method)),
        AuthMode::Secret(secret) if !secret.is_empty() => Ok((
            shape_secret(
                secret.clone(),
                &auth_kind_for_method(auth_provider, method, auth_kind),
            ),
            format!("vault({auth_provider})"),
            method,
        )),
        AuthMode::Secret(_) => Err(format!("empty vault credential for '{auth_provider}'")),
    }
}

fn auth_kind_for_method(
    provider: &str,
    method: Option<AuthMethod>,
    default: &AuthKind,
) -> AuthKind {
    if provider == "anthropic" && method == Some(AuthMethod::ApiKey) {
        AuthKind::ApiKeyHeader {
            header: "x-api-key".to_string(),
        }
    } else {
        default.clone()
    }
}

fn apply_auth_method_quirks(
    provider: &str,
    method: Option<AuthMethod>,
    frozen: &mut FrozenRenderConfig,
) {
    if (provider == "anthropic" && method == Some(AuthMethod::ApiKey))
        || (provider == "opencode-go" && frozen.family_id == WireFamilyId::AnthropicMessages)
    {
        apply_anthropic_apikey_quirks(frozen);
    }
}

/// Build the ChatGPT codex backend's complete auth header set: `Authorization: Bearer
/// <token>`, the `ChatGPT-Account-Id` decoded from the token's JWT, a per-session
/// `session-id`, an `originator`, and a `User-Agent`. The account id is the load-bearing
/// derivation — FAIL LOUD (a clear, non-secret message) if the token carries no
/// `chatgpt_account_id` claim rather than send a request with no/empty account header (which
/// the codex backend rejects opaquely). Adapted from OpenCode's codex.ts header construction
/// (MIT, Copyright (c) 2025 opencode; see THIRD-PARTY-NOTICES.md).
/// broca uses vault credentials and the frozen cache scope for the session header.
/// All of these are HTTP headers (off the rendered/hashed bytes).
fn chatgpt_codex_auth(
    auth: &AuthMode,
    auth_provider: &str,
    cache_scope_key: Option<&str>,
    vault_account_id: Option<&str>,
) -> Result<(AuthHandle, String), String> {
    // A ChatGPT subscription token is an OAuth access token, never an API-key header.
    let token = match auth {
        AuthMode::Bearer(t) | AuthMode::Secret(t) if !t.is_empty() => t.clone(),
        _ => return Err(format!("no ChatGPT token for '{auth_provider}'")),
    };

    // The vault-served account id wins: the vault parses it from the token at refresh
    // time, so it tracks a re-login that swaps accounts. The local JWT decode remains the
    // fallback for an explicit Bearer token.
    let account_id =
        cortexkit_chatgpt_policy::account_id(vault_account_id, &token).map_err(|_| {
            format!(
                "the '{auth_provider}' ChatGPT (codex) token carries no chatgpt_account_id \
                 claim — is this a ChatGPT-subscription token (not a plain OpenAI platform \
                 key)?"
            )
        })?;

    Ok((
        chatgpt_codex_headers(token, account_id, cache_scope_key),
        format!("chatgpt({auth_provider})"),
    ))
}

fn chatgpt_secret_refresher_for(
    auth: &AuthMode,
    opts: &ResolveOptions,
    cache_scope_key: Option<&str>,
) -> Option<Arc<dyn CredentialRefresher>> {
    if !matches!(auth, AuthMode::Secret(_)) {
        return None;
    }
    let source = opts.secret_refresher.as_ref()?.0.clone();
    let cache_scope_key = cache_scope_key.map(str::to_owned);
    Some(Arc::new(ShapedSecretRefresher::new(
        source,
        move |secret| {
            // Decode each replacement rather than reuse admission's account header. The transport
            // checks both this header and the vault's account metadata before adopting the token.
            let account_id = chatgpt_account_id(&secret).unwrap_or_default();
            chatgpt_codex_headers(secret, account_id, cache_scope_key.as_deref())
        },
    )))
}

fn chatgpt_codex_headers(
    token: String,
    account_id: String,
    cache_scope_key: Option<&str>,
) -> AuthHandle {
    let headers = vec![
        ("authorization".to_string(), format!("Bearer {token}")),
        ("chatgpt-account-id".to_string(), account_id),
        (
            // The backend derives prompt-cache affinity from this header and applies the same
            // 64-character limit as the body field. Clamp with the shared digest so a caller's
            // long scope names the same cache identity in both places.
            "session-id".to_string(),
            cache_scope_key.map_or_else(
                || "broca".to_string(),
                broca_provider::hash::clamp_prompt_cache_key,
            ),
        ),
        // The codex backend gates newer-model visibility on the CLIENT IDENTITY pair:
        // luna-class models are served only when both `originator` and `User-Agent`
        // identify as the codex CLI (live A/B matrix: broca/broca and opencode/opencode
        // both 404, codex_cli_rs/codex_cli_rs 200 — version irrelevant, 0.100 works).
        // Presenting the codex identity here is the backend's de-facto client contract
        // for this OAuth path, the same class as the Claude-Code identity lead the
        // anthropic OAuth path requires in the request body.
        (
            "originator".to_string(),
            cortexkit_chatgpt_policy::CODEX_CLI_ORIGINATOR.to_string(),
        ),
        (
            "user-agent".to_string(),
            format!(
                "codex_cli_rs/0.144.0 ({}; {})",
                std::env::consts::OS,
                std::env::consts::ARCH
            ),
        ),
    ];
    AuthHandle::Headers(headers)
}

/// Extract the ChatGPT account id from a token's JWT payload alone, with no vault id. The
/// shared ChatGPT policy reads the `chatgpt_account_id` claim, then the one nested under
/// `https://api.openai.com/auth`, then the first organization id. Returns `None` if the token
/// isn't a JWT or carries no such claim. The signature is not verified: broca reads its own
/// token here, it does not validate it.
fn chatgpt_account_id(token: &str) -> Option<String> {
    cortexkit_chatgpt_policy::account_id(None, token).ok()
}

#[cfg(all(test, feature = "live"))]
mod tests {
    use super::*;
    use broca_protocol::content::{ContentBlock, Message, Role};
    use broca_protocol::request::CallOptions;
    use broca_provider::families::{anthropic_frozen_config, AnthropicMessages};
    use broca_provider::policy::{ReasoningPolicy, SystemPromptPolicy};
    use broca_provider::wire::WireFamily;

    fn anthropic_options() -> CallOptions {
        CallOptions::new(vec![Message::new(
            Role::System,
            vec![ContentBlock::Text {
                text: "caller system".to_string(),
            }],
        )])
    }

    fn header<'a>(request: &'a broca_provider::wire::HttpRequest, name: &str) -> Option<&'a str> {
        request
            .headers
            .iter()
            .find(|(candidate, _)| candidate == name)
            .map(|(_, value)| value.as_str())
    }

    /// The subject of the overlay-retirement refusal test: present in the vendored snapshot,
    /// resolved by no other test here, suppressed only inside `seed_catalog_for_resolution`.
    const TEST_RETIRED_PROVIDER: &str = "deepseek";
    const TEST_RETIRED_MODEL: &str = "deepseek-v4-flash";

    /// Seed the process-global catalog cache from the bundled snapshot.
    ///
    /// Resolution reads the cache and no longer falls back to a compile-time catalog, so a unit
    /// test that resolves a model has to put one there first. Production seeds it from the
    /// fusiform poll; this is the offline equivalent, and it uses the vendored snapshot so both
    /// catalog-content and auth-method assertions exercise production identities.
    ///
    /// One test-only addition to the production overlay: a `remove` of a model the snapshot
    /// presently lists, so the "retired by broca's own overlay" refusal has a subject. The
    /// production overlay carried a real one (deepseek-chat) until upstream withdrew that id
    /// on 2026-09-18; a refusal message contract must not depend on a vendor keeping a dead
    /// row, so the subject lives here. No other test in this module resolves the model.
    fn seed_catalog_for_resolution() {
        use std::sync::Once;
        static SEEDED: Once = Once::new();
        SEEDED.call_once(|| {
            let mut overlay: serde_json::Value =
                serde_json::from_str(include_str!("../data/overlay.json"))
                    .expect("bundled overlay is JSON");
            overlay["remove"][TEST_RETIRED_PROVIDER] = serde_json::json!([TEST_RETIRED_MODEL]);
            let catalog = crate::Catalog::from_json_str(
                include_str!("../data/models.dev.json"),
                &overlay.to_string(),
            )
            .expect("bundled snapshot is well formed");
            let cache = std::sync::Arc::new(crate::CatalogCache::empty());
            cache.install_parsed_for_test(&catalog, 0, crate::CatalogSource::Fusiform);
            crate::install_global_catalog_cache(cache);
        });
    }

    #[test]
    fn opencode_go_anthropic_route_uses_the_sdk_api_key_header_shape() {
        let auth_kind = provider_route_auth_kind(
            "opencode-go",
            WireFamilyId::AnthropicMessages,
            &AuthKind::Bearer,
        );
        assert!(matches!(
            auth_kind,
            AuthKind::ApiKeyHeader { header } if header == "x-api-key"
        ));

        let mut frozen = anthropic_frozen_config("minimax-m3");
        apply_auth_method_quirks("opencode-go", None, &mut frozen);
        assert_eq!(frozen.system_prompt, SystemPromptPolicy::TopLevelField);
        assert_eq!(frozen.reasoning, ReasoningPolicy::None);
    }

    #[test]
    fn opencode_go_resume_restores_recorded_family_and_base_url() {
        seed_catalog_for_resolution();
        let options = ResolveOptions::default();
        let session_id = ClaudeCodePersona::derive_session_id("opaque-session-ref");
        let fresh = resolve_catalog_episode_with_client(
            "opencode-go/gpt-6-luna",
            Some(2_048),
            &AuthMode::None,
            &options,
            broca_provider::build_provider_http_client(),
            EpisodeRenderConfig {
                opencode_go_session_id: Some(&session_id),
                ..Default::default()
            },
        )
        .expect("the current model pin resolves");
        assert_eq!(fresh.family_id, WireFamilyId::OpenAiResponses);
        assert_eq!(fresh.base_url, "https://opencode.ai/inference/go/v1");

        let qwen = resolve_catalog_episode_with_client(
            "opencode-go/qwen3.5-plus",
            Some(2_048),
            &AuthMode::None,
            &options,
            broca_provider::build_provider_http_client(),
            EpisodeRenderConfig {
                opencode_go_session_id: Some(&session_id),
                ..Default::default()
            },
        )
        .expect("the Go Qwen compatibility model resolves");
        assert_eq!(
            qwen.provider.render_config_blob()["opencode_go_qwen_thinking"],
            true
        );

        let deepseek = resolve_catalog_episode_with_client(
            "opencode-go/deepseek-v4-pro",
            Some(2_048),
            &AuthMode::None,
            &options,
            broca_provider::build_provider_http_client(),
            EpisodeRenderConfig {
                opencode_go_session_id: Some(&session_id),
                ..Default::default()
            },
        )
        .expect("the Go DeepSeek compatibility model resolves");
        let deepseek_config = deepseek.provider.render_config_blob();
        assert_eq!(deepseek_config["opencode_go_deepseek_thinking"], true);
        assert!(deepseek_config.get("reasoning_echo").is_some());

        let kimi = resolve_catalog_episode_with_client(
            "opencode-go/kimi-k2.6",
            Some(2_048),
            &AuthMode::None,
            &options,
            broca_provider::build_provider_http_client(),
            EpisodeRenderConfig {
                opencode_go_session_id: Some(&session_id),
                ..Default::default()
            },
        )
        .expect("the Go Kimi compatibility model resolves");
        assert_eq!(
            kimi.provider.render_config_blob()["opencode_go_long_cache_retention_unsupported"],
            true
        );

        let resumed = resolve_catalog_episode_with_client(
            "opencode-go/gpt-6-luna",
            Some(2_048),
            &AuthMode::None,
            &options,
            broca_provider::build_provider_http_client(),
            EpisodeRenderConfig {
                wire_family: Some(WireFamilyId::OpenAiChat),
                opencode_go_session_id: Some(&session_id),
                opencode_go_reasoning_signature: Some(false),
                base_url: Some("https://opencode.ai/zen/go/v1"),
                ..Default::default()
            },
        )
        .expect("the recorded route remains resolvable");
        assert_eq!(resumed.family_id, WireFamilyId::OpenAiChat);
        assert_eq!(resumed.base_url, "https://opencode.ai/zen/go/v1");
        let frozen = resumed.provider.render_config_blob();
        assert_eq!(frozen["family_id"], "openai_chat");
        assert_eq!(frozen["base_url"], "https://opencode.ai/zen/go/v1");
        assert!(frozen.get("opencode_go_session_id").is_none());
        assert!(frozen.get("opencode_go_reasoning_signature").is_none());
    }

    #[test]
    fn catalog_refusal_distinguishes_overlay_retirement_from_unknown_model() {
        seed_catalog_for_resolution();
        let resolve_error = |model| {
            resolve_provider(
                model,
                None,
                1,
                &AuthMode::Secret("unused".to_string()),
                &ResolveOptions::default(),
            )
            .err()
            .unwrap_or_else(|| panic!("catalog unexpectedly served {model}"))
        };

        let retired = format!("{TEST_RETIRED_PROVIDER}/{TEST_RETIRED_MODEL}");
        assert_eq!(
            resolve_error(&retired).to_string(),
            format!(
                "catalog model '{retired}' is retired by broca's own overlay \
                 (crates/broca-catalog/data/overlay.json `remove`), not missing upstream"
            )
        );
        assert_eq!(
            resolve_error("deepseek/no-such-model").to_string(),
            "catalog has no served model 'deepseek/no-such-model' (unknown provider/model, \
             or the provider is pending — not yet catalog-served; pass a base_url to reach it \
             as an openai-compatible endpoint)"
        );
    }

    /// The chat-send gate reads `model.kind`: a stated kind other than `chat` is refused as a
    /// permanent configuration error naming the model and its kind; a stated `chat` and an
    /// absent kind both admit.
    #[test]
    fn a_stated_non_chat_model_kind_is_refused_for_chat_sends() {
        let chat_row = |kind: Option<&str>| {
            let mut row = serde_json::json!({
                "existence": "present",
                "limit.context": 200000,
                "capability.attachment": false,
                "capability.reasoning": false,
                "capability.tool_call": true,
            });
            if let Some(kind) = kind {
                row["model.kind"] = serde_json::json!(kind);
            }
            row
        };
        let response: crate::FusiformCatalogResponse = serde_json::from_value(serde_json::json!({
            "source": "test",
            "resolved_at_ms": 0,
            "catalog_version": 1,
            "models": {
                "anthropic/unstated": chat_row(None),
                "anthropic/stated-chat": chat_row(Some("chat")),
                "anthropic/embedder": chat_row(Some("embedding")),
                "cloudflare-workers-ai/@cf/cloudflare/clef": {
                    "existence": "present",
                    "model.kind": "decision",
                    "limit.context": 65536,
                    "capability.attachment": true,
                },
            },
        }))
        .expect("test response");
        let cache = crate::CatalogCache::empty();
        cache
            .install_for_test(response, crate::CatalogSource::Fusiform)
            .expect("installs");
        let catalog = cache.snapshot().expect("installed");

        let refused =
            |provider: &str, model: &str| match refuse_non_chat_model(&catalog, provider, model) {
                Err(ResolutionError::Config(message)) => message,
                other => panic!("{provider}/{model} must be refused as Config, got {other:?}"),
            };
        assert_eq!(
            refused("cloudflare-workers-ai", "@cf/cloudflare/clef"),
            "catalog model 'cloudflare-workers-ai/@cf/cloudflare/clef' is a decision model \
             (model.kind \"decision\"), not a chat model; session.send serves only chat models"
        );
        let embedder = refused("anthropic", "embedder");
        assert!(embedder.contains("'anthropic/embedder'") && embedder.contains("embedding"));
        let class =
            broca_core::RunError::from(ResolutionError::Config(embedder)).admission_error_class();
        assert_eq!(class, broca_protocol::ErrorClass::Permanent);

        refuse_non_chat_model(&catalog, "anthropic", "stated-chat").expect("chat admits");
        refuse_non_chat_model(&catalog, "anthropic", "unstated")
            .expect("an absent kind admits exactly as before");
        refuse_non_chat_model(&catalog, "anthropic", "not-listed")
            .expect("an unlisted model is left to resolution's own refusal");
    }

    /// Resolve an id on the ChatGPT override path the way the serve layer does, with no
    /// credential (the pause-only constructor), returning the frozen config or the refusal.
    fn resolve_chatgpt(model: &str) -> Result<FrozenRenderConfig, String> {
        seed_catalog_for_resolution();
        resolve_chatgpt_pause_provider_with_client(
            model,
            None,
            None,
            &ResolveOptions {
                auth_method: Some(AuthMethod::Chatgpt),
                family_override: Some(WireFamilyId::OpenAiResponsesChatgpt),
                ..ResolveOptions::default()
            },
            broca_provider::build_provider_http_client(),
        )
        .map(|resolved| {
            serde_json::from_value(resolved.provider.render_config_blob())
                .expect("resolved provider exposes its frozen config")
        })
        .map_err(|error| error.to_string())
    }

    #[test]
    fn a_real_catalog_pro_id_resolves_as_itself_on_every_path() {
        // On the ChatGPT override path: the catalog lists gpt-5.5-pro, so it is sent as that
        // model, with no effort attached.
        let frozen = resolve_chatgpt("openai/gpt-5.5-pro")
            .unwrap_or_else(|error| panic!("a catalog -pro id must resolve: {error}"));
        assert_eq!(frozen.wire_model_id, "gpt-5.5-pro");
        assert_eq!(frozen.reasoning_effort, None);
        assert_eq!(frozen.service_tier, None);
        // On the ordinary catalog path.
        seed_catalog_for_resolution();
        let resolved = resolve_provider(
            "deepseek/deepseek-v4-pro",
            None,
            2048,
            &AuthMode::Secret("unused".to_string()),
            &ResolveOptions {
                auth_method: Some(AuthMethod::ApiKey),
                ..ResolveOptions::default()
            },
        )
        .unwrap_or_else(|error| panic!("deepseek-v4-pro must resolve: {error}"));
        assert_eq!(resolved.wire_model, "deepseek-v4-pro");
    }

    #[test]
    fn a_moniker_only_pro_id_is_refused_as_unknown() {
        // gpt-5.6-sol is a catalog model; gpt-5.6-sol-pro existed only as a moniker for it with
        // effort max. It is now an unknown model, not a silent effort mapping.
        assert!(resolve_chatgpt("openai/gpt-5.6-sol").is_ok());
        let error =
            resolve_chatgpt("openai/gpt-5.6-sol-pro").expect_err("a -pro moniker must not resolve");
        assert!(
            error.contains("catalog has no served model 'openai/gpt-5.6-sol-pro'"),
            "{error}"
        );
    }

    #[test]
    fn a_moniker_only_fast_id_is_refused_naming_service_tier() {
        // gpt-5.6-luna is a catalog model; gpt-5.6-luna-fast existed only as a moniker for it
        // with the priority tier. Priority is now requested as service_tier, so the moniker is an
        // unknown model and the refusal says how to ask for priority instead.
        assert!(resolve_chatgpt("openai/gpt-5.6-luna").is_ok());
        let error = resolve_chatgpt("openai/gpt-5.6-luna-fast")
            .expect_err("a -fast moniker must not resolve");
        assert!(
            error.contains("catalog has no served model 'openai/gpt-5.6-luna-fast'"),
            "{error}"
        );
        assert!(error.contains("service_tier: \"priority\""), "{error}");
    }

    fn resolved_anthropic(method: AuthMethod) -> (ProviderResolution, FrozenRenderConfig) {
        seed_catalog_for_resolution();
        let resolved = resolve_provider(
            "anthropic/claude-opus-4-8",
            None,
            2048,
            &AuthMode::Secret("test-secret".to_string()),
            &ResolveOptions {
                auth_method: Some(method),
                ..ResolveOptions::default()
            },
        )
        .unwrap_or_else(|error| panic!("resolve anthropic {method:?}: {error}"));
        let frozen = serde_json::from_value(resolved.provider.render_config_blob())
            .expect("resolved provider exposes its frozen config");
        (resolved, frozen)
    }

    fn anthropic_endpoint(
        method: AuthMethod,
        frozen: &FrozenRenderConfig,
    ) -> broca_provider::wire::HttpRequest {
        let (auth, _, effective_method) = resolve_auth(
            &AuthMode::Secret("test-secret".to_string()),
            &crate::default_auth_kind(WireFamilyId::AnthropicMessages),
            "anthropic",
            Some(method),
        )
        .expect("shape anthropic credential");
        assert_eq!(effective_method, Some(method));
        AnthropicMessages.endpoint(frozen, &auth, b"{}".to_vec())
    }

    #[test]
    fn production_anthropic_method_selects_body_and_endpoint_auth_shape() {
        let (api_key, api_key_frozen) = resolved_anthropic(AuthMethod::ApiKey);
        assert_eq!(api_key.auth_method, Some(AuthMethod::ApiKey));
        assert_eq!(
            api_key_frozen.system_prompt,
            SystemPromptPolicy::TopLevelField
        );
        assert_eq!(api_key_frozen.reasoning, ReasoningPolicy::None);
        let api_key_body: serde_json::Value =
            serde_json::from_slice(&api_key.provider.render_request(&anthropic_options()))
                .expect("api-key body is JSON");
        assert!(api_key_body.get("thinking").is_none());
        assert_eq!(api_key_body["system"][0]["text"], "caller system");
        let api_key_request = anthropic_endpoint(AuthMethod::ApiKey, &api_key_frozen);
        assert_eq!(header(&api_key_request, "x-api-key"), Some("test-secret"));
        assert!(header(&api_key_request, "authorization").is_none());
        assert!(header(&api_key_request, "anthropic-beta").is_none());
        // An api-key request identifies as broca, not as Claude Code.
        assert_eq!(
            header(&api_key_request, "user-agent"),
            Some(concat!("broca/", env!("CARGO_PKG_VERSION")))
        );
        assert!(header(&api_key_request, "x-app").is_none());

        let (oauth, oauth_frozen) = resolved_anthropic(AuthMethod::Oauth);
        assert_eq!(oauth.auth_method, Some(AuthMethod::Oauth));
        assert!(matches!(
            oauth_frozen.system_prompt,
            SystemPromptPolicy::RequiredIdentityLead { .. }
        ));
        assert!(matches!(
            oauth_frozen.reasoning,
            ReasoningPolicy::AnthropicInterleaved { .. }
        ));
        let oauth_body: serde_json::Value =
            serde_json::from_slice(&oauth.provider.render_request(&anthropic_options()))
                .expect("OAuth body is JSON");
        assert!(oauth_body["system"][0]["text"]
            .as_str()
            .is_some_and(|text| text.contains("Claude Code")));
        assert!(oauth_body.get("thinking").is_some());
        let oauth_request = anthropic_endpoint(AuthMethod::Oauth, &oauth_frozen);
        assert_eq!(
            header(&oauth_request, "authorization"),
            Some("Bearer test-secret")
        );
        assert!(header(&oauth_request, "x-api-key").is_none());
        assert!(header(&oauth_request, "anthropic-beta")
            .is_some_and(|value| value.contains("oauth-2025-04-20")));
        // The OAuth identity-lead shape keeps the Claude Code client identity.
        assert_eq!(
            header(&oauth_request, "user-agent"),
            Some("claude-cli/2.1.280 (external, cli)")
        );
        assert_eq!(header(&oauth_request, "x-app"), Some("cli"));
    }

    #[test]
    fn api_key_method_does_not_change_other_provider_auth_kinds() {
        let (openai, _, _) = resolve_auth(
            &AuthMode::Secret("openai-key".to_string()),
            &AuthKind::Bearer,
            "openai",
            Some(AuthMethod::ApiKey),
        )
        .expect("shape OpenAI key");
        assert!(matches!(openai, AuthHandle::Bearer(value) if value == "openai-key"));

        let gemini_kind = AuthKind::ApiKeyHeader {
            header: "x-goog-api-key".to_string(),
        };
        let (gemini, _, _) = resolve_auth(
            &AuthMode::Secret("gemini-key".to_string()),
            &gemini_kind,
            "google",
            Some(AuthMethod::ApiKey),
        )
        .expect("shape Gemini key");
        assert!(matches!(
            gemini,
            AuthHandle::ApiKeyHeader { header, value }
                if header == "x-goog-api-key" && value == "gemini-key"
        ));
    }

    // JWTs with an `alg:none` header + a base64url payload (signature ignored — we read our
    // own token, never validate it). Generated to carry the account-id in each claim shape.
    const TOP_LEVEL: &str =
        "eyJhbGciOiJub25lIn0.eyJjaGF0Z3B0X2FjY291bnRfaWQiOiAiYWNjdC10b3AifQ.sig";
    const NESTED: &str = "eyJhbGciOiJub25lIn0.eyJodHRwczovL2FwaS5vcGVuYWkuY29tL2F1dGgiOiB7ImNoYXRncHRfYWNjb3VudF9pZCI6ICJhY2N0LW5lc3RlZCJ9fQ.sig";
    const ORG: &str =
        "eyJhbGciOiJub25lIn0.eyJvcmdhbml6YXRpb25zIjogW3siaWQiOiAib3JnLWZpcnN0In1dfQ.sig";
    const NO_CLAIM: &str = "eyJhbGciOiJub25lIn0.eyJlbWFpbCI6ICJ4QHkueiJ9.sig";

    #[test]
    fn retired_antigravity_routes_refuse_with_live_replacements() {
        seed_catalog_for_resolution();
        for (route, replacement) in [
            ("gemini-3.7-flash", "google/antigravity-gemini-3.8-flash"),
            (
                "gemini-3.7-flash-low",
                "google/antigravity-gemini-3.8-flash",
            ),
            (
                "gemini-3.7-flash-medium",
                "google/antigravity-gemini-3.8-flash",
            ),
            (
                "gemini-3.7-flash-high",
                "google/antigravity-gemini-3.8-flash",
            ),
            ("gemini-3.6-flash", "google/antigravity-gemini-3.8-flash"),
            (
                "gemini-3.6-flash-low",
                "google/antigravity-gemini-3.8-flash",
            ),
            (
                "gemini-3.6-flash-medium",
                "google/antigravity-gemini-3.8-flash",
            ),
            (
                "gemini-3.6-flash-high",
                "google/antigravity-gemini-3.8-flash",
            ),
            ("gemini-3.1-pro", "google/antigravity-gemini-4-argon"),
            ("gemini-3.1-pro-low", "google/antigravity-gemini-4-argon"),
            ("gemini-3.1-pro-high", "google/antigravity-gemini-4-argon"),
            ("gemini-pro-agent", "google/antigravity-gemini-4-argon"),
            ("claude-sonnet-4-6-thinking", "anthropic/claude-sonnet-5-5"),
        ] {
            for prefix in ["", "antigravity-"] {
                let model = format!("google/{prefix}{route}");
                let result = resolve_provider(
                    &model,
                    None,
                    2048,
                    &AuthMode::Bearer("unused".into()),
                    &ResolveOptions {
                        auth_method: Some(AuthMethod::Antigravity),
                        family_override: Some(WireFamilyId::GoogleCodeAssist),
                        ..Default::default()
                    },
                );
                let error = result
                    .err()
                    .unwrap_or_else(|| panic!("served retired {model}"));
                assert!(matches!(error, ResolutionError::Config(_)));
                let advice = if route == "claude-sonnet-4-6-thinking" {
                    " through the Anthropic provider"
                } else {
                    " (medium)"
                };
                assert_eq!(
                    error.to_string(),
                    format!(
                        "model '{model}' is unservable: Google withdrew this Antigravity route; \
                         use '{replacement}'{advice} instead"
                    )
                );
            }
        }
    }

    #[test]
    fn live_antigravity_replacements_and_native_gemini_still_resolve() {
        seed_catalog_for_resolution();
        for (model, family, expected_wire_id) in [
            (
                "google/antigravity-gemini-3.8-flash",
                Some(WireFamilyId::GoogleCodeAssist),
                "gemini-3.8-flash-medium",
            ),
            (
                "google/antigravity-gemini-4-argon",
                Some(WireFamilyId::GoogleCodeAssist),
                "gemini-4-argon-medium",
            ),
            // Pin the native family independently of catalog membership: these withdrawals
            // concern Antigravity, not the ordinary Gemini API.
            (
                "google/gemini-3.1-pro",
                Some(WireFamilyId::Gemini),
                "gemini-3.1-pro",
            ),
        ] {
            let result = resolve_provider(
                model,
                None,
                2048,
                &AuthMode::Bearer("unused".into()),
                &ResolveOptions {
                    family_override: family,
                    project_id: Some("live-route-project"),
                    ..Default::default()
                },
            );
            let resolved = result.unwrap_or_else(|error| panic!("{model}: {error}"));
            assert_eq!(resolved.wire_model, expected_wire_id);
        }
    }

    #[test]
    fn antigravity_wire_model_aliases_map_only_registered_routes() {
        let assert_wire_bytes = |route: &str, wire_id: &str| {
            use broca_provider::families::{google_codeassist_frozen_config, GoogleCodeAssist};
            let mut actual =
                google_codeassist_frozen_config(google_codeassist_wire_model_id(route));
            let mut expected = google_codeassist_frozen_config(wire_id);
            actual.project_id = Some("wire-bytes-project".into());
            expected.project_id = Some("wire-bytes-project".into());
            let mut request = anthropic_options();
            request.step_context = Some(broca_protocol::StepContext {
                session_address: None,
                credential_id: None,
                account_ref: None,
                run_id: "live-route-bytes".into(),
                step_index: 0,
            });
            assert_eq!(
                GoogleCodeAssist.render(&actual, &request),
                GoogleCodeAssist.render(&expected, &request),
                "{route} wire bytes"
            );
        };
        let mapped = [
            ("gemini-3.5-flash", "gemini-3.5-flash-low"),
            ("gemini-3.5-flash-high", "gemini-3-flash-agent"),
            ("gemini-3.5-flash-low", "gemini-3.5-flash-extra-low"),
            ("gemini-3.8-flash", "gemini-3.8-flash-medium"),
            ("gemini-4-argon", "gemini-4-argon-medium"),
            ("gemini-3-flash", "gemini-3-flash-medium"),
            ("gemini-3-pro", "gemini-3-pro-low"),
        ];
        for (route, wire_id) in mapped {
            assert_eq!(google_codeassist_wire_model_id(route), wire_id);
            assert_wire_bytes(route, wire_id);
            assert_wire_bytes(&format!("antigravity-{route}"), wire_id);
            assert_eq!(
                google_codeassist_wire_model_id(&format!("antigravity-{route}")),
                wire_id
            );
        }

        // These live backend ids need no alias, including the literal 3.5-flash wire tiers.
        for tier_id in [
            "gemini-3.5-flash-extra-low",
            "gemini-3.5-flash-lite",
            "gemini-3.8-flash-low",
            "gemini-3.8-flash-medium",
            "gemini-3.8-flash-high",
            "gemini-3.8-flash-tiered",
            "gemini-3-flash-low",
            "gemini-3-flash-medium",
            "gemini-3-flash-high",
            "gemini-3-pro-low",
            "gemini-3-pro-high",
            "gemini-4-argon-low",
            "gemini-4-argon-medium",
            "gemini-4-argon-high",
        ] {
            assert_eq!(google_codeassist_wire_model_id(tier_id), tier_id);
            assert_wire_bytes(tier_id, tier_id);
            assert_wire_bytes(&format!("antigravity-{tier_id}"), tier_id);
            assert_eq!(
                google_codeassist_wire_model_id(&format!("antigravity-{tier_id}")),
                tier_id
            );
        }

        assert_eq!(
            google_codeassist_wire_model_id("antigravity-gemini-2.5-flash"),
            "gemini-2.5-flash"
        );
        assert_eq!(
            google_codeassist_wire_model_id("gemini-3-pro-high"),
            "gemini-3-pro-high"
        );
        assert_eq!(
            google_codeassist_wire_model_id("unmapped-model"),
            "unmapped-model"
        );
    }

    /// A Code Assist variant selects the same backend id the caller would get by spelling the
    /// tier in the model id, and only ids with tier thinking settings count as tiers.
    #[test]
    fn code_assist_variant_tiers_follow_the_route_aliases_and_the_family_tier_table() {
        for (model, level, wire_id) in [
            (
                "antigravity-gemini-3.8-flash",
                "high",
                Some("gemini-3.8-flash-high"),
            ),
            (
                "antigravity-gemini-3.8-flash",
                "medium",
                Some("gemini-3.8-flash-medium"),
            ),
            (
                "antigravity-gemini-3.8-flash-high",
                "low",
                Some("gemini-3.8-flash-low"),
            ),
            ("antigravity-gemini-3.8-flash", "minimal", None),
            // 3.5-flash's user-facing tiers are aliased to other backend ids.
            (
                "antigravity-gemini-3.5-flash",
                "low",
                Some("gemini-3.5-flash-extra-low"),
            ),
            (
                "antigravity-gemini-3.5-flash",
                "high",
                Some("gemini-3-flash-agent"),
            ),
            ("antigravity-claude-sonnet-4-6-thinking", "high", None),
        ] {
            assert_eq!(
                google_codeassist_tier_wire_id(model, level).as_deref(),
                wire_id,
                "{model} {level}"
            );
        }
        assert_eq!(
            google_codeassist_tiers("antigravity-gemini-3.8-flash"),
            ["low", "medium", "high"]
        );
        assert_eq!(
            google_codeassist_tiers("antigravity-gemini-3.5-flash"),
            ["low", "high"]
        );
        assert!(google_codeassist_tiers("antigravity-claude-sonnet-4-6-thinking").is_empty());
    }

    #[test]
    fn chatgpt_account_id_extracts_from_every_claim_shape() {
        assert_eq!(chatgpt_account_id(TOP_LEVEL).as_deref(), Some("acct-top"));
        assert_eq!(chatgpt_account_id(NESTED).as_deref(), Some("acct-nested"));
        assert_eq!(chatgpt_account_id(ORG).as_deref(), Some("org-first"));
    }

    #[test]
    fn chatgpt_account_id_none_when_no_claim_or_not_a_jwt() {
        assert_eq!(chatgpt_account_id(NO_CLAIM), None);
        assert_eq!(chatgpt_account_id("not-a-jwt"), None);
        assert_eq!(chatgpt_account_id("only.two"), None);
    }

    /// The full account-id resolution order: a non-blank vault id verbatim, then the top-level
    /// claim, then the nested `https://api.openai.com/auth` claim, then the first organization id.
    /// Blank or non-string values fall through, values are never trimmed, and the payload must be
    /// unpadded base64url JSON.
    #[test]
    fn chatgpt_account_id_resolution_order_and_token_shape() {
        use base64::Engine;
        let encode = |claims: serde_json::Value| {
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(serde_json::to_vec(&claims).unwrap())
        };
        let token =
            |claims: serde_json::Value| format!("eyJhbGciOiJub25lIn0.{}.sig", encode(claims));
        let resolve = |vault: Option<&str>, token: &str| {
            chatgpt_codex_auth(
                &AuthMode::Bearer(token.to_string()),
                "openai",
                Some("ses-1"),
                vault,
            )
            .map(|(handle, _)| {
                let AuthHandle::Headers(headers) = handle else {
                    panic!("expected the codex header set");
                };
                headers
                    .into_iter()
                    .find(|(name, _)| name == "chatgpt-account-id")
                    .map(|(_, value)| value)
                    .unwrap()
            })
        };
        let every_shape = token(serde_json::json!({
            "chatgpt_account_id": "acct-top",
            "https://api.openai.com/auth": {"chatgpt_account_id": "acct-nested"},
            "organizations": [{"id": "org-first"}],
        }));
        assert_eq!(resolve(None, &every_shape).as_deref(), Ok("acct-top"));
        assert_eq!(
            resolve(Some(" acct-vault "), &every_shape).as_deref(),
            Ok(" acct-vault "),
            "a vault id wins and is used as served"
        );
        let blank_top = token(serde_json::json!({
            "chatgpt_account_id": " ",
            "https://api.openai.com/auth": {"chatgpt_account_id": "acct-nested"},
        }));
        assert_eq!(resolve(None, &blank_top).as_deref(), Ok("acct-nested"));
        let numeric_top = token(serde_json::json!({
            "chatgpt_account_id": 7,
            "https://api.openai.com/auth": {"chatgpt_account_id": "acct-nested"},
        }));
        assert_eq!(resolve(None, &numeric_top).as_deref(), Ok("acct-nested"));
        let blank_nested = token(serde_json::json!({
            "https://api.openai.com/auth": {"chatgpt_account_id": ""},
            "organizations": [{"id": "org-first"}, {"id": "org-second"}],
        }));
        assert_eq!(resolve(None, &blank_nested).as_deref(), Ok("org-first"));
        let blank_first_org = token(serde_json::json!({
            "organizations": [{"id": ""}, {"id": "org-second"}],
        }));
        assert!(
            resolve(None, &blank_first_org).is_err(),
            "only the first organization counts"
        );
        let untrimmed = token(serde_json::json!({"chatgpt_account_id": " acct "}));
        assert_eq!(resolve(None, &untrimmed).as_deref(), Ok(" acct "));
        let unsigned = format!(
            "eyJhbGciOiJub25lIn0.{}",
            encode(serde_json::json!({"chatgpt_account_id": "acct-unsigned"}))
        );
        assert_eq!(resolve(None, &unsigned).as_deref(), Ok("acct-unsigned"));
        // `{"chatgpt_account_id":"acct"}` is 29 bytes, so its padded encodings end in `=`.
        let claims = br#"{"chatgpt_account_id":"acct"}"#;
        for payload in [
            base64::engine::general_purpose::URL_SAFE.encode(claims),
            base64::engine::general_purpose::STANDARD.encode(claims),
        ] {
            assert!(payload.ends_with('='));
            let padded = format!("eyJhbGciOiJub25lIn0.{payload}.sig");
            assert!(
                resolve(None, &padded).is_err(),
                "a padded payload is not read"
            );
        }
        assert_eq!(
            resolve(None, NO_CLAIM),
            Err(
                "the 'openai' ChatGPT (codex) token carries no chatgpt_account_id claim — is \
                 this a ChatGPT-subscription token (not a plain OpenAI platform key)?"
                    .to_owned()
            )
        );
    }

    /// The shared ChatGPT policy's account-id vectors, run through broca's codex auth: each
    /// resolves to the vector's account header, or fails loud without one.
    #[test]
    fn shared_chatgpt_account_id_vectors_agree_with_codex_auth() {
        let document: serde_json::Value =
            serde_json::from_str(cortexkit_chatgpt_policy::vectors::ACCOUNT_IDS_JSON).unwrap();
        let vectors = document["vectors"].as_array().unwrap();
        assert!(vectors.len() >= 4);
        for vector in vectors {
            let name = vector["name"].as_str().unwrap();
            let token = vector["access_token"].as_str().unwrap();
            let result = chatgpt_codex_auth(
                &AuthMode::Bearer(token.to_string()),
                "openai",
                Some("ses-1"),
                vector["vault_account_id"].as_str(),
            );
            match vector["expected"]["account_id"].as_str() {
                Some(expected) => {
                    let (AuthHandle::Headers(headers), _) = result.unwrap() else {
                        panic!("{name}: expected the codex header set");
                    };
                    assert_eq!(
                        headers
                            .iter()
                            .find(|(header, _)| header == "chatgpt-account-id")
                            .map(|(_, value)| value.as_str()),
                        Some(expected),
                        "{name}"
                    );
                }
                None => {
                    let error = result.unwrap_err();
                    assert!(error.contains("chatgpt_account_id"), "{name}");
                    assert!(!error.contains(token), "{name}");
                }
            }
        }
    }

    #[test]
    fn chatgpt_codex_auth_fails_loud_without_account_claim() {
        // A token with no chatgpt_account_id must FAIL with a clear, non-secret message —
        // never produce a header set with an empty account id.
        let err = chatgpt_codex_auth(
            &AuthMode::Bearer(NO_CLAIM.to_string()),
            "openai",
            Some("ses-1"),
            None,
        )
        .unwrap_err();
        assert!(
            err.contains("chatgpt_account_id"),
            "fail-loud message names the missing claim; got: {err}"
        );
        // And it must not echo the token.
        assert!(!err.contains(NO_CLAIM));
    }

    #[test]
    fn empty_vault_account_id_falls_back_to_the_jwt_claim() {
        for empty in ["", "  \t"] {
            let (handle, _) = chatgpt_codex_auth(
                &AuthMode::Bearer(TOP_LEVEL.to_string()),
                "openai",
                Some("ses-1"),
                Some(empty),
            )
            .unwrap();
            let AuthHandle::Headers(headers) = handle else {
                panic!("expected AuthHandle::Headers");
            };
            assert_eq!(
                headers
                    .iter()
                    .find(|(name, _)| name == "chatgpt-account-id")
                    .map(|(_, value)| value.as_str()),
                Some("acct-top")
            );
        }
    }

    #[test]
    fn empty_account_ids_fail_loud() {
        use base64::Engine;

        for vault_id in ["", " \t "] {
            let error = chatgpt_codex_auth(
                &AuthMode::Bearer(NO_CLAIM.to_string()),
                "openai",
                Some("ses-1"),
                Some(vault_id),
            )
            .unwrap_err();
            assert!(error.contains("chatgpt_account_id"));
        }

        for claim in ["", "  \t"] {
            let payload = serde_json::to_vec(&serde_json::json!({
                "chatgpt_account_id": claim
            }))
            .unwrap();
            let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload);
            let token = format!("eyJhbGciOiJub25lIn0.{encoded}.sig");
            for vault_id in [Some(""), Some(" \t "), None] {
                let error = chatgpt_codex_auth(
                    &AuthMode::Bearer(token.clone()),
                    "openai",
                    Some("ses-1"),
                    vault_id,
                )
                .unwrap_err();
                assert!(error.contains("chatgpt_account_id"));
            }
        }
    }

    #[test]
    fn chatgpt_codex_auth_builds_the_codex_header_set() {
        let (handle, desc) = chatgpt_codex_auth(
            &AuthMode::Bearer(TOP_LEVEL.to_string()),
            "openai",
            Some("ses-xyz"),
            None,
        )
        .unwrap();
        assert_eq!(desc, "chatgpt(openai)");
        let headers = match handle {
            AuthHandle::Headers(h) => h,
            _ => panic!("expected AuthHandle::Headers"),
        };
        let get = |k: &str| {
            headers
                .iter()
                .find(|(n, _)| n == k)
                .map(|(_, v)| v.as_str())
        };
        assert_eq!(
            get("authorization"),
            Some(format!("Bearer {TOP_LEVEL}").as_str())
        );
        assert_eq!(get("chatgpt-account-id"), Some("acct-top"));
        assert_eq!(get("session-id"), Some("ses-xyz"));
        // The backend serves newer models only to the codex CLI identity pair.
        assert_eq!(get("originator"), Some("codex_cli_rs"));
        assert!(
            get("user-agent").is_some_and(
                |ua| ua.starts_with("codex_cli_rs/") && ua.contains(std::env::consts::OS)
            )
        );
    }

    /// The vault-served account id wins over the token's JWT claim (it tracks re-logins);
    /// and it rescues a token whose JWT carries no claim at all.
    #[test]
    fn caller_cache_scope_sets_the_codex_affinity_header() {
        let caller_key = "alfonso-classify-cache-scope-".repeat(4);
        let expected = broca_provider::hash::clamp_prompt_cache_key(&caller_key);
        let (handle, _) = chatgpt_codex_auth(
            &AuthMode::Bearer(TOP_LEVEL.to_string()),
            "openai",
            Some(&caller_key),
            None,
        )
        .unwrap();
        let AuthHandle::Headers(headers) = handle else {
            panic!("expected the codex header set");
        };
        assert_eq!(
            headers
                .iter()
                .find(|(name, _)| name == "session-id")
                .map(|(_, value)| value.as_str()),
            Some(expected.as_str())
        );
    }

    #[test]
    fn chatgpt_codex_auth_prefers_the_vault_account_id() {
        let account = |handle: AuthHandle| match handle {
            AuthHandle::Headers(h) => h
                .into_iter()
                .find(|(n, _)| n == "chatgpt-account-id")
                .map(|(_, v)| v),
            _ => panic!("expected AuthHandle::Headers"),
        };
        // Vault id beats the JWT claim.
        let (handle, _) = chatgpt_codex_auth(
            &AuthMode::Bearer(TOP_LEVEL.to_string()),
            "openai",
            Some("ses-1"),
            Some("acct-vault"),
        )
        .unwrap();
        assert_eq!(account(handle).as_deref(), Some("acct-vault"));
        // Vault id rescues a claim-less token (the JWT fallback would fail loud).
        let (handle, _) = chatgpt_codex_auth(
            &AuthMode::Bearer(NO_CLAIM.to_string()),
            "openai",
            Some("ses-1"),
            Some("acct-vault"),
        )
        .unwrap();
        assert_eq!(account(handle).as_deref(), Some("acct-vault"));
    }

    /// Read the headers and complete body from an actual HTTP request, without logging either.
    async fn capture_codex_request(
        socket: &mut tokio::net::TcpStream,
    ) -> (std::collections::BTreeMap<String, String>, Vec<u8>) {
        use tokio::io::AsyncReadExt;

        let mut request = Vec::new();
        let mut chunk = [0u8; 8192];
        let header_end = loop {
            let read = socket.read(&mut chunk).await.unwrap();
            assert_ne!(read, 0, "request headers must complete");
            request.extend_from_slice(&chunk[..read]);
            if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                break end + 4;
            }
        };
        let headers: std::collections::BTreeMap<_, _> = std::str::from_utf8(&request[..header_end])
            .unwrap()
            .lines()
            .skip(1)
            .filter_map(|line| line.split_once(':'))
            .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_owned()))
            .collect();
        let length: usize = headers["content-length"].parse().unwrap();
        while request.len() < header_end + length {
            let read = socket.read(&mut chunk).await.unwrap();
            assert_ne!(read, 0, "request body must complete");
            request.extend_from_slice(&chunk[..read]);
        }
        (headers, request[header_end..header_end + length].to_vec())
    }

    #[tokio::test]
    async fn websocket_step_and_recorded_https_warm_share_cache_affinity() {
        use futures::{SinkExt, StreamExt};
        use tokio::io::AsyncWriteExt;
        use tokio_tungstenite::tungstenite::{
            self,
            handshake::server::{Callback, ErrorResponse, Request, Response},
            Message as WsMessage,
        };
        struct CaptureHeaders<'a>(&'a mut std::collections::BTreeMap<String, String>);
        impl Callback for CaptureHeaders<'_> {
            fn on_request(
                self,
                request: &Request,
                response: Response,
            ) -> Result<Response, ErrorResponse> {
                *self.0 = request
                    .headers()
                    .iter()
                    .map(|(name, value)| (name.to_string(), value.to_str().unwrap().to_owned()))
                    .collect();
                Ok(response)
            }
        }

        seed_catalog_for_resolution();
        let long_key = "caller-cache-scope-".repeat(8);
        for (caller_key, session_id, served_account) in [
            (
                Some("caller-scope"),
                Some("different-session"),
                Some("acct-vault"),
            ),
            (
                Some(long_key.as_str()),
                Some("different-session"),
                Some("acct-vault"),
            ),
            (None, Some(long_key.as_str()), None),
            (None, None, None),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let capture = tokio::spawn(async move {
                tokio::time::timeout(std::time::Duration::from_secs(10), async {
                    let (tcp, _) = listener.accept().await.unwrap();
                    let mut headers = std::collections::BTreeMap::new();
                    let mut config = tungstenite::protocol::WebSocketConfig::default();
                    config.extensions.permessage_deflate = Some(tungstenite::extensions::compression::deflate::DeflateConfig::default());
                    let mut socket = tokio_tungstenite::accept_hdr_async_with_config(tcp, CaptureHeaders(&mut headers), Some(config)).await.unwrap();
                    let request = socket.next().await.unwrap().unwrap();
                    let WsMessage::Text(request) = request else { panic!("a response.create text frame is required"); };
                    let step_body: serde_json::Value = serde_json::from_slice(request.as_bytes()).unwrap();
                    assert_eq!(step_body["type"], "response.create");
                    let event = concat!("{\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",",
                        "\"output\":[],\"usage\":{\"input_tokens\":42,\"output_tokens\":0,\"input_tokens_details\":{\"cached_tokens\":42}}}}");
                    socket.send(WsMessage::Text(event.into())).await.unwrap();
                    // The step's socket stays open while an independent HTTPS connection warms its prefix.
                    let (mut http, _) = listener.accept().await.unwrap();
                    let (warm_headers, warm_body) = capture_codex_request(&mut http).await;
                    let sse = format!("data: {event}\n\n");
                    let reply = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{sse}", sse.len());
                    http.write_all(reply.as_bytes()).await.unwrap();
                    (headers, step_body, warm_headers, serde_json::from_slice::<serde_json::Value>(&warm_body).unwrap())
                }).await.expect("socket step and HTTPS warm must both reach the mock")
            });
            let auth = AuthMode::Secret(TOP_LEVEL.to_owned());
            let client = broca_provider::build_provider_http_client();
            let step = resolve_catalog_episode_at_base_url(
                "openai/gpt-5.4-mini",
                &url,
                Some(2048),
                &auth,
                &ResolveOptions {
                    family_override: Some(WireFamilyId::OpenAiResponsesChatgpt),
                    auth_method: Some(AuthMethod::Chatgpt),
                    session_id,
                    account_id: served_account,
                    frozen_account_ref: served_account,
                    prompt_cache_key: caller_key,
                    ..Default::default()
                },
                client.clone(),
                EpisodeRenderConfig::default(),
            )
            .unwrap_or_else(|_| panic!("step must resolve"));
            let frozen: FrozenRenderConfig =
                serde_json::from_value(step.provider.render_config_blob()).unwrap();
            let options = CallOptions::new(vec![Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "ping".into(),
                }],
            )]);
            let base = step.provider.render_request(&options);
            let mut events = step
                .provider
                .step(broca_protocol::StepId(1), &options)
                .await;
            let mut complete = false;
            let mut socket_attempt = false;
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                while let Some(event) = events.recv().await {
                    match event {
                        broca_protocol::NormalizedEvent::Control(
                            broca_protocol::ProviderControl::StepFinished { .. },
                        ) => complete = true,
                        broca_protocol::NormalizedEvent::Control(
                            broca_protocol::ProviderControl::AttemptFinished { attempt },
                        ) => socket_attempt |= attempt.transport.as_deref() == Some("websocket"),
                        _ => {}
                    }
                }
            })
            .await
            .expect("socket step must terminate");
            assert!(
                complete && socket_attempt,
                "the actual model step must complete over WebSocket"
            );
            let warm = provider_for_recorded_config(
                "openai",
                "gpt-5.4-mini",
                frozen.clone(),
                &auth,
                Some(AuthMethod::Chatgpt),
                served_account,
                served_account,
                client,
                &broca_core::clock::Clock::default(),
            )
            .unwrap_or_else(|_| panic!("recorded warm provider must resolve"));
            warm.warm(&base)
                .await
                .unwrap_or_else(|_| panic!("HTTPS warm must complete"));
            step.provider.close_run_transport();
            let (step_headers, step_body, warm_headers, warm_body) = capture.await.unwrap();
            for name in [
                "authorization",
                "session-id",
                "chatgpt-account-id",
                "originator",
                "user-agent",
            ] {
                assert!(
                    step_headers.contains_key(name),
                    "socket handshake must carry {name}"
                );
                assert!(
                    step_headers.get(name) == warm_headers.get(name),
                    "warm and socket step must share {name}"
                );
            }
            assert_eq!(
                step_body.get("prompt_cache_key"),
                warm_body.get("prompt_cache_key")
            );
            match &frozen.prompt_cache_key {
                Some(key) => assert_eq!(step_body["prompt_cache_key"], serde_json::json!(key)),
                None => assert!(
                    step_body.get("prompt_cache_key").is_none(),
                    "legacy fallback has no body key on either pipe"
                ),
            }
            assert!(step_headers.contains_key("openai-beta"));
            assert!(
                !warm_headers.contains_key("openai-beta"),
                "socket-only beta must not leak onto warm"
            );
            assert!(step_headers.contains_key("thread-id"));
            assert!(!warm_headers.contains_key("thread-id"));
            assert!(step_headers.contains_key("x-client-request-id"));
            assert!(!warm_headers.contains_key("x-client-request-id"));
        }
    }

    #[tokio::test]
    async fn recorded_codex_warm_sends_the_same_session_headers_as_a_step() {
        use tokio::io::AsyncWriteExt;

        seed_catalog_for_resolution();
        let long_key = "caller-cache-scope-".repeat(8);
        // Exercise caller scope precedence, clamping, session-derived scope and legacy fallback.
        for (caller_key, session_id, served_account) in [
            (
                Some("caller-scope"),
                Some("different-session"),
                Some("acct-vault"),
            ),
            (
                Some(long_key.as_str()),
                Some("different-session"),
                Some("acct-vault"),
            ),
            (None, Some(long_key.as_str()), None),
            (None, None, None),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let capture = tokio::spawn(async move {
                tokio::time::timeout(std::time::Duration::from_secs(10), async {
                    let mut requests = Vec::new();
                    for _ in 0..2 {
                        let (mut socket, _) = listener.accept().await.unwrap();
                        requests.push(capture_codex_request(&mut socket).await);
                        let body = concat!(
                            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",",
                            "\"output\":[],\"usage\":{\"input_tokens\":42,\"output_tokens\":0,",
                            "\"input_tokens_details\":{\"cached_tokens\":42}}}}\n\n"
                        );
                        let reply = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        socket.write_all(reply.as_bytes()).await.unwrap();
                    }
                    requests
                })
                .await
                .expect("both step and warm must reach the capture server")
            });
            let auth = AuthMode::Secret(TOP_LEVEL.to_owned());
            let client = broca_provider::build_provider_http_client();
            let step = resolve_catalog_episode_at_base_url(
                "openai/gpt-5.4-mini",
                &url,
                Some(2048),
                &auth,
                &ResolveOptions {
                    family_override: Some(WireFamilyId::OpenAiResponsesChatgpt),
                    auth_method: Some(AuthMethod::Chatgpt),
                    session_id,
                    account_id: served_account,
                    frozen_account_ref: served_account,
                    prompt_cache_key: caller_key,
                    ..Default::default()
                },
                client.clone(),
                EpisodeRenderConfig::default(),
            )
            .unwrap_or_else(|_| panic!("step must resolve"));
            // The capture server reads two HTTP bodies, one step and one warm; pin HTTPS
            // so the header comparison does not send a socket upgrade to this SSE fixture.
            step.provider.set_chatgpt_https_policy(Arc::new(|| true));
            let frozen: FrozenRenderConfig =
                serde_json::from_value(step.provider.render_config_blob()).unwrap();
            let options = CallOptions::new(vec![Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "ping".into(),
                }],
            )]);
            let base = step.provider.render_request(&options);
            let mut events = step
                .provider
                .step(broca_protocol::StepId(1), &options)
                .await;
            let mut completed = false;
            while let Some(event) = events.recv().await {
                completed |= matches!(
                    event,
                    broca_protocol::NormalizedEvent::Control(
                        broca_protocol::ProviderControl::StepFinished { .. }
                    )
                );
            }
            assert!(completed, "the real step must finish successfully");
            let warm = provider_for_recorded_config(
                "openai",
                "gpt-5.4-mini",
                frozen.clone(),
                &auth,
                Some(AuthMethod::Chatgpt),
                served_account,
                served_account,
                client,
                &broca_core::clock::Clock::default(),
            )
            .unwrap_or_else(|_| panic!("recorded warm provider must resolve"));
            assert!(
                warm.render_request(&options) == base,
                "recorded render bytes must not change"
            );
            assert!(
                warm.warm(&base).await.is_ok(),
                "warm must send successfully"
            );
            let requests = capture.await.unwrap();
            let (step_headers, step_body) = &requests[0];
            let (warm_headers, warm_body) = &requests[1];
            let codex_headers = [
                "authorization",
                "chatgpt-account-id",
                "session-id",
                "originator",
                "user-agent",
            ];
            let missing: Vec<_> = codex_headers
                .iter()
                .copied()
                .filter(|name| !warm_headers.contains_key(*name))
                .collect();
            assert!(
                missing.is_empty(),
                "warm missing Codex headers: {missing:?}"
            );
            let different: Vec<_> = codex_headers
                .iter()
                .copied()
                .filter(|name| step_headers.get(*name) != warm_headers.get(*name))
                .collect();
            assert!(
                different.is_empty(),
                "warm/step Codex headers differ: {different:?}"
            );
            // HTTP computes Content-Length from the output-capped warm body. All other request
            // headers, including the complete Codex set, must match without exceptions.
            let different: std::collections::BTreeSet<_> = step_headers
                .keys()
                .chain(warm_headers.keys())
                .filter(|name| name.as_str() != "content-length")
                .filter(|name| step_headers.get(*name) != warm_headers.get(*name))
                .collect();
            assert!(
                different.is_empty(),
                "request headers differ: {different:?}"
            );
            assert!(
                step_headers["authorization"] == format!("Bearer {TOP_LEVEL}"),
                "authorization must use the served token"
            );
            assert!(
                warm_headers["chatgpt-account-id"] == served_account.unwrap_or("acct-top"),
                "chatgpt-account-id must use served metadata or JWT fallback"
            );
            let expected_key = caller_key
                .or(session_id)
                .map(broca_provider::hash::clamp_prompt_cache_key);
            assert!(
                frozen.prompt_cache_key == expected_key,
                "scope must freeze with the step's clamping rule"
            );
            assert!(
                warm_headers["session-id"] == expected_key.as_deref().unwrap_or("broca"),
                "session-id must name the frozen scope"
            );
            assert!(
                warm_headers["originator"] == "codex_cli_rs",
                "originator must identify the Codex CLI"
            );
            assert!(
                warm_headers["user-agent"].starts_with("codex_cli_rs/"),
                "user-agent must identify the Codex CLI"
            );
            assert!(
                step_body == &base,
                "step must send its exact rendered bytes"
            );
            // The output cap is the warm's sole body edit, independently derived from the base.
            let base_text = std::str::from_utf8(&base).unwrap();
            let expected_warm =
                base_text.replacen("\"model\":", "\"max_output_tokens\":16,\"model\":", 1);
            assert!(
                warm_body == expected_warm.as_bytes(),
                "warm body must retain the prefix byte-identically"
            );
        }
    }
}

#[cfg(test)]
mod responses_tool_admission_tests {
    use super::*;
    use broca_protocol::request::{CallOptions, Tool};
    use broca_provider::families::{
        openai_responses_chatgpt_frozen_config, openai_responses_frozen_config, OpenAiResponses,
        OpenAiResponsesChatgpt,
    };
    use broca_provider::wire::WireFamily;
    use serde_json::json;

    fn configs() -> [FrozenRenderConfig; 2] {
        [
            openai_responses_frozen_config("gpt-4.1"),
            openai_responses_chatgpt_frozen_config("gpt-4.1"),
        ]
    }

    fn request() -> CallOptions {
        let mut opts = CallOptions::new(vec![]);
        opts.tools = ["read", "zoom"].into_iter().map(|name| Tool {
            name: name.into(),
            description: name.into(),
            input_schema: json!({"type":"object","properties":{"filePath":{"type":"string"},"limit":{"type":"integer"}},"required":["filePath"]}),
            module: None,
            execution_mode: Default::default(), reply: None,
        }).collect::<Vec<_>>().into();
        opts
    }

    fn render(config: &FrozenRenderConfig) -> Vec<u8> {
        match config.family_id {
            WireFamilyId::OpenAiResponses => OpenAiResponses.render(config, &request()),
            WireFamilyId::OpenAiResponsesChatgpt => {
                OpenAiResponsesChatgpt.render(config, &request())
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn newly_admitted_responses_tools_are_explicitly_non_strict() {
        for config in configs() {
            let admitted = EpisodeRenderConfig::default().apply(config);
            let blob = serde_json::to_value(&admitted).unwrap();
            assert_eq!(blob["responses_tools_non_strict"], true);
            assert_eq!(
                admitted.chatgpt_effort_updates,
                admitted.family_id == WireFamilyId::OpenAiResponsesChatgpt
            );
            let restored: FrozenRenderConfig = serde_json::from_value(blob).unwrap();
            let body: serde_json::Value = serde_json::from_slice(&render(&restored)).unwrap();
            assert_eq!(body["tools"].as_array().unwrap().len(), 2);
            for tool in body["tools"].as_array().unwrap() {
                assert_eq!(tool.get("strict"), Some(&json!(false)));
                assert_eq!(tool["parameters"]["required"], json!(["filePath"]));
            }
            assert_eq!(render(&restored), render(&admitted));
        }
    }

    #[test]
    fn resumed_pre_flag_responses_tools_omit_strict_byte_identically() {
        for config in configs() {
            let before = render(&config);
            let blob = serde_json::to_value(&config).unwrap();
            assert!(blob.get("responses_tools_non_strict").is_none());
            let recorded: FrozenRenderConfig = serde_json::from_value(blob.clone()).unwrap();
            assert!(!recorded.responses_tools_non_strict);
            let resumed = EpisodeRenderConfig {
                responses_tools_non_strict: Some(recorded.responses_tools_non_strict),
                chatgpt_effort_updates: Some(recorded.chatgpt_effort_updates),
                ..Default::default()
            }
            .apply(config);
            assert_eq!(serde_json::to_value(&resumed).unwrap(), blob);
            let bytes = render(&resumed);
            let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            for tool in body["tools"].as_array().unwrap() {
                assert!(tool.get("strict").is_none());
            }
            assert_eq!(bytes, before);
        }
    }

    #[test]
    fn chatgpt_wire_is_frozen_without_inventing_legacy_defaults() {
        let wire = serde_json::from_str(include_str!(
            "../../broca-provider/tests/fixtures/chatgpt_codex_0_163_identity.json"
        ))
        .unwrap();
        let config = openai_responses_chatgpt_frozen_config("gpt-5.3-codex");
        let admitted = EpisodeRenderConfig {
            chatgpt_wire: Some(&wire),
            ..Default::default()
        }
        .apply(config.clone());
        assert_eq!(admitted.chatgpt_wire.as_ref(), Some(&wire));
        let blob = serde_json::to_value(&admitted).unwrap();
        let recorded: FrozenRenderConfig = serde_json::from_value(blob.clone()).unwrap();
        let resumed = EpisodeRenderConfig {
            chatgpt_wire: recorded.chatgpt_wire.as_ref(),
            ..Default::default()
        }
        .apply(config.clone());
        assert_eq!(serde_json::to_value(&resumed).unwrap(), blob);
        assert_eq!(render(&resumed), render(&admitted));
        let legacy = EpisodeRenderConfig::default().apply(config);
        assert!(legacy.chatgpt_wire.is_none());
        assert!(serde_json::to_value(&legacy)
            .unwrap()
            .get("chatgpt_wire")
            .is_none());
    }
}
