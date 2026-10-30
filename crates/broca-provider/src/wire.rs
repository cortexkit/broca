//! The wire-family contract — the BEHAVIOR half of the provider framework.
//!
//! A `WireFamily` owns one API grammar: it renders a canonical request to the
//! exact wire BYTES that grammar sends, and normalizes that grammar's response
//! stream back to canonical events. There are only ~5 of these (most providers are
//! pure config over one of them).
//!
//! The load-bearing rule is the `render` TYPE BOUNDARY: `render` is handed ONLY a
//! `&FrozenRenderConfig` (a bounded, typed, frozen-at-run-start bundle) and the
//! `&CallOptions` for this step. It cannot see the `ProviderSpec`/`ModelSpec`/`raw`
//! catalog data, nor auth, nor any ambient state (clock/env/RNG) — so the rendered
//! bytes are a PURE function of those two inputs. That's what makes the bytes
//! reproducible on resume: freeze the `FrozenRenderConfig` once, and every later
//! re-render of the same step produces identical bytes.
//! The explicit media hooks additionally accept verified per-request payloads. They still
//! perform no storage I/O and observe no ambient state: preparation supplies the same bytes
//! from the session's captured objects on retry and resume. An absent prepared bundle uses
//! the original two-input renderer unchanged.
//!
//! This module is the PURE contract — traits + the frozen-config type — with no
//! transport. The adapter that implements the loop's `Provider` trait by driving a
//! `WireFamily` over real HTTP/SSE lives in the transport driver (the `live`
//! feature), since `Provider::step` needs a network leg this pure layer doesn't
//! carry.

use broca_protocol::{CallOptions, NormalizedEvent, StepId};

/// Re-exported so the frozen cache policy and its caller-declared tail can be named from one
/// module, the way `CacheTtl` and `MarkerSyntax` already are.
pub use broca_protocol::request::CacheTail;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::policy::{
    ReasoningPolicy, SystemPromptPolicy, TokenLimitField, ToolIdPolicy, UsagePolicy,
};
use crate::spec::WireFamilyId;

/// Prompt-cache policy resolved before freeze and carried inside the render config.
/// Renderers read only this frozen value, so resume replays place the same provider
/// cache hints and breakpoints byte-for-byte.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CachePolicy {
    pub version: u32,
    #[serde(flatten)]
    pub class: CacheClass,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "class", rename_all = "snake_case")]
pub enum CacheClass {
    Markers {
        syntax: MarkerSyntax,
        ttl: CacheTtl,
        max_breakpoints: u32,
        lookback_blocks: u32,
        /// The caller's frozen declaration about its final message. Resolved at admission
        /// like `ttl`, so a resume replays the same placement instead of re-deciding it.
        /// Absent is the pre-field shape and plans markers exactly as before.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tail: Option<CacheTail>,
        /// Render each marker without a `ttl` field, which leaves its lifetime to the
        /// provider's own default (Anthropic's is 5m, the value `ttl` then records for
        /// scheduling). Only broca's no-request default sets this; a caller's explicit request
        /// keeps `ttl` on the wire, so its bytes are the same as before the default existed.
        /// Absent is the pre-field shape and means `false`.
        #[serde(default, skip_serializing_if = "is_false")]
        omit_ttl: bool,
    },
    Hints {
        cache_key: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        retention: Option<String>,
    },
    None,
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarkerSyntax {
    AnthropicCacheControl,
    BedrockCachePoint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CacheTtl {
    #[serde(rename = "5m")]
    FiveMinutes,
    #[serde(rename = "1h")]
    OneHour,
}

impl CacheTtl {
    pub fn as_str(self) -> &'static str {
        match self {
            CacheTtl::FiveMinutes => "5m",
            CacheTtl::OneHour => "1h",
        }
    }
}

/// The bounded, typed render inputs frozen at run start — the SOLE input to
/// `render`/`normalizer` besides `CallOptions`. Everything here is resolved from a
/// `ProviderSpec`/`ModelSpec` once (so the catalog can update freely without
/// changing an in-flight run's rendered bytes), and nothing unbounded (like the
/// catalog `raw` passthrough) is included — keeping the frozen bundle both bounded
/// and complete.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FrozenRenderConfig {
    pub family_id: WireFamilyId,
    /// The family render ABI version. A change means a resume can't reproduce the
    /// original bytes, so resume must stop for review rather than re-render.
    pub renderer_version: u32,
    /// The wire model id (request `model` field, or endpoint path for path-labeled APIs).
    pub wire_model_id: String,
    /// The API base URL, when the provider isn't at the family's default host. This
    /// is what makes the OpenAI-compatible long tail config-only: every
    /// OpenAI-compatible provider is the OpenAI-Chat family pointed at a different
    /// `base_url` (e.g. `https://api.deepseek.com/v1`). `None` = the family default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    /// Max output tokens to request.
    pub max_output_tokens: u32,
    /// The caller's explicit output-token cap before provider defaults are applied. `None`
    /// means the caller omitted the field, allowing a family-specific default. Keeping this
    /// distinction in the frozen bundle lets resumed renders make the same default-vs-clamp
    /// decision without guessing from a concrete fallback value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caller_max_output_tokens: Option<u32>,
    /// The resolved reasoning policy (how thinking is requested + echoed).
    pub reasoning: ReasoningPolicy,
    /// How tool-call ids round-trip.
    pub tool_id: ToolIdPolicy,
    /// Where the system prompt goes (+ any required identity lead).
    pub system_prompt: SystemPromptPolicy,
    /// How usage is read off the response (used by the normalizer).
    pub usage: UsagePolicy,
    /// Resolved prompt-cache behavior. `None` is omitted from frozen blobs so runs
    /// without cache policy keep the exact pre-cache serialized config shape.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache: Option<CachePolicy>,
    /// Which request field carries the output-token limit.
    #[serde(default)]
    pub token_limit_field: TokenLimitField,
    /// Body options merged into every request (e.g. provider-required flags).
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub static_options: Value,
    /// AWS region for SigV4/region-scoped endpoints (e.g. Bedrock Runtime).
    /// This is auth KIND metadata only: never credentials, timestamps, or signatures.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aws_region: Option<String>,
    /// The cloud project the request is bound to — a BODY-VISIBLE render input for the
    /// Google Antigravity / Code-Assist family (the envelope's `project` field). Frozen at
    /// run start (resolved from the vault alongside the token) so a resume renders the same
    /// project. `None` for every other family. Unlike the token (off-hash, attached at the
    /// endpoint), this IS in the rendered bytes, so it must be frozen, not re-resolved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    /// Code Assist envelope identity, frozen at admission. Absent on older episodes: they
    /// retain their original requestId and omit session identity on resume.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codeassist_identity: Option<CodeAssistIdentity>,
    /// A caller-selected prompt-cache scope, emitted by OpenAI renderers. ChatGPT codex uses
    /// the run session id only when this caller scope is absent. A per-run constant, it is frozen
    /// here (reproduced from the blob on resume) rather than carried per-step like `step_context`.
    /// Other families retain it without emitting it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_cache_key: Option<String>,
    /// A reasoning effort for the ChatGPT codex backend. The caller-facing `-pro` suffix
    /// resolves to the base wire model plus `effort="max"` (the native codex CLI's pro
    /// setting, wire-captured), so it must be frozen for byte-identical resume. `None`
    /// keeps every non-pro render unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    /// The body `service_tier` value, sent by the OpenAI Responses, ChatGPT codex, OpenAI Chat
    /// and Gemini renderers when set. A caller's `service_tier` sets it where its provider family
    /// maps the tier to this field (see `crate::service_tier`). Freezing it keeps resumed request
    /// bytes identical.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<String>,
    /// Anthropic's body `speed` value (`"fast"` for fast mode, which is how a caller's
    /// `priority` tier renders there); when set, the fast-mode beta header is sent too. `None`
    /// is omitted, keeping every other render byte-identical.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub speed: Option<String>,
    /// The caller's service tier and the provider it was admitted against. Never rendered: the
    /// bytes are in `service_tier` / `speed`. Kept so a provider's refusal can be recognised
    /// and reported as a refusal of this tier. `None` (no tier requested) is omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_tier_request: Option<crate::service_tier::ServiceTierRequest>,
    /// Legacy DeepSeek flag, retained only to reproduce pre-policy frozen blobs on resume.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub reasoning_content_required: bool,
    /// Which reasoning field and representation to echo on which assistant turns. Frozen at
    /// run start rather than re-derived from today's catalog when an episode resumes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_echo: Option<crate::policy::ReasoningEchoPolicy>,
    /// Whether the provider requires a string `content` (not JSON null) on tool-call-only
    /// assistant turns. When set, OpenAI-Chat render uses `""` instead of `null`. Frozen
    /// from the provider's quirks at run start. DEFAULT FALSE — no provider is opted in.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub tool_call_content_empty_string: bool,
    /// The caller's reasoning variant, resolved at admission from the model's catalog reasoning
    /// options into the wire parameters its family sends (see `crate::variant`). Frozen so a
    /// resume renders the same parameters even if the catalog has changed since. `None` (no
    /// variant) is omitted, keeping every run without one byte-identical to before the field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<crate::variant::FrozenVariant>,
    /// Set this flag to render Responses function tools with `strict: false`, keeping optional
    /// parameters optional. The value is frozen at admission; older blobs lack the field, so
    /// resumed runs omit `strict` to reproduce their original bytes.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub responses_tools_non_strict: bool,
    /// Whether Codex may use input configuration updates for effort changes, recorded when
    /// the episode starts. Old frozen blobs lack this field and keep request-level effort.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub chatgpt_effort_updates: bool,
    /// Released Codex policy and identity. Absence retains legacy request bytes on resume.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chatgpt_wire: Option<crate::chatgpt_wire::ChatgptWire>,
    /// Preserve and echo Responses message phases and Gemini text-part thought signatures.
    /// Older frozen configs omit this flag and continue discarding those fields on resume.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub provider_text_state: bool,
    /// Decode and replay Bedrock Converse reasoning blocks. Older frozen configs omit this
    /// switch and retain their original reasoning-free requests and normalized content.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub bedrock_reasoning: bool,
    /// The run's structured-output settings, decided at admission (see
    /// `crate::structured_output::decide`) and rendered into every request of the run: the
    /// submit tool, the family's schema encoding, or schema instructions. `None` (a run
    /// without structured output) is omitted, keeping every such render byte-identical.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub structured_output: Option<broca_protocol::structured_output::FrozenStructuredOutput>,
    /// The Claude Code persona an Anthropic OAuth (identity-lead) run speaks as: the billing
    /// block, `metadata.user_id`, the persona key order and `cch` signature in the body, and the
    /// Claude Code headers and betas (see `crate::claude_code_persona`). Frozen at admission so
    /// every step and every resume sends the same identity. `None` is the shape OAuth runs had
    /// before the persona existed (identity block only, canonical key order); a run recorded
    /// that way keeps rendering it, and blobs recorded without the field read back as `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claude_code_persona: Option<ClaudeCodePersona>,
    /// Whether this episode's Anthropic requests ask the runtime to hand a content-filter
    /// refusal to another model itself: the body's `fallbacks: "default"` and the
    /// `server-side-fallback` beta that follows from it. Set at admission only for an OAuth
    /// (Claude-subscription) identity-lead run whose catalog row states the capability. A
    /// resumed run keeps its recorded value. Omitted when false, so every config recorded
    /// before this field rendered (and keeps rendering) exactly the bytes it did.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub server_side_fallback: bool,
    /// OpenCode Go's transport-only session header. It is re-derived from the session reference
    /// after restart and is not part of the frozen request-body identity.
    #[serde(skip)]
    pub opencode_go_session_id: Option<String>,
    /// Whether OpenAI Chat replay should use OpenCode Go's reasoning signature. Older frozen
    /// configs omit this flag and retain their prior body.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub opencode_go_reasoning_signature: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub opencode_go_qwen_thinking: bool,
    /// Whether this Go model uses the DeepSeek-style `thinking` field. Older frozen configs omit
    /// this flag and retain their prior body.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub opencode_go_deepseek_thinking: bool,
    /// Whether Go's Kimi K2.6 route rejects 24-hour OpenAI prompt-cache hints.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub opencode_go_long_cache_retention_unsupported: bool,
    /// The media profile frozen at admission (see `crate::media_profile::freeze_media_profile`)
    /// and refrozen when a model switch refreezes the run config. It gates media rendering: a
    /// config without one renders exactly as before media existed. Optional and omitted when
    /// absent, so it is not a must-understand field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media: Option<crate::media_profile::MediaProfile>,
    /// The model a step the source model refuses is re-sent to, frozen at admission with its own
    /// complete render config. Present only on an Anthropic OAuth episode that was eligible
    /// for the client-side refusal fallback when it was admitted; absent on every other
    /// config, which then serializes exactly as before the field existed.
    ///
    /// It lives inside the source config so it is restored with the recorded bundle on resume
    /// and covered by the frozen-config drift check: an older build that drops the unknown
    /// field re-serializes a different blob and refuses the resume instead of losing the
    /// fallback. The fallback config itself never carries one, so a fallback cannot chain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refusal_fallback: Option<Box<RefusalFallbackConfig>>,
}

/// The client-side refusal fallback an episode froze: which model a refused step goes to, that
/// model's context window, and the render config its requests are rendered with.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RefusalFallbackConfig {
    /// The fallback model's catalog handle, recorded on the steps it answers.
    pub model: broca_protocol::request::ModelHandle,
    /// The fallback model's catalog context window, when known. A step whose prompt already
    /// fills it is not sent there.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_limit: Option<u64>,
    /// The fallback model's own render config: the same account, persona, cache policy and
    /// structured output as the source, with the fallback's wire model id and its own output
    /// cap.
    pub frozen: FrozenRenderConfig,
}

impl FrozenRenderConfig {
    /// The config one step renders with: this config, with `max_output_tokens` lowered to the
    /// step's own cap (`StepContext::max_output_tokens`) when the run loop lowered it to keep the
    /// request inside the context limit. Every renderer reads the cap from the config, so this is
    /// the one place a step's cap enters the bytes. A step without a lower cap borrows the config
    /// unchanged and renders exactly as before.
    pub fn for_step(&self, opts: &CallOptions) -> std::borrow::Cow<'_, Self> {
        match opts
            .step_context
            .as_ref()
            .and_then(|step| step.max_output_tokens)
        {
            Some(cap) if cap < self.max_output_tokens => {
                let mut lowered = self.clone();
                lowered.max_output_tokens = cap;
                std::borrow::Cow::Owned(lowered)
            }
            _ => std::borrow::Cow::Borrowed(self),
        }
    }

    /// The output cap this config's requests carry and the smallest cap a step may lower it to,
    /// or `None` when the rendered body does not carry `max_output_tokens` as its cap, so lowering
    /// it would change nothing:
    ///
    /// - the ChatGPT codex backend omits the field (the backend rejects it);
    /// - an Antigravity tier model renders its tier ceiling unless the caller set a cap and the
    ///   renderer is version 7 or later;
    /// - Bedrock before renderer version 3 renders the caller's cap without bounding it.
    ///
    /// Anthropic requires `max_tokens` above the thinking budget, so with thinking enabled the
    /// floor is one token above the budget the request sends.
    ///
    /// Only builds with broca-core linked (the `live` provider, or this crate's tests) have the
    /// type it returns.
    #[cfg(any(feature = "live", test))]
    pub fn output_limits(&self) -> Option<broca_core::output_reservation::OutputLimits> {
        let renders_cap = match self.family_id {
            WireFamilyId::OpenAiResponsesChatgpt => false,
            WireFamilyId::GoogleCodeAssist => {
                crate::families::google_codeassist::renders_frozen_output_cap(self)
            }
            WireFamilyId::BedrockConverse => {
                self.renderer_version >= 3 || self.caller_max_output_tokens.is_none()
            }
            WireFamilyId::OpenAiChat
            | WireFamilyId::OpenAiResponses
            | WireFamilyId::AnthropicMessages
            | WireFamilyId::Gemini => true,
        };
        if !renders_cap {
            return None;
        }
        let variant_budget = match &self.variant {
            Some(crate::variant::FrozenVariant {
                request: crate::variant::VariantRequest::Anthropic { thinking, .. },
                ..
            }) => Some(
                thinking
                    .as_ref()
                    .and_then(|thinking| thinking.get("budget_tokens"))
                    .and_then(Value::as_u64)
                    .and_then(|budget| u32::try_from(budget).ok())
                    .unwrap_or(0),
            ),
            _ => None,
        };
        let thinking_budget = variant_budget.unwrap_or(match self.reasoning {
            ReasoningPolicy::AnthropicInterleaved { budget_tokens, .. } => budget_tokens,
            _ => 0,
        });
        let required = if thinking_budget > 0 {
            thinking_budget.saturating_add(1)
        } else {
            0
        };
        Some(broca_core::output_reservation::OutputLimits::new(
            self.max_output_tokens,
            required,
        ))
    }
}

/// One entry of an Anthropic response's `usage.iterations`: the part of the response one model
/// produced. When the runtime hands a refusal to another model itself, the response carries one
/// entry per model it ran, each with its own token counts.
pub use broca_protocol::usage::UsageIteration;

/// Conversation identity and episode-local request numbering for the Antigravity envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeAssistIdentity {
    pub session_id: String,
    pub agent_id: String,
    pub trajectory_id: String,
    /// WAL sequence at admission plus one. Together with the run-local step index this
    /// gives increasing session-wide request steps, with gaps for non-request WAL records.
    pub step_offset: u64,
    /// Admission time, not render time: retries and resume must reproduce the same bytes.
    pub admitted_at_ms: u64,
}

impl CodeAssistIdentity {
    pub fn for_session(session_ref: &str, head_seq: u64, admitted_at_ms: u64) -> Self {
        use sha2::{Digest, Sha256};
        let digest = |domain: &str| {
            Sha256::digest(format!("broca-codeassist-{domain}-v1\0{session_ref}").as_bytes())
        };
        let session = digest("session");
        let mut number = [0u8; 8];
        number.copy_from_slice(&session[..8]);
        let uuid = |domain| {
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(&digest(domain)[..16]);
            uuid_v4_text(bytes)
        };
        Self {
            session_id: format!("-{}", u64::from_be_bytes(number) & i64::MAX as u64),
            agent_id: uuid("agent"),
            trajectory_id: uuid("trajectory"),
            step_offset: head_seq.saturating_add(1),
            admitted_at_ms,
        }
    }
}

/// The frozen identity of a Claude Code persona run. Every value here is either in the request
/// body or must agree with it, so it is fixed at admission rather than re-derived per step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaudeCodePersona {
    /// The Claude Code version spoken: `cc_version` in the billing block and the version in
    /// the user-agent. Frozen so a later version bump leaves admitted runs' bytes alone.
    pub version: String,
    /// `metadata.user_id.device_id`: 64 lowercase hex digits, stable per account and machine.
    pub device_id: String,
    /// `metadata.user_id.account_uuid`, the Anthropic account the credential belongs to. Absent
    /// when unknown, and then omitted from `user_id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_uuid: Option<String>,
    /// `metadata.user_id.session_id` and the `x-claude-code-session-id` header: one UUID per
    /// broca session.
    pub session_id: String,
    /// `cc_prompt_id` in the billing block: one UUID per episode, shared by all its steps.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_id: Option<String>,
    /// The 3-hex `cc_version` suffix, pinned for the whole session: computed once from the
    /// session's first durable user message and inherited by its later episodes, so a
    /// compaction that rewrites that message does not change the billing text. Absent (a
    /// persona recorded before the suffix was pinned) means the renderer computes it from the
    /// first user message of the request it renders, as such an episode always did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cc_version_suffix: Option<String>,
    /// One-based human-prompt ordinal from durable session history, frozen for this episode.
    /// Tool-result follow-ups and steers share it. Absent on personas recorded before 2.1.302.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_ordinal: Option<u64>,
}

impl ClaudeCodePersona {
    /// The persona version new runs are admitted with.
    pub const CURRENT_VERSION: &'static str = crate::claude_code_persona::CLAUDE_CODE_VERSION;

    /// A device id stable per account and per broca installation: SHA-256 over a domain string,
    /// the installation's persisted id and the account, as 64 lowercase hex digits. Deriving it
    /// (rather than drawing one at random per process) keeps a restart from looking like a new
    /// device.
    pub fn derive_device_id(install_id: &str, account: &str) -> String {
        use sha2::{Digest, Sha256};
        let digest = Sha256::digest(
            format!("broca-claude-code-device-id-v1\0{install_id}\0{account}").as_bytes(),
        );
        digest.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    /// A session id for a broca session, derived from its session reference so every episode
    /// of the session, before and after a restart, sends the same id. UUID-shaped (version 4,
    /// RFC variant) because the reference client sends a random UUID there.
    pub fn derive_session_id(session_ref: &str) -> String {
        use sha2::{Digest, Sha256};
        let digest =
            Sha256::digest(format!("broca-claude-code-session-id-v1\0{session_ref}").as_bytes());
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&digest[..16]);
        uuid_v4_text(bytes)
    }
}

/// Format 16 bytes as a version-4, RFC-variant UUID (the version and variant bits are set).
pub fn uuid_v4_text(mut bytes: [u8; 16]) -> String {
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

/// One HTTP request the family wants sent: method, full URL, headers, body bytes.
/// Auth is attached when this is built (in `endpoint`), AFTER the body is
/// rendered, so it never enters the rendered/hashed bytes.
///
/// Header values and body bytes can contain credentials or prompts, so `Debug` is implemented
/// manually to redact them. Do not derive `Debug` for this type.
#[derive(Clone)]
pub struct HttpRequest {
    pub method: HttpMethod,
    pub url: String,
    /// Header name/value pairs in a fixed order.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl std::fmt::Debug for HttpRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let headers: Vec<(&str, &str)> = self
            .headers
            .iter()
            .map(|(name, _)| (name.as_str(), "<redacted>"))
            .collect();
        let body = format!("<{} bytes>", self.body.len());
        f.debug_struct("HttpRequest")
            .field("method", &self.method)
            .field("url", &self.url)
            .field("headers", &headers)
            .field("body", &body)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpMethod {
    Post,
}

/// What a runtime auth credential supplies. Held by the adapter, attached in
/// `endpoint` — never seen by `render`.
///
/// Every credential-bearing value is redacted by the manual `Debug` implementation. Do not
/// derive `Debug` for this type. Equality compares the secrets too; use it only as a boolean
/// ("is this the token that was just rejected?"), never to print either side.
#[derive(Clone, PartialEq, Eq)]
pub enum AuthHandle {
    /// `Authorization: Bearer <token>`.
    Bearer(String),
    /// A named API-key header.
    ApiKeyHeader { header: String, value: String },
    /// AWS SigV4 request signing. The transport/auth layer computes these signed
    /// headers after render from the method, URL, headers, and body.
    SigV4 {
        region: String,
        service: String,
        signed_headers: Vec<(String, String)>,
    },
    /// A complete, pre-computed header set to attach verbatim (name/value pairs in
    /// order), including the `Authorization` header. The auth-resolution layer builds
    /// this when a credential needs more than one header derived from runtime context —
    /// e.g. the ChatGPT codex backend wants `Authorization` plus a `ChatGPT-Account-Id`
    /// decoded from the token's JWT and a per-session `session-id`. Like `SigV4`'s
    /// `signed_headers`, the derivation happens in the auth layer (off the rendered/hashed
    /// bytes); `endpoint` just attaches what it's given.
    Headers(Vec<(String, String)>),
    /// No credential.
    None,
}

impl std::fmt::Debug for AuthHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthHandle::Bearer(_) => f.debug_tuple("Bearer").field(&"<redacted>").finish(),
            AuthHandle::ApiKeyHeader { header, .. } => f
                .debug_struct("ApiKeyHeader")
                .field("header", header)
                .field("value", &"<redacted>")
                .finish(),
            AuthHandle::SigV4 {
                region, service, ..
            } => f
                .debug_struct("SigV4")
                .field("region", region)
                .field("service", service)
                .finish(),
            AuthHandle::Headers(headers) => {
                let headers: Vec<(&str, &str)> = headers
                    .iter()
                    .map(|(name, _)| (name.as_str(), "<redacted>"))
                    .collect();
                f.debug_tuple("Headers").field(&headers).finish()
            }
            AuthHandle::None => f.write_str("None"),
        }
    }
}

/// A wire grammar: canonical → bytes (render) and bytes → canonical (normalize).
pub trait WireFamily: Send + Sync {
    /// The provider-shaped request as a structured value, BEFORE byte emission — the
    /// pure policy-decision + assembly stage. A PURE function of `(frozen, opts)`
    /// (see the module docs for why that purity is load-bearing). `render` is defined
    /// as `canonical_json_bytes(shape(...))`, so this is where every family's request
    /// shaping lives; exposing it (rather than only the emitted bytes) lets the
    /// decode/MITM side and conformance tests reuse the shaped value without
    /// re-emitting bytes.
    fn shape(&self, frozen: &FrozenRenderConfig, opts: &CallOptions) -> Value;

    /// Render `opts` to the EXACT, DETERMINISTIC request body bytes this grammar
    /// sends. Defined as `emit∘shape` — `canonical_json_bytes(self.shape(frozen,
    /// opts))` (sorted keys, the deterministic JSON the cache/resume hash is taken
    /// over). A family whose wire is not canonical JSON over its shaped value MAY
    /// override this, but every current family uses the default.
    fn render(&self, frozen: &FrozenRenderConfig, opts: &CallOptions) -> Vec<u8> {
        broca_protocol::canonical_json_bytes(&self.shape(frozen, opts))
    }

    /// Low-level shaping with explicit verified payloads. `opts.prompt` is the prepared,
    /// optionally tagged prompt. Families with native media override this hook; families not
    /// yet supporting it retain their carrier markers. No payloads enter the frozen config.
    fn shape_with_media(
        &self,
        frozen: &FrozenRenderConfig,
        opts: &CallOptions,
        _media: &[crate::media_render::InlineMedia<'_>],
    ) -> Value {
        self.shape(frozen, opts)
    }

    /// Low-level emission hook, including any family-specific signing. Live callers should
    /// use `media_request::render_prepared` to enforce limits on these complete emitted bytes.
    fn render_with_media(
        &self,
        frozen: &FrozenRenderConfig,
        opts: &CallOptions,
        media: &[crate::media_render::InlineMedia<'_>],
    ) -> Vec<u8> {
        broca_protocol::canonical_json_bytes(&self.shape_with_media(frozen, opts, media))
    }

    /// The HTTP shape (method, URL, headers) for `body`. Auth attaches HERE, after
    /// render — never on the rendered/hashed bytes.
    fn endpoint(
        &self,
        frozen: &FrozenRenderConfig,
        auth: &AuthHandle,
        body: Vec<u8>,
    ) -> HttpRequest;

    /// A fresh per-stream incremental normalizer (response bytes → canonical
    /// events). The wire framing (text/SSE vs binary event-stream) is the
    /// normalizer's internal concern.
    fn normalizer(&self, frozen: &FrozenRenderConfig, step_id: StepId)
        -> Box<dyn StreamNormalizer>;

    /// The body of a cache warm for `base`, a request body this family rendered and already
    /// sent: `base` changed only so the provider generates nothing, which keeps the prompt
    /// prefix `base` cached. `Err` says why this family, or `base`'s shape, cannot be warmed;
    /// nothing is then sent. The default cannot warm.
    fn warm_body(&self, _frozen: &FrozenRenderConfig, _base: &[u8]) -> Result<Vec<u8>, String> {
        Err("this wire family has no zero-output request to warm a cache with".into())
    }
}

/// Stateful response-bytes → canonical normalization. `push` is fed raw chunks and
/// returns any newly-complete events; the wire framing is the implementor's
/// concern, so this stays grammar-generic.
pub trait StreamNormalizer: Send {
    /// Enable envelope-only response facts for a journaled attempt. Wrappers forward
    /// the shared sink; without it no additional usage objects are cloned or parsed.
    fn set_response_evidence(&mut self, _evidence: crate::normalize::SharedResponseEvidence) {}

    /// Capture provider-reported facts from an HTTP refusal, without interpreting it
    /// as a successful stream or emitting replay usage.
    fn observe_error_response(&mut self, _response: &Value) {}

    /// Socket ping/pong records have no exposed payload lengths; synthetic SSE framing is not
    /// measured data. Wrappers forward this setting without changing liveness counters.
    fn websocket_telemetry(&mut self) {}
    /// Real-content telemetry is observational; it never resets the legacy deadline markers.
    fn content_progress(&self) -> broca_protocol::provider::ContentProgress {
        Default::default()
    }
    /// Omit untrusted backend error text from diagnostics when the transport requires privacy.
    /// Wrappers forward this; it never changes model-output normalization or request bytes.
    fn private_error_diagnostics(&mut self) {}

    /// Append raw response bytes; return any newly-complete events.
    fn push(&mut self, chunk: &[u8]) -> Result<Vec<NormalizedEvent>, NormalizeError>;

    /// Monotonic count of complete data-bearing wire records decoded so far. For SSE,
    /// comment-only, blank, and empty-data records are not progress. The transport uses
    /// changes in this marker to reset its idle deadline; raw HTTP chunks alone are not
    /// progress because a peer may drip an incomplete record.
    fn progress_marker(&self) -> u64 {
        0
    }

    /// Monotonic count of complete wire records that prove the peer is ALIVE, including
    /// records that carry no content, such as SSE `: keepalive` comments. `None` (the
    /// default) means the grammar has no such records.
    ///
    /// When this returns `Some`, the transport runs two deadlines: a short liveness
    /// deadline reset by this marker (a dead or dripping connection trips it), and a long
    /// content deadline reset only by [`Self::progress_marker`] (a peer that sends
    /// keepalives forever and never any content trips it). When it returns `None`, the
    /// transport keeps its single idle deadline reset by `progress_marker`, exactly as
    /// before liveness existed. It must count complete records only, never raw bytes, for
    /// the same reason `progress_marker` does.
    fn liveness_marker(&self) -> Option<u64> {
        None
    }

    /// Last complete SSE event's name or JSON `type`, without response content.
    /// Wrappers must forward this so transport failures retain the wire discriminator.
    fn last_event_type(&self) -> Option<&str> {
        None
    }

    /// Number of SSE keepalive comments since the most recent content record.
    fn keepalives_after_content(&self) -> Option<u64> {
        None
    }

    /// Bounded per-type counts of decoded content events, without their payloads.
    fn event_type_counts(&self) -> Option<&[(String, u64)]> {
        None
    }

    /// The response stream has ENDED (the transport saw EOF). Return any final
    /// events the grammar emits only at the end. This exists because some grammars
    /// have no in-band terminal event — e.g. OpenAI's stream just stops after a
    /// `[DONE]` sentinel, and the usage chunk arrives after the finish reason — so
    /// the assembled message is emitted here. Grammars with an explicit terminal
    /// event (e.g. Anthropic's `message_stop`) emit there; an SSE grammar still uses
    /// this to flush its decoder, because a final record ending in a bare CR is only
    /// complete once the stream has ended.
    fn finish(&mut self) -> Result<Vec<NormalizedEvent>, NormalizeError> {
        Ok(Vec::new())
    }

    /// The successful response's HTTP headers, handed over once per attempt before the first
    /// body chunk. Most grammars report everything in the body and ignore this; Gemini is the
    /// exception, reporting the service tier it served in the `x-gemini-service-tier` header.
    /// A wrapping normalizer must forward this to the one it wraps.
    fn observe_response_headers(&mut self, _headers: &ResponseHeaders) {}

    /// The response's own report of how the provider altered the request, once decoded
    /// (Anthropic's `input_transformations` on `message_start`). `None` until the stream has
    /// carried it, and always `None` for grammars without one. A wrapping normalizer must
    /// forward this to the one it wraps.
    fn input_transformations(&self) -> Option<Vec<broca_protocol::InputTransformation>> {
        None
    }

    /// Whether the request this stream answers did NOT ask the provider to report input
    /// transformations, so a missing report means "not measured" rather than "none". False for
    /// grammars without the report. A wrapping normalizer must forward this too.
    fn input_transformations_unmeasured(&self) -> bool {
        false
    }
}

/// Response headers as the transport received them, reduced to what a normalizer can read:
/// lowercase names and the values that are valid UTF-8. Kept free of the HTTP client's types
/// so the pure normalize layer stays transport-free.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResponseHeaders(Vec<(String, String)>);

impl ResponseHeaders {
    /// Build from `(name, value)` pairs; names are lowercased so lookups are case-insensitive,
    /// as HTTP header names are.
    pub fn new<N: Into<String>, V: Into<String>>(pairs: impl IntoIterator<Item = (N, V)>) -> Self {
        Self(
            pairs
                .into_iter()
                .map(|(name, value)| (name.into().to_ascii_lowercase(), value.into()))
                .collect(),
        )
    }

    /// The first value of the header `name` (any case), if the response carried it.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

/// A response normalization failure.
#[derive(Debug, Clone)]
pub enum NormalizeError {
    /// The provider response was malformed or violated its wire protocol.
    Invalid(String),
    /// A response framing buffer exceeded a defensive memory limit.
    StreamLimit(String),
    /// A tool call's arguments were left incomplete because the output-token ceiling was
    /// reached, as the provider's own stop reason says. The response is well formed; the
    /// request asked for more output than its cap allows, so resending it is cut the same way.
    OutputCeiling(OutputCeilingCut),
}

/// What the provider reported about an output ceiling that cut a tool call mid-arguments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputCeilingCut {
    /// The provider's stop reason, verbatim (`max_tokens`, `length`, `MAX_TOKENS`, ...).
    pub provider_stop_reason: String,
    /// The output-token cap the request was rendered with, when the normalizer knows it.
    pub max_output_tokens: Option<u32>,
    /// The output-token count the provider reported for the response, when it did.
    pub output_tokens: Option<u64>,
    /// The tool whose arguments were cut.
    pub tool_name: String,
    /// How many argument bytes arrived before the cut.
    pub argument_bytes: usize,
}

/// The provider's statement that output stopped at the output-token ceiling. A normalizer
/// builds one only from a stop reason that names the ceiling; anything else keeps the
/// ordinary argument-parse failure.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CeilingStop<'a> {
    pub provider_stop_reason: &'a str,
    pub max_output_tokens: Option<u32>,
    pub output_tokens: Option<u64>,
}

impl NormalizeError {
    pub(crate) fn stream_limit(message: impl Into<String>) -> Self {
        Self::StreamLimit(message.into())
    }

    /// The wire error class this failure carries.
    ///
    /// An unparseable or over-limit stream is transient: the next attempt gets a fresh stream,
    /// and nothing durable recorded a partial step. An output-ceiling cut is permanent: the
    /// retry sends the same bytes with the same cap, and the provider stops it at the same
    /// place, so every further attempt is spend with no chance of success.
    pub fn class(&self) -> broca_protocol::error::ErrorClass {
        match self {
            Self::Invalid(_) | Self::StreamLimit(_) => broca_protocol::error::ErrorClass::Transient,
            Self::OutputCeiling(_) => broca_protocol::error::ErrorClass::Permanent,
        }
    }

    /// Test-only discriminator. `StreamLimit` and `Invalid` share the transient class, so
    /// only tests need to tell the limit arm apart from a malformed stream.
    #[cfg(test)]
    pub(crate) fn is_stream_limit(&self) -> bool {
        matches!(self, Self::StreamLimit(_))
    }
}

/// Decode a provider's terminal tool-argument buffer. An exactly empty buffer is the
/// wire representation used by zero-argument calls; every non-empty buffer must be JSON.
///
/// `ceiling` is the provider's statement, when it made one, that output stopped at the
/// output-token ceiling. When the arguments do not parse AND that statement is present, the
/// failure is named as the ceiling cut rather than as a malformed stream. Without it the
/// ordinary parse failure stands, so an unexplained bad buffer is never promoted to a
/// permanent error.
pub(crate) fn decode_terminal_tool_arguments(
    arguments: &str,
    tool_name: &str,
    ceiling: Option<CeilingStop<'_>>,
) -> Result<Value, NormalizeError> {
    if arguments.is_empty() {
        return Ok(Value::Object(Default::default()));
    }
    serde_json::from_str(arguments).map_err(|error| match ceiling {
        Some(stop) => NormalizeError::OutputCeiling(stop.cut(tool_name, arguments)),
        None => NormalizeError::Invalid(format!("invalid terminal tool arguments: {error}")),
    })
}

const TOOL_ARGS_WHITESPACE_LIMIT: usize = 4096;

/// Counts only the new delta's JSON whitespace suffix, never rescanning the accumulated
/// buffer. A run is parsed once, when it first exceeds 4096 bytes. Whitespace inside an
/// unterminated string cannot fire: the prefix before that run is not a complete JSON value.
#[derive(Default)]
pub(crate) struct TrailingWhitespaceDetector {
    trailing: usize,
}

impl TrailingWhitespaceDetector {
    fn observe(&mut self, arguments: &str, delta: &str) -> Option<(usize, usize)> {
        let suffix = json_whitespace_suffix(delta);
        let previous = self.trailing;
        self.trailing = if suffix == delta.len() {
            previous.saturating_add(suffix)
        } else {
            suffix
        };
        if self.trailing > TOOL_ARGS_WHITESPACE_LIMIT
            && (previous <= TOOL_ARGS_WHITESPACE_LIMIT || suffix != delta.len())
        {
            return complete_prefix(arguments, self.trailing);
        }
        None
    }
}

fn json_whitespace_suffix(arguments: &str) -> usize {
    arguments
        .bytes()
        .rev()
        .take_while(|byte| matches!(byte, b' ' | b'\t' | b'\r' | b'\n'))
        .count()
}

fn complete_prefix(arguments: &str, whitespace: usize) -> Option<(usize, usize)> {
    let json_bytes = arguments.len().checked_sub(whitespace)?;
    serde_json::from_str::<Value>(&arguments[..json_bytes]).ok()?;
    Some((json_bytes, whitespace))
}

/// Shared streaming detector and terminal backstop. The cut is a completed step with a
/// rejected call, not a provider failure; only byte counts, never arguments, leave this guard.
#[derive(Default)]
pub(crate) struct ToolArgsGuard {
    detectors: std::collections::HashMap<u64, TrailingWhitespaceDetector>,
    cut: Option<broca_protocol::provider::ToolArgsTrailingGarbage>,
}

impl ToolArgsGuard {
    pub(crate) fn stream(&mut self, key: u64, id: &str, name: &str, arguments: &str, delta: &str) {
        if self.cut.is_none() {
            let counts = self
                .detectors
                .entry(key)
                .or_default()
                .observe(arguments, delta);
            self.mark(id, name, counts);
        }
    }

    pub(crate) fn terminal(&mut self, id: &str, name: &str, arguments: &str) {
        let whitespace = json_whitespace_suffix(arguments);
        if self.cut.is_none() && whitespace > TOOL_ARGS_WHITESPACE_LIMIT {
            self.mark(id, name, complete_prefix(arguments, whitespace));
        }
    }

    pub(crate) fn replace(&mut self, key: u64, id: &str, name: &str, arguments: &str) {
        self.detectors.entry(key).or_default().trailing = json_whitespace_suffix(arguments);
        self.terminal(id, name, arguments);
    }

    fn mark(&mut self, id: &str, name: &str, counts: Option<(usize, usize)>) {
        if let Some((json_bytes, whitespace_bytes)) = counts {
            self.cut = Some(broca_protocol::provider::ToolArgsTrailingGarbage {
                tool_call_id: broca_protocol::ToolCallId(id.to_owned()),
                tool_name: name.to_owned(),
                json_bytes,
                whitespace_bytes,
                omitted_tool_calls: 0,
            });
        }
    }

    pub(crate) fn fired(&self) -> bool {
        self.cut.is_some()
    }

    pub(crate) fn omit_tool(&mut self) {
        if let Some(cut) = &mut self.cut {
            cut.omitted_tool_calls += 1;
        }
    }

    pub(crate) fn decode(
        &mut self,
        arguments: &str,
        name: &str,
        ceiling: Option<CeilingStop<'_>>,
        completed: bool,
    ) -> Result<Option<Value>, NormalizeError> {
        if self.fired() {
            // Empty buffers on a cut are still in progress, not zero-argument calls.
            if arguments.is_empty() && completed {
                return Ok(Some(Value::Object(Default::default())));
            }
            if let Ok(input) = serde_json::from_str(arguments) {
                return Ok(Some(input));
            }
            self.omit_tool();
            return Ok(None);
        }
        decode_terminal_tool_arguments(arguments, name, ceiling).map(Some)
    }

    pub(crate) fn event(&self) -> Option<NormalizedEvent> {
        self.cut.clone().map(|cut| {
            NormalizedEvent::Control(broca_protocol::ProviderControl::ToolArgsTrailingGarbage {
                cut,
            })
        })
    }
}

impl CeilingStop<'_> {
    /// The cut this stop reason explains, for one tool call's argument buffer.
    pub(crate) fn cut(&self, tool_name: &str, arguments: &str) -> OutputCeilingCut {
        OutputCeilingCut {
            provider_stop_reason: self.provider_stop_reason.to_owned(),
            max_output_tokens: self.max_output_tokens,
            output_tokens: self.output_tokens,
            tool_name: tool_name.to_owned(),
            argument_bytes: arguments.len(),
        }
    }
}

impl std::fmt::Display for OutputCeilingCut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "output ceiling reached mid tool call: finish=length, max_output_tokens="
        )?;
        match self.max_output_tokens {
            Some(cap) => write!(f, "{cap}")?,
            None => f.write_str("unknown")?,
        }
        write!(
            f,
            " (tool {}, {} argument bytes received; provider stop reason {}",
            self.tool_name, self.argument_bytes, self.provider_stop_reason
        )?;
        if let Some(output_tokens) = self.output_tokens {
            write!(f, ", output_tokens={output_tokens}")?;
        }
        f.write_str(")")
    }
}

impl std::fmt::Display for NormalizeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(message) => write!(f, "normalize: {message}"),
            Self::StreamLimit(message) => write!(f, "stream limit: {message}"),
            Self::OutputCeiling(cut) => write!(f, "{cut}"),
        }
    }
}

impl std::error::Error for NormalizeError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_handle_debug_redacts_every_credential_value() {
        let secret = "auth-secret-literal";
        let handles = [
            AuthHandle::Bearer(secret.to_string()),
            AuthHandle::ApiKeyHeader {
                header: "x-api-key".to_string(),
                value: secret.to_string(),
            },
            AuthHandle::SigV4 {
                region: "us-east-1".to_string(),
                service: "bedrock".to_string(),
                signed_headers: vec![("authorization".to_string(), secret.to_string())],
            },
            AuthHandle::Headers(vec![("authorization".to_string(), secret.to_string())]),
            AuthHandle::None,
        ];

        for handle in handles {
            let debug = format!("{handle:?}");
            assert!(!debug.contains(secret));
        }
    }

    #[test]
    fn http_request_debug_redacts_headers_and_body() {
        let secret = "request-secret-literal";
        let request = HttpRequest {
            method: HttpMethod::Post,
            url: "https://provider.example/v1/messages".to_string(),
            headers: vec![("authorization".to_string(), secret.to_string())],
            body: secret.as_bytes().to_vec(),
        };

        let debug = format!("{request:?}");
        assert!(!debug.contains(secret));
        assert!(debug.contains("authorization"));
        assert!(debug.contains("<22 bytes>"));
    }

    #[test]
    fn cache_policy_wire_shape_uses_top_level_class_tag() {
        let policy = CachePolicy {
            version: 1,
            class: CacheClass::Markers {
                syntax: MarkerSyntax::AnthropicCacheControl,
                ttl: CacheTtl::OneHour,
                max_breakpoints: 4,
                lookback_blocks: 20,
                tail: None,
                omit_ttl: false,
            },
        };
        let value = serde_json::to_value(&policy).expect("serialize policy");
        assert_eq!(
            value,
            serde_json::json!({
                "version": 1,
                "class": "markers",
                "syntax": "anthropic_cache_control",
                "ttl": "1h",
                "max_breakpoints": 4,
                "lookback_blocks": 20,
            })
        );
        let restored: CachePolicy = serde_json::from_value(value).expect("deserialize policy");
        assert_eq!(restored, policy);
    }

    #[test]
    fn cache_policy_is_serde_additive() {
        let frozen = crate::families::anthropic_frozen_config("claude-opus-4-8");
        assert_eq!(frozen.cache, None);

        let serialized = serde_json::to_string(&frozen).expect("serialize frozen config");
        let value: serde_json::Value = serde_json::from_str(&serialized).expect("json value");
        assert!(
            value.get("cache").is_none(),
            "cache=None must serialize to the old frozen-config shape"
        );

        let restored: FrozenRenderConfig =
            serde_json::from_str(&serialized).expect("old-shaped frozen config deserializes");
        assert_eq!(restored.cache, None);
    }
}

#[cfg(test)]
mod output_cap_tests {
    use super::*;
    use crate::families::frozen_config_for_family;
    use broca_protocol::{ContentBlock, Message, Role, StepContext};

    fn opts(cap: Option<u32>) -> CallOptions {
        let mut opts = CallOptions::new(vec![Message::new(
            Role::User,
            vec![ContentBlock::Text { text: "hi".into() }],
        )]);
        opts.step_context = Some(StepContext {
            run_id: "r".into(),
            step_index: 1,
            session_address: None,
            credential_id: None,
            account_ref: None,
            max_output_tokens: cap,
        });
        opts
    }

    /// The families whose body carries the frozen cap: a step without a lower cap renders the
    /// frozen config byte for byte, and a step with one renders that cap in place of it.
    #[test]
    fn a_step_cap_replaces_the_frozen_cap_and_its_absence_changes_nothing() {
        for family_id in [
            WireFamilyId::AnthropicMessages,
            WireFamilyId::OpenAiResponses,
            WireFamilyId::OpenAiChat,
            WireFamilyId::Gemini,
            WireFamilyId::BedrockConverse,
        ] {
            let frozen = frozen_config_for_family(family_id, "m");
            let family = crate::registry::wire_family(family_id).unwrap();
            let unchanged = opts(None);
            assert!(matches!(
                frozen.for_step(&unchanged),
                std::borrow::Cow::Borrowed(_)
            ));
            assert_eq!(
                family.render(&frozen.for_step(&unchanged), &unchanged),
                family.render(&frozen, &unchanged),
                "{family_id:?}"
            );
            // A cap at or above the frozen one is not a lowering.
            let higher = opts(Some(frozen.max_output_tokens + 1));
            assert!(matches!(
                frozen.for_step(&higher),
                std::borrow::Cow::Borrowed(_)
            ));

            let cap = frozen.max_output_tokens - 7;
            let lowered = opts(Some(cap));
            let body =
                String::from_utf8(family.render(&frozen.for_step(&lowered), &lowered)).unwrap();
            assert!(body.contains(&format!(":{cap}")), "{family_id:?}: {body}");
            assert!(
                !body.contains(&format!(":{}", frozen.max_output_tokens)),
                "{family_id:?}: {body}"
            );
        }
    }

    #[test]
    fn output_limits_follow_what_each_family_renders() {
        // The ChatGPT codex backend omits the field, so there is nothing to lower.
        assert_eq!(
            frozen_config_for_family(WireFamilyId::OpenAiResponsesChatgpt, "m").output_limits(),
            None
        );
        // An Antigravity tier model without a caller cap renders its tier ceiling.
        let tier = frozen_config_for_family(WireFamilyId::GoogleCodeAssist, "gemini-3-flash-agent");
        assert_eq!(tier.output_limits(), None);
        let mut capped = tier.clone();
        capped.caller_max_output_tokens = Some(10_000);
        assert!(capped.output_limits().is_some());

        let responses = frozen_config_for_family(WireFamilyId::OpenAiResponses, "m");
        let limits = responses.output_limits().unwrap();
        assert_eq!(limits.max, responses.max_output_tokens);
        assert_eq!(
            limits.floor,
            broca_core::output_reservation::MIN_USEFUL_OUTPUT_TOKENS.min(limits.max)
        );

        // Anthropic needs `max_tokens` above the thinking budget.
        let mut anthropic = frozen_config_for_family(WireFamilyId::AnthropicMessages, "m");
        anthropic.max_output_tokens = 64_000;
        anthropic.reasoning = ReasoningPolicy::AnthropicInterleaved {
            budget_tokens: 31_999,
            last_turn_only: true,
            echo_scope: crate::policy::ThinkingEchoScope::AllAssistantMessages,
            cross_model: crate::policy::CrossModelThinking::AnyAnthropicModel,
        };
        assert_eq!(anthropic.output_limits().unwrap().floor, 32_000);
    }
}
