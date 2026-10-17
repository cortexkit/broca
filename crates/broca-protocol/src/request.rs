//! Request-side canonical types: the `CallOptions` a provider module receives,
//! plus `stop_when` loop control. Mirrors the Vercel `LanguageModelV4`
//! CallOptions, adapted to subc-routed tools.

use std::sync::Arc;

/// Session classes are opaque labels, not paths or provider identities.
pub fn validate_session_class(value: &str) -> Result<(), &'static str> {
    if (1..=32).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
    {
        Ok(())
    } else {
        Err("session_class must contain 1-32 lowercase ASCII letters, digits, '_' or '-'")
    }
}

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::content::Message;

/// A concrete model handle — the runner is GIVEN this (model selection is the
/// caller's job). `{provider_module_id, model_id}` identifies which provider
/// module + model a run is bound to; recorded in the WAL `run_config` so a
/// model/provider change between a run and its resume is detected as a mismatch.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ModelHandle {
    pub provider_module_id: String,
    pub model_id: String,
}

impl ModelHandle {
    pub fn new(provider_module_id: impl Into<String>, model_id: impl Into<String>) -> Self {
        Self {
            provider_module_id: provider_module_id.into(),
            model_id: model_id.into(),
        }
    }
}

/// How a tool's calls may be scheduled within a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    /// May run in parallel with other parallel tools (results still source-ordered).
    #[default]
    Parallel,
    /// Forces serial execution relative to the rest of the batch.
    Sequential,
}

/// A tool offered to the model. `execute` is dispatch-to-subc (the runner is the
/// executor); the provider only sees the declaration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Tool {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub module: Option<String>,
    #[serde(default)]
    pub execution_mode: ExecutionMode,
}

/// What the model is allowed/forced to do with tools this step.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ToolChoice {
    #[default]
    Auto,
    None,
    Required,
    Tool {
        name: String,
    },
}

/// Output shape requested of the model.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponseFormat {
    #[default]
    Text,
    Json {
        name: String,
        schema: Value,
    },
}

/// Generation controls (provider clamps to its own limits).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct GenerationConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stop_sequences: Vec<String>,
}

/// The caller's session role for admission-time prompt-cache policy resolution.
/// The default role is applied by the resolver, not serde, so an absent `role`
/// can still be distinguished from an absent cache request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheRole {
    Primary,
    Ephemeral,
}

/// The caller's assertion about whether the FINAL message of its prompt repeats.
///
/// This is a declaration, not something broca can derive. Two prompts can be identical in
/// shape -- one user message on a run with no tools -- and want opposite marker placement.
/// A fixed evidence pack re-sent to a fresh session wants its final message anchored, because
/// that message IS the repeating prefix. A stable header followed by a per-round tail does
/// not, because the tail is different by construction next round and an anchor there buys a
/// cache write no later request can read. Only the caller knows which of the two it is sending.
///
/// One variant on purpose. `Volatile` asserts a fact the caller has and broca does not; there
/// is deliberately no `Stable` counterpart, because nothing would consume it and offering it
/// would imply broca acts on a claim it cannot check. An absent value is therefore "the caller
/// made no assertion", which is NOT the same as "the caller says the tail is stable".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheTail {
    /// The final message differs on the next request; do not spend a breakpoint on it.
    Volatile,
}

/// The caller's explicit choice to switch provider prompt caching off for a send.
///
/// Some providers get cache breakpoints by default when a send says nothing about caching
/// (first-party Anthropic does). This is how a caller declines them. One variant on purpose:
/// "on" is what an absent value already means wherever a default applies, and a caller who
/// wants caching with particular settings says so with `role` or `ttl`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheMode {
    /// Place no cache breakpoints and send no cache hints, whatever the provider's default.
    None,
}

/// Admission-only credential selection supplied by the caller. It remains a string until
/// admission so an unknown method can be reported in the caller-facing error taxonomy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthRequest {
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
}

/// Admission-only prompt-cache preferences supplied by the caller. Renderers never
/// read this request field directly; the module resolves it into the frozen render
/// config before the run starts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<CacheRole>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl: Option<String>,
    /// An opaque caller-selected provider-cache scope, resolved and frozen at admission.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// Whether the caller declares its final message varies between requests. Absent means
    /// no assertion was made and marker placement is unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tail: Option<CacheTail>,
    /// `none` switches prompt caching off for this send. Absent means no such request, which
    /// leaves the provider's default in place and serializes to no bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<CacheMode>,
}

/// The run identity for ONE step — the run this step belongs to, and which step it is.
/// Both values are DETERMINISTIC across a resume (the run id is fixed for the run; the step
/// index is reconstructed as `completed_steps + 1` from the WAL), so a render that derives
/// body bytes from them reproduces identical bytes live-vs-resume. Carried on `CallOptions`
/// (rebuilt per step from run state) rather than the frozen render config, so run identity
/// never enters the config-drift hash — the frozen config stays "how to render", not "which
/// run". Only a family whose wire body embeds a per-request correlation id (e.g. Antigravity's
/// `requestId`) reads this; every other family ignores it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepContext {
    /// The run this step belongs to.
    pub run_id: String,
    /// The 1-based step index within the run.
    pub step_index: u64,
}

fn arc_value_is_null(value: &Arc<Value>) -> bool {
    value.is_null()
}

/// An episode's durable render inputs and its position in the lineage prompt.
#[derive(Debug, Clone, PartialEq)]
pub struct EpisodeRenderInput {
    pub prompt_start: usize,
    /// First user message in the admitted input, not a later steer or model step.
    pub first_user: Option<usize>,
    pub frozen_config: Value,
}

/// Metadata used only when rendering a request. Keeping it behind the existing
/// shared pointer avoids increasing the stack space needed by suspended agent runs.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RenderHistory {
    pub episodes: Vec<EpisodeRenderInput>,
    /// Number of canonical messages present when the compaction view was applied.
    /// Raw messages with smaller ordinals were produced before that view. Absent
    /// means no compaction view; replacement messages have no ordinal.
    pub compaction_prefix_len: Option<u64>,
}

impl From<Vec<EpisodeRenderInput>> for RenderHistory {
    fn from(episodes: Vec<EpisodeRenderInput>) -> Self {
        Self {
            episodes,
            compaction_prefix_len: None,
        }
    }
}

impl std::ops::Deref for RenderHistory {
    type Target = Vec<EpisodeRenderInput>;

    fn deref(&self) -> &Self::Target {
        &self.episodes
    }
}

impl std::ops::DerefMut for RenderHistory {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.episodes
    }
}

/// A cache marker declaration addressed by position in the native rendered prompt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeclaredMessageMarker {
    /// Index in CallOptions.prompt; renderers remap after family filtering and merging.
    pub message_index: usize,
    pub rank: u32,
}

/// The normalized, provider-facing request for ONE step.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CallOptions {
    /// This call belongs to a retention-bearing session; content-derived diagnostics must not persist.
    #[serde(skip)]
    pub ephemeral_session: bool,
    pub prompt: Vec<Message>,
    #[serde(default)]
    pub tools: Arc<Vec<Tool>>,
    #[serde(default)]
    pub tool_choice: Arc<ToolChoice>,
    #[serde(default)]
    pub response_format: Arc<ResponseFormat>,
    #[serde(default)]
    pub generation: Arc<GenerationConfig>,
    /// Opaque provider-specific options (passed through).
    #[serde(default, skip_serializing_if = "arc_value_is_null")]
    pub provider_options: Arc<Value>,
    /// The run/step identity for this step. The main execution loop always populates it (it
    /// has the run id and step id available at the call). It is left `None` only in isolated
    /// render tests and golden files that don't exercise run identity. A wire family that
    /// requires the identity (one whose body embeds a per-request id) treats `None` as a hard
    /// error rather than a silent default, so a missing populate can't quietly produce
    /// non-deterministic bytes.
    ///
    /// NOT request CONTENT — it's render INPUT (run identity passed to render), consumed
    /// DIRECTLY off this struct by the one family that embeds it. So it is `serde(skip)`:
    /// it must never appear in the GENERIC canonical render (which serializes `CallOptions`
    /// whole), else run identity would leak into every family's bytes and a queued-then-
    /// drained run (different run id) would render differently from an idle send. It is never
    /// deserialized either — the loop rebuilds it fresh each step.
    #[serde(skip)]
    pub step_context: Option<StepContext>,
    /// The model THIS request is being rendered for (the run's current model). A render
    /// INPUT used to decide whether each assistant message's provider-signed reasoning is
    /// safe to echo: a signature minted by a DIFFERENT model than `target_model` is
    /// stripped (echoing it is a hard provider error). Compared against each
    /// `Message.origin`. Like `step_context` it is `serde(skip)` — a render input (run
    /// identity), never request content, so it can't leak into the generic canonical
    /// render. `None` in isolated render/golden tests (the cross-model check is then
    /// skipped and positional rules stand). The loop populates it each step from the
    /// run's authoritative model.
    #[serde(skip)]
    pub target_model: Option<ModelHandle>,
    /// Cache marker priority fixed for the session. Absent keeps the original marker placement.
    /// Used only by marker renderers; excluded from canonical request serialization.
    #[serde(skip)]
    pub marker_order_version: Option<u32>,
    /// Frozen episode boundaries/configs reconstructed from WAL; never canonical content.
    #[serde(skip)]
    pub render_episodes: Arc<RenderHistory>,
    /// Setup's frozen ranks, addressed by native rendered-prompt position, never ordinal.
    /// Renderer-only metadata: canonical request bytes and their hash domain stay unchanged.
    #[serde(skip)]
    pub declared_markers: Vec<DeclaredMessageMarker>,
}

impl CallOptions {
    /// A bare text call with a prompt and no tools.
    pub fn new(prompt: Vec<Message>) -> Self {
        Self {
            ephemeral_session: false,
            prompt,
            tools: Arc::new(Vec::new()),
            tool_choice: Arc::new(ToolChoice::default()),
            response_format: Arc::new(ResponseFormat::default()),
            generation: Arc::new(GenerationConfig::default()),
            provider_options: Arc::new(Value::Null),
            step_context: None,
            target_model: None,
            marker_order_version: None,
            render_episodes: Arc::default(),
            declared_markers: Vec::new(),
        }
    }

    /// Reuse one request's frozen settings with a request-local transformed prompt.
    /// The durable prompt is deliberately not cloned: a transform owns its output and
    /// the original remains available for the next append-only step.
    pub fn with_prompt(&self, prompt: Vec<Message>) -> Self {
        Self {
            ephemeral_session: self.ephemeral_session,
            prompt,
            tools: Arc::clone(&self.tools),
            tool_choice: Arc::clone(&self.tool_choice),
            response_format: Arc::clone(&self.response_format),
            generation: Arc::clone(&self.generation),
            provider_options: Arc::clone(&self.provider_options),
            step_context: self.step_context.clone(),
            target_model: self.target_model.clone(),
            marker_order_version: self.marker_order_version,
            render_episodes: Arc::clone(&self.render_episodes),
            declared_markers: self.declared_markers.clone(),
        }
    }
}

/// A user-message reminder injected after the run reaches a cumulative tool-call count.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BudgetNudge {
    pub at_tool_calls: u64,
    pub text: String,
}

/// Tool-call reminders for this episode and an independent, lower-only session input cap.
/// The runner freezes reminder config at admission and replays injections from durable history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BudgetConfig {
    /// Session-wide face-value input cap, including cache reads and writes. May only decrease.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_input_tokens: Option<u64>,
    #[serde(default)]
    pub nudges: Vec<BudgetNudge>,
    #[serde(default = "unlimited_tool_calls")]
    pub finalize_at_tool_calls: u64,
    #[serde(default)]
    pub finalize_text: String,
}

fn unlimited_tool_calls() -> u64 {
    u64::MAX
}

impl Default for BudgetConfig {
    fn default() -> Self {
        Self {
            max_input_tokens: None,
            nudges: Vec::new(),
            finalize_at_tool_calls: u64::MAX,
            finalize_text: String::new(),
        }
    }
}

/// Identifies which budget message was injected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BudgetKind {
    Nudge,
    Finalize,
}

/// Controls prompt-cache warming during this send's tool waits. This runner setting is not
/// part of the request rendered for the provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeepWarm {
    Auto,
    #[default]
    Off,
}

/// The caller's request PAYLOAD for one submission — everything that defines a turn EXCEPT
/// the coordinator-assigned identity (`run_id`/`session`/`submission_id`). It is what a
/// `session.send` carries, and what a durable `Queued` record stores so a queued-then-
/// drained submission runs with the SAME config it would have had if sent when idle.
///
/// A submission is a REQUEST, not a frozen config: the render config is resolved/frozen
/// when the run actually STARTS (at drain, for a queued submission; immediately, for an
/// idle send) — so both paths freeze config at run-start, consistently. The queue is a pure
/// deferral, never a config-mutating path (a queued-then-drained run renders the same bytes
/// as the identical request sent when idle).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubmissionRequest {
    /// The restart policy applies only to this send. If omitted, a graceful daemon stop interrupts the run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_restart: Option<OnRestart>,
    /// If `keep_warm` is omitted, this send does not warm, even if an earlier send enabled it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep_warm: Option<KeepWarm>,
    /// Bind observation carried through durable queuing to RunStarted. Excluded from
    /// send-id identity and render configuration: a changed bind must not split a lineage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    /// The principal the daemon stamped on the bind this submission arrived over, recorded on
    /// the `RunStarted` that runs it. Excluded from send-id identity, like `project_id`: the same
    /// send from two principals is still one send.
    ///
    /// Never serialized with the request. Only the serve layer sets it, from the bind, so no
    /// caller payload can supply one; and a durable `Queued` record carries it as its own
    /// top-level field, leaving the stored request's bytes unchanged.
    #[serde(skip)]
    pub principal: Option<crate::BindPrincipal>,
    /// What the route this submission arrived over said about scopes: the daemon's stamp, or
    /// why there is none. Admission decides the owner check and the scope to freeze from it.
    /// Never serialized, for the same reasons as `principal`: only the serve layer sets it, and
    /// a queued submission's episode takes the scope the session had already frozen.
    #[serde(skip)]
    pub scope: crate::SendScope,
    pub model: ModelHandle,
    pub input: Vec<Message>,
    /// Requested tool surface. `None` means inherit the last episode's frozen tools at
    /// admission; `Some([])` deliberately starts a tool-less run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<Tool>>,
    /// Harness policy for an explicit tool surface. Omitted surfaces derive this set from the
    /// inherited frozen tools when admission resolves them.
    #[serde(default)]
    pub policy_allowed: std::collections::HashSet<String>,
    #[serde(default)]
    pub tool_choice: ToolChoice,
    #[serde(default)]
    pub generation: GenerationConfig,
    /// Admission-only cache preferences. They ride the durable queued request so a
    /// queued run resolves the same policy as the identical idle send.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache: Option<CacheRequest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget: Option<BudgetConfig>,
    /// Caller-scoped credential selection, resolved and frozen when this submission is admitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<AuthRequest>,
    /// Caller-supplied context-window override, resolved against the catalog when admitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_limit: Option<u64>,
    /// Opaque caller-declared workload class, frozen when the run is admitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work_class: Option<String>,
    /// The caller's requested reasoning variant (a models.dev variant name such as `high`),
    /// sent inside the wire `model` object. Kept apart from `model` so the model identity that
    /// resume compares stays unchanged. Recorded at admission; not yet applied to the request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
    /// The caller's requested service tier (top-level `service_tier` on `session.send`).
    /// Checked against what the model's provider family and catalog offer, then frozen, when
    /// the run is admitted. Absent costs no bytes on a queued record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<ServiceTier>,
    #[serde(default)]
    pub stop_when: Vec<StopCondition>,
    /// How this session's context is managed, as the caller asked. `None` means the caller
    /// said nothing: admission inherits the session's frozen value, or applies the process
    /// default on a session's first episode. Absent costs no bytes on a queued record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_management: Option<ContextManagement>,
    /// The runner parameters a steer or interrupt into an existing session omitted. Admission
    /// takes each from the session's latest frozen config instead of its default, so the new
    /// turn renders the same request prefix the session was already rendering. Empty for any
    /// other send, which costs no bytes on a queued record.
    #[serde(default, skip_serializing_if = "std::collections::BTreeSet::is_empty")]
    pub inherit_frozen: std::collections::BTreeSet<InheritedParam>,
    /// The schema-shaped output this run asks for, exactly as the send carried it. Per run:
    /// a send without it starts a run with none, and a steer never takes it from the session.
    /// Absent costs no bytes on a queued record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub structured_output: Option<crate::structured_output::StructuredOutput>,
}

/// A runner parameter a send may leave to the session's frozen value; see
/// [`SubmissionRequest::inherit_frozen`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InheritedParam {
    /// The model and its reasoning variant. The request's own `model` is then an empty
    /// placeholder, which admission replaces.
    Model,
    ToolChoice,
    Generation,
    StopWhen,
    /// The cache policy and caller cache key, as the latest episode's render config froze them
    /// (resolved from that episode's `cache` request).
    Cache,
    Budget,
    ContextLimit,
    WorkClass,
    ServiceTier,
}

impl InheritedParam {
    /// Every inheritable parameter, paired with the `session.send` member that carries it.
    pub const ALL: [(InheritedParam, &'static str); 9] = [
        (InheritedParam::Model, "model"),
        (InheritedParam::ToolChoice, "tool_choice"),
        (InheritedParam::Generation, "generation"),
        (InheritedParam::StopWhen, "stop_when"),
        (InheritedParam::Cache, "cache"),
        (InheritedParam::Budget, "budget"),
        (InheritedParam::ContextLimit, "context_limit"),
        (InheritedParam::WorkClass, "work_class"),
        (InheritedParam::ServiceTier, "service_tier"),
    ];
}

/// How a `session.send` asks for its message to be delivered when a run is already active on
/// the session. With no run active every mode starts a turn exactly like a plain send.
///
/// The wire spellings are exactly `"queue"`, `"steer"` and `"interrupt"`; any other value fails
/// to decode rather than falling back to the default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Delivery {
    /// Durably queue the message and start it as the next turn once the active run ends. This
    /// is the default, and what a send that omits `delivery` does.
    #[default]
    Queue,
    /// Add the message to the RUNNING turn: the model sees it as a user message at the turn's
    /// next step boundary, never between a tool call and its result.
    Steer,
    /// Stop the running turn (it ends `Cancelled`) and start the message as the next turn, ahead
    /// of any plain queued sends.
    Interrupt,
}

impl Delivery {
    /// The wire spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Delivery::Queue => "queue",
            Delivery::Steer => "steer",
            Delivery::Interrupt => "interrupt",
        }
    }
}

/// The service tier a caller asks a provider to serve a run at, on one axis every tiered
/// provider shares: `flex` (cheaper, slower, sheddable) < `default` < `priority` (faster, pricier).
///
/// The wire spellings are exactly `"flex"`, `"default"` and `"priority"`. There are no aliases:
/// `"fast"` is OpenAI's marketing name for priority, and accepting it would leave two spellings
/// of one request forever, so it fails to decode like any other unknown value. Which bytes a tier
/// becomes is decided per provider family by `broca_provider::service_tier`, never here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceTier {
    Flex,
    Default,
    Priority,
}

impl ServiceTier {
    /// Every tier, cheapest first. Refusals list the accepted spellings in this order.
    pub const ALL: [ServiceTier; 3] = [
        ServiceTier::Flex,
        ServiceTier::Default,
        ServiceTier::Priority,
    ];

    /// The wire spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            ServiceTier::Flex => "flex",
            ServiceTier::Default => "default",
            ServiceTier::Priority => "priority",
        }
    }
}

impl std::fmt::Display for ServiceTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How a session's prompt is kept inside the model's context window. Chosen once, on the
/// session's first episode, and frozen onto every `RunStarted` after it.
///
/// The wire spellings are exactly `"none"`, `"compaction"` and `{"transform": "<module id>"}`.
/// Anything else fails to decode: falling back to `"none"` would let a run reach the window
/// its caller believed was managed.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextManagement {
    /// No management: a run that reaches the window ends with a context-overflow error.
    None,
    /// A transform module named by id. broca does not serve this value: admission, resume and
    /// boot refuse it by name with `context_management_unavailable`. It still decodes because
    /// older `RunStarted` and `Queued` records carry it, and a WAL holding them must stay
    /// readable rather than fail to decode.
    Transform(String),
    /// broca-native summarise-and-continue. Parsed so a caller can name it, but refused at
    /// admission until the mode exists.
    Compaction,
}

impl std::fmt::Display for ContextManagement {
    /// The wire spelling, so a refusal quotes the value exactly as a caller would send it.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => f.write_str("\"none\""),
            Self::Compaction => f.write_str("\"compaction\""),
            Self::Transform(module) => write!(f, "{{\"transform\": {module:?}}}"),
        }
    }
}

/// Loop-termination predicates — OR'd together, evaluated AFTER every step
/// (before the next tool dispatch). Predicate-based, not a soft prompt max-steps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StopCondition {
    /// Stop once this many steps have completed (the enforced max-steps).
    StepCount { n: u64 },
    /// Stop after a step whose assistant message calls this tool (any tool if None).
    HasToolCall {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
    },
    /// Stop when the loop would naturally finish (a step with no tool calls).
    LoopFinished,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn ephemeral_session_is_local_only_and_survives_prompt_transforms() {
        let mut opts = CallOptions::new(Vec::new());
        let forever_bytes = serde_json::to_vec(&opts).unwrap();
        assert!(!opts.ephemeral_session);
        opts.ephemeral_session = true;
        assert_eq!(serde_json::to_vec(&opts).unwrap(), forever_bytes);
        assert!(opts.clone().ephemeral_session);
        assert!(opts.with_prompt(Vec::new()).ephemeral_session);
        let decoded: CallOptions = serde_json::from_value(json!({
            "prompt": [], "ephemeral_session": true
        }))
        .unwrap();
        assert!(
            !decoded.ephemeral_session,
            "only the runner can set the local flag"
        );
    }

    #[test]
    fn submission_request_tool_module_absent_deserializes_as_none() {
        let decoded: SubmissionRequest = serde_json::from_value(json!({
            "model": {"provider_module_id": "stub", "model_id": "m"},
            "input": [],
            "tools": [{
                "name": "edit",
                "description": "edit files",
                "input_schema": {"type": "object"},
                "execution_mode": "parallel"
            }]
        }))
        .unwrap();

        assert_eq!(decoded.tools.as_ref().unwrap()[0].module, None);
    }

    #[test]
    fn submission_request_tool_module_round_trips_when_present() {
        let request = SubmissionRequest {
            on_restart: None,
            keep_warm: None,
            scope: Default::default(),
            project_id: None,
            principal: None,
            model: ModelHandle::new("stub", "m"),
            input: Vec::new(),
            tools: Some(vec![Tool {
                name: "edit".into(),
                description: "edit files".into(),
                input_schema: json!({"type": "object"}),
                module: Some("aft".into()),
                execution_mode: ExecutionMode::Parallel,
            }]),
            policy_allowed: ["edit".to_string()].into_iter().collect(),
            tool_choice: ToolChoice::Auto,
            generation: GenerationConfig::default(),
            cache: None,
            stop_when: Vec::new(),
            budget: None,
            auth: None,
            context_limit: None,
            work_class: None,
            context_management: None,
            variant: None,
            service_tier: None,
            inherit_frozen: Default::default(),
            structured_output: None,
        };

        let encoded = serde_json::to_value(&request).unwrap();
        assert_eq!(encoded["tools"][0]["module"], "aft");
        let decoded: SubmissionRequest = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded, request);
    }
}

#[cfg(test)]
mod context_management_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn context_management_wire_spellings_round_trip() {
        for (value, wire) in [
            (ContextManagement::None, json!("none")),
            (ContextManagement::Compaction, json!("compaction")),
            (
                ContextManagement::Transform("magic-context".into()),
                json!({"transform": "magic-context"}),
            ),
        ] {
            assert_eq!(serde_json::to_value(&value).unwrap(), wire);
            assert_eq!(
                serde_json::from_value::<ContextManagement>(wire).unwrap(),
                value
            );
        }
    }

    /// A value outside the set is a decode error, never a silent `"none"`.
    #[test]
    fn unknown_context_management_values_do_not_decode() {
        for wire in [
            json!("summarize"),
            json!("transform"),
            json!("None"),
            json!({"transform": 7}),
            json!({"reduce": "magic-context"}),
            json!(null),
            json!(true),
        ] {
            assert!(
                serde_json::from_value::<ContextManagement>(wire.clone()).is_err(),
                "{wire} must not decode"
            );
        }
    }
}

#[cfg(test)]
mod budget_compat_tests {
    use super::*;

    #[test]
    fn absent_submission_budget_is_omitted_and_defaults_to_none() {
        let request = SubmissionRequest {
            on_restart: None,
            keep_warm: None,
            scope: Default::default(),
            project_id: None,
            principal: None,
            model: ModelHandle::new("stub", "m"),
            input: Vec::new(),
            tools: Some(Vec::new()),
            policy_allowed: Default::default(),
            tool_choice: ToolChoice::Auto,
            generation: GenerationConfig::default(),
            cache: None,
            budget: None,
            auth: None,
            context_limit: None,
            work_class: None,
            variant: None,
            service_tier: None,
            stop_when: Vec::new(),
            context_management: None,
            inherit_frozen: Default::default(),
            structured_output: None,
        };
        let encoded = serde_json::to_value(&request).unwrap();
        assert!(encoded.get("budget").is_none());
        assert!(encoded.get("context_management").is_none());
        let decoded: SubmissionRequest = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded.budget, None);
    }
}

/// Per-send policy for a graceful process restart. Crashes always interrupt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnRestart {
    /// Preserve the historical terminal interruption behavior.
    #[default]
    Interrupt,
    /// Pause at a safe boundary and continue the same episode after boot.
    Resume,
}

#[cfg(test)]
mod declared_marker_tests {
    use super::*;
    #[test]
    fn declared_marker_input_keeps_canonical_request_bytes_unchanged() {
        let mut options = CallOptions::new(vec![]);
        let before = serde_json::to_vec(&options).unwrap();
        options.declared_markers = vec![DeclaredMessageMarker {
            message_index: 0,
            rank: 9,
        }];
        assert_eq!(serde_json::to_vec(&options).unwrap(), before);
        let decoded: CallOptions = serde_json::from_slice(&before).unwrap();
        assert!(decoded.declared_markers.is_empty());
        assert_eq!(
            options.with_prompt(vec![]).declared_markers,
            options.declared_markers
        );
    }
}
