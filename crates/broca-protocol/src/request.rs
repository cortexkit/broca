//! Request-side canonical types: the `CallOptions` a provider module receives,
//! plus `stop_when` loop control. Mirrors the Vercel `LanguageModelV4`
//! CallOptions, adapted to subc-routed tools.

pub use cortexkit_role_llm_runner::directives::DirectiveName;
pub use cortexkit_role_llm_runner::send::AccountRoute;
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
use crate::json::Json;

/// A concrete model handle — the runner is GIVEN this (model selection is the
/// caller's job). `{provider_module_id, model_id}` identifies which provider
/// module + model a run is bound to; recorded in the log `run_config` so a
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tool {
    pub name: String,
    pub description: String,
    /// Compact JSON text keeps tool lists from retaining a heap node for every schema object.
    pub input_schema: Json,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub module: Option<String>,
    #[serde(default)]
    pub execution_mode: ExecutionMode,
    /// The longest the tool may take to answer one call, declared by the caller that declared
    /// the tool. Broca waits for the call's terminal reply this long or its default wait,
    /// whichever is longer, so a tool that legitimately holds a call for minutes is not cut off
    /// and its run ended interrupted. Frozen with the rest of the tool at run start. It is dispatch metadata only:
    /// no provider request ever renders it. Absent costs no bytes.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_tool_reply"
    )]
    pub reply: Option<ToolReply>,
}

impl Tool {
    /// Compare schemas by JSON value, not just by their serialized text.
    pub fn has_same_input_schema(&self, other: &Self) -> bool {
        // Equal text covers the common case without parsing. Values can also compare equal
        // with different spellings, such as floating-point positive and negative zero.
        self.input_schema == other.input_schema
            || self.input_schema.parse() == other.input_schema.parse()
    }
}

impl PartialEq for Tool {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.description == other.description
            && self.module == other.module
            && self.execution_mode == other.execution_mode
            && self.reply == other.reply
            && self.has_same_input_schema(other)
    }
}

/// The largest `reply.max_ms` a declared tool may carry: one day. The same bound the tool
/// provider role puts on a catalog tool's `reply.max_ms`
/// (`cortexkit_role_tool_provider::catalog::check_reply`), so a deadline means the same thing
/// whether a catalog or a `session.send` declared it.
pub const TOOL_REPLY_MAX_MS: u64 = 86_400_000;

/// A declared tool's reply deadline: exactly `{max_ms}`. The shape of a catalog tool's `reply`
/// without its `hold_argument`, which a caller-declared tool has no catalog schema pin to
/// support. Strict: an unknown member, a missing `max_ms`, or a `max_ms` outside
/// 1..=[`TOOL_REPLY_MAX_MS`] is refused, never read as no deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolReply {
    #[serde(deserialize_with = "deserialize_tool_reply_max_ms")]
    pub max_ms: u64,
}

impl ToolReply {
    /// Why `max_ms` is not a deadline a declared tool may carry, if it is not.
    pub fn check_max_ms(max_ms: u64) -> Result<(), &'static str> {
        if (1..=TOOL_REPLY_MAX_MS).contains(&max_ms) {
            Ok(())
        } else {
            Err("must be an integer from 1 to 86,400,000")
        }
    }
}

/// A present `reply` must be a deadline object: `null` is refused like any other wrong type
/// rather than read as no deadline, so a caller that meant to declare one learns it did not.
fn deserialize_tool_reply<'de, D>(deserializer: D) -> Result<Option<ToolReply>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    // Decode through a JSON value so only the object form is accepted: serde's derived struct
    // decoding would also take a one-element array (`[1800000]`) as `{max_ms: 1800000}`.
    let value = Value::deserialize(deserializer)?;
    if !value.is_object() {
        return Err(serde::de::Error::custom(format!(
            "reply must be an object holding max_ms; got {value}"
        )));
    }
    serde_json::from_value(value)
        .map(Some)
        .map_err(serde::de::Error::custom)
}

fn deserialize_tool_reply_max_ms<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let max_ms = u64::deserialize(deserializer)?;
    ToolReply::check_max_ms(max_ms)
        .map(|()| max_ms)
        .map_err(|reason| serde::de::Error::custom(format!("reply.max_ms {reason}")))
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
    pub max_output_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stop_sequences: Vec<String>,
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

/// Admission-only credential selection supplied by the caller. It remains a string until
/// admission so an unknown method can be reported in the caller-facing error taxonomy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthRequest {
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_id: Option<String>,
}

/// The run identity for ONE step — the run this step belongs to, and which step it is.
/// Both values are DETERMINISTIC across a resume (the run id is fixed for the run; the step
/// index is reconstructed as `completed_steps + 1` from the log), so a render that derives
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
    /// The session's log address, passed only to transport diagnostics and never rendered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_address: Option<String>,
    /// The frozen credential identity, passed only to transport diagnostics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_id: Option<String>,
    /// The frozen provider account, passed only to transport diagnostics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_ref: Option<String>,
    /// This step's output-token cap, when the run lowered it below the frozen render config's
    /// cap so that the estimated input plus the output reservation stays inside the model's
    /// context window. `None` renders the frozen cap unchanged. The runner records the lowered
    /// value in the session's log (`output_reservation_clamped`) before sending, so a resume and
    /// every request rebuilt from the log render the same cap.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
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
    /// Mode-3 episode boundaries use native transcript ordinals, excluding global
    /// instructions and view insertions. Render-only prefixes must not shift them.
    pub raw_ordinals: bool,
    /// Number of canonical messages present when the compaction view was applied.
    /// Raw messages with smaller ordinals were produced before that view. Absent
    /// means no compaction view; replacement messages have no ordinal.
    pub compaction_prefix_len: Option<u64>,
    /// The ordinal of the first message of the newest run whose tool set differs from the tool
    /// set of the run before it. Anthropic refuses signed thinking that was sent under another
    /// tool set, so a route that binds reasoning to its prefix sends no signed reasoning from a
    /// message with a smaller ordinal (or none), exactly as it drops reasoning before a
    /// compaction view. Absent means no run changed the tool set. Computed from each run's start
    /// record by `broca_core::signed_floor`, never from memory, so a resumed or rebuilt request
    /// renders the same bytes.
    pub signed_floor: Option<u64>,
    /// An effort for this request's answer alone, lower than the one its run froze: a fork's
    /// ask that lowers its effort (llm-runner `fork_answer.effort_policy`). A renderer sends it
    /// in the provider's form that changes the effort for one answer and leaves the request's
    /// own settings and every earlier byte alone, so the cached prefix holds. Absent for every
    /// other request.
    pub answer_effort: Option<String>,
}

impl From<Vec<EpisodeRenderInput>> for RenderHistory {
    fn from(episodes: Vec<EpisodeRenderInput>) -> Self {
        Self {
            episodes,
            raw_ordinals: false,
            compaction_prefix_len: None,
            signed_floor: None,
            answer_effort: None,
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
    /// Frozen episode boundaries/configs reconstructed from the log; never canonical content.
    #[serde(skip)]
    pub render_episodes: Arc<RenderHistory>,
    /// Setup's frozen ranks, addressed by native rendered-prompt position, never ordinal.
    /// Renderer-only metadata: canonical request bytes and their hash domain stay unchanged.
    #[serde(skip)]
    pub declared_markers: Vec<DeclaredMessageMarker>,
    /// The bytes of the images the applied compaction view inserted into `prompt`, which name
    /// them by reference only. A render input like the fields above, never request content.
    #[serde(skip)]
    pub view_media: Arc<ViewMedia>,
}

/// The bytes of the media a compaction view inserted, by digest (`MediaRef::sha256`). A view's
/// inserted image is no medium the session captured, so the session's own media can't supply it;
/// the view holds its bytes, and the request it applies to carries them here for its render.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct ViewMedia(std::collections::BTreeMap<String, Arc<[u8]>>);

impl ViewMedia {
    /// Adds `bytes`, whose digest is `sha256`.
    pub fn insert(&mut self, sha256: String, bytes: Arc<[u8]>) {
        self.0.insert(sha256, bytes);
    }

    /// The bytes whose digest is `sha256`, if the view inserted them.
    pub fn get(&self, sha256: &str) -> Option<&[u8]> {
        self.0.get(sha256).map(|bytes| &bytes[..])
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Names the digests, never the bytes.
impl std::fmt::Debug for ViewMedia {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set().entries(self.0.keys()).finish()
    }
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
            view_media: Arc::default(),
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
            view_media: Arc::clone(&self.view_media),
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

/// How a run keeps the provider's prompt cache warm (llm-runner `keep_warm`), as its admission
/// took it from the session's cache policy. A run's start records it: `"auto"`, `"always"` or
/// `{"hours": {"from", "to", "tz"}}`; `off` records nothing. Never part of the rendered request.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeepWarm {
    /// Warm between turns and during tool waits, until the warms since the last real request
    /// cost as much as one cache rewrite.
    Auto,
    #[default]
    Off,
    /// Warm at any time, up to [`WARM_COUNT_CAP`] warms since the last real request.
    Always,
    /// Warm only inside the local-time window, up to [`WARM_COUNT_CAP`] warms since the last
    /// real request.
    Hours(WarmHours),
}

/// How many warms `always` and `hours` send after a real request before they stop.
pub const WARM_COUNT_CAP: u32 = 24;

impl KeepWarm {
    /// The warming a cache request asks for: its `keep_warm`, with its `hours` window. `None`
    /// for `off`, so an off run's start records no `keep_warm`.
    pub fn of(cache: &cortexkit_role_llm_runner::cache::Cache) -> Option<Self> {
        use cortexkit_role_llm_runner::cache::KeepWarm as Requested;
        match cache.keep_warm() {
            Requested::Auto => Some(Self::Auto),
            Requested::Always => Some(Self::Always),
            // The request's shape was checked at admission, so a window is present; a request
            // that somehow lacks one warms nothing rather than warming at any hour.
            Requested::Hours => cache.hours.as_ref().map(|hours| {
                Self::Hours(WarmHours {
                    from: hours.from.clone(),
                    to: hours.to.clone(),
                    tz: hours.tz.clone(),
                })
            }),
            _ => None,
        }
    }

    /// Whether this mode warms at all.
    pub fn warms(&self) -> bool {
        *self != Self::Off
    }
}

/// A `keep_warm: hours` window: from `from` up to, not including, `to`, both `HH:MM` local time
/// in the IANA zone `tz`. `to` before `from` crosses midnight.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WarmHours {
    pub from: String,
    pub to: String,
    pub tz: String,
}

impl WarmHours {
    /// Whether `tz` names a zone in broca's bundled time-zone database. The database is built
    /// in, not read from the host, so every machine answers alike.
    pub fn known_zone(tz: &str) -> bool {
        jiff::tz::db().get(tz).is_ok()
    }

    /// Whether the instant `at_ms` (Unix milliseconds) falls inside the window. An unknown zone
    /// or a malformed time is outside, so a window broca can't read never warms.
    pub fn contains(&self, at_ms: u64) -> bool {
        let (Some(from), Some(to)) = (minute_of_day(&self.from), minute_of_day(&self.to)) else {
            return false;
        };
        let Ok(zone) = jiff::tz::db().get(&self.tz) else {
            return false;
        };
        let Some(now) = i64::try_from(at_ms)
            .ok()
            .and_then(|ms| jiff::Timestamp::from_millisecond(ms).ok())
        else {
            return false;
        };
        let local = now.to_zoned(zone);
        let minute =
            u32::from(local.hour().unsigned_abs()) * 60 + u32::from(local.minute().unsigned_abs());
        if from < to {
            (from..to).contains(&minute)
        } else {
            minute >= from || minute < to
        }
    }
}

/// `HH:MM` as minutes after midnight.
fn minute_of_day(time: &str) -> Option<u32> {
    let (hour, minute) = time.split_once(':')?;
    let (hour, minute) = (hour.parse::<u32>().ok()?, minute.parse::<u32>().ok()?);
    (hour < 24 && minute < 60).then_some(hour * 60 + minute)
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
    /// Untrusted display marks, validated before admission and excluded from request bytes.
    #[serde(skip)]
    pub message_marks: serde_json::Map<String, serde_json::Value>,
    /// The storage policy the send carries (llm-runner `session_retention`), decoded and checked
    /// by the serve layer. Never serialized with the request: the session's first send carrying
    /// one freezes it in a record of its own, beside the record that holds the send.
    #[serde(skip)]
    pub retention_policy: Option<cortexkit_role_llm_runner::session_retention::RetentionPolicy>,
    /// Caller-owned routing decision, distinct from routing's allocated account decision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_decision_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_route: Option<AccountRoute>,
    /// Held by the server after read-only preflight; never supplied by a client or persisted.
    #[serde(skip)]
    pub staged_uploads: Option<Arc<StagedUploads>>,
    /// Committed upload references, in the attachments contract's order after the prompt.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub content: Vec<cortexkit_role_llm_runner::attachments::ContentPart>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub directives: Option<Vec<cortexkit_role_llm_runner::directives::DirectiveName>>,
    /// The restart policy applies only to this send. If omitted, a graceful daemon stop interrupts the run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_restart: Option<OnRestart>,
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
    /// The cache policy a planless session's creating send carries (llm-runner `cache`). Only a
    /// creating send may carry one, and a creating send never waits in the queue, so it is never
    /// stored with a queued request.
    #[serde(skip)]
    pub cache: Option<cortexkit_role_llm_runner::cache::Cache>,
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
    /// Abstract tier retained by a queued send from a newer writer. Separate from the legacy
    /// provider-tier enum so old recorded requests keep their vocabulary. Records preserve
    /// unfamiliar strings; admission must refuse a level it cannot apply rather than downgrade it.
    /// The boot-gated serve writer populates this independently of the legacy tier member.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_tier_level: Option<String>,
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
    /// The caller's own id for the user message this send creates (llm-runner 0.5.1 §9.11):
    /// 1 to 64 characters of `[A-Za-z0-9_-]`, checked where the send is decoded. It is never
    /// rendered and never part of the message body. It is part of the send's identity, and it
    /// is stored only there: the identity digest (`submission_identity_digest` in broca-core)
    /// carries the value itself, and the `RunStarted`, `Steered` or `Queued` record that holds
    /// the digest then requires the session's log feature `client-message-id/v1`. Never serialized
    /// with the request, so a queued record's request bytes do not change.
    #[serde(skip)]
    pub client_message_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StagedUploads {
    pub identity: crate::BindIdentity,
    pub items: Vec<StagedUpload>,
    pub profile: Option<Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StagedUpload {
    pub reference: crate::MediaRef,
    pub bytes: Arc<Vec<u8>>,
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
    Budget,
    ContextLimit,
    WorkClass,
    ServiceTier,
}

impl InheritedParam {
    /// Every inheritable parameter, paired with the `session.send` member that carries it.
    pub const ALL: [(InheritedParam, &'static str); 8] = [
        (InheritedParam::Model, "model"),
        (InheritedParam::ToolChoice, "tool_choice"),
        (InheritedParam::Generation, "generation"),
        (InheritedParam::StopWhen, "stop_when"),
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

pub use cortexkit_role_llm_runner::delivery_order::Mode as DeliveryMode;

/// What a send using the `delivery_order` group asked for: its ordered modes, and optionally a
/// key under which a newer send from the same sender replaces this one while it is undelivered,
/// and a wall-clock time (Unix milliseconds) after which it is dropped undelivered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryOrder {
    pub modes: Vec<DeliveryMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dedupe_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<cortexkit_role_llm_runner::delivery_order::TemplateUse>,
}

impl DeliveryOrder {
    /// Whether the send may start a run on an idle session. A send without `idle_start` never
    /// starts one: it only joins runs something else started.
    pub fn starts_runs(&self) -> bool {
        self.modes.contains(&DeliveryMode::IdleStart)
    }

    /// Whether the send has expired at `now_ms`.
    pub fn expired_at(&self, now_ms: u64) -> bool {
        self.expires_at_ms.is_some_and(|expires| expires <= now_ms)
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
    /// older `RunStarted` and `Queued` records carry it, and a session's log holding them must stay
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
    fn tool_schema_round_trip_keeps_the_value_wire_spelling() {
        let schema = r#"{ "type" : "object", "properties" : { "z": {"minimum": 2.50, "type": "number"}, "a": {"type": "string"} } }"#;
        let wire = format!(r#"{{"name":"read","description":"","input_schema":{schema}}}"#);
        let legacy_schema: Value = serde_json::from_str(schema).unwrap();
        let legacy_schema = serde_json::to_string(&legacy_schema).unwrap();
        let legacy_wire = format!(
            r#"{{"name":"read","description":"","input_schema":{legacy_schema},"execution_mode":"parallel"}}"#,
        );

        let tool: Tool = serde_json::from_str(&wire).unwrap();
        assert_eq!(tool.input_schema.text(), legacy_schema);
        assert_eq!(serde_json::to_string(&tool).unwrap(), legacy_wire);
    }

    #[test]
    fn tool_schema_comparison_keeps_value_equality() {
        let make_tool = |minimum: &str| {
            serde_json::from_str::<Tool>(&format!(
                r#"{{"name":"read","description":"","input_schema":{{"minimum":{minimum}}}}}"#,
            ))
            .unwrap()
        };
        let positive = make_tool("0.0");
        let negative = make_tool("-0.0");
        assert_ne!(positive.input_schema.text(), negative.input_schema.text());
        assert_eq!(
            positive == negative,
            positive.input_schema.parse() == negative.input_schema.parse(),
        );
    }

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
            message_marks: Default::default(),
            retention_policy: None,
            account_route: None,
            route_decision_id: None,
            service_tier_level: None,
            staged_uploads: None,
            content: Vec::new(),
            directives: None,
            on_restart: None,
            scope: Default::default(),
            project_id: None,
            principal: None,
            model: ModelHandle::new("stub", "m"),
            input: Vec::new(),
            tools: Some(vec![Tool {
                name: "edit".into(),
                description: "edit files".into(),
                input_schema: json!({"type": "object"}).into(),
                module: Some("aft".into()),
                execution_mode: ExecutionMode::Parallel,
                reply: None,
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
            client_message_id: None,
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
            message_marks: Default::default(),
            retention_policy: None,
            account_route: None,
            route_decision_id: None,
            service_tier_level: None,
            staged_uploads: None,
            content: Vec::new(),
            directives: None,
            on_restart: None,
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
            client_message_id: None,
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

#[cfg(test)]
mod tool_reply_tests {
    use super::*;
    use serde_json::json;

    fn tool_with(reply: Value) -> Value {
        json!({"name": "research", "description": "", "input_schema": {"type": "object"}, "reply": reply})
    }

    #[test]
    fn a_declared_reply_decodes_and_encodes_after_the_other_members() {
        let tool: Tool = serde_json::from_value(tool_with(json!({"max_ms": 1_800_000}))).unwrap();
        assert_eq!(tool.reply, Some(ToolReply { max_ms: 1_800_000 }));
        assert_eq!(
            serde_json::to_string(&tool).unwrap(),
            r#"{"name":"research","description":"","input_schema":{"type":"object"},"execution_mode":"parallel","reply":{"max_ms":1800000}}"#
        );
        for bound in [1, TOOL_REPLY_MAX_MS] {
            let tool: Tool = serde_json::from_value(tool_with(json!({"max_ms": bound}))).unwrap();
            assert_eq!(tool.reply, Some(ToolReply { max_ms: bound }));
        }
    }

    #[test]
    fn an_absent_reply_costs_no_bytes() {
        let tool: Tool = serde_json::from_value(
            json!({"name": "research", "description": "", "input_schema": {"type": "object"}}),
        )
        .unwrap();
        assert_eq!(tool.reply, None);
        assert!(!serde_json::to_string(&tool).unwrap().contains("reply"));
    }

    /// Every malformed deadline is refused rather than read as "no deadline": a caller that
    /// meant to declare one must learn it did not, before its long call is cut at the default.
    #[test]
    fn a_malformed_reply_is_refused_not_dropped() {
        for reply in [
            json!(null),
            json!({}),
            json!({"max_ms": 0}),
            json!({"max_ms": TOOL_REPLY_MAX_MS + 1}),
            json!({"max_ms": -5}),
            json!({"max_ms": 1.5}),
            json!({"max_ms": "1800000"}),
            json!({"max_ms": 1000, "hold_argument": {"name": "t", "default_ms": 1, "grace_ms": 1}}),
            json!({"max_ms": 1000, "extra": true}),
            json!(1_800_000),
            json!([1_800_000]),
        ] {
            assert!(
                serde_json::from_value::<Tool>(tool_with(reply.clone())).is_err(),
                "{reply} must be refused"
            );
        }
    }

    /// Unknown members of a tool other than `reply` stay tolerated and dropped, as before.
    #[test]
    fn other_unknown_tool_members_are_still_tolerated() {
        let tool: Tool = serde_json::from_value(json!({
            "name": "research", "description": "", "input_schema": {"type": "object"},
            "timeout_ms": 5, "strict": true
        }))
        .unwrap();
        assert_eq!(tool.reply, None);
        assert_eq!(
            serde_json::to_string(&tool).unwrap(),
            r#"{"name":"research","description":"","input_schema":{"type":"object"},"execution_mode":"parallel"}"#
        );
    }
}

#[cfg(test)]
mod keep_warm_tests {
    use super::{KeepWarm, WarmHours};
    use cortexkit_role_llm_runner::cache::{Cache, Hours, KeepWarm as Requested};

    /// 2026-11-17 22:30 UTC, in Unix milliseconds.
    const LATE_EVENING_UTC_MS: u64 = 1_794_954_600_000;

    fn hours(from: &str, to: &str, tz: &str) -> WarmHours {
        WarmHours {
            from: from.into(),
            to: to.into(),
            tz: tz.into(),
        }
    }

    #[test]
    fn a_run_start_records_the_contract_spellings() {
        assert_eq!(serde_json::to_value(KeepWarm::Auto).unwrap(), "auto");
        assert_eq!(serde_json::to_value(KeepWarm::Always).unwrap(), "always");
        assert_eq!(
            serde_json::to_value(KeepWarm::Hours(hours("22:00", "06:00", "Etc/UTC"))).unwrap(),
            serde_json::json!({"hours": {"from": "22:00", "to": "06:00", "tz": "Etc/UTC"}})
        );
        // What runs recorded before the other modes existed still reads back.
        assert_eq!(
            serde_json::from_value::<KeepWarm>(serde_json::json!("auto")).unwrap(),
            KeepWarm::Auto
        );
    }

    #[test]
    fn a_request_off_or_omitted_warms_nothing() {
        assert_eq!(KeepWarm::of(&Cache::default()), None);
        let mut cache = Cache::default();
        cache.keep_warm = Some(Requested::Off);
        assert_eq!(KeepWarm::of(&cache), None);
        cache.keep_warm = Some(Requested::Always);
        assert_eq!(KeepWarm::of(&cache), Some(KeepWarm::Always));
        cache.keep_warm = Some(Requested::Hours);
        cache.hours = Some(Hours::new("09:00", "17:00", "Europe/Istanbul"));
        assert_eq!(
            KeepWarm::of(&cache),
            Some(KeepWarm::Hours(hours("09:00", "17:00", "Europe/Istanbul")))
        );
    }

    #[test]
    fn a_window_is_half_open_and_may_cross_midnight() {
        let night = hours("22:00", "06:00", "Etc/UTC");
        assert!(night.contains(LATE_EVENING_UTC_MS));
        let ends_at_the_minute = hours("21:00", "22:30", "Etc/UTC");
        assert!(!ends_at_the_minute.contains(LATE_EVENING_UTC_MS));
        let starts_at_the_minute = hours("22:30", "23:00", "Etc/UTC");
        assert!(starts_at_the_minute.contains(LATE_EVENING_UTC_MS));
        let day = hours("06:00", "22:00", "Etc/UTC");
        assert!(!day.contains(LATE_EVENING_UTC_MS));
    }

    #[test]
    fn a_window_reads_local_time_in_its_zone() {
        // 22:30 UTC is 01:30 the next day in Istanbul (UTC+3).
        assert!(hours("01:00", "02:00", "Europe/Istanbul").contains(LATE_EVENING_UTC_MS));
        assert!(!hours("22:00", "23:00", "Europe/Istanbul").contains(LATE_EVENING_UTC_MS));
    }

    #[test]
    fn zones_come_from_the_bundled_database() {
        assert!(WarmHours::known_zone("Etc/UTC"));
        assert!(WarmHours::known_zone("America/Sao_Paulo"));
        assert!(!WarmHours::known_zone("Mars/Olympus_Mons"));
        assert!(!hours("22:00", "06:00", "Mars/Olympus_Mons").contains(LATE_EVENING_UTC_MS));
    }
}
