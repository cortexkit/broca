//! Client-facing JSON contracts for broca's session management surface.
//!
//! This crate lets wire consumers share one contract type set with the server without pulling in
//! the coordinator, provider, catalog, or subc transport trees.
#![forbid(unsafe_code)]

pub mod agent_data;
pub mod role;
pub use role::*;
pub mod warm;
pub use warm::*;
pub mod model_view;
pub use model_view::*;

use broca_protocol::caller::SharedProviderError;
use broca_protocol::request::{
    CacheMode, CacheRole, CacheTail, GenerationConfig, StopCondition, Tool, ToolChoice,
};
use broca_protocol::{BudgetConfig, DisplayEvent, FinishReason, ToolCallId, Usage};
use serde::{Deserialize, Serialize};

/// The complete encoded subc frame limit, including its fixed 21-byte header.
pub const SUBC_MAX_FRAME_BYTES: u64 = 4_194_304;

/// The subc frame header is always 21 bytes. Include it with the JSON body when checking the
/// transport limit; otherwise a body can fit the limit before framing and exceed it afterward.
/// This matches `subc_protocol::HEADER_LEN`, defined in `subc-protocol/src/lib.rs:417`.
pub const SUBC_FRAME_HEADER_BYTES: u64 = 21;

#[derive(Serialize)]
struct ResultEnvelope<'a, T: Serialize> {
    result: &'a T,
}

/// Count a unary JSON reply as the full encoded frame, including its result envelope and header.
pub fn serialized_result_frame_bytes(result: &impl Serialize) -> u64 {
    broca_protocol::serialized_json_bytes(&ResultEnvelope { result })
        .saturating_add(SUBC_FRAME_HEADER_BYTES)
}

/// A route request: the method name plus its operation-specific params.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RouteRequest {
    pub method: String,
    #[serde(default)]
    pub params: serde_json::Value,
}

fn deserialize_on_restart<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<broca_protocol::request::OnRestart>, D::Error> {
    broca_protocol::request::OnRestart::deserialize(deserializer).map(Some)
}

fn deserialize_session_class<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<String>, D::Error> {
    let value = String::deserialize(deserializer)
        .map_err(|error| serde::de::Error::custom(format!("session_class: {error}")))?;
    broca_protocol::request::validate_session_class(&value).map_err(serde::de::Error::custom)?;
    Ok(Some(value))
}

/// `session.send` params. The session comes from the bound route identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SendParams {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_directives"
    )]
    pub directives: Option<Vec<cortexkit_role_llm_runner::directives::DirectiveName>>,
    /// Opaque session label, frozen on the first send; omission on later sends inherits it.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_session_class"
    )]
    pub session_class: Option<String>,
    /// Graceful restart policy for this send. Omitted means interrupt.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_on_restart"
    )]
    pub on_restart: Option<broca_protocol::request::OnRestart>,
    /// Enables prompt-cache warming only while this send's run waits on tools. Omitted means off.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep_warm: Option<broca_protocol::request::KeepWarm>,
    /// Requested transcript retention in seconds. Admission refuses it until retention is offered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention: Option<u64>,
    /// The user prompt for this turn. Omit it when `prompt_blocks` supplies the prompt.
    #[serde(default)]
    pub prompt: String,
    /// Caller-declared text blocks for one user message, in render order.
    ///
    /// The final block is the varying tail; preceding blocks form an Anthropic cache prefix.
    /// Other providers preserve the blocks but do not infer an intra-message cache boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_blocks: Option<Vec<String>>,
    /// Optional role-scoped system prompt. Empty or whitespace-only values have no effect.
    #[serde(default)]
    pub system: Option<String>,
    /// Optional client-chosen idempotency key for one logical send.
    #[serde(default)]
    pub send_id: Option<String>,
    /// Caller-selected routing affinity; absent uses the session's identity.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_conversation_key"
    )]
    pub conversation_key: Option<String>,
    /// A human-readable title for the work this send carries, shown by dashboards and logs in
    /// place of the bare session name. Accepted on every delivery mode and recorded on the WAL
    /// record the send writes. INFORMATIONAL ONLY: it is never rendered into a provider request
    /// and is not part of routing, cache keys or the send's identity, so a retry under the same
    /// `send_id` with a different title gets the recorded answer and keeps the first title.
    /// The VALUE is strict: at most 200 Unicode scalar values, not blank, and no control
    /// characters (newlines and tabs included). A violation is refused, naming the field; a title
    /// is never truncated.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_title"
    )]
    pub title: Option<String>,
    /// The caller's own id for the user message this send creates (llm-runner 0.5.1 §9.11,
    /// the `client_message_id` group): 1 to 64 characters of `[A-Za-z0-9_-]`, refused naming
    /// the field otherwise. Part of the send's identity, so a retry under the same `send_id`
    /// must carry the same id, or none when the first send carried none. Never rendered, never
    /// in the stored message body and never in `session.read`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_client_message_id"
    )]
    pub client_message_id: Option<String>,
    /// Required on a send that starts or queues a turn. A `steer` or `interrupt` into a session
    /// that already has an episode may omit it and takes the session's frozen model.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<ModelParams>,
    /// Omitted inherits the prior episode's frozen surface; an empty array requests no tools.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<Tool>>,
    #[serde(default)]
    pub tool_choice: ToolChoice,
    #[serde(default)]
    pub generation: GenerationConfig,
    #[serde(default, deserialize_with = "deserialize_stop_when")]
    pub stop_when: Vec<StopCondition>,
    /// Admission-only prompt-cache preferences.
    #[serde(default)]
    pub cache: Option<CacheParams>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_budget"
    )]
    pub budget: Option<BudgetConfig>,
    /// Schema-shaped output for this send's run: `{mode, schema, name?}`, where `mode` is
    /// `final_text` (shape the final answer) or `submit_tool` (offer a `submit_result` tool
    /// whose call ends the run). Strict: every member rule of the shared role crate
    /// (`cortexkit_role_llm_runner::structured_output`) is checked as the send is decoded, and a
    /// schema broca cannot compile is refused there too, naming the member's path.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_structured_output"
    )]
    pub structured_output: Option<broca_protocol::structured_output::StructuredOutput>,
    /// A caller-scoped credential selection resolved once at admission.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<AuthParams>,
    /// The caller's explicit context-window override. `None` means use the catalog's value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_limit: Option<u64>,
    /// Opaque caller-declared workload class, frozen at admission and carried to usage export.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work_class: Option<String>,
    /// How the session's context is managed: `"none"`, `{"transform": "<module id>"}` or
    /// `"compaction"`. The session's first episode binds it; a later send may omit it to
    /// inherit, and one carrying a different value is refused. Omitted on a first episode means
    /// the process default. Unknown FIELDS on `SendParams` stay tolerated (see `StatusParams`),
    /// but the `context_management` VALUE is strict: an unknown value is refused rather than
    /// read as none. `{"transform": ...}` decodes here and admission refuses it by name (broca
    /// does not serve it), as it does `"compaction"` until that mode exists.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_context_management"
    )]
    pub context_management: Option<broca_protocol::request::ContextManagement>,
    /// The service tier to ask the provider for: `"flex"`, `"default"` or `"priority"`.
    /// Absent means the provider's own default and renders exactly as a send without the field.
    /// Admission refuses a tier the model's provider family has no request bytes for, or that
    /// the catalog has no rate for, naming the tiers that are offered; it is never downgraded
    /// silently. The VALUE is strict like `context_management`'s: `"fast"` or any other
    /// spelling is refused rather than ignored.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_service_tier"
    )]
    pub service_tier: Option<broca_protocol::request::ServiceTier>,
    /// Explicitly marks a send as single-turn so its transcript need not be projected into the
    /// materialized session store. The WAL remains authoritative and available to `session.read`.
    ///
    /// The name describes THIS turn; the cost lands on every LATER read of the session. Skipping
    /// projection leaves the store watermark where it was, so a subsequent `session.read` merges
    /// the store prefix with a WAL tail that grows with each turn. A 23k-message session whose
    /// projection had stalled read at ~127ms per page against ~13ms for one projected normally,
    /// with no error and nothing at the call site to attribute it to.
    ///
    /// So it is correct for a caller that will never reopen the session, and wrong for anything
    /// resumable — including sends that merely LOOK fire-and-forget. Absent means materialize,
    /// which is the safe direction: a caller who has not considered the question pays storage
    /// rather than inheriting a read pathology it cannot see.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub one_shot: Option<bool>,
    /// How to deliver the message when a run is already active on the session: `"queue"`
    /// (the default, and what an absent field means) queues it as the next turn; `"steer"` adds
    /// it to the running turn at its next step boundary; `"interrupt"` cancels the running turn
    /// and starts the message as the next turn, ahead of queued sends. With no run active every
    /// mode starts a turn like a plain send (a steer into a PAUSED run is refused
    /// `run_paused`). The mode is part of the send's identity: a retry with the same `send_id`
    /// and another mode is refused `send_id_reuse` naming `delivery`. The VALUE is strict like
    /// `service_tier`'s: an unknown mode is refused, naming the field.
    ///
    /// Or an ARRAY, the `delivery_order` group: an ordered list of `"tool_result"`,
    /// `"turn_system"`, `"steer"` and `"idle_start"` (non-empty, no mode twice). At each request
    /// boundary the runner takes the first mode possible there; when none is, the send waits,
    /// durably queued. A list without `"idle_start"` never starts a run. The legacy strings keep
    /// their meaning exactly: absent or `"queue"` is `["idle_start"]`, `"steer"` is
    /// `["steer", "idle_start"]`. Older runners refuse an array by this field's strict decode.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_delivery"
    )]
    pub delivery: Option<SendDelivery>,
    /// `delivery_order` group: a newer send from the same sender with the same key replaces a
    /// queued, undelivered one, which then ends `superseded`. Distinct from `send_id`: a retry
    /// with the same `send_id` is the same send, not a replacement.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_dedupe_key"
    )]
    pub dedupe_key: Option<String>,
    /// `delivery_order` group: Unix milliseconds after which a still-undelivered send is
    /// dropped, recorded durably, and ends `expired`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_expires_at"
    )]
    pub expires_at_ms: Option<u64>,
    /// `delivery_order` group: the sender names a plan-declared template, its digest and its
    /// typed slot values instead of a prompt. The rendered text falls through `turn_system`
    /// to the next possible mode because no provider lane supports turn-scoped system messages.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_template"
    )]
    pub template: Option<cortexkit_role_llm_runner::delivery_order::TemplateUse>,
    /// Deprecated inert compatibility field. The coordinator determines append behavior from the
    /// run identity.
    #[serde(default)]
    pub append_episode: bool,
}

/// Check the shared run parameters before broca's legacy parameter decoder can lose a
/// nested field name. Legacy stop-condition lists remain accepted beside the role object.
pub fn check_run_parameters(params: &serde_json::Value) -> Result<(), String> {
    let mut request = cortexkit_role_llm_runner::send::SendRequest::new("", "");
    for field in ["stop_when", "generation", "tools", "directives"] {
        if let Some(value) = params.get(field) {
            if field == "stop_when" && value.is_array() || field == "tools" && value.is_null() {
                continue;
            }
            request.runner_params.insert(field.into(), value.clone());
        }
    }
    request.max_steps()?;
    request.max_output_tokens()?;
    request.no_tools()?;
    request.directives()?;
    Ok(())
}

/// Check every `tools[i].reply` of a `session.send` before anything else reads the send, so a
/// malformed deadline is refused `invalid_params` naming its exact member
/// (`tools[0].reply.max_ms`, `tools[1].reply.timeout`, ...) rather than the whole `tools` field,
/// and before admission writes anything. The rules are [`broca_protocol::ToolReply`]'s: an
/// object holding exactly a `max_ms` integer from 1 to
/// [`broca_protocol::request::TOOL_REPLY_MAX_MS`]. Other unknown members of a tool are left as
/// they always were: tolerated and dropped.
pub fn check_tool_replies(params: &serde_json::Value) -> Result<(), (String, String)> {
    let Some(tools) = params.get("tools").and_then(serde_json::Value::as_array) else {
        return Ok(());
    };
    for (index, tool) in tools.iter().enumerate() {
        let Some(reply) = tool.get("reply") else {
            continue;
        };
        let field = format!("tools[{index}].reply");
        let Some(members) = reply.as_object() else {
            return Err((
                field.clone(),
                format!("{field} must be an object holding max_ms; got {reply}"),
            ));
        };
        if let Some(unknown) = members.keys().find(|key| key.as_str() != "max_ms") {
            let field = format!("{field}.{unknown}");
            return Err((
                field.clone(),
                format!("{field} is not a reply member; a declared tool's reply holds only max_ms"),
            ));
        }
        let field = format!("{field}.max_ms");
        let checked = match members.get("max_ms") {
            None => Err("is required"),
            Some(value) => value
                .as_u64()
                .ok_or("must be an integer from 1 to 86,400,000")
                .and_then(broca_protocol::ToolReply::check_max_ms),
        };
        if let Err(reason) = checked {
            return Err((field.clone(), format!("{field} {reason}")));
        }
    }
    Ok(())
}

fn deserialize_directives<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Vec<cortexkit_role_llm_runner::directives::DirectiveName>>, D::Error> {
    let value = serde_json::Value::deserialize(deserializer)?;
    let mut request = cortexkit_role_llm_runner::send::SendRequest::new("", "");
    request.runner_params.insert("directives".into(), value);
    request.directives().map_err(serde::de::Error::custom)
}

fn deserialize_stop_when<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<StopCondition>, D::Error> {
    let value = serde_json::Value::deserialize(deserializer)?;
    if value.is_array() {
        return serde_json::from_value(value).map_err(serde::de::Error::custom);
    }
    let mut request = cortexkit_role_llm_runner::send::SendRequest::new("", "");
    request.runner_params.insert("stop_when".into(), value);
    request
        .max_steps()
        .map(|limit| {
            limit
                .into_iter()
                .map(|n| StopCondition::StepCount { n })
                .collect()
        })
        .map_err(serde::de::Error::custom)
}

fn deserialize_budget<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<BudgetConfig>, D::Error> {
    let value = serde_json::Value::deserialize(deserializer)?;
    let budget = checked_role_budget(value).map_err(serde::de::Error::custom)?;
    let Some(budget) = budget else {
        return Ok(None);
    };
    let request = cortexkit_role_llm_runner::send::SendRequest::new("", "").with_budget(budget);
    serde_json::from_value(request.runner_params["budget"].clone())
        .map(Some)
        .map_err(serde::de::Error::custom)
}

fn checked_role_budget(
    value: serde_json::Value,
) -> Result<Option<cortexkit_role_llm_runner::send::Budget>, String> {
    use cortexkit_role_llm_runner::send::SendRequest;
    let mut request = SendRequest::new("", "");
    request.runner_params.insert("budget".into(), value);
    let budget = request.budget()?;
    if let Some(budget) = &budget {
        budget.check(true, None)?;
    }
    Ok(budget)
}

/// Serde identifies the enclosing `budget` field; preserve the role contract's exact
/// offending member path in the serving adapter's `invalid_params.detail.field` instead.
pub fn send_budget_refusal_field(params: &serde_json::Value) -> Option<String> {
    checked_role_budget(params.get("budget")?.clone()).err()
}

fn deserialize_structured_output<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<broca_protocol::structured_output::StructuredOutput>, D::Error> {
    let value = serde_json::Value::deserialize(deserializer)?;
    checked_structured_output(value).map_err(serde::de::Error::custom)
}

/// The role crate's member, reference, dialect and size checks, then broca's own compile
/// check. Returns the offending field path.
fn checked_structured_output(
    value: serde_json::Value,
) -> Result<Option<broca_protocol::structured_output::StructuredOutput>, String> {
    use cortexkit_role_llm_runner::send::SendRequest;
    let mut request = SendRequest::new("", "");
    request
        .runner_params
        .insert(broca_protocol::structured_output::FIELD.into(), value);
    let structured_output = request.structured_output()?;
    if let Some(structured_output) = &structured_output {
        broca_protocol::structured_output::check_compiles(structured_output)?;
    }
    Ok(structured_output)
}

/// Serde identifies the enclosing `structured_output` field; report the role contract's exact
/// offending member path (`structured_output.schema`, `structured_output.mode`, ...) in the
/// serving adapter's `invalid_params.detail.field` instead.
pub fn send_structured_output_refusal_field(params: &serde_json::Value) -> Option<String> {
    checked_structured_output(params.get("structured_output")?.clone()).err()
}

fn deserialize_conversation_key<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    let key = value
        .as_str()
        .filter(|key| {
            !key.is_empty()
                && key.len() <= 256
                && key.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
        })
        .ok_or_else(|| {
            serde::de::Error::custom(
                "conversation_key must be 1-256 bytes of printable ASCII (0x21-0x7E)",
            )
        })?;
    Ok(Some(key.to_owned()))
}

/// The longest `session.send` title accepted, counted in Unicode scalar values (Rust `char`s),
/// so a title's limit does not depend on how many bytes its script encodes to.
pub const TITLE_MAX_CHARS: usize = 200;

/// Whether `title` is a title `session.send` accepts: 1 to [`TITLE_MAX_CHARS`] Unicode scalar
/// values, not all whitespace, and no control character (`char::is_control`, which covers
/// newline, carriage return and tab), since a title is shown on one line of a dashboard or log.
pub fn title_is_valid(title: &str) -> bool {
    !title.trim().is_empty()
        && title.chars().count() <= TITLE_MAX_CHARS
        && !title.chars().any(char::is_control)
}

/// Decode `SendParams.title`, refusing a value [`title_is_valid`] rejects (and null or a
/// non-string) with a message naming the field, so the caller is refused before anything is
/// written rather than having the title silently cut or dropped.
fn deserialize_title<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    let title = value
        .as_str()
        .filter(|title| title_is_valid(title))
        .ok_or_else(|| {
            serde::de::Error::custom(
                "title must be a string of 1-200 characters that is not blank and has no control characters",
            )
        })?;
    Ok(Some(title.to_owned()))
}

fn deserialize_client_message_id<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    let id = value
        .as_str()
        .filter(|id| cortexkit_role_llm_runner::display::client_message_id_valid(id))
        .ok_or_else(|| {
            serde::de::Error::custom(
                "client_message_id must be a string of 1-64 characters from [A-Za-z0-9_-]",
            )
        })?;
    Ok(Some(id.to_owned()))
}

/// Decode `SendParams.context_management`, naming the field and the accepted spellings on a
/// refusal. serde's own message ("unknown variant", "expected a string") says neither, and a
/// caller reading it cannot tell which of a send's many fields was wrong.
fn deserialize_context_management<'de, D>(
    deserializer: D,
) -> Result<Option<broca_protocol::request::ContextManagement>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let Some(value) = Option::<serde_json::Value>::deserialize(deserializer)? else {
        return Ok(None);
    };
    serde_json::from_value(value.clone())
        .map(Some)
        .map_err(|error| {
            serde::de::Error::custom(format!(
                "context_management must be \"none\", \"compaction\" or \
                 {{\"transform\": \"<module id>\"}}; got {value} ({error})"
            ))
        })
}

/// Decode `SendParams.service_tier`, naming the field and the three accepted spellings on a
/// refusal. A JSON `null` reads as absent.
fn deserialize_service_tier<'de, D>(
    deserializer: D,
) -> Result<Option<broca_protocol::request::ServiceTier>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let Some(value) = Option::<serde_json::Value>::deserialize(deserializer)? else {
        return Ok(None);
    };
    serde_json::from_value(value.clone())
        .map(Some)
        .map_err(|_| {
            serde::de::Error::custom(format!(
                "service_tier must be \"flex\", \"default\" or \"priority\"; got {value}"
            ))
        })
}

/// A send's `delivery`: one of the legacy mode strings, or an ordered list of delivery modes
/// (the `delivery_order` group). This is the one place the two forms are told apart.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum SendDelivery {
    /// `"queue"`, `"steer"` or `"interrupt"`, with the meaning they always had.
    Mode(cortexkit_role_llm_runner::send::Delivery),
    /// An ordered list of delivery modes (the `delivery_order` group).
    Order(Vec<broca_protocol::DeliveryMode>),
}

fn deserialize_dedupe_key<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Option<String>, D::Error> {
    String::deserialize(d)
        .map(Some)
        .map_err(|error| serde::de::Error::custom(format!("dedupe_key: {error}")))
}

fn deserialize_expires_at<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
    u64::deserialize(d)
        .map(Some)
        .map_err(|error| serde::de::Error::custom(format!("expires_at_ms: {error}")))
}

fn deserialize_template<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Option<cortexkit_role_llm_runner::delivery_order::TemplateUse>, D::Error> {
    cortexkit_role_llm_runner::delivery_order::TemplateUse::deserialize(d)
        .map(Some)
        .map_err(|error| serde::de::Error::custom(format!("template: {error}")))
}

impl SendDelivery {
    /// The legacy mode, when `delivery` is a string.
    pub fn mode(&self) -> Option<broca_protocol::Delivery> {
        match self {
            SendDelivery::Mode(mode) => Some(match mode {
                cortexkit_role_llm_runner::send::Delivery::Queue => broca_protocol::Delivery::Queue,
                cortexkit_role_llm_runner::send::Delivery::Steer => broca_protocol::Delivery::Steer,
                cortexkit_role_llm_runner::send::Delivery::Interrupt => {
                    broca_protocol::Delivery::Interrupt
                }
            }),
            SendDelivery::Order(_) => None,
        }
    }
}

impl SendParams {
    /// The legacy delivery mode this send runs with (see [`legacy_delivery`]).
    pub fn legacy_delivery(&self) -> broca_protocol::Delivery {
        legacy_delivery(self.delivery.as_ref())
    }

    /// Decode the ordered-delivery members. Templates require a list and are rendered from
    /// the attested sender's frozen plan before the send is recorded.
    pub fn delivery_order(
        &self,
    ) -> Result<Option<broca_protocol::DeliveryOrder>, (&'static str, String)> {
        if self.template.is_some() && !matches!(self.delivery, Some(SendDelivery::Order(_))) {
            return Err((
                "template",
                "template is valid only when delivery is an ordered list".into(),
            ));
        }
        let mut order = delivery_order(
            self.delivery.as_ref(),
            self.dedupe_key.as_deref(),
            self.expires_at_ms,
        )?;
        if let Some(order) = &mut order {
            order.template = self.template.clone();
        }
        Ok(order)
    }
}

/// The legacy delivery mode a send runs with: its string `delivery`, or the default `queue` when
/// `delivery` is absent or an ordered list (an ordered send is queued).
pub fn legacy_delivery(delivery: Option<&SendDelivery>) -> broca_protocol::Delivery {
    delivery.and_then(SendDelivery::mode).unwrap_or_default()
}

/// The `delivery_order` group as a send uses it, or `None` for a send that uses none of it (which
/// then behaves exactly as before the group existed). `dedupe_key` and `expires_at_ms` are valid
/// only when `delivery` is an ordered list: with a legacy string or no `delivery` they are
/// refused, so a runner that would ignore them never sees them as part of an ordinary send.
/// Refusals name the field.
pub fn delivery_order(
    delivery: Option<&SendDelivery>,
    dedupe_key: Option<&str>,
    expires_at_ms: Option<u64>,
) -> Result<Option<broca_protocol::DeliveryOrder>, (&'static str, String)> {
    use cortexkit_role_llm_runner::delivery_order::{
        DeliveryOrder, DEDUPE_KEY_FIELD, EXPIRES_AT_FIELD,
    };
    let modes = match delivery {
        Some(SendDelivery::Order(modes)) => Some(modes.as_slice()),
        _ => None,
    };
    let mut members = serde_json::Map::new();
    if let Some(key) = dedupe_key {
        members.insert(DEDUPE_KEY_FIELD.into(), key.into());
    }
    if let Some(at) = expires_at_ms {
        members.insert(EXPIRES_AT_FIELD.into(), at.into());
    }
    DeliveryOrder::decode(modes, &members)
        .map(|order| {
            order.map(|order| broca_protocol::DeliveryOrder {
                modes: order.modes,
                dedupe_key: order.dedupe_key,
                expires_at_ms: order.expires_at_ms,
                template: order.template,
            })
        })
        .map_err(|field| (field, format!("invalid ordered delivery member {field}")))
}

/// Use the role's string-or-array decoder; its wire enum is private, so retain a local
/// carrier only for broca's optional-send-id request shape.
fn deserialize_delivery<'de, D>(deserializer: D) -> Result<Option<SendDelivery>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    let request: cortexkit_role_llm_runner::send::SendRequest = serde_json::from_value(
        serde_json::json!({"send_id": "decode-delivery", "delivery": value}),
    )
    .map_err(serde::de::Error::custom)?;
    Ok(match (request.delivery_order, request.delivery) {
        (Some(modes), _) => Some(SendDelivery::Order(modes)),
        (_, Some(mode)) => Some(SendDelivery::Mode(mode)),
        _ => None,
    })
}

/// Every top-level key [`SendParams`] reads. Anything else on a `session.send` is ignored by
/// design; the server warns about it once so an ignored option is visible.
///
/// Read from the derived `Deserialize` impl itself rather than kept as a hand list, so a field
/// added to the struct is known here the moment it exists and can never be reported as unknown.
pub fn send_params_keys() -> &'static [&'static str] {
    static KEYS: std::sync::OnceLock<&'static [&'static str]> = std::sync::OnceLock::new();
    KEYS.get_or_init(struct_field_names::<SendParams>)
}

/// Every key [`ModelParams`] reads inside a send's `model` object, derived the same way.
pub fn model_params_keys() -> &'static [&'static str] {
    static KEYS: std::sync::OnceLock<&'static [&'static str]> = std::sync::OnceLock::new();
    KEYS.get_or_init(struct_field_names::<ModelParams>)
}

/// The wire field names a derived `Deserialize` struct accepts.
///
/// serde's derive hands its complete field list (after renames) to `deserialize_struct`; this
/// deserializer records that list and stops. Panics if `T` is not a plain derived struct, which
/// only a change to this crate's own types could cause.
fn struct_field_names<T: serde::de::DeserializeOwned>() -> &'static [&'static str] {
    use serde::de::{self, Visitor};

    struct Capture<'a>(&'a mut Option<&'static [&'static str]>);

    impl<'de> serde::Deserializer<'de> for Capture<'_> {
        type Error = de::value::Error;

        fn deserialize_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
            Err(de::Error::custom("not a struct"))
        }

        fn deserialize_struct<V: Visitor<'de>>(
            self,
            _name: &'static str,
            fields: &'static [&'static str],
            _visitor: V,
        ) -> Result<V::Value, Self::Error> {
            *self.0 = Some(fields);
            Err(de::Error::custom("field names captured"))
        }

        serde::forward_to_deserialize_any! {
            bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes
            byte_buf option unit unit_struct newtype_struct seq tuple tuple_struct map enum
            identifier ignored_any
        }
    }

    let mut fields = None;
    let _ = T::deserialize(Capture(&mut fields));
    fields.expect("a derived struct reports its field names")
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthParams {
    /// The auth method to use, overriding the provider's configured default.
    pub method: String,
    /// Selects one credential when a provider holds several under one method.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelParams {
    pub provider: String,
    pub model: String,
    /// The requested reasoning variant, named as in the model's models.dev entry (e.g. `high`).
    /// It travels inside `model` because that is where callers (following OpenCode) already
    /// send it. Admission resolves it to the model's reasoning effort and freezes that effort for
    /// the run, so resumed sends keep the same reasoning behavior.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<CacheRole>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl: Option<String>,
    /// An opaque caller-chosen cache scope. Callers that share a key share provider cache
    /// affinity; changing it starts a newly frozen scope for the admitted run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// The caller's declaration that its final message varies between requests, so a
    /// breakpoint there would be paid for and never read. Absent means no declaration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tail: Option<CacheTail>,
    /// `"none"` switches prompt caching off for this send, including a provider's default
    /// breakpoints (first-party Anthropic places them when a send carries no cache request).
    /// Absent leaves that default in place. Admission refuses `none` combined with `role`,
    /// `ttl` or `tail`, because those fields configure the caching `none` switches off.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<CacheMode>,
}

/// `session.retract` params.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetractParams {
    pub submission_id: String,
}

/// `run.cancel` params.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelParams {
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<String>,
}

/// `run.status` params. An absent `run_id` selects session status; a present value selects one
/// durable run.
///
/// Unknown fields are rejected because an absent `run_id` selects session status. A
/// misspelling such as `runId` must not silently change a per-run query into a session query.
///
/// Check caller compatibility before applying strict decoding to another operation. Tests
/// and `docs/module-surface.md` document supported request fields and compatibility rules.
/// `SendParams` and `CacheParams` remain extensible so a newer caller can send optional fields
/// to an older daemon during a rolling upgrade without every request being refused.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatusParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
}

/// A durable control-stream position shared by `session.read` and `session.subscribe`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CursorWire {
    pub wal_seq: u64,
    pub sub_index: u32,
}

/// `session.subscribe` params: `from` is `"start"`, `"live"`, or a durable cursor.
///
/// Passing the `head` returned by `session.read` resumes strictly after that cursor, making the
/// read-to-subscribe handoff gapless and duplicate-free.
///
/// STRICT ON UNKNOWN FIELDS: an absent `from` attaches live, so a misspelled `from` would
/// silently ask a different question and miss every event before the attach.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscribeParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<FromWire>,
}

/// `session.read` params. The session comes from the bound route identity.
///
/// With no `from_ordinal`, the server returns the newest page, starting at
/// `prompt_len.saturating_sub(limit)`. An explicit `from_ordinal` returns the forward range
/// `[from_ordinal, from_ordinal + limit)`. `next_from_ordinal` is present only when the page ends
/// before `prompt_len`. To walk backward, start with the newest page and request earlier ordinal
/// ranges ending at that page's first ordinal.
///
/// A PAGE IS AN ORDINAL RANGE AND NOTHING MORE. It is not aligned to turns, episodes, or
/// tool-call boundaries, and no future change should make a consumer's correctness depend on
/// alignment it does not promise. Concretely: a tool call and its result are adjacent messages,
/// so a page boundary splits them routinely -- and because back-fill walks backward, the RESULT
/// commonly arrives one page BEFORE its call. A consumer pairing the two must hold unmatched
/// halves across page boundaries in both directions.
///
/// Stated because a consumer discovered it by building the holding logic and then could not tell
/// whether they were compensating for an intended property or masking a defect. From the
/// server's side those are identical, so the guarantee has to be written down rather than
/// inferred from observed behaviour.
///
/// STRICT ON UNKNOWN FIELDS, for the same reason as `StatusParams`: absence of `from_ordinal`
/// is meaningful ("the newest page"), so a misspelled `fromOrdinal` from a camelCase encoder
/// doesn't drop a field. It asks a different question, and a back-filling reader gets the
/// newest page instead of the range it asked for, well-formed, with nothing to say so.
///
/// Tightened only after reading every caller's source, not this struct's shape. Checked
/// 2026-09-25: prefrontal once sent `direction: "newest_last"` on every read and removed it
/// (the deployed ck-prefrontal-core contains 0 copies of that string, against 12 of
/// `from_ordinal` as a control); alfonso-tui sends only `limit` and `from_ordinal`; prefrontal's
/// merge-tools-replay experiment script sends the same two. A stale local clone,
/// prefrontal-e2e-clone (head 2026-09-19), still sends `direction`, and anything built from it
/// now gets `invalid_params` naming the field rather than a silently ignored key.
///
/// The field set grew once, with `include_tools`, and the struct stayed strict on purpose. The
/// rolling-upgrade concern that keeps `SendParams` lenient cuts the other way here: a newer
/// caller asking an older daemon for tools gets `invalid_params` naming the field. A lenient
/// daemon would drop the key and answer without `tools`, which reads exactly like "this session
/// has no episode" -- an absent answer passed off as a real one.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadParams {
    /// Continue the out-of-prompt late-approval stream strictly after this durable call key.
    /// Such a page contains no messages. An unavailable cursor is `invalid_params`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub late_after: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_ordinal: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
    /// Opt in to `tools` and `tools_run_id` on the response: the tool list the session's latest
    /// episode was admitted with. Off by default because it can cost a WAL read beyond the page,
    /// and omitted from the request when false so existing request bytes are unchanged.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub include_tools: bool,
    /// Read strictly after the message with this `mid`, taken from an earlier page of the
    /// lineage `lineage_id` names. Refused `unknown_mid` when that lineage has no such message,
    /// never answered with the newest page instead. Requires `lineage_id`, and cannot be combined
    /// with `from_ordinal`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_mid: Option<String>,
    /// The lineage the caller's cursor belongs to. A read naming any other lineage than the
    /// session's current one is refused `lineage_changed`, whatever its mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage_id: Option<String>,
    /// A byte cap on the page. Transcript reads budget the complete serialized reply body,
    /// with a default and maximum of 3 MiB (3,145,728 bytes); larger requests are capped to
    /// [`READ_MAXIMUM_MAX_BYTES`]. Messages and late approvals share that budget, including all
    /// metadata and continuation cursors. One message and one late entry, when present, are kept
    /// whole even when they exceed `max_bytes`. The complete encoded frame (including its fixed
    /// header) is still bounded: `reply_too_large` names the message or atomic group if it cannot
    /// fit the transport limit.
    /// Model reads use the same serialized reply-body budget and 3 MiB default/maximum. They
    /// keep replacement groups and insertion-and-anchor groups whole; a whole group is refused
    /// only when its complete encoded reply frame exceeds the transport frame limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
    /// Ask for the values a hook replaced on each message it changed. broca runs no hooks yet,
    /// so no message carries any.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub include_originals: bool,
    /// `inline` (the default) or `descriptors`, which serves large payloads as refs in place
    /// and every entry's content identities (llm-runner §4.1). Any other value is refused
    /// naming `media`; a model-view read refuses `descriptors`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media: Option<cortexkit_role_llm_runner::read::MediaMode>,
}

/// Wire spelling for a subscribe attach point.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FromWire {
    Named(String),
    Cursor { wal_seq: u64, sub_index: u32 },
}

/// `session.send` response.
///
/// For a send with `delivery: "steer"` that joined a running turn, `active` names THE RUN THAT
/// CARRIES YOUR MESSAGE, which is the run that was already running, not a new one. It is sent
/// once the steered message is durable, and it guarantees the message is in the transcript;
/// the message reaches a model within that turn only if the turn takes another step (a cancel,
/// an error or a limit can end it first, and then the next turn renders the message as
/// history). A retry of that steer answers the same `active` while the run is live and that
/// run's `finished` once it has ended; it never starts a turn.
///
/// A steer can also come back `pending`. The running turn stops taking steers the moment it
/// decides it will render no further step (it is about to write its terminal or pause), and a
/// steer arriving after that point has no running turn to join. It is then treated like any
/// send with no running turn: durably queued as the next turn, answered `pending` with its
/// submission id, and `active` under its own run when it starts. With no run active at all a
/// steer starts a turn and answers `active` for that new run.
///
/// A session whose last run is PAUSED answers no new message with any of these states: every
/// send that is not the paused run's own retry, whatever its `delivery` other than interrupt,
/// is refused `run_paused` and nothing is written. The caller re-authenticates and resends the
/// paused run's own `send_id` (answered `active` for that run, which resumes), cancels it, or
/// sends with `delivery: "interrupt"`. An interrupt answers `pending`
/// with the submission its message was queued as, and that message becomes `active` under its
/// own run when it starts, exactly like a queued send.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SendResult {
    Paused {
        run_id: String,
        /// An unfamiliar reason from a newer runner reads as `Unknown`, never as a known one.
        pause_reason: broca_protocol::OpenPauseReason,
    },
    Active {
        run_id: String,
    },
    Finished {
        run_id: String,
        /// An unfamiliar reason from a newer runner reads as `Unknown`, never as `error`.
        reason: broca_protocol::OpenRunFinishReason,
    },
    Pending {
        submission_id: String,
    },
    /// A newer runner state this client does not interpret; callers keep it non-actionable.
    #[serde(other)]
    Unknown,
}

/// Serialize `message` as callers read it: every `text_with_state` block becomes a plain `text`
/// block with the same text, the model's text is cleaned where its writing run cleans it, and
/// every other block is written unchanged.
fn serialize_caller_message<S: serde::Serializer>(
    message: &broca_protocol::Message,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    use broca_protocol::ContentBlock;
    // The model's text as every exposure shows it: cleaned of tag notation where the run that
    // wrote it cleans (llm-runner §9.6). The stored message keeps the raw text.
    let exposed = broca_protocol::tags::exposed_message(message);
    let message: &broca_protocol::Message = &exposed;
    if !message
        .content
        .iter()
        .any(|block| matches!(block, ContentBlock::TextWithState { .. }))
    {
        return message.serialize(serializer);
    }
    CallerMessageRef(message).serialize(serializer)
}

struct CallerMessageRef<'a>(&'a broca_protocol::Message);

impl serde::Serialize for CallerMessageRef<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;

        let mut fields = 2;
        fields += usize::from(self.0.cache_prefix_blocks.is_some());
        fields += usize::from(self.0.origin.is_some());
        let mut state = serializer.serialize_struct("Message", fields)?;
        state.serialize_field("role", &self.0.role)?;
        state.serialize_field("content", &CallerContentRef(&self.0.content))?;
        if let Some(cache_prefix_blocks) = self.0.cache_prefix_blocks {
            state.serialize_field("cache_prefix_blocks", &cache_prefix_blocks)?;
        }
        if let Some(origin) = &self.0.origin {
            state.serialize_field("origin", origin)?;
        }
        state.end()
    }
}

struct CallerContentRef<'a>(&'a [broca_protocol::ContentBlock]);

impl serde::Serialize for CallerContentRef<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;

        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for block in self.0 {
            sequence.serialize_element(&CallerBlockRef(block))?;
        }
        sequence.end()
    }
}

struct CallerBlockRef<'a>(&'a broca_protocol::ContentBlock);

impl serde::Serialize for CallerBlockRef<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use broca_protocol::ContentBlock;
        use serde::ser::SerializeStruct;

        match self.0 {
            ContentBlock::TextWithState { text, .. } => {
                let mut state = serializer.serialize_struct("ContentBlock", 2)?;
                state.serialize_field("type", "text")?;
                state.serialize_field("text", text)?;
                state.end()
            }
            block => block.serialize(serializer),
        }
    }
}

/// One message in a `session.read` page.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct SessionReadMessage {
    pub ordinal: u64,
    /// Message-coordinate spelling used by consumers for stable message badges.
    pub mid: String,
    /// Written in the caller-facing vocabulary: assistant text that carries provider state
    /// (`text_with_state`) goes out as a plain `text` block. The state is opaque bytes broca
    /// keeps only to send back to the provider; a caller reading the transcript wants the text.
    #[serde(serialize_with = "serialize_caller_message")]
    pub message: broca_protocol::Message,
    /// The producing run, not the page's latest run. Historical/imported messages with
    /// no recoverable producer omit this member; legacy replies without it still decode.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_run_attribution"
    )]
    pub run: Option<broca_core::RunAttribution>,
    /// One entry per tool call the message carries, in its order: what broca recorded about
    /// each call's dispatch. Absent on a message with no tool call.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<SessionReadToolCall>,
    /// The message's runner tags: one row per tagged part, in block order. Absent on a message
    /// of a session admitted before tags existed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<SessionReadTag>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivered_as: Option<cortexkit_role_llm_runner::delivery_order::EntryDelivery>,
    /// The content identity of the whole message exactly as an inline read serves it
    /// (`content_digest`, JCS SHA-256). Served on every entry of a descriptors read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
    /// The content identity of each top-level part, in message order, keyed as the tag rows
    /// key them. Served beside `digest`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub part_digests: Option<Vec<cortexkit_role_llm_runner::read::descriptors::PartDigest>>,
    /// The message as a descriptors read serves it, large payloads replaced by refs in place.
    /// When present it is what `message` serializes as; never decoded.
    #[serde(skip)]
    pub described: Option<serde_json::Value>,
}

impl Serialize for SessionReadMessage {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        /// The message member: the described form when a descriptors read made one, else the
        /// message as callers read it.
        struct Body<'a>(&'a broca_protocol::Message, Option<&'a serde_json::Value>);
        impl Serialize for Body<'_> {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                match self.1 {
                    Some(described) => described.serialize(serializer),
                    None => serialize_caller_message(self.0, serializer),
                }
            }
        }
        // The member order and omissions are the ones the derived form always wrote, so an
        // inline read keeps its exact bytes.
        #[derive(Serialize)]
        struct Wire<'a> {
            ordinal: u64,
            mid: &'a str,
            message: Body<'a>,
            #[serde(
                skip_serializing_if = "Option::is_none",
                serialize_with = "serialize_run_attribution"
            )]
            run: &'a Option<broca_core::RunAttribution>,
            #[serde(skip_serializing_if = "<[_]>::is_empty")]
            tool_calls: &'a [SessionReadToolCall],
            #[serde(skip_serializing_if = "<[_]>::is_empty")]
            tags: &'a [SessionReadTag],
            #[serde(skip_serializing_if = "Option::is_none")]
            delivered_as: &'a Option<cortexkit_role_llm_runner::delivery_order::EntryDelivery>,
            #[serde(skip_serializing_if = "Option::is_none")]
            digest: &'a Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            part_digests: &'a Option<Vec<cortexkit_role_llm_runner::read::descriptors::PartDigest>>,
        }
        Wire {
            ordinal: self.ordinal,
            mid: &self.mid,
            message: Body(&self.message, self.described.as_ref()),
            run: &self.run,
            tool_calls: &self.tool_calls,
            tags: &self.tags,
            delivered_as: &self.delivered_as,
            digest: &self.digest,
            part_digests: &self.part_digests,
        }
        .serialize(serializer)
    }
}

impl SessionReadMessage {
    /// Serve this entry as a descriptors read does (llm-runner §4.2): its content identities
    /// over the full inline message, and large payloads replaced by refs in place, largest first
    /// past the fixed per-message budget. Selection depends only on this message, so every
    /// read of it on one lineage gives the same bytes. `Err` when the message's structure alone
    /// exceeds the budget, which a descriptors read refuses `reply_too_large`.
    pub fn describe(&mut self) -> Result<(), u64> {
        use cortexkit_role_llm_runner::read::descriptors::{
            describe, PartDigest, Payload, Reference, DESCRIPTOR_MESSAGE_MAX,
        };
        self.described = None;
        let inline = serde_json::to_value(CallerMessage(&self.message))
            .expect("a stored message serializes");
        self.digest = Some(cortexkit_role_llm_runner::content_digest::content_digest(
            &inline,
        ));
        let parts = inline
            .get("content")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default();
        self.part_digests = Some(
            parts
                .iter()
                .enumerate()
                .map(|(index, part)| {
                    PartDigest::new(broca_protocol::tags::part_key(self.ordinal, index), part)
                })
                .collect(),
        );
        let mut payloads = Vec::new();
        for (index, part) in parts.iter().enumerate() {
            let at = |field: &str| format!("/content/{index}/{field}");
            let text = |value: &serde_json::Value| value.as_str().map(str::to_owned);
            match part.get("type").and_then(serde_json::Value::as_str) {
                Some("text") => {
                    if let Some(text) = text(&part["text"]) {
                        payloads.push((at("text"), Reference::text(None, &text)));
                    }
                }
                Some("reasoning") => {
                    if let Some(text) = text(&part["text"]) {
                        payloads.push((at("text"), Reference::reasoning(None, text.as_bytes())));
                    }
                }
                Some("redacted_reasoning") => {
                    if let Some(data) = text(&part["data"]) {
                        payloads.push((at("data"), Reference::reasoning(None, data.as_bytes())));
                    }
                }
                Some("tool_call") => {
                    let name = part["tool_name"].as_str().unwrap_or_default();
                    let bytes = serde_json::to_vec(&part["input"]).expect("JSON");
                    payloads.push((at("input"), Reference::arguments(None, name, &bytes)));
                }
                Some("tool_result") => {
                    let output = &part["output"];
                    for field in ["text", "message", "reason"] {
                        if let Some(text) = text(&output[field]) {
                            payloads.push((
                                at(&format!("output/{field}")),
                                Reference::text(None, &text),
                            ));
                        }
                    }
                    if let Some(value) = output.get("value") {
                        let encoded = serde_json::to_string(value).expect("JSON");
                        payloads.push((at("output/value"), Reference::text(None, &encoded)));
                    }
                }
                _ => {}
            }
        }
        let payloads: Vec<Payload> = payloads
            .into_iter()
            .map(|(pointer, reference)| Payload {
                pointer,
                reference,
                required: false,
            })
            .collect();
        // The budget is the fixed per-message bound less this entry's envelope, measured with
        // the message itself left out.
        let envelope = serialized_json_len(&*self)
            .saturating_sub(serde_json::to_vec(&inline).expect("JSON").len());
        let budget = DESCRIPTOR_MESSAGE_MAX.saturating_sub(envelope);
        match describe(&inline, &payloads, budget) {
            Ok(described) => {
                if described != inline {
                    self.described = Some(described);
                }
                Ok(())
            }
            Err(_) => Err(self.ordinal),
        }
    }
}

/// A message as callers read it, for serializing outside a read entry.
struct CallerMessage<'a>(&'a broca_protocol::Message);

impl Serialize for CallerMessage<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serialize_caller_message(self.0, serializer)
    }
}

/// The exposed content of part `index` of `message` as a descriptors `session.resolve` serves
/// it, `range` already applied to `text` (a text part's exposed text): text over the inline
/// threshold becomes a `text_ref` keyed by the part; any other part is served as its stored
/// structure with large payloads replaced by refs.
pub fn describe_resolved_part(
    message: &broca_protocol::Message,
    ordinal: u64,
    index: usize,
    text: String,
) -> serde_json::Value {
    use cortexkit_role_llm_runner::read::descriptors::{Reference, PART_INLINE_MAX};
    let part = broca_protocol::tags::part_key(ordinal, index);
    let is_text = matches!(
        message.content.get(index),
        Some(
            broca_protocol::ContentBlock::Text { .. }
                | broca_protocol::ContentBlock::TextWithState { .. }
        )
    );
    if is_text {
        return if text.len() > PART_INLINE_MAX {
            serde_json::to_value(Reference::text(Some(part), &text)).expect("JSON")
        } else {
            serde_json::Value::String(text)
        };
    }
    // A single-part message holding only this part, described as a read describes it, then
    // its one part taken back out.
    let single = broca_protocol::Message::new(
        message.role,
        message.content.get(index).cloned().into_iter().collect(),
    );
    let mut entry = SessionReadMessage {
        ordinal,
        mid: format!("m{ordinal}"),
        message: single,
        run: None,
        tool_calls: Vec::new(),
        tags: Vec::new(),
        delivered_as: None,
        digest: None,
        part_digests: None,
        described: None,
    };
    let _ = entry.describe();
    let body = entry
        .described
        .unwrap_or_else(|| serde_json::to_value(CallerMessage(&entry.message)).expect("JSON"));
    body.get("content")
        .and_then(|content| content.get(0))
        .cloned()
        .unwrap_or(serde_json::Value::Null)
}

fn serialized_json_len(value: &impl Serialize) -> usize {
    serde_json::to_vec(value).map_or(0, |bytes| bytes.len())
}

fn serialize_run_attribution<S: serde::Serializer>(
    run: &Option<broca_core::RunAttribution>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    run.as_ref()
        .map(|run| cortexkit_role_llm_runner::read::RunAttribution {
            run_id: run.run_id.clone(),
            episode: run.episode.clone(),
            is_final: run.is_final,
        })
        .serialize(serializer)
}

pub use cortexkit_role_llm_runner::tags::PartTag as SessionReadTag;

/// The tag rows of `message`, the transcript message at `ordinal`; empty when it has no tags.
pub fn tag_rows(ordinal: u64, message: &broca_protocol::Message) -> Vec<SessionReadTag> {
    message
        .part_tags()
        .map(|tags| {
            tags.parts
                .iter()
                .enumerate()
                .map(|(index, tag)| {
                    let part = broca_protocol::tags::part_key(ordinal, index);
                    match tag {
                        Some(tag) => {
                            SessionReadTag::tagged(part, broca_protocol::tags::tag_mark(*tag))
                        }
                        None => SessionReadTag::untagged(part),
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The lineage state returned beside a `session.read` page.
///
/// EVERY FIELD HERE DESCRIBES THE LAST RUN, NOT THE PAGE. A lineage accumulates episodes, so a
/// page can carry messages from several runs while this block reports only the most recent one —
/// `last_run_id` names the scope the rest inherit. Rendering `state`/`reason` as a property of the
/// transcript, or of any message on it, is a claim this data does not support: an earlier episode
/// that hit `max_steps` sits under a `completed` state and reads as finished.
///
/// Attribute the claim to the run: "the last run ended: max_steps" is true at every page size and
/// needs no assumption about how many episodes a session holds. Per-message completion is not
/// available on this page today; a consumer needing it must follow `session.subscribe`, where the
/// terminal and the assembled message ride the same ordered stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionReadLineageState {
    /// The run the fields below describe — the most recent episode in the lineage, which is not
    /// necessarily the run that produced the last message on the page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run_id: Option<String>,
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<SharedProviderError>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
}

/// A `session.read` response page.
///
/// `head` is the last durable control unit covered by the same snapshot as this page. A fresh or
/// empty lineage omits it. `session.subscribe` with `from: head` starts strictly after it, so the
/// handoff has neither a gap nor a duplicate. `lineage_id` is the opaque durable WAL identity;
/// legacy lineages omit it until their next write.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionReadResponse {
    pub messages: Vec<SessionReadMessage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_from_ordinal: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head: Option<CursorWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage_id: Option<String>,
    pub lineage_state: SessionReadLineageState,
    /// Present only when the request set `include_tools` and the session has an episode: the
    /// tool list frozen on the durable `RunStarted` of the session's LATEST episode, in its
    /// recorded order. Absent (never `[]`) for a session with no episode, so "no episode" and
    /// "an episode admitted with no tools" stay distinguishable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<SessionReadTool>>,
    /// The run whose `RunStarted` supplied `tools`; present exactly when `tools` is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tools_run_id: Option<String>,
    /// Prepared calls and their durable per-call states, including calls in an open batch.
    /// Omitted when the read source cannot report approval metadata; `[]` means none exist.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deferred_calls: Option<Vec<DeferredCallView>>,
    /// Late answers and isolated execution outcomes, outside the conversation prompt.
    /// Omitted when not reported by the read source; `[]` means there are no late answers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub late_approvals: Option<Vec<LateApprovalView>>,
    /// The last episode's open batch, or an explicit `none`. Omitted when not reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub open_batch: Option<OpenBatchView>,
    /// Continue late approvals using `session.read {late_after: next_late_after}`. Each
    /// entry is atomic: one entry larger than `max_bytes` is returned whole, alone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_late_after: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum OpenBatchView {
    None,
    NotReported {
        reason: String,
    },
    Open {
        batch_id: String,
        step_id: u64,
        calls: Vec<OpenBatchCall>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenBatchCall {
    pub call_id: String,
    pub state: broca_core::OpenCallState,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeferredCallView {
    pub call_key: String,
    pub batch_id: String,
    pub call_id: String,
    pub state: broca_core::OpenCallState,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LateApprovalView {
    pub call_key: String,
    pub elicitation_id: String,
    pub decision: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<broca_protocol::ToolResult>,
}

/// The typed invalid-params body for an unavailable late-approval cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvalidLateCursor {
    pub code: String,
    pub message: String,
    pub detail: crate::FieldDetail,
}
impl InvalidLateCursor {
    pub fn new(cursor: &str) -> Self {
        Self {
            code: crate::INVALID_PARAMS.into(),
            message: format!("unknown late_after cursor '{cursor}'"),
            detail: crate::FieldDetail {
                field: "late_after".into(),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AwaitingApproval {
    pub batch_id: String,
    pub pending: Vec<PendingApproval>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingApproval {
    pub call_key: String,
    pub call_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub elicitation_id: Option<String>,
    pub expires_at_ms: u64,
}

#[cfg(test)]
#[path = "../tests/support/run_usage.rs"]
mod usage_compat;

#[cfg(test)]
mod approval_wire_tests {
    use super::*;
    use sha2::{Digest, Sha256};

    #[test]
    fn every_run_status_golden_preserves_existing_fields() {
        let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden");
        let mut checked = 0;
        for entry in std::fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if !path
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("run_status.")
            {
                continue;
            }
            let bytes = std::fs::read(&path).unwrap();
            let golden = std::str::from_utf8(&bytes).unwrap().trim_end();
            let value: serde_json::Value = serde_json::from_str(golden).unwrap();
            let reply = value.get("result").unwrap_or(&value);
            // Decoded as the whole `run.status` answer, so the answers for a queued send's run
            // id (`pending`, or a joined run's status with its `delivered` receipt) are checked
            // the same way.
            let status: RunStatusReply = serde_json::from_value(reply.clone()).unwrap();
            let encoded = serde_json::to_vec(&status).unwrap();
            let expected = serde_json::to_vec(reply).unwrap();
            let normalized: serde_json::Value =
                serde_json::from_slice(&crate::usage_compat::legacy_bytes(&encoded, &expected))
                    .unwrap();
            // New vectors pin the full usage object; legacy fixtures pin their original fields.
            if path
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("run_status.usage_")
            {
                assert_eq!(
                    serde_json::to_value(status).unwrap(),
                    *reply,
                    "{}",
                    path.display()
                );
            } else {
                assert_eq!(normalized, *reply, "{}", path.display());
            }
            checked += 1;
        }
        assert!(
            checked >= 25,
            "expected the full run.status golden inventory, got {checked}"
        );
        println!("checked {checked} run.status goldens");
    }

    #[test]
    fn active_awaiting_approval_golden() {
        let response = RunStatusResponse::Active {
            auth_selection: None,
            reason: None,
            deadline_ms: None,
            usage: None,
            step: None,
            title: None,
            awaiting_approval: Some(AwaitingApproval {
                batch_id: "r-b1".into(),
                pending: vec![
                    PendingApproval {
                        call_key: "5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:7".into(),
                        call_id: "deferred".into(),
                        elicitation_id: Some("el-1".into()),
                        expires_at_ms: 1799999999999,
                    },
                    PendingApproval {
                        call_key: "5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:8".into(),
                        call_id: "unfiled".into(),
                        elicitation_id: None,
                        expires_at_ms: 1799999999999,
                    },
                ],
            }),
        };
        let golden = include_str!("../tests/golden/run_status.awaiting_approval.json").trim();
        assert_eq!(
            format!("{:x}", Sha256::digest(golden)),
            "5ddfce8768e1e5e53fa1d0c08a599190ffadfd1fc08182491eab7f2d23b6e8bf"
        );
        assert_eq!(serde_json::to_string(&response).unwrap(), golden);
        assert_eq!(
            serde_json::from_str::<RunStatusResponse>(golden).unwrap(),
            response
        );
    }

    #[test]
    fn session_read_call_states_and_late_approvals_golden() {
        let mut response: SessionReadResponse = serde_json::from_str(
            r#"{"messages":[],"lineage_state":{"last_run_id":"r","state":"active"}}"#,
        )
        .unwrap();
        response.deferred_calls = Some(
            [
                ("l:7", "deferred", broca_core::OpenCallState::Deferred),
                ("l:8", "approved", broca_core::OpenCallState::Approved),
                ("l:9", "sent", broca_core::OpenCallState::Dispatched),
                ("l:10", "closed", broca_core::OpenCallState::Terminated),
            ]
            .into_iter()
            .map(|(key, id, state)| DeferredCallView {
                call_key: key.into(),
                batch_id: "r-b1".into(),
                call_id: id.into(),
                state,
            })
            .collect(),
        );
        response.late_approvals = Some(vec![LateApprovalView {
            call_key: "l:10".into(),
            elicitation_id: "el-4".into(),
            decision: "approved".into(),
            outcome: Some("unknown".into()),
            result: None,
        }]);
        response.open_batch = Some(OpenBatchView::Open {
            batch_id: "r-b1".into(),
            step_id: 1,
            calls: [
                ("deferred", broca_core::OpenCallState::Deferred),
                ("approved", broca_core::OpenCallState::Approved),
                ("sent", broca_core::OpenCallState::Dispatched),
                ("closed", broca_core::OpenCallState::Terminated),
                ("unsent", broca_core::OpenCallState::NotSent),
            ]
            .into_iter()
            .map(|(id, state)| OpenBatchCall {
                call_id: id.into(),
                state,
            })
            .collect(),
        });
        let golden = include_str!("../tests/golden/session_read.approvals.json").trim();
        assert_eq!(
            format!("{:x}", Sha256::digest(golden)),
            "4ba5ba8ff1223d968a15ffe51b27c01bc09e1eb0376295aaadaef8ee3728ef5e"
        );
        assert_eq!(serde_json::to_string(&response).unwrap(), golden);
        assert_eq!(
            serde_json::from_str::<SessionReadResponse>(golden).unwrap(),
            response
        );
    }

    #[test]
    fn an_unreported_approval_view_is_not_an_empty_view() {
        let mut response: SessionReadResponse =
            serde_json::from_str(r#"{"messages":[],"lineage_state":{"state":"idle"}}"#).unwrap();
        let unknown = serde_json::to_value(&response).unwrap();
        assert!(unknown.get("deferred_calls").is_none());
        assert!(unknown.get("late_approvals").is_none());
        assert!(unknown.get("open_batch").is_none());
        response.open_batch = Some(OpenBatchView::None);
        response.deferred_calls = Some(Vec::new());
        response.late_approvals = Some(Vec::new());
        let empty = serde_json::to_value(&response).unwrap();
        assert_eq!(empty["open_batch"], serde_json::json!({"state":"none"}));
        assert_eq!(empty["deferred_calls"], serde_json::json!([]));
        assert_eq!(empty["late_approvals"], serde_json::json!([]));
    }
}

/// One tool as a session's episode declared it, copied from the durable `RunStarted`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionReadTool {
    pub name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
    /// The module the tool was routed to, when the episode recorded one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub module: Option<String>,
}

impl From<&broca_protocol::request::Tool> for SessionReadTool {
    fn from(tool: &broca_protocol::request::Tool) -> Self {
        Self {
            name: tool.name.clone(),
            description: tool.description.clone(),
            input_schema: tool.input_schema.clone(),
            module: tool.module.clone(),
        }
    }
}

pub type ReadResult = SessionReadResponse;

/// `vault.audit` request parameters: none. An absent, null or empty params object all mean the
/// same audit; an unknown field is refused so a caller expecting an option that does not exist
/// learns so.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VaultAuditParams {}

/// The `vault.audit` answer: what the credential vault lists under broca's read grants,
/// set against the credentials broca's configuration routes to.
///
/// `Unanswered` is its own variant, never an empty `Checked`: a listing that did not arrive
/// says nothing about whether a credential vanished, and must not read as "nothing did".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum VaultAuditResponse {
    /// The vault answered `credential.list_scoped`.
    Checked {
        /// The grant principal the listing was computed for, for example `reserved:broca`.
        principal: String,
        /// The vault's digest of the listed rows and grants; equal digests mean nothing changed.
        view: String,
        /// Every row the vault listed, in its order, including rows that need re-authentication.
        rows: Vec<VaultAuditRow>,
        /// Every credential broca's configuration routes to that the listing did NOT include:
        /// the credential was removed, renamed, or is no longer covered by broca's grants.
        missing: Vec<VaultAuditMissing>,
    },
    /// The audit could not get a listing, so it knows nothing about any credential.
    Unanswered { reason: String },
}

/// One credential the vault listed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VaultAuditRow {
    pub credential_id: String,
    /// The vault's record kind, for example `oauth` or `apikey`.
    #[serde(rename = "type")]
    pub credential_type: String,
    /// The vault's lifecycle state, for example `active` or `needs_reauth`, verbatim.
    pub state: String,
    /// The provider account, where the vault captured one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
    /// The broca routing keys that resolve to this credential; empty when broca's
    /// configuration does not route to it.
    #[serde(default)]
    pub referenced_by: Vec<String>,
}

/// A credential broca's configuration routes to that the vault did not list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VaultAuditMissing {
    pub credential_id: String,
    /// The broca routing keys that resolve to it.
    pub referenced_by: Vec<String>,
}

/// Non-secret authentication identity frozen for a paused run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PausedRunAuth {
    pub credential_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_ref: Option<String>,
    /// Absent on older selections that froze no account decision; unknown paths stay verbatim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

/// A run-keyed status response resolved from the live actor, durable index, or WAL.
/// `auth_selection` is the credential id frozen at admission, never a current routing
/// decision. It is absent when no credential was frozen or the frozen config is unavailable.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RunStatusResponse {
    Active {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth_selection: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        deadline_ms: Option<u64>,
        #[serde(
            default,
            deserialize_with = "crate::role::deserialize_optional_run_usage",
            skip_serializing_if = "Option::is_none"
        )]
        usage: Option<broca_protocol::usage::RunUsage>,
        /// Snapshot of the live actor's in-flight model step; never read from WAL.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        step: Option<broca_protocol::provider::InFlightStep>,
        /// The title the run's send carried, as its WAL record holds it. Absent (never empty)
        /// when the send carried none or the run predates titles.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        /// The pending list contains only durably Deferred calls; an older `awaiting_approval`
        /// WAL marker may supply filing IDs but cannot keep an approved or closed call pending.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        awaiting_approval: Option<AwaitingApproval>,
    },
    Paused {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth_selection: Option<String>,
        /// Explicit restart-pause discriminator; authentication-pause replies omit this field to preserve their existing wire shape.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pause_reason: Option<broca_protocol::OpenPauseReason>,
        #[serde(
            default,
            deserialize_with = "crate::role::deserialize_optional_run_usage",
            skip_serializing_if = "Option::is_none"
        )]
        usage: Option<broca_protocol::usage::RunUsage>,
        #[serde(default)]
        retries_used: u32,
        /// Why the run paused, spelled as `session.read` spells `lineage_state.reason`
        /// (for example `auth_required`). Absent when the pause record is unavailable.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
        /// The typed error behind the pause, the same value `session.read` reports in
        /// `lineage_state.error`. Absent when the pause record carries none.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<SharedProviderError>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth: Option<PausedRunAuth>,
        /// The title the run's send carried, as its WAL record holds it. Absent (never empty)
        /// when the send carried none or the run predates titles.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
    },
    Interrupted {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth_selection: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
        /// Diagnostic cause of the interruption, when the terminal records one. Its presence
        /// does not change the run's interrupted state into an error state.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<SharedProviderError>,
        #[serde(
            default,
            deserialize_with = "broca_protocol::content::deserialize_optional_caller_finish_reason",
            skip_serializing_if = "Option::is_none"
        )]
        final_step_finish_reason: Option<FinishReason>,
        /// Steer send ids durable in this episode whose input was not included in a model
        /// step, in WAL order. A present empty list means WAL replay found none;
        /// the field is omitted when no WAL was read.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        undelivered_steers: Option<Vec<String>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        indeterminate_tool_calls: Option<Vec<ToolCallId>>,
        #[serde(
            default,
            deserialize_with = "crate::role::deserialize_optional_run_usage",
            skip_serializing_if = "Option::is_none"
        )]
        usage: Option<broca_protocol::usage::RunUsage>,
        #[serde(default)]
        retries_used: u32,
        /// The title the run's send carried, as its WAL record holds it. Absent (never empty)
        /// when the send carried none or the run predates titles.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        /// On a run admitted with structured output: the same structured-output result the
        /// `run.result` answer for this run carries (`RunResultResponse::structured_output`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        structured_output: Option<broca_protocol::structured_output::StructuredOutputResult>,
    },
    Completed {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth_selection: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        directives: Option<Vec<cortexkit_role_llm_runner::directives::Directive>>,
        #[serde(
            default,
            deserialize_with = "broca_protocol::content::deserialize_optional_caller_finish_reason",
            skip_serializing_if = "Option::is_none"
        )]
        final_step_finish_reason: Option<FinishReason>,
        /// Steer send ids durable in this episode whose input was not included in a model
        /// step, in WAL order. A present empty list means WAL replay found none;
        /// the field is omitted when no WAL was read.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        undelivered_steers: Option<Vec<String>>,
        #[serde(
            default,
            deserialize_with = "crate::role::deserialize_optional_run_usage",
            skip_serializing_if = "Option::is_none"
        )]
        usage: Option<broca_protocol::usage::RunUsage>,
        #[serde(default)]
        retries_used: u32,
        /// The title the run's send carried, as its WAL record holds it. Absent (never empty)
        /// when the send carried none or the run predates titles.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        /// On a run admitted with structured output: the same structured-output result the
        /// `run.result` answer for this run carries (`RunResultResponse::structured_output`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        structured_output: Option<broca_protocol::structured_output::StructuredOutputResult>,
    },
    MaxSteps {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth_selection: Option<String>,
        #[serde(
            default,
            deserialize_with = "broca_protocol::content::deserialize_optional_caller_finish_reason",
            skip_serializing_if = "Option::is_none"
        )]
        final_step_finish_reason: Option<FinishReason>,
        /// Steer send ids durable in this episode whose input was not included in a model
        /// step, in WAL order. A present empty list means WAL replay found none;
        /// the field is omitted when no WAL was read.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        undelivered_steers: Option<Vec<String>>,
        #[serde(
            default,
            deserialize_with = "crate::role::deserialize_optional_run_usage",
            skip_serializing_if = "Option::is_none"
        )]
        usage: Option<broca_protocol::usage::RunUsage>,
        #[serde(default)]
        retries_used: u32,
        /// The title the run's send carried, as its WAL record holds it. Absent (never empty)
        /// when the send carried none or the run predates titles.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        /// On a run admitted with structured output: the same structured-output result the
        /// `run.result` answer for this run carries (`RunResultResponse::structured_output`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        structured_output: Option<broca_protocol::structured_output::StructuredOutputResult>,
    },
    BudgetExhausted {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth_selection: Option<String>,
        detail: broca_protocol::BudgetExhaustedDetail,
        #[serde(
            default,
            deserialize_with = "broca_protocol::content::deserialize_optional_caller_finish_reason",
            skip_serializing_if = "Option::is_none"
        )]
        final_step_finish_reason: Option<FinishReason>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        undelivered_steers: Option<Vec<String>>,
        #[serde(
            default,
            deserialize_with = "crate::role::deserialize_optional_run_usage",
            skip_serializing_if = "Option::is_none"
        )]
        usage: Option<broca_protocol::usage::RunUsage>,
        #[serde(default)]
        retries_used: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        /// On a run admitted with structured output: the same structured-output result the
        /// `run.result` answer for this run carries (`RunResultResponse::structured_output`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        structured_output: Option<broca_protocol::structured_output::StructuredOutputResult>,
    },
    ContextCeiling {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth_selection: Option<String>,
        context_ceiling: broca_protocol::ContextCeilingDetail,
        #[serde(
            default,
            deserialize_with = "broca_protocol::content::deserialize_optional_caller_finish_reason",
            skip_serializing_if = "Option::is_none"
        )]
        final_step_finish_reason: Option<FinishReason>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        undelivered_steers: Option<Vec<String>>,
        #[serde(
            default,
            deserialize_with = "crate::role::deserialize_optional_run_usage",
            skip_serializing_if = "Option::is_none"
        )]
        usage: Option<broca_protocol::usage::RunUsage>,
        #[serde(default)]
        retries_used: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        structured_output: Option<broca_protocol::structured_output::StructuredOutputResult>,
    },
    Cancelled {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth_selection: Option<String>,
        #[serde(
            default,
            deserialize_with = "broca_protocol::content::deserialize_optional_caller_finish_reason",
            skip_serializing_if = "Option::is_none"
        )]
        final_step_finish_reason: Option<FinishReason>,
        /// Steer send ids durable in this episode whose input was not included in a model
        /// step, in WAL order. A present empty list means WAL replay found none;
        /// the field is omitted when no WAL was read.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        undelivered_steers: Option<Vec<String>>,
        #[serde(
            default,
            deserialize_with = "crate::role::deserialize_optional_run_usage",
            skip_serializing_if = "Option::is_none"
        )]
        usage: Option<broca_protocol::usage::RunUsage>,
        #[serde(default)]
        retries_used: u32,
        /// The title the run's send carried, as its WAL record holds it. Absent (never empty)
        /// when the send carried none or the run predates titles.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        /// On a run admitted with structured output: the same structured-output result the
        /// `run.result` answer for this run carries (`RunResultResponse::structured_output`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        structured_output: Option<broca_protocol::structured_output::StructuredOutputResult>,
    },
    Error {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth_selection: Option<String>,
        #[serde(
            default,
            deserialize_with = "broca_protocol::content::deserialize_optional_caller_finish_reason",
            skip_serializing_if = "Option::is_none"
        )]
        final_step_finish_reason: Option<FinishReason>,
        /// Steer send ids durable in this episode whose input was not included in a model
        /// step, in WAL order. A present empty list means WAL replay found none;
        /// the field is omitted when no WAL was read.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        undelivered_steers: Option<Vec<String>>,
        #[serde(
            default,
            deserialize_with = "crate::role::deserialize_optional_run_usage",
            skip_serializing_if = "Option::is_none"
        )]
        usage: Option<broca_protocol::usage::RunUsage>,
        #[serde(default)]
        retries_used: u32,
        /// The typed error the run ended with, the same value `session.read` reports in
        /// `lineage_state.error`. Absent when the terminal record carries none.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<SharedProviderError>,
        /// The title the run's send carried, as its WAL record holds it. Absent (never empty)
        /// when the send carried none or the run predates titles.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        /// On a run admitted with structured output: the same structured-output result the
        /// `run.result` answer for this run carries (`RunResultResponse::structured_output`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        structured_output: Option<broca_protocol::structured_output::StructuredOutputResult>,
    },
    TransformUnavailable {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth_selection: Option<String>,
        #[serde(
            default,
            deserialize_with = "broca_protocol::content::deserialize_optional_caller_finish_reason",
            skip_serializing_if = "Option::is_none"
        )]
        final_step_finish_reason: Option<FinishReason>,
        /// Steer send ids durable in this episode whose input was not included in a model
        /// step, in WAL order. A present empty list means WAL replay found none;
        /// the field is omitted when no WAL was read.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        undelivered_steers: Option<Vec<String>>,
        #[serde(
            default,
            deserialize_with = "crate::role::deserialize_optional_run_usage",
            skip_serializing_if = "Option::is_none"
        )]
        usage: Option<broca_protocol::usage::RunUsage>,
        #[serde(default)]
        retries_used: u32,
        /// The title the run's send carried, as its WAL record holds it. Absent (never empty)
        /// when the send carried none or the run predates titles.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        /// On a run admitted with structured output: the same structured-output result the
        /// `run.result` answer for this run carries (`RunResultResponse::structured_output`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        structured_output: Option<broca_protocol::structured_output::StructuredOutputResult>,
    },
    /// Fallback for a newer state string; old-runner clients do not treat it as terminal.
    #[serde(other)]
    Unknown,
}

#[cfg(test)]
mod open_state_tests {
    use super::{
        decode_subscribe_event, AccountSelectionRefusedDetail, DisplayEvent, FlowPrincipalRefusal,
        QueuedSendState, RunPaused, RunPausedDetail, RunStatusResponse, SendResult, SubscribeEvent,
        UnrecognizedRunState,
    };
    use broca_protocol::{ControlUnit, OpenState, PauseReason, RunFinishReason, RunId};

    #[test]
    fn unknown_run_status_state_decodes_to_a_nonterminal_fallback() {
        let status: RunStatusResponse =
            serde_json::from_str(r#"{"state":"future_runner_state","extra":"future field"}"#)
                .unwrap();
        assert!(matches!(status, RunStatusResponse::Unknown));
    }

    #[test]
    fn run_status_auth_selection_is_optional_and_round_trips() {
        for state in [
            "active",
            "paused",
            "interrupted",
            "completed",
            "max_steps",
            "cancelled",
            "error",
            "transform_unavailable",
        ] {
            for selection in [None, Some("apikey:cline-pass")] {
                let mut wire = serde_json::json!({"state": state});
                if let Some(selection) = selection {
                    wire["auth_selection"] = selection.into();
                }
                let status: RunStatusResponse = serde_json::from_value(wire).unwrap();
                let encoded = serde_json::to_value(&status).unwrap();
                assert_eq!(
                    encoded.get("auth_selection").and_then(|v| v.as_str()),
                    selection
                );
                assert_eq!(encoded.get("auth_selection").is_some(), selection.is_some());
                assert_eq!(
                    serde_json::from_value::<RunStatusResponse>(encoded).unwrap(),
                    status
                );
            }
        }
    }

    #[test]
    fn run_result_auth_selection_is_optional_and_round_trips() {
        for selection in [None, Some("apikey:cline-pass")] {
            let mut wire = serde_json::json!({"run_id": "run-1", "state": "error"});
            if let Some(selection) = selection {
                wire["auth_selection"] = selection.into();
            }
            let result: super::RunResultResponse = serde_json::from_value(wire).unwrap();
            assert_eq!(result.auth_selection.as_deref(), selection);
            let encoded = serde_json::to_value(&result).unwrap();
            assert_eq!(encoded.get("auth_selection").is_some(), selection.is_some());
            assert_eq!(
                serde_json::from_value::<super::RunResultResponse>(encoded).unwrap(),
                result
            );
        }
    }

    #[test]
    fn unknown_send_state_decodes_without_closing_the_wire_vocabulary() {
        let result: SendResult = serde_json::from_str(r#"{"state":"future_send_state"}"#).unwrap();
        assert!(matches!(result, SendResult::Unknown));
    }

    #[test]
    fn unknown_queued_send_state_decodes_to_a_non_actionable_fallback() {
        let state: QueuedSendState =
            serde_json::from_str(r#"{"state":"future_queue_state"}"#).unwrap();
        assert_eq!(state, QueuedSendState::Unknown);
    }

    #[test]
    fn unknown_pause_and_finish_reasons_decode_as_explicit_unknown_states() {
        let status: RunStatusResponse =
            serde_json::from_str(r#"{"state":"paused","pause_reason":"future_pause"}"#).unwrap();
        let RunStatusResponse::Paused { pause_reason, .. } = &status else {
            panic!("expected a paused status, got {status:?}");
        };
        assert_eq!(
            pause_reason,
            &Some(OpenState::Unknown("future_pause".into()))
        );

        let paused: SendResult = serde_json::from_str(
            r#"{"state":"paused","run_id":"run-1","pause_reason":"future_pause"}"#,
        )
        .unwrap();
        assert_eq!(
            paused,
            SendResult::Paused {
                run_id: "run-1".into(),
                pause_reason: OpenState::Unknown("future_pause".into()),
            }
        );

        let finished: SendResult = serde_json::from_str(
            r#"{"state":"finished","run_id":"run-1","reason":"future_finish"}"#,
        )
        .unwrap();
        assert_eq!(
            finished,
            SendResult::Finished {
                run_id: "run-1".into(),
                reason: OpenState::Unknown("future_finish".into()),
            }
        );

        let refusal: RunPaused = serde_json::from_value(serde_json::json!({
            "code": "run_paused",
            "message": "paused",
            "detail": {"kind": "run_paused", "run_id": "run-1", "reason": "future_pause"}
        }))
        .unwrap();
        assert_eq!(
            refusal.detail,
            RunPausedDetail::RunPaused {
                run_id: "run-1".into(),
                reason: OpenState::Unknown("future_pause".into()),
            }
        );
    }

    #[test]
    fn known_pause_and_finish_reasons_keep_their_wire_bytes() {
        let paused = SendResult::Paused {
            run_id: "run-1".into(),
            pause_reason: PauseReason::AuthRequired.into(),
        };
        let bytes = r#"{"state":"paused","run_id":"run-1","pause_reason":"auth_required"}"#;
        assert_eq!(serde_json::to_string(&paused).unwrap(), bytes);
        assert_eq!(serde_json::from_str::<SendResult>(bytes).unwrap(), paused);

        let finished = SendResult::Finished {
            run_id: "run-1".into(),
            reason: RunFinishReason::Completed.into(),
        };
        let bytes = r#"{"state":"finished","run_id":"run-1","reason":"completed"}"#;
        assert_eq!(serde_json::to_string(&finished).unwrap(), bytes);
        assert_eq!(serde_json::from_str::<SendResult>(bytes).unwrap(), finished);

        let refusal = RunPaused::new("run-1".into(), PauseReason::Restart);
        let value = serde_json::to_value(&refusal).unwrap();
        assert_eq!(
            value["detail"],
            serde_json::json!({"kind": "run_paused", "run_id": "run-1", "reason": "restart"})
        );
    }

    #[test]
    fn subscribe_units_with_unknown_run_reasons_become_unrecognized_run_states() {
        let paused: SubscribeEvent = serde_json::from_value(serde_json::json!({
            "kind": "control",
            "cursor": {"wal_seq": 7, "sub_index": 1},
            "unit": {"type": "paused", "run_id": "run-1", "reason": "future_pause"}
        }))
        .unwrap();
        assert_eq!(
            paused,
            SubscribeEvent::UnrecognizedRunState {
                wal_seq: 7,
                sub_index: 1,
                state: UnrecognizedRunState::Paused {
                    run_id: RunId("run-1".into()),
                    reason: "future_pause".into(),
                },
            }
        );

        let body = br#"{"kind":"control","cursor":{"wal_seq":8,"sub_index":0},"unit":{"type":"run_finished","run_id":"run-1","reason":"future_finish","retries_used":0}}"#;
        let finished = decode_subscribe_event(body).unwrap();
        let SubscribeEvent::UnrecognizedRunState { state, .. } = &finished else {
            panic!("expected an unrecognized run state, got {finished:?}");
        };
        assert_eq!(state.run_id(), Some(&RunId("run-1".into())));
        assert_eq!(state.reason(), "future_finish");
        assert!(matches!(state, UnrecognizedRunState::Finished { .. }));
        assert_eq!(
            serde_json::to_value(&finished).unwrap(),
            serde_json::json!({
                "kind": "control",
                "cursor": {"wal_seq": 8, "sub_index": 0},
                "unit": {"type": "run_finished", "run_id": "run-1", "reason": "future_finish"}
            })
        );

        // A known reason still decodes as the ordinary control unit.
        let known = decode_subscribe_event(
            br#"{"kind":"control","cursor":{"wal_seq":9,"sub_index":0},"unit":{"type":"run_finished","run_id":"run-1","reason":"completed"}}"#,
        )
        .unwrap();
        assert!(matches!(
            known,
            SubscribeEvent::Control {
                unit: ControlUnit::RunFinished {
                    reason: RunFinishReason::Completed,
                    ..
                },
                ..
            }
        ));

        // A pause without the run id it must carry is refused even with an unknown reason, and
        // a unit with a known reason that fails to decode for another cause is still refused.
        let malformed = decode_subscribe_event(
            br#"{"kind":"control","cursor":{"wal_seq":10,"sub_index":0},"unit":{"type":"paused","reason":"future_pause"}}"#,
        );
        assert!(malformed.is_err(), "{malformed:?}");
        let malformed_known = decode_subscribe_event(
            br#"{"kind":"control","cursor":{"wal_seq":11,"sub_index":0},"unit":{"type":"run_finished","reason":"completed","retries_used":"x"}}"#,
        );
        assert!(malformed_known.is_err(), "{malformed_known:?}");
    }

    #[test]
    fn unknown_subscribe_envelopes_and_event_types_reset_display_state() {
        let envelope: SubscribeEvent =
            serde_json::from_str(r#"{"kind":"future_lane_event"}"#).unwrap();
        assert_eq!(
            envelope,
            SubscribeEvent::Display(DisplayEvent::DisplayReset)
        );

        let display: SubscribeEvent = serde_json::from_value(serde_json::json!({
            "kind": "display",
            "event": {"type": "future_display_event", "payload": "ignored"}
        }))
        .unwrap();
        assert_eq!(display, SubscribeEvent::Display(DisplayEvent::DisplayReset));

        let control: SubscribeEvent = serde_json::from_value(serde_json::json!({
            "kind": "control",
            "cursor": {"wal_seq": 1, "sub_index": 0},
            "unit": {"type": "future_control_unit", "payload": "ignored"}
        }))
        .unwrap();
        assert!(matches!(
            control,
            SubscribeEvent::Control {
                unit: broca_protocol::caller::ControlUnit::DisplayReset,
                ..
            }
        ));
    }

    #[test]
    fn refusal_classes_are_open_when_consumers_decode_them() {
        let mut principal =
            serde_json::to_value(FlowPrincipalRefusal::new(true, "reserved:agent")).unwrap();
        principal["class"] = serde_json::json!("future_class");
        let principal: FlowPrincipalRefusal = serde_json::from_value(principal).unwrap();
        assert_eq!(
            serde_json::to_value(&principal).unwrap()["class"],
            "future_class"
        );

        let account: AccountSelectionRefusedDetail = serde_json::from_value(serde_json::json!({
            "routing_code": "account_pool_exhausted",
            "class": "future_class"
        }))
        .unwrap();
        assert_eq!(account.class.as_str(), "future_class");
    }

    #[test]
    fn run_status_error_keeps_an_unrecognized_provider_class() {
        let status: RunStatusResponse = serde_json::from_value(serde_json::json!({
            "state": "error",
            "error": {"class": "future_class", "message": "new provider outcome"}
        }))
        .unwrap();
        let RunStatusResponse::Error {
            error: Some(error), ..
        } = status
        else {
            panic!("expected the error status")
        };
        assert_eq!(error.class_wire(), "future_class");
        assert!(error.has_unknown_class());
    }
}

impl RunStatusResponse {
    /// The run's recorded title, when the reply carries one.
    pub fn title(&self) -> Option<&str> {
        match self {
            Self::Active { title, .. }
            | Self::Paused { title, .. }
            | Self::Interrupted { title, .. }
            | Self::Completed { title, .. }
            | Self::MaxSteps { title, .. }
            | Self::BudgetExhausted { title, .. }
            | Self::ContextCeiling { title, .. }
            | Self::Cancelled { title, .. }
            | Self::Error { title, .. }
            | Self::TransformUnavailable { title, .. } => title.as_deref(),
            Self::Unknown => None,
        }
    }
}

/// The `run.status {run_id}` answer as it goes out on the wire.
///
/// Almost always a run's own status. A queued send's reply names only its submission id
/// (`sub-<run id>`), and callers poll `run.status` on the run id inside it, so broca answers
/// that id for as long as the send exists, even when the send never becomes a run of its own.
/// This is a broca extension beyond the llm-runner role contract, which answers an id that
/// names no run `unknown`:
///
/// - while the send is queued: `{"state": "pending"}`;
/// - once it was delivered into a run that was already going (as a steer, say): that run's
///   whole status, plus `delivered: {as: "step", ref: <that run's id>}`;
/// - once it started its own run: that run's status, which is an ordinary run status;
/// - once it ended undelivered: `{"state": "superseded"}`, `{"state": "expired"}` or
///   `{"state": "retracted"}`, for good.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum RunStatusReply {
    Run {
        #[serde(flatten)]
        status: Box<RunStatusResponse>,
        /// Set only when the asked id is a queued send delivered into another run: `status`
        /// is then that run's, and `ref` names it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delivered: Option<cortexkit_role_llm_runner::send::Delivered>,
    },
    Send(QueuedSendState),
}

impl<'de> Deserialize<'de> for RunStatusReply {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RunPayload {
            #[serde(flatten)]
            status: Box<RunStatusResponse>,
            #[serde(default)]
            delivered: Option<cortexkit_role_llm_runner::send::Delivered>,
        }

        let value = serde_json::Value::deserialize(deserializer)?;
        let state = value
            .get("state")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        if matches!(state, "pending" | "superseded" | "expired" | "retracted") {
            let send = serde_json::from_value::<QueuedSendState>(value)
                .map_err(serde::de::Error::custom)?;
            return Ok(Self::Send(send));
        }
        let run = serde_json::from_value::<RunPayload>(value).map_err(serde::de::Error::custom)?;
        Ok(Self::Run {
            status: run.status,
            delivered: run.delivered,
        })
    }
}

impl From<RunStatusResponse> for RunStatusReply {
    fn from(status: RunStatusResponse) -> Self {
        Self::Run {
            status: Box::new(status),
            delivered: None,
        }
    }
}

/// The state of a queued send that is not, or never became, a run: see [`RunStatusReply`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum QueuedSendState {
    /// Queued and not yet delivered.
    Pending,
    /// Replaced by a newer send under the same dedupe key; never delivered.
    Superseded,
    /// Its expiry passed before it was delivered; never delivered.
    Expired,
    /// Withdrawn by `session.retract` before it started.
    Retracted,
    #[serde(other)]
    Unknown,
}

/// One decoded subscribe-stream event. Keep its control payload by value: boxing it would change
/// the public Rust shape even though optional provider details make that variant larger.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum SubscribeEvent {
    Control {
        wal_seq: u64,
        sub_index: u32,
        unit: broca_protocol::caller::ControlUnit,
    },
    Display(DisplayEvent),
    /// A `paused` or `run_finished` control unit whose reason is newer than this client.
    ///
    /// The run did pause or end, so a caller waiting on it must stop waiting, but this client
    /// cannot say why. It is neither a failed run nor a re-authentication pause, and a caller
    /// must not report it as either.
    UnrecognizedRunState {
        wal_seq: u64,
        sub_index: u32,
        state: UnrecognizedRunState,
    },
}

/// A run pause or ending whose reason this client does not know, as the subscribe stream
/// carried it. See [`SubscribeEvent::UnrecognizedRunState`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnrecognizedRunState {
    /// The run paused, so it may still resume under the same run id.
    Paused {
        run_id: broca_protocol::RunId,
        reason: String,
    },
    /// The run ended. `run_id` is absent only when the producer predates run ids on
    /// terminals, exactly as on [`broca_protocol::ControlUnit::RunFinished`].
    Finished {
        run_id: Option<broca_protocol::RunId>,
        reason: String,
    },
}

impl UnrecognizedRunState {
    /// The run this pause or ending names, when the producer recorded one.
    pub fn run_id(&self) -> Option<&broca_protocol::RunId> {
        match self {
            Self::Paused { run_id, .. } => Some(run_id),
            Self::Finished { run_id, .. } => run_id.as_ref(),
        }
    }

    /// The reason exactly as the producer spelled it.
    pub fn reason(&self) -> &str {
        match self {
            Self::Paused { reason, .. } | Self::Finished { reason, .. } => reason,
        }
    }
}

/// The control unit an [`UnrecognizedRunState`] was read from, written back with only the
/// fields this client understood.
#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum UnrecognizedRunStateRef<'a> {
    Paused {
        run_id: &'a broca_protocol::RunId,
        reason: &'a str,
    },
    RunFinished {
        #[serde(skip_serializing_if = "Option::is_none")]
        run_id: Option<&'a broca_protocol::RunId>,
        reason: &'a str,
    },
}

impl<'a> From<&'a UnrecognizedRunState> for UnrecognizedRunStateRef<'a> {
    fn from(state: &'a UnrecognizedRunState) -> Self {
        match state {
            UnrecognizedRunState::Paused { run_id, reason } => Self::Paused { run_id, reason },
            UnrecognizedRunState::Finished { run_id, reason } => Self::RunFinished {
                run_id: run_id.as_ref(),
                reason,
            },
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum UnrecognizedSubscribeEventRef<'a> {
    Control {
        cursor: CursorWire,
        unit: UnrecognizedRunStateRef<'a>,
    },
}

/// The run id and reason of a `paused` or `run_finished` unit, read to find an unfamiliar
/// reason. Other fields are not checked: a newer runner that adds a reason may also change
/// them, and the caller only needs to know which run paused or ended.
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RunStateProbe {
    Paused {
        run_id: broca_protocol::RunId,
        reason: broca_protocol::OpenPauseReason,
    },
    RunFinished {
        #[serde(default)]
        run_id: Option<broca_protocol::RunId>,
        reason: broca_protocol::OpenRunFinishReason,
    },
    #[serde(other)]
    Other,
}

/// The unrecognized run state a pause or finish unit carries, or `None` when the unit is not a
/// pause or finish, lacks the fields the probe needs, or carries a known reason. In the last
/// case the closed control unit's own decoding error is the one to report.
fn unrecognized_run_state(unit: &serde_json::Value) -> Option<UnrecognizedRunState> {
    match RunStateProbe::deserialize(unit).ok()? {
        RunStateProbe::Paused {
            run_id,
            reason: broca_protocol::OpenState::Unknown(reason),
        } => Some(UnrecognizedRunState::Paused { run_id, reason }),
        RunStateProbe::RunFinished {
            run_id,
            reason: broca_protocol::OpenState::Unknown(reason),
        } => Some(UnrecognizedRunState::Finished { run_id, reason }),
        RunStateProbe::Paused { .. } | RunStateProbe::RunFinished { .. } | RunStateProbe::Other => {
            None
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum SubscribeEventRef<'a> {
    Control {
        cursor: CursorWire,
        unit: &'a broca_protocol::caller::ControlUnit,
    },
    Display {
        event: &'a DisplayEvent,
    },
}

/// Encode one control-lane subscribe event directly from its borrowed payload.
///
/// This avoids building an owned JSON value before emitting the stream frame while keeping
/// [`SubscribeEventRef`] as the single serialization shape for subscribe events.
pub fn encode_subscribe_control(
    wal_seq: u64,
    sub_index: u32,
    unit: &broca_protocol::caller::ControlUnit,
) -> serde_json::Result<Vec<u8>> {
    serde_json::to_vec(&SubscribeEventRef::Control {
        cursor: CursorWire { wal_seq, sub_index },
        unit,
    })
}

/// Encode one display-lane subscribe event directly from its borrowed payload.
///
/// This avoids copying streamed deltas into an intermediate JSON tree before byte emission.
pub fn encode_subscribe_display(event: &DisplayEvent) -> serde_json::Result<Vec<u8>> {
    serde_json::to_vec(&SubscribeEventRef::Display { event })
}

// This owned mirror decodes the public by-value event shape; changing it to a box just to satisfy
// a size lint would make the event payloads asymmetric internally.
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
enum SubscribeEventOwned {
    Control {
        cursor: CursorWire,
        // Kept as a JSON value so a unit with a newer pause or finish reason can be read again
        // as an unrecognized run state after the closed control unit refuses it.
        unit: serde_json::Value,
    },
    Display {
        event: DisplayEvent,
    },
    #[serde(other)]
    Unknown,
}

impl Serialize for SubscribeEvent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Control {
                wal_seq,
                sub_index,
                unit,
            } => SubscribeEventRef::Control {
                cursor: CursorWire {
                    wal_seq: *wal_seq,
                    sub_index: *sub_index,
                },
                unit,
            }
            .serialize(serializer),
            Self::Display(event) => SubscribeEventRef::Display { event }.serialize(serializer),
            Self::UnrecognizedRunState {
                wal_seq,
                sub_index,
                state,
            } => UnrecognizedSubscribeEventRef::Control {
                cursor: CursorWire {
                    wal_seq: *wal_seq,
                    sub_index: *sub_index,
                },
                unit: state.into(),
            }
            .serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for SubscribeEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match SubscribeEventOwned::deserialize(deserializer)? {
            SubscribeEventOwned::Control { cursor, unit } => {
                match broca_protocol::caller::ControlUnit::deserialize(&unit) {
                    Ok(control) => Ok(Self::Control {
                        wal_seq: cursor.wal_seq,
                        sub_index: cursor.sub_index,
                        unit: control,
                    }),
                    Err(error) => match unrecognized_run_state(&unit) {
                        Some(state) => Ok(Self::UnrecognizedRunState {
                            wal_seq: cursor.wal_seq,
                            sub_index: cursor.sub_index,
                            state,
                        }),
                        None => Err(serde::de::Error::custom(error)),
                    },
                }
            }
            SubscribeEventOwned::Display { event } => Ok(Self::Display(event)),
            SubscribeEventOwned::Unknown => Ok(Self::Display(DisplayEvent::DisplayReset)),
        }
    }
}

/// Decode one `StreamData` frame body into a subscribe event.
pub fn decode_subscribe_event(body: &[u8]) -> Result<SubscribeEvent, String> {
    serde_json::from_slice(body).map_err(|error| format!("decode subscribe event: {error}"))
}

/// The error code refusing a scoped send into a session that was recorded without a scope and
/// cannot adopt the stamped one.
pub const SCOPE_ADOPTION_REFUSED: &str = "scope_adoption_refused";

/// The error body refusing a scoped send into a session recorded without a scope.
///
/// Such a session may adopt a stamped scope only if the scope's owner equals the principal
/// recorded on every earlier episode. When it cannot, the send is refused with this body, on
/// the `session.send` reply and on the route bind. The caller branches on `detail`:
/// `no_recorded_principal` means some earlier episode predates principal recording, so no
/// owner can ever match and adopting needs an owner's decision; `principal_mismatch` means the
/// session belongs to a different principal, named in `recorded`. `flow_scope` means an
/// automation requires its own fresh history, irrespective of the recorded principals.
///
/// It serializes with the same members, in the same order, as subc's `ErrorBody` (`code`,
/// `message`, `detail`), so it goes on the wire as an ordinary error body with a typed
/// detail. The exact bytes of these details are pinned by golden files under
/// `tests/golden/`, because callers are built against them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeAdoptionRefused {
    /// Always [`SCOPE_ADOPTION_REFUSED`].
    pub code: String,
    /// Human-readable; derived from `detail`, never parsed.
    pub message: String,
    pub detail: ScopeAdoptionRefusedDetail,
}

/// Why a session recorded without a scope cannot adopt the stamped one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScopeAdoptionRefusedDetail {
    /// Automation scopes require a fresh session, even when the owner matches all history.
    FlowScope,
    /// At least one earlier episode recorded no principal (it was written before principals
    /// were recorded, or its bind carried none), so it can never match an owner.
    NoRecordedPrincipal,
    /// Every earlier episode recorded a principal, but not the scope's owner. `recorded` is that
    /// principal as a scope owner is spelled: `reserved:<module_id>`, `direct` or `unverified`.
    PrincipalMismatch { recorded: String },
}

impl ScopeAdoptionRefusedDetail {
    /// The mismatch detail for a recorded principal, spelled the way a scope owner is.
    pub fn principal_mismatch(recorded: &broca_protocol::BindPrincipal) -> Self {
        Self::PrincipalMismatch {
            recorded: recorded.as_owner_string(),
        }
    }
}

impl ScopeAdoptionRefused {
    pub fn new(detail: ScopeAdoptionRefusedDetail) -> Self {
        let message = match &detail {
            ScopeAdoptionRefusedDetail::FlowScope => "a session recorded without a scope cannot adopt a flow scope; start a new session under the flow scope".to_owned(),
            ScopeAdoptionRefusedDetail::NoRecordedPrincipal => {
                "this session was recorded without a scope and an earlier episode recorded no \
                 principal, so it cannot adopt the stamped scope; start a new session under the \
                 scope"
                    .to_owned()
            }
            ScopeAdoptionRefusedDetail::PrincipalMismatch { recorded } => format!(
                "this session was recorded without a scope under principal '{recorded}', which \
                 is not the stamped scope's owner, so it cannot adopt the scope; start a new \
                 session under the scope"
            ),
        };
        Self {
            code: SCOPE_ADOPTION_REFUSED.to_owned(),
            message,
            detail,
        }
    }
}

#[cfg(test)]
mod scope_adoption_refused_golden {
    use super::*;
    use sha2::{Digest, Sha256};

    /// Serialize a refusal, compare it byte for byte with its golden file, and check the file
    /// itself against the digest pinned here. The digest makes the golden file part of the
    /// contract too: regenerating it to match a changed type fails until the digest is changed
    /// on purpose, and callers are told.
    fn assert_golden(refusal: ScopeAdoptionRefused, file: &str, sha256: &str) {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/golden")
            .join(file);
        let golden = std::fs::read(&path)
            .unwrap_or_else(|error| panic!("read golden {}: {error}", path.display()));
        let digest = format!("{:x}", Sha256::digest(&golden));
        println!("golden {} sha256 {digest}", path.display());
        assert_eq!(
            String::from_utf8(serde_json::to_vec(&refusal).unwrap()).unwrap(),
            String::from_utf8(golden.clone()).unwrap(),
            "serialized refusal differs from {}",
            path.display()
        );
        assert_eq!(
            serde_json::from_slice::<ScopeAdoptionRefused>(&golden).unwrap(),
            refusal
        );
        assert_eq!(digest, sha256, "golden file {} changed", path.display());
    }

    #[test]
    fn no_recorded_principal_refusal_bytes_are_pinned() {
        assert_golden(
            ScopeAdoptionRefused::new(ScopeAdoptionRefusedDetail::NoRecordedPrincipal),
            "scope_adoption_refused.no_recorded_principal.json",
            NO_RECORDED_PRINCIPAL_SHA256,
        );
    }

    #[test]
    fn principal_mismatch_refusal_bytes_are_pinned() {
        let recorded = broca_protocol::BindPrincipal::Reserved {
            module_id: "prefrontal-core".into(),
        };
        assert_golden(
            ScopeAdoptionRefused::new(ScopeAdoptionRefusedDetail::principal_mismatch(&recorded)),
            "scope_adoption_refused.principal_mismatch.json",
            PRINCIPAL_MISMATCH_SHA256,
        );
    }

    #[test]
    fn flow_adoption_refusal_bytes_are_pinned() {
        assert_golden(
            ScopeAdoptionRefused::new(ScopeAdoptionRefusedDetail::FlowScope),
            "scope_adoption_refused.flow_scope.json",
            "7f103c46d678b1f94175ab88975e4d0021c0a254a2f8b887f236f52076347445",
        );
    }

    #[test]
    fn flow_principal_refusal_bytes_are_pinned() {
        for (required, file, digest) in [
            (
                true,
                "flow_scope_required.json",
                "267d9609618c40d7e1534904b07c3560f77285c998a392dc41d1863a851d6393",
            ),
            (
                false,
                "sender_not_owner.json",
                "c6bccebac77204eed29a035d35b9ce5100e09bd82997f2a07194b4bb8c0aa580",
            ),
        ] {
            let bytes = std::fs::read(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("tests/golden")
                    .join(file),
            )
            .unwrap();
            let refusal = FlowPrincipalRefusal::new(required, "reserved:basal");
            assert_eq!(serde_json::to_vec(&refusal).unwrap(), bytes);
            assert_eq!(
                serde_json::from_slice::<FlowPrincipalRefusal>(&bytes).unwrap(),
                refusal
            );
            assert_eq!(format!("{:x}", Sha256::digest(&bytes)), digest);
        }
    }

    #[test]
    fn flow_identity_refusal_bytes_are_pinned() {
        for (field, file, digest) in [
            (
                FlowMismatchField::Sender,
                "scope_sender_mismatch.json",
                "1ca077342a83e79c41b06c3be0d6e717d7339d38de7207fbfbf98b09cd2058de",
            ),
            (
                FlowMismatchField::FlowId,
                "scope_flow_id_mismatch.json",
                "c35e8d58e078ac5ce21b2c2d31768543a0b76196a3ff978b92831814286de84e",
            ),
        ] {
            let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/golden")
                .join(file);
            let bytes = std::fs::read(path).unwrap();
            let refusal = ScopeFlowMismatch::new(field);
            assert_eq!(serde_json::to_vec(&refusal).unwrap(), bytes);
            assert_eq!(
                serde_json::from_slice::<ScopeFlowMismatch>(&bytes).unwrap(),
                refusal
            );
            assert_eq!(format!("{:x}", Sha256::digest(&bytes)), digest);
        }
    }

    const NO_RECORDED_PRINCIPAL_SHA256: &str =
        "1a101548852bbb768351eae68b0d3692043bf22bfebe83949106bbcdc190be4d";
    const PRINCIPAL_MISMATCH_SHA256: &str =
        "7d2c76afd8de75284ad47a9be3af2cf970606319da3937879a58bdf4c0879c58";
}

/// A principal-specific admission refusal; existing scope refusal bytes stay unchanged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowPrincipalRefusal {
    pub code: String,
    pub message: String,
    pub class: broca_protocol::OpenErrorClass,
    pub detail: FlowPrincipalRefusalDetail,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowPrincipalRefusalDetail {
    pub kind: String,
    pub principal: String,
}

impl FlowPrincipalRefusal {
    pub fn new(scope_required: bool, principal: &str) -> Self {
        let (code, message) = if scope_required {
            (
                "flow_scope_required",
                "this principal may send only under a flow scope",
            )
        } else {
            (
                "sender_not_owner",
                "this request differs from the session's frozen sender",
            )
        };
        Self {
            code: code.into(),
            message: message.into(),
            class: broca_protocol::ErrorClass::Permanent.into(),
            detail: FlowPrincipalRefusalDetail {
                kind: code.into(),
                principal: principal.into(),
            },
        }
    }
}

/// Which frozen automation field a later session send changed: its driving principal or flow ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FlowMismatchField {
    Sender,
    FlowId,
}

/// A later send may not switch a flow session's automation or driving principal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeFlowMismatch {
    pub code: String,
    pub message: String,
    pub detail: FlowMismatchDetail,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowMismatchDetail {
    pub field: FlowMismatchField,
}

impl ScopeFlowMismatch {
    pub fn new(field: FlowMismatchField) -> Self {
        let (code, name) = match field {
            FlowMismatchField::Sender => ("scope_sender_mismatch", "sender"),
            FlowMismatchField::FlowId => ("scope_flow_id_mismatch", "flow_id"),
        };
        Self {
            code: code.into(),
            message: format!("this send differs from the session's frozen {name}"),
            detail: FlowMismatchDetail { field },
        }
    }
}

/// The error code refusing a send that does not come from the identity allowed to drive the
/// session.
pub const SCOPE_OWNER_MISMATCH: &str = "scope_owner_mismatch";

/// The error body refusing a send from the wrong identity. Terminal for that send: nothing was
/// written, and the same send over the same route meets the same refusal.
///
/// Serializes with the members of subc's `ErrorBody` (`code`, `message`, `detail`), like
/// [`ScopeAdoptionRefused`]; the bytes are pinned by golden files under `tests/golden/`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeOwnerMismatch {
    /// Always [`SCOPE_OWNER_MISMATCH`].
    pub code: String,
    /// Human-readable; derived from `detail`, never parsed.
    pub message: String,
    pub detail: ScopeOwnerMismatchDetail,
}

/// Who the session accepts, and what the refused send presented. Principals are spelled the
/// way a scope owner is: `reserved:<module_id>`, `direct` or `unverified`; an absent one means
/// the route carried no principal (or, for a scope, no stamp).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScopeOwnerMismatchDetail {
    /// Agent-run scopes accept the reserved Basal carrier, not their owning agent.
    AgentRunSender {
        owner: String,
        #[serde(rename = "ref")]
        scope_ref: String,
        scope_epoch: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        presented_principal: Option<String>,
    },
    /// The session is scoped: it accepts only its owner, over a route stamped with exactly the
    /// frozen `(owner, ref, scope_epoch)`.
    Scoped {
        owner: String,
        #[serde(rename = "ref")]
        scope_ref: String,
        scope_epoch: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        presented_principal: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        presented_scope: Option<PresentedScope>,
    },
    /// The route was stamped with this scope but opened by someone other than its owner; only
    /// the owner may freeze or adopt its scope.
    NotTheOwner {
        owner: String,
        #[serde(rename = "ref")]
        scope_ref: String,
        scope_epoch: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        presented_principal: Option<String>,
    },
    /// The session is unscoped: it accepts only the principal of its first send.
    FirstPrincipal {
        first: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        presented_principal: Option<String>,
    },
}

/// A scope stamp as a refusal names it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PresentedScope {
    pub owner: String,
    #[serde(rename = "ref")]
    pub scope_ref: String,
    pub scope_epoch: u64,
}

impl ScopeOwnerMismatch {
    pub fn new(detail: ScopeOwnerMismatchDetail) -> Self {
        let message = match &detail {
            ScopeOwnerMismatchDetail::AgentRunSender { owner, .. } => format!(
                "an agent-run scope owned by '{owner}' accepts sends only from its reserved:basal carrier under the session's scope identity, never from its owner"
            ),
            ScopeOwnerMismatchDetail::Scoped {
                owner, scope_ref, ..
            } => format!(
                "this session is scoped to '{owner}' (ref '{scope_ref}') and accepts sends only \
                 from that owner over a route stamped with the same scope"
            ),
            ScopeOwnerMismatchDetail::NotTheOwner { owner, .. } => format!(
                "the route is stamped with a scope owned by '{owner}' but was not opened by that \
                 owner; only the owner may start a session under its scope"
            ),
            ScopeOwnerMismatchDetail::FirstPrincipal { first, .. } => format!(
                "this session was started by '{first}' without a scope and accepts later sends \
                 only from that principal"
            ),
        };
        Self {
            code: SCOPE_OWNER_MISMATCH.to_owned(),
            message,
            detail,
        }
    }
}

/// The error code refusing a send into a scoped session on a daemon that does not advertise
/// `scopes/v1`: no route there can carry the owner's stamp. Terminal; nothing was written.
pub const SCOPE_UNSUPPORTED: &str = "scope_unsupported";

/// The error code for a send that would freeze or adopt its stamped scope while the session is
/// busy. Retryable: nothing was written.
pub const SCOPE_FREEZE_BUSY: &str = "scope_freeze_busy";

/// The error body for [`SCOPE_FREEZE_BUSY`].
///
/// A session freezes its scope on the `RunStarted` of the episode the freezing send opens. A
/// send that arrives while a run is active, or while submissions are queued, would be queued,
/// and its episode would start later with nothing to carry the stamp. So it is refused, after
/// the adoption rules have passed (a send that can never adopt gets `scope_adoption_refused`
/// instead), and the caller retries once the session is idle. Serializes with the members of
/// subc's `ErrorBody`; the bytes are pinned by golden files under `tests/golden/`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeFreezeBusy {
    /// Always [`SCOPE_FREEZE_BUSY`].
    pub code: String,
    /// Human-readable; derived from `detail`, never parsed.
    pub message: String,
    pub detail: ScopeFreezeBusyDetail,
}

/// Why the session is busy: a run is active (named by `run_id`), or submissions are queued.
/// Tagged `kind`, like every other broca refusal detail, because callers branch on `code` and
/// then `detail.kind`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScopeFreezeBusyDetail {
    RunActive { run_id: String },
    QueueNonempty,
}

impl ScopeFreezeBusy {
    pub fn new(detail: ScopeFreezeBusyDetail) -> Self {
        let message = match &detail {
            ScopeFreezeBusyDetail::RunActive { run_id } => format!(
                "this send would freeze its scope on the session, but run '{run_id}' is active; \
                 retry when the session is idle"
            ),
            ScopeFreezeBusyDetail::QueueNonempty => "this send would freeze its scope on the \
                 session, but submissions are queued; retry when the session is idle"
                .to_owned(),
        };
        Self {
            code: SCOPE_FREEZE_BUSY.to_owned(),
            message,
            detail,
        }
    }
}

/// The error code answering ANY new message sent into a session whose last run is PAUSED (for
/// example awaiting re-authentication): a plain or queued send and a steer alike. There is no
/// running turn to join, the paused one renders nothing until its pause is resolved, and a
/// message queued behind it would wait for a run nothing is going to end. PERMANENT for this
/// send: nothing was written, and the same send meets the same refusal until the caller acts.
/// The caller re-authenticates and resends the paused run's OWN `send_id` (which resumes that
/// run and is never refused), or cancels the paused run with `run.cancel`, or sends with
/// `delivery: "interrupt"`, which ends the paused run and starts the message.
pub const RUN_PAUSED: &str = "run_paused";

/// The error body for [`RUN_PAUSED`]. Serializes with the members of subc's `ErrorBody`; the
/// bytes are pinned by a golden file under `tests/golden/`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunPaused {
    /// Always [`RUN_PAUSED`].
    pub code: String,
    /// Human-readable; derived from `detail`, never parsed.
    pub message: String,
    pub detail: RunPausedDetail,
}

/// The paused run a refused steer would have joined, and why it is paused, as recorded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RunPausedDetail {
    RunPaused {
        run_id: String,
        reason: broca_protocol::OpenPauseReason,
    },
}

impl RunPaused {
    pub fn new(run_id: String, reason: broca_protocol::PauseReason) -> Self {
        Self {
            code: RUN_PAUSED.to_owned(),
            message: format!(
                "run '{run_id}' is paused ({}), so this session takes no new message: resend \
                 that run's own send_id once the pause is resolved, cancel it, or send with \
                 delivery \"interrupt\" to end it and start this message",
                reason.as_str()
            ),
            detail: RunPausedDetail::RunPaused {
                run_id,
                reason: reason.into(),
            },
        }
    }
}

#[cfg(test)]
pub(crate) mod scope_refusal_golden {
    use super::*;
    use sha2::{Digest, Sha256};

    /// Serialize a body, compare it byte for byte with its golden file, and check the file
    /// against the digest pinned here, so regenerating the file to match a changed type fails
    /// until the digest is changed on purpose.
    pub(crate) fn assert_golden<T>(body: T, file: &str, sha256: &str)
    where
        T: Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/golden")
            .join(file);
        let golden = std::fs::read(&path)
            .unwrap_or_else(|error| panic!("read golden {}: {error}", path.display()));
        let digest = format!("{:x}", Sha256::digest(&golden));
        println!("golden {} sha256 {digest}", path.display());
        assert_eq!(
            String::from_utf8(serde_json::to_vec(&body).unwrap()).unwrap(),
            String::from_utf8(golden.clone()).unwrap(),
            "serialized body differs from {}",
            path.display()
        );
        assert_eq!(serde_json::from_slice::<T>(&golden).unwrap(), body);
        assert_eq!(digest, sha256, "golden file {} changed", path.display());
    }

    #[test]
    fn freeze_busy_run_active_bytes_are_pinned() {
        assert_golden(
            ScopeFreezeBusy::new(ScopeFreezeBusyDetail::RunActive {
                run_id: "run-sid-abc".into(),
            }),
            "scope_freeze_busy.run_active.json",
            FREEZE_BUSY_RUN_ACTIVE_SHA256,
        );
    }

    #[test]
    fn freeze_busy_queue_nonempty_bytes_are_pinned() {
        assert_golden(
            ScopeFreezeBusy::new(ScopeFreezeBusyDetail::QueueNonempty),
            "scope_freeze_busy.queue_nonempty.json",
            FREEZE_BUSY_QUEUE_NONEMPTY_SHA256,
        );
    }

    #[test]
    fn owner_mismatch_scoped_bytes_are_pinned() {
        assert_golden(
            ScopeOwnerMismatch::new(ScopeOwnerMismatchDetail::Scoped {
                owner: "reserved:prefrontal-core".into(),
                scope_ref: "head-1".into(),
                scope_epoch: 3,
                presented_principal: Some("reserved:magic-context".into()),
                presented_scope: None,
            }),
            "scope_owner_mismatch.scoped.json",
            OWNER_MISMATCH_SCOPED_SHA256,
        );
    }

    const FREEZE_BUSY_RUN_ACTIVE_SHA256: &str =
        "e08543c4a642f5aedfeccb6953422c0687220dad3fc59ac8165540509faf74ea";
    const FREEZE_BUSY_QUEUE_NONEMPTY_SHA256: &str =
        "14bb27bf301a4269477bef5786265a574237ddc9e0761a6ba41938f7e85c68fc";
    const OWNER_MISMATCH_SCOPED_SHA256: &str =
        "256f24d10df54c0bdbcdb402a12c847f8327de7d24120eec4583181760c6d0ed";
}

/// The error code refusing an operation on a session whose WAL a newer broca build wrote.
pub const WAL_NEWER_WRITER: &str = "wal_newer_writer";
/// The error code refusing a read of a session whose stored history proves it existed, while
/// neither a live nor an archived WAL covers it. Permanent: serving the stored rows alone would
/// present a truncated transcript, and no retry brings the log back.
pub const HISTORY_UNREACHABLE: &str = "history_unreachable";

/// The error body refusing an operation on a session whose WAL holds a complete frame that
/// requires WAL features this build does not know.
///
/// The WAL's bytes are intact, so this is not corruption, and nothing was written. Retrying
/// cannot help: the caller has to place a broca build that knows every feature in
/// `detail.missing`. That is why it has its own code instead of the generic `open_failed`,
/// which also covers causes a retry can clear. Callers branch on `code`, then on
/// `detail.kind` (always `newer_writer` today; a later kind would name a different refusal
/// under the same code). The message keeps the full refusal text for logs and is never parsed.
///
/// It serializes with the same members, in the same order, as subc's `ErrorBody` (`code`,
/// `message`, `detail`), so it goes on the wire as an ordinary error body with a typed
/// detail. Its exact bytes are pinned by a golden file under `tests/golden/`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalNewerWriter {
    /// Always [`WAL_NEWER_WRITER`].
    pub code: String,
    /// Human-readable; the same text the refusal carried before it had its own code.
    pub message: String,
    pub detail: WalNewerWriterDetail,
}

/// What a newer-writer refusal names, tagged by `kind` like other typed refusal details.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WalNewerWriterDetail {
    /// `missing` lists the WAL feature names the refused frame requires and this build does
    /// not know, sorted.
    NewerWriter { missing: Vec<String> },
}

impl WalNewerWriter {
    pub fn new(message: impl Into<String>, missing: Vec<String>) -> Self {
        Self {
            code: WAL_NEWER_WRITER.to_owned(),
            message: message.into(),
            detail: WalNewerWriterDetail::NewerWriter { missing },
        }
    }
}

#[cfg(test)]
mod wal_newer_writer_golden {
    use super::*;
    use sha2::{Digest, Sha256};

    /// The golden carries the message a running daemon answered `session.read` with for this
    /// refusal (it was `open_failed` then), so the pinned message is one callers have seen.
    #[test]
    fn wal_newer_writer_refusal_bytes_are_pinned() {
        let refusal = WalNewerWriter::new(
            "open failed: wal range-read failed: wal written by a newer broca (NewerWriter): \
             the version 3 frame at offset 69 requires features this build does not know: \
             probe-unknown/v1",
            vec!["probe-unknown/v1".into()],
        );
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/golden/wal_newer_writer.json");
        let golden = std::fs::read(&path)
            .unwrap_or_else(|error| panic!("read golden {}: {error}", path.display()));
        let digest = format!("{:x}", Sha256::digest(&golden));
        println!("golden {} sha256 {digest}", path.display());
        assert_eq!(
            String::from_utf8(serde_json::to_vec(&refusal).unwrap()).unwrap(),
            String::from_utf8(golden.clone()).unwrap(),
            "serialized refusal differs from {}",
            path.display()
        );
        assert_eq!(
            serde_json::from_slice::<WalNewerWriter>(&golden).unwrap(),
            refusal
        );
        // The digest makes the golden file part of the contract: regenerating it to match a
        // changed type still fails until this digest is updated on purpose, which is the
        // point at which callers built against the old bytes must be told.
        assert_eq!(
            digest,
            WAL_NEWER_WRITER_SHA256,
            "golden file {} changed",
            path.display()
        );
    }

    const WAL_NEWER_WRITER_SHA256: &str =
        "86f8dff3deab30b01d852f7b4167b389b548977ebfa27b563ad33f17a6544625";
}

/// The error code refusing a send because the routing module would not choose a provider
/// account for it.
pub const ACCOUNT_SELECTION_REFUSED: &str = "account_selection_refused";

/// The error body for [`ACCOUNT_SELECTION_REFUSED`]. Nothing was written: the run never
/// started. Callers branch on `detail.routing_code` and `detail.class`, never on the message.
///
/// It serializes with the same members, in the same order, as subc's `ErrorBody` (`code`,
/// `message`, `detail`). Its exact bytes are pinned by golden files under `tests/golden/`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountSelectionRefused {
    /// Always [`ACCOUNT_SELECTION_REFUSED`].
    pub code: String,
    /// Human-readable; derived from `detail`, never parsed.
    pub message: String,
    pub detail: AccountSelectionRefusedDetail,
}

/// Routing's refusal, copied verbatim, with broca's classification of it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountSelectionRefusedDetail {
    /// Routing's own refusal code, for example `account_pool_exhausted`.
    pub routing_code: String,
    /// `transient` when retrying the same send later may be admitted (an account hold, an
    /// exhausted or reserve-limited pool, unknown quota, a stale roster), `permanent` when it
    /// cannot without someone acting first (every candidate excluded, or a request routing
    /// rejected as malformed or conflicting).
    pub class: broca_protocol::OpenErrorClass,
    /// Routing's `retry_after_s`, verbatim, when it sent one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_s: Option<u64>,
    /// The same wait as the structured retry hint every broca error class carries, so a
    /// caller's ordinary transient-retry policy reads it without knowing routing's spelling.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_secs: Option<f64>,
    /// Routing's id for the decision behind the refusal, when it sent one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision_id: Option<String>,
}

impl AccountSelectionRefused {
    pub fn new(
        routing_code: String,
        class: broca_protocol::ErrorClass,
        retry_after_s: Option<u64>,
        decision_id: Option<String>,
    ) -> Self {
        let retry = match retry_after_s {
            Some(seconds) => format!("; retry after {seconds} s"),
            None => String::new(),
        };
        Self {
            code: ACCOUNT_SELECTION_REFUSED.to_owned(),
            message: format!(
                "the routing module refused to choose a provider account for this send \
                 ({routing_code}){retry}"
            ),
            detail: AccountSelectionRefusedDetail {
                routing_code,
                class: class.into(),
                retry_after_s,
                retry_after_secs: retry_after_s.map(|seconds| seconds as f64),
                decision_id,
            },
        }
    }
}

#[cfg(test)]
mod account_selection_refused_golden {
    use super::*;
    use sha2::{Digest, Sha256};

    fn assert_golden(body: AccountSelectionRefused, file: &str, sha256: &str) {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/golden")
            .join(file);
        let golden = std::fs::read(&path)
            .unwrap_or_else(|error| panic!("read golden {}: {error}", path.display()));
        let digest = format!("{:x}", Sha256::digest(&golden));
        println!("golden {} sha256 {digest}", path.display());
        assert_eq!(
            String::from_utf8(serde_json::to_vec(&body).unwrap()).unwrap(),
            String::from_utf8(golden.clone()).unwrap(),
            "serialized refusal differs from {}",
            path.display()
        );
        assert_eq!(
            serde_json::from_slice::<AccountSelectionRefused>(&golden).unwrap(),
            body
        );
        assert_eq!(digest, sha256, "golden file {} changed", path.display());
    }

    #[test]
    fn transient_pool_exhausted_refusal_bytes_are_pinned() {
        assert_golden(
            AccountSelectionRefused::new(
                "account_pool_exhausted".into(),
                broca_protocol::ErrorClass::Transient,
                Some(1260),
                Some("acd-0123".into()),
            ),
            "account_selection_refused.pool_exhausted.json",
            POOL_EXHAUSTED_SHA256,
        );
    }

    #[test]
    fn permanent_admission_conflict_refusal_bytes_are_pinned() {
        assert_golden(
            AccountSelectionRefused::new(
                "admission_conflict".into(),
                broca_protocol::ErrorClass::Permanent,
                None,
                Some("acd-0456".into()),
            ),
            "account_selection_refused.admission_conflict.json",
            ADMISSION_CONFLICT_SHA256,
        );
    }

    const POOL_EXHAUSTED_SHA256: &str =
        "786bfd039451ebb5f5cecd0d7f45428acc2b23ca518d90299ba656dc9963602e";
    const ADMISSION_CONFLICT_SHA256: &str =
        "dd54e79088fd3443de13d848f3949d7ad4c1743f7b9b9cc436d4d923d8533780";
}

#[cfg(test)]
mod status_params_strictness {
    use super::*;

    #[test]
    fn input_cap_budget_and_nudges_are_closed_and_cap_is_positive() {
        for (budget, field) in [
            (
                serde_json::json!({"max_input_tokens":0}),
                "budget.max_input_tokens",
            ),
            (
                serde_json::json!({"max_input_tokens":1,"extra":true}),
                "budget.extra",
            ),
            (
                serde_json::json!({"nudges":[{"at_tool_calls":1,"text":"stop","extra":true}]}),
                "budget.nudges[0].extra",
            ),
        ] {
            let error = serde_json::from_value::<SendParams>(
                serde_json::json!({"input":[],"budget":budget}),
            )
            .unwrap_err()
            .to_string();
            assert!(error.contains(field), "{error}");
        }
        let send: SendParams =
            serde_json::from_value(serde_json::json!({"input":[],"budget":{"max_input_tokens":1}}))
                .unwrap();
        assert_eq!(send.budget.unwrap().max_input_tokens, Some(1));
    }

    /// A misspelled `run_id` must REFUSE rather than answer a different question.
    ///
    /// The camelCase arm is not hypothetical: a Swift or JS encoder emits the property name as
    /// written, so a client whose field is `runId` sends `runId`. Before this was strict, that
    /// decoded to `run_id: None` -- a valid request for SESSION status -- and the caller got a
    /// well-formed answer about the wrong subject with nothing to signal it.
    #[test]
    fn a_misspelled_run_id_refuses_instead_of_selecting_session_status() {
        let good: StatusParams = serde_json::from_str(r#"{"run_id":"r-1"}"#).unwrap();
        assert_eq!(good.run_id.as_deref(), Some("r-1"));

        for wrong in [
            r#"{"runId":"r-1"}"#,
            r#"{"run":"r-1"}"#,
            r#"{"runID":"r-1"}"#,
        ] {
            let err = serde_json::from_str::<StatusParams>(wrong).unwrap_err();
            assert!(
                err.to_string().contains("unknown field"),
                "{wrong} must be refused by name, got: {err}"
            );
        }
    }

    /// The legitimate absence still works: `{}` is "session status", which is the whole reason
    /// the field is optional. Without this arm, strictness could be "fixed" by making the field
    /// required, which would break every session-status caller.
    #[test]
    fn an_absent_run_id_still_selects_session_status() {
        let p: StatusParams = serde_json::from_str("{}").unwrap();
        assert!(p.run_id.is_none());
    }

    /// THE COUNTER-ARM: the growing structs must stay lenient, or a rolling upgrade breaks.
    ///
    /// `work_class` landed on SendParams hours ago. A caller built against today's wire, talking
    /// to a daemon that predates it, must have the field IGNORED rather than fail the call. This
    /// asserts the split is deliberate -- someone applying `deny_unknown_fields` uniformly across
    /// this file reddens here and reads why.
    #[test]
    fn send_params_stay_lenient_so_a_newer_caller_degrades_rather_than_fails() {
        let body = serde_json::json!({
            "prompt": "hi",
            "model": {"provider": "p", "model": "m"},
            "a_field_a_future_broca_adds": 1
        });
        let parsed: SendParams = serde_json::from_value(body)
            .expect("a growing params struct must ignore a field it does not know");
        assert_eq!(parsed.prompt, "hi");
    }

    /// Lenient on unknown fields, strict on this field's value: the two rules coexist.
    #[test]
    fn send_params_context_management_is_strict_while_the_struct_stays_lenient() {
        use broca_protocol::request::ContextManagement;
        let send = |value: Option<serde_json::Value>| {
            let mut body = serde_json::json!({
                "prompt": "hi",
                "model": {"provider": "p", "model": "m"},
                "a_field_a_future_broca_adds": 1
            });
            if let Some(value) = value {
                body["context_management"] = value;
            }
            serde_json::from_value::<SendParams>(body)
        };
        assert_eq!(send(None).unwrap().context_management, None);
        assert_eq!(
            send(Some(serde_json::json!(null)))
                .unwrap()
                .context_management,
            None
        );
        assert_eq!(
            send(Some(serde_json::json!("none")))
                .unwrap()
                .context_management,
            Some(ContextManagement::None)
        );
        assert_eq!(
            send(Some(serde_json::json!({"transform": "magic-context"})))
                .unwrap()
                .context_management,
            Some(ContextManagement::Transform("magic-context".into()))
        );
        let error = send(Some(serde_json::json!("summarize")))
            .unwrap_err()
            .to_string();
        assert!(error.contains("context_management must be"), "{error}");
        // An omitted value adds no bytes to what a caller sends.
        let omitted = send(None).unwrap();
        assert!(serde_json::to_value(&omitted)
            .unwrap()
            .get("context_management")
            .is_none());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use broca_protocol::caller::ControlUnit;
    use broca_protocol::content::{AssistantMessage, ContentBlock, ToolCall};
    use broca_protocol::{ErrorClass, MessageId, ProviderError, RunId, ToolCallId};
    use serde::de::DeserializeOwned;

    fn assert_json_round_trip<T>(json: serde_json::Value)
    where
        T: Serialize + DeserializeOwned,
    {
        let decoded: T = serde_json::from_value(json.clone()).expect("fixture deserializes");
        assert_eq!(
            serde_json::to_value(decoded).expect("fixture serializes"),
            json
        );
    }

    fn assert_run_status_round_trip(json: serde_json::Value) {
        let decoded: RunStatusResponse =
            serde_json::from_value(json.clone()).expect("fixture deserializes");
        let actual = serde_json::to_vec(&decoded).expect("fixture serializes");
        let golden = serde_json::to_vec(&json).unwrap();
        let actual: serde_json::Value =
            serde_json::from_slice(&crate::usage_compat::legacy_bytes(&actual, &golden)).unwrap();
        assert_eq!(actual, json);
    }

    #[test]
    fn moved_request_types_round_trip_without_shape_drift() {
        assert_json_round_trip::<RouteRequest>(serde_json::json!({
            "method": "session.read",
            "params": {"from_ordinal": 4, "limit": 2}
        }));
        assert_json_round_trip::<ModelParams>(serde_json::json!({
            "provider": "stub", "model": "m"
        }));
        assert_json_round_trip::<CacheParams>(serde_json::json!({
            "role": "ephemeral", "ttl": "5m"
        }));
        assert_json_round_trip::<CacheParams>(serde_json::json!({
            "role": "ephemeral", "ttl": "5m", "key": "alfonso-classify-v1"
        }));
        assert_json_round_trip::<CacheParams>(serde_json::json!({
            "role": "ephemeral", "ttl": "5m", "tail": "volatile"
        }));
        assert_json_round_trip::<SendParams>(serde_json::json!({
            "prompt": "hello",
            "system": "be concise",
            "send_id": "send-1",
            "model": {"provider": "stub", "model": "m"},
            "tools": [],
            "tool_choice": {"type": "auto"},
            "generation": {},
            "stop_when": [],
            "cache": {"role": "ephemeral", "ttl": "5m", "key": "alfonso-classify-v1"},
            "budget": {
                "nudges": [],
                "finalize_at_tool_calls": 30,
                "finalize_text": "answer now"
            },
            "auth": {"method": "apikey", "account": "astro"},
            "one_shot": true,
            "append_episode": false
        }));
        assert_json_round_trip::<RetractParams>(serde_json::json!({"submission_id": "sub-1"}));
        assert_json_round_trip::<CancelParams>(serde_json::json!({"run_id": "run-1"}));
        assert_json_round_trip::<StatusParams>(serde_json::json!({"run_id": "run-1"}));
        assert_json_round_trip::<SubscribeParams>(serde_json::json!({
            "from": {"wal_seq": 8, "sub_index": 2}
        }));
        assert_json_round_trip::<ReadParams>(serde_json::json!({
            "from_ordinal": 4, "limit": 2
        }));
        assert_json_round_trip::<ReadParams>(serde_json::json!({
            "from_ordinal": 4, "limit": 2, "include_tools": true
        }));
        assert_json_round_trip::<FromWire>(serde_json::json!({"wal_seq": 8, "sub_index": 2}));
        assert_json_round_trip::<CursorWire>(serde_json::json!({"wal_seq": 8, "sub_index": 2}));
    }

    #[test]
    fn conversation_key_values_are_strict() {
        for key in [
            serde_json::json!(""),
            serde_json::json!("x".repeat(257)),
            serde_json::json!("a b"),
            serde_json::json!("a\n"),
            serde_json::json!("é"),
            serde_json::json!(42),
            serde_json::Value::Null,
        ] {
            let error =
                serde_json::from_value::<SendParams>(serde_json::json!({"conversation_key": key}))
                    .unwrap_err();
            assert!(error.to_string().contains("conversation_key"), "{error}");
        }
        for key in ["consult:123:subject:phase".to_owned(), "!".repeat(256)] {
            let parsed: SendParams =
                serde_json::from_value(serde_json::json!({"conversation_key": key})).unwrap();
            assert_eq!(parsed.conversation_key, Some(key));
        }
        let parsed: SendParams = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(parsed.conversation_key, None);
        assert!(serde_json::to_value(parsed)
            .unwrap()
            .get("conversation_key")
            .is_none());
    }

    #[test]
    fn send_key_lists_come_from_the_structs_themselves() {
        assert_eq!(
            send_params_keys(),
            &[
                "directives",
                "session_class",
                "on_restart",
                "keep_warm",
                "retention",
                "prompt",
                "prompt_blocks",
                "system",
                "send_id",
                "conversation_key",
                "title",
                "client_message_id",
                "model",
                "tools",
                "tool_choice",
                "generation",
                "stop_when",
                "cache",
                "budget",
                "structured_output",
                "auth",
                "context_limit",
                "work_class",
                "context_management",
                "service_tier",
                "one_shot",
                "delivery",
                "dedupe_key",
                "expires_at_ms",
                "template",
                "append_episode",
            ]
        );
        assert_eq!(model_params_keys(), &["provider", "model", "variant"]);
    }

    #[test]
    fn a_model_variant_is_read_and_an_absent_one_keeps_legacy_send_bytes() {
        let with: SendParams = serde_json::from_value(serde_json::json!({
            "prompt": "hello",
            "model": {"provider": "openai", "model": "gpt-5", "variant": "high"}
        }))
        .unwrap();
        assert_eq!(with.model.unwrap().variant.as_deref(), Some("high"));
        let without: SendParams = serde_json::from_value(serde_json::json!({
            "prompt": "hello",
            "model": {"provider": "openai", "model": "gpt-5"}
        }))
        .unwrap();
        assert_eq!(without.model.as_ref().unwrap().variant, None);
        assert!(!String::from_utf8(serde_json::to_vec(&without).unwrap())
            .unwrap()
            .contains("variant"));
    }

    #[test]
    fn auth_method_remains_an_unparsed_wire_string() {
        let decoded: SendParams = serde_json::from_value(serde_json::json!({
            "prompt": "hello",
            "model": {"provider": "openai", "model": "gpt-5"},
            "auth": {"method": "api-key"}
        }))
        .expect("wire decoding must not reject an unknown method spelling");
        assert_eq!(decoded.auth.unwrap().method, "api-key");
    }

    #[test]
    fn absent_prompt_blocks_preserves_frozen_legacy_send_bytes() {
        let legacy: SendParams = serde_json::from_value(serde_json::json!({
            "prompt": "hello",
            "model": {"provider": "openai", "model": "gpt-5"}
        }))
        .unwrap();
        assert_eq!(legacy.prompt_blocks, None);
        assert_eq!(legacy.retention, None);
        assert_eq!(
            serde_json::to_vec(&legacy).unwrap(),
            br#"{"prompt":"hello","system":null,"send_id":null,"model":{"provider":"openai","model":"gpt-5"},"tool_choice":{"type":"auto"},"generation":{},"stop_when":[],"cache":null,"append_episode":false}"#
        );
    }

    #[test]
    fn retention_is_parsed_without_making_other_send_fields_strict() {
        for retention in [0, 3600, u64::MAX] {
            let parsed: SendParams = serde_json::from_value(serde_json::json!({
                "prompt": "hello",
                "model": {"provider": "p", "model": "m"},
                "retention": retention,
                "a_field_a_future_broca_adds": true
            }))
            .unwrap();
            assert_eq!(parsed.retention, Some(retention));
            assert_eq!(
                serde_json::to_value(parsed).unwrap()["retention"],
                retention
            );
        }
    }

    #[test]
    fn send_auth_defaults_to_none_and_preserves_legacy_bytes() {
        let legacy: SendParams = serde_json::from_value(serde_json::json!({
            "prompt": "hello",
            "model": {"provider": "openai", "model": "gpt-5"}
        }))
        .unwrap();
        assert_eq!(legacy.auth, None);

        let legacy_bytes = serde_json::to_vec(&legacy).unwrap();
        let legacy_value: serde_json::Value = serde_json::from_slice(&legacy_bytes).unwrap();
        assert!(
            legacy_value.get("auth").is_none(),
            "a None auth pin must retain the pre-field session.send shape"
        );

        let decoded: SendParams = serde_json::from_slice(&legacy_bytes).unwrap();
        assert_eq!(decoded.auth, None);
        assert_eq!(
            serde_json::to_vec(&decoded).unwrap(),
            legacy_bytes,
            "decoding and re-encoding legacy session.send params must preserve their bytes"
        );
    }

    fn unavailable_error() -> ProviderError {
        ProviderError {
            provider_details: None,
            provider_subcode: None,
            refusal: None,
            class: ErrorClass::ProviderUnavailable,
            message: "provider stream disconnected".into(),
            status: None,
            retry_after_secs: None,
            provider_code: None,
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
        }
    }

    #[test]
    fn session_read_serializes_provider_unavailable_as_shared_transient() {
        let state = SessionReadLineageState {
            last_run_id: Some("run-1".into()),
            state: "error".into(),
            reason: Some("error".into()),
            error: Some(unavailable_error().into()),
            usage: None,
        };

        assert_eq!(
            serde_json::to_string(&state).unwrap(),
            r#"{"last_run_id":"run-1","state":"error","reason":"error","error":{"class":"transient","message":"provider stream disconnected"}}"#
        );
    }

    #[test]
    fn moved_response_types_round_trip_without_shape_drift() {
        assert_json_round_trip::<SendResult>(serde_json::json!({
            "state": "active", "run_id": "run-1"
        }));
        assert_json_round_trip::<SessionReadMessage>(serde_json::json!({
            "ordinal": 4,
            "mid": "m4",
            "message": {"role": "user", "content": [{"type": "text", "text": "hello"}]}
        }));
        assert_json_round_trip::<SessionReadLineageState>(serde_json::json!({
            "last_run_id": "run-1",
            "state": "completed",
            "reason": "completed",
            "usage": {
                "input_tokens": 1,
                "cached_input_tokens": 0,
                "cache_write_tokens": 0,
                "output_tokens": 2,
                "reasoning_tokens": 0
            }
        }));
        assert_json_round_trip::<SessionReadResponse>(serde_json::json!({
            "messages": [],
            "next_from_ordinal": 4,
            "head": {"wal_seq": 8, "sub_index": 2},
            "lineage_id": "9bb6d92f-1290-4d1f-a8f1-b8bcf5673073",
            "lineage_state": {"state": "active"}
        }));
        let legacy_read: SessionReadResponse = serde_json::from_value(serde_json::json!({
            "messages": [],
            "lineage_state": {"state": "idle"}
        }))
        .unwrap();
        assert_eq!(legacy_read.lineage_id, None);
        assert_run_status_round_trip(serde_json::json!({
            "state": "completed",
            "final_step_finish_reason": "stop",
            "retries_used": 0,
            "usage": {
                "input_tokens": 1,
                "cached_input_tokens": 0,
                "cache_write_tokens": 0,
                "output_tokens": 2,
                "reasoning_tokens": 0
            }
        }));
    }

    #[test]
    fn anthropic_context_overflow_status_matches_real_daemon_golden() {
        use sha2::{Digest, Sha256};

        // This fixture is generated by the account-selection rig from the real run.status reply.
        let bytes = include_bytes!("../tests/golden/run_status.context_overflow_anthropic.json");
        assert_eq!(
            format!("{:x}", Sha256::digest(bytes)),
            "dfe4248d27cbc09736639c53b9e6d0a158338e553d20f11b47801964c9ee9cc2"
        );
        let status: RunStatusResponse = serde_json::from_slice(bytes).unwrap();
        assert_eq!(
            crate::usage_compat::legacy_bytes(&serde_json::to_vec(&status).unwrap(), bytes),
            bytes
        );
        let value = serde_json::to_value(status).unwrap();
        assert_eq!(value["state"], "error");
        assert_eq!(value["error"]["class"], "context_overflow");
        assert_eq!(value["error"]["provider_code"], "context_length_exceeded");
        assert_eq!(value["error"]["status"], 400);
        assert_eq!(value["retries_used"], 0);
    }

    #[test]
    fn xai_billing_exhausted_status_matches_real_daemon_golden() {
        use sha2::{Digest, Sha256};

        // This fixture is generated by the account-selection rig from the real run.status reply.
        let bytes = include_bytes!("../tests/golden/run_status.billing_exhausted_xai.json");
        assert_eq!(
            format!("{:x}", Sha256::digest(bytes)),
            "2c7dde5dd91989a2d95170669ff367ce69d4eccbd86c5ecaf3a0aa6938de7e55"
        );
        let status: RunStatusResponse = serde_json::from_slice(bytes).unwrap();
        assert_eq!(
            crate::usage_compat::legacy_bytes(&serde_json::to_vec(&status).unwrap(), bytes),
            bytes
        );
        let value = serde_json::to_value(status).unwrap();
        assert_eq!(value["state"], "error");
        assert_eq!(value["error"]["class"], "permanent");
        assert_eq!(value["error"]["provider_code"], "billing_exhausted");
        assert_eq!(value["error"]["status"], 403);
        assert_eq!(value["error"]["account_ref"], "acct-x");
        assert_eq!(value["error"]["credential_id"], "apikey:xai");
        assert_eq!(value["retries_used"], 0);
    }

    /// Generated by the account-selection rig from the real run.status reply, against a fake
    /// OpenRouter upstream answering a 402 RECONSTRUCTED from a production refusal (verbatim
    /// message, `metadata` cut down to `limit_source`).
    #[test]
    fn openrouter_credit_insufficient_status_matches_real_daemon_golden() {
        use sha2::{Digest, Sha256};

        let bytes =
            include_bytes!("../tests/golden/run_status.credit_insufficient_openrouter.json");
        assert_eq!(
            format!("{:x}", Sha256::digest(bytes)),
            "990411488c11dbe4e8ae47d36db3c663c80a656257a89caec8eec56f920c389b"
        );
        let status: RunStatusResponse = serde_json::from_slice(bytes).unwrap();
        assert_eq!(
            crate::usage_compat::legacy_bytes(&serde_json::to_vec(&status).unwrap(), bytes),
            bytes
        );
        let value = serde_json::to_value(status).unwrap();
        assert_eq!(value["state"], "error");
        assert_eq!(value["error"]["class"], "permanent");
        assert_eq!(value["error"]["provider_code"], "credit_insufficient");
        assert_eq!(value["error"]["status"], 402);
        assert_eq!(value["error"]["credential_id"], "apikey:openrouter");
        assert_eq!(value["error"]["affordable_max_tokens"], 97335);
        assert_eq!(value["retries_used"], 0);
    }

    /// Generated by the account-selection rig from the real run.status reply, against a fake
    /// Anthropic upstream answering Anthropic's PUBLIC out-of-credit shape, not a captured
    /// refusal: HTTP 400 `{"type":"error","error":{"type":"invalid_request_error","message":"Your
    /// credit balance is too low to access the Anthropic API. Please go to Plans & Billing to
    /// upgrade or purchase credits."}}`. Regenerate it from the first real refusal.
    #[test]
    fn anthropic_apikey_billing_exhausted_status_matches_real_daemon_golden() {
        use sha2::{Digest, Sha256};

        let bytes =
            include_bytes!("../tests/golden/run_status.billing_exhausted_anthropic_apikey.json");
        assert_eq!(
            format!("{:x}", Sha256::digest(bytes)),
            "c003efaaf2dc4fc88d742c3fbe38da331f8d6912dab22d0ce243f7f478323b97"
        );
        let status: RunStatusResponse = serde_json::from_slice(bytes).unwrap();
        assert_eq!(
            crate::usage_compat::legacy_bytes(&serde_json::to_vec(&status).unwrap(), bytes),
            bytes
        );
        let value = serde_json::to_value(status).unwrap();
        assert_eq!(value["state"], "error");
        assert_eq!(value["error"]["class"], "permanent");
        assert_eq!(value["error"]["provider_code"], "billing_exhausted");
        assert_eq!(value["error"]["status"], 400);
        assert_eq!(value["error"]["credential_id"], "apikey:anthropic:work");
        assert!(value["error"].get("account_ref").is_none());
        assert_eq!(value["retries_used"], 0);
    }

    /// Generated by the account-selection rig from the real run.status reply, against a fake
    /// OpenAI upstream answering a RECONSTRUCTED body, not a captured one: production recorded an
    /// HTTP 400 with `error.type` `invalid_request_error`, `error.code` `invalid_json_schema` and
    /// `error.param` `text.format.schema`, and the message text is written for the test.
    /// Once a real body is captured, put it in the rig's `OPENAI_INVALID_SCHEMA_BODY` and
    /// regenerate this file through the rig.
    #[test]
    fn openai_request_invalid_status_matches_real_daemon_golden() {
        use sha2::{Digest, Sha256};

        let bytes = include_bytes!("../tests/golden/run_status.request_invalid_openai.json");
        assert_eq!(
            format!("{:x}", Sha256::digest(bytes)),
            "cb03a7ce9d9fa35ff201b6efc4ce3824ab1bdd18da3594bd5c130c8878a86f25"
        );
        let status: RunStatusResponse = serde_json::from_slice(bytes).unwrap();
        assert_eq!(
            crate::usage_compat::legacy_bytes(&serde_json::to_vec(&status).unwrap(), bytes),
            bytes
        );
        let value = serde_json::to_value(status).unwrap();
        assert_eq!(value["state"], "error");
        assert_eq!(value["error"]["class"], "permanent");
        assert_eq!(value["error"]["provider_code"], "request_invalid");
        assert_eq!(value["error"]["provider_subcode"], "invalid_json_schema");
        assert_eq!(value["error"]["status"], 400);
        assert!(value["error"].get("credential_id").is_none());
        assert_eq!(value["retries_used"], 0);
    }

    /// Generated through the real daemon against a scripted Anthropic compiler refusal:
    /// this golden records a completed run's best-effort `grammar_too_large` status.
    #[test]
    fn grammar_too_large_status_matches_real_daemon_golden() {
        use sha2::{Digest, Sha256};
        let bytes = include_bytes!("../tests/golden/run_status.grammar_too_large.json");
        assert_eq!(
            format!("{:x}", Sha256::digest(bytes)),
            "11f75a0f522e8f178a6a55f3e77b98dffd1eda031f71ccbeba8531fe2b6143bc"
        );
        let status: RunStatusResponse = serde_json::from_slice(bytes).unwrap();
        assert_eq!(
            crate::usage_compat::legacy_bytes(&serde_json::to_vec(&status).unwrap(), bytes),
            bytes
        );
        let value = serde_json::to_value(status).unwrap();
        assert_eq!(value["state"], "completed");
        assert_eq!(value["structured_output"]["enforcement"], "best_effort");
        assert_eq!(value["structured_output"]["reason"], "grammar_too_large");
        assert_eq!(value["structured_output"]["validation"], "passed");
        assert_eq!(value["retries_used"], 0);
    }

    /// Generated by the account-selection rig from the real run.status reply: an Anthropic 429
    /// that outlasted the transport's in-step attempts on a pinned, labelled API key.
    #[test]
    fn rate_limited_credential_status_matches_real_daemon_golden() {
        use sha2::{Digest, Sha256};

        let bytes =
            include_bytes!("../tests/golden/run_status.transient_rate_limited_credential.json");
        assert_eq!(
            format!("{:x}", Sha256::digest(bytes)),
            "e31d5d8e7c850ff68c335ea2f7148b5590abaa6e827319549d209be83b87fbe2"
        );
        let status: RunStatusResponse = serde_json::from_slice(bytes).unwrap();
        assert_eq!(
            crate::usage_compat::legacy_bytes(&serde_json::to_vec(&status).unwrap(), bytes),
            bytes
        );
        let value = serde_json::to_value(status).unwrap();
        assert_eq!(value["state"], "error");
        assert_eq!(value["error"]["class"], "transient");
        assert_eq!(value["error"]["provider_code"], "rate_limited");
        assert_eq!(value["error"]["status"], 429);
        assert_eq!(value["error"]["credential_id"], "apikey:anthropic:work");
        assert_eq!(value["retries_used"], 5);
    }

    #[test]
    fn google_code_assist_account_limit_status_matches_real_daemon_golden() {
        use sha2::{Digest, Sha256};

        // This fixture is generated by the account-selection rig from the real run.status reply.
        let bytes =
            include_bytes!("../tests/golden/run_status.account_limit_exhausted_code_assist.json");
        assert_eq!(
            format!("{:x}", Sha256::digest(bytes)),
            "727d64c8b28f7215d38c74e0b89cca5294c4fbaedeea248fdb1aa31e15761ef5"
        );
        let status: RunStatusResponse = serde_json::from_slice(bytes).unwrap();
        assert_eq!(
            crate::usage_compat::legacy_bytes(&serde_json::to_vec(&status).unwrap(), bytes),
            bytes
        );
        let value = serde_json::to_value(status).unwrap();
        assert_eq!(value["state"], "error");
        assert_eq!(value["error"]["class"], "transient");
        assert_eq!(value["error"]["provider_code"], "account_limit_exhausted");
        assert_eq!(value["error"]["status"], 429);
        assert_eq!(value["error"]["account_ref"], "acct-g");
        assert_eq!(value["error"]["credential_id"], "antigravity:google");
        assert_eq!(value["error"]["resets_at_ms"], 1_800_294_489_199_u64);
        // Google's structured RPC details ride along as an optional, additive field.
        assert_eq!(
            value["error"]["provider_details"]["errorInfo"]["reason"],
            "QUOTA_EXHAUSTED"
        );
        assert_eq!(value["retries_used"], 0);
    }

    #[test]
    fn terminal_status_undelivered_steers_are_additive() {
        use sha2::{Digest, Sha256};

        for (bytes, digest) in [
            (
                include_str!("../tests/golden/run_status.undelivered_steer.json"),
                "8ad44e6215d1f661037e4d6eddd362320eebd70f150ca8af7ff6d84d051a5405",
            ),
            (
                include_str!("../tests/golden/run_status.completed_no_undelivered_steers.json"),
                "05f69229bb04c2ac18895c1f0ae56fc009bde1e958ce4670f81b3074762bb31a",
            ),
        ] {
            assert_eq!(format!("{:x}", Sha256::digest(bytes.as_bytes())), digest);
            let pinned: serde_json::Value = serde_json::from_str(bytes).unwrap();
            assert_run_status_round_trip(pinned["result"].clone());
        }
        for state in [
            "interrupted",
            "completed",
            "max_steps",
            "cancelled",
            "error",
            "transform_unavailable",
        ] {
            let legacy = serde_json::json!({"state": state, "retries_used": 0});
            assert_run_status_round_trip(legacy.clone());
            let mut empty = legacy.clone();
            empty["undelivered_steers"] = serde_json::json!([]);
            assert_run_status_round_trip(empty);
        }
    }

    #[test]
    fn nonterminal_status_omits_undelivered_steers() {
        for value in [
            serde_json::json!({"state": "active"}),
            serde_json::json!({"state": "paused", "retries_used": 0}),
            serde_json::json!({"state": "unknown"}),
        ] {
            let decoded: RunStatusResponse = serde_json::from_value(value.clone()).unwrap();
            let encoded = serde_json::to_value(decoded).unwrap();
            assert!(encoded.get("undelivered_steers").is_none());
            assert_eq!(encoded, value);
        }
    }

    #[test]
    fn interrupted_status_tool_call_ids_are_additive() {
        let legacy: RunStatusResponse = serde_json::from_value(serde_json::json!({
            "state": "interrupted",
            "retries_used": 0
        }))
        .unwrap();
        assert!(matches!(
            legacy,
            RunStatusResponse::Interrupted {
                indeterminate_tool_calls: None,
                ..
            }
        ));

        assert_run_status_round_trip(serde_json::json!({
            "state": "interrupted",
            "indeterminate_tool_calls": ["call-1"],
            "retries_used": 0
        }));
    }

    #[test]
    fn crash_orphan_status_golden_keeps_interrupted_state_and_typed_cause() {
        let bytes = include_bytes!("../tests/golden/run_status.crash_orphan.json");
        let decoded: RunStatusResponse = serde_json::from_slice(bytes).unwrap();
        let RunStatusResponse::Interrupted { ref error, .. } = decoded else {
            panic!("the cause must not reclassify an interrupted run as an error");
        };
        let error = serde_json::to_value(error).unwrap();
        assert_eq!(error["class"], "transient");
        assert_eq!(error["provider_code"], "previous_process_ended_unsealed");
        assert_eq!(
            crate::usage_compat::legacy_bytes(&serde_json::to_vec(&decoded).unwrap(), bytes),
            bytes
        );

        let legacy = br#"{"state":"interrupted","retries_used":0}"#;
        let legacy_status: RunStatusResponse = serde_json::from_slice(legacy).unwrap();
        assert_eq!(serde_json::to_vec(&legacy_status).unwrap(), legacy);
    }

    /// A status serialized before the cause fields existed must still decode, and the decoded
    /// value must re-serialize to the same bytes: the new keys stay absent rather than
    /// appearing as `null`.
    #[test]
    fn status_cause_fields_are_additive() {
        let legacy_error = serde_json::json!({"state": "error", "retries_used": 2});
        let decoded: RunStatusResponse = serde_json::from_value(legacy_error.clone()).unwrap();
        assert_eq!(
            decoded,
            RunStatusResponse::Error {
                auth_selection: None,
                title: None,
                final_step_finish_reason: None,
                undelivered_steers: None,
                usage: None,
                retries_used: 2,
                error: None,
                structured_output: None,
            }
        );
        assert_eq!(serde_json::to_value(&decoded).unwrap(), legacy_error);

        let legacy_paused = serde_json::json!({"state": "paused", "retries_used": 0});
        let decoded: RunStatusResponse = serde_json::from_value(legacy_paused.clone()).unwrap();
        assert_eq!(
            decoded,
            RunStatusResponse::Paused {
                auth_selection: None,
                title: None,
                pause_reason: None,
                usage: None,
                retries_used: 0,
                reason: None,
                error: None,
                auth: None,
            }
        );
        assert_eq!(serde_json::to_value(&decoded).unwrap(), legacy_paused);

        let with_cause = serde_json::json!({
            "state": "paused",
            "retries_used": 0,
            "reason": "auth_required",
            "error": {"class": "auth_required", "message": "reauth"}
        });
        assert_run_status_round_trip(with_cause.clone());

        // A consumer compiled against the shape without the cause fields still decodes a
        // response that carries them; it simply does not see them.
        #[derive(Debug, PartialEq, Deserialize)]
        #[serde(tag = "state", rename_all = "snake_case")]
        enum PreviousRunStatus {
            Paused {
                #[serde(default)]
                usage: Option<Usage>,
                #[serde(default)]
                retries_used: u32,
            },
            Error {
                #[serde(default)]
                final_step_finish_reason: Option<FinishReason>,

                #[serde(default)]
                usage: Option<Usage>,
                #[serde(default)]
                retries_used: u32,
            },
        }
        assert_eq!(
            serde_json::from_value::<PreviousRunStatus>(with_cause).unwrap(),
            PreviousRunStatus::Paused {
                usage: None,
                retries_used: 0,
            }
        );
        assert_eq!(
            serde_json::from_value::<PreviousRunStatus>(serde_json::json!({
                "state": "error",
                "retries_used": 1,
                "error": {"class": "permanent", "message": "boom"}
            }))
            .unwrap(),
            PreviousRunStatus::Error {
                final_step_finish_reason: None,
                usage: None,
                retries_used: 1,
            }
        );
    }

    #[test]
    fn subscribe_events_round_trip_in_the_stream_shape() {
        let control = SubscribeEvent::Control {
            wal_seq: 8,
            sub_index: 2,
            unit: ControlUnit::RunStarted {
                run_id: RunId("run-1".into()),
                submission_id: None,
            },
        };
        let json = serde_json::to_value(&control).expect("control serializes");
        assert_eq!(
            json,
            serde_json::json!({
                "kind": "control",
                "cursor": {"wal_seq": 8, "sub_index": 2},
                "unit": {"type": "run_started", "run_id": "run-1"}
            })
        );
        assert_eq!(
            decode_subscribe_event(&serde_json::to_vec(&json).unwrap()).unwrap(),
            control
        );

        let display = SubscribeEvent::Display(DisplayEvent::DisplayGap { dropped: 3 });
        let json = serde_json::to_value(&display).expect("display serializes");
        assert_eq!(
            serde_json::from_value::<SubscribeEvent>(json).unwrap(),
            display
        );
    }

    fn payload_control() -> ControlUnit {
        ControlUnit::AssistantMessage {
            message: Arc::new(AssistantMessage {
                message_id: MessageId("message-1".into()),
                content: vec![
                    ContentBlock::Text {
                        text: "calling a tool".into(),
                    },
                    ContentBlock::ToolCall(ToolCall {
                        tool_call_id: ToolCallId("call-1".into()),
                        tool_name: "search".into(),
                        input: serde_json::json!({"query": "borrowed serialization"}),
                        thought_signature: None,
                    }),
                ],
            }),
        }
    }

    fn display_delta() -> DisplayEvent {
        DisplayEvent::TextDelta {
            message_id: MessageId("message-1".into()),
            tool_call_id: None,
            delta: "streamed text".into(),
        }
    }

    #[test]
    fn borrowed_subscribe_encoders_round_trip_payloads() {
        let unit = payload_control();
        let control = encode_subscribe_control(8, 2, &unit).expect("control event encodes");
        assert_eq!(
            decode_subscribe_event(&control).expect("control event decodes"),
            SubscribeEvent::Control {
                wal_seq: 8,
                sub_index: 2,
                unit,
            }
        );

        let event = display_delta();
        let display = encode_subscribe_display(&event).expect("display event encodes");
        assert_eq!(
            decode_subscribe_event(&display).expect("display event decodes"),
            SubscribeEvent::Display(event)
        );
    }

    #[test]
    fn borrowed_subscribe_encoders_match_legacy_json_shapes() {
        let unit = payload_control();
        let encoded = encode_subscribe_control(8, 2, &unit).expect("control event encodes");
        let actual: serde_json::Value =
            serde_json::from_slice(&encoded).expect("encoded control is JSON");
        let legacy = serde_json::json!({
            "kind": "control",
            "cursor": {"wal_seq": 8, "sub_index": 2},
            "unit": unit,
        });
        assert_eq!(actual, legacy);

        let event = display_delta();
        let encoded = encode_subscribe_display(&event).expect("display event encodes");
        let actual: serde_json::Value =
            serde_json::from_slice(&encoded).expect("encoded display is JSON");
        let legacy = serde_json::json!({
            "kind": "display",
            "event": event,
        });
        assert_eq!(actual, legacy);
    }

    /// Replaces `legacy_read_direction_is_ignored`, which pinned the leniency while prefrontal
    /// still sent `direction`. A misspelled or retired key now refuses by name instead of
    /// silently changing which page comes back.
    #[test]
    fn read_params_refuse_an_unknown_field_rather_than_change_the_page() {
        for body in [
            serde_json::json!({"from_ordinal": 2, "limit": 5, "direction": "newest_last"}),
            serde_json::json!({"fromOrdinal": 2, "limit": 5}),
            // The tools opt-in is a known field only in its exact spelling.
            serde_json::json!({"includeTools": true}),
            serde_json::json!({"include_tool": true}),
        ] {
            let err = serde_json::from_value::<ReadParams>(body.clone())
                .expect_err(&format!("must refuse {body}"));
            assert!(err.to_string().contains("unknown field"), "{err}");
        }
        // The legitimate shapes still decode: both fields, one of them, or neither (the newest
        // page). Without these arms, strictness could be satisfied by breaking real callers.
        let full: ReadParams =
            serde_json::from_value(serde_json::json!({"from_ordinal": 2, "limit": 5})).unwrap();
        assert_eq!((full.from_ordinal, full.limit), (Some(2), Some(5)));
        let limit_only: ReadParams =
            serde_json::from_value(serde_json::json!({"limit": 500})).unwrap();
        assert_eq!(
            (limit_only.from_ordinal, limit_only.limit),
            (None, Some(500))
        );
        let empty: ReadParams = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(empty, ReadParams::default());
        assert!(!empty.include_tools, "tools are opt-in");
        let with_tools: ReadParams =
            serde_json::from_value(serde_json::json!({"limit": 5, "include_tools": true})).unwrap();
        assert!(with_tools.include_tools);
        // A caller that does not opt in sends exactly the bytes it sent before the field existed.
        assert_eq!(
            serde_json::to_string(&ReadParams {
                from_ordinal: Some(2),
                limit: Some(5),
                ..ReadParams::default()
            })
            .unwrap(),
            r#"{"from_ordinal":2,"limit":5}"#
        );
    }
}

#[cfg(test)]
mod send_finished_tests {
    use super::*;

    #[test]
    fn finished_send_reply_pins_wire_bytes() {
        let result = SendResult::Finished {
            run_id: "run-1".into(),
            reason: broca_protocol::RunFinishReason::Interrupted.into(),
        };
        let bytes = br#"{"state":"finished","run_id":"run-1","reason":"interrupted"}"#;
        assert_eq!(serde_json::to_vec(&result).unwrap(), bytes);
        assert_eq!(serde_json::from_slice::<SendResult>(bytes).unwrap(), result);
    }
}

#[cfg(test)]
mod delivery_params {
    use super::*;

    fn send(delivery: serde_json::Value) -> Result<SendParams, serde_json::Error> {
        serde_json::from_value(serde_json::json!({
            "prompt": "hello",
            "model": {"provider": "mock", "model": "m"},
            "delivery": delivery,
        }))
    }

    /// The three modes decode; null reads as absent; an absent field serializes to no bytes.
    #[test]
    fn delivery_decodes_the_three_modes_and_null_as_absent() {
        for (wire, mode) in [
            ("queue", cortexkit_role_llm_runner::send::Delivery::Queue),
            ("steer", cortexkit_role_llm_runner::send::Delivery::Steer),
            (
                "interrupt",
                cortexkit_role_llm_runner::send::Delivery::Interrupt,
            ),
        ] {
            assert_eq!(
                send(serde_json::json!(wire)).unwrap().delivery,
                Some(SendDelivery::Mode(mode))
            );
        }
        let absent = send(serde_json::Value::Null).unwrap();
        assert_eq!(absent.delivery, None);
        assert!(serde_json::to_value(&absent)
            .unwrap()
            .get("delivery")
            .is_none());
    }

    /// An unknown mode is refused, and the refusal names the field and the accepted modes.
    #[test]
    fn an_unknown_delivery_is_refused_by_name() {
        for bad in [
            serde_json::json!("Steer"),
            serde_json::json!("followup"),
            serde_json::json!(1),
        ] {
            let error = send(bad.clone()).expect_err("unknown mode").to_string();
            assert!(
                error.contains("delivery must be \"queue\", \"steer\" or \"interrupt\""),
                "{bad}: {error}"
            );
        }
    }
}

#[cfg(test)]
mod delivery_order_params {
    use super::*;
    use broca_protocol::DeliveryMode;

    fn send(extra: serde_json::Value) -> Result<SendParams, serde_json::Error> {
        let mut params = serde_json::json!({
            "prompt": "hello",
            "model": {"provider": "mock", "model": "m"},
        });
        params
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        serde_json::from_value(params)
    }

    /// An ordered list decodes in order and keeps its bytes; it never reads as a legacy mode.
    #[test]
    fn an_ordered_list_decodes_and_round_trips() {
        let params = send(serde_json::json!({"delivery": ["tool_result", "steer"], "dedupe_key": "k", "expires_at_ms": 9})).unwrap();
        assert_eq!(
            params.delivery,
            Some(SendDelivery::Order(vec![
                DeliveryMode::ToolResult,
                DeliveryMode::Steer
            ]))
        );
        assert_eq!(params.legacy_delivery(), broca_protocol::Delivery::Queue);
        assert_eq!(
            serde_json::to_value(&params).unwrap()["delivery"],
            serde_json::json!(["tool_result", "steer"])
        );
        let order = params.delivery_order().unwrap().unwrap();
        assert_eq!(
            order.modes,
            vec![DeliveryMode::ToolResult, DeliveryMode::Steer]
        );
        assert_eq!(order.dedupe_key.as_deref(), Some("k"));
        assert_eq!(order.expires_at_ms, Some(9));
    }

    /// A send using none of the group has no order, whatever its legacy mode, so it runs exactly
    /// as before.
    #[test]
    fn legacy_strings_keep_their_meaning() {
        for wire in [
            serde_json::json!({}),
            serde_json::json!({"delivery": "queue"}),
            serde_json::json!({"delivery": "steer"}),
            serde_json::json!({"delivery": "interrupt"}),
        ] {
            assert_eq!(send(wire).unwrap().delivery_order().unwrap(), None);
        }
    }

    /// `dedupe_key`, `expires_at_ms` and `template` are valid only with an ordered list: with a
    /// legacy string or no `delivery` the send is refused naming the field, never run with the
    /// member ignored. A list retains the template for validation against the frozen plan.
    #[test]
    fn group_members_without_an_ordered_list_are_refused_by_name() {
        for delivery in [None, Some("queue"), Some("steer"), Some("interrupt")] {
            for (member, value) in [
                ("dedupe_key", serde_json::json!("k")),
                ("expires_at_ms", serde_json::json!(5)),
                (
                    "template",
                    serde_json::json!({"id": "t", "digest": "a".repeat(64), "slots": {}}),
                ),
            ] {
                let mut params = serde_json::json!({ member: value });
                if let Some(delivery) = delivery {
                    params["delivery"] = serde_json::json!(delivery);
                }
                let refused = send(params).unwrap().delivery_order().unwrap_err();
                assert_eq!(refused.0, member, "{delivery:?}");
            }
        }
        let listed =
            send(serde_json::json!({"delivery": ["steer"], "template": {"id": "t", "digest": "a".repeat(64), "slots": {}}}))
                .unwrap();
        assert_eq!(
            listed
                .delivery_order()
                .unwrap()
                .unwrap()
                .template
                .unwrap()
                .id,
            "t"
        );
        let empty = send(serde_json::json!({"delivery": ["steer"], "dedupe_key": ""})).unwrap();
        assert_eq!(empty.delivery_order().unwrap_err().0, "dedupe_key");
    }

    #[test]
    fn null_delivery_order_members_are_not_silently_omitted() {
        for member in ["dedupe_key", "expires_at_ms", "template"] {
            let error = send(serde_json::json!({"delivery":["steer"], member:null}))
                .unwrap_err()
                .to_string();
            assert!(error.contains(member), "{error}");
        }
    }

    /// Empty lists, repeated modes and unknown modes are refused naming the field.
    #[test]
    fn a_malformed_list_is_refused() {
        for bad in [
            serde_json::json!([]),
            serde_json::json!(["steer", "steer"]),
            serde_json::json!(["queue"]),
            serde_json::json!([1]),
        ] {
            let error = send(serde_json::json!({"delivery": bad.clone()}))
                .expect_err("refused")
                .to_string();
            assert!(error.contains("delivery"), "{bad}: {error}");
        }
    }
}

#[cfg(test)]
mod keep_warm_tests {
    use super::*;
    use broca_protocol::request::KeepWarm;

    #[test]
    fn send_keep_warm_is_strict_and_names_the_invalid_field() {
        for value in [
            serde_json::json!("always"),
            serde_json::json!(true),
            serde_json::json!(1),
            serde_json::json!({"auto": true}),
        ] {
            let params = serde_json::json!({"prompt": "hello", "keep_warm": value});
            let error = serde_json::from_value::<SendParams>(params.clone()).unwrap_err();
            assert_eq!(
                role::failing_field::<SendParams>(&params, &error).as_deref(),
                Some("keep_warm")
            );
        }
        assert!(send_params_keys().contains(&"keep_warm"));
        let absent: SendParams =
            serde_json::from_value(serde_json::json!({"prompt": "hello"})).unwrap();
        assert_eq!(absent.keep_warm.unwrap_or_default(), KeepWarm::Off);
        assert!(serde_json::to_value(&absent)
            .unwrap()
            .get("keep_warm")
            .is_none());
    }

    #[test]
    fn send_keep_warm_auto_and_off_match_goldens() {
        for (policy, file, hash) in [
            (
                "auto",
                "send_params.keep_warm_auto.json",
                "234f9522e4aa8c99aa67cf5baffda9456e3dfd704a8cebf7eb18ae91f11241d9",
            ),
            (
                "off",
                "send_params.keep_warm_off.json",
                "3bfe44f6013f981dcfd6939581512a5f6e7054881d0ac9d92f3ba60a741aa5c3",
            ),
        ] {
            let params: SendParams =
                serde_json::from_value(serde_json::json!({"prompt": "hello", "keep_warm": policy}))
                    .unwrap();
            scope_refusal_golden::assert_golden(params, file, hash);
        }
    }

    #[test]
    fn send_params_cache_shared_prefix_matches_golden() {
        let params: SendParams = serde_json::from_value(serde_json::json!({
            "cache": {"key": "comment-review-3f9a1c2b7d5e0a91"},
            "prompt_blocks": ["You are a concise reviewer.", "Review this comment."]
        }))
        .unwrap();
        scope_refusal_golden::assert_golden(
            params,
            "send_params.cache_shared_prefix.json",
            "88b2dd5ec22e75e42ee37fdbe719976e59514700fa2aacb63c6a26eff2581d66",
        );
    }
}

#[cfg(test)]
mod send_tool_shape_tests {
    use super::*;

    /// The `session.send` tool shape as callers send it and broca re-encodes it, before a
    /// declared tool could carry its own reply deadline. Absent members must keep costing no
    /// bytes, so these exact bytes survive every later optional member of `Tool`.
    #[test]
    fn send_declared_tools_keep_their_bytes() {
        let tools = r#"[{"name":"research","description":"Ask sidekick a question.","input_schema":{"properties":{"request":{"type":"string"}},"type":"object"},"module":"prefrontal-core","execution_mode":"parallel"},{"name":"note","description":"","input_schema":{"type":"object"},"execution_mode":"sequential"}]"#;
        let params: SendParams = serde_json::from_value(serde_json::json!({
            "prompt": "hello",
            "tools": serde_json::from_str::<serde_json::Value>(tools).unwrap(),
        }))
        .unwrap();
        assert_eq!(serde_json::to_string(&params.tools).unwrap(), tools);
        let decoded: Vec<Tool> = serde_json::from_str(tools).unwrap();
        assert_eq!(serde_json::to_string(&decoded).unwrap(), tools);
    }

    /// The `session.send` params Core sends for a lead session's long-running `research` tool:
    /// a send-declared tool with `reply: {max_ms: 1800000}`.
    #[test]
    fn send_params_tool_reply_matches_golden() {
        let params: SendParams = serde_json::from_value(serde_json::json!({
            "prompt": "Research how a resumed run keeps its frozen tools.",
            "tools": [{
                "name": "research",
                "description": "Ask sidekick a question.",
                "input_schema": {
                    "type": "object",
                    "properties": {"request": {"type": "string"}},
                    "required": ["request"]
                },
                "module": "prefrontal-core",
                "reply": {"max_ms": 1_800_000}
            }]
        }))
        .unwrap();
        assert!(check_tool_replies(&serde_json::to_value(&params).unwrap()).is_ok());
        assert_eq!(
            params.tools.as_ref().unwrap()[0].reply,
            Some(broca_protocol::ToolReply { max_ms: 1_800_000 })
        );
        scope_refusal_golden::assert_golden(
            params,
            "send_params.tool_reply.json",
            SEND_PARAMS_TOOL_REPLY_SHA256,
        );
    }

    const SEND_PARAMS_TOOL_REPLY_SHA256: &str =
        "501fb9c1284bb31577d149b884d2828771a0b5f76f319f5b3bab944612886338";

    /// Each malformed `tools[i].reply` is refused naming its exact member, and serde's own decode
    /// of the send refuses it too, so no path reads it as "no deadline".
    #[test]
    fn malformed_tool_replies_are_refused_naming_the_member() {
        let ok = serde_json::json!({"name": "ok", "description": "", "input_schema": {}});
        for (reply, field) in [
            (serde_json::json!({}), "tools[1].reply.max_ms"),
            (serde_json::json!({"max_ms": 0}), "tools[1].reply.max_ms"),
            (
                serde_json::json!({"max_ms": 86_400_001u64}),
                "tools[1].reply.max_ms",
            ),
            (
                serde_json::json!({"max_ms": "600000"}),
                "tools[1].reply.max_ms",
            ),
            (serde_json::json!({"max_ms": 1.5}), "tools[1].reply.max_ms"),
            (serde_json::json!({"max_ms": -1}), "tools[1].reply.max_ms"),
            (
                serde_json::json!({"max_ms": 600_000, "grace_ms": 5}),
                "tools[1].reply.grace_ms",
            ),
            (serde_json::json!(600_000), "tools[1].reply"),
            (serde_json::json!(null), "tools[1].reply"),
            (serde_json::json!([]), "tools[1].reply"),
        ] {
            let mut tool = ok.clone();
            tool["reply"] = reply.clone();
            let params = serde_json::json!({"prompt": "hi", "tools": [ok.clone(), tool]});
            let (refused, message) = check_tool_replies(&params).unwrap_err();
            assert_eq!(refused, field, "{reply}");
            assert!(message.starts_with(field), "{reply}: {message}");
            assert!(
                serde_json::from_value::<SendParams>(params).is_err(),
                "{reply} must not decode as a send"
            );
        }
        let declared = serde_json::json!({"prompt": "hi", "tools": [ok.clone(), {
            "name": "held", "description": "", "input_schema": {}, "reply": {"max_ms": 1}
        }]});
        assert!(check_tool_replies(&declared).is_ok());
        assert!(check_tool_replies(&serde_json::json!({"prompt": "hi"})).is_ok());
    }
}

#[cfg(test)]
mod restart_wire_tests {
    use super::*;

    #[test]
    fn restart_pause_status_and_invalid_policy_match_goldens() {
        scope_refusal_golden::assert_golden(
            RunStatusResponse::Paused {
                auth_selection: None,
                title: None,
                pause_reason: Some(broca_protocol::PauseReason::Restart.into()),
                usage: None,
                retries_used: 0,
                reason: Some("restart".into()),
                error: None,
                auth: None,
            },
            "run_status.paused_restart.json",
            "772ecd026752bc3e58e27e7ac300abef5408455a4ead4a8cfb7cf591c5bae3f8",
        );
        use sha2::{Digest, Sha256};
        let refusal = include_bytes!("../tests/golden/invalid_params.on_restart.json");
        assert_eq!(
            format!("{:x}", Sha256::digest(refusal)),
            "5ac94e184f148b0fc204e6647ec87c9a2bffbbeff864efd4fef582d17bfc7316"
        );
    }

    #[test]
    fn restart_policy_is_strict_and_defaults_to_interrupt() {
        for value in [
            serde_json::json!("always"),
            serde_json::json!(true),
            serde_json::json!(null),
            serde_json::json!(1),
        ] {
            let params = serde_json::json!({"prompt":"hi", "on_restart":value});
            let error = serde_json::from_value::<SendParams>(params.clone()).unwrap_err();
            assert_eq!(
                failing_field::<SendParams>(&params, &error).as_deref(),
                Some("on_restart")
            );
        }
        let absent: SendParams =
            serde_json::from_value(serde_json::json!({"prompt":"hi"})).unwrap();
        assert_eq!(
            absent.on_restart.unwrap_or_default(),
            broca_protocol::request::OnRestart::Interrupt
        );
        assert!(send_params_keys().contains(&"on_restart"));
    }
}

#[cfg(test)]
mod client_message_id_wire_tests {
    use super::*;

    /// Every value outside 1 to 64 characters of `[A-Za-z0-9_-]` (llm-runner 0.5.1 §9.11) is
    /// refused naming `client_message_id`: empty, one over the limit, a space, a dot, a
    /// non-ASCII letter, and values that are not strings.
    #[test]
    fn invalid_client_message_ids_are_refused_naming_the_field() {
        for id in [
            serde_json::json!(""),
            serde_json::json!("a".repeat(65)),
            serde_json::json!("has space"),
            serde_json::json!("dot.ted"),
            serde_json::json!("caf\u{e9}"),
            serde_json::Value::Null,
            serde_json::json!(7),
        ] {
            let params = serde_json::json!({"prompt": "hi", "client_message_id": id});
            let error = serde_json::from_value::<SendParams>(params.clone())
                .expect_err("an invalid client_message_id must be refused");
            assert_eq!(
                failing_field::<SendParams>(&params, &error).as_deref(),
                Some("client_message_id"),
                "{id:?}"
            );
        }
    }

    /// The grammar's edges are accepted verbatim, and an absent id stays absent and costs a
    /// serialized send no bytes.
    #[test]
    fn valid_client_message_ids_are_kept_and_an_absent_one_writes_nothing() {
        for id in ["a".to_owned(), "a".repeat(64), "Az09_-".to_owned()] {
            let parsed: SendParams = serde_json::from_value(
                serde_json::json!({"prompt": "hi", "client_message_id": id}),
            )
            .unwrap();
            assert_eq!(parsed.client_message_id.as_deref(), Some(id.as_str()));
        }
        let absent: SendParams =
            serde_json::from_value(serde_json::json!({"prompt": "hi"})).unwrap();
        assert_eq!(absent.client_message_id, None);
        assert!(serde_json::to_value(absent)
            .unwrap()
            .get("client_message_id")
            .is_none());
    }
}

#[cfg(test)]
mod title_wire_tests {
    use super::*;

    /// Every value the title contract refuses: empty, blank, a control character (newline, tab
    /// and the rest), one character over the limit in a one-byte and a two-byte script, and
    /// values that are not strings at all. Each is refused naming `title`, never cut down.
    #[test]
    fn invalid_titles_are_refused_naming_the_field() {
        for title in [
            serde_json::json!(""),
            serde_json::json!("   "),
            serde_json::json!("\t"),
            serde_json::json!("line one\nline two"),
            serde_json::json!("tab\there"),
            serde_json::json!("carriage\rreturn"),
            serde_json::json!("bell\u{7}"),
            serde_json::json!("del\u{7f}"),
            serde_json::json!("next-line\u{85}"),
            serde_json::json!("x".repeat(TITLE_MAX_CHARS + 1)),
            serde_json::json!("é".repeat(TITLE_MAX_CHARS + 1)),
            serde_json::Value::Null,
            serde_json::json!(42),
            serde_json::json!(["a"]),
        ] {
            let params = serde_json::json!({"prompt": "hi", "title": title});
            let error = serde_json::from_value::<SendParams>(params.clone())
                .expect_err("an invalid title must be refused");
            assert_eq!(
                failing_field::<SendParams>(&params, &error).as_deref(),
                Some("title"),
                "{title:?}"
            );
            assert_eq!(
                error.to_string(),
                "title must be a string of 1-200 characters that is not blank and has no control characters"
            );
        }
    }

    /// The limit counts Unicode scalar values, not bytes: 200 two-byte characters (400 bytes)
    /// and 200 four-byte emoji are accepted whole, as is a title with inner spaces. An absent
    /// title stays absent and costs a serialized send no bytes.
    #[test]
    fn valid_titles_are_kept_verbatim_and_an_absent_one_writes_nothing() {
        for title in [
            "Fix the login bug".to_owned(),
            "x".repeat(TITLE_MAX_CHARS),
            "é".repeat(TITLE_MAX_CHARS),
            "🧪".repeat(TITLE_MAX_CHARS),
            " leading and trailing spaces ".to_owned(),
        ] {
            let parsed: SendParams =
                serde_json::from_value(serde_json::json!({"prompt": "hi", "title": title}))
                    .unwrap();
            assert_eq!(parsed.title.as_deref(), Some(title.as_str()));
        }
        let absent: SendParams =
            serde_json::from_value(serde_json::json!({"prompt": "hi"})).unwrap();
        assert_eq!(absent.title, None);
        assert!(serde_json::to_value(absent).unwrap().get("title").is_none());
    }

    /// The `session.send` request a caller sends with a title, as a golden fixture. It decodes with the title
    /// carried verbatim and every other member exactly as without it.
    #[test]
    fn a_titled_send_request_matches_its_golden() {
        use sha2::{Digest, Sha256};
        let bytes = include_bytes!("../tests/golden/send_request.title.json");
        assert_eq!(
            format!("{:x}", Sha256::digest(bytes)),
            "51c0652eba5b5a8bbe59d66908f11af7b4415138514613045efdb67132cd8da7"
        );
        let titled: SendParams = serde_json::from_slice(bytes).unwrap();
        assert_eq!(titled.title.as_deref(), Some("Fix the login bug"));
        let mut untitled_json: serde_json::Value = serde_json::from_slice(bytes).unwrap();
        untitled_json.as_object_mut().unwrap().remove("title");
        let untitled: SendParams = serde_json::from_value(untitled_json).unwrap();
        let mut titled_value = serde_json::to_value(&titled).unwrap();
        titled_value.as_object_mut().unwrap().remove("title");
        assert_eq!(titled_value, serde_json::to_value(&untitled).unwrap());
    }

    /// The `run.status` reply for a completed run whose send carried a title, as a golden fixture; the same
    /// file is compared with a real reply in the serve-path tests. A reply without a title keeps
    /// its earlier bytes (see `run_status.completed_no_undelivered_steers.json`).
    #[test]
    fn a_titled_run_status_matches_its_golden() {
        use sha2::{Digest, Sha256};
        let bytes = include_bytes!("../tests/golden/run_status.completed_title.json");
        assert_eq!(
            format!("{:x}", Sha256::digest(bytes)),
            "6f19b40f6f7fceaccc2f3a327a29851b867b0478c2d9004e1f9855d007420cca"
        );
        let reply: serde_json::Value = serde_json::from_slice(bytes).unwrap();
        let status: RunStatusResponse = serde_json::from_value(reply["result"].clone()).unwrap();
        assert_eq!(
            status,
            RunStatusResponse::Completed {
                auth_selection: None,
                directives: None,
                final_step_finish_reason: Some(FinishReason::Stop),
                undelivered_steers: Some(Vec::new()),
                usage: Some(Default::default()),
                retries_used: 0,
                title: Some("Fix the login bug".into()),
                structured_output: None,
            }
        );
        assert_eq!(status.title(), Some("Fix the login bug"));
        let encoded = serde_json::to_vec(&status).unwrap();
        let golden = serde_json::to_vec(&reply["result"]).unwrap();
        let normalized: serde_json::Value =
            serde_json::from_slice(&crate::usage_compat::legacy_bytes(&encoded, &golden)).unwrap();
        assert_eq!(normalized, reply["result"]);
        let untitled =
            include_bytes!("../tests/golden/run_status.completed_no_undelivered_steers.json");
        let untitled: serde_json::Value = serde_json::from_slice(untitled).unwrap();
        let decoded: RunStatusResponse =
            serde_json::from_value(untitled["result"].clone()).unwrap();
        assert_eq!(decoded.title(), None);
        let bytes = serde_json::to_vec(&decoded).unwrap();
        let golden = serde_json::to_vec(&untitled["result"]).unwrap();
        let normalized: serde_json::Value =
            serde_json::from_slice(&crate::usage_compat::legacy_bytes(&bytes, &golden)).unwrap();
        assert_eq!(normalized, untitled["result"]);
        // A live run's status carries the title too, and an untitled one keeps its bare shape.
        assert_eq!(
            serde_json::to_value(RunStatusResponse::Active {
                auth_selection: None,
                reason: None,
                deadline_ms: None,
                usage: None,
                step: None,
                title: None,
                awaiting_approval: None
            })
            .unwrap(),
            serde_json::json!({"state": "active"})
        );
        assert_eq!(
            serde_json::from_value::<RunStatusResponse>(
                serde_json::json!({"state": "active", "title": "t"})
            )
            .unwrap(),
            RunStatusResponse::Active {
                auth_selection: None,
                reason: None,
                deadline_ms: None,
                usage: None,
                awaiting_approval: None,
                step: None,
                title: Some("t".into())
            }
        );
    }

    /// The refusal of an invalid title, as a golden fixture; the serve-path tests compare a real refusal
    /// with the same file.
    #[test]
    fn the_invalid_title_refusal_matches_its_golden() {
        use sha2::{Digest, Sha256};
        let bytes = include_bytes!("../tests/golden/invalid_params.title.json");
        assert_eq!(
            format!("{:x}", Sha256::digest(bytes)),
            "8b4ac71bb3f8760677cfc5e6256b2d13831d83565920f790346e031d5c1cb0ba"
        );
        let refusal: serde_json::Value = serde_json::from_slice(bytes).unwrap();
        assert_eq!(refusal["code"], "invalid_params");
        assert_eq!(refusal["detail"], serde_json::json!({"field": "title"}));
        let error =
            serde_json::from_value::<SendParams>(serde_json::json!({"prompt": "hi", "title": ""}))
                .unwrap_err();
        assert_eq!(refusal["message"], error.to_string());
    }
}

#[cfg(test)]
mod late_page_goldens {
    use super::*;
    fn page(key: &str, id: &str, text: &str, next: Option<&str>) -> SessionReadResponse {
        let mut page: SessionReadResponse = serde_json::from_str(
            r#"{"messages":[],"lineage_state":{"last_run_id":"r","state":"interrupted"}}"#,
        )
        .unwrap();
        page.late_approvals = Some(vec![LateApprovalView {
            call_key: key.into(),
            elicitation_id: id.into(),
            decision: "approved".into(),
            outcome: Some("completed".into()),
            result: Some(broca_protocol::ToolResult {
                tool_call_id: broca_protocol::ToolCallId("c".into()),
                output: broca_protocol::content::ToolOutput::Text { text: text.into() },
                is_error: false,
            }),
        }]);
        page.next_late_after = next.map(String::from);
        page
    }
    fn pin<T: Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug>(
        value: T,
        golden: &str,
        digest: &str,
    ) {
        use sha2::{Digest, Sha256};
        let golden = golden.trim();
        assert_eq!(format!("{:x}", Sha256::digest(golden)), digest);
        assert_eq!(serde_json::to_string(&value).unwrap(), golden);
        assert_eq!(serde_json::from_str::<T>(golden).unwrap(), value);
    }
    #[test]
    fn late_default_page_golden() {
        pin(
            page("l:4", "el-1", "late-one", Some("l:4")),
            include_str!("../tests/golden/session_read.late_default.json"),
            "0af9d6174d5d35b549a6088420887ead13c406f71fbf7ec5aa039af20fe0df58",
        );
    }
    #[test]
    fn late_continuation_page_golden() {
        pin(
            page("l:5", "el-2", "late-two", Some("l:5")),
            include_str!("../tests/golden/session_read.late_continuation.json"),
            "f71c405440445d913c20be34ef454833811bef73f3788cd21881fb22c4beb974",
        );
    }
    #[test]
    fn late_final_page_golden() {
        pin(
            page("l:6", "el-3", "late-three", None),
            include_str!("../tests/golden/session_read.late_final.json"),
            "832e846bddd1c0e7c6b875cd288291abf6132eb6b886aa6872ef1fd4aef019a0",
        );
    }
    #[test]
    fn late_bad_cursor_golden() {
        pin(
            InvalidLateCursor::new("gone:1"),
            include_str!("../tests/golden/session_read.late_bad_cursor.json"),
            "b9fbfb59a40ebcaa79108eef3a67ce9966deac61d1a1d3cb48e925ea6ed360b6",
        );
    }
    #[test]
    fn late_after_is_optional_and_stable() {
        let params: ReadParams = serde_json::from_str(r#"{"late_after":"l:4"}"#).unwrap();
        assert_eq!(params.late_after.as_deref(), Some("l:4"));
        assert_eq!(
            serde_json::to_string(&params).unwrap(),
            r#"{"late_after":"l:4"}"#
        );
    }
}

/// A reader-only build cannot freeze or advance a session under an agent-run scope.
pub const AGENT_RUN_SCOPES_UNSUPPORTED: &str = "agent_run_scopes_unsupported";
/// A later send changed the agent-run or agent identifier frozen on the session.
pub const SCOPE_RUN_MISMATCH: &str = "scope_run_mismatch";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunMismatchField {
    RunId,
    AgentId,
}

/// Refuse a send that changed the session's fixed identifiers. `detail.field` says whether
/// `run_id` or `agent_id` changed; repeating the send cannot fix the refusal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeRunMismatch {
    pub code: String,
    pub message: String,
    pub detail: RunMismatchDetail,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunMismatchDetail {
    pub field: RunMismatchField,
}

impl ScopeRunMismatch {
    pub fn new(field: RunMismatchField) -> Self {
        let name = match field {
            RunMismatchField::RunId => "run_id",
            RunMismatchField::AgentId => "agent_id",
        };
        Self {
            code: SCOPE_RUN_MISMATCH.into(),
            message: format!("this send differs from the session's frozen {name}"),
            detail: RunMismatchDetail { field },
        }
    }
}

/// Refusal from a build with agent-run scope writing disabled. Includes the full session
/// scope so callers can identify the session whose work cannot start or resume.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunScopesUnsupported {
    pub code: String,
    pub message: String,
    pub class: broca_protocol::OpenErrorClass,
    pub detail: broca_protocol::SessionScope,
}

impl AgentRunScopesUnsupported {
    pub fn new(scope: broca_protocol::SessionScope) -> Self {
        Self {
            code: AGENT_RUN_SCOPES_UNSUPPORTED.into(),
            message:
                "this build reads agent-run scopes but cannot start or advance work under them"
                    .into(),
            class: broca_protocol::ErrorClass::Permanent.into(),
            detail: scope,
        }
    }
}

#[cfg(test)]
mod agent_run_scope_tests {
    use super::*;

    #[test]
    fn agent_run_scope_refusal_bytes_are_pinned() {
        for principal in ["reserved:prefrontal-core", "reserved:other"] {
            let refusal = ScopeOwnerMismatch::new(ScopeOwnerMismatchDetail::AgentRunSender {
                owner: "reserved:prefrontal-core".into(),
                scope_ref: "run-1".into(),
                scope_epoch: 3,
                presented_principal: Some(principal.into()),
            });
            let pinned = format!(
                r#"{{"code":"scope_owner_mismatch","message":"an agent-run scope owned by 'reserved:prefrontal-core' accepts sends only from its reserved:basal carrier under the session's scope identity, never from its owner","detail":{{"kind":"agent_run_sender","owner":"reserved:prefrontal-core","ref":"run-1","scope_epoch":3,"presented_principal":"{principal}"}}}}"#
            );
            assert_eq!(serde_json::to_string(&refusal).unwrap(), pinned);
            assert_eq!(
                serde_json::from_str::<ScopeOwnerMismatch>(&pinned).unwrap(),
                refusal
            );
        }
        for (field, pinned) in [
            (
                RunMismatchField::RunId,
                r#"{"code":"scope_run_mismatch","message":"this send differs from the session's frozen run_id","detail":{"field":"run_id"}}"#,
            ),
            (
                RunMismatchField::AgentId,
                r#"{"code":"scope_run_mismatch","message":"this send differs from the session's frozen agent_id","detail":{"field":"agent_id"}}"#,
            ),
        ] {
            assert_eq!(
                serde_json::to_string(&ScopeRunMismatch::new(field)).unwrap(),
                pinned
            );
            let decoded: ScopeRunMismatch = serde_json::from_str(pinned).unwrap();
            assert_eq!(decoded.detail.field, field);
        }
        let scope = serde_json::from_str(r#"{"owner":{"kind":"reserved","module_id":"prefrontal-core"},"ref":"run-1","scope_epoch":3,"sender":{"kind":"reserved","module_id":"basal"},"agent_id":"agent-1","run_id":"run-1"}"#).unwrap();
        let refusal = AgentRunScopesUnsupported::new(scope);
        let pinned = r#"{"code":"agent_run_scopes_unsupported","message":"this build reads agent-run scopes but cannot start or advance work under them","class":"permanent","detail":{"owner":{"kind":"reserved","module_id":"prefrontal-core"},"ref":"run-1","scope_epoch":3,"sender":{"kind":"reserved","module_id":"basal"},"agent_id":"agent-1","run_id":"run-1"}}"#;
        assert_eq!(serde_json::to_string(&refusal).unwrap(), pinned);
        assert_eq!(
            serde_json::from_str::<AgentRunScopesUnsupported>(pinned).unwrap(),
            refusal
        );
    }
}

#[cfg(test)]
mod session_class_tests {
    use super::SendParams;
    use serde_json::{json, Value};

    fn refusal(value: Value) {
        let error =
            serde_json::from_value::<SendParams>(json!({"session_class": value})).unwrap_err();
        assert!(error.to_string().contains("session_class"), "{error}");
    }
    #[test]
    fn absent_class_preserves_wire_bytes() {
        let request: SendParams = serde_json::from_value(json!({})).unwrap();
        assert!(request.session_class.is_none());
        assert!(serde_json::to_value(request)
            .unwrap()
            .get("session_class")
            .is_none());
    }
    #[test]
    fn accepts_lowercase_digits_underscore_hyphen_and_length_boundaries() {
        for class in [
            "a",
            "0",
            "_",
            "-",
            "head",
            "saved_2-head",
            "abcdefghijklmnopqrstuvwxyz012345",
        ] {
            let request: SendParams =
                serde_json::from_value(json!({"session_class":class})).unwrap();
            assert_eq!(request.session_class.as_deref(), Some(class));
        }
    }
    #[test]
    fn refuses_empty_class() {
        refusal(json!(""));
    }
    #[test]
    fn refuses_long_class() {
        refusal(json!("a".repeat(33)));
    }
    #[test]
    fn refuses_uppercase_class() {
        refusal(json!("Head"));
    }
    #[test]
    fn refuses_whitespace_class() {
        for value in [" ", "head x", "head\n", "\thead"] {
            refusal(json!(value));
        }
    }
    #[test]
    fn refuses_path_class() {
        for value in ["head/x", "..", "head\\x", "head.x"] {
            refusal(json!(value));
        }
    }
    #[test]
    fn refuses_unicode_class() {
        for value in ["hé", "あ", "🦀"] {
            refusal(json!(value));
        }
    }
    #[test]
    fn refuses_null_class() {
        refusal(Value::Null);
    }
    #[test]
    fn refuses_nonstring_class() {
        for value in [json!(1), json!(true), json!([]), json!({})] {
            refusal(value);
        }
    }
}

#[cfg(test)]
mod caller_message_tests {
    use super::*;
    use broca_protocol::content::{ContentBlock, Role, TextProviderFormat, TextProviderState};
    use serde_json::json;

    fn read_message(content: Vec<ContentBlock>) -> SessionReadMessage {
        SessionReadMessage {
            delivered_as: None,
            tags: Vec::new(),
            ordinal: 3,
            mid: "m3".into(),
            message: broca_protocol::Message::new(Role::Assistant, content),
            run: None,
            tool_calls: Vec::new(),
            described: None,
            digest: None,
            part_digests: None,
        }
    }

    /// The member names of a JSON object, sorted.
    fn members(value: &serde_json::Value) -> Vec<String> {
        let mut names: Vec<String> = value.as_object().unwrap().keys().cloned().collect();
        names.sort();
        names
    }

    /// A consumer that re-encodes read messages byte-exactly decodes them strictly, so the
    /// default (inline) read adds no member to an entry, its message or any block, whatever
    /// the computed tag rules attach to the stored message. Only an opt-in descriptors read adds
    /// `digest` and `part_digests`. The member sets below are the ones served before the
    /// computed rules.
    #[test]
    fn the_default_read_shape_has_exactly_the_members_it_always_had() {
        use broca_protocol::content::{EncryptedReasoningFormat, ToolCall, ToolOutput, ToolResult};
        let result = |id: &str, output: ToolOutput| {
            ContentBlock::ToolResult(ToolResult {
                tool_call_id: broca_protocol::ToolCallId(id.into()),
                output,
                is_error: false,
            })
        };
        let content = vec![
            ContentBlock::Text {
                text: "§3§ plain".into(),
            },
            ContentBlock::TextWithState {
                text: "stateful".into(),
                provider_state: TextProviderState {
                    format: TextProviderFormat::OpenAiResponses,
                    data: "{}".into(),
                },
            },
            ContentBlock::Reasoning {
                text: "thought".into(),
                signature: Some("sig".into()),
            },
            ContentBlock::RedactedReasoning {
                data: "opaque".into(),
                provider_format: Some(EncryptedReasoningFormat::OpenAiResponses),
            },
            ContentBlock::ToolCall(ToolCall {
                tool_call_id: broca_protocol::ToolCallId("c".into()),
                tool_name: "t".into(),
                input: json!({"a": 1}),
                thought_signature: Some("ts".into()),
            }),
            result("c", ToolOutput::Text { text: "out".into() }),
            result("d", ToolOutput::Json { value: json!({}) }),
            result(
                "e",
                ToolOutput::Error {
                    message: "no".into(),
                },
            ),
            result(
                "f",
                ToolOutput::Denied {
                    reason: "no".into(),
                },
            ),
        ];
        let mut entry = computed(read_message(content.clone()), true);
        entry.message.origin = Some(broca_protocol::ModelHandle::new("p", "m"));
        let body = serde_json::to_value(&entry).unwrap();
        assert_eq!(members(&body), ["message", "mid", "ordinal", "tags"]);
        assert_eq!(members(&body["message"]), ["content", "origin", "role"]);
        let blocks: Vec<Vec<String>> = body["message"]["content"]
            .as_array()
            .unwrap()
            .iter()
            .map(members)
            .collect();
        let expected: Vec<Vec<&str>> = vec![
            vec!["text", "type"],
            vec!["text", "type"],
            vec!["signature", "text", "type"],
            vec!["data", "provider_format", "type"],
            vec![
                "input",
                "thought_signature",
                "tool_call_id",
                "tool_name",
                "type",
            ],
            vec!["is_error", "output", "tool_call_id", "type"],
            vec!["is_error", "output", "tool_call_id", "type"],
            vec!["is_error", "output", "tool_call_id", "type"],
            vec!["is_error", "output", "tool_call_id", "type"],
        ];
        assert_eq!(blocks, expected);
        let outputs: Vec<Vec<String>> = body["message"]["content"].as_array().unwrap()[5..]
            .iter()
            .map(|block| members(&block["output"]))
            .collect();
        assert_eq!(
            outputs,
            [
                vec!["kind", "text"],
                vec!["kind", "value"],
                vec!["kind", "message"],
                vec!["kind", "reason"]
            ]
        );
        // The same message without the computed rules serializes to the same members.
        let plain = serde_json::to_value(read_message(content)).unwrap();
        assert_eq!(
            members(&plain["message"]["content"][0]),
            members(&body["message"]["content"][0])
        );
        // The rows carry only the key and, on a counted part, the number.
        for row in body["tags"].as_array().unwrap() {
            let names = members(row);
            assert!(names == ["part"] || names == ["part", "tag"], "{names:?}");
        }
    }

    /// Number `entry`'s message under the computed rules of a tag-rendering run, cleaning the
    /// model's text (`clean`) or not.
    fn computed(mut entry: SessionReadMessage, clean: bool) -> SessionReadMessage {
        let mut high = 0;
        broca_protocol::tags::number_message(
            &mut entry.message,
            broca_protocol::tags::ComputedRules {
                render: true,
                clean,
            },
            &mut high,
        )
        .unwrap();
        entry.tags = tag_rows(entry.ordinal, &entry.message);
        entry
    }

    /// A read shows the model's text cleaned of tag notation when the run that wrote it cleans,
    /// as written otherwise (an untagged run, a structured-output run's final text); the stored
    /// message keeps the raw text either way.
    #[test]
    fn a_read_cleans_the_model_text_only_where_its_run_cleans() {
        let raw = "§3§ hi as §1§ says";
        let content = vec![ContentBlock::Text { text: raw.into() }];
        let cleaned = computed(read_message(content.clone()), true);
        let body = serde_json::to_value(&cleaned).unwrap();
        assert_eq!(body["message"]["content"][0]["text"], "hi as  says");
        assert_eq!(body["tags"], json!([{"part": "m3#0", "tag": "§1§"}]));
        assert_eq!(cleaned.message.content, content, "stored text stays raw");
        let raw_kept = computed(read_message(content.clone()), false);
        let body = serde_json::to_value(&raw_kept).unwrap();
        assert_eq!(body["message"]["content"][0]["text"], raw);
    }

    /// A descriptors read serves the full inline message's content identities and replaces a
    /// payload over the inline threshold in place by a ref; small payloads and the message's
    /// structure stay as an inline read serves them.
    #[test]
    fn a_descriptors_read_describes_large_payloads_in_place_with_inline_identities() {
        use cortexkit_role_llm_runner::read::descriptors::PART_INLINE_MAX;
        let large = "x".repeat(PART_INLINE_MAX + 1);
        let mut entry = read_message(vec![
            ContentBlock::Text {
                text: "small".into(),
            },
            ContentBlock::Text {
                text: large.clone(),
            },
        ]);
        let inline = serde_json::to_value(&entry).unwrap();
        entry.describe().unwrap();
        let body = serde_json::to_value(&entry).unwrap();
        assert_eq!(
            body["digest"],
            json!(cortexkit_role_llm_runner::content_digest::content_digest(
                &inline["message"]
            ))
        );
        assert_eq!(
            body["part_digests"][1]["digest"],
            json!(cortexkit_role_llm_runner::content_digest::content_digest(
                &inline["message"]["content"][1]
            ))
        );
        assert_eq!(body["part_digests"][0]["part"], "m3#0");
        assert_eq!(body["message"]["content"][0]["text"], "small");
        let reference = &body["message"]["content"][1]["text"];
        assert_eq!(reference["type"], "text_ref");
        assert_eq!(reference["byte_size"], json!(large.len()));
        assert!(reference.get("part").is_none(), "a nested ref has no key");
        cortexkit_role_llm_runner::read::descriptors::validate_references(&body["message"])
            .unwrap();
        // An inline read of the same entry is unchanged.
        let mut again = read_message(entry.message.content.clone());
        again.ordinal = entry.ordinal;
        assert_eq!(serde_json::to_value(&again).unwrap(), inline);
    }

    /// A read entry carries one tag row per tagged part, after `tool_calls`; the message
    /// itself keeps the exact bytes it had before tags existed, and an untagged entry has no
    /// `tags` member at all.
    #[test]
    fn tagged_entries_carry_part_tag_rows_beside_an_unchanged_message() {
        let content = vec![
            ContentBlock::Text {
                text: "first".into(),
            },
            ContentBlock::Text {
                text: "second".into(),
            },
        ];
        let plain = read_message(content.clone());
        let mut tagged = read_message(content);
        tagged.message.set_tags(Some(std::sync::Arc::new(
            broca_protocol::tags::PartTags::all([7, 8]),
        )));
        tagged.tags = tag_rows(tagged.ordinal, &tagged.message);
        assert!(tag_rows(plain.ordinal, &plain.message).is_empty());

        let plain_json = serde_json::to_value(&plain).unwrap();
        let tagged_json = serde_json::to_value(&tagged).unwrap();
        assert!(plain_json.get("tags").is_none());
        assert_eq!(tagged_json["message"], plain_json["message"]);
        assert_eq!(
            serde_json::to_string(&tagged).unwrap(),
            r#"{"ordinal":3,"mid":"m3","message":{"role":"assistant","content":[{"type":"text","text":"first"},{"type":"text","text":"second"}]},"tags":[{"part":"m3#0","tag":"§7§"},{"part":"m3#1","tag":"§8§"}]}"#
        );
        let decoded: SessionReadMessage = serde_json::from_value(tagged_json).unwrap();
        assert_eq!(decoded.tags, tagged.tags);
    }

    /// A caller reads the transcript's text, never the provider bytes kept to send back. A
    /// worker's final reply carrying a ChatGPT phase must reach the caller as plain text, or a
    /// parser looking for its text sees a serialized envelope instead.
    #[test]
    fn text_with_provider_state_reaches_callers_as_plain_text() {
        let message = read_message(vec![
            ContentBlock::Text {
                text: "before ".into(),
            },
            ContentBlock::TextWithState {
                text: "<result>done</result>".into(),
                provider_state: TextProviderState {
                    format: TextProviderFormat::OpenAiResponsesChatgpt,
                    data: r#"{"id":"msg_1","phase":"final_answer"}"#.into(),
                },
            },
        ]);
        let wire = serde_json::to_value(&message).unwrap();
        assert_eq!(
            wire["message"]["content"],
            json!([
                {"type": "text", "text": "before "},
                {"type": "text", "text": "<result>done</result>"},
            ])
        );
        assert!(!wire.to_string().contains("provider_state"));
        assert!(!wire.to_string().contains("final_answer"));
    }

    #[test]
    fn borrowed_stateful_message_keeps_cache_and_origin_metadata() {
        let mut message = broca_protocol::Message::assistant_from(
            vec![ContentBlock::TextWithState {
                text: "final text".into(),
                provider_state: TextProviderState {
                    format: TextProviderFormat::OpenAiResponsesChatgpt,
                    data: "opaque".into(),
                },
            }],
            broca_protocol::ModelHandle::new("openai", "gpt-test"),
        );
        message.cache_prefix_blocks = Some(1);
        let response = SessionReadMessage {
            delivered_as: None,
            tags: Vec::new(),
            ordinal: 0,
            mid: "m0".into(),
            message,
            run: None,
            tool_calls: Vec::new(),
            described: None,
            digest: None,
            part_digests: None,
        };

        let wire = serde_json::to_value(&response).unwrap();
        assert_eq!(wire["message"]["cache_prefix_blocks"], 1);
        assert_eq!(wire["message"]["origin"]["provider_module_id"], "openai");
        assert_eq!(wire["message"]["origin"]["model_id"], "gpt-test");
        assert_eq!(
            wire["message"]["content"],
            json!([{"type": "text", "text": "final text"}])
        );
    }

    #[test]
    fn stateful_caller_message_serializer_uses_borrowed_content() {
        let source = include_str!("lib.rs");
        let start = source.find("fn serialize_caller_message").unwrap();
        let end = source[start..]
            .find("/// One message in a `session.read` page.")
            .map(|offset| start + offset)
            .unwrap();
        let serializer = &source[start..end];

        assert!(serializer.contains("CallerMessageRef(message).serialize(serializer)"));
        assert!(
            !serializer.contains(".clone()"),
            "caller-facing state conversion must not clone message blocks"
        );
    }

    /// Messages without provider state serialize exactly as the canonical message does, so the
    /// conversion never moves bytes of an ordinary page.
    #[test]
    fn messages_without_provider_state_are_unchanged() {
        let message = read_message(vec![ContentBlock::Text {
            text: "plain".into(),
        }]);
        assert_eq!(
            serde_json::to_value(&message).unwrap()["message"],
            serde_json::to_value(&message.message).unwrap()
        );
    }

    /// A summary-bearing OpenAI Responses reasoning item is written exactly as stored, with no
    /// derived readable part. Consumers line part-level data up with the stored block positions,
    /// so an inserted block would shift every later part away from its stored index.
    #[test]
    fn responses_reasoning_summary_is_read_as_the_stored_blocks_only() {
        let item = r#"{"id":"rs_1","type":"reasoning","summary":[{"type":"summary_text","text":"**Planning** check inputs"}],"encrypted_content":"cipher"}"#;
        let message = read_message(vec![
            ContentBlock::RedactedReasoning {
                data: item.into(),
                provider_format: Some(broca_protocol::EncryptedReasoningFormat::OpenAiResponses),
            },
            ContentBlock::Text {
                text: "All good.".into(),
            },
        ]);
        assert_eq!(
            serde_json::to_value(&message).unwrap()["message"]["content"],
            json!([
                {"type": "redacted_reasoning", "data": item, "provider_format": "open_ai_responses"},
                {"type": "text", "text": "All good."},
            ])
        );
    }
}
pub use broca_protocol::run_events;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunEventGetParams {
    pub event_key: String,
}

/// The JCS string is the exact UTF-8 byte sequence hashed by the bus notification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum RunEventGetResponse {
    Found { event_key: String, jcs: String },
    Absent,
}
