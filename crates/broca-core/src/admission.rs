//! Compact durable facts needed by admission before replaying a WAL tail.
//!
//! The checkpoint is a projection of the WAL, never an authority independent of it. Callers
//! accept it only at the store watermark and then fold every durable record through the WAL head.

use std::sync::Arc;

use broca_protocol::{
    canonical_json_bytes, content::Message, request::SubmissionRequest, BindPrincipal, RunId,
    SessionScope, SubmissionId, Usage,
};
use broca_wal::RunConfig;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::PendingSubmission;

pub(crate) fn attempt_credential_for_send(
    frozen: &broca_wal::record::AttemptCredential,
    served_account_ref: Option<String>,
    auth_refreshed: bool,
) -> broca_wal::record::AttemptCredential {
    broca_wal::record::AttemptCredential {
        credential_ref: if auth_refreshed {
            None
        } else {
            frozen
                .auth_selection
                .as_ref()
                .and_then(|auth| auth.resolved_credential_id.clone())
        },
        account_ref: served_account_ref,
        ..frozen.clone()
    }
}

fn logical_input(request: &SubmissionRequest) -> std::borrow::Cow<'_, [Message]> {
    if request.content.is_empty() {
        return std::borrow::Cow::Borrowed(&request.input);
    }
    let mut input = request.input.clone();
    for message in &mut input {
        message
            .content
            .retain(|block| !matches!(block, broca_protocol::ContentBlock::Media(_)));
    }
    std::borrow::Cow::Owned(input)
}

/// Return the first caller-payload field that differs between two deterministic sends.
///
/// The exhaustive destructuring is intentional: adding a submission field cannot silently
/// escape send-id identity checks. [`submission_identity_digest`] serializes this same field set.
pub fn submission_difference(
    existing: &SubmissionRequest,
    requested: &SubmissionRequest,
) -> Option<&'static str> {
    let SubmissionRequest {
        message_marks: _,
        route_decision_id: existing_route_decision_id,
        account_route: existing_account_route,
        staged_uploads: _,
        content: existing_content,
        directives: existing_directives,
        project_id: _, // Record the bind in the WAL, but Some/None must not change send-id identity.
        principal: _,  // Same send from two principals is one send; the principal is only recorded.
        scope: _, // The route's scope stamp: checked against the session's owner, never part of send identity.
        on_restart: existing_on_restart,
        model: existing_model,
        input: _,
        tools: existing_tools,
        policy_allowed: existing_policy_allowed,
        tool_choice: existing_tool_choice,
        generation: existing_generation,
        cache: existing_cache,
        budget: existing_budget,
        auth: existing_auth,
        context_limit: existing_context_limit,
        work_class: _,
        variant: existing_variant,
        service_tier: existing_service_tier,
        service_tier_level: existing_service_tier_level,
        stop_when: existing_stop_when,
        context_management: existing_context_management,
        inherit_frozen: existing_inherit_frozen,
        structured_output: existing_structured_output,
        // Compared from the stored identity digest instead (`digest_client_message_id`): a
        // request rebuilt from a durable `Queued` record never carries it.
        client_message_id: _,
    } = existing;
    let SubmissionRequest {
        message_marks: _,
        route_decision_id: requested_route_decision_id,
        account_route: requested_account_route,
        staged_uploads: _,
        content: requested_content,
        directives: requested_directives,
        project_id: _, // Record the bind in the WAL, but Some/None must not change send-id identity.
        principal: _,  // Same send from two principals is one send; the principal is only recorded.
        scope: _, // The route's scope stamp: checked against the session's owner, never part of send identity.
        on_restart: requested_on_restart,
        model: requested_model,
        input: _,
        tools: requested_tools,
        policy_allowed: requested_policy_allowed,
        tool_choice: requested_tool_choice,
        generation: requested_generation,
        cache: requested_cache,
        budget: requested_budget,
        auth: requested_auth,
        context_limit: requested_context_limit,
        work_class: _,
        variant: requested_variant,
        service_tier: requested_service_tier,
        service_tier_level: requested_service_tier_level,
        stop_when: requested_stop_when,
        context_management: requested_context_management,
        inherit_frozen: requested_inherit_frozen,
        structured_output: requested_structured_output,
        client_message_id: _,
    } = requested;

    [
        (
            existing_route_decision_id != requested_route_decision_id,
            "route_decision_id",
        ),
        (
            existing_account_route != requested_account_route,
            "account_route",
        ),
        (existing_content != requested_content, "content"),
        (existing_directives != requested_directives, "directives"),
        (
            existing_on_restart.unwrap_or_default() != requested_on_restart.unwrap_or_default(),
            "on_restart",
        ),
        (existing_model != requested_model, "model"),
        (logical_input(existing) != logical_input(requested), "input"),
        (existing_tools != requested_tools, "tools"),
        (
            existing_policy_allowed != requested_policy_allowed,
            "policy_allowed",
        ),
        (existing_tool_choice != requested_tool_choice, "tool_choice"),
        (existing_generation != requested_generation, "generation"),
        (existing_cache != requested_cache, "cache"),
        (existing_budget != requested_budget, "budget"),
        (existing_auth != requested_auth, "auth"),
        (
            existing_context_limit != requested_context_limit,
            "context_limit",
        ),
        (existing_variant != requested_variant, "variant"),
        (
            existing_service_tier != requested_service_tier,
            "service_tier",
        ),
        (
            existing_service_tier_level != requested_service_tier_level,
            "service_tier_level",
        ),
        (existing_stop_when != requested_stop_when, "stop_when"),
        (
            existing_context_management != requested_context_management,
            "context_management",
        ),
        (
            existing_structured_output != requested_structured_output,
            "structured_output",
        ),
    ]
    .into_iter()
    .find_map(|(differs, field)| differs.then_some(field))
    .or_else(|| inherited_difference(existing_inherit_frozen, requested_inherit_frozen))
}

/// Which `session.send` member two sends differ on in what they leave to the session's frozen
/// config: the first parameter one send omitted (and so inherits) while the other named it. A
/// send that omits `generation` and one that names the same values explicitly are different
/// sends, since only the first follows the session's config.
fn inherited_difference(
    existing: &std::collections::BTreeSet<broca_protocol::request::InheritedParam>,
    requested: &std::collections::BTreeSet<broca_protocol::request::InheritedParam>,
) -> Option<&'static str> {
    broca_protocol::request::InheritedParam::ALL
        .iter()
        .find(|(param, _)| existing.contains(param) != requested.contains(param))
        .map(|(_, member)| *member)
}

#[derive(Serialize, Deserialize)]
struct SubmissionIdentityDigestV1 {
    /// Normalized display metadata is compared directly, never hashed with message content.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    message_marks: Option<broca_protocol::MessageMarks>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    route_decision_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    account_route: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    directives: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    on_restart: Option<String>,
    version: u8,
    model: String,
    input: String,
    tools: String,
    policy_allowed: String,
    tool_choice: String,
    generation: String,
    cache: String,
    budget: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    auth: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    context_limit: Option<String>,
    /// Omitted when the send carried no variant, so a digest stored before the field existed
    /// still compares equal to the same request today.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    variant: Option<String>,
    /// Omitted when the send carried no service tier, for the same reason as `variant`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    service_tier: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    service_tier_level: Option<String>,
    stop_when: String,
    /// Absent when the caller omitted the field, so every digest stored before the field existed
    /// still compares equal to the same request sent today.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    context_management: Option<String>,
    /// The send's delivery mode, as its wire spelling. Absent for `queue`, the default, so
    /// every digest stored before the field existed, and every plain send's digest, is unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    delivery: Option<String>,
    /// The runner parameters the send leaves to the session's frozen config
    /// (`SubmissionRequest::inherit_frozen`). Absent when empty, so every digest stored before
    /// the field existed, and the digest of every send that inherits nothing, is unchanged.
    #[serde(default, skip_serializing_if = "std::collections::BTreeSet::is_empty")]
    inherit_frozen: std::collections::BTreeSet<broca_protocol::request::InheritedParam>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    conversation_key: Option<String>,
    /// Absent when the send carried no `structured_output`, so every digest stored before the
    /// field existed, and every send without one, is unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    structured_output: Option<String>,
    /// The send's `client_message_id` (llm-runner 0.5.1 §9.11), the value itself rather than
    /// a digest of it: this is the one durable place broca keeps it. A queued send's retry is
    /// compared against it here, and a display row (once broca serves `history_display`) will
    /// read it back from here. Absent when the send carried
    /// none, so every digest stored before the member existed, and every send without one, is
    /// unchanged. A record holding a digest with it requires the WAL feature
    /// `client-message-id/v1` (`WalRecord::required_features`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    client_message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    content: Option<String>,
}

/// The `client_message_id` a stored identity digest carries, or `None` when it carries none
/// or does not parse. This is how a retry of a queued send is compared, since a request rebuilt
/// from a durable `Queued` record never carries the id itself.
pub fn digest_client_message_id(stored: &str) -> Option<String> {
    serde_json::from_str::<SubmissionIdentityDigestV1>(stored)
        .ok()
        .and_then(|digest| digest.client_message_id)
}

fn digest_json(value: serde_json::Value) -> String {
    crate::request_digest_hex(&canonical_json_bytes(&value))
}

/// Versioned SHA-256 identity for every field used by [`submission_difference`]. Per-field
/// digests retain the existing field-specific reuse error after an actor reopen without storing
/// the request itself. Set-valued policy names are sorted before canonical JSON encoding.
pub fn submission_identity_digest(request: &SubmissionRequest) -> String {
    send_identity_digest(request, broca_protocol::Delivery::Queue)
}

/// [`submission_identity_digest`] for a send with an explicit delivery mode. The mode is part of
/// the send's identity: a retry with the same key and another mode is refused naming
/// `delivery`. `Queue`, the default, digests exactly as a send that never named a mode.
pub fn send_identity_digest(
    request: &SubmissionRequest,
    delivery: broca_protocol::Delivery,
) -> String {
    send_identity_digest_with_conversation_key(request, delivery, None)
}

/// Send identity including the hash of caller routing affinity, without storing the key.
pub fn send_identity_digest_with_conversation_key(
    request: &SubmissionRequest,
    delivery: broca_protocol::Delivery,
    conversation_key: Option<&str>,
) -> String {
    serde_json::to_string(&identity_digest_v1(request, delivery, conversation_key))
        .expect("submission identity digests are JSON-serializable")
}

fn identity_digest_v1(
    request: &SubmissionRequest,
    delivery: broca_protocol::Delivery,
    conversation_key: Option<&str>,
) -> SubmissionIdentityDigestV1 {
    let SubmissionRequest {
        message_marks: _,
        route_decision_id,
        account_route,
        staged_uploads: _,
        content,
        directives,
        project_id: _, // Record the bind in the WAL, but Some/None must not change send-id identity.
        principal: _,  // Same send from two principals is one send; the principal is only recorded.
        scope: _, // The route's scope stamp: checked against the session's owner, never part of send identity.
        on_restart,
        model,
        input: _,
        tools,
        policy_allowed,
        tool_choice,
        generation,
        cache,
        budget,
        auth,
        context_limit,
        work_class: _,
        variant,
        service_tier,
        service_tier_level,
        stop_when,
        context_management,
        inherit_frozen,
        structured_output,
        client_message_id,
    } = request;
    let mut policy_allowed: Vec<&str> = policy_allowed.iter().map(String::as_str).collect();
    policy_allowed.sort_unstable();
    SubmissionIdentityDigestV1 {
        message_marks: normalized_message_marks(request),
        route_decision_id: route_decision_id.clone(),
        account_route: account_route
            .as_ref()
            .map(|route| digest_json(serde_json::json!(route))),
        content: (!content.is_empty()).then(|| digest_json(serde_json::json!(content))),
        directives: directives
            .as_ref()
            .map(|names| digest_json(serde_json::json!(names))),
        version: 1,
        on_restart: (*on_restart == Some(broca_protocol::request::OnRestart::Resume))
            .then(|| digest_json(serde_json::json!("resume"))),
        model: digest_json(serde_json::json!(model)),
        input: digest_json(serde_json::json!(logical_input(request))),
        tools: digest_json(serde_json::json!(tools)),
        policy_allowed: digest_json(serde_json::json!(policy_allowed)),
        tool_choice: digest_json(serde_json::json!(tool_choice)),
        generation: digest_json(serde_json::json!(generation)),
        cache: digest_json(serde_json::json!(cache)),
        budget: digest_json(serde_json::json!(budget)),
        auth: auth
            .as_ref()
            .map(|auth| digest_json(serde_json::json!(auth))),
        context_limit: context_limit.map(|limit| digest_json(serde_json::json!(limit))),
        variant: variant
            .as_ref()
            .map(|variant| digest_json(serde_json::json!(variant))),
        service_tier: service_tier.map(|tier| digest_json(serde_json::json!(tier))),
        service_tier_level: service_tier_level.clone(),
        stop_when: digest_json(serde_json::json!(stop_when)),
        context_management: context_management
            .as_ref()
            .map(|value| digest_json(serde_json::json!(value))),
        delivery: (delivery != broca_protocol::Delivery::Queue)
            .then(|| delivery.as_str().to_owned()),
        inherit_frozen: inherit_frozen.clone(),
        conversation_key: conversation_key.map(|key| digest_json(serde_json::json!(key))),
        structured_output: structured_output
            .as_ref()
            .map(|value| digest_json(serde_json::json!(value))),
        client_message_id: client_message_id.clone(),
    }
}

fn normalized_message_marks(request: &SubmissionRequest) -> Option<broca_protocol::MessageMarks> {
    let marks: broca_protocol::MessageMarks =
        serde_json::from_value(serde_json::Value::Object(request.message_marks.clone())).ok()?;
    (marks.speaks_for.is_some() || marks.notice).then_some(marks)
}

/// Identify routing-affinity reuse as well as differences in the submission and delivery.
pub fn send_identity_difference_with_conversation_key(
    stored: &str,
    requested: &SubmissionRequest,
    delivery: broca_protocol::Delivery,
    conversation_key: Option<&str>,
) -> Option<&'static str> {
    let Ok(stored) = serde_json::from_str::<SubmissionIdentityDigestV1>(stored) else {
        return Some("request identity");
    };
    if stored.version != 1 {
        return Some("request identity");
    }
    let requested = identity_digest_v1(requested, delivery, conversation_key);
    [
        (
            stored
                .message_marks
                .as_ref()
                .and_then(|marks| marks.speaks_for.as_ref())
                != requested
                    .message_marks
                    .as_ref()
                    .and_then(|marks| marks.speaks_for.as_ref()),
            "speaks_for",
        ),
        (
            stored
                .message_marks
                .as_ref()
                .is_some_and(|marks| marks.notice)
                != requested
                    .message_marks
                    .as_ref()
                    .is_some_and(|marks| marks.notice),
            "notice",
        ),
        (
            stored.route_decision_id != requested.route_decision_id,
            "route_decision_id",
        ),
        (
            stored.account_route != requested.account_route,
            "account_route",
        ),
        (
            stored.client_message_id != requested.client_message_id,
            "client_message_id",
        ),
        (stored.content != requested.content, "content"),
        (stored.directives != requested.directives, "directives"),
        (stored.delivery != requested.delivery, "delivery"),
        (
            stored.conversation_key != requested.conversation_key,
            "conversation_key",
        ),
        (stored.on_restart != requested.on_restart, "on_restart"),
        (stored.model != requested.model, "model"),
        (stored.input != requested.input, "input"),
        (stored.tools != requested.tools, "tools"),
        (
            stored.policy_allowed != requested.policy_allowed,
            "policy_allowed",
        ),
        (stored.tool_choice != requested.tool_choice, "tool_choice"),
        (stored.generation != requested.generation, "generation"),
        (stored.cache != requested.cache, "cache"),
        (stored.budget != requested.budget, "budget"),
        (stored.auth != requested.auth, "auth"),
        (
            stored.context_limit != requested.context_limit,
            "context_limit",
        ),
        (stored.variant != requested.variant, "variant"),
        (
            stored.service_tier != requested.service_tier,
            "service_tier",
        ),
        (
            stored.service_tier_level != requested.service_tier_level,
            "service_tier_level",
        ),
        (stored.stop_when != requested.stop_when, "stop_when"),
        (
            stored.context_management != requested.context_management,
            "context_management",
        ),
        (
            stored.structured_output != requested.structured_output,
            "structured_output",
        ),
    ]
    .into_iter()
    .find_map(|(differs, field)| differs.then_some(field))
    .or_else(|| inherited_difference(&stored.inherit_frozen, &requested.inherit_frozen))
}

/// The last episode's coarse durable state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EpisodeDisposition {
    Running,
    Paused,
    Finished,
}

pub use crate::budget_kernel::{BudgetReplayStateV1, BUDGET_REPLAY_STATE_VERSION};

/// Admission facts for the last episode at a materialization watermark.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LastEpisodeV1 {
    /// Timed pause evidence restored from a checkpoint; waking still requires validation against the WAL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_limit_pause: Option<Box<broca_wal::record::AccountLimitPause>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel_at: Option<broca_wal::record::RunCancelAt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_restart: Option<broca_protocol::request::OnRestart>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pause_reason: Option<broca_protocol::PauseReason>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep_warm: Option<broca_protocol::request::KeepWarm>,
    /// Recorded in the admission checkpoint so later episodes inherit the session's cache marker order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub marker_order_version: Option<u32>,
    pub run_id: RunId,
    /// Effective config: the `RunStarted` value plus a valid pre-call refreeze, if present.
    pub config: RunConfig,
    pub input: Vec<Message>,
    pub provider_call_started: bool,
    pub config_refrozen: bool,
    pub disposition: EpisodeDisposition,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
    #[serde(default)]
    pub retries_used: u32,
    /// Missing means legacy or untrustworthy budget state and must never seed an incremental run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget_replay_state: Option<BudgetReplayStateV1>,
    /// The request identity digest the episode's `RunStarted` recorded, so re-driving the
    /// episode after a restart keeps the identity a keyed retry is compared against without
    /// asking the run index. Missing for sends that cannot repeat, for records written before
    /// the field, and in checkpoints stored before it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_identity_digest: Option<String>,
}

/// Who has driven a session, folded from every one of its `RunStarted` records.
///
/// Admission decides from this whether a send may enter the session: a scoped session accepts
/// only its scope's owner, an unscoped one may adopt a scope only when every earlier episode
/// recorded that owner as its principal, and an unscoped one otherwise accepts only the
/// principal of its first send. Those rules read the WHOLE history, and the materialized fast
/// path at admission reads only a WAL tail, so the history is summarised here rather than
/// re-read on every send.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionOwnershipV1 {
    /// How many episodes (`RunStarted` records) have been folded.
    #[serde(default)]
    pub episodes: u64,
    /// The scope the session's latest scoped `RunStarted` froze. Every episode repeats it until
    /// its owner reopens the scope at a later epoch; the episode that reopens it records the new
    /// epoch, and that is the scope later sends are judged against. `None` while the session is
    /// unscoped. Builds that kept the first scope here never admitted a later epoch, so for
    /// the histories they wrote the first and the latest scope are the same.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<SessionScope>,
    /// The principal recorded on the session's first episode. `None` when there is no episode
    /// yet, or the first episode recorded none (the bind carried none, or it predates principal
    /// recording); `episodes` tells the two apart.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_principal: Option<BindPrincipal>,
    /// Whether any episode recorded no principal. Such a session can never adopt a scope,
    /// because nothing says who drove that episode.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub any_unrecorded_principal: bool,
    /// Every distinct principal an episode recorded, in the order each first appeared.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recorded_principals: Vec<BindPrincipal>,
    /// The session's first episode, which admitted it: its run id and the names of the tools
    /// it was admitted with. `None` before any episode, and in a summary stored before this
    /// member existed, which a reader then treats as unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_episode: Option<FirstEpisodeV1>,
    /// The latest distinct episode manifest. Refreshes do not replace the first episode's
    /// identity; unchanged sessions omit this field and retain their original checkpoint bytes.
    /// Older summaries also omit it because their episodes shared the first manifest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_manifest: Option<Arc<broca_wal::PlanManifest>>,
    /// Whether the WAL contains session-change facts. False is omitted so unrefreshed logs
    /// retain their old checkpoint bytes and admission need not probe for change records.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub session_change_recorded: bool,
    /// Versioned sender-policy fold, present only for histories with session-change records.
    /// Missing on an older refreshed checkpoint means the complete WAL must be folded again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender_admission: Option<SenderAdmissionV1>,
    /// Whether this summary was folded by a build that records the first episode's frozen
    /// plan manifest (`FirstEpisodeV1::manifest`). A summary stored by an older build, or
    /// re-stored by one after a rollback, lacks it and may have dropped a manifest, so it is
    /// treated like a summary that does not exist: admission folds the session's whole WAL
    /// instead of building on it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub plan_manifest_folded: bool,
    /// The scope (owner and `ref`) whose `route.bind` first reserved the session's storage
    /// address, read from the session inventory, never from the WAL. Admission fills it in only
    /// while the session has no episode, and only then does it apply: until a first send is
    /// admitted the history names no owner, so without it any route naming the same harness and
    /// session could become the session's first principal and lock the reserving owner out.
    /// `None` when the address was reserved without a stamp, predates reservations, or the
    /// session already has episodes. Never serialized: a checkpoint summarises history, and this
    /// is not history.
    #[serde(skip)]
    pub reservation: Option<crate::scope_admission::ScopeReservation>,
}

impl Default for SessionOwnershipV1 {
    /// The summary of a session with no episode yet, folded by this build.
    fn default() -> Self {
        Self {
            episodes: 0,
            scope: None,
            first_principal: None,
            any_unrecorded_principal: false,
            recorded_principals: Vec::new(),
            first_episode: None,
            latest_manifest: None,
            session_change_recorded: false,
            sender_admission: None,
            plan_manifest_folded: true,
            reservation: None,
        }
    }
}

/// The episode that admitted a session, as its `RunStarted` froze it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FirstEpisodeV1 {
    pub run_id: RunId,
    /// The model-facing names of the tools the episode was admitted with, in their order.
    pub tool_names: Vec<String>,
    /// The plan manifest the episode froze, when the session was admitted with a fetch plan.
    /// Later episodes retain it until a session refresh applies a new manifest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest: Option<Arc<broca_wal::PlanManifest>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SenderAdmissionV1 {
    pub version: u32,
    pub accepted_senders: Option<Vec<cortexkit_role_llm_runner::delivery_order::PlanSender>>,
}

/// The senders a session's frozen plan manifest grants, as admission reads them.
pub fn manifest_senders(
    manifest: &broca_wal::PlanManifest,
) -> Vec<cortexkit_role_llm_runner::delivery_order::PlanSender> {
    serde_json::from_str::<serde_json::Value>(manifest.plan.as_json_text())
        .ok()
        .and_then(|plan| {
            plan.as_object()
                .and_then(|plan| cortexkit_role_llm_runner::delivery_order::plan_senders(plan).ok())
        })
        .unwrap_or_default()
}

impl SessionOwnershipV1 {
    /// Admission policy changes at durable refresh acceptance, independently of when the
    /// execution manifest applies. Already admitted queued submissions keep their admission.
    pub fn effective_senders(&self) -> Vec<cortexkit_role_llm_runner::delivery_order::PlanSender> {
        self.sender_admission
            .as_ref()
            .and_then(|fold| fold.accepted_senders.clone())
            .unwrap_or_else(|| {
                self.latest_manifest
                    .as_deref()
                    .or_else(|| self.first_episode.as_ref()?.manifest.as_deref())
                    .map(manifest_senders)
                    .unwrap_or_default()
            })
    }

    /// Identity-only state for cancel/retract checks. The first episode also carries tool
    /// names and provider-supplied text, so it must not enter a long-lived owner cache.
    pub fn for_owner_access(&self) -> Self {
        Self {
            episodes: self.episodes,
            scope: self.scope.clone(),
            first_principal: self.first_principal.clone(),
            any_unrecorded_principal: self.any_unrecorded_principal,
            recorded_principals: self.recorded_principals.clone(),
            first_episode: None,
            latest_manifest: None,
            session_change_recorded: false,
            sender_admission: None,
            plan_manifest_folded: false,
            // Cancel and retract never read the reservation, and this copy can outlive the
            // empty history the reservation applies to.
            reservation: None,
        }
    }
}

/// Track the first frozen model and reasoning-effort settings and whether any episode changed them.
/// This constant-size history lets unchanged sessions avoid a full WAL read at admission.
/// Stored in the existing checkpoint JSON, not in a new store table or column.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct EffortHistoryV1 {
    pub first_config: serde_json::Value,
    /// Separate presence from JSON null, which is a valid legacy frozen config.
    pub has_first: bool,
    pub changed: bool,
}

impl EffortHistoryV1 {}

/// Version-one admission projection at an exact WAL sequence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdmissionCheckpointV1 {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recovery_cuts: Vec<broca_wal::recovery::RecoveryCut>,
    /// Reference-only media resume state, omitted for media-free histories. A captured-reference
    /// index without a valid matching seed requires a named, counted full-replay repair.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media: Option<crate::media_request::DurableMediaSeed>,
    /// Missing on older checkpoints: reconstruct from the full WAL before extending totals.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head: Option<crate::head::HeadProjection>,
    /// Whether any deferred call occurred in the complete prefix. Missing means an older
    /// checkpoint cannot show whether a call before its watermark still needs an answer,
    /// a withdrawal, or a late execution report.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub has_deferred_calls: Option<bool>,
    /// Session input-token sum, frozen cap and missing-usage flag, independent of episode count.
    /// Checkpoints without this field must be rebuilt from the complete WAL before admission.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_budget: Option<crate::input_budget::InputTokenBudget>,
    /// The session_class label recorded by the session's first send. Absence also
    /// represents checkpoints created before WALs recorded session classes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_class: Option<String>,
    /// Sequence of the last record folded into this checkpoint.
    pub at_seq: u64,
    pub last_episode: Option<LastEpisodeV1>,
    /// Durable queued submissions that have not been retracted or consumed, in WAL order.
    /// Shared ownership keeps reconstruction-snapshot clones from copying full request payloads;
    /// the custom serde functions preserve the version-one JSON array byte shape.
    #[serde(
        serialize_with = "serialize_pending_fifo",
        deserialize_with = "deserialize_pending_fifo"
    )]
    pub pending_fifo: Arc<Vec<PendingSubmission>>,
    /// Who has driven the session, over its whole history. Always present in a checkpoint this
    /// build folds. `None` only in a checkpoint stored by a build without this field, whose
    /// history summary is unknown: admission (`load_prior_wal_snapshot`) and projection then
    /// fold the complete WAL instead of building on it, and projection stores the repaired
    /// checkpoint. Serialized last, after every field an older build knows; an older build
    /// ignores it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ownership: Option<SessionOwnershipV1>,
    /// Missing means an older checkpoint cannot prove that historical efforts were unchanged.
    /// Folding every recorded episode keeps only the first config and a sticky change bit,
    /// so the summary does not grow with the WAL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort_history: Option<EffortHistoryV1>,
    /// Proof folded from the first run in a complete WAL. Older checkpoints lack
    /// it and must be repaired from the WAL before projecting a content-bearing tail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage_retention: Option<crate::LineageRetention>,
    /// Absent is an incomplete summary and must never be repaired from only a WAL tail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compaction: Option<crate::compaction::CompactionCheckpoint>,
    /// The lineage's runner-tag state (see [`crate::tags`]), folded from its origin: whether its
    /// records are tagged and the tag the next part gets. Absent in a checkpoint stored before
    /// tags existed, or folded from a WAL tail, which must be repaired from the complete WAL
    /// before a writer may tag anything, or it could reuse a number.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<crate::tags::LineageTags>,
    /// The `DeliveryOrdered` record whose `Queued` record has not been folded yet. The two are
    /// written in one barrier, but a running turn's own records can land between them, and a
    /// checkpoint can be cut there; the `Queued` record takes its order from here when it comes.
    ///
    /// The session's actor writes the pair back to back, so a record only the actor writes
    /// (`RunStarted`, `Queued`, `Retracted`, `Steered`, another `DeliveryOrdered`) arriving first
    /// means the `Queued` record was lost to a torn tail: the entry is dropped then, and never
    /// outlives the next thing the session's actor writes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_order: Option<StagedOrder>,
    /// The complete WAL's reservation fold, including measurements before the store watermark.
    /// Missing on older checkpoints; a tail alone cannot recover an earlier measured request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_reservation: Option<crate::output_reservation::OutputReservationHistory>,
    /// Whether this complete prefix contains any model attempt, across all episodes.
    /// Missing on older checkpoints means unknown, never proof that no request was sent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub has_model_attempt: Option<bool>,
    /// The newest run's tool set and whether any run ever changed it, so an append admitted
    /// from this checkpoint on a route that binds reasoning to its prefix knows where kept
    /// signed reasoning stops (`crate::signed_floor`). Unknown on a checkpoint stored before the
    /// field existed or folded from a WAL tail: admission then folds the complete WAL, and never
    /// reads the absence as "unchanged". A digest only, never tool text. Serialized last, so an
    /// older build, whose decoder ignores members it doesn't know, reads the rest unchanged.
    #[serde(
        default,
        skip_serializing_if = "crate::signed_floor::ToolSetHistory::is_unknown"
    )]
    pub tool_set_history: crate::signed_floor::ToolSetHistory,
}

/// A `DeliveryOrdered` record set aside for the `Queued` record that follows it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StagedOrder {
    pub submission_id: SubmissionId,
    pub order: broca_protocol::DeliveryOrder,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sent_by: Option<broca_protocol::BindPrincipal>,
}

impl AdmissionCheckpointV1 {
    pub fn empty() -> Self {
        Self {
            recovery_cuts: Vec::new(),
            media: None,
            head: Some(crate::head::HeadProjection::default()),
            has_deferred_calls: Some(false),
            input_budget: Some(crate::input_budget::InputTokenBudget::default()),
            session_class: None,
            at_seq: 0,
            last_episode: None,
            ownership: Some(SessionOwnershipV1::default()),
            effort_history: Some(EffortHistoryV1::default()),
            lineage_retention: Some(crate::LineageRetention::Unstarted),
            pending_fifo: Arc::new(Vec::new()),
            compaction: Some(crate::compaction::CompactionCheckpoint::None),
            tags: Some(crate::tags::LineageTags::unstarted()),
            pending_order: None,
            output_reservation: Some(
                crate::output_reservation::OutputReservationHistory::default(),
            ),
            has_model_attempt: Some(false),
            tool_set_history: crate::signed_floor::ToolSetHistory::Known(Default::default()),
        }
    }
}

impl AdmissionCheckpointV1 {}

impl AdmissionCheckpointV1 {}

impl Default for AdmissionCheckpointV1 {
    fn default() -> Self {
        let mut checkpoint = Self::empty();
        // A default value is not proof that a historical prefix contained no attempts.
        checkpoint.has_model_attempt = None;
        checkpoint
    }
}

fn serialize_pending_fifo<S>(
    pending: &Arc<Vec<PendingSubmission>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    pending.as_ref().serialize(serializer)
}

fn deserialize_pending_fifo<'de, D>(
    deserializer: D,
) -> Result<Arc<Vec<PendingSubmission>>, D::Error>
where
    D: Deserializer<'de>,
{
    Vec::<PendingSubmission>::deserialize(deserializer).map(Arc::new)
}

#[cfg(test)]
mod tests {
    use broca_protocol::{
        content::{ContentBlock, Message, Role},
        request::{ModelHandle, SubmissionRequest},
        Delivery,
    };

    use super::*;

    fn message(text: &str) -> Message {
        Message {
            turn_system: false,
            cache_prefix_blocks: None,
            role: Role::User,
            content: vec![ContentBlock::Text { text: text.into() }],
            origin: None,
            ordinal: None,
            tags: Default::default(),
        }
    }

    #[test]
    fn conversation_key_participates_in_send_identity() {
        let request = request("hello");
        let legacy = send_identity_digest(&request, Delivery::Queue);
        assert_eq!(
            send_identity_digest_with_conversation_key(&request, Delivery::Queue, None),
            legacy
        );
        let keyed = send_identity_digest_with_conversation_key(
            &request,
            Delivery::Queue,
            Some("consult:a"),
        );
        assert_ne!(keyed, legacy);
        assert!(
            !keyed.contains("consult:a"),
            "only a hash belongs in run data"
        );
        assert_eq!(
            send_identity_difference_with_conversation_key(
                &keyed,
                &request,
                Delivery::Queue,
                Some("consult:a")
            ),
            None
        );
        for key in [None, Some("consult:b")] {
            assert_eq!(
                send_identity_difference_with_conversation_key(
                    &keyed,
                    &request,
                    Delivery::Queue,
                    key
                ),
                Some("conversation_key")
            );
        }
        assert_eq!(
            send_identity_difference_with_conversation_key(
                &legacy,
                &request,
                Delivery::Queue,
                Some("consult:a")
            ),
            Some("conversation_key")
        );
    }

    #[test]
    fn legacy_send_identity_bytes_are_pinned() {
        assert_eq!(
            submission_identity_digest(&request("hello")),
            r#"{"version":1,"model":"c5dbb399b46de938a059bde30f33175be1e2d7fb823a01e60decafb956c62fd5","input":"88d732a13b910aeb6b247d174228f3ad9225fdc6ee34b2c83da34123b89f89e8","tools":"4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945","policy_allowed":"4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945","tool_choice":"d7522ccd58caaccd080ff42fc701046086ea73d690df7b1b597b620e0aa33284","generation":"44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a","cache":"74234e98afe7498fb5daf1f36ac2d78acc339464f950703b8c019892f982b90b","budget":"74234e98afe7498fb5daf1f36ac2d78acc339464f950703b8c019892f982b90b","stop_when":"4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945"}"#
        );
    }

    fn request(text: &str) -> SubmissionRequest {
        SubmissionRequest {
            message_marks: Default::default(),
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
            model: ModelHandle::new("stub", "queued"),
            input: vec![message(text)],
            tools: Some(Vec::new()),
            policy_allowed: Default::default(),
            tool_choice: Default::default(),
            generation: Default::default(),
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
        }
    }

    #[test]
    fn account_route_identity_distinguishes_provider_model_and_absence() {
        let base = request("hello");
        let mut routed = base.clone();
        routed.account_route = Some(broca_protocol::request::AccountRoute::new("pool", "model"));
        assert_eq!(submission_difference(&routed, &routed.clone()), None);
        assert_eq!(
            submission_identity_digest(&routed),
            submission_identity_digest(&routed.clone())
        );
        for route in [
            None,
            Some(broca_protocol::request::AccountRoute::new(
                "other-pool",
                "model",
            )),
            Some(broca_protocol::request::AccountRoute::new(
                "pool",
                "other-model",
            )),
        ] {
            let mut different = base.clone();
            different.account_route = route;
            assert_eq!(
                submission_difference(&routed, &different),
                Some("account_route")
            );
            assert_ne!(
                submission_identity_digest(&routed),
                submission_identity_digest(&different)
            );
        }
    }

    #[test]
    fn work_class_is_excluded_from_submission_identity() {
        let unclassified = request("same");
        let mut classified = unclassified.clone();
        classified.work_class = Some("mason".into());

        assert_eq!(submission_difference(&unclassified, &classified), None);
        assert_eq!(
            submission_identity_digest(&unclassified),
            submission_identity_digest(&classified)
        );
    }
}

#[cfg(test)]
mod message_mark_identity_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn marks_are_raw_retry_metadata_and_leave_every_existing_hash_unchanged() {
        let plain: SubmissionRequest = serde_json::from_value(json!({
            "model":broca_protocol::ModelHandle::new("fake", "fake"),"input":[]
        }))
        .unwrap();
        let old_bytes = submission_identity_digest(&plain);
        let mut marked = plain.clone();
        marked.message_marks = json!({"speaks_for":{"agent_id":"Ada"}})
            .as_object()
            .unwrap()
            .clone();
        let mut identity: serde_json::Value =
            serde_json::from_str(&submission_identity_digest(&marked)).unwrap();
        assert_eq!(
            identity.as_object_mut().unwrap().remove("message_marks"),
            Some(json!({"speaks_for":{"agent_id":"Ada"}}))
        );
        assert_eq!(
            identity,
            serde_json::from_str::<serde_json::Value>(&old_bytes).unwrap()
        );
        let mut explicit_false = plain;
        explicit_false
            .message_marks
            .insert("notice".into(), json!(false));
        assert_eq!(submission_identity_digest(&explicit_false), old_bytes);
    }
}
