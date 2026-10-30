//! Compact durable facts needed by admission before replaying a WAL tail.
//!
//! The checkpoint is a projection of the WAL, never an authority independent of it. Callers
//! accept it only at the store watermark and then fold every durable record through the WAL head.

use std::sync::Arc;

use broca_protocol::{
    canonical_json_bytes, content::Message, request::SubmissionRequest, BindPrincipal, RunId,
    SessionScope, SubmissionId, Usage,
};
use broca_wal::{FramedRecord, RunConfig, WalRecord};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{PendingSubmission, ReplayError};

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
        staged_uploads: _,
        content: existing_content,
        directives: existing_directives,
        project_id: _, // Record the bind in the WAL, but Some/None must not change send-id identity.
        principal: _,  // Same send from two principals is one send; the principal is only recorded.
        scope: _, // The route's scope stamp: checked against the session's owner, never part of send identity.
        on_restart: existing_on_restart,
        keep_warm: existing_keep_warm,
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
        stop_when: existing_stop_when,
        context_management: existing_context_management,
        inherit_frozen: existing_inherit_frozen,
        structured_output: existing_structured_output,
        // Compared from the stored identity digest instead (`digest_client_message_id`): a
        // request rebuilt from a durable `Queued` record never carries it.
        client_message_id: _,
    } = existing;
    let SubmissionRequest {
        staged_uploads: _,
        content: requested_content,
        directives: requested_directives,
        project_id: _, // Record the bind in the WAL, but Some/None must not change send-id identity.
        principal: _,  // Same send from two principals is one send; the principal is only recorded.
        scope: _, // The route's scope stamp: checked against the session's owner, never part of send identity.
        on_restart: requested_on_restart,
        keep_warm: requested_keep_warm,
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
        stop_when: requested_stop_when,
        context_management: requested_context_management,
        inherit_frozen: requested_inherit_frozen,
        structured_output: requested_structured_output,
        client_message_id: _,
    } = requested;

    [
        (existing_content != requested_content, "content"),
        (existing_directives != requested_directives, "directives"),
        (
            existing_on_restart.unwrap_or_default() != requested_on_restart.unwrap_or_default(),
            "on_restart",
        ),
        (
            existing_keep_warm.unwrap_or_default() != requested_keep_warm.unwrap_or_default(),
            "keep_warm",
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    directives: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    on_restart: Option<String>,
    version: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    keep_warm: Option<String>,
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
        staged_uploads: _,
        content,
        directives,
        project_id: _, // Record the bind in the WAL, but Some/None must not change send-id identity.
        principal: _,  // Same send from two principals is one send; the principal is only recorded.
        scope: _, // The route's scope stamp: checked against the session's owner, never part of send identity.
        on_restart,
        keep_warm,
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
        stop_when,
        context_management,
        inherit_frozen,
        structured_output,
        client_message_id,
    } = request;
    let mut policy_allowed: Vec<&str> = policy_allowed.iter().map(String::as_str).collect();
    policy_allowed.sort_unstable();
    SubmissionIdentityDigestV1 {
        content: (!content.is_empty()).then(|| digest_json(serde_json::json!(content))),
        directives: directives
            .as_ref()
            .map(|names| digest_json(serde_json::json!(names))),
        version: 1,
        on_restart: (*on_restart == Some(broca_protocol::request::OnRestart::Resume))
            .then(|| digest_json(serde_json::json!("resume"))),
        keep_warm: (*keep_warm == Some(broca_protocol::request::KeepWarm::Auto))
            .then(|| "auto".to_owned()),
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

/// Identify the first field whose digest differs from a previously persisted identity. A
/// malformed/unknown digest returns the generic identity label and therefore fails closed.
pub fn submission_identity_difference(
    stored: &str,
    requested: &SubmissionRequest,
) -> Option<&'static str> {
    send_identity_difference(stored, requested, broca_protocol::Delivery::Queue)
}

/// [`submission_identity_difference`] for a send with an explicit delivery mode. A different
/// mode is named first: it is the difference the caller most needs to see.
pub fn send_identity_difference(
    stored: &str,
    requested: &SubmissionRequest,
    delivery: broca_protocol::Delivery,
) -> Option<&'static str> {
    send_identity_difference_with_conversation_key(stored, requested, delivery, None)
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
            stored.client_message_id != requested.client_message_id,
            "client_message_id",
        ),
        (stored.content != requested.content, "content"),
        (stored.directives != requested.directives, "directives"),
        (stored.keep_warm != requested.keep_warm, "keep_warm"),
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
    /// Whether this summary was folded by a build that records the first episode's frozen
    /// plan manifest (`FirstEpisodeV1::manifest`). A summary stored by an older build, or
    /// re-stored by one after a rollback, lacks it and may have dropped a manifest, so it is
    /// treated like a summary that does not exist: admission folds the session's whole WAL
    /// instead of building on it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub plan_manifest_folded: bool,
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
            plan_manifest_folded: true,
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
    /// Every later episode renders its tools from it and carries it on its own `RunStarted`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest: Option<Arc<broca_wal::PlanManifest>>,
}

impl SessionOwnershipV1 {
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
            plan_manifest_folded: false,
        }
    }

    /// Only episode starts change ownership. Prompts, queues, budget and compaction state
    /// are irrelevant to which principals may cancel or retract a session.
    pub fn fold_owner_records(&mut self, records: &[FramedRecord]) {
        self.first_episode = None;
        self.plan_manifest_folded = false;
        for frame in records {
            if let WalRecord::RunStarted {
                principal, scope, ..
            } = &frame.record
            {
                self.fold_run_started(principal.as_ref(), scope.as_ref());
            }
        }
    }

    pub(crate) fn fold_run_started(
        &mut self,
        principal: Option<&BindPrincipal>,
        scope: Option<&SessionScope>,
    ) {
        if self.episodes == 0 {
            self.first_principal = principal.cloned();
        }
        self.episodes = self.episodes.saturating_add(1);
        match principal {
            None => self.any_unrecorded_principal = true,
            Some(principal) => {
                if !self.recorded_principals.contains(principal) {
                    self.recorded_principals.push(principal.clone());
                }
            }
        }
        // Every episode of a scoped session carries a scope, so an episode without one leaves
        // the frozen scope as it is rather than clearing it.
        if let Some(scope) = scope {
            self.scope = Some(scope.clone());
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

impl EffortHistoryV1 {
    fn observe(&mut self, config: &serde_json::Value) {
        fn signature(config: &serde_json::Value) -> serde_json::Value {
            serde_json::json!([
                config["family_id"],
                config["wire_model_id"],
                config["chatgpt_effort_updates"],
                config["reasoning_effort"],
                config["reasoning"]["effort"]
            ])
        }
        if self.has_first {
            self.changed |= signature(&self.first_config) != signature(config);
        } else {
            self.first_config = config.clone();
            self.has_first = true;
        }
    }
}

/// Version-one admission projection at an exact WAL sequence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdmissionCheckpointV1 {
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
    /// A newly applied record in a tail needs canonical replay to recover its
    /// message count. Missing compaction state is checked separately at the seed.
    pub(crate) fn compaction_boundary_complete(&self) -> bool {
        if !self.media_seed_complete() {
            return false;
        }
        self.compaction
            .as_ref()
            .and_then(|checkpoint| checkpoint.state())
            .is_none_or(|state| state.thinking_boundary_complete())
    }

    pub fn empty() -> Self {
        Self {
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
        }
    }

    /// Derive a checkpoint from a complete WAL prefix.
    pub fn from_frames(records: &[FramedRecord]) -> Result<Self, ReplayError> {
        let mut checkpoint = Self::empty();
        if !records.is_empty() && !crate::retention::starts_at_lineage_origin(records) {
            checkpoint.head = None;
            checkpoint.has_deferred_calls = None;
            checkpoint.lineage_retention = None;
            checkpoint.tags = None;
            checkpoint.output_reservation = None;
        }
        checkpoint.fold_frames(records)?;
        if checkpoint
            .compaction
            .as_ref()
            .and_then(|state| state.state())
            .is_some()
        {
            // Admission does not count transcript messages. Canonical replay determines
            // how many were present when the view was applied, including tool turns.
            checkpoint.compaction = Some(
                crate::replay::replay(
                    &records
                        .iter()
                        .map(|frame| frame.record.clone())
                        .collect::<Vec<_>>(),
                )?
                .compaction,
            );
        }
        Ok(checkpoint)
    }

    /// Fold an ordered WAL suffix into this checkpoint.
    pub fn fold_frames(&mut self, records: &[FramedRecord]) -> Result<(), ReplayError> {
        if let Some(crate::media_request::DurableMediaSeed::Ready(media)) = &mut self.media {
            media.resume_budget(
                self.last_episode
                    .as_ref()
                    .and_then(|episode| episode.config.budget.clone()),
            );
        }
        if self.at_seq == 0
            && records
                .iter()
                .any(|frame| crate::compaction::CapturedMedia::captures_any(&frame.record))
        {
            self.media = Some(crate::media_request::DurableMediaSeed::Ready(Box::new(
                crate::media_request::MediaCheckpoint::empty(),
            )));
        }
        for framed in records {
            self.fold_record(framed.seq, &framed.record)?;
        }
        Ok(())
    }

    /// Fold one WAL record. The mutation is committed only after all corruption checks pass.
    pub fn fold_record(&mut self, seq: u64, record: &WalRecord) -> Result<(), ReplayError> {
        if self.at_seq == 0
            && self.media.is_none()
            && crate::compaction::CapturedMedia::captures_any(record)
        {
            self.media = Some(crate::media_request::DurableMediaSeed::Ready(Box::new(
                crate::media_request::MediaCheckpoint::empty(),
            )));
        }
        if let Some(crate::media_request::DurableMediaSeed::Ready(media)) = &mut self.media {
            media
                .fold(seq, record)
                .map_err(|reason| ReplayError::CorruptLineage {
                    episode_run_id: self
                        .last_episode
                        .as_ref()
                        .map(|episode| episode.run_id.clone())
                        .unwrap_or_else(|| RunId(String::new())),
                    reason,
                })?;
        }
        if let Some(history) = &mut self.output_reservation {
            history.fold(record);
        }
        if let Some(head) = &mut self.head {
            head.observe(record);
        }
        if matches!(record, WalRecord::ToolCallDeferred { .. }) {
            self.has_deferred_calls = Some(true);
        }
        if let Some(budget) = &mut self.input_budget {
            budget
                .fold(record)
                .map_err(|reason| ReplayError::CorruptLineage {
                    episode_run_id: self
                        .last_episode
                        .as_ref()
                        .map(|episode| episode.run_id.clone())
                        .unwrap_or_else(|| RunId(String::new())),
                    reason,
                })?;
        }
        // A `Queued` record takes the order its `DeliveryOrdered` record set aside; any other
        // record only the session's actor writes drops an entry whose `Queued` was lost.
        let staged = match record {
            WalRecord::DeliveryOrdered { .. }
            | WalRecord::Queued { .. }
            | WalRecord::RunStarted { .. }
            | WalRecord::Retracted { .. }
            | WalRecord::Steered { .. } => self.pending_order.take(),
            _ => None,
        };
        let staged_for_queued = match record {
            WalRecord::Queued { submission_id, .. } => {
                staged.filter(|staged| &staged.submission_id == submission_id)
            }
            _ => None,
        };
        let (order_for_queued, sent_by_for_queued) = match staged_for_queued {
            Some(staged) => (Some(staged.order), staged.sent_by),
            None => (None, None),
        };
        match record {
            WalRecord::RunStarted {
                session_class,
                retention,
                on_restart,
                keep_warm,
                run_id,
                config,
                input,
                submission_id,
                request_identity_digest,
                marker_order_version,
                principal,
                scope,
                manifest,
                ..
            } => {
                if let Some(previous) = &self.last_episode {
                    if previous.disposition != EpisodeDisposition::Finished {
                        return Err(ReplayError::CorruptLineage {
                            episode_run_id: previous.run_id.clone(),
                            reason: "a new RunStarted opened before the previous episode reached a durable RunFinished".into(),
                        });
                    }
                }
                if self.last_episode.is_none() {
                    self.session_class = session_class.clone();
                }
                if let Some(ownership) = self.ownership.as_mut() {
                    if ownership.episodes == 0 {
                        ownership.first_episode = Some(FirstEpisodeV1 {
                            run_id: run_id.clone(),
                            tool_names: config.tools.iter().map(|tool| tool.name.clone()).collect(),
                            manifest: manifest.as_deref().cloned().map(Arc::new),
                        });
                    }
                    ownership.fold_run_started(principal.as_ref(), scope.as_ref());
                }
                if self.lineage_retention == Some(crate::LineageRetention::Unstarted) {
                    self.lineage_retention = Some(match retention {
                        Some(seconds) => crate::LineageRetention::Retained { seconds: *seconds },
                        None => crate::LineageRetention::Forever,
                    });
                }
                if let Some(history) = &mut self.effort_history {
                    history.observe(&config.frozen_render_config);
                }
                self.last_episode = Some(LastEpisodeV1 {
                    cancel_at: None,
                    on_restart: *on_restart,
                    pause_reason: None,
                    keep_warm: *keep_warm,
                    marker_order_version: *marker_order_version,
                    run_id: run_id.clone(),
                    config: (**config).clone(),
                    input: input.as_ref().clone(),
                    provider_call_started: false,
                    config_refrozen: false,
                    disposition: EpisodeDisposition::Running,
                    usage: None,
                    retries_used: 0,
                    budget_replay_state: config.budget.is_some().then(BudgetReplayStateV1::fresh),
                    request_identity_digest: request_identity_digest.clone(),
                });
                if let Some(submission_id) = submission_id {
                    remove_pending(&mut self.pending_fifo, submission_id);
                }
            }
            WalRecord::RunConfigRefrozen {
                renderer_version,
                frozen_render_config,
                ..
            } => {
                if let Some(episode) = self.last_episode.as_mut() {
                    if episode.provider_call_started {
                        return Err(ReplayError::CorruptLineage {
                            episode_run_id: episode.run_id.clone(),
                            reason: "RunConfigRefrozen occurred after a provider call started"
                                .into(),
                        });
                    }
                    if episode.config_refrozen {
                        return Err(ReplayError::CorruptLineage {
                            episode_run_id: episode.run_id.clone(),
                            reason: "RunConfigRefrozen occurred more than once in one episode"
                                .into(),
                        });
                    }
                    episode.config.renderer_version = *renderer_version;
                    episode.config.frozen_render_config = frozen_render_config.clone();
                    episode.config_refrozen = true;
                    if let Some(history) = &mut self.effort_history {
                        history.observe(frozen_render_config);
                    }
                }
            }
            WalRecord::StructuredOutputDowngraded { run_id } => {
                if let Some(episode) = &mut self.last_episode {
                    if &episode.run_id == run_id {
                        broca_protocol::structured_output::FrozenStructuredOutput::downgrade_render_config(&mut episode.config.frozen_render_config);
                    }
                }
            }
            WalRecord::RunAuthRefrozen { auth_selection, .. } => {
                if let Some(episode) = self.last_episode.as_mut() {
                    episode.config.auth_selection = Some(auth_selection.clone());
                }
            }
            WalRecord::RunCancelRequested { run_id, at } => {
                if let Some(episode) = self.last_episode.as_mut().filter(|ep| {
                    ep.run_id == *run_id
                        && ep.disposition != EpisodeDisposition::Finished
                        && ep.cancel_at != Some(broca_wal::record::RunCancelAt::Now)
                }) {
                    episode.cancel_at = Some(*at);
                }
            }
            WalRecord::StepStarted { .. } => {
                if let Some(episode) = self.last_episode.as_mut() {
                    episode.provider_call_started = true;
                    // Replay clears a pause at a step boundary but does not unseal a terminal.
                    if episode.disposition == EpisodeDisposition::Paused {
                        episode.disposition = EpisodeDisposition::Running;
                        episode.pause_reason = None;
                    }
                }
            }
            WalRecord::RunPaused { reason, .. } => {
                if let Some(episode) = self.last_episode.as_mut() {
                    episode.disposition = EpisodeDisposition::Paused;
                    episode.pause_reason = Some(*reason);
                }
            }
            WalRecord::RunFinished {
                usage,
                retries_used,
                ..
            } => {
                if let Some(episode) = self.last_episode.as_mut() {
                    episode.disposition = EpisodeDisposition::Finished;
                    episode.usage = Some(*usage);
                    episode.retries_used = *retries_used;
                }
            }
            WalRecord::ModelStepFinished { retries_used, .. } => {
                if let Some(episode) = self.last_episode.as_mut() {
                    episode.retries_used = episode.retries_used.saturating_add(*retries_used);
                }
            }
            WalRecord::BudgetFired { .. } => {}
            WalRecord::DeliveryOrdered {
                submission_id,
                order,
                sent_by,
            } => {
                self.pending_order = Some(StagedOrder {
                    submission_id: submission_id.clone(),
                    order: order.clone(),
                    sent_by: sent_by.clone(),
                });
            }
            WalRecord::SendEnded { submission_id, .. }
            | WalRecord::SendDelivered { submission_id, .. } => {
                remove_pending(&mut self.pending_fifo, submission_id);
            }
            WalRecord::Queued {
                keep_warm,
                written_at_ms,
                ts_ms,
                submission_id,
                request,
                request_identity_digest,
                principal,
                send_id,
                title,
                delivery,
                conversation_key,
                hooks,
                ..
            } => crate::queue::insert_in_drain_order(
                Arc::make_mut(&mut self.pending_fifo),
                PendingSubmission {
                    admitted_at_ms: written_at_ms.or(*ts_ms),
                    hooks: hooks.clone(),
                    conversation_key: conversation_key.clone(),
                    submission_id: submission_id.clone(),
                    request: {
                        let mut request = request.as_ref().clone();
                        request.keep_warm = keep_warm.or(request.keep_warm);
                        request
                    },
                    request_identity_digest: request_identity_digest.clone(),
                    principal: principal.clone(),
                    send_id: send_id.clone(),
                    title: title.clone(),
                    delivery: *delivery,
                    order: order_for_queued,
                    sent_by: sent_by_for_queued,
                },
            ),
            WalRecord::Retracted { submission_id, .. } => {
                remove_pending(&mut self.pending_fifo, submission_id);
            }
            WalRecord::ToolBatchStarted { .. }
            | WalRecord::ToolDispatchIntent { .. }
            | WalRecord::ToolNoDispatch { .. }
            | WalRecord::ToolResult { .. }
            | WalRecord::ToolBatchFinished { .. }
            | WalRecord::TurnFinished { .. }
            | WalRecord::ApprovalRequested { .. }
            | WalRecord::ApprovalResolved { .. } => {}
            // A steered message joins the running episode's conversation; admission keeps no
            // prompt, and a steer neither opens, pauses nor ends an episode nor touches the queue.
            WalRecord::Steered { .. } => {}
            // A record type written by a newer build: informational by the contract on
            // `WalRecord`, so it changes no admission state (only `at_seq` advances past it).
            WalRecord::WriterOpened { .. }
            | WalRecord::WriterClosed { .. }
            | WalRecord::VisibilityLate { .. }
            | WalRecord::Unknown => {}
            // Compaction state is folded below; it does not change admission's episode counters.
            WalRecord::CompactionRequest { .. }
            | WalRecord::CompactionMessage { .. }
            | WalRecord::CompactionAnswer { .. }
            | WalRecord::CompactionWait { .. }
            | WalRecord::CompactionViewApplied { .. }
            | WalRecord::ContextCeilingWait { .. }
            | WalRecord::ContextCeilingDecided { .. }
            | WalRecord::OutputReservationClamped { .. }
            | WalRecord::CompactionRideWait { .. }
            | WalRecord::CompactionRideWaitEnded { .. }
            | WalRecord::CompactionNotice { .. } => {}
            // Provider attempt evidence is informational only, skipped exactly like `Unknown`.
            WalRecord::ModelAttemptFinished { .. } => {}
            WalRecord::AttemptReserved { .. }
            | WalRecord::AttemptFinished { .. }
            | WalRecord::AttemptReleased { .. }
            | WalRecord::AttemptCorrected { .. } => {}
            // A refused step neither opens, pauses nor ends an episode: its re-send commits
            // under the same step id, and the run's `RunFinished` carries the refused call's
            // usage in its totals.
            WalRecord::ModelStepRefused { .. } => {}
            // A resume's own account of what it found: informational, skipped like `Unknown`.
            WalRecord::ResumeStarted { .. } => {}
            // A cache warm changes no episode, queue or ownership state.
            WalRecord::CacheWarmed { .. } => {}
            // Approval and late-call records change neither admission ownership nor episode/queue
            // membership. Replay and the separate approval projection retain their per-call state.
            WalRecord::ToolCallDeferred { .. }
            | WalRecord::ApprovalRecorded { .. }
            | WalRecord::AwaitingApproval { .. }
            | WalRecord::LateApprovalRecorded { .. }
            | WalRecord::LateDispatchIntent { .. }
            | WalRecord::LateResult { .. } => {}
            // A provider-held call admits nothing and owns nothing: admission ownership and
            // episode/queue membership come from the call's batch and run records, not this one.
            WalRecord::ToolCallProviderHeld { .. } => {}
            // An informational record, skipped exactly like `Unknown`.
            WalRecord::InputTokenCapFrozen { .. }
            | WalRecord::InputTokenBudgetExhausted { .. }
            | WalRecord::StructuredOutputResult { .. }
            | WalRecord::QuestionWithdrawalAcknowledged { .. } => {}
        }
        if let Some(episode) = self.last_episode.as_mut() {
            if let Some(facts) = episode.budget_replay_state.as_mut() {
                if crate::budget_kernel::apply(
                    crate::budget_kernel::State {
                        facts,
                        config: episode.config.budget.as_ref(),
                        run_id: Some(&episode.run_id),
                    },
                    record,
                )
                .is_err()
                {
                    episode.budget_replay_state = None;
                }
            }
        }
        self.at_seq = seq;
        if let Some(tags) = &mut self.tags {
            tags.fold(record);
        }
        if let Some(compaction) = &mut self.compaction {
            compaction.fold(record);
            if let (
                WalRecord::CompactionMessage {
                    kind: broca_wal::CompactionMessageKind::Setup,
                    ..
                },
                crate::compaction::CompactionCheckpoint::State(state),
            ) = (record, compaction)
            {
                state.history.current_model = self
                    .last_episode
                    .as_ref()
                    .map(|episode| episode.config.model.clone());
                // The first run's account was folded before the checkpoint had a state
                // (Setup creates it), so it would otherwise be lost on a refold. Seed it
                // here from the durable episode that wrote it, so a restart rebuilds the
                // same baseline without memory.
                if state.current_account.is_none() {
                    state.current_account = self.last_episode.as_ref().and_then(|episode| {
                        episode
                            .config
                            .auth_selection
                            .as_ref()
                            .and_then(crate::compaction::view::selection_account_label)
                    });
                }
            }
        }
        Ok(())
    }
}

impl AdmissionCheckpointV1 {
    /// Only media references recorded in captured history can authorize byte reads.
    pub fn captured_media_reference(&self, sha256: &str) -> Option<&broca_protocol::MediaRef> {
        self.compaction.as_ref()?.captured()?.by_digest(sha256)
    }

    pub fn has_captured_media(&self) -> bool {
        self.compaction
            .as_ref()
            .and_then(|checkpoint| checkpoint.captured())
            .is_some_and(|captured| !captured.is_empty())
    }

    pub fn media_seed_complete(&self) -> bool {
        let has_media = self.has_captured_media();
        if !has_media {
            return true;
        }
        let reason = match &self.media {
            Some(crate::media_request::DurableMediaSeed::Ready(seed))
                if seed.at_seq == self.at_seq =>
            {
                return true
            }
            Some(crate::media_request::DurableMediaSeed::Ready(_)) => {
                "media_seed_watermark_mismatch".to_string()
            }
            Some(crate::media_request::DurableMediaSeed::Unreadable(value)) => {
                crate::media_request::MediaCheckpoint::decode(value).unwrap_err()
            }
            None => "media_seed_missing".to_string(),
        };
        crate::media_request::count_seed_fallback(&reason);
        false
    }

    pub fn compaction_complete(&self) -> bool {
        match &self.compaction {
            Some(_) => self.compaction_boundary_complete(),
            None => !self
                .ownership
                .as_ref()
                .and_then(|ownership| ownership.first_episode.as_ref())
                .and_then(|episode| episode.manifest.as_ref())
                .is_some_and(|manifest| crate::compaction::has_compaction(manifest)),
        }
    }
    /// Whether admission metadata, including input accounting, covers the entire session.
    /// Older checkpoints need a complete WAL fold to recover these facts before incremental reuse.
    pub fn ownership_complete(&self) -> bool {
        self.effort_history.is_some()
            && self.input_budget.is_some()
            && self.lineage_retention.is_some()
            && self.tags.is_some()
            && self
                .ownership
                .as_ref()
                .is_some_and(|ownership| ownership.plan_manifest_folded)
    }
}

impl AdmissionCheckpointV1 {
    /// Supply the first effort anchor without replaying history when the checkpoint proves
    /// no episode ever changed effort. Changed or unknown histories still need full WAL replay.
    pub fn restore_unchanged_effort(&self, prior: &mut crate::ReplayState) {
        if prior.render_episodes.is_some() {
            return;
        }
        if let Some(history) = self
            .effort_history
            .as_ref()
            .filter(|history| !history.changed)
        {
            if history.has_first {
                prior.render_episodes = Some(vec![broca_protocol::request::EpisodeRenderInput {
                    prompt_start: 0,
                    first_user: None,
                    frozen_config: history.first_config.clone(),
                }]);
            }
        }
    }
}

impl Default for AdmissionCheckpointV1 {
    fn default() -> Self {
        Self::empty()
    }
}

fn remove_pending(pending: &mut Arc<Vec<PendingSubmission>>, submission_id: &SubmissionId) {
    Arc::make_mut(pending).retain(|item| &item.submission_id != submission_id);
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

#[derive(Serialize)]
struct AdmissionCheckpointEnvelopeRef<'a> {
    version: u32,
    checkpoint: &'a AdmissionCheckpointV1,
}

#[derive(Deserialize)]
struct AdmissionCheckpointEnvelope {
    version: u32,
    checkpoint: serde_json::Value,
}

/// Encode the shared, backend-independent checkpoint envelope.
pub fn encode_admission_checkpoint(
    checkpoint: &AdmissionCheckpointV1,
) -> Result<String, serde_json::Error> {
    serde_json::to_string(&AdmissionCheckpointEnvelopeRef {
        version: 1,
        checkpoint,
    })
}

/// Decode a supported checkpoint. Unknown versions and malformed payloads are unavailable rather
/// than an empty lineage, so callers take the full-WAL repair path.
///
/// A checkpoint stored before the ownership summary existed decodes with `ownership: None`;
/// the admission snapshot and the projection seed refuse to build on it (their full-WAL path
/// repairs it), while readers that only want its last episode may still use it.
pub fn decode_admission_checkpoint(encoded: &str) -> Option<AdmissionCheckpointV1> {
    let envelope: AdmissionCheckpointEnvelope = serde_json::from_str(encoded).ok()?;
    if envelope.version != 1 {
        return None;
    }
    serde_json::from_value(envelope.checkpoint).ok()
}

/// An authenticated source frame in an admission seed. The hash covers the complete encoded
/// frame, not its JSON payload, so moved, truncated and replaced sources all force full replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdmissionSource {
    seq: u64,
    off: u64,
    end: u64,
    identity: Vec<u8>,
}

impl AdmissionSource {
    fn read(
        &self,
        read_at: &dyn Fn(u64, u64) -> std::io::Result<Vec<u8>>,
        boundary: &AdmissionSource,
    ) -> Result<FramedRecord, crate::replay::SeedFallback> {
        use sha2::Digest;
        let bad = || crate::replay::SeedFallback {
            reason: "admission source identity mismatch",
        };
        if self.seq == 0 || self.seq > boundary.seq || self.end > boundary.end {
            return Err(bad());
        }
        let len = self
            .end
            .checked_sub(self.off)
            .filter(|len| *len > 0)
            .ok_or_else(bad)?;
        let bytes = read_at(self.off, len).map_err(|_| bad())?;
        if bytes.len() as u64 != len || sha2::Sha256::digest(&bytes).as_slice() != self.identity {
            return Err(bad());
        }
        match broca_wal::framing::decode_next(&bytes) {
            broca_wal::framing::DecodeStep::Record { framed, consumed }
                if consumed == bytes.len() && framed.seq == self.seq =>
            {
                Ok(*framed)
            }
            _ => Err(bad()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AdmissionEpisodeSeed {
    start: AdmissionSource,
    run_id: RunId,
    refreezes: Vec<AdmissionSource>,
    structured_output_downgraded: bool,
    provider_call_started: bool,
    disposition: EpisodeDisposition,
    pause_reason: Option<broca_protocol::PauseReason>,
    cancel_at: Option<broca_wal::record::RunCancelAt>,
    usage: Option<Usage>,
    retries_used: u32,
    budget_replay_state: Option<BudgetReplayStateV1>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AdmissionQueueSeed {
    source: AdmissionSource,
    submission_id: SubmissionId,
    order_source: Option<AdmissionSource>,
}

/// Content-free admission continuation for the offset index. Sources and scalars must be
/// committed in the same index transaction as the qualified projection boundary.
///
/// `scalars` is deliberately private: it has no episode, queue payload, manifest or frozen
/// render config and is not a usable v1 checkpoint until hydration completes. Compaction
/// payloads currently require full replay rather than storing a second copy in this seed.
/// Materialized-store readers continue to use `load_prior_wal_snapshot`; this seed is for
/// the offset-index admission path.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdmissionSeed {
    version: u32,
    boundary: AdmissionSource,
    lineage_id: Option<String>,
    qualified: bool,
    scalars: AdmissionCheckpointV1,
    first_run: Option<AdmissionSource>,
    episode: Option<AdmissionEpisodeSeed>,
    queued: Vec<AdmissionQueueSeed>,
    staged: Option<AdmissionSource>,
    ordinal_base: u64,
    tag_high: u64,
    tag_rules: Option<(bool, bool)>,
}

impl AdmissionSeed {
    /// Capture a complete prefix ending at the index's committed projection boundary.
    /// Index writers supply actual positioned frames, never offsets inferred from re-encoding.
    pub fn capture(
        read_at: &dyn Fn(u64, u64) -> std::io::Result<Vec<u8>>,
        boundary: &crate::replay::SeedBoundary,
        prefix: &[crate::replay::PositionedFrame],
    ) -> Result<Self, crate::replay::SeedFallback> {
        use sha2::Digest;
        let bad = || crate::replay::SeedFallback {
            reason: "incomplete admission prefix",
        };
        if prefix.first().map(|p| p.framed.seq) != Some(1)
            || prefix.last().map(|p| (p.framed.seq, p.off))
                != Some((boundary.seq, boundary.frame_off))
            || prefix
                .windows(2)
                .any(|p| p[0].framed.seq + 1 != p[1].framed.seq)
        {
            return Err(bad());
        }
        let frames: Vec<_> = prefix.iter().map(|p| p.framed.clone()).collect();
        let mut scalars = AdmissionCheckpointV1::from_frames(&frames).map_err(|_| bad())?;
        let replay =
            crate::replay::replay(&frames.iter().map(|p| p.record.clone()).collect::<Vec<_>>())
                .map_err(|_| bad())?;
        if !matches!(
            scalars.compaction,
            Some(crate::compaction::CompactionCheckpoint::None)
        ) || !replay.pending_steers.is_empty()
            || replay.open_tool_turn.is_some()
            || frames.iter().any(|p| matches!(&p.record, WalRecord::RunStarted { manifest: Some(manifest), .. } if crate::compaction::has_compaction(manifest)))
        {
            return Err(crate::replay::SeedFallback {
                reason: "admission prefix needs full replay",
            });
        }
        let source = |i: usize| -> Result<AdmissionSource, crate::replay::SeedFallback> {
            let off = prefix[i].off;
            let limit = prefix.get(i + 1).map_or(boundary.frame_end, |p| p.off);
            let bytes = read_at(off, limit.checked_sub(off).ok_or_else(bad)?).map_err(|_| bad())?;
            // FileWal metadata follows each record. A source identifies only its own
            // frame, not the lineage bytes before the next positioned record.
            let consumed = match broca_wal::framing::decode_next(&bytes) {
                broca_wal::framing::DecodeStep::Record { framed, consumed }
                    if *framed == prefix[i].framed =>
                {
                    consumed
                }
                _ => return Err(bad()),
            };
            let end = off.checked_add(consumed as u64).ok_or_else(bad)?;
            let source = AdmissionSource {
                seq: prefix[i].framed.seq,
                off,
                end,
                identity: sha2::Sha256::digest(&bytes[..consumed]).to_vec(),
            };
            if source.read(
                read_at,
                &AdmissionSource {
                    seq: boundary.seq,
                    off: boundary.frame_off,
                    end: boundary.frame_end,
                    identity: boundary.frame_identity.clone(),
                },
            )? != prefix[i].framed
            {
                return Err(bad());
            }
            Ok(source)
        };
        let first = frames
            .iter()
            .position(|p| matches!(p.record, WalRecord::RunStarted { .. }));
        let last = frames
            .iter()
            .rposition(|p| matches!(p.record, WalRecord::RunStarted { .. }));
        let episode = match (last, scalars.last_episode.take()) {
            (Some(i), Some(episode)) => Some(AdmissionEpisodeSeed {
                start: source(i)?,
                run_id: episode.run_id.clone(),
                refreezes: ((i + 1)..frames.len())
                    .filter(|j| {
                        matches!(
                            frames[*j].record,
                            WalRecord::RunConfigRefrozen { .. } | WalRecord::RunAuthRefrozen { .. }
                        )
                    })
                    .map(source)
                    .collect::<Result<_, _>>()?,
                // This is an effective-state flag, like the pause disposition, not a sequence
                // used to synthesize a record. A later render refreeze replaces an earlier downgrade.
                structured_output_downgraded: frames[i + 1..].iter().fold(
                    false,
                    |downgraded, p| match &p.record {
                        WalRecord::RunConfigRefrozen { .. } => false,
                        WalRecord::StructuredOutputDowngraded { run_id }
                            if run_id == &episode.run_id =>
                        {
                            true
                        }
                        _ => downgraded,
                    },
                ),
                provider_call_started: episode.provider_call_started,
                disposition: episode.disposition,
                pause_reason: episode.pause_reason,
                cancel_at: episode.cancel_at,
                usage: episode.usage,
                retries_used: episode.retries_used,
                budget_replay_state: episode.budget_replay_state,
            }),
            (None, None) => None,
            _ => return Err(bad()),
        };
        let queued = scalars.pending_fifo.iter().map(|pending| {
            let i = frames.iter().rposition(|p| matches!(&p.record, WalRecord::Queued { submission_id, .. } if submission_id == &pending.submission_id)).ok_or_else(bad)?;
            let order_source = if pending.order.is_some() {
                Some(source(frames[..i].iter().rposition(|p| matches!(&p.record, WalRecord::DeliveryOrdered { submission_id, .. } if submission_id == &pending.submission_id)).ok_or_else(bad)?)?)
            } else { None };
            Ok(AdmissionQueueSeed { source: source(i)?, submission_id: pending.submission_id.clone(), order_source })
        }).collect::<Result<_, crate::replay::SeedFallback>>()?;
        scalars.pending_fifo = Arc::new(Vec::new());
        let staged = scalars.pending_order.take().map(|staged| {
            source(frames.iter().rposition(|p| matches!(&p.record, WalRecord::DeliveryOrdered { submission_id, .. } if submission_id == &staged.submission_id)).ok_or_else(bad)?)
        }).transpose()?;
        if let Some(ownership) = &mut scalars.ownership {
            if let Some(first) = &mut ownership.first_episode {
                first.manifest = None;
            }
        }
        if let Some(effort) = &mut scalars.effort_history {
            effort.first_config = serde_json::Value::Null;
        }
        let tag_seed = crate::tags::TagSeed::from_prefix(&replay.prompt);
        Ok(Self {
            version: 1,
            boundary: AdmissionSource {
                seq: boundary.seq,
                off: boundary.frame_off,
                end: boundary.frame_end,
                identity: boundary.frame_identity.clone(),
            },
            lineage_id: boundary.lineage_id.clone(),
            qualified: boundary.qualified,
            scalars,
            first_run: first.map(source).transpose()?,
            episode,
            queued,
            staged,
            ordinal_base: replay.extent,
            tag_high: tag_seed.high,
            tag_rules: tag_seed.rules.map(|r| (r.render, r.clean)),
        })
    }

    fn episode(
        &self,
        read_at: &dyn Fn(u64, u64) -> std::io::Result<Vec<u8>>,
    ) -> Result<Option<LastEpisodeV1>, crate::replay::SeedFallback> {
        let Some(seed) = &self.episode else {
            return Ok(None);
        };
        let bad = || crate::replay::SeedFallback {
            reason: "invalid admission episode sources",
        };
        let start = seed.start.read(read_at, &self.boundary)?;
        if !matches!(&start.record, WalRecord::RunStarted { run_id, .. } if run_id == &seed.run_id)
        {
            return Err(bad());
        }
        let mut checkpoint = AdmissionCheckpointV1::empty();
        checkpoint
            .fold_record(start.seq, &start.record)
            .map_err(|_| bad())?;
        let mut refs = seed
            .refreezes
            .iter()
            .map(|source| {
                let frame = source.read(read_at, &self.boundary)?;
                if source.seq <= start.seq
                    || !matches!(
                        frame.record,
                        WalRecord::RunConfigRefrozen { .. } | WalRecord::RunAuthRefrozen { .. }
                    )
                {
                    return Err(bad());
                }
                Ok(frame)
            })
            .collect::<Result<Vec<_>, _>>()?;
        refs.sort_by_key(|frame| frame.seq);
        if refs.windows(2).any(|p| p[0].seq == p[1].seq) {
            return Err(bad());
        }
        checkpoint.fold_frames(&refs).map_err(|_| bad())?;
        let mut episode = checkpoint.last_episode.ok_or_else(bad)?;
        if seed.structured_output_downgraded {
            broca_protocol::structured_output::FrozenStructuredOutput::downgrade_render_config(
                &mut episode.config.frozen_render_config,
            );
        }
        // Replay may reset the budget when the tail opens another episode. Validate the stored
        // episode's budget before that reset can hide an unsupported or incomplete seed.
        match (&episode.config.budget, &seed.budget_replay_state) {
            (Some(config), Some(state)) if state.is_valid_for(config) => {}
            (None, None) => {}
            _ => return Err(bad()),
        }
        episode.provider_call_started = seed.provider_call_started;
        episode.disposition = seed.disposition;
        episode.pause_reason = seed.pause_reason;
        episode.cancel_at = seed.cancel_at;
        episode.usage = seed.usage;
        episode.retries_used = seed.retries_used;
        episode.budget_replay_state = seed.budget_replay_state.clone();
        Ok(Some(episode))
    }

    fn validate(
        &self,
        read_at: &dyn Fn(u64, u64) -> std::io::Result<Vec<u8>>,
        tail: &[crate::replay::PositionedFrame],
        safe_head: u64,
        episode: Option<&LastEpisodeV1>,
    ) -> Result<(), crate::replay::SeedFallback> {
        if self.version != 1
            || self.scalars.at_seq != self.boundary.seq
            || tail.last().map_or(self.boundary.seq, |p| p.framed.seq) != safe_head
            || self.episode.is_none()
            || self.first_run.is_none()
            || !self.scalars.ownership_complete()
            || self.scalars.head.is_none()
            || self.scalars.has_deferred_calls.is_none()
            || self.scalars.output_reservation.is_none()
            || crate::replay::tail_contains_compaction_setup(&tail.iter().map(|p| p.framed.clone()).collect::<Vec<_>>())
            || tail.iter().any(|p| matches!(&p.framed.record, WalRecord::RunStarted { manifest: Some(manifest), .. } if crate::compaction::has_compaction(manifest)))
        {
            return Err(crate::replay::SeedFallback {
                reason: "admission seed incomplete or head mismatch",
            });
        }
        crate::replay::replay_seeded_at_validated_boundary(
            read_at,
            &crate::replay::SeedBoundary {
                lineage_id: self.lineage_id.clone(),
                seq: self.boundary.seq,
                frame_off: self.boundary.off,
                frame_end: self.boundary.end,
                frame_identity: self.boundary.identity.clone(),
                qualified: self.qualified,
            },
            tail,
            episode.map(|e| e.config.model.clone()),
            episode.map(|e| e.run_id.clone()),
            episode.and_then(|e| e.config.budget.clone()),
            episode.and_then(|e| e.budget_replay_state.clone()),
            self.ordinal_base,
            true,
            crate::tags::TagSeed {
                high: self.tag_high,
                rules: self
                    .tag_rules
                    .map(|(render, clean)| broca_protocol::tags::ComputedRules { render, clean }),
            },
            Vec::new(),
        )?;
        Ok(())
    }

    /// Hydrate an admission checkpoint only after authenticating its boundary and contiguous tail.
    /// A caller must discard every partial result and fully replay on any `SeedFallback`.
    pub fn hydrate_with_base(
        &self,
        read_at: &dyn Fn(u64, u64) -> std::io::Result<Vec<u8>>,
        tail: &[crate::replay::PositionedFrame],
        safe_head: u64,
    ) -> Result<(AdmissionCheckpointV1, AdmissionCheckpointV1), crate::replay::SeedFallback> {
        let episode = self.episode(read_at)?;
        self.validate(read_at, tail, safe_head, episode.as_ref())?;
        let bad = || crate::replay::SeedFallback {
            reason: "incomplete admission scalars",
        };
        if self.scalars.last_episode.is_some()
            || !self.scalars.pending_fifo.is_empty()
            || self.scalars.pending_order.is_some()
            || self.first_run.is_some()
                != self
                    .scalars
                    .ownership
                    .as_ref()
                    .is_some_and(|o| o.first_episode.is_some())
            || self
                .scalars
                .effort_history
                .as_ref()
                .is_none_or(|e| !e.first_config.is_null())
            || !self.scalars.ownership_complete()
            || !matches!(
                self.scalars.compaction,
                Some(crate::compaction::CompactionCheckpoint::None)
            )
        {
            return Err(bad());
        }
        let mut checkpoint = self.scalars.clone();
        checkpoint.last_episode = episode;
        if let Some(source) = &self.first_run {
            let first = source.read(read_at, &self.boundary)?;
            let WalRecord::RunStarted {
                run_id,
                config,
                manifest,
                ..
            } = first.record
            else {
                return Err(bad());
            };
            let ownership = checkpoint
                .ownership
                .as_mut()
                .and_then(|o| o.first_episode.as_mut())
                .ok_or_else(bad)?;
            if ownership.run_id != run_id {
                return Err(bad());
            }
            ownership.manifest = manifest.as_deref().cloned().map(Arc::new);
            checkpoint
                .effort_history
                .as_mut()
                .ok_or_else(bad)?
                .first_config = config.frozen_render_config.clone();
        }
        let pending = self.queued.iter().map(|seed| {
            let frame = seed.source.read(read_at, &self.boundary)?;
            if !matches!(&frame.record, WalRecord::Queued { submission_id, .. } if submission_id == &seed.submission_id) { return Err(bad()); }
            let mut one = AdmissionCheckpointV1::empty();
            if let Some(source) = &seed.order_source {
                let order = source.read(read_at, &self.boundary)?;
                if order.seq >= frame.seq || !matches!(&order.record, WalRecord::DeliveryOrdered { submission_id, .. } if submission_id == &seed.submission_id) { return Err(bad()); }
                one.fold_record(order.seq, &order.record).map_err(|_| bad())?;
            }
            one.fold_record(frame.seq, &frame.record).map_err(|_| bad())?;
            Arc::unwrap_or_clone(one.pending_fifo).pop().ok_or_else(bad)
        }).collect::<Result<Vec<_>, _>>()?;
        checkpoint.pending_fifo = Arc::new(pending);
        if let Some(source) = &self.staged {
            let frame = source.read(read_at, &self.boundary)?;
            let WalRecord::DeliveryOrdered {
                submission_id,
                order,
                sent_by,
            } = frame.record
            else {
                return Err(bad());
            };
            checkpoint.pending_order = Some(StagedOrder {
                submission_id,
                order,
                sent_by,
            });
        }
        let base = checkpoint.clone();
        checkpoint
            .fold_frames(&tail.iter().map(|p| p.framed.clone()).collect::<Vec<_>>())
            .map_err(|_| bad())?;
        if !checkpoint.compaction_boundary_complete() {
            return Err(bad());
        }
        Ok((base, checkpoint))
    }

    /// Return the complete admission view through `safe_head`, discarding the prefix view.
    pub fn hydrate(
        &self,
        read_at: &dyn Fn(u64, u64) -> std::io::Result<Vec<u8>>,
        tail: &[crate::replay::PositionedFrame],
        safe_head: u64,
    ) -> Result<AdmissionCheckpointV1, crate::replay::SeedFallback> {
        self.hydrate_with_base(read_at, tail, safe_head)
            .map(|(_, head)| head)
    }

    /// Rebuild only the paused episode's request sources. A no-tool boundary plus its
    /// `RunStarted` costs two source-frame reads (the lineage header and EOF probe carry no
    /// request content), plus one read for each inline refreeze. Ownership and queued requests
    /// are irrelevant to cancellation and are not decoded here.
    pub fn paused_episode(
        &self,
        read_at: &dyn Fn(u64, u64) -> std::io::Result<Vec<u8>>,
        tail: &[crate::replay::PositionedFrame],
        safe_head: u64,
    ) -> Result<Option<LastEpisodeV1>, crate::replay::SeedFallback> {
        let episode = self.episode(read_at)?;
        self.validate(read_at, tail, safe_head, episode.as_ref())?;
        let mut checkpoint = AdmissionCheckpointV1::empty();
        checkpoint.last_episode = episode;
        checkpoint
            .fold_frames(&tail.iter().map(|p| p.framed.clone()).collect::<Vec<_>>())
            .map_err(|_| crate::replay::SeedFallback {
                reason: "paused admission tail refused",
            })?;
        Ok(checkpoint
            .last_episode
            .filter(|e| e.disposition == EpisodeDisposition::Paused))
    }
}

#[cfg(test)]
mod tests {
    use broca_protocol::{
        caller::{PauseReason, RunFinishReason},
        content::{AssistantMessage, ContentBlock, Message, Role, ToolCall},
        request::{
            BudgetConfig, BudgetKind, BudgetNudge, ExecutionMode, ModelHandle, StopCondition,
            SubmissionRequest, Tool,
        },
        BindIdentity, Delivery, MessageId, RunId, StepId, SubmissionId, ToolCallId, Usage,
    };
    use broca_wal::{AuthSelection, RunConfig, RunOrigin, WalRecord};

    use super::*;

    fn message(text: &str) -> Message {
        Message {
            cache_prefix_blocks: None,
            role: Role::User,
            content: vec![ContentBlock::Text { text: text.into() }],
            origin: None,
            ordinal: None,
            tags: Default::default(),
        }
    }

    fn config(model: &str) -> RunConfig {
        RunConfig {
            turn_directives_v1: None,
            render_tags: false,
            computed_tags: None,
            model: ModelHandle::new("stub", model),
            tools: Arc::new(vec![
                Tool {
                    name: "second".into(),
                    description: "second tool".into(),
                    input_schema: serde_json::json!({"type": "object", "order": 2}),
                    module: Some("tools-b".into()),
                    execution_mode: ExecutionMode::Parallel,
                    reply: None,
                },
                Tool {
                    name: "first".into(),
                    description: "first tool".into(),
                    input_schema: serde_json::json!({"type": "object", "order": 1}),
                    module: Some("tools-a".into()),
                    execution_mode: ExecutionMode::Sequential,
                    reply: None,
                },
            ]),
            tool_choice: Default::default(),
            generation: Default::default(),
            response_format: Default::default(),
            provider_options: serde_json::json!({"opaque": [1, 2, 3]}),
            cache_tiers: serde_json::Value::Null,
            context_epoch: 0,
            renderer_version: 1,
            frozen_render_config: serde_json::json!({"wire": "old"}),
            auth_selection: None,
            tools_were_omitted: false,
            budget: None,
            context_limit: None,
            work_class: None,
            variant: None,
            service_tier: None,
            stop_when: vec![StopCondition::HasToolCall {
                name: Some("second".into()),
            }],
            context_management: None,
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

    /// Queue preserves legacy identity bytes; other modes differ and name delivery on reuse.
    #[test]
    fn the_delivery_mode_is_part_of_the_send_identity() {
        let request = request("hello");
        let plain = submission_identity_digest(&request);
        assert_eq!(send_identity_digest(&request, Delivery::Queue), plain);
        for mode in [Delivery::Steer, Delivery::Interrupt] {
            let digest = send_identity_digest(&request, mode);
            assert_ne!(digest, plain, "{mode:?}");
            assert_eq!(
                send_identity_difference(&plain, &request, mode),
                Some("delivery")
            );
            assert_eq!(
                send_identity_difference(&digest, &request, Delivery::Queue),
                Some("delivery")
            );
            assert_eq!(send_identity_difference(&digest, &request, mode), None);
        }
        assert_ne!(
            send_identity_digest(&request, Delivery::Steer),
            send_identity_digest(&request, Delivery::Interrupt)
        );
    }

    /// A send's client_message_id is part of its identity (llm-runner 0.5.1 §9.11): a re-send
    /// with another id, without the id the first send carried, or with one where the first
    /// carried none, differs naming `client_message_id`, even with the same prompt. The digest
    /// keeps the value itself, which is where broca stores it, and a send without one keeps
    /// exactly the digest it had before the member existed.
    #[test]
    fn client_message_id_is_part_of_send_identity_and_kept_in_the_digest() {
        let plain = request("hello");
        let mut with_a = plain.clone();
        with_a.client_message_id = Some("msg-a".into());
        let mut with_b = plain.clone();
        with_b.client_message_id = Some("msg-b".into());
        let digest_a = submission_identity_digest(&with_a);
        assert_eq!(
            digest_client_message_id(&digest_a).as_deref(),
            Some("msg-a")
        );
        assert_eq!(
            digest_client_message_id(&submission_identity_digest(&plain)),
            None
        );
        assert!(!submission_identity_digest(&plain).contains("client_message_id"));
        assert_eq!(submission_identity_difference(&digest_a, &with_a), None);
        for (stored, requested) in [(&with_a, &with_b), (&with_a, &plain), (&plain, &with_a)] {
            assert_eq!(
                submission_identity_difference(&submission_identity_digest(stored), requested),
                Some("client_message_id")
            );
        }
        // The request comparison used for a queued retry leaves the id to the digest, since a
        // request decoded from a `Queued` record never carries it.
        assert_eq!(submission_difference(&plain, &with_a), None);
    }

    #[test]
    fn keep_warm_identity_is_per_send_and_off_matches_omission() {
        use broca_protocol::request::KeepWarm;
        let absent = request("hello");
        let mut off = absent.clone();
        off.keep_warm = Some(KeepWarm::Off);
        let mut auto = absent.clone();
        auto.keep_warm = Some(KeepWarm::Auto);
        assert_eq!(submission_difference(&absent, &off), None);
        assert_eq!(
            submission_identity_digest(&absent),
            submission_identity_digest(&off)
        );
        assert!(!submission_identity_digest(&absent).contains("keep_warm"));
        assert_eq!(submission_difference(&absent, &auto), Some("keep_warm"));
        assert_eq!(
            submission_identity_difference(&submission_identity_digest(&absent), &auto),
            Some("keep_warm")
        );
        assert_eq!(
            submission_identity_difference(&submission_identity_digest(&auto), &off),
            Some("keep_warm")
        );
    }

    fn request(text: &str) -> SubmissionRequest {
        SubmissionRequest {
            staged_uploads: None,
            content: Vec::new(),
            directives: None,
            on_restart: None,
            keep_warm: None,
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

    fn frame(seq: u64, record: WalRecord) -> FramedRecord {
        FramedRecord {
            seq,
            fence: 1,
            record,
        }
    }

    fn started(seq: u64, run: &str, submission_id: Option<&str>) -> FramedRecord {
        frame(
            seq,
            WalRecord::RunStarted {
                written_at_ms: None,
                tags: None,
                session_class: None,
                retention: None,
                hooks: None,
                title: None,
                on_restart: None,
                keep_warm: None,
                marker_order_version: None,
                manifest: None,
                scope: None,
                send_id: None,
                request_identity_digest: None,
                project_id: None,
                principal: None,
                ts_ms: None,
                run_id: RunId(run.into()),
                session: BindIdentity::new("/tmp/checkpoint", "h", "s"),
                config: Box::new(config(run)),
                input: vec![message(run)].into(),
                origin: RunOrigin::Fresh,
                submission_id: submission_id.map(|id| SubmissionId(id.into())),
            },
        )
    }

    #[test]
    fn admission_running_but_replay_completed_without_durable_terminal_is_intentional() {
        let records = vec![
            started(1, "r", None),
            frame(2, WalRecord::StepStarted { step_id: StepId(1) }),
            frame(
                3,
                WalRecord::ModelStepFinished {
                    fallback: Default::default(),
                    written_at_ms: None,
                    tags: None,
                    hooks: None,
                    step_id: StepId(1),
                    assistant_message: AssistantMessage {
                        message_id: MessageId("answer".into()),
                        content: vec![ContentBlock::Text {
                            text: "done".into(),
                        }],
                    }
                    .into(),
                    usage: Usage::default(),
                    finish_reason: None,
                    retries_used: 0,
                    service_tier_used: None,
                    speed_used: None,
                },
            ),
        ];
        // Admission gates new sends: no durable terminal means the episode is still running.
        // Replay directs execution: the answer is complete, so write RunFinished, not another
        // model turn. These answers must differ until that terminal is durable.
        assert_eq!(
            AdmissionCheckpointV1::from_frames(&records)
                .unwrap()
                .last_episode
                .unwrap()
                .disposition,
            EpisodeDisposition::Running
        );
        let wal: Vec<_> = records.iter().map(|frame| frame.record.clone()).collect();
        assert_eq!(
            crate::replay::replay(&wal).unwrap().resolution,
            crate::ReplayResolution::LineageIdle {
                last_reason: RunFinishReason::Completed
            }
        );
    }

    fn offset_log(records: &[FramedRecord]) -> (Vec<u8>, Vec<crate::replay::PositionedFrame>) {
        let mut bytes = Vec::new();
        let positioned = records
            .iter()
            .map(|frame| {
                let off = bytes.len() as u64;
                bytes.extend(
                    broca_wal::framing::encode(frame.seq, frame.fence, &frame.record).unwrap(),
                );
                crate::replay::PositionedFrame {
                    off,
                    framed: frame.clone(),
                }
            })
            .collect();
        (bytes, positioned)
    }

    fn offset_reader(bytes: &[u8]) -> impl Fn(u64, u64) -> std::io::Result<Vec<u8>> + '_ {
        move |off, len| {
            let off = (off as usize).min(bytes.len());
            Ok(bytes[off..off.saturating_add(len as usize).min(bytes.len())].to_vec())
        }
    }

    fn offset_boundary(
        bytes: &[u8],
        frames: &[crate::replay::PositionedFrame],
        cut: usize,
    ) -> crate::replay::SeedBoundary {
        use sha2::Digest;
        let off = frames[cut - 1].off;
        let end = frames.get(cut).map_or(bytes.len() as u64, |p| p.off);
        crate::replay::SeedBoundary {
            lineage_id: None,
            seq: frames[cut - 1].framed.seq,
            frame_off: off,
            frame_end: end,
            frame_identity: sha2::Sha256::digest(&bytes[off as usize..end as usize]).to_vec(),
            qualified: true,
        }
    }

    fn offset_step(seq: u64) -> FramedRecord {
        frame(
            seq,
            WalRecord::ModelStepFinished {
                fallback: Default::default(),
                written_at_ms: None,
                tags: None,
                hooks: None,
                step_id: StepId(1),
                assistant_message: Arc::new(AssistantMessage {
                    message_id: MessageId(format!("m{seq}")),
                    content: vec![ContentBlock::Text {
                        text: "private assistant text".into(),
                    }],
                }),
                usage: Usage::default(),
                finish_reason: None,
                retries_used: 2,
                service_tier_used: None,
                speed_used: None,
            },
        )
    }

    fn offset_corpus() -> Vec<FramedRecord> {
        let mut first = started(1, "r1", None);
        if let WalRecord::RunStarted { input, .. } = &mut first.record {
            *input = vec![message("private first input")].into();
        }
        if let WalRecord::RunStarted { config, .. } = &mut first.record {
            config.budget = Some(BudgetConfig {
                max_input_tokens: None,
                nudges: vec![],
                finalize_at_tool_calls: 12,
                finalize_text: "private finalize text".into(),
            });
        }
        let mut second = started(11, "r2", None);
        if let WalRecord::RunStarted { input, .. } = &mut second.record {
            *input = vec![message("private second input")].into();
        }
        if let WalRecord::RunStarted {
            request_identity_digest,
            config,
            ..
        } = &mut second.record
        {
            *request_identity_digest = Some("request-digest".into());
            config.tools_were_omitted = true;
        }
        vec![
            first,
            frame(
                2,
                WalRecord::RunConfigRefrozen {
                    renderer_version: 2,
                    ts_ms: None,
                    frozen_render_config: serde_json::json!({"wire": "private refrozen setup"}),
                },
            ),
            frame(
                3,
                WalRecord::RunAuthRefrozen {
                    ts_ms: None,
                    auth_selection: AuthSelection {
                        credential_id: "apikey:stub".into(),
                        account_ref: None,
                        resolved_credential_id: None,
                        vault_request: None,
                        account_decision: None,
                        auth_method: None,
                    },
                },
            ),
            offset_step(4),
            frame(
                5,
                WalRecord::DeliveryOrdered {
                    submission_id: SubmissionId("q".into()),
                    order: broca_protocol::DeliveryOrder {
                        modes: vec![broca_protocol::request::DeliveryMode::IdleStart],
                        dedupe_key: None,
                        expires_at_ms: None,
                        template: None,
                    },
                    sent_by: None,
                },
            ),
            frame(
                6,
                WalRecord::Queued {
                    submission_id: SubmissionId("q".into()),
                    request: Box::new(request("private queued input")),
                    request_identity_digest: None,
                    principal: None,
                    send_id: None,
                    title: None,
                    delivery: Some(Delivery::Queue),
                    retention: None,
                    conversation_key: None,
                    hooks: None,
                    keep_warm: None,
                    written_at_ms: None,
                    ts_ms: None,
                },
            ),
            frame(
                7,
                WalRecord::RunPaused {
                    run_id: RunId("r1".into()),
                    ts_ms: None,
                    reason: PauseReason::AuthRequired,
                    error: None,
                },
            ),
            frame(
                8,
                WalRecord::RunCancelRequested {
                    run_id: RunId("r1".into()),
                    at: broca_wal::record::RunCancelAt::Now,
                },
            ),
            frame(
                9,
                WalRecord::Retracted {
                    submission_id: SubmissionId("q".into()),
                    failure: None,
                },
            ),
            frame(
                10,
                WalRecord::RunFinished {
                    interrupt_reason: None,
                    reason: RunFinishReason::Cancelled,
                    usage: Usage::default(),
                    retries_used: 2,
                    ts_ms: None,
                    error: None,
                },
            ),
            second,
            offset_step(12),
            frame(
                13,
                WalRecord::RunPaused {
                    run_id: RunId("r2".into()),
                    ts_ms: None,
                    reason: PauseReason::AuthRequired,
                    error: None,
                },
            ),
        ]
    }

    #[test]
    fn offset_admission_equals_full_fold_at_every_publication_and_index_cut() {
        let records = offset_corpus();
        let (bytes, positioned) = offset_log(&records);
        let mut matched = 0;
        for boundary_cut in [4, 12] {
            let boundary = offset_boundary(&bytes, &positioned, boundary_cut);
            let seed = AdmissionSeed::capture(
                &offset_reader(&bytes),
                &boundary,
                &positioned[..boundary_cut],
            )
            .unwrap();
            let serialized = serde_json::to_string(&seed).unwrap();
            for secret in [
                "private first input",
                "private second input",
                "private assistant text",
                "private refrozen setup",
                "private queued input",
                "private finalize text",
                "second tool",
                "\"opaque\"",
            ] {
                assert!(
                    !serialized.contains(secret),
                    "content leaked into seed: {secret}"
                );
            }
            let seed: AdmissionSeed = serde_json::from_str(&serialized).unwrap();
            for end in boundary_cut..=records.len() {
                let end_off = positioned.get(end).map_or(bytes.len(), |p| p.off as usize);
                let read_at = offset_reader(&bytes[..end_off]);
                let snapshot = crate::run::load_prior_wal_snapshot_from_seed(
                    &read_at,
                    &seed,
                    &positioned[boundary_cut..end],
                    end as u64,
                    || panic!("valid boundary must not fall back at {boundary_cut}/{end}"),
                )
                .unwrap();
                assert_eq!(
                    snapshot.checkpoint_at_head,
                    AdmissionCheckpointV1::from_frames(&records[..end]).unwrap(),
                    "boundary={boundary_cut}, published={end}"
                );
                assert_eq!(snapshot.base_watermark, boundary.seq);
                matched += 1;
            }
        }
        assert_eq!(matched, 12);
    }

    #[test]
    fn offset_admission_real_filewal_hydrates_without_full_read() {
        offset_admission_real_filewal_case(false);
    }

    #[test]
    fn offset_admission_real_filewal_empty_tail_hydrates_without_full_read() {
        offset_admission_real_filewal_case(true);
    }

    fn offset_admission_real_filewal_case(empty_tail: bool) {
        use crate::replay::{PositionedFrame, SeedBoundary};
        use crate::WalStore;
        use broca_wal::framing::{decode_next, DecodeStep};
        use sha2::Digest;
        use std::io::{Read, Seek, SeekFrom};

        let root = broca_test_support::scratch_root().join(format!(
            "real-admission-seed-{empty_tail}-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("state-format"), "broca-state/2").unwrap();
        let records = if empty_tail {
            let mut records = vec![started(1, "r", None), offset_step(2)];
            let call_id = broca_protocol::ToolCallId("call".into());
            if let WalRecord::ModelStepFinished {
                assistant_message, ..
            } = &mut records[1].record
            {
                Arc::make_mut(assistant_message).content =
                    vec![ContentBlock::ToolCall(broca_protocol::content::ToolCall {
                        tool_call_id: call_id.clone(),
                        tool_name: "work".into(),
                        input: serde_json::json!({}),
                        thought_signature: None,
                    })];
            }
            let batch_id = broca_protocol::BatchId("batch".into());
            records.push(frame(
                3,
                WalRecord::ToolBatchStarted {
                    batch_id: batch_id.clone(),
                    step_id: StepId(1),
                    planned_calls: vec![call_id.clone()],
                },
            ));
            records.push(frame(
                4,
                WalRecord::ToolDispatchIntent {
                    dispatched_at_ms: None,
                    batch_id: batch_id.clone(),
                    tool_call_id: call_id.clone(),
                    tool_name: Some("work".into()),
                    args: Arc::new(serde_json::json!({})),
                    module_id: None,
                    schema_pin: None,
                    hooks: None,
                    approval_digest: None,
                    call_key: None,
                },
            ));
            records.push(frame(
                5,
                WalRecord::ToolResult {
                    finished_at_ms: None,
                    batch_id: batch_id.clone(),
                    result: Arc::new(broca_protocol::content::ToolResult {
                        tool_call_id: call_id,
                        output: broca_protocol::content::ToolOutput::Text {
                            text: "done".into(),
                        },
                        is_error: false,
                    }),
                    outcome: broca_wal::record::DispatchOutcome::Completed,
                    hooks: None,
                    reason: None,
                    withdraw_elicitation: None,
                    provider_held: None,
                    tags: None,
                },
            ));
            records.push(frame(6, WalRecord::ToolBatchFinished { batch_id }));
            records.push(frame(7, WalRecord::TurnFinished { step_id: StepId(1) }));
            records
        } else {
            offset_corpus()
        };
        let cut = if empty_tail { records.len() } else { 4 };
        let key = match &records[0].record {
            WalRecord::RunStarted { session, .. } => session.clone(),
            _ => unreachable!(),
        };
        let store =
            crate::FileWalStore::with_config(root.join("wal"), broca_wal::WalConfig::OVERHAUL);
        let mut wal = store.open(&key).unwrap();
        for framed in &records {
            wal.append(framed.fence, &framed.record, true).unwrap();
        }
        drop(wal);
        let path = store.wal_path(&key);
        let bytes = std::fs::read(&path).unwrap();
        let mut positioned = Vec::new();
        let mut off = 0usize;
        let mut lineage_id = None;
        let mut boundary_end = 0;
        // The empty-tail case ends at a committed TurnFinished boundary. The other case
        // uses a ModelStepFinished with no tool calls; its later durable RunFinished
        // makes that boundary safe to seed replay.
        while off < bytes.len() {
            match decode_next(&bytes[off..]) {
                DecodeStep::Record { framed, consumed } => {
                    if framed.seq == cut as u64 {
                        boundary_end = (off + consumed) as u64;
                    }
                    positioned.push(PositionedFrame {
                        off: off as u64,
                        framed: *framed,
                    });
                    off += consumed;
                }
                DecodeStep::Lineage {
                    lineage_id: found,
                    consumed,
                } => {
                    lineage_id = Some(found.to_string());
                    off += consumed;
                }
                _ => panic!("the real admission log must decode at byte {off}"),
            }
        }
        let boundary = SeedBoundary {
            lineage_id,
            seq: positioned[cut - 1].framed.seq,
            frame_off: positioned[cut - 1].off,
            frame_end: boundary_end,
            frame_identity: sha2::Sha256::digest(
                &bytes[positioned[cut - 1].off as usize..boundary_end as usize],
            )
            .to_vec(),
            qualified: true,
        };
        let read_at = |off, len| {
            let mut file = std::fs::File::open(&path)?;
            file.seek(SeekFrom::Start(off))?;
            let mut bytes = Vec::new();
            file.take(len).read_to_end(&mut bytes)?;
            Ok(bytes)
        };
        let seed = AdmissionSeed::capture(&read_at, &boundary, &positioned[..cut])
            .expect("a multi-record FileWal admission seed must capture exact source frames");
        assert_eq!(
            positioned.get(cut).map_or(bytes.len() as u64, |p| p.off) - boundary.frame_end,
            broca_wal::framing::LINEAGE_FRAME_LEN as u64
        );
        let full_reads = std::cell::Cell::new(0);
        let snapshot = crate::run::load_prior_wal_snapshot_from_seed(
            &read_at,
            &seed,
            &positioned[cut..],
            positioned.last().unwrap().framed.seq,
            || {
                full_reads.set(full_reads.get() + 1);
                panic!("a valid real FileWal admission seed must not read the full log")
            },
        )
        .unwrap();
        assert_eq!(full_reads.get(), 0);
        assert_eq!(snapshot.base_watermark, cut as u64);
        assert_eq!(snapshot.tail.is_empty(), empty_tail);
        assert_eq!(
            snapshot.checkpoint_at_head,
            AdmissionCheckpointV1::from_frames(&records).unwrap()
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn offset_admission_boundary_and_locator_mismatches_fully_repair() {
        let records = offset_corpus();
        let (bytes, positioned) = offset_log(&records);
        let boundary = offset_boundary(&bytes, &positioned, 4);
        let seed =
            AdmissionSeed::capture(&offset_reader(&bytes), &boundary, &positioned[..4]).unwrap();
        let expected = AdmissionCheckpointV1::from_frames(&records).unwrap();
        for case in 0..9 {
            let mut seed = seed.clone();
            let mut tail = positioned[4..].to_vec();
            match case {
                0 => seed.boundary.identity[0] ^= 1,
                1 => seed.qualified = false,
                2 => seed.episode.as_mut().unwrap().start.identity[0] ^= 1,
                3 => seed.lineage_id = Some("different lineage".into()),
                4 => {
                    tail.remove(0);
                }
                5 => seed.version += 1,
                6 => seed.episode.as_mut().unwrap().refreezes[0].end += 1,
                7 => {
                    seed.episode
                        .as_mut()
                        .unwrap()
                        .budget_replay_state
                        .as_mut()
                        .unwrap()
                        .version += 1
                }
                8 => seed.episode = None,
                _ => unreachable!(),
            }
            let repairs = std::cell::Cell::new(0);
            let snapshot = crate::run::load_prior_wal_snapshot_from_seed(
                &offset_reader(&bytes),
                &seed,
                &tail,
                records.len() as u64,
                || {
                    repairs.set(repairs.get() + 1);
                    Ok(records.clone())
                },
            )
            .unwrap();
            assert_eq!(repairs.get(), 1, "case {case} must repair");
            assert_eq!(snapshot.base_watermark, 0);
            assert_eq!(snapshot.checkpoint_at_head, expected);
        }
    }

    #[test]
    fn offset_admission_hydrates_queued_payloads_and_staged_order_from_sources() {
        let corpus = offset_corpus();
        let mut records = corpus[..5].to_vec();
        records.push(offset_step(6));
        records.push(corpus[5].clone());
        records.push(offset_step(8));
        records.push(corpus[6].clone());
        for (i, frame) in records.iter_mut().enumerate() {
            frame.seq = i as u64 + 1;
        }
        let (bytes, positioned) = offset_log(&records);
        for cut in [6, 8] {
            let boundary = offset_boundary(&bytes, &positioned, cut);
            let seed =
                AdmissionSeed::capture(&offset_reader(&bytes), &boundary, &positioned[..cut])
                    .unwrap();
            assert!(!serde_json::to_string(&seed)
                .unwrap()
                .contains("private queued input"));
            for end in cut..=records.len() {
                let end_off = positioned.get(end).map_or(bytes.len(), |p| p.off as usize);
                let actual = seed
                    .hydrate(
                        &offset_reader(&bytes[..end_off]),
                        &positioned[cut..end],
                        end as u64,
                    )
                    .unwrap();
                assert_eq!(
                    actual,
                    AdmissionCheckpointV1::from_frames(&records[..end]).unwrap()
                );
            }
        }
    }

    #[test]
    fn offset_admission_index_ahead_of_restored_log_falls_back_to_full_prefix() {
        let records = offset_corpus();
        let (bytes, positioned) = offset_log(&records);
        let boundary = offset_boundary(&bytes, &positioned, 12);
        let seed =
            AdmissionSeed::capture(&offset_reader(&bytes), &boundary, &positioned[..12]).unwrap();
        let restored = &bytes[..positioned[7].off as usize];
        let repaired = crate::run::load_prior_wal_snapshot_from_seed(
            &offset_reader(restored),
            &seed,
            &[],
            7,
            || Ok(records[..7].to_vec()),
        )
        .unwrap();
        assert_eq!(repaired.base_watermark, 0);
        assert_eq!(
            repaired.checkpoint_at_head,
            AdmissionCheckpointV1::from_frames(&records[..7]).unwrap()
        );
    }

    #[test]
    fn offset_admission_preserves_paused_state_with_ambiguous_tool_tail() {
        let corpus = offset_corpus();
        let mut records = corpus[..4].to_vec();
        let mut tool_step = offset_step(5);
        let call_id = ToolCallId("ambiguous".into());
        if let WalRecord::ModelStepFinished {
            step_id,
            assistant_message,
            ..
        } = &mut tool_step.record
        {
            *step_id = StepId(2);
            Arc::make_mut(assistant_message).content = vec![ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                tool_call_id: call_id.clone(),
                tool_name: "first".into(),
                input: serde_json::Value::Null,
            })];
        }
        records.push(tool_step);
        let batch_id = broca_protocol::BatchId("b2".into());
        records.push(frame(
            6,
            WalRecord::ToolBatchStarted {
                batch_id: batch_id.clone(),
                step_id: StepId(2),
                planned_calls: vec![call_id.clone()],
            },
        ));
        records.push(frame(
            7,
            WalRecord::ToolDispatchIntent {
                dispatched_at_ms: None,
                hooks: None,
                batch_id,
                tool_call_id: call_id,
                tool_name: Some("first".into()),
                args: Arc::new(serde_json::Value::Null),
                module_id: None,
                schema_pin: None,
                approval_digest: None,
                call_key: None,
            },
        ));
        records.push(frame(8, corpus[6].record.clone()));
        let (bytes, positioned) = offset_log(&records);
        let boundary = offset_boundary(&bytes, &positioned, 4);
        let seed =
            AdmissionSeed::capture(&offset_reader(&bytes), &boundary, &positioned[..4]).unwrap();
        let actual = seed
            .hydrate(&offset_reader(&bytes), &positioned[4..], 8)
            .unwrap();
        let full = AdmissionCheckpointV1::from_frames(&records).unwrap();
        assert_eq!(actual, full);
        assert_eq!(
            actual.last_episode.as_ref().unwrap().disposition,
            EpisodeDisposition::Paused
        );
        assert_eq!(
            seed.paused_episode(&offset_reader(&bytes), &positioned[4..], 8)
                .unwrap(),
            full.last_episode
        );
        assert!(matches!(
            crate::replay::replay(&records.iter().map(|p| p.record.clone()).collect::<Vec<_>>())
                .unwrap()
                .resolution,
            crate::ReplayResolution::Interrupted { .. }
        ));
    }

    #[test]
    fn offset_admission_downgrade_state_respects_refreeze_order_and_run_identity() {
        use broca_protocol::structured_output::{FrozenStructuredOutput, Mode, SchemaDelivery};
        let render = serde_json::json!({"structured_output": FrozenStructuredOutput {
            mode: Mode::FinalText, name: "result".into(), schema_jcs: "{}".into(), enforcement: "enforced".into(),
            reason: None, profile: None, delivery: SchemaDelivery::Strict, instructions_paragraph_break: false,
        }});
        for (before_refreeze, after_run, expected_delivery) in [
            (true, None, SchemaDelivery::Strict),
            (false, Some("r1"), SchemaDelivery::Unconstrained),
            (false, Some("another-run"), SchemaDelivery::Strict),
        ] {
            let mut start = started(1, "r1", None);
            if let WalRecord::RunStarted { config, .. } = &mut start.record {
                config.frozen_render_config = render.clone();
            }
            let mut records = vec![start];
            if before_refreeze {
                records.push(frame(
                    0,
                    WalRecord::StructuredOutputDowngraded {
                        run_id: RunId("r1".into()),
                    },
                ));
            }
            records.push(frame(
                0,
                WalRecord::RunConfigRefrozen {
                    renderer_version: 2,
                    ts_ms: None,
                    frozen_render_config: render.clone(),
                },
            ));
            if let Some(run) = after_run {
                records.push(frame(
                    0,
                    WalRecord::StructuredOutputDowngraded {
                        run_id: RunId(run.into()),
                    },
                ));
            }
            records.push(offset_step(0));
            let cut = records.len();
            records.push(frame(
                0,
                WalRecord::RunPaused {
                    run_id: RunId("r1".into()),
                    ts_ms: None,
                    reason: PauseReason::AuthRequired,
                    error: None,
                },
            ));
            for (i, frame) in records.iter_mut().enumerate() {
                frame.seq = i as u64 + 1;
            }
            let (bytes, positioned) = offset_log(&records);
            let boundary = offset_boundary(&bytes, &positioned, cut);
            if after_run == Some("another-run") {
                // Admission ignores unrelated downgrade metadata, but canonical replay refuses
                // its run identity. A seed must not turn that refusal into a usable checkpoint.
                assert!(AdmissionSeed::capture(
                    &offset_reader(&bytes),
                    &boundary,
                    &positioned[..cut]
                )
                .is_err());
                continue;
            }
            let seed =
                AdmissionSeed::capture(&offset_reader(&bytes), &boundary, &positioned[..cut])
                    .unwrap();
            let actual = seed
                .hydrate(
                    &offset_reader(&bytes),
                    &positioned[cut..],
                    records.len() as u64,
                )
                .unwrap();
            assert_eq!(
                actual,
                AdmissionCheckpointV1::from_frames(&records).unwrap()
            );
            let config = actual.last_episode.unwrap().config;
            assert_eq!(
                FrozenStructuredOutput::from_frozen_render_config(Some(
                    &config.frozen_render_config
                ))
                .unwrap()
                .delivery,
                expected_delivery
            );
        }
    }

    #[test]
    fn offset_admission_compaction_manifest_before_setup_requires_full_replay() {
        let mut records = offset_corpus()[..4].to_vec();
        if let WalRecord::RunStarted { manifest, .. } = &mut records[0].record {
            *manifest = Some(Box::new(broca_wal::PlanManifest { rich_ui: None, plan: broca_wal::OpaqueJson::from_json_text(serde_json::json!({"compaction_item": {"provider":"provider", "preset":"head", "params":{}}, "composition":{}}).to_string()).unwrap(),
            plan_identity: "plan".into(), composition_digest: "digest".into(), tools: Arc::new(vec![]), system_text: String::new(), system_digest: String::new(), join_version: 1, role_versions: Default::default(), fetches: vec![], declarations: vec![], }));
        }
        let (bytes, positioned) = offset_log(&records);
        let boundary = offset_boundary(&bytes, &positioned, 4);
        assert_eq!(
            AdmissionSeed::capture(&offset_reader(&bytes), &boundary, &positioned)
                .unwrap_err()
                .reason,
            "admission prefix needs full replay"
        );
    }

    #[test]
    fn offset_admission_setup_in_tail_forces_full_repair() {
        let mut records = offset_corpus()[..4].to_vec();
        records.push(frame(
            5,
            WalRecord::CompactionMessage {
                provider: "test".into(),
                kind: broca_wal::CompactionMessageKind::Setup,
                compaction_id: "setup".into(),
                version: 1,
                range: broca_wal::CompactionRange { from: 0, to: 0 },
                replacement: Arc::new(vec![]),
                sha256: "0".repeat(64),
                request_id: None,
                cursor: None,
                call_when: None,
                stability: None,
                ceiling: None,
                tag_checkpoint: None,
                ride_wait_ms: None,
            },
        ));
        let (bytes, positioned) = offset_log(&records);
        let boundary = offset_boundary(&bytes, &positioned, 4);
        let seed =
            AdmissionSeed::capture(&offset_reader(&bytes), &boundary, &positioned[..4]).unwrap();
        let repairs = std::cell::Cell::new(0);
        let repaired = crate::run::load_prior_wal_snapshot_from_seed(
            &offset_reader(&bytes),
            &seed,
            &positioned[4..],
            5,
            || {
                repairs.set(repairs.get() + 1);
                Ok(records.clone())
            },
        )
        .unwrap();
        assert_eq!(repairs.get(), 1);
        assert_eq!(
            repaired.checkpoint_at_head,
            AdmissionCheckpointV1::from_frames(&records).unwrap()
        );
    }

    #[test]
    fn offset_admission_paused_request_has_exact_source_read_budget() {
        let records = offset_corpus();
        let (bytes, positioned) = offset_log(&records);
        for (cut, end, expected_reads) in [(4, 7, 4), (12, 13, 2)] {
            let boundary = offset_boundary(&bytes, &positioned, cut);
            let seed =
                AdmissionSeed::capture(&offset_reader(&bytes), &boundary, &positioned[..cut])
                    .unwrap();
            let end_off = positioned.get(end).map_or(bytes.len(), |p| p.off as usize);
            let reads = std::cell::Cell::new(0);
            let read_at = |off, len| {
                let data = offset_reader(&bytes[..end_off])(off, len)?;
                if matches!(broca_wal::framing::decode_next(&data), broca_wal::framing::DecodeStep::Record { consumed, .. } if consumed == data.len())
                {
                    reads.set(reads.get() + 1);
                }
                Ok(data)
            };
            let episode = seed
                .paused_episode(&read_at, &positioned[cut..end], end as u64)
                .unwrap()
                .unwrap();
            let full = AdmissionCheckpointV1::from_frames(&records[..end])
                .unwrap()
                .last_episode
                .unwrap();
            assert_eq!(
                serde_json::to_vec(&episode).unwrap(),
                serde_json::to_vec(&full).unwrap()
            );
            assert_eq!(
                reads.get(),
                expected_reads,
                "two base source frames plus one per refreeze"
            );
        }
    }

    #[test]
    fn offset_admission_crash_cut_after_wal_barrier_before_seed_commit() {
        use std::io::{Read, Seek, SeekFrom, Write};
        let dir = std::path::Path::new("target/scratch").join(format!(
            "admission-cut-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let wal_path = dir.join("session.wal");
        let index_path = dir.join("admission-seed.json");
        let records = offset_corpus();
        let (bytes, positioned) = offset_log(&records);
        let boundary = offset_boundary(&bytes, &positioned, 4);
        let seed =
            AdmissionSeed::capture(&offset_reader(&bytes), &boundary, &positioned[..4]).unwrap();
        // Persist the old committed seed, then publish each later WAL prefix without committing
        // a replacement seed. Reopening must reconcile the durable tail, not lose its pause.
        std::fs::write(&index_path, serde_json::to_vec(&seed).unwrap()).unwrap();
        for end in 4..=records.len() {
            let end_off = positioned.get(end).map_or(bytes.len(), |p| p.off as usize);
            let mut wal = std::fs::File::create(&wal_path).unwrap();
            wal.write_all(&bytes[..end_off]).unwrap();
            wal.sync_all().unwrap();
            drop(wal);
            let seed: AdmissionSeed =
                serde_json::from_slice(&std::fs::read(&index_path).unwrap()).unwrap();
            let read_at = |off, len| {
                let mut file = std::fs::File::open(&wal_path)?;
                file.seek(SeekFrom::Start(off))?;
                let mut data = Vec::new();
                file.take(len).read_to_end(&mut data)?;
                Ok(data)
            };
            let snapshot = crate::run::load_prior_wal_snapshot_from_seed(
                &read_at,
                &seed,
                &positioned[4..end],
                end as u64,
                || panic!("lagging seed must reconcile the synced WAL"),
            )
            .unwrap();
            let full = AdmissionCheckpointV1::from_frames(&records[..end]).unwrap();
            assert_eq!(snapshot.checkpoint_at_head, full);
            assert_eq!(
                serde_json::to_vec(&snapshot.checkpoint_at_head.last_episode).unwrap(),
                serde_json::to_vec(&full.last_episode).unwrap()
            );
            if full
                .last_episode
                .as_ref()
                .is_some_and(|e| e.disposition == EpisodeDisposition::Paused)
            {
                assert_eq!(
                    serde_json::to_vec(
                        &seed
                            .paused_episode(&read_at, &positioned[4..end], end as u64)
                            .unwrap()
                    )
                    .unwrap(),
                    serde_json::to_vec(&full.last_episode).unwrap()
                );
            }
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn admission_paused_but_replay_interrupted_with_ambiguous_call_is_intentional() {
        let batch_id = broca_protocol::BatchId("batch".into());
        let call_id = ToolCallId("c".into());
        let records = vec![
            started(1, "r", None),
            frame(
                2,
                WalRecord::ModelStepFinished {
                    fallback: Default::default(),
                    written_at_ms: None,
                    tags: None,
                    hooks: None,
                    step_id: StepId(1),
                    assistant_message: AssistantMessage {
                        message_id: MessageId("tools".into()),
                        content: vec![ContentBlock::ToolCall(ToolCall {
                            thought_signature: None,
                            tool_call_id: call_id.clone(),
                            tool_name: "first".into(),
                            input: serde_json::Value::Null,
                        })],
                    }
                    .into(),
                    usage: Usage::default(),
                    finish_reason: None,
                    retries_used: 0,
                    service_tier_used: None,
                    speed_used: None,
                },
            ),
            frame(
                3,
                WalRecord::ToolBatchStarted {
                    batch_id: batch_id.clone(),
                    step_id: StepId(1),
                    planned_calls: vec![call_id.clone()],
                },
            ),
            frame(
                4,
                WalRecord::ToolDispatchIntent {
                    dispatched_at_ms: None,
                    hooks: None,
                    batch_id,
                    tool_call_id: call_id,
                    tool_name: Some("first".into()),
                    args: Arc::new(serde_json::Value::Null),
                    module_id: None,
                    schema_pin: None,
                    approval_digest: None,
                    call_key: None,
                },
            ),
            frame(
                5,
                WalRecord::RunPaused {
                    run_id: RunId("r".into()),
                    ts_ms: None,
                    reason: PauseReason::AuthRequired,
                    error: None,
                },
            ),
        ];
        // Admission preserves the durable auth pause, including the caller's ability to cancel it.
        // Replay must instead stop for review: the intent has no terminal, so retrying could
        // duplicate an effect. Collapsing the decisions loses either the pause or the call set.
        assert_eq!(
            AdmissionCheckpointV1::from_frames(&records)
                .unwrap()
                .last_episode
                .unwrap()
                .disposition,
            EpisodeDisposition::Paused
        );
        let wal: Vec<_> = records.iter().map(|frame| frame.record.clone()).collect();
        assert_eq!(
            crate::replay::replay(&wal).unwrap().resolution,
            crate::ReplayResolution::Interrupted {
                indeterminate: vec![ToolCallId("c".into())]
            }
        );
    }

    #[test]
    fn admission_running_but_replay_interrupted_for_unsealed_provider_free_seed_is_intentional() {
        let mut seed = started(1, "seed", None);
        let WalRecord::RunStarted { origin, config, .. } = &mut seed.record else {
            unreachable!()
        };
        *origin = RunOrigin::SeededFrom {
            origin_kind: broca_wal::SeedOriginKind::Import,
            source_ref: "transcript".into(),
            origin_device_pseudonym: None,
            base_seq: None,
        };
        config.model = ModelHandle::new("import", "transcript");
        config.renderer_version = 0;
        config.frozen_render_config = serde_json::Value::Null;
        let records = vec![seed];
        // Admission sees an unterminated episode that must be sealed before a new send.
        // Replay cannot call a provider with an import-marker config: repair is append-only.
        // No dispatch ever happened, so interruption must carry an empty ambiguous-call set;
        // it is recovery, not a human cancellation.
        assert_eq!(
            AdmissionCheckpointV1::from_frames(&records)
                .unwrap()
                .last_episode
                .unwrap()
                .disposition,
            EpisodeDisposition::Running
        );
        let wal: Vec<_> = records.iter().map(|frame| frame.record.clone()).collect();
        assert_eq!(
            crate::replay::replay(&wal).unwrap().resolution,
            crate::ReplayResolution::Interrupted {
                indeterminate: vec![]
            }
        );
    }

    fn terminal_usage() -> Usage {
        Usage {
            input_tokens: Some(11),
            cached_input_tokens: Some(2),
            cache_write_tokens: Some(3),
            output_tokens: Some(5),
            reasoning_tokens: Some(1),
        }
    }

    fn corpus() -> Vec<FramedRecord> {
        let mut budget_start = started(11, "r2", Some("three"));
        let WalRecord::RunStarted { config, .. } = &mut budget_start.record else {
            unreachable!()
        };
        config.budget = Some(BudgetConfig {
            max_input_tokens: None,
            nudges: vec![BudgetNudge {
                at_tool_calls: 2,
                text: "nudge".into(),
            }],
            finalize_at_tool_calls: 2,
            finalize_text: "finalize".into(),
        });
        vec![
            frame(
                1,
                WalRecord::Queued {
                    written_at_ms: None,
                    retention: None,
                    ts_ms: None,
                    hooks: None,
                    title: None,
                    keep_warm: None,
                    conversation_key: None,
                    send_id: None,
                    delivery: None,
                    submission_id: SubmissionId("one".into()),
                    request: Box::new(request("one")),
                    request_identity_digest: None,
                    principal: None,
                },
            ),
            frame(
                2,
                WalRecord::Queued {
                    written_at_ms: None,
                    retention: None,
                    ts_ms: None,
                    hooks: None,
                    title: None,
                    keep_warm: None,
                    conversation_key: None,
                    send_id: None,
                    delivery: None,
                    submission_id: SubmissionId("two".into()),
                    request: Box::new(request("two")),
                    request_identity_digest: None,
                    principal: None,
                },
            ),
            started(3, "r1", Some("one")),
            frame(
                4,
                WalRecord::RunConfigRefrozen {
                    renderer_version: 7,
                    ts_ms: None,
                    frozen_render_config: serde_json::json!({"wire": "new"}),
                },
            ),
            frame(5, WalRecord::StepStarted { step_id: StepId(1) }),
            frame(
                6,
                WalRecord::RunPaused {
                    run_id: RunId("r1".into()),
                    ts_ms: None,
                    reason: PauseReason::AuthRequired,
                    error: None,
                },
            ),
            frame(7, WalRecord::StepStarted { step_id: StepId(1) }),
            frame(
                8,
                WalRecord::RunFinished {
                    interrupt_reason: None,
                    ts_ms: None,
                    reason: RunFinishReason::Completed,
                    usage: Usage::default(),
                    error: None,
                    retries_used: 0,
                },
            ),
            frame(
                9,
                WalRecord::Retracted {
                    submission_id: SubmissionId("two".into()),
                    failure: None,
                },
            ),
            frame(
                10,
                WalRecord::Queued {
                    written_at_ms: None,
                    retention: None,
                    ts_ms: None,
                    hooks: None,
                    title: None,
                    keep_warm: None,
                    conversation_key: None,
                    send_id: None,
                    delivery: None,
                    submission_id: SubmissionId("three".into()),
                    request: Box::new(request("three")),
                    request_identity_digest: None,
                    principal: None,
                },
            ),
            budget_start,
            frame(
                12,
                WalRecord::ModelStepFinished {
                    fallback: Default::default(),
                    written_at_ms: None,
                    tags: None,
                    hooks: None,
                    step_id: StepId(1),
                    assistant_message: AssistantMessage {
                        message_id: MessageId("tools".into()),
                        content: ["a", "b"]
                            .into_iter()
                            .map(|id| {
                                ContentBlock::ToolCall(ToolCall {
                                    thought_signature: None,
                                    tool_call_id: ToolCallId(id.into()),
                                    tool_name: "first".into(),
                                    input: serde_json::Value::Null,
                                })
                            })
                            .collect(),
                    }
                    .into(),
                    usage: Usage::default(),
                    finish_reason: None,
                    retries_used: 0,
                    service_tier_used: None,
                    speed_used: None,
                },
            ),
            frame(
                13,
                WalRecord::ToolBatchStarted {
                    batch_id: broca_protocol::BatchId("batch".into()),
                    step_id: StepId(1),
                    planned_calls: vec![ToolCallId("a".into()), ToolCallId("b".into())],
                },
            ),
            frame(
                14,
                WalRecord::ToolNoDispatch {
                    dispatched_at_ms: None,
                    tags: None,
                    hooks: None,
                    batch_id: broca_protocol::BatchId("batch".into()),
                    tool_call_id: ToolCallId("a".into()),
                    reason: broca_wal::NoDispatchReason::Denied {
                        reason: "denied".into(),
                    },
                },
            ),
            frame(
                15,
                WalRecord::ToolNoDispatch {
                    dispatched_at_ms: None,
                    tags: None,
                    hooks: None,
                    batch_id: broca_protocol::BatchId("batch".into()),
                    tool_call_id: ToolCallId("b".into()),
                    reason: broca_wal::NoDispatchReason::Denied {
                        reason: "denied".into(),
                    },
                },
            ),
            frame(
                16,
                WalRecord::ToolBatchFinished {
                    batch_id: broca_protocol::BatchId("batch".into()),
                },
            ),
            frame(
                17,
                WalRecord::BudgetFired {
                    tags: None,
                    run_id: RunId("r2".into()),
                    kind: BudgetKind::Nudge,
                    threshold: 2,
                    ordinal: 4,
                    caller_text: Some("nudge".into()),
                },
            ),
            frame(
                18,
                WalRecord::BudgetFired {
                    tags: None,
                    run_id: RunId("r2".into()),
                    kind: BudgetKind::Finalize,
                    threshold: 2,
                    ordinal: 5,
                    caller_text: Some("finalize".into()),
                },
            ),
            frame(
                19,
                WalRecord::ModelStepFinished {
                    fallback: Default::default(),
                    written_at_ms: None,
                    tags: None,
                    hooks: None,
                    step_id: StepId(2),
                    assistant_message: AssistantMessage {
                        message_id: MessageId("answer".into()),
                        content: vec![ContentBlock::Text {
                            text: "answer".into(),
                        }],
                    }
                    .into(),
                    usage: Usage::default(),
                    finish_reason: None,
                    retries_used: 0,
                    service_tier_used: None,
                    speed_used: None,
                },
            ),
            frame(
                20,
                WalRecord::RunFinished {
                    interrupt_reason: None,
                    ts_ms: None,
                    reason: RunFinishReason::Interrupted,
                    usage: terminal_usage(),
                    error: None,
                    retries_used: 0,
                },
            ),
        ]
    }

    #[test]
    fn every_split_carries_budget_state_and_matches_literal_history() {
        let records = corpus();
        let full = AdmissionCheckpointV1::from_frames(&records).expect("full fold");
        // One literal per WAL cut: r1 is unbudgeted; r2 counts two calls, fires a nudge
        // then finalize, and spends the finalize allowance on its durable answer.
        type BudgetFacts<'a> = (u64, &'a [usize], bool, bool);
        let expected: &[Option<BudgetFacts<'_>>] = &[
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some((0, &[], false, false)),
            Some((2, &[], false, false)),
            Some((2, &[], false, false)),
            Some((2, &[], false, false)),
            Some((2, &[], false, false)),
            Some((2, &[], false, false)),
            Some((2, &[0], false, false)),
            Some((2, &[0], true, true)),
            Some((2, &[0], true, false)),
            Some((2, &[0], true, false)),
        ];
        assert_eq!(expected.len(), records.len() + 1);
        for (split, expected) in expected.iter().enumerate() {
            let mut incremental =
                AdmissionCheckpointV1::from_frames(&records[..split]).expect("prefix fold");
            let actual = incremental
                .last_episode
                .as_ref()
                .and_then(|episode| episode.budget_replay_state.as_ref())
                .map(|state| {
                    (
                        state.tool_call_count,
                        state.fired_nudge_indices.as_slice(),
                        state.finalize_fired,
                        state.finalize_pending,
                    )
                });
            assert_eq!(
                actual.as_ref(),
                expected.as_ref(),
                "literal budget at cut {split}"
            );
            incremental
                .fold_frames(&records[split..])
                .expect("tail fold");
            assert_eq!(incremental, full, "split at record {split}");
        }
    }

    #[test]
    fn budget_decisions_match_literal_corpus_cuts() {
        use crate::budget_kernel::decide;
        let records = corpus();
        let expected: &[&[(BudgetKind, u64)]] = &[
            &[],
            &[],
            &[],
            &[],
            &[],
            &[],
            &[],
            &[],
            &[],
            &[],
            &[],
            &[],
            &[(BudgetKind::Nudge, 2), (BudgetKind::Finalize, 2)],
            &[(BudgetKind::Nudge, 2), (BudgetKind::Finalize, 2)],
            &[(BudgetKind::Nudge, 2), (BudgetKind::Finalize, 2)],
            &[(BudgetKind::Nudge, 2), (BudgetKind::Finalize, 2)],
            &[(BudgetKind::Nudge, 2), (BudgetKind::Finalize, 2)],
            &[(BudgetKind::Finalize, 2)],
            &[],
            &[],
            &[],
        ];
        assert_eq!(expected.len(), records.len() + 1);
        let mut checkpoint = AdmissionCheckpointV1::empty();
        for (cut, expected) in expected.iter().enumerate() {
            let due: Vec<_> = checkpoint
                .last_episode
                .as_ref()
                .and_then(|episode| {
                    episode
                        .budget_replay_state
                        .as_ref()
                        .zip(episode.config.budget.as_ref())
                })
                .map(|(state, config)| {
                    decide(state, config)
                        .into_iter()
                        .map(|due| (due.kind, due.threshold))
                        .collect()
                })
                .unwrap_or_default();
            assert_eq!(
                due.as_slice(),
                *expected,
                "literal due firings at cut {cut}"
            );
            if let Some(frame) = records.get(cut) {
                checkpoint
                    .fold_record(frame.seq, &frame.record)
                    .expect("corpus fold");
            }
        }
    }

    #[test]
    fn full_derivation_has_literal_episode_and_budget_facts() {
        let records = corpus();
        let checkpoint = AdmissionCheckpointV1::from_frames(&records).expect("checkpoint");
        let episode = checkpoint.last_episode.expect("last episode");
        assert_eq!(episode.run_id, RunId("r2".into()));
        assert_eq!(episode.config.model, ModelHandle::new("stub", "r2"));
        assert!(!episode.provider_call_started);
        let budget = episode.budget_replay_state.unwrap();
        assert_eq!(
            (
                budget.tool_call_count,
                budget.fired_nudge_indices,
                budget.finalize_fired,
                budget.finalize_pending
            ),
            (2, vec![0], true, false)
        );
        assert_eq!(episode.disposition, EpisodeDisposition::Finished);
        assert_eq!(episode.usage, Some(terminal_usage()));
        assert!(checkpoint.pending_fifo.is_empty());
    }

    #[test]
    fn non_budget_episode_facts_match_independent_replay_authority() {
        let records = corpus();
        let checkpoint = AdmissionCheckpointV1::from_frames(&records).expect("checkpoint");
        let wal_records: Vec<_> = records.iter().map(|frame| frame.record.clone()).collect();
        let replay = crate::replay(&wal_records).expect("replay");
        let episode = checkpoint.last_episode.expect("last episode");
        assert_eq!(Some(episode.run_id), replay.run_id);
        assert_eq!(Some(episode.config), replay.run_config);
        assert_eq!(episode.provider_call_started, replay.provider_call_started);
    }

    #[test]
    fn admission_queue_matches_independent_queue_authority_at_every_cut() {
        let records = corpus();
        for cut in 0..=records.len() {
            let checkpoint = AdmissionCheckpointV1::from_frames(&records[..cut]).unwrap();
            let wal_records: Vec<_> = records[..cut]
                .iter()
                .map(|frame| frame.record.clone())
                .collect();
            assert_eq!(
                checkpoint.pending_fifo.as_ref(),
                &crate::pending_submissions(&wal_records),
                "queue at cut {cut}"
            );
        }
    }

    #[test]
    fn refreeze_corruption_checks_match_replay() {
        let after_call = vec![
            started(1, "r", None),
            frame(2, WalRecord::StepStarted { step_id: StepId(1) }),
            frame(
                3,
                WalRecord::RunConfigRefrozen {
                    renderer_version: 2,
                    ts_ms: None,
                    frozen_render_config: serde_json::Value::Null,
                },
            ),
        ];
        assert!(AdmissionCheckpointV1::from_frames(&after_call).is_err());
        assert!(crate::replay(
            &after_call
                .iter()
                .map(|frame| frame.record.clone())
                .collect::<Vec<_>>()
        )
        .is_err());

        let duplicate = vec![
            started(1, "r", None),
            frame(
                2,
                WalRecord::RunConfigRefrozen {
                    renderer_version: 2,
                    ts_ms: None,
                    frozen_render_config: serde_json::Value::Null,
                },
            ),
            frame(
                3,
                WalRecord::RunConfigRefrozen {
                    renderer_version: 3,
                    ts_ms: None,
                    frozen_render_config: serde_json::Value::Null,
                },
            ),
        ];
        assert!(AdmissionCheckpointV1::from_frames(&duplicate).is_err());
        assert!(crate::replay(
            &duplicate
                .iter()
                .map(|frame| frame.record.clone())
                .collect::<Vec<_>>()
        )
        .is_err());

        let auth_after_calls = vec![
            started(1, "r", None),
            frame(2, WalRecord::StepStarted { step_id: StepId(1) }),
            frame(
                3,
                WalRecord::RunAuthRefrozen {
                    ts_ms: Some(10),
                    auth_selection: AuthSelection {
                        auth_method: None,
                        credential_id: "chatgpt:openai".into(),
                        account_ref: Some("first-account".into()),
                        resolved_credential_id: None,
                        vault_request: None,
                        account_decision: None,
                    },
                },
            ),
            frame(
                4,
                WalRecord::RunAuthRefrozen {
                    ts_ms: Some(20),
                    auth_selection: AuthSelection {
                        auth_method: None,
                        credential_id: "chatgpt:openai".into(),
                        account_ref: Some("second-account".into()),
                        resolved_credential_id: None,
                        vault_request: None,
                        account_decision: None,
                    },
                },
            ),
        ];
        let checkpoint = AdmissionCheckpointV1::from_frames(&auth_after_calls)
            .expect("auth metadata may refreeze after calls and more than once");
        let replayed = crate::replay(
            &auth_after_calls
                .iter()
                .map(|frame| frame.record.clone())
                .collect::<Vec<_>>(),
        )
        .expect("replay accepts independent auth refreezes");
        let expected = AuthSelection {
            auth_method: None,
            credential_id: "chatgpt:openai".into(),
            account_ref: Some("second-account".into()),
            resolved_credential_id: None,
            vault_request: None,
            account_decision: None,
        };
        assert_eq!(
            checkpoint
                .last_episode
                .and_then(|episode| episode.config.auth_selection),
            Some(expected.clone())
        );
        assert_eq!(
            replayed.run_config.and_then(|config| config.auth_selection),
            Some(expected)
        );
    }

    #[test]
    fn budget_replay_state_folds_tool_calls_and_config_indices() {
        let mut started = started(1, "budget", None);
        let WalRecord::RunStarted { config, .. } = &mut started.record else {
            unreachable!("fixture starts an episode");
        };
        config.budget = Some(BudgetConfig {
            max_input_tokens: None,
            nudges: vec![
                BudgetNudge {
                    at_tool_calls: 1,
                    text: "first".into(),
                },
                BudgetNudge {
                    at_tool_calls: 1,
                    text: "second".into(),
                },
            ],
            finalize_at_tool_calls: 2,
            finalize_text: "finalize".into(),
        });
        let records = vec![
            started,
            frame(
                2,
                WalRecord::ModelStepFinished {
                    fallback: Default::default(),
                    written_at_ms: None,
                    tags: None,
                    hooks: None,
                    step_id: StepId(1),
                    assistant_message: AssistantMessage {
                        message_id: MessageId("m".into()),
                        content: vec![ContentBlock::ToolCall(ToolCall {
                            thought_signature: None,
                            tool_call_id: ToolCallId("c".into()),
                            tool_name: "first".into(),
                            input: serde_json::Value::Null,
                        })],
                    }
                    .into(),
                    usage: Usage::default(),
                    finish_reason: None,
                    retries_used: 0,
                    service_tier_used: None,
                    speed_used: None,
                },
            ),
            frame(
                3,
                WalRecord::BudgetFired {
                    tags: None,
                    run_id: RunId("budget".into()),
                    kind: BudgetKind::Nudge,
                    threshold: 1,
                    ordinal: 0,
                    caller_text: Some("first".into()),
                },
            ),
            frame(
                4,
                WalRecord::BudgetFired {
                    tags: None,
                    run_id: RunId("budget".into()),
                    kind: BudgetKind::Nudge,
                    threshold: 1,
                    ordinal: 1,
                    caller_text: Some("second".into()),
                },
            ),
        ];
        let checkpoint = AdmissionCheckpointV1::from_frames(&records).expect("fold budget state");
        assert_eq!(
            checkpoint
                .last_episode
                .and_then(|episode| episode.budget_replay_state),
            Some(BudgetReplayStateV1 {
                version: BUDGET_REPLAY_STATE_VERSION,
                tool_call_count: 1,
                fired_nudge_indices: vec![0, 1],
                finalize_fired: false,
                finalize_pending: false,
                budget_last_threshold: Some(1),
            })
        );
    }

    #[test]
    fn durable_finalize_response_spends_admission_pending_allowance() {
        let mut start = started(1, "budget", None);
        let budget = BudgetConfig {
            max_input_tokens: None,
            nudges: vec![],
            finalize_at_tool_calls: 1,
            finalize_text: "finalize".into(),
        };
        let WalRecord::RunStarted { config, .. } = &mut start.record else {
            unreachable!();
        };
        config.budget = Some(budget.clone());
        let step = |id, content| WalRecord::ModelStepFinished {
            fallback: Default::default(),
            written_at_ms: None,
            tags: None,
            hooks: None,
            step_id: StepId(id),
            assistant_message: AssistantMessage {
                message_id: MessageId(format!("m{id}")),
                content,
            }
            .into(),
            usage: Usage::default(),
            finish_reason: None,
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        };
        let mut checkpoint = AdmissionCheckpointV1::from_frames(&[
            start,
            frame(
                2,
                step(
                    1,
                    vec![ContentBlock::ToolCall(ToolCall {
                        thought_signature: None,
                        tool_call_id: ToolCallId("c".into()),
                        tool_name: "first".into(),
                        input: serde_json::Value::Null,
                    })],
                ),
            ),
            frame(
                3,
                WalRecord::BudgetFired {
                    tags: None,
                    run_id: RunId("budget".into()),
                    kind: BudgetKind::Finalize,
                    threshold: 1,
                    ordinal: 3,
                    caller_text: Some("finalize".into()),
                },
            ),
        ])
        .expect("pending finalize");
        let pending = checkpoint
            .last_episode
            .as_ref()
            .unwrap()
            .budget_replay_state
            .as_ref()
            .unwrap();
        assert!(pending.finalize_pending);
        assert!(pending.is_valid_for(&budget));
        checkpoint
            .fold_record(
                4,
                &step(
                    2,
                    vec![ContentBlock::Text {
                        text: "final answer".into(),
                    }],
                ),
            )
            .expect("durable finalize response");
        let spent = checkpoint
            .last_episode
            .unwrap()
            .budget_replay_state
            .unwrap();
        assert!(
            !spent.finalize_pending,
            "a durable finalize response spends the allowance"
        );
        assert!(spent.finalize_fired);
        assert_eq!(spent.tool_call_count, 1);
        // Structural validation cannot tell whether the finalize response is already durable.
        let mut stale = spent.clone();
        stale.finalize_pending = true;
        assert!(stale.is_valid_for(&budget));
    }

    #[test]
    fn terminal_usage_folds_and_the_next_episode_resets_it() {
        let mut checkpoint = AdmissionCheckpointV1::from_frames(&[
            started(1, "r1", None),
            frame(
                2,
                WalRecord::RunFinished {
                    interrupt_reason: None,
                    ts_ms: None,
                    reason: RunFinishReason::Completed,
                    usage: terminal_usage(),
                    error: None,
                    retries_used: 0,
                },
            ),
        ])
        .expect("terminal checkpoint");
        assert_eq!(
            checkpoint
                .last_episode
                .as_ref()
                .and_then(|episode| episode.usage),
            Some(terminal_usage())
        );

        checkpoint
            .fold_record(3, &started(3, "r2", None).record)
            .expect("next episode starts");
        assert_eq!(
            checkpoint
                .last_episode
                .as_ref()
                .and_then(|episode| episode.usage),
            None,
            "a new episode must not inherit the preceding run's usage"
        );
    }

    #[test]
    fn checkpoint_json_with_pending_payload_is_byte_stable() {
        let checkpoint = AdmissionCheckpointV1 {
            media: None,
            output_reservation: None,
            head: None,
            has_deferred_calls: None,
            tags: None,
            pending_order: None,
            input_budget: None,
            session_class: None,
            lineage_retention: None,
            compaction: None,
            effort_history: None,
            ownership: Some(SessionOwnershipV1::default()),
            at_seq: 7,
            last_episode: None,
            pending_fifo: Arc::new(vec![PendingSubmission {
                admitted_at_ms: None,
                order: None,
                sent_by: None,
                hooks: None,
                title: None,
                conversation_key: None,
                delivery: None,
                send_id: None,
                submission_id: SubmissionId("golden".into()),
                request: request("payload"),
                request_identity_digest: Some("digest".into()),
                principal: None,
            }]),
        };
        let encoded = encode_admission_checkpoint(&checkpoint).expect("encode checkpoint golden");
        assert_eq!(
            encoded,
            r#"{"version":1,"checkpoint":{"at_seq":7,"last_episode":null,"pending_fifo":[{"submission_id":"golden","request":{"model":{"provider_module_id":"stub","model_id":"queued"},"input":[{"role":"user","content":[{"type":"text","text":"payload"}]}],"tools":[],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]},"request_identity_digest":"digest"}],"ownership":{"episodes":0,"plan_manifest_folded":true}}}"#
        );
        assert_eq!(decode_admission_checkpoint(&encoded), Some(checkpoint));
    }

    /// A checkpoint stored before the ownership summary existed (these are its exact bytes)
    /// decodes with an unknown summary, never an empty one, so admission and projection can
    /// tell they must fold the whole WAL to learn who drove the session.
    #[test]
    fn a_checkpoint_stored_without_the_ownership_summary_has_an_unknown_summary() {
        let pre_ownership = r#"{"version":1,"checkpoint":{"at_seq":7,"last_episode":null,"pending_fifo":[{"submission_id":"golden","request":{"model":{"provider_module_id":"stub","model_id":"queued"},"input":[{"role":"user","content":[{"type":"text","text":"payload"}]}],"tools":[],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]},"request_identity_digest":"digest"}]}}"#;
        let decoded = decode_admission_checkpoint(pre_ownership).expect("still decodes");
        assert_eq!(decoded.ownership, None);
        assert_eq!(decoded.pending_fifo.len(), 1);
        assert!(!decoded.ownership_complete());
    }

    /// A summary stored by a build that does not record the first episode's plan manifest (it
    /// lacks `plan_manifest_folded`) is not built on, because a manifest it never recorded may
    /// be missing from it; one this build folds is.
    #[test]
    fn a_summary_that_predates_the_plan_manifest_is_not_built_on() {
        let unaware = r#"{"version":1,"checkpoint":{"at_seq":7,"last_episode":null,"pending_fifo":[],"ownership":{"episodes":1,"first_episode":{"run_id":"r","tool_names":[]}}}}"#;
        let decoded = decode_admission_checkpoint(unaware).expect("still decodes");
        assert!(decoded.ownership.is_some());
        assert!(!decoded.ownership_complete());
        assert!(AdmissionCheckpointV1::empty().ownership_complete());
    }

    /// The ownership summary folds every episode: the first principal, whether any episode
    /// recorded none, each distinct principal in order, and the latest frozen scope. An
    /// episode that reopened the scope at a later epoch replaces it, and an episode without a
    /// scope leaves it unchanged.
    #[test]
    fn ownership_folds_every_episode_and_keeps_the_latest_frozen_scope() {
        let core = BindPrincipal::Reserved {
            module_id: "prefrontal-core".into(),
        };
        let scope = |epoch| SessionScope {
            owner: core.clone(),
            scope_ref: "head-1".into(),
            scope_epoch: epoch,
            flow_id: None,
            sender: None,
            agent_id: None,
            run_id: None,
        };
        let mut ownership = SessionOwnershipV1::default();
        ownership.fold_run_started(Some(&core), None);
        ownership.fold_run_started(None, None);
        ownership.fold_run_started(Some(&BindPrincipal::Direct), Some(&scope(3)));
        ownership.fold_run_started(Some(&core), Some(&scope(4)));
        ownership.fold_run_started(Some(&core), None);
        assert_eq!(
            ownership,
            SessionOwnershipV1 {
                episodes: 5,
                scope: Some(scope(4)),
                first_principal: Some(core.clone()),
                any_unrecorded_principal: true,
                recorded_principals: vec![core.clone(), BindPrincipal::Direct],
                // `fold_run_started` alone records no first-episode facts; the checkpoint's own
                // `RunStarted` fold sets them, from the record's run id and tools.
                first_episode: None,
                plan_manifest_folded: true,
            }
        );
    }

    #[test]
    fn codec_round_trips_and_accepts_v1_without_usage() {
        let checkpoint = AdmissionCheckpointV1::from_frames(&corpus()).expect("checkpoint");
        let encoded = encode_admission_checkpoint(&checkpoint).expect("encode");
        assert_eq!(decode_admission_checkpoint(&encoded), Some(checkpoint));

        let mut legacy: serde_json::Value =
            serde_json::from_str(&encoded).expect("checkpoint json");
        let last_episode = legacy["checkpoint"]["last_episode"]
            .as_object_mut()
            .expect("last episode object");
        last_episode.remove("usage");
        last_episode.remove("budget_replay_state");
        let legacy_encoded = serde_json::to_string(&legacy).expect("legacy checkpoint json");
        let legacy_decoded =
            decode_admission_checkpoint(&legacy_encoded).expect("legacy v1 decodes");
        assert_eq!(
            legacy_decoded
                .last_episode
                .as_ref()
                .and_then(|episode| episode.usage),
            None
        );
        assert_eq!(
            legacy_decoded
                .last_episode
                .as_ref()
                .and_then(|episode| episode.budget_replay_state.as_ref()),
            None,
            "an absent budget state is legacy/unknown, never a default-filled seed"
        );
        let reencoded = encode_admission_checkpoint(&legacy_decoded).expect("re-encode legacy");
        assert_eq!(
            decode_admission_checkpoint(&reencoded),
            Some(legacy_decoded),
            "a v1 checkpoint without usage remains round-trip compatible"
        );

        assert_eq!(
            decode_admission_checkpoint(r#"{"version":2,"checkpoint":{}}"#),
            None
        );
        assert_eq!(decode_admission_checkpoint("not json"), None);
    }

    #[test]
    fn auth_pin_participates_in_submission_identity_without_changing_legacy_digest_bytes() {
        let unpinned = request("same");
        let legacy_digest = submission_identity_digest(&unpinned);
        assert!(
            !legacy_digest.contains("\"auth\""),
            "an absent auth pin must retain the pre-field identity shape"
        );

        let mut pinned = unpinned.clone();
        pinned.auth = Some(broca_protocol::AuthRequest {
            method: "apikey".into(),
            account: Some("astro".into()),
        });
        assert_eq!(submission_difference(&unpinned, &pinned), Some("auth"));
        assert_eq!(
            submission_identity_difference(&legacy_digest, &pinned),
            Some("auth")
        );
    }

    #[test]
    fn a_variant_is_part_of_submission_identity_and_absence_keeps_the_old_digest() {
        let plain = request("same");
        let mut with_variant = plain.clone();
        with_variant.variant = Some("high".into());

        assert_eq!(
            submission_difference(&plain, &with_variant),
            Some("variant")
        );
        assert_ne!(
            submission_identity_digest(&plain),
            submission_identity_digest(&with_variant)
        );
        assert_eq!(
            submission_identity_difference(&submission_identity_digest(&plain), &with_variant),
            Some("variant")
        );
        assert!(
            !submission_identity_digest(&plain).contains("variant"),
            "a digest without a variant must keep the shape stored before the field existed"
        );
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

    #[test]
    fn submission_digest_matches_the_exhaustive_identity_comparison() {
        let mut left = request("same");
        left.policy_allowed.insert("beta".into());
        left.policy_allowed.insert("alpha".into());
        let mut right = request("same");
        right.policy_allowed.insert("alpha".into());
        right.policy_allowed.insert("beta".into());
        assert_eq!(submission_difference(&left, &right), None);
        assert_eq!(
            submission_identity_digest(&left),
            submission_identity_digest(&right),
            "HashSet insertion order is outside request identity"
        );

        right.input = vec![message("changed")];
        assert_eq!(submission_difference(&left, &right), Some("input"));
        assert_eq!(
            submission_identity_difference(&submission_identity_digest(&left), &right),
            Some("input")
        );
        assert_ne!(
            submission_identity_digest(&left),
            submission_identity_digest(&right)
        );
    }
}
