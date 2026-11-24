//! The session command types and the helpers the store actor shares: the commands and replies
//! a session's actor answers (re-exported from `session_cmd`), and the rules a send's admission
//! applies to them.

use std::time::Duration;

use broca_core::{RunError, RunIndexState};
use broca_protocol::{request::SubmissionRequest, RunId};

/// How long an idle actor (no run, no pending work, no subscribers) holds the lease
/// before releasing it. Keeps the lease bounded AND exercises the reopen/close races in
/// v1 (a close path that never runs is a close path that rots).
pub const IDLE_TTL: Duration = Duration::from_secs(30);

/// How long the close teardown waits for the executor's `on_session_close` hook (a
/// best-effort wire send to free a tool-route channel) before proceeding with the close
/// anyway. Bounds external I/O so a stalled send can't hold the registry slot + lease open.
pub(crate) const ON_SESSION_CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

pub use crate::session_cmd::{
    ImportOutcome, LineageStatus, ScopeFreezeBusyReason, SendKey, SendOutcome, SendPlan,
};

pub(crate) use crate::session_cmd::{
    CancelAuthority, CancelOutcome, CutLockReply, LineageCmd, ReleaseOutcome, RetractOutcome,
    SessionFlushPrefixOutcome, SessionRefreshOutcome, SessionRefreshPolicyOutcome,
    SubscribeOutcome, WarmOutcome,
};
pub(crate) use crate::session_cmd::{StringSender, SHUTDOWN_SEAL_BOUND};

/// What an actor retired by a mutating cut hands to the cut.
pub struct RetiredForCut {
    /// The session lease, handed over only after the writer's descriptor is gone.
    /// `None` when the writer could not be closed and joined cleanly.
    pub tenure: Option<CutTenure>,
    /// The close of the actor's registry slot, already `Closing`. The cut commits it
    /// when it releases its gates, so the slot is absent before anyone else gets in.
    pub close: crate::registry::CloseToken,
}

/// The session lease a retired actor hands to a cut. Only the actor builds one, once its
/// writer stopped and its descriptors closed, so holding one proves no writer of the session is
/// live. It can only keep the session locked: dropping it releases the lease.
pub struct CutTenure(#[allow(dead_code)] Box<dyn std::any::Any + Send>);

impl CutTenure {
    pub(crate) fn store(lease: Box<dyn std::any::Any + Send>) -> Self {
        Self(lease)
    }
}

/// Recover the finish reason from a terminal run-index state; active and paused are unfinished.
pub(crate) fn indexed_finish_reason(
    state: &RunIndexState,
) -> Option<broca_protocol::RunFinishReason> {
    use broca_protocol::RunFinishReason;
    match state {
        RunIndexState::Active | RunIndexState::Paused => None,
        RunIndexState::Completed => Some(RunFinishReason::Completed),
        RunIndexState::Interrupted => Some(RunFinishReason::Interrupted),
        RunIndexState::MaxSteps => Some(RunFinishReason::MaxSteps),
        RunIndexState::BudgetExhausted => Some(RunFinishReason::BudgetExhausted),
        RunIndexState::ContextCeiling => Some(RunFinishReason::ContextCeiling),
        RunIndexState::Cancelled => Some(RunFinishReason::Cancelled),
        RunIndexState::Error => Some(RunFinishReason::Error),
        RunIndexState::TransformUnavailable => Some(RunFinishReason::TransformUnavailable),
    }
}

/// Admission checks of a send's `structured_output` that depend on the session, after the
/// serve layer has checked the value itself. Returns the field of the `invalid_params` refusal.
///
/// A steer joining the active run runs under that run's frozen request, so it may only repeat
/// it exactly. Any other send starts a run: its admitted tool set (the plan's frozen tools, the
/// send's own, or the tools it inherits by omitting them) must not include `submit_result`, and
/// must be empty for `final_text`.
///
/// `latest` is the session's latest run as frozen: its provider render config and its tools.
pub(crate) fn check_structured_output(
    structured_output: &broca_protocol::structured_output::StructuredOutput,
    request: &SubmissionRequest,
    plan: Option<&SendPlan>,
    latest: Option<(&serde_json::Value, &[broca_protocol::request::Tool])>,
    joins_active_run: bool,
) -> Result<(), String> {
    use broca_protocol::structured_output::{FrozenStructuredOutput, FIELD};
    if joins_active_run {
        let frozen = latest.and_then(|(render, _)| {
            FrozenStructuredOutput::from_frozen_render_config(Some(render))
        });
        return match frozen {
            Some(frozen) if frozen.matches(structured_output) => Ok(()),
            _ => Err(FIELD.to_owned()),
        };
    }
    let tools: Vec<String> = match (plan.and_then(|plan| plan.manifest.as_ref()), &request.tools) {
        (Some(manifest), _) => manifest
            .tools
            .iter()
            .map(|tool| tool.name.clone())
            .collect(),
        (None, Some(tools)) => tools.iter().map(|tool| tool.name.clone()).collect(),
        (None, None) => latest
            .map(|(_, tools)| tools.iter().map(|tool| tool.name.clone()).collect())
            .unwrap_or_default(),
    };
    structured_output.check(true, &tools)
}

/// Fill each runner parameter `request` left to the session (`inherit_frozen`) from `frozen`, the
/// session's latest episode's config, so the new turn renders the same request prefix that
/// episode did. The cache policy is not a send's parameter: each run takes it from the session
/// (`store_actor::cache_policy`).
pub(crate) fn inherit_frozen_parameters(
    request: &mut SubmissionRequest,
    frozen: &broca_records::record::RunConfig,
) {
    use broca_protocol::request::InheritedParam;
    for param in request.inherit_frozen.clone() {
        match param {
            InheritedParam::Model => {
                request.model = frozen.model.clone();
                request.variant.clone_from(&frozen.variant);
            }
            InheritedParam::ToolChoice => request.tool_choice = frozen.tool_choice.clone(),
            InheritedParam::Generation => request.generation = frozen.generation.clone(),
            InheritedParam::StopWhen => request.stop_when.clone_from(&frozen.stop_when),
            InheritedParam::Budget => {
                request.budget.clone_from(&frozen.budget);
                // Inherit tool-call reminders without the preceding run's input cap, which a send
                // never inherits.
                if let Some(budget) = &mut request.budget {
                    budget.max_input_tokens = None;
                }
            }
            InheritedParam::ContextLimit => request.context_limit = frozen.context_limit,
            InheritedParam::WorkClass => request.work_class.clone_from(&frozen.work_class),
            InheritedParam::ServiceTier => request.service_tier = frozen.service_tier,
        }
    }
}

/// The settings a running run froze that a joining steer must agree with.
pub(crate) struct SteerFrozen<'a> {
    pub model: &'a broca_protocol::request::ModelHandle,
    pub variant: Option<&'a str>,
    pub generation: &'a broca_protocol::request::GenerationConfig,
    pub joining_temperature: Option<Option<f64>>,
    pub stop_when: &'a [broca_protocol::request::StopCondition],
    pub directives: Option<&'a [broca_protocol::request::DirectiveName]>,
    pub auth_selection: Option<&'a broca_records::record::AuthSelection>,
}

impl<'a> From<&'a broca_records::record::RunConfig> for SteerFrozen<'a> {
    fn from(config: &'a broca_records::record::RunConfig) -> Self {
        Self {
            model: &config.model,
            variant: config.variant.as_deref(),
            generation: &config.generation,
            joining_temperature: None,
            stop_when: &config.stop_when,
            directives: config.turn_directives_v1.as_deref(),
            auth_selection: config.auth_selection.as_ref(),
        }
    }
}

/// Compare only named settings: a joining steer inherits every setting it omits. The order
/// is stable so a send changing several settings always gets the same first field in its reply.
pub(crate) fn steer_settings_conflict(
    request: &SubmissionRequest,
    frozen: SteerFrozen<'_>,
    stop_sequences_named: bool,
) -> Option<&'static str> {
    use broca_protocol::request::InheritedParam;
    if !request.inherit_frozen.contains(&InheritedParam::Model) && request.model != *frozen.model {
        return Some("model");
    }
    if request.variant.is_some() && request.variant.as_deref() != frozen.variant {
        return Some("model.variant");
    }
    let generation = &request.generation;
    let frozen_generation = &frozen.generation;
    let joining_temperature = if request.inherit_frozen.contains(&InheritedParam::Generation) {
        generation.temperature
    } else {
        frozen.joining_temperature.unwrap_or(generation.temperature)
    };
    if generation.max_output_tokens.is_some()
        && generation.max_output_tokens != frozen_generation.max_output_tokens
    {
        return Some("generation.max_output_tokens");
    }
    if generation.temperature.is_some() && joining_temperature != frozen_generation.temperature {
        return Some("generation.temperature");
    }
    if generation.top_p.is_some() && generation.top_p != frozen_generation.top_p {
        return Some("generation.top_p");
    }
    if (stop_sequences_named || !generation.stop_sequences.is_empty())
        && generation.stop_sequences != frozen_generation.stop_sequences
    {
        return Some("generation.stop_sequences");
    }
    if !request.inherit_frozen.contains(&InheritedParam::StopWhen)
        && request.stop_when.as_slice() != frozen.stop_when
    {
        return Some("stop_when.max_steps");
    }
    if request
        .directives
        .as_ref()
        .is_some_and(|names| names.as_slice() != frozen.directives.unwrap_or_default())
    {
        return Some("directives");
    }
    // Checked last, so a send that also conflicts on an earlier setting still reports that
    // setting rather than `auth`.
    if let Some(pin) = &request.auth {
        if !steer_auth_pin_matches(pin, frozen.auth_selection) {
            return Some("auth");
        }
    }
    None
}

/// Whether a joining steer's auth pin names the credential the active run is already using.
///
/// The run's credential is the one admission froze (or an auth refreeze replaced), read from the
/// episode config, never the provider's configured default. Its id has the form
/// `<method>:<provider>[:<account>]`. A pin naming only a method matches on the method alone,
/// even when the run was routed to an account. A pin naming an account matches only that frozen
/// account: a running run cannot be moved to another account, and a run with no frozen account
/// cannot be told which one it used. A run with no frozen selection at all (no vault, or a record
/// older than the field) cannot confirm any pin, so every pin is refused rather than ignored.
fn steer_auth_pin_matches(
    pin: &broca_protocol::request::AuthRequest,
    frozen: Option<&broca_records::record::AuthSelection>,
) -> bool {
    let Some(selection) = frozen else {
        return false;
    };
    let mut segments = selection.credential_id.splitn(3, ':');
    let method = segments.next();
    let _provider = segments.next();
    let account = segments.next();
    if method != Some(pin.method.as_str()) {
        return false;
    }
    match &pin.account {
        None => true,
        Some(named) => account == Some(named.as_str()),
    }
}

/// Now, in milliseconds since the Unix epoch.
pub(crate) fn epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

/// The reply to a warm the executor did not complete.
pub(crate) fn warm_error_outcome(
    error: crate::executor::WarmError,
    request_body_sha256: String,
) -> WarmOutcome {
    use crate::executor::WarmError;
    use broca_wire::WarmRefusalDetail as Refusal;
    let detail = match error {
        WarmError::Unsupported(reason) => Refusal::WarmUnsupported { reason },
        WarmError::PrefixUnavailable(reason) => Refusal::WarmPrefixUnavailable {
            request_body_sha256: Some(request_body_sha256),
            reason,
        },
        WarmError::AccountChanged { frozen, served } => Refusal::AccountChanged {
            frozen_account: frozen,
            served_account: served,
        },
        // The warm itself found the provider account exhausted (`account_limit_exhausted`):
        // reported as `account_walled`, the refusal every later warm meets until the reset.
        WarmError::Provider(error) if error.is_account_limit_exhausted() => {
            match error.resets_at_ms {
                Some(resets_at_ms) => Refusal::AccountWalled { resets_at_ms },
                None => Refusal::WarmFailed {
                    error: (*error).into(),
                },
            }
        }
        WarmError::Provider(error) => Refusal::WarmFailed {
            error: (*error).into(),
        },
        WarmError::Unavailable(message) => return WarmOutcome::Transient(message),
    };
    WarmOutcome::Refused(broca_wire::WarmRefusal::new(detail))
}

/// The answer to a send whose run was refused before it durably started. Each refusal keeps its
/// own typed outcome, so the caller gets the same code, detail and class whichever storage the
/// session is kept in; any other admission error is answered with the class its `RunError`
/// states.
pub(crate) fn admission_refusal_outcome(error: RunError) -> SendOutcome {
    match error {
        RunError::MediaUnsupported(message) => {
            crate::person_media::outcome(RunError::MediaUnsupported(message))
        }
        RunError::MediaTooLarge(detail) => SendOutcome::MediaTooLarge(detail),
        RunError::Upload(error) => SendOutcome::Upload(error),
        RunError::Transient(message) => SendOutcome::Transient(message),
        RunError::SendIdReuse { differing } => SendOutcome::SendIdReuse { differing },
        RunError::VariantUnsupported(e) => SendOutcome::VariantUnsupported(e),
        RunError::ToolAliasInvalid(e) => SendOutcome::ToolAliasInvalid(e),
        error @ RunError::CompactionContextLimitMissing => SendOutcome::DeliveryRefused {
            code: "invalid_params",
            field: "context_limit",
            message: error.to_string(),
        },
        RunError::AccountSelectionRefused(refusal) => SendOutcome::AccountSelectionRefused(refusal),
        RunError::RoutingUnavailable(unavailable) => SendOutcome::RoutingUnavailable(unavailable),
        RunError::CredentialsUnavailable => SendOutcome::CredentialsUnavailable,
        RunError::CredentialAmbiguous {
            credential,
            candidate_ids,
        } => SendOutcome::CredentialAmbiguous {
            credential,
            candidate_ids,
        },
        error => SendOutcome::ClassifiedError {
            class: error.admission_error_class(),
            reason: error.to_string(),
        },
    }
}

/// The run id a queued submission starts under: `queue_submission` names it `sub-<run_id>`.
pub(crate) fn run_id_of_queued(submission_id: &broca_protocol::SubmissionId) -> RunId {
    RunId(
        submission_id
            .0
            .strip_prefix("sub-")
            .unwrap_or(&submission_id.0)
            .to_owned(),
    )
}

/// Whether a keyed send's retry asks for the same delivery order as the send it repeats: the
/// list, dedupe key and template, by llm-runner's comparison (`same_payload`), which leaves out
/// the expiry.
pub(crate) fn same_order(
    retry: &broca_protocol::DeliveryOrder,
    earlier: &broca_protocol::DeliveryOrder,
) -> bool {
    let role_order = |order: &broca_protocol::DeliveryOrder| {
        let mut published =
            cortexkit_role_llm_runner::delivery_order::DeliveryOrder::new(order.modes.clone());
        published.dedupe_key = order.dedupe_key.clone();
        published.expires_at_ms = order.expires_at_ms;
        published.template = order.template.clone();
        published
    };
    role_order(retry).same_payload(&role_order(earlier))
}

pub(crate) use crate::session_cmd::pre_user;
