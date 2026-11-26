//! One usage export fact per model attempt: each provider request a run sent, with the usage
//! the provider reported for it, beside the run's segment facts.
//!
//! A segment fact sums one (model, credential) slice of a run, so its reader can't tell which
//! requests the sum came from: whether one request's prompt crossed a long-prompt price
//! threshold, which service tier served it, or whether a request with no usage was refused or cut
//! off. An attempt fact answers those for one request.
//!
//! The usage comes from the segment builder itself ([`super::compute_run_billing`]): each share
//! of a step's usage that a segment counted goes to the attempt that answered the step, so a
//! segment's attempts sum to the segment by construction. A step's attempts are its tries in
//! record order. The shares recorded at the step's refusal or end belong to the last attempt
//! recorded before that record and after the step's previous shares; the step's earlier
//! attempts failed and were tried again, and carry no usage. Several shares from one record
//! (one request a provider handed on to another model) belong to one attempt.
//!
//! When the run's attempts and shares don't line up that way (a run recorded before attempts
//! were, or a share with no attempt before it), the run gets no attempt facts and its segments
//! stay unmarked, so it prices from its segments as before. Warms are never attempts: each warm
//! keeps its own fact, unmarked.

use std::collections::HashMap;

use broca_protocol::{BindIdentity, RunId, Usage};
use broca_records::record::RunConfig;

use super::input::{BillingEvent, BillingRecord};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    append_len_prefixed, compute_run_billing, credential_id_method, usage_policy_version,
    ChargeBasis, ChargeBasisResolver, ModelShare, SpendSegment,
};

/// What the session recorded about one model attempt, as the attempt fact needs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptEvidence {
    /// The seq of the attempt's record in the session, in the same numbering as the records
    /// handed to [`compute_attempt_facts`].
    pub seq: u64,
    pub step_id: u64,
    /// The try within the step, from 1.
    pub attempt: u32,
    pub attempt_id: Option<String>,
    pub outcome: String,
    pub http_status: Option<u32>,
    pub response_bytes: u64,
    /// Whether any of the answer's content arrived.
    pub content_received: bool,
    pub sent_at_ms: Option<u64>,
    /// When the attempt's record was written.
    pub recorded_at_ms: u64,
    /// The account the provider reported serving the request, when it reported one.
    pub served_account_ref: Option<String>,
    /// The provider's report of how it changed the request's input before the model ran, as the
    /// attempt's record keeps it; `None` when there was no report, or the request didn't ask for
    /// one.
    pub input_transformations: Option<Vec<broca_protocol::InputTransformation>>,
}

/// One model's share of an attempt's usage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptUsage {
    pub provider: String,
    pub model: String,
    pub usage: Usage,
    /// The service tier the provider reported for this share; absent when it reported none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub served_service_tier: Option<String>,
    /// The segment fact that counted this share too, which a reader records without pricing it.
    pub segment_fact_id: String,
}

/// The usage export fact for one model attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename = "model_attempt")]
pub struct AttemptFact {
    pub fact_id: String,
    pub run_id: RunId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_run_id: Option<String>,
    /// The daemon-stamped flow that made the request; absent on agent-run and unscoped facts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flow_id: Option<String>,
    #[serde(with = "super::fact_session")]
    pub session: BindIdentity,
    pub step_id: u64,
    pub attempt: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt_id: Option<String>,
    /// How the attempt ended, as recorded (`complete`, `http_error`, `chunk_error`, ...); kept
    /// open.
    pub outcome: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_status: Option<u32>,
    /// The model the request was sent to: the run's model, or for a step its model refused, the
    /// refusal-fallback model the step was sent to next. Who answered is in `usage_by_model`.
    pub provider: String,
    pub model: String,
    /// The usage the provider reported for this request, by the model each part counts under.
    /// Absent when it reported none (a retried try, a request that got no answer, a reply cut
    /// off before its usage), which is unknown usage, never zero.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage_by_model: Option<Vec<AttemptUsage>>,
    /// Always `true`: broca's usage counts reasoning tokens inside `output_tokens`, whatever the
    /// vendor reported.
    pub reasoning_included_in_output: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage_policy_version: Option<u32>,
    /// The service tier the run asked for: `standard`, `fast` or `ultrafast`.
    pub requested_service_tier: String,
    /// The speed the provider reported serving the request at, when it reported one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub served_speed: Option<String>,
    pub content_received: bool,
    pub response_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_selection: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_credential_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub served_account_ref: Option<String>,
    pub charge_basis: ChargeBasis,
    pub charge_basis_mapping_version: u32,
    pub charge_basis_overrides: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sent_at_ms: Option<u64>,
    pub recorded_at_ms: u64,
    /// The provider's report of how it changed this request's input before the model ran,
    /// verbatim and in input order: for OpenAI, each reasoning item it removed (`type`
    /// `reasoning`, its `id`, `action` `removed`, and a `reason` such as `prefix_mismatch`); for
    /// Anthropic, each signed thinking block it dropped (`type` `thinking_dropped`, its `path`,
    /// and a `reason`). Removed reasoning means the model reasoned again, at the cost of more
    /// output tokens. An empty list is a report of no change. Absent when the provider sent no
    /// report or the request didn't ask for one, which is unknown, never "no change".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_transformations: Option<Vec<broca_protocol::InputTransformation>>,
}

/// A run's export facts with its attempts: its segments, and one fact per attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunAttemptFacts {
    /// The run's segments, every one but a warm's marked [`SpendSegment::usage_in_attempts`]
    /// when `attempts` is not empty.
    pub segments: Vec<SpendSegment>,
    /// Empty when the run's attempts and shares don't line up (see the module doc).
    pub attempts: Vec<AttemptFact>,
}

/// The export facts of `run_id`: its segments, as [`super::compute_segments_for_run`] builds
/// them, and a fact per attempt in `attempts`, the run's attempt records in seq order.
pub fn compute_attempt_facts<R: ChargeBasisResolver>(
    records: &[BillingRecord],
    run_id: &RunId,
    attempts: &[AttemptEvidence],
    source_instance_id: &str,
    resolver: &R,
) -> RunAttemptFacts {
    let billing = compute_run_billing(records, run_id, source_instance_id, resolver);
    let Some(owners) = share_owners(attempts, &billing.shares) else {
        return RunAttemptFacts {
            segments: billing.segments,
            attempts: Vec::new(),
        };
    };
    let context = RunContext::read(records, run_id);
    let Some(session) = context.session.clone() else {
        return RunAttemptFacts {
            segments: billing.segments,
            attempts: Vec::new(),
        };
    };
    let mut owned: Vec<Vec<&ModelShare>> = vec![Vec::new(); attempts.len()];
    for (owner, share) in owners {
        owned[owner].push(share);
    }
    let mut facts = Vec::with_capacity(attempts.len());
    for (attempt, shares) in attempts.iter().zip(&owned) {
        let Some(config) = context.config_at(attempt.seq) else {
            // An attempt before the run's first start belongs to no run here.
            return RunAttemptFacts {
                segments: billing.segments,
                attempts: Vec::new(),
            };
        };
        facts.push(attempt_fact(
            source_instance_id,
            run_id,
            &session,
            &context,
            config,
            attempt,
            shares,
            resolver,
        ));
    }
    let mut segments = billing.segments;
    if !facts.is_empty() {
        for segment in segments.iter_mut().filter(|segment| !segment.warm) {
            segment.usage_in_attempts = true;
        }
    }
    RunAttemptFacts {
        segments,
        attempts: facts,
    }
}

/// Which attempt each share belongs to, as indexes into `attempts`; `None` when a share has no
/// attempt to go to.
fn share_owners<'s>(
    attempts: &[AttemptEvidence],
    shares: &'s [ModelShare],
) -> Option<Vec<(usize, &'s ModelShare)>> {
    // Each step's attempts, in seq order, so a share looks only at its own step's.
    let mut by_step: HashMap<u64, Vec<usize>> = HashMap::new();
    for (index, attempt) in attempts.iter().enumerate() {
        by_step.entry(attempt.step_id).or_default().push(index);
    }
    // The seq of each step's last share record, before which its next shares' attempt can't be.
    let mut claimed: HashMap<u64, u64> = HashMap::new();
    // The attempt that owns the shares of the record at each seq.
    let mut owner_at: HashMap<u64, usize> = HashMap::new();
    let mut owners = Vec::with_capacity(shares.len());
    for share in shares {
        let after = claimed.get(&share.step_id).copied();
        if after == Some(share.seq) {
            // Another model's share of the same request.
            owners.push((*owner_at.get(&share.seq)?, share));
            continue;
        }
        let owner = by_step
            .get(&share.step_id)?
            .iter()
            .rev()
            .copied()
            .find(|&index| {
                let attempt = &attempts[index];
                attempt.seq < share.seq && after.is_none_or(|after| attempt.seq > after)
            })?;
        claimed.insert(share.step_id, share.seq);
        owner_at.insert(share.seq, owner);
        owners.push((owner, share));
    }
    Some(owners)
}

/// What the run's records say about each attempt's request: the run's effective config over its
/// seqs, the model each refused step was sent to next, and each step's served speed.
struct RunContext {
    /// The run's config from each seq on, in seq order: its starts and refreezes.
    configs: Vec<(u64, RunConfig)>,
    session: Option<BindIdentity>,
    scope: Option<broca_protocol::SessionScope>,
    /// The seq of each refused step's refusal.
    refused_at: HashMap<u64, u64>,
    /// The refusal-fallback model each routed step went to.
    routed: HashMap<u64, broca_protocol::request::ModelHandle>,
    speed: HashMap<u64, String>,
}

impl RunContext {
    fn read(records: &[BillingRecord], run_id: &RunId) -> Self {
        let mut context = RunContext {
            configs: Vec::new(),
            session: None,
            scope: None,
            refused_at: HashMap::new(),
            routed: HashMap::new(),
            speed: HashMap::new(),
        };
        let mut own = false;
        for framed in records {
            match &framed.event {
                BillingEvent::RunStarted {
                    run_id: started,
                    session,
                    config,
                    scope,
                    ..
                } => {
                    own = started == run_id;
                    if own {
                        context.configs.push((framed.seq, (**config).clone()));
                        context.session = Some(session.clone());
                        context.scope = scope.clone();
                    }
                }
                _ if !own => {}
                BillingEvent::ConfigRefrozen {
                    renderer_version,
                    frozen_render_config,
                    ..
                } => {
                    if let Some((_, config)) = context.configs.last() {
                        let mut config = config.clone();
                        config.renderer_version = *renderer_version;
                        config.frozen_render_config = frozen_render_config.clone();
                        context.configs.push((framed.seq, config));
                    }
                }
                BillingEvent::AuthRefrozen { auth_selection, .. } => {
                    if let Some((_, config)) = context.configs.last() {
                        let mut config = config.clone();
                        config.auth_selection = Some(auth_selection.clone());
                        context.configs.push((framed.seq, config));
                    }
                }
                BillingEvent::StepRefused { step_id, .. } => {
                    context.refused_at.insert(step_id.0, framed.seq);
                }
                BillingEvent::StepFinished {
                    step_id,
                    speed_used,
                    fallback,
                    ..
                } => {
                    if let Some(route) = &fallback.route {
                        context.routed.insert(step_id.0, route.model.clone());
                    }
                    if let Some(speed) = speed_used {
                        context.speed.insert(step_id.0, speed.clone());
                    }
                }
                _ => {}
            }
        }
        context
    }

    /// The run's config in effect when the record at `seq` was written.
    fn config_at(&self, seq: u64) -> Option<&RunConfig> {
        self.configs
            .iter()
            .rev()
            .find(|(from, _)| *from < seq)
            .map(|(_, config)| config)
    }
}

// The run's identity, what its records say, and the attempt's own evidence; grouping them would
// only rename the same values.
#[allow(clippy::too_many_arguments)]
fn attempt_fact<R: ChargeBasisResolver>(
    source_instance_id: &str,
    run_id: &RunId,
    session: &BindIdentity,
    context: &RunContext,
    config: &RunConfig,
    attempt: &AttemptEvidence,
    shares: &[&ModelShare],
    resolver: &R,
) -> AttemptFact {
    let selection = config.auth_selection.as_ref();
    let credential_id = selection.map(|selection| selection.credential_id.clone());
    let method = credential_id.as_deref().and_then(credential_id_method);
    let (charge_basis, charge_basis_mapping_version) =
        resolver.resolve_charge_basis(config.billed_provider(), method.unwrap_or(""));
    let routed = context
        .refused_at
        .get(&attempt.step_id)
        .filter(|refused| attempt.seq > **refused)
        .and_then(|_| context.routed.get(&attempt.step_id));
    let (provider, model) = match routed {
        Some(model) => (model.provider_module_id.to_string(), model.model_id.clone()),
        None => (
            config.billed_provider().to_owned(),
            config.model.model_id.clone(),
        ),
    };
    let usage_by_model = (!shares.is_empty()).then(|| {
        shares
            .iter()
            .map(|share| AttemptUsage {
                provider: share.provider.clone(),
                model: share.model.clone(),
                usage: share.usage,
                served_service_tier: share.served_service_tier.clone(),
                segment_fact_id: share.segment_fact_id.clone(),
            })
            .collect()
    });
    AttemptFact {
        fact_id: attempt_fact_id(source_instance_id, session, run_id, attempt.seq),
        run_id: run_id.clone(),
        agent_id: context
            .scope
            .as_ref()
            .and_then(|scope| scope.agent_id.clone()),
        agent_run_id: context
            .scope
            .as_ref()
            .and_then(|scope| scope.run_id.clone()),
        flow_id: context
            .scope
            .as_ref()
            .and_then(|scope| scope.flow_id.clone()),
        session: session.clone(),
        step_id: attempt.step_id,
        attempt: attempt.attempt,
        attempt_id: attempt.attempt_id.clone(),
        outcome: attempt.outcome.clone(),
        http_status: attempt.http_status,
        provider,
        model,
        usage_by_model,
        reasoning_included_in_output: true,
        usage_policy_version: usage_policy_version(&config.frozen_render_config),
        requested_service_tier: broca_protocol::frozen_service_tier(&config.frozen_render_config)
            .to_owned(),
        served_speed: context.speed.get(&attempt.step_id).cloned(),
        content_received: attempt.content_received,
        response_bytes: attempt.response_bytes,
        auth_selection: credential_id,
        resolved_credential_id: selection
            .and_then(|selection| selection.resolved_credential_id.clone()),
        account_ref: selection.and_then(|selection| selection.account_ref.clone()),
        served_account_ref: attempt.served_account_ref.clone(),
        charge_basis,
        charge_basis_mapping_version,
        charge_basis_overrides: resolver.charge_basis_overrides(),
        sent_at_ms: attempt.sent_at_ms,
        recorded_at_ms: attempt.recorded_at_ms,
        input_transformations: attempt.input_transformations.clone(),
    }
}

/// The id of an attempt's fact, over the attempt record's seq. The encoding adds a tag no other
/// fact id has, so it can never equal a segment's or a warm's, whatever the numbers.
fn attempt_fact_id(
    source_instance_id: &str,
    session: &BindIdentity,
    run_id: &RunId,
    seq: u64,
) -> String {
    let mut input = Vec::new();
    for value in [
        source_instance_id.to_string(),
        session.project_root.to_string_lossy().into_owned(),
        session.harness.clone(),
        session.session.clone(),
        run_id.0.clone(),
        "model_attempt".to_string(),
    ] {
        append_len_prefixed(&mut input, value.as_bytes());
    }
    input.extend_from_slice(&seq.to_le_bytes());
    let digest = Sha256::digest(input);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests;
