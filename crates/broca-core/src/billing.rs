//! Pure billing-segment derivation from the durable WAL.
//!
//! A segment is opened by a `RunStarted`, `RunConfigRefrozen`, or `RunAuthRefrozen` record,
//! or by a model call answered by another model than the open segment's (a step routed to the
//! refusal-fallback model, or one an Anthropic server-side fallback served), and is closed only
//! when the run reaches a durable terminal record or one of those opens the next segment. This module
//! deliberately has no clock or filesystem access: rebuilding an export from
//! the same WAL must reproduce the same facts byte-for-byte.

use broca_protocol::{
    caller::RunFinishReason, request::ModelHandle, BindIdentity, RunId, StepId, Usage,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

pub mod attempt;
mod fact_session;
pub mod input;

pub use input::{BillingEvent, BillingRecord};

/// When installing a session WAL received from another machine, select its
/// `RunStarted` ids that are absent from this machine's current session WAL.
/// `incoming` contains the verified source WAL prefix through the move's frozen
/// byte offset; `local` contains the WAL that installation will replace, if any.
/// Durably record the result with [`crate::ExportIndex::record_imported_runs`]
/// before publishing the received WAL.
///
/// This is set membership, not a sequence or timestamp floor. An unfinished local
/// episode must stay billable if installation fails and the old WAL resumes.
/// The caller supplies only the verified cut's records through its end offset.
pub fn imported_run_ids(incoming: &[BillingRecord], local: &[BillingRecord]) -> Vec<RunId> {
    fn started_ids(records: &[BillingRecord]) -> BTreeSet<String> {
        records
            .iter()
            .filter_map(|frame| match &frame.event {
                BillingEvent::RunStarted { run_id, .. } => Some(run_id.0.clone()),
                _ => None,
            })
            .collect()
    }

    started_ids(incoming)
        .difference(&started_ids(local))
        .cloned()
        .map(RunId)
        .collect()
}

/// Version of the canonical provider-usage interpretation that counts xAI's sibling
/// reasoning tokens separately. Runs frozen under earlier policies remain unversioned.
pub const USAGE_POLICY_VERSION: u32 = 2;

/// How a provider credential contributes to spend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChargeBasis {
    MeteredApi,
    SubscriptionIncluded,
    Unknown,
}

/// Precision of a segment's durable timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimestampPrecision {
    Exact,
    Degraded,
    Terminal,
    // There is deliberately no variant for ABSENT, and that is a limitation rather than a
    // decision anyone made. `occurred_at_precision` says `Terminal` on facts whose
    // `occurred_at_ms` is `None` because the write site has nothing truthful to put there:
    // `Degraded` already means "derived from a coarser source", which is a different claim
    // from "there is no timestamp", so writing it would swap one wrong answer for another.
    //
    // The consequence is that no discipline at a call site can fix the field -- every write
    // in the tree is correct with respect to this type. Adding a variant is a wire change to
    // an export lane consumers have already ingested and cursored past, so it needs their
    // agreement on the shape, not a unilateral edit. Until then, `occurred_at_ms.is_some()`
    // is the only sound test for whether a fact can be placed in a period.
}

/// The configuration-layer seam used by [`compute_segments`].  The method is a
/// string because `broca-core` must not depend on the serve adapter's auth
/// routing type; the adapter resolves its own `AuthMethod` into this seam.
///
/// The `u32` a resolution returns is the SHIPPED mapping version of the build doing the
/// resolving; local configuration never raises it. Which local override rows were in effect
/// is reported separately by [`ChargeBasisResolver::charge_basis_overrides`].
pub trait ChargeBasisResolver {
    fn resolve_charge_basis(&self, provider_id: &str, method: &str) -> (ChargeBasis, u32);

    /// [`NO_CHARGE_BASIS_OVERRIDES`] when the resolver applies the shipped table unchanged,
    /// otherwise a digest identifying the exact local override rows applied on top of it.
    /// Deliberately required rather than defaulted: a wrapper that forgot to forward it would
    /// otherwise stamp "none" on facts that were resolved with overrides.
    fn charge_basis_overrides(&self) -> String;
}

/// The `charge_basis_overrides` value meaning "no local override rows were in effect": the
/// fact was resolved against exactly the shipped table named by its mapping version.
pub const NO_CHARGE_BASIS_OVERRIDES: &str = "none";

impl ChargeBasis {
    /// The wire spelling, identical to the serde form.
    pub fn as_str(self) -> &'static str {
        match self {
            ChargeBasis::MeteredApi => "metered_api",
            ChargeBasis::SubscriptionIncluded => "subscription_included",
            ChargeBasis::Unknown => "unknown",
        }
    }
}

/// The `charge_basis_overrides` value for a set of local override rows, each
/// `(provider, auth method, basis)`.
///
/// Returns [`NO_CHARGE_BASIS_OVERRIDES`] for no rows. Otherwise a SHA-256 over the rows sorted
/// by provider then auth method, rendered as 64 lowercase hex characters. Sorting here makes
/// the digest a function of WHICH rows are in effect, not of the order a config file listed
/// them, so two deployments with identical overrides get the same value. Each field is
/// length-prefixed so no pair of different rows can encode to the same bytes. The digest only
/// identifies the table; it cannot be reversed into the rows.
pub fn charge_basis_overrides_digest(rows: &[(String, String, ChargeBasis)]) -> String {
    if rows.is_empty() {
        return NO_CHARGE_BASIS_OVERRIDES.to_string();
    }
    let mut sorted: Vec<&(String, String, ChargeBasis)> = rows.iter().collect();
    sorted.sort_by(|left, right| (&left.0, &left.1).cmp(&(&right.0, &right.1)));
    let mut input = Vec::new();
    for (provider, method, basis) in sorted {
        append_len_prefixed(&mut input, provider.as_bytes());
        append_len_prefixed(&mut input, method.as_bytes());
        append_len_prefixed(&mut input, basis.as_str().as_bytes());
    }
    let digest = Sha256::digest(input);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// A bare function has no override rows, so it reports [`NO_CHARGE_BASIS_OVERRIDES`].
impl<F> ChargeBasisResolver for F
where
    F: Fn(&str, &str) -> (ChargeBasis, u32),
{
    fn resolve_charge_basis(&self, provider_id: &str, method: &str) -> (ChargeBasis, u32) {
        self(provider_id, method)
    }

    fn charge_basis_overrides(&self) -> String {
        NO_CHARGE_BASIS_OVERRIDES.to_string()
    }
}

/// One maximal WAL slice with a homogeneous billing identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpendSegment {
    pub run_id: RunId,
    /// The agent ID from the scope saved on `RunStarted`, never inferred from a session name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    /// The scope's agent run, distinct from broca's episode `run_id` and not part of `fact_id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_run_id: Option<String>,
    /// The storage identity; its namespace root is omitted from exported project attribution.
    #[serde(with = "fact_session")]
    pub session: BindIdentity,
    pub segment_ordinal: u64,
    pub fact_id: String,
    pub provider: String,
    pub model: String,
    /// The frozen credential id, without any secret material.
    pub auth_selection: Option<String>,
    /// The vault-served account id the run authenticated with, frozen verbatim at admission.
    /// Absent (never an empty string) when the run has no vault account, so the consumer maps
    /// it to an unlabeled provider. Deliberately NOT a component of the credential id and NOT
    /// part of `fact_id` — it labels a fact without changing its identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_ref: Option<String>,
    /// Vault-resolved credential identity, stored alongside the routing key used to select
    /// the credential and never substituted for that key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_credential_id: Option<String>,
    /// The parameters broca sent on the vault `credential.get` that resolved this run's frozen
    /// credential selection, so this fact can be matched to the vault's own record of that
    /// request. Absent when the run made no vault request (OpenCode/mock) or predates the field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vault_request: Option<broca_records::record::VaultRequest>,
    /// How the run's provider account was chosen, as its selection froze it: the
    /// `AccountDecision::path` (`routed`, `disabled`, `unreachable`, `pinned`, `legacy_static`,
    /// ...). Absent when the run recorded none (no vault account to choose, or a run admitted
    /// before broca asked routing), so those facts keep their bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_decision: Option<String>,
    /// Opaque workload class declared by the caller and frozen at run admission.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work_class: Option<String>,
    pub charge_basis: ChargeBasis,
    /// The shipped charge-basis table version of the build that wrote this fact. Local
    /// configuration never raises it; see `charge_basis_overrides` for local rows.
    pub charge_basis_mapping_version: u32,
    /// Which local override rows were applied on top of the shipped table:
    /// `"none"` means none were (exactly the shipped table); any other value is a 64-character
    /// lowercase hex SHA-256 identifying the override rows. ABSENT only on facts written
    /// before this field existed -- the producer always sets it, so absence alone carries
    /// that meaning and stored facts keep their exact bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub charge_basis_overrides: Option<String>,
    /// Identifies the frozen usage interpretation under which this segment was normalized.
    ///
    /// To find xAI facts affected by the pre-v2 undercount, use:
    /// `provider = xai AND reasoning_tokens > 0 AND`
    /// `(usage_policy_version IS NULL OR usage_policy_version < 2)`.
    /// The null arm is required because pre-fix facts were written without this field; SQL
    /// comparisons against NULL do not evaluate true, and existing facts are not back-stamped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage_policy_version: Option<u32>,
    pub usage: Usage,
    /// This segment's usage broken down by the service tier each step's provider response
    /// REPORTED (`service_tier_used` on the step), keyed by the tier exactly as the provider
    /// spelled it: `{"default": Usage, "priority": Usage}`. A segment is one (model, auth)
    /// slice, and a provider can serve its steps at different tiers (a priority request
    /// downgraded to default under load is billed at default), so pricing needs the split.
    ///
    /// A step whose response reported no tier is NOT in this map: its tier is unknown, and
    /// filing it under a guessed tier would price it at a rate nobody reported. So the values
    /// here need NOT sum to `usage` -- the difference is exactly the usage of the steps that
    /// reported nothing. Absent (no key at all) when no step in the segment reported a tier,
    /// which keeps such a fact's bytes and `fact_id` exactly what they were before this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage_by_service_tier: Option<BTreeMap<String, Usage>>,
    pub retries_used: u32,
    pub pricing_at_ms: Option<u64>,
    /// Unlike its sibling below, this one does track its timestamp: every row without a
    /// `pricing_at_ms` is labelled `Degraded` and every `Exact` row has one. Measured across
    /// the export lane rather than assumed, because nothing enforces it -- no consumer reads
    /// this field, so the agreement rests entirely on the write site having known the
    /// timestamp could be absent. Keep that property when adding a write path.
    pub pricing_at_precision: TimestampPrecision,
    pub occurred_at_ms: Option<u64>,
    /// How the timestamp beside it was derived -- NOT whether one exists.
    ///
    /// This says `Terminal` on every fact ever exported, including the ones whose
    /// `occurred_at_ms` is `None`, because it describes the derivation rule rather than the
    /// outcome of applying it. A reader deciding whether a fact can be placed in a period must
    /// test `occurred_at_ms.is_some()`; branching on this field instead returns a confident
    /// answer for a row that cannot support one.
    ///
    /// The undateable rows are a closed set: the one-time backfill that re-derived facts for
    /// runs predating `ts_ms` on the WAL envelope. Bounded and historical, so the share shrinks
    /// as traffic accumulates -- but it is 17% of facts and a tenth of all recorded output
    /// tokens today, which is enough for a time-bucketed total to be quietly wrong rather than
    /// visibly incomplete.
    pub occurred_at_precision: TimestampPrecision,
    /// Only the final segment carries the run's terminal reason.
    pub terminal_reason: Option<RunFinishReason>,
    /// `true` only on the fact for one cache warm attempt, priced from its own reported usage.
    /// Legacy facts describe successful refreshes; new facts with `bill-attempts/v1` evidence
    /// carry `warm_outcome` so a failed attempt is not mistaken for a refreshed cache.
    /// Each fact uses its own WAL record: its
    /// `segment_ordinal` is that record's WAL sequence and its `fact_id` is taken over that
    /// sequence, so it never moves the ids or ordinals of the run's ordinary segments. Absent on
    /// every other fact, which keeps their bytes and ids exactly what they were.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub warm: bool,
    /// Outcome of a warm attempt with `bill-attempts/v1` evidence. Absent on legacy success-only
    /// warm facts, ordinary model-step facts and facts stored before this field existed;
    /// a failure can still carry provider-reported billable usage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warm_outcome: Option<broca_records::record::BillingAttemptOutcome>,
    /// `true` when the run's usage is also exported one fact per model attempt
    /// ([`attempt::AttemptFact`]), each share of which names its segment. A reader
    /// prices the attempts and records a segment marked this way without pricing it, so no usage
    /// is counted twice. Absent on a run without attempt facts, whose segments price as before;
    /// a reader must not infer the rule from finding attempts, which it may read before or after
    /// their segment.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub usage_in_attempts: bool,
}

/// One model's share of one step's billed usage, as a segment counted it: the unit an attempt
/// fact carries. A step answered by one model has one share; a step a provider handed on inside
/// one request has one per model, in the order the segment counted them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelShare {
    /// The seq of the record the usage came from: the step's end, or its refusal.
    pub seq: u64,
    pub step_id: u64,
    /// Whether the usage came from the step's refusal, which the run's own model was billed for
    /// before the step went to its refusal-fallback model.
    pub refused: bool,
    pub provider: String,
    pub model: String,
    pub usage: Usage,
    /// The service tier the provider reported for this share; absent when it reported none.
    pub served_service_tier: Option<String>,
    /// The ordinary segment the usage was counted in.
    pub segment_ordinal: u64,
    pub segment_fact_id: String,
}

/// A run's segments and the shares they counted, from [`compute_run_billing`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunBilling {
    pub segments: Vec<SpendSegment>,
    /// The run's step shares in record order. Warms are not shares: each warm is its own fact.
    pub shares: Vec<ModelShare>,
}

#[derive(Debug)]
struct OpenRun {
    source_instance_id: String,
    run_id: RunId,
    session: BindIdentity,
    config: broca_records::record::RunConfig,
    scope: Option<broca_protocol::SessionScope>,
    segments: Vec<SpendSegment>,
    freeze_ts_ms: Option<u64>,
    usage: Usage,
    /// The open segment's usage from steps that reported a service tier, by that tier.
    usage_by_service_tier: BTreeMap<String, Usage>,
    retries_used: u32,
    terminal_ts_ms: Option<u64>,
    terminal_reason: Option<RunFinishReason>,
    pause_candidate: Option<Option<u64>>,
    segment_open: bool,
    /// The model that answered the open segment's steps when it is not the run's own: the
    /// refusal-fallback model a step was routed to, or the model an Anthropic server-side
    /// fallback served it with. `None` means the run's model.
    serving: Option<ModelHandle>,
    /// Model calls folded into the open segment.
    segment_calls: u32,
    /// Every share counted so far, in record order.
    shares: Vec<ModelShare>,
}

impl OpenRun {
    fn new(
        source_instance_id: &str,
        run_id: RunId,
        session: BindIdentity,
        config: broca_records::record::RunConfig,
        ts_ms: Option<u64>,
        scope: Option<broca_protocol::SessionScope>,
    ) -> Self {
        Self {
            source_instance_id: source_instance_id.to_string(),
            run_id,
            session,
            config,
            scope,
            segments: Vec::new(),
            freeze_ts_ms: ts_ms,
            usage: Usage::default(),
            usage_by_service_tier: BTreeMap::new(),
            retries_used: 0,
            terminal_ts_ms: None,
            terminal_reason: None,
            pause_candidate: None,
            segment_open: true,
            serving: None,
            segment_calls: 0,
            shares: Vec::new(),
        }
    }

    /// Each serving iteration joins the existing homogeneous segment, or opens a
    /// new one when its model differs. Ordinary single-model calls keep their bytes.
    fn record_usage<R: ChargeBasisResolver>(
        &mut self,
        resolver: &R,
        seq: u64,
        record: &BillingEvent,
        at_ms: Option<u64>,
        retries_used: u32,
        service_tier: Option<&str>,
    ) -> u32 {
        let episode_model = self.config.model.clone();
        let entries = crate::refusal_fallback::StepServing::of_event(record)
            .map(|serving| serving.usage_by_model(&episode_model, None))
            .unwrap_or_default();
        let iterations = match record {
            BillingEvent::StepFinished { fallback, .. } => fallback.usage_iterations.as_deref(),
            BillingEvent::StepRefused {
                usage_iterations, ..
            } => usage_iterations.as_deref(),
            _ => None,
        };
        let mut closed_retries = 0u32;
        for (index, (provider, model, _, usage)) in entries.iter().enumerate() {
            let model = ModelHandle::new(*provider, *model);
            // Named as the segment names it: the run's own model under the provider it bills as.
            let share_provider = if model == self.config.model {
                self.config.billed_provider().to_owned()
            } else {
                model.provider_module_id.to_string()
            };
            let share_model = model.model_id.clone();
            let serving = (model != self.config.model).then_some(model);
            closed_retries =
                closed_retries.saturating_add(self.bill_under(resolver, serving, at_ms));
            self.segment_calls = self.segment_calls.saturating_add(1);
            self.usage += *usage;
            let last = index + 1 == entries.len();
            // Top-level response metadata describes only the last serving iteration.
            let tier = iterations
                .and_then(|entries| entries[index].other.get("service_tier"))
                .and_then(serde_json::Value::as_str)
                .or_else(|| last.then_some(service_tier).flatten());
            let (step_id, refused) = match record {
                BillingEvent::StepFinished { step_id, .. } => (step_id.0, false),
                BillingEvent::StepRefused { step_id, .. } => (step_id.0, true),
                _ => (0, false),
            };
            let segment_ordinal = self.segments.len() as u64;
            self.shares.push(ModelShare {
                seq,
                step_id,
                refused,
                provider: share_provider,
                model: share_model,
                usage: *usage,
                served_service_tier: tier.map(str::to_owned),
                segment_ordinal,
                segment_fact_id: fact_id(
                    &self.source_instance_id,
                    &self.session,
                    &self.run_id,
                    segment_ordinal,
                ),
            });
            if let Some(tier) = tier {
                *self
                    .usage_by_service_tier
                    .entry(tier.to_owned())
                    .or_default() += *usage;
            }
            if last {
                self.retries_used = self.retries_used.saturating_add(retries_used);
            }
        }
        closed_retries
    }

    /// Bill the calls that follow under `serving`. When that is another model than the open
    /// segment's and the segment already holds a call, the segment closes and a new one opens
    /// at `at_ms`, as a refreeze does; returns the retries the closed segment carried. A run
    /// whose calls are all answered by its own model never splits here, so its segments keep
    /// their ordinals and fact ids.
    fn bill_under<R: ChargeBasisResolver>(
        &mut self,
        resolver: &R,
        serving: Option<ModelHandle>,
        at_ms: Option<u64>,
    ) -> u32 {
        if serving == self.serving {
            return 0;
        }
        let mut closed_retries = 0;
        if self.segment_calls > 0 {
            closed_retries = self.retries_used;
            self.close_segment(resolver, false);
            self.freeze_ts_ms = at_ms;
            self.segment_open = true;
        }
        self.serving = serving;
        closed_retries
    }

    fn close_segment<R: ChargeBasisResolver>(&mut self, resolver: &R, terminal: bool) {
        let credential_id = self
            .config
            .auth_selection
            .as_ref()
            .map(|selection| selection.credential_id.clone());
        // The account the run authenticated with, frozen verbatim at admission. NEVER derived
        // from the credential id's components — only the vault-served account id labels spend.
        let account_ref = self
            .config
            .auth_selection
            .as_ref()
            .and_then(|selection| selection.account_ref.clone());
        let resolved_credential_id = self
            .config
            .auth_selection
            .as_ref()
            .and_then(|selection| selection.resolved_credential_id.clone());
        let vault_request = self
            .config
            .auth_selection
            .as_ref()
            .and_then(|selection| selection.vault_request.clone());
        let account_decision = self
            .config
            .auth_selection
            .as_ref()
            .and_then(|selection| selection.account_decision.as_ref())
            .map(|decision| decision.path.clone());
        let work_class = self.config.work_class.clone();
        let method = credential_id.as_deref().and_then(credential_id_method);
        let (charge_basis, mapping_version) =
            resolver.resolve_charge_basis(self.config.billed_provider(), method.unwrap_or(""));
        let charge_basis_overrides = resolver.charge_basis_overrides();
        let segment_ordinal = self.segments.len() as u64;
        let pricing_at_ms = self.freeze_ts_ms.or(self.terminal_ts_ms);
        let pricing_at_precision = if self.freeze_ts_ms.is_some() {
            TimestampPrecision::Exact
        } else {
            TimestampPrecision::Degraded
        };
        let fact_id = fact_id(
            &self.source_instance_id,
            &self.session,
            &self.run_id,
            segment_ordinal,
        );
        self.segments.push(SpendSegment {
            run_id: self.run_id.clone(),
            agent_id: self.scope.as_ref().and_then(|scope| scope.agent_id.clone()),
            agent_run_id: self.scope.as_ref().and_then(|scope| scope.run_id.clone()),
            session: self.session.clone(),
            segment_ordinal,
            fact_id,
            provider: self.serving.as_ref().map_or_else(
                || self.config.billed_provider().to_owned(),
                |serving| serving.provider_module_id.to_string(),
            ),
            model: self
                .serving
                .as_ref()
                .unwrap_or(&self.config.model)
                .model_id
                .clone(),
            auth_selection: credential_id,
            account_ref,
            resolved_credential_id,
            vault_request,
            account_decision,
            work_class,
            charge_basis,
            charge_basis_mapping_version: mapping_version,
            charge_basis_overrides: Some(charge_basis_overrides),
            usage_policy_version: usage_policy_version(&self.config.frozen_render_config),
            usage: self.usage,
            usage_by_service_tier: (!self.usage_by_service_tier.is_empty())
                .then(|| std::mem::take(&mut self.usage_by_service_tier)),
            retries_used: self.retries_used,
            pricing_at_ms,
            pricing_at_precision,
            occurred_at_ms: self.terminal_ts_ms,
            occurred_at_precision: TimestampPrecision::Terminal,
            terminal_reason: terminal
                .then_some(self.terminal_reason.unwrap_or(RunFinishReason::Interrupted)),
            warm: false,
            warm_outcome: None,
            usage_in_attempts: false,
        });
        self.usage = Usage::default();
        self.usage_by_service_tier.clear();
        self.retries_used = 0;
        self.freeze_ts_ms = None;
        self.segment_open = false;
        self.segment_calls = 0;
    }

    /// Closes the run's open segment and returns its segments, moving its shares to `shares`.
    fn finish<R: ChargeBasisResolver>(
        mut self,
        resolver: &R,
        shares: &mut Vec<ModelShare>,
    ) -> Vec<SpendSegment> {
        shares.append(&mut self.shares);
        if self.segment_open {
            self.close_segment(resolver, self.terminal_reason.is_some());
        }
        // A refreeze closes a segment before the terminal record is available. Once
        // the durable terminal timestamp is known, backfill it onto every earlier
        // segment so period attribution does not drop non-final slices.
        if let Some(terminal_ts_ms) = self.terminal_ts_ms {
            for segment in &mut self.segments {
                if segment.occurred_at_ms.is_none() {
                    segment.occurred_at_ms = Some(terminal_ts_ms);
                }
            }
        }
        self.segments
    }
}

/// Compute billing-homogeneous segments from one run or a complete session lineage.
///
/// The input must be in WAL order. The function is intentionally pure: terminal
/// and freeze timestamps come only from fields present in the records. Records that
/// lack those fields therefore produce `None` rather than using a rebuild-time clock.
pub fn compute_segments<R: ChargeBasisResolver>(
    records: &[BillingRecord],
    source_instance_id: &str,
    resolver: &R,
) -> Vec<SpendSegment> {
    compute_segments_matching(records, None, source_instance_id, resolver, &mut Vec::new())
}

/// Compute billing-homogeneous segments for one target run without constructing
/// segments for the other runs in the same lineage.
pub fn compute_segments_for_run<R: ChargeBasisResolver>(
    records: &[BillingRecord],
    run_id: &RunId,
    source_instance_id: &str,
    resolver: &R,
) -> Vec<SpendSegment> {
    compute_segments_matching(
        records,
        Some(run_id),
        source_instance_id,
        resolver,
        &mut Vec::new(),
    )
}

/// [`compute_segments_for_run`], with the step shares its segments counted.
pub fn compute_run_billing<R: ChargeBasisResolver>(
    records: &[BillingRecord],
    run_id: &RunId,
    source_instance_id: &str,
    resolver: &R,
) -> RunBilling {
    let mut shares = Vec::new();
    let segments = compute_segments_matching(
        records,
        Some(run_id),
        source_instance_id,
        resolver,
        &mut shares,
    );
    RunBilling { segments, shares }
}

fn compute_segments_matching<R: ChargeBasisResolver>(
    records: &[BillingRecord],
    target_run: Option<&RunId>,
    source_instance_id: &str,
    resolver: &R,
    shares: &mut Vec<ModelShare>,
) -> Vec<SpendSegment> {
    let mut output = Vec::new();
    let mut open: Option<OpenRun> = None;
    let mut closed_retries_used = 0u32;
    // Every episode's session and effective config (refreezes applied), by run id, tracked even
    // when only another run's facts are wanted, plus which episode is the latest. A cache warm is
    // billed under the config of the run whose request it re-sent, which may have ended, and may
    // even be followed by a newer episode, before the warm's record was written.
    let mut episodes: std::collections::HashMap<
        RunId,
        (
            BindIdentity,
            broca_records::record::RunConfig,
            Option<broca_protocol::SessionScope>,
        ),
    > = std::collections::HashMap::new();
    let mut latest_run: Option<RunId> = None;
    // Steps routed to the refusal-fallback model: a warm that re-sends one of their requests is
    // billed under the model it went to.
    let mut routed_steps: std::collections::HashMap<(RunId, StepId), ModelHandle> =
        std::collections::HashMap::new();

    for framed in records {
        match &framed.event {
            BillingEvent::RunStarted {
                run_id,
                session,
                config,
                ts_ms,
                scope,
                ..
            } => {
                episodes.insert(
                    run_id.clone(),
                    (session.clone(), (**config).clone(), scope.clone()),
                );
                latest_run = Some(run_id.clone());
                if let Some(previous) = open.take() {
                    output.extend(previous.finish(resolver, shares));
                }
                closed_retries_used = 0;
                if target_run.is_none_or(|target| target == run_id) {
                    open = Some(OpenRun::new(
                        source_instance_id,
                        run_id.clone(),
                        session.clone(),
                        (**config).clone(),
                        *ts_ms,
                        scope.clone(),
                    ));
                }
            }
            BillingEvent::ConfigRefrozen {
                renderer_version,
                ts_ms,
                frozen_render_config,
            } => {
                if let Some((_, config, _)) =
                    latest_run.as_ref().and_then(|run| episodes.get_mut(run))
                {
                    config.renderer_version = *renderer_version;
                    config.frozen_render_config = frozen_render_config.clone();
                }
                if let Some(current) = open.as_mut() {
                    current.pause_candidate = None;
                    closed_retries_used = closed_retries_used.saturating_add(current.retries_used);
                    current.close_segment(resolver, false);
                    current.config.renderer_version = *renderer_version;
                    current.config.frozen_render_config = frozen_render_config.clone();
                    current.freeze_ts_ms = *ts_ms;
                    current.segment_open = true;
                }
            }
            BillingEvent::AuthRefrozen {
                ts_ms,
                auth_selection,
            } => {
                if let Some((_, config, _)) =
                    latest_run.as_ref().and_then(|run| episodes.get_mut(run))
                {
                    config.auth_selection = Some(auth_selection.clone());
                }
                if let Some(current) = open.as_mut() {
                    current.pause_candidate = None;
                    closed_retries_used = closed_retries_used.saturating_add(current.retries_used);
                    current.close_segment(resolver, false);
                    current.config.auth_selection = Some(auth_selection.clone());
                    current.freeze_ts_ms = *ts_ms;
                    current.segment_open = true;
                }
            }
            BillingEvent::StepFinished {
                written_at_ms,
                step_id,
                retries_used,
                service_tier_used,
                fallback,
                ..
            } => {
                if let (Some(run), Some(route)) = (latest_run.as_ref(), fallback.route.as_ref()) {
                    routed_steps.insert((run.clone(), *step_id), route.model.clone());
                }
                if let Some(current) = open.as_mut() {
                    current.pause_candidate = None;
                    closed_retries_used = closed_retries_used.saturating_add(current.record_usage(
                        resolver,
                        framed.seq,
                        &framed.event,
                        *written_at_ms,
                        *retries_used,
                        service_tier_used.as_deref(),
                    ));
                }
            }
            // A refused call was billed: it joins the segment of the model that refused it.
            BillingEvent::StepRefused { ts_ms, .. } => {
                if let Some(current) = open.as_mut() {
                    current.pause_candidate = None;
                    closed_retries_used = closed_retries_used.saturating_add(current.record_usage(
                        resolver,
                        framed.seq,
                        &framed.event,
                        *ts_ms,
                        0,
                        None,
                    ));
                }
            }
            BillingEvent::RunFinished {
                reason,
                ts_ms,
                retries_used,
                ..
            } => {
                if let Some(mut current) = open.take() {
                    // The terminal aggregate includes retries burned by a failed step, which has
                    // no ModelStepFinished record. Earlier segments keep their step-derived
                    // values; only the still-unrepresented residue belongs to the final segment.
                    let represented = closed_retries_used.saturating_add(current.retries_used);
                    let residue = retries_used.saturating_sub(represented);
                    current.retries_used = current.retries_used.saturating_add(residue);
                    current.terminal_ts_ms = *ts_ms;
                    current.terminal_reason = Some(*reason);
                    output.extend(current.finish(resolver, shares));
                }
            }
            BillingEvent::RunPaused { ts_ms, .. } => {
                if let Some(current) = open.as_mut() {
                    // A pause can be resumed under the same run id. Defer closing
                    // until the record stream ends so later steps are not stranded
                    // outside a segment; if no later step arrives, this is the final
                    // terminal timestamp for the derived view.
                    current.pause_candidate = Some(*ts_ms);
                }
            }
            BillingEvent::Warmed {
                run_id,
                step_id,
                usage,
                sent_at_ms,
                envelope,
                trigger,
                ..
            } => {
                // A warm is billed under the run whose request it re-sent. A warm naming a run
                // this WAL holds no start of has no config to bill it with, and yields no fact.
                if let Some((session, config, scope)) = episodes.get(run_id) {
                    if target_run.is_none_or(|target| target == run_id) {
                        // A late manual source warm historically followed its step's committed
                        // fallback route. Only explicitly tagged new source maintenance may
                        // choose the source model; old records must retain their fact bytes.
                        let source =
                            trigger.as_deref() == Some(crate::cache_warm::SOURCE_KEEP_WARM_TRIGGER);
                        let routed = routed_steps
                            .get(&(run_id.clone(), *step_id))
                            .filter(|_| !source)
                            .map(|model| {
                                let mut config = config.clone();
                                config.model = model.clone();
                                config
                            });
                        let mut segment = warm_segment(
                            source_instance_id,
                            run_id,
                            session,
                            routed.as_ref().unwrap_or(config),
                            envelope
                                .as_deref()
                                .map_or(*usage, |e| e.usage.unwrap_or_default()),
                            framed.seq,
                            envelope
                                .as_deref()
                                .and_then(|e| e.sent_at_ms.or(e.finished_at_ms))
                                .unwrap_or(*sent_at_ms),
                            envelope.as_deref().map(|e| e.outcome),
                            resolver,
                        );
                        segment.agent_id = scope.as_ref().and_then(|scope| scope.agent_id.clone());
                        segment.agent_run_id =
                            scope.as_ref().and_then(|scope| scope.run_id.clone());
                        output.push(segment);
                    }
                }
            }
        }
    }

    if let Some(mut current) = open {
        // Without RunFinished there is no durable run-total aggregate to reconcile; preserve
        // only retries derived from completed steps in the dangling segment.
        if let Some(ts_ms) = current.pause_candidate.take() {
            current.terminal_ts_ms = ts_ms;
            current.terminal_reason = Some(RunFinishReason::Interrupted);
        }
        output.extend(current.finish(resolver, shares));
    }

    for share in shares.iter_mut() {
        share.usage = share.usage.with_complete_cache_write_lifetimes();
    }
    for segment in &mut output {
        segment.usage = segment.usage.with_complete_cache_write_lifetimes();
        if let Some(tiers) = &mut segment.usage_by_service_tier {
            for usage in tiers.values_mut() {
                *usage = usage.with_complete_cache_write_lifetimes();
            }
        }
    }
    output
}

/// Record version 2 only when the saved run configuration explicitly uses the
/// separate-reasoning-token normalizer. Leave unknown or older formats unset so downstream
/// consumers can identify and repair those records.
///
/// THE SINGLE-VALUE MATCH IS THE POINT, NOT AN OVERSIGHT, AND WIDENING IT CORRUPTS HISTORY.
/// `open_ai_usage_reasoning_separate` is a policy variant the xAI accounting fix INTRODUCED,
/// so the string exists only in configs frozen by binaries that already had the fix. Matching
/// it is therefore a version discriminator by construction: the presence of the string IS the
/// evidence, and no other field in the frozen config carries that information.
///
/// Matching on the `usage` key's PRESENCE instead looks like the obvious repair -- every
/// provider records a policy, so six of seven currently go unstamped and `IS NULL` is
/// ambiguous for them. It was tried and reverted. Every provider has recorded a usage policy
/// since long before the fix (measured: all of the oldest WALs carrying `run_started` carry a
/// `usage` key, and plain `open_ai_usage` predates the separate variant by months), so the
/// widened rule stamps generation 2 onto runs frozen by binaries that did not have the fix --
/// claiming provenance they never had, on the one field a consumer uses to tell them apart.
///
/// The ambiguity for the other six providers is real and unresolved, and it CANNOT be fixed
/// here: for them the frozen config holds no evidence of which binary froze it. Resolving it
/// needs a separate engine-generation field written at run start, which is a different field
/// with a different meaning -- not a widening of this one.
fn usage_policy_version(frozen_render_config: &serde_json::Value) -> Option<u32> {
    match frozen_render_config
        .get("usage")
        .and_then(serde_json::Value::as_str)
    {
        Some("open_ai_usage_reasoning_separate") => Some(USAGE_POLICY_VERSION),
        _ => None,
    }
}

/// Extract the auth method (the leading `<method>:` component) from a frozen credential id
/// (`<method>:<provider>[:<account>]`). The method drives charge-basis resolution. The account
/// component is deliberately NOT parsed here: usage labeling reads the vault-served account id
/// frozen on `AuthSelection`, never a credential-id component.
fn credential_id_method(id: &str) -> Option<&str> {
    let mut parts = id.splitn(3, ':');
    let method = parts.next().filter(|value| !value.is_empty());
    let provider = parts.next();
    if method.is_none() || provider.is_none() || provider == Some("") {
        return None;
    }
    method
}

/// Stable SHA-256 fact identity over the source instance, bind triple, run, and
/// segment ordinal. Length-prefixing makes the tuple encoding injective, and the
/// digest is independent of timestamps and usage so a rebuild can reconcile facts.
/// Namespace-backed sessions still hash their storage namespace, not a project placeholder.
fn fact_id(
    source_instance_id: &str,
    session: &BindIdentity,
    run_id: &RunId,
    segment_ordinal: u64,
) -> String {
    let mut input = Vec::new();
    for value in [
        source_instance_id.to_string(),
        session.project_root.to_string_lossy().into_owned(),
        session.harness.clone(),
        session.session.clone(),
        run_id.0.clone(),
    ] {
        append_len_prefixed(&mut input, value.as_bytes());
    }
    input.extend_from_slice(&segment_ordinal.to_le_bytes());
    let digest = Sha256::digest(input);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// The fact for one cache warm, derived from its own record alone: `wal_seq` (the record's WAL
/// sequence) is both its ordinal and the input to its id, so it never depends on, or moves, how
/// many ordinary segments its run has.
// The inputs are the warm's own facts plus the run it bills under; grouping them would only
// rename the same values.
#[allow(clippy::too_many_arguments)]
fn warm_segment<R: ChargeBasisResolver>(
    source_instance_id: &str,
    run_id: &RunId,
    session: &BindIdentity,
    config: &broca_records::record::RunConfig,
    usage: Usage,
    wal_seq: u64,
    sent_at_ms: u64,
    outcome: Option<broca_records::record::BillingAttemptOutcome>,
    resolver: &R,
) -> SpendSegment {
    let selection = config.auth_selection.as_ref();
    let credential_id = selection.map(|selection| selection.credential_id.clone());
    let method = credential_id.as_deref().and_then(credential_id_method);
    let (charge_basis, mapping_version) =
        resolver.resolve_charge_basis(config.billed_provider(), method.unwrap_or(""));
    SpendSegment {
        agent_id: None,
        agent_run_id: None,
        run_id: run_id.clone(),
        session: session.clone(),
        segment_ordinal: wal_seq,
        fact_id: warm_fact_id(source_instance_id, session, run_id, wal_seq),
        provider: config.billed_provider().into(),
        model: config.model.model_id.clone(),
        auth_selection: credential_id,
        account_ref: selection.and_then(|selection| selection.account_ref.clone()),
        resolved_credential_id: selection
            .and_then(|selection| selection.resolved_credential_id.clone()),
        vault_request: selection.and_then(|selection| selection.vault_request.clone()),
        account_decision: selection
            .and_then(|selection| selection.account_decision.as_ref())
            .map(|decision| decision.path.clone()),
        work_class: config.work_class.clone(),
        charge_basis,
        charge_basis_mapping_version: mapping_version,
        charge_basis_overrides: Some(resolver.charge_basis_overrides()),
        usage_policy_version: usage_policy_version(&config.frozen_render_config),
        usage,
        usage_by_service_tier: None,
        retries_used: 0,
        pricing_at_ms: Some(sent_at_ms),
        pricing_at_precision: TimestampPrecision::Exact,
        occurred_at_ms: Some(sent_at_ms),
        occurred_at_precision: TimestampPrecision::Exact,
        terminal_reason: None,
        warm: true,
        warm_outcome: outcome,
        usage_in_attempts: false,
    }
}

/// The id of a cache warm's fact. The encoding adds a tag the ordinary [`fact_id`] never has, so
/// a warm's id can never equal an ordinary segment's, whatever the numbers.
fn warm_fact_id(
    source_instance_id: &str,
    session: &BindIdentity,
    run_id: &RunId,
    wal_seq: u64,
) -> String {
    let mut input = Vec::new();
    for value in [
        source_instance_id.to_string(),
        session.project_root.to_string_lossy().into_owned(),
        session.harness.clone(),
        session.session.clone(),
        run_id.0.clone(),
        "cache_warm".to_string(),
    ] {
        append_len_prefixed(&mut input, value.as_bytes());
    }
    input.extend_from_slice(&wal_seq.to_le_bytes());
    let digest = Sha256::digest(input);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn append_len_prefixed(output: &mut Vec<u8>, value: &[u8]) {
    output.extend_from_slice(&(value.len() as u64).to_le_bytes());
    output.extend_from_slice(value);
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_protocol::{
        request::{GenerationConfig, ModelHandle, ResponseFormat, ToolChoice},
        StepId,
    };
    use broca_records::{record::AuthSelection, record::RunConfig};
    use serde_json::json;

    pub(super) fn resolver(provider: &str, method: &str) -> (ChargeBasis, u32) {
        match (provider, method) {
            ("anthropic", "apikey") => (ChargeBasis::MeteredApi, 1),
            ("anthropic", "oauth") => (ChargeBasis::SubscriptionIncluded, 1),
            _ => (ChargeBasis::Unknown, 1),
        }
    }

    pub(super) fn config(provider: &str, model: &str, credential_id: Option<&str>) -> RunConfig {
        RunConfig {
            global_system: None,
            account_route: None,
            route_decision_id: None,
            served_provider: None,
            account_limit_pause: false,
            turn_directives_v1: None,
            render_tags: false,
            computed_tags: None,
            model: ModelHandle::new(provider, model),
            tools: vec![].into(),
            tool_choice: ToolChoice::Auto,
            generation: GenerationConfig::default(),
            response_format: ResponseFormat::default(),
            provider_options: serde_json::Value::Null,
            cache_tiers: serde_json::Value::Null,
            context_epoch: 0,
            renderer_version: 1,
            frozen_render_config: serde_json::Value::Null,
            auth_selection: credential_id.map(|credential_id| AuthSelection {
                billing_vault_facts: None,
                auth_method: None,
                credential_id: credential_id.into(),
                account_ref: None,
                resolved_credential_id: None,
                vault_request: None,
                account_decision: None,
            }),
            tools_were_omitted: false,
            budget: None,
            context_limit: None,
            work_class: None,
            variant: None,
            service_tier: None,
            stop_when: vec![],
            context_management: None,
        }
    }

    pub(super) fn frame(seq: u64, event: BillingEvent) -> BillingRecord {
        BillingRecord { seq, event }
    }

    pub(super) fn started(seq: u64, run_id: &str, ts_ms: Option<u64>) -> BillingRecord {
        frame(
            seq,
            BillingEvent::RunStarted {
                run_id: RunId(run_id.into()),
                session: BindIdentity::new("/tmp/project", "runner", "session"),
                config: Box::new(config("anthropic", "claude", Some("apikey:anthropic:work"))),
                ts_ms,
                scope: None,
            },
        )
    }

    pub(super) fn step(seq: u64, id: u64, usage: Usage, retries_used: u32) -> BillingRecord {
        frame(
            seq,
            BillingEvent::StepFinished {
                step_id: StepId(id),
                written_at_ms: None,
                usage,
                retries_used,
                service_tier_used: None,
                speed_used: None,
                fallback: Default::default(),
            },
        )
    }

    pub(super) fn usage(input_tokens: u64, output_tokens: u64) -> Usage {
        Usage {
            input_tokens: Some(input_tokens),
            output_tokens: Some(output_tokens),
            ..Usage::default()
        }
    }

    pub(super) fn synthetic_iteration_records(
        models: [&str; 2],
        retries: u32,
    ) -> Vec<BillingRecord> {
        let mut start = started(1, "synthetic-run", Some(100));
        if let BillingEvent::RunStarted {
            session,
            config: frozen,
            ..
        } = &mut start.event
        {
            *session = BindIdentity::new("/synthetic", "capture", "l3");
            **frozen = config(
                "anthropic",
                "claude-opus-5-5",
                Some("oauth:anthropic:synthetic"),
            );
        }
        let mut handoff = step(
            2,
            1,
            Usage {
                input_tokens: Some(2),
                output_tokens: Some(29287),
                cached_input_tokens: Some(20068),
                cache_write_tokens: Some(4),
                reasoning_tokens: Some(23621),
                ..Usage::default()
            },
            retries,
        );
        if let BillingEvent::StepFinished {
            fallback,
            written_at_ms,
            ..
        } = &mut handoff.event
        {
            *written_at_ms = Some(150);
            fallback.served_model = Some(models[1].into());
            fallback.usage_iterations = Some(serde_json::from_value(json!([
                {"model":models[0], "input_tokens":4, "output_tokens":2124, "cache_read_input_tokens":20068, "cache_creation_input_tokens":0},
                {"model":models[1], "input_tokens":2, "output_tokens":29287, "cache_read_input_tokens":20068, "cache_creation_input_tokens":4}
            ])).unwrap());
        }
        let mut end = terminal(3, Some(200));
        if let BillingEvent::RunFinished { retries_used, .. } = &mut end.event {
            *retries_used = retries;
        }
        vec![start, handoff, end]
    }

    #[test]
    fn imported_run_floor_is_incoming_minus_local_started_ids() {
        let local = vec![started(1, "z-local-unfinished", Some(900))];
        let incoming = vec![
            local[0].clone(),
            started(5, "a-imported", Some(1)),
            started(8, "b-imported", None),
            started(9, "a-imported", Some(1000)),
            frame(10, BillingEvent::RunPaused { ts_ms: None }),
        ];
        assert_eq!(
            imported_run_ids(&incoming, &local),
            vec![RunId("a-imported".into()), RunId("b-imported".into())]
        );
        assert!(imported_run_ids(&incoming, &incoming).is_empty());
        assert_eq!(
            imported_run_ids(&local, &[]),
            vec![RunId("z-local-unfinished".into())]
        );
    }

    #[test]
    fn charge_basis_wire_spelling_is_pinned() {
        assert_eq!(
            serde_json::to_string(&ChargeBasis::MeteredApi).unwrap(),
            "\"metered_api\""
        );
        assert_eq!(
            serde_json::to_string(&ChargeBasis::SubscriptionIncluded).unwrap(),
            "\"subscription_included\""
        );
        assert_eq!(
            serde_json::to_string(&ChargeBasis::Unknown).unwrap(),
            "\"unknown\""
        );
    }

    /// A `started` fixture whose frozen `AuthSelection` can carry a vault account_ref (and/or
    /// drop the selection entirely) so the account-labeling tests control exactly what froze.
    fn started_with_account(
        seq: u64,
        run_id: &str,
        ts_ms: Option<u64>,
        credential_id: Option<&str>,
        account_ref: Option<&str>,
    ) -> BillingRecord {
        let mut config = config("anthropic", "claude", credential_id);
        if let Some(selection) = config.auth_selection.as_mut() {
            selection.account_ref = account_ref.map(str::to_string);
        }
        frame(
            seq,
            BillingEvent::RunStarted {
                run_id: RunId(run_id.into()),
                session: BindIdentity::new("/tmp/project", "runner", "session"),
                config: Box::new(config),
                ts_ms,
                scope: None,
            },
        )
    }

    pub(super) fn terminal(seq: u64, ts_ms: Option<u64>) -> BillingRecord {
        frame(
            seq,
            BillingEvent::RunFinished {
                reason: RunFinishReason::Completed,
                ts_ms,
                retries_used: 0,
            },
        )
    }

    #[test]
    fn account_ref_does_not_change_metering_fact_identity() {
        // Two identical runs that differ ONLY in the frozen account_ref must produce the same
        // fact_id: account_ref labels a fact for the consumer but is excluded from identity
        // (fact_id derivation). Mirrors the budget caller-text identity test.
        let with_account = vec![
            started_with_account(
                1,
                "acct",
                Some(100),
                Some("chatgpt:openai"),
                Some("acct-uuid-1"),
            ),
            step(2, 1, usage(3, 1), 0),
            terminal(3, Some(101)),
        ];
        let without_account = vec![
            started_with_account(1, "acct", Some(100), Some("chatgpt:openai"), None),
            step(2, 1, usage(3, 1), 0),
            terminal(3, Some(101)),
        ];
        let with_segments = compute_segments_for_run(
            &with_account,
            &RunId("acct".into()),
            "instance-a",
            &resolver,
        );
        let without_segments = compute_segments_for_run(
            &without_account,
            &RunId("acct".into()),
            "instance-a",
            &resolver,
        );
        assert_eq!(
            with_segments[0].fact_id, without_segments[0].fact_id,
            "account_ref must NOT enter fact_id derivation"
        );
        // The label itself still differs — proving it is stamped, just identity-neutral.
        assert_eq!(with_segments[0].account_ref.as_deref(), Some("acct-uuid-1"));
        assert_eq!(without_segments[0].account_ref, None);
    }

    /// A model step whose provider response reported `tier` (or reported none).
    fn tiered_step(seq: u64, id: u64, usage: Usage, tier: Option<&str>) -> BillingRecord {
        let mut framed = step(seq, id, usage, 0);
        if let BillingEvent::StepFinished {
            service_tier_used, ..
        } = &mut framed.event
        {
            *service_tier_used = tier.map(str::to_owned);
        }
        framed
    }

    /// Three steps at two reported tiers and one step that reported nothing, in one segment.
    fn mixed_tier_run(tiers: [Option<&str>; 4]) -> Vec<BillingRecord> {
        vec![
            started(1, "run", Some(100)),
            tiered_step(2, 1, usage(10, 2), tiers[0]),
            tiered_step(3, 2, usage(20, 4), tiers[1]),
            tiered_step(4, 3, usage(7, 1), tiers[2]),
            tiered_step(5, 4, usage(100, 50), tiers[3]),
            terminal(6, Some(200)),
        ]
    }

    #[test]
    fn segment_splits_usage_by_reported_tier_and_leaves_unreported_steps_out() {
        let records = mixed_tier_run([Some("priority"), Some("default"), Some("priority"), None]);
        let segments =
            compute_segments_for_run(&records, &RunId("run".into()), "instance-a", &resolver);
        assert_eq!(segments.len(), 1, "one (model, auth) slice");
        let segment = &segments[0];
        assert_eq!(
            segment.usage,
            usage(137, 57),
            "the total still counts every step"
        );
        let by_tier = segment
            .usage_by_service_tier
            .as_ref()
            .expect("steps reported a tier, so the segment carries the split");
        assert_eq!(
            by_tier,
            &BTreeMap::from([
                ("default".to_string(), usage(20, 4)),
                ("priority".to_string(), usage(17, 3)),
            ]),
            "each reported tier carries exactly its own steps' usage"
        );
        // The step that reported nothing is in no bucket, so the map falls short of the total
        // by exactly its usage.
        let mut mapped = Usage::default();
        for tier_usage in by_tier.values() {
            mapped += *tier_usage;
        }
        assert_eq!(mapped, usage(37, 7));
        assert_ne!(
            mapped, segment.usage,
            "the map need not sum to the segment total"
        );

        let json = serde_json::to_value(segment).unwrap();
        assert_eq!(
            json["usage_by_service_tier"],
            json!({
                "default": {"input_tokens": 20, "output_tokens": 4},
                "priority": {"input_tokens": 17, "output_tokens": 3},
            })
        );
    }

    /// Export facts are immutable once shipped, and consumers re-read them through `SpendSegment`.
    /// A fact stored before the segment gained any later optional field must read back and
    /// re-serialize to exactly the bytes it was stored with: an optional field that is absent
    /// on a stored fact has to stay absent rather than acquire a default value.
    #[test]
    fn a_stored_fact_reserializes_to_its_stored_bytes() {
        for stored in [
            r#"{"run_id":"run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"segment_ordinal":0,"fact_id":"e1f385e6803bfab6a51fbd03917e2927e6f68a99ea34642b3dbc1fff7ab5bf0e","provider":"anthropic","model":"claude","auth_selection":"chatgpt:openai","account_ref":"acct-9","charge_basis":"unknown","charge_basis_mapping_version":1,"usage":{"input_tokens":10,"output_tokens":2},"retries_used":0,"pricing_at_ms":100,"pricing_at_precision":"exact","occurred_at_ms":200,"occurred_at_precision":"terminal","terminal_reason":"completed"}"#,
            r#"{"run_id":"run-export-fixture","session":{"project_root":"/tmp/export","harness":"runner","session":"metered"},"segment_ordinal":0,"fact_id":"f88b51a02c4a07dc832eaef5c1cf738610301c4935be970a290bdf515d289400","provider":"xai","model":"grok-4","auth_selection":"apikey:xai:work","charge_basis":"metered_api","charge_basis_mapping_version":4,"usage_policy_version":2,"usage":{"input_tokens":10,"cached_input_tokens":0,"cache_write_tokens":0,"output_tokens":2,"reasoning_tokens":0},"retries_used":0,"pricing_at_ms":100,"pricing_at_precision":"exact","occurred_at_ms":200,"occurred_at_precision":"terminal","terminal_reason":"completed"}"#,
        ] {
            let segment: SpendSegment = serde_json::from_str(stored).expect("stored fact parses");
            assert_eq!(serde_json::to_string(&segment).unwrap(), stored);
        }
    }

    /// A fact stored with a recorded vault request, captured from the serializer while
    /// `VaultRequest` was `{ min_ttl_ms, force_refresh: bool }`, before the request gained an
    /// operation name. It must re-serialize to exactly these bytes: a stored fact that predates
    /// a request field keeps that field absent.
    #[test]
    fn a_stored_fact_with_a_vault_request_reserializes_to_its_stored_bytes() {
        const STORED: &str = r#"{"run_id":"run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"segment_ordinal":0,"fact_id":"e1f385e6803bfab6a51fbd03917e2927e6f68a99ea34642b3dbc1fff7ab5bf0e","provider":"anthropic","model":"claude","auth_selection":"chatgpt:openai","account_ref":"acct-9","vault_request":{"min_ttl_ms":600000,"force_refresh":false},"charge_basis":"unknown","charge_basis_mapping_version":1,"charge_basis_overrides":"none","usage":{"input_tokens":10,"output_tokens":2},"retries_used":0,"pricing_at_ms":100,"pricing_at_precision":"exact","occurred_at_ms":200,"occurred_at_precision":"terminal","terminal_reason":"completed"}"#;
        let segment: SpendSegment = serde_json::from_str(STORED).expect("stored fact parses");
        assert!(
            segment.vault_request.is_some(),
            "the pin must carry a vault request, or it proves nothing about one"
        );
        assert_eq!(serde_json::to_string(&segment).unwrap(), STORED);
    }
}
