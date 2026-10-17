//! Pure billing-segment derivation from the durable WAL.
//!
//! A segment is opened by a `RunStarted`, `RunConfigRefrozen`, or `RunAuthRefrozen` record
//! and is closed only when the run reaches a durable terminal record. This module
//! deliberately has no clock or filesystem access: rebuilding an export from
//! the same WAL must reproduce the same facts byte-for-byte.

use broca_protocol::{caller::RunFinishReason, BindIdentity, RunId, Usage};
use broca_wal::{FramedRecord, WalRecord};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

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
pub fn imported_run_ids(incoming: &[FramedRecord], local: &[FramedRecord]) -> Vec<RunId> {
    fn started_ids(records: &[FramedRecord]) -> BTreeSet<String> {
        records
            .iter()
            .filter_map(|frame| match &frame.record {
                WalRecord::RunStarted { run_id, .. } => Some(run_id.0.clone()),
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
    /// The complete bind triple that scopes the fact.
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
    pub vault_request: Option<broca_wal::VaultRequest>,
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
    /// `true` only on the fact for one cache warm: a re-send of the run's most recent request
    /// that generated nothing and kept the provider's cached prefix alive, priced like any
    /// request from its own usage. Such a fact is derived from its own WAL record: its
    /// `segment_ordinal` is that record's WAL sequence and its `fact_id` is taken over that
    /// sequence, so it never moves the ids or ordinals of the run's ordinary segments. Absent on
    /// every other fact, which keeps their bytes and ids exactly what they were.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub warm: bool,
}

#[derive(Debug)]
struct OpenRun {
    source_instance_id: String,
    run_id: RunId,
    session: BindIdentity,
    config: broca_wal::RunConfig,
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
}

impl OpenRun {
    fn new(
        source_instance_id: &str,
        run_id: RunId,
        session: BindIdentity,
        config: broca_wal::RunConfig,
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
        }
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
        let (charge_basis, mapping_version) = resolver
            .resolve_charge_basis(&self.config.model.provider_module_id, method.unwrap_or(""));
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
            provider: self.config.model.provider_module_id.clone(),
            model: self.config.model.model_id.clone(),
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
        });
        self.usage = Usage::default();
        self.usage_by_service_tier.clear();
        self.retries_used = 0;
        self.freeze_ts_ms = None;
        self.segment_open = false;
    }

    fn finish<R: ChargeBasisResolver>(mut self, resolver: &R) -> Vec<SpendSegment> {
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
    records: &[FramedRecord],
    source_instance_id: &str,
    resolver: &R,
) -> Vec<SpendSegment> {
    compute_segments_matching(records, None, source_instance_id, resolver)
}

/// Compute billing-homogeneous segments for one target run without constructing
/// segments for the other runs in the same lineage.
pub fn compute_segments_for_run<R: ChargeBasisResolver>(
    records: &[FramedRecord],
    run_id: &RunId,
    source_instance_id: &str,
    resolver: &R,
) -> Vec<SpendSegment> {
    compute_segments_matching(records, Some(run_id), source_instance_id, resolver)
}

fn compute_segments_matching<R: ChargeBasisResolver>(
    records: &[FramedRecord],
    target_run: Option<&RunId>,
    source_instance_id: &str,
    resolver: &R,
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
            broca_wal::RunConfig,
            Option<broca_protocol::SessionScope>,
        ),
    > = std::collections::HashMap::new();
    let mut latest_run: Option<RunId> = None;

    for framed in records {
        match &framed.record {
            WalRecord::RunStarted {
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
                    output.extend(previous.finish(resolver));
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
            WalRecord::RunConfigRefrozen {
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
            WalRecord::RunAuthRefrozen {
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
            WalRecord::ModelStepFinished {
                usage,
                retries_used,
                service_tier_used,
                ..
            } => {
                if let Some(current) = open.as_mut() {
                    current.pause_candidate = None;
                    current.usage += *usage;
                    // Only a step that reported its tier is filed under one; see
                    // `SpendSegment::usage_by_service_tier`.
                    if let Some(tier) = service_tier_used {
                        *current
                            .usage_by_service_tier
                            .entry(tier.clone())
                            .or_default() += *usage;
                    }
                    current.retries_used = current.retries_used.saturating_add(*retries_used);
                }
            }
            WalRecord::RunFinished {
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
                    output.extend(current.finish(resolver));
                }
            }
            WalRecord::RunPaused { ts_ms, .. } => {
                if let Some(current) = open.as_mut() {
                    // A pause can be resumed under the same run id. Defer closing
                    // until the record stream ends so later steps are not stranded
                    // outside a segment; if no later step arrives, this is the final
                    // terminal timestamp for the derived view.
                    current.pause_candidate = Some(*ts_ms);
                }
            }
            WalRecord::CacheWarmed {
                run_id,
                usage,
                sent_at_ms,
                ..
            } => {
                // A warm is billed under the run whose request it re-sent. A warm naming a run
                // this WAL holds no start of has no config to bill it with, and yields no fact.
                if let Some((session, config, scope)) = episodes.get(run_id) {
                    if target_run.is_none_or(|target| target == run_id) {
                        let mut segment = warm_segment(
                            source_instance_id,
                            run_id,
                            session,
                            config,
                            *usage,
                            framed.seq,
                            *sent_at_ms,
                            resolver,
                        );
                        segment.agent_id = scope.as_ref().and_then(|scope| scope.agent_id.clone());
                        segment.agent_run_id =
                            scope.as_ref().and_then(|scope| scope.run_id.clone());
                        output.push(segment);
                    }
                }
            }
            _ => {}
        }
    }

    if let Some(mut current) = open {
        // Without RunFinished there is no durable run-total aggregate to reconcile; preserve
        // only retries derived from completed steps in the dangling segment.
        if let Some(ts_ms) = current.pause_candidate.take() {
            current.terminal_ts_ms = ts_ms;
            current.terminal_reason = Some(RunFinishReason::Interrupted);
        }
        output.extend(current.finish(resolver));
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
    config: &broca_wal::RunConfig,
    usage: Usage,
    wal_seq: u64,
    sent_at_ms: u64,
    resolver: &R,
) -> SpendSegment {
    let selection = config.auth_selection.as_ref();
    let credential_id = selection.map(|selection| selection.credential_id.clone());
    let method = credential_id.as_deref().and_then(credential_id_method);
    let (charge_basis, mapping_version) =
        resolver.resolve_charge_basis(&config.model.provider_module_id, method.unwrap_or(""));
    SpendSegment {
        agent_id: None,
        agent_run_id: None,
        run_id: run_id.clone(),
        session: session.clone(),
        segment_ordinal: wal_seq,
        fact_id: warm_fact_id(source_instance_id, session, run_id, wal_seq),
        provider: config.model.provider_module_id.clone(),
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

/// Exhaustive classification of billing-identity boundaries.  Keeping this as a
/// match over every WAL variant makes adding a new config-bearing variant a compile
/// error until its billing-boundary semantics are consciously classified here.
#[cfg(test)]
fn billing_boundary_kind(record: &WalRecord) -> Option<&'static str> {
    match record {
        WalRecord::RunStarted { .. } => Some("run_started"),
        WalRecord::RunConfigRefrozen { .. } => Some("run_config_refrozen"),
        WalRecord::RunAuthRefrozen { .. } => Some("run_auth_refrozen"),
        WalRecord::ModelStepFinished { .. }
        | WalRecord::StepStarted { .. }
        | WalRecord::ToolBatchStarted { .. }
        | WalRecord::ToolDispatchIntent { .. }
        | WalRecord::ToolNoDispatch { .. }
        | WalRecord::ToolResult { .. }
        | WalRecord::ToolBatchFinished { .. }
        | WalRecord::TurnFinished { .. }
        | WalRecord::BudgetFired { .. }
        | WalRecord::RunFinished { .. }
        | WalRecord::RunPaused { .. }
        | WalRecord::RunCancelRequested { .. }
        | WalRecord::Queued { .. }
        | WalRecord::Steered { .. }
        | WalRecord::Retracted { .. }
        | WalRecord::ApprovalRequested { .. }
        | WalRecord::ApprovalResolved { .. } => None,
        // A record type written by a newer build cannot carry config: the contract on
        // `WalRecord` forbids a skippable record from changing billing identity.
        WalRecord::Unknown => None,
        // This reader release decodes compaction records but does not act on them yet.
        WalRecord::CompactionRequest { .. }
        | WalRecord::CompactionMessage { .. }
        | WalRecord::CompactionAnswer { .. }
        | WalRecord::CompactionWait { .. } => None,
        // Provider attempt evidence is informational only; it never carries config.
        WalRecord::ModelAttemptFinished { .. } => None,
        // A resume's own account of what it found is informational; it never carries config.
        WalRecord::ResumeStarted { .. } => None,
        // A cache warm record carries no config; billing makes it its own fact under the config
        // of the run it names.
        WalRecord::CacheWarmed { .. } => None,
        // Approval state, withdrawals and isolated late execution do not contain a new model
        // configuration. Billing keeps the originating RunStarted's frozen pricing context.
        WalRecord::ToolCallDeferred { .. }
        | WalRecord::ApprovalRecorded { .. }
        | WalRecord::AwaitingApproval { .. }
        | WalRecord::LateApprovalRecorded { .. }
        | WalRecord::LateDispatchIntent { .. }
        | WalRecord::LateResult { .. } => None,
        // An informational record, skipped exactly like `Unknown`.
        WalRecord::InputTokenCapFrozen { .. }
        | WalRecord::InputTokenBudgetExhausted { .. }
        | WalRecord::StructuredOutputResult { .. }
        | WalRecord::QuestionWithdrawalAcknowledged { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_protocol::{
        content::{AssistantMessage, ContentBlock, FinishReason, Message, Role},
        request::{BudgetKind, GenerationConfig, ModelHandle, ResponseFormat, ToolChoice},
        StepId,
    };
    use broca_wal::{AuthSelection, RunConfig, RunOrigin};
    use serde_json::json;
    use std::collections::BTreeSet;

    fn resolver(provider: &str, method: &str) -> (ChargeBasis, u32) {
        match (provider, method) {
            ("anthropic", "apikey") => (ChargeBasis::MeteredApi, 1),
            ("anthropic", "oauth") => (ChargeBasis::SubscriptionIncluded, 1),
            _ => (ChargeBasis::Unknown, 1),
        }
    }

    fn config(provider: &str, model: &str, credential_id: Option<&str>) -> RunConfig {
        RunConfig {
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

    fn frame(seq: u64, record: WalRecord) -> FramedRecord {
        FramedRecord {
            seq,
            fence: 1,
            record,
        }
    }

    fn started(seq: u64, run_id: &str, ts_ms: Option<u64>) -> FramedRecord {
        frame(
            seq,
            WalRecord::RunStarted {
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
                run_id: RunId(run_id.into()),
                session: BindIdentity::new("/tmp/project", "runner", "session"),
                ts_ms,
                config: Box::new(config("anthropic", "claude", Some("apikey:anthropic:work"))),
                input: vec![Message::new(
                    Role::User,
                    vec![ContentBlock::Text {
                        text: "hello".into(),
                    }],
                )]
                .into(),
                origin: RunOrigin::Fresh,
                submission_id: None,
            },
        )
    }

    fn step(seq: u64, id: u64, usage: Usage, retries_used: u32) -> FramedRecord {
        frame(
            seq,
            WalRecord::ModelStepFinished {
                hooks: None,
                step_id: StepId(id),
                assistant_message: (AssistantMessage {
                    message_id: broca_protocol::MessageId(format!("m{id}")),
                    content: vec![ContentBlock::Text { text: "ok".into() }],
                })
                .into(),
                usage,
                finish_reason: Some(FinishReason::Stop),
                retries_used,
                service_tier_used: None,
                speed_used: None,
            },
        )
    }

    fn usage(input_tokens: u64, output_tokens: u64) -> Usage {
        Usage {
            input_tokens: Some(input_tokens),
            output_tokens: Some(output_tokens),
            ..Usage::default()
        }
    }

    #[test]
    fn budget_exhausted_terminal_reason_reaches_export_facts() {
        let records = vec![
            started(1, "run", Some(100)),
            step(2, 1, usage(10, 2), 0),
            frame(
                3,
                WalRecord::RunFinished {
                    reason: RunFinishReason::BudgetExhausted,
                    ts_ms: Some(200),
                    usage: usage(10, 2),
                    error: None,
                    retries_used: 0,
                },
            ),
        ];
        let segments = compute_segments(&records, "instance-a", &resolver);
        assert_eq!(segments.len(), 1);
        assert_eq!(
            serde_json::to_value(&segments[0]).unwrap()["terminal_reason"],
            "budget_exhausted"
        );
    }

    #[test]
    fn imported_run_floor_is_incoming_minus_local_started_ids() {
        let local = vec![started(1, "z-local-unfinished", Some(900))];
        let incoming = vec![
            local[0].clone(),
            started(5, "a-imported", Some(1)),
            started(8, "b-imported", None),
            started(9, "a-imported", Some(1000)),
            frame(10, WalRecord::TurnFinished { step_id: StepId(1) }),
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
    fn partition_sums_independently_derived_run_total_and_covers_refreeze_steps() {
        let records = vec![
            started(1, "run", Some(100)),
            step(2, 1, usage(10, 2), 1),
            frame(
                3,
                WalRecord::RunConfigRefrozen {
                    renderer_version: 2,
                    ts_ms: Some(150),
                    frozen_render_config: json!({"usage": "open_ai_usage_reasoning_separate"}),
                },
            ),
            step(4, 2, usage(20, 3), 2),
            frame(
                5,
                WalRecord::RunFinished {
                    reason: RunFinishReason::Completed,
                    ts_ms: Some(200),
                    usage: usage(30, 5),
                    error: None,
                    retries_used: 3,
                },
            ),
        ];
        let segments = compute_segments(&records, "instance-a", &resolver);

        let independently_derived_total =
            records.iter().fold(Usage::default(), |mut total, record| {
                if let WalRecord::ModelStepFinished { usage, .. } = &record.record {
                    total += *usage;
                }
                total
            });
        let segmented_total = segments
            .iter()
            .fold(Usage::default(), |mut total, segment| {
                total += segment.usage;
                total
            });
        assert_eq!(segments.len(), 2);
        assert_eq!(segmented_total, independently_derived_total);
        assert_eq!(segments[0].usage, usage(10, 2));
        assert_eq!(segments[1].usage, usage(20, 3));
        assert_eq!(segments[0].retries_used, 1);
        assert_eq!(segments[1].retries_used, 2);
        assert_eq!(segments[0].occurred_at_ms, Some(200));
        assert_eq!(segments[1].occurred_at_ms, Some(200));
        assert_eq!(
            segments[1].terminal_reason,
            Some(RunFinishReason::Completed)
        );
        assert_eq!(segments[0].terminal_reason, None);
        assert_eq!(segments[0].usage_policy_version, None);
        assert_eq!(segments[1].usage_policy_version, Some(USAGE_POLICY_VERSION));

        let legacy_records = vec![
            started(10, "legacy-refreeze", None),
            step(11, 1, usage(1, 1), 0),
            frame(
                12,
                WalRecord::RunConfigRefrozen {
                    renderer_version: 2,
                    ts_ms: None,
                    frozen_render_config: json!({"wire": "legacy"}),
                },
            ),
            step(13, 2, usage(1, 1), 0),
            frame(
                14,
                WalRecord::RunFinished {
                    reason: RunFinishReason::Completed,
                    ts_ms: None,
                    usage: Usage::default(),
                    error: None,
                    retries_used: 0,
                },
            ),
        ];
        let legacy_segments = compute_segments(&legacy_records, "instance-a", &resolver);
        assert_eq!(legacy_segments.len(), 2);
        assert_eq!(legacy_segments[0].occurred_at_ms, None);
        assert_eq!(legacy_segments[1].occurred_at_ms, None);
        assert_eq!(legacy_segments[0].usage_policy_version, None);
        assert_eq!(legacy_segments[1].usage_policy_version, None);
    }

    #[test]
    fn budget_caller_text_does_not_change_metering_fact_identity() {
        let first = vec![
            started(1, "budget", Some(100)),
            step(2, 1, usage(3, 1), 0),
            frame(
                3,
                WalRecord::BudgetFired {
                    run_id: RunId("budget".into()),
                    kind: BudgetKind::Nudge,
                    threshold: 1,
                    ordinal: 2,
                    caller_text: Some("first text".into()),
                },
            ),
            frame(
                4,
                WalRecord::RunFinished {
                    reason: RunFinishReason::Completed,
                    ts_ms: Some(101),
                    usage: usage(3, 1),
                    error: None,
                    retries_used: 0,
                },
            ),
        ];
        let mut second = first.clone();
        let WalRecord::BudgetFired { caller_text, .. } = &mut second[2].record else {
            unreachable!("fixture includes a budget firing");
        };
        *caller_text = Some("different replay-only text".into());
        let first_segments =
            compute_segments_for_run(&first, &RunId("budget".into()), "instance-a", &resolver);
        let second_segments =
            compute_segments_for_run(&second, &RunId("budget".into()), "instance-a", &resolver);
        assert_eq!(first_segments, second_segments);
        assert_eq!(first_segments[0].fact_id, second_segments[0].fact_id);
    }

    #[test]
    fn target_run_derivation_is_identical_to_filtering_the_full_lineage() {
        let records = vec![
            started(1, "before", Some(10)),
            step(2, 1, usage(3, 1), 1),
            frame(
                3,
                WalRecord::RunFinished {
                    reason: RunFinishReason::Completed,
                    ts_ms: Some(20),
                    usage: usage(3, 1),
                    error: None,
                    retries_used: 1,
                },
            ),
            started(4, "target", Some(30)),
            step(5, 2, usage(5, 2), 1),
            frame(
                6,
                WalRecord::RunConfigRefrozen {
                    renderer_version: 2,
                    ts_ms: Some(40),
                    frozen_render_config: json!({"wire": "target-refreeze"}),
                },
            ),
            step(7, 3, usage(7, 3), 2),
            frame(
                8,
                WalRecord::RunFinished {
                    reason: RunFinishReason::Completed,
                    ts_ms: Some(50),
                    usage: usage(12, 5),
                    error: None,
                    retries_used: 3,
                },
            ),
            started(9, "after", Some(60)),
            step(10, 4, usage(11, 4), 0),
            frame(
                11,
                WalRecord::RunFinished {
                    reason: RunFinishReason::Completed,
                    ts_ms: Some(70),
                    usage: usage(11, 4),
                    error: None,
                    retries_used: 0,
                },
            ),
        ];
        let target_run = RunId("target".into());
        let filtered: Vec<_> = compute_segments(&records, "instance-a", &resolver)
            .into_iter()
            .filter(|segment| segment.run_id == target_run)
            .collect();

        let targeted = compute_segments_for_run(&records, &target_run, "instance-a", &resolver);

        assert_eq!(targeted, filtered);
        assert_eq!(
            serde_json::to_vec(&targeted).unwrap(),
            serde_json::to_vec(&filtered).unwrap(),
            "targeted derivation preserves serialized fact bytes"
        );
    }

    #[test]
    fn terminal_failure_retries_are_attributed_to_the_final_segment() {
        let segments = compute_segments(
            &[
                started(1, "failed", Some(100)),
                frame(2, WalRecord::StepStarted { step_id: StepId(1) }),
                frame(
                    3,
                    WalRecord::RunFinished {
                        reason: RunFinishReason::Error,
                        ts_ms: Some(200),
                        usage: Usage::default(),
                        error: None,
                        retries_used: 2,
                    },
                ),
            ],
            "instance-a",
            &resolver,
        );

        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].retries_used, 2);
    }

    #[test]
    fn terminal_failure_retry_residue_lands_after_a_refreeze_split() {
        let segments = compute_segments(
            &[
                started(1, "split-failure", Some(100)),
                step(2, 1, usage(10, 2), 1),
                frame(
                    3,
                    WalRecord::RunConfigRefrozen {
                        renderer_version: 2,
                        ts_ms: Some(150),
                        frozen_render_config: json!({"wire": "new"}),
                    },
                ),
                frame(4, WalRecord::StepStarted { step_id: StepId(2) }),
                frame(
                    5,
                    WalRecord::RunFinished {
                        reason: RunFinishReason::Error,
                        ts_ms: Some(200),
                        usage: usage(10, 2),
                        error: None,
                        retries_used: 3,
                    },
                ),
            ],
            "instance-a",
            &resolver,
        );

        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].retries_used, 1);
        assert_eq!(segments[1].retries_used, 2);
    }

    #[test]
    fn successful_run_terminal_aggregate_does_not_double_count_step_retries() {
        let segments = compute_segments(
            &[
                started(1, "successful", Some(100)),
                step(2, 1, usage(10, 2), 2),
                frame(
                    3,
                    WalRecord::RunFinished {
                        reason: RunFinishReason::Completed,
                        ts_ms: Some(200),
                        usage: usage(10, 2),
                        error: None,
                        retries_used: 2,
                    },
                ),
            ],
            "instance-a",
            &resolver,
        );

        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].retries_used, 2);
    }

    #[test]
    fn boundary_list_is_exhaustive_and_closed() {
        let found: BTreeSet<_> = [
            billing_boundary_kind(&started(1, "r", Some(1)).record),
            billing_boundary_kind(
                &frame(
                    2,
                    WalRecord::RunConfigRefrozen {
                        renderer_version: 1,
                        ts_ms: Some(2),
                        frozen_render_config: serde_json::Value::Null,
                    },
                )
                .record,
            ),
            billing_boundary_kind(
                &frame(
                    3,
                    WalRecord::RunAuthRefrozen {
                        ts_ms: Some(3),
                        auth_selection: AuthSelection {
                            credential_id: "chatgpt:openai".into(),
                            account_ref: Some("account".into()),
                            resolved_credential_id: None,
                            vault_request: None,
                            account_decision: None,
                        },
                    },
                )
                .record,
            ),
        ]
        .into_iter()
        .flatten()
        .collect();
        let expected: BTreeSet<_> = ["run_started", "run_config_refrozen", "run_auth_refrozen"]
            .into_iter()
            .collect();
        assert_eq!(found, expected);
        // The exhaustive match above is the pin: a new WAL variant cannot compile
        // until its billing-boundary classification is explicitly chosen.
    }

    #[test]
    fn fact_ids_are_deterministic_and_distinguish_run_and_segment_ordinal() {
        let records = vec![
            started(1, "run", Some(100)),
            frame(
                2,
                WalRecord::RunConfigRefrozen {
                    renderer_version: 1,
                    ts_ms: Some(101),
                    frozen_render_config: serde_json::Value::Null,
                },
            ),
            frame(
                3,
                WalRecord::RunFinished {
                    reason: RunFinishReason::Completed,
                    ts_ms: Some(200),
                    usage: Usage::default(),
                    error: None,
                    retries_used: 0,
                },
            ),
        ];
        let first = compute_segments(&records, "instance-a", &resolver);
        let second = compute_segments(&records, "instance-a", &resolver);
        assert_eq!(first[0].fact_id, second[0].fact_id);
        assert_ne!(first[0].fact_id, first[1].fact_id);

        let other_run = compute_segments(
            &[
                started(1, "other-run", Some(100)),
                frame(
                    2,
                    WalRecord::RunFinished {
                        reason: RunFinishReason::Completed,
                        ts_ms: Some(200),
                        usage: Usage::default(),
                        error: None,
                        retries_used: 0,
                    },
                ),
            ],
            "instance-a",
            &resolver,
        );
        assert_ne!(first[0].fact_id, other_run[0].fact_id);
    }

    #[test]
    fn usage_export_carries_frozen_work_class_and_omits_absent_class() {
        let mut classified = started(1, "classified", Some(100));
        if let WalRecord::RunStarted { config, .. } = &mut classified.record {
            config.work_class = Some("mason".into());
        }
        let records = vec![
            classified,
            frame(
                2,
                WalRecord::RunFinished {
                    reason: RunFinishReason::Completed,
                    ts_ms: Some(200),
                    usage: Usage::default(),
                    error: None,
                    retries_used: 0,
                },
            ),
        ];
        let classified_segment = compute_segments(&records, "instance-a", &resolver);
        assert_eq!(classified_segment[0].work_class.as_deref(), Some("mason"));
        assert_eq!(
            serde_json::to_value(&classified_segment[0])
                .unwrap()
                .get("work_class"),
            Some(&serde_json::json!("mason"))
        );

        let records = vec![
            started(1, "unclassified", Some(100)),
            frame(
                2,
                WalRecord::RunFinished {
                    reason: RunFinishReason::Completed,
                    ts_ms: Some(200),
                    usage: Usage::default(),
                    error: None,
                    retries_used: 0,
                },
            ),
        ];
        let unclassified_segment = compute_segments(&records, "instance-a", &resolver);
        assert_eq!(unclassified_segment[0].work_class, None);
        assert!(
            serde_json::to_value(&unclassified_segment[0])
                .unwrap()
                .get("work_class")
                .is_none(),
            "an absent workload class must not become an empty string"
        );
    }

    #[test]
    fn normal_run_is_one_segment_and_timestamps_prefer_freeze_then_terminal() {
        let exact_records = vec![
            started(1, "exact", Some(100)),
            step(2, 1, usage(1, 1), 0),
            frame(
                3,
                WalRecord::RunFinished {
                    reason: RunFinishReason::Completed,
                    ts_ms: Some(200),
                    usage: Usage::default(),
                    error: None,
                    retries_used: 0,
                },
            ),
        ];
        let exact = compute_segments(&exact_records, "instance-a", &resolver);
        assert_eq!(exact.len(), 1);
        assert_eq!(exact[0].pricing_at_ms, Some(100));
        assert_eq!(exact[0].pricing_at_precision, TimestampPrecision::Exact);
        assert_eq!(exact[0].occurred_at_ms, Some(200));
        assert_eq!(exact[0].occurred_at_precision, TimestampPrecision::Terminal);

        let degraded_records = vec![
            started(1, "legacy", None),
            step(2, 1, usage(1, 1), 0),
            frame(
                3,
                WalRecord::RunFinished {
                    reason: RunFinishReason::Completed,
                    ts_ms: Some(200),
                    usage: Usage::default(),
                    error: None,
                    retries_used: 0,
                },
            ),
        ];
        let degraded = compute_segments(&degraded_records, "instance-a", &resolver);
        assert_eq!(degraded[0].pricing_at_ms, Some(200));
        assert_eq!(
            degraded[0].pricing_at_precision,
            TimestampPrecision::Degraded
        );

        let absent_terminal = vec![
            started(1, "pre-fix", None),
            frame(
                2,
                WalRecord::RunFinished {
                    reason: RunFinishReason::Completed,
                    ts_ms: None,
                    usage: Usage::default(),
                    error: None,
                    retries_used: 0,
                },
            ),
        ];
        let absent = compute_segments(&absent_terminal, "instance-a", &resolver);
        assert_eq!(absent[0].pricing_at_ms, None);
        assert_eq!(absent[0].occurred_at_ms, None);
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
    ) -> FramedRecord {
        let mut config = config("anthropic", "claude", credential_id);
        if let Some(selection) = config.auth_selection.as_mut() {
            selection.account_ref = account_ref.map(str::to_string);
        }
        frame(
            seq,
            WalRecord::RunStarted {
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
                run_id: RunId(run_id.into()),
                session: BindIdentity::new("/tmp/project", "runner", "session"),
                ts_ms,
                config: Box::new(config),
                input: vec![Message::new(
                    Role::User,
                    vec![ContentBlock::Text {
                        text: "hello".into(),
                    }],
                )]
                .into(),
                origin: RunOrigin::Fresh,
                submission_id: None,
            },
        )
    }

    fn terminal(seq: u64, ts_ms: Option<u64>, usage: Usage) -> FramedRecord {
        frame(
            seq,
            WalRecord::RunFinished {
                reason: RunFinishReason::Completed,
                ts_ms,
                usage,
                error: None,
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
            terminal(3, Some(101), usage(3, 1)),
        ];
        let without_account = vec![
            started_with_account(1, "acct", Some(100), Some("chatgpt:openai"), None),
            step(2, 1, usage(3, 1), 0),
            terminal(3, Some(101), usage(3, 1)),
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

    #[test]
    fn segment_stamps_the_frozen_vault_account_verbatim_and_never_the_credential_component() {
        // A run frozen with a vault account_id stamps it onto the segment verbatim.
        let records = vec![
            started_with_account(
                1,
                "run",
                Some(100),
                Some("chatgpt:openai"),
                Some("acct-uuid-42"),
            ),
            step(2, 1, usage(10, 2), 0),
            terminal(3, Some(200), usage(10, 2)),
        ];
        let segments = compute_segments(&records, "instance-a", &resolver);
        assert_eq!(segments.len(), 1);
        assert_eq!(
            segments[0].account_ref.as_deref(),
            Some("acct-uuid-42"),
            "the frozen vault account_id is stamped verbatim"
        );

        // A credential id WITH an account component but NO frozen vault account must NOT label
        // the segment from the credential id — the account component is never a derivation source.
        let legacy = vec![
            started_with_account(1, "run2", Some(100), Some("apikey:anthropic:work"), None),
            step(2, 1, usage(10, 2), 0),
            terminal(3, Some(200), usage(10, 2)),
        ];
        let legacy_segments = compute_segments(&legacy, "instance-a", &resolver);
        assert_eq!(
            legacy_segments[0].account_ref, None,
            "account_ref is never derived from the credential id's account component"
        );
    }

    #[test]
    fn run_without_vault_identity_serializes_no_account_ref_key() {
        // An opencode/mock run has no auth_selection; its serialized fact must OMIT the
        // account_ref key entirely (the consumer maps absent to unlabeled:<provider>), never
        // emit null or an empty string.
        let records = vec![
            started_with_account(1, "run", Some(100), None, None),
            step(2, 1, usage(10, 2), 0),
            terminal(3, Some(200), usage(10, 2)),
        ];
        let segments = compute_segments(&records, "instance-a", &resolver);
        assert_eq!(segments[0].account_ref, None);
        assert_eq!(segments[0].resolved_credential_id, None);
        let json = serde_json::to_value(&segments[0]).expect("serialize segment");
        assert!(json.get("resolved_credential_id").is_none());
        assert_eq!(segments[0].vault_request, None);
        assert!(json.get("vault_request").is_none());
        assert!(
            json.get("account_ref").is_none(),
            "an absent account_ref must be omitted from the serialized fact, got: {json}"
        );

        // A stamped fact carries the key with the verbatim value.
        let stamped = vec![
            started_with_account(1, "run2", Some(100), Some("chatgpt:openai"), Some("acct-9")),
            step(2, 1, usage(1, 1), 0),
            terminal(3, Some(200), usage(1, 1)),
        ];
        let stamped_json =
            serde_json::to_value(&compute_segments(&stamped, "instance-a", &resolver)[0]).unwrap();
        assert_eq!(
            stamped_json
                .get("account_ref")
                .and_then(|value| value.as_str()),
            Some("acct-9")
        );
    }

    /// A model step whose provider response reported `tier` (or reported none).
    fn tiered_step(seq: u64, id: u64, usage: Usage, tier: Option<&str>) -> FramedRecord {
        let mut framed = step(seq, id, usage, 0);
        if let WalRecord::ModelStepFinished {
            service_tier_used, ..
        } = &mut framed.record
        {
            *service_tier_used = tier.map(str::to_owned);
        }
        framed
    }

    /// Three steps at two reported tiers and one step that reported nothing, in one segment.
    fn mixed_tier_run(tiers: [Option<&str>; 4]) -> Vec<FramedRecord> {
        vec![
            started(1, "run", Some(100)),
            tiered_step(2, 1, usage(10, 2), tiers[0]),
            tiered_step(3, 2, usage(20, 4), tiers[1]),
            tiered_step(4, 3, usage(7, 1), tiers[2]),
            tiered_step(5, 4, usage(100, 50), tiers[3]),
            terminal(6, Some(200), usage(137, 57)),
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

    #[test]
    fn a_segment_with_no_reported_tier_keeps_its_fact_bytes_and_identity() {
        let untiered = compute_segments(&mixed_tier_run([None; 4]), "instance-a", &resolver);
        let tiered = compute_segments(
            &mixed_tier_run([Some("priority"), Some("default"), None, None]),
            "instance-a",
            &resolver,
        );
        assert_eq!(untiered[0].usage_by_service_tier, None);
        let untiered_bytes = serde_json::to_string(&untiered[0]).unwrap();
        assert!(
            !untiered_bytes.contains("usage_by_service_tier"),
            "no reporting step, no key: {untiered_bytes}"
        );
        // Reporting a tier adds exactly the one key and changes nothing else, identity included.
        assert_eq!(tiered[0].fact_id, untiered[0].fact_id);
        let mut tiered_json = serde_json::to_value(&tiered[0]).unwrap();
        tiered_json
            .as_object_mut()
            .unwrap()
            .remove("usage_by_service_tier")
            .expect("the tiered fact carries the split");
        assert_eq!(
            tiered_json,
            serde_json::from_str::<serde_json::Value>(&untiered_bytes).unwrap()
        );
    }

    #[test]
    fn agent_run_export_attribution_preserves_ordinary_bytes_and_fact_identity() {
        let mut records = vec![
            started_with_account(1, "run", Some(100), Some("chatgpt:openai"), Some("acct-9")),
            step(2, 1, usage(10, 2), 0),
            terminal(3, Some(200), usage(10, 2)),
        ];
        let ordinary = compute_segments(&records, "instance-a", &resolver);
        const PINNED: &str = r#"{"run_id":"run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"segment_ordinal":0,"fact_id":"e1f385e6803bfab6a51fbd03917e2927e6f68a99ea34642b3dbc1fff7ab5bf0e","provider":"anthropic","model":"claude","auth_selection":"chatgpt:openai","account_ref":"acct-9","charge_basis":"unknown","charge_basis_mapping_version":1,"charge_basis_overrides":"none","usage":{"input_tokens":10,"output_tokens":2},"retries_used":0,"pricing_at_ms":100,"pricing_at_precision":"exact","occurred_at_ms":200,"occurred_at_precision":"terminal","terminal_reason":"completed"}"#;
        assert_eq!(serde_json::to_string(&ordinary[0]).unwrap(), PINNED);
        assert_eq!(ordinary[0].agent_id, None);
        assert_eq!(ordinary[0].agent_run_id, None);
        if let WalRecord::RunStarted { scope, .. } = &mut records[0].record {
            *scope = Some(serde_json::from_str(r#"{"owner":{"kind":"reserved","module_id":"prefrontal-core"},"ref":"parent-run","scope_epoch":1,"sender":{"kind":"reserved","module_id":"basal"},"agent_id":"agent-1","run_id":"parent-run"}"#).unwrap());
        }
        let scoped = compute_segments(&records, "instance-a", &resolver);
        assert_eq!(scoped[0].agent_id.as_deref(), Some("agent-1"));
        assert_eq!(scoped[0].agent_run_id.as_deref(), Some("parent-run"));
        assert_eq!(scoped[0].run_id.0, "run");
        assert_eq!(scoped[0].fact_id, ordinary[0].fact_id);
        let encoded = serde_json::to_string(&scoped[0]).unwrap();
        assert_eq!(
            encoded.replace(r#","agent_id":"agent-1","agent_run_id":"parent-run""#, ""),
            PINNED
        );
        records.push(warmed(4, "run", usage(5, 0), 250));
        let facts = compute_segments(&records, "instance-a", &resolver);
        assert_eq!(facts.len(), 2);
        assert!(facts[1].warm);
        assert_eq!(facts[1].agent_id.as_deref(), Some("agent-1"));
        assert_eq!(facts[1].agent_run_id.as_deref(), Some("parent-run"));
    }

    #[test]
    fn pre_vault_request_export_fact_bytes_are_unchanged() {
        let records = vec![
            started_with_account(1, "run", Some(100), Some("chatgpt:openai"), Some("acct-9")),
            step(2, 1, usage(10, 2), 0),
            terminal(3, Some(200), usage(10, 2)),
        ];
        let segments = compute_segments(&records, "instance-a", &resolver);
        // Captured from the serializer before export facts carried the vault request
        // parameters. A run that made no vault request must keep exactly these bytes, except
        // for `charge_basis_overrides`, which every newly derived fact carries by design (here
        // "none": the test resolver has no override rows). Stored facts without that key keep
        // their bytes; `a_stored_fact_reserializes_to_its_stored_bytes` pins that.
        const LEGACY: &str = r#"{"run_id":"run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"segment_ordinal":0,"fact_id":"e1f385e6803bfab6a51fbd03917e2927e6f68a99ea34642b3dbc1fff7ab5bf0e","provider":"anthropic","model":"claude","auth_selection":"chatgpt:openai","account_ref":"acct-9","charge_basis":"unknown","charge_basis_mapping_version":1,"charge_basis_overrides":"none","usage":{"input_tokens":10,"output_tokens":2},"retries_used":0,"pricing_at_ms":100,"pricing_at_precision":"exact","occurred_at_ms":200,"occurred_at_precision":"terminal","terminal_reason":"completed"}"#;
        assert_eq!(serde_json::to_string(&segments[0]).unwrap(), LEGACY);
    }

    /// The facts a run with provider attempt evidence exports, captured before a session could
    /// record anything about its prompt cache between requests. Informational records leave the
    /// facts alone, and every fact keeps exactly these bytes and ids.
    #[test]
    fn pre_cache_warm_export_fact_bytes_are_pinned() {
        let attempt = |seq: u64, step: u64| {
            frame(
                seq,
                WalRecord::ModelAttemptFinished {
                    content_progress: Default::default(),
                    step_id: StepId(step),
                    attempt: 0,
                    request_body_len: 10,
                    request_body_sha256: "aa".into(),
                    http_status: Some(200),
                    content_type: None,
                    response_body_len: 5,
                    response_body_sha256: "bb".into(),
                    response_body_cap: None,
                    outcome: broca_protocol::AttemptOutcome::Complete,
                    ts_ms: 150,
                    sent_at_ms: None,
                    transport: None,
                    websocket_request: None,
                    websocket_close_code: None,
                    input_transformations: None,
                    credential_refresh: None,
                    served_account_ref: None,
                },
            )
        };
        let records = vec![
            started_with_account(
                1,
                "run",
                Some(100),
                Some("apikey:anthropic:work"),
                Some("acct-1"),
            ),
            attempt(2, 1),
            step(3, 1, usage(10, 2), 0),
            attempt(4, 2),
            step(5, 2, usage(30, 4), 0),
            terminal(6, Some(200), usage(40, 6)),
        ];
        let segments = compute_segments(&records, "instance-a", &resolver);
        const PINNED: &str = r#"[{"run_id":"run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"segment_ordinal":0,"fact_id":"e1f385e6803bfab6a51fbd03917e2927e6f68a99ea34642b3dbc1fff7ab5bf0e","provider":"anthropic","model":"claude","auth_selection":"apikey:anthropic:work","account_ref":"acct-1","charge_basis":"metered_api","charge_basis_mapping_version":1,"charge_basis_overrides":"none","usage":{"input_tokens":40,"output_tokens":6},"retries_used":0,"pricing_at_ms":100,"pricing_at_precision":"exact","occurred_at_ms":200,"occurred_at_precision":"terminal","terminal_reason":"completed"}]"#;
        assert_eq!(serde_json::to_string(&segments).unwrap(), PINNED);
    }

    fn warmed(seq: u64, run_id: &str, usage: Usage, sent_at_ms: u64) -> FramedRecord {
        frame(
            seq,
            WalRecord::CacheWarmed {
                trigger: None,
                run_id: RunId(run_id.into()),
                step_id: StepId(2),
                request_body_sha256: "aa".into(),
                warm_body_sha256: "bb".into(),
                usage,
                served_account_ref: Some("acct-1".into()),
                ratelimit_headers: Default::default(),
                sent_at_ms,
                ttl_ms: Some(300_000),
                expires_at_ms: Some(sent_at_ms + 300_000),
            },
        )
    }

    /// A warm is its own fact, marked `warm`, priced from its own usage and dated by its own
    /// send, and it leaves the run's ordinary facts byte-identical: their ids and ordinals do not
    /// depend on how many warms the run had.
    #[test]
    fn a_warm_is_its_own_fact_and_leaves_ordinary_facts_unchanged() {
        let base = vec![
            started_with_account(
                1,
                "run",
                Some(100),
                Some("apikey:anthropic:work"),
                Some("acct-1"),
            ),
            step(2, 1, usage(10, 2), 0),
            terminal(3, Some(200), usage(10, 2)),
        ];
        let warm_usage = Usage {
            input_tokens: Some(3),
            cached_input_tokens: Some(4_000),
            cache_write_tokens: Some(0),
            ..Usage::default()
        };
        let mut with_warms = base.clone();
        with_warms.push(warmed(9, "run", warm_usage, 500));
        with_warms.push(warmed(12, "run", Usage::default(), 800));

        let ordinary = compute_segments(&base, "instance-a", &resolver);
        let all = compute_segments(&with_warms, "instance-a", &resolver);
        assert_eq!(all.len(), ordinary.len() + 2);
        assert_eq!(&all[..ordinary.len()], &ordinary[..]);
        for segment in &ordinary {
            assert!(!serde_json::to_string(segment).unwrap().contains("warm"));
        }

        let warm = &all[ordinary.len()];
        assert!(warm.warm);
        assert_eq!(warm.segment_ordinal, 9);
        assert_eq!(warm.usage, warm_usage);
        assert_eq!(warm.occurred_at_ms, Some(500));
        assert_eq!(warm.terminal_reason, None);
        assert_eq!(warm.account_ref.as_deref(), Some("acct-1"));
        assert_eq!(warm.charge_basis, ChargeBasis::MeteredApi);
        assert_ne!(warm.fact_id, all[ordinary.len() + 1].fact_id);
        assert!(!ordinary
            .iter()
            .any(|segment| segment.fact_id == warm.fact_id));
        // Derived from the record alone: rebuilding, or targeting the run, gives the same fact.
        assert_eq!(
            compute_segments_for_run(&with_warms, &RunId("run".into()), "instance-a", &resolver),
            all
        );
        // Usage the provider did not report stays absent on the fact.
        let unreported = serde_json::to_value(&all[ordinary.len() + 1]).unwrap();
        assert_eq!(unreported["usage"], json!({}));
        assert_eq!(unreported["warm"], json!(true));
    }

    /// A warm is billed under the run it names, even when its record lands after a newer episode
    /// started; a targeted derivation for another run never picks it up, and a warm naming a run
    /// the WAL never started yields no fact.
    #[test]
    fn a_warm_is_billed_only_under_the_run_it_names() {
        let records = vec![
            started(1, "first", Some(100)),
            step(2, 1, usage(10, 2), 0),
            terminal(3, Some(200), usage(10, 2)),
            warmed(4, "first", usage(1, 0), 300),
            started(5, "second", Some(400)),
            warmed(6, "first", usage(1, 0), 500),
            warmed(7, "never-started", usage(1, 0), 600),
        ];
        let second =
            compute_segments_for_run(&records, &RunId("second".into()), "instance-a", &resolver);
        assert!(second.iter().all(|segment| !segment.warm));
        let first =
            compute_segments_for_run(&records, &RunId("first".into()), "instance-a", &resolver);
        assert_eq!(first.iter().filter(|segment| segment.warm).count(), 2);
        let all = compute_segments(&records, "instance-a", &resolver);
        assert_eq!(all.iter().filter(|segment| segment.warm).count(), 2);
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

    /// A fact derived from a run whose selection carries every field this build records
    /// (account and a vault request with its operation name). Captured from the serializer
    /// before facts carried how the account was chosen: a run that does not say keeps exactly
    /// these bytes.
    #[test]
    fn pre_account_decision_export_fact_bytes_are_unchanged() {
        let mut started = started_with_account(
            1,
            "run",
            Some(100),
            Some("oauth:anthropic:work"),
            Some("acct-9"),
        );
        if let WalRecord::RunStarted { config, .. } = &mut started.record {
            config.auth_selection.as_mut().unwrap().vault_request = Some(broca_wal::VaultRequest {
                min_ttl_ms: Some(600_000),
                force_refresh: None,
                op: Some(broca_wal::VaultReadOp::GetScoped),
            });
        }
        let records = vec![
            started,
            step(2, 1, usage(10, 2), 0),
            terminal(3, Some(200), usage(10, 2)),
        ];
        let segments = compute_segments(&records, "instance-a", &resolver);
        const LEGACY: &str = r#"{"run_id":"run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"segment_ordinal":0,"fact_id":"e1f385e6803bfab6a51fbd03917e2927e6f68a99ea34642b3dbc1fff7ab5bf0e","provider":"anthropic","model":"claude","auth_selection":"oauth:anthropic:work","account_ref":"acct-9","vault_request":{"min_ttl_ms":600000,"op":"get_scoped"},"charge_basis":"subscription_included","charge_basis_mapping_version":1,"charge_basis_overrides":"none","usage":{"input_tokens":10,"output_tokens":2},"retries_used":0,"pricing_at_ms":100,"pricing_at_precision":"exact","occurred_at_ms":200,"occurred_at_precision":"terminal","terminal_reason":"completed"}"#;
        assert_eq!(serde_json::to_string(&segments[0]).unwrap(), LEGACY);
        let stored: SpendSegment = serde_json::from_str(LEGACY).unwrap();
        assert_eq!(serde_json::to_string(&stored).unwrap(), LEGACY);
    }

    /// A run whose selection recorded how its account was chosen exports that path as one more
    /// key and changes nothing else, `fact_id` included.
    #[test]
    fn the_account_decision_path_rides_to_the_fact_and_nothing_else_moves() {
        let plain = vec![
            started_with_account(
                1,
                "run",
                Some(100),
                Some("oauth:anthropic:work"),
                Some("acct-9"),
            ),
            step(2, 1, usage(10, 2), 0),
            terminal(3, Some(200), usage(10, 2)),
        ];
        let mut decided = plain.clone();
        if let WalRecord::RunStarted { config, .. } = &mut decided[0].record {
            config.auth_selection.as_mut().unwrap().account_decision =
                Some(broca_wal::AccountDecision {
                    conversation_key: None,
                    conversation_key_source: None,
                    path: broca_wal::ACCOUNT_DECISION_DISABLED.into(),
                    admission_id: Some("run".into()),
                    decision_id: None,
                });
        }
        let plain = compute_segments(&plain, "instance-a", &resolver);
        let decided = compute_segments(&decided, "instance-a", &resolver);
        assert_eq!(decided[0].account_decision.as_deref(), Some("disabled"));
        assert_eq!(decided[0].fact_id, plain[0].fact_id);
        let mut decided_json = serde_json::to_value(&decided[0]).unwrap();
        assert_eq!(
            decided_json
                .as_object_mut()
                .unwrap()
                .remove("account_decision"),
            Some(serde_json::json!("disabled"))
        );
        assert_eq!(decided_json, serde_json::to_value(&plain[0]).unwrap());
    }

    /// A warm of a run whose account was chosen by routing carries that decision path as well as
    /// `warm`: both optional keys sit on the same fact, and an ordinary fact still has neither
    /// `warm` nor any byte it did not have before.
    #[test]
    fn a_warm_fact_carries_the_account_decision_beside_warm() {
        let mut records = vec![
            started_with_account(
                1,
                "run",
                Some(100),
                Some("oauth:anthropic:work"),
                Some("acct-9"),
            ),
            step(2, 1, usage(10, 2), 0),
            terminal(3, Some(200), usage(10, 2)),
            warmed(4, "run", usage(1, 0), 300),
        ];
        if let WalRecord::RunStarted { config, .. } = &mut records[0].record {
            config.auth_selection.as_mut().unwrap().account_decision =
                Some(broca_wal::AccountDecision {
                    path: broca_wal::ACCOUNT_DECISION_DISABLED.into(),
                    admission_id: Some("run".into()),
                    decision_id: None,
                    conversation_key: None,
                    conversation_key_source: None,
                });
        }
        let segments = compute_segments(&records, "instance-a", &resolver);
        assert_eq!(segments.len(), 2);
        let ordinary = serde_json::to_value(&segments[0]).unwrap();
        assert_eq!(ordinary["account_decision"], json!("disabled"));
        assert!(ordinary.get("warm").is_none());
        let warm = serde_json::to_value(&segments[1]).unwrap();
        assert_eq!(warm["account_decision"], json!("disabled"));
        assert_eq!(warm["warm"], json!(true));
    }

    #[test]
    fn auth_refreeze_splits_spend_without_relabeling_the_earlier_segment() {
        let records = vec![
            started_with_account(1, "same-run", Some(100), Some("chatgpt:openai"), None),
            step(2, 1, usage(10, 2), 1),
            frame(
                3,
                WalRecord::RunAuthRefrozen {
                    ts_ms: Some(150),
                    auth_selection: AuthSelection {
                        credential_id: "chatgpt:openai".into(),
                        account_ref: Some("acct-resolved".into()),
                        resolved_credential_id: None,
                        vault_request: None,
                        account_decision: None,
                    },
                },
            ),
            step(4, 2, usage(20, 3), 2),
            terminal(5, Some(200), usage(30, 5)),
        ];

        let segments = compute_segments(&records, "instance-a", &resolver);

        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].usage, usage(10, 2));
        assert_eq!(segments[0].retries_used, 1);
        assert_eq!(segments[0].account_ref, None);
        assert_eq!(segments[1].usage, usage(20, 3));
        assert_eq!(segments[1].retries_used, 2);
        assert_eq!(segments[1].account_ref.as_deref(), Some("acct-resolved"));
        assert_eq!(segments[1].pricing_at_ms, Some(150));
        assert_eq!(segments[0].terminal_reason, None);
        assert_eq!(
            segments[1].terminal_reason,
            Some(RunFinishReason::Completed)
        );
        assert_ne!(segments[0].fact_id, segments[1].fact_id);
    }

    #[test]
    fn refreeze_keeps_the_frozen_account_and_a_reauth_episode_stamps_a_fresh_one() {
        // Within a run, a refreeze closes a segment but the run's frozen account_ref rides BOTH
        // segments (a mid-run refreeze does not re-resolve the account).
        let records = vec![
            started_with_account(
                1,
                "run",
                Some(100),
                Some("chatgpt:openai"),
                Some("acct-admission"),
            ),
            step(2, 1, usage(10, 2), 0),
            frame(
                3,
                WalRecord::RunConfigRefrozen {
                    renderer_version: 2,
                    ts_ms: Some(150),
                    frozen_render_config: json!({"wire": "new"}),
                },
            ),
            step(4, 2, usage(20, 3), 0),
            terminal(5, Some(200), usage(30, 5)),
        ];
        let segments = compute_segments(&records, "instance-a", &resolver);
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].account_ref.as_deref(), Some("acct-admission"));
        assert_eq!(
            segments[1].account_ref.as_deref(),
            Some("acct-admission"),
            "a mid-run refreeze keeps the account frozen at admission"
        );

        // A reauth continuation that starts a FRESH episode re-resolves credentials and freezes
        // the FRESH account_id; its segment carries the new account while the earlier episode
        // keeps its own.
        let lineage = vec![
            started_with_account(
                1,
                "run-1",
                Some(100),
                Some("chatgpt:openai"),
                Some("acct-old"),
            ),
            step(2, 1, usage(1, 1), 0),
            terminal(3, Some(110), usage(1, 1)),
            started_with_account(
                4,
                "run-2",
                Some(200),
                Some("chatgpt:openai"),
                Some("acct-fresh"),
            ),
            step(5, 1, usage(2, 2), 0),
            terminal(6, Some(210), usage(2, 2)),
        ];
        let first =
            compute_segments_for_run(&lineage, &RunId("run-1".into()), "instance-a", &resolver);
        let second =
            compute_segments_for_run(&lineage, &RunId("run-2".into()), "instance-a", &resolver);
        assert_eq!(first[0].account_ref.as_deref(), Some("acct-old"));
        assert_eq!(
            second[0].account_ref.as_deref(),
            Some("acct-fresh"),
            "a reauth fresh episode stamps the freshly resolved account_id"
        );
    }
}
