//! Per-model-step diagnostics in the immutable usage export stream (`kind: "model_step"`).
//!
//! A model step fact describes ONE committed model step of a run: which model answered, what
//! the provider reported for usage and cache, how many HTTP attempts it took, the digest of the
//! request that produced the committed answer, and when it ran. Astrocyte reads these to show
//! live per-step cache behaviour.
//!
//! THESE FACTS ARE NEVER SPEND. Session facts (no `kind`) are the billing record and are derived
//! by [`crate::billing`] from the WAL alone; nothing there reads a model step fact, and a model
//! step fact never replaces, splits or changes a session fact or its `fact_id`. Step facts are
//! diagnostics with a 30-day lifetime ([`MODEL_STEP_FACT_RETENTION_MS`]); session facts are kept
//! forever.
//!
//! The derivation here is pure: [`ModelStepFold`] folds WAL records in order and yields one fact
//! per durable `model_step_finished`, so re-folding the same records always yields the same
//! bytes. Storage, the 30-day guard and the asynchronous writer live in the store and serve
//! crates.
//!
//! No content is carried: no prompt, assistant text, tool argument or tool output.

use std::collections::BTreeMap;

use broca_protocol::{BindIdentity, FinishReason, Usage};
use broca_wal::{FramedRecord, WalRecord};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// How long a model step fact stays in `export_facts`: 30 days after its age basis (see
/// [`FoldedStep::age_basis_ms`]). A periodic sweep deletes it then. Session and System One facts
/// have no expiry and are never touched by that sweep.
pub const MODEL_STEP_FACT_RETENTION_MS: u64 = 30 * 24 * 60 * 60 * 1000;

/// One committed model step, as written to `export_facts.segment_json`.
///
/// Field order here is the serialized order, after the leading `"kind":"model_step"`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename = "model_step")]
pub struct ModelStepFact {
    /// Lowercase hex SHA-256 identity; see [`model_step_fact_id`] for the exact bytes.
    pub fact_id: String,
    /// The session WAL's lineage id in UUID form (the same string `meta.lineage_id` and run
    /// events' `epoch` carry). A WAL keeps its lineage id for life; a replaced or re-imported
    /// WAL gets a new one.
    pub lineage_id: String,
    /// broca's episode id, the same value as a session fact's `run_id`.
    pub run_id: String,
    /// The run's model step number: 1 for the run's first committed step, rising by one per
    /// committed step. A retried or auth-paused step keeps its number, so (`run_id`, `step_id`)
    /// names exactly one committed step.
    pub step_id: u64,
    /// The session reference run events carry (`session_ref`): 32 lowercase hex characters
    /// derived from the bind triple.
    pub session_ref: String,
    /// The complete bind triple, exactly as a session fact's `session`.
    pub session: BindIdentity,
    /// The agent id from the run's scope, as on the run's session facts. Absent when unscoped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    /// The scope's agent run id, as on the run's session facts. Absent when unscoped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_run_id: Option<String>,
    /// The frozen provider module id, as a session fact's `provider`.
    pub provider: String,
    /// The frozen model id, as a session fact's `model`.
    pub model: String,
    /// The reasoning variant the run froze. Absent when the send carried none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
    /// The wire family the run's render config froze (`anthropic_messages`, `openai_responses`,
    /// ...). Absent when the run froze no render config.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wire_family: Option<String>,
    /// This step's provider-reported usage: the same `Usage` object, member names and
    /// absent-versus-zero rule as a session fact's `usage`. A member the provider did not report
    /// is absent, never zero.
    pub usage: Usage,
    /// The provider's finish reason for this step. Absent when it reported none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<FinishReason>,
    /// Provider HTTP attempts this step took, retries included, aborted attempts included, and
    /// attempts made before an auth pause under the same step number included. At least 1.
    pub attempts: u32,
    /// Whether the committed answer came from anything but the step's first attempt.
    pub retried: bool,
    /// Byte length of the rendered request body of the attempt that produced the committed
    /// answer. Absent when the WAL holds no attempt record for the step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_body_len: Option<u64>,
    /// Lowercase hex SHA-256 of that same request body. Absent exactly when the length is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_body_sha256: Option<String>,
    /// Unix ms the step's first attempt was handed to the HTTP client.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at_ms: Option<u64>,
    /// Unix ms the committed attempt decoded its first real-content record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_content_at_ms: Option<u64>,
    /// Unix ms the committed attempt ended.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at_ms: Option<u64>,
    /// The run's frozen context window limit. Absent when the run froze none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_limit: Option<u64>,
    /// The run's frozen prompt-cache policy, summarized. Absent when the run froze none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_policy: Option<ModelStepCachePolicy>,
}

/// The parts of a run's frozen prompt-cache policy that explain its cache numbers, copied
/// verbatim from the frozen render config's `cache` object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelStepCachePolicy {
    /// `markers` (explicit breakpoints, e.g. Anthropic `cache_control`), `hints` (a cache key
    /// sent to the provider) or `none`.
    pub class: String,
    /// The breakpoint lifetime of a `markers` policy: `5m` or `1h`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl: Option<String>,
    /// The most breakpoints a `markers` policy places in one request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_breakpoints: Option<u64>,
    /// The retention a `hints` policy asks the provider for, verbatim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention: Option<String>,
}

/// The fact identity: lowercase hex SHA-256 over, in order, the length-prefixed UTF-8 bytes of
/// `model_step/v1`, `lineage_id` and `run_id`, followed by `step_id` as 8 little-endian bytes.
/// Each length prefix is the byte length as 8 little-endian bytes. It depends on nothing that can
/// change on re-derivation, so writing the same step twice always hits the same row.
pub fn model_step_fact_id(lineage_id: &str, run_id: &str, step_id: u64) -> String {
    let mut input = Vec::new();
    for value in ["model_step/v1", lineage_id, run_id] {
        input.extend_from_slice(&(value.len() as u64).to_le_bytes());
        input.extend_from_slice(value.as_bytes());
    }
    input.extend_from_slice(&step_id.to_le_bytes());
    let digest = Sha256::digest(input);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Who a session's facts belong to; constant for one WAL.
#[derive(Debug, Clone)]
pub struct StepFactIdentity {
    pub lineage_id: String,
    pub session: BindIdentity,
    pub session_ref: String,
}

impl StepFactIdentity {
    pub fn new(lineage_id: impl Into<String>, session: &BindIdentity) -> Self {
        Self {
            lineage_id: lineage_id.into(),
            session: session.clone(),
            session_ref: crate::wal_writer::agent_session_ref(session),
        }
    }
}

/// A fact the fold produced, with the time its 30-day lifetime counts from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoldedStep {
    pub fact: ModelStepFact,
    /// `finished_at_ms` when the WAL recorded it, otherwise the run's `run_started.ts_ms`.
    /// `None` only for a run whose WAL carries neither (pre-metering records); the writer then
    /// dates the fact from when it wrote it.
    pub age_basis_ms: Option<u64>,
}

/// The run a fold is inside: everything a step fact copies from its run's frozen config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FoldRun {
    run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    started_at_ms: Option<u64>,
    provider: String,
    model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    variant: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    wire_family: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    context_limit: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cache_policy: Option<ModelStepCachePolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    agent_run_id: Option<String>,
}

/// The attempts recorded so far for one not-yet-committed step.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct AttemptTally {
    count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    first_sent_at_ms: Option<u64>,
    /// The newest attempt: the one a following `model_step_finished` committed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last: Option<AttemptEvidence>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AttemptEvidence {
    request_body_len: u64,
    request_body_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    first_content_at_ms: Option<u64>,
    finished_at_ms: u64,
}

/// The resumable state of a step-fact derivation over one WAL. It serializes, so a writer can
/// persist it beside the WAL sequence it has folded through and continue from there.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelStepFold {
    /// `None` before the first `run_started` the fold saw, and after a `run_finished`. A step
    /// outside a known run yields no fact: its model and identity are unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    run: Option<FoldRun>,
    /// Attempt records of the open run's uncommitted steps, by step id. Normally at most one
    /// entry; it is cleared when the step commits and when the run starts or ends.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    attempts: BTreeMap<u64, AttemptTally>,
}

impl ModelStepFold {
    /// Fold one record; returns the fact for a committed model step of a known run.
    pub fn apply(
        &mut self,
        frame: &FramedRecord,
        identity: &StepFactIdentity,
    ) -> Option<FoldedStep> {
        match &frame.record {
            WalRecord::RunStarted {
                run_id,
                ts_ms,
                config,
                scope,
                ..
            } => {
                self.attempts.clear();
                self.run = Some(FoldRun {
                    run_id: run_id.0.clone(),
                    started_at_ms: *ts_ms,
                    provider: config.model.provider_module_id.clone(),
                    model: config.model.model_id.clone(),
                    variant: config.variant.clone(),
                    wire_family: wire_family(&config.frozen_render_config),
                    context_limit: config.context_limit,
                    cache_policy: cache_policy(&config.frozen_render_config),
                    agent_id: scope.as_ref().and_then(|scope| scope.agent_id.clone()),
                    agent_run_id: scope.as_ref().and_then(|scope| scope.run_id.clone()),
                });
                None
            }
            // The only change to a run's render config after it starts. Later steps render under
            // the replacement, so they report its family and cache policy.
            WalRecord::RunConfigRefrozen {
                frozen_render_config,
                ..
            } => {
                if let Some(run) = self.run.as_mut() {
                    run.wire_family = wire_family(frozen_render_config);
                    run.cache_policy = cache_policy(frozen_render_config);
                }
                None
            }
            WalRecord::ModelAttemptFinished {
                step_id,
                request_body_len,
                request_body_sha256,
                ts_ms,
                content_progress,
                sent_at_ms,
                ..
            } => {
                if self.run.is_some() {
                    let tally = self.attempts.entry(step_id.0).or_default();
                    tally.count = tally.count.saturating_add(1);
                    if tally.first_sent_at_ms.is_none() {
                        tally.first_sent_at_ms = *sent_at_ms;
                    }
                    tally.last = Some(AttemptEvidence {
                        request_body_len: *request_body_len,
                        request_body_sha256: request_body_sha256.clone(),
                        first_content_at_ms: content_progress.first_content_at_ms,
                        finished_at_ms: *ts_ms,
                    });
                }
                None
            }
            WalRecord::ModelStepFinished {
                step_id,
                usage,
                finish_reason,
                retries_used,
                ..
            } => {
                let run = self.run.as_ref()?;
                let tally = self.attempts.remove(&step_id.0).unwrap_or_default();
                // `retries_used` is the authoritative retry count of the attempt sequence that
                // committed; attempt records also count attempts an auth pause cut short under
                // the same step number, and are absent on WALs older than those records.
                let attempts = tally.count.max(retries_used.saturating_add(1));
                let last = tally.last;
                let finished_at_ms = last.as_ref().map(|attempt| attempt.finished_at_ms);
                Some(FoldedStep {
                    age_basis_ms: finished_at_ms.or(run.started_at_ms),
                    fact: ModelStepFact {
                        fact_id: model_step_fact_id(&identity.lineage_id, &run.run_id, step_id.0),
                        lineage_id: identity.lineage_id.clone(),
                        run_id: run.run_id.clone(),
                        step_id: step_id.0,
                        session_ref: identity.session_ref.clone(),
                        session: identity.session.clone(),
                        agent_id: run.agent_id.clone(),
                        agent_run_id: run.agent_run_id.clone(),
                        provider: run.provider.clone(),
                        model: run.model.clone(),
                        variant: run.variant.clone(),
                        wire_family: run.wire_family.clone(),
                        usage: *usage,
                        finish_reason: *finish_reason,
                        attempts,
                        retried: attempts > 1,
                        request_body_len: last.as_ref().map(|attempt| attempt.request_body_len),
                        request_body_sha256: last
                            .as_ref()
                            .map(|attempt| attempt.request_body_sha256.clone()),
                        started_at_ms: tally.first_sent_at_ms,
                        first_content_at_ms: last
                            .as_ref()
                            .and_then(|attempt| attempt.first_content_at_ms),
                        finished_at_ms,
                        context_limit: run.context_limit,
                        cache_policy: run.cache_policy.clone(),
                    },
                })
            }
            WalRecord::RunFinished { .. } => {
                self.run = None;
                self.attempts.clear();
                None
            }
            _ => None,
        }
    }
}

fn wire_family(frozen_render_config: &serde_json::Value) -> Option<String> {
    frozen_render_config
        .get("family_id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
}

fn cache_policy(frozen_render_config: &serde_json::Value) -> Option<ModelStepCachePolicy> {
    let cache = frozen_render_config.get("cache")?;
    let text = |key: &str| {
        cache
            .get(key)
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
    };
    Some(ModelStepCachePolicy {
        class: text("class")?,
        ttl: text("ttl"),
        max_breakpoints: cache
            .get("max_breakpoints")
            .and_then(serde_json::Value::as_u64),
        retention: text("retention"),
    })
}

#[cfg(test)]
#[path = "model_step_fact_tests.rs"]
mod tests;
