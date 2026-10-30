//! The durable session-change state: what a session accepted, applied and still holds pending.
//!
//! A `session.refresh` the runner accepts is durable before its reply, as a
//! `WalRecord::SessionRefreshAccepted` carrying the generation, policy and fetched manifest.
//! A `session.refresh_policy` that changes the pending rung is durable as
//! `WalRecord::SessionRefreshPolicyChanged`, and a `session.flush_prefix` as
//! `WalRecord::SessionPrefixFlushed`. All three require the `session-change/v1`
//! must-understand feature: a reader without it refuses the WAL rather than running the next
//! episode with the wrong tools or answering an identical repeat as stale.
//!
//! This module folds those records with the session's `RunStarted` episodes, its applied
//! compaction views and its model switches into the generation counters the baseline reports.
//! Folding writes nothing; the session actor performs acceptance and its durability barrier.
//! Earlier episodes keep resuming from their own manifests, byte-identical: a refresh applies
//! only when a later `RunStarted` freezes its manifest.
//!
//! A change never applies inside a running run. The earliest it applies is the start of the
//! next run, and the `immediately` rung means from the next run. The `on_prefix_rebuild` rung
//! waits for a real frozen-prefix rebuild after its acceptance: a compaction view applied, a
//! model switch between episodes, or a prefix flush. A new run alone is not a rebuild, because
//! stateless requests still hit the provider's prompt cache across runs.

use std::collections::BTreeMap;
use std::sync::Arc;

use broca_wal::{FramedRecord, PlanManifest, WalRecord};

/// The rungs this runner supports for each surface are: Both surfaces
/// apply at the next `RunStarted` that freezes the pending manifest: `immediately` at the next
/// run, `on_prefix_rebuild` at the next run after a real frozen-prefix rebuild (a compaction
/// view applied, a model switch between episodes, or a prefix flush). A new run alone is not a
/// rebuild. The finer `on_system_rebuild` and `on_history_rebuild` rungs have no separate
/// rebuild broca can detect, so they are refused `rung_unsupported`.
pub const TOOLS_RUNGS: &[&str] = &["on_prefix_rebuild", "immediately"];
/// The system-text surface supports the same two rungs as the tools surface: both are frozen
/// together on the applying `RunStarted`.
pub const SYSTEM_TEXT_RUNGS: &[&str] = &["on_prefix_rebuild", "immediately"];

/// Name the first changed surface that cannot detect the requested rebuild point. Removing
/// all items changes a surface too; testing only nonempty new lists would miss that case.
pub fn unsupported_surface(
    current: Option<&PlanManifest>,
    next: &PlanManifest,
    policy: &str,
) -> Option<&'static str> {
    if TOOLS_RUNGS.contains(&policy) && SYSTEM_TEXT_RUNGS.contains(&policy) {
        return None;
    }
    let tools_changed = current.is_none_or(|old| old.tools != next.tools);
    let system_changed = current.is_none_or(|old| old.system_text != next.system_text);
    Some(if tools_changed || !system_changed {
        "tools"
    } else {
        "system_text"
    })
}

/// A refresh has no model at acceptance. Check its model-bearing items only when a new
/// episode would apply it, using the same rule as first-send admission.
pub fn check_applying_model(
    pending: &PendingState,
    model: &broca_protocol::ModelHandle,
    variant: Option<&str>,
) -> Result<(), crate::Refusal> {
    let value: serde_json::Value =
        serde_json::from_str(pending.manifest.plan.as_json_text()).expect("admitted plan JSON");
    let plan = crate::parse_plan_with(
        &value,
        crate::CompactionSupport::Supported,
        crate::OutputRefsSupport::Supported,
    )?;
    crate::admit::check_plan_model(&plan, &model.provider_module_id, &model.model_id, variant)
        .map_err(|mismatch| crate::Refusal::PendingPlanModelMismatch {
            generation: pending.generation,
            mismatch: Box::new(mismatch),
        })
}

/// A pending change: the highest accepted generation not yet applied, with its latest policy and
/// the manifest it will freeze when it applies.
#[derive(Debug, Clone, PartialEq)]
pub struct PendingState {
    pub generation: u64,
    pub policy: String,
    pub manifest: Arc<PlanManifest>,
}

/// The latest applied change.
#[derive(Debug, Clone, PartialEq)]
pub struct EffectiveState {
    pub generation: u64,
    /// One of the runner role's `applied_by` names: `fold` when a prefix rebuild applied it,
    /// `append` when the next run applied it with the prefix intact.
    pub applied_by: String,
    pub manifest_digest: String,
}

/// The session-change facts a WAL folds, for the baseline and for repeat classification.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SessionChangeState {
    /// The highest change generation the runner accepted. `0` before any.
    pub accepted_generation: u64,
    /// The highest change generation a prefix rebuild or an append applied. `0` before any.
    pub applied_generation: u64,
    /// The pending change and its latest policy, when one is pending.
    pub pending: Option<PendingState>,
    /// The latest applied change, absent while none applied.
    pub effective: Option<EffectiveState>,
    /// Every accepted generation's plan identity and policy, for identical-repeat checks. An
    /// identical repeat (same generation, plan identity and policy) gets the same answer and
    /// fetches nothing; any other generation at or below the accepted one is `generation_stale`.
    pub accepted: BTreeMap<u64, (String, String)>,
    /// The original reply flags, retained even after the change applies or its policy changes.
    pub accepted_replies: BTreeMap<u64, (bool, bool)>,
    /// The `send_id` of every accepted `session.refresh_policy`, with the generation and policy
    /// it recorded, so a repeat answers identically and a reuse with another body is refused
    /// `send_id_reuse` naming the differing field.
    pub policy_send_ids: BTreeMap<String, (u64, String)>,
    /// The `send_id` of every accepted `session.flush_prefix`, with the pending generation it
    /// reported, for the same idempotency.
    pub flush_send_ids: BTreeMap<String, Option<u64>>,
    /// Each applied generation's cause, for policy refusals after later changes.
    pub applied: BTreeMap<u64, String>,
    /// The latest episode's frozen manifest, not the session's admitting manifest.
    pub current_manifest: Option<Arc<PlanManifest>>,
    /// The manifest of the latest prefix rebuild (or the admitting episode before any fold).
    /// Appended changes are reported separately by `effective`.
    pub prefix_manifest: Option<Arc<PlanManifest>>,
    /// A real rebuild has occurred since the pending acceptance.
    pub rebuild_pending: bool,
    /// A flush has not yet reached a new episode, including a flush before acceptance.
    pub flush_pending: bool,
    pub last_model: Option<String>,
}

impl SessionChangeState {
    /// Fold the session's WAL tail into its session-change state. The input is the session's
    /// durable records in WAL order; any other record is skipped. A `RunStarted` whose manifest
    /// freezes the pending manifest applies it: `immediately` at the next run, and
    /// `on_prefix_rebuild` only once a compaction view applied, the model switched between
    /// episodes, or a prefix flush arrived after the acceptance. The applying run records the
    /// change as `fold` when a rebuild applied it and `append` otherwise.
    pub fn fold(records: &[FramedRecord]) -> Self {
        let mut state = Self::default();
        // Whether a real frozen-prefix rebuild happened since the pending acceptance: a
        // compaction view applied, a model switch between episodes, or a prefix flush.
        let mut rebuild_since_accept = false;
        let mut last_model: Option<String> = None;
        // Whether a flush arrived since the pending acceptance, forcing the next run to apply
        // whatever its policy.
        let mut flushed_since_accept = false;
        for framed in records {
            match &framed.record {
                WalRecord::SessionRefreshAccepted {
                    generation,
                    policy,
                    manifest,
                    run_in_progress,
                    applies_from_next_run,
                } => {
                    state
                        .accepted_replies
                        .insert(*generation, (*run_in_progress, *applies_from_next_run));
                    let identity = manifest.plan_identity.clone();
                    state
                        .accepted
                        .insert(*generation, (identity, policy.clone()));
                    if *generation > state.accepted_generation {
                        state.accepted_generation = *generation;
                    }
                    // The actor checks generation ordering at the serialized write point. Each
                    // acceptance therefore supersedes the previous pending change in WAL order.
                    state.pending = Some(PendingState {
                        generation: *generation,
                        policy: policy.clone(),
                        manifest: Arc::new((**manifest).clone()),
                    });
                    rebuild_since_accept = flushed_since_accept;
                }
                WalRecord::SessionRefreshPolicyChanged {
                    generation,
                    policy,
                    send_id,
                } => {
                    state
                        .policy_send_ids
                        .insert(send_id.clone(), (*generation, policy.clone()));
                    if let Some(pending) = state.pending.as_mut() {
                        if pending.generation == *generation {
                            pending.policy.clone_from(policy);
                        }
                    }
                }
                WalRecord::SessionPrefixFlushed {
                    send_id,
                    pending_generation,
                } => {
                    state
                        .flush_send_ids
                        .insert(send_id.clone(), *pending_generation);
                    // A flush is a real prefix rebuild for the pending change: the next run
                    // applies it whatever its policy, and the application counts as a fold.
                    rebuild_since_accept = true;
                    flushed_since_accept = true;
                }
                WalRecord::CompactionViewApplied { .. } => {
                    rebuild_since_accept = true;
                    state.prefix_manifest = state.current_manifest.clone();
                }
                WalRecord::RunStarted {
                    manifest, config, ..
                } => {
                    let model = format!(
                        "{}:{}",
                        config.model.provider_module_id, config.model.model_id
                    );
                    if let Some(previous) = last_model.replace(model.clone()) {
                        if previous != model {
                            rebuild_since_accept = true;
                            state.prefix_manifest = manifest.as_deref().cloned().map(Arc::new);
                        }
                    }
                    if let Some(pending) = state.pending.clone() {
                        let applies = flushed_since_accept
                            || match pending.policy.as_str() {
                                "immediately" => true,
                                "on_prefix_rebuild" => rebuild_since_accept || flushed_since_accept,
                                // The writer never accepts a rung the session does not support, so
                                // replay never meets one here. Treat it as waiting, never as applying,
                                // so a record with an unsupported policy cannot change a running run.
                                _ => false,
                            };
                        let manifest_matches = manifest
                            .as_ref()
                            .is_some_and(|started| started.as_ref() == pending.manifest.as_ref());
                        if applies && manifest_matches {
                            let applied_by = if rebuild_since_accept {
                                "fold"
                            } else {
                                "append"
                            };
                            state.applied_generation = pending.generation;
                            state.applied.insert(pending.generation, applied_by.into());
                            state.effective = Some(EffectiveState {
                                generation: pending.generation,
                                applied_by: applied_by.into(),
                                manifest_digest: crate::plan_identity(
                                    &serde_json::to_value(pending.manifest.as_ref())
                                        .expect("manifest JSON"),
                                )
                                .expect("canonical manifest"),
                            });
                            if applied_by == "fold" {
                                state.prefix_manifest = Some(pending.manifest.clone());
                            }
                            state.pending = None;
                            rebuild_since_accept = false;
                            flushed_since_accept = false;
                        }
                    }
                    state.current_manifest = manifest.as_deref().cloned().map(Arc::new);
                    if state.prefix_manifest.is_none() || flushed_since_accept {
                        state.prefix_manifest = state.current_manifest.clone();
                    }
                    flushed_since_accept = false;
                }
                _ => {}
            }
        }
        state.rebuild_pending = rebuild_since_accept;
        state.flush_pending = flushed_since_accept;
        state.last_model = last_model;
        state
    }

    /// The plan identity and policy accepted under `generation`, if any, for
    /// `session_change::refresh_order`'s repeat check.
    pub fn earlier(&self, generation: u64) -> Option<(&str, &str)> {
        self.accepted
            .get(&generation)
            .map(|(plan, policy)| (plan.as_str(), policy.as_str()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_manifest(plan_identity: &str, composition_digest: &str) -> PlanManifest {
        let plan_text = format!(r#"{{"id":"{plan_identity}"}}"#);
        PlanManifest {
            rich_ui: None,
            plan: broca_wal::OpaqueJson::from_json_text(plan_text).unwrap(),
            plan_identity: plan_identity.into(),
            composition_digest: composition_digest.into(),
            fetches: Vec::new(),
            tools: Arc::new(Vec::new()),
            system_text: String::new(),
            system_digest: "s".into(),
            join_version: 1,
            role_versions: Default::default(),
            declarations: Vec::new(),
        }
    }

    fn accepted(generation: u64, policy: &str, identity: &str) -> WalRecord {
        WalRecord::SessionRefreshAccepted {
            generation,
            policy: policy.into(),
            run_in_progress: true,
            applies_from_next_run: policy == "immediately",
            manifest: Box::new(test_manifest(identity, "c")),
        }
    }

    /// A `RunStarted` freezing `identity` (or none), with a fixed mock model so consecutive
    /// episodes without a switch do not count as a rebuild. Built from JSON so the test does
    /// not repeat every `RunConfig` field.
    fn run_started(identity: Option<&str>) -> WalRecord {
        let manifest = identity.map(|identity| {
            let manifest = test_manifest(identity, "c");
            serde_json::to_value(manifest).unwrap()
        });
        let mut record = serde_json::json!({
            "type": "run_started",
            "run_id": format!("run-{identity:?}"),
            "session": {"project_root": "/tmp", "harness": "h", "session": "s"},
            "config": {
                "model": {"provider_module_id": "mock", "model_id": "mock-model"},
                "tools": [],
                "tool_choice": {"type": "auto"},
                "generation": {},
                "response_format": {"type": "text"},
                "context_epoch": 0,
                "renderer_version": 1
            },
            "input": [],
            "origin": {"kind": "fresh"}
        });
        if let Some(manifest) = manifest {
            record["manifest"] = manifest;
        }
        serde_json::from_value(record).unwrap()
    }

    fn framed(record: WalRecord) -> FramedRecord {
        FramedRecord {
            seq: 0,
            fence: 0,
            record,
        }
    }

    #[test]
    fn accept_before_reply_is_pending() {
        let records = [framed(accepted(1, "immediately", "plan-a"))];
        let state = SessionChangeState::fold(&records);
        assert_eq!(state.accepted_generation, 1);
        assert_eq!(state.applied_generation, 0);
        assert_eq!(state.pending.as_ref().unwrap().generation, 1);
        assert!(state.effective.is_none());
        assert_eq!(state.earlier(1), Some(("plan-a", "immediately")));
        assert_eq!(state.accepted_replies.get(&1), Some(&(true, true)));
    }

    #[test]
    fn original_acceptance_survives_policy_change_and_application() {
        let records = [
            framed(accepted(1, "on_prefix_rebuild", "plan-b")),
            framed(WalRecord::SessionRefreshPolicyChanged {
                generation: 1,
                policy: "immediately".into(),
                send_id: "policy-1".into(),
            }),
            framed(run_started(Some("plan-b"))),
        ];
        let recovered = SessionChangeState::fold(&records);
        assert_eq!(recovered.applied_generation, 1);
        assert_eq!(recovered.earlier(1), Some(("plan-b", "on_prefix_rebuild")));
        assert_eq!(recovered.accepted_replies.get(&1), Some(&(true, false)));
        assert_eq!(
            recovered.policy_send_ids.get("policy-1"),
            Some(&(1, "immediately".into()))
        );
    }

    #[test]
    fn pending_before_apply_survives_an_old_manifest_run() {
        let records = [
            framed(accepted(1, "immediately", "plan-b")),
            framed(run_started(Some("plan-a"))),
        ];
        let state = SessionChangeState::fold(&records);
        assert_eq!(state.accepted_generation, 1);
        assert_eq!(state.applied_generation, 0);
        assert_eq!(state.pending.as_ref().unwrap().generation, 1);
    }

    #[test]
    fn apply_at_run_started_freezes_the_new_manifest() {
        let records = [
            framed(accepted(1, "immediately", "plan-b")),
            framed(run_started(Some("plan-b"))),
        ];
        let state = SessionChangeState::fold(&records);
        assert_eq!(state.accepted_generation, 1);
        assert_eq!(state.applied_generation, 1);
        assert!(state.pending.is_none());
        let effective = state.effective.unwrap();
        assert_eq!(effective.generation, 1);
        assert_eq!(effective.applied_by, "append");
    }

    #[test]
    fn on_prefix_rebuild_waits_for_a_rebuild() {
        let accept = framed(accepted(1, "on_prefix_rebuild", "plan-b"));
        let old_run = framed(run_started(Some("plan-a")));
        let state = SessionChangeState::fold(&[accept.clone(), old_run]);
        assert!(state.pending.is_some());
        assert_eq!(state.applied_generation, 0);

        let flush = framed(WalRecord::SessionPrefixFlushed {
            send_id: "flush-1".into(),
            pending_generation: Some(1),
        });
        let new_run = framed(run_started(Some("plan-b")));
        let state = SessionChangeState::fold(&[
            accept,
            framed(run_started(Some("plan-a"))),
            flush,
            new_run,
        ]);
        assert_eq!(state.applied_generation, 1);
        assert_eq!(state.effective.unwrap().applied_by, "fold");
    }
}
