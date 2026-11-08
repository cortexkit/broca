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

/// Compaction authority and preset state are session-lived. Refreshes may change
/// composition, tools and system text, but never the compaction item itself.
pub fn check_compaction_item(
    current: Option<&PlanManifest>,
    next: &serde_json::Value,
) -> Result<(), crate::Refusal> {
    let current = current
        .map(|manifest| {
            serde_json::from_str::<serde_json::Value>(manifest.plan.as_json_text())
                .expect("frozen plan is JSON")
        })
        .unwrap_or(serde_json::Value::Null);
    let item = |plan: &serde_json::Value| {
        plan.get("compaction_item")
            .filter(|item| !item.is_null())
            .cloned()
            .unwrap_or(serde_json::Value::Null)
    };
    if crate::plan_identity(&item(&current))? != crate::plan_identity(&item(next))? {
        return Err(crate::Refusal::invalid("plan.compaction_item"));
    }
    Ok(())
}

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
    // Sender changes affect admission policy, not the model-facing tools or system text.
    if !tools_changed && !system_changed {
        return None;
    }
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
/// the manifest it will freeze when it applies, held as `M`. Today's fold holds the manifest
/// itself. The new storage's fold holds a reference to it instead: the setup parts the manifest
/// is stored as (its plan, each provider's answer, its tools, its system text and its
/// declarations) and the rest of its members, as a run start references the manifest it froze.
#[derive(Debug, Clone, PartialEq)]
pub struct Pending<M> {
    pub generation: u64,
    pub policy: String,
    pub manifest: M,
}

/// A pending change as today's fold holds it, with its manifest.
pub type PendingState = Pending<Arc<PlanManifest>>;

/// A pending change that applies at a run start: its generation, and the runner role's
/// `applied_by` name for how (`fold` after a prefix rebuild, `append` with the prefix intact).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Applying {
    pub generation: u64,
    pub applied_by: &'static str,
}

/// The key a run's model is compared by when deciding whether the model switched between runs.
pub fn model_key(model: &broca_protocol::ModelHandle) -> String {
    format!("{}:{}", model.provider_module_id, model.model_id)
}

/// Whether a pending change under `policy` applies at a run start on the model keyed `model`, and
/// if it does, its `applied_by` name. `rebuild` and `flushed` say whether a prefix rebuild and a
/// flush happened since the change was accepted, and `last_model` is the previous run's model
/// key: a run on another model is a rebuild too. `immediately` applies at the next run, and
/// `on_prefix_rebuild` once a rebuild happened; a flush applies the change whatever its policy.
/// A policy the session doesn't support never applies, so a record carrying one can't change a
/// run.
pub fn applies_at_run_start(
    policy: &str,
    rebuild: bool,
    flushed: bool,
    last_model: Option<&str>,
    model: &str,
) -> Option<&'static str> {
    let rebuild = rebuild || last_model.is_some_and(|previous| previous != model);
    let applies = flushed
        || match policy {
            "immediately" => true,
            "on_prefix_rebuild" => rebuild,
            _ => false,
        };
    applies.then_some(if rebuild { "fold" } else { "append" })
}

/// The session-change transitions both folds make, over a manifest held as `M`: today's fold
/// over a WAL ([`SessionChangeState::fold`]) and the new storage's operational fold. Each method
/// is one record's effect, so the two folds can't drift apart.
///
/// A run start is two steps. [`Self::run_started`] notes the run's model and manifest, and when
/// the pending change applies at it, [`Self::applied`] applies it after. Whether it applies is
/// decided before either, by [`Self::applying`]. The new storage records that decision in the
/// run start's batch; today's fold makes it again on replay.
#[derive(Debug, Clone, PartialEq)]
pub struct Transitions<M> {
    /// The highest change generation accepted. `0` before any.
    pub accepted_generation: u64,
    /// The highest change generation applied. `0` before any.
    pub applied_generation: u64,
    pub pending: Option<Pending<M>>,
    /// The latest run's frozen manifest.
    pub current: Option<M>,
    /// The manifest of the latest prefix rebuild, or of the first run before any.
    pub prefix: Option<M>,
    /// A real prefix rebuild happened since the pending acceptance: a compaction view applied,
    /// a model switch between runs, or a prefix flush.
    pub rebuild_since_accept: bool,
    /// A flush has not yet reached a new run, including a flush before the acceptance.
    pub flushed_since_accept: bool,
    /// The latest run's model, by [`model_key`].
    pub last_model: Option<String>,
}

impl<M> Default for Transitions<M> {
    fn default() -> Self {
        Self {
            accepted_generation: 0,
            applied_generation: 0,
            pending: None,
            current: None,
            prefix: None,
            rebuild_since_accept: false,
            flushed_since_accept: false,
            last_model: None,
        }
    }
}

impl<M: Clone> Transitions<M> {
    /// A refresh accepted under `generation`. The writer checks generation order where it
    /// writes, so each acceptance supersedes the pending change in log order. A flush before the
    /// acceptance still counts as a rebuild for it.
    pub fn accepted(&mut self, generation: u64, policy: &str, manifest: M) {
        if generation > self.accepted_generation {
            self.accepted_generation = generation;
        }
        self.pending = Some(Pending {
            generation,
            policy: policy.to_owned(),
            manifest,
        });
        self.rebuild_since_accept = self.flushed_since_accept;
    }

    /// The pending change's policy changed, when `generation` is still the pending one.
    pub fn policy_changed(&mut self, generation: u64, policy: &str) {
        if let Some(pending) = self.pending.as_mut() {
            if pending.generation == generation {
                policy.clone_into(&mut pending.policy);
            }
        }
    }

    /// A prefix flush: a real prefix rebuild for the pending change, which the next run applies
    /// whatever its policy, as a fold.
    pub fn flushed(&mut self) {
        self.rebuild_since_accept = true;
        self.flushed_since_accept = true;
    }

    /// A compaction view applied: a real prefix rebuild, whose prefix is the current manifest.
    pub fn view_applied(&mut self) {
        self.rebuild_since_accept = true;
        self.prefix = self.current.clone();
    }

    /// Whether the pending change applies at a run start on the model keyed `model`, and how
    /// ([`applies_at_run_start`]). Asked before the run start's own transitions.
    pub fn applying(&self, model: &str) -> Option<Applying> {
        let pending = self.pending.as_ref()?;
        applies_at_run_start(
            &pending.policy,
            self.rebuild_since_accept,
            self.flushed_since_accept,
            self.last_model.as_deref(),
            model,
        )
        .map(|applied_by| Applying {
            generation: pending.generation,
            applied_by,
        })
    }

    /// A run started on the model keyed `model`, freezing `manifest`. A run on another model
    /// than the previous run is a prefix rebuild whose prefix is this run's manifest. A first
    /// run, or one after a flush, starts a new prefix.
    pub fn run_started(&mut self, model: String, manifest: Option<M>) {
        if let Some(previous) = self.last_model.replace(model.clone()) {
            if previous != model {
                self.rebuild_since_accept = true;
                self.prefix = manifest.clone();
            }
        }
        self.current = manifest;
        if self.prefix.is_none() || self.flushed_since_accept {
            self.prefix = self.current.clone();
        }
        self.flushed_since_accept = false;
    }

    /// The pending change applied at the run start just folded, as `applying` decided before
    /// it. Returns the change it applied; `None`, changing nothing, when no change is pending.
    /// Applied by a rebuild (`fold`), its manifest is the new prefix.
    pub fn applied(&mut self, applying: Applying) -> Option<Pending<M>> {
        let pending = self.pending.take()?;
        self.applied_generation = pending.generation;
        if applying.applied_by == "fold" {
            self.prefix = Some(pending.manifest.clone());
        }
        self.rebuild_since_accept = false;
        self.flushed_since_accept = false;
        Some(pending)
    }
}

/// The latest applied change.
#[derive(Debug, Clone, PartialEq)]
pub struct EffectiveState {
    pub generation: u64,
    /// One of the runner role's `applied_by` names: `fold` when a prefix rebuild applied it,
    /// `append` when the next run applied it with the prefix intact.
    pub applied_by: String,
    pub manifest_digest: String,
    pub composition_digest: Option<String>,
    pub absent_items: Vec<cortexkit_role_llm_runner::baseline::AbsentItem>,
}

impl EffectiveState {
    /// The facts the baseline reports for the change `applying` applied, which froze `manifest`:
    /// the whole manifest's digest, its composition digest when it has one, and the plan items
    /// its fetch left absent.
    pub fn of(manifest: &PlanManifest, applying: Applying) -> Self {
        let facts = crate::baseline_facts(manifest);
        Self {
            generation: applying.generation,
            applied_by: applying.applied_by.into(),
            manifest_digest: crate::plan_identity(
                &serde_json::to_value(manifest).expect("manifest JSON"),
            )
            .expect("canonical manifest"),
            composition_digest: (!facts.composition_digest.is_empty())
                .then_some(facts.composition_digest),
            absent_items: facts.absent_items,
        }
    }
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
    /// change as `fold` when a rebuild applied it and `append` otherwise. The transitions are
    /// [`Transitions`]'s, which the new storage's fold makes too.
    pub fn fold(records: &[FramedRecord]) -> Self {
        let mut state = Self::default();
        let mut change = Transitions::<Arc<PlanManifest>>::default();
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
                    change.accepted(*generation, policy, Arc::new((**manifest).clone()));
                }
                WalRecord::SessionRefreshPolicyChanged {
                    generation,
                    policy,
                    send_id,
                } => {
                    state
                        .policy_send_ids
                        .insert(send_id.clone(), (*generation, policy.clone()));
                    change.policy_changed(*generation, policy);
                }
                WalRecord::SessionPrefixFlushed {
                    send_id,
                    pending_generation,
                } => {
                    state
                        .flush_send_ids
                        .insert(send_id.clone(), *pending_generation);
                    change.flushed();
                }
                WalRecord::CompactionViewApplied { .. }
                | WalRecord::CompactionViewAppliedV2 { .. } => change.view_applied(),
                WalRecord::RunStarted {
                    manifest, config, ..
                } => {
                    let model = model_key(&config.model);
                    let started = manifest.as_deref().cloned().map(Arc::new);
                    // Replay decides again what the writer decided at this run start: the
                    // pending change applies when its policy's trigger came and the run froze
                    // its manifest.
                    let applying = change.applying(&model).filter(|_| {
                        started.as_ref().is_some_and(|started| {
                            change
                                .pending
                                .as_ref()
                                .is_some_and(|pending| pending.manifest == *started)
                        })
                    });
                    change.run_started(model, started);
                    if let Some(applying) = applying {
                        if let Some(pending) = change.applied(applying) {
                            state
                                .applied
                                .insert(applying.generation, applying.applied_by.into());
                            state.effective = Some(EffectiveState::of(&pending.manifest, applying));
                        }
                    }
                }
                _ => {}
            }
        }
        state.accepted_generation = change.accepted_generation;
        state.applied_generation = change.applied_generation;
        state.pending = change.pending;
        state.current_manifest = change.current;
        state.prefix_manifest = change.prefix;
        state.rebuild_pending = change.rebuild_since_accept;
        state.flush_pending = change.flushed_since_accept;
        state.last_model = change.last_model;
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

    #[test]
    fn compaction_provider_preset_and_params_cannot_refresh() {
        let mut current = test_manifest("old", "c");
        let plan = serde_json::json!({"compaction_item":{"provider":"ck-mc","preset":"head","params":{"n":1}},"composition":{"generation":1}});
        current.plan = broca_wal::OpaqueJson::from_json_text(plan.to_string()).unwrap();
        for (field, replacement) in [
            ("provider", serde_json::json!("other")),
            ("preset", serde_json::json!("worker")),
            ("params", serde_json::json!({"n":2})),
        ] {
            let mut next = plan.clone();
            next["compaction_item"][field] = replacement;
            let refusal = check_compaction_item(Some(&current), &next).unwrap_err();
            assert_eq!(refusal.code(), "invalid_params");
            assert_eq!(
                refusal.detail(),
                serde_json::json!({"field":"plan.compaction_item"})
            );
        }
        let mut removed = plan.clone();
        removed.as_object_mut().unwrap().remove("compaction_item");
        assert!(check_compaction_item(Some(&current), &removed).is_err());
        assert!(check_compaction_item(None, &plan).is_err());
    }

    #[test]
    fn compaction_composition_only_refresh_keeps_the_item() {
        let mut current = test_manifest("old", "c");
        let plan = serde_json::json!({"compaction_item":{"provider":"ck-mc","preset":"head","params":{"n":1}},"composition":{"generation":1}});
        current.plan = broca_wal::OpaqueJson::from_json_text(plan.to_string()).unwrap();
        let mut next = plan;
        next["composition"] = serde_json::json!({"generation":2,"other":"changed"});
        next["compaction_item"]["params"]["n"] = serde_json::json!(1.0);
        check_compaction_item(Some(&current), &next).unwrap();
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

    #[test]
    fn sender_only_refresh_never_requires_a_surface_rung() {
        let current = test_manifest("old", "c");
        let mut next = current.clone();
        next.plan = broca_wal::OpaqueJson::from_json_text(
            r#"{"senders":[{"module":"occipital","idle_start":true}]}"#,
        )
        .unwrap();
        for policy in [
            "immediately",
            "on_prefix_rebuild",
            "on_system_rebuild",
            "on_history_rebuild",
        ] {
            assert_eq!(
                unsupported_surface(Some(&current), &next, policy),
                None,
                "{policy}"
            );
        }
        next.system_text = "changed".into();
        assert_eq!(
            unsupported_surface(Some(&current), &next, "on_system_rebuild"),
            Some("system_text")
        );
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
    fn applied_baseline_facts_follow_the_manifest_for_append_and_fold() {
        let mut manifest = test_manifest("plan-b", "applied-composition");
        manifest.plan = broca_wal::OpaqueJson::from_json_text(
            r#"{"tool_items":[{"provider":"optional","optional":true}],"system_text_items":[{"provider":"optional","optional":true}]}"#,
        ).unwrap();
        manifest.fetches.push(broca_wal::ProviderFetch {
            provider: "optional".into(),
            outcome: broca_wal::FetchOutcome::Absent {
                reason: "refused:unavailable".into(),
            },
        });
        for rebuild in [false, true] {
            let mut accept = accepted(1, "immediately", "plan-b");
            if let WalRecord::SessionRefreshAccepted {
                manifest: frozen, ..
            } = &mut accept
            {
                **frozen = manifest.clone();
            }
            let mut applying_run = run_started(Some("plan-b"));
            if let WalRecord::RunStarted {
                manifest: frozen, ..
            } = &mut applying_run
            {
                *frozen = Some(Box::new(manifest.clone()));
            }
            let mut records = vec![framed(run_started(Some("plan-a"))), framed(accept)];
            if rebuild {
                records.push(framed(WalRecord::SessionPrefixFlushed {
                    send_id: "flush".into(),
                    pending_generation: Some(1),
                }));
            }
            records.push(framed(applying_run));
            let state = SessionChangeState::fold(&records);
            let effective = state.effective.as_ref().unwrap();
            assert_eq!(
                effective.applied_by,
                if rebuild { "fold" } else { "append" }
            );
            assert_eq!(
                effective.composition_digest.as_deref(),
                Some("applied-composition")
            );
            assert_eq!(
                serde_json::to_value(&effective.absent_items).unwrap(),
                serde_json::json!([
                    {"provider":"optional","kind":"tool","reason":"refused","provider_code":"unavailable"},
                    {"provider":"optional","kind":"system_text","reason":"refused","provider_code":"unavailable"}
                ])
            );
            assert_eq!(
                effective.absent_items,
                crate::baseline_facts(&manifest).absent_items
            );
            assert_eq!(
                effective.manifest_digest,
                crate::plan_identity(&serde_json::to_value(&manifest).unwrap()).unwrap()
            );
            assert_eq!(
                state.prefix_manifest.as_ref().unwrap().composition_digest,
                if rebuild { "applied-composition" } else { "c" }
            );

            // Later runs must not replace the facts of the last applied refresh.
            records.push(framed(run_started(None)));
            assert_eq!(
                SessionChangeState::fold(&records).effective,
                state.effective
            );
        }
    }

    #[test]
    fn applied_manifest_without_composition_has_no_effective_digest() {
        let mut accept = accepted(1, "immediately", "plan-b");
        if let WalRecord::SessionRefreshAccepted { manifest, .. } = &mut accept {
            manifest.composition_digest.clear();
        }
        let mut run = run_started(Some("plan-b"));
        if let WalRecord::RunStarted { manifest, .. } = &mut run {
            manifest.as_mut().unwrap().composition_digest.clear();
        }
        let state = SessionChangeState::fold(&[framed(accept), framed(run)]);
        let effective = state.effective.unwrap();
        assert!(effective.composition_digest.is_none());
        assert!(effective.absent_items.is_empty());
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

    /// The fold as it was written before its transitions moved into [`Transitions`], kept
    /// verbatim so the shared transitions are checked against it.
    fn fold_before_sharing(records: &[FramedRecord]) -> SessionChangeState {
        let mut state = SessionChangeState::default();
        let mut rebuild_since_accept = false;
        let mut last_model: Option<String> = None;
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
                    rebuild_since_accept = true;
                    flushed_since_accept = true;
                }
                WalRecord::CompactionViewApplied { .. }
                | WalRecord::CompactionViewAppliedV2 { .. } => {
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
                            let facts = crate::baseline_facts(&pending.manifest);
                            state.effective = Some(EffectiveState {
                                generation: pending.generation,
                                applied_by: applied_by.into(),
                                manifest_digest: crate::plan_identity(
                                    &serde_json::to_value(pending.manifest.as_ref())
                                        .expect("manifest JSON"),
                                )
                                .expect("canonical manifest"),
                                composition_digest: (!facts.composition_digest.is_empty())
                                    .then_some(facts.composition_digest),
                                absent_items: facts.absent_items,
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

    /// A `RunStarted` like [`run_started`]'s, on the mock provider's model `model_id`.
    fn run_started_on(identity: Option<&str>, model_id: &str) -> WalRecord {
        let mut record = run_started(identity);
        if let WalRecord::RunStarted { config, .. } = &mut record {
            config.model.model_id = model_id.into();
        }
        record
    }

    /// Every sequence of up to five session-change records, over refreshes of both policies,
    /// policy changes, flushes, applied views and runs on two models that freeze the latest
    /// accepted manifest, an older one or none, folds to the same state through the shared
    /// transitions as through the fold written before them.
    #[test]
    fn shared_transitions_fold_every_short_log_as_before() {
        const SYMBOLS: usize = 9;
        const LONGEST: u32 = 5;
        // The record each symbol stands for once `accepted` refreshes were accepted before it.
        fn record(symbol: usize, accepted: u64) -> WalRecord {
            let latest = format!("plan-{accepted}");
            match symbol {
                0 => accepted_record(accepted + 1, "immediately"),
                1 => accepted_record(accepted + 1, "on_prefix_rebuild"),
                2 => WalRecord::SessionRefreshPolicyChanged {
                    generation: accepted,
                    policy: "immediately".into(),
                    send_id: format!("policy-{accepted}"),
                },
                3 => WalRecord::SessionPrefixFlushed {
                    send_id: format!("flush-{accepted}"),
                    pending_generation: Some(accepted),
                },
                4 => WalRecord::CompactionViewAppliedV2 {
                    step_id: 1,
                    version: 1,
                    checksum: "c".into(),
                },
                5 => run_started_on(Some(&latest), "mock-model"),
                6 => run_started_on(Some(&latest), "other-model"),
                7 => run_started_on(Some("plan-0"), "mock-model"),
                _ => run_started_on(None, "mock-model"),
            }
        }
        fn accepted_record(generation: u64, policy: &str) -> WalRecord {
            accepted(generation, policy, &format!("plan-{generation}"))
        }
        let table: Vec<Vec<FramedRecord>> = (0..=u64::from(LONGEST))
            .map(|accepted| {
                (0..SYMBOLS)
                    .map(|symbol| framed(record(symbol, accepted)))
                    .collect()
            })
            .collect();
        let mut compared = 0;
        for length in 1..=LONGEST {
            for mut code in 0..SYMBOLS.pow(length) {
                let mut accepted = 0;
                let mut records = Vec::new();
                for _ in 0..length {
                    let symbol = code % SYMBOLS;
                    records.push(table[accepted][symbol].clone());
                    if symbol <= 1 {
                        accepted += 1;
                    }
                    code /= SYMBOLS;
                }
                assert_eq!(
                    SessionChangeState::fold(&records),
                    fold_before_sharing(&records),
                    "{:?}",
                    records.iter().map(|r| &r.record).collect::<Vec<_>>()
                );
                compared += 1;
            }
        }
        assert_eq!(
            compared,
            (1..=LONGEST).map(|n| SYMBOLS.pow(n)).sum::<usize>()
        );
    }
}
