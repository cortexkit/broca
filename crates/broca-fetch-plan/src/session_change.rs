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

use std::sync::Arc;

use broca_wal::PlanManifest;

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

/// How a refresh's plan changes the session's compaction item, compared with the latest plan
/// the session accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionItemChange {
    /// Both name the same item, or neither names one.
    Same,
    /// The refresh names an item where the session had none.
    Added,
    /// The refresh names none where the session had one.
    Removed,
    /// Both name an item, with a different provider, preset or params.
    Changed,
}

/// How `next`'s compaction item compares with `latest`'s: the latest plan the session accepted,
/// which is its pending refresh's when one waits to apply, else the one its latest run froze.
/// Each storage decides which changes it takes; [`check_compaction_item`] is today's.
pub fn compaction_item_change(
    latest: Option<&PlanManifest>,
    next: &serde_json::Value,
) -> Result<CompactionItemChange, crate::Refusal> {
    let latest = latest
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
    let (old, new) = (item(&latest), item(next));
    let same = crate::plan_identity(&old)? == crate::plan_identity(&new)?;
    Ok(match (old.is_null(), new.is_null(), same) {
        (_, _, true) => CompactionItemChange::Same,
        (true, _, false) => CompactionItemChange::Added,
        (false, true, false) => CompactionItemChange::Removed,
        (false, false, false) => CompactionItemChange::Changed,
    })
}

/// The refusal of a refresh that changes or removes the session's compaction item, and on
/// today's storage of one that adds an item: compaction authority and preset state are
/// session-lived.
pub fn compaction_item_refusal() -> crate::Refusal {
    crate::Refusal::invalid("plan.compaction_item")
}

/// Today's storage's rule: compaction authority and preset state are session-lived, so a
/// refresh may change composition, tools and system text, but never the compaction item itself,
/// and can't add one either.
pub fn check_compaction_item(
    latest: Option<&PlanManifest>,
    next: &serde_json::Value,
) -> Result<(), crate::Refusal> {
    match compaction_item_change(latest, next)? {
        CompactionItemChange::Same => Ok(()),
        _ => Err(compaction_item_refusal()),
    }
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

pub use broca_protocol::session_change::{
    applies_at_run_start, model_key, Applying, Pending, Transitions,
};

/// A pending change as today's fold holds it, with its manifest.
pub type PendingState = Pending<Arc<PlanManifest>>;
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

    /// These facts as the new storage records them in the applying run start's batch.
    pub fn applied_change(&self) -> broca_protocol::session_change::AppliedChange {
        broca_protocol::session_change::AppliedChange {
            generation: self.generation,
            applied_by: self.applied_by.clone(),
            manifest_digest: self.manifest_digest.clone(),
            composition_digest: self.composition_digest.clone(),
            absent_items: self
                .absent_items
                .iter()
                .map(|item| serde_json::to_value(item).expect("an absent item encodes"))
                .collect(),
        }
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

    /// The classifier names each change against the latest accepted plan, and today's rule
    /// refuses every one but keeping the item, an added one included, with the same refusal.
    #[test]
    fn a_refresh_keeps_adds_removes_or_changes_the_compaction_item() {
        let item = serde_json::json!({"provider":"ck-mc","preset":"head","params":{"n":1}});
        let with_item = serde_json::json!({"compaction_item": item, "composition": {}});
        let without = serde_json::json!({"compaction_item": null, "composition": {}});
        let mut compacted = test_manifest("old", "c");
        compacted.plan = broca_wal::OpaqueJson::from_json_text(with_item.to_string()).unwrap();
        let mut plain = test_manifest("old", "c");
        plain.plan = broca_wal::OpaqueJson::from_json_text(without.to_string()).unwrap();
        let mut changed = with_item.clone();
        changed["compaction_item"]["preset"] = serde_json::json!("worker");
        for (latest, next, expected) in [
            (Some(&compacted), &with_item, CompactionItemChange::Same),
            (Some(&plain), &without, CompactionItemChange::Same),
            (None, &without, CompactionItemChange::Same),
            (Some(&plain), &with_item, CompactionItemChange::Added),
            (None, &with_item, CompactionItemChange::Added),
            (Some(&compacted), &without, CompactionItemChange::Removed),
            (Some(&compacted), &changed, CompactionItemChange::Changed),
        ] {
            assert_eq!(compaction_item_change(latest, next).unwrap(), expected);
            let checked = check_compaction_item(latest, next);
            if expected == CompactionItemChange::Same {
                checked.unwrap();
            } else {
                let refusal = checked.unwrap_err();
                assert_eq!(refusal.code(), "invalid_params");
                assert_eq!(
                    refusal.detail(),
                    serde_json::json!({"field":"plan.compaction_item"})
                );
                assert_eq!(refusal.detail(), compaction_item_refusal().detail());
            }
        }
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
}
