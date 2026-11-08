//! What a session's fetch plan decides now, read from the store's operational fold
//! (`Operations::plan`) and the setup parts its manifests are stored as: the senders the session
//! admits and the model a send that leaves its model to the session runs on. Both follow the
//! session's latest accepted refresh from its acceptance, before any run applies it, by the
//! rules today's head and ownership summary fold (`broca_core::head::HeadProjection`,
//! `SessionOwnershipV1::effective_senders`). Every reader on the new storage asks here, never
//! the first run's manifest.

use std::sync::Arc;

use broca_session_store::fold::operations::{ManifestRef, Operations};
use cortexkit_role_llm_runner::delivery_order::PlanSender;
use cortexkit_role_llm_runner::head_selection::PlanModel;
use serde_json::Value;

use crate::parts::Parts;

/// The manifest the session's senders come from: the latest accepted refresh's once one was
/// accepted, else the one the session's first run froze. `None` for a session without a plan.
pub fn sender_manifest(
    operations: &Operations,
    parts: &dyn Parts,
) -> Result<Option<Arc<broca_wal::PlanManifest>>, String> {
    if let Some(accepted) = accepted(operations) {
        return stored(accepted, parts).map(Some);
    }
    first_manifest(operations, parts)
}

/// The plan senders the session admits now, each with its grants and templates: what every
/// sender check on the new storage reads.
pub fn senders(operations: &Operations, parts: &dyn Parts) -> Result<Vec<PlanSender>, String> {
    Ok(sender_manifest(operations, parts)?
        .as_deref()
        .map(broca_core::admission::manifest_senders)
        .unwrap_or_default())
}

/// Whether a refresh was accepted, which makes its senders the session's sender policy: from
/// then on a sender its plan doesn't name is refused rather than treated as before.
pub fn refreshed(operations: &Operations) -> bool {
    accepted(operations).is_some()
}

/// The model the session's plan chooses for its next runs: the latest refresh that chooses one,
/// else the first run's plan. `None` when no plan chooses one.
pub fn selection(operations: &Operations, parts: &dyn Parts) -> Result<Option<PlanModel>, String> {
    if let Some(selection) = operations
        .plan
        .as_ref()
        .and_then(|plan| plan.selection.as_ref())
    {
        return serde_json::from_value(selection.clone())
            .map(Some)
            .map_err(|error| format!("a refresh's recorded model choice: {error}"));
    }
    Ok(first_manifest(operations, parts)?
        .as_deref()
        .and_then(broca_core::head::manifest_selection))
}

/// The model `manifest`'s plan chooses, as a refresh records it (`SessionRefresh::selection`):
/// today's reading of the plan's `model` (`broca_core::head::manifest_selection`).
pub fn recorded_selection(manifest: &broca_wal::PlanManifest) -> Option<Value> {
    broca_core::head::manifest_selection(manifest)
        .map(|selection| serde_json::to_value(selection).expect("a plan model encodes"))
}

fn accepted(operations: &Operations) -> Option<&ManifestRef> {
    operations.plan.as_ref()?.accepted.as_ref()
}

fn first_manifest(
    operations: &Operations,
    parts: &dyn Parts,
) -> Result<Option<Arc<broca_wal::PlanManifest>>, String> {
    let Some(first) = operations.owner.first_start.as_ref() else {
        return Ok(None);
    };
    crate::run::frozen_manifest(first, parts)
        .map_err(|error| format!("the session's first run start: {error:?}"))
}

fn stored(
    manifest: &ManifestRef,
    parts: &dyn Parts,
) -> Result<Arc<broca_wal::PlanManifest>, String> {
    crate::run::stored_manifest(manifest, parts)
        .map_err(|error| format!("the accepted refresh's manifest: {error:?}"))
}
