//! Compaction on the current contract: what a send's calls to the compaction provider read from
//! the session's state. Those calls are Setup, notices of new entries, and rides (a call made just
//! before a request is sent, asking for a newer view when something changed, such as the model
//! or the cache expiring).
//!
//! The decisions are today's (`broca_core::compaction_v2`). Today's loop finds their inputs by
//! folding the whole log on every step; here they come from the operational fold, which keeps
//! each as a latest value (see `broca_session_store::fold::operations::Operations`).

use broca_core::compaction::CompactionHistory;
use broca_session_store::fold::operations::Estimate;

/// The request-size history compaction's estimate reads, rebuilt from the fold's latest send
/// facts the way today's `CompactionHistory` folds them: the newest step's usage, measured for
/// the model that served it, and the request body lengths around it.
pub fn history(estimate: &Estimate) -> CompactionHistory {
    let mut history = CompactionHistory {
        current_model: estimate.run_model.clone(),
        current_request_bytes: estimate
            .current_request_bytes
            .and_then(|bytes| usize::try_from(bytes).ok()),
        previous_request_bytes: estimate
            .previous_request_bytes
            .and_then(|bytes| usize::try_from(bytes).ok()),
        ..CompactionHistory::default()
    };
    if !estimate.stepped {
        return history;
    }
    let fallback: broca_wal::record::StepFallback = estimate
        .step_fallback
        .clone()
        .and_then(|value| serde_json::from_value(value).ok())
        .unwrap_or_default();
    let requested = fallback
        .route
        .as_ref()
        .map(|route| &route.model)
        .or(estimate.step_model.as_ref());
    // One prompt measurement, not the sum of prompts sent to every model that served a
    // server-side handoff, as today's history keeps it.
    let native = requested.and_then(|requested| {
        broca_protocol::usage::model_usage(
            fallback
                .served_model
                .as_deref()
                .unwrap_or(&requested.model_id),
            estimate.step_usage.unwrap_or_default(),
            fallback.usage_iterations.as_deref(),
        )
        .last()
        .map(|(model, usage)| {
            (
                broca_protocol::ModelHandle::new(&requested.provider_module_id, *model),
                *usage,
            )
        })
    });
    history.previous_native_usage = native.as_ref().map(|(_, usage)| *usage);
    history.previous_model = native.map(|(model, _)| model);
    history
}
