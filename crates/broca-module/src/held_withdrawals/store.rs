//! Cancellation evidence for held calls on the session store, without reading the transcript.

use std::collections::{HashMap, HashSet};

use broca_core::tool_plane::LateResultRegistration;
use broca_protocol::RunFinishReason;
use broca_session_store::record::{CallOutcome, Record};

use crate::store_open::StoreRuntime;

pub(super) async fn cancellation_identity(
    runtime: &StoreRuntime,
    call: &LateResultRegistration,
) -> Result<Option<bool>, String> {
    let mut identities = cancellation_identities(runtime, std::slice::from_ref(call)).await?;
    identities
        .remove(&call.call_key)
        .unwrap_or_else(|| Err("withdrawal dispatch intent is missing".into()))
}

/// [`cancellation_identity`] for several calls of one session, by call key, from one read of
/// its ledger: back to the oldest of their intents, not once per call.
pub(super) async fn cancellation_identities(
    runtime: &StoreRuntime,
    calls: &[LateResultRegistration],
) -> Result<HashMap<String, Result<Option<bool>, String>>, String> {
    let Some(first) = calls.first() else {
        return Ok(HashMap::new());
    };
    let view = runtime
        .current_view(&first.bind)
        .await
        .map_err(|error| error.to_string())?
        .ok_or("withdrawal session is missing")?;
    // Only a call's suffix can contain its terminal and the run's end. Stop at the oldest
    // intent, then use the run-id index to find each run start rather than walking earlier
    // history.
    let mut unseen: HashSet<&str> = calls.iter().map(|call| call.call_key.as_str()).collect();
    let records = view
        .ledger_tail_until(|record| match record {
            Record::ToolIntent(intent) => {
                intent
                    .call_key
                    .as_deref()
                    .is_some_and(|key| unseen.remove(key))
                    && unseen.is_empty()
            }
            _ => false,
        })
        .await
        .map_err(|error| format!("{error:?}"))?;
    let mut identities = HashMap::new();
    for call in calls {
        let identity = call_identity(runtime, &view, &records, call).await;
        identities.insert(call.call_key.clone(), identity);
    }
    Ok(identities)
}

/// Whether `call` was cancelled while held, from `records`, the session's ledger from the
/// newest record back to at least the call's intent.
async fn call_identity(
    runtime: &StoreRuntime,
    view: &crate::store_read::StoredView<broca_session_store::backend::LocalBackend>,
    records: &[(u64, Record)],
    call: &LateResultRegistration,
) -> Result<Option<bool>, String> {
    let Some(at) = records.iter().position(|(_, record)| {
        matches!(record, Record::ToolIntent(intent) if intent.call_key.as_deref() == Some(&call.call_key))
    }) else {
        return Err("withdrawal dispatch intent is missing".into());
    };
    // Newest first: the call's suffix runs from the newest record to its intent.
    let records = &records[..=at];
    let Some(Record::ToolIntent(intent)) = records.last().map(|(_, record)| record) else {
        return Err("withdrawal dispatch intent is missing".into());
    };
    let held = records.iter().any(|(_, record)| {
        matches!(record, Record::ToolHeld(held) if held.call_key == call.call_key
            && held.batch_id == intent.batch_id && held.tool_call_id == intent.tool_call_id)
    });
    let terminal = records.iter().find_map(|(_, record)| match record {
        Record::ToolOutcome(outcome)
            if outcome.batch_id == intent.batch_id
                && outcome.tool_call_id == intent.tool_call_id =>
        {
            Some(outcome)
        }
        _ => None,
    });
    let cancelled = records.iter().any(|(_, record)| {
        matches!(record, Record::RunEnd(end) if end.run_id == intent.run_id
            && end.reason == RunFinishReason::Cancelled)
    });
    if !held
        || !cancelled
        || !terminal.is_some_and(|outcome| {
            outcome.outcome == CallOutcome::Interrupted
                && outcome.reason.as_deref() == Some("provider_held_cancelled")
        })
    {
        return Ok(None);
    }
    let start = view
        .find_run_start(&intent.run_id)
        .await
        .map_err(|error| format!("{error:?}"))?
        .ok_or("withdrawal run start is missing")?;
    let class = runtime
        .class_of(&call.bind)
        .map_err(|error| error.to_string())?
        .ok_or("withdrawal session is missing")?;
    let admission = broca_session_loop::run::admission_of_start(
        &start,
        runtime.parts_for(class, view.head().cipher()),
    )
    .map_err(|error| format!("withdrawal plan is unreadable: {error:?}"))?;
    match admission.manifest.as_deref() {
        Some(manifest) => broca_core::ToolManifest::from_tools(Vec::new())
            .with_plan_manifest(manifest)
            .map(|manifest| Some(manifest.uses_session_identity(&call.module)))
            .map_err(|error| format!("withdrawal plan is unreadable: {error}")),
        None => Ok(Some(false)),
    }
}
