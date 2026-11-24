//! The calls a provider still holds for a session about to be deleted, read from its ledger.

use broca_records::session_storage::StorageClass;
use broca_session_store::keys::Cipher;
use broca_session_store::record::{CallOutcome, Record};

use crate::retention::HeldAtDeletion;
use crate::store_open::StoreRuntime;

/// The calls of `records` (the session's whole ledger) that a provider held and that never
/// completed. A call the run cancelled while it was held, or one a restart interrupted, is still
/// running at the provider until it is withdrawn. A call whose run start or plan can't be read
/// is left out: its withdrawal couldn't say whether it names the session.
pub(super) fn held_at_deletion(
    runtime: &StoreRuntime,
    class: StorageClass,
    cipher: Cipher,
    records: &[(u64, Record)],
) -> Vec<HeldAtDeletion> {
    let mut calls = Vec::new();
    for (_, record) in records {
        let Record::ToolHeld(held) = record else {
            continue;
        };
        let completed = records.iter().any(|(_, record)| {
            matches!(record, Record::ToolOutcome(outcome) if outcome.batch_id == held.batch_id
                && outcome.tool_call_id == held.tool_call_id
                && outcome.outcome == CallOutcome::Completed)
        });
        if completed {
            continue;
        }
        let run_id = records.iter().find_map(|(_, record)| match record {
            Record::ToolIntent(intent)
                if intent.batch_id == held.batch_id && intent.tool_call_id == held.tool_call_id =>
            {
                Some(&intent.run_id)
            }
            _ => None,
        });
        let start = run_id.and_then(|run_id| {
            records.iter().find_map(|(_, record)| match record {
                Record::RunStart(start) if &start.run_id == run_id => Some(start),
                _ => None,
            })
        });
        let session_identity = start
            .ok_or_else(|| "the call's run start is missing".to_string())
            .and_then(|start| {
                broca_session_loop::run::admission_of_start(start, runtime.parts_for(class, cipher))
                    .map_err(|error| format!("{error:?}"))
            })
            .and_then(|admission| match admission.manifest.as_deref() {
                Some(manifest) => broca_core::ToolManifest::from_tools(Vec::new())
                    .with_plan_manifest(manifest)
                    .map(|manifest| manifest.uses_session_identity(&held.module))
                    .map_err(|error| error.to_string()),
                None => Ok(false),
            });
        match session_identity {
            Ok(session_identity) => calls.push(HeldAtDeletion {
                module: held.module.clone(),
                call_key: held.call_key.clone(),
                session_identity,
            }),
            Err(error) => tracing::warn!(target: "late_results", call_key = %held.call_key,
                "a held call of a deleted session is not withdrawn: {error}"),
        }
    }
    calls
}
