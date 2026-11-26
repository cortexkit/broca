//! Cancellation evidence for held calls on the session store, without reading the transcript.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use broca_core::tool_plane::LateResultRegistration;
use broca_protocol::{BindIdentity, RunFinishReason};
use broca_records::session_storage::StorageClass;
use broca_session_store::backend::SessionAddr;
use broca_session_store::record::{CallOutcome, Record};
use broca_session_store::session::{Position, Session};

use crate::store_open::{view_error, StoreRuntime};

const MAX_MEMO_ENTRIES: usize = 1024;

type Identities = HashMap<String, Result<Option<bool>, String>>;

/// A withdrawal can wait through many retries. Keep its answer, not the view used to read it.
#[derive(Default)]
pub(super) struct CancellationMemo(Mutex<HashMap<(BindIdentity, String), Answer>>);

#[derive(Clone, Copy)]
struct Answer {
    class: StorageClass,
    position: Position,
    identity: Option<bool>,
}

impl CancellationMemo {
    pub(super) fn forget(&self, call: &LateResultRegistration) {
        self.forget_calls(&call.bind, std::iter::once(call.call_key.as_str()));
    }

    pub(super) fn forget_calls<'a>(
        &self,
        bind: &BindIdentity,
        calls: impl Iterator<Item = &'a str>,
    ) {
        let mut memo = self.0.lock().expect("cancellation memo");
        for key in calls {
            memo.remove(&(bind.clone(), key.to_owned()));
        }
    }

    fn candidates(&self, calls: &[LateResultRegistration]) -> HashMap<String, Answer> {
        let memo = self.0.lock().expect("cancellation memo");
        calls
            .iter()
            .filter_map(|call| {
                memo.get(&(call.bind.clone(), call.call_key.clone()))
                    .map(|answer| (call.call_key.clone(), *answer))
            })
            .collect()
    }

    fn remember(&self, call: &LateResultRegistration, answer: Answer) {
        let mut memo = self.0.lock().expect("cancellation memo");
        let room = memo.len() < MAX_MEMO_ENTRIES;
        match memo.entry((call.bind.clone(), call.call_key.clone())) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                entry.insert(answer);
            }
            std::collections::hash_map::Entry::Vacant(entry) if room => {
                entry.insert(answer);
            }
            _ => {}
        }
    }
}

pub(super) async fn cancellation_identity(
    memo: &CancellationMemo,
    runtime: &StoreRuntime,
    call: &LateResultRegistration,
) -> Result<Option<bool>, String> {
    let mut identities = cancellation_identities(memo, runtime, std::slice::from_ref(call)).await?;
    identities
        .remove(&call.call_key)
        .unwrap_or_else(|| Err("withdrawal dispatch intent is missing".into()))
}

/// [`cancellation_identity`] for several calls of one session, by call key, from one read of
/// its ledger: back to the oldest of their intents, not once per call.
pub(super) async fn cancellation_identities(
    memo: &CancellationMemo,
    runtime: &StoreRuntime,
    calls: &[LateResultRegistration],
) -> Result<Identities, String> {
    let Some(first) = calls.first() else {
        return Ok(Identities::new());
    };
    let bind = &first.bind;
    let class = runtime
        .class_of(bind)
        .map_err(|error| error.to_string())?
        .ok_or("withdrawal session is missing")?;
    let mut identities = Identities::new();
    let candidates = memo.candidates(calls);
    let memo_position = if candidates.is_empty() {
        None
    } else {
        // A negative answer can become positive after a cancel. A seal, revert or replacement
        // can also change what is readable. Only reuse evidence from the current stored position.
        Session::stored_position(
            &runtime.class_backend(class),
            &SessionAddr(broca_core::session_addr(bind)),
            runtime.deployment(),
        )
        .await
        .ok()
        .flatten()
    };
    if let Some(position) = memo_position {
        identities.extend(candidates.into_iter().filter_map(|(key, answer)| {
            (answer.class == class && answer.position == position)
                .then_some((key, Ok(answer.identity)))
        }));
    }
    let mut unseen: HashSet<&str> = calls
        .iter()
        .map(|call| call.call_key.as_str())
        .filter(|key| !identities.contains_key(*key))
        .collect();
    if unseen.is_empty() {
        return Ok(identities);
    }
    let view = runtime
        .current_view_in(bind, class, "held_withdrawals")
        .await
        .map_err(|error| view_error(error).to_string())?
        .ok_or("withdrawal session is missing")?;
    if !identities.is_empty() && memo_position != Some(view.position()) {
        // The writer advanced between the probe and the load. Keep the whole batch on the view's
        // position, rather than mixing answers from two snapshots.
        identities.clear();
        unseen.extend(calls.iter().map(|call| call.call_key.as_str()));
    }
    // Only a call's suffix can contain its terminal and the run's end. Stop at the oldest
    // intent, then use the run-id index to find each run start rather than walking earlier
    // history.
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
    for call in calls {
        if identities.contains_key(&call.call_key) {
            continue;
        }
        let identity = call_identity(runtime, class, &view, &records, call).await;
        if let Ok(identity) = identity {
            memo.remember(
                call,
                Answer {
                    class,
                    position: view.position(),
                    identity,
                },
            );
        }
        identities.insert(call.call_key.clone(), identity);
    }
    Ok(identities)
}

/// Whether `call` was cancelled while held, from `records`, the session's ledger from the
/// newest record back to at least the call's intent.
async fn call_identity(
    runtime: &StoreRuntime,
    class: StorageClass,
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
