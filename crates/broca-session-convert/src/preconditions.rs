//! What a session's WAL says about whether it can move into the new storage now. Each refusal
//! is named by the reason a move reports (`not_movable {reason}`). The daemon adds what only it
//! knows: the operations the new storage serves, its run index, registered late results and
//! scheduled cache work.

use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::keys::DeploymentKey;
use broca_session_store::session::Session;
use broca_wal::{FramedRecord, WalLineageId, WalRecord};

/// Open work: a run that hasn't ended (active, paused, or held by a provider), a send queued,
/// pending or ordered and not yet delivered, or an approval still open.
pub const SESSION_BUSY: &str = "session_busy";
/// A tool batch whose results arrived but which didn't close. Today's storage and the new one
/// place such a turn's message at different times, so it can't be carried across.
pub const OPEN_TOOL_BATCH: &str = "open_tool_batch";
/// Something the new run loop refuses to run: first-protocol compaction, approval questions, or
/// an input-token budget.
pub const UNSUPPORTED_FEATURE: &str = "unsupported_feature";

// The features behind an `UNSUPPORTED_FEATURE` refusal, each named for counting the sessions it
// keeps from moving. A move reports only `UNSUPPORTED_FEATURE`.

/// A call deferred to a person's answer (an approval question).
pub const APPROVAL_QUESTIONS: &str = "approval_questions";
/// A run with an input-token budget.
pub const INPUT_TOKEN_BUDGET: &str = "input_token_budget";
/// Compaction under the first compaction protocol.
pub const FIRST_PROTOCOL_COMPACTION: &str = "first_protocol_compaction";

/// The first reason the WAL `records` gives for not moving its session now, if any.
pub fn wal_refusal(
    records: &[FramedRecord],
    lineage: Option<WalLineageId>,
) -> Option<&'static str> {
    let raw: Vec<WalRecord> = records.iter().map(|frame| frame.record.clone()).collect();
    if !broca_core::pending_submissions(&raw).is_empty() {
        return Some(SESSION_BUSY);
    }
    if let Some(lineage) = lineage {
        if !broca_core::deferred_call_obligations(lineage, records).is_empty() {
            return Some(SESSION_BUSY);
        }
    }
    let unfinished = raw.iter().any(|record| match record {
        WalRecord::RunStarted { run_id, .. } => broca_core::reconcile_run_state(&raw, &run_id.0)
            .is_none_or(|state| !state.state.is_terminal()),
        _ => false,
    });
    if unfinished {
        return Some(SESSION_BUSY);
    }
    let mut open_batches = Vec::new();
    for record in &raw {
        match record {
            WalRecord::ToolBatchStarted { batch_id, .. } => open_batches.push(batch_id.clone()),
            WalRecord::ToolBatchFinished { batch_id, .. } => {
                open_batches.retain(|open| open != batch_id)
            }
            _ => {}
        }
    }
    if !open_batches.is_empty() {
        return Some(OPEN_TOOL_BATCH);
    }
    if unsupported_in_wal(records).is_some() {
        return Some(UNSUPPORTED_FEATURE);
    }
    None
}

/// The first feature the new run loop refuses that the WAL `records` use, of those the WAL alone
/// shows: [`APPROVAL_QUESTIONS`] or [`INPUT_TOKEN_BUDGET`]. [`wal_refusal`] refuses such a
/// session as [`UNSUPPORTED_FEATURE`].
pub fn unsupported_in_wal(records: &[FramedRecord]) -> Option<&'static str> {
    records.iter().find_map(|frame| match &frame.record {
        WalRecord::ToolCallDeferred { .. } => Some(APPROVAL_QUESTIONS),
        WalRecord::RunStarted { config, .. }
            if config
                .budget
                .as_ref()
                .is_some_and(|budget| budget.max_input_tokens.is_some()) =>
        {
            Some(INPUT_TOKEN_BUDGET)
        }
        _ => None,
    })
}

/// The first reason the converted session gives for not moving it now, read from the new
/// storage's own folds of its records: compaction under the first protocol. Folds an in-memory
/// copy, so nothing is written anywhere.
///
/// Compaction views on the current contract move with the session: its open log holds every view
/// record, from which the run loop restores them and carries copies through each seal.
pub async fn converted_refusal(
    prepared: &crate::stage::Prepared,
) -> Result<Option<&'static str>, String> {
    let backend = MemoryBackend::default();
    let addr = SessionAddr("precondition".into());
    let triple = broca_session_store::head::SessionTriple {
        project_root: "/precondition".into(),
        harness: "precondition".into(),
        session: "precondition".into(),
    };
    let mut session = Session::create(
        &backend,
        addr,
        triple,
        &DeploymentKey::new(1, &[0; 32]),
        prepared.appends_in_conversation,
    )
    .await
    .map_err(|error| format!("fold the converted records: {error:?}"))?;
    session
        .append(&prepared.converted.records, false)
        .await
        .map_err(|error| format!("fold the converted records: {error:?}"))?;
    if session.conversation.first_protocol_compaction
        || session.operations.first_protocol_compaction
    {
        return Ok(Some(UNSUPPORTED_FEATURE));
    }
    Ok(None)
}
