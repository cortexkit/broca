//! What a session's WAL says about whether it can move into the new storage now. Each refusal
//! is named by the reason a move reports (`not_movable {reason}`). The daemon adds what only it
//! knows: the operations the new storage serves, its run index, registered late results and
//! scheduled cache work.

use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::keys::DeploymentKey;
use broca_session_store::session::Session;
use broca_wal::{FramedRecord, WalLineageId, WalRecord};

/// Open work a move can't carry: a run that hasn't ended (active, paused other than for sign-in,
/// or holding calls), a send queued or pending other than an ordered send that waits for a tool
/// result, or an approval still open. The two exceptions move as they are (see
/// [`wal_refusal`]).
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
///
/// Two kinds of open work move with the session as they are, because the new storage serves
/// them as today's does: an ordered send whose only delivery mode is `tool_result`, which waits
/// for a later run's tool batch or a send that supersedes it, and a latest run paused for
/// sign-in with no call held (see [`paused_for_sign_in`]), which waits for its own send's
/// retry, a cancel or an interrupt. Today's storage leaves both as they are when it opens the
/// session.
pub fn wal_refusal(
    records: &[FramedRecord],
    lineage: Option<WalLineageId>,
) -> Option<&'static str> {
    let raw: Vec<WalRecord> = records.iter().map(|frame| frame.record.clone()).collect();
    if !broca_core::pending_submissions(&raw)
        .iter()
        .all(waits_for_a_tool_result)
    {
        return Some(SESSION_BUSY);
    }
    if let Some(lineage) = lineage {
        if !broca_core::deferred_call_obligations(lineage, records).is_empty() {
            return Some(SESSION_BUSY);
        }
    }
    let paused = paused_in(&raw);
    let unfinished = raw.iter().any(|record| match record {
        WalRecord::RunStarted { run_id, .. } => {
            paused.as_deref() != Some(run_id.0.as_str())
                && broca_core::reconcile_run_state(&raw, &run_id.0)
                    .is_none_or(|state| !state.state.is_terminal())
        }
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

/// Whether the pending send `pending` is an ordered send that only a tool result delivers: it
/// joins no run by itself, and today's storage keeps it queued until a later run's tool batch
/// carries it or a send with its dedupe key supersedes it.
fn waits_for_a_tool_result(pending: &broca_core::PendingSubmission) -> bool {
    pending
        .order
        .as_ref()
        .is_some_and(|order| order.modes == [broca_protocol::DeliveryMode::ToolResult])
}

/// The run id of the session's latest run when it is paused for sign-in with no call held, no
/// step in flight and no tool batch open: the one unfinished run a move carries as it is. Today's
/// storage leaves such a run paused when it opens the session (its restart resume skips sign-in
/// pauses), and answers a new send with the pause, until the run's own send is retried, or the
/// run is cancelled or interrupted.
pub fn paused_for_sign_in(records: &[FramedRecord]) -> Option<String> {
    let raw: Vec<WalRecord> = records.iter().map(|frame| frame.record.clone()).collect();
    paused_in(&raw)
}

fn paused_in(raw: &[WalRecord]) -> Option<String> {
    let latest = broca_core::replay(raw).ok()?.run_id?;
    let state = broca_core::reconcile_run_state(raw, &latest.0)?;
    let paused = state.state == broca_core::RunIndexState::Paused
        && state.pause_reason == Some(broca_protocol::PauseReason::AuthRequired)
        && state.held_calls.is_empty()
        && state.live_step.is_none();
    paused.then_some(latest.0)
}

/// Whether the session's run-index rows `rows` let it move: each terminal, with no withdrawal
/// owed and no missing export. `exports_settled` comes from the export reconciler's exclusions:
/// a fact exists, the run was imported, or it expired. Usage is not evidence of export debt;
/// old terminal rows and runs with no model answer can have no usage forever.
/// The row of a run the move carries paused for sign-in (`paused`, from [`paused_for_sign_in`])
/// is the exception: only the run itself finishes it, on the new storage.
pub fn run_rows_settled(
    rows: &[broca_core::RunIndexRow],
    paused: Option<&str>,
    exports_settled: bool,
) -> bool {
    exports_settled
        && rows.iter().all(|row| {
            let carried = paused == Some(row.run_id.as_str())
                && row.state == broca_core::RunIndexState::Paused
                && !row.withdrawals_owed;
            carried || (row.state.is_terminal() && !row.withdrawals_owed)
        })
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
