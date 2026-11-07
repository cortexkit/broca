//! Today's answers to `run.status`, `run.result` and `send.status` for a session closed on
//! today's storage, built from its WAL alone by today's own rules: what the converter's check
//! compares the new storage's answers with, log by log.
//!
//! The module serves these ops through more paths than a WAL: the run index's rows and a live
//! actor. For a closed session with no run index they reduce to these. A run the WAL leaves
//! unfinished is the one difference: today's read opens the session first, which seals the run,
//! so the answer here (active) is not the one a caller would get; the check counts such runs
//! apart.

use broca_protocol::RunId;
use broca_wal::FramedRecord;

use crate::serve::ServeError;

/// `run.status` for `run_id`, as the wire answers it.
pub fn run_status_reply(
    records: &[FramedRecord],
    run_id: &RunId,
) -> Result<broca_wire::RunStatusReply, ServeError> {
    use broca_core::delivery_order::QueuedSendFate;
    use broca_wire::{QueuedSendState, RunStatusReply, RunStatusResponse};
    let status = run_status(records, run_id)?;
    if !matches!(status, RunStatusResponse::Unknown) {
        return Ok(status.into());
    }
    Ok(
        match broca_core::delivery_order::queued_send_fate(
            records.iter().map(|frame| &frame.record),
            &run_id.0,
        ) {
            None => status.into(),
            Some(QueuedSendFate::Pending) => RunStatusReply::Send(QueuedSendState::Pending),
            Some(QueuedSendFate::Ended(reason)) => RunStatusReply::Send(match reason {
                broca_wal::record::SendEndReason::Superseded { .. } => QueuedSendState::Superseded,
                broca_wal::record::SendEndReason::Expired => QueuedSendState::Expired,
            }),
            Some(QueuedSendFate::Retracted) => RunStatusReply::Send(QueuedSendState::Retracted),
            Some(QueuedSendFate::Delivered { run_id: into, .. }) => RunStatusReply::Run {
                status: Box::new(run_status(records, &into)?),
                delivered: Some(
                    cortexkit_role_llm_runner::send::Delivered::new(
                        cortexkit_role_llm_runner::send::delivered_as::STEP,
                    )
                    .with_ref(into.0),
                ),
            },
            Some(QueuedSendFate::Started) => run_status(records, run_id)?.into(),
        },
    )
}

/// `run.status` for the run `run_id` itself.
pub fn run_status(
    records: &[FramedRecord],
    run_id: &RunId,
) -> Result<broca_wire::RunStatusResponse, ServeError> {
    use broca_wire::RunStatusResponse;
    let state =
        broca_core::reconcile_run_state(records.iter().map(|frame| &frame.record), &run_id.0);
    let mut response = crate::serve::run_status_response_with_reader_policy(state, false);
    match &mut response {
        RunStatusResponse::Interrupted { reason, .. } => {
            *reason = crate::serve::recorded_interrupt_reason(records, run_id);
        }
        RunStatusResponse::Completed { directives, .. }
            if crate::serve::wake_directive_requested(records, run_id) =>
        {
            *directives = crate::serve::final_message_of(records, run_id)
                .ok()
                .and_then(|message| {
                    crate::serve::completed_directives(records, run_id, &message.text)
                });
        }
        _ => {}
    }
    crate::serve::checked_run_status(response, &run_id.0)
}

/// `run.result` for the run `run_id` of the session `session_name`. The final message's id is
/// today's position form, `m<ordinal>`, whatever form its log spells message ids in.
pub fn run_result(
    records: &[FramedRecord],
    run_id: &RunId,
    session_name: &str,
) -> Result<broca_wire::RunResultResponse, ServeError> {
    let state =
        broca_core::reconcile_run_state(records.iter().map(|frame| &frame.record), &run_id.0);
    let auth_selection = state.as_ref().and_then(crate::serve::frozen_auth_selection);
    let detail = state.as_ref().and_then(|state| state.budget_detail.clone());
    let context_ceiling = state
        .as_ref()
        .and_then(|state| state.context_ceiling.clone());
    let structured_output = state
        .as_ref()
        .and_then(|state| state.structured_output.clone());
    let usage = state
        .as_ref()
        .map(|state| state.run_usage.clone())
        .unwrap_or_default();
    let finish = state.as_ref().and_then(|state| state.finish.clone());
    let status = crate::serve::run_status_response_with_reader_policy(state, false);
    let Some((state, mut reason, error)) = crate::serve::result_state(status) else {
        return Err(ServeError::UnknownRun(run_id.0.clone()));
    };
    if state == "interrupted" {
        reason = crate::serve::recorded_interrupt_reason(records, run_id);
    }
    let mut directives = None;
    let final_message = if state == "completed" {
        let message = crate::serve::final_message_of(records, run_id)?;
        directives = crate::serve::completed_directives(records, run_id, &message.text);
        Some(message)
    } else {
        None
    };
    crate::serve::checked_run_result(
        broca_wire::RunResultResponse {
            finish: if matches!(state.as_str(), "active" | "paused") {
                None
            } else {
                finish
            },
            auth_selection,
            directives,
            usage,
            detail,
            context_ceiling,
            run_id: run_id.0.clone(),
            state,
            reason,
            error,
            final_message,
            structured_output,
        },
        session_name,
    )
}

/// `send.status` for the send `send_id` whose run id is `own_run`, asked with no principal.
pub fn send_status(
    records: &[FramedRecord],
    own_run: RunId,
    send_id: &str,
) -> Result<broca_wire::SendReplyWire, ServeError> {
    crate::serve::send_status_of_records(records.to_vec(), own_run, send_id, None)
}
