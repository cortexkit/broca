//! Payload-independent send receipts rebuilt from the durable WAL for retries and polling.

use broca_core::delivery_order::{queued_send_fate, QueuedSendFate};
use broca_protocol::{RunId, SubmissionId};
use broca_wal::WalRecord;
use cortexkit_role_llm_runner::send::{delivered_as, Delivered};

use crate::actor::{indexed_finish_reason, recorded_steers, steer_delivery, SendOutcome};

/// The same fate lookup serves a validated re-send and a read-only `send.status`.
/// No writer, executor or actor handle is available here: a lookup cannot deliver work.
pub(crate) fn recorded_send_outcome(
    records: &[WalRecord],
    own_run: &RunId,
    send_id: &str,
) -> Option<SendOutcome> {
    if let Some(steer) = recorded_steers(records).get(send_id) {
        let target = steer.run_id.as_ref()?;
        return Some(run_outcome(
            records,
            target,
            steer_delivery(records, send_id),
            false,
        ));
    }
    let submission_id = SubmissionId(format!("sub-{}", own_run.0));
    match queued_send_fate(records, &own_run.0)? {
        QueuedSendFate::Pending => Some(SendOutcome::Pending { submission_id }),
        QueuedSendFate::Ended(reason) => Some(SendOutcome::SendEnded {
            submission_id,
            reason,
        }),
        QueuedSendFate::Retracted => Some(SendOutcome::Retracted { submission_id }),
        QueuedSendFate::Delivered { run_id, .. } => Some(run_outcome(
            records,
            &run_id,
            Some(Delivered::new(delivered_as::STEP).with_ref(run_id.0.clone())),
            true,
        )),
        QueuedSendFate::Started => {
            let ordered = records.iter().any(|record| {
                matches!(record,
                WalRecord::DeliveryOrdered { submission_id: id, .. } if id == &submission_id)
            });
            let delivered =
                ordered.then(|| Delivered::new(delivered_as::TURN).with_ref(own_run.0.clone()));
            Some(run_outcome(records, own_run, delivered, true))
        }
    }
}

fn run_outcome(
    records: &[WalRecord],
    run_id: &RunId,
    delivered: Option<Delivered>,
    report_pause: bool,
) -> SendOutcome {
    let Some(state) = broca_core::reconcile_run_state(records, &run_id.0) else {
        return SendOutcome::Error(
            "the run carrying a recorded send is missing from the WAL".into(),
        );
    };
    if let Some(reason) = indexed_finish_reason(&state.state) {
        SendOutcome::Finished {
            run_id: run_id.clone(),
            reason,
            delivered,
        }
    } else if let Some(reason) = state.pause_reason.filter(|_| report_pause) {
        SendOutcome::Paused {
            run_id: run_id.clone(),
            reason,
            delivered,
        }
    } else {
        SendOutcome::Active {
            run_id: run_id.clone(),
            delivered,
        }
    }
}
