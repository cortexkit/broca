//! Payload-independent send receipts rebuilt from the durable WAL for retries and polling.

use broca_core::delivery_order::{queued_send_fate, QueuedSendFate};
use broca_protocol::{BindPrincipal, RunId, SubmissionId};
use broca_wal::WalRecord;
use cortexkit_role_llm_runner::send::{delivered_as, Delivered};

use crate::actor::{indexed_finish_reason, recorded_steers, steer_delivery, SendOutcome};

/// Submission attribution is immutable: never infer it from the current plan or caller.
pub(crate) fn recorded_submission_sender(
    records: &[WalRecord],
    submission_id: &SubmissionId,
) -> Option<BindPrincipal> {
    let mut queued = false;
    let mut ordered = false;
    let mut principal = None;
    let mut sent_by = None;
    for record in records {
        match record {
            WalRecord::DeliveryOrdered {
                submission_id: id,
                sent_by: sender,
                ..
            } if id == submission_id => {
                if ordered || queued {
                    return None;
                }
                ordered = true;
                sent_by = sender.clone();
            }
            WalRecord::Queued {
                submission_id: id,
                principal: sender,
                ..
            } if id == submission_id => {
                if queued {
                    return None;
                }
                queued = true;
                principal = sender.clone();
            }
            _ => {}
        }
    }
    // Delegated sends run under the owner: only DeliveryOrdered.sent_by preserves
    // their actual sender. Ordinary sends record their sender on Queued instead.
    queued.then(|| sent_by.or(principal)).flatten()
}

fn run_sender(records: &[WalRecord], run: &RunId) -> Option<Option<BindPrincipal>> {
    let submission = SubmissionId(format!("sub-{}", run.0));
    if records.iter().any(|record| {
        matches!(record,
        WalRecord::Queued { submission_id, .. } if submission_id == &submission)
    }) {
        return Some(recorded_submission_sender(records, &submission));
    }
    records.iter().find_map(|record| match record {
        WalRecord::RunStarted {
            run_id, principal, ..
        } if run_id == run => Some(principal.clone()),
        _ => None,
    })
}

/// Preserve legacy IDs, qualifying a delegated sender only when another sender already
/// owns that ID. Both admission and send.status use the same durable attribution.
pub(crate) fn resolve_sender_run_id(
    records: &[WalRecord],
    base: &RunId,
    send_id: &str,
    sender: &BindPrincipal,
) -> Result<RunId, String> {
    use sha2::Digest;
    let qualified = RunId(format!(
        "{}-sender-{:x}",
        base.0,
        sha2::Sha256::digest(serde_json::to_vec(sender).expect("a principal serializes"))
    ));
    if let Some(recorded) = run_sender(records, &qualified) {
        return if recorded.as_ref() == Some(sender) {
            Ok(qualified)
        } else {
            Err("the qualified send ID has no matching durable sender".into())
        };
    }
    match run_sender(records, base) {
        Some(Some(recorded)) if &recorded == sender => Ok(base.clone()),
        Some(_) => Ok(qualified),
        // A legacy steer has no run of its own, but its owner's send key still occupies
        // the unqualified namespace. Delegated string steers already use ordered records.
        None if recorded_steers(records).contains_key(send_id) => Ok(qualified),
        None => Ok(base.clone()),
    }
}

/// The same fate lookup serves a validated re-send and a read-only `send.status`.
/// No writer, executor or actor handle is available here: a lookup cannot deliver work.
/// Ordered sends disable legacy steer lookup because those keys belong to the owner,
/// not to another sender sharing the same send_id.
pub(crate) fn recorded_send_outcome(
    records: &[WalRecord],
    own_run: &RunId,
    send_id: &str,
    legacy_steers: bool,
) -> Option<SendOutcome> {
    if legacy_steers {
        let steers = recorded_steers(records);
        if let Some(steer) = steers.get(send_id) {
            let target = steer.run_id.as_ref()?;
            return Some(run_outcome(
                records,
                target,
                steer_delivery(records, send_id),
                false,
            ));
        }
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
