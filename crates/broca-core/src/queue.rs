//! The durable submission queue (Phase 2, B3).
//!
//! A submission is one prompt injection into a session. While a run is active, a new
//! prompt is durably QUEUED (`WalRecord::Queued{submission_id, message}`); it can be
//! RETRACTED (`Retracted{submission_id}`) while still pending, or CONSUMED into a run
//! (the coordinator writes `RunStarted{submission_id}`). This module derives the PENDING
//! set and the retract decision PURELY from the durable WAL records — no in-memory queue,
//! so the queue survives a crash and is unambiguous across one.
//!
//! The coordinator (a later phase) owns WHEN to drain/consume and serializes appends
//! under its actor lock; this module gives it the pure, replay-derived decisions to
//! append. The crash-safety is proven here at the record level.
//!
//! ## Why it's crash-unambiguous BY CONSTRUCTION
//! `pending = Queued − Retracted − consumed`, where `consumed = { id : ∃
//! RunStarted{submission_id == id} }`:
//!  - a crash BEFORE the consuming `RunStarted` ⇒ the submission is still in
//!    `Queued − Retracted` and not consumed ⇒ still pending ⇒ re-drained on re-open;
//!  - a crash AFTER ⇒ a `RunStarted{submission_id}` exists ⇒ consumed ⇒ never
//!    double-started.
//!
//! A `Retracted` for an already-consumed id retracts nothing (the submission is gone from
//! pending either way); a `RunStarted` consuming a retracted id is something the
//! coordinator must not do — it checks [`retract_decision`]/`pending_submissions` under
//! its lock before consuming — but even if it did, the set algebra removes the id once
//! (it's not in pending). Order of `Queued`/`Retracted`/`RunStarted` in the WAL doesn't
//! change the final set; the set is order-independent.

use std::collections::HashSet;

use broca_protocol::{SubmissionId, SubmissionRequest};
use broca_wal::WalRecord;
use serde::{Deserialize, Serialize};

/// One pending submission: its id + the caller's full request, ready to consume into a run
/// with the SAME config it was submitted with.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingSubmission {
    /// Durable pre_user output. The caller request stays unchanged for keyed retries;
    /// the drain uses hooks.final as the rendered input without running hooks again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hooks: Option<Box<broca_wal::Hooks>>,
    /// Routing affinity retained until this submission admits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation_key: Option<String>,
    pub submission_id: SubmissionId,
    pub request: SubmissionRequest,
    /// Canonical identity for a deterministic send-id request. Absent for nonce sends and
    /// legacy queued records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_identity_digest: Option<String>,
    /// The principal of the bind that queued this submission, from its `Queued` record. The
    /// drain records this one on the run it starts. Kept here rather than read from `request`,
    /// because the request never serializes its principal and this value must survive the
    /// admission checkpoint's round trip through the store.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal: Option<broca_protocol::BindPrincipal>,
    /// The caller's `send_id`, from the `Queued` record. The drain writes it on the run it
    /// starts, so the WAL keeps the key on the record that carries the message even when the
    /// queue was rebuilt after a restart. Absent for sends without one and older records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub send_id: Option<String>,
    /// The caller's title, from the `Queued` record. The drain writes it on the run it starts,
    /// so the run carries the title it was queued with even after a restart. Absent for sends
    /// without one and older records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// How the send asked to be delivered, from the `Queued` record. `Some(Interrupt)` drains
    /// ahead of every plain entry (see [`insert_in_drain_order`]). Absent for a plain queued
    /// send and for older records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery: Option<broca_protocol::Delivery>,
    /// The ordered delivery list, dedupe key and expiry of a send using the `delivery_order`
    /// group, from the `DeliveryOrdered` record written just before its `Queued` record. Absent
    /// for every other queued send, which drains into a run exactly as before.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order: Option<broca_protocol::DeliveryOrder>,
    /// The plan-named sender of an ordered send from a module other than the session's owner,
    /// from its `DeliveryOrdered` record. `principal` is then the session's frozen principal,
    /// which the send runs under; this is whose dedupe key the send carries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sent_by: Option<broca_protocol::BindPrincipal>,
}

impl PendingSubmission {
    /// Whether this entry was queued by an interrupting send.
    pub fn is_interrupt(&self) -> bool {
        self.delivery == Some(broca_protocol::Delivery::Interrupt)
    }

    /// Whether draining this entry may start a run: every plain queued send, and an ordered send
    /// only when its list names `idle_start`.
    pub fn starts_runs(&self) -> bool {
        self.order
            .as_ref()
            .is_none_or(broca_protocol::DeliveryOrder::starts_runs)
    }
}

/// The submission a record removes from the pending queue without starting a run: a tombstone
/// (`Retracted`, or `SendEnded` for a replaced or expired ordered send) or the delivery of an
/// ordered send into a running turn (`SendDelivered`).
pub fn removes_pending(record: &WalRecord) -> Option<&SubmissionId> {
    match record {
        WalRecord::Retracted { submission_id, .. }
        | WalRecord::SendEnded { submission_id, .. }
        | WalRecord::SendDelivered { submission_id, .. } => Some(submission_id),
        _ => None,
    }
}

/// Add `pending` to a queue kept in drain order: an interrupt goes after the interrupts already
/// pending and before every plain entry, a plain entry goes last.
///
/// This is the one ordering rule, used both when the queue is rebuilt from the WAL and when a
/// live send is queued, so the order after a restart is the order before it.
pub fn insert_in_drain_order(queue: &mut Vec<PendingSubmission>, pending: PendingSubmission) {
    let at = if pending.is_interrupt() {
        queue
            .iter()
            .position(|queued| !queued.is_interrupt())
            .unwrap_or(queue.len())
    } else {
        queue.len()
    };
    queue.insert(at, pending);
}

/// The pending submissions for a session, derived purely from its durable WAL records,
/// in DRAIN order. Pending = queued, minus retracted, minus consumed-by-a-RunStarted.
///
/// Drain order is every pending interrupt (a `Queued` carrying `delivery: interrupt`) first,
/// then every plain entry, each group in the WAL order of its `Queued` records: an interrupt's
/// message is the very next turn, and earlier plain sends run after it, unchanged.
///
/// A build that predates `Queued.delivery` ignores the field and drains everything in plain
/// WAL order. After a rollback onto such a build an interrupt therefore runs behind the plain
/// sends queued before it. That changes ORDER only: every message is still delivered exactly
/// once, which is the sense in which the field is safe to leave ungated.
pub fn pending_submissions(records: &[WalRecord]) -> Vec<PendingSubmission> {
    let retracted: HashSet<&SubmissionId> = records.iter().filter_map(removes_pending).collect();
    type Ordered<'a> = (
        &'a broca_protocol::DeliveryOrder,
        &'a Option<broca_protocol::BindPrincipal>,
    );
    let mut orders: std::collections::HashMap<&SubmissionId, Ordered<'_>> =
        std::collections::HashMap::new();
    let consumed: HashSet<&SubmissionId> = records
        .iter()
        .filter_map(|r| match r {
            WalRecord::RunStarted {
                submission_id: Some(id),
                ..
            } => Some(id),
            _ => None,
        })
        .collect();
    let mut pending = Vec::new();
    for record in records {
        if let WalRecord::DeliveryOrdered {
            submission_id,
            order,
            sent_by,
        } = record
        {
            orders.insert(submission_id, (order, sent_by));
        }
        if let WalRecord::Queued {
            keep_warm,
            submission_id,
            request,
            request_identity_digest,
            principal,
            send_id,
            title,
            delivery,
            conversation_key,
            hooks,
            ..
        } = record
        {
            if retracted.contains(submission_id) || consumed.contains(submission_id) {
                continue;
            }
            insert_in_drain_order(
                &mut pending,
                PendingSubmission {
                    hooks: hooks.clone(),
                    conversation_key: conversation_key.clone(),
                    submission_id: submission_id.clone(),
                    request: {
                        let mut request = request.as_ref().clone();
                        request.keep_warm = keep_warm.or(request.keep_warm);
                        request
                    },
                    request_identity_digest: request_identity_digest.clone(),
                    principal: principal.clone(),
                    send_id: send_id.clone(),
                    title: title.clone(),
                    delivery: *delivery,
                    order: orders.get(submission_id).map(|(order, _)| (*order).clone()),
                    sent_by: orders
                        .get(submission_id)
                        .and_then(|(_, sent_by)| (*sent_by).clone()),
                },
            );
        }
    }
    pending
}

/// The outcome of a retract request, decided purely from the durable records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetractDecision {
    /// The submission is still pending → the caller (coordinator) appends a durable
    /// `Retracted{submission_id}` tombstone.
    Retract,
    /// The submission was already consumed into a run (a `RunStarted{submission_id}`
    /// exists) → it cannot be retracted; the run is active/terminal.
    AlreadyStarted,
    /// No such submission is queued (unknown id, or already retracted) → nothing to do.
    NotPending,
}

/// Decide whether `submission_id` can be retracted, given the session's durable records.
/// Pure: the coordinator calls this under its lock, then appends the `Retracted` record
/// iff the decision is [`RetractDecision::Retract`]. A submission consumed into a run is
/// `AlreadyStarted` (a `RunStarted{submission_id}` is durable); a still-pending one is
/// `Retract`; anything else (unknown / already-retracted / not-yet-queued) is
/// `NotPending`.
pub fn retract_decision(records: &[WalRecord], submission_id: &SubmissionId) -> RetractDecision {
    // Consumed wins over pending: once a RunStarted references the id, the run exists and
    // a retract is too late, regardless of whether a Queued record is also present.
    let consumed = records.iter().any(|r| {
        matches!(
            r,
            WalRecord::RunStarted { submission_id: Some(id), .. } if id == submission_id
        )
    });
    if consumed {
        return RetractDecision::AlreadyStarted;
    }
    let pending = pending_submissions(records)
        .iter()
        .any(|p| &p.submission_id == submission_id);
    if pending {
        RetractDecision::Retract
    } else {
        RetractDecision::NotPending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_protocol::{
        content::{ContentBlock, Message, Role},
        request::{GenerationConfig, ModelHandle, ToolChoice},
        BindIdentity, RunId,
    };
    use broca_wal::{RunConfig, RunOrigin};

    fn msg(text: &str) -> Message {
        Message {
            cache_prefix_blocks: None,
            role: Role::User,
            content: vec![ContentBlock::Text { text: text.into() }],
            origin: None,
            ordinal: None,
            tags: Default::default(),
        }
    }

    fn queued(id: &str, text: &str) -> WalRecord {
        queued_as(id, text, None)
    }

    #[test]
    fn queued_input_hooks_survive_replay_and_checkpoint_roundtrip() {
        let hooks = Box::new(broca_wal::Hooks {
            original: None,
            final_value: Some(serde_json::json!([msg("final")])),
            ops: vec![broca_wal::HookOp {
                module: "p0".into(),
                hook: "pre_user".into(),
                phase: None,
                op: "append".into(),
                block: Some(0),
                note: None,
            }],
            unavailable: Some(vec!["p1".into()]),
        });
        let mut record = queued("hooked", "original");
        let WalRecord::Queued {
            hooks: recorded, ..
        } = &mut record
        else {
            unreachable!()
        };
        *recorded = Some(hooks.clone());
        let pending = pending_submissions(&[record.clone()]);
        assert_eq!(pending[0].request.input, vec![msg("original")]);
        assert_eq!(pending[0].hooks.as_ref(), Some(&hooks));
        let mut checkpoint = crate::AdmissionCheckpointV1::default();
        checkpoint.fold_record(1, &record).unwrap();
        assert_eq!(checkpoint.pending_fifo[0].hooks.as_ref(), Some(&hooks));
        let encoded = crate::encode_admission_checkpoint(&checkpoint).unwrap();
        let decoded = crate::decode_admission_checkpoint(&encoded).unwrap();
        assert_eq!(decoded.pending_fifo[0].hooks.as_ref(), Some(&hooks));
    }

    fn interrupt(id: &str, text: &str) -> WalRecord {
        queued_as(id, text, Some(broca_protocol::Delivery::Interrupt))
    }

    fn queued_as(id: &str, text: &str, delivery: Option<broca_protocol::Delivery>) -> WalRecord {
        WalRecord::Queued {
            retention: None,
            ts_ms: None,
            hooks: None,
            title: None,
            keep_warm: None,
            conversation_key: None,
            delivery,
            send_id: None,
            submission_id: SubmissionId(id.into()),
            request: Box::new(SubmissionRequest {
                directives: None,
                on_restart: None,
                keep_warm: None,
                scope: Default::default(),
                project_id: None,
                principal: None,
                model: ModelHandle::new("stub", "m"),
                input: vec![msg(text)],
                tools: Some(vec![]),
                policy_allowed: Default::default(),
                tool_choice: ToolChoice::Auto,
                generation: GenerationConfig::default(),
                cache: None,
                stop_when: vec![],
                budget: None,
                auth: None,
                context_limit: None,
                work_class: None,
                context_management: None,
                variant: None,
                service_tier: None,
                inherit_frozen: Default::default(),
                structured_output: None,
            }),
            request_identity_digest: None,
            principal: None,
        }
    }

    #[test]
    fn queued_keep_warm_survives_replay_and_drain_without_freezing_the_next_send() {
        use broca_protocol::request::KeepWarm;
        for policy in [None, Some(KeepWarm::Auto), Some(KeepWarm::Off)] {
            let mut record = queued("one", "hello");
            let WalRecord::Queued { keep_warm, .. } = &mut record else {
                unreachable!()
            };
            *keep_warm = policy;
            let restored: WalRecord =
                serde_json::from_str(&serde_json::to_string(&record).unwrap()).unwrap();
            let pending = pending_submissions(std::slice::from_ref(&restored));
            let run = crate::RunRequest::from_submission(
                pending[0].request.clone(),
                RunId("r".into()),
                BindIdentity::new("/tmp/p", "h", "s"),
                SubmissionId("one".into()),
            );
            assert_eq!(run.keep_warm, policy);
            assert_eq!(run.to_submission().keep_warm, policy);
            let mut checkpoint = crate::AdmissionCheckpointV1::empty();
            checkpoint.fold_record(1, &restored).unwrap();
            assert_eq!(checkpoint.pending_fifo[0].request.keep_warm, policy);
            let mut started = consumed("r", "one");
            let WalRecord::RunStarted { keep_warm, .. } = &mut started else {
                unreachable!()
            };
            *keep_warm = policy;
            checkpoint.fold_record(2, &started).unwrap();
            assert_eq!(checkpoint.last_episode.as_ref().unwrap().keep_warm, policy);
            assert_eq!(crate::replay(&[started]).unwrap().keep_warm, policy);
            assert_eq!(
                pending_submissions(&[queued("next", "next")])[0]
                    .request
                    .keep_warm,
                None
            );
        }
    }

    fn retracted(id: &str) -> WalRecord {
        WalRecord::Retracted {
            submission_id: SubmissionId(id.into()),
            failure: None,
        }
    }

    fn consumed(run: &str, sub: &str) -> WalRecord {
        WalRecord::RunStarted {
            tags: None,
            session_class: None,
            retention: None,
            hooks: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            marker_order_version: None,
            manifest: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            ts_ms: None,
            run_id: RunId(run.into()),
            session: BindIdentity::new("/tmp/p", "h", "s"),
            config: Box::new(RunConfig {
                turn_directives_v1: None,
                render_tags: false,
                computed_tags: None,
                model: ModelHandle::new("stub", "m"),
                tools: vec![].into(),
                tool_choice: ToolChoice::Auto,
                generation: GenerationConfig::default(),
                response_format: Default::default(),
                provider_options: serde_json::Value::Null,
                cache_tiers: serde_json::Value::Null,
                context_epoch: 0,
                renderer_version: 1,
                frozen_render_config: serde_json::Value::Null,
                auth_selection: None,
                tools_were_omitted: false,
                budget: None,
                context_limit: None,
                work_class: None,
                variant: None,
                service_tier: None,
                stop_when: Vec::new(),
                context_management: None,
            }),
            input: vec![].into(),
            origin: RunOrigin::Fresh,
            submission_id: Some(SubmissionId(sub.into())),
        }
    }

    fn ids(p: &[PendingSubmission]) -> Vec<String> {
        p.iter().map(|s| s.submission_id.0.clone()).collect()
    }

    #[test]
    fn pending_is_fifo_queued_minus_retracted_minus_consumed() {
        let recs = vec![
            queued("s1", "a"),
            queued("s2", "b"),
            queued("s3", "c"),
            retracted("s2"),      // s2 retracted
            consumed("r1", "s1"), // s1 consumed
        ];
        // Only s3 remains, and FIFO order is preserved.
        assert_eq!(ids(&pending_submissions(&recs)), vec!["s3"]);
    }

    /// Interrupts drain first, in WAL order among themselves, then plain entries in WAL order:
    /// P1, P2, I gives I, P1, P2; and I1, I2 behind P1 give I1, I2, P1. Retracted and consumed
    /// interrupts leave the order of the rest unchanged.
    #[test]
    fn interrupts_drain_ahead_of_plain_entries_in_wal_order() {
        let recs = vec![queued("p1", "a"), queued("p2", "b"), interrupt("i", "c")];
        assert_eq!(ids(&pending_submissions(&recs)), vec!["i", "p1", "p2"]);

        let recs = vec![
            queued("p1", "a"),
            interrupt("i1", "b"),
            interrupt("i2", "c"),
        ];
        assert_eq!(ids(&pending_submissions(&recs)), vec!["i1", "i2", "p1"]);

        let recs = vec![
            queued("p1", "a"),
            interrupt("i1", "b"),
            queued("p2", "c"),
            interrupt("i2", "d"),
            consumed("r1", "i1"),
        ];
        assert_eq!(ids(&pending_submissions(&recs)), vec!["i2", "p1", "p2"]);

        // The live insertion rule gives the same order as the rebuild.
        let mut live = Vec::new();
        for record in &recs[..4] {
            let pending = pending_submissions(std::slice::from_ref(record));
            insert_in_drain_order(&mut live, pending.into_iter().next().unwrap());
        }
        assert_eq!(ids(&live), vec!["i1", "i2", "p1", "p2"]);
    }

    #[test]
    fn order_independent_set_algebra() {
        // Retracted/consumed BEFORE the Queued (a weird but legal WAL order) → same set.
        let recs = vec![
            retracted("s2"),
            consumed("r1", "s1"),
            queued("s1", "a"),
            queued("s2", "b"),
            queued("s3", "c"),
        ];
        assert_eq!(ids(&pending_submissions(&recs)), vec!["s3"]);
    }

    #[test]
    fn retract_decision_cases() {
        let recs = vec![queued("s1", "a"), queued("s2", "b"), consumed("r1", "s2")];
        // s1 pending → Retract.
        assert_eq!(
            retract_decision(&recs, &SubmissionId("s1".into())),
            RetractDecision::Retract
        );
        // s2 consumed → AlreadyStarted.
        assert_eq!(
            retract_decision(&recs, &SubmissionId("s2".into())),
            RetractDecision::AlreadyStarted
        );
        // unknown → NotPending.
        assert_eq!(
            retract_decision(&recs, &SubmissionId("nope".into())),
            RetractDecision::NotPending
        );
    }

    #[test]
    fn consumed_wins_over_a_present_queued() {
        // Both a Queued AND a RunStarted for the same id: consumed wins (a retract is
        // too late once the run exists).
        let recs = vec![queued("s1", "a"), consumed("r1", "s1")];
        assert!(pending_submissions(&recs).is_empty());
        assert_eq!(
            retract_decision(&recs, &SubmissionId("s1".into())),
            RetractDecision::AlreadyStarted
        );
    }
}
