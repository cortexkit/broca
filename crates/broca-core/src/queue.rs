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
    /// Display marks are retained in admission checkpoints as well as their Queued source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_marks: Option<broca_protocol::MessageMarks>,
    /// Display provenance is restored from the Queued record because checkpoint
    /// serialization omits it; sender labels must not change admission identities.
    #[serde(skip)]
    pub display_sent_by: Option<broca_protocol::BindPrincipal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admitted_at_ms: Option<u64>,
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
            message_marks,
            sent_by: display_sent_by,
            written_at_ms,
            ts_ms,
            keep_warm: _,
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
                    message_marks: message_marks.clone(),
                    display_sent_by: display_sent_by.clone(),
                    admitted_at_ms: written_at_ms.or(*ts_ms),
                    hooks: hooks.clone(),
                    conversation_key: conversation_key.clone(),
                    submission_id: submission_id.clone(),
                    request: request.as_ref().clone(),
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
            turn_system: false,
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

    fn interrupt(id: &str, text: &str) -> WalRecord {
        queued_as(id, text, Some(broca_protocol::Delivery::Interrupt))
    }

    fn queued_as(id: &str, text: &str, delivery: Option<broca_protocol::Delivery>) -> WalRecord {
        WalRecord::Queued {
            message_marks: None,
            sent_by: None,
            written_at_ms: None,
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
                message_marks: Default::default(),
                account_route: None,
                route_decision_id: None,
                service_tier_level: None,
                staged_uploads: None,
                content: Vec::new(),
                directives: None,
                on_restart: None,
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
                client_message_id: None,
            }),
            request_identity_digest: None,
            principal: None,
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
            message_marks: None,
            compaction_plan_version: None,
            sent_by: None,
            written_at_ms: None,
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
                global_system: None,
                account_route: None,
                route_decision_id: None,
                served_provider: None,
                account_limit_pause: false,
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
}
