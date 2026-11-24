//! The durable submission queue (Phase 2, B3).
//!
//! A submission is one prompt injection into a session. While a run is active, a new
//! prompt is durably QUEUED (`WalRecord::Queued{submission_id, message}`); it can be
//! RETRACTED (`Retracted{submission_id}`) while still pending, or CONSUMED into a run
//! (the coordinator writes `RunStarted{submission_id}`). This module derives the PENDING
//! set and the retract decision PURELY from the durable log records — no in-memory queue,
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
//! (it's not in pending). Order of `Queued`/`Retracted`/`RunStarted` in the log doesn't
//! change the final set; the set is order-independent.

use broca_protocol::{SubmissionId, SubmissionRequest};
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
    pub hooks: Option<Box<broca_records::record::Hooks>>,
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
    /// starts, so the log keeps the key on the record that carries the message even when the
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

impl PendingSubmission {}
