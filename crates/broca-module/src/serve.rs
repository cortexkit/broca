//! The session/run SERVE surface — canonical Rust request/response types + a
//! transport-agnostic dispatch trait.
//!
//! This is the `session.*` / `run.*` management surface from `docs/module-surface.md`,
//! expressed as Rust types and ONE dispatch trait ([`ServeHandler`]). It is
//! TRANSPORT-AGNOSTIC: it knows nothing about subc frames or the wire. The subc serve
//! binary (a thin adapter in `broca-subc`, against the published subc-protocol/transport,
//! mirroring the other Rust modules' hand-rolled serve loops) decodes an inbound route
//! request into one of these, calls the handler, and encodes the response back. The
//! dispatch LOGIC lives here so it is unit/integration-testable without a live daemon —
//! the same layering as `broca-core` (transport-agnostic loop) + `broca-subc` (the only
//! subc-coupled crate).
//!
//! [`LineageRegistry`](crate::LineageRegistry) implements [`ServeHandler`] by routing each
//! request to the per-session actor (the registry's reopen-retry methods). The serve types
//! are deliberately a thin mapping over the actor outcomes — they're the wire-facing
//! shape, kept separate so the wire contract and the internal command enums can evolve
//! independently.

pub mod admission_refusal;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::fanout::{Delivery, FromCursor};
use crate::registry::{LineageKey, OpenError};

/// Shared refusal spelling for wire adapters of this management surface.
pub use cortexkit_role_llm_runner::errors::PRE_USER_UNAVAILABLE;
pub use cortexkit_role_llm_runner::errors::{ReplyTooLargeDetail, ReplyTooLargeSubject};

/// A `session.read` request: a paged read over the durable assembled transcript.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionReadRequest {
    pub late_after: Option<String>,
    pub session: LineageKey,
    pub from_ordinal: Option<u64>,
    pub limit: Option<u64>,
    /// Also return the tool list frozen on the latest episode's `RunStarted`.
    pub include_tools: bool,
    pub include_rich_ui: bool,
    pub include_display_facts: bool,
    /// Continue after a mid only while the history snapshot is unchanged.
    pub after_mid: Option<String>,
    pub history_version: Option<u64>,
    /// Internal migration input; ignored by serving.
    pub lineage_id: Option<String>,
    /// The page's byte cap; `None` is the default cap.
    pub max_bytes: Option<u64>,
    /// `blocks` exposes stored media; `descriptors` serves payload refs and content identities.
    pub media: cortexkit_role_llm_runner::read::MediaMode,
    /// `tool_media: "refs"` on a `media_refs` read: tool results with media keep their parts.
    pub tool_media: bool,
}

impl SessionReadRequest {
    /// A read of `session` from `from_ordinal` (the newest page when `None`), leaving
    /// `after_mid`, `lineage_id` and `max_bytes` unset.
    pub fn new(session: LineageKey, from_ordinal: Option<u64>, limit: Option<u64>) -> Self {
        Self {
            late_after: None,
            session,
            from_ordinal,
            limit,
            include_tools: false,
            include_rich_ui: false,
            include_display_facts: false,
            after_mid: None,
            history_version: None,
            lineage_id: None,
            max_bytes: None,
            media: cortexkit_role_llm_runner::read::MediaMode::Inline,
            tool_media: false,
        }
    }
}

fn json_bytes(value: &impl Serialize) -> u64 {
    broca_protocol::serialized_json_bytes(value)
}

// Optional cursor members are omitted entirely when absent. Include the comma, quoted key,
// and colon when adjusting the measured reply after a cursor changes.
fn cursor_bytes<T: Serialize>(name: &str, cursor: &Option<T>) -> u64 {
    cursor
        .as_ref()
        .map_or(0, |value| name.len() as u64 + 4 + json_bytes(value))
}

/// Budget the completed wire reply, including attribution, tools, approval metadata and cursors.
/// Ranges keep their oldest messages; tails keep their newest. Late entries share the same
/// budget, not a second budget of their own. Keep at least one message and one late entry when
/// present: atomic oversized items or the fixed metadata can still exceed the budget.
pub(crate) fn reply_frame_limit_refusal(
    bytes: u64,
    subject: Option<ReplyTooLargeSubject>,
) -> Option<ServeError> {
    if bytes <= broca_wire::SUBC_MAX_FRAME_BYTES {
        return None;
    }
    subject.map(|subject| {
        ServeError::ReplyTooLarge(ReplyTooLargeDetail::new(
            bytes,
            broca_wire::SUBC_MAX_FRAME_BYTES,
            subject,
        ))
    })
}

pub(crate) fn cap_page_bytes(
    page: &mut SessionReadResponse,
    cap: u64,
    tail: bool,
    session: &str,
) -> Result<(), ServeError> {
    let mut total = json_bytes(page);
    let mut start = 0;
    let mut end = page.messages.len();
    while total >= cap && end - start > 1 {
        let removed = if tail {
            start += 1;
            &page.messages[start - 1]
        } else {
            end -= 1;
            total =
                total.saturating_sub(cursor_bytes("next_from_ordinal", &page.next_from_ordinal));
            page.next_from_ordinal = Some(page.messages[end - 1].ordinal + 1);
            total =
                total.saturating_add(cursor_bytes("next_from_ordinal", &page.next_from_ordinal));
            &page.messages[end]
        };
        // The array still has an item, so removing an item also removes one comma.
        total = total.saturating_sub(json_bytes(removed).saturating_add(1));
    }
    page.messages.truncate(end);
    page.messages.drain(..start);

    if let Some(late) = &mut page.late_approvals {
        while total >= cap && late.len() > 1 {
            let removed = late.pop().expect("more than one late entry");
            total = total.saturating_sub(json_bytes(&removed).saturating_add(1));
            total = total.saturating_sub(cursor_bytes("next_late_after", &page.next_late_after));
            page.next_late_after = late.last().map(|entry| entry.call_key.clone());
            total = total.saturating_add(cursor_bytes("next_late_after", &page.next_late_after));
        }
    }
    let frame_bytes = broca_wire::serialized_result_frame_bytes(page);
    let subject = page
        .messages
        .first()
        .map(|message| ReplyTooLargeSubject::Message {
            session: session.to_owned(),
            ordinal: message.ordinal,
        });
    if let Some(error) = reply_frame_limit_refusal(frame_bytes, subject) {
        return Err(error);
    }
    Ok(())
}

#[cfg(test)]
#[path = "serve/read_page_tests.rs"]
mod read_page_tests;

/// Where a `session.subscribe` attaches from (the wire spelling: `"start" | "live" |
/// cursor`). `live` attaches strictly after the durable head captured by the attach barrier,
/// remaining quiet and open until future units arrive regardless of the current lineage state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SubscribeFrom {
    /// From the very start of the lineage (seq 1).
    Start,
    /// Strictly after the durable head captured by the attach barrier.
    Live,
    /// Strictly after a durable cursor (a resume from the client's last-processed cursor).
    After { wal_seq: u64, sub_index: u32 },
    /// A wire cursor bound to a log incarnation and the original frame bytes.
    Resume { cursor: broca_wire::CursorWire },
}

/// A `session.import` request carrying canonical history only.
#[derive(Clone)]
pub struct SessionImportRequest {
    pub project_id: Option<String>,
    /// The principal the daemon stamped on the importing bind; recorded on the RunStarted.
    pub principal: Option<broca_protocol::BindPrincipal>,
    /// What the importing route says about scopes. An import seeds a session's first episode,
    /// so a session whose address an owner's scoped bind reserved accepts it only over a route
    /// stamped with that owner's reserving scope ref. `Internal` (a caller inside broca) takes no
    /// such check.
    pub scope: broca_protocol::SendScope,
    pub session: LineageKey,
    pub run_id: broca_protocol::RunId,
    pub messages: Vec<broca_protocol::Message>,
    pub source_ref: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionImportResponse {
    pub run_id: String,
    pub synthesized_tool_results: usize,
    pub dropped_orphan_tool_results: usize,
    /// System messages the import discarded. `None` means the server did not report the
    /// count, not that none were dropped -- a server predating this field still discarded
    /// them, so defaulting to zero here would turn "unknown" into a false all-clear.
    #[serde(default)]
    pub dropped_system_messages: Option<usize>,
    pub already_imported: bool,
}

/// A `session.send` request: start a run now (idle lineage) or durably queue new work.
#[derive(Clone)]
pub struct SendRequest {
    pub session_class: Option<String>,
    pub retention: Option<u64>,
    /// Routing affinity for this send's admission, never frozen into run data.
    pub conversation_key: Option<String>,
    pub session: LineageKey,
    /// Identity assigned by the serve adapter. It is kept separate from the submission payload
    /// because the payload may preserve an omitted tool surface until admission.
    pub run_id: broca_protocol::RunId,
    /// The caller's `send_id` when `run_id` was derived from one. Such a send is deduplicated
    /// against earlier sends with the same key, and the key is written on the WAL record that
    /// carries its message. `None` means a nonce-minted run id, which cannot repeat and so skips
    /// the identity digest and lookup.
    pub send_id: Option<String>,
    /// The caller's human-readable title, recorded on the WAL record this send writes. Not part
    /// of the send's identity, so a retry with another title is answered as the first send was.
    pub title: Option<String>,
    /// The caller's request payload. An omitted `tools` value remains durable if this send is
    /// queued and resolves only when the episode starts.
    pub request: Box<broca_protocol::request::SubmissionRequest>,
    /// Skip the derived transcript projection for this run. WAL durability and reads are unchanged.
    pub skip_materialization: bool,
    pub kind: crate::executor::RunKind,
    /// How the send asked to be delivered when a run is already active (`Queue` when the
    /// caller said nothing). Part of the send's identity: a retry with the same `send_id` and a
    /// different mode is refused `send_id_reuse` naming `delivery`.
    pub delivery: broca_protocol::Delivery,
    /// Whether the wire send explicitly named `generation.stop_sequences`, including `[]`.
    /// An empty normalized vector alone cannot distinguish clearing from omission.
    pub stop_sequences_named: bool,
    /// The fetch plan the send carried: its identity, and for a session the serve layer found
    /// with no episode, the manifest admission built from it.
    pub plan: Option<crate::actor::SendPlan>,
    /// The send's `delivery_order` group (an ordered `delivery` list, a `dedupe_key` or an
    /// expiry), or `None` for a send that uses none of it and so behaves exactly as before.
    pub order: Option<broca_protocol::DeliveryOrder>,
}

/// Where a session stands with respect to fetch plans, as its durable history says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanState {
    /// The history cannot be read without the session's actor; the actor decides.
    Unknown,
    /// No admitting episode yet (possibly only an import seed): a plan is fetched and frozen.
    NotAdmitted,
    /// The session has episodes. `frozen` is its plan's identity, `None` when it was admitted
    /// without a plan.
    Admitted { frozen: Option<String> },
}

/// Why a `session.refresh`, `session.refresh_policy` or `session.flush_prefix` is refused,
/// as the wire reply's code, message and detail. Every refusal writes nothing.
#[derive(Debug, Clone, PartialEq)]
pub enum SessionChangeError {
    /// The caller is not the session's owner.
    OwnerMismatch(Box<broca_core::OwnerMismatch>),
    /// The request is malformed, naming the field.
    InvalidParams { field: String, message: String },
    /// A `send_id` was reused with a different body, naming the differing field.
    SendIdReuse { field: String, message: String },
    /// The policy names no rung the session supports for a surface the change touches.
    RungUnsupported { surface: String, rung: String },
    /// The generation is at or below the accepted one and is not an identical repeat.
    GenerationStale { accepted: u64 },
    /// The policy names a generation no longer pending because a later refresh superseded it.
    PendingSuperseded { generation: u64, pending: u64 },
    /// The policy names a generation already applied.
    AlreadyApplied { generation: u64, applied_by: String },
    /// Fetching the refreshed plan's providers failed as at admission.
    FetchRefused(broca_fetch_plan::Refusal),
    /// A refresh would add a compaction provider to a session that can gain one, but the
    /// session or the plan can't take it: `invalid_params` on `plan.compaction_item`, its
    /// detail naming the reason (`broca_session_loop::enroll::Refusal::reason`).
    CompactionNotAddable(broca_session_loop::enroll::Refusal),
    /// The session has no baseline yet, or the writer failed. Retryable when transient.
    Transient(String),
    /// The op is not served (no session-change state, or the handler cannot tell).
    Unsupported(String),
}

impl SessionChangeError {
    pub fn unsupported(op: impl Into<String>) -> Self {
        Self::Unsupported(op.into())
    }

    /// The wire code.
    pub fn code(&self) -> &'static str {
        match self {
            Self::OwnerMismatch(_) => cortexkit_role_llm_runner::errors::SCOPE_OWNER_MISMATCH,
            Self::InvalidParams { .. } => cortexkit_role_llm_runner::errors::INVALID_PARAMS,
            Self::SendIdReuse { .. } => cortexkit_role_llm_runner::errors::SEND_ID_REUSE,
            Self::RungUnsupported { .. } => cortexkit_role_llm_runner::errors::RUNG_UNSUPPORTED,
            Self::GenerationStale { .. } => cortexkit_role_llm_runner::errors::GENERATION_STALE,
            Self::PendingSuperseded { .. } => cortexkit_role_llm_runner::errors::PENDING_SUPERSEDED,
            Self::AlreadyApplied { .. } => cortexkit_role_llm_runner::errors::ALREADY_APPLIED,
            Self::FetchRefused(refusal) => refusal.code(),
            Self::CompactionNotAddable(_) => cortexkit_role_llm_runner::errors::INVALID_PARAMS,
            Self::Transient(_) => cortexkit_role_llm_runner::errors::TRANSIENT,
            Self::Unsupported(_) => "open_failed",
        }
    }

    /// Whether the same request may succeed when retried unchanged.
    pub fn retryable(&self) -> bool {
        match self {
            Self::FetchRefused(refusal) => refusal.retryable(),
            Self::Transient(_) => true,
            _ => cortexkit_role_llm_runner::errors::is_retryable(self.code()),
        }
    }
}

/// The detail of a [`SessionChangeError::CompactionNotAddable`] reply: the field, as the
/// refusal of a changed or removed compaction item names it, and the reason's stable name.
pub fn compaction_not_addable_detail(
    refusal: broca_session_loop::enroll::Refusal,
) -> serde_json::Value {
    serde_json::json!({"field": "plan.compaction_item", "reason": refusal.reason()})
}

/// What a refresh that adds a compaction provider is answered once the context limit of its
/// run's `model` was resolved (`resolved`): accepted with a limit; refused `no_context_limit`
/// when resolution gave none; refused transiently, with its cause, when resolution couldn't run.
/// Only the resolved limit counts: a refresh carries no `context_limit` of its own, and a later
/// send's can't be counted on.
pub(crate) fn context_limit_outcome(
    resolved: Result<Option<u64>, String>,
    model: &cortexkit_role_llm_runner::head_selection::PlanModel,
) -> Result<(), SessionChangeError> {
    match resolved {
        Ok(Some(_)) => Ok(()),
        Ok(None) => Err(SessionChangeError::CompactionNotAddable(
            broca_session_loop::enroll::Refusal::NoContextLimit,
        )),
        Err(cause) => Err(SessionChangeError::Transient(format!(
            "the context limit of {}/{} couldn't be resolved: {cause}",
            model.provider, model.model
        ))),
    }
}

/// What a plan-change op's preflight reads about the session before it checks the request: who
/// owns the session, the plan opener of its scoped runs, and its plan-change facts for the
/// request's generation and `send_id`.
#[derive(Debug, Clone, Default)]
pub(crate) struct ChangeRead {
    /// The scope and principal the session's first run froze, which name its owner; `None`
    /// before the session's first run.
    pub first_run: Option<(
        Option<broca_protocol::SessionScope>,
        Option<broca_protocol::BindPrincipal>,
    )>,
    /// The plan opener the latest scoped run that names one names (`scope_plan_opener`).
    pub opener: Option<String>,
    pub facts: crate::plan_change::ChangeFacts,
}

impl ChangeRead {
    /// The caller's refusal when it isn't the session's owner, or when the session has no run
    /// yet and so no baseline to change.
    fn check_owner(
        &self,
        principal: Option<&broca_protocol::BindPrincipal>,
        scope: &broca_protocol::SendScope,
    ) -> Result<(), SessionChangeError> {
        let Some((frozen, first)) = &self.first_run else {
            return Err(SessionChangeError::Transient(
                "the session has no baseline yet; send its first send first".into(),
            ));
        };
        match owner_mismatch(frozen.as_ref(), first.as_ref(), principal, scope) {
            Some(mismatch) => Err(SessionChangeError::OwnerMismatch(Box::new(mismatch))),
            None => Ok(()),
        }
    }
}

/// A `session.send` response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SendResponse {
    Paused {
        run_id: String,
        pause_reason: broca_protocol::PauseReason,
        /// As on `Active`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delivered: Option<cortexkit_role_llm_runner::send::Delivered>,
    },
    /// The run started immediately (the lineage was idle).
    Active {
        run_id: String,
        /// A retried steer's delivery receipt, once a model step has rendered it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delivered: Option<cortexkit_role_llm_runner::send::Delivered>,
    },
    Finished {
        run_id: String,
        reason: broca_protocol::RunFinishReason,
        /// As on `Active`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delivered: Option<cortexkit_role_llm_runner::send::Delivered>,
    },
    /// A run is active; this prompt was durably queued. The `submission_id` lets the
    /// caller later retract it.
    Pending { submission_id: String },
}

/// Why a new message was refused before anything was written: the session's last run is paused.
#[derive(Debug, Clone, PartialEq)]
pub struct RunPausedRefusal {
    /// The paused run.
    pub run_id: String,
    /// The pause reason as the WAL records it.
    pub reason: broca_protocol::PauseReason,
}

/// A `session.retract` response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetractResponse {
    Retracted,
    /// Too late — the submission was already consumed into a run.
    AlreadyStarted,
    /// No such pending submission.
    NotPending,
}

/// A `run.cancel` response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancelResponse {
    Ack,
    /// The run is not the active one (already finished, or never the active run_id).
    NotActive,
}

/// A `run.status` / `session.status` response — the lineage's current state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum StatusResponse {
    Idle,
    Active { run_id: String, step: u64 },
}

pub use broca_wire::{
    CursorWire, RunStatusResponse, SessionReadLineageState, SessionReadMessage,
    SessionReadResponse, SessionReadTool,
};

/// Shows each message's media as the read's `media` mode asks: `blocks` keeps stored media
/// references, and `descriptors` serves large payloads as refs with content identities.
pub(crate) fn apply_media_mode(
    response: &mut SessionReadResponse,
    media: cortexkit_role_llm_runner::read::MediaMode,
    tool_media: bool,
    session: &str,
) -> Result<(), ServeError> {
    if media == cortexkit_role_llm_runner::read::MediaMode::Blocks {
        for entry in &mut response.messages {
            entry.show_media_blocks_with_identity();
        }
    }
    if media == cortexkit_role_llm_runner::read::MediaMode::MediaRefs {
        for entry in &mut response.messages {
            if tool_media {
                entry.show_media_refs_with_tool_media();
            } else {
                entry.show_media_refs();
            }
        }
    }
    if media == cortexkit_role_llm_runner::read::MediaMode::Descriptors {
        for entry in &mut response.messages {
            // Only structure that is not payload can outgrow the per-message budget.
            if let Err(ordinal) = entry.describe() {
                return Err(ServeError::ReplyTooLarge(ReplyTooLargeDetail::new(
                    json_bytes(entry),
                    cortexkit_role_llm_runner::read::descriptors::DESCRIPTOR_MESSAGE_MAX as u64,
                    ReplyTooLargeSubject::Message {
                        session: session.to_owned(),
                        ordinal,
                    },
                )));
            }
        }
    }
    Ok(())
}

/// Fills each message's `tool_calls`, and a tool result delivered as a send's carrier call key,
/// from the dispatch facts of the tool turns the messages belong to, keyed by the 0-based ordinal
/// of each turn's assistant message (`broca_core::dispatches_of_turns`). `lineage_id` is the
/// log's, which a call's derived key names.
pub fn apply_tool_call_dispatches(
    response: &mut SessionReadResponse,
    dispatches: &std::collections::BTreeMap<u64, Vec<broca_core::ToolCallDispatch>>,
    lineage_id: &str,
) {
    let calls_of = |message: &broca_protocol::Message| -> Vec<String> {
        message
            .content
            .iter()
            .filter_map(|block| match block {
                broca_protocol::content::ContentBlock::ToolCall(call) => {
                    Some(call.tool_call_id.0.clone())
                }
                _ => None,
            })
            .collect()
    };
    for message in &mut response.messages {
        if let Some(carrier) = message
            .run
            .as_ref()
            .and_then(|run| run.delivery.as_ref())
            .and_then(|delivery| delivery.tool_call_id.as_ref())
        {
            if let Some(dispatch) = message
                .ordinal
                .checked_sub(2)
                .and_then(|ordinal| dispatches.get(&ordinal))
                .and_then(|calls| calls.iter().find(|call| &call.tool_call_id == carrier))
            {
                if let Some(receipt) = &mut message.delivered_as {
                    receipt.call_key = dispatch.key_seq.or(dispatch.intent_seq).and_then(|seq| {
                        broca_core::tool_call_key(
                            Some(lineage_id),
                            seq,
                            dispatch.recorded_call_key.as_deref(),
                        )
                    });
                }
            }
        }
        let ids = calls_of(&message.message);
        if ids.is_empty() {
            continue;
        }
        message.tool_calls = match dispatches.get(&message.ordinal.saturating_sub(1)) {
            // The step that produced this message holds exactly these calls: report them.
            Some(found) if found.iter().map(|d| &d.tool_call_id).eq(ids.iter()) => found
                .iter()
                .map(|dispatch| {
                    let mut call = broca_wire::SessionReadToolCall::new(&dispatch.tool_call_id);
                    call.call_key = dispatch.key_seq.or(dispatch.intent_seq).and_then(|seq| {
                        broca_core::tool_call_key(
                            Some(lineage_id),
                            seq,
                            dispatch.recorded_call_key.as_deref(),
                        )
                    });
                    call.dispatched_to = dispatch.dispatched_to.clone();
                    call.indeterminate = dispatch.indeterminate;
                    call
                })
                .collect(),
            // No recorded step produced this message (an imported one, say): report each
            // call with no dispatch fact rather than guess one.
            _ => ids
                .into_iter()
                .map(broca_wire::SessionReadToolCall::new)
                .collect(),
        };
    }
}

/// The creation and completion times of a page's messages, by zero-based ordinal, kept for an
/// `include_display_facts` read before the page becomes a reply (which serves neither without
/// the opt-in).
#[derive(Debug, Clone, Default)]
pub struct DisplayTimes {
    creations: std::collections::BTreeMap<u64, u64>,
    completions: std::collections::BTreeMap<u64, u64>,
}

impl DisplayTimes {
    pub fn of(page: &broca_core::SessionReadPage) -> Self {
        Self {
            creations: page
                .messages
                .iter()
                .filter_map(|entry| entry.created_at.map(|at| (entry.ordinal, at)))
                .collect(),
            completions: page
                .messages
                .iter()
                .filter_map(|entry| entry.completed_at.map(|at| (entry.ordinal, at)))
                .collect(),
        }
    }
}

/// Reasoning summaries recorded for each message, by zero-based ordinal.
pub type RecordedSummaries =
    std::collections::BTreeMap<u64, Vec<cortexkit_role_llm_runner::read::ReasoningSummary>>;

/// What `include_display_facts` adds to each message of `response`, the same for both storages:
/// its creation time; for an assistant message its completion time and reasoning summaries; the
/// sender, message marks (`speaks_for`, `notice`), notices and per-call times `facts` holds for it (by zero-based ordinal), and for a
/// user message its send's `client_message_id`. A message that makes tool calls and has no
/// per-call facts names each call with no times.
///
/// Today's storage reads reasoning summaries from the message's reasoning blocks (`recorded` is
/// `None`); the new storage serves the ones it recorded when it placed the message, as a seal
/// prunes the blocks they are read from.
pub fn apply_display_facts(
    response: &mut SessionReadResponse,
    times: &DisplayTimes,
    facts: &std::collections::BTreeMap<u64, broca_core::display_facts::DisplayFacts>,
    recorded: Option<&RecordedSummaries>,
) {
    for entry in &mut response.messages {
        entry.created_at = times.creations.get(&(entry.ordinal - 1)).copied();
        if entry.message.role == broca_protocol::Role::Assistant {
            entry.completed_at = times.completions.get(&(entry.ordinal - 1)).copied();
            entry.reasoning_summaries = match recorded {
                Some(recorded) => recorded
                    .get(&(entry.ordinal - 1))
                    .cloned()
                    .unwrap_or_default(),
                None => entry
                    .message
                    .content
                    .iter()
                    .enumerate()
                    .filter_map(|(part, block)| {
                        let stored = serde_json::to_value(block).ok()?;
                        let text = cortexkit_role_llm_runner::display::reasoning_summary(&stored)?;
                        Some(cortexkit_role_llm_runner::read::ReasoningSummary::new(
                            u32::try_from(part).ok()?,
                            text,
                        ))
                    })
                    .collect(),
            };
        }
        if let Some(facts) = facts.get(&(entry.ordinal - 1)) {
            if let Some(marks) = &facts.message_marks {
                entry.speaks_for = marks.speaks_for.clone();
                entry.notice = marks.notice;
            }
            entry.sender = facts.sender.clone();
            entry.send_id = facts.send_id.clone().filter(|_| entry.sender.is_some());
            entry.notices = facts
                .notices
                .iter()
                .map(|notice| notice.fact.clone())
                .collect();
            if entry.message.role == broca_protocol::Role::User {
                entry.client_message_id = facts.client_message_id.clone();
            }
            entry.tool_times = facts.tool_times.clone();
        }
        if entry.tool_times.is_empty() {
            entry.tool_times = entry
                .message
                .content
                .iter()
                .filter_map(|block| match block {
                    broca_protocol::ContentBlock::ToolCall(call) => Some(
                        cortexkit_role_llm_runner::read::ToolTime::new(&call.tool_call_id.0),
                    ),
                    _ => None,
                })
                .collect();
        }
    }
}

/// The runs whose Rich UI stamps an `include_rich_ui` read of `response` needs: the producing
/// run of each assistant message that names one, each once, in page order.
pub fn rich_ui_runs(response: &SessionReadResponse) -> Vec<String> {
    let mut runs: Vec<String> = Vec::new();
    for entry in &response.messages {
        if entry.message.role != broca_protocol::Role::Assistant {
            continue;
        }
        if let Some(run) = &entry.run {
            if !runs.contains(&run.run_id) {
                runs.push(run.run_id.clone());
            }
        }
    }
    runs
}

/// Stamps each assistant message of `response` with its producing run's Rich UI stamp, from
/// `stamps` (by run id; a run it doesn't name has none).
pub fn apply_rich_ui(
    response: &mut SessionReadResponse,
    stamps: &std::collections::BTreeMap<
        String,
        Option<cortexkit_role_llm_runner::rich_ui::RichUiStamp>,
    >,
) {
    for entry in &mut response.messages {
        if entry.message.role != broca_protocol::Role::Assistant {
            continue;
        }
        let Some(run) = &entry.run else { continue };
        entry.rich_ui = stamps.get(&run.run_id).cloned().flatten();
    }
}

pub fn session_read_response(
    page: broca_core::SessionReadPage,
    tools: Option<crate::registry::SessionReadTools>,
) -> SessionReadResponse {
    let (tools, tools_run_id) = match tools {
        Some(tools) => (
            Some(tools.tools.iter().map(SessionReadTool::from).collect()),
            Some(tools.run_id.0),
        ),
        None => (None, None),
    };
    SessionReadResponse {
        history_version: 0,
        incarnation: page
            .lineage_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "unwritten".into()),
        next_late_after: None,
        messages: page
            .messages
            .into_iter()
            .map(|message| SessionReadMessage {
                send_id: None,
                speaks_for: None,
                notice: false,
                sender: None,
                notices: Vec::new(),
                created_at: None,
                completed_at: None,
                reasoning_summaries: Vec::new(),
                client_message_id: None,
                tool_times: Vec::new(),
                rich_ui: None,
                model_fallback: None,
                written_at: message.written_at,
                delivered_as: message
                    .run
                    .as_ref()
                    .and_then(|run| run.delivery.as_ref())
                    .map(|delivery| {
                        cortexkit_role_llm_runner::delivery_order::EntryDelivery::new(delivery.mode)
                    }),
                ordinal: message.ordinal + 1,
                mid: format!("m{}", message.ordinal),
                part_keys: Some(broca_wire::part_keys(message.ordinal, &message.message)),
                message: message.message,
                run: message.run,
                tool_calls: Vec::new(),
                described: None,
                digest: None,
                part_digests: None,
            })
            .collect(),
        next_from_ordinal: page.next_from_ordinal.map(|position| position + 1),
        head: page.head.map(CursorWire::from),
        lineage_id: page.lineage_id.map(|lineage_id| lineage_id.to_string()),
        lineage_state: SessionReadLineageState {
            last_run_id: page.lineage_state.last_run_id.map(|run_id| run_id.0),
            state: page.lineage_state.state.as_str().to_string(),
            reason: page
                .lineage_state
                .reason
                .map(|reason| reason.as_str().to_string()),
            error: page.lineage_state.error.map(Into::into),
            usage: page.lineage_state.usage,
        },
        tools,
        tools_run_id,
        // Deferred approvals were retired: no session records a deferred or late-approved call.
        deferred_calls: None,
        late_approvals: None,
        open_batch: None,
    }
}

/// The `send.status` reply for a send's recorded `outcome`. `baseline` is the baseline the
/// session's first run froze when that run is the send's own run `own_run`; it is read only when
/// the reply names that run.
pub(crate) fn send_status_reply(
    outcome: crate::actor::SendOutcome,
    own_run: &broca_protocol::RunId,
    baseline: impl FnOnce() -> Option<broca_wire::BaselineWire>,
) -> Result<broca_wire::SendReplyWire, ServeError> {
    let (result, delivered) = match outcome {
        crate::actor::SendOutcome::Finished {
            run_id,
            reason,
            delivered,
        } => (
            broca_wire::SendResult::Finished {
                run_id: run_id.0,
                reason: reason.into(),
            },
            delivered,
        ),
        crate::actor::SendOutcome::Active { run_id, delivered } => (
            broca_wire::SendResult::Active { run_id: run_id.0 },
            delivered,
        ),
        crate::actor::SendOutcome::Paused {
            run_id,
            reason,
            delivered,
        } => (
            broca_wire::SendResult::Paused {
                run_id: run_id.0,
                pause_reason: reason.into(),
            },
            delivered,
        ),
        crate::actor::SendOutcome::Pending { submission_id } => (
            broca_wire::SendResult::Pending {
                submission_id: submission_id.0,
            },
            None,
        ),
        crate::actor::SendOutcome::Retracted { submission_id } => {
            return Err(ServeError::SendRetracted {
                submission_id: submission_id.0,
            })
        }
        crate::actor::SendOutcome::SendEnded {
            submission_id,
            reason,
        } => {
            return Err(ServeError::SendEnded {
                submission_id: submission_id.0,
                outcome: reason.as_str(),
            })
        }
        crate::actor::SendOutcome::Error(message) => return Err(ServeError::Open(message, None)),
        _ => unreachable!("recorded send fate cannot admit work"),
    };
    let baseline = match &result {
        broca_wire::SendResult::Active { run_id }
        | broca_wire::SendResult::Finished { run_id, .. }
            if run_id == &own_run.0 =>
        {
            baseline()
        }
        _ => None,
    };
    Ok(broca_wire::SendReplyWire {
        result,
        baseline,
        delivered,
    })
}

/// The plan-change rungs this runner supports per surface, which every ready baseline states.
pub(crate) fn supported_rungs() -> cortexkit_role_llm_runner::baseline::Rungs {
    cortexkit_role_llm_runner::baseline::Rungs {
        tools: broca_fetch_plan::session_change::TOOLS_RUNGS
            .iter()
            .map(|rung| rung.to_string())
            .collect(),
        system_text: broca_fetch_plan::session_change::SYSTEM_TEXT_RUNGS
            .iter()
            .map(|rung| rung.to_string())
            .collect(),
    }
}

/// The baseline of a session admitted with a fetch plan, read from its frozen manifest alone.
pub(crate) fn planned_baseline(manifest: &broca_wal::PlanManifest) -> broca_wire::BaselineWire {
    let facts = broca_fetch_plan::baseline_facts(manifest);
    let json =
        |value: serde_json::Result<serde_json::Value>| value.expect("a baseline item is JSON");
    broca_wire::BaselineWire::planned(
        facts
            .items
            .iter()
            .map(|item| json(serde_json::to_value(item)))
            .collect(),
        facts
            .absent_items
            .iter()
            .map(|item| json(serde_json::to_value(item)))
            .collect(),
        facts.composition_digest,
        facts.tool_names,
    )
}

/// Why the caller is not the owner a session's first run recorded, if it is not: the owner of
/// the scope it froze, `frozen`, when the session is scoped, else the principal of its first
/// send, `first`. A session whose first run recorded neither has no owner to hold a caller to.
pub(crate) fn owner_mismatch(
    frozen: Option<&broca_protocol::SessionScope>,
    first: Option<&broca_protocol::BindPrincipal>,
    principal: Option<&broca_protocol::BindPrincipal>,
    scope: &broca_protocol::SendScope,
) -> Option<broca_core::OwnerMismatch> {
    match (frozen, first) {
        (Some(frozen), _) => {
            (principal != Some(&frozen.owner)).then(|| broca_core::OwnerMismatch::Scoped {
                frozen: frozen.clone(),
                presented_principal: principal.cloned(),
                presented_scope: match scope {
                    broca_protocol::SendScope::Stamped(stamp) => Some(stamp.scope.clone()),
                    _ => None,
                },
            })
        }
        (None, Some(first)) => {
            (principal != Some(first)).then(|| broca_core::OwnerMismatch::FirstPrincipal {
                first: first.clone(),
                presented_principal: principal.cloned(),
            })
        }
        (None, None) => None,
    }
}

/// A frozen automation's plan may grant sends only to the sender recorded in its
/// flow/run scope. Reserved principals use the module spelling carried by `plan.senders`,
/// not the owner spelling.
pub fn scope_plan_opener(scope: &broca_protocol::SessionScope) -> Option<String> {
    if scope.flow_id.is_none() && scope.run_id.is_none() {
        return None;
    }
    scope.sender.as_ref().map(|sender| match sender {
        broca_protocol::BindPrincipal::Reserved { module_id } => module_id.clone(),
        other => other.as_owner_string(),
    })
}

/// A run's final message's text, as `run.result` serves it: its text parts joined in order with
/// nothing inserted, its reasoning parts left out. `clean` says whether the run that wrote it
/// cleans the model's text of tag notation on every exposure.
pub(crate) fn joined_text(message: &broca_protocol::Message, clean: bool) -> String {
    message
        .content
        .iter()
        .filter_map(|block| match block {
            broca_protocol::content::ContentBlock::Text { text }
            | broca_protocol::content::ContentBlock::TextWithState { text, .. } => {
                Some(broca_protocol::tags::exposed_model_text(clean, text))
            }
            _ => None,
        })
        .collect()
}

/// The directives a completed run that asked for the `wake` directive reports: the one its final
/// message's text `text` ends with, if any.
pub(crate) fn wake_directives(
    text: &str,
) -> Option<Vec<cortexkit_role_llm_runner::directives::Directive>> {
    cortexkit_role_llm_runner::directives::parse_trailing(text)
        .map(|directive| vec![directive.into()])
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub(crate) struct RunToolCallsAnchor {
    file_id: u64,
    seq: u64,
    byte_end: u64,
}

impl From<broca_wal::WalByteAnchor> for RunToolCallsAnchor {
    fn from(anchor: broca_wal::WalByteAnchor) -> Self {
        Self {
            file_id: anchor.file_id,
            seq: anchor.seq,
            byte_end: anchor.byte_end,
        }
    }
}

impl From<RunToolCallsAnchor> for broca_wal::WalByteAnchor {
    fn from(anchor: RunToolCallsAnchor) -> Self {
        Self {
            file_id: anchor.file_id,
            seq: anchor.seq,
            byte_end: anchor.byte_end,
        }
    }
}

/// A continuation resumes at the next call's assistant record, not at the session's WAL head.
/// For a session kept in the new storage the seqs are the store's own, `lineage_id` names the
/// session's log incarnation, and no byte anchors are kept.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RunToolCallsCursor {
    version: u8,
    run_id: String,
    pub(crate) lineage_id: Option<String>,
    pub(crate) run_start_seq: u64,
    pub(crate) run_end_seq: u64,
    pub(crate) step_seq: u64,
    call_index: usize,
    pub(crate) batch_end_seq: u64,
    ordinal: u64,
    anchor_before: Option<RunToolCallsAnchor>,
    after_batch: Vec<RunToolCallsPosition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    authentication: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct RunToolCallsPosition {
    step_seq: u64,
    call_index: usize,
    batch_end_seq: u64,
    ordinal: u64,
    anchor_before: Option<RunToolCallsAnchor>,
}

#[derive(Clone, Debug)]
pub(crate) struct IndexedRunToolCall {
    pub(crate) call: cortexkit_role_llm_runner::tool_calls::RunToolCall,
    /// The seq of the record holding the assistant message that made the call.
    pub(crate) step_seq: u64,
    /// The call's position among the calls its step's batch lists.
    pub(crate) call_index: usize,
    /// The seq of the record that ended the call's batch, or of the run's last record while the
    /// batch is open.
    pub(crate) batch_end_seq: u64,
    pub(crate) ordinal: u64,
    pub(crate) after_batch: Vec<RunToolCallsPosition>,
}

#[derive(Clone, Copy)]
pub(crate) struct RunToolCallsPageContext<'a> {
    pub(crate) run_id: &'a str,
    pub(crate) lineage_id: Option<&'a str>,
    pub(crate) run_start_seq: u64,
    pub(crate) run_end_seq: u64,
    /// The WAL range the calls were read from, whose byte anchors a cursor keeps; `None` for a
    /// session kept in the new storage, whose cursors keep none.
    pub(crate) range: Option<&'a broca_wal::WalRangeRead>,
    pub(crate) range_start_seq: u64,
    pub(crate) fallback: Option<RunToolCallsAnchor>,
    pub(crate) seed: Option<&'a RunToolCallsCursor>,
}

pub(crate) fn run_tool_call_outcome(
    outcome: broca_wal::record::DispatchOutcome,
    has_intent: bool,
    reason: Option<&str>,
    is_error: bool,
) -> &'static str {
    use broca_wal::record::DispatchOutcome;
    use cortexkit_role_llm_runner::tool_calls::outcomes;

    match outcome {
        DispatchOutcome::Unknown => outcomes::INDETERMINATE,
        DispatchOutcome::MaxSteps | DispatchOutcome::Interrupted => outcomes::NOT_EXECUTED,
        DispatchOutcome::Completed if reason.is_some() || (!has_intent && is_error) => {
            outcomes::REFUSED
        }
        DispatchOutcome::Completed if is_error => outcomes::FAILED,
        DispatchOutcome::Completed => outcomes::SUCCEEDED,
    }
}

pub(crate) fn invalid_tool_call_cursor(message: &str) -> ServeError {
    ServeError::InvalidParams {
        field: cortexkit_role_llm_runner::tool_calls::AFTER_FIELD.into(),
        message: message.into(),
    }
}

/// Sign a cursor so callers can resume only from positions broca actually returned.
fn run_tool_calls_cursor_authentication(cursor: &RunToolCallsCursor) -> Result<String, ServeError> {
    use sha2::{Digest, Sha256};

    static KEY: std::sync::OnceLock<Result<[u8; 32], String>> = std::sync::OnceLock::new();
    let key = KEY.get_or_init(|| {
        let mut key = [0u8; 32];
        getrandom::fill(&mut key).map_err(|error| error.to_string())?;
        Ok(key)
    });
    let key = key.as_ref().map_err(|error| {
        ServeError::Open(
            format!("could not initialize run.tool_calls cursor authentication: {error}"),
            Some(broca_protocol::ErrorClass::Transient),
        )
    })?;
    let mut unsigned = cursor.clone();
    unsigned.authentication = None;
    let payload = serde_json::to_vec(&unsigned).map_err(|error| {
        ServeError::Open(format!("encode run.tool_calls cursor: {error}"), None)
    })?;

    let mut padded_key = [0u8; 64];
    padded_key[..key.len()].copy_from_slice(key);
    let mut inner_pad = padded_key;
    for byte in &mut inner_pad {
        *byte ^= 0x36;
    }
    let mut inner = Sha256::new();
    inner.update(inner_pad);
    inner.update(b"broca run.tool_calls cursor v1\0");
    inner.update(payload);
    let inner_digest = inner.finalize();

    let mut outer_pad = padded_key;
    for byte in &mut outer_pad {
        *byte ^= 0x5c;
    }
    let mut outer = Sha256::new();
    outer.update(outer_pad);
    outer.update(inner_digest);
    Ok(format!("{:x}", outer.finalize()))
}

fn run_tool_calls_cursor_is_authentic(cursor: &RunToolCallsCursor) -> Result<bool, ServeError> {
    let Some(provided) = cursor.authentication.as_deref() else {
        return Ok(false);
    };
    let expected = run_tool_calls_cursor_authentication(cursor)?;
    let mut difference = 0u8;
    for (left, right) in provided.as_bytes().iter().zip(expected.as_bytes()) {
        difference |= left ^ right;
    }
    Ok(provided.len() == expected.len() && difference == 0)
}

fn indexed_run_tool_call_position(
    entry: &IndexedRunToolCall,
    range: Option<&broca_wal::WalRangeRead>,
    range_start_seq: u64,
    fallback: Option<RunToolCallsAnchor>,
) -> RunToolCallsPosition {
    let anchor_before = if entry.step_seq == range_start_seq {
        fallback
    } else {
        range.and_then(|range| {
            entry
                .step_seq
                .checked_sub(1)
                .and_then(|seq| range.anchor_at(seq))
                .map(Into::into)
        })
    };
    RunToolCallsPosition {
        step_seq: entry.step_seq,
        call_index: entry.call_index,
        batch_end_seq: entry.batch_end_seq,
        ordinal: entry.ordinal,
        anchor_before,
    }
}

fn run_tool_calls_cursor_token(
    position: &RunToolCallsPosition,
    after_batch: &[RunToolCallsPosition],
    run_id: &str,
    lineage_id: Option<&str>,
    run_start_seq: u64,
    run_end_seq: u64,
) -> Result<String, ServeError> {
    let mut cursor = RunToolCallsCursor {
        version: 1,
        run_id: run_id.to_owned(),
        lineage_id: lineage_id.map(str::to_owned),
        run_start_seq,
        run_end_seq,
        step_seq: position.step_seq,
        call_index: position.call_index,
        batch_end_seq: position.batch_end_seq,
        ordinal: position.ordinal,
        anchor_before: position.anchor_before,
        after_batch: after_batch.to_vec(),
        authentication: None,
    };
    cursor.authentication = Some(run_tool_calls_cursor_authentication(&cursor)?);
    serde_json::to_string(&cursor)
        .map_err(|error| ServeError::Open(format!("encode run.tool_calls cursor: {error}"), None))
}

fn next_run_tool_calls_cursor(
    entries: &[IndexedRunToolCall],
    index: usize,
    context: &RunToolCallsPageContext<'_>,
) -> Result<Option<String>, ServeError> {
    if let Some(entry) = entries.get(index) {
        let position = indexed_run_tool_call_position(
            entry,
            context.range,
            context.range_start_seq,
            context.fallback,
        );
        return run_tool_calls_cursor_token(
            &position,
            &entry.after_batch,
            context.run_id,
            context.lineage_id,
            context.run_start_seq,
            context.run_end_seq,
        )
        .map(Some);
    }
    let Some(cursor) = context.seed else {
        return Ok(None);
    };
    let Some((position, after_batch)) = cursor.after_batch.split_first() else {
        return Ok(None);
    };
    run_tool_calls_cursor_token(
        position,
        after_batch,
        context.run_id,
        context.lineage_id,
        context.run_start_seq,
        context.run_end_seq,
    )
    .map(Some)
}

/// The most calls a `run.tool_calls` page of `request` holds.
pub(crate) fn tool_calls_limit(
    request: &cortexkit_role_llm_runner::tool_calls::ToolCallsRequest,
) -> usize {
    request
        .limit
        .unwrap_or(broca_core::SESSION_READ_DEFAULT_LIMIT)
        .min(broca_core::SESSION_READ_MAX_LIMIT) as usize
}

/// The cursor `token` names, refused unless it is one this process returned for the run
/// `run_id`.
pub(crate) fn checked_tool_calls_cursor(
    token: &str,
    run_id: &str,
) -> Result<RunToolCallsCursor, ServeError> {
    let cursor: RunToolCallsCursor = serde_json::from_str(token).map_err(|_| {
        invalid_tool_call_cursor("run.tool_calls.after is not a cursor returned for this run")
    })?;
    if cursor.version != 1
        || cursor.run_id != run_id
        || cursor.run_start_seq == 0
        || cursor.run_end_seq < cursor.step_seq
        || cursor.batch_end_seq < cursor.step_seq
        || cursor.batch_end_seq > cursor.run_end_seq
        || cursor.step_seq <= cursor.run_start_seq
        || cursor
            .anchor_before
            .is_some_and(|anchor| anchor.seq != cursor.step_seq - 1)
    {
        return Err(invalid_tool_call_cursor(
            "run.tool_calls.after is not a cursor returned for this run",
        ));
    }
    if !run_tool_calls_cursor_is_authentic(&cursor)? {
        return Err(invalid_tool_call_cursor(
            "run.tool_calls.after was not returned for this run",
        ));
    }
    Ok(cursor)
}

/// The `run.tool_calls` page of the run's calls `entries`: from the call the context's cursor
/// names when it has one, else from the run's first call, at most `limit` calls and the page
/// byte target, with the cursor of the next call when more follow.
pub(crate) fn run_tool_calls_answer(
    mut entries: Vec<IndexedRunToolCall>,
    limit: usize,
    context: &RunToolCallsPageContext<'_>,
) -> Result<cortexkit_role_llm_runner::tool_calls::ToolCallsPage, ServeError> {
    use cortexkit_role_llm_runner::tool_calls::ToolCallsPage;

    if let Some(cursor) = context.seed {
        let Some(index) = entries.iter().position(|entry| {
            entry.step_seq == cursor.step_seq && entry.call_index == cursor.call_index
        }) else {
            return Err(invalid_tool_call_cursor(
                "run.tool_calls.after does not name a call in this run",
            ));
        };
        entries.drain(..index);
        for entry in &mut entries {
            entry.after_batch = cursor.after_batch.clone();
        }
    } else {
        let mut future_batches = Vec::new();
        for entry in &entries {
            if future_batches
                .last()
                .is_none_or(|position: &RunToolCallsPosition| position.step_seq != entry.step_seq)
            {
                future_batches.push(indexed_run_tool_call_position(
                    entry,
                    context.range,
                    context.range_start_seq,
                    context.fallback,
                ));
            }
        }
        for entry in &mut entries {
            entry.after_batch = future_batches
                .iter()
                .filter(|position| position.step_seq > entry.step_seq)
                .cloned()
                .collect();
        }
    }

    let run_id = context.run_id.to_owned();
    let max_bytes = broca_wire::role::READ_DEFAULT_MAX_BYTES;
    let mut page_calls = Vec::new();
    let mut consumed = 0usize;
    while consumed < entries.len() && page_calls.len() < limit {
        let entry = &entries[consumed];
        let mut candidate_calls = page_calls.clone();
        candidate_calls.push(entry.call.clone());
        let mut candidate = ToolCallsPage::new(run_id.clone(), candidate_calls);
        if let Some(next) = next_run_tool_calls_cursor(&entries, consumed + 1, context)? {
            candidate = candidate.with_next(next);
        }
        let encoded_bytes = serde_json::to_vec(&candidate)
            .map_err(|error| ServeError::Open(error.to_string(), None))?
            .len() as u64;
        if !page_calls.is_empty() && encoded_bytes > max_bytes {
            break;
        }
        page_calls.push(entry.call.clone());
        consumed += 1;
        if encoded_bytes > max_bytes {
            break;
        }
    }
    let mut page = ToolCallsPage::new(run_id.clone(), page_calls);
    if let Some(next) = next_run_tool_calls_cursor(&entries, consumed, context)? {
        page = page.with_next(next);
    }

    let frame_bytes = broca_wire::serialized_result_frame_bytes(&page);
    if frame_bytes > broca_wire::SUBC_MAX_FRAME_BYTES {
        let call = page
            .calls
            .first()
            .expect("a too-large run.tool_calls page contains a call");
        return Err(ServeError::ReplyTooLarge(ReplyTooLargeDetail::new(
            frame_bytes,
            broca_wire::SUBC_MAX_FRAME_BYTES,
            ReplyTooLargeSubject::ToolCall {
                run_id,
                tool_call_id: call.tool_call_id.clone(),
                call_key: call.call_key.clone(),
            },
        )));
    }
    Ok(page)
}

pub(crate) fn frozen_auth_selection(state: &broca_core::ReconciledRunState) -> Option<String> {
    // Reuse the admission selection already decoded for lifecycle resolution. Current
    // routing and the vault's resolved alias are not the credential key the run froze.
    state
        .auth_selection
        .as_ref()
        .map(|selection| selection.credential_id.clone())
        .filter(|credential_id| !credential_id.is_empty())
}

/// A run's `run.result` state, the pause reason it reports, and the error it carries, from its
/// status. `None` for a run the session doesn't know.
pub(crate) fn result_state(
    status: RunStatusResponse,
) -> Option<(
    String,
    Option<String>,
    Option<broca_protocol::caller::SharedProviderError>,
)> {
    Some(match status {
        RunStatusResponse::Unknown => return None,
        RunStatusResponse::Active { .. } => ("active".to_owned(), None, None),
        RunStatusResponse::Paused { reason, error, .. } => ("paused".to_owned(), reason, error),
        RunStatusResponse::Interrupted { .. } => ("interrupted".to_owned(), None, None),
        RunStatusResponse::Completed { .. } => ("completed".to_owned(), None, None),
        RunStatusResponse::MaxSteps { .. } => ("max_steps".to_owned(), None, None),
        RunStatusResponse::BudgetExhausted { .. } => (
            cortexkit_role_llm_runner::run::states::BUDGET_EXHAUSTED.to_owned(),
            None,
            None,
        ),
        RunStatusResponse::Cancelled { .. } => ("cancelled".to_owned(), None, None),
        RunStatusResponse::ContextCeiling { .. } => ("context_ceiling".to_owned(), None, None),
        RunStatusResponse::Error { error, .. } => ("error".to_owned(), None, error),
        RunStatusResponse::TransformUnavailable { .. } => {
            ("transform_unavailable".to_owned(), None, None)
        }
    })
}

/// A `run.result` reply that fits a frame. A structured-output value is never cut short, so an
/// answer it makes too large is refused naming the run; otherwise the final message of the
/// session `session` is the subject.
pub(crate) fn checked_run_result(
    response: broca_wire::RunResultResponse,
    session: &str,
) -> Result<broca_wire::RunResultResponse, ServeError> {
    let subject = if response
        .structured_output
        .as_ref()
        .is_some_and(|output| output.value.is_some())
    {
        Some(ReplyTooLargeSubject::Run {
            run_id: response.run_id.clone(),
        })
    } else {
        response
            .final_message
            .as_ref()
            .map(|message| ReplyTooLargeSubject::Message {
                session: session.to_owned(),
                ordinal: message.ordinal,
            })
    };
    if let Some(error) = reply_frame_limit_refusal(
        broca_wire::serialized_result_frame_bytes(&response),
        subject,
    ) {
        return Err(error);
    }
    Ok(response)
}

/// A `run.status` reply for run `run_id` that fits a frame, by the same rule as `run.result`: a
/// structured-output value is never cut short, so an answer it makes too large is refused naming
/// the run.
pub(crate) fn checked_run_status(
    response: RunStatusResponse,
    run_id: &str,
) -> Result<RunStatusResponse, ServeError> {
    let carries_value = serde_json::to_value(&response)
        .ok()
        .is_some_and(|value| value.pointer("/structured_output/value").is_some());
    if let Some(error) = reply_frame_limit_refusal(
        broca_wire::serialized_result_frame_bytes(&response),
        carries_value.then(|| ReplyTooLargeSubject::Run {
            run_id: run_id.to_owned(),
        }),
    ) {
        return Err(error);
    }
    Ok(response)
}

pub(crate) fn run_status_response_with_reader_policy(
    state: Option<broca_core::ReconciledRunState>,
    preserve_pause: bool,
) -> RunStatusResponse {
    use broca_core::RunIndexState as State;

    let Some(state) = state else {
        return RunStatusResponse::Unknown;
    };
    let auth_selection = frozen_auth_selection(&state);
    let final_step_finish_reason = state.final_step_finish_reason;
    let finish = state.finish;
    let undelivered_steers = state.undelivered_steers;
    let indeterminate_tool_calls = state.indeterminate_tool_calls;
    // `RunUsage` is the published llm-runner type, so it goes on the wire as it is.
    let usage = Some(state.run_usage);
    let retries_used = state.retries_used;
    let title = state.title;
    let structured_output = state.structured_output;
    // Carried as `session.read` carries them: the error verbatim through the caller-facing
    // wrapper, the pause reason through the same spelling function, so both surfaces
    // report one run's cause identically.
    let error = state.error.map(Into::into);
    let pause_reason = state.pause_reason.map(|reason| {
        broca_core::SessionReadReason::Pause(reason)
            .as_str()
            .to_string()
    });
    // A provider-held park is an implementation recovery boundary, not an owner pause.
    let public_state = if state.state == State::Paused
        && state.pause_reason == Some(broca_protocol::PauseReason::ToolHeld)
        && !preserve_pause
    {
        State::Active
    } else {
        state.state
    };
    match public_state {
        // Deadline waits and provider-held calls are independent runner-owned waits.
        // Consumers recognize them from their fields, never from the informational reason.
        State::Active => RunStatusResponse::Active {
            awaits: None,
            auth_selection,
            reason: match (state.ceiling_deadline_ms, state.ride_wait_deadline_ms) {
                (Some(_), _) => Some("context_ceiling_wait".into()),
                (None, Some(_)) => {
                    Some(cortexkit_role_llm_runner::run::RUN_REASON_COMPACTION_WAIT.into())
                }
                (None, None) => None,
            },
            deadline_ms: state.ceiling_deadline_ms.or(state.ride_wait_deadline_ms),
            usage,
            step: state.live_step,
            title,
            awaiting_approval: None,
            held_calls: state.held_calls,
        },
        State::Paused => RunStatusResponse::Paused {
            awaits: None,
            resume_not_before_ms: state
                .account_limit_pause
                .as_ref()
                .map(|pause| pause.resume_not_before_ms),
            auth_selection,
            pause_reason: state
                .pause_reason
                .filter(|reason| {
                    matches!(
                        reason,
                        broca_protocol::PauseReason::Restart
                            | broca_protocol::PauseReason::AccountLimit
                            | broca_protocol::PauseReason::ToolHeld
                    )
                })
                .map(Into::into),
            usage,
            retries_used,
            reason: pause_reason,
            error,
            auth: state
                .auth_selection
                .map(|selection| broca_wire::PausedRunAuth {
                    // Prefer the vault's resolved identity; older records retain the requested key.
                    credential_id: selection
                        .resolved_credential_id
                        .unwrap_or(selection.credential_id),
                    account_ref: selection.account_ref,
                    routing_code: selection
                        .account_decision
                        .as_ref()
                        .filter(|decision| {
                            decision.path == broca_wal::ACCOUNT_DECISION_AUTH_REQUIRED
                        })
                        .map(|_| "auth_required".to_owned()),
                    path: selection
                        .account_decision
                        .map(|decision| match decision.path.as_str() {
                            // WAL AccountDecision fallback paths used the configured static account,
                            // so the wire reports "default", not a routed choice or caller pin.
                            "disabled" | "unreachable" | "unsupported" | "single_candidate"
                            | "legacy_static" | "candidates_unknown" | "auth_required" => {
                                "default".into()
                            }
                            _ => decision.path,
                        }),
                }),
            title,
            held_calls: state.held_calls,
        },
        State::Interrupted => RunStatusResponse::Interrupted {
            auth_selection,
            reason: None,
            error,
            final_step_finish_reason,
            finish,
            undelivered_steers,
            indeterminate_tool_calls,
            usage,
            retries_used,
            title,
            structured_output: structured_output.clone(),
        },
        State::Completed => RunStatusResponse::Completed {
            auth_selection,
            directives: None,
            final_step_finish_reason,
            finish,
            undelivered_steers,
            usage,
            retries_used,
            title,
            structured_output: structured_output.clone(),
        },
        State::MaxSteps => RunStatusResponse::MaxSteps {
            auth_selection,
            final_step_finish_reason,
            finish,
            undelivered_steers,
            usage,
            retries_used,
            title,
            structured_output: structured_output.clone(),
        },
        State::BudgetExhausted => RunStatusResponse::BudgetExhausted {
            auth_selection,
            detail: state
                .budget_detail
                .expect("budget terminal retains its durable detail"),
            final_step_finish_reason,
            finish,
            undelivered_steers,
            usage,
            retries_used,
            title,
            structured_output: structured_output.clone(),
        },
        State::ContextCeiling => RunStatusResponse::ContextCeiling {
            auth_selection,
            context_ceiling: state
                .context_ceiling
                .expect("ceiling terminal retains its durable detail"),
            final_step_finish_reason,
            finish,
            undelivered_steers,
            usage,
            retries_used,
            title,
            structured_output: structured_output.clone(),
        },
        State::Cancelled => RunStatusResponse::Cancelled {
            auth_selection,
            final_step_finish_reason,
            finish,
            undelivered_steers,
            usage,
            retries_used,
            title,
            structured_output: structured_output.clone(),
        },
        State::Error => RunStatusResponse::Error {
            awaits: None,
            auth_selection,
            final_step_finish_reason,
            finish,
            undelivered_steers,
            usage,
            retries_used,
            error,
            title,
            structured_output: structured_output.clone(),
        },
        State::TransformUnavailable => RunStatusResponse::TransformUnavailable {
            auth_selection,
            final_step_finish_reason,
            finish,
            undelivered_steers,
            usage,
            retries_used,
            title,
            structured_output: structured_output.clone(),
        },
    }
}

/// Where a request refused because the module is stopping was turned away. The refusal body is
/// the same either way; each place keeps the message it has always answered with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoppingAt {
    /// A session actor refused to admit the send.
    Admission,
    /// The session could not be opened, or its run could not be started.
    Open,
}

/// An error from the serve surface (distinct from a successful per-request outcome). Opening
/// failures describe lease or store problems; `SendIdReuse` is a typed request rejection.
#[derive(Debug, Clone)]
pub enum ServeError {
    SessionLogMissing {
        session: String,
        harness: String,
    },
    Upload(broca_core::media_uploads::UploadError),
    MediaTooLarge(cortexkit_role_llm_runner::errors::MediaTooLargeDetail),
    ContextCeiling(broca_protocol::ContextCeilingDetail),
    UnknownSend,
    SessionMoving {
        cut_id: String,
    },
    SessionMoved {
        holder: Option<String>,
    },
    Expired {
        expired_at_ms: u64,
    },
    RetentionInvalid(crate::retention::InvalidRetention),
    VariantUnsupported(broca_core::run::VariantRefusal),
    ToolAliasInvalid(broca_core::run::ToolAliasInvalid),
    ServiceTierInvalid {
        reason: &'static str,
        message: String,
    },
    /// The session is held by another live writer (another process/host).
    SessionHeld(String),
    /// Opening or admission failed. The optional class is set only from a known cause.
    Open(String, Option<broca_protocol::ErrorClass>),
    /// broca refused this request because it is stopping. This request did nothing; repeat it
    /// after the restart. It says nothing about whether an earlier request with the same send
    /// id was admitted: a keyed resend is answered from the record once broca serves again.
    ModuleStopping(StoppingAt),
    /// The store proves history exists, but neither live nor archived WAL can cover it.
    HistoryUnreachable {
        session_ref: String,
        reason: String,
    },
    /// A whole message or model-view atomic group would exceed one encoded subc frame.
    ReplyTooLarge(ReplyTooLargeDetail),
    /// The session's WAL holds a frame that requires WAL features this build does not know
    /// (`missing`): a newer broca wrote it. Nothing was written and retrying cannot help, so
    /// it answers with its own wire code rather than [`ServeError::Open`]. `reason` is the
    /// text [`ServeError::Open`] would have carried, so the message reads exactly as before.
    RetainedSessionTooLarge {
        max_bytes: u64,
    },
    SessionTooLarge {
        max_bytes: u64,
    },
    WalNewerWriter {
        missing: Vec<String>,
        reason: String,
    },
    /// The queued submission named by a deterministic send identity was durably retracted.
    SendRetracted {
        submission_id: String,
    },
    /// The ordered send named by a deterministic send identity ended undelivered: `outcome` is
    /// `superseded` (a newer send from the same sender replaced it under its dedupe key) or
    /// `expired`.
    SendEnded {
        submission_id: String,
        outcome: &'static str,
    },
    /// A send using the `delivery_order` group was refused by its sender: `code` is
    /// `sender_not_permitted` or `idle_start_not_granted`.
    DeliveryRefused {
        code: &'static str,
        message: String,
    },
    /// A send asked the new storage for `feature`, which it doesn't serve: a creating send's
    /// feature, as a head starts nowhere else, or a send continuing a session compacted under
    /// the first compaction protocol, which moved read-only. Refused
    /// [`crate::store_entries::NOT_SERVED_ON_NEW_STORAGE`], permanent, with nothing written.
    NotServedOnNewStorage {
        feature: &'static str,
    },
    /// A template send whose `part_tag` slot names a part the transcript does not show, refused
    /// `template_part_gone` with the slot named in its detail.
    TemplatePartGone {
        slot: String,
    },
    /// A tag read (`session.read {tag, call_key}`) the session can't answer: `unknown_call` for
    /// a call it holds no request for, `unknown_tag` for a number that request didn't show or
    /// that names no part broca numbered.
    TagReadRefused {
        code: &'static str,
        message: String,
    },
    /// A deterministic send identity was reused with a different frozen request field.
    SendIdReuse {
        differing: String,
    },
    /// Admission failed before durable start because a dependency is temporarily unavailable.
    Transient(String),
    CredentialsUnavailable,
    CredentialAmbiguous {
        credential: String,
        candidate_ids: Vec<String>,
    },
    /// A read arrived while the session's running run waits for its compaction Setup answer,
    /// which may anchor the run's tag numbers at a checkpoint. Refused `transient`, writing
    /// nothing, so no reader sees a number that changes once the answer is durable.
    TagsAwaitSetup,
    /// An import was rejected by lineage/content admission.
    Import(String),
    /// A deterministic import identity was reused with different bytes.
    ImportIdReuse,
    /// Import cannot run while the lineage actor has an active provider run.
    ImportBusy,
    /// `session.delete` found work the deletion would lose (named): a run active or paused,
    /// queued sends, or spend not yet exported. Nothing was deleted.
    SessionBusy(String),
    /// The owner check refused the send; nothing was written.
    ScopeRefused(broca_core::ScopeRefusal),
    /// The attested caller is not admitted by the plan's reader policy.
    ReadAccessRefused,
    /// The send would freeze or adopt its stamped scope while the session is busy; nothing was
    /// written and the caller retries when it is idle.
    ScopeFreezeBusy {
        reason: crate::actor::ScopeFreezeBusyReason,
        run_id: Option<String>,
    },
    /// A new message (plain, queued or steered) was sent while the session's last run is
    /// paused: there is no running turn to join, and the paused one renders nothing until its
    /// pause is resolved. Nothing was written; the caller resends the paused run's own send_id
    /// once the pause is resolved, cancels it, or interrupts.
    RunPaused(RunPausedRefusal),
    /// The send's fetch plan, or a member it carried beside one, was refused before anything
    /// was written.
    PlanRefused(broca_fetch_plan::Refusal),
    /// A request member is malformed, unknown, or combined with one it excludes.
    InvalidParams {
        field: String,
        message: String,
    },
    /// A requested version no longer names the session's active history.
    HistoryChanged,
    CursorExpired,
    HistoryRewriting,
    /// `after_mid` names no message of the lineage.
    UnknownMid(String),
    /// `run.result` names no run of the session.
    UnknownRun(String),
    /// The session doesn't exist, and the request doesn't create it.
    UnknownSession,
    /// The routing module refused to choose a provider account for the send; nothing was
    /// written.
    AccountSelectionRefused(broca_core::AccountSelectionRefusal),
    /// The routing module gave no usable account decision for the send (unreachable, no
    /// decision in time, or a pick broca did not offer); nothing was written. Transient.
    RoutingUnavailable(broca_core::RoutingUnavailable),
    /// `session.warm` was refused; the body is sent exactly as the broca-wire type serializes.
    WarmRefused(Box<broca_wire::WarmRefusal>),
    /// A steer or queued send was refused before writing or interrupting a run.
    PreUserUnavailable {
        provider: String,
    },
}

impl From<OpenError> for ServeError {
    fn from(e: OpenError) -> Self {
        match e {
            OpenError::SessionLogMissing {
                session, harness, ..
            } => Self::SessionLogMissing { session, harness },
            OpenError::SessionMoving { cut_id } => Self::SessionMoving { cut_id },
            OpenError::SessionMoved { holder } => Self::SessionMoved { holder },
            OpenError::Expired { expired_at_ms } => ServeError::Expired { expired_at_ms },
            OpenError::RetentionInvalid(detail) => ServeError::RetentionInvalid(detail),
            OpenError::SessionClassInvalid(message) => ServeError::InvalidParams {
                field: "session_class".into(),
                message,
            },
            OpenError::LeaseHeld(s) => ServeError::SessionHeld(s),
            // Legacy open errors erased the cause; do not infer retryability from text.
            OpenError::HistoryChanged => ServeError::HistoryChanged,
            OpenError::HistoryRewriting => ServeError::HistoryRewriting,
            OpenError::Open(s) => ServeError::Open(s, None),
            OpenError::ModuleStopping => ServeError::ModuleStopping(StoppingAt::Open),
            OpenError::Classified { reason, class } => ServeError::Open(reason, Some(class)),
            OpenError::HistoryUnreachable {
                session_ref,
                reason,
            } => ServeError::HistoryUnreachable {
                session_ref,
                reason,
            },
            OpenError::NewerWriter { missing, reason } => {
                ServeError::WalNewerWriter { missing, reason }
            }
            OpenError::NotServedOnNewStorage { feature } => {
                ServeError::NotServedOnNewStorage { feature }
            }
            OpenError::UnknownSession => ServeError::UnknownSession,
        }
    }
}

impl std::fmt::Display for ServeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServeError::CredentialsUnavailable => write!(f, "the vault credential listing has not loaded yet"),
            ServeError::CredentialAmbiguous { credential, candidate_ids } => write!(f, "credential {credential} is ambiguous: {}", candidate_ids.join(", ")),
            ServeError::SessionLogMissing { session, harness } => OpenError::SessionLogMissing { session: session.clone(), harness: harness.clone(), read: None }.fmt(f),
            Self::Upload(error) => error.fmt(f),
            Self::MediaTooLarge(_) => f.write_str("committed image exceeds a reported image limit"),
            ServeError::UnknownSend => write!(f, "send_id is not recorded for this caller"),
            ServeError::SessionMoving { cut_id } => write!(f, "session is moving under cut {cut_id}"),
            ServeError::SessionMoved { .. } => {
                write!(f, "this session moved to another computer and is held there")
            }
            ServeError::Expired { expired_at_ms } => write!(f, "lineage expired at {expired_at_ms}"),
            ServeError::RetentionInvalid(_) => write!(f, "invalid retention"),
            ServeError::SessionHeld(s) => write!(f, "session '{s}' held by a live writer"),
            ServeError::VariantUnsupported(e) => write!(f, "open failed: run resolution failed: {e}"),
            ServeError::ToolAliasInvalid(e) => write!(f, "{e}"),
            ServeError::ServiceTierInvalid { message, .. } => f.write_str(message),
            ServeError::SessionTooLarge { max_bytes } => write!(f, "session log cannot exceed {max_bytes} bytes"),
            ServeError::RetainedSessionTooLarge { max_bytes } => write!(f, "retained session log cannot exceed {max_bytes} bytes"),
            ServeError::Open(s, _)
            | ServeError::HistoryUnreachable { reason: s, .. }
            | ServeError::WalNewerWriter { reason: s, .. } => {
                write!(f, "open failed: {s}")
            }
            ServeError::ReplyTooLarge(_) => {
                f.write_str("the session.read reply exceeds the transport frame limit")
            }
            ServeError::SendRetracted { submission_id } => {
                write!(f, "submission '{submission_id}' was retracted")
            }
            ServeError::SendEnded {
                submission_id,
                outcome,
            } => write!(f, "submission '{submission_id}' ended {outcome}"),
            ServeError::DeliveryRefused { message, .. } => f.write_str(message),
            ServeError::NotServedOnNewStorage { feature } => {
                f.write_str(&crate::store_actor::unserved_message(feature))
            }
            ServeError::TemplatePartGone { slot } => write!(
                f,
                "the template's {slot} slot names a part the transcript does not show"
            ),
            ServeError::TagReadRefused { message, .. } => f.write_str(message),
            ServeError::SendIdReuse { differing } => write!(
                f,
                "send_id was already used with different {differing}; retries must be byte-identical"
            ),
            ServeError::Transient(s) => write!(f, "transient admission failure: {s}"),
            // Each place keeps the message it has always answered this refusal with.
            ServeError::ModuleStopping(StoppingAt::Admission) => {
                f.write_str("transient admission failure: module is stopping")
            }
            ServeError::ModuleStopping(StoppingAt::Open) => {
                f.write_str("open failed: module is stopping")
            }
            ServeError::TagsAwaitSetup => f.write_str(
                "the running run's tag numbers wait for its compaction Setup answer; retry shortly",
            ),
            ServeError::ContextCeiling(detail) => write!(f, "context_ceiling: estimated request size {} in window {} ({})", detail.size, detail.window, detail.cause),
            ServeError::Import(s) => write!(f, "import rejected: {s}"),
            ServeError::ImportIdReuse => write!(f, "import_id was reused with different content"),
            ServeError::ImportBusy => write!(f, "session has an active run"),
            ServeError::SessionBusy(message) => f.write_str(message),
            ServeError::ScopeRefused(refusal) => write!(f, "scope refused: {refusal:?}"),
            ServeError::ReadAccessRefused => f.write_str("the session's read policy does not admit this caller"),
            ServeError::ScopeFreezeBusy { reason, run_id } => write!(
                f,
                "the send would freeze its scope but the session is busy ({reason:?}, run {run_id:?})"
            ),
            ServeError::RunPaused(refusal) => write!(
                f,
                "run '{}' is paused ({}), so this session takes no new message; resend that \
                 run's own send_id once the pause is resolved, cancel it, or send with delivery \
                 \"interrupt\"",
                refusal.run_id,
                refusal.reason.as_str()
            ),
            ServeError::PlanRefused(refusal) => f.write_str(&refusal.message()),
            ServeError::PreUserUnavailable { provider } => write!(f, "pre_user_unavailable: {provider}"),
            ServeError::InvalidParams { message, .. } => f.write_str(message),
            ServeError::HistoryRewriting => f.write_str("history is being rewritten; retry after its writer finishes recovery"),
            ServeError::HistoryChanged => f.write_str("the session's history changed; read its newest page"),
            ServeError::CursorExpired => f.write_str("the subscribe cursor is not held in this session log"),
            ServeError::UnknownMid(mid) => write!(f, "the lineage has no message '{mid}'"),
            ServeError::UnknownRun(run_id) => write!(f, "the session has no run '{run_id}'"),
            ServeError::UnknownSession => f.write_str("session does not exist"),
            ServeError::AccountSelectionRefused(refusal) => write!(
                f,
                "routing refused to choose a provider account ({})",
                refusal.routing_code
            ),
            ServeError::RoutingUnavailable(unavailable) => write!(
                f,
                "the routing module gave no usable account decision ({})",
                unavailable.reason.as_str()
            ),
            ServeError::WarmRefused(refusal) => f.write_str(&refusal.message),
        }
    }
}

impl std::error::Error for ServeError {}

/// The transport-agnostic dispatch surface the subc serve adapter calls. One method per
/// `session.*` / `run.*` op. The registry implements this by routing to the per-session
/// actor; a `Reopen` from the actor (it was closing) is handled INSIDE the registry's
/// reopen-retry loop, so a handler method never surfaces it — every method either returns a
/// real outcome or a typed `ServeError`.
#[async_trait]
pub trait ServeHandler: Send + Sync {
    async fn check_plan_selection(
        &self,
        plan: &serde_json::Value,
    ) -> Result<
        Option<cortexkit_role_llm_runner::head_selection::PlanModel>,
        cortexkit_role_llm_runner::head_selection::PlanModelProblem,
    > {
        cortexkit_role_llm_runner::head_selection::check_plan(
            plan,
            &broca_wire::broca_role_describe(env!("CARGO_PKG_VERSION")).role,
            |_| None,
        )
    }
    async fn media_read(
        &self,
        _session: &LineageKey,
        _params: serde_json::Value,
    ) -> Result<serde_json::Value, ServeError> {
        Err(ServeError::InvalidParams {
            field: "sha256".into(),
            message: "session.media_read unavailable".into(),
        })
    }
    async fn upload(
        &self,
        _session: &LineageKey,
        _principal: Option<&broca_protocol::BindPrincipal>,
        _scope: &broca_protocol::SendScope,
        _method: &str,
        _params: serde_json::Value,
    ) -> Result<serde_json::Value, ServeError> {
        Err(ServeError::InvalidParams {
            field: "upload_id".into(),
            message: "uploads unavailable".into(),
        })
    }
    fn backup_classes(&self) -> Vec<String> {
        vec!["head".into()]
    }
    /// `memory.report`: what the daemon's open sessions and caches hold, estimated by walking
    /// them now. `None` from a handler without sessions to report.
    async fn memory_report(&self) -> Option<broca_wire::MemoryReport> {
        None
    }
    /// Check the move latch before authorization reads frozen session metadata.
    /// File-backed handlers enforce the durable fence even for already-bound routes.
    async fn check_migration(&self, _session: &LineageKey, _write: bool) -> Result<(), ServeError> {
        Ok(())
    }
    /// Whether the executor has installed the writer's compaction plane.
    fn compaction_supported(&self) -> bool {
        false
    }

    /// Serve one `compaction-runner/v1` operation (`bind`, `session.entries`,
    /// `session.apply_view`, `session.applied_view`) for the session a route is bound to.
    /// `caller` is the daemon-attested module id, never a body field. Handlers without
    /// mode-3 sessions refuse every operation as `unknown_method`.
    async fn compaction_runner(
        &self,
        _session: &LineageKey,
        _caller: Option<String>,
        method: String,
        _params: Box<serde_json::value::RawValue>,
    ) -> Result<Box<serde_json::value::RawValue>, broca_core::compaction_v2::protocol::wire::Refusal>
    {
        Err(broca_core::compaction_v2::protocol::wire::Refusal::new(
            "unknown_method",
            format!("{method} is not served by this handler"),
            None,
        ))
    }

    /// Authorize the daemon-attested caller before any session operation has an effect.
    /// Handlers without history (such as protocol test doubles) have nothing to freeze.
    async fn authorize_session(
        &self,
        _session: &LineageKey,
        _principal: Option<&broca_protocol::BindPrincipal>,
    ) -> Result<(), ServeError> {
        Ok(())
    }

    /// Check read policy before dispatch; handlers without stored plans keep their existing rule.
    async fn authorize_session_read(
        &self,
        session: &LineageKey,
        access: &crate::reader_admission::ReadAccess,
    ) -> Result<(), ServeError> {
        self.authorize_session(session, access.principal.as_ref())
            .await
    }

    /// Reader policies are served only for sessions kept, or being created, in the new storage.
    fn plan_readers_supported(
        &self,
        _session: &LineageKey,
        _class: Option<&str>,
    ) -> Result<bool, ServeError> {
        Ok(false)
    }

    /// Diagnostic ownership observation only; a cold cache is unknown, not a non-owner.
    /// Implementations must not read storage or open an actor to answer this query.
    fn cached_session_owner(
        &self,
        _session: &LineageKey,
        _principal: Option<&broca_protocol::BindPrincipal>,
    ) -> Option<bool> {
        None
    }

    /// Owner-only authorization for cancel/retract, independent of route scope and epoch.
    /// A handler without recorded ownership has no protected session.
    async fn authorize_session_owner(
        &self,
        _session: &LineageKey,
        _principal: Option<&broca_protocol::BindPrincipal>,
        _scope: &broca_protocol::SendScope,
    ) -> Result<(), ServeError> {
        Ok(())
    }

    /// Authorize cancellation before migration fences or parameter validation can disclose
    /// session state. Only an immediate stop may use a named sender's interrupt grant.
    /// `None` means an `at` other than `"now"`/`"step_boundary"`, including non-strings.
    /// An omitted `at` is passed as `Some(RunCancelAt::Now)`.
    async fn authorize_session_cancel(
        &self,
        session: &LineageKey,
        principal: Option<&broca_protocol::BindPrincipal>,
        scope: &broca_protocol::SendScope,
        _at: Option<broca_wal::record::RunCancelAt>,
    ) -> Result<(), ServeError> {
        self.authorize_session_owner(session, principal, scope)
            .await
    }

    /// Retraction defaults to owner-only unless the handler supports recorded senders.
    /// The principal must come from the daemon's bind, never from request parameters.
    async fn authorize_session_retract(
        &self,
        session: &LineageKey,
        principal: Option<&broca_protocol::BindPrincipal>,
        scope: &broca_protocol::SendScope,
        _submission_id: Option<&str>,
    ) -> Result<(), ServeError> {
        self.authorize_session_owner(session, principal, scope)
            .await
    }

    /// `session.send` — start or durably queue a run.
    async fn send(&self, req: SendRequest) -> Result<SendResponse, ServeError>;

    /// Read a send's recorded fate without opening the session's actor. An auth-paused
    /// send stays paused here; only an explicit re-send re-drives authentication.
    async fn send_status(
        &self,
        _session: &LineageKey,
        _run_id: broca_protocol::RunId,
        _send_id: &str,
        _principal: Option<&broca_protocol::BindPrincipal>,
    ) -> Result<broca_wire::SendReplyWire, ServeError> {
        Err(ServeError::UnknownSend)
    }

    /// The adoption check a stamped `route.bind` gets before it is acknowledged: `Ok(Some)` when
    /// the bound session was recorded without a scope and its history refuses the stamp. Only
    /// adoption is decided here, plus the reservation of a session with no episode yet: a stamp
    /// whose owner or ref differs from the scope that reserved the session's address is
    /// `Err(ServeError::ScopeRefused)`, the refusal its first send would get. Every other scope
    /// rule is decided on each send, since an episode can be written between the bind and the
    /// send. `Err` also when the history cannot be read, so whether it refuses the stamp is
    /// unknown: the bind is refused rather than accepted. The default implementation has no
    /// history source and returns no refusal.
    async fn bind_adoption_refusal(
        &self,
        _session: &LineageKey,
        _principal: Option<&broca_protocol::BindPrincipal>,
        _scope: &broca_protocol::BindScope,
    ) -> Result<Option<broca_core::AdoptionRefusal>, ServeError> {
        Ok(None)
    }

    /// `session.import` — durably seed an empty lineage without provider execution.
    async fn import(
        &self,
        _req: SessionImportRequest,
    ) -> Result<SessionImportResponse, ServeError> {
        Err(ServeError::Import("session.import is unsupported".into()))
    }

    /// `session.import` from a file source, and `session.install_view`: commands only a session
    /// kept in the new storage serves. A handler without the new storage refuses them by name.
    async fn store_command(
        &self,
        _session: &LineageKey,
        _command: crate::store_import::StoreCommand,
    ) -> Result<crate::store_import::StoreOutcome, ServeError> {
        Ok(crate::store_import::StoreOutcome::Refused(
            crate::store_import::ImportRefusal::new(
                "new_storage_unavailable",
                "this module doesn't create sessions in the new storage",
            ),
        ))
    }

    /// Whether `session` is served from the new storage. Refuses a session owned by the new
    /// storage when this handler has no runtime for it, before dispatch can consult today's WAL.
    fn kept_in_new_storage(&self, _session: &LineageKey) -> Result<bool, ServeError> {
        Ok(false)
    }

    /// The applied plan, read without opening an actor. `None` means the session is kept on
    /// the old storage, which does not serve this operation.
    async fn plan_view(
        &self,
        _session: &LineageKey,
    ) -> Result<Option<broca_wire::plan_view::PlanViewResponse>, crate::store_plan_view::Error>
    {
        Ok(None)
    }

    /// `session.tag_map`: which message and part each part number of the session belongs to
    /// (see `store_tag_map`). `None` for a session today's storage keeps, which the operation
    /// doesn't serve; a handler without the new storage keeps every session there.
    async fn tag_map(
        &self,
        _session: &LineageKey,
        _params: &broca_wire::tag_map::TagMapParams,
    ) -> Result<Option<broca_wire::tag_map::TagMapResponse>, ServeError> {
        Ok(None)
    }

    /// `session.read {tag, call_key}` (llm-runner `tag_read`): the message part the model saw as
    /// part number `tag` in the request that produced call `call_key` (see `store_tag_read`),
    /// for the session's owner alone. `principal` is the one the daemon attests opened the
    /// route and `scope` the daemon's stamp on it. `None` for a handler without the new storage,
    /// which keeps no request's part numbers.
    async fn tag_read(
        &self,
        _session: &LineageKey,
        _request: &cortexkit_role_llm_runner::tag_read::TagReadRequest,
        _principal: Option<&broca_protocol::BindPrincipal>,
        _scope: &broca_protocol::SendScope,
    ) -> Result<Option<cortexkit_role_llm_runner::tag_read::TagReadAnswer>, ServeError> {
        Ok(None)
    }

    /// `session.request_dump`: rebuilds the request each step of run `run_id` sent and writes
    /// the bodies that match their recorded digests (see `store_request_dump`). `None` for a
    /// session today's storage keeps, which the operation doesn't serve.
    async fn request_dump(
        &self,
        _session: &LineageKey,
        _run_id: &str,
    ) -> Result<Option<broca_wire::request_dump::RequestDumpResponse>, ServeError> {
        Ok(None)
    }

    /// `session.delete`: deletes the session for the operator when it holds no work a deletion
    /// would lose (see `retention_store`). `None` for a session today's storage keeps, which the
    /// operation doesn't serve.
    async fn delete_session(
        &self,
        _session: &LineageKey,
    ) -> Result<Option<broca_wire::session_delete::DeleteResponse>, ServeError> {
        Ok(None)
    }

    /// `session.restore_from_backup`: restores the session, or with `all` every session kept in
    /// the new storage, from the files the store holds after a backup came back (see
    /// `registry::store_restore`). A handler without the new storage refuses by name.
    async fn restore_from_backup(
        &self,
        _session: &LineageKey,
        _all: bool,
    ) -> Result<serde_json::Value, crate::registry::store_move::MoveRefusal> {
        Err(crate::registry::store_move::MoveRefusal::new(
            "store_unavailable",
            "this module keeps no sessions in the new storage",
        ))
    }

    /// Where a file-source import may read from. `None` without the new storage.
    fn import_roots(&self) -> Option<std::sync::Arc<crate::store_import::ImportRoots>> {
        None
    }

    /// `session.retract` — retract a still-pending queued submission.
    async fn retract(
        &self,
        session: &LineageKey,
        submission_id: broca_protocol::SubmissionId,
    ) -> Result<RetractResponse, ServeError>;

    /// `session.subscribe` — attach a control-stream subscriber; deliveries arrive on
    /// `sink`. Returns once attached (the attach barrier was taken).
    async fn subscribe(
        &self,
        session: &LineageKey,
        from: SubscribeFrom,
        sink: tokio::sync::mpsc::Sender<Delivery>,
    ) -> Result<(), ServeError>;

    /// `session.read` — read a durable transcript page without opening a lineage actor or
    /// acquiring the session lease.
    async fn read(&self, req: SessionReadRequest) -> Result<SessionReadResponse, ServeError>;

    /// The plan manifest the session froze at admission. `None` for a session admitted without a
    /// plan, and from a handler that cannot read the session's history.
    /// Locate a stored part for output-reference reads without scanning the transcript.
    async fn tag_location(
        &self,
        _session: &LineageKey,
        _tag: u64,
    ) -> Result<Option<broca_core::TagLocation>, ServeError> {
        Ok(None)
    }

    async fn frozen_manifest(
        &self,
        _session: &LineageKey,
    ) -> Result<Option<std::sync::Arc<broca_wal::PlanManifest>>, ServeError> {
        Ok(None)
    }

    /// For a session kept in the new storage, the compaction view the request of the model step
    /// that wrote message `ordinal` (counting from 0) was rendered under, which output references
    /// check a named part against: `Some(None)` when no view applied. `None` for a session kept
    /// on today's storage, whose views output references read from its log instead
    /// ([`crate::output_refs::view_at_writing_step`]).
    async fn store_writing_view(
        &self,
        _session: &LineageKey,
        _ordinal: u64,
    ) -> Result<Option<Option<broca_core::compaction_provider::view::CompactionView>>, ServeError>
    {
        Ok(None)
    }

    /// The namespace output references use. A converted session retains its source WAL's
    /// lineage even though its history cursors use the new store incarnation.
    async fn output_ref_namespace(
        &self,
        _session: &LineageKey,
        incarnation: &str,
    ) -> Result<String, ServeError> {
        Ok(incarnation.to_owned())
    }

    /// The history version output-reference ids of `session` are scoped to, given `served`, the
    /// version its reads serve: the stored transcript's own version, which only a rewrite of the
    /// stored history moves. A reference names stored tool output, so an id stays valid across a
    /// change in how reads expose stored replies (`crate::store_exposure`).
    async fn output_ref_history_version(
        &self,
        _session: &LineageKey,
        served: u64,
    ) -> Result<u64, ServeError> {
        Ok(served)
    }

    /// `session.output_refs` (llm-runner §9.13): one page of an output-reference set, derived
    /// from the session log on every read (see [`crate::output_refs`]).
    async fn output_refs(
        &self,
        session: &LineageKey,
        request: &cortexkit_role_llm_runner::output_refs::ListRequest,
    ) -> Result<
        Result<cortexkit_role_llm_runner::output_refs::Page, crate::output_refs::OutputRefRefusal>,
        ServeError,
    > {
        crate::output_refs::list(self, session, request).await
    }

    /// `session.output_ref_read` (llm-runner §9.13): a byte range of one output reference's
    /// content, derived from the session log on every read (see [`crate::output_refs`]).
    async fn output_ref_read(
        &self,
        session: &LineageKey,
        request: &cortexkit_role_llm_runner::output_refs::ReadRequest,
    ) -> Result<
        Result<
            cortexkit_role_llm_runner::output_refs::ReadReply,
            crate::output_refs::OutputRefRefusal,
        >,
        ServeError,
    > {
        crate::output_refs::read(self, session, request).await
    }

    /// `run.cancel` — cooperatively cancel the active run (→ Cancelled).
    async fn cancel(
        &self,
        session: &LineageKey,
        run_id: broca_protocol::RunId,
    ) -> Result<CancelResponse, ServeError>;

    /// Boundary-aware adapter. Implementations without the runtime must refuse, not silently
    /// convert a boundary cancellation into an immediate one.
    async fn cancel_at(
        &self,
        session: &LineageKey,
        run_id: broca_protocol::RunId,
        at: broca_wal::record::RunCancelAt,
    ) -> Result<CancelResponse, ServeError> {
        if at == broca_wal::record::RunCancelAt::Now {
            self.cancel(session, run_id).await
        } else {
            // Permanent: a serve implementation without the boundary runtime will not gain it
            // on a retry, so a caller must not keep re-sending the same request.
            Err(ServeError::Open(
                "step-boundary cancellation is not installed".into(),
                Some(broca_protocol::ErrorClass::Permanent),
            ))
        }
    }

    /// Cancel with the daemon-attested caller, never an identity from the request body.
    /// Handlers without named-sender admission retain owner-only cancellation.
    async fn cancel_from(
        &self,
        session: &LineageKey,
        run_id: broca_protocol::RunId,
        at: broca_wal::record::RunCancelAt,
        principal: Option<&broca_protocol::BindPrincipal>,
        scope: &broca_protocol::SendScope,
    ) -> Result<CancelResponse, ServeError> {
        self.authorize_session_owner(session, principal, scope)
            .await?;
        self.cancel_at(session, run_id, at).await
    }

    /// `run.status` / `session.status` — the lineage's current state.
    async fn status(&self, session: &LineageKey) -> Result<StatusResponse, ServeError>;

    async fn run_event_get(
        &self,
        _session: &LineageKey,
        _event_key: &str,
    ) -> Result<broca_wire::RunEventGetResponse, ServeError> {
        Ok(broca_wire::RunEventGetResponse::Absent)
    }

    /// `run.status{run_id}` — the lifecycle state of a SPECIFIC run, via the three-tier
    /// resolution (live actor → durable index terminal → WAL reconciliation). The wire
    /// surface of the durable run index: a consumer reaches a finished/crashed run's outcome
    /// here even when it's not in the consumer's live subscription.
    async fn run_status(
        &self,
        session: &LineageKey,
        run_id: broca_protocol::RunId,
    ) -> Result<RunStatusResponse, ServeError>;

    /// What `run.status{run_id}` answers on the wire: [`Self::run_status`], and, for a run id a
    /// queued send was minted from, what became of that send (see
    /// [`broca_wire::RunStatusReply`]). The default answers the run's own status only.
    async fn run_status_reply(
        &self,
        session: &LineageKey,
        run_id: broca_protocol::RunId,
    ) -> Result<broca_wire::RunStatusReply, ServeError> {
        self.run_status(session, run_id).await.map(Into::into)
    }

    /// The run's own frozen start; `None` has the same unknown meaning as `run.status`.
    async fn run_admission(
        &self,
        _session: &LineageKey,
        _run_id: broca_protocol::RunId,
    ) -> Result<Option<broca_wire::RunAdmissionResponse>, ServeError> {
        Ok(None)
    }

    /// The two facts that spell a live event's public message ids: whether the log is a mode-3
    /// log, and its lineage. A live subscription asks for these once, so it reads only the
    /// newest page and the log's first run, never the head's media, run or spend facts.
    async fn message_identity(
        &self,
        session: &LineageKey,
    ) -> Result<(bool, Option<String>), ServeError> {
        self.head(session)
            .await
            .map(|head| (head.mode3, head.lineage_id))
    }

    /// `session.history`'s head for a session kept in the new storage, resolving `mid` when the
    /// read names a message; `None` for a session on today's storage, which `head` answers.
    async fn store_history_head(
        &self,
        _session: &LineageKey,
        _mid: Option<&str>,
    ) -> Result<Option<crate::store_entries::HistoryHead>, ServeError> {
        Ok(None)
    }

    /// `session.head` — the session's head without message bodies. For retained sessions,
    /// `updated_at` is the durable activity timestamp that sets expiry, not the WAL mtime.
    async fn head(&self, _session: &LineageKey) -> Result<broca_wire::HeadResponse, ServeError> {
        Err(ServeError::Open(
            "session.head is unsupported".into(),
            Some(broca_protocol::ErrorClass::Permanent),
        ))
    }

    async fn head_for(
        &self,
        session: &LineageKey,
        _principal: Option<&broca_protocol::BindPrincipal>,
        _scope: &broca_protocol::SendScope,
    ) -> Result<broca_wire::HeadResponse, ServeError> {
        self.head(session).await
    }

    /// `session.warm` — re-send the session's most recent model request shaped to generate
    /// nothing, keeping the provider's cached prefix alive.
    async fn warm(
        &self,
        _session: &LineageKey,
        _lineage_id: Option<String>,
    ) -> Result<broca_wire::WarmReply, ServeError> {
        Err(ServeError::Open(
            "session.warm is unsupported".into(),
            Some(broca_protocol::ErrorClass::Permanent),
        ))
    }

    /// `session.fork_prepare` (llm-runner `fork_answer`) on `session`, for the route's attested
    /// `principal`. The default serves no fork.
    async fn fork_prepare(
        &self,
        _session: &LineageKey,
        _principal: Option<&broca_protocol::BindPrincipal>,
    ) -> Result<
        cortexkit_role_llm_runner::fork_answer::ForkPrepareReply,
        crate::fork_answer::ForkError,
    > {
        Err(crate::fork_answer::ForkError::Unsupported(
            "this runner serves no fork".into(),
        ))
    }

    /// `session.fork_ask` on a fork of `session`.
    async fn fork_ask(
        &self,
        _session: &LineageKey,
        _principal: Option<&broca_protocol::BindPrincipal>,
        _request: cortexkit_role_llm_runner::fork_answer::ForkAskRequest,
    ) -> Result<cortexkit_role_llm_runner::fork_answer::ForkAskReply, crate::fork_answer::ForkError>
    {
        Err(crate::fork_answer::ForkError::Unsupported(
            "this runner serves no fork".into(),
        ))
    }

    /// `session.fork_end` on a fork of `session`.
    async fn fork_end(
        &self,
        _session: &LineageKey,
        _principal: Option<&broca_protocol::BindPrincipal>,
        _request: cortexkit_role_llm_runner::fork_answer::ForkEndRequest,
    ) -> Result<cortexkit_role_llm_runner::fork_answer::ForkEndReply, crate::fork_answer::ForkError>
    {
        Err(crate::fork_answer::ForkError::Unsupported(
            "this runner serves no fork".into(),
        ))
    }

    /// `run.result{run_id}` — a run's state and, once it completed, its final message.
    async fn run_result(
        &self,
        _session: &LineageKey,
        _run_id: broca_protocol::RunId,
    ) -> Result<broca_wire::RunResultResponse, ServeError> {
        Err(ServeError::Open(
            "run.result is unsupported".into(),
            Some(broca_protocol::ErrorClass::Permanent),
        ))
    }

    /// `run.tool_calls{run_id, after?, limit?}` — page the durable tool-call ledger for a run.
    async fn run_tool_calls(
        &self,
        _session: &LineageKey,
        _request: cortexkit_role_llm_runner::tool_calls::ToolCallsRequest,
    ) -> Result<cortexkit_role_llm_runner::tool_calls::ToolCallsPage, ServeError> {
        Err(ServeError::Open(
            "run.tool_calls is unsupported".into(),
            Some(broca_protocol::ErrorClass::Permanent),
        ))
    }

    /// `session.baseline` — what the session froze at admission. Answered only to the session's
    /// owner, `principal` being the caller the daemon stamped on the route.
    async fn baseline(
        &self,
        _session: &LineageKey,
        _principal: Option<&broca_protocol::BindPrincipal>,
        _scope: &broca_protocol::SendScope,
    ) -> Result<broca_wire::BaselineReply, ServeError> {
        Err(ServeError::Open(
            "session.baseline is unsupported".into(),
            Some(broca_protocol::ErrorClass::Permanent),
        ))
    }

    /// The baseline the reply to a send answered with run `run_id` carries: `Some` exactly
    /// when that run is the episode that admitted the session (the admitting send, or a retry
    /// of it), from what the session's actor already holds. It reads nothing, so it is `None`
    /// whenever the actor does not know the first episode, and the owner reads
    /// `session.baseline` instead.
    fn admission_baseline(
        &self,
        _session: &LineageKey,
        _run_id: &broca_protocol::RunId,
    ) -> Option<broca_wire::BaselineWire> {
        None
    }

    /// Whether the session was ever written (it has a WAL, live or archived). A send may leave
    /// runner parameters to the session's frozen values only then. An unavailable classifier
    /// refuses retryably rather than inventing history or discarding frozen settings.
    fn session_written(&self, _session: &LineageKey) -> Result<bool, ServeError> {
        Err(ServeError::Open(
            "cannot determine whether the session has a WAL; retry the send".into(),
            Some(broca_protocol::ErrorClass::Transient),
        ))
    }

    /// Where `session` stands with respect to fetch plans. The default cannot tell.
    async fn plan_state(&self, _session: &LineageKey) -> PlanState {
        PlanState::Unknown
    }

    /// The durable flow/run opener, if this session has frozen one. Reads and ordinary
    /// unscoped or plain-scoped sessions keep their existing admission policy.
    async fn frozen_scope_opener(&self, _session: &LineageKey) -> Option<String> {
        None
    }

    /// What admits a send's fetch plan. The default has none, so such a send is refused.
    fn plan_admitter(&self) -> Option<std::sync::Arc<dyn broca_fetch_plan::PlanAdmitter>> {
        None
    }

    /// Whether the actual executor can honor write-time transforms. Admission must check this
    /// before provider fetches or session opening; the default handler serves no hooks.
    fn hook_plane_available(&self) -> bool {
        false
    }

    /// A send can omit its plan after admission; a reader must still refuse the session's
    /// frozen transforms without opening the actor or advancing its in-flight run.
    async fn frozen_plan_requires_hooks(&self, _session: &LineageKey) -> bool {
        false
    }

    /// A later send can omit its plan; approval subscriptions still require the inbound
    /// route to carry a scope identity supplied by the daemon.
    async fn frozen_plan_requires_approval(&self, _session: &LineageKey) -> bool {
        false
    }

    /// The session-change identities for drift checks: the effective plan identity and the
    /// latest accepted plan identity, when the session has session-change state (it received
    /// a refresh). `None` when the session never received one, keeping today's plain
    /// different-plan refusal exactly.
    async fn session_change_identities(
        &self,
        _session: &LineageKey,
    ) -> Option<(Option<String>, Option<String>)> {
        None
    }

    /// Accept a `session.refresh`: fetch, record durably, and hold pending. Implemented by
    /// the registry through the lineage actor; the default has no session to refresh.
    async fn session_refresh(
        &self,
        _session: &LineageKey,
        _principal: Option<&broca_protocol::BindPrincipal>,
        _scope: &broca_protocol::SendScope,
        _plan: serde_json::Map<String, serde_json::Value>,
        _policy: String,
        _generation: u64,
    ) -> Result<cortexkit_role_llm_runner::session_change::RefreshReply, SessionChangeError> {
        Err(SessionChangeError::unsupported("session.refresh"))
    }

    /// Change the pending change's rung.
    async fn session_refresh_policy(
        &self,
        _session: &LineageKey,
        _principal: Option<&broca_protocol::BindPrincipal>,
        _scope: &broca_protocol::SendScope,
        _generation: u64,
        _policy: String,
        _send_id: String,
    ) -> Result<cortexkit_role_llm_runner::session_change::RefreshPolicyReply, SessionChangeError>
    {
        Err(SessionChangeError::unsupported("session.refresh_policy"))
    }

    /// Drop the frozen prefix and apply any pending change at the next run.
    async fn session_flush_prefix(
        &self,
        _session: &LineageKey,
        _principal: Option<&broca_protocol::BindPrincipal>,
        _scope: &broca_protocol::SendScope,
        _send_id: String,
    ) -> Result<cortexkit_role_llm_runner::session_change::FlushPrefixReply, SessionChangeError>
    {
        Err(SessionChangeError::unsupported("session.flush_prefix"))
    }
}

impl crate::LineageRegistry {
    /// The preflight's check of a refresh's compaction item against the latest plan the session
    /// accepted (`ChangeFacts::latest_manifest`). Keeping the item is always fine; removing or
    /// changing it is refused, and so is adding one on today's storage. On the new storage a
    /// session may gain one: the window must be whole, and a context limit must resolve for the
    /// plan's model (`plan_model`), else the session's, since a run on the compaction contract
    /// can't run without one and the provider can't be removed again. A limit resolution that
    /// couldn't run refuses transiently, with its cause. The plan's system text is checked once
    /// it's fetched, where the actor writes the refresh (`plan_change::accept_refresh`).
    async fn check_compaction_change(
        &self,
        facts: &crate::plan_change::ChangeFacts,
        plan: &serde_json::Value,
        plan_model: Option<cortexkit_role_llm_runner::head_selection::PlanModel>,
    ) -> Result<(), SessionChangeError> {
        use broca_fetch_plan::session_change::{
            compaction_item_change, compaction_item_refusal, CompactionItemChange,
        };
        use broca_session_loop::enroll::Refusal;
        let change = compaction_item_change(facts.latest_manifest(), plan)
            .map_err(SessionChangeError::FetchRefused)?;
        let enrollment = match (change, &facts.enrollment) {
            (CompactionItemChange::Same, _) => return Ok(()),
            (CompactionItemChange::Added, Some(enrollment)) => enrollment,
            _ => return Err(SessionChangeError::FetchRefused(compaction_item_refusal())),
        };
        if !enrollment.window_whole {
            return Err(SessionChangeError::CompactionNotAddable(
                Refusal::WindowNotWhole,
            ));
        }
        let Some(model) = plan_model.or_else(|| enrollment.session_model.clone()) else {
            return Err(SessionChangeError::CompactionNotAddable(
                Refusal::NoContextLimit,
            ));
        };
        context_limit_outcome(self.compaction_context_limit(&model).await, &model)
    }

    /// What a plan-change op's preflight reads about `session`, for a request naming
    /// `generation` and `send_id`. `wal_error` words a failure to read the session's WAL, as
    /// each op words it.
    async fn read_change(
        &self,
        session: &LineageKey,
        generation: Option<u64>,
        send_id: Option<&str>,
        wal_error: fn(String) -> String,
    ) -> Result<ChangeRead, SessionChangeError> {
        use crate::store_reads::{StoreReadReply, StoreReadRequest};
        // A session kept in the new storage is read from its fold and its id index.
        if let Some(store) = self
            .store_holding(session)
            .map_err(|error| SessionChangeError::Transient(wal_error(format!("{error:?}"))))?
        {
            let request = StoreReadRequest::PlanChange {
                generation,
                send_id: send_id.map(str::to_owned),
            };
            return match store.read(self, session, request).await {
                Ok(Some(StoreReadReply::PlanChange(Ok(read)))) => Ok(read.0),
                Ok(Some(StoreReadReply::PlanChange(Err(error)))) => {
                    Err(SessionChangeError::Transient(wal_error(error)))
                }
                // The session's creation never committed, so it has no run to change.
                Ok(None) => Ok(ChangeRead::default()),
                Ok(Some(_)) => Err(SessionChangeError::Transient(
                    "a store read was answered with another read's reply".into(),
                )),
                Err(error) => Err(SessionChangeError::Transient(wal_error(format!(
                    "{error:?}"
                )))),
            };
        }
        // A session that doesn't exist has no run to change.
        Ok(ChangeRead::default())
    }
}

#[async_trait]
impl ServeHandler for crate::LineageRegistry {
    fn cached_session_owner(
        &self,
        session: &LineageKey,
        principal: Option<&broca_protocol::BindPrincipal>,
    ) -> Option<bool> {
        crate::LineageRegistry::cached_session_owner(self, session, principal)
    }

    fn plan_readers_supported(
        &self,
        session: &LineageKey,
        _class: Option<&str>,
    ) -> Result<bool, ServeError> {
        if self.store_holding(session)?.is_some() {
            return Ok(true);
        }
        // A session that can't be created yet (its key still loading after a start) is refused
        // as retryable, as the opener and the send's own admission refuse it, never answered "not
        // supported", which refuses the plan for good.
        if !self.store_can_create() {
            if let Some(pending) = self.store_creation_pending() {
                return Err(pending.into());
            }
        }
        Ok(self.store_can_create())
    }
    async fn authorize_session_read(
        &self,
        session: &LineageKey,
        access: &crate::reader_admission::ReadAccess,
    ) -> Result<(), ServeError> {
        use crate::store_reads::{StoreReadReply, StoreReadRequest};
        if let Some(store) = self.store_holding(session)? {
            return match store
                .read(
                    self,
                    session,
                    StoreReadRequest::AuthorizationRead(access.clone()),
                )
                .await?
            {
                Some(StoreReadReply::AuthorizationRead(result)) => result,
                None => {
                    self.authorize_session(session, access.principal.as_ref())
                        .await
                }
                Some(_) => Err(crate::store_reads::mismatched()),
            };
        }
        self.authorize_session(session, access.principal.as_ref())
            .await
    }

    async fn fork_prepare(
        &self,
        session: &LineageKey,
        principal: Option<&broca_protocol::BindPrincipal>,
    ) -> Result<
        cortexkit_role_llm_runner::fork_answer::ForkPrepareReply,
        crate::fork_answer::ForkError,
    > {
        self.fork_manager()?.prepare(session, principal).await
    }

    async fn fork_ask(
        &self,
        session: &LineageKey,
        principal: Option<&broca_protocol::BindPrincipal>,
        request: cortexkit_role_llm_runner::fork_answer::ForkAskRequest,
    ) -> Result<cortexkit_role_llm_runner::fork_answer::ForkAskReply, crate::fork_answer::ForkError>
    {
        self.fork_manager()?.ask(session, principal, request).await
    }

    async fn fork_end(
        &self,
        session: &LineageKey,
        principal: Option<&broca_protocol::BindPrincipal>,
        request: cortexkit_role_llm_runner::fork_answer::ForkEndRequest,
    ) -> Result<cortexkit_role_llm_runner::fork_answer::ForkEndReply, crate::fork_answer::ForkError>
    {
        self.fork_manager()?.end(session, principal, request).await
    }

    async fn check_plan_selection(
        &self,
        plan: &serde_json::Value,
    ) -> Result<
        Option<cortexkit_role_llm_runner::head_selection::PlanModel>,
        cortexkit_role_llm_runner::head_selection::PlanModelProblem,
    > {
        crate::LineageRegistry::check_plan_selection(self, plan).await
    }
    async fn run_event_get(
        &self,
        session: &LineageKey,
        event_key: &str,
    ) -> Result<broca_wire::RunEventGetResponse, ServeError> {
        self.route_run_event(session, event_key).await
    }
    fn backup_classes(&self) -> Vec<String> {
        crate::LineageRegistry::backup_classes(self)
    }
    async fn memory_report(&self) -> Option<broca_wire::MemoryReport> {
        Some(crate::LineageRegistry::memory_report(self).await)
    }
    async fn check_migration(&self, session: &LineageKey, write: bool) -> Result<(), ServeError> {
        self.check_move(session, write).map_err(Into::into)
    }
    fn compaction_supported(&self) -> bool {
        crate::LineageRegistry::compaction_supported(self)
    }

    async fn compaction_runner(
        &self,
        session: &LineageKey,
        caller: Option<String>,
        method: String,
        params: Box<serde_json::value::RawValue>,
    ) -> Result<Box<serde_json::value::RawValue>, broca_core::compaction_v2::protocol::wire::Refusal>
    {
        self.route_compaction_v2(session, caller, method, params)
            .await
    }

    async fn tag_location(
        &self,
        session: &LineageKey,
        tag: u64,
    ) -> Result<Option<broca_core::TagLocation>, ServeError> {
        if let Some(store) = self.store_holding(session)? {
            use crate::store_reads::{StoreReadReply, StoreReadRequest};
            return match store
                .read(self, session, StoreReadRequest::TagLocation(tag))
                .await?
            {
                Some(StoreReadReply::TagLocation(location)) => location.map(Some),
                Some(_) => Err(crate::store_reads::mismatched()),
                // A session whose creation never committed holds no part.
                None => Ok(Some(broca_core::TagLocation {
                    found: None,
                    indexed_through: u64::MAX,
                })),
            };
        }
        // A session the store doesn't hold holds no part.
        Ok(Some(broca_core::TagLocation {
            found: None,
            indexed_through: u64::MAX,
        }))
    }

    async fn output_ref_history_version(
        &self,
        session: &LineageKey,
        served: u64,
    ) -> Result<u64, ServeError> {
        if self.store_holding(session)?.is_none() {
            return Ok(served);
        }
        crate::store_exposure::stored_history_version(served).ok_or(ServeError::HistoryChanged)
    }

    async fn output_ref_namespace(
        &self,
        session: &LineageKey,
        incarnation: &str,
    ) -> Result<String, ServeError> {
        use crate::store_reads::{StoreReadReply, StoreReadRequest};
        let Some(store) = self.store_holding(session)? else {
            return Ok(incarnation.to_owned());
        };
        match store
            .read(self, session, StoreReadRequest::OutputRefNamespace)
            .await?
        {
            Some(StoreReadReply::OutputRefNamespace(namespace)) => Ok(namespace),
            Some(_) => Err(crate::store_reads::mismatched()),
            None => Ok(incarnation.to_owned()),
        }
    }

    async fn store_writing_view(
        &self,
        session: &LineageKey,
        ordinal: u64,
    ) -> Result<Option<Option<broca_core::compaction_provider::view::CompactionView>>, ServeError>
    {
        use crate::store_reads::{StoreReadReply, StoreReadRequest};
        match self.store_holding(session)? {
            Some(store) => {
                match store
                    .read(self, session, StoreReadRequest::WritingView { ordinal })
                    .await?
                {
                    Some(StoreReadReply::WritingView(view)) => view.map(Some),
                    Some(_) => Err(crate::store_reads::mismatched()),
                    None => Ok(Some(None)),
                }
            }
            None => Ok(None),
        }
    }

    async fn frozen_manifest(
        &self,
        session: &LineageKey,
    ) -> Result<Option<std::sync::Arc<broca_wal::PlanManifest>>, ServeError> {
        Ok(self
            .session_ownership_for_bind(session)
            .await
            .and_then(|ownership| ownership.first_episode)
            .and_then(|first| first.manifest))
    }

    async fn authorize_session(
        &self,
        session: &LineageKey,
        principal: Option<&broca_protocol::BindPrincipal>,
    ) -> Result<(), ServeError> {
        if !broca_core::scope_admission::is_flow_only(principal) {
            return Ok(());
        }
        let sender = self.frozen_sender(session).await?;
        broca_core::scope_admission::decide_sender_access(
            principal,
            sender.as_ref().map(Option::as_ref),
        )
        .map_err(ServeError::ScopeRefused)
    }

    async fn authorize_session_owner(
        &self,
        session: &LineageKey,
        principal: Option<&broca_protocol::BindPrincipal>,
        scope: &broca_protocol::SendScope,
    ) -> Result<(), ServeError> {
        let ownership = self.owner_ownership(session).await?;
        broca_core::scope_admission::decide_owner_access(&ownership, principal, scope)
            .map_err(ServeError::ScopeRefused)
    }

    async fn authorize_session_cancel(
        &self,
        session: &LineageKey,
        principal: Option<&broca_protocol::BindPrincipal>,
        scope: &broca_protocol::SendScope,
        at: Option<broca_wal::record::RunCancelAt>,
    ) -> Result<(), ServeError> {
        // Admission must not synchronize the writer: even a pending StepStarted would
        // otherwise become durable before an invalid cancellation point is rejected.
        let refusal = match self
            .authorize_session_owner(session, principal, scope)
            .await
        {
            Ok(()) => return Ok(()),
            Err(ServeError::ScopeRefused(refusal)) => refusal,
            Err(error) => return Err(error),
        };
        if at != Some(broca_wal::record::RunCancelAt::StepBoundary) {
            if let Some(broca_protocol::BindPrincipal::Reserved { module_id }) = principal {
                if self.interrupt_cancel_granted(session, module_id).await? {
                    return Ok(());
                }
            }
        }
        Err(ServeError::ScopeRefused(refusal))
    }

    async fn authorize_session_retract(
        &self,
        session: &LineageKey,
        principal: Option<&broca_protocol::BindPrincipal>,
        scope: &broca_protocol::SendScope,
        submission_id: Option<&str>,
    ) -> Result<(), ServeError> {
        // Retraction admits the submission's recorded sender without requiring the
        // interrupt:true grant that lets a non-owner stop the running turn.
        match self
            .authorize_session_owner(session, principal, scope)
            .await
        {
            Err(refusal @ ServeError::ScopeRefused(_)) => {
                if let (Some(sender @ broca_protocol::BindPrincipal::Reserved { .. }), Some(id)) =
                    (principal, submission_id)
                {
                    if self.recorded_submission_sender(session, id).await?.as_ref() == Some(sender)
                    {
                        return Ok(());
                    }
                }
                Err(refusal)
            }
            result => result,
        }
    }

    async fn upload(
        &self,
        session: &LineageKey,
        principal: Option<&broca_protocol::BindPrincipal>,
        scope: &broca_protocol::SendScope,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, ServeError> {
        crate::uploads::upload(self, session, principal, scope, method, params).await
    }

    async fn media_read(
        &self,
        session: &LineageKey,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, ServeError> {
        crate::uploads::media_read(self, session, params).await
    }

    async fn send(&self, mut req: SendRequest) -> Result<SendResponse, ServeError> {
        use crate::actor::SendOutcome;
        // broca keeps no input-token budget and doesn't declare `input_token_cap`, so a send
        // naming one is refused before anything is staged or opened, rather than admitted and
        // then failed when its run starts.
        if req
            .request
            .budget
            .as_ref()
            .is_some_and(|budget| budget.max_input_tokens.is_some())
        {
            return Err(ServeError::InvalidParams {
                field: "budget.max_input_tokens".into(),
                message: "broca keeps no input-token budget (input_token_cap is not declared)"
                    .into(),
            });
        }
        // A session the store doesn't hold yet has no run, and an owner's scoped bind may have
        // reserved its address. Refuse anyone else's first send here, before staging media or
        // creating the session. Once the store holds it, its actor applies the reservation while
        // it has no run (`StoreActor::send_scope`).
        if req.request.scope != broca_protocol::SendScope::Internal
            && self.store_holding(&req.session)?.is_none()
        {
            // A reservation that cannot be read refuses the send as transient, never as "not
            // reserved", which would let anyone start the session.
            let reservation = self
                .scope_reservation(&req.session)
                .await
                .map_err(ServeError::Transient)?;
            let ownership = broca_core::SessionOwnershipV1 {
                reservation,
                ..Default::default()
            };
            broca_core::scope_admission::check_scope_reservation(
                &ownership,
                req.request.principal.as_ref(),
                &req.request.scope,
            )
            .map_err(ServeError::ScopeRefused)?;
        }
        if !req.request.content.is_empty() && !self.session_written(&req.session)? {
            let mut request = (*req.request).clone();
            crate::person_media::stage(self.upload_store().as_ref(), &req.session, &mut request)
                .map_err(media_preflight_error)?;
            let profile = self
                .media_profile(&request)
                .await
                .map_err(media_preflight_error)?;
            let model = format!(
                "{}/{}",
                request.model.provider_module_id, request.model.model_id
            );
            crate::person_media::check(profile.as_deref(), &model, &request.input)
                .map_err(media_preflight_error)?;
            if let Some(held) = &mut request.staged_uploads {
                std::sync::Arc::make_mut(held).profile = profile
                    .as_ref()
                    .map(|profile| serde_json::to_value(profile).expect("media profile JSON"));
            }
            req.request.staged_uploads = request.staged_uploads;
        }
        // Opening an actor creates the lineage header. A fresh run scope has no frozen
        // history to consult, so reject a wrong carrier or tools before creating any WAL.
        // Existing sessions still use the actor's authoritative frozen-ownership check.
        if broca_core::scope_admission::AGENT_RUN_SCOPES_WRITER_ENABLED
            && matches!(&req.request.scope, broca_protocol::SendScope::Stamped(bind) if bind.scope.run_id.is_some())
        {
            if let Some(ownership) = self.session_ownership_for_bind(&req.session).await {
                if ownership.episodes == 0 {
                    if let Some(field) = broca_core::scope_admission::model_only_scope_tool_field(
                        &req.request.scope,
                        req.request
                            .tools
                            .as_ref()
                            .is_some_and(|tools| !tools.is_empty()),
                        req.plan
                            .as_ref()
                            .and_then(|plan| plan.manifest.as_ref())
                            .is_some_and(|manifest| !manifest.tools.is_empty()),
                    ) {
                        return Err(ServeError::InvalidParams {
                            field: field.to_owned(),
                            message: format!("agent-run sends cannot carry {field}"),
                        });
                    }
                    broca_core::decide_scope(
                        &ownership,
                        req.request.principal.as_ref(),
                        &req.request.scope,
                    )
                    .map_err(ServeError::ScopeRefused)?;
                }
            }
        }
        // Opening a fresh actor writes the WAL's first frame, so refuse a scoped send with
        // legacy tools before opening it. A session that already has episodes goes to the actor,
        // so a resent `send_id` of an admitted run gets its recorded answer before the tool
        // check runs.
        if req.plan.is_none()
            && req
                .request
                .tools
                .as_ref()
                .is_some_and(|tools| !tools.is_empty())
        {
            if let Some(ownership) = self.session_ownership_for_bind(&req.session).await {
                if ownership.episodes == 0 {
                    let decision = broca_core::decide_scope(
                        &ownership,
                        req.request.principal.as_ref(),
                        &req.request.scope,
                    )
                    .map_err(ServeError::ScopeRefused)?;
                    if decision.scope().is_some() {
                        return Err(ServeError::PlanRefused(
                            broca_fetch_plan::Refusal::ScopedToolsRequirePlan,
                        ));
                    }
                }
            }
        }
        match self
            .route_submission_with_order(
                &req.session,
                req.run_id,
                *req.request,
                req.kind,
                match req.send_id {
                    Some(send_id) => crate::actor::SendKey::SendId(send_id),
                    None => crate::actor::SendKey::Minted,
                },
                req.skip_materialization,
                req.delivery,
                req.plan,
                req.conversation_key,
                req.title,
                req.retention,
                req.session_class,
                req.stop_sequences_named,
                req.order,
            )
            .await?
        {
            SendOutcome::RetentionInvalid(detail) => Err(ServeError::RetentionInvalid(detail)),
            SendOutcome::SessionClassInvalid(message) => Err(ServeError::InvalidParams {
                field: "session_class".into(),
                message,
            }),
            SendOutcome::InputTokenBudgetInvalid(message) => Err(ServeError::InvalidParams {
                field: "budget.max_input_tokens".into(),
                message,
            }),
            SendOutcome::ContextCeiling(detail) => Err(ServeError::ContextCeiling(detail)),
            SendOutcome::StructuredOutputInvalid { field } => Err(ServeError::InvalidParams {
                message: format!("{field} is not admissible for this send"),
                field,
            }),
            SendOutcome::Finished {
                run_id,
                reason,
                delivered,
            } => Ok(SendResponse::Finished {
                run_id: run_id.0,
                reason,
                delivered,
            }),
            SendOutcome::Paused {
                run_id,
                reason,
                delivered,
            } => Ok(SendResponse::Paused {
                run_id: run_id.0,
                pause_reason: reason,
                delivered,
            }),
            SendOutcome::SessionMoving { cut_id } => Err(ServeError::SessionMoving { cut_id }),
            SendOutcome::SessionMoved { holder } => Err(ServeError::SessionMoved { holder }),
            SendOutcome::Active { run_id, delivered } => Ok(SendResponse::Active {
                run_id: run_id.0,
                delivered,
            }),
            SendOutcome::Pending { submission_id } => Ok(SendResponse::Pending {
                submission_id: submission_id.0,
            }),
            // Reopen is consumed by the registry's retry loop; Error surfaces as Open.
            SendOutcome::Reopen => unreachable!("registry retry consumes Reopen"),
            SendOutcome::Retracted { submission_id } => Err(ServeError::SendRetracted {
                submission_id: submission_id.0,
            }),
            SendOutcome::SendEnded {
                submission_id,
                reason,
            } => Err(ServeError::SendEnded {
                submission_id: submission_id.0,
                outcome: reason.as_str(),
            }),
            SendOutcome::DeliveryRefused {
                code: "invalid_params",
                field,
                message,
            } => Err(ServeError::InvalidParams {
                field: field.into(),
                message,
            }),
            SendOutcome::DeliveryRefused { code, message, .. } => {
                Err(ServeError::DeliveryRefused { code, message })
            }
            SendOutcome::TemplatePartGone { slot } => Err(ServeError::TemplatePartGone { slot }),
            SendOutcome::SendIdReuse { differing } => Err(ServeError::SendIdReuse {
                differing: differing.to_owned(),
            }),
            SendOutcome::SteerSettingsConflict { field } => Err(ServeError::InvalidParams {
                field: field.to_owned(),
                message: format!("joining steer cannot change the active run's frozen {field}"),
            }),
            SendOutcome::VariantUnsupported(e) => Err(ServeError::VariantUnsupported(e)),
            SendOutcome::ToolAliasInvalid(e) => Err(ServeError::ToolAliasInvalid(e)),
            SendOutcome::RouteDecisionFixed => Err(ServeError::InvalidParams {
                field: "route_decision_id".into(),
                message: "route_decision_id is fixed for the active run".into(),
            }),
            SendOutcome::AccountRouteFixed => Err(ServeError::InvalidParams {
                field: "account_route".into(),
                message: "account_route is fixed for the active run".into(),
            }),
            SendOutcome::ServiceTierInvalid { reason, message } => {
                Err(ServeError::ServiceTierInvalid { reason, message })
            }
            SendOutcome::Upload(error) => Err(ServeError::Upload(error)),
            SendOutcome::MediaTooLarge(detail) => Err(ServeError::MediaTooLarge(detail)),
            // Actor errors can combine replay, storage and lifecycle failures after type erasure.
            SendOutcome::Error(e) => Err(ServeError::Open(e, None)),
            SendOutcome::ClassifiedError { reason, class } => {
                Err(ServeError::Open(reason, Some(class)))
            }
            SendOutcome::ModuleStopping(at) => Err(ServeError::ModuleStopping(at)),
            SendOutcome::NotServedOnNewStorage { feature } => {
                Err(ServeError::NotServedOnNewStorage { feature })
            }
            SendOutcome::SessionTooLarge { max_bytes } => {
                Err(ServeError::SessionTooLarge { max_bytes })
            }
            SendOutcome::RetainedSessionTooLarge { max_bytes } => {
                Err(ServeError::RetainedSessionTooLarge { max_bytes })
            }
            SendOutcome::NewerWriter { missing, reason } => {
                Err(ServeError::WalNewerWriter { missing, reason })
            }
            SendOutcome::Transient(e) => Err(ServeError::Transient(e)),
            SendOutcome::CredentialsUnavailable => Err(ServeError::CredentialsUnavailable),
            SendOutcome::CredentialAmbiguous {
                credential,
                candidate_ids,
            } => Err(ServeError::CredentialAmbiguous {
                credential,
                candidate_ids,
            }),
            SendOutcome::AccountSelectionRefused(refusal) => {
                Err(ServeError::AccountSelectionRefused(refusal))
            }
            SendOutcome::RoutingUnavailable(unavailable) => {
                Err(ServeError::RoutingUnavailable(unavailable))
            }
            SendOutcome::ScopeRefused(refusal) => Err(ServeError::ScopeRefused(refusal)),
            SendOutcome::ScopeFreezeBusy { reason, run_id } => Err(ServeError::ScopeFreezeBusy {
                reason,
                run_id: run_id.map(|run_id| run_id.0),
            }),
            SendOutcome::RunPaused { run_id, reason } => {
                Err(ServeError::RunPaused(RunPausedRefusal {
                    run_id: run_id.0,
                    reason,
                }))
            }
            SendOutcome::PlanRefused(refusal) => Err(ServeError::PlanRefused(refusal)),
            SendOutcome::PreUserUnavailable { provider } => {
                Err(ServeError::PreUserUnavailable { provider })
            }
        }
    }

    async fn send_status(
        &self,
        session: &LineageKey,
        own_run: broca_protocol::RunId,
        send_id: &str,
        principal: Option<&broca_protocol::BindPrincipal>,
    ) -> Result<broca_wire::SendReplyWire, ServeError> {
        if let Some(store) = self.store_holding(session)? {
            return crate::store_status::StoreRead::new(&store, session, self)
                .send_status(own_run, send_id, principal)
                .await;
        }
        Err(ServeError::UnknownSend)
    }

    async fn plan_state(&self, session: &LineageKey) -> PlanState {
        match self.session_ownership_for_bind(session).await {
            None => PlanState::Unknown,
            Some(ownership) if ownership.episodes == 0 || ownership.first_episode.is_none() => {
                PlanState::NotAdmitted
            }
            Some(ownership) => PlanState::Admitted {
                frozen: ownership
                    .first_episode
                    .and_then(|first| first.manifest)
                    .map(|manifest| manifest.plan_identity.clone()),
            },
        }
    }

    async fn frozen_scope_opener(&self, session: &LineageKey) -> Option<String> {
        self.session_ownership_for_bind(session)
            .await
            .and_then(|ownership| ownership.scope)
            .as_ref()
            .and_then(scope_plan_opener)
    }

    fn plan_admitter(&self) -> Option<std::sync::Arc<dyn broca_fetch_plan::PlanAdmitter>> {
        crate::LineageRegistry::plan_admitter(self)
    }

    fn hook_plane_available(&self) -> bool {
        crate::LineageRegistry::hook_plane_available(self)
    }

    async fn frozen_plan_requires_hooks(&self, session: &LineageKey) -> bool {
        self.session_ownership_for_bind(session)
            .await
            .and_then(|ownership| ownership.first_episode)
            .and_then(|first| first.manifest)
            .is_some_and(|manifest| broca_core::hooks::has_transforms(&manifest))
    }

    async fn frozen_plan_requires_approval(&self, session: &LineageKey) -> bool {
        self.session_ownership_for_bind(session)
            .await
            .and_then(|ownership| ownership.first_episode)
            .and_then(|first| first.manifest)
            .and_then(|manifest| {
                serde_json::from_str::<serde_json::Value>(manifest.plan.as_json_text()).ok()
            })
            .and_then(|plan| {
                plan.get("step_transform_items")
                    .and_then(serde_json::Value::as_array)
                    .cloned()
            })
            .is_some_and(|items| {
                items.iter().any(|item| {
                    item.get("subscriptions")
                        .and_then(serde_json::Value::as_array)
                        .is_some_and(|subscriptions| {
                            subscriptions.iter().any(|subscription| {
                                subscription
                                    .get("phase")
                                    .and_then(serde_json::Value::as_str)
                                    == Some("approve")
                            })
                        })
                })
            })
    }

    async fn bind_adoption_refusal(
        &self,
        session: &LineageKey,
        principal: Option<&broca_protocol::BindPrincipal>,
        scope: &broca_protocol::BindScope,
    ) -> Result<Option<broca_core::AdoptionRefusal>, ServeError> {
        let ownership = match self.bind_ownership(session).await {
            crate::registry::BindOwnership::Known(ownership) => ownership,
            crate::registry::BindOwnership::NoHistorySource => return Ok(None),
            // Unknown history must not read as "nothing refuses the stamp": that would let a
            // bind adopt a scope its history forbids whenever the history could not be read.
            crate::registry::BindOwnership::Unreadable(reason) => {
                return Err(ServeError::Open(
                    format!("route.bind adoption check: session history unreadable: {reason}"),
                    Some(broca_protocol::ErrorClass::Transient),
                ));
            }
        };
        // A stamp of another scope meets the refusal its first send would get, so the route is
        // never acknowledged for a session whose address another scope's bind reserved.
        let stamped = broca_protocol::SendScope::Stamped(scope.clone());
        broca_core::scope_admission::check_scope_reservation(&ownership, principal, &stamped)
            .map_err(ServeError::ScopeRefused)?;
        match broca_core::decide_scope(&ownership, principal, &stamped) {
            Err(broca_core::ScopeRefusal::AdoptionRefused(refusal)) => Ok(Some(refusal)),
            _ => Ok(None),
        }
    }

    async fn store_command(
        &self,
        session: &LineageKey,
        command: crate::store_import::StoreCommand,
    ) -> Result<crate::store_import::StoreOutcome, ServeError> {
        Ok(self.route_store_command(session, command).await?)
    }

    fn import_roots(&self) -> Option<std::sync::Arc<crate::store_import::ImportRoots>> {
        self.store_runtime().map(|runtime| runtime.import_roots())
    }

    fn kept_in_new_storage(&self, session: &LineageKey) -> Result<bool, ServeError> {
        Ok(self.store_holding(session)?.is_some())
    }

    async fn restore_from_backup(
        &self,
        session: &LineageKey,
        all: bool,
    ) -> Result<serde_json::Value, crate::registry::store_move::MoveRefusal> {
        crate::LineageRegistry::restore_from_backup(self, session, all).await
    }

    async fn plan_view(
        &self,
        session: &LineageKey,
    ) -> Result<Option<broca_wire::plan_view::PlanViewResponse>, crate::store_plan_view::Error>
    {
        if let Some(store) = self.store_holding(session).map_err(ServeError::from)? {
            return crate::store_plan_view::serve(self, &store, session)
                .await
                .map(Some);
        }
        Err(crate::store_plan_view::Error::NoPlan)
    }

    async fn tag_map(
        &self,
        session: &LineageKey,
        params: &broca_wire::tag_map::TagMapParams,
    ) -> Result<Option<broca_wire::tag_map::TagMapResponse>, ServeError> {
        if let Some(store) = self.store_holding(session)? {
            return crate::store_tag_map::serve(self, &store, session, params)
                .await
                .map(Some);
        }
        Ok(Some(broca_wire::tag_map::TagMapResponse::unnumbered()))
    }

    async fn tag_read(
        &self,
        session: &LineageKey,
        request: &cortexkit_role_llm_runner::tag_read::TagReadRequest,
        principal: Option<&broca_protocol::BindPrincipal>,
        scope: &broca_protocol::SendScope,
    ) -> Result<Option<cortexkit_role_llm_runner::tag_read::TagReadAnswer>, ServeError> {
        self.check_expired(session, None).await?;
        let Some(store) = self.store_holding(session)? else {
            return Ok(None);
        };
        crate::store_tag_read::check_owner(self, session, request, principal, scope).await?;
        let Some(view) = store.current_view(session).await? else {
            return Err(ServeError::TagReadRefused {
                code: cortexkit_role_llm_runner::errors::UNKNOWN_CALL,
                message: format!("no call has key {}", request.call_key),
            });
        };
        let resolved =
            crate::store_tag_read::resolve(&view, request.tag, &request.call_key).await?;
        // The entry as `session.read` serves it, with display facts, as the contract asks.
        let mut read = SessionReadRequest::new(session.clone(), Some(resolved.ordinal), Some(1));
        read.include_display_facts = true;
        let message = self
            .read(read)
            .await?
            .messages
            .into_iter()
            .find(|message| message.ordinal == resolved.ordinal)
            .ok_or_else(|| {
                ServeError::Open(
                    format!("message {} didn't read back", resolved.ordinal),
                    Some(broca_protocol::ErrorClass::Permanent),
                )
            })?;
        let entry = serde_json::to_value(&message)
            .and_then(serde_json::from_value)
            .map_err(|error| {
                ServeError::Open(
                    format!("message {} as a read entry: {error}", resolved.ordinal),
                    Some(broca_protocol::ErrorClass::Permanent),
                )
            })?;
        Ok(Some(
            cortexkit_role_llm_runner::tag_read::TagReadAnswer::new(entry, resolved.part),
        ))
    }

    async fn request_dump(
        &self,
        session: &LineageKey,
        run_id: &str,
    ) -> Result<Option<broca_wire::request_dump::RequestDumpResponse>, ServeError> {
        match self.store_holding(session)? {
            Some(store) => crate::store_request_dump::serve(&store, session, run_id).await,
            None => Ok(None),
        }
    }

    async fn delete_session(
        &self,
        session: &LineageKey,
    ) -> Result<Option<broca_wire::session_delete::DeleteResponse>, ServeError> {
        use crate::retention::store::OperatorDeletion;
        let Some(store) = self.store_holding(session)? else {
            // An address holding no session has nothing to delete.
            return Ok(Some(broca_wire::session_delete::DeleteResponse {
                deleted: false,
            }));
        };
        let retention = self.handle().retention_service().ok_or_else(|| {
            ServeError::Transient("retention is not running in this module yet".into())
        })?;
        let now_ms = retention.clock.now_ms();
        match retention
            .delete_store_session_for_operator(&store, session, now_ms)
            .await
            .map_err(|error| ServeError::Open(error, None))?
        {
            OperatorDeletion::Deleted => Ok(Some(broca_wire::session_delete::DeleteResponse {
                deleted: true,
            })),
            OperatorDeletion::Absent => Ok(Some(broca_wire::session_delete::DeleteResponse {
                deleted: false,
            })),
            OperatorDeletion::Busy(work) => Err(ServeError::SessionBusy(format!(
                "the session can't be deleted: {work}"
            ))),
            OperatorDeletion::Held => Err(ServeError::Transient(
                "the session is open or moving; it can be deleted once it closes idle".into(),
            )),
        }
    }

    async fn import(&self, req: SessionImportRequest) -> Result<SessionImportResponse, ServeError> {
        use crate::actor::ImportOutcome;
        // Refused before the session is routed or opened, so a transcript naming media inline
        // writes nothing: a person names media only through upload references.
        if let Err((field, message)) = broca_wire::check_supplied_media(&req.messages) {
            return Err(ServeError::InvalidParams { field, message });
        }
        // An import writes the session's first episode and records its principal, so on a
        // session with no episode it meets the scope reservation exactly as a first send does,
        // before the session is opened.
        if req.scope != broca_protocol::SendScope::Internal {
            match self.bind_ownership(&req.session).await {
                crate::registry::BindOwnership::Known(ownership) => {
                    broca_core::scope_admission::check_scope_reservation(
                        &ownership,
                        req.principal.as_ref(),
                        &req.scope,
                    )
                    .map_err(ServeError::ScopeRefused)?;
                }
                crate::registry::BindOwnership::NoHistorySource => {}
                crate::registry::BindOwnership::Unreadable(reason) => {
                    return Err(ServeError::Open(
                        format!("session.import owner check: session history unreadable: {reason}"),
                        Some(broca_protocol::ErrorClass::Transient),
                    ));
                }
            }
        }
        let request = broca_core::ImportRequest {
            project_id: req.project_id,
            principal: req.principal,
            run_id: req.run_id,
            session: req.session.clone(),
            messages: req.messages,
            origin: broca_core::ImportOrigin {
                source_ref: req.source_ref,
            },
        };
        match self.route_import(&req.session, request).await? {
            ImportOutcome::SessionMoving { cut_id } => Err(ServeError::SessionMoving { cut_id }),
            ImportOutcome::SessionMoved { holder } => Err(ServeError::SessionMoved { holder }),
            ImportOutcome::Imported(outcome) => Ok(SessionImportResponse {
                run_id: outcome.run_id.0,
                synthesized_tool_results: outcome.synthesized_tool_results,
                dropped_orphan_tool_results: outcome.dropped_orphan_tool_results,
                dropped_system_messages: Some(outcome.dropped_system_messages),
                already_imported: outcome.already_imported,
            }),
            ImportOutcome::Busy => Err(ServeError::ImportBusy),
            ImportOutcome::Reopen => unreachable!("registry retry consumes Reopen"),
            ImportOutcome::Error(broca_core::ImportError::ImportIdReuse) => {
                Err(ServeError::ImportIdReuse)
            }
            ImportOutcome::Error(broca_core::ImportError::SuppliedMedia(found)) => {
                Err(ServeError::InvalidParams {
                    field: found.path(),
                    message: found.to_string(),
                })
            }
            ImportOutcome::Error(error) => Err(ServeError::Import(error.to_string())),
        }
    }

    async fn retract(
        &self,
        session: &LineageKey,
        submission_id: broca_protocol::SubmissionId,
    ) -> Result<RetractResponse, ServeError> {
        use crate::actor::RetractOutcome;
        match self.route_retract(session, submission_id).await? {
            RetractOutcome::SessionMoving { cut_id } => Err(ServeError::SessionMoving { cut_id }),
            RetractOutcome::SessionMoved { holder } => Err(ServeError::SessionMoved { holder }),
            RetractOutcome::SessionTooLarge { max_bytes } => {
                Err(ServeError::SessionTooLarge { max_bytes })
            }
            RetractOutcome::RetainedSessionTooLarge { max_bytes } => {
                Err(ServeError::RetainedSessionTooLarge { max_bytes })
            }
            RetractOutcome::Retracted => Ok(RetractResponse::Retracted),
            RetractOutcome::AlreadyStarted => Ok(RetractResponse::AlreadyStarted),
            RetractOutcome::NotPending => Ok(RetractResponse::NotPending),
            RetractOutcome::Reopen => unreachable!("registry retry consumes Reopen"),
            // This string combines WAL reads and writes; the original cause is unavailable.
            RetractOutcome::Error(e) => Err(ServeError::Open(e, None)),
        }
    }

    async fn subscribe(
        &self,
        session: &LineageKey,
        from: SubscribeFrom,
        sink: tokio::sync::mpsc::Sender<Delivery>,
    ) -> Result<(), ServeError> {
        use crate::actor::SubscribeOutcome;
        let outcome = match from {
            SubscribeFrom::Start => {
                self.route_subscribe(session, FromCursor::Start, sink)
                    .await?
            }
            SubscribeFrom::Live => self.route_subscribe_live(session, sink).await?,
            SubscribeFrom::After { wal_seq, sub_index } => {
                self.route_subscribe(
                    session,
                    FromCursor::After(broca_core::Cursor::new(wal_seq, sub_index)),
                    sink,
                )
                .await?
            }
            SubscribeFrom::Resume { cursor } => {
                let (Some(incarnation), Some(digest)) =
                    (cursor.incarnation.as_deref(), cursor.digest.as_deref())
                else {
                    return Err(ServeError::CursorExpired);
                };
                let proof = crate::fanout::ResumeCursorProof::new(incarnation, digest).ok_or_else(
                    || ServeError::InvalidParams {
                        field: "from".into(),
                        message: "subscribe cursor has malformed identity".into(),
                    },
                )?;
                self.route_subscribe(
                    session,
                    FromCursor::Resume {
                        cursor: broca_core::Cursor::new(cursor.wal_seq, cursor.sub_index),
                        proof,
                    },
                    sink,
                )
                .await?
            }
        };
        match outcome {
            SubscribeOutcome::Attached => Ok(()),
            SubscribeOutcome::Reopen => unreachable!("registry retry consumes Reopen"),
            // Attachment errors erase both replay and channel failures.
            SubscribeOutcome::Error(e) => Err(ServeError::Open(e, None)),
        }
    }

    async fn read(&self, req: SessionReadRequest) -> Result<SessionReadResponse, ServeError> {
        crate::registry::count_session_read();
        match self.store_holding(&req.session)? {
            Some(store) => crate::store_read::serve_read(self, &store, req).await,
            None => {
                // Retention deletes an expired session and leaves only its expiry marker, so
                // the address must still read as expired, not as never written.
                self.check_expired(&req.session, None).await?;
                crate::store_read::serve_unwritten(&req)
            }
        }
    }

    async fn store_history_head(
        &self,
        session: &LineageKey,
        mid: Option<&str>,
    ) -> Result<Option<crate::store_entries::HistoryHead>, ServeError> {
        match self.store_holding(session)? {
            Some(store) => crate::store_entries::history_head(self, &store, session, mid)
                .await
                .map(Some),
            None => Ok(None),
        }
    }

    async fn head(&self, session: &LineageKey) -> Result<broca_wire::HeadResponse, ServeError> {
        self.head_for(session, None, &broca_protocol::SendScope::default())
            .await
    }

    async fn message_identity(
        &self,
        _session: &LineageKey,
    ) -> Result<(bool, Option<String>), ServeError> {
        // The store keeps each message's public id as it was minted or converted, so its
        // stream's ids need no translation.
        Ok((false, None))
    }

    async fn head_for(
        &self,
        session: &LineageKey,
        principal: Option<&broca_protocol::BindPrincipal>,
        scope: &broca_protocol::SendScope,
    ) -> Result<broca_wire::HeadResponse, ServeError> {
        match self.store_holding(session)? {
            Some(store) => {
                crate::store_head::serve_head(self, &store, session, principal, scope).await
            }
            // A session no send created: its head names nothing, as an unwritten one's does.
            // One retention deleted still answers expired.
            None => {
                self.check_expired(session, None).await?;
                Ok(broca_wire::HeadResponse::default())
            }
        }
    }

    async fn warm(
        &self,
        session: &LineageKey,
        lineage_id: Option<String>,
    ) -> Result<broca_wire::WarmReply, ServeError> {
        use crate::actor::WarmOutcome;
        match self.route_warm(session, lineage_id).await? {
            WarmOutcome::SessionMoving { cut_id } => Err(ServeError::SessionMoving { cut_id }),
            WarmOutcome::SessionMoved { holder } => Err(ServeError::SessionMoved { holder }),
            WarmOutcome::Warmed(reply) => Ok(reply),
            WarmOutcome::Refused(refusal) => Err(ServeError::WarmRefused(Box::new(refusal))),
            WarmOutcome::Reopen => unreachable!("registry retry consumes Reopen"),
            WarmOutcome::Transient(error) => Err(ServeError::Transient(error)),
            // Warm errors erase replay, storage and actor failures.
            WarmOutcome::Error(error) => Err(ServeError::Open(error, None)),
        }
    }

    async fn run_result(
        &self,
        session: &LineageKey,
        run_id: broca_protocol::RunId,
    ) -> Result<broca_wire::RunResultResponse, ServeError> {
        if let Some(store) = self.store_holding(session)? {
            return crate::store_status::StoreRead::new(&store, session, self)
                .run_result(&run_id)
                .await;
        }
        self.check_expired(session, Some(&run_id.0)).await?;
        Err(ServeError::UnknownRun(run_id.0))
    }

    async fn run_tool_calls(
        &self,
        session: &LineageKey,
        request: cortexkit_role_llm_runner::tool_calls::ToolCallsRequest,
    ) -> Result<cortexkit_role_llm_runner::tool_calls::ToolCallsPage, ServeError> {
        if let Err(field) = request.check() {
            return Err(ServeError::InvalidParams {
                field: field.into(),
                message: "run.tool_calls.limit must be greater than zero".into(),
            });
        }
        if let Some(store) = self.store_holding(session)? {
            return crate::store_status::StoreRead::new(&store, session, self)
                .run_tool_calls(request)
                .await;
        }
        self.check_expired(session, Some(&request.run_id)).await?;
        Err(ServeError::UnknownRun(request.run_id))
    }

    async fn baseline(
        &self,
        session: &LineageKey,
        principal: Option<&broca_protocol::BindPrincipal>,
        scope: &broca_protocol::SendScope,
    ) -> Result<broca_wire::BaselineReply, ServeError> {
        if let Some(store) = self.store_holding(session)? {
            return crate::store_status::StoreRead::new(&store, session, self)
                .baseline(principal, scope)
                .await;
        }
        Ok(broca_wire::BaselineReply::not_yet())
    }

    fn admission_baseline(
        &self,
        session: &LineageKey,
        run_id: &broca_protocol::RunId,
    ) -> Option<broca_wire::BaselineWire> {
        self.first_episode(session)
            .filter(|first| &first.run_id == run_id)
            .map(|first| {
                let mut baseline = match &first.manifest {
                    Some(manifest) => planned_baseline(manifest),
                    None => broca_wire::BaselineWire::plan_less(first.tool_names),
                };
                baseline.cache = self
                    .admitted_cache(session, run_id)
                    .map(|declaration| declaration.to_wire());
                // The admitting send carries the same rungs `session.baseline` will report.
                baseline.rungs = Some(supported_rungs());
                baseline
            })
    }

    async fn session_change_identities(
        &self,
        session: &LineageKey,
    ) -> Option<(Option<String>, Option<String>)> {
        use crate::store_reads::{StoreReadReply, StoreReadRequest};
        if let Some(store) = self.store_holding(session).ok()? {
            return match store
                .read(self, session, StoreReadRequest::PlanIdentities)
                .await
                .ok()??
            {
                StoreReadReply::PlanIdentities(identities) => identities.ok()?,
                _ => None,
            };
        }
        None
    }

    async fn session_refresh(
        &self,
        session: &LineageKey,
        principal: Option<&broca_protocol::BindPrincipal>,
        scope: &broca_protocol::SendScope,
        plan: serde_json::Map<String, serde_json::Value>,
        policy: String,
        generation: u64,
    ) -> Result<cortexkit_role_llm_runner::session_change::RefreshReply, SessionChangeError> {
        use cortexkit_role_llm_runner::{rungs, session_change};
        let supported = self
            .store_holding(session)
            .map_err(|error| SessionChangeError::Transient(error.to_string()))?
            .is_some();
        crate::reader_admission::check_plan(&plan, supported).map_err(|error| match error {
            ServeError::InvalidParams { field, message } => {
                SessionChangeError::InvalidParams { field, message }
            }
            error => SessionChangeError::Transient(error.to_string()),
        })?;
        let _change_guard = self.lock_session_change(session).await;
        let read = self
            .read_change(session, Some(generation), None, |error| {
                format!("cannot read the session WAL: {error}")
            })
            .await?;
        read.check_owner(principal, scope)?;
        let senders =
            broca_fetch_plan::plan_senders(&plan).map_err(SessionChangeError::FetchRefused)?;
        cortexkit_role_llm_runner::scope_admission::check_plan_senders(
            read.opener.as_deref(),
            &senders,
        )
        .map_err(|field| {
            SessionChangeError::FetchRefused(broca_fetch_plan::Refusal::invalid(field))
        })?;
        if !rungs::ALL.contains(&policy.as_str()) {
            return Err(SessionChangeError::InvalidParams {
                field: "policy".into(),
                message: format!(
                    "policy must be one of {}; got {policy}",
                    rungs::ALL.join(", ")
                ),
            });
        }
        let plan_value = serde_json::Value::Object(plan.clone());
        let plan_identity = broca_fetch_plan::plan_identity(&plan_value).map_err(|_| {
            SessionChangeError::InvalidParams {
                field: "plan".into(),
                message: "the plan is not canonical JSON".into(),
            }
        })?;
        if let Some((run_in_progress, applies_from_next_run)) =
            crate::plan_change::refresh_order(&read.facts, generation, &plan_identity, &policy)?
        {
            return Ok(session_change::RefreshReply::new(
                cortexkit_role_llm_runner::baseline::PendingChange {
                    generation,
                    policy: policy.clone(),
                },
                run_in_progress,
                applies_from_next_run,
            ));
        }
        let plan_model = self
            .check_plan_selection(&plan_value)
            .await
            .map_err(|problem| {
                SessionChangeError::FetchRefused(broca_fetch_plan::Refusal::InvalidParams {
                    detail: problem.detail(),
                })
            })?;
        self.check_compaction_change(&read.facts, &plan_value, plan_model)
            .await?;
        // Fetch with the same admission code as a first send, never a second copy: parse
        // the plan, then admit its providers under the deadline. Refusals write nothing.
        let output_refs = if self
            .plan_admitter()
            .is_some_and(|admitter| admitter.admits_output_refs())
        {
            broca_fetch_plan::OutputRefsSupport::Supported
        } else {
            broca_fetch_plan::OutputRefsSupport::Unsupported
        };
        {
            let params_map = {
                let mut map = serde_json::Map::new();
                map.insert("plan".into(), serde_json::Value::Object(plan));
                map
            };
            if let Err(refusal) =
                broca_fetch_plan::check_send_members_with(&params_map, output_refs)
            {
                return Err(SessionChangeError::FetchRefused(refusal));
            }
        }
        let support = if self.compaction_supported() {
            broca_fetch_plan::CompactionSupport::Supported
        } else {
            broca_fetch_plan::CompactionSupport::Unsupported
        };
        let parsed = broca_fetch_plan::parse_plan_with(&plan_value, support, output_refs)
            .map_err(SessionChangeError::FetchRefused)?;
        if !parsed.step_items.is_empty() && !self.hook_plane_available() {
            return Err(SessionChangeError::FetchRefused(
                broca_fetch_plan::Refusal::invalid("plan.step_transform_items"),
            ));
        }
        let Some(admitter) = self.plan_admitter() else {
            return Err(SessionChangeError::FetchRefused(
                broca_fetch_plan::Refusal::invalid("plan"),
            ));
        };
        // Scoped and approval checks ride the same gates as a first send, before any fetch.
        let scope_for_admit = match scope {
            broca_protocol::SendScope::Stamped(stamp) => Some(&stamp.scope),
            _ => None,
        };
        if scope_for_admit.is_none()
            && parsed.step_items.iter().any(|item| {
                item.subscriptions.iter().any(|subscription| {
                    subscription.phase
                        == Some(
                            cortexkit_role_step_transform_provider::subscription::Phase::Approve,
                        )
                })
            })
        {
            return Err(SessionChangeError::FetchRefused(
                broca_fetch_plan::Refusal::invalid("plan.step_transform_items"),
            ));
        }
        if scope_for_admit.is_none()
            && parsed
                .tool_items
                .iter()
                .any(|item| item.preset.as_deref() == Some(broca_fetch_plan::CONFINED_TOOL_PRESET))
        {
            return Err(SessionChangeError::FetchRefused(
                broca_fetch_plan::Refusal::invalid("plan.tool_items"),
            ));
        }
        let admitted = admitter
            .admit(&parsed, session, scope_for_admit)
            .await
            .map_err(SessionChangeError::FetchRefused)?;
        if let Some(surface) = broca_fetch_plan::session_change::unsupported_surface(
            read.facts.current_manifest.as_deref(),
            &admitted.manifest,
            &policy,
        ) {
            return Err(SessionChangeError::RungUnsupported {
                surface: surface.into(),
                rung: policy,
            });
        }
        // Durably record the accepted generation, policy and fetched manifest before replying.
        match self
            .route_session_refresh(
                session,
                generation,
                policy.clone(),
                Box::new(admitted.manifest),
            )
            .await
        {
            Ok(crate::actor::SessionRefreshOutcome::Accepted {
                run_in_progress,
                applies_from_next_run,
            }) => Ok(session_change::RefreshReply::new(
                cortexkit_role_llm_runner::baseline::PendingChange { generation, policy },
                run_in_progress,
                applies_from_next_run,
            )),
            Ok(crate::actor::SessionRefreshOutcome::Reopen) => Err(SessionChangeError::Transient(
                "the session actor is closing; retry the refresh".into(),
            )),
            Ok(crate::actor::SessionRefreshOutcome::Refused(error)) => Err(error),
            Ok(crate::actor::SessionRefreshOutcome::Error(error)) => {
                Err(SessionChangeError::Transient(error))
            }
            Err(error) => Err(SessionChangeError::Transient(error.to_string())),
        }
    }

    async fn session_refresh_policy(
        &self,
        session: &LineageKey,
        principal: Option<&broca_protocol::BindPrincipal>,
        scope: &broca_protocol::SendScope,
        generation: u64,
        policy: String,
        send_id: String,
    ) -> Result<cortexkit_role_llm_runner::session_change::RefreshPolicyReply, SessionChangeError>
    {
        use cortexkit_role_llm_runner::{rungs, session_change};
        let _change_guard = self.lock_session_change(session).await;
        let read = self
            .read_change(session, Some(generation), Some(&send_id), |error| error)
            .await?;
        read.check_owner(principal, scope)?;
        if !rungs::ALL.contains(&policy.as_str()) {
            return Err(SessionChangeError::InvalidParams {
                field: "policy".into(),
                message: format!(
                    "policy must be one of {}; got {policy}",
                    rungs::ALL.join(", ")
                ),
            });
        }
        if send_id.is_empty() {
            return Err(SessionChangeError::InvalidParams {
                field: "send_id".into(),
                message: "send_id is required".into(),
            });
        }
        if crate::plan_change::check_policy(&read.facts, generation, &policy, &send_id)? {
            return Ok(session_change::RefreshPolicyReply::new(
                cortexkit_role_llm_runner::baseline::PendingChange { generation, policy },
            ));
        }
        match self
            .route_session_refresh_policy(session, generation, policy.clone(), send_id)
            .await
        {
            Ok(crate::actor::SessionRefreshPolicyOutcome::Changed) => {
                Ok(session_change::RefreshPolicyReply::new(
                    cortexkit_role_llm_runner::baseline::PendingChange { generation, policy },
                ))
            }
            Ok(crate::actor::SessionRefreshPolicyOutcome::Refused(error)) => Err(error),
            Ok(crate::actor::SessionRefreshPolicyOutcome::Reopen) => {
                Err(SessionChangeError::Transient(
                    "the session actor is closing; retry the policy change".into(),
                ))
            }
            Ok(crate::actor::SessionRefreshPolicyOutcome::Error(error)) => {
                Err(SessionChangeError::Transient(error))
            }
            Err(error) => Err(SessionChangeError::Transient(error.to_string())),
        }
    }

    async fn session_flush_prefix(
        &self,
        session: &LineageKey,
        principal: Option<&broca_protocol::BindPrincipal>,
        scope: &broca_protocol::SendScope,
        send_id: String,
    ) -> Result<cortexkit_role_llm_runner::session_change::FlushPrefixReply, SessionChangeError>
    {
        use cortexkit_role_llm_runner::session_change;
        let _change_guard = self.lock_session_change(session).await;
        let read = self
            .read_change(session, None, Some(&send_id), |error| {
                format!("cannot read the session WAL: {error}")
            })
            .await?;
        read.check_owner(principal, scope)?;
        if send_id.is_empty() {
            return Err(SessionChangeError::InvalidParams {
                field: "send_id".into(),
                message: "send_id is required".into(),
            });
        }
        if let Err(pending_generation) = crate::plan_change::flush(&read.facts) {
            return Ok(session_change::FlushPrefixReply::new(pending_generation));
        }
        match self.route_session_flush_prefix(session, send_id).await {
            Ok(crate::actor::SessionFlushPrefixOutcome::Flushed { pending_generation }) => {
                Ok(session_change::FlushPrefixReply::new(pending_generation))
            }
            Ok(crate::actor::SessionFlushPrefixOutcome::Reopen) => {
                Err(SessionChangeError::Transient(
                    "the session actor is closing; retry the flush".into(),
                ))
            }
            Ok(crate::actor::SessionFlushPrefixOutcome::Error(error)) => {
                Err(SessionChangeError::Transient(error))
            }
            Err(error) => Err(SessionChangeError::Transient(error.to_string())),
        }
    }

    fn session_written(&self, session: &LineageKey) -> Result<bool, ServeError> {
        Ok(self.store_holding(session)?.is_some())
    }

    async fn cancel(
        &self,
        session: &LineageKey,
        run_id: broca_protocol::RunId,
    ) -> Result<CancelResponse, ServeError> {
        self.cancel_at(session, run_id, broca_wal::record::RunCancelAt::Now)
            .await
    }

    async fn cancel_at(
        &self,
        session: &LineageKey,
        run_id: broca_protocol::RunId,
        at: broca_wal::record::RunCancelAt,
    ) -> Result<CancelResponse, ServeError> {
        use crate::actor::CancelOutcome;
        match self.route_cancel_at(session, run_id, at).await? {
            CancelOutcome::ScopeRefused(refusal) => Err(ServeError::ScopeRefused(refusal)),
            CancelOutcome::Error(error) => Err(ServeError::Open(error, None)),
            CancelOutcome::SessionMoving { cut_id } => Err(ServeError::SessionMoving { cut_id }),
            CancelOutcome::SessionMoved { holder } => Err(ServeError::SessionMoved { holder }),
            CancelOutcome::Ack => Ok(CancelResponse::Ack),
            CancelOutcome::NotActive => Ok(CancelResponse::NotActive),
            CancelOutcome::Reopen => unreachable!("registry retry consumes Reopen"),
        }
    }

    async fn cancel_from(
        &self,
        session: &LineageKey,
        run_id: broca_protocol::RunId,
        at: broca_wal::record::RunCancelAt,
        principal: Option<&broca_protocol::BindPrincipal>,
        scope: &broca_protocol::SendScope,
    ) -> Result<CancelResponse, ServeError> {
        use crate::actor::CancelOutcome;
        match self
            .resolve_cancel_authority(session, principal, scope, at)
            .await?
        {
            crate::actor::CancelAuthority::Owner => {
                return self.cancel_at(session, run_id, at).await
            }
            crate::actor::CancelAuthority::Sender => {}
        }
        // Refuse before opening an actor: recovery itself can write. The actor rechecks
        // the sender grant against any session.refresh accepted since this read.
        match self
            .route_cancel_from(session, run_id, at, principal.cloned(), scope.clone())
            .await?
        {
            CancelOutcome::ScopeRefused(refusal) => Err(ServeError::ScopeRefused(refusal)),
            CancelOutcome::Error(error) => Err(ServeError::Open(error, None)),
            CancelOutcome::SessionMoving { cut_id } => Err(ServeError::SessionMoving { cut_id }),
            CancelOutcome::SessionMoved { holder } => Err(ServeError::SessionMoved { holder }),
            CancelOutcome::Ack => Ok(CancelResponse::Ack),
            CancelOutcome::NotActive => Ok(CancelResponse::NotActive),
            CancelOutcome::Reopen => unreachable!("registry retry consumes Reopen"),
        }
    }

    async fn status(&self, session: &LineageKey) -> Result<StatusResponse, ServeError> {
        use crate::actor::LineageStatus;
        match self.route_status(session).await? {
            LineageStatus::Idle => Ok(StatusResponse::Idle),
            LineageStatus::Active { run_id, step } => Ok(StatusResponse::Active {
                run_id: run_id.0,
                step,
            }),
            LineageStatus::Closing => unreachable!("registry retry consumes Closing"),
        }
    }

    async fn run_status(
        &self,
        session: &LineageKey,
        run_id: broca_protocol::RunId,
    ) -> Result<RunStatusResponse, ServeError> {
        if let Some(store) = self.store_holding(session)? {
            return crate::store_status::StoreRead::new(&store, session, self)
                .run_status(&run_id)
                .await;
        }
        self.check_expired(session, Some(&run_id.0)).await?;
        Ok(RunStatusResponse::Unknown)
    }

    async fn run_admission(
        &self,
        session: &LineageKey,
        run_id: broca_protocol::RunId,
    ) -> Result<Option<broca_wire::RunAdmissionResponse>, ServeError> {
        if let Some(store) = self.store_holding(session)? {
            return crate::store_status::StoreRead::new(&store, session, self)
                .run_admission(&run_id)
                .await;
        }
        self.check_expired(session, Some(&run_id.0)).await?;
        Ok(None)
    }

    async fn run_status_reply(
        &self,
        session: &LineageKey,
        run_id: broca_protocol::RunId,
    ) -> Result<broca_wire::RunStatusReply, ServeError> {
        if let Some(store) = self.store_holding(session)? {
            return crate::store_status::StoreRead::new(&store, session, self)
                .run_status_reply(&run_id)
                .await;
        }
        self.check_expired(session, Some(&run_id.0)).await?;
        Ok(RunStatusResponse::Unknown.into())
    }
}

#[cfg(test)]
mod held_status_tests {
    use super::*;

    fn state(index: broca_core::RunIndexState) -> broca_core::ReconciledRunState {
        broca_core::ReconciledRunState {
            finish: None,
            account_limit_pause: None,
            state: index,
            held_calls: Vec::new(),
            budget_detail: None,
            context_ceiling: None,
            ceiling_deadline_ms: None,
            ride_wait_deadline_ms: None,
            structured_output: None,
            live_step: None,
            undelivered_steers: None,
            final_step_finish_reason: None,
            indeterminate_tool_calls: None,
            usage: None,
            run_usage: Default::default(),
            retries_used: 0,
            error: None,
            pause_reason: None,
            auth_selection: None,
            title: None,
        }
    }

    #[test]
    fn active_and_paused_status_omit_empty_held_calls() {
        for index in [
            broca_core::RunIndexState::Active,
            broca_core::RunIndexState::Paused,
        ] {
            let wire = serde_json::to_value(run_status_response_with_reader_policy(
                Some(state(index)),
                false,
            ))
            .unwrap();
            assert!(
                wire.get("held_calls").is_none(),
                "empty held_calls must be omitted, not null or []: {wire}"
            );
            let decoded: cortexkit_role_llm_runner::run::RunStatus =
                serde_json::from_value(wire).unwrap();
            assert!(!decoded.is_held_wait());
            assert!(decoded.held_calls().is_empty());
        }
    }

    #[test]
    fn provider_held_recovery_parks_report_active_status_with_open_calls() {
        let expected = serde_json::json!([{
            "call_key": "lineage:4", "tool": "ask", "provider": "tools",
            "reason": "running", "held_at_ms": 42
        }]);
        for index in [
            broca_core::RunIndexState::Active,
            broca_core::RunIndexState::Paused,
        ] {
            let mut reconciled = state(index.clone());
            reconciled.pause_reason = (index == broca_core::RunIndexState::Paused)
                .then_some(broca_protocol::PauseReason::ToolHeld);
            reconciled
                .held_calls
                .push(cortexkit_role_llm_runner::run::HeldCall::new(
                    "lineage:4",
                    "ask",
                    "tools",
                    "running",
                    42,
                ));
            let wire = serde_json::to_value(run_status_response_with_reader_policy(
                Some(reconciled),
                false,
            ))
            .unwrap();
            assert_eq!(
                wire["held_calls"], expected,
                "open held calls must be reported on live and paused runs"
            );
            assert!(wire.get("deadline_ms").is_none());
            assert_eq!(wire["state"], "active");
            assert!(wire.get("pause_reason").is_none());
            assert!(wire.get("reason").is_none());
            let decoded: cortexkit_role_llm_runner::run::RunStatus =
                serde_json::from_value(wire).unwrap();
            assert!(decoded.is_held_wait());
            assert_eq!(decoded.held_calls()[0].tool(), "ask");
            assert!(!cortexkit_role_tool_provider::result::is_person_wait(
                decoded.held_calls()[0].reason()
            ));
        }
    }
}

#[cfg(test)]
mod paused_auth_tests {
    use super::run_status_response_with_reader_policy;

    #[test]
    fn paused_auth_uses_frozen_identity_and_open_path_vocabulary() {
        for (frozen, wire) in [
            ("routed", "routed"),
            ("pinned", "pinned"),
            ("disabled", "default"),
            ("unreachable", "default"),
            ("unsupported", "default"),
            ("single_candidate", "default"),
            ("legacy_static", "default"),
            ("candidates_unknown", "default"),
            ("auth_required", "default"),
            ("future_choice", "future_choice"),
        ] {
            for reason in [
                broca_protocol::PauseReason::Restart,
                broca_protocol::PauseReason::AuthRequired,
            ] {
                let state = broca_core::ReconciledRunState {
                    finish: None,
                    account_limit_pause: None,
                    held_calls: Vec::new(),
                    run_usage: Default::default(),
                    budget_detail: None,
                    context_ceiling: None,
                    ceiling_deadline_ms: None,
                    structured_output: None,
                    live_step: None,
                    title: None,
                    state: broca_core::RunIndexState::Paused,
                    undelivered_steers: None,
                    final_step_finish_reason: None,
                    indeterminate_tool_calls: None,
                    usage: None,
                    retries_used: 0,
                    error: None,
                    pause_reason: Some(reason),
                    auth_selection: Some(broca_wal::AuthSelection {
                        billing_vault_facts: None,
                        auth_method: None,
                        credential_id: "requested-key".into(),
                        resolved_credential_id: Some("resolved-key".into()),
                        account_ref: Some("7b66addd-0000-4000-8000-000000000001".into()),
                        vault_request: None,
                        account_decision: Some(broca_wal::AccountDecision {
                            path: frozen.into(),
                            conversation_key: None,
                            conversation_key_source: None,
                            admission_id: None,
                            decision_id: None,
                        }),
                    }),
                    ride_wait_deadline_ms: None,
                };
                let value = serde_json::to_value(run_status_response_with_reader_policy(
                    Some(state.clone()),
                    false,
                ))
                .unwrap();
                assert_eq!(value["auth_selection"], "requested-key");
                assert_eq!(value["auth"]["path"], wire);
                assert_eq!(value["auth"]["credential_id"], "resolved-key");
                assert_eq!(
                    value["auth"]["account_ref"],
                    "7b66addd-0000-4000-8000-000000000001"
                );
                let mut legacy = state;
                let selection = legacy.auth_selection.as_mut().unwrap();
                selection.resolved_credential_id = None;
                selection.account_ref = None;
                selection.account_decision = None;
                let value = serde_json::to_value(run_status_response_with_reader_policy(
                    Some(legacy.clone()),
                    false,
                ))
                .unwrap();
                assert_eq!(
                    value["auth"],
                    serde_json::json!({"credential_id": "requested-key"})
                );
                legacy
                    .auth_selection
                    .as_mut()
                    .unwrap()
                    .credential_id
                    .clear();
                let value = serde_json::to_value(run_status_response_with_reader_policy(
                    Some(legacy.clone()),
                    false,
                ))
                .unwrap();
                assert!(value.get("auth_selection").is_none());
                legacy.auth_selection = None;
                let value = serde_json::to_value(run_status_response_with_reader_policy(
                    Some(legacy),
                    false,
                ))
                .unwrap();
                assert!(value.get("auth").is_none());
                assert!(value.get("auth_selection").is_none());
            }
        }
    }
}

#[cfg(test)]
mod context_ceiling_tests {
    use super::*;
    fn reconciled(state: broca_core::RunIndexState) -> broca_core::ReconciledRunState {
        broca_core::ReconciledRunState {
            finish: None,
            account_limit_pause: None,
            held_calls: Vec::new(),
            state,
            context_ceiling: None,
            ceiling_deadline_ms: None,
            ride_wait_deadline_ms: None,
            budget_detail: None,
            structured_output: None,
            live_step: None,
            undelivered_steers: None,
            final_step_finish_reason: None,
            indeterminate_tool_calls: None,
            usage: None,
            run_usage: Default::default(),
            retries_used: 0,
            error: None,
            pause_reason: None,
            auth_selection: None,
            title: None,
        }
    }
    #[test]
    fn a_ceiling_wait_status_is_active_with_reason_and_absolute_deadline() {
        let mut state = reconciled(broca_core::RunIndexState::Active);
        state.ceiling_deadline_ms = Some(123_456);
        let wire = serde_json::to_value(run_status_response_with_reader_policy(Some(state), false))
            .unwrap();
        assert_eq!(wire["state"], "active");
        assert_eq!(
            wire["reason"],
            cortexkit_role_llm_runner::run::RUN_REASON_CONTEXT_CEILING_WAIT
        );
        assert_eq!(wire["deadline_ms"], 123_456);
        assert!(wire.get("pause_reason").is_none());
    }
    #[test]
    fn a_ceiling_terminal_status_carries_detail_without_a_provider_error() {
        let mut state = reconciled(broca_core::RunIndexState::ContextCeiling);
        state.context_ceiling = Some(broca_protocol::ContextCeilingDetail::new(
            100,
            90,
            "view_timeout",
        ));
        let wire = serde_json::to_value(run_status_response_with_reader_policy(Some(state), false))
            .unwrap();
        assert_eq!(wire["state"], "context_ceiling");
        assert_eq!(
            wire["context_ceiling"],
            serde_json::json!({"window":100,"size":90,"cause":"view_timeout"})
        );
        assert!(wire.get("error").is_none());
        assert!(wire.get("deadline_ms").is_none());
    }
    /// A rewrite ride's reported wait is a runner-owned wait (llm-runner 0.5.2 §6): the
    /// active run carries its `deadline_ms`, with the display reason `compaction_wait`, and the
    /// role's `RunStatus` reads it as a wait until that deadline.
    #[test]
    fn a_ride_wait_reports_compaction_wait_by_its_deadline() {
        let mut state = reconciled(broca_core::RunIndexState::Active);
        state.ride_wait_deadline_ms = Some(234_567);
        let wire = serde_json::to_value(run_status_response_with_reader_policy(Some(state), false))
            .unwrap();
        assert_eq!(wire["state"], "active");
        assert_eq!(wire["reason"], "compaction_wait");
        assert_eq!(wire["deadline_ms"], 234_567);
        let status: cortexkit_role_llm_runner::run::RunStatus =
            serde_json::from_value(wire).unwrap();
        assert_eq!(status.runner_wait_until(), Some(234_567));
    }
}

fn media_preflight_error(error: broca_core::RunError) -> ServeError {
    match error {
        broca_core::RunError::Upload(error) => ServeError::Upload(error),
        broca_core::RunError::MediaTooLarge(detail) => ServeError::MediaTooLarge(detail),
        broca_core::RunError::MediaUnsupported(message) => ServeError::DeliveryRefused {
            code: "media_unsupported",
            message,
        },
        broca_core::RunError::Transient(message) => ServeError::Transient(message),
        error => ServeError::InvalidParams {
            field: "content".into(),
            message: error.to_string(),
        },
    }
}
