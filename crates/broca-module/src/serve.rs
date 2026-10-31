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
    /// Read strictly after the message with this `mid`, taken from an earlier page of the
    /// lineage `lineage_id`. The serve adapter refuses a request that sets it without
    /// `lineage_id`, or together with `from_ordinal`.
    pub after_mid: Option<String>,
    /// The lineage the caller's cursor belongs to; a read naming another is refused.
    pub lineage_id: Option<String>,
    /// The page's byte cap; `None` is the default cap.
    pub max_bytes: Option<u64>,
    /// `blocks` exposes stored media; `descriptors` serves payload refs and content identities.
    pub media: cortexkit_role_llm_runner::read::MediaMode,
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
            after_mid: None,
            lineage_id: None,
            max_bytes: None,
            media: cortexkit_role_llm_runner::read::MediaMode::Inline,
        }
    }
}

/// The ordinal a `mid` names: broca spells a message's id `m<ordinal>`. `None` for any other
/// spelling, which names no message.
fn ordinal_of_mid(mid: &str) -> Option<u64> {
    let digits = mid.strip_prefix('m')?;
    let canonical = !digits.is_empty()
        && digits.bytes().all(|byte| byte.is_ascii_digit())
        && (digits == "0" || !digits.starts_with('0'));
    canonical.then(|| digits.parse().ok()).flatten()
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
fn reply_frame_limit_refusal(
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

fn model_group_reply_too_large_subject(
    page: &broca_wire::ModelPage,
) -> Option<ReplyTooLargeSubject> {
    page.messages
        .first()
        .map(|entry| ReplyTooLargeSubject::ModelGroup {
            source: entry.source.clone(),
        })
}

fn cap_page_bytes(
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
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SubscribeFrom {
    /// From the very start of the lineage (seq 1).
    Start,
    /// Strictly after the durable head captured by the attach barrier.
    Live,
    /// Strictly after a durable cursor (a resume from the client's last-processed cursor).
    After { wal_seq: u64, sub_index: u32 },
}

/// A `session.import` request carrying canonical history only.
#[derive(Clone)]
pub struct SessionImportRequest {
    pub project_id: Option<String>,
    /// The principal the daemon stamped on the importing bind; recorded on the RunStarted.
    pub principal: Option<broca_protocol::BindPrincipal>,
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
    /// No episode yet: a plan sent now is fetched and frozen.
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

/// Classify policy retries and refusals without side effects. The serve preflight avoids
/// opening an actor for a refusal; the actor repeats the check at the serialized write point.
pub(crate) fn check_session_policy(
    change: &broca_fetch_plan::session_change::SessionChangeState,
    generation: u64,
    policy: &str,
    send_id: &str,
) -> Result<bool, SessionChangeError> {
    if let Some((earlier_generation, earlier_policy)) = change.policy_send_ids.get(send_id) {
        let field = if generation != *earlier_generation {
            Some("generation")
        } else if policy != earlier_policy {
            Some("policy")
        } else {
            None
        };
        return match field {
            None => Ok(true),
            Some(field) => Err(SessionChangeError::SendIdReuse {
                field: field.into(),
                message: format!("send_id {send_id} was used with another {field}"),
            }),
        };
    }
    if let Some(applied_by) = change.applied.get(&generation) {
        return Err(SessionChangeError::AlreadyApplied {
            generation,
            applied_by: applied_by.clone(),
        });
    }
    let Some(pending) = change
        .pending
        .as_ref()
        .filter(|pending| pending.generation == generation)
    else {
        return Err(SessionChangeError::PendingSuperseded {
            generation,
            pending: change.accepted_generation,
        });
    };
    if let Some(surface) = broca_fetch_plan::session_change::unsupported_surface(
        change.current_manifest.as_deref(),
        &pending.manifest,
        policy,
    ) {
        return Err(SessionChangeError::RungUnsupported {
            surface: surface.into(),
            rung: policy.into(),
        });
    }
    Ok(false)
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

/// The answer for `params` from a read page, when one of its messages holds the tag.
fn resolve_in_page(
    page: &SessionReadResponse,
    params: &broca_wire::ResolveParams,
) -> Option<Result<broca_wire::ResolveResponse, (&'static str, String)>> {
    page.messages.iter().find_map(|entry| {
        let number = params.number().ok()?;
        let row = entry
            .tags
            .iter()
            .find(|row| row.tag.as_deref() == Some(params.tag.as_str()))?;
        let (index, _) = entry
            .message
            .part_tags()?
            .tagged()
            .find(|(_, tag)| *tag == number)?;
        // The part as every exposure shows it: the model's text cleaned where the run that
        // wrote it cleans, any other part in its complete stored encoding.
        let stored = broca_protocol::tags::exposed_part_text(&entry.message, index)?;
        Some(match broca_wire::resolve_range(&stored, params.range) {
            Some(text) => {
                let descriptors =
                    params.media == Some(cortexkit_role_llm_runner::read::MediaMode::Descriptors);
                let content = descriptors.then(|| {
                    broca_wire::describe_resolved_part(
                        &entry.message,
                        entry.ordinal,
                        index,
                        text.clone(),
                    )
                });
                let mut reply = broca_wire::ResolveResponse::new(
                    &params.tag,
                    entry.ordinal,
                    &entry.mid,
                    &row.part,
                    text,
                );
                if let Some(content) = content {
                    reply.content = content;
                }
                reply.range = params.range;
                Ok(reply)
            }
            None => Err((
                broca_wire::INVALID_PARAMS,
                format!(
                    "range must lie within the part's {} characters",
                    stored.chars().count()
                ),
            )),
        })
    })
}

fn session_read_response(
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
        next_late_after: None,
        messages: page
            .messages
            .into_iter()
            .map(|message| SessionReadMessage {
                rich_ui: None,
                written_at: message.written_at,
                delivered_as: message
                    .run
                    .as_ref()
                    .and_then(|run| run.delivery.as_ref())
                    .map(|delivery| {
                        cortexkit_role_llm_runner::delivery_order::EntryDelivery::new(delivery.mode)
                    }),
                ordinal: message.ordinal,
                mid: format!("m{}", message.ordinal),
                tags: broca_wire::tag_rows(message.ordinal, &message.message),
                message: message.message,
                run: message.run,
                tool_calls: Vec::new(),
                described: None,
                digest: None,
                part_digests: None,
            })
            .collect(),
        next_from_ordinal: page.next_from_ordinal,
        head: page.head.map(|cursor| CursorWire {
            wal_seq: cursor.wal_seq,
            sub_index: cursor.sub_index,
        }),
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
        deferred_calls: page
            .deferred_calls
            .as_ref()
            .map(crate::approval::call_views),
        late_approvals: page
            .deferred_calls
            .as_ref()
            .map(|_| crate::approval::late_views(&page.late_approvals)),
        open_batch: None,
    }
}

/// The durable terminal of run `run_id`'s episode, if the WAL holds one: its reason and error.
fn durable_terminal(
    records: &[broca_wal::FramedRecord],
    run_id: &broca_protocol::RunId,
) -> Option<(
    broca_protocol::caller::RunFinishReason,
    Option<broca_protocol::ProviderError>,
)> {
    let start = records.iter().position(|framed| {
        matches!(&framed.record, broca_wal::WalRecord::RunStarted { run_id: id, .. } if id == run_id)
    })?;
    records[start + 1..]
        .iter()
        .take_while(|framed| !matches!(framed.record, broca_wal::WalRecord::RunStarted { .. }))
        .find_map(|framed| match &framed.record {
            broca_wal::WalRecord::RunFinished { reason, error, .. } => {
                Some((*reason, error.clone()))
            }
            _ => None,
        })
}

/// The session's first `RunStarted`: its start record, which froze what the session runs with.
fn first_run_started(records: &[broca_wal::FramedRecord]) -> Option<&broca_wal::WalRecord> {
    records
        .iter()
        .map(|framed| &framed.record)
        .find(|record| matches!(record, broca_wal::WalRecord::RunStarted { .. }))
}

/// The baseline the session's first `RunStarted` (`start`) froze: built from its plan manifest
/// when the session was admitted with a fetch plan, else only the tool names in its config.
fn baseline_of(start: &broca_wal::WalRecord) -> broca_wire::BaselineWire {
    match start {
        broca_wal::WalRecord::RunStarted {
            manifest: Some(manifest),
            ..
        } => planned_baseline(manifest),
        broca_wal::WalRecord::RunStarted { config, .. } => broca_wire::BaselineWire::plan_less(
            config.tools.iter().map(|tool| tool.name.clone()).collect(),
        ),
        _ => broca_wire::BaselineWire::plan_less(Vec::new()),
    }
}

/// The baseline of a session admitted with a fetch plan, read from its frozen manifest alone.
fn planned_baseline(manifest: &broca_wal::PlanManifest) -> broca_wire::BaselineWire {
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

/// Why the caller is not the owner the start record `start` names, if it is not: the frozen
/// scope's owner when the session is scoped, else the principal of the session's first send. A
/// session whose start record names neither has no recorded owner to hold a caller to.
fn baseline_owner_mismatch(
    start: &broca_wal::WalRecord,
    principal: Option<&broca_protocol::BindPrincipal>,
    scope: &broca_protocol::SendScope,
) -> Option<broca_core::OwnerMismatch> {
    let broca_wal::WalRecord::RunStarted {
        scope: frozen,
        principal: first,
        ..
    } = start
    else {
        return None;
    };
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

/// The final assistant message of the completed run `run_id`: the last assistant message of the
/// transcript as it stood when that run's episode ended. Its text parts are joined in order with
/// nothing inserted, and its reasoning parts are left out.
fn final_message_of(
    records: &[broca_wal::FramedRecord],
    run_id: &broca_protocol::RunId,
) -> Result<broca_wire::FinalMessage, ServeError> {
    // Missing records and replay failures do not expose a retryable I/O cause.
    let started = |framed: &broca_wal::FramedRecord| matches!(&framed.record, broca_wal::WalRecord::RunStarted { run_id: id, .. } if id == run_id);
    let Some(start) = records.iter().position(started) else {
        return Err(ServeError::Open(
            format!("run '{}' has no readable RunStarted", run_id.0),
            None,
        ));
    };
    let end = records[start + 1..]
        .iter()
        .position(|framed| matches!(framed.record, broca_wal::WalRecord::RunStarted { .. }))
        .map_or(records.len(), |offset| start + 1 + offset);
    let transcript = broca_core::replay_transcript(&records[..end])
        .map_err(|error| ServeError::Open(format!("replay failed: {error}"), None))?;
    final_message_from_prompt(&transcript, run_id)
}

fn final_message_from_prompt(
    transcript: &[broca_protocol::Message],
    run_id: &broca_protocol::RunId,
) -> Result<broca_wire::FinalMessage, ServeError> {
    let (ordinal, message) = transcript
        .iter()
        .enumerate()
        .rev()
        .find(|(_, message)| message.role == broca_protocol::content::Role::Assistant)
        .ok_or_else(|| {
            ServeError::Open(
                format!("completed run '{}' has no assistant message", run_id.0),
                None,
            )
        })?;
    let ordinal = message.ordinal.unwrap_or(ordinal as u64);
    // The text as every exposure shows it: cleaned when a tag-rendering run under the computed
    // rules wrote it, raw for any other run and for a structured-output run's final text.
    let message = broca_protocol::tags::exposed_message(message);
    let text: String = message
        .content
        .iter()
        .filter_map(|block| match block {
            broca_protocol::content::ContentBlock::Text { text }
            | broca_protocol::content::ContentBlock::TextWithState { text, .. } => {
                Some(text.as_str())
            }
            _ => None,
        })
        .collect();
    Ok(broca_wire::FinalMessage {
        ordinal,
        mid: format!("m{ordinal}"),
        text,
    })
}

pub(crate) fn recorded_interrupt_reason(
    records: &[broca_wal::FramedRecord],
    run_id: &broca_protocol::RunId,
) -> Option<String> {
    let mut current = None;
    let mut reason = None;
    for frame in records {
        match &frame.record {
            broca_wal::WalRecord::RunStarted { run_id, .. } => current = Some(run_id),
            broca_wal::WalRecord::RunFinished {
                reason: broca_protocol::RunFinishReason::Interrupted,
                interrupt_reason,
                ..
            } if current == Some(run_id) => reason = interrupt_reason.clone(),
            _ => {}
        }
    }
    reason
}

fn wake_directive_requested(
    records: &[broca_wal::FramedRecord],
    run_id: &broca_protocol::RunId,
) -> bool {
    records
        .iter()
        .rev()
        .find_map(|frame| match &frame.record {
            broca_wal::WalRecord::RunStarted {
                run_id: id, config, ..
            } if id == run_id => Some(config.turn_directives_v1.as_ref().is_some_and(|names| {
                names.contains(&cortexkit_role_llm_runner::directives::DirectiveName::Wake)
            })),
            _ => None,
        })
        .unwrap_or(false)
}

fn completed_directives(
    records: &[broca_wal::FramedRecord],
    run_id: &broca_protocol::RunId,
    text: &str,
) -> Option<Vec<cortexkit_role_llm_runner::directives::Directive>> {
    // Parse the stored final message, never a provider request decorated with runner tags.
    if !wake_directive_requested(records, run_id) {
        return None;
    }
    cortexkit_role_llm_runner::directives::parse_trailing(text)
        .map(|directive| vec![directive.into()])
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
struct RunToolCallsAnchor {
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
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RunToolCallsCursor {
    version: u8,
    run_id: String,
    lineage_id: Option<String>,
    run_start_seq: u64,
    run_end_seq: u64,
    step_seq: u64,
    call_index: usize,
    batch_end_seq: u64,
    ordinal: u64,
    anchor_before: Option<RunToolCallsAnchor>,
    after_batch: Vec<RunToolCallsPosition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    authentication: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct RunToolCallsPosition {
    step_seq: u64,
    call_index: usize,
    batch_end_seq: u64,
    ordinal: u64,
    anchor_before: Option<RunToolCallsAnchor>,
}

#[derive(Clone, Debug)]
struct IndexedRunToolCall {
    call: cortexkit_role_llm_runner::tool_calls::RunToolCall,
    step_seq: u64,
    call_index: usize,
    batch_end_seq: u64,
    ordinal: u64,
    after_batch: Vec<RunToolCallsPosition>,
}

#[derive(Clone, Copy)]
struct RunToolCallsPageContext<'a> {
    run_id: &'a str,
    lineage_id: Option<&'a str>,
    run_start_seq: u64,
    run_end_seq: u64,
    range: &'a broca_wal::WalRangeRead,
    range_start_seq: u64,
    fallback: Option<RunToolCallsAnchor>,
    seed: Option<&'a RunToolCallsCursor>,
}

struct RunToolCallBuilder {
    call: cortexkit_role_llm_runner::tool_calls::RunToolCall,
    has_intent: bool,
    terminal: bool,
}

fn committed_tool_step_ordinals(
    records: &[broca_wal::FramedRecord],
) -> std::collections::HashMap<u64, u64> {
    let wal_records: Vec<_> = records.iter().map(|framed| framed.record.clone()).collect();
    let Ok(replayed) = broca_core::replay::replay(&wal_records) else {
        return std::collections::HashMap::new();
    };
    replayed
        .committed_tool_turns
        .into_iter()
        .filter_map(|(ordinal, record_index)| {
            records
                .get(record_index)
                .map(|framed| (framed.seq, ordinal as u64))
        })
        .collect()
}

fn run_tool_call_records(
    range: &broca_wal::WalRangeRead,
) -> Result<&[broca_wal::FramedRecord], ServeError> {
    match range.records() {
        broca_wal::WalRangeRecords::Complete(records) => Ok(records),
        broca_wal::WalRangeRecords::Absent => Ok(&[]),
        broca_wal::WalRangeRecords::Partial(_) | broca_wal::WalRangeRecords::Unknown(_) => {
            Err(ServeError::Open(
                "run.tool_calls could not establish a complete WAL range".into(),
                Some(broca_protocol::ErrorClass::Transient),
            ))
        }
    }
}

fn run_tool_call_bounds(records: &[broca_wal::FramedRecord], run_id: &str) -> Option<(u64, u64)> {
    let start = records.iter().position(|framed| {
        matches!(&framed.record, broca_wal::WalRecord::RunStarted { run_id: started, .. } if started.0 == run_id)
    })?;
    let start_seq = records[start].seq;
    let mut end_seq = records.last().map_or(start_seq, |framed| framed.seq);
    for framed in &records[start + 1..] {
        match &framed.record {
            broca_wal::WalRecord::RunFinished { .. } => {
                end_seq = framed.seq;
                break;
            }
            broca_wal::WalRecord::RunStarted { .. } => {
                end_seq = framed.seq.saturating_sub(1);
                break;
            }
            _ => {}
        }
    }
    Some((start_seq, end_seq))
}

fn run_tool_call_outcome(
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

/// Walk WAL records in transcript order so call ordinals match the assistant message that asked
/// for them. Tool results are used only to count their stored bytes; their content is never copied
/// into the run-call response.
fn indexed_run_tool_calls(
    records: &[broca_wal::FramedRecord],
    target_run_id: &str,
    lineage_id: Option<&str>,
    seed: Option<&RunToolCallsCursor>,
    step_ordinals: &std::collections::HashMap<u64, u64>,
) -> Result<Vec<IndexedRunToolCall>, ServeError> {
    use broca_wal::WalRecord;
    use cortexkit_role_llm_runner::tool_calls::outcomes;

    let mut entries = Vec::new();
    let mut current_run = seed.map(|_| target_run_id.to_owned());
    let mut prompt_len = seed.map_or(0, |cursor| cursor.ordinal);
    let mut pending_steers = 0u64;
    let mut active_calls: Vec<RunToolCallBuilder> = Vec::new();
    let mut active_step_seq = 0u64;
    let mut active_step_ordinal = 0u64;
    let mut active_batch_id: Option<String> = None;

    let finish_calls = |calls: &mut Vec<RunToolCallBuilder>,
                        step_seq: u64,
                        batch_end_seq: u64,
                        ordinal: u64,
                        entries: &mut Vec<IndexedRunToolCall>| {
        for (call_index, builder) in calls.iter_mut().enumerate() {
            if !builder.terminal && builder.has_intent {
                builder.call.outcome = outcomes::INDETERMINATE.into();
            }
            if !builder.call.outcome.is_empty() {
                entries.push(IndexedRunToolCall {
                    call: builder.call.clone(),
                    step_seq,
                    call_index,
                    batch_end_seq,
                    ordinal,
                    after_batch: Vec::new(),
                });
            }
        }
    };

    for framed in records {
        match &framed.record {
            WalRecord::RunStarted { run_id, input, .. } => {
                current_run = Some(run_id.0.clone());
                prompt_len = prompt_len.saturating_add(input.len() as u64);
                pending_steers = 0;
                active_calls.clear();
                active_batch_id = None;
            }
            WalRecord::StepStarted { .. } => {
                prompt_len = prompt_len.saturating_add(pending_steers);
                pending_steers = 0;
            }
            WalRecord::ModelStepFinished {
                assistant_message, ..
            } => {
                active_calls.clear();
                active_batch_id = None;
                active_step_seq = framed.seq;
                active_step_ordinal = step_ordinals
                    .get(&framed.seq)
                    .copied()
                    .unwrap_or(prompt_len);
                let calls: Vec<_> = assistant_message.tool_calls().collect();
                if current_run.as_deref() == Some(target_run_id) {
                    active_calls.extend(calls.iter().enumerate().map(|(position, call)| {
                        RunToolCallBuilder {
                            call: cortexkit_role_llm_runner::tool_calls::RunToolCall::new(
                                call.tool_call_id.0.clone(),
                                call.tool_name.clone(),
                                call.input.clone(),
                                "",
                                active_step_ordinal,
                            )
                            .with_position(position as u32),
                            has_intent: false,
                            terminal: false,
                        }
                    }));
                }
                if calls.is_empty() {
                    prompt_len = prompt_len.saturating_add(1);
                }
            }
            WalRecord::ToolBatchStarted { batch_id, .. } => {
                active_batch_id = Some(batch_id.0.clone());
            }
            WalRecord::ToolCallDeferred {
                batch_id, call_id, ..
            } if active_batch_id.as_deref() == Some(batch_id.0.as_str()) => {
                if let (Some(lineage_id), Some(call)) = (
                    lineage_id,
                    active_calls.iter_mut().find(|call| {
                        call.call.tool_call_id == call_id.0
                            && call.call.call_key.is_none()
                            && !call.has_intent
                            && !call.terminal
                    }),
                ) {
                    call.call.call_key =
                        broca_core::tool_call_key(Some(lineage_id), framed.seq, None);
                }
            }
            WalRecord::ToolDispatchIntent {
                batch_id,
                tool_call_id,
                args,
                module_id,
                call_key,
                ..
            } if active_batch_id.as_deref() == Some(batch_id.0.as_str()) => {
                if let Some(call) = active_calls.iter_mut().find(|call| {
                    call.call.tool_call_id == tool_call_id.0 && !call.has_intent && !call.terminal
                }) {
                    call.has_intent = true;
                    call.call.args = (**args).clone();
                    call.call.dispatched_to = module_id.clone();
                    call.call.call_key =
                        broca_core::tool_call_key(lineage_id, framed.seq, call_key.as_deref());
                }
            }
            WalRecord::ToolNoDispatch {
                batch_id,
                tool_call_id,
                ..
            } if active_batch_id.as_deref() == Some(batch_id.0.as_str()) => {
                if let Some(call) = active_calls
                    .iter_mut()
                    .find(|call| call.call.tool_call_id == tool_call_id.0 && !call.terminal)
                {
                    call.call.outcome = outcomes::REFUSED.into();
                    call.terminal = true;
                }
            }
            WalRecord::ToolResult {
                batch_id,
                result,
                outcome,
                reason,
                ..
            } if active_batch_id.as_deref() == Some(batch_id.0.as_str()) => {
                if let Some(call) = active_calls
                    .iter_mut()
                    .find(|call| call.call.tool_call_id == result.tool_call_id.0 && !call.terminal)
                {
                    call.call.outcome = run_tool_call_outcome(
                        *outcome,
                        call.has_intent,
                        reason.as_deref(),
                        result.is_error,
                    )
                    .into();
                    call.call.result_bytes = Some(
                        serde_json::to_vec(result.as_ref())
                            .map_err(|error| {
                                ServeError::Open(
                                    format!("could not encode stored tool result size: {error}"),
                                    None,
                                )
                            })?
                            .len() as u64,
                    );
                    call.terminal = true;
                }
            }
            WalRecord::ToolBatchFinished { batch_id }
                if active_batch_id.as_deref() == Some(batch_id.0.as_str()) =>
            {
                if current_run.as_deref() == Some(target_run_id) {
                    for builder in &mut active_calls {
                        builder.call.result_ordinal = Some(active_step_ordinal + 1);
                    }
                    finish_calls(
                        &mut active_calls,
                        active_step_seq,
                        framed.seq,
                        active_step_ordinal,
                        &mut entries,
                    );
                }
                // Each batch adds two prompt messages—one assistant and one tool-role message,
                // regardless of how many calls the batch contains.
                prompt_len = prompt_len.saturating_add(2);
                active_calls.clear();
                active_batch_id = None;
            }
            WalRecord::Steered { input, .. } => {
                pending_steers = pending_steers.saturating_add(input.len() as u64);
            }
            WalRecord::SendDelivered {
                input: Some(input), ..
            } => {
                pending_steers = pending_steers.saturating_add(input.len() as u64);
            }
            WalRecord::BudgetFired { run_id, .. }
                if current_run.as_deref() == Some(run_id.0.as_str()) =>
            {
                prompt_len = prompt_len.saturating_add(1);
            }
            WalRecord::RunFinished { .. } => {
                if current_run.as_deref() == Some(target_run_id) {
                    break;
                } else {
                    prompt_len = prompt_len.saturating_add(pending_steers);
                    pending_steers = 0;
                    current_run = None;
                }
            }
            _ => {}
        }
    }

    if current_run.as_deref() == Some(target_run_id) && !active_calls.is_empty() {
        finish_calls(
            &mut active_calls,
            active_step_seq,
            records.last().map_or(active_step_seq, |record| record.seq),
            active_step_ordinal,
            &mut entries,
        );
    }
    Ok(entries)
}

fn invalid_tool_call_cursor(message: &str) -> ServeError {
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
    range: &broca_wal::WalRangeRead,
    range_start_seq: u64,
    fallback: Option<RunToolCallsAnchor>,
) -> RunToolCallsPosition {
    let anchor_before = if entry.step_seq == range_start_seq {
        fallback
    } else {
        entry
            .step_seq
            .checked_sub(1)
            .and_then(|seq| range.anchor_at(seq))
            .map(Into::into)
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

fn frozen_auth_selection(state: &broca_core::ReconciledRunState) -> Option<String> {
    // Reuse the admission selection already decoded for lifecycle resolution. Current
    // routing and the vault's resolved alias are not the credential key the run froze.
    state
        .auth_selection
        .as_ref()
        .map(|selection| selection.credential_id.clone())
        .filter(|credential_id| !credential_id.is_empty())
}

fn run_status_response(state: Option<broca_core::ReconciledRunState>) -> RunStatusResponse {
    use broca_core::RunIndexState as State;

    let Some(state) = state else {
        return RunStatusResponse::Unknown;
    };
    let auth_selection = frozen_auth_selection(&state);
    let final_step_finish_reason = state.final_step_finish_reason;
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
    {
        State::Active
    } else {
        state.state
    };
    match public_state {
        // Deadline waits and provider-held calls are independent runner-owned waits.
        // Consumers recognize them from their fields, never from the informational reason.
        State::Active => RunStatusResponse::Active {
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
            auth_selection,
            pause_reason: state
                .pause_reason
                .filter(|reason| {
                    matches!(
                        reason,
                        broca_protocol::PauseReason::Restart
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
            undelivered_steers,
            usage,
            retries_used,
            title,
            structured_output: structured_output.clone(),
        },
        State::MaxSteps => RunStatusResponse::MaxSteps {
            auth_selection,
            final_step_finish_reason,
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
            undelivered_steers,
            usage,
            retries_used,
            title,
            structured_output: structured_output.clone(),
        },
        State::Cancelled => RunStatusResponse::Cancelled {
            auth_selection,
            final_step_finish_reason,
            undelivered_steers,
            usage,
            retries_used,
            title,
            structured_output: structured_output.clone(),
        },
        State::Error => RunStatusResponse::Error {
            auth_selection,
            final_step_finish_reason,
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
            undelivered_steers,
            usage,
            retries_used,
            title,
            structured_output: structured_output.clone(),
        },
    }
}

/// An error from the serve surface (distinct from a successful per-request outcome). Opening
/// failures describe lease or store problems; `SendIdReuse` is a typed request rejection.
#[derive(Debug, Clone)]
pub enum ServeError {
    Upload(broca_core::media_uploads::UploadError),
    ContextCeiling(broca_protocol::ContextCeilingDetail),
    UnknownSend,
    SessionMoving {
        cut_id: String,
    },
    SessionMoved {
        holder: String,
    },
    Expired {
        expired_at_ms: u64,
    },
    RetentionInvalid(crate::retention::InvalidRetention),
    VariantUnsupported(broca_core::run::VariantRefusal),
    /// The session is held by another live writer (another process/host).
    SessionHeld(String),
    /// Opening or admission failed. The optional class is set only from a known cause.
    Open(String, Option<broca_protocol::ErrorClass>),
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
    /// A template send whose `part_tag` slot names a part the transcript does not show, refused
    /// `template_part_gone` with the slot named in its detail.
    TemplatePartGone {
        slot: String,
    },
    /// A deterministic send identity was reused with a different frozen request field.
    SendIdReuse {
        differing: String,
    },
    /// Admission failed before durable start because a dependency is temporarily unavailable.
    Transient(String),
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
    /// The owner check refused the send; nothing was written.
    ScopeRefused(broca_core::ScopeRefusal),
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
    /// A read named a lineage that is not the session's current one.
    LineageChanged {
        requested: String,
    },
    /// `after_mid` names no message of the lineage.
    UnknownMid(String),
    /// `run.result` names no run of the session.
    UnknownRun(String),
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
            OpenError::Open(s) => ServeError::Open(s, None),
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
        }
    }
}

impl std::fmt::Display for ServeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Upload(error) => error.fmt(f),
            ServeError::UnknownSend => write!(f, "send_id is not recorded for this caller"),
            ServeError::SessionMoving { cut_id } => write!(f, "session is moving under cut {cut_id}"),
            ServeError::SessionMoved { holder } => write!(f, "session moved to {holder}"),
            ServeError::Expired { expired_at_ms } => write!(f, "lineage expired at {expired_at_ms}"),
            ServeError::RetentionInvalid(_) => write!(f, "invalid retention"),
            ServeError::SessionHeld(s) => write!(f, "session '{s}' held by a live writer"),
            ServeError::VariantUnsupported(e) => write!(f, "open failed: run resolution failed: {e}"),
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
            ServeError::TemplatePartGone { slot } => write!(
                f,
                "the template's {slot} slot names a part the transcript does not show"
            ),
            ServeError::SendIdReuse { differing } => write!(
                f,
                "send_id was already used with different {differing}; retries must be byte-identical"
            ),
            ServeError::Transient(s) => write!(f, "transient admission failure: {s}"),
            ServeError::TagsAwaitSetup => f.write_str(
                "the running run's tag numbers wait for its compaction Setup answer; retry shortly",
            ),
            ServeError::ContextCeiling(detail) => write!(f, "context_ceiling: estimated request size {} in window {} ({})", detail.size, detail.window, detail.cause),
            ServeError::Import(s) => write!(f, "import rejected: {s}"),
            ServeError::ImportIdReuse => write!(f, "import_id was reused with different content"),
            ServeError::ImportBusy => write!(f, "session has an active run"),
            ServeError::ScopeRefused(refusal) => write!(f, "scope refused: {refusal:?}"),
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
            ServeError::LineageChanged { requested } => write!(
                f,
                "the session's lineage is not '{requested}'; re-read it from the tail"
            ),
            ServeError::UnknownMid(mid) => write!(f, "the lineage has no message '{mid}'"),
            ServeError::UnknownRun(run_id) => write!(f, "the session has no run '{run_id}'"),
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
    /// Check the move latch before authorization reads frozen session metadata.
    /// File-backed handlers enforce the durable fence even for already-bound routes.
    async fn check_migration(&self, _session: &LineageKey, _write: bool) -> Result<(), ServeError> {
        Ok(())
    }
    /// Whether the executor has installed the writer's compaction plane.
    fn compaction_supported(&self) -> bool {
        false
    }

    /// Model-view reads are separate from raw reads so existing transcript replies keep
    /// their shape and do not decode the checkpoint's compaction payload.
    async fn read_model(
        &self,
        _req: SessionReadRequest,
    ) -> Result<broca_wire::ModelPage, ServeError> {
        Err(ServeError::InvalidParams {
            field: "view".into(),
            message: "model view is unsupported".into(),
        })
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
    /// adoption is decided here; every other scope rule is decided on each send, since an
    /// episode can be written between the bind and the send. `Err` when the history cannot be
    /// read, so whether it refuses the stamp is unknown: the bind is refused rather than
    /// accepted. The default implementation has no history source and returns no refusal.
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
    async fn frozen_manifest(
        &self,
        _session: &LineageKey,
    ) -> Result<Option<std::sync::Arc<broca_wal::PlanManifest>>, ServeError> {
        Ok(None)
    }

    /// Every durable record of the session's current lineage, oldest first, read under the
    /// expiry check of run `run_id`. `None` from a handler that keeps no log reader. On-demand
    /// history uses it for send metadata; output references use it to find a past compaction view.
    async fn session_log(
        &self,
        _session: &LineageKey,
        _run_id: &str,
    ) -> Result<Option<Vec<broca_wal::WalRecord>>, ServeError> {
        Ok(None)
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

    /// Where the session's materialized transcript holds the part tagged `tag`, from the
    /// store's tag index (see [`broca_core::TagLocation`]). `None` when this handler keeps no
    /// tag index, which leaves [`ServeHandler::resolve`] to read the transcript from its start.
    async fn tag_location(
        &self,
        _session: &LineageKey,
        _tag: u64,
    ) -> Result<Option<broca_core::TagLocation>, ServeError> {
        Ok(None)
    }

    /// `session.resolve` — the stored text of the part tagged `params.tag`, or a character
    /// range of it. A refusal is answered as its typed code and message.
    ///
    /// The tag index names the part's message, which is then read as a one-message
    /// `session.read` page: the store's row and the WAL past the store's watermark, never the
    /// whole transcript. A tag the index does not hold can only be in the transcript past the
    /// materialized prefix, which is read from there, through the same page path (the WAL
    /// after the watermark). Every read goes through `session.read`'s own path, so resolve
    /// sees exactly what a read sees, under the same authorization; the pages after the first
    /// are pinned to its lineage, so a lineage recreated mid-search is refused rather than
    /// searched half old, half new.
    async fn resolve(
        &self,
        session: &LineageKey,
        params: &broca_wire::ResolveParams,
    ) -> Result<Result<broca_wire::ResolveResponse, (&'static str, String)>, ServeError> {
        let number = match params.number() {
            Ok(number) => number,
            Err(_) => {
                return Ok(Err((
                    broca_wire::INVALID_PARAMS,
                    "tag must have the form §N§ for a positive integer N".into(),
                )))
            }
        };
        let mut location = self.tag_location(session, number).await?;
        let mut lineage_id = None;
        if let Some((ordinal, _)) = location.and_then(|location| location.found) {
            let page = self
                .read(SessionReadRequest::new(
                    session.clone(),
                    Some(ordinal),
                    Some(1),
                ))
                .await?;
            lineage_id = page.lineage_id.clone();
            if let Some(answer) = resolve_in_page(&page, params) {
                return Ok(answer);
            }
            // The indexed location may predate a lineage change repaired by this read.
            // Its old prefix bound cannot exclude parts carried into the new lineage.
            location = None;
        }
        // Not in the index: past the materialized prefix, or a store without an index.
        let mut from = location.map_or(0, |location| location.indexed_through);
        loop {
            let mut request = SessionReadRequest::new(
                session.clone(),
                Some(from),
                Some(broca_core::SESSION_READ_MAX_LIMIT),
            );
            request.lineage_id = lineage_id.clone();
            let page = self.read(request).await?;
            if lineage_id.is_none() {
                lineage_id = page.lineage_id.clone();
            }
            if let Some(answer) = resolve_in_page(&page, params) {
                return Ok(answer);
            }
            match page.next_from_ordinal {
                Some(next) if next > from => from = next,
                _ => {
                    return Ok(Err((
                        broca_wire::UNKNOWN_TAG,
                        format!(
                            "no part of the session's transcript holds tag {}",
                            params.tag
                        ),
                    )))
                }
            }
        }
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
    /// Fill each message's `tool_calls` with what the WAL recorded about its calls' dispatch.
    /// The facts of a turn in this read's WAL tail come with the page; an older turn's come from
    /// the session's cache (see [`crate::LineageRegistry::tool_turn_dispatches`]).
    async fn attribute_tool_calls(
        &self,
        session: &LineageKey,
        response: &mut SessionReadResponse,
        tail: &std::collections::BTreeMap<u64, Vec<broca_core::ToolCallDispatch>>,
        prompt_len: u64,
    ) -> Result<(), ServeError> {
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
        let ordinals: Vec<u64> = response
            .messages
            .iter()
            .filter_map(|message| {
                if message
                    .run
                    .as_ref()
                    .and_then(|run| run.delivery.as_ref())
                    .is_some_and(|delivery| {
                        delivery.mode == broca_protocol::DeliveryMode::ToolResult
                    })
                {
                    message.ordinal.checked_sub(1)
                } else if !calls_of(&message.message).is_empty() {
                    Some(message.ordinal)
                } else {
                    None
                }
            })
            .collect();
        if ordinals.is_empty() {
            return Ok(());
        }
        let Some(lineage_id) = response.lineage_id.clone() else {
            // No lineage, so no call has a key; every call is reported with no dispatch fact.
            for message in &mut response.messages {
                message.tool_calls = calls_of(&message.message)
                    .into_iter()
                    .map(|tool_call_id| broca_wire::SessionReadToolCall {
                        tool_call_id,
                        call_key: None,
                        dispatched_to: None,
                        indeterminate: false,
                    })
                    .collect();
            }
            return Ok(());
        };
        let dispatches = self
            .tool_turn_dispatches(session, &lineage_id, tail, &ordinals, prompt_len)
            .await?;
        for message in &mut response.messages {
            if let Some(carrier) = message
                .run
                .as_ref()
                .and_then(|run| run.delivery.as_ref())
                .and_then(|delivery| delivery.tool_call_id.as_ref())
            {
                if let Some(dispatch) = message
                    .ordinal
                    .checked_sub(1)
                    .and_then(|ordinal| dispatches.get(&ordinal))
                    .and_then(|calls| calls.iter().find(|call| &call.tool_call_id == carrier))
                {
                    if let Some(receipt) = &mut message.delivered_as {
                        receipt.call_key = dispatch.intent_seq.and_then(|seq| {
                            broca_core::tool_call_key(
                                Some(&lineage_id),
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
            message.tool_calls = match dispatches.get(&message.ordinal) {
                // The step that produced this message holds exactly these calls: report them.
                Some(found) if found.iter().map(|d| &d.tool_call_id).eq(ids.iter()) => found
                    .iter()
                    .map(|dispatch| broca_wire::SessionReadToolCall {
                        tool_call_id: dispatch.tool_call_id.clone(),
                        call_key: dispatch.intent_seq.and_then(|seq| {
                            broca_core::tool_call_key(
                                Some(&lineage_id),
                                seq,
                                dispatch.recorded_call_key.as_deref(),
                            )
                        }),
                        dispatched_to: dispatch.dispatched_to.clone(),
                        indeterminate: dispatch.indeterminate,
                    })
                    .collect(),
                // No recorded step produced this message (an imported one, say): report each
                // call with no dispatch fact rather than guess one.
                _ => ids
                    .into_iter()
                    .map(|tool_call_id| broca_wire::SessionReadToolCall {
                        tool_call_id,
                        call_key: None,
                        dispatched_to: None,
                        indeterminate: false,
                    })
                    .collect(),
            };
        }
        Ok(())
    }
}

#[async_trait]
impl ServeHandler for crate::LineageRegistry {
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
    async fn check_migration(&self, session: &LineageKey, write: bool) -> Result<(), ServeError> {
        self.check_move(session, write).map_err(Into::into)
    }
    fn compaction_supported(&self) -> bool {
        crate::LineageRegistry::compaction_supported(self)
    }

    async fn tag_location(
        &self,
        session: &LineageKey,
        tag: u64,
    ) -> Result<Option<broca_core::TagLocation>, ServeError> {
        self.route_tag_location(session, tag)
            .await
            .map_err(ServeError::from)
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

    async fn session_log(
        &self,
        session: &LineageKey,
        run_id: &str,
    ) -> Result<Option<Vec<broca_wal::WalRecord>>, ServeError> {
        let range = self
            .read_run_tool_calls_range(session, run_id, 0, None, None)
            .await?;
        match range.records() {
            broca_wal::WalRangeRecords::Complete(records) => Ok(Some(
                records.iter().map(|framed| framed.record.clone()).collect(),
            )),
            broca_wal::WalRangeRecords::Absent => Ok(Some(Vec::new())),
            broca_wal::WalRangeRecords::Partial(_) | broca_wal::WalRangeRecords::Unknown(_) => {
                Err(ServeError::Open(
                    "the session log could not be read completely".into(),
                    Some(broca_protocol::ErrorClass::Transient),
                ))
            }
        }
    }

    async fn read_model(
        &self,
        req: SessionReadRequest,
    ) -> Result<broca_wire::ModelPage, ServeError> {
        if req.after_mid.is_some() {
            return Err(ServeError::InvalidParams {
                field: "view".into(),
                message: "model view does not support after_mid".into(),
            });
        }
        let page = self
            .route_read_model_page(
                &req.session,
                &broca_core::ModelPageRequest {
                    from_ordinal: req.from_ordinal,
                    after_mid: None,
                    limit: req.limit,
                    max_bytes: req.max_bytes,
                },
            )
            .await?;
        if req
            .lineage_id
            .as_ref()
            .is_some_and(|requested| page.lineage_id.as_ref() != Some(requested))
        {
            return Err(ServeError::LineageChanged {
                requested: req.lineage_id.unwrap(),
            });
        }
        let mut page: broca_wire::ModelPage = page.try_into().map_err(|error| {
            ServeError::Open(
                format!("model page source: {error}"),
                Some(broca_protocol::ErrorClass::Permanent),
            )
        })?;
        if broca_protocol::attachments_enabled()
            && req.media == cortexkit_role_llm_runner::read::MediaMode::Inline
        {
            page.render_inline_media();
        }
        let subject = model_group_reply_too_large_subject(&page);
        if let Some(error) =
            reply_frame_limit_refusal(broca_wire::serialized_result_frame_bytes(&page), subject)
        {
            return Err(error);
        }
        Ok(page)
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

    async fn authorize_session_retract(
        &self,
        session: &LineageKey,
        principal: Option<&broca_protocol::BindPrincipal>,
        scope: &broca_protocol::SendScope,
        submission_id: Option<&str>,
    ) -> Result<(), ServeError> {
        // Runner 0.7.29 CONTRACT.md:2333-2362 (§9.2) extends only session.retract:
        // owners keep their exact path; another attested sender may withdraw only its
        // recorded submission. run.cancel remains owner-only, even for that sender.
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
            SendOutcome::Upload(error) => Err(ServeError::Upload(error)),
            // Actor errors can combine replay, storage and lifecycle failures after type erasure.
            SendOutcome::Error(e) => Err(ServeError::Open(e, None)),
            SendOutcome::ClassifiedError { reason, class } => {
                Err(ServeError::Open(reason, Some(class)))
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
        self.check_expired(session, Some(&own_run.0)).await?;
        let records: Vec<_> = self
            .read_durable_wal(session)
            .await?
            .0
            .into_iter()
            .map(|frame| frame.record)
            .collect();
        let own_run =
            if let Some(sender @ broca_protocol::BindPrincipal::Reserved { .. }) = principal {
                crate::send_fate::resolve_sender_run_id(&records, &own_run, send_id, sender)
                    .map_err(|_| ServeError::UnknownSend)?
            } else {
                own_run
            };
        // Ordered senders share a session, not each other's send keys. Legacy owner records
        // omit sent_by, so their principal is the one on Queued or RunStarted.
        let submission = format!("sub-{}", own_run.0);
        let sender = records.iter().find_map(|record| match record {
            broca_wal::WalRecord::DeliveryOrdered {
                submission_id,
                sent_by: Some(sender),
                ..
            } if submission_id.0 == submission => Some(Some(sender)),
            broca_wal::WalRecord::Queued {
                submission_id,
                principal,
                ..
            } if submission_id.0 == submission => Some(principal.as_ref()),
            broca_wal::WalRecord::RunStarted {
                run_id, principal, ..
            } if run_id == &own_run => Some(principal.as_ref()),
            _ => None,
        });
        let sender = sender.or_else(|| {
            records.iter().find_map(|record| match record {
                broca_wal::WalRecord::RunStarted { principal, .. } => Some(principal.as_ref()),
                _ => None,
            })
        });
        if sender
            .flatten()
            .is_some_and(|sender| Some(sender) != principal)
        {
            return Err(ServeError::UnknownSend);
        }
        let legacy_steers = !records.iter().any(|record| matches!(record,
            broca_wal::WalRecord::DeliveryOrdered { submission_id, .. } if submission_id.0 == submission));
        let outcome =
            crate::send_fate::recorded_send_outcome(&records, &own_run, send_id, legacy_steers)
                .ok_or(ServeError::UnknownSend)?;
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
            crate::actor::SendOutcome::Error(message) => {
                return Err(ServeError::Open(message, None))
            }
            _ => unreachable!("recorded send fate cannot admit work"),
        };
        let baseline = match &result {
            broca_wire::SendResult::Active { run_id } | broca_wire::SendResult::Finished { run_id, .. } if run_id == &own_run.0 => {
                records.iter().find(|record| matches!(record, broca_wal::WalRecord::RunStarted { .. }))
                    .filter(|record| matches!(record, broca_wal::WalRecord::RunStarted { run_id, .. } if run_id == &own_run))
                    .map(baseline_of)
            }
            _ => None,
        };
        Ok(broca_wire::SendReplyWire {
            result,
            baseline,
            delivered,
        })
    }

    async fn plan_state(&self, session: &LineageKey) -> PlanState {
        match self.session_ownership_for_bind(session).await {
            None => PlanState::Unknown,
            Some(ownership) if ownership.episodes == 0 => PlanState::NotAdmitted,
            Some(ownership) => PlanState::Admitted {
                frozen: ownership
                    .first_episode
                    .and_then(|first| first.manifest)
                    .map(|manifest| manifest.plan_identity.clone()),
            },
        }
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
        match broca_core::decide_scope(
            &ownership,
            principal,
            &broca_protocol::SendScope::Stamped(scope.clone()),
        ) {
            Err(broca_core::ScopeRefusal::AdoptionRefused(refusal)) => Ok(Some(refusal)),
            _ => Ok(None),
        }
    }

    async fn import(&self, req: SessionImportRequest) -> Result<SessionImportResponse, ServeError> {
        use crate::actor::ImportOutcome;
        // Refused before the session is routed or opened, so a transcript naming media inline
        // writes nothing: a person names media only through upload references.
        if let Err((field, message)) = broca_wire::check_supplied_media(&req.messages) {
            return Err(ServeError::InvalidParams { field, message });
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
        };
        match outcome {
            SubscribeOutcome::Attached => Ok(()),
            SubscribeOutcome::Reopen => unreachable!("registry retry consumes Reopen"),
            // Attachment errors erase both replay and channel failures.
            SubscribeOutcome::Error(e) => Err(ServeError::Open(e, None)),
        }
    }

    async fn read(&self, req: SessionReadRequest) -> Result<SessionReadResponse, ServeError> {
        let late_only = req.late_after.is_some();
        // A mid not of the form `m<ordinal>` names no message. The read still runs (from past
        // the last ordinal, so it returns no message), because a read naming a stale lineage
        // must be refused `lineage_changed` before its mid is checked, whatever the mid.
        let after = req.after_mid.as_deref().map(ordinal_of_mid);
        let from_ordinal = match after {
            Some(Some(ordinal)) => Some(ordinal.saturating_add(1)),
            Some(None) => Some(u64::MAX),
            None => req.from_ordinal,
        };
        let crate::registry::RouteReadPage {
            mut page,
            tools,
            dispatches,
            open_batch,
            ..
        } = self
            .route_read_page(
                &req.session,
                if late_only { Some(0) } else { from_ordinal },
                if late_only { Some(0) } else { req.limit },
                req.include_tools && !late_only,
            )
            .await
            .map_err(|error| {
                if late_only && matches!(error, crate::registry::OpenError::Expired { .. }) {
                    let refusal =
                        broca_wire::InvalidLateCursor::new(req.late_after.as_deref().unwrap());
                    ServeError::InvalidParams {
                        field: refusal.detail.field,
                        message: refusal.message,
                    }
                } else {
                    ServeError::from(error)
                }
            })?;
        if let Some(requested) = &req.lineage_id {
            let current = page.lineage_id.map(|lineage_id| lineage_id.to_string());
            if current.as_deref() != Some(requested.as_str()) {
                return Err(ServeError::LineageChanged {
                    requested: requested.clone(),
                });
            }
        }
        // A Setup answer may still anchor the running run's numbers at a checkpoint and change
        // every number from its cut (llm-runner §9.6): until it is durable, refuse rather than
        // show numbers a later read would contradict. The window lasts one Setup call.
        if page.tags_await_setup {
            return Err(ServeError::TagsAwaitSetup);
        }
        if let (Some(after), Some(mid)) = (after, &req.after_mid) {
            if !after.is_some_and(|ordinal| ordinal < page.prompt_len) {
                return Err(ServeError::UnknownMid(mid.clone()));
            }
        }
        let cap = req
            .max_bytes
            .unwrap_or(broca_wire::READ_DEFAULT_MAX_BYTES)
            .min(broca_wire::READ_MAXIMUM_MAX_BYTES);
        let unreported = matches!(
            open_batch,
            Some(broca_wire::OpenBatchView::NotReported { .. })
        );
        if late_only && unreported {
            return Err(ServeError::Open(
                "late approval metadata is not reported because the WAL tail is incomplete".into(),
                Some(broca_protocol::ErrorClass::Transient),
            ));
        }
        let default_page = req.from_ordinal.unwrap_or(0) == 0 && req.after_mid.is_none();
        let had_late = !page.late_approvals.is_empty();
        let (late, next_late_after) = if !unreported && (late_only || default_page) {
            let (entries, next) =
                crate::approval::late_page(&page.late_approvals, req.late_after.as_deref(), cap)
                    .map_err(|error| ServeError::InvalidParams {
                        field: error.detail.field,
                        message: error.message,
                    })?;
            (Some(entries), next)
        } else {
            (None, None)
        };
        page.late_approvals.clear();
        if late_only {
            page.messages.clear();
            page.next_from_ordinal = None;
        }
        let prompt_len = page.prompt_len;
        let mut response = session_read_response(page, tools);
        if req.include_rich_ui {
            let mut stamps = std::collections::BTreeMap::new();
            for entry in &mut response.messages {
                if entry.message.role != broca_protocol::Role::Assistant {
                    continue;
                }
                let Some(run) = &entry.run else { continue };
                if let std::collections::btree_map::Entry::Vacant(slot) =
                    stamps.entry(run.run_id.clone())
                {
                    let stamp = self
                        .session_log(&req.session, &run.run_id)
                        .await?
                        .into_iter()
                        .flatten()
                        .find_map(|record| match record {
                            broca_wal::WalRecord::RunStarted {
                                run_id, manifest, ..
                            } if run_id.0 == run.run_id => {
                                manifest.and_then(|manifest| manifest.rich_ui)
                            }
                            _ => None,
                        });
                    slot.insert(stamp);
                }
                entry.rich_ui = stamps[&run.run_id].clone();
            }
        }
        if had_late || late_only {
            response.late_approvals = late;
        }
        response.next_late_after = next_late_after;
        if open_batch.is_none() || unreported {
            response.deferred_calls = None;
            response.late_approvals = None;
        }
        // Preserve legacy session.read JSON when the session has no approval records.
        // Omitting approval metadata means it is not reported, rather than asserting empty lists.
        response.open_batch = if unreported {
            open_batch
        } else {
            response.deferred_calls.as_ref().and(open_batch)
        };
        if late_only {
            response.deferred_calls = None;
            response.open_batch = None;
        }
        self.attribute_tool_calls(&req.session, &mut response, &dispatches, prompt_len)
            .await?;
        if req.media == cortexkit_role_llm_runner::read::MediaMode::Blocks {
            for entry in &mut response.messages {
                if broca_protocol::attachments_enabled() {
                    entry.show_media_blocks_with_identity();
                } else {
                    entry.show_media_blocks();
                }
            }
        }
        if req.media == cortexkit_role_llm_runner::read::MediaMode::Descriptors {
            for entry in &mut response.messages {
                // Only structure that is not payload can outgrow the per-message budget.
                if let Err(ordinal) = entry.describe() {
                    return Err(ServeError::ReplyTooLarge(ReplyTooLargeDetail::new(
                        json_bytes(entry),
                        cortexkit_role_llm_runner::read::descriptors::DESCRIPTOR_MESSAGE_MAX as u64,
                        ReplyTooLargeSubject::Message {
                            session: req.session.session.clone(),
                            ordinal,
                        },
                    )));
                }
            }
        }
        cap_page_bytes(
            &mut response,
            cap,
            from_ordinal.is_none(),
            &req.session.session,
        )?;
        Ok(response)
    }

    async fn head(&self, session: &LineageKey) -> Result<broca_wire::HeadResponse, ServeError> {
        self.head_for(session, None, &broca_protocol::SendScope::default())
            .await
    }

    async fn head_for(
        &self,
        session: &LineageKey,
        principal: Option<&broca_protocol::BindPrincipal>,
        scope: &broca_protocol::SendScope,
    ) -> Result<broca_wire::HeadResponse, ServeError> {
        // The newest page of one message: the store's last row and the WAL past its watermark,
        // so the cost follows what is not yet materialized, never the session's length.
        let crate::registry::RouteReadPage {
            page,
            at_seq,
            head_facts,
            head_facts_complete,
            newest_record_ms,
            retention_activity_ms,
            last_title,
            ..
        } = self.route_read_page(session, None, Some(1), false).await?;
        // A session never written has no lineage, and then nothing else to report either.
        // A session never written answers every member absent, `input` included: the
        // conformance kit (0.10.2 `never_written_session_empty_head`) requires it, although
        // llm-runner 0.5.1 §4.3 lets such a session state `input`.
        let Some(lineage_id) = page.lineage_id else {
            return Ok(broca_wire::HeadResponse::default());
        };
        if !head_facts_complete {
            return Err(crate::registry::OpenError::Open(
                "session.head cannot attest metadata from an incomplete WAL suffix".into(),
            )
            .into());
        }
        // Retained sessions report expiry activity; maintenance must not move that clock.
        // Otherwise use the live WAL file's own write time, a stat; else
        // (its WAL was folded into an archive) the newest timestamped record of the read's
        // tail. Only a folded session whose tail holds no timestamped record pays for a pass
        // over its whole WAL.
        let updated_at = match retention_activity_ms
            .or_else(|| self.live_wal_written_ms(session).or(newest_record_ms))
        {
            Some(at) => at,
            None => self
                .read_durable_wal(session)
                .await?
                .0
                .iter()
                .rev()
                .find_map(|framed| broca_core::record_time_ms(&framed.record))
                .unwrap_or(0),
        };
        let media_facts = if broca_protocol::attachments_enabled() {
            Some(
                self.media_facts_for_head(
                    session,
                    crate::registry::media_head::Request {
                        lineage: lineage_id,
                        at_seq,
                        run_id: page.lineage_state.last_run_id.as_ref(),
                        state: page.lineage_state.state.as_str(),
                        model: head_facts.model.as_ref(),
                    },
                )
                .await?,
            )
        } else {
            None
        };
        let interrupted_reason = if page.lineage_state.state.as_str() == "interrupted" {
            if let Some(facts) = &media_facts {
                facts.interrupt_reason.clone()
            } else if let Some(run_id) = &page.lineage_state.last_run_id {
                recorded_interrupt_reason(&self.read_durable_wal(session).await?.0, run_id)
            } else {
                None
            }
        } else {
            None
        };
        let sender = head_facts.sender(principal, scope, broca_wire::DELIVERY_MODES);
        let last_run_error = if page.lineage_state.state.as_str() == "error" {
            if let Some(run_id) = page.lineage_state.last_run_id.clone() {
                // Query the named run through the same terminal-record reconciliation as
                // run.status, rather than classifying provider diagnostics a second time.
                match self.run_status(session, run_id).await? {
                    RunStatusResponse::Error {
                        error: Some(error), ..
                    } => {
                        let mut summary =
                            cortexkit_role_llm_runner::read::LastRunError::new(error.class_wire());
                        summary.retry_after_secs = error
                            .retry_after_secs
                            .and_then(cortexkit_role_llm_runner::read::RetryAfterSecs::new);
                        summary.resets_at_ms = error.resets_at_ms;
                        Some(summary)
                    }
                    _ => None,
                }
            } else {
                None
            }
        } else {
            None
        };
        let input = media_facts.map_or_else(broca_wire::head_input, |facts| facts.input);
        Ok(broca_wire::HeadResponse {
            last_run_rich_ui: head_facts.last_run_rich_ui,
            last_run_error,
            model: head_facts.model,
            usage: head_facts.usage.filter(|usage| usage.consistent()),
            sender,
            incarnation: lineage_id.incarnation(),
            lineage_id: Some(lineage_id.to_string()),
            last_ordinal: page.prompt_len.checked_sub(1),
            first_ordinal: Some(0),
            history_version: Some(0),
            head: page.head.map(|cursor| CursorWire {
                wal_seq: cursor.wal_seq,
                sub_index: cursor.sub_index,
            }),
            last_run_state: page
                .lineage_state
                .last_run_id
                .map(|run_id| broca_wire::LastRunState {
                    run_id: run_id.0,
                    state: page.lineage_state.state.as_str().to_owned(),
                    reason: interrupted_reason.or_else(|| {
                        page.lineage_state
                            .reason
                            .filter(|reason| reason.as_str() != "interrupted")
                            .map(|reason| reason.as_str().to_owned())
                    }),
                }),
            updated_at: Some(updated_at),
            cache: self
                .cache_clock_reading(session)
                .map(|reading| broca_wire::HeadCacheWire {
                    last_sent_at_ms: reading.last_sent_at_ms,
                    ttl_ms: reading.ttl_ms,
                    expires_at_ms: reading.expires_at_ms(),
                    prefix_tokens: reading.prefix_tokens,
                    expiry_basis: broca_wire::EXPIRY_BASIS_ESTIMATED.into(),
                }),
            title: last_title,
            input: Some(input),
        })
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
        let row_read = self.read_run_row_state(session, &run_id).await?;
        let (state, mut records) = match &row_read {
            Some(read) => (Some(read.state.clone()), Some(read.records.clone())),
            None => {
                self.route_run_state_with_wal(session, run_id.clone())
                    .await?
            }
        };
        let auth_selection = state.as_ref().and_then(frozen_auth_selection);
        let mut detail = state.as_ref().and_then(|state| state.budget_detail.clone());
        let mut context_ceiling = state
            .as_ref()
            .and_then(|state| state.context_ceiling.clone());
        let mut structured_output = state
            .as_ref()
            .and_then(|state| state.structured_output.clone());
        let mut usage = state
            .as_ref()
            .map(|state| state.run_usage.clone())
            .unwrap_or_default();
        let status = run_status_response(state);
        let (mut state, mut reason, mut error) = match status {
            RunStatusResponse::Unknown => return Err(ServeError::UnknownRun(run_id.0)),
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
        };
        if state == "active" {
            // The live actor still calls a run active for a moment after its terminal is
            // durable, while `session.read` and `session.head` already read that terminal from
            // the WAL. The durable terminal wins, so a caller that saw the run end on a page is
            // never told it is still running.
            let read = match records.take() {
                Some(records) => records,
                None => self.read_durable_wal(session).await?.0,
            };
            if let Some((reason, terminal_error)) = durable_terminal(&read, &run_id) {
                let reconciled = broca_core::reconcile_run_state(
                    read.iter().map(|frame| &frame.record),
                    &run_id.0,
                );
                detail = reconciled
                    .as_ref()
                    .and_then(|state| state.budget_detail.clone());
                usage = reconciled
                    .as_ref()
                    .map(|state| state.run_usage.clone())
                    .unwrap_or_default();
                context_ceiling = reconciled
                    .as_ref()
                    .and_then(|state| state.context_ceiling.clone());
                structured_output = reconciled.and_then(|state| state.structured_output);
                state = serde_json::to_value(reason)
                    .ok()
                    .and_then(|value| value.as_str().map(str::to_owned))
                    .unwrap_or(state);
                error = terminal_error.map(Into::into);
            }
            records = Some(read);
        }
        if state == "interrupted" {
            let read = match records.take() {
                Some(records) => records,
                None => self.read_durable_wal(session).await?.0,
            };
            reason = recorded_interrupt_reason(&read, &run_id);
            records = Some(read);
        }
        let mut directives = None;
        let final_message = if state == "completed" {
            let records = match records {
                Some(records) => records,
                None => self.read_durable_wal(session).await?.0,
            };
            let message = match &row_read {
                Some(read) => final_message_from_prompt(&read.replayed.prompt, &run_id)?,
                None => final_message_of(&records, &run_id)?,
            };
            directives = completed_directives(&records, &run_id, &message.text);
            Some(message)
        } else {
            None
        };
        let response = broca_wire::RunResultResponse {
            auth_selection,
            directives,
            usage,
            detail,
            context_ceiling,
            run_id: run_id.0,
            state,
            reason,
            error,
            final_message,
            structured_output,
        };
        // A structured-output value is never cut short, so an answer it makes too large is
        // refused naming the run; otherwise the final message is the subject.
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
                    session: session.session.clone(),
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

    async fn run_tool_calls(
        &self,
        session: &LineageKey,
        request: cortexkit_role_llm_runner::tool_calls::ToolCallsRequest,
    ) -> Result<cortexkit_role_llm_runner::tool_calls::ToolCallsPage, ServeError> {
        use cortexkit_role_llm_runner::tool_calls::ToolCallsPage;

        if let Err(field) = request.check() {
            return Err(ServeError::InvalidParams {
                field: field.into(),
                message: "run.tool_calls.limit must be greater than zero".into(),
            });
        }
        let run_id = request.run_id.clone();
        let row_read = self
            .read_run_row(session, &broca_protocol::RunId(run_id.clone()))
            .await?;
        let limit = request
            .limit
            .unwrap_or(broca_core::SESSION_READ_DEFAULT_LIMIT)
            .min(broca_core::SESSION_READ_MAX_LIMIT) as usize;
        let (range, run_start_seq, run_end_seq, lineage_id, seed, range_start_seq, fallback) =
            match request.after.as_deref() {
                None => {
                    let range = match &row_read {
                        Some(read) => {
                            let mut range =
                                broca_wal::WalRangeRead::without_anchors(read.records.clone());
                            range.lineage_id = read.lineage_id;
                            range
                        }
                        None => {
                            self.read_run_tool_calls_range(session, &run_id, 0, None, None)
                                .await?
                        }
                    };
                    let records = match &row_read {
                        Some(read) => read.records.as_slice(),
                        None => run_tool_call_records(&range)?,
                    };
                    let Some((start, end)) = run_tool_call_bounds(records, &run_id) else {
                        return Err(ServeError::UnknownRun(run_id));
                    };
                    let lineage_id = range.lineage_id.as_ref().map(|lineage| lineage.to_string());
                    (range, start, end, lineage_id, None, 0, None)
                }
                Some(token) => {
                    let cursor: RunToolCallsCursor = serde_json::from_str(token).map_err(|_| {
                        invalid_tool_call_cursor(
                            "run.tool_calls.after is not a cursor returned for this run",
                        )
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
                    let lineage_id = cursor.lineage_id.clone();
                    let range = match &row_read {
                        Some(read) => {
                            let records = read
                                .records
                                .iter()
                                .filter(|f| {
                                    f.seq >= cursor.step_seq && f.seq <= cursor.batch_end_seq
                                })
                                .cloned()
                                .collect();
                            let mut range = broca_wal::WalRangeRead::without_anchors(records);
                            range.lineage_id = read.lineage_id;
                            range
                        }
                        None => {
                            self.read_run_tool_calls_range(
                                session,
                                &run_id,
                                cursor.step_seq - 1,
                                cursor.anchor_before.map(Into::into),
                                Some(cursor.batch_end_seq),
                            )
                            .await?
                        }
                    };
                    let observed_lineage =
                        range.lineage_id.as_ref().map(|lineage| lineage.to_string());
                    if observed_lineage
                        .as_ref()
                        .is_some_and(|observed| lineage_id.as_ref() != Some(observed))
                    {
                        return Err(invalid_tool_call_cursor(
                            "run.tool_calls.after belongs to a different WAL lineage",
                        ));
                    }
                    (
                        range,
                        cursor.run_start_seq,
                        cursor.run_end_seq,
                        lineage_id,
                        Some(cursor.clone()),
                        cursor.step_seq,
                        cursor.anchor_before,
                    )
                }
            };
        let row_records = row_read.as_ref().map(|read| {
            read.records
                .iter()
                .filter(|f| {
                    seed.as_ref().is_none_or(|cursor| {
                        f.seq >= cursor.step_seq && f.seq <= cursor.batch_end_seq
                    })
                })
                .cloned()
                .collect::<Vec<_>>()
        });
        let records = match &row_records {
            Some(records) => records.as_slice(),
            None => run_tool_call_records(&range)?,
        };
        let step_ordinals = if seed.is_none() {
            match &row_read {
                Some(read) => read
                    .replayed
                    .committed_tool_turns
                    .iter()
                    .filter_map(|(index, record_index)| {
                        let message = read.replayed.prompt.get(*index)?;
                        let seq = read.records.get(*record_index)?.seq;
                        Some((seq, message.ordinal.unwrap_or(*index as u64)))
                    })
                    .collect(),
                None => committed_tool_step_ordinals(records),
            }
        } else {
            std::collections::HashMap::new()
        };
        let mut entries = indexed_run_tool_calls(
            records,
            &run_id,
            lineage_id.as_deref(),
            seed.as_ref(),
            &step_ordinals,
        )?;
        if let Some(cursor) = &seed {
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
                    .is_none_or(|position: &RunToolCallsPosition| {
                        position.step_seq != entry.step_seq
                    })
                {
                    future_batches.push(indexed_run_tool_call_position(
                        entry,
                        &range,
                        range_start_seq,
                        fallback,
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

        let page_context = RunToolCallsPageContext {
            run_id: &run_id,
            lineage_id: lineage_id.as_deref(),
            run_start_seq,
            run_end_seq,
            range: &range,
            range_start_seq,
            fallback,
            seed: seed.as_ref(),
        };
        let max_bytes = broca_wire::role::READ_DEFAULT_MAX_BYTES;
        let mut page_calls = Vec::new();
        let mut consumed = 0usize;
        while consumed < entries.len() && page_calls.len() < limit {
            let entry = &entries[consumed];
            let mut candidate_calls = page_calls.clone();
            candidate_calls.push(entry.call.clone());
            let mut candidate = ToolCallsPage::new(run_id.clone(), candidate_calls);
            if let Some(next) = next_run_tool_calls_cursor(&entries, consumed + 1, &page_context)? {
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
        if let Some(next) = next_run_tool_calls_cursor(&entries, consumed, &page_context)? {
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

    async fn baseline(
        &self,
        session: &LineageKey,
        principal: Option<&broca_protocol::BindPrincipal>,
        scope: &broca_protocol::SendScope,
    ) -> Result<broca_wire::BaselineReply, ServeError> {
        let (records, _) = self.read_durable_wal(session).await?;
        let Some(start) = first_run_started(&records) else {
            return Ok(broca_wire::BaselineReply::not_yet());
        };
        if let Some(mismatch) = baseline_owner_mismatch(start, principal, scope) {
            return Err(ServeError::ScopeRefused(
                broca_core::ScopeRefusal::OwnerMismatch(Box::new(mismatch)),
            ));
        }
        let mut baseline = baseline_of(start);
        if let broca_wal::WalRecord::RunStarted { config, .. } = start {
            baseline.cache = self
                .cache_declaration(config)
                .map(|declaration| declaration.to_wire());
        }
        // Every ready baseline on a runner declaring `session_change` states the rungs it
        // supports per surface, and the generations it accepted, applied and holds pending.
        let change = broca_fetch_plan::session_change::SessionChangeState::fold(&records);
        if let Some(manifest) = &change.prefix_manifest {
            let cache = baseline.cache.take();
            baseline = planned_baseline(manifest);
            baseline.cache = cache;
        }
        baseline.accepted_generation = change.accepted_generation;
        baseline.applied_generation = change.applied_generation;
        baseline.pending = change.pending.as_ref().map(|pending| {
            cortexkit_role_llm_runner::baseline::PendingChange {
                generation: pending.generation,
                policy: pending.policy.clone(),
            }
        });
        baseline.effective = change.effective.as_ref().map(|effective| {
            cortexkit_role_llm_runner::baseline::EffectiveBaseline {
                generation: effective.generation,
                applied_by: effective.applied_by.clone(),
                manifest_digest: effective.manifest_digest.clone(),
            }
        });
        baseline.rungs = Some(cortexkit_role_llm_runner::baseline::Rungs {
            tools: broca_fetch_plan::session_change::TOOLS_RUNGS
                .iter()
                .map(|rung| rung.to_string())
                .collect(),
            system_text: broca_fetch_plan::session_change::SYSTEM_TEXT_RUNGS
                .iter()
                .map(|rung| rung.to_string())
                .collect(),
        });
        Ok(broca_wire::BaselineReply::ready(baseline))
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
                baseline.rungs = Some(cortexkit_role_llm_runner::baseline::Rungs {
                    tools: broca_fetch_plan::session_change::TOOLS_RUNGS
                        .iter()
                        .map(|rung| rung.to_string())
                        .collect(),
                    system_text: broca_fetch_plan::session_change::SYSTEM_TEXT_RUNGS
                        .iter()
                        .map(|rung| rung.to_string())
                        .collect(),
                });
                baseline
            })
    }

    async fn session_change_identities(
        &self,
        session: &LineageKey,
    ) -> Option<(Option<String>, Option<String>)> {
        let (records, _) = self.read_durable_wal(session).await.ok()?;
        let change = broca_fetch_plan::session_change::SessionChangeState::fold(&records);
        if change.accepted_generation == 0 {
            return None;
        }
        let accepted = change
            .accepted
            .get(&change.accepted_generation)
            .map(|(identity, _)| identity.clone());
        let effective = if change.applied_generation > 0 {
            change
                .accepted
                .get(&change.applied_generation)
                .map(|(identity, _)| identity.clone())
        } else {
            first_run_started(&records).and_then(|start| match start {
                broca_wal::WalRecord::RunStarted {
                    manifest: Some(manifest),
                    ..
                } => Some(manifest.plan_identity.clone()),
                broca_wal::WalRecord::RunStarted { .. } => None,
                _ => None,
            })
        };
        Some((effective, accepted))
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
        let _change_guard = self.lock_session_change(session).await;
        let (records, _) = self.read_durable_wal(session).await.map_err(|error| {
            SessionChangeError::Transient(format!("cannot read the session WAL: {error}"))
        })?;
        let Some(start) = first_run_started(&records) else {
            return Err(SessionChangeError::Transient(
                "the session has no baseline yet; send its first send first".into(),
            ));
        };
        if let Some(mismatch) = baseline_owner_mismatch(start, principal, scope) {
            return Err(SessionChangeError::OwnerMismatch(Box::new(mismatch)));
        }
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
        let change = broca_fetch_plan::session_change::SessionChangeState::fold(&records);
        let earlier = change.earlier(generation);
        match session_change::refresh_order(
            change.accepted_generation,
            generation,
            &plan_identity,
            &policy,
            earlier,
        ) {
            session_change::RefreshOrder::Repeat => {
                let (run_in_progress, applies_from_next_run) = change.accepted_replies[&generation];
                return Ok(session_change::RefreshReply::new(
                    cortexkit_role_llm_runner::baseline::PendingChange {
                        generation,
                        policy: policy.clone(),
                    },
                    run_in_progress,
                    applies_from_next_run,
                ));
            }
            session_change::RefreshOrder::Stale => {
                return Err(SessionChangeError::GenerationStale {
                    accepted: change.accepted_generation,
                });
            }
            session_change::RefreshOrder::New => {}
        }
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
            change.current_manifest.as_deref(),
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
        let (records, _) = self
            .read_durable_wal(session)
            .await
            .map_err(|error| SessionChangeError::Transient(error.to_string()))?;
        let Some(start) = first_run_started(&records) else {
            return Err(SessionChangeError::Transient(
                "the session has no baseline yet; send its first send first".into(),
            ));
        };
        if let Some(mismatch) = baseline_owner_mismatch(start, principal, scope) {
            return Err(SessionChangeError::OwnerMismatch(Box::new(mismatch)));
        }
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
        let change = broca_fetch_plan::session_change::SessionChangeState::fold(&records);
        if check_session_policy(&change, generation, &policy, &send_id)? {
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
        let (records, _) = self.read_durable_wal(session).await.map_err(|error| {
            SessionChangeError::Transient(format!("cannot read the session WAL: {error}"))
        })?;
        let Some(start) = first_run_started(&records) else {
            return Err(SessionChangeError::Transient(
                "the session has no baseline yet; send its first send first".into(),
            ));
        };
        if let Some(mismatch) = baseline_owner_mismatch(start, principal, scope) {
            return Err(SessionChangeError::OwnerMismatch(Box::new(mismatch)));
        }
        if send_id.is_empty() {
            return Err(SessionChangeError::InvalidParams {
                field: "send_id".into(),
                message: "send_id is required".into(),
            });
        }
        let change = broca_fetch_plan::session_change::SessionChangeState::fold(&records);
        if let Some(pending_generation) = change.flush_send_ids.get(&send_id) {
            return Ok(session_change::FlushPrefixReply::new(*pending_generation));
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
        self.wal_written(session).ok_or_else(|| {
            ServeError::Open(
                "cannot determine whether the session has a WAL; retry the send".into(),
                Some(broca_protocol::ErrorClass::Transient),
            )
        })
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
        let row_read = self.read_run_row_state(session, &run_id).await?;
        let state = match &row_read {
            Some(read) => Some(read.state.clone()),
            None => self.route_run_state(session, run_id.clone()).await?,
        };
        let mut response = run_status_response(state);
        // Lifecycle resolution above remains authoritative when its transcript reader or
        // final text is unavailable. Enrich the answer only with readable durable metadata.
        let metadata = if matches!(
            response,
            RunStatusResponse::Interrupted { .. } | RunStatusResponse::Completed { .. }
        ) {
            match &row_read {
                Some(read) => Some(read.records.clone()),
                None => self.read_durable_wal(session).await.ok().map(|read| read.0),
            }
        } else {
            None
        };
        match (&mut response, metadata.as_deref()) {
            (RunStatusResponse::Interrupted { reason, .. }, Some(records)) => {
                *reason = recorded_interrupt_reason(records, &run_id);
            }
            (RunStatusResponse::Completed { directives, .. }, Some(records))
                if wake_directive_requested(records, &run_id) =>
            {
                *directives = match &row_read {
                    Some(read) => final_message_from_prompt(&read.replayed.prompt, &run_id),
                    None => final_message_of(records, &run_id),
                }
                .ok()
                .and_then(|message| completed_directives(records, &run_id, &message.text));
            }
            _ => {}
        }
        if let RunStatusResponse::Active {
            awaiting_approval, ..
        } = &mut response
        {
            *awaiting_approval = self.cached_awaiting_approval(session, &run_id);
        }
        // The same frame rule as `run.result`: a structured-output value is never cut short,
        // so an answer it makes too large is refused naming the run.
        let carries_value = serde_json::to_value(&response)
            .ok()
            .is_some_and(|value| value.pointer("/structured_output/value").is_some());
        if let Some(error) = reply_frame_limit_refusal(
            broca_wire::serialized_result_frame_bytes(&response),
            carries_value.then(|| ReplyTooLargeSubject::Run {
                run_id: run_id.0.clone(),
            }),
        ) {
            return Err(error);
        }
        Ok(response)
    }

    async fn run_status_reply(
        &self,
        session: &LineageKey,
        run_id: broca_protocol::RunId,
    ) -> Result<broca_wire::RunStatusReply, ServeError> {
        use broca_core::delivery_order::QueuedSendFate;
        use broca_wire::{QueuedSendState, RunStatusReply};
        let status = self.run_status(session, run_id.clone()).await?;
        if !matches!(status, RunStatusResponse::Unknown) {
            return Ok(status.into());
        }
        // No run carries this id. It may still be the id a queued send was minted from, which
        // callers poll until the send is a run; the WAL says what became of the send.
        let reply = match self.route_queued_send_fate(session, &run_id).await? {
            None => status.into(),
            Some(QueuedSendFate::Pending) => RunStatusReply::Send(QueuedSendState::Pending),
            Some(QueuedSendFate::Ended(reason)) => RunStatusReply::Send(match reason {
                broca_wal::record::SendEndReason::Superseded { .. } => QueuedSendState::Superseded,
                broca_wal::record::SendEndReason::Expired => QueuedSendState::Expired,
            }),
            Some(QueuedSendFate::Retracted) => RunStatusReply::Send(QueuedSendState::Retracted),
            // Delivered into a run that was already going: the send is part of that run now,
            // so its status is that run's, with the receipt naming the run.
            Some(QueuedSendFate::Delivered { run_id: into, .. }) => RunStatusReply::Run {
                status: Box::new(self.run_status(session, into.clone()).await?),
                delivered: Some(
                    cortexkit_role_llm_runner::send::Delivered::new(
                        cortexkit_role_llm_runner::send::delivered_as::STEP,
                    )
                    .with_ref(into.0),
                ),
            },
            // The send started its own run after the first lookup: answer that run.
            Some(QueuedSendFate::Started) => self.run_status(session, run_id).await?.into(),
        };
        Ok(reply)
    }
}

#[cfg(test)]
mod held_status_tests {
    use super::*;

    fn state(index: broca_core::RunIndexState) -> broca_core::ReconciledRunState {
        broca_core::ReconciledRunState {
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
            let wire = serde_json::to_value(run_status_response(Some(state(index)))).unwrap();
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
            let wire = serde_json::to_value(run_status_response(Some(reconciled))).unwrap();
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
    use super::run_status_response;

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
                let value = serde_json::to_value(run_status_response(Some(state.clone()))).unwrap();
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
                let value =
                    serde_json::to_value(run_status_response(Some(legacy.clone()))).unwrap();
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
                let value =
                    serde_json::to_value(run_status_response(Some(legacy.clone()))).unwrap();
                assert!(value.get("auth_selection").is_none());
                legacy.auth_selection = None;
                let value = serde_json::to_value(run_status_response(Some(legacy))).unwrap();
                assert!(value.get("auth").is_none());
                assert!(value.get("auth_selection").is_none());
            }
        }
    }
}

#[cfg(test)]
#[path = "serve/run_tool_calls_tests.rs"]
mod run_tool_calls_tests;

#[cfg(test)]
mod context_ceiling_tests {
    use super::*;
    fn reconciled(state: broca_core::RunIndexState) -> broca_core::ReconciledRunState {
        broca_core::ReconciledRunState {
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
        let wire = serde_json::to_value(run_status_response(Some(state))).unwrap();
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
        let wire = serde_json::to_value(run_status_response(Some(state))).unwrap();
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
        let wire = serde_json::to_value(run_status_response(Some(state))).unwrap();
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

/// Offset-based head for overhaul roots only (not yet serving production).
///
/// Today's `head_for` serves the newest page of one message through the store and the
/// WAL past its watermark. The offset index serves the same head from the session row
/// and the newest message row's byte range: the head decodes only those frames, never
/// the whole log. A Setup in the tail renumbers indexed tags, so the caller advances
/// the index before serving when [`head_tail_needs_index_up`] is true. Production offset reads
/// wire `broca.db` into `head_for`; this helper already pins the decode bound the head
/// must keep.
///
/// The head's byte range is the newest message row's `[lo_off, hi_off)`: one frame for a
/// single-message turn, the turn's span for a tool turn whose two messages share it.
/// With frame-decode counting on, serving the head decodes only that range.
#[allow(dead_code, reason = "used once broca.db opens on overhaul state roots")]
pub(crate) fn head_byte_range_for_offsets(
    newest: Option<&crate::registry::OffsetPageRow>,
) -> Option<(u64, u64)> {
    newest.map(|row| (row.lo_off, row.hi_off))
}

/// Whether a head whose tail holds a durable Setup may be served from the current index.
///
/// Delegates to the replay crate's detector so head, page and tail checks cannot drift
/// apart. A durable Setup before its index commit (durable tail, stale index) forces a
/// catch-up: recovery replays the Setup into the index before the head is served.
#[allow(dead_code, reason = "used once broca.db opens on overhaul state roots")]
pub(crate) fn head_tail_needs_index_up(tail: &[broca_wal::FramedRecord]) -> bool {
    broca_core::replay::tail_contains_compaction_setup(tail)
}

/// Offset head decode-bounding and Setup catch-up tests.
///
/// Pins that the head decodes only its own row range and that a Setup in the tail
/// forces an index catch-up before serving, including the crash-cut where the Setup is
/// durable but its index commit has not happened.
#[cfg(test)]
mod offset_head_tests {
    use std::cell::Cell;
    use std::sync::Arc;

    use broca_protocol::{
        caller::RunFinishReason,
        content::{AssistantMessage, ContentBlock, Message, Role},
        request::ModelHandle,
        BindIdentity, MessageId, RunId, StepId, Usage,
    };
    use broca_wal::{
        CompactionMessageKind, FramedRecord, RunConfig, RunOrigin, SetupTagCheckpoint, WalRecord,
    };

    use super::{head_byte_range_for_offsets, head_tail_needs_index_up};
    use crate::registry::{decode_frames_at_offsets, OffsetPageRow};

    fn user(text: &str) -> Message {
        Message::new(Role::User, vec![ContentBlock::Text { text: text.into() }])
    }

    fn run_started() -> WalRecord {
        WalRecord::RunStarted {
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
            run_id: RunId("r1".into()),
            session: BindIdentity::new("/tmp/project", "runner", "session"),
            config: Box::new(RunConfig {
                turn_directives_v1: None,
                render_tags: false,
                computed_tags: None,
                model: ModelHandle::new("stub", "m"),
                tools: vec![].into(),
                tool_choice: Default::default(),
                generation: Default::default(),
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
            input: vec![user("task")].into(),
            origin: RunOrigin::Fresh,
            submission_id: None,
        }
    }

    fn text_step() -> WalRecord {
        WalRecord::ModelStepFinished {
            fallback: Default::default(),
            written_at_ms: None,
            tags: None,
            hooks: None,
            step_id: StepId(1),
            assistant_message: Arc::new(AssistantMessage {
                message_id: MessageId("m1".into()),
                content: vec![ContentBlock::Text {
                    text: "answer".into(),
                }],
            }),
            usage: Usage::default(),
            finish_reason: None,
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        }
    }

    fn run_finished() -> WalRecord {
        WalRecord::RunFinished {
            interrupt_reason: None,
            ts_ms: None,
            reason: RunFinishReason::Completed,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        }
    }

    fn encode_log(records: &[WalRecord]) -> (Vec<u8>, Vec<(u64, u64)>) {
        let mut bytes = Vec::new();
        let mut offsets = Vec::new();
        for (i, record) in records.iter().enumerate() {
            let start = bytes.len() as u64;
            bytes.extend(broca_wal::framing::encode(i as u64 + 1, 1, record).expect("encode"));
            offsets.push((start, bytes.len() as u64));
        }
        (bytes, offsets)
    }

    #[test]
    fn head_decodes_only_its_range_and_catches_setup_in_tail() {
        let records = vec![
            run_started(),
            WalRecord::StepStarted { step_id: StepId(1) },
            text_step(),
            run_finished(),
        ];
        let (bytes, offsets) = encode_log(&records);
        // The head is the newest message's row: here the single answer frame.
        let newest = OffsetPageRow {
            ord: 1,
            lo_off: offsets[2].0,
            hi_off: offsets[2].1,
        };
        let range = head_byte_range_for_offsets(Some(&newest)).expect("head range");
        assert_eq!(range, (offsets[2].0, offsets[2].1));
        assert!(head_byte_range_for_offsets(None).is_none());
        // With counting on, the head decodes one frame, not the whole log.
        let counter = Cell::new(0u64);
        let head = decode_frames_at_offsets(&bytes, &[range], Some(&counter)).expect("head decode");
        assert_eq!(head.len(), 1);
        assert_eq!(counter.get(), 1);
        assert!(counter.get() < records.len() as u64);
        assert!(matches!(
            head[0].record,
            WalRecord::ModelStepFinished { .. }
        ));
        // No Setup in this tail: no catch-up.
        let tail: Vec<FramedRecord> = head;
        assert!(!head_tail_needs_index_up(&tail));
        // A durable Setup in the tail forces a catch-up before the head is served.
        // Crash-cut: these Setup bytes are durable while its index commit has not happened;
        // the reader sees the Setup past the indexed end and advances the index first.
        let mut with_setup = tail.clone();
        with_setup.push(FramedRecord {
            seq: with_setup.len() as u64 + 10,
            fence: 1,
            record: WalRecord::CompactionMessage {
                provider: "test".into(),
                kind: CompactionMessageKind::Setup,
                compaction_id: "cmp-1".into(),
                version: 1,
                range: broca_wal::CompactionRange { from: 0, to: 0 },
                replacement: Arc::new(Vec::new()),
                sha256: "0".repeat(64),
                request_id: None,
                cursor: None,
                call_when: None,
                stability: None,
                ceiling: None,
                tag_checkpoint: Some(SetupTagCheckpoint {
                    tag_checkpoint: 3,
                    cut: "m1".into(),
                }),
                ride_wait_ms: None,
            },
        });
        assert!(head_tail_needs_index_up(&with_setup));
    }
}

#[cfg(test)]
mod raw_database_composition_tests {
    use super::*;
    use broca_core::FileWalStore;
    use broca_wal::{Wal, WalRecord};
    use std::sync::{Arc, Mutex};

    struct NoActor;
    #[async_trait]
    impl crate::registry::LineageOpener for NoActor {
        async fn open(
            &self,
            _: &LineageKey,
            _: u64,
            _: crate::registry::RegistryHandle,
        ) -> Result<crate::registry::ActorHandle, crate::registry::OpenError> {
            panic!("read-only composition must not open an actor")
        }
    }

    #[tokio::test]
    async fn raw_database_test_bundle_preserves_legacy_head_page_and_cursor_bytes() {
        let root = broca_test_support::scratch_root()
            .join(format!("raw-composition-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("state-format"), "broca-state/2\n").unwrap();
        let key = LineageKey::new(root.join("project"), "runner", "session");
        let wal_store = Arc::new(FileWalStore::with_config(
            root.join("wal"),
            broca_wal::WalConfig::OVERHAUL,
        ));
        std::fs::create_dir_all(root.join("wal")).unwrap();
        let mut wal = broca_wal::FileWal::open(wal_store.wal_path(&key))
            .unwrap()
            .with_feature_floor(root.join("wal-floor"));
        let mut start: WalRecord = serde_json::from_str(
            include_str!("../../broca-wal/tests/fixtures/before_message_times.jsonl")
                .lines()
                .next()
                .unwrap(),
        )
        .unwrap();
        if let WalRecord::RunStarted { session, title, .. } = &mut start {
            *session = key.clone();
            *title = Some("WAL-only title".into());
        }
        let step: WalRecord = serde_json::from_value(serde_json::json!({"type":"model_step_finished","written_at_ms":1234,"step_id":1,"assistant_message":{"message_id":"m","content":[{"type":"text","text":"answer"}]},"usage":{"input_tokens":7,"output_tokens":3},"retries_used":0})).unwrap();
        let terminal: WalRecord = serde_json::from_value(serde_json::json!({"type":"run_finished","reason":"completed","usage":{"input_tokens":7,"output_tokens":3},"completed_steps":1,"retries_used":0})).unwrap();
        for record in [
            start,
            WalRecord::StepStarted {
                step_id: broca_protocol::StepId(1),
            },
            step,
            terminal,
        ] {
            wal.append(1, &record, false).unwrap();
        }
        wal.sync().unwrap();
        let publication = wal.durable_publication().unwrap();
        let db = Arc::new(broca_store::db::OverhaulDb::open(&root).unwrap());
        db.publish(
            &key,
            &broca_core::agent_session_ref(&key),
            broca_store::db::LogLocation::Live,
            &publication,
        )
        .unwrap();
        let bundle = Arc::new(crate::registry::RawPageTestBundle {
            db,
            publications: Mutex::new(std::collections::HashMap::from([(
                key.clone(),
                publication,
            )])),
        });
        let legacy = crate::registry::LineageRegistry::new_with_reader(
            Arc::new(NoActor),
            wal_store.clone(),
            None,
        );
        let raw =
            crate::registry::LineageRegistry::new_with_reader(Arc::new(NoActor), wal_store, None)
                .with_raw_pages_for_test(bundle.clone());
        let counter = broca_wal::access_counter::WalAccessCounter::default();
        let raw_head = counter
            .operation("composed raw head")
            .run_future(raw.head(&key))
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_vec(&raw_head).unwrap(),
            serde_json::to_vec(&legacy.head(&key).await.unwrap()).unwrap()
        );
        for from in [None, Some(0), Some(1), Some(500)] {
            let request = SessionReadRequest::new(key.clone(), from, Some(1));
            let actual = counter
                .operation(format!("composed raw page {from:?}"))
                .run_future(raw.read(request.clone()))
                .await
                .unwrap();
            assert_eq!(
                serde_json::to_vec(&actual).unwrap(),
                serde_json::to_vec(&legacy.read(request).await.unwrap()).unwrap()
            );
        }
        let mut request = SessionReadRequest::new(key.clone(), None, Some(1));
        request.lineage_id = Some("wrong-lineage".into());
        assert!(matches!(
            raw.read(request).await,
            Err(ServeError::LineageChanged { .. })
        ));
        let head = raw.head(&key).await.unwrap();
        let mut request = SessionReadRequest::new(key.clone(), None, Some(1));
        request.lineage_id = head.lineage_id;
        request.after_mid = Some("m0".into());
        let next = raw.read(request).await.unwrap();
        assert_eq!(next.messages[0].mid, "m1");
        let fixture = std::fs::read(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../scripts/fixtures/resume-close-out-sealed.wal"),
        )
        .unwrap();
        let mut at = 0;
        while at < fixture.len() {
            match broca_wal::framing::decode_next(&fixture[at..]) {
                broca_wal::framing::DecodeStep::Record { framed, consumed } => {
                    let mut record = framed.record;
                    if let WalRecord::RunStarted {
                        session,
                        run_id,
                        written_at_ms,
                        ..
                    } = &mut record
                    {
                        *session = key.clone();
                        run_id.0.push_str("-composed");
                        *written_at_ms = Some(2000);
                    }
                    if let WalRecord::ModelStepFinished { written_at_ms, .. } = &mut record {
                        *written_at_ms = Some(3000);
                    }
                    wal.append(1, &record, false).unwrap();
                    at += consumed;
                }
                broca_wal::framing::DecodeStep::Lineage { consumed, .. } => at += consumed,
                _ => panic!("invalid tool corpus"),
            }
        }
        wal.sync().unwrap();
        let publication = wal.durable_publication().unwrap();
        bundle
            .db
            .publish(
                &key,
                &broca_core::agent_session_ref(&key),
                broca_store::db::LogLocation::Live,
                &publication,
            )
            .unwrap();
        bundle
            .publications
            .lock()
            .unwrap()
            .insert(key.clone(), publication);
        for from in [None, Some(2), Some(3), Some(4)] {
            let request = SessionReadRequest::new(key.clone(), from, Some(2));
            let actual = counter
                .operation(format!("composed raw tool page {from:?}"))
                .run_future(raw.read(request.clone()))
                .await
                .unwrap();
            assert_eq!(
                serde_json::to_vec(&actual).unwrap(),
                serde_json::to_vec(&legacy.read(request).await.unwrap()).unwrap()
            );
        }
        assert!(!root.join("run-index.db").exists());
        for op in counter.snapshot() {
            assert!(
                op.seed_fallbacks.is_empty(),
                "{}: {:?}",
                op.name,
                op.seed_fallbacks
            );
            assert!(
                op.reads
                    .iter()
                    .all(|r| r.class != broca_wal::access_counter::ReadClass::Origin),
                "{}: {:?}",
                op.name,
                op.reads
            );
        }
        // Close every handle into the root before deleting it: the registries
        // and the bundle keep broca.db and the log open, and Windows refuses to
        // delete an open file (OS error 32).
        drop(raw);
        drop(legacy);
        drop(bundle);
        drop(wal);
        std::fs::remove_dir_all(root).unwrap();
    }
}
