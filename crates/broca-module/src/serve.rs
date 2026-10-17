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
use broca_core::{CapInstallRequest, CapMutation, SpendDeltaRequest};

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
    /// Read strictly after the message with this `mid`, taken from an earlier page of the
    /// lineage `lineage_id`. The serve adapter refuses a request that sets it without
    /// `lineage_id`, or together with `from_ordinal`.
    pub after_mid: Option<String>,
    /// The lineage the caller's cursor belongs to; a read naming another is refused.
    pub lineage_id: Option<String>,
    /// The page's byte cap; `None` is the default cap.
    pub max_bytes: Option<u64>,
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
            after_mid: None,
            lineage_id: None,
            max_bytes: None,
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

/// A `session.send` response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SendResponse {
    Paused {
        run_id: String,
        pause_reason: broca_protocol::PauseReason,
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
                ordinal: message.ordinal,
                mid: format!("m{}", message.ordinal),
                message: message.message,
                run: message.run,
                tool_calls: Vec::new(),
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
    let text: String = message
        .content
        .iter()
        .filter_map(|block| match block {
            broca_protocol::content::ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();
    Ok(broca_wire::FinalMessage {
        ordinal,
        mid: format!("m{ordinal}"),
        text,
    })
}

fn run_status_response(state: Option<broca_core::ReconciledRunState>) -> RunStatusResponse {
    use broca_core::RunIndexState as State;

    let Some(state) = state else {
        return RunStatusResponse::Unknown;
    };
    let final_step_finish_reason = state.final_step_finish_reason;
    let undelivered_steers = state.undelivered_steers;
    let indeterminate_tool_calls = state.indeterminate_tool_calls;
    let usage = state.usage;
    let retries_used = state.retries_used;
    let title = state.title;
    // Carried as `session.read` carries them: the error verbatim through the caller-facing
    // wrapper, the pause reason through the same spelling function, so both surfaces
    // report one run's cause identically.
    let error = state.error.map(Into::into);
    let pause_reason = state.pause_reason.map(|reason| {
        broca_core::SessionReadReason::Pause(reason)
            .as_str()
            .to_string()
    });
    match state.state {
        State::Active => RunStatusResponse::Active {
            step: state.live_step,
            title,
            awaiting_approval: None,
        },
        State::Paused => RunStatusResponse::Paused {
            pause_reason: state
                .pause_reason
                .filter(|reason| *reason == broca_protocol::PauseReason::Restart),
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
        },
        State::Interrupted => RunStatusResponse::Interrupted {
            error,
            final_step_finish_reason,
            undelivered_steers,
            indeterminate_tool_calls,
            usage,
            retries_used,
            title,
        },
        State::Completed => RunStatusResponse::Completed {
            final_step_finish_reason,
            undelivered_steers,
            usage,
            retries_used,
            title,
        },
        State::MaxSteps => RunStatusResponse::MaxSteps {
            final_step_finish_reason,
            undelivered_steers,
            usage,
            retries_used,
            title,
        },
        State::Cancelled => RunStatusResponse::Cancelled {
            final_step_finish_reason,
            undelivered_steers,
            usage,
            retries_used,
            title,
        },
        State::Error => RunStatusResponse::Error {
            final_step_finish_reason,
            undelivered_steers,
            usage,
            retries_used,
            error,
            title,
        },
        State::TransformUnavailable => RunStatusResponse::TransformUnavailable {
            final_step_finish_reason,
            undelivered_steers,
            usage,
            retries_used,
            title,
        },
    }
}

/// An error from the serve surface (distinct from a successful per-request outcome). Opening
/// failures describe lease or store problems; `SendIdReuse` is a typed request rejection.
#[derive(Debug, Clone)]
pub enum ServeError {
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
    /// A deterministic send identity was reused with a different frozen request field.
    SendIdReuse {
        differing: String,
    },
    /// Admission failed before durable start because a dependency is temporarily unavailable.
    Transient(String),
    /// A durable cap receiver needs a fresh baseline before it can continue.
    BaselineRequired,
    /// The daemon's cap table was not configured.
    CapUnavailable(String),
    /// Admission was denied by a durable budget cap.
    BudgetDenied {
        cap_id: String,
        reason: String,
    },
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
            OpenError::NewerWriter { missing, reason } => {
                ServeError::WalNewerWriter { missing, reason }
            }
        }
    }
}

impl std::fmt::Display for ServeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServeError::SessionMoving { cut_id } => write!(f, "session is moving under cut {cut_id}"),
            ServeError::SessionMoved { holder } => write!(f, "session moved to {holder}"),
            ServeError::Expired { expired_at_ms } => write!(f, "lineage expired at {expired_at_ms}"),
            ServeError::RetentionInvalid(_) => write!(f, "invalid retention"),
            ServeError::SessionHeld(s) => write!(f, "session '{s}' held by a live writer"),
            ServeError::VariantUnsupported(e) => write!(f, "open failed: run resolution failed: {e}"),
            ServeError::SessionTooLarge { max_bytes } => write!(f, "session log cannot exceed {max_bytes} bytes"),
            ServeError::RetainedSessionTooLarge { max_bytes } => write!(f, "retained session log cannot exceed {max_bytes} bytes"),
            ServeError::Open(s, _) | ServeError::WalNewerWriter { reason: s, .. } => {
                write!(f, "open failed: {s}")
            }
            ServeError::ReplyTooLarge(_) => {
                f.write_str("the session.read reply exceeds the transport frame limit")
            }
            ServeError::SendRetracted { submission_id } => {
                write!(f, "submission '{submission_id}' was retracted")
            }
            ServeError::SendIdReuse { differing } => write!(
                f,
                "send_id was already used with different {differing}; retries must be byte-identical"
            ),
            ServeError::Transient(s) => write!(f, "transient admission failure: {s}"),
            ServeError::BaselineRequired => f.write_str("baseline_required"),
            ServeError::CapUnavailable(s) => write!(f, "cap table unavailable: {s}"),
            ServeError::BudgetDenied { cap_id, reason } => {
                write!(f, "budget_denied: cap '{cap_id}' ({reason})")
            }
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

    /// The outer error is routing failure; the inner error is the role's provider refusal.
    /// `caller` must be the daemon-verified module stamp, not a request member.
    async fn compaction_ready(
        &self,
        _session: &LineageKey,
        _caller: &str,
        _request: broca_wire::CompactionReady,
    ) -> Result<Result<broca_wire::ReadyReply, &'static str>, ServeError> {
        Ok(Err(broca_wire::NOT_SESSION_COMPACTION_PROVIDER))
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

    /// `session.send` — start or durably queue a run.
    async fn send(&self, req: SendRequest) -> Result<SendResponse, ServeError>;

    /// Install or remove one daemon-scoped durable cap.
    async fn cap_install(&self, _req: CapInstallRequest) -> Result<CapMutation, ServeError> {
        Err(ServeError::CapUnavailable(
            "cap table is not configured".into(),
        ))
    }

    /// Apply one already-priced daemon-scoped spend delta.
    async fn spend_delta(&self, _req: SpendDeltaRequest) -> Result<CapMutation, ServeError> {
        Err(ServeError::CapUnavailable(
            "cap table is not configured".into(),
        ))
    }

    /// The adoption check a stamped `route.bind` gets before it is acknowledged: `Some` when
    /// the bound session was recorded without a scope and its history refuses the stamp. Only
    /// adoption is decided here; every other scope rule is decided on each send, since an
    /// episode can be written between the bind and the send. The default knows no history.
    async fn bind_adoption_refusal(
        &self,
        _session: &LineageKey,
        _principal: Option<&broca_protocol::BindPrincipal>,
        _scope: &broca_protocol::BindScope,
    ) -> Option<broca_core::AdoptionRefusal> {
        None
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

    /// `run.status{run_id}` — the lifecycle state of a SPECIFIC run, via the three-tier
    /// resolution (live actor → durable index terminal → WAL reconciliation). The wire
    /// surface of the durable run index: a consumer reaches a finished/crashed run's outcome
    /// here even when it's not in the consumer's live subscription.
    async fn run_status(
        &self,
        session: &LineageKey,
        run_id: broca_protocol::RunId,
    ) -> Result<RunStatusResponse, ServeError>;

    /// `session.head` — the session's head without message bodies. For retained sessions,
    /// `updated_at` is the durable activity timestamp that sets expiry, not the WAL mtime.
    async fn head(&self, _session: &LineageKey) -> Result<broca_wire::HeadResponse, ServeError> {
        Err(ServeError::Open(
            "session.head is unsupported".into(),
            Some(broca_protocol::ErrorClass::Permanent),
        ))
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
    /// runner parameters to the session's frozen values only then. A store that cannot tell
    /// answers `true`, and admission refuses the send if the session has no episode after all.
    fn session_written(&self, _session: &LineageKey) -> bool {
        true
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
            .filter(|message| !calls_of(&message.message).is_empty())
            .map(|message| message.ordinal)
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
                        call_key: dispatch.intent_seq.map(|seq| format!("{lineage_id}:{seq}")),
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
    fn backup_classes(&self) -> Vec<String> {
        crate::LineageRegistry::backup_classes(self)
    }
    async fn check_migration(&self, session: &LineageKey, write: bool) -> Result<(), ServeError> {
        self.check_move(session, write).map_err(Into::into)
    }
    fn compaction_supported(&self) -> bool {
        crate::LineageRegistry::compaction_supported(self)
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
        let page: broca_wire::ModelPage = page.try_into().map_err(|error| {
            ServeError::Open(
                format!("model page source: {error}"),
                Some(broca_protocol::ErrorClass::Permanent),
            )
        })?;
        let subject = model_group_reply_too_large_subject(&page);
        if let Some(error) =
            reply_frame_limit_refusal(broca_wire::serialized_result_frame_bytes(&page), subject)
        {
            return Err(error);
        }
        Ok(page)
    }

    async fn compaction_ready(
        &self,
        session: &LineageKey,
        caller: &str,
        request: broca_wire::CompactionReady,
    ) -> Result<Result<broca_wire::ReadyReply, &'static str>, ServeError> {
        if request.session != session.session {
            return Err(ServeError::InvalidParams {
                field: "session".into(),
                message: "ready session differs from bound session".into(),
            });
        }
        match self
            .route_compaction_ready(session, caller, request)
            .await?
        {
            crate::actor::CompactionReadyOutcome::Accepted => {
                Ok(Ok(broca_wire::ReadyReply::default()))
            }
            crate::actor::CompactionReadyOutcome::NotProvider => {
                Ok(Err(broca_wire::NOT_SESSION_COMPACTION_PROVIDER))
            }
            crate::actor::CompactionReadyOutcome::Reopen => {
                unreachable!("registry retries close races")
            }
        }
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

    async fn send(&self, req: SendRequest) -> Result<SendResponse, ServeError> {
        use crate::actor::SendOutcome;
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
            .route_submission_with_class(
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
            )
            .await?
        {
            SendOutcome::RetentionInvalid(detail) => Err(ServeError::RetentionInvalid(detail)),
            SendOutcome::SessionClassInvalid(message) => Err(ServeError::InvalidParams {
                field: "session_class".into(),
                message,
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
            SendOutcome::Paused { run_id, reason } => Ok(SendResponse::Paused {
                run_id: run_id.0,
                pause_reason: reason,
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
            SendOutcome::SendIdReuse { differing } => Err(ServeError::SendIdReuse {
                differing: differing.to_owned(),
            }),
            SendOutcome::SteerSettingsConflict { field } => Err(ServeError::InvalidParams {
                field: field.to_owned(),
                message: format!("joining steer cannot change the active run's frozen {field}"),
            }),
            SendOutcome::VariantUnsupported(e) => Err(ServeError::VariantUnsupported(e)),
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
            SendOutcome::BudgetDenied { cap_id, reason } => {
                Err(ServeError::BudgetDenied { cap_id, reason })
            }
            SendOutcome::AccountSelectionRefused(refusal) => {
                Err(ServeError::AccountSelectionRefused(refusal))
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
    ) -> Option<broca_core::AdoptionRefusal> {
        let ownership = self.session_ownership_for_bind(session).await?;
        match broca_core::decide_scope(
            &ownership,
            principal,
            &broca_protocol::SendScope::Stamped(scope.clone()),
        ) {
            Err(broca_core::ScopeRefusal::AdoptionRefused(refusal)) => Some(refusal),
            _ => None,
        }
    }

    async fn cap_install(&self, req: CapInstallRequest) -> Result<CapMutation, ServeError> {
        self.install_cap(req).await.map_err(|error| match error {
            broca_core::CapStoreError::BaselineRequired => ServeError::BaselineRequired,
            broca_core::CapStoreError::Storage(message)
            | broca_core::CapStoreError::Invalid(message) => ServeError::CapUnavailable(message),
        })
    }

    async fn spend_delta(&self, req: SpendDeltaRequest) -> Result<CapMutation, ServeError> {
        self.apply_spend_delta(req)
            .await
            .map_err(|error| match error {
                broca_core::CapStoreError::BaselineRequired => ServeError::BaselineRequired,
                broca_core::CapStoreError::Storage(message)
                | broca_core::CapStoreError::Invalid(message) => {
                    ServeError::CapUnavailable(message)
                }
            })
    }

    async fn import(&self, req: SessionImportRequest) -> Result<SessionImportResponse, ServeError> {
        use crate::actor::ImportOutcome;
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
        cap_page_bytes(
            &mut response,
            cap,
            from_ordinal.is_none(),
            &req.session.session,
        )?;
        Ok(response)
    }

    async fn head(&self, session: &LineageKey) -> Result<broca_wire::HeadResponse, ServeError> {
        // The newest page of one message: the store's last row and the WAL past its watermark,
        // so the cost follows what is not yet materialized, never the session's length.
        let crate::registry::RouteReadPage {
            page,
            newest_record_ms,
            retention_activity_ms,
            last_title,
            ..
        } = self.route_read_page(session, None, Some(1), false).await?;
        // A session never written has no lineage, and then nothing else to report either.
        let Some(lineage_id) = page.lineage_id else {
            return Ok(broca_wire::HeadResponse::default());
        };
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
        Ok(broca_wire::HeadResponse {
            lineage_id: Some(lineage_id.to_string()),
            last_ordinal: page.prompt_len.checked_sub(1),
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
                    reason: page
                        .lineage_state
                        .reason
                        .map(|reason| reason.as_str().to_owned()),
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
        let (state, mut records) = self
            .route_run_state_with_wal(session, run_id.clone())
            .await?;
        let status = run_status_response(state);
        let (mut state, reason, mut error) = match status {
            RunStatusResponse::Unknown => return Err(ServeError::UnknownRun(run_id.0)),
            RunStatusResponse::Active { .. } => ("active".to_owned(), None, None),
            RunStatusResponse::Paused { reason, error, .. } => ("paused".to_owned(), reason, error),
            RunStatusResponse::Interrupted { .. } => ("interrupted".to_owned(), None, None),
            RunStatusResponse::Completed { .. } => ("completed".to_owned(), None, None),
            RunStatusResponse::MaxSteps { .. } => ("max_steps".to_owned(), None, None),
            RunStatusResponse::Cancelled { .. } => ("cancelled".to_owned(), None, None),
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
                state = serde_json::to_value(reason)
                    .ok()
                    .and_then(|value| value.as_str().map(str::to_owned))
                    .unwrap_or(state);
                error = terminal_error.map(Into::into);
            }
            records = Some(read);
        }
        let final_message = if state == "completed" {
            let records = match records {
                Some(records) => records,
                None => self.read_durable_wal(session).await?.0,
            };
            Some(final_message_of(&records, &run_id)?)
        } else {
            None
        };
        let response = broca_wire::RunResultResponse {
            run_id: run_id.0,
            state,
            reason,
            error,
            final_message,
        };
        let subject =
            response
                .final_message
                .as_ref()
                .map(|message| ReplyTooLargeSubject::Message {
                    session: session.session.clone(),
                    ordinal: message.ordinal,
                });
        if let Some(error) = reply_frame_limit_refusal(
            broca_wire::serialized_result_frame_bytes(&response),
            subject,
        ) {
            return Err(error);
        }
        Ok(response)
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
                baseline
            })
    }

    fn session_written(&self, session: &LineageKey) -> bool {
        self.wal_written(session)
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
        let state = self.route_run_state(session, run_id.clone()).await?;
        let mut response = run_status_response(state);
        if let RunStatusResponse::Active {
            awaiting_approval, ..
        } = &mut response
        {
            *awaiting_approval = self.cached_awaiting_approval(session, &run_id);
        }
        Ok(response)
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
                };
                let value = serde_json::to_value(run_status_response(Some(state.clone()))).unwrap();
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
                legacy.auth_selection = None;
                let value = serde_json::to_value(run_status_response(Some(legacy))).unwrap();
                assert!(value.get("auth").is_none());
            }
        }
    }
}
