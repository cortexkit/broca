//! Reads of a session kept in the new session storage.
//!
//! A read loads the session read-only (`Session::view`): it never takes the lease or writes, a
//! torn tail reads as not yet written, and a log that needs a recovery refuses until a writer
//! repairs it. Each reply is built from what the session decided when it placed each message
//! (`EntryFacts`) and from its operational fold, so a read costs a load and the pages it serves,
//! never a replay. A reply here equals the reply today's reader builds from a WAL for the same
//! history, apart from the differences the stage-6 design names: no WAL lineage, and cursors
//! on the store's own record seqs.

use broca_core::{
    RunAttribution, SessionReadLineageState, SessionReadMessage, SessionReadPage,
    SessionReadReason, SessionReadStateKind,
};
use broca_protocol::content::Role;
use broca_session_store::backend::SessionBackend;
use broca_session_store::fold::operations::Run;
use broca_session_store::session::{Session, SessionError, StoredEntry};

/// Why a store read refused.
#[derive(Debug)]
pub enum StoreReadError {
    /// The session couldn't be read: missing, damaged, or a backend failure.
    Session(SessionError),
    /// A message has no entry facts, so the page can't say what today's read says about it.
    /// Only a session converted from today's storage without them can hold one.
    NoFacts { ordinal: u64 },
    /// A message's entry facts name a value no build of this format writes.
    BadFact { ordinal: u64, field: &'static str },
}

impl From<SessionError> for StoreReadError {
    fn from(error: SessionError) -> Self {
        StoreReadError::Session(error)
    }
}

/// How many messages the session's transcript holds: the placed ones, and the assistant
/// message whose tool calls are still running, at its reserved ordinal.
fn transcript_len<B: SessionBackend>(session: &Session<B>) -> u64 {
    session.conversation.placed + u64::from(session.conversation.open_tool_turn().is_some())
}

/// A `session.read` page: up to `limit` messages from `from_ordinal` (counting from 0, as
/// today's page builder takes it), or the newest `limit` without one. Its dispatch facts, head
/// cursor and lineage id are left to the caller.
pub async fn read_page<B: SessionBackend>(
    session: &Session<B>,
    from_ordinal: Option<u64>,
    limit: Option<u64>,
) -> Result<SessionReadPage, StoreReadError> {
    let capped_limit = limit
        .unwrap_or(broca_core::SESSION_READ_DEFAULT_LIMIT)
        .clamp(1, broca_core::SESSION_READ_MAX_LIMIT);
    let prompt_len = transcript_len(session);
    let start = match from_ordinal {
        Some(from) => from.min(prompt_len),
        None => prompt_len.saturating_sub(capped_limit),
    };
    let end = start.saturating_add(capped_limit).min(prompt_len);
    // One entry past the page, to tell whether the page's last message ends its run.
    let wanted = usize::try_from(end - start)
        .unwrap_or(usize::MAX)
        .saturating_add(1);
    let mut entries = session.entries(start + 1, wanted).await?;
    let after = (entries.len() as u64 > end - start)
        .then(|| entries.pop())
        .flatten();
    let latest = session.operations.run.as_ref();
    let mut messages = Vec::with_capacity(entries.len());
    for (index, entry) in entries.iter().enumerate() {
        let next = entries.get(index + 1).or(after.as_ref());
        messages.push(page_message(entry, next, latest)?);
    }
    let next_from_ordinal = (end < prompt_len).then_some(end);
    Ok(SessionReadPage {
        messages,
        next_from_ordinal,
        head: None,
        lineage_id: None,
        lineage_state: lineage_state(session),
        prompt_len,
        deferred_calls: None,
        late_approvals: Vec::new(),
        withdrawals_owed: false,
        tags_await_setup: false,
    })
}

/// One page message from its entry. `next` is the entry after it, if any; `latest` is the
/// session's latest run.
fn page_message(
    entry: &StoredEntry,
    next: Option<&StoredEntry>,
    latest: Option<&Run>,
) -> Result<SessionReadMessage, StoreReadError> {
    let facts = entry.state.facts.as_ref().ok_or(StoreReadError::NoFacts {
        ordinal: entry.ordinal,
    })?;
    let ordinal = entry.ordinal - 1;
    let mut message = entry.message.clone();
    message.ordinal = Some(ordinal);
    let completed_at = broca_core::compaction_v2::entries::project_completion(
        &message,
        entry.next.as_ref(),
        facts
            .written_at_ms
            .filter(|_| facts.native && message.role == Role::Assistant),
    );
    let delivery = delivery(facts)?;
    let run = facts.run_id.as_ref().map(|run_id| {
        let next_run = next.and_then(|next| next.state.facts.as_ref()?.run_id.as_ref());
        // A run's messages are contiguous, so its last one is the one the next message doesn't
        // share. It is final once the run has ended: every run before the latest one has, since
        // a run starts only after the one before it ends.
        let ended = match latest {
            Some(run) if &run.run_id == run_id => run.finished.is_some(),
            _ => true,
        };
        RunAttribution {
            run_id: run_id.clone(),
            episode: run_id.clone(),
            is_final: next_run != Some(run_id) && ended,
            delivery: delivery.clone(),
        }
    });
    Ok(SessionReadMessage {
        created_at: facts.created_at_ms,
        written_at: facts.written_at_ms,
        completed_at,
        ordinal,
        message,
        run,
    })
}

/// How a delivered send reached the message, from its entry facts.
fn delivery(
    facts: &broca_session_store::record::EntryFacts,
) -> Result<Option<broca_protocol::tags::DeliveryAttribution>, StoreReadError> {
    let Some(mode) = facts.delivery_mode.clone() else {
        return Ok(None);
    };
    let mode = serde_json::from_value(serde_json::Value::String(mode)).map_err(|_| {
        StoreReadError::BadFact {
            ordinal: facts.ordinal,
            field: "delivery_mode",
        }
    })?;
    Ok(Some(broca_protocol::tags::DeliveryAttribution {
        mode,
        tool_call_id: facts.delivery_tool_call_id.clone(),
    }))
}

/// The session's lifecycle beside a page: its latest run's state, and for an ended or paused
/// run the error and usage its end or pause recorded, which the operational fold keeps.
fn lineage_state<B: SessionBackend>(session: &Session<B>) -> SessionReadLineageState {
    let Some(run) = session.operations.run.as_ref() else {
        return SessionReadLineageState {
            last_run_id: None,
            state: SessionReadStateKind::Idle,
            reason: None,
            error: None,
            usage: None,
        };
    };
    let last_run_id = Some(broca_protocol::RunId(run.run_id.clone()));
    let error = || {
        run.end_error
            .clone()
            .and_then(|error| serde_json::from_value(error).ok())
    };
    match (run.finished, run.paused) {
        (Some(reason), _) => SessionReadLineageState {
            last_run_id,
            state: broca_core::state_for_terminal(reason),
            reason: Some(SessionReadReason::Terminal(reason)),
            error: error(),
            usage: run.end_usage.filter(|_| run.completed_steps > 0),
        },
        (None, Some(reason)) => SessionReadLineageState {
            last_run_id,
            state: SessionReadStateKind::Paused,
            reason: Some(SessionReadReason::Pause(reason)),
            error: error(),
            usage: None,
        },
        (None, None) => SessionReadLineageState {
            last_run_id,
            state: SessionReadStateKind::Active,
            reason: None,
            error: None,
            usage: None,
        },
    }
}
