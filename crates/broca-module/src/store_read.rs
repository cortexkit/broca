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

/// A `session.read` page, and each of its messages' stable id (`EntryFacts::mid`), in order.
pub struct StorePage {
    pub page: SessionReadPage,
    pub mids: Vec<String>,
}

/// A `session.read` page: up to `limit` messages from `from_ordinal` (counting from 0, as
/// today's page builder takes it), or the newest `limit` without one. Its dispatch facts, head
/// cursor and lineage id are left to the caller.
pub async fn read_page<B: SessionBackend>(
    session: &Session<B>,
    from_ordinal: Option<u64>,
    limit: Option<u64>,
) -> Result<StorePage, StoreReadError> {
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
    let mut mids = Vec::with_capacity(entries.len());
    for (index, entry) in entries.iter().enumerate() {
        let next = entries.get(index + 1).or(after.as_ref());
        messages.push(page_message(entry, next, latest)?);
        // `page_message` has refused an entry without facts.
        mids.push(
            entry
                .state
                .facts
                .as_ref()
                .map(|facts| facts.mid.clone())
                .unwrap_or_default(),
        );
    }
    let next_from_ordinal = (end < prompt_len).then_some(end);
    let page = SessionReadPage {
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
    };
    Ok(StorePage { page, mids })
}

/// Serves `session.read` for a session kept in the new storage. The reply is today's for the
/// same history, built from the session's head, its folds and its entry facts, never from a WAL.
///
/// Until the store keeps per-call facts, a read that opts in to display facts, tools or Rich UI
/// stamps is refused by name rather than answered without them, as is a late-approval cursor:
/// the new storage takes no approval questions, so no cursor names one.
pub async fn serve_read(
    store: &crate::store_open::StoreRuntime,
    req: crate::serve::SessionReadRequest,
) -> Result<broca_wire::SessionReadResponse, crate::serve::ServeError> {
    use crate::serve::ServeError;
    if let Some(cursor) = req.late_after.as_deref() {
        let refusal = broca_wire::InvalidLateCursor::new(cursor);
        return Err(ServeError::InvalidParams {
            field: refusal.detail.field,
            message: refusal.message,
        });
    }
    for (asked, field) in [
        (req.include_display_facts, "include_display_facts"),
        (req.include_tools, "include_tools"),
        (req.include_rich_ui, "include_rich_ui"),
    ] {
        if asked {
            return Err(ServeError::InvalidParams {
                field: field.into(),
                message: format!("{field} isn't served yet for a session kept in the new storage"),
            });
        }
    }
    let Some(session) = store.view(&req.session).await? else {
        // The session's creation never committed, so it holds nothing: an empty transcript.
        let mut response = crate::serve::session_read_response(empty_page(), None);
        response.incarnation = "unwritten".into();
        return Ok(response);
    };
    let head = session.head();
    if req
        .history_version
        .is_some_and(|requested| requested != head.history_version)
    {
        return Err(ServeError::HistoryChanged);
    }
    let from_ordinal = match &req.after_mid {
        Some(mid) => match session.find_message(mid).await.map_err(read_failed)? {
            // The 1-based ordinal of the message named, which is the 0-based start after it.
            Some(ordinal) => Some(ordinal),
            None => return Err(ServeError::UnknownMid(mid.clone())),
        },
        None => req.from_ordinal.map(|at| at.saturating_sub(1)),
    };
    let StorePage { mut page, mids } =
        read_page(&session, from_ordinal, req.limit)
            .await
            .map_err(|error| match error {
                StoreReadError::Session(error) => read_failed(error),
                other => ServeError::Open(
                    format!("the session's stored facts can't be read: {other:?}"),
                    Some(broca_protocol::ErrorClass::Permanent),
                ),
            })?;
    crate::store_entries::mark_reply_cleaning(&session, store.parts(), &mut page.messages)
        .await
        .map_err(|error| {
            // A backend failure and a damaged start read alike here, so no class is claimed.
            ServeError::Open(
                format!("the session's run settings can't be read: {error}"),
                None,
            )
        })?;
    let mut response = crate::serve::session_read_response(page, None);
    response.history_version = head.history_version;
    response.incarnation = broca_session_store::codec::hex(&head.incarnation.0);
    for (entry, mid) in response.messages.iter_mut().zip(mids) {
        entry.mid = mid;
    }
    crate::serve::apply_media_mode(&mut response, req.media, &req.session.session)?;
    let cap = req
        .max_bytes
        .unwrap_or(broca_wire::READ_DEFAULT_MAX_BYTES)
        .min(broca_wire::READ_MAXIMUM_MAX_BYTES);
    crate::serve::cap_page_bytes(
        &mut response,
        cap,
        from_ordinal.is_none(),
        &req.session.session,
    )?;
    Ok(response)
}

/// The page of a session that holds nothing.
fn empty_page() -> SessionReadPage {
    SessionReadPage {
        messages: Vec::new(),
        next_from_ordinal: None,
        head: None,
        lineage_id: None,
        lineage_state: SessionReadLineageState {
            last_run_id: None,
            state: SessionReadStateKind::Idle,
            reason: None,
            error: None,
            usage: None,
        },
        prompt_len: 0,
        deferred_calls: None,
        late_approvals: Vec::new(),
        withdrawals_owed: false,
        tags_await_setup: false,
    }
}

/// A store read that failed. Only a backend failure is worth a retry: anything else is the
/// session's stored bytes or state being wrong, which reading again won't change.
pub(crate) fn read_failed(error: SessionError) -> crate::serve::ServeError {
    let class = match error {
        SessionError::Backend(_) => broca_protocol::ErrorClass::Transient,
        _ => broca_protocol::ErrorClass::Permanent,
    };
    crate::serve::ServeError::Open(
        format!("the session could not be read from the new storage: {error:?}"),
        Some(class),
    )
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

/// The session's usage by model, as `session.head` serves it, from the fold that sums each
/// step's model shares. `None` until a step counted.
pub fn session_usage(
    fold: Option<&broca_session_store::fold::operations::SessionUsage>,
) -> Option<cortexkit_role_llm_runner::usage::SessionUsage> {
    use broca_session_store::fold::operations::UsageTotals;
    use cortexkit_role_llm_runner::usage::{ModelUsage, RunUsage, SessionUsage};
    let fold = fold?;
    fn counts(totals: &UsageTotals) -> [Option<u64>; 5] {
        [
            totals.input_tokens,
            totals.cached_input_tokens,
            totals.cache_write_tokens,
            totals.output_tokens,
            totals.reasoning_tokens,
        ]
    }
    let mut run = RunUsage::new();
    run.steps = fold.totals.steps;
    run.unreported_steps = fold.totals.unreported_steps;
    [
        run.input_tokens,
        run.cached_input_tokens,
        run.cache_write_tokens,
        run.output_tokens,
        run.reasoning_tokens,
    ] = counts(&fold.totals);
    run.by_model = fold
        .by_model
        .iter()
        .map(|entry| {
            let mut model = ModelUsage::new(&entry.provider, &entry.model);
            if let Some(variant) = &entry.variant {
                model = model.with_variant(variant);
            }
            model.steps = entry.totals.steps;
            model.unreported_steps = entry.totals.unreported_steps;
            [
                model.input_tokens,
                model.cached_input_tokens,
                model.cache_write_tokens,
                model.output_tokens,
                model.reasoning_tokens,
            ] = counts(&entry.totals);
            model
        })
        .collect();
    let mut usage = SessionUsage::new(&fold.since);
    usage.totals = run;
    Some(usage)
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

/// What `session.read` serves about a message besides its page entry, from the facts the
/// session recorded when it placed the message, as today's read serves them for broca's own
/// sends: the send's id, sender and marks for a user message, each tool call's key, and the
/// text delivered with the message's tool calls.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DisplayFacts {
    /// For a user message: the `client_message_id` of the send that placed it.
    pub client_message_id: Option<String>,
    /// For a user message: the attested module id of the send's sender.
    pub sender: Option<String>,
    /// The send's `speaks_for` mark, as stored.
    pub speaks_for: Option<serde_json::Value>,
    /// The send's positive notice mark.
    pub notice: bool,
    /// One per tool call the message made, in call order.
    pub calls: Vec<CallDisplay>,
    /// Text delivered with the message's tool calls, in the order it was delivered.
    pub notices: Vec<NoticeDisplay>,
}

/// A tool call's identity, as reads serve it.
#[derive(Debug, Clone, PartialEq)]
pub struct CallDisplay {
    pub tool_call_id: String,
    pub call_key: String,
    /// The runner that minted `call_key`, when it wasn't broca: a key is unique only within the
    /// runner that minted it.
    pub call_carrier: Option<String>,
    pub schema_pin: Option<serde_json::Value>,
}

/// Text delivered with one of a message's tool calls.
#[derive(Debug, Clone, PartialEq)]
pub struct NoticeDisplay {
    /// The tool call's position among the message's content blocks.
    pub part: u32,
    pub call_key: Option<String>,
    pub send_id: Option<String>,
    pub sender: Option<String>,
    pub text: String,
}

/// The display facts of `entry`.
pub fn display_facts(entry: &StoredEntry) -> DisplayFacts {
    let user = entry.message.role == Role::User;
    let facts = entry.state.facts.as_ref();
    let marks = facts.and_then(|facts| facts.marks.as_ref());
    let calls: Vec<CallDisplay> = entry
        .state
        .turn
        .iter()
        .flat_map(|turn| &turn.calls)
        .map(|call| CallDisplay {
            tool_call_id: call.tool_call_id.0.clone(),
            call_key: call.call_key.clone(),
            call_carrier: call.carrier.clone(),
            schema_pin: call.schema_pin.clone(),
        })
        .collect();
    let key_at = |part: u32| {
        let block = entry.message.content.get(part as usize)?;
        let broca_protocol::ContentBlock::ToolCall(call) = block else {
            return None;
        };
        calls
            .iter()
            .find(|known| known.tool_call_id == call.tool_call_id.0)
            .map(|known| known.call_key.clone())
    };
    let notices = entry
        .state
        .delivered
        .iter()
        .map(|delivered| NoticeDisplay {
            part: delivered.part,
            call_key: key_at(delivered.part),
            send_id: delivered.send_id.clone(),
            sender: delivered
                .notice
                .as_ref()
                .map(|notice| notice.sender.clone()),
            text: delivered.text.clone(),
        })
        .collect();
    DisplayFacts {
        client_message_id: facts
            .and_then(|facts| facts.client_message_id.clone())
            .filter(|_| user),
        sender: facts
            .and_then(|facts| facts.sender.clone())
            .filter(|_| user),
        speaks_for: marks.and_then(|marks| marks.get("speaks_for").cloned()),
        notice: marks.and_then(|marks| marks.get("notice")) == Some(&serde_json::Value::Bool(true)),
        calls,
        notices,
    }
}
