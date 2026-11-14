//! `run.tool_calls` for a session kept in the new storage: the run's calls in the order its
//! steps made them, each with its outcome, key, dispatch module and result size, paged as
//! today's storage pages them (`serve::run_tool_calls_answer`).
//!
//! Today's answer walks the run's WAL records (`serve::indexed_run_tool_calls`). This walks the
//! store's records of the run with the same rules, record for record:
//!
//! - **A step's calls:** the assistant message's tool calls, at the ordinal the message's entry
//!   facts give it. A new step drops calls of an earlier one that never got a batch.
//! - **A dispatch intent:** the first copy of the call's id with no intent and no outcome takes
//!   the intent's arguments (the call's own input when the intent stored none), its module and
//!   its key.
//! - **An outcome:** a call never dispatched (`NotDispatched`, today's `tool_no_dispatch`) is
//!   refused, and carries no result size. Any other outcome is read with its reply as today's
//!   `tool_result` is (`serve::run_tool_call_outcome`), and carries the size of the stored
//!   result. A call with no key yet takes the key the outcome recorded.
//! - **The batch's end:** every call of the batch is listed, its results at the next ordinal. A
//!   call with an intent and no outcome is indeterminate; one with neither isn't listed.
//! - **A run still running:** the open batch's calls are listed as they stand, with no result
//!   ordinal.
//!
//! The keys are the ones the store recorded: a moved session keeps today's keys, and a session
//! the new storage started has its own. A cursor carries the store's seqs and names the
//! session's log incarnation, so a cursor today's storage issued, or one from a session that was
//! created again, is refused naming `after`.

use broca_protocol::content::ContentBlock;
use broca_session_store::backend::SessionBackend;
use broca_session_store::idindex::IdKind;
use broca_session_store::record::{CallOutcome, Record, ToolReply};
use broca_session_store::session::{Session, SessionError};
use cortexkit_role_llm_runner::tool_calls::{
    outcomes, RunToolCall, ToolCallsPage, ToolCallsRequest,
};

use crate::serve::{
    checked_tool_calls_cursor, invalid_tool_call_cursor, run_tool_call_outcome,
    run_tool_calls_answer, tool_calls_limit, IndexedRunToolCall, RunToolCallsPageContext,
    ServeError,
};

/// The `run.tool_calls` page `request` asks of `session`.
pub async fn run_tool_calls<B: SessionBackend>(
    session: &Session<B>,
    request: &ToolCallsRequest,
) -> Result<ToolCallsPage, ServeError> {
    let run_id = request.run_id.as_str();
    let limit = tool_calls_limit(request);
    let incarnation = broca_session_store::codec::hex(&session.head().incarnation.0);
    let open_ordinal = session
        .conversation
        .open_entry()
        .map(|(ordinal, _, _)| ordinal);
    match request.after.as_deref() {
        None => {
            let Some(records) = run_records(session, run_id)
                .await
                .map_err(crate::store_read::read_failed)?
            else {
                return Err(ServeError::UnknownRun(run_id.to_owned()));
            };
            let (Some((run_start_seq, _)), Some((run_end_seq, _))) =
                (records.first(), records.last())
            else {
                return Err(ServeError::UnknownRun(run_id.to_owned()));
            };
            let entries = walk(&records, run_id, false, open_ordinal)?;
            run_tool_calls_answer(
                entries,
                limit,
                &RunToolCallsPageContext {
                    run_id,
                    lineage_id: Some(&incarnation),
                    run_start_seq: *run_start_seq,
                    run_end_seq: *run_end_seq,
                    range: None,
                    range_start_seq: 0,
                    fallback: None,
                    seed: None,
                },
            )
        }
        Some(token) => {
            let cursor = checked_tool_calls_cursor(token, run_id)?;
            if cursor.lineage_id.as_deref() != Some(incarnation.as_str()) {
                return Err(invalid_tool_call_cursor(
                    "run.tool_calls.after belongs to another session log",
                ));
            }
            // The cursor's batch, from the record holding its step's assistant message.
            let records = session
                .records_between(cursor.step_seq, cursor.batch_end_seq)
                .await
                .map_err(crate::store_read::read_failed)?;
            let entries = walk(&records, run_id, true, open_ordinal)?;
            run_tool_calls_answer(
                entries,
                limit,
                &RunToolCallsPageContext {
                    run_id,
                    lineage_id: cursor.lineage_id.as_deref(),
                    run_start_seq: cursor.run_start_seq,
                    run_end_seq: cursor.run_end_seq,
                    range: None,
                    range_start_seq: cursor.step_seq,
                    fallback: None,
                    seed: Some(&cursor),
                },
            )
        }
    }
}

/// The records of the run `run_id`, from its start through its end, or through the session's
/// last record while it runs. `None` when no run of the session has the id.
async fn run_records<B: SessionBackend>(
    session: &Session<B>,
    run_id: &str,
) -> Result<Option<Vec<(u64, Record)>>, SessionError> {
    let latest = session
        .operations
        .run
        .as_ref()
        .is_some_and(|run| run.run_id == run_id);
    // The latest run's start, when the session keeps it in memory, is the one the id index
    // would find: a run id names one run start.
    let kept_start = latest.then(|| session.kept_run_start(run_id)).flatten();
    let named = match kept_start {
        Some(_) => Vec::new(),
        None => session.find_id(IdKind::RunId, run_id).await?,
    };
    let Some(first) = kept_start.or_else(|| {
        named
            .iter()
            .find(|(_, record)| matches!(record, Record::RunStart(_)))
            .map(|(seq, _)| *seq)
    }) else {
        return Ok(None);
    };
    let end = named
        .iter()
        .filter(|(_, record)| matches!(record, Record::RunEnd(_)))
        .map(|(seq, _)| *seq)
        .max();
    let mut records = match end {
        Some(end) if !latest => session.records_between(first, end).await?,
        _ => session.records_from(first).await?,
    };
    // A run with no end recorded runs through the session's last record, or up to the next
    // run's start.
    if let Some(next) = records
        .iter()
        .skip(1)
        .position(|(_, record)| matches!(record, Record::RunStart(_)))
    {
        records.truncate(next + 1);
    }
    if let Some(end) = records
        .iter()
        .position(|(_, record)| matches!(record, Record::RunEnd(end) if end.run_id == run_id))
    {
        records.truncate(end + 1);
    }
    Ok(Some(records))
}

/// A call of the step being walked, and what its batch has recorded for it so far.
struct Builder {
    call: RunToolCall,
    has_intent: bool,
    terminal: bool,
}

/// The run's listed calls, from `records`: the run's records from its start, or, `seeded`, the
/// records of one of its batches from its step's assistant message on. `open_ordinal` is the
/// ordinal of the assistant message whose calls are still running, for a message whose entry
/// facts aren't among `records`.
fn walk(
    records: &[(u64, Record)],
    run_id: &str,
    seeded: bool,
    open_ordinal: Option<u64>,
) -> Result<Vec<IndexedRunToolCall>, ServeError> {
    let damaged = |reason: &str| {
        ServeError::Open(
            format!("run.tool_calls can't read the run {run_id}: {reason}"),
            Some(broca_protocol::ErrorClass::Permanent),
        )
    };
    let mut entries = Vec::new();
    let mut current = seeded;
    let mut active: Vec<Builder> = Vec::new();
    let mut step_seq = 0u64;
    let mut ordinal: Option<u64> = None;
    let mut awaiting_facts = false;
    let mut batch: Option<String> = None;
    let mut reply: Option<&ToolReply> = None;

    let finish = |active: &mut Vec<Builder>,
                  step_seq: u64,
                  batch_end_seq: u64,
                  ordinal: Option<u64>,
                  entries: &mut Vec<IndexedRunToolCall>|
     -> Result<(), ServeError> {
        if active.is_empty() {
            return Ok(());
        }
        let ordinal = ordinal
            .or(open_ordinal)
            .ok_or_else(|| damaged("a step's assistant message has no ordinal"))?;
        for (call_index, builder) in active.iter_mut().enumerate() {
            if !builder.terminal && builder.has_intent {
                builder.call.outcome = outcomes::INDETERMINATE.into();
            }
            if !builder.call.outcome.is_empty() {
                let mut call = builder.call.clone();
                call.ordinal = ordinal;
                if let Some(result) = call.result_ordinal.as_mut() {
                    *result = ordinal + 1;
                }
                entries.push(IndexedRunToolCall {
                    call,
                    step_seq,
                    call_index,
                    batch_end_seq,
                    ordinal: ordinal.saturating_sub(1),
                    after_batch: Vec::new(),
                });
            }
        }
        Ok(())
    };

    for (seq, record) in records {
        match record {
            Record::RunStart(start) => {
                current = start.run_id == run_id;
                active.clear();
                batch = None;
            }
            Record::Assistant(assistant) => {
                active.clear();
                batch = None;
                step_seq = *seq;
                ordinal = None;
                awaiting_facts = true;
                if current {
                    let calls = assistant
                        .message
                        .content
                        .iter()
                        .filter_map(|block| match block {
                            ContentBlock::ToolCall(call) => Some(call),
                            _ => None,
                        });
                    active.extend(calls.enumerate().map(|(position, call)| {
                        Builder {
                            call: RunToolCall::new(
                                call.tool_call_id.0.clone(),
                                call.tool_name.clone(),
                                call.input.clone().parse(),
                                "",
                                0,
                            )
                            .with_position(position as u32),
                            has_intent: false,
                            terminal: false,
                        }
                    }));
                }
            }
            Record::EntryFacts(facts) if awaiting_facts => {
                ordinal = Some(facts.ordinal);
                awaiting_facts = false;
            }
            Record::TurnFacts(turn) if ordinal.is_none() => ordinal = Some(turn.ordinal),
            Record::BatchStart(start) => batch = Some(start.batch_id.clone()),
            Record::ToolIntent(intent) if batch.as_deref() == Some(intent.batch_id.as_str()) => {
                if let Some(call) = active.iter_mut().find(|call| {
                    call.call.tool_call_id == intent.tool_call_id.0
                        && !call.has_intent
                        && !call.terminal
                }) {
                    call.has_intent = true;
                    if let Some(args) = &intent.args {
                        call.call.args = args.clone().parse();
                    }
                    call.call.dispatched_to = intent.module_id.clone();
                    call.call.call_key = intent.call_key.clone();
                }
            }
            Record::ToolReply(replied) => reply = Some(replied),
            Record::ToolOutcome(outcome) if batch.as_deref() == Some(outcome.batch_id.as_str()) => {
                let replied = reply
                    .take()
                    .filter(|replied| replied.result.tool_call_id == outcome.tool_call_id)
                    .ok_or_else(|| damaged("a tool outcome has no reply"))?;
                let id = outcome.tool_call_id.0.as_str();
                if outcome.outcome == CallOutcome::NotDispatched {
                    let at = active
                        .iter()
                        .position(|call| {
                            call.call.tool_call_id == id
                                && !call.terminal
                                && !call.has_intent
                                && call.call.call_key.is_none()
                        })
                        .or_else(|| {
                            active
                                .iter()
                                .position(|call| call.call.tool_call_id == id && !call.terminal)
                        });
                    if let Some(call) = at.and_then(|at| active.get_mut(at)) {
                        if call.call.call_key.is_none() {
                            call.call.call_key = outcome.call_key.clone();
                        }
                        call.call.outcome = outcomes::REFUSED.into();
                        call.terminal = true;
                    }
                } else if let Some(call) = active
                    .iter_mut()
                    .find(|call| call.call.tool_call_id == id && !call.terminal)
                {
                    use broca_wal::record::DispatchOutcome;
                    if call.call.call_key.is_none() {
                        call.call.call_key = outcome.call_key.clone();
                    }
                    let dispatch = match outcome.outcome {
                        CallOutcome::Completed => DispatchOutcome::Completed,
                        CallOutcome::Unknown => DispatchOutcome::Unknown,
                        CallOutcome::Interrupted => DispatchOutcome::Interrupted,
                        CallOutcome::MaxSteps => DispatchOutcome::MaxSteps,
                        CallOutcome::NotDispatched => unreachable!("matched above"),
                    };
                    call.call.outcome = run_tool_call_outcome(
                        dispatch,
                        call.has_intent,
                        outcome.reason.as_deref(),
                        replied.result.is_error,
                    )
                    .into();
                    call.call.result_bytes = Some(
                        serde_json::to_vec(&replied.result)
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
            Record::BatchEnd(end) if batch.as_deref() == Some(end.batch_id.as_str()) => {
                if current {
                    for builder in &mut active {
                        // The results' ordinal follows the message's, set when it is listed.
                        builder.call.result_ordinal = Some(0);
                    }
                    finish(&mut active, step_seq, *seq, ordinal, &mut entries)?;
                }
                active.clear();
                batch = None;
            }
            Record::RunEnd(end) => {
                if end.run_id == run_id && current {
                    break;
                }
                current = false;
            }
            _ => {}
        }
    }
    if current && !active.is_empty() {
        let last = records.last().map_or(step_seq, |(seq, _)| *seq);
        finish(&mut active, step_seq, last, ordinal, &mut entries)?;
    }
    Ok(entries)
}
