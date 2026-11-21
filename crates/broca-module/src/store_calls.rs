//! What `session.read` serves about each tool call of a message in a session kept in the
//! session store: the call's key, the module it was dispatched to, and whether it may or may
//! not have run (`indeterminate`). The store determines these facts once:
//!
//! - **A placed turn:** the facts its writer recorded in the batch that placed the turn's
//!   results (`TurnFacts`). Every call of a placed turn has its result, so none is
//!   indeterminate.
//! - **The open turn:** the assistant message whose calls are still running is served from
//!   the operational fold's open batch: a call with a dispatch intent and no result is
//!   indeterminate, whether or not a provider holds it, because an intent and no result mean
//!   it may have run. Its module is the one its intent recorded, found through the session's
//!   id lookup.
//! - **A turn no recorded step produced:** an imported turn's facts name keys another runner
//!   minted (`CallFacts::carrier`), so each call retains its original key and carrier,
//!   without inventing a broca dispatch.
//!
//! A call without an intent never left: it isn't indeterminate and has no module. An
//! answered call (denied, invalid, synthetic, or refused with its batch) has the key its
//! outcome recorded.
//! A tool result delivered as a send's carrier names the carrier call's key (`delivered_as`).

use broca_protocol::content::ContentBlock;
use broca_session_store::backend::SessionBackend;
use broca_session_store::fold::operations::CallState;
use broca_session_store::record::TurnFacts;
use broca_session_store::session::{Session, SessionError, StoredEntry};
use broca_wire::{SessionReadResponse, SessionReadToolCall};

/// What one page message's tool calls are served from.
#[derive(Debug, Clone, PartialEq)]
pub enum TurnOf {
    /// A placed message, with the turn facts its writer recorded, if any.
    Placed(Option<std::sync::Arc<TurnFacts>>),
    /// The assistant message whose calls are still running, served from the open batch. Once
    /// all its results are placed it has its facts, though reads serve it as running until its
    /// batch closes (`store_read::results_await_close`); those name the key of a copy the open
    /// batch can't.
    Open(Option<std::sync::Arc<TurnFacts>>),
}

impl TurnOf {
    pub fn of(entry: &StoredEntry) -> Self {
        if entry.open {
            TurnOf::Open(entry.state.turn.clone())
        } else {
            TurnOf::Placed(entry.state.turn.clone())
        }
    }
}

/// A call with no dispatch fact: never sent, or produced by no recorded step.
fn unattributed(tool_call_id: &str) -> SessionReadToolCall {
    SessionReadToolCall::new(tool_call_id)
}

/// The ids of the tool calls `message` makes, in its order.
fn call_ids(message: &broca_protocol::Message) -> Vec<&str> {
    message
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::ToolCall(call) => Some(call.tool_call_id.0.as_str()),
            _ => None,
        })
        .collect()
}

/// The facts of a placed turn's call `id`, served by `session.read`. The first facts naming
/// the id are the call's.
fn placed_call(turn: Option<&TurnFacts>, id: &str) -> SessionReadToolCall {
    served_call(
        turn.and_then(|turn| turn.calls.iter().find(|call| call.tool_call_id.0 == id)),
        id,
    )
}

/// The facts of the call at `index` among a placed turn's calls `ids`: each copy of a
/// repeated call id is represented by its own facts, using `crate::store_read::facts_by_call`.
fn placed_turn_call(turn: Option<&TurnFacts>, ids: &[String], index: usize) -> SessionReadToolCall {
    let names: Vec<&str> = ids.iter().map(String::as_str).collect();
    let call = turn.and_then(|turn| crate::store_read::facts_by_call(&names, turn)[index]);
    served_call(call, &ids[index])
}

/// A placed call served from its facts, if it has any broca recorded.
fn served_call(
    call: Option<&broca_session_store::record::CallFacts>,
    id: &str,
) -> SessionReadToolCall {
    let mut served = unattributed(id);
    if let Some(call) = call {
        served.call_key = Some(call.call_key.clone());
        served.call_carrier = call.carrier.as_deref().map(str::to_owned);
        served.dispatched_to = call.dispatched_to.as_deref().map(str::to_owned);
    }
    served
}

/// The facts of copy `copy` (counting from 0) of the open turn's call `id`, from the open
/// batch. The fold marks every copy of an id with the intent that anchored its first copy, so
/// only that copy is the intent's: a later copy of the id was never sent. A dispatched call is
/// indeterminate while no copy of its id has a result.
async fn open_call<B: SessionBackend>(
    session: &Session<B>,
    id: &str,
    copy: usize,
) -> Result<SessionReadToolCall, SessionError> {
    let Some(batch) = session
        .operations
        .run
        .as_ref()
        .and_then(|run| run.open_batch.as_ref())
    else {
        return Ok(unattributed(id));
    };
    let positions: Vec<usize> = (0..batch.calls.len())
        .filter(|position| batch.id(*position).0 == id)
        .collect();
    let Some(&position) = positions.get(copy) else {
        return Ok(unattributed(id));
    };
    let answered = positions
        .iter()
        .any(|position| batch.state(*position) == CallState::Terminated);
    let call = &batch.calls[position];
    let dispatched = positions
        .iter()
        .find(|position| batch.calls[**position].intent)
        .is_some_and(|first| *first == position);
    if !dispatched {
        // A copy answered without an intent has the key its outcome recorded, and nothing
        // else; one not answered yet, or a later copy of a dispatched id, has neither.
        let mut served = unattributed(id);
        served.call_key = call.key.clone().filter(|_| !call.intent);
        return Ok(served);
    }
    let Some(key) = call.key.clone() else {
        return Ok(unattributed(id));
    };
    let intent = session
        .find_id(broca_session_store::idindex::IdKind::CallKey, &key)
        .await?
        .into_iter()
        .find_map(|(_, record)| match record {
            broca_session_store::record::Record::ToolIntent(intent) => Some(intent),
            _ => None,
        })
        .ok_or_else(|| {
            SessionError::Fold(format!("no dispatch intent records the call key {key}"))
        })?;
    let mut served = SessionReadToolCall::new(id).with_call_key(key);
    served.dispatched_to = intent.module_id;
    served.indeterminate = !answered;
    Ok(served)
}

/// Fills each message's `tool_calls`, and the carrier call's key of a tool result delivered as a
/// send. `turns` says, per message in order, what its calls are served
/// from ([`crate::store_read::StorePage::turns`]).
pub async fn attribute_tool_calls<B: SessionBackend>(
    session: &Session<B>,
    response: &mut SessionReadResponse,
    turns: &[TurnOf],
) -> Result<(), SessionError> {
    for index in 0..response.messages.len() {
        let message = &response.messages[index];
        let ids: Vec<String> = call_ids(&message.message)
            .into_iter()
            .map(str::to_owned)
            .collect();
        if !ids.is_empty() {
            let mut calls = Vec::with_capacity(ids.len());
            for (position, id) in ids.iter().enumerate() {
                // Which copy of its id the call is, counting from 0.
                let copy = ids[..position]
                    .iter()
                    .filter(|earlier| *earlier == id)
                    .count();
                calls.push(match turns.get(index) {
                    Some(TurnOf::Open(turn)) => {
                        let mut call = open_call(session, id, copy).await?;
                        // A copy answered without an intent whose key the open batch lost to
                        // its id's intent (see `open_call`) takes the key its turn's facts name.
                        if call.call_key.is_none() {
                            let names: Vec<&str> = ids.iter().map(String::as_str).collect();
                            call.call_key = turn
                                .as_deref()
                                .and_then(|turn| {
                                    crate::store_read::facts_by_call(&names, turn)[position]
                                })
                                .filter(|facts| facts.carrier.is_none())
                                .map(|facts| facts.call_key.clone());
                        }
                        call
                    }
                    Some(TurnOf::Placed(turn)) => placed_turn_call(turn.as_deref(), &ids, position),
                    None => unattributed(id),
                });
            }
            response.messages[index].tool_calls = calls;
        }
        // A send delivered as a call's result is the turn's results message; its carrier call
        // is one the assistant message just before it made.
        let message = &response.messages[index];
        let carrier = message
            .run
            .as_ref()
            .and_then(|run| run.delivery.as_ref())
            .filter(|delivery| delivery.mode == broca_protocol::DeliveryMode::ToolResult)
            .and_then(|delivery| delivery.tool_call_id.clone());
        let (Some(carrier), true, Some(assistant)) = (
            carrier,
            message.delivered_as.is_some(),
            message.ordinal.checked_sub(1).filter(|at| *at > 0),
        ) else {
            continue;
        };
        let turn = match index
            .checked_sub(1)
            .filter(|before| response.messages[*before].ordinal == assistant)
        {
            Some(before) => match turns.get(before) {
                Some(TurnOf::Placed(turn)) => turn.clone(),
                _ => None,
            },
            None => session
                .entries(assistant, 1)
                .await?
                .into_iter()
                .find(|entry| entry.ordinal == assistant && !entry.open)
                .and_then(|entry| entry.state.turn),
        };
        let call = placed_call(turn.as_deref(), &carrier);
        if let Some(receipt) = &mut response.messages[index].delivered_as {
            receipt.call_key = call.call_key;
            receipt.call_carrier = call.call_carrier;
        }
    }
    Ok(())
}
