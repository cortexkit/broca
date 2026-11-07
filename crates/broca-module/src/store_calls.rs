//! What `session.read` serves about each tool call of a message, for a session kept in the new
//! storage: the call's key, the module it was dispatched to, and whether it may or may not have
//! run (`indeterminate`). Today's read works these out from the WAL records after the step that
//! made the calls (`broca_core::context::dispatches_of_turns`); the store decides them once:
//!
//! - **A placed turn:** the facts its writer recorded in the batch that placed the turn's results
//!   (`TurnFacts`). Every call of a placed turn has its result, so none is indeterminate.
//! - **The open turn:** the assistant message whose calls are still running is served from the
//!   operational fold's open batch: a call with a dispatch intent and no result is indeterminate,
//!   whether or not a provider holds it, as today's rule reads (an intent and no result). Its
//!   module is the one its intent recorded, found through the session's id lookup.
//! - **A turn no recorded step produced:** an imported turn's facts name keys another runner
//!   minted (`CallFacts::carrier`), so, as today's read does for a message no step of its log
//!   produced, each call is served with no dispatch fact.
//!
//! A call without an intent never left: it has no key and no module, and isn't indeterminate.
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
    Placed(Option<TurnFacts>),
    /// The assistant message whose calls are still running.
    Open,
}

impl TurnOf {
    pub fn of(entry: &StoredEntry) -> Self {
        if entry.open {
            TurnOf::Open
        } else {
            TurnOf::Placed(entry.state.turn.clone())
        }
    }
}

/// A call with no dispatch fact: never sent, or produced by no recorded step.
fn unattributed(tool_call_id: &str) -> SessionReadToolCall {
    SessionReadToolCall {
        tool_call_id: tool_call_id.to_owned(),
        call_key: None,
        dispatched_to: None,
        indeterminate: false,
    }
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

/// The facts of a placed turn's call `id`, served as today's read serves them. The first facts
/// naming the id are the call's: a model may repeat an id, and today's read gives every copy the
/// first intent's facts.
fn placed_call(turn: Option<&TurnFacts>, id: &str) -> SessionReadToolCall {
    match turn.and_then(|turn| turn.calls.iter().find(|call| call.tool_call_id.0 == id)) {
        Some(call) if call.carrier.is_none() => SessionReadToolCall {
            tool_call_id: id.to_owned(),
            call_key: Some(call.call_key.clone()),
            dispatched_to: call.dispatched_to.clone(),
            indeterminate: false,
        },
        _ => unattributed(id),
    }
}

/// The facts of the open turn's call `id`, from the open batch. A call is indeterminate when it
/// has an intent and no copy of its id has a result yet.
async fn open_call<B: SessionBackend>(
    session: &Session<B>,
    id: &str,
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
    let Some(key) = positions
        .iter()
        .find_map(|position| batch.key(*position))
        .map(str::to_owned)
    else {
        return Ok(unattributed(id));
    };
    let answered = positions
        .iter()
        .any(|position| batch.state(*position) == CallState::Terminated);
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
    Ok(SessionReadToolCall {
        tool_call_id: id.to_owned(),
        call_key: Some(key),
        dispatched_to: intent.module_id,
        indeterminate: !answered,
    })
}

/// Fills each message's `tool_calls`, and the carrier call's key of a tool result delivered as a
/// send, as today's read does. `turns` says, per message in order, what its calls are served
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
            for id in &ids {
                calls.push(match turns.get(index) {
                    Some(TurnOf::Open) => open_call(session, id).await?,
                    Some(TurnOf::Placed(turn)) => placed_call(turn.as_ref(), id),
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
        let key = placed_call(turn.as_ref(), &carrier).call_key;
        if let Some(receipt) = &mut response.messages[index].delivered_as {
            receipt.call_key = key;
        }
    }
    Ok(())
}
