//! `session.read {tag, call_key}` (llm-runner `tag_read`) for a session kept in the session store:
//! the message part the model saw as part number `tag` in the request that produced the tool
//! call `call_key`.
//!
//! The call fixes which request is meant. Its dispatch intent (found through the id index by its
//! call key) names its run and batch, and the batch's start recorded the part numbers the request
//! whose answer planned the batch showed (`BatchStart::shown_tags`). A number that request didn't
//! show is refused, even when the session numbers a part with it: the model can't have read that
//! part in that request.
//!
//! The number itself never names another part: numbers are given once per session from each
//! message's recorded base and never reused, including after an undo, so the session's own
//! numbering (`Session::compaction_tagged_entry`, projected as `session.entries`
//! projects it) finds the part the request showed under it.
//!
//! The read runs over a read-only view of the session as stored, as `session.tag_map` does: it
//! never takes the session's lease, and an open actor only makes its records durable first.

use broca_session_store::backend::SessionBackend;
use broca_session_store::idindex::IdKind;
use broca_session_store::record::{Record, TagRange, ToolIntent};
use broca_session_store::session::Session;
use cortexkit_role_llm_runner::errors::{UNKNOWN_CALL, UNKNOWN_TAG};

use crate::serve::ServeError;

/// Where a citation resolves: the stored ordinal of the message holding the part, and the part's
/// zero-based index among the message's stored blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Resolved {
    pub ordinal: u64,
    pub part: u32,
}

/// Resolves part number `tag` in the request that produced call `call_key` of `session`.
pub async fn resolve<B: SessionBackend>(
    session: &Session<B>,
    tag: u64,
    call_key: &str,
) -> Result<Resolved, ServeError> {
    let (intent_seq, intent) = intent_of(session, call_key).await?;
    let (shown, call_mid) = request_of(session, intent_seq, &intent).await?;
    // An undo that removed the call's message removed the request's answer with it: the call
    // is no longer part of the session's history.
    let present = session
        .entries_by_mid(std::slice::from_ref(&call_mid))
        .await
        .map_err(crate::store_read::read_failed)?;
    if present.is_empty() {
        return Err(refused(
            UNKNOWN_CALL,
            format!("call {call_key}'s message is no longer in the session"),
        ));
    }
    if !TagRange::holds(&shown, tag) {
        return Err(refused(
            UNKNOWN_TAG,
            format!("the request that made call {call_key} didn't show part number {tag}"),
        ));
    }
    let resolved = part_numbered(session, tag).await?.ok_or_else(|| {
        refused(
            UNKNOWN_TAG,
            format!("no part of the session has number {tag}"),
        )
    })?;
    // An imported message's numbers are another runner's, which the contract never resolves.
    if let Some(receipt) = session
        .import_receipt()
        .await
        .map_err(crate::store_read::read_failed)?
    {
        if resolved.ordinal <= receipt.imported_messages {
            return Err(refused(
                UNKNOWN_TAG,
                format!("part number {tag} is on a message the session imported"),
            ));
        }
    }
    Ok(resolved)
}

/// The dispatch intent recorded for `call_key`, with its seq. A call that never took one (an
/// imported call, a call refused before dispatch, a key no call had) is unknown.
async fn intent_of<B: SessionBackend>(
    session: &Session<B>,
    call_key: &str,
) -> Result<(u64, ToolIntent), ServeError> {
    session
        .find_id(IdKind::CallKey, call_key)
        .await
        .map_err(crate::store_read::read_failed)?
        .into_iter()
        .find_map(|(seq, record)| match record {
            Record::ToolIntent(intent) => Some((seq, intent)),
            _ => None,
        })
        .ok_or_else(|| refused(UNKNOWN_CALL, format!("no call has key {call_key}")))
}

/// The part numbers the request that produced `intent`'s call showed, and the id of the
/// assistant message its answer made. Reads the records of the call's run up to the intent: the
/// call's batch start recorded the numbers, and names the step whose start names the message.
async fn request_of<B: SessionBackend>(
    session: &Session<B>,
    intent_seq: u64,
    intent: &ToolIntent,
) -> Result<(Vec<TagRange>, String), ServeError> {
    let run_start = session
        .find_id(IdKind::RunId, &intent.run_id)
        .await
        .map_err(crate::store_read::read_failed)?
        .into_iter()
        .find_map(|(seq, record)| matches!(record, Record::RunStart(_)).then_some(seq))
        .ok_or_else(|| damaged(format!("run {} has no start", intent.run_id)))?;
    let records = session
        .records_between(run_start, intent_seq)
        .await
        .map_err(crate::store_read::read_failed)?;
    let mut records = records.into_iter().rev().map(|(_, record)| record);
    let batch = records
        .by_ref()
        .find_map(|record| match record {
            Record::BatchStart(batch)
                if batch.run_id == intent.run_id && batch.batch_id == intent.batch_id =>
            {
                Some(batch)
            }
            _ => None,
        })
        .ok_or_else(|| damaged(format!("batch {} has no start", intent.batch_id)))?;
    let mid = records
        .find_map(|record| match record {
            Record::StepStart(step)
                if step.run_id == intent.run_id && step.step_id == batch.step_id =>
            {
                Some(step.mid)
            }
            _ => None,
        })
        .flatten()
        .ok_or_else(|| damaged(format!("step {} named no message", batch.step_id)))?;
    Ok((batch.shown_tags, mid))
}

/// The message and part the session numbers `tag`, as `session.entries` numbers them: the entry
/// the search over the numbering bases finds, or the one before it, since the message found may
/// be the results half of a tool turn, whose numbers the turn's call message carries.
async fn part_numbered<B: SessionBackend>(
    session: &Session<B>,
    tag: u64,
) -> Result<Option<Resolved>, ServeError> {
    let entries = crate::store_read::served_compaction_entries_by_tag(session, &[tag])
        .await
        .map_err(crate::store_read::read_failed)?;
    for stored in &entries {
        let entry = broca_session_loop::entries::project(stored).map_err(damaged)?;
        let Some(part) = entry.parts.iter().find(|part| part.tag == Some(tag)) else {
            continue;
        };
        let part = part.id.parse::<u32>().map_err(|_| {
            damaged(format!(
                "message {} has a part whose id {:?} is no block index",
                stored.ordinal, part.id
            ))
        })?;
        return Ok(Some(Resolved {
            ordinal: stored.ordinal,
            part,
        }));
    }
    Ok(None)
}

/// Refuses `scope_owner_mismatch` unless the caller is the session's owner on a plain route:
/// `principal`, as the daemon attests it, is the owner the session's history records (the
/// owner of the scope its runs froze, or else the principal of its first run), and the route's
/// stamp, if any, is neither a flow's nor an agent run's. A reader the plan lists is no owner.
pub(crate) async fn check_owner(
    registry: &crate::LineageRegistry,
    session: &crate::LineageKey,
    request: &cortexkit_role_llm_runner::tag_read::TagReadRequest,
    principal: Option<&broca_protocol::BindPrincipal>,
    scope: &broca_protocol::SendScope,
) -> Result<(), ServeError> {
    use cortexkit_role_llm_runner::scope_admission::ScopeKind;
    let owner = match registry.bind_ownership(session).await {
        crate::registry::BindOwnership::Known(ownership) => ownership
            .scope
            .map(|scope| scope.owner)
            .or(ownership.first_principal),
        crate::registry::BindOwnership::NoHistorySource => None,
        crate::registry::BindOwnership::Unreadable(error) => {
            return Err(damaged(format!("the session's owner: {error}")))
        }
    };
    let stamped = match scope {
        broca_protocol::SendScope::Stamped(stamp) => Some(&stamp.scope),
        _ => None,
    };
    let kind = match stamped.map(|scope| (scope.flow_id.as_deref(), scope.run_id.as_deref())) {
        Some((Some(flow_id), _)) => ScopeKind::Flow { flow_id },
        Some((None, Some(run_id))) => ScopeKind::Run { run_id },
        _ => ScopeKind::Plain,
    };
    // Canonical spellings, so a module's two ids across a rename compare as one owner.
    let sender = principal
        .map(broca_protocol::BindPrincipal::canonical_owner_string)
        .unwrap_or_default();
    let owner = owner.map(|owner| owner.canonical_owner_string());
    request
        .check_owner(owner.as_deref(), &sender, kind)
        .map_err(|code| {
            refused(
                code,
                "only the session's owner, on its own session route, resolves a part number".into(),
            )
        })
}

fn refused(code: &'static str, message: String) -> ServeError {
    ServeError::TagReadRefused { code, message }
}

/// A refusal for records the session holds that can't be read as written.
fn damaged(message: impl std::fmt::Display) -> ServeError {
    ServeError::Open(
        format!("the call's request can't be read back: {message}"),
        Some(broca_protocol::ErrorClass::Permanent),
    )
}
