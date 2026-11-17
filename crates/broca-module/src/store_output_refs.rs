//! What output references read of a session kept in the new storage, as today's read of the
//! session's log answers it: where a part number sits, and the compaction view the request of
//! the model step that wrote a message was rendered under. Both are read from facts recorded
//! when each message was placed (`EntryFacts::runner_tag_base`, `EntryFacts::legacy_view_seq`),
//! never by replaying the session, so either read costs a bounded number of lookups whatever
//! the session's length.

use broca_core::compaction_provider::view::CompactionView;
use broca_session_loop::parts::Parts;
use broca_session_store::backend::SessionBackend;
use broca_session_store::record::Record;
use broca_session_store::session::Session;

/// The type name a first-protocol compaction view keeps as a ledger event on a converted
/// session (today's `CompactionViewApplied` record).
const LEGACY_VIEW_EVENT: &str = "compaction_view_applied";

/// The computed part-number rules run `run_id` froze (`broca_core::tags::computed_rules` over
/// its run config), read back from the run's start and the setup parts it names. `None` for a
/// run that numbers nothing, or one the session holds no start for (an import).
pub async fn run_rules<B: SessionBackend>(
    session: &Session<B>,
    parts: &dyn Parts,
    run_id: &str,
) -> Result<Option<broca_protocol::tags::ComputedRules>, String> {
    use broca_session_loop::writer::Writer;
    let Some(start) = Writer::run_start(session, run_id)
        .await
        .map_err(|error| format!("run {run_id}'s start: {error:?}"))?
    else {
        return Ok(None);
    };
    let facts = broca_session_loop::run::run_frozen_facts(&start, parts)
        .map_err(|error| format!("run {run_id}'s config: {error:?}"))?;
    Ok(facts.computed_rules())
}

/// Where part number `tag` sits as the run that numbered it showed it: the message whose
/// numbered parts take it (`Session::tagged_entry`), numbered under its run's rules from its
/// recorded base as today's read numbers it, and the part that takes `tag`, as `(zero-based
/// ordinal, block index)`. `found` is `None` when no part takes the number. The store answers
/// for every message it holds, so the location says it covers them all (`indexed_through`), and
/// a caller never scans the transcript for a number the store doesn't hold.
pub async fn tag_location<B: SessionBackend>(
    session: &Session<B>,
    parts: &dyn Parts,
    tag: u64,
) -> Result<broca_core::TagLocation, String> {
    let mut location = broca_core::TagLocation {
        found: None,
        indexed_through: u64::MAX,
    };
    let Some(entry) = session
        .tagged_entry(tag)
        .await
        .map_err(|error| format!("finding part number {tag}: {error:?}"))?
    else {
        return Ok(location);
    };
    let Some(facts) = entry.state.facts.as_ref() else {
        return Ok(location);
    };
    let (Some(run_id), Some(base)) = (facts.run_id.as_deref(), facts.runner_tag_base) else {
        return Ok(location);
    };
    let Some(rules) = run_rules(session, parts, run_id).await? else {
        return Ok(location);
    };
    let mut message = broca_protocol::Message::clone(&entry.message);
    let mut high = base;
    broca_protocol::tags::number_message(&mut message, rules, &mut high)
        .map_err(|_| format!("message {}'s part numbers overflow", entry.ordinal))?;
    location.found = message.part_tags().and_then(|tags| {
        let index = tags.parts.iter().position(|number| *number == Some(tag))?;
        Some((entry.ordinal - 1, index))
    });
    Ok(location)
}

/// The first-protocol compaction view in force when the model step that wrote message
/// `ordinal` (counting from 0) sent its request: the view event the message's entry facts name
/// (`EntryFacts::legacy_view_seq`), read by its seq (`Session::ledger_record`). `None` when they
/// name none: no such view had applied, as on every session the run loop writes, which never
/// applies a first-protocol view.
pub async fn writing_view<B: SessionBackend>(
    session: &Session<B>,
    ordinal: u64,
) -> Result<Option<CompactionView>, String> {
    let entries = crate::store_read::served_entries(session, ordinal + 1, 1)
        .await
        .map_err(|error| format!("message {}: {error:?}", ordinal + 1))?;
    let Some(seq) = entries
        .first()
        .filter(|entry| entry.ordinal == ordinal + 1)
        .and_then(|entry| entry.state.facts.as_ref())
        .and_then(|facts| facts.legacy_view_seq)
    else {
        return Ok(None);
    };
    let damaged = |what: &str| format!("the view event at seq {seq} {what}");
    let record = session
        .ledger_record(seq)
        .await
        .map_err(|error| damaged(&format!("can't be read: {error:?}")))?
        .ok_or_else(|| damaged("is missing"))?;
    let Record::Event(event) = record else {
        return Err(damaged("is not an event"));
    };
    if event.name != LEGACY_VIEW_EVENT {
        return Err(damaged(&format!("is a {} event", event.name)));
    }
    // Today's record holds the view as the JSON text the provider returned.
    let view = match event.body.as_ref().and_then(|body| body.get("view")) {
        Some(serde_json::Value::String(text)) => serde_json::from_str(text),
        Some(value) => serde_json::from_value(value.clone()),
        None => return Err(damaged("holds no view")),
    };
    // A view that doesn't parse is read as none applied.
    Ok(view.ok())
}
