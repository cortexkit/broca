//! `session.tag_map` for a session kept in the session store: which message and part each part
//! number belongs to, the number the model is shown in front of the part.
//!
//! The numbers come from the code that already numbers each message, never from a numbering of
//! this read's own:
//! - on the current compaction contract, the request shows each part the number
//!   `session.entries` serves it with (`broca_session_loop::entries::project`, which numbers the
//!   entry from its recorded `tag_base`), so this read projects each message the same way;
//! - off it, a run numbers its own messages under the tag rules it froze, from each message's
//!   recorded `runner_tag_base`, as output references locate a number
//!   (`store_output_refs::tag_location`) and `session.read` numbers a page.
//!
//! Both are recorded beside each message when it is placed, so a page reads only its own
//! messages, sealed history included, and never replays the session. The read runs over a
//! read-only view of the session as stored (`StoreRuntime::durable_view`), and never takes the
//! session's lease. With an actor open, the actor first makes its records durable and names
//! where they end, so the read sees only what a power loss can't take back.

use std::collections::HashMap;

use broca_protocol::tags::ComputedRules;
use broca_protocol::BindIdentity;
use broca_session_loop::parts::Parts;
use broca_session_store::backend::SessionBackend;
use broca_session_store::session::{Session, StoredEntry};
use broca_wire::tag_map::{TagMapParams, TagMapResponse, TagMapRow};

use crate::serve::ServeError;
use crate::store_open::StoreRuntime;

/// Serves `session.tag_map` for the session `key`, which the session store keeps. A session whose
/// creation never committed holds no message, so it numbers nothing, as `session.read` reads it
/// as an empty transcript.
pub async fn serve(
    registry: &crate::LineageRegistry,
    store: &StoreRuntime,
    key: &BindIdentity,
    params: &TagMapParams,
) -> Result<TagMapResponse, ServeError> {
    let Some(class) = store.class_of(key)? else {
        return Ok(TagMapResponse::unnumbered());
    };
    let Some(view) = store.durable_view(registry, key, "session.tag_map").await? else {
        return Ok(TagMapResponse::unnumbered());
    };
    let parts = store.parts_for(class, view.head().cipher());
    page(&view, parts, params).await
}

/// One page of the tag map of `session`, whose runs name their setup parts in `parts`: the
/// numbered parts of up to `params.page_limit()` messages after `params.after_ordinal`.
pub async fn page<B: SessionBackend>(
    session: &Session<B>,
    parts: &dyn Parts,
    params: &TagMapParams,
) -> Result<TagMapResponse, ServeError> {
    let len = crate::store_read::transcript_len(session);
    let from = params.after_ordinal.unwrap_or(0).saturating_add(1);
    let limit = usize::try_from(params.page_limit()).unwrap_or(usize::MAX);
    let contract = crate::store_entries::on_contract(session);
    let latest_renders = latest_run_renders(session, parts, contract).map_err(damaged)?;
    let (entries, rows) = if contract {
        // Read as `session.entries` reads them, so a session that enrolled in the contract
        // numbers the messages placed before it as the enrollment decided.
        let entries = crate::store_read::served_compaction_entries(session, from, limit)
            .await
            .map_err(crate::store_read::read_failed)?;
        let rows = if latest_renders {
            contract_rows(&entries)?
        } else {
            Vec::new()
        };
        (entries, rows)
    } else {
        let entries = crate::store_read::served_entries(session, from, limit)
            .await
            .map_err(crate::store_read::read_failed)?;
        let rows = runner_rows(session, parts, &entries).await?;
        (entries, rows)
    };
    // Off the contract a run that doesn't render numbers numbers nothing, so the page's rows
    // already say whether an earlier run did.
    let numbered = latest_renders || !rows.is_empty();
    Ok(TagMapResponse {
        numbered,
        rows,
        next_after_ordinal: entries
            .last()
            .map(|entry| entry.ordinal)
            .filter(|last| *last < len),
    })
}

/// Whether the session's latest run shows the model part numbers: on the contract, when its
/// plan renders them or names a compaction item (`entries::Tags::for_run`); off it, when the
/// run froze computed tag rules that render (`broca_core::tags::computed_rules`). False for a
/// session with no run start.
fn latest_run_renders<B: SessionBackend>(
    session: &Session<B>,
    parts: &dyn Parts,
    contract: bool,
) -> Result<bool, String> {
    let Some(start) = session
        .operations
        .run
        .as_ref()
        .and_then(|run| run.start.as_ref())
    else {
        return Ok(false);
    };
    if contract {
        let admission = broca_session_loop::run::admission_of_start(start, parts)
            .map_err(|error| format!("the latest run's start: {error:?}"))?;
        Ok(broca_session_loop::entries::Tags::for_run(
            admission.manifest.as_deref(),
            &admission.render_config,
        )
        .render)
    } else {
        let facts = broca_session_loop::run::run_frozen_facts(start, parts)
            .map_err(|error| format!("the latest run's config: {error:?}"))?;
        Ok(facts.computed_rules().is_some_and(|rules| rules.render))
    }
}

/// The numbered parts of `entries`, as `session.entries` numbers them.
fn contract_rows(entries: &[StoredEntry]) -> Result<Vec<TagMapRow>, ServeError> {
    let mut rows = Vec::new();
    for stored in entries {
        let entry = broca_session_loop::entries::project(stored).map_err(damaged)?;
        for part in &entry.parts {
            let Some(tag) = part.tag else { continue };
            let index = part.id.parse::<u64>().map_err(|_| {
                damaged(format!(
                    "message {} has a part whose id {:?} is no block index",
                    stored.ordinal, part.id
                ))
            })?;
            rows.push(TagMapRow {
                tag,
                mid: entry.mid.clone(),
                ordinal: stored.ordinal,
                part: index,
                role: stored.message.role,
            });
        }
    }
    Ok(rows)
}

/// The numbered parts of `entries`, each message numbered by the run that placed it under that
/// run's rules from its recorded base, as `store_output_refs::tag_location` numbers it. A
/// message no run numbered (no base recorded, or an import) has none.
async fn runner_rows<B: SessionBackend>(
    session: &Session<B>,
    parts: &dyn Parts,
    entries: &[StoredEntry],
) -> Result<Vec<TagMapRow>, ServeError> {
    let mut runs: HashMap<String, Option<ComputedRules>> = HashMap::new();
    let mut rows = Vec::new();
    for stored in entries {
        let Some(facts) = stored.state.facts.as_ref() else {
            continue;
        };
        let (Some(run_id), Some(base)) = (facts.run_id.as_deref(), facts.runner_tag_base) else {
            continue;
        };
        if !runs.contains_key(run_id) {
            let rules = crate::store_output_refs::run_rules(session, parts, run_id)
                .await
                .map_err(damaged)?;
            runs.insert(run_id.to_owned(), rules);
        }
        let Some(rules) = runs[run_id] else { continue };
        let mut message = broca_protocol::Message::clone(&stored.message);
        let mut high = base;
        broca_protocol::tags::number_message(&mut message, rules, &mut high).map_err(|_| {
            damaged(format!(
                "message {}'s part numbers overflow",
                stored.ordinal
            ))
        })?;
        let Some(tags) = message.part_tags() else {
            continue;
        };
        for (index, tag) in tags.parts.iter().enumerate() {
            let Some(tag) = tag else { continue };
            rows.push(TagMapRow {
                tag: *tag,
                mid: facts.mid.clone(),
                ordinal: stored.ordinal,
                part: index as u64,
                role: stored.message.role,
            });
        }
    }
    Ok(rows)
}

/// A refusal for facts the session recorded that can't be read back.
fn damaged(message: impl std::fmt::Display) -> ServeError {
    ServeError::Open(
        format!("the session's part numbers can't be read: {message}"),
        Some(broca_protocol::ErrorClass::Permanent),
    )
}
