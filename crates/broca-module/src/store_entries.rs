//! What the compaction provider and history readers read from a session kept in the new storage
//! (`broca_session_store`): the `compaction-runner/v1` route's operations and `session.history`.
//!
//! Every read loads the session read-only (`StoreRuntime::view`): no lease, no actor, no WAL. A
//! reply here equals the reply today's storage builds for the same history, apart from the
//! differences the new storage has by design: no WAL lineage, the store's incarnation and
//! history version, no subscribe cursor, and the store's message ids.
//!
//! The `compaction-runner/v1` route on a store session:
//! - `bind` admits only the session's frozen compaction provider: the provider named by the plan
//!   its latest run froze, else by its first run's plan, read back from the setup parts each
//!   run's start names ([`frozen_provider`]). The session must be on the current compaction
//!   contract (its first run froze a compaction plan version). Refusals are today's
//!   (`compaction_not_permitted` with the same reasons). Admission is all `bind` does here: it
//!   doesn't push a Setup to the provider when it binds, as today's actor does once per actor
//!   life, because nothing on this storage holds the provider's per-session compaction state
//!   between runs. The next run sends its own Setup when it starts.
//! - `session.entries` serves the compaction entries `Session::entries` stores, projected by
//!   `broca_session_loop::entries::project`, whose output the converter's `entries` command
//!   compares, entry by entry, with today's projection of the same live logs.
//! - `session.applied_view` serves the view the session's latest step applied, rebuilt from the
//!   open log by the same fold the run loop restores its compaction state with.
//! - `session.apply_view` writes an accepted view, which only the session's writer may do. The
//!   store actor doesn't take one yet, so it is refused by name (`not_served_on_new_storage`).
//!
//! `session.history` on a store session (`crate::history::read`) takes its head facts from
//! [`history_head`] and its pages from `session.read`, which a store session serves from the
//! store. Message ids name the store's own ids, which only the store resolves. The raw form is
//! served whole. The display form needs per-call times and statuses, the Rich UI dialect and
//! catalog digest each run froze at its start (`rich_ui`), and output references, which the
//! store doesn't serve yet, so it is refused by name.
//!
//! A read cleans tag notation out of a reply wherever the run that wrote it cleans, as today's
//! read does: [`mark_reply_cleaning`] marks each message from the tag rules its run froze.

use broca_core::compaction_v2::protocol::{
    read::{Cursor, Entry, Head, ReadPage, ReadRequest, Select},
    view::{AppliedView, AppliedViewRequest},
    wire::{not_permitted_reasons, Refusal},
};
use broca_protocol::BindIdentity;
use broca_session_loop::parts::Parts;
use broca_session_store::backend::SessionBackend;
use broca_session_store::record::RunStart;
use broca_session_store::session::{Session, StoredEntry};
use serde_json::value::{to_raw_value, RawValue};

use crate::store_open::StoreRuntime;

/// The refusal code of an operation this build doesn't serve for a session kept in the new
/// storage. Permanent: retrying on this build can't succeed.
pub const NOT_SERVED_ON_NEW_STORAGE: &str = "not_served_on_new_storage";

fn internal(message: impl Into<String>) -> Refusal {
    Refusal::new("internal_error", message, None)
}

/// How many messages the session's transcript holds as reads serve it
/// (`store_read::transcript_len`).
fn transcript_len<B: SessionBackend>(session: &Session<B>) -> u64 {
    crate::store_read::transcript_len(session)
}

/// Whether the session is on the current compaction contract: its first run froze a compaction
/// plan version. Today's storage decides it the same way from a log's first run start
/// (`broca_core::is_mode3_log`). A session with no run yet isn't.
pub fn on_contract<B: SessionBackend>(session: &Session<B>) -> bool {
    session
        .operations
        .owner
        .first_start
        .as_ref()
        .is_some_and(|start| start.compaction_plan_version.is_some())
}

/// The compaction provider in force: the one the plan of the session's latest run names, or,
/// when that run froze no plan, the one its first run's plan names. That is the rule today's
/// ownership summary keeps (`SessionOwnershipV1::latest_manifest`, else the first episode's
/// manifest), read here from the setup parts each run's start names. `None` for a session with
/// no run, or whose plan in force names no compaction item.
pub fn frozen_provider<B: SessionBackend>(
    parts: &dyn Parts,
    session: &Session<B>,
) -> Result<Option<String>, String> {
    let manifest_of = |start: &RunStart, which: &str| {
        broca_session_loop::run::admission_of_start(start, parts)
            .map(|admission| admission.manifest)
            .map_err(|error| format!("the session's {which} run start: {error:?}"))
    };
    let latest = match session
        .operations
        .run
        .as_ref()
        .and_then(|run| run.start.as_ref())
    {
        Some(start) => manifest_of(start, "latest")?,
        None => None,
    };
    let manifest = match latest {
        Some(manifest) => Some(manifest),
        None => match session.operations.owner.first_start.as_ref() {
            Some(first) => manifest_of(first, "first")?,
            None => None,
        },
    };
    Ok(manifest
        .as_deref()
        .map(broca_core::compaction::item)
        .transpose()?
        .flatten()
        .map(|item| item.0))
}

/// Serves one `compaction-runner/v1` operation for the session `key`, kept in the new storage.
/// `caller` is the daemon-attested module id of the route, never a body field.
pub async fn compaction_runner(
    store: &StoreRuntime,
    key: &BindIdentity,
    caller: Option<String>,
    method: String,
    params: Box<RawValue>,
) -> Result<Box<RawValue>, Refusal> {
    let session = store
        .view(key)
        .await
        .map_err(|error| internal(error.to_string()))?;
    serve_compaction(
        store.parts(),
        session.as_deref(),
        key,
        caller.as_deref(),
        &method,
        &params,
    )
    .await
}

/// [`compaction_runner`] over a loaded `session`, whose runs name their setup parts in `parts`.
/// `None` is a session whose creation never committed.
pub async fn serve_compaction<B: SessionBackend>(
    parts: &dyn Parts,
    session: Option<&Session<B>>,
    key: &BindIdentity,
    caller: Option<&str>,
    method: &str,
    params: &RawValue,
) -> Result<Box<RawValue>, Refusal> {
    let session = admit(parts, session, caller)?;
    match method {
        "bind" => to_raw_value(&serde_json::json!({})).map_err(|error| internal(error.to_string())),
        "session.entries" => {
            let request: ReadRequest = serde_json::from_str(params.get())
                .map_err(|error| Refusal::invalid_params("", error.to_string()))?;
            if matches!(request.select, Select::After { limit: Some(0), .. }) {
                return Err(Refusal::invalid_params(
                    "select.limit",
                    "compaction entries limit must be at least 1",
                ));
            }
            let page = entries_page(session, &request).await.map_err(internal)?;
            to_raw_value(&page).map_err(|error| internal(error.to_string()))
        }
        "session.applied_view" => {
            let _: AppliedViewRequest = serde_json::from_str(params.get())
                .map_err(|error| Refusal::invalid_params("", error.to_string()))?;
            let records = session
                .open_log_records()
                .await
                .map_err(|error| internal(format!("{error:?}")))?;
            let state = broca_session_loop::compaction::restore(&records, key, &session.operations)
                .map_err(internal)?;
            let answer = match state.applied_received() {
                Some(view) => serde_json::json!({ "view": view }),
                None => serde_json::json!({}),
            };
            to_raw_value(&answer).map_err(|error| internal(error.to_string()))
        }
        // Applied by the session's actor (`apply_view`): a caller that holds no actor, such as
        // the converter's check over a session built in memory, can't apply one.
        "session.apply_view" => Err(Refusal::new(
            NOT_SERVED_ON_NEW_STORAGE,
            "session.apply_view is applied by the session's actor",
            Some(serde_json::json!({ "method": method })),
        )),
        _ => Err(Refusal::unknown_method(method)),
    }
}

/// The route's admission, as every method of it is admitted: only the compaction provider the
/// plan in force names, on a session on the current compaction contract. The session, once
/// admitted.
fn admit<'s, B: SessionBackend>(
    parts: &dyn Parts,
    session: Option<&'s Session<B>>,
    caller: Option<&str>,
) -> Result<&'s Session<B>, Refusal> {
    // A session whose creation never committed holds no run, so it isn't on the contract, as
    // today's storage reads a session with no log.
    let Some(session) = session else {
        return Err(Refusal::compaction_not_permitted(
            not_permitted_reasons::SESSION_NOT_HELD,
        ));
    };
    let contract = on_contract(session);
    let provider = if contract {
        frozen_provider(parts, session).map_err(internal)?
    } else {
        None
    };
    broca_core::compaction_v2::authority::check_bind(contract, provider.as_deref(), caller)?;
    Ok(session)
}

/// `session.apply_view` on the route, for the session `key` kept in the new storage: admitted
/// and read as today's route reads it, then applied by the session's actor, which records the
/// view and sets it in the compaction state it hands to every run (`StoreCommand::ApplyView`).
pub async fn apply_view(
    registry: &crate::registry::LineageRegistry,
    store: &StoreRuntime,
    key: &BindIdentity,
    caller: Option<String>,
    params: Box<RawValue>,
) -> Result<Box<RawValue>, Refusal> {
    #[derive(serde::Deserialize)]
    struct RawApply {
        view: Box<RawValue>,
    }
    let session = store
        .view(key)
        .await
        .map_err(|error| internal(error.to_string()))?;
    admit(store.parts(), session.as_deref(), caller.as_deref())?;
    drop(session);
    let request: RawApply = serde_json::from_str(params.get())
        .map_err(|error| Refusal::invalid_params("", error.to_string()))?;
    broca_core::compaction_v2::received::ReceivedView::parse(&request.view)
        .map_err(|error| Refusal::invalid_params("view", error.to_string()))?;
    let command = crate::store_import::StoreCommand::ApplyView(Box::new(
        crate::store_import::ApplyViewCommand { view: request.view },
    ));
    use crate::store_import::StoreOutcome;
    match registry.route_store_command(key, command).await {
        Ok(StoreOutcome::ViewApplied(answer)) => {
            RawValue::from_string(answer).map_err(|error| internal(error.to_string()))
        }
        Ok(StoreOutcome::Refused(refusal)) => {
            Err(Refusal::new(refusal.code, refusal.message, None))
        }
        Ok(other) => Err(internal(format!(
            "session.apply_view was answered {other:?}"
        ))),
        Err(error) => Err(internal(error.to_string())),
    }
}

/// A stored message as the compaction entry `session.entries` serves.
fn project(stored: &StoredEntry) -> Result<Entry, String> {
    broca_session_loop::entries::project(stored)
}

/// Every compaction entry of the session, oldest first: what a view is checked against.
pub async fn all_entries<B: SessionBackend>(session: &Session<B>) -> Result<Vec<Entry>, String> {
    crate::store_read::served_entries(session, 1, usize::MAX)
        .await
        .map_err(|error| format!("{error:?}"))?
        .iter()
        .map(project)
        .collect()
}

/// A `session.entries` page, as today's storage pages its whole transcript
/// (`broca_core::compaction_v2::views::page`) with the newest entry and the applied view as its
/// head. An `after` read loads only the entries from the ordinal it names, one past its limit so
/// the page knows whether more follow; a read by ids or tags looks at every entry.
pub async fn entries_page<B: SessionBackend>(
    session: &Session<B>,
    request: &ReadRequest,
) -> Result<ReadPage, String> {
    let len = transcript_len(session);
    let (from, count) = match &request.select {
        Select::After {
            after_ordinal,
            limit,
        } => (
            after_ordinal.unwrap_or(0).saturating_add(1),
            limit.map_or(usize::MAX, |limit| {
                usize::try_from(limit)
                    .unwrap_or(usize::MAX)
                    .saturating_add(1)
            }),
        ),
        _ => (1, usize::MAX),
    };
    let stored = crate::store_read::served_entries(session, from, count)
        .await
        .map_err(|error| format!("{error:?}"))?;
    let entries = stored.iter().map(project).collect::<Result<Vec<_>, _>>()?;
    let newest = match entries.last() {
        Some(last) if last.ordinal >= len => Some(cursor(last)),
        _ if len == 0 => None,
        _ => {
            let last = crate::store_read::served_entries(session, len, 1)
                .await
                .map_err(|error| format!("{error:?}"))?;
            last.first().map(project).transpose()?.as_ref().map(cursor)
        }
    };
    let mut head = Head::new();
    head.newest = newest;
    head.applied_view = session
        .operations
        .view_applied
        .as_ref()
        .map(|applied| AppliedView::new(applied.view.version, &applied.view.checksum));
    Ok(broca_core::compaction_v2::views::page(
        &entries, request, head,
    ))
}

fn cursor(entry: &Entry) -> Cursor {
    Cursor::new(entry.ordinal, &entry.mid, &entry.digest)
}

/// The raw `session.history` entries of a `session.read` page, as `session.history` serves them,
/// apart from each message's id, which the reader sets. `None` in place of an entry with no
/// written time, which the raw form refuses (`history_unrepresentable`). Lets a check build the
/// raw form from either storage's page with the same code the op serves it with.
pub fn raw_history_entries(page: broca_core::SessionReadPage) -> Vec<Option<serde_json::Value>> {
    crate::serve::session_read_response(page, None)
        .messages
        .iter()
        .map(|entry| {
            entry
                .written_at
                .map(|written_at| crate::history::raw_entry(entry, written_at))
        })
        .collect()
}

/// The refusal of a part-number lookup (`ServeHandler::tag_location`) on a session kept in the
/// new storage. The lookup isn't compaction-facing: output references (`session.output_refs`,
/// `session.output_ref_read` and the display form of `session.history`) use it to find the part
/// a reply's part number named, then check it against the compaction view the writing step
/// rendered with, which they read from today's WAL. Until output references are served from the
/// store, the lookup is refused by name, permanently, rather than answered as "no such part".
pub fn tag_location_unserved() -> crate::serve::ServeError {
    crate::serve::ServeError::Open(
        format!(
            "{NOT_SERVED_ON_NEW_STORAGE}: output references are not served yet for a session kept \
             in the new storage"
        ),
        Some(broca_protocol::ErrorClass::Permanent),
    )
}

/// What `session.history` reads about a session kept in the new storage besides its pages,
/// which today's storage answers from `session.head` and from the WAL form of a message id.
#[derive(Debug, Clone, PartialEq)]
pub struct HistoryHead {
    /// The identity a history cursor is bound to: the store's incarnation, or `unwritten` for a
    /// session whose creation never committed.
    pub incarnation: String,
    pub history_version: u64,
    /// The newest message's ordinal, counting from 1; `None` for an empty transcript.
    pub last_ordinal: Option<u64>,
    /// The ordinal (from 1) of the message the read names by id, when the session holds one by
    /// that id. Only the store resolves its own ids: they aren't positions, as today's are.
    pub mid_ordinal: Option<u64>,
}

/// The [`HistoryHead`] of the session `key`, resolving `mid` when the read names a message.
pub async fn history_head(
    store: &StoreRuntime,
    key: &BindIdentity,
    mid: Option<&str>,
) -> Result<HistoryHead, crate::serve::ServeError> {
    let Some(session) = store.view(key).await? else {
        // The session's creation never committed, so it holds nothing, as `session.read` serves
        // it.
        return Ok(HistoryHead {
            incarnation: "unwritten".into(),
            history_version: 0,
            last_ordinal: None,
            mid_ordinal: None,
        });
    };
    let len = transcript_len(&session);
    let mid_ordinal = match mid {
        Some(mid) => session
            .find_message(mid)
            .await
            .map_err(crate::store_read::read_failed)?,
        None => None,
    };
    let head = session.head();
    Ok(HistoryHead {
        incarnation: broca_session_store::codec::hex(&head.incarnation.0),
        history_version: head.history_version,
        last_ordinal: (len > 0).then_some(len),
        mid_ordinal,
    })
}

/// Marks each model-written message of a read page whose run cleans its replies, as today's
/// read marks it, so every exposure (`broca_protocol::tags::exposed_message`) cleans tag notation
/// out of the reply text exactly where today's does. Today's read decides it from the computed
/// tag rules the run froze (`broca_core::tags::computed_rules` over its run config), so this reads
/// each run's config back from its start and the setup parts it names, once per run on the page.
/// The mark carries only what exposure reads (whether the run cleans); a read that serves no
/// display facts reads nothing else from it. A message no run wrote (an import) isn't cleaned,
/// as today's isn't.
pub async fn mark_reply_cleaning<B: SessionBackend>(
    session: &Session<B>,
    parts: &dyn Parts,
    messages: &mut [broca_core::SessionReadMessage],
) -> Result<(), String> {
    use broca_protocol::tags::{ComputedTags, PartTags};
    use broca_session_loop::writer::Writer;
    let mut cleans: std::collections::HashMap<String, Option<bool>> = Default::default();
    for message in messages {
        if message.message.role != broca_protocol::content::Role::Assistant {
            continue;
        }
        let Some(run) = &message.run else {
            continue;
        };
        if !cleans.contains_key(&run.run_id) {
            let start = Writer::run_start(session, &run.run_id)
                .await
                .map_err(|error| format!("run {}'s start: {error:?}", run.run_id))?;
            let rules = match start {
                Some(start) => {
                    let config = broca_session_loop::run::run_config_of_start(&start, parts)
                        .map_err(|error| format!("run {}'s config: {error:?}", run.run_id))?;
                    broca_core::tags::computed_rules(&config).map(|rules| rules.clean)
                }
                None => None,
            };
            cleans.insert(run.run_id.clone(), rules);
        }
        if cleans[&run.run_id] == Some(true) {
            message.message.set_tags(Some(std::sync::Arc::new(PartTags {
                parts: vec![None; message.message.content.len()],
                computed: Some(ComputedTags {
                    render: true,
                    clean: true,
                    base: 0,
                }),
            })));
        }
    }
    Ok(())
}
