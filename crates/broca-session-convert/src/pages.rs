//! `session-convert pages <path>...`: for every session log on the current compaction contract
//! (every log with `PAGES_ALL=1`), today's `session.read` replies against the replies the new
//! storage serves from the log's converted session, compared as the wire serves them, page by
//! page from the start, `PAGES_LIMIT` messages at a time (7 by default, so paging and the
//! final-message rule across a page boundary are exercised).
//!
//! Each log is walked in five read modes: the default read, each opt-in alone
//! (`include_display_facts`, `include_tools`, `include_rich_ui`) and all three together. Today's
//! replies are built from the WAL alone by today's own steps (`store_read::todays`); the store's
//! by the function its `session.read` serves with (`store_read::serve_view`), from the open log
//! and again after sealing at the newest possible cut. Every served message is compared whole,
//! and so are a page's tool list, its run and lineage state, and where the next page starts.
//!
//! Some reply members are each storage's own and aren't compared; each is checked on the store's
//! side instead: its history version and incarnation must be its head's, and the approval
//! members (`deferred_calls`, `late_approvals`, `open_batch`, `next_late_after`) must be absent,
//! as today serves them for a session with no approvals. The store serves no subscribe cursor
//! (`head`) yet; pages without one are counted.
//!
//! After sealing, the opaque payloads the seal pruned from a finished run (encrypted reasoning,
//! reasoning signatures, a text block's provider state, a tool call's thought signature) are set
//! aside in today's message where the store's holds them emptied or removed, and counted.
//!
//! The report names counts, modes, field paths and log names only, never a log's content.

use std::collections::BTreeMap;

use broca_module::serve::{SessionReadRequest, SessionReadResponse};
use broca_module::store_read::todays::Todays;
use broca_session_loop::parts::MemoryParts;
use broca_session_store::backend::{MemoryBackend, SessionAddr, SessionBackend};
use broca_session_store::keys::DeploymentKey;
use broca_session_store::session::Session;
use futures::executor::block_on;
use serde::Serialize;
use serde_json::{json, Value};

use super::{convert_log, each_log, triple, write_report, Args, Item};

/// The read modes walked: a name, and whether it asks for display facts, tools and Rich UI.
const MODES: [(&str, bool, bool, bool); 5] = [
    ("default", false, false, false),
    ("display_facts", true, false, false),
    ("tools", false, true, false),
    ("rich_ui", false, false, true),
    ("all", true, true, true),
];

/// The message members whose presence in today's replies is counted, so an equal count is known
/// to cover each.
const MEMBERS: [&str; 12] = [
    "sender",
    "notices",
    "created_at",
    "completed_at",
    "reasoning_summaries",
    "client_message_id",
    "tool_times",
    "rich_ui",
    "written_at",
    "tool_calls",
    "delivered_as",
    "run",
];

#[derive(Default, Serialize)]
struct ModeReport {
    /// Messages today serves walking every page, and how many the store serves equal, from the
    /// open log and again after sealing (for the logs a seal could cut).
    messages: u64,
    equal_open: u64,
    sealed_messages: u64,
    equal_sealed: u64,
    /// Pages walked, and how many the store's equal in the members beside the messages.
    pages: u64,
    pages_equal_open: u64,
    sealed_pages: u64,
    pages_equal_sealed: u64,
    /// Of today's messages, how many carry each member (and, for `tool_times`, how many carry
    /// a call key and a time); of today's pages, how many carry `tools`.
    members: BTreeMap<String, u64>,
}

#[derive(Default, Serialize)]
struct PagesReport {
    logs: u64,
    mode3_logs: u64,
    /// Logs today's read takes as having no lineage: it serves them no display facts.
    without_lineage: u64,
    sealed_logs: u64,
    pruned_payloads: u64,
    modes: BTreeMap<String, ModeReport>,
    /// The store's pages that fail a check made on its side only, by check.
    store_checks_failed: BTreeMap<String, u64>,
    /// The store's pages without a subscribe cursor, which today's serve.
    store_pages_without_head: u64,
    /// The first difference of each mismatching page, by mode, side and field path.
    mismatches: BTreeMap<String, u64>,
    /// A few mismatching logs per difference, to read by hand.
    examples: BTreeMap<String, Vec<String>>,
    failures: BTreeMap<String, u64>,
}

impl PagesReport {
    fn note(&mut self, key: String, example: String) {
        let key: String = key.chars().take(100).collect();
        *self.mismatches.entry(key.clone()).or_default() += 1;
        let examples = self.examples.entry(key).or_default();
        if examples.len() < 5 {
            examples.push(example);
        }
    }

    fn fail(&mut self, what: &str, error: String, name: &str) {
        let key: String = format!("{what}: {error}").chars().take(100).collect();
        *self.failures.entry(key.clone()).or_default() += 1;
        let examples = self.examples.entry(key).or_default();
        if examples.len() < 5 {
            examples.push(name.to_owned());
        }
    }
}

/// A read of `session` from 1-based `from`, `limit` messages, in `mode`.
fn request(
    name: &str,
    from: u64,
    limit: u64,
    mode: (&str, bool, bool, bool),
) -> SessionReadRequest {
    let key = broca_protocol::BindIdentity::new("/pages", "pages", name);
    let mut req = SessionReadRequest::new(key, Some(from), Some(limit));
    (
        _,
        req.include_display_facts,
        req.include_tools,
        req.include_rich_ui,
    ) = mode;
    req
}

/// Every page `read` serves, walked from the start by `next_from_ordinal`.
fn walk(
    mut read: impl FnMut(u64) -> Result<SessionReadResponse, String>,
) -> Result<Vec<SessionReadResponse>, String> {
    let mut pages = Vec::new();
    let mut from = 1;
    loop {
        let page = read(from)?;
        let next = page.next_from_ordinal;
        pages.push(page);
        match next {
            Some(next) if next > from => from = next,
            _ => return Ok(pages),
        }
    }
}

/// What a page serves beside its messages, as compared: every member it serializes except the
/// messages (compared one by one) and the members each storage keeps for itself, which the
/// store's side is checked for instead (see [`store_pages`]).
fn page_members(page: &SessionReadResponse) -> Value {
    let mut value = serde_json::to_value(page).expect("replies serialize");
    let members = value.as_object_mut().expect("a reply is an object");
    for own in [
        "messages",
        "history_version",
        "incarnation",
        "head",
        "deferred_calls",
        "late_approvals",
        "open_batch",
        "next_late_after",
    ] {
        members.remove(own);
    }
    value
}

/// Today's message with the payloads the store's sealed copy holds pruned set aside, and how
/// many were. A payload counts as pruned only where the store's message holds it empty.
pub(super) fn set_aside(got: &Value, expected: &Value) -> (Value, u64) {
    let mut expected = expected.clone();
    let mut pruned = 0;
    let blocks = got["message"]["content"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    if let Some(content) = expected["message"]["content"].as_array_mut() {
        for (block, served) in content.iter_mut().zip(&blocks) {
            if block["type"] == "redacted_reasoning"
                && served["type"] == "redacted_reasoning"
                && served["data"] == ""
                && block["data"] != ""
            {
                block["data"] = json!("");
                pruned += 1;
            }
            if block["type"] == "reasoning"
                && served["type"] == "reasoning"
                && served.get("signature").is_none()
                && block.get("signature").is_some()
            {
                block.as_object_mut().map(|block| block.remove("signature"));
                pruned += 1;
            }
            // A text block's provider state, emptied (`OpaqueField::TextProviderState`).
            if block["type"] == served["type"]
                && served["provider_state"]["data"] == ""
                && block["provider_state"]["data"]
                    .as_str()
                    .is_some_and(|data| !data.is_empty())
            {
                block["provider_state"]["data"] = json!("");
                pruned += 1;
            }
            // A tool call's thought signature, removed (`ToolCallThoughtSignature`).
            if block["type"] == "tool_call"
                && served["type"] == "tool_call"
                && served.get("thought_signature").is_none()
                && block.get("thought_signature").is_some()
            {
                block
                    .as_object_mut()
                    .map(|block| block.remove("thought_signature"));
                pruned += 1;
            }
        }
    }
    (expected, pruned)
}

/// The store's pages of `session`, in `mode`, with what only the store's side is checked for
/// counted in `report`.
fn store_pages<B: SessionBackend>(
    session: &Session<B>,
    parts: &MemoryParts,
    name: &str,
    limit: u64,
    mode: (&str, bool, bool, bool),
    report: &mut PagesReport,
) -> Result<Vec<SessionReadResponse>, String> {
    let pages = walk(|from| {
        block_on(broca_module::store_read::serve_view(
            session,
            parts,
            &request(name, from, limit, mode),
        ))
        .map_err(|error| format!("{error:?}"))
    })?;
    let head = session.head();
    let incarnation = broca_session_store::codec::hex(&head.incarnation.0);
    for page in &pages {
        let mut failed = |check: &str| {
            *report.store_checks_failed.entry(check.into()).or_default() += 1;
        };
        if page.history_version != head.history_version {
            failed("history_version is not the head's");
        }
        if page.incarnation != incarnation {
            failed("incarnation is not the head's");
        }
        if page.deferred_calls.is_some()
            || page.late_approvals.is_some()
            || page.open_batch.is_some()
            || page.next_late_after.is_some()
        {
            failed("approval members served");
        }
        report.store_pages_without_head += u64::from(page.head.is_none());
    }
    Ok(pages)
}

/// Compares the store's pages `got` with today's `expected` in `mode`, after sealing when
/// `sealed`, counting into `report`.
fn compare(
    report: &mut PagesReport,
    name: &str,
    mode: &str,
    sealed: bool,
    got: &[SessionReadResponse],
    expected: &[SessionReadResponse],
) {
    let side = if sealed { "sealed" } else { "open" };
    if got.len() != expected.len() {
        report.note(
            format!(
                "{mode} {side}: {} pages served, {} today",
                got.len(),
                expected.len()
            ),
            name.to_owned(),
        );
    }
    let mut pruned_payloads = 0;
    let mut first_difference = None;
    let (mut pages_equal, mut equal, mut messages) = (0, 0, 0);
    for (got, expected) in got.iter().zip(expected) {
        if page_members(got) == page_members(expected) {
            pages_equal += 1;
        } else if first_difference.is_none() {
            // `PAGES_SHOW=<log name>` prints that log's first differing page's members (never its
            // messages) both ways.
            if std::env::var("PAGES_SHOW").is_ok_and(|show| show == name) {
                eprintln!(
                    "{mode} {side} page\nstore: {}\ntoday: {}",
                    page_members(got),
                    page_members(expected)
                );
            }
            first_difference = broca_session_convert::inverse::first_difference(
                &page_members(got),
                &page_members(expected),
                "page",
            )
            .map(|path| (path, String::new()));
        }
        if got.messages.len() != expected.messages.len() && first_difference.is_none() {
            first_difference = Some((
                format!(
                    "page: {} messages served, {} today",
                    got.messages.len(),
                    expected.messages.len()
                ),
                String::new(),
            ));
        }
        for (served, today) in got.messages.iter().zip(&expected.messages) {
            messages += 1;
            let served = serde_json::to_value(served).expect("messages serialize");
            let today = serde_json::to_value(today).expect("messages serialize");
            let today = if sealed {
                let (today, pruned) = set_aside(&served, &today);
                pruned_payloads += pruned;
                today
            } else {
                today
            };
            if served == today {
                equal += 1;
            } else if first_difference.is_none() {
                let path = broca_session_convert::inverse::first_difference(&served, &today, "")
                    .unwrap_or_else(|| "?".into());
                // `PAGES_SHOW=<log name>` prints that log's first differing message's shape
                // both ways: which members each serves and the message's roles and part kinds.
                if std::env::var("PAGES_SHOW").is_ok_and(|show| show == name) {
                    let brief = |value: &Value| {
                        json!({
                            "ordinal": value["ordinal"],
                            "role": value["message"]["role"],
                            "members": value.as_object().map(|object| object.keys().cloned().collect::<Vec<_>>()),
                            "parts": value["message"]["content"].as_array().map(|parts| {
                                parts.iter().map(|part| part["type"].clone()).collect::<Vec<_>>()
                            }),
                        })
                    };
                    eprintln!(
                        "{mode} {side} {path}\nstore: {}\ntoday: {}",
                        brief(&served),
                        brief(&today)
                    );
                }
                first_difference = Some((path, format!(" @{}", today["ordinal"])));
            }
        }
    }
    let entry = report.modes.entry(mode.to_owned()).or_default();
    if sealed {
        entry.sealed_pages += got.len() as u64;
        entry.pages_equal_sealed += pages_equal;
        entry.sealed_messages += messages;
        entry.equal_sealed += equal;
    } else {
        entry.pages_equal_open += pages_equal;
        entry.equal_open += equal;
    }
    report.pruned_payloads += pruned_payloads;
    if let Some((path, at)) = first_difference {
        let path: String = path.chars().take(80).collect();
        report.note(format!("{mode} {side} {path}"), format!("{name}{at}"));
    }
}

/// Counts which members today's pages `pages` carry, into `mode`'s report.
fn count_members(report: &mut ModeReport, pages: &[SessionReadResponse]) {
    report.pages += pages.len() as u64;
    for page in pages {
        *report.members.entry("page tools".into()).or_default() += u64::from(page.tools.is_some());
        for message in &page.messages {
            report.messages += 1;
            let value = serde_json::to_value(message).expect("messages serialize");
            for member in MEMBERS {
                *report.members.entry(member.into()).or_default() +=
                    u64::from(value.get(member).is_some());
            }
            let keyed = message
                .tool_times
                .iter()
                .any(|time| time.call_key.is_some());
            let timed = message
                .tool_times
                .iter()
                .any(|time| time.started_at.is_some() || time.unanswered.is_some());
            *report
                .members
                .entry("tool_times with a call key".into())
                .or_default() += u64::from(keyed);
            *report
                .members
                .entry("tool_times with a time or unanswered".into())
                .or_default() += u64::from(timed);
        }
    }
}

/// The converted session of the log `old`, built in `backend` at `addr` by appending the
/// converted records one at a time, with the seqs of its runs' inputs (where a seal can cut)
/// and its setup parts.
pub(super) fn built_session<B: SessionBackend>(
    old: &super::OldLog,
    name: &str,
    backend: B,
    addr: &SessionAddr,
    deployment: &DeploymentKey,
) -> Result<(Session<B>, Vec<u64>, MemoryParts), String> {
    let converted = convert_log(old).map_err(|e| format!("convert: {}", e.0))?;
    let parts = MemoryParts {
        parts: converted.parts.clone(),
    };
    block_on(async {
        let mut session = Session::create(backend, addr.clone(), triple(name), deployment, false)
            .await
            .map_err(|e| format!("create: {e:?}"))?;
        let mut run_inputs = Vec::new();
        for (index, record) in converted.records.iter().enumerate() {
            let seq = session
                .append(std::slice::from_ref(record), false)
                .await
                .map_err(|e| format!("append {}: {e:?}", record.name()))?;
            // A converted record that names another by seq counts on this.
            if seq != index as u64 + broca_session_convert::FIRST_SEQ {
                return Err(format!("converted record {index} took seq {seq}"));
            }
            if matches!(record, broca_session_store::record::Record::Input(input) if input.placement == broca_session_store::record::Placement::RunInput)
            {
                run_inputs.push(seq);
            }
        }
        Ok((session, run_inputs, parts))
    })
}

/// Seals `session` at the newest of its runs' inputs `run_inputs` a seal can cut at, and closes
/// it. False when no cut can be sealed: none is a clean cut, or the session names compaction
/// views, which only a caller that carries them can seal, as this check doesn't.
pub(super) fn seal_at_newest_input<B: SessionBackend>(
    mut session: Session<B>,
    run_inputs: &[u64],
) -> Result<bool, String> {
    block_on(async {
        let mut sealed = false;
        for cut in run_inputs.iter().rev() {
            match session.seal(Some(*cut), 0).await {
                Ok(_) => {
                    sealed = true;
                    break;
                }
                Err(broca_session_store::session::SessionError::BadCut(_)) => continue,
                Err(broca_session_store::session::SessionError::ViewNotCarried) => break,
                Err(e) => return Err(format!("seal: {e:?}")),
            }
        }
        session.close().await.map_err(|e| format!("close: {e:?}"))?;
        Ok(sealed)
    })
}

pub(super) fn run(args: &Args) {
    let mut report = PagesReport::default();
    let deployment = DeploymentKey::new(1, &[0x5a; 32]);
    let limit = std::env::var("PAGES_LIMIT")
        .ok()
        .and_then(|limit| limit.parse().ok())
        .unwrap_or(7);
    each_log(args, |item| {
        let Item::Log {
            name, old: Ok(old), ..
        } = item
        else {
            return;
        };
        report.logs += 1;
        let records: Vec<broca_wal::WalRecord> = old.iter().map(|f| f.record.clone()).collect();
        let mode3 = broca_core::is_mode3_log(&records);
        // `PAGES_ALL=1` takes every log, not only those on the current compaction contract.
        if !mode3 && std::env::var("PAGES_ALL").is_err() {
            return;
        }
        report.mode3_logs += u64::from(mode3);
        report.without_lineage += u64::from(old.lineage.is_none());
        let todays = Todays::new(&old.records, old.lineage.as_deref());
        let mut today = Vec::new();
        for mode in MODES {
            match walk(|from| {
                todays
                    .read(&request(&name, from, limit, mode))
                    .map_err(|error| format!("{error:?}"))
            }) {
                Ok(pages) => {
                    count_members(report.modes.entry(mode.0.into()).or_default(), &pages);
                    today.push(pages);
                }
                Err(e) => return report.fail(&format!("today's {} pages", mode.0), e, &name),
            }
        }
        let backend = MemoryBackend::default();
        let addr = SessionAddr(name.clone());
        let (session, run_inputs, parts) =
            match built_session(&old, &name, &backend, &addr, &deployment) {
                Ok(built) => built,
                Err(e) => return report.fail("session", e, &name),
            };
        for (mode, today) in MODES.iter().zip(&today) {
            match store_pages(&session, &parts, &name, limit, *mode, &mut report) {
                Ok(open) => compare(&mut report, &name, mode.0, false, &open, today),
                Err(e) => return report.fail(&format!("the store's {} pages", mode.0), e, &name),
            }
        }
        let sealed = match seal_at_newest_input(session, &run_inputs) {
            Ok(sealed) => sealed,
            Err(e) => return report.fail("session", e, &name),
        };
        if !sealed {
            return;
        }
        report.sealed_logs += 1;
        let view = match block_on(Session::view(&backend, addr, &triple(&name), &deployment)) {
            Ok(view) => view,
            Err(e) => return report.fail("view", format!("{e:?}"), &name),
        };
        for (mode, today) in MODES.iter().zip(&today) {
            match store_pages(&*view, &parts, &name, limit, *mode, &mut report) {
                Ok(after) => compare(&mut report, &name, mode.0, true, &after, today),
                Err(e) => {
                    return report.fail(&format!("the store's sealed {} pages", mode.0), e, &name)
                }
            }
        }
    });
    write_report(&report, args.out_file.as_deref());
}

/// `session-convert read-cost <path>...`: for each log, what every 32-message page with every
/// opt-in reads from storage, the log stored as a session's runs store it, sealed at each run's
/// input (`broca_session_convert::read_cost`). Reports the session's stored objects and bytes and
/// the most any page read, never a log's content.
pub(super) fn run_read_cost(args: &Args) {
    use broca_session_convert::read_cost::{page_reads, store, stored_objects};
    let mut logs = Vec::new();
    each_log(args, |item| {
        let Item::Log {
            name, old: Ok(old), ..
        } = item
        else {
            return;
        };
        let started = std::time::Instant::now();
        let measured = store(&old, old.lineage.as_deref(), &name).and_then(|stored| {
            let from: Vec<u64> = (1..=stored.messages).step_by(32).collect();
            let reads = page_reads(&stored, &from, 32, true)?;
            let plain = page_reads(&stored, &from, 32, false)?;
            let (objects, bytes) = stored_objects(&stored);
            Ok(json!({
                "log": name,
                "messages": stored.messages,
                "seals": stored.seals,
                "stored_objects": objects,
                "stored_bytes": bytes,
                "pages": reads.len(),
                "most_objects_per_page": reads.iter().map(|read| read.objects.len()).max(),
                "most_object_bytes_per_page": reads.iter().map(|read| read.object_bytes).max(),
                "most_log_reads_per_page": reads.iter().map(|read| read.log_reads).max(),
                "objects_over_all_pages": reads.iter().map(|read| read.objects.len()).sum::<usize>(),
                "default_most_objects_per_page": plain.iter().map(|read| read.objects.len()).max(),
                "default_objects_over_all_pages": plain.iter().map(|read| read.objects.len()).sum::<usize>(),
                "most_objects_the_opt_ins_add": reads
                    .iter()
                    .zip(&plain)
                    .map(|(all, plain)| all.objects.len().saturating_sub(plain.objects.len()))
                    .max(),
                "elapsed_ms": started.elapsed().as_millis() as u64,
            }))
        });
        logs.push(measured.unwrap_or_else(|error| json!({"log": name, "error": error})));
    });
    write_report(&logs, args.out_file.as_deref());
}
