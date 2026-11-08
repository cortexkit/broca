//! `session-convert history <path>...`: for every session log on the current compaction
//! contract (every log with `HISTORY_ALL=1`), today's `session.history` replies against the
//! replies the new storage serves from the log's converted session, page by page, in both forms
//! (raw and display), walked from the newest page by `older` cursors and from the first message
//! by `newer` cursors, `HISTORY_LIMIT` messages a page (7 by default).
//!
//! Both sides run the history read itself (`broca_module::history::read`) over a serve handler:
//! today's answers from the WAL alone by today's own steps, the store's from the converted
//! session, from the open log and again after sealing at the newest possible cut
//! (`broca_module::store_read::history_sides`). Each storage keeps its own incarnation and
//! history version, so today's side is given the store's, and cursors compare as the positions
//! they hold. Every row is compared whole, and so is each page beside its rows.
//!
//! After sealing, the opaque payloads the seal pruned from a finished run are set aside in
//! today's raw rows where the store's holds them emptied or removed, as the page check does.
//!
//! The report names counts, modes, field paths and log names only, never a log's content.

use std::collections::BTreeMap;

use broca_module::store_read::history_sides::TodaysHistory;
use broca_session_convert::stored_history::{history, Backend, StoredHistory};
use broca_session_loop::parts::MemoryParts;
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::keys::DeploymentKey;
use broca_session_store::session::Session;
use futures::executor::block_on;
use serde::Serialize;
use serde_json::{json, Value};

use super::{each_log, triple, write_report, Args, Item};
use broca_session_convert::counting::Counting;

/// The walks compared: a name, the form, and whether it goes newest first.
const WALKS: [(&str, &str, bool); 4] = [
    ("raw newest", "raw", true),
    ("raw oldest", "raw", false),
    ("display newest", "display", true),
    ("display oldest", "display", false),
];

#[derive(Default, Serialize)]
struct WalkReport {
    /// Rows today serves walking every page, and how many the store serves equal, from the open
    /// log and again after sealing (for the logs a seal could cut).
    rows: u64,
    equal_open: u64,
    sealed_rows: u64,
    equal_sealed: u64,
    /// Pages walked, and how many the store's equal beside their rows.
    pages: u64,
    pages_equal_open: u64,
    sealed_pages: u64,
    pages_equal_sealed: u64,
    /// Of today's rows, how many carry each member at the row's top level, in a display row
    /// (`row.<name>`), and in a part (`part.<name>`), so an equal count is known to cover each.
    members: BTreeMap<String, u64>,
}

#[derive(Default, Serialize)]
struct HistoryReport {
    logs: u64,
    mode3_logs: u64,
    sealed_logs: u64,
    pruned_payloads: u64,
    walks: BTreeMap<String, WalkReport>,
    /// The first difference of each mismatching walk, by walk, side and field path.
    mismatches: BTreeMap<String, u64>,
    /// A few mismatching logs per difference, to read by hand.
    examples: BTreeMap<String, Vec<String>>,
    failures: BTreeMap<String, u64>,
}

impl HistoryReport {
    fn note(&mut self, key: String, example: String) {
        let key: String = key.chars().take(120).collect();
        *self.mismatches.entry(key.clone()).or_default() += 1;
        let examples = self.examples.entry(key).or_default();
        if examples.len() < 5 {
            examples.push(example);
        }
    }

    fn fail(&mut self, what: &str, error: String, name: &str) {
        let key: String = format!("{what}: {error}").chars().take(120).collect();
        *self.failures.entry(key.clone()).or_default() += 1;
        let examples = self.examples.entry(key).or_default();
        if examples.len() < 5 {
            examples.push(name.to_owned());
        }
    }
}

/// Every page `read` serves in `form`, from the newest page by `older` cursors when `newest`,
/// else from the first message by `newer` cursors. A refused read ends the walk as a page
/// naming the refusal, so a refusal on one side only is a difference like any other.
fn walk(
    read: impl Fn(&Value) -> Result<Value, String>,
    form: &str,
    newest: bool,
    limit: u64,
) -> Vec<Value> {
    let mut params = json!({ "form": form, "limit": limit });
    if !newest {
        params["from_ordinal"] = json!(1);
    }
    let follow = if newest { "older" } else { "newer" };
    let mut pages = Vec::new();
    loop {
        let page = match read(&params) {
            Ok(page) => page,
            Err(error) => {
                pages.push(json!({ "refused": error }));
                return pages;
            }
        };
        let next = page.get(follow).and_then(Value::as_str).map(str::to_owned);
        pages.push(page);
        match next {
            Some(cursor) => params = json!({ "form": form, "limit": limit, "cursor": cursor }),
            None => return pages,
        }
    }
}

/// A page beside its rows.
fn page_members(page: &Value) -> Value {
    let mut page = page.clone();
    if let Some(object) = page.as_object_mut() {
        object.remove("messages");
    }
    page
}

/// Counts which members today's rows `pages` carry, into `walk`'s report: a raw row's own and
/// its message's parts', a display row's own (`row.<name>`) and its parts' (`part.<name>`).
fn count_members(report: &mut WalkReport, pages: &[Value]) {
    report.pages += pages.len() as u64;
    for row in pages.iter().flat_map(rows) {
        report.rows += 1;
        let Some(object) = row.as_object() else {
            continue;
        };
        for key in object.keys() {
            *report.members.entry(key.clone()).or_default() += 1;
        }
        let display = row.get("row");
        if display
            .and_then(|display| display["output_refs"]["count"].as_u64())
            .is_some_and(|count| count > 0)
        {
            *report
                .members
                .entry("row.output_refs naming a part".into())
                .or_default() += 1;
        }
        for key in display
            .and_then(Value::as_object)
            .into_iter()
            .flat_map(|display| display.keys())
        {
            *report.members.entry(format!("row.{key}")).or_default() += 1;
        }
        let parts = display
            .and_then(|display| display.get("parts"))
            .or_else(|| {
                row.get("message")
                    .and_then(|message| message.get("content"))
            });
        for part in parts.and_then(Value::as_array).into_iter().flatten() {
            for key in part.as_object().into_iter().flat_map(|part| part.keys()) {
                *report.members.entry(format!("part.{key}")).or_default() += 1;
            }
        }
    }
}

fn rows(page: &Value) -> &[Value] {
    page.get("messages")
        .and_then(Value::as_array)
        .map_or(&[], Vec::as_slice)
}

/// Compares the store's pages `got` with today's `expected` in `walk`, after sealing when
/// `sealed`, counting into `report`.
fn compare(
    report: &mut HistoryReport,
    name: &str,
    walk: &str,
    sealed: bool,
    got: &[Value],
    expected: &[Value],
) {
    let side = if sealed { "sealed" } else { "open" };
    let mut first_difference = None;
    if got.len() != expected.len() {
        first_difference = Some((
            format!("{} pages served, {} today", got.len(), expected.len()),
            String::new(),
        ));
    }
    let (mut pages_equal, mut equal, mut rows_seen, mut pruned_payloads) = (0, 0, 0, 0);
    for (got, expected) in got.iter().zip(expected) {
        if page_members(got) == page_members(expected) {
            pages_equal += 1;
        } else if first_difference.is_none() {
            first_difference = broca_session_convert::inverse::first_difference(
                &page_members(got),
                &page_members(expected),
                "page",
            )
            .map(|path| (path, String::new()));
        }
        if rows(got).len() != rows(expected).len() && first_difference.is_none() {
            first_difference = Some((
                format!(
                    "page: {} rows served, {} today",
                    rows(got).len(),
                    rows(expected).len()
                ),
                String::new(),
            ));
        }
        for (served, today) in rows(got).iter().zip(rows(expected)) {
            rows_seen += 1;
            // Raw rows are messages as `session.read` serves them.
            let today = if sealed && today.get("message").is_some() {
                let (today, pruned) = super::pages::set_aside(served, today);
                pruned_payloads += pruned;
                today
            } else {
                today.clone()
            };
            if *served == today {
                equal += 1;
            } else if first_difference.is_none() {
                let path = broca_session_convert::inverse::first_difference(served, &today, "")
                    .unwrap_or_else(|| "?".into());
                // `HISTORY_SHOW=<log name>` prints that log's first differing row both ways.
                if std::env::var("HISTORY_SHOW").is_ok_and(|show| show == name) {
                    eprintln!("{walk} {side} {path}\nstore: {served}\ntoday: {today}");
                }
                first_difference = Some((path, format!(" @{}", today["ordinal"])));
            }
        }
    }
    let entry = report.walks.entry(walk.to_owned()).or_default();
    if sealed {
        entry.sealed_pages += got.len() as u64;
        entry.pages_equal_sealed += pages_equal;
        entry.sealed_rows += rows_seen;
        entry.equal_sealed += equal;
    } else {
        entry.pages_equal_open += pages_equal;
        entry.equal_open += equal;
    }
    report.pruned_payloads += pruned_payloads;
    if let Some((path, at)) = first_difference {
        let path: String = path.chars().take(90).collect();
        report.note(format!("{walk} {side} {path}"), format!("{name}{at}"));
    }
}

/// The store's pages of `session` on every walk.
fn store_walks(
    session: &Session<&Backend>,
    parts: &MemoryParts,
    name: &str,
    limit: u64,
) -> Vec<Vec<Value>> {
    let handler = StoredHistory { session, parts };
    WALKS
        .iter()
        .map(|(_, form, newest)| {
            walk(
                |params| history(&handler, name, params),
                form,
                *newest,
                limit,
            )
        })
        .collect()
}

/// Today's pages on every walk, its head naming the store's `session`'s incarnation and
/// history version.
fn todays_walks(
    old: &super::OldLog,
    session: &Session<&Backend>,
    name: &str,
    limit: u64,
) -> Result<Vec<Vec<Value>>, String> {
    let head = session.head();
    let handler = TodaysHistory::new(
        &old.records,
        old.lineage.as_deref(),
        broca_session_store::codec::hex(&head.incarnation.0),
        head.history_version,
    )
    .map_err(|error| format!("{error:?}"))?;
    Ok(WALKS
        .iter()
        .map(|(_, form, newest)| {
            walk(
                |params| history(&handler, name, params),
                form,
                *newest,
                limit,
            )
        })
        .collect())
}

pub(super) fn run(args: &Args) {
    let mut report = HistoryReport::default();
    let deployment = DeploymentKey::new(1, &[0x5a; 32]);
    let limit = std::env::var("HISTORY_LIMIT")
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
        let records: Vec<broca_wal::WalRecord> = old.iter().map(|f| f.record.clone()).collect();
        let mode3 = broca_core::is_mode3_log(&records);
        if !mode3 && std::env::var("HISTORY_ALL").is_err() {
            return;
        }
        report.logs += 1;
        report.mode3_logs += u64::from(mode3);
        let backend = Counting::new(MemoryBackend::default());
        let addr = SessionAddr(name.clone());
        let (session, run_inputs, parts) =
            match super::pages::built_session(&old, &name, &backend, &addr, &deployment) {
                Ok(built) => built,
                Err(e) => return report.fail("session", e, &name),
            };
        let today = match todays_walks(&old, &session, &name, limit) {
            Ok(today) => today,
            Err(e) => return report.fail("today's head", e, &name),
        };
        for ((walk, _, _), pages) in WALKS.iter().zip(&today) {
            count_members(report.walks.entry((*walk).into()).or_default(), pages);
        }
        let open = store_walks(&session, &parts, &name, limit);
        for (((walk, _, _), got), expected) in WALKS.iter().zip(&open).zip(&today) {
            compare(&mut report, &name, walk, false, got, expected);
        }
        let sealed = match super::pages::seal_at_newest_input(session, &run_inputs) {
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
        // A seal keeps the incarnation and history version, so today's walks still apply.
        let after = store_walks(&view, &parts, &name, limit);
        for (((walk, _, _), got), expected) in WALKS.iter().zip(&after).zip(&today) {
            compare(&mut report, &name, walk, true, got, expected);
        }
    });
    write_report(&report, args.out_file.as_deref());
}
