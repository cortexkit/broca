//! `session-convert entries-check <path>...`: for every session log, what a session kept in the new
//! storage serves the compaction provider and history readers, against what today's storage
//! serves for the same history. Each log is converted and built in memory, as the `pages` check
//! builds it, and both sides are served by the same code their op serves them with:
//!
//! - **`session.history`, raw form:** every page of the transcript, 50 messages at a time (or
//!   `PAGES_LIMIT`), today's from `broca_core::read_session_page` over the log, the store's from
//!   `broca_module::store_read::read_page` with the part numbers and reply-cleaning marks
//!   (`store_entries::attach_part_tags`), each turned into raw entries by
//!   `store_entries::raw_history_entries`. Message ids are each storage's own and are left out.
//! - **The `compaction-runner/v1` bind:** today's admission (`check_bind` over the log's mode and
//!   the compaction provider its ownership summary keeps) against the store's
//!   (`store_entries::serve_compaction`), for the log's own provider and for another caller.
//! - **`session.entries`:** for a session today's bind admits, every `after` page 7 entries at a
//!   time, the whole transcript in full and compact detail, and reads by ids and by part numbers,
//!   today's paged from `transcript::project` with the newest entry and the applied view as its
//!   head.
//! - **`session.applied_view`:** today's from the compaction state folded from the log.
//!
//! Everything is compared as JSON. The report names counts, field paths and log names only.

use std::collections::BTreeMap;

use broca_core::compaction_v2::protocol::read::{Detail, Head, ReadRequest, Select};
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::session::Session;
use futures::executor::block_on;
use serde::Serialize;
use serde_json::{json, Value};

use crate::{convert_log, each_log, triple, write_report, Args, Item};

#[derive(Default, Serialize)]
struct EntriesCheckReport {
    logs: u64,
    mode3_logs: u64,
    /// Raw `session.history` entries today serves, walking every page, and how many the store
    /// serves equal.
    history_messages: u64,
    history_equal: u64,
    /// Of today's entries, how many have no written time, which the raw form refuses on both
    /// storages (`history_unrepresentable`), and how many replies today's read cleans of tag
    /// notation, so an equal count is known to cover cleaning.
    history_without_written_at: u64,
    history_cleaned_replies: u64,
    /// Bind decisions compared (two callers per log), and how many agree.
    binds: u64,
    binds_equal: u64,
    /// Today's bind decision for the log's own provider: admitted, or refused by reason.
    bind_outcomes: BTreeMap<String, u64>,
    /// `session.entries` reads compared on admitted sessions, and how many pages are equal.
    entries_reads: u64,
    entries_equal: u64,
    entries_compared: u64,
    applied_views: u64,
    applied_views_equal: u64,
    /// Of those, how many sessions apply a view.
    with_applied_view: u64,
    /// The first difference of each mismatching log, by op and field path.
    mismatches: BTreeMap<String, u64>,
    /// A few mismatching logs per difference, to read by hand.
    examples: BTreeMap<String, Vec<String>>,
    failures: BTreeMap<String, u64>,
}

impl EntriesCheckReport {
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

    /// Compares one reply of `op`; a difference is noted by its first differing path.
    fn compare(&mut self, op: &str, got: &Value, expected: &Value, name: &str) -> bool {
        if got == expected {
            return true;
        }
        let path = broca_session_convert::inverse::first_difference(got, expected, "")
            .unwrap_or_else(|| "?".into());
        self.note(format!("{op} {path}"), name.to_owned());
        false
    }
}

/// The bind a log's compaction state names: the harness and session of its first compaction
/// marker, which today's fold checks the state against, or a stand-in for a log with none.
fn log_bind(old: &[broca_wal::FramedRecord], name: &str) -> broca_protocol::BindIdentity {
    old.iter()
        .find_map(|frame| match &frame.record {
            broca_wal::WalRecord::CompactionMarker {
                harness, session, ..
            } => Some(broca_protocol::BindIdentity::new(
                "/entries-check",
                harness.clone(),
                session.clone(),
            )),
            _ => None,
        })
        .unwrap_or_else(|| {
            broca_protocol::BindIdentity::new("/entries-check", "entries-check", name)
        })
}

/// A refusal or an answer, as JSON, so two can be compared whole.
fn outcome<T: Serialize, E: Serialize>(result: Result<T, E>) -> Value {
    match result {
        Ok(value) => json!({ "ok": value }),
        Err(refusal) => json!({ "refused": refusal }),
    }
}

/// The raw history entries of every page `page` serves, walked from the start `limit` messages
/// at a time.
fn walk<F>(mut page: F, limit: u64) -> Result<Vec<Option<Value>>, String>
where
    F: FnMut(u64, u64) -> Result<broca_core::SessionReadPage, String>,
{
    let mut entries = Vec::new();
    let mut from = 0;
    loop {
        let read = page(from, limit)?;
        let next = read.next_from_ordinal;
        entries.extend(broca_module::store_entries::raw_history_entries(read));
        match next {
            Some(next) if next > from => from = next,
            _ => return Ok(entries),
        }
    }
}

pub(crate) fn run(args: &Args) {
    let mut report = EntriesCheckReport::default();
    let deployment = crate::deployment();
    let limit = std::env::var("PAGES_LIMIT")
        .ok()
        .and_then(|limit| limit.parse().ok())
        .unwrap_or(50);
    let mut index = 0usize;
    each_log(args, |item| {
        let Item::Log {
            name, old: Ok(old), ..
        } = item
        else {
            return;
        };
        index += 1;
        if !(index - 1).is_multiple_of(args.every) {
            return;
        }
        report.logs += 1;
        let framed: Vec<broca_wal::FramedRecord> = old.iter().cloned().collect();
        let records: Vec<broca_wal::WalRecord> = framed.iter().map(|f| f.record.clone()).collect();
        let mode3 = broca_core::is_mode3_log(&records);
        report.mode3_logs += u64::from(mode3);
        let converted = match convert_log(&old) {
            Ok(converted) => converted,
            Err(e) => return report.fail("convert", e.0, &name),
        };
        let parts = broca_session_loop::parts::MemoryParts {
            parts: converted.parts.clone(),
        };
        let backend = MemoryBackend::default();
        let session = block_on(async {
            let mut session = Session::create(
                &backend,
                SessionAddr(name.clone()),
                triple(&name),
                &deployment,
                false,
            )
            .await
            .map_err(|e| format!("create: {e:?}"))?;
            for record in &converted.records {
                session
                    .append(std::slice::from_ref(record), false)
                    .await
                    .map_err(|e| format!("append {}: {e:?}", record.name()))?;
            }
            Ok::<_, String>(session)
        });
        let session = match session {
            Ok(session) => session,
            Err(e) => return report.fail("session", e, &name),
        };

        // `session.history`, raw form.
        let today = walk(
            |from, limit| {
                broca_core::read_session_page(
                    &broca_core::StorePageSnapshot::empty(),
                    &framed,
                    Some(from),
                    Some(limit),
                )
                .map_err(|e| e.to_string())
            },
            limit,
        );
        let today_cleaned = (|| -> Result<u64, String> {
            let mut cleaned = 0;
            let mut from = 0;
            loop {
                let read = broca_core::read_session_page(
                    &broca_core::StorePageSnapshot::empty(),
                    &framed,
                    Some(from),
                    Some(limit),
                )
                .map_err(|e| e.to_string())?;
                cleaned += read
                    .messages
                    .iter()
                    .filter(|message| broca_protocol::tags::cleans_model_text(&message.message))
                    .count() as u64;
                match read.next_from_ordinal {
                    Some(next) if next > from => from = next,
                    _ => return Ok(cleaned),
                }
            }
        })();
        let store = walk(
            |from, limit| {
                block_on(async {
                    let read =
                        broca_module::store_read::read_page(&session, Some(from), Some(limit))
                            .await
                            .map_err(|e| format!("{e:?}"))?;
                    let mut page = read.page;
                    broca_module::store_entries::attach_part_tags(
                        &session,
                        &parts,
                        &mut page.messages,
                        &read.numbering,
                    )
                    .await?;
                    Ok(page)
                })
            },
            limit,
        );
        match (today, store, today_cleaned) {
            (Ok(today), Ok(store), Ok(cleaned)) => {
                report.history_messages += today.len() as u64;
                report.history_without_written_at +=
                    today.iter().filter(|entry| entry.is_none()).count() as u64;
                report.history_cleaned_replies += cleaned;
                if today.len() != store.len() {
                    report.note(
                        format!(
                            "history: {} entries served, {} today",
                            store.len(),
                            today.len()
                        ),
                        name.clone(),
                    );
                } else {
                    for (got, expected) in store.iter().zip(&today) {
                        let got = got.clone().unwrap_or(Value::Null);
                        let expected = expected.clone().unwrap_or(Value::Null);
                        if report.compare("history", &got, &expected, &name) {
                            report.history_equal += 1;
                        } else {
                            break;
                        }
                    }
                }
            }
            (Err(e), _, _) | (_, _, Err(e)) => report.fail("today's history", e, &name),
            (_, Err(e), _) => report.fail("the store's history", e, &name),
        }

        // The compaction route.
        let bind = log_bind(&framed, &name);
        let ownership = match broca_core::AdmissionCheckpointV1::from_frames(&framed) {
            Ok(checkpoint) => checkpoint.ownership.unwrap_or_default(),
            Err(e) => return report.fail("today's ownership", e.to_string(), &name),
        };
        let manifest = ownership
            .latest_manifest
            .as_deref()
            .or_else(|| ownership.first_episode.as_ref()?.manifest.as_deref());
        let provider = match manifest.map(broca_core::compaction::item).transpose() {
            Ok(item) => item.flatten().map(|item| item.0),
            Err(e) => return report.fail("today's compaction item", e, &name),
        };
        let serve = |caller: &str, method: &str, params: Value| -> Value {
            let params = serde_json::value::to_raw_value(&params).expect("params encode");
            outcome(
                block_on(broca_module::store_entries::serve_compaction(
                    &parts,
                    Some(&session),
                    &bind,
                    Some(caller),
                    method,
                    &params,
                ))
                .map(|reply| serde_json::from_str::<Value>(reply.get()).expect("a reply is JSON")),
            )
        };
        let own = provider.clone().unwrap_or_else(|| "ck-mc".into());
        let mut admitted = false;
        for caller in [own.as_str(), "another-provider"] {
            let today = outcome(
                broca_core::compaction_v2::authority::check_bind(
                    mode3,
                    provider.as_deref(),
                    Some(caller),
                )
                .map(|()| json!({})),
            );
            if caller == own {
                admitted = today.get("ok").is_some();
                let reason = today["refused"]["detail"]["reason"]
                    .as_str()
                    .unwrap_or("admitted");
                *report.bind_outcomes.entry(reason.into()).or_default() += 1;
            }
            report.binds += 1;
            let store = serve(caller, "bind", json!({}));
            if report.compare("bind", &store, &today, &name) {
                report.binds_equal += 1;
            }
        }
        if !admitted {
            return;
        }
        let Some(lineage) = old.lineage.clone() else {
            return report.fail("lineage", "a mode-3 log has no lineage frame".into(), &name);
        };
        let entries = match broca_core::compaction_v2::transcript::project(&framed, &lineage) {
            Ok(transcript) => transcript.entries,
            Err(e) => return report.fail("today's entries", e, &name),
        };
        let state = match broca_core::compaction_v2::session::load(&bind, &framed) {
            Ok(state) => state,
            Err(e) => return report.fail("today's compaction state", e, &name),
        };
        report.applied_views += 1;
        report.with_applied_view += u64::from(state.applied().is_some());
        let applied = match state.applied_received() {
            Some(view) => json!({ "ok": { "view": view } }),
            None => json!({ "ok": {} }),
        };
        if report.compare(
            "applied_view",
            &serve(&own, "session.applied_view", json!({})),
            &applied,
            &name,
        ) {
            report.applied_views_equal += 1;
        }
        let page = |request: &ReadRequest| {
            let mut head = Head::new();
            head.newest = broca_core::compaction_v2::views::newest(&entries);
            head.applied_view = state.applied_identity();
            json!({ "ok": broca_core::compaction_v2::views::page(&entries, request, head) })
        };
        let mut requests: Vec<ReadRequest> = Vec::new();
        for detail in [Detail::Full, Detail::Compact] {
            requests.push(ReadRequest::new(
                Select::After {
                    after_ordinal: None,
                    limit: None,
                },
                detail,
            ));
        }
        let tags: Vec<u64> = entries
            .iter()
            .flat_map(|entry| entry.parts.iter().filter_map(|part| part.tag))
            .collect();
        if let (Some(first), Some(last)) = (tags.first(), tags.last()) {
            requests.push(ReadRequest::new(
                Select::Tags {
                    tags: vec![*first, tags[tags.len() / 2], *last, u64::MAX >> 12],
                },
                Detail::Compact,
            ));
        }
        if let (Some(first), Some(last)) = (entries.first(), entries.last()) {
            requests.push(ReadRequest::new(
                Select::Mids {
                    mids: vec![
                        last.mid.clone(),
                        "no-such-message".into(),
                        first.mid.clone(),
                        entries[entries.len() / 2].mid.clone(),
                    ],
                },
                Detail::Full,
            ));
        }
        // Every `after` page, as a provider walks the transcript.
        let mut after = None;
        loop {
            let request = ReadRequest::new(
                Select::After {
                    after_ordinal: after,
                    limit: Some(7),
                },
                Detail::Full,
            );
            let expected = page(&request);
            let next = expected["ok"]["next_after"].as_u64();
            requests.push(request);
            match next {
                Some(next) if Some(next) != after => after = Some(next),
                _ => break,
            }
        }
        for request in &requests {
            report.entries_reads += 1;
            let got = serve(
                &own,
                "session.entries",
                serde_json::to_value(request).expect("a request encodes"),
            );
            let expected = page(request);
            report.entries_compared += expected["ok"]["entries"]
                .as_array()
                .map_or(0, |entries| entries.len() as u64);
            if report.compare("entries", &got, &expected, &name) {
                report.entries_equal += 1;
            }
        }
    });
    write_report(&report, args.out_file.as_deref());
}
