//! `session-convert calls-check <path>...`: what `session.read` serves about each tool call
//! (`tool_calls`: key, dispatch module, `indeterminate`) and a carrier result's call key
//! (`delivered_as.call_key`), served by today's storage from the log and by the new storage from
//! the converted session, compared message by message.
//!
//! A call is indeterminate only while its batch is open, so each log is compared at cut points
//! as well as whole: the log cut right after each dispatch intent (a call in flight) and right
//! after each provider-held record (a held call), the states a crash or a pause leaves. Every
//! cut is converted on its own, as a resume would find it.
//!
//! - **Today's side:** the pages `broca_core::read_session_page_owned_with_dispatches` serves
//!   from the cut log, each turn's dispatch facts taken from the page's tail and else from the
//!   whole cut log (`broca_core::tool_call_dispatches`), as the registry's read takes them, then
//!   attributed by `broca_module::serve::apply_tool_call_dispatches`.
//! - **The store's side:** the pages `broca_module::store_read::read_page` serves from the
//!   converted session, attributed by `broca_module::store_calls::attribute_tool_calls`.
//!
//! The report names counts, field paths, log names and cut positions only.

use std::collections::BTreeMap;

use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::keys::DeploymentKey;
use broca_session_store::session::Session;
use futures::executor::block_on;
use serde::Serialize;
use serde_json::{json, Value};

use crate::{each_log, triple, write_report, Args, Item};

#[derive(Default, Serialize)]
struct CallsCheckReport {
    logs: u64,
    /// Cut points compared (each log's whole length is one), and how many served alike.
    cuts: u64,
    cuts_equal: u64,
    /// Cut points where today serves at least one call as indeterminate: the comparison sees
    /// the case it checks.
    cuts_with_indeterminate: u64,
    /// Calls answered without a dispatch intent (denied, invalid, synthetic, or refused with
    /// their batch), which today keys by the record that answered them, over every cut, and the
    /// cuts that hold at least one.
    calls_answered_without_intent: u64,
    cuts_with_call_answered_without_intent: u64,
    /// Cut points right after a provider-held record, and what today serves for the held call
    /// there: indeterminate or not.
    held_cuts: u64,
    held_served_indeterminate: u64,
    held_served_determinate: u64,
    /// Calls compared, and of those how many today serves with a key, with a module, and as
    /// indeterminate.
    calls: u64,
    calls_with_key: u64,
    calls_with_module: u64,
    calls_indeterminate: u64,
    /// Tool results delivered as a send whose carrier call today names by key.
    carrier_keys: u64,
    /// Cut points whose two sides serve a different number of messages; their calls aren't
    /// compared.
    message_counts_differ: u64,
    /// The first difference at each mismatching cut, by field path.
    mismatches: BTreeMap<String, u64>,
    /// A few mismatching logs per difference, with the cut's record count.
    examples: BTreeMap<String, Vec<String>>,
    failures: BTreeMap<String, u64>,
}

impl CallsCheckReport {
    fn note(&mut self, key: String, example: String) {
        let key: String = key.chars().take(100).collect();
        *self.mismatches.entry(key.clone()).or_default() += 1;
        let examples = self.examples.entry(key).or_default();
        if examples.len() < 5 {
            examples.push(example);
        }
    }

    fn fail(&mut self, what: &str, error: String, example: String) {
        let key: String = format!("{what}: {error}").chars().take(100).collect();
        *self.failures.entry(key.clone()).or_default() += 1;
        let examples = self.examples.entry(key).or_default();
        if examples.len() < 5 {
            examples.push(example);
        }
    }
}

/// What each page message carries about its calls, as served: its ordinal, `tool_calls` and
/// the carrier key of its delivery.
fn served(response: &impl Serialize) -> Vec<Value> {
    let response = serde_json::to_value(response).expect("a read reply serializes");
    response["messages"]
        .as_array()
        .map(|messages| {
            messages
                .iter()
                .map(|message| {
                    json!({
                        "ordinal": message["ordinal"],
                        "tool_calls": message.get("tool_calls").cloned().unwrap_or_else(|| json!([])),
                        "carrier_key": message["delivered_as"]["call_key"],
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Today's tool-call attribution of every page of `framed`, `limit` messages at a time. The
/// dispatch facts of the turns each page asks about come from its own tail first and else from
/// the whole log, as the registry's read takes them; a log without a lineage names no key.
fn today(
    framed: &[broca_wal::FramedRecord],
    lineage: Option<&str>,
    limit: u64,
) -> Result<Vec<Value>, String> {
    let whole = broca_core::tool_call_dispatches(framed).map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    let mut from = 0;
    loop {
        let (page, tail) = broca_core::read_session_page_owned_with_dispatches(
            broca_core::StorePageSnapshot::empty(),
            framed,
            Some(from),
            Some(limit),
        )
        .map_err(|e| e.to_string())?;
        let next = page.next_from_ordinal;
        let mut response = broca_module::serve::session_read_response(page, None);
        // The turns the read asks about, as `attribute_tool_calls` picks them: a carrier
        // result's turn, else a message's own.
        let asked: Vec<u64> = response
            .messages
            .iter()
            .filter_map(|message| {
                let delivered = message
                    .run
                    .as_ref()
                    .and_then(|run| run.delivery.as_ref())
                    .is_some_and(|delivery| {
                        delivery.mode == broca_protocol::DeliveryMode::ToolResult
                    });
                let calls = message
                    .message
                    .content
                    .iter()
                    .any(|block| matches!(block, broca_protocol::ContentBlock::ToolCall(_)));
                if delivered {
                    message.ordinal.checked_sub(2)
                } else if calls {
                    message.ordinal.checked_sub(1)
                } else {
                    None
                }
            })
            .collect();
        let dispatches: BTreeMap<u64, Vec<broca_core::ToolCallDispatch>> = asked
            .iter()
            .filter_map(|ordinal| {
                tail.get(ordinal)
                    .or_else(|| whole.get(ordinal))
                    .map(|facts| (*ordinal, facts.clone()))
            })
            .collect();
        match lineage {
            Some(lineage) => {
                broca_module::serve::apply_tool_call_dispatches(&mut response, &dispatches, lineage)
            }
            // Without a lineage no call has a key: every call is served with no dispatch fact,
            // which is what attribution over no facts serves.
            None if !asked.is_empty() => {
                broca_module::serve::apply_tool_call_dispatches(&mut response, &BTreeMap::new(), "")
            }
            None => {}
        }
        out.extend(served(&response));
        match next {
            Some(next) if next > from => from = next,
            _ => return Ok(out),
        }
    }
}

/// The store's tool-call attribution of every page of `session`.
async fn store(session: &Session<&MemoryBackend>, limit: u64) -> Result<Vec<Value>, String> {
    let mut out = Vec::new();
    let mut from = 0;
    loop {
        let read = broca_module::store_read::read_page(session, Some(from), Some(limit))
            .await
            .map_err(|e| format!("{e:?}"))?;
        let next = read.page.next_from_ordinal;
        let mut response = broca_module::serve::session_read_response(read.page, None);
        broca_module::store_calls::attribute_tool_calls(session, &mut response, &read.turns)
            .await
            .map_err(|e| format!("{e:?}"))?;
        out.extend(served(&response));
        match next {
            Some(next) if next > from => from = next,
            _ => return Ok(out),
        }
    }
}

/// The cut points of a log: right after each dispatch intent (at most `max` of them, spread
/// over the log) and each provider-held record (each with the held call's id), and the whole
/// log.
fn cuts(old: &[broca_wal::FramedRecord], max: usize) -> Vec<(usize, Option<String>)> {
    let mut intents = Vec::new();
    let mut held = Vec::new();
    for (index, frame) in old.iter().enumerate() {
        match &frame.record {
            broca_wal::WalRecord::ToolDispatchIntent { .. } => intents.push(index + 1),
            broca_wal::WalRecord::ToolCallProviderHeld { tool_call_id, .. } => {
                held.push((index + 1, Some(tool_call_id.0.clone())));
            }
            _ => {}
        }
    }
    let step = intents.len().div_ceil(max.max(1)).max(1);
    let mut out: Vec<(usize, Option<String>)> = intents
        .into_iter()
        .step_by(step)
        .map(|end| (end, None))
        .collect();
    out.extend(held);
    out.push((old.len(), None));
    out.sort();
    out.dedup_by_key(|(end, _)| *end);
    out
}

pub(crate) fn run(args: &Args) {
    let mut report = CallsCheckReport::default();
    let deployment = DeploymentKey::new(1, &[0x5a; 32]);
    let limit = std::env::var("PAGES_LIMIT")
        .ok()
        .and_then(|limit| limit.parse().ok())
        .unwrap_or(50);
    let max_cuts = std::env::var("CALLS_MAX_CUTS")
        .ok()
        .and_then(|max| max.parse().ok())
        .unwrap_or(12);
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
        if args.max_records.is_some_and(|max| old.len() > max) {
            return;
        }
        report.logs += 1;
        let lineage = old.lineage.as_deref();
        for (end, held) in cuts(&old, max_cuts) {
            let at = format!("{name} @{end}");
            let framed = &old[..end];
            let expected = match today(framed, lineage, limit) {
                Ok(expected) => expected,
                Err(e) => {
                    report.fail("today's read", e, at);
                    continue;
                }
            };
            let converted = match broca_session_convert::convert(framed, lineage) {
                Ok(converted) => converted,
                Err(e) => {
                    report.fail("convert", e.0, at);
                    continue;
                }
            };
            let got = block_on(async {
                let backend = MemoryBackend::default();
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
                store(&session, limit).await
            });
            let got = match got {
                Ok(got) => got,
                Err(e) => {
                    report.fail("the store's read", e, at);
                    continue;
                }
            };
            report.cuts += 1;
            let without_intent = broca_core::tool_call_dispatches(framed)
                .map(|all| {
                    all.values()
                        .flatten()
                        .filter(|call| call.intent_seq.is_none() && call.key_seq.is_some())
                        .count() as u64
                })
                .unwrap_or(0);
            report.calls_answered_without_intent += without_intent;
            report.cuts_with_call_answered_without_intent += u64::from(without_intent > 0);
            let calls = || {
                expected.iter().flat_map(|message| {
                    message["tool_calls"]
                        .as_array()
                        .cloned()
                        .unwrap_or_default()
                })
            };
            report.cuts_with_indeterminate +=
                u64::from(calls().any(|call| call["indeterminate"] == json!(true)));
            for call in calls() {
                report.calls += 1;
                report.calls_with_key += u64::from(!call["call_key"].is_null());
                report.calls_with_module += u64::from(!call["dispatched_to"].is_null());
                report.calls_indeterminate += u64::from(call["indeterminate"] == json!(true));
            }
            report.carrier_keys += expected
                .iter()
                .filter(|message| !message["carrier_key"].is_null())
                .count() as u64;
            if let Some(held) = &held {
                report.held_cuts += 1;
                // The held call's latest copy on the page, which is the open turn's.
                match calls()
                    .filter(|call| call["tool_call_id"] == json!(held))
                    .collect::<Vec<_>>()
                    .pop()
                {
                    Some(call) if call["indeterminate"] == json!(true) => {
                        report.held_served_indeterminate += 1
                    }
                    Some(_) => report.held_served_determinate += 1,
                    None => {}
                }
            }
            if got.len() != expected.len() {
                report.message_counts_differ += 1;
                report.note(
                    format!("{} messages served, {} today", got.len(), expected.len()),
                    at,
                );
                continue;
            }
            match got
                .iter()
                .zip(&expected)
                .find(|(got, expected)| got != expected)
            {
                None => report.cuts_equal += 1,
                Some((got, expected)) => {
                    let path = broca_session_convert::inverse::first_difference(got, expected, "")
                        .unwrap_or_else(|| "?".into());
                    report.note(path, format!("{at} message {}", expected["ordinal"]));
                }
            }
        }
    });
    write_report(&report, args.out_file.as_deref());
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A page message as JSON, for comparing two pages field by field.
    fn page_message_json(message: &broca_core::SessionReadMessage) -> Value {
        json!({
            "ordinal": message.ordinal,
            "created_at": message.created_at,
            "written_at": message.written_at,
            "completed_at": message.completed_at,
            "message": message.message,
            "run": message.run,
        })
    }

    /// Every page of a transcript, walked from the start by `next_from_ordinal`, `limit`
    /// messages at a time, with each page's state (lineage state, length, next ordinal).
    fn walk_pages<F>(mut page: F, limit: u64) -> Result<(Vec<Value>, Vec<String>), String>
    where
        F: FnMut(u64, u64) -> Result<broca_core::SessionReadPage, String>,
    {
        let mut messages = Vec::new();
        let mut states = Vec::new();
        let mut from = 0;
        loop {
            let read = page(from, limit)?;
            states.push(format!(
                "{:?} len {} next {:?}",
                read.lineage_state, read.prompt_len, read.next_from_ordinal
            ));
            messages.extend(read.messages.iter().map(page_message_json));
            match read.next_from_ordinal {
                Some(next) if next > from => from = next,
                _ => return Ok((messages, states)),
            }
        }
    }

    /// [`walk_pages`] over the pages the new format serves for `session`.
    async fn walk_store_pages(
        session: &Session<&MemoryBackend>,
        limit: u64,
    ) -> Result<(Vec<Value>, Vec<String>), String> {
        let mut messages = Vec::new();
        let mut states = Vec::new();
        let mut from = 0;
        loop {
            let read = broca_module::store_read::read_page(session, Some(from), Some(limit))
                .await
                .map_err(|e| format!("{e:?}"))?
                .page;
            states.push(format!(
                "{:?} len {} next {:?}",
                read.lineage_state, read.prompt_len, read.next_from_ordinal
            ));
            messages.extend(read.messages.iter().map(page_message_json));
            match read.next_from_ordinal {
                Some(next) if next > from => from = next,
                _ => return Ok((messages, states)),
            }
        }
    }

    const RUN: &str = r#"{"type":"run_started","run_id":"r1","session":{"project_root":"/project","harness":"runner","session":"s"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[{"name":"bash","description":"run","input_schema":{"type":"object"},"execution_mode":"parallel"}],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hi"}]}],"origin":{"kind":"fresh"}}"#;

    fn log(lines: &[&str]) -> Vec<broca_wal::FramedRecord> {
        lines
            .iter()
            .enumerate()
            .map(|(i, line)| broca_wal::FramedRecord {
                seq: i as u64 + 1,
                fence: 0,
                record: serde_json::from_str(line)
                    .unwrap_or_else(|e| panic!("fixture line {i}: {e}")),
            })
            .collect()
    }

    /// The store's attribution of a log's converted session.
    fn store_of(framed: &[broca_wal::FramedRecord], lineage: &str) -> Vec<Value> {
        let converted = broca_session_convert::convert(framed, Some(lineage)).unwrap();
        block_on(async {
            let backend = MemoryBackend::default();
            let mut session = Session::create(
                &backend,
                SessionAddr("s".into()),
                triple("s"),
                &DeploymentKey::new(1, &[0x5a; 32]),
                false,
            )
            .await
            .unwrap();
            for record in &converted.records {
                session
                    .append(std::slice::from_ref(record), false)
                    .await
                    .unwrap();
            }
            store(&session, 50).await.unwrap()
        })
    }

    /// A step with two calls: `c1` in flight, `c2` held by its provider, then each answered. At
    /// every cut both storages serve the same calls, and today's serves a call indeterminate from
    /// its intent until its result, held or not.
    #[test]
    fn calls_in_flight_and_held_read_alike_at_every_cut() {
        let lines = [
            RUN,
            r#"{"type":"step_started","step_id":1}"#,
            r#"{"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m1","content":[{"type":"tool_call","tool_call_id":"c1","tool_name":"bash","input":{"cmd":"ls"}},{"type":"tool_call","tool_call_id":"c2","tool_name":"bash","input":{"cmd":"pwd"}}]},"usage":{"input_tokens":3},"finish_reason":"tool_calls","retries_used":0}"#,
            r#"{"type":"tool_batch_started","batch_id":"b1","step_id":1,"planned_calls":["c1","c2"]}"#,
            r#"{"type":"tool_dispatch_intent","batch_id":"b1","tool_call_id":"c1","args":{"cmd":"ls"},"module_id":"tools"}"#,
            r#"{"type":"tool_dispatch_intent","batch_id":"b1","tool_call_id":"c2","args":{"cmd":"pwd"},"module_id":"tools"}"#,
            r#"{"type":"tool_call_provider_held","batch_id":"b1","tool_call_id":"c2","call_key":"l1:6","module":"tools","reason":"waiting","held_at_ms":5,"sequential":false}"#,
            r#"{"type":"tool_result","batch_id":"b1","result":{"tool_call_id":"c1","output":{"kind":"text","text":"ok"},"is_error":false},"outcome":"completed"}"#,
            r#"{"type":"tool_result","batch_id":"b1","result":{"tool_call_id":"c2","output":{"kind":"text","text":"here"},"is_error":false},"outcome":"completed"}"#,
            r#"{"type":"tool_batch_finished","batch_id":"b1"}"#,
        ];
        let old = log(&lines);
        // Whether each call is indeterminate after the cut, as today's read serves it.
        let expected = [
            (5, [true, false]),
            (6, [true, true]),
            (7, [true, true]),
            (8, [false, true]),
            (10, [false, false]),
        ];
        for (end, indeterminate) in expected {
            let framed = &old[..end];
            let today = today(framed, Some("l1"), 50).unwrap();
            let calls: Vec<&Value> = today
                .iter()
                .flat_map(|message| message["tool_calls"].as_array().into_iter().flatten())
                .collect();
            assert_eq!(
                calls
                    .iter()
                    .map(|call| call["indeterminate"] == json!(true))
                    .collect::<Vec<_>>(),
                indeterminate,
                "today's calls after record {end}: {calls:?}"
            );
            assert_eq!(calls[0]["call_key"], json!("l1:5"), "after record {end}");
            assert_eq!(calls[0]["dispatched_to"], json!("tools"));
            assert_eq!(store_of(framed, "l1"), today, "after record {end}");
        }
    }

    /// A step with a call the policy denied beside a dispatched one: today keys the denied call
    /// by the record that answered it, and the store serves the same key from the outcome it
    /// recorded, at every cut and on the closed turn. The denied call has no module and is never
    /// indeterminate.
    #[test]
    fn a_denied_call_reads_alike_with_its_key() {
        let lines = [
            RUN,
            r#"{"type":"step_started","step_id":1}"#,
            r#"{"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m1","content":[{"type":"tool_call","tool_call_id":"c1","tool_name":"bash","input":{"cmd":"ls"}},{"type":"tool_call","tool_call_id":"c2","tool_name":"bash","input":{"cmd":"rm"}}]},"usage":{"input_tokens":3},"finish_reason":"tool_calls","retries_used":0}"#,
            r#"{"type":"tool_batch_started","batch_id":"b1","step_id":1,"planned_calls":["c1","c2"]}"#,
            r#"{"type":"tool_dispatch_intent","batch_id":"b1","tool_call_id":"c1","args":{"cmd":"ls"},"module_id":"tools"}"#,
            r#"{"type":"tool_no_dispatch","batch_id":"b1","tool_call_id":"c2","reason":{"kind":"denied","reason":"not allowed"}}"#,
            r#"{"type":"tool_result","batch_id":"b1","result":{"tool_call_id":"c1","output":{"kind":"text","text":"ok"},"is_error":false},"outcome":"completed"}"#,
            r#"{"type":"tool_batch_finished","batch_id":"b1"}"#,
        ];
        let old = log(&lines);
        for end in [6, 7, 8] {
            let framed = &old[..end];
            let today = today(framed, Some("l1"), 50).unwrap();
            let calls: Vec<&Value> = today
                .iter()
                .flat_map(|message| message["tool_calls"].as_array().into_iter().flatten())
                .collect();
            assert_eq!(calls[1]["tool_call_id"], json!("c2"), "after record {end}");
            assert_eq!(
                calls[1]["call_key"],
                json!("l1:6"),
                "today keys the denied call by its answer, after record {end}"
            );
            assert_eq!(calls[1]["dispatched_to"], Value::Null);
            assert_eq!(calls[1]["indeterminate"], json!(false));
            assert_eq!(store_of(framed, "l1"), today, "after record {end}");
        }
    }

    /// A step that repeats a call id: the first copy is sent, and the second is answered with
    /// the duplicate error without being sent. Today keys each copy by its own record, the first
    /// by its intent and the second by its answer, and the store serves the same two keys, at
    /// every cut and on the closed turn.
    #[test]
    fn a_repeated_call_id_keys_each_copy_by_its_own_record() {
        let lines = [
            RUN,
            r#"{"type":"step_started","step_id":1}"#,
            r#"{"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m1","content":[{"type":"tool_call","tool_call_id":"c1","tool_name":"bash","input":{"cmd":"ls"}},{"type":"tool_call","tool_call_id":"c1","tool_name":"bash","input":{"cmd":"pwd"}}]},"usage":{"input_tokens":3},"finish_reason":"tool_calls","retries_used":0}"#,
            r#"{"type":"tool_batch_started","batch_id":"b1","step_id":1,"planned_calls":["c1","c1"]}"#,
            r#"{"type":"tool_dispatch_intent","batch_id":"b1","tool_call_id":"c1","args":{"cmd":"ls"},"module_id":"tools"}"#,
            r#"{"type":"tool_result","batch_id":"b1","result":{"tool_call_id":"c1","output":{"kind":"text","text":"ok"},"is_error":false},"outcome":"completed"}"#,
            r#"{"type":"tool_result","batch_id":"b1","result":{"tool_call_id":"c1","output":{"kind":"error","message":"duplicate tool_call_id from provider"},"is_error":true},"outcome":"completed"}"#,
            r#"{"type":"tool_batch_finished","batch_id":"b1"}"#,
        ];
        let old = log(&lines);
        for end in [5, 6, 7, 8] {
            let framed = &old[..end];
            let today = today(framed, Some("l1"), 50).unwrap();
            let keys: Vec<&Value> = today
                .iter()
                .flat_map(|message| message["tool_calls"].as_array().into_iter().flatten())
                .map(|call| &call["call_key"])
                .collect();
            if end >= 7 {
                assert_eq!(
                    keys,
                    [&json!("l1:5"), &json!("l1:7")],
                    "today keys each copy by its own record, after record {end}"
                );
            }
            assert_eq!(store_of(framed, "l1"), today, "after record {end}");
        }
        // The conversion keys the second copy's outcome by its answer, and its turn's facts name
        // both keys.
        use broca_session_store::record::Record;
        let converted = broca_session_convert::convert(&old, Some("l1")).unwrap();
        let outcome_keys: Vec<Option<&str>> = converted
            .records
            .iter()
            .filter_map(|record| match record {
                Record::ToolOutcome(outcome) => Some(outcome.call_key.as_deref()),
                _ => None,
            })
            .collect();
        assert_eq!(outcome_keys, [None, Some("l1:7")]);
        let turn = converted
            .records
            .iter()
            .rev()
            .find_map(|record| match record {
                Record::TurnFacts(turn) => Some(turn),
                _ => None,
            })
            .expect("the turn has facts");
        let turn_keys: Vec<&str> = turn
            .calls
            .iter()
            .map(|call| call.call_key.as_str())
            .collect();
        assert_eq!(turn_keys, ["l1:5", "l1:7"]);
    }

    /// A batch's results enter the transcript only once the batch closes, on both storages:
    /// broca may still join a delivered note to the last result as the batch closes, so a read
    /// between the batch's last result and its end shows the assistant message without them,
    /// and a read after the end shows them. Every page message and each page's state agree with
    /// today's at each cut.
    #[test]
    fn a_batch_s_results_are_served_once_it_closes() {
        let lines = [
            RUN,
            r#"{"type":"step_started","step_id":1}"#,
            r#"{"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m1","content":[{"type":"tool_call","tool_call_id":"c1","tool_name":"bash","input":{"cmd":"ls"}}]},"usage":{"input_tokens":3},"finish_reason":"tool_calls","retries_used":0}"#,
            r#"{"type":"tool_batch_started","batch_id":"b1","step_id":1,"planned_calls":["c1"]}"#,
            r#"{"type":"tool_dispatch_intent","batch_id":"b1","tool_call_id":"c1","args":{"cmd":"ls"},"module_id":"tools"}"#,
            r#"{"type":"tool_result","batch_id":"b1","result":{"tool_call_id":"c1","output":{"kind":"text","text":"ok"},"is_error":false},"outcome":"completed"}"#,
            r#"{"type":"tool_batch_finished","batch_id":"b1"}"#,
        ];
        let old = log(&lines);
        // After the intent, after the batch's last result, after its end.
        for (end, messages) in [(5, 2), (6, 2), (7, 3)] {
            let framed = &old[..end];
            let today = walk_pages(
                |from, limit| {
                    broca_core::read_session_page(
                        &broca_core::StorePageSnapshot::empty(),
                        framed,
                        Some(from),
                        Some(limit),
                    )
                    .map_err(|e| e.to_string())
                },
                7,
            )
            .unwrap();
            let converted = broca_session_convert::convert(framed, Some("l1")).unwrap();
            let store = block_on(async {
                let backend = MemoryBackend::default();
                let mut session = Session::create(
                    &backend,
                    SessionAddr("s".into()),
                    triple("s"),
                    &DeploymentKey::new(1, &[0x5a; 32]),
                    false,
                )
                .await
                .unwrap();
                for record in &converted.records {
                    session
                        .append(std::slice::from_ref(record), false)
                        .await
                        .unwrap();
                }
                walk_store_pages(&session, 7).await.unwrap()
            });
            assert_eq!(
                today.0.len(),
                messages,
                "today's messages after record {end}"
            );
            assert_eq!(store, today, "after record {end}");
        }
    }
}
