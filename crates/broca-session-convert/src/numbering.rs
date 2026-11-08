//! `session-convert numbering <path>...`: for every session log, which part numbers its
//! messages carry, and where each is shown, two ways:
//! - **The runner's numbering** (llm-runner's tags): stamped on a v1 run's records, or computed
//!   by `broca_protocol::tags::number_message` from each later run's checkpoint. Today's read
//!   (`store_read::todays`) attaches it to every message it serves; off the compaction contract
//!   it is what a request shows the model, and what output references resolve `$$tc(N)`
//!   against. On the contract today's read attaches none, so it is recomputed here from the
//!   converted session, each message under its run's rules (`EntryFacts::render_tags` and
//!   `clean_reply`), continuing one session-wide high.
//! - **The compaction projection's numbering**, on the contract only: each message's
//!   `EntryFacts::tag_base` and the blocks the projection numbers
//!   (`broca_core::compaction_v2::metadata::compaction_part_numbers`). A request rendered on the
//!   contract shows the model these.
//!
//! A number is compared where the model sees it: a text block's on that block, and a tool
//! call's on its result (the projection numbers the call; the runner numbers the result). Per
//! log mode, per kind: positions both number alike, differently, or only one numbers. Also
//! checked: the converted session's `EntryFacts::runner_tag_base` against the base today's read
//! numbers each message after. The report names counts, kinds and log names only, never a log's
//! content.

use std::collections::{BTreeMap, HashMap};

use broca_module::serve::SessionReadRequest;
use broca_module::store_read::todays::Todays;
use broca_protocol::content::{ContentBlock, Message, Role};
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::session::Session;
use futures::executor::block_on;
use serde::Serialize;

use super::{convert_log, each_log, triple, write_report, Args, Item};

#[derive(Default, Serialize)]
struct Counts {
    logs: u64,
    /// Logs with at least one position the two numberings disagree on.
    logs_differing: u64,
    /// Per kind: numbered alike, numbered differently, numbered only by the runner, numbered
    /// only by the compaction projection.
    equal: BTreeMap<String, u64>,
    differ: BTreeMap<String, u64>,
    runner_only: BTreeMap<String, u64>,
    projection_only: BTreeMap<String, u64>,
    /// Per kind, the blocks today's read numbers.
    today_numbered: BTreeMap<String, u64>,
    /// Messages today's read numbers after a base, and how many the converted session's
    /// `runner_tag_base` names the same base for.
    today_bases: u64,
    runner_base_equal: u64,
    /// A few differing positions: log, ordinal, kind, the runner's number, the projection's.
    examples: Vec<String>,
}

#[derive(Default, Serialize)]
struct NumberingReport {
    logs: u64,
    /// By log mode: `contract` (a compaction provider's plan), `computed` (the runner's
    /// numbering is computed: a v2 run), `stamped` (it is stamped on the records: v1 runs only),
    /// `none` (today's read numbers nothing).
    modes: BTreeMap<String, Counts>,
    failures: BTreeMap<String, u64>,
}

/// Where a number is shown: a block of a message, or a call's result.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Shown {
    Block(u64, usize),
    Call(String),
}

/// A block's kind as the counts name it.
fn kind(role: Role, block: &ContentBlock) -> String {
    let role = match role {
        Role::User => "user",
        Role::Assistant => "assistant",
        Role::Tool => "tool",
        Role::System => "system",
    };
    let block = match block {
        ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. }
            if text.is_empty() =>
        {
            "empty_text"
        }
        ContentBlock::Text { .. } | ContentBlock::TextWithState { .. } => "text",
        ContentBlock::Reasoning { .. } | ContentBlock::RedactedReasoning { .. } => "reasoning",
        ContentBlock::ToolCall(_) => "tool_call",
        ContentBlock::ToolResult(result) => match &result.output {
            broca_protocol::content::ToolOutput::Json { .. } => "tool_result_json",
            broca_protocol::content::ToolOutput::Text { .. } => "tool_result_text",
            _ => "tool_result_other",
        },
        ContentBlock::Media(_) => "media",
        _ => "other",
    };
    format!("{role}/{block}")
}

/// Where each of `message`'s `numbers` is shown, with the kind of the block that carries it.
fn shown(
    message: &Message,
    ordinal: u64,
    numbers: &[Option<u64>],
) -> HashMap<Shown, (u64, String)> {
    let mut out = HashMap::new();
    for (index, (block, number)) in message.content.iter().zip(numbers).enumerate() {
        let Some(number) = number else { continue };
        let at = match block {
            ContentBlock::ToolCall(call) => Shown::Call(call.tool_call_id.0.clone()),
            ContentBlock::ToolResult(result) => Shown::Call(result.tool_call_id.0.clone()),
            _ => Shown::Block(ordinal, index),
        };
        out.insert(at, (*number, kind(message.role, block)));
    }
    out
}

fn failed(report: &mut NumberingReport, what: String) {
    *report
        .failures
        .entry(what.chars().take(100).collect())
        .or_default() += 1;
}

pub(super) fn run(args: &Args) {
    let mut report = NumberingReport::default();
    let deployment = crate::deployment();
    each_log(args, |item| {
        let Item::Log {
            name, old: Ok(old), ..
        } = item
        else {
            return;
        };
        report.logs += 1;
        let records: Vec<broca_wal::WalRecord> = old.iter().map(|f| f.record.clone()).collect();
        let contract = broca_core::is_mode3_log(&records);
        let todays = Todays::new(&old.records, old.lineage.as_deref());
        let mut today = Vec::new();
        let mut from = 1;
        loop {
            let key = broca_protocol::BindIdentity::new("/numbering", "numbering", name.as_str());
            match todays.read(&SessionReadRequest::new(key, Some(from), Some(64))) {
                Ok(page) => {
                    let next = page.next_from_ordinal;
                    today.extend(page.messages);
                    match next {
                        Some(next) if next > from => from = next,
                        _ => break,
                    }
                }
                Err(e) => return failed(&mut report, format!("today's pages: {e:?}")),
            }
        }
        let converted = match convert_log(&old) {
            Ok(converted) => converted,
            Err(e) => return failed(&mut report, format!("convert: {}", e.0)),
        };
        let backend = MemoryBackend::default();
        let entries = block_on(async {
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
            session
                .entries(1, usize::MAX)
                .await
                .map_err(|e| format!("entries: {e:?}"))
        });
        let entries = match entries {
            Ok(entries) => entries,
            Err(e) => return failed(&mut report, e),
        };
        let tagged: Vec<_> = today
            .iter()
            .filter_map(|served| served.message.part_tags())
            .collect();
        let mode = if contract {
            "contract"
        } else if tagged.iter().any(|tags| tags.computed.is_some()) {
            "computed"
        } else if tagged
            .iter()
            .any(|tags| tags.parts.iter().any(Option::is_some))
        {
            "stamped"
        } else {
            "none"
        };
        let counts = report.modes.entry(mode.into()).or_default();
        counts.logs += 1;
        // The runner's numbering: today's read's, or on the contract, recomputed.
        let mut runner = HashMap::new();
        let mut projection = HashMap::new();
        let mut high = 0;
        for served in &today {
            let numbers: Vec<Option<u64>> = served
                .message
                .part_tags()
                .map(|tags| tags.parts.clone())
                .unwrap_or_default();
            for (block, number) in served.message.content.iter().zip(&numbers) {
                if number.is_some() {
                    *counts
                        .today_numbered
                        .entry(kind(served.message.role, block))
                        .or_default() += 1;
                }
            }
            let entry = entries.iter().find(|entry| entry.ordinal == served.ordinal);
            if let Some(base) = served
                .message
                .part_tags()
                .and_then(|tags| tags.computed.as_ref())
                .map(|computed| computed.base)
            {
                counts.today_bases += 1;
                let stored = entry
                    .and_then(|entry| entry.state.facts.as_ref())
                    .and_then(|facts| facts.runner_tag_base);
                counts.runner_base_equal += u64::from(stored == Some(base));
                if stored != Some(base) && counts.examples.len() < 20 {
                    counts.examples.push(format!(
                        "{name} ordinal {} of {}: today's base {base}, stored {stored:?}",
                        served.ordinal,
                        today.len()
                    ));
                }
            }
            if !contract {
                runner.extend(shown(&served.message, served.ordinal, &numbers));
                continue;
            }
            let Some((entry, facts)) =
                entry.and_then(|entry| Some((entry, entry.state.facts.as_ref()?)))
            else {
                continue;
            };
            let mut numbered = entry.message.clone();
            let rules = broca_protocol::tags::ComputedRules {
                render: facts.render_tags,
                clean: facts.clean_reply,
            };
            if broca_protocol::tags::number_message(&mut numbered, rules, &mut high).is_err() {
                return failed(&mut report, "runner numbering overflowed".into());
            }
            let numbers = numbered
                .part_tags()
                .map(|tags| tags.parts.clone())
                .unwrap_or_default();
            runner.extend(shown(&entry.message, entry.ordinal, &numbers));
            let numbers = broca_core::compaction_v2::metadata::compaction_part_numbers(
                &entry.message,
                facts.tag_base,
            );
            projection.extend(shown(&entry.message, entry.ordinal, &numbers));
        }
        let counts = report.modes.entry(mode.into()).or_default();
        if !contract {
            return;
        }
        let mut differs = false;
        let mut positions: Vec<&Shown> = runner.keys().chain(projection.keys()).collect();
        positions.sort_by_key(|at| format!("{at:?}"));
        positions.dedup();
        for at in positions {
            let (a, b) = (runner.get(at), projection.get(at));
            let kind = a.or(b).map(|(_, kind)| kind.clone()).unwrap_or_default();
            let bucket = match (a, b) {
                (Some((a, _)), Some((b, _))) if a == b => &mut counts.equal,
                (Some(_), Some(_)) => &mut counts.differ,
                (Some(_), None) => &mut counts.runner_only,
                (None, Some(_)) => &mut counts.projection_only,
                (None, None) => continue,
            };
            *bucket.entry(kind.clone()).or_default() += 1;
            if a.map(|(n, _)| n) != b.map(|(n, _)| n) {
                differs = true;
                if counts.examples.len() < 20 {
                    counts.examples.push(format!(
                        "{name} {at:?} {kind}: runner {:?}, projection {:?}",
                        a.map(|(n, k)| (n, k)),
                        b.map(|(n, k)| (n, k))
                    ));
                }
            }
        }
        counts.logs_differing += u64::from(differs);
    });
    write_report(&report, args.out_file.as_deref());
}
