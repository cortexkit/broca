//! The cost of a `session.read` page with every opt-in on a long session in the new storage,
//! counted at the backend: the opt-ins read nothing a default read of the page doesn't, and a
//! page reads at 150,000 messages no more than twice what it reads at 1,500.

use broca_wal::{FramedRecord, WalRecord};

use crate::read_cost::{page_reads, store, store_then_seal, stored_objects};

/// A log of `runs` runs, `per_run` messages each: every run's input, one tool call and its
/// result, then text replies.
fn synthetic(runs: usize, per_run: usize) -> Vec<FramedRecord> {
    synthetic_log(runs, per_run, false, false)
}

/// A frozen plan that names a first-protocol compaction provider, as `RunStarted` JSON spells
/// it.
fn compacting_manifest() -> serde_json::Value {
    serde_json::json!({
        "plan": serde_json::json!({"compaction_item": {"provider": "compactor", "params": {}}})
            .to_string(),
        "plan_identity": "0".repeat(64),
        "composition_digest": "0".repeat(64),
        "fetches": [],
        "tools": [],
        "system_text": "",
        "system_digest": "0".repeat(64),
        "join_version": 1,
        "role_versions": {"compaction-provider": "v1"},
    })
}

/// A first-protocol view, version `version`, that replaces every message before ordinal
/// `first_kept` (counting from 0) with nothing.
fn folding_view(version: usize, first_kept: usize) -> String {
    let view = serde_json::json!({
        "session_ref": "s", "view_version": version, "lineage_id": "lineage",
        "covers_through": {"ordinal": first_kept - 1, "mid": format!("m{}", first_kept - 1)},
        "ranges": [{"kind": "replace", "from": 0, "to": first_kept, "bytes": "[]"}],
        "digest": "0".repeat(64),
    });
    serde_json::json!({
        "type": "compaction_view_applied",
        "provider": "compactor",
        "view_version": version,
        "view": view.to_string(),
        "choice": {"kind": "applied"},
    })
    .to_string()
}

/// [`synthetic`], and when `refs`, its messages carry the times history serves, its runs number
/// their parts under the computed tag rules, and each run's last reply names part numbers 1, 5
/// and 9, which the first run's input and replies take, so output references resolve them from
/// the session's oldest sealed segment, each at its own message. With `views`, its plan names a
/// first-protocol compaction provider, and before every run's last reply but the first run's a
/// view folds every earlier run away, so that reply's request never showed those parts.
fn synthetic_log(runs: usize, per_run: usize, refs: bool, views: bool) -> Vec<FramedRecord> {
    let (tags, written) = if refs {
        (
            r#","render_tags":true,"computed_tags":{"checkpoint":0}"#,
            r#","written_at_ms":100"#,
        )
    } else {
        ("", "")
    };
    let manifest = if views {
        format!(r#","manifest":{}"#, compacting_manifest())
    } else {
        String::new()
    };
    let mut lines = Vec::new();
    for run in 0..runs {
        lines.push(format!(
            r#"{{"type":"run_started","run_id":"run-{run}","session":{{"project_root":"/project","harness":"runner","session":"s"}},"config":{{"model":{{"provider_module_id":"p","model_id":"m"}},"tools":[{{"name":"bash","description":"run","input_schema":{{"type":"object"}},"execution_mode":"parallel"}}],"tool_choice":{{"type":"auto"}},"generation":{{}},"response_format":{{"type":"text"}},"context_epoch":0,"renderer_version":1{tags}}},"input":[{{"role":"user","content":[{{"type":"text","text":"question {run}"}}]}}],"origin":{{"kind":"fresh"}}{written}{manifest}}}"#
        ));
        lines.push(r#"{"type":"step_started","step_id":0}"#.to_owned());
        lines.push(format!(
            r#"{{"type":"model_step_finished","step_id":0,"assistant_message":{{"message_id":"m-{run}-0","content":[{{"type":"tool_call","tool_call_id":"c-{run}","tool_name":"bash","input":{{}}}}]}},"usage":{{"input_tokens":1}},"finish_reason":"tool_calls","retries_used":0{written}}}"#
        ));
        lines.push(format!(
            r#"{{"type":"tool_batch_started","batch_id":"b-{run}","step_id":0,"planned_calls":["c-{run}"]}}"#
        ));
        lines.push(format!(
            r#"{{"type":"tool_dispatch_intent","batch_id":"b-{run}","tool_call_id":"c-{run}","args":{{}},"dispatched_at_ms":100}}"#
        ));
        lines.push(format!(
            r#"{{"type":"tool_result","batch_id":"b-{run}","result":{{"tool_call_id":"c-{run}","output":{{"kind":"text","text":"ok"}},"is_error":false}},"outcome":"completed","finished_at_ms":150}}"#
        ));
        lines.push(format!(
            r#"{{"type":"tool_batch_finished","batch_id":"b-{run}"}}"#
        ));
        for step in 1..per_run - 2 {
            let last = step == per_run - 3;
            let text = if refs && last {
                "see $$tc(1), $$tc(5) and $$tc(9)".to_owned()
            } else {
                format!("reply {step}")
            };
            if views && last && run > 0 {
                lines.push(folding_view(run, run * per_run));
            }
            lines.push(format!(r#"{{"type":"step_started","step_id":{step}}}"#));
            lines.push(format!(
                r#"{{"type":"model_step_finished","step_id":{step},"assistant_message":{{"message_id":"m-{run}-{step}","content":[{{"type":"text","text":"{text}"}}]}},"usage":{{"input_tokens":1}},"finish_reason":"stop","retries_used":0{written}}}"#
            ));
        }
        lines.push(
            r#"{"type":"run_finished","reason":"completed","usage":{},"retries_used":0}"#
                .to_owned(),
        );
    }
    lines
        .iter()
        .enumerate()
        .map(|(index, line)| FramedRecord {
            seq: index as u64 + 1,
            fence: 0,
            record: serde_json::from_str::<WalRecord>(line)
                .unwrap_or_else(|error| panic!("line {index}: {error}")),
        })
        .collect()
}

/// What pages from `from` read, over a session of `runs` runs of `per_run` messages: the most
/// sealed objects any page read with every opt-in, the most bytes, the most objects the opt-ins
/// added to a default read of the same page, and the bytes the session stores.
fn worst(runs: usize, per_run: usize, from: &[u64]) -> (usize, u64, usize, u64) {
    let started = std::time::Instant::now();
    let old = synthetic(runs, per_run);
    let stored = store(&old, Some("0b3c3c1e-1f2a-4c1d-9e57-5a0e5f0c0001"), "long").unwrap();
    assert_eq!(stored.messages, (runs * per_run) as u64);
    assert!(
        stored.seals + 1 >= runs,
        "every run but the last is sealed: {} seals",
        stored.seals
    );
    let (objects, bytes) = stored_objects(&stored);
    let reads = page_reads(&stored, from, 32, true).unwrap();
    let plain = page_reads(&stored, from, 32, false).unwrap();
    let most = reads.iter().map(|read| read.objects.len()).max().unwrap();
    let most_bytes = reads.iter().map(|read| read.object_bytes).max().unwrap();
    let added = reads
        .iter()
        .zip(&plain)
        .map(|(all, plain)| all.objects.len().saturating_sub(plain.objects.len()))
        .max()
        .unwrap();
    eprintln!(
        "messages={} seals={} objects={objects} bytes={bytes} objects_per_page={:?} \
         distinct_objects_per_page={:?} default_objects_per_page={:?} \
         most_bytes_per_page={most_bytes} log_reads={:?} elapsed_ms={}",
        stored.messages,
        stored.seals,
        reads
            .iter()
            .map(|read| read.objects.len())
            .collect::<Vec<_>>(),
        reads
            .iter()
            .map(|read| read
                .objects
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len())
            .collect::<Vec<_>>(),
        plain
            .iter()
            .map(|read| read.objects.len())
            .collect::<Vec<_>>(),
        reads.iter().map(|read| read.log_reads).collect::<Vec<_>>(),
        started.elapsed().as_millis()
    );
    (most, most_bytes, added, bytes)
}

/// Pages from the start, across a run boundary, the middle and the end of a session of
/// `messages` messages in runs of `per_run`.
fn pages(messages: u64, per_run: u64) -> Vec<u64> {
    vec![1, per_run - 10, messages / 2 + 7, messages - 31]
}

#[test]
fn a_page_read_with_every_opt_in_stays_bounded_from_1500_to_150k_messages() {
    // At most 64 earlier runs are kept for rendering past a seal, so the long session takes
    // fewer, longer runs.
    let (small, _, small_added, _) = worst(3, 500, &pages(1_500, 500));
    let (large, large_bytes, large_added, total) = worst(50, 3_000, &pages(150_000, 3_000));
    // The opt-ins read nothing a default read of the same page doesn't: every fact they add is
    // recorded beside the page's own messages, or in the session's folds.
    assert_eq!(
        (small_added, large_added),
        (0, 0),
        "objects the opt-ins added to a page's reads at 1.5k and 150k messages"
    );
    // A page reads the directory and the segment blocks its messages lie in, which grow with
    // the depth of the session's directory, not its length: a hundred times the messages may
    // not double a page's reads. Reading the whole history at 150k messages would fetch every
    // one of its hundreds of stored objects.
    assert!(
        large <= 2 * small,
        "a page read {large} objects at 150k messages, {small} at 1.5k"
    );
    assert!(
        large_bytes * 10 < total,
        "a page read {large_bytes} bytes of the {total} stored"
    );
}

/// The history read cost at one size: for every page in `from` and both forms, what the page
/// read beyond loading the session's view.
struct HistoryCost {
    /// The most object fetches, distinct objects and bytes any page read.
    most: usize,
    most_distinct: usize,
    most_bytes: u64,
    /// The objects the session stores.
    total_objects: usize,
    /// Display rows whose output references name a part.
    referring_rows: usize,
}

/// `session.history` pages from each ordinal in `from`, 32 messages long, raw and display, over
/// a session of `runs` runs of `per_run` messages whose runs' last replies name part number 1.
/// With `compare`, every page is also checked equal to today's reply for the same log. With
/// `views`, the log's later replies were written under first-protocol views (see
/// [`synthetic_log`]), whose records the session's seals have moved into sealed history.
fn history_cost(
    runs: usize,
    per_run: usize,
    from: &[u64],
    compare: bool,
    views: bool,
) -> HistoryCost {
    use crate::stored_history::{history, StoredHistory};
    let old = synthetic_log(runs, per_run, true, views);
    let lineage = "0b3c3c1e-1f2a-4c1d-9e57-5a0e5f0c0002";
    // A converted session's records take the seqs the converter numbered them by, which a view's
    // seq names; the cost bar seals as runs end, as the run loop does.
    let stored = if views {
        store_then_seal(&old, Some(lineage), "history")
    } else {
        store(&old, Some(lineage), "history")
    }
    .unwrap();
    assert_eq!(stored.messages, (runs * per_run) as u64);
    // Every run but the last is sealed, with the view records before its input.
    assert!(stored.seals + 1 >= runs, "{} seals", stored.seals);
    let (total_objects, total) = stored_objects(&stored);
    let view = futures::executor::block_on(broca_session_store::session::Session::view(
        &stored.backend,
        stored.addr.clone(),
        &stored.triple,
        &stored.deployment,
    ))
    .unwrap();
    let head = view.head();
    let today = broca_module::store_read::history_sides::TodaysHistory::new(
        &old,
        Some(lineage),
        broca_session_store::codec::hex(&head.incarnation.0),
        broca_module::store_exposure::served_history_version(head.history_version),
    )
    .unwrap();
    let handler = StoredHistory {
        session: &view,
        parts: &stored.parts,
    };
    let mut cost = HistoryCost {
        most: 0,
        most_distinct: 0,
        most_bytes: 0,
        total_objects,
        referring_rows: 0,
    };
    let mut objects_per_page = Vec::new();
    for from in from {
        for form in ["raw", "display"] {
            let params = serde_json::json!({ "form": form, "from_ordinal": from, "limit": 32 });
            stored.backend.take();
            let page = history(&handler, "history", &params).unwrap();
            let reads = stored.backend.take();
            let distinct = reads
                .objects
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len();
            objects_per_page.push((reads.objects.len(), distinct));
            cost.most = cost.most.max(reads.objects.len());
            cost.most_distinct = cost.most_distinct.max(distinct);
            cost.most_bytes = cost.most_bytes.max(reads.object_bytes);
            let rows = page["messages"].as_array().unwrap();
            assert_eq!(
                rows[0]["ordinal"],
                serde_json::json!(from),
                "{form} from {from}"
            );
            cost.referring_rows += rows
                .iter()
                .filter(|row| row["row"]["output_refs"]["count"].as_u64() > Some(0))
                .count();
            if compare {
                assert_eq!(
                    page,
                    history(&today, "history", &params).unwrap(),
                    "{form} history from {from} differs from today's"
                );
            }
        }
    }
    eprintln!(
        "messages={} seals={} objects={total_objects} bytes={total} \
         (fetches, distinct objects) per page={objects_per_page:?} \
         most_bytes_per_page={} referring_rows={}",
        stored.messages, stored.seals, cost.most_bytes, cost.referring_rows
    );
    cost
}

#[test]
fn a_history_page_stays_bounded_from_15k_to_150k_messages() {
    // Pages from the start, across the first run's end and a middle run's end (each ending in a
    // reply that names parts of the first run), and the newest page.
    let pages = |runs: u64, per_run: u64| {
        vec![
            1,
            per_run - 10,
            per_run * (runs / 2) - 10,
            runs * per_run - 31,
        ]
    };
    let small = history_cost(5, 3_000, &pages(5, 3_000), true, false);
    let large = history_cost(50, 3_000, &pages(50, 3_000), false, false);
    // Each size's pages hold three replies that name parts, found in the oldest segment.
    assert_eq!((small.referring_rows, large.referring_rows), (3, 3));
    // A page reads the directory and the blocks its own messages lie in; an output reference
    // adds the segments a search for its part number probes, whose count grows with the depth
    // of the session's directory, not its length, and the run start that names its rules. Ten
    // times the messages may not double a page's fetches or bytes, where a read of the whole
    // history would grow tenfold, and a page reads a small share of what the session stores.
    assert!(
        large.most <= 2 * small.most,
        "a history page fetched {} objects at 150k messages, {} at 15k",
        large.most,
        small.most
    );
    assert!(
        large.most_bytes <= 2 * small.most_bytes,
        "a history page read {} bytes at 150k messages, {} at 15k",
        large.most_bytes,
        small.most_bytes
    );
    assert!(
        large.most_distinct * 4 < large.total_objects,
        "a history page read {} of the {} objects stored",
        large.most_distinct,
        large.total_objects
    );
}

/// Output references check a named part against the first-protocol view the writing step's
/// request was rendered under, read from the view event's seq, as today's read finds it in the
/// log: after the seals that moved every view's record into sealed history, each page of both
/// forms is today's. The first run's reply names parts of its own run in a request that showed
/// them; every later run's reply names them in a request a view had folded the first run out of.
#[test]
fn output_refs_read_the_writing_steps_legacy_view_after_a_seal() {
    let cost = history_cost(4, 40, &[1, 30, 70, 110, 129], true, true);
    assert_eq!(cost.referring_rows, 4);
}

/// Every message the store serves carries the part numbers today's read attaches to it, and
/// every part number is found where today's read numbers it (`store_output_refs::tag_location`),
/// in sealed history and in the window: output references then name the same parts on both
/// storages without reading past the parts they name. A number no part takes is found nowhere.
#[test]
fn part_numbers_are_found_where_todays_read_numbers_them() {
    use broca_module::serve::SessionReadRequest;
    use std::collections::BTreeMap;
    let lineage = "0b3c3c1e-1f2a-4c1d-9e57-5a0e5f0c0003";
    let old = synthetic_log(3, 40, true, false);
    let stored = store_then_seal(&old, Some(lineage), "numbers").unwrap();
    // Every run's input is a cut, so the window holds the last run only.
    assert!(stored.seals >= 2, "{} seals", stored.seals);
    let view = futures::executor::block_on(broca_session_store::session::Session::view(
        &stored.backend,
        stored.addr.clone(),
        &stored.triple,
        &stored.deployment,
    ))
    .unwrap();
    let today = broca_module::store_read::todays::Todays::new(&old, Some(lineage));
    let key = broca_protocol::BindIdentity::new("/numbers", "numbers", "numbers");
    let request = SessionReadRequest::new(key, Some(1), Some(stored.messages));
    let expected = today.read(&request).unwrap();
    let served = futures::executor::block_on(broca_module::store_read::serve_view(
        &view,
        &stored.parts,
        &request,
    ))
    .unwrap();
    assert_eq!(served.messages.len(), 120);
    let mut numbered = BTreeMap::new();
    for (served, today) in served.messages.iter().zip(&expected.messages) {
        let tags = today.message.part_tags();
        assert!(tags.is_some(), "today numbers message {}", today.ordinal);
        assert_eq!(
            served.message.part_tags(),
            tags,
            "message {}",
            today.ordinal
        );
        for (index, number) in tags.unwrap().tagged() {
            numbered.insert(number, (today.ordinal - 1, index));
        }
    }
    assert!(numbered.len() > 100, "{} numbered parts", numbered.len());
    let high = *numbered.keys().last().unwrap();
    for number in 1..=high + 2 {
        let location = futures::executor::block_on(broca_module::store_output_refs::tag_location(
            &view,
            &stored.parts,
            number,
        ))
        .unwrap();
        assert_eq!(
            location.found,
            numbered.get(&number).copied(),
            "part number {number}"
        );
    }
}
