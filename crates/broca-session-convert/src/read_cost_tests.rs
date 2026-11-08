//! The cost of a `session.read` page with every opt-in on a long session in the new storage,
//! counted at the backend: the opt-ins read nothing a default read of the page doesn't, and a
//! page reads at 150,000 messages no more than twice what it reads at 1,500.

use broca_wal::{FramedRecord, WalRecord};

use crate::read_cost::{page_reads, store, stored_objects};

/// A log of `runs` runs, `per_run` messages each: every run's input, one tool call and its
/// result, then text replies.
fn synthetic(runs: usize, per_run: usize) -> Vec<FramedRecord> {
    let mut lines = Vec::new();
    for run in 0..runs {
        lines.push(format!(
            r#"{{"type":"run_started","run_id":"run-{run}","session":{{"project_root":"/project","harness":"runner","session":"s"}},"config":{{"model":{{"provider_module_id":"p","model_id":"m"}},"tools":[{{"name":"bash","description":"run","input_schema":{{"type":"object"}},"execution_mode":"parallel"}}],"tool_choice":{{"type":"auto"}},"generation":{{}},"response_format":{{"type":"text"}},"context_epoch":0,"renderer_version":1}},"input":[{{"role":"user","content":[{{"type":"text","text":"question {run}"}}]}}],"origin":{{"kind":"fresh"}}}}"#
        ));
        lines.push(r#"{"type":"step_started","step_id":0}"#.to_owned());
        lines.push(format!(
            r#"{{"type":"model_step_finished","step_id":0,"assistant_message":{{"message_id":"m-{run}-0","content":[{{"type":"tool_call","tool_call_id":"c-{run}","tool_name":"bash","input":{{}}}}]}},"usage":{{"input_tokens":1}},"finish_reason":"tool_calls","retries_used":0}}"#
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
            lines.push(format!(r#"{{"type":"step_started","step_id":{step}}}"#));
            lines.push(format!(
                r#"{{"type":"model_step_finished","step_id":{step},"assistant_message":{{"message_id":"m-{run}-{step}","content":[{{"type":"text","text":"reply {step}"}}]}},"usage":{{"input_tokens":1}},"finish_reason":"stop","retries_used":0}}"#
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
