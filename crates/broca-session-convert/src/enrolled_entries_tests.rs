//! A session converted from today's storage that then enrolled in the current compaction
//! contract, read through `session.entries` as the route pages it.

use broca_core::compaction_v2::protocol::read::{Detail, ReadRequest, Select};
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::DeploymentKey;
use broca_session_store::record::Record;
use broca_session_store::session::Session;
use broca_wal::{FramedRecord, WalRecord};
use futures::executor::block_on;
use serde_json::{json, Value};

use crate::convert;

fn log(lines: &[String]) -> Vec<FramedRecord> {
    lines
        .iter()
        .enumerate()
        .map(|(i, line)| FramedRecord {
            seq: i as u64 + 1,
            fence: 0,
            record: serde_json::from_str::<WalRecord>(line)
                .unwrap_or_else(|e| panic!("fixture line {i}: {e}")),
        })
        .collect()
}

/// A run of today's log, with `input` as its input messages.
fn run(id: &str, input: Value) -> String {
    json!({
        "type": "run_started",
        "run_id": id,
        "session": {"project_root": "/project", "harness": "runner", "session": "s"},
        "config": {
            "model": {"provider_module_id": "p", "model_id": "m"},
            "tools": [],
            "tool_choice": {"type": "auto"},
            "generation": {},
            "response_format": {"type": "text"},
            "context_epoch": 0,
            "renderer_version": 1,
        },
        "input": input,
        "origin": {"kind": "fresh"},
    })
    .to_string()
}

/// A model step answering `text`, then the run's end.
fn answered(id: &str, text: &str) -> [String; 2] {
    [
        json!({
            "type": "model_step_finished",
            "step_id": 1,
            "assistant_message": {"message_id": id, "content": [{"type": "text", "text": text}]},
            "usage": {},
            "retries_used": 0,
        })
        .to_string(),
        r#"{"type":"run_finished","reason":"completed","usage":{},"retries_used":0}"#.to_owned(),
    ]
}

fn text(role: &str, text: &str) -> Value {
    json!({"role": role, "content": [{"type": "text", "text": text}]})
}

/// Today's log of two runs: the first's input opens with the session's global system message
/// and carries a turn-scoped note after the person's message. Ordinals: 1 the global system
/// message, 2 the person's, 3 the note, 4 the answer, then 5 and 6.
fn todays_log() -> Vec<FramedRecord> {
    let mut note = text("system", "a turn-scoped note");
    note["turn_system"] = json!(true);
    let mut lines = vec![run(
        "r1",
        json!([
            text("system", "You are terse."),
            text("user", "read a"),
            note
        ]),
    )];
    lines.extend(answered("m1", "ok"));
    lines.push(run("r2", json!([text("user", "more")])));
    lines.extend(answered("m2", "fine"));
    log(&lines)
}

/// The session converted from [`todays_log`], enrolled as an enrolling run enrolls it: its
/// decisions made by the run loop's own rule, stored, and named by a `CompactionEnrolled` record
/// in the batch of the enrolling run's start.
async fn enrolled(backend: &MemoryBackend) -> Session<&MemoryBackend> {
    let converted = convert(&todays_log(), None).unwrap();
    assert!(converted.lost.is_empty(), "{:?}", converted.lost);
    let mut session = Session::create(
        backend,
        SessionAddr("0123456789abcdef".into()),
        SessionTriple {
            project_root: "/project".into(),
            harness: "runner".into(),
            session: "s".into(),
        },
        &DeploymentKey::new(1, &[7; 32]),
        true,
    )
    .await
    .unwrap();
    session.append(&converted.records, true).await.unwrap();
    let decided = broca_session_loop::enroll::decide(&session, 0)
        .await
        .unwrap();
    assert_eq!(decided.through_ordinal, 6);
    assert!(decided.entries.entries[0].suppressed);
    assert!(decided.entries.entries[1..]
        .iter()
        .all(|entry| !entry.suppressed));
    let overlay = session.put_enrollment(&decided.entries).await.unwrap();
    let mut start = converted
        .records
        .iter()
        .rev()
        .find_map(|record| match record {
            Record::RunStart(start) => Some(start.as_ref().clone()),
            _ => None,
        })
        .unwrap();
    start.run_id = "r3".into();
    start.send_id = Some("send-r3".into());
    start.compaction_plan_version = Some(2);
    session
        .append(
            &[
                Record::RunStart(Box::new(start)),
                Record::CompactionEnrolled(decided.record(overlay)),
            ],
            true,
        )
        .await
        .unwrap();
    assert!(session.operations.enrolled.is_some());
    session
}

/// Every entry `session.entries` serves, read by `after` in pages of `limit`, and the newest
/// entry each page's head names.
async fn paged(session: &Session<&MemoryBackend>, limit: u64) -> (Vec<Value>, Vec<Value>) {
    let mut entries = Vec::new();
    let mut newest = Vec::new();
    let mut after = None;
    loop {
        let request = ReadRequest::new(
            Select::After {
                after_ordinal: after,
                limit: Some(limit),
            },
            Detail::Full,
        );
        let page = broca_module::store_entries::entries_page(session, &request)
            .await
            .unwrap();
        let page = serde_json::to_value(&page).unwrap();
        let served = page["entries"].as_array().unwrap();
        assert!(served.len() as u64 <= limit, "{page}");
        entries.extend(served.iter().cloned());
        newest.push(page["head"]["newest"].clone());
        match page["next_after"].as_u64() {
            Some(next) => after = Some(next),
            None => return (entries, newest),
        }
    }
}

/// `session.entries` on the enrolled session serves every message, paged by `after` with any
/// limit: ordinals from 1 with no gap, the suppressed global system message as a system entry
/// with no parts at ordinal 1, whose digest is the contract's digest of such an entry at every
/// read, and the turn-scoped note as served before. Each page's head names the newest entry.
#[test]
fn an_enrolled_converted_session_pages_every_message_with_no_gap() {
    let backend = MemoryBackend::default();
    block_on(async {
        let session = enrolled(&backend).await;
        let (whole, _) = paged(&session, 100).await;
        let ordinals: Vec<u64> = whole
            .iter()
            .map(|entry| entry["ordinal"].as_u64().unwrap())
            .collect();
        assert_eq!(ordinals, (1..=6).collect::<Vec<_>>());
        let suppressed = &whole[0];
        assert_eq!(suppressed["role"], "system", "{suppressed}");
        assert_eq!(suppressed["parts"], json!([]), "{suppressed}");
        assert!(suppressed.get("notice").is_none(), "{suppressed}");
        assert_eq!(
            suppressed["digest"],
            cortexkit_role_compaction::read::entry_digest(
                cortexkit_role_compaction::read::Role::System,
                None,
                &[],
            )
        );
        let note = &whole[2];
        assert_eq!(note["role"], "system", "{note}");
        assert_eq!(note["parts"][0]["text"], "a turn-scoped note", "{note}");
        for limit in [1, 2, 3] {
            let (entries, newest) = paged(&session, limit).await;
            assert_eq!(entries, whole, "pages of {limit}");
            let last = whole.last().unwrap();
            for cursor in newest {
                assert_eq!(cursor["ordinal"], last["ordinal"], "pages of {limit}");
                assert_eq!(cursor["mid"], last["mid"], "pages of {limit}");
                assert_eq!(cursor["digest"], last["digest"], "pages of {limit}");
            }
        }
    });
}
