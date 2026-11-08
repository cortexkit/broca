//! Message marks (llm-runner's `MessageMarks`: `speaks_for` and a true `notice`) on a log of
//! today's storage, converted into the new storage: each record keeps its send's marks, a ride's
//! envelope keeps the agent its module spoke for, the conversion loses nothing and inverts to the
//! same log, and an `include_display_facts` read of the converted session serves the marks as
//! today's read of the log does.

use broca_module::serve::{SessionReadRequest, SessionReadResponse};
use broca_module::store_read::todays::Todays;
use broca_session_loop::parts::MemoryParts;
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::DeploymentKey;
use broca_session_store::record::Record;
use broca_session_store::session::Session;
use broca_wal::{FramedRecord, WalRecord};
use futures::executor::block_on;
use serde_json::{json, Value};

use super::inverse::{comparable, first_difference, invert, without_nulls};

const RUN: &str = r#"{"type":"run_started","run_id":"RUN","session":{"project_root":"/project","harness":"runner","session":"s"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[{"name":"bash","description":"run","input_schema":{"type":"object"},"execution_mode":"parallel"}],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hi"}]}],"origin":{"kind":"fresh"}}"#;

const LINEAGE: &str = "0b3c3c1e-1f2a-4c1d-9e57-5a0e5f0c0003";

fn log(lines: &[String]) -> Vec<FramedRecord> {
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

/// A queued send of `text` as `submission_id`, with the members `extra` adds.
fn queued(submission_id: &str, text: &str, extra: Value) -> String {
    let mut record = json!({
        "type": "queued",
        "submission_id": submission_id,
        "request": {
            "model": {"provider_module_id": "p", "model_id": "m"},
            "input": [{"role": "user", "content": [{"type": "text", "text": text}]}],
            "policy_allowed": [],
            "tool_choice": {"type": "auto"},
            "generation": {},
            "stop_when": [],
        },
        "written_at_ms": 5,
    });
    for (key, value) in extra.as_object().unwrap() {
        record[key] = value.clone();
    }
    record.to_string()
}

/// A log whose sends carry marks: the first run's input speaks for Ada; a steer marked as a
/// notice is placed at the next step boundary; an ordered send speaking for Grace rides on a
/// tool result, its envelope naming her; and a queued notice starts the second run.
fn marked() -> Vec<FramedRecord> {
    log(&[
        RUN.replace("RUN", "r1").replace(
            r#""origin""#,
            r#""message_marks":{"speaks_for":{"agent_id":"Ada"}},"origin""#,
        ),
        r#"{"type":"delivery_ordered","submission_id":"sub-q","order":{"modes":["tool_result","turn_system","steer"]}}"#.into(),
        queued(
            "sub-q",
            "note",
            json!({"send_id": "q", "message_marks": {"speaks_for": {"agent_id": "Grace"}}}),
        ),
        r#"{"type":"step_started","step_id":1}"#.into(),
        r#"{"type":"steered","input":[{"role":"user","content":[{"type":"text","text":"also this"}]}],"written_at_ms":7,"message_marks":{"notice":true}}"#.into(),
        r#"{"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m1","content":[{"type":"tool_call","tool_call_id":"c1","tool_name":"bash","input":{"cmd":"ls"}}]},"usage":{},"finish_reason":"tool_calls","retries_used":0}"#.into(),
        r#"{"type":"tool_batch_started","batch_id":"b1","step_id":1,"planned_calls":["c1"]}"#.into(),
        r#"{"type":"tool_dispatch_intent","batch_id":"b1","tool_call_id":"c1","args":{"cmd":"ls"}}"#.into(),
        r#"{"type":"tool_result","batch_id":"b1","result":{"tool_call_id":"c1","output":{"kind":"text","text":"ok"},"is_error":false},"outcome":"completed"}"#.into(),
        r#"{"type":"tool_batch_finished","batch_id":"b1"}"#.into(),
        r#"{"type":"send_delivered","submission_id":"sub-q","run_id":"r1","mode":"tool_result","tool_call_id":"c1","appended_text":"\n\nnote","written_at_ms":5,"ride":{"call_key":"lineage:7","send_id":"q","sender":"broca","speaks_for":{"agent_id":"Grace"},"forms":["tool_result","turn_system"],"form":"append"}}"#.into(),
        r#"{"type":"turn_finished","step_id":1}"#.into(),
        r#"{"type":"step_started","step_id":2}"#.into(),
        r#"{"type":"model_step_finished","step_id":2,"assistant_message":{"message_id":"m2","content":[{"type":"text","text":"done"}]},"usage":{},"finish_reason":"stop","retries_used":0}"#.into(),
        r#"{"type":"run_finished","reason":"completed","usage":{},"retries_used":0}"#.into(),
        queued("sub-q2", "later", json!({"message_marks": {"notice": true}})),
        RUN.replace("RUN", "r2")
            .replace(r#""text":"hi""#, r#""text":"later""#)
            .replace(r#""origin""#, r#""submission_id":"sub-q2","origin""#),
        r#"{"type":"step_started","step_id":1}"#.into(),
        r#"{"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m3","content":[{"type":"text","text":"ok"}]},"usage":{},"finish_reason":"stop","retries_used":0}"#.into(),
        r#"{"type":"run_finished","reason":"completed","usage":{},"retries_used":0}"#.into(),
    ])
}

/// Each record keeps its send's marks, and the ride's envelope the agent Grace: nothing is
/// lost, and the converted records invert to the same log.
#[test]
fn marks_convert_onto_their_records_and_round_trip() {
    let old = marked();
    let converted = crate::convert(&old, Some(LINEAGE)).unwrap();
    assert!(converted.lost.is_empty(), "{:?}", converted.lost);
    let ada = json!({"speaks_for": {"agent_id": "Ada"}});
    let notice = json!({"notice": true});
    let mut starts = Vec::new();
    let mut queued = Vec::new();
    let mut steers = Vec::new();
    let mut rides = Vec::new();
    for record in &converted.records {
        match record {
            Record::RunStart(start) => starts.push(start.message_marks.clone()),
            Record::QueuedSend(send) => queued.push(send.message_marks.clone()),
            Record::Steer(steer) => steers.push(steer.message_marks.clone()),
            Record::ResultAppend(append) => {
                rides.push(
                    append
                        .ride
                        .as_ref()
                        .and_then(|ride| ride.speaks_for.clone()),
                );
            }
            _ => {}
        }
    }
    assert_eq!(starts, [Some(ada), None]);
    assert_eq!(
        queued,
        [
            Some(json!({"speaks_for": {"agent_id": "Grace"}})),
            Some(notice.clone())
        ]
    );
    assert_eq!(steers, [Some(notice)]);
    assert_eq!(rides, [Some(json!({"agent_id": "Grace"}))]);
    let rebuilt: Vec<_> = invert(&converted.records, &converted.parts)
        .unwrap()
        .iter()
        .map(without_nulls)
        .collect();
    let original = comparable(&old);
    assert_eq!(rebuilt.len(), original.len());
    for (index, (a, b)) in original.iter().zip(&rebuilt).enumerate() {
        assert_eq!(first_difference(a, b, ""), None, "record {index}");
    }
}

/// The reply in each read mode to a read of the whole session.
fn replies(read: impl Fn(&SessionReadRequest) -> SessionReadResponse) -> Vec<SessionReadResponse> {
    [
        (false, false, false),
        (true, false, false),
        (false, true, false),
        (false, false, true),
        (true, true, true),
    ]
    .into_iter()
    .map(|(display, tools, rich_ui)| {
        let key = broca_protocol::BindIdentity::new("/marks", "marks", "s");
        let mut request = SessionReadRequest::new(key, Some(1), Some(64));
        request.include_display_facts = display;
        request.include_tools = tools;
        request.include_rich_ui = rich_ui;
        read(&request)
    })
    .collect()
}

/// The converted session reads as today's log does in every read mode, and its display facts
/// serve each mark today's serve: the input that speaks for Ada, the steer's and the drained
/// send's notices, and the ride's notice speaking for Grace.
#[test]
fn a_converted_session_serves_its_marks_as_todays_read_does() {
    let old = marked();
    let converted = crate::convert(&old, Some(LINEAGE)).unwrap();
    let parts = MemoryParts {
        parts: converted.parts.clone(),
    };
    let backend = MemoryBackend::default();
    let mut session = block_on(Session::create(
        &backend,
        SessionAddr("marks".into()),
        SessionTriple {
            project_root: "/marks".into(),
            harness: "marks".into(),
            session: "s".into(),
        },
        &DeploymentKey::new(1, &[0x5a; 32]),
        false,
    ))
    .unwrap();
    for record in &converted.records {
        block_on(session.append(std::slice::from_ref(record), false)).unwrap();
    }
    let today = Todays::new(&old, Some(LINEAGE));
    let todays = replies(|request| today.read(request).unwrap());
    let stored = replies(|request| {
        block_on(broca_module::store_read::serve_view(
            &session, &parts, request,
        ))
        .unwrap()
    });
    // Today's display read serves every mark the log holds.
    let display = &todays[1].messages;
    let speaking: Vec<_> = display
        .iter()
        .filter_map(|message| message.speaks_for.as_ref())
        .map(|speaks_for| speaks_for.agent_id.as_str())
        .collect();
    assert_eq!(speaking, ["Ada"]);
    assert_eq!(
        display.iter().filter(|message| message.notice).count(),
        2,
        "the steer's message and the drained send's"
    );
    let rides: Vec<_> = display
        .iter()
        .flat_map(|message| &message.notices)
        .map(|notice| notice.speaks_for.as_ref().map(|s| s.agent_id.as_str()))
        .collect();
    assert_eq!(rides, [Some("Grace")]);
    for (index, (stored, today)) in stored.iter().zip(&todays).enumerate() {
        assert_eq!(
            serde_json::to_value(&stored.messages).unwrap(),
            serde_json::to_value(&today.messages).unwrap(),
            "read mode {index}"
        );
    }
}
