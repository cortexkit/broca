use broca_session_store::record::{CallOutcome, Placement, Record};
use broca_wal::{FramedRecord, WalRecord};

use super::convert;

const RUN: &str = r#"{"type":"run_started","run_id":"RUN","session":{"project_root":"/project","harness":"runner","session":"s"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[{"name":"bash","description":"run","input_schema":{"type":"object"},"execution_mode":"parallel"}],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hi"}]}],"origin":{"kind":"fresh"}}"#;

fn log(lines: &[&str]) -> Vec<FramedRecord> {
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

fn names(records: &[Record]) -> Vec<&'static str> {
    records.iter().map(Record::name).collect()
}

#[test]
fn a_tool_step_splits_into_transcript_and_ledger_records() {
    let run1 = RUN.replace("RUN", "run-1");
    let run2 = RUN.replace("RUN", "run-2");
    let old = log(&[
        &run1,
        r#"{"type":"step_started","step_id":1}"#,
        r#"{"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m1","content":[{"type":"tool_call","tool_call_id":"c1","tool_name":"bash","input":{"cmd":"ls"}}]},"usage":{"input_tokens":3},"finish_reason":"tool_calls","retries_used":0}"#,
        r#"{"type":"tool_batch_started","batch_id":"b1","step_id":1,"planned_calls":["c1"]}"#,
        r#"{"type":"tool_dispatch_intent","batch_id":"b1","tool_call_id":"c1","args":{"cmd":"ls"}}"#,
        r#"{"type":"tool_result","batch_id":"b1","result":{"tool_call_id":"c1","output":{"kind":"text","text":"ok"},"is_error":false},"outcome":"unknown"}"#,
        r#"{"type":"tool_batch_finished","batch_id":"b1"}"#,
        r#"{"type":"turn_finished","step_id":1}"#,
        r#"{"type":"input_token_cap_frozen","limit":5}"#,
        r#"{"type":"run_finished","reason":"completed","usage":{},"retries_used":0}"#,
        &run2,
    ]);
    let converted = convert(&old, None).unwrap();
    assert_eq!(
        names(&converted.records),
        [
            "SetupStub",
            "RunStart",
            "Input",
            "StepStart",
            "Assistant",
            "StepEnd",
            "BatchStart",
            "ToolIntent",
            "ToolReply",
            "ToolOutcome",
            "BatchEnd",
            // A record whose fields nothing reads individually yet travels whole as an event.
            "Event",
            "RunEnd",
            // The second run has the same setup, so it gets no second stub.
            "RunStart",
            "Input",
        ]
    );

    let intent = converted
        .records
        .iter()
        .find_map(|r| match r {
            Record::ToolIntent(intent) => Some(intent),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        intent.args, None,
        "arguments equal to the call's input aren't stored twice"
    );

    let outcome = converted
        .records
        .iter()
        .find_map(|r| match r {
            Record::ToolOutcome(outcome) => Some(outcome),
            _ => None,
        })
        .unwrap();
    assert_eq!(outcome.outcome, CallOutcome::Unknown);
    assert!(matches!(
        &converted.records[2],
        Record::Input(input) if input.placement == Placement::RunInput && input.run_id == "run-1"
    ));
    assert!(matches!(
        &converted.records[11],
        // The event names a run only when the old record did.
        Record::Event(event) if event.name == "input_token_cap_frozen"
            && event.run_id.is_none()
            && event.body == Some(serde_json::json!({"limit": 5}))
    ));
    assert!(matches!(
        &converted.records[12],
        Record::RunEnd(end) if end.run_id == "run-1"
    ));

    assert!(converted.lost.is_empty(), "{:?}", converted.lost);
    // Tools, response format: two parts, each stored once though both runs named them.
    assert_eq!(converted.parts.len(), 2);
    assert_eq!(converted.runs, 2);
    assert_eq!(converted.dropped["turn_finished"].count, 1);
}

#[test]
fn unhandled_fields_wrong_types_and_missing_fields_are_reported_lost() {
    // A planned-call list under a name the converter doesn't take, a step id that isn't a
    // number, and a required field that is missing.
    let mut out = super::Converted::default();
    let mut object = serde_json::Map::new();
    object.insert("batch_id".into(), serde_json::json!("b1"));
    object.insert("planned".into(), serde_json::json!(["c1"]));
    object.insert("step_id".into(), serde_json::json!("one"));
    let mut f = super::Fields::new("tool_batch_started", object);
    assert_eq!(f.req_string("batch_id"), "b1");
    assert_eq!(f.req_u64("step_id"), 0);
    assert_eq!(f.req_u64("attempt"), 0);
    f.finish(&mut out);
    let lost: Vec<&str> = out.lost.keys().map(String::as_str).collect();
    assert_eq!(
        lost,
        [
            "tool_batch_started.attempt (missing)",
            "tool_batch_started.planned",
            "tool_batch_started.step_id (not an unsigned integer)",
        ]
    );
}

#[test]
fn changed_dispatch_arguments_are_kept() {
    let old = log(&[
        &RUN.replace("RUN", "r"),
        r#"{"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m1","content":[{"type":"tool_call","tool_call_id":"c1","tool_name":"bash","input":{"cmd":"ls"}}]},"usage":{},"retries_used":0}"#,
        r#"{"type":"tool_dispatch_intent","batch_id":"b1","tool_call_id":"c1","args":{"cmd":"ls -la"}}"#,
    ]);
    let converted = convert(&old, None).unwrap();
    let Some(Record::ToolIntent(intent)) = converted.records.last() else {
        panic!("last record is the intent");
    };
    assert_eq!(intent.args, Some(serde_json::json!({"cmd": "ls -la"})));
}

/// A refusal-fallback schedule carries to the next run under the same model and is dropped
/// under another: the run start records what today's admission decided.
#[test]
fn a_run_start_records_the_refusal_schedule_admission_carried() {
    use broca_session_store::record::RunStart;
    let schedule = serde_json::json!({
        "source": {"provider_module_id": "p", "model_id": "m"},
        "fallback": {"provider_module_id": "p", "model_id": "fallback"},
        "phase": {"kind": "fallback", "remaining": 9},
        "re_refusals": 0,
    });
    let step = format!(
        r#"{{"type":"model_step_finished","step_id":1,"assistant_message":{{"message_id":"a","content":[{{"type":"text","text":"ok"}}]}},"usage":{{}},"retries_used":0,"refusal_fallback":{schedule}}}"#
    );
    let end = r#"{"type":"run_finished","reason":"completed","usage":{},"retries_used":0}"#;
    let other_model = RUN
        .replace("RUN", "r3")
        .replace(r#""model_id":"m""#, r#""model_id":"other""#);
    let old = log(&[
        &RUN.replace("RUN", "r1"),
        r#"{"type":"step_started","step_id":1}"#,
        &step,
        end,
        &RUN.replace("RUN", "r2"),
        end,
        &other_model,
    ]);
    let records = convert(&old, None).unwrap().records;
    let schedules: Vec<Option<serde_json::Value>> = records
        .iter()
        .filter_map(|record| match record {
            Record::RunStart(start) => {
                let start: &RunStart = start;
                Some(start.refusal_schedule.clone())
            }
            _ => None,
        })
        .collect();
    assert_eq!(schedules, [None, Some(schedule), None]);
}

/// A session the new storage creates is refused by today's WAL store at the same state root, for
/// reads and writes alike. The head the new storage's local backend writes, rooted at
/// `SESSION_STORAGE_DIR` and addressed by today's `session_addr`, is exactly where today's guard
/// looks: without this, a rollback to today's storage would create a second log for the session.
#[test]
fn todays_wal_store_refuses_a_session_the_new_storage_created() {
    use broca_session_store::backend::{LocalBackend, SessionAddr};
    use broca_session_store::head::SessionTriple;
    use broca_session_store::keys::DeploymentKey;
    use broca_session_store::session::Session;

    let state = std::env::temp_dir().join(format!("broca-storage-guard-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&state);
    let bind = broca_protocol::BindIdentity::new(state.join("proj"), "runner", "guarded");
    let other = broca_protocol::BindIdentity::new(state.join("proj"), "runner", "other");
    let wal = broca_core::FileWalStore::new(state.join("wal"));
    // The control: before the new storage holds it, the session passes.
    wal.check_move(&bind, true).unwrap();
    let backend = LocalBackend::new(state.join(broca_wal::SESSION_STORAGE_DIR));
    futures::executor::block_on(Session::create(
        &backend,
        SessionAddr(broca_core::session_addr(&bind)),
        SessionTriple {
            project_root: bind.project_root.to_string_lossy().into_owned(),
            harness: bind.harness.clone(),
            session: bind.session.clone(),
        },
        &DeploymentKey::new(1, &[0x5a; 32]),
        true,
    ))
    .unwrap();
    for write in [false, true] {
        match wal.check_move(&bind, write) {
            Err(broca_wal::WalError::NewerWriter { missing, .. }) => {
                assert_eq!(missing, [broca_wal::SESSION_STORAGE_FEATURE]);
            }
            other => panic!("write={write}: expected the session-storage refusal: {other:?}"),
        }
    }
    wal.check_move(&other, true).unwrap();
    std::fs::remove_dir_all(&state).unwrap();
}
