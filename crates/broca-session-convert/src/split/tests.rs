use broca_wal::{FramedRecord, WalRecord};

use super::{check_splits, split_points};
use crate::convert;

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

const RUN: &str = r#"{"type":"run_started","run_id":"RUN","session":{"project_root":"/project","harness":"runner","session":"s"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[{"name":"bash","description":"run","input_schema":{"type":"object"},"execution_mode":"parallel"}],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hi"}]}],"origin":{"kind":"fresh"}}"#;

/// Two runs: the first makes a tool call and finishes, the second is cut off mid-batch with a
/// dispatched call and no result. Every split point is checked, including those inside the open
/// tool turn and the open batch, which only the checkpoint carries across a log boundary.
#[test]
fn every_split_of_a_tool_history_folds_to_the_same_state() {
    let old = log(&[
        &RUN.replace("RUN", "run-1"),
        r#"{"type":"step_started","step_id":1}"#,
        r#"{"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m1","content":[{"type":"tool_call","tool_call_id":"c1","tool_name":"bash","input":{"cmd":"ls"}}]},"usage":{"input_tokens":3},"finish_reason":"tool_calls","retries_used":0}"#,
        r#"{"type":"tool_batch_started","batch_id":"b1","step_id":1,"planned_calls":["c1"]}"#,
        r#"{"type":"tool_dispatch_intent","batch_id":"b1","tool_call_id":"c1","args":{"cmd":"ls"}}"#,
        r#"{"type":"tool_result","batch_id":"b1","result":{"tool_call_id":"c1","output":{"kind":"text","text":"ok"},"is_error":false},"outcome":"completed"}"#,
        r#"{"type":"tool_batch_finished","batch_id":"b1"}"#,
        r#"{"type":"run_finished","reason":"completed","usage":{"input_tokens":3},"retries_used":0}"#,
        &RUN.replace("RUN", "run-2"),
        r#"{"type":"step_started","step_id":1}"#,
        r#"{"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m2","content":[{"type":"tool_call","tool_call_id":"c2","tool_name":"bash","input":{"cmd":"rm x"}}]},"usage":{"input_tokens":5},"finish_reason":"tool_calls","retries_used":0}"#,
        r#"{"type":"tool_batch_started","batch_id":"b2","step_id":1,"planned_calls":["c2"]}"#,
        r#"{"type":"tool_dispatch_intent","batch_id":"b2","tool_call_id":"c2","args":{"cmd":"rm x"}}"#,
    ]);
    let converted = convert(&old, None).unwrap();
    let len = converted.records.len();
    let check = check_splits(&converted.records, true, &split_points(len, 64, 0)).unwrap();
    assert_eq!(check.points, len as u64 + 1);
    assert!(check.differing.is_empty(), "{:?}", check.differing);
}

#[test]
fn long_logs_get_their_ends_and_evenly_spread_points() {
    assert_eq!(split_points(3, 64, 4), [0, 1, 2, 3]);
    assert_eq!(split_points(100, 64, 3), [0, 25, 50, 75, 100]);
}
