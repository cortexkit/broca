//! `provider-held/v1` on the write path: a run that is never provider-held writes the same
//! ordinary frames as before the feature existed and never raises the feature floor, while a
//! held run gates exactly its `ToolCallProviderHeld` and `RunPaused {tool_held}` frames and
//! raises the floor before the first of them reaches the file.

use super::*;

const RUN_STARTED: &str = r#"{"type":"run_started","run_id":"r","session":{"project_root":"/tmp/p","harness":"h","session":"s"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;

/// The same first batch with `t1` held by its provider: the run pauses `tool_held`, and the
/// late result later arrives as an ordinary `ToolResult` under the original batch.
const HELD_RUN: &[&str] = &[
    RUN_STARTED,
    r#"{"type":"step_started","step_id":1}"#,
    r#"{"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m1","content":[{"type":"tool_call","tool_call_id":"t1","tool_name":"ask","input":{"q":"a"}},{"type":"tool_call","tool_call_id":"t2","tool_name":"edit","input":{"f":"a"}}]},"usage":{},"finish_reason":"tool_calls","retries_used":0}"#,
    r#"{"type":"tool_batch_started","batch_id":"r-b1","step_id":1,"planned_calls":["t1","t2"]}"#,
    r#"{"type":"tool_dispatch_intent","batch_id":"r-b1","tool_call_id":"t1","tool_name":"ask","args":{"q":"a"}}"#,
    r#"{"type":"tool_dispatch_intent","batch_id":"r-b1","tool_call_id":"t2","tool_name":"edit","args":{"f":"a"}}"#,
    r#"{"type":"tool_call_provider_held","batch_id":"r-b1","tool_call_id":"t1","call_key":"lin_9:5","module":"core","reason":"parent_answer","held_at_ms":1761220000000,"sequential":false}"#,
    r#"{"type":"tool_result","batch_id":"r-b1","result":{"tool_call_id":"t2","output":{"kind":"text","text":"done"},"is_error":false},"outcome":"completed"}"#,
    r#"{"type":"run_paused","run_id":"r","ts_ms":1761220000001,"reason":"tool_held"}"#,
    r#"{"type":"tool_result","batch_id":"r-b1","result":{"tool_call_id":"t1","output":{"kind":"text","text":"yes"},"is_error":false},"outcome":"completed"}"#,
    r#"{"type":"tool_batch_finished","batch_id":"r-b1"}"#,
    r#"{"type":"turn_finished","step_id":1}"#,
    r#"{"type":"run_finished","reason":"completed","usage":{},"retries_used":0}"#,
];

fn records(lines: &[&str]) -> Vec<WalRecord> {
    lines
        .iter()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

/// In a held run only the held record and the `tool_held` pause need `provider-held/v1`:
/// every other frame, the held call's late `ToolResult` included, stays an ordinary frame that
/// a build without the feature still reads. This build lists the feature, so it reads the two
/// gated frames too, and a reader whose table lacks the name refuses exactly those two.
#[test]
fn a_held_run_gates_only_the_held_record_and_its_pause() {
    assert!(KNOWN_FEATURES.contains(&PROVIDER_HELD_FEATURE));
    let mut gated = Vec::new();
    for (seq, record) in records(HELD_RUN).iter().enumerate() {
        let bytes = encode(seq as u64 + 1, 3, record).unwrap();
        match decode_next(&bytes) {
            DecodeStep::Record { framed, .. } => assert_eq!(&framed.record, record),
            _ => panic!("frame {seq} must decode in a build that lists the feature"),
        }
        if bytes[4] == GATED_FRAMING_VERSION {
            assert_eq!(record.required_features(), [PROVIDER_HELD_FEATURE]);
            gated.push(seq);
        } else {
            assert_eq!(bytes[4], FRAMING_VERSION);
        }
    }
    let kinds: Vec<&str> = gated.iter().map(|at| HELD_RUN[*at]).collect();
    assert_eq!(kinds, [HELD_RUN[6], HELD_RUN[8]]);
}
