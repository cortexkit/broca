//! A build that predates `dispatch-module/v1` refuses a session whose tool dispatch intent names
//! the module its call was dispatched to, through the real serve path, as a newer writer naming
//! the feature. It never reads the intent with the module ignored: that would report a
//! dispatched call as never dispatched.
//!
//! It plays the older build with `broca_wal::forget_feature_for_test`, which makes this
//! process's WAL decoder treat `dispatch-module/v1` as unknown, exactly as a build released
//! before the name existed would. That setting is process-wide, so this is the only test in its
//! binary.

mod delivery_rig;

use broca_wal::{FileWal, Wal, WalRecord, DISPATCH_MODULE_FEATURE};
use delivery_rig::*;
use subc_protocol::FrameType;

#[tokio::test]
async fn a_build_without_dispatch_module_refuses_the_session_as_newer_writer() {
    let mut rig = Rig::start("dispatch-module-older-build").await;
    // A finished episode with one tool call whose intent names its module, written to the WAL
    // before any send, so no lineage actor holds the session or caches its state.
    let record = |value: serde_json::Value| -> WalRecord { serde_json::from_value(value).unwrap() };
    let records = [
        record(serde_json::json!({
            "type": "run_started", "run_id": "run-1",
            "session": serde_json::to_value(&rig.session).unwrap(),
            "config": {"model": {"provider_module_id": "fake", "model_id": "fake"}, "tools": [],
                "tool_choice": {"type": "auto"}, "generation": {},
                "response_format": {"type": "text"}, "context_epoch": 0, "renderer_version": 1},
            "input": [{"role": "user", "content": [{"type": "text", "text": "hi"}]}],
            "origin": {"kind": "fresh"}
        })),
        WalRecord::StepStarted {
            step_id: broca_protocol::StepId(1),
        },
        record(serde_json::json!({
            "type": "model_step_finished", "step_id": 1,
            "assistant_message": {"message_id": "m1", "content": [{"type": "tool_call",
                "tool_call_id": "t1", "tool_name": "lookup", "input": {}}]},
            "usage": {}, "finish_reason": "tool_calls"
        })),
        record(serde_json::json!({
            "type": "tool_batch_started", "batch_id": "b1", "step_id": 1, "planned_calls": ["t1"]
        })),
        record(serde_json::json!({
            "type": "tool_dispatch_intent", "batch_id": "b1", "tool_call_id": "t1",
            "tool_name": "lookup", "args": {}, "module_id": "aft"
        })),
        record(serde_json::json!({
            "type": "tool_result", "batch_id": "b1", "outcome": "completed",
            "result": {"tool_call_id": "t1", "output": {"kind": "text", "text": "ok"},
                "is_error": false}
        })),
        record(serde_json::json!({"type": "tool_batch_finished", "batch_id": "b1"})),
        record(serde_json::json!({"type": "turn_finished", "step_id": 1})),
        WalRecord::StepStarted {
            step_id: broca_protocol::StepId(2),
        },
        record(serde_json::json!({
            "type": "model_step_finished", "step_id": 2,
            "assistant_message": {"message_id": "m2", "content": [{"type": "text",
                "text": "answer"}]},
            "usage": {}
        })),
        record(serde_json::json!({"type": "run_finished", "reason": "completed", "usage": {}})),
    ];
    let mut wal = FileWal::open(&rig.wal_path)
        .unwrap()
        .with_feature_floor(rig.dir.join(broca_wal::FEATURE_FLOOR_FILENAME));
    for record in &records {
        wal.append(1, record, true).unwrap();
    }
    drop(wal);
    let before = rig.wal_bytes();
    let needle = br#"{"requires":["dispatch-module/v1"],"record":{"type":"tool_dispatch_intent""#;
    assert!(before.windows(needle.len()).any(|window| window == needle));

    // The build under test, which knows `dispatch-module/v1`, reads the session.
    let read = rig
        .request(
            7,
            serde_json::json!({"method": "session.read", "params": {}}),
        )
        .await;
    let page = result(&read);
    assert_eq!(
        page["messages"].as_array().unwrap().last().unwrap()["message"]["content"][0]["text"],
        "answer",
        "{page}"
    );

    // A build that predates the feature refuses the session on every path, and the refusal
    // names the feature it is missing.
    let _older = broca_wal::forget_feature_for_test(DISPATCH_MODULE_FEATURE);
    for (label, body) in [
        (
            "session.read",
            serde_json::json!({"method": "session.read", "params": {}}),
        ),
        (
            "session.send",
            serde_json::json!({"method": "session.send", "params": {
                "prompt": "next", "send_id": "turn-2",
                "model": {"provider": "fake", "model": "fake"}
            }}),
        ),
    ] {
        let reply = rig.request(7, body).await;
        assert_eq!(reply.header.ty, FrameType::Error, "{label}");
        let error: serde_json::Value = serde_json::from_slice(&reply.body).unwrap();
        assert_eq!(error["code"], "wal_newer_writer", "{label}: {error}");
        assert_eq!(
            error["detail"],
            serde_json::json!({"kind": "newer_writer", "missing": ["dispatch-module/v1"]}),
            "{label}: {error}"
        );
    }
    assert_eq!(rig.wal_bytes(), before, "a refusal writes nothing");
    rig.finish().await;
}
