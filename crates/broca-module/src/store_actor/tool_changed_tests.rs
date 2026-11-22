//! A call its provider refuses because it now serves another major of the tool raises
//! `tool_changed` on the control lane (llm-runner `tool_versions`), projected from the call's
//! stored outcome, so a replay from a cursor serves it again.

use broca_protocol::caller::ControlUnit;
use cortexkit_role_llm_runner::live::{LiveEvent, ToolChanged};
use serde_json::json;

use super::tests::{actor, executor, idle, read_step, send, text_step};
use crate::actor::SendOutcome;
use crate::store_control::StoreControl;

#[tokio::test]
async fn a_major_change_refusal_raises_tool_changed_on_its_calls_message() {
    let expected = format!("tp2:read:{}:2", "a".repeat(64));
    let current = format!("tp2:read:{}:3", "b".repeat(64));
    let mut executor = executor(vec![read_step("a1"), text_step("a2", "done")], None);
    executor.planes.major_changed =
        Some(json!({"tool": "read", "expected": expected, "current": current}));
    let (commands, served, _) = actor(executor).await;
    assert!(matches!(
        send(&commands, "r1", "read it").await,
        SendOutcome::Active { .. }
    ));
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    let records = session.open_log_records().await.unwrap();
    let mut control = StoreControl::default();
    let mut changed = Vec::new();
    for (at, (seq, record)) in records.iter().enumerate() {
        let next = records.get(at + 1).map(|(_, record)| record);
        for unit in control.project(*seq, record, next).unwrap() {
            if let Some(LiveEvent::ToolChanged(event)) = unit.event {
                changed.push((unit.unit, event));
            }
        }
    }
    let [(unit, event)] = changed.as_slice() else {
        panic!("one tool_changed, got {changed:?}");
    };
    assert_eq!(*event, ToolChanged::new("aft", "read", expected, current));
    assert!(
        matches!(unit, ControlUnit::MessageBoundary { .. }),
        "it rides the boundary of the call's message: {unit:?}"
    );
}
