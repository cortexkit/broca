//! Drive both drain modes over the real module connection against a scripted model and tools.

use broca_protocol::PauseReason;
use broca_session_store::record::Record;
use serde_json::{json, Value};
use subc_protocol::{Frame, FrameType};

use crate::delivery_rig::{result, Rig, Step};

fn now_ms() -> u64 {
    broca_core::clock::epoch_millis().unwrap()
}

fn ready(frame: &Frame) -> bool {
    frame.header.ty == FrameType::Request
        && frame.header.channel == 0
        && frame.body == br#"{"op":"module.drain_ready"}"#
}

async fn health(rig: &mut Rig) {
    let reply = rig.request(0, json!({"op":"health.check"})).await;
    assert_eq!(reply.header.ty, FrameType::Response);
    assert!(
        !rig.skipped.iter().any(ready),
        "unexpected module.drain_ready"
    );
}

async fn check_refused(rig: &mut Rig) {
    let refused = rig.send("refused", "must not be admitted", None).await;
    assert_eq!(refused.header.ty, FrameType::Error);
    let body: Value = serde_json::from_slice(&refused.body).unwrap();
    assert_eq!(body["detail"]["cause"], "module_stopping", "{body}");
    assert!(
        !rig.skipped.iter().any(ready),
        "unexpected module.drain_ready"
    );
}

#[test]
fn manifest_declares_accepting_drain() {
    let manifest = broca_module_serve::manifest::manifest("broca");
    assert!(manifest
        .capabilities
        .unwrap()
        .provides
        .iter()
        .any(|cap| cap == "accepting-drain/v1"));
}

#[tokio::test]
async fn ordinary_drain_stops_admission_without_readiness_exchange() {
    let mut rig = Rig::start("ordinary-drain").await;
    rig.drain_push(false, now_ms() + 60_000).await;
    check_refused(&mut rig).await;
    health(&mut rig).await;
    assert!(!rig.session_stored().await);
    rig.finish().await;
}

#[tokio::test]
async fn accepting_drain_finishes_step_and_recovers_queued_send_and_unsent_tools() {
    let mut rig = Rig::start("accepting-drain").await;
    let _diagnostics = rig.failure_diagnostics();
    rig.script.push(&[Step::Calls(&["first", "second"])]);
    rig.script.hold_model(true);
    let active = rig
        .send_with("in-flight", "run", None, json!({"on_restart":"resume"}))
        .await;
    let run_id = result(&active)["run_id"].as_str().unwrap().to_owned();
    rig.wait_entered().await;

    rig.drain_push(true, now_ms() + 60_000).await;
    // An idle session would normally start immediately; a send behind the active run
    // would have queued even without the drain.
    let idle = rig.bind_session(8, "idle-during-drain").await;
    let pending = rig.request(8, json!({"method":"session.send", "params":{
        "send_id":"deferred", "prompt":"run after restart", "model":{"provider":"fake","model":"fake"}
    }})).await;
    let submission = result(&pending)["submission_id"]
        .as_str()
        .expect("pending reply")
        .to_owned();
    let idle_records = rig.records_for(&idle).await;
    assert!(idle_records.iter().any(
        |record| matches!(record, Record::QueuedSend(send) if send.submission_id == submission)
    ));
    assert!(!idle_records
        .iter()
        .any(|record| matches!(record, Record::RunStart(_))));
    health(&mut rig).await;
    assert_eq!(rig.script.prompts.lock().unwrap().len(), 1);
    assert!(!rig
        .records()
        .await
        .iter()
        .any(|record| matches!(record, Record::StepEnd(_))));

    rig.script.hold_model(false);
    let ready = rig.drain_ready().await;
    // Receipt of readiness proves the end and pause were already durable, not just scheduled.
    let records = rig.records().await;
    assert!(records
        .iter()
        .any(|record| matches!(record, Record::StepEnd(end) if end.run_id == run_id)));
    assert!(records.iter().any(|record| matches!(record, Record::Pause(pause) if pause.run_id == run_id && pause.reason == PauseReason::Restart)));
    assert_eq!(
        records
            .iter()
            .filter(|r| matches!(r, Record::StepStart(_)))
            .count(),
        1
    );
    assert!(rig.script.dispatched.lock().unwrap().is_empty());

    rig.drain_frame(FrameType::Response, ready.header.corr, ready.body)
        .await;
    rig.drain_push(true, now_ms() + 60_000).await;
    check_refused(&mut rig).await;
    health(&mut rig).await;
    rig.drain_frame(FrameType::Goodbye, 0, Vec::new()).await;
    assert!(rig.registry().stop_and_seal().await);
    let mut rig = rig.restart().await;
    let idle = rig.bind_session(8, "idle-during-drain").await;
    rig.registry().enable_restart_resumes();
    let sweep = rig.registry().recover_store_crash_orphans().await.unwrap();
    assert!(sweep.failed.is_empty(), "{sweep:?}");
    rig.wait_for_finished(2).await;
    broca_test_support::wait_until("deferred send finishes after restart", || async {
        let records = rig.records_for(&idle).await;
        if records.iter().any(|record| matches!(record, Record::RunEnd(end) if Some(end.run_id.as_str()) == submission.strip_prefix("sub-"))) {
            Ok(())
        } else {
            Err(format!("idle session log: {records:?}"))
        }
    }).await;
    let calls = rig.script.dispatched.lock().unwrap().clone();
    assert_eq!(
        calls.len(),
        2,
        "each call dispatches once after restart: {calls:?}"
    );
    assert!(calls.iter().any(|id| id == "first"));
    assert!(calls.iter().any(|id| id == "second"));
    rig.finish().await;
}

#[tokio::test]
async fn accepting_drain_deadline_stops_admission_while_step_is_stuck() {
    let mut rig = Rig::start("accepting-drain-deadline").await;
    rig.script.hold_model(true);
    rig.send("stuck", "model never releases its step", None)
        .await;
    rig.wait_entered().await;
    let stopping = rig.registry().stopping_signal();
    rig.drain_push(true, now_ms() + 1_000).await;
    health(&mut rig).await;
    assert!(
        !stopping.is_cancelled(),
        "admission stays open before the deadline"
    );
    broca_test_support::wait_for("accepting drain expires", stopping.cancelled(), || {
        format!(
            "in-flight runs: {}",
            rig.registry().slot_counts().runs_in_flight
        )
    })
    .await;
    assert_eq!(rig.registry().slot_counts().runs_in_flight, 1);
    check_refused(&mut rig).await;
    health(&mut rig).await;
    rig.finish().await;
}

#[tokio::test]
async fn accepting_drain_finishes_dispatched_tool_batch_before_ready() {
    let mut rig = Rig::start("accepting-drain-tool-batch").await;
    rig.script.push(&[
        Step::Call("in-flight-tool"),
        Step::Text("not before restart"),
    ]);
    rig.script.hold_tool(true);
    let active = rig
        .send_with("tools", "run", None, json!({"on_restart":"resume"}))
        .await;
    assert_eq!(active.header.ty, FrameType::Response);
    rig.wait_entered().await;
    assert!(rig.records().await.iter().any(|record| matches!(record, Record::ToolIntent(intent) if intent.tool_call_id.0 == "in-flight-tool")));
    rig.drain_push(true, now_ms() + 60_000).await;
    health(&mut rig).await;
    assert!(!rig.registry().stopping_signal().is_cancelled());
    assert!(!rig
        .records()
        .await
        .iter()
        .any(|record| matches!(record, Record::BatchEnd(_))));

    rig.script.hold_tool(false);
    let ready = rig.drain_ready().await;
    let records = rig.records().await;
    let closed = records
        .iter()
        .position(|record| matches!(record, Record::BatchEnd(_)))
        .expect("tool results are durable");
    assert!(records[closed + 1..].iter().any(
        |record| matches!(record, Record::Pause(pause) if pause.reason == PauseReason::Restart)
    ));
    assert_eq!(rig.script.prompts.lock().unwrap().len(), 1);
    assert_eq!(&*rig.script.dispatched.lock().unwrap(), &["in-flight-tool"]);
    rig.drain_frame(FrameType::Response, ready.header.corr, ready.body)
        .await;
    rig.finish().await;
}
