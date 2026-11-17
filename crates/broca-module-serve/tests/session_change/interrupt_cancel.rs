use super::*;
use broca_protocol::RunFinishReason;
use broca_session_store::record::Record;

fn sender_plan(interrupt: bool) -> Value {
    let mut plan = plan(&["work"], "system");
    plan["senders"] = json!([{"module":"phone","idle_start":true,"interrupt":interrupt}]);
    plan
}

async fn bind_phone(rig: &mut Rig) {
    rig.bind_as(
        8,
        Some(Principal::Reserved {
            module_id: "phone".into(),
        }),
    )
    .await;
}

async fn ready_sender(tag: &str, interrupt: bool) -> Rig {
    let mut rig = Rig::start(tag).await;
    rig.bind_as(
        7,
        Some(Principal::Reserved {
            module_id: "owner".into(),
        }),
    )
    .await;
    rig.registry()
        .set_plan_admitter(Arc::new(Admitter::default()));
    bind_phone(&mut rig).await;
    rig.script.push(&[Step::Text("first")]);
    let sent = send(&mut rig, "first", Some(sender_plan(interrupt)), "fake").await;
    assert_eq!(sent.header.ty, FrameType::Response, "{sent:?}");
    rig.wait_idle().await;
    rig
}

async fn start_held(rig: &mut Rig, id: &str, tool: bool) -> String {
    if tool {
        rig.script.push(&[Step::Call("held-call")]);
        rig.script.hold_tool(true);
    } else {
        rig.script.hold_model(true);
    }
    let sent = send(rig, id, None, "fake").await;
    assert_eq!(sent.header.ty, FrameType::Response, "{sent:?}");
    rig.wait_entered().await;
    result(&sent)["run_id"].as_str().unwrap().into()
}

async fn stop(rig: &mut Rig, run: &str, at: Option<&str>) -> Frame {
    let mut params = json!({"run_id":run});
    if let Some(at) = at {
        params["at"] = json!(at);
    }
    rig.request(8, json!({"method":"run.cancel","params":params}))
        .await
}

async fn refused(rig: &mut Rig, run: &str, at: Option<&str>) {
    let before = rig.records().await;
    let reply = stop(rig, run, at).await;
    assert_eq!(reply.header.ty, FrameType::Error, "{reply:?}");
    let error: Value = serde_json::from_slice(&reply.body).unwrap();
    assert_eq!(error["code"], "scope_owner_mismatch", "{error}");
    assert!(
        String::from_utf8_lossy(&reply.body).contains("phone"),
        "{error}"
    );
    assert_eq!(
        rig.records().await,
        before,
        "refused cancellation wrote history"
    );
}

async fn assert_cancelled(rig: &mut Rig, run: &str) {
    let channel = 7;
    broca_test_support::wait_for(
        "cancelled session becomes idle",
        async {
            loop {
                let reply = rig
                    .request(channel, json!({"method":"run.status","params":{}}))
                    .await;
                if result(&reply)["state"] == "idle" {
                    break;
                }
                tokio::task::yield_now().await;
            }
        },
        || format!("waiting for run {run}"),
    )
    .await;
    let reply = rig
        .request(
            channel,
            json!({"method":"run.result","params":{"run_id":run}}),
        )
        .await;
    assert_eq!(result(&reply)["state"], "cancelled");
    assert_eq!(
        rig.records()
            .await
            .iter()
            .filter(|r| matches!(
                r,
                Record::RunEnd(end) if end.reason == RunFinishReason::Cancelled
            ))
            .count(),
        1
    );
}

#[tokio::test]
async fn interrupt_granted_sender_stops_model_and_repeated_stop_writes_no_terminal() {
    let mut rig = ready_sender("sender-stop-model", true).await;
    let run = start_held(&mut rig, "held", false).await;
    assert_eq!(result(&stop(&mut rig, &run, None).await), json!("ack"));
    assert_cancelled(&mut rig, &run).await;
    let before = rig.records().await;
    assert_eq!(
        result(&stop(&mut rig, &run, Some("now")).await),
        json!("not_active")
    );
    assert_eq!(rig.records().await, before);
    rig.finish().await;
}

#[tokio::test]
async fn interrupt_granted_sender_stops_waiting_tool_without_resending_it() {
    let mut rig = ready_sender("sender-stop-tool", true).await;
    let run = start_held(&mut rig, "held", true).await;
    assert_eq!(
        result(&stop(&mut rig, &run, Some("now")).await),
        json!("ack")
    );
    let accepted = rig.records().await;
    assert_eq!(result(&stop(&mut rig, &run, None).await), json!("ack"));
    assert_eq!(
        rig.records().await,
        accepted,
        "an outstanding stop is idempotent"
    );
    rig.script.hold_tool(false);
    assert_cancelled(&mut rig, &run).await;
    assert_eq!(*rig.script.dispatched.lock().unwrap(), vec!["held-call"]);
    assert_eq!(rig.script.prompts.lock().unwrap().len(), 2);
    rig.finish().await;
}

#[tokio::test]
async fn sender_without_interrupt_and_non_now_cancel_are_refused_by_name() {
    let mut rig = ready_sender("sender-stop-no-grant", false).await;
    let run = start_held(&mut rig, "held", false).await;
    refused(&mut rig, &run, None).await;
    assert_eq!(
        refresh(&mut rig, &sender_plan(true), "on_prefix_rebuild", 1)
            .await
            .header
            .ty,
        FrameType::Response
    );
    refused(&mut rig, &run, Some("step_boundary")).await;
    assert_eq!(result(&stop(&mut rig, &run, None).await), json!("ack"));
    assert_cancelled(&mut rig, &run).await;
    rig.finish().await;
}

#[tokio::test]
async fn sender_removed_or_interrupt_revoked_by_refresh_is_refused() {
    let mut rig = ready_sender("sender-stop-removed", true).await;
    let run = start_held(&mut rig, "held", false).await;
    for (generation, next) in [(1, sender_plan(false)), (2, plan(&["work"], "system"))] {
        assert_eq!(
            refresh(&mut rig, &next, "on_prefix_rebuild", generation)
                .await
                .header
                .ty,
            FrameType::Response
        );
        refused(&mut rig, &run, None).await;
    }
    assert_eq!(
        refresh(&mut rig, &sender_plan(true), "on_prefix_rebuild", 3)
            .await
            .header
            .ty,
        FrameType::Response
    );
    assert_eq!(result(&stop(&mut rig, &run, None).await), json!("ack"));
    assert_cancelled(&mut rig, &run).await;
    rig.finish().await;
}

#[tokio::test]
async fn sender_added_by_idle_refresh_stops_the_next_run() {
    let (mut rig, _, _) = ready("sender-stop-added").await;
    bind_phone(&mut rig).await;
    assert_eq!(
        refresh(&mut rig, &sender_plan(true), "on_prefix_rebuild", 1)
            .await
            .header
            .ty,
        FrameType::Response
    );
    let run = start_held(&mut rig, "held", false).await;
    assert_eq!(result(&stop(&mut rig, &run, None).await), json!("ack"));
    assert_cancelled(&mut rig, &run).await;
    rig.finish().await;
}

#[tokio::test]
async fn another_sessions_sender_is_refused_without_writes() {
    let other = ready_sender("sender-stop-other-grant", true).await;
    let (mut rig, _, _) = ready("sender-stop-other-session").await;
    bind_phone(&mut rig).await;
    let run = start_held(&mut rig, "held", false).await;
    refused(&mut rig, &run, None).await;
    assert_eq!(
        refresh(&mut rig, &sender_plan(true), "on_prefix_rebuild", 1)
            .await
            .header
            .ty,
        FrameType::Response
    );
    assert_eq!(result(&stop(&mut rig, &run, None).await), json!("ack"));
    assert_cancelled(&mut rig, &run).await;
    rig.finish().await;
    other.finish().await;
}

async fn unlisted_cancel_at_is_read_only(channel: u16, tag: &str) {
    let mut rig = Rig::start(tag).await;
    rig.bind_as(
        7,
        Some(Principal::Reserved {
            module_id: "owner".into(),
        }),
    )
    .await;
    bind_phone(&mut rig).await;
    rig.registry()
        .set_plan_admitter(Arc::new(Admitter::default()));
    rig.script.push(&[
        Step::Call("intact-call"),
        Step::Text("unlisted cancel left the run intact"),
    ]);
    rig.script.hold_model(true);
    let sent = send(&mut rig, "held", Some(sender_plan(true)), "fake").await;
    assert_eq!(sent.header.ty, FrameType::Response, "{sent:?}");
    let run = result(&sent)["run_id"].as_str().unwrap().to_owned();
    rig.wait_entered().await;
    let before_head = call(&mut rig, "session.head", json!({})).await.body;
    let before_result = call(&mut rig, "run.result", json!({"run_id":run}))
        .await
        .body;
    let before_transcript = call(&mut rig, "session.read", json!({"limit":500}))
        .await
        .body;
    let before_wal = rig.records().await;
    let reply = rig.request(channel, json!({"method":"run.cancel","params":{"run_id":run,"at":"not-a-declared-cancellation-point"}})).await;
    let after_wal = rig.records().await;
    let after_head = call(&mut rig, "session.head", json!({})).await.body;
    let after_result = call(&mut rig, "run.result", json!({"run_id":run}))
        .await
        .body;
    let after_transcript = call(&mut rig, "session.read", json!({"limit":500}))
        .await
        .body;
    rig.script.hold_model(false);
    rig.wait_idle().await;
    let final_result = call(&mut rig, "run.result", json!({"run_id":run})).await;
    rig.finish().await;
    assert_eq!(reply.header.ty, FrameType::Error, "{reply:?}");
    assert_eq!(
        after_wal, before_wal,
        "unlisted at changed the session's records"
    );
    assert_eq!(
        after_transcript, before_transcript,
        "unlisted at changed the transcript"
    );
    assert_eq!(after_head, before_head, "unlisted at changed session.head");
    assert_eq!(
        after_result, before_result,
        "unlisted at changed run.result"
    );
    let error: Value = serde_json::from_slice(&reply.body).unwrap();
    assert_eq!(error["code"], "invalid_params", "{error}");
    assert_eq!(error["detail"]["field"], "at", "{error}");
    assert_eq!(
        result(&final_result)["state"],
        "completed",
        "the invalid point must not cancel or advance the held turn"
    );
}

#[tokio::test]
async fn unlisted_cancel_at_from_owner_preserves_run_and_transcript_bytes() {
    unlisted_cancel_at_is_read_only(7, "unlisted-owner-intact").await;
}

#[tokio::test]
async fn unlisted_cancel_at_from_granted_sender_preserves_run_and_transcript_bytes() {
    unlisted_cancel_at_is_read_only(8, "unlisted-sender-intact").await;
}
