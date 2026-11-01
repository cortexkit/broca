use super::*;
use broca_protocol::RunFinishReason;
use broca_wal::WalRecord;

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
    let before = rig.wal_bytes();
    let reply = stop(rig, run, at).await;
    assert_eq!(reply.header.ty, FrameType::Error, "{reply:?}");
    let error: Value = serde_json::from_slice(&reply.body).unwrap();
    assert_eq!(error["code"], "scope_owner_mismatch", "{error}");
    assert!(
        String::from_utf8_lossy(&reply.body).contains("phone"),
        "{error}"
    );
    assert_eq!(
        rig.wal_bytes(),
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
            .iter()
            .filter(|r| matches!(
                r,
                WalRecord::RunFinished {
                    reason: RunFinishReason::Cancelled,
                    ..
                }
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
    let before = rig.wal_bytes();
    assert_eq!(
        result(&stop(&mut rig, &run, Some("now")).await),
        json!("not_active")
    );
    assert_eq!(rig.wal_bytes(), before);
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
    let accepted = rig.wal_bytes();
    assert_eq!(result(&stop(&mut rig, &run, None).await), json!("ack"));
    assert_eq!(
        rig.wal_bytes(),
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

fn accepted_stop_prefix(bytes: &[u8]) -> Vec<u8> {
    let mut at = 0;
    while at + 53 <= bytes.len() {
        let len = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
        let end = at + 53 + len;
        assert!(end <= bytes.len());
        if bytes[at + 4] != 2 {
            let payload: Value = serde_json::from_slice(&bytes[at + 53..end]).unwrap();
            let record = if bytes[at + 4] == 3 {
                &payload["record"]
            } else {
                &payload
            };
            if record["type"] == "run_cancel_requested" {
                return bytes[..end].to_vec();
            }
        }
        at = end;
    }
    panic!("ack must follow a durable cancel request");
}

#[tokio::test]
async fn crash_after_sender_stop_acceptance_recovers_one_terminal_and_never_resends_call() {
    let mut rig = ready_sender("sender-stop-crash", true).await;
    let run = start_held(&mut rig, "held", true).await;
    assert_eq!(result(&stop(&mut rig, &run, None).await), json!("ack"));
    let prefix = accepted_stop_prefix(&rig.wal_bytes());
    rig.finish().await;

    // Replay the accepted stop's WAL prefix into a fresh SQLite-backed rig, with no
    // cached run status or terminal. Recovery must close the run from the durable request
    // without dispatching the tool again.
    let dir = broca_test_support::scratch_root()
        .join(format!("sender-stop-recovered-{}", std::process::id()));
    broca_test_support::remove_rig_dir(&dir);
    let mut recovered = Rig::open_fixture(dir.clone()).await;
    recovered.seed_fixture_wal(&prefix);
    recovered
        .bind_as(
            7,
            Some(Principal::Reserved {
                module_id: "owner".into(),
            }),
        )
        .await;
    bind_phone(&mut recovered).await;
    assert_cancelled(&mut recovered, &run).await;
    assert!(recovered.script.dispatched.lock().unwrap().is_empty());
    assert!(recovered.script.prompts.lock().unwrap().is_empty());
    assert_eq!(
        result(&stop(&mut recovered, &run, None).await),
        json!("not_active")
    );
    assert_eq!(
        recovered
            .records()
            .iter()
            .filter(|r| matches!(
                r,
                WalRecord::RunFinished {
                    reason: RunFinishReason::Cancelled,
                    ..
                }
            ))
            .count(),
        1
    );
    recovered.finish().await;
}
