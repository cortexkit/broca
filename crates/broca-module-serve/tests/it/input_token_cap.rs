//! Input-token-cap conformance through broca's real serving path and WAL-backed runner.
use crate::delivery_rig::*;
use broca_protocol::{ContentBlock, ToolCall, ToolCallId, Usage};
use serde_json::json;

fn refusal(frame: &subc_protocol::Frame) -> serde_json::Value {
    assert_eq!(
        frame.header.ty,
        subc_protocol::FrameType::Error,
        "{}",
        String::from_utf8_lossy(&frame.body)
    );
    serde_json::from_slice(&frame.body).unwrap()
}

fn measured(input: Option<u64>, call: bool) -> Step {
    Step::Usage {
        content: if call {
            vec![ContentBlock::ToolCall(ToolCall {
                tool_call_id: ToolCallId("budget-call".into()),
                tool_name: "work".into(),
                input: json!({}),
                thought_signature: None,
            })]
        } else {
            vec![ContentBlock::Text {
                text: "answer".into(),
            }]
        },
        usage: Usage {
            input_tokens: input,
            cached_input_tokens: Some(2),
            cache_write_tokens: Some(3),
            output_tokens: Some(99),
            ..Default::default()
        },
    }
}

fn raw_result(frame: &subc_protocol::Frame) -> String {
    #[derive(serde::Deserialize)]
    struct Reply<'a> {
        #[serde(borrow)]
        result: &'a serde_json::value::RawValue,
    }
    serde_json::from_slice::<Reply<'_>>(&frame.body)
        .unwrap()
        .result
        .get()
        .to_owned()
}

#[tokio::test]
async fn budget_exhausted_status_golden_and_result_through_serve() {
    let mut rig = Rig::start("input-cap-golden").await;
    rig.script.push(&[measured(Some(5), true)]);
    let send = rig
        .send_with(
            "cap",
            "hello",
            None,
            json!({"budget":{"max_input_tokens":10}}),
        )
        .await;
    let run = result(&send)["run_id"].as_str().unwrap().to_owned();
    let records = rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    assert!(rig.script.dispatched.lock().unwrap().is_empty());
    assert!(records.iter().any(|record| matches!(record, broca_wal::WalRecord::ToolResult { reason: Some(reason), .. } if reason == "budget_exhausted")));
    let status = rig
        .request(7, json!({"method":"run.status","params":{"run_id":run}}))
        .await;
    let served = raw_result(&status);
    let path = golden_path("run_status.budget_exhausted.json");
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, format!("{served}\n")).unwrap();
        println!("run_status.budget_exhausted.json: {served}");
    }
    assert_eq!(served, std::fs::read_to_string(path).unwrap().trim_end());
    assert_eq!(
        result(&status)["detail"],
        json!({"limit":10,"spent":10,"cause":"limit_reached"})
    );
    let answer = rig
        .request(7, json!({"method":"run.result","params":{"run_id":run}}))
        .await;
    assert_eq!(result(&answer)["state"], "budget_exhausted");
    let typed: cortexkit_role_llm_runner::run::RunResult =
        serde_json::from_value(result(&answer)).unwrap();
    assert!(typed.run_state().is_budget_exhausted());
    assert_eq!(result(&answer)["detail"], result(&status)["detail"]);
    assert!(result(&answer).get("final_message").is_none());
    rig.finish().await;
}

#[tokio::test]
async fn unreported_usage_detail_keeps_only_measured_steps_before_unknown() {
    let mut rig = Rig::start("input-cap-unreported").await;
    rig.script
        .push(&[measured(Some(5), false), measured(None, true)]);
    rig.send_with(
        "first",
        "hello",
        None,
        json!({"budget":{"max_input_tokens":50}}),
    )
    .await;
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let send = rig.send("second", "more", None).await;
    let run = result(&send)["run_id"].as_str().unwrap().to_owned();
    rig.wait_for_finished(2).await;
    rig.wait_idle().await;
    let status = rig
        .request(7, json!({"method":"run.status","params":{"run_id":run}}))
        .await;
    // The first episode reported 5 fresh + 2 cached + 3 cache-write tokens. The second
    // omitted fresh input, so spent stays 10 rather than estimating that step from its cache split.
    assert_eq!(
        raw_result(&status),
        r#"{"state":"budget_exhausted","detail":{"limit":50,"spent":10,"cause":"usage_unreported"},"final_step_finish_reason":"tool_calls","undelivered_steers":[],"usage":{"cached_input_tokens":2,"cache_write_tokens":3,"output_tokens":99},"retries_used":0}"#
    );
    assert_eq!(
        result(&status)["detail"],
        json!({"limit":50,"spent":10,"cause":"usage_unreported"})
    );
    assert!(rig.script.dispatched.lock().unwrap().is_empty());
    rig.finish().await;
}

#[tokio::test]
async fn closed_budget_and_nudges_refuse_unknown_members_by_name() {
    let mut rig = Rig::start("input-cap-closed").await;
    for (budget, field) in [
        (json!({"max_input_tokens":10,"unknown":1}), "budget.unknown"),
        (
            json!({"nudges":[{"at_tool_calls":1,"text":"stop","unknown":1}]}),
            "budget.nudges[0].unknown",
        ),
        (json!({"max_input_tokens":0}), "budget.max_input_tokens"),
    ] {
        let reply = rig
            .send_with("bad", "hello", None, json!({"budget":budget}))
            .await;
        let error = refusal(&reply);
        assert_eq!(error["code"], "invalid_params");
        assert_eq!(error["detail"]["field"], field);
        assert!(error.to_string().contains(field), "{error}");
    }
    rig.finish().await;
}

#[tokio::test]
async fn cap_adopts_lowers_refuses_raise_and_omission_keeps_it_through_serve() {
    let mut rig = Rig::start("input-cap-freeze").await;
    rig.script.push(&[
        measured(Some(0), false),
        measured(Some(0), false),
        measured(Some(0), false),
    ]);
    rig.send("uncapped", "hello", None).await;
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    rig.send_with(
        "adopt",
        "more",
        None,
        json!({"budget":{"max_input_tokens":50}}),
    )
    .await;
    rig.wait_for_finished(2).await;
    rig.wait_idle().await;
    let raise = rig
        .send_with(
            "raise",
            "more",
            None,
            json!({"budget":{"max_input_tokens":51}}),
        )
        .await;
    assert_eq!(refusal(&raise)["code"], "invalid_params");
    assert!(refusal(&raise)
        .to_string()
        .contains("budget.max_input_tokens"));
    rig.send_with(
        "lower",
        "more",
        None,
        json!({"budget":{"max_input_tokens":15}}),
    )
    .await;
    rig.wait_for_finished(3).await;
    rig.wait_idle().await;
    let send = rig.send("omitted", "more", None).await;
    let run = result(&send)["run_id"].as_str().unwrap().to_owned();
    rig.wait_for_finished(4).await;
    rig.wait_idle().await;
    assert_eq!(
        rig.script.prompts.lock().unwrap().len(),
        3,
        "no step at exactly fifteen"
    );
    let status = rig
        .request(7, json!({"method":"run.status","params":{"run_id":run}}))
        .await;
    assert_eq!(
        result(&status)["detail"],
        json!({"limit":15,"spent":15,"cause":"limit_reached"})
    );
    rig.finish().await;
}

#[tokio::test]
async fn queued_lower_cap_stops_the_active_crossing_step_before_tools() {
    let mut rig = Rig::start("input-cap-live-lower").await;
    *rig.script.hold_model.lock().unwrap() = true;
    rig.script.push(&[measured(Some(5), true)]);
    rig.send_with(
        "first",
        "hello",
        None,
        json!({"budget":{"max_input_tokens":50}}),
    )
    .await;
    let queued = rig
        .send_with(
            "lower",
            "later",
            None,
            json!({"budget":{"max_input_tokens":10}}),
        )
        .await;
    assert_eq!(result(&queued)["state"], "pending");
    let raised = rig
        .send_with(
            "raise",
            "later",
            None,
            json!({"budget":{"max_input_tokens":11}}),
        )
        .await;
    assert_eq!(refusal(&raised)["code"], "invalid_params");
    *rig.script.hold_model.lock().unwrap() = false;
    rig.script.model_gate.notify_waiters();
    rig.wait_for_finished(2).await;
    rig.wait_idle().await;
    assert!(rig.script.dispatched.lock().unwrap().is_empty());
    assert_eq!(rig.script.prompts.lock().unwrap().len(), 1);
    rig.finish().await;
}
