//! Structured output through broca's real serving path and WAL-backed runner: admission
//! refusals by field, the submit tool ending a run, the result on `run.status` and `run.result`,
//! and a frozen schema surviving a restart between steps.
use crate::delivery_rig::*;
use broca_protocol::{ContentBlock, ToolCall, ToolCallId};
use broca_provider::spec::CapabilityFlag;
use serde_json::{json, Value};

fn schema() -> Value {
    json!({
        "type": "object",
        "properties": {"answer": {"type": "string"}},
        "required": ["answer"]
    })
}

fn submit_tool() -> Value {
    json!({"structured_output": {"mode": "submit_tool", "schema": schema()}})
}

fn call(id: &str, tool: &str, input: Value) -> ContentBlock {
    ContentBlock::ToolCall(ToolCall {
        tool_call_id: ToolCallId(id.into()),
        tool_name: tool.into(),
        input,
        thought_signature: None,
    })
}

fn refusal(frame: &subc_protocol::Frame) -> Value {
    assert_eq!(
        frame.header.ty,
        subc_protocol::FrameType::Error,
        "{}",
        String::from_utf8_lossy(&frame.body)
    );
    serde_json::from_slice(&frame.body).unwrap()
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

/// Run one `submit_tool` send whose model step calls `submit_result` with `value` beside a
/// `work` call, and return the run id, its served `run.status` bytes and its `run.result`.
async fn submitted(rig: &mut Rig, value: Value) -> (String, String, Value) {
    *rig.script.structured_output_support.lock().unwrap() = CapabilityFlag::Known(true);
    rig.script.push(&[Step::Blocks(vec![
        call("submit-1", "submit_result", value),
        call("sibling-1", "work", json!({})),
    ])]);
    let send = rig.send_with("so", "hello", None, submit_tool()).await;
    let run = result(&send)["run_id"].as_str().unwrap().to_owned();
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let status = rig
        .request(7, json!({"method":"run.status","params":{"run_id":run}}))
        .await;
    let answer = rig
        .request(7, json!({"method":"run.result","params":{"run_id":run}}))
        .await;
    (run, raw_result(&status), result(&answer))
}

fn check_golden(file: &str, served: &str) {
    let path = golden_path(file);
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, format!("{served}\n")).unwrap();
        println!("{file}: {served}");
    }
    assert_eq!(served, std::fs::read_to_string(path).unwrap().trim_end());
}

#[tokio::test]
async fn submit_result_ends_the_run_and_skips_its_sibling_through_serve() {
    let mut rig = Rig::start("so-submit-golden").await;
    let (run, status, answer) = submitted(&mut rig, json!({"answer": "Paris"})).await;
    check_golden("run_status.structured_output_submit.json", &status);
    // Nothing of the submitting step reached the tool plane, and the run ended after it.
    assert!(rig.script.dispatched.lock().unwrap().is_empty());
    assert_eq!(rig.script.prompts.lock().unwrap().len(), 1);
    let status: Value = serde_json::from_str(&status).unwrap();
    assert_eq!(status["state"], "completed");
    assert_eq!(answer["state"], "completed");
    assert_eq!(answer["structured_output"], status["structured_output"]);
    let typed: cortexkit_role_llm_runner::run::RunResult =
        serde_json::from_value(answer.clone()).unwrap();
    let output = typed.structured_output.unwrap();
    assert_eq!(output.well_formed(), Ok(()));
    assert_eq!(output.validation, "passed");
    assert_eq!(output.value, Some(json!({"answer": "Paris"})));
    assert_eq!(
        output.skipped_siblings,
        Some(vec![
            broca_protocol::structured_output::SkippedSibling::new("sibling-1", "work")
        ])
    );
    let records = rig.records();
    let skipped: Vec<_> = records
        .iter()
        .filter_map(|record| match record {
            broca_wal::WalRecord::ToolResult {
                result,
                reason: Some(reason),
                ..
            } if reason == "skipped_after_submit" => Some(result.tool_call_id.0.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(skipped, ["sibling-1"]);
    assert!(!records
        .iter()
        .any(|record| matches!(record, broca_wal::WalRecord::ToolDispatchIntent { .. })));
    assert!(records.iter().any(|record| matches!(
        record,
        broca_wal::WalRecord::StructuredOutputResult { run_id, .. } if run_id.0 == run
    )));
    rig.finish().await;
}

#[tokio::test]
async fn a_value_failing_the_schema_still_completes_through_serve() {
    let mut rig = Rig::start("so-failed-golden").await;
    let (_, status, answer) = submitted(&mut rig, json!({"answer": 42})).await;
    check_golden("run_status.structured_output_failed.json", &status);
    let output = &answer["structured_output"];
    assert_eq!(answer["state"], "completed");
    assert_eq!(output["validation"], "failed");
    assert_eq!(output["value"], json!({"answer": 42}));
    assert!(output["errors"][0]
        .as_str()
        .unwrap()
        .starts_with("#/answer: "));
    rig.finish().await;
}

#[tokio::test]
async fn admission_refuses_by_field_and_writes_nothing() {
    let mut rig = Rig::start("so-refusals").await;
    let big = "a".repeat(70_000);
    for (structured_output, field) in [
        (
            json!({"mode": "final_text", "schema": schema()}),
            "structured_output.mode",
        ),
        (
            json!({"mode": "submit_tool", "schema": {"$ref": "https://example.com/s.json"}}),
            "structured_output.schema",
        ),
        (
            json!({"mode": "submit_tool", "schema": {"type": 12}}),
            "structured_output.schema",
        ),
        (
            json!({"mode": "submit_tool", "schema": {"$schema": "http://json-schema.org/draft-07/schema#"}}),
            "structured_output.schema",
        ),
        (
            json!({"mode": "submit_tool", "schema": {"description": big}}),
            "structured_output.schema",
        ),
        (
            json!({"mode": "submit_tool", "schema": schema(), "extra": 1}),
            "structured_output.extra",
        ),
        (
            json!({"mode": "submit_tool", "schema": schema(), "name": "Upper"}),
            "structured_output.name",
        ),
    ] {
        let reply = rig
            .send_with(
                "refused",
                "hello",
                None,
                json!({ "structured_output": structured_output }),
            )
            .await;
        let body = refusal(&reply);
        assert_eq!(body["code"], "invalid_params", "{body}");
        assert_eq!(body["detail"]["field"], field, "{body}");
    }
    // An admitted tool named `submit_result` collides with the runner's own.
    let reply = rig
        .send_with(
            "reserved",
            "hello",
            None,
            json!({
                "tools": [{"name": "submit_result", "description": "x", "input_schema": {"type": "object"}}],
                "structured_output": {"mode": "submit_tool", "schema": schema()},
            }),
        )
        .await;
    assert_eq!(refusal(&reply)["detail"]["field"], "structured_output");
    assert!(rig.records().is_empty(), "a refused send writes nothing");
    rig.finish().await;
}

#[tokio::test]
async fn a_keyed_retry_and_a_steer_must_repeat_the_frozen_request() {
    let mut rig = Rig::start("so-identity").await;
    *rig.script.structured_output_support.lock().unwrap() = CapabilityFlag::Known(true);
    rig.script.hold_model(true);
    let send = rig.send_with("keyed", "hello", None, submit_tool()).await;
    let run = result(&send)["run_id"].as_str().unwrap().to_owned();
    rig.wait_entered().await;
    let mut other = submit_tool();
    other["structured_output"]["schema"]["required"] = json!([]);
    let reuse = rig.send_with("keyed", "hello", None, other.clone()).await;
    let body = refusal(&reuse);
    assert_eq!(body["code"], "send_id_reuse", "{body}");
    assert!(body.to_string().contains("structured_output"), "{body}");
    let steer = rig
        .send_with("steer-differs", "more", Some("steer"), other)
        .await;
    assert_eq!(refusal(&steer)["detail"]["field"], "structured_output");
    let steer = rig
        .send_with("steer-same", "more", Some("steer"), submit_tool())
        .await;
    assert_eq!(result(&steer)["run_id"], run);
    rig.script.hold_model(false);
    rig.wait_for_finished(1).await;
    rig.finish().await;
}

#[tokio::test]
async fn a_run_without_structured_output_after_one_with_it_carries_none() {
    let mut rig = Rig::start("so-not-inherited").await;
    let (_, _, first) = submitted(&mut rig, json!({"answer": "x"})).await;
    assert!(first.get("structured_output").is_some());
    let send = rig.send("plain", "again", None).await;
    let run = result(&send)["run_id"].as_str().unwrap().to_owned();
    rig.wait_for_finished(2).await;
    rig.wait_idle().await;
    let answer = rig
        .request(7, json!({"method":"run.result","params":{"run_id":run}}))
        .await;
    assert!(result(&answer).get("structured_output").is_none());
    let rendered = rig.script.rendered.lock().unwrap().clone();
    let last: Value = serde_json::from_slice(rendered.last().unwrap()).unwrap();
    assert!(
        !last["tools"].to_string().contains("submit_result")
            && last.get("response_format").is_none(),
        "{last}"
    );
    rig.finish().await;
}

/// A crash cut between steps: the frozen schema survives a restart, and the resumed request
/// carries exactly the tool and schema bytes of the first.
#[tokio::test]
async fn the_frozen_schema_survives_a_restart_between_steps_byte_for_byte() {
    let mut rig = Rig::start("so-restart").await;
    *rig.script.structured_output_support.lock().unwrap() = CapabilityFlag::Known(true);
    rig.script.push(&[Step::Call("before-restart")]);
    rig.script.hold_tool(true);
    // A send that asks to resume after a graceful restart, so the stop pauses the run rather
    // than sealing it.
    let mut params = submit_tool();
    params["on_restart"] = json!("resume");
    let send = rig.send_with("restart", "hello", None, params).await;
    let run = result(&send)["run_id"].as_str().unwrap().to_owned();
    rig.wait_entered().await;
    let first: Value = serde_json::from_slice(&rig.script.rendered.lock().unwrap()[0]).unwrap();
    let mut rig = rig.restart_at_tool_boundary().await;
    // The restarted rig's route no longer vouches for schemas; the resumed run must keep the
    // decision its episode froze rather than decide again.
    *rig.script.structured_output_support.lock().unwrap() = CapabilityFlag::Known(false);
    rig.script.push(&[Step::Blocks(vec![call(
        "submit-after",
        "submit_result",
        json!({"answer": "y"}),
    )])]);
    // The boot resume of a restart pause, as the module runs it after its routes are back.
    assert_eq!(
        rig.registry()
            .resume_restart_pauses(
                std::sync::Arc::new(broca_core::FileWalStore::new(rig.dir.join("wal"))),
                vec![(run.clone(), rig.session.clone())],
            )
            .await,
        (1, 0)
    );
    rig.wait_for_finished(2).await;
    rig.wait_idle().await;
    let rendered = rig.script.rendered.lock().unwrap().clone();
    let last: Value = serde_json::from_slice(rendered.last().unwrap()).unwrap();
    assert_eq!(
        serde_json::to_vec(&last["tools"]).unwrap(),
        serde_json::to_vec(&first["tools"]).unwrap()
    );
    assert!(first["tools"].to_string().contains("submit_result"));
    let answer = rig
        .request(7, json!({"method":"run.result","params":{"run_id":run}}))
        .await;
    let output = &result(&answer)["structured_output"];
    assert_eq!(output["enforcement"], "enforced");
    assert_eq!(output["validation"], "passed");
    assert_eq!(output["value"], json!({"answer": "y"}));
    rig.finish().await;
}
