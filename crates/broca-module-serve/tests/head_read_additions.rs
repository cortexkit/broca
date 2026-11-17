//! Default reply bytes captured through the session-bound management route.
mod delivery_rig;
use broca_session_store::record::Record;
use delivery_rig::*;
use serde_json::json;

fn capture_path(name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(name)
}

fn assert_capture(frame: &subc_protocol::Frame, name: &str) {
    assert_eq!(frame.header.ty, subc_protocol::FrameType::Response);
    let path = capture_path(name);
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, [&frame.body[..], b"\n"].concat()).unwrap();
        println!("{name}: {}", String::from_utf8_lossy(&frame.body));
    }
    assert_eq!(
        frame.body.as_slice(),
        std::fs::read(path).unwrap().strip_suffix(b"\n").unwrap()
    );
}

#[tokio::test]
async fn unknown_facts_keep_pre_addition_default_reply_bytes() {
    let mut rig = Rig::start("unknown-head-facts").await;
    let read = rig
        .request(7, json!({"method":"session.read","params":{}}))
        .await;
    assert_capture(&read, "session_read.before_head_additions.json");
    let head = rig
        .request(7, json!({"method":"session.head","params":{}}))
        .await;
    assert_capture(&head, "session_head.before_head_additions.json");
    rig.finish().await;
}

fn capture_known_with_run(frame: &subc_protocol::Frame, name: &str, run: Option<&str>) {
    assert_eq!(
        frame.header.ty,
        subc_protocol::FrameType::Response,
        "{:?}",
        result(frame)
    );
    let answer = result(frame);
    // Cursor binding adds an incarnation and a frame digest, not a message-byte change.
    let mut bytes = delivery_rig::normalize_cursor_digests(
        &frame.body,
        answer["incarnation"].as_str().unwrap_or(""),
    );
    for field in ["lineage_id", "incarnation"] {
        if let Some(value) = answer[field].as_str() {
            bytes = bytes.replace(
                &format!("\"{field}\":\"{value}\""),
                &format!("\"{field}\":\"@LOG_ID@\""),
            );
        }
    }
    if let Some(at) = answer["updated_at"].as_u64() {
        bytes = bytes.replace(
            &format!("\"updated_at\":{at}"),
            "\"updated_at\":\"@UPDATED_AT@\"",
        );
    }
    // A served history's version is allocated above every earlier one, so it is not stable
    // across runs; 0 (never served) stays literal.
    if let Some(version) = answer["history_version"]
        .as_u64()
        .filter(|version| *version > 0)
    {
        bytes = bytes.replace(
            &format!("\"history_version\":{version}"),
            "\"history_version\":\"@HISTORY_VERSION@\"",
        );
    }
    if let Some(run) = run {
        bytes = bytes.replace(run, "@RUN_ID@");
    }
    if let Some(version) = answer["implementation_version"].as_str() {
        bytes = bytes.replace(
            &format!("\"implementation_version\":\"{version}\""),
            "\"implementation_version\":\"@VERSION@\"",
        );
    }
    let path = capture_path(name);
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, format!("{bytes}\n")).unwrap();
        println!("{name}: {bytes}");
    }
    assert_eq!(bytes, std::fs::read_to_string(path).unwrap().trim_end());
}

#[tokio::test]
async fn newly_written_times_and_session_spend_survive_projection_and_restart() {
    let mut rig =
        Rig::start_with_idle_ttl("new-message-times", std::time::Duration::from_millis(100)).await;
    let _diagnostics = rig.failure_diagnostics();
    rig.script.push(&[Step::Usage {
        content: vec![broca_protocol::ContentBlock::Text {
            text: "answer".into(),
        }],
        usage: broca_protocol::Usage {
            input_tokens: Some(9),
            output_tokens: Some(0),
            ..Default::default()
        },
    }]);
    let sent = rig.send("first", "question", None).await;
    let first_run = result(&sent)["run_id"].clone();
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let before = result(
        &rig.request(7, json!({"method":"session.read","params":{}}))
            .await,
    );
    assert!(before["messages"]
        .as_array()
        .unwrap()
        .iter()
        .all(|entry| entry["written_at"].as_u64().is_some_and(|at| at > 0)));
    let first_head = result(
        &rig.request(7, json!({"method":"session.head","params":{}}))
            .await,
    );
    assert_eq!(first_head["usage"]["since"], first_run);
    assert!(first_head["usage"].get("reasoning_tokens").is_none());
    rig.wait_closed().await;
    let mut rig = rig.restart().await;
    let after = result(
        &rig.request(7, json!({"method":"session.read","params":{}}))
            .await,
    );
    assert_eq!(before["messages"], after["messages"]);
    rig.script.push(&[Step::Text("unreported")]);
    let second = rig.send("second", "again", None).await;
    assert_eq!(
        second.header.ty,
        subc_protocol::FrameType::Response,
        "{}",
        String::from_utf8_lossy(&second.body)
    );
    rig.wait_for_finished(2).await;
    rig.wait_idle().await;
    let head = result(
        &rig.request(7, json!({"method":"session.head","params":{}}))
            .await,
    );
    assert_eq!(head["incarnation"], first_head["incarnation"]);
    assert_eq!(head["usage"]["since"], first_run);
    assert_eq!(head["usage"]["steps"], 2);
    assert_eq!(head["usage"]["unreported_steps"], 1);
    assert_eq!(head["usage"]["input_tokens"], 9);
    assert_eq!(head["usage"]["output_tokens"], 0);
    assert!(head["usage"].get("reasoning_tokens").is_none());
    rig.finish().await;
}

#[tokio::test]
async fn queued_input_and_steer_keep_their_admission_times() {
    let mut rig = Rig::start("queued-admission-times").await;
    let _diagnostics = rig.failure_diagnostics();
    rig.script.push(&[
        Step::Call("held"),
        Step::Text("active answer"),
        Step::Text("queued answer"),
    ]);
    rig.script.hold_tool(true);
    rig.send("active", "first", None).await;
    rig.wait_entered().await;
    let queued = rig.send("queued", "queued input", Some("queue")).await;
    assert_eq!(queued.header.ty, subc_protocol::FrameType::Response);
    let steered = rig.send("steered", "steered input", Some("steer")).await;
    assert_eq!(steered.header.ty, subc_protocol::FrameType::Response);
    let records = rig.records().await;
    let queued_at = records
        .iter()
        .find_map(|record| match record {
            Record::QueuedSend(send) => send.written_at_ms,
            _ => None,
        })
        .expect("queue admission records a time");
    let steered_at = records
        .iter()
        .find_map(|record| match record {
            Record::Steer(steer) => steer.written_at_ms,
            _ => None,
        })
        .expect("steer admission records a time");
    rig.script.hold_tool(false);
    let records = rig.wait_for_finished(2).await;
    rig.wait_idle().await;
    assert!(records.iter().any(|record| matches!(record,
        Record::RunStart(start) if start.submission_id.is_some() && start.written_at_ms == Some(queued_at))));
    let page = result(
        &rig.request(7, json!({"method":"session.read","params":{}}))
            .await,
    );
    for (text, at) in [("queued input", queued_at), ("steered input", steered_at)] {
        let entry = page["messages"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["message"]["content"][0]["text"] == text)
            .unwrap();
        assert_eq!(entry["written_at"], at);
    }
    rig.finish().await;
}

#[tokio::test]
async fn reasoning_tokens_are_an_output_breakdown_on_run_and_session_spend() {
    let mut rig = Rig::start("reasoning-spend").await;
    let _diagnostics = rig.failure_diagnostics();
    rig.script.push(&[
        Step::Usage {
            content: vec![broca_protocol::ContentBlock::ToolCall(
                broca_protocol::ToolCall {
                    tool_call_id: broca_protocol::ToolCallId("reasoning-call".into()),
                    tool_name: "work".into(),
                    input: json!({}).into(),
                    thought_signature: None,
                },
            )],
            usage: broca_protocol::Usage {
                input_tokens: Some(7),
                output_tokens: Some(10),
                reasoning_tokens: Some(3),
                ..Default::default()
            },
        },
        Step::Usage {
            content: vec![broca_protocol::ContentBlock::Text {
                text: "answer".into(),
            }],
            usage: broca_protocol::Usage {
                input_tokens: Some(9),
                output_tokens: Some(12),
                reasoning_tokens: Some(4),
                ..Default::default()
            },
        },
    ]);
    let send = rig.send("reasoning", "question", None).await;
    let run = result(&send)["run_id"].as_str().unwrap().to_owned();
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let result = result(
        &rig.request(7, json!({"method":"run.result","params":{"run_id":run}}))
            .await,
    );
    assert_eq!(result["usage"]["output_tokens"], 22);
    assert_eq!(result["usage"]["reasoning_tokens"], 7);
    let head = rig
        .request(7, json!({"method":"session.head","params":{}}))
        .await;
    let answer = delivery_rig::result(&head);
    assert_eq!(answer["usage"]["since"], run);
    assert_eq!(answer["usage"]["output_tokens"], 22);
    assert_eq!(answer["usage"]["reasoning_tokens"], 7);
    assert_eq!(answer["usage"]["input_tokens"], 16);
    assert_eq!(answer["usage"]["steps"], 2);
    assert_eq!(
        answer["sender"]["modes"],
        json!(["tool_result", "turn_system", "steer", "idle_start"])
    );
    capture_known_with_run(&head, "session_head.reasoning_usage.json", Some(&run));
    rig.finish().await;
}

fn capture_runner_display_sample(name: &str, frame: &subc_protocol::Frame) {
    if let Some(root) = std::env::var_os("BROCA_LLM_RUNNER_SAMPLE_DIR") {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(root);
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join(name), &frame.body).unwrap();
    }
}

async fn head_run_error_matches_status(storage: &str) {
    let mut rig = Rig::start(&format!("head-run-error-{storage}")).await;
    let _diagnostics = rig.failure_diagnostics();
    *rig.script.media_frozen.lock().unwrap() =
        Some(broca_provider::families::frozen_config_for_family(
            broca_provider::spec::WireFamilyId::AnthropicMessages,
            "fake",
        ));
    broca_core::media_store::create_media_directories(&rig.dir).unwrap();
    rig.script.push(&[Step::Text("ready")]);
    let sent = rig
        .send("bootstrap", "prepare a synthetic session", None)
        .await;
    assert_eq!(sent.header.ty, subc_protocol::FrameType::Response);
    rig.wait_idle().await;
    rig.wait_closed().await;

    let cases = [
        (
            "billing_exhausted",
            Some(json!({"class":"permanent", "provider_code":"billing_exhausted"})),
        ),
        (
            "account_limit_exhausted",
            Some(
                json!({"class":"transient", "provider_code":"account_limit_exhausted",
                "retry_after_secs":120.5, "resets_at_ms":1800000120500_u64}),
            ),
        ),
        // A codeless permanent error is served with the contract's catch-all code.
        (
            "permanent_no_code",
            Some(json!({"class":"permanent","provider_code":"provider_error"})),
        ),
        ("completed", None),
    ];
    let mut observed = Vec::new();
    for (name, summary) in cases {
        if let Some(summary) = &summary {
            let mut error = summary.clone();
            error["message"] = json!("synthetic provider refusal");
            rig.script.normalized_steps.lock().unwrap().push_back(vec![
                broca_protocol::NormalizedEvent::Control(broca_protocol::ProviderControl::Error {
                    error: serde_json::from_value(error).unwrap(),
                    retries_used: 0,
                }),
            ]);
        } else {
            rig.script.push(&[Step::Text("completed synthetic answer")]);
        }
        let sent = rig.send(name, "synthetic question", None).await;
        assert_eq!(sent.header.ty, subc_protocol::FrameType::Response);
        let run_id = result(&sent)["run_id"].clone();
        rig.wait_idle().await;
        rig.wait_closed().await;
        let status = rig
            .request(7, json!({"method":"run.status","params":{"run_id":run_id}}))
            .await;
        assert_eq!(status.header.ty, subc_protocol::FrameType::Response);
        let head = rig
            .request(7, json!({"method":"session.head","params":{}}))
            .await;
        assert_eq!(head.header.ty, subc_protocol::FrameType::Response);
        capture_runner_display_sample(&format!("{storage}.{name}.json"), &head);
        observed.push((name, summary, result(&head), result(&status)));
    }
    rig.finish().await;

    for (name, summary, head, status) in observed {
        let state = if summary.is_some() {
            "error"
        } else {
            "completed"
        };
        assert_eq!(head["last_run_state"]["state"], state, "{storage}: {name}");
        assert_eq!(status["state"], state, "{storage}: {name}");
        if let Some(summary) = summary {
            assert_eq!(head["last_run_error"], summary, "{storage}: {name}");
            assert_eq!(
                head["last_run_error"]["provider_code"],
                status["error"]["provider_code"]
            );
        } else {
            assert!(
                head.get("last_run_error").is_none(),
                "{storage}: {name}: {head}"
            );
        }
    }
}

#[tokio::test]
async fn store_head_run_error_carries_provider_code_from_status() {
    head_run_error_matches_status("store").await;
}
