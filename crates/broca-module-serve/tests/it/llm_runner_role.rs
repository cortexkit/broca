//! The `llm-runner/v1` role surface through the real serve path: `session.read`,
//! `session.subscribe`, `session.head`, `run.result`, `role.describe` and their refusals.
//!
//! Every run goes through the real core loop over a real file WAL (the delivery rig), so each
//! record a test reads back was written by core or the lineage actor exactly as in production.

use broca_module::ServeHandler;
use broca_protocol::content::{ContentBlock, Message, Role};
use broca_protocol::RunId;
use broca_session_store::record::Record;

use crate::delivery_rig;
use delivery_rig::*;
use std::time::Duration;

use subc_protocol::{Frame, FrameType};

/// `body` with the run id and lineage id it names replaced by placeholders, so the rest of its
/// bytes can be pinned: both are minted per run from a per-process scratch directory.
fn normalized(body: &[u8], run_id: &str, lineage_id: &str) -> String {
    normalize_snapshot_bytes(body, run_id, lineage_id)
}

/// The pinned text of `file`, without its trailing newline.
fn pinned(file: &str) -> String {
    std::fs::read_to_string(golden_path(file))
        .unwrap()
        .trim_end_matches('\n')
        .to_owned()
}

/// A finished session of one run that called the `work` tool once before answering.
async fn one_tool_turn(tag: &str) -> (Rig, String) {
    let mut rig = Rig::start(tag).await;
    rig.script
        .push(&[Step::Call("call-1"), Step::Text("answer")]);
    let active = rig.send("turn-1", "hi", None).await;
    let run_id = result(&active)["run_id"].as_str().unwrap().to_owned();
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    (rig, run_id)
}

fn native_finish_events(
    raw: Option<&str>,
    legacy: broca_protocol::FinishReason,
    content: Vec<ContentBlock>,
) -> Vec<broca_protocol::NormalizedEvent> {
    use broca_protocol::{AssistantMessage, MessageId, NormalizedEvent, ProviderControl, Usage};
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId("native-finish".into()),
                content,
            },
        }),
        NormalizedEvent::Control(
            serde_json::from_value(serde_json::json!({
                "type":"step_finished", "step_id":1, "usage":{},
                "finish_reason":legacy, "raw_finish_reason":raw,
            }))
            .unwrap(),
        ),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: legacy,
            retries_used: 0,
        }),
    ]
}

#[tokio::test]
async fn message_senders_preserve_attested_owner_on_fresh_and_steered_rows() {
    let mut rig = Rig::start("message-sender-steer").await;
    rig.bind_as(
        7,
        Some(subc_protocol::Principal::Reserved {
            module_id: "actual-route".into(),
        }),
    )
    .await;
    rig.script
        .push(&[Step::Call("sender-call"), Step::Text("answer")]);
    rig.script.hold_tool(true);
    let started = rig.send("sender-start", "fresh", None).await;
    assert_eq!(result(&started)["state"], "active");
    rig.wait_entered().await;
    let joined = rig.send("sender-join", "steered", Some("steer")).await;
    assert_eq!(result(&joined)["state"], "active");
    rig.script.hold_tool(false);
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    rig = rig.restart().await;
    let history = call(
        &mut rig,
        "session.history",
        serde_json::json!({"form":"display"}),
    )
    .await;
    let page = result(&history);
    let users: Vec<_> = page["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| entry["row"]["role"] == "user")
        .collect();
    assert_eq!(users.len(), 2);
    for user in users {
        assert_eq!(user["row"]["sender"], "actual-route", "{user}");
    }
    for entry in page["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| entry["row"]["role"] != "user")
    {
        assert!(entry["row"].get("sender").is_none());
    }
    capture_sender_samples(&page, "owner-steer");
    let read = call(&mut rig, "session.read", serde_json::json!({})).await;
    assert!(!String::from_utf8(read.body).unwrap().contains("\"sender\""));
    rig.finish().await;
}

struct DisplaySenderPlan;

#[async_trait::async_trait]
impl broca_fetch_plan::PlanAdmitter for DisplaySenderPlan {
    async fn admit(
        &self,
        plan: &broca_fetch_plan::Plan,
        _session: &broca_protocol::BindIdentity,
        _scope: Option<&broca_protocol::SessionScope>,
    ) -> Result<broca_fetch_plan::Admitted, broca_fetch_plan::Refusal> {
        Ok(broca_fetch_plan::Admitted {
            manifest: broca_wal::PlanManifest {
                rich_ui: None,
                plan: broca_wal::OpaqueJson::from_json_text(plan.text.clone()).unwrap(),
                plan_identity: plan.identity.clone(),
                composition_digest: plan.composition_digest.clone(),
                fetches: vec![],
                tools: std::sync::Arc::new(vec![]),
                system_text: String::new(),
                system_digest: String::new(),
                join_version: 1,
                role_versions: Default::default(),
                declarations: vec![],
            },
        })
    }
}

async fn sender_history(rig: &mut Rig) -> serde_json::Value {
    let frame = call(
        rig,
        "session.history",
        serde_json::json!({"form":"display"}),
    )
    .await;
    result(&frame)
}

fn assert_display_sender(page: &serde_json::Value, text: &str, sender: &str) {
    let rows: Vec<_> = page["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| entry["row"]["role"] == "user" && entry["row"]["text"] == text)
        .collect();
    assert_eq!(rows.len(), 1, "{text}: {page}");
    assert_eq!(rows[0]["row"]["sender"], sender, "{text}: {page}");
}

/// Writes the read page `page` as a reader sample named `name` under `BROCA_SENDER_SAMPLES`,
/// with its checksum.
fn capture_sender_samples(page: &serde_json::Value, name: &str) {
    let Some(root) = std::env::var_os("BROCA_SENDER_SAMPLES") else {
        return;
    };
    use sha2::{Digest, Sha256};
    let root = std::path::PathBuf::from(root).join(name);
    std::fs::create_dir_all(&root).unwrap();
    let bytes = serde_json::to_vec(page).unwrap();
    std::fs::write(root.join("history.json"), &bytes).unwrap();
    let sums = format!("{:x}  history.json\n", Sha256::digest(&bytes));
    std::fs::write(root.join("SHA256SUMS"), sums).unwrap();
}

#[tokio::test]
async fn message_senders_cover_named_start_join_interrupt_template_and_queue_restart() {
    use cortexkit_role_llm_runner::delivery_order::{template_digest, Template};
    let mut rig = Rig::start("message-sender-matrix").await;
    rig.registry()
        .set_plan_admitter(std::sync::Arc::new(DisplaySenderPlan));
    rig.bind_as(
        7,
        Some(subc_protocol::Principal::Reserved {
            module_id: "owner".into(),
        }),
    )
    .await;
    rig.bind_as(
        13,
        Some(subc_protocol::Principal::Reserved {
            module_id: "named".into(),
        }),
    )
    .await;
    let template = Template::new("plain", "template text", Default::default());
    let composition = serde_json::json!({"providers":[]});
    let digest = cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap();
    let plan = serde_json::json!({"tool_items":[],"system_text_items":[],"step_transform_items":[],"composition":composition,"composition_digest":digest,"senders":[{"module":"named","idle_start":true,"interrupt":true,"templates":[template]}]});
    let baseline = call(&mut rig, "session.send", serde_json::json!({"prompt":"baseline","send_id":"baseline","model":{"provider":"fake","model":"fake"},"plan":plan})).await;
    assert_eq!(result(&baseline)["state"], "active");
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    for (id, prompt, delivery) in [
        (
            "named-start",
            "named fresh",
            serde_json::json!(["idle_start"]),
        ),
        (
            "idle-interrupt",
            "idle interrupt",
            serde_json::json!("interrupt"),
        ),
    ] {
        let sent = rig.request(13, serde_json::json!({"method":"session.send","params":{"send_id":id,"model":{"provider":"fake","model":"fake"},"prompt":prompt,"delivery":delivery}})).await;
        assert_eq!(result(&sent)["state"], "active", "{sent:?}");
        rig.wait_for_finished(if id == "named-start" { 2 } else { 3 })
            .await;
        rig.wait_idle().await;
    }
    rig.script.hold_model(true);
    let active = call(
        &mut rig,
        "session.send",
        serde_json::json!({"send_id":"owner-active","prompt":"owner active","model":{"provider":"fake","model":"fake"}}),
    )
    .await;
    assert_eq!(result(&active)["state"], "active");
    rig.wait_entered().await;
    let joined = rig.request(13, serde_json::json!({"method":"session.send","params":{"send_id":"named-join","prompt":"named join","delivery":["steer"]}})).await;
    assert!(matches!(
        result(&joined)["state"].as_str(),
        Some("pending" | "active")
    ));
    rig.script.hold_model(false);
    rig.wait_for_finished(4).await;
    rig.wait_idle().await;
    rig.script.hold_model(true);
    let active = call(
        &mut rig,
        "session.send",
        serde_json::json!({"send_id":"before-interrupt","prompt":"before interrupt","model":{"provider":"fake","model":"fake"}}),
    )
    .await;
    assert_eq!(result(&active)["state"], "active");
    rig.wait_entered().await;
    let interrupted = rig.request(13, serde_json::json!({"method":"session.send","params":{"send_id":"active-interrupt","model":{"provider":"fake","model":"fake"},"prompt":"active interrupt","delivery":"interrupt"}})).await;
    assert!(matches!(
        result(&interrupted)["state"].as_str(),
        Some("pending" | "active")
    ));
    rig.script.hold_model(false);
    rig.wait_for_finished(6).await;
    rig.wait_idle().await;
    let templated = rig.request(13, serde_json::json!({"method":"session.send","params":{"send_id":"template","model":{"provider":"fake","model":"fake"},"delivery":["idle_start"],"template":{"id":"plain","digest":template_digest(&template),"slots":{}}}})).await;
    assert_eq!(result(&templated)["state"], "active");
    rig.wait_for_finished(7).await;
    rig.wait_idle().await;
    rig.script.hold_model(true);
    let active = call(
        &mut rig,
        "session.send",
        serde_json::json!({"send_id":"before-restart","prompt":"before restart","model":{"provider":"fake","model":"fake"}}),
    )
    .await;
    assert_eq!(result(&active)["state"], "active");
    rig.wait_entered().await;
    let queued = rig.request(13, serde_json::json!({"method":"session.send","params":{"send_id":"queued-restart","model":{"provider":"fake","model":"fake"},"prompt":"queued after restart","delivery":["idle_start"]}})).await;
    assert_eq!(result(&queued)["state"], "pending");
    let (sealed, ()) = tokio::join!(rig.registry().stop_and_seal(), async {
        rig.script.hold_model(false);
    });
    assert!(sealed);
    rig = rig.restart().await;
    rig.registry()
        .set_plan_admitter(std::sync::Arc::new(DisplaySenderPlan));
    rig.bind_as(
        7,
        Some(subc_protocol::Principal::Reserved {
            module_id: "owner".into(),
        }),
    )
    .await;
    let wake = call(&mut rig, "session.send", serde_json::json!({"send_id":"reopen","prompt":"reopen","model":{"provider":"fake","model":"fake"}})).await;
    assert!(matches!(
        result(&wake)["state"].as_str(),
        Some("pending" | "active")
    ));
    rig.wait_for_finished(10).await;
    rig.wait_idle().await;
    let page = sender_history(&mut rig).await;
    for text in [
        "named fresh",
        "named join",
        "idle interrupt",
        "active interrupt",
        "template text",
        "queued after restart",
    ] {
        assert_display_sender(&page, text, "named");
    }
    // The page names each message's sender; every run, the one the queued send started after
    // the restart included, runs under the owner's principal.
    let owner = serde_json::to_value(broca_protocol::BindPrincipal::Reserved {
        module_id: "owner".into(),
    })
    .unwrap();
    for record in rig.records().await {
        if let Record::RunStart(start) = record {
            assert_eq!(start.principal.as_ref(), Some(&owner), "{}", start.run_id);
        }
    }
    capture_sender_samples(&page, "matrix");
    rig.finish().await;
}

#[tokio::test]
async fn finish_reason_reports_provider_limits_without_a_caller_cap_on_every_family() {
    use broca_provider::spec::WireFamilyId as Family;
    let mut missing = Vec::new();
    for (family, raw) in [
        (Family::AnthropicMessages, "max_tokens"),
        (Family::OpenAiChat, "length"),
        (Family::OpenAiResponses, "incomplete:max_output_tokens"),
        (
            Family::OpenAiResponsesChatgpt,
            "incomplete:max_output_tokens",
        ),
        (Family::Gemini, "MAX_TOKENS"),
        (Family::GoogleCodeAssist, "MAX_TOKENS"),
        (Family::BedrockConverse, "max_tokens"),
    ] {
        let mut rig = Rig::start(&format!("native-limit-{family:?}")).await;
        let mut frozen = broca_provider::families::frozen_config_for_family(family, "fake");
        frozen.project_id = Some("fixture-project".into());
        *rig.script.media_frozen.lock().unwrap() = Some(frozen);
        rig.script
            .normalized_steps
            .lock()
            .unwrap()
            .push_back(native_finish_events(
                Some(raw),
                broca_protocol::FinishReason::Length,
                vec![ContentBlock::Text {
                    text: "provider-limited reply".into(),
                }],
            ));
        let sent = rig.send("native-limit", "no caller output cap", None).await;
        let run = result(&sent)["run_id"].as_str().unwrap().to_owned();
        rig.wait_for_finished(1).await;
        rig.wait_idle().await;
        let output_frame = call(&mut rig, "run.result", serde_json::json!({"run_id":run})).await;
        let status_frame = call(&mut rig, "run.status", serde_json::json!({"run_id":run})).await;
        let output = result(&output_frame);
        let status = result(&status_frame);
        assert_eq!(
            status["final_step_finish_reason"], "length",
            "legacy field: {family:?}"
        );
        if output["finish"] != "length" || status["finish"] != "length" {
            missing.push(format!(
                "{family:?}: result={} status={}",
                output["finish"], status["finish"]
            ));
        }
        if let Some(path) = std::env::var_os("BROCA_FINISH_SAMPLES") {
            use sha2::{Digest, Sha256};
            let path = std::path::PathBuf::from(path).join(format!("{family:?}"));
            std::fs::create_dir_all(&path).unwrap();
            let mut sums = String::new();
            for (name, bytes) in [
                ("run-result.json", output_frame.body.as_slice()),
                ("run-status.json", status_frame.body.as_slice()),
            ] {
                std::fs::write(path.join(name), bytes).unwrap();
                sums.push_str(&format!("{:x}  {name}\n", Sha256::digest(bytes)));
            }
            std::fs::write(path.join("SHA256SUMS"), sums).unwrap();
        }
        rig.finish().await;
    }
    assert!(
        missing.is_empty(),
        "provider length evidence was not served: {missing:?}"
    );
}

#[tokio::test]
async fn finish_reason_uses_only_the_last_native_step_and_survives_restart() {
    use broca_protocol::{FinishReason, ToolCall, ToolCallId};
    let mut rig = Rig::start("native-finish-absence").await;
    *rig.script.media_frozen.lock().unwrap() =
        Some(broca_provider::families::openai_chat_frozen_config("fake"));
    let tool = ContentBlock::ToolCall(ToolCall {
        tool_call_id: ToolCallId("native-tool".into()),
        tool_name: "work".into(),
        input: serde_json::json!({}).into(),
        thought_signature: None,
    });
    let oversized = "x".repeat(65);
    let mut receipts = Vec::new();
    for (index, raw, expected) in [
        (0, Some("stop"), Some("stop")),
        (1, None, None),
        (2, Some("future_reason"), Some("other")),
        (3, Some(oversized.as_str()), None),
    ] {
        if index == 1 {
            rig.script.hold_tool(true);
            rig.script
                .normalized_steps
                .lock()
                .unwrap()
                .push_back(native_finish_events(
                    Some("tool_calls"),
                    FinishReason::ToolCalls,
                    vec![tool.clone()],
                ));
        }
        rig.script
            .normalized_steps
            .lock()
            .unwrap()
            .push_back(native_finish_events(
                raw,
                FinishReason::Stop,
                vec![ContentBlock::Text {
                    text: "answer".into(),
                }],
            ));
        let sent = rig.send(&format!("native-{index}"), "answer", None).await;
        let run = result(&sent)["run_id"].as_str().unwrap().to_owned();
        if index == 1 {
            rig.wait_entered().await;
            for op in ["run.result", "run.status"] {
                let frame = call(&mut rig, op, serde_json::json!({"run_id":run})).await;
                assert!(result(&frame).get("finish").is_none(), "active {op}");
            }
            rig.script.hold_tool(false);
        }
        rig.wait_for_finished(index + 1).await;
        rig.wait_idle().await;
        for op in ["run.result", "run.status"] {
            let frame = call(&mut rig, op, serde_json::json!({"run_id":run})).await;
            assert_eq!(
                result(&frame)
                    .get("finish")
                    .and_then(serde_json::Value::as_str),
                expected,
                "{op}: {index}"
            );
        }
        receipts.push((run, expected));
    }
    rig = rig.restart().await;
    for (run, expected) in receipts {
        for op in ["run.result", "run.status"] {
            let frame = call(&mut rig, op, serde_json::json!({"run_id":run})).await;
            assert_eq!(
                result(&frame)
                    .get("finish")
                    .and_then(serde_json::Value::as_str),
                expected,
                "recovered {op}: {run}"
            );
        }
    }
    rig.finish().await;
}

#[tokio::test]
async fn display_facts_opt_in_preserves_default_and_matches_stored_times() {
    // Only the resume cursor's incarnation and source-frame digest extend the historical pin.
    let mut rig = Rig::start("read-display-facts").await;
    *rig.script.schema_pin.lock().unwrap() = Some(broca_wal::DispatchSchemaPin {
        schema_digest: "a".repeat(64),
        semantics: 7,
    });
    rig.script
        .push(&[Step::Call("facts-call"), Step::Text("answer")]);
    let active = rig
        .send_with(
            "facts-turn",
            "hi",
            None,
            serde_json::json!({"client_message_id":"phone_facts_1"}),
        )
        .await;
    let run = result(&active)["run_id"].as_str().unwrap().to_owned();
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let default = call(&mut rig, "session.read", serde_json::json!({})).await;
    let plain = result(&default);
    let lineage = plain["incarnation"].as_str().unwrap();
    let mut baseline = normalized(&default.body, &run, lineage);
    for entry in plain["messages"].as_array().unwrap() {
        if let Some(at) = entry["written_at"].as_u64() {
            baseline = baseline.replace(
                &format!("\"written_at\":{at},"),
                "\"written_at\":\"@RECORDED_TIME@\",",
            );
        }
        assert!(entry.get("client_message_id").is_none());
        assert!(entry.get("tool_times").is_none());
    }
    if std::env::var_os("BROCA_CAPTURE_DISPLAY_BASELINE").is_some() {
        std::fs::write(
            golden_path("session_read.default_display_facts_baseline.json"),
            &baseline,
        )
        .unwrap();
    }
    assert_eq!(
        baseline,
        pinned("session_read.default_display_facts_baseline.json")
    );
    let decorated = call(
        &mut rig,
        "session.read",
        serde_json::json!({"include_display_facts":true}),
    )
    .await;
    let facts = result(&decorated);
    assert_eq!(facts["messages"][0]["client_message_id"], "phone_facts_1");
    // Recorded creation times (0.8.5) ride the same opt-in; the user entry's is its send's
    // acceptance, which this session recorded.
    assert!(facts["messages"][0]["created_at"].is_u64());
    // Without its display facts, an opted-in read is the default read.
    let mut stripped = facts.clone();
    for entry in stripped["messages"].as_array_mut().unwrap() {
        let entry = entry.as_object_mut().unwrap();
        for fact in [
            "client_message_id",
            "tool_times",
            "completed_at",
            "created_at",
            "reasoning_summaries",
        ] {
            entry.remove(fact);
        }
    }
    assert_eq!(plain, stripped);
    let records = rig.records().await;
    let started = records
        .iter()
        .find_map(|record| match record {
            Record::ToolIntent(intent) if intent.tool_call_id.0 == "facts-call" => {
                intent.dispatched_at_ms
            }
            _ => None,
        })
        .unwrap();
    let finished = records
        .iter()
        .find_map(|record| match record {
            Record::ToolOutcome(outcome) if outcome.tool_call_id.0 == "facts-call" => {
                outcome.finished_at_ms
            }
            _ => None,
        })
        .unwrap();
    let times = &facts["messages"][1]["tool_times"];
    assert_eq!(times.as_array().unwrap().len(), 1);
    assert_eq!(times[0]["tool_call_id"], "facts-call");
    assert_eq!(times[0]["started_at"], started);
    assert_eq!(times[0]["finished_at"], finished);
    let history = call(
        &mut rig,
        "session.history",
        serde_json::json!({"form":"display"}),
    )
    .await;
    let history_page = result(&history);
    let tool = &history_page["messages"][1]["row"]["parts"][0];
    assert_eq!(tool["started_at"], started);
    assert_eq!(tool["finished_at"], finished);
    assert!(tool.get("unanswered").is_none());
    assert!(tool["call_key"].is_string());
    assert!(tool["schema_pin"].is_string());
    assert_eq!(times[0]["call_key"], tool["call_key"]);
    assert_eq!(times[0]["schema_pin"], tool["schema_pin"]);
    if let Some(path) = std::env::var_os("BROCA_DISPLAY_FACTS_SAMPLES") {
        use sha2::{Digest, Sha256};
        let path = std::path::PathBuf::from(path);
        std::fs::create_dir_all(&path).unwrap();
        let mut sums = String::new();
        for (name, bytes) in [
            ("default-read.json", default.body.as_slice()),
            ("opt-in-read.json", decorated.body.as_slice()),
            ("history.json", history.body.as_slice()),
        ] {
            std::fs::write(path.join(name), bytes).unwrap();
            sums.push_str(&format!("{:x}  {name}\n", Sha256::digest(bytes)));
        }
        let row = serde_json::to_vec(&history_page["messages"][1]).unwrap();
        std::fs::write(path.join("history-row.json"), &row).unwrap();
        sums.push_str(&format!("{:x}  history-row.json\n", Sha256::digest(&row)));
        std::fs::write(path.join("SHA256SUMS"), sums).unwrap();
    }
    rig.finish().await;
}

/// A `session.read` and a `session.subscribe` from the start, each with no params, of a session
/// whose one run called a tool. Existing member and cursor/unit bytes remain pinned separately
/// from the default read-entry times, dispatch attribution, live metadata and message boundaries.
#[tokio::test]
async fn read_and_subscribe_bytes_without_new_params_are_pinned() {
    // Only the resume cursor's incarnation and source-frame digest extend the historical pin.
    let (mut rig, run_id) = one_tool_turn("pin-read").await;
    let read = rig
        .request(
            7,
            serde_json::json!({"method": "session.read", "params": {}}),
        )
        .await;
    let page = result(&read);
    let lineage_id = page["incarnation"].as_str().unwrap().to_owned();
    // Times are intentional default envelope additions. Require each one, then remove only
    // its envelope spelling to compare the older member bytes; new shape goldens pin the additions.
    let mut body = normalized(&read.body, &run_id, &lineage_id);
    for entry in page["messages"].as_array().unwrap() {
        let at = entry["written_at"]
            .as_u64()
            .expect("new messages have recorded times");
        assert!(at > 0);
        let ordinal = entry["ordinal"].as_u64().unwrap();
        let timed = format!("\"written_at\":{at},\"ordinal\":{ordinal}");
        assert_eq!(body.matches(&timed).count(), 1);
        body = body.replace(&timed, &format!("\"ordinal\":{ordinal}"));
    }
    // The call key is `<session id>:<seq of the call's dispatch intent>`.
    let intent_seq = page["messages"][1]["tool_calls"][0]["call_key"]
        .as_str()
        .and_then(|key| key.rsplit_once(':'))
        .map(|(_, seq)| seq.to_owned())
        .expect("the call has a key");
    let attribution = format!(
        r#","tool_calls":[{{"tool_call_id":"call-1","call_key":"@SESSION_ID@:{intent_seq}","dispatched_to":"{WORK_MODULE}","indeterminate":false}}]"#
    );
    assert_eq!(body.matches(&attribution).count(), 1, "{body}");
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(
            golden_path("session_read.tool_turn.json"),
            format!("{}\n", body.replace(&attribution, "")),
        )
        .unwrap();
    }
    assert_eq!(
        body.replace(&attribution, ""),
        pinned("session_read.tool_turn.json")
    );
    assert!(
        !pinned("session_read.tool_turn.json").contains("tool_calls"),
        "the pinned bytes predate the member"
    );

    let frames: Vec<Frame> = rig
        .subscribe_collect(
            9,
            serde_json::json!({"from": "start"}),
            Duration::from_millis(500),
        )
        .await;
    assert!(
        frames.iter().all(|f| f.header.ty == FrameType::StreamData),
        "a subscription from the start streams: {frames:?}"
    );
    let events: Vec<String> = frames
        .iter()
        .map(|f| normalized(&f.body, &run_id, &lineage_id))
        .collect();
    let legacy: Vec<_> = events
        .iter()
        .filter_map(|body| {
            let value: serde_json::Value = serde_json::from_str(body).unwrap();
            if value["kind"] != "control" || value["unit"]["type"] == "message_boundary" {
                return None;
            }
            let split = body
                .rfind(",\"seq\":")
                .expect("live members follow the unchanged payload");
            let payload = format!("{}}}", &body[..split]);
            Some(payload.replace(
                ",\"incarnation\":\"@LINEAGE_ID@\",\"digest\":\"@CURSOR_DIGEST@\"",
                "",
            ))
        })
        .collect();
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(
            golden_path("session_subscribe.tool_turn.legacy.jsonl"),
            format!("{}\n", legacy.join("\n")),
        )
        .unwrap();
    }
    assert_eq!(
        legacy.join("\n"),
        pinned("session_subscribe.tool_turn.legacy.jsonl")
    );
    for (index, body) in events.iter().enumerate() {
        let value: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(value["seq"], index as u64 + 1);
        // The contract makes `event` optional: a unit with no lifecycle event carries none.
        if !value["event"].is_null() {
            cortexkit_role_llm_runner::live::LiveEvent::decode_strict(&value["event"]).unwrap();
        }
    }
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        let sample = format!("{}\n", events.join("\n"));
        std::fs::write(golden_path("session_subscribe.tool_turn.jsonl"), &sample).unwrap();
        println!("GOLDEN_BEGIN session_subscribe.tool_turn.jsonl\n{sample}GOLDEN_END session_subscribe.tool_turn.jsonl");
    }
    assert_eq!(
        events.join("\n"),
        tool_turn_sample_with_recorded_times(
            &pinned("session_subscribe.tool_turn.jsonl"),
            &rig.records().await,
        )
    );
    rig.finish().await;
}

#[tokio::test]
async fn tagged_open_tool_turn_read_bytes_are_pinned() {
    // The pinned bytes include two members added after the golden was first written: the
    // resume cursor's incarnation and source-frame digest, and the `role` each read entry carries.
    let (mut rig, _) = crate::plan_admission::plan_rig("pin-tagged-open-read").await;
    rig.script.push(&[
        Step::Usage {
            content: vec![
                ContentBlock::Text {
                    text: "checking".into(),
                },
                ContentBlock::ToolCall(broca_protocol::ToolCall {
                    tool_call_id: broca_protocol::ToolCallId("call-1".into()),
                    tool_name: "work".into(),
                    input: serde_json::json!({}).into(),
                    thought_signature: None,
                }),
            ],
            usage: Default::default(),
        },
        Step::Call("call-open"),
        Step::Text("answer"),
    ]);
    rig.script.hold_tool(true);
    let composition = serde_json::json!({"providers": [{"provider": WORK_MODULE, "tools": [
        {"name": "work", "capabilities": []}
    ]}], "step_transform_items": []});
    let plan = serde_json::json!({
        "tool_items": [{"provider": WORK_MODULE, "preset": "head", "params": {}}],
        "system_text_items": [], "step_transform_items": [], "render_tags": true,
        "composition_digest": cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap(),
        "composition": composition,
    });
    let active = rig.request(7, serde_json::json!({"method": "session.send", "params": {
        "send_id": "tagged-turn", "prompt": "hi", "model": {"provider": "fake", "model": "fake"},
        "plan": plan,
    }})).await;
    let run_id = result(&active)["run_id"].as_str().unwrap().to_owned();
    rig.wait_entered().await;

    // After call-1 commits, pause the next model request, then hold call-open's execution.
    // session.read can then see the assistant's open call before its batch commits, so its
    // part rows must exist without depending on commit-time numbering.
    rig.script.hold_model(true);
    rig.script.hold_tool(false);
    rig.wait_entered().await;
    rig.script.hold_tool(true);
    rig.script.hold_model(false);
    rig.wait_entered().await;
    let records = rig.records().await;
    let config_all = rig.run_configs().await;
    let config = config_all.first().unwrap();
    assert!(config.render_tags);
    assert!(config.computed_tags.is_some());
    assert!(records.iter().any(|record| matches!(record,
        Record::ToolOutcome(outcome) if outcome.tool_call_id.0 == "call-1")));
    assert!(!records.iter().any(|record| matches!(record,
        Record::ToolOutcome(outcome) if outcome.tool_call_id.0 == "call-open")));

    let read = rig
        .request(
            7,
            serde_json::json!({"method": "session.read", "params": {}}),
        )
        .await;
    let page = result(&read);
    let lineage_id = page["incarnation"].as_str().unwrap();
    assert_eq!(page["messages"].as_array().unwrap().len(), 4);
    assert_eq!(
        page["messages"][0]["part_keys"],
        serde_json::json!(["m0#0"])
    );
    assert_eq!(
        page["messages"][1]["part_keys"],
        serde_json::json!(["m1#0", "m1#1"])
    );
    assert_eq!(
        page["messages"][2]["part_keys"],
        serde_json::json!(["m2#0"])
    );
    assert_eq!(
        page["messages"][3]["part_keys"],
        serde_json::json!(["m3#0"])
    );
    assert_eq!(
        page["messages"][3]["tool_calls"][0]["tool_call_id"],
        "call-open"
    );
    let mut body = normalized(&read.body, &run_id, lineage_id);
    // Entry times are pinned by session_read.written_at.json. This golden pins part identity, so
    // remove each recorded time from the envelope (the user message must carry one) and compare
    // the rest.
    for entry in page["messages"].as_array().unwrap() {
        let ordinal = entry["ordinal"].as_u64().unwrap();
        match entry["written_at"].as_u64() {
            Some(at) => {
                let timed = format!("\"written_at\":{at},\"ordinal\":{ordinal}");
                assert_eq!(body.matches(&timed).count(), 1);
                body = body.replace(&timed, &format!("\"ordinal\":{ordinal}"));
            }
            None => assert_ne!(ordinal, 0, "the user message has a recorded time"),
        }
    }
    let file = "session_read.tagged_open_tool_turn.json";
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(golden_path(file), format!("{body}\n")).unwrap();
        println!("GOLDEN_BEGIN {file}\n{body}\nGOLDEN_END {file}");
    }
    assert_eq!(body, pinned(file));
    rig.script.hold_tool(false);
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    rig.finish().await;
}

/// A read held between a tool turn's write and its batch commit (the tool provider holds the
/// call) exposes stable part identity. The public read no longer exposes the former `tags`
/// field's rendered `§N§` labels; committing the batch must not change keys a reader holds.
#[tokio::test]
async fn a_held_tool_turn_keeps_the_part_keys_a_read_exposed() {
    let (mut rig, _) = crate::plan_admission::plan_rig("held-turn-text-number").await;
    rig.script.push(&[
        Step::Usage {
            content: vec![
                ContentBlock::Text {
                    text: "checking".into(),
                },
                ContentBlock::ToolCall(broca_protocol::ToolCall {
                    tool_call_id: broca_protocol::ToolCallId("call-held".into()),
                    tool_name: "work".into(),
                    input: serde_json::json!({}).into(),
                    thought_signature: None,
                }),
            ],
            usage: Default::default(),
        },
        Step::Text("answer"),
    ]);
    rig.script.hold_tool(true);
    let composition = serde_json::json!({"providers": [{"provider": WORK_MODULE, "tools": [
        {"name": "work", "capabilities": []}
    ]}], "step_transform_items": []});
    let plan = serde_json::json!({
        "tool_items": [{"provider": WORK_MODULE, "preset": "head", "params": {}}],
        "system_text_items": [], "step_transform_items": [], "render_tags": true,
        "composition_digest": cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap(),
        "composition": composition,
    });
    rig.request(
        7,
        serde_json::json!({"method": "session.send", "params": {
            "send_id": "held-text", "prompt": "hi", "model": {"provider": "fake", "model": "fake"},
            "plan": plan,
        }}),
    )
    .await;
    // The tool provider now holds call-held: its dispatch intent is durable, its result is not.
    rig.wait_entered().await;
    assert!(!rig.records().await.iter().any(|record| matches!(record,
        Record::ToolOutcome(outcome) if outcome.tool_call_id.0 == "call-held")));

    let held = rig
        .request(
            7,
            serde_json::json!({"method": "session.read", "params": {}}),
        )
        .await;
    let held = result(&held).clone();
    let expected = serde_json::json!(["m1#0", "m1#1"]);
    assert_eq!(
        held["messages"][1]["part_keys"], expected,
        "the held turn's parts already have stable keys: {held}"
    );

    rig.script.hold_tool(false);
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let after = rig
        .request(
            7,
            serde_json::json!({"method": "session.read", "params": {}}),
        )
        .await;
    let after = result(&after).clone();
    assert_eq!(after["incarnation"], held["incarnation"]);
    assert_eq!(after["history_version"], held["history_version"]);
    assert_eq!(
        after["messages"][1]["part_keys"], held["messages"][1]["part_keys"],
        "part identity exposed by a read is unchanged after commit"
    );
    rig.finish().await;
}
// ---- helpers ------------------------------------------------------------------------------

/// Keep the captured frame template, substituting only this run's two WAL observations.
/// The expected clocks come from durable records, never from the stream being checked.
fn tool_turn_sample_with_recorded_times(sample: &str, records: &[Record]) -> String {
    sample
        .lines()
        .map(|line| {
            let value: serde_json::Value = serde_json::from_str(line).unwrap();
            let (field, start) = match value["event"]["type"].as_str() {
                Some("tool_started") => ("started_at", true),
                Some("tool_finished") => ("finished_at", false),
                _ => return line.to_owned(),
            };
            let call = value["event"]["tool_call_id"].as_str().unwrap();
            let recorded = records
                .iter()
                .find_map(|record| match record {
                    Record::ToolIntent(intent) if start && intent.tool_call_id.0 == call => {
                        Some(intent.dispatched_at_ms)
                    }
                    Record::ToolOutcome(outcome) if !start && outcome.tool_call_id.0 == call => {
                        Some(outcome.finished_at_ms)
                    }
                    _ => None,
                })
                .flatten()
                .expect("the real tool turn recorded its observation");
            let captured = value["event"][field]
                .as_u64()
                .expect("the sample retains the real captured time");
            line.replace(
                &format!("\"{field}\":{captured}"),
                &format!("\"{field}\":{recorded}"),
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The role's error body of a refusal frame.
fn refusal(frame: &Frame) -> serde_json::Value {
    assert_eq!(
        frame.header.ty,
        FrameType::Error,
        "{}",
        String::from_utf8_lossy(&frame.body)
    );
    serde_json::from_slice(&frame.body).unwrap()
}

async fn call(rig: &mut Rig, method: &str, params: serde_json::Value) -> Frame {
    rig.request(7, serde_json::json!({"method": method, "params": params}))
        .await
}

async fn read(rig: &mut Rig, params: serde_json::Value) -> ReadPage {
    let reply = call(rig, "session.read", params).await;
    serde_json::from_value(result(&reply)).unwrap()
}

/// Send a refused request and prove it wrote nothing: the WAL holds the same bytes after it.
async fn refused_writing_nothing(
    rig: &mut Rig,
    method: &str,
    params: serde_json::Value,
) -> serde_json::Value {
    let before = rig.records().await;
    let reply = call(rig, method, params.clone()).await;
    let body = refusal(&reply);
    assert_eq!(
        rig.records().await.len(),
        before.len(),
        "{method} {params} was refused {body} but the WAL grew"
    );
    assert_eq!(rig.records().await, before);
    body
}

/// A finished session of three runs, each a prompt and a one-text answer: six messages.
async fn three_turns(tag: &str) -> Rig {
    let mut rig = Rig::start(tag).await;
    for (n, answer) in ["one", "two", "three"].into_iter().enumerate() {
        rig.script.push(&[Step::Text(answer)]);
        rig.send(&format!("turn-{n}"), &format!("prompt {n}"), None)
            .await;
        rig.wait_for_finished(n + 1).await;
        rig.wait_idle().await;
    }
    rig
}

fn ordinals(page: &ReadPage) -> Vec<u64> {
    page.messages.iter().map(|m| m.ordinal).collect()
}

#[tokio::test]
async fn reply_too_large_message_bytes_match_published_vector_through_serve() {
    let vectors = reply_too_large_vectors();
    let case = vectors["refusals"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["name"] == "raw message exceeds the transport frame limit")
        .unwrap();
    let mut calibration = Rig::start("reply-too-large-calibration").await;
    import_reply_too_large_session(&mut calibration, 9, String::new()).await;
    let calibration_frame = calibration
        .request(
            9,
            serde_json::json!({"method":"session.read","params":case["request"]}),
        )
        .await;
    assert_eq!(calibration_frame.header.ty, FrameType::Response);
    let expected_frame_bytes = case["refusal"]["detail"]["bytes"].as_u64().unwrap() as usize;
    let calibration_frame_bytes = calibration_frame.body.len() + subc_protocol::HEADER_LEN;
    let final_text_bytes = expected_frame_bytes - calibration_frame_bytes;
    calibration.finish().await;

    let mut rig = Rig::start("reply-too-large-vector").await;
    import_reply_too_large_session(&mut rig, 9, "x".repeat(final_text_bytes)).await;
    let frame = rig
        .request(
            9,
            serde_json::json!({"method":"session.read","params":case["request"]}),
        )
        .await;
    assert_eq!(frame.header.ty, FrameType::Error);
    let refusal = &case["refusal"];
    let detail: cortexkit_role_llm_runner::errors::ReplyTooLargeDetail =
        serde_json::from_value(refusal["detail"].clone()).unwrap();
    assert_eq!(serde_json::to_value(&detail).unwrap(), refusal["detail"]);
    #[derive(serde::Serialize)]
    struct ExpectedError<'a> {
        code: &'a str,
        message: &'a str,
        detail: &'a cortexkit_role_llm_runner::errors::ReplyTooLargeDetail,
    }
    assert_eq!(
        frame.body,
        serde_json::to_vec(&ExpectedError {
            code: refusal["code"].as_str().unwrap(),
            message: refusal["message"].as_str().unwrap(),
            detail: &detail,
        })
        .unwrap()
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&frame.body).unwrap(),
        *refusal
    );
    rig.finish().await;
}

use cortexkit_role_llm_runner::{
    describe::check_describe,
    read::{HeadMeta, ReadPage},
    run::RunResult,
};

const REPLY_TOO_LARGE_VECTOR_SESSION: &str = "ses_01J9Z8Y7X6W5V4U3T2S1R0Q9P8";

async fn import_reply_too_large_session(rig: &mut Rig, channel: u16, final_text: String) {
    let session = rig
        .bind_session(channel, REPLY_TOO_LARGE_VECTOR_SESSION)
        .await;
    let mut messages: Vec<_> = (0..7)
        .map(|ordinal| {
            let role = if ordinal % 2 == 0 {
                Role::User
            } else {
                Role::Assistant
            };
            Message::new(
                role,
                vec![ContentBlock::Text {
                    text: format!("history {ordinal}").into(),
                }],
            )
        })
        .collect();
    messages.push(Message::new(
        Role::Assistant,
        vec![ContentBlock::Text {
            text: final_text.into(),
        }],
    ));
    rig.registry()
        .import(broca_module::serve::SessionImportRequest {
            project_id: None,
            principal: None,
            scope: broca_protocol::SendScope::Internal,
            session,
            run_id: RunId("reply-too-large-import".into()),
            messages,
            source_ref: "fixture:reply-too-large".into(),
        })
        .await
        .unwrap();
}

/// Read the published crate's copy of the refusal vectors.
fn reply_too_large_vectors() -> serde_json::Value {
    let contents = cortexkit_role_llm_runner::vectors::FILES
        .iter()
        .find_map(|(name, contents)| (*name == "reply-too-large.json").then_some(*contents))
        .expect("the role crate publishes reply-too-large.json");
    serde_json::from_str(contents).unwrap()
}

// ---- role.describe ------------------------------------------------------------------------

/// `role.describe` passes the role's own consumer check: the v1 major with its required ops,
/// every op of each declared group, the session-capability source and the read byte cap. It
/// declares the groups broca serves and none it does not.
#[tokio::test]
async fn role_describe_passes_the_roles_check_and_declares_what_broca_serves() {
    let mut rig = Rig::start("describe").await;
    let reply = call(&mut rig, "role.describe", serde_json::json!({})).await;
    let raw = result(&reply);
    let describe = check_describe(&raw).unwrap_or_else(|p| panic!("{raw}: {p:?}"));
    for group in [
        "transcript_reads",
        "run_ops",
        "run_status",
        "held_calls",
        "run_usage",
        "run_cancel",
        "retract",
        "dispatch_attribution",
        "streaming",
        "live_events",
        "steer",
        "queue",
        "interrupt",
        "retention",
        "session_change",
    ] {
        assert!(describe.declares(group), "{group}");
    }
    // broca keeps no input-token budget, so it leaves the cap undeclared.
    for group in [
        "model_view",
        "compaction",
        "call-modes/v1",
        "input_token_cap",
    ] {
        assert!(!describe.declares(group), "{group}");
    }
    assert_eq!(raw["session_capabilities_from"], "admission");
    assert_eq!(
        raw["retention"],
        serde_json::json!({"max_seconds":2592000,"delete_within_ms":3600000})
    );
    assert!(describe.majors[0].ops.iter().any(|op| op == "run.status"));
    assert_eq!(
        raw["max_bytes"],
        // Default and maximum are both the shared 3 MiB reply budget (READ_REPLY_MAX_BYTES).
        serde_json::json!({"default": 3145728, "maximum": 3145728})
    );
    assert_eq!(raw["majors"][0]["stability"], "alpha");
    // It describes the build, never the session, so a second answer is the same.
    let again = call(&mut rig, "role.describe", serde_json::Value::Null).await;
    assert_eq!(result(&again), raw);
    let body = refusal(&call(&mut rig, "role.describe", serde_json::json!({"x": 1})).await);
    assert_eq!(body["detail"]["field"], "x");
    rig.finish().await;
}

// ---- session.read ---------------------------------------------------------------------------

/// A never-written session exposes the unwritten snapshot without creating a log.
#[tokio::test]
async fn a_never_written_session_has_an_unwritten_page_and_head() {
    let mut rig = Rig::start("unwritten").await;
    let page = read(&mut rig, serde_json::json!({})).await;
    assert_eq!(page, ReadPage::empty(0, "unwritten"));
    let head: HeadMeta = serde_json::from_value(result(
        &call(&mut rig, "session.head", serde_json::json!({})).await,
    ))
    .unwrap();
    assert_eq!(head, HeadMeta::empty(0, "unwritten"));
    assert!(
        rig.records().await.is_empty(),
        "a read of an unwritten session wrote"
    );
    rig.finish().await;
}

/// The tail is the newest page, a range stops at its limit with a cursor, and `after_mid` reads
/// strictly after the message it names; every page carries the same snapshot identity.
#[tokio::test]
async fn tail_range_and_after_mid_reads() {
    let mut rig = three_turns("modes").await;
    let all = read(&mut rig, serde_json::json!({})).await;
    assert_eq!(ordinals(&all), [1, 2, 3, 4, 5, 6]);
    assert!(all.history_version > 0);
    assert_ne!(all.incarnation, "unwritten");

    let tail = read(&mut rig, serde_json::json!({"limit": 2})).await;
    assert_eq!(ordinals(&tail), [5, 6], "the tail is the newest page");
    assert_eq!(tail.next_from_ordinal, None);

    let range = read(&mut rig, serde_json::json!({"from_ordinal": 0, "limit": 2})).await;
    assert_eq!(ordinals(&range), [1, 2]);
    assert_eq!(range.next_from_ordinal, Some(3));
    let first = read(&mut rig, serde_json::json!({"from_ordinal": 1, "limit": 2})).await;
    assert_eq!(first, range, "zero aliases the first ordinal");

    let after = read(
        &mut rig,
        serde_json::json!({"after_mid": all.messages[1].mid, "history_version": all.history_version}),
    )
    .await;
    assert_eq!(ordinals(&after), [3, 4, 5, 6]);
    let past_end = read(
        &mut rig,
        serde_json::json!({"after_mid": all.messages[5].mid, "history_version": all.history_version}),
    )
    .await;
    assert!(past_end.messages.is_empty());
    for page in [&tail, &range, &after, &past_end] {
        assert_eq!(page.incarnation, all.incarnation);
        assert_eq!(page.history_version, all.history_version);
    }
    // Broca runs no hooks, so asking for originals adds nothing.
    let with_originals = read(&mut rig, serde_json::json!({"include_originals": true})).await;
    assert_eq!(with_originals, all);
    rig.finish().await;
}

/// Each refusal of a read is by name, writes nothing, and is never answered from the tail: a
/// stale snapshot version, a mid the session does not hold, and every misuse of the
/// request, naming the member at fault.
#[tokio::test]
async fn read_refusals_are_named_and_write_nothing() {
    let mut rig = three_turns("refusals").await;
    let page = read(&mut rig, serde_json::json!({})).await;
    let version = page.history_version;
    for params in [
        serde_json::json!({"history_version": version - 1}),
        serde_json::json!({"from_ordinal": 0, "history_version": version - 1}),
        serde_json::json!({"after_mid": page.messages[1].mid, "history_version": version - 1}),
        // Version mismatch is checked before even a malformed mid.
        serde_json::json!({"after_mid": "bogus", "history_version": version - 1}),
    ] {
        let body = refused_writing_nothing(&mut rig, "session.read", params).await;
        assert_eq!(body["code"], "history_changed", "{body}");
    }
    for mid in ["m6", "m99", "bogus", "m01", "m"] {
        let body = refused_writing_nothing(
            &mut rig,
            "session.read",
            serde_json::json!({"after_mid": mid, "history_version": version}),
        )
        .await;
        assert_eq!(body["code"], "unknown_mid", "{mid}: {body}");
    }
    for (params, field) in [
        (serde_json::json!({"after_mid": "m1"}), "history_version"),
        (
            serde_json::json!({"after_mid": "m1", "lineage_id": "legacy"}),
            "history_version",
        ),
        (
            serde_json::json!({"after_mid": page.messages[1].mid, "history_version": version, "from_ordinal": 0}),
            "from_ordinal",
        ),
        (serde_json::json!({"from_ordinl": 0}), "from_ordinl"),
        (serde_json::json!({"view": "model"}), "view"),
        (serde_json::json!({"max_bytes": "lots"}), "max_bytes"),
    ] {
        let body = refused_writing_nothing(&mut rig, "session.read", params.clone()).await;
        assert_eq!(body["code"], "invalid_params", "{params}: {body}");
        assert_eq!(body["detail"]["field"], field, "{params}: {body}");
    }
    for legacy in [
        serde_json::Value::Null,
        serde_json::json!("ignored-lineage"),
    ] {
        let compatible = read(&mut rig, serde_json::json!({"lineage_id": legacy})).await;
        assert_eq!(compatible, page, "legacy lineage selectors are input-only");
    }
    let body =
        refused_writing_nothing(&mut rig, "session.head", serde_json::json!({"x": true})).await;
    assert_eq!(
        (body["code"].as_str(), body["detail"]["field"].as_str()),
        (Some("invalid_params"), Some("x"))
    );
    rig.finish().await;
}

/// A byte cap stops a range page early with a cursor to the next message, keeps a tail page at
/// the newest message, and lets a single message larger than the cap through alone and whole.
#[tokio::test]
async fn pages_are_capped_by_bytes() {
    let mut rig = Rig::start("bytes").await;
    let big = format!("big-{}", "x".repeat(16 * 1024));
    rig.script.push(&[Step::Text("small"), Step::Text("after")]);
    rig.send("turn-0", &big, None).await;
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    rig.send("turn-1", "next", None).await;
    rig.wait_for_finished(2).await;
    rig.wait_idle().await;
    let all = read(&mut rig, serde_json::json!({})).await;
    assert_eq!(ordinals(&all), [1, 2, 3, 4]);

    let one = read(
        &mut rig,
        serde_json::json!({"from_ordinal": 2, "limit": 100, "max_bytes": 1}),
    )
    .await;
    assert_eq!(one.messages, all.messages[1..2]);
    assert_eq!(one.next_from_ordinal, Some(3));

    let oversize = read(
        &mut rig,
        serde_json::json!({"from_ordinal": 0, "limit": 100, "max_bytes": 1024}),
    )
    .await;
    assert_eq!(
        oversize.messages,
        all.messages[..1],
        "the big message comes alone"
    );
    assert!(oversize.messages[0].message.to_string().contains(&big));
    assert_eq!(oversize.next_from_ordinal, Some(2));

    let tail = read(&mut rig, serde_json::json!({"max_bytes": 1})).await;
    assert_eq!(
        tail.messages,
        all.messages[3..],
        "a capped tail keeps the newest"
    );
    assert_eq!(tail.next_from_ordinal, None);
    rig.finish().await;
}

// ---- dispatch attribution --------------------------------------------------------------------

/// Two sibling calls of one step get distinct keys, a model id recurring in a later step gets a
/// distinct key again, and every call names the module it was dispatched to. A key is the
/// key its dispatch intent recorded, under the session's lineage.
#[tokio::test]
async fn every_call_has_its_own_key_and_module() {
    let mut rig = Rig::start("keys").await;
    rig.script.push(&[
        Step::Calls(&["call_0", "call_1"]),
        Step::Call("call_0"),
        Step::Text("done"),
    ]);
    rig.send("turn-0", "go", None).await;
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let page = read(&mut rig, serde_json::json!({})).await;
    let entries: Vec<_> = page
        .messages
        .iter()
        .flat_map(|m| m.tool_calls.iter().cloned())
        .collect();
    let ids: Vec<&str> = entries.iter().map(|e| e.tool_call_id.as_str()).collect();
    assert_eq!(ids, ["call_0", "call_1", "call_0"]);
    let keys: Vec<String> = entries
        .iter()
        .map(|e| e.call_key.clone().unwrap())
        .collect();
    assert_eq!(
        keys.iter().collect::<std::collections::BTreeSet<_>>().len(),
        3,
        "{keys:?}"
    );
    // Each key is the one the call's own dispatch intent recorded, under the session's id.
    let expected: Vec<String> = rig
        .records()
        .await
        .iter()
        .filter_map(|record| match record {
            Record::ToolIntent(intent) => intent.call_key.clone(),
            _ => None,
        })
        .collect();
    assert_eq!(keys, expected);
    let sessions: std::collections::BTreeSet<_> = keys
        .iter()
        .map(|key| key.split_once(':').expect("<session id>:<seq>").0)
        .collect();
    assert_eq!(sessions.len(), 1, "{keys:?}");
    for entry in &entries {
        assert_eq!(entry.dispatched_to.as_deref(), Some(WORK_MODULE));
        assert!(!entry.indeterminate);
    }
    // A page holding only part of the transcript reports the same keys for the calls it holds.
    let later = read(&mut rig, serde_json::json!({"from_ordinal": 4, "limit": 1})).await;
    assert_eq!(
        later.messages[0].tool_calls[0].call_key.as_ref(),
        Some(&keys[2])
    );
    rig.finish().await;
}

// ---- session.head ---------------------------------------------------------------------------

/// `session.head` names the snapshot, the newest ordinal, the control head and the last run,
/// with the time of the newest write, and no message body.
#[tokio::test]
async fn head_reports_the_session_without_bodies() {
    let mut rig = three_turns("head").await;
    let page = read(&mut rig, serde_json::json!({})).await;
    let raw = result(&call(&mut rig, "session.head", serde_json::json!({})).await);
    let head: HeadMeta = serde_json::from_value(raw.clone()).unwrap();
    assert!(head.snapshot_consistent(), "{raw}");
    assert_eq!(head.incarnation, page.incarnation);
    assert_eq!(head.history_version, page.history_version);
    assert!(raw.get("lineage_id").is_none());
    assert_eq!(head.last_ordinal, Some(6));
    assert_eq!(head.head, page.head);
    let last = head.last_run_state.unwrap();
    assert_eq!(last.state, "completed");
    assert!(head.updated_at.is_some_and(|at| at > 1_700_000_000_000));
    assert!(raw.get("messages").is_none() && !raw.to_string().contains("prompt 0"));
    assert_eq!(head.first_ordinal, Some(1));
    assert_eq!(head.cut, None);
    assert!(
        head.history_version > 0,
        "first 0.8 serve supersedes version zero"
    );
    // A send's prompt is text only, a known fact, so `no` rather than `unknown`.
    assert_eq!(
        head.input
            .as_ref()
            .and_then(|input| input.carries_attachments()),
        Some(false)
    );
    rig.finish().await;
}

// ---- run.result -----------------------------------------------------------------------------

/// A completed run answers its final message: the text parts joined exactly as emitted,
/// reasoning left out; a final message with no text answers `text: ""`. An unknown run is
/// refused `unknown_run` and nothing is written.
#[tokio::test]
async fn run_result_answers_the_final_text() {
    use broca_protocol::content::ContentBlock;
    let mut rig = Rig::start("run-result").await;
    let text = |t: &str| ContentBlock::Text { text: t.into() };
    let reasoning = |t: &str| ContentBlock::Reasoning {
        text: t.into(),
        signature: None,
    };
    rig.script.push(&[
        Step::Call("call-1"),
        Step::Blocks(vec![
            text("{\"answer\":\""),
            reasoning("thinking"),
            text("yes"),
            text("\",\"n\":1}"),
        ]),
        Step::Blocks(vec![reasoning("only thinking")]),
    ]);
    let first = rig.send("turn-0", "go", None).await;
    let run_1 = result(&first)["run_id"].as_str().unwrap().to_owned();
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let second = rig.send("turn-1", "again", None).await;
    let run_2 = result(&second)["run_id"].as_str().unwrap().to_owned();
    rig.wait_for_finished(2).await;
    rig.wait_idle().await;

    let one: RunResult = serde_json::from_value(result(
        &call(&mut rig, "run.result", serde_json::json!({"run_id": run_1})).await,
    ))
    .unwrap();
    assert!(one.final_message_consistent());
    assert_eq!(one.state, "completed");
    let message = one.final_message.unwrap();
    assert_eq!(message.text, "{\"answer\":\"yes\",\"n\":1}");
    let page = read(&mut rig, serde_json::json!({"from_ordinal": 4, "limit": 1})).await;
    assert_eq!(message.ordinal, 4);
    assert_eq!(message.mid, page.messages[0].mid);

    let two: RunResult = serde_json::from_value(result(
        &call(&mut rig, "run.result", serde_json::json!({"run_id": run_2})).await,
    ))
    .unwrap();
    assert_eq!(two.final_message.unwrap().text, "");

    let body = refused_writing_nothing(
        &mut rig,
        "run.result",
        serde_json::json!({"run_id": "nope"}),
    )
    .await;
    assert_eq!(body["code"], "unknown_run");
    for (params, field) in [
        (serde_json::json!({}), "run_id"),
        (serde_json::json!({"run_id": run_1, "extra": 1}), "extra"),
    ] {
        let body = refused_writing_nothing(&mut rig, "run.result", params).await;
        assert_eq!(
            (body["code"].as_str(), body["detail"]["field"].as_str()),
            (Some("invalid_params"), Some(field))
        );
    }
    rig.finish().await;
}

#[tokio::test]
async fn run_result_refuses_a_final_message_that_exceeds_the_frame_limit() {
    use broca_protocol::ContentBlock;

    let mut rig = Rig::start("run-result-frame-limit").await;
    rig.script.push(&[Step::Blocks(vec![ContentBlock::Text {
        text: "x".repeat(4 * 1024 * 1024).into(),
    }])]);
    let sent = rig.send("large-answer", "answer", None).await;
    let run_id = result(&sent)["run_id"].as_str().unwrap().to_owned();
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;

    let body = refusal(
        &call(
            &mut rig,
            "run.result",
            serde_json::json!({"run_id": run_id}),
        )
        .await,
    );
    assert_eq!(body["code"], broca_wire::REPLY_TOO_LARGE);
    assert!(body["detail"]["bytes"].as_u64().unwrap() > broca_wire::SUBC_MAX_FRAME_BYTES);
    assert_eq!(body["detail"]["limit"], broca_wire::SUBC_MAX_FRAME_BYTES);
    assert_eq!(body["detail"]["subject"]["kind"], "message");
    assert_eq!(body["detail"]["subject"]["session"], rig.session.session);
    assert_eq!(body["detail"]["subject"]["ordinal"], 2);
    rig.finish().await;
}

// ---- session.subscribe and session.send ------------------------------------------------------

async fn assert_first_send_defaults(tag: &str, delivery: serde_json::Value) {
    {
        let mut rig = Rig::start(tag).await;
        rig.script.push(&[Step::Text("revived")]);
        let reply = call(
            &mut rig,
            "session.send",
            serde_json::json!({"prompt": "revive", "send_id": "revive-1", "delivery": delivery,
                "model": {"provider": "fake", "model": "fake"}}),
        )
        .await;
        assert!(matches!(
            result(&reply)["state"].as_str(),
            Some("active" | "pending")
        ));
        rig.wait_for_finished(1).await;
        let config_all = rig.run_configs().await;
        let config = &config_all[0];
        assert_eq!(
            config.model,
            broca_protocol::request::ModelHandle::new("fake", "fake")
        );
        assert_eq!(config.budget, None);
        assert_eq!(config.context_limit, None);
        assert_eq!(config.service_tier, None);
        rig.finish().await;
    }
}

#[tokio::test]
async fn a_fresh_idle_start_with_a_model_uses_defaults() {
    assert_first_send_defaults("fresh-idle-start", serde_json::json!(["idle_start"])).await;
}

#[tokio::test]
async fn a_fresh_steer_with_a_model_uses_defaults() {
    assert_first_send_defaults("fresh-steer", serde_json::json!("steer")).await;
}

#[tokio::test]
async fn an_idle_start_only_list_does_not_inherit_an_existing_episode() {
    let mut rig = Rig::start("idle-start-defaults").await;
    rig.script
        .push(&[Step::Text("first"), Step::Text("second")]);
    let first = call(
        &mut rig,
        "session.send",
        serde_json::json!({
            "prompt": "first", "send_id": "first",
            "model": {"provider": "fake", "model": "fake"},
            "generation": {"temperature": 0.25},
            "budget": {"nudges": [{"at_tool_calls": 5, "text": "finish soon"}]},
            "context_limit": 32000,
            "service_tier": "priority"
        }),
    )
    .await;
    assert_eq!(result(&first)["state"], "active");
    rig.wait_for_finished(1).await;
    let frozen_all = rig.run_configs().await;
    let frozen = frozen_all.first().unwrap();
    assert!(frozen.budget.is_some());
    assert_eq!(frozen.generation.temperature, Some(0.25));
    rig.wait_idle().await;
    let missing_model = refused_writing_nothing(
        &mut rig,
        "session.send",
        serde_json::json!({
            "prompt": "next", "send_id": "no-model", "delivery": ["idle_start"]
        }),
    )
    .await;
    assert_eq!(missing_model["detail"]["field"], "model");
    let next = call(
        &mut rig,
        "session.send",
        serde_json::json!({
            "prompt": "next", "send_id": "next", "delivery": ["idle_start"],
            "model": {"provider": "fake", "model": "fake"}
        }),
    )
    .await;
    result(&next);
    rig.wait_for_finished(2).await;
    let config_all = rig.run_configs().await;
    let config = &config_all[1];
    assert_eq!(config.budget, None);
    assert_eq!(config.context_limit, None);
    assert_eq!(config.service_tier, None);
    assert_eq!(config.generation.temperature, None);
    rig.finish().await;
}

#[tokio::test]
async fn joining_delivery_lists_inherit_the_existing_episode() {
    for mode in ["steer", "tool_result", "turn_system"] {
        let mut rig = Rig::start(&format!("joining-list-{mode}")).await;
        rig.script
            .push(&[Step::Text("first"), Step::Text("second")]);
        let first = call(
            &mut rig,
            "session.send",
            serde_json::json!({
                "prompt": "first", "send_id": "first",
                "model": {"provider": "fake", "model": "fake"},
                "generation": {"temperature": 0.25}
            }),
        )
        .await;
        result(&first);
        rig.wait_for_finished(1).await;
        rig.wait_idle().await;
        let next = call(
            &mut rig,
            "session.send",
            serde_json::json!({
                "prompt": "next", "send_id": "next", "delivery": [mode, "idle_start"]
            }),
        )
        .await;
        result(&next);
        rig.wait_for_finished(2).await;
        let config_all = rig.run_configs().await;
        let config = &config_all[1];
        assert_eq!(config.generation.temperature, Some(0.25), "{mode}");
        assert_eq!(
            config.model,
            broca_protocol::request::ModelHandle::new("fake", "fake")
        );
        rig.finish().await;
    }
}

#[tokio::test]
async fn a_fresh_send_without_a_model_refuses_precisely() {
    {
        let mut rig = Rig::start("missing-model").await;
        let body = refused_writing_nothing(
            &mut rig,
            "session.send",
            serde_json::json!({"prompt": "revive", "send_id": "missing", "delivery": "steer"}),
        )
        .await;
        assert!(
            body["message"]
                .as_str()
                .unwrap()
                .contains("model is required"),
            "{body}"
        );
        assert!(
            !body["message"]
                .as_str()
                .unwrap()
                .contains("runner parameters"),
            "{body}"
        );
        rig.finish().await;
    }
}

/// `session.subscribe` refuses an unknown member by name instead of attaching live.
#[tokio::test]
async fn subscribe_is_strict_on_unknown_members() {
    let (mut rig, _) = one_tool_turn("subscribe-strict").await;
    let frames = rig
        .subscribe_collect(
            9,
            serde_json::json!({"form": "start"}),
            Duration::from_millis(300),
        )
        .await;
    let body = refusal(&frames[0]);
    assert_eq!(
        (body["code"].as_str(), body["detail"]["field"].as_str()),
        (Some("invalid_params"), Some("form"))
    );
    let frames = rig
        .subscribe_collect(
            11,
            serde_json::json!({"from": "yesterday"}),
            Duration::from_millis(300),
        )
        .await;
    assert_eq!(refusal(&frames[0])["detail"]["field"], "from");
    rig.finish().await;
}

/// A steer into a session that has a model may omit `model` and runs on the session's frozen
/// one; any other send without a model is refused naming it, writing nothing.
#[tokio::test]
async fn a_steer_without_a_model_takes_the_sessions_frozen_model() {
    let (mut rig, _) = one_tool_turn("frozen-model").await;
    let body = refused_writing_nothing(
        &mut rig,
        "session.send",
        serde_json::json!({"prompt": "queued", "send_id": "turn-q"}),
    )
    .await;
    assert_eq!(
        (body["code"].as_str(), body["detail"]["field"].as_str()),
        (Some("invalid_params"), Some("model"))
    );
    rig.script.push(&[Step::Text("steered answer")]);
    let reply = call(
        &mut rig,
        "session.send",
        serde_json::json!({"prompt": "steer me", "send_id": "turn-s", "delivery": "steer"}),
    )
    .await;
    assert_eq!(result(&reply)["state"], "active");
    rig.wait_for_finished(2).await;
    let models: Vec<_> = rig
        .run_configs()
        .await
        .into_iter()
        .map(|config| config.model)
        .collect();
    assert_eq!(models.len(), 2);
    assert_eq!(models[0], models[1], "the steer ran on the frozen model");
    // A brand-new session has no frozen model, so a steer into it must name one.
    let mut fresh = Rig::start("frozen-model-fresh").await;
    let body = refused_writing_nothing(
        &mut fresh,
        "session.send",
        serde_json::json!({"prompt": "hi", "send_id": "s", "delivery": "steer"}),
    )
    .await;
    assert_eq!(body["detail"]["field"], "model");
    fresh.finish().await;
    rig.finish().await;
}

/// An unknown delivery mode is refused naming `delivery`, writing nothing.
#[tokio::test]
async fn an_unknown_delivery_is_refused_naming_it() {
    let (mut rig, _) = one_tool_turn("unknown-delivery").await;
    let body = refused_writing_nothing(
        &mut rig,
        "session.send",
        serde_json::json!({"prompt": "x", "send_id": "s2", "delivery": "broadcast",
            "model": {"provider": "fake", "model": "fake"}}),
    )
    .await;
    assert_eq!(
        (body["code"].as_str(), body["detail"]["field"].as_str()),
        (Some("invalid_params"), Some("delivery"))
    );
    rig.finish().await;
}

// ---- session.baseline -----------------------------------------------------------------------

/// Before the first send the baseline is `not_yet`. The reply to the send that admits the
/// session carries the baseline, as does every retry of that send, and `session.baseline`
/// answers the same: no fetched items and no composition digest, since the session was admitted
/// without a fetch plan, the tool names it was admitted with, and both generations 0. A later
/// send's reply carries none.
#[tokio::test]
async fn the_admitting_send_and_session_baseline_answer_the_same_baseline() {
    use cortexkit_role_llm_runner::{baseline::BaselineReply, send::SendReply};
    let mut rig = Rig::start("baseline").await;
    let before: BaselineReply = serde_json::from_value(result(
        &call(&mut rig, "session.baseline", serde_json::json!({})).await,
    ))
    .unwrap();
    assert!(before.is_not_yet() && before.baseline.is_none());

    rig.script.push(&[Step::Text("one"), Step::Text("two")]);
    let first = rig.send("turn-1", "hi", None).await;
    let hint = serde_json::from_value::<SendReply>(result(&first))
        .unwrap()
        .baseline
        .expect("the admitting send carries the baseline");
    assert!(hint.plan_consistent());
    assert_eq!(hint.composition_digest, None);
    assert!(hint.items.is_empty());
    assert_eq!(hint.tool_names, ["work"]);
    assert_eq!((hint.accepted_generation, hint.applied_generation), (0, 0));
    assert_eq!(
        result(&first)["baseline"],
        serde_json::json!({"items": [], "tool_names": ["work"], "accepted_generation": 0,
            "applied_generation": 0, "rungs":{"tools":["on_prefix_rebuild","immediately"],"system_text":["on_prefix_rebuild","immediately"]}})
    );
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let retry = rig.send("turn-1", "hi", None).await;
    assert_eq!(result(&retry)["baseline"], result(&first)["baseline"]);
    let answered: BaselineReply = serde_json::from_value(result(
        &call(&mut rig, "session.baseline", serde_json::Value::Null).await,
    ))
    .unwrap();
    assert_eq!(answered.state, "ready");
    assert_eq!(answered.baseline.as_ref(), Some(&hint));

    let second = rig.send("turn-2", "again", None).await;
    assert!(result(&second).get("baseline").is_none());
    // The refusal below must write nothing, which it proves by comparing WAL bytes before and
    // after. Let the second run finish first, or its own records land between the two reads.
    rig.wait_for_finished(2).await;
    rig.wait_idle().await;
    let body =
        refused_writing_nothing(&mut rig, "session.baseline", serde_json::json!({"x": 1})).await;
    assert_eq!(body["detail"]["field"], "x");
    rig.finish().await;
}

/// `session.baseline` is answered only to the principal of the session's first send; anyone
/// else is refused `scope_owner_mismatch`, and nothing is written.
#[tokio::test]
async fn session_baseline_is_answered_only_to_the_owner() {
    let mut rig = Rig::start("baseline-owner").await;
    let owner = subc_protocol::Principal::Reserved {
        module_id: "baseline-owner".into(),
    };
    rig.bind_as(7, Some(owner)).await;
    rig.script.push(&[Step::Text("one")]);
    rig.send("turn-1", "hi", None).await;
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let answered = call(&mut rig, "session.baseline", serde_json::json!({})).await;
    assert_eq!(result(&answered)["state"], "ready");

    rig.bind_as(
        13,
        Some(subc_protocol::Principal::Reserved {
            module_id: "stranger".into(),
        }),
    )
    .await;
    let before = rig.records().await;
    let refused = rig
        .request(
            13,
            serde_json::json!({"method": "session.baseline", "params": {}}),
        )
        .await;
    assert_eq!(refusal(&refused)["code"], "scope_owner_mismatch");
    assert_eq!(rig.records().await, before);
    rig.finish().await;
}

/// An interrupt that omits the model, sent while a run is active, is recorded as a queued send
/// carrying the session's frozen model (and its variant), not the serve layer's empty
/// placeholder, beside `inherit_frozen`, the parameters it takes from the session at admission,
/// and its run runs on that model.
#[tokio::test]
async fn a_pending_interrupt_without_a_model_records_the_frozen_model() {
    let mut rig = Rig::start("pending-frozen-model").await;
    rig.script.hold_model(true);
    let first = call(
        &mut rig,
        "session.send",
        serde_json::json!({"prompt": "task", "send_id": "turn-1",
            "model": {"provider": "fake", "model": "fake", "variant": "high"}}),
    )
    .await;
    assert_eq!(result(&first)["state"], "active");
    rig.wait_entered().await;
    let pending = call(
        &mut rig,
        "session.send",
        serde_json::json!({"prompt": "stop", "send_id": "stop-1", "delivery": "interrupt"}),
    )
    .await;
    assert_eq!(result(&pending)["state"], "pending", "{}", result(&pending));
    // A retry of the pending interrupt is the same send. The interrupt cancels the held run, so
    // by the time the retry arrives it may already have started: the retry then answers active
    // with the run that the pending reply's submission named (`sub-<run_id>`), never a new run.
    let retry = call(
        &mut rig,
        "session.send",
        serde_json::json!({"prompt": "stop", "send_id": "stop-1", "delivery": "interrupt"}),
    )
    .await;
    let submission = result(&pending)["submission_id"]
        .as_str()
        .expect("a pending reply names its submission")
        .to_owned();
    match result(&retry)["state"].as_str() {
        Some("pending") => assert_eq!(result(&retry), result(&pending)),
        Some("active") => assert_eq!(
            result(&retry)["run_id"]
                .as_str()
                .map(|run| format!("sub-{run}")),
            Some(submission),
            "{}",
            result(&retry)
        ),
        _ => panic!("retry of a pending interrupt: {}", result(&retry)),
    }

    let queued = rig
        .queued_requests()
        .await
        .into_iter()
        .next()
        .expect("the interrupt is recorded as a queued send");
    assert_eq!(
        queued.model,
        broca_protocol::request::ModelHandle::new("fake", "fake")
    );
    assert_eq!(queued.variant.as_deref(), Some("high"));
    assert!(queued
        .inherit_frozen
        .contains(&broca_protocol::request::InheritedParam::Model));
    assert!(queued
        .inherit_frozen
        .contains(&broca_protocol::request::InheritedParam::Generation));

    rig.script.hold_model(false);
    rig.wait_for_finished(2).await;
    let models: Vec<_> = rig
        .run_configs()
        .await
        .into_iter()
        .map(|config| (config.model, config.variant))
        .collect();
    assert_eq!(models.len(), 2);
    assert_eq!(
        models[0], models[1],
        "the interrupt ran on the frozen model"
    );
    rig.finish().await;
}

// ---- client_message_id ----------------------------------------------------------------------

/// A send's `client_message_id` (llm-runner 0.5.1 §9.11) through the real serve path. It is
/// stored only in the identity digest the send's run start (or queued send) records; it is never
/// in `session.read`. A retry with the same id is
/// answered as a retry; one with another id, or without the id, is refused `send_id_reuse`
/// naming `client_message_id` and writes nothing. A malformed id, and an id on a send without
/// a `send_id`, are refused `invalid_params` naming it and write nothing.
#[tokio::test]
async fn client_message_id_is_part_of_the_send_identity_and_never_read() {
    let mut rig = Rig::start("client-message-id").await;
    rig.script.push(&[Step::Text("answer")]);
    let with_id = serde_json::json!({"client_message_id": "msg-1"});
    let first = rig.send_with("turn-1", "hi", None, with_id.clone()).await;
    let run_id = result(&first)["run_id"].as_str().unwrap().to_owned();
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;

    let records = rig.records().await;
    let digest = records
        .iter()
        .find_map(|record| match record {
            Record::RunStart(start) => start.request_identity_digest.clone(),
            _ => None,
        })
        .expect("the keyed send recorded its identity");
    assert_eq!(
        broca_core::digest_client_message_id(&digest).as_deref(),
        Some("msg-1")
    );

    let read = call(&mut rig, "session.read", serde_json::json!({})).await;
    assert!(
        !String::from_utf8_lossy(&read.body).contains("msg-1"),
        "session.read carries the client_message_id: {}",
        String::from_utf8_lossy(&read.body)
    );

    let retry = rig.send_with("turn-1", "hi", None, with_id.clone()).await;
    assert_eq!(result(&retry)["run_id"], run_id.as_str());

    for (extra, code) in [
        (
            serde_json::json!({"client_message_id": "msg-2"}),
            "send_id_reuse",
        ),
        (serde_json::json!({}), "send_id_reuse"),
        (
            serde_json::json!({"client_message_id": "bad id"}),
            "invalid_params",
        ),
        (
            serde_json::json!({"client_message_id": "msg-1", "send_id": null}),
            "invalid_params",
        ),
    ] {
        let body =
            refused_writing_nothing(&mut rig, "session.send", keyed("turn-1", extra.clone())).await;
        assert_eq!(body["code"], code, "{extra}: {body}");
        assert_eq!(
            body["detail"]["field"], "client_message_id",
            "{extra}: {body}"
        );
    }

    // A send queued behind a running turn keeps its id in the `Queued` record's identity, and
    // a retry of the still-pending send is compared against it.
    rig.script.push(&[Step::Text("busy")]);
    rig.script.push(&[Step::Text("later")]);
    rig.script.hold_model(true);
    rig.send("turn-2", "busy", None).await;
    rig.wait_entered().await;
    let queued_with = serde_json::json!({"client_message_id": "msg-3", "tools":null});
    let queued = call(
        &mut rig,
        "session.send",
        keyed("turn-3", queued_with.clone()),
    )
    .await;
    assert_eq!(result(&queued)["state"], "pending", "{}", result(&queued));
    let queued_receipt = rig
        .records()
        .await
        .into_iter()
        .find_map(|record| match record {
            Record::QueuedSend(send) => send.receipt.map(|receipt| receipt.parse()),
            _ => None,
        })
        .expect("the queued send has a receipt");
    assert_eq!(
        queued_receipt["request_identity_digest"]
            .as_str()
            .and_then(broca_core::digest_client_message_id)
            .as_deref(),
        Some("msg-3"),
        "{queued_receipt}"
    );
    let again = call(&mut rig, "session.send", keyed("turn-3", queued_with)).await;
    assert_eq!(result(&again)["state"], "pending", "{}", result(&again));
    for extra in [
        serde_json::json!({"client_message_id": "msg-4", "tools":null}),
        serde_json::json!({"tools":null}),
    ] {
        let body =
            refused_writing_nothing(&mut rig, "session.send", keyed("turn-3", extra.clone())).await;
        assert_eq!(body["code"], "send_id_reuse", "{extra}: {body}");
        assert_eq!(
            body["detail"]["field"], "client_message_id",
            "{extra}: {body}"
        );
    }
    rig.script.hold_model(false);
    rig.wait_for_finished(3).await;
    rig.finish().await;
}

/// `session.send` params for the rig's `work` tool session under `send_id`, with `extra` merged
/// in; a `null` in `extra` removes that member.
fn keyed(send_id: &str, extra: serde_json::Value) -> serde_json::Value {
    let mut params = serde_json::json!({
        "prompt": "hi",
        "send_id": send_id,
        "model": {"provider": "fake", "model": "fake"},
        "tools": [{"name": "work", "description": "does work", "input_schema": {"type": "object"}}]
    });
    for (key, value) in extra.as_object().unwrap() {
        if value.is_null() {
            params.as_object_mut().unwrap().remove(key);
        } else {
            params[key] = value.clone();
        }
    }
    params
}

#[tokio::test]
async fn unsupported_output_refs_precedes_plan_overrides_and_never_writes() {
    use cortexkit_role_llm_runner::{capabilities as groups, describe::RoleDescribe, ops};
    let mut rig = Rig::start_with_scopes("unsupported-output-refs").await;
    let describe = call(&mut rig, ops::ROLE_DESCRIBE, serde_json::json!({})).await;
    let describe: RoleDescribe = serde_json::from_value(result(&describe)).unwrap();
    assert!(describe.declares(groups::OUTPUT_REFS));
    // This rig installs no plan admitter. Even though production discovery advertises the
    // reader, a handler without an output-reference admitter must refuse before overrides.
    // A plan-less send, sent first so the session has a baseline: `session.refresh` below
    // refuses a session with no baseline before it ever reaches the plan, and this test is
    // about the plan check's place, not about that refusal.
    rig.script.push(&[Step::Text("baseline")]);
    let baseline = call(
        &mut rig,
        ops::SESSION_SEND,
        serde_json::json!({
            "send_id":"unsupported-baseline","prompt":"baseline",
            "model":{"provider":"fake","model":"fake"},"tools":[]
        }),
    )
    .await;
    assert_eq!(result(&baseline)["state"], "active", "baseline send");
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    // The checks below compare against this head and this WAL, so nothing they claim to have
    // refused may have written.
    for flag in [true, false] {
        let before = result(&call(&mut rig, ops::SESSION_HEAD, serde_json::json!({})).await);
        let wal = rig.records().await;
        let plan = serde_json::json!({"items":[{"output_refs":flag}]});
        let mut requests = vec![(
            ops::SESSION_SEND,
            serde_json::json!({
                "send_id":format!("unsupported-send-{flag}"),"prompt":"must not be stored",
                "model":{"provider":"fake","model":"fake"},
                "tools":[],"plan":plan.clone()
            }),
        )];
        if describe.declares(groups::SESSION_CHANGE) {
            requests.push((
                ops::SESSION_REFRESH,
                serde_json::json!({"plan":plan.clone(),"policy":"immediately","generation":1}),
            ));
        }
        if describe.declares(groups::ADOPT) {
            requests.push((
                ops::SESSION_ADOPT,
                serde_json::json!({"plan":plan,"send_id":format!("unsupported-adopt-{flag}")}),
            ));
        }
        for (method, params) in requests {
            let reply = call(&mut rig, method, params).await;
            let error = refusal(&reply);
            assert_eq!(error["code"], "invalid_params", "{method}: {error}");
            assert_eq!(
                error["detail"]["field"], "plan.items[].output_refs",
                "{method}: {error}"
            );
            assert_eq!(
                rig.records().await,
                wal,
                "{method} must refuse before writing"
            );
            let after = result(&call(&mut rig, ops::SESSION_HEAD, serde_json::json!({})).await);
            assert_eq!(after, before, "{method} must preserve the session head");
        }
    }
    rig.finish().await;
}

fn assert_stream_sequences(frames: &[Frame]) -> Vec<serde_json::Value> {
    let values: Vec<serde_json::Value> = frames
        .iter()
        .map(|frame| {
            assert_eq!(
                frame.header.ty,
                FrameType::StreamData,
                "{}",
                String::from_utf8_lossy(&frame.body)
            );
            serde_json::from_slice(&frame.body).unwrap()
        })
        .collect();
    assert!(!values.is_empty());
    for (index, value) in values.iter().enumerate() {
        assert_eq!(value["seq"], index as u64 + 1, "{value}");
        // The contract makes `event` optional: a unit with no lifecycle event carries none.
        if !value["event"].is_null() {
            cortexkit_role_llm_runner::live::LiveEvent::decode_strict(&value["event"]).unwrap();
        }
    }
    values
}

#[tokio::test]
async fn one_stream_sequence_counts_both_lanes_and_restarts_for_each_subscribe() {
    let mut rig = Rig::start("live-sequence").await;
    rig.script.push(&[Step::Stream(vec![ContentBlock::Text {
        text: "streamed text".into(),
    }])]);
    rig.script.hold_model(true);
    rig.send("sequence-turn", "task", None).await;
    let script = rig.script.clone();
    let first = rig
        .subscribe_collect_after_attach(
            9,
            serde_json::json!({"from":"start"}),
            Duration::from_secs(1),
            move || script.hold_model(false),
        )
        .await;
    let values = assert_stream_sequences(&first);
    assert!(values.iter().any(|value| value["kind"] == "control"));
    assert!(
        values.iter().any(|value| value["kind"] == "display"),
        "the forwarder must count actual display arrivals"
    );
    assert!(values
        .iter()
        .any(|value| value["event"]["type"] == "run_finished"));
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let second = rig
        .subscribe_collect(
            10,
            serde_json::json!({"from":"start"}),
            Duration::from_millis(500),
        )
        .await;
    assert_stream_sequences(&second);
    rig.finish().await;
}

#[tokio::test]
async fn live_tool_observations_and_control_bytes_survive_a_real_serve_restart() {
    assert_tool_replay_survives_restart("live-tool-restart", None).await;
}

#[tokio::test]
async fn delayed_tool_replay_survives_a_real_serve_restart() {
    // The transport withholds replay for longer than the quiet window, even when
    // the server has already produced it. Silence must not stand in for attachment.
    assert_tool_replay_survives_restart("delayed-tool-restart", Some(Duration::from_secs(1))).await;
}

async fn assert_tool_replay_survives_restart(tag: &str, replay_delay: Option<Duration>) {
    let (mut rig, _) = one_tool_turn(tag).await;
    let records = rig.records().await;
    let started = records
        .iter()
        .find_map(|record| match record {
            Record::ToolIntent(intent) => intent.dispatched_at_ms,
            _ => None,
        })
        .expect("the real tool call recorded its dispatch observation");
    let finished = records
        .iter()
        .find_map(|record| match record {
            Record::ToolOutcome(outcome) => outcome.finished_at_ms,
            _ => None,
        })
        .expect("the real tool call recorded its finish observation");
    if let Some(delay) = replay_delay {
        rig.delay_next_subscription_reads(delay);
    }
    // The display reset follows the durable control replay. Wait for that barrier
    // before starting the quiet window, so a slow attachment cannot look empty.
    let before = rig
        .subscribe_collect_after_display_reset(
            9,
            serde_json::json!({"from":"start"}),
            Duration::from_millis(500),
            || {},
        )
        .await;
    let values = assert_stream_sequences(&before);
    assert!(values
        .iter()
        .any(|value| value["event"]["type"] == "tool_started"
            && value["event"]["started_at"] == started));
    assert!(values
        .iter()
        .any(|value| value["event"]["type"] == "tool_finished"
            && value["event"]["finished_at"] == finished));
    // Closing a connection alone does not release a live registry's writer lease.
    let bytes = rig.records().await;
    assert!(rig.registry().stop_and_seal().await);
    assert_eq!(
        rig.records().await,
        bytes,
        "stopping an already-finished run writes nothing"
    );
    rig = rig.restart().await;
    if let Some(delay) = replay_delay {
        rig.delay_next_subscription_reads(delay);
    }
    let after = rig
        .subscribe_collect_after_display_reset(
            9,
            serde_json::json!({"from":"start"}),
            Duration::from_millis(500),
            || {},
        )
        .await;
    assert_stream_sequences(&after);
    assert_eq!(
        before.iter().map(|frame| &frame.body).collect::<Vec<_>>(),
        after.iter().map(|frame| &frame.body).collect::<Vec<_>>()
    );
    rig.finish().await;
}

#[tokio::test]
async fn stateful_text_control_is_plain_text_live_and_in_attach_replay() {
    use broca_protocol::content::{TextProviderFormat, TextProviderState};
    use broca_protocol::{ToolCall, ToolCallId};

    fn assistant_frame(frames: &[Frame]) -> Vec<u8> {
        frames
            .iter()
            .find(|frame| {
                let value: serde_json::Value = serde_json::from_slice(&frame.body).unwrap();
                value["kind"] == "control" && value["unit"]["type"] == "assistant_message"
            })
            .expect("assembled assistant message crossed serve")
            .body
            .clone()
    }

    #[derive(serde::Deserialize)]
    struct ControlFrame {
        unit: Box<serde_json::value::RawValue>,
        event: serde_json::Value,
    }

    let mut captures = Vec::new();
    for (name, format, data) in [
        (
            "responses",
            TextProviderFormat::OpenAiResponses,
            r#"{"id":"synthetic-response-item","type":"message","phase":"final_answer"}"#,
        ),
        (
            "gemini",
            TextProviderFormat::Gemini,
            r#"{"thoughtSignature":"synthetic-text-signature"}"#,
        ),
    ] {
        let content = vec![
            ContentBlock::TextWithState {
                text: "answer".into(),
                provider_state: TextProviderState {
                    format,
                    data: data.into(),
                },
            },
            ContentBlock::Reasoning {
                text: "thinking".into(),
                signature: Some("synthetic-reasoning-signature".into()),
            },
            ContentBlock::ToolCall(ToolCall {
                tool_call_id: ToolCallId("synthetic-call".into()),
                tool_name: "work".into(),
                input: serde_json::json!({"path": "/synthetic/input.txt"}).into(),
                thought_signature: Some("synthetic-tool-signature".into()),
            }),
        ];
        let mut rig = Rig::start(&format!("subscribe-stateful-{name}")).await;
        rig.script
            .push(&[Step::Stream(content.clone()), Step::Text("done")]);
        rig.script.hold_stream(true);
        rig.send("synthetic-send", "hi", None).await;
        rig.wait_entered().await;
        let script = rig.script.clone();
        let live = assistant_frame(
            &rig.subscribe_collect_after_display_reset(
                20,
                serde_json::json!({"from":"start"}),
                Duration::from_secs(5),
                move || script.hold_stream(false),
            )
            .await,
        );
        let records = rig.wait_for_finished(1).await;
        let stored = records.iter().find_map(|record| match record {
            Record::Assistant(assistant) => Some(&assistant.message.content),
            _ => None,
        });
        assert_eq!(stored, Some(&content));
        rig.wait_idle().await;
        let page = read(&mut rig, serde_json::json!({})).await;
        let descriptors = read(&mut rig, serde_json::json!({"media":"descriptors"})).await;
        let wal = rig.records().await;
        assert!(rig.registry().stop_and_seal().await);
        rig = rig.restart().await;
        let replay = assistant_frame(
            &rig.subscribe_collect(
                21,
                serde_json::json!({"from":"start"}),
                Duration::from_secs(5),
            )
            .await,
        );
        // Reopening the session appends only its new writer's record; attaching adds nothing.
        let after = rig.records().await;
        assert_eq!(
            after[..wal.len()],
            wal[..],
            "attaching never rewrites provider state"
        );
        assert!(
            after[wal.len()..]
                .iter()
                .all(|record| matches!(record, Record::WriterOpened(_))),
            "{:?}",
            &after[wal.len()..]
        );
        rig.finish().await;
        eprintln!(
            "SUBSCRIBE_FLATTEN_SAMPLE {name}.live {}",
            std::str::from_utf8(&live).unwrap()
        );
        eprintln!(
            "SUBSCRIBE_FLATTEN_SAMPLE {name}.replay {}",
            std::str::from_utf8(&replay).unwrap()
        );
        captures.push((content, live, replay, page, descriptors));
    }

    for (content, live, replay, page, descriptors) in captures {
        let live: ControlFrame = serde_json::from_slice(&live).unwrap();
        let replay: ControlFrame = serde_json::from_slice(&replay).unwrap();
        assert_eq!(live.unit.get().as_bytes(), replay.unit.get().as_bytes());
        assert_eq!(
            live.event,
            serde_json::json!({"type":"message_finished","mid":page.messages[1].mid})
        );
        assert_eq!(replay.event, live.event);
        cortexkit_role_llm_runner::live::LiveEvent::decode_strict(&live.event).unwrap();
        let unit: serde_json::Value = serde_json::from_str(live.unit.get()).unwrap();
        let expected = serde_json::json!([
            {"type":"text","text":"answer"},
            content[1],
            content[2],
        ]);
        assert_eq!(unit["message"]["content"], expected);
        assert!(!live.unit.get().contains("provider_state"));
        let entry = page
            .messages
            .iter()
            .find(|entry| entry.ordinal == 2)
            .unwrap();
        assert_eq!(entry.message["content"], expected);
        let entry = descriptors
            .messages
            .iter()
            .find(|entry| entry.ordinal == 2)
            .unwrap();
        assert_eq!(entry.message["content"], expected);
    }
}

async fn pin_live_sample(rig: &mut Rig, frames: &[Frame], run_id: &str, file: &str) {
    let page = read(rig, serde_json::json!({})).await;
    let lineage = &page.incarnation;
    let values = assert_stream_sequences(frames);
    let mut lines = Vec::new();
    for (frame, value) in frames.iter().zip(&values) {
        let event = &value["event"];
        // The contract makes `event` optional: a unit with no lifecycle event carries none.
        if !event.is_null() {
            cortexkit_role_llm_runner::live::LiveEvent::decode_strict(event).unwrap();
        }
        if let (Some(mid), Some(part)) = (event["mid"].as_str(), event["part"].as_str()) {
            let entry = page
                .messages
                .iter()
                .find(|m| m.mid == mid)
                .expect("live mid is stored");
            let stored: Message = serde_json::from_value(entry.message.clone()).unwrap();
            let index: usize = part.parse().unwrap();
            assert!(
                index < stored.content.len(),
                "display part lies in the stored message"
            );
            match (&stored.content[index], event["type"].as_str().unwrap()) {
                (ContentBlock::Text { .. }, "text_delta")
                | (ContentBlock::Reasoning { .. }, "reasoning_delta")
                | (ContentBlock::ToolCall(_), "tool_input_delta") => {}
                pair => panic!("delta addresses a different block: {pair:?}"),
            }
        }
        let mut body = normalized(&frame.body, run_id, lineage);
        for field in ["started_at", "finished_at"] {
            if let Some(at) = event[field].as_u64() {
                body = body.replace(
                    &format!("\"{field}\":{at}"),
                    &format!("\"{field}\":1700000000000"),
                );
            }
        }
        lines.push(body);
    }
    let sample = lines.join("\n") + "\n";
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(golden_path(file), &sample).unwrap();
    }
    assert_eq!(sample.trim_end(), pinned(file));
}

#[tokio::test]
async fn live_display_samples_match_stored_parts_and_attach_replays_current_step() {
    // The control lane carries only durable units, and a step's start is written without a
    // sync: nothing a crash could lose depends on it, since a resumed run sends the request
    // again. So its `step_started` control is published when the next sync makes it durable,
    // which is after the step's first display deltas, and an attach made while step 1 streams
    // gets the display snapshot with no step controls before it. Display deltas name their
    // message themselves, so a reader never needs the control to place them.
    use broca_protocol::{ToolCall, ToolCallId};
    let tool = ContentBlock::ToolCall(ToolCall {
        thought_signature: None,
        tool_call_id: ToolCallId("sample-call".into()),
        tool_name: "work".into(),
        input: serde_json::json!({}).into(),
    });
    for name in ["text", "tool", "reasoning", "steer", "attach"] {
        let mut rig = Rig::start(&format!("live-sample-{name}")).await;
        let content = match name {
            "tool" | "steer" => vec![tool.clone()],
            "reasoning" => vec![
                ContentBlock::Reasoning {
                    text: "thinking".into(),
                    signature: Some("opaque".into()),
                },
                ContentBlock::Text {
                    text: "answer".into(),
                },
            ],
            _ => vec![ContentBlock::Text {
                text: "answer".into(),
            }],
        };
        let last_part = content.len() as u64 - 1;
        rig.script
            .push(&[Step::Stream(content), Step::Text("done")]);
        rig.script.hold_stream(true);
        let reply = rig.send("sample", "hi", None).await;
        let run_id = result(&reply)["run_id"].as_str().unwrap().to_owned();
        rig.wait_entered().await;
        if name == "steer" {
            rig.send("sample-steer", "steered", Some("steer")).await;
        }
        let frames = if name == "attach" {
            let mut frames = Vec::new();
            for channel in 20..28 {
                frames = rig
                    .subscribe_collect(channel, serde_json::json!({}), Duration::from_millis(100))
                    .await;
                if frames.iter().any(|f| {
                    serde_json::from_slice::<serde_json::Value>(&f.body).unwrap()["event"]["part"]
                        == "0"
                }) {
                    break;
                }
            }
            let display: Vec<_> = frames
                .iter()
                .map(|f| serde_json::from_slice::<serde_json::Value>(&f.body).unwrap())
                .filter(|v| v["kind"] == "display")
                .collect();
            assert_eq!(display.len(), 2, "reset then one coalesced part");
            assert_eq!(display[0]["event"]["type"], "display_reset");
            rig.script.hold_stream(false);
            frames
        } else {
            rig.subscribe_release_after_part(20, last_part).await
        };
        rig.wait_for_finished(1).await;
        rig.wait_idle().await;
        let page = read(&mut rig, serde_json::json!({"from_ordinal":2, "limit":1})).await;
        let mid = &page.messages[0].mid;
        assert!(frames
            .iter()
            .any(
                |f| serde_json::from_slice::<serde_json::Value>(&f.body).unwrap()["event"]["mid"]
                    == *mid
            ));
        pin_live_sample(
            &mut rig,
            &frames,
            &run_id,
            &format!("session_subscribe.live_{name}.jsonl"),
        )
        .await;
        rig.finish().await;
    }
}

#[tokio::test]
async fn live_display_mid_after_reopen_comes_from_seeded_placement() {
    let mut rig = Rig::start("display-reopen").await;
    rig.script.push(&[Step::Text("first")]);
    rig.send("first", "first prompt", None).await;
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    rig.wait_closed().await;
    let mut rig = rig.restart().await;
    rig.script.push(&[Step::Stream(vec![ContentBlock::Text {
        text: "second".into(),
    }])]);
    rig.script.hold_stream(true);
    let admitted = rig.send("second", "second prompt", None).await;
    assert_eq!(
        admitted.header.ty,
        FrameType::Response,
        "{}",
        String::from_utf8_lossy(&admitted.body)
    );
    rig.wait_entered().await;
    let frames = rig.subscribe_release_after_part(20, 0).await;
    rig.wait_for_finished(2).await;
    rig.wait_idle().await;
    let page = read(&mut rig, serde_json::json!({})).await;
    let stored = page.messages.last().unwrap();
    let message: Message = serde_json::from_value(stored.message.clone()).unwrap();
    assert_eq!(message.role, Role::Assistant);
    assert_eq!(
        message.content,
        vec![ContentBlock::Text {
            text: "second".into()
        }]
    );
    let delta = frames
        .iter()
        .map(|f| serde_json::from_slice::<serde_json::Value>(&f.body).unwrap())
        .find(|v| v["event"]["type"] == "text_delta")
        .unwrap();
    assert_eq!(delta["event"]["mid"], stored.mid);
    assert_eq!(stored.ordinal, 4);
    rig.finish().await;
}

#[tokio::test]
async fn live_finalize_filter_restreams_the_stored_block_indices() {
    let mut rig = Rig::start("display-finalize").await;
    rig.script.push(&[
        Step::Call("first-call"),
        Step::Stream(vec![
            ContentBlock::ToolCall(broca_protocol::ToolCall {
                thought_signature: None,
                tool_call_id: broca_protocol::ToolCallId("omitted-call".into()),
                tool_name: "work".into(),
                input: serde_json::json!({}).into(),
            }),
            ContentBlock::Text {
                text: "final answer".into(),
            },
        ]),
    ]);
    rig.script.hold_tool(true);
    rig.send_with(
        "finalize",
        "task",
        None,
        serde_json::json!({"budget":{"finalize_at_tool_calls":1,"finalize_text":"finish"}}),
    )
    .await;
    rig.wait_entered().await;
    rig.script.hold_stream(true);
    rig.script.hold_tool(false);
    rig.wait_entered().await;
    let frames = rig.subscribe_release_after_part(20, 1).await;
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let values = assert_stream_sequences(&frames);
    let last_reset = values
        .iter()
        .rposition(|v| v["kind"] == "display" && v["event"]["type"] == "display_reset")
        .unwrap();
    let final_deltas: Vec<_> = values[last_reset + 1..]
        .iter()
        .filter(|v| v["kind"] == "display")
        .collect();
    assert_eq!(
        final_deltas.len(),
        1,
        "the omitted tool's live row is wiped: {final_deltas:?}"
    );
    assert_eq!(final_deltas[0]["event"]["type"], "text_delta");
    assert_eq!(final_deltas[0]["event"]["part"], "0");
    let page = read(&mut rig, serde_json::json!({})).await;
    let stored = page.messages.last().unwrap();
    assert_eq!(final_deltas[0]["event"]["mid"], stored.mid);
    let message: Message = serde_json::from_value(stored.message.clone()).unwrap();
    assert_eq!(
        message.content,
        vec![ContentBlock::Text {
            text: "final answer".into()
        }]
    );
    rig.finish().await;
}

#[tokio::test]
async fn live_subscribe_limits_cut_only_typed_arguments_not_existing_units() {
    let (mut rig, _) = one_tool_turn("live-limits").await;
    let unlimited = rig
        .subscribe_collect(
            20,
            serde_json::json!({"from":"start"}),
            Duration::from_millis(100),
        )
        .await;
    let unlimited = assert_stream_sequences(&unlimited);
    let unlimited_started = unlimited
        .iter()
        .find(|v| v["event"]["type"] == "tool_started")
        .expect("tool lifecycle replays without limits");
    assert!(
        unlimited_started["event"]["call_key"].is_string(),
        "the projector attaches the call key: {unlimited_started}"
    );
    let frames = rig.subscribe_collect(20, serde_json::json!({"from":"start","limits":{"arguments_bytes":1,"result_bytes":1,"error_result_bytes":1}}), Duration::from_millis(100)).await;
    let values = assert_stream_sequences(&frames);
    let started = values
        .iter()
        .find(|v| v["event"]["type"] == "tool_started")
        .expect("tool lifecycle replays");
    assert_eq!(started["event"]["arguments"], "{");
    assert_eq!(started["event"]["arguments_truncated"], true);
    // Cutting the arguments must not drop the call identity a client joins on.
    assert_eq!(
        started["event"]["call_key"],
        unlimited_started["event"]["call_key"]
    );
    assert_eq!(
        started["event"]["schema_pin"],
        unlimited_started["event"]["schema_pin"]
    );
    let call = values
        .iter()
        .find(|v| v["unit"]["type"] == "tool_call")
        .expect("the existing authoritative call unit replays separately");
    assert_eq!(call["unit"]["call"]["input"], serde_json::json!({}));
    rig.finish().await;
}

#[tokio::test]
async fn live_display_after_steer_uses_the_next_step_placement() {
    let mut rig = Rig::start("display-steer-next").await;
    rig.script.push(&[
        Step::Stream(vec![ContentBlock::ToolCall(broca_protocol::ToolCall {
            thought_signature: None,
            tool_call_id: broca_protocol::ToolCallId("steer-call".into()),
            tool_name: "work".into(),
            input: serde_json::json!({}).into(),
        })]),
        Step::Stream(vec![ContentBlock::Text {
            text: "after steer".into(),
        }]),
    ]);
    rig.script.hold_stream(true);
    rig.send("first", "task", None).await;
    rig.wait_entered().await;
    rig.send("steer", "new direction", Some("steer")).await;
    let frames = rig.subscribe_release_after_part(20, 0).await;
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let values = assert_stream_sequences(&frames);
    let delta = values
        .iter()
        .find(|v| v["event"]["type"] == "text_delta" && v["event"]["delta"] == "after steer")
        .unwrap();
    let page = read(&mut rig, serde_json::json!({})).await;
    assert_eq!(page.messages.last().unwrap().ordinal, 5);
    assert_eq!(delta["event"]["mid"], page.messages.last().unwrap().mid);
    rig.finish().await;
}

#[tokio::test]
async fn live_attach_after_cancel_discards_the_uncommitted_step() {
    let mut rig = Rig::start("display-cancelled-step").await;
    rig.script.push(&[Step::Stream(vec![ContentBlock::Text {
        text: "discarded partial".into(),
    }])]);
    rig.script.hold_stream(true);
    let reply = rig.send("partial", "task", None).await;
    let run_id = result(&reply)["run_id"].as_str().unwrap().to_owned();
    rig.wait_entered().await;
    // The held model has streamed its partial; wait for that live delta itself, since on a
    // loaded runner it can arrive after any fixed quiet window.
    let before = rig
        .subscribe_collect_until(20, serde_json::json!({}), Duration::from_millis(100), |f| {
            serde_json::from_slice::<serde_json::Value>(&f.body).unwrap()["event"]["delta"]
                == "discarded partial"
        })
        .await;
    assert!(before.iter().any(
        |f| serde_json::from_slice::<serde_json::Value>(&f.body).unwrap()["event"]["delta"]
            == "discarded partial"
    ));
    rig.cancel(&run_id).await;
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let page = read(&mut rig, serde_json::json!({})).await;
    assert!(page
        .messages
        .iter()
        .all(
            |entry| serde_json::from_value::<Message>(entry.message.clone())
                .unwrap()
                .role
                != Role::Assistant
        ));
    let frames = rig
        .subscribe_collect(21, serde_json::json!({}), Duration::from_millis(100))
        .await;
    let values = assert_stream_sequences(&frames);
    assert_eq!(
        values.len(),
        1,
        "an ended run has no current-step display prefix to replay"
    );
    assert_eq!(values[0]["event"]["type"], "display_reset");
    rig.finish().await;
}

#[tokio::test]
async fn late_subscriber_gets_context_captured_before_the_step_streamed() {
    let mut rig = Rig::start("display-context-late-attach").await;
    rig.script.push(&[Step::Stream(vec![ContentBlock::Text {
        text: "before attach".into(),
    }])]);
    rig.script.hold_stream(true);
    rig.send("late", "task", None).await;
    rig.wait_entered().await;
    assert_eq!(
        rig.script.prompts.lock().unwrap().len(),
        1,
        "the provider ran before any subscribe call"
    );
    let frames = rig.subscribe_release_after_part(20, 0).await;
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let delta = frames
        .iter()
        .map(|f| serde_json::from_slice::<serde_json::Value>(&f.body).unwrap())
        .find(|v| v["event"]["type"] == "text_delta")
        .expect("late attach replays the streamed part");
    assert_eq!(delta["event"]["part"], "0");
    assert_eq!(delta["event"]["delta"], "before attach");
    let page = read(&mut rig, serde_json::json!({})).await;
    assert_eq!(page.messages.last().unwrap().ordinal, 2);
    assert_eq!(delta["event"]["mid"], page.messages.last().unwrap().mid);
    rig.finish().await;
}

fn upload_staging_snapshot(root: &std::path::Path) -> Vec<(std::path::PathBuf, Vec<u8>)> {
    let mut entries = Vec::new();
    for dir in ["objects", "refs"] {
        for entry in std::fs::read_dir(root.join("media-uploads").join(dir)).unwrap() {
            let path = entry.unwrap().path();
            entries.push((path.clone(), std::fs::read(path).unwrap()));
        }
    }
    entries.sort();
    entries
}

#[tokio::test]
async fn upload_staging_is_declared_but_invalid_pdf_is_not_admitted() {
    let mut rig = Rig::start("upload-roundtrip").await;
    broca_core::media_store::create_media_directories(&rig.dir).unwrap();
    let begin = serde_json::json!({"mime":"application/pdf","bytes":3,"sha256":"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"});
    let missing = call(&mut rig, "session.upload_begin", begin.clone()).await;
    assert_eq!(refusal(&missing)["code"], "unknown_session");
    rig.script.push(&[Step::Text("ready")]);
    rig.send("turn-1", "hi", None).await;
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let wal_before = rig.records().await;
    let created = call(&mut rig, "session.upload_begin", begin).await;
    let id = result(&created)["upload_id"].as_str().unwrap().to_owned();
    let append = serde_json::json!({"upload_id":id,"offset":0,"data":"YWJj"});
    let first = call(&mut rig, "session.upload_append", append.clone()).await;
    assert_eq!(result(&first)["received"], 3);
    let before = upload_staging_snapshot(&rig.dir);
    let retry = call(&mut rig, "session.upload_append", append).await;
    assert_eq!(refusal(&retry)["code"], "upload_offset_mismatch");
    assert_eq!(refusal(&retry)["detail"]["received"], 3);
    assert_eq!(upload_staging_snapshot(&rig.dir), before);
    let conflict = call(
        &mut rig,
        "session.upload_append",
        serde_json::json!({"upload_id":id,"offset":0,"data":"YWJk"}),
    )
    .await;
    assert_eq!(refusal(&conflict)["code"], "upload_offset_mismatch");
    assert_eq!(refusal(&conflict)["detail"]["received"], 3);
    assert_eq!(upload_staging_snapshot(&rig.dir), before);
    let commit = serde_json::json!({"upload_id":id});
    let first = call(&mut rig, "session.upload_commit", commit.clone()).await;
    assert_eq!(
        result(&first),
        serde_json::json!({"state":"committed","sha256":"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad","mime":"application/pdf","bytes":3})
    );
    let before = upload_staging_snapshot(&rig.dir);
    assert_eq!(
        result(&call(&mut rig, "session.upload_commit", commit).await),
        result(&first)
    );
    let refused = rig
        .send_with(
            "turn-2",
            "file",
            None,
            serde_json::json!({"content":[{"type":"upload","upload_id":id}]}),
        )
        .await;
    assert_eq!(refusal(&refused)["code"], "media_unsupported");
    assert!(refusal(&refused)["message"]
        .as_str()
        .unwrap()
        .contains("pdf"));
    assert_eq!(upload_staging_snapshot(&rig.dir), before);
    assert_eq!(rig.records().await, wal_before);
    let describe = call(&mut rig, "role.describe", serde_json::json!({})).await;
    let checked = cortexkit_role_llm_runner::describe::check_describe(&result(&describe)).unwrap();
    assert!(checked.declares(cortexkit_role_llm_runner::capabilities::ATTACHMENTS));
    rig.finish().await;
}

#[tokio::test]
async fn upload_requires_send_standing_before_any_staging_write() {
    let mut rig = Rig::start("upload-standing").await;
    broca_core::media_store::create_media_directories(&rig.dir).unwrap();
    rig.bind_as(
        7,
        Some(subc_protocol::Principal::Reserved {
            module_id: "owner".into(),
        }),
    )
    .await;
    rig.script.push(&[Step::Text("ready")]);
    rig.send("turn-1", "hi", None).await;
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let begin = serde_json::json!({"mime":"application/pdf","bytes":3,"sha256":"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"});
    let created = call(&mut rig, "session.upload_begin", begin.clone()).await;
    let id = result(&created)["upload_id"].as_str().unwrap().to_owned();
    rig.bind_as(
        13,
        Some(subc_protocol::Principal::Reserved {
            module_id: "stranger".into(),
        }),
    )
    .await;
    let before = upload_staging_snapshot(&rig.dir);
    let wal_before = rig.records().await;
    for (method, params) in [
        ("session.upload_begin", begin),
        (
            "session.upload_append",
            serde_json::json!({"upload_id":id,"offset":0,"data":"YWJj"}),
        ),
        ("session.upload_commit", serde_json::json!({"upload_id":id})),
        ("session.upload_status", serde_json::json!({"upload_id":id})),
    ] {
        let refused = rig
            .request(13, serde_json::json!({"method":method,"params":params}))
            .await;
        assert_eq!(
            refusal(&refused)["code"],
            "scope_owner_mismatch",
            "{method}"
        );
        assert_eq!(upload_staging_snapshot(&rig.dir), before, "{method}");
        assert_eq!(rig.records().await, wal_before);
    }
    rig.finish().await;
}

#[tokio::test]
async fn upload_raw_chunk_cap_is_enforced_on_real_serve_path() {
    use base64::Engine;
    let mut rig = Rig::start("upload-cap").await;
    broca_core::media_store::create_media_directories(&rig.dir).unwrap();
    rig.script.push(&[Step::Text("ready")]);
    rig.send("turn-1", "hi", None).await;
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let size = cortexkit_role_llm_runner::attachments::CHUNK_MAX;
    let created = call(&mut rig, "session.upload_begin", serde_json::json!({"mime":"application/pdf","bytes":size+1,"sha256":"0000000000000000000000000000000000000000000000000000000000000000"})).await;
    let id = result(&created)["upload_id"].as_str().unwrap().to_owned();
    let before = upload_staging_snapshot(&rig.dir);
    let data = base64::engine::general_purpose::STANDARD.encode(vec![1; size + 1]);
    let refused = call(
        &mut rig,
        "session.upload_append",
        serde_json::json!({"upload_id":id,"offset":0,"data":data}),
    )
    .await;
    assert_eq!(refusal(&refused)["code"], "upload_chunk_too_large");
    assert!(refusal(&refused)["message"].as_str().unwrap().contains(&id));
    assert_eq!(upload_staging_snapshot(&rig.dir), before);
    let data = base64::engine::general_purpose::STANDARD.encode(vec![1; size]);
    let accepted = call(
        &mut rig,
        "session.upload_append",
        serde_json::json!({"upload_id":id,"offset":0,"data":data}),
    )
    .await;
    assert_eq!(result(&accepted)["received"], size);
    rig.finish().await;
}

#[tokio::test]
async fn upload_plan_named_sender_has_standing_on_the_frozen_session() {
    struct SenderPlanAdmitter;
    #[async_trait::async_trait]
    impl broca_fetch_plan::PlanAdmitter for SenderPlanAdmitter {
        async fn admit(
            &self,
            plan: &broca_fetch_plan::Plan,
            _session: &broca_protocol::BindIdentity,
            _scope: Option<&broca_protocol::SessionScope>,
        ) -> Result<broca_fetch_plan::Admitted, broca_fetch_plan::Refusal> {
            Ok(broca_fetch_plan::Admitted {
                manifest: broca_wal::PlanManifest {
                    rich_ui: None,
                    plan: broca_wal::OpaqueJson::from_json_text(plan.text.clone()).unwrap(),
                    plan_identity: plan.identity.clone(),
                    composition_digest: plan.composition_digest.clone(),
                    fetches: vec![],
                    tools: std::sync::Arc::new(vec![]),
                    system_text: String::new(),
                    system_digest: String::new(),
                    join_version: 1,
                    role_versions: Default::default(),
                    declarations: vec![],
                },
            })
        }
    }
    let mut rig = Rig::start("upload-plan-sender").await;
    broca_core::media_store::create_media_directories(&rig.dir).unwrap();
    rig.registry()
        .set_plan_admitter(std::sync::Arc::new(SenderPlanAdmitter));
    rig.bind_as(
        7,
        Some(subc_protocol::Principal::Reserved {
            module_id: "owner".into(),
        }),
    )
    .await;
    let composition = serde_json::json!({"providers":[]});
    let digest = cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap();
    let plan = serde_json::json!({"tool_items":[],"system_text_items":[],"step_transform_items":[],"composition":composition,"composition_digest":digest,"senders":[{"module":"ck-mc","idle_start":true}]});
    rig.script.push(&[Step::Text("ready")]);
    let started = call(&mut rig, "session.send", serde_json::json!({"prompt":"hi","send_id":"turn-1","model":{"provider":"fake","model":"fake"},"plan":plan})).await;
    assert_eq!(result(&started)["state"], "active");
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    rig.bind_as(
        13,
        Some(subc_protocol::Principal::Reserved {
            module_id: "ck-mc".into(),
        }),
    )
    .await;
    let before = rig.records().await;
    let created = rig.request(13, serde_json::json!({"method":"session.upload_begin","params":{"mime":"application/pdf","bytes":3,"sha256":"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"}})).await;
    let id = result(&created)["upload_id"].as_str().unwrap().to_owned();
    let appended = rig.request(13, serde_json::json!({"method":"session.upload_append","params":{"upload_id":id,"offset":0,"data":"YWJj"}})).await;
    assert_eq!(result(&appended)["received"], 3);
    let committed = rig
        .request(
            13,
            serde_json::json!({"method":"session.upload_commit","params":{"upload_id":id}}),
        )
        .await;
    assert_eq!(result(&committed)["state"], "committed");
    assert_eq!(rig.records().await, before);
    rig.finish().await;
}

#[cfg(feature = "test-attachments")]
#[tokio::test]
async fn debug_pdf_send_is_refused_before_any_session_write() {
    use sha2::{Digest, Sha256};
    let mut rig = Rig::start("debug-pdf-send-gate").await;
    broca_core::media_store::create_media_directories(&rig.dir).unwrap();
    let uploads = broca_core::media_uploads::Uploads::at(&rig.dir);
    let bytes = b"%PDF-1.7\n%%EOF\n";
    let now = broca_module::person_media::now_seconds();
    let id = uploads
        .begin(
            &rig.session,
            &cortexkit_role_llm_runner::attachments::UploadBeginRequest::new(
                "application/pdf",
                bytes.len() as u64,
                format!("{:x}", Sha256::digest(bytes)),
            ),
            now,
        )
        .unwrap()
        .upload_id;
    uploads.append(&rig.session, &id, 0, bytes, now).unwrap();
    uploads.commit(&rig.session, &id, now).unwrap();
    let before = upload_staging_snapshot(&rig.dir);
    assert!(!rig.session_stored().await);
    let refused = rig
        .send_with(
            "pdf",
            "inspect",
            None,
            serde_json::json!({"content":[{"type":"upload","upload_id":id}]}),
        )
        .await;
    assert_eq!(refusal(&refused)["code"], "media_unsupported");
    let message = refusal(&refused)["message"].as_str().unwrap().to_owned();
    assert!(
        message.contains("pdf") && message.contains("modality") && message.contains("model"),
        "{message}"
    );
    assert!(
        !rig.session_stored().await,
        "a refused send must not create a session"
    );
    assert_eq!(upload_staging_snapshot(&rig.dir), before);
    for class in ["media", "media-durable", "media-ephemeral"] {
        for dir in ["objects", "refs"] {
            assert_eq!(
                std::fs::read_dir(rig.dir.join(class).join(dir))
                    .unwrap()
                    .count(),
                0
            );
        }
    }
    let describe = call(&mut rig, "role.describe", serde_json::json!({})).await;
    let checked = cortexkit_role_llm_runner::describe::check_describe(&result(&describe)).unwrap();
    assert!(checked.declares(cortexkit_role_llm_runner::capabilities::ATTACHMENTS));
    rig.finish().await;
}

/// A refused first send can leave a log holding only its lineage frame (the actor opened its
/// writer). Locating that log's first run must end at the log's end, so the read answers at
/// once with no messages instead of scanning forward forever.
#[tokio::test]
async fn a_read_after_a_refused_first_send_answers_unwritten() {
    let mut rig = Rig::start("refused-then-read").await;
    let refused = call(
        &mut rig,
        "session.send",
        serde_json::json!({
            "prompt": "x", "send_id": "refused",
            "model": {"provider": "fake", "model": "fake"},
            "structured_output": {"mode": "submit_tool", "schema": {"type": "object"}},
            "tools": [{"name": "submit_result", "description": "d", "input_schema": {"type": "object"}}]
        }),
    )
    .await;
    assert_eq!(refusal(&refused)["code"], "invalid_params");
    let page = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        read(&mut rig, serde_json::json!({})),
    )
    .await
    .expect("a read after a refused first send must not hang");
    assert!(
        page.messages.is_empty(),
        "a refused send wrote messages: {page:?}"
    );
    rig.finish().await;
}
