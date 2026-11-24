//! Capture-shaped synthetic replies at the family, run-loop, stored-record and reader boundaries.
mod delivery_rig;

use broca_core::refusal_fallback::{self, CommittedStep, StepRoute};
use broca_protocol::{
    content::{AssistantMessage, ContentBlock, Message, Role},
    CallOptions, FinishReason, ModelHandle, NormalizedEvent, ProviderControl, StepId,
};
use broca_provider::{
    conformance::normalize_recorded,
    families::{anthropic_frozen_config, anthropic_renderer_version, AnthropicMessages},
    wire::WireFamily,
};
use broca_records::record::RefusalFallbackPhase;
use broca_session_store::record::Record;
use delivery_rig::{result, Rig};
use serde_json::{json, Value};

const L2: &str = include_str!("../../broca-provider/tests/fixtures/content_filter/l2.sse");
const L3: &str = include_str!("../../broca-provider/tests/fixtures/content_filter/l3.sse");
const L4: &str = include_str!("../../broca-provider/tests/fixtures/content_filter/l4.sse");

fn normalize(sse: &str) -> Vec<NormalizedEvent> {
    normalize_recorded(
        &AnthropicMessages,
        &anthropic_frozen_config("claude-opus-5-5"),
        StepId(1),
        sse.as_bytes(),
        7,
    )
    .expect("synthetic capture normalizes through the family")
}

fn assistant(events: &[NormalizedEvent]) -> &AssistantMessage {
    events
        .iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                Some(message)
            }
            _ => None,
        })
        .expect("assembled assistant message")
}

fn finished(events: &[NormalizedEvent]) -> &ProviderControl {
    events
        .iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(control @ ProviderControl::StepFinished { .. }) => {
                Some(control)
            }
            _ => None,
        })
        .expect("finished step")
}

fn wire_event(sse: &str, kind: &str) -> Value {
    sse.lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .find(|event| event["type"] == kind)
        .unwrap()
}

fn source() -> ModelHandle {
    ModelHandle::new("anthropic", "claude-opus-5-5")
}

fn target() -> ModelHandle {
    ModelHandle::new("anthropic", "claude-opus-4-8")
}

#[test]
fn l2_thinking_only_refusal_normalizes_as_content_filter() {
    let events = normalize(L2);
    let message = assistant(&events);
    assert!(matches!(
        message.content.as_slice(),
        [ContentBlock::Reasoning { .. }]
    ));
    assert_eq!(message.tool_calls().count(), 0);
    let ProviderControl::StepFinished {
        finish_reason,
        usage,
        served_model,
        ..
    } = finished(&events)
    else {
        unreachable!()
    };
    assert_eq!(*finish_reason, FinishReason::ContentFilter);
    assert_eq!(usage.input_tokens, Some(4));
    assert_eq!(usage.output_tokens, Some(1596));
    assert_eq!(usage.reasoning_tokens, Some(1596));
    assert_eq!(usage.cache_write_tokens, Some(20068));
    assert_eq!(*served_model, None);
}

#[tokio::test]
async fn l2_thinking_only_refusal_resends_the_same_step() {
    let mut rig = Rig::start("capture-l2-refusal").await;
    *rig.script.refusal_fallback_model.lock().unwrap() = Some(target());
    rig.script
        .normalized_steps
        .lock()
        .unwrap()
        .push_back(normalize(L2));
    let sent = rig
        .send_with(
            "l2",
            "Synthetic prompt",
            None,
            json!({
                "model": {"provider":"anthropic", "model":"claude-opus-5-5"}
            }),
        )
        .await;
    assert!(result(&sent)["run_id"].is_string());
    rig.wait_idle().await;
    let records = rig.records().await;
    let refused: Vec<_> = records
        .iter()
        .filter_map(|r| match r {
            Record::StepRefused(refused) => Some(refused),
            _ => None,
        })
        .collect();
    assert_eq!(refused.len(), 1);
    assert_eq!(refused[0].step_id, 1);
    assert_eq!(refused[0].usage.output_tokens, Some(1596));
    assert_eq!(refused[0].usage.cache_write_tokens, Some(20068));
    let committed: Vec<_> = records
        .iter()
        .filter_map(|r| match r {
            Record::StepEnd(end) => Some(end),
            _ => None,
        })
        .collect();
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[0].step_id, 1);
    let fallback = committed[0]
        .fallback
        .as_ref()
        .expect("the step records its fallback")
        .parse();
    assert_eq!(
        fallback["route"]["model"],
        serde_json::to_value(target()).unwrap()
    );
    let prompts = rig.script.prompts.lock().unwrap().clone();
    assert_eq!(prompts.len(), 2);
    assert_eq!(
        prompts[0], prompts[1],
        "refused thinking is not resent as history"
    );
    rig.finish().await;
}

/// A refusal that stops while a tool block is open (its id, name and placeholder input arrived,
/// its arguments and `content_block_stop` never did) is a refusal with no call: the step is
/// resent rather than committed as a tool step, and nothing is dispatched.
#[tokio::test]
async fn a_refusal_cutting_a_tool_block_resends_the_step_without_a_call() {
    let sse = [
        json!({"type":"message_start","message":{"model":"claude-opus-5-5","id":"msg_cut","type":"message","role":"assistant","content":[],"usage":{"input_tokens":4,"output_tokens":1}}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_cut","name":"refresh","input":{}}}),
        json!({"type":"message_delta","delta":{"stop_reason":"refusal"},"usage":{"output_tokens":3}}),
        json!({"type":"message_stop"}),
    ]
    .iter()
    .map(|event| format!("event: {}\ndata: {event}\n\n", event["type"].as_str().unwrap()))
    .collect::<String>();
    let events = normalize(&sse);
    let calls = assistant(&events).tool_calls().count();
    let mut rig = Rig::start("capture-refusal-open-tool").await;
    *rig.script.refusal_fallback_model.lock().unwrap() = Some(target());
    rig.script
        .normalized_steps
        .lock()
        .unwrap()
        .push_back(events);
    let sent = rig
        .send_with(
            "open-tool",
            "Synthetic prompt",
            None,
            json!({
                "model": {"provider":"anthropic", "model":"claude-opus-5-5"}
            }),
        )
        .await;
    assert!(result(&sent)["run_id"].is_string());
    rig.wait_idle().await;
    let records = rig.records().await;
    let refused = records
        .iter()
        .filter(|r| matches!(r, Record::StepRefused(_)))
        .count();
    assert_eq!(refused, 1, "the refusal reaches the resend path");
    assert!(rig.script.dispatched.lock().unwrap().is_empty());
    assert_eq!(rig.script.prompts.lock().unwrap().len(), 2);
    assert_eq!(calls, 0);
    rig.finish().await;
}

#[test]
fn l3_keeps_handoff_and_both_usage_iterations_separate_from_last_iteration_usage() {
    let events = normalize(L3);
    let message = assistant(&events);
    assert!(matches!(message.content.as_slice(), [
        ContentBlock::Reasoning { .. },
        ContentBlock::ProviderFallback { from_model, to_model, .. },
        ContentBlock::Reasoning { .. },
        ContentBlock::ToolCall(_),
    ] if from_model == "claude-opus-5-5" && to_model == "claude-opus-4-8"));
    let ProviderControl::StepFinished {
        finish_reason,
        usage,
        served_model,
        usage_iterations,
        ..
    } = finished(&events)
    else {
        unreachable!()
    };
    assert_eq!(*finish_reason, FinishReason::ToolCalls);
    assert_eq!(served_model.as_deref(), Some("claude-opus-4-8"));
    assert_eq!(usage.input_tokens, Some(2));
    assert_eq!(usage.output_tokens, Some(29287));
    assert_eq!(usage.cached_input_tokens, Some(20068));
    assert_eq!(usage.cache_write_tokens, Some(4));
    let iterations = serde_json::to_value(usage_iterations.as_ref().unwrap()).unwrap();
    assert_eq!(
        iterations,
        wire_event(L3, "message_delta")["usage"]["iterations"]
    );
    assert_eq!(iterations.as_array().unwrap().len(), 2);
    assert_eq!(iterations[0]["type"], "message");
    assert_eq!(iterations[0]["model"], "claude-opus-5-5");
    assert_eq!(iterations[0]["input_tokens"], 4);
    assert_eq!(iterations[0]["output_tokens"], 2124);
    assert_eq!(iterations[1]["type"], "fallback_message");
    assert_eq!(iterations[1]["model"], "claude-opus-4-8");
    assert_eq!(iterations[1]["input_tokens"], 2);
    assert_eq!(iterations[1]["output_tokens"], 29287);
}

#[test]
fn l3_server_handoff_resets_the_clean_source_streak() {
    let events = normalize(L3);
    let ProviderControl::StepFinished { served_model, .. } = finished(&events) else {
        unreachable!()
    };
    let mut schedule = refusal_fallback::on_source_refusal(None, &source(), &target(), Some(1));
    schedule.phase = RefusalFallbackPhase::Probe { clean: 9 };
    let step = CommittedStep {
        route: StepRoute::Source,
        refused: false,
        served_by_server_side_fallback: served_model.is_some()
            || assistant(&events)
                .content
                .iter()
                .any(|block| matches!(block, ContentBlock::ProviderFallback { .. })),
    };
    let after = refusal_fallback::on_step_committed(Some(&schedule), step, Some(2)).unwrap();
    assert_eq!(after.phase, RefusalFallbackPhase::Probe { clean: 0 });
    let clean = CommittedStep {
        served_by_server_side_fallback: false,
        ..step
    };
    let after = refusal_fallback::on_step_committed(Some(&schedule), clean, Some(2)).unwrap();
    assert_eq!(after.phase, RefusalFallbackPhase::Probe { clean: 10 });
}

#[test]
fn l4_reply_is_normal_and_stored_handoff_echo_is_verbatim() {
    let events = normalize(L3);
    let mut frozen = anthropic_frozen_config("claude-opus-5-5");
    frozen.server_side_fallback = true;
    frozen.renderer_version = anthropic_renderer_version(true);
    let stored = serde_json::to_vec(assistant(&events)).unwrap();
    let restored: AssistantMessage = serde_json::from_slice(&stored).unwrap();
    let request = CallOptions::new(vec![
        Message::assistant_from(restored.content, source()),
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "Synthetic follow-up.".into(),
            }],
        ),
    ]);
    let bytes = AnthropicMessages.render(&frozen, &request);
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    let echoed = body["messages"][0]["content"]
        .as_array()
        .unwrap()
        .iter()
        .find(|part| part["type"] == "fallback")
        .unwrap();
    let original = L3
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .find(|event| event["content_block"]["type"] == "fallback")
        .unwrap()["content_block"]
        .clone();
    assert_eq!(
        original["trigger"],
        json!({"type":"refusal", "category":"cyber"})
    );
    assert_eq!(*echoed, original);
    assert_eq!(
        raw_request_fallback(&bytes),
        raw_request_fallback(include_bytes!(
            "../../broca-provider/tests/fixtures/content_filter/l4.request.json"
        ))
    );

    let events = normalize(L4);
    assert!(matches!(
        assistant(&events).content.as_slice(),
        [ContentBlock::Reasoning { .. }, ContentBlock::ToolCall(_)]
    ));
    let ProviderControl::StepFinished {
        finish_reason,
        served_model,
        usage,
        usage_iterations,
        ..
    } = finished(&events)
    else {
        unreachable!()
    };
    assert_eq!(*finish_reason, FinishReason::ToolCalls);
    assert_eq!(*served_model, None);
    assert_eq!(usage.input_tokens, Some(4));
    assert_eq!(usage.output_tokens, Some(6292));
    assert_eq!(usage.cache_write_tokens, Some(31481));
    assert_eq!(usage.cached_input_tokens, Some(20068));
    assert_eq!(
        serde_json::to_value(usage_iterations)
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

fn raw_request_fallback(bytes: &[u8]) -> String {
    #[derive(serde::Deserialize)]
    struct Request {
        messages: Vec<MessageContent>,
    }
    #[derive(serde::Deserialize)]
    struct MessageContent {
        content: Vec<Box<serde_json::value::RawValue>>,
    }
    serde_json::from_slice::<Request>(bytes)
        .unwrap()
        .messages
        .into_iter()
        .flat_map(|message| message.content)
        .find(|block| serde_json::from_str::<Value>(block.get()).unwrap()["type"] == "fallback")
        .unwrap()
        .get()
        .to_owned()
}

fn capture(name: &str, bytes: &[u8]) {
    if std::env::var_os("BROCA_CAPTURE_SAMPLES").is_some() {
        use base64::Engine;
        use std::io::Write;
        let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
        gzip.write_all(bytes).unwrap();
        println!(
            "BROCA_READER_SAMPLE {name} {}",
            base64::engine::general_purpose::STANDARD.encode(gzip.finish().unwrap())
        );
    }
}

#[tokio::test]
async fn l3_real_serve_readers_keep_the_stored_handoff_and_iterations() {
    let events = normalize(L3);
    let expected = assistant(&events).content.clone();
    let mut rig = Rig::start("capture-l3-readers").await;
    *rig.script.refusal_fallback_model.lock().unwrap() = Some(target());
    rig.script
        .normalized_steps
        .lock()
        .unwrap()
        .push_back(events);
    let sent = rig.send_with("l3", "Synthetic prompt", None, json!({
        "model": {"provider":"anthropic", "model":"claude-opus-5-5"},
        "tools":[{"name":"submit_result", "description":"Synthetic tool", "input_schema":{"type":"object"}}]
    })).await;
    assert!(result(&sent)["run_id"].is_string());
    rig.wait_idle().await;
    let records = rig.records().await;
    assert!(!records
        .iter()
        .any(|record| matches!(record, Record::StepRefused(_))));
    let message = records
        .iter()
        .find_map(|record| match record {
            Record::Assistant(assistant) if assistant.step_id == 1 => Some(&assistant.message),
            _ => None,
        })
        .expect("L3 committed by the real run loop");
    assert_eq!(message.content, expected);
    let record = records
        .iter()
        .find_map(|record| match record {
            Record::StepEnd(end) if end.step_id == 1 => Some(end),
            _ => None,
        })
        .expect("L3's step ended");
    let usage = record.usage.unwrap_or_default();
    let fallback = record
        .fallback
        .as_ref()
        .expect("the step records its fallback")
        .parse();
    assert_eq!(fallback["served_model"], "claude-opus-4-8");
    let iterations = fallback["usage_iterations"].clone();
    assert_eq!(
        iterations,
        wire_event(L3, "message_delta")["usage"]["iterations"]
    );
    assert_eq!(usage.input_tokens, Some(6));
    assert_eq!(usage.output_tokens, Some(31411));
    assert_eq!(usage.cached_input_tokens, Some(40136));
    assert_eq!(usage.cache_write_tokens, Some(4));
    // Neither iteration reports a reasoning-token breakdown; the last iteration's top-level
    // breakdown cannot describe reasoning usage for the whole step.
    assert_eq!(usage.reasoning_tokens, None);
    // The step's usage, split by the model that served each part of it.
    let by_model = serde_json::to_value(&record.by_model).unwrap();
    let by_model = by_model.as_array().unwrap();
    assert_eq!(by_model.len(), 2);
    let source_usage = by_model
        .iter()
        .find(|row| row["model"] == "claude-opus-5-5")
        .unwrap()["usage"]
        .clone();
    let target_usage = by_model
        .iter()
        .find(|row| row["model"] == "claude-opus-4-8")
        .unwrap()["usage"]
        .clone();
    assert_eq!(source_usage["output_tokens"], 2124);
    assert_eq!(target_usage["output_tokens"], 29287);
    capture("step-end.json", &serde_json::to_vec(record).unwrap());

    for (name, method, params) in [
        ("session.read.default.json", "session.read", json!({})),
        (
            "session.read.descriptors.json",
            "session.read",
            json!({"media":"descriptors"}),
        ),
        ("session.history.default.json", "session.history", json!({})),
        (
            "session.history.display.json",
            "session.history",
            json!({"form":"display", "limit":50}),
        ),
    ] {
        let frame = rig
            .request(7, json!({"method":method, "params":params}))
            .await;
        assert_eq!(frame.header.ty, subc_protocol::FrameType::Response);
        let body = result(&frame);
        assert!(!body.is_null(), "reader returned a result");
        assert!(
            String::from_utf8_lossy(&frame.body).contains("provider_fallback"),
            "{name} keeps a typed handoff"
        );
        capture(name, &frame.body);
    }
    rig.finish().await;
}
