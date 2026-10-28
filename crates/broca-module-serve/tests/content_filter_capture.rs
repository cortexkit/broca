//! Capture-shaped synthetic replies at the family, run-loop, WAL, and reader boundaries.
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
use broca_wal::{RefusalFallbackPhase, WalRecord};
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
    let records = rig.records();
    let refused: Vec<_> = records
        .iter()
        .filter(|r| matches!(r, WalRecord::ModelStepRefused { .. }))
        .collect();
    assert_eq!(refused.len(), 1);
    assert!(
        matches!(refused[0], WalRecord::ModelStepRefused { step_id: StepId(1), usage, .. }
        if usage.output_tokens == Some(1596) && usage.cache_write_tokens == Some(20068))
    );
    let committed: Vec<_> = records
        .iter()
        .filter_map(|r| match r {
            WalRecord::ModelStepFinished {
                step_id, fallback, ..
            } => Some((step_id, fallback)),
            _ => None,
        })
        .collect();
    assert_eq!(committed.len(), 1);
    assert_eq!(*committed[0].0, StepId(1));
    assert_eq!(committed[0].1.route.as_ref().unwrap().model, target());
    let prompts = rig.script.prompts.lock().unwrap().clone();
    assert_eq!(prompts.len(), 2);
    assert_eq!(
        prompts[0], prompts[1],
        "refused thinking is not resent as history"
    );
    rig.finish().await;
}

#[test]
fn l3_keeps_handoff_and_both_usage_iterations_separate_from_last_iteration_usage() {
    let events = normalize(L3);
    let message = assistant(&events);
    assert!(matches!(message.content.as_slice(), [
        ContentBlock::Reasoning { .. },
        ContentBlock::ProviderFallback { from_model, to_model },
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
fn l4_reply_is_normal_but_current_handoff_echo_loses_the_trigger() {
    let events = normalize(L3);
    let mut frozen = anthropic_frozen_config("claude-opus-5-5");
    frozen.server_side_fallback = true;
    frozen.renderer_version = anthropic_renderer_version(true);
    let request = CallOptions::new(vec![
        Message::assistant_from(assistant(&events).content.clone(), source()),
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
    // Characterize the lossy echo, not a byte-identical round trip: the stored part has no
    // trigger field, so the next request cannot reproduce the provider's original block.
    assert_eq!(
        echoed,
        &json!({"type":"fallback", "from":{"model":"claude-opus-5-5"}, "to":{"model":"claude-opus-4-8"}})
    );
    assert_ne!(*echoed, original);

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
    let records = rig.records();
    assert!(!records
        .iter()
        .any(|record| matches!(record, WalRecord::ModelStepRefused { .. })));
    let (record, usage, fallback) = records
        .iter()
        .find_map(|record| match record {
            WalRecord::ModelStepFinished {
                step_id: StepId(1),
                assistant_message: message,
                usage,
                fallback,
                ..
            } => {
                assert_eq!(message.content, expected);
                Some((record, usage, fallback))
            }
            _ => None,
        })
        .expect("L3 committed by the real run loop");
    assert_eq!(fallback.served_model.as_deref(), Some("claude-opus-4-8"));
    let iterations = serde_json::to_value(fallback.usage_iterations.as_ref().unwrap()).unwrap();
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
    let mut projection = broca_core::replay::RunUsageProjection::new(
        records
            .iter()
            .find_map(|record| match record {
                WalRecord::RunStarted { run_id, .. } => Some(run_id.clone()),
                _ => None,
            })
            .unwrap(),
    );
    for record in &records {
        projection.observe(record);
        if matches!(
            record,
            WalRecord::ModelStepFinished {
                step_id: StepId(1),
                ..
            }
        ) {
            break;
        }
    }
    let by_model = serde_json::to_value(&projection.usage).unwrap()["by_model"].clone();
    let by_model = by_model.as_array().unwrap();
    assert_eq!(by_model.len(), 2);
    let source_usage = by_model
        .iter()
        .find(|row| row["model"] == "claude-opus-5-5")
        .unwrap();
    let target_usage = by_model
        .iter()
        .find(|row| row["model"] == "claude-opus-4-8")
        .unwrap();
    assert_eq!(source_usage["output_tokens"], 2124);
    assert_eq!(target_usage["output_tokens"], 29287);
    capture(
        "model-step-finished.json",
        &serde_json::to_vec(record).unwrap(),
    );

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
