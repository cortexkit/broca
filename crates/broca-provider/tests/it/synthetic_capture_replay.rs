//! Offline snapshots for every non-empty live-harness artifact. The bytes and expected events
//! are synthetic, but retain the family event order, chunking and usage-field presence.

use broca_protocol::{ContentBlock, DisplayEvent, NormalizedEvent, ProviderControl, StepId};
use broca_provider::families::{
    anthropic_frozen_config, gemini_frozen_config, openai_chat_frozen_config,
    openai_responses_frozen_config, AnthropicMessages, Gemini, OpenAiChat, OpenAiResponses,
};
use broca_provider::wire::WireFamily;
use serde_json::Value;

// Capture snapshots predate transcript coordinates. Keep every historical payload field
// pinned, and check the new coordinates separately against the assembled stored message.
fn captured_event(event: &NormalizedEvent) -> String {
    match event {
        NormalizedEvent::Display(DisplayEvent::TextDelta { message_id, tool_call_id, delta, .. }) =>
            format!("Display(TextDelta {{ message_id: {message_id:?}, tool_call_id: {tool_call_id:?}, delta: {delta:?} }})"),
        NormalizedEvent::Display(DisplayEvent::ReasoningDelta { message_id, delta, .. }) =>
            format!("Display(ReasoningDelta {{ message_id: {message_id:?}, delta: {delta:?} }})"),
        NormalizedEvent::Display(DisplayEvent::ToolInputDelta { tool_call_id, delta, .. }) =>
            format!("Display(ToolInputDelta {{ tool_call_id: {tool_call_id:?}, delta: {delta:?} }})"),
        _ => format!("{event:?}"),
    }
}

fn check_display_coordinates(events: &[NormalizedEvent], capture: &str) {
    let message = events
        .iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                Some(message)
            }
            _ => None,
        })
        .expect("completed capture step has an assembled message");
    for event in events {
        let NormalizedEvent::Display(display) = event else {
            continue;
        };
        if display.delta().is_none() {
            continue;
        }
        let (mid, part) = display
            .coordinates()
            .expect("every normalized delta has a stored part");
        assert!(
            mid.is_none(),
            "{capture}: only the actor attaches transcript mids"
        );
        let block = message
            .content
            .get(part as usize)
            .expect("delta addresses a stored block");
        match (display, block) {
            (
                DisplayEvent::TextDelta {
                    message_id, delta, ..
                },
                ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. },
            )
            | (
                DisplayEvent::ReasoningDelta {
                    message_id, delta, ..
                },
                ContentBlock::Reasoning { text, .. },
            ) => {
                assert_eq!(
                    message_id, &message.message_id,
                    "{capture}: provider identity"
                );
                assert!(
                    text.contains(delta),
                    "{capture}: delta belongs to its stored part"
                );
            }
            (DisplayEvent::ReasoningDelta { .. }, ContentBlock::RedactedReasoning { .. }) => {}
            (DisplayEvent::ToolInputDelta { tool_call_id, .. }, ContentBlock::ToolCall(call)) => {
                assert_eq!(
                    tool_call_id, &call.tool_call_id,
                    "{capture}: tool part identity"
                )
            }
            pair => panic!("{capture}: delta addresses the wrong stored block: {pair:?}"),
        }
    }
}

fn replay(relative: &str) {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/live/captures")
        .join(relative);
    let mut capture: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let model = capture["model"].as_str().unwrap();
    let wire_family = if capture["family"] == "provider_switch" {
        if relative.contains("anthropic") {
            "anthropic"
        } else if relative.contains("gemini") {
            "gemini"
        } else {
            "openai_chat"
        }
    } else {
        capture["family"].as_str().unwrap()
    };
    let (family, frozen): (Box<dyn WireFamily>, _) = match wire_family {
        "anthropic" => (Box::new(AnthropicMessages), anthropic_frozen_config(model)),
        "gemini" => (Box::new(Gemini), gemini_frozen_config(model)),
        "openai_chat" => (Box::new(OpenAiChat), openai_chat_frozen_config(model)),
        "openai_responses" => (
            Box::new(OpenAiResponses),
            openai_responses_frozen_config(model),
        ),
        other => panic!("unsupported capture family: {other}"),
    };
    let body = capture["raw_response_text"]
        .as_str()
        .unwrap()
        .replace("\r\n", "\n");
    assert!(!body.is_empty());
    let mut actual = Vec::new();
    let mut step = String::new();
    let mut step_id = 1;
    // Live tool loops concatenate two response bodies. Each body gets a fresh normalizer,
    // just as the transport gives each model step its own normalizer.
    for packet in body.split_inclusive("\n\n") {
        step.push_str(packet);
        let data = packet.lines().find_map(|line| line.strip_prefix("data: "));
        let terminal = data.is_some_and(|data| {
            if data == "[DONE]" {
                return true;
            }
            let value: Value = serde_json::from_str(data).unwrap();
            matches!(
                value["type"].as_str(),
                Some("message_stop" | "response.completed")
            ) || value["candidates"][0]["finishReason"].is_string()
        });
        if terminal {
            let mut normalizer = family.normalizer(&frozen, StepId(step_id));
            let start = actual.len();
            for chunk in step.as_bytes().chunks(61) {
                actual.extend(normalizer.push(chunk).expect("synthetic stream normalizes"));
            }
            actual.extend(normalizer.finish().expect("synthetic stream ends cleanly"));
            check_display_coordinates(&actual[start..], relative);
            step.clear();
            step_id += 1;
        }
    }
    assert!(
        step.trim().is_empty(),
        "{relative}: complete response bodies"
    );
    assert!(step_id > 1, "{relative}: at least one completed step");
    let actual: Vec<String> = actual.iter().map(captured_event).collect();
    if std::env::var_os("BROCA_UPDATE_SYNTHETIC_CAPTURE_EVENTS").is_some() {
        capture["normalized_events"] = serde_json::to_value(&actual).unwrap();
        std::fs::write(
            &path,
            format!("{}\n", serde_json::to_string_pretty(&capture).unwrap()),
        )
        .unwrap();
    }
    let expected: Vec<String> =
        serde_json::from_value(capture["normalized_events"].clone()).unwrap();
    assert_eq!(
        actual, expected,
        "{relative}: display, canonical content and terminal usage"
    );
}

macro_rules! capture_test {
    ($name:ident, $path:literal) => {
        #[test]
        fn $name() {
            replay($path);
        }
    };
}

capture_test!(anthropic_signed_thinking, "anthropic/signed_thinking.json");
capture_test!(anthropic_tool_loop_nonce, "anthropic/tool_loop_nonce.json");
capture_test!(gemini_text_completes, "gemini/text_completes.json");
capture_test!(gemini_tool_call, "gemini/tool_call.json");
capture_test!(
    openai_chat_text_completes,
    "openai_chat/text_completes.json"
);
capture_test!(openai_chat_tool_call, "openai_chat/tool_call.json");
capture_test!(
    openai_responses_text_completes,
    "openai_responses/text_completes.json"
);
capture_test!(
    openai_responses_tool_call,
    "openai_responses/tool_call.json"
);
capture_test!(
    switch_ant_gem_anthropic_a,
    "provider_switch/ant_gem_anthropic_leg_a.json"
);
capture_test!(
    switch_ant_gem_gemini_b,
    "provider_switch/ant_gem_gemini_leg_b.json"
);
capture_test!(
    switch_ant_oai_anthropic_a,
    "provider_switch/ant_oai_anthropic_leg_a.json"
);
capture_test!(
    switch_ant_oai_chat_b,
    "provider_switch/ant_oai_openai_chat_leg_b.json"
);
capture_test!(switch_anthropic_a, "provider_switch/anthropic_leg_a.json");
capture_test!(switch_anthropic_b, "provider_switch/anthropic_leg_b.json");
capture_test!(switch_gemini_b, "provider_switch/gemini_leg_b.json");
capture_test!(
    switch_oai_ant_anthropic_b,
    "provider_switch/oai_ant_anthropic_leg_b.json"
);
capture_test!(
    switch_oai_ant_chat_a,
    "provider_switch/oai_ant_openai_chat_leg_a.json"
);
capture_test!(
    switch_oai_gem_gemini_b,
    "provider_switch/oai_gem_gemini_leg_b.json"
);
capture_test!(
    switch_oai_gem_chat_a,
    "provider_switch/oai_gem_openai_chat_leg_a.json"
);
capture_test!(switch_chat_a, "provider_switch/openai_chat_leg_a.json");
