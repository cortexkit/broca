//! Real codec → the stored assistant message → outbound bytes, with synthetic opaque provider
//! state.

use broca_protocol::{
    content::{AssistantMessage, ContentBlock, Role},
    provider::{NormalizedEvent, ProviderControl},
    CallOptions, ModelHandle, StepId,
};
use broca_provider::{
    families::{
        gemini_frozen_config, google_codeassist_frozen_config,
        openai_responses_chatgpt_frozen_config, openai_responses_frozen_config, Gemini,
        GoogleCodeAssist, OpenAiResponses, OpenAiResponsesChatgpt,
    },
    wire::{FrozenRenderConfig, WireFamily},
};
use broca_records::record::NoDispatchReason;
use serde_json::{json, Value};

fn decoded(
    family: &dyn WireFamily,
    frozen: &FrozenRenderConfig,
    fixture: &str,
) -> AssistantMessage {
    let mut normalizer = family.normalizer(frozen, StepId(1));
    let mut events = Vec::new();
    for chunk in fixture.as_bytes().chunks(7) {
        events.extend(normalizer.push(chunk).unwrap());
    }
    events.extend(normalizer.finish().unwrap());
    events
        .into_iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                Some(message)
            }
            _ => None,
        })
        .expect("fixture produces a complete assistant message")
}

/// The request the next step sends after `message`: the answer as the run stores it, then, when
/// it called tools, a denied result for each call, as a run that refused them records.
fn replayed(message: &AssistantMessage) -> CallOptions {
    let model = ModelHandle::new("fixture-provider", "model");
    let mut prompt = vec![broca_protocol::content::Message::assistant_from(
        message.content.clone(),
        model.clone(),
    )];
    let denied = NoDispatchReason::Denied {
        reason: "fixture".into(),
    };
    let results: Vec<_> = message
        .tool_calls()
        .map(|call| {
            ContentBlock::ToolResult(broca_core::no_dispatch_result(&call.tool_call_id, &denied))
        })
        .collect();
    if !results.is_empty() {
        prompt.push(broca_protocol::content::Message::new(Role::Tool, results));
    }
    broca_protocol::content::stamp_ordinals(&mut prompt);
    let mut opts = CallOptions::new(prompt);
    opts.target_model = Some(model);
    opts.step_context = Some(broca_protocol::StepContext {
        session_address: None,
        credential_id: None,
        account_ref: None,
        run_id: "state".into(),
        step_index: 2,
        max_output_tokens: None,
    });
    opts
}

fn responses_roundtrip(family: &dyn WireFamily, frozen: FrozenRenderConfig) {
    let message = decoded(
        family,
        &frozen,
        include_str!("../fixtures/provider_phase.sse"),
    );
    assert_eq!(message.content.len(), 5);
    let ids: Vec<_> = message
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::TextWithState { provider_state, .. } => Some(
                serde_json::from_str::<Value>(&provider_state.data).unwrap()["id"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
            ),
            _ => None,
        })
        .collect();
    assert_eq!(ids, ["msg_preamble", "msg_answer", "msg_empty", "msg_tail"]);
    let opts = replayed(&message);
    let body: Value = serde_json::from_slice(&family.render(&frozen, &opts)).unwrap();
    assert_eq!(
        broca_protocol::canonical_json_bytes(&body["input"]),
        include_str!("../fixtures/provider_phase_input.golden.json")
            .trim_end()
            .as_bytes()
    );
    assert_state_gates(family, &frozen, opts, "phase");
}

fn gemini_roundtrip(family: &dyn WireFamily, mut frozen: FrozenRenderConfig, wrapped: bool) {
    frozen.project_id = wrapped.then(|| "fixture-project".into());
    let fixture = include_str!("../fixtures/provider_signed_text.sse");
    let wrapped_fixture = if wrapped {
        let response: Value = serde_json::from_str(
            fixture
                .lines()
                .find_map(|line| line.strip_prefix("data: "))
                .unwrap(),
        )
        .unwrap();
        format!("data: {}\n\n", json!({"response":response}))
    } else {
        fixture.into()
    };
    let message = decoded(family, &frozen, &wrapped_fixture);
    assert_eq!(message.content.len(), 5);
    assert!(
        matches!(&message.content[1], ContentBlock::TextWithState {text,..} if text.is_empty())
    );
    let opts = replayed(&message);
    let body: Value = serde_json::from_slice(&family.render(&frozen, &opts)).unwrap();
    let contents = if wrapped {
        &body["request"]["contents"]
    } else {
        &body["contents"]
    };
    assert_eq!(
        broca_protocol::canonical_json_bytes(contents),
        include_str!("../fixtures/provider_signed_text_contents.golden.json")
            .trim_end()
            .as_bytes()
    );
    assert_state_gates(family, &frozen, opts, "thoughtSignature");
}

fn assert_state_gates(
    family: &dyn WireFamily,
    frozen: &FrozenRenderConfig,
    opts: CallOptions,
    key: &str,
) {
    for mode in [
        "foreign-model",
        "foreign-provider",
        "unknown-origin",
        "unknown-target",
        "foreign-family",
    ] {
        let mut opts = opts.clone();
        match mode {
            "foreign-model" => {
                opts.target_model = Some(ModelHandle::new("fixture-provider", "another-model"))
            }
            "foreign-provider" => {
                opts.target_model = Some(ModelHandle::new("another-provider", "model"))
            }
            "unknown-origin" => opts
                .prompt
                .iter_mut()
                .for_each(|message| message.origin = None),
            "unknown-target" => opts.target_model = None,
            _ => {
                for message in &mut opts.prompt {
                    for block in &mut message.content {
                        if let ContentBlock::TextWithState { provider_state, .. } = block {
                            use broca_protocol::content::TextProviderFormat as F;
                            provider_state.format = match provider_state.format {
                                F::OpenAiResponses => F::OpenAiResponsesChatgpt,
                                F::OpenAiResponsesChatgpt => F::OpenAiResponses,
                                F::Gemini => F::GoogleCodeAssist,
                                F::GoogleCodeAssist => F::Gemini,
                            };
                        }
                    }
                }
            }
        }
        let bytes = family.render(frozen, &opts);
        let body = String::from_utf8(bytes).unwrap();
        assert!(
            !body.contains(&format!("\"{key}\":")),
            "{mode} must not echo opaque state"
        );
        assert!(
            body.contains(if key == "phase" {
                "Found seven."
            } else {
                "answer one"
            }),
            "{mode} must retain portable visible text"
        );
    }
}

#[test]
fn responses_phase_is_sent_back() {
    responses_roundtrip(&OpenAiResponses, openai_responses_frozen_config("model"));
}
#[test]
fn chatgpt_phase_is_sent_back() {
    responses_roundtrip(
        &OpenAiResponsesChatgpt,
        openai_responses_chatgpt_frozen_config("model"),
    );
}
#[test]
fn gemini_signed_text_is_sent_back() {
    gemini_roundtrip(&Gemini, gemini_frozen_config("model"), false);
}
#[test]
fn codeassist_signed_text_is_sent_back() {
    gemini_roundtrip(
        &GoogleCodeAssist,
        google_codeassist_frozen_config("model"),
        true,
    );
}

#[test]
fn legacy_frozen_configs_keep_pre_state_decode_and_render_bytes() {
    let cases: Vec<Value> =
        serde_json::from_str(include_str!("../fixtures/pre_provider_text_state.json")).unwrap();
    let families: [&dyn WireFamily; 4] = [
        &OpenAiResponses,
        &OpenAiResponsesChatgpt,
        &Gemini,
        &GoogleCodeAssist,
    ];
    for (index, (family, case)) in families.into_iter().zip(cases).enumerate() {
        let frozen: FrozenRenderConfig = serde_json::from_value(case["frozen"].clone()).unwrap();
        assert!(!frozen.provider_text_state);
        let fixture = if index < 2 {
            include_str!("../fixtures/provider_phase.sse")
        } else {
            include_str!("../fixtures/provider_signed_text.sse")
        };
        let fixture = if index == 3 {
            let response: Value = serde_json::from_str(
                fixture
                    .lines()
                    .find_map(|line| line.strip_prefix("data: "))
                    .unwrap(),
            )
            .unwrap();
            format!("data: {}\n\n", json!({"response":response}))
        } else {
            fixture.to_owned()
        };
        let message = decoded(family, &frozen, &fixture);
        assert!(message
            .content
            .iter()
            .all(|block| !matches!(block, ContentBlock::TextWithState { .. })));
        let mut opts = CallOptions::new(vec![broca_protocol::content::Message::assistant_from(
            message.content.clone(),
            ModelHandle::new("fixture-provider", "model"),
        )]);
        opts.target_model = Some(ModelHandle::new("fixture-provider", "model"));
        opts.step_context = Some(broca_protocol::StepContext {
            session_address: None,
            credential_id: None,
            account_ref: None,
            run_id: "legacy".into(),
            step_index: 2,
            max_output_tokens: None,
        });
        let body: Value = serde_json::from_slice(&family.render(&frozen, &opts)).unwrap();
        let expected = if index < 2 {
            r#"[{"content":"Checking. ","role":"assistant","type":"message"},{"arguments":"{\"key\":\"x\"}","call_id":"call_1","name":"lookup","type":"function_call"},{"content":"Found seven.End.","role":"assistant","type":"message"}]"#
        } else {
            r#"[{"parts":[{"text":"unsigned prefix"},{"text":"answer one"},{"text":"answer two"},{"text":"unsigned suffix"}],"role":"model"}]"#
        };
        let actual = if index < 2 {
            &body["input"]
        } else if index == 2 {
            &body["contents"]
        } else {
            &body["request"]["contents"]
        };
        assert_eq!(
            broca_protocol::canonical_json_bytes(actual),
            expected.as_bytes()
        );
    }
}

/// A Responses step whose reasoning item carries summary text: the live stream shows that text
/// on the display lane, but that changes neither what is stored nor what is sent back.
/// The step is decoded by the real normalizer and rendered after the person's message; the
/// stored block must be the provider's raw item and the rebuilt request must echo it unchanged,
/// for both the stateless (`store: false`) and the legacy continuation shapes.
#[test]
fn responses_reasoning_summary_request_bytes_are_sent_back() {
    const ITEM: &str = r#"{"id":"rs_summary","type":"reasoning","summary":[{"type":"summary_text","text":"**Planning** I will check the inputs."},{"type":"summary_text","text":"**Answering** The inputs are fine."}],"encrypted_content":"synthetic-cipher"}"#;
    let model = ModelHandle::new("fixture-provider", "model");
    for stateless in [true, false] {
        let mut frozen = openai_responses_frozen_config("model");
        if !frozen.static_options.is_object() {
            frozen.static_options = json!({});
        }
        frozen.static_options["store"] = json!(!stateless);
        let message = decoded(
            &OpenAiResponses,
            &frozen,
            include_str!("../fixtures/responses_reasoning_summary.sse"),
        );
        let stored = vec![
            ContentBlock::RedactedReasoning {
                data: ITEM.into(),
                provider_format: Some(broca_protocol::EncryptedReasoningFormat::OpenAiResponses),
            },
            ContentBlock::Text {
                text: "All good.".into(),
            },
        ];
        assert_eq!(message.content, stored, "the step stores only the raw item");

        let mut prompt = vec![
            broca_protocol::content::Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "check".into(),
                }],
            ),
            broca_protocol::content::Message::assistant_from(
                message.content.clone(),
                model.clone(),
            ),
        ];
        broca_protocol::content::stamp_ordinals(&mut prompt);

        let mut opts = CallOptions::new(prompt);
        opts.target_model = Some(model.clone());
        opts.step_context = Some(broca_protocol::StepContext {
            session_address: None,
            credential_id: None,
            account_ref: None,
            run_id: "summary".into(),
            step_index: 2,
            max_output_tokens: None,
        });
        let body: Value = serde_json::from_slice(&OpenAiResponses.render(&frozen, &opts)).unwrap();
        let expected = if stateless {
            concat!(
                r#"[{"content":"check","role":"user","type":"message"},"#,
                r#"{"encrypted_content":"synthetic-cipher","id":"rs_summary","summary":[{"text":"**Planning** I will check the inputs.","type":"summary_text"},{"text":"**Answering** The inputs are fine.","type":"summary_text"}],"type":"reasoning"},"#,
                r#"{"content":"All good.","role":"assistant","type":"message"}]"#
            )
        } else {
            concat!(
                r#"[{"content":"check","role":"user","type":"message"},"#,
                r#"{"content":null,"encrypted_content":"synthetic-cipher","summary":[{"text":"**Planning** I will check the inputs.","type":"summary_text"},{"text":"**Answering** The inputs are fine.","type":"summary_text"}],"type":"reasoning"},"#,
                r#"{"content":"All good.","role":"assistant","type":"message"}]"#
            )
        };
        assert_eq!(
            String::from_utf8(broca_protocol::canonical_json_bytes(&body["input"])).unwrap(),
            expected,
            "stateless={stateless}"
        );
    }
}
