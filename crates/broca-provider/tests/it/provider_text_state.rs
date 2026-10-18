//! Real codec → framed WAL → core replay → outbound bytes, with synthetic opaque provider state.

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
use broca_wal::{
    framing::{decode_next, encode, DecodeStep, PROVIDER_TEXT_STATE_FEATURE},
    WalRecord,
};
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

fn replayed(frozen: &FrozenRenderConfig, message: &AssistantMessage) -> CallOptions {
    let model = ModelHandle::new("fixture-provider", "model");
    let mut records: Vec<WalRecord> = vec![serde_json::from_value(json!({
        "type":"run_started", "run_id":"state", "session":{"project_root":"/tmp/provider-state","harness":"test","session":"state"},
        "config":{"model":model,"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":frozen.renderer_version,"frozen_render_config":frozen},
        "input":[],"origin":{"kind":"fresh"}
    })).unwrap(), serde_json::from_value(json!({
        "type":"model_step_finished","step_id":1,"assistant_message":message,"usage":{},"retries_used":0
    })).unwrap()];
    let calls: Vec<_> = message
        .tool_calls()
        .map(|call| call.tool_call_id.0.clone())
        .collect();
    if !calls.is_empty() {
        records.push(serde_json::from_value(json!({"type":"tool_batch_started","batch_id":"b","step_id":1,"planned_calls":calls})).unwrap());
        for call in calls {
            records.push(serde_json::from_value(json!({"type":"tool_no_dispatch","batch_id":"b","tool_call_id":call,"reason":{"kind":"denied","reason":"fixture"}})).unwrap());
        }
        records.push(
            serde_json::from_value(json!({"type":"tool_batch_finished","batch_id":"b"})).unwrap(),
        );
    }
    let restored: Vec<_> = records
        .iter()
        .enumerate()
        .map(|(index, record)| {
            let bytes = encode(index as u64 + 1, 1, record).unwrap();
            let DecodeStep::Record { framed, .. } = decode_next(&bytes) else {
                panic!("framed WAL record")
            };
            assert_eq!(
                serde_json::to_vec(&framed.record).unwrap(),
                serde_json::to_vec(record).unwrap()
            );
            framed.record
        })
        .collect();
    for record in &restored[..2] {
        assert!(record
            .required_features()
            .contains(&PROVIDER_TEXT_STATE_FEATURE));
    }
    let state = broca_core::replay(&restored).unwrap();
    let assistant = state
        .prompt
        .iter()
        .find(|message| message.role == Role::Assistant)
        .unwrap();
    assert_eq!(assistant.origin, Some(model.clone()));
    assert_eq!(
        assistant.content, message.content,
        "replay must preserve state bytes and item identity"
    );
    let mut opts = CallOptions::new(state.prompt);
    opts.target_model = Some(model);
    opts.step_context = Some(broca_protocol::StepContext {
        run_id: "state".into(),
        step_index: 2,
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
    let opts = replayed(&frozen, &message);
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
    let opts = replayed(&frozen, &message);
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
fn responses_phase_survives_wal_replay() {
    responses_roundtrip(&OpenAiResponses, openai_responses_frozen_config("model"));
}
#[test]
fn chatgpt_phase_survives_wal_replay() {
    responses_roundtrip(
        &OpenAiResponsesChatgpt,
        openai_responses_chatgpt_frozen_config("model"),
    );
}
#[test]
fn gemini_signed_text_survives_wal_replay() {
    gemini_roundtrip(&Gemini, gemini_frozen_config("model"), false);
}
#[test]
fn codeassist_signed_text_survives_wal_replay() {
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
        let record: WalRecord = serde_json::from_value(json!({"type":"model_step_finished","step_id":1,"assistant_message":message,"usage":{},"retries_used":0})).unwrap();
        assert!(record.required_features().is_empty());
        let bytes = encode(1, 1, &record).unwrap();
        let DecodeStep::Record { framed, .. } = decode_next(&bytes) else {
            panic!("legacy record")
        };
        let WalRecord::ModelStepFinished {
            assistant_message, ..
        } = framed.record
        else {
            panic!("legacy assistant")
        };
        let mut opts = CallOptions::new(vec![broca_protocol::content::Message::assistant_from(
            assistant_message.content.clone(),
            ModelHandle::new("fixture-provider", "model"),
        )]);
        opts.target_model = Some(ModelHandle::new("fixture-provider", "model"));
        opts.step_context = Some(broca_protocol::StepContext {
            run_id: "legacy".into(),
            step_index: 2,
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
