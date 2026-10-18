//! Frozen JSON and outbound request bytes from before Responses phases and Gemini text signatures
//! were retained. These configurations must not opt into echoing newly supported metadata on resume.

use broca_protocol::{
    content::{ContentBlock, Message, Role},
    CallOptions, ModelHandle,
};
use broca_provider::{
    families::{Gemini, GoogleCodeAssist, OpenAiResponses, OpenAiResponsesChatgpt},
    wire::{FrozenRenderConfig, WireFamily},
};
use serde_json::Value;

/// The pinned cases: each a frozen config recorded before `provider_text_state` existed and the
/// body its renderer version sent for [`legacy_call_options`].
pub(crate) const LEGACY_PINS: &str = include_str!("../fixtures/pre_provider_text_state.json");

/// The prompt the pinned bodies were rendered from.
pub(crate) fn legacy_call_options() -> CallOptions {
    let mut opts = CallOptions::new(vec![
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "question".into(),
            }],
        ),
        Message::assistant_from(
            vec![
                ContentBlock::Text {
                    text: "commentary".into(),
                },
                ContentBlock::Text {
                    text: "answer".into(),
                },
            ],
            ModelHandle::new("provider", "model"),
        ),
    ]);
    opts.target_model = Some(ModelHandle::new("provider", "model"));
    opts.step_context = Some(broca_protocol::StepContext {
        run_id: "legacy".into(),
        step_index: 2,
    });
    opts
}

#[test]
fn pre_provider_text_state_configs_and_render_bytes_are_pinned() {
    let families: [&dyn WireFamily; 4] = [
        &OpenAiResponses,
        &OpenAiResponsesChatgpt,
        &Gemini,
        &GoogleCodeAssist,
    ];
    let opts = legacy_call_options();
    let cases: Vec<Value> = serde_json::from_str(LEGACY_PINS).unwrap();
    assert_eq!(cases.len(), 4);
    for (family, case) in families.into_iter().zip(cases) {
        let frozen: FrozenRenderConfig = serde_json::from_value(case["frozen"].clone()).unwrap();

        assert_eq!(serde_json::to_value(&frozen).unwrap(), case["frozen"]);
        assert_eq!(
            family.render(&frozen, &opts),
            case["body"].as_str().unwrap().as_bytes()
        );
    }
}
