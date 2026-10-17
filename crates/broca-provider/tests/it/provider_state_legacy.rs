//! Byte pins for episodes admitted before text-part provider state was supported.

use broca_protocol::{
    content::{ContentBlock, Message, Role},
    CallOptions, ModelHandle,
};
use broca_provider::{
    families::{Gemini, GoogleCodeAssist, OpenAiResponses, OpenAiResponsesChatgpt},
    wire::{FrozenRenderConfig, WireFamily},
};
use serde_json::Value;

#[test]
fn pre_provider_text_state_configs_and_render_bytes_are_pinned() {
    let families: [&dyn WireFamily; 4] = [
        &OpenAiResponses,
        &OpenAiResponsesChatgpt,
        &Gemini,
        &GoogleCodeAssist,
    ];
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
    let cases: Vec<Value> =
        serde_json::from_str(include_str!("../fixtures/pre_provider_text_state.json")).unwrap();
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
