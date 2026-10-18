//! Pre-reasoning-preservation render configs are literal fixtures, not rebuilt from today's
//! admission defaults. Resume must keep their request bytes even when history contains reasoning.

use broca_protocol::{
    content::{ContentBlock, EncryptedReasoningFormat, Message, Role},
    CallOptions, ModelHandle,
};
use broca_provider::{registry::wire_family, wire::FrozenRenderConfig};
use serde_json::Value;

#[test]
fn old_bedrock_and_meta_configs_resume_with_exact_request_bytes() {
    let cases: Vec<Value> =
        serde_json::from_str(include_str!("../fixtures/reasoning_context_legacy.json")).unwrap();
    assert_eq!(cases.len(), 2);
    for case in cases {
        let frozen: FrozenRenderConfig = serde_json::from_value(case["frozen"].clone()).unwrap();
        let model = ModelHandle::new(case["provider"].as_str().unwrap(), "model");
        let mut opts = CallOptions::new(vec![
            Message::new(Role::User, vec![ContentBlock::Text { text: "question".into() }]),
            Message::assistant_from(
                vec![
                    ContentBlock::Reasoning { text: "think".into(), signature: Some("sig".into()) },
                    ContentBlock::RedactedReasoning { data: "AQID".into(), provider_format: None },
                    ContentBlock::RedactedReasoning {
                        data: r#"{"id":"r1","type":"reasoning","summary":[],"encrypted_content":"cipher","status":"completed"}"#.into(),
                        provider_format: Some(EncryptedReasoningFormat::OpenAiResponses),
                    },
                    ContentBlock::Text { text: "answer".into() },
                ], model.clone(),
            ),
            Message::new(Role::User, vec![ContentBlock::Text { text: "next".into() }]),
        ]);
        opts.target_model = Some(model);
        let family = wire_family(frozen.family_id).unwrap();
        let before_resume = family.render(&frozen, &opts);
        assert_eq!(before_resume, case["body"].as_str().unwrap().as_bytes());
        let recorded = serde_json::to_vec(&frozen).unwrap();
        let resumed: FrozenRenderConfig = serde_json::from_slice(&recorded).unwrap();
        assert_eq!(serde_json::to_value(&resumed).unwrap(), case["frozen"]);
        assert_eq!(family.render(&resumed, &opts), before_resume);
    }
}
