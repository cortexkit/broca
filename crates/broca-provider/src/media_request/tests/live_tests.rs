use super::*;
use broca_protocol::CallOptions;
use std::sync::{Arc, Mutex};

pub(super) fn frozen(id: WireFamilyId) -> crate::wire::FrozenRenderConfig {
    let mut frozen = crate::families::frozen_config_for_family(id, "gemini-3-test");
    if id == WireFamilyId::GoogleCodeAssist {
        frozen.project_id = Some("fixture-project".into());
    }
    let quirk = match id {
        WireFamilyId::AnthropicMessages => Some(crate::media_profile::MediaQuirk::Anthropic),
        WireFamilyId::OpenAiResponses | WireFamilyId::OpenAiChat => {
            Some(crate::media_profile::MediaQuirk::Openai)
        }
        WireFamilyId::Gemini => Some(crate::media_profile::MediaQuirk::Google),
        _ => None,
    };
    frozen.media = Some(select_media_profile(
        id,
        quirk,
        "gemini-3-test",
        profile().modalities,
        Some(1_000_000),
    ));
    frozen
}

fn assert_media_free_legacy_render_parity_without_tail_requests(id: WireFamilyId) {
    let fixture = Fixture::new();
    let reads = Arc::new(Mutex::new(0));
    let count = reads.clone();
    let renderer = LiveMedia::new(
        fixture.store.clone(),
        Box::new(move |_| {
            *count.lock().unwrap() += 1;
            panic!("a media-free request must not request a WAL tail");
        }),
    );
    let frozen = frozen(id);
    let mut opts = CallOptions::new(vec![Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: "cache this exact text".into(),
        }],
    )]);
    opts.step_context = Some(broca_protocol::StepContext {
        run_id: "media-free".into(),
        step_index: 1,
        session_address: None,
        credential_id: None,
        account_ref: None,
        max_output_tokens: None,
    });
    let family = crate::registry::wire_family(id).unwrap();
    let legacy = family.render(&frozen, &opts);
    assert_eq!(
        renderer
            .render(&serde_json::to_value(&frozen).unwrap(), &opts, true, true)
            .unwrap(),
        legacy,
        "{id:?}"
    );
    assert_eq!(*reads.lock().unwrap(), 0);
}

macro_rules! media_free_family {
    ($test:ident, $family:ident) => {
        #[test]
        fn $test() {
            assert_media_free_legacy_render_parity_without_tail_requests(WireFamilyId::$family);
        }
    };
}
media_free_family!(
    live_media_free_anthropic_legacy_render_parity_without_tail_requests,
    AnthropicMessages
);
media_free_family!(
    live_media_free_bedrock_legacy_render_parity_without_tail_requests,
    BedrockConverse
);
media_free_family!(
    live_media_free_openai_chat_legacy_render_parity_without_tail_requests,
    OpenAiChat
);
media_free_family!(
    live_media_free_openai_responses_legacy_render_parity_without_tail_requests,
    OpenAiResponses
);
media_free_family!(
    live_media_free_chatgpt_legacy_render_parity_without_tail_requests,
    OpenAiResponsesChatgpt
);
media_free_family!(
    live_media_free_gemini_legacy_render_parity_without_tail_requests,
    Gemini
);
media_free_family!(
    live_media_free_codeassist_legacy_render_parity_without_tail_requests,
    GoogleCodeAssist
);
