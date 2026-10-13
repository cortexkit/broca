use broca_module_serve::wire::{SendParams, SendParamsExt};
use broca_protocol::{
    content::{ContentBlock, Message, Role},
    request::CallOptions,
    BindIdentity, RunId,
};
use broca_provider::{
    families::{
        openai_chat_frozen_config, openai_responses_frozen_config, OpenAiChat, OpenAiResponses,
    },
    policy::SystemPromptPolicy,
    wire::WireFamily,
};
use serde_json::{json, Value};

fn prompt_blocks(blocks: &[&str]) -> CallOptions {
    let params: SendParams = serde_json::from_value(json!({
        "model": {"provider": "openai", "model": "m"},
        "prompt_blocks": blocks
    }))
    .unwrap();
    let request = params
        .into_send_request(BindIdentity::new("/tmp", "h", "s"), RunId("r".into()))
        .unwrap();
    CallOptions::new(request.request.input)
}

#[test]
fn chat_prompt_blocks_are_exact_parts() {
    let body = OpenAiChat.shape(
        &openai_chat_frozen_config("m"),
        &prompt_blocks(&["stable context", "new request"]),
    );
    assert_eq!(
        body["messages"],
        json!([{"role": "user", "content": [
            {"type": "text", "text": "stable context"}, {"type": "text", "text": "new request"}
        ]}])
    );
}

#[test]
fn responses_prompt_blocks_are_exact_parts() {
    let body = OpenAiResponses.shape(
        &openai_responses_frozen_config("m"),
        &prompt_blocks(&["stable context", "new request"]),
    );
    assert_eq!(
        body["input"],
        json!([{"type": "message", "role": "user", "content": [
            {"type": "input_text", "text": "stable context"}, {"type": "input_text", "text": "new request"}
        ]}])
    );
}

#[test]
fn chat_prompt_blocks_never_fuse_words() {
    let bytes = OpenAiChat.render(&openai_chat_frozen_config("m"), &prompt_blocks(&["a", "b"]));
    assert!(!String::from_utf8(bytes).unwrap().contains("ab"));
}

#[test]
fn responses_prompt_blocks_never_fuse_words() {
    let bytes = OpenAiResponses.render(
        &openai_responses_frozen_config("m"),
        &prompt_blocks(&["a", "b"]),
    );
    assert!(!String::from_utf8(bytes).unwrap().contains("ab"));
}

fn system_blocks() -> CallOptions {
    // Multiple blocks expose accidental use of user content-part arrays for system messages.
    CallOptions::new(vec![Message::new(
        Role::System,
        vec![
            ContentBlock::Text {
                text: "first".into(),
            },
            ContentBlock::Text {
                text: "second".into(),
            },
        ],
    )])
}

#[test]
fn chat_system_blocks_keep_legacy_scalar() {
    let body: Value = serde_json::from_slice(
        &OpenAiChat.render(&openai_chat_frozen_config("m"), &system_blocks()),
    )
    .unwrap();
    assert_eq!(
        body["messages"],
        json!([{"role": "system", "content": "firstsecond"}])
    );
}

#[test]
fn responses_system_blocks_keep_legacy_scalar() {
    let mut frozen = openai_responses_frozen_config("m");
    let instructions: Value =
        serde_json::from_slice(&OpenAiResponses.render(&frozen, &system_blocks())).unwrap();
    assert_eq!(instructions["instructions"], "firstsecond");
    assert_eq!(instructions["input"], json!([]));
    frozen.system_prompt = SystemPromptPolicy::SystemMessage;
    let body: Value =
        serde_json::from_slice(&OpenAiResponses.render(&frozen, &system_blocks())).unwrap();
    assert_eq!(
        body["input"],
        json!([{"type": "message", "role": "system", "content": "firstsecond"}])
    );
    assert!(body.get("instructions").is_none());
}
