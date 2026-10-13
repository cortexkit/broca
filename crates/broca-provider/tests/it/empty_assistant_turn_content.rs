//! An assistant turn with no text and no tool calls, rendered for a provider that rejects
//! `content: null`.
//!
//! A model step can bill output tokens and assemble neither text nor a tool call. That empty
//! turn is durable, so replay carries it into the next episode and it gets rendered as history.
//! The `tool_call_content_empty_string` quirk was written for "tool calls and no text", which
//! left this shape emitting `{"role":"assistant","content":null}` — refused by Ollama-Cloud
//! with 400 "invalid message content type: <nil>", killing the whole continuation over a turn
//! the model itself produced. Production held 32 such episodes; all 5 that were ever continued
//! failed with exactly that status.
//!
//! Both arms matter. Opted in, an empty turn must render `""`. Opted out, it must still render
//! `null`, because that is the OpenAI convention and every provider that has not asked for the
//! quirk is currently served that way.

use broca_protocol::content::{ContentBlock, Message, Role};
use broca_protocol::request::{CallOptions, ModelHandle};
use broca_provider::families::{openai_chat_frozen_config, OpenAiChat};
use broca_provider::wire::WireFamily;
use serde_json::Value;

/// A lineage whose middle assistant turn is empty — the durable shape a text-less, call-less
/// step leaves behind — followed by the user turn that makes it history rather than the tail.
fn prompt_with_an_empty_assistant_turn() -> Vec<Message> {
    vec![
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "first".into(),
            }],
        ),
        Message::assistant_from(
            Vec::new(),
            ModelHandle::new("ollama-cloud", "deepseek-v4-flash"),
        ),
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "second".into(),
            }],
        ),
    ]
}

fn render_assistant_content(opted_in: bool) -> Value {
    let mut config = openai_chat_frozen_config("deepseek-v4-flash");
    config.tool_call_content_empty_string = opted_in;
    let options = CallOptions::new(prompt_with_an_empty_assistant_turn());

    let body: Value = serde_json::from_slice(&OpenAiChat.render(&config, &options))
        .expect("the rendered body is JSON");

    let messages = body["messages"]
        .as_array()
        .expect("the body carries a messages array");
    let assistant = messages
        .iter()
        .find(|m| m["role"] == "assistant")
        .expect("the empty assistant turn survives to the wire");
    assert!(
        assistant.get("tool_calls").is_none(),
        "the fixture turn has no tool calls; if this fires the fixture stopped testing the \
         no-tool-call path and the quirk's old scope would cover it"
    );
    assistant["content"].clone()
}

#[test]
fn an_opted_in_provider_gets_an_empty_string_for_a_text_less_call_less_turn() {
    assert_eq!(
        render_assistant_content(true),
        Value::String(String::new()),
        "a provider that rejects null content must not receive null for an empty turn"
    );
}

#[test]
fn an_opted_out_provider_still_gets_null() {
    assert_eq!(
        render_assistant_content(false),
        Value::Null,
        "the quirk is opt-in; unset must leave the OpenAI convention untouched"
    );
}
