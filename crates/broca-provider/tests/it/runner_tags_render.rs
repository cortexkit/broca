//! Runner tags in rendered requests, for every wire family.
//!
//! Tags are metadata: a prompt whose messages carry tags renders byte-for-byte like the same
//! prompt without them, so a run that does not render tags sends exactly what it sent before
//! tags existed. A run frozen with `render_tags` prefixes `§N§ ` to tagged user text and
//! tool-result text, and on Anthropic Messages to plain assistant text as well. It never shows
//! a tag in signed reasoning, a tool call or a JSON tool output, on any family. Its bytes are
//! pinned per family here.

use std::sync::Arc;

use broca_protocol::{
    content::{ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult},
    request::{CallOptions, ModelHandle, Tool},
    tags::{render_tags_into, PartTags, TagRendering},
    ToolCallId,
};
use broca_provider::families::{
    anthropic_frozen_config, bedrock_frozen_config, gemini_frozen_config,
    google_codeassist_frozen_config, openai_chat_frozen_config,
    openai_responses_chatgpt_frozen_config, openai_responses_frozen_config, AnthropicMessages,
    BedrockConverse, Gemini, GoogleCodeAssist, OpenAiChat, OpenAiResponses, OpenAiResponsesChatgpt,
};
use broca_provider::wire::{FrozenRenderConfig, WireFamily};
use sha2::{Digest, Sha256};

fn tagged(mut message: Message, parts: &[u64]) -> Message {
    message.set_tags(Some(Arc::new(PartTags::all(parts.iter().copied()))));
    message
}

/// A user turn, an assistant tool turn with text, signed reasoning and a call, the tool turn's
/// text and JSON results, and a second user message. With `with_tags`, every part is tagged.
fn prompt(with_tags: bool) -> Vec<Message> {
    let tag = |message: Message, parts: &[u64]| {
        if with_tags {
            tagged(message, parts)
        } else {
            message
        }
    };
    let call = |id: &str| ToolCall {
        tool_call_id: ToolCallId(id.into()),
        tool_name: "lookup".into(),
        input: serde_json::json!({"q": "tags"}),
        thought_signature: None,
    };
    vec![
        tag(
            Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "hello".into(),
                }],
            ),
            &[1],
        ),
        tag(
            Message::assistant_from(
                vec![
                    ContentBlock::Reasoning {
                        text: "thinking".into(),
                        signature: Some("c2lnbmVk".into()),
                    },
                    ContentBlock::Text {
                        text: "let me look".into(),
                    },
                    ContentBlock::ToolCall(call("call-1")),
                    ContentBlock::ToolCall(call("call-2")),
                ],
                ModelHandle::new("p", "m"),
            ),
            &[2, 3, 4, 5],
        ),
        tag(
            Message::new(
                Role::Tool,
                vec![
                    ContentBlock::ToolResult(ToolResult {
                        tool_call_id: ToolCallId("call-1".into()),
                        output: ToolOutput::Text {
                            text: "found it".into(),
                        },
                        is_error: false,
                    }),
                    ContentBlock::ToolResult(ToolResult {
                        tool_call_id: ToolCallId("call-2".into()),
                        output: ToolOutput::Json {
                            value: serde_json::json!({"hits": 2}),
                        },
                        is_error: false,
                    }),
                ],
            ),
            &[6, 7],
        ),
        tag(
            Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "thanks".into(),
                }],
            ),
            &[8],
        ),
    ]
}

fn options(prompt: Vec<Message>) -> CallOptions {
    let mut options = CallOptions::new(prompt);
    // Families that embed a per-request id derive it from the run and step.
    options.step_context = Some(broca_protocol::StepContext {
        run_id: "run-tags".into(),
        step_index: 2,
    });
    options.tools = Arc::new(vec![Tool {
        name: "lookup".into(),
        description: "look something up".into(),
        input_schema: serde_json::json!({"type":"object","properties":{"q":{"type":"string"}}}),
        module: None,
        execution_mode: Default::default(),
    }]);
    options
}

fn families() -> Vec<(&'static str, Box<dyn WireFamily>, FrozenRenderConfig)> {
    let mut codeassist = google_codeassist_frozen_config("gemini-2.5-flash");
    codeassist.project_id = Some("test-project".into());
    vec![
        (
            "anthropic_messages",
            Box::new(AnthropicMessages),
            anthropic_frozen_config("claude-opus-4-8"),
        ),
        (
            "openai_chat",
            Box::new(OpenAiChat),
            openai_chat_frozen_config("gpt-4o"),
        ),
        (
            "openai_responses",
            Box::new(OpenAiResponses),
            openai_responses_frozen_config("gpt-4o"),
        ),
        (
            "openai_responses_chatgpt",
            Box::new(OpenAiResponsesChatgpt),
            openai_responses_chatgpt_frozen_config("gpt-5-codex"),
        ),
        (
            "gemini",
            Box::new(Gemini),
            gemini_frozen_config("gemini-2.5-flash"),
        ),
        ("google_codeassist", Box::new(GoogleCodeAssist), codeassist),
        (
            "bedrock_converse",
            Box::new(BedrockConverse),
            bedrock_frozen_config("amazon.nova-lite-v1:0"),
        ),
    ]
}

/// The SHA-256 of each family's rendered request for the tagged prompt with `render_tags` on,
/// whose bytes are kept in `tests/golden/runner_tags/<family>.json`. A change here changes what
/// a tag-rendering run sends, so it is a renderer change.
const RENDERED_SHA256: &[(&str, &str)] = &[
    (
        "anthropic_messages",
        "3a9a06b3a9d6d6abed6574a838e493fc6bb61b9f884b1738178d0a99973b1662",
    ),
    (
        "openai_chat",
        "b94de02ef5e59ded372ac839714ee3e366d4ed527121a9242c7f8c65b7361932",
    ),
    (
        "openai_responses",
        "890bdd44d303dd947405afe02dc65e2f82a841cf27ddad4b5863f42d68555eb7",
    ),
    (
        "openai_responses_chatgpt",
        "f716d889f02d58f68d9c549178003eb1288874532cd0f5e63d8d639a5d1aa6a8",
    ),
    (
        "gemini",
        "2c6d77fa5fdad81d8ca041e2d9a7837500caf408cf78f4d082b1ac82dee87afa",
    ),
    (
        "google_codeassist",
        "d191804878cfc48dd04ace1e59bce637702430a6a76b69944843ad2a6e3edcd1",
    ),
    (
        "bedrock_converse",
        "71b429efaedeaf6dcc070289a3bb6ec0cdb9ead18b3e86cbf942af2492c3f0bd",
    ),
];

fn hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[test]
fn tags_never_change_a_request_rendered_without_render_tags() {
    for (name, family, frozen) in families() {
        let plain = family.render(&frozen, &options(prompt(false)));
        let tagged = family.render(&frozen, &options(prompt(true)));
        assert_eq!(
            plain, tagged,
            "{name}: tags must not reach the request bytes"
        );
        assert!(
            !String::from_utf8_lossy(&tagged).contains('§'),
            "{name}: no tag mark without render_tags"
        );
    }
}

#[test]
fn render_tags_prefixes_only_user_and_tool_result_text_and_is_pinned_per_family() {
    let mut digests = Vec::new();
    for (name, family, frozen) in families() {
        let rendering = TagRendering::for_family(Some(name));
        let mut rendered_prompt = prompt(true);
        render_tags_into(&mut rendered_prompt, rendering);
        let body = family.render(&frozen, &options(rendered_prompt.clone()));
        let text = String::from_utf8(body.clone()).expect("a JSON request body");
        for mark in ["§1§ hello", "§6§ found it", "§8§ thanks"] {
            assert!(text.contains(mark), "{name}: missing {mark} in {text}");
        }
        // Only Anthropic shows the assistant text's tag. No family shows the thinking's (2), a
        // tool call's (4, 5) or the JSON result's (7).
        assert_eq!(
            text.contains("§3§ let me look"),
            name == "anthropic_messages",
            "{name}: assistant text {text}"
        );
        assert_eq!(rendering.assistant_text, name == "anthropic_messages");
        for untouched in ["§2§", "§4§", "§5§", "§7§"] {
            assert!(
                !text.contains(untouched),
                "{name}: thinking, a call or a JSON result was rendered with {untouched}: {text}"
            );
        }
        if name != "anthropic_messages" {
            assert!(!text.contains("§3§"), "{name}: {text}");
        }
        // Rendering an already rendered prompt again changes nothing: a part's rendering is
        // fixed the first time and every later request repeats it.
        render_tags_into(&mut rendered_prompt, rendering);
        assert_eq!(
            family.render(&frozen, &options(rendered_prompt)),
            body,
            "{name}: rendering is not idempotent"
        );
        let golden = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/golden/runner_tags")
            .join(format!("{name}.json"));
        if std::env::var_os("BROCA_WRITE_RUNNER_TAG_GOLDENS").is_some() {
            std::fs::create_dir_all(golden.parent().unwrap()).unwrap();
            std::fs::write(&golden, &body).unwrap();
        }
        let pinned = std::fs::read(&golden)
            .unwrap_or_else(|error| panic!("read golden {}: {error}", golden.display()));
        assert_eq!(
            String::from_utf8_lossy(&pinned),
            text,
            "{name}: rendered bytes differ from {}",
            golden.display()
        );
        digests.push((name, hex(&pinned)));
    }
    let expected: Vec<(&str, String)> = RENDERED_SHA256
        .iter()
        .map(|(name, digest)| (*name, (*digest).to_owned()))
        .collect();
    assert_eq!(digests, expected, "a family's tag-rendered bytes changed");
}
