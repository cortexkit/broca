//! `ReasoningContentRequired` (codec.md §6.3): the DeepSeek reasoning echo.
//!
//! DeepSeek's thinking mode needs each earlier assistant turn's reasoning sent back as
//! `reasoning_content` on EVERY assistant message, including turns without a tool call. When
//! the provider opts in (the catalog opts in DeepSeek's own API), the OpenAI-Chat render puts
//! the stored reasoning text there verbatim, or `""` when the turn stored none. Every other
//! provider without an echo policy renders without the field, as in the legacy shape.
//! Goldens pin DeepSeek, Kimi and OpenRouter policies across user boundaries, text-only turns,
//! tool calls and missing reasoning. The legacy scalar flag remains covered for resumed runs.

use crate::render_golden_support;

use broca_protocol::content::EncryptedReasoningFormat;
use broca_protocol::{
    content::{ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult},
    request::{CallOptions, ModelHandle, Tool, ToolChoice},
    ToolCallId,
};
use broca_provider::families::{
    openai_chat_frozen_config, OpenAiChat, OPENAI_CHAT_RENDERER_VERSION,
};
use broca_provider::policy::{ReasoningEchoPolicy, ReasoningEchoTurns};
use broca_provider::wire::{FrozenRenderConfig, WireFamily};
use serde_json::{json, Value};

fn deepseek() -> ModelHandle {
    ModelHandle::new("deepseek", "deepseek-v4-flash")
}

fn read_call(id: &str, path: &str) -> ContentBlock {
    ContentBlock::ToolCall(ToolCall {
        thought_signature: None,
        tool_call_id: ToolCallId(id.into()),
        tool_name: "read".into(),
        input: json!({ "path": path }),
    })
}

fn read_result(id: &str, text: &str) -> Message {
    Message::new(
        Role::Tool,
        vec![ContentBlock::ToolResult(ToolResult {
            tool_call_id: ToolCallId(id.into()),
            output: ToolOutput::Text { text: text.into() },
            is_error: false,
        })],
    )
}

fn user(text: &str) -> Message {
    Message::new(Role::User, vec![ContentBlock::Text { text: text.into() }])
}

fn reasoning(text: &str) -> ContentBlock {
    ContentBlock::Reasoning {
        text: text.into(),
        signature: None,
    }
}

/// A multi-turn DeepSeek tool loop covering each assistant shape the echo treats differently:
/// a tool-call turn with stored reasoning, a text turn (no tool call) with stored reasoning, a
/// new user message between them, then a tool-call turn and a text turn with no stored
/// reasoning at all.
fn tool_loop() -> CallOptions {
    let mut opts = CallOptions::new(vec![
        Message::new(
            Role::System,
            vec![ContentBlock::Text {
                text: "You are a coding agent.".into(),
            }],
        ),
        user("Fix the bug."),
        Message::assistant_from(
            vec![
                reasoning("I should read main.rs first."),
                read_call("call_00_a", "main.rs"),
            ],
            deepseek(),
        ),
        read_result("call_00_a", "fn main() {}"),
        Message::assistant_from(
            vec![
                reasoning("The file is empty; report it."),
                ContentBlock::Text {
                    text: "main.rs is empty.".into(),
                },
            ],
            deepseek(),
        ),
        user("Check lib.rs too."),
        Message::assistant_from(vec![read_call("call_01_b", "lib.rs")], deepseek()),
        read_result("call_01_b", "pub fn f() {}"),
        Message::assistant_from(
            vec![ContentBlock::Text {
                text: "lib.rs defines f.".into(),
            }],
            deepseek(),
        ),
        user("Thanks."),
    ]);
    opts.tools = vec![Tool {
        name: "read".into(),
        description: "Read a file.".into(),
        input_schema: json!({
            "type": "object",
            "properties": { "path": { "type": "string" } },
            "required": ["path"]
        }),
        module: None,
        execution_mode: Default::default(),
        reply: None,
    }]
    .into();
    opts.tool_choice = ToolChoice::Auto.into();
    opts
}

fn deepseek_config(reasoning_content_required: bool) -> FrozenRenderConfig {
    let mut frozen = openai_chat_frozen_config("deepseek-v4-flash");
    frozen.base_url = Some("https://api.deepseek.com".into());
    frozen.reasoning_content_required = reasoning_content_required;
    frozen
}

#[test]
fn pre_chat_reasoning_policy_configs_and_request_bytes_are_pinned() {
    let cases: Vec<Value> =
        serde_json::from_str(include_str!("../fixtures/pre_chat_reasoning_policy.json")).unwrap();
    assert_eq!(cases.len(), 4);
    let opts = CallOptions::new(vec![
        user("question"),
        Message::new(
            Role::Assistant,
            vec![
                reasoning("earlier thinking"),
                ContentBlock::Text {
                    text: "answer".into(),
                },
            ],
        ),
        user("again"),
    ]);
    for case in cases {
        let frozen: FrozenRenderConfig = serde_json::from_value(case["frozen"].clone()).unwrap();
        assert_eq!(serde_json::to_value(&frozen).unwrap(), case["frozen"]);
        assert_eq!(
            OpenAiChat.render(&frozen, &opts),
            case["body"].as_str().unwrap().as_bytes()
        );
    }
}

#[test]
fn kimi_echoes_history_before_last_user_and_empty_text_turns_golden() {
    let mut frozen = openai_chat_frozen_config("kimi-k3");
    frozen.reasoning_echo = Some(ReasoningEchoPolicy::KIMI);
    assert_golden(
        OpenAiChat.render(&frozen, &tool_loop()),
        include_str!("../fixtures/openai_chat_kimi_reasoning_echo_render_golden.json"),
        include_str!("../fixtures/openai_chat_kimi_reasoning_echo_render_golden.json.version"),
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/openai_chat_kimi_reasoning_echo_render_golden.json"
        ),
        "OpenAI-Chat Kimi reasoning echo",
    );
    let mut legacy = deepseek_config(true);
    let expected = OpenAiChat.render(&legacy, &tool_loop());
    legacy.reasoning_content_required = false;
    legacy.reasoning_echo = Some(ReasoningEchoPolicy::DEEPSEEK);
    assert_eq!(OpenAiChat.render(&legacy, &tool_loop()), expected);
}

fn openrouter_state(reasoning: &str, details: Value) -> ContentBlock {
    ContentBlock::RedactedReasoning {
        data: json!({"reasoning": reasoning, "reasoning_details": details}).to_string(),
        provider_format: Some(EncryptedReasoningFormat::OpenRouter),
    }
}

#[test]
fn openrouter_echoes_opaque_details_on_tool_and_text_turns_golden() {
    let mut opts = tool_loop();
    opts.prompt[2].content[0] = openrouter_state(
        "Plan",
        json!([
            {"type":"reasoning.text","text":"Plan","format":"unknown-v2","index":0,"id":null,"extra":{"keep":true}},
            {"type":"reasoning.encrypted","data":"opaque+/==","format":"google-gemini-v1","index":1,"id":"r1"}
        ]),
    );
    opts.prompt[4].content[0] = openrouter_state(
        "Summary",
        json!([
            {"type":"reasoning.summary","summary":"Summary","format":"anthropic-claude-v1","id":"r2"}
        ]),
    );
    let mut frozen = openai_chat_frozen_config("google/gemini-2.5-flash");
    frozen.reasoning_echo = Some(ReasoningEchoPolicy::OPENROUTER);
    assert_golden(
        OpenAiChat.render(&frozen, &opts),
        include_str!("../fixtures/openai_chat_openrouter_reasoning_echo_render_golden.json"),
        include_str!(
            "../fixtures/openai_chat_openrouter_reasoning_echo_render_golden.json.version"
        ),
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/openai_chat_openrouter_reasoning_echo_render_golden.json"
        ),
        "OpenAI-Chat OpenRouter reasoning echo",
    );
    for policy in [None, Some(ReasoningEchoPolicy::KIMI)] {
        frozen.reasoning_echo = policy;
        let body = render(&frozen, &opts);
        assert!(
            !body.to_string().contains("opaque+/=="),
            "foreign format must not leak"
        );
        assert!(assistant_messages(&body)
            .iter()
            .all(|m| m.get("reasoning_details").is_none()));
    }
}

#[test]
fn echo_policy_turn_coverage_is_frozen_data() {
    let mut frozen = deepseek_config(false);
    frozen.reasoning_echo = Some(ReasoningEchoPolicy {
        turns: ReasoningEchoTurns::ToolCalls,
        ..ReasoningEchoPolicy::KIMI
    });
    let assistants = assistant_messages(&render(&frozen, &tool_loop()));
    assert_eq!(
        assistants[0]["reasoning_content"],
        "I should read main.rs first."
    );
    assert!(assistants[1].get("reasoning_content").is_none());
    assert_eq!(assistants[2]["reasoning_content"], "");
    assert!(assistants[3].get("reasoning_content").is_none());
}

#[test]
fn openrouter_decodes_and_persists_reasoning_details_losslessly() {
    use broca_protocol::{
        provider::{NormalizedEvent, ProviderControl},
        StepId,
    };
    let mut frozen = openai_chat_frozen_config("google/gemini-2.5-flash");
    frozen.reasoning_echo = Some(ReasoningEchoPolicy::OPENROUTER);
    let mut decoder = OpenAiChat.normalizer(&frozen, StepId(1));
    let fixture = include_bytes!("../fixtures/openrouter_reasoning_details.sse");
    let mut events = Vec::new();
    for chunk in fixture.chunks(7) {
        events.extend(decoder.push(chunk).unwrap());
    }
    events.extend(decoder.finish().unwrap());
    let message = events
        .into_iter()
        .find_map(|e| match e {
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                Some(message)
            }
            _ => None,
        })
        .expect("fixture assembles");
    let persisted = serde_json::to_vec(&message.content).unwrap();
    let restored: Vec<ContentBlock> = serde_json::from_slice(&persisted).unwrap();
    let ContentBlock::RedactedReasoning {
        data,
        provider_format,
    } = &restored[0]
    else {
        panic!("missing opaque state")
    };
    assert_eq!(*provider_format, Some(EncryptedReasoningFormat::OpenRouter));
    let envelope: Value = serde_json::from_str(data).unwrap();
    assert_eq!(
        envelope,
        json!({"reasoning":"Think carefully.", "reasoning_details":[
            {"type":"reasoning.text","text":"Think ","format":"unknown-v2","index":0,"id":null,"vendor":{"x":[1,2]}},
            {"type":"reasoning.text","text":"carefully.","format":"unknown-v2","index":0,"id":null},
            {"type":"reasoning.encrypted","data":"AQID+/==","format":"google-gemini-v1","index":1,"id":"encrypted-1"},
            {"type":"reasoning.summary","summary":"Check the clock","format":"anthropic-claude-v1","index":2,"id":"summary-2"}
        ]})
    );
    let opts = CallOptions::new(vec![Message::new(Role::Assistant, restored)]);
    let body = render(&frozen, &opts);
    assert_eq!(
        body["messages"][0]["reasoning_details"],
        envelope["reasoning_details"]
    );
    assert!(
        body["messages"][0].get("reasoning").is_none(),
        "details take precedence over redundant scalar text"
    );
}

#[test]
fn openrouter_scalar_only_reasoning_round_trips_without_foreign_echo() {
    use broca_protocol::{
        provider::{NormalizedEvent, ProviderControl},
        StepId,
    };
    let mut frozen = openai_chat_frozen_config("test");
    frozen.reasoning_echo = Some(ReasoningEchoPolicy::OPENROUTER);
    let mut decoder = OpenAiChat.normalizer(&frozen, StepId(1));
    let mut events = decoder.push(b"data: {\"id\":\"r\",\"choices\":[{\"delta\":{\"reasoning\":\"raw reasoning\",\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n").unwrap();
    events.extend(decoder.finish().unwrap());
    let content = events
        .into_iter()
        .find_map(|e| match e {
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                Some(message.content)
            }
            _ => None,
        })
        .unwrap();
    let opts = CallOptions::new(vec![Message::new(Role::Assistant, content)]);
    assert_eq!(
        render(&frozen, &opts)["messages"][0]["reasoning"],
        "raw reasoning"
    );
    frozen.reasoning_echo = Some(ReasoningEchoPolicy::DEEPSEEK);
    assert_eq!(
        render(&frozen, &opts)["messages"][0]["reasoning_content"],
        ""
    );
}

fn render(frozen: &FrozenRenderConfig, opts: &CallOptions) -> Value {
    serde_json::from_slice(&OpenAiChat.render(frozen, opts)).unwrap()
}

fn assistant_messages(body: &Value) -> Vec<Value> {
    body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|m| m["role"] == "assistant")
        .cloned()
        .collect()
}

const ECHO_GOLDEN: &str =
    include_str!("../fixtures/openai_chat_deepseek_reasoning_echo_render_golden.json");
const ECHO_GOLDEN_VERSION: &str =
    include_str!("../fixtures/openai_chat_deepseek_reasoning_echo_render_golden.json.version");
const NO_ECHO_GOLDEN: &str =
    include_str!("../fixtures/openai_chat_reasoning_dropped_render_golden.json");
const NO_ECHO_GOLDEN_VERSION: &str =
    include_str!("../fixtures/openai_chat_reasoning_dropped_render_golden.json.version");

fn assert_golden(rendered: Vec<u8>, golden: &str, version: &str, golden_path: &str, name: &str) {
    let version_path = format!("{golden_path}.version");
    if render_golden_support::update_if_requested(
        golden_path,
        &version_path,
        &rendered,
        golden.as_bytes(),
        version,
        OPENAI_CHAT_RENDERER_VERSION,
        name,
    ) {
        return;
    }
    render_golden_support::assert_matches_or_requires_bump(
        &rendered,
        golden.as_bytes(),
        version,
        OPENAI_CHAT_RENDERER_VERSION,
        name,
    );
}

/// DeepSeek with the quirk: every assistant turn carries `reasoning_content`, the stored text
/// where there is one and `""` where there is none, on tool-call and text turns alike.
#[test]
fn deepseek_tool_loop_echoes_reasoning_on_every_assistant_turn_golden() {
    assert_golden(
        OpenAiChat.render(&deepseek_config(true), &tool_loop()),
        ECHO_GOLDEN,
        ECHO_GOLDEN_VERSION,
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/openai_chat_deepseek_reasoning_echo_render_golden.json"
        ),
        "OpenAI-Chat DeepSeek reasoning echo",
    );
}

/// Every provider without the quirk (and a DeepSeek run admitted before it was opted in) renders
/// the same conversation with the reasoning dropped and no `reasoning_content` field at all.
#[test]
fn tool_loop_without_the_quirk_drops_reasoning_golden() {
    for frozen in [
        deepseek_config(false),
        openai_chat_frozen_config("deepseek-v4-flash"),
    ] {
        assert_golden(
            OpenAiChat.render(&frozen, &tool_loop()),
            NO_ECHO_GOLDEN,
            NO_ECHO_GOLDEN_VERSION,
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/openai_chat_reasoning_dropped_render_golden.json"
            ),
            "OpenAI-Chat reasoning dropped",
        );
    }
}

#[test]
fn default_off_renders_no_reasoning_content() {
    let body = render(&openai_chat_frozen_config("gpt-4o"), &tool_loop());
    for m in assistant_messages(&body) {
        assert!(
            m.get("reasoning_content").is_none(),
            "default-off must NOT emit reasoning_content on any assistant turn: {m:#?}"
        );
    }
}

/// DeepSeek's guide requires the reasoning back on all previous assistant turns, "even for
/// turns where the model did not perform a tool call". Echoing only tool-call turns (the earlier
/// rule) left text turns without it.
#[test]
fn enabled_echoes_reasoning_on_every_assistant_turn() {
    let assistants = assistant_messages(&render(&deepseek_config(true), &tool_loop()));
    let echoed: Vec<(bool, Value)> = assistants
        .iter()
        .map(|m| {
            (
                m.get("tool_calls").is_some(),
                m["reasoning_content"].clone(),
            )
        })
        .collect();
    assert_eq!(
        echoed,
        vec![
            (true, json!("I should read main.rs first.")),
            (false, json!("The file is empty; report it.")),
            (true, json!("")),
            (false, json!("")),
        ],
        "each assistant turn carries its stored reasoning, or an empty string"
    );
}

/// Several reasoning blocks in one turn are sent back as their concatenation, unchanged: the
/// echo is the turn's whole thinking, not its first or last fragment.
#[test]
fn enabled_echoes_multiple_reasoning_blocks_verbatim() {
    let text = "line one\n  indented \"quoted\" ünicode ";
    let opts = CallOptions::new(vec![
        user("hi"),
        Message::assistant_from(
            vec![
                reasoning(text),
                reasoning("second part"),
                read_call("call_1", "a"),
            ],
            deepseek(),
        ),
    ]);
    let assistants = assistant_messages(&render(&deepseek_config(true), &opts));
    assert_eq!(
        assistants[0]["reasoning_content"],
        json!(format!("{text}second part"))
    );
}

#[test]
fn default_off_reasoning_blocks_match_the_legacy_dropped_render_bytes() {
    let with_reasoning = tool_loop();
    let mut legacy = with_reasoning.clone();
    for message in &mut legacy.prompt {
        message
            .content
            .retain(|block| !matches!(block, ContentBlock::Reasoning { .. }));
    }

    let frozen = openai_chat_frozen_config("deepseek-v4-flash");
    assert_eq!(
        OpenAiChat.render(&frozen, &with_reasoning),
        OpenAiChat.render(&frozen, &legacy),
        "quirk-off renders must stay byte-identical to the legacy reasoning-dropping shape"
    );
}

fn tool_call_only_prompt() -> Vec<Message> {
    vec![
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "use a tool".into(),
            }],
        ),
        Message::assistant_from(
            vec![ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId("call_1".into()),
                tool_name: "get_time".into(),
                input: json!({}),
            })],
            broca_protocol::request::ModelHandle::new("ollama-cloud", "test"),
        ),
    ]
}

fn render_tool_call_content(tool_call_content_empty_string: bool) -> Value {
    let mut frozen = openai_chat_frozen_config("test-model");
    frozen.tool_call_content_empty_string = tool_call_content_empty_string;
    serde_json::from_slice(&OpenAiChat.render(&frozen, &CallOptions::new(tool_call_only_prompt())))
        .unwrap()
}

fn tool_call_assistant(body: &Value) -> &Value {
    body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["role"] == "assistant" && m.get("tool_calls").is_some())
        .expect("tool-call assistant turn")
}

#[test]
fn tool_call_content_default_off_renders_null() {
    let body = render_tool_call_content(false);
    let turn = tool_call_assistant(&body);
    assert!(
        turn["content"].is_null(),
        "default-off must emit content: null on a tool-call-only assistant turn: {turn:#?}"
    );
}

#[test]
fn tool_call_content_enabled_renders_empty_string() {
    let body = render_tool_call_content(true);
    let turn = tool_call_assistant(&body);
    assert_eq!(
        turn["content"],
        json!(""),
        "opted-in provider must emit content: \"\" on a tool-call-only assistant turn"
    );
}
