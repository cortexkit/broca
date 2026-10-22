//! Golden-byte test for the Gemini render.

use crate::render_golden_support;

use broca_protocol::{
    content::{ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult},
    request::{CallOptions, ModelHandle, Tool, ToolChoice},
    ToolCallId,
};
use broca_provider::families::{gemini_frozen_config, Gemini, GEMINI_RENDERER_VERSION};
use broca_provider::wire::WireFamily;
use serde_json::{json, Value};

const PROVIDER: &str = "google";
const MODEL_X: &str = "gemini-3.5-flash-low";
const MODEL_Y: &str = "gemini-3.5-flash-high";

pub(crate) fn fixed_request() -> CallOptions {
    let mut opts = CallOptions::new(vec![
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::System,
            content: vec![ContentBlock::Text {
                text: "Be concise.".into(),
            }],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: "What time is it in Madrid?".into(),
            }],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::Assistant,
            content: vec![ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId("call_01".into()),
                tool_name: "get_time".into(),
                input: json!({"timezone": "Europe/Madrid"}),
            })],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::Tool,
            content: vec![ContentBlock::ToolResult(ToolResult {
                tool_call_id: ToolCallId("call_01".into()),
                output: ToolOutput::Text {
                    text: "14:32".into(),
                },
                is_error: false,
            })],
        },
    ]);
    opts.tools = vec![
        Tool {
            name: "get_time".into(),
            description: "Get the current time in an IANA timezone.".into(),
            input_schema: json!({
                "type": "object",
                "properties": { "timezone": { "type": "string" } },
                "required": ["timezone"]
            }),
            module: None,
            execution_mode: Default::default(),
            reply: None,
        },
        Tool {
            name: "get_weather".into(),
            description: "Get the weather for a city.".into(),
            input_schema: json!({
                "type": "object",
                "properties": { "city": { "type": "string" } }
            }),
            module: None,
            execution_mode: Default::default(),
            reply: None,
        },
    ]
    .into();
    opts.tool_choice = ToolChoice::Auto.into();
    opts
}

fn json_tool_result_request(value: Value) -> CallOptions {
    CallOptions::new(vec![
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::Assistant,
            content: vec![ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId("call_json".into()),
                tool_name: "get_data".into(),
                input: json!({}),
            })],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::Tool,
            content: vec![ContentBlock::ToolResult(ToolResult {
                tool_call_id: ToolCallId("call_json".into()),
                output: ToolOutput::Json { value },
                is_error: false,
            })],
        },
    ])
}

fn render_json_tool_result(value: Value) -> Vec<u8> {
    Gemini.render(
        &gemini_frozen_config("gemini-2.0-flash"),
        &json_tool_result_request(value),
    )
}

fn function_response(rendered: &[u8]) -> Value {
    let body: Value = serde_json::from_slice(rendered).expect("Gemini render is valid JSON");
    body["contents"][1]["parts"][0]["functionResponse"]["response"].clone()
}

fn two_tool_turn_request(first_id: &str, second_id: &str) -> CallOptions {
    CallOptions::new(vec![
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::Assistant,
            content: vec![ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId(first_id.into()),
                tool_name: "search".into(),
                input: json!({"query": "broca"}),
            })],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::Tool,
            content: vec![ContentBlock::ToolResult(ToolResult {
                tool_call_id: ToolCallId(first_id.into()),
                output: ToolOutput::Text {
                    text: "search-result".into(),
                },
                is_error: false,
            })],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::Assistant,
            content: vec![ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId(second_id.into()),
                tool_name: "read".into(),
                input: json!({"path": "README.md"}),
            })],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::Tool,
            content: vec![ContentBlock::ToolResult(ToolResult {
                tool_call_id: ToolCallId(second_id.into()),
                output: ToolOutput::Text {
                    text: "read-result".into(),
                },
                is_error: false,
            })],
        },
    ])
}

fn tool_turn_names(contents: &Value) -> Vec<&str> {
    vec![
        contents[0]["parts"][0]["functionCall"]["name"]
            .as_str()
            .expect("first functionCall name"),
        contents[1]["parts"][0]["functionResponse"]["name"]
            .as_str()
            .expect("first functionResponse name"),
        contents[2]["parts"][0]["functionCall"]["name"]
            .as_str()
            .expect("second functionCall name"),
        contents[3]["parts"][0]["functionResponse"]["name"]
            .as_str()
            .expect("second functionResponse name"),
    ]
}

pub(crate) const GOLDEN: &str = include_str!("../fixtures/gemini_render_golden.json");
pub(crate) const GOLDEN_VERSION: &str =
    include_str!("../fixtures/gemini_render_golden.json.version");

/// The frozen config the golden bytes are rendered with.
pub(crate) fn golden_frozen() -> broca_provider::wire::FrozenRenderConfig {
    gemini_frozen_config("gemini-2.0-flash")
}

fn contains_object_key(value: &Value, needle: &str) -> bool {
    match value {
        Value::Object(object) => {
            object.contains_key(needle)
                || object
                    .values()
                    .any(|value| contains_object_key(value, needle))
        }
        Value::Array(array) => array.iter().any(|value| contains_object_key(value, needle)),
        _ => false,
    }
}

#[test]
fn gemini_render_matches_golden() {
    let frozen = golden_frozen();
    let bytes = Gemini.render(&frozen, &fixed_request());
    let rendered = String::from_utf8(bytes).expect("rendered bytes are UTF-8");

    if render_golden_support::update_if_requested(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/gemini_render_golden.json"
        ),
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/gemini_render_golden.json.version"
        ),
        rendered.as_bytes(),
        GOLDEN.as_bytes(),
        GOLDEN_VERSION,
        GEMINI_RENDERER_VERSION,
        "Gemini",
    ) {
        return;
    }

    render_golden_support::assert_matches_or_requires_bump(
        rendered.as_bytes(),
        GOLDEN.as_bytes(),
        GOLDEN_VERSION,
        GEMINI_RENDERER_VERSION,
        "Gemini",
    );
}

#[test]
fn gemini_scopes_repeated_tool_call_ids_to_their_own_assistant_group() {
    let frozen = gemini_frozen_config("gemini-2.0-flash");
    let rendered: Value =
        serde_json::from_slice(&Gemini.render(&frozen, &two_tool_turn_request("call_0", "call_0")))
            .expect("Gemini render is valid JSON");

    assert_eq!(
        tool_turn_names(&rendered["contents"]),
        ["search", "search", "read", "read"]
    );
}

#[test]
fn gemini_distinct_tool_call_ids_keep_pinned_multi_turn_bytes() {
    let frozen = gemini_frozen_config("gemini-2.0-flash");
    let rendered = Gemini.render(&frozen, &two_tool_turn_request("call_search", "call_read"));

    assert_eq!(
        rendered,
        br#"{"contents":[{"parts":[{"functionCall":{"args":{"query":"broca"},"name":"search"}}],"role":"model"},{"parts":[{"functionResponse":{"name":"search","response":{"result":"search-result"}}}],"role":"user"},{"parts":[{"functionCall":{"args":{"path":"README.md"},"name":"read"}}],"role":"model"},{"parts":[{"functionResponse":{"name":"read","response":{"result":"read-result"}}}],"role":"user"}],"generationConfig":{"maxOutputTokens":2048}}"#
    );
}

#[test]
fn gemini_wraps_array_json_tool_results_in_an_object() {
    let rendered = render_json_tool_result(json!([{"row": 1}, {"row": 2}]));

    assert_eq!(
        function_response(&rendered),
        json!({"result": [{"row": 1}, {"row": 2}]})
    );
}

#[test]
fn gemini_wraps_scalar_and_null_json_tool_results_in_an_object() {
    for (input, expected) in [
        (json!(42), json!({"result": 42})),
        (json!(true), json!({"result": true})),
        (Value::Null, json!({"result": null})),
    ] {
        assert_eq!(function_response(&render_json_tool_result(input)), expected);
    }
}

#[test]
fn gemini_object_json_tool_result_render_bytes_stay_pinned() {
    let rendered = render_json_tool_result(json!({
        "nested": [1, 2],
        "ok": true
    }));

    assert_eq!(
        rendered,
        br#"{"contents":[{"parts":[{"functionCall":{"args":{},"name":"get_data"}}],"role":"model"},{"parts":[{"functionResponse":{"name":"get_data","response":{"nested":[1,2],"ok":true}}}],"role":"user"}],"generationConfig":{"maxOutputTokens":2048}}"#
    );
}

fn signed_call_request(target_model: &str) -> CallOptions {
    let mut opts = CallOptions::new(vec![
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "look it up".into(),
            }],
        ),
        Message::assistant_from(
            vec![ContentBlock::ToolCall(ToolCall {
                tool_call_id: ToolCallId("call_01".into()),
                tool_name: "get_time".into(),
                input: json!({"timezone": "Europe/Madrid"}),
                thought_signature: Some("gemini-signed-call".into()),
            })],
            ModelHandle::new(PROVIDER, MODEL_X),
        ),
    ]);
    opts.target_model = Some(ModelHandle::new(PROVIDER, target_model));
    opts
}

#[test]
fn gemini_same_model_echoes_function_call_thought_signature() {
    let frozen = gemini_frozen_config(MODEL_X);
    let rendered: Value =
        serde_json::from_slice(&Gemini.render(&frozen, &signed_call_request(MODEL_X))).unwrap();

    assert_eq!(
        rendered["contents"][1]["parts"][0]["thoughtSignature"],
        json!("gemini-signed-call")
    );
}

#[test]
fn gemini_cross_model_strips_function_call_thought_signature() {
    let frozen = gemini_frozen_config(MODEL_Y);
    let rendered: Value =
        serde_json::from_slice(&Gemini.render(&frozen, &signed_call_request(MODEL_Y))).unwrap();

    assert!(
        rendered["contents"][1]["parts"][0]
            .get("thoughtSignature")
            .is_none(),
        "a signature minted by a different model must not be replayed"
    );
    assert_eq!(
        rendered["contents"][1]["parts"][0]["functionCall"]["name"],
        json!("get_time")
    );
}

fn signed_reasoning_request(origin: Option<ModelHandle>, target_model: &str) -> CallOptions {
    let mut opts = CallOptions::new(vec![
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "continue".into(),
            }],
        ),
        Message {
            cache_prefix_blocks: None,
            role: Role::Assistant,
            content: vec![
                ContentBlock::Reasoning {
                    text: "unsigned reasoning".into(),
                    signature: None,
                },
                ContentBlock::Reasoning {
                    text: "signed reasoning".into(),
                    signature: Some("reasoning-sig".into()),
                },
                ContentBlock::Text {
                    text: "visible answer".into(),
                },
                ContentBlock::RedactedReasoning {
                    data: "redacted-sig".into(),
                    provider_format: None,
                },
                ContentBlock::ToolCall(ToolCall {
                    tool_call_id: ToolCallId("call_02".into()),
                    tool_name: "get_weather".into(),
                    input: json!({"city": "Madrid"}),
                    thought_signature: Some("call-sig".into()),
                }),
            ],
            origin,
            ordinal: None,
            tags: Default::default(),
        },
    ]);
    opts.target_model = Some(ModelHandle::new(PROVIDER, target_model));
    opts
}

#[test]
fn gemini_same_model_echoes_signed_reasoning_and_calls_in_source_order() {
    let frozen = gemini_frozen_config(MODEL_X);
    let opts = signed_reasoning_request(Some(ModelHandle::new(PROVIDER, MODEL_X)), MODEL_X);
    let rendered: Value = serde_json::from_slice(&Gemini.render(&frozen, &opts)).unwrap();

    assert_eq!(
        rendered["contents"][1]["parts"],
        json!([
            {
                "thought": true,
                "text": "signed reasoning",
                "thoughtSignature": "reasoning-sig"
            },
            {"text": "visible answer"},
            {
                "thought": true,
                "thoughtSignature": "redacted-sig"
            },
            {
                "functionCall": {
                    "name": "get_weather",
                    "args": {"city": "Madrid"}
                },
                "thoughtSignature": "call-sig"
            }
        ])
    );
}

#[test]
fn gemini_unsafe_origins_strip_reasoning_and_call_signatures() {
    let expected = json!([
        {"text": "visible answer"},
        {
            "functionCall": {
                "name": "get_weather",
                "args": {"city": "Madrid"}
            }
        }
    ]);
    let cases = [
        signed_reasoning_request(Some(ModelHandle::new(PROVIDER, MODEL_X)), MODEL_Y),
        signed_reasoning_request(None, MODEL_X),
    ];

    for opts in cases {
        let target = opts.target_model.as_ref().expect("target model");
        let frozen = gemini_frozen_config(&target.model_id);
        let rendered: Value = serde_json::from_slice(&Gemini.render(&frozen, &opts)).unwrap();
        assert_eq!(rendered["contents"][1]["parts"], expected);
    }
}

#[test]
fn gemini_render_strips_json_schema_meta_keys_from_tool_parameters() {
    let mut opts = fixed_request();
    let mut tools = opts.tools.as_ref().clone();
    tools[0].input_schema = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "timezone": {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "string"
            }
        },
        "required": ["timezone"]
    });
    opts.tools = tools.into();
    let frozen = gemini_frozen_config("gemini-2.0-flash");

    let rendered: Value = serde_json::from_slice(&Gemini.render(&frozen, &opts)).unwrap();
    let parameters = &rendered["tools"][0]["functionDeclarations"][0]["parameters"];

    assert!(!contains_object_key(parameters, "$schema"));
    assert!(
        contains_object_key(&opts.tools[0].input_schema, "$schema"),
        "rendering must not mutate the canonical tool schema"
    );
}
