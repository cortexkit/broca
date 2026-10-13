//! Golden-byte test for the Google Antigravity / Code-Assist render.
//!
//! Pins the exact rendered envelope bytes: the `cloudcode-pa` wrap around the Gemini body,
//! the frozen `project`, and the DETERMINISTIC `requestId` (a pure function of the fixed
//! `(run_id, step_index)` in the request). Regenerate intentionally with
//! `BROCA_UPDATE_GOLDEN=1` (which also requires a renderer_version bump for a real change).

use crate::render_golden_support;

use broca_protocol::{
    content::{ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult},
    request::{CallOptions, Tool, ToolChoice},
    StepContext, ToolCallId,
};
use broca_provider::families::{
    google_codeassist_frozen_config, GoogleCodeAssist, GOOGLE_CODEASSIST_RENDERER_VERSION,
};
use broca_provider::wire::WireFamily;
use serde_json::{json, Value};

fn fixed_request() -> CallOptions {
    let mut opts = CallOptions::new(vec![
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            role: Role::System,
            content: vec![ContentBlock::Text {
                text: "Be concise.".into(),
            }],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: "What time is it in Madrid?".into(),
            }],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
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
    opts.tools = vec![Tool {
        name: "get_time".into(),
        description: "Get the current time in an IANA timezone.".into(),
        input_schema: json!({
            "type": "object",
            "properties": { "timezone": { "type": "string" } },
            "required": ["timezone"]
        }),
        module: None,
        execution_mode: Default::default(),
    }]
    .into();
    opts.tool_choice = ToolChoice::Auto.into();
    // Fixed run/step identity → a fixed deterministic requestId in the golden.
    opts.step_context = Some(StepContext {
        run_id: "run-golden-0001".into(),
        step_index: 2,
    });
    opts
}

fn repeated_tool_call_id_request() -> CallOptions {
    let mut opts = CallOptions::new(vec![
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            role: Role::Assistant,
            content: vec![ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId("call_0".into()),
                tool_name: "search".into(),
                input: json!({"query": "broca"}),
            })],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            role: Role::Tool,
            content: vec![ContentBlock::ToolResult(ToolResult {
                tool_call_id: ToolCallId("call_0".into()),
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
            role: Role::Assistant,
            content: vec![ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId("call_0".into()),
                tool_name: "read".into(),
                input: json!({"path": "README.md"}),
            })],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            role: Role::Tool,
            content: vec![ContentBlock::ToolResult(ToolResult {
                tool_call_id: ToolCallId("call_0".into()),
                output: ToolOutput::Text {
                    text: "read-result".into(),
                },
                is_error: false,
            })],
        },
    ]);
    opts.step_context = Some(StepContext {
        run_id: "run-collision-0001".into(),
        step_index: 3,
    });
    opts
}

const GOLDEN: &str = include_str!("../fixtures/google_codeassist_render_golden.json");
const GOLDEN_VERSION: &str =
    include_str!("../fixtures/google_codeassist_render_golden.json.version");

#[test]
fn google_codeassist_render_matches_golden() {
    let mut frozen = google_codeassist_frozen_config("gemini-2.5-flash");
    frozen.project_id = Some("encouraging-env-qwp21".into());
    let bytes = GoogleCodeAssist.render(&frozen, &fixed_request());
    let rendered = String::from_utf8(bytes).expect("rendered bytes are UTF-8");

    if render_golden_support::update_if_requested(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/google_codeassist_render_golden.json"
        ),
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/google_codeassist_render_golden.json.version"
        ),
        rendered.as_bytes(),
        GOLDEN.as_bytes(),
        GOLDEN_VERSION,
        GOOGLE_CODEASSIST_RENDERER_VERSION,
        "Google Code Assist",
    ) {
        return;
    }

    render_golden_support::assert_matches_or_requires_bump(
        rendered.as_bytes(),
        GOLDEN.as_bytes(),
        GOLDEN_VERSION,
        GOOGLE_CODEASSIST_RENDERER_VERSION,
        "Google Code Assist",
    );
}

#[test]
fn google_codeassist_scopes_repeated_tool_call_ids_to_their_own_assistant_group() {
    let mut frozen = google_codeassist_frozen_config("gemini-2.5-flash");
    frozen.project_id = Some("encouraging-env-qwp21".into());
    let rendered: Value =
        serde_json::from_slice(&GoogleCodeAssist.render(&frozen, &repeated_tool_call_id_request()))
            .expect("CodeAssist render is valid JSON");
    let contents = &rendered["request"]["contents"];

    assert_eq!(
        [
            &contents[0]["parts"][0]["functionCall"]["name"],
            &contents[1]["parts"][0]["functionResponse"]["name"],
            &contents[2]["parts"][0]["functionCall"]["name"],
            &contents[3]["parts"][0]["functionResponse"]["name"],
        ],
        [
            &json!("search"),
            &json!("search"),
            &json!("read"),
            &json!("read"),
        ]
    );
}

#[test]
fn google_codeassist_inherits_object_wrapping_for_array_json_tool_results() {
    let mut request = fixed_request();
    let ContentBlock::ToolResult(result) = &mut request.prompt[3].content[0] else {
        panic!("fixed request must contain a tool result");
    };
    result.output = ToolOutput::Json {
        value: json!(["first", "second"]),
    };
    let mut frozen = google_codeassist_frozen_config("gemini-2.5-flash");
    frozen.project_id = Some("encouraging-env-qwp21".into());

    let rendered: Value = serde_json::from_slice(&GoogleCodeAssist.render(&frozen, &request))
        .expect("CodeAssist render is valid JSON");

    assert_eq!(
        rendered["request"]["contents"][2]["parts"][0]["functionResponse"]["response"],
        json!({"result": ["first", "second"]})
    );
}

#[test]
fn gemini_3_5_flash_tier_uses_its_budget_and_output_limit() {
    let mut frozen = google_codeassist_frozen_config("gemini-3.5-flash-low");
    frozen.project_id = Some("encouraging-env-qwp21".into());
    assert_eq!(frozen.caller_max_output_tokens, None);

    let bytes = GoogleCodeAssist.render(&frozen, &fixed_request());
    let rendered: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let generation = &rendered["request"]["generationConfig"];
    assert_eq!(generation["maxOutputTokens"], json!(65_536));
    assert_eq!(
        generation["thinkingConfig"],
        json!({ "thinkingBudget": 4000, "includeThoughts": true })
    );
    assert!(
        generation["thinkingConfig"].get("thinkingLevel").is_none(),
        "Gemini 3.5 Flash tiers use numeric budgets, not thinking levels"
    );
}

#[test]
fn gemini_3_5_flash_tier_preserves_a_smaller_explicit_output_limit() {
    let mut frozen = google_codeassist_frozen_config("gemini-3.5-flash-low");
    frozen.project_id = Some("encouraging-env-qwp21".into());
    frozen.max_output_tokens = 1;
    frozen.caller_max_output_tokens = Some(1);

    let rendered: serde_json::Value =
        serde_json::from_slice(&GoogleCodeAssist.render(&frozen, &fixed_request())).unwrap();
    assert_eq!(
        rendered["request"]["generationConfig"]["maxOutputTokens"],
        json!(1)
    );
}

#[test]
fn gemini_3_5_flash_tier_clamps_an_explicit_output_limit_to_its_ceiling() {
    let mut frozen = google_codeassist_frozen_config("gemini-3.5-flash-low");
    frozen.project_id = Some("encouraging-env-qwp21".into());
    frozen.max_output_tokens = 200_000;
    frozen.caller_max_output_tokens = Some(200_000);

    let rendered: serde_json::Value =
        serde_json::from_slice(&GoogleCodeAssist.render(&frozen, &fixed_request())).unwrap();
    assert_eq!(
        rendered["request"]["generationConfig"]["maxOutputTokens"],
        json!(65_536)
    );
}

#[test]
fn gemini_3_flash_tier_uses_thinking_level_without_a_budget() {
    let mut frozen = google_codeassist_frozen_config("gemini-3-flash-medium");
    frozen.project_id = Some("encouraging-env-qwp21".into());

    let bytes = GoogleCodeAssist.render(&frozen, &fixed_request());
    let rendered: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let generation = &rendered["request"]["generationConfig"];
    assert_eq!(generation["maxOutputTokens"], json!(2048));
    assert_eq!(
        generation["thinkingConfig"],
        json!({ "thinkingLevel": "medium", "includeThoughts": true })
    );
    assert!(generation["thinkingConfig"].get("thinkingBudget").is_none());
}

/// The requestId in the golden is deterministic: rendering the same fixed request twice
/// yields byte-identical bytes (the render-twice cache-stability gate).
#[test]
fn google_codeassist_render_is_deterministic() {
    let mut frozen = google_codeassist_frozen_config("gemini-2.5-flash");
    frozen.project_id = Some("encouraging-env-qwp21".into());
    let a = GoogleCodeAssist.render(&frozen, &fixed_request());
    let b = GoogleCodeAssist.render(&frozen, &fixed_request());
    assert_eq!(
        a, b,
        "render-twice must be byte-identical (deterministic requestId)"
    );
}
