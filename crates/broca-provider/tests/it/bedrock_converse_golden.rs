//! Golden-byte test for the AWS Bedrock ConverseStream render. Pins the UNSIGNED
//! JSON request body; SigV4 timestamps, credentials, and signatures are attached by
//! `endpoint` after render and must never enter this hash domain.

use crate::render_golden_support;

use broca_protocol::{
    content::{ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult},
    request::{CallOptions, Tool, ToolChoice},
    ToolCallId,
};
use broca_provider::families::{
    bedrock_frozen_config_with_region, earlier_renderer_versions, reproduces_renderer_version,
    BedrockConverse, EarlierRendererVersion, BEDROCK_RENDERER_VERSION,
};
use broca_provider::wire::{
    AuthHandle, CacheClass, CachePolicy, CacheTtl, MarkerSyntax, WireFamily,
};
use serde_json::json;

fn fixed_request() -> CallOptions {
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
                tool_call_id: ToolCallId("tooluse_01".into()),
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
                tool_call_id: ToolCallId("tooluse_01".into()),
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
    let mut generation = opts.generation.as_ref().clone();
    generation.temperature = Some(0.2);
    opts.generation = generation.into();
    opts
}

fn finalize_request() -> CallOptions {
    let mut opts = fixed_request();
    // Core marks its final answer turn with None, while the earlier tool exchange stays in history.
    opts.prompt.push(Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: "Finalize the response.".into(),
        }],
    ));
    opts.tool_choice = ToolChoice::None.into();
    opts
}

const GOLDEN: &str = include_str!("../fixtures/bedrock_converse_render_golden.json");
const GOLDEN_VERSION: &str =
    include_str!("../fixtures/bedrock_converse_render_golden.json.version");

#[test]
fn bedrock_converse_render_matches_golden_unsigned_body() {
    let frozen =
        bedrock_frozen_config_with_region("anthropic.claude-3-5-sonnet-20240620-v1:0", "us-west-2");
    let bytes = BedrockConverse.render(&frozen, &fixed_request());
    let rendered = String::from_utf8(bytes).expect("rendered bytes are UTF-8");

    if render_golden_support::update_if_requested(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/bedrock_converse_render_golden.json"
        ),
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/bedrock_converse_render_golden.json.version"
        ),
        rendered.as_bytes(),
        GOLDEN.as_bytes(),
        GOLDEN_VERSION,
        BEDROCK_RENDERER_VERSION,
        "Bedrock Converse",
    ) {
        return;
    }

    render_golden_support::assert_matches_or_requires_bump(
        rendered.as_bytes(),
        GOLDEN.as_bytes(),
        GOLDEN_VERSION,
        BEDROCK_RENDERER_VERSION,
        "Bedrock Converse",
    );
    assert!(!rendered.contains("Authorization"));
    assert!(!rendered.contains("x-amz-date"));
    assert!(!rendered.contains("AWS4-HMAC-SHA256"));
    assert!(!rendered.contains("modelId"));
}

const FINALIZE_GOLDEN: &str =
    include_str!("../fixtures/bedrock_converse_finalize_tool_history_render_golden.json");
const FINALIZE_GOLDEN_VERSION: &str =
    include_str!("../fixtures/bedrock_converse_finalize_tool_history_render_golden.json.version");

#[test]
fn bedrock_finalize_turn_keeps_tool_config_for_tool_history() {
    let frozen =
        bedrock_frozen_config_with_region("anthropic.claude-3-5-sonnet-20240620-v1:0", "us-west-2");
    let bytes = BedrockConverse.render(&frozen, &finalize_request());
    let rendered = String::from_utf8(bytes).expect("rendered bytes are UTF-8");

    if render_golden_support::update_if_requested(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/bedrock_converse_finalize_tool_history_render_golden.json"
        ),
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/bedrock_converse_finalize_tool_history_render_golden.json.version"
        ),
        rendered.as_bytes(),
        FINALIZE_GOLDEN.as_bytes(),
        FINALIZE_GOLDEN_VERSION,
        BEDROCK_RENDERER_VERSION,
        "Bedrock Converse finalize-with-tool-history",
    ) {
        return;
    }

    let body: serde_json::Value = serde_json::from_str(&rendered).unwrap();
    assert_eq!(body["toolConfig"]["toolChoice"], json!({"auto": {}}));
    assert_eq!(body["toolConfig"]["tools"].as_array().unwrap().len(), 2);
    let messages = body["messages"].as_array().unwrap();
    assert!(messages.iter().any(|message| {
        message["content"]
            .as_array()
            .unwrap()
            .iter()
            .any(|block| block.get("toolUse").is_some())
    }));
    assert!(messages.iter().any(|message| {
        message["content"]
            .as_array()
            .unwrap()
            .iter()
            .any(|block| block.get("toolResult").is_some())
    }));
    assert_eq!(
        messages.last().unwrap()["content"][0]["text"],
        "Finalize the response."
    );

    render_golden_support::assert_matches_or_requires_bump(
        rendered.as_bytes(),
        FINALIZE_GOLDEN.as_bytes(),
        FINALIZE_GOLDEN_VERSION,
        BEDROCK_RENDERER_VERSION,
        "Bedrock Converse finalize-with-tool-history",
    );
}

#[test]
fn bedrock_omits_tool_config_when_no_tools_are_available() {
    let frozen =
        bedrock_frozen_config_with_region("anthropic.claude-3-5-sonnet-20240620-v1:0", "us-west-2");
    let mut opts = CallOptions::new(vec![Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: "Hello.".into(),
        }],
    )]);
    opts.tool_choice = ToolChoice::None.into();

    let body: serde_json::Value =
        serde_json::from_slice(&BedrockConverse.render(&frozen, &opts)).unwrap();
    assert!(body.get("toolConfig").is_none());
}

#[test]
fn bedrock_finalize_renderer_versions_refuse_only_old_finalize_reproduction() {
    let family = broca_provider::spec::WireFamilyId::BedrockConverse;
    for version in [2, 3] {
        assert!(!reproduces_renderer_version(family, version));
        let reason = earlier_renderer_versions(family)
            .iter()
            .find_map(|(declared, disposition)| (*declared == version).then_some(disposition))
            .expect("every old Bedrock renderer version is declared");
        assert!(matches!(
            reason,
            EarlierRendererVersion::NotReproducible(text)
                if *text == "earlier versions sent no toolConfig on a finalize request whose history carries tool blocks"
        ));
    }

    // Auto turns with the same tool history keep their pre-bump bytes. The new bytes are specific
    // to a finalize choice, for which the earlier renderer versions are refused on resume.
    let frozen =
        bedrock_frozen_config_with_region("anthropic.claude-3-5-sonnet-20240620-v1:0", "us-west-2");
    for version in [2, 3] {
        let mut old_config = frozen.clone();
        old_config.renderer_version = version;
        assert_eq!(
            BedrockConverse.render(&old_config, &fixed_request()),
            GOLDEN.trim_end_matches('\n').as_bytes(),
        );
    }
}

#[test]
fn bedrock_endpoint_attaches_sigv4_headers_after_render() {
    let frozen =
        bedrock_frozen_config_with_region("anthropic.claude-3-5-sonnet-20240620-v1:0", "us-west-2");
    let body = BedrockConverse.render(&frozen, &fixed_request());
    let request = BedrockConverse.endpoint(
        &frozen,
        &AuthHandle::SigV4 {
            region: "us-west-2".into(),
            service: "bedrock-runtime".into(),
            signed_headers: vec![
                ("x-amz-date".into(), "20260102T030405Z".into()),
                (
                    "authorization".into(),
                    "AWS4-HMAC-SHA256 Credential=AKIA...".into(),
                ),
            ],
        },
        body.clone(),
    );

    assert_eq!(request.body, body);
    assert_eq!(request.url, "https://bedrock-runtime.us-west-2.amazonaws.com/model/anthropic.claude-3-5-sonnet-20240620-v1%3A0/converse-stream");
    assert!(request
        .headers
        .iter()
        .any(|(name, value)| name == "accept" && value == "application/vnd.amazon.eventstream"));
    assert!(request
        .headers
        .iter()
        .any(|(name, _)| name == "authorization"));
}

fn cache_marker_request() -> CallOptions {
    CallOptions::new(vec![
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::System,
            content: vec![
                ContentBlock::Text {
                    text: "System alpha.".into(),
                },
                ContentBlock::Text {
                    text: "System beta.".into(),
                },
            ],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: "m0 stable prefix".into(),
            }],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::Assistant,
            content: vec![ContentBlock::Text {
                text: "m1 stable answer".into(),
            }],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: "Earlier user boundary.".into(),
            }],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::Assistant,
            content: vec![ContentBlock::Text {
                text: "Earlier assistant answer.".into(),
            }],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: "Latest user boundary.".into(),
            }],
        },
    ])
}

fn bedrock_cache_policy() -> CachePolicy {
    CachePolicy {
        version: 1,
        class: CacheClass::Markers {
            syntax: MarkerSyntax::BedrockCachePoint,
            ttl: CacheTtl::OneHour,
            max_breakpoints: 4,
            lookback_blocks: 20,
            tail: None,
            omit_ttl: false,
        },
    }
}

const CACHE_MARKERS_GOLDEN: &str =
    include_str!("../fixtures/bedrock_converse_cache_markers_render_golden.json");
const CACHE_MARKERS_GOLDEN_VERSION: &str =
    include_str!("../fixtures/bedrock_converse_cache_markers_render_golden.json.version");

#[test]
fn bedrock_converse_cache_markers_render_matches_golden_unsigned_body() {
    let mut frozen =
        bedrock_frozen_config_with_region("anthropic.claude-3-5-sonnet-20240620-v1:0", "us-west-2");
    frozen.cache = Some(bedrock_cache_policy());
    let bytes = BedrockConverse.render(&frozen, &cache_marker_request());
    let rendered = String::from_utf8(bytes).expect("rendered bytes are UTF-8");

    if render_golden_support::update_if_requested(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/bedrock_converse_cache_markers_render_golden.json"
        ),
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/bedrock_converse_cache_markers_render_golden.json.version"
        ),
        rendered.as_bytes(),
        CACHE_MARKERS_GOLDEN.as_bytes(),
        CACHE_MARKERS_GOLDEN_VERSION,
        BEDROCK_RENDERER_VERSION,
        "Bedrock Converse cache-marker",
    ) {
        return;
    }

    render_golden_support::assert_matches_or_requires_bump(
        rendered.as_bytes(),
        CACHE_MARKERS_GOLDEN.as_bytes(),
        CACHE_MARKERS_GOLDEN_VERSION,
        BEDROCK_RENDERER_VERSION,
        "Bedrock Converse cache-marker",
    );
}
