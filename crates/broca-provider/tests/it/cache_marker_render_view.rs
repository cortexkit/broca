use broca_protocol::{
    content::{ContentBlock, Message, Role},
    request::{CallOptions, DeclaredMessageMarker, ModelHandle},
};
use broca_provider::families::{anthropic_frozen_config, bedrock_frozen_config_with_region};
use broca_provider::families::{AnthropicMessages, BedrockConverse};
use broca_provider::policy::{ReasoningPolicy, ThinkingEchoScope};
use broca_provider::wire::{
    CacheClass, CachePolicy, CacheTail, CacheTtl, FrozenRenderConfig, MarkerSyntax, WireFamily,
};
use serde_json::{json, Value};

fn msg(role: Role, content: Vec<ContentBlock>) -> Message {
    Message {
        role,
        content,
        cache_prefix_blocks: None,
        origin: None,
        ordinal: None,
        tags: Default::default(),
    }
}

fn assistant_from(model: &str, content: Vec<ContentBlock>) -> Message {
    Message {
        cache_prefix_blocks: None,
        role: Role::Assistant,
        content,
        origin: Some(ModelHandle::new("anthropic", model)),
        ordinal: None,
        tags: Default::default(),
    }
}

fn text(value: &str) -> ContentBlock {
    ContentBlock::Text { text: value.into() }
}

fn reasoning(value: &str) -> ContentBlock {
    ContentBlock::Reasoning {
        text: value.into(),
        signature: Some(format!("sig-{value}")),
    }
}

fn many_reasoning() -> Vec<ContentBlock> {
    (0..8).map(|idx| reasoning(&format!("r{idx}"))).collect()
}

fn many_text_blocks() -> Vec<ContentBlock> {
    (0..3).map(|idx| text(&format!("rendered-{idx}"))).collect()
}

fn marker_policy(syntax: MarkerSyntax, lookback_blocks: u32) -> CachePolicy {
    CachePolicy {
        version: 1,
        class: CacheClass::Markers {
            syntax,
            ttl: CacheTtl::OneHour,
            max_breakpoints: 4,
            lookback_blocks,
            tail: None,
            omit_ttl: false,
        },
    }
}

fn volatile_tail_policy(syntax: MarkerSyntax) -> CachePolicy {
    CachePolicy {
        version: 1,
        class: CacheClass::Markers {
            syntax,
            ttl: CacheTtl::OneHour,
            max_breakpoints: 4,
            lookback_blocks: 20,
            tail: Some(CacheTail::Volatile),
            omit_ttl: false,
        },
    }
}

/// A stable system block, an invariant user block, and a user block that differs every
/// request -- the prompt shape the tail declaration exists to describe.
fn split_prefix_and_tail_request() -> CallOptions {
    CallOptions::new(vec![
        msg(Role::System, vec![text("cacheable system tail")]),
        msg(Role::User, vec![text("invariant draft snapshot")]),
        msg(Role::User, vec![text("this round's fold")]),
    ])
}

fn marker_request(assistant_blocks: Vec<ContentBlock>) -> CallOptions {
    CallOptions::new(vec![
        msg(Role::System, vec![text("cacheable system tail")]),
        msg(Role::User, vec![text("m0")]),
        msg(Role::Assistant, vec![text("m1")]),
        msg(Role::User, vec![text("previous user")]),
        msg(Role::Assistant, assistant_blocks),
        msg(Role::User, vec![text("latest user")]),
    ])
}

fn anthropic_body(mut opts: CallOptions, lookback_blocks: u32) -> Value {
    let mut frozen = anthropic_frozen_config("claude-opus-4-8");
    frozen.cache = Some(marker_policy(
        MarkerSyntax::AnthropicCacheControl,
        lookback_blocks,
    ));
    opts.target_model = Some(ModelHandle::new("anthropic", "claude-opus-4-8"));
    AnthropicMessages.shape(&frozen, &opts)
}

fn bedrock_body(opts: &CallOptions, lookback_blocks: u32) -> Value {
    let mut frozen =
        bedrock_frozen_config_with_region("anthropic.claude-3-5-sonnet-20240620-v1:0", "us-west-2");
    frozen.cache = Some(marker_policy(
        MarkerSyntax::BedrockCachePoint,
        lookback_blocks,
    ));
    BedrockConverse.shape(&frozen, opts)
}

fn anthropic_block_has_marker(block: &Value) -> bool {
    block.get("cache_control").is_some()
}

fn anthropic_message_has_marker(body: &Value, index: usize) -> bool {
    body["messages"][index]["content"]
        .as_array()
        .expect("message content array")
        .iter()
        .any(anthropic_block_has_marker)
}

fn anthropic_system_tail_has_marker(body: &Value) -> bool {
    body["system"]
        .as_array()
        .expect("system array")
        .last()
        .is_some_and(anthropic_block_has_marker)
}

fn anthropic_marker_count(body: &Value) -> usize {
    fn count(value: &Value) -> usize {
        let here = value.get("cache_control").is_some() as usize;
        match value {
            Value::Array(values) => here + values.iter().map(count).sum::<usize>(),
            Value::Object(map) => here + map.values().map(count).sum::<usize>(),
            _ => here,
        }
    }
    count(body)
}

fn declared_marker_request() -> CallOptions {
    let mut opts = CallOptions::new(vec![
        msg(Role::System, vec![text("system one")]),
        msg(Role::System, vec![text("system two")]),
        msg(Role::User, vec![text("least stable")]),
        Message::assistant_from(
            vec![ContentBlock::RedactedReasoning {
                data: "foreign thought".into(),
                provider_format: None,
            }],
            ModelHandle::new("google", "gemini-3.5-flash-low"),
        ),
        msg(Role::User, vec![text("most stable")]),
        msg(Role::Assistant, vec![text("second most stable")]),
        msg(Role::User, vec![text("third most stable")]),
        msg(Role::Assistant, vec![text("fourth most stable")]),
        msg(Role::User, vec![text("tail")]),
    ]);
    opts.marker_order_version = Some(1);
    opts.target_model = Some(ModelHandle::new("anthropic", "claude-opus-4-8"));
    opts
}

fn anthropic_body_with_slots(opts: &CallOptions, max_breakpoints: u32) -> Value {
    let mut frozen = anthropic_frozen_config("claude-opus-4-8");
    frozen.cache = Some(CachePolicy {
        version: 1,
        class: CacheClass::Markers {
            syntax: MarkerSyntax::AnthropicCacheControl,
            ttl: CacheTtl::OneHour,
            max_breakpoints,
            lookback_blocks: 20,
            tail: None,
            omit_ttl: false,
        },
    });
    AnthropicMessages.shape(&frozen, opts)
}

fn anthropic_marked_message_indices(body: &Value) -> Vec<usize> {
    (0..body["messages"].as_array().unwrap().len())
        .filter(|&index| anthropic_message_has_marker(body, index))
        .collect()
}

#[test]
fn anthropic_declared_markers_remap_and_rank_under_slot_pressure() {
    let mut opts = declared_marker_request();
    opts.declared_markers = vec![
        DeclaredMessageMarker {
            message_index: 7,
            rank: 1,
        },
        DeclaredMessageMarker {
            message_index: 2,
            rank: 0,
        },
        DeclaredMessageMarker {
            message_index: 6,
            rank: 2,
        },
        DeclaredMessageMarker {
            message_index: 5,
            rank: 3,
        },
        DeclaredMessageMarker {
            message_index: 4,
            rank: u32::MAX,
        },
    ];

    for (slots, expected) in [(4, vec![1, 2, 3, 5]), (2, vec![1, 5])] {
        let body = anthropic_body_with_slots(&opts, slots);
        assert_eq!(body["messages"].as_array().unwrap().len(), 6);
        assert_eq!(body["messages"][1]["content"][0]["text"], "most stable");
        assert_eq!(body["messages"][5]["content"][0]["text"], "tail");
        assert_eq!(
            anthropic_marked_message_indices(&body),
            expected,
            "{slots} slots"
        );
        assert!(!anthropic_system_tail_has_marker(&body));
        assert_eq!(anthropic_marker_count(&body), slots as usize);
    }
}

#[test]
fn anthropic_declared_markers_skip_filtered_and_coalesced_messages() {
    let opts = declared_marker_request();
    let baseline = anthropic_body_with_slots(&opts, 2);
    let mut declared = opts;
    declared.declared_markers = vec![
        DeclaredMessageMarker {
            message_index: 1,
            rank: u32::MAX,
        },
        DeclaredMessageMarker {
            message_index: 3,
            rank: u32::MAX,
        },
        DeclaredMessageMarker {
            message_index: usize::MAX,
            rank: u32::MAX,
        },
    ];

    let body = anthropic_body_with_slots(&declared, 2);
    assert_eq!(body["system"].as_array().unwrap().len(), 2);
    assert_eq!(body["system"][1]["text"], "system one\nsystem two");
    assert_eq!(anthropic_marked_message_indices(&body), vec![5]);
    assert!(anthropic_system_tail_has_marker(&body));
    assert_eq!(
        serde_json::to_vec(&body).unwrap(),
        serde_json::to_vec(&baseline).unwrap()
    );
}

#[test]
fn anthropic_declared_markers_keep_declaration_order_for_equal_ranks() {
    let mut opts = declared_marker_request();
    opts.declared_markers = vec![
        DeclaredMessageMarker {
            message_index: 5,
            rank: 2,
        },
        DeclaredMessageMarker {
            message_index: 4,
            rank: 2,
        },
    ];

    let body = anthropic_body_with_slots(&opts, 2);
    assert_eq!(anthropic_marked_message_indices(&body), vec![2, 5]);
    assert_eq!(anthropic_marker_count(&body), 2);
}

#[test]
fn anthropic_legacy_marker_order_ignores_declared_markers() {
    for version in [None, Some(0), Some(2)] {
        let mut opts = declared_marker_request();
        opts.marker_order_version = version;
        let baseline = anthropic_body_with_slots(&opts, 2);
        opts.declared_markers = vec![DeclaredMessageMarker {
            message_index: 4,
            rank: 2,
        }];
        let body = anthropic_body_with_slots(&opts, 2);
        assert_eq!(
            serde_json::to_vec(&body).unwrap(),
            serde_json::to_vec(&baseline).unwrap()
        );
    }
}

#[test]
fn anthropic_no_declarations_preserves_version_one_bytes() {
    let mut opts = CallOptions::new(vec![
        msg(Role::System, vec![text("system")]),
        msg(Role::User, vec![text("question")]),
        msg(Role::Assistant, vec![text("answer")]),
        msg(Role::User, vec![text("tail")]),
    ]);
    opts.marker_order_version = Some(1);
    let bytes = serde_json::to_vec(&anthropic_body_with_slots(&opts, 4)).unwrap();
    assert_eq!(
        bytes,
        br#"{"max_tokens":2048,"messages":[{"content":[{"text":"question","type":"text"}],"role":"user"},{"content":[{"text":"answer","type":"text"}],"role":"assistant"},{"content":[{"cache_control":{"ttl":"1h","type":"ephemeral"},"text":"tail","type":"text"}],"role":"user"}],"model":"claude-opus-4-8","stream":true,"system":[{"text":"You are Claude Code, Anthropic's official CLI for Claude.","type":"text"},{"cache_control":{"ttl":"1h","type":"ephemeral"},"text":"system","type":"text"}],"thinking":{"budget_tokens":1024,"type":"enabled"}}"#
    );
}

fn bedrock_block_is_cache_point(block: &Value) -> bool {
    block.get("cachePoint").is_some()
}

fn bedrock_message_has_marker(body: &Value, index: usize) -> bool {
    body["messages"][index]["content"]
        .as_array()
        .expect("message content array")
        .iter()
        .any(bedrock_block_is_cache_point)
}

fn bedrock_system_has_marker(body: &Value) -> bool {
    body["system"]
        .as_array()
        .expect("system array")
        .iter()
        .any(bedrock_block_is_cache_point)
}

fn bedrock_cache_point_ttls(body: &Value) -> Vec<String> {
    fn collect(value: &Value, out: &mut Vec<String>) {
        if let Some(ttl) = value
            .get("cachePoint")
            .and_then(|cache_point| cache_point.get("ttl"))
            .and_then(Value::as_str)
        {
            out.push(ttl.to_string());
        }
        match value {
            Value::Array(values) => values.iter().for_each(|value| collect(value, out)),
            Value::Object(map) => map.values().for_each(|value| collect(value, out)),
            _ => {}
        }
    }

    let mut ttls = Vec::new();
    collect(body, &mut ttls);
    ttls
}

#[test]
fn anthropic_absent_block_target_preserves_legacy_multi_block_bytes() {
    let opts = CallOptions::new(vec![msg(
        Role::User,
        vec![text("stable head"), text("legacy tail")],
    )]);

    let bytes = serde_json::to_vec(&anthropic_body(opts, 20)).unwrap();
    assert_eq!(
        bytes,
        br#"{"max_tokens":2048,"messages":[{"content":[{"text":"stable head","type":"text"},{"cache_control":{"ttl":"1h","type":"ephemeral"},"text":"legacy tail","type":"text"}],"role":"user"}],"model":"claude-opus-4-8","stream":true,"system":[{"text":"You are Claude Code, Anthropic's official CLI for Claude.","type":"text"}],"thinking":{"budget_tokens":1024,"type":"enabled"}}"#
    );
}

#[test]
fn anthropic_stale_cache_prefix_count_falls_back_to_last_cacheable_block() {
    let mut message = msg(
        Role::User,
        vec![text("stable head"), text("remaining tail")],
    );
    message.cache_prefix_blocks = Some(3);

    let body = anthropic_body(CallOptions::new(vec![message]), 20);
    let blocks = body["messages"][0]["content"].as_array().unwrap();
    assert!(!anthropic_block_has_marker(&blocks[0]));
    assert!(anthropic_block_has_marker(&blocks[1]));
}

#[test]
fn bedrock_ignores_intra_message_cache_prefix_metadata() {
    let legacy = msg(Role::User, vec![text("stable head"), text("varying tail")]);
    let mut declared = legacy.clone();
    declared.cache_prefix_blocks = Some(1);

    let legacy_body = bedrock_body(&CallOptions::new(vec![legacy]), 20);
    let declared_body = bedrock_body(&CallOptions::new(vec![declared]), 20);
    assert_eq!(declared_body, legacy_body);
}

#[test]
fn anthropic_stripped_reasoning_does_not_inflate_bridge_distance() {
    let mut blocks = many_reasoning();
    blocks.push(text("visible answer"));
    let mut opts = marker_request(blocks);
    let assistant_content = opts.prompt[4].content.clone();
    opts.prompt[4] = assistant_from("claude-different", assistant_content);

    let body = anthropic_body(opts, 4);

    assert!(anthropic_system_tail_has_marker(&body));
    assert!(!anthropic_message_has_marker(&body, 2));
    assert_eq!(anthropic_marker_count(&body), 4);
}

#[test]
fn anthropic_rendered_text_distance_still_triggers_bridge() {
    let opts = marker_request(many_text_blocks());

    let body = anthropic_body(opts, 4);

    assert!(!anthropic_system_tail_has_marker(&body));
    assert!(anthropic_message_has_marker(&body, 2));
    assert_eq!(anthropic_marker_count(&body), 4);
}

#[test]
fn anthropic_empty_rendered_message_does_not_shift_marker_indices() {
    let dropped_old_index = 3;
    let opts = CallOptions::new(vec![
        msg(Role::System, vec![text("cacheable system tail")]),
        msg(Role::User, vec![text("m0")]),
        msg(Role::Assistant, vec![text("m1")]),
        msg(Role::User, vec![text("latest")]),
        Message::assistant_from(
            vec![ContentBlock::RedactedReasoning {
                data: "foreign-signature".into(),
                provider_format: None,
            }],
            ModelHandle::new("google", "gemini-3.5-flash-low"),
        ),
    ]);

    let body = anthropic_body(opts, 20);
    let messages = body["messages"].as_array().expect("messages array");

    assert_eq!(messages.len(), dropped_old_index);
    assert!(messages.iter().all(|message| {
        message["content"]
            .as_array()
            .is_some_and(|content| !content.is_empty())
    }));
    let marked_indices = (0..messages.len())
        .filter(|index| anthropic_message_has_marker(&body, *index))
        .collect::<Vec<_>>();
    assert!(marked_indices
        .iter()
        .all(|index| *index < dropped_old_index));
    assert_eq!(anthropic_marker_count(&body), 4);
}

/// A marker never lands on a thinking block. The request ends on a new user message, so only
/// the legacy last-assistant-message echo scope still renders the earlier thinking (the
/// tool-loop scope strips it as part of a finished turn); runs frozen under that scope keep
/// rendering this way on resume, so the placement rule is checked there.
#[test]
fn anthropic_leading_reasoning_keeps_marker_on_following_text() {
    let mut opts = CallOptions::new(vec![
        msg(Role::User, vec![text("m0")]),
        assistant_from(
            "claude-opus-4-8",
            vec![reasoning("private"), text("visible answer")],
        ),
        msg(Role::User, vec![text("latest")]),
    ]);

    let mut frozen = anthropic_frozen_config("claude-opus-4-8");
    frozen.cache = Some(marker_policy(MarkerSyntax::AnthropicCacheControl, 20));
    frozen.reasoning = ReasoningPolicy::AnthropicInterleaved {
        budget_tokens: 1024,
        last_turn_only: true,
        echo_scope: ThinkingEchoScope::LastAssistantMessage,
        cross_model: broca_provider::policy::CrossModelThinking::SameModelOnly,
    };
    opts.target_model = Some(ModelHandle::new("anthropic", "claude-opus-4-8"));
    let body = AnthropicMessages.shape(&frozen, &opts);
    let assistant_parts = body["messages"][1]["content"]
        .as_array()
        .expect("assistant content array");
    assert_eq!(assistant_parts[0]["type"], json!("thinking"));
    assert!(!anthropic_block_has_marker(&assistant_parts[0]));
    assert_eq!(assistant_parts[1]["type"], json!("text"));
    assert!(anthropic_block_has_marker(&assistant_parts[1]));
}

/// The keep-all counterpart: under the family default (every assistant message echoes its
/// thinking) the leading reasoning block is rendered here too -- the assistant is the only
/// assistant, so keep-all and the legacy last-assistant scope render it identically -- and the
/// marker must still land on the following text, never on the thinking block.
#[test]
fn anthropic_leading_reasoning_keeps_marker_on_following_text_under_keep_all() {
    let mut opts = CallOptions::new(vec![
        msg(Role::User, vec![text("m0")]),
        assistant_from(
            "claude-opus-4-8",
            vec![reasoning("private"), text("visible answer")],
        ),
        msg(Role::User, vec![text("latest")]),
    ]);

    let mut frozen = anthropic_frozen_config("claude-opus-4-8");
    frozen.cache = Some(marker_policy(MarkerSyntax::AnthropicCacheControl, 20));
    assert!(
        matches!(
            frozen.reasoning,
            ReasoningPolicy::AnthropicInterleaved {
                echo_scope: ThinkingEchoScope::AllAssistantMessages,
                ..
            }
        ),
        "the family default freezes the keep-all scope"
    );
    opts.target_model = Some(ModelHandle::new("anthropic", "claude-opus-4-8"));
    let body = AnthropicMessages.shape(&frozen, &opts);
    let assistant_parts = body["messages"][1]["content"]
        .as_array()
        .expect("assistant content array");
    assert_eq!(assistant_parts[0]["type"], json!("thinking"));
    assert!(!anthropic_block_has_marker(&assistant_parts[0]));
    assert_eq!(assistant_parts[1]["type"], json!("text"));
    assert!(anthropic_block_has_marker(&assistant_parts[1]));
}

#[test]
fn anthropic_volatile_tail_withholds_the_final_message_marker_across_a_resume() {
    let opts = split_prefix_and_tail_request();
    let mut frozen = anthropic_frozen_config("claude-opus-4-8");
    frozen.cache = Some(volatile_tail_policy(MarkerSyntax::AnthropicCacheControl));

    let body = AnthropicMessages.shape(&frozen, &opts);

    assert!(anthropic_system_tail_has_marker(&body));
    assert!(
        anthropic_message_has_marker(&body, 0),
        "the invariant block keeps its marker"
    );
    assert!(
        !anthropic_message_has_marker(&body, 1),
        "the declared-volatile final message is not anchored"
    );
    assert_eq!(anthropic_marker_count(&body), 2);

    // The declaration is read off the FROZEN config, never off the request, so a resume that
    // rehydrates that config renders the same bytes instead of re-deciding placement.
    let restored: FrozenRenderConfig =
        serde_json::from_slice(&serde_json::to_vec(&frozen).expect("serialize frozen config"))
            .expect("deserialize frozen config");
    assert_eq!(
        AnthropicMessages.shape(&restored, &opts),
        body,
        "a resumed render must reproduce the withheld anchor"
    );
}

#[test]
fn bedrock_volatile_tail_withholds_the_final_message_cache_point() {
    let opts = split_prefix_and_tail_request();
    let mut frozen =
        bedrock_frozen_config_with_region("anthropic.claude-3-5-sonnet-20240620-v1:0", "us-west-2");
    frozen.cache = Some(volatile_tail_policy(MarkerSyntax::BedrockCachePoint));

    let body = BedrockConverse.shape(&frozen, &opts);

    assert!(bedrock_system_has_marker(&body));
    assert!(bedrock_message_has_marker(&body, 0));
    assert!(
        !bedrock_message_has_marker(&body, 1),
        "the declaration is threaded through the Bedrock renderer unchanged"
    );
    assert_eq!(bedrock_cache_point_ttls(&body).len(), 2);
}

#[test]
fn bedrock_stripped_reasoning_does_not_inflate_bridge_distance() {
    let mut blocks = many_reasoning();
    blocks.push(text("visible answer"));
    let opts = marker_request(blocks);

    let body = bedrock_body(&opts, 4);

    assert!(bedrock_system_has_marker(&body));
    assert!(!bedrock_message_has_marker(&body, 2));
    assert_eq!(bedrock_cache_point_ttls(&body).len(), 4);
}

#[test]
fn bedrock_rendered_text_distance_still_triggers_bridge() {
    let opts = marker_request(many_text_blocks());

    let body = bedrock_body(&opts, 4);

    assert!(!bedrock_system_has_marker(&body));
    assert!(bedrock_message_has_marker(&body, 2));
    assert_eq!(bedrock_cache_point_ttls(&body).len(), 4);
}

#[test]
fn bedrock_empty_rendered_message_does_not_shift_marker_indices() {
    let dropped_old_index = 3;
    let opts = CallOptions::new(vec![
        msg(Role::System, vec![text("cacheable system tail")]),
        msg(Role::User, vec![text("m0")]),
        msg(Role::Assistant, vec![text("m1")]),
        msg(Role::User, vec![text("latest")]),
        Message::assistant_from(
            vec![ContentBlock::RedactedReasoning {
                data: "foreign-signature".into(),
                provider_format: None,
            }],
            ModelHandle::new("google", "gemini-3.5-flash-low"),
        ),
    ]);

    let body = bedrock_body(&opts, 20);
    let messages = body["messages"].as_array().expect("messages array");

    assert_eq!(messages.len(), dropped_old_index);
    assert!(messages.iter().all(|message| {
        message["content"]
            .as_array()
            .is_some_and(|content| !content.is_empty())
    }));
    let marked_indices = (0..messages.len())
        .filter(|index| bedrock_message_has_marker(&body, *index))
        .collect::<Vec<_>>();
    assert!(marked_indices
        .iter()
        .all(|index| *index < dropped_old_index));
    assert_eq!(bedrock_cache_point_ttls(&body).len(), 4);
}

#[test]
fn bedrock_passthrough_cache_points_are_stripped_before_planned_markers() {
    let mut opts = CallOptions::new(vec![msg(Role::User, vec![text("canonical")])]);
    opts.provider_options = json!({
        "system": [
            {"text": "passthrough system"},
            {"cachePoint": {"type": "default", "ttl": "5m"}}
        ],
        "messages": [
            {
                "role": "user",
                "content": [
                    {"text": "m0"},
                    {"cachePoint": {"type": "default", "ttl": "5m"}}
                ]
            },
            {
                "role": "assistant",
                "content": [
                    {"text": "m1", "cache_control": {"type": "ephemeral", "ttl": "5m"}}
                ]
            },
            {
                "role": "user",
                "content": [
                    {"text": "latest"},
                    {"cachePoint": {"type": "default", "ttl": "5m"}}
                ]
            }
        ]
    })
    .into();

    let body = bedrock_body(&opts, 20);
    let ttls = bedrock_cache_point_ttls(&body);

    assert_eq!(ttls, vec!["1h", "1h", "1h", "1h"]);
    assert!(body.to_string().contains("cachePoint"));
    assert!(!body.to_string().contains("cache_control"));
    assert!(!body.to_string().contains("cacheControl"));
}
