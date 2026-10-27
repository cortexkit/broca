//! Runs the AnthropicMessages family through the conformance harness — the first
//! family to pass the gate every future family (and every parallel author) must
//! pass. Feature-free: all four checks are pure (render + normalize, no transport).

use broca_protocol::{
    content::{ContentBlock, FinishReason, Message, Role, ToolCall},
    request::{CallOptions, Tool, ToolChoice},
    NormalizedEvent, ProviderControl, StepId, ToolCallId, Usage,
};
use broca_provider::conformance::{
    check_freeze_against_mutation, check_render_determinism, normalize_recorded,
};
use broca_provider::families::{anthropic_frozen_config, resolve_frozen, AnthropicMessages};
use broca_provider::policy::{
    Quirks, ReasoningPolicy, SystemPromptPolicy, ThinkingEchoScope, UsagePolicy,
};
use broca_provider::spec::{
    AuthKind, Capabilities, CapabilityFlag, CostSchedule, Limits, ModelSpec, Overlay, ProviderSpec,
    WireFamilyId,
};
use broca_provider::wire::{CacheClass, CachePolicy, CacheTtl, MarkerSyntax, WireFamily};
use serde_json::json;

/// A request that exercises many tools (so a HashMap-ordered tool array, if any
/// family had one, would surface), reasoning echo, and a tool call.
fn rich_request() -> CallOptions {
    let mut opts = CallOptions::new(vec![
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::System,
            content: vec![ContentBlock::Text {
                text: "Sys.".into(),
            }],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text { text: "Hi".into() }],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::Assistant,
            content: vec![
                ContentBlock::Reasoning {
                    text: "thinking".into(),
                    signature: Some("sig-z".into()),
                },
                ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId("t1".into()),
                    tool_name: "a_tool".into(),
                    input: json!({"x": 1}),
                }),
            ],
        },
    ]);
    // Many tools — a deterministic array regardless of any internal map use.
    opts.tools = (0..12)
        .map(|i| Tool { name: format!("tool_{i:02}"), description: format!("tool number {i}"), input_schema: json!({"type": "object", "properties": {format!("p{i}"): {"type": "string"}}}), module: None, execution_mode: Default::default(), reply: None, })
        .collect::<Vec<_>>()
        .into();
    opts.tool_choice = ToolChoice::Auto.into();
    opts
}

#[test]
fn anthropic_passes_render_determinism() {
    let frozen = anthropic_frozen_config("claude-opus-4-8");
    let report = check_render_determinism(&AnthropicMessages, &frozen, &rich_request());
    assert!(
        report.passed(),
        "conformance failures: {:?}",
        report.failures()
    );
}

/// An Anthropic ProviderSpec + ModelSpec for the freeze check.
fn anthropic_spec() -> (ProviderSpec, ModelSpec) {
    let provider = ProviderSpec {
        id: "anthropic".into(),
        wire_family: WireFamilyId::AnthropicMessages,
        base_url: "https://api.anthropic.com".into(),
        auth_kind: AuthKind::OAuth,
        static_headers: Default::default(),
        static_options: serde_json::Value::Null,
        quirks: Quirks {
            reasoning: ReasoningPolicy::AnthropicInterleaved {
                budget_tokens: 1024,
                last_turn_only: true,
                echo_scope: ThinkingEchoScope::AllAssistantMessages,
                cross_model: broca_provider::policy::CrossModelThinking::AnyAnthropicModel,
            },
            tool_id: Default::default(),
            system_prompt: SystemPromptPolicy::RequiredIdentityLead {
                identity: "You are Claude Code, Anthropic's official CLI for Claude.".into(),
            },
            usage: UsagePolicy::AnthropicUsage,
            token_limit_field: Default::default(),
            reasoning_echo: None,
            tool_call_content_empty_string: false,
            media: None,
        },
        raw: serde_json::Value::Null,
    };
    let model = ModelSpec {
        id: "claude-opus-4-8".into(),
        display_name: "Claude Opus".into(),
        family: Some("claude".into()),
        capabilities: Capabilities {
            reasoning: CapabilityFlag::Known(true),
            tool_call: CapabilityFlag::Known(true),
            interleaved: CapabilityFlag::Known(true),
            ..Default::default()
        },
        limits: Limits::default(),
        cost: CostSchedule::default(),
        reasoning_options: Default::default(),
        input_modalities: Default::default(),
        release_date: None,
        status: None,
        overlay: Overlay::default(),
        raw: serde_json::Value::Null,
    };
    (provider, model)
}

#[test]
fn anthropic_freeze_survives_catalog_mutation() {
    let (provider, model) = anthropic_spec();
    let mut spec = (provider, model);

    let result = check_freeze_against_mutation(
        &AnthropicMessages,
        &mut spec,
        &rich_request(),
        // resolve: build the frozen config from the CURRENT spec.
        |(p, m)| resolve_frozen(p, m, 2048, 1).expect("test specs publish known capability flags"),
        // mutate: change render-affecting spec fields AFTER the freeze — a leaked
        // input would change the resumed bytes.
        |(p, m)| {
            m.id = "claude-something-NEW".into();
            p.quirks.reasoning = ReasoningPolicy::AnthropicInterleaved {
                budget_tokens: 9999,
                last_turn_only: true,
                echo_scope: ThinkingEchoScope::LastAssistantMessage,
                cross_model: broca_provider::policy::CrossModelThinking::SameModelOnly,
            };
            p.static_options = json!({"injected": "after-freeze"});
        },
    );
    assert!(result.is_ok(), "{}", result.unwrap_err());
}

fn cache_policy_request() -> CallOptions {
    CallOptions::new(vec![
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::System,
            content: vec![
                ContentBlock::Text {
                    text: "Cache system A.".into(),
                },
                ContentBlock::Text {
                    text: "Cache system B.".into(),
                },
            ],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text { text: "m0".into() }],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::Assistant,
            content: vec![ContentBlock::Text { text: "m1".into() }],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: "previous".into(),
            }],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::Assistant,
            content: vec![ContentBlock::Text {
                text: "answer".into(),
            }],
        },
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: "latest".into(),
            }],
        },
    ])
}

fn anthropic_marker_cache_policy() -> CachePolicy {
    CachePolicy {
        version: 1,
        class: CacheClass::Markers {
            syntax: MarkerSyntax::AnthropicCacheControl,
            ttl: CacheTtl::OneHour,
            max_breakpoints: 4,
            lookback_blocks: 20,
            tail: None,
            omit_ttl: false,
        },
    }
}

fn anthropic_frozen_with_cache_policy() -> broca_provider::wire::FrozenRenderConfig {
    let mut frozen = anthropic_frozen_config("claude-opus-4-8");
    frozen.cache = Some(anthropic_marker_cache_policy());
    frozen
}

#[test]
fn anthropic_cache_policy_passes_render_determinism() {
    let frozen = anthropic_frozen_with_cache_policy();
    let report = check_render_determinism(&AnthropicMessages, &frozen, &cache_policy_request());
    assert!(
        report.passed(),
        "conformance failures: {:?}",
        report.failures()
    );
}

#[test]
fn anthropic_cache_policy_freeze_survives_catalog_mutation() {
    let mut spec = anthropic_spec();
    let result = check_freeze_against_mutation(
        &AnthropicMessages,
        &mut spec,
        &cache_policy_request(),
        |(p, m)| {
            let mut frozen =
                resolve_frozen(p, m, 2048, 1).expect("test specs publish known capability flags");
            frozen.cache = Some(anthropic_marker_cache_policy());
            frozen
        },
        |(p, m)| {
            m.id = "claude-cache-mutated".into();
            p.static_options = json!({"after": "freeze"});
        },
    );
    assert!(result.is_ok(), "{}", result.unwrap_err());
}

#[test]
fn anthropic_cache_policy_resume_blob_rerenders_identically() {
    let frozen = anthropic_frozen_with_cache_policy();
    let opts = cache_policy_request();
    let first = AnthropicMessages.render(&frozen, &opts);
    let blob = serde_json::to_vec(&frozen).expect("serialize frozen config");
    let restored: broca_provider::wire::FrozenRenderConfig =
        serde_json::from_slice(&blob).expect("deserialize frozen config");
    let second = AnthropicMessages.render(&restored, &opts);
    assert_eq!(first, second);
}

#[test]
fn anthropic_normalizer_against_recorded_stream() {
    // A minimal recorded Anthropic SSE stream: a text delta + a final message.
    let recorded = concat!(
        "event: message_start\n",
        "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"usage\":{\"input_tokens\":10,\"output_tokens\":0}}}\n\n",
        "event: content_block_start\n",
        "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
        "event: content_block_delta\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello\"}}\n\n",
        "event: content_block_stop\n",
        "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
        "event: message_delta\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":5}}\n\n",
        "event: message_stop\n",
        "data: {\"type\":\"message_stop\"}\n\n",
    );
    let frozen = anthropic_frozen_config("claude-opus-4-8");

    // Chunk size 7 deliberately splits SSE records mid-line to exercise the
    // normalizer's incremental framing.
    let events = normalize_recorded(
        &AnthropicMessages,
        &frozen,
        StepId(1),
        recorded.as_bytes(),
        7,
    )
    .expect("normalize ok");

    // The assembled control units must include an AssistantMessage with the text
    // and a terminal StepFinished/Finished — the normalizer reconstructed the turn.
    let has_assistant_text = events.iter().any(|e| match e {
        NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => message
            .content
            .iter()
            .any(|b| matches!(b, ContentBlock::Text { text } if text == "Hello")),
        _ => false,
    });
    assert!(
        has_assistant_text,
        "expected an assembled AssistantMessage with text 'Hello'; got {events:?}"
    );

    let has_terminal = events.iter().any(|e| {
        matches!(
            e,
            NormalizedEvent::Control(ProviderControl::StepFinished { .. })
                | NormalizedEvent::Control(ProviderControl::Finished { .. })
        )
    });
    assert!(
        has_terminal,
        "expected a terminal StepFinished/Finished; got {events:?}"
    );
}

/// Anthropic ends a response it declined to write with `stop_reason: "refusal"`. That is a
/// safety decision, not a natural end, so the step reports it as `content_filter`, which is
/// how the OpenAI families report a refused answer. Read as `stop`, a refusal would look like
/// a finished answer to a caller deciding whether to retry or escalate. The `end_turn` row
/// shows that the test reads the stream's own stop reason, not a constant.
#[test]
fn anthropic_refusal_stop_reason_finishes_the_step_as_content_filter() {
    for (stop_reason, expected) in [
        ("refusal", FinishReason::ContentFilter),
        ("end_turn", FinishReason::Stop),
    ] {
        let recorded = format!(
            "event: message_start\n\
             data: {{\"type\":\"message_start\",\"message\":{{\"id\":\"msg_r\",\"usage\":{{\"input_tokens\":9,\"output_tokens\":0}}}}}}\n\n\
             event: message_delta\n\
             data: {{\"type\":\"message_delta\",\"delta\":{{\"stop_reason\":\"{stop_reason}\"}},\"usage\":{{\"output_tokens\":0}}}}\n\n\
             event: message_stop\n\
             data: {{\"type\":\"message_stop\"}}\n\n",
        );
        let frozen = anthropic_frozen_config("claude-opus-4-8");
        let events = normalize_recorded(
            &AnthropicMessages,
            &frozen,
            StepId(1),
            recorded.as_bytes(),
            7,
        )
        .expect("normalize ok");
        let finish_reason = events
            .iter()
            .find_map(|event| match event {
                NormalizedEvent::Control(ProviderControl::StepFinished {
                    finish_reason, ..
                }) => Some(*finish_reason),
                _ => None,
            })
            .expect("the stream finishes the step");
        assert_eq!(finish_reason, expected, "stop_reason {stop_reason:?}");
    }
}

/// Drive a single Anthropic `message_delta` usage object through the normalizer and
/// return the resulting step usage. The stream opens with a `message_start` carrying
/// input tokens, then a `message_delta` carrying the terminal `usage` we want to test.
fn normalize_anthropic_usage(usage: serde_json::Value) -> Usage {
    let recorded = format!(
        "event: message_start\n\
         data: {{\"type\":\"message_start\",\"message\":{{\"id\":\"msg_u\",\"usage\":{{\"input_tokens\":12,\"output_tokens\":0}}}}}}\n\n\
         event: content_block_start\n\
         data: {{\"type\":\"content_block_start\",\"index\":0,\"content_block\":{{\"type\":\"text\",\"text\":\"\"}}}}\n\n\
         event: content_block_delta\n\
         data: {{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{{\"type\":\"text_delta\",\"text\":\"ok\"}}}}\n\n\
         event: content_block_stop\n\
         data: {{\"type\":\"content_block_stop\",\"index\":0}}\n\n\
         event: message_delta\n\
         data: {{\"type\":\"message_delta\",\"delta\":{{\"stop_reason\":\"end_turn\"}},\"usage\":{usage}}}\n\n\
         event: message_stop\n\
         data: {{\"type\":\"message_stop\"}}\n\n",
    );
    let frozen = anthropic_frozen_config("claude-opus-4-8");
    let events = normalize_recorded(
        &AnthropicMessages,
        &frozen,
        StepId(1),
        recorded.as_bytes(),
        7,
    )
    .expect("normalize ok");
    events
        .into_iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::StepFinished { usage, .. }) => Some(usage),
            _ => None,
        })
        .expect("step usage")
}

/// Newer Anthropic API responses carry `output_tokens_details.thinking_tokens` — the
/// reasoning subset of output (subset semantics, like OpenAI's reasoning split). The
/// normalizer surfaces it as `usage.reasoning_tokens` WITHOUT subtracting from
/// `output_tokens` (the provider's billing total) and WITHOUT touching `fill_tokens`
/// (reasoning is output-side, never request-side input). Absent on older responses
/// leaves the default None. We read only the provider-reported field; we never synthesize
/// it from thinking-block sizes.
#[test]
fn anthropic_usage_maps_thinking_tokens_detail_without_touching_output_or_fill() {
    for (wire, reasoning, output) in [
        // thinking_tokens present: surfaced; output_tokens stays the provider total.
        (
            json!({"output_tokens": 40, "output_tokens_details": {"thinking_tokens": 17}}),
            Some(17u64),
            40u64,
        ),
        (
            json!({"output_tokens": 0, "output_tokens_details": {"thinking_tokens": 0}}),
            Some(0),
            0,
        ),
        // No details object (older response): reasoning stays absent.
        (json!({"output_tokens": 40}), None, 40),
        // Details present but thinking_tokens absent: reasoning stays absent.
        (
            json!({"output_tokens": 40, "output_tokens_details": {"some_other_field": 5}}),
            None,
            40,
        ),
    ] {
        let usage = normalize_anthropic_usage(wire);
        assert_eq!(usage.reasoning_tokens, reasoning, "reasoning split");
        assert_eq!(
            usage.output_tokens,
            Some(output),
            "output_tokens is the provider total, unchanged"
        );
        // Reasoning is output-side; it must never count toward the request-side fill.
        // input_tokens is 12 (from message_start), no cache-read, so fill = 12.
        assert_eq!(
            usage.fill_tokens(),
            Some(12),
            "fill is input + cache-read only"
        );
    }
}
