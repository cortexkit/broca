//! Golden-byte test for the Anthropic request renderer.
//!
//! `render_request` is the cache/resume hash domain: the bytes it produces must be
//! byte-identical across runs (and across refactors of the renderer). This test
//! pins the rendered bytes for a fixed, representative request to a committed
//! golden file. A change to the rendered bytes — intended or not — fails this test;
//! an INTENTIONAL change requires updating the golden AND bumping
//! `ANTHROPIC_RENDERER_VERSION` (a renderer change means a resume can't reproduce
//! the original request).
//!
//! `render_request` is pure (no network), so it is driven here with a dummy token.

// A file-level `cfg` DELETES this file when the feature is off, and a deleted file reports
// "running 0 tests ... test result: ok" -- indistinguishable at a glance from a suite that
// ran and passed, and impossible to instrument because no code survives to carry a notice.
// Gating a module instead leaves this shell compiled, so the absence can announce itself.
#[cfg(not(feature = "live"))]
#[test]
fn this_suite_needs_its_feature_enabled() {
    eprintln!(
        "SKIP anthropic_render_golden: compiled without its feature, so none of its cases exist. \
         This reports `ok` -- re-run with `-F broca-provider/live` to execute them."
    );
}

#[cfg(feature = "live")]
mod gated {

    use crate::render_golden_support;
    use broca_core::provider::Provider;

    use broca_protocol::{
        content::{ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult},
        request::{CallOptions, Tool, ToolChoice},
        ToolCallId,
    };
    use broca_provider::families::{
        anthropic_frozen_config, AnthropicMessages, ANTHROPIC_RENDERER_VERSION,
    };
    use broca_provider::live::LiveAnthropicProvider;
    use broca_provider::wire::{CacheClass, CachePolicy, CacheTtl, MarkerSyntax, WireFamily};
    use serde_json::json;

    /// A fixed, representative request that exercises the load-bearing render paths:
    /// a system message, a user turn, an assistant turn with signed reasoning + a tool
    /// call (the LAST assistant turn), an older assistant turn also carrying signed
    /// reasoning (echoed too under the keep-all default), a tool-result turn, and two
    /// tools with a forced tool choice.
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
            // An OLDER assistant turn carrying signed reasoning. Under the keep-all default
            // its reasoning is echoed back like every other assistant message's; only a
            // cross-model signature (a different producing model) would be stripped.
            Message {
                cache_prefix_blocks: None,
                origin: None,
                ordinal: None,
                role: Role::Assistant,
                content: vec![
                    ContentBlock::Reasoning {
                        text: "older thinking".into(),
                        signature: Some("old-sig".into()),
                    },
                    ContentBlock::Text {
                        text: "Let me check.".into(),
                    },
                ],
            },
            Message {
                cache_prefix_blocks: None,
                origin: None,
                ordinal: None,
                role: Role::User,
                content: vec![ContentBlock::Text {
                    text: "Please.".into(),
                }],
            },
            // The LAST assistant turn: signed reasoning (echoed byte-verbatim) + a
            // tool call.
            Message {
                cache_prefix_blocks: None,
                origin: None,
                ordinal: None,
                role: Role::Assistant,
                content: vec![
                    ContentBlock::Reasoning {
                        text: "I should call get_time for Europe/Madrid.".into(),
                        signature: Some("sig-abc123".into()),
                    },
                    ContentBlock::ToolCall(ToolCall {
                        thought_signature: None,
                        tool_call_id: ToolCallId("toolu_01".into()),
                        tool_name: "get_time".into(),
                        input: json!({"timezone": "Europe/Madrid"}),
                    }),
                ],
            },
            // The tool result turn.
            Message {
                cache_prefix_blocks: None,
                origin: None,
                ordinal: None,
                role: Role::Tool,
                content: vec![ContentBlock::ToolResult(ToolResult {
                    tool_call_id: ToolCallId("toolu_01".into()),
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
            },
        ]
        .into();
        opts.tool_choice = ToolChoice::Auto.into();
        opts
    }

    /// The provider under test, with a dummy token (render_request never touches the
    /// network) and pinned max_tokens / thinking_budget so the bytes are fixed.
    fn provider() -> LiveAnthropicProvider {
        LiveAnthropicProvider::new("claude-opus-4-8", "dummy-token-not-used").with_max_tokens(2048)
    }

    const GOLDEN: &str = include_str!("../fixtures/anthropic_render_golden.json");
    const GOLDEN_VERSION: &str = include_str!("../fixtures/anthropic_render_golden.json.version");

    #[test]
    fn anthropic_render_matches_golden() {
        let bytes = provider().render_request(&fixed_request());
        let rendered = String::from_utf8(bytes).expect("rendered bytes are UTF-8");

        if render_golden_support::update_if_requested(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/anthropic_render_golden.json"
            ),
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/anthropic_render_golden.json.version"
            ),
            rendered.as_bytes(),
            GOLDEN.as_bytes(),
            GOLDEN_VERSION,
            ANTHROPIC_RENDERER_VERSION,
            "Anthropic",
        ) {
            return;
        }

        render_golden_support::assert_matches_or_requires_bump(
            rendered.as_bytes(),
            GOLDEN.as_bytes(),
            GOLDEN_VERSION,
            ANTHROPIC_RENDERER_VERSION,
            "Anthropic",
        );
    }

    fn cache_marker_request() -> CallOptions {
        CallOptions::new(vec![
            Message {
                cache_prefix_blocks: None,
                origin: None,
                ordinal: None,
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
                role: Role::User,
                content: vec![ContentBlock::Text {
                    text: "m0 stable prefix".into(),
                }],
            },
            Message {
                cache_prefix_blocks: None,
                origin: None,
                ordinal: None,
                role: Role::Assistant,
                content: vec![ContentBlock::Text {
                    text: "m1 stable answer".into(),
                }],
            },
            Message {
                cache_prefix_blocks: None,
                origin: None,
                ordinal: None,
                role: Role::User,
                content: vec![ContentBlock::Text {
                    text: "Earlier user boundary.".into(),
                }],
            },
            Message {
                cache_prefix_blocks: None,
                origin: None,
                ordinal: None,
                role: Role::Assistant,
                content: vec![ContentBlock::Text {
                    text: "Earlier assistant answer.".into(),
                }],
            },
            Message {
                cache_prefix_blocks: None,
                origin: None,
                ordinal: None,
                role: Role::User,
                content: vec![ContentBlock::Text {
                    text: "Latest user boundary.".into(),
                }],
            },
        ])
    }

    #[test]
    fn tail_first_render_matches_golden_and_resumes_byte_identically() {
        let mut frozen = anthropic_frozen_config("claude-opus-4-8");
        frozen.cache = Some(anthropic_cache_policy());
        let mut opts = cache_marker_request();
        opts.marker_order_version = Some(1);
        let bytes = AnthropicMessages.render(&frozen, &opts);
        assert_eq!(
            bytes,
            include_str!("../fixtures/anthropic_tail_first_render_golden.json")
                .trim_end()
                .as_bytes()
        );
        for version in [None, Some(1)] {
            opts.marker_order_version = version;
            let start: broca_wal::WalRecord = serde_json::from_value(json!({
                "type": "run_started", "run_id": "r1",
                "session": {"project_root": "/tmp/p", "harness": "h", "session": "s"},
                "marker_order_version": version,
                "config": {
                    "model": {"provider_module_id": "anthropic", "model_id": "claude-opus-4-8"},
                    "tools": [], "tool_choice": {"type": "auto"}, "generation": {},
                    "response_format": {"type": "text"}, "context_epoch": 0, "renderer_version": 1
                },
                "input": opts.prompt, "origin": {"kind": "fresh"}
            }))
            .unwrap();
            let persisted = serde_json::to_vec(&start).unwrap();
            let decoded = serde_json::from_slice(&persisted).unwrap();
            let replay = broca_core::replay::replay(&[decoded]).unwrap();
            let mut resumed = CallOptions::new(replay.prompt);
            resumed.marker_order_version = replay.marker_order_version;
            assert_eq!(
                AnthropicMessages.render(&frozen, &resumed),
                AnthropicMessages.render(&frozen, &opts)
            );
        }
    }

    fn anthropic_cache_policy() -> CachePolicy {
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

    const CACHE_MARKERS_GOLDEN: &str =
        include_str!("../fixtures/anthropic_cache_markers_render_golden.json");
    const CACHE_MARKERS_GOLDEN_VERSION: &str =
        include_str!("../fixtures/anthropic_cache_markers_render_golden.json.version");

    #[test]
    fn anthropic_cache_markers_render_matches_golden() {
        let mut frozen = anthropic_frozen_config("claude-opus-4-8");
        frozen.cache = Some(anthropic_cache_policy());
        let bytes = AnthropicMessages.render(&frozen, &cache_marker_request());
        let rendered = String::from_utf8(bytes).expect("rendered bytes are UTF-8");

        if render_golden_support::update_if_requested(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/anthropic_cache_markers_render_golden.json"
            ),
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/anthropic_cache_markers_render_golden.json.version"
            ),
            rendered.as_bytes(),
            CACHE_MARKERS_GOLDEN.as_bytes(),
            CACHE_MARKERS_GOLDEN_VERSION,
            ANTHROPIC_RENDERER_VERSION,
            "Anthropic cache-marker",
        ) {
            return;
        }

        render_golden_support::assert_matches_or_requires_bump(
            rendered.as_bytes(),
            CACHE_MARKERS_GOLDEN.as_bytes(),
            CACHE_MARKERS_GOLDEN_VERSION,
            ANTHROPIC_RENDERER_VERSION,
            "Anthropic cache-marker",
        );
    }

    /// A render config exactly as a run admitted by renderer version 2 recorded it, before the
    /// thinking-echo scope existed. A resumed run renders from this blob, so its bytes must never
    /// move: the golden below pins them.
    const LEGACY_ECHO_FROZEN_CONFIG: &str = r#"{
        "family_id": "anthropic_messages",
        "renderer_version": 2,
        "wire_model_id": "claude-opus-4-8",
        "max_output_tokens": 2048,
        "reasoning": {"mode": "anthropic_interleaved", "budget_tokens": 1024, "last_turn_only": true},
        "tool_id": "passthrough",
        "system_prompt": {
            "mode": "required_identity_lead",
            "identity": "You are Claude Code, Anthropic's official CLI for Claude."
        },
        "usage": "anthropic_usage",
        "cache": {
            "version": 1, "class": "markers", "syntax": "anthropic_cache_control",
            "ttl": "1h", "max_breakpoints": 4, "lookback_blocks": 20
        },
        "token_limit_field": "max_tokens"
    }"#;

    fn assistant_step(n: u32, thinking: &str) -> Message {
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            role: Role::Assistant,
            content: vec![
                ContentBlock::Reasoning {
                    text: thinking.into(),
                    signature: Some(format!("sig-step-{n}")),
                },
                ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId(format!("toolu_step_{n}")),
                    tool_name: "tick".into(),
                    input: json!({ "n": n }),
                }),
            ],
        }
    }

    fn tool_step(n: u32) -> Message {
        Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            role: Role::Tool,
            content: vec![ContentBlock::ToolResult(ToolResult {
                tool_call_id: ToolCallId(format!("toolu_step_{n}")),
                output: ToolOutput::Text {
                    text: format!("tick {n}"),
                },
                is_error: false,
            })],
        }
    }

    /// A finished earlier turn (its thinking must always be stripped), then a tool loop of
    /// three steps where every assistant message reasons and calls a tool. This is the
    /// fourth request of the loop, answering the third tool call.
    fn tool_loop_request() -> CallOptions {
        let text = |role: Role, text: &str| Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            role,
            content: vec![ContentBlock::Text { text: text.into() }],
        };
        let mut earlier_answer = text(Role::Assistant, "Earlier answer.");
        earlier_answer.content.insert(
            0,
            ContentBlock::Reasoning {
                text: "earlier turn thinking".into(),
                signature: Some("sig-earlier".into()),
            },
        );
        let mut opts = CallOptions::new(vec![
            text(Role::System, "Be concise."),
            text(Role::User, "Earlier question."),
            earlier_answer,
            text(Role::User, "Call tick three times."),
            assistant_step(1, "first tick"),
            tool_step(1),
            assistant_step(2, "second tick"),
            tool_step(2),
            assistant_step(3, "third tick"),
            tool_step(3),
        ]);
        opts.tools = vec![Tool {
            name: "tick".into(),
            description: "Return the tick number.".into(),
            input_schema: json!({
                "type": "object",
                "properties": { "n": { "type": "integer" } },
                "required": ["n"]
            }),
            module: None,
            execution_mode: Default::default(),
        }]
        .into();
        opts.tool_choice = ToolChoice::Auto.into();
        opts
    }

    const LEGACY_ECHO_TOOL_LOOP_GOLDEN: &str =
        include_str!("../fixtures/anthropic_legacy_echo_tool_loop_render_golden.json");
    const LEGACY_ECHO_TOOL_LOOP_GOLDEN_VERSION: &str =
        include_str!("../fixtures/anthropic_legacy_echo_tool_loop_render_golden.json.version");

    /// A run recorded before the thinking-echo scope existed must keep rendering exactly the
    /// bytes it rendered when it was admitted: only the last assistant message echoes its
    /// thinking. The recorded blob must also re-serialize unchanged, because resume compares it
    /// with the provider's config and refuses on any difference.
    #[test]
    fn anthropic_legacy_echo_tool_loop_render_matches_golden() {
        let recorded: serde_json::Value =
            serde_json::from_str(LEGACY_ECHO_FROZEN_CONFIG).expect("legacy blob is JSON");
        let frozen: broca_provider::wire::FrozenRenderConfig =
            serde_json::from_value(recorded.clone()).expect("legacy blob deserializes");
        assert_eq!(
            serde_json::to_value(&frozen).expect("serialize legacy config"),
            recorded,
            "a recorded legacy config must re-serialize to the same blob"
        );
        let bytes = AnthropicMessages.render(&frozen, &tool_loop_request());
        let rendered = String::from_utf8(bytes).expect("rendered bytes are UTF-8");

        if render_golden_support::update_if_requested(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/anthropic_legacy_echo_tool_loop_render_golden.json"
            ),
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/anthropic_legacy_echo_tool_loop_render_golden.json.version"
            ),
            rendered.as_bytes(),
            LEGACY_ECHO_TOOL_LOOP_GOLDEN.as_bytes(),
            LEGACY_ECHO_TOOL_LOOP_GOLDEN_VERSION,
            ANTHROPIC_RENDERER_VERSION,
            "Anthropic legacy-echo tool loop",
        ) {
            return;
        }

        render_golden_support::assert_matches_or_requires_bump(
            rendered.as_bytes(),
            LEGACY_ECHO_TOOL_LOOP_GOLDEN.as_bytes(),
            LEGACY_ECHO_TOOL_LOOP_GOLDEN_VERSION,
            ANTHROPIC_RENDERER_VERSION,
            "Anthropic legacy-echo tool loop",
        );
    }

    const TOOL_LOOP_GOLDEN: &str =
        include_str!("../fixtures/anthropic_tool_loop_render_golden.json");
    const TOOL_LOOP_GOLDEN_VERSION: &str =
        include_str!("../fixtures/anthropic_tool_loop_render_golden.json.version");

    /// The same request under the family default (the keep-all scope): every assistant message
    /// keeps its signed thinking, including the earlier turn's -- the renderer strips nothing
    /// by position.
    #[test]
    fn anthropic_tool_loop_render_matches_golden() {
        let mut frozen = anthropic_frozen_config("claude-opus-4-8");
        frozen.cache = Some(anthropic_cache_policy());
        let bytes = AnthropicMessages.render(&frozen, &tool_loop_request());
        let rendered = String::from_utf8(bytes).expect("rendered bytes are UTF-8");

        if render_golden_support::update_if_requested(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/anthropic_tool_loop_render_golden.json"
            ),
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/anthropic_tool_loop_render_golden.json.version"
            ),
            rendered.as_bytes(),
            TOOL_LOOP_GOLDEN.as_bytes(),
            TOOL_LOOP_GOLDEN_VERSION,
            ANTHROPIC_RENDERER_VERSION,
            "Anthropic tool loop",
        ) {
            return;
        }

        render_golden_support::assert_matches_or_requires_bump(
            rendered.as_bytes(),
            TOOL_LOOP_GOLDEN.as_bytes(),
            TOOL_LOOP_GOLDEN_VERSION,
            ANTHROPIC_RENDERER_VERSION,
            "Anthropic tool loop",
        );
    }

    /// The Claude Code persona the persona goldens render with, every value fixed.
    fn golden_persona() -> broca_provider::wire::ClaudeCodePersona {
        broca_provider::wire::ClaudeCodePersona {
            version: "2.1.280".into(),
            device_id: "0".repeat(64),
            account_uuid: Some("00000000-0000-4000-8000-000000000000".into()),
            session_id: "11111111-1111-4111-8111-111111111111".into(),
            prompt_id: Some("33333333-3333-4333-8333-333333333333".into()),
            cc_version_suffix: None,
            prompt_ordinal: None,
        }
    }

    /// Separate goldens preserve the 2.1.280 request fixtures: episodes admitted under that
    /// version must still render those recorded bytes when resumed after a client upgrade.
    #[test]
    fn anthropic_302_persona_and_tool_loop_match_goldens() {
        for (opts, name, stored, pin) in [
            (
                fixed_request(),
                "anthropic_302_persona_render_golden.json",
                include_str!("../fixtures/anthropic_302_persona_render_golden.json"),
                include_str!("../fixtures/anthropic_302_persona_render_golden.json.version"),
            ),
            (
                tool_loop_request(),
                "anthropic_302_persona_tool_loop_render_golden.json",
                include_str!("../fixtures/anthropic_302_persona_tool_loop_render_golden.json"),
                include_str!(
                    "../fixtures/anthropic_302_persona_tool_loop_render_golden.json.version"
                ),
            ),
        ] {
            let mut frozen = anthropic_frozen_config("claude-opus-4-8");
            let mut persona = golden_persona();
            persona.version = "2.1.302".into();
            persona.prompt_ordinal = Some(2);
            frozen.claude_code_persona = Some(persona);
            if name.contains("tool_loop") {
                frozen.cache = Some(anthropic_cache_policy());
            }
            let bytes = AnthropicMessages.render(&frozen, &opts);
            let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
            if !render_golden_support::update_if_requested(
                &path,
                &format!("{path}.version"),
                &bytes,
                stored.as_bytes(),
                pin,
                ANTHROPIC_RENDERER_VERSION,
                name,
            ) {
                render_golden_support::assert_matches_or_requires_bump(
                    &bytes,
                    stored.as_bytes(),
                    pin,
                    ANTHROPIC_RENDERER_VERSION,
                    name,
                );
            }
        }
    }

    const PERSONA_GOLDEN: &str = include_str!("../fixtures/anthropic_persona_render_golden.json");
    const PERSONA_GOLDEN_VERSION: &str =
        include_str!("../fixtures/anthropic_persona_render_golden.json.version");

    /// The request of `anthropic_render_matches_golden` from a run admitted with the Claude Code
    /// persona: the billing block leads the system, `metadata.user_id` is set, the keys are in
    /// the persona's order and `cch` is signed into the bytes. The golden without the persona
    /// stays as it was, because runs admitted before the persona still render it.
    #[test]
    fn anthropic_persona_render_matches_golden() {
        let mut frozen = anthropic_frozen_config("claude-opus-4-8");
        frozen.claude_code_persona = Some(golden_persona());
        let bytes = AnthropicMessages.render(&frozen, &fixed_request());
        let rendered = String::from_utf8(bytes).expect("rendered bytes are UTF-8");

        if render_golden_support::update_if_requested(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/anthropic_persona_render_golden.json"
            ),
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/anthropic_persona_render_golden.json.version"
            ),
            rendered.as_bytes(),
            PERSONA_GOLDEN.as_bytes(),
            PERSONA_GOLDEN_VERSION,
            ANTHROPIC_RENDERER_VERSION,
            "Anthropic persona",
        ) {
            return;
        }

        render_golden_support::assert_matches_or_requires_bump(
            rendered.as_bytes(),
            PERSONA_GOLDEN.as_bytes(),
            PERSONA_GOLDEN_VERSION,
            ANTHROPIC_RENDERER_VERSION,
            "Anthropic persona",
        );
    }

    const PERSONA_TOOL_LOOP_GOLDEN: &str =
        include_str!("../fixtures/anthropic_persona_tool_loop_render_golden.json");
    const PERSONA_TOOL_LOOP_GOLDEN_VERSION: &str =
        include_str!("../fixtures/anthropic_persona_tool_loop_render_golden.json.version");

    /// The tool loop of `anthropic_tool_loop_render_matches_golden` under the persona, with
    /// cache markers: the system breakpoint lands on the caller's text, after the billing and
    /// identity blocks, which never carry one.
    #[test]
    fn anthropic_persona_tool_loop_render_matches_golden() {
        let mut frozen = anthropic_frozen_config("claude-opus-4-8");
        frozen.cache = Some(anthropic_cache_policy());
        frozen.claude_code_persona = Some(golden_persona());
        let bytes = AnthropicMessages.render(&frozen, &tool_loop_request());
        let rendered = String::from_utf8(bytes).expect("rendered bytes are UTF-8");

        if render_golden_support::update_if_requested(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/anthropic_persona_tool_loop_render_golden.json"
            ),
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/anthropic_persona_tool_loop_render_golden.json.version"
            ),
            rendered.as_bytes(),
            PERSONA_TOOL_LOOP_GOLDEN.as_bytes(),
            PERSONA_TOOL_LOOP_GOLDEN_VERSION,
            ANTHROPIC_RENDERER_VERSION,
            "Anthropic persona tool loop",
        ) {
            return;
        }

        render_golden_support::assert_matches_or_requires_bump(
            rendered.as_bytes(),
            PERSONA_TOOL_LOOP_GOLDEN.as_bytes(),
            PERSONA_TOOL_LOOP_GOLDEN_VERSION,
            ANTHROPIC_RENDERER_VERSION,
            "Anthropic persona tool loop",
        );
    }

    /// A render config exactly as a run admitted with the keep-all echo scope recorded it, before
    /// the rule for echoing thinking across Anthropic models was part of the frozen config. A
    /// resumed run renders from this blob, so its bytes must never move: the golden below pins
    /// them.
    const LEGACY_CROSS_MODEL_FROZEN_CONFIG: &str = r#"{
        "family_id": "anthropic_messages",
        "renderer_version": 2,
        "wire_model_id": "claude-opus-4-8",
        "max_output_tokens": 2048,
        "reasoning": {
            "mode": "anthropic_interleaved", "budget_tokens": 1024, "last_turn_only": true,
            "echo_scope": "all_assistant_messages"
        },
        "tool_id": "passthrough",
        "system_prompt": {
            "mode": "required_identity_lead",
            "identity": "You are Claude Code, Anthropic's official CLI for Claude."
        },
        "usage": "anthropic_usage",
        "cache": {
            "version": 1, "class": "markers", "syntax": "anthropic_cache_control",
            "ttl": "1h", "max_breakpoints": 4, "lookback_blocks": 20
        },
        "token_limit_field": "max_tokens"
    }"#;

    /// A conversation that switched models: `claude-opus-4-8` reasons and calls a tool, then
    /// ends on a turn carrying only thinking; `claude-opus-5` does the same; a Gemini model
    /// answers with its own reasoning; and the request goes back to `claude-opus-4-8`. Every
    /// assistant message names the model that produced it.
    fn model_round_trip_request() -> CallOptions {
        use broca_protocol::request::ModelHandle;
        let text = |role: Role, text: &str| {
            Message::new(role, vec![ContentBlock::Text { text: text.into() }])
        };
        let reasoning = |label: &str| ContentBlock::Reasoning {
            text: format!("{label} thinking"),
            signature: Some(format!("sig-{label}")),
        };
        let call = |label: &str| {
            ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId(format!("toolu_{label}")),
                tool_name: "tick".into(),
                input: json!({ "label": label }),
            })
        };
        let result = |label: &str| {
            Message::new(
                Role::Tool,
                vec![ContentBlock::ToolResult(ToolResult {
                    tool_call_id: ToolCallId(format!("toolu_{label}")),
                    output: ToolOutput::Text {
                        text: format!("ticked {label}"),
                    },
                    is_error: false,
                })],
            )
        };
        let opus_4_8 = ModelHandle::new("anthropic", "claude-opus-4-8");
        let opus_5 = ModelHandle::new("anthropic", "claude-opus-5");
        let gemini = ModelHandle::new("google", "gemini-3.5-pro");
        let mut opts = CallOptions::new(vec![
            text(Role::System, "Be concise."),
            text(Role::User, "Tick once."),
            Message::assistant_from(vec![reasoning("a1"), call("a1")], opus_4_8.clone()),
            result("a1"),
            Message::assistant_from(vec![reasoning("a2")], opus_4_8.clone()),
            text(Role::User, "Tick again."),
            Message::assistant_from(vec![reasoning("b1"), call("b1")], opus_5.clone()),
            result("b1"),
            Message::assistant_from(vec![reasoning("b2")], opus_5),
            text(Role::User, "Summarize."),
            Message::assistant_from(
                vec![
                    reasoning("g1"),
                    ContentBlock::Text {
                        text: "Two ticks.".into(),
                    },
                ],
                gemini,
            ),
            text(Role::User, "Tick once more."),
        ]);
        opts.tools = vec![Tool {
            name: "tick".into(),
            description: "Return the tick label.".into(),
            input_schema: json!({
                "type": "object",
                "properties": { "label": { "type": "string" } },
                "required": ["label"]
            }),
            module: None,
            execution_mode: Default::default(),
        }]
        .into();
        opts.tool_choice = ToolChoice::Auto.into();
        opts.target_model = Some(opus_4_8);
        opts
    }

    const LEGACY_CROSS_MODEL_GOLDEN: &str =
        include_str!("../fixtures/anthropic_legacy_cross_model_render_golden.json");
    const LEGACY_CROSS_MODEL_GOLDEN_VERSION: &str =
        include_str!("../fixtures/anthropic_legacy_cross_model_render_golden.json.version");

    /// A run recorded before the cross-model thinking rule was frozen must keep rendering exactly
    /// the bytes it rendered when it was admitted: thinking is echoed only to the model that
    /// produced it, and a turn left empty by that is omitted. The recorded blob must also
    /// re-serialize unchanged, because resume compares it with the provider's config and refuses
    /// on any difference.
    #[test]
    fn anthropic_legacy_cross_model_render_matches_golden() {
        let recorded: serde_json::Value =
            serde_json::from_str(LEGACY_CROSS_MODEL_FROZEN_CONFIG).expect("legacy blob is JSON");
        let frozen: broca_provider::wire::FrozenRenderConfig =
            serde_json::from_value(recorded.clone()).expect("legacy blob deserializes");
        assert_eq!(
            serde_json::to_value(&frozen).expect("serialize legacy config"),
            recorded,
            "a recorded legacy config must re-serialize to the same blob"
        );
        let bytes = AnthropicMessages.render(&frozen, &model_round_trip_request());
        let rendered = String::from_utf8(bytes).expect("rendered bytes are UTF-8");

        if render_golden_support::update_if_requested(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/anthropic_legacy_cross_model_render_golden.json"
            ),
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/anthropic_legacy_cross_model_render_golden.json.version"
            ),
            rendered.as_bytes(),
            LEGACY_CROSS_MODEL_GOLDEN.as_bytes(),
            LEGACY_CROSS_MODEL_GOLDEN_VERSION,
            ANTHROPIC_RENDERER_VERSION,
            "Anthropic legacy cross-model",
        ) {
            return;
        }

        render_golden_support::assert_matches_or_requires_bump(
            rendered.as_bytes(),
            LEGACY_CROSS_MODEL_GOLDEN.as_bytes(),
            LEGACY_CROSS_MODEL_GOLDEN_VERSION,
            ANTHROPIC_RENDERER_VERSION,
            "Anthropic legacy cross-model",
        );
    }

    const CROSS_MODEL_GOLDEN: &str =
        include_str!("../fixtures/anthropic_cross_model_render_golden.json");
    const CROSS_MODEL_GOLDEN_VERSION: &str =
        include_str!("../fixtures/anthropic_cross_model_render_golden.json.version");

    /// The same request under the family default: every Anthropic model's signed thinking is
    /// echoed back unchanged, including the other Anthropic model's turn that carries only
    /// thinking, while the Gemini turn's reasoning is still stripped.
    #[test]
    fn anthropic_cross_model_render_matches_golden() {
        let mut frozen = anthropic_frozen_config("claude-opus-4-8");
        frozen.cache = Some(anthropic_cache_policy());
        let bytes = AnthropicMessages.render(&frozen, &model_round_trip_request());
        let rendered = String::from_utf8(bytes).expect("rendered bytes are UTF-8");

        if render_golden_support::update_if_requested(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/anthropic_cross_model_render_golden.json"
            ),
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/anthropic_cross_model_render_golden.json.version"
            ),
            rendered.as_bytes(),
            CROSS_MODEL_GOLDEN.as_bytes(),
            CROSS_MODEL_GOLDEN_VERSION,
            ANTHROPIC_RENDERER_VERSION,
            "Anthropic cross-model",
        ) {
            return;
        }

        render_golden_support::assert_matches_or_requires_bump(
            rendered.as_bytes(),
            CROSS_MODEL_GOLDEN.as_bytes(),
            CROSS_MODEL_GOLDEN_VERSION,
            ANTHROPIC_RENDERER_VERSION,
            "Anthropic cross-model",
        );
    }
}
