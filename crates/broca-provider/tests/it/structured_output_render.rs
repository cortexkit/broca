//! Structured output on each wire family: the schema encoding a route the catalog vouches for
//! is sent, the submit tool beside the run's own tools, what a schema outside the family's
//! subset profile is sent as instead, and the instructions a route with no encoding gets.
//!
//! Every case renders from a frozen config decided by `structured_output::decide`, the same
//! function admission uses, so the bytes asserted here are the bytes a run sends.

use std::sync::Arc;

use broca_protocol::content::{ContentBlock, Message, Role};
use broca_protocol::request::{CallOptions, DeclaredMessageMarker, StepContext, Tool};
use broca_protocol::structured_output::{
    FrozenStructuredOutput, Mode, SchemaDelivery, StructuredOutput,
};
use broca_provider::families::{
    anthropic_frozen_config, bedrock_frozen_config, gemini_frozen_config,
    google_codeassist_frozen_config, openai_chat_frozen_config,
    openai_responses_chatgpt_frozen_config, openai_responses_frozen_config, AnthropicMessages,
    BedrockConverse, Gemini, GoogleCodeAssist, OpenAiChat, OpenAiResponses, OpenAiResponsesChatgpt,
};
use broca_provider::spec::{CapabilityFlag, WireFamilyId};
use broca_provider::structured_output::{decide, prepare};
use broca_provider::wire::{FrozenRenderConfig, WireFamily};
use serde_json::{json, Value};

const SUPPORTED: CapabilityFlag = CapabilityFlag::Known(true);

/// A closed object with every property required: inside every profile.
fn closed() -> Value {
    json!({
        "type": "object",
        "properties": {"city": {"type": "string"}, "verdict": {"enum": ["big", "small"]}},
        "required": ["city", "verdict"],
        "additionalProperties": false
    })
}

/// A schema with `oneOf`, which the ChatGPT backend refused in strict mode in live probes:
/// outside every strict subset profile but xAI's.
fn one_of() -> Value {
    let mut schema = closed();
    schema["properties"]["shape"] = json!({"oneOf": [{"type": "string"}, {"type": "number"}]});
    schema["required"] = json!(["city", "verdict", "shape"]);
    schema
}

fn with_structured_output(
    mut frozen: FrozenRenderConfig,
    provider_id: &str,
    mode: Mode,
    schema: Value,
) -> (FrozenRenderConfig, FrozenStructuredOutput) {
    let decided = decide(
        frozen.family_id,
        provider_id,
        frozen.base_url.as_deref(),
        SUPPORTED,
        &StructuredOutput::new(mode, schema).with_name("city_report"),
    )
    .unwrap();
    frozen.structured_output = Some(decided.clone());
    (frozen, decided)
}

fn opts(tools: bool) -> CallOptions {
    let mut opts = CallOptions::new(vec![
        Message::new(
            Role::System,
            vec![ContentBlock::Text {
                text: "You report on cities.".into(),
            }],
        ),
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "Tell me about Paris.".into(),
            }],
        ),
    ]);
    if tools {
        opts.tools = Arc::new(vec![Tool {
            name: "lookup_city".into(),
            description: "Look a city up.".into(),
            input_schema: json!({"type": "object", "properties": {"name": {"type": "string"}}}),
            module: None,
            execution_mode: Default::default(),
            reply: None,
        }]);
    }
    opts.step_context = Some(StepContext {
        session_address: None,
        credential_id: None,
        account_ref: None,
        run_id: "run-so".into(),
        step_index: 1,
    });
    opts
}

fn body(family: &dyn WireFamily, frozen: &FrozenRenderConfig, opts: &CallOptions) -> Value {
    serde_json::from_slice(&family.render(frozen, opts)).unwrap()
}

fn tool_named<'a>(tools: &'a Value, name: &str, field: &[&str]) -> &'a Value {
    tools
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| field.iter().fold(*tool, |value, key| &value[*key]) == name)
        .unwrap_or_else(|| panic!("no tool {name} in {tools}"))
}

#[test]
fn responses_final_text_is_strict_inside_openai_strict_and_plain_outside() {
    for (family, frozen) in [
        (
            &OpenAiResponses as &dyn WireFamily,
            openai_responses_frozen_config("gpt-6-luna"),
        ),
        (
            &OpenAiResponsesChatgpt,
            openai_responses_chatgpt_frozen_config("gpt-6-luna"),
        ),
    ] {
        let (inside, decided) =
            with_structured_output(frozen.clone(), "openai", Mode::FinalText, closed());
        assert_eq!(decided.enforcement, "enforced");
        assert_eq!(decided.profile.as_deref(), Some("openai_strict"));
        let sent = body(family, &inside, &opts(false));
        assert_eq!(
            sent["text"]["format"],
            json!({"type": "json_schema", "name": "city_report", "schema": closed(), "strict": true})
        );
        assert!(sent.get("tools").is_none());

        let (outside, decided) =
            with_structured_output(frozen, "openai", Mode::FinalText, one_of());
        assert_eq!(decided.reason.as_deref(), Some("schema_outside_subset"));
        // The backend validates `text.format` even without `strict`, so a schema it cannot
        // enforce is never sent as the format; it reaches the model as instructions.
        assert_eq!(decided.delivery, SchemaDelivery::Unconstrained);
        let sent = body(family, &outside, &opts(false));
        assert!(
            sent["text"].get("format").is_none(),
            "an out-of-subset schema must not be sent as text.format: {}",
            sent["text"]
        );
        assert!(
            serde_json::to_string(&sent)
                .unwrap()
                .contains("city_report"),
            "the schema still reaches the model through the instructions"
        );
    }
}

#[test]
fn responses_submit_tool_is_strict_beside_non_strict_tools_and_never_forced() {
    let mut frozen = openai_responses_chatgpt_frozen_config("gpt-6-luna");
    frozen.responses_tools_non_strict = true;
    let (frozen, decided) = with_structured_output(frozen, "openai", Mode::SubmitTool, closed());
    assert_eq!(decided.enforcement, "enforced");
    let sent = body(&OpenAiResponsesChatgpt, &frozen, &opts(true));
    let tools = &sent["tools"];
    assert_eq!(tools.as_array().unwrap().len(), 2);
    assert_eq!(tools[1]["name"], "submit_result");
    assert_eq!(tools[1]["parameters"], closed());
    assert_eq!(tools[1]["strict"], true);
    assert_eq!(tool_named(tools, "lookup_city", &["name"])["strict"], false);
    assert_eq!(sent["tool_choice"], "auto");
    assert!(sent.get("text").is_none());

    // Outside the openai_strict subset profile the submit tool still goes out, explicitly
    // non-strict.
    let mut frozen = openai_responses_frozen_config("gpt-6-luna");
    frozen.responses_tools_non_strict = true;
    let (frozen, _) = with_structured_output(frozen, "openai", Mode::SubmitTool, one_of());
    let sent = body(&OpenAiResponses, &frozen, &opts(true));
    assert_eq!(
        tool_named(&sent["tools"], "submit_result", &["name"])["strict"],
        false
    );
}

#[test]
fn meta_uses_the_responses_encoding_with_its_own_profile() {
    let mut frozen = openai_responses_frozen_config("muse-spark-1.3");
    frozen.base_url = Some("https://api.meta.ai/v1".into());
    let root_any_of = json!({"anyOf": [closed(), closed()]});
    let (_, decided) = with_structured_output(frozen.clone(), "meta", Mode::FinalText, root_any_of);
    assert_eq!(decided.profile.as_deref(), Some("meta_strict"));
    assert_eq!(decided.reason.as_deref(), Some("schema_outside_subset"));
    let (frozen, decided) = with_structured_output(frozen, "meta", Mode::FinalText, closed());
    assert_eq!(decided.enforcement, "enforced");
    let sent = body(&OpenAiResponses, &frozen, &opts(false));
    assert_eq!(sent["text"]["format"]["strict"], true);
}

#[test]
fn anthropic_merges_the_format_into_output_config_and_marks_the_submit_tool_strict() {
    let mut frozen = anthropic_frozen_config("claude-sonnet-5-5");
    frozen.variant = Some(broca_provider::variant::FrozenVariant {
        name: "high".into(),
        request: broca_provider::variant::VariantRequest::Anthropic {
            thinking: Some(json!({"type": "adaptive"})),
            effort: Some("high".into()),
        },
    });
    let (final_text, decided) =
        with_structured_output(frozen.clone(), "anthropic", Mode::FinalText, closed());
    assert_eq!(decided.profile.as_deref(), Some("anthropic_so"));
    let sent = body(&AnthropicMessages, &final_text, &opts(false));
    assert_eq!(
        sent["output_config"],
        json!({"effort": "high", "format": {"type": "json_schema", "schema": closed()}})
    );

    let (submit, _) =
        with_structured_output(frozen.clone(), "anthropic", Mode::SubmitTool, closed());
    let sent = body(&AnthropicMessages, &submit, &opts(true));
    let tool = tool_named(&sent["tools"], "submit_result", &["name"]);
    assert_eq!(tool["strict"], true);
    assert_eq!(tool["input_schema"], closed());
    assert!(tool_named(&sent["tools"], "lookup_city", &["name"])
        .get("strict")
        .is_none());
    // The submit tool is never forced: forcing breaks under manual thinking.
    assert_eq!(sent["tool_choice"], json!({"type": "auto"}));
    assert!(sent["output_config"].get("format").is_none());

    // Live probes of Claude Sonnet 5.5 and Opus 5.5 showed `pattern` and `minLength` enforced:
    // such a schema still goes in
    // `output_config.format`.
    let mut bounded = closed();
    bounded["properties"]["city"]["pattern"] = json!("^[A-Z]");
    bounded["properties"]["city"]["minLength"] = json!(3);
    let (enforced, decided) = with_structured_output(
        frozen.clone(),
        "anthropic",
        Mode::FinalText,
        bounded.clone(),
    );
    assert_eq!(decided.enforcement, "enforced");
    let sent = body(&AnthropicMessages, &enforced, &opts(false));
    assert_eq!(sent["output_config"]["format"]["schema"], bounded);

    // Anthropic refused `oneOf` with an HTTP 400 in live probes: the final text falls back to
    // instructions.
    let (outside, decided) = with_structured_output(frozen, "anthropic", Mode::FinalText, one_of());
    assert_eq!(decided.delivery, SchemaDelivery::Unconstrained);
    let sent = body(&AnthropicMessages, &outside, &opts(false));
    assert!(sent["output_config"].get("format").is_none());
    assert!(sent["system"].to_string().contains("city_report"));
}

#[test]
fn xai_responses_enforces_the_xai_subset_in_both_modes() {
    let mut xai = openai_responses_frozen_config("grok-4.5");
    xai.base_url = Some("https://api.x.ai/v1".into());
    xai.responses_tools_non_strict = true;
    let (final_text, decided) =
        with_structured_output(xai.clone(), "xai", Mode::FinalText, one_of());
    assert_eq!(decided.enforcement, "enforced");
    assert_eq!(decided.profile.as_deref(), Some("xai"));
    let sent = body(&OpenAiResponses, &final_text, &opts(false));
    assert_eq!(
        sent["text"]["format"],
        json!({
            "type": "json_schema", "name": "city_report", "schema": one_of(), "strict": true
        })
    );
    let (submit, decided) = with_structured_output(xai.clone(), "xai", Mode::SubmitTool, one_of());
    assert_eq!(decided.enforcement, "enforced");
    let sent = body(&OpenAiResponses, &submit, &opts(true));
    let submit = tool_named(&sent["tools"], "submit_result", &["name"]);
    assert_eq!(submit["parameters"], one_of());
    assert_eq!(submit["strict"], true);
    assert_eq!(sent["tool_choice"], "auto");
    assert!(sent.get("text").is_none());

    let outside = json!({"type": "object", "properties": {
        "answer": {"type": "string", "minLength": 2049}
    }});
    let (frozen, decided) = with_structured_output(xai, "xai", Mode::FinalText, outside);
    assert_eq!(decided.enforcement, "best_effort");
    assert_eq!(decided.reason.as_deref(), Some("schema_outside_subset"));
    let sent = body(&OpenAiResponses, &frozen, &opts(false));
    assert!(sent["text"].get("format").is_none());
    assert!(serde_json::to_string(&sent)
        .unwrap()
        .contains("city_report"));
}

#[test]
fn xai_sends_response_format_and_kimi_gets_instructions() {
    let mut xai = openai_chat_frozen_config("grok-4.7");
    xai.base_url = Some("https://api.x.ai/v1".into());
    let (frozen, decided) = with_structured_output(xai.clone(), "xai", Mode::FinalText, one_of());
    assert_eq!(
        (decided.enforcement.as_str(), decided.profile.as_deref()),
        ("enforced", Some("xai"))
    );
    let sent = body(&OpenAiChat, &frozen, &opts(false));
    assert_eq!(
        sent["response_format"],
        json!({"type": "json_schema", "json_schema": {"name": "city_report", "schema": one_of(), "strict": true}})
    );
    let (submit, _) = with_structured_output(xai, "xai", Mode::SubmitTool, closed());
    let sent = body(&OpenAiChat, &submit, &opts(true));
    assert_eq!(
        tool_named(&sent["tools"], "submit_result", &["function", "name"])["function"]
            ["parameters"],
        closed()
    );
    assert!(sent.get("response_format").is_none());

    let mut kimi = openai_chat_frozen_config("kimi-k3");
    kimi.base_url = Some("https://api.moonshot.ai/v1".into());
    let (frozen, decided) = with_structured_output(kimi, "moonshotai", Mode::FinalText, closed());
    assert_eq!(decided.reason.as_deref(), Some("route_unsupported"));
    let sent = body(&OpenAiChat, &frozen, &opts(false));
    assert!(sent.get("response_format").is_none());
    let system = sent["messages"][0].to_string();
    assert!(system.contains("You report on cities."), "{system}");
    assert!(
        system.contains("city_report") && system.contains("draft 2020-12"),
        "{system}"
    );
}

#[test]
fn gemini_shapes_final_text_and_cannot_enforce_tool_input() {
    let (frozen, decided) = with_structured_output(
        gemini_frozen_config("gemini-3.8-flash"),
        "google",
        Mode::FinalText,
        closed(),
    );
    assert_eq!(decided.enforcement, "enforced");
    let sent = body(&Gemini, &frozen, &opts(false));
    assert_eq!(
        sent["generationConfig"]["responseMimeType"],
        "application/json"
    );
    assert_eq!(sent["generationConfig"]["responseJsonSchema"], closed());

    let mut codeassist = google_codeassist_frozen_config("gemini-3.8-flash");
    codeassist.project_id = Some("proj".into());
    let (frozen, _) = with_structured_output(codeassist, "google", Mode::FinalText, closed());
    let sent = body(&GoogleCodeAssist, &frozen, &opts(false));
    assert_eq!(
        sent["request"]["generationConfig"]["responseJsonSchema"],
        closed()
    );

    let (frozen, decided) = with_structured_output(
        gemini_frozen_config("gemini-3.8-flash"),
        "google",
        Mode::SubmitTool,
        closed(),
    );
    assert_eq!(decided.reason.as_deref(), Some("mode_not_enforceable"));
    let sent = body(&Gemini, &frozen, &opts(true));
    let declarations = &sent["tools"][0]["functionDeclarations"];
    assert_eq!(
        tool_named(declarations, "submit_result", &["name"])["name"],
        "submit_result"
    );
    assert!(sent["generationConfig"].get("responseJsonSchema").is_none());
}

#[test]
fn bedrock_has_no_encoding_but_still_offers_the_submit_tool() {
    let (frozen, decided) = with_structured_output(
        bedrock_frozen_config("anthropic.claude-sonnet-5-5"),
        "amazon-bedrock",
        Mode::SubmitTool,
        closed(),
    );
    assert_eq!(decided.reason.as_deref(), Some("route_unsupported"));
    let sent = body(&BedrockConverse, &frozen, &opts(true));
    let tools = sent["toolConfig"]["tools"].to_string();
    assert!(tools.contains("submit_result"), "{tools}");
}

#[test]
fn a_run_without_structured_output_renders_from_its_own_options() {
    // `prepare` hands back the caller's options untouched, so every family renders exactly the
    // bytes its goldens pin.
    let opts = opts(true);
    for frozen in [
        openai_chat_frozen_config("m"),
        anthropic_frozen_config("m"),
        gemini_frozen_config("m"),
        openai_responses_frozen_config("m"),
    ] {
        assert!(matches!(
            prepare(&frozen, &opts),
            std::borrow::Cow::Borrowed(_)
        ));
    }
}

#[test]
fn a_frozen_decision_renders_byte_identically_after_a_round_trip_through_the_wal() {
    for (family, frozen, mode) in [
        (
            &OpenAiChat as &dyn WireFamily,
            openai_chat_frozen_config("grok-4.7"),
            Mode::SubmitTool,
        ),
        (
            &AnthropicMessages,
            anthropic_frozen_config("claude-sonnet-5-5"),
            Mode::FinalText,
        ),
        (
            &OpenAiResponses,
            openai_responses_frozen_config("gpt-6-luna"),
            Mode::FinalText,
        ),
    ] {
        let provider = if frozen.family_id == WireFamilyId::OpenAiChat {
            "xai"
        } else {
            "p"
        };
        let (frozen, _) = with_structured_output(frozen, provider, mode, closed());
        let tools = mode == Mode::SubmitTool;
        let first = family.render(&frozen, &opts(tools));
        let resumed: FrozenRenderConfig =
            serde_json::from_value(serde_json::to_value(&frozen).unwrap()).unwrap();
        assert_eq!(resumed, frozen);
        assert_eq!(family.render(&resumed, &opts(tools)), first);
    }
}

#[test]
fn instructions_without_a_system_message_keep_renderer_positions_aligned() {
    let mut kimi = openai_chat_frozen_config("kimi-k3");
    kimi.base_url = Some("https://api.moonshot.ai/v1".into());
    let (frozen, _) = with_structured_output(kimi, "moonshotai", Mode::FinalText, closed());
    let mut opts = CallOptions::new(vec![Message::new(
        Role::User,
        vec![ContentBlock::Text { text: "hi".into() }],
    )]);
    opts.declared_markers = vec![DeclaredMessageMarker {
        message_index: 0,
        rank: 1,
    }];
    let prepared = prepare(&frozen, &opts);
    assert_eq!(prepared.prompt.len(), 2);
    assert_eq!(prepared.prompt[0].role, Role::System);
    assert_eq!(prepared.prompt[1].role, Role::User);
    assert_eq!(prepared.declared_markers[0].message_index, 1);
}

/// Claude Sonnet 5.5 and Opus 5.5 refuse every forcing `tool_choice` with an HTTP 400, with or
/// without thinking (measured in live probes, October 2026). A `submit_tool` run never sends one to the Anthropic family,
/// whatever the caller asked for.
#[test]
fn anthropic_submit_tool_never_forces_a_tool() {
    use broca_protocol::request::ToolChoice;
    let (frozen, _) = with_structured_output(
        anthropic_frozen_config("claude-opus-5-5"),
        "anthropic",
        Mode::SubmitTool,
        closed(),
    );
    for choice in [
        ToolChoice::Auto,
        ToolChoice::Required,
        ToolChoice::Tool {
            name: "submit_result".into(),
        },
        ToolChoice::Tool {
            name: "lookup_city".into(),
        },
    ] {
        let mut opts = opts(true);
        opts.tool_choice = Arc::new(choice.clone());
        let sent = body(&AnthropicMessages, &frozen, &opts);
        assert_eq!(sent["tool_choice"], json!({"type": "auto"}), "{choice:?}");
        assert_eq!(
            tool_named(&sent["tools"], "submit_result", &["name"])["strict"],
            true
        );
    }
    // A caller who switches tools off keeps that: `none` forces nothing.
    let mut opts = opts(true);
    opts.tool_choice = Arc::new(ToolChoice::None);
    let sent = body(&AnthropicMessages, &frozen, &opts);
    assert_eq!(sent["tool_choice"], json!({"type": "none"}));
}

#[test]
fn aggregator_fixture_is_delivered_without_strict_constraints() {
    let schema: Value =
        serde_json::from_str(include_str!("../fixtures/aggregator-schema.json")).unwrap();
    for mode in [Mode::SubmitTool, Mode::FinalText] {
        let (frozen, decision) = with_structured_output(
            anthropic_frozen_config("claude-opus-5-5"),
            "anthropic",
            mode,
            schema.clone(),
        );
        assert_eq!(decision.reason.as_deref(), Some("schema_too_complex"));
        let rendered = body(&AnthropicMessages, &frozen, &opts(false));
        if mode == Mode::SubmitTool {
            let submit = tool_named(&rendered["tools"], "submit_result", &["name"]);
            assert!(submit.get("strict").is_none());
            assert_eq!(submit["input_schema"], schema);
        } else {
            assert!(rendered.pointer("/output_config/format").is_none());
        }
    }
}
