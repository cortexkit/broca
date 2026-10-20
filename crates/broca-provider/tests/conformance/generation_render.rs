//! Generation-control render coverage across families.
//!
//! These request fields are body-visible inputs that come from `CallOptions.generation`.
//! A run freeze stores the caller's generation config in the run config, and resume rebuilds
//! `CallOptions` from that frozen data before rendering again, so every family that supports a
//! field must emit it with the provider's exact wire spelling.

use broca_protocol::{
    content::{ContentBlock, Message, Role},
    request::CallOptions,
    StepContext,
};
use broca_provider::conformance::check_render_determinism;
use broca_provider::families::{
    anthropic_apikey_frozen_config, bedrock_frozen_config_with_region, gemini_frozen_config,
    google_codeassist_frozen_config, openai_chat_frozen_config,
    openai_responses_chatgpt_frozen_config, openai_responses_frozen_config, AnthropicMessages,
    BedrockConverse, Gemini, GoogleCodeAssist, OpenAiChat, OpenAiResponses, OpenAiResponsesChatgpt,
};
use broca_provider::wire::{FrozenRenderConfig, WireFamily};
use serde_json::{json, Value};

fn base_request() -> CallOptions {
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
                text: "Say hi.".into(),
            }],
        },
    ]);
    // Code-Assist derives a deterministic requestId from step identity, so populate it in the
    // shared test request used for every family.
    opts.step_context = Some(StepContext {
        session_address: None,
        credential_id: None,
        account_ref: None,
        run_id: "run-generation-01".into(),
        step_index: 2,
    });
    opts
}

fn generation_request() -> CallOptions {
    let mut opts = base_request();
    let mut generation = opts.generation.as_ref().clone();
    generation.temperature = Some(0.1);
    generation.top_p = Some(0.9);
    generation.stop_sequences = vec!["END".into()];
    opts.generation = generation.into();
    opts
}

fn render_json(family: &dyn WireFamily, frozen: &FrozenRenderConfig, opts: &CallOptions) -> Value {
    serde_json::from_slice(&family.render(frozen, opts)).expect("rendered bytes are JSON")
}

fn anthropic_body(opts: &CallOptions) -> Value {
    let frozen = anthropic_apikey_frozen_config("claude-opus-4-8");
    render_json(&AnthropicMessages, &frozen, opts)
}

fn openai_chat_body(opts: &CallOptions) -> Value {
    let frozen = openai_chat_frozen_config("gpt-4o");
    render_json(&OpenAiChat, &frozen, opts)
}

fn openai_responses_body(opts: &CallOptions) -> Value {
    let frozen = openai_responses_frozen_config("gpt-4.1");
    render_json(&OpenAiResponses, &frozen, opts)
}

fn openai_responses_chatgpt_body(opts: &CallOptions) -> Value {
    let frozen = openai_responses_chatgpt_frozen_config("gpt-5-codex");
    render_json(&OpenAiResponsesChatgpt, &frozen, opts)
}

fn gemini_body(opts: &CallOptions) -> Value {
    let frozen = gemini_frozen_config("gemini-2.0-flash");
    render_json(&Gemini, &frozen, opts)
}

fn google_codeassist_body(opts: &CallOptions) -> Value {
    let mut frozen = google_codeassist_frozen_config("gemini-2.5-flash");
    frozen.project_id = Some("proj-generation".into());
    render_json(&GoogleCodeAssist, &frozen, opts)
}

struct Case {
    name: &'static str,
    render: fn(&CallOptions) -> Value,
    temperature_ptr: &'static str,
    top_p_ptr: &'static str,
    stop_ptr: &'static str,
    emits_sampling: bool,
    emits_stop: bool,
}

#[test]
fn changed_families_render_generation_controls_with_exact_wire_spellings() {
    let with_generation = generation_request();
    let without_generation = base_request();
    let cases = [
        Case {
            name: "anthropic_messages",
            render: anthropic_body,
            temperature_ptr: "/temperature",
            top_p_ptr: "/top_p",
            stop_ptr: "/stop_sequences",
            emits_sampling: true,
            emits_stop: true,
        },
        Case {
            name: "openai_chat",
            render: openai_chat_body,
            temperature_ptr: "/temperature",
            top_p_ptr: "/top_p",
            stop_ptr: "/stop",
            emits_sampling: true,
            emits_stop: true,
        },
        Case {
            name: "openai_responses",
            render: openai_responses_body,
            temperature_ptr: "/temperature",
            top_p_ptr: "/top_p",
            stop_ptr: "/stop",
            emits_sampling: true,
            emits_stop: false,
        },
        Case {
            name: "openai_responses_chatgpt",
            render: openai_responses_chatgpt_body,
            temperature_ptr: "/temperature",
            top_p_ptr: "/top_p",
            stop_ptr: "/stop",
            emits_sampling: false,
            emits_stop: false,
        },
        Case {
            name: "gemini",
            render: gemini_body,
            temperature_ptr: "/generationConfig/temperature",
            top_p_ptr: "/generationConfig/topP",
            stop_ptr: "/generationConfig/stopSequences",
            emits_sampling: true,
            emits_stop: true,
        },
        Case {
            name: "google_codeassist",
            render: google_codeassist_body,
            temperature_ptr: "/request/generationConfig/temperature",
            top_p_ptr: "/request/generationConfig/topP",
            stop_ptr: "/request/generationConfig/stopSequences",
            emits_sampling: true,
            emits_stop: true,
        },
    ];

    for case in cases {
        let rendered = (case.render)(&with_generation);
        if case.emits_sampling {
            assert_eq!(
                rendered.pointer(case.temperature_ptr),
                Some(&json!(0.1)),
                "{} must render temperature at {}: {rendered}",
                case.name,
                case.temperature_ptr,
            );
            assert_eq!(
                rendered.pointer(case.top_p_ptr),
                Some(&json!(0.9)),
                "{} must render top_p at {}: {rendered}",
                case.name,
                case.top_p_ptr,
            );
        } else {
            assert!(
                rendered.pointer(case.temperature_ptr).is_none()
                    && rendered.pointer(case.top_p_ptr).is_none(),
                "{} must omit unsupported sampling controls: {rendered}",
                case.name,
            );
        }
        if case.emits_stop {
            assert_eq!(
                rendered.pointer(case.stop_ptr),
                Some(&json!(["END"])),
                "{} must render stop sequences at {}: {rendered}",
                case.name,
                case.stop_ptr,
            );
        } else {
            assert!(
                rendered.pointer(case.stop_ptr).is_none(),
                "{} must not invent a stop field: {rendered}",
                case.name,
            );
        }

        let rendered_default = (case.render)(&without_generation);
        assert!(
            rendered_default.pointer(case.temperature_ptr).is_none(),
            "{} must omit unset temperature: {rendered_default}",
            case.name,
        );
        assert!(
            rendered_default.pointer(case.top_p_ptr).is_none(),
            "{} must omit unset top_p: {rendered_default}",
            case.name,
        );
        assert!(
            rendered_default.pointer(case.stop_ptr).is_none(),
            "{} must omit empty stop sequences: {rendered_default}",
            case.name,
        );
    }
}

#[test]
fn openai_chat_generation_request_stays_deterministic() {
    let frozen = openai_chat_frozen_config("gpt-4o");
    let report = check_render_determinism(&OpenAiChat, &frozen, &generation_request());
    assert!(
        report.passed(),
        "generation-bearing render must stay pure: {:?}",
        report.failures()
    );
}

#[test]
fn openai_chat_generation_round_trips_through_resume_render_path() {
    let frozen = openai_chat_frozen_config("gpt-4o");
    let original_opts = generation_request();
    let first = OpenAiChat.render(&frozen, &original_opts);

    let blob = serde_json::to_vec(&frozen).expect("serialize frozen config");
    let restored: FrozenRenderConfig =
        serde_json::from_slice(&blob).expect("deserialize frozen config");

    let mut resumed_opts = base_request();
    resumed_opts.generation = original_opts.generation.clone();
    resumed_opts.step_context = original_opts.step_context.clone();
    let second = OpenAiChat.render(&restored, &resumed_opts);

    assert_eq!(
        first, second,
        "resume must re-render the same generation-bearing request bytes"
    );
}

#[test]
fn bedrock_generation_controls_remain_pinned_in_inference_config() {
    let frozen =
        bedrock_frozen_config_with_region("anthropic.claude-3-5-sonnet-20240620-v1:0", "us-west-2");
    let rendered = render_json(&BedrockConverse, &frozen, &generation_request());
    assert_eq!(
        rendered.pointer("/inferenceConfig/temperature"),
        Some(&json!(0.1))
    );
    assert_eq!(rendered.pointer("/inferenceConfig/topP"), Some(&json!(0.9)));
    assert_eq!(
        rendered.pointer("/inferenceConfig/stopSequences"),
        Some(&json!(["END"]))
    );
}
