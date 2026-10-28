//! Google Antigravity / Code-Assist family conformance gate.
//!
//! The render-determinism check is the LOAD-BEARING gate for this family: the envelope's
//! `requestId` is body-visible and must be reproducible, so render-twice + perturbed-ambient
//! prove the deterministic-from-(run_id, step_index) requestId never varies by process state.

use broca_protocol::{
    content::{ContentBlock, Message, Role, ToolCall},
    request::{CallOptions, Tool, ToolChoice},
    StepContext, ToolCallId,
};
use broca_provider::conformance::{check_freeze_against_mutation, check_render_determinism};
use broca_provider::families::{
    google_codeassist_frozen_config, resolve_frozen, GoogleCodeAssist,
    GOOGLE_CODEASSIST_RENDERER_VERSION,
};
use broca_provider::policy::{
    Quirks, SystemPromptPolicy, TokenLimitField, ToolIdPolicy, UsagePolicy,
};
use broca_provider::spec::{
    AuthKind, Capabilities, CapabilityFlag, CostSchedule, Limits, ModelSpec, Overlay, ProviderSpec,
    WireFamilyId,
};
use serde_json::json;

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
            content: vec![ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId("call_1".into()),
                tool_name: "a_tool".into(),
                input: json!({"x": 1}),
            })],
        },
    ]);
    opts.tools = (0..12)
        .map(|i| Tool { name: format!("tool_{i:02}"), description: format!("tool number {i}"), input_schema: json!({"type": "object", "properties": {format!("p{i}"): {"type": "string"}}}), module: None, execution_mode: Default::default(), reply: None, })
        .collect::<Vec<_>>()
        .into();
    opts.tool_choice = ToolChoice::Auto.into();
    // The family REQUIRES step identity; populate it so render runs (and so the determinism
    // check exercises the requestId derivation).
    opts.step_context = Some(StepContext {
        session_address: None,
        credential_id: None,
        account_ref: None,
        run_id: "run-conf-01".into(),
        step_index: 3,
        max_output_tokens: None,
    });
    opts
}

fn frozen() -> broca_provider::wire::FrozenRenderConfig {
    let mut f = google_codeassist_frozen_config("gemini-2.5-flash");
    f.project_id = Some("proj-conf".into());
    f
}

#[test]
fn google_codeassist_passes_render_determinism() {
    // render-twice + perturbed-ambient: the requestId must be deterministic from the fixed
    // (run_id, step_index), never from clock/RNG/env.
    let report = check_render_determinism(&GoogleCodeAssist, &frozen(), &rich_request());
    assert!(
        report.passed(),
        "conformance failures: {:?}",
        report.failures()
    );
}

fn codeassist_spec() -> (ProviderSpec, ModelSpec) {
    let provider = ProviderSpec {
        id: "google".into(),
        wire_family: WireFamilyId::GoogleCodeAssist,
        base_url: String::new(),
        auth_kind: AuthKind::Bearer,
        static_headers: Default::default(),
        static_options: serde_json::Value::Null,
        quirks: Quirks {
            usage: UsagePolicy::GeminiUsageMetadata,
            token_limit_field: TokenLimitField::GeminiMaxOutputTokens,
            system_prompt: SystemPromptPolicy::SystemInstructionField,
            tool_id: ToolIdPolicy::SyntheticIndex,
            ..Default::default()
        },
        raw: serde_json::Value::Null,
    };
    let model = ModelSpec {
        id: "gemini-2.5-flash".into(),
        display_name: "Gemini 2.5 Flash".into(),
        family: Some("gemini".into()),
        capabilities: Capabilities {
            tool_call: CapabilityFlag::Known(true),
            temperature: CapabilityFlag::Known(true),
            ..Default::default()
        },
        limits: Limits::default(),
        cost: CostSchedule::default(),
        reasoning_options: Default::default(),
        server_side_fallback: None,
        input_modalities: Default::default(),
        release_date: None,
        status: None,
        overlay: Overlay::default(),
        raw: serde_json::Value::Null,
    };
    (provider, model)
}

#[test]
fn google_codeassist_freeze_survives_catalog_mutation() {
    let mut spec = codeassist_spec();
    let result = check_freeze_against_mutation(
        &GoogleCodeAssist,
        &mut spec,
        &rich_request(),
        |(p, m)| {
            // The catalog resolver doesn't fill project_id (the executor does at run start),
            // so freeze it here to match a real frozen config.
            let mut f = resolve_frozen(p, m, 2048, GOOGLE_CODEASSIST_RENDERER_VERSION)
                .expect("test specs publish known capability flags");
            f.project_id = Some("proj-conf".into());
            f
        },
        |(p, m)| {
            m.id = "gemini-3-pro".into();
            p.quirks.token_limit_field = TokenLimitField::MaxTokens;
            p.static_options = json!({"injected": "after-freeze"});
        },
    );
    assert!(result.is_ok(), "{}", result.unwrap_err());
}

/// The requestId varies by step (so each step gets its own correlation id) but is stable for
/// a given step — the property the live backend echoes and resume reproduces.
#[test]
fn request_id_is_per_step_but_stable() {
    use broca_provider::wire::WireFamily;
    let mut step2 = rich_request();
    step2.step_context = Some(StepContext {
        session_address: None,
        credential_id: None,
        account_ref: None,
        run_id: "run-conf-01".into(),
        step_index: 2,
        max_output_tokens: None,
    });
    let mut step3 = rich_request();
    step3.step_context = Some(StepContext {
        session_address: None,
        credential_id: None,
        account_ref: None,
        run_id: "run-conf-01".into(),
        step_index: 3,
        max_output_tokens: None,
    });
    let b2a = GoogleCodeAssist.render(&frozen(), &step2);
    let b2b = GoogleCodeAssist.render(&frozen(), &step2);
    let b3 = GoogleCodeAssist.render(&frozen(), &step3);
    assert_eq!(b2a, b2b, "same step → identical bytes");
    assert_ne!(
        b2a, b3,
        "different step → different requestId → different bytes"
    );
}
