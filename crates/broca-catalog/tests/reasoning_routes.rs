//! A separate binary because live resolution installs a process-global catalog fixture.
#![cfg(all(feature = "bundled-catalog", feature = "live"))]

use std::sync::{Arc, Once};

use broca_catalog::{
    live::{provider_for_recorded_config, resolve_provider, AuthMethod, AuthMode, ResolveOptions},
    runtime::{install_global_catalog_cache, CatalogCache, CatalogSource},
    Catalog,
};
use broca_protocol::{
    content::{ContentBlock, EncryptedReasoningFormat, Message, Role, ToolOutput, ToolResult},
    CallOptions, ModelHandle, NormalizedEvent, ProviderControl, StepId, ToolCallId,
};
use broca_provider::{
    families::{renderer_version_for, resolve_frozen},
    registry::wire_family,
    spec::WireFamilyId,
    wire::{AuthHandle, FrozenRenderConfig},
};
use serde_json::{json, Value};

const BASE: &str = include_str!("../data/models.dev.json");
const OVERLAY: &str = include_str!("../data/overlay.json");

fn catalog() -> Catalog {
    Catalog::from_json_str(BASE, OVERLAY).unwrap()
}

fn install() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let cache = Arc::new(CatalogCache::empty());
        cache.install_parsed_for_test(&catalog(), 1, CatalogSource::Fusiform);
        install_global_catalog_cache(cache);
    });
}

fn prompt(provider: &str, model: &str) -> CallOptions {
    CallOptions::new(vec![
        Message::new(Role::User, vec![ContentBlock::Text { text: "First".into() }]),
        Message::assistant_from(
            vec![
                ContentBlock::RedactedReasoning {
                    data: r#"{"type":"reasoning","id":"r1","summary":[],"encrypted_content":"opaque"}"#.into(),
                    provider_format: Some(EncryptedReasoningFormat::OpenAiResponses),
                },
                ContentBlock::Text { text: "Answer".into() },
            ],
            ModelHandle::new(provider, model),
        ),
        Message::new(Role::User, vec![ContentBlock::Text { text: "Next".into() }]),
    ])
}

fn golden(name: &str, bytes: &[u8]) {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(name);
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, bytes).unwrap();
    }
    assert_eq!(bytes, std::fs::read(path).unwrap());
}

fn assert_route(provider: &str, model: &str, base_url: &str) {
    install();
    let resolved = resolve_provider(
        &format!("{provider}/{model}"),
        None,
        1024,
        &AuthMode::Secret("not-a-real-key".into()),
        &ResolveOptions {
            auth_method: Some(AuthMethod::ApiKey),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(resolved.family_id, WireFamilyId::OpenAiResponses);
    assert_eq!(resolved.base_url, base_url);
    let frozen: FrozenRenderConfig =
        serde_json::from_value(resolved.provider.render_config_blob()).unwrap();
    assert_eq!(frozen.base_url.as_deref(), Some(base_url));
    assert_eq!(
        frozen.static_options,
        json!({"store": false, "include": ["reasoning.encrypted_content"]})
    );
    let bytes = resolved.provider.render_request(&prompt(provider, model));
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["store"], false);
    assert_eq!(body["include"], json!(["reasoning.encrypted_content"]));
    assert_eq!(body["input"][1]["encrypted_content"], "opaque");
    assert!(body.get("previous_response_id").is_none());
    assert_eq!(body["max_output_tokens"], 1024);
    let request =
        wire_family(frozen.family_id)
            .unwrap()
            .endpoint(&frozen, &AuthHandle::None, bytes.clone());
    assert_eq!(request.url, format!("{base_url}/responses"));
    golden(&format!("{provider}-reasoning-responses.json"), &bytes);
}

#[test]
fn xai_reasoning_route_is_stateless_responses() {
    assert_route("xai", "grok-4.5", "https://api.x.ai/v1");
}

#[test]
fn openai_api_key_reasoning_route_is_stateless_responses() {
    assert_route("openai", "gpt-5.1", "https://api.openai.com/v1");
}

#[test]
fn meta_reasoning_route_is_stateless_responses() {
    assert_route("meta", "muse-spark-1.3", "https://api.meta.ai/v1");
}

#[test]
fn responses_host_inventory_limits_new_policy_to_meta() {
    let catalog = catalog();
    let mut hosts: Vec<_> = catalog
        .provider_ids()
        .filter(|id| {
            catalog
                .provider(id)
                .is_some_and(|spec| spec.wire_family == WireFamilyId::OpenAiResponses)
        })
        .collect();
    hosts.sort();
    assert_eq!(hosts, ["meta", "openai", "xai"]);
}

#[test]
fn bedrock_admission_freezes_reasoning_without_changing_renderer_version() {
    let catalog = catalog();
    let (spec, model) = catalog
        .model_for_serving("amazon-bedrock", "amazon.nova-2-lite-v1:0")
        .unwrap();
    let frozen =
        resolve_frozen(&spec, &model, 2048, renderer_version_for(spec.wire_family)).unwrap();
    assert!(frozen.bedrock_reasoning);
    // Reasoning is metadata on the output-cap ABI, not a separate render version.
    assert_eq!(frozen.renderer_version, 4);
    let recorded = serde_json::to_value(&frozen).unwrap();
    assert_eq!(recorded["bedrock_reasoning"], true);
}

#[test]
fn meta_replays_normalized_encrypted_reasoning_across_tool_and_user_turns() {
    install();
    let resolved = resolve_provider(
        "meta/muse-spark-1.3",
        None,
        1024,
        &AuthMode::Secret("not-a-real-key".into()),
        &ResolveOptions {
            auth_method: Some(AuthMethod::ApiKey),
            ..Default::default()
        },
    )
    .unwrap();
    let frozen: FrozenRenderConfig =
        serde_json::from_value(resolved.provider.render_config_blob()).unwrap();
    // Doc-derived SSE output items from Meta's "Reasoning items in multi-turn input".
    // Ciphertext is synthetic. Completed output items must survive the actual Responses normalizer.
    // https://dev.meta.ai/docs/protocols/responses#reasoning-items
    let stream = include_bytes!("fixtures/meta_reasoning.sse");
    let events = broca_provider::conformance::normalize_recorded(
        wire_family(frozen.family_id).unwrap().as_ref(),
        &frozen,
        StepId(1),
        stream,
        7,
    )
    .unwrap();
    let content = events
        .into_iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                Some(message.content)
            }
            _ => None,
        })
        .unwrap();
    assert!(matches!(
        &content[0],
        ContentBlock::RedactedReasoning {
            provider_format: Some(EncryptedReasoningFormat::OpenAiResponses),
            ..
        }
    ));
    let opts = CallOptions::new(vec![
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "First".into(),
            }],
        ),
        Message::assistant_from(content, ModelHandle::new("meta", "muse-spark-1.3")),
        Message::new(
            Role::Tool,
            vec![ContentBlock::ToolResult(ToolResult {
                tool_call_id: ToolCallId("call_meta".into()),
                output: ToolOutput::Text { text: "7".into() },
                is_error: false,
            })],
        ),
        Message::assistant_from(
            vec![ContentBlock::Text {
                text: "Answer".into(),
            }],
            ModelHandle::new("meta", "muse-spark-1.3"),
        ),
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "Next".into(),
            }],
        ),
    ]);
    let bytes = resolved.provider.render_request(&opts);
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        body["input"][1],
        json!({"id":"rs_meta","type":"reasoning","summary":[],"encrypted_content":"synthetic-cipher"})
    );
    assert_eq!(body["input"][2]["call_id"], "call_meta");
    assert_eq!(body["input"][3]["call_id"], "call_meta");
    golden("meta-reasoning-tool-replay.json", &bytes);
}

#[test]
fn recorded_bedrock_and_meta_configs_rebuild_without_adopting_new_policy() {
    install();
    let cases: Value = serde_json::from_str(include_str!(
        "../../broca-provider/tests/fixtures/reasoning_context_legacy.json"
    ))
    .unwrap();
    for case in cases.as_array().unwrap() {
        let provider = case["provider"].as_str().unwrap();
        let frozen: FrozenRenderConfig = serde_json::from_value(case["frozen"].clone()).unwrap();
        let old_blob = serde_json::to_value(&frozen).unwrap();
        let resumed = provider_for_recorded_config(
            provider,
            "model",
            frozen,
            &AuthMode::Secret("not-a-real-key".into()),
            Some(AuthMethod::ApiKey),
            None,
            None,
            broca_provider::build_provider_http_client(),
            &broca_core::clock::Clock::default(),
        )
        .unwrap();
        assert_eq!(resumed.render_config_blob(), old_blob);
        let request = CallOptions::new(vec![
            Message::new(Role::User, vec![ContentBlock::Text { text: "question".into() }]),
            Message::assistant_from(vec![
                ContentBlock::Reasoning { text: "think".into(), signature: Some("sig".into()) },
                ContentBlock::RedactedReasoning { data: "AQID".into(), provider_format: None },
                ContentBlock::RedactedReasoning {
                    data: r#"{"id":"r1","type":"reasoning","summary":[],"encrypted_content":"cipher","status":"completed"}"#.into(),
                    provider_format: Some(EncryptedReasoningFormat::OpenAiResponses),
                },
                ContentBlock::Text { text: "answer".into() },
            ], ModelHandle::new(provider, "model")),
            Message::new(Role::User, vec![ContentBlock::Text { text: "next".into() }]),
        ]);
        assert_eq!(
            resumed.render_request(&request),
            case["body"].as_str().unwrap().as_bytes()
        );
        let body: Value = serde_json::from_slice(&resumed.render_request(&request)).unwrap();
        assert!(body.get("store").is_none());
        assert!(body.get("include").is_none());
    }
}

#[test]
fn non_reasoning_snapshot_models_keep_chat() {
    let catalog = catalog();
    for (provider, model) in [("xai", "grok-composer-2.5-fast"), ("openai", "gpt-4.1")] {
        let (spec, _) = catalog.model_for_serving(provider, model).unwrap();
        assert_eq!(spec.wire_family, WireFamilyId::OpenAiChat);
        assert!(spec.static_options.is_null());
    }
}

#[test]
fn old_chat_run_resumes_byte_identically() {
    install();
    let mut old_overlay: Value = serde_json::from_str(OVERLAY).unwrap();
    for provider in ["xai", "openai"] {
        old_overlay["patch"][provider]["wire_family"] = json!("openai_chat");
    }
    let old_catalog = Catalog::from_json_str(BASE, &old_overlay.to_string()).unwrap();
    for (provider, model) in [("xai", "grok-4.5"), ("openai", "gpt-5.1")] {
        let (spec, model_spec) = old_catalog.model_for_serving(provider, model).unwrap();
        let frozen = resolve_frozen(
            &spec,
            &model_spec,
            1024,
            renderer_version_for(spec.wire_family),
        )
        .unwrap();
        let recorded: FrozenRenderConfig =
            serde_json::from_value(serde_json::to_value(&frozen).unwrap()).unwrap();
        let opts = prompt(provider, model);
        let bytes = wire_family(frozen.family_id)
            .unwrap()
            .render(&frozen, &opts);
        golden(&format!("{provider}-reasoning-legacy-chat.json"), &bytes);
        let resumed = provider_for_recorded_config(
            provider,
            model,
            recorded,
            &AuthMode::Secret("not-a-real-key".into()),
            Some(AuthMethod::ApiKey),
            None,
            None,
            broca_provider::build_provider_http_client(),
            &broca_core::clock::Clock::default(),
        )
        .unwrap();
        assert_eq!(resumed.render_request(&opts), bytes);
        assert_eq!(
            resumed.render_config_blob(),
            serde_json::to_value(&frozen).unwrap()
        );
        let endpoint =
            wire_family(frozen.family_id)
                .unwrap()
                .endpoint(&frozen, &AuthHandle::None, bytes);
        assert_eq!(endpoint.url, format!("{}/chat/completions", spec.base_url));
    }
}
