//! A separate binary because live resolution installs a process-global catalog fixture.
#![cfg(all(feature = "bundled-catalog", feature = "live"))]

use std::sync::{Arc, Once};

use broca_catalog::{
    live::{provider_for_recorded_config, resolve_provider, AuthMethod, AuthMode, ResolveOptions},
    runtime::{install_global_catalog_cache, CatalogCache, CatalogSource},
    Catalog,
};
use broca_protocol::{
    content::{ContentBlock, EncryptedReasoningFormat, Message, Role},
    CallOptions, ModelHandle,
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
