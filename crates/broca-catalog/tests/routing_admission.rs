//! Routing is chosen before a new run is recorded; resumes reuse the complete recorded bundle.
#![cfg(feature = "live")]

use std::sync::Arc;

use broca_catalog::{
    live::{
        AuthMethod, AuthMode, EpisodeRenderConfig, FrozenRenderConfig, ResolveOptions, WireFamilyId,
    },
    CatalogCache, FusiformCatalogResponse,
};
use broca_protocol::{
    content::{ContentBlock, Message, Role},
    CallOptions, StepContext,
};
use serde_json::{json, Value};

fn resolve(
    model: &str,
    opts: &ResolveOptions<'_>,
    recorded: Option<&FrozenRenderConfig>,
) -> Result<broca_catalog::live::ProviderResolution, broca_catalog::live::ResolutionError> {
    broca_catalog::live::resolve_catalog_episode_with_client(
        model,
        Some(1024),
        &AuthMode::None,
        opts,
        broca_provider::build_provider_http_client(),
        EpisodeRenderConfig {
            recorded_config: recorded,
            opencode_go_session_id: Some("routing-test"),
            ..Default::default()
        },
    )
}

#[test]
fn facts_drive_admission_and_recorded_bytes_survive_refresh() {
    let mut response: FusiformCatalogResponse = serde_json::from_str(include_str!(
        "fixtures/routing-parity/fusiform-served-with-wire-facts.json"
    ))
    .unwrap();
    let withheld: FusiformCatalogResponse =
        serde_json::from_str(include_str!("fixtures/fusiform_withheld_opencode_go.json")).unwrap();
    response.models.extend(withheld.models);
    response
        .models
        .get_mut("deepseek/deepseek-flash")
        .unwrap()
        .insert(
            "wire.api".into(),
            json!({"value":"https://fact.example/v1", "from":"future-source"}),
        );
    let mut alias = response.models["google/antigravity-gemini-3.8-flash"].clone();
    alias.insert("route.reasoning_wire_ids".into(), json!({"low":"opaque-backend-low", "medium":"opaque-backend-default", "high":"opaque-backend-high"}));
    response
        .models
        .insert("google/antigravity-fixture-route".into(), alias);
    // Corrections explain upstream changes; only the model's facts select a route.
    response.wire_corrections = vec![
        json!({"model":"minimax-m2.7", "field":"wire.api", "value":"https://must-not-route.example", "withheld":false}),
        json!({"model":"deepseek-flash", "field":"wire.api", "value":"https://must-not-route.example", "withheld":true}),
    ];
    let cache = Arc::new(CatalogCache::empty());
    cache.refresh(response.clone()).unwrap();
    broca_catalog::install_global_catalog_cache(cache.clone());

    let error = resolve("opencode-go/minimax-m2.7", &ResolveOptions::default(), None)
        .err()
        .expect("withheld endpoint refuses");
    assert!(error.to_string().contains("opencode-go/minimax-m2.7"));
    assert!(error.to_string().contains("missing wire.api"), "{error}");
    let meta = resolve(
        "meta/muse-wonder-full-1.0",
        &ResolveOptions {
            auth_method: Some(AuthMethod::Oauth),
            ..Default::default()
        },
        None,
    )
    .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(
        meta.family_id,
        WireFamilyId::OpenAiResponses,
        "Meta credential persona"
    );
    assert_eq!(
        meta.base_url, "https://api.meta.ai/v1",
        "Meta credential persona"
    );
    let routed = resolve("deepseek/deepseek-flash", &ResolveOptions::default(), None)
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(routed.base_url, "https://fact.example/v1");
    assert_eq!(routed.family_id, WireFamilyId::OpenAiChat);
    let opts = ResolveOptions {
        family_override: Some(WireFamilyId::GoogleCodeAssist),
        auth_method: Some(AuthMethod::Antigravity),
        project_id: Some("routing-project"),
        variant: Some("high"),
        ..Default::default()
    };
    let routed =
        resolve("google/antigravity-fixture-route", &opts, None).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(routed.wire_model, "opaque-backend-high");
    let frozen: FrozenRenderConfig =
        serde_json::from_value(routed.provider.render_config_blob()).unwrap();
    let mut prompt = CallOptions::new(vec![Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: "Hello".into(),
        }],
    )]);
    prompt.step_context = Some(StepContext {
        session_address: None,
        credential_id: None,
        account_ref: None,
        run_id: "routing-run".into(),
        step_index: 0,
        max_output_tokens: None,
    });
    let bytes = routed.provider.render_request(&prompt);
    assert_eq!(
        serde_json::from_slice::<Value>(&bytes).unwrap()["model"],
        "opaque-backend-high"
    );

    response.catalog_version += 1;
    response.resolved_at_ms += 1;
    let facts = response
        .models
        .get_mut("google/antigravity-fixture-route")
        .unwrap();
    facts.clear();
    facts.insert("existence".into(), json!("absent"));
    facts.insert(
        "successor".into(),
        json!("google/antigravity-gemini-4-argon"),
    );
    cache.refresh(response).unwrap();
    let error = resolve("google/antigravity-fixture-route", &opts, None)
        .err()
        .expect("fresh send refuses withdrawal");
    assert!(
        error
            .to_string()
            .contains("use 'google/antigravity-gemini-4-argon' instead"),
        "{error}"
    );
    let resumed = resolve("google/antigravity-fixture-route", &opts, Some(&frozen))
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(
        resumed.provider.render_config_blob(),
        routed.provider.render_config_blob()
    );
    assert_eq!(resumed.provider.render_request(&prompt), bytes);
}
