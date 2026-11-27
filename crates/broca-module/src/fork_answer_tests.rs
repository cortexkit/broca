use super::*;

#[test]
fn fork_sessions_are_named_after_their_head() {
    let head = BindIdentity::new("/p", "opencode", "ses_head");
    let fork = fork_session(&head, "fork-0123456789abcdef0123456789abcdef");
    assert_eq!(
        fork.session,
        "ses_head:fork:fork-0123456789abcdef0123456789abcdef"
    );
    assert_eq!(
        (fork.project_root.clone(), fork.harness.clone()),
        (head.project_root.clone(), head.harness.clone())
    );
    assert_eq!(
        head_of(&fork, "fork-0123456789abcdef0123456789abcdef"),
        head
    );
}

#[test]
fn only_minted_fork_ids_name_a_session() {
    let minted = mint_fork_id().unwrap();
    assert!(minted_fork_id(&minted), "{minted}");
    assert_ne!(minted, mint_fork_id().unwrap());
    for foreign in [
        "",
        "fork-",
        "fork-0123456789ABCDEF0123456789abcdef",
        "fork-0123456789abcdef0123456789abcde",
        "fork-0123456789abcdef0123456789abcdef0",
        "../fork-0123456789abcdef0123456789abcdef",
        "x-0123456789abcdef0123456789abcdef",
    ] {
        assert!(!minted_fork_id(foreign), "{foreign}");
    }
}

#[test]
fn account_refusals_carry_the_contract_detail() {
    let held = ForkError::AccountHeld {
        routing_code: None,
        mismatch: Some("credential".into()),
    };
    assert_eq!(held.code(), "fork_account_held");
    assert_eq!(held.detail(), Some(json!({"mismatch": "credential"})));
    let blocked = ForkError::AccountBlocked {
        routing_code: "no_account".into(),
    };
    assert_eq!(
        blocked.detail(),
        Some(json!({"routing_code": "no_account"}))
    );
    assert_eq!(ForkError::UnknownFork.detail(), None);
}

/// An ask's hint lowers its answer's effort through the provider's rule, read from the render
/// config the base's run froze; a run with none lowers nothing.
#[test]
fn an_ask_lowers_its_effort_from_the_base_runs_frozen_config() {
    use broca_provider::variant::{FrozenVariant, VariantRequest};
    let mut opus = broca_provider::families::anthropic_apikey_frozen_config("claude-opus-5-5");
    opus.effort_change_cache = true;
    opus.variant = Some(FrozenVariant {
        name: "high".into(),
        request: VariantRequest::Anthropic {
            thinking: Some(json!({"type": "adaptive"})),
            effort: Some("high".into()),
        },
    });
    let frozen = serde_json::to_value(&opus).unwrap();
    assert_eq!(lowered_effort(&frozen, "low").as_deref(), Some("low"));
    assert_eq!(lowered_effort(&frozen, "high"), None);
    assert_eq!(lowered_effort(&Value::Null, "low"), None);
}

#[test]
fn note_free_fork_matches_the_0149_primary_and_actual_fallback_bytes() {
    use broca_protocol::{CallOptions, ContentBlock, Message, ModelHandle, Role};
    use broca_provider::{
        families::anthropic_apikey_frozen_config,
        wire::{FrozenRenderConfig, RefusalFallbackConfig},
    };
    let mut primary = anthropic_apikey_frozen_config("claude-opus-5-5");
    let mut fallback = anthropic_apikey_frozen_config("claude-opus-4-8");
    primary.cache = None;
    fallback.cache = None;
    let source = ModelHandle::new("anthropic", "claude-opus-5-5");
    let target = ModelHandle::new("anthropic", "claude-opus-4-8");
    let mut opts = CallOptions::new(vec![Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: "fork question".into(),
        }],
    )]);
    opts.target_model = Some(source.clone());
    let config = |frozen: &FrozenRenderConfig| {
        serde_json::from_value(json!({"model":source,"tools":[],"tool_choice":broca_protocol::request::ToolChoice::Auto,"generation":broca_protocol::request::GenerationConfig::default(),"response_format":broca_protocol::request::ResponseFormat::default(),"context_epoch":0,"renderer_version":2,"frozen_render_config":frozen})).unwrap()
    };
    let primary_pin = br#"{"max_tokens":2048,"messages":[{"content":[{"text":"fork question","type":"text"}],"role":"user"}],"model":"claude-opus-5-5","stream":true,"system":[]}"#;
    assert_eq!(render(&config(&primary), &opts).unwrap(), primary_pin);
    primary.refusal_fallback = Some(Box::new(RefusalFallbackConfig {
        model: target.clone(),
        context_limit: None,
        frozen: fallback,
    }));
    assert_eq!(render(&config(&primary), &opts).unwrap(), primary_pin);
    opts.target_model = Some(target);
    // 0.4.149's no-media fork helper incorrectly selected the primary for a routed retry.
    // The pin is the exact body its fallback renderer sent, not that incorrect reconstruction.
    let fallback_pin = br#"{"max_tokens":2048,"messages":[{"content":[{"text":"fork question","type":"text"}],"role":"user"}],"model":"claude-opus-4-8","stream":true,"system":[]}"#;
    assert_eq!(render(&config(&primary), &opts).unwrap(), fallback_pin);
}
