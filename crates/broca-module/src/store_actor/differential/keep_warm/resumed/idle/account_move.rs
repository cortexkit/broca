//! Rebuilt warm requests retain the config and account of the attempt being replayed.

use super::*;

cipher_tests!(
    idle_warm_uses_moved_account_and_recorded_bytes,
    recorded_request_uses_config_at_its_attempt,
    recorded_request_without_refreezes_keeps_pinned_bytes,
);

fn selection(account: &str) -> broca_records::record::AuthSelection {
    serde_json::from_value(json!({
        "credential_id": format!("cred-{account}"),
        "resolved_credential_id": format!("cred-{account}"),
        "account_ref": account,
        "auth_method": "oauth",
        "account_decision": {
            "path": "routed", "admission_id": "r1",
            "decision_id": format!("decision-{account}"),
            "conversation_key": "broca-session:conversation",
        },
    }))
    .unwrap()
}

fn frozen(account: &str) -> broca_provider::wire::FrozenRenderConfig {
    let mut frozen = warmable();
    frozen.claude_code_persona = Some(broca_provider::wire::ClaudeCodePersona {
        version: "2.1.280".into(),
        device_id: "0".repeat(64),
        account_uuid: Some(account.into()),
        session_id: "11111111-1111-4111-8111-111111111111".into(),
        prompt_id: Some("33333333-3333-4333-8333-333333333333".into()),
        cc_version_suffix: Some("e29".into()),
        prompt_ordinal: Some(1),
    });
    frozen
}

fn render(request: &broca_core::RecordedRequest) -> Vec<u8> {
    let provider = broca_provider::LiveProvider::new(
        broca_provider::families::AnthropicMessages,
        serde_json::from_value(request.config.frozen_render_config.clone()).unwrap(),
        broca_provider::wire::AuthHandle::Bearer("test-token".into()),
    );
    broca_core::provider::Provider::render_request(&provider, &request.opts)
}

/// The `moved-account` history and the requests its three attempts sent. Its first step ran
/// on account A; its second hit A's usage limit, moved to B, and succeeded there.
fn moved_history() -> (Converted, Vec<Vec<u8>>) {
    let mut history = converted("moved-account");
    let bodies = std::mem::take(&mut history.bodies);
    (history, bodies)
}

async fn idle_warm_uses_moved_account_and_recorded_bytes(cipher: Cipher) {
    let side = super::super::super::side(cipher).await;
    let (converted, bodies) = moved_history();
    let clock = Arc::new(AtomicU64::new(crate::actor::epoch_ms()));
    let executor = Arc::new(Warming {
        clock: Some(clock.clone()),
        ..Default::default()
    });
    let read_clock = clock.clone();
    let mut actor = reopened(&side, converted, cipher, executor.clone())
        .await
        .with_clock(Arc::new(move || read_clock.load(Ordering::SeqCst)));
    let rebuilt = actor.recorded_warm_request().await.unwrap();
    assert_eq!(rebuilt.step_id.0, 2);
    assert_eq!(render(&rebuilt), bodies[2]);
    assert_eq!(
        rebuilt.request_body_sha256,
        broca_core::render::request_digest_hex(&bodies[2])
    );
    assert_ne!(bodies[1], bodies[2], "the account changes request identity");
    clock.store(actor.idle_warm_due().await.unwrap(), Ordering::SeqCst);
    assert!(matches!(
        warm(&mut actor).await,
        crate::actor::WarmOutcome::Warmed(_)
    ));
    assert_eq!(*executor.sent.lock().unwrap(), vec![bodies[2].clone()]);
    for configs in [&executor.quota_configs, &executor.warm_configs] {
        let configs = configs.lock().unwrap();
        assert_eq!(configs.len(), 1);
        let config = &configs[0];
        assert_eq!(config.auth_selection.as_ref().unwrap(), &selection("B"));
        assert_eq!(
            config.frozen_render_config,
            serde_json::to_value(frozen("B")).unwrap()
        );
    }
    let body: serde_json::Value = serde_json::from_slice(&bodies[2]).unwrap();
    let identity: serde_json::Value =
        serde_json::from_str(body["metadata"]["user_id"].as_str().unwrap()).unwrap();
    assert_eq!(identity["account_uuid"], "B");
    side.registry.stop_and_seal().await;
}

async fn recorded_request_uses_config_at_its_attempt(cipher: Cipher) {
    let side = super::super::super::side(cipher).await;
    let (converted, bodies) = moved_history();
    let actor = reopened(&side, converted, cipher, Arc::new(Warming::default())).await;
    // The current account selection reflects the later move. Rebuilding an older attempt must
    // not borrow that account.
    let stored = actor.session.open_log_records().await.unwrap();
    let attempts: Vec<_> = stored
        .iter()
        .enumerate()
        .filter_map(|(at, (_, record))| matches!(record, Record::Attempt(_)).then_some(at))
        .collect();
    assert_eq!(attempts.len(), 3);
    for (index, at) in attempts.into_iter().enumerate() {
        let rebuilt = crate::store_actor::recorded_warm::request(
            &actor.session,
            &stored[..=at],
            &actor.parts,
            &bind(),
            "r1",
            None,
        )
        .await
        .unwrap();
        let account = if index < 2 { "A" } else { "B" };
        assert_eq!(
            rebuilt.config.auth_selection.as_ref().unwrap(),
            &selection(account)
        );
        assert_eq!(
            rebuilt.config.frozen_render_config,
            serde_json::to_value(frozen(account)).unwrap()
        );
        assert_eq!(render(&rebuilt), bodies[index]);
        assert_eq!(
            rebuilt.request_body_sha256,
            broca_core::render::request_digest_hex(&bodies[index])
        );
    }
    side.registry.stop_and_seal().await;
}

async fn recorded_request_without_refreezes_keeps_pinned_bytes(cipher: Cipher) {
    let side = super::super::super::side(cipher).await;
    let actor = head(
        &side,
        cipher,
        Arc::new(Warming::default()),
        converted("finished-head"),
        Arc::new(AtomicU64::new(crate::actor::epoch_ms())),
    )
    .await;
    let rebuilt = actor.recorded_warm_request().await.unwrap();
    let bytes = render(&rebuilt);
    assert_eq!(
        broca_core::render::request_digest_hex(&bytes),
        "be64e9208c5939f55503d36ecc0a1e421c4098cf3e26121961fd29847ad2ef5a"
    );
    assert_eq!(
        broca_core::render::request_digest_hex(&bytes),
        rebuilt.request_body_sha256
    );
    side.registry.stop_and_seal().await;
}
