fn cached_attempt(account: &str, status: u32, sent: u64) -> broca_session_store::record::Record {
    use broca_session_store::{codec::Digest, record::{Attempt, Record}};
    Record::Attempt(Attempt {
        run_id: "cached-run".into(), step_id: 1, attempt: 0, attempt_id: None,
        outcome: "complete".into(), http_status: Some(status),
        request_body_len: 0, request_body_sha256: Digest([0; 32]),
        response_body_len: 0, response_body_sha256: Digest([0; 32]),
        ts_ms: sent + 10, sent_at_ms: Some(sent), first_content_at_ms: None,
        last_content_at_ms: None, transport: None, served_account_ref: Some(account.into()),
        billing: None, telemetry: None, reached_cache: Some((200..300).contains(&status)),
        requested_cache_ttl_ms: None,
        tools_digest: None,
        system_digest: None,
        signed_floor: None,
    })
}

#[tokio::test]
async fn a_restarted_store_session_loads_hold_cap_evidence_before_admission() {
    use broca_core::provider::AcceptedSendEvidence;
    let mut session = store_session().await;
    session.append(&[
        cached_attempt("acct-old", 200, 1),
        cached_attempt("acct-a", 200, 900_000),
        cached_attempt("acct-b", 200, 910_000),
        cached_attempt("acct-a", 429, 990_000),
    ], true).await.unwrap();
    let backend = *session.backend();
    let address = session.address().clone();
    session.close().await.unwrap();
    let session = broca_session_store::session::Session::load(
        backend, address,
        &broca_session_store::head::SessionTriple {
            project_root: "/project".into(), harness: "test".into(), session: "session".into(),
        },
        &broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
    ).await.unwrap();
    let client = broca_provider::build_provider_http_client();
    let bind = broca_protocol::BindIdentity::new("/project", "test", "session");
    assert_eq!(client.session_accepted_send_evidence(&bind, "acct-a"), AcceptedSendEvidence::NotLoaded);
    assert_eq!(client.session_last_accepted_at_ms(&bind, "acct-a"), None);
    let frozen = serde_json::json!({});
    let config = broca_records::record::RunConfig {
        account_route: None, global_system: None, route_decision_id: None, served_provider: None, account_limit_pause: true,
        turn_directives_v1: None, render_tags: false, computed_tags: None,
        model: broca_protocol::request::ModelHandle { provider_module_id: "test".into(), model_id: "test".into() },
        tools: vec![].into(), tool_choice: Default::default(), generation: Default::default(),
        response_format: Default::default(), provider_options: serde_json::Value::Null,
        cache_tiers: serde_json::Value::Null, context_epoch: 0, renderer_version: 1,
        frozen_render_config: frozen.clone(),
        auth_selection: Some(serde_json::from_value(serde_json::json!({
            "credential_id": "oauth:test:acct-a", "account_ref": "acct-a"
        })).unwrap()),
        tools_were_omitted: false, budget: None, context_limit: None, work_class: None,
        variant: None, service_tier: None, stop_when: vec![], context_management: None,
    };
    let hold = || broca_module_serve::account_routing::account_hold_cap(
        broca_core::SameAccountWait::Max, Some(&config), 1_000_000,
        |account| client.session_last_accepted_at_ms(&bind, account),
    );
    assert!(hold().evidence_unknown);
    let evidence = seed_store_accepted_send(&client, &session, &bind, "acct-a", &frozen, 1_000_000).await.unwrap();
    assert_eq!(evidence, AcceptedSendEvidence::At(900_000));
    assert_eq!(client.session_last_accepted_at_ms(&bind, "acct-a"), Some(Some(900_000)));
    assert_eq!(hold().max_s, 200);
    assert!(!hold().evidence_unknown);
    assert_eq!(client.session_last_accepted_at_ms(&bind, "acct-b"), None, "other accounts remain unloaded");
    let expiry = broca_core::session_account_cache_expiry_ms(
        client.session_last_accepted_at_ms(&bind, "acct-a").unwrap(), &frozen,
    );
    assert_eq!(expiry, Some(1_200_000), "the refusal must not refresh the accepted-send clock");
    assert_eq!(seed_store_accepted_send(&client, &session, &bind, "acct-missing", &frozen, 1_000_000).await.unwrap(), AcceptedSendEvidence::NoneWithinWindow);
    let empty = store_session().await;
    let empty_bind = broca_protocol::BindIdentity::new("/project", "test", "empty");
    assert_eq!(seed_store_accepted_send(&client, &empty, &empty_bind, "acct-a", &frozen, 1_000_000).await.unwrap(), AcceptedSendEvidence::None);
    assert_eq!(client.session_last_accepted_at_ms(&empty_bind, "acct-a"), Some(None));
}
