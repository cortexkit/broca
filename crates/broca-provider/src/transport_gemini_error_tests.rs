use super::*;

async fn assert_permission_not_retried<F: WireFamily + 'static>(family: F, id: WireFamilyId) {
    let body = if id == WireFamilyId::GoogleCodeAssist {
        r#"data: {"response":{"error":{"code":403,"status":"PERMISSION_DENIED","message":"denied"}}}

"#
    } else {
        "data: {\"error\":{\"code\":403,\"status\":\"PERMISSION_DENIED\",\"message\":\"denied\"}}\n\n"
    };
    let server = ScriptedServer::start(vec![ScriptResponse::Http {
        status: 200,
        headers: vec![("content-type", "text/event-stream")],
        body: body.as_bytes(),
    }])
    .await;
    let mut frozen = frozen_config_for_family(id, "model");
    frozen.base_url = Some(server.url.clone());
    frozen.project_id = Some("test-project".into());
    let provider = LiveProvider::new(family, frozen, AuthHandle::None);
    let mut opts = CallOptions::new(Vec::new());
    opts.step_context = Some(StepContext { session_address: None, credential_id: None, account_ref: None, run_id: "gemini-error-test".into(),
    step_index: 1, max_output_tokens: None, });
    let mut rx = provider.start_step(
        StepId(1),
        Arc::new(opts),
        CancellationToken::new(),
        None,
    );
    let events = tokio::time::timeout(Duration::from_secs(10), async {
        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(event);
        }
        events
    })
    .await
    .expect("permission denial must terminate without retry");
    let error = terminal_error(&events);
    assert_eq!(error.class, ErrorClass::Permanent);
    assert_eq!(error.provider_code.as_deref(), Some("PERMISSION_DENIED"));
    assert_eq!(error.status, Some(403));
    assert_eq!(error.message, "denied");
    assert_eq!(server.request_bodies().len(), 1);
    assert!(events.iter().any(|event| matches!(
        event,
        NormalizedEvent::Control(ProviderControl::Error { retries_used: 0, .. })
    )));
}

#[tokio::test]
async fn gemini_and_codeassist_in_band_errors_keep_class_code_and_detail() {
    for (family, id) in [
        (&Gemini as &dyn WireFamily, WireFamilyId::Gemini),
        (&GoogleCodeAssist as &dyn WireFamily, WireFamilyId::GoogleCodeAssist),
    ] {
        for (status, code, detail, class) in [
            (403, "PERMISSION_DENIED", "denied", ErrorClass::Permanent),
            (500, "INTERNAL", "backend failed", ErrorClass::Transient),
            (429, "RESOURCE_EXHAUSTED", "daily quota exhausted", ErrorClass::Transient),
        ] {
            let error = serde_json::json!({"error": {"code": status, "status": code, "message": detail}});
            let value = if id == WireFamilyId::GoogleCodeAssist {
                serde_json::json!({"response": error})
            } else {
                error
            };
            let frozen = frozen_config_for_family(id, "model");
            let mut normalizer = family.normalizer(&frozen, StepId(1));
            let mut events = normalizer.push(format!("data: {value}\n\n").as_bytes()).unwrap();
            events.extend(normalizer.finish().expect("error envelope is terminal, not truncated"));
            let error = terminal_error(&events);
            assert_eq!(error.class, class, "{id:?} {code}");
            assert_eq!(error.status, Some(status));
            assert_eq!(error.provider_code.as_deref(), Some(code));
            assert_eq!(error.message, detail);
            assert_eq!(error.class.step_reissuable(), class == ErrorClass::Transient);
            assert!(!events.iter().any(|event| matches!(event,
                NormalizedEvent::Control(ProviderControl::Finished { .. }))));
        }
    }
    assert_permission_not_retried(Gemini, WireFamilyId::Gemini).await;
    assert_permission_not_retried(GoogleCodeAssist, WireFamilyId::GoogleCodeAssist).await;
}
