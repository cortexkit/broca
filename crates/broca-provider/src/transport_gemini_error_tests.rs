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

/// Google's account quota wall (a 429 whose ErrorInfo reason is `QUOTA_EXHAUSTED`) can arrive as
/// the terminal record of a 2xx stream instead of an HTTP 429. It is the same wall either way:
/// the step ends after one attempt, the error keeps its reset, and the attempt records the wall
/// for the account, so the run never re-asks the exhausted account within the step.
async fn assert_in_band_quota_wall<F: WireFamily + 'static>(family: F, id: WireFamilyId) {
    const SUCCESS: &str = r#"{"candidates":[{"content":{"role":"model","parts":[{"text":"hi"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1}}"#;
    let wrap = |body: &str| -> &'static [u8] {
        let record = if id == WireFamilyId::GoogleCodeAssist {
            format!("data: {{\"response\":{body}}}\n\n")
        } else {
            format!("data: {body}\n\n")
        };
        Box::leak(record.into_boxed_str()).as_bytes()
    };
    // The second response is what a re-send would get; a wall must leave it unused.
    let server = ScriptedServer::start(vec![
        ScriptResponse::Http {
            status: 200,
            headers: vec![("content-type", "text/event-stream")],
            body: wrap(CODE_ASSIST_QUOTA_DELAY_ONLY_BODY),
        },
        ScriptResponse::Http {
            status: 200,
            headers: vec![("content-type", "text/event-stream")],
            body: wrap(SUCCESS),
        },
    ])
    .await;
    let mut frozen = frozen_config_for_family(id, "model");
    frozen.base_url = Some(server.url.clone());
    frozen.project_id = Some("test-project".into());
    let provider = LiveProvider::new(family, frozen, AuthHandle::None);
    let before_ms = crate::transport::now_millis();
    let mut opts = CallOptions::new(Vec::new());
    opts.step_context = Some(StepContext {
        session_address: None,
        credential_id: None,
        account_ref: None,
        run_id: "gemini-quota-wall-test".into(),
        step_index: 1,
        max_output_tokens: None,
    });
    let mut rx = provider.start_step(
        StepId(1),
        Arc::new(opts),
        CancellationToken::new(),
        None,
    );
    let events = tokio::time::timeout(Duration::from_secs(30), async {
        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(event);
        }
        events
    })
    .await
    .expect("the step ends");
    let after_ms = crate::transport::now_millis();
    assert_eq!(server.request_bodies().len(), 1, "{id:?}: never re-sent within the step");
    let error = terminal_error(&events);
    assert_eq!(error.class, ErrorClass::Transient, "{id:?}");
    assert!(error.is_account_limit_exhausted(), "{id:?}: {error:?}");
    assert_eq!(error.status, Some(429));
    assert_eq!(error.resets_at_source.as_deref(), Some("retry_after"));
    let resets_at_ms = error.resets_at_ms.expect("reset from the stated delay");
    assert!(
        (before_ms + CODE_ASSIST_QUOTA_DELAY_MS..=after_ms + CODE_ASSIST_QUOTA_DELAY_MS)
            .contains(&resets_at_ms),
        "{id:?}: {resets_at_ms}"
    );
    let limit = events
        .iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }) => {
                attempt.account_observation.limit.as_ref()
            }
            _ => None,
        })
        .expect("the attempt records the account wall");
    assert_eq!(
        limit.disposition,
        broca_protocol::provider::AccountLimitDisposition::ACCOUNT
    );
    assert_eq!(limit.resets_at_ms, Some(resets_at_ms));
}

#[tokio::test]
async fn gemini_and_codeassist_in_band_quota_wall_is_an_account_wall() {
    assert_in_band_quota_wall(Gemini, WireFamilyId::Gemini).await;
    assert_in_band_quota_wall(GoogleCodeAssist, WireFamilyId::GoogleCodeAssist).await;
}
