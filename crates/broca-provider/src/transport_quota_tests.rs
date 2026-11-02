// Included in transport's test module to reuse its scripted HTTP server.

#[tokio::test]
async fn quota_transport_headers_tapped_on_success_and_refusal() {
    for status in [200, 429] {
        let server = ScriptedServer::start(vec![ScriptResponse::Http {
            status,
            headers: vec![
                ("anthropic-ratelimit-unified-status", "allowed_warning"),
                ("x-codex-primary-used-percent", "100"),
            ],
            body: b"{}",
        }])
        .await;
        let capture = Arc::new(crate::quota::tests::Capture::default());
        let mut request = super::PreparedHttpRequest::from(HttpRequest {
            method: HttpMethod::Post,
            url: server.url.clone(),
            headers: vec![],
            body: b"{}".to_vec(),
        });
        request.quota_observer = Some(capture.clone());
        let mut trace = super::AttemptTrace {
            quota_observer: request.quota_observer.clone(),
            ..Default::default()
        };
        let (tx, _rx) = mpsc::channel(8);
        let before = super::now_millis();
        let result = super::drive_attempt(
            &super::build_provider_http_client(),
            &request,
            &Default::default(),
            Box::new(SuccessAttemptNormalizer),
            &tx,
            Duration::from_secs(5),
            &mut trace,
        )
        .await;
        assert_eq!(result.is_ok(), status == 200);
        let headers = capture.headers.lock().unwrap();
        assert_eq!(headers.len(), 1);
        assert_eq!(
            headers[0].0["anthropic-ratelimit-unified-status"],
            "allowed_warning"
        );
        assert_eq!(headers[0].0["x-codex-primary-used-percent"], "100");
        assert!(headers[0].1 >= before && headers[0].1 <= super::now_millis());
    }
}

#[tokio::test]
async fn quota_sse_tap_preserves_frame_and_responses_normalizer_output() {
    let base = concat!(
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_quota\"}}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_quota\",\"status\":\"completed\"}}\n\n",
    );
    let frame = r#"{ "type": "codex.rate_limits", "rate_limits": {"used_percent":1.000e+00} }"#;
    let with_frame = format!("data: {frame}\n\n{base}");
    async fn normalized(
        body: &str,
        capture: Option<Arc<dyn crate::quota::QuotaObserver>>,
    ) -> Vec<NormalizedEvent> {
        let (tx, mut rx) = mpsc::channel(32);
        let mut trace = super::AttemptTrace {
            quota_observer: capture,
            ..Default::default()
        };
        // Split both the SSE field and JSON body across chunks, like a network read.
        let chunks: Vec<Result<Vec<u8>, std::io::Error>> =
            body.as_bytes().chunks(7).map(|b| Ok(b.to_vec())).collect();
        let complete = super::drive_body_stream_traced(
            futures::stream::iter(chunks),
            OpenAiResponses.normalizer(&openai_responses_frozen_config("m"), StepId(1)),
            &tx,
            usize::MAX,
            &mut trace,
        )
        .await
        .unwrap();
        drop(tx);
        let mut out = Vec::new();
        while let Some(event) = rx.recv().await {
            out.push(event);
        }
        out.extend(complete);
        out
    }
    let capture = Arc::new(crate::quota::tests::Capture::default());
    let original = normalized(base, None).await;
    let with_tap = normalized(&with_frame, Some(capture.clone())).await;
    let without_tap = normalized(&with_frame, None).await;
    assert_eq!(original, with_tap);
    assert_eq!(original, without_tap);
    assert_eq!(capture.frames.lock().unwrap().len(), 1);
    assert_eq!(capture.frames.lock().unwrap()[0].0, frame);
}

#[tokio::test]
async fn quota_provider_options_reach_live_attempts() {
    let server = ScriptedServer::start(vec![ScriptResponse::Http {
        status: 200,
        headers: vec![("x-codex-primary-used-percent", "100")],
        body: concat!(
            "data: {\"type\":\"codex.rate_limits\",\"rate_limits\":null}\n\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_quota\"}}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_quota\",\"status\":\"completed\"}}\n\n",
        ).as_bytes(),
    }]).await;
    let capture = Arc::new(crate::quota::tests::Capture::default());
    let mut frozen = openai_responses_frozen_config("m");
    frozen.base_url = Some(server.url.clone());
    let provider = crate::factory::live_provider_with_options(
        None,
        frozen,
        AuthHandle::None,
        crate::factory::ProviderOptions {
            quota_observer: Some(capture.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    let mut events = provider.step(StepId(1), &CallOptions::new(vec![])).await;
    while events.recv().await.is_some() {}
    assert_eq!(capture.headers.lock().unwrap().len(), 1);
    assert_eq!(capture.frames.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn quota_recorded_warm_headers_and_sse_are_tapped() {
    for status in [200, 429] {
        let server = ScriptedServer::start(vec![ScriptResponse::Http {
            status,
            headers: vec![("x-codex-primary-used-percent", "100")],
            body: b"data: {\"type\":\"codex.rate_limits\"}\n\n",
        }])
        .await;
        let capture = Arc::new(crate::quota::tests::Capture::default());
        let mut client = super::build_provider_http_client();
        client.warm_quota_observer = Some(capture.clone());
        let request = HttpRequest {
            method: HttpMethod::Post,
            url: server.url.clone(),
            headers: vec![],
            body: b"{}".to_vec(),
        };
        let result = super::send_warm_legacy(
            &client,
            &Default::default(),
            request,
            WireFamilyId::OpenAiResponsesChatgpt,
            None,
        )
        .await;
        assert_eq!(result.is_ok(), status == 200);
        assert_eq!(capture.headers.lock().unwrap().len(), 1);
        assert_eq!(
            capture.frames.lock().unwrap()[0].0,
            r#"{"type":"codex.rate_limits"}"#
        );
    }
}

#[test]
fn quota_normalizer_borrows_records_and_forwards_two_chunk_quota_once() {
    for family in [
        WireFamilyId::OpenAiResponses,
        WireFamilyId::OpenAiResponsesChatgpt,
    ] {
        let frozen = frozen_config_for_family(family, "m");
        let mut normalizer: Box<dyn StreamNormalizer> = match family {
            WireFamilyId::OpenAiResponses => OpenAiResponses.normalizer(&frozen, StepId(1)),
            _ => OpenAiResponsesChatgpt.normalizer(&frozen, StepId(1)),
        };
        let capture = Arc::new(crate::quota::tests::Capture::default());
        let observed = capture.clone();
        normalizer.set_raw_sse_observer(Arc::new(move |frame| {
            crate::quota::observe_frame(observed.as_ref(), frame)
        }));
        normalizer.push(b"data: {\"type\":\"response.created\",\"response\":{\"id\":\"r\"}}\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"ordinary\"}\n\n").unwrap();
        assert_eq!(capture.frame_calls.load(Ordering::Relaxed), 0);
        let payload = r#"{ "type": "codex.rate_limits", "rate_limits": null }"#;
        let wire = format!("data: {payload}\n\n");
        let split = wire.find("rate_limits").unwrap() + 4;
        normalizer.push(&wire.as_bytes()[..split]).unwrap();
        assert_eq!(capture.frame_calls.load(Ordering::Relaxed), 0);
        normalizer.push(&wire.as_bytes()[split..]).unwrap();
        normalizer.finish().unwrap();
        assert_eq!(capture.frame_calls.load(Ordering::Relaxed), 1);
        assert_eq!(capture.frames.lock().unwrap()[0].0, payload);
    }
}
