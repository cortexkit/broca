#[tokio::test]
async fn attempt_reserved_before_every_send() {
    let server = ScriptedServer::start(vec![
        ScriptResponse::Http {
            status: 503,
            headers: vec![],
            body: b"busy",
        },
        ScriptResponse::Http {
            status: 503,
            headers: vec![],
            body: b"busy",
        },
        ScriptResponse::Http {
            status: 200,
            headers: vec![],
            body: b"done",
        },
    ])
    .await;
    let mut provider = retry_test_provider(&server, 0);
    use_virtual_retry_waits(&mut provider);
    let hook = Arc::new(TestAttemptHook::new(server.order.clone()));
    let mut stream = broca_core::Provider::step_with_runtime(
        &provider,
        StepId(1),
        Arc::new(CallOptions::new(vec![])),
        broca_core::provider::StepRuntime {
            cancel: CancellationToken::new(),
            deadline: None,
            seal: None,
            progress: None,
            first_attempt: 0,
            attempt_hook: Some(hook.clone()),
        },
    )
    .await;
    while stream.recv().await.is_some() {}
    assert_eq!(
        *server.order.lock().unwrap(),
        [
            "durable", "write", "outcome", "durable", "write", "outcome", "durable", "write",
            "outcome"
        ]
    );
    let sends = hook.sends.lock().unwrap();
    assert_eq!(sends.len(), 3);
    assert_eq!(
        sends
            .iter()
            .map(|(_, send)| send.attempt)
            .collect::<Vec<_>>(),
        [0, 1, 2]
    );
    let ids: std::collections::BTreeSet<_> = sends.iter().map(|(id, _)| id).collect();
    assert_eq!(ids.len(), 3);
}

#[tokio::test]
async fn failed_attempt_receipt_precedes_billing_and_is_not_serialized() {
    let server = ScriptedServer::start(vec![ScriptResponse::Http {
        status: 400,
        headers: vec![],
        body: b"bad request",
    }])
    .await;
    let provider = retry_test_provider(&server, 0);
    let hook = Arc::new(TestAttemptHook::new(server.order.clone()));
    let mut stream = broca_core::Provider::step_with_runtime(
        &provider,
        StepId(1),
        Arc::new(CallOptions::new(vec![])),
        broca_core::provider::StepRuntime {
            cancel: CancellationToken::new(),
            deadline: None,
            seal: None,
            progress: None,
            first_attempt: 0,
            attempt_hook: Some(hook.clone()),
        },
    )
    .await;
    while stream.recv().await.is_some() {}
    let settled = hook.finished.lock().unwrap();
    assert_eq!(settled.len(), 1);
    let attempt = &settled[0].1;
    assert_eq!(attempt.outcome, AttemptOutcome::HttpError);
    assert!(
        attempt.account_observation.failure_received_at_ms.is_some(),
        "billing settlement must already receive the original failure timestamp"
    );
    let bytes = serde_json::to_vec(attempt).unwrap();
    let mut without_receipt = attempt.clone();
    without_receipt.account_observation.failure_received_at_ms = None;
    assert_eq!(bytes, serde_json::to_vec(&without_receipt).unwrap());
    let decoded: ModelAttempt = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(*decoded.account_observation, Default::default());
}

#[tokio::test]
async fn attempt_hook_failure_aborts_http_before_network_write() {
    let server = ScriptedServer::start(vec![]).await;
    let provider = retry_test_provider(&server, 0);
    let hook = Arc::new(TestAttemptHook {
        deny: true,
        ..TestAttemptHook::new(server.order.clone())
    });
    let mut stream = broca_core::Provider::step_with_runtime(
        &provider,
        StepId(1),
        Arc::new(CallOptions::new(vec![])),
        broca_core::provider::StepRuntime {
            cancel: CancellationToken::new(),
            deadline: None,
            seal: None,
            progress: None,
            first_attempt: 0,
            attempt_hook: Some(hook),
        },
    )
    .await;
    let mut events = Vec::new();
    while let Some(event) = stream.recv().await {
        events.push(event);
    }
    assert_eq!(
        terminal_error(&events).provider_code.as_deref(),
        Some("attempt_durability_failed")
    );
    assert!(server.request_bodies().is_empty());
}


#[tokio::test]
async fn compressed_refusal_is_two_attempts() {
    let server = ScriptedServer::start(vec![
        ScriptResponse::Http {
            status: 415,
            headers: vec![],
            body: b"",
        },
        ScriptResponse::Http {
            status: 200,
            headers: vec![],
            body: b"done",
        },
    ])
    .await;
    let address: std::net::SocketAddr =
        server.url.strip_prefix("http://").unwrap().parse().unwrap();
    let client = ProviderHttpClient::resolving_anthropic_api_to(address, true);
    let request = PreparedHttpRequest::from(HttpRequest {
        method: HttpMethod::Post,
        url: format!("http://api.anthropic.com:{}/messages", address.port()),
        headers: vec![],
        body: vec![b'x'; 100_000],
    });
    let hook = Arc::new(TestAttemptHook::new(server.order.clone()));
    let mut trace = AttemptTrace {
        attempt_hook: Some(hook.clone()),
        ..Default::default()
    };
    let (tx, _rx) = mpsc::channel(32);
    drive_attempt(
        &client,
        &request,
        &Clock::default(),
        Box::new(SuccessAttemptNormalizer),
        &tx,
        Duration::from_secs(5),
        &mut trace,
    )
    .await
    .unwrap();
    trace
        .settle_send(
            &RequestDigest::of(&request.body),
            AttemptOutcome::Complete,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        *server.order.lock().unwrap(),
        ["durable", "write", "outcome", "durable", "write", "outcome"]
    );
    let sends = hook.sends.lock().unwrap();
    assert_eq!(sends.len(), 2);
    assert_ne!(sends[0].0, sends[1].0);
    assert!(sends[0].1.compressed);
    assert_eq!(sends[0].1.send_index, 1);
    assert!(!sends[1].1.compressed);
    assert_eq!(sends[1].1.send_index, 2);
    let terminals = hook.finished.lock().unwrap();
    assert_eq!(terminals[0].0, sends[0].0);
    assert_eq!(
        terminals[0].2,
        broca_wal::record::BillingAttemptOutcome::CompressionRefused
    );
    assert_eq!(terminals[0].1.http_status, Some(415));
    assert!(terminals[0].1.sent_at_ms.is_some());
    assert_eq!(terminals[1].0, sends[1].0);
}

#[tokio::test]
async fn attempt_hook_reserves_websocket_retries_and_https_fallback() {
    use futures::{SinkExt, StreamExt};
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let address = listener.local_addr().unwrap();
    let order = Arc::new(Mutex::new(Vec::new()));
    let wire_order = order.clone();
    let peer = tokio::spawn(async move {
        for _ in 0..6 {
            let (stream, _) = listener.accept().await.unwrap();
            let mut config = tungstenite::protocol::WebSocketConfig::default();
            config.extensions.permessage_deflate =
                Some(tungstenite::extensions::compression::deflate::DeflateConfig::default());
            let mut socket = tokio_tungstenite::accept_async_with_config(stream, Some(config))
                .await
                .unwrap();
            let frame = socket.next().await.unwrap().unwrap();
            assert!(frame.is_text());
            wire_order.lock().unwrap().push("write");
            socket
                .send(tungstenite::Message::Close(None))
                .await
                .unwrap();
        }
        let (mut stream, _) = listener.accept().await.unwrap();
        read_http_request(&mut stream).await;
        wire_order.lock().unwrap().push("write");
        write_http_response(&mut stream, 200, &[], b"done").await;
    });
    let mut frozen = openai_chat_frozen_config("m");
    frozen.family_id = crate::spec::WireFamilyId::OpenAiResponsesChatgpt;
    frozen.base_url = Some(format!("http://{address}"));
    let mut provider = LiveProvider::new(RetryTestFamily::new(0), frozen, AuthHandle::None);
    use_virtual_retry_waits(&mut provider);
    let hook = Arc::new(TestAttemptHook::new(order.clone()));
    let mut stream = broca_core::Provider::step_with_runtime(
        &provider,
        StepId(1),
        Arc::new(CallOptions::new(vec![])),
        broca_core::provider::StepRuntime {
            cancel: CancellationToken::new(),
            deadline: None,
            seal: None,
            progress: None,
            first_attempt: 0,
            attempt_hook: Some(hook.clone()),
        },
    )
    .await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while stream.recv().await.is_some() {}
    })
    .await
    .unwrap();
    peer.await.unwrap();
    assert_eq!(
        *order.lock().unwrap(),
        ["durable", "write", "outcome"].repeat(7)
    );
    assert_eq!(hook.sends.lock().unwrap().len(), 7);
    let finished = hook.finished.lock().unwrap();
    assert!(finished[..6]
        .iter()
        .all(|(_, attempt, _)| attempt.transport.as_deref() == Some("websocket")));
    assert_eq!(finished[6].1.transport.as_deref(), Some("https"));
}
