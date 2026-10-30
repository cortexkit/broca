use super::*;

fn run_provider(
    mock: &Mock,
    client: &ProviderHttpClient,
    turn: &str,
) -> LiveProvider<OpenAiResponsesChatgpt> {
    let mut provider = codex_provider(mock);
    provider.client = client.clone();
    let mut frozen = (*provider.frozen).clone();
    let identity = frozen.chatgpt_wire.as_mut().unwrap();
    identity.turn_id = turn.into();
    identity.turn_started_at_unix_ms += if turn == "one" { 0 } else { 1 };
    provider.frozen = Arc::new(frozen);
    provider
}

fn bind(session: &str) -> BindIdentity {
    BindIdentity::new("/project", "harness", session)
}

#[tokio::test]
async fn codex_cross_run_two_completed_runs_reuse_handshake_and_suffix() {
    let mock = Mock::start(vec![text_events(), text_events()], false, false).await;
    let client = build_provider_http_client();
    let root = broca_test_support::scratch_root().join("codex-cross-run");
    let bind = BindIdentity::new(root.join("project"), "websocket", "conversation");
    let wal = FileWalStore::new(root.join("wal"));
    let leases = FileLeaseStore::new(root.join("lease"));
    for (index, turn) in ["one", "two"].into_iter().enumerate() {
        let provider = run_provider(&mock, &client, turn);
        let mut request = tool_run_request(&bind);
        request.run_id = RunId(format!("cross-run-{turn}"));
        request.tools.clear();
        request.policy_allowed.clear();
        request.input[0].content = vec![ContentBlock::Text {
            text: format!("question {index}"),
        }];
        let (tx, mut rx) = mpsc::channel(256);
        let collect = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let summary = run_loop(
            request,
            &provider,
            &StubToolPlane::new(),
            &leases,
            &AllowAll,
            &wal,
            &tx,
        )
        .await
        .unwrap();
        drop(tx);
        collect.await.unwrap();
        assert_eq!(summary.reason, RunFinishReason::Completed);
        assert_eq!(summary.steps, 1);
    }
    let seen = mock.observed.lock().unwrap();
    assert_eq!(seen.handshakes, 1);
    assert_eq!(seen.requests.len(), 2);
    let frame: Value = serde_json::from_slice(&seen.requests[1]).unwrap();
    assert_eq!(frame["previous_response_id"], "r");
    assert_eq!(frame["input"].as_array().unwrap().len(), 1);
    assert_eq!(frame["input"][0]["content"][0]["text"], "question 1");
    assert_eq!(frame["client_metadata"]["turn_id"], "two");
}

#[tokio::test]
async fn codex_cross_run_turn_state_is_fresh_but_current_turn_metadata_is_sent() {
    let mut replies = vec![
        serde_json::json!({"type":"response.metadata","headers":{"x-codex-turn-state":"first-turn-only"}}),
    ];
    replies.extend(text_events());
    let mock = Mock::start(vec![replies, text_events()], false, false).await;
    let client = build_provider_http_client();
    let first = run_provider(&mock, &client, "one");
    first.bind_run_transport(&bind("session"));
    let events = step(&first, 1, &opts()).await;
    first.complete_run_transport();
    let second = run_provider(&mock, &client, "two");
    second.bind_run_transport(&bind("session"));
    let events = step(&second, 1, &follow_up(opts(), &events, "Next")).await;
    assert!(codex_trace(&events, 0).continuation);
    second.complete_run_transport();
    let seen = mock.observed.lock().unwrap();
    assert_eq!(seen.handshakes, 1);
    let frame: Value = serde_json::from_slice(&seen.requests[1]).unwrap();
    assert!(frame["client_metadata"].get("x-codex-turn-state").is_none());
    assert_eq!(frame["client_metadata"]["turn_id"], "two");
    assert!(frame["client_metadata"]["x-codex-turn-metadata"]
        .as_str()
        .unwrap()
        .contains("two"));
}

async fn reset_case(trigger: &str) {
    let mock = Mock::start(vec![text_events(), text_events()], false, false).await;
    let client = build_provider_http_client();
    let first = run_provider(&mock, &client, "one");
    first.bind_run_transport(&bind("session"));
    let events = step(&first, 1, &opts()).await;
    first.complete_run_transport();
    let fresh_client = build_provider_http_client();
    let mut second = run_provider(
        &mock,
        if trigger == "restart" {
            &fresh_client
        } else {
            &client
        },
        "two",
    );
    if trigger == "account" || trigger == "token" {
        let mut auth = second.auth.write().unwrap();
        let AuthHandle::Headers(mut headers) = (*auth.auth).clone() else {
            panic!("header auth")
        };
        let (name, replacement) = if trigger == "account" {
            ("chatgpt-account-id", "other-account")
        } else {
            ("authorization", "Bearer rotated-token")
        };
        headers.iter_mut().find(|(key, _)| key == name).unwrap().1 = replacement.into();
        auth.auth = Arc::new(AuthHandle::Headers(headers));
    }
    if trigger == "revision" {
        second.set_credential_revision(2);
    }
    if trigger == "routing" || trigger == "installation" {
        let mut frozen = (*second.frozen).clone();
        let wire = frozen.chatgpt_wire.as_mut().unwrap();
        if trigger == "installation" {
            wire.installation_id = "different-installation".into();
        } else {
            wire.window_id = "different-window".into();
        }
        second.frozen = Arc::new(frozen);
    }
    if trigger == "idle" {
        second.websocket.advance_clock(Duration::from_secs(10 * 60));
    }
    if trigger == "delete" || trigger == "move" {
        client
            .chatgpt_connections()
            .retire_session(&bind("session"));
    }
    if trigger == "revoke" || trigger == "shutdown" {
        client.chatgpt_connections().clear();
    }
    second.bind_run_transport(&bind("session"));
    let mut next = follow_up(opts(), &events, "Next");
    if trigger == "history" {
        next.prompt[0].content = vec![ContentBlock::Text {
            text: "edited".into(),
        }];
    }
    if trigger == "compaction" {
        next.prompt.remove(0);
    }
    let events = step(&second, 1, &next).await;
    assert!(!codex_trace(&events, 0).continuation, "{trigger}");
    let expected_reason = match trigger {
        "account" | "token" | "revision" => "account_or_credential_change",
        "idle" => "new_connection_idle",
        "restart" | "delete" | "move" | "revoke" | "shutdown" => "new_connection_run_start",
        "history" => "input_mismatch",
        "compaction" => "input_mismatch",
        _ => "no_completed_response",
    };
    assert_eq!(
        codex_trace(&events, 0).full_input_reason.as_deref(),
        Some(expected_reason),
        "{trigger}"
    );
    let canonical: Value = serde_json::from_slice(&second.render_request(&next)).unwrap();
    let seen = mock.observed.lock().unwrap();
    assert_eq!(
        seen.handshakes,
        if trigger == "history" || trigger == "compaction" {
            1
        } else {
            2
        },
        "{trigger}"
    );
    assert_eq!(seen.requests.len(), 2);
    let frame: Value = serde_json::from_slice(&seen.requests[1]).unwrap();
    assert!(frame.get("previous_response_id").is_none(), "{trigger}");
    assert_eq!(frame["input"], canonical["input"], "{trigger}");
}

#[tokio::test]
async fn codex_cross_run_history_change_is_full() {
    reset_case("history").await;
}
#[tokio::test]
async fn codex_cross_run_compaction_is_full() {
    reset_case("compaction").await;
}
#[tokio::test]
async fn codex_cross_run_account_change_is_full() {
    reset_case("account").await;
}
#[tokio::test]
async fn codex_cross_run_token_rotation_is_full() {
    reset_case("token").await;
}
#[tokio::test]
async fn codex_cross_run_credential_revision_change_is_full() {
    reset_case("revision").await;
}
#[tokio::test]
async fn codex_cross_run_routing_change_is_full() {
    reset_case("routing").await;
}
#[tokio::test]
async fn codex_cross_run_idle_eviction_is_full() {
    reset_case("idle").await;
}
#[tokio::test]
async fn codex_cross_run_installation_change_is_full() {
    reset_case("installation").await;
}

#[tokio::test]
async fn codex_cross_run_age_cap_is_full_despite_recent_compatible_borrows() {
    let mock = Mock::start(vec![text_events(); 6], false, false).await;
    let client = build_provider_http_client();
    let mut options = opts();
    for (index, minutes) in [0, 9, 18, 27, 36, 41].into_iter().enumerate() {
        let provider = run_provider(&mock, &client, &index.to_string());
        provider
            .websocket
            .advance_clock(Duration::from_secs(minutes * 60));
        provider.bind_run_transport(&bind("session"));
        let events = step(&provider, 1, &options).await;
        assert_eq!(
            codex_trace(&events, 0).continuation,
            index > 0 && minutes < 40
        );
        if minutes == 41 {
            assert_eq!(
                codex_trace(&events, 0).full_input_reason.as_deref(),
                Some("new_connection_age")
            );
        }
        options = follow_up(options, &events, "Next");
        provider.complete_run_transport();
    }
    let seen = mock.observed.lock().unwrap();
    assert_eq!(seen.handshakes, 2);
    let frame: Value = serde_json::from_slice(&seen.requests[5]).unwrap();
    assert!(frame.get("previous_response_id").is_none());
    assert!(frame["input"].as_array().unwrap().len() > 1);
}
#[tokio::test]
async fn codex_cross_run_restart_is_full() {
    reset_case("restart").await;
}
#[tokio::test]
async fn codex_cross_run_session_deletion_is_full() {
    reset_case("delete").await;
}
#[tokio::test]
async fn codex_cross_run_session_move_is_full() {
    reset_case("move").await;
}
#[tokio::test]
async fn codex_cross_run_revocation_is_full() {
    reset_case("revoke").await;
}
#[tokio::test]
async fn codex_cross_run_shutdown_is_full() {
    reset_case("shutdown").await;
}

#[tokio::test]
async fn codex_cross_run_cancel_retires_borrowed_socket() {
    let mock = Mock::start(vec![text_events(), vec![], text_events()], false, false).await;
    let client = build_provider_http_client();
    let first = run_provider(&mock, &client, "one");
    first.bind_run_transport(&bind("session"));
    let first_events = step(&first, 1, &opts()).await;
    first.complete_run_transport();
    let second = run_provider(&mock, &client, "two");
    second.bind_run_transport(&bind("session"));
    let cancel = CancellationToken::new();
    let mut rx = second
        .step_with_cancel(
            StepId(1),
            &follow_up(opts(), &first_events, "Next"),
            cancel.clone(),
            None,
        )
        .await;
    while mock.observed.lock().unwrap().requests.len() < 2 {
        mock.changed.notified().await;
    }
    cancel.cancel();
    while rx.recv().await.is_some() {}
    second.close_run_transport();
    mock.wait_closes(1).await;
    let third = run_provider(&mock, &client, "three");
    third.bind_run_transport(&bind("session"));
    let events = step(&third, 1, &opts()).await;
    assert!(!codex_trace(&events, 0).continuation);
    assert_eq!(mock.observed.lock().unwrap().handshakes, 2);
}

#[tokio::test]
async fn codex_cross_run_failure_retires_borrowed_socket() {
    let failure = serde_json::json!({"type":"error","status":400,"error":{"type":"invalid_request_error","code":"invalid_request_error","message":"refused"}});
    let mock = Mock::start(
        vec![text_events(), vec![failure], text_events()],
        false,
        false,
    )
    .await;
    let client = build_provider_http_client();
    let first = run_provider(&mock, &client, "one");
    first.bind_run_transport(&bind("session"));
    let initial = step(&first, 1, &opts()).await;
    first.complete_run_transport();
    let second = run_provider(&mock, &client, "two");
    second.bind_run_transport(&bind("session"));
    let events = step(&second, 1, &follow_up(opts(), &initial, "Next")).await;
    assert_eq!(terminal_error(&events).status, Some(400));
    // Even an erroneous caller reporting completion must not return a failed stream.
    second.complete_run_transport();
    mock.wait_closes(1).await;
    let third = run_provider(&mock, &client, "three");
    third.bind_run_transport(&bind("session"));
    let events = step(&third, 1, &opts()).await;
    assert!(!codex_trace(&events, 0).continuation);
    assert_eq!(mock.observed.lock().unwrap().handshakes, 2);
}

#[tokio::test]
async fn codex_cross_run_full_bind_and_exclusive_borrow_fence_connections() {
    let mock = Mock::start(vec![text_events(); 5], false, false).await;
    let client = build_provider_http_client();
    let first = run_provider(&mock, &client, "one");
    first.bind_run_transport(&bind("session"));
    step(&first, 1, &opts()).await;
    // An overlapping run cannot borrow the first run's still-leased socket.
    let overlap = run_provider(&mock, &client, "two");
    overlap.bind_run_transport(&bind("session"));
    step(&overlap, 1, &opts()).await;
    overlap.complete_run_transport();
    first.complete_run_transport();
    for other in [
        bind("other"),
        BindIdentity::new("/other", "harness", "session"),
        BindIdentity::new("/project", "other", "session"),
    ] {
        let provider = run_provider(&mock, &client, "two");
        provider.bind_run_transport(&other);
        let events = step(&provider, 1, &opts()).await;
        assert!(!codex_trace(&events, 0).continuation);
        provider.complete_run_transport();
    }
    assert_eq!(mock.observed.lock().unwrap().handshakes, 5);
}

#[tokio::test]
async fn codex_cross_run_idle_timer_closes_without_another_run() {
    let mock = Mock::start(vec![text_events(); 2], false, false).await;
    let client = build_provider_http_client();
    let first = run_provider(&mock, &client, "one");
    first.bind_run_transport(&bind("session"));
    step(&first, 1, &opts()).await;
    first.complete_run_transport();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(10 * 60)).await;
    tokio::task::yield_now().await;
    tokio::time::resume();
    mock.wait_closes(1).await;
    let next = run_provider(&mock, &client, "two");
    next.bind_run_transport(&bind("session"));
    let events = step(&next, 1, &opts()).await;
    assert_eq!(
        codex_trace(&events, 0).full_input_reason.as_deref(),
        Some("new_connection_idle")
    );
}

#[tokio::test]
async fn codex_cross_run_capacity_evicts_oldest_idle_connection() {
    let mock = Mock::start(vec![text_events(); 67], false, false).await;
    let client = build_provider_http_client();
    let mut last = Vec::new();
    for n in 0..65 {
        let provider = run_provider(&mock, &client, "one");
        provider.bind_run_transport(&bind(&n.to_string()));
        last = step(&provider, 1, &opts()).await;
        provider.complete_run_transport();
    }
    mock.wait_closes(1).await;
    let newest = run_provider(&mock, &client, "two");
    newest.bind_run_transport(&bind("64"));
    let events = step(&newest, 1, &follow_up(opts(), &last, "Next")).await;
    assert!(codex_trace(&events, 0).continuation);
    newest.complete_run_transport();
    let oldest = run_provider(&mock, &client, "two");
    oldest.bind_run_transport(&bind("0"));
    let events = step(&oldest, 1, &opts()).await;
    assert!(!codex_trace(&events, 0).continuation);
    assert_eq!(
        codex_trace(&events, 0).full_input_reason.as_deref(),
        Some("new_connection_run_start_capacity")
    );
    assert_eq!(mock.observed.lock().unwrap().handshakes, 66);
}

#[tokio::test]
async fn codex_cross_run_https_fallback_does_not_leak_into_the_next_run() {
    let mock = Mock::start(vec![text_events(); 3], false, false).await;
    let client = build_provider_http_client();
    let first = run_provider(&mock, &client, "one");
    first.bind_run_transport(&bind("session"));
    let events = step(&first, 1, &opts()).await;
    first.complete_run_transport();
    let second = run_provider(&mock, &client, "two");
    second.bind_run_transport(&bind("session"));
    second.websocket.switch_to_https();
    let events = step(&second, 1, &follow_up(opts(), &events, "Next")).await;
    assert_eq!(attempts(&events)[0].transport.as_deref(), Some("https"));
    second.complete_run_transport();
    let third = run_provider(&mock, &client, "three");
    third.bind_run_transport(&bind("session"));
    let events = step(&third, 1, &opts()).await;
    assert!(!codex_trace(&events, 0).continuation);
    assert_eq!(mock.observed.lock().unwrap().handshakes, 2);
}

#[tokio::test]
async fn codex_cross_run_drop_without_completion_retires_borrowed_state() {
    let mock = Mock::start(vec![text_events(); 2], false, false).await;
    let client = build_provider_http_client();
    let first = run_provider(&mock, &client, "one");
    first.bind_run_transport(&bind("session"));
    let events = step(&first, 1, &opts()).await;
    drop(first);
    mock.wait_closes(1).await;
    let second = run_provider(&mock, &client, "two");
    second.bind_run_transport(&bind("session"));
    let events = step(&second, 1, &follow_up(opts(), &events, "Next")).await;
    assert!(!codex_trace(&events, 0).continuation);
    assert_eq!(mock.observed.lock().unwrap().handshakes, 2);
}

#[tokio::test]
async fn codex_cross_run_return_near_age_limit_uses_remaining_lifetime() {
    let mock = Mock::start(vec![text_events(); 6], false, false).await;
    let client = build_provider_http_client();
    let mut options = opts();
    for index in 0..6 {
        if index > 0 {
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(if index < 5 {
                9 * 60
            } else {
                3 * 60 + 50
            }))
            .await;
            tokio::time::resume();
        }
        let provider = run_provider(&mock, &client, &index.to_string());
        provider.bind_run_transport(&bind("session"));
        let events = step(&provider, 1, &options).await;
        assert_eq!(codex_trace(&events, 0).continuation, index > 0);
        options = follow_up(options, &events, "Next");
        provider.complete_run_transport();
    }
    assert_eq!(mock.observed.lock().unwrap().handshakes, 1);
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(9)).await;
    tokio::time::resume();
    tokio::task::yield_now().await;
    assert_eq!(mock.observed.lock().unwrap().closes, 0);
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::time::resume();
    mock.wait_closes(1).await;
}

#[tokio::test]
async fn codex_cross_run_receipt_byte_budget_evicts_oldest_idle_socket() {
    let mock = Mock::start(vec![text_events(); 4], false, false).await;
    let mut client = build_provider_http_client();
    client.chatgpt_connections = ChatgptConnections::with_byte_budget(512 * 1024);
    let mut options = opts();
    options.prompt[0].content = vec![ContentBlock::Text {
        text: "p".repeat(400_000),
    }];
    let first = run_provider(&mock, &client, "one");
    first.bind_run_transport(&bind("first"));
    step(&first, 1, &options).await;
    first.complete_run_transport();
    assert_eq!(mock.observed.lock().unwrap().closes, 0);
    let second = run_provider(&mock, &client, "one");
    second.bind_run_transport(&bind("second"));
    let events = step(&second, 1, &options).await;
    second.complete_run_transport();
    mock.wait_closes(1).await;
    let newest = run_provider(&mock, &client, "two");
    newest.bind_run_transport(&bind("second"));
    assert!(
        codex_trace(
            &step(&newest, 1, &follow_up(options.clone(), &events, "Next")).await,
            0
        )
        .continuation
    );
    newest.complete_run_transport();
    let oldest = run_provider(&mock, &client, "two");
    oldest.bind_run_transport(&bind("first"));
    let events = step(&oldest, 1, &options).await;
    assert!(!codex_trace(&events, 0).continuation);
    assert_eq!(
        codex_trace(&events, 0).full_input_reason.as_deref(),
        Some("new_connection_run_start_receipt_bytes")
    );
    assert_eq!(mock.observed.lock().unwrap().handshakes, 3);
}

#[tokio::test]
async fn codex_cross_run_single_receipt_over_byte_budget_is_not_retained() {
    let mock = Mock::start(vec![text_events(); 2], false, false).await;
    let mut client = build_provider_http_client();
    client.chatgpt_connections = ChatgptConnections::with_byte_budget(512 * 1024);
    let mut options = opts();
    options.prompt[0].content = vec![ContentBlock::Text {
        text: "p".repeat(600_000),
    }];
    let first = run_provider(&mock, &client, "one");
    first.bind_run_transport(&bind("session"));
    let events = step(&first, 1, &options).await;
    first.complete_run_transport();
    mock.wait_closes(1).await;
    let second = run_provider(&mock, &client, "two");
    second.bind_run_transport(&bind("session"));
    let events = step(&second, 1, &follow_up(options, &events, "Next")).await;
    assert!(!codex_trace(&events, 0).continuation);
    assert_eq!(
        codex_trace(&events, 0).full_input_reason.as_deref(),
        Some("new_connection_run_start_receipt_too_large")
    );
    assert_eq!(mock.observed.lock().unwrap().handshakes, 2);
}

#[tokio::test]
async fn codex_cross_run_connection_limit_retires_only_that_credentials_idle_sockets() {
    let limit =
        serde_json::json!({"type":"error","error":{"code":"websocket_connection_limit_reached"}});
    let mock = Mock::start(
        vec![
            text_events(),
            text_events(),
            text_events(),
            vec![limit],
            text_events(),
            text_events(),
            text_events(),
            text_events(),
        ],
        false,
        false,
    )
    .await;
    let client = build_provider_http_client();
    let idle = run_provider(&mock, &client, "one");
    idle.bind_run_transport(&bind("idle"));
    step(&idle, 1, &opts()).await;
    idle.complete_run_transport();
    let other = run_provider(&mock, &client, "one");
    let mut headers = match &*other.auth.read().unwrap().auth {
        AuthHandle::Headers(headers) => headers.clone(),
        _ => panic!("header auth"),
    };
    headers
        .iter_mut()
        .find(|(key, _)| key == "authorization")
        .unwrap()
        .1 = "Bearer other-token".into();
    other.auth.write().unwrap().auth = Arc::new(AuthHandle::Headers(headers));
    other.bind_run_transport(&bind("other-credential"));
    let other_events = step(&other, 1, &opts()).await;
    other.complete_run_transport();
    let active = run_provider(&mock, &client, "one");
    active.bind_run_transport(&bind("active"));
    let active_events = step(&active, 1, &opts()).await;
    let limited = run_provider(&mock, &client, "two");
    limited.bind_run_transport(&bind("limited"));
    let events = step(&limited, 1, &opts()).await;
    assert_eq!(attempts(&events).len(), 2);
    assert_eq!(attempts(&events)[0].outcome, AttemptOutcome::ChunkError);
    assert_eq!(attempts(&events)[1].outcome, AttemptOutcome::Complete);
    assert_eq!(
        codex_trace(&events, 1).full_input_reason.as_deref(),
        Some("connection_limit")
    );
    for attempt in attempts(&events) {
        assert_eq!(
            attempt
                .websocket_request
                .as_ref()
                .unwrap()
                .codex
                .as_ref()
                .unwrap()
                .physical_send_count,
            1
        );
    }
    mock.wait_closes(2).await;
    assert!(mock.observed.lock().unwrap().closes_at_request[4] >= 2);
    let other_next = run_provider(&mock, &client, "two");
    other_next.auth.write().unwrap().auth = other.current_auth().auth;
    other_next.bind_run_transport(&bind("other-credential"));
    assert!(
        codex_trace(
            &step(&other_next, 1, &follow_up(opts(), &other_events, "Next")).await,
            0
        )
        .continuation
    );
    assert!(
        codex_trace(
            &step(&active, 2, &follow_up(opts(), &active_events, "Next")).await,
            0
        )
        .continuation
    );
    assert_eq!(mock.observed.lock().unwrap().handshakes, 5);
    let idle_next = run_provider(&mock, &client, "two");
    idle_next.bind_run_transport(&bind("idle"));
    let events = step(&idle_next, 1, &opts()).await;
    assert_eq!(
        codex_trace(&events, 0).full_input_reason.as_deref(),
        Some("connection_limit")
    );
}

#[tokio::test]
async fn codex_cross_run_connection_limit_uses_identical_codex_counts_for_retained_and_fresh() {
    for reuse in [false, true] {
        let failed = serde_json::json!({"type":"error","error":{"code":"websocket_connection_limit_reached"}});
        let mut replies = vec![text_events()];
        replies.extend((0..6).map(|_| vec![failed.clone()]));
        replies.push(text_events());
        let mock = Mock::start(replies, false, false).await;
        let client = build_provider_http_client();
        let first = run_provider(&mock, &client, "one");
        first.bind_run_transport(&bind("session"));
        let initial = step(&first, 1, &opts()).await;
        first.complete_run_transport();
        if !reuse {
            client
                .chatgpt_connections()
                .retire_session(&bind("session"));
        }
        let mut second = run_provider(&mock, &client, "two");
        let waits = Arc::new(Mutex::new(Vec::new()));
        second.retry_clock.virtual_waits = Some(waits.clone());
        second.bind_run_transport(&bind("session"));
        let options = follow_up(opts(), &initial, "Next");
        let canonical = second.render_request(&options);
        let events = step(&second, 1, &options).await;
        let recorded = attempts(&events);
        assert_eq!(
            recorded.len(),
            7,
            "reuse={reuse}: six socket sends, then one successful HTTPS send"
        );
        assert_eq!(codex_trace(&events, 0).continuation, reuse);
        for (index, attempt) in recorded.iter().enumerate() {
            assert_canonical(attempt, &canonical);
            if index < 6 {
                assert_eq!(attempt.transport.as_deref(), Some("websocket"));
                assert_eq!(attempt.outcome, AttemptOutcome::ChunkError);
                assert_eq!(codex_trace(&events, index).physical_send_count, 1);
                if index > 0 {
                    assert!(!codex_trace(&events, index).continuation);
                }
            } else {
                assert_eq!(attempt.transport.as_deref(), Some("https"));
                assert_eq!(attempt.outcome, AttemptOutcome::Complete);
            }
        }
        let waits = waits.lock().unwrap();
        assert_eq!(waits.len(), 6);
        assert_codex_stream_backoffs(&waits[..5]);
        assert_eq!(waits[5], Duration::ZERO);
        let seen = mock.observed.lock().unwrap();
        assert_eq!(seen.requests.len(), 7);
        assert_eq!(seen.http_bodies.len(), 1);
        assert_eq!(seen.handshakes, if reuse { 6 } else { 7 });
    }
}

#[tokio::test]
async fn cache_evidence_live_records_extension_age_resend_and_restart_restore() {
    let mut replies = vec![text_events(); 8];
    replies[5][1]["response"]["usage"]["input_tokens_details"]["cached_tokens"] = Value::from(0);
    let mock = Mock::start(replies, false, false).await;
    let mut client = build_provider_http_client();
    let root = broca_test_support::scratch_root().join("cache-evidence-live-records");
    let session = BindIdentity::new(
        "/synthetic-cache-evidence-project",
        "scripted-websocket",
        "cache-evidence-conversation",
    );
    let wal = FileWalStore::new(root.join("wal"));
    let leases = FileLeaseStore::new(root.join("lease"));
    for (index, minutes) in [0, 9, 18, 27, 36, 41, 41, 41].into_iter().enumerate() {
        // A new HTTP client models a process restart: no socket or fingerprint survives.
        if index == 6 {
            client = build_provider_http_client();
        }
        let provider = run_provider(&mock, &client, &index.to_string());
        provider
            .websocket
            .advance_clock(Duration::from_secs(minutes * 60));
        let mut request = tool_run_request(&session);
        request.run_id = RunId(format!("cache-evidence-run-{index}"));
        request.tools.clear();
        request.policy_allowed.clear();
        request.input[0].content = vec![ContentBlock::Text {
            text: format!("synthetic question {index}"),
        }];
        let (tx, mut rx) = mpsc::channel(256);
        let collect = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let summary = broca_test_support::wait_for(
            "cache evidence run",
            run_loop(
                request,
                &provider,
                &StubToolPlane::new(),
                &leases,
                &AllowAll,
                &wal,
                &tx,
            ),
            || "scripted provider did not finish".into(),
        )
        .await
        .unwrap();
        drop(tx);
        collect.await.unwrap();
        assert_eq!(summary.reason, RunFinishReason::Completed);
        assert_eq!(summary.steps, 1);
    }
    let frames = wal.read_records(&session).unwrap();
    let attempts: Vec<_> = frames
        .iter()
        .filter(|f| matches!(f.record, WalRecord::ModelAttemptFinished { .. }))
        .collect();
    assert_eq!(attempts.len(), 8);
    let first = serde_json::to_value(&attempts[0].record).unwrap();
    assert!(first.get("cache_evidence").is_none());
    for pair in attempts.windows(2) {
        let attempt = serde_json::to_value(&pair[1].record).unwrap();
        let evidence = &attempt["cache_evidence"];
        assert_eq!(evidence["extends_previous"], true, "{attempt}");
        assert!(evidence["first_divergence"].is_null());
        assert!(evidence["compared_to"]
            .as_str()
            .unwrap()
            .ends_with(&format!(":{}", pair[0].seq)));
    }
    let extension = serde_json::to_value(&attempts[1].record).unwrap();
    let bust = serde_json::to_value(&attempts[5].record).unwrap();
    let restart = serde_json::to_value(&attempts[6].record).unwrap();
    assert_eq!(
        extension["websocket_request"]["codex"]["continuation"],
        true
    );
    assert_eq!(bust["websocket_request"]["codex"]["continuation"], false);
    assert_eq!(
        bust["websocket_request"]["codex"]["full_input_reason"],
        "new_connection_age"
    );
    assert_eq!(
        restart["websocket_request"]["codex"]["full_input_reason"],
        "new_connection_run_start"
    );
    if let Some(directory) = std::env::var_os("BROCA_CACHE_EVIDENCE_SAMPLES") {
        let directory = std::path::PathBuf::from(directory);
        std::fs::create_dir_all(&directory).unwrap();
        let mut sums = String::new();
        for (name, record) in [
            ("extension.json", extension),
            ("full-input-bust.json", bust),
        ] {
            let mut bytes = serde_json::to_vec_pretty(&record).unwrap();
            bytes.push(b'\n');
            sums.push_str(&format!("{:x}  {name}\n", Sha256::digest(&bytes)));
            std::fs::write(directory.join(name), bytes).unwrap();
        }
        std::fs::write(directory.join("SHA256SUMS"), sums).unwrap();
    }
}

#[tokio::test]
async fn cache_evidence_send_and_first_text_do_not_wait_for_fingerprint() {
    for https in [false, true] {
        let mock = Mock::start(vec![text_events()], false, false).await;
        let provider = codex_provider(&mock);
        provider.bind_run_transport(&bind("fingerprint-gate"));
        if https {
            provider.websocket.switch_to_https();
        }
        let session = provider.cache_session.lock().unwrap().clone().unwrap();
        let gate = Arc::new(cache_runtime::Gate::default());
        *session.1.lock().unwrap() = Some(Arc::clone(&gate));
        let mut rx = provider.step(StepId(1), &opts()).await;
        let observed = broca_test_support::event_timeout(
            "first text while fingerprint is blocked",
            async {
                while let Some(event) = rx.recv().await {
                    if matches!(
                        event,
                        NormalizedEvent::Display(broca_protocol::DisplayEvent::TextDelta { .. })
                    ) {
                        return;
                    }
                    assert!(
                        !matches!(
                            event,
                            NormalizedEvent::Control(ProviderControl::AttemptFinished { .. })
                        ),
                        "attempt must join its fingerprint"
                    );
                }
                panic!("provider closed without text");
            },
            || "fingerprint gate still closed".into(),
        )
        .await;
        let received = {
            let seen = mock.observed.lock().unwrap();
            seen.requests.len() + seen.http_bodies.len()
        };
        let started = broca_test_support::event_timeout(
            "fingerprint worker starts",
            async {
                while !gate.entered.load(Ordering::Acquire) {
                    tokio::task::yield_now().await;
                }
            },
            || "worker has not started".into(),
        )
        .await;
        gate.release();
        assert!(observed.is_ok());
        assert!(started.is_ok());
        assert_eq!(received, 1, "send already reached the scripted transport");
        while rx.recv().await.is_some() {}
    }
}

#[tokio::test]
async fn cache_evidence_captures_model_change_through_the_run_loop() {
    let mut changed = text_events();
    changed[1]["response"]["usage"]["input_tokens_details"]["cached_tokens"] = Value::from(0);
    let mock = Mock::start(vec![text_events(), changed], false, false).await;
    let client = build_provider_http_client();
    let root = broca_test_support::scratch_root().join("cache-evidence-model-change");
    let session = BindIdentity::new(
        root.join("synthetic-project"),
        "scripted-websocket",
        "model-change-conversation",
    );
    let wal = FileWalStore::new(root.join("wal"));
    let leases = FileLeaseStore::new(root.join("lease"));
    for (index, model) in ["m", "changed-m"].into_iter().enumerate() {
        let mut provider = run_provider(&mock, &client, &index.to_string());
        let mut frozen = (*provider.frozen).clone();
        frozen.wire_model_id = model.into();
        provider.frozen = Arc::new(frozen);
        let mut request = tool_run_request(&session);
        request.run_id = RunId(format!("model-change-run-{index}"));
        request.model.model_id = model.into();
        request.tools.clear();
        request.policy_allowed.clear();
        request.input[0].content = vec![ContentBlock::Text {
            text: format!("synthetic model question {index}"),
        }];
        let (tx, mut rx) = mpsc::channel(256);
        let collect = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let summary = broca_test_support::wait_for(
            "model-change capture run",
            run_loop(
                request,
                &provider,
                &StubToolPlane::new(),
                &leases,
                &AllowAll,
                &wal,
                &tx,
            ),
            || "scripted model did not finish".into(),
        )
        .await
        .unwrap();
        drop(tx);
        collect.await.unwrap();
        assert_eq!(summary.reason, RunFinishReason::Completed);
    }
    let frames = wal.read_records(&session).unwrap();
    let attempts: Vec<_> = frames
        .iter()
        .filter(|f| matches!(f.record, WalRecord::ModelAttemptFinished { .. }))
        .collect();
    assert_eq!(attempts.len(), 2);
    let record = serde_json::to_value(&attempts[1].record).unwrap();
    assert_eq!(record["cache_evidence"]["model_changed"], true);
    assert_eq!(record["cache_evidence"]["extends_previous"], false);
    assert_eq!(
        record["websocket_request"]["codex"]["full_input_reason"],
        "model_changed"
    );
    assert!(record["cache_evidence"]["compared_to"]
        .as_str()
        .unwrap()
        .ends_with(&format!(":{}", attempts[0].seq)));
    if let Some(directory) = std::env::var_os("BROCA_CACHE_EVIDENCE_SAMPLES") {
        let directory = std::path::PathBuf::from(directory);
        std::fs::create_dir_all(&directory).unwrap();
        let mut bytes = serde_json::to_vec_pretty(&attempts[1].record).unwrap();
        bytes.push(b'\n');
        std::fs::write(directory.join("model-change.json"), bytes).unwrap();
    }
}
