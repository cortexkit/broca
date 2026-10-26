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
    let mock = Mock::start(vec![text_events()], false, false).await;
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
    assert!(!codex_trace(&step(&oldest, 1, &options).await, 0).continuation);
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
    assert!(
        !codex_trace(
            &step(&second, 1, &follow_up(options, &events, "Next")).await,
            0
        )
        .continuation
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
}
