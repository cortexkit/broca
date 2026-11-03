use super::*;

async fn assert_socket_code_ends_without_resending(code: &str, retry_after: Option<&str>) {
    let mut frame = serde_json::json!({
        "type":"error", "status":if code == "flex_unavailable" { 429 } else { 503 },
        "error":{"code":code, "message":"scripted error"},
    });
    if let Some(seconds) = retry_after {
        frame["headers"] = serde_json::json!({"retry-after":seconds});
    }
    let mock = Mock::start(vec![vec![frame]; 8], true, false).await;
    *mock.http_error.lock().unwrap() = Some((
        if code == "flex_unavailable" { 429 } else { 503 },
        serde_json::json!({"error":{"code":code,"message":"scripted error"}}).to_string(),
    ));
    let mut provider = provider(&mock);
    virtual_attempts(&mut provider, &[0; 32]);
    // A long attempt must not affect the terminal decision. Codex has no total waiting budget,
    // and there is deliberately no step deadline.
    *provider
        .retry_clock
        .virtual_retry_attempts
        .as_ref()
        .unwrap()
        .lock()
        .unwrap() = std::iter::repeat_n(Duration::from_millis(10_582), 32).collect();
    let (events, logs) = logged_step(&provider).await;
    assert_eq!(
        attempts(&events).len(),
        1,
        "{code} must end on the first socket attempt"
    );
    assert_eq!(attempts(&events)[0].outcome, AttemptOutcome::HttpError);
    assert_eq!(terminal_error(&events).provider_code.as_deref(), Some(code));
    assert!(matches!(
        events.last(),
        Some(NormalizedEvent::Control(ProviderControl::Error {
            retries_used: 0,
            ..
        }))
    ));
    assert!(provider
        .retry_clock
        .virtual_waits
        .as_ref()
        .unwrap()
        .lock()
        .unwrap()
        .is_empty());
    let warnings = logs.0.lock().unwrap();
    assert_eq!(
        warnings.last().unwrap()["retry_reason"],
        if code == "flex_unavailable" {
            "codex_terminal"
        } else {
            "no_server_retry_time"
        }
    );
    let seen = mock.observed.lock().unwrap();
    assert_eq!(seen.requests.len(), 1);
    assert!(
        seen.http_bodies.is_empty(),
        "terminal socket errors must not fall back to HTTPS"
    );
}

#[tokio::test]
async fn websocket_flex_unavailable_with_server_time_ends_after_first_attempt() {
    assert_socket_code_ends_without_resending("flex_unavailable", Some("120")).await;
}

#[tokio::test]
async fn websocket_server_is_overloaded_without_server_time_ends_after_first_attempt() {
    assert_socket_code_ends_without_resending("server_is_overloaded", None).await;
}
