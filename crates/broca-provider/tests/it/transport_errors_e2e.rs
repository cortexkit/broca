//! HTTP error-mode e2e — wire status codes classify to the expected `ErrorClass`. Retryable
//! statuses fail once and then succeed on a sequenced fixture, proving the transport re-sends;
//! HTTP 401 instead PAUSES the run, while HTTP 403 remains a terminal permanent error. Driven through the shared rig
//! against aimock with deterministic per-fixture error injection (OpenAI Chat carrier;
//! taxonomy lives in the family-agnostic transport layer).

use crate::family_e2e;

use broca_core::OutputEvent;
use broca_protocol::error::ErrorClass;
use broca_protocol::{
    caller::{PauseReason, RunFinishReason},
    display::DisplayEvent,
};
use broca_provider::families::{openai_chat_frozen_config, OpenAiChat};
use family_e2e::{run_family_e2e, RunSpec};

/// Build a catch-all fixture that returns an aimock error response with the given HTTP
/// status (and optional `retryAfter` for 429).
fn error_fixtures(status: u16, retry_after: Option<u32>) -> String {
    let retry_field = retry_after
        .map(|s| format!(r#","retryAfter": {s}"#))
        .unwrap_or_default();
    format!(
        r#"{{
      "fixtures": [
        {{ "match": {{}},
          "response": {{
            "error": {{ "message": "injected {status}" }},
            "status": {status}
            {retry_field}
          }}
        }}
      ]
    }}"#
    )
}

fn retry_then_success_fixtures(status: u16, retry_after: Option<u32>) -> String {
    let retry_field = retry_after
        .map(|seconds| format!(r#", "retryAfter": {seconds}"#))
        .unwrap_or_default();
    format!(
        r#"{{
      "fixtures": [
        {{ "match": {{ "sequenceIndex": 0 }},
          "response": {{
            "error": {{ "message": "injected {status}" }},
            "status": {status}{retry_field}
          }}
        }},
        {{ "match": {{ "sequenceIndex": 1 }},
          "response": {{ "content": "retry succeeded" }}
        }}
      ]
    }}"#
    )
}

async fn assert_http_retry_succeeds(status: u16, retry_after: Option<u32>) {
    let started = tokio::time::Instant::now();
    let spec = RunSpec::new(retry_then_success_fixtures(status, retry_after))
        .prompt("trigger transient error");
    let Some(outcome) =
        run_family_e2e(OpenAiChat, openai_chat_frozen_config("mock-model"), spec).await
    else {
        panic!("aimock/node absent — test must run against live aimock, not skip");
    };

    assert!(
        outcome.first_error().is_none(),
        "HTTP {status} should be absorbed by a successful retry"
    );
    assert_eq!(outcome.summary.reason, RunFinishReason::Completed);
    assert_eq!(outcome.final_text(), "retry succeeded");
    assert_eq!(
        outcome
            .events
            .iter()
            .filter(|event| matches!(event, OutputEvent::Display(DisplayEvent::DisplayReset)))
            .count(),
        1,
        "the retry must reset the failed generation exactly once"
    );
    if let Some(seconds) = retry_after {
        assert!(
            started.elapsed() >= std::time::Duration::from_secs(seconds.into()),
            "Retry-After must pace the successful resend"
        );
    }
}

async fn assert_http_error_class(status: u16, expected: ErrorClass, retry_after: Option<u32>) {
    let spec = RunSpec::new(error_fixtures(status, retry_after)).prompt("trigger error");

    let Some(outcome) =
        run_family_e2e(OpenAiChat, openai_chat_frozen_config("mock-model"), spec).await
    else {
        panic!("aimock/node absent — test must run against live aimock, not skip");
    };

    let err = outcome
        .first_error()
        .unwrap_or_else(|| panic!("expected classified provider error for HTTP {status}"));
    assert_eq!(
        err.class, expected,
        "HTTP {status} should map per transport::http_error (got {:?}): {}",
        err.class, err.message
    );
    assert_eq!(
        outcome.summary.reason,
        RunFinishReason::Error,
        "run must terminate with Error after any transport retry budget is exhausted"
    );

    if let Some(secs) = retry_after {
        assert_eq!(
            err.retry_after_secs,
            Some(secs as f64),
            "Retry-After from aimock should parse onto ProviderError"
        );
    }
}

/// HTTP 500 is transient, so the second sequenced fixture completes the same step.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_500_retries_then_succeeds() {
    assert_http_retry_succeeds(500, None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_502_retries_then_succeeds() {
    assert_http_retry_succeeds(502, None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_503_retries_then_succeeds() {
    assert_http_retry_succeeds(503, None).await;
}

/// `transport.rs::http_error`: 401 → Auth, which the loop treats as a RESUMABLE pause (a
/// `Paused` control unit + a durable RunPaused) rather than a terminal Error — the credential can
/// be refreshed and the run continued. HTTP 403 is covered separately because it is permanent.
async fn assert_http_auth_pauses(status: u16) {
    let spec = RunSpec::new(error_fixtures(status, None)).prompt("trigger error");
    let Some(outcome) =
        run_family_e2e(OpenAiChat, openai_chat_frozen_config("mock-model"), spec).await
    else {
        panic!("aimock/node absent — test must run against live aimock, not skip");
    };

    let (reason, error) = outcome
        .first_pause()
        .unwrap_or_else(|| panic!("HTTP {status} (auth) should PAUSE the run"));
    assert_eq!(
        reason,
        PauseReason::AuthRequired,
        "an auth status pauses the run for re-auth"
    );
    // The pause carries the classified Auth error detail.
    assert_eq!(
        error.map(|e| e.class),
        Some(ErrorClass::Auth),
        "the pause carries the Auth-classified provider error"
    );
    // A pause is NOT a terminal Error and the run did not finish Completed.
    assert!(
        outcome.first_error().is_none(),
        "an auth pause emits Paused, not a terminal Error"
    );
    assert_eq!(
        outcome.summary.reason,
        RunFinishReason::Interrupted,
        "a paused run surfaces as Interrupted (resumable), not Completed/Error"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_401_pauses_for_reauth() {
    assert_http_auth_pauses(401).await;
}

/// `transport.rs::http_error`: 403 → Permanent; the credential is valid but forbidden, so
/// re-authentication would not repair the request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_403_is_permanent_not_reauth() {
    assert_http_error_class(403, ErrorClass::Permanent, None).await;
}

/// `transport.rs::http_error`: 400 | 404 → Permanent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_400_is_permanent() {
    assert_http_error_class(400, ErrorClass::Permanent, None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_404_is_permanent() {
    assert_http_error_class(404, ErrorClass::Permanent, None).await;
}

/// HTTP 429 is retried once, paced by Retry-After, then completes on the next fixture.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_429_retry_after_paces_then_succeeds() {
    assert_http_retry_succeeds(429, Some(2)).await;
}
