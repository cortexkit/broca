use super::*;
use crate::account_routing::RoutingReply;
use broca_core::AuthSelection;

const CAPTURE: &str =
    include_str!("../tests/fixtures/usage-observe/codex-quota-capture-2026-11-02.json");
const HEADERS_REQUEST: &str =
    include_str!("../tests/fixtures/usage-observe/broca-codex-headers-request.json");
const FRAME_REQUEST: &str =
    include_str!("../tests/fixtures/usage-observe/broca-codex-frame-request.json");
const HEADERS_REPLY: &str =
    include_str!("../tests/fixtures/usage-observe/broca-codex-headers-reply.json");
const FRAME_REPLY: &str =
    include_str!("../tests/fixtures/usage-observe/broca-codex-frame-reply.json");

fn selection(id: Option<&str>, account: Option<&str>) -> AuthSelection {
    AuthSelection {
        credential_id: "alias-not-a-vault-id".into(),
        resolved_credential_id: id.map(str::to_owned),
        account_ref: account.map(str::to_owned),
        billing_vault_facts: None,
        vault_request: None,
        account_decision: None,
        auth_method: None,
    }
}
fn tap(
    queue: &Arc<Queue>,
    provider: &str,
    method: &str,
    id: &str,
    account: Option<&str>,
) -> Arc<dyn QuotaObserver> {
    observer_for(
        provider,
        Some(method),
        Some(&selection(Some(id), account)),
        || Some(queue.clone()),
    )
    .unwrap()
}
fn header_map(value: &Value) -> reqwest::header::HeaderMap {
    let mut headers = reqwest::header::HeaderMap::new();
    for (name, value) in value.as_object().unwrap() {
        headers.insert(
            reqwest::header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
            value.as_str().unwrap().parse().unwrap(),
        );
    }
    headers
}
fn pending(queue: &Queue) -> Vec<Value> {
    queue
        .pending
        .lock()
        .unwrap()
        .values()
        .map(|p| serde_json::from_str(p.row.get()).unwrap())
        .collect()
}

#[test]
fn quota_oaiauth_capture_exact_rows_including_429() {
    let captures: Vec<Value> = serde_json::from_str(CAPTURE).unwrap();
    for (index, capture) in captures.iter().enumerate() {
        let queue = Arc::new(Queue::default());
        let observer = tap(
            &queue,
            "openai",
            "chatgpt",
            "vault-record",
            Some("vault-account"),
        );
        let at = 12345 + index as u64;
        let mut expected = json!({"provider":"codex", "credentialId":"vault-record", "account":"vault-account", "observedAtMs":at});
        if let Some(headers) = capture.get("headers") {
            let mut input = header_map(headers);
            input.insert(
                "x-codex-turn-state",
                "secret-conversation-token".parse().unwrap(),
            );
            input.insert("content-type", "text/event-stream".parse().unwrap());
            observer.headers(&input, at);
            expected["headers"] = headers.clone();
        } else {
            observer.frame(&capture["frame"].to_string(), at);
            expected["frame"] = capture["frame"].clone();
        }
        assert_eq!(
            pending(&queue),
            vec![expected],
            "capture {index}, status {}",
            capture["status"]
        );
    }
    assert_eq!(captures[0]["status"], 429);
    assert_eq!(captures[1]["status"], 200);
}

#[test]
fn quota_insula_vectors_pin_requests_and_reply_reasons() {
    for (input, reply, kind) in [
        (HEADERS_REQUEST, HEADERS_REPLY, Kind::Headers),
        (FRAME_REQUEST, FRAME_REPLY, Kind::Frame),
    ] {
        let expected: Value = serde_json::from_str(input).unwrap();
        let queue = Arc::new(Queue::default());
        for row in expected["params"]["observations"].as_array().unwrap() {
            let observer = tap(
                &queue,
                "openai",
                "chatgpt",
                row["credentialId"].as_str().unwrap(),
                None,
            );
            if kind == Kind::Headers {
                let mut headers = header_map(&row["headers"]);
                headers.insert("x-codex-turn-state", "never-forward".parse().unwrap());
                observer.headers(&headers, row["observedAtMs"].as_u64().unwrap());
            } else {
                observer.frame(
                    &row["frame"].to_string(),
                    row["observedAtMs"].as_u64().unwrap(),
                );
            }
        }
        let rows: Vec<_> = std::mem::take(&mut *queue.pending.lock().unwrap())
            .into_values()
            .map(|p| p.row)
            .collect();
        let actual: Value = serde_json::from_slice(&request(&rows)).unwrap();
        // Canonical re-serialization compares all fields, with deterministic object ordering.
        assert_eq!(
            serde_json::to_vec(&actual).unwrap(),
            serde_json::to_vec(&expected).unwrap()
        );
        queue.health.reply(reply.as_bytes()).unwrap();
        assert_eq!(
            queue.health.snapshot()["ignored"],
            json!({"unknown_credential":1})
        );
    }
}

#[test]
fn quota_claude_unified_headers_verbatim_success_and_refusal() {
    for status in [200, 429] {
        let queue = Arc::new(Queue::default());
        let observer = tap(&queue, "anthropic", "oauth", "claude-record", None);
        let headers = json!({
            "anthropic-ratelimit-unified-status":"allowed_warning",
            "anthropic-ratelimit-unified-5h-utilization":"0.5000",
            "anthropic-ratelimit-unified-reset":"1793774135",
        });
        let mut input = header_map(&headers);
        input.insert("x-codex-primary-used-percent", "100".parse().unwrap());
        observer.headers(&input, status);
        observer.frame(r#"{"type":"codex.rate_limits"}"#, status);
        assert_eq!(
            pending(&queue),
            vec![
                json!({"provider":"claude", "credentialId":"claude-record", "observedAtMs":status, "headers":headers})
            ]
        );
    }
}

#[test]
fn quota_ineligible_credentials_do_not_even_access_the_queue() {
    for (provider, method, id) in [
        ("anthropic", "apikey", Some("id")),
        ("openai", "apikey", Some("id")),
        ("other", "oauth", Some("id")),
        ("other", "chatgpt", Some("id")),
        ("anthropic", "oauth", None),
        ("openai", "chatgpt", None),
    ] {
        assert!(observer_for(
            provider,
            Some(method),
            Some(&selection(id, None)),
            || panic!("ineligible queue access")
        )
        .is_none());
    }
}

#[test]
fn quota_allowlist_includes_other_families_but_never_turn_state() {
    for name in [
        "x-codex-plan-type",
        "x-codex-credits-balance",
        "x-foo-primary-used-percent",
        "x-foo-secondary-reset-at",
        "x-foo-limit-name",
    ] {
        assert!(codex_header(name), "{name}");
    }
    for name in [
        "x-codex-turn-state",
        "x-primary-foo",
        "x-secondary-foo",
        "x-limit-name",
        "other",
        "authorization",
    ] {
        assert!(!codex_header(name), "{name}");
    }
}

#[test]
fn quota_raw_frame_keeps_number_and_whitespace_spelling() {
    let queue = Arc::new(Queue::default());
    let observer = tap(&queue, "openai", "chatgpt", "record", None);
    let frame = r#"{ "type": "codex.rate_limits", "rate_limits": {"used_percent":1.000e+00}, "raw":"\u0072" }"#;
    observer.frame(frame, 3);
    let locked = queue.pending.lock().unwrap();
    let row = locked.values().next().unwrap().row.get();
    assert!(row.contains(frame), "{row}");
}

#[test]
fn quota_oversize_overflow_and_contention_are_counted() {
    let queue = Arc::new(Queue::default());
    let observer = tap(&queue, "openai", "chatgpt", "record", None);
    observer.frame(
        &format!(
            r#"{{"type":"codex.rate_limits","extra":"{}"}}"#,
            "a".repeat(ROW_LIMIT)
        ),
        1,
    );
    assert!(pending(&queue).is_empty());
    for i in 0..PENDING_LIMIT {
        tap(&queue, "openai", "chatgpt", &i.to_string(), None)
            .frame(r#"{"type":"codex.rate_limits"}"#, 1);
    }
    observer.frame(r#"{"type":"codex.rate_limits"}"#, 1);
    let locked = queue.pending.lock().unwrap();
    observer.frame(r#"{"type":"codex.rate_limits"}"#, 2);
    assert_eq!(locked.len(), PENDING_LIMIT);
    assert_eq!(queue.health.rows_dropped.load(Relaxed), 3);
}

#[derive(Default)]
struct FakeTransport {
    requests: Mutex<Vec<Value>>,
    fail: bool,
}
#[async_trait::async_trait]
impl RoutingTransport for FakeTransport {
    async fn request(&self, body: Vec<u8>) -> RoutingReply {
        self.requests
            .lock()
            .unwrap()
            .push(serde_json::from_slice(&body).unwrap());
        if self.fail {
            Err("offline".into())
        } else {
            Ok(Ok(HEADERS_REPLY.as_bytes().to_vec()))
        }
    }
}
async fn settle() {
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(start_paused = true)]
async fn quota_coalesces_per_identity_provider_kind_and_batches_every_30s() {
    let queue = Arc::new(Queue::default());
    let transport = Arc::new(FakeTransport::default());
    spawn_sender(queue.clone(), transport.clone());
    settle().await;
    let observer = tap(&queue, "openai", "chatgpt", "record", None);
    for (at, value) in [(10, "1"), (9, "stale"), (10, "tie-latest")] {
        observer.headers(&header_map(&json!({"x-codex-plan-type":value})), at);
    }
    observer.frame(r#"{"type":"codex.rate_limits"}"#, 11);
    tap(&queue, "anthropic", "oauth", "record", None).headers(
        &header_map(&json!({"anthropic-ratelimit-unified-status":"allowed"})),
        12,
    );
    tap(&queue, "openai", "chatgpt", "other", None)
        .headers(&header_map(&json!({"x-codex-plan-type":"pro"})), 13);
    tokio::time::advance(Duration::from_secs(29)).await;
    settle().await;
    assert!(transport.requests.lock().unwrap().is_empty());
    tokio::time::advance(Duration::from_secs(1)).await;
    settle().await;
    let requests = transport.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let rows = requests[0]["params"]["observations"].as_array().unwrap();
    assert_eq!(rows.len(), 4);
    assert!(rows
        .iter()
        .any(|r| r["headers"]["x-codex-plan-type"] == "tie-latest" && r["observedAtMs"] == 10));
    drop(requests);
    assert!(pending(&queue).is_empty());
    assert_eq!(
        queue.health.snapshot(),
        json!({"rows_sent":4,"batches_sent":1,"transport_failures":0,"rows_dropped":0,"ignored":{"unknown_credential":1}})
    );
}

#[tokio::test(start_paused = true)]
async fn quota_failed_batch_is_one_shot_and_health_counts_failure() {
    let queue = Arc::new(Queue::default());
    let transport = Arc::new(FakeTransport {
        fail: true,
        ..Default::default()
    });
    spawn_sender(queue.clone(), transport.clone());
    settle().await;
    tap(&queue, "openai", "chatgpt", "record", None).frame(r#"{"type":"codex.rate_limits"}"#, 1);
    tokio::time::advance(INTERVAL).await;
    settle().await;
    tokio::time::advance(INTERVAL * 4).await;
    settle().await;
    assert_eq!(transport.requests.lock().unwrap().len(), 1);
    assert_eq!(
        queue.health.snapshot(),
        json!({"rows_sent":1,"batches_sent":1,"transport_failures":1,"rows_dropped":0,"ignored":{}})
    );
}

#[test]
fn quota_equal_timestamp_uses_receipt_order_not_builder_completion_order() {
    let queue = Queue::default();
    for (receive_order, value) in [(2, "latest-received"), (1, "slow-earlier-builder")] {
        queue.push(
            Row {
                provider: Provider::Codex,
                credential_id: "record",
                account: None,
                observed_at_ms: 7,
                headers: Some(BTreeMap::from([("x-codex-plan-type", value)])),
                frame: None,
            },
            Kind::Headers,
            receive_order,
        );
    }
    assert_eq!(
        pending(&queue)[0]["headers"]["x-codex-plan-type"],
        "latest-received"
    );
}

#[test]
fn quota_frame_gate_skips_parse_and_atomic_for_ordinary_output() {
    let queue = Arc::new(Queue::default());
    let observer = tap(&queue, "openai", "chatgpt", "record", None);
    for frame in [
        r#"{"type":"response.output_text.delta","delta":"ordinary"}"#,
        r#"{"type":"response.completed","response":{"output":[]}}"#,
        "not JSON",
    ] {
        observer.frame(frame, 1);
    }
    assert_eq!(queue.receive_order.load(Relaxed), 0);
    assert!(pending(&queue).is_empty());
}

#[test]
fn quota_marker_in_text_delta_is_parsed_but_not_forwarded() {
    let queue = Arc::new(Queue::default());
    let observer = tap(&queue, "openai", "chatgpt", "record", None);
    observer.frame(
        r#"{"type":"response.output_text.delta","delta":"codex.rate_limits"}"#,
        1,
    );
    assert_eq!(queue.receive_order.load(Relaxed), 1);
    assert!(pending(&queue).is_empty());
}

#[test]
#[ignore = "one-off optimized-build quota frame timing; run with --release --ignored --nocapture"]
fn quota_frame_hot_path_release_timing() {
    use std::{hint::black_box, time::Instant};
    assert!(!black_box(cfg!(debug_assertions)), "measure with --release");
    let observer = Observer {
        queue: Arc::new(Queue::default()),
        provider: Provider::Codex,
        credential_id: "record".into(),
        account: None,
    };
    let mut delta = json!({"type":"response.output_text.delta","delta":""});
    let padding = 200 - delta.to_string().len();
    delta["delta"] = json!("x".repeat(padding));
    let delta = delta.to_string();
    let mut completed = json!({"type":"response.completed","response":{"id":"r","status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":""}]}]}});
    let padding = 100 * 1024 - completed.to_string().len();
    completed["response"]["output"][0]["content"][0]["text"] = json!("x".repeat(padding));
    let completed = completed.to_string();
    assert_eq!(delta.len(), 200);
    assert_eq!(completed.len(), 100 * 1024);
    fn elapsed_ns(iterations: u32, mut run: impl FnMut()) -> f64 {
        let start = Instant::now();
        for _ in 0..iterations {
            run();
        }
        start.elapsed().as_nanos() as f64 / f64::from(iterations)
    }
    for (label, frame, iterations) in [
        ("delta", &delta, 100_000),
        ("completed", &completed, 10_000),
    ] {
        for _ in 0..1000 {
            observer.observe_candidate(black_box(frame), 0);
            observer.frame(black_box(frame), 0);
        }
        let mut before = Vec::new();
        let mut after = Vec::new();
        for _ in 0..9 {
            // Bypass only the new gate to reproduce the prior atomic plus full envelope parse.
            before.push(elapsed_ns(iterations, || {
                observer.observe_candidate(black_box(frame), 0)
            }));
            after.push(elapsed_ns(iterations, || {
                observer.frame(black_box(frame), 0)
            }));
        }
        before.sort_by(f64::total_cmp);
        after.sort_by(f64::total_cmp);
        println!("quota frame timing: {label} bytes={} samples=9 iterations_per_sample={iterations} before_ns={:.2} after_ns={:.2}", frame.len(), before[4], after[4]);
    }
    assert!(pending(&observer.queue).is_empty());
}
