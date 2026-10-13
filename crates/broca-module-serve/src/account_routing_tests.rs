//! Tests for account selection through routing. The observe bodies are compared, as parsed
//! JSON, with the bodies routing's own tests send for broca (prefrontal
//! `crates/prefrontal-routing-module/src/account_selection.rs`, the `broca_*` and
//! `observations_and_releases_*` tests at 92b42c1de), so a drift in either side's field names
//! shows here.

use super::*;
use broca_protocol::{AttemptAccountLimit, AttemptAccountObservation, AttemptOutcome};
use broca_subc::vault::ScopedCredential;
use std::sync::atomic::AtomicUsize;

/// Routing's test clock, used in its pinned bodies.
const NOW: i64 = 1_790_683_200_000;

/// A routing stand-in: answers each request from a script and keeps every request it got.
#[derive(Default)]
struct FakeRouting {
    requests: Mutex<Vec<Value>>,
    reply: Mutex<Option<RoutingReply>>,
    /// Never answer, as a hung routing module would.
    hang: bool,
    answered: AtomicUsize,
}

impl FakeRouting {
    fn answering(reply: RoutingReply) -> Arc<Self> {
        Arc::new(Self {
            reply: Mutex::new(Some(reply)),
            ..Self::default()
        })
    }

    fn refusing(code: &str, detail: Value) -> Arc<Self> {
        Self::answering(Ok(Err(serde_json::to_vec(&json!({
            "code": code,
            "message": format!("refused: {code}"),
            "detail": detail,
        }))
        .unwrap())))
    }

    fn choosing(account_id: &str, credential_id: &str) -> Arc<Self> {
        Self::answering(Ok(Ok(serde_json::to_vec(&json!({ "result": {
            "admission": { "id": "run-1", "state": "reserved" },
            "account": {
                "provider": "anthropic",
                "account_id": account_id,
                "credential_id": credential_id,
                "refresh_adapter": "anthropic",
                "method": "oauth",
            },
            "pin": { "id": "pin-1", "generation": 1, "mode": "run_frozen" },
            "reason": { "code": "cold_best", "decision_id": "acd-7", "policy_version": 2 },
        }}))
        .unwrap())))
    }

    fn hung() -> Arc<Self> {
        Arc::new(Self {
            hang: true,
            ..Self::default()
        })
    }

    fn methods(&self) -> Vec<String> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .map(|request| request["method"].as_str().unwrap_or_default().to_string())
            .collect()
    }

    fn observed(&self) -> Vec<Value> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request["method"] == ACCOUNT_OBSERVE_OP)
            .map(|request| request["params"].clone())
            .collect()
    }
}

#[async_trait::async_trait]
impl RoutingTransport for FakeRouting {
    async fn request(&self, body: Vec<u8>) -> RoutingReply {
        self.requests
            .lock()
            .unwrap()
            .push(serde_json::from_slice(&body).unwrap());
        if self.hang {
            std::future::pending::<()>().await;
        }
        self.answered.fetch_add(1, Ordering::SeqCst);
        self.reply
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| Ok(Ok(br#"{"result":{}}"#.to_vec())))
    }
}

fn client(routing: &Arc<FakeRouting>) -> RoutingClient {
    RoutingClient::new(
        Arc::clone(routing) as Arc<dyn RoutingTransport>,
        AccountSelectionConfig::default(),
    )
}

fn row(id: &str, account_id: Option<&str>) -> ScopedCredential {
    ScopedCredential {
        id: id.into(),
        categories: vec!["anthropic-native".into()],
        credential_type: "oauth".into(),
        serves: vec!["anthropic".into()],
        refresh_adapter: Some("anthropic".into()),
        state: "active".into(),
        record_version: 1,
        operations: vec!["read".into()],
        account_id: account_id.map(str::to_string),
        email: None,
        org_name: None,
        provider_ids: Vec::new(),
        auth_method: None,
    }
}

fn listing(rows: Vec<ScopedCredential>) -> Arc<ScopedList> {
    Arc::new(ScopedList {
        credentials: rows,
        grants: 1,
        grant_tuples: Vec::new(),
        view: "vault-view-1".into(),
    })
}

fn two_accounts() -> Arc<ScopedList> {
    listing(vec![
        row("oauth:anthropic", Some("acct-a")),
        row("oauth:anthropic:acct-b", Some("acct-b")),
        row("apikey:anthropic", None),
        row("chatgpt:openai", Some("acct-c")),
    ])
}

fn session() -> BindIdentity {
    BindIdentity::new("/tmp/project", "runner", "session")
}

async fn choose(
    client: &RoutingClient,
    static_credential: &str,
    caller_named_account: bool,
    listing: Option<Arc<ScopedList>>,
) -> Result<AccountChoice, AccountSelectionRefusal> {
    let static_credential = CredentialId::parse(static_credential).unwrap();
    choose_account(
        client,
        ChoiceInputs {
            conversation_key: None,
            inherited_conversation_key: None,
            static_credential: &static_credential,
            caller_named_account,
            model: "claude-opus-5-5",
            session: &session(),
            run_id: "run-1",
            initial_input_bytes: 42,
        },
        async move { listing },
        tokio::time::Instant::now() + ADMISSION_BUDGET,
    )
    .await
}

fn path_of(choice: &AccountChoice) -> &str {
    choice.decision().map(|d| d.path.as_str()).unwrap_or("none")
}

// ---- admission ------------------------------------------------------------------------------

#[tokio::test]
async fn account_selection_disabled_takes_the_static_pick_without_retrying() {
    let routing = FakeRouting::refusing(SELECTION_DISABLED, json!({ "code": SELECTION_DISABLED }));
    let client = client(&routing);
    let choice = choose(&client, "oauth:anthropic", false, Some(two_accounts()))
        .await
        .unwrap();
    assert_eq!(
        choice,
        AccountChoice::Static(AccountDecision {
            conversation_key: Some(conversation_key(&session())),
            conversation_key_source: Some(broca_wal::ConversationKeySource::Session),
            path: "disabled".into(),
            admission_id: Some("run-1".into()),
            decision_id: None,
        })
    );
    assert_eq!(
        routing.methods(),
        vec![ACCOUNT_FOR_OP],
        "disabled is an answer: asked once, never again"
    );
    assert_eq!(client.counters.get("disabled"), 1);
}

#[tokio::test]
async fn routing_unreachable_falls_back_with_the_counter_raised() {
    let routing = FakeRouting::answering(Err("route.open refused: target_unavailable".into()));
    let client = client(&routing);
    let choice = choose(&client, "oauth:anthropic", false, Some(two_accounts()))
        .await
        .expect("routing being down never refuses an admission");
    assert_eq!(path_of(&choice), "unreachable");
    assert!(matches!(choice, AccountChoice::Static(_)));
    assert_eq!(client.counters.get("unreachable"), 1);
    assert_eq!(client.counters.snapshot()["unreachable"], 1);
}

#[tokio::test]
async fn a_malformed_routing_reply_falls_back_as_unreachable() {
    for reply in [
        Ok(Ok(b"not json".to_vec())),
        Ok(Ok(br#"{"result":{"account":{}}}"#.to_vec())),
        Ok(Err(b"not an error body".to_vec())),
        Ok(Err(
            br#"{"code":"route_state_unavailable","message":"store"}"#.to_vec(),
        )),
    ] {
        let routing = FakeRouting::answering(reply);
        let client = client(&routing);
        let choice = choose(&client, "oauth:anthropic", false, Some(two_accounts()))
            .await
            .unwrap();
        assert_eq!(path_of(&choice), "unreachable");
    }
}

#[tokio::test]
async fn a_hung_routing_costs_at_most_the_admission_budget() {
    let routing = FakeRouting::hung();
    let client = client(&routing);
    let static_credential = CredentialId::parse("oauth:anthropic").unwrap();
    let budget = Duration::from_millis(250);
    let started = std::time::Instant::now();
    let choice = choose_account(
        &client,
        ChoiceInputs {
            conversation_key: None,
            inherited_conversation_key: None,
            static_credential: &static_credential,
            caller_named_account: false,
            model: "claude-opus-5-5",
            session: &session(),
            run_id: "run-1",
            initial_input_bytes: 0,
        },
        async { Some(two_accounts()) },
        tokio::time::Instant::now() + budget,
    )
    .await
    .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(path_of(&choice), "unreachable");
    assert_eq!(
        routing.methods(),
        vec![ACCOUNT_FOR_OP],
        "the request was sent"
    );
    assert!(
        elapsed >= budget && elapsed < budget + Duration::from_millis(500),
        "waited {elapsed:?} for a {budget:?} budget"
    );
    // The production budget leaves the caller's 10 s send deadline room for a 5 s vault read.
    assert!(ADMISSION_BUDGET <= Duration::from_secs(2));
}

#[tokio::test]
async fn a_slow_listing_shares_the_admission_budget() {
    let routing = FakeRouting::choosing("acct-b", "oauth:anthropic:acct-b");
    let client = client(&routing);
    let static_credential = CredentialId::parse("oauth:anthropic").unwrap();
    let started = std::time::Instant::now();
    let choice = choose_account(
        &client,
        ChoiceInputs {
            conversation_key: None,
            inherited_conversation_key: None,
            static_credential: &static_credential,
            caller_named_account: false,
            model: "claude-opus-5-5",
            session: &session(),
            run_id: "run-1",
            initial_input_bytes: 0,
        },
        std::future::pending::<Option<Arc<ScopedList>>>(),
        tokio::time::Instant::now() + Duration::from_millis(200),
    )
    .await
    .unwrap();
    assert!(started.elapsed() < Duration::from_millis(700));
    assert_eq!(path_of(&choice), "candidates_unknown");
    assert!(routing.methods().is_empty());
}

#[tokio::test]
async fn a_caller_pin_is_kept_and_routing_is_not_asked() {
    let routing = FakeRouting::choosing("acct-b", "oauth:anthropic:acct-b");
    let client = client(&routing);
    let choice = choose(&client, "oauth:anthropic:work", true, Some(two_accounts()))
        .await
        .unwrap();
    assert_eq!(path_of(&choice), "pinned");
    assert_eq!(
        choice.decision().unwrap().admission_id.as_deref(),
        Some("run-1"),
        "a pinned run is still reported"
    );
    assert!(routing.methods().is_empty());
}

#[tokio::test]
async fn one_candidate_account_asks_nothing() {
    let routing = FakeRouting::choosing("acct-b", "oauth:anthropic:acct-b");
    let client = client(&routing);
    // Two records of ONE account are one candidate.
    let one = listing(vec![
        row("oauth:anthropic", Some("acct-a")),
        row("oauth:anthropic:again", Some("acct-a")),
    ]);
    let choice = choose(&client, "oauth:anthropic", false, Some(one))
        .await
        .unwrap();
    assert_eq!(path_of(&choice), "single_candidate");
    assert!(routing.methods().is_empty());
}

#[tokio::test]
async fn legacy_static_neither_asks_nor_reports() {
    let routing = FakeRouting::choosing("acct-b", "oauth:anthropic:acct-b");
    let client = RoutingClient::new(
        Arc::clone(&routing) as Arc<dyn RoutingTransport>,
        AccountSelectionConfig {
            legacy_static: true,
            ..AccountSelectionConfig::default()
        },
    );
    let choice = choose(&client, "oauth:anthropic", false, Some(two_accounts()))
        .await
        .unwrap();
    assert_eq!(path_of(&choice), "legacy_static");
    assert_eq!(choice.decision().unwrap().admission_id, None);
    assert!(routing.methods().is_empty());
}

#[tokio::test]
async fn an_api_key_run_is_not_an_account_choice() {
    let routing = FakeRouting::choosing("acct-b", "oauth:anthropic:acct-b");
    let client = client(&routing);
    let choice = choose(&client, "apikey:anthropic", false, Some(two_accounts()))
        .await
        .unwrap();
    assert_eq!(choice, AccountChoice::NotApplicable);
    assert!(routing.methods().is_empty());
}

#[tokio::test]
async fn a_routing_answer_names_the_credential_and_the_decision() {
    let routing = FakeRouting::choosing("acct-b", "oauth:anthropic:acct-b");
    let client = client(&routing);
    let choice = choose(&client, "oauth:anthropic", false, Some(two_accounts()))
        .await
        .unwrap();
    assert_eq!(
        choice,
        AccountChoice::Routed {
            account_id: "acct-b".into(),
            credential_id: "oauth:anthropic:acct-b".into(),
            decision: AccountDecision {
                conversation_key: Some(conversation_key(&session())),
                conversation_key_source: Some(broca_wal::ConversationKeySource::Session),
                path: "routed".into(),
                admission_id: Some("run-1".into()),
                decision_id: Some("acd-7".into()),
            },
            view_digest: "vault-view-1".into(),
        }
    );
    let request = routing.requests.lock().unwrap()[0].clone();
    assert_eq!(
        request["params"],
        json!({
            "provider": "anthropic",
            "model": "claude-opus-5-5",
            "affinity_model": "claude-opus-5-5",
            "conversation_key": conversation_key(&session()),
            "caller": { "runner": "broca" },
            "admission_id": "run-1",
            "mode": "admit",
            "initial_input_bytes": 42,
            "initial_input_estimated": true,
            "candidates": {
                "view_digest": "vault-view-1",
                "accounts": [
                    { "account_id": "acct-a", "credential_id": "oauth:anthropic" },
                    { "account_id": "acct-b", "credential_id": "oauth:anthropic:acct-b" },
                ],
            },
        }),
        "only this provider's oauth rows that name an account are candidates"
    );
}

#[tokio::test]
async fn caller_affinity_changes_only_the_conversation_key_in_the_routing_body() {
    let routing = FakeRouting::choosing("acct-b", "oauth:anthropic:acct-b");
    let client = client(&routing);
    choose(&client, "oauth:anthropic", false, Some(two_accounts()))
        .await
        .unwrap();
    let legacy = routing.requests.lock().unwrap()[0]["params"].clone();
    let credential = CredentialId::parse("oauth:anthropic").unwrap();
    let choice = choose_account(
        &client,
        ChoiceInputs {
            conversation_key: Some("consult:42:subject:phase"),
            inherited_conversation_key: None,
            static_credential: &credential,
            caller_named_account: false,
            model: "claude-opus-5-5",
            session: &session(),
            run_id: "run-1",
            initial_input_bytes: 42,
        },
        async { Some(two_accounts()) },
        tokio::time::Instant::now() + ADMISSION_BUDGET,
    )
    .await
    .unwrap();
    assert_eq!(
        choice.decision().unwrap().conversation_key_source,
        Some(broca_wal::ConversationKeySource::Caller)
    );
    let mut expected = legacy;
    expected["conversation_key"] = json!("consult:42:subject:phase");
    assert_eq!(
        serde_json::to_vec(&routing.requests.lock().unwrap()[1]["params"]).unwrap(),
        serde_json::to_vec(&expected).unwrap()
    );
}

#[tokio::test]
async fn a_routing_answer_for_another_provider_is_not_used() {
    let routing = FakeRouting::choosing("acct-c", "chatgpt:openai");
    let client = client(&routing);
    let choice = choose(&client, "oauth:anthropic", false, Some(two_accounts()))
        .await
        .unwrap();
    assert_eq!(path_of(&choice), "unreachable");
}

#[tokio::test]
async fn two_admissions_of_the_same_send_send_the_same_admission_id() {
    let mut bodies = Vec::new();
    for _ in 0..2 {
        let routing = FakeRouting::refusing(SELECTION_DISABLED, json!({}));
        let client = client(&routing);
        choose(&client, "oauth:anthropic", false, Some(two_accounts()))
            .await
            .unwrap();
        bodies.push(routing.requests.lock().unwrap()[0].clone());
    }
    assert_eq!(bodies[0]["params"]["admission_id"], "run-1");
    assert_eq!(
        serde_json::to_vec(&bodies[0]).unwrap(),
        serde_json::to_vec(&bodies[1]).unwrap(),
        "a retry of the same send must present the same admission, byte for byte"
    );
    assert_eq!(admission_id("run-1"), admission_id("run-1"));
}

#[tokio::test]
async fn account_selection_unsupported_falls_back_quietly_with_a_counter() {
    let routing = FakeRouting::refusing(SELECTION_UNSUPPORTED, json!({}));
    let client = client(&routing);
    let choice = choose(&client, "oauth:anthropic", false, Some(two_accounts()))
        .await
        .unwrap();
    assert_eq!(path_of(&choice), "unsupported");
    assert_eq!(client.counters.get("unsupported"), 1);
}

#[tokio::test]
async fn auth_required_admits_for_a_fresh_login() {
    let routing = FakeRouting::refusing(AUTH_REQUIRED, json!({ "decision_id": "acd-9" }));
    let client = client(&routing);
    let choice = choose(&client, "oauth:anthropic", false, Some(two_accounts()))
        .await
        .unwrap();
    assert_eq!(
        choice,
        AccountChoice::AuthRequired(AccountDecision {
            conversation_key: Some(conversation_key(&session())),
            conversation_key_source: Some(broca_wal::ConversationKeySource::Session),
            path: "auth_required".into(),
            admission_id: Some("run-1".into()),
            decision_id: Some("acd-9".into()),
        })
    );
}

async fn refusal_for(code: &str) -> AccountSelectionRefusal {
    let routing = FakeRouting::refusing(
        code,
        json!({ "code": code, "retry_after_s": 1260, "decision_id": "acd-3", "exclusions": [] }),
    );
    let client = client(&routing);
    let refusal = choose(&client, "oauth:anthropic", false, Some(two_accounts()))
        .await
        .expect_err("refused");
    assert_eq!(routing.methods(), vec![ACCOUNT_FOR_OP], "asked once");
    assert_eq!(client.counters.get(&format!("refused.{code}")), 1);
    refusal
}

fn transient(code: &str) -> AccountSelectionRefusal {
    AccountSelectionRefusal {
        routing_code: code.into(),
        retry_after_s: Some(1260),
        decision_id: Some("acd-3".into()),
        class: ErrorClass::Transient,
    }
}

fn permanent(code: &str) -> AccountSelectionRefusal {
    AccountSelectionRefusal {
        class: ErrorClass::Permanent,
        ..transient(code)
    }
}

#[tokio::test]
async fn account_hold_is_refused_transient() {
    assert_eq!(refusal_for("account_hold").await, transient("account_hold"));
}

#[tokio::test]
async fn account_pool_exhausted_is_refused_not_a_fallback() {
    assert_eq!(
        refusal_for("account_pool_exhausted").await,
        transient("account_pool_exhausted")
    );
}

#[tokio::test]
async fn account_pool_reserve_limited_is_refused_transient() {
    assert_eq!(
        refusal_for("account_pool_reserve_limited").await,
        transient("account_pool_reserve_limited")
    );
}

#[tokio::test]
async fn account_quota_unknown_is_refused_transient() {
    assert_eq!(
        refusal_for("account_quota_unknown").await,
        transient("account_quota_unknown")
    );
}

#[tokio::test]
async fn account_selection_unavailable_is_refused_transient_not_a_fallback() {
    assert_eq!(
        refusal_for("account_selection_unavailable").await,
        transient("account_selection_unavailable")
    );
}

#[tokio::test]
async fn account_not_authorized_is_refused_permanent() {
    assert_eq!(
        refusal_for("account_not_authorized").await,
        permanent("account_not_authorized")
    );
}

#[tokio::test]
async fn admission_conflict_is_refused_permanent() {
    assert_eq!(
        refusal_for("admission_conflict").await,
        permanent("admission_conflict")
    );
}

#[tokio::test]
async fn invalid_request_is_refused_permanent() {
    assert_eq!(
        refusal_for("invalid_request").await,
        permanent("invalid_request")
    );
}

#[test]
fn a_refusal_without_a_wait_or_decision_copies_their_absence() {
    let outcome = classify_account_for_reply(Err(
        br#"{"code":"invalid_request","message":"m","detail":{"code":"invalid_request","retry_after_s":null,"decision_id":null,"exclusions":[]}}"#.to_vec(),
    ));
    assert_eq!(
        outcome,
        AccountForOutcome::Refused(AccountSelectionRefusal {
            routing_code: "invalid_request".into(),
            retry_after_s: None,
            decision_id: None,
            class: ErrorClass::Permanent,
        })
    );
}

// ---- configuration --------------------------------------------------------------------------

#[test]
fn the_break_glass_defaults_off_and_a_misspelling_is_refused() {
    assert_eq!(
        AccountSelectionConfig::parse("{}").unwrap(),
        AccountSelectionConfig::default()
    );
    assert!(!AccountSelectionConfig::default().legacy_static);
    let on = AccountSelectionConfig::parse(
        r#"{ // operator break-glass
            "account_selection": { "legacy_static": true, "routing_module": "routing-b", },
            "providers": {} }"#,
    )
    .unwrap();
    assert!(on.legacy_static);
    assert_eq!(on.routing_module, "routing-b");
    let error = AccountSelectionConfig::parse(r#"{"account_selection":{"legacy_statc":true}}"#)
        .unwrap_err();
    assert!(error.contains("unknown field"), "{error}");
}

// ---- report bodies --------------------------------------------------------------------------

fn attempt(http_status: Option<u16>, observation: AttemptAccountObservation) -> ModelAttempt {
    ModelAttempt {
        attempt: 0,
        request_body_len: 10,
        request_body_sha256: "aa".into(),
        http_status,
        content_type: None,
        response_body_len: 0,
        response_body_sha256: "bb".into(),
        response_body_cap: None,
        outcome: if http_status.is_some() {
            AttemptOutcome::Complete
        } else {
            AttemptOutcome::ConnectFailed
        },
        ts_ms: NOW as u64,
        sent_at_ms: None,
        transport: None,
        websocket_request: None,
        websocket_close_code: None,
        input_transformations: None,
        credential_refresh: None,
        served_account_ref: None,
        account_observation: Box::new(observation),
    }
}

fn headers(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect()
}

#[test]
fn admission_started_matches_routings_pinned_body() {
    let routing_pin = json!({
        "event_id": "lineage:4", "caller": { "runner": "broca" },
        "kind": "admission_started", "admission_id": "adm-finish", "run_id": "run-finish"
    });
    assert_eq!(
        admission_started_body("lineage:4", "adm-finish", "run-finish"),
        routing_pin
    );
}

#[test]
fn attempt_with_headers_matches_routings_pinned_body() {
    let routing_pin = json!({
        "event_id": "lineage:1",
        "caller": { "runner": "broca" },
        "kind": "attempt",
        "admission_id": "adm-raw",
        "upstream_origin": "upstream",
        "observed_at_ms": NOW,
        "view_digest": "credential-read-view",
        "headers": {
            "anthropic-ratelimit-unified-5h-utilization": "0.995",
            "anthropic-ratelimit-unified-5h-reset": ((NOW + 3_600_000) / 1000).to_string(),
            "anthropic-ratelimit-unified-fallback-percentage": "0.12345"
        }
    });
    let observed = attempt(
        Some(200),
        AttemptAccountObservation {
            ratelimit_headers: headers(&[
                ("anthropic-ratelimit-unified-5h-utilization", "0.995"),
                (
                    "anthropic-ratelimit-unified-5h-reset",
                    &((NOW + 3_600_000) / 1000).to_string(),
                ),
                ("anthropic-ratelimit-unified-fallback-percentage", "0.12345"),
            ]),
            limit: None,
        },
    );
    assert_eq!(
        attempt_body(
            "lineage:1",
            "adm-raw",
            &observed,
            Some("credential-read-view")
        ),
        routing_pin
    );
}

#[test]
fn attempt_without_a_status_line_matches_routings_pinned_body() {
    let routing_pin = json!({ "event_id": "lineage:3", "caller": { "runner": "broca" },
        "kind": "attempt", "admission_id": "adm-transport",
        "upstream_origin": "upstream", "terminal_reason": "transport_failure" });
    let observed = attempt(None, AttemptAccountObservation::default());
    assert_eq!(
        attempt_body("lineage:3", "adm-transport", &observed, Some("ignored")),
        routing_pin
    );
}

#[test]
fn account_limit_exhausted_matches_routings_pinned_body() {
    let routing_pin = json!({
        "event_id": "e-2",
        "caller": { "runner": "broca" },
        "kind": "account_limit_exhausted",
        "admission_id": "adm-1",
        "limit_window": "seven_day",
        "resets_at_ms": NOW + 3_600_000,
        "resets_at_source": "header",
        "observed_at_ms": NOW + 1_000,
    });
    let limit = AttemptAccountLimit {
        resets_at_ms: Some((NOW + 3_600_000) as u64),
        resets_at_source: Some("header".into()),
        limit_window: Some("seven_day".into()),
    };
    let mut observed = attempt(
        Some(429),
        AttemptAccountObservation {
            ratelimit_headers: BTreeMap::new(),
            limit: Some(limit.clone()),
        },
    );
    observed.ts_ms = (NOW + 1_000) as u64;
    assert_eq!(attempt_body("e-2", "adm-1", &observed, None), routing_pin);

    // With headers, the same body plus the headers and their origin, nothing else.
    observed.account_observation.ratelimit_headers =
        headers(&[("anthropic-ratelimit-unified-status", "rejected")]);
    let mut expected = routing_pin;
    expected["upstream_origin"] = json!("upstream");
    expected["headers"] = json!({ "anthropic-ratelimit-unified-status": "rejected" });
    assert_eq!(attempt_body("e-2", "adm-1", &observed, None), expected);
}

/// Routing's pin spells the terminal `success`; broca sends its own terminal reason name
/// (`completed`), which routing keeps without reading. Every other member must match.
#[test]
fn run_finished_matches_routings_pinned_body() {
    let routing_pin = json!({ "event_id": "lineage:5", "caller": { "runner": "broca" },
        "kind": "run_finished", "admission_id": "adm-finish", "run_id": "run-finish",
        "terminal_reason": "success", "upstream_origin": "upstream", "headers": {
            "anthropic-ratelimit-unified-7d-utilization": "1.0"
        } });
    let latest = headers(&[("anthropic-ratelimit-unified-7d-utilization", "1.0")]);
    let mut body = run_finished_body(
        "lineage:5",
        "adm-finish",
        "run-finish",
        RunFinishReason::Completed,
        Some(&latest),
    );
    assert_eq!(body["terminal_reason"], "completed");
    body["terminal_reason"] = json!("success");
    assert_eq!(body, routing_pin);

    // Routing's follow-up pin removes the headers and keeps the rest.
    let mut no_headers = routing_pin;
    no_headers.as_object_mut().unwrap().remove("headers");
    let mut body = run_finished_body(
        "lineage:5",
        "adm-finish",
        "run-finish",
        RunFinishReason::Completed,
        None,
    );
    body["terminal_reason"] = json!("success");
    assert_eq!(body, no_headers);
}

#[test]
fn scoped_not_found_carries_what_routing_excludes_by() {
    assert_eq!(
        scoped_not_found_body(
            "adm-1:scoped_not_found:1",
            "adm-1",
            "oauth:anthropic:acct-b",
            "view-1"
        ),
        json!({
            "event_id": "adm-1:scoped_not_found:1",
            "caller": { "runner": "broca" },
            "kind": "scoped_not_found",
            "admission_id": "adm-1",
            "credential_id": "oauth:anthropic:acct-b",
            "view_digest": "view-1",
        })
    );
}

#[test]
fn headers_pass_through_unknown_names_and_odd_values_verbatim() {
    let odd = headers(&[
        ("anthropic-ratelimit-unified-5h-utilization", " 0.9950 "),
        ("anthropic-ratelimit-unified-7d-utilization", "1e-3"),
        ("anthropic-ratelimit-unified-some-future-window", ""),
        ("anthropic-ratelimit-unified-status", "allowed_warning"),
    ]);
    let observed = attempt(
        Some(200),
        AttemptAccountObservation {
            ratelimit_headers: odd.clone(),
            limit: None,
        },
    );
    let body = attempt_body("e", "adm", &observed, None);
    assert_eq!(body["headers"], json!(odd));
}

#[test]
fn event_id_is_the_lineage_and_wal_sequence_joined_by_a_colon() {
    let dir = broca_test_support::scratch_root()
        .join(format!("broca-account-event-id-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut wal = broca_wal::FileWal::open(dir.join("session.wal")).unwrap();
    use broca_wal::Wal;
    let seq = wal.append(0, &any_record(), true).unwrap();
    let lineage = wal.lineage_id().expect("an appended log has a lineage");
    assert_eq!(
        event_id(Some(lineage), seq),
        Some(format!("{lineage}:{seq}"))
    );
    assert_eq!(event_id(None, seq), None);
    let _ = std::fs::remove_dir_all(&dir);
}

// ---- the reporter ---------------------------------------------------------------------------

async fn settle(routing: &FakeRouting, expected: usize) {
    for _ in 0..200 {
        if routing.observed().len() >= expected {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // Long enough for an unexpected extra report to arrive as well.
    tokio::time::sleep(Duration::from_millis(50)).await;
}

fn lineage_wal() -> broca_wal::FileWal {
    // scratch_root is shared by this process. A clock reading is not a unique allocation:
    // simultaneous fixtures can get the same timestamp and become writers of the same WAL.
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = broca_test_support::scratch_root().join(format!(
        "broca-account-lineage-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    broca_wal::FileWal::open(dir.join("session.wal")).unwrap()
}

#[test]
fn lineage_fixtures_have_independent_writers() {
    use broca_wal::Wal;
    // Open both before either appends. Reusing a path would let the first append grow the
    // second writer's file behind its back, even if fixtures usually finish one at a time.
    let mut first = lineage_wal();
    let mut second = lineage_wal();
    first.append(0, &any_record(), true).unwrap();
    second.append(0, &any_record(), true).unwrap();
    assert_ne!(first.lineage_id(), second.lineage_id());
}

fn lineage() -> WalLineageId {
    let mut wal = lineage_wal();
    use broca_wal::Wal;
    wal.append(0, &any_record(), true).unwrap();
    wal.lineage_id().unwrap()
}

#[tokio::test]
async fn run_finished_is_sent_once_for_every_terminal_reason() {
    for reason in [
        RunFinishReason::Completed,
        RunFinishReason::Interrupted,
        RunFinishReason::Cancelled,
        RunFinishReason::Error,
    ] {
        let routing = FakeRouting::refusing(SELECTION_DISABLED, json!({}));
        let client = Arc::new(client(&routing));
        let durable = Arc::new(AtomicU64::new(100));
        let reporter = RunReporter::start(
            Arc::clone(&client),
            "run-1".into(),
            "run-1".into(),
            None,
            durable,
        );
        let lineage = lineage();
        broca_core::AccountReporter::run_started(reporter.as_ref(), Some(lineage), 1);
        reporter.finish(Some(lineage), 9, reason);
        // A second finish for the same run is never sent: the report task has ended.
        reporter.finish(Some(lineage), 10, reason);
        settle(&routing, 2).await;
        let observed = routing.observed();
        let kinds: Vec<_> = observed.iter().map(|body| body["kind"].clone()).collect();
        assert_eq!(
            kinds,
            vec![json!("admission_started"), json!("run_finished")],
            "{reason:?}"
        );
        assert_eq!(observed[1]["event_id"], format!("{lineage}:9"));
        assert_eq!(
            observed[1]["terminal_reason"],
            serde_json::to_value(reason).unwrap()
        );
        assert_eq!(client.counters.get("observe_disabled"), 2);
    }
}

#[tokio::test]
async fn an_attempt_is_reported_only_once_its_record_is_durable_and_in_order() {
    let routing = FakeRouting::refusing(SELECTION_DISABLED, json!({}));
    let client = Arc::new(client(&routing));
    let durable = Arc::new(AtomicU64::new(2));
    let reporter = RunReporter::start(
        Arc::clone(&client),
        "run-1".into(),
        "run-1".into(),
        Some("vault-view-1".into()),
        Arc::clone(&durable),
    );
    let lineage = lineage();
    let first = attempt(
        Some(200),
        AttemptAccountObservation {
            ratelimit_headers: headers(&[("anthropic-ratelimit-unified-5h-utilization", "0.25")]),
            limit: None,
        },
    );
    let second = attempt(Some(200), AttemptAccountObservation::default());
    broca_core::AccountReporter::attempt_recorded(reporter.as_ref(), Some(lineage), 3, &first);
    broca_core::AccountReporter::attempt_recorded(reporter.as_ref(), Some(lineage), 5, &second);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        routing.observed().is_empty(),
        "record 3 is not durable yet, so nothing is reported"
    );
    durable.store(5, Ordering::Release);
    reporter.finish(Some(lineage), 6, RunFinishReason::Completed);
    settle(&routing, 3).await;
    let observed = routing.observed();
    assert_eq!(observed.len(), 3);
    assert_eq!(observed[0]["event_id"], format!("{lineage}:3"));
    assert_eq!(observed[0]["view_digest"], "vault-view-1");
    assert_eq!(observed[1]["event_id"], format!("{lineage}:5"));
    assert_eq!(observed[1].get("headers"), None);
    // run_finished carries the latest reading seen, even when the last attempt had none.
    assert_eq!(
        observed[2]["headers"],
        json!({ "anthropic-ratelimit-unified-5h-utilization": "0.25" })
    );
}

#[tokio::test]
async fn a_hung_routing_never_holds_the_reporting_run() {
    let routing = FakeRouting::hung();
    let client = Arc::new(client(&routing));
    let reporter = RunReporter::start(
        Arc::clone(&client),
        "run-1".into(),
        "run-1".into(),
        None,
        Arc::new(AtomicU64::new(100)),
    );
    let lineage = lineage();
    let started = std::time::Instant::now();
    broca_core::AccountReporter::run_started(reporter.as_ref(), Some(lineage), 1);
    broca_core::AccountReporter::attempt_recorded(
        reporter.as_ref(),
        Some(lineage),
        2,
        &attempt(Some(200), AttemptAccountObservation::default()),
    );
    reporter.finish(Some(lineage), 3, RunFinishReason::Completed);
    assert!(
        started.elapsed() < Duration::from_millis(50),
        "the hooks queue and return"
    );
}

#[tokio::test]
async fn a_sealed_run_reports_its_interrupted_terminal_once() {
    let routing = FakeRouting::refusing(SELECTION_DISABLED, json!({}));
    let client = Arc::new(client(&routing));
    let lineage = lineage();
    let selection = |decision: Option<AccountDecision>| broca_core::AuthSelection {
        credential_id: "oauth:anthropic".into(),
        account_ref: None,
        resolved_credential_id: None,
        vault_request: None,
        account_decision: decision,
    };
    let sealed = |auth_selection| broca_module::SealedRun {
        session: session(),
        run_id: broca_protocol::RunId("run-1".into()),
        auth_selection,
        lineage: Some(lineage),
        terminal_seq: 12,
    };
    report_sealed(
        Arc::clone(&client),
        &sealed(Some(selection(Some(AccountDecision {
            conversation_key: None,
            conversation_key_source: None,
            path: "disabled".into(),
            admission_id: Some("run-1".into()),
            decision_id: None,
        })))),
    );
    // No admission id (the break-glass path), or no selection at all: nothing is reported.
    report_sealed(
        Arc::clone(&client),
        &sealed(Some(selection(Some(AccountDecision {
            conversation_key: None,
            conversation_key_source: None,
            path: "legacy_static".into(),
            admission_id: None,
            decision_id: None,
        })))),
    );
    report_sealed(Arc::clone(&client), &sealed(None));
    settle(&routing, 1).await;
    let observed = routing.observed();
    assert_eq!(observed.len(), 1);
    assert_eq!(
        observed[0],
        run_finished_body(
            &format!("{lineage}:12"),
            "run-1",
            "run-1",
            RunFinishReason::Interrupted,
            None
        )
    );
}

#[tokio::test]
async fn a_scoped_not_found_is_reported_and_a_still_listed_credential_alarms() {
    let routing = FakeRouting::refusing(SELECTION_DISABLED, json!({}));
    let client = Arc::new(client(&routing));
    report_scoped_not_found(
        Arc::clone(&client),
        "run-1".into(),
        "oauth:anthropic:acct-b".into(),
        "vault-view-1".into(),
        async { Some(two_accounts()) },
    );
    settle(&routing, 1).await;
    let observed = routing.observed();
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0]["kind"], "scoped_not_found");
    assert_eq!(observed[0]["credential_id"], "oauth:anthropic:acct-b");
    assert_eq!(observed[0]["view_digest"], "vault-view-1");
    assert_eq!(client.counters.get("scoped_not_found"), 1);
    assert_eq!(client.counters.get("scoped_not_found_still_listed"), 1);

    // A re-list that dropped the credential raises no alarm.
    report_scoped_not_found(
        Arc::clone(&client),
        "run-2".into(),
        "oauth:anthropic:acct-b".into(),
        "vault-view-1".into(),
        async { Some(listing(vec![row("oauth:anthropic", Some("acct-a"))])) },
    );
    settle(&routing, 2).await;
    assert_eq!(client.counters.get("scoped_not_found_still_listed"), 1);
}

/// Any record, to give a scratch log its lineage id.
fn any_record() -> broca_wal::WalRecord {
    broca_wal::WalRecord::RunFinished {
        ts_ms: None,
        reason: RunFinishReason::Completed,
        usage: Default::default(),
        error: None,
        retries_used: 0,
    }
}
