//! Tests for account selection through routing. The observe bodies are compared, as parsed
//! JSON, with the bodies routing's own tests send for broca (prefrontal
//! `crates/prefrontal-routing-module/src/account_selection.rs`, the `broca_*` and
//! `observations_and_releases_*` tests at 92b42c1de), so a drift in either side's field names
//! shows here.

use super::*;
use broca_protocol::{AttemptAccountLimit, AttemptAccountObservation, AttemptOutcome};
use broca_subc::vault::ScopedCredential;
use std::sync::atomic::AtomicUsize;

#[test]
fn routed_accounts_discovery_lists_only_self_routed_sign_in_methods() {
    for describe in [
        broca_wire::broca_role_describe("test"),
        broca_wire::broca_role_describe_with_writers("test", true, true, true),
    ] {
        let value = serde_json::to_value(&describe).unwrap();
        let checked = cortexkit_role_llm_runner::describe::check_describe(&value).unwrap();
        assert!(checked.declares(cortexkit_role_llm_runner::capabilities::ROUTED_ACCOUNTS));
        assert_eq!(
            value["routed_accounts"],
            json!(["oauth", "chatgpt", "apikey"])
        );
        for method in [
            AuthMethod::ApiKey,
            AuthMethod::Oauth,
            AuthMethod::Antigravity,
            AuthMethod::Chatgpt,
        ] {
            assert_eq!(
                selects_accounts(method),
                checked
                    .routed_accounts
                    .as_ref()
                    .unwrap()
                    .iter()
                    .any(|listed| listed == method.as_token()),
                "{method:?}"
            );
        }
    }
}

/// Routing's test clock, used in its pinned bodies.
const NOW: i64 = 1_790_683_200_000;

/// A routing stand-in: answers each request from a script and keeps every request it got.
#[derive(Default)]
struct FakeRouting {
    requests: Mutex<Vec<Value>>,
    request_bytes: Mutex<Vec<Vec<u8>>>,
    reply: Mutex<Option<RoutingReply>>,
    /// Never answer, as a hung routing module would.
    hang: bool,
    answered: AtomicUsize,
    tiers: bool,
    tier_probes: AtomicUsize,
    backoff: bool,
    credential_refused: bool,
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
    async fn observe_credential_refused_supported(&self) -> Result<bool, String> {
        Ok(self.credential_refused)
    }
    async fn observe_backoff_supported(&self) -> Result<bool, String> {
        Ok(self.backoff)
    }
    async fn service_tier_supported(&self) -> Result<bool, String> {
        self.tier_probes.fetch_add(1, Ordering::SeqCst);
        Ok(self.tiers)
    }
    async fn request(&self, body: Vec<u8>) -> RoutingReply {
        self.request_bytes.lock().unwrap().push(body.clone());
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

#[tokio::test]
async fn tierless_account_request_does_not_probe_router_discovery() {
    let routing = FakeRouting::choosing("acct-a", "credential-a");
    let client = client(&routing);
    let params = json!({"provider":"anthropic","model":"fixture","admission_id":"run"});
    let outcome = client
        .account_for(
            params.clone(),
            tokio::time::Instant::now() + ADMISSION_BUDGET,
        )
        .await;
    assert!(matches!(outcome, AccountForOutcome::Chosen { .. }));
    assert_eq!(
        routing.tier_probes.load(Ordering::SeqCst),
        0,
        "a tierless admission must not issue role.describe"
    );
    assert_eq!(
        *routing.requests.lock().unwrap(),
        vec![json!({"method":ACCOUNT_FOR_OP,"params":params})]
    );
}

#[tokio::test]
async fn head_selection_account_tier_is_capability_gated_and_chosen() {
    for supported in [false, true] {
        let routing = Arc::new(FakeRouting {
            tiers: supported,
            reply: Mutex::new(Some(Ok(Ok(serde_json::to_vec(&json!({"result":{
                "account":{"account_id":"acct-a","credential_id":"opaque-a"},
                "service_tier_chosen":"fast"}}))
            .unwrap())))),
            ..FakeRouting::default()
        });
        let result = client(&routing)
            .account_for(
                json!({"service_tier":"ultrafast"}),
                tokio::time::Instant::now() + ADMISSION_BUDGET,
            )
            .await;
        assert!(
            matches!(result, AccountForOutcome::Chosen { service_tier_chosen, .. }
            if service_tier_chosen.as_deref() == supported.then_some("fast"))
        );
        assert_eq!(
            routing.requests.lock().unwrap()[0]["params"]
                .get("service_tier")
                .and_then(Value::as_str),
            supported.then_some("ultrafast")
        );
        assert_eq!(routing.tier_probes.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn head_selection_account_decision_tier_cannot_step_down() {
    for chosen in [
        Some("standard"),
        Some("fast"),
        Some("ultrafast"),
        Some("future"),
        None,
    ] {
        let mut result = json!({"account":{"account_id":"acct-a","credential_id":"opaque-a"}});
        if let Some(chosen) = chosen {
            result["service_tier_chosen"] = chosen.into();
        }
        let routing = Arc::new(FakeRouting {
            tiers: true,
            reply: Mutex::new(Some(Ok(Ok(
                serde_json::to_vec(&json!({"result":result})).unwrap()
            )))),
            ..FakeRouting::default()
        });
        let answer = client(&routing)
            .account_for(
                json!({"decision_id":"fixed-fast","service_tier":"fast"}),
                tokio::time::Instant::now() + ADMISSION_BUDGET,
            )
            .await;
        assert_eq!(
            matches!(answer, AccountForOutcome::Chosen { .. }),
            chosen == Some("fast"),
            "chosen={chosen:?}: {answer:?}"
        );
    }
}

/// A listed credential with the metadata the vault sends: the provider it serves and how it
/// authenticates. The id is an opaque record name; nothing reads it for either.
fn row(id: &str, account_id: Option<&str>, provider: &str, method: &str) -> ScopedCredential {
    ScopedCredential {
        id: id.into(),
        categories: vec!["llm-provider".into()],
        credential_type: method.into(),
        serves: vec![provider.into()],
        refresh_adapter: Some(provider.into()),
        state: "active".into(),
        record_version: 1,
        operations: vec!["read".into()],
        account_id: account_id.map(str::to_string),
        email: None,
        org_name: None,
        provider_ids: vec![provider.into()],
        auth_method: Some(method.into()),
    }
}

/// An anthropic subscription login, as the vault lists one.
fn anthropic(id: &str, account_id: &str) -> ScopedCredential {
    row(id, Some(account_id), "anthropic", "oauth")
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
        anthropic("oauth:anthropic", "acct-a"),
        anthropic("oauth:anthropic:acct-b", "acct-b"),
        row("apikey:anthropic", None, "anthropic", "apikey"),
        row("chatgpt:openai", Some("acct-c"), "openai", "chatgpt"),
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
) -> Result<AccountChoice, ChoiceRefusal> {
    let static_credential = CredentialId::parse(static_credential).unwrap();
    choose_account(
        client,
        ChoiceInputs {
            account_route: None,
            pool: None,
            credential_id: None,
            service_tier: None,
            decision_id: None,
            hold_cap: Default::default(),
            conversation_key: None,
            inherited_conversation_key: None,
            static_credential: &static_credential,
            caller_named_account,
            model: "claude-opus-5-5",
            session: &session(),
            send_id: None,
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

#[test]
fn account_for_hold_cap_params_preserve_legacy_members_and_bound_the_previous_account() {
    use broca_core::SameAccountWait::{Max, Minutes};
    let previous = |frozen: Value| -> broca_wal::RunConfig {
        serde_json::from_value(json!({
            "model":{"provider_module_id":"anthropic","model_id":"previous-model"},
            "tools":[], "tool_choice":{"type":"auto"}, "generation":{},
            "response_format":{"type":"text"}, "context_epoch":0, "renderer_version":1,
            "auth_selection":{"credential_id":"oauth:anthropic:previous","account_ref":"previous-account"},
            "frozen_render_config": frozen
        })).unwrap()
    };
    let five = previous(json!({"cache":{"class":"markers","ttl":"5m"}}));
    let hour = previous(json!({"cache":{"class":"markers","ttl":"1h"}}));
    let unknown = previous(Value::Null);
    let legacy = json!({
        "provider":"anthropic", "model":"new-model", "affinity_model":"new-model",
        "conversation_key":"conversation", "caller":{"runner":"broca"},
        "admission_id":"run-1", "mode":"admit", "initial_input_bytes":42,
        "initial_input_estimated":true,
        "candidates":{"view_digest":"vault-view-1","accounts":[
            {"account_id":"acct-a","credential_id":"oauth:anthropic"},
            {"account_id":"acct-b","credential_id":"oauth:anthropic:acct-b"}
        ]}
    });
    let candidates = Candidates::from_listing(&two_accounts(), AuthMethod::Oauth, "anthropic");
    for (name, setting, previous, accepted, seconds, unknown_evidence) in [
        (
            "default 15 bounded by cache",
            Minutes(15),
            Some(&five),
            Some(Some(900_000)),
            199,
            false,
        ),
        (
            "minute cap",
            Minutes(1),
            Some(&five),
            Some(Some(900_000)),
            60,
            false,
        ),
        (
            "max one hour",
            Max,
            Some(&hour),
            Some(Some(900_000)),
            3499,
            false,
        ),
        (
            "unknown TTL five minutes",
            Max,
            Some(&unknown),
            Some(Some(900_000)),
            199,
            false,
        ),
        ("disabled", Minutes(0), Some(&hour), None, 0, false),
        ("no previous run", Minutes(15), None, None, 0, false),
        (
            "no accepted response on previous account",
            Minutes(15),
            Some(&five),
            Some(None),
            0,
            false,
        ),
        ("expired", Max, Some(&five), Some(Some(0)), 0, false),
        (
            "subsecond floors to zero",
            Max,
            Some(&five),
            Some(Some(700_999)),
            0,
            false,
        ),
        ("unloaded", Minutes(15), Some(&five), None, 0, true),
    ] {
        let cap = account_hold_cap(setting, previous, 1_000_500, |account| {
            assert_eq!(account, "previous-account");
            accepted
        });
        assert_eq!(
            cap,
            AccountHoldCap {
                max_s: seconds,
                evidence_unknown: unknown_evidence
            },
            "{name}"
        );
        let mut params = account_for_params(&AccountForRequest {
            account_route: None,
            pool: None,
            credential_id: None,
            service_tier: None,
            decision_id: None,
            provider: "anthropic",
            model: "new-model",
            conversation_key: "conversation",
            admission_id: "run-1",
            initial_input_bytes: 42,
            candidates: &candidates,
            hold_cap: cap,
        });
        let mut expected = legacy.clone();
        expected["hold_wait_max_s"] = json!(seconds);
        assert_eq!(params, expected, "{name}");
        assert_eq!(
            params.as_object_mut().unwrap().remove("hold_wait_max_s"),
            Some(json!(seconds))
        );
        assert_eq!(
            params, legacy,
            "the new hold member must not alter existing params: {name}"
        );
    }
}

#[tokio::test]
async fn account_for_counts_only_sent_unknown_hold_caps_not_real_zero_or_static_choices() {
    let before = account_limit_hold_cap_unknown();
    let credential = CredentialId::parse("oauth:anthropic").unwrap();
    for (unknown, pinned, expected) in [(false, false, 0), (true, true, 0), (true, false, 1)] {
        let routing = FakeRouting::choosing("acct-b", "oauth:anthropic:acct-b");
        let client = client(&routing);
        choose_account(
            &client,
            ChoiceInputs {
                account_route: None,
                pool: None,
                credential_id: None,
                service_tier: None,
                decision_id: None,
                hold_cap: AccountHoldCap {
                    max_s: 0,
                    evidence_unknown: unknown,
                },
                conversation_key: None,
                inherited_conversation_key: None,
                static_credential: &credential,
                caller_named_account: pinned,
                model: "claude-opus-5-5",
                session: &session(),
                send_id: None,
                run_id: "run-1",
                initial_input_bytes: 42,
            },
            async { Some(two_accounts()) },
            tokio::time::Instant::now() + ADMISSION_BUDGET,
        )
        .await
        .unwrap();
        assert_eq!(account_limit_hold_cap_unknown(), before + expected);
        if !pinned {
            assert_eq!(
                routing.requests.lock().unwrap()[0]["params"]["hold_wait_max_s"],
                0
            );
        }
    }
}

#[tokio::test]
async fn account_selection_disabled_keeps_the_static_credential() {
    let routing = FakeRouting::refusing(
        SELECTION_DISABLED,
        json!({ "code": SELECTION_DISABLED, "decision_id": "acd-disabled" }),
    );
    let client = client(&routing);
    let (choice, events) = broca_test_support::capture_logs(choose(
        &client,
        "oauth:anthropic",
        false,
        Some(two_accounts()),
    ))
    .await;
    let AccountChoice::Static(decision) =
        choice.expect("the operator off-switch keeps the static account fallback")
    else {
        panic!("selection off must retain the static credential")
    };
    assert_eq!(decision.path, broca_wal::ACCOUNT_DECISION_DISABLED);
    assert_eq!(decision.admission_id, None);
    assert_eq!(decision.decision_id.as_deref(), Some("acd-disabled"));
    assert_eq!(routing.methods(), vec![ACCOUNT_FOR_OP]);
    assert_eq!(client.counters.get("disabled"), 1);
    assert_eq!(client.counters.get("refused"), 0);
    let event = events
        .iter()
        .find(|event| {
            event.fields.get("routing_code").map(String::as_str) == Some(SELECTION_DISABLED)
        })
        .expect("the static fallback log preserves routing's code");
    assert_eq!(event.target, "routing");
    assert_eq!(
        event.fields.get("decision_id").map(String::as_str),
        Some("acd-disabled")
    );
}

/// Routing being down refuses the send, typed and transient, instead of admitting it on an
/// account routing never chose. Asked once, never retried here: the caller retries the send.
#[tokio::test]
async fn routing_unreachable_refuses_the_send_routing_unavailable() {
    let routing = FakeRouting::answering(Err("route.open refused: target_unavailable".into()));
    let client = client(&routing);
    let refusal = choose(&client, "oauth:anthropic", false, Some(two_accounts()))
        .await
        .expect_err("broca never picks an account when routing cannot answer");
    assert_eq!(
        refusal,
        ChoiceRefusal::Unavailable(RoutingUnavailable::unreachable())
    );
    let error = broca_core::RunError::from(refusal);
    assert_eq!(error.admission_error_class(), ErrorClass::Transient);
    assert_eq!(routing.methods(), vec![ACCOUNT_FOR_OP]);
    assert_eq!(client.counters.get("unreachable"), 1);
    assert_eq!(client.counters.snapshot()["unreachable"], 1);
}

pub(crate) async fn unavailable_admission_error() -> broca_core::RunError {
    let routing = FakeRouting::answering(Err("route.open refused: target_unavailable".into()));
    let refusal = choose(
        &client(&routing),
        "oauth:anthropic",
        false,
        Some(two_accounts()),
    )
    .await
    .expect_err("unreachable routing refuses admission");
    assert_eq!(routing.methods(), vec![ACCOUNT_FOR_OP]);
    refusal.into()
}

#[tokio::test]
async fn a_malformed_routing_reply_is_refused_as_unreachable() {
    for reply in [
        Ok(Ok(b"not json".to_vec())),
        Ok(Ok(br#"{"result":{"account":{}}}"#.to_vec())),
        Ok(Err(b"not an error body".to_vec())),
    ] {
        let routing = FakeRouting::answering(reply);
        let client = client(&routing);
        let refusal = choose(&client, "oauth:anthropic", false, Some(two_accounts()))
            .await
            .unwrap_err();
        assert_eq!(
            refusal,
            ChoiceRefusal::Unavailable(RoutingUnavailable::unreachable())
        );
    }
}

// Paused clock with tokio's Instant: the budget is measured in virtual time, so the bound
// checks the code's own waiting and not how promptly the host scheduled this test.
#[tokio::test(start_paused = true)]
async fn a_hung_routing_costs_at_most_the_admission_budget() {
    let routing = FakeRouting::hung();
    let client = client(&routing);
    let static_credential = CredentialId::parse("oauth:anthropic").unwrap();
    let budget = Duration::from_millis(250);
    let started = tokio::time::Instant::now();
    let (choice, events) = broca_test_support::capture_logs(choose_account(
        &client,
        ChoiceInputs {
            account_route: None,
            pool: None,
            credential_id: None,
            service_tier: None,
            decision_id: None,
            hold_cap: Default::default(),
            conversation_key: None,
            inherited_conversation_key: None,
            static_credential: &static_credential,
            caller_named_account: false,
            model: "claude-opus-5-5",
            session: &session(),
            send_id: None,
            run_id: "run-1",
            initial_input_bytes: 0,
        },
        async { Some(two_accounts()) },
        tokio::time::Instant::now() + budget,
    ))
    .await;
    let refusal = choice.unwrap_err();
    let elapsed = started.elapsed();
    assert_eq!(
        refusal,
        ChoiceRefusal::Unavailable(RoutingUnavailable::no_decision_within_budget(
            ADMISSION_BUDGET.as_millis() as u64
        ))
    );
    assert_eq!(client.counters.get("no_decision_within_budget"), 1);
    assert_eq!(
        routing.methods(),
        vec![ACCOUNT_FOR_OP],
        "the request was sent"
    );
    assert!(
        elapsed >= budget && elapsed < budget + Duration::from_millis(500),
        "waited {elapsed:?} for a {budget:?} budget"
    );
    assert!(events.iter().any(|event| {
        event.target == "routing"
            && event.level == tracing::Level::WARN
            && event.message().contains("is refused routing_unavailable")
    }));
    // The production budget leaves the caller's 10 s send deadline room for a 5 s vault read.
    assert!(ADMISSION_BUDGET <= Duration::from_secs(2));
}

// Paused clock with tokio's Instant: the budget is measured in virtual time, so the bound
// checks the code's own waiting and not how promptly the host scheduled this test.
#[tokio::test(start_paused = true)]
async fn a_slow_listing_shares_the_admission_budget() {
    let routing = FakeRouting::choosing("acct-b", "oauth:anthropic:acct-b");
    let client = client(&routing);
    let static_credential = CredentialId::parse("oauth:anthropic").unwrap();
    let started = tokio::time::Instant::now();
    let choice = choose_account(
        &client,
        ChoiceInputs {
            account_route: None,
            pool: None,
            credential_id: None,
            service_tier: None,
            decision_id: None,
            hold_cap: Default::default(),
            conversation_key: None,
            inherited_conversation_key: None,
            static_credential: &static_credential,
            caller_named_account: false,
            model: "claude-opus-5-5",
            session: &session(),
            send_id: None,
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
        None,
        "a static pin reserved no routing admission"
    );
    assert!(routing.methods().is_empty());
}

#[tokio::test]
async fn one_candidate_account_asks_nothing() {
    let routing = FakeRouting::choosing("acct-b", "oauth:anthropic:acct-b");
    let client = client(&routing);
    // Two records of ONE account are one candidate.
    let one = listing(vec![
        anthropic("oauth:anthropic", "acct-a"),
        anthropic("oauth:anthropic:again", "acct-a"),
    ]);
    let choice = choose(&client, "oauth:anthropic", false, Some(one))
        .await
        .unwrap();
    assert_eq!(path_of(&choice), "single_candidate");
    assert!(routing.methods().is_empty());
}

#[tokio::test]
async fn an_api_key_run_without_hints_is_not_an_account_choice() {
    let routing = FakeRouting::choosing("acct-b", "oauth:anthropic:acct-b");
    let client = client(&routing);
    let choice = choose(&client, "apikey:anthropic", false, Some(two_accounts()))
        .await
        .unwrap();
    assert_eq!(choice, AccountChoice::NotApplicable);
    assert!(routing.methods().is_empty());
}

fn api_keys() -> Arc<ScopedList> {
    let mut a = row("key-a", None, "anthropic", "apikey");
    a.refresh_adapter = None;
    let mut b = a.clone();
    b.id = "key-b".into();
    listing(vec![a, b])
}

async fn choose_api_key(
    client: &RoutingClient,
    pool: Option<&str>,
    credential_id: Option<&str>,
    pinned: bool,
    rows: Option<Arc<ScopedList>>,
) -> Result<AccountChoice, ChoiceRefusal> {
    choose_api_key_with_decision(client, pool, credential_id, pinned, rows, None).await
}

async fn choose_api_key_with_decision(
    client: &RoutingClient,
    pool: Option<&str>,
    credential_id: Option<&str>,
    pinned: bool,
    rows: Option<Arc<ScopedList>>,
    decision_id: Option<&str>,
) -> Result<AccountChoice, ChoiceRefusal> {
    let key = CredentialId::parse("apikey:anthropic:configured").unwrap();
    choose_account(
        client,
        ChoiceInputs {
            account_route: None,
            static_credential: &key,
            caller_named_account: pinned,
            pool,
            credential_id,
            decision_id,
            service_tier: None,
            conversation_key: None,
            inherited_conversation_key: None,
            hold_cap: Default::default(),
            model: "claude-opus-5-5",
            session: &session(),
            send_id: None,
            run_id: "run-1",
            initial_input_bytes: 42,
        },
        async { rows },
        tokio::time::Instant::now() + ADMISSION_BUDGET,
    )
    .await
}

fn api_key_router(method: &str, credential_id: &str) -> Arc<FakeRouting> {
    FakeRouting::answering(Ok(Ok(serde_json::to_vec(&json!({"result":{
        "account":{"account_id":credential_id,"credential_id":credential_id,"method":method}
    }}))
    .unwrap())))
}

#[tokio::test]
async fn apikey_pool_routes_the_answered_key_not_the_hint() {
    let routing = api_key_router("apikey", "key-b");
    let result = choose_api_key(
        &client(&routing),
        Some("keys"),
        Some("key-a"),
        false,
        Some(api_keys()),
    )
    .await;
    if let Some(dir) = std::env::var_os("BROCA_APIKEY_SAMPLE_DIR") {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("role.describe.json"),
            serde_json::to_vec(&broca_wire::broca_role_describe("sample")).unwrap(),
        )
        .unwrap();
        let requests = routing.requests.lock().unwrap();
        let params = requests
            .iter()
            .find(|request| request["method"] == ACCOUNT_FOR_OP)
            .map(|request| &request["params"]);
        std::fs::write(
            dir.join("route.account_for.params.json"),
            params
                .map(|params| serde_json::to_vec(params).unwrap())
                .unwrap_or_default(),
        )
        .unwrap();
    }
    let choice = result.unwrap();
    assert!(
        matches!(choice, AccountChoice::Routed { credential_id, method: AuthMethod::ApiKey, .. } if credential_id == "key-b")
    );
    let requests = routing.requests.lock().unwrap();
    assert_eq!(requests[0]["params"]["pool"], "keys");
    assert_eq!(
        requests[0]["params"]["requested_account"],
        json!({"credential_id":"key-a"})
    );
    assert_eq!(requests[0]["params"]["mode"], "admit");
    assert_eq!(
        requests[0]["params"]["candidates"]["accounts"],
        json!([
            {"account_id":"key-a","credential_id":"key-a"},
            {"account_id":"key-b","credential_id":"key-b"}
        ])
    );
}

#[tokio::test]
async fn apikey_vault_account_identity_is_offered_and_checked_with_the_key() {
    let mut rows = (*api_keys()).clone();
    rows.credentials[1].account_id = Some("key-owner".into());
    let rows = Some(Arc::new(rows));
    let wrong_account = api_key_router("apikey", "key-b");
    assert_eq!(
        choose_api_key(
            &client(&wrong_account),
            Some("keys"),
            None,
            false,
            rows.clone()
        )
        .await,
        Err(ChoiceRefusal::Unavailable(
            RoutingUnavailable::invalid_pick()
        ))
    );
    let routing = FakeRouting::answering(Ok(Ok(serde_json::to_vec(&json!({"result":{
        "account":{"method":"apikey","account_id":"key-owner","credential_id":"key-b"}
    }}))
    .unwrap())));
    assert!(
        matches!(choose_api_key(&client(&routing), Some("keys"), None, false, rows).await.unwrap(),
        AccountChoice::Routed { account_id, credential_id, .. } if account_id == "key-owner" && credential_id == "key-b")
    );
}

#[tokio::test]
async fn apikey_decision_without_hints_still_requires_routing() {
    let routing = api_key_router("apikey", "key-a");
    assert!(matches!(
        choose_api_key_with_decision(
            &client(&routing),
            None,
            None,
            false,
            Some(api_keys()),
            Some("route-decision")
        )
        .await
        .unwrap(),
        AccountChoice::Routed { .. }
    ));
    assert_eq!(
        routing.requests.lock().unwrap()[0]["params"]["decision_id"],
        "route-decision"
    );
}

#[tokio::test]
async fn apikey_routing_refuses_wrong_method_or_unloadable_pick() {
    for (method, id) in [("oauth", "key-b"), ("apikey", "unknown"), ("", "key-b")] {
        let routing = api_key_router(method, id);
        assert_eq!(
            choose_api_key(
                &client(&routing),
                Some("keys"),
                None,
                false,
                Some(api_keys())
            )
            .await,
            Err(ChoiceRefusal::Unavailable(
                RoutingUnavailable::invalid_pick()
            ))
        );
    }
    for field in ["state", "operations", "auth_method", "provider_ids"] {
        let mut rows = (*api_keys()).clone();
        let picked = &mut rows.credentials[1];
        match field {
            "state" => picked.state = "needs_reauth".into(),
            "operations" => picked.operations.clear(),
            "auth_method" => picked.auth_method = Some("oauth".into()),
            "provider_ids" => picked.provider_ids = vec!["openai".into()],
            _ => unreachable!(),
        }
        let routing = api_key_router("apikey", "key-b");
        assert_eq!(
            choose_api_key(
                &client(&routing),
                Some("keys"),
                None,
                false,
                Some(Arc::new(rows))
            )
            .await,
            Err(ChoiceRefusal::Unavailable(
                RoutingUnavailable::invalid_pick()
            )),
            "{field}"
        );
    }
}

#[tokio::test]
async fn apikey_legacy_unhinted_and_pinned_keep_the_configured_vault_record() {
    use crate::auth_routing::{resolve_credential_id, MethodConfig};
    use crate::vault_config::{VaultAddressing, VaultCredentialIds};
    let root = broca_test_support::scratch_root().join("apikey-static-mapping");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("auth-methods.json");
    std::fs::write(&path, br#"{"defaults":{"anthropic":"apikey"}}"#).unwrap();
    let methods = MethodConfig::load(&path).unwrap();
    let addressing = VaultAddressing::new(VaultCredentialIds::from_pairs([
        ("apikey:anthropic", "operator-default"),
        ("apikey:anthropic:work", "operator-work"),
    ]));
    for (account, expected) in [(None, "operator-default"), (Some("work"), "operator-work")] {
        let key = resolve_credential_id("anthropic", None, account, &methods, AuthMethod::Oauth);
        let routing = FakeRouting::refusing(SELECTION_DISABLED, json!({}));
        let choice = choose_account(
            &client(&routing),
            ChoiceInputs {
                account_route: None,
                static_credential: &key,
                caller_named_account: account.is_some(),
                pool: None,
                credential_id: None,
                decision_id: None,
                service_tier: None,
                conversation_key: None,
                inherited_conversation_key: None,
                hold_cap: Default::default(),
                model: "claude-opus-5-5",
                session: &session(),
                send_id: None,
                run_id: "run-1",
                initial_input_bytes: 42,
            },
            async { Some(api_keys()) },
            tokio::time::Instant::now() + ADMISSION_BUDGET,
        )
        .await
        .unwrap();
        assert!(!matches!(choice, AccountChoice::Routed { .. }));
        assert_eq!(addressing.vault_id_for(&key), expected);
        assert!(routing.methods().is_empty());
    }
}

#[tokio::test]
async fn apikey_hint_requires_listing_but_not_multiple_keys_or_provider_metadata() {
    let routing = api_key_router("apikey", "key-a");
    assert!(matches!(
        choose_api_key(&client(&routing), Some("keys"), None, false, None).await,
        Err(ChoiceRefusal::Unavailable(_))
    ));
    assert!(routing.methods().is_empty());
    let mut rows = (*api_keys()).clone();
    rows.credentials.truncate(1);
    rows.credentials[0].provider_ids.clear();
    let choice = choose_api_key(
        &client(&routing),
        None,
        Some("key-a"),
        false,
        Some(Arc::new(rows)),
    )
    .await
    .unwrap();
    assert!(
        matches!(choice, AccountChoice::Routed { credential_id, .. } if credential_id == "key-a")
    );
    let disabled = FakeRouting::refusing(SELECTION_DISABLED, json!({}));
    assert!(
        matches!(choose_api_key(&client(&disabled), Some("keys"), None, false, Some(api_keys())).await,
        Err(ChoiceRefusal::Refused(refusal)) if refusal.routing_code == SELECTION_DISABLED)
    );
}

#[tokio::test]
async fn apikey_pin_with_pool_or_hint_is_refused_before_routing() {
    for (pool, hint) in [(Some("keys"), None), (None, Some("key-a"))] {
        let routing = api_key_router("apikey", "key-b");
        let result = choose_api_key(&client(&routing), pool, hint, true, Some(api_keys())).await;
        assert!(
            matches!(result, Err(ChoiceRefusal::Refused(refusal)) if refusal.routing_detail.as_deref() == Some(&json!({"field":"auth.account"})))
        );
        assert!(routing.methods().is_empty());
    }
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
            service_tier_chosen: None,
            account_id: "acct-b".into(),
            credential_id: "oauth:anthropic:acct-b".into(),
            method: AuthMethod::Oauth,
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
            "hold_wait_max_s": 0,
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
            account_route: None,
            pool: None,
            credential_id: None,
            service_tier: None,
            decision_id: None,
            hold_cap: Default::default(),
            conversation_key: Some("consult:42:subject:phase"),
            inherited_conversation_key: None,
            static_credential: &credential,
            caller_named_account: false,
            model: "claude-opus-5-5",
            session: &session(),
            send_id: None,
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

/// A pick broca did not offer (here another provider's credential) is a routing defect: the
/// send is refused `routing_unavailable` with reason `invalid_pick`, still transient, and one
/// ERROR names the candidate count and the picked id.
#[tokio::test]
async fn a_routing_pick_broca_did_not_offer_is_refused_and_logged_as_an_error() {
    let routing = FakeRouting::choosing("acct-c", "chatgpt:openai");
    let client = client(&routing);
    let (result, events) = broca_test_support::capture_logs(choose(
        &client,
        "oauth:anthropic",
        false,
        Some(two_accounts()),
    ))
    .await;
    let refusal = result.unwrap_err();
    assert_eq!(
        refusal,
        ChoiceRefusal::Unavailable(RoutingUnavailable::invalid_pick())
    );
    assert_eq!(
        broca_core::RunError::from(refusal).admission_error_class(),
        ErrorClass::Transient
    );
    assert_eq!(client.counters.get("invalid_pick"), 1);
    let errors: Vec<_> = events
        .iter()
        .filter(|event| event.level == tracing::Level::ERROR)
        .collect();
    assert_eq!(errors.len(), 1, "one ERROR per occurrence");
    let message = errors[0].message();
    assert!(message.contains("chatgpt:openai"), "{message}");
    assert!(message.contains("not one of the 2 candidates"), "{message}");
}

/// An offered credential under another account than the one routing names is not a pick broca
/// offered either.
#[tokio::test]
async fn a_pick_pairing_an_offered_credential_with_another_account_is_invalid() {
    let routing = FakeRouting::choosing("acct-a", "oauth:anthropic:acct-b");
    let client = client(&routing);
    let refusal = choose(&client, "oauth:anthropic", false, Some(two_accounts()))
        .await
        .unwrap_err();
    assert_eq!(
        refusal,
        ChoiceRefusal::Unavailable(RoutingUnavailable::invalid_pick())
    );
}

/// Provider and method come from the vault's metadata, never from the credential id. The ids
/// here would parse to other providers and methods; the metadata says anthropic/oauth, so they
/// are anthropic's candidates and routing's pick of one is used verbatim. A row whose id reads
/// as an anthropic oauth login but whose metadata says otherwise is not offered.
#[tokio::test]
async fn candidates_and_the_routed_pick_come_from_vault_metadata_not_the_id() {
    let routing = FakeRouting::choosing("acct-y", "apikey:openai:work");
    let client = client(&routing);
    let rows = listing(vec![
        anthropic("oauth:openai:personal", "acct-x"),
        anthropic("apikey:openai:work", "acct-y"),
        row(
            "oauth:anthropic:looks-right",
            Some("acct-z"),
            "openai",
            "oauth",
        ),
        row(
            "oauth:anthropic:apikey",
            Some("acct-w"),
            "anthropic",
            "apikey",
        ),
    ]);
    let choice = choose(&client, "oauth:anthropic", false, Some(rows))
        .await
        .unwrap();
    let request = routing.requests.lock().unwrap()[0].clone();
    assert_eq!(
        request["params"]["candidates"]["accounts"],
        json!([
            { "account_id": "acct-y", "credential_id": "apikey:openai:work" },
            { "account_id": "acct-x", "credential_id": "oauth:openai:personal" },
        ])
    );
    match choice {
        AccountChoice::Routed {
            credential_id,
            method,
            ..
        } => {
            assert_eq!(
                credential_id, "apikey:openai:work",
                "the vault id, verbatim"
            );
            assert_eq!(method, AuthMethod::Oauth, "the method from metadata");
        }
        other => panic!("expected a routed choice, got {other:?}"),
    }
}

#[test]
fn candidates_accept_new_provider_ids_without_parsing_credential_names() {
    let mut credential = row(
        "oauth:future-provider:work",
        Some("acct-future"),
        "future-provider",
        "oauth",
    );
    credential.provider_ids.push("another-new-provider".into());
    let rows = listing(vec![credential]);
    let candidates = Candidates::from_listing(&rows, AuthMethod::Oauth, "future-provider");
    assert!(candidates.offers("acct-future", "oauth:future-provider:work"));
    assert!(
        Candidates::from_listing(&rows, AuthMethod::Oauth, "anthropic")
            .accounts
            .is_empty()
    );
}

/// A listed credential missing `provider_ids` or `auth_method` is never offered: it is not
/// guessed from its id. Each is warned about once (naming only the id and the missing field)
/// for as long as it stays unmapped, not once per admission, and counted; one that gains its
/// metadata leaves the count.
#[tokio::test]
async fn an_unmapped_credential_is_never_offered_warned_once_and_counted() {
    let mut no_providers = anthropic("oauth:anthropic:acct-c", "acct-c");
    no_providers.provider_ids.clear();
    let mut no_method = anthropic("oauth:anthropic:acct-d", "acct-d");
    no_method.auth_method = None;
    let rows = listing(vec![
        anthropic("oauth:anthropic", "acct-a"),
        anthropic("oauth:anthropic:acct-b", "acct-b"),
        no_providers.clone(),
        no_method,
    ]);
    let cache = ListingCache::default();
    let (_, events) = broca_test_support::capture_logs(async {
        for _ in 0..3 {
            cache.store((*rows).clone());
        }
    })
    .await;
    assert_eq!(cache.unmapped().count(), 2);
    let warnings: Vec<String> = events
        .iter()
        .filter(|event| event.level == tracing::Level::WARN)
        .map(|event| event.message().to_owned())
        .collect();
    assert_eq!(warnings.len(), 2, "{warnings:?}");
    assert!(warnings
        .iter()
        .any(|w| w.contains("oauth:anthropic:acct-c") && w.contains("no provider_ids")));
    assert!(warnings
        .iter()
        .any(|w| w.contains("oauth:anthropic:acct-d") && w.contains("no auth_method")));

    let routing = FakeRouting::choosing("acct-b", "oauth:anthropic:acct-b");
    let client = client(&routing);
    choose(&client, "oauth:anthropic", false, Some(rows.clone()))
        .await
        .unwrap();
    assert_eq!(
        routing.requests.lock().unwrap()[0]["params"]["candidates"]["accounts"],
        json!([
            { "account_id": "acct-a", "credential_id": "oauth:anthropic" },
            { "account_id": "acct-b", "credential_id": "oauth:anthropic:acct-b" },
        ]),
        "unmapped credentials are not routed"
    );

    // Routing picking an unmapped credential anyway is a pick broca did not offer.
    let routing = FakeRouting::choosing("acct-c", "oauth:anthropic:acct-c");
    let client = self::client(&routing);
    assert_eq!(
        choose(&client, "oauth:anthropic", false, Some(rows.clone()))
            .await
            .unwrap_err(),
        ChoiceRefusal::Unavailable(RoutingUnavailable::invalid_pick())
    );

    let mut fixed = (*rows).clone();
    fixed.credentials[2].provider_ids = vec!["anthropic".into()];
    cache.store(fixed);
    assert_eq!(cache.unmapped().count(), 1);
}

/// A non-model record that broca's exact grant lists, such as the new session storage's
/// deployment key, has no category, no auth_method and no provider ids. It is not a model
/// credential, so it is neither warned about nor counted as unmapped, and never offered.
#[tokio::test]
async fn an_uncategorized_exact_grant_record_is_not_judged_for_routing_metadata() {
    let mut deployment_key = anthropic("opaque:broca-deployment:1", "unused");
    deployment_key.categories.clear();
    deployment_key.credential_type = "unknown".into();
    deployment_key.serves.clear();
    deployment_key.refresh_adapter = None;
    deployment_key.account_id = None;
    deployment_key.provider_ids.clear();
    deployment_key.auth_method = None;
    let mut model_without_method = anthropic("oauth:anthropic:acct-d", "acct-d");
    model_without_method.auth_method = None;
    let rows = listing(vec![
        anthropic("oauth:anthropic", "acct-a"),
        deployment_key,
        model_without_method,
    ]);
    let cache = ListingCache::default();
    let (_, events) = broca_test_support::capture_logs(async {
        cache.store((*rows).clone());
    })
    .await;
    let unmapped: Vec<_> = cache.unmapped().snapshot().into_keys().collect();
    assert_eq!(
        unmapped,
        ["oauth:anthropic:acct-d"],
        "only the model credential is judged"
    );
    let warnings: Vec<String> = events
        .iter()
        .filter(|event| event.level == tracing::Level::WARN)
        .map(|event| event.message().to_owned())
        .collect();
    assert!(
        warnings
            .iter()
            .all(|w| !w.contains("opaque:broca-deployment:1")),
        "{warnings:?}"
    );
    assert_eq!(
        Candidates::from_listing(&rows, AuthMethod::Oauth, "anthropic").accounts,
        [Candidate {
            account_id: "acct-a".into(),
            credential_id: "oauth:anthropic".into()
        }]
    );
    assert!(Candidates::api_keys(&rows, "anthropic").accounts.is_empty());
}

/// `health.check` reports the unmapped count inside the account-selection block.
#[test]
fn health_reports_the_unmapped_count_in_the_account_block() {
    let routing = FakeRouting::refusing(SELECTION_DISABLED, json!({}));
    let client = client(&routing);
    let cache = ListingCache::default();
    let mut unmapped = anthropic("oauth:anthropic:acct-c", "acct-c");
    unmapped.auth_method = None;
    cache.store((*listing(vec![anthropic("oauth:anthropic", "acct-a"), unmapped])).clone());
    publish_counters(client.counters());
    publish_unmapped(cache.unmapped());
    let metrics = published_metrics();
    assert_eq!(metrics["unmapped_credentials"], 1);
    assert_eq!(metrics["routed"], 0, "the counters stay in the same block");
}

#[tokio::test]
async fn two_admissions_of_the_same_send_send_the_same_admission_id() {
    let mut bodies = Vec::new();
    for _ in 0..2 {
        let routing = FakeRouting::choosing("acct-a", "oauth:anthropic");
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
}

#[tokio::test]
async fn undeclared_disabled_and_unsupported_codes_are_refusals_not_fallbacks() {
    for code in ["disabled", "unsupported"] {
        let routing = FakeRouting::refusing(code, json!({}));
        let client = client(&routing);
        let ChoiceRefusal::Refused(refusal) =
            choose(&client, "oauth:anthropic", false, Some(two_accounts()))
                .await
                .unwrap_err()
        else {
            panic!("routing answered a refusal")
        };
        assert_eq!(refusal.routing_code, code);
        assert_eq!(refusal.class, ErrorClass::Transient);
        assert_eq!(client.counters.get("disabled"), 0);
    }
}

#[tokio::test]
async fn account_selection_unsupported_is_refused_permanent_with_its_code_and_detail() {
    assert_eq!(
        refusal_for("account_selection_unsupported").await,
        permanent("account_selection_unsupported")
    );
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
            admission_id: None,
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
    match refusal {
        ChoiceRefusal::Refused(refusal) => refusal,
        other => panic!("expected routing's refusal, got {other:?}"),
    }
}

#[tokio::test]
async fn refusal_log_names_send_and_routing_decision() {
    let routing = FakeRouting::refusing(
        "account_hold",
        json!({
            "code": "account_hold",
            "retry_after_s": 1260,
            "decision_id": "acd-refused",
            "exclusions": [],
        }),
    );
    let client = client(&routing);
    let credential = CredentialId::parse("oauth:anthropic").unwrap();
    let session = session();
    let (result, events) = broca_test_support::capture_logs(choose_account(
        &client,
        ChoiceInputs {
            account_route: None,
            pool: None,
            credential_id: None,
            service_tier: None,
            decision_id: None,
            hold_cap: Default::default(),
            conversation_key: None,
            inherited_conversation_key: None,
            static_credential: &credential,
            caller_named_account: false,
            model: "claude-opus-5-5",
            session: &session,
            send_id: Some("send-refused"),
            run_id: "run-refused",
            initial_input_bytes: 42,
        },
        async { Some(two_accounts()) },
        tokio::time::Instant::now() + ADMISSION_BUDGET,
    ))
    .await;

    let ChoiceRefusal::Refused(refusal) = result.expect_err("routing refusal passes through")
    else {
        panic!("expected routing's refusal")
    };
    assert_eq!(refusal.routing_code, "account_hold");
    assert_eq!(refusal.class, ErrorClass::Transient);
    assert_eq!(refusal.retry_after_s, Some(1260));
    assert_eq!(refusal.decision_id.as_deref(), Some("acd-refused"));
    let event = events
        .iter()
        .find(|event| event.target == "routing" && event.level == tracing::Level::WARN)
        .expect("routing refusal warning");
    assert_eq!(
        event.fields.get("send_id").map(String::as_str),
        Some("send-refused")
    );
    assert_eq!(
        event.fields.get("decision_id").map(String::as_str),
        Some("acd-refused")
    );
}

fn transient(code: &str) -> AccountSelectionRefusal {
    AccountSelectionRefusal {
        routing_code: code.into(),
        retry_after_s: Some(1260),
        resets_at_ms: None,
        routing_detail: Some(Box::new(json!({
            "code": code, "retry_after_s": 1260, "decision_id": "acd-3", "exclusions": []
        }))),
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
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let refusal = refusal_for("account_hold").await;
    let after = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let reset = refusal.resets_at_ms.expect("legacy hold has an end time");
    assert!((before + 1_260_000..=after + 1_260_000).contains(&reset));
    assert_eq!(
        refusal,
        AccountSelectionRefusal {
            resets_at_ms: Some(reset),
            ..transient("account_hold")
        }
    );
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
async fn outage_no_eligible_account_is_an_account_limit_not_unreachable() {
    let detail = json!({
        "code": "no_eligible_account", "retry_after_s": 1260,
        "reset_at_ms": 4_000_000_000_000_u64, "decision_id": "acd-reserve",
        "exclusions": [{"account_id": "acct-a", "reason": "weekly_reserve"}],
    });
    let routing = FakeRouting::refusing("no_eligible_account", detail.clone());
    let client = client(&routing);
    let (result, events) = broca_test_support::capture_logs(choose(
        &client,
        "oauth:anthropic",
        false,
        Some(two_accounts()),
    ))
    .await;
    let ChoiceRefusal::Refused(refusal) = result.unwrap_err() else {
        panic!("routing answered, so it is not unavailable")
    };
    let body = broca_wire::AccountSelectionRefused::new(
        refusal.routing_code,
        refusal.class,
        refusal.retry_after_s,
        refusal.decision_id,
    )
    .with_routing_detail(refusal.routing_detail, refusal.resets_at_ms);
    let wire = serde_json::to_value(body).unwrap();
    assert_eq!(wire["code"], "account_limit_exhausted");
    assert_eq!(wire["detail"]["provider_code"], "account_limit_exhausted");
    assert_eq!(wire["detail"]["class"], "transient");
    assert_eq!(wire["detail"]["routing_code"], "no_eligible_account");
    assert_eq!(wire["detail"]["routing_detail"], detail);
    assert_eq!(wire["detail"]["resets_at_ms"], 4_000_000_000_000_u64);
    assert_eq!(wire["detail"]["retry_after_secs"], 1260.0);
    assert!(!wire.to_string().contains("unreachable"));
    assert!(!wire.to_string().contains("could not be reached"));
    assert!(
        events
            .iter()
            .any(|event| event.message().contains("(no_eligible_account)")),
        "round-trip outcome names routing's code: {events:?}"
    );
    assert!(events
        .iter()
        .all(|event| !event.message().contains("unreachable")));
    assert_eq!(client.counters.get("unreachable"), 0);
}

#[tokio::test]
async fn an_unknown_routing_refusal_is_transient_and_preserved() {
    assert_eq!(
        refusal_for("new_routing_refusal").await,
        transient("new_routing_refusal")
    );
    assert_eq!(
        refusal_for("route_state_unavailable").await,
        transient("route_state_unavailable")
    );
}

#[test]
fn published_and_deployed_refusal_classes_are_mapped_by_name() {
    for code in [
        "account_hold",
        "account_pool_exhausted",
        "account_pool_reserve_limited",
        "account_quota_unknown",
        "account_selection_unavailable",
        "pin_generation_conflict",
        "no_eligible_account",
        "store_error",
        "internal_error",
        "future_code",
    ] {
        assert_eq!(classify_refusal(code), ErrorClass::Transient, "{code}");
    }
    for code in [
        "invalid_request",
        "unknown_decision",
        "admission_conflict",
        "unscoped_caller",
        "account_not_authorized",
        "account_selection_unsupported",
    ] {
        assert_eq!(classify_refusal(code), ErrorClass::Permanent, "{code}");
    }
    assert_eq!(classify_refusal(AUTH_REQUIRED), ErrorClass::Auth);
    let outcome = classify_account_for_reply(Err(serde_json::to_vec(&json!({
        "code": SELECTION_DISABLED,
        "message": "account selection is switched off",
        "detail": { "decision_id": "acd-off" },
    }))
    .unwrap()));
    assert_eq!(outcome.kind(), SELECTION_DISABLED);
    assert_eq!(
        outcome,
        AccountForOutcome::SelectionDisabled {
            decision_id: Some("acd-off".into())
        }
    );
}

fn assert_hold_timing(detail: Value, reset: Option<u64>, wait: Option<u64>) {
    let outcome = classify_account_for_reply_at(
        Err(serde_json::to_vec(&json!({
            "code": "account_hold", "message": "held", "detail": detail,
        }))
        .unwrap()),
        NOW as u64,
    );
    let AccountForOutcome::Refused(refusal) = outcome else {
        panic!("expected a hold")
    };
    assert_eq!(refusal.resets_at_ms, reset);
    assert_eq!(refusal.retry_after_s, wait);
    assert_eq!(refusal.class, ErrorClass::Transient);
    let body = broca_wire::AccountSelectionRefused::new(
        refusal.routing_code,
        refusal.class,
        refusal.retry_after_s,
        refusal.decision_id,
    )
    .with_routing_detail(refusal.routing_detail.clone(), refusal.resets_at_ms);
    assert_eq!(body.code, "account_limit_exhausted");
    assert_eq!(body.detail.resets_at_ms, reset);
    assert_eq!(body.detail.retry_after_secs, wait.map(|s| s as f64));
    assert_eq!(*refusal.routing_detail.unwrap(), detail);
}

#[test]
fn account_hold_until_ms_takes_precedence_over_legacy_retry() {
    assert_hold_timing(
        json!({"until_ms": 0, "retry_after_s": 60}),
        Some(0),
        Some(0),
    );
    assert_hold_timing(
        json!({"until_ms": NOW + 60_001, "retry_after_s": 900, "reset_at": "not a date"}),
        Some(NOW as u64 + 60_001),
        Some(61),
    );
}

#[test]
fn account_hold_legacy_retry_seconds_are_used_without_parsing_reset_at() {
    assert_hold_timing(
        json!({"retry_after_s": 60, "reset_at": "2026-10-28T12:00:00Z"}),
        Some(NOW as u64 + 60_000),
        Some(60),
    );
    assert_hold_timing(
        json!({"retry_after_s": 60, "reset_at": "not a date"}),
        Some(NOW as u64 + 60_000),
        Some(60),
    );
}

#[test]
fn account_hold_legacy_retry_without_reset_at_has_an_end_time() {
    assert_hold_timing(
        json!({"retry_after_s": 60}),
        Some(NOW as u64 + 60_000),
        Some(60),
    );
    assert_hold_timing(json!({"retry_after_s": 0}), Some(NOW as u64), Some(0));
}

#[test]
fn account_hold_legacy_retry_saturates_without_overflow() {
    assert_hold_timing(
        json!({"retry_after_s": u64::MAX}),
        Some(u64::MAX),
        Some((u64::MAX - NOW as u64).div_ceil(1000)),
    );
}

#[test]
fn account_hold_without_timing_remains_an_untimed_account_limit() {
    assert_hold_timing(json!({}), None, None);
    assert_hold_timing(json!({"reset_at": "2026-10-28T12:00:00Z"}), None, None);
}

#[test]
fn a_reset_time_without_a_wait_produces_a_retry_hint() {
    let reset = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 60_000;
    for (code, field) in [
        ("account_pool_exhausted", "reset_at_ms"),
        ("account_hold", "until_ms"),
        ("account_quota_unknown", "reset_at_ms"),
        ("account_pool_reserve_limited", "reset_at_ms"),
    ] {
        let outcome = classify_account_for_reply(Err(serde_json::to_vec(&json!({
            "code": code, "message": "account limited", "detail": {field: reset}
        }))
        .unwrap()));
        let AccountForOutcome::Refused(refusal) = outcome else {
            panic!("expected refusal")
        };
        assert_eq!(refusal.resets_at_ms, Some(reset));
        assert!(matches!(refusal.retry_after_s, Some(59..=60)));
        let body = broca_wire::AccountSelectionRefused::new(
            refusal.routing_code,
            refusal.class,
            refusal.retry_after_s,
            refusal.decision_id,
        )
        .with_routing_detail(refusal.routing_detail, refusal.resets_at_ms);
        assert_eq!(body.code, "account_limit_exhausted");
        assert_eq!(body.detail.resets_at_ms, Some(reset));
        assert!(body.detail.retry_after_secs.is_some());
    }
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
async fn account_not_authorized_is_refused_permanent_with_its_code_and_detail() {
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
            resets_at_ms: None,
            routing_detail: Some(Box::new(json!({
                "code": "invalid_request", "retry_after_s": null,
                "decision_id": null, "exclusions": []
            }))),
            decision_id: None,
            class: ErrorClass::Permanent,
        })
    );
}

// ---- configuration --------------------------------------------------------------------------

/// There is no switch that bypasses routing. The removed `legacy_static` key, on or off, is
/// refused by the strict loader like any unknown key, so a leftover setting fails the load
/// loudly instead of being silently ignored.
#[test]
fn the_removed_break_glass_and_any_unknown_key_are_refused() {
    assert_eq!(
        AccountSelectionConfig::parse("{}").unwrap(),
        AccountSelectionConfig::default()
    );
    let named = AccountSelectionConfig::parse(
        r#"{ // a routing module of its own
            "account_selection": { "routing_module": "routing-b", },
            "providers": {} }"#,
    )
    .unwrap();
    assert_eq!(named.routing_module, "routing-b");
    for leftover in [
        r#"{"account_selection":{"legacy_static":true}}"#,
        r#"{"account_selection":{"legacy_static":false,"routing_module":"routing-b"}}"#,
    ] {
        let error = AccountSelectionConfig::parse(leftover).unwrap_err();
        assert!(error.contains("unknown field `legacy_static`"), "{error}");
    }
    let error = AccountSelectionConfig::parse(r#"{"account_selection":{"legacy_statc":true}}"#)
        .unwrap_err();
    assert!(error.contains("unknown field"), "{error}");
}

// ---- report bodies --------------------------------------------------------------------------

fn attempt(http_status: Option<u16>, mut observation: AttemptAccountObservation) -> ModelAttempt {
    observation.failure_received_at_ms = Some(NOW as u64);
    ModelAttempt {
        content_progress: Default::default(),
        attempt: 0,
        request_body_len: 10,
        request_body_sha256: "aa".into(),
        http_status,
        content_type: None,
        response_body_len: 0,
        response_body_sha256: "bb".into(),
        response_body_cap: None,
        outcome: match http_status {
            Some(200..=299) => AttemptOutcome::Complete,
            Some(_) => AttemptOutcome::HttpError,
            None => AttemptOutcome::ConnectFailed,
        },
        ts_ms: NOW as u64,
        sent_at_ms: None,
        transport: None,
        websocket_request: None,
        cache_evidence: None,
        websocket_close_code: None,
        input_transformations: None,
        input_transformations_unmeasured: false,
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

fn report_key_limit(client: Arc<RoutingClient>, decision: &AccountDecision) -> bool {
    let Some(reporter) = RunReporter::start_for_decision(
        client,
        decision,
        "run-1".into(),
        None,
        Arc::new(AtomicU64::new(3)),
    ) else {
        return false;
    };
    let lineage = lineage();
    broca_core::AccountReporter::run_started(reporter.as_ref(), Some(lineage), 1);
    let observed = attempt(
        Some(429),
        AttemptAccountObservation {
            limit: Some(AttemptAccountLimit {
                disposition: broca_protocol::provider::AccountLimitDisposition::ACCOUNT,
                observed_at_ms: 12_000,
                resets_at_ms: Some(42_000),
                resets_at_source: Some("retry_after".into()),
                limit_window: None,
            }),
            ..Default::default()
        },
    );
    broca_core::AccountReporter::attempt_recorded(reporter.as_ref(), Some(lineage), 2, &observed);
    reporter.finish(Some(lineage), 3, RunFinishReason::Error);
    true
}

async fn wait_for_reporter_release(routing: &Arc<FakeRouting>) {
    broca_test_support::wait_until("admission reporters release routing", || async {
        if Arc::strong_count(routing) == 1 {
            Ok(())
        } else {
            Err("a routing client or reporter is still active".into())
        }
    })
    .await;
}

#[tokio::test]
async fn static_key_limits_send_nothing_but_routed_limits_name_the_issued_admission() {
    for routed in [false, true] {
        let routing = FakeRouting::answering(Ok(Ok(serde_json::to_vec(&json!({"result":{
            "admission":{"id":"run-1","state":"reserved"},
            "account":{"account_id":"key-b","credential_id":"key-b","method":"apikey"}
        }}))
        .unwrap())));
        let client = Arc::new(client(&routing));
        let choice = choose_api_key(
            &client,
            routed.then_some("key-pool"),
            None,
            !routed,
            Some(api_keys()),
        )
        .await
        .unwrap();
        let decision = choice.decision().unwrap();
        let started = report_key_limit(Arc::clone(&client), decision);
        drop(client);
        wait_for_reporter_release(&routing).await;
        let reports = routing.observed();
        if routed {
            assert!(started);
            assert_eq!(decision.admission_id.as_deref(), Some("run-1"));
            assert_eq!(reports.len(), 3);
            assert_eq!(reports[0]["kind"], "admission_started");
            assert_eq!(reports[1]["kind"], "account_limit_exhausted");
            assert_eq!(reports[1]["admission_id"], "run-1");
            assert_eq!(reports[1]["observed_at_ms"], 12_000);
            assert_eq!(reports[2]["kind"], "run_finished");
            let requests = routing.requests.lock().unwrap();
            let admission = requests
                .iter()
                .find(|request| request["method"] == ACCOUNT_FOR_OP)
                .unwrap();
            assert_eq!(admission["params"]["admission_id"], "run-1");
        } else {
            assert!(reports.is_empty(), "static-key reports: {reports:?}");
            assert!(!started);
            assert_eq!(decision.admission_id, None);
            assert!(routing.methods().is_empty());
        }
    }
}

#[tokio::test]
async fn legacy_unheld_admissions_never_start_reporters_or_report_sealed_runs() {
    for path in [
        "pinned",
        "single_candidate",
        "candidates_unknown",
        "disabled",
        "legacy_static",
        "auth_required",
        "future_static",
    ] {
        let routing = Arc::new(FakeRouting::default());
        let client = Arc::new(client(&routing));
        let decision = AccountDecision {
            conversation_key: None,
            conversation_key_source: None,
            path: path.into(),
            admission_id: Some("fabricated-by-an-older-runner".into()),
            decision_id: None,
        };
        let started = report_key_limit(Arc::clone(&client), &decision);
        report_sealed(
            Arc::clone(&client),
            &broca_module::SealedRun {
                session: session(),
                run_id: broca_protocol::RunId("run-1".into()),
                auth_selection: Some(broca_core::AuthSelection {
                    billing_vault_facts: None,
                    auth_method: Some("apikey".into()),
                    credential_id: "key-b".into(),
                    account_ref: None,
                    resolved_credential_id: None,
                    vault_request: None,
                    account_decision: Some(decision),
                }),
                lineage: Some(lineage()),
                terminal_seq: 4,
            },
        );
        drop(client);
        wait_for_reporter_release(&routing).await;
        let reports = routing.observed();
        assert!(!started, "{path} created a reporter: {reports:?}");
        assert!(reports.is_empty(), "{path} emitted reports: {reports:?}");
    }
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
            failure_received_at_ms: None,
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
fn account_limit_exhausted_uses_receipt_time_not_attempt_end() {
    let mut observed = attempt(
        Some(429),
        AttemptAccountObservation {
            failure_received_at_ms: None,
            ratelimit_headers: BTreeMap::new(),
            limit: Some(AttemptAccountLimit {
                disposition: broca_protocol::provider::AccountLimitDisposition::ACCOUNT,
                observed_at_ms: 12_000,
                resets_at_ms: Some(42_000),
                resets_at_source: Some("retry_after".into()),
                limit_window: None,
            }),
        },
    );
    observed.ts_ms = 19_000;
    let report = attempt_body("receipt-event", "admitted-run", &observed, None);
    assert_eq!(report["kind"], "account_limit_exhausted");
    assert_eq!(report["admission_id"], "admitted-run");
    assert_eq!(report["observed_at_ms"], 12_000);
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
        disposition: broca_protocol::provider::AccountLimitDisposition::ACCOUNT,
        observed_at_ms: (NOW + 1_000) as u64,
        resets_at_ms: Some((NOW + 3_600_000) as u64),
        resets_at_source: Some("header".into()),
        limit_window: Some("seven_day".into()),
    };
    let mut observed = attempt(
        Some(429),
        AttemptAccountObservation {
            failure_received_at_ms: None,
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
            failure_received_at_ms: None,
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
async fn terminal_transient_backoff_is_gated_and_precedes_release() {
    for supported in [false, true] {
        let routing = Arc::new(FakeRouting {
            backoff: supported,
            ..FakeRouting::default()
        });
        let client = Arc::new(client(&routing));
        let reporter = RunReporter::start(
            client,
            "adm-key".into(),
            "run-key".into(),
            None,
            Arc::new(AtomicU64::new(10)),
        );
        let lineage = lineage();
        let observed = attempt(Some(429), AttemptAccountObservation::default());
        broca_core::AccountReporter::attempt_recorded(
            reporter.as_ref(),
            Some(lineage),
            2,
            &observed,
        );
        let error = serde_json::from_value(
            json!({"class":"transient","message":"slow down","status":429,
            "retry_after_secs":1.125,"provider_code":"rate_limited"}),
        )
        .unwrap();
        reporter.terminal_transient(Some(lineage), 3, &error);
        reporter.finish(Some(lineage), 3, RunFinishReason::Error);
        broca_test_support::wait_until("API-key release report", || async {
            let observed = routing.observed();
            if observed
                .last()
                .is_some_and(|body| body["kind"] == "run_finished")
            {
                Ok(observed)
            } else {
                Err(format!("reports: {observed:?}"))
            }
        })
        .await;
        let reports = routing.observed();
        assert_eq!(reports.len(), if supported { 3 } else { 2 });
        if supported {
            assert_eq!(
                reports[1],
                json!({"event_id":format!("{lineage}:3:backoff"),"kind":"transient_backoff",
                "caller":{"runner":"broca"},"admission_id":"adm-key","retry_at_ms":NOW + 1125})
            );
            assert_ne!(reports[1]["event_id"], reports[2]["event_id"]);
        }
        assert_eq!(reports.last().unwrap()["admission_id"], "adm-key");
    }
}

#[tokio::test]
async fn terminal_transient_backoff_uses_receipt_time_not_settlement_time() {
    for (receipt, retry_seconds, expected) in [
        (Some(12_000), None, Some(17_000)),
        (Some(12_000), Some(1.25), Some(13_250)),
        (None, None, None),
    ] {
        let routing = Arc::new(FakeRouting {
            backoff: true,
            ..FakeRouting::default()
        });
        let reporter = RunReporter::start(
            Arc::new(client(&routing)),
            "adm-receipt".into(),
            "run-receipt".into(),
            None,
            Arc::new(AtomicU64::new(4)),
        );
        let lineage = lineage();
        let mut observed = attempt(Some(503), AttemptAccountObservation::default());
        observed.account_observation.failure_received_at_ms = receipt;
        observed.ts_ms = 19_000;
        broca_core::AccountReporter::attempt_recorded(
            reporter.as_ref(),
            Some(lineage),
            2,
            &observed,
        );
        let error = serde_json::from_value(json!({
            "class":"transient", "message":"unavailable", "status":503,
            "retry_after_secs":retry_seconds,
        }))
        .unwrap();
        reporter.terminal_transient(Some(lineage), 4, &error);
        reporter.finish(Some(lineage), 4, RunFinishReason::Error);
        drop(reporter);
        broca_test_support::wait_until("receipt reports drained", || async {
            if Arc::strong_count(&routing) == 1 {
                Ok(())
            } else {
                Err("reporter still owns routing".into())
            }
        })
        .await;
        let reports = routing.observed();
        let backoff: Vec<_> = reports
            .iter()
            .filter(|report| report["kind"] == "transient_backoff")
            .collect();
        if let Some(expected) = expected {
            assert_eq!(backoff.len(), 1);
            assert_eq!(backoff[0]["retry_at_ms"], expected);
        } else {
            assert!(
                backoff.is_empty(),
                "missing receipt must never use attempt end"
            );
        }
    }
}

#[tokio::test]
async fn terminal_transient_backoff_requires_a_failed_final_attempt_outcome() {
    for (outcome, status, backoff) in [
        (AttemptOutcome::Complete, Some(503), false),
        (AttemptOutcome::ToolArgsTrailingGarbage, Some(200), false),
        (AttemptOutcome::Cancelled, None, false),
        (AttemptOutcome::SealedInFlight, None, false),
        (AttemptOutcome::Unknown("future".into()), None, false),
        (AttemptOutcome::ProviderError, Some(200), true),
        (AttemptOutcome::ConnectFailed, None, true),
        (AttemptOutcome::HttpError, Some(503), true),
    ] {
        let routing = Arc::new(FakeRouting {
            backoff: true,
            ..FakeRouting::default()
        });
        let reporter = RunReporter::start(
            Arc::new(client(&routing)),
            "adm-final-attempt".into(),
            "run-final-attempt".into(),
            None,
            Arc::new(AtomicU64::new(4)),
        );
        let lineage = lineage();
        let mut observed = attempt(Some(503), AttemptAccountObservation::default());
        observed.ts_ms = 1_000;
        observed.account_observation.failure_received_at_ms = Some(1_000);
        broca_core::AccountReporter::attempt_recorded(
            reporter.as_ref(),
            Some(lineage),
            2,
            &observed,
        );
        // The recorded outcome, not the status, decides whether the final attempt failed.
        observed.http_status = status;
        observed.outcome = outcome.clone();
        observed.ts_ms = 42_000;
        observed.account_observation.failure_received_at_ms = Some(42_000);
        broca_core::AccountReporter::attempt_recorded(
            reporter.as_ref(),
            Some(lineage),
            3,
            &observed,
        );
        let error =
            serde_json::from_value(json!({"class":"transient","message":"failed"})).unwrap();
        reporter.terminal_transient(Some(lineage), 4, &error);
        reporter.finish(Some(lineage), 4, RunFinishReason::Error);
        let reports = broca_test_support::wait_until("final attempt release report", || async {
            let reports = routing.observed();
            if reports
                .last()
                .is_some_and(|body| body["kind"] == "run_finished")
            {
                Ok(reports)
            } else {
                Err(format!("reports: {reports:?}"))
            }
        })
        .await;
        assert_eq!(reports.len(), if backoff { 4 } else { 3 }, "{outcome:?}");
        if backoff {
            assert_eq!(reports[2]["kind"], "transient_backoff");
            assert_eq!(reports[2]["retry_at_ms"], 47_000);
        } else {
            assert!(reports
                .iter()
                .all(|body| body["kind"] != "transient_backoff"));
        }
    }
}

#[tokio::test]
async fn terminal_transient_backoff_excludes_account_limits_and_invalid_delays() {
    for extra in [
        json!({"provider_code":"account_limit_exhausted","status":429,"retry_after_secs":1.0}),
        json!({"provider_code":"rate_limited","retry_after_secs":-1.0}),
        json!({"class":"auth_required","status":401,"retry_after_secs":1.0}),
        json!({"class":"permanent","provider_code":"billing_exhausted","retry_after_secs":1.0}),
    ] {
        let routing = Arc::new(FakeRouting {
            backoff: true,
            ..FakeRouting::default()
        });
        let reporter = RunReporter::start(
            Arc::new(client(&routing)),
            "adm-key".into(),
            "run-key".into(),
            None,
            Arc::new(AtomicU64::new(10)),
        );
        let lineage = lineage();
        broca_core::AccountReporter::attempt_recorded(
            reporter.as_ref(),
            Some(lineage),
            2,
            &attempt(Some(429), AttemptAccountObservation::default()),
        );
        let mut error = json!({"class":"transient","message":"refused"});
        error
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        reporter.terminal_transient(Some(lineage), 3, &serde_json::from_value(error).unwrap());
        reporter.finish(Some(lineage), 3, RunFinishReason::Error);
        let reports = broca_test_support::wait_until("untimed API-key release report", || async {
            let observed = routing.observed();
            if observed
                .last()
                .is_some_and(|body| body["kind"] == "run_finished")
            {
                Ok(observed)
            } else {
                Err(format!("reports: {observed:?}"))
            }
        })
        .await;
        assert_eq!(reports.len(), 2);
    }
}

async fn terminal_credential_reports(
    error: broca_protocol::ProviderError,
    method: &str,
    credential_id: Option<&str>,
) -> Vec<Value> {
    let routing = Arc::new(FakeRouting {
        backoff: true,
        credential_refused: true,
        ..FakeRouting::default()
    });
    let reporter = RunReporter::start(
        Arc::new(client(&routing)),
        "adm-refused".into(),
        "run-refused".into(),
        None,
        Arc::new(AtomicU64::new(10)),
    );
    let lineage = lineage();
    broca_core::AccountReporter::run_started(reporter.as_ref(), Some(lineage), 1);
    let observation = AttemptAccountObservation {
        failure_received_at_ms: None,
        limit: error
            .is_account_limit_exhausted()
            .then(|| AttemptAccountLimit {
                disposition: broca_protocol::provider::AccountLimitDisposition::ACCOUNT,
                observed_at_ms: NOW as u64,
                resets_at_ms: Some((NOW + 60_000) as u64),
                resets_at_source: Some("header".into()),
                limit_window: None,
            }),
        ..Default::default()
    };
    broca_core::AccountReporter::attempt_recorded(
        reporter.as_ref(),
        Some(lineage),
        2,
        &attempt(error.status, observation),
    );
    reporter.terminal_credential_refused(Some(lineage), 3, &error, Some(method), credential_id);
    reporter.terminal_transient(Some(lineage), 3, &error);
    reporter.finish(Some(lineage), 3, RunFinishReason::Error);
    broca_test_support::wait_until("credential refusal release report", || async {
        let reports = routing.observed();
        if reports
            .last()
            .is_some_and(|body| body["kind"] == "run_finished")
        {
            Ok(reports)
        } else {
            Err(format!("reports: {reports:?}"))
        }
    })
    .await
}

fn terminal_error(extra: Value) -> broca_protocol::ProviderError {
    let mut error = json!({"class":"permanent","message":"refused"});
    error
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    serde_json::from_value(error).unwrap()
}

#[tokio::test]
async fn credential_refused_billing_precedes_finish_for_every_routed_method() {
    for method in ["apikey", "oauth", "chatgpt"] {
        for code in [
            "billing_exhausted",
            "credit_insufficient",
            "account_not_entitled",
            "usage_not_included",
        ] {
            let reports = terminal_credential_reports(
                terminal_error(json!({"provider_code":code,"status":402,"retry_after_secs":60.0})),
                method,
                Some("vault-key"),
            )
            .await;
            assert_eq!(
                reports
                    .iter()
                    .map(|body| body["kind"].clone())
                    .collect::<Vec<_>>(),
                vec![
                    json!("admission_started"),
                    json!("attempt"),
                    json!("credential_refused"),
                    json!("run_finished")
                ],
                "{method}: {code}"
            );
            assert_eq!(reports[2]["admission_id"], "adm-refused");
            assert_eq!(reports[2]["credential_id"], "vault-key");
            assert_eq!(reports[2]["provider_code"], code);
            assert!(reports[2].get("retry_at_ms").is_none());
            assert_ne!(reports[2]["event_id"], reports[3]["event_id"]);
            assert_eq!(
                reports[2]["event_id"],
                format!(
                    "{}:credential_refused",
                    reports[3]["event_id"].as_str().unwrap()
                )
            );
        }
    }
}

#[tokio::test]
async fn credential_refused_requires_router_group() {
    for (backoff, supported) in [(true, false), (false, true), (false, false), (true, true)] {
        let routing = Arc::new(FakeRouting {
            backoff,
            credential_refused: supported,
            ..FakeRouting::default()
        });
        client(&routing)
            .observe(json!({"kind":"credential_refused","admission_id":"adm-refused"}))
            .await;
        assert_eq!(routing.observed().len(), usize::from(backoff && supported));
    }
}

#[tokio::test]
async fn credential_refused_auth_is_only_for_api_keys() {
    for method in ["apikey", "oauth", "chatgpt"] {
        for status in [401, 403] {
            let reports = terminal_credential_reports(
                terminal_error(json!({"class":"auth_required","status":status,"provider_code":"invalid_api_key"})),
                method,
                Some("vault-key"),
            ).await;
            let refused: Vec<_> = reports
                .iter()
                .filter(|body| body["kind"] == "credential_refused")
                .collect();
            assert_eq!(
                refused.len(),
                usize::from(method == "apikey"),
                "{method}: {status}"
            );
            if method == "apikey" {
                assert_eq!(refused[0]["provider_code"], "invalid_api_key");
            }
        }
    }
    for extra in [
        json!({"class":"permanent","status":403}),
        json!({"class":"auth_required","status":500}),
    ] {
        let reports =
            terminal_credential_reports(terminal_error(extra), "apikey", Some("vault-key")).await;
        assert_eq!(reports.len(), 3);
    }
}

#[tokio::test]
async fn credential_refused_auth_without_provider_code_still_blocks_key() {
    let reports = terminal_credential_reports(
        terminal_error(json!({"class":"auth_required","status":401})),
        "apikey",
        Some("vault-key"),
    )
    .await;
    assert_eq!(reports.len(), 4);
    assert_eq!(reports[2]["kind"], "credential_refused");
    assert_eq!(reports[2]["admission_id"], "adm-refused");
    assert_eq!(reports[2]["credential_id"], "vault-key");
    assert!(reports[2].get("provider_code").is_none());
    assert!(reports[2].get("retry_at_ms").is_none());
}

#[tokio::test]
async fn credential_refused_excludes_resettable_and_transient_limits() {
    for (code, class, refused, attempt_kind, backoff) in [
        ("billing_exhausted", "permanent", true, "attempt", false),
        (
            "account_limit_exhausted",
            "transient",
            false,
            "account_limit_exhausted",
            false,
        ),
        ("rate_limited", "transient", false, "attempt", true),
    ] {
        let reports = terminal_credential_reports(
            terminal_error(
                json!({"class":class,"provider_code":code,"status":429,"retry_after_secs":1.0}),
            ),
            "apikey",
            None,
        )
        .await;
        assert_eq!(reports[1]["kind"], attempt_kind);
        assert_eq!(
            reports
                .iter()
                .filter(|body| body["kind"] == "credential_refused")
                .count(),
            usize::from(refused),
            "{code}"
        );
        assert_eq!(
            reports
                .iter()
                .filter(|body| body["kind"] == "transient_backoff")
                .count(),
            usize::from(backoff),
            "{code}"
        );
    }
}

#[tokio::test]
async fn credential_refused_request_bytes_preserve_code_and_absence() {
    let routing = Arc::new(FakeRouting {
        backoff: true,
        credential_refused: true,
        ..FakeRouting::default()
    });
    for (name, code) in [
        ("billing", Some("billing_exhausted")),
        ("auth-no-code", None),
    ] {
        client(&routing)
            .observe(credential_refused_body(
                "lineage:3:credential_refused",
                "adm-refused",
                Some("vault-key"),
                code,
            ))
            .await;
        let bytes = routing
            .request_bytes
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .clone();
        let parsed: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed["method"], "route.account_observe");
        let mut expected = json!({"event_id":"lineage:3:credential_refused","caller":{"runner":"broca"},"kind":"credential_refused","admission_id":"adm-refused","credential_id":"vault-key"});
        if let Some(code) = code {
            expected["provider_code"] = json!(code);
        }
        assert_eq!(parsed["params"], expected);
        let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
        println!("credential-refused-sample {name} {hex}");
    }
    assert!(credential_refused_body("event", "adm", None, None)
        .get("credential_id")
        .is_none());
}

#[tokio::test]
async fn backoff_reports_never_reach_router_without_its_group() {
    let routing = Arc::new(FakeRouting::default());
    client(&routing)
        .observe(json!({"kind":"transient_backoff","event_id":"end:backoff",
        "admission_id":"run-key","retry_at_ms":NOW + 1000}))
        .await;
    assert!(routing.requests.lock().unwrap().is_empty());
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
            failure_received_at_ms: None,
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

// Paused clock with tokio's Instant: the budget is measured in virtual time, so the bound
// checks the code's own waiting and not how promptly the host scheduled this test.
#[tokio::test(start_paused = true)]
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
    let started = tokio::time::Instant::now();
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
        billing_vault_facts: None,
        auth_method: None,
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
            path: "routed".into(),
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
        async { Some(listing(vec![anthropic("oauth:anthropic", "acct-a")])) },
    );
    settle(&routing, 2).await;
    assert_eq!(client.counters.get("scoped_not_found_still_listed"), 1);
}

/// Any record, to give a scratch log its lineage id.
fn any_record() -> broca_wal::WalRecord {
    broca_wal::WalRecord::RunFinished {
        interrupt_reason: None,
        ts_ms: None,
        reason: RunFinishReason::Completed,
        usage: Default::default(),
        error: None,
        retries_used: 0,
    }
}

#[tokio::test]
async fn a_caller_decision_is_forwarded_even_with_a_pin_and_is_not_runner_reportable() {
    let routing = FakeRouting::choosing("acct-b", "oauth:anthropic:acct-b");
    let client = client(&routing);
    let credential = CredentialId::parse("oauth:anthropic").unwrap();
    let session = session();
    let choice = choose_account(
        &client,
        ChoiceInputs {
            account_route: None,
            pool: None,
            credential_id: None,
            service_tier: None,
            decision_id: Some("caller-decision"),
            hold_cap: Default::default(),
            conversation_key: None,
            inherited_conversation_key: None,
            static_credential: &credential,
            caller_named_account: true,
            model: "claude-opus-5-5",
            session: &session,
            send_id: None,
            run_id: "run-caller",
            initial_input_bytes: 42,
        },
        async { Some(two_accounts()) },
        tokio::time::Instant::now() + ADMISSION_BUDGET,
    )
    .await
    .unwrap();
    assert_eq!(
        routing.requests.lock().unwrap()[0]["params"]["decision_id"],
        "caller-decision"
    );
    let decision = choice.decision().unwrap();
    assert_eq!(decision.decision_id.as_deref(), Some("acd-7"));
    assert_eq!(
        decision.admission_id, None,
        "caller-owned decisions cannot enable any runner reporter"
    );
}

#[tokio::test]
async fn an_unknown_caller_decision_never_falls_back_to_a_static_account() {
    let routing = FakeRouting::refusing("unknown_decision", json!({}));
    let client = client(&routing);
    let credential = CredentialId::parse("oauth:anthropic").unwrap();
    let session = session();
    let result = choose_account(
        &client,
        ChoiceInputs {
            account_route: None,
            pool: None,
            credential_id: None,
            service_tier: None,
            decision_id: Some("expired"),
            hold_cap: Default::default(),
            conversation_key: None,
            inherited_conversation_key: None,
            static_credential: &credential,
            caller_named_account: false,
            model: "claude-opus-5-5",
            session: &session,
            send_id: None,
            run_id: "run-caller",
            initial_input_bytes: 42,
        },
        async { Some(listing(vec![anthropic("oauth:anthropic", "acct-a")])) },
        tokio::time::Instant::now() + ADMISSION_BUDGET,
    )
    .await;
    assert!(
        matches!(result, Err(ChoiceRefusal::Refused(refusal)) if refusal.routing_code == "unknown_decision")
    );
    assert_eq!(routing.methods(), vec!["route.account_for"]);
}

async fn choose_with_auth_hints(
    client: &RoutingClient,
    credential: &str,
    view: Option<Arc<ScopedList>>,
) -> Result<AccountChoice, ChoiceRefusal> {
    let credential = CredentialId::parse(credential).unwrap();
    choose_account(
        client,
        ChoiceInputs {
            account_route: None,
            decision_id: None,
            service_tier: None,
            pool: Some("selected-pool"),
            credential_id: Some("preferred"),
            conversation_key: None,
            inherited_conversation_key: None,
            static_credential: &credential,
            caller_named_account: false,
            model: "fixture",
            session: &session(),
            send_id: None,
            run_id: "run-hints",
            initial_input_bytes: 1,
            hold_cap: Default::default(),
        },
        async { view },
        tokio::time::Instant::now() + ADMISSION_BUDGET,
    )
    .await
}

#[tokio::test]
async fn auth_routing_hints_consult_routing_even_for_one_candidate() {
    let routing = FakeRouting::choosing("acct-a", "oauth:anthropic");
    let choice = choose_with_auth_hints(
        &client(&routing),
        "oauth:anthropic",
        Some(listing(vec![anthropic("oauth:anthropic", "acct-a")])),
    )
    .await
    .unwrap();
    assert!(
        matches!(choice, AccountChoice::Routed {credential_id, ..} if credential_id == "oauth:anthropic")
    );
    let requests = routing.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0]["params"]["pool"], "selected-pool");
    assert_eq!(
        requests[0]["params"]["requested_account"]["credential_id"],
        "preferred"
    );
}

#[tokio::test]
async fn auth_routing_hints_refuse_an_unknown_candidate_view() {
    let routing = FakeRouting::choosing("acct-a", "oauth:anthropic");
    assert!(matches!(
        choose_with_auth_hints(&client(&routing), "oauth:anthropic", None).await,
        Err(ChoiceRefusal::Unavailable(_))
    ));
    assert!(routing.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn auth_routing_hints_cannot_fall_back_when_selection_is_disabled() {
    let routing = FakeRouting::refusing(SELECTION_DISABLED, json!({}));
    assert!(
        matches!(choose_with_auth_hints(&client(&routing), "oauth:anthropic", Some(two_accounts())).await,
        Err(ChoiceRefusal::Refused(refusal)) if refusal.routing_code == SELECTION_DISABLED)
    );
}

#[tokio::test]
async fn auth_routing_hints_are_ignored_for_unlisted_methods() {
    let routing = FakeRouting::choosing("acct-a", "oauth:anthropic");
    assert_eq!(
        choose_with_auth_hints(&client(&routing), "antigravity:google", None)
            .await
            .unwrap(),
        AccountChoice::NotApplicable
    );
    assert!(routing.requests.lock().unwrap().is_empty());
}
