use super::*;
use cortexkit_role_routing::price::RatesPerMtok;
use cortexkit_role_routing::select::{RouteRef, RunnerRef};
use std::sync::atomic::{AtomicUsize, Ordering};

fn number(text: &str) -> serde_json::Number {
    serde_json::from_str(text).unwrap()
}

/// A route on `provider/model` priced $3 in and $15 out per million tokens.
fn route(provider: &str, model: &str, price: Option<Price>) -> SelectAnswer {
    let route = RouteRef::new(provider, model);
    let answer = SelectAnswer::new(
        "decision-1",
        route.clone(),
        RunnerRef::new("broca", route),
        200_000,
        64_000,
        u64::MAX,
        "the cheapest route meeting the demand",
    );
    match price {
        Some(price) => answer.with_price(price),
        None => answer,
    }
}

fn metered() -> Price {
    Price::metered(RatesPerMtok::new(number("3"), number("15")))
}

/// Routing's answer, and every request it got.
struct FakeSelect {
    answer: Mutex<Result<SelectAnswer, RoutingCallError>>,
    asked: Mutex<Vec<SelectRequest>>,
}

impl FakeSelect {
    fn answering(answer: Result<SelectAnswer, RoutingCallError>) -> Arc<Self> {
        Arc::new(Self {
            answer: Mutex::new(answer),
            asked: Mutex::new(Vec::new()),
        })
    }
}

#[async_trait]
impl Select for FakeSelect {
    async fn select(&self, request: &SelectRequest) -> Result<SelectAnswer, RoutingCallError> {
        self.asked.lock().unwrap().push(request.clone());
        match &*self.answer.lock().unwrap() {
            Ok(answer) => Ok(answer.clone()),
            Err(RoutingCallError::Refused {
                code,
                message,
                detail,
            }) => Err(RoutingCallError::Refused {
                code: code.clone(),
                message: message.clone(),
                detail: detail.clone(),
            }),
            Err(_) => Err(RoutingCallError::Unavailable("down".into())),
        }
    }
}

/// A session runtime whose runs end as `ends` says, one per send, in order; a run reads
/// `active` the first time it is read.
#[derive(Default)]
struct FakeSessions {
    ends: Mutex<Vec<Value>>,
    sent: Mutex<Vec<SendRequest>>,
    reads: Mutex<HashMap<String, usize>>,
    cancelled: Mutex<Vec<String>>,
    /// Sends wait for this to open, so two calls can meet in flight.
    gate: Option<Arc<tokio::sync::Notify>>,
}

impl FakeSessions {
    fn ending(ends: Vec<Value>) -> Arc<Self> {
        Arc::new(Self {
            ends: Mutex::new(ends),
            ..Default::default()
        })
    }

    fn sent(&self) -> Vec<SendRequest> {
        self.sent.lock().unwrap().clone()
    }
}

#[async_trait]
impl Sessions for FakeSessions {
    async fn send(&self, request: SendRequest) -> Result<SendResponse, ServeError> {
        if let Some(gate) = &self.gate {
            gate.notified().await;
        }
        let run_id = request.run_id.0.clone();
        self.sent.lock().unwrap().push(request);
        Ok(SendResponse::Active {
            run_id,
            delivered: None,
        })
    }

    async fn run_result(
        &self,
        session: &LineageKey,
        run_id: RunId,
    ) -> Result<RunResultResponse, ServeError> {
        let index = self
            .sent
            .lock()
            .unwrap()
            .iter()
            .position(|send| &send.session == session)
            .expect("a read follows its send");
        let reads = {
            let mut reads = self.reads.lock().unwrap();
            let count = reads.entry(run_id.0.clone()).or_default();
            *count += 1;
            *count
        };
        let mut end = if reads == 1 {
            json!({"state": "active"})
        } else {
            self.ends.lock().unwrap()[index].clone()
        };
        if end["state"] == "paused" && self.cancelled.lock().unwrap().contains(&run_id.0) {
            end = json!({"state": "cancelled"});
        }
        end["run_id"] = json!(run_id.0);
        Ok(serde_json::from_value(end).unwrap())
    }

    async fn cancel(&self, _session: &LineageKey, run_id: RunId) -> Result<(), ServeError> {
        self.cancelled.lock().unwrap().push(run_id.0);
        Ok(())
    }
}

struct Allow;

#[async_trait]
impl Gate for Allow {
    async fn admit(&self, _scope: &SendScope, _provider: &str) -> Result<(), Vec<u8>> {
        Ok(())
    }
}

struct Refuse(AtomicUsize);

#[async_trait]
impl Gate for Refuse {
    async fn admit(&self, _scope: &SendScope, provider: &str) -> Result<(), Vec<u8>> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Err(unspent(refused(
            batch::MODEL_UNAVAILABLE,
            format!("{provider} may not see this scope's text"),
        )))
    }
}

/// A completion run's end: `usage` with one step on `provider/model`.
fn completed(text: &str, input: Option<u64>, output: Option<u64>) -> Value {
    let mut usage = json!({"steps": 1, "unreported_steps": 0, "by_model": [
        {"provider": "anthropic", "model": "claude-sonnet", "steps": 1, "unreported_steps": 0}
    ]});
    if let Some(input) = input {
        usage["input_tokens"] = json!(input);
        usage["by_model"][0]["input_tokens"] = json!(input);
    }
    if let Some(output) = output {
        usage["output_tokens"] = json!(output);
        usage["by_model"][0]["output_tokens"] = json!(output);
    }
    json!({"state": "completed", "usage": usage,
           "final_message": {"ordinal": 2, "mid": "m2", "text": text}})
}

fn principal() -> BindPrincipal {
    BindPrincipal::Reserved {
        module_id: "basal".into(),
    }
}

fn flow_scope() -> SendScope {
    SendScope::Stamped(broca_protocol::BindScope {
        scope: broca_protocol::SessionScope {
            owner: BindPrincipal::Reserved {
                module_id: "basal".into(),
            },
            agent_id_frozen: false,
            scope_ref: "flow-scope".into(),
            scope_epoch: 1,
            flow_id: Some("flow-1".into()),
            sender: None,
            agent_id: None,
            run_id: None,
        },
        owner_authorized: true,
        kind: broca_protocol::ScopeKind::Head,
        parent: None,
        parent_state: None,
    })
}

struct Rig {
    complete: Arc<Complete>,
    select: Arc<FakeSelect>,
    sessions: Arc<FakeSessions>,
    store: Arc<SystemOneStore>,
}

fn rig(select: Arc<FakeSelect>, sessions: Arc<FakeSessions>) -> Rig {
    let store = Arc::new(SystemOneStore::open_in_memory().unwrap());
    let complete = Arc::new(
        Complete::new(
            store.clone(),
            "test-owner",
            select.clone(),
            sessions.clone(),
            "broca",
        )
        .with_clock(Arc::new(|| 1_000_000))
        .with_poll(Duration::from_millis(1)),
    );
    Rig {
        complete,
        select,
        sessions,
        store,
    }
}

fn arguments(value: Value) -> Box<RawValue> {
    serde_json::value::to_raw_value(&value).unwrap()
}

fn text_call() -> Value {
    json!({"demand": {"iq": 60, "eq": 20}, "prompt": "Name a colour.", "system": "Be brief.",
           "max_output_tokens": 500, "max_cost_usd": 0.5})
}

impl Rig {
    async fn call(&self, args: Value, call_key: Option<&str>) -> Answer {
        self.complete
            .answer(
                Some(&principal()),
                &flow_scope(),
                &arguments(args),
                call_key,
                &Allow,
            )
            .await
    }
}

fn refusal(body: &[u8]) -> Value {
    serde_json::from_slice(body).unwrap()
}

/// A text completion asks routing as its caller and scope kind, sends the caller's text on the
/// chosen route with no tools and no prompt cache under the caller's principal and scope, and
/// answers the text with the cost of the reported tokens at the route's frozen rates.
#[tokio::test]
async fn a_completion_routes_by_demand_runs_once_and_prices_its_tokens() {
    let rig = rig(
        FakeSelect::answering(Ok(route("anthropic", "claude-sonnet", Some(metered())))),
        FakeSessions::ending(vec![completed("Teal.", Some(1_000), Some(500))]),
    );
    let answer = rig.call(text_call(), Some("k1")).await.unwrap();
    // 1,000 tokens at $3 and 500 at $15 per million.
    assert_eq!(
        serde_json::from_str::<Value>(answer.get()).unwrap(),
        json!({"text": "Teal.", "model": {"provider": "anthropic", "model": "claude-sonnet"},
               "usage": {"input_tokens": 1000, "output_tokens": 500}, "cost_usd": 0.0105})
    );
    let asked = rig.select.asked.lock().unwrap().clone();
    assert_eq!(asked.len(), 1);
    assert_eq!(asked[0].consumer, "broca.complete.basal.flow");
    assert_eq!(asked[0].runners, Some(vec!["broca".to_owned()]));
    let wanted = &asked[0].requirements;
    assert_eq!((wanted.iq, wanted.eq), (60, Some(20)));
    assert_eq!(wanted.max_output_tokens, Some(500));
    assert_eq!(wanted.max_cost_usd, Some(number("0.5")));
    assert_eq!(wanted.structured_output, None);
    // The prompt's and system text's byte lengths, plus broca's framing.
    assert_eq!(
        wanted.input_tokens,
        Some(("Name a colour.".len() + "Be brief.".len()) as u64 + OVERHEAD_TOKENS)
    );
    let sent = rig.sessions.sent();
    assert_eq!(sent.len(), 1);
    let send = &sent[0];
    assert_eq!(send.session.harness, "broca");
    assert!(send.session.session.starts_with("llm-complete:"));
    assert!(send.session.session.ends_with(":0"));
    assert_eq!(send.send_id.as_deref(), Some(SEND_ID));
    assert_eq!(send.retention, Some(RETENTION_SECS));
    assert!(
        send.skip_materialization,
        "the session closes when its run ends"
    );
    let request = &send.request;
    assert_eq!(
        (
            request.model.provider_module_id.as_str(),
            request.model.model_id.as_str()
        ),
        ("anthropic", "claude-sonnet")
    );
    assert_eq!(request.tools, Some(Vec::new()));
    assert_eq!(request.generation.max_output_tokens, Some(500));
    assert_eq!(
        request.cache.as_ref().and_then(|cache| cache.ttl),
        Some(cortexkit_role_llm_runner::cache::Ttl::None)
    );
    assert!(request.structured_output.is_none());
    assert_eq!(request.principal, Some(principal()));
    assert_eq!(request.scope, flow_scope());
    let texts: Vec<_> = request
        .input
        .iter()
        .map(|message| {
            let text = match message.content.as_slice() {
                [broca_protocol::content::ContentBlock::Text { text }] => text.to_string(),
                other => panic!("one text block, not {other:?}"),
            };
            (message.role, text)
        })
        .collect();
    assert_eq!(
        texts,
        [
            (
                broca_protocol::content::Role::System,
                "Be brief.".to_owned()
            ),
            (
                broca_protocol::content::Role::User,
                "Name a colour.".to_owned()
            ),
        ]
    );
}

/// A repeat of the key answers the recorded result byte for byte, and asks routing and the
/// provider nothing. The same key with other arguments is refused, spending nothing.
#[tokio::test]
async fn a_repeat_of_the_key_replays_and_another_body_is_refused() {
    let rig = rig(
        FakeSelect::answering(Ok(route("anthropic", "claude-sonnet", Some(metered())))),
        FakeSessions::ending(vec![completed("Teal.", Some(1_000), Some(500))]),
    );
    let first = rig.call(text_call(), Some("k1")).await.unwrap();
    let again = rig.call(text_call(), Some("k1")).await.unwrap();
    assert_eq!(first.get(), again.get());
    assert_eq!(rig.select.asked.lock().unwrap().len(), 1);
    assert_eq!(rig.sessions.sent().len(), 1);
    let mut other = text_call();
    other["prompt"] = json!("Name a fruit.");
    let refused = refusal(&rig.call(other, Some("k1")).await.unwrap_err());
    assert_eq!(refused["code"], "invalid_params");
    assert_eq!(refused["detail"]["field"], "call_key");
    assert_eq!(refused["detail"]["no_spend"], true);
    assert_eq!(rig.sessions.sent().len(), 1);
}

/// A route whose input estimate plus the whole output cap could cost more than the ceiling is
/// refused before anything is sent or recorded; so is a route routing could not price.
#[tokio::test]
async fn a_route_over_the_ceiling_or_unpriced_is_refused_before_the_call() {
    let rig = rig(
        FakeSelect::answering(Ok(route("anthropic", "claude-sonnet", Some(metered())))),
        FakeSessions::ending(vec![]),
    );
    let mut call = text_call();
    // 500 output tokens alone cost $0.0075.
    call["max_cost_usd"] = json!(0.005);
    let refused = refusal(&rig.call(call, Some("k1")).await.unwrap_err());
    assert_eq!(refused["code"], "cost_exceeded");
    assert_eq!(refused["detail"]["no_spend"], true);
    assert!(refused["detail"]["estimate_usd"].as_f64().unwrap() > 0.005);
    assert!(rig.sessions.sent().is_empty());
    // Nothing was recorded: a sweep past every retention finds no call to remove.
    assert_eq!(rig.store.sweep(i64::MAX).unwrap().batches, 0);

    *rig.select.answer.lock().unwrap() = Ok(route("anthropic", "claude-sonnet", None));
    let refused = refusal(&rig.call(text_call(), Some("k2")).await.unwrap_err());
    assert_eq!(refused["code"], "cost_unpriceable");
    assert_eq!(refused["detail"]["no_spend"], true);
    assert!(rig.sessions.sent().is_empty());
}

/// Routing finding no route answers `no_route`; any other routing failure is a transient
/// `provider_error`. Both spent nothing.
#[tokio::test]
async fn routing_refusals_map_to_completion_codes() {
    let rig = rig(
        FakeSelect::answering(Err(RoutingCallError::Refused {
            code: "no_route".into(),
            message: "nothing meets iq 99".into(),
            detail: Value::Null,
        })),
        FakeSessions::ending(vec![]),
    );
    let refused = refusal(&rig.call(text_call(), Some("k1")).await.unwrap_err());
    assert_eq!(refused["code"], "no_route");
    assert_eq!(refused["detail"]["class"], "permanent");
    assert_eq!(refused["detail"]["no_spend"], true);
    *rig.select.answer.lock().unwrap() = Err(RoutingCallError::Unavailable("down".into()));
    let refused = refusal(&rig.call(text_call(), Some("k1")).await.unwrap_err());
    assert_eq!(refused["code"], "provider_error");
    assert_eq!(refused["detail"]["class"], "transient");
    assert_eq!(refused["detail"]["no_spend"], true);
    assert!(rig.sessions.sent().is_empty());
}

/// With a schema the run is asked for a final-text JSON answer, routing is told structured
/// output is required, and the validated value is the answer. A value that failed the schema is
/// `output_invalid`, charged as measured.
#[tokio::test]
async fn a_schema_call_answers_json_and_a_failed_value_is_output_invalid() {
    let schema = json!({"type": "object", "properties": {"colour": {"type": "string"}},
                        "required": ["colour"]});
    let mut passed = completed(r#"{"colour":"teal"}"#, Some(1_000), Some(500));
    passed["structured_output"] = json!({"mode": "final_text", "enforcement": "enforced",
        "validation": "passed", "value": {"colour": "teal"}});
    let mut failed = completed(r#"{"hue":"teal"}"#, Some(1_000), Some(500));
    failed["structured_output"] = json!({"mode": "final_text", "enforcement": "best_effort",
        "validation": "failed", "value": {"hue": "teal"},
        "errors": ["#: \"colour\" is a required property"]});
    let rig = rig(
        FakeSelect::answering(Ok(route("anthropic", "claude-sonnet", Some(metered())))),
        FakeSessions::ending(vec![passed, failed]),
    );
    let mut call = text_call();
    call["output_schema"] = schema.clone();
    let answer = rig.call(call.clone(), Some("k1")).await.unwrap();
    let answer: Value = serde_json::from_str(answer.get()).unwrap();
    assert_eq!(answer["json"], json!({"colour": "teal"}));
    assert!(answer.get("text").is_none());
    assert_eq!(
        rig.select.asked.lock().unwrap()[0]
            .requirements
            .structured_output,
        Some(true)
    );
    let sent = rig.sessions.sent();
    let structured = sent[0].request.structured_output.as_ref().unwrap();
    assert_eq!(
        structured.mode,
        broca_protocol::structured_output::Mode::FinalText
    );
    assert_eq!(structured.schema, schema);

    let refused = refusal(&rig.call(call, Some("k2")).await.unwrap_err());
    assert_eq!(refused["code"], "output_invalid");
    assert_eq!(refused["detail"]["cost_usd"], 0.0105);
    assert!(refused["message"].as_str().unwrap().contains("colour"));
}

/// A schema broca can't compile, an unknown member and an unstamped caller are refused before
/// routing is asked.
#[tokio::test]
async fn admission_refuses_bad_arguments_and_unstamped_callers() {
    let rig = rig(
        FakeSelect::answering(Ok(route("anthropic", "claude-sonnet", Some(metered())))),
        FakeSessions::ending(vec![]),
    );
    let mut call = text_call();
    call["output_schema"] = json!({"$ref": "https://example.com/schema.json"});
    let refused = refusal(&rig.call(call, Some("k1")).await.unwrap_err());
    assert_eq!(refused["code"], "invalid_params");
    assert_eq!(refused["detail"]["field"], "output_schema");
    let mut call = text_call();
    call["model"] = json!("anthropic/claude-sonnet");
    let refused = refusal(&rig.call(call, Some("k1")).await.unwrap_err());
    assert_eq!(refused["detail"]["field"], "model");
    // Basal needs a flow; any other caller needs a stamped run or flow scope.
    for (caller, code) in [
        (Some(principal()), "flow_scope_required"),
        (None, "scope_required"),
    ] {
        let refused = refusal(
            &rig.complete
                .answer(
                    caller.as_ref(),
                    &SendScope::Unstamped,
                    &arguments(text_call()),
                    Some("k1"),
                    &Allow,
                )
                .await
                .unwrap_err(),
        );
        assert_eq!(refused["code"], code);
        assert_eq!(refused["detail"]["no_spend"], true);
    }
    assert!(rig.select.asked.lock().unwrap().is_empty());
}

/// An answer whose output count the provider didn't report is withheld as `cost_unmeasured`,
/// which carries no other spend mark.
#[tokio::test]
async fn an_answer_without_its_token_counts_is_withheld() {
    let rig = rig(
        FakeSelect::answering(Ok(route("anthropic", "claude-sonnet", Some(metered())))),
        FakeSessions::ending(vec![completed("Teal.", Some(1_000), None)]),
    );
    let refused = refusal(&rig.call(text_call(), Some("k1")).await.unwrap_err());
    assert_eq!(refused["code"], "cost_unmeasured");
    for mark in ["cost_usd", "cost_unknown", "no_spend"] {
        assert!(refused["detail"].get(mark).is_none(), "{mark}");
    }
}

#[tokio::test]
async fn unknown_completion_spend_uses_the_charge_ceiling_on_resend() {
    let failed = json!({"state": "error", "usage": {"steps": 0, "unreported_steps": 0},
        "error": {"class": "transient", "message": "stream cut", "provider_code": "provider_error"}});
    let rig = rig(
        FakeSelect::answering(Ok(route("anthropic", "claude-sonnet", Some(metered())))),
        FakeSessions::ending(vec![failed, completed("Teal.", Some(1_000), Some(500))]),
    );
    let first = refusal(&rig.call(text_call(), Some("unknown")).await.unwrap_err());
    assert_eq!(first["detail"]["cost_unknown"], true);
    let again = rig.call(text_call(), Some("unknown")).await;
    assert!(
        again.is_err(),
        "the resend must not start another completion: {again:?}"
    );
    let again = refusal(&again.unwrap_err());
    assert_eq!(again["code"], "cost_exceeded");
    assert_eq!(again["detail"]["no_spend"], true);
    assert_eq!(rig.sessions.sent().len(), 1);
}

/// An unbilled transient failure is recorded, but a repeat can still run in a fresh session.
#[tokio::test]
async fn a_transient_failure_is_tried_again_in_a_fresh_session() {
    let failed = json!({"state": "error", "usage": {"steps": 0, "unreported_steps": 0},
        "error": {"class": "transient", "message": "all slots occupied", "provider_code": "concurrency_full"}});
    let rig = rig(
        FakeSelect::answering(Ok(route("anthropic", "claude-sonnet", Some(metered())))),
        FakeSessions::ending(vec![failed, completed("Teal.", Some(1_000), Some(500))]),
    );
    let refused = refusal(&rig.call(text_call(), Some("k1")).await.unwrap_err());
    assert_eq!(refused["code"], "rate_limited");
    assert_eq!(refused["detail"]["no_spend"], true);
    let answer = rig.call(text_call(), Some("k1")).await.unwrap();
    assert!(answer.get().contains("Teal."));
    let sent = rig.sessions.sent();
    assert_eq!(sent.len(), 2);
    assert!(sent[1].session.session.ends_with(":1"));
    assert_eq!(
        sent[0].session.session.trim_end_matches(":0"),
        sent[1].session.session.trim_end_matches(":1")
    );
    // Routing chose once: every attempt runs on the route the call was admitted with.
    assert_eq!(rig.select.asked.lock().unwrap().len(), 1);
    // The answer is final now.
    let again = rig.call(text_call(), Some("k1")).await.unwrap();
    assert_eq!(answer.get(), again.get());
    assert_eq!(rig.sessions.sent().len(), 2);
}

/// A run that pauses on its account's usage limit is stopped and answered
/// `account_limit_exhausted`, spending nothing; a permanent provider refusal answered before
/// any model work is `invalid_item` and is final.
#[tokio::test]
async fn a_paused_run_is_stopped_and_a_refused_request_is_final() {
    let paused = json!({"state": "paused", "reason": "account_limit",
        "usage": {"steps": 0, "unreported_steps": 0}});
    let invalid = json!({"state": "error", "usage": {"steps": 0, "unreported_steps": 0},
        "error": {"class": "permanent", "message": "bad request", "provider_code": "provider_error",
                  "status": 400}});
    let rig = rig(
        FakeSelect::answering(Ok(route("anthropic", "claude-sonnet", Some(metered())))),
        FakeSessions::ending(vec![paused, invalid]),
    );
    let refused = refusal(&rig.call(text_call(), Some("k1")).await.unwrap_err());
    assert_eq!(refused["code"], "account_limit_exhausted");
    assert_eq!(refused["detail"]["no_spend"], true);
    assert_eq!(rig.sessions.cancelled.lock().unwrap().len(), 1);
    let refused = refusal(&rig.call(text_call(), Some("k2")).await.unwrap_err());
    assert_eq!(refused["code"], "invalid_item");
    assert_eq!(refused["detail"]["no_spend"], true);
    let again = refusal(&rig.call(text_call(), Some("k2")).await.unwrap_err());
    assert_eq!(again, refused);
    assert_eq!(rig.sessions.sent().len(), 2);
}

/// Two calls with one key at once share one run and one answer.
#[tokio::test]
async fn concurrent_repeats_share_one_run() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let sessions = Arc::new(FakeSessions {
        ends: Mutex::new(vec![completed("Teal.", Some(1_000), Some(500))]),
        gate: Some(gate.clone()),
        ..Default::default()
    });
    let rig = rig(
        FakeSelect::answering(Ok(route("anthropic", "claude-sonnet", Some(metered())))),
        sessions,
    );
    let (first, second) = tokio::join!(rig.call(text_call(), Some("k1")), async {
        // Let the first call reach its send before the second arrives.
        tokio::task::yield_now().await;
        let second = rig.call(text_call(), Some("k1"));
        gate.notify_one();
        second.await
    });
    assert_eq!(first.unwrap().get(), second.unwrap().get());
    assert_eq!(rig.sessions.sent().len(), 1);
}

/// A scope the private-content gate refuses sends nothing and records nothing.
#[tokio::test]
async fn a_gated_scope_sends_nothing() {
    let rig = rig(
        FakeSelect::answering(Ok(route("anthropic", "claude-sonnet", Some(metered())))),
        FakeSessions::ending(vec![]),
    );
    let gate = Refuse(AtomicUsize::new(0));
    let body = rig
        .complete
        .answer(
            Some(&principal()),
            &flow_scope(),
            &arguments(text_call()),
            Some("k1"),
            &gate,
        )
        .await
        .unwrap_err();
    assert_eq!(refusal(&body)["detail"]["no_spend"], true);
    assert_eq!(gate.0.load(Ordering::Relaxed), 1);
    assert!(rig.sessions.sent().is_empty());
}

/// Every `llm_complete` reply shape a caller decodes, as the tool-provider route writes it: the
/// catalog listing both tools, a text answer, a JSON answer, its replay, and refusals before and
/// after the call, each for a consumer with no result format and one that declared
/// `tool-result/v1`.
/// Written under `BROCA_LLM_COMPLETE_SAMPLE_DIR` when it names a folder, for the callers to decode
/// before the tool is served.
#[tokio::test]
async fn route_replies_for_the_callers() {
    use crate::system_one::{BatchCatalog, Config, ModelPin, SystemOne};
    struct NoBatchCatalog;
    impl BatchCatalog for NoBatchCatalog {
        fn row(&self, _: &str) -> Option<broca_catalog::ModelBatchRow> {
            None
        }
    }
    /// The private-content answer that the caller's flow read none of the person's private
    /// content, so it may reach any provider routing picks.
    struct Unmarked;
    #[async_trait]
    impl super::super::Asks for Unmarked {
        async fn marked(
            &self,
            _scope_ref: &str,
            _parent_scope_ref: Option<&str>,
        ) -> Option<broca_subc::PrivateContent> {
            Some(broca_subc::PrivateContent::unmarked())
        }

        async fn flow_providers(
            &self,
            _flow_id: &str,
        ) -> Result<broca_subc::FlowPolicy, broca_subc::FlowProvidersError> {
            Err(broca_subc::FlowProvidersError::Unanswered(
                "no Basal behind this route".into(),
            ))
        }
    }
    let dir = std::env::var_os("BROCA_LLM_COMPLETE_SAMPLE_DIR").map(std::path::PathBuf::from);
    let mut json_answer = completed(r#"{"colour":"teal"}"#, Some(1_000), Some(500));
    json_answer["structured_output"] = json!({"mode": "final_text", "enforcement": "enforced",
        "validation": "passed", "value": {"colour": "teal"}});
    let refused_by_provider = json!({"state": "error", "usage": {"steps": 0, "unreported_steps": 0},
        "error": {"class": "transient", "message": "upstream stream ended early",
                  "provider_code": "provider_error"}});
    let schema = json!({"type": "object", "properties": {"colour": {"type": "string"}},
                        "required": ["colour"]});
    let mut json_call = text_call();
    json_call["output_schema"] = schema;
    let mut over_ceiling = text_call();
    over_ceiling["max_cost_usd"] = json!(0.005);
    for (format, folder) in [
        (None, "bare"),
        (Some(super::super::route::TOOL_RESULT_V1), "tool-result-v1"),
    ] {
        let ends = vec![
            completed("Teal.", Some(1_000), Some(500)),
            json_answer.clone(),
            refused_by_provider.clone(),
        ];
        let rig = rig(
            FakeSelect::answering(Ok(route("anthropic", "claude-sonnet", Some(metered())))),
            FakeSessions::ending(ends),
        );
        let system = SystemOne::new(
            rig.store.clone(),
            "samples",
            Arc::new(NoBatchCatalog),
            Vec::<ModelPin>::new(),
            Config::default(),
            Arc::default(),
        )
        .with_store_role(crate::system_one::LLM_CLASSIFY_STORE_ROLE);
        let tools = Arc::new(
            super::super::route::LlmTools::new(
                Arc::new(system),
                crate::config::LlmToolsConfig {
                    advertise_tags: true,
                    image_references: false,
                    private_content_providers: Vec::new(),
                },
                Arc::new(Unmarked),
            )
            .with_complete(rig.complete.clone()),
        );
        let consumer = super::super::route::ConsumerFormat {
            result_format: format,
            held_reply: false,
        };
        let ask = |body: Value| {
            let tools = tools.clone();
            async move {
                tools
                    .answer_for_consumer(
                        &serde_json::to_vec(&body).unwrap(),
                        Some(&principal()),
                        &flow_scope(),
                        consumer,
                    )
                    .await
            }
        };
        let call = |args: &Value, key: &str| json!({"name": COMPLETE_TOOL_NAME, "call_key": key, "arguments": args});
        let samples = [
            (
                "catalog",
                ask(json!({"name": "tool.catalog", "arguments": {"params": {}}})).await,
            ),
            ("text-answer", ask(call(&text_call(), "k-text")).await),
            ("json-answer", ask(call(&json_call, "k-json")).await),
            ("json-answer-replay", ask(call(&json_call, "k-json")).await),
            (
                "refused-provider-error",
                ask(call(&text_call(), "k-failed")).await,
            ),
            (
                "refused-cost-exceeded",
                ask(call(&over_ceiling, "k-over")).await,
            ),
        ];
        let mut texts = Vec::new();
        for (name, answer) in samples {
            let (kind, bytes) = match answer {
                Ok(raw) => ("ok", raw.get().as_bytes().to_vec()),
                Err(body) => ("refused", body),
            };
            texts.push((name, kind, bytes));
        }
        // The catalog lists both tools; a JSON answer replays byte for byte.
        let catalog: Value = serde_json::from_slice(&texts[0].2).unwrap();
        let names: Vec<_> = catalog["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|tool| tool["name"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(names, ["llm_classify", COMPLETE_TOOL_NAME]);
        assert_eq!(texts[2].2, texts[3].2);
        assert_eq!(
            texts.iter().map(|(_, kind, _)| *kind).collect::<Vec<_>>(),
            ["ok", "ok", "ok", "ok", "refused", "refused"],
            "{:?}",
            texts
                .iter()
                .map(|(name, _, bytes)| format!("{name}: {}", String::from_utf8_lossy(bytes)))
                .collect::<Vec<_>>()
        );
        if let Some(dir) = &dir {
            let folder = dir.join(folder);
            std::fs::create_dir_all(&folder).unwrap();
            for (name, kind, bytes) in &texts {
                std::fs::write(folder.join(format!("{name}.{kind}.json")), bytes).unwrap();
            }
        }
    }
}
