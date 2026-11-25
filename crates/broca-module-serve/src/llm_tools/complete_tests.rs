use super::*;
use cortexkit_role_routing::price::RatesPerMtok;
use cortexkit_role_routing::select::{RouteRef, RunnerAuth, RunnerRef};
use std::sync::atomic::{AtomicUsize, Ordering};

fn number(text: &str) -> serde_json::Number {
    serde_json::from_str(text).unwrap()
}

/// A route on `provider/model` priced $3 in and $15 out per million tokens.
pub(crate) fn route(provider: &str, model: &str, price: Option<Price>) -> SelectAnswer {
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

pub(crate) fn metered() -> Price {
    Price::metered(RatesPerMtok::new(number("3"), number("15")))
}

type Hook = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

/// Routing's answer, and every request it got.
pub(crate) struct FakeSelect {
    answer: Mutex<Result<SelectAnswer, RoutingCallError>>,
    asked: Mutex<Vec<SelectRequest>>,
    /// Run before the next select answers: what another process does meanwhile.
    before: Mutex<Option<Hook>>,
    /// The next select panics.
    panic_next: std::sync::atomic::AtomicBool,
}

impl FakeSelect {
    pub(crate) fn answering(answer: Result<SelectAnswer, RoutingCallError>) -> Arc<Self> {
        Arc::new(Self {
            answer: Mutex::new(answer),
            asked: Mutex::new(Vec::new()),
            before: Mutex::new(None),
            panic_next: std::sync::atomic::AtomicBool::new(false),
        })
    }
}

#[async_trait]
impl Select for FakeSelect {
    async fn select(&self, request: &SelectRequest) -> Result<SelectAnswer, RoutingCallError> {
        if self.panic_next.swap(false, Ordering::SeqCst) {
            panic!("routing's stand-in panics");
        }
        let before = self.before.lock().unwrap().take();
        if let Some(before) = before {
            before.await;
        }
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

/// A session runtime whose runs end as `ends` says, one per session, in order; a run reads
/// `active` the first time it is read. A repeat of a send into a session that already holds a
/// run answers that run, as a repeated send id does in a real session.
#[derive(Default)]
pub(crate) struct FakeSessions {
    ends: Mutex<Vec<Value>>,
    sent: Mutex<Vec<SendRequest>>,
    reads: Mutex<HashMap<String, usize>>,
    cancelled: Mutex<Vec<String>>,
    /// Sends wait for this to open, so two calls can meet in flight.
    gate: Option<Arc<tokio::sync::Notify>>,
    /// This many reads of a run fail before any succeeds.
    read_errors: Mutex<usize>,
    /// A cancel is accepted but never stops the run.
    ignore_cancel: bool,
}

impl FakeSessions {
    pub(crate) fn ending(ends: Vec<Value>) -> Arc<Self> {
        Arc::new(Self {
            ends: Mutex::new(ends),
            ..Default::default()
        })
    }

    pub(crate) fn sent(&self) -> Vec<SendRequest> {
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
        let mut sent = self.sent.lock().unwrap();
        if let Some(first) = sent.iter().find(|send| send.session == request.session) {
            return Ok(SendResponse::Active {
                run_id: first.run_id.0.clone(),
                delivered: None,
            });
        }
        sent.push(request);
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
        {
            let mut errors = self.read_errors.lock().unwrap();
            if *errors > 0 {
                *errors -= 1;
                return Err(ServeError::Open(
                    "the run's records could not be read".into(),
                    None,
                ));
            }
        }
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
        if end["state"] == "paused"
            && !self.ignore_cancel
            && self.cancelled.lock().unwrap().contains(&run_id.0)
        {
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
pub(crate) fn completed(text: &str, input: Option<u64>, output: Option<u64>) -> Value {
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
    /// The time the rig's clock reads, in milliseconds.
    now: Arc<std::sync::atomic::AtomicI64>,
}

fn rig(select: Arc<FakeSelect>, sessions: Arc<FakeSessions>) -> Rig {
    rig_on(
        Arc::new(SystemOneStore::open_in_memory().unwrap()),
        "test-owner",
        select,
        sessions,
    )
}

/// A rig on `store`, which another rig may share as another process would, holding records as
/// `owner`.
fn rig_on(
    store: Arc<SystemOneStore>,
    owner: &str,
    select: Arc<FakeSelect>,
    sessions: Arc<FakeSessions>,
) -> Rig {
    let now = Arc::new(std::sync::atomic::AtomicI64::new(1_000_000));
    let clock = now.clone();
    let complete = Arc::new(
        Complete::new(
            store.clone(),
            owner,
            select.clone(),
            sessions.clone(),
            "broca",
        )
        .with_clock(Arc::new(move || clock.load(Ordering::SeqCst)))
        .with_poll(Duration::from_millis(1))
        .with_stop_wait(Duration::from_millis(50)),
    );
    Rig {
        complete,
        select,
        sessions,
        store,
        now,
    }
}

fn arguments(value: Value) -> Box<RawValue> {
    serde_json::value::to_raw_value(&value).unwrap()
}

pub(crate) fn text_call() -> Value {
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
                None,
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
                    None,
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

/// A transient failure is answered and recorded, and it is final for its key: a repeat answers
/// the same refusal with the same spend mark and runs nothing.
#[tokio::test]
async fn a_transient_failure_is_final_for_its_key() {
    let failed = json!({"state": "error", "usage": {"steps": 0, "unreported_steps": 0},
        "error": {"class": "transient", "message": "stream cut", "provider_code": "provider_error"}});
    let rig = rig(
        FakeSelect::answering(Ok(route("anthropic", "claude-sonnet", Some(metered())))),
        FakeSessions::ending(vec![failed, completed("Teal.", Some(1_000), Some(500))]),
    );
    let first = rig.call(text_call(), Some("k1")).await.unwrap_err();
    let refused = refusal(&first);
    assert_eq!(refused["code"], "provider_error");
    assert_eq!(refused["detail"]["class"], "transient");
    assert_eq!(refused["detail"]["cost_unknown"], true);
    let again = rig.call(text_call(), Some("k1")).await.unwrap_err();
    assert_eq!(again, first);
    assert_eq!(rig.sessions.sent().len(), 1);
    assert_eq!(rig.select.asked.lock().unwrap().len(), 1);
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
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(refusal(&body)["detail"]["no_spend"], true);
    assert_eq!(gate.0.load(Ordering::Relaxed), 1);
    assert!(rig.sessions.sent().is_empty());
}

/// Consent holds as the route's would decide them: the held calls it already has, the calls it
/// was asked to decide, and what it decides.
struct FakeConsent {
    held: Mutex<HashMap<String, Answer>>,
    asked: Mutex<Vec<(String, String, String, String)>>,
    releases: bool,
}

impl FakeConsent {
    fn deciding(releases: bool) -> Self {
        Self {
            held: Mutex::default(),
            asked: Mutex::default(),
            releases,
        }
    }
}

#[async_trait]
impl Consent for FakeConsent {
    fn held(&self, batch_id: &str) -> Option<Answer> {
        self.held.lock().unwrap().get(batch_id).cloned()
    }

    async fn consent(
        &self,
        _refused: Vec<u8>,
        _scope: &SendScope,
        call: ConsentCall<'_>,
    ) -> Consented {
        self.asked.lock().unwrap().push((
            call.batch_id.clone(),
            call.provider.to_owned(),
            call.model.clone(),
            call.frozen_row.clone(),
        ));
        if self.releases {
            return Consented::Released;
        }
        let held =
            serde_json::value::to_raw_value(&json!({"held": {"reason": "consent"}})).unwrap();
        self.held
            .lock()
            .unwrap()
            .insert(call.batch_id, Ok(held.clone()));
        Consented::Answered(Ok(held))
    }
}

/// A call the gate refuses for want of the person's consent is handed to consent holds with the
/// route routing chose and its frozen prices, and runs nothing until the person answers; a
/// repeat of its key while it waits answers as held, asking routing nothing. A caller that can't
/// wait gets the gate's refusal. A grant that releases the call runs it at once.
#[tokio::test]
async fn a_call_waiting_for_consent_runs_nothing_and_a_release_runs_it() {
    let rig = rig(
        FakeSelect::answering(Ok(route("anthropic", "claude-sonnet", Some(metered())))),
        FakeSessions::ending(vec![completed("Teal.", Some(1_000), Some(500))]),
    );
    let gate = Refuse(AtomicUsize::new(0));
    let ask = |consent: Option<&'static FakeConsent>, call_key: &'static str| {
        let complete = rig.complete.clone();
        let gate = &gate;
        async move {
            complete
                .answer(
                    Some(&principal()),
                    &flow_scope(),
                    &arguments(text_call()),
                    Some(call_key),
                    gate,
                    consent.map(|consent| consent as &dyn Consent),
                )
                .await
        }
    };
    let body = ask(None, "k0").await.unwrap_err();
    assert_eq!(refusal(&body)["detail"]["no_spend"], true);

    let holds: &'static FakeConsent = Box::leak(Box::new(FakeConsent::deciding(false)));
    let held = ask(Some(holds), "k1").await.unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(held.get()).unwrap()["held"]["reason"],
        "consent"
    );
    let asked = holds.asked.lock().unwrap().clone();
    assert_eq!(asked.len(), 1);
    let (batch_id, provider, model, frozen_row) = &asked[0];
    assert_eq!(batch_id, &held_batch_id(&key_of("k1")));
    assert_eq!(held_key(batch_id), Some(key_of("k1").as_str()));
    assert_eq!(
        (provider.as_str(), model.as_str()),
        ("anthropic", "anthropic/claude-sonnet")
    );
    let frozen: Frozen = serde_json::from_str(frozen_row).unwrap();
    assert_eq!(frozen.decision_id, "decision-1");
    let selects = rig.select.asked.lock().unwrap().len();
    let again = ask(Some(holds), "k1").await.unwrap();
    assert_eq!(again.get(), held.get());
    assert_eq!(
        rig.select.asked.lock().unwrap().len(),
        selects,
        "a held repeat asks nothing"
    );
    assert!(rig.sessions.sent().is_empty());

    let releases: &'static FakeConsent = Box::leak(Box::new(FakeConsent::deciding(true)));
    let answer = ask(Some(releases), "k2").await.unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(answer.get()).unwrap()["text"],
        "Teal."
    );
    assert_eq!(rig.sessions.sent().len(), 1);
}

/// A completion the person approved runs on the route and prices it was held with, without
/// asking routing again, under the scope it is given: here none, as after a restart. Its answer
/// is recorded, so running the approval again answers the record and sends nothing.
#[tokio::test]
async fn an_approved_completion_runs_on_its_held_route() {
    let rig = rig(
        FakeSelect::answering(Ok(route("anthropic", "claude-sonnet", Some(metered())))),
        FakeSessions::ending(vec![completed("Teal.", Some(1_000), Some(500))]),
    );
    let holds = FakeConsent::deciding(false);
    rig.complete
        .answer(
            Some(&principal()),
            &flow_scope(),
            &arguments(text_call()),
            Some("k1"),
            &Refuse(AtomicUsize::new(0)),
            Some(&holds),
        )
        .await
        .unwrap();
    let (batch_id, _, _, frozen_row) = holds.asked.lock().unwrap()[0].clone();
    let selects = rig.select.asked.lock().unwrap().len();
    let approved = |frozen_row: String| Approved {
        batch_id: batch_id.clone(),
        batch_scope: admit(
            Some(&principal()),
            &flow_scope(),
            &arguments(text_call()),
            Some("k1"),
        )
        .unwrap()
        .scope,
        principal: Some(principal()),
        request: serde_json::to_string(&text_call()).unwrap(),
        args_digest: super::super::admission::hash(&text_call()),
        frozen_row,
    };
    assert!(!rig.complete.recorded(&key_of("k1")).await);
    let answer = rig
        .complete
        .approved(approved(frozen_row.clone()), &SendScope::Internal)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(answer.get()).unwrap()["text"],
        "Teal."
    );
    assert_eq!(
        rig.select.asked.lock().unwrap().len(),
        selects,
        "routing isn't asked again"
    );
    let sent = rig.sessions.sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].request.scope, SendScope::Internal);
    assert_eq!(sent[0].request.principal, Some(principal()));
    assert!(rig.complete.recorded(&key_of("k1")).await);

    // Approved on a route other than the one the record froze.
    let mut other: Frozen = serde_json::from_str(&frozen_row).unwrap();
    other.model.provider = "openai".into();
    let refused = rig
        .complete
        .approved(
            approved(serde_json::to_string(&other).unwrap()),
            &SendScope::Internal,
        )
        .await;
    // The record already holds an outcome: it answers that, and runs nothing more.
    assert_eq!(refused.unwrap().get(), answer.get());
    assert_eq!(rig.sessions.sent().len(), 1);
}

/// The record key of a text call from the test caller under `call_key`.
fn key_of(call_key: &str) -> String {
    match admit(
        Some(&principal()),
        &flow_scope(),
        &arguments(text_call()),
        Some(call_key),
    ) {
        Ok(call) => call.key,
        Err(refusal) => panic!("the test call is admitted: {refusal:?}"),
    }
}

/// The completion's send names routing's decision, and the key pool and credential routing
/// chose, so the run is admitted on the account its price was frozen for. A route with no key
/// pool (a subscription account) names the decision alone.
#[tokio::test]
async fn the_send_carries_routings_decision_and_account_choice() {
    let mut pooled = route("openai", "gpt", Some(metered()));
    pooled.runner = pooled.runner.clone().with_auth(RunnerAuth {
        pool: Some("keys".into()),
        credential_id: Some("key-b".into()),
    });
    let rig = rig(
        FakeSelect::answering(Ok(pooled)),
        FakeSessions::ending(vec![
            completed("Teal.", Some(1_000), Some(500)),
            completed("Plum.", Some(1_000), Some(500)),
        ]),
    );
    rig.call(text_call(), Some("k1")).await.unwrap();
    *rig.select.answer.lock().unwrap() = Ok(route("anthropic", "claude-sonnet", Some(metered())));
    rig.call(text_call(), Some("k2")).await.unwrap();
    let sent = rig.sessions.sent();
    assert_eq!(sent.len(), 2);
    let pooled = &sent[0].request;
    assert_eq!(pooled.route_decision_id.as_deref(), Some("decision-1"));
    let auth = pooled.auth.as_ref().expect("the key pool routing chose");
    assert_eq!(
        (
            auth.method.as_str(),
            auth.pool.as_deref(),
            auth.credential_id.as_deref(),
            auth.account.as_deref()
        ),
        ("apikey", Some("keys"), Some("key-b"), None)
    );
    let subscription = &sent[1].request;
    assert_eq!(
        subscription.route_decision_id.as_deref(),
        Some("decision-1")
    );
    assert!(subscription.auth.is_none());
}

/// A run that couldn't be read hasn't been seen to end: the call answers its spend unknown and
/// records nothing, and a repeat of the key reads the same run again rather than answering the
/// read failure or starting another run.
#[tokio::test]
async fn a_failed_read_leaves_the_attempt_for_a_repeat_to_read() {
    let sessions = Arc::new(FakeSessions {
        ends: Mutex::new(vec![completed("Teal.", Some(1_000), Some(500))]),
        read_errors: Mutex::new(1),
        ..Default::default()
    });
    let rig = rig(
        FakeSelect::answering(Ok(route("anthropic", "claude-sonnet", Some(metered())))),
        sessions,
    );
    let refused = refusal(&rig.call(text_call(), Some("k1")).await.unwrap_err());
    assert_eq!(refused["code"], "provider_error");
    assert_eq!(refused["detail"]["cost_unknown"], true);
    assert!(rig
        .store
        .outcomes(STORE_ROLE, &key_of("k1"))
        .unwrap()
        .is_empty());
    let answer = rig.call(text_call(), Some("k1")).await.unwrap();
    assert!(answer.get().contains("Teal."));
    assert_eq!(rig.sessions.sent().len(), 1, "one run, read twice");
    assert_eq!(rig.select.asked.lock().unwrap().len(), 1);
}

/// A run broca cancelled that never stops is waited for only so long. The call then answers its
/// spend unknown and records nothing, so a repeat of the key reads the same run again.
#[tokio::test]
async fn a_cancelled_run_that_never_stops_is_not_waited_on_forever() {
    let paused = json!({"state": "paused", "reason": "account_limit",
        "usage": {"steps": 0, "unreported_steps": 0}});
    let sessions = Arc::new(FakeSessions {
        ends: Mutex::new(vec![paused]),
        ignore_cancel: true,
        ..Default::default()
    });
    let rig = rig(
        FakeSelect::answering(Ok(route("anthropic", "claude-sonnet", Some(metered())))),
        sessions,
    );
    let answer = tokio::time::timeout(Duration::from_secs(30), rig.call(text_call(), Some("k1")))
        .await
        .expect("the call answers once its cancel has had its time");
    let refused = refusal(&answer.unwrap_err());
    assert_eq!(refused["code"], "provider_error");
    assert_eq!(refused["detail"]["cost_unknown"], true);
    assert_eq!(rig.sessions.cancelled.lock().unwrap().len(), 1);
    assert!(rig
        .store
        .outcomes(STORE_ROLE, &key_of("k1"))
        .unwrap()
        .is_empty());
}

/// Another process sharing the store can run and record the call between this one's lookup and
/// its claim. The claim's record is the one that counts: its answer is answered as recorded, and
/// this process neither runs the call again nor adds its cost to the record a second time.
#[tokio::test]
async fn a_call_recorded_after_the_lookup_is_answered_from_the_claimed_record() {
    let other = rig(
        FakeSelect::answering(Ok(route("anthropic", "claude-sonnet", Some(metered())))),
        FakeSessions::ending(vec![completed("Teal.", Some(1_000), Some(500))]),
    );
    let this = rig_on(
        other.store.clone(),
        "this-owner",
        FakeSelect::answering(Ok(route("anthropic", "claude-sonnet", Some(metered())))),
        FakeSessions::ending(vec![completed("Plum.", Some(1_000), Some(500))]),
    );
    let first = other.complete.clone();
    let (recorded, answered) = tokio::sync::oneshot::channel();
    *this.select.before.lock().unwrap() = Some(Box::pin(async move {
        let answer = first
            .answer(
                Some(&principal()),
                &flow_scope(),
                &arguments(text_call()),
                Some("k1"),
                &Allow,
                None,
            )
            .await;
        let _ = recorded.send(answer);
    }));
    let answer = this.call(text_call(), Some("k1")).await.unwrap();
    let recorded = answered.await.unwrap().unwrap();
    assert_eq!(answer.get(), recorded.get());
    assert!(this.sessions.sent().is_empty());
    // The $0.50 ceiling, and one run's $0.0105.
    assert_eq!(
        this.store.ledger(STORE_ROLE, &key_of("k1")).unwrap(),
        Some((Some(500_000_000), 10_500_000))
    );
}

/// A repeat of a running call's key with other arguments is refused, spending nothing, rather
/// than joining the running call and answering its answer and cost.
#[tokio::test]
async fn a_concurrent_repeat_with_other_arguments_is_refused() {
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
        tokio::task::yield_now().await;
        let mut other = text_call();
        other["prompt"] = json!("Name a fruit.");
        let second = rig.call(other, Some("k1"));
        gate.notify_one();
        second.await
    });
    assert!(first.unwrap().get().contains("Teal."));
    let refused = refusal(&second.unwrap_err());
    assert_eq!(refused["code"], "invalid_params");
    assert_eq!(refused["detail"]["field"], "call_key");
    assert_eq!(refused["detail"]["no_spend"], true);
    assert_eq!(rig.sessions.sent().len(), 1);
}

/// A call whose task panicked leaves its key free: a later repeat runs, rather than joining the
/// call that panicked and answering that it stopped without an answer.
#[tokio::test]
async fn a_call_that_panicked_does_not_hold_its_key() {
    let rig = rig(
        FakeSelect::answering(Ok(route("anthropic", "claude-sonnet", Some(metered())))),
        FakeSessions::ending(vec![completed("Teal.", Some(1_000), Some(500))]),
    );
    rig.select.panic_next.store(true, Ordering::SeqCst);
    let complete = rig.complete.clone();
    let panicked = tokio::spawn(async move {
        complete
            .answer(
                Some(&principal()),
                &flow_scope(),
                &arguments(text_call()),
                Some("k1"),
                &Allow,
                None,
            )
            .await
    })
    .await;
    assert!(panicked.unwrap_err().is_panic());
    let answer = rig.call(text_call(), Some("k1")).await.unwrap();
    assert!(answer.get().contains("Teal."));
}

/// A record past its seven days is not replayed, even before the sweep removes it: the repeat
/// is refused as transient, spending nothing. Once the sweep has removed it, the key starts a
/// new call in a new session with a new run, never the old week's.
#[tokio::test]
async fn a_key_past_its_week_is_not_replayed_and_then_starts_a_new_call() {
    let rig = rig(
        FakeSelect::answering(Ok(route("anthropic", "claude-sonnet", Some(metered())))),
        FakeSessions::ending(vec![
            completed("Teal.", Some(1_000), Some(500)),
            completed("Plum.", Some(1_000), Some(500)),
        ]),
    );
    let first = rig.call(text_call(), Some("k1")).await.unwrap();
    assert!(first.get().contains("Teal."));
    rig.now
        .fetch_add(broca_store::system_one::RETENTION_MS, Ordering::SeqCst);
    let refused = refusal(&rig.call(text_call(), Some("k1")).await.unwrap_err());
    assert_eq!(refused["code"], "provider_error");
    assert_eq!(refused["detail"]["class"], "transient");
    assert_eq!(refused["detail"]["no_spend"], true);
    assert_eq!(rig.sessions.sent().len(), 1);
    assert_eq!(rig.select.asked.lock().unwrap().len(), 1);
    let swept = rig.store.sweep(rig.now.load(Ordering::SeqCst)).unwrap();
    assert_eq!(swept.batches, 1);
    let again = rig.call(text_call(), Some("k1")).await.unwrap();
    assert!(again.get().contains("Plum."));
    let sent = rig.sessions.sent();
    assert_eq!(sent.len(), 2);
    assert_ne!(sent[0].session, sent[1].session);
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
