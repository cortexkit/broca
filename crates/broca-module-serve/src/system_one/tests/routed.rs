use super::*;
use crate::account_routing::{Candidate, Candidates, RoutingClient, RoutingTransport};
use crate::system_one::credential::CredentialSource;
use crate::system_one::{Routing, RoutingCredentials};
use cortexkit_role_routing::{
    describe::{ConsumerEntry, RoleDescribe},
    select::{PinOutcome, RouteRef, RunnerAuth, RunnerRef, SelectAnswer, SelectRequest},
};
use std::sync::atomic::AtomicBool;

struct Key(String);
#[async_trait]
impl CredentialSource for Key {
    fn credential_id(&self) -> Option<String> {
        Some(self.0.clone())
    }
    async fn bearer(&self) -> Result<Option<String>, String> {
        panic!("stand-in must not read a secret")
    }
}
struct Keys;
#[async_trait]
impl RoutingCredentials for Keys {
    async fn candidates(&self, _provider: &str) -> Result<Candidates, String> {
        Ok(Candidates {
            view_digest: "view".into(),
            accounts: vec![
                Candidate {
                    account_id: "a".into(),
                    credential_id: "apikey:a".into(),
                },
                Candidate {
                    account_id: "b".into(),
                    credential_id: "apikey:b".into(),
                },
            ],
        })
    }
    fn source(&self, credential_id: &str) -> Arc<dyn CredentialSource> {
        Arc::new(Key(credential_id.into()))
    }
}

struct Router {
    override_refusal: Mutex<Option<(&'static str, &'static str)>>,
    lose_granted_renewal_replies: AtomicUsize,
    lose_first_admission_before_grant: AtomicBool,
    invalid_reselection_account: AtomicBool,
    renewal_reply_gate: Mutex<Option<Arc<Semaphore>>>,
    reselect_unreachable: AtomicBool,
    reselection_refusal: Mutex<Option<&'static str>>,
    check_expired_content: Mutex<Option<Arc<SystemOneStore>>>,
    admissions: Mutex<HashMap<String, Value>>,
    active_admission: Mutex<Option<String>>,
    pin_held: AtomicBool,
    panic_on_admission: AtomicBool,
    invalid_account: AtomicBool,
    override_answer: Mutex<Option<(&'static str, Value)>>,
    observe_failures: AtomicUsize,
    panic_on_attempt: AtomicBool,
    fail_attempts: AtomicBool,
    check_pending: Mutex<Option<Arc<SystemOneStore>>>,
    pending_seen: AtomicBool,
    calls: Mutex<Vec<Value>>,
    describe: Mutex<RoleDescribe>,
    models: Mutex<Vec<String>>,
    selects: AtomicUsize,
    refused: Mutex<Option<(&'static str, &'static str)>>,
    admission_refused: Mutex<Option<(&'static str, &'static str)>>,
    report_refused: Mutex<Option<&'static str>>,
    refuse_reselection: AtomicBool,
    panic_after_initial_report: AtomicBool,
    unreachable: AtomicBool,
    lose_ack: AtomicBool,
    reports: Mutex<HashMap<String, Value>>,
    lease_ms: AtomicUsize,
    renew_refused: AtomicBool,
    renew_transport_failures: AtomicUsize,
    renew_times: Mutex<Vec<u64>>,
    renewed: Notify,
    started: tokio::time::Instant,
}

impl Router {
    fn new(run_reports: bool, fields: bool) -> Arc<Self> {
        let mut describe = RoleDescribe::new(
            vec![ConsumerEntry {
                id: "classifier".into(),
                priced_only: true,
            }],
            false,
        )
        .with_capability("report_outcomes")
        .with_capability("observe_backoff")
        .with_capability("observe_credential_refused");
        if run_reports {
            describe = describe.with_capability("report_usage_run_id");
        }
        if fields {
            describe.requirement_fields.extend([
                "questions".into(),
                "input_tokens".into(),
                "max_cost_usd".into(),
            ]);
        }
        Arc::new(Self {
            override_refusal: Mutex::new(None),
            lose_granted_renewal_replies: AtomicUsize::new(0),
            lose_first_admission_before_grant: AtomicBool::new(false),
            invalid_reselection_account: AtomicBool::new(false),
            renewal_reply_gate: Mutex::new(None),
            reselect_unreachable: AtomicBool::new(false),
            reselection_refusal: Mutex::new(None),
            check_expired_content: Mutex::new(None),
            admissions: Mutex::new(HashMap::new()),
            active_admission: Mutex::new(None),
            pin_held: AtomicBool::new(false),
            panic_on_admission: AtomicBool::new(false),
            invalid_account: AtomicBool::new(false),
            override_answer: Mutex::new(None),
            observe_failures: AtomicUsize::new(0),
            panic_on_attempt: AtomicBool::new(false),
            fail_attempts: AtomicBool::new(false),
            check_pending: Mutex::new(None),
            pending_seen: AtomicBool::new(false),
            calls: Mutex::new(vec![]),
            describe: Mutex::new(describe),
            models: Mutex::new(vec![JEV.into()]),
            selects: AtomicUsize::new(0),
            refused: Mutex::new(None),
            admission_refused: Mutex::new(None),
            report_refused: Mutex::new(None),
            refuse_reselection: AtomicBool::new(false),
            panic_after_initial_report: AtomicBool::new(false),
            unreachable: AtomicBool::new(false),
            lose_ack: AtomicBool::new(false),
            reports: Mutex::new(HashMap::new()),
            lease_ms: AtomicUsize::new(600_000),
            renew_refused: AtomicBool::new(false),
            renew_transport_failures: AtomicUsize::new(0),
            renew_times: Mutex::new(vec![]),
            renewed: Notify::new(),
            started: tokio::time::Instant::now(),
        })
    }
    fn now(&self) -> u64 {
        1_000_000 + self.started.elapsed().as_millis() as u64
    }
    fn bodies(&self, method: &str) -> Vec<Value> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|body| body["method"] == method)
            .map(|body| body["params"].clone())
            .collect()
    }
    fn clear(&self) {
        self.calls.lock().unwrap().clear();
    }

    async fn finish_admission_reply(&self, renewing: bool) -> Result<(), String> {
        if renewing {
            let gate = self.renewal_reply_gate.lock().unwrap().clone();
            if let Some(gate) = gate {
                let _permit = gate.acquire().await.unwrap();
            }
            if self
                .lose_granted_renewal_replies
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err("renewal was granted but its reply was lost".into());
            }
        }
        Ok(())
    }
}

#[async_trait]
impl RoutingTransport for Router {
    async fn request(&self, body: Vec<u8>) -> Result<Result<Vec<u8>, Vec<u8>>, String> {
        let body: Value = serde_json::from_slice(&body).unwrap();
        self.calls.lock().unwrap().push(body.clone());
        if self.unreachable.load(Ordering::SeqCst) {
            return Err("route closed".into());
        }
        if let Some((method, code)) = *self.override_refusal.lock().unwrap() {
            if body["method"] == method {
                return Ok(Err(serde_json::to_vec(
                    &json!({"code":code,"message":"scripted refusal"}),
                )
                .unwrap()));
            }
        }
        let params = &body["params"];
        if let Some((method, answer)) = &*self.override_answer.lock().unwrap() {
            if body["method"] == *method {
                return Ok(Ok(serde_json::to_vec(&json!({"result":answer})).unwrap()));
            }
        }
        let refused = |code, message| {
            Ok(Err(serde_json::to_vec(
                &json!({"code":code,"message":message}),
            )
            .unwrap()))
        };
        let result = match body["method"].as_str().unwrap() {
            "role.describe" => serde_json::to_value(self.describe.lock().unwrap().clone()).unwrap(),
            "route.select" => {
                if self.selects.load(Ordering::SeqCst) > 0 {
                    if let Some(code) = *self.reselection_refusal.lock().unwrap() {
                        return refused(code, "replacement select refused");
                    }
                }
                if self.selects.load(Ordering::SeqCst) > 0
                    && self.reselect_unreachable.load(Ordering::SeqCst)
                {
                    return Err("replacement selection route closed".into());
                }
                if let Some((code, message)) = *self.refused.lock().unwrap() {
                    return refused(code, message);
                }
                let request: SelectRequest = serde_json::from_value(params.clone()).unwrap();
                let count = self.selects.fetch_add(1, Ordering::SeqCst);
                let model = {
                    let models = self.models.lock().unwrap();
                    models[count.min(models.len() - 1)].clone()
                };
                let (provider, model) = model.split_once('/').unwrap();
                let route = RouteRef::new(provider, model);
                let mut answer = SelectAnswer::new(
                    format!("decision-{count}"),
                    route.clone(),
                    RunnerRef::new("broca", route).with_auth(RunnerAuth {
                        pool: Some("keys".into()),
                        credential_id: None,
                    }),
                    1_000_000,
                    1_000,
                    self.now() + 600_000,
                    "stand-in decision",
                );
                if request.pin.is_some() {
                    answer = answer.with_pin_outcome(PinOutcome::Pinned);
                }
                cortexkit_role_routing::select::check_answer(&request, &answer).unwrap();
                serde_json::to_value(answer).unwrap()
            }
            "route.account_for" => {
                if params["mode"] != "renew"
                    && self
                        .lose_first_admission_before_grant
                        .swap(false, Ordering::SeqCst)
                {
                    return Err(
                        "first admission is still pending remotely; its answer was lost".into(),
                    );
                }
                if let Some(store) = &*self.check_pending.lock().unwrap() {
                    let row = store.routing(STORE_ROLE, "routed").unwrap().unwrap();
                    let journal: Value = serde_json::from_str(&row.runs).unwrap();
                    self.pending_seen.store(
                        journal["pending_admission"]["id"] == params["admission_id"],
                        Ordering::SeqCst,
                    );
                }
                cortexkit_role_routing::account::parse_account_for_request(params).unwrap();
                let admission_id = params["admission_id"].as_str().unwrap();
                let replay = self.admissions.lock().unwrap().get(admission_id).cloned();
                if let Some(answer) = replay {
                    self.finish_admission_reply(params["mode"] == "renew")
                        .await?;
                    return Ok(Ok(serde_json::to_vec(&json!({"result": answer})).unwrap()));
                }
                let renewing = params["mode"] == "renew";
                if !renewing {
                    if let Some((code, message)) = *self.admission_refused.lock().unwrap() {
                        return refused(code, message);
                    }
                }
                if renewing {
                    self.renew_times.lock().unwrap().push(self.now());
                    self.renewed.notify_waiters();
                }
                if renewing && self.renew_refused.load(Ordering::SeqCst) {
                    return refused("pin_generation_conflict", "pin changed");
                }
                if renewing
                    && self
                        .renew_transport_failures
                        .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                        .is_ok()
                {
                    return Err("lost renewal acknowledgement".into());
                }
                let decision = params["decision_id"].as_str().unwrap();
                if !renewing
                    && decision != "decision-0"
                    && self.refuse_reselection.load(Ordering::SeqCst)
                {
                    return refused(
                        "account_not_authorized",
                        "replacement credential is not authorized",
                    );
                }
                let credential = params["requested_account"]["credential_id"]
                    .as_str()
                    .unwrap_or(if decision == "decision-0" {
                        "apikey:a"
                    } else {
                        "apikey:b"
                    });
                let account = credential.strip_prefix("apikey:").unwrap();
                let generation = params["expected_pin"]["generation"]
                    .as_u64()
                    .map_or(1, |generation| generation + 1);
                let mut answer = json!({"admission":{"id":params["admission_id"],"state":"admitted"},
                    "account":{"provider":params["provider"],"method":"apikey","account_id":account,"credential_id":credential},
                    "pin":{"id":"pin","generation":generation,"affinity_expires_at_ms":self.now()+3_600_000,"admission_valid_until_ms":self.now()+60_000,"lease_valid_until_ms":self.now()+self.lease_ms.load(Ordering::SeqCst) as u64,"mode":if renewing {"renew"} else {"admit"}},
                    "reason":{"code":if renewing {"renewed"} else {"headroom"},"decision_id":decision,"policy_version":"test","quota_age_band":"fresh"}});
                if self.invalid_account.load(Ordering::SeqCst)
                    || (self.selects.load(Ordering::SeqCst) > 1
                        && self.invalid_reselection_account.load(Ordering::SeqCst))
                {
                    answer["account"]["credential_id"] = json!("apikey:unoffered");
                }
                self.admissions
                    .lock()
                    .unwrap()
                    .insert(admission_id.into(), answer.clone());
                *self.active_admission.lock().unwrap() = Some(admission_id.into());
                self.pin_held.store(true, Ordering::SeqCst);
                if self.panic_on_admission.swap(false, Ordering::SeqCst) {
                    panic!("crash after admission grant before freeze");
                }
                self.finish_admission_reply(renewing).await?;
                answer
            }
            "route.account_observe" => {
                if params["kind"] == "attempt"
                    && self.panic_on_attempt.swap(false, Ordering::SeqCst)
                {
                    panic!("crash at post-provider observation delivery");
                }
                if params["kind"] == "attempt" && self.fail_attempts.load(Ordering::SeqCst) {
                    return Err("attempt observation unavailable".into());
                }
                if self
                    .observe_failures
                    .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                    .is_ok()
                {
                    return Err("observation delivery unavailable".into());
                }
                cortexkit_role_routing::observe::parse_account_observe_request_serving(
                    params,
                    &self.describe.lock().unwrap().capabilities,
                )
                .unwrap();
                if params["kind"] == "run_finished"
                    && params["admission_id"].as_str()
                        == self.active_admission.lock().unwrap().as_deref()
                {
                    self.pin_held.store(false, Ordering::SeqCst);
                }
                json!({"observation":{"id":params["event_id"],"duplicate":false,"bound":true,"effects":[]}})
            }
            "route.report" => {
                if let Some(store) = &*self.check_expired_content.lock().unwrap() {
                    let row = store.lookup(STORE_ROLE, "routed").unwrap().unwrap();
                    assert!(
                        row.outcomes.is_empty() && row.identity == "{}",
                        "expired item content must be scrubbed before waiting on routing"
                    );
                }
                cortexkit_role_routing::report::parse_report_request_serving(
                    params,
                    &self.describe.lock().unwrap().capabilities,
                )
                .unwrap();
                if let Some(code) = *self.report_refused.lock().unwrap() {
                    return refused(code, "report rejected");
                }
                let key = format!("{}:{}", params["decision_id"], params["usage"]["run_id"]);
                let mut reports = self.reports.lock().unwrap();
                if let Some(previous) = reports.get(&key) {
                    assert_eq!(
                        previous, params,
                        "report identity must replay identical usage"
                    );
                } else {
                    reports.insert(key, params.clone());
                }
                drop(reports);
                if self
                    .panic_after_initial_report
                    .swap(false, Ordering::SeqCst)
                {
                    panic!(
                        "stop after initial account failure report, before reselection allowance"
                    );
                }
                if self.lose_ack.swap(false, Ordering::SeqCst) {
                    return Err("lost report acknowledgement".into());
                }
                json!({"decision_id":params["decision_id"],"report_id":"report","outcome":params["outcome"]})
            }
            method => panic!("unexpected routing method {method}"),
        };
        Ok(Ok(serde_json::to_vec(&json!({"result":result})).unwrap()))
    }
}

struct Bound {
    stand_in: Arc<StandIn>,
    store: Arc<SystemOneStore>,
    credentials: Arc<Mutex<Vec<String>>>,
    credential: String,
}
#[async_trait]
impl ClassifierProvider for Bound {
    fn egress_host(&self) -> String {
        "stand-in.invalid".into()
    }
    fn credential_id(&self) -> Option<String> {
        Some(self.credential.clone())
    }
    fn with_credential(
        &self,
        credential: Arc<dyn CredentialSource>,
    ) -> Option<Arc<dyn ClassifierProvider>> {
        Some(Arc::new(Self {
            stand_in: self.stand_in.clone(),
            store: self.store.clone(),
            credentials: self.credentials.clone(),
            credential: credential.credential_id().unwrap(),
        }))
    }
    async fn call(&self, body: String) -> ProviderReply {
        let routing = self
            .store
            .routing(STORE_ROLE, "routed")
            .unwrap()
            .expect("choice stored before provider request");
        let choice: Value = serde_json::from_str(&routing.choice).unwrap();
        assert_eq!(
            choice["account"]["account"]["credential_id"],
            self.credential
        );
        assert!(choice["select"]["decision_id"].as_str().is_some());
        let journal: Value = serde_json::from_str(&routing.runs).unwrap();
        assert!(journal["runs"].as_array().unwrap().last().unwrap()["id"]
            .as_str()
            .is_some());
        self.credentials
            .lock()
            .unwrap()
            .push(self.credential.clone());
        self.stand_in.call(body).await
    }
}

struct FrozenCatalog {
    rows: Rows,
    forbidden: AtomicBool,
}
impl BatchCatalog for FrozenCatalog {
    fn row(&self, model: &str) -> Option<ModelBatchRow> {
        assert!(
            !self.forbidden.load(Ordering::SeqCst),
            "stored batches must not look up catalog facts"
        );
        self.rows.row(model)
    }
}
struct RoutedRig {
    service: Arc<SystemOne>,
    provider: Arc<StandIn>,
    router: Arc<Router>,
    store: Arc<SystemOneStore>,
    credentials: Arc<Mutex<Vec<String>>>,
    catalog: Arc<FrozenCatalog>,
}

fn routed_rig(run_reports: bool, fields: bool) -> RoutedRig {
    let store = Arc::new(SystemOneStore::open_in_memory().unwrap());
    routed_rig_with_store(store, Router::new(run_reports, fields))
}
fn routed_rig_with_store(store: Arc<SystemOneStore>, router: Arc<Router>) -> RoutedRig {
    let provider = Arc::new(StandIn::default());
    let credentials = Arc::new(Mutex::new(vec![]));
    let bound = Arc::new(Bound {
        stand_in: provider.clone(),
        store: store.clone(),
        credentials: credentials.clone(),
        credential: "fixed-unused".into(),
    });
    let clock = router.clone();
    let config = Config {
        routed_models: true,
        max_attempts: 1,
        ..Default::default()
    };
    let mut rows = catalog().0.clone();
    rows.get_mut(CLEF).unwrap().attachment = true;
    let catalog = Arc::new(FrozenCatalog {
        rows: Rows(rows),
        forbidden: AtomicBool::new(false),
    });
    let service = SystemOne::new(
        store.clone(),
        "routed-process",
        catalog.clone(),
        vec![ModelPin::jev(bound.clone()), ModelPin::clef(bound)],
        config,
        Arc::new(AtomicUsize::new(0)),
    )
    .with_clock(Arc::new(move || clock.now() as i64))
    .with_routing(Routing {
        client: Arc::new(RoutingClient::new(router.clone(), Default::default())),
        credentials: Arc::new(Keys),
        module_id: "broca".into(),
    });
    RoutedRig {
        service: Arc::new(service),
        provider,
        router,
        store,
        credentials,
        catalog,
    }
}
fn routed_request(states: &[&str]) -> String {
    json!({"batch_id":"routed","questions":{"q":{"type":"noul","instructions":"true?"}},"items":states.iter().map(|state| json!({"state":state})).collect::<Vec<_>>(),"max_cost_usd":1.0}).to_string()
}
fn answered(tokens: Option<u64>) -> String {
    let mut body = json!({"model":"jev-1.13.0","answers":{"q":{"type":"noul","noul":0.5}}});
    if let Some(tokens) = tokens {
        body["usage"] = json!({"input_tokens":tokens,"output_tokens":0});
    }
    body.to_string()
}
async fn call(rig: &RoutedRig, params: &str) -> Result<Box<serde_json::value::RawValue>, Value> {
    rig.service
        .clone()
        .spawn_call(params.into(), test_scope())
        .await
        .map_err(|body| serde_json::from_slice(&body).unwrap())
}

async fn wait_for(label: &'static str, ready: impl Fn() -> bool) {
    broca_test_support::event_timeout(
        label,
        async {
            while !ready() {
                tokio::task::yield_now().await;
            }
        },
        || label.into(),
    )
    .await
    .unwrap();
}

const CLEF: &str = "cloudflare-workers-ai/@cf/cloudflare/clef";

#[tokio::test]
async fn audit_paid_answer_is_committed_before_observation_crash() {
    for crash in [false, true] {
        let root = broca_test_support::scratch_root().join(format!(
            "audit-paid-{crash}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("system-one.db");
        let router = Router::new(true, true);
        let rig = routed_rig_with_store(
            Arc::new(SystemOneStore::open(&path).unwrap()),
            router.clone(),
        );
        rig.provider
            .script("paid", vec![Script::Answer(answered(Some(7)))]);
        router.panic_on_attempt.store(crash, Ordering::SeqCst);
        router.fail_attempts.store(!crash, Ordering::SeqCst);
        let first = call(&rig, &routed_request(&["paid"])).await;
        if !crash {
            assert!(
                first.is_ok(),
                "routing delivery failure must not delay the answer"
            );
        }
        let stored = rig.store.lookup(STORE_ROLE, "routed").unwrap().unwrap();
        assert!(
            matches!(
                stored.outcomes.first().map(|item| &item.outcome),
                Some(broca_store::system_one::Outcome::Answered(_))
            ),
            "paid outcome must precede routing delivery"
        );
        assert_eq!(rig.provider.calls("paid"), 1);
        drop(rig);
        router.fail_attempts.store(false, Ordering::SeqCst);
        let store = Arc::new(SystemOneStore::open(&path).unwrap());
        store.release_abandoned().unwrap();
        let resumed = routed_rig_with_store(store, router.clone());
        let replay = call(&resumed, &routed_request(&["paid"])).await.unwrap();
        assert_eq!(resumed.provider.calls("paid"), 0);
        if let Ok(first) = first {
            assert_eq!(replay.get(), first.get());
        }
        resumed.service.recover_routing().await.unwrap();
        let row = resumed
            .store
            .routing(STORE_ROLE, "routed")
            .unwrap()
            .unwrap();
        assert!(!row.pending, "owed observation must drain after restart");
        assert!(router
            .bodies("route.account_observe")
            .iter()
            .any(|body| body["kind"] == "attempt"));
    }
}

#[tokio::test]
async fn audit_admission_intent_is_durable_before_rpc() {
    let rig = routed_rig(true, true);
    *rig.router.check_pending.lock().unwrap() = Some(rig.store.clone());
    rig.provider
        .script("x", vec![Script::Answer(answered(Some(7)))]);
    call(&rig, &routed_request(&["x"])).await.unwrap();
    assert!(rig.router.pending_seen.load(Ordering::SeqCst));
}

#[tokio::test]
async fn audit_local_credential_failure_is_transient_not_account_refusal() {
    let rig = routed_rig(true, true);
    rig.provider.script("x", vec![Script::Credential]);
    let reply: Value =
        serde_json::from_str(call(&rig, &routed_request(&["x"])).await.unwrap().get()).unwrap();
    assert_eq!(reply["items"][0]["error"]["code"], "provider_error");
    assert_eq!(rig.router.selects.load(Ordering::SeqCst), 1);
    assert!(rig
        .router
        .bodies("route.account_observe")
        .iter()
        .all(|body| body["kind"] != "credential_refused"));
    assert!(rig
        .router
        .bodies("route.report")
        .iter()
        .all(|body| body.get("refusal_class").is_none()));
}

#[tokio::test]
async fn audit_readmission_preserves_answered_and_contract_refusals() {
    for code in [
        "store_error",
        "internal_error",
        "account_selection_unavailable",
        "no_eligible_account",
        "unknown_decision",
        "account_hold",
        "account_quota_unknown",
        "account_pool_exhausted",
        "account_pool_reserve_limited",
        "account_selection_disabled",
        "invalid_request",
        "admission_conflict",
    ] {
        let rig = routed_rig(true, true);
        rig.provider
            .script("first", vec![Script::Answer(answered(Some(7)))]);
        rig.provider
            .script("second", vec![Script::Status(503, None)]);
        let params = routed_request(&["first", "second"]);
        call(&rig, &params).await.unwrap();
        let before = rig.store.routing(STORE_ROLE, "routed").unwrap().unwrap();
        *rig.router.admission_refused.lock().unwrap() = Some((code, "router refusal"));
        let error = call(&rig, &params).await.unwrap_err();
        assert_eq!(
            error["code"],
            if matches!(code, "invalid_request" | "admission_conflict") {
                "internal_error"
            } else {
                "routing_refused"
            },
            "{code}"
        );
        assert_eq!(error["detail"]["class"], "permanent");
        if !matches!(code, "invalid_request" | "admission_conflict") {
            assert_eq!(error["detail"]["routing_code"], code);
            assert_eq!(error["detail"]["routing_reason"], "router refusal");
        }
        let after = rig.store.routing(STORE_ROLE, "routed").unwrap().unwrap();
        assert_eq!(before.choice, after.choice);
        assert_eq!(
            rig.store
                .lookup(STORE_ROLE, "routed")
                .unwrap()
                .unwrap()
                .outcomes
                .len(),
            2
        );
    }
}

#[tokio::test]
async fn audit_invalid_routing_answer_is_permanent_internal_error() {
    let rig = routed_rig(true, true);
    *rig.router.override_answer.lock().unwrap() = Some(("route.select", json!({"broken":true})));
    let error = call(&rig, &routed_request(&["x"])).await.unwrap_err();
    assert_eq!(error["code"], "internal_error");
    assert_eq!(error["detail"]["class"], "permanent");
}

#[tokio::test]
async fn audit_unservable_reselection_preserves_original_decision() {
    let rig = routed_rig(true, true);
    *rig.router.models.lock().unwrap() = vec![JEV.into(), "openai/gated".into()];
    rig.provider.script("x", vec![Script::AccountError]);
    let params = routed_request(&["x"]);
    let reply: Value = serde_json::from_str(call(&rig, &params).await.unwrap().get()).unwrap();
    assert_eq!(reply["decision_id"], "decision-0");
    assert_eq!(reply["model"], JEV);
    assert_eq!(
        rig.router.bodies("route.report").last().unwrap()["outcome"],
        "launch_failed"
    );
    rig.router.clear();
    call(&rig, &params).await.unwrap();
    assert!(rig.router.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn audit_outbox_stops_at_first_failed_observation() {
    let rig = routed_rig(true, true);
    rig.router.observe_failures.store(100, Ordering::SeqCst);
    rig.provider
        .script("x", vec![Script::Answer(answered(Some(7)))]);
    call(&rig, &routed_request(&["x"])).await.unwrap();
    assert!(rig
        .router
        .bodies("route.account_observe")
        .iter()
        .all(|body| body["kind"] == "admission_started"));
    assert!(rig.router.bodies("route.report").is_empty());
}

#[tokio::test]
async fn audit_report_basis_is_frozen_for_decision() {
    let rig = routed_rig(false, true);
    rig.provider
        .script("a", vec![Script::Answer(answered(Some(7)))]);
    rig.provider.script(
        "b",
        vec![Script::Status(503, None), Script::Answer(answered(Some(9)))],
    );
    let params = routed_request(&["a", "b"]);
    call(&rig, &params).await.unwrap();
    rig.router
        .describe
        .lock()
        .unwrap()
        .capabilities
        .push("report_usage_run_id".into());
    call(&rig, &params).await.unwrap();
    let reports = rig.router.bodies("route.report");
    assert_eq!(reports.len(), 1);
    assert!(reports[0]["usage"].get("run_id").is_none());
}

#[test]
fn audit_classifier_keys_require_explicit_provider_binding() {
    let rows: broca_subc::vault::ScopedList = serde_json::from_value(json!({
        "view":"view", "grants":0,"grant_tuples":[],"credentials": [
            {"id":"unknown-key","state":"active","record_version":1,"categories":[],"type":"apikey","serves":[],"operations":["read"],"auth_method":"apikey","provider_ids":[]},
            {"id":"wrong-key","state":"active","record_version":1,"categories":[],"type":"apikey","serves":[],"operations":["read"],"auth_method":"apikey","provider_ids":["openai"]},
            {"id":"right-key","state":"active","record_version":1,"categories":[],"type":"apikey","serves":[],"operations":["read"],"auth_method":"apikey","provider_ids":["typesafe"]}
        ]})).unwrap();
    let candidates = super::super::routed::classifier_candidates(&rows, "typesafe");
    assert_eq!(
        candidates
            .accounts
            .iter()
            .map(|candidate| candidate.credential_id.as_str())
            .collect::<Vec<_>>(),
        ["right-key"]
    );
}

#[test]
fn audit_routing_recovery_is_started_outside_retention_gate() {
    let main = include_str!("../../main.rs");
    let start = main
        .find("system_one.spawn_routing_recovery(")
        .expect("routing recovery must be started explicitly on every root");
    let gate = main
        .find("state_format.start(BackgroundTask::RetentionSweeper")
        .unwrap();
    assert!(
        start < gate,
        "routing recovery must not be inside legacy-only retention"
    );
}

#[tokio::test(start_paused = true)]
async fn audit_pending_reports_do_not_extend_item_content_retention() {
    let mut rig = routed_rig(true, true);
    rig.provider
        .script("private-content", vec![Script::Answer(answered(Some(7)))]);
    *rig.router.report_refused.lock().unwrap() = Some("store_error");
    call(&rig, &routed_request(&["private-content"]))
        .await
        .unwrap();
    tokio::time::advance(std::time::Duration::from_millis(
        broca_store::system_one::RETENTION_MS as u64 + 1,
    ))
    .await;
    rig.service.recover_routing().await.unwrap();
    let stored = rig.store.lookup(STORE_ROLE, "routed").unwrap();
    assert!(stored.as_ref().is_none_or(
        |batch| !batch.identity.contains("private-content") && batch.outcomes.is_empty()
    ));
    assert!(
        rig.store.routing(STORE_ROLE, "routed").unwrap().is_some(),
        "unacknowledged report must remain"
    );
    Arc::get_mut(&mut rig.service).unwrap().config.routed_models = false;
    let refusal = call(&rig, &exits::pinned(&["private-content"]))
        .await
        .unwrap_err();
    assert_eq!(refusal["code"], "routing_unavailable");
    assert!(refusal["message"].as_str().unwrap().contains("expired"));
    let archive = rig.store.routing(STORE_ROLE, "routed").unwrap().unwrap();
    assert_eq!(archive.choice, "null");
    assert!(!archive.runs.contains("private-content"));
    *rig.router.report_refused.lock().unwrap() = None;
    rig.service.recover_routing().await.unwrap();
    assert!(rig.store.lookup(STORE_ROLE, "routed").unwrap().is_none());
}

#[tokio::test(start_paused = true)]
async fn audit_retention_scrubs_without_a_routing_connection() {
    let mut rig = routed_rig(true, true);
    rig.provider
        .script("private-content", vec![Script::Answer(answered(Some(7)))]);
    *rig.router.report_refused.lock().unwrap() = Some("store_error");
    call(&rig, &routed_request(&["private-content"]))
        .await
        .unwrap();
    let routing = Arc::get_mut(&mut rig.service).unwrap().routing.take();
    tokio::time::advance(std::time::Duration::from_millis(
        broca_store::system_one::RETENTION_MS as u64 + 1,
    ))
    .await;
    rig.service.recover_routing().await.unwrap();
    let archived = rig.store.lookup(STORE_ROLE, "routed").unwrap().unwrap();
    assert_eq!(archived.identity, "{}");
    assert!(archived.outcomes.is_empty());
    assert!(archived.scope.is_none());
    Arc::get_mut(&mut rig.service).unwrap().routing = routing;
    *rig.router.report_refused.lock().unwrap() = None;
    rig.service.recover_routing().await.unwrap();
    assert!(rig.store.lookup(STORE_ROLE, "routed").unwrap().is_none());
}

#[tokio::test(start_paused = true)]
async fn audit_expiry_scrubs_before_any_network_delivery() {
    let rig = routed_rig(true, true);
    rig.provider
        .script("private-content", vec![Script::Answer(answered(Some(7)))]);
    *rig.router.report_refused.lock().unwrap() = Some("store_error");
    call(&rig, &routed_request(&["private-content"]))
        .await
        .unwrap();
    *rig.router.check_expired_content.lock().unwrap() = Some(rig.store.clone());
    tokio::time::advance(std::time::Duration::from_millis(
        broca_store::system_one::RETENTION_MS as u64 + 1,
    ))
    .await;
    rig.service.recover_routing().await.unwrap();
    let row = rig.store.lookup(STORE_ROLE, "routed").unwrap().unwrap();
    assert!(row.outcomes.is_empty() && row.identity == "{}");
    *rig.router.report_refused.lock().unwrap() = None;
    rig.service.recover_routing().await.unwrap();
    assert!(rig.store.lookup(STORE_ROLE, "routed").unwrap().is_none());
}

#[tokio::test(start_paused = true)]
async fn audit_failed_renewal_keeps_inflight_answer_and_stops_next_item() {
    let rig = routed_rig(true, true);
    rig.router.lease_ms.store(12_000, Ordering::SeqCst);
    rig.router.renew_refused.store(true, Ordering::SeqCst);
    let gate = Arc::new(Semaphore::new(0));
    rig.provider
        .script("paid", vec![Script::Held(gate.clone(), answered(Some(7)))]);
    rig.provider
        .script("unsent", vec![Script::Answer(answered(Some(9)))]);
    let service = rig.service.clone();
    let task = tokio::spawn(async move {
        service
            .spawn_call(routed_request(&["paid", "unsent"]), test_scope())
            .await
    });
    wait_for("paid request entered", || rig.provider.calls("paid") > 0).await;
    tokio::time::advance(std::time::Duration::from_secs(8)).await;
    wait_for("lease renewal entered", || {
        !rig.router.renew_times.lock().unwrap().is_empty()
    })
    .await;
    gate.add_permits(1);
    let reply: Value = serde_json::from_str(task.await.unwrap().unwrap().get()).unwrap();
    assert!(
        reply["items"][0].get("answers").is_some(),
        "in-flight paid answer must finish after renewal refusal"
    );
    assert_eq!(rig.provider.calls("unsent"), 0);
    assert_eq!(reply["items"][1]["error"]["code"], "provider_error");
    assert_eq!(reply["items"][1]["error"]["class"], "transient");
}

#[tokio::test]
async fn audit_rejected_account_is_released_and_never_dispatched() {
    let rig = routed_rig(true, true);
    rig.router.invalid_account.store(true, Ordering::SeqCst);
    let error = call(&rig, &routed_request(&["x"])).await.unwrap_err();
    assert_eq!(error["code"], "internal_error");
    assert_eq!(error["detail"]["class"], "permanent");
    assert!(!rig.router.pin_held.load(Ordering::SeqCst));
    assert_eq!(rig.provider.calls("x"), 0);
    assert_eq!(
        rig.router.bodies("route.account_observe")[0]["kind"],
        "run_finished"
    );
}

#[tokio::test]
async fn audit_admission_grant_crash_replays_intent_and_releases_before_next_run() {
    let root = broca_test_support::scratch_root().join(format!(
        "audit-admission-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("system-one.db");
    let rig = routed_rig_with_store(
        Arc::new(SystemOneStore::open(&path).unwrap()),
        Router::new(true, true),
    );
    rig.router.panic_on_admission.store(true, Ordering::SeqCst);
    let router = rig.router.clone();
    assert!(call(&rig, &routed_request(&["x"])).await.is_err());
    assert_eq!(rig.provider.calls("x"), 0);
    assert!(router.pin_held.load(Ordering::SeqCst));
    let original = router.bodies("route.account_for")[0].clone();
    broca_test_support::wait_until("panicked admission call releases its durable hold", || {
        std::future::ready(
            if rig
                .store
                .routing_batch_running(STORE_ROLE, "routed")
                .unwrap()
            {
                Err("the fallback store release is still running".into())
            } else {
                Ok(())
            },
        )
    })
    .await;
    drop(rig);
    let store = Arc::new(SystemOneStore::open(&path).unwrap());
    store.release_abandoned().unwrap();
    let resumed = routed_rig_with_store(store, router.clone());
    resumed.service.recover_routing().await.unwrap();
    assert_eq!(router.bodies("route.account_for")[1], original);
    assert!(!router.pin_held.load(Ordering::SeqCst));
    resumed
        .provider
        .script("x", vec![Script::Answer(answered(Some(7)))]);
    call(&resumed, &routed_request(&["x"])).await.unwrap();
    assert_eq!(
        router.selects.load(Ordering::SeqCst),
        1,
        "committed decision must survive failed admission freeze"
    );
    assert_eq!(resumed.provider.calls("x"), 1);
}

#[tokio::test(start_paused = true)]
async fn audit_latest_renewal_admission_releases_bound_pin() {
    let _clock = ManualClock::new();
    let rig = routed_rig(true, true);
    rig.router.lease_ms.store(12_000, Ordering::SeqCst);
    let gate = Arc::new(Semaphore::new(0));
    rig.provider
        .script("held", vec![Script::Held(gate.clone(), answered(Some(7)))]);
    let service = rig.service.clone();
    let task = tokio::spawn(async move {
        service
            .spawn_call(routed_request(&["held"]), test_scope())
            .await
    });
    wait_for("held request entered", || rig.provider.calls("held") > 0).await;
    let renewed = rig.router.renewed.notified();
    tokio::time::advance(std::time::Duration::from_secs(8)).await;
    broca_test_support::event_timeout("lease renewal", renewed, || "router did not renew".into())
        .await
        .unwrap();
    wait_for("renewal admission stored", || {
        rig.router.bodies("route.account_for").len() >= 2
    })
    .await;
    let admissions = rig.router.bodies("route.account_for");
    let original_id = admissions[0]["admission_id"].as_str().unwrap();
    let latest_id = admissions[1]["admission_id"].as_str().unwrap();
    let wrong_finish = json!({"method":"route.account_observe", "params":{"event_id":"negative-control", "kind":"run_finished", "admission_id":original_id}});
    rig.router
        .request(serde_json::to_vec(&wrong_finish).unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(
        rig.router.pin_held.load(Ordering::SeqCst),
        "finishing original admission must not release a renewed pin"
    );
    gate.add_permits(1);
    task.await.unwrap().unwrap();
    assert!(!rig.router.pin_held.load(Ordering::SeqCst));
    assert_eq!(
        rig.router.bodies("route.account_observe").last().unwrap()["admission_id"],
        latest_id
    );
}

#[tokio::test(start_paused = true)]
async fn audit_invalid_renewal_keeps_paid_answer_and_releases_rejected_admission() {
    let _clock = ManualClock::new();
    let rig = routed_rig(true, true);
    rig.router.lease_ms.store(12_000, Ordering::SeqCst);
    let gate = Arc::new(Semaphore::new(0));
    rig.provider
        .script("paid", vec![Script::Held(gate.clone(), answered(Some(7)))]);
    let service = rig.service.clone();
    let task = tokio::spawn(async move {
        service
            .spawn_call(routed_request(&["paid"]), test_scope())
            .await
    });
    broca_test_support::event_timeout(
        "paid request entered",
        async {
            while rig.provider.calls("paid") == 0 {
                tokio::task::yield_now().await;
            }
        },
        || "provider did not enter".into(),
    )
    .await
    .unwrap();
    rig.router.invalid_account.store(true, Ordering::SeqCst);
    let renewed = rig.router.renewed.notified();
    tokio::time::advance(std::time::Duration::from_secs(8)).await;
    broca_test_support::event_timeout("lease renewal", renewed, || "router did not renew".into())
        .await
        .unwrap();
    gate.add_permits(1);
    let reply: Value = serde_json::from_str(task.await.unwrap().unwrap().get()).unwrap();
    assert!(reply["items"][0].get("answers").is_some());
    assert!(!rig.router.pin_held.load(Ordering::SeqCst));
    assert!(matches!(
        rig.store
            .lookup(STORE_ROLE, "routed")
            .unwrap()
            .unwrap()
            .outcomes[0]
            .outcome,
        broca_store::system_one::Outcome::Answered(_)
    ));
    let report = &rig.router.bodies("route.report")[0];
    assert_eq!(report["outcome"], "completed");
    assert!(report.get("runner_code").is_none());
    assert!(report.get("refusal_class").is_none());
}

#[tokio::test(start_paused = true)]
async fn audit_recovery_runs_at_boot_and_interval_and_skips_corrupt_batch() {
    let _clock = ManualClock::new();
    let root = broca_test_support::scratch_root().join(format!(
        "audit-recovery-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("system-one.db");
    let rig = routed_rig_with_store(
        Arc::new(SystemOneStore::open(&path).unwrap()),
        Router::new(true, true),
    );
    *rig.router.report_refused.lock().unwrap() = Some("store_error");
    rig.provider
        .script("x", vec![Script::Answer(answered(Some(7)))]);
    call(&rig, &routed_request(&["x"])).await.unwrap();
    rusqlite::Connection::open(&path).unwrap().execute_batch("CREATE TEMP TABLE broken AS SELECT * FROM batch WHERE batch_id='routed'; UPDATE broken SET batch_id='a-bad', routing_runs='not-json'; INSERT INTO batch SELECT * FROM broken;").unwrap();
    let stop = tokio_util::sync::CancellationToken::new();
    let worker = rig.service.spawn_routing_recovery(stop.clone());
    let before = rig.router.bodies("route.report").len();
    broca_test_support::event_timeout(
        "boot routing report",
        async {
            while rig.router.bodies("route.report").len() == before {
                tokio::task::yield_now().await;
            }
        },
        || "boot recovery never attempted healthy row after bad row".into(),
    )
    .await
    .unwrap();
    *rig.router.report_refused.lock().unwrap() = None;
    tokio::time::advance(std::time::Duration::from_secs(30)).await;
    broca_test_support::event_timeout(
        "periodic routing report",
        async {
            while rig
                .store
                .routing(STORE_ROLE, "routed")
                .unwrap()
                .unwrap()
                .pending
            {
                tokio::task::yield_now().await;
            }
        },
        || "periodic recovery did not drain healthy batch".into(),
    )
    .await
    .unwrap();
    stop.cancel();
    worker.await.unwrap();
}

#[tokio::test]
async fn routed_batch_freezes_requirements_account_and_replays_bytes() {
    let rig = routed_rig(true, true);
    for state in ["first", "second"] {
        rig.provider
            .script(state, vec![Script::Answer(answered(Some(10)))]);
    }
    assert!(rig.service.describe_live().await.routed_models.is_some());
    let params = routed_request(&["first", "second"]);
    let reply = call(&rig, &params).await.unwrap();
    let value: Value = serde_json::from_str(reply.get()).unwrap();
    assert_eq!(value["model"], JEV);
    assert_eq!(value["decision_id"], "decision-0");
    let select = &rig.router.bodies("route.select")[0];
    let schema = r#"{"q":{"type":"noul","instructions":"true?"}}"#.len() as u64;
    assert_eq!(
        select,
        &json!({"consumer":"classifier","requirements":{"iq":0,"context_window":512+schema+8,"questions":1,"input_tokens":2*(512+schema)+7+8,"max_cost_usd":1.0},"runners":["broca"]})
    );
    let accounts = rig.router.bodies("route.account_for");
    assert_eq!(accounts[0]["decision_id"], "decision-0");
    assert_eq!(accounts[0]["pool"], "keys");
    assert!(accounts[0].get("caller").is_none());
    assert_eq!(*rig.credentials.lock().unwrap(), ["apikey:a", "apikey:a"]);
    assert_eq!(
        rig.router.bodies("route.report")[0]["usage"]["input_tokens"],
        20
    );
    rig.router.clear();
    rig.catalog.forbidden.store(true, Ordering::SeqCst);
    rig.router.unreachable.store(true, Ordering::SeqCst);
    let replay = call(&rig, &params).await.unwrap();
    assert_eq!(replay.get(), reply.get());
    assert!(rig.router.calls.lock().unwrap().is_empty());
    assert_eq!(rig.provider.calls("first"), 1);
    assert_eq!(rig.provider.calls("second"), 1);
}

#[tokio::test]
async fn routed_requirements_omit_unadvertised_fields_and_include_images() {
    let rig = routed_rig(true, false);
    *rig.router.models.lock().unwrap() = vec![CLEF.into()];
    rig.provider
        .script("picture", vec![Script::Answer(answered(Some(10)))]);
    let mut params: Value = serde_json::from_str(&routed_request(&["picture"])).unwrap();
    params["items"][0]["images"] = json!([{"content_type":"image/png","base64":"AQ=="}]);
    let _ = call(&rig, &params.to_string()).await.unwrap();
    let select = &rig.router.bodies("route.select")[0];
    assert_eq!(select["requirements"]["modalities"], json!(["image"]));
    for field in ["questions", "input_tokens", "max_cost_usd"] {
        assert!(select["requirements"].get(field).is_none());
    }
    assert_eq!(
        select["requirements"]["context_window"],
        512 + r#"{"q":{"type":"noul","instructions":"true?"}}"#.len() as u64 + 9 + 4096
    );
}

#[tokio::test]
async fn routed_pin_is_strict_and_refusal_preserves_routing_reason() {
    let rig = routed_rig(true, true);
    *rig.router.refused.lock().unwrap() = Some(("pin_unavailable", "the pinned route is paused"));
    let mut params: Value = serde_json::from_str(&routed_request(&["first"])).unwrap();
    params["model"] = json!(JEV);
    let error = call(&rig, &params.to_string()).await.unwrap_err();
    assert_eq!(error["code"], "routing_refused");
    assert_eq!(error["detail"]["class"], "permanent");
    assert_eq!(error["detail"]["routing_code"], "pin_unavailable");
    assert_eq!(
        error["detail"]["routing_reason"],
        "the pinned route is paused"
    );
    assert_eq!(error["detail"]["model"], JEV);
    let select = &rig.router.bodies("route.select")[0];
    assert_eq!(select["pin"], JEV);
    assert!(select.get("pin_fallback").is_none());
    assert!(rig.store.lookup(STORE_ROLE, "routed").unwrap().is_none());
}

#[tokio::test]
async fn routed_unavailable_or_unready_never_sends_or_stores() {
    for unready in [true, false] {
        let rig = routed_rig(true, true);
        if unready {
            rig.router.describe.lock().unwrap().consumers.clear();
        } else {
            rig.router.unreachable.store(true, Ordering::SeqCst);
        }
        assert!(rig.service.describe_live().await.routed_models.is_none());
        let error = call(&rig, &routed_request(&["first"])).await.unwrap_err();
        assert_eq!(error["code"], "routing_unavailable");
        assert_eq!(error["detail"]["class"], "transient");
        assert!(rig.router.bodies("route.select").is_empty());
        assert!(rig.provider.bodies.lock().unwrap().is_empty());
        assert!(rig.store.lookup(STORE_ROLE, "routed").unwrap().is_none());
    }
}

#[tokio::test]
async fn routed_unknown_adapter_and_gated_openai_are_refused_and_reported() {
    for model in ["elsewhere/unknown", OPENAI_DECISIONS] {
        let rig = routed_rig(true, true);
        *rig.router.models.lock().unwrap() = vec![model.into()];
        assert!(!rig
            .service
            .describe_live()
            .await
            .models
            .iter()
            .any(|entry| entry.model == model));
        let error = call(&rig, &routed_request(&["first"])).await.unwrap_err();
        assert_eq!(error["code"], "model_unavailable");
        assert!(error["detail"].get("routing_code").is_none());
        assert!(error["detail"].get("routing_answer").is_some());
        assert!(error["message"].as_str().unwrap().contains(model));
        assert!(rig.provider.bodies.lock().unwrap().is_empty());
        assert_eq!(
            rig.router.bodies("route.report")[0]["runner_code"],
            "model_unavailable"
        );
    }
}

#[tokio::test]
async fn routed_transient_resend_uses_frozen_model_and_requested_account() {
    let rig = routed_rig(true, true);
    rig.provider.script(
        "retry",
        vec![Script::Status(503, None), Script::Answer(answered(Some(7)))],
    );
    rig.provider
        .script("done", vec![Script::Answer(answered(Some(11)))]);
    let params = routed_request(&["retry", "done"]);
    let first: Value = serde_json::from_str(call(&rig, &params).await.unwrap().get()).unwrap();
    assert_eq!(first["items"][0]["error"]["code"], "provider_error");
    rig.router.clear();
    rig.catalog.forbidden.store(true, Ordering::SeqCst);
    let second: Value = serde_json::from_str(call(&rig, &params).await.unwrap().get()).unwrap();
    assert_eq!(first["decision_id"], second["decision_id"]);
    assert_eq!(rig.router.bodies("route.select").len(), 0);
    assert_eq!(rig.router.bodies("role.describe").len(), 1);
    assert_eq!(
        rig.router.bodies("route.account_for")[0]["requested_account"]["credential_id"],
        "apikey:a"
    );
    assert_eq!(rig.provider.calls("done"), 1);
    assert_eq!(
        *rig.credentials.lock().unwrap(),
        ["apikey:a", "apikey:a", "apikey:a"]
    );
    assert_eq!(
        rig.router.bodies("route.report")[0]["usage"]["input_tokens"],
        7
    );
}

#[tokio::test]
async fn routed_account_failure_reselects_once_before_any_answer() {
    let rig = routed_rig(true, true);
    *rig.router.models.lock().unwrap() = vec![JEV.into(), CLEF.into()];
    rig.provider.script(
        "retry",
        vec![Script::AccountError, Script::Answer(answered(Some(7)))],
    );
    rig.provider
        .script("second", vec![Script::Answer(answered(Some(11)))]);
    let reply: Value = serde_json::from_str(
        call(&rig, &routed_request(&["retry", "second"]))
            .await
            .unwrap()
            .get(),
    )
    .unwrap();
    assert_eq!(reply["model"], CLEF);
    assert_eq!(reply["decision_id"], "decision-1");
    assert_eq!(rig.router.bodies("route.select").len(), 2);
    assert_eq!(
        *rig.credentials.lock().unwrap(),
        ["apikey:a", "apikey:b", "apikey:b"]
    );
    let reports = rig.router.bodies("route.report");
    assert_eq!(reports.len(), 2);
    assert_eq!(reports[0]["outcome"], "failed");
    assert_eq!(reports[0]["refusal_class"], "credential_refused");
    assert!(reports[0]["usage"].get("input_tokens").is_none());
    assert_eq!(reports[1]["outcome"], "completed");
    assert_eq!(
        rig.router
            .bodies("route.account_observe")
            .iter()
            .filter(|body| body["kind"] == "run_finished")
            .count(),
        2
    );
}

#[tokio::test]
async fn routed_account_failure_after_answer_is_frozen_and_replayed() {
    let rig = routed_rig(true, true);
    rig.provider
        .script("done", vec![Script::Answer(answered(Some(10)))]);
    rig.provider.script("bad", vec![Script::AccountError]);
    let params = routed_request(&["done", "bad", "not-sent"]);
    let reply = call(&rig, &params).await.unwrap();
    let value: Value = serde_json::from_str(reply.get()).unwrap();
    assert_eq!(value["items"][1]["error"]["code"], "account_not_entitled");
    assert_eq!(value["items"][2]["error"], value["items"][1]["error"]);
    assert_eq!(rig.router.bodies("route.select").len(), 1);
    assert_eq!(rig.provider.calls("not-sent"), 0);
    rig.router.clear();
    assert_eq!(call(&rig, &params).await.unwrap().get(), reply.get());
    assert!(rig.router.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn routed_second_account_failure_cannot_reselect_again() {
    let rig = routed_rig(true, true);
    rig.provider.script("bad", vec![Script::AccountError]);
    let params = routed_request(&["bad"]);
    let first = call(&rig, &params).await.unwrap();
    assert_eq!(rig.router.bodies("route.select").len(), 2);
    assert_eq!(rig.provider.calls("bad"), 2);
    rig.router.clear();
    assert_eq!(call(&rig, &params).await.unwrap().get(), first.get());
    assert!(rig.router.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn routed_without_run_reports_waits_for_final_and_reports_once() {
    let rig = routed_rig(false, false);
    rig.provider.script(
        "retry",
        vec![Script::Status(503, None), Script::Answer(answered(Some(7)))],
    );
    rig.provider
        .script("done", vec![Script::Answer(answered(Some(11)))]);
    let params = routed_request(&["retry", "done"]);
    call(&rig, &params).await.unwrap();
    assert!(rig.router.bodies("route.report").is_empty());
    call(&rig, &params).await.unwrap();
    rig.service.recover_routing().await.unwrap();
    let reports = rig.router.bodies("route.report");
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0]["outcome"], "completed");
    assert!(reports[0]["usage"].get("input_tokens").is_none());
    assert!(reports[0]["usage"].get("cost_usd").is_none());
    assert!(reports[0]["usage"].get("run_id").is_none());
}

#[tokio::test]
async fn routed_restart_retries_identical_report_after_lost_ack() {
    let directory = broca_test_support::scratch_root().join(format!(
        "routed-report-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join("system-one.db");
    let router = Router::new(true, true);
    let rig = routed_rig_with_store(
        Arc::new(SystemOneStore::open(&path).unwrap()),
        router.clone(),
    );
    rig.provider
        .script("done", vec![Script::Answer(answered(None))]);
    router.lose_ack.store(true, Ordering::SeqCst);
    let reply = call(&rig, &routed_request(&["done"])).await.unwrap();
    let first = router.bodies("route.report")[0].clone();
    assert!(first["usage"].get("input_tokens").is_none());
    drop(rig);
    let restarted = routed_rig_with_store(
        Arc::new(SystemOneStore::open(&path).unwrap()),
        router.clone(),
    );
    restarted.service.recover_routing().await.unwrap();
    assert_eq!(router.bodies("route.report"), vec![first.clone(), first]);
    assert_eq!(router.reports.lock().unwrap().len(), 1);
    router.clear();
    assert_eq!(
        call(&restarted, &routed_request(&["done"]))
            .await
            .unwrap()
            .get(),
        reply.get()
    );
    assert!(router.calls.lock().unwrap().is_empty());
    drop(restarted);
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test(start_paused = true)]
async fn routed_lease_renews_on_schedule_with_frozen_pin_and_stable_retry_id() {
    let _clock = ManualClock::new();
    for lease_ms in [12_000, 600_000] {
        let rig = routed_rig(true, true);
        rig.router.lease_ms.store(lease_ms, Ordering::SeqCst);
        rig.router
            .renew_transport_failures
            .store(1, Ordering::SeqCst);
        let gate = Arc::new(Semaphore::new(0));
        rig.provider
            .script("held", vec![Script::Held(gate.clone(), answered(Some(10)))]);
        let entered = rig.provider.entered.notified();
        let service = rig.service.clone();
        let task = tokio::spawn(async move {
            service
                .spawn_call(routed_request(&["held"]), test_scope())
                .await
        });
        broca_test_support::event_timeout("routed provider entered", entered, || {
            "provider did not enter".into()
        })
        .await
        .unwrap();
        let routing = rig.store.routing(STORE_ROLE, "routed").unwrap().unwrap();
        let journal: Value = serde_json::from_str(&routing.runs).unwrap();
        let due = journal["runs"][0]["renew_at_ms"].as_u64().unwrap();
        let started_at = rig.router.now();
        let wait = due - started_at;
        tokio::time::advance(std::time::Duration::from_millis(wait - 1)).await;
        assert!(rig.router.renew_times.lock().unwrap().is_empty());
        let renewed = rig.router.renewed.notified();
        tokio::time::advance(std::time::Duration::from_millis(1)).await;
        broca_test_support::event_timeout("lease renewal", renewed, || {
            "router did not renew".into()
        })
        .await
        .unwrap();
        gate.add_permits(1);
        let reply = task.await.unwrap().unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(reply.get()).unwrap()["items"][0]["usage"]
                ["input_tokens"],
            10
        );
        let admissions = rig.router.bodies("route.account_for");
        assert_eq!(
            admissions.len(),
            3,
            "one admit and two identical renewal transport attempts"
        );
        assert_eq!(admissions[1], admissions[2]);
        assert_eq!(
            admissions[1]["admission_id"],
            format!(
                "{}:renew:1",
                admissions[0]["admission_id"].as_str().unwrap()
            )
        );
        assert_eq!(
            admissions[1]["expected_pin"],
            json!({"id":"pin","generation":1})
        );
        assert_eq!(
            admissions[1]["requested_account"]["credential_id"],
            "apikey:a"
        );
        assert_eq!(rig.router.renew_times.lock().unwrap()[0], due);
    }
}

#[tokio::test(start_paused = true)]
async fn routed_refused_renewal_fails_unanswered_without_further_dispatch() {
    let _clock = ManualClock::new();
    let rig = routed_rig(true, true);
    rig.router.lease_ms.store(12_000, Ordering::SeqCst);
    rig.router.renew_refused.store(true, Ordering::SeqCst);
    rig.provider
        .script("done", vec![Script::Answer(answered(Some(10)))]);
    let gate = Arc::new(Semaphore::new(0));
    rig.provider
        .script("held", vec![Script::Held(gate.clone(), answered(Some(10)))]);
    let entered = rig.provider.entered.notified();
    let service = rig.service.clone();
    let task = tokio::spawn(async move {
        service
            .spawn_call(routed_request(&["done", "held", "never"]), test_scope())
            .await
    });
    broca_test_support::event_timeout("routed provider entered", entered, || {
        "provider did not enter".into()
    })
    .await
    .unwrap();
    let journal: Value = serde_json::from_str(
        &rig.store
            .routing(STORE_ROLE, "routed")
            .unwrap()
            .unwrap()
            .runs,
    )
    .unwrap();
    let due = journal["runs"][0]["renew_at_ms"].as_u64().unwrap();
    let renewed = rig.router.renewed.notified();
    tokio::time::advance(std::time::Duration::from_millis(due - rig.router.now())).await;
    broca_test_support::event_timeout("lease renewal", renewed, || "router did not renew".into())
        .await
        .unwrap();
    gate.add_permits(1);
    let reply: Value = serde_json::from_str(task.await.unwrap().unwrap().get()).unwrap();
    assert!(reply["items"][0].get("answers").is_some());
    assert!(reply["items"][1].get("answers").is_some());
    assert_eq!(reply["items"][2]["error"]["code"], "provider_error");
    assert!(reply["items"][2]["error"]["message"]
        .as_str()
        .unwrap()
        .contains("pin_generation_conflict"));
    assert_eq!(rig.provider.calls("never"), 0);
    assert_eq!(rig.router.bodies("route.select").len(), 1);
    assert_eq!(rig.router.bodies("route.account_for").len(), 2);
    assert_eq!(rig.router.bodies("route.report")[0]["outcome"], "failed");
}

#[tokio::test]
async fn routed_short_completed_call_has_no_renewal() {
    let rig = routed_rig(true, true);
    rig.provider
        .script("done", vec![Script::Answer(answered(Some(10)))]);
    call(&rig, &routed_request(&["done"])).await.unwrap();
    assert!(rig.router.renew_times.lock().unwrap().is_empty());
    assert_eq!(rig.router.bodies("route.account_for").len(), 1);
}

/// Keep idle Tokio from advancing a paused clock while a SQLite blocking task is finishing.
/// Only the test's explicit advances are allowed to move a lease deadline.
struct ManualClock(tokio::task::JoinHandle<()>);
impl ManualClock {
    fn new() -> Self {
        Self(tokio::spawn(async {
            loop {
                tokio::task::yield_now().await;
            }
        }))
    }
}
impl Drop for ManualClock {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[tokio::test]
async fn routed_fresh_account_refusal_preserves_pin_and_committed_selection() {
    let rig = routed_rig(true, true);
    *rig.router.admission_refused.lock().unwrap() =
        Some(("account_pool_exhausted", "no account has credit"));
    let mut request: Value = serde_json::from_str(&routed_request(&["first"])).unwrap();
    request["model"] = json!(JEV);
    for _ in 0..2 {
        let refusal = call(&rig, &request.to_string()).await.unwrap_err();
        assert_eq!(refusal["code"], "routing_refused");
        assert_eq!(refusal["detail"]["model"], JEV);
        assert_eq!(refusal["detail"]["routing_code"], "account_pool_exhausted");
        assert!(rig.store.lookup(STORE_ROLE, "routed").unwrap().is_some());
    }
    assert_eq!(rig.router.bodies("route.select").len(), 1);
    assert!(rig.router.bodies("route.report").is_empty());
    assert!(rig.router.bodies("route.account_observe").is_empty());
}

#[tokio::test]
async fn routed_answered_readmission_refusal_preserves_batch_without_run_or_penalty() {
    for routing_code in ["auth_required", "account_not_authorized"] {
        let rig = routed_rig(true, true);
        rig.provider
            .script("done", vec![Script::Answer(answered(Some(10)))]);
        rig.provider
            .script("retry", vec![Script::Status(503, None)]);
        let params = routed_request(&["done", "retry"]);
        call(&rig, &params).await.unwrap();
        let before = rig.store.lookup(STORE_ROLE, "routed").unwrap().unwrap();
        rig.router.clear();
        *rig.router.admission_refused.lock().unwrap() = Some((routing_code, "admission refused"));
        let refusal = call(&rig, &params).await.unwrap_err();
        assert_eq!(refusal["code"], "routing_refused");
        assert_eq!(refusal["detail"]["routing_code"], routing_code);
        assert_eq!(refusal["detail"]["class"], "permanent");
        assert_eq!(
            rig.store
                .lookup(STORE_ROLE, "routed")
                .unwrap()
                .unwrap()
                .outcomes,
            before.outcomes
        );
        assert!(rig.router.bodies("route.select").is_empty());
        assert!(rig.router.bodies("route.report").is_empty());
        assert!(rig.router.bodies("route.account_observe").is_empty());
        *rig.router.admission_refused.lock().unwrap() = None;
        rig.provider
            .script("retry", vec![Script::Answer(answered(Some(9)))]);
        let reply: Value = serde_json::from_str(call(&rig, &params).await.unwrap().get()).unwrap();
        assert!(reply["items"][1].get("answers").is_some());
        assert_eq!(rig.provider.calls("done"), 1);
        assert_eq!(rig.provider.calls("retry"), 2);
        assert!(rig.router.bodies("route.select").is_empty());
    }
}

#[tokio::test]
async fn routed_retry_outage_keeps_stored_answers_and_account_bytes_unchanged() {
    let rig = routed_rig(true, true);
    rig.provider
        .script("done", vec![Script::Answer(answered(Some(10)))]);
    rig.provider
        .script("retry", vec![Script::Status(503, None)]);
    let params = routed_request(&["done", "retry"]);
    call(&rig, &params).await.unwrap();
    let before = rig.store.lookup(STORE_ROLE, "routed").unwrap().unwrap();
    let routing = rig.store.routing(STORE_ROLE, "routed").unwrap().unwrap();
    rig.router.clear();
    rig.router.unreachable.store(true, Ordering::SeqCst);
    let refusal = call(&rig, &params).await.unwrap_err();
    assert_eq!(refusal["code"], "routing_unavailable");
    assert_eq!(refusal["detail"]["class"], "transient");
    let after = rig.store.lookup(STORE_ROLE, "routed").unwrap().unwrap();
    assert_eq!(before.outcomes, after.outcomes);
    assert_eq!(before.spent_nanos, after.spent_nanos);
    assert_eq!(
        rig.store.routing(STORE_ROLE, "routed").unwrap().unwrap(),
        routing
    );
    assert!(rig.router.bodies("route.select").is_empty());
    assert!(rig.router.bodies("route.account_for").is_empty());
    assert_eq!(rig.provider.calls("retry"), 1);
}

#[tokio::test]
async fn routed_resettable_account_limit_reports_reset_without_reselection() {
    let rig = routed_rig(true, true);
    *rig.router.models.lock().unwrap() = vec![CLEF.into()];
    rig.provider
        .script("limited", vec![Script::AccountLimit(10_000)]);
    let reply: Value = serde_json::from_str(
        call(&rig, &routed_request(&["limited", "not-sent"]))
            .await
            .unwrap()
            .get(),
    )
    .unwrap();
    assert_eq!(
        reply["items"][0]["error"]["code"],
        "account_limit_exhausted"
    );
    assert_eq!(rig.router.bodies("route.select").len(), 1);
    assert_eq!(rig.provider.calls("not-sent"), 0);
    let observations = rig.router.bodies("route.account_observe");
    let observation = observations
        .iter()
        .find(|body| body["kind"] == "account_limit_exhausted")
        .unwrap();
    let reset = observation["resets_at_ms"].as_u64().unwrap();
    assert_eq!(
        reset,
        observation["observed_at_ms"].as_u64().unwrap() + 10_000
    );
    assert_eq!(observation["resets_at_source"], "retry_after");
    let report = &rig.router.bodies("route.report")[0];
    assert_eq!(report["reset_at_ms"], reset);
    assert!(report.get("refusal_class").is_none());
}

#[tokio::test]
async fn routed_invalid_key_is_retried_before_credential_refused_observation() {
    let rig = routed_rig(true, true);
    rig.provider.script(
        "auth",
        vec![
            Script::Status(401, None),
            Script::Status(401, None),
            Script::Answer(answered(Some(10))),
        ],
    );
    let reply: Value =
        serde_json::from_str(call(&rig, &routed_request(&["auth"])).await.unwrap().get()).unwrap();
    assert_eq!(reply["decision_id"], "decision-1");
    assert_eq!(
        *rig.credentials.lock().unwrap(),
        ["apikey:a", "apikey:a", "apikey:b"]
    );
    assert_eq!(
        rig.router
            .bodies("route.account_observe")
            .iter()
            .filter(|body| body["kind"] == "credential_refused")
            .count(),
        1
    );
    assert_eq!(
        rig.router.bodies("route.report")[0]["refusal_class"],
        "credential_refused"
    );
}

#[tokio::test]
async fn routed_restart_mid_batch_reports_committed_usage_and_keeps_choice() {
    let directory = broca_test_support::scratch_root().join(format!(
        "routed-mid-batch-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join("system-one.db");
    let router = Router::new(true, true);
    let rig = routed_rig_with_store(
        Arc::new(SystemOneStore::open(&path).unwrap()),
        router.clone(),
    );
    rig.provider
        .script("done", vec![Script::Answer(answered(Some(10)))]);
    rig.provider.script("unfinished", vec![Script::Panic]);
    let params = routed_request(&["done", "unfinished"]);
    assert_eq!(
        call(&rig, &params).await.unwrap_err()["code"],
        "internal_error"
    );
    assert!(router.bodies("route.report").is_empty());
    // A disappeared owner releases its row before the new process recovers durable work.
    rig.store
        .release(STORE_ROLE, "routed", &rig.service.owner)
        .unwrap();
    drop(rig);
    let restarted = routed_rig_with_store(
        Arc::new(SystemOneStore::open(&path).unwrap()),
        router.clone(),
    );
    restarted.service.recover_routing().await.unwrap();
    restarted.service.recover_routing().await.unwrap();
    let reports = router.bodies("route.report");
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0]["outcome"], "failed");
    for field in ["input_tokens", "output_tokens", "cost_usd"] {
        assert!(reports[0]["usage"].get(field).is_none());
    }
    restarted
        .provider
        .script("unfinished", vec![Script::Answer(answered(Some(7)))]);
    let reply: Value =
        serde_json::from_str(call(&restarted, &params).await.unwrap().get()).unwrap();
    assert_eq!(reply["decision_id"], "decision-0");
    assert_eq!(restarted.provider.calls("done"), 0);
    assert_eq!(router.bodies("route.select").len(), 1);
    assert_eq!(router.bodies("route.report").len(), 2);
    assert_eq!(router.bodies("route.report")[1]["usage"]["input_tokens"], 7);
    drop(restarted);
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn routed_per_decision_report_keeps_exact_fully_measured_usage() {
    let rig = routed_rig(false, true);
    rig.provider
        .script("first", vec![Script::Answer(answered(Some(10)))]);
    rig.provider
        .script("second", vec![Script::Answer(answered(Some(8)))]);
    let reply: Value = serde_json::from_str(
        call(&rig, &routed_request(&["first", "second"]))
            .await
            .unwrap()
            .get(),
    )
    .unwrap();
    let report = &rig.router.bodies("route.report")[0];
    assert_eq!(report["usage"]["input_tokens"], 18);
    assert_eq!(report["usage"]["output_tokens"], 0);
    assert_eq!(report["usage"]["cost_usd"], 0.000000756);
    assert!(report["usage"].get("run_id").is_none());
    assert_eq!(reply["usage"]["input_tokens"], 18);
}

#[tokio::test]
async fn routed_report_omits_only_the_aggregate_that_is_unknown() {
    let rig = routed_rig(true, true);
    rig.provider.script(
        "partial",
        vec![Script::Answer(
            r#"{"answers":{"q":{"type":"noul","noul":0.5}},"usage":{"input_tokens":10}}"#.into(),
        )],
    );
    rig.provider
        .script("full", vec![Script::Answer(answered(Some(8)))]);
    let reply: Value = serde_json::from_str(
        call(&rig, &routed_request(&["partial", "full"]))
            .await
            .unwrap()
            .get(),
    )
    .unwrap();
    let report = &rig.router.bodies("route.report")[0];
    assert_eq!(report["usage"]["input_tokens"], 18);
    assert!(report["usage"].get("output_tokens").is_none());
    assert_eq!(reply["usage"]["input_tokens"], 18);
    assert_eq!(reply["usage"]["output_tokens"], 0);
}

#[tokio::test]
async fn routed_run_reports_only_dispatched_items_not_spend_guard_skips() {
    let rig = routed_rig(true, true);
    rig.provider
        .script("expensive", vec![Script::Answer(answered(Some(3000)))]);
    let mut request: Value =
        serde_json::from_str(&routed_request(&["expensive", "not-sent"])).unwrap();
    request["max_cost_usd"] = json!(0.0001);
    let reply: Value =
        serde_json::from_str(call(&rig, &request.to_string()).await.unwrap().get()).unwrap();
    assert_eq!(reply["items"][1]["error"]["code"], "cost_exceeded");
    assert_eq!(rig.provider.calls("not-sent"), 0);
    assert_eq!(rig.router.bodies("route.report")[0]["outcome"], "completed");
    assert_eq!(
        rig.router.bodies("route.report")[0]["usage"]["input_tokens"],
        3000
    );
}

#[tokio::test]
async fn routed_resend_over_spend_guard_admits_no_run() {
    let rig = routed_rig(true, true);
    rig.provider
        .script("retry", vec![Script::Status(503, None)]);
    rig.provider
        .script("expensive", vec![Script::Answer(answered(Some(3000)))]);
    let mut request: Value =
        serde_json::from_str(&routed_request(&["retry", "expensive"])).unwrap();
    request["max_cost_usd"] = json!(0.0001);
    call(&rig, &request.to_string()).await.unwrap();
    rig.router.clear();
    let reply: Value =
        serde_json::from_str(call(&rig, &request.to_string()).await.unwrap().get()).unwrap();
    assert_eq!(reply["items"][0]["error"]["code"], "cost_exceeded");
    assert_eq!(reply["decision_id"], "decision-0");
    assert!(rig.router.calls.lock().unwrap().is_empty());
    assert_eq!(rig.provider.calls("retry"), 1);
}

#[tokio::test]
async fn routed_pinned_account_success_carries_a_stable_decision_id() {
    let rig = routed_rig(true, true);
    rig.provider
        .script("pinned", vec![Script::Answer(answered(Some(10)))]);
    let mut params: Value = serde_json::from_str(&routed_request(&["pinned"])).unwrap();
    params["model"] = json!(JEV);
    let reply = call(&rig, &params.to_string()).await.unwrap();
    let value: Value = serde_json::from_str(reply.get()).unwrap();
    assert_eq!(value["model"], JEV);
    assert_eq!(value["decision_id"], "decision-0");
    assert_eq!(rig.router.bodies("route.select")[0]["pin"], JEV);
    rig.router.clear();
    assert_eq!(
        call(&rig, &params.to_string()).await.unwrap().get(),
        reply.get()
    );
    assert!(rig.router.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn routed_refused_credential_after_answer_fails_unanswered_without_reselection() {
    let rig = routed_rig(true, true);
    rig.provider
        .script("done", vec![Script::Answer(answered(Some(10)))]);
    rig.provider.script("auth", vec![Script::Status(401, None)]);
    let params = routed_request(&["done", "auth", "never"]);
    let reply = call(&rig, &params).await.unwrap();
    let value: Value = serde_json::from_str(reply.get()).unwrap();
    assert_eq!(value["items"][1]["error"]["code"], "auth_failed");
    assert_eq!(value["items"][2]["error"], value["items"][1]["error"]);
    assert_eq!(rig.provider.calls("auth"), 2);
    assert_eq!(rig.provider.calls("never"), 0);
    assert_eq!(rig.router.bodies("route.select").len(), 1);
    assert_eq!(
        rig.router.bodies("route.report")[0]["refusal_class"],
        "credential_refused"
    );
    rig.router.clear();
    assert_eq!(call(&rig, &params).await.unwrap().get(), reply.get());
    assert!(rig.router.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn routed_report_terminal_refusals_are_not_retried() {
    for code in ["unknown_decision", "report_conflict"] {
        let rig = routed_rig(true, true);
        *rig.router.report_refused.lock().unwrap() = Some(code);
        rig.provider
            .script("done", vec![Script::Answer(answered(Some(10)))]);
        call(&rig, &routed_request(&["done"])).await.unwrap();
        rig.service.recover_routing().await.unwrap();
        assert_eq!(rig.router.bodies("route.report").len(), 1);
        let record = rig.store.routing(STORE_ROLE, "routed").unwrap().unwrap();
        assert!(!record.pending);
        let journal: Value = serde_json::from_str(&record.runs).unwrap();
        let report = journal["outbox"]
            .as_array()
            .unwrap()
            .iter()
            .find(|rpc| rpc["method"] == "route.report")
            .unwrap();
        assert_eq!(report["conflict"], false);
        assert_eq!(report["closed"], true);
    }
}

#[tokio::test]
async fn routed_optional_outcome_and_credential_groups_are_respected() {
    let rig = routed_rig(true, true);
    rig.router
        .describe
        .lock()
        .unwrap()
        .capabilities
        .retain(|group| {
            !matches!(
                group.as_str(),
                "report_outcomes" | "observe_credential_refused"
            )
        });
    rig.provider.script("bad", vec![Script::Status(401, None)]);
    call(&rig, &routed_request(&["bad"])).await.unwrap();
    for report in rig.router.bodies("route.report") {
        assert_eq!(report["outcome"], "failed");
        assert!(report.get("refusal_class").is_none());
    }
    assert!(rig
        .router
        .bodies("route.account_observe")
        .iter()
        .all(|body| body["kind"] != "credential_refused"));
    let gated = routed_rig(true, true);
    gated
        .router
        .describe
        .lock()
        .unwrap()
        .capabilities
        .retain(|group| group != "report_outcomes");
    *gated.router.models.lock().unwrap() = vec!["elsewhere/unknown".into()];
    assert_eq!(
        call(&gated, &routed_request(&["not-sent"]))
            .await
            .unwrap_err()["code"],
        "model_unavailable"
    );
    assert_eq!(
        gated.router.bodies("route.report")[0]["outcome"],
        "cancelled"
    );
}

#[tokio::test]
async fn routed_paid_fact_keeps_provider_account_metadata_and_unchanged_shape() {
    let mut rig = routed_rig(true, true);
    let export = Arc::new(broca_store::SqliteStore::open_in_memory().unwrap());
    Arc::get_mut(&mut rig.service).unwrap().export = Some(export.clone());
    rig.provider
        .script("done", vec![Script::Attributed(answered(Some(10)))]);
    let request = routed_request(&["done"]);
    call(&rig, &request).await.unwrap();
    call(&rig, &request).await.unwrap();
    let facts = export.read_export_page(None, 100).unwrap().facts;
    assert_eq!(facts.len(), 1);
    let fact = serde_json::to_value(&facts[0].segment).unwrap();
    assert_eq!(fact["kind"], "system_one");
    assert_eq!(fact["auth_selection"], "apikey:a");
    assert_eq!(fact["account_ref"], "provider-account-123");
    assert!(fact.get("decision_id").is_none());
}

#[tokio::test]
async fn routed_identity_keeps_the_callers_pin_or_its_omission() {
    let rig = routed_rig(true, true);
    rig.provider
        .script("done", vec![Script::Answer(answered(Some(10)))]);
    let params = routed_request(&["done"]);
    call(&rig, &params).await.unwrap();
    let batch = rig.store.lookup(STORE_ROLE, "routed").unwrap().unwrap();
    let identity: Value = serde_json::from_str(&batch.identity).unwrap();
    assert!(identity.get("model").is_none());
    assert_eq!(batch.model, JEV);
    let mut changed: Value = serde_json::from_str(&params).unwrap();
    changed["model"] = json!(JEV);
    rig.router.clear();
    let error = call(&rig, &changed.to_string()).await.unwrap_err();
    assert_eq!(error["code"], "batch_id_reuse");
    assert_eq!(error["detail"]["field"], "model");
    assert!(rig.router.calls.lock().unwrap().is_empty());
    assert_eq!(rig.provider.calls("done"), 1);
}

#[tokio::test]
async fn routed_switch_off_validates_before_claim_and_replays_only_original_pins() {
    for pinned in [false, true] {
        let mut rig = routed_rig(true, true);
        rig.provider
            .script("done", vec![Script::Answer(answered(Some(10)))]);
        let params = if pinned {
            exits::pinned(&["done"])
        } else {
            routed_request(&["done"])
        };
        let reply = call(&rig, &params).await.unwrap();
        Arc::get_mut(&mut rig.service).unwrap().config.routed_models = false;
        rig.router.clear();
        if pinned {
            assert_eq!(call(&rig, &params).await.unwrap().get(), reply.get());
        } else {
            rig.catalog.forbidden.store(true, Ordering::SeqCst);
            let refusal = call(&rig, &params).await.unwrap_err();
            assert_eq!(refusal["code"], "invalid_params");
            assert_eq!(refusal["detail"]["field"], "model");
            rig.catalog.forbidden.store(false, Ordering::SeqCst);
        }
        assert!(rig.router.calls.lock().unwrap().is_empty());
        assert_eq!(rig.provider.calls("done"), 1);
        assert!(rig.service.describe_live().await.routed_models.is_none());
        let mut new: Value = serde_json::from_str(&params).unwrap();
        new["batch_id"] = json!("new");
        new.as_object_mut().unwrap().remove("model");
        assert_eq!(
            call(&rig, &new.to_string()).await.unwrap_err(),
            json!({"code":"invalid_params","message":"model is required without routed_models","detail":{"class":"permanent","field":"model"}})
        );
        assert!(rig.store.lookup(STORE_ROLE, "new").unwrap().is_none());
        assert!(rig.router.calls.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn routed_unpinned_unfinished_batch_is_untouched_while_off_and_continues_on() {
    let directory = broca_test_support::scratch_root().join(format!(
        "routed-switch-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join("system-one.db");
    let mut rig = routed_rig_with_store(
        Arc::new(SystemOneStore::open(&path).unwrap()),
        Router::new(true, true),
    );
    rig.provider
        .script("done", vec![Script::Answer(answered(Some(10)))]);
    rig.provider.script(
        "retry",
        vec![Script::Status(503, None), Script::Answer(answered(Some(7)))],
    );
    let params = routed_request(&["done", "retry"]);
    call(&rig, &params).await.unwrap();
    rig.router.clear();
    Arc::get_mut(&mut rig.service).unwrap().config.routed_models = false;
    let before = std::fs::read(&path).unwrap();
    let error = call(&rig, &params).await.unwrap_err();
    assert_eq!(error["code"], "invalid_params");
    assert_eq!(error["detail"]["field"], "model");
    assert_eq!(error["message"], "model is required without routed_models");
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert!(rig.router.calls.lock().unwrap().is_empty());
    assert_eq!(rig.provider.calls("retry"), 1);
    Arc::get_mut(&mut rig.service).unwrap().config.routed_models = true;
    let reply: Value = serde_json::from_str(call(&rig, &params).await.unwrap().get()).unwrap();
    assert_eq!(reply["decision_id"], "decision-0");
    assert!(reply["items"][1].get("answers").is_some());
    assert!(rig.router.bodies("route.select").is_empty());
    assert_eq!(
        rig.router.bodies("route.account_for")[0]["requested_account"]["credential_id"],
        "apikey:a"
    );
    assert_eq!(rig.provider.calls("done"), 1);
    drop(rig);
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn routed_pending_report_drains_with_switch_off() {
    let mut rig = routed_rig(true, true);
    rig.provider
        .script("done", vec![Script::Answer(answered(Some(10)))]);
    rig.router.lose_ack.store(true, Ordering::SeqCst);
    call(&rig, &routed_request(&["done"])).await.unwrap();
    let report = rig.router.bodies("route.report")[0].clone();
    assert!(
        rig.store
            .routing(STORE_ROLE, "routed")
            .unwrap()
            .unwrap()
            .pending
    );
    Arc::get_mut(&mut rig.service).unwrap().config.routed_models = false;
    rig.router.clear();
    rig.service.recover_routing().await.unwrap();
    assert_eq!(rig.router.bodies("route.report"), vec![report]);
    assert!(
        !rig.store
            .routing(STORE_ROLE, "routed")
            .unwrap()
            .unwrap()
            .pending
    );
    assert!(rig.router.bodies("route.select").is_empty());
    assert!(rig.router.bodies("route.account_for").is_empty());
    assert_eq!(rig.provider.calls("done"), 1);
}

#[tokio::test]
async fn routed_restart_before_reselection_allowance_still_selects_exactly_once() {
    let rig = routed_rig(true, true);
    rig.provider.script(
        "auth",
        vec![
            Script::Status(401, None),
            Script::Status(401, None),
            Script::Answer(answered(Some(10))),
        ],
    );
    rig.router
        .panic_after_initial_report
        .store(true, Ordering::SeqCst);
    let params = routed_request(&["auth"]);
    assert_eq!(
        call(&rig, &params).await.unwrap_err()["code"],
        "internal_error"
    );
    broca_test_support::wait_until("routed panic releases batch hold", || {
        std::future::ready(
            if rig
                .store
                .routing_batch_running(STORE_ROLE, "routed")
                .unwrap()
            {
                Err("batch is still running".into())
            } else {
                Ok(())
            },
        )
    })
    .await;
    assert_eq!(rig.router.selects.load(Ordering::SeqCst), 1);
    let before: Value = serde_json::from_str(
        &rig.store
            .routing(STORE_ROLE, "routed")
            .unwrap()
            .unwrap()
            .runs,
    )
    .unwrap();
    assert_eq!(before["reselected"], false);
    let (store, router) = (rig.store.clone(), rig.router.clone());
    drop(rig);
    let rig = routed_rig_with_store(store, router);
    rig.provider
        .script("auth", vec![Script::Answer(answered(Some(10)))]);
    rig.service.recover_routing().await.unwrap();
    let reply = call(&rig, &params).await.unwrap();
    let value: Value = serde_json::from_str(reply.get()).unwrap();
    assert_eq!(value["decision_id"], "decision-1");
    assert!(value["items"][0].get("answers").is_some());
    assert_eq!(rig.router.selects.load(Ordering::SeqCst), 2);
    assert_eq!(rig.router.reports.lock().unwrap().len(), 2);
    rig.router.clear();
    assert_eq!(call(&rig, &params).await.unwrap().get(), reply.get());
    assert!(rig.router.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn routed_refused_reselection_account_keeps_original_choice_and_diagnostics() {
    for run_reports in [false, true] {
        let rig = routed_rig(run_reports, true);
        rig.router.refuse_reselection.store(true, Ordering::SeqCst);
        rig.provider.script("auth", vec![Script::Status(401, None)]);
        let params = routed_request(&["auth", "never"]);
        let refusal = call(&rig, &params).await.unwrap_err();
        assert_eq!(refusal["code"], "routing_refused");
        assert_eq!(refusal["detail"]["routing_code"], "account_not_authorized");
        assert_eq!(refusal["detail"]["class"], "permanent");
        assert_eq!(rig.provider.calls("auth"), 2);
        assert_eq!(rig.provider.calls("never"), 0);
        let reports = rig.router.bodies("route.report");
        assert_eq!(reports.len(), 2);
        assert_eq!(reports[1]["decision_id"], "decision-1");
        assert_eq!(reports[1]["outcome"], "launch_failed");
        assert_eq!(
            rig.router
                .bodies("route.account_observe")
                .iter()
                .filter(|body| body["kind"] == "admission_started")
                .count(),
            1
        );
        let record = rig.store.routing(STORE_ROLE, "routed").unwrap().unwrap();
        let journal: Value = serde_json::from_str(&record.runs).unwrap();
        assert_eq!(journal["reselected"], true);
        assert_eq!(
            journal["reselection"]["choice"]["select"]["decision_id"],
            "decision-1"
        );
        assert_eq!(
            journal["reselection"]["refusal"]["detail"]["routing_code"],
            "account_not_authorized"
        );
        assert_eq!(journal["reselection"]["finished"], true);
        assert_eq!(
            serde_json::from_str::<Value>(&record.choice).unwrap()["select"]["decision_id"],
            "decision-0"
        );
        rig.router.clear();
        rig.service.recover_routing().await.unwrap();
        assert_eq!(call(&rig, &params).await.unwrap_err(), refusal);
        assert!(rig.router.calls.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn routed_refused_readmission_without_run_reports_adds_no_final_penalty() {
    let rig = routed_rig(false, true);
    rig.provider
        .script("retry", vec![Script::Status(503, None)]);
    let params = routed_request(&["retry"]);
    call(&rig, &params).await.unwrap();
    *rig.router.admission_refused.lock().unwrap() =
        Some(("account_not_authorized", "credential not authorized"));
    let refusal = call(&rig, &params).await.unwrap_err();
    assert_eq!(refusal["code"], "routing_refused");
    rig.service.recover_routing().await.unwrap();
    assert!(rig.router.bodies("route.report").is_empty());
}

#[tokio::test]
async fn routed_restart_after_account_failure_before_cascade_cannot_dispatch_again() {
    for answered_first in [true, false] {
        let directory = broca_test_support::scratch_root().join(format!(
            "routed-cascade-{}-{answered_first}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("system-one.db");
        let rig = routed_rig_with_store(
            Arc::new(SystemOneStore::open(&path).unwrap()),
            Router::new(true, true),
        );
        let database = rusqlite::Connection::open(&path).unwrap();
        let fault_choice = if answered_first {
            "decision-0"
        } else {
            "decision-1"
        };
        database.execute_batch(&format!("CREATE TRIGGER interrupt_cascade BEFORE INSERT ON item_outcome WHEN NEW.item_index=2 AND (SELECT json_extract(routing_choice, '$.select.decision_id') FROM batch WHERE batch_id=NEW.batch_id)='{fault_choice}' BEGIN SELECT RAISE(ABORT, 'interrupted account-error cascade'); END;")).unwrap();
        let states = if answered_first {
            rig.provider
                .script("done", vec![Script::Answer(answered(Some(10)))]);
            vec!["done", "auth", "never"]
        } else {
            vec!["auth", "never1", "never"]
        };
        rig.provider.script("auth", vec![Script::Status(401, None)]);
        let params = routed_request(&states);
        assert_eq!(
            call(&rig, &params).await.unwrap_err()["code"],
            "internal_error"
        );
        assert_eq!(
            rig.store
                .lookup(STORE_ROLE, "routed")
                .unwrap()
                .unwrap()
                .outcomes
                .len(),
            2
        );
        let router = rig.router.clone();
        assert_eq!(
            router.selects.load(Ordering::SeqCst),
            if answered_first { 1 } else { 2 }
        );
        drop(rig);
        database
            .execute_batch("DROP TRIGGER interrupt_cascade")
            .unwrap();
        drop(database);
        let recovered = routed_rig_with_store(
            Arc::new(SystemOneStore::open(&path).unwrap()),
            router.clone(),
        );
        router.clear();
        recovered.service.recover_routing().await.unwrap();
        let reply = call(&recovered, &params).await.unwrap();
        let value: Value = serde_json::from_str(reply.get()).unwrap();
        assert_eq!(value["decision_id"], fault_choice);
        assert_eq!(value["items"][2]["error"]["code"], "auth_failed");
        if answered_first {
            assert!(value["items"][0].get("answers").is_some());
        }
        assert!(router.bodies("route.select").is_empty());
        assert!(router.bodies("route.account_for").is_empty());
        assert!(recovered.provider.calls.lock().unwrap().is_empty());
        router.clear();
        assert_eq!(call(&recovered, &params).await.unwrap().get(), reply.get());
        assert!(router.calls.lock().unwrap().is_empty());
        drop(recovered);
        std::fs::remove_dir_all(directory).unwrap();
    }
}

#[path = "routed/exits.rs"]
mod exits;
