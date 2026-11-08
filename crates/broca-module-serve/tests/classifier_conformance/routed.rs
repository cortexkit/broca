//! Real-route routing and vault fixtures for the opt-in classifier conformance run.

use super::*;
use cortexkit_role_classifier_conformance::{RoutedDecision, RoutingObservations, RoutingOutcome};
use cortexkit_role_routing::{
    describe::{ConsumerEntry, RoleDescribe},
    select::{PinOutcome, RouteRef, RunnerAuth, RunnerRef, SelectAnswer},
};
use subc_client_rs::{BindDecision, HandlerOutcome, ModuleHandler, RequestCtx, RouteBindRequest};

const ROUTER: &str = "classifier-routing";
const POOL: &str = "classifier-keys";
const ACCOUNTS: [&str; 2] = ["account-1", "account-2"];

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn describe() -> RoleDescribe {
    let mut describe = RoleDescribe::new(
        vec![ConsumerEntry {
            id: "classifier".into(),
            priced_only: true,
        }],
        false,
    )
    .with_capability("report_outcomes")
    .with_capability("report_usage_run_id")
    .with_capability("observe_backoff")
    .with_capability("observe_credential_refused");
    describe.requirement_fields.extend([
        "questions".into(),
        "input_tokens".into(),
        "max_cost_usd".into(),
    ]);
    cortexkit_role_routing::describe::check_describe(&describe).unwrap();
    describe
}

struct Admission {
    request: Value,
    answer: Value,
    choice: RoutedDecision,
    finished: bool,
}

#[derive(Default)]
struct Inner {
    admission_refusal: Option<cortexkit_role_classifier_conformance::RoutingRefusalFixture>,
    renewal_refusal: Option<cortexkit_role_classifier_conformance::RoutingRefusalFixture>,
    renewal_due: Option<u64>,
    script: Option<Vec<RoutingOutcome>>,
    cursor: usize,
    next_decision: usize,
    observations: RoutingObservations,
    decisions: HashMap<String, RoutedDecision>,
    admissions: HashMap<String, Admission>,
    events: HashMap<String, Value>,
    reports: HashMap<(String, Option<String>), Value>,
    calls: Vec<Value>,
}

#[derive(Default)]
pub(super) struct State(Mutex<Inner>);

impl State {
    pub(super) fn script(&self, outcomes: Vec<RoutingOutcome>) {
        assert!(!outcomes.is_empty());
        let mut inner = self.0.lock().unwrap();
        inner.script = Some(outcomes);
        inner.cursor = 0;
        inner.observations = RoutingObservations::default();
        inner.admission_refusal = None;
        inner.renewal_refusal = None;
        inner.renewal_due = None;
    }

    pub(super) fn admission_refusal(
        &self,
        refusal: cortexkit_role_classifier_conformance::RoutingRefusalFixture,
    ) {
        self.script(vec![RoutingOutcome::Refused {
            code: refusal.code.clone(),
            message: refusal.message.clone(),
        }]);
        self.0.lock().unwrap().admission_refusal = Some(refusal);
    }

    pub(super) fn renewal_refusal(
        &self,
        refusal: cortexkit_role_classifier_conformance::RoutingRefusalFixture,
    ) {
        let mut inner = self.0.lock().unwrap();
        assert!(inner.script.is_some());
        inner.renewal_refusal = Some(refusal);
    }

    fn renewal_deadline(&self, request: &Value) -> Option<u64> {
        let mut inner = self.0.lock().unwrap();
        if request["method"] == "route.account_observe"
            && request["params"]["kind"] == "attempt"
            && inner.observations.provider_requests.len() == 1
        {
            inner.renewal_due.take()
        } else {
            None
        }
    }

    async fn await_renewal_window(&self, request: &Value) {
        if let Some(due) = self.renewal_deadline(request) {
            // Attempt observations are flushed after the paid answer commits. Hold its
            // acknowledgement until the lease expires, so the next item must renew before dispatch.
            broca_test_support::wait_until("fixture lease renewal window", || async {
                if now() >= due {
                    Ok(())
                } else {
                    Err(format!("renewal due at {due}, now {}", now()))
                }
            })
            .await;
        }
    }

    pub(super) fn finish(&self) {
        let mut inner = self.0.lock().unwrap();
        inner.script = None;
        inner.admission_refusal = None;
        inner.renewal_refusal = None;
        inner.renewal_due = None;
    }

    pub(super) fn observations(&self) -> RoutingObservations {
        self.0.lock().unwrap().observations.clone()
    }

    pub(super) fn provider_request(&self, head: &str, path: &str, request: &Value) {
        assert_eq!(path, "/v1/systemone", "routed fixture only serves TypeSafe");
        let token = head
            .lines()
            .find_map(|line| line.strip_prefix("authorization: bearer "))
            .expect("real provider requests must load a pooled key from the vault");
        let credential = token
            .strip_prefix("synthetic-classifier-key-")
            .expect("unknown provider token");
        let mut inner = self.0.lock().unwrap();
        let mut admitted = inner
            .admissions
            .values()
            .filter(|admission| !admission.finished && admission.choice.account == credential);
        let choice = admitted
            .next()
            .expect("provider sent without an active admitted credential")
            .choice
            .clone();
        assert!(
            admitted.all(|admission| admission.choice == choice),
            "HTTP credential ambiguously names multiple decisions"
        );
        let model = format!(
            "typesafe/{}",
            request["model"]
                .as_str()
                .expect("HTTP request names its model")
        );
        assert_eq!(
            model, choice.model,
            "HTTP model differs from the credential's admitted decision"
        );
        // Count every HTTP request, including the single same-account authentication
        // retry permitted by classifier §6.8. Replay must add no request to this log.
        if inner.script.is_some() {
            inner.observations.provider_requests.push(RoutedDecision {
                model,
                account: credential.into(),
                decision_id: choice.decision_id,
            });
        }
    }

    fn request(&self, request: Value) -> FixtureReply {
        let mut inner = self.0.lock().unwrap();
        inner.calls.push(request.clone());
        let params = &request["params"];
        let result = match request["method"].as_str().unwrap() {
            "role.describe" => serde_json::to_value(describe()).unwrap(),
            "route.select" => {
                let query = cortexkit_role_routing::select::parse_select_request_serving(
                    params,
                    &describe().capabilities,
                )
                .unwrap();
                assert_eq!(query.consumer, "classifier");
                assert_eq!(
                    query.runners.as_deref(),
                    Some(["broca".to_owned()].as_slice())
                );
                let outcome = if let Some(script) = &inner.script {
                    let outcome = script[inner.cursor.min(script.len() - 1)].clone();
                    inner.cursor += 1;
                    inner.observations.selections += 1;
                    outcome
                } else {
                    inner.next_decision += 1;
                    RoutingOutcome::Selected(RoutedDecision {
                        model: query.pin.clone().unwrap_or_else(|| JEV.into()),
                        account: ACCOUNTS[0].into(),
                        decision_id: format!("normal-{}", inner.next_decision),
                    })
                };
                let choice = match outcome {
                    RoutingOutcome::Selected(choice) => choice,
                    RoutingOutcome::Refused { code, message } => {
                        if let Some(refusal) = &inner.admission_refusal {
                            return FixtureReply::Refused(refusal.clone());
                        }
                        return FixtureReply::Error { code, message };
                    }
                    RoutingOutcome::Unavailable => return FixtureReply::Unavailable,
                };
                assert!(ACCOUNTS.contains(&choice.account.as_str()));
                let (provider, model) = choice.model.split_once('/').unwrap();
                let route = RouteRef::new(provider, model);
                let mut answer = SelectAnswer::new(
                    &choice.decision_id,
                    route.clone(),
                    RunnerRef::new("broca", route).with_auth(RunnerAuth {
                        pool: Some(POOL.into()),
                        credential_id: Some(choice.account.clone()),
                    }),
                    1,
                    1,
                    now() + 600_000,
                    "scripted classifier decision",
                );
                if query.pin.is_some() {
                    answer = answer.with_pin_outcome(PinOutcome::Pinned);
                }
                cortexkit_role_routing::select::check_answer(&query, &answer).unwrap();
                if let Some(previous) = inner
                    .decisions
                    .insert(choice.decision_id.clone(), choice.clone())
                {
                    assert_eq!(previous, choice, "decision ids cannot change their choice");
                }
                serde_json::to_value(answer).unwrap()
            }
            "route.account_for" => {
                cortexkit_role_routing::account::parse_account_for_request(params).unwrap();
                let id = params["admission_id"].as_str().unwrap();
                if let Some(admission) = inner.admissions.get(id) {
                    assert_eq!(
                        admission.request, *params,
                        "admission replay changed its body"
                    );
                    return FixtureReply::Response(admission.answer.clone());
                }
                let decision = params["decision_id"].as_str().unwrap();
                let choice = inner
                    .decisions
                    .get(decision)
                    .expect("admission must name an existing decision")
                    .clone();
                assert_eq!(params["provider"], "typesafe");
                assert_eq!(
                    params["model"],
                    choice.model.strip_prefix("typesafe/").unwrap()
                );
                let renewing = params["mode"] == "renew";
                if renewing {
                    if let Some(refusal) = &inner.renewal_refusal {
                        return FixtureReply::Refused(refusal.clone());
                    }
                }
                let pin_id = if renewing {
                    let old = inner
                        .admissions
                        .values_mut()
                        .find(|admission| {
                            !admission.finished
                                && admission.answer["pin"]["id"] == params["expected_pin"]["id"]
                                && admission.answer["pin"]["generation"]
                                    == params["expected_pin"]["generation"]
                        })
                        .expect("renewal must bind the latest live generation");
                    assert_eq!(old.choice, choice);
                    old.finished = true;
                    old.answer["pin"]["id"].as_str().unwrap().to_owned()
                } else {
                    assert_eq!(params["pool"], POOL);
                    assert!(params["candidates"]["accounts"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|candidate| candidate["credential_id"] == choice.account));
                    format!("pin-{id}")
                };
                assert_eq!(params["requested_account"]["credential_id"], choice.account);
                let generation = params["expected_pin"]["generation"]
                    .as_u64()
                    .map_or(1, |old| old + 1);
                let lease_ms = if inner.renewal_refusal.is_some() {
                    1_200
                } else {
                    120_000
                };
                let admitted_at = now();
                if inner.renewal_refusal.is_some() {
                    inner.renewal_due = Some(admitted_at + lease_ms);
                }
                let answer = json!({"admission":{"id":id,"state":"admitted"},
                    "account":{"provider":"typesafe","method":"apikey","account_id":choice.account,"credential_id":choice.account},
                    "pin":{"id":pin_id,"generation":generation,"affinity_expires_at_ms":admitted_at+3_600_000,"admission_valid_until_ms":admitted_at+120_000,"lease_valid_until_ms":admitted_at+lease_ms,"mode":if renewing {"renew"} else {"admit"}},
                    "reason":{"code":if renewing {"renewed"} else {"headroom"},"decision_id":decision,"policy_version":"fixture","quota_age_band":"fresh"}});
                let _: cortexkit_role_routing::account::AccountForAnswer =
                    serde_json::from_value(answer.clone()).unwrap();
                inner.admissions.insert(
                    id.into(),
                    Admission {
                        request: params.clone(),
                        answer: answer.clone(),
                        choice,
                        finished: false,
                    },
                );
                answer
            }
            "route.account_observe" => {
                cortexkit_role_routing::observe::parse_account_observe_request_serving(
                    params,
                    &describe().capabilities,
                )
                .unwrap();
                let id = params["event_id"].as_str().unwrap();
                let duplicate = if let Some(previous) = inner.events.get(id) {
                    assert_eq!(previous, params, "event replay changed its body");
                    true
                } else {
                    false
                };
                if params["kind"] == "transient_backoff" && !duplicate {
                    let admission = inner
                        .admissions
                        .get(params["admission_id"].as_str().unwrap())
                        .expect("backoff must name an admitted run");
                    assert!(
                        !admission.finished,
                        "transient_backoff must precede run_finished"
                    );
                    assert!(id.ends_with(":backoff"));
                    assert!(params["retry_at_ms"].as_u64().is_some());
                }
                let bound = if let Some(admission) = inner
                    .admissions
                    .get_mut(params["admission_id"].as_str().unwrap())
                {
                    if params["kind"] == "run_finished" {
                        admission.finished = true;
                    }
                    true
                } else {
                    false
                };
                inner.events.insert(id.into(), params.clone());
                json!({"observation":{"id":id,"duplicate":duplicate,"bound":bound,"effects":[]}})
            }
            "route.report" => {
                cortexkit_role_routing::report::parse_report_request_serving(
                    params,
                    &describe().capabilities,
                )
                .unwrap();
                let decision = params["decision_id"].as_str().unwrap();
                assert!(inner.decisions.contains_key(decision));
                let key = (
                    decision.to_owned(),
                    params["usage"]["run_id"].as_str().map(str::to_owned),
                );
                if let Some(previous) = inner.reports.get(&key) {
                    if previous != params {
                        return FixtureReply::Error {
                            code: "report_conflict".into(),
                            message: "report identity changed".into(),
                        };
                    }
                }
                inner.reports.insert(key, params.clone());
                json!({"decision_id":decision,"report_id":format!("report-{decision}"),"outcome":params["outcome"]})
            }
            method => panic!("unexpected routing method {method}"),
        };
        FixtureReply::Response(result)
    }
}

enum FixtureReply {
    Refused(cortexkit_role_classifier_conformance::RoutingRefusalFixture),
    Response(Value),
    Error { code: String, message: String },
    Unavailable,
}

pub(super) fn write_daemon_config(path: PathBuf, mut config: Value, routing: bool) {
    if routing {
        assert!(config["modules"]["broca"]["env"]
            .as_object_mut()
            .unwrap()
            .remove("BROCA_PROVIDER_BASE_URL")
            .is_some());
    }
    std::fs::write(path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();
}

pub(super) fn configure(path: &Path) {
    let mut config: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    config["classifier_routing"] = json!({"enabled":true});
    config["account_selection"] = json!({"routing_module":ROUTER});
    config["auth_methods"] = json!({"defaults":{"typesafe":"apikey"}});
    std::fs::write(path, serde_json::to_vec(&config).unwrap()).unwrap();
}

pub(super) struct Services {
    pub(super) state: Arc<State>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for Services {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

fn manifest(id: &str, ops: &[&str]) -> subc_protocol::manifest::ModuleManifest {
    use subc_protocol::manifest::{
        Concurrency, ManagementOperation, ManagementOperationKind, ProviderRole,
    };
    subc_protocol::manifest::ModuleManifest::builder(id, env!("CARGO_PKG_VERSION"))
        .provides(vec![ProviderRole::ManagementSurface {
            operations: ops
                .iter()
                .map(|name| ManagementOperation {
                    name: (*name).into(),
                    kind: ManagementOperationKind::Mutate,
                    description: None,
                })
                .collect(),
            config_schema: json!({}),
            observability: vec![],
            identity_scope: vec![],
            concurrency: Concurrency::ModuleManaged,
        }])
        .build()
}

async fn register_service<H: ModuleHandler + 'static>(
    connection: &Path,
    id: &str,
    ops: &[&str],
    handler: H,
) -> tokio::task::JoinHandle<()> {
    let (_, serve) = subc_client_rs::serve_with_handle(connection, manifest(id, ops), handler)
        .await
        .unwrap_or_else(|error| panic!("register {id}: {error}"));
    tokio::spawn(async move {
        serve.await.expect("conformance service transport");
    })
}

struct Router {
    state: Arc<State>,
    callers: Mutex<HashMap<(u16, u32), bool>>,
}

#[async_trait]
impl ModuleHandler for Router {
    async fn on_bind(&self, request: &RouteBindRequest) -> BindDecision {
        let broca = matches!(&request.principal, Some(subc_protocol::Principal::Reserved { module_id }) if module_id == "broca");
        self.callers
            .lock()
            .unwrap()
            .insert((request.handle.channel, request.handle.epoch), broca);
        if broca {
            BindDecision::accept()
        } else {
            BindDecision::reject(
                "unauthorized",
                "only the authenticated runner may use routing",
            )
        }
    }

    async fn handle(&self, context: RequestCtx, body: Vec<u8>) -> HandlerOutcome {
        let route = context.route_handle();
        assert_eq!(
            self.callers
                .lock()
                .unwrap()
                .get(&(route.channel, route.epoch)),
            Some(&true)
        );
        let request: Value = serde_json::from_slice(&body).unwrap();
        self.state.await_renewal_window(&request).await;
        match self.state.request(request) {
            FixtureReply::Response(result) => {
                HandlerOutcome::Response(serde_json::to_vec(&json!({"result":result})).unwrap())
            }
            FixtureReply::Error { code, message } => HandlerOutcome::Error { code, message },
            FixtureReply::Refused(refusal) => HandlerOutcome::ErrorWithDetail {
                code: refusal.code,
                message: refusal.message,
                detail: refusal.detail,
            },
            // Not answering is a real transport timeout, not an answered routing refusal.
            // The fixture's service task is aborted when the rig is dropped.
            FixtureReply::Unavailable => std::future::pending().await,
        }
    }
}

fn credentials() -> Vec<Value> {
    ACCOUNTS.iter().map(|id| json!({"id":id,"categories":["llm-provider"],"type":"apikey","serves":["typesafe"],
        "provider_ids":["typesafe"],"auth_method":"apikey","state":"active","record_version":1,"operations":["read"]})).collect()
}

struct Vault;

impl Vault {
    fn request(request: &Value) -> Value {
        match request["method"].as_str().unwrap() {
            "credential.list_scoped" => {
                json!({"credentials":credentials(),"grants":1,"grant_tuples":[],"view":"classifier-fixture"})
            }
            "credential.get_scoped" => {
                let id = request["params"]["credential_id"].as_str().unwrap();
                assert!(ACCOUNTS.contains(&id));
                json!({"payload":format!("synthetic-classifier-key-{id}").as_bytes(),"expires_at_ms":null,"record_version":1,"credential_id":id,"account_id":null})
            }
            method => panic!("unsupported classifier vault method {method}"),
        }
    }
}

#[async_trait]
impl ModuleHandler for Vault {
    async fn on_bind(&self, request: &RouteBindRequest) -> BindDecision {
        if matches!(&request.principal, Some(subc_protocol::Principal::Reserved { module_id }) if module_id == "broca")
        {
            BindDecision::accept()
        } else {
            BindDecision::reject("unauthorized", "fixture grants cover broca only")
        }
    }

    async fn handle(&self, _: RequestCtx, body: Vec<u8>) -> HandlerOutcome {
        let result = Self::request(&serde_json::from_slice(&body).unwrap());
        HandlerOutcome::Response(serde_json::to_vec(&json!({"result":result})).unwrap())
    }
}

pub(super) async fn register(connection: &Path, state: Arc<State>) -> Services {
    let mut services = Services {
        state: state.clone(),
        tasks: vec![],
    };
    services.tasks.push(
        register_service(
            connection,
            ROUTER,
            &[
                "role.describe",
                "route.select",
                "route.account_for",
                "route.account_observe",
                "route.report",
            ],
            Router {
                state,
                callers: Default::default(),
            },
        )
        .await,
    );
    services.tasks.push(
        register_service(
            connection,
            "claustrum",
            &["credential.list_scoped", "credential.get_scoped"],
            Vault,
        )
        .await,
    );
    services
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(state: &State, method: &str, params: Value) -> Value {
        match state.request(json!({"method":method,"params":params})) {
            FixtureReply::Response(result) => result,
            FixtureReply::Error { code, message } => panic!("{code}: {message}"),
            FixtureReply::Refused(refusal) => panic!("{}: {}", refusal.code, refusal.message),
            FixtureReply::Unavailable => panic!("unexpected timeout"),
        }
    }

    fn admit(state: &State) -> Value {
        call(
            state,
            "route.select",
            json!({"consumer":"classifier","requirements":{"iq":0},"runners":["broca"],"pin":JEV}),
        );
        call(
            state,
            "route.account_for",
            json!({"provider":"typesafe","method":"apikey","model":"jev-1.13.0",
            "conversation_key":"fixture","admission_id":"first","mode":"admit","decision_id":"scripted",
            "pool":POOL,"requested_account":{"credential_id":"account-1"},
            "candidates":{"accounts":[{"account_id":"account-1","credential_id":"account-1"}],"view_digest":"fixture"},"initial_input_bytes":4}),
        )
    }

    fn scripted_state() -> State {
        let state = State::default();
        state.script(vec![RoutingOutcome::Selected(RoutedDecision {
            model: JEV.into(),
            account: "account-1".into(),
            decision_id: "scripted".into(),
        })]);
        state
    }

    #[test]
    fn routed_fixture_transient_backoff_is_bound_before_release_and_replays_after() {
        let state = scripted_state();
        admit(&state);
        let backoff = json!({"event_id":"lineage:3:backoff","kind":"transient_backoff",
            "admission_id":"first","caller":{"runner":"broca"},"retry_at_ms":47_000});
        let observed = call(&state, "route.account_observe", backoff.clone());
        assert_eq!(observed["observation"]["bound"], true);
        assert_eq!(observed["observation"]["duplicate"], false);
        assert!(!state.0.lock().unwrap().admissions["first"].finished);
        call(
            &state,
            "route.account_observe",
            json!({"event_id":"lineage:3","kind":"run_finished","admission_id":"first",
                "caller":{"runner":"broca"},"run_id":"run-1","terminal_reason":"error",
                "upstream_origin":"upstream"}),
        );
        assert!(state.0.lock().unwrap().admissions["first"].finished);
        let replay = call(&state, "route.account_observe", backoff);
        assert_eq!(replay["observation"]["duplicate"], true);
        assert_eq!(state.0.lock().unwrap().events.len(), 2);
    }

    #[test]
    #[should_panic(expected = "transient_backoff must precede run_finished")]
    fn routed_fixture_transient_backoff_rejects_a_new_observation_after_release() {
        let state = scripted_state();
        admit(&state);
        call(
            &state,
            "route.account_observe",
            json!({"event_id":"lineage:3","kind":"run_finished","admission_id":"first",
                "caller":{"runner":"broca"},"run_id":"run-1","terminal_reason":"error",
                "upstream_origin":"upstream"}),
        );
        call(
            &state,
            "route.account_observe",
            json!({"event_id":"lineage:3:backoff","kind":"transient_backoff",
                "admission_id":"first","caller":{"runner":"broca"},"retry_at_ms":47_000}),
        );
    }

    #[test]
    fn routed_fixture_accepts_account_for_without_method() {
        let state = scripted_state();
        call(
            &state,
            "route.select",
            json!({"consumer":"classifier","requirements":{"iq":0},"runners":["broca"],"pin":JEV}),
        );
        let answer = call(
            &state,
            "route.account_for",
            json!({"provider":"typesafe","model":"jev-1.13.0",
            "conversation_key":"fixture","admission_id":"first","mode":"admit","decision_id":"scripted",
            "pool":POOL,"requested_account":{"credential_id":"account-1"},
            "candidates":{"accounts":[{"account_id":"account-1","credential_id":"account-1"}],"view_digest":"fixture"},"initial_input_bytes":4}),
        );
        assert_eq!(answer["account"]["credential_id"], "account-1");
    }

    #[tokio::test]
    async fn routed_fixture_counts_every_real_http_attempt() {
        let routing = Arc::new(scripted_state());
        admit(&routing);
        let provider = Arc::new(StandIn {
            routing: Some(routing.clone()),
            ..Default::default()
        });
        let url = start_stand_in(provider.clone()).await;
        let client = reqwest::Client::new();
        for _ in 0..2 {
            let reply = client
                .post(format!("{url}/v1/systemone"))
                .bearer_auth("synthetic-classifier-key-account-1")
                .json(&json!({"model":"jev-1.13.0","state":"retry-probe"}))
                .send()
                .await
                .unwrap();
            assert_eq!(
                reply.status().as_u16(),
                500,
                "unscripted requests are still counted"
            );
        }
        assert_eq!(provider.received.lock().unwrap().len(), 2);
        let observations = routing.observations();
        assert_eq!(observations.selections, 1);
        assert_eq!(
            observations.provider_requests.len(),
            2,
            "same-account retry is a second HTTP request"
        );
        assert_eq!(
            observations.provider_requests[0],
            observations.provider_requests[1]
        );
    }

    #[test]
    fn routed_fixture_renews_replays_and_releases_latest_admission() {
        let state = scripted_state();
        let first = admit(&state);
        let request = json!({"provider":"typesafe","method":"apikey","model":"jev-1.13.0","conversation_key":"fixture",
            "admission_id":"renewed","mode":"renew","decision_id":"scripted",
            "expected_pin":{"id":first["pin"]["id"],"generation":first["pin"]["generation"]},
            "requested_account":{"credential_id":"account-1"},"initial_input_bytes":4});
        let renewal = call(&state, "route.account_for", request.clone());
        assert_eq!(call(&state, "route.account_for", request), renewal);
        assert_eq!(renewal["pin"]["generation"], 2);
        assert_eq!(renewal["pin"]["id"], first["pin"]["id"]);
        let event = json!({"event_id":"finish","kind":"run_finished","admission_id":"renewed","run_id":"run","observed_at_ms":now()});
        assert_eq!(
            call(&state, "route.account_observe", event.clone())["observation"]["duplicate"],
            false
        );
        assert_eq!(
            call(&state, "route.account_observe", event)["observation"]["duplicate"],
            true
        );
        assert!(state
            .0
            .lock()
            .unwrap()
            .admissions
            .values()
            .all(|admission| admission.finished));
    }

    #[test]
    fn routed_fixture_refusal_hooks_preserve_detail_and_renew_after_paid_item() {
        use cortexkit_role_classifier_conformance::RoutingRefusalFixture;
        let refusal =
            RoutingRefusalFixture::new("account_hold", "held").with_detail(json!({"until_ms":123}));
        let state = Arc::new(scripted_state());
        state.admission_refusal(refusal.clone());
        let reply = state.request(json!({"method":"route.select","params":{"consumer":"classifier","requirements":{"iq":0},"runners":["broca"]}}));
        let FixtureReply::Refused(answer) = reply else {
            panic!("hook did not refuse");
        };
        assert_eq!(answer.code, "account_hold");
        assert_eq!(answer.detail, json!({"until_ms":123}));

        state.script(vec![RoutingOutcome::Selected(RoutedDecision {
            model: JEV.into(),
            account: ACCOUNTS[0].into(),
            decision_id: "scripted".into(),
        })]);
        state.renewal_refusal(refusal);
        let admitted = admit(&state);
        state.provider_request(
            &format!(
                "authorization: bearer synthetic-classifier-key-{}\r\n",
                ACCOUNTS[0]
            ),
            "/v1/systemone",
            &json!({"model":"jev-1.13.0"}),
        );
        assert_eq!(
            state.renewal_deadline(
                &json!({"method":"route.account_observe","params":{"kind":"admission_started"}})
            ),
            None
        );
        assert_eq!(
            state.renewal_deadline(
                &json!({"method":"route.account_observe","params":{"kind":"attempt"}})
            ),
            Some(admitted["pin"]["lease_valid_until_ms"].as_u64().unwrap())
        );
        assert_eq!(
            state.renewal_deadline(
                &json!({"method":"route.account_observe","params":{"kind":"attempt"}})
            ),
            None
        );
        let mut request = state
            .0
            .lock()
            .unwrap()
            .admissions
            .values()
            .next()
            .unwrap()
            .request
            .clone();
        request["admission_id"] = json!("renewal-refused");
        request["mode"] = json!("renew");
        request["expected_pin"] =
            json!({"id":admitted["pin"]["id"],"generation":admitted["pin"]["generation"]});
        let FixtureReply::Refused(answer) =
            state.request(json!({"method":"route.account_for","params":request}))
        else {
            panic!("renewal hook did not refuse");
        };
        assert_eq!(answer.detail, json!({"until_ms":123}));
        assert_eq!(state.observations().provider_requests.len(), 1);
        state.finish();
        assert!(state.0.lock().unwrap().renewal_refusal.is_none());
    }

    #[test]
    fn routed_fixture_report_is_idempotent_and_conflicts_are_final() {
        let state = scripted_state();
        admit(&state);
        let report = json!({"decision_id":"scripted","outcome":"completed","usage":{"run_id":"batch","cost_usd":0.01}});
        let first = call(&state, "route.report", report.clone());
        assert_eq!(call(&state, "route.report", report.clone()), first);
        let mut changed = report;
        changed["usage"]["cost_usd"] = json!(0.02);
        assert!(
            matches!(state.request(json!({"method":"route.report","params":changed})), FixtureReply::Error { code, .. } if code == "report_conflict")
        );
    }

    #[test]
    fn routed_fixture_writes_real_auth_config_and_preserves_legacy_bytes() {
        let root = broca_test_support::scratch_root();
        std::fs::create_dir_all(&root).unwrap();
        let temp = root.join(format!("classifier-routing-config-{}", std::process::id()));
        broca_test_support::remove_rig_dir(&temp);
        std::fs::create_dir_all(&temp).unwrap();
        let legacy = temp.join("legacy.json");
        let routed = temp.join("routed.json");
        let config =
            json!({"modules":{"broca":{"env":{"BROCA_PROVIDER_BASE_URL":"http://fixture/v1"}}}});
        let captured = b"{\n  \"modules\": {\n    \"broca\": {\n      \"env\": {\n        \"BROCA_PROVIDER_BASE_URL\": \"http://fixture/v1\"\n      }\n    }\n  }\n}";
        write_daemon_config(legacy.clone(), config.clone(), false);
        assert_eq!(std::fs::read(legacy).unwrap(), captured);
        write_daemon_config(routed.clone(), config, true);
        let stored: Value = serde_json::from_slice(&std::fs::read(routed).unwrap()).unwrap();
        assert!(stored["modules"]["broca"]["env"]
            .get("BROCA_PROVIDER_BASE_URL")
            .is_none());
        let broca = temp.join("broca.json");
        std::fs::write(&broca, b"{}").unwrap();
        configure(&broca);
        let stored: Value = serde_json::from_slice(&std::fs::read(broca).unwrap()).unwrap();
        assert_eq!(stored["classifier_routing"]["enabled"], true);
        assert_eq!(stored["account_selection"]["routing_module"], ROUTER);
        broca_test_support::remove_rig_dir(&temp);
    }

    #[test]
    fn routed_fixture_vault_lists_only_explicit_readable_api_keys() {
        let listing = Vault::request(&json!({"method":"credential.list_scoped","params":{}}));
        let rows = listing["credentials"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
        for row in rows {
            assert_eq!(row["provider_ids"], json!(["typesafe"]));
            assert_eq!(row["auth_method"], "apikey");
            assert!(
                row.get("account_id").is_none(),
                "pool uses public credential ids"
            );
            let loaded = Vault::request(
                &json!({"method":"credential.get_scoped","params":{"credential_id":row["id"]}}),
            );
            let bytes: Vec<u8> = serde_json::from_value(loaded["payload"].clone()).unwrap();
            assert_eq!(
                String::from_utf8(bytes).unwrap(),
                format!("synthetic-classifier-key-{}", row["id"].as_str().unwrap())
            );
        }
    }
}

fn expected_inapplicability(name: &str) -> Option<&'static str> {
    match name {
        "auth_failure_stops_the_call_unstored" => Some("routed_models stores account failures; covered by routed_partial_failure_keeps_model"),
        "auth_failure_on_first_call_refused" => Some("routed_models stores account failures; covered by routed_initial_account_failure_bounded"),
        "image_reference_follows_declaration" => Some("no served model takes images"),
        "image_reference_resolved_and_answered" | "image_reference_oversized_refused_like_inline"
            | "image_source_down_refused_transient" | "image_reference_identity_is_its_handle" => Some("no served model takes images"),
        _ => None,
    }
}

fn expected_verdict() -> SuiteVerdict {
    SuiteVerdict::PassedExceptNotApplicable {
        not_applicable: CHECKS
            .iter()
            .filter(|spec| expected_inapplicability(spec.name).is_some())
            .map(|spec| spec.name)
            .collect(),
    }
}

fn validate_report(
    report: &cortexkit_role_classifier_conformance::SuiteReport,
) -> Result<(), String> {
    if report.checks.len() != CHECKS.len() {
        return Err("kit omitted or added an unaccounted check".into());
    }
    let mut errors = vec![];
    for (check, spec) in report.checks.iter().zip(CHECKS) {
        if check.check != spec.name {
            errors.push(format!("expected {}, got {}", spec.name, check.check));
        }
        let expected = match expected_inapplicability(spec.name) {
            Some(reason) => CheckOutcome::NotApplicable {
                reason: reason.into(),
            },
            None => CheckOutcome::Passed,
        };
        if check.outcome != expected {
            errors.push(format!(
                "{}: {:?}; expected {:?}",
                check.check, check.outcome, expected
            ));
        }
    }
    if report.verdict != expected_verdict() {
        errors.push(format!("unexpected kit verdict: {:?}", report.verdict));
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("\n"))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn classifier_conformance_suite_passes_with_routing_enabled() {
    let rig = start_rig_options("routing", Served::Jev, None, true)
        .await
        .expect("the routing conformance run requires the real daemon; it must not silently skip");
    let report = run_suite(&Subject { rig: &rig }).await;
    for check in &report.checks {
        println!("{}: {:?}", check.check, check.outcome);
    }
    validate_report(&report)
        .unwrap_or_else(|failures| panic!("routing-enabled classifier conformance:\n{failures}"));
    assert_eq!(report.verdict, expected_verdict());
    let services = rig.routed.as_ref().unwrap();
    let calls = &services.state.0.lock().unwrap().calls;
    for method in [
        "route.select",
        "route.account_for",
        "route.account_observe",
        "route.report",
    ] {
        assert!(
            calls.iter().any(|call| call["method"] == method),
            "the real transport did not reach {method}"
        );
    }
}

#[cfg(test)]
mod coverage_tests {
    use super::*;
    use cortexkit_role_classifier_conformance::{CheckReport, SuiteReport};

    fn passing_report() -> SuiteReport {
        SuiteReport {
            verdict: expected_verdict(),
            checks: CHECKS
                .iter()
                .map(|spec| CheckReport {
                    check: spec.name,
                    outcome: expected_inapplicability(spec.name).map_or(
                        CheckOutcome::Passed,
                        |reason| CheckOutcome::NotApplicable {
                            reason: reason.into(),
                        },
                    ),
                })
                .collect(),
        }
    }

    #[test]
    fn routed_coverage_guard_requires_each_named_case_to_pass() {
        let report = passing_report();
        validate_report(&report).unwrap();
        for name in ROUTED_CASES {
            let mut report = report.clone();
            report
                .checks
                .iter_mut()
                .find(|check| check.check == name)
                .unwrap()
                .outcome = CheckOutcome::NotApplicable {
                reason: "routing hook unavailable".into(),
            };
            let failure = validate_report(&report).unwrap_err();
            assert!(failure.contains(name));
        }
    }

    #[test]
    fn routed_coverage_guard_rejects_missing_duplicate_and_unexpected_na() {
        let mut missing = passing_report();
        missing.checks.pop();
        assert!(validate_report(&missing).is_err());
        let mut duplicate = passing_report();
        duplicate.checks[0] = duplicate.checks[1].clone();
        assert!(validate_report(&duplicate).is_err());
        let mut skipped = passing_report();
        skipped.checks[0].outcome = CheckOutcome::NotApplicable {
            reason: "stand-in missing".into(),
        };
        assert!(validate_report(&skipped).is_err());
        let mut dishonest = passing_report();
        dishonest
            .checks
            .iter_mut()
            .find(|check| check.check == "image_reference_follows_declaration")
            .unwrap()
            .outcome = CheckOutcome::NotApplicable {
            reason: "daemon missing".into(),
        };
        assert!(validate_report(&dishonest).is_err());
    }
}
