use super::*;
use crate::llm_tools::held_images_tests::{stamp, Io, Marked};
use crate::llm_tools::{consent::Consent, held::HeldCalls, Asks, LlmTools};
use broca_protocol::{BindPrincipal, SendScope};
use broca_store::grants::{GrantsStore, HeldState};
use broca_subc::{ConsentRecord, ConsentState};
use std::collections::BTreeMap;
use tokio_util::sync::CancellationToken;

struct Unmarked;

#[async_trait]
impl Asks for Unmarked {
    async fn marked(&self, _: &str, _: Option<&str>) -> Option<broca_subc::PrivateContent> {
        Some(broca_subc::PrivateContent::unmarked())
    }

    async fn flow_providers(
        &self,
        _: &str,
    ) -> Result<broca_subc::FlowPolicy, broca_subc::FlowProvidersError> {
        Ok(broca_subc::FlowPolicy::Allowed(
            broca_subc::ApprovedFlowProviders {
                version: 1,
                code_hash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .into(),
                providers: vec![JEV.split_once('/').unwrap().0.into()],
            },
        ))
    }
}

struct ToolPin {
    provider: Arc<StandIn>,
    used: Arc<Mutex<Vec<String>>>,
    credential: String,
}

#[async_trait]
impl ClassifierProvider for ToolPin {
    fn egress_host(&self) -> String {
        "stand-in.invalid".into()
    }

    fn credential_id(&self) -> Option<String> {
        Some(self.credential.clone())
    }

    fn with_credential(
        &self,
        source: Arc<dyn CredentialSource>,
    ) -> Option<Arc<dyn ClassifierProvider>> {
        Some(Arc::new(Self {
            provider: self.provider.clone(),
            used: self.used.clone(),
            credential: source.credential_id().unwrap(),
        }))
    }

    async fn call(&self, body: String) -> ProviderReply {
        self.used.lock().unwrap().push(self.credential.clone());
        self.provider.call(body).await
    }
}

struct ToolRig {
    router: Arc<Router>,
    tools: Arc<LlmTools>,
    credentials: Arc<Mutex<Vec<String>>>,
    provider: Arc<StandIn>,
    grants: Arc<GrantsStore>,
    io: Arc<Io>,
    consent: Arc<Consent>,
}

impl ToolRig {
    fn new(asks: Arc<dyn Asks>) -> Self {
        let router = Router::new(true, true);
        let provider = Arc::new(StandIn::default());
        provider.script("sample", vec![Script::Answer(answered(Some(19)))]);
        let credentials = Arc::new(Mutex::new(vec![]));
        let pin = Arc::new(ToolPin {
            provider: provider.clone(),
            used: credentials.clone(),
            credential: "fixed-key".into(),
        });
        let clock = router.clone();
        let mut rows = catalog().0.clone();
        let jev = rows.get_mut(JEV).unwrap();
        jev.model_name = Some("Jev".into());
        jev.provider_name = Some("TypeSafe".into());
        let service = SystemOne::new(
            Arc::new(SystemOneStore::open_in_memory().unwrap()),
            "tool-process",
            Arc::new(Rows(rows)),
            vec![ModelPin::jev(pin)],
            Config::default(),
            Arc::default(),
        )
        .with_clock(Arc::new(move || clock.now() as i64))
        .with_routing(Routing {
            client: Arc::new(RoutingClient::new(router.clone(), Default::default())),
            credentials: Arc::new(Keys),
            module_id: "broca".into(),
        });
        let grants = Arc::new(GrantsStore::open_in_memory().unwrap());
        let held = Arc::new(HeldCalls::new(grants.clone(), "tool-process".into()));
        let io = Arc::new(Io::default());
        let clock = router.clone();
        let consent = Arc::new(
            Consent::new(grants.clone(), io.clone())
                .with_clock(Arc::new(move || clock.now() as i64)),
        );
        // The test records the answer directly, without racing the consent watcher.
        let stop = CancellationToken::new();
        stop.cancel();
        let tools = Arc::new(
            LlmTools::new(
                Arc::new(service.fork_for_tools()),
                crate::config::LlmToolsConfig {
                    private_content_providers: vec![],
                    ..Default::default()
                },
                asks,
            )
            .with_held(held)
            .with_consent(consent.clone(), stop),
        );
        Self {
            router,
            tools,
            credentials,
            provider,
            grants,
            io,
            consent,
        }
    }

    async fn call(
        &self,
        scope: &SendScope,
        key: &str,
        model: Option<&str>,
    ) -> Result<Box<RawValue>, Vec<u8>> {
        let mut arguments = json!({"questions":{"q":{"type":"noul","instructions":"true?"}},"item":{"state":"sample"},"max_cost_usd":1.0});
        if let Some(model) = model {
            arguments["model"] = json!(model);
        }
        let request = json!({"name":"llm_classify","arguments":arguments,"call_key":key});
        self.tools
            .answer(
                &serde_json::to_vec(&request).unwrap(),
                Some(&BindPrincipal::Reserved {
                    module_id: "basal".into(),
                }),
                scope,
            )
            .await
    }
}

fn flow() -> SendScope {
    let SendScope::Stamped(mut bind) = stamp() else {
        unreachable!()
    };
    bind.scope.flow_id = Some("flow-1".into());
    bind.scope.run_id = None;
    bind.scope.agent_id = None;
    SendScope::Stamped(bind)
}

fn sample(name: &str, bytes: &[u8]) {
    if let Some(dir) = std::env::var_os("BROCA_LLM_CLASSIFY_SAMPLE_DIR") {
        let dir = std::path::PathBuf::from(dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(name), bytes).unwrap();
    }
}

#[tokio::test]
async fn classify_tool_routes_basal_flow_and_program_run() {
    for (kind, scope) in [("flow", flow()), ("run", stamp())] {
        let rig = ToolRig::new(Arc::new(Unmarked));
        let reply = rig.call(&scope, "unpinned", None).await.unwrap();
        let answer: Value = serde_json::from_str(reply.get()).unwrap();
        assert_eq!(answer["model"], JEV);
        assert!(answer["cost_usd"].as_f64().unwrap() > 0.0);
        let selects = rig.router.bodies("route.select");
        assert_eq!(selects.len(), 1);
        assert_eq!(
            selects[0]["consumer"],
            format!("broca.classifier.basal.{kind}")
        );
        assert_eq!(selects[0]["requirements"]["iq"], 1);
        assert_eq!(selects[0]["requirements"]["questions"], 1);
        assert_eq!(selects[0]["runners"], json!(["broca"]));
        assert!(selects[0].get("pin").is_none());
        assert_eq!(&*rig.credentials.lock().unwrap(), &["apikey:a"]);
        let accounts = rig.router.bodies("route.account_for");
        assert_eq!(accounts.len(), 1);
        assert_eq!(accounts[0]["decision_id"], "decision-0");
        assert!(!rig.router.pin_held.load(Ordering::SeqCst));
        if kind == "flow" {
            sample(
                "routing-select.json",
                &rig.router.select_bytes.lock().unwrap()[0],
            );
            sample("answered.json", reply.get().as_bytes());
        }
    }
}

#[tokio::test]
async fn classify_tool_pin_keeps_its_static_credential_when_unpinned_routes() {
    let rig = ToolRig::new(Arc::new(Unmarked));
    let pinned = rig.call(&flow(), "pinned", Some(JEV)).await.unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(pinned.get()).unwrap()["model"],
        JEV
    );
    assert!(rig.router.calls.lock().unwrap().is_empty());
    let _ = rig.call(&flow(), "unpinned", None).await.unwrap();
    assert_eq!(
        &*rig.credentials.lock().unwrap(),
        &["fixed-key", "apikey:a"]
    );
    assert_eq!(rig.router.bodies("route.select").len(), 1);
}

#[tokio::test]
async fn classify_tool_replays_without_a_second_select_or_provider_call() {
    let rig = ToolRig::new(Arc::new(Unmarked));
    let scope = flow();
    let (first, concurrent) = tokio::join!(
        rig.call(&scope, "repeat", None),
        rig.call(&scope, "repeat", None)
    );
    let first = first.unwrap();
    assert_eq!(first.get(), concurrent.unwrap().get());
    assert_eq!(
        first.get(),
        rig.call(&scope, "repeat", None).await.unwrap().get()
    );
    assert_eq!(rig.provider.calls("sample"), 1);
    assert_eq!(rig.router.bodies("route.select").len(), 1);
    assert_eq!(rig.router.bodies("route.account_for").len(), 1);
}

#[tokio::test]
async fn classify_tool_private_content_holds_before_account_admission_and_resumes() {
    let rig = ToolRig::new(Arc::new(Marked));
    let scope = stamp();
    let key = "consent";
    let held = rig.call(&scope, key, None).await.unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(held.get()).unwrap()["held"]["reason"],
        "consent"
    );
    let row = rig
        .grants
        .recovering(rig.router.now() as i64)
        .unwrap()
        .into_iter()
        .find(|row| row.call_key == key)
        .expect("routed call held");
    assert_eq!(row.provider, JEV.split_once('/').unwrap().0);
    assert_eq!(row.model, JEV);
    assert_eq!(rig.call(&scope, key, None).await.unwrap().get(), held.get());
    assert_eq!(rig.router.bodies("route.select").len(), 1);
    assert!(rig.router.bodies("route.account_for").is_empty());
    assert!(rig.router.bodies("route.report").is_empty());
    assert_eq!(rig.provider.calls("sample"), 0);

    *rig.io.answer.lock().unwrap() = Some(ConsentRecord {
        elicitation_id: "card".into(),
        state: ConsentState::Answered,
        fields: BTreeMap::from([
            ("requester_ref".into(), json!(row.dedup_key)),
            ("answered_choice_id".into(), json!("allow_run")),
        ]),
    });
    rig.consent
        .wait_once(&row.dedup_key, rig.router.now() as i64)
        .await
        .unwrap();
    rig.tools.recover_authorized().unwrap();
    wait_for("approved classifier settles", || {
        rig.grants
            .lookup(&row.batch_id, rig.router.now() as i64)
            .unwrap()
            .is_some_and(|row| row.state == HeldState::Final)
    })
    .await;
    let reply = rig.call(&scope, key, None).await.unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(reply.get()).unwrap()["model"],
        JEV
    );
    assert_eq!(rig.router.bodies("route.select").len(), 1);
    assert_eq!(
        rig.router.bodies("route.account_for")[0]["decision_id"],
        "decision-0"
    );
    assert_eq!(rig.provider.calls("sample"), 1);
    assert!(!rig.router.pin_held.load(Ordering::SeqCst));
}

#[tokio::test]
async fn classify_tool_routing_refusal_keeps_detail_and_replays_no_spend() {
    let rig = ToolRig::new(Arc::new(Unmarked));
    *rig.router.refused.lock().unwrap() =
        Some(("no_route", "no decision model can serve this request"));
    let bytes = rig.call(&flow(), "refused", None).await.unwrap_err();
    let refused: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(refused["code"], "routing_refused");
    assert_eq!(refused["detail"]["routing_code"], "no_route");
    assert_eq!(
        refused["detail"]["routing_reason"],
        "no decision model can serve this request"
    );
    assert_eq!(refused["detail"]["no_spend"], true);
    assert!(refused["detail"].get("cost_unknown").is_none());
    assert!(refused["detail"].get("cost_usd").is_none());
    assert_eq!(rig.call(&flow(), "refused", None).await.unwrap_err(), bytes);
    assert_eq!(rig.router.bodies("route.select").len(), 1);
    assert!(rig.router.bodies("route.account_for").is_empty());
    assert_eq!(rig.provider.calls("sample"), 0);
    sample("routing-refused.json", &bytes);
}

#[tokio::test]
async fn classify_tool_rejects_a_nondecision_or_subscription_selection_without_spend() {
    for (model, price) in [
        ("openai/gpt-chat", None),
        (
            JEV,
            Some(
                cortexkit_role_routing::price::Price::subscription_equivalent(
                    cortexkit_role_routing::price::RatesPerMtok::new(1.into(), 0.into()),
                    1.into(),
                ),
            ),
        ),
    ] {
        let rig = ToolRig::new(Arc::new(Unmarked));
        let (provider, model) = model.split_once('/').unwrap();
        let route = RouteRef::new(provider, model);
        let mut selected = SelectAnswer::new(
            "rejected",
            route.clone(),
            RunnerRef::new("broca", route),
            1_000_000,
            1_000,
            rig.router.now() + 60_000,
            "stand-in decision",
        );
        selected.price = price;
        *rig.router.override_answer.lock().unwrap() = Some(("route.select", json!(selected)));
        let answer = rig.call(&flow(), "rejected", None).await.unwrap_err();
        let refused: Value = serde_json::from_slice(&answer).unwrap();
        assert_eq!(refused["code"], "routing_refused");
        assert_eq!(refused["detail"]["no_spend"], true);
        assert_eq!(
            rig.router.bodies("route.report")[0]["outcome"],
            "launch_failed"
        );
        assert_eq!(
            rig.call(&flow(), "rejected", None).await.unwrap_err(),
            answer
        );
        assert_eq!(rig.router.bodies("route.select").len(), 1);
        assert!(rig.router.bodies("route.account_for").is_empty());
        assert_eq!(rig.provider.calls("sample"), 0);
    }
}
