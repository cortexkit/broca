//! Every refusal `llm_classify` answers states its spend under the `cortexkit-llm-tools`
//! refusal-spend rule. These tests drive the route end to end, one provider outcome or
//! admission failure at a time, and read each refusal with the contract's own checker.

use super::*;
use crate::system_one::{
    BatchCatalog, ClassifierProvider, Config, ModelPin, ProviderReply, SystemOne,
};
use cortexkit_llm_tools::errors::{refusal_spend, RefusalSpend};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Catalog;

impl BatchCatalog for Catalog {
    fn row(&self, _: &str) -> Option<broca_catalog::ModelBatchRow> {
        Some(broca_catalog::ModelBatchRow {
            kind: Some("decision".into()),
            context_tokens: None,
            attachment: false,
            input_rate_nanos: Some(42_000_000),
            output_rate_nanos: Some(0),
            model_name: None,
            provider_name: None,
        })
    }
}

type Script = Arc<dyn Fn() -> ProviderReply + Send + Sync>;

/// One provider outcome: its name, the reply the provider gives, the refusal code and spend.
type Case = (
    &'static str,
    Box<dyn Fn() -> ProviderReply + Send + Sync>,
    &'static str,
    RefusalSpend,
);

struct Scripted {
    calls: AtomicUsize,
    reply: Script,
}

#[async_trait::async_trait]
impl ClassifierProvider for Scripted {
    fn egress_host(&self) -> String {
        "localhost".into()
    }

    fn credential_id(&self) -> Option<String> {
        Some("apikey:typesafe".into())
    }

    async fn call(&self, _: String) -> ProviderReply {
        self.calls.fetch_add(1, Ordering::SeqCst);
        (self.reply)()
    }
}

struct Rig {
    tools: Arc<LlmTools>,
    provider: Arc<Scripted>,
}

const ANSWER: &[u8] =
    br#"{"model":"jev-1.13.0","answers":{"q":{"type":"noul","noul":0.5}},"usage":{"input_tokens":100}}"#;

impl Rig {
    fn new(reply: impl Fn() -> ProviderReply + Send + Sync + 'static) -> Self {
        Self::with_providers(reply, vec!["typesafe".into()])
    }

    fn with_providers(
        reply: impl Fn() -> ProviderReply + Send + Sync + 'static,
        private_content_providers: Vec<String>,
    ) -> Self {
        let provider = Arc::new(Scripted {
            calls: AtomicUsize::new(0),
            reply: Arc::new(reply),
        });
        let system = SystemOne::new(
            Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap()),
            "refusal-spend",
            Arc::new(Catalog),
            vec![ModelPin::jev(provider.clone())],
            Config::default(),
            Arc::default(),
        )
        .with_store_role(crate::system_one::LLM_CLASSIFY_STORE_ROLE)
        .with_export(
            Arc::new(broca_store::SqliteStore::open_in_memory().unwrap()),
            Arc::new(crate::ChargeBasisTable::shipped_defaults()),
        );
        let tools = Arc::new(LlmTools::new(
            Arc::new(system),
            crate::config::LlmToolsConfig {
                advertise_tags: true,
                image_references: false,
                private_content_providers,
            },
            Arc::new(NoPrivateContent),
        ));
        Self { tools, provider }
    }

    async fn send(&self, request: Value, scope: &SendScope) -> Answer {
        self.tools
            .answer(
                &serde_json::to_vec(&request).unwrap(),
                Some(&principal()),
                scope,
            )
            .await
    }

    /// The refusal body of one `llm_classify` call keyed `call_key`.
    async fn refuse(&self, call_key: &str, arguments: Value) -> Value {
        let answer = self
            .send(
                json!({"name": CLASSIFY_TOOL_NAME, "call_key": call_key, "arguments": arguments}),
                &stamped(),
            )
            .await;
        body(answer)
    }
}

fn principal() -> BindPrincipal {
    BindPrincipal::Reserved {
        module_id: "owner".into(),
    }
}

fn stamped() -> SendScope {
    SendScope::Stamped(broca_protocol::BindScope {
        scope: broca_protocol::SessionScope {
            owner: principal(),
            scope_ref: "run".into(),
            scope_epoch: 1,
            flow_id: None,
            sender: None,
            agent_id: Some("agent".into()),
            run_id: Some("run".into()),
        },
        owner_authorized: true,
        kind: broca_protocol::ScopeKind::Head,
        parent: None,
        parent_state: None,
    })
}

fn arguments() -> Value {
    json!({
        "model": crate::system_one::JEV_1_13_0,
        "item": {"state": "traffic"},
        "questions": {"q": {"type": "noul", "instructions": "Is traffic urgent?"}},
        "max_cost_usd": 1.0
    })
}

fn body(answer: Answer) -> Value {
    match answer {
        Ok(reply) => panic!("expected a refusal, got {}", reply.get()),
        Err(body) => serde_json::from_slice(&body).expect("a refusal body is JSON"),
    }
}

/// The spend the contract's checker reads from a refusal body; a `SpendViolation` fails.
fn spend(body: &Value) -> RefusalSpend {
    let refusal: Refusal = serde_json::from_value(body.clone()).expect("a refusal decodes");
    refusal_spend(&refusal).unwrap_or_else(|violation| panic!("{violation}: {body}"))
}

/// Every final provider outcome, as the provider clients report it, against the code and the
/// spend the refusal carries. A provider error answer the provider does not bill and a failure
/// before anything was sent spend nothing; anything that may have reached a billed model is
/// unknown spend.
#[tokio::test]
async fn every_provider_outcome_states_its_spend() {
    use ProviderReply::*;
    let status = |status| {
        move || Status {
            status,
            retry_after_ms: None,
        }
    };
    let workers = |status, code| {
        move || WorkersAiError {
            status,
            code,
            retry_after_ms: None,
        }
    };
    let cases: Vec<Case> = vec![
        (
            "http 401",
            Box::new(status(401)),
            "auth_failed",
            RefusalSpend::NoSpend,
        ),
        (
            "http 403",
            Box::new(status(403)),
            "auth_failed",
            RefusalSpend::NoSpend,
        ),
        (
            "http 404",
            Box::new(status(404)),
            "model_unavailable",
            RefusalSpend::NoSpend,
        ),
        (
            "http 429",
            Box::new(status(429)),
            "rate_limited",
            RefusalSpend::NoSpend,
        ),
        (
            "http 529",
            Box::new(status(529)),
            "rate_limited",
            RefusalSpend::NoSpend,
        ),
        (
            "http 400",
            Box::new(status(400)),
            "invalid_item",
            RefusalSpend::NoSpend,
        ),
        (
            "http 413",
            Box::new(status(413)),
            "invalid_item",
            RefusalSpend::NoSpend,
        ),
        (
            "http 422",
            Box::new(status(422)),
            "invalid_item",
            RefusalSpend::NoSpend,
        ),
        (
            "http 408",
            Box::new(status(408)),
            "provider_error",
            RefusalSpend::ChargeCeiling,
        ),
        (
            "http 500",
            Box::new(status(500)),
            "provider_error",
            RefusalSpend::ChargeCeiling,
        ),
        (
            "http 503",
            Box::new(status(503)),
            "provider_error",
            RefusalSpend::ChargeCeiling,
        ),
        (
            "workers 3036",
            Box::new(workers(429, Some(3036))),
            "account_limit_exhausted",
            RefusalSpend::NoSpend,
        ),
        (
            "workers 5035",
            Box::new(workers(403, Some(5035))),
            "account_not_entitled",
            RefusalSpend::NoSpend,
        ),
        (
            "workers 5016",
            Box::new(workers(403, Some(5016))),
            "account_not_entitled",
            RefusalSpend::NoSpend,
        ),
        (
            "workers 3023",
            Box::new(workers(403, Some(3023))),
            "account_not_entitled",
            RefusalSpend::NoSpend,
        ),
        (
            "workers 3042",
            Box::new(workers(404, Some(3042))),
            "model_unavailable",
            RefusalSpend::NoSpend,
        ),
        (
            "workers 5007",
            Box::new(workers(400, Some(5007))),
            "model_unavailable",
            RefusalSpend::NoSpend,
        ),
        (
            "workers 403",
            Box::new(workers(403, None)),
            "auth_failed",
            RefusalSpend::NoSpend,
        ),
        (
            "workers 5004",
            Box::new(workers(400, Some(5004))),
            "invalid_item",
            RefusalSpend::NoSpend,
        ),
        (
            "workers 3006",
            Box::new(workers(413, Some(3006))),
            "invalid_item",
            RefusalSpend::NoSpend,
        ),
        (
            "workers 3040",
            Box::new(workers(429, Some(3040))),
            "rate_limited",
            RefusalSpend::NoSpend,
        ),
        (
            "workers 3007",
            Box::new(workers(408, Some(3007))),
            "provider_error",
            RefusalSpend::ChargeCeiling,
        ),
        (
            "workers 500",
            Box::new(workers(500, None)),
            "provider_error",
            RefusalSpend::ChargeCeiling,
        ),
        (
            "timeout",
            Box::new(|| Transport("the request timed out".into())),
            "provider_error",
            RefusalSpend::ChargeCeiling,
        ),
        (
            "lost connection",
            Box::new(|| Transport("the request failed in transport".into())),
            "provider_error",
            RefusalSpend::ChargeCeiling,
        ),
        (
            "never connected",
            Box::new(|| Unconnected("the connection failed".into())),
            "provider_error",
            RefusalSpend::NoSpend,
        ),
        (
            "no credential",
            Box::new(|| Credential("invented".into())),
            "auth_failed",
            RefusalSpend::NoSpend,
        ),
        (
            "undecodable 200",
            Box::new(|| Answered(b"not json".to_vec())),
            "provider_error",
            RefusalSpend::ChargeCeiling,
        ),
        (
            "200 without answers",
            Box::new(|| Answered(br#"{"answers":[1]}"#.to_vec())),
            "provider_error",
            RefusalSpend::ChargeCeiling,
        ),
        (
            "200 without input tokens",
            Box::new(|| Answered(br#"{"answers":{"q":{"type":"noul","noul":0.5}}}"#.to_vec())),
            "cost_unmeasured",
            RefusalSpend::ChargeCeiling,
        ),
    ];
    for (name, reply, code, want) in cases {
        let rig = Rig::new(reply);
        let refusal = rig.refuse("outcome", arguments()).await;
        assert_eq!(refusal["code"], code, "{name}: {refusal}");
        assert_eq!(spend(&refusal), want, "{name}: {refusal}");
        assert_eq!(rig.provider.calls.load(Ordering::SeqCst), 1, "{name}");
    }
}

/// A provider answer the tool's result type refuses is refused after its cost was measured,
/// so the refusal carries that cost: the same number the answer would have carried.
#[tokio::test]
async fn a_malformed_answer_carries_its_measured_cost() {
    let rig = Rig::new(|| {
        ProviderReply::Answered(
            br#"{"answers":{"q":"not an answer"},"usage":{"input_tokens":100}}"#.to_vec(),
        )
    });
    let refusal = rig.refuse("malformed", arguments()).await;
    assert_eq!(refusal["code"], "provider_error");
    // The same usage answered well costs what the refusal carries.
    let answered = Rig::new(|| ProviderReply::Answered(ANSWER.to_vec()));
    let result = answered
        .send(
            json!({"name": CLASSIFY_TOOL_NAME, "call_key": "malformed", "arguments": arguments()}),
            &stamped(),
        )
        .await
        .expect("a well-formed answer succeeds");
    let cost = serde_json::from_str::<Value>(result.get()).unwrap()["cost_usd"]
        .as_f64()
        .unwrap();
    assert!(cost > 0.0);
    assert_eq!(spend(&refusal), RefusalSpend::Measured(cost), "{refusal}");
    // A repeat replays the stored answer and refuses it the same way, without a second call.
    let again = rig.refuse("malformed", arguments()).await;
    assert_eq!(again, refusal);
    assert_eq!(rig.provider.calls.load(Ordering::SeqCst), 1);
}

/// Every refusal returned before anything reached a provider spends nothing.
#[tokio::test]
async fn refusals_before_dispatch_spend_nothing() {
    let rig = Rig::new(|| ProviderReply::Answered(ANSWER.to_vec()));
    let mut cases: Vec<(&str, Value)> = Vec::new();

    let unscoped = rig
        .send(
            json!({"name": CLASSIFY_TOOL_NAME, "call_key": "k", "arguments": arguments()}),
            &SendScope::Internal,
        )
        .await;
    cases.push(("scope_required", body(unscoped)));
    let mut unknown_field = arguments();
    unknown_field["extra"] = json!(1);
    cases.push(("invalid_params", rig.refuse("k", unknown_field).await));
    let mut unpinned = arguments();
    unpinned["model"] = json!("typesafe/not-pinned");
    cases.push(("model_unavailable", rig.refuse("k2", unpinned).await));
    let mut too_cheap = arguments();
    too_cheap["max_cost_usd"] = json!(0.0);
    cases.push(("cost_exceeded", rig.refuse("k3", too_cheap).await));
    let undecodable = rig.tools.answer(b"{", Some(&principal()), &stamped()).await;
    cases.push(("invalid_params", body(undecodable)));
    let late = rig
        .tools
        .answer(
            &serde_json::to_vec(&json!({"name": "late_results", "arguments": {}})).unwrap(),
            None,
            &stamped(),
        )
        .await;
    cases.push(("scope_required", body(late)));
    for (code, refusal) in &cases {
        assert_eq!(refusal["code"], *code, "{refusal}");
        assert_eq!(spend(refusal), RefusalSpend::NoSpend, "{refusal}");
    }

    // Tool-provider protocol errors carry no class, so they are read by their member.
    let unknown = rig
        .send(json!({"name": "no_such_tool", "arguments": {}}), &stamped())
        .await;
    let unknown = body(unknown);
    assert_eq!(unknown["code"], "unknown_method");
    assert_eq!(unknown["detail"]["no_spend"], true, "{unknown}");
    let catalog = rig
        .send(
            json!({"name": "tool.catalog", "arguments": {"preset": "invented"}}),
            &stamped(),
        )
        .await;
    let catalog = body(catalog);
    assert_eq!(catalog["code"], "invalid_request");
    assert_eq!(catalog["detail"]["no_spend"], true, "{catalog}");
    assert_eq!(rig.provider.calls.load(Ordering::SeqCst), 0);
}

/// Private-content admission ends a call before dispatch, so its refusal spends nothing.
#[tokio::test]
async fn a_private_content_refusal_spends_nothing() {
    let rig = Rig::with_providers(|| ProviderReply::Answered(ANSWER.to_vec()), Vec::new());
    let refusal = rig.refuse("private", arguments()).await;
    assert_eq!(spend(&refusal), RefusalSpend::NoSpend, "{refusal}");
    assert_eq!(rig.provider.calls.load(Ordering::SeqCst), 0);
}

/// Every consent outcome that ends a held call unsent spends nothing.
#[test]
fn not_started_spends_nothing_for_every_consent_reason() {
    for reason in [
        "consent_required",
        "denied",
        "consent_unavailable",
        "pending",
        "withdrawn",
        "expired",
        "private_content_unknown",
        "private_content_unreachable",
    ] {
        let body: Value = serde_json::from_str(&super::super::held::not_started(reason)).unwrap();
        assert_eq!(body["detail"]["consent"], reason);
        assert_eq!(spend(&body), RefusalSpend::NoSpend, "{reason}");
    }
}

/// A repeat with the same call key answers the stored refusal with the same spend members,
/// without a second provider call; a repeat with other arguments is refused at no spend.
#[tokio::test]
async fn a_replay_answers_the_same_mark() {
    let rig = Rig::new(|| ProviderReply::Status {
        status: 400,
        retry_after_ms: None,
    });
    let first = rig.refuse("replayed", arguments()).await;
    assert_eq!(first["code"], "invalid_item");
    assert_eq!(spend(&first), RefusalSpend::NoSpend);
    let again = rig.refuse("replayed", arguments()).await;
    assert_eq!(again, first, "a replay returns the identical refusal");
    assert_eq!(rig.provider.calls.load(Ordering::SeqCst), 1);

    let mut other = arguments();
    other["item"] = json!({"state": "other"});
    let reuse = rig.refuse("replayed", other).await;
    assert_eq!(reuse["code"], "batch_id_reuse");
    assert_eq!(spend(&reuse), RefusalSpend::NoSpend);
    assert_eq!(rig.provider.calls.load(Ordering::SeqCst), 1);
}

/// An item error stored before model tools marked refusals has no spend member. The contract
/// says a repeat returns the same spend members as the first answer, so it replays unmarked
/// (`Undeclared`) rather than gaining a mark its first answer never had.
#[tokio::test]
async fn a_stored_error_from_before_marks_replays_unmarked() {
    let rig = Rig::new(|| ProviderReply::Status {
        status: 400,
        retry_after_ms: None,
    });
    let first = rig.refuse("legacy", arguments()).await;
    let batch_id = hash(&json!({
        "principal": principal().as_owner_string(),
        "owner": principal().as_owner_string(),
        "scope_ref": "run",
        "scope_epoch": 1,
        "call_key": "legacy",
    }));
    let mut stored = rig
        .tools
        .system
        .stored_batch(&batch_id)
        .await
        .unwrap()
        .unwrap();
    let args = arguments();
    let params = format!(
        "{{\"batch_id\":{},\"model\":{},\"questions\":{},\"items\":[{}],\"max_cost_usd\":1.0}}",
        json!(batch_id),
        args["model"],
        args["questions"],
        args["item"],
    );
    let replay = |stored: broca_store::system_one::StoredBatch| {
        let system = rig.tools.system.clone();
        let params = params.clone();
        async move {
            body(convert(
                &system.replay_tool(&params, &stored).await.unwrap(),
            ))
        }
    };
    assert_eq!(replay(stored.clone()).await, first);

    let broca_store::system_one::Outcome::Failed(text) = &stored.outcomes[0].outcome else {
        panic!("the item error is stored");
    };
    let mut legacy: Value = serde_json::from_str(text).unwrap();
    assert_eq!(legacy["no_spend"], true, "the stored error keeps its mark");
    legacy.as_object_mut().unwrap().remove("no_spend");
    stored.outcomes[0].outcome = broca_store::system_one::Outcome::Failed(legacy.to_string());
    let unmarked = replay(stored).await;
    assert_eq!(unmarked["code"], "invalid_item");
    assert_eq!(spend(&unmarked), RefusalSpend::Undeclared);
}

/// The marks are the model tools' only: the shared classifier route keeps answering item
/// errors without any spend member.
#[tokio::test]
async fn classifier_replies_carry_no_spend_member() {
    // A rate limit is stored for the first item and stops the call, so the second is unsent.
    let rig = Rig::new(|| ProviderReply::Status {
        status: 429,
        retry_after_ms: None,
    });
    let classifier = Arc::new(SystemOne::new(
        Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap()),
        "classifier",
        Arc::new(Catalog),
        vec![ModelPin::jev(rig.provider.clone())],
        Config::default(),
        Arc::default(),
    ));
    let params = json!({"batch_id":"classifier","model":crate::system_one::JEV_1_13_0,
        "questions":{"q":{"type":"noul","instructions":"Flag?"}},"items":[{"state":"a"},{"state":"b"}]});
    let reply = classifier
        .spawn_call(
            params.to_string(),
            broca_store::system_one::BatchScope::Run {
                agent_id: "agent".into(),
                run_id: "run".into(),
            },
        )
        .await
        .unwrap();
    let reply: Value = serde_json::from_str(reply.get()).unwrap();
    for item in reply["items"].as_array().unwrap() {
        let error = item["error"].as_object().expect("each item failed");
        assert!(
            !error.contains_key("no_spend") && !error.contains_key("cost_unknown"),
            "{reply}"
        );
    }
}
