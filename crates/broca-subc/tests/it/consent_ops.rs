//! Consent requester bodies cross real daemon routes without a compatibility envelope.
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use broca_protocol::BindIdentity;
use broca_subc::{ConsentClient, RouteOpenRetry, WithdrawalReply};
use serde_json::{json, Value};

use super::common::Rig;

fn fixture(name: &str) -> Value {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/consent");
    serde_json::from_slice(&std::fs::read(root.join(name)).unwrap()).unwrap()
}

fn exchange(request: &str, reply: &str) -> Value {
    json!({"request":fixture(request), "reply":fixture(reply)})
}

struct Script {
    exchanges: Mutex<VecDeque<Value>>,
    bodies: Mutex<Vec<Vec<u8>>>,
}

struct Provider(Arc<Script>);

#[async_trait::async_trait]
impl subc_client_rs::ModuleHandler for Provider {
    async fn on_bind(&self, _: &subc_client_rs::RouteBindRequest) -> subc_client_rs::BindDecision {
        subc_client_rs::BindDecision::accept()
    }

    async fn handle(
        &self,
        _: subc_client_rs::RequestCtx,
        body: Vec<u8>,
    ) -> subc_client_rs::HandlerOutcome {
        self.0.bodies.lock().unwrap().push(body.clone());
        let vector = self
            .0
            .exchanges
            .lock()
            .unwrap()
            .pop_front()
            .expect("no unscripted op");
        assert_eq!(
            body,
            serde_json::to_vec(&vector["request"]).unwrap(),
            "real provider must receive the pinned parameter bytes"
        );
        if let Some(error) = vector.get("error") {
            subc_client_rs::HandlerOutcome::Error {
                code: error["code"].as_str().unwrap().into(),
                message: "scripted consent refusal".into(),
            }
        } else {
            subc_client_rs::HandlerOutcome::Response(serde_json::to_vec(&vector["reply"]).unwrap())
        }
    }
}

struct Serving {
    client: ConsentClient,
    script: Arc<Script>,
    serve: tokio::task::JoinHandle<()>,
    _rig: Rig,
}

impl Drop for Serving {
    fn drop(&mut self) {
        self.serve.abort();
    }
}

async fn start(tag: &str, exchanges: Vec<Value>) -> Option<Serving> {
    use subc_protocol::manifest::{
        Concurrency, ManagementOperation, ManagementOperationKind, ModuleManifest, ProviderRole,
    };
    let rig = Rig::start_with_modules(tag, vec![]).await?;
    let conn = Arc::new(rig.connect().await);
    let script = Arc::new(Script {
        exchanges: Mutex::new(exchanges.into()),
        bodies: Mutex::new(vec![]),
    });
    let manifest = ModuleManifest::builder("cingulate", env!("CARGO_PKG_VERSION"))
        .provides(vec![ProviderRole::ManagementSurface {
            operations: [
                "consent.request",
                "consent.answers",
                "consent.get",
                "consent.withdraw",
                "consent.await",
                "consent.report_execution",
                "consent.detach",
            ]
            .into_iter()
            .map(|name| ManagementOperation {
                name: name.into(),
                kind: ManagementOperationKind::Mutate,
                description: None,
            })
            .collect(),
            config_schema: json!({}),
            observability: vec![],
            identity_scope: vec![],
            concurrency: Concurrency::ModuleManaged,
        }])
        .build();
    let (_, serve) = subc_client_rs::serve_with_handle(
        &rig.connection_file,
        manifest,
        Provider(Arc::clone(&script)),
    )
    .await
    .unwrap();
    let serve = tokio::spawn(async move {
        serve.await.expect("scripted consent provider serve loop");
    });
    let identity = BindIdentity::new(&rig.dir, "broca", "broca-consent");
    // The SDK scripted provider accepts the bind. Production uses ConsentClient::open
    // with a daemon-attested scope; these tests pin the actual route request/ERROR transport.
    let route = conn
        .route_open_management_within("cingulate", &identity, RouteOpenRetry::default_patience())
        .await
        .unwrap();
    Some(Serving {
        client: ConsentClient::on_route(conn, route, Duration::from_secs(2)),
        script,
        serve,
        _rig: rig,
    })
}

fn assert_drained(serving: &Serving, count: usize) {
    assert!(
        serving.script.exchanges.lock().unwrap().is_empty(),
        "every scripted vector executed"
    );
    assert_eq!(
        serving.script.bodies.lock().unwrap().len(),
        count,
        "no retry or extra op hidden in the adapter"
    );
}

#[tokio::test]
async fn request_get_and_withdraw_match_the_frozen_vectors_on_a_real_route() {
    let filing = exchange(
        "broca/held-call-request.json",
        "upstream/consent-requester-v2/cerebellum-reply.json",
    );
    let get = exchange(
        "upstream/consent-requester-v2/cerebellum-get-request.json",
        "upstream/consent-requester-v2/cerebellum-get-reply.json",
    );
    let withdraw = exchange(
        "upstream/consent-requester-v1/withdraw-request.json",
        "upstream/consent-requester-v1/withdraw-reply.json",
    );
    let Some(serving) = start(
        "consent-vectors",
        vec![
            filing.clone(),
            get.clone(),
            withdraw.clone(),
            withdraw.clone(),
        ],
    )
    .await
    else {
        return;
    };
    let reply = serving
        .client
        .request(&filing["request"]["params"])
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(reply).unwrap(),
        filing["reply"]["result"]
    );
    let record = serving.client.get("el_1").await.unwrap();
    assert!(!record.is_terminal());
    assert_eq!(
        serde_json::to_value(record).unwrap(),
        get["reply"]["result"]
    );
    for _ in 0..2 {
        match serving.client.withdraw("el_2").await {
            WithdrawalReply::Acknowledged(record) => {
                assert_eq!(
                    serde_json::to_value(record).unwrap(),
                    withdraw["reply"]["result"]
                )
            }
            other => panic!("withdrawal should acknowledge, including a repeat: {other:?}"),
        }
    }
    assert_drained(&serving, 4);
    serving.client.close().await.unwrap();
}

#[tokio::test]
async fn answers_and_await_match_upstream_requester_bodies() {
    let fresh = exchange(
        "upstream/consent-requester-v2/cerebellum-ref-answers-request.json",
        "upstream/consent-requester-v2/cerebellum-ref-answers-reply.json",
    );
    let wait = exchange(
        "upstream/consent-requester-v2/cerebellum-ref-await-request.json",
        "upstream/consent-requester-v2/cerebellum-ref-await-reply.json",
    );
    let Some(serving) = start("consent-feed", vec![fresh.clone(), wait.clone()]).await else {
        return;
    };
    let page = serving.client.answers(0).await.unwrap();
    assert_eq!(page.records.len(), 1);
    assert_eq!(
        serde_json::to_value(&page).unwrap(),
        fresh["reply"]["result"]
    );
    let record = serving.client.r#await("el_1", 0).await.unwrap();
    assert_eq!(
        serde_json::to_value(record).unwrap(),
        wait["reply"]["result"]
    );
    assert_drained(&serving, 2);
}

#[tokio::test]
async fn withdrawal_stop_and_retry_classes_cross_real_error_frames() {
    let mut answered = exchange(
        "upstream/consent-requester-v1/withdraw-request.json",
        "upstream/consent-requester-v1/withdraw-reply.json",
    );
    answered["reply"]["result"]["state"] = json!("answered");
    let mut expired = answered.clone();
    expired["reply"]["result"]["state"] = json!("expired");
    let codes = [
        "elicitation_not_owner",
        "elicitation_not_found",
        "elicitation_store_error",
        "consent_unavailable",
        "scope_not_synced",
        "scope_changed",
        "scope_ended",
        "future_refusal",
    ];
    let mut exchanges = vec![answered, expired];
    exchanges.extend(codes.iter().map(|code| {
        json!({
            "request": {"method": "consent.withdraw", "params": {"elicitation_id": "el_call"}},
            "error": {"code": code},
        })
    }));
    let Some(serving) = start("consent-withdraw-classes", exchanges).await else {
        return;
    };
    for state in ["answered", "expired"] {
        assert!(
            matches!(serving.client.withdraw("el_2").await, WithdrawalReply::NoLongerOpen { code } if code == state)
        );
    }
    for code in codes {
        let reply = serving.client.withdraw("el_call").await;
        match code {
            "elicitation_not_owner" | "elicitation_not_found" => assert!(
                matches!(reply, WithdrawalReply::NoLongerOpen { code: actual } if actual == code)
            ),
            "elicitation_store_error"
            | "consent_unavailable"
            | "scope_not_synced"
            | "scope_changed" => assert!(
                matches!(reply, WithdrawalReply::Retryable(error) if error.code() == Some(code))
            ),
            _ => assert!(
                matches!(reply, WithdrawalReply::Refused(error) if error.code() == Some(code))
            ),
        }
    }
    assert_drained(&serving, 10);
}

struct RawApprovalTool {
    seen: Arc<Mutex<Vec<Vec<u8>>>>,
}
#[async_trait::async_trait]
impl subc_client_rs::ModuleHandler for RawApprovalTool {
    async fn on_bind(&self, _: &subc_client_rs::RouteBindRequest) -> subc_client_rs::BindDecision {
        subc_client_rs::BindDecision::accept()
    }
    async fn handle(
        &self,
        _: subc_client_rs::RequestCtx,
        body: Vec<u8>,
    ) -> subc_client_rs::HandlerOutcome {
        self.seen.lock().unwrap().push(body);
        subc_client_rs::HandlerOutcome::Response(
            serde_json::to_vec(&json!({"content":[{"type":"text","text":"ok"}],"isError":false}))
                .unwrap(),
        )
    }
}

#[tokio::test]
async fn approved_raw_dispatch_reaches_a_real_provider_without_reserializing_saved_bytes() {
    use broca_core::{BatchCtx, ToolManifest, ToolPlane};
    use broca_protocol::{BatchId, RunId, ToolCall, ToolCallId};
    use subc_protocol::manifest::{Concurrency, ModuleManifest, ProviderRole};
    let Some(rig) = Rig::start_with_modules("approved-raw-carrier", vec![]).await else {
        return;
    };
    let conn = rig.connect().await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let manifest = ModuleManifest::builder("approval-tools", env!("CARGO_PKG_VERSION"))
        .provides(vec![ProviderRole::ToolProvider {
            tools: vec![],
            identity_scope: vec![],
            concurrency: Concurrency::ModuleManaged,
            emits_push: false,
            sub_supervises: false,
        }])
        .build();
    let (_, serve) = subc_client_rs::serve_with_handle(
        &rig.connection_file,
        manifest,
        RawApprovalTool { seen: seen.clone() },
    )
    .await
    .unwrap();
    let serve = tokio::spawn(async move { serve.await.expect("raw approval provider serve loop") });
    let plane = broca_subc::SubcToolPlane::new(conn, "approval-tools");
    let ctx = BatchCtx {
        batch_id: BatchId("b".into()),
        run_id: RunId("r".into()),
        bind: BindIdentity::new(&rig.dir, "broca", "raw"),
        scope: None,
    };
    let bytes = r#"{"z":1e-6,"a":2}"#;
    let call = ToolCall {
        tool_call_id: ToolCallId("c".into()),
        tool_name: "edit".into(),
        input: serde_json::from_str(bytes).unwrap(),
        thought_signature: None,
    };
    assert_ne!(
        call.input.to_string(),
        bytes,
        "probe must distinguish reserialization"
    );
    let tool=serde_json::from_value(json!({"name":"edit","description":"edit","input_schema":{"type":"object"},"module":"approval-tools","execution_mode":"parallel"})).unwrap();
    plane
        .batch_started(
            &ctx,
            std::slice::from_ref(&call),
            &ToolManifest::from_tools(vec![tool]),
        )
        .await
        .unwrap();
    assert!(matches!(
        plane
            .dispatch_approved(
                &ctx,
                &call,
                bytes,
                "lineage:7",
                &broca_core::tool_plane::DispatchSignals::default()
            )
            .await,
        broca_core::DispatchResult::Completed(_)
    ));
    let bodies = seen.lock().unwrap().clone();
    assert_eq!(bodies.len(), 1);
    let raw = String::from_utf8(bodies[0].clone()).unwrap();
    assert!(raw.contains(&format!("\"arguments\":{bytes}")));
    let envelope: Value = serde_json::from_slice(&bodies[0]).unwrap();
    assert_eq!(envelope["call_key"], "lineage:7");
    assert!(envelope.get("approval_digest").is_none());
    plane.batch_finished(&ctx).await.unwrap();
    serve.abort();
}
