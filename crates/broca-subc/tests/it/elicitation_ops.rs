//! Real ck-subc + module SDK route operations, with independently pinned C9 payload bytes.
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use broca_protocol::BindIdentity;
use broca_subc::{
    AnswersCursor, ElicitationClient, HeldCallRequest, RouteOpenRetry, WithdrawalReply,
};
use serde_json::{json, Value};

use super::common::Rig;

fn fixture(name: &str) -> Value {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/elicitation");
    serde_json::from_slice(&std::fs::read(root.join(name)).unwrap()).unwrap()
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
            serde_json::to_vec(&json!({
                "method": vector["method"], "params": vector["request"],
            }))
            .unwrap(),
            "real provider must receive the pinned parameter bytes"
        );
        if let Some(error) = vector.get("error") {
            subc_client_rs::HandlerOutcome::Error {
                code: error["code"].as_str().unwrap().into(),
                message: "scripted C9 refusal".into(),
            }
        } else {
            subc_client_rs::HandlerOutcome::Response(serde_json::to_vec(&vector["reply"]).unwrap())
        }
    }
}

struct Serving {
    client: ElicitationClient,
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
    let manifest = ModuleManifest::builder("prefrontal-core", env!("CARGO_PKG_VERSION"))
        .provides(vec![ProviderRole::ManagementSurface {
            operations: [
                "elicitation.request",
                "elicitation.answers",
                "elicitation.get",
                "elicitation.withdraw",
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
        serve
            .await
            .expect("scripted elicitation provider serve loop");
    });
    let identity = BindIdentity::new(&rig.dir, "broca", "broca-elicitation");
    // The SDK scripted provider accepts the bind. Production uses ElicitationClient::open
    // with a daemon-attested scope; these tests pin the actual route request/ERROR transport.
    let route = conn
        .route_open_management_within(
            "prefrontal-core",
            &identity,
            RouteOpenRetry::default_patience(),
        )
        .await
        .unwrap();
    Some(Serving {
        client: ElicitationClient::on_route(conn, route, Duration::from_secs(2)),
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
    let filing = fixture("c9/request.json");
    let get = fixture("requester/get.json");
    let withdraw = fixture("requester/withdraw.json");
    let Some(serving) = start(
        "elicitation-vectors",
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
    let request: HeldCallRequest = serde_json::from_value(filing["request"].clone()).unwrap();
    let reply = serving.client.request(&request).await.unwrap();
    assert_eq!(serde_json::to_value(reply).unwrap(), filing["reply"]);
    let record = serving.client.get("@ELICITATION_ID@").await.unwrap();
    assert!(!record.is_terminal());
    assert_eq!(serde_json::to_value(record).unwrap(), get["reply"]);
    for _ in 0..2 {
        match serving.client.withdraw("@ELICITATION_ID@").await {
            WithdrawalReply::Acknowledged(record) => {
                assert_eq!(serde_json::to_value(record).unwrap(), withdraw["reply"])
            }
            other => panic!("withdrawal should acknowledge, including a repeat: {other:?}"),
        }
    }
    assert_drained(&serving, 4);
    serving.client.close().await.unwrap();
}

#[tokio::test]
async fn old_incarnation_refusal_surfaces_and_fresh_read_keeps_answers() {
    let old = fixture("c9/answers-old-incarnation.json");
    let mut fresh = fixture("c9/answers-incarnation.json");
    fresh["reply"]["records"] = json!([{
        "elicitation_id": "el_call", "call_key": "br:episode:17", "state": "answered",
        "answered_choice_id": "approve", "approval_digest": fixture("c9/request.json")["reply"]["approval_digest"],
    }]);
    let object = fixture("c9/answers-object.json");
    let Some(serving) = start(
        "elicitation-cursor",
        vec![old.clone(), fresh.clone(), object.clone()],
    )
    .await
    else {
        return;
    };
    let cursor: AnswersCursor = serde_json::from_value(old["request"]["since"].clone()).unwrap();
    let error = serving
        .client
        .answers(Some(&cursor))
        .await
        .expect_err("old incarnation is not an empty page");
    assert_eq!(error.code(), old["error"]["code"].as_str());
    let page = serving.client.answers(None).await.unwrap();
    assert_eq!(page.records.len(), 1);
    assert_eq!(serde_json::to_value(&page).unwrap(), fresh["reply"]);
    let page = serving.client.answers(Some(&page.cursor)).await.unwrap();
    assert_eq!(serde_json::to_value(page).unwrap(), object["reply"]);
    assert_drained(&serving, 3);
}

#[tokio::test]
async fn withdrawal_stop_and_retry_classes_cross_real_error_frames() {
    let mut answered = fixture("requester/withdraw.json");
    answered["reply"]["state"] = json!("answered");
    let mut expired = answered.clone();
    expired["reply"]["state"] = json!("expired");
    let codes = [
        "elicitation_not_owner",
        "elicitation_not_found",
        "elicitation_store_error",
        "elicitation_unavailable",
        "scope_not_synced",
        "scope_changed",
        "scope_ended",
        "future_refusal",
    ];
    let mut exchanges = vec![answered, expired];
    exchanges.extend(codes.iter().map(|code| {
        json!({
            "method": "elicitation.withdraw", "request": {"elicitation_id": "el_call"},
            "error": {"code": code},
        })
    }));
    let Some(serving) = start("elicitation-withdraw-classes", exchanges).await else {
        return;
    };
    for state in ["answered", "expired"] {
        assert!(
            matches!(serving.client.withdraw("@ELICITATION_ID@").await, WithdrawalReply::NoLongerOpen { code } if code == state)
        );
    }
    for code in codes {
        let reply = serving.client.withdraw("el_call").await;
        match code {
            "elicitation_not_owner" | "elicitation_not_found" => assert!(
                matches!(reply, WithdrawalReply::NoLongerOpen { code: actual } if actual == code)
            ),
            "elicitation_store_error"
            | "elicitation_unavailable"
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
                "digest",
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
    assert_eq!(envelope["approval_digest"], "digest");
    plane.batch_finished(&ctx).await.unwrap();
    serve.abort();
}
