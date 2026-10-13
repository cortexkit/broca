//! Real ck-subc and a scripted SDK provider: role negotiation, wire bytes and deadlines.
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use broca_core::{HookPlane, STEP_TRANSFORM_HOOK_CAP};
use broca_protocol::BindIdentity;
use broca_subc::SubcHookPlane;
use cortexkit_role_step_transform_provider::{hook::HookCall, subscription::DeclareRequest};
use serde_json::{json, Value};
use tokio::time::Instant;

use super::common::Rig;

fn fixture(name: &str) -> Value {
    let root =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/step-transform");
    serde_json::from_slice(&std::fs::read(root.join(name)).unwrap()).unwrap()
}

fn describe() -> Value {
    fixture("role-describe.json")["canonical"][0]["answer"].clone()
}

enum Reply {
    Json(Value),
    Refuse(&'static str),
    Silent,
}

struct Script {
    describe: Mutex<Value>,
    replies: Mutex<VecDeque<Reply>>,
    binds: Mutex<Vec<Value>>,
    bodies: Mutex<Vec<Vec<u8>>>,
    hook_seen: tokio::sync::Notify,
    bind_silent: bool,
}

struct Provider(Arc<Script>);

#[async_trait::async_trait]
impl subc_client_rs::ModuleHandler for Provider {
    async fn on_bind(
        &self,
        request: &subc_client_rs::RouteBindRequest,
    ) -> subc_client_rs::BindDecision {
        self.0.binds.lock().unwrap().push(json!({
            "identity": request.identity,
            "role_versions": request.role_versions,
            "scope": request.scope,
        }));
        if self.0.bind_silent {
            std::future::pending::<()>().await;
        }
        subc_client_rs::BindDecision::accept()
    }

    async fn handle(
        &self,
        _: subc_client_rs::RequestCtx,
        body: Vec<u8>,
    ) -> subc_client_rs::HandlerOutcome {
        let request: Value = serde_json::from_slice(&body).unwrap();
        self.0.bodies.lock().unwrap().push(body);
        if request["method"] == "role.describe" {
            assert_eq!(request, json!({"method": "role.describe", "params": {}}));
            return subc_client_rs::HandlerOutcome::Response(
                serde_json::to_vec(&*self.0.describe.lock().unwrap()).unwrap(),
            );
        }
        assert!(
            matches!(
                request["method"].as_str(),
                Some("transform.hook" | "transform.declare")
            ),
            "role requests must never be tool envelopes: {request}"
        );
        self.0.hook_seen.notify_one();
        let reply = self
            .0
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("no extra retries");
        match reply {
            Reply::Json(value) => {
                subc_client_rs::HandlerOutcome::Response(serde_json::to_vec(&value).unwrap())
            }
            Reply::Refuse(code) => subc_client_rs::HandlerOutcome::Error {
                code: code.into(),
                message: "scripted refusal".into(),
            },
            Reply::Silent => std::future::pending().await,
        }
    }
}

struct Serving {
    plane: Arc<SubcHookPlane>,
    script: Arc<Script>,
    bind: BindIdentity,
    serve: Vec<tokio::task::JoinHandle<()>>,
    _rig: Rig,
}

impl Drop for Serving {
    fn drop(&mut self) {
        for serve in &self.serve {
            serve.abort();
        }
    }
}

async fn start(tag: &str, replies: Vec<Reply>, description: Value, bind_silent: bool) -> Serving {
    use subc_protocol::manifest::{Concurrency, ModuleManifest, ProviderRole};
    let rig = Rig::start_with_modules(tag, vec![])
        .await
        .expect("real ck-subc required");
    let conn = Arc::new(rig.connect().await);
    let script = Arc::new(Script {
        describe: Mutex::new(description),
        replies: Mutex::new(replies.into()),
        binds: Mutex::new(vec![]),
        bodies: Mutex::new(vec![]),
        hook_seen: tokio::sync::Notify::new(),
        bind_silent,
    });
    let mut serves = Vec::new();
    for provider in ["transform-test", "transform-test-other"] {
        let manifest = ModuleManifest::builder(provider, env!("CARGO_PKG_VERSION"))
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
            Provider(script.clone()),
        )
        .await
        .unwrap();
        serves.push(tokio::spawn(async move {
            serve.await.expect("scripted hook provider serve loop");
        }));
    }
    let bind = BindIdentity::new(&rig.dir, "broca", "ses_01J9Z");
    Serving {
        plane: Arc::new(SubcHookPlane::new(conn)),
        script,
        bind,
        serve: serves,
        _rig: rig,
    }
}

fn call(bind: &BindIdentity) -> HookCall {
    let mut params = fixture("hook-requests.json")["requests"][0]["request"].clone();
    params["session"] = json!(bind.session);
    params["harness"] = json!(bind.harness);
    serde_json::from_value(params).unwrap()
}

async fn hook(serving: &Serving) -> bool {
    serving
        .plane
        .hook_with_deadline(
            "transform-test",
            &serving.bind,
            call(&serving.bind),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .is_ok()
}

fn bind_count(serving: &Serving) -> usize {
    serving.script.binds.lock().unwrap().len()
}

fn op_count(serving: &Serving, method: &str) -> usize {
    serving
        .script
        .bodies
        .lock()
        .unwrap()
        .iter()
        .filter(|body| serde_json::from_slice::<Value>(body).unwrap()["method"] == method)
        .count()
}

#[tokio::test]
async fn hook_and_declaration_use_the_role_route_and_frozen_wire_vectors() {
    let vectors = fixture("hook-requests.json")["requests"]
        .as_array()
        .unwrap()
        .clone();
    let declaration = json!({"subscriptions": [], "provider_extra": {"unchanged": true}});
    let serving = start(
        "hook-wire",
        std::iter::once(Reply::Json(declaration.clone()))
            .chain(
                vectors
                    .iter()
                    .map(|_| Reply::Json(json!({"answer": "pass"}))),
            )
            .collect(),
        describe(),
        false,
    )
    .await;
    let request: DeclareRequest = serde_json::from_value(json!({"preset": "head",
        "params": {"nested": [1, null, {"x": false}]}, "composition": {"opaque": true}}))
    .unwrap();
    let result = serving
        .plane
        .declare(
            "transform-test",
            &serving.bind,
            &request,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert_eq!(result, declaration);
    for vector in &vectors {
        let call: HookCall = serde_json::from_value(vector["request"].clone()).unwrap();
        assert!(matches!(
            serving
                .plane
                .hook("transform-test", &serving.bind, call)
                .await,
            Ok(cortexkit_role_step_transform_provider::answer::HookAnswer::Pass)
        ));
    }
    {
        let bodies = serving.script.bodies.lock().unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&bodies[1]).unwrap(),
            json!({"method": "transform.declare", "params": serde_json::to_value(request).unwrap()})
        );
        for (body, vector) in bodies[2..].iter().zip(&vectors) {
            assert_eq!(
                serde_json::from_slice::<Value>(body).unwrap(),
                json!({"method": "transform.hook", "params": vector["request"]}),
                "{}",
                vector["name"]
            );
        }
    }
    assert_eq!(bind_count(&serving), 1);
    assert_eq!(op_count(&serving, "role.describe"), 1);
    {
        let binds = serving.script.binds.lock().unwrap();
        assert_eq!(binds[0]["identity"]["harness"], "runner");
        assert_eq!(
            binds[0]["role_versions"],
            json!({"step-transform-provider": "v1"})
        );
        assert!(
            binds[0].get("scope").is_none_or(Value::is_null),
            "unscoped role route"
        );
    }
    serving.plane.close_session_routes(&serving.bind).await;
}

#[tokio::test]
async fn route_cache_keys_include_every_caller_identity_component() {
    let serving = start(
        "hook-key",
        (0..6)
            .map(|_| Reply::Json(json!({"answer": "pass"})))
            .collect(),
        describe(),
        false,
    )
    .await;
    assert!(hook(&serving).await);
    assert!(hook(&serving).await);
    for bind in [
        BindIdentity::new(
            &serving.bind.project_root,
            "another-caller",
            &serving.bind.session,
        ),
        BindIdentity::new(
            &serving.bind.project_root,
            &serving.bind.harness,
            "another-session",
        ),
        BindIdentity::new(
            serving.bind.project_root.join("another-project"),
            &serving.bind.harness,
            &serving.bind.session,
        ),
    ] {
        assert!(serving
            .plane
            .hook("transform-test", &bind, call(&bind))
            .await
            .is_ok());
        serving.plane.close_session_routes(&bind).await;
    }
    assert!(serving
        .plane
        .hook("transform-test-other", &serving.bind, call(&serving.bind))
        .await
        .is_ok());
    assert_eq!(bind_count(&serving), 5);
}

#[tokio::test]
async fn transient_refusal_reopens_and_retries_only_once() {
    let serving = start(
        "hook-transient",
        vec![
            Reply::Refuse("transient"),
            Reply::Refuse("transient"),
            Reply::Json(json!({"answer": "pass"})),
        ],
        describe(),
        false,
    )
    .await;
    assert!(!hook(&serving).await);
    assert_eq!(op_count(&serving, "transform.hook"), 2);
    assert_eq!(bind_count(&serving), 2);
    assert!(hook(&serving).await);
    assert_eq!(bind_count(&serving), 3);
}

#[tokio::test]
async fn terminal_refusal_and_bad_answer_do_not_retry_and_next_call_reopens() {
    let serving = start(
        "hook-failure",
        vec![
            Reply::Refuse("invalid_params"),
            Reply::Json(json!({"answer": "unknown"})),
            Reply::Json(json!({"answer": "pass"})),
        ],
        describe(),
        false,
    )
    .await;
    assert!(!hook(&serving).await);
    assert_eq!(op_count(&serving, "transform.hook"), 1);
    assert!(!hook(&serving).await);
    assert_eq!(op_count(&serving, "transform.hook"), 2);
    assert!(hook(&serving).await);
    assert_eq!(bind_count(&serving), 3);
}

#[tokio::test]
async fn missing_required_op_is_named_before_hooks_or_declarations() {
    let mut description = describe();
    description["majors"][0]["ops"] = json!(["role.describe", "transform.declare"]);
    let serving = start(
        "hook-missing-op",
        vec![Reply::Json(json!({"subscriptions": []}))],
        description,
        false,
    )
    .await;
    let request = DeclareRequest::new(Default::default());
    let error = serving
        .plane
        .declare(
            "transform-test",
            &serving.bind,
            &request,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();
    assert!(format!("{error:?}").contains("transform.hook"));
    assert!(format!("{error:?}").contains("transform-test"));
    assert_eq!(op_count(&serving, "transform.declare"), 0);
    assert_eq!(op_count(&serving, "transform.hook"), 0);
}

#[tokio::test]
async fn mismatched_caller_identity_never_opens_a_route() {
    let serving = start(
        "hook-mismatch",
        vec![
            Reply::Json(json!({"answer": "pass"})),
            Reply::Json(json!({"answer": "pass"})),
        ],
        describe(),
        false,
    )
    .await;
    for field in ["harness", "session"] {
        let mut call = call(&serving.bind);
        if field == "harness" {
            call.harness = "runner".into();
        } else {
            call.session = "other".into();
        }
        assert!(serving
            .plane
            .hook("transform-test", &serving.bind, call)
            .await
            .is_err());
    }
    assert_eq!(bind_count(&serving), 0);
}

#[tokio::test]
async fn budget_bounds_silent_provider_and_route_open_with_250ms_tolerance() {
    assert_eq!(STEP_TRANSFORM_HOOK_CAP, 30_000);
    for silent_bind in [false, true] {
        let serving = start(
            if silent_bind {
                "hook-bind-budget"
            } else {
                "hook-request-budget"
            },
            vec![Reply::Silent, Reply::Json(json!({"answer": "pass"}))],
            describe(),
            silent_bind,
        )
        .await;
        let started = Instant::now();
        assert!(serving
            .plane
            .hook_with_deadline(
                "transform-test",
                &serving.bind,
                call(&serving.bind),
                started + Duration::from_millis(150)
            )
            .await
            .is_err());
        assert!(started.elapsed() <= Duration::from_millis(400));
        assert_eq!(bind_count(&serving), 1, "the real provider was reached");
        if !silent_bind {
            assert_eq!(op_count(&serving, "transform.hook"), 1);
            assert!(hook(&serving).await);
            assert_eq!(bind_count(&serving), 2, "timed-out route was evicted");
        }
    }
}

#[tokio::test]
async fn cancelling_a_hook_evicts_the_inflight_route() {
    let serving = start(
        "hook-cancel",
        vec![Reply::Silent, Reply::Json(json!({"answer": "pass"}))],
        describe(),
        false,
    )
    .await;
    let plane = serving.plane.clone();
    let bind = serving.bind.clone();
    let task = tokio::spawn(async move { plane.hook("transform-test", &bind, call(&bind)).await });
    tokio::time::timeout(Duration::from_secs(2), serving.script.hook_seen.notified())
        .await
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(hook(&serving).await);
    assert_eq!(bind_count(&serving), 2);
}

#[tokio::test]
async fn transient_retry_consumes_the_original_deadline() {
    let serving = start(
        "hook-retry-budget",
        vec![Reply::Refuse("transient"), Reply::Silent],
        describe(),
        false,
    )
    .await;
    let started = Instant::now();
    assert!(serving
        .plane
        .hook_with_deadline(
            "transform-test",
            &serving.bind,
            call(&serving.bind),
            started + Duration::from_millis(200)
        )
        .await
        .is_err());
    assert_eq!(
        op_count(&serving, "transform.hook"),
        2,
        "retry actually reached the provider"
    );
    assert!(started.elapsed() <= Duration::from_millis(450));
}

#[tokio::test]
async fn declaration_vectors_and_refusals_are_preserved_for_admission() {
    let vectors = fixture("declare.json")["requests"]
        .as_array()
        .unwrap()
        .clone();
    let declaration = fixture("declare.json")["declarations"][0]["declaration"].clone();
    let serving = start(
        "hook-declare",
        std::iter::once(Reply::Refuse("transient"))
            .chain(vectors.iter().map(|_| Reply::Json(declaration.clone())))
            .collect(),
        describe(),
        false,
    )
    .await;
    let request = DeclareRequest::new(Default::default());
    assert!(
        matches!(serving.plane.declare("transform-test", &serving.bind, &request,
        Instant::now() + Duration::from_secs(2)).await,
        Err(broca_fetch_plan::FetchError::Refused { code, retryable: true, .. }) if code == "transient")
    );
    assert_eq!(
        op_count(&serving, "transform.declare"),
        1,
        "admission owns declaration retries"
    );
    for vector in &vectors {
        let request: DeclareRequest = serde_json::from_value(vector["request"].clone()).unwrap();
        assert_eq!(
            serving
                .plane
                .declare(
                    "transform-test",
                    &serving.bind,
                    &request,
                    Instant::now() + Duration::from_secs(2)
                )
                .await
                .unwrap(),
            declaration
        );
        let bodies = serving.script.bodies.lock().unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(bodies.last().unwrap()).unwrap(),
            json!({"method": "transform.declare", "params": vector["request"]})
        );
    }
    assert_eq!(bind_count(&serving), 2);
}
