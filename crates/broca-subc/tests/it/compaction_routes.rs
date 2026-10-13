//! Real ck-subc with an in-process SDK provider; vectors pin the role wire surface.
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use broca_core::{
    compaction_provider::{setup::SetupRequest, status::StepStatus},
    CompactionPlane,
};
use broca_protocol::BindIdentity;
use broca_subc::SubcCompactionPlane;
use serde_json::{json, Value};
use tokio::time::Instant;

use super::common::Rig;

fn fixture(name: &str) -> Value {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/compaction-provider-v1");
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
    describe: Value,
    replies: Mutex<VecDeque<Reply>>,
    binds: Mutex<Vec<Value>>,
    bodies: Mutex<Vec<Vec<u8>>>,
    seen: tokio::sync::Notify,
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
            "identity": request.identity, "role_versions": request.role_versions, "scope": request.scope,
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
                serde_json::to_vec(&self.0.describe).unwrap(),
            );
        }
        assert!(
            matches!(
                request["method"].as_str(),
                Some("compaction.setup" | "compaction.step")
            ),
            "not a tool-call envelope: {request}"
        );
        self.0.seen.notify_one();
        let reply = self
            .0
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("no extra calls or retries");
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
    plane: Arc<SubcCompactionPlane>,
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
        describe: description,
        replies: Mutex::new(replies.into()),
        binds: Mutex::new(vec![]),
        bodies: Mutex::new(vec![]),
        seen: tokio::sync::Notify::new(),
        bind_silent,
    });
    let mut serves = Vec::new();
    for provider in ["compaction-test", "compaction-other"] {
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
            serve
                .await
                .expect("scripted compaction provider serve loop");
        }));
    }
    let bind = BindIdentity::new(&rig.dir, "broca", "ses_01J9Z");
    Serving {
        plane: Arc::new(SubcCompactionPlane::new(conn)),
        script,
        bind,
        serve: serves,
        _rig: rig,
    }
}
fn setup(bind: &BindIdentity) -> SetupRequest {
    let mut request = fixture("setup.json")["requests"][0]["request"].clone();
    request["session"] = json!(bind.session);
    request["harness"] = json!(bind.harness);
    serde_json::from_value(request).unwrap()
}
fn step(bind: &BindIdentity) -> StepStatus {
    let mut request = fixture("status.json")["requests"][0]["request"].clone();
    request["session"] = json!(bind.session);
    request["harness"] = json!(bind.harness);
    serde_json::from_value(request).unwrap()
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
async fn step_ok(serving: &Serving) -> bool {
    serving
        .plane
        .step("compaction-test", &serving.bind, step(&serving.bind))
        .await
        .is_ok()
}

#[tokio::test]
async fn setup_and_step_use_the_unscoped_role_route_and_frozen_vectors() {
    let setup_vectors = fixture("setup.json");
    let status_vectors = fixture("status.json");
    let answer_vectors = fixture("answers.json");
    let replies = setup_vectors["requests"]
        .as_array()
        .unwrap()
        .iter()
        .map(|_| Reply::Json(setup_vectors["answers"][0]["answer"].clone()))
        .chain(
            status_vectors["requests"]
                .as_array()
                .unwrap()
                .iter()
                .map(|_| Reply::Json(answer_vectors["answers"][0]["answer"].clone())),
        )
        .collect();
    let serving = start("compaction-wire", replies, describe(), false).await;
    let mut expected = vec![];
    for vector in setup_vectors["requests"].as_array().unwrap() {
        let request: SetupRequest = serde_json::from_value(vector["request"].clone()).unwrap();
        let answer = serving
            .plane
            .setup("compaction-test", &serving.bind, request)
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(answer).unwrap(),
            setup_vectors["answers"][0]["answer"]
        );
        expected.push(json!({"method": "compaction.setup", "params": vector["request"]}));
    }
    for vector in status_vectors["requests"].as_array().unwrap() {
        let request: StepStatus = serde_json::from_value(vector["request"].clone()).unwrap();
        let answer = serving
            .plane
            .step("compaction-test", &serving.bind, request)
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(answer).unwrap(),
            answer_vectors["answers"][0]["answer"]
        );
        expected.push(json!({"method": "compaction.step", "params": vector["request"]}));
    }
    assert_eq!(bind_count(&serving), 1);
    assert_eq!(op_count(&serving, "role.describe"), 1);
    let actual: Vec<Value> = serving
        .script
        .bodies
        .lock()
        .unwrap()
        .iter()
        .skip(1)
        .map(|body| serde_json::from_slice(body).unwrap())
        .collect();
    assert_eq!(
        actual, expected,
        "preset, params, composition and status pass through unchanged"
    );
    {
        let binds = serving.script.binds.lock().unwrap();
        assert_eq!(binds[0]["identity"]["harness"], "runner");
        assert_eq!(
            binds[0]["role_versions"],
            json!({"compaction-provider": "v1"})
        );
        assert!(binds[0].get("scope").is_none_or(Value::is_null));
    }
    serving.plane.close_session_routes(&serving.bind).await;
}

#[tokio::test]
async fn route_slots_are_isolated_by_session_harness_root_and_provider() {
    let serving = start(
        "compaction-isolation",
        (0..6)
            .map(|_| {
                Reply::Json(json!({
                    "answer": "noop", "request_id": "cr-000042"
                }))
            })
            .collect(),
        describe(),
        false,
    )
    .await;
    let mut keys = vec![serving.bind.clone(); 5];
    keys[1].session = "other-session".into();
    keys[2].harness = "other-harness".into();
    keys[3].project_root = serving._rig.dir.join("other-root");
    for (index, bind) in keys.iter().enumerate() {
        let provider = if index == 4 {
            "compaction-other"
        } else {
            "compaction-test"
        };
        serving
            .plane
            .step(provider, bind, step(bind))
            .await
            .unwrap();
    }
    assert!(step_ok(&serving).await);
    assert_eq!(bind_count(&serving), 5);
    assert_eq!(op_count(&serving, "role.describe"), 5);
}

#[tokio::test]
async fn setup_and_step_refuse_mismatched_caller_identity_before_opening_a_route() {
    let serving = start("compaction-identity", vec![], describe(), false).await;
    for mismatch in ["session", "harness"] {
        let mut request = serde_json::to_value(setup(&serving.bind)).unwrap();
        request[mismatch] = json!("wrong");
        assert!(serving
            .plane
            .setup(
                "compaction-test",
                &serving.bind,
                serde_json::from_value(request).unwrap()
            )
            .await
            .is_err());
        let mut request = serde_json::to_value(step(&serving.bind)).unwrap();
        request[mismatch] = json!("wrong");
        assert!(serving
            .plane
            .step(
                "compaction-test",
                &serving.bind,
                serde_json::from_value(request).unwrap()
            )
            .await
            .is_err());
    }
    assert_eq!(bind_count(&serving), 0);
    assert!(serving.script.bodies.lock().unwrap().is_empty());
}

#[tokio::test]
async fn incomplete_role_discovery_never_calls_setup_or_step() {
    for (index, vector) in fixture("role-describe.json")["refused"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        let serving = start(
            &format!("compaction-discovery-{index}"),
            vec![],
            vector["answer"].clone(),
            false,
        )
        .await;
        assert!(serving
            .plane
            .setup("compaction-test", &serving.bind, setup(&serving.bind))
            .await
            .is_err());
        assert!(!step_ok(&serving).await);
        assert_eq!(op_count(&serving, "compaction.setup"), 0);
        assert_eq!(op_count(&serving, "compaction.step"), 0);
        assert_eq!(
            bind_count(&serving),
            2,
            "bad discovery invalidates the route"
        );
    }
}

#[tokio::test]
async fn unknown_answer_tags_are_failed_calls_and_invalidate_the_route() {
    let serving = start(
        "compaction-unknown",
        vec![
            Reply::Json(json!({"answer": "future-setup", "request_id": "cr-000001"})),
            Reply::Json(json!({"answer": "future-step", "request_id": "cr-000042"})),
            Reply::Json(json!({"answer": "noop", "request_id": "cr-000042"})),
        ],
        describe(),
        false,
    )
    .await;
    assert!(serving
        .plane
        .setup("compaction-test", &serving.bind, setup(&serving.bind))
        .await
        .is_err());
    assert!(!step_ok(&serving).await);
    assert!(step_ok(&serving).await);
    assert_eq!(bind_count(&serving), 3);
    assert_eq!(op_count(&serving, "role.describe"), 3);
    assert_eq!(op_count(&serving, "compaction.step"), 2);
}

#[tokio::test]
async fn role_refuse_answers_are_returned_but_transport_refusals_are_not_retried() {
    let setup_refuse = fixture("setup.json")["answers"][4]["answer"].clone();
    let step_refuse = fixture("answers.json")["answers"][6]["answer"].clone();
    let serving = start(
        "compaction-refuse",
        vec![
            Reply::Json(setup_refuse.clone()),
            Reply::Json(step_refuse.clone()),
            Reply::Refuse("transient"),
            Reply::Json(json!({"answer":"noop", "request_id":"cr-000042"})),
        ],
        describe(),
        false,
    )
    .await;
    let answer = serving
        .plane
        .setup("compaction-test", &serving.bind, setup(&serving.bind))
        .await
        .unwrap();
    assert_eq!(serde_json::to_value(answer).unwrap(), setup_refuse);
    let answer = serving
        .plane
        .step("compaction-test", &serving.bind, step(&serving.bind))
        .await
        .unwrap();
    assert_eq!(serde_json::to_value(answer).unwrap(), step_refuse);
    assert_eq!(bind_count(&serving), 1);
    assert!(!step_ok(&serving).await);
    assert_eq!(
        op_count(&serving, "compaction.step"),
        2,
        "no retry of the issued request"
    );
    assert!(step_ok(&serving).await);
    assert_eq!(bind_count(&serving), 2);
}

#[tokio::test]
async fn deadline_bounds_route_open_queueing_and_silent_provider_calls() {
    let serving = start("compaction-bind-deadline", vec![], describe(), true).await;
    let started = Instant::now();
    assert!(serving
        .plane
        .setup_with_deadline(
            "compaction-test",
            &serving.bind,
            setup(&serving.bind),
            started + Duration::from_millis(100)
        )
        .await
        .is_err());
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(op_count(&serving, "compaction.setup"), 0);

    let serving = start(
        "compaction-call-deadline",
        vec![
            Reply::Silent,
            Reply::Silent,
            Reply::Json(json!({"answer":"noop", "request_id":"cr-000042"})),
        ],
        describe(),
        false,
    )
    .await;
    assert!(serving
        .plane
        .setup_with_deadline(
            "compaction-test",
            &serving.bind,
            setup(&serving.bind),
            Instant::now() + Duration::from_millis(200)
        )
        .await
        .is_err());
    let plane = serving.plane.clone();
    let bind = serving.bind.clone();
    let blocked = tokio::spawn(async move {
        plane
            .step_with_deadline(
                "compaction-test",
                &bind,
                step(&bind),
                Instant::now() + Duration::from_secs(1),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        // The setup consumed the first notification; wait until the silent step is dispatched.
        loop {
            if op_count(&serving, "compaction.step") == 1 {
                break;
            }
            serving.script.seen.notified().await;
        }
    })
    .await
    .unwrap();
    let started = Instant::now();
    assert!(serving
        .plane
        .step_with_deadline(
            "compaction-test",
            &serving.bind,
            step(&serving.bind),
            started + Duration::from_millis(50)
        )
        .await
        .is_err());
    assert!(started.elapsed() < Duration::from_millis(500));
    assert_eq!(
        op_count(&serving, "compaction.step"),
        1,
        "queued request must expire before dispatch"
    );
    blocked.abort();
    assert!(blocked.await.unwrap_err().is_cancelled());
    assert!(step_ok(&serving).await);
    assert_eq!(
        bind_count(&serving),
        3,
        "timeouts and cancelled calls rediscover"
    );
}
