//! Real step-transform routes shared by the daemon and actor integration tests.
#![allow(dead_code)]
use async_trait::async_trait;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use subc_client_rs::{HandlerOutcome, ModuleHandler};
use subc_protocol::manifest::{Concurrency, ModuleManifest, ProviderRole};
use tokio::sync::Notify;

pub const MODULE: &str = "scripted-transforms";
pub type Answer = dyn Fn(&Value) -> Value + Send + Sync;

pub struct State {
    pub subscriptions: Vec<Value>,
    pub requests: Mutex<Vec<Value>>,
    pub tool_calls: Mutex<Vec<Value>>,
    pub binds: Mutex<Vec<Value>>,
    pub answer: Mutex<Arc<Answer>>,
    pub never: Mutex<HashSet<String>>,
    pub held: Mutex<HashSet<String>>,
    pub release: Notify,
    pub refuse_declare: Mutex<bool>,
    pub entered: Notify,
    pub schema: Mutex<Value>,
}
impl State {
    pub fn new(subscriptions: Vec<Value>) -> Arc<Self> {
        Arc::new(Self {
            subscriptions,
            requests: Default::default(),
            tool_calls: Default::default(),
            binds: Default::default(),
            answer: Mutex::new(Arc::new(|_| {
                serde_json::to_value(
                    cortexkit_role_step_transform_provider::answer::HookAnswer::Pass,
                )
                .unwrap()
            })),
            never: Default::default(),
            held: Default::default(),
            release: Notify::new(),
            refuse_declare: Mutex::new(false),
            entered: Notify::new(),
            schema: Mutex::new(json!({"type":"object"})),
        })
    }
    pub fn answer_with(&self, f: impl Fn(&Value) -> Value + Send + Sync + 'static) {
        *self.answer.lock().unwrap() = Arc::new(f);
    }
    pub fn hooks(&self) -> Vec<Value> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r["method"] == "transform.hook")
            .map(|r| r["params"].clone())
            .collect()
    }
    pub fn counts(&self) -> HashMap<String, usize> {
        let mut counts = HashMap::new();
        for call in self.hooks() {
            *counts.entry(key(&call)).or_default() += 1;
        }
        counts
    }
    pub fn declares(&self) -> usize {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r["method"] == "transform.declare")
            .count()
    }
    pub fn clear(&self) {
        self.requests.lock().unwrap().clear();
    }
}
pub fn key(call: &Value) -> String {
    let hook = call["hook"].as_str().unwrap();
    call["phase"]
        .as_str()
        .map_or_else(|| hook.to_owned(), |phase| format!("{hook}:{phase}"))
}
pub fn subscription(hook: &str, phase: Option<&str>, policy: &str, budget: u64) -> Value {
    let mut sub = json!({"hook":hook,"ops":if hook=="pre_tool" {json!([])} else {json!(["append","prepend"])},"on_unavailable":policy,"budget_ms":budget});
    if let Some(phase) = phase {
        sub["phase"] = phase.into();
    }
    sub
}
pub fn append(text: &str) -> Value {
    serde_json::to_value(
        cortexkit_role_step_transform_provider::answer::HookAnswer::Ops {
            ops: vec![
                cortexkit_role_step_transform_provider::answer::Operation::Append {
                    block: 0,
                    text: text.into(),
                    note: None,
                },
            ],
        },
    )
    .unwrap()
}

pub fn plan(subscriptions: &[Value], grants: bool) -> Value {
    let items = json!([{"provider":MODULE,"preset":"worker","params":{"verbatim":[null,{"x":1}]},"subscriptions":subscriptions}]);
    let composition = json!({"providers":[{"provider":MODULE,"tools":[{"name":"work","capabilities":[]}]}],"step_transform_items":items});
    let mut plan = json!({"tool_items":[{"provider":MODULE,"preset":"worker","params":{}}],"system_text_items":[],"step_transform_items":items,
        "composition_digest":cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap(),"composition":composition});
    if grants {
        plan["user_grants"] = json!([{"module":MODULE,"hook":"post_tool","tools":["work"]}]);
    }
    plan
}
pub async fn send(
    rig: &mut super::delivery_rig::Rig,
    id: &str,
    plan: Option<&Value>,
    delivery: Option<&str>,
) -> subc_protocol::Frame {
    let mut params =
        json!({"prompt":"input","send_id":id,"model":{"provider":"fake","model":"fake"}});
    if let Some(plan) = plan {
        params["plan"] = plan.clone();
    }
    if let Some(delivery) = delivery {
        params["delivery"] = delivery.into();
    }
    rig.request(7, json!({"method":"session.send","params":params}))
        .await
}
struct Transform(Arc<State>);
#[async_trait]
impl ModuleHandler for Transform {
    async fn on_bind(
        &self,
        req: &subc_client_rs::RouteBindRequest,
    ) -> subc_client_rs::BindDecision {
        self.0.binds.lock().unwrap().push(
            json!({"roles":req.role_versions,"scope":req.scope,"harness":req.identity.harness}),
        );
        subc_client_rs::BindDecision::accept()
    }
    async fn handle(&self, _: subc_client_rs::RequestCtx, body: Vec<u8>) -> HandlerOutcome {
        let request: Value = serde_json::from_slice(&body).unwrap();
        self.0.requests.lock().unwrap().push(request.clone());
        let params = request
            .get("params")
            .or_else(|| request.get("arguments"))
            .unwrap();
        let answer = match request
            .get("method")
            .or_else(|| request.get("name"))
            .and_then(Value::as_str)
        {
            Some("role.describe") => {
                json!({"majors":[{"version":"step-transform-provider/v1","ops":["role.describe","transform.declare","transform.hook"],"stability":"alpha"},{"version":"tool-provider/v1","ops":["role.describe","tool.catalog"],"stability":"alpha"}],"implementation_version":"test","capabilities":[]})
            }
            Some("transform.declare") => {
                if *self.0.refuse_declare.lock().unwrap() {
                    return HandlerOutcome::Error {
                        code: "preset_refused".into(),
                        message: "declarations are unavailable on resume".into(),
                    };
                }
                json!({"subscriptions":self.0.subscriptions})
            }
            Some("transform.hook") => {
                self.0.entered.notify_one();
                let never = self.0.never.lock().unwrap().contains(&key(params));
                if never {
                    return std::future::pending().await;
                }
                loop {
                    let released = self.0.release.notified();
                    if !self.0.held.lock().unwrap().contains(&key(params)) {
                        break;
                    }
                    released.await;
                }
                let answer = self.0.answer.lock().unwrap().clone();
                answer(params)
            }
            Some("tool.catalog") => {
                let schema = self.0.schema.lock().unwrap().clone();
                json!({"generation":"1","catalog_digest":"catalog","composition_digest":cortexkit_role_tool_provider::catalog::composition_digest(&params["composition"]).unwrap(),
                    "tools":[{"name":"work","schema_digest":cortexkit_role_tool_provider::catalog::schema_digest(&schema).unwrap(),"semantics":1,"capabilities":[],"description":"does work","input_schema":schema,"result_ops":["append","prepend","replace"]}]})
            }
            Some("work") => {
                self.0.tool_calls.lock().unwrap().push(request.clone());
                loop {
                    let released = self.0.release.notified();
                    if !self.0.held.lock().unwrap().contains("work") {
                        break;
                    }
                    released.await;
                }
                json!({"content":[{"type":"text","text":"worked"}]})
            }
            _ => panic!("unexpected scripted provider request: {request}"),
        };
        HandlerOutcome::Response(serde_json::to_vec(&answer).unwrap())
    }
}
pub async fn register(connection: &Path, state: Arc<State>) -> tokio::task::JoinHandle<()> {
    register_as(connection, MODULE, state).await
}

pub async fn register_as(
    connection: &Path,
    module: &str,
    state: Arc<State>,
) -> tokio::task::JoinHandle<()> {
    let (_, serving) = subc_client_rs::serve_with_handle(
        connection,
        ModuleManifest::builder(module, env!("CARGO_PKG_VERSION"))
            .provides(vec![ProviderRole::ToolProvider {
                tools: vec![],
                identity_scope: vec![],
                concurrency: Concurrency::ModuleManaged,
                emits_push: false,
                sub_supervises: false,
            }])
            .build(),
        Transform(state),
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = serving.await;
    })
}
pub struct Daemon {
    pub child: broca_test_support::RigChild,
    pub connection_file: PathBuf,
    serving: tokio::task::JoinHandle<()>,
}
impl Drop for Daemon {
    fn drop(&mut self) {
        self.serving.abort();
        broca_test_support::kill_and_reap(&mut self.child);
    }
}
impl Daemon {
    pub async fn start(dir: &Path, state: Arc<State>) -> Self {
        let source = broca_test_support::subc_bin("ck-subc", "SUBC_CORE_BIN");
        assert!(source.exists(), "real ck-subc required: {source:?}");
        let runtime = dir.join("subc-runtime");
        let config = dir.join("config/cortexkit/subc.jsonc");
        std::fs::create_dir_all(&runtime).unwrap();
        std::fs::create_dir_all(config.parent().unwrap()).unwrap();
        std::fs::write(config, r#"{"version":1,"modules":{}}"#).unwrap();
        let child = broca_test_support::retry_executable_file_busy(|| {
            broca_test_support::tokio_rig_command(
                broca_test_support::ckdev_binary(source.clone(), dir),
                dir,
                "daemon",
            )?
            .env("XDG_CONFIG_HOME", dir.join("config"))
            .env("XDG_RUNTIME_DIR", &runtime)
            .env("XDG_DATA_HOME", dir.join("data-home"))
            .env("SUBC_PORT", "0")
            .env_remove("SUBC_LAUNCH_NONCE")
            .env_remove("SUBC_LAUNCH_NONCE_FD")
            .env_remove("SUBC_MODULE_ID")
            .kill_on_drop(true)
            .spawn()
        })
        .unwrap();
        let child = broca_test_support::RigChild::new(child, dir, "daemon");
        let connection_file = runtime.join("subc-connection.json");
        tokio::time::timeout(Duration::from_secs(20), async {
            while !connection_file.exists() {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();
        let serving = register(&connection_file, state).await;
        Self {
            child,
            connection_file,
            serving,
        }
    }
    pub async fn install(&self, rig: &super::delivery_rig::Rig) {
        let conn = Arc::new(
            broca_subc::SubcConnection::connect(&self.connection_file)
                .await
                .unwrap(),
        );
        rig.registry()
            .set_plan_admitter(Arc::new(broca_subc::SubcPlanAdmitter::new(conn.clone())));
        rig.set_hook_plane(Arc::new(broca_subc::SubcHookPlane::new(conn)));
    }
    pub async fn finish(mut self) {
        self.serving.abort();
        self.child.kill().await.unwrap();
        self.child.wait().await.unwrap();
    }
}

/// A fully frozen fixture for crash/resume: no admission path is available after the cut.
pub fn frozen(state: &State) -> Arc<broca_wal::PlanManifest> {
    let tool: broca_protocol::Tool = serde_json::from_value(json!({"name":"work","description":"does work","input_schema":state.schema.lock().unwrap().clone(),"module":MODULE,"execution_mode":"sequential"})).unwrap();
    Arc::new(broca_wal::PlanManifest {
        plan: broca_wal::OpaqueJson::from_json_text(plan(&state.subscriptions, false).to_string())
            .unwrap(),
        plan_identity: "frozen".into(),
        composition_digest: "frozen".into(),
        tools: vec![tool.clone()].into(),
        system_text: String::new(),
        system_digest: String::new(),
        join_version: 1,
        role_versions: Default::default(),
        fetches: vec![broca_wal::ProviderFetch {
            provider: MODULE.into(),
            outcome: broca_wal::FetchOutcome::Answered {
                reply: None,
                catalog_digest: "catalog".into(),
                tools: vec![tool].into(),
                text: None,
                result_ops: Some(
                    [(
                        "work".into(),
                        vec!["append".into(), "prepend".into(), "replace".into()],
                    )]
                    .into_iter()
                    .collect(),
                ),
            },
        }],
        declarations: vec![broca_wal::FrozenDeclaration {
            provider: MODULE.into(),
            answer: broca_wal::OpaqueJson::from_json_text(
                json!({"subscriptions":state.subscriptions}).to_string(),
            )
            .unwrap(),
        }],
    })
}
