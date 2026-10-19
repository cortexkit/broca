//! Fetch-plan admission through the real `ck-subc` daemon.
//!
//! Stand-in providers register with the daemon from inside this process, through the module
//! SDK, and serve the `tool-provider/v1` catalog: `tool.catalog` with each provider's tools and
//! system text, and `transform.declare` with its subscriptions, all read from the composition
//! the request carries. broca's subc admitter fetches the vendored head plan through the daemon
//! on routes it opens declaring `role_versions: {"tool-provider": "v1"}`, which each stand-in
//! sees on its bind. A plan naming a module the daemon does not know is refused
//! `fetch_unavailable` with reason `unknown_module`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broca_fetch_plan::PlanAdmitter;
use broca_subc::{SubcConnection, SubcPlanAdmitter};
use serde_json::{json, Value};

fn sibling_bin(name: &str, env_key: &str) -> PathBuf {
    broca_test_support::subc_bin(name, env_key)
}

/// A vendored plan as broca is sent it: without the `compaction_item` the head plans carry,
/// since broca does not take a compaction provider and refuses a plan naming one.
fn vector(relative: &str) -> Value {
    let mut plan = raw_vector(relative);
    plan.as_object_mut().unwrap().remove("compaction_item");
    plan
}

/// A vendored fetch-plan vector file, exactly as vendored.
fn raw_vector(relative: &str) -> Value {
    let path = relative.split('/').fold(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("broca-fetch-plan")
            .join("tests")
            .join("fixtures")
            .join("fetch-plan-v1"),
        |path, part| path.join(part),
    );
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::Digest;
    sha2::Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// What one stand-in saw: the role versions of each bind, and each op it was asked.
#[derive(Default)]
struct Seen {
    binds: Vec<Option<std::collections::BTreeMap<String, String>>>,
    ops: Vec<String>,
    catalogs: Vec<Value>,
}

/// A provider serving the v1 catalog for module `id`, answering from the request's composition.
struct StandIn {
    id: &'static str,
    seen: Arc<Mutex<Seen>>,
}

impl StandIn {
    fn catalog(&self, arguments: &Value) -> Value {
        let composition = &arguments["composition"];
        let digest =
            cortexkit_role_tool_provider::catalog::composition_digest(composition).unwrap();
        let mut tools: Vec<Value> = composition["providers"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["provider"] == self.id)
            .map(|entry| entry["tools"].as_array().unwrap().clone())
            .unwrap_or_default()
            .into_iter()
            .map(|tool| {
                let schema = json!({"type": "object"});
                json!({
                    "name": tool["name"],
                    "schema_digest": cortexkit_role_tool_provider::catalog::schema_digest(&schema).unwrap(),
                    "semantics": 1,
                    "capabilities": tool["capabilities"],
                    "description": format!("{} {}", self.id, tool["name"].as_str().unwrap()),
                    "input_schema": schema,
                })
            })
            .collect();
        if arguments.get("preset").is_none() {
            tools.clear();
        }
        let mut answer = json!({
            "generation": "1",
            "catalog_digest": sha256_hex(self.id.as_bytes()),
            "composition_digest": digest,
            "tools": tools,
        });
        if arguments.get("system_text").is_some() {
            let text = format!("{} over subc {digest}", self.id);
            answer["system_text"] = json!({
                "text": text,
                "item_digest": sha256_hex(text.as_bytes()),
                "preflight_digest": "preflight",
                "composition_digest": digest,
            });
        }
        answer
    }

    fn declare(&self, arguments: &Value) -> Value {
        let subscriptions = arguments["composition"]["step_transform_items"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|item| item["provider"] == self.id)
            .map(|item| item["subscriptions"].clone())
            .unwrap_or(json!([]));
        json!({ "subscriptions": subscriptions })
    }
}

#[async_trait]
impl subc_client_rs::ModuleHandler for StandIn {
    async fn handle(
        &self,
        _ctx: subc_client_rs::RequestCtx,
        body: Vec<u8>,
    ) -> subc_client_rs::HandlerOutcome {
        let request: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        let op = request["name"].as_str().unwrap_or_default().to_owned();
        self.seen.lock().unwrap().ops.push(op.clone());
        let answer = match op.as_str() {
            "tool.catalog" => {
                self.seen
                    .lock()
                    .unwrap()
                    .catalogs
                    .push(request["arguments"].clone());
                if self.id == "plexus" {
                    return subc_client_rs::HandlerOutcome::Error {
                        code: "catalog_refused".into(),
                        message: "optional catalog unavailable".into(),
                    };
                }
                self.catalog(&request["arguments"])
            }
            "transform.declare" => self.declare(&request["arguments"]),
            other => {
                return subc_client_rs::HandlerOutcome::Error {
                    code: "unsupported_operation".into(),
                    message: format!("no op {other}"),
                }
            }
        };
        subc_client_rs::HandlerOutcome::Response(serde_json::to_vec(&answer).unwrap())
    }

    async fn on_bind(
        &self,
        req: &subc_client_rs::RouteBindRequest,
    ) -> subc_client_rs::BindDecision {
        self.seen
            .lock()
            .unwrap()
            .binds
            .push(req.role_versions.clone());
        subc_client_rs::BindDecision::accept()
    }
}

fn stand_in_manifest(id: &str) -> subc_protocol::manifest::ModuleManifest {
    use subc_protocol::manifest::{Concurrency, ExecutionMode, IdentityScope, ProviderRole, Tool};
    subc_protocol::manifest::ModuleManifest::builder(id, env!("CARGO_PKG_VERSION"))
        .provides(vec![ProviderRole::ToolProvider {
            tools: vec![Tool {
                name: "noop".to_string(),
                description: Some("Stands in for the provider's own tools.".into()),
                execution_mode: ExecutionMode::Pure,
                schema: json!({"type": "object"}),
            }],
            identity_scope: vec![IdentityScope::Project, IdentityScope::Session],
            concurrency: Concurrency::ModuleManaged,
            emits_push: false,
            sub_supervises: false,
        }])
        .build()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_plan_is_fetched_through_the_real_daemon_on_v1_routes() {
    let daemon_source = sibling_bin("ck-subc", "SUBC_CORE_BIN");
    if !daemon_source.exists() {
        assert!(
            std::env::var_os("BROCA_REQUIRE_SUBC").is_none(),
            "plan_fetch_real_daemon: {daemon_source:?} is missing, but BROCA_REQUIRE_SUBC requires it"
        );
        println!("SKIP plan_fetch_real_daemon: {daemon_source:?} is missing");
        return;
    }
    let daemon_bin =
        broca_test_support::ckdev_binary(daemon_source, broca_test_support::scratch_root());
    std::env::remove_var("SUBC_MODULE_ID");
    std::env::remove_var("SUBC_LAUNCH_NONCE");
    let root = broca_test_support::scratch_root()
        .join(format!("broca-plan-fetch-daemon-{}", std::process::id()));
    broca_test_support::remove_rig_dir(&root);
    let config_home = root.join("config");
    let runtime_dir = root.join("runtime");
    let data_home = root.join("data-home");
    let project_root = root.join("project");
    for dir in [&runtime_dir, &data_home, &project_root] {
        std::fs::create_dir_all(dir).unwrap();
    }
    let config_path = config_home.join("cortexkit").join("subc.jsonc");
    std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
    std::fs::write(
        &config_path,
        serde_json::to_vec(&json!({"version": 1, "modules": {}})).unwrap(),
    )
    .unwrap();
    let connection_file = runtime_dir.join("subc-connection.json");
    let daemon = broca_test_support::retry_executable_file_busy(|| {
        broca_test_support::tokio_rig_command(&daemon_bin, &root, "daemon")?
            .env("XDG_CONFIG_HOME", &config_home)
            .env("XDG_RUNTIME_DIR", &runtime_dir)
            .env("XDG_DATA_HOME", &data_home)
            .env("SUBC_PORT", "0")
            .env_remove("SUBC_LAUNCH_NONCE")
            .env_remove("SUBC_LAUNCH_NONCE_FD")
            .env_remove("SUBC_MODULE_ID")
            .kill_on_drop(true)
            .spawn()
    })
    .expect("spawn ck-subc");
    let mut daemon = broca_test_support::RigChild::new(daemon, &root, "daemon");
    broca_test_support::wait_until("the daemon publishes its connection file", || async {
        if connection_file.exists() {
            Ok(())
        } else {
            Err("connection file absent".into())
        }
    })
    .await;

    let mut seen: HashMap<&str, Arc<Mutex<Seen>>> = HashMap::new();
    let mut serving = Vec::new();
    for id in ["aft", "magic-context", "prefrontal-core", "plexus"] {
        let record = Arc::new(Mutex::new(Seen::default()));
        seen.insert(id, record.clone());
        let (_, serve) = subc_client_rs::serve_with_handle(
            &connection_file,
            stand_in_manifest(id),
            StandIn { id, seen: record },
        )
        .await
        .expect("register a stand-in provider");
        serving.push(tokio::spawn(async move {
            let _ = serve.await;
        }));
    }

    let conn = SubcConnection::connect(&connection_file)
        .await
        .expect("connect to the daemon");
    let admitter = Arc::new(SubcPlanAdmitter::new(Arc::new(conn)));
    let session = broca_protocol::BindIdentity::new(
        project_root.canonicalize().unwrap(),
        "runner",
        "planned",
    );

    let plan = broca_fetch_plan::parse_plan(
        &vector("plans/broca-head-no-compaction.json"),
        broca_fetch_plan::CompactionSupport::Unsupported,
    )
    .unwrap();
    let manifest = admitter
        .admit(&plan, &session, None)
        .await
        .expect("the plan is admitted through the daemon")
        .manifest;
    let names: Vec<&str> = manifest.tools.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(
        names,
        [
            "bash",
            "bash_status",
            "edit",
            "read",
            "ctx_memory",
            "ctx_note",
            "ctx_search",
            "browser_use",
            "computer_use"
        ]
    );
    assert_eq!(
        manifest.system_text,
        ["prefrontal-core", "aft", "magic-context"]
            .map(|id| format!("{id} over subc {}", plan.composition_digest))
            .join("\n\n")
    );
    let v1 = Some(
        [("tool-provider".to_owned(), "v1".to_owned())]
            .into_iter()
            .collect(),
    );
    for (id, record) in &seen {
        if *id == "plexus" {
            continue;
        }
        let record = record.lock().unwrap();
        assert!(!record.binds.is_empty(), "{id} was fetched");
        assert!(
            record.binds.iter().all(|versions| *versions == v1),
            "{id}: every fetch route declared tool-provider v1: {:?}",
            record.binds
        );
        let catalogs = record.ops.iter().filter(|op| *op == "tool.catalog").count();
        assert_eq!(catalogs, 1, "{id} answered one catalog fetch");
    }
    let mut magic_context_ops = seen["magic-context"].lock().unwrap().ops.clone();
    magic_context_ops.sort();
    assert_eq!(magic_context_ops, ["tool.catalog"]);

    // A module the daemon does not know.
    let unknown = broca_fetch_plan::parse_plan(
        &vector("plans/broca-worker-unknown-provider.json"),
        broca_fetch_plan::CompactionSupport::Unsupported,
    )
    .unwrap();
    let refusal = admitter
        .admit(&unknown, &session, None)
        .await
        .expect_err("an unknown module is refused");
    assert_eq!(refusal.code(), "fetch_unavailable");
    assert_eq!(
        refusal.detail(),
        json!({"provider": "no-such-module", "reason": "unknown_module"})
    );

    // The real daemon fetcher is installed on the session registry, so admission, WAL
    // freezing, model rendering and restart all exercise the same manifest.
    use crate::delivery_rig::{result, Rig, Step};
    let mut rig = Rig::start("optional-tool-real-daemon").await;
    rig.registry().set_plan_admitter(admitter);
    rig.script.push(&[Step::Text("admitted")]);
    rig.bind(7).await;
    // This rig binds an unscoped route, and a `worker` tool preset is admitted only over a
    // scoped one (a scope the real daemon must also hold live). Ask for a non-confining preset;
    // the composition, and so its digest, does not carry the tool items' presets.
    let mut optional = raw_vector("plans/broca-worker-optional-tool-with-text.json");
    for item in optional["tool_items"].as_array_mut().unwrap() {
        item["preset"] = json!("reader");
    }
    let reply = result(&rig.request(7, json!({"method":"session.send", "params":{
        "send_id":"optional-first", "prompt":"hi", "model":{"provider":"fake","model":"fake"},
        "plan":optional
    }})).await);
    rig.wait_for_finished(1).await;
    let baseline = reply["baseline"].clone();
    assert_eq!(
        baseline["composition_digest"],
        "ae5f5d0a9238f443ea4676e31000c9233381a4c07ee962bd941c3d51fe933fb4"
    );
    assert!(!baseline["tool_names"]
        .as_array()
        .unwrap()
        .contains(&json!("web_search")));
    assert_eq!(
        baseline["absent_items"],
        json!([
            {"provider":"plexus","kind":"tool","reason":"refused","provider_code":"catalog_refused"},
            {"provider":"plexus","kind":"system_text","reason":"refused","provider_code":"catalog_refused"}
        ])
    );
    let frozen = rig
        .records()
        .iter()
        .find_map(|r| match r {
            broca_wal::WalRecord::RunStarted { manifest, .. } => manifest.clone(),
            _ => None,
        })
        .unwrap();
    assert!(frozen
        .system_text
        .contains(baseline["composition_digest"].as_str().unwrap()));
    assert!(!frozen
        .system_text
        .contains(optional["composition_digest"].as_str().unwrap()));
    let expected_composition =
        raw_vector("compositions/broca-worker-optional-tool-with-text-admitted.json");
    for id in ["aft", "prefrontal-core"] {
        let record = seen[id].lock().unwrap();
        let request = record.catalogs.last().unwrap();
        assert_eq!(request["composition"], expected_composition);
        assert!(request.get("preset").is_none());
    }
    let counts: HashMap<_, _> = seen
        .iter()
        .map(|(id, s)| (*id, s.lock().unwrap().ops.len()))
        .collect();
    assert_eq!(counts["plexus"], 1);
    rig.wait_closed().await;
    let mut rig = rig.restart().await;
    rig.bind(7).await;
    let resumed = result(
        &rig.request(7, json!({"method":"session.baseline","params":{}}))
            .await,
    );
    assert_eq!(resumed["baseline"], baseline);
    rig.script.push(&[Step::Text("resumed")]);
    let continued = rig.request(7, json!({"method":"session.send","params":{
        "send_id":"optional-resumed", "prompt":"continue", "model":{"provider":"fake","model":"fake"}
    }})).await;
    assert!(
        result(&continued)["run_id"].is_string(),
        "resume admitted a new run"
    );
    rig.wait_for_finished(2).await;
    for (id, record) in &seen {
        assert_eq!(
            record.lock().unwrap().ops.len(),
            counts[id],
            "resume never refetches {id}"
        );
    }
    rig.finish().await;

    for task in serving {
        task.abort();
    }
    broca_test_support::kill_and_reap_tree(&mut daemon);
    drop(daemon);
    broca_test_support::remove_rig_dir(&root);
}
