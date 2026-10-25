//! Provider presets on real daemon routes, with all daemon state isolated under the rig.

use async_trait::async_trait;
use broca_core::{BatchCtx, DispatchResult, ToolManifest, ToolPlane};
use broca_fetch_plan::PlanAdmitter;
use broca_protocol::{BindIdentity, SessionScope, ToolCall, ToolCallId};
use broca_subc::{SubcConnection, SubcPlanAdmitter, SubcToolPlane};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use subc_control::ConsumerIdentity;
use tokio::process::Child;

const OWNER: &str = "preset-owner";
// AFT permits shell execution for the daemon's trusted broca runner identity.
// A test carrier with an arbitrary module id receives a shell-disabled remote tool facade.
const CARRIER: &str = "broca";
const SCOPE: &str = "preset-scope";

pub(super) fn sibling_bin(name: &str, key: &str) -> PathBuf {
    broca_test_support::subc_bin(name, key)
}

pub(super) struct Rig {
    child: Child,
    pub dir: PathBuf,
    pub connection_file: PathBuf,
    nonce: PathBuf,
    sync: PathBuf,
    events: PathBuf,
}

impl Drop for Rig {
    fn drop(&mut self) {
        broca_test_support::kill_and_reap_tree(&mut self.child);
        broca_test_support::dump_rig_process_logs_on_failure(&self.dir, "daemon");
        broca_test_support::remove_rig_dir(&self.dir);
    }
}

impl Rig {
    pub async fn start(aft: Option<&Path>) -> Self {
        let daemon = sibling_bin("ck-subc", "SUBC_CORE_BIN");
        let stub = sibling_bin("fake-aft-stub", "FAKE_AFT_STUB_BIN");
        assert!(
            daemon.exists() && stub.exists(),
            "required daemon/stub binaries missing: {daemon:?}, {stub:?}"
        );
        let daemon = broca_test_support::ckdev_binary(daemon, broca_test_support::scratch_root());
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-presets-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let config = dir.join("config");
        let data = dir.join("data");
        let runtime = dir.join("runtime");
        let home = dir.join("home");
        let project = dir.join("project");
        for path in [
            &config,
            &data,
            &runtime,
            &home,
            &project,
            &config.join("cortexkit"),
            &data.join("cortexkit/run"),
        ] {
            std::fs::create_dir_all(path).unwrap();
        }
        assert!(std::process::Command::new("git")
            .args(["init", "--quiet"])
            .arg(&project)
            .status()
            .unwrap()
            .success());
        std::fs::write(project.join("hello.txt"), "frozen preset reaches AFT\n").unwrap();
        // The child daemon derives its process roster and machine identity from XDG_DATA_HOME.
        // Keeping both under the rig prevents startup from sweeping production children or sharing its identity.
        let live_children = data.join("cortexkit/run/live-children.json");
        let machine_id = data.join("cortexkit/machine-id");
        std::fs::write(&machine_id, "0123456789abcdef0123456789abcdef\n").unwrap();
        let sync = dir.join("scopes.json");
        let events = dir.join("owner-events.jsonl");
        let nonce = dir.join("carrier.nonce");
        let module = |id: &str, extra: Value| {
            let mut env = extra;
            env["FAKE_AFT_MODULE_ID"] = json!(id);
            json!({"program": stub, "args": [], "env": env, "enabled": true, "reserved": true})
        };
        let mut modules = json!({
            OWNER: module(OWNER, json!({"FAKE_AFT_SCOPE_SYNC_PATH": sync, "FAKE_AFT_EVENTS_PATH": events})),
            CARRIER: module(CARRIER, json!({"FAKE_AFT_LAUNCH_NONCE_PATH": nonce})),
        });
        if let Some(aft) = aft {
            modules["aft"] = json!({"program": aft, "args": [], "env": {}, "enabled": true, "reserved": true, "health": {"deadline_ms": 20000, "failure_threshold": 5}});
        }
        std::fs::write(
            config.join("cortexkit/subc.jsonc"),
            serde_json::to_vec_pretty(
                &json!({"version": 1, "scope_authority_owners": [OWNER], "modules": modules}),
            )
            .unwrap(),
        )
        .unwrap();
        let child = broca_test_support::retry_executable_file_busy(|| {
            broca_test_support::tokio_rig_command(&daemon, &dir, "daemon")?
                .env("HOME", &home)
                .env("XDG_CONFIG_HOME", &config)
                .env("XDG_DATA_HOME", &data)
                .env("XDG_RUNTIME_DIR", &runtime)
                .env("XDG_STATE_HOME", dir.join("state"))
                .env("SUBC_PORT", "0")
                .env_remove("SUBC_MODULE_ID")
                .env_remove("SUBC_LAUNCH_NONCE")
                .env_remove("SUBC_LAUNCH_NONCE_FD")
                .current_dir(&project)
                .kill_on_drop(true)
                .spawn()
        })
        .unwrap();
        println!(
            "isolated live_children={} machine_id={}",
            live_children.display(),
            machine_id.display()
        );
        let rig = Self {
            child,
            connection_file: runtime.join("subc-connection.json"),
            dir,
            nonce,
            sync,
            events,
        };
        rig.wait_file(&rig.connection_file).await;
        rig
    }

    async fn wait_file(&self, path: &Path) -> String {
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            if let Ok(text) = std::fs::read_to_string(path) {
                if !text.is_empty() {
                    return text;
                }
            }
            assert!(
                Instant::now() < deadline,
                "{} missing; daemon stderr: {}",
                path.display(),
                self.logs()
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    pub fn logs(&self) -> String {
        broca_test_support::rig_process_logs(&self.dir, "daemon")
    }

    pub async fn connect(&self) -> Arc<SubcConnection> {
        let launch_nonce = self.wait_file(&self.nonce).await.trim().to_owned();
        Arc::new(
            SubcConnection::connect_with_consumer_identity(
                &self.connection_file,
                Some(ConsumerIdentity {
                    module_id: CARRIER.into(),
                    launch_nonce,
                }),
            )
            .await
            .unwrap(),
        )
    }

    pub async fn scope(&self) -> SessionScope {
        std::fs::write(&self.sync, serde_json::to_vec(&json!([{"ref": SCOPE, "scope_epoch": 1, "kind": "head", "attributes": {}, "carriers": [{"principal": {"kind": "reserved", "module_id": CARRIER}}]}])).unwrap()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let event = std::fs::read_to_string(&self.events)
                .unwrap_or_default()
                .lines()
                .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                .find(|event| {
                    event["corr"] == 1000001
                        && (event["kind"] == "scope_sync_response" || event["kind"] == "error")
                });
            if let Some(event) = event {
                assert_eq!(event["kind"], "scope_sync_response", "{event}");
                assert!(
                    event["body_json"]["results"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .all(|result| result["outcome"] != "refused"),
                    "{event}"
                );
                break;
            }
            assert!(
                Instant::now() < deadline,
                "scope sync timed out: {}",
                self.logs()
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        serde_json::from_value(json!({"owner": {"kind": "reserved", "module_id": OWNER}, "ref": SCOPE, "scope_epoch": 1})).unwrap()
    }

    pub fn session(&self, name: &str) -> BindIdentity {
        BindIdentity::new(
            self.dir.join("project").canonicalize().unwrap(),
            "preset-test",
            name,
        )
    }

    pub async fn stop(&mut self) {
        #[cfg(unix)]
        if let Some(pid) = self.child.id() {
            assert!(std::process::Command::new("kill")
                .args(["-TERM", &pid.to_string()])
                .status()
                .unwrap()
                .success());
            tokio::time::timeout(Duration::from_secs(30), self.child.wait())
                .await
                .expect("daemon and supervised modules shut down")
                .unwrap();
        }
        #[cfg(not(unix))]
        broca_test_support::kill_and_reap(&mut self.child);
    }
}

pub(super) fn call(name: &str, input: Value, id: &str) -> ToolCall {
    ToolCall {
        tool_call_id: ToolCallId(id.into()),
        tool_name: name.into(),
        input,
        thought_signature: None,
    }
}

pub(super) async fn dispatch(
    plane: &SubcToolPlane,
    session: BindIdentity,
    scope: Option<SessionScope>,
    manifest: &ToolManifest,
    call: &ToolCall,
) -> broca_protocol::ToolResult {
    let ctx = BatchCtx {
        media_capture: None,
        batch_id: broca_protocol::BatchId(format!("batch-{}", call.tool_call_id.0)),
        bind: session,
        run_id: broca_protocol::RunId("preset-run".into()),
        scope,
    };
    plane
        .batch_started(&ctx, std::slice::from_ref(call), manifest)
        .await
        .unwrap();
    let result = plane.dispatch(&ctx, call).await;
    plane.batch_finished(&ctx).await.unwrap();
    match result {
        DispatchResult::Completed(result) => result,
        other => panic!("call not completed: {other:?}"),
    }
}

pub(super) fn plan_for(tools: Value) -> Value {
    let composition = json!({"providers": [{"provider": "aft", "tools": tools}, {"provider": "prefrontal-core", "tools": []}], "step_transform_items": []});
    json!({"tool_items": [{"provider": "aft", "preset": "worker", "params": {}}], "system_text_items": [{"provider": "prefrontal-core", "preset": "worker", "params": {}}], "step_transform_items": [], "composition_digest": cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap(), "composition": composition})
}

pub(super) struct StandIn {
    pub seen: Arc<Mutex<Vec<Value>>>,
    pub id: &'static str,
}
#[async_trait]
impl subc_client_rs::ModuleHandler for StandIn {
    async fn handle(
        &self,
        _: subc_client_rs::RequestCtx,
        bytes: Vec<u8>,
    ) -> subc_client_rs::HandlerOutcome {
        let request: Value = serde_json::from_slice(&bytes).unwrap();
        self.seen.lock().unwrap().push(request.clone());
        let answer = if request["name"] == "tool.catalog" {
            let args = &request["arguments"];
            let tools = args["composition"]["providers"].as_array().unwrap().iter().find(|entry| entry["provider"] == self.id).unwrap()["tools"].as_array().unwrap().iter().map(|tool| {
                let schema = json!({"type": "object"});
                json!({"name": tool["name"], "capabilities": tool["capabilities"], "semantics": 1, "schema_digest": cortexkit_role_tool_provider::catalog::schema_digest(&schema).unwrap(), "description": "test tool", "input_schema": schema})
            }).collect::<Vec<_>>();
            let digest =
                cortexkit_role_tool_provider::catalog::composition_digest(&args["composition"])
                    .unwrap();
            let mut answer = json!({"generation": "1", "catalog_digest": "0".repeat(64), "composition_digest": digest, "tools": tools});
            if args.get("system_text").is_some() {
                answer["system_text"] = json!({"text": "", "item_digest": broca_fetch_plan::plan::sha256_hex(b""), "preflight_digest": "test", "composition_digest": digest});
            }
            answer
        } else {
            json!({"content": [], "isError": false})
        };
        subc_client_rs::HandlerOutcome::Response(serde_json::to_vec(&answer).unwrap())
    }
}

pub(super) struct SdkDescriber(pub subc_client_rs::ModuleHandle);
#[async_trait]
impl broca_subc::ScopeDescriber for SdkDescriber {
    async fn describe(
        &self,
        owner: &subc_protocol::Principal,
        scope_ref: &str,
    ) -> Result<broca_subc::DescribedScope, String> {
        let reply = self
            .0
            .scope_describe(owner.clone(), scope_ref.to_owned())
            .await
            .map_err(|error| error.to_string())?;
        Ok(broca_subc::DescribedScope {
            status: reply.status,
            scope_epoch: reply.scope_epoch,
            owner_synced: reply.owner_synced,
            owner_configured: reply.owner_configured,
            stamp: reply.scope,
        })
    }
}

pub(super) async fn register(
    rig: &Rig,
    id: &'static str,
    seen: Arc<Mutex<Vec<Value>>>,
) -> (subc_client_rs::ModuleHandle, tokio::task::JoinHandle<()>) {
    use subc_protocol::manifest::{
        Concurrency, ExecutionMode, IdentityScope, ModuleManifest, ProviderRole, Tool,
    };
    let manifest = ModuleManifest::builder(id, "1")
        .provides(vec![ProviderRole::ToolProvider {
            tools: vec![Tool {
                name: "read".into(),
                description: None,
                execution_mode: ExecutionMode::Pure,
                schema: json!({"type": "object"}),
            }],
            identity_scope: vec![IdentityScope::Project, IdentityScope::Session],
            concurrency: Concurrency::ModuleManaged,
            emits_push: false,
            sub_supervises: false,
        }])
        .build();
    let (handle, serve) =
        subc_client_rs::serve_with_handle(&rig.connection_file, manifest, StandIn { id, seen })
            .await
            .unwrap();
    (
        handle,
        tokio::spawn(async move {
            let _ = serve.await;
        }),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn planned_and_planless_calls_reach_the_provider_with_the_right_envelope() {
    let mut rig = Rig::start(None).await;
    let aft_seen = Arc::new(Mutex::new(Vec::new()));
    let (_, aft) = register(&rig, "aft", aft_seen.clone()).await;
    let (core_handle, core) =
        register(&rig, "prefrontal-core", Arc::new(Mutex::new(Vec::new()))).await;
    let conn = rig.connect().await;
    let scope = rig.scope().await;
    let session = rig.session("planned");
    let value = plan_for(json!([{"name": "read", "capabilities": []}]));
    let plan =
        broca_fetch_plan::parse_plan(&value, broca_fetch_plan::CompactionSupport::Unsupported)
            .unwrap();
    let frozen = SubcPlanAdmitter::new(conn.clone())
        .admit(&plan, &session, Some(&scope))
        .await
        .unwrap()
        .manifest;
    let manifest = ToolManifest::from_tools(frozen.tools.as_ref().clone())
        .with_plan_manifest(&frozen)
        .unwrap();
    let plane = SubcToolPlane::new(Arc::try_unwrap(conn).ok().unwrap(), "aft");
    plane.set_scope_describer(Arc::new(SdkDescriber(core_handle)));
    let result = dispatch(
        &plane,
        session.clone(),
        Some(scope.clone()),
        &manifest,
        &call("read", json!({}), "planned"),
    )
    .await;
    assert!(!result.is_error);
    let result = dispatch(
        &plane,
        rig.session("plain"),
        Some(scope),
        &ToolManifest::from_tools(frozen.tools.as_ref().clone()),
        &call("read", json!({}), "plain"),
    )
    .await;
    assert!(!result.is_error);
    let seen = aft_seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 3);
    assert_eq!(seen[0]["name"], "tool.catalog");
    assert_eq!(seen[0]["preset"], "worker");
    assert_eq!(
        serde_json::to_vec(&seen[0]["arguments"]["composition"]).unwrap(),
        serde_json::to_vec(&value["composition"]).unwrap()
    );
    assert_eq!(seen[1]["preset"], "worker");
    assert!(seen[2].get("preset").is_none());
    plane.close_session_routes(&session).await;
    aft.abort();
    core.abort();
    rig.stop().await;
}
