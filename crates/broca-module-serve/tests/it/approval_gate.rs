//! Scoped approval gates through the production broca process and a real ck-subc daemon.
//! Scripted providers log route operations; assertions use those logs and durable WAL order.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use broca_core::{RunIndex, WalStore};
use broca_protocol::BindIdentity;
use broca_subc::{RouteHandle, RouteOpenRetry, SubcConnection};
use broca_wal::{DeferredDecision, FramedRecord, WalRecord};
use serde_json::{json, Value};
use subc_client_rs::{HandlerOutcome, ModuleHandler};
use subc_control::ConsumerIdentity;
use subc_protocol::manifest::{
    Concurrency, ManagementOperation, ManagementOperationKind, ModuleManifest, ProviderRole,
};
use subc_protocol::scope::ScopeSelector;
use subc_protocol::Principal;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

const OWNER: &str = "approval-owner";
const TOOLS: &str = "approval-tools";
const APPROVED_BYTES: &str = r#"{"a":2,"f":"approved","z":0.000001}"#;
const QUESTION: &str = "el-scripted";
const DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

struct Script {
    wal: broca_core::FileWalStore,
    session: BindIdentity,
    logs: Mutex<Vec<Value>>,
    filings: Mutex<Vec<Value>>,
    dispatches: Mutex<Vec<Vec<u8>>>,
    model_requests: Mutex<Vec<Vec<u8>>>,
    answer: Mutex<Option<&'static str>>,
    withdraw_enabled: Mutex<bool>,
    expiry_ms: u64,
}
impl Script {
    fn records(&self) -> Vec<FramedRecord> {
        self.wal.read_records(&self.session).unwrap()
    }
    fn log(&self, event: Value) {
        if !matches!(
            event["event"].as_str(),
            Some("elicitation.answers" | "question_bind")
        ) {
            println!("APPROVAL EVENT {event}");
        }
        self.logs.lock().unwrap().push(event);
    }
}

fn response(value: Value) -> HandlerOutcome {
    HandlerOutcome::Response(serde_json::to_vec(&value).unwrap())
}

struct Tools(Arc<Script>);
#[async_trait::async_trait]
impl ModuleHandler for Tools {
    async fn on_bind(
        &self,
        req: &subc_client_rs::RouteBindRequest,
    ) -> subc_client_rs::BindDecision {
        self.0
            .log(json!({"event":"tool_bind", "scope":req.scope, "roles":req.role_versions}));
        subc_client_rs::BindDecision::accept()
    }
    async fn handle(&self, _: subc_client_rs::RequestCtx, body: Vec<u8>) -> HandlerOutcome {
        let request: Value = serde_json::from_slice(&body).unwrap();
        let method = request
            .get("method")
            .or_else(|| request.get("name"))
            .and_then(Value::as_str)
            .unwrap();
        let params = request
            .get("params")
            .or_else(|| request.get("arguments"))
            .unwrap();
        self.0.log(json!({"event":method, "params":params}));
        match method {
            "role.describe" => response(json!({"majors":[
                {"version":"step-transform-provider/v1", "ops":["role.describe","transform.declare","transform.hook"], "stability":"alpha"},
                {"version":"tool-provider/v1", "ops":["role.describe","tool.catalog"], "stability":"alpha"}
            ], "implementation_version":"test", "capabilities":[]})),
            "transform.declare" => response(json!({"subscriptions":subscriptions()})),
            "tool.catalog" => {
                let schema = json!({"type":"object"});
                response(
                    json!({"generation":"1", "catalog_digest":"scripted", "composition_digest":cortexkit_role_tool_provider::catalog::composition_digest(&params["composition"]).unwrap(),
                    "tools":[{"name":"work", "description":"does work", "schema_digest":cortexkit_role_tool_provider::catalog::schema_digest(&schema).unwrap(),
                        "semantics":1, "capabilities":[], "input_schema":schema}]}),
                )
            }
            "transform.hook" => match params["phase"].as_str() {
                Some("mutate") => {
                    response(json!({"answer":"mutate", "input":{"z":1e-6,"f":"approved","a":2}}))
                }
                Some("approve") => {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis();
                    response(
                        json!({"answer":"ask", "ask":{"prompt":"Allow work?", "expires_at_ms":now + u128::from(self.0.expiry_ms),
                        "on_expiry":"deny", "material_damage":false, "late_execution":"notify_only"}}),
                    )
                }
                Some("validate") => response(json!({"answer":"pass"})),
                _ => panic!("unexpected hook: {request}"),
            },
            "work" => {
                let frames = self.0.records();
                assert!(frames.iter().any(|frame| matches!(
                    &frame.record,
                    WalRecord::ApprovalRecorded {
                        decision: DeferredDecision::Approved,
                        ..
                    }
                )));
                assert!(frames.iter().any(|frame| matches!(&frame.record, WalRecord::ToolDispatchIntent {approval_digest: Some(digest), ..} if digest == DIGEST)));
                assert!(
                    String::from_utf8_lossy(&body)
                        .contains(&format!("\"arguments\":{APPROVED_BYTES}")),
                    "approved bytes must cross the real route unchanged"
                );
                assert_eq!(request["approval_digest"], DIGEST);
                self.0.dispatches.lock().unwrap().push(body);
                response(json!({"content":[{"type":"text","text":"worked"}], "isError":false}))
            }
            _ => panic!("unexpected tool provider request: {request}"),
        }
    }
}

struct Questions(Arc<Script>);
#[async_trait::async_trait]
impl ModuleHandler for Questions {
    async fn on_bind(
        &self,
        req: &subc_client_rs::RouteBindRequest,
    ) -> subc_client_rs::BindDecision {
        assert_eq!(
            req.principal,
            Some(Principal::Reserved {
                module_id: "broca".into()
            })
        );
        let scope = serde_json::to_value(&req.scope).unwrap();
        assert_eq!(scope["owner"], json!({"kind":"reserved","module_id":OWNER}));
        assert_eq!(scope["ref"], "approval-head");
        self.0.log(json!({"event":"question_bind", "scope":scope}));
        subc_client_rs::BindDecision::accept()
    }
    async fn handle(&self, _: subc_client_rs::RequestCtx, body: Vec<u8>) -> HandlerOutcome {
        let request: Value = serde_json::from_slice(&body).unwrap();
        let params = &request["params"];
        self.0
            .log(json!({"event":request["method"], "params":params}));
        match request["method"].as_str().unwrap() {
            "elicitation.request" => {
                let frames = self.0.records();
                let prepared = frames
                    .iter()
                    .find(|frame| matches!(frame.record, WalRecord::ToolCallDeferred { .. }))
                    .expect("Prepared must be durable before filing");
                assert!(params["call_key"]
                    .as_str()
                    .unwrap()
                    .ends_with(&format!(":{}", prepared.seq)));
                assert_eq!(params["args_jcs"], APPROVED_BYTES);
                use sha2::{Digest, Sha256};
                assert_eq!(
                    params["args_hash"],
                    format!("{:x}", Sha256::digest(APPROVED_BYTES.as_bytes()))
                );
                assert_eq!(params["executor"], "runner");
                assert_eq!(params["module"], TOOLS);
                assert_eq!(params["tool"], "work");
                let mut filings = self.0.filings.lock().unwrap();
                if let Some(first) = filings.first() {
                    assert_eq!(
                        params["call_key"], first["call_key"],
                        "resume must re-file the same key"
                    );
                }
                filings.push(params.clone());
                response(
                    json!({"elicitation_id":QUESTION, "args_hash":params["args_hash"], "approval_digest":DIGEST}),
                )
            }
            "elicitation.answers" => {
                assert!(params["since"].is_null() || params["since"].is_object());
                let answer = *self.0.answer.lock().unwrap();
                let records = answer.map_or_else(Vec::new, |choice| {
                    let filings = self.0.filings.lock().unwrap();
                    filings.first().map_or_else(Vec::new, |filing| vec![json!({"state":"answered", "call_key":filing["call_key"],
                        "elicitation_id":QUESTION, "answered_choice_id":choice, "approval_digest":DIGEST})])
                });
                response(
                    json!({"records":records, "cursor":{"provider_incarnation":"scripted-core", "seq":u64::from(answer.is_some())}}),
                )
            }
            "elicitation.withdraw" => {
                assert_eq!(params["elicitation_id"], QUESTION);
                if !*self.0.withdraw_enabled.lock().unwrap() {
                    return HandlerOutcome::Error {
                        code: "elicitation_unavailable".into(),
                        message: "retry withdrawal on next boot".into(),
                    };
                }
                response(json!({"elicitation_id":QUESTION, "state":"withdrawn"}))
            }
            other => panic!("unexpected elicitation op: {other}"),
        }
    }
}

fn subscriptions() -> Vec<Value> {
    ["mutate", "approve", "validate"].into_iter().map(|phase| json!({"hook":"pre_tool", "phase":phase, "ops":[], "on_unavailable":"refuse", "budget_ms":10000})).collect()
}
fn plan() -> Value {
    let items = json!([{"provider":TOOLS, "preset":"worker", "params":{}, "subscriptions":subscriptions()}]);
    let composition = json!({"providers":[{"provider":TOOLS, "tools":[{"name":"work", "capabilities":[]}]}], "step_transform_items":items});
    json!({"tool_items":[{"provider":TOOLS, "preset":"worker", "params":{}}], "system_text_items":[], "step_transform_items":items,
        "composition_digest":cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap(), "composition":composition})
}

struct Rig {
    daemon: tokio::process::Child,
    dir: PathBuf,
    connection: PathBuf,
    cli: PathBuf,
    script: Arc<Script>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}
impl Drop for Rig {
    fn drop(&mut self) {
        if std::thread::panicking() {
            dump(&self.records());
            fn logs(dir: &std::path::Path) {
                for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        logs(&path);
                    } else if matches!(
                        path.extension().and_then(|ext| ext.to_str()),
                        Some("jsonl" | "log")
                    ) {
                        println!(
                            "APPROVAL DIAGNOSTIC {}\n{}",
                            path.display(),
                            std::fs::read_to_string(&path).unwrap_or_default()
                        );
                    }
                }
            }
            logs(&self.dir.join("data"));
        }
        for task in &self.tasks {
            task.abort();
        }
        broca_test_support::kill_and_reap(&mut self.daemon);
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
impl Rig {
    async fn start(tag: &str, provider: bool) -> Option<Self> {
        let subc = broca_test_support::subc_bin("ck-subc", "SUBC_CORE_BIN");
        let stub = broca_test_support::subc_bin("fake-aft-stub", "FAKE_AFT_STUB_BIN");
        if !subc.exists() || !stub.exists() {
            assert!(
                std::env::var_os("BROCA_REQUIRE_SUBC").is_none(),
                "approval rig requires ck-subc and fake-aft-stub: {subc:?}, {stub:?}"
            );
            println!(
                "SKIP approval_gate: no real daemon fixtures (BROCA_REQUIRE_SUBC makes this fail)"
            );
            return None;
        }
        assert!(subc.is_absolute() && stub.is_absolute() && subc.exists() && stub.exists());
        let cli = subc.with_file_name(format!("ck{}", std::env::consts::EXE_SUFFIX));
        assert!(cli.exists(), "CLI must be beside the daemon");
        let dir = broca_test_support::scratch_root()
            .join(format!("broca-approval-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let dir = dir.canonicalize().unwrap();
        let session = BindIdentity::new(&dir, "approval-e2e", tag);
        let script = Arc::new(Script {
            wal: broca_core::FileWalStore::new(dir.join("state/wal")),
            session,
            logs: Mutex::new(vec![]),
            filings: Mutex::new(vec![]),
            dispatches: Mutex::new(vec![]),
            model_requests: Mutex::new(vec![]),
            answer: Mutex::new(None),
            withdraw_enabled: Mutex::new(true),
            expiry_ms: if provider { 600_000 } else { 3_000 },
        });
        let (model_url, model_task) = model(script.clone()).await;
        let config_path = dir.join("config/cortexkit/subc.jsonc");
        let runtime = dir.join("runtime");
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::create_dir_all(&runtime).unwrap();
        let scopes = dir.join("scopes.json");
        std::fs::write(
            &scopes,
            serde_json::to_vec(
                &json!([{"ref":"approval-head", "scope_epoch":1, "kind":"head", "attributes":{},
            "carriers":[{"principal":{"kind":"reserved","module_id":"broca"}}]}]),
            )
            .unwrap(),
        )
        .unwrap();
        let module = broca_test_support::ckdev_binary(env!("CARGO_BIN_EXE_ck-broca"), &dir);
        let config = json!({"version":1, "scope_authority_owners":[OWNER], "modules":{
            "broca":{"program":module, "args":[], "enabled":true, "reserved":true,
                "env":{"BROCA_STATE_ROOT":dir.join("state"), "BROCA_PROVIDER_BASE_URL":model_url}},
            OWNER:{"program":stub, "args":[], "enabled":true, "reserved":true,
                "env":{"FAKE_AFT_MODULE_ID":OWNER, "FAKE_AFT_SCOPE_SYNC_PATH":scopes, "FAKE_AFT_EVENTS_PATH":dir.join("owner-events.jsonl"),
                    "FAKE_AFT_LAUNCH_NONCE_PATH":dir.join("owner.nonce")}}
        }});
        std::fs::write(config_path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();
        let (subc, cli) = broca_test_support::ckdev_binary_with_companion(subc, cli, &dir);
        let daemon = broca_test_support::retry_executable_file_busy(|| {
            tokio::process::Command::new(&subc)
                .env("HOME", &dir)
                .env("XDG_CONFIG_HOME", dir.join("config"))
                .env("XDG_RUNTIME_DIR", &runtime)
                .env("XDG_DATA_HOME", dir.join("data"))
                .env("SUBC_PORT", "0")
                .env_remove("BROCA_STATE_ROOT")
                .env_remove("SUBC_MODULE_ID")
                .env_remove("SUBC_LAUNCH_NONCE")
                .env_remove("SUBC_LAUNCH_NONCE_FD")
                .kill_on_drop(true)
                .spawn()
        })
        .unwrap();
        let mut rig = Self {
            daemon,
            connection: runtime.join("subc-connection.json"),
            dir,
            cli,
            script,
            tasks: vec![model_task],
        };
        wait("connection file", || rig.connection.exists()).await;
        wait("scope.sync accepted", || {
            std::fs::read_to_string(rig.dir.join("owner-events.jsonl"))
                .unwrap_or_default()
                .lines()
                .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                .any(|event| {
                    event["kind"] == "scope_sync_response"
                        && event["body_json"]["results"]
                            .as_array()
                            .is_some_and(|results| {
                                results.iter().all(|entry| entry["outcome"] != "refused")
                            })
                })
        })
        .await;
        let tool_manifest = ModuleManifest::builder(TOOLS, env!("CARGO_PKG_VERSION"))
            .provides(vec![ProviderRole::ToolProvider {
                tools: vec![],
                identity_scope: vec![],
                concurrency: Concurrency::ModuleManaged,
                emits_push: false,
                sub_supervises: false,
            }])
            .build();
        let (_, serving) = subc_client_rs::serve_with_handle(
            &rig.connection,
            tool_manifest,
            Tools(rig.script.clone()),
        )
        .await
        .unwrap();
        rig.tasks
            .push(tokio::spawn(async move { serving.await.unwrap() }));
        if provider {
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
            let (_, serving) = subc_client_rs::serve_with_handle(
                &rig.connection,
                manifest,
                Questions(rig.script.clone()),
            )
            .await
            .unwrap();
            rig.tasks
                .push(tokio::spawn(async move { serving.await.unwrap() }));
        }
        Some(rig)
    }
    async fn owner(&self) -> SubcConnection {
        wait("owner nonce", || self.dir.join("owner.nonce").exists()).await;
        let nonce = std::fs::read_to_string(self.dir.join("owner.nonce")).unwrap();
        SubcConnection::connect_with_consumer_identity(
            &self.connection,
            Some(ConsumerIdentity {
                module_id: OWNER.into(),
                launch_nonce: nonce.trim().into(),
            }),
        )
        .await
        .unwrap()
    }
    async fn route(&self, conn: &SubcConnection, scoped: bool) -> RouteHandle {
        let retry = RouteOpenRetry::within(Duration::from_secs(30), Duration::from_millis(100));
        if scoped {
            conn.route_open_management_scoped_within(
                "broca",
                &self.script.session,
                ScopeSelector {
                    owner: Principal::Reserved {
                        module_id: OWNER.into(),
                    },
                    scope_ref: "approval-head".into(),
                    scope_epoch: Some(1),
                },
                retry,
            )
            .await
            .unwrap()
        } else {
            conn.route_open_management_within("broca", &self.script.session, retry)
                .await
                .unwrap()
        }
    }
    async fn send(&self, scoped: bool) -> Result<Value, broca_subc::ConnError> {
        let conn = self.owner().await;
        let route = self.route(&conn, scoped).await;
        conn.route_request(&route, serde_json::to_vec(&json!({"method":"session.send", "params":{
            "send_id":"approval-send", "prompt":"Do work", "model":{"provider":"mock", "model":"mock-model"},
            "on_restart":"resume", "plan":plan()
        }})).unwrap()).await
    }
    async fn pending(&self) -> Value {
        let conn = self.owner().await;
        let route = self.route(&conn, true).await;
        let run_id = self
            .records()
            .into_iter()
            .find_map(|record| match record {
                WalRecord::RunStarted { run_id, .. } => Some(run_id),
                _ => None,
            })
            .expect("run started before approval wait");
        tokio::time::timeout(Duration::from_secs(45), async {
            loop {
                let reply = conn
                    .route_request(
                        &route,
                        serde_json::to_vec(
                            &json!({"method":"run.status", "params":{"run_id":run_id}}),
                        )
                        .unwrap(),
                    )
                    .await
                    .unwrap()["result"]
                    .clone();
                if reply["awaiting_approval"]["pending"][0]["elicitation_id"] == QUESTION {
                    return reply;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("active status must publish the filed question")
    }
    fn records(&self) -> Vec<WalRecord> {
        self.script
            .records()
            .into_iter()
            .map(|frame| frame.record)
            .collect()
    }
    async fn filed(&self, n: usize) {
        wait("question filed", || {
            self.script.filings.lock().unwrap().len() >= n
        })
        .await;
        wait("filing marker synced", || self.records().iter().any(|record| matches!(record,
            WalRecord::AwaitingApproval {pending, ..} if pending.iter().any(|entry| entry.elicitation_id.as_deref() == Some(QUESTION))))).await;
    }
    async fn finished(&self) -> Vec<WalRecord> {
        wait("run finished", || {
            self.records()
                .iter()
                .any(|record| matches!(record, WalRecord::RunFinished { .. }))
        })
        .await;
        self.records()
    }
    async fn pid(&self) -> u32 {
        let roster = self.dir.join("data/cortexkit/run/live-children.json");
        let read = || {
            std::fs::read(&roster)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                .and_then(|roster| {
                    roster["children"]
                        .as_array()?
                        .iter()
                        .find(|entry| entry["module_id"] == "broca")
                        .cloned()
                })
                .and_then(|entry| entry["pid"].as_u64())
                .map(|pid| u32::try_from(pid).unwrap())
        };
        wait("broca process roster", || read().is_some()).await;
        read().unwrap()
    }
    async fn kill(&self) {
        let pid = self.pid().await;
        let process = subc_os::Process::open(pid)
            .unwrap()
            .expect("broca process is live");
        process.signal(subc_os::Signal::Kill).unwrap();
        wait("broca killed", || {
            subc_os::Process::open(pid).unwrap().is_none()
        })
        .await;
        self.script.log(json!({"event":"killed", "pid":pid}));
    }
    async fn restart(&self) {
        let output = tokio::process::Command::new(&self.cli)
            .args(["module", "restart", "broca"])
            .env("HOME", &self.dir)
            .env("XDG_CONFIG_HOME", self.dir.join("config"))
            .env("XDG_RUNTIME_DIR", self.dir.join("runtime"))
            .env("XDG_DATA_HOME", self.dir.join("data"))
            .env_remove("BROCA_STATE_ROOT")
            .output()
            .await
            .unwrap();
        assert!(
            output.status.success(),
            "real module restart failed: {output:?}"
        );
    }
}

async fn wait(label: &str, ready: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(45), async {
        while !ready() {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("did not observe {label}"));
}

async fn model(script: Arc<Script>) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let mut requests = 0;
        while let Ok((stream, _)) = listener.accept().await {
            let mut reader = BufReader::new(stream);
            let mut length = 0;
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).await.unwrap() == 0 {
                    return;
                }
                if line == "\r\n" {
                    break;
                }
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = value.trim().parse().unwrap();
                }
            }
            let mut body = vec![0; length];
            reader.read_exact(&mut body).await.unwrap();
            script.model_requests.lock().unwrap().push(body.clone());
            script.log(
                json!({"event":"model", "request":serde_json::from_slice::<Value>(&body).unwrap()}),
            );
            let sse = if requests == 0 {
                concat!(
                "data: {\"id\":\"approval-model\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"tool_calls\":[{\"index\":0,\"id\":\"call-work\",\"type\":\"function\",\"function\":{\"name\":\"work\",\"arguments\":\"{\\\"f\\\":\\\"original\\\"}\"}}]}}]}\n\n",
                "data: {\"id\":\"approval-model\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n", "data: [DONE]\n\n")
            } else {
                concat!(
                "data: {\"id\":\"approval-done\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"done\"}}]}\n\n",
                "data: {\"id\":\"approval-done\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n", "data: [DONE]\n\n")
            };
            requests += 1;
            let mut stream = reader.into_inner();
            stream.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{sse}", sse.len()).as_bytes()).await.unwrap();
            stream.shutdown().await.unwrap();
        }
    });
    (url, task)
}

fn count(records: &[WalRecord], predicate: fn(&WalRecord) -> bool) -> usize {
    records.iter().filter(|record| predicate(record)).count()
}
fn position(records: &[WalRecord], predicate: fn(&WalRecord) -> bool) -> usize {
    records
        .iter()
        .position(predicate)
        .expect("expected WAL record")
}
fn dump(records: &[WalRecord]) {
    for record in records {
        println!("APPROVAL WAL {}", serde_json::to_string(record).unwrap());
    }
}
fn no_dispatch(rig: &Rig, records: &[WalRecord]) {
    assert!(rig.script.dispatches.lock().unwrap().is_empty());
    assert_eq!(
        count(records, |record| matches!(
            record,
            WalRecord::ToolDispatchIntent { .. }
        )),
        0
    );
}
fn closed(rig: &Rig, records: &[WalRecord], decision: DeferredDecision, refusal: &str) {
    no_dispatch(rig, records);
    let answers: Vec<_> = records
        .iter()
        .filter_map(|record| match record {
            WalRecord::ApprovalRecorded {
                decision,
                closing_result,
                ..
            } => Some((*decision, closing_result)),
            _ => None,
        })
        .collect();
    assert_eq!(answers.len(), 1);
    assert_eq!(answers[0].0, decision);
    let result = answers[0].1.as_ref().expect("closing result");
    assert!(result.is_error);
    assert!(
        matches!(&result.output, broca_protocol::ToolOutput::Error {message} if message.starts_with(refusal))
    );
    assert_eq!(
        count(records, |record| matches!(
            record,
            WalRecord::ToolNoDispatch { .. } | WalRecord::ToolResult { .. }
        )),
        0
    );
    assert!(
        position(records, |r| matches!(r, WalRecord::ToolCallDeferred { .. }))
            < position(records, |r| matches!(r, WalRecord::ApprovalRecorded { .. }))
    );
    dump(records);
}
fn approved(rig: &Rig, records: &[WalRecord]) {
    let calls = rig.script.dispatches.lock().unwrap();
    assert_eq!(calls.len(), 1, "an approved call dispatches once");
    let raw = String::from_utf8_lossy(&calls[0]);
    assert!(raw.contains(&format!("\"arguments\":{APPROVED_BYTES}")));
    assert_ne!(
        serde_json::from_str::<Value>(APPROVED_BYTES)
            .unwrap()
            .to_string(),
        APPROVED_BYTES,
        "the fixture must detect argument reserialization"
    );
    let call: Value = serde_json::from_slice(&calls[0]).unwrap();
    assert_eq!(
        call["call_key"],
        rig.script.filings.lock().unwrap()[0]["call_key"]
    );
    assert_eq!(call["approval_digest"], DIGEST);
    for predicate in [
        (|r: &WalRecord| matches!(r, WalRecord::ToolCallDeferred { .. })) as fn(&WalRecord) -> bool,
        |r| {
            matches!(
                r,
                WalRecord::ApprovalRecorded {
                    decision: DeferredDecision::Approved,
                    ..
                }
            )
        },
        |r| matches!(r, WalRecord::ToolDispatchIntent { .. }),
        |r| matches!(r, WalRecord::ToolResult { .. }),
    ] {
        assert_eq!(count(records, predicate), 1);
    }
    let order: Vec<_> = [
        (|r: &WalRecord| matches!(r, WalRecord::ToolCallDeferred { .. })) as fn(&WalRecord) -> bool,
        |r| matches!(r, WalRecord::ApprovalRecorded { .. }),
        |r| matches!(r, WalRecord::ToolDispatchIntent { .. }),
        |r| matches!(r, WalRecord::ToolResult { .. }),
        |r| matches!(r, WalRecord::ToolBatchFinished { .. }),
        |r| matches!(r, WalRecord::RunFinished { .. }),
    ]
    .into_iter()
    .map(|predicate| position(records, predicate))
    .collect();
    assert!(
        order.windows(2).all(|pair| pair[0] < pair[1]),
        "durable gate order: {order:?}"
    );
    let logs = rig.script.logs.lock().unwrap();
    let filing = logs
        .iter()
        .position(|entry| entry["event"] == "elicitation.request")
        .unwrap();
    let dispatch = logs
        .iter()
        .position(|entry| entry["event"] == "work")
        .unwrap();
    assert!(filing < dispatch);
    let validates: Vec<_> = logs
        .iter()
        .enumerate()
        .filter(|(_, entry)| {
            entry["event"] == "transform.hook" && entry["params"]["phase"] == "validate"
        })
        .map(|(i, _)| i)
        .collect();
    assert_eq!(validates.len(), 2);
    assert!(validates[0] < filing && filing < validates[1] && validates[1] < dispatch);
    dump(records);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn approved_question_dispatches_saved_bytes_once() {
    tokio::time::timeout(Duration::from_secs(120), async {
        let Some(rig) = Rig::start("approve", true).await else {
            return;
        };
        rig.send(true).await.unwrap();
        rig.filed(1).await;
        let pending = rig.pending().await;
        assert_eq!(pending["state"], "active");
        assert_eq!(
            pending["awaiting_approval"]["pending"][0]["elicitation_id"],
            QUESTION
        );
        assert!(rig.script.dispatches.lock().unwrap().is_empty());
        *rig.script.answer.lock().unwrap() = Some("approve");
        approved(&rig, &rig.finished().await);
    })
    .await
    .expect("bounded real daemon approve scenario");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn denied_question_closes_named_refusal_without_dispatch() {
    tokio::time::timeout(Duration::from_secs(120), async {
        let Some(rig) = Rig::start("deny", true).await else {
            return;
        };
        rig.send(true).await.unwrap();
        rig.filed(1).await;
        *rig.script.answer.lock().unwrap() = Some("deny");
        closed(
            &rig,
            &rig.finished().await,
            DeferredDecision::Declined,
            "pre_tool_declined",
        );
    })
    .await
    .expect("bounded real daemon deny scenario");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_restart_keeps_question_open_and_resumes_same_wait() {
    tokio::time::timeout(Duration::from_secs(120), async {
        let Some(rig) = Rig::start("restart", true).await else {
            return;
        };
        rig.send(true).await.unwrap();
        rig.filed(1).await;
        let first = rig.pending().await;
        rig.restart().await;
        rig.filed(2).await;
        let resumed = rig.pending().await;
        assert_eq!(
            first["awaiting_approval"], resumed["awaiting_approval"],
            "restart resumes the same question and deadline"
        );
        let records = rig.records();
        assert_eq!(
            count(&records, |r| matches!(
                r,
                WalRecord::RunPaused {
                    reason: broca_protocol::PauseReason::Restart,
                    ..
                }
            )),
            1
        );
        assert_eq!(
            count(&records, |r| matches!(
                r,
                WalRecord::ModelStepFinished { .. }
            )),
            1
        );
        assert_eq!(
            count(&records, |r| matches!(r, WalRecord::RunStarted { .. })),
            1
        );
        assert_eq!(
            count(&records, |r| matches!(
                r,
                WalRecord::ToolBatchFinished { .. }
                    | WalRecord::ToolResult { .. }
                    | WalRecord::RunFinished { .. }
            )),
            0
        );
        assert!(!rig
            .script
            .logs
            .lock()
            .unwrap()
            .iter()
            .any(|entry| entry["event"] == "elicitation.withdraw"));
        *rig.script.answer.lock().unwrap() = Some("approve");
        approved(&rig, &rig.finished().await);
        assert_eq!(
            rig.script.filings.lock().unwrap().len(),
            2,
            "one original filing and one re-filing"
        );
        let Some(control) = Rig::start("restart-control", true).await else {
            panic!("control requires the same daemon fixtures")
        };
        control.send(true).await.unwrap();
        control.filed(1).await;
        *control.script.answer.lock().unwrap() = Some("approve");
        approved(&control, &control.finished().await);
        let resumed = rig.script.model_requests.lock().unwrap();
        let uninterrupted = control.script.model_requests.lock().unwrap();
        assert_eq!(resumed.len(), 2);
        assert_eq!(uninterrupted.len(), 2);
        assert_eq!(
            resumed[1], uninterrupted[1],
            "resumed next model request must be byte-identical to the no-restart control"
        );
    })
    .await
    .expect("bounded real daemon restart scenario");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_kill_seals_interrupted_and_boot_withdraws_terminal_debt() {
    tokio::time::timeout(Duration::from_secs(150), async {
        let Some(rig) = Rig::start("crash", true).await else {
            return;
        };
        *rig.script.withdraw_enabled.lock().unwrap() = false;
        let reply = rig.send(true).await.unwrap();
        let run_id = reply["result"]["run_id"].as_str().unwrap();
        rig.filed(1).await;
        rig.kill().await;
        let records = rig.finished().await;
        no_dispatch(&rig, &records);
        assert!(records.iter().any(|r| matches!(
            r,
            WalRecord::RunFinished {
                reason: broca_protocol::RunFinishReason::Interrupted,
                ..
            }
        )));
        assert_eq!(
            count(&records, |r| matches!(
                r,
                WalRecord::RunPaused { .. } | WalRecord::ApprovalRecorded { .. }
            )),
            0
        );
        assert_eq!(
            count(&records, |r| matches!(
                r,
                WalRecord::ToolResult {
                    withdraw_elicitation: Some(_),
                    ..
                }
            )),
            1
        );
        assert!(
            position(&records, |r| matches!(r, WalRecord::ToolResult { .. }))
                < position(&records, |r| matches!(r, WalRecord::RunFinished { .. }))
        );
        wait("first withdrawal attempt", || {
            rig.script
                .logs
                .lock()
                .unwrap()
                .iter()
                .any(|entry| entry["event"] == "elicitation.withdraw")
        })
        .await;
        let store = broca_store::SqliteStore::open(rig.dir.join("state/run-index.db")).unwrap();
        wait("terminal index with debt", || {
            store
                .lookup_run(run_id)
                .unwrap()
                .is_some_and(|row| row.state.is_terminal() && row.withdrawals_owed)
        })
        .await;
        assert_eq!(
            count(&rig.records(), |r| matches!(
                r,
                WalRecord::QuestionWithdrawalAcknowledged { .. }
            )),
            0
        );
        // A second boot sees only a terminal row, so the crash-orphan and restart-resume
        // passes cannot discover it. Only the withdrawal-debt boot task can open its actor.
        rig.kill().await;
        *rig.script.withdraw_enabled.lock().unwrap() = true;
        wait("withdrawal acknowledged after terminal-only boot", || {
            rig.records()
                .iter()
                .any(|r| matches!(r, WalRecord::QuestionWithdrawalAcknowledged { .. }))
        })
        .await;
        wait("withdrawal discovery cleared", || {
            !store.lookup_run(run_id).unwrap().unwrap().withdrawals_owed
        })
        .await;
        let records = rig.records();
        assert_eq!(
            count(&records, |r| matches!(
                r,
                WalRecord::QuestionWithdrawalAcknowledged { .. }
            )),
            1
        );
        assert_eq!(
            count(&records, |r| matches!(
                r,
                WalRecord::ModelStepFinished { .. } | WalRecord::RunFinished { .. }
            )),
            2
        );
        no_dispatch(&rig, &records);
        let logs = rig.script.logs.lock().unwrap();
        let killed = logs
            .iter()
            .rposition(|entry| entry["event"] == "killed")
            .unwrap();
        assert!(logs
            .iter()
            .skip(killed + 1)
            .any(|entry| entry["event"] == "elicitation.withdraw"));
        dump(&records);
    })
    .await
    .expect("bounded real daemon crash scenario");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unscoped_approve_plan_refuses_without_writing() {
    tokio::time::timeout(Duration::from_secs(120), async {
        let Some(rig) = Rig::start("scope", true).await else {
            return;
        };
        let path = rig.script.wal.wal_path(&rig.script.session);
        assert!(!path.exists());
        let error = rig.send(false).await.unwrap_err();
        assert!(
            error.to_string().contains("approve_requires_scope"),
            "{error}"
        );
        assert!(!path.exists(), "the refusal must not create a WAL");
        assert!(rig.script.filings.lock().unwrap().is_empty());
        assert!(
            rig.script.logs.lock().unwrap().is_empty(),
            "no provider fetch or model work before scope refusal"
        );
    })
    .await
    .expect("bounded real daemon scope scenario");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unreachable_provider_closes_unavailable_after_prepared_without_dispatch() {
    tokio::time::timeout(Duration::from_secs(120), async {
        let Some(rig) = Rig::start("unavailable", false).await else {
            return;
        };
        rig.send(true).await.unwrap();
        let records = rig.finished().await;
        closed(
            &rig,
            &records,
            DeferredDecision::Unavailable,
            "pre_tool_unavailable",
        );
        assert_eq!(
            count(&records, |r| matches!(
                r,
                WalRecord::ToolCallDeferred { .. }
            )),
            1
        );
        assert!(records.iter().any(|r| matches!(
            r,
            WalRecord::ApprovalRecorded {
                elicitation_id: None,
                decision: DeferredDecision::Unavailable,
                ..
            }
        )));
    })
    .await
    .expect("bounded real daemon unavailable scenario");
}
