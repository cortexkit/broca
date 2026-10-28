//! Scoped approval gates through the production broca process and a real ck-subc daemon.
//! A scripted Cingulate consent service fills checked-in wire examples with each call's
//! tool identity and argument hash. The tests check that those fields bind the person's
//! answer to the saved call, whose exact argument bytes reach the tool only after approval
//! is durable. Consent issues no digest, so tool requests must omit approval_digest.
//! Set CK_CINGULATE_BIN (or place .test-rigs/ck-cingulate) to test the real service;
//! BROCA_REQUIRE_CINGULATE makes a missing build fail rather than skip.

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

fn consent_vector(path: &str) -> Value {
    let root =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../broca-subc/tests/fixtures/consent");
    serde_json::from_slice(&std::fs::read(root.join(path)).unwrap()).unwrap()
}

struct Script {
    wal: broca_core::FileWalStore,
    session: BindIdentity,
    logs: Mutex<Vec<Value>>,
    filings: Mutex<Vec<Value>>,
    dispatches: Mutex<Vec<Vec<u8>>>,
    model_requests: Mutex<Vec<Vec<u8>>>,
    answer: Mutex<Option<&'static str>>,
    withdraw_enabled: Mutex<bool>,
    /// While set, a consent route opened after the card was filed waits in `on_bind`.
    /// Broca opens one route per consent operation, so this parks the run's first
    /// `consent.await` before Cingulate sees it. A test uses it to land a restart
    /// between filing the card and waiting on it.
    hold_question_binds: Mutex<bool>,
    expiry_ms: u64,
    created_at_ms: u128,
}
impl Script {
    fn records(&self) -> Vec<FramedRecord> {
        self.wal.read_records(&self.session).unwrap()
    }
    fn log(&self, event: Value) {
        if !matches!(
            event["event"].as_str(),
            Some("consent.answers" | "question_bind")
        ) {
            // Wall-clock milliseconds line these events up with the timestamps in the
            // daemon and broca logs that the rig prints when a test fails.
            let at_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis();
            println!("APPROVAL EVENT at_ms={at_ms} {event}");
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
                assert!(frames
                    .iter()
                    .any(|frame| matches!(&frame.record, WalRecord::ToolDispatchIntent { .. })));
                assert!(
                    String::from_utf8_lossy(&body)
                        .contains(&format!("\"arguments\":{APPROVED_BYTES}")),
                    "approved bytes must cross the real route unchanged"
                );
                assert!(request.get("approval_digest").is_none());
                self.0.dispatches.lock().unwrap().push(body);
                response(json!({"content":[{"type":"text","text":"worked"}], "isError":false}))
            }
            _ => panic!("unexpected tool provider request: {request}"),
        }
    }
}

struct Consent(Arc<Script>);

impl Script {
    fn consent_record(&self) -> Value {
        let record = if self.answer.lock().unwrap().is_some() {
            consent_vector("upstream/consent-requester-v2/cerebellum-ref-answers-reply.json")
                ["result"]["records"][0]
                .clone()
        } else {
            consent_vector("upstream/consent-requester-v2/cerebellum-ref-await-reply.json")
                ["result"]
                .clone()
        };
        let mut record = record;
        let filings = self.filings.lock().unwrap();
        let filing = filings.first().expect("card exists before readback");
        for (key, value) in filing.as_object().unwrap() {
            record[key] = value.clone();
        }
        record["elicitation_id"] = json!(QUESTION);
        record["requester_label"] = json!("reserved:broca");
        record["scope"] =
            json!({"owner":{"kind":"reserved","module_id":OWNER},"ref":"approval-head"});
        record["subject"] = json!({"agent_id":"approval-head","head_agent_id":"approval-head"});
        record["created_at_ms"] = json!(self.created_at_ms);
        record["expires_at_ms"] = json!(self.created_at_ms + u128::from(self.expiry_ms));
        if let Some(choice) = *self.answer.lock().unwrap() {
            record["answered_at_ms"] = json!(self.created_at_ms + 1);
            record["state"] = json!("answered");
            record["answered_choice_id"] = json!(choice);
        }
        record
    }
}
#[async_trait::async_trait]
impl ModuleHandler for Consent {
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
        let filed = !self.0.filings.lock().unwrap().is_empty();
        if filed && *self.0.hold_question_binds.lock().unwrap() {
            self.0.log(json!({"event":"question_bind_held"}));
            while *self.0.hold_question_binds.lock().unwrap() {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
        subc_client_rs::BindDecision::accept()
    }
    async fn handle(&self, _: subc_client_rs::RequestCtx, body: Vec<u8>) -> HandlerOutcome {
        let request: Value = serde_json::from_slice(&body).unwrap();
        let params = &request["params"];
        self.0
            .log(json!({"event":request["method"], "params":params}));
        match request["method"].as_str().unwrap() {
            "consent.request" => {
                let frames = self.0.records();
                let prepared = frames
                    .iter()
                    .find(|frame| matches!(frame.record, WalRecord::ToolCallDeferred { .. }))
                    .expect("Prepared must be durable before filing");
                let WalRecord::ToolCallDeferred {
                    args_jcs,
                    args_hash,
                    ..
                } = &prepared.record
                else {
                    unreachable!()
                };
                assert_eq!(args_jcs, APPROVED_BYTES);
                let call_key = params["requester_ref"].as_str().unwrap();
                assert!(call_key.ends_with(&format!(":{}", prepared.seq)));
                assert_eq!(params["dedup_key"], call_key);
                use sha2::{Digest, Sha256};
                assert_eq!(
                    args_hash,
                    &format!("{:x}", Sha256::digest(APPROVED_BYTES.as_bytes()))
                );
                assert_eq!(params["args_digest"], *args_hash);
                assert_eq!(
                    params["binding"],
                    json!({"args_hash":args_hash,"module":TOOLS,"tool":"work","schema_pin":null})
                );
                let mut expected = consent_vector("broca/held-call-request.json")["params"].clone();
                for (key, value) in [
                    ("requester_ref", json!(call_key)),
                    ("dedup_key", json!(call_key)),
                    ("binding", params["binding"].clone()),
                    ("args_digest", json!(args_hash)),
                    ("prompt", json!("Allow work?")),
                    ("title", json!("Run work")),
                    ("target", json!({"kind":"module","label":TOOLS})),
                    ("material_damage", json!(false)),
                    ("late_execution", json!("notify_only")),
                    ("expires_in_ms", params["expires_in_ms"].clone()),
                ] {
                    expected[key] = value;
                }
                assert_eq!(
                    *params, expected,
                    "serve uses the held-call consent mapping, without identity or raw arguments"
                );
                assert!(params["expires_in_ms"]
                    .as_u64()
                    .is_some_and(|ms| ms > 0 && ms <= self.0.expiry_ms));
                let mut filings = self.0.filings.lock().unwrap();
                assert!(
                    filings.is_empty(),
                    "a recorded card is read back, not re-filed"
                );
                filings.push(params.clone());
                let mut reply =
                    consent_vector("upstream/consent-requester-v2/cerebellum-reply.json");
                reply["result"]["elicitation_id"] = json!(QUESTION);
                response(reply)
            }
            "consent.answers" => {
                assert!(params["since"].is_u64());
                let answered = self.0.answer.lock().unwrap().is_some();
                let mut reply = consent_vector(
                    "upstream/consent-requester-v2/cerebellum-ref-answers-reply.json",
                );
                reply["result"]["records"] = if answered && params["since"].as_u64().unwrap() < 1 {
                    json!([self.0.consent_record()])
                } else {
                    json!([])
                };
                reply["result"]["cursor"] = json!(u64::from(answered));
                response(reply)
            }
            "consent.get" | "consent.await" => {
                assert_eq!(params["elicitation_id"], QUESTION);
                assert!(self.0.records().iter().any(|frame| matches!(&frame.record,
                    WalRecord::AwaitingApproval { pending, .. } if pending.iter().any(|entry| entry.elicitation_id.as_deref() == Some(QUESTION)))),
                    "card id is durable before get or await");
                if request["method"] == "consent.await" {
                    let wait_ms = params["timeout_ms"].as_u64().unwrap();
                    assert!(wait_ms <= 120_000);
                    let _ = tokio::time::timeout(Duration::from_millis(wait_ms), async {
                        while self.0.answer.lock().unwrap().is_none() {
                            tokio::time::sleep(Duration::from_millis(25)).await;
                        }
                    })
                    .await;
                }
                let record = self.0.consent_record();
                self.0
                    .log(json!({"event":"consent_reply", "record":record}));
                response(json!({"result":record}))
            }
            "consent.withdraw" => {
                assert_eq!(params["elicitation_id"], QUESTION);
                if !*self.0.withdraw_enabled.lock().unwrap() {
                    return HandlerOutcome::Error {
                        code: "consent_unavailable".into(),
                        message: "retry withdrawal on next boot".into(),
                    };
                }
                let mut record = self.0.consent_record();
                record["state"] = json!("withdrawn");
                response(json!({"result":record}))
            }
            "consent.report_execution" => {
                assert_eq!(params["elicitation_id"], QUESTION);
                assert_eq!(params["execution"], "executed");
                assert!(self
                    .0
                    .records()
                    .iter()
                    .any(|frame| matches!(frame.record, WalRecord::ToolResult { .. })));
                response(json!({"result":self.0.consent_record()}))
            }
            other => panic!("unexpected consent op: {other}"),
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
            // Startup can fail before the WAL exists. A second panic here aborts libtest,
            // hiding the original failure and preventing daemon cleanup.
            match self.script.wal.read_records(&self.script.session) {
                Ok(frames) => dump(
                    &frames
                        .into_iter()
                        .map(|frame| frame.record)
                        .collect::<Vec<_>>(),
                ),
                Err(error) => eprintln!("APPROVAL DIAGNOSTIC cannot read WAL: {error}"),
            }
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
        // The daemon's modules write into this directory (broca creates its state root while
        // it starts), and killing only the daemon leaves them running. End the whole tree
        // first, so nothing can refill or recreate the directory during or after removal.
        broca_test_support::kill_and_reap_tree(&mut self.daemon);
        broca_test_support::dump_rig_process_logs_on_failure(&self.dir, "daemon");
        broca_test_support::remove_rig_dir(&self.dir);
    }
}
impl Rig {
    async fn start(tag: &str, provider: bool) -> Option<Self> {
        Self::start_with_cingulate(tag, provider, None).await
    }

    async fn start_with_cingulate(
        tag: &str,
        provider: bool,
        cingulate: Option<PathBuf>,
    ) -> Option<Self> {
        let subc = broca_test_support::subc_bin("ck-subc", "SUBC_CORE_BIN");
        let stub = broca_test_support::subc_bin("fake-aft-stub", "FAKE_AFT_STUB_BIN");
        if !subc.exists() || !stub.exists() {
            assert!(
                std::env::var_os("BROCA_REQUIRE_SUBC").is_none()
                    && (cingulate.is_none()
                        || std::env::var_os("BROCA_REQUIRE_CINGULATE").is_none()),
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
            hold_question_binds: Mutex::new(false),
            created_at_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            expiry_ms: if provider || cingulate.is_some() {
                600_000
            } else {
                3_000
            },
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
                &json!([{"ref":"approval-head", "scope_epoch":1, "kind":"head", "attributes":{"agent_id":"approval-head"},
            "carriers":[{"principal":{"kind":"reserved","module_id":"broca"}}]}]),
            )
            .unwrap(),
        )
        .unwrap();
        let module = broca_test_support::ckdev_binary(env!("CARGO_BIN_EXE_ck-broca"), &dir);
        let mut config = json!({"version":1, "scope_authority_owners":[OWNER], "modules":{
            "broca":{"program":module, "args":[], "enabled":true, "reserved":true,
                "env":{"BROCA_STATE_ROOT":dir.join("state"), "BROCA_PROVIDER_BASE_URL":model_url}},
            OWNER:{"program":stub, "args":[], "enabled":true, "reserved":true,
                "env":{"FAKE_AFT_MODULE_ID":OWNER, "FAKE_AFT_SCOPE_SYNC_PATH":scopes, "FAKE_AFT_EVENTS_PATH":dir.join("owner-events.jsonl"),
                    "FAKE_AFT_LAUNCH_NONCE_PATH":dir.join("owner.nonce")}}
        }});
        if let Some(binary) = cingulate {
            let binary = broca_test_support::ckdev_binary(binary, &dir);
            config["modules"]["cingulate"] =
                json!({"program":binary,"args":[],"enabled":true,"reserved":true});
        }
        std::fs::write(config_path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();
        let (subc, cli) = broca_test_support::ckdev_binary_with_companion(subc, cli, &dir);
        let daemon = broca_test_support::retry_executable_file_busy(|| {
            broca_test_support::tokio_rig_command(&subc, &dir, "daemon")?
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
            let manifest = ModuleManifest::builder("cingulate", env!("CARGO_PKG_VERSION"))
                .provides(vec![ProviderRole::ManagementSurface {
                    operations: [
                        "consent.request",
                        "consent.answers",
                        "consent.get",
                        "consent.withdraw",
                        "consent.await",
                        "consent.report_execution",
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
                Consent(rig.script.clone()),
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
                ScopeSelector::new(
                    Principal::Reserved {
                        module_id: OWNER.into(),
                    },
                    "approval-head",
                    Some(1),
                ),
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
    /// Position of the next scripted event, so a later check can count only what follows.
    fn mark(&self) -> usize {
        self.script.logs.lock().unwrap().len()
    }
    fn events_since(&self, mark: usize, name: &str) -> usize {
        self.script.logs.lock().unwrap()[mark..]
            .iter()
            .filter(|entry| entry["event"] == name)
            .count()
    }
    async fn paused(&self) {
        wait("run paused for restart", || {
            self.records().iter().any(|record| {
                matches!(
                    record,
                    WalRecord::RunPaused {
                        reason: broca_protocol::PauseReason::Restart,
                        ..
                    }
                )
            })
        })
        .await;
    }
    /// `ck module restart` returns once the daemon accepts the request. The daemon then
    /// drains the old process, which can take its whole 30 s drain deadline, before it
    /// launches a new one. Waiting for the resume separately keeps a later wait timing
    /// the resumed run rather than the drain, and names which half stalled.
    async fn resumed(&self) {
        self.paused().await;
        wait("restarted broca resumed the paused run", || {
            self.records()
                .iter()
                .any(|record| matches!(record, WalRecord::ResumeStarted { .. }))
        })
        .await;
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
        matches!(&result.output, broca_protocol::ToolOutput::Error { message, .. } if message.starts_with(refusal))
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
        rig.script.filings.lock().unwrap()[0]["requester_ref"]
    );
    assert!(call.get("approval_digest").is_none());
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
        .position(|entry| entry["event"] == "consent.request")
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
async fn setup_failure_without_wal_preserves_panic_and_reaps_daemon() {
    const CHILD: &str = "BROCA_APPROVAL_SETUP_FAILURE_CHILD";
    if std::env::var_os(CHILD).is_some() {
        tokio::time::timeout(Duration::from_secs(120), async {
            let Some(rig) = Rig::start("setup-failure", true).await else {
                return;
            };
            assert!(rig.script.wal.read_records(&rig.script.session).is_err());
            println!("APPROVAL_SETUP_DAEMON_PID={}", rig.daemon.id().unwrap());
            println!("APPROVAL_SETUP_RIG_DIR={}", rig.dir.display());
            panic!("intentional approval setup failure before WAL creation");
        })
        .await
        .expect("bounded approval setup-failure scenario");
        return;
    }
    let output = tokio::time::timeout(
        Duration::from_secs(150),
        tokio::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "approval_gate::setup_failure_without_wal_preserves_panic_and_reaps_daemon",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .env(
                "BROCA_STATE_ROOT",
                broca_test_support::scratch_root().join("approval-setup-child-state"),
            )
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("bounded approval setup-failure subprocess")
    .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stdout.contains("running 1 test"), "{stdout}\n{stderr}");
    if output.status.success() && stdout.contains("SKIP approval_gate") {
        println!("{stdout}");
        return;
    }
    // libtest prints `test <name> ... ` without a newline before the child's own output, so
    // a marker may share a line with that prefix.
    let marker = |name: &str| {
        stdout.lines().find_map(|line| {
            line.split_once(name)
                .map(|(_, value)| value.trim().to_owned())
        })
    };
    let pid = marker("APPROVAL_SETUP_DAEMON_PID=")
        .expect("child must report the daemon pid")
        .parse::<u32>()
        .unwrap();
    let process = subc_os::Process::open(pid).unwrap();
    let survived = process.is_some();
    // A broken destructor can abort the child. Stop its daemon before reporting the regression.
    if let Some(process) = process {
        let _ = process.signal(subc_os::Signal::Terminate);
        if tokio::time::timeout(Duration::from_secs(5), async {
            while subc_os::Process::open(pid).unwrap().is_some() {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .is_err()
        {
            let _ = process.signal(subc_os::Signal::Kill);
        }
    }
    assert_eq!(output.status.code(), Some(101), "{stdout}\n{stderr}");
    assert!(
        stderr.contains("intentional approval setup failure"),
        "{stderr}"
    );
    assert!(
        stderr.contains("APPROVAL DIAGNOSTIC cannot read WAL"),
        "{stderr}"
    );
    assert!(!survived, "daemon {pid} survived unwinding");
    let dir = marker("APPROVAL_SETUP_RIG_DIR=").expect("child must report its rig directory");
    assert!(
        !std::path::Path::new(&dir).exists(),
        "rig directory survived unwinding: {dir}\n{stderr}"
    );
}

/// The pid of the broca module, from the daemon's own record of the modules it launched.
/// Broca can still be starting when `Rig::start` returns, so wait for the daemon to list it.
#[cfg(unix)]
async fn broca_module_pid(rig: &Rig) -> u32 {
    let live = rig.dir.join("data/cortexkit/run/live-children.json");
    let read = || -> Option<u32> {
        let record: Value = serde_json::from_slice(&std::fs::read(&live).ok()?).ok()?;
        record["children"]
            .as_array()?
            .iter()
            .find(|child| child["module_id"] == "broca")?["pid"]
            .as_u64()?
            .try_into()
            .ok()
    };
    wait("broca module launched", || read().is_some()).await;
    read().unwrap()
}

/// Keeps a module process alive, stopped where it is, across a rig's cleanup.
///
/// A stopped process alone does not stay alive: when the daemon dies, the module's process
/// group is left with no member whose parent is in another group of the same session, and
/// the kernel then sends a stopped member SIGHUP, which ends it. Adding a `sleep` started by
/// this test to that group keeps it attached. Dropping the hold kills both processes.
#[cfg(unix)]
struct HeldModule {
    pid: u32,
    anchor: std::process::Child,
}
#[cfg(unix)]
impl HeldModule {
    fn stop(pid: u32) -> Self {
        use std::os::unix::process::CommandExt;
        // SAFETY: getpgid only reads the group id of the process holding `pid`.
        let group = unsafe { libc::getpgid(pid as libc::pid_t) };
        assert!(
            group > 0,
            "no process group for module {pid}: {}",
            std::io::Error::last_os_error()
        );
        let anchor = std::process::Command::new("sleep")
            .arg("120")
            .process_group(group)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap_or_else(|error| panic!("cannot join module {pid}'s group {group}: {error}"));
        let stopped = std::process::Command::new("kill")
            .args(["-STOP", &pid.to_string()])
            .status()
            .unwrap();
        assert!(stopped.success(), "cannot stop module {pid}");
        Self { pid, anchor }
    }
}
#[cfg(unix)]
impl Drop for HeldModule {
    fn drop(&mut self) {
        let _ = std::process::Command::new("kill")
            .args(["-KILL", &self.pid.to_string()])
            // Cleanup has usually ended the module already; that is the passing case.
            .stderr(std::process::Stdio::null())
            .status();
        let _ = self.anchor.kill();
        let _ = self.anchor.wait();
    }
}

/// Killing only the daemon orphans its modules. Broca, often still starting up then, goes
/// on creating files under its state root, so it can recreate the rig directory or make
/// its removal fail. Holding broca stopped keeps it alive across the rig's cleanup unless
/// cleanup itself ends it, so the check below fails whenever cleanup leaves a module running.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rig_cleanup_ends_supervised_modules_before_removing_its_directory() {
    tokio::time::timeout(Duration::from_secs(120), async {
        let Some(rig) = Rig::start("cleanup-modules", true).await else {
            return;
        };
        let pid = broca_module_pid(&rig).await;
        let hold = HeldModule::stop(pid);
        let dir = rig.dir.clone();
        drop(rig);
        let running = subc_os::Process::open(pid)
            .unwrap()
            .is_some_and(|process| process.observe().is_some());
        drop(hold);
        assert!(!running, "broca module {pid} outlived rig cleanup");
        assert!(!dir.exists(), "rig directory survived cleanup: {dir:?}");
    })
    .await
    .expect("bounded real daemon cleanup scenario");
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
        // This scenario restarts while a wait is open. Run status publishes the card as
        // soon as its id is durable, before broca sends its first consent.await, so a
        // restart issued on that signal alone can land before any wait exists. The
        // restart-before-await test covers that ordering.
        wait("first consent.await reached Cingulate", || {
            rig.events_since(0, "consent.await") >= 1
        })
        .await;
        let mark = rig.mark();
        rig.restart().await;
        rig.resumed().await;
        wait("recorded card read back on resume", || {
            rig.events_since(mark, "consent.get") >= 1
                && rig.events_since(mark, "consent.await") >= 1
        })
        .await;
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
            .any(|entry| entry["event"] == "consent.withdraw"));
        *rig.script.answer.lock().unwrap() = Some("approve");
        approved(&rig, &rig.finished().await);
        assert_eq!(
            rig.script.filings.lock().unwrap().len(),
            1,
            "resume must use the recorded card rather than file a second one"
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

/// A restart can land after the card's id is durable but before broca's first
/// `consent.await` reaches Cingulate. The resumed run must still read the recorded card
/// back and wait on it, without filing a second card.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_before_first_await_reads_recorded_card_back() {
    tokio::time::timeout(Duration::from_secs(120), async {
        let Some(rig) = Rig::start("restart-before-await", true).await else {
            return;
        };
        *rig.script.hold_question_binds.lock().unwrap() = true;
        rig.send(true).await.unwrap();
        rig.filed(1).await;
        let first = rig.pending().await;
        wait("post-filing consent route parked", || {
            rig.events_since(0, "question_bind_held") >= 1
        })
        .await;
        let mark = rig.mark();
        rig.restart().await;
        rig.paused().await;
        assert_eq!(
            rig.events_since(0, "consent.await"),
            0,
            "the restart must land before the first wait reaches Cingulate"
        );
        *rig.script.hold_question_binds.lock().unwrap() = false;
        rig.resumed().await;
        wait("recorded card read back on resume", || {
            rig.events_since(mark, "consent.get") >= 1
                && rig.events_since(mark, "consent.await") >= 1
        })
        .await;
        let resumed = rig.pending().await;
        assert_eq!(
            first["awaiting_approval"], resumed["awaiting_approval"],
            "restart resumes the same question and deadline"
        );
        assert_eq!(rig.events_since(0, "consent.withdraw"), 0);
        *rig.script.answer.lock().unwrap() = Some("approve");
        approved(&rig, &rig.finished().await);
        assert_eq!(
            rig.script.filings.lock().unwrap().len(),
            1,
            "resume must use the recorded card rather than file a second one"
        );
    })
    .await
    .expect("bounded real daemon restart-before-await scenario");
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
                .any(|entry| entry["event"] == "consent.withdraw")
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
            .any(|entry| entry["event"] == "consent.withdraw"));
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

fn cingulate_build(path: Option<PathBuf>, required: bool) -> Result<Option<PathBuf>, String> {
    match path.filter(|path| path.is_file()) {
        Some(path) => Ok(Some(path.canonicalize().map_err(|error| error.to_string())?)),
        None if required => Err("real Cingulate test requires a placed build: set CK_CINGULATE_BIN or place .test-rigs/ck-cingulate (BROCA_REQUIRE_CINGULATE is set)".into()),
        None => Ok(None),
    }
}

#[test]
fn missing_cingulate_build_skips_only_when_not_required() {
    assert_eq!(cingulate_build(None, false).unwrap(), None);
    let error = cingulate_build(None, true).unwrap_err();
    assert!(error.contains("CK_CINGULATE_BIN"), "{error}");
    assert!(error.contains("BROCA_REQUIRE_CINGULATE"), "{error}");
    let missing = broca_test_support::scratch_root().join("absent-cingulate-build");
    assert!(!missing.exists());
    assert_eq!(cingulate_build(Some(missing.clone()), false).unwrap(), None);
    assert!(cingulate_build(Some(missing), true).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_cingulate_holds_and_withdraws_unanswered_call() {
    let path = std::env::var_os("CK_CINGULATE_BIN")
        .map(PathBuf::from)
        .or_else(|| {
            Some(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
                "../../.test-rigs/ck-cingulate{}",
                std::env::consts::EXE_SUFFIX
            )))
        });
    let Some(build) = cingulate_build(path, std::env::var_os("BROCA_REQUIRE_CINGULATE").is_some())
        .unwrap_or_else(|error| {
            panic!("real_cingulate_holds_and_withdraws_unanswered_call: {error}")
        })
    else {
        println!("SKIP real_cingulate_holds_and_withdraws_unanswered_call: no placed Cingulate build (set CK_CINGULATE_BIN; BROCA_REQUIRE_CINGULATE makes this fail)");
        return;
    };
    tokio::time::timeout(Duration::from_secs(120), async {
        let Some(rig) = Rig::start_with_cingulate("real-cingulate", false, Some(build)).await else {
            println!("SKIP real_cingulate_holds_and_withdraws_unanswered_call: no SUBC fixtures (BROCA_REQUIRE_SUBC makes this fail)");
            return;
        };
        let reply = rig.send(true).await.unwrap();
        let run_id = reply["result"]["run_id"].as_str().unwrap();
        wait("real Cingulate card recorded", || rig.records().iter().any(|record| matches!(record,
            WalRecord::AwaitingApproval { pending, .. } if pending.iter().any(|entry| entry.elicitation_id.is_some())))).await;
        assert!(rig.script.dispatches.lock().unwrap().is_empty());
        let conn = rig.owner().await;
        let route = rig.route(&conn, true).await;
        conn.route_request(&route, serde_json::to_vec(&json!({"method":"run.cancel", "params":{"run_id":run_id}})).unwrap()).await.unwrap();
        let records = rig.finished().await;
        no_dispatch(&rig, &records);
        wait("real Cingulate withdrawal acknowledged", || rig.records().iter().any(|record| matches!(record,
            WalRecord::QuestionWithdrawalAcknowledged { answer: broca_wal::WithdrawalAnswer::Acknowledged, .. }))).await;
    }).await.expect("bounded placed Cingulate scenario");
}

#[tokio::test]
async fn diagnostics_findings_uses_authenticated_head_owner_route() {
    let Some(rig) = Rig::start("diagnostics-findings", false).await else {
        return;
    };
    let conn = rig.owner().await;
    let route = rig.route(&conn, true).await;
    let reply = conn
        .route_request(
            &route,
            serde_json::to_vec(&json!({
                "method": "diagnostics.findings", "params": {}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(reply["result"]["findings"].is_array(), "{reply}");
    assert_eq!(reply["result"]["window_ms"], 21_600_000);
    assert!(reply["result"]["generated_at_ms"].as_i64().unwrap() > 0);
    assert!(serde_json::to_vec(&reply).unwrap().len() < 65_536);
}
