//! Differential coverage of the installed legacy and format-2 service compositions.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use base64::Engine;
use broca_subc::{RouteHandle, SubcConnection};
use broca_wal::framing::{decode_next, DecodeStep};
use serde_json::{json, Value};
use subc_protocol::manifest::{
    Concurrency, ExecutionMode, IdentityScope, ManagementOperation, ManagementOperationKind,
    ModuleManifest, ProviderRole, Tool,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Notify;

const TOOLS: &str = "differential-tools";
const PROMPT: &str = "content-sentinel-user-79bb642e";
const SYSTEM: &str = "content-sentinel-system-307a6cb8";
const TITLE: &str = "content-sentinel-title-238ed435";
const ANSWER: &str = "content-sentinel-answer-5bcd1cc0";
const OUTPUT: &str = "content-sentinel-tool-6257cdad";
const SENTINELS: &[&str] = &[PROMPT, SYSTEM, TITLE, ANSWER, OUTPUT];

fn no_legacy_index(root: &Path) {
    for name in ["run-index.db", "run-index.db-wal", "run-index.db-shm"] {
        assert!(
            !root.join(name).exists(),
            "unexpected {}",
            root.join(name).display()
        );
    }
}

fn inspect_database(root: &Path) {
    no_legacy_index(root);
    let db = rusqlite::Connection::open_with_flags(
        root.join("broca.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let tables = db
        .prepare("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    for required in [
        "session",
        "message",
        "run",
        "episode_seed",
        "projection_boundary",
    ] {
        assert!(
            tables.iter().any(|name| name == required),
            "missing {required}"
        );
    }
    for table in tables {
        let quoted = format!("\"{}\"", table.replace('"', "\"\""));
        let columns = db
            .prepare(&format!("PRAGMA table_info({quoted})"))
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        for column in &columns {
            assert!(
                !["payload", "content", "title", "prompt", "usage", "billing", "messages"]
                    .contains(&column.as_str()),
                "content column {table}.{column}"
            );
        }
        let mut statement = db.prepare(&format!("SELECT * FROM {quoted}")).unwrap();
        let mut rows = statement.query([]).unwrap();
        while let Some(row) = rows.next().unwrap() {
            for (i, column) in columns.iter().enumerate() {
                use rusqlite::types::ValueRef;
                if let ValueRef::Text(bytes) | ValueRef::Blob(bytes) = row.get_ref(i).unwrap() {
                    for sentinel in SENTINELS {
                        assert!(
                            !bytes
                                .windows(sentinel.len())
                                .any(|w| w == sentinel.as_bytes()),
                            "content sentinel in {table}.{}",
                            column
                        );
                    }
                }
            }
        }
    }
    let (sessions, seeds): (u64, u64) = db
        .query_row(
            "SELECT count(*), sum(length(admission_seed)>0 AND length(tail_seed)>0) FROM session",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(sessions, 1);
    assert_eq!(seeds, 1);
    // Scan sidecars as well: a committed leak must not disappear behind a later SQL update.
    for name in ["broca.db", "broca.db-wal", "broca.db-shm"] {
        if let Ok(bytes) = std::fs::read(root.join(name)) {
            for sentinel in SENTINELS {
                assert!(
                    !bytes
                        .windows(sentinel.len())
                        .any(|w| w == sentinel.as_bytes()),
                    "content sentinel in {name}"
                );
            }
        }
    }
}

#[derive(Default)]
struct Script {
    requests: Mutex<Vec<Vec<u8>>>,
    dispatches: Mutex<Vec<Value>>,
    reports: Mutex<Vec<Value>>,
    tail_entered: Notify,
    tail_release: Notify,
}

async fn provider(script: Arc<Script>) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/v1", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let script = script.clone();
            tokio::spawn(async move {
                let mut bytes = Vec::new();
                let (start, length) = loop {
                    let mut buf = [0; 4096];
                    let n = socket.read(&mut buf).await.unwrap();
                    assert_ne!(n, 0, "incomplete provider request");
                    bytes.extend_from_slice(&buf[..n]);
                    if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = std::str::from_utf8(&bytes[..end]).unwrap();
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                let (name, value) = line.split_once(':')?;
                                name.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().unwrap())
                            })
                            .expect("content length");
                        break (end + 4, length);
                    }
                };
                while bytes.len() < start + length {
                    let mut buf = [0; 4096];
                    let n = socket.read(&mut buf).await.unwrap();
                    assert_ne!(n, 0);
                    bytes.extend_from_slice(&buf[..n]);
                }
                let turn = {
                    let mut requests = script.requests.lock().unwrap();
                    let turn = requests.len();
                    requests.push(bytes[start..start + length].to_vec());
                    turn
                };
                if turn == 4 {
                    script.tail_entered.notify_one();
                    script.tail_release.notified().await;
                }
                let (status, content_type, body) = if turn == 2 {
                    ("401 Unauthorized", "application/json",
                        json!({"error":{"code":"authentication_error","message":"reauthenticate the fixture"}}).to_string())
                } else {
                    assert!(turn <= 4, "unexpected provider request {turn}");
                    let call = turn < 2;
                    let delta = if call {
                        json!({"role":"assistant","tool_calls":[{"index":0,
                            "id":format!("call-{turn}"),"type":"function","function":{
                                "name":"work","arguments":format!("{{\"n\":{turn}}}")}}]})
                    } else {
                        json!({"role":"assistant","content":ANSWER})
                    };
                    let chunk = |delta: Value, finish: Value| {
                        json!({"id":format!("response-{turn}"),
                        "object":"chat.completion.chunk","model":"differential-model",
                        "choices":[{"index":0,"delta":delta,"finish_reason":finish}]})
                    };
                    let first = chunk(delta, Value::Null);
                    let mut last =
                        chunk(json!({}), json!(if call { "tool_calls" } else { "stop" }));
                    last["usage"] =
                        json!({"prompt_tokens":17,"completion_tokens":3,"total_tokens":20});
                    (
                        "200 OK",
                        "text/event-stream",
                        format!("data: {first}\n\ndata: {last}\n\ndata: [DONE]\n\n"),
                    )
                };
                let response = format!("HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\nx-should-retry: false\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len());
                socket.write_all(response.as_bytes()).await.unwrap();
            });
        }
    });
    (url, task)
}

struct Tools {
    script: Arc<Script>,
    overhaul: Option<PathBuf>,
}

#[async_trait]
impl subc_client_rs::ModuleHandler for Tools {
    async fn handle(
        &self,
        _: subc_client_rs::RequestCtx,
        body: Vec<u8>,
    ) -> subc_client_rs::HandlerOutcome {
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["name"], "work");
        self.script.dispatches.lock().unwrap().push(value);
        if let Some(root) = &self.overhaul {
            no_legacy_index(root);
        }
        subc_client_rs::HandlerOutcome::Response(
            serde_json::to_vec(&json!({"content":[{"type":"text","text":OUTPUT}]})).unwrap(),
        )
    }
}

struct Accounts {
    script: Arc<Script>,
    vault: bool,
}

#[async_trait]
impl subc_client_rs::ModuleHandler for Accounts {
    async fn handle(
        &self,
        _: subc_client_rs::RequestCtx,
        body: Vec<u8>,
    ) -> subc_client_rs::HandlerOutcome {
        let value: Value = serde_json::from_slice(&body).unwrap();
        let result = match value["method"].as_str().unwrap() {
            "credential.list_scoped" if self.vault => json!({"credentials":[{
                "id":"apikey:xai","categories":["llm-provider"],"type":"apikey",
                "serves":["xai"],"provider_ids":["xai"],"auth_method":"apikey",
                "state":"active","record_version":1,"operations":["read"],"account_id":"acct-fixture"
            },{
                "id":"apikey:xai:spare","categories":["llm-provider"],"type":"apikey",
                "serves":["xai"],"provider_ids":["xai"],"auth_method":"apikey",
                "state":"active","record_version":1,"operations":["read"],"account_id":"acct-spare"
            }],"grants":1,"grant_tuples":[],"view":"differential-view"}),
            "credential.get_scoped" if self.vault => json!({"payload":b"fixture-token".to_vec(),
                "expires_at_ms":null,"record_version":1,"credential_id":"apikey:xai","account_id":"acct-fixture"}),
            "credential.report_auth_failure" if self.vault => json!({}),
            "route.account_for" if !self.vault => json!({
                "admission":{"id":value["params"]["admission_id"],"state":"reserved"},
                "account":{"provider":"xai","account_id":"acct-fixture","credential_id":"apikey:xai","method":"apikey"},
                "pin":{"id":"pin-fixture","generation":1,"mode":"run_frozen"},
                "reason":{"code":"cold_best","decision_id":"decision-fixture","policy_version":2}}),
            "route.account_observe" if !self.vault => {
                self.script
                    .reports
                    .lock()
                    .unwrap()
                    .push(value["params"].clone());
                json!({})
            }
            other => panic!("unexpected account operation {other}"),
        };
        subc_client_rs::HandlerOutcome::Response(
            serde_json::to_vec(&json!({"result":result})).unwrap(),
        )
    }
}

fn management(id: &str, operations: &[&str]) -> ModuleManifest {
    ModuleManifest::builder(id, env!("CARGO_PKG_VERSION"))
        .provides(vec![ProviderRole::ManagementSurface {
            operations: operations
                .iter()
                .map(|name| ManagementOperation {
                    name: (*name).into(),
                    kind: ManagementOperationKind::Mutate,
                    description: None,
                })
                .collect(),
            config_schema: json!({}),
            observability: vec![],
            identity_scope: vec![],
            concurrency: Concurrency::ModuleManaged,
        }])
        .build()
}

async fn register<H: subc_client_rs::ModuleHandler + 'static>(
    file: &Path,
    manifest: ModuleManifest,
    handler: H,
) -> tokio::task::JoinHandle<()> {
    let (_, serve) = subc_client_rs::serve_with_handle(file, manifest, handler)
        .await
        .unwrap();
    tokio::spawn(async move {
        serve.await.unwrap();
    })
}

struct Rig {
    root: PathBuf,
    state: PathBuf,
    overhaul: bool,
    script: Arc<Script>,
    provider: tokio::task::JoinHandle<()>,
    daemon: Option<broca_test_support::RigChild>,
    modules: Vec<tokio::task::JoinHandle<()>>,
    replies: Vec<Value>,
}

impl Drop for Rig {
    fn drop(&mut self) {
        self.stop();
        self.provider.abort();
        broca_test_support::remove_rig_dir(&self.root);
    }
}

impl Rig {
    async fn new(overhaul: bool) -> Self {
        let root = broca_test_support::scratch_root().join(format!(
            "overhaul-differential-{}-{overhaul}",
            std::process::id()
        ));
        broca_test_support::remove_rig_dir(&root);
        let state = root.join("state");
        std::fs::create_dir_all(&state).unwrap();
        if overhaul {
            // Mark the fresh root before installing its catalog fixture. An unmarked root
            // containing a catalog is deliberately classified as legacy by daemon boot.
            assert_eq!(std::fs::read_dir(&state).unwrap().count(), 0);
            std::fs::write(state.join("state-format"), b"broca-state/2\n").unwrap();
        }
        for dir in [
            root.join("config/cortexkit"),
            root.join("runtime"),
            root.join("home"),
            state.join("catalog"),
        ] {
            std::fs::create_dir_all(dir).unwrap();
        }
        let script = Arc::new(Script::default());
        let (url, provider) = provider(script.clone()).await;
        // Both roots render the same synthetic Chat transport, independently of live catalogs.
        std::fs::write(state.join("catalog/fusiform-lkg.json"), json!({
            "catalog_version":1,"resolved_at_ms":1,"source":"fixture","models":{
                "xai/differential-model":{
                    "existence":"present","capability.tool_call":true,"capability.attachment":false,
                    "capability.reasoning":false,"limit.context":128000,"limit.output":4096,
                    "wire.npm":{"value":"@ai-sdk/openai-compatible","from":"fixture"},
                    "wire.api":{"value":"https://differential.invalid/v1","from":"fixture"}
                }
            }
        }).to_string()).unwrap();
        let mut config = json!({"providers":{"xai":{"base_url":url}}});
        if overhaul {
            config["state_format"] = json!(2);
        }
        std::fs::write(root.join("broca.jsonc"), config.to_string()).unwrap();
        std::fs::write(
            root.join("auth-methods.json"),
            r#"{"defaults":{"xai":"apikey"}}"#,
        )
        .unwrap();
        std::fs::write(root.join("config/cortexkit/subc.jsonc"), json!({"version":1,"modules":{
            "broca":{"program":broca_test_support::ckdev_binary(env!("CARGO_BIN_EXE_ck-broca"), broca_test_support::scratch_root()),
                "args":[],"enabled":true,"env":{
                    "BROCA_STATE_ROOT":state,"BROCA_CONFIG_PATH":root.join("broca.jsonc"),
                    "BROCA_AUTH_METHODS_PATH":root.join("auth-methods.json"),"BROCA_TOOL_MODULE":TOOLS,
                    "XDG_DATA_HOME":root.join("data-home")
                }}
        }}).to_string()).unwrap();
        let mut rig = Self {
            root,
            state,
            overhaul,
            script,
            provider,
            daemon: None,
            modules: vec![],
            replies: vec![],
        };
        rig.start().await;
        rig
    }

    fn check(&self) {
        if self.overhaul {
            no_legacy_index(&self.state);
        }
    }

    fn file(&self) -> PathBuf {
        self.root.join("runtime/subc-connection.json")
    }

    fn identity(&self) -> broca_protocol::BindIdentity {
        // Keep project identity identical; only the state-root path may be substituted.
        broca_protocol::BindIdentity::new(
            broca_test_support::scratch_root().canonicalize().unwrap(),
            "differential",
            "session",
        )
    }

    async fn start(&mut self) {
        self.check();
        let _ = std::fs::remove_file(self.file());
        let binary = broca_test_support::ckdev_binary(
            broca_test_support::subc_bin("ck-subc", "SUBC_CORE_BIN"),
            broca_test_support::scratch_root(),
        );
        let child = broca_test_support::retry_executable_file_busy(|| {
            broca_test_support::tokio_rig_command(&binary, &self.root, "daemon")?
                .env("XDG_CONFIG_HOME", self.root.join("config"))
                .env("XDG_RUNTIME_DIR", self.root.join("runtime"))
                .env("XDG_DATA_HOME", self.root.join("data-home"))
                .env("HOME", self.root.join("home"))
                .env("SUBC_PORT", "0")
                .env_remove("BROCA_STATE_ROOT")
                .env_remove("SUBC_MODULE_ID")
                .env_remove("SUBC_LAUNCH_NONCE")
                .env_remove("SUBC_LAUNCH_NONCE_FD")
                .kill_on_drop(true)
                .spawn()
        })
        .unwrap();
        self.daemon = Some(broca_test_support::RigChild::new(
            child, &self.root, "daemon",
        ));
        broca_test_support::wait_until("daemon connection file", || async {
            self.check();
            self.file()
                .exists()
                .then_some(())
                .ok_or_else(|| "no connection file".into())
        })
        .await;
        self.modules.push(
            register(
                &self.file(),
                management(
                    "claustrum",
                    &[
                        "credential.list_scoped",
                        "credential.get_scoped",
                        "credential.report_auth_failure",
                    ],
                ),
                Accounts {
                    script: self.script.clone(),
                    vault: true,
                },
            )
            .await,
        );
        self.modules.push(
            register(
                &self.file(),
                management(
                    "prefrontal-routing",
                    &["route.account_for", "route.account_observe"],
                ),
                Accounts {
                    script: self.script.clone(),
                    vault: false,
                },
            )
            .await,
        );
        let manifest = ModuleManifest::builder(TOOLS, env!("CARGO_PKG_VERSION"))
            .provides(vec![ProviderRole::ToolProvider {
                tools: vec![Tool {
                    name: "work".into(),
                    description: Some("Return the fixture output.".into()),
                    execution_mode: ExecutionMode::Pure,
                    schema: json!({"type":"object"}),
                }],
                identity_scope: vec![IdentityScope::Project, IdentityScope::Session],
                concurrency: Concurrency::ModuleManaged,
                emits_push: false,
                sub_supervises: false,
            }])
            .build();
        self.modules.push(
            register(
                &self.file(),
                manifest,
                Tools {
                    script: self.script.clone(),
                    overhaul: self.overhaul.then(|| self.state.clone()),
                },
            )
            .await,
        );
        self.check();
    }

    fn stop(&mut self) {
        self.check();
        if let Some(mut child) = self.daemon.take() {
            broca_test_support::kill_and_reap_tree(&mut child);
        }
        for task in self.modules.drain(..) {
            task.abort();
        }
        self.check();
    }

    async fn connect(&self) -> (SubcConnection, RouteHandle) {
        let connection = SubcConnection::connect(&self.file()).await.unwrap();
        let route = connection
            .route_open_management("broca", &self.identity())
            .await
            .unwrap();
        self.check();
        (connection, route)
    }

    async fn request(
        &self,
        connection: &SubcConnection,
        route: &RouteHandle,
        method: &str,
        params: Value,
    ) -> Result<Value, Value> {
        self.check();
        let reply = connection
            .route_request_terminal(
                route,
                serde_json::to_vec(&json!({"method":method,"params":params})).unwrap(),
            )
            .await
            .unwrap();
        self.check();
        match reply {
            Ok(bytes) => Ok(serde_json::from_slice::<Value>(&bytes).unwrap()["result"].clone()),
            Err(bytes) => Err(serde_json::from_slice(&bytes).unwrap()),
        }
    }

    async fn state(
        &self,
        connection: &SubcConnection,
        route: &RouteHandle,
        run: &str,
        wanted: &str,
    ) -> Value {
        broca_test_support::wait_until(wanted, || async {
            let value = self
                .request(connection, route, "run.status", json!({"run_id":run}))
                .await
                .unwrap();
            if value["state"] == wanted {
                Ok(value)
            } else {
                Err(value.to_string())
            }
        })
        .await
    }

    fn send(&self, tail: bool) -> Value {
        json!({"session":"session","send_id":if tail { "tail" } else { "tool-turn" },
            "prompt":if tail { "continue the history" } else { PROMPT },"system":SYSTEM,"title":TITLE,
            "model":{"provider":"xai","model":"differential-model"},"cache":{"key":"differential-cache"},
            "tools":[{"name":"work","description":"Return the fixture output.","input_schema":{"type":"object"},"module":TOOLS}]})
    }
}

#[derive(Debug)]
struct Source {
    seq: u64,
    end: u64,
    record: Value,
}

struct Sources {
    lineage: String,
    records: Vec<Source>,
    logical: BTreeMap<u64, usize>,
    root: String,
    runs: Vec<String>,
    file_id: u64,
    projected: BTreeSet<(u64, u64)>,
    overhaul: bool,
}

impl Sources {
    fn read(rig: &Rig) -> Self {
        fn logs(dir: &Path, found: &mut Vec<PathBuf>) {
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    logs(&path, found);
                } else if path.extension().is_some_and(|e| e == "wal") {
                    found.push(path);
                }
            }
        }
        let mut paths = vec![];
        logs(&rig.state, &mut paths);
        assert_eq!(paths.len(), 1, "one lineage WAL: {paths:?}");
        let bytes = std::fs::read(&paths[0]).unwrap();
        let mut at = 0;
        let mut records = vec![];
        let mut lineage = None;
        while at < bytes.len() {
            match decode_next(&bytes[at..]) {
                DecodeStep::Lineage {
                    lineage_id,
                    consumed,
                } => {
                    assert_eq!(at, 0);
                    assert!(lineage_id.incarnation().is_some());
                    lineage = Some(lineage_id.to_string());
                    at += consumed;
                }
                DecodeStep::Record { framed, consumed } => {
                    at += consumed;
                    records.push(Source {
                        seq: framed.seq,
                        end: at as u64,
                        record: serde_json::to_value(framed.record).unwrap(),
                    });
                }
                _ => panic!("invalid or incomplete WAL at {at}"),
            }
        }
        let logical = records
            .iter()
            .filter(|s| !lifecycle(&s.record))
            .enumerate()
            .map(|(i, s)| (s.seq, i + 1))
            .collect();
        let runs = records
            .iter()
            .filter(|s| s.record["type"] == "run_started")
            .map(|s| s.record["run_id"].as_str().unwrap().to_owned())
            .collect();
        let range = broca_wal::read_records_range(
            &paths[0],
            broca_wal::DEFAULT_MAX_READ_BYTES,
            0,
            None,
            None,
        )
        .unwrap();
        let file_id = range
            .anchor_at(records.last().unwrap().seq)
            .unwrap()
            .file_id;
        let framed: Vec<_> = records
            .iter()
            .map(|s| broca_wal::FramedRecord {
                seq: s.seq,
                fence: 0,
                record: serde_json::from_value(s.record.clone()).unwrap(),
            })
            .collect();
        let projected = broca_core::project_control(&framed)
            .into_iter()
            .map(|(c, _)| (c.wal_seq, u64::from(c.sub_index)))
            .collect();
        Self {
            lineage: lineage.unwrap(),
            records,
            logical,
            root: rig.state.to_string_lossy().into_owned(),
            runs,
            file_id,
            projected,
            overhaul: rig.overhaul,
        }
    }

    fn source(&self, seq: u64) -> &Source {
        self.records
            .iter()
            .find(|s| s.seq == seq)
            .unwrap_or_else(|| panic!("no source record at {seq}"))
    }

    fn position(&self, seq: u64) -> usize {
        if seq == 0 {
            return 0;
        }
        *self
            .logical
            .get(&seq)
            .unwrap_or_else(|| panic!("cursor points at nonlogical source {seq}"))
    }
}

fn lifecycle(record: &Value) -> bool {
    matches!(
        record["type"].as_str(),
        Some("writer_opened" | "writer_closed")
    )
}

fn fields(value: &Value, expected: &[&str]) {
    let actual: BTreeSet<_> = value
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        actual,
        expected.iter().copied().collect(),
        "cursor field coverage"
    );
}

impl Sources {
    fn text(&self, text: &str) -> String {
        let mut text = text.replace(&self.root, "@ROOT@");
        for (i, run) in self.runs.iter().enumerate() {
            text = text.replace(run, &format!("@RUN-{i}@"));
        }
        for (i, source) in self
            .records
            .iter()
            .filter(|s| s.record["attempt_id"].is_string())
            .enumerate()
        {
            let id = source.record["attempt_id"].as_str().unwrap();
            assert_eq!(id.len(), 32);
            assert!(id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)));
            text = text.replace(id, &format!("@ATTEMPT-{i}@"));
        }
        text
    }

    fn history_cursor(&self, token: &str) -> Value {
        let mut value: Value = serde_json::from_slice(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(token)
                .unwrap(),
        )
        .unwrap();
        fields(&value, &["lineage", "boundary", "backward"]);
        assert_eq!(value["lineage"], self.lineage);
        value["lineage"] = json!("@LINEAGE@");
        value
    }

    fn anchor(&self, anchor: &mut Value, step: u64) {
        // The anchor is a seek hint for the legacy log; format-2 reads from run rows.
        // Both unchanged tokens must still return the same complete remaining call pages.
        if self.overhaul {
            assert!(
                anchor.is_null(),
                "run-row cursor must not invent a file anchor"
            );
        } else {
            fields(anchor, &["file_id", "seq", "byte_end"]);
            assert_eq!(anchor["file_id"], self.file_id);
            let seq = anchor["seq"].as_u64().unwrap();
            assert_eq!(seq, step - 1);
            assert_eq!(anchor["byte_end"], self.source(seq).end);
        }
        *anchor = json!("@ROOT-LOCAL-SEEK-HINT@");
    }

    fn call_position(&self, position: &mut Value) {
        let step = position["step_seq"].as_u64().unwrap();
        let batch = position["batch_end_seq"].as_u64().unwrap();
        assert_eq!(self.source(step).record["type"], "model_step_finished");
        assert_eq!(self.source(batch).record["type"], "tool_batch_finished");
        let call_index = position["call_index"].as_u64().unwrap() as usize;
        assert!(self.source(step).record["assistant_message"]["content"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|b| b["type"] == "tool_call")
            .nth(call_index)
            .is_some());
        position["step_seq"] = json!(self.position(step));
        position["batch_end_seq"] = json!(self.position(batch));
        self.anchor(&mut position["anchor_before"], step);
    }

    fn calls_cursor(&self, token: &str) -> Value {
        let mut value: Value = serde_json::from_str(token).unwrap();
        // The authenticator is per-process; unchanged-token acceptance and cross-root and
        // tampered-token refusal check it separately from the source-position comparison.
        fields(
            &value,
            &[
                "version",
                "run_id",
                "lineage_id",
                "run_start_seq",
                "run_end_seq",
                "step_seq",
                "call_index",
                "batch_end_seq",
                "ordinal",
                "anchor_before",
                "after_batch",
                "authentication",
            ],
        );
        assert_eq!(value["version"], 1);
        let authentication = value["authentication"].as_str().unwrap();
        assert_eq!(authentication.len(), 64);
        assert!(authentication
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)));
        value["authentication"] = json!("@PROCESS-AUTHENTICATOR@");
        assert_eq!(value["lineage_id"], self.lineage);
        value["lineage_id"] = json!("@LINEAGE@");
        let start = value["run_start_seq"].as_u64().unwrap();
        let end = value["run_end_seq"].as_u64().unwrap();
        assert_eq!(self.source(start).record["type"], "run_started");
        assert_eq!(self.source(start).record["run_id"], value["run_id"]);
        assert_eq!(self.source(end).record["type"], "run_finished");
        assert_eq!(self.source(end).record["run_id"], value["run_id"]);
        value["run_start_seq"] = json!(self.position(start));
        value["run_end_seq"] = json!(self.position(end));
        self.call_position(&mut value);
        for position in value["after_batch"].as_array_mut().unwrap() {
            fields(
                position,
                &[
                    "step_seq",
                    "call_index",
                    "batch_end_seq",
                    "ordinal",
                    "anchor_before",
                ],
            );
            self.call_position(position);
        }
        value["run_id"] = json!(self.text(value["run_id"].as_str().unwrap()));
        value
    }

    fn normalize(&self, value: &Value) -> Value {
        match value {
            Value::Array(values) => {
                Value::Array(values.iter().map(|v| self.normalize(v)).collect())
            }
            Value::Object(object) => {
                if object.contains_key("wal_seq") {
                    assert!(
                        self.projected.contains(&cursor(value)),
                        "cursor is not a projected source unit: {value}"
                    );
                }
                let mut result = object.clone();
                for (key, value) in object {
                    result[key] = match key.as_str() {
                        "lineage_id" | "incarnation" if !value.is_null() => {
                            assert_eq!(value, &self.lineage);
                            json!("@LINEAGE@")
                        }
                        "wal_seq" => json!(self.position(value.as_u64().unwrap())),
                        "call_key" | "event_id" if !value.is_null() => {
                            let (lineage, seq) = value.as_str().unwrap().rsplit_once(':').unwrap();
                            assert_eq!(lineage, self.lineage);
                            let seq = seq.parse::<u64>().unwrap();
                            let source = &self.source(seq).record;
                            if key == "call_key" {
                                assert_eq!(source["type"], "tool_dispatch_intent");
                                if let Some(id) = object.get("tool_call_id") {
                                    assert_eq!(id, &source["tool_call_id"]);
                                }
                            } else {
                                let expected = match object["kind"].as_str().unwrap() {
                                    "admission_started" => "run_started",
                                    "attempt" => "model_attempt_finished",
                                    "run_finished" => "run_finished",
                                    other => panic!("unexpected accounting event {other}"),
                                };
                                assert_eq!(source["type"], expected);
                            }
                            json!(format!("@LINEAGE@:{}", self.position(seq)))
                        }
                        "older" | "newer" if value.is_string() => {
                            self.history_cursor(value.as_str().unwrap())
                        }
                        "next" if value.is_string() && value.as_str().unwrap().starts_with('{') => {
                            self.calls_cursor(value.as_str().unwrap())
                        }
                        "written_at" | "started_at" | "finished_at" | "updated_at"
                        | "observed_at_ms" | "last_sent_at_ms" | "expires_at_ms" | "sent_at_ms"
                        | "ts_ms"
                            if !value.is_null() =>
                        {
                            assert!(
                                value.as_u64().is_some_and(|n| n > 0),
                                "invalid timestamp {key}: {value}"
                            );
                            json!("@TIMESTAMP@")
                        }
                        _ => self.normalize(value),
                    };
                }
                Value::Object(result)
            }
            Value::String(text) => json!(self.text(text)),
            _ => value.clone(),
        }
    }
}

fn correspondence(left: &Sources, right: &Sources) {
    assert_ne!(left.lineage, right.lineage);
    let left_records: Vec<_> = left
        .records
        .iter()
        .filter(|s| !lifecycle(&s.record))
        .collect();
    let right_records: Vec<_> = right
        .records
        .iter()
        .filter(|s| !lifecycle(&s.record))
        .collect();
    assert_eq!(
        left_records.len(),
        right_records.len(),
        "logical WAL length"
    );
    for (a, b) in left_records.iter().zip(&right_records) {
        assert_eq!(
            a.record["type"], b.record["type"],
            "source correspondence {} / {}",
            a.seq, b.seq
        );
        for field in ["step_id", "tool_call_id", "batch_id", "args"] {
            assert_eq!(
                left.normalize(&a.record[field]),
                right.normalize(&b.record[field]),
                "source identity {field} at {} / {}",
                a.seq,
                b.seq
            );
        }
    }
    assert!(right.records.iter().any(|s| lifecycle(&s.record)));
    assert!(
        left_records
            .iter()
            .zip(right_records)
            .any(|(a, b)| a.seq != b.seq),
        "the differential must exercise distinct physical WAL positions"
    );
}

async fn pages(
    rig: &mut Rig,
    connection: &SubcConnection,
    route: &RouteHandle,
    run: &str,
) -> String {
    for (method, params) in [
        ("session.head", json!({})),
        ("run.status", json!({"run_id":run})),
        ("run.result", json!({"run_id":run})),
    ] {
        let reply = rig
            .request(connection, route, method, params)
            .await
            .unwrap();
        rig.replies.push(json!({"method":method,"reply":reply}));
    }
    let full = rig
        .request(
            connection,
            route,
            "session.read",
            json!({"include_tools":true}),
        )
        .await
        .unwrap();
    assert!(full["messages"].as_array().unwrap().len() >= 6);
    let mut ordinal = 0;
    let mut collected = vec![];
    loop {
        let page = rig
            .request(
                connection,
                route,
                "session.read",
                json!({"from_ordinal":ordinal,
            "limit":2,"lineage_id":full["lineage_id"],"include_tools":true}),
            )
            .await
            .unwrap();
        collected.extend(page["messages"].as_array().unwrap().iter().cloned());
        rig.replies
            .push(json!({"method":"session.read.page","reply":page}));
        let Some(next) = page["next_from_ordinal"].as_u64() else {
            break;
        };
        assert!(next > ordinal);
        ordinal = next;
    }
    assert_eq!(collected, full["messages"].as_array().unwrap().clone());
    rig.replies
        .push(json!({"method":"session.read.full","reply":full}));
    for form in ["raw", "display"] {
        let mut params = json!({"limit":2});
        if form == "display" {
            params["form"] = json!(form);
        }
        let mut cursors = BTreeSet::new();
        let mut seen = BTreeSet::new();
        loop {
            let page = rig
                .request(connection, route, "session.history", params.clone())
                .await
                .unwrap();
            for message in page["messages"].as_array().unwrap() {
                assert!(seen.insert(message["mid"].to_string()), "history overlap");
            }
            rig.replies
                .push(json!({"method":format!("session.history.{form}"),"reply":page}));
            let Some(next) = page["older"].as_str() else {
                break;
            };
            assert!(
                cursors.insert(next.to_owned()),
                "history continuation did not advance"
            );
            params["cursor"] = json!(next);
        }
        assert_eq!(seen.len(), collected.len(), "history has holes");
    }
    let first = rig
        .request(
            connection,
            route,
            "run.tool_calls",
            json!({"run_id":run,"limit":1}),
        )
        .await
        .unwrap();
    assert_eq!(first["calls"].as_array().unwrap().len(), 1);
    assert!(first["calls"][0]["call_key"].is_string());
    let next = first["next"]
        .as_str()
        .expect("nonempty run.tool_calls continuation")
        .to_owned();
    let second = rig
        .request(
            connection,
            route,
            "run.tool_calls",
            json!({"run_id":run,"limit":1,"after":next}),
        )
        .await
        .unwrap();
    assert_eq!(second["calls"].as_array().unwrap().len(), 1);
    assert!(second["calls"][0]["call_key"].is_string());
    assert_ne!(
        first["calls"][0]["tool_call_id"],
        second["calls"][0]["tool_call_id"]
    );
    assert!(second.get("next").is_none_or(Value::is_null));
    rig.replies.extend([
        json!({"method":"run.tool_calls.first","reply":first}),
        json!({"method":"run.tool_calls.next","reply":second}),
    ]);
    next
}

fn cursor(value: &Value) -> (u64, u64) {
    (
        value["wal_seq"].as_u64().unwrap(),
        value["sub_index"].as_u64().unwrap(),
    )
}

fn terminal(event: &Value, run: &str) -> bool {
    event["kind"] == "control"
        && event["unit"]["type"] == "run_finished"
        && event["unit"]["run_id"] == run
}

async fn live_tail(
    rig: &mut Rig,
    connection: &SubcConnection,
    route: &RouteHandle,
    handoff: Value,
    resumed_run: &str,
) {
    let subscription = connection
        .route_open_management("broca", &rig.identity())
        .await
        .unwrap();
    let mut stream = connection
        .subscribe(
            &subscription,
            serde_json::to_vec(&json!({
                "method":"session.subscribe","params":{"from":handoff}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    // Receiving the historical terminal proves attachment is installed before new work
    // starts, so the next run cannot accidentally be covered by replay alone.
    let mut events = vec![];
    loop {
        let bytes = stream
            .next()
            .await
            .unwrap()
            .expect("historical attachment reaches the resumed terminal");
        let event: Value = serde_json::from_slice(&bytes).unwrap();
        let done = terminal(&event, resumed_run);
        events.push(event);
        rig.check();
        if done {
            break;
        }
    }
    let sent = rig
        .request(connection, route, "session.send", rig.send(true))
        .await
        .unwrap();
    let run = sent["run_id"].as_str().unwrap().to_owned();
    rig.replies
        .push(json!({"method":"session.send.tail","reply":sent}));
    rig.script.tail_entered.notified().await;
    rig.check();
    if rig.overhaul {
        inspect_database(&rig.state);
    }
    rig.script.tail_release.notify_one();
    loop {
        let bytes = stream
            .next()
            .await
            .unwrap()
            .expect("live subscription stays open");
        let event: Value = serde_json::from_slice(&bytes).unwrap();
        let done = terminal(&event, &run);
        events.push(event);
        rig.check();
        if done {
            break;
        }
    }
    drop(stream);
    connection.route_close(&subscription).await.unwrap();
    let status = rig.state(connection, route, &run, "completed").await;
    rig.replies
        .push(json!({"method":"run.status.tail","reply":status}));
    let controls: Vec<_> = events.iter().filter(|v| v["kind"] == "control").collect();
    assert!(!controls.is_empty());
    let positions: Vec<_> = controls.iter().map(|v| cursor(&v["cursor"])).collect();
    assert!(
        positions[0] > cursor(&handoff),
        "attach re-emitted its handoff"
    );
    assert!(
        positions.windows(2).all(|w| w[0] < w[1]),
        "duplicate or reordered live cursor"
    );
    let sources = Sources::read(rig);
    let expected: Vec<_> = sources
        .projected
        .iter()
        .copied()
        .filter(|p| *p > cursor(&handoff) && *p <= *positions.last().unwrap())
        .collect();
    assert_eq!(
        positions, expected,
        "live handoff must cover every projected WAL unit"
    );

    // Read the same durable interval after the live tail has finished. Matching the entire
    // control interval checks holes as well as duplicates at the replay/live handoff.
    let replay_route = connection
        .route_open_management("broca", &rig.identity())
        .await
        .unwrap();
    let mut replay = connection
        .subscribe(
            &replay_route,
            serde_json::to_vec(&json!({
                "method":"session.subscribe","params":{"from":handoff}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    let mut replayed = vec![];
    loop {
        let bytes = replay
            .next()
            .await
            .unwrap()
            .expect("replay reaches the tail terminal");
        let event: Value = serde_json::from_slice(&bytes).unwrap();
        let done = terminal(&event, &run);
        if event["kind"] == "control" {
            replayed.push(event);
        }
        if done {
            break;
        }
    }
    drop(replay);
    connection.route_close(&replay_route).await.unwrap();
    assert_eq!(
        positions,
        replayed
            .iter()
            .map(|v| cursor(&v["cursor"]))
            .collect::<Vec<_>>()
    );
    for (live, replay) in controls.iter().zip(&replayed) {
        assert_eq!(live["unit"], replay["unit"]);
        assert_eq!(live["event"], replay["event"]);
    }
    rig.replies
        .push(json!({"method":"session.subscribe.handoff","handoff":handoff,"events":events}));
    let history = rig
        .request(connection, route, "session.history", json!({"limit":100}))
        .await
        .unwrap();
    rig.replies
        .push(json!({"method":"session.history.after_tail","reply":history}));
}

async fn differential() {
    let mut legacy = Rig::new(false).await;
    let mut overhaul = Rig::new(true).await;
    let mut runs = vec![];
    let mut handoffs = vec![];
    for rig in [&mut legacy, &mut overhaul] {
        let (connection, route) = rig.connect().await;
        let sent = rig
            .request(&connection, &route, "session.send", rig.send(false))
            .await
            .unwrap();
        let run = sent["run_id"].as_str().unwrap().to_owned();
        rig.replies
            .push(json!({"method":"session.send.first","reply":sent}));
        let paused = rig.state(&connection, &route, &run, "paused").await;
        assert_eq!(paused["reason"], "auth_required");
        rig.replies
            .push(json!({"method":"run.status.paused","reply":paused}));
        assert_eq!(
            rig.script.dispatches.lock().unwrap().len(),
            2,
            "two real dispatched calls"
        );
        broca_test_support::wait_until("pre-restart durable attempt reports", || async {
            rig.check();
            let reports = rig.script.reports.lock().unwrap();
            if reports.iter().filter(|v| v["kind"] == "attempt").count() == 3 {
                Ok(())
            } else {
                Err(format!("reports={reports:?}"))
            }
        })
        .await;
        let head = rig
            .request(&connection, &route, "session.head", json!({}))
            .await
            .unwrap();
        handoffs.push(head["head"].clone());
        rig.replies
            .push(json!({"method":"session.head.paused","reply":head}));
        if rig.overhaul {
            inspect_database(&rig.state);
        }
        connection.route_close(&route).await.unwrap();
        drop(connection);
        rig.stop();
        rig.start().await;
        runs.push(run);
    }
    let (lc, lr) = legacy.connect().await;
    let (oc, or) = overhaul.connect().await;
    let mut tokens = vec![];
    for (i, (rig, connection, route)) in [(&mut legacy, &lc, &lr), (&mut overhaul, &oc, &or)]
        .into_iter()
        .enumerate()
    {
        let resumed = rig
            .request(connection, route, "session.send", rig.send(false))
            .await
            .unwrap();
        assert_eq!(resumed["run_id"], runs[i], "restart resumes the same run");
        rig.replies
            .push(json!({"method":"session.send.resume","reply":resumed}));
        rig.state(connection, route, &runs[i], "completed").await;
        assert_eq!(
            rig.script.dispatches.lock().unwrap().len(),
            2,
            "resume must not redispatch"
        );
        tokens.push(pages(rig, connection, route, &runs[i]).await);
        if rig.overhaul {
            inspect_database(&rig.state);
        }
    }
    for (i, (rig, connection, route)) in [(&legacy, &lc, &lr), (&overhaul, &oc, &or)]
        .into_iter()
        .enumerate()
    {
        let other = 1 - i;
        let foreign = rig
            .request(
                connection,
                route,
                "run.tool_calls",
                json!({"run_id":runs[i],"after":tokens[other]}),
            )
            .await
            .unwrap_err();
        assert_eq!(foreign["error"]["code"], "invalid_params");
        let mut tampered: Value = serde_json::from_str(&tokens[i]).unwrap();
        tampered["ordinal"] = json!(tampered["ordinal"].as_u64().unwrap() + 1);
        let refusal = rig
            .request(
                connection,
                route,
                "run.tool_calls",
                json!({"run_id":runs[i],"after":tampered.to_string()}),
            )
            .await
            .unwrap_err();
        assert_eq!(refusal["error"]["code"], "invalid_params");
    }
    live_tail(&mut legacy, &lc, &lr, handoffs[0].clone(), &runs[0]).await;
    live_tail(&mut overhaul, &oc, &or, handoffs[1].clone(), &runs[1]).await;
    for rig in [&legacy, &overhaul] {
        broca_test_support::wait_until("routing reports for both completed runs", || async {
            rig.check();
            let reports = rig.script.reports.lock().unwrap();
            if reports
                .iter()
                .filter(|v| v["kind"] == "run_finished")
                .count()
                == 2
            {
                Ok(())
            } else {
                Err(format!("reports={reports:?}"))
            }
        })
        .await;
    }
    let left = Sources::read(&legacy);
    let right = Sources::read(&overhaul);
    correspondence(&left, &right);
    assert_eq!(left.runs.len(), 2);
    assert_eq!(right.runs.len(), 2);
    for (i, (a, b)) in legacy.replies.iter().zip(&overhaul.replies).enumerate() {
        assert_eq!(
            serde_json::to_vec(&left.normalize(a)).unwrap(),
            serde_json::to_vec(&right.normalize(b)).unwrap(),
            "wire response {i}: {}",
            a["method"]
        );
    }
    assert_eq!(legacy.replies.len(), overhaul.replies.len());
    let left_requests = legacy.script.requests.lock().unwrap().clone();
    let right_requests = overhaul.script.requests.lock().unwrap().clone();
    assert_eq!(
        left_requests.len(),
        5,
        "initial tools, auth pause, restart resume and live tail"
    );
    assert_eq!(
        left_requests, right_requests,
        "provider request bodies, without JSON reserialization"
    );
    // The resumed request includes the dispatched outputs; equality of two empty prompts
    // would not exercise reconstruction from the persisted conversation.
    assert!(std::str::from_utf8(&left_requests[3])
        .unwrap()
        .contains(OUTPUT));
    assert_eq!(
        left.normalize(&json!(*legacy.script.dispatches.lock().unwrap())),
        right.normalize(&json!(*overhaul.script.dispatches.lock().unwrap()))
    );
    let left_reports = legacy.script.reports.lock().unwrap().clone();
    let right_reports = overhaul.script.reports.lock().unwrap().clone();
    assert!(left_reports.iter().any(|v| v["kind"] == "attempt"));
    assert_eq!(
        left.normalize(&json!(left_reports)),
        right.normalize(&json!(right_reports)),
        "accounting event source identity"
    );
    assert!(
        legacy.state.join("run-index.db").exists(),
        "the control root must use the legacy store"
    );
    inspect_database(&overhaul.state);
    legacy.check();
    overhaul.check();
    lc.route_close(&lr).await.unwrap();
    oc.route_close(&or).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_daemon_legacy_and_overhaul_are_field_scoped_differential() {
    assert!(broca_test_support::subc_bin("ck-subc", "SUBC_CORE_BIN").is_file(),
        "the real-daemon differential requires SUBC_CORE_BIN; it must not pass as a dependency skip");
    broca_test_support::event_timeout(
        "legacy/overhaul real-daemon differential",
        differential(),
        || "inspect the differential daemon logs".into(),
    )
    .await
    .unwrap();
}

#[test]
fn differential_oracle_preserves_unlisted_fields_and_checks_own_source() {
    let sources = Sources {
        lineage: "own-lineage".into(),
        root: "/own/root".into(),
        runs: vec!["run-a".into()],
        file_id: 7,
        projected: BTreeSet::from([(3, 2)]),
        overhaul: false,
        logical: BTreeMap::from([(3, 1)]),
        records: vec![Source {
            seq: 3,
            end: 120,
            record: json!({"type":"tool_dispatch_intent","tool_call_id":"call-a"}),
        }],
    };
    let value = json!({"lineage_id":"own-lineage","call_key":"own-lineage:3","tool_call_id":"call-a",
        "cursor":{"wal_seq":3,"sub_index":2},"run_id":"run-a","written_at":123,
        "usage":{"input_tokens":17},"unknown_future_field":"must survive","path":"/own/root/state"});
    let normalized = sources.normalize(&value);
    assert_eq!(normalized["cursor"], json!({"wal_seq":1,"sub_index":2}));
    assert_eq!(normalized["usage"], value["usage"]);
    assert_eq!(normalized["unknown_future_field"], "must survive");
    for bad in [
        json!({"lineage_id":"other-lineage"}),
        json!({"call_key":"own-lineage:4"}),
        json!({"call_key":"own-lineage:3","tool_call_id":"wrong-call"}),
    ] {
        assert!(std::panic::catch_unwind(|| sources.normalize(&bad)).is_err());
    }
}

#[test]
fn differential_cursor_field_coverage_rejects_uncompared_additions() {
    fields(
        &json!({"lineage":"own","boundary":2,"backward":true}),
        &["lineage", "boundary", "backward"],
    );
    assert!(std::panic::catch_unwind(|| fields(
        &json!({"lineage":"own","boundary":2,"backward":true,"new_field":9}),
        &["lineage", "boundary", "backward"]
    ))
    .is_err());
}

#[test]
fn differential_cursor_validates_root_local_seek_hint_and_all_fields() {
    fn sources(overhaul: bool) -> Sources {
        let shift = u64::from(overhaul);
        let bodies = [
            json!({"type":"run_started","run_id":"run-a"}),
            json!({"type":"model_step_finished","assistant_message":{"content":[{"type":"tool_call"}]}}),
            json!({"type":"tool_batch_finished"}),
            json!({"type":"model_step_finished","assistant_message":{"content":[{"type":"tool_call"}]}}),
            json!({"type":"tool_batch_finished"}),
            json!({"type":"run_finished","run_id":"run-a"}),
        ];
        let records: Vec<_> = bodies
            .into_iter()
            .enumerate()
            .map(|(i, record)| Source {
                seq: i as u64 + 1 + shift,
                end: (i as u64 + 1) * 100,
                record,
            })
            .collect();
        Sources {
            lineage: format!("lineage-{overhaul}"),
            logical: records
                .iter()
                .enumerate()
                .map(|(i, s)| (s.seq, i + 1))
                .collect(),
            records,
            root: "/root".into(),
            runs: vec!["run-a".into()],
            file_id: 7,
            projected: BTreeSet::new(),
            overhaul,
        }
    }
    let left = sources(false);
    let right = sources(true);
    let token = json!({"version":1,"run_id":"run-a","lineage_id":left.lineage,
        "run_start_seq":1,"run_end_seq":6,"step_seq":4,"call_index":0,"batch_end_seq":5,"ordinal":3,
        "anchor_before":{"file_id":7,"seq":3,"byte_end":300},"after_batch":[],"authentication":"0".repeat(64)});
    let mut other = token.clone();
    other["lineage_id"] = json!(right.lineage);
    for key in ["run_start_seq", "run_end_seq", "step_seq", "batch_end_seq"] {
        other[key] = json!(other[key].as_u64().unwrap() + 1);
    }
    other["anchor_before"] = Value::Null;
    assert_eq!(
        left.calls_cursor(&token.to_string()),
        right.calls_cursor(&other.to_string())
    );
    let mut extra = token.clone();
    extra["uncompared"] = json!(true);
    assert!(std::panic::catch_unwind(|| left.calls_cursor(&extra.to_string())).is_err());
    let mut wrong_end = token.clone();
    wrong_end["anchor_before"]["byte_end"] = json!(299);
    assert!(std::panic::catch_unwind(|| left.calls_cursor(&wrong_end.to_string())).is_err());
    let mut invented_hint = other.clone();
    invented_hint["anchor_before"] = token["anchor_before"].clone();
    assert!(std::panic::catch_unwind(|| right.calls_cursor(&invented_hint.to_string())).is_err());
}
