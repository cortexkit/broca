//! Differential coverage of the installed legacy and format-2 service compositions.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
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
const GENERATION_DURATION_FIELDS: &[&str] = &[
    "sent_to_last_content_ms",  // crates/broca-protocol/src/usage.rs:116
    "first_to_last_content_ms", // crates/broca-protocol/src/usage.rs:117
];

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
    reservations: Mutex<Vec<Result<Option<ReservationReceipt>, String>>>,
    dispatches: Mutex<Vec<Value>>,
    reports: Mutex<Vec<Value>>,
    tail_entered: Notify,
    tail_release: Notify,
    observed: Mutex<Observed>,
}

#[derive(Clone, Debug)]
struct ReservationReceipt {
    lineage: String,
    attempt_id: String,
    seq: u64,
    end: u64,
    durable_seq: u64,
    durable_end: u64,
    reservation: Value,
}

#[derive(Default)]
struct Observed {
    phase: String,
    waiting: String,
    module_state: Option<String>,
    run_state: Option<Value>,
    last_event: Option<Value>,
}

struct Trace {
    label: &'static str,
    root: PathBuf,
    state: PathBuf,
    script: Arc<Script>,
}

#[derive(Default)]
struct WalObservation {
    last: Option<(u64, String)>,
    run_state: Option<Value>,
    completed_tools: BTreeSet<String>,
    incomplete: bool,
}

fn observed_run_state(record: &Value, current_run: &mut Value) -> Option<Value> {
    let (state, run_id) = match record["type"].as_str()? {
        "run_started" => {
            *current_run = record["run_id"].clone();
            ("active", current_run.clone())
        }
        "run_paused" => ("paused", record["run_id"].clone()),
        "run_finished" => ("finished", current_run.clone()),
        _ => return None,
    };
    Some(json!({"run_id":run_id,"state":state,"reason":record["reason"]}))
}

fn observe_wal(state: &Path) -> WalObservation {
    let mut observation = WalObservation::default();
    let Ok(entries) = std::fs::read_dir(state.join("wal")) else {
        return observation;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().is_none_or(|extension| extension != "wal") {
            continue;
        }
        let Ok(bytes) = std::fs::read(path) else {
            continue;
        };
        let mut at = 0;
        let mut current_run = Value::Null;
        while at < bytes.len() {
            match decode_next(&bytes[at..]) {
                DecodeStep::Lineage { consumed, .. } => at += consumed,
                DecodeStep::Record { framed, consumed } => {
                    at += consumed;
                    if let broca_wal::WalRecord::ToolResult { result, .. } = &framed.record {
                        observation
                            .completed_tools
                            .insert(result.tool_call_id.0.clone());
                    }
                    let Ok(record) = serde_json::to_value(&framed.record) else {
                        continue;
                    };
                    let kind = record["type"].as_str().unwrap_or("unknown");
                    observation.last = Some((framed.seq, kind.to_owned()));
                    if let Some(state) = observed_run_state(&record, &mut current_run) {
                        observation.run_state = Some(state);
                    }
                }
                _ => {
                    observation.incomplete = true;
                    break;
                }
            }
        }
    }
    observation
}

impl Trace {
    fn waiting(&self, what: impl Into<String>) {
        self.script.observed.lock().unwrap().waiting = what.into();
    }

    fn event(&self, event: &Value) {
        self.script.observed.lock().unwrap().last_event = Some(event.clone());
    }

    fn snapshot(&self) -> String {
        let wal = observe_wal(&self.state);
        let observed = self.script.observed.lock().unwrap();
        let last = wal
            .last
            .map_or_else(|| "none".into(), |(seq, kind)| format!("{kind}@{seq}"));
        format!("root={} scratch={} waiting={} last_record={} incomplete_tail={} run_state={} wal_run_state={} last_event={} provider_requests={} dispatched_calls={} reports={}",
            self.label, self.root.display(), observed.waiting, last, wal.incomplete,
            observed.run_state.as_ref().unwrap_or(&Value::Null), wal.run_state.as_ref().unwrap_or(&Value::Null),
            observed.last_event.as_ref().unwrap_or(&Value::Null), self.script.requests.lock().unwrap().len(),
            self.script.dispatches.lock().unwrap().len(), self.script.reports.lock().unwrap().len())
    }

    fn complete(&self, phase: &str) {
        eprintln!("differential phase complete: {phase}; {}", self.snapshot());
    }

    async fn phase<T>(&self, phase: &str, work: impl Future<Output = T>) -> T {
        self.script.observed.lock().unwrap().phase = phase.to_owned();
        self.waiting(phase);
        let label = format!("differential {}: {phase}", self.label);
        let result = broca_test_support::event_timeout(&label, work, || self.failure_snapshot())
            .await
            .unwrap_or_else(|_| panic!("{label} timed out; {}", self.failure_snapshot()));
        self.complete(phase);
        result
    }

    fn failure_snapshot(&self) -> String {
        let (phase, module_state) = {
            let observed = self.script.observed.lock().unwrap();
            (observed.phase.clone(), observed.module_state.clone())
        };
        format!(
            "phase={phase}; state_root={}; last module state: {}; {}\n{}",
            self.state.display(),
            module_state.as_deref().unwrap_or("not observed"),
            self.snapshot(),
            broca_test_support::rig_log_diagnostics(
                &self.root.join("data-home/cortexkit"),
                &self.state,
            )
        )
    }
}

async fn provider(
    script: Arc<Script>,
    billing_root: Option<PathBuf>,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/v1", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let script = script.clone();
            let billing_root = billing_root.clone();
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
                let receipt = match &billing_root {
                    Some(root) => {
                        capture_reservation(root, turn, &bytes[start..start + length]).map(Some)
                    }
                    None => Ok(None),
                };
                script.reservations.lock().unwrap().push(receipt);
                script.observed.lock().unwrap().last_event = Some(json!({"provider_request":turn}));
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
        let dispatched = self.script.dispatches.lock().unwrap().len();
        self.script.observed.lock().unwrap().last_event =
            Some(json!({"tool_dispatches":dispatched}));
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

/// The connection file precedes module readiness. Retry only the daemon's coded warming refusal,
/// and inspect its supervisor report before each attempt so a dead broca is not waited out.
async fn rig_management_open<T, State, Open>(
    trace: &Trace,
    module_state: impl Fn() -> State,
    open: impl Fn() -> Open,
) -> T
where
    State: Future<Output = Value>,
    Open: Future<Output = broca_subc::connection::ConnResult<T>>,
{
    let description = format!("{} management route to broca", trace.label);
    broca_test_support::wait_until(&description, || async {
        let state = module_state().await;
        trace.script.observed.lock().unwrap().module_state = Some(state.to_string());
        if broca_has_exited(&state) {
            panic!(
                "broca process exited or failed to start before management route opened; {}",
                trace.failure_snapshot()
            );
        }
        match open().await {
            Ok(route) => Ok(route),
            Err(error) => {
                trace.script.observed.lock().unwrap().module_state =
                    Some(format!("{state}; last open refusal: {error}"));
                if error.code() != Some("module_warming") {
                    panic!(
                        "management route to broca failed: {error}; {}",
                        trace.failure_snapshot()
                    );
                }
                Err(trace.failure_snapshot())
            }
        }
    })
    .await
}

fn broca_has_exited(report: &Value) -> bool {
    report["supervisor.list"]["modules"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|module| module["module_id"] == "broca")
        .any(|module| {
            matches!(module["state"].as_str(), Some("failed" | "stopped"))
                // A restart must not hide a boot crash even if its replacement is already live.
                || ["last_exit_code", "last_exit_signal", "last_exit_ms", "last_exit_kind"]
                    .iter()
                    .any(|key| !module[key].is_null())
        })
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
    trace: Arc<Trace>,
    daemon_logs: Vec<PathBuf>,
    completed: bool,
}

impl Drop for Rig {
    fn drop(&mut self) {
        let keep = !self.completed || std::thread::panicking();
        if keep {
            // During unwinding a second panic aborts the test binary, so diagnostics are
            // printed and nothing else may fail here.
            eprintln!("{}", self.diagnostics());
        }
        self.cleanup();
        self.provider.abort();
        if !keep {
            broca_test_support::remove_rig_dir(&self.root);
        }
    }
}

impl Rig {
    /// Keep process identity beside the exit status: the rig owns a daemon child, while the
    /// daemon owns broca. Their stdout/stderr streams do not identify broca's boot failures.
    fn diagnostics(&mut self) -> String {
        let mut output = format!(
            "differential failure artifacts: root={} scratch={} state={}\n{}",
            self.trace.label,
            self.root.display(),
            self.state.display(),
            self.trace.failure_snapshot()
        );
        match self.daemon.as_mut() {
            Some(child) => {
                let pid = child.id();
                let status = match child.try_wait() {
                    Ok(Some(status)) => format!("exited before cleanup: {status}"),
                    Ok(None) => "running".to_owned(),
                    Err(error) => format!("status unavailable: {error}"),
                };
                output.push_str(&format!(
                    "\ndaemon-{} pid={pid:?}: {status}\n",
                    self.daemon_logs.len().saturating_sub(1) / 2
                ));
            }
            None => output.push_str("\ndaemon: none spawned\n"),
        }
        let logs = self
            .daemon_logs
            .chunks(2)
            .enumerate()
            .map(|(index, paths)| {
                // start() names each daemon child `daemon-<n>` and keeps its two log paths in order.
                let mut part = format!("--- daemon-{index} ---\n");
                for path in paths {
                    let label = path.display().to_string();
                    broca_test_support::append_file_tail(&mut part, &label, path);
                }
                part
            });
        output.extend(logs);
        output
    }

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
        let (url, provider) = provider(script.clone(), overhaul.then(|| state.clone())).await;
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
        let trace = Arc::new(Trace {
            label: if overhaul { "format-2" } else { "legacy" },
            root: root.clone(),
            state: state.clone(),
            script: script.clone(),
        });
        Self {
            root,
            state,
            overhaul,
            script,
            provider,
            daemon: None,
            modules: vec![],
            replies: vec![],
            trace,
            daemon_logs: vec![],
            completed: false,
        }
    }

    fn check(&self) {
        if self.overhaul {
            no_legacy_index(&self.state);
        }
        for receipt in self.script.reservations.lock().unwrap().iter() {
            assert!(
                receipt.is_ok(),
                "{} physical-attempt reservation: {receipt:?}",
                self.trace.label
            );
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
        let name = format!("daemon-{}", self.daemon_logs.len() / 2);
        self.daemon_logs.extend([
            self.root.join(format!("{name}.stdout.log")),
            self.root.join(format!("{name}.stderr.log")),
        ]);
        self.trace.waiting(format!("spawn {name}"));
        let child = broca_test_support::retry_executable_file_busy(|| {
            broca_test_support::tokio_rig_command(&binary, &self.root, &name)?
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
        self.daemon = Some(broca_test_support::RigChild::new(child, &self.root, &name));
        self.trace.waiting("daemon connection file");
        broca_test_support::wait_until(
            &format!("{} daemon connection file", self.trace.label),
            || async {
                self.check();
                self.file()
                    .exists()
                    .then_some(())
                    .ok_or_else(|| self.trace.snapshot())
            },
        )
        .await;
        self.trace.waiting("register claustrum");
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
        self.trace.waiting("register prefrontal-routing");
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
        self.trace.waiting("register tool provider");
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

    async fn stop(&mut self) {
        self.check();
        if let Some(mut child) = self.daemon.take() {
            tokio::task::spawn_blocking(move || broca_test_support::kill_and_reap_tree(&mut child))
                .await
                .unwrap();
        }
        for task in self.modules.drain(..) {
            task.abort();
        }
        self.check();
    }

    fn cleanup(&mut self) {
        if let Some(mut child) = self.daemon.take() {
            broca_test_support::kill_and_reap_tree(&mut child);
        }
        for task in self.modules.drain(..) {
            task.abort();
        }
    }

    /// Connect and open broca's management route, waiting while broca reports that it is not
    /// live yet.
    async fn connect(&self) -> (SubcConnection, RouteHandle) {
        self.trace.waiting("connect to daemon");
        let connection = SubcConnection::connect(&self.file()).await.unwrap();
        self.trace.waiting("open management route");
        let route = rig_management_open(
            &self.trace,
            || connection.supervisor_module_report("broca"),
            || async {
                connection
                    .route_open_management_within(
                        "broca",
                        &self.identity(),
                        // The shared hang guard owns the wait; each probe must return its refusal.
                        broca_subc::connection::RouteOpenRetry::within(
                            std::time::Duration::ZERO,
                            std::time::Duration::ZERO,
                        ),
                    )
                    .await
            },
        )
        .await;
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
        self.trace.waiting(format!("request {method}"));
        let reply = connection
            .route_request_terminal(
                route,
                serde_json::to_vec(&json!({"method":method,"params":params})).unwrap(),
            )
            .await
            .unwrap();
        self.check();
        match reply {
            Ok(bytes) => {
                let result = serde_json::from_slice::<Value>(&bytes).unwrap()["result"].clone();
                if matches!(method, "session.send" | "run.status" | "run.result") {
                    self.script.observed.lock().unwrap().run_state = Some(result.clone());
                }
                Ok(result)
            }
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
        broca_test_support::wait_until(
            &format!("{} run state {wanted}", self.trace.label),
            || async {
                let value = self
                    .request(connection, route, "run.status", json!({"run_id":run}))
                    .await
                    .unwrap();
                if value["state"] == wanted {
                    Ok(value)
                } else {
                    Err(self.trace.snapshot())
                }
            },
        )
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

fn source_wal_path(state: &Path) -> Result<PathBuf, String> {
    fn logs(dir: &Path, found: &mut Vec<PathBuf>) -> Result<(), String> {
        for entry in std::fs::read_dir(dir).map_err(|error| error.to_string())? {
            let path = entry.map_err(|error| error.to_string())?.path();
            if path.is_dir() {
                logs(&path, found)?;
            } else if path.extension().is_some_and(|extension| extension == "wal") {
                found.push(path);
            }
        }
        Ok(())
    }
    let mut paths = vec![];
    logs(state, &mut paths)?;
    if paths.len() != 1 {
        return Err(format!("expected one lineage WAL: {paths:?}"));
    }
    Ok(paths.remove(0))
}

fn capture_reservation(
    state: &Path,
    attempt: usize,
    body: &[u8],
) -> Result<ReservationReceipt, String> {
    use broca_wal::head_proof::{sidecar_path, PublishedLog};
    let path = source_wal_path(state)?;
    let sidecar = std::fs::read(sidecar_path(&path))
        .map_err(|error| format!("reservation head proof: {error}"))?;
    let bytes = std::fs::read(&path).map_err(|error| error.to_string())?;
    let published = PublishedLog::open(&bytes, &sidecar)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "reservation has no valid synced-prefix proof".to_owned())?;
    let end = usize::try_from(published.head().durable_end).map_err(|error| error.to_string())?;
    // Check the writer's synced-prefix proof when HTTP arrives, before sending the response.
    let (lineage, records) = decode_sources(&bytes[..end]);
    let reservations: Vec<_> = records
        .iter()
        .filter(|source| source.record["type"] == "attempt_reserved")
        .collect();
    if reservations.len() != attempt + 1 {
        return Err(format!(
            "physical attempt {} has {} durable reservations",
            attempt + 1,
            reservations.len()
        ));
    }
    let reserved = reservations[attempt];
    let id = reserved.record["attempt_id"]
        .as_str()
        .ok_or("reservation has no attempt_id")?;
    let reservation = &reserved.record["reservation"];
    if reservation["request_body_bytes"].as_u64() != Some(body.len() as u64)
        || reservation["compressed"] != false
    {
        return Err(format!("reservation {id} does not describe this HTTP body"));
    }
    if records.iter().any(|source| {
        source.record.get("attempt_id").and_then(Value::as_str) == Some(id)
            && source.record["type"] != "attempt_reserved"
    }) {
        return Err(format!(
            "attempt {id} settled before its HTTP request was answered"
        ));
    }
    Ok(ReservationReceipt {
        lineage,
        attempt_id: id.to_owned(),
        seq: reserved.seq,
        end: reserved.end,
        durable_seq: published.head().durable_seq,
        durable_end: published.head().durable_end,
        reservation: reservation.clone(),
    })
}

fn assert_billing_attempts(
    lineage: &str,
    overhaul: bool,
    records: &[Source],
    requests: &[Vec<u8>],
    receipts: &[Result<Option<ReservationReceipt>, String>],
) {
    use sha2::{Digest, Sha256};
    let attempts: Vec<_> = records
        .iter()
        .filter(|source| source.record["type"] == "model_attempt_finished")
        .collect();
    assert_eq!(
        attempts.len(),
        requests.len(),
        "one terminal per physical model attempt"
    );
    assert_eq!(
        receipts.len(),
        requests.len(),
        "one observation per physical HTTP request"
    );
    for (terminal, body) in attempts.iter().zip(requests) {
        assert_eq!(terminal.record["request_body_len"], body.len() as u64);
        assert_eq!(
            terminal.record["request_body_sha256"],
            format!("{:x}", Sha256::digest(body))
        );
    }
    if !overhaul {
        assert!(
            !records.iter().any(|source| billing_only(&source.record)),
            "legacy WAL has billing-only records"
        );
        assert!(receipts.iter().all(|receipt| matches!(receipt, Ok(None))));
        for terminal in attempts {
            for field in ["attempt_id", "reservation", "envelope"] {
                assert!(
                    terminal.record[field].is_null(),
                    "legacy terminal gained {field}"
                );
            }
        }
        return;
    }
    let reservations: Vec<_> = records
        .iter()
        .filter(|source| source.record["type"] == "attempt_reserved")
        .collect();
    assert_eq!(
        reservations.len(),
        requests.len(),
        "one billing reservation per physical attempt"
    );
    let mut by_id = BTreeMap::new();
    let mut current_run = Value::Null;
    for source in records {
        if source.record["type"] == "run_started" {
            current_run = source.record["run_id"].clone();
        }
        if source.record["type"] == "attempt_reserved" {
            let id = source.record["attempt_id"].as_str().unwrap();
            assert_eq!(id.len(), 32);
            assert!(id
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
            assert!(
                by_id.insert(id, source).is_none(),
                "duplicate reservation {id}"
            );
            assert!(current_run.is_string());
            assert_eq!(source.record["reservation"]["run_id"], current_run);
        }
    }
    let mut terminals = BTreeMap::new();
    for (((reserved, terminal), body), receipt) in reservations
        .iter()
        .zip(&attempts)
        .zip(requests)
        .zip(receipts)
    {
        let receipt = receipt
            .as_ref()
            .expect("valid reservation proof")
            .as_ref()
            .expect("format-2 reservation receipt");
        let id = reserved.record["attempt_id"].as_str().unwrap();
        let reservation = &reserved.record["reservation"];
        assert_eq!(receipt.lineage, lineage);
        assert_eq!(
            receipt.attempt_id, id,
            "reservation order must match HTTP arrival order"
        );
        assert_eq!(receipt.seq, reserved.seq);
        assert_eq!(receipt.end, reserved.end);
        assert_eq!(receipt.reservation, *reservation);
        assert!(
            receipt.durable_seq >= reserved.seq && receipt.durable_end >= reserved.end,
            "reservation must already be published durable when HTTP arrives"
        );
        assert!(
            receipt.durable_seq < terminal.seq && reserved.seq < terminal.seq,
            "reservation durability must precede its terminal"
        );
        assert_eq!(terminal.record["attempt_id"], id);
        assert_eq!(terminal.record["reservation"], *reservation);
        for field in ["step_id", "attempt"] {
            assert!(reservation[field].is_u64());
            assert_eq!(terminal.record[field], reservation[field]);
        }
        assert_eq!(reservation["request_body_bytes"], body.len() as u64);
        assert_eq!(reservation["provider"], "xai");
        assert_eq!(reservation["endpoint_host"], "127.0.0.1");
        assert_eq!(
            reservation["model_sent"],
            serde_json::from_slice::<Value>(body).unwrap()["model"]
        );
        assert_eq!(reservation["send_index"], 1);
        assert_eq!(reservation["warm"], false);
        assert_eq!(reservation["compressed"], false);
        let envelope = &terminal.record["envelope"];
        assert!(envelope.is_object());
        assert_eq!(envelope["sent"], true);
        assert_eq!(envelope["http_status"], terminal.record["http_status"]);
        assert_eq!(
            envelope["response_bytes"],
            terminal.record["response_body_len"]
        );
        assert_eq!(envelope["sent_at_ms"], terminal.record["sent_at_ms"]);
        assert_eq!(envelope["finished_at_ms"], terminal.record["ts_ms"]);
        assert!(
            terminals.insert(id, terminal.seq).is_none(),
            "duplicate attempt terminal {id}"
        );
    }
    let mut corrections = BTreeSet::new();
    for source in records.iter().filter(|source| {
        billing_only(&source.record) && source.record["type"] != "attempt_reserved"
    }) {
        let id = source.record["attempt_id"].as_str().unwrap();
        let reserved = by_id
            .get(id)
            .expect("settlement must name a reserved attempt");
        assert!(reserved.seq < source.seq);
        assert_eq!(source.record["reservation"], reserved.record["reservation"]);
        match source.record["type"].as_str().unwrap() {
            "attempt_finished" | "attempt_released" => {
                assert!(source.record["envelope"].is_object());
                if source.record["type"] == "attempt_released" {
                    assert_eq!(source.record["envelope"]["sent"], false);
                }
                assert!(
                    terminals.insert(id, source.seq).is_none(),
                    "attempt has two terminals: {id}"
                );
            }
            "attempt_corrected" => {
                assert!(
                    *terminals
                        .get(id)
                        .expect("correction must follow a terminal")
                        < source.seq
                );
                assert!(source.record["envelope"].is_object());
                assert!(corrections.insert(source.record["correction_id"].as_str().unwrap()));
            }
            _ => unreachable!(),
        }
    }
    // This clean HTTP fixture has no abandoned sends, recovery settlements or corrections.
    assert_eq!(
        records
            .iter()
            .filter(|source| billing_only(&source.record))
            .count(),
        reservations.len(),
        "unexpected billing kind: inspect it before excluding it from correspondence"
    );
}

fn decode_sources(bytes: &[u8]) -> (String, Vec<Source>) {
    let mut at = 0;
    let mut records = vec![];
    let mut lineage: Option<broca_wal::WalLineageId> = None;
    let mut next_seq = 1;
    while at < bytes.len() {
        match decode_next(&bytes[at..]) {
            DecodeStep::Lineage {
                lineage_id,
                consumed,
            } => {
                // Mirror FileWal recovery: crates/broca-wal/src/file_wal.rs:3115-3125,3278-3289.
                // Repeated headers authenticate the first lineage; only records advance sequence.
                if let Some(first) = lineage {
                    assert_eq!(lineage_id, first, "lineage changed at byte {at}");
                    if let Some(incarnation) = first.incarnation() {
                        assert_eq!(
                            lineage_id.incarnation(),
                            Some(incarnation),
                            "incarnation changed at byte {at}"
                        );
                    }
                } else {
                    assert_eq!(at, 0, "the first lineage frame must start the WAL");
                    lineage = Some(lineage_id);
                }
                at += consumed;
            }
            DecodeStep::Record { framed, consumed } => {
                assert!(
                    lineage.is_some(),
                    "record before the initial lineage frame at byte {at}"
                );
                assert_eq!(
                    framed.seq, next_seq,
                    "noncontiguous record sequence at byte {at}"
                );
                next_seq += 1;
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
    (
        lineage
            .expect("WAL must have an initial lineage frame")
            .to_string(),
        records,
    )
}

impl Sources {
    fn assert_api_key_attempts(&self, reports: &[Value], expected_statuses: &[u64]) {
        // API keys bypass the subscription-account chooser; a vault-supplied account_ref
        // does not create a chooser decision. In legacy and format-2 state storage, this
        // run's attempt evidence is ModelAttemptFinished in the WAL, not subscription-account
        // reports through route.account_observe.
        let starts: Vec<_> = self
            .records
            .iter()
            .filter(|source| source.record["type"] == "run_started")
            .collect();
        assert!(
            !starts.is_empty(),
            "attempt evidence must name an admitted run"
        );
        for start in starts {
            let selection = &start.record["config"]["auth_selection"];
            assert_eq!(selection["credential_id"], "apikey:xai");
            assert_eq!(selection["account_ref"], "acct-fixture");
            assert!(
                selection.get("account_decision").is_none_or(Value::is_null),
                "an API-key run has no subscription-account admission decision"
            );
        }
        let statuses: Vec<_> = self
            .records
            .iter()
            .filter(|source| source.record["type"] == "model_attempt_finished")
            .map(|source| {
                source.record["http_status"]
                    .as_u64()
                    .expect("scripted HTTP attempt status")
            })
            .collect();
        assert_eq!(
            statuses, expected_statuses,
            "durable model-attempt evidence"
        );
        assert!(
            reports.is_empty(),
            "API-key runs must not produce subscription-account reports: {reports:?}"
        );
    }

    fn read(rig: &Rig) -> Self {
        let path = source_wal_path(&rig.state).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let (lineage, records) = decode_sources(&bytes);
        assert_billing_attempts(
            &lineage,
            rig.overhaul,
            &records,
            &rig.script.requests.lock().unwrap(),
            &rig.script.reservations.lock().unwrap(),
        );
        let logical = records
            .iter()
            .filter(|s| comparable_source(&s.record))
            .enumerate()
            .map(|(i, s)| (s.seq, i + 1))
            .collect();
        let runs = records
            .iter()
            .filter(|s| s.record["type"] == "run_started")
            .map(|s| s.record["run_id"].as_str().unwrap().to_owned())
            .collect();
        let range =
            broca_wal::read_records_range(&path, broca_wal::DEFAULT_MAX_READ_BYTES, 0, None, None)
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
            lineage,
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

fn billing_only(record: &Value) -> bool {
    // Format-2 physical-send bookkeeping has no legacy counterpart and is checked separately.
    // ModelAttemptFinished remains comparable because both formats write that terminal.
    matches!(
        record["type"].as_str(),
        Some("attempt_reserved" | "attempt_released" | "attempt_finished" | "attempt_corrected")
    )
}

fn comparable_source(record: &Value) -> bool {
    !lifecycle(record) && !billing_only(record)
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
            .filter(|s| s.record.get("attempt_id").is_some_and(Value::is_string))
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
        assert!(end > start, "run.tool_calls end must follow its start");
        assert_eq!(self.source(end).record["type"], "run_finished");
        // RunStarted owns the episode; RunFinished has no run_id.
        // See crates/broca-wal/src/record.rs:643-658,1241-1260.
        assert!(
            !self.records.iter().any(|source| source.seq > start
                && source.seq < end
                && matches!(
                    source.record["type"].as_str(),
                    Some("run_started" | "run_finished")
                )),
            "run.tool_calls endpoints must delimit one episode"
        );
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

    fn subscribe_lanes(&self, events: &Value) -> Value {
        let mut control = vec![];
        let mut display = vec![];
        let mut previous_cursor = None;
        for (index, event) in events
            .as_array()
            .expect("subscribe events must be an array")
            .iter()
            .enumerate()
        {
            let frame = event
                .as_object()
                .expect("subscribe frame must be an object");
            let seq = frame
                .get("seq")
                .and_then(Value::as_u64)
                .expect("subscribe frame must carry seq");
            assert_eq!(
                seq,
                cortexkit_role_llm_runner::live::FIRST_SEQ + index as u64,
                "each root must retain its own contiguous merged stream sequence"
            );
            let mut normalized = event.clone();
            // seq counts merged deliveries, not durable units.
            // See crates/broca-module-serve/src/serve.rs:4144-4257.
            normalized["seq"] = json!("@STREAM_SEQ@");
            // No cross-lane order is promised: crates/broca-module/src/fanout.rs:1379-1435.
            match frame
                .get("kind")
                .and_then(Value::as_str)
                .expect("subscribe frame must carry its lane")
            {
                "control" => {
                    let position = cursor(&event["cursor"]);
                    if let Some(previous) = previous_cursor {
                        assert!(
                            previous < position,
                            "control cursors must be strictly ordered without duplicates"
                        );
                    }
                    previous_cursor = Some(position);
                    control.push(self.normalize(&normalized));
                }
                "display" => display.push(self.normalize(&normalized)),
                other => panic!("unknown subscribe lane {other}"),
            }
        }
        json!({"control":control,"display":display})
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
                        "events"
                            if object.get("method").and_then(Value::as_str).is_some_and(
                                |method| {
                                    method == "session.subscribe"
                                        || method.starts_with("session.subscribe.")
                                },
                            ) =>
                        {
                            self.subscribe_lanes(value)
                        }
                        "usage" if value.get("generation").is_some() => {
                            let entries = value["generation"]
                                .as_array()
                                .expect("usage.generation must be an array");
                            let mut usage = self.normalize(value);
                            for (entry, normalized) in entries
                                .iter()
                                .zip(usage["generation"].as_array_mut().unwrap())
                            {
                                assert!(
                                    entry.is_object(),
                                    "usage.generation entry must be an object: {entry}"
                                );
                                // Independent calls have different wall-clock intervals, not different token totals.
                                for &field in GENERATION_DURATION_FIELDS {
                                    let duration = entry.get(field).unwrap_or_else(|| {
                                        panic!("missing usage.generation.{field}: {entry}")
                                    });
                                    assert!(duration.as_u64().is_some(), "usage.generation.{field} must be non-negative milliseconds: {duration}");
                                    normalized[field] = json!("@DURATION_MS@");
                                }
                            }
                            usage
                        }
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
        .filter(|s| comparable_source(&s.record))
        .collect();
    let right_records: Vec<_> = right
        .records
        .iter()
        .filter(|s| comparable_source(&s.record))
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
                a.record.get(field).map(|value| left.normalize(value)),
                b.record.get(field).map(|value| right.normalize(value)),
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
    let trace = rig.trace.clone();
    for (method, params) in [
        ("session.head", json!({})),
        ("run.status", json!({"run_id":run})),
        ("run.result", json!({"run_id":run})),
    ] {
        let reply = trace
            .phase(method, rig.request(connection, route, method, params))
            .await
            .unwrap();
        rig.replies.push(json!({"method":method,"reply":reply}));
    }
    let full = trace
        .phase(
            "full session.read",
            rig.request(
                connection,
                route,
                "session.read",
                json!({"include_tools":true}),
            ),
        )
        .await
        .unwrap();
    assert!(full["messages"].as_array().unwrap().len() >= 6);
    let collected = trace
        .phase("paged session.read", async {
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
            collected
        })
        .await;
    assert_eq!(collected, full["messages"].as_array().unwrap().clone());
    rig.replies
        .push(json!({"method":"session.read.full","reply":full}));
    for form in ["raw", "display"] {
        trace
            .phase(&format!("session.history {form}"), async {
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
            })
            .await;
    }
    let first = trace
        .phase(
            "run.tool_calls first page",
            rig.request(
                connection,
                route,
                "run.tool_calls",
                json!({"run_id":run,"limit":1}),
            ),
        )
        .await
        .unwrap();
    assert_eq!(first["calls"].as_array().unwrap().len(), 1);
    assert!(first["calls"][0]["call_key"].is_string());
    let next = first["next"]
        .as_str()
        .expect("nonempty run.tool_calls continuation")
        .to_owned();
    let second = trace
        .phase(
            "run.tool_calls continuation",
            rig.request(
                connection,
                route,
                "run.tool_calls",
                json!({"run_id":run,"limit":1,"after":next}),
            ),
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
    let trace = rig.trace.clone();
    let (mut stream, mut events) = trace
        .phase("live attach: historical handoff", async {
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
                trace.waiting(format!("historical terminal for {resumed_run}"));
                let bytes = stream
                    .next()
                    .await
                    .unwrap()
                    .expect("historical attachment reaches the resumed terminal");
                let event: Value = serde_json::from_slice(&bytes).unwrap();
                trace.event(&event);
                let done = terminal(&event, resumed_run);
                events.push(event);
                rig.check();
                if done {
                    break;
                }
            }
            (stream, events)
        })
        .await;
    let run = trace
        .phase("live attach: held tail admission", async {
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
            run
        })
        .await;
    trace
        .phase("live attach: live terminal", async {
            loop {
                trace.waiting(format!("live terminal for {run}"));
                let bytes = stream
                    .next()
                    .await
                    .unwrap()
                    .expect("live subscription stays open");
                let event: Value = serde_json::from_slice(&bytes).unwrap();
                trace.event(&event);
                let done = terminal(&event, &run);
                events.push(event);
                rig.check();
                if done {
                    break;
                }
            }
            // SubscribeStream::drop invalidates this route's handle, so do not close it again.
            drop(stream);
            let status = rig.state(connection, route, &run, "completed").await;
            rig.replies
                .push(json!({"method":"run.status.tail","reply":status}));
        })
        .await;
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
    let replayed = trace
        .phase("live attach: durable replay", async {
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
                trace.waiting(format!("durable replay terminal for {run}"));
                let bytes = replay
                    .next()
                    .await
                    .unwrap()
                    .expect("replay reaches the tail terminal");
                let event: Value = serde_json::from_slice(&bytes).unwrap();
                trace.event(&event);
                let done = terminal(&event, &run);
                if event["kind"] == "control" {
                    replayed.push(event);
                }
                if done {
                    break;
                }
            }
            // SubscribeStream::drop invalidates this route's handle, so do not close it again.
            drop(replay);
            replayed
        })
        .await;
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
    let history = trace
        .phase(
            "session.history after live tail",
            rig.request(connection, route, "session.history", json!({"limit":100})),
        )
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
        let trace = rig.trace.clone();
        let (connection, route) = trace
            .phase("start", async {
                rig.start().await;
                rig.connect().await
            })
            .await;
        let sent = trace
            .phase(
                "first send",
                rig.request(&connection, &route, "session.send", rig.send(false)),
            )
            .await
            .unwrap();
        let run = sent["run_id"].as_str().unwrap().to_owned();
        rig.replies
            .push(json!({"method":"session.send.first","reply":sent}));
        for call in ["call-0", "call-1"] {
            trace
                .phase(
                    &format!("tool call {call}"),
                    broca_test_support::wait_until(
                        &format!("{} tool result {call}", trace.label),
                        || async {
                            rig.check();
                            if observe_wal(&rig.state).completed_tools.contains(call) {
                                Ok(())
                            } else {
                                Err(trace.snapshot())
                            }
                        },
                    ),
                )
                .await;
        }
        let paused = trace
            .phase("auth pause", rig.state(&connection, &route, &run, "paused"))
            .await;
        assert_eq!(paused["reason"], "auth_required");
        rig.replies
            .push(json!({"method":"run.status.paused","reply":paused}));
        assert_eq!(
            rig.script.dispatches.lock().unwrap().len(),
            2,
            "two real dispatched calls"
        );
        trace
            .phase("pre-restart API-key attempt evidence", async {
                Sources::read(rig)
                    .assert_api_key_attempts(&rig.script.reports.lock().unwrap(), &[200, 200, 401]);
            })
            .await;
        let head = trace
            .phase(
                "paused head",
                rig.request(&connection, &route, "session.head", json!({})),
            )
            .await
            .unwrap();
        handoffs.push(head["head"].clone());
        rig.replies
            .push(json!({"method":"session.head.paused","reply":head}));
        if rig.overhaul {
            inspect_database(&rig.state);
        }
        trace
            .phase("restart", async {
                trace.waiting("close pre-restart route");
                connection.route_close(&route).await.unwrap();
                drop(connection);
                trace.waiting("stop pre-restart daemon tree");
                rig.stop().await;
                rig.start().await;
            })
            .await;
        runs.push(run);
    }
    let (lc, lr) = legacy
        .trace
        .phase("reconnect after restart", legacy.connect())
        .await;
    let (oc, or) = overhaul
        .trace
        .phase("reconnect after restart", overhaul.connect())
        .await;
    let mut tokens = vec![];
    for (i, (rig, connection, route)) in [(&mut legacy, &lc, &lr), (&mut overhaul, &oc, &or)]
        .into_iter()
        .enumerate()
    {
        let trace = rig.trace.clone();
        trace
            .phase("resume", async {
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
            })
            .await;
        tokens.push(pages(rig, connection, route, &runs[i]).await);
        if rig.overhaul {
            inspect_database(&rig.state);
        }
    }
    for (i, (rig, connection, route)) in [(&legacy, &lc, &lr), (&overhaul, &oc, &or)]
        .into_iter()
        .enumerate()
    {
        let trace = rig.trace.clone();
        trace
            .phase("run.tool_calls rejection checks", async {
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
                assert_eq!(foreign["code"], "invalid_params", "{foreign}");
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
                assert_eq!(refusal["code"], "invalid_params", "{refusal}");
            })
            .await;
    }
    live_tail(&mut legacy, &lc, &lr, handoffs[0].clone(), &runs[0]).await;
    live_tail(&mut overhaul, &oc, &or, handoffs[1].clone(), &runs[1]).await;
    for rig in [&legacy, &overhaul] {
        let trace = rig.trace.clone();
        trace
            .phase("final API-key attempt evidence", async {
                rig.check();
                Sources::read(rig).assert_api_key_attempts(
                    &rig.script.reports.lock().unwrap(),
                    &[200, 200, 401, 200, 200],
                );
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
    assert!(
        legacy.state.join("run-index.db").exists(),
        "the control root must use the legacy store"
    );
    inspect_database(&overhaul.state);
    legacy.check();
    overhaul.check();
    legacy.trace.complete("source and byte correspondence");
    overhaul.trace.complete("source and byte correspondence");
    legacy
        .trace
        .phase("close final route", lc.route_close(&lr))
        .await
        .unwrap();
    overhaul
        .trace
        .phase("close final route", oc.route_close(&or))
        .await
        .unwrap();
    legacy.completed = true;
    overhaul.completed = true;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_daemon_legacy_and_overhaul_are_field_scoped_differential() {
    assert!(broca_test_support::subc_bin("ck-subc", "SUBC_CORE_BIN").is_file(),
        "the real-daemon differential requires SUBC_CORE_BIN; it must not pass as a dependency skip");
    differential().await;
}

fn diagnostic_rig(tag: &str, overhaul: bool) -> Rig {
    let root = broca_test_support::scratch_root().join(tag);
    let state = root.join("state");
    std::fs::create_dir_all(state.join("wal")).unwrap();
    let script = Arc::new(Script::default());
    let trace = Arc::new(Trace {
        label: if overhaul { "format-2" } else { "legacy" },
        root: root.clone(),
        state: state.clone(),
        script: script.clone(),
    });
    let log = root.join("daemon-0.stderr.log");
    std::fs::write(&log, "retained daemon output").unwrap();
    Rig {
        root,
        state,
        overhaul,
        script,
        trace,
        daemon: None,
        provider: tokio::spawn(std::future::pending()),
        modules: vec![],
        replies: vec![],
        daemon_logs: vec![log],
        completed: false,
    }
}

#[tokio::test]
async fn differential_failure_preserves_both_roots_and_daemon_logs() {
    let legacy = diagnostic_rig("differential-failed-legacy", false);
    let overhaul = diagnostic_rig("differential-failed-format-2", true);
    let roots = [legacy.root.clone(), overhaul.root.clone()];
    drop(legacy);
    drop(overhaul);
    for root in roots {
        assert_eq!(
            std::fs::read_to_string(root.join("daemon-0.stderr.log")).unwrap(),
            "retained daemon output"
        );
        assert!(root.join("state/wal").is_dir());
        broca_test_support::remove_rig_dir(root);
    }
    let mut success = diagnostic_rig("differential-succeeded", false);
    let root = success.root.clone();
    success.completed = true;
    drop(success);
    assert!(!root.exists(), "successful rigs must still clean up");
}

#[tokio::test]
async fn differential_failure_dump_names_exited_child_and_broca_logs() {
    const CHILD_ENV: &str = "BROCA_DIFFERENTIAL_DUMP_CHILD";
    const EXIT_ENV: &str = "BROCA_DIFFERENTIAL_EXIT_CHILD";
    const TEST: &str = "differential_failure_dump_names_exited_child_and_broca_logs";
    if std::env::var_os(EXIT_ENV).is_some() {
        std::process::exit(1);
    }
    if let Some(root) = std::env::var_os(CHILD_ENV) {
        let mut rig = diagnostic_rig(Path::new(&root).to_str().unwrap(), true);
        write_startup_logs(&rig.root);
        let child = broca_test_support::tokio_rig_command(
            std::env::current_exe().unwrap(),
            &rig.root,
            "daemon-0",
        )
        .unwrap()
        .args(["--exact", TEST, "--nocapture"])
        .env(EXIT_ENV, "1")
        .spawn()
        .unwrap();
        let mut child = broca_test_support::RigChild::new(child, &rig.root, "daemon-0");
        assert_eq!(child.wait().await.unwrap().code(), Some(1));
        rig.daemon = Some(child);
        panic!("intentional rig failure to exercise the drop diagnostic");
    }
    let root = broca_test_support::scratch_root().join("failure-dump-child");
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", TEST, "--nocapture"])
        .env(CHILD_ENV, &root)
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    for expected in [
        "intentional rig failure",
        "differential failure artifacts: root=format-2",
        "broca boot final marker",
        "broca startup stderr",
    ] {
        assert!(stderr.contains(expected), "missing {expected:?}: {stderr}");
    }
    let child_status = stderr
        .lines()
        .find(|line| line.starts_with("daemon-0 pid="))
        .unwrap_or_else(|| panic!("daemon child status was not named: {stderr}"));
    assert!(
        child_status.contains("exited before cleanup: exit") && child_status.ends_with("1"),
        "{child_status}"
    );
    assert!(
        stderr.contains(
            &root
                .join("data-home/cortexkit/broca/logs/broca.log")
                .display()
                .to_string()
        ),
        "{stderr}"
    );
    assert!(root.exists(), "failed rig artifacts must be retained");
    broca_test_support::remove_rig_dir(root);
}

#[tokio::test(start_paused = true)]
async fn warming_guard_timeout_reports_the_broca_log_tail() {
    // The outer phase guard can expire before the inner readiness guard. Exercise both paths
    // with a paused clock so neither diagnostic can silently lose the module's evidence.
    for outer_phase in [false, true] {
        let mut rig = diagnostic_rig(&format!("warming-timeout-{outer_phase}"), true);
        write_startup_logs(&rig.root);
        let start = tokio::time::Instant::now();
        let result = capture_panic(async {
            let open = rig_management_open(
                &rig.trace,
                || async { running_module_report() },
                || async {
                    // Read fresh evidence, not just the log that existed before the first probe.
                    std::fs::write(
                        rig.root.join("data-home/cortexkit/broca/logs/latest.log"),
                        "late boot evidence\n",
                    )
                    .unwrap();
                    Err::<(), _>(coded_refusal("module_warming"))
                },
            );
            if outer_phase {
                rig.trace.phase("restart", open).await;
            } else {
                rig.script.observed.lock().unwrap().phase = "restart".into();
                open.await;
            }
        })
        .await;
        assert_eq!(start.elapsed(), broca_test_support::EVENT_HANG_GUARD);
        assert!(result.contains("timed out"), "{result}");
        for expected in [
            "phase=restart",
            "root=format-2",
            "last module state:",
            "module_warming",
            "broca boot final marker",
            "late boot evidence",
            "broca startup stderr",
        ] {
            assert!(result.contains(expected), "missing {expected:?}: {result}");
        }
        assert!(
            result.contains(&format!("state_root={}", rig.state.display())),
            "{result}"
        );
        assert!(
            result.contains(
                &rig.root
                    .join("data-home/cortexkit/broca/logs/broca.log")
                    .display()
                    .to_string()
            ),
            "{result}"
        );
        assert!(
            !result.contains("old boot header"),
            "the report must be a tail: {result}"
        );
        rig.completed = true;
    }
}

fn coded_refusal(code: &str) -> broca_subc::ConnError {
    let body = serde_json::json!({
        "code": code,
        "message": "module_id 'broca' is supervised but not available \
                    (state=running, enabled=true, live=false)",
    });
    broca_subc::ConnError::Rejected(
        broca_subc::connection::RejectedMessage::from_error_frame_body_for_test(
            &serde_json::to_vec(&body).unwrap(),
        ),
    )
}

fn write_startup_logs(root: &Path) {
    let logs = root.join("data-home/cortexkit/broca/logs");
    std::fs::create_dir_all(&logs).unwrap();
    let mut text = String::from("old boot header\n");
    for line in 1..=210 {
        text.push_str(&format!("broca startup line {line}\n"));
    }
    text.push_str("broca boot final marker\n");
    std::fs::write(logs.join("broca.log"), text).unwrap();
    let run_logs = root.join("data-home/cortexkit/run/logs");
    std::fs::create_dir_all(&run_logs).unwrap();
    std::fs::write(run_logs.join("broca.stderr.log"), "broca startup stderr\n").unwrap();
}

async fn capture_panic(work: impl Future<Output = ()>) -> String {
    use futures::FutureExt;
    let panic = std::panic::AssertUnwindSafe(work)
        .catch_unwind()
        .await
        .unwrap_err();
    panic
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| panic.downcast_ref::<&str>().map(|s| (*s).to_owned()))
        .expect("panic message must be text")
}

fn running_module_report() -> Value {
    json!({"supervisor.list": {"modules": [{
        "module_id": "broca", "state": "running", "enabled": true, "live": false,
        "spawn_generation": 1,
    }]}})
}

#[tokio::test(start_paused = true)]
async fn warming_guard_retries_until_the_management_route_opens() {
    let mut rig = diagnostic_rig("warming-ready", true);
    let attempts = std::cell::Cell::new(0);
    let probes = std::cell::Cell::new(0);
    let start = tokio::time::Instant::now();
    let opened = rig_management_open(
        &rig.trace,
        || {
            let mut report = running_module_report();
            if probes.get() == 0 {
                report["supervisor.list"]["modules"][0]["state"] = json!("starting");
                report["supervisor.list"]["modules"][0]["spawn_generation"] = json!(0);
            }
            probes.set(probes.get() + 1);
            std::future::ready(report)
        },
        || {
            attempts.set(attempts.get() + 1);
            std::future::ready(if attempts.get() == 3 {
                Ok("management route")
            } else {
                Err(coded_refusal("module_warming"))
            })
        },
    )
    .await;
    assert_eq!(opened, "management route");
    assert_eq!(attempts.get(), 3);
    assert_eq!(probes.get(), 3);
    assert!(start.elapsed() < broca_test_support::EVENT_HANG_GUARD);
    rig.completed = true;
}

#[tokio::test(start_paused = true)]
async fn warming_guard_fails_immediately_for_nonwarming_errors() {
    let mut rig = diagnostic_rig("warming-terminal", false);
    for error in [
        coded_refusal("permission_denied"),
        coded_refusal("module_reloading"),
        broca_subc::ConnError::Rejected("module_warming in plain text".into()),
        broca_subc::ConnError::Transport("connection lost".into()),
    ] {
        let expected = error.to_string();
        let error = Mutex::new(Some(error));
        let attempts = std::cell::Cell::new(0);
        let start = tokio::time::Instant::now();
        let report = capture_panic(async {
            rig_management_open(
                &rig.trace,
                || async { running_module_report() },
                || {
                    attempts.set(attempts.get() + 1);
                    std::future::ready(Err::<(), _>(
                        error
                            .lock()
                            .unwrap()
                            .take()
                            .expect("must not retry terminal errors"),
                    ))
                },
            )
            .await;
        })
        .await;
        assert!(report.contains(&expected), "{report}");
        assert_eq!(attempts.get(), 1);
        assert_eq!(start.elapsed(), std::time::Duration::ZERO);
    }
    rig.completed = true;
}

#[tokio::test(start_paused = true)]
async fn warming_guard_fails_immediately_when_broca_exits() {
    let mut rig = diagnostic_rig("warming-exited", true);
    write_startup_logs(&rig.root);
    let probes = std::cell::Cell::new(0);
    let opens = std::cell::Cell::new(0);
    let start = tokio::time::Instant::now();
    let report = capture_panic(async {
        rig_management_open(
            &rig.trace,
            || {
                probes.set(probes.get() + 1);
                let mut state = running_module_report();
                if probes.get() == 2 {
                    state["supervisor.list"]["modules"][0]["last_exit_code"] = json!(1);
                }
                std::future::ready(state)
            },
            || {
                opens.set(opens.get() + 1);
                std::future::ready(Err::<(), _>(coded_refusal("module_warming")))
            },
        )
        .await;
    })
    .await;
    assert_eq!(probes.get(), 2);
    assert_eq!(
        opens.get(),
        1,
        "a dead broca must not get another open attempt"
    );
    assert!(start.elapsed() < broca_test_support::EVENT_HANG_GUARD);
    for expected in [
        "broca process exited",
        "\"last_exit_code\":1",
        "broca boot final marker",
    ] {
        assert!(report.contains(expected), "{report}");
    }
    // Unrelated module exits do not condemn broca, while a failed spawn and a signal exit do.
    assert!(!broca_has_exited(&json!({"supervisor.list": {"modules": [
        {"module_id":"other", "state":"failed", "last_exit_code":1}
    ]}})));
    for state in [
        json!({"module_id":"broca", "state":"failed", "spawn_generation":0}),
        json!({"module_id":"broca", "state":"stopped", "spawn_generation":1}),
        json!({"module_id":"broca", "state":"running", "last_exit_signal":9}),
    ] {
        assert!(broca_has_exited(
            &json!({"supervisor.list": {"modules":[state]}})
        ));
    }
    rig.completed = true;
}

#[tokio::test]
async fn differential_phase_diagnostic_names_root_and_observed_state() {
    let mut rig = diagnostic_rig("differential-observed", true);
    let mut bytes = broca_wal::framing::encode(
        7,
        1,
        &broca_wal::WalRecord::StepStarted {
            step_id: broca_protocol::StepId(1),
        },
    )
    .unwrap();
    bytes.extend_from_slice(b"torn suffix");
    std::fs::write(rig.state.join("wal/observed.wal"), bytes).unwrap();
    rig.script.observed.lock().unwrap().run_state = Some(json!({"state":"paused"}));
    rig.trace.event(&json!({"unit":{"type":"run_paused"}}));
    rig.trace.waiting("auth pause");
    let diagnostic = rig.trace.snapshot();
    for expected in [
        "root=format-2",
        "waiting=auth pause",
        "last_record=step_started@7",
        "incomplete_tail=true",
        "run_state={\"state\":\"paused\"}",
        "run_paused",
    ] {
        assert!(
            diagnostic.contains(expected),
            "missing {expected}: {diagnostic}"
        );
    }
    assert!(diagnostic.contains(&rig.root.display().to_string()));
    assert_eq!(rig.trace.phase("helper completion", async { 42 }).await, 42);
    rig.completed = true;
}

#[tokio::test]
async fn differential_source_oracle_accepts_repeated_lineage_after_reopen() {
    use broca_wal::{FileWal, Wal, WalConfig, WalRecord};

    let mut legacy = diagnostic_rig("differential-lineage-legacy", false);
    let mut overhaul = diagnostic_rig("differential-lineage-format-2", true);
    let mut decoded = vec![];
    for rig in [&mut legacy, &mut overhaul] {
        let config = if rig.overhaul {
            std::fs::write(rig.state.join("state-format"), b"broca-state/2\n").unwrap();
            WalConfig::OVERHAUL
        } else {
            WalConfig::LEGACY
        };
        let path = rig.state.join("wal/source.wal");
        let floor = rig.state.join(broca_wal::floor::FEATURE_FLOOR_FILENAME);
        let open = || {
            FileWal::open_with_recovery(&path, config, floor.clone())
                .unwrap()
                .with_feature_floor(floor.clone())
                .with_wal_config(config)
                .unwrap()
        };
        let mut wal = open();
        let previous_writer = wal.writer_id();
        if let Some(writer_id) = previous_writer {
            wal.append(
                1,
                &WalRecord::WriterOpened {
                    writer_id,
                    previous_writer_id: None,
                    recovered_through_seq: 0,
                    cause: "open".into(),
                },
                true,
            )
            .unwrap();
        }
        wal.append(
            1,
            &WalRecord::StepStarted {
                step_id: broca_protocol::StepId(1),
            },
            true,
        )
        .unwrap();
        if let Some(writer_id) = previous_writer {
            wal.append(1, &WalRecord::WriterClosed { writer_id }, true)
                .unwrap();
        }
        let lineage = wal.lineage_id().unwrap();
        assert!(lineage.incarnation().is_some());
        let recovered_through_seq = wal.last_seq();
        drop(wal);
        let mut wal = open();
        assert_eq!(wal.lineage_id(), Some(lineage));
        if let Some(writer_id) = wal.writer_id() {
            wal.append(
                1,
                &WalRecord::WriterOpened {
                    writer_id,
                    previous_writer_id: previous_writer,
                    recovered_through_seq,
                    cause: "reopen".into(),
                },
                true,
            )
            .unwrap();
        }
        wal.append(
            1,
            &WalRecord::StepStarted {
                step_id: broca_protocol::StepId(2),
            },
            true,
        )
        .unwrap();
        drop(wal);

        let bytes = std::fs::read(&path).unwrap();
        let header = &bytes[..broca_wal::framing::LINEAGE_FRAME_LEN];
        assert!(
            bytes
                .windows(header.len())
                .filter(|window| *window == header)
                .count()
                >= 3,
            "the production writer must supply recurring lineage metadata"
        );
        let sources = Sources::read(rig);
        assert_eq!(sources.lineage, lineage.to_string());
        let expected_sequences = if rig.overhaul {
            vec![1, 2, 3, 4, 5]
        } else {
            vec![1, 2]
        };
        assert_eq!(
            sources
                .records
                .iter()
                .map(|source| source.seq)
                .collect::<Vec<_>>(),
            expected_sequences
        );
        assert_eq!(
            sources.logical.len(),
            2,
            "headers and writer markers are not logical records"
        );
        assert_eq!(sources.position(if rig.overhaul { 2 } else { 1 }), 1);
        assert_eq!(sources.position(if rig.overhaul { 5 } else { 2 }), 2);
        decoded.push(sources);
    }
    correspondence(&decoded[0], &decoded[1]);
    legacy.completed = true;
    overhaul.completed = true;
}

fn source_lineage_header(tag: &str) -> Vec<u8> {
    use broca_wal::Wal;
    let root = broca_test_support::scratch_root().join(tag);
    let path = root.join("source.wal");
    let mut wal = broca_wal::FileWal::open(&path).unwrap();
    wal.append(
        1,
        &broca_wal::WalRecord::StepStarted {
            step_id: broca_protocol::StepId(1),
        },
        true,
    )
    .unwrap();
    drop(wal);
    let bytes = std::fs::read(&path).unwrap();
    let header = bytes[..broca_wal::framing::LINEAGE_FRAME_LEN].to_vec();
    broca_test_support::remove_rig_dir(root);
    header
}

fn source_header_without_incarnation(header: &[u8]) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    let mut header = header.to_vec();
    // Older lineage identities are opaque bytes, not necessarily UUID-v4 incarnations.
    header[broca_wal::framing::HEADER_LEN + 6] &= 0x0f;
    let digest_start = broca_wal::framing::HEADER_LEN - 32;
    let mut digest = Sha256::new();
    digest.update(&header[4..digest_start]);
    digest.update(&header[broca_wal::framing::HEADER_LEN..]);
    header[digest_start..broca_wal::framing::HEADER_LEN].copy_from_slice(&digest.finalize());
    header
}

#[test]
fn differential_source_oracle_rejects_changed_lineage_and_sequence_gaps() {
    let header = source_lineage_header("differential-lineage-original");
    let foreign = source_lineage_header("differential-lineage-foreign");
    assert_ne!(header, foreign);
    let no_incarnation = source_header_without_incarnation(&header);
    let record = |seq| {
        broca_wal::framing::encode(
            seq,
            1,
            &broca_wal::WalRecord::StepStarted {
                step_id: broca_protocol::StepId(seq),
            },
        )
        .unwrap()
    };
    let first = record(1);
    let second = record(2);
    for (name, bytes) in [
        (
            "foreign lineage",
            [&header[..], &first, &foreign, &header, &second].concat(),
        ),
        (
            "changed incarnation",
            [&header[..], &first, &no_incarnation, &second].concat(),
        ),
        (
            "sequence gap",
            [&header[..], &first, &header, &record(3)].concat(),
        ),
        (
            "duplicate sequence",
            [&header[..], &first, &header, &first].concat(),
        ),
        ("late first header", [&first[..], &header].concat()),
    ] {
        assert!(
            std::panic::catch_unwind(|| decode_sources(&bytes)).is_err(),
            "accepted {name}"
        );
    }
}

#[test]
fn differential_source_oracle_preserves_optional_initial_incarnation() {
    let header =
        source_header_without_incarnation(&source_lineage_header("differential-lineage-opaque"));
    let lineage = match decode_next(&header) {
        DecodeStep::Lineage { lineage_id, .. } => lineage_id,
        _ => panic!("fixture must contain valid lineage metadata"),
    };
    assert!(lineage.incarnation().is_none());
    let first = broca_wal::framing::encode(
        1,
        1,
        &broca_wal::WalRecord::StepStarted {
            step_id: broca_protocol::StepId(1),
        },
    )
    .unwrap();
    let second = broca_wal::framing::encode(
        2,
        1,
        &broca_wal::WalRecord::StepStarted {
            step_id: broca_protocol::StepId(2),
        },
    )
    .unwrap();
    let bytes = [&header[..], &header, &first, &header, &second, &header].concat();
    let (actual_lineage, records) = decode_sources(&bytes);
    assert_eq!(actual_lineage, lineage.to_string());
    assert_eq!(
        records.iter().map(|record| record.seq).collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(records[0].end as usize, header.len() * 2 + first.len());
    assert_eq!(
        records[1].end as usize,
        header.len() * 3 + first.len() + second.len()
    );
}

#[test]
fn differential_billing_records_are_checked_but_not_corresponded() {
    let legacy = billing_fixture(false);
    let overhaul = billing_fixture(true);
    for fixture in [&legacy, &overhaul] {
        assert_billing_attempts(
            &fixture.sources.lineage,
            fixture.sources.overhaul,
            &fixture.sources.records,
            &fixture.requests,
            &fixture.receipts,
        );
    }
    correspondence(&legacy.sources, &overhaul.sources);
    assert_eq!(legacy.sources.position(2), overhaul.sources.position(4));
    assert!(!overhaul.sources.logical.contains_key(&3));
}

struct BillingFixture {
    sources: Sources,
    requests: Vec<Vec<u8>>,
    receipts: Vec<Result<Option<ReservationReceipt>, String>>,
}

fn billing_fixture(overhaul: bool) -> BillingFixture {
    use sha2::{Digest, Sha256};
    let body = br#"{"model":"differential-model"}"#.to_vec();
    let id = "a".repeat(32);
    let lineage = format!("lineage-{overhaul}");
    let reservation = json!({"run_id":"run-a","step_id":1,"attempt":0,"send_index":1,
        "warm":false,"reserved_at_ms":1,"provider":"xai","endpoint_host":"127.0.0.1",
        "model_sent":"differential-model","credential":{"credential_ref":"apikey:xai","account_ref":"acct-fixture"},
        "session":{"project_root":"/project","harness":"differential","session":"session","session_ref":"ref"},
        "run":{},"request_body_bytes":body.len(),"compressed":false});
    let mut terminal = json!({"type":"model_attempt_finished","step_id":1,"attempt":0,
        "request_body_len":body.len(),"request_body_sha256":format!("{:x}", Sha256::digest(&body)),
        "http_status":200,"response_body_len":3,"sent_at_ms":2,"ts_ms":3});
    let mut bodies = vec![];
    if overhaul {
        bodies.push(json!({"type":"writer_opened"}));
    }
    bodies.push(json!({"type":"run_started","run_id":"run-a"}));
    let receipt = if overhaul {
        bodies.push(json!({"type":"attempt_reserved","attempt_id":id,"reservation":reservation}));
        terminal["attempt_id"] = json!(id);
        terminal["reservation"] = reservation.clone();
        terminal["envelope"] = json!({"sent":true,"http_status":200,"response_bytes":3,"sent_at_ms":2,"finished_at_ms":3});
        Some(ReservationReceipt {
            lineage: lineage.clone(),
            attempt_id: id,
            seq: 3,
            end: 300,
            durable_seq: 3,
            durable_end: 300,
            reservation,
        })
    } else {
        None
    };
    bodies.extend([terminal, json!({"type":"run_finished"})]);
    let records: Vec<_> = bodies
        .into_iter()
        .enumerate()
        .map(|(index, record)| Source {
            seq: index as u64 + 1,
            end: (index as u64 + 1) * 100,
            record,
        })
        .collect();
    let logical = records
        .iter()
        .filter(|source| comparable_source(&source.record))
        .enumerate()
        .map(|(index, source)| (source.seq, index + 1))
        .collect();
    BillingFixture {
        sources: Sources {
            lineage,
            records,
            logical,
            root: "/state".into(),
            runs: vec!["run-a".into()],
            file_id: 7,
            projected: BTreeSet::new(),
            overhaul,
        },
        requests: vec![body],
        receipts: vec![Ok(receipt)],
    }
}

#[test]
fn differential_billing_ledger_rejects_unmatched_or_undurable_attempts() {
    for kind in [
        "attempt_reserved",
        "attempt_released",
        "attempt_finished",
        "attempt_corrected",
    ] {
        assert!(billing_only(&json!({"type":kind})));
    }
    for kind in [
        "model_attempt_finished",
        "cache_warmed",
        "run_started",
        "attempt_future",
    ] {
        assert!(!billing_only(&json!({"type":kind})), "must not hide {kind}");
    }
    for fault in [
        "missing reservation",
        "wrong lineage",
        "wrong order",
        "unpublished sequence",
        "unpublished bytes",
        "late proof",
        "wrong terminal id",
        "changed reservation",
        "wrong envelope",
        "orphan settlement",
        "duplicate settlement",
        "unexpected correction",
    ] {
        let mut fixture = billing_fixture(true);
        match fault {
            "missing reservation" => {
                fixture.sources.records.remove(2);
            }
            "wrong lineage" => {
                fixture.receipts[0]
                    .as_mut()
                    .unwrap()
                    .as_mut()
                    .unwrap()
                    .lineage = "foreign".into()
            }
            "wrong order" => {
                fixture.receipts[0]
                    .as_mut()
                    .unwrap()
                    .as_mut()
                    .unwrap()
                    .attempt_id = "b".repeat(32)
            }
            "unpublished sequence" => {
                fixture.receipts[0]
                    .as_mut()
                    .unwrap()
                    .as_mut()
                    .unwrap()
                    .durable_seq = 2
            }
            "unpublished bytes" => {
                fixture.receipts[0]
                    .as_mut()
                    .unwrap()
                    .as_mut()
                    .unwrap()
                    .durable_end = 299
            }
            "late proof" => {
                fixture.receipts[0]
                    .as_mut()
                    .unwrap()
                    .as_mut()
                    .unwrap()
                    .durable_seq = 4
            }
            "wrong terminal id" => {
                fixture.sources.records[3].record["attempt_id"] = json!("b".repeat(32))
            }
            "changed reservation" => {
                fixture.sources.records[3].record["reservation"]["step_id"] = json!(2)
            }
            "wrong envelope" => {
                fixture.sources.records[3].record["envelope"]["http_status"] = json!(401)
            }
            "orphan settlement" | "duplicate settlement" | "unexpected correction" => {
                let mut record = fixture.sources.records[3].record.clone();
                record["type"] = json!(if fault == "unexpected correction" {
                    "attempt_corrected"
                } else {
                    "attempt_finished"
                });
                record["correction_id"] = json!("c".repeat(32));
                if fault == "orphan settlement" {
                    record["attempt_id"] = json!("b".repeat(32));
                }
                fixture.sources.records.push(Source {
                    seq: 6,
                    end: 600,
                    record,
                });
            }
            _ => unreachable!(),
        }
        assert!(
            std::panic::catch_unwind(|| assert_billing_attempts(
                &fixture.sources.lineage,
                true,
                &fixture.sources.records,
                &fixture.requests,
                &fixture.receipts
            ))
            .is_err(),
            "accepted {fault}"
        );
    }
    let mut legacy = billing_fixture(false);
    let mut overhaul = billing_fixture(true);
    legacy.sources.records.push(Source {
        seq: 4,
        end: 400,
        record: overhaul.sources.records[2].record.clone(),
    });
    assert!(std::panic::catch_unwind(|| assert_billing_attempts(
        &legacy.sources.lineage,
        false,
        &legacy.sources.records,
        &legacy.requests,
        &legacy.receipts
    ))
    .is_err());
    let legacy = billing_fixture(false);
    overhaul.sources.records.push(Source {
        seq: 6,
        end: 600,
        record: json!({"type":"attempt_future"}),
    });
    assert!(
        std::panic::catch_unwind(|| correspondence(&legacy.sources, &overhaul.sources)).is_err()
    );
}

#[tokio::test]
async fn differential_billing_receipt_requires_a_published_reservation() {
    use broca_wal::head_proof::{PublicationStatus, Publisher, SyncedPrefix};
    use broca_wal::{FileWal, Wal, WalConfig, WalRecord};
    let mut rig = diagnostic_rig("differential-reservation-proof", true);
    std::fs::write(rig.state.join("state-format"), b"broca-state/2\n").unwrap();
    let path = rig.state.join("wal/source.wal");
    let floor = rig.state.join(broca_wal::floor::FEATURE_FLOOR_FILENAME);
    let mut wal = FileWal::open_with_recovery(&path, WalConfig::OVERHAUL, floor.clone())
        .unwrap()
        .with_feature_floor(floor)
        .with_wal_config(WalConfig::OVERHAUL)
        .unwrap();
    wal.append(
        1,
        &WalRecord::StepStarted {
            step_id: broca_protocol::StepId(1),
        },
        true,
    )
    .unwrap();
    let mut publisher = Publisher::from_synced(SyncedPrefix::sync(&wal).unwrap()).unwrap();
    assert_eq!(
        publisher
            .publish(broca_wal::framing::appended_ms_now)
            .unwrap(),
        PublicationStatus::Published
    );
    let fixture = billing_fixture(true);
    let reserved: WalRecord =
        serde_json::from_value(fixture.sources.records[2].record.clone()).unwrap();
    let seq = wal.append(1, &reserved, true).unwrap();
    assert!(
        capture_reservation(&rig.state, 0, &fixture.requests[0]).is_err(),
        "a stale proof must not cover a later reservation"
    );
    publisher.sync(&wal).unwrap();
    assert_eq!(
        publisher
            .publish(broca_wal::framing::appended_ms_now)
            .unwrap(),
        PublicationStatus::Published
    );
    let receipt = capture_reservation(&rig.state, 0, &fixture.requests[0]).unwrap();
    assert_eq!(receipt.seq, seq);
    assert_eq!(receipt.attempt_id, "a".repeat(32));
    assert!(receipt.durable_seq >= seq && receipt.durable_end >= receipt.end);
    assert!(capture_reservation(&rig.state, 1, &fixture.requests[0]).is_err());
    assert!(capture_reservation(&rig.state, 0, b"different body").is_err());
    let settled: WalRecord = serde_json::from_value(
        json!({"type":"attempt_finished", "attempt_id":"a".repeat(32),
        "reservation":fixture.sources.records[2].record["reservation"],
        "envelope":{"sent":"unknown","outcome":"unknown","response_bytes":0}}),
    )
    .unwrap();
    wal.append(1, &settled, true).unwrap();
    publisher.sync(&wal).unwrap();
    assert_eq!(
        publisher
            .publish(broca_wal::framing::appended_ms_now)
            .unwrap(),
        PublicationStatus::Published
    );
    assert!(
        capture_reservation(&rig.state, 0, &fixture.requests[0]).is_err(),
        "an already-settled attempt cannot authorize this send"
    );
    rig.completed = true;
}

#[test]
fn differential_api_key_attempts_use_wal_without_routing_reports() {
    for overhaul in [false, true] {
        let mut sources = Sources {
            lineage: "own-lineage".into(),
            root: "/state".into(),
            runs: vec!["run-a".into()],
            file_id: 7,
            projected: BTreeSet::new(),
            logical: BTreeMap::new(),
            overhaul,
            records: vec![Source {
                seq: 1,
                end: 100,
                record: json!({"type":"run_started","config":{"auth_selection":{
                    "credential_id":"apikey:xai","account_ref":"acct-fixture"
                }}}),
            }],
        };
        for (index, status) in [200, 200, 401].into_iter().enumerate() {
            sources.records.push(Source {
                seq: index as u64 + 2,
                end: 200 + index as u64 * 100,
                record: json!({"type":"model_attempt_finished","http_status":status}),
            });
        }
        sources.assert_api_key_attempts(&[], &[200, 200, 401]);
        assert!(std::panic::catch_unwind(
            || sources.assert_api_key_attempts(&[json!({"kind":"attempt"})], &[200, 200, 401])
        )
        .is_err());
        assert!(
            std::panic::catch_unwind(|| sources.assert_api_key_attempts(&[], &[200, 200])).is_err()
        );
        sources.records[0].record["config"]["auth_selection"]["account_decision"] =
            json!({"admission_id":"run-a"});
        assert!(
            std::panic::catch_unwind(|| sources.assert_api_key_attempts(&[], &[200, 200, 401]))
                .is_err()
        );
    }
}

#[test]
fn differential_subscribe_lanes_allow_only_cross_lane_reordering() {
    let (sources, frames) = subscribe_lane_fixture();
    for method in [
        "session.subscribe",
        "session.subscribe.handoff",
        "session.subscribe.replay",
        "session.subscribe.live",
    ] {
        let legacy = subscribe_reply(method, &frames, &[0, 2, 1, 3, 4]);
        let overhaul = subscribe_reply(method, &frames, &[0, 1, 2, 3, 4]);
        assert_eq!(
            sources.normalize(&legacy),
            sources.normalize(&overhaul),
            "{method}"
        );
        let display_first = subscribe_reply(method, &frames, &[2, 0, 1, 3, 4]);
        assert_eq!(
            sources.normalize(&legacy),
            sources.normalize(&display_first),
            "live fan-out does not promise run_started before display"
        );
    }
}

fn subscribe_lane_fixture() -> (Sources, Vec<Value>) {
    use broca_protocol::{caller::ControlUnit, display::DisplayEvent, RunId, StepId};
    use cortexkit_role_llm_runner::live::{LiveEvent, RunEnded, RunStarted};
    let control = |seq, unit, event| {
        serde_json::from_slice::<Value>(
            &broca_wire::encode_live_subscribe_control(seq, 0, &unit, 1, &event).unwrap(),
        )
        .unwrap()
    };
    let display = |delta: &str| {
        serde_json::from_slice::<Value>(
            &broca_wire::encode_live_subscribe_display(
                &DisplayEvent::TextDelta {
                    mid: Some("m1".into()),
                    part: Some(0),
                    message_id: broca_protocol::MessageId("provider-message".into()),
                    tool_call_id: None,
                    delta: delta.into(),
                },
                1,
            )
            .unwrap(),
        )
        .unwrap()
    };
    let frames = vec![
        control(
            1,
            ControlUnit::RunStarted {
                run_id: RunId("run-a".into()),
                submission_id: None,
            },
            LiveEvent::RunStarted(RunStarted::new("run-a".to_owned())),
        ),
        control(
            2,
            ControlUnit::StepStarted { step_id: StepId(1) },
            LiveEvent::DisplayReset,
        ),
        display("first"),
        display("second"),
        control(
            3,
            ControlUnit::RunFinished {
                run_id: Some(RunId("run-a".into())),
                reason: broca_protocol::RunFinishReason::Completed,
                usage: None,
                final_step_finish_reason: None,
                indeterminate_tool_calls: None,
                retries_used: 0,
            },
            LiveEvent::RunFinished(RunEnded::new("run-a".to_owned(), "completed".to_owned())),
        ),
    ];
    let records: Vec<_> = [
        json!({"type":"run_started","run_id":"run-a"}),
        json!({"type":"step_started","step_id":1}),
        json!({"type":"run_finished"}),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, record)| Source {
        seq: index as u64 + 1,
        end: (index as u64 + 1) * 100,
        record,
    })
    .collect();
    (
        Sources {
            lineage: "lane-lineage".into(),
            logical: records
                .iter()
                .enumerate()
                .map(|(index, source)| (source.seq, index + 1))
                .collect(),
            records,
            root: "/state".into(),
            runs: vec!["run-a".into()],
            file_id: 7,
            projected: BTreeSet::from([(1, 0), (2, 0), (3, 0)]),
            overhaul: false,
        },
        frames,
    )
}

fn subscribe_reply(method: &str, frames: &[Value], order: &[usize]) -> Value {
    let events: Vec<_> = order
        .iter()
        .enumerate()
        .map(|(index, &frame)| {
            let mut frame = frames[frame].clone();
            frame["seq"] = json!(cortexkit_role_llm_runner::live::FIRST_SEQ + index as u64);
            frame
        })
        .collect();
    json!({"method":method,"events":events})
}

#[test]
fn differential_subscribe_lane_contents_and_contract_remain_exact() {
    let (sources, frames) = subscribe_lane_fixture();
    let baseline = sources.normalize(&subscribe_reply(
        "session.subscribe.handoff",
        &frames,
        &[0, 1, 2, 3, 4],
    ));
    for fault in [
        "display content",
        "control unit",
        "control event",
        "display metadata",
    ] {
        let mut changed = frames.clone();
        match fault {
            "display content" => changed[2]["event"]["delta"] = json!("changed"),
            "control unit" => changed[1]["unit"]["step_id"] = json!(2),
            "control event" => changed[1]["event"] = json!({"type":"other-event"}),
            "display metadata" => changed[2]["metadata"] = json!("must survive"),
            _ => unreachable!(),
        }
        assert_ne!(
            baseline,
            sources.normalize(&subscribe_reply(
                "session.subscribe.handoff",
                &changed,
                &[0, 1, 2, 3, 4]
            )),
            "masked {fault}"
        );
    }
    let mut original = frames.clone();
    original[2]["event"]["seq"] = json!(1);
    let mut changed = original.clone();
    changed[2]["event"]["seq"] = json!(99);
    assert_ne!(
        sources.normalize(&subscribe_reply(
            "session.subscribe",
            &original,
            &[0, 1, 2, 3, 4]
        )),
        sources.normalize(&subscribe_reply(
            "session.subscribe",
            &changed,
            &[0, 1, 2, 3, 4]
        )),
        "nested seq is payload, not a merged ordinal"
    );
    for order in [vec![0, 1, 3, 2, 4], vec![0, 1, 3, 4], vec![0, 2, 3, 4]] {
        assert_ne!(
            baseline["events"],
            sources.normalize(&subscribe_reply(
                "session.subscribe.replay",
                &frames,
                &order
            ))["events"],
            "lane order and frame presence must remain exact"
        );
    }
    let left = subscribe_reply("session.read", &frames, &[0, 2, 1, 3, 4]);
    let right = subscribe_reply("session.read", &frames, &[0, 1, 2, 3, 4]);
    assert_ne!(
        sources.normalize(&left),
        sources.normalize(&right),
        "only subscribe collections are split"
    );
    for fault in [
        "missing seq",
        "seq gap",
        "missing lane",
        "unknown lane",
        "missing cursor",
        "unknown cursor",
    ] {
        let mut reply = subscribe_reply("session.subscribe.live", &frames, &[0, 1, 2, 3, 4]);
        match fault {
            "missing seq" => {
                reply["events"][1].as_object_mut().unwrap().remove("seq");
            }
            "seq gap" => reply["events"][1]["seq"] = json!(99),
            "missing lane" => {
                reply["events"][1].as_object_mut().unwrap().remove("kind");
            }
            "unknown lane" => reply["events"][1]["kind"] = json!("unknown"),
            "missing cursor" => {
                reply["events"][1].as_object_mut().unwrap().remove("cursor");
            }
            "unknown cursor" => reply["events"][1]["cursor"]["wal_seq"] = json!(99),
            _ => unreachable!(),
        }
        assert!(
            std::panic::catch_unwind(|| sources.normalize(&reply)).is_err(),
            "accepted {fault}"
        );
    }
    for order in [vec![1, 0, 2, 3, 4], vec![0, 1, 1, 2, 3, 4]] {
        let reply = subscribe_reply("session.subscribe.replay", &frames, &order);
        assert!(
            std::panic::catch_unwind(|| sources.normalize(&reply)).is_err(),
            "control cursors must not reorder or repeat"
        );
    }
}

#[test]
fn differential_generation_durations_are_scoped_to_usage_in_all_replies() {
    assert_eq!(
        GENERATION_DURATION_FIELDS,
        ["sent_to_last_content_ms", "first_to_last_content_ms"]
    );
    fields(
        &generation_entry(0, 0),
        &[
            "model",
            "service_tier",
            "speed",
            "model_calls",
            "output_tokens",
            "reasoning_tokens",
            "sent_to_last_content_ms",
            "first_to_last_content_ms",
        ],
    );
    let sources = generation_sources(false);
    let left = generation_replies(&sources, generation_entry(1, 0));
    let right = generation_replies(&sources, generation_entry(4, 2));
    for (legacy, overhaul) in left.iter().zip(&right) {
        assert_eq!(
            sources.normalize(legacy),
            sources.normalize(overhaul),
            "{}",
            legacy["method"]
        );
    }
}

fn generation_entry(sent_to_last: u64, first_to_last: u64) -> Value {
    serde_json::to_value(broca_protocol::usage::GenerationUsage {
        model: "generation-model".into(),
        service_tier: Some("priority".into()),
        speed: Some("fast".into()),
        model_calls: 2,
        output_tokens: 6,
        reasoning_tokens: Some(0),
        sent_to_last_content_ms: sent_to_last,
        first_to_last_content_ms: first_to_last,
    })
    .unwrap()
}

fn generation_sources(overhaul: bool) -> Sources {
    let mut sources = billing_fixture(overhaul).sources;
    sources
        .projected
        .insert((sources.records.last().unwrap().seq, 0));
    sources
}

fn generation_replies(sources: &Sources, entry: Value) -> Vec<Value> {
    let usage = json!({"input_tokens":7,"output_tokens":6,"generation":[entry]});
    let mut replies: Vec<_> = [
        "run.status.paused",
        "run.result",
        "run.status",
        "run.status.tail",
    ]
    .into_iter()
    .map(|method| json!({"method":method,"reply":{"usage":usage}}))
    .collect();
    replies.push(json!({"method":"session.subscribe.handoff","events":[{
        "kind":"control","seq":cortexkit_role_llm_runner::live::FIRST_SEQ,
        "cursor":{"wal_seq":sources.records.last().unwrap().seq,"sub_index":0},
        "unit":{"type":"run_finished","run_id":"run-a","usage":usage},
        "event":{"type":"run_finished","run_id":"run-a","usage":usage}
    }]}));
    replies
}

#[test]
fn differential_generation_duration_fields_are_required_and_nonnegative() {
    for overhaul in [false, true] {
        let sources = generation_sources(overhaul);
        let normalize = |entry| sources.normalize(&generation_replies(&sources, entry)[0]);
        let zero = normalize(generation_entry(0, 0));
        for &field in GENERATION_DURATION_FIELDS {
            assert_eq!(
                zero["reply"]["usage"]["generation"][0][field],
                "@DURATION_MS@"
            );
            let mut missing = generation_entry(0, 0);
            missing.as_object_mut().unwrap().remove(field);
            assert!(
                std::panic::catch_unwind(|| normalize(missing.clone())).is_err(),
                "missing {field} must fail even if both formats omit it"
            );
            for invalid in [json!(-1), Value::Null, json!("unknown"), json!(0.5)] {
                let mut entry = generation_entry(0, 0);
                entry[field] = invalid;
                assert!(
                    std::panic::catch_unwind(|| normalize(entry.clone())).is_err(),
                    "accepted invalid {field}"
                );
            }
        }
        for generation in [Value::Null, json!({}), json!([null])] {
            assert!(std::panic::catch_unwind(
                || sources.normalize(&json!({"usage":{"generation":generation}}))
            )
            .is_err());
        }
    }
}

#[test]
fn differential_generation_metadata_and_entry_presence_remain_exact() {
    let sources = generation_sources(false);
    let normalize = |entry| sources.normalize(&generation_replies(&sources, entry)[0]);
    let original = normalize(generation_entry(1, 0));
    for (field, changed) in [
        ("model", json!("different-model")),
        ("service_tier", json!("standard")),
        ("speed", json!("slow")),
        ("model_calls", json!(3)),
        ("output_tokens", json!(9)),
        ("reasoning_tokens", json!(1)),
    ] {
        let mut entry = generation_entry(4, 2);
        entry[field] = changed;
        assert_ne!(original, normalize(entry), "masked semantic field {field}");
        let mut missing = generation_entry(4, 2);
        missing.as_object_mut().unwrap().remove(field);
        assert_ne!(
            original,
            normalize(missing),
            "lost semantic field presence {field}"
        );
    }
    let mut left = generation_entry(1, 0);
    let mut right = generation_entry(4, 2);
    left["sent_to_first_content_ms"] = json!(1);
    right["sent_to_first_content_ms"] = json!(4);
    assert_ne!(
        normalize(left),
        normalize(right),
        "an unlisted duration-like field must remain exact"
    );
    let left = json!({"generation":[generation_entry(1, 0)]});
    let right = json!({"generation":[generation_entry(4, 2)]});
    assert_ne!(
        sources.normalize(&left),
        sources.normalize(&right),
        "generation outside usage is not normalized"
    );
    let left = json!({"usage":{"generation":[generation_entry(1, 0)],"sent_to_last_content_ms":1}});
    let right =
        json!({"usage":{"generation":[generation_entry(4, 2)],"sent_to_last_content_ms":4}});
    assert_ne!(
        sources.normalize(&left),
        sources.normalize(&right),
        "duration outside the generation entry is not normalized"
    );
    let mut other = generation_entry(4, 2);
    other["model"] = json!("other-model");
    let left = json!({"usage":{"generation":[generation_entry(1, 0),other.clone()]}});
    let missing = json!({"usage":{"generation":[generation_entry(4, 2)]}});
    let reordered = json!({"usage":{"generation":[other,generation_entry(4, 2)]}});
    assert_ne!(
        sources.normalize(&left),
        sources.normalize(&missing),
        "entry count must remain exact"
    );
    assert_ne!(
        sources.normalize(&left),
        sources.normalize(&reordered),
        "entry order must remain exact"
    );
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

fn cursor_sources(overhaul: bool) -> Sources {
    let shift = u64::from(overhaul);
    let bodies = [
        json!({"type":"run_started","run_id":"run-a"}),
        json!({"type":"model_step_finished","assistant_message":{"content":[{"type":"tool_call"}]}}),
        json!({"type":"tool_batch_finished"}),
        json!({"type":"model_step_finished","assistant_message":{"content":[{"type":"tool_call"}]}}),
        json!({"type":"tool_batch_finished"}),
        json!({"type":"run_finished"}),
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

fn cursor_token(sources: &Sources) -> Value {
    let shift = u64::from(sources.overhaul);
    json!({"version":1,"run_id":"run-a","lineage_id":sources.lineage,
        "run_start_seq":1+shift,"run_end_seq":6+shift,"step_seq":4+shift,"call_index":0,"batch_end_seq":5+shift,"ordinal":3,
        "anchor_before":if sources.overhaul { Value::Null } else { json!({"file_id":7,"seq":3,"byte_end":300}) },
        "after_batch":[],"authentication":"0".repeat(64)})
}

#[test]
fn differential_cursor_validates_root_local_seek_hint_and_all_fields() {
    let left = cursor_sources(false);
    let right = cursor_sources(true);
    let token = cursor_token(&left);
    let other = cursor_token(&right);
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

#[test]
fn differential_cursor_end_is_owned_by_one_structural_episode() {
    for overhaul in [false, true] {
        let sources = cursor_sources(overhaul);
        let token = cursor_token(&sources);
        assert!(sources
            .records
            .last()
            .unwrap()
            .record
            .get("run_id")
            .is_none());
        sources.calls_cursor(&token.to_string());
        for fault in [
            "wrong start id",
            "wrong start kind",
            "end at start",
            "wrong end kind",
            "intervening start",
            "intervening finish",
        ] {
            let mut sources = cursor_sources(overhaul);
            let mut token = cursor_token(&sources);
            match fault {
                "wrong start id" => sources.records[0].record["run_id"] = json!("foreign-run"),
                "wrong start kind" => sources.records[0].record["type"] = json!("step_started"),
                "end at start" => token["run_end_seq"] = token["run_start_seq"].clone(),
                "wrong end kind" => sources.records[5].record["type"] = json!("step_started"),
                "intervening start" => {
                    sources.records[2].record = json!({"type":"run_started","run_id":"run-a"})
                }
                "intervening finish" => sources.records[2].record = json!({"type":"run_finished"}),
                _ => unreachable!(),
            }
            assert!(
                std::panic::catch_unwind(|| sources.calls_cursor(&token.to_string())).is_err(),
                "accepted {fault}"
            );
        }

        let mut history = cursor_sources(overhaul);
        let mut token = cursor_token(&history);
        let next_episode: Vec<_> = history
            .records
            .iter()
            .map(|source| Source {
                seq: source.seq + 6,
                end: source.end + 600,
                record: source.record.clone(),
            })
            .collect();
        history.records[0].record["run_id"] = json!("previous-run");
        history.records.extend(next_episode);
        history.runs = vec!["previous-run".into(), "run-a".into()];
        history.logical = history
            .records
            .iter()
            .enumerate()
            .map(|(index, source)| (source.seq, index + 1))
            .collect();
        for field in ["run_start_seq", "run_end_seq", "step_seq", "batch_end_seq"] {
            token[field] = json!(token[field].as_u64().unwrap() + 6);
        }
        if !overhaul {
            token["anchor_before"]["seq"] = json!(9);
            token["anchor_before"]["byte_end"] = json!(900);
        }
        let normalized = history.calls_cursor(&token.to_string());
        assert_eq!(normalized["run_start_seq"], 7);
        token["run_end_seq"] = json!(6 + u64::from(overhaul));
        assert!(
            std::panic::catch_unwind(|| history.calls_cursor(&token.to_string())).is_err(),
            "an earlier episode's RunFinished cannot close the current run"
        );
    }
}

#[test]
fn differential_wal_observation_infers_finished_run_from_start() {
    let mut current = Value::Null;
    let started = observed_run_state(
        &json!({"type":"run_started","run_id":"first-run"}),
        &mut current,
    )
    .unwrap();
    assert_eq!(started["run_id"], "first-run");
    let paused = observed_run_state(
        &json!({"type":"run_paused","run_id":"first-run","reason":"auth_required"}),
        &mut current,
    )
    .unwrap();
    assert_eq!(paused["run_id"], "first-run");
    let finish = json!({"type":"run_finished","reason":"completed"});
    assert!(finish.get("run_id").is_none());
    assert_eq!(
        observed_run_state(&finish, &mut current).unwrap()["run_id"],
        "first-run"
    );
    observed_run_state(
        &json!({"type":"run_started","run_id":"second-run"}),
        &mut current,
    )
    .unwrap();
    let finished = observed_run_state(&finish, &mut current).unwrap();
    assert_eq!(finished["run_id"], "second-run");
    assert_eq!(finished["state"], "finished");
}
