//! A SIGTERM to the real `ck-broca` while a run's tool call is running lets that call finish.
//!
//! The real daemon supervises the real `ck-broca` and the `fake-aft-stub` tool module; the model
//! is a local HTTP server replaying OpenAI chat SSE: a tool call first, then a text answer. The
//! tool holds its call for a few seconds, well inside the module's shutdown grace. broca is
//! sent SIGTERM while the stub holds the call. The run must then finish that call and record
//! its result `completed`, make no second model request, and end `Interrupted`, never
//! `Cancelled` and with nothing left `outcome_unknown`.
//!
//! Also exercises `ck module restart`: the channel-0 drain must start the boundary stop before
//! GOODBYE starts the final grace. Unix only: the signal cases and process census use Unix tools.
#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use broca_protocol::caller::RunFinishReason;
use broca_wal::{DispatchOutcome, WalRecord};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::time::{sleep, Instant};

/// The first model response: one call to the `slow` tool, carrying the delay the stub holds the
/// call for and a tag the test finds the call by in the stub's event log. The 3 s delay is long
/// enough that the SIGTERM certainly lands while the call runs, and far inside the module's
/// 10 s shutdown grace.
const TOOL_CALL_SSE: &str = concat!(
    "data: {\"id\":\"chatcmpl-stop\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"}}]}\n\n",
    "data: {\"id\":\"chatcmpl-stop\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_slow\",\"type\":\"function\",\"function\":{\"name\":\"slow\",\"arguments\":\"{\\\"delay_ms\\\":3000,\\\"tag\\\":\\\"graceful-stop\\\"}\"}}]}}]}\n\n",
    "data: {\"id\":\"chatcmpl-stop\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
    "data: [DONE]\n\n",
);

/// Any later model response: a plain answer. A run that ignored the stop would ask for it.
const TEXT_SSE: &str = concat!(
    "data: {\"id\":\"chatcmpl-done\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"done\"}}]}\n\n",
    "data: {\"id\":\"chatcmpl-done\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
    "data: [DONE]\n\n",
);

fn sibling_bin(name: &str, env_key: &str) -> PathBuf {
    broca_test_support::subc_bin(name, env_key)
}

/// A gated run exports `BROCA_REQUIRE_SUBC`, which turns a missing daemon binary from a loud
/// skip into a failure.
fn require_subc() -> bool {
    std::env::var_os("BROCA_REQUIRE_SUBC").is_some()
}

/// Serves `TOOL_CALL_SSE` to the first request and `TEXT_SSE` to every later one, counting them.
async fn serve_model(requests: Arc<AtomicUsize>) -> String {
    serve_recording_model(requests, Arc::new(std::sync::Mutex::new(Vec::new())), 3000).await
}

async fn serve_recording_model(
    requests: Arc<AtomicUsize>,
    bodies: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
    delay_ms: u64,
) -> String {
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind the model server");
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let requests = Arc::clone(&requests);
            let bodies = bodies.clone();
            tokio::spawn(async move {
                let mut reader = BufReader::new(stream);
                let mut content_length = 0usize;
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
                        return;
                    }
                    if line == "\r\n" || line == "\n" {
                        break;
                    }
                    if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        content_length = value.trim().parse().unwrap_or(0);
                    }
                }
                let mut body = vec![0u8; content_length];
                if reader.read_exact(&mut body).await.is_err() {
                    return;
                }
                bodies.lock().unwrap().push(body);
                let sse = if requests.fetch_add(1, Ordering::SeqCst) == 0 {
                    TOOL_CALL_SSE.replace("3000", &delay_ms.to_string())
                } else {
                    TEXT_SSE.to_owned()
                };
                let mut stream = reader.into_inner();
                let head = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    sse.len()
                );
                let _ = stream.write_all(head.as_bytes()).await;
                let _ = stream.write_all(sse.as_bytes()).await;
                let _ = stream.shutdown().await;
            });
        }
    });
    url
}

struct Rig {
    daemon: Child,
    dir: PathBuf,
    connection_file: PathBuf,
    events: PathBuf,
    ck_cli: PathBuf,
}

impl Drop for Rig {
    fn drop(&mut self) {
        broca_test_support::kill_and_reap(&mut self.daemon);
        broca_test_support::dump_rig_process_logs_on_failure(&self.dir, "daemon");
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl Rig {
    async fn start(model_url: &str) -> Option<Self> {
        let subc_source = sibling_bin("ck-subc", "SUBC_CORE_BIN");
        let fake_aft = sibling_bin("fake-aft-stub", "FAKE_AFT_STUB_BIN");
        for bin in [&subc_source, &fake_aft] {
            if !bin.exists() {
                assert!(
                    !require_subc(),
                    "graceful_stop_real_daemon: {bin:?} is missing, but BROCA_REQUIRE_SUBC \
                     requires it"
                );
                println!(
                    "SKIP graceful_stop_real_daemon: {bin:?} is missing. This reports `ok`; set \
                     BROCA_REQUIRE_SUBC=1 to make it a failure instead."
                );
                return None;
            }
        }
        let ck_source = subc_source.with_file_name(format!("ck{}", std::env::consts::EXE_SUFFIX));
        let (subc_core, ck_cli) = broca_test_support::ckdev_binary_with_companion(
            &subc_source,
            &ck_source,
            broca_test_support::scratch_root(),
        );
        std::env::remove_var("SUBC_MODULE_ID");
        std::env::remove_var("SUBC_LAUNCH_NONCE");

        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-graceful-stop-e2e-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let config_home = dir.join("config");
        let runtime_dir = dir.join("runtime");
        let config_path = config_home.join("cortexkit").join("subc.jsonc");
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::create_dir_all(&runtime_dir).unwrap();
        let events = dir.join("aft-events.jsonl");
        let config = serde_json::json!({
            "version": 1,
            "modules": {
                "broca": {
                    "program": broca_test_support::ckdev_binary(
                        env!("CARGO_BIN_EXE_ck-broca"),
                        broca_test_support::scratch_root(),
                    ),
                    "args": [],
                    "env": {
                        "BROCA_STATE_ROOT": runtime_dir.join("broca-state"),
                        "BROCA_PROVIDER_BASE_URL": model_url,
                        "BROCA_TOOL_MODULE": "aft"
                    },
                    "enabled": true,
                    "health": {"cadence_ms": 10000, "deadline_ms": 2000}
                },
                "aft": {
                    "program": fake_aft,
                    "args": [],
                    "env": {
                        "FAKE_AFT_MODULE_ID": "aft",
                        "FAKE_AFT_TOOLS": "slow",
                        "FAKE_AFT_TOOLCALL_RESULT": "SLOW-DONE",
                        "FAKE_AFT_EVENTS_PATH": events
                    },
                    "enabled": true
                }
            }
        });
        std::fs::write(&config_path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();
        let connection_file = runtime_dir.join("subc-connection.json");
        let daemon = broca_test_support::retry_executable_file_busy(|| {
            broca_test_support::tokio_rig_command(&subc_core, &dir, "daemon")?
                .env("XDG_CONFIG_HOME", &config_home)
                .env("XDG_RUNTIME_DIR", &runtime_dir)
                .env("XDG_DATA_HOME", runtime_dir.join("data-home"))
                .env_remove("BROCA_STATE_ROOT")
                .env("SUBC_PORT", "0")
                .kill_on_drop(true)
                .spawn()
        })
        .expect("spawn ck-subc");
        let rig = Self {
            daemon,
            dir,
            connection_file,
            events,
            ck_cli,
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        while !rig.connection_file.exists() {
            assert!(
                Instant::now() < deadline,
                "the daemon never published its connection file"
            );
            sleep(Duration::from_millis(25)).await;
        }
        assert!(rig.wait_for_module_live().await, "broca never became live");
        Some(rig)
    }

    async fn wait_for_module_live(&self) -> bool {
        let probe = broca_protocol::BindIdentity::new(&self.dir, "probe", "graceful-stop");
        let deadline = Instant::now() + Duration::from_secs(15);
        while Instant::now() < deadline {
            if let Ok(conn) = broca_subc::SubcConnection::connect(&self.connection_file).await {
                if conn.route_open_management("broca", &probe).await.is_ok() {
                    return true;
                }
            }
            sleep(Duration::from_millis(50)).await;
        }
        false
    }

    fn wal_dir(&self) -> PathBuf {
        self.connection_file
            .parent()
            .unwrap()
            .join("broca-state")
            .join("wal")
    }

    async fn restart(&self) {
        self.restart_with_args(&[]).await;
    }

    /// Restart with an explicit drain ceiling. Returns how many restart drains of broca the
    /// daemon had already logged, so [`Self::wait_for_restart_drain_verdict`] reads the line
    /// for this restart and never an earlier one. The count is taken before the request: this
    /// drain can't end before the restart is asked for.
    async fn restart_with_drain(&self, drain: Duration) -> usize {
        let earlier = self.restart_drain_lines().len();
        let drain_ms = drain.as_millis().to_string();
        self.restart_with_args(&["--drain-ms", &drain_ms]).await;
        earlier
    }

    async fn restart_with_args(&self, extra: &[&str]) {
        // Use the CLI beside the daemon so the request and its drain policy are real, not
        // a test's imitation of module control frames. The CLI acknowledges initiation only.
        let child = broca_test_support::retry_executable_file_busy(|| {
            Command::new(&self.ck_cli)
                .env_remove("BROCA_STATE_ROOT")
                .env("XDG_CONFIG_HOME", self.dir.join("config"))
                .env("XDG_RUNTIME_DIR", self.connection_file.parent().unwrap())
                .env("XDG_DATA_HOME", self.dir.join("runtime/data-home"))
                .args(["module", "restart", "broca"])
                .args(extra)
                // `spawn` inherits stdio, unlike `output`; capture it for the failure message.
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
        })
        .expect("spawn ck module restart broca");
        let output = child
            .wait_with_output()
            .await
            .expect("run ck module restart broca");
        assert!(output.status.success(), "ck restart failed: {output:?}");
    }

    async fn health(&self) -> Value {
        broca_subc::SubcConnection::connect(&self.connection_file)
            .await
            .unwrap()
            .supervisor_health_probe("broca")
            .await
            .unwrap()
            .metrics
            .expect("real health.check includes metrics")
    }

    async fn client(&self) -> broca_module_serve::SessionClient {
        broca_module_serve::SessionClient::bind(
            broca_subc::SubcConnection::connect(&self.connection_file)
                .await
                .unwrap(),
            "broca",
            broca_protocol::BindIdentity::new(self.dir.join("project"), "runner", "restart-run"),
        )
        .await
        .unwrap_or_else(|error| {
            eprintln!(
                "{}",
                super::session_client::rig_log_diagnostics(
                    &self.dir.join("runtime/data-home/cortexkit"),
                    &self.dir.join("runtime/broca-state"),
                )
            );
            panic!("bind broca: {error:?}");
        })
    }

    async fn wait_for_tool(&self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while !self.stub_holds_the_call() {
            assert!(Instant::now() < deadline, "tool never received call");
            sleep(Duration::from_millis(25)).await;
        }
    }

    async fn wait_for_replacement(&self, launch: &Value, budget: Duration) {
        let deadline = Instant::now() + budget;
        loop {
            if let Ok(conn) = broca_subc::SubcConnection::connect(&self.connection_file).await {
                if let Ok(health) = conn.supervisor_health_probe("broca").await {
                    if health
                        .metrics
                        .as_ref()
                        .is_some_and(|m| m["launch_id"] != *launch)
                    {
                        return;
                    }
                }
            }
            assert!(
                Instant::now() < deadline,
                "restart did not replace broca within {budget:?}"
            );
            sleep(Duration::from_millis(50)).await;
        }
    }

    /// The daemon's own record of how broca's restart drain ended, read from its log.
    ///
    /// When a drain ends the daemon logs `module drain complete` with `drained=true` only if
    /// broca had no forwarded request in flight and every busy gauge it declared read zero;
    /// `drained=false` means the drain ceiling passed first, or the wait itself failed, and the
    /// daemon forced teardown.
    /// Returns `drained` and the line it came from, for the first restart drain after the
    /// `earlier` ones [`Self::restart_with_drain`] counted. The wait is bounded by `budget`,
    /// which a caller sets past the drain ceiling so that a drain held to its ceiling still
    /// reports `drained=false` here, rather than looking like a line that never came.
    async fn wait_for_restart_drain_verdict(
        &self,
        earlier: usize,
        budget: Duration,
    ) -> (bool, String) {
        let deadline = Instant::now() + budget;
        loop {
            if let Some(line) = self.restart_drain_lines().into_iter().nth(earlier) {
                let drained = line
                    .split_whitespace()
                    .find_map(|field| field.strip_prefix("drained="))
                    .unwrap_or_else(|| panic!("the drain line carries no drained field: {line}"));
                return (drained == "true", line);
            }
            assert!(
                Instant::now() < deadline,
                "the daemon logged no end of broca's restart drain within {budget:?}"
            );
            sleep(Duration::from_millis(50)).await;
        }
    }

    /// Every `module drain complete` line for a restart of broca in the daemon's log, oldest
    /// first. The daemon writes `subc.<UTC day>.log` under `<data home>/cortexkit/run/logs`;
    /// each rig has its own data home, and every day's file is read in date order, so a test
    /// that runs across midnight still finds its line.
    fn restart_drain_lines(&self) -> Vec<String> {
        let logs = self.dir.join("runtime/data-home/cortexkit/run/logs");
        let Ok(entries) = std::fs::read_dir(logs) else {
            return Vec::new();
        };
        let mut segments: Vec<PathBuf> = entries
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("subc.") && name.ends_with(".log"))
            })
            .collect();
        segments.sort();
        segments
            .iter()
            .flat_map(|path| {
                std::fs::read_to_string(path)
                    .unwrap_or_default()
                    .lines()
                    .filter(|line| {
                        line.contains("module drain complete;")
                            && line.contains(" module_id=broca ")
                            && line.contains(" reason=Restart ")
                    })
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Whether the stub has received the tagged tool call, which it then holds.
    fn stub_holds_the_call(&self) -> bool {
        std::fs::read_to_string(&self.events)
            .unwrap_or_default()
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .any(|event| {
                event["kind"] == "request_received"
                    && event["body_json"]["arguments"]["tag"] == "graceful-stop"
            })
    }
}

/// The records of the WAL whose `RunStarted` names `session`, found by scanning the directory:
/// the daemon normalizes the project root before broca hashes it into the WAL's address, so a
/// path computed from the test's own spelling can name a file that never exists.
fn session_records(wal_dir: &Path, session: &str) -> Option<Vec<WalRecord>> {
    for entry in std::fs::read_dir(wal_dir).ok()?.flatten() {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("wal") {
            continue;
        }
        let Ok(range) =
            broca_wal::read_records_range(&path, broca_wal::DEFAULT_MAX_READ_BYTES, 0, None, None)
        else {
            continue;
        };
        let broca_wal::WalRangeRecords::Complete(frames) = range.records() else {
            continue;
        };
        let records: Vec<_> = frames.iter().map(|frame| frame.record.clone()).collect();
        if records.iter().any(|record| {
            matches!(record, WalRecord::RunStarted { session: bound, .. } if bound.session == session)
        }) {
            return Some(records);
        }
    }
    None
}

/// The live `ck-broca` pids the daemon supervises.
fn broca_children(daemon_pid: u32) -> Vec<u32> {
    let out = std::process::Command::new("pgrep")
        .args(["-P", &daemon_pid.to_string(), "-x", "ckdev-broca"])
        .output()
        .expect("run pgrep");
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|line| line.trim().parse().ok())
        .collect()
}

fn client_deadline_diagnostics(rig: &mut Rig, client: &mut Child, session: &str) -> String {
    let mut output = String::new();
    super::session_client::append_process_status(&mut output, "broca-session", client);
    output.push_str(&super::session_client::daemon_diagnostics(
        &rig.dir,
        session,
        &mut rig.daemon,
    ));
    eprintln!("{output}");
    output
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sigterm_during_a_tool_call_lets_it_finish_and_ends_the_run_interrupted() {
    let requests = Arc::new(AtomicUsize::new(0));
    let model_url = serve_model(Arc::clone(&requests)).await;
    let Some(mut rig) = Rig::start(&model_url).await else {
        return;
    };
    let project_root = rig.dir.join("project");
    std::fs::create_dir_all(&project_root).unwrap();
    let session = "graceful-stop";
    let tools = serde_json::json!([{
        "name": "slow",
        "description": "Takes a while.",
        "input_schema": {"type": "object"}
    }])
    .to_string();
    // The client only starts the run; its own fate once broca stops does not matter here.
    let mut client = Command::new(env!("CARGO_BIN_EXE_broca-session"))
        .arg("--subc")
        .arg(&rig.connection_file)
        .arg("--project-root")
        .arg(&project_root)
        .arg("--session")
        .arg(session)
        .args([
            "run",
            "--prompt",
            "Run the slow tool.",
            "--model",
            "mock/mock-model",
            "--tools",
            &tools,
        ])
        // A child's output bypasses the test harness's capture, so inheriting it would
        // interleave raw client lines with the harness's own result lines.
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn broca-session");

    let deadline = Instant::now() + Duration::from_secs(30);
    while !rig.stub_holds_the_call() {
        assert!(
            Instant::now() < deadline,
            "the run never reached its tool call\n{}",
            client_deadline_diagnostics(&mut rig, &mut client, session)
        );
        sleep(Duration::from_millis(25)).await;
    }

    let daemon_pid = rig.daemon.id().expect("daemon pid");
    let broca = broca_children(daemon_pid);
    assert_eq!(broca.len(), 1, "one supervised ck-broca: {broca:?}");
    let status = std::process::Command::new("kill")
        .args(["-TERM", &broca[0].to_string()])
        .status()
        .expect("run kill");
    assert!(status.success(), "kill -TERM {} failed", broca[0]);

    let deadline = Instant::now() + Duration::from_secs(30);
    let records = loop {
        if let Some(records) = session_records(&rig.wal_dir(), session) {
            if records
                .iter()
                .any(|record| matches!(record, WalRecord::RunFinished { .. }))
            {
                break records;
            }
        }
        assert!(
            Instant::now() < deadline,
            "the stopped run never wrote its terminal: {:?}\n{}",
            session_records(&rig.wal_dir(), session),
            client_deadline_diagnostics(&mut rig, &mut client, session)
        );
        sleep(Duration::from_millis(50)).await;
    };

    let results: Vec<(DispatchOutcome, bool)> = records
        .iter()
        .filter_map(|record| match record {
            WalRecord::ToolResult {
                result, outcome, ..
            } => Some((*outcome, result.is_error)),
            _ => None,
        })
        .collect();
    assert_eq!(
        results,
        vec![(DispatchOutcome::Completed, false)],
        "the call in flight finished and its result was recorded; nothing is outcome_unknown"
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record, WalRecord::StepStarted { .. }))
            .count(),
        1,
        "no model step starts after the stop"
    );
    assert_eq!(
        requests.load(Ordering::SeqCst),
        1,
        "no model request is made after the stop"
    );
    let terminals: Vec<RunFinishReason> = records
        .iter()
        .filter_map(|record| match record {
            WalRecord::RunFinished { reason, .. } => Some(*reason),
            _ => None,
        })
        .collect();
    assert_eq!(
        terminals,
        vec![RunFinishReason::Interrupted],
        "one Interrupted terminal, never Cancelled"
    );
}

async fn start_keyed_run(rig: &Rig, policy: Option<&str>) -> String {
    let project = rig.dir.join("project");
    std::fs::create_dir_all(&project).unwrap();
    let key = broca_protocol::BindIdentity::new(&project, "runner", "restart-run");
    let conn = broca_subc::SubcConnection::connect(&rig.connection_file)
        .await
        .unwrap();
    let route = conn.route_open_management("broca", &key).await.unwrap();
    let mut params = serde_json::json!({
        "prompt": "Run the slow tool.", "send_id": "restart-send",
        "model": {"provider":"mock", "model":"mock-model"},
        "tools": [{"name":"slow", "description":"Takes a while.", "input_schema":{"type":"object"}}]
    });
    if let Some(policy) = policy {
        params["on_restart"] = serde_json::json!(policy);
    }
    let reply = conn
        .route_request(
            &route,
            serde_json::to_vec(&serde_json::json!({"method":"session.send", "params":params}))
                .unwrap(),
        )
        .await
        .unwrap();
    reply["result"]["run_id"].as_str().unwrap().to_owned()
}

async fn wait_records(rig: &Rig, predicate: impl Fn(&[WalRecord]) -> bool) -> Vec<WalRecord> {
    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        if let Some(records) = session_records(&rig.wal_dir(), "restart-run") {
            if predicate(&records) {
                return records;
            }
        }
        assert!(
            Instant::now() < deadline,
            "WAL did not reach expected state: {:?}",
            session_records(&rig.wal_dir(), "restart-run")
        );
        sleep(Duration::from_millis(25)).await;
    }
}

async fn signal_during_tool(rig: &Rig, signal: &str) {
    rig.wait_for_tool().await;
    let children = broca_children(rig.daemon.id().unwrap());
    assert_eq!(children.len(), 1);
    assert!(std::process::Command::new("kill")
        .args([signal, &children[0].to_string()])
        .status()
        .unwrap()
        .success());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_module_restart_declares_busy_gauges_and_ends_drain_early() {
    let url = serve_model(Arc::new(AtomicUsize::new(0))).await;
    let Some(rig) = Rig::start(&url).await else {
        return;
    };
    let conn = broca_subc::SubcConnection::connect(&rig.connection_file)
        .await
        .unwrap();
    let entries = conn.catalog_list().await.unwrap();
    let entry = entries.iter().find(|e| e.module_id == "broca").unwrap();
    let busy = entry
        .self_signals
        .as_ref()
        .unwrap()
        .iter()
        .filter(|signal| signal.kind == subc_protocol::manifest::SelfSignalKind::Busy)
        .collect::<Vec<_>>();
    assert_eq!(
        busy.len(),
        1,
        "the daemon must see broca's busy declaration"
    );
    assert_eq!(busy[0].name, "runs_in_flight");
    assert_eq!(
        busy[0].anchored_to,
        subc_protocol::manifest::SignalAnchor::HealthGauges {
            gauges: vec!["runs_in_flight".into(), "opening".into(), "closing".into()],
        }
    );
    let metrics = rig.health().await;
    for gauge in ["runs_in_flight", "opening", "closing"] {
        assert_eq!(metrics[gauge], 0, "idle gauge {gauge}");
    }
    // The 120 s drain ceiling only bounds a hang. The proof is the daemon's own verdict: it
    // ends the drain by quiescence only once nothing forwarded to broca is in flight and it has
    // read broca's declared gauges as zero; otherwise it holds the drain to the ceiling and
    // records that instead. The verdict wait runs past the ceiling, so a drain held to it
    // fails on the verdict, not on a missing line.
    let drain = Duration::from_secs(120);
    let earlier = rig.restart_with_drain(drain).await;
    let (drained, line) = rig
        .wait_for_restart_drain_verdict(earlier, drain + Duration::from_secs(30))
        .await;
    assert!(
        drained,
        "the daemon must end an idle broca's restart drain by quiescence, not at its \
         {drain:?} ceiling: {line}"
    );
    rig.wait_for_replacement(&metrics["launch_id"], Duration::from_secs(80))
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn module_restart_during_long_tool_pauses_with_idle_gauge_before_goodbye() {
    let requests = Arc::new(AtomicUsize::new(0));
    let url = serve_recording_model(
        requests.clone(),
        Arc::new(std::sync::Mutex::new(Vec::new())),
        15000,
    )
    .await;
    let Some(rig) = Rig::start(&url).await else {
        return;
    };
    let run_id = start_keyed_run(&rig, Some("resume")).await;
    rig.wait_for_tool().await;
    let active = rig.health().await;
    assert_eq!(
        active["runs_in_flight"], 1,
        "the tool in flight must hold the drain"
    );
    assert_eq!(active["opening"], 0);
    assert_eq!(active["closing"], 0);
    // The tool takes 15 s, so the run reaches its boundary at about 15 s. The 120 s drain
    // ceiling only bounds a hang: under heavy host load the daemon can wait out a slow health
    // probe, so against the default 30 s ceiling a drain could reach its ceiling with nothing
    // left holding it open. The proof is the daemon's verdict below.
    let drain = Duration::from_secs(120);
    let earlier = rig.restart_with_drain(drain).await;
    let records = wait_records(&rig, |records| {
        records.iter().any(|r| {
            matches!(
                r,
                WalRecord::RunPaused { .. } | WalRecord::RunFinished { .. }
            )
        })
    })
    .await;
    assert!(
        records.iter().any(|r| matches!(
            r,
            WalRecord::RunPaused {
                reason: broca_protocol::PauseReason::Restart,
                ..
            }
        )),
        "the drain must pause at the boundary, not complete or interrupt: {records:?}"
    );
    assert!(
        records.iter().any(|r| matches!(
            r,
            WalRecord::ToolResult {
                outcome: DispatchOutcome::Completed,
                ..
            }
        )),
        "no early seal of the long tool"
    );
    // During a drain the daemon refuses ordinary client health probes, so the test can't read
    // broca's gauge directly. The daemon itself still reads it, and ends the drain by
    // quiescence only once the paused run has stopped counting as busy. Its verdict wait runs
    // past the ceiling, so a run that held the drain open fails on the verdict.
    let (drained, line) = rig
        .wait_for_restart_drain_verdict(earlier, drain + Duration::from_secs(30))
        .await;
    assert!(
        drained,
        "restart-paused work must let the daemon end the drain by quiescence, not at its \
         {drain:?} ceiling: {line}"
    );
    rig.wait_for_replacement(&active["launch_id"], Duration::from_secs(80))
        .await;
    let records = wait_records(&rig, |records| {
        records.iter().any(|r| {
            matches!(
                r,
                WalRecord::RunFinished {
                    reason: RunFinishReason::Completed,
                    ..
                }
            )
        })
    })
    .await;
    assert_eq!(
        records
            .iter()
            .filter(|r| matches!(r, WalRecord::RunStarted { .. }))
            .count(),
        1
    );
    assert!(records
        .iter()
        .any(|r| matches!(r, WalRecord::ResumeStarted { .. })));
    assert!(records.iter().any(|r| matches!(r,
        WalRecord::RunStarted { run_id: id, .. } if id.0 == run_id)));
    assert!(!records.iter().any(|r| matches!(
        r,
        WalRecord::RunFinished {
            reason: RunFinishReason::Interrupted,
            ..
        }
    )));
    assert_eq!(requests.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn module_restart_interrupt_policy_finishes_long_tool_at_boundary() {
    let requests = Arc::new(AtomicUsize::new(0));
    let url = serve_recording_model(
        requests.clone(),
        Arc::new(std::sync::Mutex::new(Vec::new())),
        15000,
    )
    .await;
    let Some(rig) = Rig::start(&url).await else {
        return;
    };
    start_keyed_run(&rig, None).await;
    rig.wait_for_tool().await;
    rig.restart().await;
    let records = wait_records(&rig, |records| {
        records
            .iter()
            .any(|r| matches!(r, WalRecord::RunFinished { .. }))
    })
    .await;
    assert!(matches!(
        records.last(),
        Some(WalRecord::RunFinished {
            reason: RunFinishReason::Interrupted,
            error: None,
            ..
        })
    ));
    assert!(records.iter().any(|r| matches!(
        r,
        WalRecord::ToolResult {
            outcome: DispatchOutcome::Completed,
            ..
        }
    )));
    assert!(!records
        .iter()
        .any(|r| matches!(r, WalRecord::RunPaused { .. })));
    assert_eq!(requests.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_resume_continues_same_episode_and_request_bytes() {
    let bodies = Arc::new(std::sync::Mutex::new(Vec::new()));
    let requests = Arc::new(AtomicUsize::new(0));
    let url = serve_recording_model(requests.clone(), bodies.clone(), 3000).await;
    let Some(rig) = Rig::start(&url).await else {
        return;
    };
    let run_id = start_keyed_run(&rig, Some("resume")).await;
    signal_during_tool(&rig, "-TERM").await;
    let records = wait_records(&rig, |records| {
        records.iter().any(|record| {
            matches!(
                record,
                WalRecord::RunFinished {
                    reason: RunFinishReason::Completed,
                    ..
                }
            )
        })
    })
    .await;
    let starts = records
        .iter()
        .filter_map(|record| {
            if let WalRecord::RunStarted {
                run_id,
                send_id,
                on_restart,
                ..
            } = record
            {
                Some((run_id.0.clone(), send_id.clone(), *on_restart))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(
        starts,
        vec![(
            run_id,
            Some("restart-send".into()),
            Some(broca_protocol::request::OnRestart::Resume)
        )]
    );
    let pause = records
        .iter()
        .position(|record| {
            matches!(
                record,
                WalRecord::RunPaused {
                    reason: broca_protocol::PauseReason::Restart,
                    ..
                }
            )
        })
        .expect("graceful boundary must pause, not interrupt");
    let resume = records
        .iter()
        .position(|record| matches!(record, WalRecord::ResumeStarted { .. }))
        .expect("boot must resume");
    let next_step = records
        .iter()
        .rposition(|record| matches!(record, WalRecord::StepStarted { .. }))
        .unwrap();
    assert!(pause < resume && resume < next_step);
    assert!(!records.iter().any(|record| matches!(
        record,
        WalRecord::RunFinished {
            reason: RunFinishReason::Interrupted,
            ..
        }
    )));
    assert_eq!(requests.load(Ordering::SeqCst), 2);
    let floor =
        std::fs::read_to_string(rig.wal_dir().parent().unwrap().join("wal-feature-floor")).unwrap();
    assert!(floor.contains("restart-pause/v1"));

    let control_bodies = Arc::new(std::sync::Mutex::new(Vec::new()));
    let control_requests = Arc::new(AtomicUsize::new(0));
    let control_url =
        serve_recording_model(control_requests.clone(), control_bodies.clone(), 3000).await;
    let control = Rig::start(&control_url).await.unwrap();
    start_keyed_run(&control, Some("resume")).await;
    wait_records(&control, |records| {
        records.iter().any(|record| {
            matches!(
                record,
                WalRecord::RunFinished {
                    reason: RunFinishReason::Completed,
                    ..
                }
            )
        })
    })
    .await;
    assert_eq!(
        bodies.lock().unwrap()[1],
        control_bodies.lock().unwrap()[1],
        "resumed request must match uninterrupted control byte for byte"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_interrupt_preserves_graceful_terminal() {
    let requests = Arc::new(AtomicUsize::new(0));
    let url = serve_model(requests.clone()).await;
    let Some(rig) = Rig::start(&url).await else {
        return;
    };
    start_keyed_run(&rig, Some("interrupt")).await;
    signal_during_tool(&rig, "-TERM").await;
    let records = wait_records(&rig, |records| {
        records
            .iter()
            .any(|record| matches!(record, WalRecord::RunFinished { .. }))
    })
    .await;
    assert!(matches!(
        records.last(),
        Some(WalRecord::RunFinished {
            reason: RunFinishReason::Interrupted,
            ..
        })
    ));
    assert!(!records
        .iter()
        .any(|record| matches!(record, WalRecord::RunPaused { .. })));
    assert_eq!(requests.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resume_policy_with_tool_past_drain_is_interrupted_unknown_never_paused() {
    let url = serve_recording_model(
        Arc::new(AtomicUsize::new(0)),
        Arc::new(std::sync::Mutex::new(Vec::new())),
        60000,
    )
    .await;
    let Some(rig) = Rig::start(&url).await else {
        return;
    };
    start_keyed_run(&rig, Some("resume")).await;
    signal_during_tool(&rig, "-TERM").await;
    let records = wait_records(&rig, |records| {
        records
            .iter()
            .any(|record| matches!(record, WalRecord::RunFinished { .. }))
    })
    .await;
    assert!(matches!(
        records.last(),
        Some(WalRecord::RunFinished {
            reason: RunFinishReason::Interrupted,
            ..
        })
    ));
    assert!(records.iter().any(|record| matches!(
        record,
        WalRecord::ToolResult {
            outcome: DispatchOutcome::Unknown,
            ..
        }
    )));
    assert!(!records
        .iter()
        .any(|record| matches!(record, WalRecord::RunPaused { .. })));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sigkill_with_resume_policy_still_seals_crash_orphan() {
    let url = serve_model(Arc::new(AtomicUsize::new(0))).await;
    let Some(rig) = Rig::start(&url).await else {
        return;
    };
    let run_id = start_keyed_run(&rig, Some("resume")).await;
    let launch = rig.health().await["launch_id"].clone();
    signal_during_tool(&rig, "-KILL").await;
    rig.wait_for_replacement(&launch, Duration::from_secs(30))
        .await;
    let records = wait_records(&rig, |records| {
        records
            .iter()
            .any(|record| matches!(record, WalRecord::RunFinished { .. }))
    })
    .await;
    assert!(matches!(
        records.last(),
        Some(WalRecord::RunFinished {
            reason: RunFinishReason::Interrupted,
            ..
        })
    ));
    assert!(!records
        .iter()
        .any(|record| matches!(record, WalRecord::RunPaused { .. })));

    let client = rig.client().await;
    let status = client.run_status(&run_id).await.unwrap();
    let value = serde_json::to_value(&status).unwrap();
    assert_eq!(value["state"], "interrupted");
    assert_eq!(value["error"]["class"], "transient");
    assert_eq!(
        value["error"]["provider_code"],
        "previous_process_ended_unsealed"
    );
    assert_eq!(value["error"]["message"], "the previous broca process ended without sealing this run: host crash, kill, or power loss");

    // Pin only a reply from the real restarted module, never a test-built response.
    let bytes = serde_json::to_vec(&status).unwrap();
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../broca-wire/tests/golden/run_status.crash_orphan.json");
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, &bytes).unwrap();
    }
    use sha2::{Digest, Sha256};
    assert_eq!(
        format!("{:x}", Sha256::digest(&bytes)),
        "212d487e8c557357753484a912903a496fe6e5713a8ec9d842509f718460fc6f"
    );
    assert_eq!(
        bytes,
        std::fs::read(&path).expect("generate the crash-orphan golden through the real serve path")
    );

    let mut stream = client
        .subscribe(broca_module::SubscribeFrom::Start)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut diagnostic = None;
        while let Some(body) = stream.next().await.unwrap() {
            let event = broca_wire::decode_subscribe_event(&body).unwrap();
            if let broca_wire::SubscribeEvent::Control { unit, .. } = event {
                match unit {
                    broca_protocol::ControlUnit::Error { error } => {
                        diagnostic = Some(serde_json::to_value(error).unwrap());
                    }
                    broca_protocol::ControlUnit::RunFinished { reason, .. } => {
                        assert_eq!(reason, RunFinishReason::Interrupted);
                        assert_eq!(diagnostic.as_ref(), Some(&value["error"]));
                        return;
                    }
                    _ => {}
                }
            }
        }
        panic!("subscription ended without the interrupted terminal");
    })
    .await
    .expect("crash-orphan terminal replay timed out");

    let db =
        rusqlite::Connection::open(rig.wal_dir().parent().unwrap().join("run-index.db")).unwrap();
    let state: String = db
        .query_row(
            "SELECT state FROM run_index WHERE run_id = ?1",
            [&run_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        state, "interrupted",
        "the diagnostic must not reclassify the run index"
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    let facts: Vec<Value> = loop {
        let mut stmt = db
            .prepare("SELECT segment_json FROM export_facts WHERE run_id = ?1")
            .unwrap();
        let facts: Vec<Value> = stmt
            .query_map([&run_id], |row| row.get::<_, String>(0))
            .unwrap()
            .map(|row| serde_json::from_str(&row.unwrap()).unwrap())
            .collect();
        if !facts.is_empty() {
            break facts;
        }
        assert!(
            Instant::now() < deadline,
            "crash-orphan export facts were not persisted"
        );
        sleep(Duration::from_millis(25)).await;
    };
    assert_eq!(facts.len(), 1);
    assert_eq!(facts[0]["terminal_reason"], "interrupted");
    assert!(
        facts[0].get("error").is_none(),
        "export facts carry reasons, not interruption diagnostics"
    );
}
