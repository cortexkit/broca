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
    last_wait: std::sync::Mutex<String>,
}

impl Drop for Rig {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("last rig wait: {}", self.last_wait.lock().unwrap());
            eprintln!(
                "{}",
                broca_test_support::rig_log_diagnostics(
                    &self.dir.join("runtime/data-home/cortexkit"),
                    &self.dir.join("runtime/broca-state"),
                )
            );
        }
        broca_test_support::kill_and_reap_tree(&mut self.daemon);
        broca_test_support::dump_rig_process_logs_on_failure(&self.dir, "daemon");
        broca_test_support::remove_rig_dir(&self.dir);
    }
}

impl Rig {
    fn observe_wait(&self, observation: impl Into<String>) {
        *self.last_wait.lock().unwrap() = observation.into();
    }

    async fn start(model_url: &str) -> Option<Self> {
        // Periodic health eviction would race the manual restart under test. Drain probes
        // are direct health.check calls and ignore this cadence.
        Self::start_with_health(
            model_url,
            serde_json::json!({
                "cadence_ms": 3600000, "deadline_ms": 5000, "failure_threshold": 3
            }),
        )
        .await
    }

    async fn start_with_health(model_url: &str, health: Value) -> Option<Self> {
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
        broca_test_support::remove_rig_dir(&dir);
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
                    "health": health
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
            last_wait: std::sync::Mutex::new("waiting for the daemon connection file".into()),
        };
        broca_test_support::wait_for(
            "the daemon's published connection file",
            async {
                while !rig.connection_file.exists() {
                    sleep(Duration::from_millis(25)).await;
                }
            },
            || rig.restart_diagnostics(),
        )
        .await;
        assert!(rig.wait_for_module_live().await, "broca never became live");
        Some(rig)
    }

    async fn wait_for_module_live(&self) -> bool {
        let probe = broca_protocol::BindIdentity::new(&self.dir, "probe", "graceful-stop");
        let deadline = Instant::now() + Duration::from_secs(15);
        while Instant::now() < deadline {
            match broca_subc::SubcConnection::connect(&self.connection_file).await {
                Ok(conn) => {
                    let opened = conn.route_open_management("broca", &probe).await;
                    self.observe_wait(format!("waiting for broca's management route: {opened:?}"));
                    if opened.is_ok() {
                        return true;
                    }
                }
                Err(error) => self.observe_wait(format!(
                    "waiting for broca's management route: connect {error:?}"
                )),
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
    /// daemon had already logged, so [`Self::restart_drain_verdict`] reads the line
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
        self.observe_wait("waiting for ck module restart broca acknowledgement");
        let output = broca_test_support::wait_for(
            "ck module restart broca acknowledgement",
            child.wait_with_output(),
            || self.restart_diagnostics(),
        )
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
            self.observe_wait(format!(
                "waiting for the tagged tool call; last stub events: {:?}",
                std::fs::read_to_string(&self.events)
            ));
            assert!(Instant::now() < deadline, "tool never received call");
            sleep(Duration::from_millis(25)).await;
        }
    }

    async fn wait_for_replacement(&self, launch: &Value) {
        let description = format!("routable replacement of broca launch {launch}");
        self.observe_wait(format!("waiting for {description}; no health reply yet"));
        broca_test_support::wait_for(
            &description,
            async {
                loop {
                    let health =
                        match broca_subc::SubcConnection::connect(&self.connection_file).await {
                            Ok(conn) => conn
                                .supervisor_health_probe("broca")
                                .await
                                .map_err(|e| format!("health: {e:?}")),
                            Err(error) => Err(format!("connect: {error:?}")),
                        };
                    self.observe_wait(format!(
                        "waiting for {description}; last observation: {health:?}"
                    ));
                    if health.ok().and_then(|h| h.metrics).is_some_and(|m| {
                        m["launch_id"]
                            .as_str()
                            .is_some_and(|id| Some(id) != launch.as_str())
                    }) {
                        return;
                    }
                    sleep(Duration::from_millis(50)).await;
                }
            },
            || self.restart_diagnostics(),
        )
        .await;
    }

    /// The daemon's own record of how broca's restart drain ended, read from its log.
    ///
    /// When a drain ends the daemon logs `module drain complete` with `drained=true` only if
    /// broca had no forwarded request in flight and every busy gauge it declared read zero;
    /// `drained=false` means the drain ceiling passed first and the daemon forced teardown.
    /// Read only after the replacement is routable: the daemon writes this record synchronously
    /// before replacing the module. A manual restart coalesced with a health-triggered restart
    /// has no second verdict; restarting without a registered connection can also skip draining.
    /// Waiting for those nonexistent records would turn a completed restart into a spurious hang.
    fn restart_drain_verdict(&self, earlier: usize) -> Result<(bool, String), String> {
        let lines = self.restart_drain_lines();
        let line = lines.into_iter().nth(earlier).ok_or_else(|| {
            format!(
                "broca was replaced without restart drain verdict #{earlier}; {}",
                self.restart_diagnostics()
            )
        })?;
        let drained = line
            .split_whitespace()
            .find_map(|field| field.strip_prefix("drained="))
            .ok_or_else(|| format!("the drain line carries no drained field: {line}"))?;
        Ok((drained == "true", line))
    }

    fn restart_diagnostics(&self) -> String {
        let lines = self.daemon_log_lines();
        let health = health_restart_evidence(&lines);
        let events: Vec<_> = lines
            .iter()
            .filter(|line| {
                line.contains("broca") || line.contains("drain") || line.contains("ERROR")
            })
            .collect();
        format!("{}; {health}; restart drain records: {:?}; daemon lifecycle/health/drain events (timestamps retained): {:?}",
            self.last_wait.lock().unwrap(), self.restart_drain_lines(), events)
    }

    /// Every `module drain complete` line for a restart of broca in the daemon's log, oldest
    /// first. The daemon writes `subc.<UTC day>.log` under `<data home>/cortexkit/run/logs`;
    /// each rig has its own data home, and every day's file is read in date order, so a test
    /// that runs across midnight still finds its line.
    fn restart_drain_lines(&self) -> Vec<String> {
        self.daemon_log_lines()
            .into_iter()
            .filter(|line| {
                line.contains("module drain complete;")
                    && line.contains(" module_id=broca ")
                    && line.contains(" reason=Restart ")
            })
            .collect()
    }

    fn daemon_log_lines(&self) -> Vec<String> {
        daemon_log_lines(&self.dir)
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

fn health_restart_evidence(lines: &[String]) -> String {
    let for_broca = |line: &&String| {
        line.split_whitespace()
            .any(|field| field == "module_id=broca")
    };
    let failures: Vec<_> = lines
        .iter()
        .filter(for_broca)
        .filter(|line| {
            line.contains("health.check probe failed")
                || line.contains("module health threshold breached")
                || line.contains("critical module health alert")
                || line.contains("health restart budget exhausted")
        })
        .collect();
    let restarts: Vec<_> = lines
        .iter()
        .filter(for_broca)
        .filter(|line| line.contains("health-triggered module restart"))
        .collect();
    format!("observed broca periodic health failures/alerts: {failures:?}; observed broca health-triggered module restarts: {restarts:?}")
}

fn daemon_log_lines(dir: &Path) -> Vec<String> {
    let logs = dir.join("runtime/data-home/cortexkit/run/logs");
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
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .collect()
}

#[test]
fn restart_health_evidence_reads_all_dated_logs_and_retains_timestamps() {
    let dir = broca_test_support::scratch_root().join("restart-health-evidence");
    let logs = dir.join("runtime/data-home/cortexkit/run/logs");
    std::fs::create_dir_all(&logs).unwrap();
    let failure =
        "2026-10-20T23:59:59Z WARN subc health.check probe failed module_id=broca detail=timeout";
    let restart =
        "2026-10-21T00:00:01Z WARN subc health-triggered module restart module_id=broca delay_ms=0";
    std::fs::write(logs.join("subc.2026-10-20.log"), failure).unwrap();
    std::fs::write(logs.join("subc.2026-10-21.log"), restart).unwrap();
    let evidence = health_restart_evidence(&daemon_log_lines(&dir));
    assert!(evidence.contains(failure), "{evidence}");
    assert!(evidence.contains(restart), "{evidence}");
}

#[test]
fn restart_health_evidence_does_not_infer_eviction_from_other_events() {
    let lines = [
        "INFO subc module drain complete; module_id=broca reason=Restart drained=true",
        "WARN subc health.check probe failed module_id=aft detail=timeout",
        "WARN subc health-triggered module restart module_id=broca-other delay_ms=0",
    ]
    .map(str::to_owned);
    assert_eq!(health_restart_evidence(&lines),
        "observed broca periodic health failures/alerts: []; observed broca health-triggered module restarts: []");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn health_restart_can_coalesce_manual_restart_without_another_drain_verdict() {
    let url = serve_model(Arc::new(AtomicUsize::new(0))).await;
    let Some(rig) = Rig::start_with_health(
        &url,
        serde_json::json!({
            "cadence_ms": 100, "deadline_ms": 500, "failure_threshold": 1
        }),
    )
    .await
    else {
        return;
    };
    let launch = rig.health().await["launch_id"].clone();
    let children = broca_children(rig.daemon.id().unwrap());
    assert_eq!(children.len(), 1);
    let signal = |name: &str| {
        std::process::Command::new("kill")
            .args([name, &children[0].to_string()])
            .status()
            .unwrap()
    };
    assert!(signal("-STOP").success());
    // Hold the real module, not a CPU burner: periodic health evicts it, and that restart's
    // drain reaches its ceiling. Queue the manual restart while the health-triggered restart
    // is tearing down the held module: it coalesces with that restart, with no second drain.
    // Release the process even when the hang guard fires so teardown can finish normally.
    let observed = broca_test_support::event_timeout(
        "health-triggered restart drain of held broca",
        async {
            loop {
                let evidence = health_restart_evidence(&rig.daemon_log_lines());
                rig.observe_wait(format!("waiting for periodic health eviction: {evidence}"));
                if evidence.contains("health-triggered module restart")
                    && !rig.restart_drain_lines().is_empty()
                {
                    return evidence;
                }
                sleep(Duration::from_millis(25)).await;
            }
        },
        || rig.restart_diagnostics(),
    )
    .await;
    if observed.is_err() {
        assert!(signal("-CONT").success());
    }
    let evidence = observed.expect("the held module must trigger a logged periodic health restart");
    assert!(
        evidence.contains("module health threshold breached"),
        "{evidence}"
    );
    assert!(evidence.contains("module_id=broca"), "{evidence}");
    eprintln!("held-process evidence: {evidence}");
    let (earlier, released) =
        tokio::join!(rig.restart_with_drain(Duration::from_secs(120)), async {
            let received = broca_test_support::event_timeout(
                "manual restart queued during health teardown",
                async {
                    loop {
                        if rig.daemon_log_lines().iter().any(|line| {
                            line.contains("supervisor request received")
                                && line.contains("op=supervisor.restart")
                                && line.contains("module_id=broca")
                        }) {
                            break;
                        }
                        sleep(Duration::from_millis(25)).await;
                    }
                },
                || rig.restart_diagnostics(),
            )
            .await;
            assert!(signal("-CONT").success());
            received
        });
    released.expect("manual restart must reach the daemon before releasing the held module");
    rig.wait_for_replacement(&launch).await;
    assert_eq!(
        rig.restart_drain_lines().len(),
        earlier,
        "the coalesced restart must not emit a second drain verdict"
    );
    let message = rig
        .restart_drain_verdict(earlier)
        .expect_err("a replaced module with no second drain must be diagnosed immediately");
    assert!(
        message.contains("was replaced without restart drain verdict"),
        "{message}"
    );
    assert!(
        message.contains("health-triggered module restart"),
        "{message}"
    );
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

async fn wait_records(
    rig: &Rig,
    description: &str,
    predicate: impl Fn(&[WalRecord]) -> bool,
) -> Vec<WalRecord> {
    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        let records = session_records(&rig.wal_dir(), "restart-run");
        rig.observe_wait(format!(
            "waiting for {description}; last WAL records: {records:?}"
        ));
        if let Some(records) = records {
            if predicate(&records) {
                return records;
            }
        }
        assert!(
            Instant::now() < deadline,
            "WAL did not reach {description}: {:?}",
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
    // Keep the drain ceiling beyond the shared hang guard: only quiescence can replace this
    // idle instance before the guard fires. Check the daemon's verdict after that observable
    // effect, rather than waiting for a verdict that a coalesced restart never emits.
    let drain = Duration::from_secs(120);
    let earlier = rig.restart_with_drain(drain).await;
    rig.wait_for_replacement(&metrics["launch_id"]).await;
    let (drained, line) = rig
        .restart_drain_verdict(earlier)
        .unwrap_or_else(|message| panic!("{message}"));
    assert!(
        drained,
        "the daemon must end an idle broca's restart drain by quiescence, not at its \
         {drain:?} ceiling: {line}"
    );
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
    // Leave the daemon time to observe the tool boundary without reaching its drain ceiling.
    // The shared hang guard bounds replacement; the daemon's verdict proves quiescence.
    let drain = Duration::from_secs(120);
    let earlier = rig.restart_with_drain(drain).await;
    let records = wait_records(&rig, "restart-run terminal or pause", |records| {
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
    // quiescence only once the paused run has stopped counting as busy.
    rig.wait_for_replacement(&active["launch_id"]).await;
    let (drained, line) = rig
        .restart_drain_verdict(earlier)
        .unwrap_or_else(|message| panic!("{message}"));
    assert!(
        drained,
        "restart-paused work must let the daemon end the drain by quiescence, not at its \
         {drain:?} ceiling: {line}"
    );
    let records = wait_records(&rig, "resumed run completion", |records| {
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
    let records = wait_records(&rig, "interrupted run at tool boundary", |records| {
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
    let records = wait_records(&rig, "resumed run completion", |records| {
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
    wait_records(&control, "uninterrupted control completion", |records| {
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
    let records = wait_records(&rig, "explicit-interrupt terminal", |records| {
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
    let records = wait_records(&rig, "interrupted run with unresolved tool", |records| {
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
    rig.wait_for_replacement(&launch).await;
    let records = wait_records(&rig, "crash-orphan terminal", |records| {
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
    assert_eq!(value["reason"], "runner_crash");
    assert_eq!(value["usage"]["steps"], 1);
    assert_eq!(value["usage"]["unreported_steps"], 1);
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
    let golden =
        std::fs::read(&path).expect("generate the crash-orphan golden through the real serve path");
    assert_eq!(bytes, golden);

    let mut stream = client
        .subscribe(broca_module::SubscribeFrom::Start)
        .await
        .unwrap();
    broca_test_support::event_timeout(
        "crash-orphan terminal subscription replay",
        async {
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
        },
        || format!("WAL directory={}", rig.wal_dir().display()),
    )
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
    let facts: Vec<Value> =
        broca_test_support::wait_until("crash-orphan export facts persisted", || async {
            let mut stmt = db
                .prepare("SELECT segment_json FROM export_facts WHERE run_id = ?1")
                .unwrap();
            let facts: Vec<Value> = stmt
                .query_map([&run_id], |row| row.get::<_, String>(0))
                .unwrap()
                .map(|row| serde_json::from_str(&row.unwrap()).unwrap())
                .collect();
            if !facts.is_empty() {
                return Ok(facts);
            }
            Err(format!("run_id={run_id}, export_facts={}", facts.len()))
        })
        .await;
    assert_eq!(facts.len(), 1);
    assert_eq!(facts[0]["terminal_reason"], "interrupted");
    assert!(
        facts[0].get("error").is_none(),
        "export facts carry reasons, not interruption diagnostics"
    );
}

async fn interrupt_reference_reply(rig: &Rig, run_id: &str, method: &str) -> Value {
    let connection = broca_subc::SubcConnection::connect(&rig.connection_file)
        .await
        .unwrap();
    let key = broca_protocol::BindIdentity::new(rig.dir.join("project"), "runner", "restart-run");
    let route = connection
        .route_open_management("broca", &key)
        .await
        .unwrap();
    let reply = connection
        .route_request(
            &route,
            serde_json::to_vec(&serde_json::json!({"method":method,"params":{"run_id":run_id}}))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(reply.get("error").is_none(), "{reply}");
    let mut result = reply["result"].clone();
    if result.get("run_id").is_some() {
        result["run_id"] = serde_json::json!("@RUN_ID@");
    }
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interrupt_reasons_survive_real_kill_cuts_and_pin_serve_replies() {
    for planned in [true, false] {
        let url = serve_model(Arc::new(AtomicUsize::new(0))).await;
        let Some(rig) = Rig::start(&url).await else {
            return;
        };
        let run_id = start_keyed_run(&rig, None).await;
        rig.wait_for_tool().await;
        let launch = rig.health().await["launch_id"].clone();
        if planned {
            rig.restart().await;
        } else {
            signal_during_tool(&rig, "-KILL").await;
        }
        rig.wait_for_replacement(&launch).await;
        let expected = if planned {
            "runner_shutdown"
        } else {
            "runner_crash"
        };
        let records = wait_records(&rig, "planned-shutdown or crash terminal", |records| {
            records
                .iter()
                .any(|record| matches!(record, WalRecord::RunFinished { .. }))
        })
        .await;
        assert!(
            matches!(records.last(), Some(WalRecord::RunFinished { interrupt_reason: Some(reason), .. }) if reason == expected),
            "planned={planned}, expected {expected}, last record: {:?}",
            records.last()
        );
        let before = serde_json::to_vec(records.last().unwrap()).unwrap();
        // Cut after the terminal's sync, then boot again: recovery must not overwrite its cause.
        let launch = rig.health().await["launch_id"].clone();
        let children = broca_children(rig.daemon.id().unwrap());
        assert_eq!(children.len(), 1);
        assert!(std::process::Command::new("kill")
            .args(["-KILL", &children[0].to_string()])
            .status()
            .unwrap()
            .success());
        rig.wait_for_replacement(&launch).await;
        let after = wait_records(&rig, "durable terminal after second kill", |records| {
            records
                .iter()
                .any(|record| matches!(record, WalRecord::RunFinished { .. }))
        })
        .await;
        assert_eq!(serde_json::to_vec(after.last().unwrap()).unwrap(), before);
        for method in ["run.status", "run.result"] {
            let value = interrupt_reference_reply(&rig, &run_id, method).await;
            assert_eq!(value["state"], "interrupted");
            assert_eq!(value["reason"], expected);
            assert!(value.get("directives").is_none());
            let bytes = serde_json::to_vec(&value).unwrap();
            let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
                "../broca-wire/tests/golden/{}.{}.json",
                method.replace('.', "_"),
                expected
            ));
            if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
                std::fs::write(&path, &bytes).unwrap();
            }
            assert_eq!(
                bytes,
                std::fs::read(&path)
                    .expect("capture interrupt reference replies through the real serve path")
            );
        }
    }
}
