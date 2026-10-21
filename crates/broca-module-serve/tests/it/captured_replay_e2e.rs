//! Real module end-to-end proof against bytes captured from a provider.
//!
//! The replay server is deliberately an HTTP server rather than a provider or normalizer stub:
//! the deployed module performs its own request rendering, HTTP exchange, SSE decoding, and
//! normalization against the checked-in provider body.

use super::session_client::{self, ClientRun, CLIENT_DEADLINE};
use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Instant};

const REFUSAL_CAPTURE: &str =
    include_str!("../../../broca-provider/tests/fixtures/openai_chat_refusal_only.sse");
const TOOL_CAPTURE: &str = include_str!(
    "../../../broca-provider/tests/fixtures/openai_chat_split_tool_call_arguments.sse"
);
const ANTHROPIC_TOOL_CAPTURE: &str =
    include_str!("../../../broca-provider/tests/fixtures/anthropic-tool-call.sse");
const REFUSAL_CAPTURES: &[&str] = &[REFUSAL_CAPTURE];
const TOOL_LOOP_CAPTURES: &[&str] = &[TOOL_CAPTURE, REFUSAL_CAPTURE];
// Same capture twice: the fixture is a tool_use turn with no natural post-tool
// text completion. The second serve is so we can assert the anthropic
// tool_result wire on the follow-up request. finalize_at_tool_calls=1 then
// forces RunFinishReason::Completed (production budget-finalize, not a model
// stop). This is not the openai tool-loop proof (TOOL_CAPTURE then
// REFUSAL_CAPTURE).
const ANTHROPIC_TOOL_LOOP_CAPTURES: &[&str] = &[ANTHROPIC_TOOL_CAPTURE, ANTHROPIC_TOOL_CAPTURE];

struct ReplayServer {
    base_url: String,
    requests: Arc<AtomicUsize>,
    request_bodies: Arc<Mutex<Vec<Vec<u8>>>>,
    task: JoinHandle<()>,
}

impl Drop for ReplayServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl ReplayServer {
    async fn start(captures: &'static [&'static str]) -> Self {
        Self::start_with_hold(captures, false).await
    }

    async fn start_holding_responses(captures: &'static [&'static str]) -> Self {
        Self::start_with_hold(captures, true).await
    }

    async fn start_with_hold(captures: &'static [&'static str], hold_response: bool) -> Self {
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind replay server");
        let addr = listener.local_addr().expect("read replay server address");
        let requests = Arc::new(AtomicUsize::new(0));
        let request_count = Arc::clone(&requests);
        let request_bodies = Arc::new(Mutex::new(Vec::new()));
        let captured_bodies = Arc::clone(&request_bodies);
        let captures = Arc::new(captures.to_vec());
        let response_captures = Arc::clone(&captures);
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let request_count = Arc::clone(&request_count);
                let captured_bodies = Arc::clone(&captured_bodies);
                let response_captures = Arc::clone(&response_captures);
                tokio::spawn(async move {
                    if let Err(error) = serve_capture(
                        stream,
                        response_captures,
                        request_count,
                        captured_bodies,
                        hold_response,
                    )
                    .await
                    {
                        eprintln!("replay server connection failed: {error}");
                    }
                });
            }
        });
        Self {
            base_url: format!("http://{addr}"),
            requests,
            request_bodies,
            task,
        }
    }
}

async fn serve_capture(
    stream: tokio::net::TcpStream,
    captures: Arc<Vec<&'static str>>,
    requests: Arc<AtomicUsize>,
    request_bodies: Arc<Mutex<Vec<Vec<u8>>>>,
    hold_response: bool,
) -> std::io::Result<()> {
    let mut reader = BufReader::new(stream);
    let mut request_line = String::new();
    reader.read_line(&mut request_line).await?;
    let mut content_length = 0usize;
    let mut request_is_chunked = false;
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).await?;
        if line == "\r\n" || line == "\n" {
            break;
        }
        let lower = line.to_ascii_lowercase();
        if let Some(value) = lower.strip_prefix("content-length:") {
            content_length = value.trim().parse().unwrap_or(0);
        }
        if lower.starts_with("transfer-encoding:") && lower.contains("chunked") {
            request_is_chunked = true;
        }
    }
    let mut body = Vec::new();
    if request_is_chunked {
        loop {
            let mut size_line = String::new();
            reader.read_line(&mut size_line).await?;
            let size = size_line
                .trim()
                .split(';')
                .next()
                .unwrap_or_default()
                .trim();
            let size = usize::from_str_radix(size, 16).unwrap_or(0);
            if size == 0 {
                loop {
                    let mut trailer = String::new();
                    reader.read_line(&mut trailer).await?;
                    if trailer == "\r\n" || trailer == "\n" {
                        break;
                    }
                }
                break;
            }
            let start = body.len();
            body.resize(start + size, 0);
            reader.read_exact(&mut body[start..]).await?;
            let mut chunk_end = [0u8; 2];
            reader.read_exact(&mut chunk_end).await?;
        }
    } else {
        body.resize(content_length, 0);
        reader.read_exact(&mut body).await?;
    }
    let mut stream = reader.into_inner();
    let request_number = requests.fetch_add(1, Ordering::SeqCst);
    request_bodies.lock().unwrap().push(body);
    if hold_response {
        let mut sink = tokio::io::sink();
        tokio::io::copy(&mut stream, &mut sink).await?;
        return Ok(());
    }
    let capture = captures
        .get(request_number)
        .or_else(|| captures.last())
        .expect("replay server has at least one capture");
    stream
        .write_all(
            b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncache-control: no-cache\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n",
        )
        .await?;
    for chunk in capture.as_bytes().split_inclusive(|byte| *byte == b'\n') {
        stream
            .write_all(format!("{:X}\r\n", chunk.len()).as_bytes())
            .await?;
        stream.write_all(chunk).await?;
        stream.write_all(b"\r\n").await?;
        stream.flush().await?;
        sleep(Duration::from_millis(1)).await;
    }
    stream.write_all(b"0\r\n\r\n").await?;
    stream.shutdown().await
}

struct Daemon {
    child: RefCell<Child>,
    dir: PathBuf,
    connection_file: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        broca_test_support::kill_and_reap_tree(self.child.get_mut());
        broca_test_support::dump_rig_process_logs_on_failure(&self.dir, "daemon");
        broca_test_support::remove_rig_dir(&self.dir);
    }
}

impl Daemon {
    async fn start(
        provider_base_url: &str,
        aft_tools: &str,
        aft_result: &str,
        catalog: Option<serde_json::Value>,
    ) -> Self {
        let subc_core = sibling_bin("ck-subc", "SUBC_CORE_BIN");
        let fake_aft = sibling_bin("fake-aft-stub", "FAKE_AFT_STUB_BIN");
        assert!(subc_core.exists(), "subc-core missing at {subc_core:?}");
        assert!(fake_aft.exists(), "fake-aft-stub missing at {fake_aft:?}");
        let subc_core =
            broca_test_support::ckdev_binary(&subc_core, broca_test_support::scratch_root());

        static SCRUB_ATTESTATION: std::sync::Once = std::sync::Once::new();
        SCRUB_ATTESTATION.call_once(|| {
            std::env::remove_var("SUBC_MODULE_ID");
            std::env::remove_var("SUBC_LAUNCH_NONCE");
        });

        // One folder per test, named by a short digest of the test's path (the thread name).
        // The full path is too long: the daemon's connection temp file under it passed Windows'
        // 260-character path limit, so its create failed. The digest also avoids `::`, which a
        // Windows file name cannot contain.
        let test = std::thread::current().name().unwrap_or("test").to_owned();
        let digest = {
            use sha2::{Digest, Sha256};
            format!("{:x}", Sha256::digest(test.as_bytes()))
        };
        let dir = broca_test_support::scratch_root().join(format!(
            "replay-{}-{}",
            std::process::id(),
            &digest[..12]
        ));
        broca_test_support::remove_rig_dir(&dir);
        let config_home = dir.join("config");
        let runtime_dir = dir.join("runtime");
        let config_path = config_home.join("cortexkit").join("subc.jsonc");
        std::fs::create_dir_all(config_path.parent().unwrap()).expect("create subc config dir");
        std::fs::create_dir_all(&runtime_dir).expect("create runtime dir");
        if let Some(catalog) = catalog {
            let catalog_path = runtime_dir.join("broca-state/catalog/fusiform-lkg.json");
            std::fs::create_dir_all(catalog_path.parent().unwrap()).expect("create catalog dir");
            std::fs::write(&catalog_path, serde_json::to_vec_pretty(&catalog).unwrap())
                .expect("write catalog LKG");
        }

        let module_serve = broca_test_support::ckdev_binary(
            env!("CARGO_BIN_EXE_ck-broca"),
            broca_test_support::scratch_root(),
        );
        let config = serde_json::json!({
            "version": 1,
            "modules": {
                "broca": {
                    "program": module_serve,
                    "args": [],
                    "env": {
                        // Named explicitly so the WAL lands where `wal_dir` reads it back and
                        // where the catalog LKG above is seeded.
                        "BROCA_STATE_ROOT": runtime_dir.join("broca-state"),
                        "BROCA_PROVIDER_BASE_URL": provider_base_url,
                        "BROCA_TOOL_MODULE": "aft"
                    },
                    "enabled": true
                },
                "aft": {
                    "program": fake_aft,
                    "args": [],
                    "env": {
                        "FAKE_AFT_MODULE_ID": "aft",
                        "FAKE_AFT_TOOLS": aft_tools,
                        "FAKE_AFT_TOOLCALL_RESULT": aft_result
                    },
                    "enabled": true
                }
            }
        });
        std::fs::write(&config_path, serde_json::to_vec_pretty(&config).unwrap())
            .expect("write subc config");

        let connection_file = runtime_dir.join("subc-connection.json");
        let child = broca_test_support::retry_executable_file_busy(|| {
            broca_test_support::tokio_rig_command(&subc_core, &dir, "daemon")?
                // The daemon derives its per-module capture directory from the DATA home
                // (`<data-home>/cortexkit/run/logs/<module_id>.stderr.log`), not from
                // XDG_RUNTIME_DIR. A rig that leaves it unset appends its own "broca" module's
                // stderr into the OPERATOR'S production capture file, where placement gates read
                // seal-line counts. Pinning it under the rig keeps the count about production.
                .env("XDG_CONFIG_HOME", &config_home)
                .env("XDG_RUNTIME_DIR", &runtime_dir)
                .env("XDG_DATA_HOME", runtime_dir.join("data-home"))
                // The broca module is given its own BROCA_STATE_ROOT in the config above; dropping
                // any inherited copy keeps every other module the daemon spawns off it as well.
                .env_remove("BROCA_STATE_ROOT")
                .env("SUBC_PORT", "0")
                .kill_on_drop(true)
                .spawn()
        })
        .expect("spawn subc-core");
        let daemon = Self {
            child: RefCell::new(child),
            dir,
            connection_file,
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        while !daemon.connection_file.exists() {
            assert!(
                Instant::now() < deadline,
                "daemon never published its connection file"
            );
            sleep(Duration::from_millis(25)).await;
        }
        assert!(
            daemon.wait_for_module_live().await,
            "broca module never became live\n{}",
            broca_test_support::rig_log_diagnostics(
                &runtime_dir.join("data-home"),
                &daemon.broca_state_root()
            )
        );
        daemon
    }

    async fn wait_for_module_live(&self) -> bool {
        let probe = broca_protocol::BindIdentity::new(&self.dir, "probe", "readiness");
        // A hang guard, not a readiness budget: on a loaded Windows runner the module's boot has
        // taken longer than 15 s. A failure dumps broca's own logs (see `start`).
        let deadline = Instant::now() + broca_test_support::EVENT_HANG_GUARD;
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
        self.broca_state_root().join("wal")
    }

    fn broca_state_root(&self) -> PathBuf {
        self.connection_file.parent().unwrap().join("broca-state")
    }
}

fn sibling_bin(name: &str, env_key: &str) -> PathBuf {
    broca_test_support::subc_bin(name, env_key)
}

struct ClientRequest<'a> {
    project_root: &'a Path,
    session: &'a str,
    prompt: &'a str,
    model: &'a str,
    tools: &'a str,
    budget_json: Option<&'a str>,
}

async fn run_client(
    daemon: &Daemon,
    replay: &ReplayServer,
    request: ClientRequest<'_>,
) -> ClientRun {
    run_client_with_deadline(daemon, replay, request, CLIENT_DEADLINE)
        .await
        .unwrap_or_else(|diagnostics| panic!("client deadline expired\n{diagnostics}"))
}

async fn run_client_with_deadline(
    daemon: &Daemon,
    replay: &ReplayServer,
    request: ClientRequest<'_>,
    deadline: Duration,
) -> Result<ClientRun, String> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_broca-session"));
    command
        .arg("--subc")
        .arg(&daemon.connection_file)
        .arg("--project-root")
        .arg(request.project_root)
        .arg("--session")
        .arg(request.session)
        .args([
            "run",
            "--prompt",
            request.prompt,
            "--model",
            request.model,
            "--tools",
            request.tools,
        ])
        .args(
            request
                .budget_json
                .into_iter()
                .flat_map(|budget| ["--budget-json", budget]),
        );
    session_client::run_with_deadline(
        &mut command,
        deadline,
        |_| {},
        || {
            let mut diagnostics = session_client::daemon_diagnostics(
                &daemon.dir,
                request.session,
                &mut daemon.child.borrow_mut(),
            );
            diagnostics.push_str(&format!(
                "capture server requests received: {}\n",
                replay.requests.load(Ordering::SeqCst)
            ));
            diagnostics
        },
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_module_completes_against_captured_openai_chat_bytes() {
    let replay = ReplayServer::start(REFUSAL_CAPTURES).await;
    let daemon = Daemon::start(&replay.base_url, "get_time", "unused", None).await;
    let project_root = daemon.dir.join("project");
    std::fs::create_dir_all(&project_root).expect("create project root");
    let run = run_client(
        &daemon,
        &replay,
        ClientRequest {
            project_root: &project_root,
            session: "captured-replay",
            prompt: "Refuse this request.",
            model: "mock/mock-model",
            tools: "[]",
            budget_json: None,
        },
    )
    .await;

    assert!(run.lines.iter().any(|line| line.contains("BOUND")));
    assert!(run
        .lines
        .iter()
        .any(|line| line.contains("SEND state=active")));
    assert!(run.lines.iter().any(|line| line.contains("RUN_STARTED")));
    assert!(run
        .lines
        .iter()
        .any(|line| line.contains("I can’t help with that.")));
    assert!(run
        .lines
        .iter()
        .any(|line| line.contains("RUN_FINISHED") && line.contains("reason=Completed")));
    assert_eq!(run.exit_code, 0);
    assert!(replay.requests.load(Ordering::SeqCst) >= 1);
    let request = serde_json::from_slice::<serde_json::Value>(
        replay.request_bodies.lock().unwrap().first().unwrap(),
    )
    .expect("provider request is JSON");
    assert_eq!(request["model"], "mock/mock-model");
    assert_eq!(request["stream"], true);

    let wal_dir = daemon.wal_dir();
    assert!(wal_dir.exists(), "WAL directory should exist: {wal_dir:?}");
    assert!(std::fs::read_dir(wal_dir)
        .expect("read WAL directory")
        .next()
        .is_some());
    let diagnostics = session_client::daemon_diagnostics(
        &daemon.dir,
        "captured-replay",
        &mut daemon.child.borrow_mut(),
    );
    let kinds = diagnostics
        .split("--- session WAL record kinds:")
        .nth(1)
        .unwrap();
    let mut previous = 0;
    for kind in [
        "RunStarted",
        "StepStarted",
        "ModelAttemptFinished",
        "RunFinished",
    ] {
        let position = kinds
            .find(kind)
            .unwrap_or_else(|| panic!("missing {kind}: {kinds}"));
        assert!(position > previous, "record kinds out of order: {kinds}");
        previous = position;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_module_closes_a_tool_loop_against_sequential_captures() {
    let replay = ReplayServer::start(TOOL_LOOP_CAPTURES).await;
    let daemon = Daemon::start(&replay.base_url, "search_token", "TOKEN-REFRESHED", None).await;
    let project_root = daemon.dir.join("project");
    std::fs::create_dir_all(&project_root).expect("create project root");
    let tools = serde_json::json!([{
        "name": "search_token",
        "description": "Search the token index.",
        "input_schema": {
            "type": "object",
            "properties": { "query": { "type": "string" } },
            "required": ["query"]
        }
    }])
    .to_string();
    let run = run_client(
        &daemon,
        &replay,
        ClientRequest {
            project_root: &project_root,
            session: "captured-tool-loop",
            prompt: "Find the token refresh guidance.",
            model: "mock/mock-model",
            tools: &tools,
            budget_json: None,
        },
    )
    .await;

    assert!(run.lines.iter().any(|line| line.contains("BOUND")));
    assert!(run.lines.iter().any(|line| line.contains("RUN_STARTED")));
    assert!(
        run.lines
            .iter()
            .filter(|line| line.contains("STEP_STARTED"))
            .count()
            >= 2
    );
    assert!(run
        .lines
        .iter()
        .any(|line| line.contains("TOOL_CALL") && line.contains("search_token")));
    assert!(run
        .lines
        .iter()
        .any(|line| line.contains("TOOL_RESULT") && line.contains("TOKEN-REFRESHED")));
    assert!(run
        .lines
        .iter()
        .any(|line| line.contains("FINAL_TEXT I can’t help with that.")));
    assert!(run
        .lines
        .iter()
        .any(|line| line.contains("RUN_FINISHED") && line.contains("reason=Completed")));
    assert_eq!(run.exit_code, 0);
    assert!(replay.requests.load(Ordering::SeqCst) >= 2);

    let bodies = replay.request_bodies.lock().unwrap();
    let second_request = serde_json::from_slice::<serde_json::Value>(
        bodies.get(1).expect("second provider request body"),
    )
    .expect("second provider request is JSON");
    let messages = second_request["messages"]
        .as_array()
        .expect("OpenAI request has rendered messages");
    assert!(messages
        .iter()
        .any(|message| { message["role"] == "tool" && message["content"] == "TOKEN-REFRESHED" }));

    let wal_dir = daemon.wal_dir();
    assert!(wal_dir.exists(), "WAL directory should exist: {wal_dir:?}");
    assert!(std::fs::read_dir(wal_dir)
        .expect("read WAL directory")
        .next()
        .is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_deadline_dumps_rig_diagnostics_when_capture_server_holds_response() {
    let replay = ReplayServer::start_holding_responses(REFUSAL_CAPTURES).await;
    let daemon = Daemon::start(&replay.base_url, "get_time", "unused", None).await;
    let project_root = daemon.dir.join("project");
    std::fs::create_dir_all(&project_root).expect("create project root");

    let result = run_client_with_deadline(
        &daemon,
        &replay,
        ClientRequest {
            project_root: &project_root,
            session: "held-capture-deadline",
            prompt: "Refuse this request.",
            model: "mock/mock-model",
            tools: "[]",
            budget_json: None,
        },
        Duration::from_secs(3),
    )
    .await;
    let diagnostics = match result {
        Ok(_) => panic!("the client unexpectedly completed against a held response"),
        Err(diagnostics) => diagnostics,
    };

    assert!(replay.requests.load(Ordering::SeqCst) >= 1);
    for section in [
        "broca-session process status:",
        "subc-core daemon process status:",
        "capture server requests received:",
        "supervised broca stderr:",
        "supervised aft stderr:",
        "daemon dated subc.*.log segments:",
        "broca structured log segments:",
        "broca watchdog stall-*.jsonl files:",
        "WAL file inventory:",
        "session WAL record kinds: held-capture-deadline",
        "RunStarted",
        "StepStarted",
        "reaped broca-session after deadline:",
    ] {
        assert!(
            diagnostics.contains(section),
            "missing {section} in:\n{diagnostics}"
        );
    }
    assert!(
        !diagnostics.contains("ModelAttemptFinished"),
        "a held response cannot finish its attempt: {diagnostics}"
    );
    assert!(
        !diagnostics.contains("RunFinished"),
        "a held response cannot finish its run: {diagnostics}"
    );
    let mut missing_path_output = String::new();
    broca_test_support::append_file_tail(
        &mut missing_path_output,
        "missing diagnostic fixture",
        &daemon.dir.join("missing-diagnostic-fixture.log"),
    );
    assert!(missing_path_output.contains("absent"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_module_completes_against_captured_anthropic_bytes() {
    let replay = ReplayServer::start(ANTHROPIC_TOOL_LOOP_CAPTURES).await;
    let catalog = serde_json::json!({
        "source": "captured-replay",
        "resolved_at_ms": 1,
        "catalog_version": 1,
        "models": {
            "anthropic/claude-opus-5": {
                "existence": "present",
                "capability.attachment": false,
                "capability.reasoning": true,
                "capability.tool_call": true,
                "limit.context": 1000000
            }
        }
    });
    let daemon = Daemon::start(&replay.base_url, "get_time", "Madrid time", Some(catalog)).await;
    let project_root = daemon.dir.join("project");
    std::fs::create_dir_all(&project_root).expect("create project root");
    let tools = serde_json::json!([{
        "name": "get_time",
        "description": "Get the current time.",
        "input_schema": {
            "type": "object",
            "properties": { "timezone": { "type": "string" } },
            "required": ["timezone"]
        }
    }])
    .to_string();
    let run = run_client(
        &daemon,
        &replay,
        ClientRequest {
            project_root: &project_root,
            session: "captured-anthropic-replay",
            prompt: "What time is it in Madrid?",
            model: "anthropic/claude-opus-5",
            tools: &tools,
            budget_json: Some(
                r#"{"nudges":[],"finalize_at_tool_calls":1,"finalize_text":"Stop after the captured tool call."}"#,
            ),
        },
    )
    .await;

    assert!(run.lines.iter().any(|line| line.contains("BOUND")));
    assert!(run.lines.iter().any(|line| line.contains("RUN_STARTED")));
    // These markers prove local dispatch and presentation only. The rendered second request below
    // is the round-trip proof.
    assert!(run
        .lines
        .iter()
        .any(|line| line.contains("TOOL_CALL") && line.contains("get_time")));
    assert!(run
        .lines
        .iter()
        .any(|line| line.contains("TOOL_RESULT") && line.contains("Madrid time")));
    assert!(run
        .lines
        .iter()
        .any(|line| line.contains("FINAL_TEXT An invented blue-tile sample.")));
    assert!(run
        .lines
        .iter()
        .any(|line| line.contains("RUN_FINISHED") && line.contains("reason=Completed")));
    assert_eq!(run.exit_code, 0);
    assert!(replay.requests.load(Ordering::SeqCst) >= 2);

    let request = serde_json::from_slice::<serde_json::Value>(
        replay.request_bodies.lock().unwrap().first().unwrap(),
    )
    .expect("provider request is JSON");
    assert_eq!(request["model"], "claude-opus-5");
    assert!(request["messages"].is_array());
    assert!(request["system"].is_array());
    assert_eq!(request["thinking"]["type"], "enabled");
    assert_eq!(request["thinking"]["budget_tokens"], 1024);

    let bodies = replay.request_bodies.lock().unwrap();
    let second_request = serde_json::from_slice::<serde_json::Value>(
        bodies.get(1).expect("second provider request body"),
    )
    .expect("second provider request is JSON");
    assert!(second_request["messages"]
        .as_array()
        .unwrap()
        .iter()
        .any(|message| {
            message["role"] == "user"
                && message["content"].as_array().is_some_and(|blocks| {
                    blocks.iter().any(|block| {
                        block["type"] == "tool_result" && block["content"] == "Madrid time"
                    })
                })
        }));

    let wal_dir = daemon.wal_dir();
    assert!(wal_dir.exists(), "WAL directory should exist: {wal_dir:?}");
    assert!(std::fs::read_dir(wal_dir)
        .expect("read WAL directory")
        .next()
        .is_some());
}

/// Send one Anthropic tool-loop turn through the real module on its own session. `Err` is the
/// admission refusal; `Ok` waits for the run to finish and returns its `RunStarted` config.
async fn send_anthropic_turn(
    daemon: &Daemon,
    project_root: &Path,
    session: &str,
    variant: Option<&str>,
) -> Result<broca_wal::RunConfig, String> {
    let identity = broca_protocol::BindIdentity::new(project_root, "variant-e2e", session);
    let conn = broca_subc::SubcConnection::connect(&daemon.connection_file)
        .await
        .expect("connect to daemon");
    let client = broca_module_serve::SessionClient::bind_with_timeout(
        conn,
        "broca",
        identity.clone(),
        Duration::from_secs(10),
    )
    .await
    .unwrap_or_else(|error| {
        eprintln!(
            "{}",
            super::session_client::daemon_diagnostics(
                &daemon.dir,
                session,
                &mut daemon.child.borrow_mut(),
            )
        );
        panic!("bind broca session: {error:?}");
    });
    let tools: Vec<broca_protocol::request::Tool> = serde_json::from_value(serde_json::json!([{
        "name": "get_time",
        "description": "Get the current time.",
        "input_schema": {
            "type": "object",
            "properties": { "timezone": { "type": "string" } },
            "required": ["timezone"]
        }
    }]))
    .unwrap();
    let outcome = client
        .send(broca_module_serve::client::SendSpec {
            prompt: "What time is it in Madrid?",
            system: None,
            provider: "anthropic",
            model: "claude-opus-5",
            variant,
            service_tier: None,
            tools: Some(tools),
            send_id: None,
            max_output_tokens: None,
            cache: None,
            budget: Some(
                serde_json::from_str(
                    r#"{"nudges":[],"finalize_at_tool_calls":1,"finalize_text":"Stop after the captured tool call."}"#,
                )
                .unwrap(),
            ),
            auth: None,
            one_shot: None,
            context_management: None,
        })
        .await
        .map_err(|error| error.to_string())?;
    assert!(
        matches!(
            outcome,
            broca_module_serve::client::SendOutcome::Active { .. }
        ),
        "an idle session starts the run: {outcome:?}"
    );
    broca_test_support::wait_for(
        "variant run durably finishes",
        async {
            loop {
                if let Some(frames) = session_frames(&daemon.wal_dir(), session) {
                    let started = frames.iter().find_map(|record| match record {
                        broca_wal::WalRecord::RunStarted { config, .. } => Some((**config).clone()),
                        _ => None,
                    });
                    let finished = frames
                        .iter()
                        .any(|record| matches!(record, broca_wal::WalRecord::RunFinished { .. }));
                    if let (Some(config), true) = (started, finished) {
                        return Ok(config);
                    }
                }
                sleep(Duration::from_millis(50)).await;
            }
        },
        || describe_wal_dir(&daemon.wal_dir()),
    )
    .await
}

/// The records of the WAL whose `RunStarted` names `session`, found by SCANNING the WAL
/// directory rather than computing the address.
///
/// The address is a hash over the bind triple as the MODULE keyed it, and the daemon normalizes
/// the project root before the module sees it (on macOS /var becomes /private/var). A test that
/// recomputes the address from its own spelling of the root is only right where the two
/// normalizations agree -- and where they don't, it polls a file that never exists, so a run
/// that finished reads as one that hung, and a check that a refused send wrote nothing passes
/// no matter what was written. Each session name here is unique within the test, so the name
/// is enough to find its log.
fn session_frames(wal_dir: &Path, session: &str) -> Option<Vec<broca_wal::WalRecord>> {
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
        let names_session = records.iter().any(|record| {
            matches!(record, broca_wal::WalRecord::RunStarted { session: bound, .. }
                if bound.session == session)
        });
        if names_session {
            return Some(records);
        }
    }
    None
}

/// Every WAL in the directory with the sessions its `RunStarted` records name, for a failure
/// message that says whether the run's log is missing or merely unfinished.
fn describe_wal_dir(wal_dir: &Path) -> String {
    let Ok(entries) = std::fs::read_dir(wal_dir) else {
        return format!("<cannot read {wal_dir:?}>");
    };
    let mut lines = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        let sessions =
            broca_wal::read_records_range(&path, broca_wal::DEFAULT_MAX_READ_BYTES, 0, None, None)
                .map(|range| match range.records() {
                    broca_wal::WalRangeRecords::Complete(frames) => frames
                        .iter()
                        .filter_map(|frame| match &frame.record {
                            broca_wal::WalRecord::RunStarted { session, .. } => Some(format!(
                                "{:?}|{}|{}",
                                session.project_root, session.harness, session.session
                            )),
                            broca_wal::WalRecord::RunFinished { .. } => {
                                Some("finished".to_string())
                            }
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                        .join(", "),
                    other => format!("{other:?}").chars().take(40).collect(),
                })
                .unwrap_or_else(|error| format!("read error: {error}"));
        lines.push(format!(
            "{:?}: [{sessions}]",
            path.file_name().unwrap_or_default()
        ));
    }
    if lines.is_empty() {
        "<none>".to_string()
    } else {
        lines.join("; ")
    }
}

/// A send carrying `model.variant` through the real module: a variant the model's catalog
/// options define is admitted, recorded on `RunStarted`, frozen, and rendered as the Anthropic
/// thinking + effort parameters; the same send without one renders today's request; and a
/// variant the model does not define is refused at admission, naming the model's variants, with
/// nothing written.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_variant_send_renders_its_mapped_parameters_and_an_undefined_one_is_refused() {
    let replay = ReplayServer::start(ANTHROPIC_TOOL_LOOP_CAPTURES).await;
    // `capability.reasoning_options` is copied from what fusiform serves for claude-opus-4-6
    // (its served-payloads fixture, anyapi row): an on/off toggle, the effort levels low,
    // medium and high, and a thinking-token budget. That derives the variants
    // [none, low, medium, high].
    let catalog = serde_json::json!({
        "source": "captured-replay",
        "resolved_at_ms": 1,
        "catalog_version": 1,
        "models": {
            "anthropic/claude-opus-5": {
                "existence": "present",
                "capability.attachment": false,
                "capability.reasoning": true,
                "capability.reasoning_options": [
                    {"type": "toggle"},
                    {"type": "effort", "values": ["low", "medium", "high"]},
                    {"max": 127999, "min": 1024, "type": "budget_tokens"}
                ],
                "capability.tool_call": true,
                "limit.context": 1000000
            }
        }
    });
    let daemon = Daemon::start(&replay.base_url, "get_time", "Madrid time", Some(catalog)).await;
    let project_root = daemon.dir.join("project");
    std::fs::create_dir_all(&project_root).expect("create project root");

    let without = send_anthropic_turn(&daemon, &project_root, "no-variant", None)
        .await
        .expect("a send without a variant is admitted");
    let requests_without = replay.request_bodies.lock().unwrap().len();
    let with = send_anthropic_turn(&daemon, &project_root, "with-variant", Some("high"))
        .await
        .expect("a defined variant is admitted");

    assert_eq!(without.variant, None, "absent stays absent on RunStarted");
    assert!(without.frozen_render_config.get("variant").is_none());
    assert_eq!(
        with.variant.as_deref(),
        Some("high"),
        "the requested variant is recorded"
    );
    assert_eq!(with.frozen_render_config["variant"]["name"], "high");

    let bodies: Vec<serde_json::Value> = replay
        .request_bodies
        .lock()
        .unwrap()
        .iter()
        .map(|body| serde_json::from_slice(body).expect("provider request is JSON"))
        .collect();
    assert_eq!(
        requests_without, 2,
        "the tool loop makes two provider requests"
    );
    let (plain, varied) = (&bodies[0], &bodies[requests_without]);
    assert_eq!(
        plain["thinking"],
        serde_json::json!({"type": "enabled", "budget_tokens": 1024})
    );
    assert!(plain.get("output_config").is_none());
    assert_eq!(
        varied["thinking"],
        serde_json::json!({"type": "adaptive", "display": "summarized"})
    );
    assert_eq!(
        varied["output_config"],
        serde_json::json!({"effort": "high"})
    );
    // The two sends are different sessions on a Claude subscription route, so each carries its
    // own Claude Code session id, prompt id and body signature. Those must differ; everything
    // else in the persona must not.
    let persona = |body: &serde_json::Value| {
        let billing = body["system"][0]["text"]
            .as_str()
            .expect("the billing block leads the system prompt")
            .to_owned();
        let user: serde_json::Value = serde_json::from_str(
            body["metadata"]["user_id"]
                .as_str()
                .expect("metadata.user_id is a JSON string"),
        )
        .expect("metadata.user_id parses");
        let version = billing
            .split("; ")
            .find(|field| field.starts_with("x-anthropic-billing-header: cc_version="))
            .expect("the billing block names its version")
            .to_owned();
        (version, user["device_id"].clone())
    };
    assert_eq!(
        persona(varied),
        persona(plain),
        "both sessions claim the same Claude Code version and device"
    );
    // Apart from the variant's two fields and those per-session values, the requests are the
    // same bytes.
    let strip = |body: &serde_json::Value| {
        let mut body = body.clone();
        let object = body.as_object_mut().unwrap();
        object.remove("thinking");
        object.remove("output_config");
        object.remove("metadata");
        object["system"].as_array_mut().unwrap().remove(0);
        serde_json::to_string(&body).unwrap()
    };
    assert_eq!(strip(varied), strip(plain));

    let requests_before_refusal = bodies.len();
    let records_before_refusal = wal_record_count(&daemon.wal_dir());
    let error = refused_variant_frame(&daemon, &project_root).await;
    assert_eq!(error.code, "variant_unsupported");
    assert_eq!(
        error.detail,
        Some(serde_json::json!({
            "model": "anthropic/claude-opus-5",
            "variant": "max",
            "defined": ["none", "low", "medium", "high"]
        }))
    );
    assert_eq!(error.message,
        "open failed: run resolution failed: model 'anthropic/claude-opus-5' does not define variant 'max'; its variants are [none, low, medium, high]");
    assert_eq!(
        wal_record_count(&daemon.wal_dir()),
        records_before_refusal,
        "a refused send adds no WAL record, including records without RunStarted"
    );
    // Found by scanning, not by computed address (see `session_frames`), so this cannot pass
    // by looking at a file the module never keyed. The two admitted sessions are found the same
    // way above, which proves the scan sees this directory's logs.
    assert!(
        session_frames(&daemon.wal_dir(), "undefined-variant").is_none(),
        "a refused send writes no RunStarted. WALs present: {}",
        describe_wal_dir(&daemon.wal_dir())
    );
    assert_eq!(
        replay.request_bodies.lock().unwrap().len(),
        requests_before_refusal,
        "a refused send reaches no provider"
    );
}

// Read the actual Error frame: SessionClient intentionally reduces errors to display strings.
async fn refused_variant_frame(daemon: &Daemon, project_root: &Path) -> subc_protocol::ErrorBody {
    use subc_control::{ClientControlRequest, ClientControlResponse};
    use subc_protocol::{Flags, Frame, FrameType, Priority, PROTOCOL_VERSION};
    use subc_transport::{authenticate_client, connection_file, read_frame, write_frame};
    let config = connection_file::read(&daemon.connection_file).unwrap();
    let endpoint = &config.endpoints[0];
    let mut socket = tokio::net::TcpStream::connect((endpoint.host.as_str(), endpoint.port))
        .await
        .unwrap();
    authenticate_client(&mut socket, &config, Duration::from_secs(10))
        .await
        .unwrap();
    let open = ClientControlRequest::RouteOpen {
        identity: subc_protocol::BindIdentity::new(
            project_root.to_path_buf(),
            "variant-e2e",
            "undefined-variant",
        ),
        target: subc_protocol::RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        consumer_identity: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
    };
    let frame = Frame::build_with_version(
        PROTOCOL_VERSION,
        FrameType::Request,
        Flags::new(false, Priority::Interactive, false),
        0,
        0,
        1,
        serde_json::to_vec(&open).unwrap(),
    )
    .unwrap();
    write_frame(&mut socket, &frame).await.unwrap();
    let opened = tokio::time::timeout(Duration::from_secs(10), read_frame(&mut socket))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let ClientControlResponse::RouteOpen {
        route_channel,
        route_epoch,
    } = serde_json::from_slice(&opened.body).unwrap()
    else {
        panic!("expected route open");
    };
    let body = serde_json::to_vec(&serde_json::json!({
        "method": "session.send", "params": {
            "prompt": "What time is it in Madrid?",
            "model": { "provider": "anthropic", "model": "claude-opus-5", "variant": "max" }
        }
    }))
    .unwrap();
    let frame = Frame::build_with_version(
        PROTOCOL_VERSION,
        FrameType::Request,
        Flags::new(false, Priority::Interactive, false),
        route_channel,
        route_epoch,
        2,
        body,
    )
    .unwrap();
    write_frame(&mut socket, &frame).await.unwrap();
    let refused = tokio::time::timeout(Duration::from_secs(10), read_frame(&mut socket))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(refused.header.ty, FrameType::Error);
    serde_json::from_slice(&refused.body).unwrap()
}

fn wal_record_count(wal_dir: &Path) -> usize {
    std::fs::read_dir(wal_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("wal"))
        .map(|path| {
            let range = broca_wal::read_records_range(
                &path,
                broca_wal::DEFAULT_MAX_READ_BYTES,
                0,
                None,
                None,
            )
            .unwrap();
            let broca_wal::WalRangeRecords::Complete(frames) = range.records() else {
                panic!("incomplete WAL read");
            };
            frames.len()
        })
        .sum()
}
