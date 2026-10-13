//! Real-spine import proof: ck-import -> subc -> ck-broca, followed by read and send.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use broca_module::serve::RunStatusResponse;
use broca_module_serve::client::{SendOutcome, SendSpec};
use broca_module_serve::SessionClient;
use broca_protocol::{BindIdentity, ContentBlock};
use broca_subc::SubcConnection;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::time::{timeout, Instant};

fn required() -> bool {
    std::env::var_os("BROCA_REQUIRE_SUBC").is_some()
        || std::env::var("CI").is_ok_and(|value| value == "true")
}

/// A workspace binary from the same build as this test: the test executable lives in
/// `<target>/<profile>/deps/`, and the workspace's binaries in `<target>/<profile>/`. Deriving it
/// from the test's own path keeps working when `CARGO_TARGET_DIR` moves the target folder.
fn target_bin(name: &str, env_name: &str) -> PathBuf {
    if let Some(path) = std::env::var_os(env_name) {
        return PathBuf::from(path);
    }
    let exe = std::env::current_exe().expect("the test binary knows its own path");
    exe.parent()
        .and_then(Path::parent)
        .expect("the test binary sits in <target>/<profile>/deps")
        .join(format!("{name}{}", std::env::consts::EXE_SUFFIX))
}

fn subc_bin() -> PathBuf {
    broca_test_support::subc_bin("ck-subc", "SUBC_CORE_BIN")
}

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../ck-session-import/tests/fixtures/opencode.db")
}

fn ck_import_bin() -> PathBuf {
    broca_test_support::ckdev_binary(
        env!("CARGO_BIN_EXE_ck-import"),
        broca_test_support::scratch_root(),
    )
}

struct AiMock {
    child: tokio::process::Child,
    base_url: String,
    dir: PathBuf,
}

impl Drop for AiMock {
    fn drop(&mut self) {
        broca_test_support::kill_and_reap(&mut self.child);
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl AiMock {
    async fn start() -> Option<Self> {
        let dir = broca_test_support::scratch_root()
            .join(format!("ck-import-aimock-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).ok()?;
        let fixtures = dir.join("fixtures.json");
        std::fs::write(
            &fixtures,
            r#"{"fixtures":[{"match":{},"response":{"content":"Continuation after import."}}]}"#,
        )
        .ok()?;
        let argv: Vec<PathBuf> = std::env::var("AIMOCK_CMD")
            .map(|command| command.split_whitespace().map(PathBuf::from).collect())
            .unwrap_or_else(|_| {
                vec![
                    PathBuf::from("node"),
                    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                        .join("../../node_modules/@copilotkit/aimock/dist/cli.js"),
                ]
            });
        let (program, args) = argv.split_first()?;
        let mut child = match Command::new(program)
            .args(args)
            .arg("--fixtures")
            .arg(&fixtures)
            .arg("--port")
            .arg("0")
            .arg("--host")
            .arg("127.0.0.1")
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
        {
            Ok(child) => child,
            Err(error) if !required() => {
                eprintln!("SKIP real_module_e2e: cannot start aimock: {error}");
                return None;
            }
            Err(error) => panic!("cannot start required aimock: {error}"),
        };
        let stdout = child.stdout.take().expect("aimock stdout was piped");
        let mut lines = BufReader::new(stdout).lines();
        let address = timeout(Duration::from_secs(20), async {
            while let Ok(Some(line)) = lines.next_line().await {
                if let Some(rest) = line.split("http://").nth(1) {
                    return Some(
                        rest.split_whitespace()
                            .next()
                            .unwrap_or(rest)
                            .trim_end_matches('/')
                            .to_string(),
                    );
                }
            }
            None
        })
        .await
        .ok()
        .flatten();
        let Some(address) = address else {
            if required() {
                panic!("required aimock did not report a listening address");
            }
            eprintln!("SKIP real_module_e2e: aimock did not report a listening address");
            return None;
        };
        Some(Self {
            child,
            base_url: format!("http://{address}"),
            dir,
        })
    }
}

struct Daemon {
    child: tokio::process::Child,
    dir: PathBuf,
    connection_file: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        broca_test_support::kill_and_reap(&mut self.child);
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl Daemon {
    async fn start(base_url: &str) -> Option<Self> {
        let subc = subc_bin();
        let module = target_bin("ck-broca", "BROCA_MODULE_SERVE_BIN");
        for (name, path) in [("ck-subc", &subc), ("ck-broca", &module)] {
            if !path.exists() {
                if required() {
                    panic!("required {name} binary is missing at {}", path.display());
                }
                eprintln!("SKIP real_module_e2e: {name} missing at {}", path.display());
                return None;
            }
        }
        let subc = broca_test_support::ckdev_binary(&subc, broca_test_support::scratch_root());
        let module = broca_test_support::ckdev_binary(&module, broca_test_support::scratch_root());
        std::env::remove_var("SUBC_MODULE_ID");
        std::env::remove_var("SUBC_LAUNCH_NONCE");
        let dir = broca_test_support::scratch_root()
            .join(format!("ck-import-rig-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let config_home = dir.join("config");
        let runtime_dir = dir.join("runtime");
        let config_path = config_home.join("cortexkit/subc.jsonc");
        std::fs::create_dir_all(config_path.parent().unwrap()).ok()?;
        std::fs::create_dir_all(&runtime_dir).ok()?;
        std::fs::write(
            &config_path,
            serde_json::to_vec_pretty(&json!({
                "version": 1,
                "modules": {
                    "broca": {
                        "program": module,
                        "args": [],
                        "env": {
                            "BROCA_PROVIDER_BASE_URL": base_url,
                            "BROCA_TOOL_MODULE": "unused-aft"
                        },
                        "enabled": true
                    }
                }
            }))
            .unwrap(),
        )
        .ok()?;
        let connection_file = runtime_dir.join("subc-connection.json");
        let child = broca_test_support::retry_executable_file_busy(|| {
            Command::new(&subc)
                // The daemon derives its per-module capture directory from the DATA home
                // (`<data-home>/cortexkit/run/logs/<module_id>.stderr.log`), not from
                // XDG_RUNTIME_DIR. A rig that leaves it unset appends its own "broca" module's
                // stderr into the OPERATOR'S production capture file, where placement gates read
                // seal-line counts. Pinning it under the rig keeps the count about production.
                .env("XDG_CONFIG_HOME", &config_home)
                .env("XDG_RUNTIME_DIR", &runtime_dir)
                .env("XDG_DATA_HOME", runtime_dir.join("data-home"))
                // broca's state root defaults to the data home pinned above; an inherited
                // BROCA_STATE_ROOT would override it and put this rig's WAL in the operator's state.
                .env_remove("BROCA_STATE_ROOT")
                .env("SUBC_PORT", "0")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .kill_on_drop(true)
                .spawn()
        })
        .ok()?;
        let deadline = Instant::now() + Duration::from_secs(20);
        while !connection_file.exists() {
            if Instant::now() >= deadline {
                panic!("subc did not publish its connection file");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let daemon = Self {
            child,
            dir,
            connection_file,
        };
        let identity = BindIdentity::new(&daemon.dir, "probe", "readiness");
        loop {
            if let Ok(connection) = SubcConnection::connect(&daemon.connection_file).await {
                if connection
                    .route_open_management("broca", &identity)
                    .await
                    .is_ok()
                {
                    return Some(daemon);
                }
            }
            if Instant::now() >= deadline {
                panic!("ck-broca did not register its management surface");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

fn transcript_contains(messages: &[broca_module::serve::SessionReadMessage], needle: &str) -> bool {
    messages.iter().any(|item| {
        item.message
            .content
            .iter()
            .any(|block| matches!(block, ContentBlock::Text { text } if text.contains(needle)))
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn imports_reads_and_continues_on_real_module_rig() {
    let Some(aimock) = AiMock::start().await else {
        return;
    };
    let Some(daemon) = Daemon::start(&aimock.base_url).await else {
        return;
    };
    let project_root = daemon.dir.join("project");
    std::fs::create_dir_all(&project_root).unwrap();
    let session_id = "fixture-import-e2e";
    let child = broca_test_support::retry_executable_file_busy(|| {
        Command::new(ck_import_bin())
            .args(["opencode", "ses-text", "--json", "--source-db"])
            .arg(fixture())
            .arg("--subc")
            .arg(&daemon.connection_file)
            .arg("--project-root")
            .arg(&project_root)
            .args(["--session", session_id])
            // `spawn` inherits stdio, unlike `output`; the events below are read from stdout.
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
    })
    .unwrap();
    let output = child.wait_with_output().await.unwrap();
    assert!(
        output.status.success(),
        "ck-import failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let events: Vec<Value> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(events.last().unwrap()["event"], "done");

    let identity = BindIdentity::new(&project_root, "ck-app", session_id);
    let connection = SubcConnection::connect(&daemon.connection_file)
        .await
        .unwrap();
    let client = SessionClient::bind(connection, "broca", identity)
        .await
        .unwrap_or_else(|error| {
            let data_home = daemon.dir.join("runtime/data-home/cortexkit");
            eprintln!(
                "{}",
                broca_test_support::rig_log_diagnostics(&data_home, &data_home.join("broca"))
            );
            panic!("bind broca: {error:?}");
        });
    let imported = client.read(None, None).await.unwrap();
    assert_eq!(imported.lineage_state.state, "completed");
    assert!(transcript_contains(
        &imported.messages,
        "Hello from a scrubbed fixture."
    ));
    assert!(transcript_contains(&imported.messages, "Fixture reply."));

    let run_id = match client
        .send(SendSpec {
            prompt: "Continue this imported session.",
            system: None,
            provider: "openai",
            model: "gpt-4o-mini",
            variant: None,
            service_tier: None,
            tools: Some(Vec::new()),
            send_id: Some("continue-import-e2e"),
            cache: None,
            budget: None,
            auth: None,
            one_shot: None,
            context_management: None,
        })
        .await
        .unwrap()
    {
        SendOutcome::Active { run_id } => run_id,
        SendOutcome::Paused {
            run_id,
            pause_reason,
        } => panic!("new send unexpectedly paused {run_id}: {pause_reason:?}"),
        SendOutcome::Finished { run_id, reason } => {
            panic!("new send unexpectedly named finished run {run_id}: {reason:?}")
        }
        SendOutcome::Pending { submission_id } => {
            panic!("idle imported lineage unexpectedly queued {submission_id}")
        }
    };
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match client.run_status(&run_id).await.unwrap() {
            RunStatusResponse::Completed { .. } => break,
            RunStatusResponse::Active { .. } if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            status => panic!("continuation did not complete: {status:?}"),
        }
    }
    let continued = client.read(None, None).await.unwrap();
    assert!(transcript_contains(
        &continued.messages,
        "Continuation after import."
    ));
}
