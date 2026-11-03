//! Native owner-operated coverage: build the published daemon and fake-AFT fixture,
//! then run with BROCA_REQUIRE_SUBC=1 so a missing fixture is a failure, not a skip.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    process::Child,
};

const OWNER: &str = "scope-rig-owner";
const PROOF: [u8; 32] = [0x11; 32];

fn hex(proof: &[u8]) -> String {
    proof.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn events(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}
fn assert_no_proof(output: &std::process::Output, proof: &[u8]) {
    for bytes in [&output.stdout, &output.stderr] {
        let text = String::from_utf8_lossy(bytes);
        assert!(
            !text.contains(&hex(proof)),
            "the CLI exposed its private hex proof"
        );
        assert!(
            !text.contains(&URL_SAFE_NO_PAD.encode(proof)),
            "the CLI exposed its private wire proof"
        );
    }
}
fn safe_output(output: &std::process::Output, proof: &[u8]) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
    .replace(&hex(proof), "<redacted>")
    .replace(&URL_SAFE_NO_PAD.encode(proof), "<redacted>")
}

struct Rig {
    daemon: Child,
    root: PathBuf,
    connection: PathBuf,
    workdir: PathBuf,
    local_project: PathBuf,
    state: PathBuf,
    aft_events: PathBuf,
}
impl Drop for Rig {
    fn drop(&mut self) {
        broca_test_support::kill_and_reap_tree(&mut self.daemon);
        broca_test_support::dump_rig_process_logs_on_failure(&self.root, "daemon");
        broca_test_support::remove_rig_dir(&self.root);
    }
}
impl Rig {
    async fn start() -> Option<Self> {
        let binary = |env: &str, name: &str| {
            std::env::var_os(env).map(PathBuf::from).unwrap_or_else(|| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("../../.test-rigs")
                    .join(name)
            })
        };
        let subc = binary("SUBC_CORE_BIN", "ck-subc");
        let stub = binary("FAKE_AFT_STUB_BIN", "fake-aft-stub");
        for path in [&subc, &stub] {
            if !path.exists() {
                assert!(
                    std::env::var_os("BROCA_REQUIRE_SUBC").is_none(),
                    "required daemon fixture is absent: {}",
                    path.display()
                );
                eprintln!("SKIP scope_real_daemon_e2e: missing {} (set BROCA_REQUIRE_SUBC=1 to require it)", path.display());
                return None;
            }
        }
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "broca-run-carrier-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&root).unwrap();
        // Windows scratch paths are already absolute; canonicalize would add a
        // verbatim prefix absent from the daemon's stamp. Unix needs symlink resolution.
        #[cfg(not(windows))]
        let root = root.canonicalize().unwrap();
        let config = root.join("config/cortexkit/subc.jsonc");
        let runtime = root.join("runtime");
        let workdir = root.join("scope-workdir");
        let local_project = root.join("local-project");
        let state = root.join("state");
        for dir in [
            config.parent().unwrap(),
            &runtime,
            &workdir,
            &local_project,
            &state.join("catalog"),
        ] {
            std::fs::create_dir_all(dir).unwrap();
        }
        // The explicit mock URL bypasses model resolution, but startup still needs a loaded LKG.
        std::fs::write(state.join("catalog/fusiform-lkg.json"), json!({
            "catalog_version":1,"resolved_at_ms":1,"source":"models.dev",
            "models":{"typesafe/jev-1.13.0":{"existence":"present","model.kind":"decision","rate.input":{"state":"stated_zero"},"rate.output":{"state":"stated_zero"}}}
        }).to_string()).unwrap();
        assert!(
            broca_catalog::CatalogCache::open(state.join("catalog/fusiform-lkg.json"))
                .snapshot()
                .is_ok()
        );
        let sync = root.join("owner-scopes.json");
        let owner_events = root.join("owner-events.jsonl");
        let aft_events = root.join("aft-events.jsonl");
        std::fs::write(
            &sync,
            json!([{
                "ref":"carrier-run", "scope_epoch":1,"kind":"head","workdir":workdir,
                "bind_proof_sha256":format!("{:x}", Sha256::digest(PROOF)),
                "carriers":[{"principal":{"kind":"direct"},"targets":["aft"]}],"attributes":{}
            }])
            .to_string(),
        )
        .unwrap();
        let module = |id: &str, mut env: Value| {
            env["FAKE_AFT_MODULE_ID"] = json!(id);
            json!({"program":stub,"args":[],"env":env,"enabled":true,"reserved":true})
        };
        std::fs::write(&config, json!({"version":1,"scope_authority_owners":[OWNER],"modules":{
            OWNER: module(OWNER, json!({"FAKE_AFT_SCOPE_SYNC_PATH":sync,"FAKE_AFT_EVENTS_PATH":owner_events})),
            "aft":module("aft", json!({"FAKE_AFT_TOOLS":"echo","FAKE_AFT_EVENTS_PATH":aft_events,
                "FAKE_AFT_CAPABILITIES":json!({"provides":[subc_protocol::scope::BIND_NO_PROJECT_CAPABILITY,subc_protocol::scope::SCOPE_WORKDIR_CAPABILITY]}).to_string()}))
        }}).to_string()).unwrap();
        let subc = broca_test_support::ckdev_binary(&subc, broca_test_support::scratch_root());
        let daemon = broca_test_support::retry_executable_file_busy(|| {
            broca_test_support::tokio_rig_command(&subc, &root, "daemon")?
                .env("HOME", &root)
                .env("XDG_CONFIG_HOME", root.join("config"))
                .env("XDG_RUNTIME_DIR", &runtime)
                .env("XDG_DATA_HOME", root.join("data"))
                .env("SUBC_PORT", "0")
                .env_remove("BROCA_STATE_ROOT")
                .env_remove("SUBC_MODULE_ID")
                .env_remove("SUBC_LAUNCH_NONCE")
                .env_remove("SUBC_LAUNCH_NONCE_FD")
                .kill_on_drop(true)
                .spawn()
        })
        .unwrap();
        let rig = Self {
            daemon,
            connection: runtime.join("subc-connection.json"),
            root,
            workdir,
            local_project,
            state,
            aft_events,
        };
        broca_test_support::wait_until("the owner accepts its Direct carrier scope", || async {
            if events(&owner_events).iter().any(|event| {
                event["kind"] == "scope_sync_response"
                    && event["body_json"]["results"]
                        .as_array()
                        .is_some_and(|rows| {
                            !rows.is_empty() && rows.iter().all(|row| row["outcome"] != "refused")
                        })
            }) {
                Ok(())
            } else {
                Err("owner scope sync is not accepted yet".into())
            }
        })
        .await;
        broca_test_support::wait_until("AFT registers in the real daemon", || async {
            let conn = broca_subc::SubcConnection::connect(&rig.connection)
                .await
                .map_err(|_| "daemon connection is not ready".to_owned())?;
            let catalog = conn
                .catalog_list()
                .await
                .map_err(|_| "catalog is not ready".to_owned())?;
            if catalog.iter().any(|entry| entry.module_id == "aft") {
                Ok(())
            } else {
                Err("AFT is not registered".into())
            }
        })
        .await;
        Some(rig)
    }

    async fn run(&self, proof: &[u8], url: &str, connection: &Path) -> std::process::Output {
        let mut command = broca_test_support::tokio_rig_command(
            Path::new(env!("CARGO_BIN_EXE_broca-run")),
            &self.root,
            "runner",
        )
        .unwrap();
        command.args(["--model","mock","--prompt","use echo then finish","--scope"])
            .arg(format!("{OWNER}/carrier-run/1")).args(["--preset", "worker"]).arg("--subc").arg(connection)
            .arg("--base-url").arg(url).arg("--project-root").arg(&self.local_project)
            .args(["--session","carrier-test","--max-steps","2","--json-events","--tools"])
            .arg(json!([{"name":"echo","description":"echo","input_schema":{"type":"object"},"execution_mode":"parallel"}]).to_string())
            .env("BROCA_STATE_ROOT", &self.state)
            .env_remove("SUBC_MODULE_ID").env_remove("SUBC_LAUNCH_NONCE").env_remove("SUBC_LAUNCH_NONCE_FD")
            .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).kill_on_drop(true);
        let mut child = broca_test_support::retry_executable_file_busy(|| command.spawn()).unwrap();
        {
            let mut stdin = child.stdin.take().unwrap();
            if !proof.is_empty() {
                stdin.write_all(hex(proof).as_bytes()).await.unwrap();
            }
        }
        broca_test_support::wait_for(
            "the Direct carrier runner exits",
            child.wait_with_output(),
            || "broca-run has not exited".into(),
        )
        .await
        .unwrap()
    }
}

struct Model {
    url: String,
    calls: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Model {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Model {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let task = tokio::spawn(async move {
            for step in 0..2 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut chunk = [0u8; 4096];
                let end = loop {
                    let n = socket.read(&mut chunk).await.unwrap();
                    assert!(n > 0, "the model request ended before its headers");
                    request.extend_from_slice(&chunk[..n]);
                    if let Some(end) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                        break end + 4;
                    }
                    assert!(request.len() < 1024 * 1024);
                };
                let headers = String::from_utf8_lossy(&request[..end]);
                let length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                while request.len() < end + length {
                    let n = socket.read(&mut chunk).await.unwrap();
                    assert!(n > 0, "the model request body was incomplete");
                    request.extend_from_slice(&chunk[..n]);
                }
                observed.fetch_add(1, Ordering::SeqCst);
                let (delta, reason) = if step == 0 {
                    (
                        json!({"role":"assistant","tool_calls":[{"index":0,"id":"call-echo","type":"function","function":{"name":"echo","arguments":"{}"}}]}),
                        "tool_calls",
                    )
                } else {
                    (json!({"role":"assistant","content":"done"}), "stop")
                };
                let first = json!({"id":"mock","object":"chat.completion.chunk","model":"mock","choices":[{"index":0,"delta":delta,"finish_reason":null}]});
                let last = json!({"id":"mock","object":"chat.completion.chunk","model":"mock","choices":[{"index":0,"delta":{},"finish_reason":reason}],"usage":{"prompt_tokens":7,"completion_tokens":2}});
                let body = format!("data: {first}\n\ndata: {last}\n\ndata: [DONE]\n\n");
                let response = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        Self { url, calls, task }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn direct_carrier_proof_opens_and_dispatches_on_the_owners_stamped_workdir() {
    let Some(rig) = Rig::start().await else {
        return;
    };
    let model = Model::start().await;
    let output = rig.run(&PROOF, &model.url, &rig.connection).await;
    assert_no_proof(&output, &PROOF);
    assert!(
        output.status.success(),
        "scoped run failed: {}",
        safe_output(&output, &PROOF)
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    let captured = events(&rig.aft_events);
    let binds: Vec<_> = captured
        .iter()
        .filter(|event| event["kind"] == "attach")
        .collect();
    assert!(!binds.is_empty());
    for bind in binds {
        assert_eq!(bind["principal"]["kind"], "direct");
        assert_eq!(bind["scope"]["owner"]["module_id"], OWNER);
        assert_eq!(bind["scope"]["ref"], "carrier-run");
        assert_eq!(bind["scope"]["scope_epoch"], 1);
        assert_eq!(bind["scope"]["workdir"], json!(rig.workdir));
        assert!(bind["identity"].get("project_root").is_none());
    }
    assert!(captured
        .iter()
        .any(|event| event["kind"] == "request_received" && event["body_json"]["name"] == "echo"));
    for entry in std::fs::read_dir(rig.local_project.join(".broca/wal")).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|extension| extension == "wal") {
            let raw = std::fs::read(path).unwrap();
            let text = String::from_utf8_lossy(&raw);
            assert!(!text.contains(&hex(&PROOF)), "the proof reached the WAL");
            assert!(
                !text.contains(&URL_SAFE_NO_PAD.encode(PROOF)),
                "the wire proof reached the WAL"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn wrong_direct_carrier_proof_is_refused_by_name_without_reaching_aft() {
    let Some(rig) = Rig::start().await else {
        return;
    };
    let wrong = [0x22; 32];
    let output = rig.run(&wrong, "http://127.0.0.1:1", &rig.connection).await;
    assert_no_proof(&output, &wrong);
    assert!(!output.status.success());
    assert!(safe_output(&output, &wrong).contains("scope_bind_proof_mismatch"));
    assert!(!events(&rig.aft_events)
        .iter()
        .any(|event| event["kind"] == "attach" || event["kind"] == "request_received"));
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_direct_carrier_proof_is_refused_before_connecting() {
    let Some(rig) = Rig::start().await else {
        return;
    };
    let output = rig
        .run(
            &[],
            "http://127.0.0.1:1",
            &rig.root.join("does-not-exist.json"),
        )
        .await;
    assert!(!output.status.success());
    let text = safe_output(&output, &PROOF);
    assert!(text.contains("scope_bind_proof_required"));
    assert!(!text.contains("connect subc"));
    assert!(!events(&rig.aft_events)
        .iter()
        .any(|event| event["kind"] == "attach"));
}
