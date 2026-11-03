//! Portable checks for the CLI's private scope handoff before any daemon bind.

use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};

use broca_protocol::BindIdentity;
use broca_subc::{carrier_scope::CarrierScope, SubcConnection};
use serde_json::{json, Value};
use subc_control::{ClientControlRequest, ClientControlResponse};
use subc_protocol::{Flags, Frame, FrameType, Priority};
use subc_transport::{connection_file, read_frame, write_frame};

struct Daemon {
    file: std::path::PathBuf,
    server: tokio::task::JoinHandle<()>,
    opens: Arc<Mutex<Vec<Value>>>,
    requests: Arc<Mutex<Vec<Value>>>,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_file(&self.file);
    }
}

impl Daemon {
    async fn start(capabilities: &[&str]) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let file = broca_test_support::scratch_root().join(format!("session-scope-{port}.json"));
        let key = vec![0x5a; 32];
        let daemon_id = [0xa5; 16];
        connection_file::write_atomic(
            &file,
            &connection_file::ConnectionInfo {
                schema: connection_file::SCHEMA_VERSION,
                wire_version: None,
                endpoints: vec![connection_file::Endpoint {
                    host: "127.0.0.1".into(),
                    port,
                }],
                key: key.clone(),
                daemon_id,
                pid: std::process::id(),
                daemon_ver: "session-scope".into(),
            },
        )
        .unwrap();
        let opens = Arc::new(Mutex::new(Vec::new()));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let server_opens = opens.clone();
        let server_requests = requests.clone();
        let capabilities: Vec<String> = capabilities.iter().map(|s| (*s).to_owned()).collect();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            subc_transport::authenticate_server(
                &mut stream,
                &key,
                &daemon_id,
                "session-scope",
                broca_test_support::STAND_IN_AUTH_DEADLINE,
            )
            .await
            .unwrap();
            let mut channel = 10;
            while let Ok(Some(frame)) = read_frame(&mut stream).await {
                let (ty, body) = match (frame.header.channel, frame.header.ty) {
                    (0, FrameType::Ping) => (FrameType::Pong, Vec::new()),
                    (0, FrameType::Request) => {
                        let value: Value = serde_json::from_slice(&frame.body).unwrap();
                        let reply = match serde_json::from_value(value.clone()).unwrap() {
                            ClientControlRequest::ServerDescribe {} => {
                                ClientControlResponse::ServerDescribe {
                                    protocol_ver: 1,
                                    subc_ops: vec![],
                                    capabilities: capabilities.clone(),
                                    connected_clients: 1,
                                    counters: None,
                                    build_git_sha: None,
                                    build_lock_digest: None,
                                    capability_requirements: vec![],
                                    machine_id: None,
                                    config_path: None,
                                }
                            }
                            ClientControlRequest::RouteOpen { .. } => {
                                server_opens.lock().unwrap().push(value);
                                channel += 1;
                                ClientControlResponse::RouteOpen {
                                    route_channel: channel,
                                    route_epoch: 1,
                                }
                            }
                            other => panic!("unexpected control: {other:?}"),
                        };
                        (FrameType::Response, serde_json::to_vec(&reply).unwrap())
                    }
                    (_, FrameType::Request) => {
                        let request: Value = serde_json::from_slice(&frame.body).unwrap();
                        server_requests.lock().unwrap().push(request.clone());
                        if request["method"] == "session.subscribe" {
                            (FrameType::StreamEnd, vec![])
                        } else if request["name"] == "tool.catalog" {
                            let answer = json!({"generation": "1", "catalog_digest": "test-catalog", "tools": [{
                                "name": "read", "description": "Read a file", "input_schema": {},
                                "schema_digest": "read-schema", "semantics": 1, "capabilities": ["code.read/v1"]
                            }]});
                            (FrameType::Response, serde_json::to_vec(&answer).unwrap())
                        } else {
                            (FrameType::Response, br#"{"result":{}}"#.to_vec())
                        }
                    }
                    (_, FrameType::Goodbye | FrameType::Cancel) => continue,
                    (_, other) => panic!("unexpected frame: {other:?}"),
                };
                let reply = Frame::build(
                    ty,
                    Flags::new(false, Priority::Interactive, false),
                    frame.header.channel,
                    frame.header.epoch,
                    frame.header.corr,
                    body,
                )
                .unwrap();
                if write_frame(&mut stream, &reply).await.is_err() {
                    break;
                }
            }
        });
        Self {
            file,
            server,
            opens,
            requests,
        }
    }
}

fn selector() -> subc_protocol::scope::ScopeSelector {
    "rig/ref/7"
        .parse::<CarrierScope>()
        .unwrap()
        .read_proof(&mut "00".repeat(32).as_bytes())
        .unwrap()
}

#[tokio::test]
async fn scoped_client_carries_selector_on_command_and_subscription_only() {
    let daemon = Daemon::start(&["scopes/v1", "scope-bind-proof/v1"]).await;
    let client = broca_module_serve::SessionClient::bind_with_scope_and_timeout(
        SubcConnection::connect(&daemon.file).await.unwrap(),
        "broca",
        BindIdentity::new("/local-only", "test", "scoped-session"),
        Some(selector()),
        std::time::Duration::ZERO,
    )
    .await
    .unwrap();
    client
        .management_raw("session.read", &json!({}))
        .await
        .unwrap()
        .unwrap();
    let mut stream = client
        .subscribe(broca_module::serve::SubscribeFrom::Start)
        .await
        .unwrap();
    assert!(stream.next().await.unwrap().is_none());
    let opens = daemon.opens.lock().unwrap();
    assert_eq!(opens.len(), 2);
    for open in opens.iter() {
        assert_eq!(open["scope"], serde_json::to_value(selector()).unwrap());
        assert_eq!(
            open["target"],
            json!({"kind": "management_surface", "module_id": "broca"})
        );
        assert!(open["identity"].get("project_root").is_none());
    }
    let requests = daemon.requests.lock().unwrap();
    assert_eq!(requests[0], json!({"method": "session.read", "params": {}}));
    assert_eq!(
        requests[1],
        json!({"method": "session.subscribe", "params": {"from": "start"}})
    );
}

#[tokio::test]
async fn scoped_client_refuses_daemon_without_proof_capability() {
    let daemon = Daemon::start(&["scopes/v1"]).await;
    let error = broca_module_serve::SessionClient::bind_with_scope_and_timeout(
        SubcConnection::connect(&daemon.file).await.unwrap(),
        "broca",
        BindIdentity::new("/local-only", "test", "scoped-session"),
        Some(selector()),
        std::time::Duration::ZERO,
    )
    .await
    .err()
    .expect("old daemons must not silently open unscoped routes");
    assert!(error.to_string().contains("scope_unsupported:"));
    assert!(daemon.opens.lock().unwrap().is_empty());
}

#[tokio::test]
async fn scoped_compose_plan_carries_selector_and_plan_preset() {
    use tokio::io::AsyncWriteExt;

    let daemon = Daemon::start(&["scopes/v1", "scope-bind-proof/v1"]).await;
    let items = daemon.file.with_extension("items.json");
    std::fs::write(
        &items,
        json!({"tool_items": [{"provider": "aft", "preset": "reader", "params": {}}],
            "system_text_items": [], "step_transform_items": []})
        .to_string(),
    )
    .unwrap();
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_broca-session"))
        .arg("--subc")
        .arg(&daemon.file)
        .args([
            "--project-root",
            ".",
            "--harness",
            "test",
            "--session",
            "compose",
            "compose-plan",
            "--scope",
            "rig/ref/7",
            "--items",
        ])
        .arg(&items)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all("00".repeat(32).as_bytes())
        .await
        .unwrap();
    let output =
        broca_test_support::wait_for("scoped compose-plan", child.wait_with_output(), || {
            "compose-plan did not finish its scoped tool catalog fetch".into()
        })
        .await
        .unwrap();
    std::fs::remove_file(items).unwrap();
    assert!(
        output.status.success(),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let plan: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(plan["tool_items"][0]["preset"], "reader");
    let opens = daemon.opens.lock().unwrap();
    assert_eq!(opens.len(), 1);
    assert_eq!(opens[0]["scope"], serde_json::to_value(selector()).unwrap());
    assert_eq!(
        opens[0]["target"],
        json!({"kind": "tool_provider", "module_id": "aft"})
    );
    let requests = daemon.requests.lock().unwrap();
    assert_eq!(requests[0]["preset"], "reader");
    assert_eq!(requests[0]["arguments"]["preset"], "reader");
    assert!(requests[0].get("scope").is_none());
    assert!(requests[0]["arguments"].get("scope").is_none());
}

#[test]
fn scoped_cli_refuses_missing_proof_before_connecting() {
    let output = Command::new(env!("CARGO_BIN_EXE_broca-session"))
        .args([
            "--subc",
            "nonexistent-scope-test-connection.json",
            "--project-root",
            ".",
            "--harness",
            "test",
            "--session",
            "missing-proof",
            "--scope",
            "rig/ref/1",
            "--bind-timeout-ms",
            "0",
            "read",
        ])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.status.code(), Some(1), "{text}");
    assert!(text.contains("scope_bind_proof_required:"), "{text}");
    assert!(!text.contains("cannot connect subc"), "{text}");
}
