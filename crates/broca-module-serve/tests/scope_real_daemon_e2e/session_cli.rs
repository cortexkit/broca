//! The Direct test client carries a scope through broca to a workdir-dependent file tool.

use super::*;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const PROOF: [u8; 32] = [0x3a; 32];
const FILE_TEXT: &str = "contents from the daemon-stamped workdir";

#[derive(Default)]
struct Seen {
    binds: Mutex<HashMap<(u16, u32), Value>>,
    reads: Mutex<Vec<Value>>,
}

struct FileTool(Arc<Seen>);

#[async_trait::async_trait]
impl subc_client_rs::ModuleHandler for FileTool {
    async fn on_bind(
        &self,
        request: &subc_client_rs::RouteBindRequest,
    ) -> subc_client_rs::BindDecision {
        self.0.binds.lock().unwrap().insert(
            (request.handle.channel, request.handle.epoch),
            json!({"scope": request.scope, "identity": request.identity}),
        );
        subc_client_rs::BindDecision::accept()
    }

    async fn handle(
        &self,
        ctx: subc_client_rs::RequestCtx,
        body: Vec<u8>,
    ) -> subc_client_rs::HandlerOutcome {
        let request: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(request["preset"], "reader");
        assert!(request.get("scope").is_none());
        let answer = match request["name"].as_str().unwrap() {
            "tool.catalog" => {
                let schema = json!({"type":"object", "properties":{"path":{"type":"string"}}, "required":["path"]});
                let mut answer = json!({"generation":"1", "catalog_digest":"session-file-catalog", "tools":[{
                    "name":"read", "description":"Read a file", "input_schema":schema,
                    "schema_digest":cortexkit_role_tool_provider::catalog::schema_digest(&schema).unwrap(),
                    "semantics":1, "capabilities":["code.read/v1"], "result_format":"content/v1"
                }]});
                if let Some(composition) = request["arguments"].get("composition") {
                    answer["composition_digest"] = json!(
                        cortexkit_role_tool_provider::catalog::composition_digest(composition)
                            .unwrap()
                    );
                }
                answer
            }
            "read" => {
                let route = ctx.route_handle();
                let bind = self.0.binds.lock().unwrap()[&(route.channel, route.epoch)].clone();
                let answer = match bind["scope"]["workdir"].as_str() {
                    Some(workdir) => {
                        let path = request["arguments"]["path"].as_str().unwrap();
                        assert_eq!(path, "hello.txt");
                        let text = std::fs::read_to_string(Path::new(workdir).join(path)).unwrap();
                        json!({"content":[{"type":"text", "text":text}], "isError":false})
                    }
                    None => {
                        json!({"content":[{"type":"text", "text":"project_required"}], "isError":true})
                    }
                };
                self.0
                    .reads
                    .lock()
                    .unwrap()
                    .push(json!({"bind":bind, "answer":answer}));
                answer
            }
            other => panic!("unexpected file-tool operation: {other}"),
        };
        subc_client_rs::HandlerOutcome::Response(serde_json::to_vec(&answer).unwrap())
    }
}

struct ReadModel {
    url: String,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for ReadModel {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl ReadModel {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/v1", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            for step in 0..4 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut chunk = [0u8; 4096];
                let end = loop {
                    let n = socket.read(&mut chunk).await.unwrap();
                    assert!(n > 0, "model request ended before its headers");
                    request.extend_from_slice(&chunk[..n]);
                    if let Some(end) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                        break end + 4;
                    }
                };
                let length: usize = String::from_utf8_lossy(&request[..end])
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap();
                while request.len() < end + length {
                    let n = socket.read(&mut chunk).await.unwrap();
                    assert!(n > 0, "model request body was incomplete");
                    request.extend_from_slice(&chunk[..n]);
                }
                let (delta, reason) = if step % 2 == 0 {
                    (
                        json!({"role":"assistant", "tool_calls":[{"index":0,"id":format!("file-{step}"),"type":"function","function":{"name":"read","arguments":"{\"path\":\"hello.txt\"}"}}]}),
                        "tool_calls",
                    )
                } else {
                    let body: Value = serde_json::from_slice(&request[end..end + length]).unwrap();
                    let result = body["messages"].as_array().unwrap().last().unwrap();
                    assert_eq!(result["role"], "tool");
                    assert!(
                        result["content"].to_string().contains(FILE_TEXT),
                        "{result}"
                    );
                    (
                        json!({"role":"assistant", "content":"file read succeeded"}),
                        "stop",
                    )
                };
                let first = json!({"id":"scope-read", "object":"chat.completion.chunk", "model":"mock-model", "choices":[{"index":0,"delta":delta,"finish_reason":null}]});
                let last = json!({"id":"scope-read", "object":"chat.completion.chunk", "choices":[{"index":0,"delta":{},"finish_reason":reason}], "usage":{"prompt_tokens":7,"completion_tokens":2}});
                let body = format!("data: {first}\n\ndata: {last}\n\ndata: [DONE]\n\n");
                let reply = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                socket.write_all(reply.as_bytes()).await.unwrap();
            }
        });
        Self { url, task }
    }
}

async fn cli(rig: &Rig, tail: &[&str]) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_broca-session"))
        .arg("--subc")
        .arg(&rig.connection_file)
        .arg("--project-root")
        .arg(&rig.dir)
        .args([
            "--harness",
            "scope-cli",
            "--session",
            "file-tools",
            "--json-events",
            "--scope",
        ])
        .arg(format!("{OWNER}/{SCOPE_REF}/{SCOPE_EPOCH}"))
        .args(tail)
        .env_remove("SUBC_MODULE_ID")
        .env_remove("SUBC_LAUNCH_NONCE")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let proof = PROOF
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(proof.as_bytes())
        .await
        .unwrap();
    let output =
        broca_test_support::wait_for("scoped session CLI", child.wait_with_output(), || {
            format!("broca-session did not finish {tail:?}")
        })
        .await
        .unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!text.contains(&proof), "the CLI echoed the private proof");
    assert!(output.status.success(), "{text}");
    output
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scoped_session_send_reads_file_from_scope_workdir_across_episodes() {
    use subc_protocol::manifest::{
        CapabilityDeclarations, Concurrency, ExecutionMode, IdentityScope, ModuleManifest,
        ProviderRole, Tool,
    };

    let model = ReadModel::start().await;
    let Some(rig) = Rig::start_configured(None, None, Some(&model.url)).await else {
        return;
    };
    let checkout = rig.dir.join("stamped-workdir");
    std::fs::create_dir_all(&checkout).unwrap();
    let checkout = checkout.canonicalize().unwrap();
    std::fs::write(checkout.join("hello.txt"), FILE_TEXT).unwrap();
    std::fs::write(rig.dir.join("hello.txt"), "wrong local project root").unwrap();
    rig.sync_scopes(
        json!([{
            "ref":SCOPE_REF, "scope_epoch":SCOPE_EPOCH, "kind":"head", "workdir":checkout,
            "bind_proof_sha256":format!("{:x}", Sha256::digest(PROOF)), "attributes":{},
            "carriers":[
                {"principal":{"kind":"direct"}, "targets":["broca", "aft"]},
                {"principal":{"kind":"reserved", "module_id":"broca"}, "targets":["aft"]}
            ]
        }]),
        1,
    )
    .await;
    let manifest = ModuleManifest::builder("aft", env!("CARGO_PKG_VERSION"))
        .provides(vec![ProviderRole::ToolProvider {
            tools: vec![Tool {
                name: "read".into(),
                description: Some("Read a file".into()),
                execution_mode: ExecutionMode::Pure,
                schema: json!({"type":"object"}),
            }],
            identity_scope: vec![IdentityScope::Project, IdentityScope::Session],
            concurrency: Concurrency::ModuleManaged,
            emits_push: false,
            sub_supervises: false,
        }])
        .capabilities(Some(CapabilityDeclarations {
            provides: vec![subc_protocol::scope::SCOPE_WORKDIR_CAPABILITY.into()],
            requires: vec![],
            must_never_reach: vec![],
        }))
        .build();
    let seen = Arc::new(Seen::default());
    let (_, serve) =
        subc_client_rs::serve_with_handle(&rig.connection_file, manifest, FileTool(seen.clone()))
            .await
            .unwrap();
    let serving = tokio::spawn(serve);
    let items = rig.dir.join("items.json");
    std::fs::write(&items, json!({"tool_items":[{"provider":"aft", "preset":"reader", "params":{}}], "system_text_items":[], "step_transform_items":[]}).to_string()).unwrap();
    let composed = cli(&rig, &["compose-plan", "--items", items.to_str().unwrap()]).await;
    let plan = rig.dir.join("plan.json");
    serde_json::from_slice::<Value>(&composed.stdout)
        .expect("compose-plan must output only the plan");
    std::fs::write(&plan, &composed.stdout).unwrap();
    let owner = rig.connect_as(OWNER, &rig.owner_nonce).await;
    let identity = broca_protocol::BindIdentity::new(&rig.dir, "scope-cli", "file-tools");
    for episode in 1..=2 {
        let sent = cli(
            &rig,
            &[
                "send",
                "--model",
                "mock/mock-model",
                "--prompt",
                "read hello.txt",
                "--plan",
                plan.to_str().unwrap(),
            ],
        )
        .await;
        let send: Value = String::from_utf8_lossy(&sent.stdout)
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .find(|event| event["event"] == "send")
            .unwrap();
        assert_eq!(send["state"], "active", "{send}");
        let route = open_scoped(&owner, &identity).await.unwrap();
        completed_run(&owner, &route, send["run_id"].as_str().unwrap()).await;
        let reads = seen.reads.lock().unwrap();
        assert_eq!(reads.len(), episode);
        let read = reads.last().unwrap();
        assert_eq!(read["bind"]["scope"]["workdir"], json!(checkout));
        assert_eq!(
            read["bind"]["scope"]["owner"],
            json!({"kind":"reserved", "module_id":OWNER})
        );
        assert!(read["bind"]["identity"].get("project_root").is_none());
        assert_eq!(read["answer"]["isError"], false);
        assert_eq!(read["answer"]["content"][0]["text"], FILE_TEXT);
    }
    let read = cli(&rig, &["read"]).await;
    let page: Value = String::from_utf8_lossy(&read.stdout)
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|event| event["event"] == "read_page")
        .unwrap();
    let tools: Vec<_> = page["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| row["message"]["role"] == "tool")
        .collect();
    assert_eq!(tools.len(), 2);
    for tool in tools {
        assert!(tool.to_string().contains(FILE_TEXT), "{tool}");
    }
    serving.abort();
}
