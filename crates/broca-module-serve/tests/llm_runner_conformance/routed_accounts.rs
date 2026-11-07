use super::*;
use cortexkit_role_llm_runner_conformance::RecordingRoutingPeer;
use futures::{SinkExt, StreamExt};

const ROUTER: &str = "conformance-routing";
const VAULT: &str = broca_module_serve::consumer_routes::CREDENTIALS_MODULE_ID;
const MODEL: &str = "conformance-routed-account";

pub(super) fn runner_declaration() -> Value {
    json!({"follows_account_choice":true})
}

pub(super) fn fixture(method: &str) -> Result<Value, HarnessError> {
    let provider = match method {
        "oauth" => "anthropic",
        "chatgpt" | "apikey" => "openai",
        _ => {
            return Err(HarnessError::new(format!(
                "no routed-account fixture for {method}"
            )))
        }
    };
    Ok(
        json!({"provider":provider,"account_id":if method == "apikey" { format!("fixture-credential-{method}") } else { format!("fixture-{method}") },
        "credential_id":format!("fixture-credential-{method}")}),
    )
}

pub(super) fn send_fields(
    session: &str,
    first: bool,
    peer: &RecordingRoutingPeer,
) -> Map<String, Value> {
    let mut fields = json!({"model":{"provider":peer.fixture()["provider"],"model":format!("{PLAN_MODEL_PREFIX}{session}")},
        "auth":{"method":peer.method()},"cache":{"key":session}});
    if peer.method() == "apikey" {
        fields["auth"]["pool"] = json!("fixture-key-pool");
    }
    if first {
        fields["system"] = json!(format!("{SESSION_MARKER}{session}"));
    }
    fields.as_object().unwrap().clone()
}

pub(super) async fn session_live(
    subject: &BrocaSubject,
    main: &Live,
    session: &str,
) -> Result<Option<Arc<Live>>, HarnessError> {
    if !session.contains("routed-account") {
        return Ok(None);
    }
    if !session
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(HarnessError::new(
            "invalid routed-account fixture session name",
        ));
    }
    let mut instances = subject.routed_instances.lock().await;
    if let Some(live) = instances.get(session).and_then(std::sync::Weak::upgrade) {
        return Ok(Some(live));
    }
    // A separate daemon keeps the production vault/routing path real without changing the
    // unrelated mock-provider cases. The session route owns the daemon, not this weak cache.
    let root = main
        .state_root
        .parent()
        .ok_or_else(|| HarnessError::new("runner fixture has no parent"))?
        .join("routed-accounts")
        .join(session);
    let live = Arc::new(subject.start_configured(&root, Some(session)).await?);
    instances.insert(session.into(), Arc::downgrade(&live));
    Ok(Some(live))
}

pub(super) fn seed_catalog(
    state_root: &Path,
    model_url: &str,
    session: &str,
) -> std::io::Result<()> {
    let path = state_root.join("catalog/fusiform-lkg.json");
    let mut value: Value = serde_json::from_slice(&std::fs::read(&path)?)?;
    for (provider, npm) in [
        ("anthropic", "@ai-sdk/anthropic"),
        ("openai", "@ai-sdk/openai"),
    ] {
        value["models"][format!("{provider}/{PLAN_MODEL_PREFIX}{session}")] = json!({
            "existence":"present","limit.context":128000,"capability.tool_call":true,
            "capability.reasoning":false,"capability.attachment":false,
            "wire.npm":{"value":npm,"from":"fixture"},
            "wire.api":{"value":model_url,"from":"fixture"}
        });
        if provider == "openai" {
            // The HTTP stand-in serves Chat Completions SSE, not Responses SSE. ChatGPT
            // still selects its separate Responses family and WebSocket fixture by method.
            value["models"][format!("{provider}/{PLAN_MODEL_PREFIX}{session}")]["wire.shape"] =
                json!({"value":"completions","from":"fixture"});
        }
    }
    std::fs::write(path, serde_json::to_vec(&value)?)
}

pub(super) fn configure(
    root: &Path,
    model_url: &str,
    daemon: &mut Value,
    settings: &mut Value,
) -> Result<(), HarnessError> {
    daemon["modules"]["broca"]["env"]
        .as_object_mut()
        .unwrap()
        .remove("BROCA_PROVIDER_BASE_URL");
    let methods = root.join("routed-auth-methods.json");
    std::fs::write(
        &methods,
        br#"{"defaults":{"anthropic":"oauth","openai":"chatgpt"}}"#,
    )
    .map_err(|error| HarnessError::new(error.to_string()))?;
    daemon["modules"]["broca"]["env"]["BROCA_AUTH_METHODS_PATH"] = json!(methods);
    settings["account_selection"] = json!({"routing_module":ROUTER});
    settings["providers"] = json!({"anthropic":{"base_url":model_url.trim_end_matches("/v1")},
        "openai":{"base_url":model_url}});
    Ok(())
}

pub(super) struct Services(Vec<tokio::task::JoinHandle<()>>);

impl Drop for Services {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

fn manifest(id: &str, ops: &[&str]) -> subc_protocol::manifest::ModuleManifest {
    use subc_protocol::manifest::{
        Concurrency, ManagementOperation, ManagementOperationKind, ProviderRole,
    };
    subc_protocol::manifest::ModuleManifest::builder(id, env!("CARGO_PKG_VERSION"))
        .provides(vec![ProviderRole::ManagementSurface {
            operations: ops
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

async fn register_service<H: subc_client_rs::ModuleHandler + 'static>(
    connection: &Path,
    id: &str,
    ops: &[&str],
    handler: H,
) -> Result<tokio::task::JoinHandle<()>, HarnessError> {
    let (_, serve) = subc_client_rs::serve_with_handle(connection, manifest(id, ops), handler)
        .await
        .map_err(|error| HarnessError::new(format!("register {id}: {error}")))?;
    Ok(tokio::spawn(async move {
        let _ = serve.await;
    }))
}

struct Router {
    shared: Arc<Shared>,
    session: String,
    runner_slot: Value,
    callers: Mutex<HashMap<(u16, u32), String>>,
}

impl Router {
    fn bind(&self, route: (u16, u32), principal: &Option<subc_protocol::Principal>) {
        let mut callers = self.callers.lock().unwrap();
        callers.remove(&route);
        if let Some(subc_protocol::Principal::Reserved { module_id }) = principal {
            callers.insert(route, module_id.clone());
        }
    }

    fn request(&self, route: (u16, u32), request: &Value) -> Result<Value, HarnessError> {
        let runner = self
            .callers
            .lock()
            .unwrap()
            .get(&route)
            .cloned()
            .ok_or_else(|| {
                HarnessError::new("routing fixture requires an authenticated module route")
            })?;
        if runner != "broca" || self.runner_slot["follows_account_choice"] != Value::Bool(true) {
            return Err(HarnessError::new(
                "runner is not declared to follow account choices",
            ));
        }
        if request["method"] == "role.describe" {
            return Ok(
                json!({"result":{"majors":[{"version":"routing/v1","ops":["route.account_for","route.account_observe"],"stability":"alpha"}],
                "implementation_version":"fixture","capabilities":[]}}),
            );
        }
        let peer = self
            .shared
            .routed_account_peers
            .lock()
            .unwrap()
            .get(&self.session)
            .cloned()
            .ok_or_else(|| HarnessError::new("routing peer is not installed"))?;
        let method = request["method"]
            .as_str()
            .ok_or_else(|| HarnessError::new("routing request has no method"))?;
        // Forward the received parameters intact; identity comes from the daemon's bind,
        // never from caller.runner or any other request member.
        peer.request(&runner, method, request["params"].clone())
    }
}

#[async_trait]
impl subc_client_rs::ModuleHandler for Router {
    async fn on_bind(
        &self,
        req: &subc_client_rs::RouteBindRequest,
    ) -> subc_client_rs::BindDecision {
        self.bind((req.handle.channel, req.handle.epoch), &req.principal);
        subc_client_rs::BindDecision::accept()
    }
    async fn handle(
        &self,
        ctx: subc_client_rs::RequestCtx,
        body: Vec<u8>,
    ) -> subc_client_rs::HandlerOutcome {
        let route = ctx.route_handle();
        let result = serde_json::from_slice(&body)
            .map_err(|error| HarnessError::new(error.to_string()))
            .and_then(|request| self.request((route.channel, route.epoch), &request));
        match result {
            Ok(answer) => {
                subc_client_rs::HandlerOutcome::Response(serde_json::to_vec(&answer).unwrap())
            }
            Err(error) => subc_client_rs::HandlerOutcome::Error {
                code: "fixture_routing".into(),
                message: error.to_string(),
            },
        }
    }
}

fn credentials() -> Vec<Value> {
    let mut rows = vec![];
    for method in ["oauth", "chatgpt", "apikey"] {
        let fixture = fixture(method).unwrap();
        for spare in [false, true] {
            let mut row = json!({"id":format!("{}{}",fixture["credential_id"].as_str().unwrap(),if spare {"-spare"} else {""}),
                "categories":["llm-provider"],"type":if method == "apikey" {"apikey"} else {"oauth"},"serves":[fixture["provider"]],
                "provider_ids":[fixture["provider"]],"auth_method":method,"state":"active","record_version":1,
                "operations":["read"],"account_id":format!("{}{}",fixture["account_id"].as_str().unwrap(),if spare {"-spare"} else {""})});
            if method == "apikey" {
                row.as_object_mut().unwrap().remove("account_id");
            }
            rows.push(row);
        }
    }
    rows
}

struct Vault;

fn credential_payload(id: &str) -> Vec<u8> {
    format!("synthetic-fixture-token-{id}").into_bytes()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct HttpRequestEvidence {
    pub authority: String,
    pub target: String,
    pub credential_id: Option<String>,
}

pub(super) fn http_request_evidence(headers: &str) -> HttpRequestEvidence {
    let authorization = headers
        .lines()
        .find_map(|line| line.strip_prefix("authorization:"))
        .map(str::trim);
    let credential_id = credentials().into_iter().find_map(|row| {
        let id = row["id"].as_str()?;
        let expected = format!("bearer {}", String::from_utf8(credential_payload(id)).ok()?);
        (authorization == Some(expected.as_str())).then(|| id.to_owned())
    });
    HttpRequestEvidence {
        authority: headers
            .lines()
            .find_map(|line| line.strip_prefix("host:"))
            .map(str::trim)
            .unwrap_or_default()
            .into(),
        target: headers
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .unwrap_or_default()
            .into(),
        credential_id,
    }
}

impl Vault {
    fn request(request: &Value) -> Result<Value, HarnessError> {
        match request["method"].as_str() {
            Some("credential.list_scoped") => Ok(json!({"result":{"credentials":credentials(),
                "grants":1,"grant_tuples":[],"view":"routed-fixture"}})),
            Some("credential.get_scoped") => {
                let row = credentials()
                    .into_iter()
                    .find(|row| row["id"] == request["params"]["credential_id"])
                    .ok_or_else(|| HarnessError::new("fixture credential not found"))?;
                Ok(
                    json!({"result":{"payload":credential_payload(row["id"].as_str().unwrap()),
                    "expires_at_ms":null,"record_version":1,"credential_id":row["id"],"account_id":row["account_id"]}}),
                )
            }
            _ => Err(HarnessError::new("unsupported fixture vault method")),
        }
    }
}

#[async_trait]
impl subc_client_rs::ModuleHandler for Vault {
    async fn handle(
        &self,
        _: subc_client_rs::RequestCtx,
        body: Vec<u8>,
    ) -> subc_client_rs::HandlerOutcome {
        let result = serde_json::from_slice(&body)
            .map_err(|error| HarnessError::new(error.to_string()))
            .and_then(|request| Self::request(&request));
        match result {
            Ok(answer) => {
                subc_client_rs::HandlerOutcome::Response(serde_json::to_vec(&answer).unwrap())
            }
            Err(error) => subc_client_rs::HandlerOutcome::Error {
                code: "fixture_vault".into(),
                message: error.to_string(),
            },
        }
    }
}

pub(super) async fn register(
    connection: &Path,
    shared: Arc<Shared>,
    session: &str,
) -> Result<Services, HarnessError> {
    let mut services = Services(vec![]);
    services.0.push(
        register_service(
            connection,
            ROUTER,
            &[
                "role.describe",
                "route.account_for",
                "route.account_observe",
            ],
            Router {
                shared,
                session: session.into(),
                runner_slot: runner_declaration(),
                callers: Default::default(),
            },
        )
        .await?,
    );
    services.0.push(
        register_service(
            connection,
            VAULT,
            &["credential.list_scoped", "credential.get_scoped"],
            Vault,
        )
        .await?,
    );
    Ok(services)
}

fn io_error(error: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
}

pub(super) async fn serve_websocket(
    stream: tokio::net::TcpStream,
    shared: Arc<Shared>,
) -> std::io::Result<()> {
    use tokio_tungstenite::tungstenite::Message;
    // The real ChatGPT client offers permessage-deflate; the server must negotiate it.
    let mut config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default();
    config.extensions.permessage_deflate = Some(Default::default());
    let mut socket = tokio_tungstenite::accept_async_with_config(stream, Some(config))
        .await
        .map_err(io_error)?;
    while let Some(frame) = socket.next().await {
        let text = match frame.map_err(io_error)? {
            Message::Text(text) => text,
            Message::Ping(bytes) => {
                socket.send(Message::Pong(bytes)).await.map_err(io_error)?;
                continue;
            }
            Message::Close(_) => break,
            _ => continue,
        };
        let wire: Value = serde_json::from_str(&text).map_err(io_error)?;
        if wire["type"] != "response.create" {
            return Err(io_error("unexpected routed WebSocket request"));
        }
        let response = &wire;
        let session = response["model"]
            .as_str()
            .and_then(|model| model.strip_prefix(PLAN_MODEL_PREFIX))
            .filter(|session| {
                shared
                    .routed_account_peers
                    .lock()
                    .unwrap()
                    .contains_key(*session)
            })
            .map(str::to_owned)
            .or_else(|| {
                response["prompt_cache_key"].as_str().and_then(|key| {
                    shared
                        .routed_account_peers
                        .lock()
                        .unwrap()
                        .keys()
                        .find(|session| {
                            broca_provider::hash::clamp_prompt_cache_key(session) == key
                        })
                        .cloned()
                })
            })
            .or_else(|| {
                let text = wire.to_string();
                let start = text.find(SESSION_MARKER)? + SESSION_MARKER.len();
                let session: String = text[start..]
                    .chars()
                    .take_while(|ch| ch.is_ascii_alphanumeric() || matches!(*ch, '-' | '_'))
                    .collect();
                shared
                    .routed_account_peers
                    .lock()
                    .unwrap()
                    .contains_key(&session)
                    .then_some(session)
            })
            .ok_or_else(|| io_error("unmatched routed WebSocket request"))?;
        let warm = response.get("generate") == Some(&Value::Bool(false));
        let turn_index = shared
            .requests
            .lock()
            .unwrap()
            .get(&session)
            .map_or(0, Vec::len);
        let text = if warm {
            String::new()
        } else {
            let turn = shared
                .scripts
                .lock()
                .unwrap()
                .get(&session)
                .and_then(|script| script.turns.get(turn_index))
                .cloned()
                .ok_or_else(|| io_error("routed request has no scripted turn"))?;
            let mut text = String::new();
            for part in &turn.parts {
                match part {
                    ScriptedPart::Text(part) => text.push_str(part),
                    _ => {
                        return Err(io_error(
                            "routed account fixture requires text-only scripted turns",
                        ))
                    }
                }
            }
            shared
                .raw_bodies
                .lock()
                .unwrap()
                .entry(session.clone())
                .or_default()
                .push(text_bytes(&wire));
            shared
                .requests
                .lock()
                .unwrap()
                .entry(session.clone())
                .or_default()
                .push(response["input"].as_array().cloned().unwrap_or_default());
            shared
                .bodies
                .lock()
                .unwrap()
                .entry(session.clone())
                .or_default()
                .push(wire.clone());
            text
        };
        let id = format!("routed-{session}-{turn_index}");
        let item_id = format!("message-{id}");
        let item = json!({"type":"message","id":item_id,"role":"assistant","status":"completed",
            "content":[{"type":"output_text","text":text,"annotations":[]}]});
        let mut events = vec![
            json!({"type":"response.created","response":{"id":id,"status":"in_progress","output":[]}}),
        ];
        if !warm {
            events.extend([
                json!({"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":item_id,"role":"assistant","status":"in_progress","content":[]}}),
                json!({"type":"response.content_part.added","item_id":item_id,"output_index":0,"content_index":0,"part":{"type":"output_text","text":"","annotations":[]}}),
                json!({"type":"response.output_text.delta","item_id":item_id,"output_index":0,"content_index":0,"delta":text}),
                json!({"type":"response.output_text.done","item_id":item_id,"output_index":0,"content_index":0,"text":text}),
                json!({"type":"response.content_part.done","item_id":item_id,"output_index":0,"content_index":0,"part":{"type":"output_text","text":text,"annotations":[]}}),
                json!({"type":"response.output_item.done","output_index":0,"item":item}),
            ]);
        }
        events.push(json!({"type":"response.completed","response":{"id":id,"status":"completed","model":response["model"],
            "output":if warm {json!([])} else {json!([item])}}}));
        for (sequence, mut event) in events.into_iter().enumerate() {
            event["sequence_number"] = json!(sequence);
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .map_err(io_error)?;
        }
    }
    Ok(())
}

fn text_bytes(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).expect("fixture JSON")
}

#[cfg(test)]
mod tests {
    use super::*;
    use cortexkit_role_llm_runner_conformance::ScriptedTurn;

    /// Exercise the routed-account kit obligations without running unrelated crash cases.
    /// The full kit still uses these same subject hooks and fixture for every listed method.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn routed_accounts_adapter_admits_and_releases_every_declared_method() {
        for (name, env) in [
            ("ck-subc", "SUBC_CORE_BIN"),
            ("fake-aft-stub", "FAKE_AFT_STUB_BIN"),
        ] {
            assert!(sibling_bin(name, env).is_file(), "{name} is required");
        }
        let scratch = broca_test_support::scratch_root().join("routed-accounts-targeted");
        std::fs::create_dir_all(&scratch).unwrap();
        let subject = BrocaSubject::new(&scratch).await;
        let live = subject.start(&scratch.join("main")).await.unwrap();
        let response = |reply| match reply {
            Reply::Response(value) => value,
            other => panic!("expected response, got {other:?}"),
        };
        let declaration = subject.routing_runner_declaration().await.unwrap();
        assert_eq!(declaration["follows_account_choice"], true);
        let discovery = broca_wire::broca_role_describe("targeted");
        let value = serde_json::to_value(discovery).unwrap();
        let checked = cortexkit_role_llm_runner::describe::check_describe(&value).unwrap();
        let methods = checked.routed_accounts.unwrap();
        assert!(methods.iter().any(|method| method == "apikey"));
        for method in methods {
            let fixture = subject.routed_account_fixture(&method).await.unwrap();
            let peer = RecordingRoutingPeer::new(&method, fixture).unwrap();
            let session = format!("routed-account-targeted-{method}");
            subject
                .install_script(
                    &session,
                    Script {
                        turns: vec![ScriptedTurn::new(vec![ScriptedPart::Text(
                            "routed account accepted".into(),
                        )])],
                    },
                )
                .await
                .unwrap();
            let route = subject
                .session_route(&live, &session, &subject.owner_stamp())
                .await
                .unwrap();
            subject
                .install_routing_peer(&session, peer.clone())
                .await
                .unwrap();
            let served = response(route.request("role.describe", json!({})).await.unwrap());
            assert_eq!(served["routed_accounts"], value["routed_accounts"]);
            let mut params = subject.send_fields(&session, true);
            params.insert("prompt".into(), json!("answer briefly"));
            params.insert("send_id".into(), json!(format!("first-{method}")));
            let admitted = response(
                route
                    .request("session.send", Value::Object(params))
                    .await
                    .unwrap(),
            );
            let run_id = admitted["run_id"].as_str().unwrap();
            broca_test_support::wait_until("routed-account completion", || async {
                let result = response(
                    route
                        .request("run.result", json!({"run_id":run_id}))
                        .await
                        .unwrap(),
                );
                if result["state"] == "completed" {
                    Ok(())
                } else {
                    Err(format!("{result}"))
                }
            })
            .await;
            let head = response(route.request("session.head", json!({})).await.unwrap());
            assert_eq!(head["last_run_state"]["run_id"], run_id);
            assert_eq!(head["last_run_state"]["state"], "completed");
            assert!(subject.model_invocations(&session).await.unwrap() > 0);
            broca_test_support::wait_until("routed-account release", || async {
                let calls = peer.calls();
                if calls.iter().any(|call| {
                    call.method == "route.account_observe" && call.body["kind"] == "run_finished"
                }) {
                    Ok(calls)
                } else {
                    Err(format!("calls: {calls:?}"))
                }
            })
            .await;
            let calls = peer.calls();
            let admission = calls
                .iter()
                .find(|call| call.method == "route.account_for")
                .expect("every declared method must admit");
            assert_eq!(admission.body["mode"], "admit");
            assert_eq!(admission.body["provider"], peer.fixture()["provider"]);
            let release = calls
                .iter()
                .find(|call| {
                    call.method == "route.account_observe" && call.body["kind"] == "run_finished"
                })
                .unwrap();
            assert_eq!(release.body["admission_id"], admission.body["admission_id"]);
            assert!(release.body["event_id"].is_string());
        }
    }

    struct PeerTransport(RecordingRoutingPeer);

    #[async_trait]
    impl broca_module_serve::account_routing::RoutingTransport for PeerTransport {
        async fn request(
            &self,
            body: Vec<u8>,
        ) -> broca_module_serve::account_routing::RoutingReply {
            let request: Value =
                serde_json::from_slice(&body).map_err(|error| error.to_string())?;
            let answer = self
                .0
                .request(
                    "broca",
                    request["method"].as_str().unwrap(),
                    request["params"].clone(),
                )
                .map_err(|error| error.to_string())?;
            Ok(Ok(serde_json::to_vec(&answer).unwrap()))
        }
    }

    #[tokio::test]
    async fn routed_accounts_oauth_fixture_completes_catalog_resolved_messages_request() {
        routed_accounts_fixture_completes_catalog_resolved_request(
            "oauth",
            "routed_accounts::tests::routed_accounts_oauth_fixture_completes_catalog_resolved_messages_request",
        )
        .await;
    }

    #[tokio::test]
    async fn routed_accounts_apikey_fixture_completes_catalog_resolved_chat_request() {
        routed_accounts_fixture_completes_catalog_resolved_request(
            "apikey",
            "routed_accounts::tests::routed_accounts_apikey_fixture_completes_catalog_resolved_chat_request",
        )
        .await;
    }

    async fn routed_accounts_fixture_completes_catalog_resolved_request(
        auth: &str,
        test_name: &str,
    ) {
        const CHILD: &str = "BROCA_ROUTED_MESSAGES_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", test_name, "--nocapture"])
                .env(CHILD, "1")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }
        use broca_catalog::live::{AuthMethod, AuthMode, EpisodeRenderConfig, ResolveOptions};
        use broca_module_serve::account_routing::{
            AccountChoice, AccountSelectionConfig, ChoiceInputs, RoutingClient,
        };
        let shared = Arc::new(Shared::default());
        let session = format!("cf-routed-account-portable-{auth}");
        let session = session.as_str();
        let auth_method = AuthMethod::from_token(auth).unwrap();
        shared.scripts.lock().unwrap().insert(
            session.into(),
            Script {
                turns: vec![cortexkit_role_llm_runner_conformance::ScriptedTurn::new(
                    vec![ScriptedPart::Text("routed account accepted".into())],
                )],
            },
        );
        let ids = fixture(auth).unwrap();
        let provider = ids["provider"].as_str().unwrap();
        let peer = RecordingRoutingPeer::new(auth, ids.clone()).unwrap();
        shared
            .routed_account_peers
            .lock()
            .unwrap()
            .insert(session.into(), peer.clone());
        let model_url = start_model(shared.clone()).await;
        let root = broca_test_support::scratch_root()
            .join(format!("routed-native-{auth}-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        seed_structured_output_catalog(&root).unwrap();
        seed_catalog(&root, &model_url, session).unwrap();
        let response =
            serde_json::from_slice(&std::fs::read(root.join("catalog/fusiform-lkg.json")).unwrap())
                .unwrap();
        let cache = Arc::new(broca_catalog::CatalogCache::empty());
        cache
            .install_for_test(response, broca_catalog::CatalogSource::Fusiform)
            .unwrap();
        broca_catalog::install_global_catalog_cache(cache);
        let fields = send_fields(session, true, &peer);
        let model = fields["model"]["model"].as_str().unwrap();
        let binding = broca_protocol::BindIdentity::new("/fixture", "broca", session);
        let static_credential =
            broca_module_serve::auth_routing::CredentialId::new(auth_method, provider, None);
        let listing = serde_json::from_value(
            Vault::request(&json!({"method":"credential.list_scoped"})).unwrap()["result"].clone(),
        )
        .unwrap();
        let client = RoutingClient::new(
            Arc::new(PeerTransport(peer.clone())),
            AccountSelectionConfig::default(),
        );
        let chosen = broca_module_serve::account_routing::choose_account(
            &client,
            ChoiceInputs {
                account_route: None,
                decision_id: None,
                service_tier: None,
                pool: fields["auth"]["pool"].as_str(),
                credential_id: None,
                conversation_key: None,
                inherited_conversation_key: None,
                static_credential: &static_credential,
                caller_named_account: false,
                model,
                session: &binding,
                send_id: Some("fixture-send"),
                run_id: "fixture-run",
                initial_input_bytes: 14,
                hold_cap: Default::default(),
            },
            async { Some(Arc::new(listing)) },
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
        let AccountChoice::Routed {
            credential_id,
            account_id,
            method,
            ..
        } = chosen
        else {
            panic!("fixture did not route its account");
        };
        assert_eq!(credential_id, ids["credential_id"]);
        assert_eq!(account_id, ids["account_id"]);
        assert_eq!(method, auth_method);
        let vault = Vault::request(
            &json!({"method":"credential.get_scoped","params":{"credential_id":credential_id}}),
        )
        .unwrap();
        let token =
            String::from_utf8(serde_json::from_value(vault["result"]["payload"].clone()).unwrap())
                .unwrap();
        let mut daemon = json!({"modules":{"broca":{"env":{}}}});
        let mut settings = json!({});
        configure(&root, &model_url, &mut daemon, &mut settings).unwrap();
        let overrides =
            broca_module_serve::provider_overrides::ProviderOverrides::parse(&settings.to_string())
                .unwrap();
        let base_url = overrides.base_url(provider).unwrap();
        let persona = broca_module_serve::claude_code_identity::fresh_persona(
            "00000000-0000-4000-8000-000000000001",
            Some(&account_id),
            &credential_id,
            &binding,
            None,
            1,
        )
        .unwrap();
        let resolved = broca_catalog::live::resolve_catalog_episode_with_client(
            &format!("{provider}/{model}"),
            Some(2048),
            &AuthMode::Secret(token),
            &ResolveOptions {
                auth_method: Some(method),
                account_id: Some(&account_id),
                frozen_account_ref: Some(&account_id),
                session_id: Some(session),
                claude_code_persona: Some(&persona),
                ..Default::default()
            },
            broca_provider::build_provider_http_client(),
            EpisodeRenderConfig {
                base_url: Some(base_url),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(resolved.auth_method, Some(auth_method));
        assert_eq!(resolved.base_url, base_url);
        let frozen = resolved.provider.render_config_blob();
        assert_eq!(frozen["base_url"], resolved.base_url);
        resolved.provider.bind_run_transport(&binding);
        let options =
            broca_protocol::request::CallOptions::new(vec![broca_protocol::Message::new(
                broca_protocol::Role::User,
                vec![broca_protocol::ContentBlock::Text {
                    text: "answer briefly".into(),
                }],
            )]);
        let mut events = resolved
            .provider
            .step(broca_protocol::StepId(1), &options)
            .await;
        let mut completed = false;
        let mut attempts = 0;
        let mut answered = false;
        while let Some(event) = broca_test_support::wait_for(
            "catalog-selected routed fixture event",
            events.recv(),
            || format!("completed={completed} attempts={attempts}"),
        )
        .await
        {
            match event {
                broca_protocol::NormalizedEvent::Control(
                    broca_protocol::ProviderControl::StepFinished { .. },
                ) => completed = true,
                broca_protocol::NormalizedEvent::Control(
                    broca_protocol::ProviderControl::AssistantMessage { message },
                ) => {
                    let text: String = message
                        .content
                        .iter()
                        .filter_map(|block| match block {
                            broca_protocol::ContentBlock::Text { text } => Some(text.as_str()),
                            _ => None,
                        })
                        .collect();
                    assert_eq!(text, "routed account accepted");
                    answered = true;
                }
                broca_protocol::NormalizedEvent::Control(
                    broca_protocol::ProviderControl::AttemptFinished { attempt },
                ) => {
                    attempts += 1;
                    assert_eq!(attempt.transport.as_deref(), Some("https"));
                    assert_eq!(
                        attempt.outcome,
                        broca_protocol::AttemptOutcome::Complete,
                        "native fixture endpoint did not complete: {:?}; received requests={:?}",
                        attempt.outcome,
                        shared.routed_http_requests.lock().unwrap().get(session)
                    );
                }
                _ => {}
            }
        }
        assert!(completed);
        assert!(answered);
        let (family, family_token, target) = match auth_method {
            AuthMethod::Oauth => (
                broca_provider::spec::WireFamilyId::AnthropicMessages,
                "anthropic_messages",
                "/v1/messages?beta=true",
            ),
            AuthMethod::ApiKey => (
                broca_provider::spec::WireFamilyId::OpenAiChat,
                "openai_chat",
                "/v1/chat/completions",
            ),
            other => panic!("no portable HTTPS fixture for {other:?}"),
        };
        assert_eq!(resolved.family_id, family);
        assert_eq!(frozen["family_id"], family_token);
        assert_eq!(
            attempts, 1,
            "the scripted success must not rely on provider retries"
        );
        assert_eq!(
            shared.routed_http_requests.lock().unwrap()[session],
            vec![HttpRequestEvidence {
                authority: model_url
                    .strip_prefix("http://")
                    .unwrap()
                    .trim_end_matches("/v1")
                    .into(),
                target: target.into(),
                credential_id: Some(credential_id),
            }]
        );
        assert_eq!(shared.requests.lock().unwrap()[session].len(), 1);
        assert_eq!(
            peer.calls()
                .iter()
                .filter(|call| call.method == "route.account_for")
                .count(),
            1
        );
        if let Some(path) = std::env::var_os("BROCA_ROUTED_MESSAGES_EVIDENCE") {
            let requests = shared.routed_http_requests.lock().unwrap();
            let request = &requests[session][0];
            let evidence = json!({"provider":provider,"model":model,"auth_method":auth,
                "family":frozen["family_id"],"frozen_base_url":frozen["base_url"],
                "account_id":account_id,"credential_id":ids["credential_id"],
                "received_authority":request.authority,"received_target":request.target,
                "received_credential_id":request.credential_id,"attempts":attempts,
                "completed":completed,"answered":answered});
            std::fs::write(path, serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn routed_account_catalog_resolves_models_for_actual_native_provider_families() {
        use broca_provider::families::{renderer_version_for, resolve_frozen};
        let root = broca_test_support::scratch_root()
            .join(format!("routed-catalog-admission-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        seed_structured_output_catalog(&root).unwrap();
        seed_catalog(&root, "http://127.0.0.1:7/v1", "native-admission").unwrap();
        let response =
            serde_json::from_slice(&std::fs::read(root.join("catalog/fusiform-lkg.json")).unwrap())
                .unwrap();
        let cache = broca_catalog::CatalogCache::empty();
        cache
            .install_for_test(response, broca_catalog::CatalogSource::Fusiform)
            .unwrap();
        let catalog = cache.snapshot().unwrap();
        for (provider_id, family) in [
            (
                "anthropic",
                broca_provider::spec::WireFamilyId::AnthropicMessages,
            ),
            (
                "openai",
                broca_provider::spec::WireFamilyId::OpenAiResponsesChatgpt,
            ),
        ] {
            let (mut provider, model) = catalog
                .model_for_serving(provider_id, &format!("{PLAN_MODEL_PREFIX}native-admission"))
                .unwrap();
            provider.wire_family = family;
            let frozen = resolve_frozen(&provider, &model, 2048, renderer_version_for(family))
                .unwrap_or_else(|error| panic!("routed model must admit on {family:?}: {error}"));
            assert_eq!(frozen.family_id, family);
            assert_eq!(
                frozen.reasoning,
                broca_provider::policy::ReasoningPolicy::None
            );
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn routed_accounts_fixtures_offer_two_real_candidates_for_each_declared_method() {
        let listing: broca_subc::vault::ScopedList = serde_json::from_value(
            Vault::request(&json!({"method":"credential.list_scoped"})).unwrap()["result"].clone(),
        )
        .unwrap();
        let describe = serde_json::to_value(broca_wire::broca_role_describe("fixtures")).unwrap();
        let checked = cortexkit_role_llm_runner::describe::check_describe(&describe).unwrap();
        for method in checked.routed_accounts.unwrap() {
            let method = method.as_str();
            let ids = fixture(method).unwrap();
            let provider = ids["provider"].as_str().unwrap();
            let auth = broca_catalog::live::AuthMethod::from_token(method).unwrap();
            assert_eq!(ids.as_object().unwrap().len(), 3);
            let candidates = if auth == broca_catalog::live::AuthMethod::ApiKey {
                broca_module_serve::account_routing::Candidates::api_keys(&listing, provider)
            } else {
                broca_module_serve::account_routing::Candidates::from_listing(
                    &listing, auth, provider,
                )
            };
            assert_eq!(candidates.distinct_accounts(), 2);
            assert!(candidates.offers(
                ids["account_id"].as_str().unwrap(),
                ids["credential_id"].as_str().unwrap()
            ));
            let peer = RecordingRoutingPeer::new(method, ids).unwrap();
            let fields = send_fields("session", true, &peer);
            assert_eq!(fields["auth"]["method"], method);
            assert!(fields["auth"].get("account").is_none());
            if method == "apikey" {
                assert_eq!(fields["auth"]["pool"], "fixture-key-pool");
                assert!(listing
                    .credentials
                    .iter()
                    .filter(|row| row.auth_method.as_deref() == Some(method))
                    .all(|row| row.account_id.is_none()));
            }
        }
        assert!(fixture("antigravity").is_err());
        assert_eq!(runner_declaration()["follows_account_choice"], true);
    }

    #[test]
    fn routed_accounts_peer_gets_intact_parameters_and_daemon_attested_caller() {
        let shared = Arc::new(Shared::default());
        let ids = fixture("oauth").unwrap();
        let peer = RecordingRoutingPeer::new("oauth", ids.clone()).unwrap();
        shared
            .routed_account_peers
            .lock()
            .unwrap()
            .insert("session".into(), peer.clone());
        let mut router = Router {
            shared,
            session: "session".into(),
            runner_slot: runner_declaration(),
            callers: Default::default(),
        };
        router.bind(
            (7, 1),
            &Some(subc_protocol::Principal::Reserved {
                module_id: "broca".into(),
            }),
        );
        let params = json!({"provider":ids["provider"],"model":MODEL,"conversation_key":"conversation",
            "admission_id":"admission","caller":{"runner":"spoofed"},"opaque":{"preserved":true}});
        let request = json!({"method":"route.account_for","params":params});
        let answer = router.request((7, 1), &request).unwrap();
        assert_eq!(
            answer["result"]["account"]["credential_id"],
            ids["credential_id"]
        );
        let calls = peer.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].runner, "broca");
        assert_eq!(calls[0].body, params);
        router.bind((7, 1), &None);
        assert!(router.request((7, 1), &request).is_err());
        assert_eq!(peer.calls().len(), 1);
        router.bind(
            (7, 1),
            &Some(subc_protocol::Principal::Reserved {
                module_id: "broca".into(),
            }),
        );
        router.runner_slot["follows_account_choice"] = Value::Bool(false);
        assert!(router.request((7, 1), &request).is_err());
        assert_eq!(peer.calls().len(), 1);
    }

    #[test]
    fn routed_accounts_native_configuration_removes_mock_bypass() {
        let root = broca_test_support::scratch_root()
            .join(format!("routed-configuration-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let mut daemon =
            json!({"modules":{"broca":{"env":{"BROCA_PROVIDER_BASE_URL":"http://wrong.invalid"}}}});
        let mut settings = json!({});
        configure(&root, "http://127.0.0.1:7/v1", &mut daemon, &mut settings).unwrap();
        assert!(daemon["modules"]["broca"]["env"]
            .get("BROCA_PROVIDER_BASE_URL")
            .is_none());
        assert_eq!(settings["account_selection"]["routing_module"], ROUTER);
        assert_eq!(
            settings["providers"]["anthropic"]["base_url"],
            "http://127.0.0.1:7"
        );
        assert_eq!(
            settings["providers"]["openai"]["base_url"],
            "http://127.0.0.1:7/v1"
        );
        let methods: Value =
            serde_json::from_slice(&std::fs::read(root.join("routed-auth-methods.json")).unwrap())
                .unwrap();
        assert_eq!(
            methods["defaults"],
            json!({"anthropic":"oauth","openai":"chatgpt"})
        );
        seed_structured_output_catalog(&root).unwrap();
        seed_catalog(&root, "http://127.0.0.1:7/v1", "native-session").unwrap();
        let catalog: Value =
            serde_json::from_slice(&std::fs::read(root.join("catalog/fusiform-lkg.json")).unwrap())
                .unwrap();
        assert!(catalog["models"]
            .get(format!("anthropic/{PLAN_MODEL_PREFIX}native-session"))
            .is_some());
        assert!(catalog["models"]
            .get(format!("openai/{PLAN_MODEL_PREFIX}native-session"))
            .is_some());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn routed_accounts_websocket_fixture_answers_actual_flat_response_create() {
        use tokio_tungstenite::tungstenite::Message;
        let shared = Arc::new(Shared::default());
        let session = "cf-routed-account-websocket";
        shared.scripts.lock().unwrap().insert(
            session.into(),
            Script {
                turns: vec![cortexkit_role_llm_runner_conformance::ScriptedTurn::new(
                    vec![ScriptedPart::Text("routed fixture text".into())],
                )],
            },
        );
        let peer = RecordingRoutingPeer::new("chatgpt", fixture("chatgpt").unwrap()).unwrap();
        shared
            .routed_account_peers
            .lock()
            .unwrap()
            .insert(session.into(), peer.clone());
        let url = start_model(shared.clone())
            .await
            .replacen("http://", "ws://", 1)
            + "/responses";
        let (mut socket, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        socket
            .send(Message::Text(
                json!({"type":"response.create","model":MODEL,"input":[],
            "prompt_cache_key":broca_provider::hash::clamp_prompt_cache_key(session)})
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
        let mut delta = false;
        loop {
            let frame = broca_test_support::wait_for(
                "routed WebSocket fixture event",
                socket.next(),
                || "waiting for completion".into(),
            )
            .await
            .unwrap()
            .unwrap();
            let Message::Text(text) = frame else {
                continue;
            };
            let value: Value = serde_json::from_str(&text).unwrap();
            if value["type"] == "response.output_text.delta" {
                assert_eq!(value["delta"], "routed fixture text");
                delta = true;
            }
            if value["type"] == "response.completed" {
                assert_eq!(value["response"]["status"], "completed");
                break;
            }
        }
        assert!(delta);
        assert_eq!(shared.requests.lock().unwrap()[session].len(), 1);
        assert!(
            peer.calls().is_empty(),
            "model traffic synthesized a routing call"
        );
        socket.close(None).await.unwrap();
    }

    #[tokio::test]
    async fn routed_accounts_chatgpt_fixture_completes_actual_provider_decoder() {
        let shared = Arc::new(Shared::default());
        let session = "cf-routed-account-provider";
        shared.scripts.lock().unwrap().insert(
            session.into(),
            Script {
                turns: vec![cortexkit_role_llm_runner_conformance::ScriptedTurn::new(
                    vec![ScriptedPart::Text("provider fixture text".into())],
                )],
            },
        );
        let peer = RecordingRoutingPeer::new("chatgpt", fixture("chatgpt").unwrap()).unwrap();
        shared
            .routed_account_peers
            .lock()
            .unwrap()
            .insert(session.into(), peer.clone());
        let url = start_model(shared.clone()).await;
        let mut frozen = broca_provider::families::openai_responses_chatgpt_frozen_config(format!(
            "{PLAN_MODEL_PREFIX}{session}"
        ));
        frozen.base_url = Some(url);
        frozen.prompt_cache_key = Some(broca_provider::hash::clamp_prompt_cache_key(session));
        frozen.chatgpt_wire = Some(broca_provider::chatgpt_wire::ChatgptWire {
            profile: broca_provider::chatgpt_wire::ChatgptWireProfile::Codex0163,
            user_agent: "conformance".into(),
            originator: "codex_cli_rs".into(),
            session_id: session.into(),
            thread_id: session.into(),
            installation_id: "fixture-installation".into(),
            window_id: "fixture-window".into(),
            turn_id: "fixture-turn".into(),
            turn_started_at_unix_ms: 1,
        });
        let provider = broca_provider::live_provider_with_options(
            Some(broca_provider::build_provider_http_client()),
            frozen,
            broca_provider::wire::AuthHandle::None,
            broca_provider::ProviderOptions {
                auth_method: Some("chatgpt".into()),
                ..Default::default()
            },
        )
        .unwrap();
        provider.bind_run_transport(&broca_protocol::BindIdentity::new(
            "/fixture", "broca", session,
        ));
        let options =
            broca_protocol::request::CallOptions::new(vec![broca_protocol::Message::new(
                broca_protocol::Role::User,
                vec![broca_protocol::ContentBlock::Text {
                    text: "answer briefly".into(),
                }],
            )]);
        let mut events = provider.step(broca_protocol::StepId(1), &options).await;
        let mut completed = false;
        let mut websocket = false;
        let mut observed = vec![];
        while let Some(event) =
            broca_test_support::wait_for("ChatGPT fixture provider event", events.recv(), || {
                "waiting for actual provider completion".into()
            })
            .await
        {
            observed.push(format!("{event:?}"));
            match event {
                broca_protocol::NormalizedEvent::Control(
                    broca_protocol::ProviderControl::StepFinished { .. },
                ) => completed = true,
                broca_protocol::NormalizedEvent::Control(
                    broca_protocol::ProviderControl::AttemptFinished { attempt },
                ) => websocket |= attempt.transport.as_deref() == Some("websocket"),
                _ => {}
            }
        }
        assert!(
            completed && websocket,
            "the actual provider did not complete a WebSocket attempt: completed={completed}, websocket={websocket}, fixture_errors={:?}, events={observed:?}",
            shared.evidence.lock().unwrap()
        );
        assert_eq!(shared.requests.lock().unwrap()[session].len(), 1);
        assert!(
            peer.calls().is_empty(),
            "provider traffic synthesized a routing call"
        );
    }
}
