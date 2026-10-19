//! Account selection through the real `ck-subc` daemon: the real `ck-broca` binary, supervised by
//! the daemon, asks a stand-in routing module (`prefrontal-routing`) which provider account each
//! new subscription run uses, and reports each run's admission, attempts and end to it. The
//! credentials vault (`claustrum`) is a stand-in too, registered from inside this process like
//! the routing module, and the Anthropic upstream is a local HTTP server.
//!
//! Each case runs on its own session of one rig, in order, because a rig costs a daemon and a
//! broca boot.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use broca_module_serve::client::{SendOutcome, SendSpec};
use broca_module_serve::SessionClient;
use broca_subc::SubcConnection;
use broca_wal::{FramedRecord, WalRecord};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MODEL: &str = "claude-opus-4-8";
const ROUTING: &str = "prefrontal-routing";
const VAULT: &str = "claustrum";

fn sibling_bin(name: &str, env_key: &str) -> PathBuf {
    broca_test_support::subc_bin(name, env_key)
}

// ---- the stand-in routing module --------------------------------------------------------------

#[derive(Clone)]
enum RoutingMode {
    Disabled,
    Choose {
        account_id: &'static str,
        credential_id: &'static str,
    },
    Refuse {
        code: &'static str,
        retry_after_s: Option<u64>,
    },
    AuthRequired,
}

struct Routing {
    mode: Mutex<RoutingMode>,
    /// Every request, as `{method, params}`.
    requests: Mutex<Vec<Value>>,
    /// Each admission routing chose an account for: its identity and the reply it got.
    admissions: Mutex<std::collections::HashMap<String, (Value, Value)>>,
}

/// What makes two `route.account_for` requests the same admission to routing, as its deployed
/// fingerprint reads them (prefrontal 3beea1862, `account_selection.rs` `fingerprint`): the
/// provider, model and affinity model (trimmed and lowercased), the conversation key, whether
/// the caller is broca, the mode, the expected pin and the requested or preferred account.
/// Candidates, the vault digest and the size estimate are left out, so a retry of the same send
/// after a vault re-list is still the same admission.
fn admission_identity(params: &Value) -> Value {
    let normalized = |value: &Value| {
        value
            .as_str()
            .map(|text| text.trim().to_ascii_lowercase())
            .unwrap_or_default()
    };
    let affinity = if params["affinity_model"].is_string() {
        &params["affinity_model"]
    } else {
        &params["model"]
    };
    json!({
        "provider": normalized(&params["provider"]),
        "model": normalized(&params["model"]),
        "affinity_model": normalized(affinity),
        "conversation_key": params["conversation_key"],
        "broca": params["caller"]["runner"]
            .as_str()
            .is_some_and(|runner| runner.eq_ignore_ascii_case("broca")),
        "mode": params.get("mode").cloned().unwrap_or(json!("admit")),
        "expected_pin": params["expected_pin"],
        "requested_account": params["requested_account"],
        "preferred_existing_account": params["preferred_existing_account"],
    })
}

impl Routing {
    fn set(&self, mode: RoutingMode) {
        *self.mode.lock().unwrap() = mode;
    }

    /// Every request naming `admission_id`, as `(method, kind, params)`.
    fn for_admission(&self, admission_id: &str) -> Vec<(String, String, Value)> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request["params"]["admission_id"] == admission_id)
            .map(|request| {
                (
                    request["method"].as_str().unwrap_or_default().to_string(),
                    request["params"]["kind"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string(),
                    request["params"].clone(),
                )
            })
            .collect()
    }

    fn account_for_count(&self, admission_id: &str) -> usize {
        self.for_admission(admission_id)
            .iter()
            .filter(|(method, ..)| method == "route.account_for")
            .count()
    }
}

fn disabled() -> subc_client_rs::HandlerOutcome {
    subc_client_rs::HandlerOutcome::ErrorWithDetail {
        code: "account_selection_disabled".into(),
        message: "account selection is disabled in routing policy".into(),
        detail: json!({ "code": "account_selection_disabled" }),
    }
}

/// The routing stand-in as the module SDK serves it.
struct RoutingModule(Arc<Routing>);

impl std::ops::Deref for RoutingModule {
    type Target = Routing;
    fn deref(&self) -> &Routing {
        &self.0
    }
}

#[async_trait]
impl subc_client_rs::ModuleHandler for RoutingModule {
    async fn handle(
        &self,
        _ctx: subc_client_rs::RequestCtx,
        body: Vec<u8>,
    ) -> subc_client_rs::HandlerOutcome {
        let request: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        self.requests.lock().unwrap().push(request.clone());
        if request["method"] != "route.account_for" {
            // Serving is off: reports are refused, as routing refuses them today.
            return disabled();
        }
        let admission = request["params"]["admission_id"].clone();
        match self.mode.lock().unwrap().clone() {
            RoutingMode::Disabled => disabled(),
            RoutingMode::Choose {
                account_id,
                credential_id,
            } => {
                // A repeated admission id replays its committed decision when the identity
                // matches and is a conflict when it does not, as routing does.
                let identity = admission_identity(&request["params"]);
                let key = admission.as_str().unwrap_or_default().to_string();
                if let Some((stored, reply)) = self.admissions.lock().unwrap().get(&key) {
                    return if *stored == identity {
                        subc_client_rs::HandlerOutcome::Response(serde_json::to_vec(reply).unwrap())
                    } else {
                        subc_client_rs::HandlerOutcome::ErrorWithDetail {
                            code: "admission_conflict".into(),
                            message: "decided with different parameters".into(),
                            detail: json!({ "code": "admission_conflict", "retry_after_s": null,
                                "decision_id": "acd-e2e", "exclusions": [] }),
                        }
                    };
                }
                let reply = json!({ "result": {
                    "admission": { "id": admission, "state": "reserved" },
                    "account": {
                        "provider": "anthropic",
                        "account_id": account_id,
                        "credential_id": credential_id,
                        "refresh_adapter": "anthropic",
                        "method": "oauth",
                    },
                    "pin": { "id": "pin-e2e", "generation": 1, "mode": "run_frozen" },
                    "reason": { "code": "cold_best", "decision_id": "acd-e2e", "policy_version": 2 },
                }});
                self.admissions
                    .lock()
                    .unwrap()
                    .insert(key, (identity, reply.clone()));
                subc_client_rs::HandlerOutcome::Response(serde_json::to_vec(&reply).unwrap())
            }
            RoutingMode::Refuse {
                code,
                retry_after_s,
            } => subc_client_rs::HandlerOutcome::ErrorWithDetail {
                code: code.into(),
                message: format!("no account admitted: {code}"),
                detail: json!({
                    "code": code,
                    "retry_after_s": retry_after_s,
                    "decision_id": "acd-refused",
                    "exclusions": [],
                }),
            },
            RoutingMode::AuthRequired => subc_client_rs::HandlerOutcome::ErrorWithDetail {
                code: "auth_required".into(),
                message: "every candidate needs re-login".into(),
                detail: json!({ "code": "auth_required", "decision_id": "acd-login", "exclusions": [] }),
            },
        }
    }

    async fn on_bind(
        &self,
        _req: &subc_client_rs::RouteBindRequest,
    ) -> subc_client_rs::BindDecision {
        subc_client_rs::BindDecision::accept()
    }
}

// ---- the stand-in credentials vault -----------------------------------------------------------

struct Vault {
    requests: Mutex<Vec<(String, Value)>>,
    /// Credential ids whose scoped reads answer `not_found`.
    not_found: Mutex<HashSet<String>>,
    /// Optional account identities returned by scoped reads, enabled only by each owning rig.
    account_ids: Mutex<std::collections::HashMap<String, String>>,
    /// Once set, the listing has dropped `oauth:anthropic:work` and carries a new digest.
    relisted: std::sync::atomic::AtomicBool,
    /// Once set, the listing also carries the labelled Anthropic API key `apikey:anthropic:work`.
    anthropic_apikey_listed: std::sync::atomic::AtomicBool,
    /// Once set, the listing also carries the OpenAI API key `apikey:openai`.
    openai_apikey_listed: std::sync::atomic::AtomicBool,
    /// Once set, the listing also carries the OpenRouter API key `apikey:openrouter`.
    openrouter_listed: std::sync::atomic::AtomicBool,
}

impl Vault {
    fn scoped_reads(&self) -> Vec<String> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(method, _)| method == "credential.get_scoped")
            .map(|(_, params)| {
                params["credential_id"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string()
            })
            .collect()
    }

    fn list_calls(&self) -> usize {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(method, _)| method == "credential.list_scoped")
            .count()
    }
}

fn listed(id: &str, account_id: &str) -> Value {
    json!({
        "id": id,
        "categories": ["anthropic-native"],
        "type": "oauth",
        "serves": ["anthropic"],
        "refresh_adapter": "anthropic",
        "state": "active",
        "record_version": 1,
        "operations": ["read"],
        "account_id": account_id,
    })
}

/// The vault stand-in as the module SDK serves it.
struct VaultModule(Arc<Vault>);

impl std::ops::Deref for VaultModule {
    type Target = Vault;
    fn deref(&self) -> &Vault {
        &self.0
    }
}

#[async_trait]
impl subc_client_rs::ModuleHandler for VaultModule {
    async fn handle(
        &self,
        _ctx: subc_client_rs::RequestCtx,
        body: Vec<u8>,
    ) -> subc_client_rs::HandlerOutcome {
        let request: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        let method = request["method"].as_str().unwrap_or_default().to_string();
        let params = request["params"].clone();
        self.requests
            .lock()
            .unwrap()
            .push((method.clone(), params.clone()));
        let result = match method.as_str() {
            "credential.list_scoped" => {
                let mut rows = vec![
                    listed("oauth:anthropic", "acct-a"),
                    listed("oauth:anthropic:acct-b", "acct-b"),
                    listed("oauth:anthropic:spare", "acct-s"),
                    json!({
                        "id": "apikey:xai",
                        "categories": ["llm-provider"],
                        "type": "apikey",
                        "serves": ["xai"],
                        "state": "active",
                        "record_version": 1,
                        "operations": ["read"],
                        "account_id": "acct-x",
                    }),
                    json!({
                        "id": "antigravity:google",
                        "categories": ["llm-provider"],
                        "type": "oauth",
                        "serves": ["google"],
                        "auth_method": "antigravity",
                        "state": "active",
                        "record_version": 1,
                        "operations": ["read"],
                        "account_id": "acct-g",
                    }),
                ];
                if self
                    .openai_apikey_listed
                    .load(std::sync::atomic::Ordering::SeqCst)
                {
                    rows.push(json!({
                        "id": "apikey:openai",
                        "categories": ["llm-provider"],
                        "type": "apikey",
                        "serves": ["openai"],
                        "state": "active",
                        "record_version": 1,
                        "operations": ["read"],
                    }));
                }
                if self
                    .anthropic_apikey_listed
                    .load(std::sync::atomic::Ordering::SeqCst)
                {
                    rows.push(json!({
                        "id": "apikey:anthropic:work",
                        "categories": ["llm-provider"],
                        "type": "apikey",
                        "serves": ["anthropic"],
                        "state": "active",
                        "record_version": 1,
                        "operations": ["read"],
                    }));
                }
                let relisted = self.relisted.load(std::sync::atomic::Ordering::SeqCst);
                if self
                    .openrouter_listed
                    .load(std::sync::atomic::Ordering::SeqCst)
                {
                    rows.push(json!({
                        "id": "apikey:openrouter",
                        "categories": ["llm-provider"],
                        "type": "apikey",
                        "serves": ["openrouter"],
                        "state": "active",
                        "record_version": 1,
                        "operations": ["read"],
                    }));
                }
                if !relisted {
                    rows.push(listed("oauth:anthropic:work", "acct-w"));
                }
                json!({
                    "credentials": rows,
                    "grants": 1,
                    "grant_tuples": [],
                    "view": if relisted { "vault-view-relisted" } else { "vault-view-e2e" },
                })
            }
            "credential.get_scoped" => {
                let id = params["credential_id"].as_str().unwrap_or_default();
                if self.not_found.lock().unwrap().contains(id) {
                    json!({ "error": { "class": "permanent", "code": "not_found" } })
                } else {
                    let mut reply = json!({
                        "payload": format!("token-{id}").into_bytes(),
                        "expires_at_ms": null,
                        "record_version": 1,
                        "credential_id": id,
                    });
                    if let Some(account_id) = self.account_ids.lock().unwrap().get(id) {
                        reply["account_id"] = json!(account_id);
                    }
                    if id == "antigravity:google" {
                        reply["project_id"] = json!("fixture-project");
                    }
                    reply
                }
            }
            _ => json!({}),
        };
        subc_client_rs::HandlerOutcome::Response(
            serde_json::to_vec(&json!({ "result": result })).unwrap(),
        )
    }

    async fn on_bind(
        &self,
        _req: &subc_client_rs::RouteBindRequest,
    ) -> subc_client_rs::BindDecision {
        subc_client_rs::BindDecision::accept()
    }
}

fn management_manifest(id: &str, ops: &[&str]) -> subc_protocol::manifest::ModuleManifest {
    use subc_protocol::manifest::{
        Concurrency, ManagementOperation, ManagementOperationKind, ProviderRole,
    };
    subc_protocol::manifest::ModuleManifest::builder(id, env!("CARGO_PKG_VERSION"))
        .provides(vec![ProviderRole::ManagementSurface {
            operations: ops
                .iter()
                .map(|name| ManagementOperation {
                    name: name.to_string(),
                    kind: ManagementOperationKind::Mutate,
                    description: None,
                })
                .collect(),
            config_schema: json!({}),
            observability: Vec::new(),
            identity_scope: Vec::new(),
            concurrency: Concurrency::ModuleManaged,
        }])
        .build()
}

// ---- the Anthropic upstream -------------------------------------------------------------------

/// The rate-limit headers every response carries: a known one, an unknown one with an odd
/// value, and a header outside the family that must not be reported.
const UPSTREAM_HEADERS: &[(&str, &str)] = &[
    ("anthropic-ratelimit-unified-5h-utilization", "0.4200"),
    ("anthropic-ratelimit-unified-some-future-window", "x;q=1, 2"),
    ("anthropic-ratelimit-requests-remaining", "5"),
];

/// Anthropic's out-of-credit refusal for an API key. ASSUMED, NOT CAPTURED: built from Anthropic's
/// public error shape because no real refusal has been recorded yet. Replace it, and regenerate
/// `run_status.billing_exhausted_anthropic_apikey.json`, from the first real refusal.
/// OpenAI's refusal of a structured-output schema. RECONSTRUCTED, NOT CAPTURED: production
/// recorded the HTTP 400 with `error.type` `invalid_request_error`, `error.code`
/// `invalid_json_schema` and `error.param` `text.format.schema`; the message text is written for
/// the test in OpenAI's usual wording. Replace it, and regenerate
/// `run_status.request_invalid_openai.json`, from a captured body.
const OPENAI_INVALID_SCHEMA_BODY: &str = r#"{"error":{"message":"Invalid schema for response_format 'result': In context=(), 'required' is required to be supplied and to be an array including every key in properties. Missing 'reason'.","type":"invalid_request_error","param":"text.format.schema","code":"invalid_json_schema"}}"#;

const ANTHROPIC_CREDIT_BALANCE_BODY: &str = r#"{"type":"error","error":{"type":"invalid_request_error","message":"Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits."}}"#;

/// OpenRouter's 402 for a request its credit cannot pay for. RECONSTRUCTED from a production
/// refusal: the message is verbatim, but the production `metadata` held more fields than the one
/// kept here, and those were not recorded.
const OPENROUTER_CREDIT_402_BODY: &str = r#"{"error":{"message":"This request requires more credits, or fewer max_tokens. You requested up to 128000 tokens, but can only afford 97335. To increase, visit https://openrouter.ai/settings/credits and add more credits","code":402,"metadata":{"limit_source":"openrouter_credits"}}}"#;

fn reported_headers() -> Value {
    json!({
        "anthropic-ratelimit-unified-5h-utilization": "0.4200",
        "anthropic-ratelimit-unified-some-future-window": "x;q=1, 2",
    })
}

fn anthropic_sse(text: &str) -> Vec<u8> {
    let events = [
        ("message_start", r#"{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","content":[],"model":"m","usage":{"input_tokens":3,"output_tokens":0}}}"#.to_string()),
        ("content_block_start", r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#.to_string()),
        ("content_block_delta", json!({"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": text}}).to_string()),
        ("content_block_stop", r#"{"type":"content_block_stop","index":0}"#.to_string()),
        ("message_delta", r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}"#.to_string()),
        ("message_stop", r#"{"type":"message_stop"}"#.to_string()),
    ];
    let mut body = String::new();
    for (event, data) in events {
        body.push_str(&format!("event: {event}\ndata: {data}\n\n"));
    }
    body.into_bytes()
}

fn output_ceiling_sse() -> Vec<u8> {
    let events = [
        json!({"type":"message_start","message":{"id":"msg_cut","usage":{"input_tokens":3,"output_tokens":0}}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"tool_1","name":"write_file","input":{}}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"content\":\"PRIVATE-TOOL-CONTENT"}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"message_delta","delta":{"stop_reason":"max_tokens"},"usage":{"output_tokens":8192}}),
        json!({"type":"message_stop"}),
    ];
    events
        .iter()
        .map(|event| format!("data: {event}\n\n"))
        .collect::<String>()
        .into_bytes()
}

#[derive(Default)]
struct Upstream {
    /// Whether each request's authorization carried the token of each credential, by id.
    /// Booleans only: a token is never kept or printed.
    authorized_as: Mutex<Vec<String>>,
    /// While set, requests are held unanswered.
    hold: std::sync::atomic::AtomicBool,
    held: std::sync::atomic::AtomicUsize,
    reject_auth: std::sync::atomic::AtomicBool,
    large_reply: std::sync::atomic::AtomicBool,
    output_ceiling: std::sync::atomic::AtomicBool,
    context_overflow: std::sync::atomic::AtomicBool,
    billing_exhausted_xai: std::sync::atomic::AtomicBool,
    account_limit_exhausted_code_assist: std::sync::atomic::AtomicBool,
    /// Answer Anthropic's out-of-credit 400 (its public shape; no real refusal is captured).
    credit_exhausted_anthropic: std::sync::atomic::AtomicBool,
    /// Answer OpenAI's request-validation 400 for a rejected structured-output schema.
    request_invalid_openai: std::sync::atomic::AtomicBool,
    grammar_limit_anthropic: std::sync::atomic::AtomicBool,
    schema_requests: Mutex<Vec<Value>>,
    /// Answer OpenRouter's 402 for an unaffordable request ([`OPENROUTER_CREDIT_402_BODY`]).
    credit_insufficient_openrouter: std::sync::atomic::AtomicBool,
    /// Answer an ordinary Anthropic rate-limit 429 with `retry-after: 0`, so the transport spends
    /// its whole in-step attempt budget without waiting.
    rate_limited_anthropic: std::sync::atomic::AtomicBool,
}

const CREDENTIALS: &[&str] = &[
    "oauth:anthropic",
    "oauth:anthropic:acct-b",
    "oauth:anthropic:work",
    "oauth:anthropic:spare",
    "apikey:xai",
    "antigravity:google",
    "apikey:anthropic:work",
    "apikey:openai",
    "apikey:openrouter",
];

async fn serve_upstream(upstream: Arc<Upstream>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let upstream = Arc::clone(&upstream);
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut chunk = [0u8; 8192];
                let header_end = loop {
                    let Ok(read) = socket.read(&mut chunk).await else {
                        return;
                    };
                    if read == 0 {
                        return;
                    }
                    request.extend_from_slice(&chunk[..read]);
                    if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                        break end + 4;
                    }
                };
                let head = String::from_utf8_lossy(&request[..header_end]).to_string();
                let length = head
                    .to_ascii_lowercase()
                    .lines()
                    .find_map(|line| {
                        line.strip_prefix("content-length:")
                            .map(str::trim)
                            .map(str::to_string)
                    })
                    .and_then(|value| value.parse::<usize>().ok())
                    .unwrap_or(0);
                while request.len() < header_end + length {
                    let Ok(read) = socket.read(&mut chunk).await else {
                        return;
                    };
                    if read == 0 {
                        return;
                    }
                    request.extend_from_slice(&chunk[..read]);
                }
                let header = |wanted: &str| {
                    head.lines().find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case(wanted)
                            .then(|| value.trim().to_string())
                    })
                };
                let authorization = header("authorization");
                // Anthropic API keys travel in `x-api-key`, not as a bearer token.
                let api_key = header("x-api-key");
                let as_credential = CREDENTIALS
                    .iter()
                    .find(|id| {
                        authorization.as_deref() == Some(format!("Bearer token-{id}").as_str())
                            || api_key.as_deref() == Some(format!("token-{id}").as_str())
                    })
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "unknown".into());
                upstream.authorized_as.lock().unwrap().push(as_credential);
                if upstream
                    .grammar_limit_anthropic
                    .load(std::sync::atomic::Ordering::SeqCst)
                {
                    let body: Value =
                        serde_json::from_slice(&request[header_end..header_end + length]).unwrap();
                    let strict = body.pointer("/output_config/format").is_some()
                        || body["tools"]
                            .as_array()
                            .is_some_and(|tools| tools.iter().any(|tool| tool["strict"] == true));
                    upstream.schema_requests.lock().unwrap().push(body);
                    let (status, content_type, body) = if strict {
                        ("400 Bad Request", "application/json", br#"{"type":"error","error":{"type":"invalid_request_error","message":"The compiled grammar is too large, which would cause performance issues."}}"#.to_vec())
                    } else {
                        (
                            "200 OK",
                            "text/event-stream",
                            anthropic_sse(r#"{"answer":"ok"}"#),
                        )
                    };
                    let reply = format!("HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n", body.len());
                    socket.write_all(reply.as_bytes()).await.unwrap();
                    socket.write_all(&body).await.unwrap();
                    socket.shutdown().await.unwrap();
                    return;
                }
                if upstream.hold.load(std::sync::atomic::Ordering::SeqCst) {
                    upstream
                        .held
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let mut byte = [0u8; 1];
                    while matches!(socket.read(&mut byte).await, Ok(n) if n > 0) {}
                    return;
                }
                if upstream
                    .reject_auth
                    .load(std::sync::atomic::Ordering::SeqCst)
                {
                    let body = br#"{"type":"error","error":{"type":"authentication_error","message":"credential rejected"}}"#;
                    let reply = format!("HTTP/1.1 401 Unauthorized\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n", body.len());
                    socket.write_all(reply.as_bytes()).await.unwrap();
                    socket.write_all(body).await.unwrap();
                    socket.shutdown().await.unwrap();
                    return;
                }
                if upstream
                    .billing_exhausted_xai
                    .load(std::sync::atomic::Ordering::SeqCst)
                {
                    let body = br#"{"code":"personal-team-blocked:spending-limit","error":"You have run out of credits or need a Grok subscription..."}"#;
                    let reply = format!("HTTP/1.1 403 Forbidden\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n", body.len());
                    socket.write_all(reply.as_bytes()).await.unwrap();
                    socket.write_all(body).await.unwrap();
                    socket.shutdown().await.unwrap();
                    return;
                }
                if upstream
                    .account_limit_exhausted_code_assist
                    .load(std::sync::atomic::Ordering::SeqCst)
                {
                    let body = br#"{"error":{"code":429,"message":"Individual quota reached. Please upgrade your subscription to increase your limits. Resets in 81h48m9s.","status":"RESOURCE_EXHAUSTED","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"QUOTA_EXHAUSTED","domain":"cloudcode-pa.googleapis.com","metadata":{"uiMessage":"true","model":"gemini-3.8-flash-high","quotaResetDelay":"81h48m9.198441001s"}}]}}"#;
                    let reply = format!("HTTP/1.1 429 Too Many Requests\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n", body.len());
                    socket.write_all(reply.as_bytes()).await.unwrap();
                    socket.write_all(body).await.unwrap();
                    socket.shutdown().await.unwrap();
                    return;
                }
                if upstream
                    .credit_exhausted_anthropic
                    .load(std::sync::atomic::Ordering::SeqCst)
                {
                    let body = ANTHROPIC_CREDIT_BALANCE_BODY.as_bytes();
                    let reply = format!("HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n", body.len());
                    socket.write_all(reply.as_bytes()).await.unwrap();
                    socket.write_all(body).await.unwrap();
                    socket.shutdown().await.unwrap();
                    return;
                }
                if upstream
                    .request_invalid_openai
                    .load(std::sync::atomic::Ordering::SeqCst)
                {
                    let body = OPENAI_INVALID_SCHEMA_BODY.as_bytes();
                    let reply = format!("HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n", body.len());
                    socket.write_all(reply.as_bytes()).await.unwrap();
                    socket.write_all(body).await.unwrap();
                    socket.shutdown().await.unwrap();
                    return;
                }
                if upstream
                    .credit_insufficient_openrouter
                    .load(std::sync::atomic::Ordering::SeqCst)
                {
                    let body = OPENROUTER_CREDIT_402_BODY.as_bytes();
                    let reply = format!("HTTP/1.1 402 Payment Required\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n", body.len());
                    socket.write_all(reply.as_bytes()).await.unwrap();
                    socket.write_all(body).await.unwrap();
                    socket.shutdown().await.unwrap();
                    return;
                }
                if upstream
                    .rate_limited_anthropic
                    .load(std::sync::atomic::Ordering::SeqCst)
                {
                    let body = br#"{"type":"error","error":{"type":"rate_limit_error","message":"Number of request tokens has exceeded your per-minute rate limit"}}"#;
                    let reply = format!("HTTP/1.1 429 Too Many Requests\r\ncontent-type: application/json\r\nretry-after: 0\r\ncontent-length: {}\r\nconnection: close\r\n\r\n", body.len());
                    let _ = socket.write_all(reply.as_bytes()).await;
                    let _ = socket.write_all(body).await;
                    let _ = socket.shutdown().await;
                    return;
                }
                if upstream
                    .context_overflow
                    .load(std::sync::atomic::Ordering::SeqCst)
                {
                    let body = br#"{"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long: 1002096 tokens > 1000000 maximum"}}"#;
                    let reply = format!("HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n", body.len());
                    socket.write_all(reply.as_bytes()).await.unwrap();
                    socket.write_all(body).await.unwrap();
                    socket.shutdown().await.unwrap();
                    return;
                }
                let body = if upstream
                    .output_ceiling
                    .load(std::sync::atomic::Ordering::SeqCst)
                {
                    output_ceiling_sse()
                } else if upstream
                    .large_reply
                    .load(std::sync::atomic::Ordering::SeqCst)
                {
                    anthropic_sse(&"x".repeat(65536))
                } else {
                    anthropic_sse("hello")
                };
                let mut reply = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n",
                    body.len()
                );
                for (name, value) in UPSTREAM_HEADERS {
                    reply.push_str(&format!("{name}: {value}\r\n"));
                }
                reply.push_str("\r\n");
                let _ = socket.write_all(reply.as_bytes()).await;
                let _ = socket.write_all(&body).await;
                let _ = socket.shutdown().await;
            });
        }
    });
    url
}

// ---- the rig -----------------------------------------------------------------------------------

struct Rig {
    daemon: broca_test_support::RigChild,
    connection_file: PathBuf,
    state_root: PathBuf,
    project_root: PathBuf,
    root: PathBuf,
    routing: Arc<Routing>,
    routing_task: Option<tokio::task::JoinHandle<()>>,
    vault: Arc<Vault>,
    vault_task: tokio::task::JoinHandle<()>,
    upstream: Arc<Upstream>,
}

impl Drop for Rig {
    fn drop(&mut self) {
        if let Some(task) = self.routing_task.take() {
            task.abort();
        }
        self.vault_task.abort();
        // The test removes the rig directory after dropping the rig, and the daemon's modules
        // write into it. The tree kill stops the daemon before listing and killing its
        // processes, so it cannot respawn a module in between.
        broca_test_support::kill_and_reap_tree(&mut self.daemon);
    }
}

#[cfg(unix)]
fn children(pid: u32) -> Vec<u32> {
    std::process::Command::new("pgrep")
        .args(["-P", &pid.to_string()])
        .output()
        .map(|output| {
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .filter_map(|line| line.trim().parse().ok())
                .collect()
        })
        .unwrap_or_default()
}

async fn register<H>(
    connection_file: &Path,
    id: &str,
    ops: &[&str],
    handler: H,
) -> tokio::task::JoinHandle<()>
where
    H: subc_client_rs::ModuleHandler + 'static,
{
    let (_, serve) =
        subc_client_rs::serve_with_handle(connection_file, management_manifest(id, ops), handler)
            .await
            .unwrap_or_else(|error| panic!("register the {id} stand-in: {error}"));
    tokio::spawn(async move {
        let _ = serve.await;
    })
}

impl Rig {
    async fn start() -> Option<Rig> {
        let daemon_bin = sibling_bin("ck-subc", "SUBC_CORE_BIN");
        if !daemon_bin.exists() {
            assert!(
                std::env::var_os("BROCA_REQUIRE_SUBC").is_none_or(|value| value.is_empty()),
                "account_selection_real_daemon: {daemon_bin:?} is missing, but BROCA_REQUIRE_SUBC \
                 requires it"
            );
            println!("SKIP account_selection_real_daemon: {daemon_bin:?} is missing");
            return None;
        }
        let daemon_bin =
            broca_test_support::ckdev_binary(&daemon_bin, broca_test_support::scratch_root());
        std::env::remove_var("SUBC_MODULE_ID");
        std::env::remove_var("SUBC_LAUNCH_NONCE");
        static NEXT_RIG: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "broca-account-selection-{}-{}",
            std::process::id(),
            NEXT_RIG.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        ));
        broca_test_support::remove_rig_dir(&root);
        let config_home = root.join("config");
        let runtime_dir = root.join("runtime");
        let data_home = root.join("data-home");
        let state_root = root.join("broca-state");
        let project_root = root.join("project");
        for dir in [
            &runtime_dir,
            &data_home,
            &project_root,
            &state_root.join("catalog"),
        ] {
            std::fs::create_dir_all(dir).unwrap();
        }
        // ck-broca carries no compiled-in model catalog. Seed the catalog file it saves and
        // falls back to when the catalog module is unreachable, as on a cold start.
        std::fs::write(
            state_root.join("catalog").join("fusiform-lkg.json"),
            serde_json::to_vec_pretty(&json!({
                "catalog_version": 1,
                "models": {
                    format!("anthropic/{MODEL}"): {
                        "capability.structured_output": true,
                        "capability.attachment": false,
                        "capability.reasoning": false,
                        "capability.tool_call": true,
                        "existence": "present",
                        "limit.context": 200000,
                        "limit.output": 8192,
                    },
                    "xai/grok-4.7": {
                        "capability.attachment": false,
                        "capability.reasoning": false,
                        "capability.tool_call": true,
                        "existence": "present",
                        "limit.context": 131072,
                        "limit.output": 4096,
                    },
                    "openai/gpt-5.5": {
                        "capability.attachment": false,
                        "capability.reasoning": false,
                        "capability.tool_call": true,
                        "existence": "present",
                        "limit.context": 400000,
                        "limit.output": 8192,
                    },
                    "openrouter/openai/gpt-4.1": {
                        "capability.attachment": false,
                        "capability.reasoning": false,
                        "capability.tool_call": true,
                        "existence": "present",
                        "limit.context": 1047576,
                        "limit.output": 32768,
                    },
                    "google/gemini-3.8-flash-high": {
                        "capability.attachment": false,
                        "capability.reasoning": true,
                        "capability.tool_call": true,
                        "existence": "present",
                        "limit.context": 1000000,
                        "limit.output": 8192,
                    },
                },
                "resolved_at_ms": 1,
                "source": "models.dev",
            }))
            .unwrap(),
        )
        .unwrap();
        let upstream = Arc::new(Upstream::default());
        let upstream_url = serve_upstream(Arc::clone(&upstream)).await;
        let broca_config = root.join("broca.jsonc");
        std::fs::write(
            &broca_config,
            json!({
                "providers": {
                    "anthropic": { "base_url": upstream_url },
                    "xai": { "base_url": upstream_url },
                    "openrouter": { "base_url": upstream_url },
                    "google": { "base_url": upstream_url },
                    "openai": { "base_url": upstream_url },
                }
            })
            .to_string(),
        )
        .unwrap();
        let methods = root.join("auth-methods.json");
        std::fs::write(
            &methods,
            br#"{"defaults":{"anthropic":"oauth","xai":"apikey","google":"antigravity","openai":"apikey","openrouter":"apikey"}}"#,
        )
        .unwrap();
        let config_path = config_home.join("cortexkit").join("subc.jsonc");
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::write(
            &config_path,
            serde_json::to_vec_pretty(&json!({
                "version": 1,
                "modules": {
                    "broca": {
                        "program": broca_test_support::ckdev_binary(
                            env!("CARGO_BIN_EXE_ck-broca"),
                            broca_test_support::scratch_root(),
                        ),
                        "args": [],
                        "env": {
                            "BROCA_STATE_ROOT": state_root,
                            "XDG_DATA_HOME": data_home,
                            "BROCA_AUTH_SOURCE": "vault",
                            "BROCA_CONFIG_PATH": broca_config,
                            "BROCA_AUTH_METHODS_PATH": methods,
                            "BROCA_TOOL_MODULE": "unused-tools",
                        },
                        "enabled": true,
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let connection_file = runtime_dir.join("subc-connection.json");
        let daemon = broca_test_support::retry_executable_file_busy(|| {
            broca_test_support::tokio_rig_command(&daemon_bin, &root, "daemon")?
                .env("XDG_CONFIG_HOME", &config_home)
                .env("XDG_RUNTIME_DIR", &runtime_dir)
                .env("XDG_DATA_HOME", &data_home)
                .env_remove("BROCA_STATE_ROOT")
                .env("SUBC_PORT", "0")
                .env_remove("SUBC_LAUNCH_NONCE")
                .env_remove("SUBC_LAUNCH_NONCE_FD")
                .env_remove("SUBC_MODULE_ID")
                .kill_on_drop(true)
                .spawn()
        })
        .expect("spawn ck-subc");
        let daemon = broca_test_support::RigChild::new(daemon, &root, "daemon");
        let deadline = Instant::now() + Duration::from_secs(20);
        while !connection_file.exists() {
            assert!(
                Instant::now() < deadline,
                "the daemon never published its connection file"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let routing = Arc::new(Routing {
            mode: Mutex::new(RoutingMode::Disabled),
            requests: Mutex::new(Vec::new()),
            admissions: Mutex::new(Default::default()),
        });
        let vault = Arc::new(Vault {
            requests: Mutex::new(Vec::new()),
            not_found: Mutex::new(HashSet::new()),
            account_ids: Mutex::new(std::collections::HashMap::new()),
            relisted: Default::default(),
            anthropic_apikey_listed: Default::default(),
            openai_apikey_listed: Default::default(),
            openrouter_listed: Default::default(),
        });
        let routing_task = register(
            &connection_file,
            ROUTING,
            &["route.account_for", "route.account_observe"],
            RoutingModule(Arc::clone(&routing)),
        )
        .await;
        let vault_task = register(
            &connection_file,
            VAULT,
            &["credential.list_scoped", "credential.get_scoped"],
            VaultModule(Arc::clone(&vault)),
        )
        .await;
        Some(Rig {
            daemon,
            connection_file,
            state_root,
            project_root: project_root.canonicalize().unwrap(),
            root,
            routing,
            routing_task: Some(routing_task),
            vault,
            vault_task,
            upstream,
        })
    }

    async fn raw_send(&self, name: &str, send_id: &str, extra: Value) -> Result<Value, Value> {
        let _client = self.client(name).await;
        let conn = SubcConnection::connect(&self.connection_file)
            .await
            .unwrap();
        let route = conn
            .route_open_management_within(
                "broca",
                &self.session(name),
                broca_subc::RouteOpenRetry::within(
                    Duration::from_secs(10),
                    Duration::from_millis(200),
                ),
            )
            .await
            .unwrap();
        let mut params = json!({"prompt": "hello", "model": {"provider": "anthropic", "model": MODEL}, "tools": [], "send_id": send_id});
        params
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        Self::request_on(&conn, &route, "session.send", params).await
    }

    /// One request on a fresh management route to session `name`: the result, or the whole
    /// refusal body (code, message and detail, which carries a classified refusal's `class`).
    async fn raw_request(&self, name: &str, method: &str, params: Value) -> Result<Value, Value> {
        let conn = SubcConnection::connect(&self.connection_file)
            .await
            .unwrap();
        let route = conn
            .route_open_management_within(
                "broca",
                &self.session(name),
                broca_subc::RouteOpenRetry::within(
                    Duration::from_secs(10),
                    Duration::from_millis(200),
                ),
            )
            .await
            .unwrap();
        Self::request_on(&conn, &route, method, params).await
    }

    async fn request_on(
        conn: &SubcConnection,
        route: &broca_subc::RouteHandle,
        method: &str,
        params: Value,
    ) -> Result<Value, Value> {
        match conn
            .route_request_terminal(
                route,
                serde_json::to_vec(&json!({"method": method, "params": params})).unwrap(),
            )
            .await
            .unwrap()
        {
            Ok(bytes) => Ok(serde_json::from_slice::<Value>(&bytes).unwrap()["result"].clone()),
            Err(bytes) => Err(serde_json::from_slice(&bytes).unwrap()),
        }
    }

    /// The rig's shared failure dump for session `name`: the daemon's supervisor record of
    /// broca, the daemon and broca log tails, and the session's WAL record kinds.
    async fn diagnostics(&self, name: &str) -> String {
        format!(
            "{}{}",
            super::session_client::supervisor_diagnostics(&self.connection_file, "broca").await,
            self.log_diagnostics(name)
        )
    }

    /// The log tails and WAL part of [`Self::diagnostics`], for a caller that cannot wait.
    fn log_diagnostics(&self, name: &str) -> String {
        super::session_client::rig_diagnostics(
            &self.root.join("data-home/cortexkit"),
            &self.state_root,
            name,
        )
    }

    /// `outcome`'s result, or a panic naming `what`, the refusal's class, code and reason, and
    /// the rig's failure dump.
    async fn expect_ok(&self, name: &str, what: &str, outcome: Result<Value, Value>) -> Value {
        match outcome {
            Ok(result) => result,
            Err(refusal) => panic!(
                "{what} on {name} was refused: {}\n{}",
                refusal_summary(&refusal),
                self.diagnostics(name).await
            ),
        }
    }

    /// `outcome`'s refusal, or a panic naming `what`, the answer it got instead, and the rig's
    /// failure dump.
    async fn expect_refused(&self, name: &str, what: &str, outcome: Result<Value, Value>) -> Value {
        match outcome {
            Err(refusal) => refusal,
            Ok(result) => panic!(
                "{what} on {name} was answered, not refused: {result}\n{}",
                self.diagnostics(name).await
            ),
        }
    }

    /// `run.status` on session `name` once it answers. A restarted broca marks the sessions its
    /// predecessor left with an unsealed run before it serves, then seals them in the background;
    /// until a session is sealed, its status is refused as transient
    /// ([`is_pending_crash_orphan_seal`]), which the caller is to retry. Only that refusal is
    /// retried, within the shared hang guard; any other refusal fails at once.
    async fn settled_status(&self, name: &str) -> Value {
        let mut last = None;
        let settled = broca_test_support::event_timeout(
            &format!("{name}: run.status past crash-orphan sealing"),
            async {
                loop {
                    match self.raw_request(name, "run.status", json!({})).await {
                        Ok(status) => return Ok(status),
                        Err(refusal) if is_pending_crash_orphan_seal(&refusal) => {
                            last = Some(refusal);
                        }
                        Err(refusal) => return Err(refusal),
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            },
            String::new,
        )
        .await;
        match settled {
            Ok(Ok(status)) => status,
            Ok(Err(refusal)) => panic!(
                "run.status on {name} was refused: {}\n{}",
                refusal_summary(&refusal),
                self.diagnostics(name).await
            ),
            Err(_) => panic!(
                "run.status on {name} still refused after {:?}; last refusal: {}\n{}",
                broca_test_support::EVENT_HANG_GUARD,
                last.as_ref().map_or("none".into(), refusal_summary),
                self.diagnostics(name).await
            ),
        }
    }

    async fn wait_held(&self) {
        broca_test_support::wait_until("provider call held", || async {
            let held = self.upstream.held.load(std::sync::atomic::Ordering::SeqCst);
            (held > 0)
                .then_some(())
                .ok_or_else(|| format!("held_calls={held}"))
        })
        .await;
    }

    async fn finished_run(&self, name: &str, id: &str) -> Vec<FramedRecord> {
        broca_test_support::wait_for(
            &format!("terminal for {name}/{id}"),
            async {
                loop {
                    let frames = self.frames(name);
                    let mut in_run = false;
                    let done = frames.iter().any(|f| match &f.record {
                        WalRecord::RunStarted { run_id, .. } => {
                            in_run = run_id.0 == id;
                            false
                        }
                        WalRecord::RunFinished { .. } => in_run,
                        _ => false,
                    });
                    if done {
                        return frames;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            },
            || self.log_diagnostics(name),
        )
        .await
    }

    /// The session's WAL bytes, empty when it has no WAL yet.
    fn wal_bytes(&self, name: &str) -> Vec<u8> {
        match self.wal_file(name) {
            Some((path, _)) => std::fs::read(path).expect("read WAL"),
            None => Vec::new(),
        }
    }

    /// The conversation key routing got for admission `id`, which must have been asked once.
    fn affinity(&self, id: &str) -> String {
        let requests = self.routing.requests.lock().unwrap();
        let admissions: Vec<_> = requests
            .iter()
            .filter(|r| r["method"] == "route.account_for" && r["params"]["admission_id"] == id)
            .collect();
        assert_eq!(admissions.len(), 1, "one admission for {id}");
        admissions[0]["params"]["conversation_key"]
            .as_str()
            .expect("a conversation key")
            .to_string()
    }

    fn assert_affinity(&self, id: &str, key: &str) {
        let requests = self.routing.requests.lock().unwrap();
        let admissions: Vec<_> = requests
            .iter()
            .filter(|r| r["method"] == "route.account_for" && r["params"]["admission_id"] == id)
            .collect();
        assert_eq!(admissions.len(), 1, "one admission for {id}");
        assert_eq!(admissions[0]["params"]["conversation_key"], key);
    }

    fn session(&self, name: &str) -> broca_protocol::BindIdentity {
        broca_protocol::BindIdentity::new(self.project_root.clone(), "runner", name)
    }

    async fn client(&self, name: &str) -> SessionClient {
        let conn = SubcConnection::connect(&self.connection_file)
            .await
            .expect("connect a caller");
        match SessionClient::bind_with_timeout(
            conn,
            "broca",
            self.session(name),
            Duration::from_secs(30),
        )
        .await
        {
            Ok(client) => client,
            Err(error) => {
                eprintln!(
                    "{}{}",
                    super::session_client::supervisor_diagnostics(&self.connection_file, "broca")
                        .await,
                    super::session_client::rig_log_diagnostics(
                        &self.root.join("data-home/cortexkit"),
                        &self.state_root,
                    )
                );
                panic!("bind broca: {error:?}");
            }
        }
    }

    /// The records of the WAL whose `RunStarted` names session `name`, found by scanning the WAL
    /// directory rather than computing the WAL's address. The address hashes the bind triple as
    /// the module received it, after the daemon normalised the project root, and that spelling
    /// differs from the test's own on Windows (`\\?\C:\...`) and on macOS (`/private/var`). A
    /// computed address there names a file that never exists, so a finished run reads as one
    /// that never wrote. Every session name in this test is unique, so the name finds its log.
    fn frames(&self, name: &str) -> Vec<FramedRecord> {
        self.wal_file(name)
            .map(|(_, frames)| frames)
            .unwrap_or_default()
    }

    /// The WAL whose `RunStarted` names session `name`, with its records; `None` when no WAL
    /// does yet. See [`Self::frames`] for why it is found by scanning.
    fn wal_file(&self, name: &str) -> Option<(std::path::PathBuf, Vec<FramedRecord>)> {
        let entries = std::fs::read_dir(self.state_root.join("wal")).ok()?;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("wal") {
                continue;
            }
            let Ok(range) = broca_wal::read_records_range(
                &path,
                broca_wal::DEFAULT_MAX_READ_BYTES,
                0,
                None,
                None,
            ) else {
                continue;
            };
            let broca_wal::WalRangeRecords::Complete(frames) = range.records() else {
                continue;
            };
            let names_session = frames.iter().any(|frame| {
                matches!(&frame.record, WalRecord::RunStarted { session, .. } if session.session == name)
            });
            if names_session {
                return Some((path, frames.to_vec()));
            }
        }
        None
    }

    /// Wait for the session's WAL to hold a record matching `done`.
    async fn wait_for(
        &self,
        name: &str,
        what: &str,
        done: impl Fn(&WalRecord) -> bool,
    ) -> Vec<FramedRecord> {
        broca_test_support::wait_until(&format!("{name}: {what}"), || async {
            let frames = self.frames(name);
            if frames.iter().any(|frame| done(&frame.record)) {
                Ok(frames)
            } else {
                Err(broca_test_support::record_kinds(
                    frames.iter().map(|f| &f.record),
                ))
            }
        })
        .await
    }

    async fn finished(&self, name: &str) -> Vec<FramedRecord> {
        self.wait_for(name, "terminal", |record| {
            matches!(record, WalRecord::RunFinished { .. })
        })
        .await
    }

    /// Wait until routing has every report kind in `kinds` for `admission_id`.
    async fn reports(&self, admission_id: &str, kinds: &[&str]) -> Vec<(String, String, Value)> {
        broca_test_support::wait_for(
            "routing report kinds",
            async {
                loop {
                    let seen = self.routing.for_admission(admission_id);
                    let have: Vec<&str> = seen.iter().map(|(_, kind, _)| kind.as_str()).collect();
                    if kinds.iter().all(|kind| have.contains(kind)) {
                        // Long enough for an unexpected extra report to arrive as well.
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        return self.routing.for_admission(admission_id);
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            },
            || {
                format!(
                    "reports={:?}, expected={kinds:?}",
                    self.routing.for_admission(admission_id)
                )
            },
        )
        .await
    }

    async fn broca_metrics(&self) -> Value {
        let conn = SubcConnection::connect(&self.connection_file)
            .await
            .unwrap();
        conn.supervisor_health_probe("broca")
            .await
            .expect("broca health")
            .metrics
            .unwrap_or(Value::Null)
    }
}

/// A refusal body's class, code and reason, for a failure message.
fn refusal_summary(refusal: &Value) -> String {
    format!(
        "class={}, code={}, reason={}, detail={}",
        refusal["detail"]["class"], refusal["code"], refusal["message"], refusal["detail"]
    )
}

/// Whether `refusal` is the one a status poll gets while a restarted broca has marked the
/// session's unsealed run for its crash-orphan seal pass but not yet sealed it: `open_failed`,
/// classed transient, which the poller is to retry.
fn is_pending_crash_orphan_seal(refusal: &Value) -> bool {
    refusal["code"] == "open_failed"
        && refusal["detail"]["class"] == "transient"
        && refusal["message"]
            .as_str()
            .is_some_and(|message| message.contains("crash-orphan sealing is pending"))
}

fn spec<'a>(send_id: &'a str, auth: Option<broca_wire::AuthParams>) -> SendSpec<'a> {
    SendSpec {
        prompt: "hello",
        system: None,
        provider: "anthropic",
        model: MODEL,
        variant: None,
        service_tier: None,
        tools: Some(Vec::new()),
        send_id: Some(send_id),
        cache: None,
        budget: None,
        auth,
        one_shot: None,
        context_management: None,
    }
}

async fn send(
    client: &SessionClient,
    send_id: &str,
    auth: Option<broca_wire::AuthParams>,
) -> String {
    match client.send(spec(send_id, auth)).await.expect("send") {
        SendOutcome::Active { run_id } | SendOutcome::Finished { run_id, .. } => run_id,
        other => panic!("unexpected send outcome {other:?}"),
    }
}

fn selection(frames: &[FramedRecord]) -> Value {
    let started = frames
        .iter()
        .find_map(|frame| match &frame.record {
            WalRecord::RunStarted { config, .. } => Some(config.auth_selection.clone()),
            _ => None,
        })
        .expect("a RunStarted");
    serde_json::to_value(started).unwrap()
}

fn session_affinity(frames: &[FramedRecord]) -> String {
    let session = frames
        .iter()
        .find_map(|frame| match &frame.record {
            WalRecord::RunStarted { session, .. } => Some(session),
            _ => None,
        })
        .expect("a RunStarted session");
    broca_module_serve::account_routing::conversation_key(session)
}

fn seq_of(frames: &[FramedRecord], pick: impl Fn(&WalRecord) -> bool) -> Vec<u64> {
    frames
        .iter()
        .filter(|frame| pick(&frame.record))
        .map(|frame| frame.seq)
        .collect()
}

/// The `<lineage>:<seq>` an event id names, split.
fn event(params: &Value) -> (String, u64) {
    let id = params["event_id"].as_str().expect("an event id");
    let (lineage, seq) = id.rsplit_once(':').expect("lineage:seq");
    assert_eq!(lineage.len(), 36, "a lineage id: {id}");
    (lineage.to_string(), seq.parse().expect("a sequence number"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn routing_decides_reports_and_never_blocks_admission_through_the_real_daemon() {
    let Some(mut rig) = Rig::start().await else {
        return;
    };

    // 1. Routing's account selection is switched off: it answers account_selection_disabled,
    // and the run is admitted on the configured default account exactly as without routing.
    // Routing is asked once and gets every report: admission, the attempt, the run's end.
    let client = rig.client("disabled").await;
    let run_id = send(&client, "send-disabled", None).await;
    let frames = rig.finished("disabled").await;
    let mut chosen = selection(&frames);
    assert_eq!(
        chosen["account_decision"],
        json!({ "conversation_key": session_affinity(&frames), "conversation_key_source": "session", "path": "disabled", "admission_id": run_id })
    );
    chosen.as_object_mut().unwrap().remove("account_decision");
    assert_eq!(
        chosen,
        json!({
            "credential_id": "oauth:anthropic",
            "resolved_credential_id": "oauth:anthropic",
            "vault_request": { "min_ttl_ms": 600000, "op": "get_scoped" },
        }),
        "apart from the decision, the selection is today's static pick"
    );
    let reports = rig
        .reports(&run_id, &["admission_started", "attempt", "run_finished"])
        .await;
    let kinds: Vec<&str> = reports
        .iter()
        .map(|(method, kind, _)| {
            if method == "route.account_for" {
                "for"
            } else {
                kind.as_str()
            }
        })
        .collect();
    assert_eq!(
        kinds,
        ["for", "admission_started", "attempt", "run_finished"]
    );
    let started_seq = seq_of(&frames, |r| matches!(r, WalRecord::RunStarted { .. }));
    let attempt_seq = seq_of(&frames, |r| {
        matches!(r, WalRecord::ModelAttemptFinished { .. })
    });
    let finished_seq = seq_of(&frames, |r| matches!(r, WalRecord::RunFinished { .. }));
    let (lineage, seq) = event(&reports[1].2);
    assert_eq!(vec![seq], started_seq);
    let (attempt_lineage, seq) = event(&reports[2].2);
    assert_eq!(
        vec![seq],
        attempt_seq,
        "the attempt report names its own record"
    );
    let (finish_lineage, seq) = event(&reports[3].2);
    assert_eq!(vec![seq], finished_seq);
    assert!(lineage == attempt_lineage && lineage == finish_lineage);
    assert_eq!(
        reports[2].2["headers"],
        reported_headers(),
        "verbatim, and only the family"
    );
    assert_eq!(reports[2].2["upstream_origin"], "upstream");
    assert_eq!(reports[3].2["headers"], reported_headers());
    assert_eq!(reports[3].2["terminal_reason"], "completed");

    // 2. Routing chooses: the run reads and uses exactly that credential, frozen with the
    // decision.
    rig.routing.set(RoutingMode::Choose {
        account_id: "acct-b",
        credential_id: "oauth:anthropic:acct-b",
    });
    let client = rig.client("routed").await;
    let run_id = send(&client, "send-routed", None).await;
    let frames = rig.finished("routed").await;
    let chosen = selection(&frames);
    assert_eq!(chosen["credential_id"], "oauth:anthropic:acct-b");
    assert_eq!(
        chosen["account_decision"],
        json!({ "conversation_key": session_affinity(&frames), "conversation_key_source": "session", "path": "routed", "admission_id": run_id, "decision_id": "acd-e2e" })
    );
    assert_eq!(
        rig.vault.scoped_reads().last().map(String::as_str),
        Some("oauth:anthropic:acct-b")
    );
    assert_eq!(
        rig.upstream
            .authorized_as
            .lock()
            .unwrap()
            .last()
            .map(String::as_str),
        Some("oauth:anthropic:acct-b"),
        "the provider was called with the chosen account's token"
    );
    rig.reports(&run_id, &["admission_started", "attempt", "run_finished"])
        .await;
    assert_eq!(rig.routing.account_for_count(&run_id), 1);

    // 3. A caller pin wins: routing is not asked, the run is still reported.
    let client = rig.client("pinned").await;
    let run_id = send(
        &client,
        "send-pinned",
        Some(broca_wire::AuthParams {
            method: "oauth".into(),
            account: Some("work".into()),
        }),
    )
    .await;
    let frames = rig.finished("pinned").await;
    let chosen = selection(&frames);
    assert_eq!(chosen["credential_id"], "oauth:anthropic:work");
    assert_eq!(
        chosen["account_decision"],
        json!({ "conversation_key": session_affinity(&frames), "conversation_key_source": "session", "path": "pinned", "admission_id": run_id })
    );
    rig.reports(&run_id, &["admission_started", "attempt", "run_finished"])
        .await;
    assert_eq!(
        rig.routing.account_for_count(&run_id),
        0,
        "a pinned send never asks routing"
    );

    // 4. A routing refusal refuses the send: typed, nothing written, nothing reported after.
    rig.routing.set(RoutingMode::Refuse {
        code: "account_pool_exhausted",
        retry_after_s: Some(60),
    });
    let conn = SubcConnection::connect(&rig.connection_file).await.unwrap();
    let route = conn
        .route_open_management_within(
            "broca",
            &rig.session("refused"),
            broca_subc::RouteOpenRetry::within(Duration::from_secs(10), Duration::from_millis(200)),
        )
        .await
        .unwrap();
    // Routing's requests for this send are the ones that arrive after this point. They are
    // picked out by position, not by recomputing the conversation key: the key hashes the
    // project root as broca received it, which the daemon normalised and the test cannot
    // spell the same way on every platform.
    let asked_before = rig.routing.requests.lock().unwrap().len();
    let reply = conn
        .route_request_terminal(
            &route,
            serde_json::to_vec(&json!({ "method": "session.send", "params": {
                "prompt": "hello",
                "model": { "provider": "anthropic", "model": MODEL },
                "tools": [],
                "send_id": "send-refused",
            }}))
            .unwrap(),
        )
        .await
        .unwrap();
    let error: Value = serde_json::from_slice(&reply.expect_err("refused")).unwrap();
    assert_eq!(error["code"], "account_selection_refused");
    assert_eq!(
        error["detail"],
        json!({
            "routing_code": "account_pool_exhausted",
            "class": "transient",
            "retry_after_s": 60,
            "retry_after_secs": 60.0,
            "decision_id": "acd-refused",
        })
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        rig.frames("refused")
            .iter()
            .all(|frame| !matches!(frame.record, WalRecord::RunStarted { .. })),
        "a refused admission writes no RunStarted"
    );
    let refused_admissions: Vec<String> = rig
        .routing
        .requests
        .lock()
        .unwrap()
        .iter()
        .skip(asked_before)
        .filter(|request| request["method"] == "route.account_for")
        .map(|request| {
            request["params"]["admission_id"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    assert_eq!(refused_admissions.len(), 1, "asked once");
    let refused: Vec<String> = rig
        .routing
        .for_admission(&refused_admissions[0])
        .into_iter()
        .map(|(method, ..)| method)
        .collect();
    assert_eq!(
        refused,
        ["route.account_for"],
        "nothing is reported for a refused admission"
    );

    // 5. Routing says every candidate needs a fresh login: the run is admitted on the configured
    // default account and paused for re-authentication; resending the same send_id (the vault
    // serves that credential) resumes it WITHOUT asking routing a second time.
    rig.routing.set(RoutingMode::AuthRequired);
    let client = rig.client("login").await;
    let run_id = send(&client, "send-login", None).await;
    let frames = rig
        .wait_for("login", "pause", |record| {
            matches!(record, WalRecord::RunPaused { .. })
        })
        .await;
    assert_eq!(
        selection(&frames)["account_decision"],
        json!({ "conversation_key": session_affinity(&frames), "conversation_key_source": "session", "path": "auth_required", "admission_id": run_id, "decision_id": "acd-login" })
    );
    rig.routing.set(RoutingMode::Disabled);
    // The pause is durable before the session's actor has seen its run end; a retry sent in
    // between would join the still-active run instead of resuming it.
    broca_test_support::wait_until("paused session becomes idle", || async {
        let status = client.status().await;
        if matches!(status, Ok(broca_module::serve::StatusResponse::Idle)) {
            Ok(())
        } else {
            Err(format!("{status:?}"))
        }
    })
    .await;
    let resumed = send(&client, "send-login", None).await;
    assert_eq!(resumed, run_id);
    rig.finished("login").await;
    rig.reports(&run_id, &["admission_started", "attempt", "run_finished"])
        .await;
    assert_eq!(
        rig.routing.account_for_count(&run_id),
        1,
        "a resume never asks routing again"
    );

    // 6. A crash mid-run: the restarted module seals the run Interrupted and reports that end,
    // without asking routing again. Unix only: the crash is a SIGKILL of the daemon's broca
    // child found with `pgrep -P`, and on Windows the daemon runs modules in a job object that
    // has no such child to signal.
    #[cfg(unix)]
    {
        rig.routing.set(RoutingMode::Choose {
            account_id: "acct-b",
            credential_id: "oauth:anthropic:acct-b",
        });
        rig.upstream
            .hold
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let client = rig.client("crash").await;
        let run_id = send(&client, "send-crash", None).await;
        rig.wait_held().await;
        let broca_pids = children(rig.daemon.id().unwrap());
        assert!(!broca_pids.is_empty(), "broca is supervised by the daemon");
        for pid in &broca_pids {
            let _ = std::process::Command::new("kill")
                .args(["-9", &pid.to_string()])
                .status();
        }
        rig.upstream
            .hold
            .store(false, std::sync::atomic::Ordering::SeqCst);
        // Reopening the session makes the restarted module end the run the crash left unfinished,
        // with an Interrupted terminal.
        let reopened = rig.client("crash").await;
        let deadline = Instant::now() + Duration::from_secs(30);
        let frames = loop {
            let _ = reopened.status().await;
            let frames = rig.frames("crash");
            if frames
                .iter()
                .any(|frame| matches!(frame.record, WalRecord::RunFinished { .. }))
            {
                break frames;
            }
            assert!(
                Instant::now() < deadline,
                "the crashed run was never sealed: {frames:#?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        let reports = rig
            .reports(&run_id, &["admission_started", "run_finished"])
            .await;
        let finished: Vec<_> = reports
            .iter()
            .filter(|(_, kind, _)| kind == "run_finished")
            .collect();
        assert_eq!(finished.len(), 1, "one report per terminal");
        assert_eq!(finished[0].2["terminal_reason"], "interrupted");
        assert_eq!(
            vec![event(&finished[0].2).1],
            seq_of(&frames, |r| matches!(r, WalRecord::RunFinished { .. }))
        );
        assert_eq!(
            rig.routing.account_for_count(&run_id),
            1,
            "the crash recovery never asks routing"
        );
    }

    // 6b. A retry of the same send after a vault re-list is the same admission. The first
    // attempt's chosen credential is not served (scoped not_found), so the send is refused
    // permanently and broca re-lists; the vault's listing has changed by then. The retry sends
    // the same admission id with a different candidate list and digest, and routing replays
    // its decision instead of answering admission_conflict.
    rig.routing.set(RoutingMode::Choose {
        account_id: "acct-s",
        credential_id: "oauth:anthropic:spare",
    });
    rig.vault
        .not_found
        .lock()
        .unwrap()
        .insert("oauth:anthropic:spare".into());
    let lists_before = rig.vault.list_calls();
    rig.vault
        .relisted
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let client = rig.client("relisted").await;
    let refused = client
        .send(spec("send-relisted", None))
        .await
        .expect_err("the unserved credential refuses the first attempt");
    assert!(
        refused.to_string().contains("account_selection_refused"),
        "{refused}"
    );
    broca_test_support::wait_until("vault re-list after not_found", || async {
        let calls = rig.vault.list_calls();
        (calls != lists_before)
            .then_some(())
            .ok_or_else(|| format!("list_calls={calls}, before={lists_before}"))
    })
    .await;
    rig.vault.not_found.lock().unwrap().clear();
    let run_id = send(&client, "send-relisted", None).await;
    let frames = rig.finished("relisted").await;
    let chosen = selection(&frames);
    assert_eq!(chosen["credential_id"], "oauth:anthropic:spare");
    assert_eq!(
        chosen["account_decision"],
        json!({ "conversation_key": session_affinity(&frames), "conversation_key_source": "session", "path": "routed", "admission_id": run_id, "decision_id": "acd-e2e" })
    );
    let asked: Vec<Value> = rig
        .routing
        .for_admission(&run_id)
        .into_iter()
        .filter(|(method, ..)| method == "route.account_for")
        .map(|(_, _, params)| params)
        .collect();
    assert_eq!(
        asked.len(),
        2,
        "the retry asked again, under the same admission id"
    );
    assert_ne!(
        asked[0]["candidates"], asked[1]["candidates"],
        "the re-list changed what the retry offered"
    );
    assert_eq!(asked[1]["candidates"]["view_digest"], "vault-view-relisted");
    assert_eq!(admission_identity(&asked[0]), admission_identity(&asked[1]));
    let not_found: Vec<_> = rig
        .routing
        .for_admission(&run_id)
        .into_iter()
        .filter(|(_, kind, _)| kind == "scoped_not_found")
        .collect();
    assert_eq!(not_found.len(), 1, "the refused attempt told routing once");
    assert_eq!(not_found[0].2["credential_id"], "oauth:anthropic:spare");
    assert_eq!(not_found[0].2["view_digest"], "vault-view-e2e");

    // 7. Routing gone: the run is admitted on the configured default account, without waiting
    // past the admission budget, and the unreachable counter is raised.
    rig.routing_task.take().unwrap().abort();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let client = rig.client("unreachable").await;
    let started = Instant::now();
    let run_id = send(&client, "send-unreachable", None).await;
    assert!(
        started.elapsed() < Duration::from_secs(8),
        "the admission waited {:?}",
        started.elapsed()
    );
    let frames = rig.finished("unreachable").await;
    let chosen = selection(&frames);
    assert_eq!(chosen["credential_id"], "oauth:anthropic");
    assert_eq!(
        chosen["account_decision"],
        json!({ "conversation_key": session_affinity(&frames), "conversation_key_source": "session", "path": "unreachable", "admission_id": run_id })
    );
    let metrics = rig.broca_metrics().await;
    assert!(
        metrics["account_selection"]["unreachable"].as_u64() >= Some(1),
        "the unreachable counter: {metrics}"
    );

    let root = rig.root.clone();
    drop(rig);
    broca_test_support::remove_rig_dir(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn caller_conversation_key_reaches_routing_and_retry_identity() {
    let Some(rig) = Rig::start().await else {
        return;
    };
    rig.routing.set(RoutingMode::Choose {
        account_id: "acct-b",
        credential_id: "oauth:anthropic:acct-b",
    });
    let extra = json!({"conversation_key": "consult:42:subject:phase"});
    let first = rig.raw_send("keyed", "turn", extra.clone()).await.unwrap();
    let id = first["run_id"].as_str().unwrap();
    let frames = rig.finished_run("keyed", id).await;
    let client = rig.client("keyed").await;
    broca_test_support::wait_until("finished keyed run becomes idle", || async {
        let status = client.status().await;
        if matches!(status, Ok(broca_module::serve::StatusResponse::Idle)) {
            Ok(())
        } else {
            Err(format!("{status:?}"))
        }
    })
    .await;
    rig.assert_affinity(id, "consult:42:subject:phase");
    assert_eq!(
        selection(&frames)["account_decision"]["conversation_key_source"],
        "caller"
    );
    // Later episodes need the raw affinity in the WAL, not just its source or a digest.
    assert_eq!(
        selection(&frames)["account_decision"]["conversation_key"],
        "consult:42:subject:phase"
    );
    let before = rig.wal_bytes("keyed");
    let replay = rig.raw_send("keyed", "turn", extra).await.unwrap();
    assert_eq!(replay["state"], "finished");
    assert_eq!(replay["run_id"], id);
    let refusal = rig
        .raw_send(
            "keyed",
            "turn",
            json!({"conversation_key": "consult:43:subject:phase"}),
        )
        .await
        .unwrap_err();
    assert_eq!(refusal["code"], "send_id_reuse");
    assert_eq!(refusal["detail"]["field"], "conversation_key");
    assert_eq!(rig.wal_bytes("keyed"), before);
    assert_eq!(rig.routing.account_for_count(id), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_conversation_keys_are_refused_without_wal_writes() {
    let Some(rig) = Rig::start().await else {
        return;
    };
    rig.routing.set(RoutingMode::Choose {
        account_id: "acct-b",
        credential_id: "oauth:anthropic:acct-b",
    });
    for key in [
        json!(""),
        json!("x".repeat(257)),
        json!("a b"),
        json!("a\n"),
        json!("é"),
        json!(42),
        Value::Null,
    ] {
        let before = rig.wal_bytes("invalid-key");
        let refusal = rig
            .raw_send("invalid-key", "turn", json!({"conversation_key": key}))
            .await
            .unwrap_err();
        assert_eq!(
            refusal,
            serde_json::from_str::<Value>(include_str!(
                "../../../broca-wire/tests/golden/invalid_params.conversation_key.json"
            ))
            .unwrap()
        );
        assert_eq!(refusal["code"], "invalid_params", "{refusal}");
        assert_eq!(refusal["detail"]["field"], "conversation_key", "{refusal}");
        assert_eq!(rig.wal_bytes("invalid-key"), before);
    }
    assert!(
        rig.routing.requests.lock().unwrap().is_empty(),
        "refused before routing"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queued_conversation_key_survives_drain_and_crash_before_drain() {
    let Some(rig) = Rig::start().await else {
        return;
    };
    rig.routing.set(RoutingMode::Choose {
        account_id: "acct-b",
        credential_id: "oauth:anthropic:acct-b",
    });
    for crash in [false, true] {
        let name = if crash {
            "queue-crash-key"
        } else {
            "queue-live-key"
        };
        rig.upstream
            .held
            .store(0, std::sync::atomic::Ordering::SeqCst);
        rig.upstream
            .hold
            .store(true, std::sync::atomic::Ordering::SeqCst);
        // Every request this test waits on goes through `expect_ok`/`expect_refused`, so a
        // refusal is reported with its class, code and reason and the rig's failure dump.
        let _client = rig.client(name).await;
        let active = rig
            .expect_ok(
                name,
                "the active send",
                rig.raw_send(name, "active", json!({})).await,
            )
            .await;
        let active = active["run_id"]
            .as_str()
            .unwrap_or_else(|| panic!("the active send names its run: {active}"))
            .to_owned();
        rig.wait_held().await;
        let queued = rig
            .expect_ok(
                name,
                "the queued send",
                rig.raw_send(
                    name,
                    "queued",
                    json!({"conversation_key": "consult:queue:phase"}),
                )
                .await,
            )
            .await;
        assert_eq!(queued["state"], "pending");
        let before = rig.wal_bytes(name);
        assert_eq!(
            rig.expect_ok(
                name,
                "the queued send's retry",
                rig.raw_send(
                    name,
                    "queued",
                    json!({"conversation_key": "consult:queue:phase"})
                )
                .await,
            )
            .await,
            queued
        );
        let reuse = rig
            .expect_refused(
                name,
                "the queued send id with another key",
                rig.raw_send(
                    name,
                    "queued",
                    json!({"conversation_key": "consult:other:phase"}),
                )
                .await,
            )
            .await;
        assert_eq!(reuse["code"], "send_id_reuse");
        assert_eq!(reuse["detail"]["field"], "conversation_key");
        assert_eq!(rig.wal_bytes(name), before);
        let id = queued["submission_id"]
            .as_str()
            .unwrap()
            .strip_prefix("sub-")
            .unwrap();
        let frames = rig.frames(name);
        assert!(frames.iter().any(|f| matches!(&f.record, WalRecord::Queued { conversation_key, .. } if conversation_key.as_deref() == Some("consult:queue:phase"))));
        assert!(!frames
            .iter()
            .any(|f| matches!(&f.record, WalRecord::RunStarted { run_id, .. } if run_id.0 == id)));
        // The crash leg kills the daemon's broca child with `kill -9`, which Windows does not
        // have; there the daemon runs modules in a job object.
        if crash && cfg!(not(unix)) {
            continue;
        }
        if crash {
            // A test-clock build holds the restarted broca's crash-orphan seal pass while this
            // file exists, so the status poll below deterministically meets the session marked
            // but not yet sealed: the window a loaded machine can also open.
            #[cfg(feature = "test-clock")]
            let seal_gate = rig.state_root.join("test-crash-orphan-seal-gate");
            #[cfg(feature = "test-clock")]
            std::fs::write(&seal_gate, b"held").unwrap();
            #[cfg(unix)]
            {
                let pids = children(rig.daemon.id().unwrap());
                assert!(!pids.is_empty());
                for pid in pids {
                    assert!(std::process::Command::new("kill")
                        .args(["-9", &pid.to_string()])
                        .status()
                        .unwrap()
                        .success());
                }
            }
            rig.upstream
                .hold
                .store(false, std::sync::atomic::Ordering::SeqCst);
            let _reopened = rig.client(name).await;
            #[cfg(feature = "test-clock")]
            {
                let pending = rig
                    .expect_refused(
                        name,
                        "run.status while the seal pass is held",
                        rig.raw_request(name, "run.status", json!({})).await,
                    )
                    .await;
                assert!(
                    is_pending_crash_orphan_seal(&pending),
                    "{}",
                    refusal_summary(&pending)
                );
                std::fs::remove_file(&seal_gate).unwrap();
            }
            rig.settled_status(name).await;
        } else {
            rig.upstream
                .hold
                .store(false, std::sync::atomic::Ordering::SeqCst);
            rig.expect_ok(
                name,
                "run.cancel of the active run",
                rig.raw_request(name, "run.cancel", json!({"run_id": active}))
                    .await,
            )
            .await;
        }
        let frames = rig.finished_run(name, id).await;
        rig.assert_affinity(id, "consult:queue:phase");
        let config = frames
            .iter()
            .find_map(|f| match &f.record {
                WalRecord::RunStarted { run_id, config, .. } if run_id.0 == id => Some(config),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            config
                .auth_selection
                .as_ref()
                .unwrap()
                .account_decision
                .as_ref()
                .unwrap()
                .conversation_key_source,
            Some(broca_wal::ConversationKeySource::Caller)
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn steer_ignores_conversation_key_and_interrupt_uses_its_own() {
    let Some(rig) = Rig::start().await else {
        return;
    };
    rig.routing.set(RoutingMode::Choose {
        account_id: "acct-b",
        credential_id: "oauth:anthropic:acct-b",
    });
    rig.upstream
        .hold
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let client = rig.client("modes-key").await;
    let active = send(&client, "active", None).await;
    rig.wait_held().await;
    let steered = rig
        .raw_send(
            "modes-key",
            "steer",
            json!({"delivery": "steer", "conversation_key": "consult:steer:ignored"}),
        )
        .await
        .unwrap();
    assert_eq!(steered["state"], "active");
    assert_eq!(steered["run_id"], active);
    assert_eq!(
        rig.routing
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r["method"] == "route.account_for")
            .count(),
        1
    );
    // The running run kept its session-derived affinity. The test cannot recompute that key,
    // because it hashes the project root as the daemon normalised it, so it checks the key is
    // the session-derived kind and not the caller's.
    let session_key = rig.affinity(&active);
    assert!(
        session_key.starts_with("broca-session:"),
        "the running run's affinity is session-derived: {session_key}"
    );
    rig.upstream
        .hold
        .store(false, std::sync::atomic::Ordering::SeqCst);
    let interrupt = rig
        .raw_send(
            "modes-key",
            "interrupt",
            json!({"delivery": "interrupt", "conversation_key": "consult:interrupt:phase"}),
        )
        .await
        .unwrap();
    let id = interrupt["submission_id"]
        .as_str()
        .unwrap()
        .strip_prefix("sub-")
        .unwrap();
    rig.finished_run("modes-key", id).await;
    rig.assert_affinity(id, "consult:interrupt:phase");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn later_send_inherits_conversation_key_after_restart() {
    let Some(rig) = Rig::start().await else {
        return;
    };
    rig.routing.set(RoutingMode::Choose {
        account_id: "acct-b",
        credential_id: "oauth:anthropic:acct-b",
    });
    let name = "inherit-affinity";
    for (send_id, key, expected, source) in [
        (
            "first",
            Some("consult:inherit"),
            "consult:inherit",
            broca_wal::ConversationKeySource::Caller,
        ),
        (
            "second",
            None,
            "consult:inherit",
            broca_wal::ConversationKeySource::Inherited,
        ),
        (
            "override",
            Some("consult:override"),
            "consult:override",
            broca_wal::ConversationKeySource::Caller,
        ),
    ] {
        // A durable terminal precedes the actor's idle transition; wait for admission readiness
        // so the next send starts an episode instead of joining the pending queue.
        let client = rig.client(name).await;
        broca_test_support::wait_until(&format!("session idle before {send_id}"), || async {
            let status = client.status().await;
            if matches!(status, Ok(broca_module::serve::StatusResponse::Idle)) {
                Ok(())
            } else {
                Err(format!("{status:?}"))
            }
        })
        .await;
        let extra = key
            .map(|key| json!({"conversation_key": key}))
            .unwrap_or_else(|| json!({}));
        let sent = rig.raw_send(name, send_id, extra).await.unwrap();
        let id = sent["run_id"]
            .as_str()
            .unwrap_or_else(|| panic!("expected an admitted episode for {send_id}: {sent}"));
        let frames = rig.finished_run(name, id).await;
        rig.assert_affinity(id, expected);
        let decision = frames
            .iter()
            .find_map(|frame| match &frame.record {
                WalRecord::RunStarted { run_id, config, .. } if run_id.0 == id => config
                    .auth_selection
                    .as_ref()
                    .and_then(|auth| auth.account_decision.as_ref()),
                _ => None,
            })
            .unwrap();
        assert_eq!(decision.conversation_key_source.as_ref(), Some(&source));
        assert_eq!(decision.conversation_key.as_deref(), Some(expected));
        if send_id == "first" {
            // Restart the module after a finished episode to force affinity recovery from disk.
            #[cfg(unix)]
            for pid in children(rig.daemon.id().unwrap()) {
                assert!(std::process::Command::new("kill")
                    .args(["-9", &pid.to_string()])
                    .status()
                    .unwrap()
                    .success());
            }
            rig.client(name).await.status().await.unwrap();
        }
    }
    let client = rig.client("first-derived").await;
    let id = send(&client, "first", None).await;
    rig.finished_run("first-derived", &id).await;
    assert!(rig.affinity(&id).starts_with("broca-session:"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paused_auth_routed_status_matches_golden_through_real_serve() {
    let Some(rig) = Rig::start().await else {
        return;
    };
    rig.routing.set(RoutingMode::Choose {
        account_id: "acct-b",
        credential_id: "oauth:anthropic:acct-b",
    });
    rig.upstream
        .reject_auth
        .store(true, std::sync::atomic::Ordering::SeqCst);
    rig.vault
        .account_ids
        .lock()
        .unwrap()
        .insert("oauth:anthropic:acct-b".into(), "acct-b".into());
    let client = rig.client("paused-auth-routed").await;
    let run_id = send(&client, "send-paused-auth-routed", None).await;
    let frames = rig
        .wait_for("paused-auth-routed", "auth pause", |record| {
            matches!(
                record,
                WalRecord::RunPaused {
                    reason: broca_protocol::PauseReason::AuthRequired,
                    ..
                }
            )
        })
        .await;
    assert_eq!(selection(&frames)["account_decision"]["path"], "routed");
    assert_eq!(selection(&frames)["account_ref"], "acct-b");
    assert_eq!(
        rig.upstream
            .authorized_as
            .lock()
            .unwrap()
            .last()
            .map(String::as_str),
        Some("oauth:anthropic:acct-b")
    );
    let status = broca_test_support::wait_until(
        "paused_auth_routed_status_matches_golden_through_real_serve: run status",
        || async {
            let status = client.run_status(&run_id).await.unwrap();
            if matches!(status, broca_wire::RunStatusResponse::Paused { .. }) {
                Ok(status)
            } else {
                Err(format!("{status:?}"))
            }
        },
    )
    .await;
    let bytes = serde_json::to_vec(&status).unwrap();
    use sha2::{Digest, Sha256};
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../broca-wire/tests/golden/run_status.paused_auth_routed.json");
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, &bytes).unwrap();
    }
    let golden = std::fs::read(&path).expect("generate the routed auth golden through real serve");
    assert_eq!(
        format!("{:x}", Sha256::digest(&golden)),
        "f975a084cb4e89e88c01de1c17343919d6f2f06da9d3fb500bf7ccfaf4edb169"
    );
    assert_eq!(bytes, golden);
    let value = serde_json::to_value(status).unwrap();
    assert_eq!(value["auth"]["credential_id"], "oauth:anthropic:acct-b");
    assert_eq!(value["auth"]["account_ref"], "acct-b");
    assert_eq!(value["auth"]["path"], "routed");
}

// This test needs the test-clock feature: FileWalStore then reads the injected
// small WAL cap. Regenerate the golden from the spawned daemon's run.status reply
// with BROCA_UPDATE_GOLDEN=1, so it covers the served error rather than a fixture.
#[cfg(feature = "test-clock")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn session_too_large_status_matches_golden_through_real_serve() {
    let rig = Rig::start()
        .await
        .expect("the size-limit golden requires the real daemon");
    std::fs::write(rig.state_root.join("test-session-wal-cap"), "16384").unwrap();
    rig.upstream
        .large_reply
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let client = rig.client("session-too-large").await;
    let run_id = send(&client, "send-session-too-large", None).await;
    let frames = rig.finished("session-too-large").await;
    assert!(
        matches!(&frames.last().unwrap().record, WalRecord::RunFinished { error: Some(error), .. } if error.provider_code.as_deref() == Some("session_too_large"))
    );
    let status = broca_test_support::wait_until(
        "session_too_large_status_matches_golden_through_real_serve: run status",
        || async {
            let status = client.run_status(&run_id).await.unwrap();
            if matches!(status, broca_wire::RunStatusResponse::Error { .. }) {
                Ok(status)
            } else {
                Err(format!("{status:?}"))
            }
        },
    )
    .await;
    let value = serde_json::to_value(&status).unwrap();
    assert_eq!(value["error"]["provider_code"], "session_too_large");
    assert_eq!(value["error"]["class"], "permanent");
    assert_eq!(
        value["error"]["message"],
        "session log cannot exceed 16384 bytes"
    );
    let bytes = serde_json::to_vec(&status).unwrap();
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../broca-wire/tests/golden/run_status.session_too_large.json");
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, &bytes).unwrap();
    }
    let golden =
        std::fs::read(&path).expect("generate the size-limit golden through the real serve path");
    use sha2::{Digest, Sha256};
    assert_eq!(
        format!("{:x}", Sha256::digest(&golden)),
        "3bcd6d3df82bcf4f59695db4386d8a628a8cfbca321c63ec7da02000a83d9b37"
    );
    assert_eq!(bytes, golden);
}

/// Regenerate the fixture only from the daemon's real run.status reply:
/// BROCA_UPDATE_GOLDEN=1 cargo test -p broca-module-serve --test it output_ceiling_status_matches_golden_through_real_serve
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn output_ceiling_status_matches_golden_through_real_serve() {
    let rig = Rig::start()
        .await
        .expect("the output-ceiling golden requires the real daemon");
    rig.upstream
        .output_ceiling
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let client = rig.client("output-ceiling").await;
    let run_id = send(&client, "send-output-ceiling", None).await;
    let frames = rig.finished("output-ceiling").await;
    assert_eq!(
        frames
            .iter()
            .filter(|frame| matches!(frame.record, WalRecord::ModelAttemptFinished { .. }))
            .count(),
        1
    );
    assert_eq!(rig.upstream.authorized_as.lock().unwrap().len(), 1);
    let status = broca_test_support::wait_until(
        "output_ceiling_status_matches_golden_through_real_serve: run status",
        || async {
            let status = client.run_status(&run_id).await.unwrap();
            if matches!(status, broca_wire::RunStatusResponse::Error { .. }) {
                Ok(status)
            } else {
                Err(format!("{status:?}"))
            }
        },
    )
    .await;
    let value = serde_json::to_value(&status).unwrap();
    assert_eq!(value["state"], "error");
    assert_eq!(value["error"]["class"], "permanent");
    assert_eq!(value["error"]["provider_code"], "output_ceiling");
    assert!(value["error"]["message"].as_str().unwrap().starts_with(
        "output ceiling reached mid tool call: finish=length, max_output_tokens=8192"
    ));
    assert!(!value.to_string().contains("PRIVATE-"));
    let bytes = serde_json::to_vec(&status).unwrap();
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../broca-wire/tests/golden/run_status.output_ceiling.json");
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, &bytes).unwrap();
    }
    let golden = std::fs::read(&path)
        .expect("generate the output-ceiling golden through the real serve path");
    use sha2::{Digest, Sha256};
    assert_eq!(
        format!("{:x}", Sha256::digest(&golden)),
        "309db3ddb13181b8be1cc1407ef818d4fc57613002861bdf514e5ddc490f01c5"
    );
    assert_eq!(bytes, golden);
}

/// Regenerate only from the daemon's real reply with BROCA_UPDATE_GOLDEN=1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn context_overflow_anthropic_status_matches_golden_through_real_serve() {
    let rig = Rig::start()
        .await
        .expect("the context-overflow golden requires the real daemon");
    rig.upstream
        .context_overflow
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let client = rig.client("context-overflow").await;
    let run_id = send(&client, "send-context-overflow", None).await;
    let frames = rig.finished("context-overflow").await;
    assert_eq!(
        frames
            .iter()
            .filter(|frame| matches!(frame.record, WalRecord::ModelAttemptFinished { .. }))
            .count(),
        1
    );
    assert_eq!(rig.upstream.authorized_as.lock().unwrap().len(), 1);
    let status = broca_test_support::wait_until(
        "context_overflow_anthropic_status_matches_golden_through_real_serve: run status",
        || async {
            let status = client.run_status(&run_id).await.unwrap();
            if matches!(status, broca_wire::RunStatusResponse::Error { .. }) {
                Ok(status)
            } else {
                Err(format!("{status:?}"))
            }
        },
    )
    .await;
    let value = serde_json::to_value(&status).unwrap();
    assert_eq!(value["state"], "error");
    assert_eq!(value["error"]["class"], "context_overflow");
    assert_eq!(value["error"]["provider_code"], "context_length_exceeded");
    assert_eq!(value["error"]["status"], 400);
    assert_eq!(value["retries_used"], 0);
    let bytes = serde_json::to_vec(&status).unwrap();
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../broca-wire/tests/golden/run_status.context_overflow_anthropic.json");
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, &bytes).unwrap();
    }
    let golden = std::fs::read(&path)
        .expect("generate the context-overflow golden through the real serve path");
    assert_eq!(bytes, golden);
}

/// Regenerate only from the daemon's real reply with BROCA_UPDATE_GOLDEN=1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn billing_exhausted_xai_status_matches_golden_through_real_serve() {
    let rig = Rig::start()
        .await
        .expect("the billing-exhausted golden requires the real daemon");
    rig.upstream
        .billing_exhausted_xai
        .store(true, std::sync::atomic::Ordering::SeqCst);
    rig.vault
        .account_ids
        .lock()
        .unwrap()
        .insert("apikey:xai".into(), "acct-x".into());
    let client = rig.client("billing-exhausted-xai").await;
    let mut xai_spec = spec("send-billing-exhausted-xai", None);
    xai_spec.provider = "xai";
    xai_spec.model = "grok-4.7";
    let run_id = match client.send(xai_spec).await.expect("send") {
        SendOutcome::Active { run_id } | SendOutcome::Finished { run_id, .. } => run_id,
        other => panic!("unexpected send outcome {other:?}"),
    };
    let frames = rig.finished("billing-exhausted-xai").await;
    assert_eq!(
        frames
            .iter()
            .filter(|frame| matches!(frame.record, WalRecord::ModelAttemptFinished { .. }))
            .count(),
        1
    );
    let status = broca_test_support::wait_until(
        "billing_exhausted_xai_status_matches_golden_through_real_serve: run status",
        || async {
            let status = client.run_status(&run_id).await.unwrap();
            if matches!(status, broca_wire::RunStatusResponse::Error { .. }) {
                Ok(status)
            } else {
                Err(format!("{status:?}"))
            }
        },
    )
    .await;
    let value = serde_json::to_value(&status).unwrap();
    assert_eq!(value["state"], "error");
    assert_eq!(value["error"]["class"], "permanent");
    assert_eq!(value["error"]["provider_code"], "billing_exhausted");
    assert_eq!(value["error"]["status"], 403);
    assert_eq!(value["error"]["account_ref"], "acct-x");
    assert_eq!(value["error"]["credential_id"], "apikey:xai");
    assert_eq!(value["retries_used"], 0);
    let bytes = serde_json::to_vec(&status).unwrap();
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../broca-wire/tests/golden/run_status.billing_exhausted_xai.json");
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, &bytes).unwrap();
    }
    let golden = std::fs::read(&path)
        .expect("generate the billing-exhausted golden through the real serve path");
    assert_eq!(bytes, golden);
}

/// The fake OpenAI upstream answers [`OPENAI_INVALID_SCHEMA_BODY`], a RECONSTRUCTED body: the
/// status, `error.type`, `error.code` and `error.param` of a production refusal, with message
/// text written for the test. Regenerate only from the daemon's real reply with
/// BROCA_UPDATE_GOLDEN=1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn request_invalid_openai_status_matches_golden_through_real_serve() {
    let rig = Rig::start()
        .await
        .expect("the request-invalid golden requires the real daemon");
    rig.vault
        .openai_apikey_listed
        .store(true, std::sync::atomic::Ordering::SeqCst);
    rig.upstream
        .request_invalid_openai
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let client = rig.client("request-invalid-openai").await;
    let mut openai_spec = spec("send-request-invalid-openai", None);
    openai_spec.provider = "openai";
    openai_spec.model = "gpt-5.5";
    let run_id = match client.send(openai_spec).await.expect("send") {
        SendOutcome::Active { run_id } | SendOutcome::Finished { run_id, .. } => run_id,
        other => panic!("unexpected send outcome {other:?}"),
    };
    let frames = rig.finished("request-invalid-openai").await;
    // One provider call: a request the provider refused as invalid is never re-sent.
    assert_eq!(
        frames
            .iter()
            .filter(|frame| matches!(frame.record, WalRecord::ModelAttemptFinished { .. }))
            .count(),
        1
    );
    assert_eq!(
        rig.upstream.authorized_as.lock().unwrap().as_slice(),
        ["apikey:openai"]
    );
    let status = broca_test_support::wait_until(
        "request_invalid_openai_status_matches_golden_through_real_serve: run status",
        || async {
            let status = client.run_status(&run_id).await.unwrap();
            if matches!(status, broca_wire::RunStatusResponse::Error { .. }) {
                Ok(status)
            } else {
                Err(format!("{status:?}"))
            }
        },
    )
    .await;
    let value = serde_json::to_value(&status).unwrap();
    assert_eq!(value["state"], "error");
    assert_eq!(value["error"]["class"], "permanent");
    assert_eq!(value["error"]["provider_code"], "request_invalid");
    assert_eq!(value["error"]["provider_subcode"], "invalid_json_schema");
    assert_eq!(value["error"]["status"], 400);
    // Not a credential's fault, so the run names no credential.
    assert!(value["error"].get("credential_id").is_none());
    assert_eq!(value["retries_used"], 0);
    let bytes = serde_json::to_vec(&status).unwrap();
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../broca-wire/tests/golden/run_status.request_invalid_openai.json");
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, &bytes).unwrap();
    }
    let golden = std::fs::read(&path)
        .expect("generate the request-invalid golden through the real serve path");
    assert_eq!(bytes, golden);
}

/// An OpenRouter request its credit cannot pay for ends the run `credit_insufficient` after one
/// attempt, naming the key in `credential_id` and the affordable `max_tokens`. The upstream answers
/// [`OPENROUTER_CREDIT_402_BODY`], reconstructed from a production refusal.
/// Regenerate only from the daemon's real reply with BROCA_UPDATE_GOLDEN=1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn credit_insufficient_openrouter_status_matches_golden_through_real_serve() {
    let rig = Rig::start()
        .await
        .expect("the credit-insufficient golden requires the real daemon");
    rig.upstream
        .credit_insufficient_openrouter
        .store(true, std::sync::atomic::Ordering::SeqCst);
    rig.vault
        .openrouter_listed
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let client = rig.client("credit-insufficient-openrouter").await;
    let mut openrouter_spec = spec("send-credit-insufficient-openrouter", None);
    openrouter_spec.provider = "openrouter";
    openrouter_spec.model = "openai/gpt-4.1";
    let run_id = match client.send(openrouter_spec).await.expect("send") {
        SendOutcome::Active { run_id } | SendOutcome::Finished { run_id, .. } => run_id,
        other => panic!("unexpected send outcome {other:?}"),
    };
    let frames = rig.finished("credit-insufficient-openrouter").await;
    assert_eq!(
        frames
            .iter()
            .filter(|frame| matches!(frame.record, WalRecord::ModelAttemptFinished { .. }))
            .count(),
        1,
        "an unaffordable request is never re-sent within the run"
    );
    assert!(rig
        .upstream
        .authorized_as
        .lock()
        .unwrap()
        .iter()
        .all(|id| id == "apikey:openrouter"));
    let status = broca_test_support::wait_until(
        "credit_insufficient_openrouter_status_matches_golden_through_real_serve: run status",
        || async {
            let status = client.run_status(&run_id).await.unwrap();
            if matches!(status, broca_wire::RunStatusResponse::Error { .. }) {
                Ok(status)
            } else {
                Err(format!("{status:?}"))
            }
        },
    )
    .await;
    let value = serde_json::to_value(&status).unwrap();
    assert_eq!(value["state"], "error");
    assert_eq!(value["error"]["class"], "permanent");
    assert_eq!(value["error"]["provider_code"], "credit_insufficient");
    assert_eq!(value["error"]["status"], 402);
    assert_eq!(value["error"]["credential_id"], "apikey:openrouter");
    assert_eq!(value["error"]["affordable_max_tokens"], 97335);
    assert!(value["error"].get("resets_at_ms").is_none());
    assert_eq!(value["retries_used"], 0);
    // An unaffordable request says nothing about whether the key is valid.
    assert!(
        rig.vault
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|(method, _)| method.starts_with("credential.list")
                || method.starts_with("credential.get")),
        "a credential-scoped refusal is never reported to the vault"
    );
    assert_golden_file(
        &serde_json::to_vec(&status).unwrap(),
        "run_status.credit_insufficient_openrouter.json",
    );
}

/// Send on the labelled Anthropic API key `apikey:anthropic:work`, pinned the way routing pins a
/// pooled key, and return the run's terminal `run.status` once it reports an error.
async fn pinned_anthropic_apikey_error_status(rig: &Rig, name: &str) -> (Value, Vec<u8>) {
    rig.vault
        .anthropic_apikey_listed
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let client = rig.client(name).await;
    let run_id = send(
        &client,
        &format!("send-{name}"),
        Some(broca_wire::AuthParams {
            method: "apikey".into(),
            account: Some("work".into()),
        }),
    )
    .await;
    let frames = rig.finished(name).await;
    assert_eq!(
        selection(&frames)["credential_id"],
        "apikey:anthropic:work",
        "the run froze the pinned key"
    );
    assert!(
        rig.upstream
            .authorized_as
            .lock()
            .unwrap()
            .iter()
            .all(|id| id == "apikey:anthropic:work"),
        "every provider call carried the pinned key"
    );
    let status = broca_test_support::wait_until(
        "pinned_anthropic_apikey_error_status: run status",
        || async {
            let status = client.run_status(&run_id).await.unwrap();
            if matches!(status, broca_wire::RunStatusResponse::Error { .. }) {
                Ok(status)
            } else {
                Err(format!("{status:?}"))
            }
        },
    )
    .await;
    // Neither the out-of-credit 400 nor the rate-limit 429 says the key itself is invalid, so the
    // daemon must not report it to the vault: the only vault calls are credential list/get reads.
    assert!(
        rig.vault
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|(method, _)| method.starts_with("credential.list")
                || method.starts_with("credential.get")),
        "a credential-scoped refusal is never reported to the vault"
    );
    (
        serde_json::to_value(&status).unwrap(),
        serde_json::to_vec(&status).unwrap(),
    )
}

fn assert_golden_file(bytes: &[u8], file: &str) {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../broca-wire/tests/golden")
        .join(file);
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, bytes).unwrap();
    }
    let golden = std::fs::read(&path)
        .unwrap_or_else(|_| panic!("generate {file} through the real serve path"));
    assert_eq!(bytes, golden);
}

/// An Anthropic API key out of credit ends the run `billing_exhausted` after one attempt, naming
/// the key in `credential_id` (an API key has no `account_ref`). The upstream answers
/// [`ANTHROPIC_CREDIT_BALANCE_BODY`], Anthropic's public shape and not a captured refusal.
/// Regenerate only from the daemon's real reply with BROCA_UPDATE_GOLDEN=1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn billing_exhausted_anthropic_apikey_status_matches_golden_through_real_serve() {
    let rig = Rig::start()
        .await
        .expect("the Anthropic billing-exhausted golden requires the real daemon");
    rig.upstream
        .credit_exhausted_anthropic
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let (value, bytes) =
        pinned_anthropic_apikey_error_status(&rig, "billing-exhausted-anthropic").await;
    assert_eq!(
        rig.upstream.authorized_as.lock().unwrap().len(),
        1,
        "never re-sent"
    );
    assert_eq!(value["state"], "error");
    assert_eq!(value["error"]["class"], "permanent");
    assert_eq!(value["error"]["provider_code"], "billing_exhausted");
    assert_eq!(value["error"]["status"], 400);
    assert_eq!(value["error"]["credential_id"], "apikey:anthropic:work");
    assert!(value["error"].get("account_ref").is_none());
    assert_eq!(value["retries_used"], 0);
    assert_golden_file(&bytes, "run_status.billing_exhausted_anthropic_apikey.json");
}

/// An ordinary Anthropic rate limit that outlasts the transport's in-step attempts ends the run
/// `transient` with the typed `rate_limited` code in place of Anthropic's `rate_limit_error`,
/// naming the key in `credential_id`. Regenerate only from the daemon's real reply with
/// BROCA_UPDATE_GOLDEN=1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transient_rate_limited_credential_status_matches_golden_through_real_serve() {
    let rig = Rig::start()
        .await
        .expect("the rate-limited golden requires the real daemon");
    rig.upstream
        .rate_limited_anthropic
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let (value, bytes) =
        pinned_anthropic_apikey_error_status(&rig, "rate-limited-credential").await;
    let sends = rig.upstream.authorized_as.lock().unwrap().len();
    assert!(
        sends > 1,
        "the 429 was retried within the step before ending the run: {sends}"
    );
    assert_eq!(value["state"], "error");
    assert_eq!(value["error"]["class"], "transient");
    assert_eq!(value["error"]["provider_code"], "rate_limited");
    assert_eq!(value["error"]["status"], 429);
    assert_eq!(value["error"]["credential_id"], "apikey:anthropic:work");
    assert!(value["error"].get("account_ref").is_none());
    assert_eq!(value["retries_used"], sends - 1);
    assert_golden_file(&bytes, "run_status.transient_rate_limited_credential.json");
}

#[cfg(feature = "test-clock")]
/// Regenerate only from the daemon's real reply with BROCA_UPDATE_GOLDEN=1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn account_limit_exhausted_code_assist_status_matches_golden_through_real_serve() {
    let rig = Rig::start()
        .await
        .expect("the Code Assist quota golden requires the real daemon");
    std::fs::write(
        rig.state_root.join("test-clock"),
        r#"{"now_ms":1800000000000}"#,
    )
    .unwrap();
    rig.upstream
        .account_limit_exhausted_code_assist
        .store(true, std::sync::atomic::Ordering::SeqCst);
    rig.vault
        .account_ids
        .lock()
        .unwrap()
        .insert("antigravity:google".into(), "acct-g".into());
    let client = rig.client("codeassist-account-limit").await;
    let mut google_spec = spec("send-codeassist-account-limit", None);
    google_spec.provider = "google";
    google_spec.model = "gemini-3.8-flash-high";
    let run_id = match client.send(google_spec).await.expect("send") {
        SendOutcome::Active { run_id } | SendOutcome::Finished { run_id, .. } => run_id,
        other => panic!("unexpected send outcome {other:?}"),
    };
    let frames = rig.finished("codeassist-account-limit").await;
    assert_eq!(
        frames
            .iter()
            .filter(|frame| matches!(frame.record, WalRecord::ModelAttemptFinished { .. }))
            .count(),
        1
    );
    let status = broca_test_support::wait_until(
        "account_limit_exhausted_code_assist_status_matches_golden_through_real_serve: run status",
        || async {
            let status = client.run_status(&run_id).await.unwrap();
            if matches!(status, broca_wire::RunStatusResponse::Error { .. }) {
                Ok(status)
            } else {
                Err(format!("{status:?}"))
            }
        },
    )
    .await;
    let value = serde_json::to_value(&status).unwrap();
    assert_eq!(value["state"], "error");
    assert_eq!(value["error"]["class"], "transient");
    assert_eq!(value["error"]["provider_code"], "account_limit_exhausted");
    assert_eq!(value["error"]["status"], 429);
    assert_eq!(value["error"]["account_ref"], "acct-g");
    assert_eq!(value["error"]["credential_id"], "antigravity:google");
    assert_eq!(value["error"]["resets_at_ms"], 1_800_294_489_199_u64);
    assert_eq!(value["retries_used"], 0);
    let bytes = serde_json::to_vec(&status).unwrap();
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../broca-wire/tests/golden/run_status.account_limit_exhausted_code_assist.json");
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, &bytes).unwrap();
    }
    let golden = std::fs::read(&path)
        .expect("generate the Code Assist account-limit golden through the real serve path");
    assert_eq!(bytes, golden);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paused_auth_unrouted_status_reports_the_frozen_default_path() {
    let Some(rig) = Rig::start().await else {
        return;
    };
    rig.upstream
        .reject_auth
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let client = rig.client("paused-auth-default").await;
    let run_id = send(&client, "send-paused-auth-default", None).await;
    let frames = rig
        .wait_for("paused-auth-default", "auth pause", |record| {
            matches!(
                record,
                WalRecord::RunPaused {
                    reason: broca_protocol::PauseReason::AuthRequired,
                    ..
                }
            )
        })
        .await;
    assert_eq!(selection(&frames)["account_decision"]["path"], "disabled");
    let status = broca_test_support::wait_until(
        "paused_auth_unrouted_status_reports_the_frozen_default_path: run status",
        || async {
            let status = client.run_status(&run_id).await.unwrap();
            if matches!(status, broca_wire::RunStatusResponse::Paused { .. }) {
                Ok(status)
            } else {
                Err(format!("{status:?}"))
            }
        },
    )
    .await;
    assert_eq!(
        serde_json::to_value(status).unwrap()["auth"],
        json!({"credential_id": "oauth:anthropic", "path": "default"})
    );
}

/// The golden comes from a real daemon with a scripted compiler refusal, never a live API.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grammar_too_large_status_and_memo_through_real_serve() {
    let rig = Rig::start()
        .await
        .expect("the grammar fallback golden requires the real daemon");
    rig.upstream
        .grammar_limit_anthropic
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let extra = json!({"structured_output":{"mode":"final_text","schema":{
        "type":"object","properties":{"answer":{"type":"string"}},"required":["answer"],"additionalProperties":false
    }}});
    let send = rig
        .raw_send("grammar-fallback", "grammar-first", extra.clone())
        .await
        .unwrap();
    let run = send["run_id"].as_str().unwrap();
    let frames = rig.finished_run("grammar-fallback", run).await;
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.record, WalRecord::StructuredOutputDowngraded { .. }))
            .count(),
        1
    );
    let client = rig.client("grammar-fallback").await;
    // The live actor can still report the run active for a moment after its terminal is
    // durable, so wait for the status to leave `active` before pinning it.
    let status = broca_test_support::wait_until(
        "grammar_too_large_status_and_memo_through_real_serve: run status",
        || async {
            let status = client.run_status(run).await.unwrap();
            if matches!(status, broca_wire::RunStatusResponse::Active { .. }) {
                Err(format!("{status:?}"))
            } else {
                Ok(status)
            }
        },
    )
    .await;
    let value = serde_json::to_value(&status).unwrap();
    assert_eq!(value["state"], "completed");
    assert_eq!(value["structured_output"]["enforcement"], "best_effort");
    assert_eq!(value["structured_output"]["reason"], "grammar_too_large");
    assert_eq!(value["structured_output"]["validation"], "passed");
    assert_eq!(value["retries_used"], 0);
    let bytes = serde_json::to_vec(&status).unwrap();
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../broca-wire/tests/golden/run_status.grammar_too_large.json");
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, &bytes).unwrap();
    }
    assert_eq!(
        bytes,
        std::fs::read(&path).expect("generate the grammar fallback golden through real serve")
    );
    let second = rig
        .raw_send("grammar-memo", "grammar-second", extra)
        .await
        .unwrap();
    let second_frames = rig
        .finished_run("grammar-memo", second["run_id"].as_str().unwrap())
        .await;
    assert!(!second_frames
        .iter()
        .any(|f| matches!(f.record, WalRecord::StructuredOutputDowngraded { .. })));
    let requests = rig.upstream.schema_requests.lock().unwrap();
    assert_eq!(requests.len(), 3, "memo admission sends no refused request");
    assert!(requests[0].pointer("/output_config/format").is_some());
    assert!(requests[1].pointer("/output_config/format").is_none());
    assert!(requests[2].pointer("/output_config/format").is_none());
}
