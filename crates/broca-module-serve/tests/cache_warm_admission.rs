//! Send unconfigured requests through the session registry, actor and core loop to a local
//! Codex backend. Admission uses the serving cache resolver and catalog to freeze the cache
//! policy and prompt_cache_key, instead of constructing a warmable config by hand.
//! This binary owns its catalog snapshot so installing it cannot race other serve tests.

use async_trait::async_trait;
use broca_catalog::live::{AuthMode, ResolveOptions, WireFamilyId};
use broca_core::{
    AllowAll, BatchCtx, DispatchResult, FileLeaseStore, FileWalStore, OutputEvent, RunError,
    RunSummary, StartMode, ToolManifest, ToolPlane, ToolPlaneError,
};
use broca_module::{
    actor::LineageStatus,
    executor::{RunExecutor, RunJob, WarmJob},
    serve::ServeHandler,
    DefaultOpener, LineageRegistry,
};
use broca_module_serve::{
    cache_warm::ProviderCacheDeclarer, resolve_cache_policy, resolve_caller_cache_key,
    wire::SendParamsExt, CacheTtlTable,
};
use broca_protocol::{BindIdentity, RunId, ToolCall, ToolOutput, ToolResult};
use broca_provider::wire::FrozenRenderConfig;
use broca_wal::WalRecord;
use serde_json::{json, Value};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, Notify},
};

#[derive(Default)]
struct HeldTool {
    started: Notify,
    release: Notify,
}
#[async_trait]
impl ToolPlane for HeldTool {
    async fn batch_started(
        &self,
        _: &BatchCtx,
        _: &[ToolCall],
        _: &ToolManifest,
    ) -> Result<(), ToolPlaneError> {
        Ok(())
    }
    async fn batch_finished(&self, _: &BatchCtx) -> Result<(), ToolPlaneError> {
        Ok(())
    }
    async fn dispatch(&self, _: &BatchCtx, call: &ToolCall) -> DispatchResult {
        self.started.notify_one();
        self.release.notified().await;
        DispatchResult::Completed(ToolResult {
            tool_call_id: call.tool_call_id.clone(),
            output: ToolOutput::Text {
                text: "done".into(),
            },
            is_error: false,
        })
    }
}

#[derive(Default)]
struct WarmConnectGate {
    connected: Notify,
    dropped: Notify,
}

struct PausedWarmConnection {
    _socket: TcpStream,
    gate: Arc<WarmConnectGate>,
}
impl Drop for PausedWarmConnection {
    fn drop(&mut self) {
        self.gate.dropped.notify_one();
    }
}

struct Executor {
    url: String,
    family: WireFamilyId,
    held: HeldTool,
    store: Arc<FileWalStore>,
    provider: Mutex<Option<Arc<Box<dyn broca_core::Provider>>>>,
    warm_connect_gate: Mutex<Option<Arc<WarmConnectGate>>>,
}
#[async_trait]
impl RunExecutor for Executor {
    async fn execute(
        &self,
        mut job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        let family = self.family;
        let policy = resolve_cache_policy(
            family,
            &job.req.model.provider_module_id,
            job.req.cache.as_ref(),
            &CacheTtlTable::shipped_defaults(),
            &job.req.session,
        )
        .unwrap();
        let key = resolve_caller_cache_key(job.req.cache.as_ref());
        let resolved = broca_catalog::live::resolve_provider_with_catalog_base_url(
            &format!(
                "{}/{}",
                job.req.model.provider_module_id, job.req.model.model_id
            ),
            &self.url,
            None,
            None,
            &AuthMode::Bearer(
                "eyJhbGciOiJub25lIn0.eyJjaGF0Z3B0X2FjY291bnRfaWQiOiAiYWNjdC10ZXN0In0.sig".into(),
            ),
            &ResolveOptions {
                family_override: Some(family),
                session_id: Some(job.req.session.session.as_str()),
                cache_policy: policy,
                prompt_cache_key: key.as_deref(),
                ..Default::default()
            },
            broca_provider::build_provider_http_client(),
        )?;
        let provider: Arc<Box<dyn broca_core::Provider>> = Arc::new(resolved.provider);
        // The admission/warm fixture serves SSE over HTTP and inspects POST bodies; socket
        // affinity is checked separately by the mixed-pipe catalog test.
        provider.set_chatgpt_https_policy(Arc::new(|| true));
        *self.provider.lock().unwrap() = Some(provider.clone());
        job.control.auth_selection = Some(broca_core::AuthSelection {
            credential_id: "chatgpt:openai".into(),
            account_ref: None,
            resolved_credential_id: None,
            vault_request: None,
            account_decision: None,
        });
        broca_core::run_loop_on_writer_admitted_with_prior_snapshot(
            job.req,
            job.writer.as_ref(),
            StartMode::FreshOrResume,
            provider.as_ref(),
            &self.held,
            &AllowAll,
            self.store.as_ref(),
            None,
            job.control,
            &out,
            job.on_admitted,
            Some(&job.prior_snapshot),
        )
        .await
    }
    fn warm_costs(
        &self,
        config: &broca_wal::RunConfig,
        prefix: u64,
        ttl: u64,
    ) -> Result<broca_core::keep_warm::WarmCosts, String> {
        broca_module_serve::cache_warm::automatic_warm_prices(config, prefix, ttl)
    }
    fn warm_after_send_ms(&self, _: &broca_wal::RunConfig) -> Option<u64> {
        Some(100)
    }
    fn responses_warm_cost(
        &self,
        config: &broca_wal::RunConfig,
        usage: broca_protocol::Usage,
    ) -> Result<f64, String> {
        broca_module_serve::cache_warm::responses_warm_cost(config, usage)
    }
    async fn warm(&self, job: WarmJob) -> Result<broca_core::WarmSent, broca_module::WarmError> {
        let gate = self.warm_connect_gate.lock().unwrap().take();
        if let Some(gate) = gate {
            // Pause this automatic warm's test transport after connecting but before writing.
            // Completing the held tool must drop the warm future and close this empty socket,
            // just as cancellation can interrupt the provider's HTTP request before its write.
            let socket = TcpStream::connect(self.url.strip_prefix("http://").unwrap())
                .await
                .unwrap();
            let _connection = PausedWarmConnection {
                _socket: socket,
                gate: gate.clone(),
            };
            gate.connected.notify_one();
            std::future::pending::<()>().await;
        }
        let provider = self.provider.lock().unwrap().clone().unwrap();
        broca_module::executor::send_warm(provider.as_ref(), job.base).await
    }
}

struct Fixture {
    registry: LineageRegistry,
    executor: Arc<Executor>,
    key: BindIdentity,
    bodies: Arc<Mutex<Vec<Vec<u8>>>>,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
impl Fixture {
    async fn start(held: bool) -> Self {
        Self::start_for(held, WireFamilyId::OpenAiResponsesChatgpt).await
    }
    async fn start_for(held: bool, family: WireFamilyId) -> Self {
        Self::start_with_usage(held, family, 3000, 4000).await
    }
    async fn start_with_usage(held: bool, family: WireFamilyId, cached: u64, total: u64) -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        static CATALOG: std::sync::Once = std::sync::Once::new();
        CATALOG.call_once(|| {
            let catalog = broca_catalog::Catalog::from_json_str(
                include_str!("../../broca-catalog/data/models.dev.json"),
                include_str!("../../broca-catalog/data/overlay.json"),
            )
            .unwrap();
            let cache = Arc::new(broca_catalog::CatalogCache::empty());
            cache.install_parsed_for_test(&catalog, 0, broca_catalog::CatalogSource::Fusiform);
            broca_catalog::install_global_catalog_cache(cache);
        });
        let root = broca_test_support::scratch_root().join(format!(
            "codex-admission-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let bodies = Arc::new(Mutex::new(Vec::new()));
        let received = bodies.clone();
        let server = tokio::spawn(async move {
            'connections: loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let mut chunk = [0; 8192];
                let end = loop {
                    let n = socket.read(&mut chunk).await.unwrap();
                    // Tool completion can cancel an automatic warm before it writes a full
                    // request. Discard that connection, not the server or later model requests.
                    if n == 0 {
                        continue 'connections;
                    }
                    bytes.extend_from_slice(&chunk[..n]);
                    if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        break end + 4;
                    }
                };
                let len: usize = String::from_utf8_lossy(&bytes[..end])
                    .to_ascii_lowercase()
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length:"))
                    .unwrap()
                    .trim()
                    .parse()
                    .unwrap();
                while bytes.len() < end + len {
                    let n = socket.read(&mut chunk).await.unwrap();
                    if n == 0 {
                        continue 'connections;
                    }
                    bytes.extend_from_slice(&chunk[..n]);
                }
                let body = bytes[end..end + len].to_vec();
                let warm =
                    serde_json::from_slice::<Value>(&body).unwrap()["max_output_tokens"] == 16;
                let first = {
                    let mut bodies = received.lock().unwrap();
                    let first = bodies.is_empty();
                    bodies.push(body);
                    first
                };
                let item = json!({"type":"function_call", "id":"fc_1", "call_id":"call_1", "name":"wait", "arguments":"{}"});
                let mut events = Vec::new();
                if held && first {
                    events.push(
                        json!({"type":"response.output_item.added", "output_index":0, "item":item}),
                    );
                    events.push(
                        json!({"type":"response.output_item.done", "output_index":0, "item":item}),
                    );
                }
                events.push(json!({"type": if warm {"response.incomplete"} else {"response.completed"}, "response":{"id":"resp_test","status":if warm {"incomplete"} else {"completed"},"output":[],"usage":{"input_tokens":total,"input_tokens_details":{"cached_tokens":cached},"output_tokens":if warm {16} else {1}}}}));
                let reply = events
                    .iter()
                    .map(|event| format!("data: {event}\n\n"))
                    .collect::<String>();
                let reply = match family {
                    WireFamilyId::OpenAiChat => format!(
                        "data: {}\n\ndata: [DONE]\n\n",
                        json!({"id":"chat_test", "choices":[{"index":0,"delta":{"content":"done"},"finish_reason":"stop"}], "usage":{"prompt_tokens":4,"completion_tokens":1}})
                    ),
                    WireFamilyId::Gemini => format!(
                        "data: {}\n\n",
                        json!({"candidates":[{"content":{"role":"model","parts":[{"text":"done"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":4,"candidatesTokenCount":1}})
                    ),
                    _ => reply,
                };
                let head = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", reply.len());
                socket.write_all(head.as_bytes()).await.unwrap();
                socket.write_all(reply.as_bytes()).await.unwrap();
            }
        });
        let store = Arc::new(FileWalStore::new(root.join("wal")));
        let executor = Arc::new(Executor {
            url,
            family,
            held: HeldTool::default(),
            store: store.clone(),
            provider: Mutex::new(None),
            warm_connect_gate: Mutex::new(None),
        });
        let opener = DefaultOpener::new(
            Arc::new(FileLeaseStore::new(root.join("leases"))),
            store.clone(),
            executor.clone(),
        )
        .with_charge_basis_resolver(Arc::new(|_: &str, _: &str| {
            (broca_core::ChargeBasis::SubscriptionIncluded, 1)
        }));
        let registry = LineageRegistry::new_with_reader(Arc::new(opener), store, None);
        registry.set_cache_declarer(Arc::new(ProviderCacheDeclarer));
        Self {
            registry,
            executor,
            key: BindIdentity::new(root.join("project"), "runner", "session"),
            bodies,
            server,
        }
    }
    async fn send(&self, cache: Option<Value>, auto: bool) {
        let (provider, model) = match self.executor.family {
            WireFamilyId::Gemini => ("google", "gemini-2.5-flash"),
            WireFamilyId::OpenAiChat => ("openai", "gpt-4o"),
            _ => ("openai", "gpt-5"),
        };
        let mut params = json!({"prompt":"do the work", "model":{"provider":provider,"model":model}, "tools":[{"name":"wait", "description":"wait", "input_schema":{"type":"object"}}]});
        if let Some(cache) = cache {
            params["cache"] = cache;
        }
        if auto {
            params["keep_warm"] = json!("auto");
        }
        let params: broca_wire::SendParams = serde_json::from_value(params).unwrap();
        let request = params
            .into_send_request(self.key.clone(), RunId("admitted".into()))
            .unwrap();
        ServeHandler::send(&self.registry, request).await.unwrap();
    }
    fn records(&self) -> Vec<WalRecord> {
        use broca_core::WalStore;
        self.executor
            .store
            .read_records(&self.key)
            .unwrap()
            .into_iter()
            .map(|frame| frame.record)
            .collect()
    }
    async fn idle(&self) {
        tokio::time::timeout(Duration::from_secs(4), async {
            while self.registry.route_status(&self.key).await.unwrap() != LineageStatus::Idle {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
    fn assert_unconfigured(&self) {
        let frozen = self
            .records()
            .into_iter()
            .find_map(|record| match record {
                WalRecord::RunStarted { config, .. } => Some(config.frozen_render_config),
                _ => None,
            })
            .unwrap();
        let frozen: FrozenRenderConfig = serde_json::from_value(frozen).unwrap();
        assert!(frozen.cache.is_none());
        assert_eq!(frozen.prompt_cache_key.as_deref(), Some("session"));
        assert_eq!(frozen.family_id, WireFamilyId::OpenAiResponsesChatgpt);
        assert_eq!(
            serde_json::from_slice::<Value>(&self.bodies.lock().unwrap()[0]).unwrap()
                ["prompt_cache_key"],
            "session"
        );
    }
}

#[tokio::test]
async fn unconfigured_codex_admission_session_warm_sends_capped_request() {
    let fixture = Fixture::start(false).await;
    fixture.send(None, false).await;
    fixture.idle().await;
    fixture.assert_unconfigured();
    ServeHandler::warm(&fixture.registry, &fixture.key, None)
        .await
        .expect("unconfigured Codex is warmable");
    assert_eq!(
        serde_json::from_slice::<Value>(&fixture.bodies.lock().unwrap()[1]).unwrap()
            ["max_output_tokens"],
        16
    );
    fixture.registry.stop_and_seal().await;
}

#[tokio::test]
async fn unconfigured_codex_admission_auto_warms_during_long_tool_wait() {
    let fixture = Fixture::start(true).await;
    fixture.send(None, true).await;
    tokio::time::timeout(
        Duration::from_secs(4),
        fixture.executor.held.started.notified(),
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(4), async {
        while !fixture.records().iter().any(|record| matches!(record, WalRecord::CacheWarmed { trigger: Some(trigger), .. } if trigger == "keep_warm")) { tokio::time::sleep(Duration::from_millis(10)).await; }
    }).await.unwrap();
    fixture.assert_unconfigured();
    assert_ne!(
        fixture.registry.route_status(&fixture.key).await.unwrap(),
        LineageStatus::Idle
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&fixture.bodies.lock().unwrap()[1]).unwrap()
            ["max_output_tokens"],
        16
    );
    fixture.executor.held.release.notify_one();
    fixture.idle().await;
    fixture.registry.stop_and_seal().await;
}

#[tokio::test]
async fn codex_partial_head_miss_stops_auto_warms_but_explicit_warm_remains_available() {
    let fixture =
        Fixture::start_with_usage(true, WireFamilyId::OpenAiResponsesChatgpt, 15_616, 180_000)
            .await;
    fixture.send(None, true).await;
    fixture.executor.held.started.notified().await;
    tokio::time::sleep(Duration::from_millis(650)).await;
    let warms: Vec<_> = fixture
        .records()
        .into_iter()
        .filter_map(|record| match record {
            WalRecord::CacheWarmed {
                trigger: Some(trigger),
                usage,
                ..
            } if trigger == "keep_warm" => Some(usage),
            _ => None,
        })
        .collect();
    assert_eq!(warms.len(), 1);
    assert_eq!(warms[0].cached_input_tokens, Some(15_616));
    assert_eq!(warms[0].input_tokens, Some(164_384));
    fixture.executor.held.release.notify_one();
    fixture.idle().await;
    ServeHandler::warm(&fixture.registry, &fixture.key, None)
        .await
        .unwrap();
    fixture.registry.stop_and_seal().await;
}

#[tokio::test]
async fn codex_prefix_hit_keeps_auto_warm_cadence() {
    let fixture =
        Fixture::start_with_usage(true, WireFamilyId::OpenAiResponsesChatgpt, 170_000, 180_000)
            .await;
    fixture.send(None, true).await;
    fixture.executor.held.started.notified().await;
    tokio::time::sleep(Duration::from_millis(650)).await;
    assert!(fixture.records().iter().filter(|record| matches!(record, WalRecord::CacheWarmed { trigger: Some(trigger), .. } if trigger == "keep_warm")).count() >= 2);
    fixture.executor.held.release.notify_one();
    fixture.idle().await;
    fixture.registry.stop_and_seal().await;
}

#[tokio::test]
async fn cancelled_auto_warm_connection_does_not_stop_admission_server() {
    let fixture =
        Fixture::start_with_usage(true, WireFamilyId::OpenAiResponsesChatgpt, 170_000, 180_000)
            .await;
    fixture.send(None, true).await;
    tokio::time::timeout(
        Duration::from_secs(4),
        fixture.executor.held.started.notified(),
    )
    .await
    .unwrap();
    let warm_count = || {
        fixture.records().iter().filter(|record| matches!(record, WalRecord::CacheWarmed { trigger: Some(trigger), .. } if trigger == "keep_warm")).count()
    };
    tokio::time::timeout(Duration::from_secs(4), async {
        while warm_count() < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    let gate = Arc::new(WarmConnectGate::default());
    *fixture.executor.warm_connect_gate.lock().unwrap() = Some(gate.clone());
    tokio::time::timeout(Duration::from_secs(4), gate.connected.notified())
        .await
        .unwrap();
    let completed_warms = warm_count();
    assert!(completed_warms >= 2, "prefix hits retain automatic cadence");
    fixture.executor.held.release.notify_one();
    tokio::time::timeout(Duration::from_secs(4), gate.dropped.notified())
        .await
        .expect("tool completion drops the connected automatic warm");
    fixture.idle().await;
    assert_eq!(
        warm_count(),
        completed_warms,
        "cancelled warms have no usage"
    );
    fixture.assert_unconfigured();
    {
        let bodies = fixture.bodies.lock().unwrap();
        assert_eq!(
            bodies.len(),
            completed_warms + 2,
            "only complete requests count"
        );
        assert_eq!(
            bodies
                .iter()
                .filter(
                    |body| serde_json::from_slice::<Value>(body).unwrap()["max_output_tokens"]
                        == 16
                )
                .count(),
            completed_warms,
            "completed automatic warms keep their output cap"
        );
    }
    assert!(!fixture.server.is_finished());
    fixture.registry.stop_and_seal().await;
}

async fn assert_admission_after_incomplete_request(request: &[u8]) {
    let fixture = Fixture::start(false).await;
    let mut socket = TcpStream::connect(fixture.executor.url.strip_prefix("http://").unwrap())
        .await
        .unwrap();
    socket.write_all(request).await.unwrap();
    socket.shutdown().await.unwrap();
    let mut reply = Vec::new();
    tokio::time::timeout(Duration::from_secs(4), socket.read_to_end(&mut reply))
        .await
        .unwrap()
        .unwrap();
    assert!(reply.is_empty(), "incomplete requests receive no response");
    assert!(
        !fixture.server.is_finished(),
        "EOF must not kill the server"
    );
    assert!(fixture.bodies.lock().unwrap().is_empty());
    fixture.send(None, false).await;
    fixture.idle().await;
    fixture.assert_unconfigured();
    assert_eq!(fixture.bodies.lock().unwrap().len(), 1);
    fixture.registry.stop_and_seal().await;
}

#[tokio::test]
async fn admission_server_survives_eof_during_headers() {
    assert_admission_after_incomplete_request(b"POST / HTTP/1.1\r\nContent-Length: 2\r\n").await;
}

#[tokio::test]
async fn admission_server_survives_eof_during_body() {
    assert_admission_after_incomplete_request(b"POST / HTTP/1.1\r\nContent-Length: 2\r\n\r\n{")
        .await;
}

#[tokio::test]
async fn codex_cache_mode_none_retains_wire_key_and_explicit_warm() {
    let fixture = Fixture::start(false).await;
    fixture.send(Some(json!({"mode":"none"})), false).await;
    fixture.idle().await;
    fixture.assert_unconfigured();
    ServeHandler::warm(&fixture.registry, &fixture.key, None)
        .await
        .unwrap();
    fixture.registry.stop_and_seal().await;
}

#[tokio::test]
async fn unconfigured_chat_and_gemini_admission_remain_unsupported() {
    for family in [WireFamilyId::OpenAiChat, WireFamilyId::Gemini] {
        let fixture = Fixture::start_for(false, family).await;
        fixture.send(None, false).await;
        fixture.idle().await;
        let config = fixture
            .records()
            .into_iter()
            .find_map(|record| match record {
                WalRecord::RunStarted { config, .. } => Some(config),
                _ => None,
            })
            .unwrap();
        let frozen: FrozenRenderConfig =
            serde_json::from_value(config.frozen_render_config).unwrap();
        assert!(frozen.cache.is_none());
        assert_eq!(frozen.family_id, family);
        let refusal = ServeHandler::warm(&fixture.registry, &fixture.key, None).await;
        assert!(
            matches!(refusal, Err(broca_module::serve::ServeError::WarmRefused(ref refusal)) if refusal.code == "warm_unsupported"),
            "{family:?}: {refusal:?}"
        );
        assert_eq!(
            fixture.bodies.lock().unwrap().len(),
            1,
            "unsupported warming sends nothing"
        );
        fixture.registry.stop_and_seal().await;
    }
}
