//! `session.warm` through the real registry, actor, WAL and export index, with the real
//! Anthropic transport against a local server that records every request body it receives.
//! Expected warm bodies are derived from the recorded base bytes by a plain text edit, and
//! expected digests from the server's own bytes, never from anything broca reports.

use super::tests::min_config;
use super::*;
use crate::cache::{CacheDeclaration, CacheDeclarer};
use crate::opener::DefaultOpener;
use crate::registry::LineageRegistry;
use crate::serve::{ServeError, ServeHandler, SessionReadRequest};
use broca_core::{
    run_loop_on_writer_admitted_with_prior_snapshot, AllowAll, BatchCtx, DispatchResult,
    ExportIndex, FileLeaseStore, FileWalStore, StartMode, ToolManifest, ToolPlane, ToolPlaneError,
};
use broca_protocol::content::{ContentBlock, Message, Role};
use broca_protocol::{BindIdentity, StepId, Usage};
use broca_provider::{
    families::{anthropic_frozen_config, frozen_config_for_family, AnthropicMessages},
    spec::WireFamilyId,
    variant::{FrozenVariant, VariantRequest},
    wire::{AuthHandle, CacheClass, CachePolicy, CacheTtl, FrozenRenderConfig, MarkerSyntax},
    LiveProvider,
};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::instrument::WithSubscriber;

/// What the server answers one connection with.
#[derive(Clone)]
enum Reply {
    /// A request asking for zero output receives cache-usage JSON; a text-generation request
    /// receives a completed text SSE response, so the test can release its final model step.
    WarmOrText,
    LowCachedWarmOrText,
    Full {
        status: u16,
        headers: Vec<(&'static str, &'static str)>,
        body: Vec<u8>,
    },
    /// A 200 that starts a stream and holds the connection until the client drops it: a model
    /// still generating.
    Stall,
    /// `reply`, sent only after `delay`.
    Late(Duration, Box<Reply>),
    /// `reply`, held back until the test releases it through the `Notify`. A release made before
    /// the request arrives is kept, so the order of the two never matters.
    Gated(Arc<tokio::sync::Notify>, Box<Reply>),
}

struct Server {
    url: String,
    bodies: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Server {
    async fn start(script: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let bodies = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = Arc::clone(&bodies);
        let task = tokio::spawn(async move {
            for reply in script {
                let (mut socket, _) = listener.accept().await.unwrap();
                let recorded = Arc::clone(&recorded);
                // Each connection on its own task, so a stalled step does not keep a warm from
                // being accepted.
                tokio::spawn(async move {
                    let body = read_body(&mut socket).await;
                    let reply = match reply {
                        Reply::WarmOrText
                            if serde_json::from_slice::<serde_json::Value>(&body).unwrap()
                                ["max_tokens"]
                                == 0 =>
                        {
                            warm_answer()
                        }
                        Reply::LowCachedWarmOrText
                            if serde_json::from_slice::<serde_json::Value>(&body).unwrap()
                                ["max_tokens"]
                                == 0 =>
                        {
                            low_cached_warm_answer()
                        }
                        Reply::WarmOrText | Reply::LowCachedWarmOrText => {
                            text_turn("tools finished")
                        }
                        reply => reply,
                    };
                    recorded.lock().unwrap().push(body);
                    write_reply(&mut socket, reply).await;
                });
            }
        });
        Self { url, bodies, task }
    }

    fn bodies(&self) -> Vec<Vec<u8>> {
        self.bodies.lock().unwrap().clone()
    }

    async fn wait_for(&self, count: usize) {
        broca_test_support::wait_for(
            "wait_for",
            async {
                loop {
                    if self.bodies.lock().unwrap().len() >= count {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            },
            || {
                format!(
                    "requests={}, wanted={count}",
                    self.bodies.lock().unwrap().len()
                )
            },
        )
        .await
    }
}

async fn read_body(socket: &mut TcpStream) -> Vec<u8> {
    let mut request = Vec::new();
    let mut chunk = [0u8; 8192];
    let header_end = loop {
        let read = socket.read(&mut chunk).await.unwrap();
        assert_ne!(read, 0, "client disconnected before sending the request");
        request.extend_from_slice(&chunk[..read]);
        if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
            break end + 4;
        }
    };
    let headers = String::from_utf8_lossy(&request[..header_end]).to_ascii_lowercase();
    let length = headers
        .lines()
        .find_map(|line| line.strip_prefix("content-length:"))
        .map(|value| value.trim().parse::<usize>().unwrap())
        .unwrap_or(0);
    while request.len() < header_end + length {
        let read = socket.read(&mut chunk).await.unwrap();
        assert_ne!(read, 0, "client disconnected mid-body");
        request.extend_from_slice(&chunk[..read]);
    }
    request[header_end..header_end + length].to_vec()
}

async fn write_reply(socket: &mut TcpStream, reply: Reply) {
    let reply = match reply {
        Reply::Late(delay, reply) => {
            tokio::time::sleep(delay).await;
            *reply
        }
        Reply::Gated(release, reply) => {
            release.notified().await;
            *reply
        }
        reply => reply,
    };
    match reply {
        Reply::WarmOrText | Reply::LowCachedWarmOrText | Reply::Late(..) | Reply::Gated(..) => {
            unreachable!("a late or gated reply is unwrapped above")
        }
        Reply::Full {
            status,
            headers,
            body,
        } => {
            let mut head = format!(
                "HTTP/1.1 {status} Scripted\r\nContent-Length: {}\r\nConnection: close\r\n",
                body.len()
            );
            for (name, value) in headers {
                head.push_str(&format!("{name}: {value}\r\n"));
            }
            head.push_str("\r\n");
            let _ = socket.write_all(head.as_bytes()).await;
            let _ = socket.write_all(&body).await;
            let _ = socket.flush().await;
        }
        Reply::Stall => {
            let start = sse(&[
                serde_json::json!({"type":"message_start","message":{"id":"msg_s","type":"message","role":"assistant","content":[],"model":"m","usage":{"input_tokens":3,"output_tokens":0}}}),
            ]);
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n",
                start.len() + 4096
            );
            let _ = socket.write_all(head.as_bytes()).await;
            let _ = socket.write_all(&start).await;
            let _ = socket.flush().await;
            let mut byte = [0u8; 1];
            while matches!(socket.read(&mut byte).await, Ok(n) if n > 0) {}
        }
    }
}

fn sse(events: &[serde_json::Value]) -> Vec<u8> {
    let mut body = String::new();
    for event in events {
        body.push_str(&format!(
            "event: {}\ndata: {event}\n\n",
            event["type"].as_str().unwrap()
        ));
    }
    body.into_bytes()
}

fn text_turn(text: &str) -> Reply {
    Reply::Full {
        status: 200,
        headers: vec![("Content-Type", "text/event-stream")],
        body: sse(&[
            serde_json::json!({"type":"message_start","message":{"id":"msg_t","type":"message","role":"assistant","content":[],"model":"m","usage":{"input_tokens":3,"output_tokens":0}}}),
            serde_json::json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
            serde_json::json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":text}}),
            serde_json::json!({"type":"content_block_stop","index":0}),
            serde_json::json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}),
            serde_json::json!({"type":"message_stop"}),
        ]),
    }
}

/// A successful warm's answer, with the usage it read and the unified rate-limit report.
fn warm_answer() -> Reply {
    Reply::Full {
        status: 200,
        headers: vec![
            ("Content-Type", "application/json"),
            ("anthropic-ratelimit-unified-status", "allowed"),
        ],
        body: br#"{"id":"msg_w","type":"message","role":"assistant","content":[],"usage":{"input_tokens":3,"cache_read_input_tokens":4000,"cache_creation_input_tokens":0,"output_tokens":0}}"#.to_vec(),
    }
}

fn low_cached_warm_answer() -> Reply {
    Reply::Full {
        status: 200,
        headers: vec![],
        body: br#"{"usage":{"input_tokens":164384,"cache_read_input_tokens":15616,"output_tokens":0}}"#.to_vec(),
    }
}

/// A warm answered without any usage report.
fn warm_answer_without_usage() -> Reply {
    Reply::Full {
        status: 200,
        headers: vec![("Content-Type", "application/json")],
        body: br#"{"id":"msg_w","type":"message","role":"assistant","content":[]}"#.to_vec(),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// The warm body for `base`, by editing its text: the one `max_tokens` value becomes 0 and the
/// one `"stream":true` member goes.
fn expected_warm_body(base: &[u8]) -> Vec<u8> {
    let text = String::from_utf8(base.to_vec()).unwrap();
    let value: serde_json::Value = serde_json::from_slice(base).unwrap();
    let max_tokens = format!(r#""max_tokens":{},"#, value["max_tokens"]);
    assert_eq!(text.matches(&max_tokens).count(), 1, "{text}");
    assert_eq!(text.matches(r#""stream":true,"#).count(), 1, "{text}");
    text.replacen(&max_tokens, r#""max_tokens":0,"#, 1)
        .replacen(r#""stream":true,"#, "", 1)
        .into_bytes()
}

/// A first-party Anthropic config frozen with 1h markers and adaptive thinking.
fn warmable(url: &str) -> FrozenRenderConfig {
    let mut frozen = anthropic_frozen_config("claude-opus-4-8");
    frozen.base_url = Some(url.to_owned());
    frozen.cache = Some(CachePolicy {
        version: 1,
        class: CacheClass::Markers {
            syntax: MarkerSyntax::AnthropicCacheControl,
            ttl: CacheTtl::OneHour,
            max_breakpoints: 4,
            lookback_blocks: 20,
            tail: None,
            omit_ttl: false,
        },
    });
    frozen.variant = Some(FrozenVariant {
        name: "high".into(),
        request: VariantRequest::Anthropic {
            thinking: Some(serde_json::json!({"type": "adaptive", "display": "summarized"})),
            effort: Some("high".into()),
        },
    });
    frozen
}

fn provider_for(frozen: FrozenRenderConfig) -> LiveProvider<AnthropicMessages> {
    LiveProvider::new(AnthropicMessages, frozen, AuthHandle::Bearer("t".into()))
}

/// Declares a session's cache with the provider families' own rules, as the serving layer does.
struct FamilyDeclarer;

impl CacheDeclarer for FamilyDeclarer {
    fn declare(&self, config: &broca_wal::RunConfig) -> Option<CacheDeclaration> {
        let frozen: FrozenRenderConfig =
            serde_json::from_value(config.frozen_render_config.clone()).ok()?;
        let declared = broca_provider::cache_warm::cache_declaration(
            &config.model.provider_module_id,
            &frozen,
            &config.tool_choice,
            &config.response_format,
        );
        Some(CacheDeclaration {
            ttl_ms: declared.ttl_ms,
            ttl_options_ms: declared.ttl_options_ms,
            read_refreshes_ttl: declared.read_refreshes_ttl,
            warm: declared.warm,
            basis: declared.basis.map(str::to_owned),
        })
    }
}

#[derive(Default)]
struct HeldWarmTool {
    started: tokio::sync::Notify,
    release: tokio::sync::Notify,
    clock: std::sync::Mutex<Option<(broca_core::CacheClock, u64)>>,
    anchored_at_ms: AtomicU64,
}
#[async_trait::async_trait]
impl ToolPlane for HeldWarmTool {
    async fn batch_started(
        &self,
        _ctx: &BatchCtx,
        _calls: &[broca_protocol::ToolCall],
        _manifest: &ToolManifest,
    ) -> Result<(), ToolPlaneError> {
        Ok(())
    }
    async fn batch_finished(&self, _ctx: &BatchCtx) -> Result<(), ToolPlaneError> {
        Ok(())
    }
    async fn dispatch(&self, _ctx: &BatchCtx, call: &broca_protocol::ToolCall) -> DispatchResult {
        // Synthetic TTLs measure the held-tool wait, not HTTP setup and WAL admission.
        if let Some((clock, ttl)) = self.clock.lock().unwrap().as_ref() {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let prefix = clock.read().and_then(|reading| reading.prefix_tokens);
            clock.note_sent(now, Some(*ttl));
            clock.note_usage(
                now,
                broca_protocol::Usage {
                    input_tokens: prefix,
                    ..Default::default()
                },
            );
            self.anchored_at_ms.store(now, Ordering::SeqCst);
        }
        self.started.notify_one();
        self.release.notified().await;
        DispatchResult::Completed(broca_protocol::ToolResult {
            tool_call_id: call.tool_call_id.clone(),
            output: broca_protocol::ToolOutput::Text {
                text: "done".into(),
            },
            is_error: false,
        })
    }
}
struct ConfiguredDeclarer(Arc<AtomicU64>);
impl CacheDeclarer for ConfiguredDeclarer {
    fn declare(&self, config: &broca_wal::RunConfig) -> Option<CacheDeclaration> {
        let mut declaration = FamilyDeclarer.declare(config)?;
        let ttl = self.0.load(Ordering::SeqCst);
        if ttl != 0 {
            declaration.ttl_ms = Some(ttl);
        }
        Some(declaration)
    }
}

struct ObservedWarmer {
    inner: Arc<dyn broca_core::keep_warm_wait::KeepWarmDriver>,
    skips: Arc<AtomicU64>,
    hold_automatic: Arc<std::sync::atomic::AtomicBool>,
    cadence_ms: Option<u64>,
}

#[derive(Clone, Debug, Default)]
struct WarmStop {
    reason: String,
    detail: Option<String>,
}

impl tracing::field::Visit for WarmStop {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "stop" {
            self.reason = value.into();
        }
    }
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        match field.name() {
            "stop" => self.reason = format!("{value:?}"),
            "detail" => self.detail = Some(format!("{value:?}")),
            _ => {}
        }
    }
}

#[derive(Clone, Default)]
struct WarmStops(Arc<std::sync::Mutex<Vec<WarmStop>>>);

impl tracing::Subscriber for WarmStops {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.target() == "cache_warm"
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        if event.metadata().target() == "cache_warm" {
            let mut stop = WarmStop::default();
            event.record(&mut stop);
            if !stop.reason.is_empty() {
                self.0.lock().unwrap().push(stop);
            }
        }
    }
}

#[async_trait::async_trait]
impl broca_core::keep_warm_wait::KeepWarmDriver for ObservedWarmer {
    fn timing(
        &self,
        config: &broca_wal::RunConfig,
    ) -> Result<broca_core::keep_warm_wait::WarmTiming, String> {
        let mut timing = self.inner.timing(config)?;
        if let Some(cadence) = self.cadence_ms {
            timing.lead_ms = timing.ttl_ms - cadence;
        }
        Ok(timing)
    }
    fn last_sent_at_ms(&self) -> Option<u64> {
        self.inner.last_sent_at_ms()
    }
    fn note_attempt(&self, attempt: &broca_protocol::ModelAttempt) {
        self.inner.note_attempt(attempt);
    }
    fn costs(
        &self,
        config: &broca_wal::RunConfig,
    ) -> Result<broca_core::keep_warm::WarmCosts, String> {
        self.inner.costs(config)
    }
    async fn warm(
        &self,
        config: &broca_wal::RunConfig,
        run_id: &RunId,
    ) -> broca_core::keep_warm_wait::WarmTick {
        // Ensure the manual request owns the gated reply before automatic IO can begin.
        while self.hold_automatic.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        let tick = self.inner.warm(config, run_id).await;
        if matches!(tick, broca_core::keep_warm_wait::WarmTick::Skipped) {
            self.skips.fetch_add(1, Ordering::SeqCst);
        }
        tick
    }
}

/// Runs the real loop with one provider, and sends warms through another (the same one unless
/// a test swaps in a different config or account).
struct WarmExecutor {
    held: HeldWarmTool,
    skips: Arc<AtomicU64>,
    hold_automatic: Arc<std::sync::atomic::AtomicBool>,
    cadence_ms: AtomicU64,
    stops: WarmStops,
    ttl_ms: Arc<AtomicU64>,
    costs: std::sync::Mutex<broca_core::keep_warm::WarmCosts>,
    quota: std::sync::Mutex<Option<String>>,
    quota_headers: std::sync::Mutex<Vec<std::collections::BTreeMap<String, String>>>,
    provider: LiveProvider<AnthropicMessages>,
    warm_provider: Box<dyn broca_core::Provider>,
    wal_store: Arc<FileWalStore>,
    /// Run ids that pause for re-authentication before their first step.
    pause: std::sync::Mutex<HashSet<String>>,
}

#[async_trait::async_trait]
impl RunExecutor for WarmExecutor {
    async fn execute(
        &self,
        job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, broca_core::RunError> {
        let mode = match job.kind {
            RunKind::FreshOrResume => StartMode::FreshOrResume,
            RunKind::AppendEpisode => StartMode::AppendEpisode,
            RunKind::ResumeAfterAuth => StartMode::ResumeAfterAuth,
        };
        let mut control = job.control;
        if let Some(inner) = control.keep_warm.take() {
            let cadence = self.cadence_ms.load(Ordering::SeqCst);
            control.keep_warm = Some(Arc::new(ObservedWarmer {
                inner,
                skips: self.skips.clone(),
                hold_automatic: self.hold_automatic.clone(),
                cadence_ms: if cadence != 0 {
                    Some(cadence)
                } else {
                    self.hold_automatic.load(Ordering::SeqCst).then_some(200)
                },
            }));
        }
        if self.pause.lock().unwrap().contains(&job.req.run_id.0) {
            control.pause_for_auth_at_start = Some(ProviderError {
                provider_subcode: None,
                refusal: None,
                class: ErrorClass::Auth,
                message: "needs reauth".into(),
                status: Some(401),
                retry_after_secs: None,
                provider_code: None,
                resets_at_ms: None,
                resets_at_source: None,
                limit_window: None,
                account_ref: None,
                credential_id: None,
            });
        }
        run_loop_on_writer_admitted_with_prior_snapshot(
            job.req,
            job.writer.as_ref(),
            mode,
            &self.provider,
            &self.held,
            &AllowAll,
            self.wal_store.as_ref(),
            None,
            control,
            &out,
            job.on_admitted,
            Some(&job.prior_snapshot),
        )
        // Scope log capture to this run future, even when it moves between runtime threads.
        .with_subscriber(self.stops.clone())
        .await
    }

    fn warm_costs(
        &self,
        _config: &broca_wal::RunConfig,
        _prefix: u64,
        _ttl: u64,
    ) -> Result<broca_core::keep_warm::WarmCosts, String> {
        Ok(*self.costs.lock().unwrap())
    }
    async fn warm_quota(
        &self,
        _config: &broca_wal::RunConfig,
        headers: &std::collections::BTreeMap<String, String>,
    ) -> Result<(), String> {
        self.quota_headers.lock().unwrap().push(headers.clone());
        match self.quota.lock().unwrap().clone() {
            Some(code)
                if code == "headers_90_percent"
                    && headers
                        .get("anthropic-ratelimit-unified-5h-utilization")
                        .is_none_or(|value| value.parse::<f64>().unwrap() < 0.9) =>
            {
                Ok(())
            }
            Some(code) => Err(code),
            None => Ok(()),
        }
    }

    async fn warm(
        &self,
        job: crate::executor::WarmJob,
    ) -> Result<broca_core::WarmSent, crate::executor::WarmError> {
        crate::executor::send_warm(self.warm_provider.as_ref(), job.base).await
    }
}

struct Fixture {
    registry: LineageRegistry,
    executor: Arc<WarmExecutor>,
    export: Arc<broca_store::SqliteStore>,
    key: BindIdentity,
    root: std::path::PathBuf,
    server: Server,
}

impl Fixture {
    async fn start(script: Vec<Reply>) -> Self {
        Self::with(script, |_| {}, provider_for).await
    }

    /// `run_config` adjusts the config the session runs (and records) with; `warm` builds the
    /// provider warms are sent through from that same config.
    async fn with<P: broca_core::Provider + 'static>(
        script: Vec<Reply>,
        run_config: impl FnOnce(&mut FrozenRenderConfig),
        warm: impl FnOnce(FrozenRenderConfig) -> P,
    ) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let server = Server::start(script).await;
        let root = broca_test_support::scratch_root().join(format!(
            "broca-cache-warm-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(root.join("project")).unwrap();
        let key = BindIdentity::new(root.join("project"), "runner", "session");
        let wal_store = Arc::new(FileWalStore::new(root.join("wal")));
        let mut frozen = warmable(&server.url);
        run_config(&mut frozen);
        let executor = Arc::new(WarmExecutor {
            held: Default::default(),
            skips: Default::default(),
            hold_automatic: Default::default(),
            cadence_ms: Default::default(),
            stops: Default::default(),
            ttl_ms: Arc::new(AtomicU64::new(0)),
            costs: std::sync::Mutex::new(broca_core::keep_warm::WarmCosts {
                warm: 1.0,
                bust: 1000.0,
            }),
            quota: Default::default(),
            quota_headers: Default::default(),
            provider: provider_for(frozen.clone()),
            warm_provider: Box::new(warm(frozen)),
            wal_store: Arc::clone(&wal_store),
            pause: Default::default(),
        });
        let export = Arc::new(broca_store::SqliteStore::open_in_memory().unwrap());
        let registry = Self::registry(&root, wal_store, &executor, &export);
        Self {
            registry,
            executor,
            export,
            key,
            root,
            server,
        }
    }

    fn registry(
        root: &std::path::Path,
        wal_store: Arc<FileWalStore>,
        executor: &Arc<WarmExecutor>,
        export: &Arc<broca_store::SqliteStore>,
    ) -> LineageRegistry {
        let opener = DefaultOpener::new(
            Arc::new(FileLeaseStore::new(root.join("leases"))),
            Arc::clone(&wal_store),
            Arc::clone(executor) as Arc<dyn RunExecutor>,
        )
        .with_export_index(export.clone())
        .with_charge_basis_resolver(Arc::new(|_: &str, _: &str| {
            (broca_core::ChargeBasis::MeteredApi, 1)
        }));
        let registry = LineageRegistry::new_with_reader(Arc::new(opener), wal_store, None);
        registry.set_cache_declarer(Arc::new(ConfiguredDeclarer(Arc::clone(&executor.ttl_ms))));
        registry
    }

    /// Stop this broca and start a new one over the same session, as a daemon restart does: the
    /// new registry's memory holds nothing about the session. On the way, `edit` rewrites the
    /// session's WAL, which is how a test dates what was sent before the restart.
    async fn restart_with(&mut self, edit: impl FnOnce(&mut Vec<WalRecord>)) {
        assert!(self.registry.stop_and_seal().await, "the old broca stopped");
        let mut records = self.records();
        edit(&mut records);
        std::fs::remove_dir_all(self.root.join("wal")).unwrap();
        let mut wal = FileWalStore::new(self.root.join("wal"))
            .open(&self.key)
            .expect("open the rewritten WAL");
        for record in &records {
            wal.append(1, record, true).expect("rewrite a record");
        }
        drop(wal);
        // A new store too: nothing the old broca's store held in memory survives a restart.
        // Only warms run after this, and the executor's own store serves runs alone.
        let wal_store = Arc::new(FileWalStore::new(self.root.join("wal")));
        self.registry = Self::registry(&self.root, wal_store, &self.executor, &self.export);
    }

    fn request(&self, run: &str, text: &str) -> RunRequest {
        let mut req = super::tests::text_req(&self.key, run);
        req.model = broca_protocol::request::ModelHandle::new("anthropic", "claude-opus-4-8");
        req.input = vec![Message::new(
            Role::User,
            vec![ContentBlock::Text { text: text.into() }],
        )];
        req
    }

    /// Send one message and wait until its run has ended.
    async fn turn(&self, run: &str, text: &str, kind: RunKind) {
        let outcome = self
            .registry
            .route_send(&self.key, self.request(run, text), kind)
            .await
            .unwrap();
        assert!(
            matches!(outcome, SendOutcome::Active { .. }),
            "unexpected send outcome {outcome:?}"
        );
        self.wait_idle().await;
    }

    async fn wait_idle(&self) {
        broca_test_support::wait_for(
            "wait_idle",
            async {
                loop {
                    if self.registry.route_status(&self.key).await.unwrap() == LineageStatus::Idle {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            },
            || format!("WAL records={}", self.records().len()),
        )
        .await
    }

    async fn warm(&self, lineage_id: Option<String>) -> Result<broca_wire::WarmReply, ServeError> {
        ServeHandler::warm(&self.registry, &self.key, lineage_id).await
    }

    async fn refused(&self, lineage_id: Option<String>) -> broca_wire::WarmRefusal {
        match self.warm(lineage_id).await {
            Err(ServeError::WarmRefused(refusal)) => *refusal,
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    fn records(&self) -> Vec<WalRecord> {
        FileWalStore::new(self.root.join("wal"))
            .read_records(&self.key)
            .unwrap()
            .into_iter()
            .map(|frame| frame.record)
            .collect()
    }

    async fn page(&self) -> broca_wire::SessionReadResponse {
        ServeHandler::read(
            &self.registry,
            SessionReadRequest {
                late_after: None,
                session: self.key.clone(),
                from_ordinal: Some(0),
                limit: Some(100),
                include_tools: false,
                after_mid: None,
                lineage_id: None,
                max_bytes: None,
            },
        )
        .await
        .unwrap()
    }

    /// Seed the session's WAL directly with one finished episode frozen with `frozen`, whose one
    /// step recorded `sha` as its request digest.
    fn seed_finished_episode(&self, frozen: &FrozenRenderConfig, provider: &str, sha: &str) {
        self.seed_with_auth(frozen, provider, sha, None);
    }

    fn seed_with_auth(
        &self,
        frozen: &FrozenRenderConfig,
        provider: &str,
        sha: &str,
        credential: Option<&str>,
    ) {
        let wal_store = FileWalStore::new(self.root.join("wal"));
        let mut wal = wal_store.open(&self.key).expect("open WAL to seed");
        let mut config = min_config();
        config.model = broca_protocol::request::ModelHandle::new(provider, "claude-opus-4-8");
        config.frozen_render_config = serde_json::to_value(frozen).unwrap();
        config.auth_selection = credential.map(|id| broca_wal::AuthSelection {
            credential_id: id.into(),
            account_ref: None,
            resolved_credential_id: None,
            vault_request: None,
            account_decision: None,
        });
        let records = [
            WalRecord::RunStarted {
                session_class: None,
                retention: None,
                hooks: None,
                title: None,
                on_restart: None,
                keep_warm: None,
                marker_order_version: None,
                manifest: None,
                scope: None,
                send_id: None,
                request_identity_digest: None,
                project_id: None,
                principal: None,
                ts_ms: Some(100),
                run_id: RunId("seeded".into()),
                session: self.key.clone(),
                config: Box::new(config),
                input: self.request("seeded", "hi").input.into(),
                origin: broca_wal::RunOrigin::Fresh,
                submission_id: None,
            },
            WalRecord::StepStarted { step_id: StepId(1) },
            WalRecord::ModelAttemptFinished {
                content_progress: Default::default(),
                step_id: StepId(1),
                attempt: 0,
                request_body_len: 10,
                request_body_sha256: sha.into(),
                http_status: Some(200),
                content_type: None,
                response_body_len: 5,
                response_body_sha256: "bb".into(),
                response_body_cap: None,
                outcome: broca_protocol::AttemptOutcome::Complete,
                ts_ms: 150,
                sent_at_ms: None,
                transport: None,
                websocket_request: None,
                websocket_close_code: None,
                input_transformations: None,
                input_transformations_unmeasured: false,
                credential_refresh: None,
                served_account_ref: None,
            },
            WalRecord::ModelStepFinished {
                hooks: None,
                step_id: StepId(1),
                assistant_message: broca_protocol::content::AssistantMessage {
                    message_id: broca_protocol::MessageId("m1".into()),
                    content: vec![ContentBlock::Text { text: "ok".into() }],
                }
                .into(),
                usage: Usage::default(),
                finish_reason: None,
                retries_used: 0,
                service_tier_used: None,
                speed_used: None,
            },
            WalRecord::RunFinished {
                ts_ms: Some(200),
                reason: broca_protocol::RunFinishReason::Completed,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            },
        ];
        for record in &records {
            wal.append(1, record, true).expect("seed a record");
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn attempt_digests(records: &[WalRecord]) -> Vec<String> {
    records
        .iter()
        .filter_map(|record| match record {
            WalRecord::ModelAttemptFinished {
                request_body_sha256,
                ..
            } => Some(request_body_sha256.clone()),
            _ => None,
        })
        .collect()
}

fn warm_records(records: &[WalRecord]) -> Vec<WalRecord> {
    records
        .iter()
        .filter(|record| matches!(record, WalRecord::CacheWarmed { .. }))
        .cloned()
        .collect()
}

fn epoch_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// After a run ends, a warm re-sends the request its last step sent, rebuilt from the WAL, with
/// only `max_tokens` and `stream` changed. It records one informational warm record, exports one
/// `warm` fact, reports what the provider read, and restarts the cache clock at its own send.
#[tokio::test(flavor = "multi_thread")]
async fn a_warm_after_the_run_re_sends_its_last_request_and_records_it() {
    let fixture = Fixture::start(vec![text_turn("hello"), warm_answer()]).await;
    fixture
        .turn("r1", "first question", RunKind::FreshOrResume)
        .await;
    let head_before = ServeHandler::head(&fixture.registry, &fixture.key)
        .await
        .unwrap();
    let clock_before = head_before.cache.expect("the run's send moved the clock");
    assert_eq!(clock_before.ttl_ms, Some(3_600_000));

    let reply = fixture.warm(None).await.expect("warmed");
    let bodies = fixture.server.bodies();
    assert_eq!(bodies.len(), 2);
    assert_eq!(reply.request_body_sha256, sha256_hex(&bodies[0]));
    assert_eq!(
        attempt_digests(&fixture.records()),
        vec![sha256_hex(&bodies[0])]
    );
    assert_eq!(bodies[1], expected_warm_body(&bodies[0]));
    assert_eq!(reply.warm_body_sha256, sha256_hex(&bodies[1]));
    assert_eq!(
        (
            reply.input_tokens,
            reply.cache_read_tokens,
            reply.cache_write_tokens
        ),
        (Some(3), Some(4000), Some(0))
    );
    assert_eq!(reply.ttl_ms, 3_600_000);
    assert_eq!(reply.expiry_basis, "estimated");

    let warms = warm_records(&fixture.records());
    let [WalRecord::CacheWarmed {
        run_id,
        step_id,
        request_body_sha256,
        warm_body_sha256,
        usage,
        ratelimit_headers,
        sent_at_ms,
        ttl_ms,
        expires_at_ms,
        ..
    }] = warms.as_slice()
    else {
        panic!("expected one warm record, got {warms:?}");
    };
    assert_eq!(run_id.0, "r1");
    assert_eq!(*step_id, StepId(1));
    assert_eq!(request_body_sha256, &reply.request_body_sha256);
    assert_eq!(warm_body_sha256, &reply.warm_body_sha256);
    assert_eq!(usage.cached_input_tokens, Some(4000));
    assert_eq!(
        ratelimit_headers.get("anthropic-ratelimit-unified-status"),
        Some(&"allowed".to_string())
    );
    assert_eq!(*ttl_ms, Some(3_600_000));
    assert_eq!(*expires_at_ms, Some(reply.expires_at_ms));
    // The expiry restarts at the warm's own send start.
    assert_eq!(reply.expires_at_ms, sent_at_ms + 3_600_000);
    assert!(*sent_at_ms >= clock_before.last_sent_at_ms);
    let head_after = ServeHandler::head(&fixture.registry, &fixture.key)
        .await
        .unwrap()
        .cache
        .unwrap();
    assert_eq!(head_after.last_sent_at_ms, *sent_at_ms);
    assert_eq!(head_after.expires_at_ms, Some(reply.expires_at_ms));

    // The export index holds the run's ordinary fact and one warm fact.
    let mut facts = Vec::new();
    broca_test_support::wait_for(
        "a_warm_after_the_run_re_sends_its_last_request_and_records_it",
        async {
            loop {
                facts = fixture.export.read_page(None, 10).unwrap().facts;
                if facts.iter().any(|fact| fact.segment.warm) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
        || format!("WAL records={}", fixture.records().len()),
    )
    .await;
    let warm_facts: Vec<_> = facts.iter().filter(|fact| fact.segment.warm).collect();
    assert_eq!(warm_facts.len(), 1, "{facts:?}");
    assert_eq!(warm_facts[0].segment.usage.cached_input_tokens, Some(4000));
    assert_eq!(facts.iter().filter(|fact| !fact.segment.warm).count(), 1);
}

/// A provider that reports no usage leaves every count absent, on the reply and on the record.
#[tokio::test(flavor = "multi_thread")]
async fn usage_the_provider_did_not_report_stays_absent() {
    let fixture = Fixture::start(vec![text_turn("hello"), warm_answer_without_usage()]).await;
    fixture.turn("r1", "q", RunKind::FreshOrResume).await;
    let reply = fixture.warm(None).await.expect("warmed");
    assert_eq!(
        (
            reply.input_tokens,
            reply.cache_read_tokens,
            reply.cache_write_tokens
        ),
        (None, None, None)
    );
    let warms = warm_records(&fixture.records());
    let [WalRecord::CacheWarmed { usage, .. }] = warms.as_slice() else {
        panic!("expected one warm record");
    };
    assert_eq!(*usage, Usage::default());
}

/// A warm changes neither the transcript nor the next real request: the session's page is
/// identical before and after, and the next turn's request is byte-identical to the same turn in
/// a session that was never warmed.
#[tokio::test(flavor = "multi_thread")]
async fn a_warm_changes_neither_the_transcript_nor_the_next_request() {
    let warmed = Fixture::start(vec![text_turn("one"), warm_answer(), text_turn("two")]).await;
    let control = Fixture::start(vec![text_turn("one"), text_turn("two")]).await;
    for fixture in [&warmed, &control] {
        fixture.turn("r1", "first", RunKind::FreshOrResume).await;
    }
    let before = warmed.page().await;
    warmed.warm(None).await.expect("warmed");
    assert_eq!(warmed.page().await, before);
    for fixture in [&warmed, &control] {
        fixture.turn("r2", "second", RunKind::AppendEpisode).await;
    }
    let warmed_bodies = warmed.server.bodies();
    let control_bodies = control.server.bodies();
    assert_eq!(warmed_bodies.len(), 3);
    assert_eq!(control_bodies.len(), 2);
    assert_eq!(
        String::from_utf8(warmed_bodies[2].clone()).unwrap(),
        String::from_utf8(control_bodies[1].clone()).unwrap()
    );
    assert_eq!(warmed.page().await.messages.len(), 4);
}

/// While the model is still generating, a warm sends the exact bytes the in-flight attempt sent:
/// its digest is the one that attempt's record carries once the attempt ends.
#[tokio::test(flavor = "multi_thread")]
async fn a_warm_mid_generation_sends_the_in_flight_bytes() {
    let fixture = Fixture::start(vec![Reply::Stall, warm_answer()]).await;
    let outcome = fixture
        .registry
        .route_send(
            &fixture.key,
            fixture.request("r1", "slow question"),
            RunKind::FreshOrResume,
        )
        .await
        .unwrap();
    assert!(matches!(outcome, SendOutcome::Active { .. }));
    fixture.server.wait_for(1).await;
    let reply = fixture.warm(None).await.expect("a running run is warmed");
    let bodies = fixture.server.bodies();
    assert_eq!(reply.request_body_sha256, sha256_hex(&bodies[0]));
    assert_eq!(bodies[1], expected_warm_body(&bodies[0]));

    fixture
        .registry
        .route_cancel(&fixture.key, RunId("r1".into()))
        .await
        .unwrap();
    fixture.wait_idle().await;
    let records = fixture.records();
    assert_eq!(
        attempt_digests(&records),
        vec![reply.request_body_sha256.clone()]
    );
    assert_eq!(warm_records(&records).len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_warm_naming_another_lineage_is_refused() {
    let fixture = Fixture::start(vec![text_turn("hello")]).await;
    fixture.turn("r1", "q", RunKind::FreshOrResume).await;
    let refusal = fixture.refused(Some("not-this-lineage".into())).await;
    assert_eq!(refusal.code, "lineage_changed");
    assert_eq!(fixture.server.bodies().len(), 1);
    assert!(warm_records(&fixture.records()).is_empty());
}

/// A paused run is never warmed, even after an earlier episode left a request to warm.
#[tokio::test(flavor = "multi_thread")]
async fn a_warm_on_a_paused_run_is_refused() {
    let fixture = Fixture::start(vec![text_turn("hello"), warm_answer()]).await;
    fixture.turn("r1", "q", RunKind::FreshOrResume).await;
    fixture.executor.pause.lock().unwrap().insert("r2".into());
    fixture.turn("r2", "q2", RunKind::AppendEpisode).await;
    let refusal = fixture.refused(None).await;
    assert_eq!(refusal.code, "run_paused");
    assert_eq!(
        refusal.detail,
        broca_wire::WarmRefusalDetail::RunPaused {
            run_id: "r2".into(),
            reason: broca_protocol::PauseReason::AuthRequired,
        }
    );
    assert_eq!(fixture.server.bodies().len(), 1, "nothing is sent");
    assert!(warm_records(&fixture.records()).is_empty());
}

/// An account found exhausted on the latest attempt is not warmed until it resets.
#[tokio::test(flavor = "multi_thread")]
async fn a_warm_on_an_exhausted_account_is_refused() {
    let fixture = Fixture::start(vec![Reply::Full {
        status: 429,
        headers: vec![
            ("Content-Type", "application/json"),
            ("anthropic-ratelimit-unified-status", "rejected"),
            ("anthropic-ratelimit-unified-reset", "4102444800"),
        ],
        body: br#"{"type":"error","error":{"type":"rate_limit_error","message":"limit"}}"#.to_vec(),
    }])
    .await;
    fixture.turn("r1", "q", RunKind::FreshOrResume).await;
    let refusal = fixture.refused(None).await;
    assert_eq!(
        refusal.detail,
        broca_wire::WarmRefusalDetail::AccountWalled {
            resets_at_ms: 4_102_444_800_000
        }
    );
    assert_eq!(fixture.server.bodies().len(), 1, "nothing is sent");
}

/// A credential that now belongs to another account is refused without sending.
#[tokio::test(flavor = "multi_thread")]
async fn a_warm_whose_credential_changed_account_is_refused() {
    let fixture = Fixture::with(
        vec![text_turn("hello"), warm_answer()],
        |_| {},
        |frozen| {
            provider_for(frozen).with_account_refs(Some("acct-1".into()), Some("acct-2".into()))
        },
    )
    .await;
    fixture.turn("r1", "q", RunKind::FreshOrResume).await;
    let refusal = fixture.refused(None).await;
    assert_eq!(
        refusal.detail,
        broca_wire::WarmRefusalDetail::AccountChanged {
            frozen_account: Some("acct-1".into()),
            served_account: Some("acct-2".into()),
        }
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(fixture.server.bodies().len(), 1, "nothing is sent");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_warm_before_any_step_is_refused() {
    let fixture = Fixture::start(vec![]).await;
    let refusal = fixture.refused(None).await;
    assert_eq!(refusal.detail, broca_wire::WarmRefusalDetail::NoStepYet);
    assert!(fixture.server.bodies().is_empty());
}

/// A session frozen without cache markers has no cached prefix to keep warm.
#[tokio::test(flavor = "multi_thread")]
async fn a_warm_of_a_session_without_cache_markers_is_unsupported() {
    let fixture = Fixture::with(
        vec![text_turn("hello"), warm_answer()],
        |frozen| frozen.cache = None,
        provider_for,
    )
    .await;
    fixture.turn("r1", "q", RunKind::FreshOrResume).await;
    assert_eq!(fixture.refused(None).await.code, "warm_unsupported");
    assert_eq!(fixture.server.bodies().len(), 1, "nothing is sent");
}

/// A Bedrock Claude session is declared unsupported with its reason, and sends nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_warm_of_a_bedrock_claude_session_is_unsupported() {
    let fixture = Fixture::start(vec![warm_answer()]).await;
    let mut bedrock =
        frozen_config_for_family(WireFamilyId::BedrockConverse, "anthropic.claude-opus-4-8");
    bedrock.cache = Some(CachePolicy {
        version: 1,
        class: CacheClass::Markers {
            syntax: MarkerSyntax::BedrockCachePoint,
            ttl: CacheTtl::FiveMinutes,
            max_breakpoints: 4,
            lookback_blocks: 20,
            tail: None,
            omit_ttl: false,
        },
    });
    fixture.seed_finished_episode(&bedrock, "amazon-bedrock", "aa");
    let refusal = fixture.refused(None).await;
    assert_eq!(
        refusal.detail,
        broca_wire::WarmRefusalDetail::WarmUnsupported {
            reason: broca_provider::cache_warm::BEDROCK_WARM_UNSUPPORTED.into()
        }
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(fixture.server.bodies().is_empty());
}

/// The expiry fence: a clock seeded from a WAL whose last request is long expired, and one with
/// under five seconds left, are both refused without sending.
#[tokio::test(flavor = "multi_thread")]
async fn a_warm_too_close_to_the_cache_expiry_is_refused() {
    let fixture = Fixture::start(vec![warm_answer()]).await;
    let frozen = warmable(&fixture.server.url);
    fixture.seed_finished_episode(&frozen, "anthropic", &"ab".repeat(32));

    // The seeded attempt records no send time, so the clock counts from its run's start.
    let expired = fixture.refused(None).await;
    assert!(matches!(
        expired.detail,
        broca_wire::WarmRefusalDetail::CacheExpired {
            expires_at_ms: Some(3_600_100),
            ..
        }
    ));

    let sent_at_ms = epoch_now_ms() - 3_600_000 + 3_000;
    fixture
        .registry
        .handle()
        .cache_clock(&fixture.key)
        .note_sent(sent_at_ms, Some(3_600_000));
    let close = fixture.refused(None).await;
    assert_eq!(close.code, "cache_expired");
    assert!(matches!(
        close.detail,
        broca_wire::WarmRefusalDetail::CacheExpired {
            expires_at_ms: Some(expires_at_ms),
            ..
        } if expires_at_ms == sent_at_ms + 3_600_000
    ));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(fixture.server.bodies().is_empty());
}

async fn codex_fixture(credential: &str, mismatch: bool) -> Fixture {
    use broca_provider::families::OpenAiResponsesChatgpt;
    let mut fixture = Fixture::with(vec![Reply::Full {
        status: 200, headers: vec![("Content-Type", "text/event-stream")],
        body: sse(&[serde_json::json!({"type":"response.incomplete","response":{"usage":{"input_tokens":20,"input_tokens_details":{"cached_tokens":10,"cache_write_tokens":0},"output_tokens":16}}})]),
    }], |_| {}, |mut frozen| {
        let url = frozen.base_url.take();
        let mut codex = frozen_config_for_family(WireFamilyId::OpenAiResponsesChatgpt, "gpt-5");
        codex.base_url = url;
        codex.reasoning_effort = Some("high".into());
        LiveProvider::new(OpenAiResponsesChatgpt, codex, AuthHandle::Bearer("t".into()))
    }).await;
    let mut frozen = frozen_config_for_family(WireFamilyId::OpenAiResponsesChatgpt, "gpt-5");
    frozen.base_url = Some(fixture.server.url.clone());
    frozen.reasoning_effort = Some("high".into());
    frozen.cache = Some(CachePolicy {
        version: 1,
        class: CacheClass::Hints {
            cache_key: "k".into(),
            retention: None,
        },
    });
    fixture.seed_with_auth(&frozen, "openai", &"ab".repeat(32), Some(credential));
    let recorded = broca_core::cache_warm::last_recorded_request(&fixture.records()).unwrap();
    let base = fixture
        .executor
        .warm_provider
        .render_request(&recorded.opts);
    let digest = sha256_hex(&base);
    fixture
        .restart_with(|records| {
            if !mismatch {
                for record in records {
                    if let WalRecord::ModelAttemptFinished {
                        request_body_sha256,
                        ..
                    } = record
                    {
                        *request_body_sha256 = digest.clone();
                    }
                }
            }
        })
        .await;
    fixture
}

#[tokio::test(flavor = "multi_thread")]
async fn codex_subscription_past_expiry_sends_after_verifying_uncapped_digest() {
    let fixture = codex_fixture("chatgpt:openai", false).await;
    let recorded = broca_core::cache_warm::last_recorded_request(&fixture.records()).unwrap();
    let base = fixture
        .executor
        .warm_provider
        .render_request(&recorded.opts);
    let expected = String::from_utf8(base.clone())
        .unwrap()
        .replacen(r#""model":"#, r#""max_output_tokens":16,"model":"#, 1)
        .into_bytes();
    let reply = fixture
        .warm(None)
        .await
        .expect("subscription re-primes an expired prefix");
    assert_eq!(reply.request_body_sha256, sha256_hex(&base));
    assert_eq!(fixture.server.bodies(), vec![expected]);
}

#[tokio::test(flavor = "multi_thread")]
async fn codex_mismatched_digest_refuses_before_adding_cap() {
    let fixture = codex_fixture("chatgpt:openai", true).await;
    let refusal = fixture.refused(None).await;
    assert_eq!(refusal.code, "warm_prefix_unavailable");
    assert!(fixture.server.bodies().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn metered_responses_past_expiry_refuses_cache_expired() {
    let fixture = codex_fixture("apikey:openai", false).await;
    // First-party OpenAI Responses uses the same hint TTL as subscription Responses.
    // Thus refusal after expiry is due to billed cache writes, not a different TTL.
    let mut records = fixture.records();
    for record in &mut records {
        if let WalRecord::RunStarted { config, .. } = record {
            let mut frozen: FrozenRenderConfig =
                serde_json::from_value(config.frozen_render_config.clone()).unwrap();
            frozen.family_id = WireFamilyId::OpenAiResponses;
            config.frozen_render_config = serde_json::to_value(frozen).unwrap();
        }
    }
    let mut fixture = fixture;
    fixture.restart_with(|saved| *saved = records).await;
    assert_eq!(fixture.refused(None).await.code, "cache_expired");
    assert!(fixture.server.bodies().is_empty());
}

/// The cache lifetime the warmable sessions here declare.
const ONE_HOUR_MS: u64 = 3_600_000;

/// Date the one model request of a finished episode: sent at `sent_at_ms`, its response ending
/// half a minute later, inside a run that started a second before it.
fn date_the_request(records: &mut [WalRecord], sent_at_ms: u64) {
    for record in records.iter_mut() {
        match record {
            WalRecord::RunStarted { ts_ms, .. } => *ts_ms = Some(sent_at_ms - 1_000),
            WalRecord::ModelAttemptFinished {
                ts_ms,
                sent_at_ms: sent,
                ..
            } => {
                *sent = Some(sent_at_ms);
                *ts_ms = sent_at_ms + 30_000;
            }
            WalRecord::RunFinished { ts_ms, .. } => *ts_ms = Some(sent_at_ms + 31_000),
            _ => {}
        }
    }
}

/// The cache clock `session.head` reports while a warm is still waiting for the provider: what
/// the warm was admitted on, before its answer moves the clock. The warm's answer must be the
/// one [`gated_warm_answer`] returned with `release`; it is let out only once `session.head` has
/// answered, so the reading cannot depend on how fast either side is scheduled.
async fn clock_while_warming(
    fixture: &Fixture,
    release: &tokio::sync::Notify,
) -> (
    Result<broca_wire::WarmReply, ServeError>,
    broca_wire::HeadCacheWire,
) {
    let head = async {
        fixture.server.wait_for(2).await;
        let cache = ServeHandler::head(&fixture.registry, &fixture.key)
            .await
            .unwrap()
            .cache
            .expect("the warm's admission seeded the clock");
        release.notify_one();
        cache
    };
    tokio::join!(fixture.warm(None), head)
}

/// A warm answer the server holds back until the returned `Notify` releases it.
fn gated_warm_answer() -> (Reply, Arc<tokio::sync::Notify>) {
    let release = Arc::new(tokio::sync::Notify::new());
    (
        Reply::Gated(Arc::clone(&release), Box::new(warm_answer())),
        release,
    )
}

/// After a restart the cache clock is rebuilt from the WAL: a session whose last request was
/// sent ten minutes ago, under a one-hour lifetime, is warmed, and its expiry counts from that
/// request's send, not from the end of its response.
#[tokio::test(flavor = "multi_thread")]
async fn after_a_restart_a_session_sent_ten_minutes_ago_is_warmed() {
    let (warm, release) = gated_warm_answer();
    let mut fixture = Fixture::start(vec![text_turn("hello"), warm]).await;
    fixture.turn("r1", "q", RunKind::FreshOrResume).await;
    let sent_at_ms = epoch_now_ms() - 10 * 60_000;
    fixture
        .restart_with(|records| date_the_request(records, sent_at_ms))
        .await;
    assert_eq!(
        fixture.registry.cache_clock_reading(&fixture.key),
        None,
        "the restarted broca holds no clock of its own"
    );

    let (reply, seeded) = clock_while_warming(&fixture, &release).await;
    assert_eq!(seeded.last_sent_at_ms, sent_at_ms);
    assert_eq!(seeded.prefix_tokens, Some(3));
    assert_eq!(seeded.expires_at_ms, Some(sent_at_ms + ONE_HOUR_MS));
    let reply = reply.expect("warmed");
    let bodies = fixture.server.bodies();
    assert_eq!(bodies[1], expected_warm_body(&bodies[0]));
    assert_eq!(reply.request_body_sha256, sha256_hex(&bodies[0]));
    assert_eq!(warm_records(&fixture.records()).len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn head_prefix_tokens_reports_the_latest_step_and_warm() {
    let fixture = Fixture::start(vec![text_turn("hello"), warm_answer()]).await;
    fixture.turn("r1", "q", RunKind::FreshOrResume).await;
    let head = ServeHandler::head(&fixture.registry, &fixture.key)
        .await
        .unwrap();
    assert_eq!(head.cache.unwrap().prefix_tokens, Some(3));
    fixture.warm(None).await.unwrap();
    let head = ServeHandler::head(&fixture.registry, &fixture.key)
        .await
        .unwrap();
    assert_eq!(head.cache.unwrap().prefix_tokens, Some(4003));
}

/// A restarted broca refuses a session whose last request, dated from the WAL, leaves under five
/// seconds of its lifetime, and reports the expiry counted from that request's send. Counted
/// from the end of the response instead, half a minute would remain and the warm would go out.
#[tokio::test(flavor = "multi_thread")]
async fn after_a_restart_a_session_sent_59m58s_ago_is_refused_as_expired() {
    let mut fixture = Fixture::start(vec![text_turn("hello"), warm_answer()]).await;
    fixture.turn("r1", "q", RunKind::FreshOrResume).await;
    let sent_at_ms = epoch_now_ms() - (59 * 60_000 + 58_000);
    fixture
        .restart_with(|records| date_the_request(records, sent_at_ms))
        .await;

    let refusal = fixture.refused(None).await;
    assert_eq!(refusal.code, "cache_expired");
    assert!(
        matches!(
            refusal.detail,
            broca_wire::WarmRefusalDetail::CacheExpired {
                expires_at_ms: Some(expires_at_ms),
                ..
            } if expires_at_ms == sent_at_ms + ONE_HOUR_MS
        ),
        "{refusal:?}"
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        fixture.server.bodies().len(),
        1,
        "only the run's own request"
    );
}

/// A warm recorded after the session's last request restarted the cache's lifetime, so after a
/// restart the clock counts from the warm. The request alone, 61 minutes old, would be expired.
#[tokio::test(flavor = "multi_thread")]
async fn after_a_restart_the_clock_counts_from_a_warm_newer_than_the_request() {
    let (warm, release) = gated_warm_answer();
    let mut fixture = Fixture::start(vec![text_turn("hello"), warm]).await;
    fixture.turn("r1", "q", RunKind::FreshOrResume).await;
    let now_ms = epoch_now_ms();
    let warmed_at_ms = now_ms - 10 * 60_000;
    fixture
        .restart_with(|records| {
            date_the_request(records, now_ms - 61 * 60_000);
            let request_body_sha256 = attempt_digests(records).pop().unwrap();
            records.push(WalRecord::CacheWarmed {
                trigger: None,
                run_id: RunId("r1".into()),
                step_id: StepId(1),
                request_body_sha256,
                warm_body_sha256: "bb".into(),
                usage: Usage::default(),
                served_account_ref: None,
                ratelimit_headers: Default::default(),
                sent_at_ms: warmed_at_ms,
                ttl_ms: Some(ONE_HOUR_MS),
                expires_at_ms: Some(warmed_at_ms + ONE_HOUR_MS),
            });
        })
        .await;

    let (reply, seeded) = clock_while_warming(&fixture, &release).await;
    assert_eq!(seeded.last_sent_at_ms, warmed_at_ms);
    assert_eq!(seeded.expires_at_ms, Some(warmed_at_ms + ONE_HOUR_MS));
    reply.expect("warmed");
}

/// An attempt that never got an HTTP status may never have reached the provider, so it does not
/// move the clock a restart rebuilds: the session's last accepted request, 61 minutes old, still
/// dates the cache, and the warm is refused as expired.
#[tokio::test(flavor = "multi_thread")]
async fn after_a_restart_an_attempt_without_a_status_does_not_move_the_clock() {
    let mut fixture = Fixture::start(vec![text_turn("hello"), warm_answer()]).await;
    fixture.turn("r1", "q", RunKind::FreshOrResume).await;
    let now_ms = epoch_now_ms();
    let accepted_at_ms = now_ms - 61 * 60_000;
    fixture
        .restart_with(|records| {
            date_the_request(records, accepted_at_ms);
            // A second episode whose one attempt failed to connect a minute ago.
            let mut second = records
                .iter()
                .find(|record| matches!(record, WalRecord::RunStarted { .. }))
                .unwrap()
                .clone();
            if let WalRecord::RunStarted { run_id, ts_ms, .. } = &mut second {
                *run_id = RunId("r2".into());
                *ts_ms = Some(now_ms - 61_000);
            }
            let mut failed = records
                .iter()
                .find(|record| matches!(record, WalRecord::ModelAttemptFinished { .. }))
                .unwrap()
                .clone();
            if let WalRecord::ModelAttemptFinished {
                http_status,
                content_type,
                response_body_len,
                outcome,
                sent_at_ms,
                ts_ms,
                ..
            } = &mut failed
            {
                *http_status = None;
                *content_type = None;
                *response_body_len = 0;
                *outcome = broca_protocol::AttemptOutcome::ConnectFailed;
                *sent_at_ms = Some(now_ms - 60_000);
                *ts_ms = now_ms - 59_000;
            }
            records.extend([
                second,
                WalRecord::StepStarted { step_id: StepId(1) },
                failed,
                WalRecord::RunFinished {
                    ts_ms: Some(now_ms - 58_000),
                    reason: broca_protocol::RunFinishReason::Error,
                    usage: Usage::default(),
                    error: None,
                    retries_used: 0,
                },
            ]);
        })
        .await;

    let refusal = fixture.refused(None).await;
    assert!(
        matches!(
            refusal.detail,
            broca_wire::WarmRefusalDetail::CacheExpired {
                expires_at_ms: Some(expires_at_ms),
                ..
            } if expires_at_ms == accepted_at_ms + ONE_HOUR_MS
        ),
        "{refusal:?}"
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        fixture.server.bodies().len(),
        1,
        "only the run's own request"
    );
}

/// When the WAL holds no request that reached the provider, a restarted broca still cannot tell
/// the cache's age and refuses as before, with the same reason.
#[tokio::test(flavor = "multi_thread")]
async fn after_a_restart_a_session_with_no_accepted_request_stays_unknown() {
    let mut fixture = Fixture::start(vec![text_turn("hello"), warm_answer()]).await;
    fixture.turn("r1", "q", RunKind::FreshOrResume).await;
    let sent_at_ms = epoch_now_ms() - 60_000;
    fixture
        .restart_with(|records| {
            date_the_request(records, sent_at_ms);
            for record in records.iter_mut() {
                if let WalRecord::ModelAttemptFinished {
                    http_status,
                    outcome,
                    ..
                } = record
                {
                    *http_status = None;
                    *outcome = broca_protocol::AttemptOutcome::NoStatus;
                }
            }
        })
        .await;

    let refusal = fixture.refused(None).await;
    assert_eq!(
        refusal.detail,
        broca_wire::WarmRefusalDetail::CacheExpired {
            expires_at_ms: None,
            reason: "no request of this session has been sent since this broca started, so the \
                     cache's expiry is unknown"
                .into(),
        }
    );
    assert_eq!(
        fixture.server.bodies().len(),
        1,
        "only the run's own request"
    );
}

/// A recorded request that does not render back to its recorded bytes is never sent: the
/// provider warms would go through renders a different body.
#[tokio::test(flavor = "multi_thread")]
async fn a_warm_whose_rebuilt_request_differs_is_refused_and_sends_nothing() {
    let fixture = Fixture::with(
        vec![text_turn("hello"), warm_answer()],
        |_| {},
        // The same server, so a warm sent in spite of the mismatch would be counted there.
        |mut frozen| {
            frozen.max_output_tokens += 1;
            provider_for(frozen)
        },
    )
    .await;
    fixture.turn("r1", "q", RunKind::FreshOrResume).await;
    let refusal = fixture.refused(None).await;
    assert_eq!(refusal.code, "warm_prefix_unavailable");
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        fixture.server.bodies().len(),
        1,
        "only the run's own request"
    );
    assert!(warm_records(&fixture.records()).is_empty());
}

/// A warm the provider refuses is reported with the provider's error and recorded nowhere.
#[tokio::test(flavor = "multi_thread")]
async fn a_warm_the_provider_refuses_is_reported_and_not_recorded() {
    let fixture = Fixture::start(vec![
        text_turn("hello"),
        Reply::Full {
            status: 529,
            headers: vec![("Content-Type", "application/json")],
            body: br#"{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}"#
                .to_vec(),
        },
    ])
    .await;
    fixture.turn("r1", "q", RunKind::FreshOrResume).await;
    let refusal = fixture.refused(None).await;
    assert_eq!(refusal.code, "warm_failed");
    assert!(warm_records(&fixture.records()).is_empty());
}

/// The admission reply's baseline and `session.baseline` both carry the session's cache
/// declaration.
#[tokio::test(flavor = "multi_thread")]
async fn the_admission_baseline_declares_the_cache() {
    let fixture = Fixture::start(vec![text_turn("hello")]).await;
    fixture.turn("r1", "q", RunKind::FreshOrResume).await;
    let expected = broca_wire::CacheDeclarationWire {
        ttl_ms: Some(3_600_000),
        ttl_options_ms: vec![300_000, 3_600_000],
        read_refreshes_ttl: Some(true),
        warm: Some("supported".into()),
        basis: Some("requested".into()),
    };
    let admitted =
        ServeHandler::admission_baseline(&fixture.registry, &fixture.key, &RunId("r1".into()))
            .expect("the admitting run's baseline");
    assert_eq!(admitted.cache, Some(expected.clone()));
    let baseline = ServeHandler::baseline(
        &fixture.registry,
        &fixture.key,
        None,
        &broca_protocol::SendScope::Unstamped,
    )
    .await
    .unwrap();
    assert_eq!(baseline.baseline.unwrap().cache, Some(expected));
}

/// A warm that hangs on its provider holds nothing of the session: a cancel of the session's
/// running run returns at once and `session.read` answers, while the warm is still waiting.
#[tokio::test(flavor = "multi_thread")]
async fn a_hanging_warm_holds_neither_cancel_nor_read() {
    let fixture = Arc::new(Fixture::start(vec![Reply::Stall, Reply::Stall]).await);
    let outcome = fixture
        .registry
        .route_send(
            &fixture.key,
            fixture.request("r1", "slow question"),
            RunKind::FreshOrResume,
        )
        .await
        .unwrap();
    assert!(matches!(outcome, SendOutcome::Active { .. }));
    fixture.server.wait_for(1).await;
    let warming = Arc::clone(&fixture);
    let hanging = tokio::spawn(async move { warming.warm(None).await });
    fixture.server.wait_for(2).await;

    let cancelled = tokio::time::timeout(
        Duration::from_secs(3),
        fixture
            .registry
            .route_cancel(&fixture.key, RunId("r1".into())),
    )
    .await
    .expect("run.cancel answers while the warm hangs");
    assert_eq!(cancelled.unwrap(), CancelOutcome::Ack);
    tokio::time::timeout(Duration::from_secs(3), fixture.page())
        .await
        .expect("session.read answers while the warm hangs");
    assert!(
        !hanging.is_finished(),
        "the warm is still waiting on its provider"
    );
    hanging.abort();
}

/// Only one warm of a session runs at a time: a second one while the first waits is refused
/// `warm_in_flight` and sends nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_warm_during_the_first_is_refused() {
    let fixture = Arc::new(Fixture::start(vec![text_turn("hello"), Reply::Stall]).await);
    fixture.turn("r1", "q", RunKind::FreshOrResume).await;
    let warming = Arc::clone(&fixture);
    let first = tokio::spawn(async move { warming.warm(None).await });
    fixture.server.wait_for(2).await;
    let second = tokio::time::timeout(Duration::from_secs(3), fixture.refused(None))
        .await
        .expect("the second warm is answered at once");
    assert_eq!(second.detail, broca_wire::WarmRefusalDetail::WarmInFlight);
    assert_eq!(second.code, "warm_in_flight");
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        fixture.server.bodies().len(),
        2,
        "the second warm sends nothing"
    );
    first.abort();
}

/// A warm that finishes after the session's next run started still writes its record (it is
/// informational) and exports its fact under the run whose request it re-sent.
#[tokio::test(flavor = "multi_thread")]
async fn a_warm_finishing_after_the_next_run_started_is_still_recorded() {
    let fixture = Arc::new(
        Fixture::start(vec![
            text_turn("one"),
            Reply::Late(Duration::from_millis(400), Box::new(warm_answer())),
            text_turn("two"),
        ])
        .await,
    );
    fixture.turn("r1", "first", RunKind::FreshOrResume).await;
    let warming = Arc::clone(&fixture);
    let warm = tokio::spawn(async move { warming.warm(None).await });
    fixture.server.wait_for(2).await;
    fixture.turn("r2", "second", RunKind::AppendEpisode).await;
    let reply = warm.await.unwrap().expect("warmed");

    let records = fixture.records();
    let second_start = records
        .iter()
        .position(
            |record| matches!(record, WalRecord::RunStarted { run_id, .. } if run_id.0 == "r2"),
        )
        .expect("the second run started");
    let warmed_at = records
        .iter()
        .position(|record| matches!(record, WalRecord::CacheWarmed { .. }))
        .expect("the warm is recorded");
    assert!(
        warmed_at > second_start,
        "the warm completed after the next run started"
    );
    let WalRecord::CacheWarmed {
        run_id,
        request_body_sha256,
        ..
    } = &records[warmed_at]
    else {
        unreachable!()
    };
    assert_eq!(run_id.0, "r1");
    assert_eq!(request_body_sha256, &reply.request_body_sha256);
    let mut warm_facts = 0;
    broca_test_support::wait_for(
        "a_warm_finishing_after_the_next_run_started_is_still_recorded",
        async {
            loop {
                warm_facts = fixture
                    .export
                    .read_page(None, 10)
                    .unwrap()
                    .facts
                    .iter()
                    .filter(|fact| fact.segment.warm && fact.segment.run_id.0 == "r1")
                    .count();
                if warm_facts == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
        || format!("WAL records={}", fixture.records().len()),
    )
    .await;
    assert_eq!(warm_facts, 1);
}

fn held_tool_turn() -> Reply {
    Reply::Full {
        status: 200,
        headers: vec![
            ("Content-Type", "text/event-stream"),
            ("anthropic-ratelimit-unified-5h-utilization", "0.89"),
        ],
        body: sse(&[
            serde_json::json!({"type":"message_start","message":{"id":"msg_tool","type":"message","role":"assistant","content":[],"model":"m","usage":{"input_tokens":3,"cache_read_input_tokens":4000,"output_tokens":0}}}),
            serde_json::json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call_wait","name":"wait","input":{}}}),
            serde_json::json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{}"}}),
            serde_json::json!({"type":"content_block_stop","index":0}),
            serde_json::json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":1}}),
            serde_json::json!({"type":"message_stop"}),
        ]),
    }
}
const HELD_CACHE_TTL_MS: u64 = 60_000;
const AUTOMATIC_CADENCE_MS: u64 = 100;

impl Fixture {
    fn held_request(&self, policy: Option<broca_protocol::request::KeepWarm>) -> RunRequest {
        let mut request = self.request("held-auto", "wait on a tool");
        request.keep_warm = policy;
        request.tools = vec![broca_protocol::Tool {
            name: "wait".into(),
            description: "held by the test".into(),
            input_schema: serde_json::json!({"type":"object"}),
            module: None,
            execution_mode: broca_protocol::ExecutionMode::Parallel,
        }];
        request.policy_allowed.insert("wait".into());
        request
    }
    async fn start_automatic_held(&self, policy: Option<broca_protocol::request::KeepWarm>) {
        // HTTP and WAL sync use real time, as does the cache clock. Keep a realistic
        // lifetime and shorten only the cadence; a slow reply must not masquerade as
        // a policy, quota or usage-based stop. Observe sends/stops after tool dispatch.
        self.executor
            .cadence_ms
            .store(AUTOMATIC_CADENCE_MS, Ordering::SeqCst);
        self.start_held(policy, HELD_CACHE_TTL_MS).await;
    }
    async fn start_held(&self, policy: Option<broca_protocol::request::KeepWarm>, ttl: u64) {
        self.executor.ttl_ms.store(ttl, Ordering::SeqCst);
        *self.executor.held.clock.lock().unwrap() =
            Some((self.registry.handle().cache_clock(&self.key), ttl));
        let request = self.held_request(policy);
        let outcome = self
            .registry
            .route_send(&self.key, request, RunKind::FreshOrResume)
            .await
            .unwrap();
        assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
        broca_test_support::wait_for(
            "held automatic run starts",
            self.executor.held.started.notified(),
            || self.warm_diagnostics(),
        )
        .await;
    }
    fn warm_requests(&self) -> usize {
        self.server
            .bodies()
            .iter()
            .filter(|body| {
                serde_json::from_slice::<serde_json::Value>(body).unwrap()["max_tokens"] == 0
            })
            .count()
    }
    fn warm_records(&self) -> Vec<WalRecord> {
        self.records()
            .into_iter()
            .filter(|record| matches!(record, WalRecord::CacheWarmed { .. }))
            .collect()
    }
    async fn wait_quota_observations(&self, wanted: usize) {
        broca_test_support::wait_until("automatic warm quota observations", || async {
            let observed = self.executor.quota_headers.lock().unwrap().len();
            (observed >= wanted)
                .then_some(())
                .ok_or_else(|| self.warm_diagnostics())
        })
        .await;
    }
    async fn wait_warms(&self, count: usize) {
        broca_test_support::wait_until("automatic warm records", || async {
            let observed = self.warm_records().len();
            (observed >= count).then_some(()).ok_or_else(|| {
                format!(
                    "warm_records={observed}, wanted={count}; {}",
                    self.warm_diagnostics()
                )
            })
        })
        .await;
    }
    fn warm_diagnostics(&self) -> String {
        format!(
            "requests: {}, skips: {}, stops: {:?}, quota checks: {}, cache clock: {:?}",
            self.warm_requests(),
            self.executor.skips.load(Ordering::SeqCst),
            self.executor.stops.0.lock().unwrap(),
            self.executor.quota_headers.lock().unwrap().len(),
            self.registry.handle().cache_clock(&self.key).read(),
        )
    }
    async fn wait_warm_stop(&self, reason: &str) -> WarmStop {
        broca_test_support::wait_for(
            "automatic warm stops",
            async {
                loop {
                    let stop = self.executor.stops.0.lock().unwrap().last().cloned();
                    if let Some(stop) = stop {
                        assert_eq!(
                            stop.reason,
                            reason,
                            "expected keep_warm stop {reason}; {}",
                            self.warm_diagnostics()
                        );
                        return stop;
                    }
                    tokio::task::yield_now().await;
                }
            },
            || self.warm_diagnostics(),
        )
        .await
    }
    async fn hold_first_warm_past_short_ttl(&self, release: &tokio::sync::Notify) {
        self.server.wait_for(2).await;
        // The request has already started sending. Hold its reply until even a cache
        // refreshed at receipt would be older than the short fixture's 200ms lifetime.
        // This forces response latency without depending on runtime load or timer jitter.
        tokio::time::sleep(Duration::from_millis(200)).await;
        release.notify_one();
    }
    async fn finish_held(&self) {
        self.executor.held.release.notify_one();
        self.wait_idle().await;
        let requests = self.warm_requests();
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(
            self.warm_requests(),
            requests,
            "tool completion disarms automatic warming"
        );
    }
}
fn automatic_script() -> Vec<Reply> {
    std::iter::once(held_tool_turn())
        .chain(std::iter::repeat_n(Reply::WarmOrText, 30))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn anthropic_low_cached_usage_keeps_automatic_warming() {
    let release = Arc::new(tokio::sync::Notify::new());
    let mut script = vec![
        held_tool_turn(),
        Reply::Gated(release.clone(), Box::new(low_cached_warm_answer())),
    ];
    script.extend(std::iter::repeat_n(Reply::LowCachedWarmOrText, 30));
    let fixture = Fixture::start(script).await;
    fixture
        .start_automatic_held(Some(broca_protocol::request::KeepWarm::Auto))
        .await;
    fixture.hold_first_warm_past_short_ttl(&release).await;
    fixture.wait_warms(3).await;
    for record in fixture.warm_records() {
        let WalRecord::CacheWarmed { usage, .. } = record else {
            unreachable!()
        };
        assert_eq!(usage.cached_input_tokens, Some(15_616));
        assert_eq!(usage.input_tokens, Some(164_384));
    }
    fixture.finish_held().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn automatic_keep_warm_records_trigger_and_refreshes() {
    let fixture = Fixture::start(automatic_script()).await;
    fixture
        .start_automatic_held(Some(broca_protocol::request::KeepWarm::Auto))
        .await;
    fixture.wait_warms(4).await;
    assert!(fixture.registry.route_status(&fixture.key).await.unwrap() != LineageStatus::Idle);
    let bodies = fixture.server.bodies();
    let expected = expected_warm_body(&bodies[0]);
    for body in &bodies[1..] {
        assert_eq!(body, &expected);
    }
    let mut previous_send = fixture.executor.held.anchored_at_ms.load(Ordering::SeqCst);
    for record in fixture.warm_records() {
        let WalRecord::CacheWarmed {
            trigger,
            request_body_sha256,
            warm_body_sha256,
            sent_at_ms,
            expires_at_ms,
            ..
        } = record
        else {
            unreachable!()
        };
        assert_eq!(trigger.as_deref(), Some("keep_warm"));
        assert_eq!(request_body_sha256, sha256_hex(&bodies[0]));
        assert_eq!(warm_body_sha256, sha256_hex(&expected));
        assert_eq!(expires_at_ms, Some(sent_at_ms + HELD_CACHE_TTL_MS));
        assert!(
            sent_at_ms < previous_send + HELD_CACHE_TTL_MS,
            "each request refreshes before the prior expiry"
        );
        previous_send = sent_at_ms;
    }
    fixture.finish_held().await;
    let warm_facts = fixture
        .export
        .read_page(None, 100)
        .unwrap()
        .facts
        .into_iter()
        .filter(|fact| fact.segment.warm && fact.segment.run_id.0 == "held-auto")
        .count();
    assert!(
        warm_facts >= 4,
        "every automatic warm produces a billed export fact"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn automatic_keep_warm_gate_limits_real_provider_requests() {
    let release = Arc::new(tokio::sync::Notify::new());
    let mut script = vec![
        held_tool_turn(),
        Reply::Gated(release.clone(), Box::new(warm_answer())),
    ];
    script.extend(std::iter::repeat_n(Reply::WarmOrText, 30));
    let fixture = Fixture::start(script).await;
    *fixture.executor.costs.lock().unwrap() = broca_core::keep_warm::WarmCosts {
        warm: 1.0,
        bust: 3.0,
    };
    // Test the spending boundary independently of cache expiry. A real cache lifetime
    // leaves room for HTTP and WAL sync; only the scheduler's cadence is shortened.
    fixture.executor.cadence_ms.store(100, Ordering::SeqCst);
    fixture
        .start_held(Some(broca_protocol::request::KeepWarm::Auto), 60_000)
        .await;
    fixture.hold_first_warm_past_short_ttl(&release).await;
    fixture.wait_warms(2).await;
    // Two requests alone could mean the next timer has not fired. Observe the scheduler's
    // budget stop, rather than accepting an expiry/quota stop or a quiet observation window.
    fixture.wait_warm_stop("budget").await;
    assert_eq!(
        fixture.warm_requests(),
        2,
        "the next billed request reaches the bust boundary"
    );
    assert_eq!(fixture.warm_records().len(), 2);
    fixture.finish_held().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn automatic_keep_warm_reports_cache_expired_after_a_slow_reply() {
    let release = Arc::new(tokio::sync::Notify::new());
    let mut script = vec![
        held_tool_turn(),
        Reply::Gated(release.clone(), Box::new(warm_answer())),
    ];
    script.extend(std::iter::repeat_n(Reply::WarmOrText, 30));
    let fixture = Fixture::start(script).await;
    fixture.executor.cadence_ms.store(100, Ordering::SeqCst);
    fixture
        .start_held(Some(broca_protocol::request::KeepWarm::Auto), 60_000)
        .await;
    fixture.server.wait_for(2).await;
    // Admit the first refresh with a realistic lifetime, then give the next attempt the
    // short lifetime. This isolates expiry after the reply from expiry before the first send.
    fixture.executor.ttl_ms.store(200, Ordering::SeqCst);
    fixture.hold_first_warm_past_short_ttl(&release).await;
    let stop = fixture.wait_warm_stop("unsupported").await;
    assert!(
        stop.detail
            .as_deref()
            .is_some_and(|detail| detail.contains("cache_expired")),
        "the overdue tick must stop for cache expiry, not warm_missed or a lost flight: {stop:?}"
    );
    assert_eq!(fixture.warm_records().len(), 1);
    assert_eq!(fixture.warm_requests(), 1);
    assert_eq!(fixture.executor.skips.load(Ordering::SeqCst), 0);
    fixture.finish_held().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn automatic_keep_warm_off_and_absent_do_not_send() {
    for policy in [None, Some(broca_protocol::request::KeepWarm::Off)] {
        let fixture = Fixture::start(automatic_script()).await;
        fixture.start_automatic_held(policy).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(fixture.warm_requests(), 0);
        fixture.finish_held().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn automatic_keep_warm_shares_the_real_manual_warm_flight() {
    let release = Arc::new(tokio::sync::Notify::new());
    let mut script = vec![
        held_tool_turn(),
        Reply::Gated(release.clone(), Box::new(warm_answer())),
    ];
    script.extend(std::iter::repeat_n(Reply::WarmOrText, 30));
    let fixture = Arc::new(Fixture::start(script).await);
    // Manual warming requires five seconds of remaining cache life. Keep a realistic
    // TTL, but shorten only the test driver's cadence and hold it until manual IO starts.
    fixture
        .executor
        .hold_automatic
        .store(true, Ordering::SeqCst);
    fixture
        .start_held(Some(broca_protocol::request::KeepWarm::Auto), 60_000)
        .await;
    let clock = fixture.registry.handle().cache_clock(&fixture.key);
    let prefix = clock.read().unwrap().prefix_tokens;
    assert!(
        prefix.is_some(),
        "held tool must retain model prefix: {:?}",
        clock.read()
    );
    let warming = fixture.clone();
    let manual = tokio::spawn(async move { warming.warm(None).await });
    fixture.server.wait_for(2).await;
    // Sending clears usage until the response arrives. Keep the fixture's known prefix
    // available while the response is held so the automatic tick reaches the shared guard.
    let sent_at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    clock.note_sent(sent_at_ms, Some(60_000));
    clock.note_usage(
        sent_at_ms,
        broca_protocol::Usage {
            input_tokens: prefix,
            ..Default::default()
        },
    );
    fixture
        .executor
        .hold_automatic
        .store(false, Ordering::SeqCst);
    broca_test_support::wait_until("automatic warm skip", || async {
        (fixture.executor.skips.load(Ordering::SeqCst) > 0)
            .then_some(())
            .ok_or_else(|| fixture.warm_diagnostics())
    })
    .await;
    assert_eq!(
        fixture.warm_requests(),
        1,
        "only the manual warm is sent while its response is held"
    );
    assert_eq!(fixture.refused(None).await.code, "warm_in_flight");
    release.notify_one();
    manual.await.unwrap().unwrap();
    fixture.wait_warms(2).await;
    assert_eq!(
        fixture.warm_requests(),
        2,
        "a skipped automatic tick reschedules after the manual warm"
    );
    fixture.finish_held().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn automatic_keep_warm_quota_decides_real_provider_warm_or_stop() {
    let cases = [
        (include_str!("../../broca-module-serve/tests/fixtures/route-account-status-v1/warm_ok.json"), true),
        (include_str!("../../broca-module-serve/tests/fixtures/route-account-status-v1/within_reserve.json"), false),
        (include_str!("../../broca-module-serve/tests/fixtures/route-account-status-v1/reading_stale.json"), false),
    ];
    for (vector, allow) in cases {
        let fixture = Fixture::start(automatic_script()).await;
        let reply: serde_json::Value = serde_json::from_str(vector).unwrap();
        if !reply["reply"]["result"]["warm_ok"].as_bool().unwrap() {
            *fixture.executor.quota.lock().unwrap() = Some(
                reply["reply"]["result"]["reason"]["code"]
                    .as_str()
                    .unwrap()
                    .into(),
            );
        }
        fixture
            .start_automatic_held(Some(broca_protocol::request::KeepWarm::Auto))
            .await;
        if allow {
            fixture.wait_warms(1).await;
        } else {
            fixture.wait_quota_observations(1).await;
            assert_eq!(fixture.warm_requests(), 0);
        }
        fixture.finish_held().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn automatic_keep_warm_rebuilds_the_clock_and_request_for_resumed_tools() {
    let mut fixture = Fixture::start(automatic_script()).await;
    *fixture.executor.quota.lock().unwrap() = Some("within_reserve".into());
    fixture
        .start_automatic_held(Some(broca_protocol::request::KeepWarm::Auto))
        .await;
    let base = fixture.server.bodies()[0].clone();
    fixture.finish_held().await;
    fixture
        .restart_with(|records| {
            // Restore a crash boundary before the tool could have left the process, so replay
            // may safely dispatch it again rather than invent a result for an ambiguous call.
            let intent = records
                .iter()
                .position(|record| matches!(record, WalRecord::ToolDispatchIntent { .. }))
                .unwrap();
            records.truncate(intent);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            for record in records {
                if let WalRecord::ModelAttemptFinished { sent_at_ms, .. } = record {
                    *sent_at_ms = Some(now);
                }
            }
        })
        .await;
    *fixture.executor.quota.lock().unwrap() = None;
    let before = fixture.warm_requests();
    let outcome = fixture
        .registry
        .route_send(
            &fixture.key,
            fixture.held_request(Some(broca_protocol::request::KeepWarm::Auto)),
            RunKind::FreshOrResume,
        )
        .await
        .unwrap();
    assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
    broca_test_support::wait_for(
        "restarted held automatic run starts",
        fixture.executor.held.started.notified(),
        || fixture.warm_diagnostics(),
    )
    .await;
    fixture.wait_warms(1).await;
    assert_eq!(
        fixture.warm_requests(),
        before + 1,
        "the recorded auto choice survives a cold restart"
    );
    let bodies = fixture.server.bodies();
    assert_eq!(
        bodies.last().unwrap(),
        &expected_warm_body(&base),
        "a new shutdown seal has no sent request, so the warm must reconstruct it from WAL"
    );
    fixture.finish_held().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn automatic_keep_warm_in_flight_does_not_hold_cancel_or_the_next_model_step() {
    for cancel in [false, true] {
        let mut script = vec![held_tool_turn(), Reply::Stall];
        script.extend(std::iter::repeat_n(Reply::WarmOrText, 10));
        let fixture = Fixture::start(script).await;
        fixture
            .start_automatic_held(Some(broca_protocol::request::KeepWarm::Auto))
            .await;
        fixture.server.wait_for(2).await;
        assert_eq!(
            fixture.refused(None).await.code,
            "warm_in_flight",
            "automatic and manual sends use the same guard"
        );
        if cancel {
            let reply = tokio::time::timeout(
                Duration::from_secs(2),
                fixture
                    .registry
                    .route_cancel(&fixture.key, RunId("held-auto".into())),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(reply, CancelOutcome::Ack);
        }
        tokio::time::timeout(Duration::from_secs(2), fixture.finish_held())
            .await
            .unwrap();
        assert!(
            fixture.warm_records().is_empty(),
            "a dropped, unanswered HTTP warm has no reported usage to record"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn automatic_keep_warm_quota_receives_the_latest_model_and_warm_headers() {
    let mut answer = warm_answer();
    if let Reply::Full { headers, .. } = &mut answer {
        headers.push(("anthropic-ratelimit-unified-5h-utilization", "0.90"));
    }
    let mut script = vec![held_tool_turn(), answer];
    script.extend(std::iter::repeat_n(Reply::WarmOrText, 10));
    let fixture = Fixture::start(script).await;
    *fixture.executor.quota.lock().unwrap() = Some("headers_90_percent".into());
    fixture
        .start_automatic_held(Some(broca_protocol::request::KeepWarm::Auto))
        .await;
    fixture.wait_warms(1).await;
    fixture.wait_quota_observations(2).await;
    assert_eq!(
        fixture.warm_requests(),
        1,
        "the next quota decision observes the warm's ninety-percent header"
    );
    let observations = fixture.executor.quota_headers.lock().unwrap().clone();
    assert_eq!(
        observations.len(),
        2,
        "a quota stop is decided once per wait"
    );
    assert_eq!(
        observations[0]["anthropic-ratelimit-unified-5h-utilization"],
        "0.89"
    );
    assert_eq!(
        observations[1]["anthropic-ratelimit-unified-5h-utilization"],
        "0.90"
    );
    fixture.finish_held().await;
}
