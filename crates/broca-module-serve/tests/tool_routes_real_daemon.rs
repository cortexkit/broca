//! Route lifetime through a real subc router, with the actor in-process and a stand-in provider.
#![cfg(feature = "test-route-close")]

#[path = "../../broca-subc/tests/common/mod.rs"]
mod common;

use async_trait::async_trait;
use broca_core::{FileLeaseStore, FileWalStore, RunError, RunSummary, WalStore};
use broca_module::{
    executor::{RunExecutor, RunJob, RunKind},
    opener::DefaultOpener,
    LineageRegistry,
};
use broca_protocol::{BindIdentity, RunFinishReason, RunId};
use broca_subc::SubcToolPlane;
use broca_wal::{RunConfig, WalRecord};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use subc_protocol::manifest::{
    Concurrency, ExecutionMode, IdentityScope, ModuleManifest, ProviderRole, Tool,
};

#[derive(Default)]
struct Observed {
    bound: AtomicUsize,
    gone: AtomicUsize,
    calls: Mutex<Vec<(u16, u32)>>,
}
struct Provider(Arc<Observed>);
#[async_trait]
impl subc_client_rs::ModuleHandler for Provider {
    async fn handle(
        &self,
        ctx: subc_client_rs::RequestCtx,
        _: Vec<u8>,
    ) -> subc_client_rs::HandlerOutcome {
        let route = ctx.route_handle();
        self.0
            .calls
            .lock()
            .unwrap()
            .push((route.channel, route.epoch));
        subc_client_rs::HandlerOutcome::Response(
            serde_json::to_vec(&json!({"content":[{"type":"text","text":"ok"}]})).unwrap(),
        )
    }
    async fn on_bind(&self, _: &subc_client_rs::RouteBindRequest) -> subc_client_rs::BindDecision {
        self.0.bound.fetch_add(1, Ordering::SeqCst);
        subc_client_rs::BindDecision::accept()
    }
    async fn on_route_gone(&self, _: &subc_client_rs::RouteHandle) {
        self.0.gone.fetch_add(1, Ordering::SeqCst);
    }
}

struct Executor(Arc<SubcToolPlane>);
#[async_trait]
impl RunExecutor for Executor {
    async fn execute(
        &self,
        job: RunJob,
        _: tokio::sync::mpsc::Sender<broca_core::OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        self.0
            .warm_bind(&job.req.session, Duration::from_secs(10))
            .await
            .map_err(|e| RunError::Config(e.to_string()))?;
        job.writer.record(WalRecord::RunStarted {
            written_at_ms: None,
            tags: None,
            session_class: None,
            retention: job.req.retention,
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
            run_id: job.req.run_id.clone(),
            session: job.req.session.clone(),
            ts_ms: None,
            config: Box::new(config()),
            input: job.req.input.clone().into(),
            origin: broca_wal::RunOrigin::Fresh,
            submission_id: job.req.submission_id.clone(),
        });
        job.writer.sync().await?;
        if let Some(ack) = job.on_admitted {
            let _ = ack.send(Ok(job.req.run_id));
        }
        job.writer.record(WalRecord::RunFinished {
            interrupt_reason: None,
            ts_ms: None,
            reason: RunFinishReason::Completed,
            usage: Default::default(),
            error: None,
            retries_used: 0,
        });
        job.writer.sync().await?;
        Ok(RunSummary {
            reason: RunFinishReason::Completed,
            paused: None,
            steps: 0,
            usage: Default::default(),
            retries_used: 0,
            final_step_finish_reason: None,
        })
    }
    async fn on_session_close(&self, session: &BindIdentity) {
        self.0.close_session_routes(session).await;
    }
    fn on_session_deleted(&self, session: &BindIdentity) {
        tokio::spawn(self.0.close_session_routes(session));
    }
}

fn config() -> RunConfig {
    RunConfig {
        model: broca_protocol::request::ModelHandle::new("fake", "fake"),
        generation: Default::default(),
        tool_choice: Default::default(),
        tools: vec![].into(),
        response_format: Default::default(),
        provider_options: Value::Null,
        cache_tiers: Value::Null,
        context_epoch: 0,
        turn_directives_v1: None,
        render_tags: false,
        computed_tags: None,
        renderer_version: 1,
        frozen_render_config: Value::Null,
        auth_selection: None,
        tools_were_omitted: false,
        budget: None,
        context_limit: None,
        work_class: None,
        variant: None,
        service_tier: None,
        stop_when: vec![],
        context_management: None,
    }
}

struct Fixture {
    rig: common::Rig,
    provider: tokio::task::JoinHandle<()>,
    observed: Arc<Observed>,
    plane: Arc<SubcToolPlane>,
    registry: Arc<LineageRegistry>,
    wal: Arc<FileWalStore>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.provider.abort();
    }
}
impl Fixture {
    async fn start(tag: &str, fail_goodbye: bool) -> Option<Self> {
        let rig = common::Rig::start_with_modules(tag, vec![]).await?;
        let connection = rig.connect().await;
        if fail_goodbye {
            connection.fail_next_goodbye_for_test();
        }
        let observed = Arc::new(Observed::default());
        let manifest = ModuleManifest::builder("aft", env!("CARGO_PKG_VERSION"))
            .provides(vec![ProviderRole::ToolProvider {
                tools: vec![Tool {
                    name: "echo".into(),
                    description: None,
                    execution_mode: ExecutionMode::Pure,
                    schema: json!({"type":"object"}),
                }],
                identity_scope: vec![IdentityScope::Project, IdentityScope::Session],
                concurrency: Concurrency::ModuleManaged,
                emits_push: false,
                sub_supervises: false,
            }])
            .build();
        let (_, serving) = subc_client_rs::serve_with_handle(
            &rig.connection_file,
            manifest,
            Provider(observed.clone()),
        )
        .await
        .unwrap();
        let provider = tokio::spawn(async move {
            let _ = serving.await;
        });
        let plane = Arc::new(SubcToolPlane::new(connection, "aft"));
        common::wait_for_aft(&plane, "echo").await;
        let wal = Arc::new(FileWalStore::new(rig.dir.join("wal")));
        let opener = DefaultOpener::new(
            Arc::new(FileLeaseStore::new(rig.dir.join("leases"))),
            wal.clone(),
            Arc::new(Executor(plane.clone())),
        );
        let registry = Arc::new(LineageRegistry::new_with_reader(
            Arc::new(opener),
            wal.clone(),
            None,
        ));
        let health = plane.clone();
        registry.set_tool_route_health(Arc::new(move |open| health.tool_routes_health(open)));
        Some(Self {
            rig,
            provider,
            observed,
            plane,
            registry,
            wal,
        })
    }
    fn bind(&self, name: &str) -> BindIdentity {
        BindIdentity::new(&self.rig.dir, "alfonso", name)
    }
    async fn run(&self, bind: &BindIdentity) {
        let request = serde_json::from_value(json!({
            "model": broca_protocol::request::ModelHandle::new("fake", "fake"),
            "input": [json!({"role":"user", "content":[{"type":"text", "text":"finish"}]})], "tools": []
        })).unwrap();
        self.registry
            .route_submission(
                bind,
                RunId("worker".into()),
                request,
                RunKind::FreshOrResume,
                false,
            )
            .await
            .unwrap();
    }
    async fn wait_closed(&self) {
        tokio::time::timeout(
            Duration::from_secs(36),
            broca_test_support::wait_until(
                "actor closes and provider receives Goodbye",
                || async {
                    if self.registry.slot_counts().open == 0
                        && self.observed.gone.load(Ordering::SeqCst) > 0
                    {
                        Ok(())
                    } else {
                        Err(self.registry.tool_routes_health().to_string())
                    }
                },
            ),
        )
        .await
        .expect("route must close within the production idle TTL plus teardown budget");
    }
}

#[tokio::test]
async fn worker_idle_releases_tool_routes() {
    let Some(f) = Fixture::start("worker-route-idle", false).await else {
        return;
    };
    let bind = f.bind("sidekick-reader");
    let (sink, stream) = tokio::sync::mpsc::channel(64);
    f.registry
        .route_subscribe(&bind, broca_module::fanout::FromCursor::Start, sink)
        .await
        .unwrap();
    f.run(&bind).await;
    broca_test_support::wait_until("worker run settles", || async {
        if f.registry.route_status(&bind).await.unwrap() == broca_module::actor::LineageStatus::Idle
        {
            Ok(())
        } else {
            Err("run still active".into())
        }
    })
    .await;
    assert_eq!(
        f.registry.tool_routes_health()["open_actor_reasons"]["live_subscriber"],
        1
    );
    // Core drops its session.subscribe stream when the worker completes.
    drop(stream);
    assert_eq!(f.observed.bound.load(Ordering::SeqCst), 1);
    assert_eq!(f.registry.tool_routes_health()["cached_routes"], 1);
    // This uses the production 30-second idle TTL, not a shorter test TTL.
    f.wait_closed().await;
    assert_eq!(f.observed.gone.load(Ordering::SeqCst), 1);
    let health = f.registry.tool_routes_health();
    assert_eq!(health["cached_routes"], 0);
    assert_eq!(health["routes_for_closed_sessions"], 0);
    assert!(!health.to_string().contains("sidekick-reader"));
    f.registry.stop_and_seal().await;
}

#[tokio::test]
async fn sweeper_deleted_session_releases_tool_routes() {
    let Some(f) = Fixture::start("sweeper-route-close", false).await else {
        return;
    };
    let bind = f.bind("retained-reader");
    let mut wal = f.wal.open(&bind).unwrap();
    wal.append(
        1,
        &WalRecord::RunStarted {
            written_at_ms: None,
            tags: None,
            session_class: None,
            retention: Some(1),
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
            run_id: RunId("expired".into()),
            session: bind.clone(),
            ts_ms: Some(0),
            config: Box::new(config()),
            input: vec![].into(),
            origin: broca_wal::RunOrigin::Fresh,
            submission_id: None,
        },
        true,
    )
    .unwrap();
    wal.append(
        1,
        &WalRecord::RunFinished {
            interrupt_reason: None,
            ts_ms: Some(0),
            reason: RunFinishReason::Completed,
            usage: Default::default(),
            error: None,
            retries_used: 0,
        },
        true,
    )
    .unwrap();
    drop(wal);
    let store = Arc::new(broca_store::SqliteStore::open(f.rig.dir.join("retention.db")).unwrap());
    let retention = Arc::new(
        broca_module::retention::RetentionService::new(
            f.wal.clone(),
            store.clone(),
            Arc::new(FileLeaseStore::new(f.rig.dir.join("leases"))),
            store,
            Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::Unknown, 1)),
        )
        .unwrap(),
    );
    f.registry.set_retention_service(retention.clone());
    // Exercise deletion with cached routes but no actor available to call on_session_close.
    f.plane
        .warm_bind(&bind, Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(
        f.registry.tool_routes_health()["routes_for_closed_sessions"],
        1
    );
    f.registry.boot_retention().await.unwrap();
    assert_eq!(
        f.registry
            .sweep_once(retention.clock.now_ms())
            .await
            .unwrap(),
        1
    );
    broca_test_support::wait_until("sweeper route Goodbye", || async {
        if f.observed.gone.load(Ordering::SeqCst) == 1 {
            Ok(())
        } else {
            Err(f.registry.tool_routes_health().to_string())
        }
    })
    .await;
    assert_eq!(
        f.registry.tool_routes_health()["routes_for_closed_sessions"],
        0
    );
}

#[tokio::test]
async fn failed_goodbye_is_retried_and_counted() {
    use broca_core::tool_plane::{BatchCtx, ToolPlane};
    let Some(f) = Fixture::start("goodbye-retry", true).await else {
        return;
    };
    let bind = f.bind("retry-reader");
    f.plane
        .warm_bind(&bind, Duration::from_secs(10))
        .await
        .unwrap();
    let old = f.plane.cached_route_channel(&bind).unwrap();
    f.plane.close_session_routes(&bind).await;
    assert_eq!(
        f.registry.tool_routes_health()["goodbye_failures_since_boot"],
        1
    );
    assert_eq!(f.registry.tool_routes_health()["pending_goodbyes"], 1);
    let ctx = BatchCtx {
        bind: bind.clone(),
        batch_id: broca_protocol::BatchId("fresh".into()),
        run_id: RunId("fresh".into()),
        scope: None,
        media_capture: None,
    };
    let call = broca_protocol::ToolCall {
        tool_call_id: broca_protocol::ToolCallId("fresh".into()),
        tool_name: "echo".into(),
        input: json!({}),
        thought_signature: None,
    };
    f.plane
        .batch_started(
            &ctx,
            std::slice::from_ref(&call),
            &broca_core::ToolManifest::from_tools(vec![]),
        )
        .await
        .unwrap();
    let fresh = f.plane.cached_route_channel(&bind).unwrap();
    assert_ne!(fresh, old, "a closing route must never carry a new call");
    assert!(matches!(
        f.plane.dispatch(&ctx, &call).await,
        broca_core::tool_plane::DispatchResult::Completed(_)
    ));
    assert_eq!(f.observed.calls.lock().unwrap().len(), 1);
    broca_test_support::wait_until("failed Goodbye is retried", || async {
        if f.observed.gone.load(Ordering::SeqCst) == 1 {
            Ok(())
        } else {
            Err(f.registry.tool_routes_health().to_string())
        }
    })
    .await;
    assert_eq!(f.registry.tool_routes_health()["pending_goodbyes"], 0);
    assert_eq!(
        f.registry.tool_routes_health()["goodbye_failures_since_boot"],
        1
    );
    f.plane.close_session_routes(&bind).await;
    assert_eq!(
        f.registry.tool_routes_health()["routes_for_closed_sessions"],
        0
    );
}
