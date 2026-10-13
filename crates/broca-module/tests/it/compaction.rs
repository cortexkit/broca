//! Exercise the coordinator with the real compaction run loop and durable store/WAL reads.

use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;

use async_trait::async_trait;
use broca_core::compaction_provider::{
    answer::StepAnswer,
    setup::{SetupAnswer, SetupRequest},
    status::StepStatus,
};
use broca_core::{
    AllowAll, CompactionPlane, CompactionUnavailable, FileLeaseStore, FileWalStore, OutputEvent,
    Provider, RunError, RunSummary, StartMode, StubReply, StubToolPlane,
};
use broca_module::actor::{SendKey, SendOutcome, SendPlan};
use broca_module::{
    DefaultOpener, LineageRegistry, RunExecutor, RunJob, RunKind, ServeHandler, SessionReadRequest,
};
use broca_protocol::{
    AssistantMessage, BindIdentity, CallOptions, ContentBlock, FinishReason, Message, MessageId,
    NormalizedEvent, ProviderControl, Role, RunId, StepId, ToolCall, ToolCallId,
};
use broca_wal::{OpaqueJson, PlanManifest};
use serde_json::json;
use tokio::sync::{mpsc, Mutex, Semaphore};

fn text(role: Role, text: &str) -> Message {
    Message::new(role, vec![ContentBlock::Text { text: text.into() }])
}

struct Compactor {
    waiting: bool,
    applied: AtomicBool,
    statuses: mpsc::UnboundedSender<StepStatus>,
}

#[async_trait]
impl CompactionPlane for Compactor {
    async fn setup(
        &self,
        _: &str,
        _: &BindIdentity,
        request: SetupRequest,
    ) -> Result<SetupAnswer, CompactionUnavailable> {
        Ok(serde_json::from_value(
            json!({"answer":"ready","request_id":request.request_id,"initial":{
                "compaction_id":"c1","version":1,"range":{"from":0,"to":0},
                "replacement":[text(Role::User, "head")]
            }}),
        )
        .unwrap())
    }

    async fn step(
        &self,
        _: &str,
        _: &BindIdentity,
        status: StepStatus,
    ) -> Result<StepAnswer, CompactionUnavailable> {
        let encoded = serde_json::to_value(&status).unwrap();
        let answer = if self.waiting && !self.applied.swap(true, Ordering::SeqCst) {
            json!({"answer":"wait","request_id":status.request_id,"reason":"working","bound_ms":20000})
        } else if !self.waiting
            && encoded["newest"]["ordinal"].as_u64().unwrap_or(0) >= 13
            && !self.applied.swap(true, Ordering::SeqCst)
        {
            json!({"answer":"compaction_message","request_id":status.request_id,
                "compaction":{"compaction_id":"c1","version":2,"range":{"from":0,"to":13},
                    "replacement":[text(Role::User, "summary")]}})
        } else {
            json!({"answer":"noop","request_id":status.request_id})
        };
        self.statuses.send(status).unwrap();
        Ok(serde_json::from_value(answer).unwrap())
    }
}

struct Model {
    tool_steps: u64,
    prompts: mpsc::UnboundedSender<Vec<Message>>,
    finish: Semaphore,
}

#[async_trait]
impl Provider for Model {
    async fn step(&self, step: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        self.prompts.send(opts.prompt.clone()).unwrap();
        let tool = step.0 <= self.tool_steps;
        if !tool {
            self.finish.acquire().await.unwrap().forget();
        }
        let content = if tool {
            vec![ContentBlock::ToolCall(ToolCall {
                tool_call_id: ToolCallId(format!("call-{}", step.0)),
                tool_name: "lookup".into(),
                input: json!({"q":"same"}),
                thought_signature: None,
            })]
        } else {
            vec![ContentBlock::Text {
                text: "done".into(),
            }]
        };
        let finish_reason = if tool {
            FinishReason::ToolCalls
        } else {
            FinishReason::Stop
        };
        let (tx, rx) = mpsc::channel(4);
        for control in [
            ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id: MessageId(format!("assistant-{}", step.0)),
                    content,
                },
            },
            ProviderControl::StepFinished {
                step_id: step,
                usage: Default::default(),
                finish_reason,
                service_tier_used: None,
                speed_used: None,
            },
            ProviderControl::Finished {
                usage: Default::default(),
                finish_reason,
                retries_used: 0,
            },
        ] {
            tx.send(NormalizedEvent::Control(control)).await.unwrap();
        }
        rx
    }
}

struct Exec {
    wal: Arc<FileWalStore>,
    store: Arc<broca_store::SqliteStore>,
    compactor: Arc<Compactor>,
    model: Arc<Model>,
}

#[async_trait]
impl RunExecutor for Exec {
    fn compaction_plane(&self) -> Option<Arc<dyn CompactionPlane>> {
        Some(self.compactor.clone())
    }

    async fn execute(
        &self,
        job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        let mode = match job.kind {
            RunKind::FreshOrResume => StartMode::FreshOrResume,
            RunKind::AppendEpisode => StartMode::AppendEpisode,
            RunKind::ResumeAfterAuth => StartMode::ResumeAfterAuth,
        };
        let tools =
            StubToolPlane::new().with_tool("lookup", |_| StubReply::ExecError("failed".into()));
        let store = job.materialized_store(self.store.clone());
        broca_core::run_loop_on_writer_admitted_with_prior_snapshot(
            job.req,
            &job.writer,
            mode,
            self.model.as_ref(),
            &tools,
            &AllowAll,
            self.wal.as_ref(),
            store,
            job.control,
            &out,
            job.on_admitted,
            Some(&job.prior_snapshot),
        )
        .await
    }
}

struct Rig {
    dir: std::path::PathBuf,
    registry: LineageRegistry,
    key: BindIdentity,
    model: Arc<Model>,
    statuses: Mutex<mpsc::UnboundedReceiver<StepStatus>>,
    prompts: Mutex<mpsc::UnboundedReceiver<Vec<Message>>>,
}

fn manifest() -> Arc<PlanManifest> {
    Arc::new(PlanManifest {
        plan: OpaqueJson::from_json_text(
            json!({"compaction_item":{
            "provider":"provider","preset":"head","params":{}},"composition":{}})
            .to_string(),
        )
        .unwrap(),
        plan_identity: "plan".into(),
        composition_digest: "digest".into(),
        tools: Arc::new(vec![]),
        system_text: String::new(),
        system_digest: String::new(),
        join_version: 1,
        role_versions: Default::default(),
        fetches: vec![],
        declarations: vec![],
    })
}

impl Rig {
    fn new(waiting: bool, tool_steps: u64) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "broca-module-compaction-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let wal = Arc::new(FileWalStore::new(dir.join("wal")));
        let store = Arc::new(broca_store::SqliteStore::open(dir.join("store.db")).unwrap());
        let (status_tx, status_rx) = mpsc::unbounded_channel();
        let (prompt_tx, prompt_rx) = mpsc::unbounded_channel();
        let compactor = Arc::new(Compactor {
            waiting,
            applied: AtomicBool::new(false),
            statuses: status_tx,
        });
        let model = Arc::new(Model {
            tool_steps,
            prompts: prompt_tx,
            finish: Semaphore::new(0),
        });
        let exec = Arc::new(Exec {
            wal: wal.clone(),
            store: store.clone(),
            compactor,
            model: model.clone(),
        });
        let opener = Arc::new(
            DefaultOpener::new(
                Arc::new(FileLeaseStore::new(dir.join("leases"))),
                wal.clone(),
                exec,
            )
            .with_idle_ttl(Duration::from_secs(3600))
            .with_run_index(store.clone())
            .with_materialized_store(store.clone()),
        );
        let registry = LineageRegistry::new_with_reader(opener, wal, Some(store));
        let key = BindIdentity::new(dir.clone(), "caller", "session");
        Self {
            dir,
            registry,
            key,
            model,
            statuses: Mutex::new(status_rx),
            prompts: Mutex::new(prompt_rx),
        }
    }

    async fn send(&self, plan: bool) {
        let request = serde_json::from_value(json!({
            "model":{"provider_module_id":"stub","model_id":"model"},
            "input":[text(Role::User, "user")], "context_limit":100000
        }))
        .unwrap();
        let result = self
            .registry
            .route_submission_with_plan(
                &self.key,
                RunId("run".into()),
                request,
                RunKind::FreshOrResume,
                SendKey::RunId,
                false,
                broca_protocol::Delivery::Queue,
                plan.then(|| SendPlan {
                    identity: "plan".into(),
                    manifest: Some(manifest()),
                }),
            )
            .await
            .unwrap();
        assert!(matches!(result, SendOutcome::Active { .. }), "{result:?}");
    }

    async fn status(&self) -> StepStatus {
        tokio::time::timeout(Duration::from_secs(5), self.statuses.lock().await.recv())
            .await
            .expect("compaction status")
            .unwrap()
    }

    async fn prompt(&self) -> Vec<Message> {
        tokio::time::timeout(Duration::from_secs(5), self.prompts.lock().await.recv())
            .await
            .expect("model prompt")
            .unwrap()
    }

    async fn ready(
        &self,
        caller: &str,
        request_id: &str,
    ) -> Result<broca_wire::ReadyReply, &'static str> {
        self.registry
            .compaction_ready(
                &self.key,
                caller,
                broca_wire::CompactionReady::new("session", request_id),
            )
            .await
            .unwrap()
    }

    async fn close(&self) {
        self.model.finish.add_permits(1);
        assert!(self.registry.stop_and_seal().await);
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[tokio::test]
async fn ready_only_from_frozen_provider_and_only_newest_request_retries_wait() {
    let rig = Rig::new(true, 0);
    assert!(rig.registry.compaction_supported());
    rig.send(true).await;
    let first = rig.status().await;
    assert_eq!(
        rig.ready("other", &first.request_id).await.unwrap_err(),
        "not_session_compaction_provider"
    );
    assert_eq!(
        serde_json::to_string(&rig.ready("provider", "older").await.unwrap()).unwrap(),
        "{}"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(80), rig.statuses.lock().await.recv())
            .await
            .is_err(),
        "wrong caller or older request must not cause a fresh status"
    );
    assert!(
        rig.prompts.lock().await.try_recv().is_err(),
        "wait sent nothing to model"
    );
    assert_eq!(
        serde_json::to_string(&rig.ready("provider", &first.request_id).await.unwrap()).unwrap(),
        "{}"
    );
    let fresh = rig.status().await;
    assert_ne!(first.request_id, fresh.request_id);
    assert_eq!(fresh.session, first.session);
    let prompt = rig.prompt().await;
    assert_eq!(prompt[0].content, text(Role::User, "head").content);
    assert_eq!(
        serde_json::to_string(&rig.ready("provider", &first.request_id).await.unwrap()).unwrap(),
        "{}"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(80), rig.statuses.lock().await.recv())
            .await
            .is_err()
    );
    rig.close().await;
}

#[tokio::test]
async fn no_compaction_provider_refuses_ready_and_model_read_rejects_after_mid() {
    let rig = Rig::new(false, 0);
    rig.send(false).await;
    rig.prompt().await;
    assert_eq!(
        rig.ready("provider", "anything").await.unwrap_err(),
        "not_session_compaction_provider"
    );
    let mut req = SessionReadRequest::new(rig.key.clone(), Some(0), Some(100));
    req.after_mid = Some("m0".into());
    assert!(matches!(rig.registry.read_model(req).await,
        Err(broca_module::ServeError::InvalidParams { field, .. }) if field == "view"));
    let req = SessionReadRequest::new(rig.key.clone(), Some(0), Some(100));
    let raw = rig.registry.read(req.clone()).await.unwrap();
    let model = rig.registry.read_model(req).await.unwrap();
    assert_eq!(model.compaction_id, None);
    assert_eq!(model.version, None);
    assert_eq!(
        serde_json::to_vec(&raw.messages[0].message).unwrap(),
        serde_json::to_vec(&model.messages[0].message).unwrap()
    );
    rig.close().await;
}

#[tokio::test]
async fn store_backed_model_page_matches_sent_guidance_tail_after_compaction() {
    let rig = Rig::new(false, 8);
    rig.send(true).await;
    let mut sent = Vec::new();
    for _ in 0..9 {
        sent = rig.prompt().await;
    }
    let req = SessionReadRequest::new(rig.key.clone(), Some(0), Some(100));
    let raw = rig.registry.read(req.clone()).await.unwrap();
    let page = rig.registry.read_model(req.clone()).await.unwrap();
    assert_eq!(page.compaction_id.as_deref(), Some("c1"));
    assert_eq!(page.version, Some(2));
    let messages: Vec<_> = page
        .messages
        .iter()
        .map(|entry| entry.message.clone())
        .collect();
    assert_eq!(
        serde_json::to_vec(&messages).unwrap(),
        serde_json::to_vec(&sent).unwrap(),
        "model read must match the native values actually sent, not undecorated store rows"
    );
    assert_eq!(
        serde_json::to_value(&page.messages[0].source).unwrap(),
        json!({
        "kind":"replacement","compaction_id":"c1","version":2,"from_ordinal":0,"to_ordinal":13})
    );
    assert_eq!(
        serde_json::to_value(&page.messages[1].source).unwrap(),
        json!({
        "kind":"message","ordinal":13,"mid":"m13"})
    );
    assert_ne!(
        page.messages.last().unwrap().message.content,
        raw.messages.last().unwrap().message.content,
        "the tail must actually contain model-only repetition guidance"
    );
    assert_eq!(page.messages[0].message.ordinal, None);
    let mut tail_req = req;
    tail_req.from_ordinal = Some(15);
    let tail = rig.registry.read_model(tail_req).await.unwrap();
    assert_eq!(
        serde_json::to_vec(&tail.messages[0].message).unwrap(),
        serde_json::to_vec(&sent[3]).unwrap(),
        "a range starting mid-episode keeps replaced counters"
    );
    rig.close().await;
}
