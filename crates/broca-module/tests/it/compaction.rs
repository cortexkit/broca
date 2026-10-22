//! Exercise the coordinator with the real compaction run loop and durable store/WAL reads.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;

use async_trait::async_trait;
use broca_core::compaction_provider::setup::{SetupAnswer, SetupRequest};
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

/// A compaction provider that answers Setup with a one-message head.
struct Compactor;

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

/// A compaction provider whose Setup waits until the test releases it, then answers with a
/// head and a tag checkpoint of 500 cut at the newest message, as a provider does for a
/// session it moved from standalone Magic Context.
struct HeldCompactor {
    arrived: Semaphore,
    release: Semaphore,
}

#[async_trait]
impl CompactionPlane for HeldCompactor {
    async fn setup(
        &self,
        _: &str,
        _: &BindIdentity,
        request: SetupRequest,
    ) -> Result<SetupAnswer, CompactionUnavailable> {
        self.arrived.add_permits(1);
        self.release.acquire().await.unwrap().forget();
        let cut = request.newest.as_ref().map(|newest| newest.mid.clone());
        Ok(serde_json::from_value(
            json!({"answer":"ready","request_id":request.request_id,"initial":{
                "compaction_id":"c1","version":1,"range":{"from":0,"to":0},
                "replacement":[text(Role::User, "head")]
            },"checkpoint":{"tag_checkpoint":500,"cut":cut}}),
        )
        .unwrap())
    }
}

struct Exec {
    wal: Arc<FileWalStore>,
    store: Arc<broca_store::SqliteStore>,
    compactor: Arc<dyn CompactionPlane>,
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
    wal: Arc<FileWalStore>,
    registry: LineageRegistry,
    key: BindIdentity,
    model: Arc<Model>,
    prompts: Mutex<mpsc::UnboundedReceiver<Vec<Message>>>,
}

fn manifest() -> Arc<PlanManifest> {
    manifest_rendering(false)
}

/// The plan's manifest, rendering tags when `render_tags`.
fn manifest_rendering(render_tags: bool) -> Arc<PlanManifest> {
    Arc::new(PlanManifest {
        plan: OpaqueJson::from_json_text(
            json!({"compaction_item":{
            "provider":"provider","preset":"head","params":{}},"composition":{},
            "render_tags":render_tags})
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
    fn new(tool_steps: u64) -> Self {
        Self::with_compactor(tool_steps, Arc::new(Compactor))
    }

    fn with_compactor(tool_steps: u64, compactor: Arc<dyn CompactionPlane>) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        // broca's owned scratch root: on Windows it is owner-only, which the store now requires.
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-module-compaction-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let wal = Arc::new(FileWalStore::new(dir.join("wal")));
        let store = Arc::new(broca_store::SqliteStore::open(dir.join("store.db")).unwrap());
        let (prompt_tx, prompt_rx) = mpsc::unbounded_channel();
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
        let registry = LineageRegistry::new_with_reader(opener, wal.clone(), Some(store));
        let key = BindIdentity::new(dir.clone(), "caller", "session");
        Self {
            dir,
            wal,
            registry,
            key,
            model,
            prompts: Mutex::new(prompt_rx),
        }
    }

    async fn send(&self, plan: bool) {
        self.send_manifest(plan.then(manifest)).await
    }

    async fn send_manifest(&self, manifest: Option<Arc<PlanManifest>>) {
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
                manifest.map(|manifest| SendPlan {
                    identity: "plan".into(),
                    manifest: Some(manifest),
                    model_check: None,
                }),
            )
            .await
            .unwrap();
        assert!(matches!(result, SendOutcome::Active { .. }), "{result:?}");
    }

    async fn prompt(&self) -> Vec<Message> {
        broca_test_support::event_timeout("prompt", self.prompts.lock().await.recv(), || {
            format!("session={:?}", self.key)
        })
        .await
        .expect("model prompt")
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

/// Setup's head is the first message the model sees, and the advance notice announcing the
/// session's transcript is durable in the WAL before the model call, so the bus publisher,
/// which projects notices from the durable WAL, can never publish one the WAL does not hold.
#[tokio::test]
async fn setup_head_renders_and_the_notice_is_durable_before_the_model_call() {
    let rig = Rig::new(0);
    assert!(rig.registry.compaction_supported());
    rig.send(true).await;
    let prompt = rig.prompt().await;
    assert_eq!(prompt[0].content, text(Role::User, "head").content);
    // The model is holding its answer, so the WAL holds exactly what preceded the call.
    let records: Vec<_> = broca_core::WalStore::read_records(rig.wal.as_ref(), &rig.key)
        .unwrap()
        .into_iter()
        .map(|frame| frame.record)
        .collect();
    let notice = records
        .iter()
        .position(|record| matches!(record, broca_wal::WalRecord::CompactionNotice { .. }))
        .expect("a notice before the first model call");
    let setup = records
        .iter()
        .position(|record| matches!(record, broca_wal::WalRecord::CompactionMessage { .. }))
        .expect("Setup recorded");
    assert!(setup < notice, "Setup is recorded before the first notice");
    let broca_wal::WalRecord::CompactionNotice { seq, headers } = &records[notice] else {
        unreachable!()
    };
    assert_eq!(*seq, 1);
    assert_eq!(headers["seq"], "1");
    assert_eq!(headers["through_ordinal"], "0");
    assert!(
        !headers.contains_key("applied_view_version"),
        "no view applies before the first one arrives"
    );
    assert!(!headers.contains_key("descends_from"));
    rig.close().await;
}

/// While a run that renders tags waits for its first Setup answer, which may anchor its numbers
/// at a checkpoint, `session.read` and `session.resolve` refuse `transient` and write nothing.
/// Once the answer is durable they answer, with the checkpoint's numbers: no read ever shows a
/// number a later read changes.
#[tokio::test]
async fn reads_wait_out_the_setup_answer_and_never_show_a_number_that_changes() {
    let held = Arc::new(HeldCompactor {
        arrived: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let rig = Rig::with_compactor(0, held.clone());
    rig.send_manifest(Some(manifest_rendering(true))).await;
    broca_test_support::event_timeout("setup call", held.arrived.acquire(), || {
        format!("session={:?}", rig.key)
    })
    .await
    .expect("Setup is called")
    .unwrap()
    .forget();
    let read = || {
        rig.registry
            .read(SessionReadRequest::new(rig.key.clone(), Some(0), Some(100)))
    };
    let before = read().await;
    assert!(
        matches!(before, Err(broca_module::ServeError::TagsAwaitSetup)),
        "a read inside the Setup window is refused: {before:?}"
    );
    let resolve = rig
        .registry
        .resolve(&rig.key, &broca_wire::ResolveParams::new("§1§"))
        .await;
    assert!(
        matches!(resolve, Err(broca_module::ServeError::TagsAwaitSetup)),
        "a resolve inside the Setup window is refused: {resolve:?}"
    );
    held.release.add_permits(1);
    rig.prompt().await;
    let after = read().await.expect("the Setup answer is durable");
    let shown: Vec<_> = after
        .messages
        .iter()
        .flat_map(|entry| entry.tags.iter().filter_map(|row| row.tag.clone()))
        .collect();
    assert_eq!(
        shown,
        vec!["§501§".to_owned()],
        "the prompt shows the checkpoint's number"
    );
    rig.close().await;
}

#[tokio::test]
async fn model_read_without_compaction_rejects_after_mid() {
    let rig = Rig::new(0);
    rig.send(false).await;
    rig.prompt().await;
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
async fn store_backed_model_page_matches_sent_guidance_tail_under_setups_head() {
    let rig = Rig::new(8);
    rig.send(true).await;
    let mut sent = Vec::new();
    for _ in 0..9 {
        sent = rig.prompt().await;
    }
    let req = SessionReadRequest::new(rig.key.clone(), Some(0), Some(100));
    let raw = rig.registry.read(req.clone()).await.unwrap();
    let page = rig.registry.read_model(req.clone()).await.unwrap();
    assert_eq!(page.compaction_id.as_deref(), Some("c1"));
    assert_eq!(page.version, Some(1));
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
        "kind":"replacement","compaction_id":"c1","version":1,"from_ordinal":0,"to_ordinal":0})
    );
    assert_eq!(
        serde_json::to_value(&page.messages[1].source).unwrap(),
        json!({
        "kind":"message","ordinal":0,"mid":"m0"})
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
        serde_json::to_vec(&sent[16]).unwrap(),
        "a range starting mid-episode keeps replaced counters"
    );
    rig.close().await;
}
