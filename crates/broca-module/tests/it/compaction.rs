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
    Provider, RunError, RunSummary, StartMode, StubReply, StubToolPlane, WalStore,
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
        _scope: Option<&broca_protocol::SessionScope>,
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
                raw_finish_reason: None,
                step_id: step,
                usage: Default::default(),
                finish_reason,
                service_tier_used: None,
                speed_used: None,
                served_model: None,
                usage_iterations: None,
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
        _scope: Option<&broca_protocol::SessionScope>,
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
    completed: Arc<Semaphore>,
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
        let mut control = job.control;
        control.admitted_context_limit = job.req.context_limit;
        let result = broca_core::run_loop_on_writer_admitted_with_prior_snapshot(
            job.req,
            &job.writer,
            mode,
            self.model.as_ref(),
            &tools,
            &AllowAll,
            self.wal.as_ref(),
            store,
            control,
            &out,
            job.on_admitted,
            Some(&job.prior_snapshot),
        )
        .await;
        self.completed.add_permits(1);
        result
    }
}

struct Rig {
    dir: std::path::PathBuf,
    wal: Arc<FileWalStore>,
    store: Arc<broca_store::SqliteStore>,
    registry: LineageRegistry,
    opener: Arc<dyn broca_module::registry::LineageOpener>,
    key: BindIdentity,
    model: Arc<Model>,
    prompts: Mutex<mpsc::UnboundedReceiver<Vec<Message>>>,
    completed: Arc<Semaphore>,
}

fn manifest() -> Arc<PlanManifest> {
    manifest_rendering(false)
}

/// The plan's manifest, rendering tags when `render_tags`.
fn manifest_rendering(render_tags: bool) -> Arc<PlanManifest> {
    Arc::new(PlanManifest {
        rich_ui: None,
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
        let completed = Arc::new(Semaphore::new(0));
        let exec = Arc::new(Exec {
            wal: wal.clone(),
            store: store.clone(),
            compactor,
            model: model.clone(),
            completed: completed.clone(),
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
        let registry =
            LineageRegistry::new_with_reader(opener.clone(), wal.clone(), Some(store.clone()));
        let key = BindIdentity::new(dir.clone(), "caller", "session");
        Self {
            dir,
            wal,
            store,
            registry,
            opener,
            key,
            model,
            prompts: Mutex::new(prompt_rx),
            completed,
        }
    }

    async fn send(&self, plan: bool) {
        self.send_manifest(plan.then(manifest)).await
    }

    async fn send_manifest(&self, manifest: Option<Arc<PlanManifest>>) {
        let result = self.submit("run", manifest).await;
        assert!(matches!(result, SendOutcome::Active { .. }), "{result:?}");
    }

    /// Send under `run_id`, carrying a plan with `manifest` when one is given, and return the
    /// coordinator's answer.
    async fn submit(&self, run_id: &str, manifest: Option<Arc<PlanManifest>>) -> SendOutcome {
        let request = serde_json::from_value(json!({
            "model":{"provider_module_id":"stub","model_id":"model"},
            "input":[text(Role::User, "user")], "context_limit":100000
        }))
        .unwrap();
        self.registry
            .route_submission_with_plan(
                &self.key,
                RunId(run_id.into()),
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
            .unwrap()
    }

    /// Every WAL record the session holds.
    fn records(&self) -> Vec<broca_wal::FramedRecord> {
        broca_core::WalStore::read_records(self.wal.as_ref(), &self.key).unwrap()
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

#[tokio::test]
async fn legacy_plan_without_an_applied_view_stays_raw_without_provider_calls_or_notices() {
    let rig = Rig::new(0);
    assert!(rig.registry.compaction_supported());
    rig.send(true).await;
    let prompt = rig.prompt().await;
    assert_eq!(prompt[0].content, text(Role::User, "user").content);
    let records = rig.records();
    assert!(!records.iter().any(|frame| matches!(
        frame.record,
        broca_wal::WalRecord::CompactionNotice { .. }
            | broca_wal::WalRecord::CompactionMessage { .. }
    )));
    rig.close().await;
}

/// A session enrolls in compaction only on its first episode, when it has no stored messages
/// yet: a session whose first episode ran without a plan cannot take one naming a compaction
/// provider later. Its send is refused `plan_drift` (no frozen plan, the plan it sent) and writes
/// nothing, so a long session can never start compacting over the whole history it already holds.
#[tokio::test]
async fn a_session_first_run_without_a_plan_cannot_enroll_in_compaction_later() {
    let rig = Rig::new(0);
    rig.send(false).await;
    // The planless run holds its model call, so its WAL is quiet while the plan is refused.
    rig.prompt().await;
    let before = rig.records();
    let refused = rig.submit("later", Some(manifest())).await;
    let SendOutcome::PlanRefused(refusal) = refused else {
        panic!("a later compaction plan must be refused: {refused:?}");
    };
    assert_eq!(refusal.code(), "plan_drift");
    assert_eq!(refusal.detail(), json!({"sent": "plan"}));
    assert_eq!(rig.records(), before, "the refused send wrote nothing");
    rig.close().await;
}

/// An imported session froze no plan on its import episode, so a plan naming a compaction
/// provider is refused `plan_drift` on its first send after the import, and nothing is written.
#[tokio::test]
async fn an_imported_session_cannot_enroll_in_compaction() {
    let rig = Rig::new(0);
    let imported = rig
        .registry
        .route_import(
            &rig.key,
            broca_core::ImportRequest {
                project_id: None,
                principal: None,
                run_id: RunId("import".into()),
                session: rig.key.clone(),
                messages: vec![
                    text(Role::User, "imported question"),
                    text(Role::Assistant, "imported answer"),
                ],
                origin: broca_core::ImportOrigin {
                    source_ref: "test:compaction-enrollment".into(),
                },
            },
        )
        .await
        .unwrap();
    assert!(
        matches!(imported, broca_module::actor::ImportOutcome::Imported(_)),
        "{imported:?}"
    );
    let before = rig.records();
    assert!(!before.is_empty());
    let refused = rig.submit("after-import", Some(manifest())).await;
    let SendOutcome::PlanRefused(refusal) = refused else {
        panic!("a compaction plan after an import must be refused: {refused:?}");
    };
    assert_eq!(refusal.code(), "plan_drift");
    assert_eq!(refusal.detail(), json!({"sent": "plan"}));
    assert_eq!(rig.records(), before, "the refused send wrote nothing");
    rig.close().await;
}

/// A frozen legacy session never calls the retired v1 Setup, so reads never wait for its
/// numbering checkpoint; public reads expose immutable history with no renderer tag rows.
#[tokio::test]
async fn legacy_reads_never_wait_for_a_retired_provider_numbering_checkpoint() {
    let held = Arc::new(HeldCompactor {
        arrived: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let rig = Rig::with_compactor(0, held.clone());
    rig.send_manifest(Some(manifest_rendering(true))).await;
    let _ = rig.prompt().await;
    assert_eq!(
        held.arrived.available_permits(),
        0,
        "the retired Setup must not be called"
    );
    let request = SessionReadRequest::new(rig.key.clone(), Some(0), Some(100));
    let before = rig.registry.read(request.clone()).await.unwrap();
    let after = rig.registry.read(request).await.unwrap();
    assert_eq!(before.messages[0].ordinal, 1);
    assert!(serde_json::to_value(&before).unwrap()["messages"]
        .as_array()
        .unwrap()
        .iter()
        .all(|entry| entry.get("tags").is_none()));
    assert_eq!(after.messages[0].message, before.messages[0].message);
    assert_eq!(after.messages[0].mid, before.messages[0].mid);
    assert_eq!(after.messages[0].part_keys, before.messages[0].part_keys);
    assert_eq!(after.history_version, before.history_version);
    rig.close().await;
}

#[tokio::test]
async fn legacy_reads_do_not_wait_after_projection_or_in_a_later_run() {
    use broca_core::MaterializedStore;

    let rig = Rig::new(0);
    rig.send_manifest(Some(manifest_rendering(true))).await;
    rig.prompt().await;
    rig.model.finish.add_permits(1);
    broca_test_support::event_timeout("first run completion", rig.completed.acquire(), || {
        format!("session={:?}", rig.key)
    })
    .await
    .expect("first run completes")
    .unwrap()
    .forget();

    let started_seq = rig
        .records()
        .iter()
        .find_map(|frame| {
            matches!(frame.record, broca_wal::WalRecord::RunStarted { .. }).then_some(frame.seq)
        })
        .expect("run input is durable");
    let snapshot = rig
        .store
        .read_page_with_watermark(&rig.key, Some(0), 1, broca_core::ReadView::Raw)
        .unwrap();
    assert!(snapshot.watermark_seq >= started_seq);

    let outcome = rig.submit("run2", None).await;
    assert!(
        matches!(
            outcome,
            SendOutcome::Active { .. } | SendOutcome::Pending { .. }
        ),
        "{outcome:?}"
    );
    rig.prompt().await;
    let snapshot = rig
        .store
        .read_page_with_watermark(&rig.key, Some(0), 1, broca_core::ReadView::Raw)
        .unwrap();
    let tail: Vec<_> = rig
        .records()
        .into_iter()
        .filter(|frame| frame.seq > snapshot.watermark_seq)
        .collect();
    assert!(tail.iter().any(|frame| matches!(
        &frame.record,
        broca_wal::WalRecord::RunStarted { run_id, .. } if run_id.0 == "run2"
    )));
    assert!(!tail.iter().any(|frame| matches!(
        frame.record,
        broca_wal::WalRecord::CompactionMessage {
            kind: broca_wal::CompactionMessageKind::Setup,
            ..
        }
    )));
    let read = rig
        .registry
        .read(SessionReadRequest::new(rig.key.clone(), Some(0), Some(1)))
        .await;
    assert!(
        read.is_ok(),
        "a frozen legacy session has no pending provider checkpoint: {read:?}"
    );
    let read = read.unwrap();
    assert_eq!(read.messages.len(), 1);
    assert_eq!(read.messages[0].part_keys, Some(vec!["m0#0".into()]));
    assert_eq!(
        read.messages[0].message.part_tags().unwrap().parts,
        vec![Some(1)]
    );
    let closed = LineageRegistry::new_with_reader(
        rig.opener.clone(),
        rig.wal.clone(),
        Some(rig.store.clone()),
    );
    let read = closed
        .read(SessionReadRequest::new(rig.key.clone(), Some(0), Some(1)))
        .await;
    assert!(
        read.is_ok(),
        "the closed-actor read must use the same frozen numbering: {read:?}"
    );
    assert_eq!(closed.slot_counts().open, 0, "reads must not open an actor");
    rig.close().await;
}

fn seed_mode3(rig: &Rig) {
    let mut start: broca_wal::WalRecord = serde_json::from_str(
        include_str!("../../../broca-wal/tests/fixtures/compaction_v2_records.jsonl")
            .lines()
            .last()
            .unwrap(),
    )
    .unwrap();
    if let broca_wal::WalRecord::RunStarted {
        session,
        run_id,
        config,
        manifest: plan,
        input,
        submission_id,
        ..
    } = &mut start
    {
        *session = rig.key.clone();
        *run_id = RunId("seed".into());
        config.model = broca_protocol::ModelHandle::new("stub", "model");
        config.global_system = None;
        config.context_limit = Some(100_000);
        *plan = Some(Box::new(manifest().as_ref().clone()));
        *input = Arc::new(vec![text(Role::User, "stored question")]);
        *submission_id = None;
    }
    let mut wal = rig.wal.open(&rig.key).unwrap();
    wal.append(1, &start, false).unwrap();
    let finish = serde_json::from_value(
        json!({"type":"run_finished","reason":"completed","usage":{},"retries_used":0}),
    )
    .unwrap();
    wal.append(1, &finish, true).unwrap();
}

#[tokio::test]
async fn compaction_v2_bound_ops_check_provider_and_store_before_accepted_without_applying_mid_run()
{
    use broca_core::compaction_v2::protocol::{
        read::{Detail, ReadPage, ReadRequest, Select},
        view::View,
    };
    let rig = Rig::new(0);
    seed_mode3(&rig);
    let raw = |value: serde_json::Value| serde_json::value::to_raw_value(&value).unwrap();
    let before = rig.wal.read_records(&rig.key).unwrap().len();
    for method in [
        "bind",
        "session.entries",
        "session.apply_view",
        "session.applied_view",
    ] {
        let refused = rig
            .registry
            .route_compaction_v2(
                &rig.key,
                Some("another-provider".into()),
                method.into(),
                raw(json!({})),
            )
            .await
            .unwrap_err();
        assert_eq!(refused.code, "compaction_not_permitted");
    }
    assert_eq!(rig.wal.read_records(&rig.key).unwrap().len(), before);
    let entries = ReadRequest::new(
        Select::After {
            after_ordinal: None,
            limit: Some(1),
        },
        Detail::Full,
    );
    let page = rig
        .registry
        .route_compaction_v2(
            &rig.key,
            Some("provider".into()),
            "session.entries".into(),
            serde_json::value::to_raw_value(&entries).unwrap(),
        )
        .await
        .unwrap();
    let page: ReadPage = serde_json::from_str(page.get()).unwrap();
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].ordinal, 1);
    assert!(page.entries[0].mid.starts_with('m') && page.entries[0].mid.contains('.'));
    let view = View::seal(1, vec![]);
    let accepted = rig
        .registry
        .route_compaction_v2(
            &rig.key,
            Some("provider".into()),
            "session.apply_view".into(),
            raw(json!({"view":view})),
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(accepted.get()).unwrap(),
        json!({"answer":"accepted"})
    );
    assert!(rig
        .wal
        .read_records(&rig.key)
        .unwrap()
        .iter()
        .any(|frame| matches!(frame.record, broca_wal::WalRecord::CompactionViewV2(_))));
    let applied = rig
        .registry
        .route_compaction_v2(
            &rig.key,
            Some("provider".into()),
            "session.applied_view".into(),
            raw(json!({})),
        )
        .await
        .unwrap();
    assert_eq!(applied.get(), "{}");
    let result = rig.submit("next", Some(manifest())).await;
    assert!(matches!(result, SendOutcome::Active { .. }), "{result:?}");
    let _ = rig.prompt().await;
    let applied = rig
        .registry
        .route_compaction_v2(
            &rig.key,
            Some("provider".into()),
            "session.applied_view".into(),
            raw(json!({})),
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(applied.get()).unwrap()["view"]["version"],
        1
    );
    rig.model.finish.add_permits(1);
    rig.close().await;
}
