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
use broca_module::{DefaultOpener, LineageRegistry, RunExecutor, RunJob, RunKind};
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
async fn compaction_v2_metadata_is_durable_when_writer_enabled() {
    use broca_core::compaction_v2::protocol::{
        read::{Detail, ReadPage, ReadRequest, Select},
        view::View,
    };
    let rig = Rig::new(0);
    seed_mode3(&rig);
    let request = ReadRequest::new(
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
            serde_json::value::to_raw_value(&request).unwrap(),
        )
        .await
        .unwrap();
    let page: ReadPage = serde_json::from_str(page.get()).unwrap();
    let session_ref = broca_core::agent_session_ref(&rig.key);
    let capture = broca_test_support::LogCapture::observe(move |event| {
        event.message() == "compaction published view refused"
            && event.fields.get("session_ref") == Some(&session_ref)
    });
    let before = rig.wal.read_records(&rig.key).unwrap().len();
    let mut expected = Vec::new();
    for (version, new_signed) in [(1, None), (2, Some("strip".into()))] {
        let view = View::seal_with_policy(
            version,
            vec![],
            Some(page.entries[0].mid.clone()),
            new_signed,
        );
        assert!(view.verify());
        expected.push((version, view.new_signed.clone(), view.checksum.clone()));
        let answer = rig
            .registry
            .route_compaction_v2(
                &rig.key,
                Some("provider".into()),
                "session.apply_view".into(),
                serde_json::value::to_raw_value(&json!({"view":view})).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(answer.get()).unwrap(),
            json!({"answer":"accepted"})
        );
    }
    let events = capture.events();
    assert!(
        events.is_empty(),
        "accepted publications must not warn: {events:?}"
    );
    let records = rig.wal.read_records(&rig.key).unwrap();
    assert_eq!(records.len(), before + 2);
    let stored: Vec<_> = records
        .iter()
        .filter_map(|frame| match &frame.record {
            broca_wal::WalRecord::CompactionViewV2(view) => Some(view),
            _ => None,
        })
        .collect();
    assert_eq!(stored.len(), 2);
    for (stored, (version, new_signed, checksum)) in stored.iter().zip(&expected) {
        assert_eq!(stored.version, *version);
        assert_eq!(
            stored.built_through.as_deref(),
            Some(page.entries[0].mid.as_str())
        );
        assert_eq!(&stored.new_signed, new_signed);
        assert_eq!(&stored.checksum, checksum);
        assert_eq!(stored.applied_by.as_deref(), Some("provider"));
    }
    let mut restored = broca_core::compaction_v2::state::State::default();
    for frame in records {
        broca_core::compaction_v2::fold::fold(&mut restored, &frame.record, &rig.key).unwrap();
    }
    let latest = restored.latest().unwrap();
    assert!(latest.verify());
    assert_eq!(latest.checksum, expected[1].2);
    assert_eq!(latest.new_signed.as_deref(), Some("strip"));
    rig.close().await;
}

#[tokio::test]
async fn compaction_v2_refused_publication_warns_once_with_policy_presence() {
    use broca_core::compaction_v2::protocol::view::View;
    let rig = Rig::new(0);
    seed_mode3(&rig);
    let session_ref = broca_core::agent_session_ref(&rig.key);
    let expected_session = session_ref.clone();
    let capture = broca_test_support::LogCapture::observe(move |event| {
        event.message() == "compaction published view refused"
            && event.fields.get("session_ref") == Some(&expected_session)
    });
    let mut view = serde_json::to_value(View::seal(7, vec![])).unwrap();
    view["checksum"] = json!("bad-checksum");
    view["built_through"] = json!("m12345678.0");
    view["new_signed"] = json!("strip");
    let before = rig.wal.read_records(&rig.key).unwrap().len();
    let answer = rig
        .registry
        .route_compaction_v2(
            &rig.key,
            Some("provider".into()),
            "session.apply_view".into(),
            serde_json::value::to_raw_value(&json!({"view":view})).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(answer.get()).unwrap(),
        json!({"answer":"refused","refusal":{"code":"bad_checksum"}})
    );
    let logs = capture.events();
    assert_eq!(logs.len(), 1, "{logs:?}");
    let event = &logs[0];
    assert_eq!(event.level, tracing::Level::WARN);
    assert_eq!(event.target, "compaction");
    for (field, expected) in [
        ("session_ref", session_ref.as_str()),
        ("view_version", "7"),
        ("refusal_code", "bad_checksum"),
        ("built_through", "true"),
        ("new_signed", "true"),
    ] {
        assert_eq!(event.fields.get(field).map(String::as_str), Some(expected));
    }
    let malformed = rig
        .registry
        .route_compaction_v2(
            &rig.key,
            Some("provider".into()),
            "session.apply_view".into(),
            serde_json::value::to_raw_value(
                &json!({"view":{"version":"not-a-number","edits":[],"checksum":""}}),
            )
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert_eq!(malformed.code, "invalid_params");
    let events = capture.events();
    assert_eq!(events.len(), 2, "{events:?}");
    assert_eq!(events[1].fields["view_version"], "unknown");
    assert_eq!(events[1].fields["refusal_code"], "invalid_params");
    assert_eq!(events[1].fields["built_through"], "false");
    assert_eq!(events[1].fields["new_signed"], "false");
    assert_eq!(rig.wal.read_records(&rig.key).unwrap().len(), before);
    rig.close().await;
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
            raw(json!({"view":view,"applied_by":"spoofed-module"})),
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
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(applied.get()).unwrap()["applied_by"],
        "provider"
    );
    let mut restored = broca_core::compaction_v2::state::State::default();
    for frame in rig.wal.read_records(&rig.key).unwrap() {
        broca_core::compaction_v2::fold::fold(&mut restored, &frame.record, &rig.key).unwrap();
    }
    assert_eq!(restored.applied_by(), Some("provider"));
    rig.model.finish.add_permits(1);
    rig.close().await;
}
