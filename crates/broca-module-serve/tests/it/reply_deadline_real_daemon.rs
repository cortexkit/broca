//! Provider-declared waits over a real daemon, with a tool held behind an explicit gate.

use super::preset_real_daemon::{call, plan_for, register, Rig};
use async_trait::async_trait;
use broca_core::{
    lease_key_for, run_loop_on_writer, run_loop_with_control, AllowAll, FileLeaseStore,
    FileWalStore, LeaseStore, Provider, RunControl, RunRequest, StartMode, StubProvider, WalHandle,
    WalStore,
};
use broca_fetch_plan::PlanAdmitter;
use broca_protocol::{
    caller::RunFinishReason,
    content::{AssistantMessage, ContentBlock, Message, Role},
    provider::{NormalizedEvent, ProviderControl},
    request::{CallOptions, ModelHandle, StopCondition, ToolChoice},
    BindIdentity, FinishReason, GenerationConfig, MessageId, RunId, StepId, Usage,
};
use broca_subc::{SubcPlanAdmitter, SubcToolPlane};
use broca_wal::WalRecord;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tokio::sync::{mpsc, Notify};

// The production floor is 120 seconds. Only this plane's debug-only test seam shortens it.
const FALLBACK_MS: u64 = 1_200;
const HELD_MS: u64 = 1_250;

#[derive(Default)]
struct Gate {
    max_ms: AtomicU64,
    catalogs: AtomicUsize,
    entered: Notify,
    release: Notify,
}

struct HeldTool(Arc<Gate>);
#[async_trait]
impl subc_client_rs::ModuleHandler for HeldTool {
    async fn handle(
        &self,
        _: subc_client_rs::RequestCtx,
        bytes: Vec<u8>,
    ) -> subc_client_rs::HandlerOutcome {
        let request: Value = serde_json::from_slice(&bytes).unwrap();
        let answer = if request["name"] == "tool.catalog" {
            self.0.catalogs.fetch_add(1, Ordering::SeqCst);
            let args = &request["arguments"];
            let schema = json!({"type":"object"});
            let mut tool = json!({"name":"read", "semantics":1, "schema_digest":cortexkit_role_tool_provider::catalog::schema_digest(&schema).unwrap(), "capabilities":[], "input_schema":schema});
            let max = self.0.max_ms.load(Ordering::SeqCst);
            if max != 0 {
                tool["reply"] = json!({"max_ms":max});
            }
            json!({"generation":"1", "catalog_digest":"0".repeat(64), "composition_digest":cortexkit_role_tool_provider::catalog::composition_digest(&args["composition"]).unwrap(), "tools":[tool]})
        } else {
            assert_eq!(request["name"], "read");
            self.0.entered.notify_one();
            self.0.release.notified().await;
            json!({"content":[{"type":"text", "text":"gate released"}], "isError":false})
        };
        subc_client_rs::HandlerOutcome::Response(serde_json::to_vec(&answer).unwrap())
    }
}

async fn register_held(rig: &Rig, gate: Arc<Gate>) -> tokio::task::JoinHandle<()> {
    use subc_protocol::manifest::{
        Concurrency, ExecutionMode, IdentityScope, ModuleManifest, ProviderRole, Tool,
    };
    let manifest = ModuleManifest::builder("aft", "1")
        .provides(vec![ProviderRole::ToolProvider {
            tools: vec![Tool {
                name: "read".into(),
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
    let (_, serve) =
        subc_client_rs::serve_with_handle(&rig.connection_file, manifest, HeldTool(gate))
            .await
            .unwrap();
    tokio::spawn(async move {
        let _ = serve.await;
    })
}

fn request(session: BindIdentity, frozen: &broca_wal::PlanManifest) -> RunRequest {
    RunRequest {
        session_class: None,
        retention: None,
        title: None,
        on_restart: None,
        keep_warm: None,
        scope: None,
        send_id: None,
        request_identity_digest: None,
        project_id: None,
        principal: None,
        run_id: RunId("reply-gate".into()),
        session,
        model: ModelHandle::new("test", "test"),
        input: vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: "read".into(),
            }],
        }],
        tools: frozen.tools.as_ref().clone(),
        policy_allowed: frozen.tools.iter().map(|tool| tool.name.clone()).collect(),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        stop_when: vec![StopCondition::StepCount { n: 4 }],
        submission_id: None,
        budget: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
        structured_output: None,
    }
}

fn scripts() -> StubProvider {
    StubProvider::new(vec![
        vec![
            NormalizedEvent::Control(ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id: MessageId("tool-step".into()),
                    content: vec![ContentBlock::ToolCall(call("read", json!({}), "held-read"))],
                },
            }),
            NormalizedEvent::Control(ProviderControl::Finished {
                usage: Usage::default(),
                finish_reason: FinishReason::ToolCalls,
                retries_used: 0,
            }),
        ],
        vec![
            NormalizedEvent::Control(ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id: MessageId("text-step".into()),
                    content: vec![ContentBlock::Text {
                        text: "done".into(),
                    }],
                },
            }),
            NormalizedEvent::Control(ProviderControl::Finished {
                usage: Usage::default(),
                finish_reason: FinishReason::Stop,
                retries_used: 0,
            }),
        ],
    ])
}

/// Parks the first writer's `StepStarted` append on that writer's thread until released. The run
/// queues `StepStarted` without waiting for it, then calls the model, so with this hold the record
/// is certainly still queued when a test ends the run at the model. The writer thread outlives the
/// run's future, and only `WalHandle::shutdown` waits for the queued append to land.
///
/// A second open of the WAL while the append is still parked lets it land only after that open
/// has measured the file. That is the order a writer outliving its run produces, so the second
/// writer's first append reports a foreign write instead of passing by luck of timing.
struct HeldAppend {
    state: std::sync::Mutex<HeldState>,
    changed: std::sync::Condvar,
    parked: Notify,
}

#[derive(Default)]
struct HeldState {
    opens: usize,
    parked: bool,
    released: bool,
    landed: bool,
}

impl HeldAppend {
    fn new(hold: bool) -> Arc<Self> {
        Arc::new(Self {
            state: std::sync::Mutex::new(HeldState {
                released: !hold,
                ..HeldState::default()
            }),
            changed: std::sync::Condvar::new(),
            parked: Notify::new(),
        })
    }

    fn release(&self) {
        self.state.lock().unwrap().released = true;
        self.changed.notify_all();
    }
}

struct HeldWal {
    inner: Box<dyn broca_wal::Wal + Send>,
    hold: Option<Arc<HeldAppend>>,
}

impl broca_wal::Wal for HeldWal {
    fn append(
        &mut self,
        fence: u64,
        record: &WalRecord,
        sync: bool,
    ) -> Result<u64, broca_wal::WalError> {
        if !matches!(record, WalRecord::StepStarted { .. }) {
            return self.inner.append(fence, record, sync);
        }
        let Some(hold) = self.hold.take() else {
            return self.inner.append(fence, record, sync);
        };
        let mut state = hold.state.lock().unwrap();
        state.parked = true;
        hold.parked.notify_one();
        while !state.released {
            state = hold.changed.wait(state).unwrap();
        }
        drop(state);
        let appended = self.inner.append(fence, record, sync);
        hold.state.lock().unwrap().landed = true;
        hold.changed.notify_all();
        appended
    }

    fn sync(&self) -> Result<(), broca_wal::WalError> {
        self.inner.sync()
    }

    fn replay(&self) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        self.inner.replay()
    }

    fn tail_after_seq(
        &self,
        after_seq: u64,
    ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        self.inner.tail_after_seq(after_seq)
    }

    fn last_seq(&self) -> u64 {
        self.inner.last_seq()
    }

    fn lineage_id(&self) -> Option<broca_wal::WalLineageId> {
        self.inner.lineage_id()
    }
}

/// A `FileWalStore` whose first opened writer holds its `StepStarted` append (see `HeldAppend`).
/// Every other operation is the file store's own.
struct HeldStore {
    inner: FileWalStore,
    hold: Arc<HeldAppend>,
}

impl WalStore for HeldStore {
    fn open(
        &self,
        session: &BindIdentity,
    ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
        self.open_with_retention(session, None)
    }

    fn open_with_retention(
        &self,
        session: &BindIdentity,
        retention: Option<u64>,
    ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
        let inner = WalStore::open_with_retention(&self.inner, session, retention)?;
        let mut state = self.hold.state.lock().unwrap();
        state.opens += 1;
        if state.opens == 1 {
            return Ok(Box::new(HeldWal {
                inner,
                hold: Some(self.hold.clone()),
            }));
        }
        if state.parked && !state.landed {
            // An earlier writer still has the parked append queued: land it now, after this
            // writer has measured the file.
            state.released = true;
            self.hold.changed.notify_all();
            while !state.landed {
                state = self.hold.changed.wait(state).unwrap();
            }
        }
        Ok(inner)
    }

    fn lineage_exists(
        &self,
        session: &BindIdentity,
        meta: Option<&broca_core::AdmissionStoreSnapshot>,
    ) -> Result<bool, broca_wal::WalError> {
        WalStore::lineage_exists(&self.inner, session, meta)
    }

    fn is_retention_lineage(&self, session: &BindIdentity) -> Result<bool, broca_wal::WalError> {
        WalStore::is_retention_lineage(&self.inner, session)
    }

    fn wal_present(&self, session: &BindIdentity) -> Option<bool> {
        WalStore::wal_present(&self.inner, session)
    }

    fn live_wal_last_write(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<std::time::SystemTime>, broca_wal::WalError> {
        WalStore::live_wal_last_write(&self.inner, session)
    }

    fn read_records(
        &self,
        session: &BindIdentity,
    ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        WalStore::read_records(&self.inner, session)
    }

    fn read_records_range(
        &self,
        session: &BindIdentity,
        after_seq: u64,
        through_seq: Option<u64>,
        anchor: Option<&broca_wal::WalByteAnchor>,
    ) -> Result<broca_wal::WalRangeRead, broca_wal::WalError> {
        WalStore::read_records_range(&self.inner, session, after_seq, through_seq, anchor)
    }

    fn read_records_range_from_valid_anchor(
        &self,
        session: &BindIdentity,
        after_seq: u64,
        through_seq: Option<u64>,
        anchor: &broca_wal::WalByteAnchor,
    ) -> Result<Option<broca_wal::WalRangeRead>, broca_wal::WalError> {
        WalStore::read_records_range_from_valid_anchor(
            &self.inner,
            session,
            after_seq,
            through_seq,
            anchor,
        )
    }

    fn flush_for_read(&self, session: &BindIdentity) -> Result<Option<u64>, broca_wal::WalError> {
        WalStore::flush_for_read(&self.inner, session)
    }

    fn file_identity(&self, session: &BindIdentity) -> Result<Option<u64>, broca_wal::WalError> {
        WalStore::file_identity(&self.inner, session)
    }
}

struct HeldModel(Notify);
#[async_trait]
impl Provider for HeldModel {
    async fn step(&self, _: StepId, _: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        self.0.notify_one();
        std::future::pending().await
    }
}

async fn held_run(declared: bool, resume: bool) {
    let mut rig = Rig::start(None).await;
    let gate = Arc::new(Gate::default());
    gate.max_ms
        .store(if declared { 180_000 } else { 0 }, Ordering::SeqCst);
    let held = register_held(&rig, gate.clone()).await;
    let (_, core) = register(&rig, "prefrontal-core", Arc::new(Mutex::new(Vec::new()))).await;
    let conn = rig.connect().await;
    let session = rig.session("reply-gate");
    let plan = broca_fetch_plan::parse_plan(
        &plan_for(json!([{"name":"read", "capabilities":[]}])),
        broca_fetch_plan::CompactionSupport::Unsupported,
    )
    .unwrap();
    let frozen = Arc::new(
        SubcPlanAdmitter::new(conn.clone())
            .admit(&plan, &session, None)
            .await
            .unwrap()
            .manifest,
    );
    let plane = SubcToolPlane::new(Arc::try_unwrap(conn).ok().unwrap(), "aft")
        .with_reply_fallback_for_test(FALLBACK_MS);
    let hold = HeldAppend::new(resume);
    let wal = HeldStore {
        inner: FileWalStore::new(rig.dir.join("wal")),
        hold: hold.clone(),
    };
    let lease = FileLeaseStore::new(rig.dir.join("leases"));
    let (tx, mut rx) = mpsc::channel(512);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let mut control = RunControl {
        manifest: Some(frozen.clone()),
        ..Default::default()
    };
    if resume {
        // Drop the first run only after its durable start reached the model seam, before any tool intent.
        // Like the session actor, the test owns this run's lease and WAL writer: dropping the run's
        // future does not stop the writer thread, which still holds the queued StepStarted append.
        let model = HeldModel(Notify::new());
        let seed_lease = lease.acquire(&lease_key_for(&session)).unwrap();
        let seed_writer =
            WalHandle::spawn(wal.open(&session).unwrap(), seed_lease.epoch()).unwrap();
        {
            let run = run_loop_on_writer(
                request(session.clone(), &frozen),
                &seed_writer,
                StartMode::FreshOrResume,
                &model,
                &plane,
                &AllowAll,
                &wal,
                None,
                control.clone(),
                &tx,
            );
            tokio::pin!(run);
            tokio::select! {
                _ = model.0.notified() => {},
                result = &mut run => panic!("seed run unexpectedly ended: {result:?}"),
                _ = tokio::time::sleep(Duration::from_secs(10)) => panic!("seed run never reached the model"),
            }
            tokio::time::timeout(Duration::from_secs(10), hold.parked.notified())
                .await
                .expect("seed writer never reached its StepStarted append");
        }
        // Land the queued append and join the writer before releasing the lease, as the actor
        // does at close. Only then may another writer open this WAL.
        hold.release();
        let _ = seed_writer.shutdown().await;
        drop(seed_lease);
        assert!(wal
            .read_records(&session)
            .unwrap()
            .iter()
            .any(|frame| matches!(
                &frame.record,
                WalRecord::RunStarted {
                    manifest: Some(_),
                    ..
                }
            )));
        // Both a changed provider and a new caller control disagree with the WAL. Neither may win.
        gate.max_ms.store(1, Ordering::SeqCst);
        let mut later = frozen.as_ref().clone();
        let broca_wal::FetchOutcome::Answered { reply, .. } = &mut later.fetches[0].outcome else {
            panic!("answered fetch")
        };
        *reply = Some(
            [(
                "read".into(),
                broca_wal::FrozenReply {
                    max_ms: 1,
                    hold_argument: None,
                },
            )]
            .into(),
        );
        control.manifest = Some(Arc::new(later));
    }
    {
        let provider = scripts();
        let started = tokio::time::Instant::now();
        let run = run_loop_with_control(
            request(session.clone(), &frozen),
            &provider,
            &plane,
            &lease,
            &AllowAll,
            &wal,
            None,
            control,
            &tx,
        );
        tokio::pin!(run);
        tokio::select! {
            _ = gate.entered.notified() => {},
            result = &mut run => panic!("run ended before the stub held the call: {result:?}"),
            _ = tokio::time::sleep(Duration::from_secs(10)) => panic!("stub never received call"),
        }
        let summary = if declared {
            // The provider does no timed work: the test owns the gate and observes a full hold.
            assert!(
                tokio::time::timeout(Duration::from_millis(HELD_MS), &mut run)
                    .await
                    .is_err(),
                "declared reply was cut at the fallback while the gate was held"
            );
            gate.release.notify_one();
            tokio::time::timeout(Duration::from_secs(10), &mut run)
                .await
                .unwrap()
                .unwrap()
        } else {
            let summary = tokio::time::timeout(Duration::from_secs(10), &mut run)
                .await
                .unwrap()
                .unwrap();
            assert!(started.elapsed() >= Duration::from_millis(FALLBACK_MS));
            gate.release.notify_one();
            summary
        };
        assert_eq!(
            summary.reason,
            if declared {
                RunFinishReason::Completed
            } else {
                RunFinishReason::Interrupted
            }
        );
        let records = wal.read_records(&session).unwrap();
        let outcomes: Vec<_> = records
            .iter()
            .filter_map(|frame| match &frame.record {
                WalRecord::ToolResult {
                    outcome, result, ..
                } => Some((*outcome, result.is_error)),
                _ => None,
            })
            .collect();
        assert_eq!(
            outcomes,
            if declared {
                vec![(broca_wal::DispatchOutcome::Completed, false)]
            } else {
                vec![(broca_wal::DispatchOutcome::Unknown, true)]
            }
        );
        assert_eq!(
            gate.catalogs.load(Ordering::SeqCst),
            1,
            "dispatch and resume must not refetch the catalog"
        );
        if resume {
            assert_eq!(
                records
                    .iter()
                    .filter(|frame| matches!(&frame.record, WalRecord::RunStarted { .. }))
                    .count(),
                1
            );
        }
    }
    plane.close_session_routes(&session).await;
    drop(tx);
    collector.await.unwrap();
    held.abort();
    core.abort();
    rig.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declared_reply_outlasts_fallback_over_real_daemon() {
    held_run(true, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_reply_still_interrupts_at_fallback_over_real_daemon() {
    held_run(false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resumed_run_uses_wal_frozen_reply_over_real_daemon() {
    held_run(true, true).await;
}
