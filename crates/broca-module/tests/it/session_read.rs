use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use broca_core::{
    project_control, replay, run_loop_on_writer_admitted, AllowAll, FileLeaseStore, FileWalStore,
    MaterializedStore, OutputEvent, Persister, RunError, RunIndex, RunIndexRow, RunIndexState,
    RunRequest, RunSummary, StartMode, StubProvider, StubToolPlane, WalStore,
};
use broca_module::executor::{RunExecutor, RunJob, RunKind};
use broca_module::serve::{SessionImportRequest, SessionReadRequest, SubscribeFrom};
use broca_module::{DefaultOpener, Delivery, LineageRegistry, ServeHandler};
use broca_protocol::caller::{ControlUnit, PauseReason, RunFinishReason};
use broca_protocol::content::{
    AssistantMessage, ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult,
};
use broca_protocol::error::ProviderError;
use broca_protocol::provider::{NormalizedEvent, ProviderControl};
use broca_protocol::request::{
    BudgetConfig, BudgetKind, BudgetNudge, ExecutionMode, ModelHandle, Tool, ToolChoice,
};
use broca_protocol::{
    BatchId, BindIdentity, ErrorClass, FinishReason, MessageId, RunId, StepId, ToolCallId, Usage,
};
use broca_wal::{DispatchOutcome, FileWal, RunConfig, Wal, WalRecord};
use tokio::sync::{mpsc, Mutex};
#[path = "session_read/attribution.rs"]
mod attribution;
#[path = "session_read/wal_cost.rs"]
mod wal_cost;

struct ScriptedCoreExec {
    wal_store: Arc<FileWalStore>,
    store: Arc<broca_store::SqliteStore>,
    scripts: Mutex<VecDeque<Vec<Vec<NormalizedEvent>>>>,
}

#[async_trait]
impl RunExecutor for ScriptedCoreExec {
    async fn execute(
        &self,
        job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        let mode = match job.kind {
            RunKind::AppendEpisode => StartMode::AppendEpisode,
            RunKind::ResumeAfterAuth => StartMode::ResumeAfterAuth,
            RunKind::FreshOrResume => StartMode::FreshOrResume,
        };
        let scripts = self
            .scripts
            .lock()
            .await
            .pop_front()
            .expect("test must provide one provider script per run");
        let provider = StubProvider::new(scripts);
        let plane = StubToolPlane::new().with_canned_text("lookup", "tool says ok");
        let materialized_store =
            job.materialized_store(self.store.clone() as Arc<dyn broca_core::SessionStore>);
        run_loop_on_writer_admitted(
            job.req,
            &job.writer,
            mode,
            &provider,
            &plane,
            &AllowAll,
            self.wal_store.as_ref(),
            materialized_store,
            job.control,
            &out,
            job.on_admitted,
        )
        .await
    }
}

struct ReadCountingWalStore {
    inner: Arc<FileWalStore>,
    full_reads: Arc<AtomicUsize>,
}

impl WalStore for ReadCountingWalStore {
    fn is_retention_lineage(&self, session: &BindIdentity) -> Result<bool, broca_wal::WalError> {
        self.inner.is_retention_lineage(session)
    }

    fn lineage_exists(
        &self,
        session: &BindIdentity,
        meta: Option<&broca_core::AdmissionStoreSnapshot>,
    ) -> Result<bool, broca_wal::WalError> {
        self.inner.lineage_exists(session, meta)
    }

    fn wal_present(&self, session: &BindIdentity) -> Option<bool> {
        self.inner.wal_present(session)
    }

    fn open(&self, session: &BindIdentity) -> Result<Box<dyn Wal + Send>, broca_wal::WalError> {
        self.inner.open(session)
    }

    fn read_records(
        &self,
        session: &BindIdentity,
    ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        self.full_reads.fetch_add(1, Ordering::SeqCst);
        self.inner.read_records(session)
    }

    fn read_records_range(
        &self,
        session: &BindIdentity,
        after_seq: u64,
        through_seq: Option<u64>,
        anchor: Option<&broca_wal::WalByteAnchor>,
    ) -> Result<broca_wal::WalRangeRead, broca_wal::WalError> {
        self.inner
            .read_records_range(session, after_seq, through_seq, anchor)
    }
}

struct ZeroRangeCountingWalStore {
    inner: Arc<FileWalStore>,
    zero_range_reads: Arc<AtomicUsize>,
}

impl WalStore for ZeroRangeCountingWalStore {
    fn open(&self, session: &BindIdentity) -> Result<Box<dyn Wal + Send>, broca_wal::WalError> {
        self.inner.open(session)
    }

    fn read_records(
        &self,
        session: &BindIdentity,
    ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        self.inner.read_records(session)
    }

    fn read_records_range(
        &self,
        session: &BindIdentity,
        after_seq: u64,
        through_seq: Option<u64>,
        anchor: Option<&broca_wal::WalByteAnchor>,
    ) -> Result<broca_wal::WalRangeRead, broca_wal::WalError> {
        if after_seq == 0 {
            self.zero_range_reads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner
            .read_records_range(session, after_seq, through_seq, anchor)
    }

    fn flush_for_read(&self, session: &BindIdentity) -> Result<Option<u64>, broca_wal::WalError> {
        self.inner.flush_for_read(session)
    }

    fn file_identity(&self, session: &BindIdentity) -> Result<Option<u64>, broca_wal::WalError> {
        self.inner.file_identity(session)
    }

    fn wal_present(&self, session: &BindIdentity) -> Option<bool> {
        self.inner.wal_present(session)
    }

    fn live_wal_last_write(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<std::time::SystemTime>, broca_wal::WalError> {
        self.inner.live_wal_last_write(session)
    }
}

struct FlushCountingWalStore {
    inner: Arc<FileWalStore>,
    flushes: Arc<AtomicUsize>,
    fail_flush: bool,
}

impl WalStore for FlushCountingWalStore {
    fn open(&self, session: &BindIdentity) -> Result<Box<dyn Wal + Send>, broca_wal::WalError> {
        self.inner.open(session)
    }

    fn read_records(
        &self,
        session: &BindIdentity,
    ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        self.inner.read_records(session)
    }

    fn read_records_range(
        &self,
        session: &BindIdentity,
        after_seq: u64,
        through_seq: Option<u64>,
        anchor: Option<&broca_wal::WalByteAnchor>,
    ) -> Result<broca_wal::WalRangeRead, broca_wal::WalError> {
        self.inner
            .read_records_range(session, after_seq, through_seq, anchor)
    }

    fn flush_for_read(&self, session: &BindIdentity) -> Result<Option<u64>, broca_wal::WalError> {
        self.flushes.fetch_add(1, Ordering::SeqCst);
        if self.fail_flush {
            return Err(broca_wal::WalError::Io(std::io::Error::other(
                "injected read-side flush failure",
            )));
        }
        self.inner.flush_for_read(session)
    }

    fn file_identity(&self, session: &BindIdentity) -> Result<Option<u64>, broca_wal::WalError> {
        self.inner.file_identity(session)
    }
}

struct NoopExec;

#[async_trait]
impl RunExecutor for NoopExec {
    async fn execute(
        &self,
        _job: RunJob,
        _out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        panic!("session.read must not start a run executor")
    }
}

struct Rig {
    dir: std::path::PathBuf,
    lease_dir: std::path::PathBuf,
    registry: LineageRegistry,
    wal_store: Arc<FileWalStore>,
    store: Arc<broca_store::SqliteStore>,
}

impl Rig {
    fn new(tag: &str, exec: Arc<dyn RunExecutor>) -> Self {
        Self::with_exec_factory(tag, move |_, _| exec)
    }

    fn with_exec_factory(
        tag: &str,
        make_exec: impl FnOnce(Arc<FileWalStore>, Arc<broca_store::SqliteStore>) -> Arc<dyn RunExecutor>,
    ) -> Self {
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-session-read-{}-{tag}-{}",
            std::process::id(),
            unique()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        let lease_dir = dir.join("leases");
        let lease_store = Arc::new(FileLeaseStore::new(&lease_dir));
        let wal_store = Arc::new(FileWalStore::new(dir.join("wal")));
        let store = Arc::new(broca_store::SqliteStore::open(dir.join("store.db")).unwrap());
        let exec = make_exec(wal_store.clone(), store.clone());
        let opener = Arc::new(
            DefaultOpener::new(lease_store, wal_store.clone(), exec)
                .with_idle_ttl(Duration::from_secs(3600))
                .with_run_index(store.clone())
                .with_materialized_store(store.clone()),
        );
        let wal_reader: Arc<dyn broca_core::WalStore> = wal_store.clone();
        let store_reader: Arc<dyn broca_core::MaterializedStore> = store.clone();
        let registry = LineageRegistry::new_with_reader(opener, wal_reader, Some(store_reader));
        Self {
            dir,
            lease_dir,
            registry,
            wal_store,
            store,
        }
    }

    fn with_counting_wal(tag: &str) -> (Self, Arc<AtomicUsize>) {
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-session-read-{}-{tag}-{}",
            std::process::id(),
            unique()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        let lease_dir = dir.join("leases");
        let lease_store = Arc::new(FileLeaseStore::new(&lease_dir));
        let wal_store = Arc::new(FileWalStore::new(dir.join("wal")));
        let full_reads = Arc::new(AtomicUsize::new(0));
        let counting_wal = Arc::new(ReadCountingWalStore {
            inner: wal_store.clone(),
            full_reads: full_reads.clone(),
        });
        let store = Arc::new(broca_store::SqliteStore::open(dir.join("store.db")).unwrap());
        let opener = Arc::new(
            DefaultOpener::new(lease_store, counting_wal.clone(), Arc::new(NoopExec))
                .with_idle_ttl(Duration::from_secs(3600))
                .with_run_index(store.clone())
                .with_materialized_store(store.clone()),
        );
        let wal_reader: Arc<dyn WalStore> = counting_wal;
        let store_reader: Arc<dyn MaterializedStore> = store.clone();
        let registry = LineageRegistry::new_with_reader(opener, wal_reader, Some(store_reader));
        (
            Self {
                dir,
                lease_dir,
                registry,
                wal_store,
                store,
            },
            full_reads,
        )
    }

    fn with_counting_zero_ranges(tag: &str) -> (Self, Arc<AtomicUsize>) {
        Self::with_counting_zero_ranges_and_exec(tag, |_, _| Arc::new(NoopExec))
    }

    /// [`Self::with_counting_zero_ranges`] with runs executed by `make_exec`.
    fn with_counting_zero_ranges_and_exec(
        tag: &str,
        make_exec: impl FnOnce(Arc<FileWalStore>, Arc<broca_store::SqliteStore>) -> Arc<dyn RunExecutor>,
    ) -> (Self, Arc<AtomicUsize>) {
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-session-read-{}-{tag}-{}",
            std::process::id(),
            unique()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        let lease_dir = dir.join("leases");
        let lease_store = Arc::new(FileLeaseStore::new(&lease_dir));
        let wal_store = Arc::new(FileWalStore::new(dir.join("wal")));
        let zero_range_reads = Arc::new(AtomicUsize::new(0));
        let counting_wal = Arc::new(ZeroRangeCountingWalStore {
            inner: wal_store.clone(),
            zero_range_reads: zero_range_reads.clone(),
        });
        let store = Arc::new(broca_store::SqliteStore::open(dir.join("store.db")).unwrap());
        let exec = make_exec(wal_store.clone(), store.clone());
        let opener = Arc::new(
            DefaultOpener::new(lease_store, counting_wal.clone(), exec)
                .with_idle_ttl(Duration::from_secs(3600))
                .with_run_index(store.clone())
                .with_materialized_store(store.clone()),
        );
        let wal_reader: Arc<dyn WalStore> = counting_wal;
        let store_reader: Arc<dyn MaterializedStore> = store.clone();
        let registry = LineageRegistry::new_with_reader(opener, wal_reader, Some(store_reader));
        (
            Self {
                dir,
                lease_dir,
                registry,
                wal_store,
                store,
            },
            zero_range_reads,
        )
    }

    fn key(&self, name: &str) -> BindIdentity {
        BindIdentity::new(self.dir.join("proj"), "runner", name)
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn unique() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    N.fetch_add(1, Ordering::SeqCst)
}

fn ctrl(unit: ProviderControl) -> NormalizedEvent {
    NormalizedEvent::Control(unit)
}

fn usage() -> Usage {
    Usage {
        input_tokens: Some(7),
        output_tokens: Some(3),
        ..Usage::default()
    }
}

fn tool_step() -> Vec<NormalizedEvent> {
    vec![
        ctrl(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId("m-tool".into()),
                content: vec![ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId("call-1".into()),
                    tool_name: "lookup".into(),
                    input: serde_json::json!({"q":"weather"}),
                })],
            },
        }),
        ctrl(ProviderControl::StepFinished {
            step_id: StepId(0),
            usage: usage(),
            finish_reason: FinishReason::ToolCalls,
            service_tier_used: None,
            speed_used: None,
        }),
        ctrl(ProviderControl::Finished {
            usage: usage(),
            finish_reason: FinishReason::ToolCalls,
            retries_used: 0,
        }),
    ]
}

fn text_step(text: &str) -> Vec<NormalizedEvent> {
    vec![
        ctrl(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId("m-text".into()),
                content: vec![ContentBlock::Text { text: text.into() }],
            },
        }),
        ctrl(ProviderControl::StepFinished {
            step_id: StepId(1),
            usage: usage(),
            finish_reason: FinishReason::Stop,
            service_tier_used: None,
            speed_used: None,
        }),
        ctrl(ProviderControl::Finished {
            usage: usage(),
            finish_reason: FinishReason::Stop,
            retries_used: 0,
        }),
    ]
}

fn error_value(message: &str) -> ProviderError {
    ProviderError {
        refusal: None,
        class: ErrorClass::Permanent,
        message: message.into(),
        status: Some(500),
        retry_after_secs: None,
        provider_code: Some("boom_code".into()),
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
    }
}

fn error_step(error: ProviderError) -> Vec<NormalizedEvent> {
    vec![ctrl(ProviderControl::Error {
        error,
        retries_used: 0,
    })]
}

fn tool(name: &str) -> Tool {
    Tool {
        name: name.into(),
        description: "test tool".into(),
        input_schema: serde_json::json!({"type":"object"}),
        module: None,
        execution_mode: ExecutionMode::Parallel,
    }
}

fn req(key: &BindIdentity, run: &str, text: &str) -> RunRequest {
    let tools = vec![tool("lookup")];
    let policy_allowed: HashSet<String> = tools.iter().map(|t| t.name.clone()).collect();
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
        run_id: RunId(run.into()),
        session: key.clone(),
        model: ModelHandle::new("stub", "stub-model"),
        input: vec![Message::new(
            Role::User,
            vec![ContentBlock::Text { text: text.into() }],
        )],
        tools,
        policy_allowed,
        tool_choice: ToolChoice::Auto,
        generation: Default::default(),
        cache: None,
        stop_when: vec![],
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

async fn send(rig: &Rig, key: &BindIdentity, run: &str, text: &str) {
    send_with_materialization(rig, key, run, text, false).await;
}

async fn send_with_materialization(
    rig: &Rig,
    key: &BindIdentity,
    run: &str,
    text: &str,
    skip_materialization: bool,
) {
    let req = req(key, run, text);
    rig.registry
        .send(broca_module::serve::SendRequest {
            session_class: None,
            retention: None,
            stop_sequences_named: false,
            title: None,
            conversation_key: None,
            delivery: Default::default(),
            session: key.clone(),
            run_id: req.run_id.clone(),
            send_id: Some(format!("send-{run}")),
            request: Box::new(req.to_submission()),
            skip_materialization,
            kind: RunKind::FreshOrResume,
            plan: None,
        })
        .await
        .expect("send starts");
}

async fn wait_status(rig: &Rig, key: &BindIdentity, run: &str, want: &str) {
    broca_test_support::wait_until(&format!("run {run} reaches {want}"), || async {
        let state = rig
            .registry
            .run_status(key, RunId(run.into()))
            .await
            .expect("status resolves");
        let got = serde_json::to_value(state).unwrap()["state"]
            .as_str()
            .unwrap()
            .to_string();
        if got == want {
            return Ok(());
        }
        Err(format!("run_state={got}"))
    })
    .await;
}

fn append_seed(wal_store: &FileWalStore, key: &BindIdentity, records: &[WalRecord]) {
    let mut wal = FileWal::open(wal_store.wal_path(key))
        .expect("open test wal")
        .with_feature_floor(wal_store.feature_floor_path());
    for record in records {
        wal.append(1, record, true).expect("append seed record");
    }
}

fn write_legacy_seed(wal_store: &FileWalStore, key: &BindIdentity, records: &[WalRecord]) {
    let path = wal_store.wal_path(key);
    std::fs::create_dir_all(path.parent().expect("WAL path has a parent"))
        .expect("create legacy WAL directory");
    let mut bytes = Vec::new();
    for (index, record) in records.iter().enumerate() {
        bytes.extend(
            broca_wal::framing::encode(index as u64 + 1, 1, record)
                .expect("encode legacy WAL record"),
        );
    }
    std::fs::write(path, bytes).expect("write legacy WAL bytes");
}

fn stored_lineage_id(wal_store: &FileWalStore, key: &BindIdentity) -> Option<String> {
    wal_store
        .read_records_range(key, 0, None, None)
        .expect("read WAL lineage metadata")
        .lineage_id
        .map(|lineage_id| lineage_id.to_string())
}

#[tokio::test]
async fn fresh_lineage_read_twice_returns_the_same_lineage_id() {
    let rig = Rig::new("lineage-id-stable", Arc::new(NoopExec));
    let key = rig.key("stable");
    append_seed(
        &rig.wal_store,
        &key,
        &[
            started(&key, "run-stable", "hello"),
            assistant_text(0, "world"),
            WalRecord::RunFinished {
                ts_ms: None,
                reason: RunFinishReason::Completed,
                usage: usage(),
                error: None,
                retries_used: 0,
            },
        ],
    );
    let framed = rig.wal_store.read_records(&key).expect("read fresh WAL");
    rig.store
        .project(&key, &framed)
        .expect("materialize the live-shaped session");

    let first = rig
        .registry
        .read(read_request(&key, Some(0), Some(10)))
        .await
        .expect("first session.read");
    let second = rig
        .registry
        .read(read_request(&key, Some(0), Some(10)))
        .await
        .expect("second session.read");
    let first_id = first
        .lineage_id
        .as_deref()
        .expect("fresh WAL has lineage id");
    assert_eq!(second.lineage_id.as_deref(), Some(first_id));
    println!(
        "CAPTURED_SESSION_READ_ENVELOPE={}",
        serde_json::to_string(&first).expect("serialize captured session.read response")
    );
}

#[tokio::test]
async fn range_projection_persists_lineage_with_its_page_checkpoint() {
    let rig = Rig::new("store-lineage-persisted", Arc::new(NoopExec));
    let key = rig.key("persisted");
    append_seed(
        &rig.wal_store,
        &key,
        &[
            started(&key, "run-persisted", "hello"),
            assistant_text(0, "world"),
            WalRecord::RunFinished {
                ts_ms: None,
                reason: RunFinishReason::Completed,
                usage: usage(),
                error: None,
                retries_used: 0,
            },
        ],
    );
    let range = rig
        .wal_store
        .read_records_range(&key, 0, None, None)
        .expect("read identified WAL range");
    let lineage_id = range.lineage_id.expect("fresh WAL lineage id");
    let watermark = Persister::project_range(rig.store.as_ref(), &key, &range)
        .expect("project identified WAL range");

    let page = rig
        .store
        .read_page_with_watermark(&key, Some(0), 10, broca_core::context::ReadView::Raw)
        .expect("read projected page metadata");
    let admission = rig
        .store
        .read_admission_snapshot(&key)
        .expect("read projected admission metadata")
        .expect("projected admission row");
    assert_eq!(page.watermark_seq, watermark);
    assert_eq!(admission.watermark_seq, watermark);
    assert_eq!(
        page.lineage_id.as_deref(),
        Some(lineage_id.to_string().as_str())
    );
    assert_eq!(admission.lineage_id, page.lineage_id);
    assert_eq!(page.anchor, range.anchor_at(watermark));
    assert_eq!(admission.anchor, page.anchor);

    let first = rig
        .registry
        .route_read(&key, Some(0), Some(10))
        .await
        .expect("first matching store-backed read");
    let second = rig
        .registry
        .route_read(&key, Some(0), Some(10))
        .await
        .expect("second matching store-backed read");
    assert_eq!(first.lineage_id, Some(lineage_id));
    assert_eq!(second, first);
}

#[tokio::test]
async fn legacy_store_snapshot_without_lineage_id_keeps_the_incremental_read() {
    let (rig, zero_range_reads) = Rig::with_counting_zero_ranges("legacy-store-lineage-unknown");
    let key = rig.key("legacy-store");
    append_seed(
        &rig.wal_store,
        &key,
        &[
            started(&key, "run-legacy-store", "legacy prompt"),
            assistant_text(0, "legacy answer"),
            WalRecord::RunFinished {
                ts_ms: None,
                reason: RunFinishReason::Completed,
                usage: usage(),
                error: None,
                retries_used: 0,
            },
        ],
    );
    let full_range = rig
        .wal_store
        .read_records_range(&key, 0, None, None)
        .expect("read complete WAL");
    rig.store
        .project(
            &key,
            match full_range.records() {
                broca_wal::WalRangeRecords::Complete(records)
                | broca_wal::WalRangeRecords::Partial(records)
                | broca_wal::WalRangeRecords::Unknown(records) => records,
                broca_wal::WalRangeRecords::Absent => &[],
            },
        )
        .expect("simulate a projection written before lineage tracking");
    let legacy_snapshot = rig
        .store
        .read_page_with_watermark(&key, Some(0), 10, broca_core::context::ReadView::Raw)
        .expect("read legacy store snapshot");
    assert_eq!(legacy_snapshot.lineage_id, None);

    let mut expected = broca_core::read_session_page(
        &broca_core::StorePageSnapshot::empty(),
        match full_range.records() {
            broca_wal::WalRangeRecords::Complete(records)
            | broca_wal::WalRangeRecords::Partial(records)
            | broca_wal::WalRangeRecords::Unknown(records) => records,
            broca_wal::WalRangeRecords::Absent => &[],
        },
        Some(0),
        Some(10),
    )
    .expect("from-zero WAL replay");
    expected.lineage_id = full_range.lineage_id;
    zero_range_reads.store(0, Ordering::SeqCst);

    let actual = rig
        .registry
        .route_read(&key, Some(0), Some(10))
        .await
        .expect("legacy store-backed session.read");
    assert_eq!(actual, expected);
    assert_eq!(
        zero_range_reads.load(Ordering::SeqCst),
        0,
        "an unknown legacy store identity must not force a sequence-zero read"
    );
}

#[tokio::test]
async fn lineage_mismatch_replays_from_zero_and_repairs_the_store_once() {
    let (rig, zero_range_reads) = Rig::with_counting_zero_ranges("store-lineage-mismatch");
    let key = rig.key("recreated");
    append_seed(
        &rig.wal_store,
        &key,
        &[
            started(&key, "run-old", "old prompt"),
            assistant_text(0, "old answer"),
            WalRecord::RunFinished {
                ts_ms: None,
                reason: RunFinishReason::Completed,
                usage: usage(),
                error: None,
                retries_used: 0,
            },
        ],
    );
    let old_range = rig
        .wal_store
        .read_records_range(&key, 0, None, None)
        .expect("read old WAL range");
    let old_lineage_id = old_range.lineage_id.expect("old WAL lineage id");
    Persister::project_range(rig.store.as_ref(), &key, &old_range)
        .expect("materialize old lineage");
    assert_eq!(
        rig.store
            .read_page_with_watermark(&key, Some(0), 10, broca_core::context::ReadView::Raw)
            .expect("read old materialized page")
            .lineage_id
            .as_deref(),
        Some(old_lineage_id.to_string().as_str())
    );

    let wal_path = rig.wal_store.wal_path(&key);
    std::fs::rename(&wal_path, wal_path.with_extension("retired"))
        .expect("retire old WAL at the same key");
    append_seed(
        &rig.wal_store,
        &key,
        &[
            started(&key, "run-new", "new prompt"),
            assistant_text(0, "new answer"),
            WalRecord::RunFinished {
                ts_ms: None,
                reason: RunFinishReason::Completed,
                usage: usage(),
                error: None,
                retries_used: 0,
            },
        ],
    );
    let new_range = rig
        .wal_store
        .read_records_range(&key, 0, None, None)
        .expect("read replacement WAL range");
    let new_lineage_id = new_range.lineage_id.expect("replacement WAL lineage id");
    assert_ne!(new_lineage_id, old_lineage_id);
    let mut expected = broca_core::read_session_page(
        &broca_core::StorePageSnapshot::empty(),
        match new_range.records() {
            broca_wal::WalRangeRecords::Complete(records)
            | broca_wal::WalRangeRecords::Partial(records)
            | broca_wal::WalRangeRecords::Unknown(records) => records,
            broca_wal::WalRangeRecords::Absent => &[],
        },
        Some(0),
        Some(10),
    )
    .expect("from-zero replacement replay");
    expected.lineage_id = Some(new_lineage_id);
    zero_range_reads.store(0, Ordering::SeqCst);

    let first = rig
        .registry
        .route_read(&key, Some(0), Some(10))
        .await
        .expect("read recreated lineage");
    assert_eq!(
        first, expected,
        "a recreated WAL must never splice in the previous lineage's store rows"
    );
    let first_full_reads = zero_range_reads.load(Ordering::SeqCst);
    assert!(
        first_full_reads > 0,
        "the mismatch arm must execute a sequence-zero WAL read"
    );
    let repaired = rig
        .store
        .read_page_with_watermark(&key, Some(0), 10, broca_core::context::ReadView::Raw)
        .expect("read repaired materialized page");
    assert_eq!(
        repaired.lineage_id.as_deref(),
        Some(new_lineage_id.to_string().as_str())
    );
    assert_eq!(repaired.watermark_seq, 2);

    let second = rig
        .registry
        .route_read(&key, Some(0), Some(10))
        .await
        .expect("read repaired lineage");
    assert_eq!(second, expected);
    assert_eq!(
        zero_range_reads.load(Ordering::SeqCst),
        first_full_reads,
        "the repaired store must prevent a second sequence-zero replay"
    );
}

/// Captures a session.read envelope whose last run ended on a PROVIDER ERROR, so the
/// `lineage_state.error` block is populated rather than omitted.
///
/// Exists because the field's shape was documented and exercised by zero captured bytes:
/// every fixture a consumer held showed `state: "completed"` with no error key, so both
/// their decode and this producer's claim about the shape rested on belief. Production has
/// error terminals in abundance, but every one is a real user or peer session and those
/// bytes are not publishable for a fixture — a synthetic session through the REAL encoder
/// pins the shape without borrowing anyone's content.
#[tokio::test]
async fn error_terminal_read_envelope_populates_the_lineage_error_block() {
    let rig = Rig::new("lineage-error-capture", Arc::new(NoopExec));
    let key = rig.key("errored");
    append_seed(
        &rig.wal_store,
        &key,
        &[
            started(&key, "run-errored", "hello"),
            assistant_text(0, "partial answer"),
            WalRecord::RunFinished {
                ts_ms: None,
                reason: RunFinishReason::Error,
                usage: usage(),
                // retry_after_secs is set deliberately. It is an Option and is OMITTED when
                // absent, so a capture without it would pin a shape missing the one field a
                // routing consumer acts on — it is what a router cools a provider for.
                error: Some(ProviderError {
                    retry_after_secs: Some(30.0),
                    ..error_value("provider rate limited the request")
                }),
                retries_used: 3,
            },
        ],
    );
    let framed = rig.wal_store.read_records(&key).expect("read errored WAL");
    rig.store
        .project(&key, &framed)
        .expect("materialize the errored session");

    let page = rig
        .registry
        .read(read_request(&key, Some(0), Some(10)))
        .await
        .expect("session.read of an errored lineage");

    assert!(
        page.lineage_state.error.is_some(),
        "an error terminal must populate lineage_state.error"
    );
    assert_eq!(page.lineage_state.state, "error");

    let captured = serde_json::to_string(&page).expect("serialize captured error envelope");

    // Assert on the SERIALIZED bytes rather than the struct: SharedProviderError is an
    // opaque newtype with no accessor (deliberately — that is what stops a caller-facing
    // schema holding a raw ProviderError), so the wire form is both the only readable
    // surface here and the exact thing a consumer decodes.
    let class_is_ratified = [
        "\"class\":\"transient\"",
        "\"class\":\"permanent\"",
        "\"class\":\"auth_required\"",
        "\"class\":\"context_overflow\"",
    ]
    .iter()
    .any(|spelling| captured.contains(spelling));
    assert!(
        class_is_ratified,
        "captured envelope must carry a ratified class spelling: {captured}"
    );

    println!("CAPTURED_ERROR_READ_ENVELOPE={captured}");
}

/// Read one run's cause through both caller surfaces and return the serialized pieces:
/// `session.read`'s `lineage_state`, and the whole `run.status` response. Serialized JSON is
/// compared because `SharedProviderError` is what a consumer decodes, and its serialization
/// (not the raw struct) is the contract both surfaces must agree on.
async fn read_and_status(
    rig: &Rig,
    key: &BindIdentity,
    run: &str,
) -> (serde_json::Value, serde_json::Value) {
    let page = rig
        .registry
        .read(read_request(key, Some(0), Some(10)))
        .await
        .expect("session.read resolves");
    assert_eq!(page.lineage_state.last_run_id.as_deref(), Some(run));
    let lineage = serde_json::to_value(&page.lineage_state).expect("serialize lineage state");
    let status = rig
        .registry
        .run_status(key, RunId(run.into()))
        .await
        .expect("run.status resolves");
    let status = serde_json::to_value(&status).expect("serialize run status");
    (lineage, status)
}

fn errored_run(key: &BindIdentity, run: &str, error: Option<ProviderError>) -> Vec<WalRecord> {
    vec![
        started(key, run, "hello"),
        assistant_text(0, "partial answer"),
        WalRecord::RunFinished {
            ts_ms: None,
            reason: RunFinishReason::Error,
            usage: usage(),
            error,
            retries_used: 3,
        },
    ]
}

fn rate_limited() -> ProviderError {
    ProviderError {
        refusal: None,
        class: ErrorClass::Transient,
        retry_after_secs: Some(30.0),
        ..error_value("provider rate limited the request")
    }
}

/// `session.read` already reports the typed terminal error; `run.status` for the same run must
/// report the identical value. The run has no index row, so status reconciles from the WAL.
#[tokio::test]
async fn run_status_error_matches_session_read_error() {
    let rig = Rig::new("status-error-cause", Arc::new(NoopExec));
    let key = rig.key("errored");
    append_seed(
        &rig.wal_store,
        &key,
        &errored_run(&key, "run-errored", Some(rate_limited())),
    );

    let (lineage, status) = read_and_status(&rig, &key, "run-errored").await;
    assert_eq!(lineage["state"], "error");
    assert_eq!(status["state"], "error");
    assert!(
        lineage.get("error").is_some(),
        "the oracle must itself report an error: {lineage}"
    );
    assert_eq!(
        status.get("error"),
        lineage.get("error"),
        "run.status must report the error session.read reports"
    );
}

/// An account-limit terminal reaches `run.status` and `session.read` with its reset time, usage
/// window and account under these exact keys, so a router can block that one account until it
/// resets instead of blocking the whole model route.
#[tokio::test]
async fn run_status_reports_an_exhausted_account_with_its_reset_window_and_account() {
    let rig = Rig::new("status-account-limit", Arc::new(NoopExec));
    let key = rig.key("account-limit");
    let exhausted = ProviderError {
        refusal: None,
        class: ErrorClass::Transient,
        message: "429: account limit".into(),
        status: Some(429),
        retry_after_secs: Some(352_835.0),
        provider_code: Some(broca_protocol::error::ACCOUNT_LIMIT_EXHAUSTED.into()),
        resets_at_ms: Some(1_790_740_800_000),
        resets_at_source: Some("header".into()),
        limit_window: Some("seven_day".into()),
        account_ref: Some("d4bc9dbc".into()),
        credential_id: None,
    };
    append_seed(
        &rig.wal_store,
        &key,
        &errored_run(&key, "run-exhausted", Some(exhausted)),
    );

    let (lineage, status) = read_and_status(&rig, &key, "run-exhausted").await;
    assert_eq!(status["state"], "error");
    assert_eq!(
        serde_json::to_string(&status["error"]).unwrap(),
        r#"{"account_ref":"d4bc9dbc","class":"transient","limit_window":"seven_day","message":"429: account limit","provider_code":"account_limit_exhausted","resets_at_ms":1790740800000,"resets_at_source":"header","retry_after_secs":352835.0,"status":429}"#,
        "run.status error (keys sorted by the JSON value map)"
    );
    assert_eq!(status.get("error"), lineage.get("error"));
}

/// The same agreement through the index fast path: a terminal `Error` row with usage used to
/// answer without reading the WAL, and the index stores no error.
#[tokio::test]
async fn indexed_error_run_status_matches_session_read_error() {
    let rig = Rig::new("status-error-cause-indexed", Arc::new(NoopExec));
    let key = rig.key("errored-indexed");
    append_seed(
        &rig.wal_store,
        &key,
        &errored_run(&key, "run-errored", Some(rate_limited())),
    );
    rig.store
        .index_run_active(&RunIndexRow {
            withdrawals_owed: false,
            title: None,
            run_id: "run-errored".into(),
            session: key.clone(),
            submission_id: None,
            request_identity_digest: None,
            state: RunIndexState::Active,
            usage: None,
            final_step_finish_reason: None,
            retries_used: 0,
        })
        .expect("index active run");
    rig.store
        .index_run_terminal(
            "run-errored",
            RunIndexState::Error,
            Some(usage()),
            Some(FinishReason::Stop),
            3,
        )
        .expect("index terminal metadata");

    let (lineage, status) = read_and_status(&rig, &key, "run-errored").await;
    assert!(lineage.get("error").is_some(), "oracle error: {lineage}");
    assert_eq!(status.get("error"), lineage.get("error"));
    // The index row's own fields are still what the response reports.
    assert_eq!(status["retries_used"], 3);
    assert_eq!(status["final_step_finish_reason"], "stop");
}

/// A pause reports its reason as its own field, spelled and valued as `session.read` reports
/// it, alongside the pause's typed error.
#[tokio::test]
async fn run_status_pause_cause_matches_session_read() {
    let rig = Rig::new("status-pause-cause", Arc::new(NoopExec));
    let key = rig.key("paused");
    append_seed(
        rig.wal_store.as_ref(),
        &key,
        &[
            started(&key, "run-paused", "auth"),
            WalRecord::RunPaused {
                ts_ms: None,
                run_id: RunId("run-paused".into()),
                reason: PauseReason::AuthRequired,
                error: Some(ProviderError {
                    refusal: None,
                    class: ErrorClass::Auth,
                    ..error_value("reauth")
                }),
            },
        ],
    );

    let (lineage, status) = read_and_status(&rig, &key, "run-paused").await;
    assert_eq!(lineage["state"], "paused");
    assert_eq!(status["state"], "paused");
    assert_eq!(lineage["reason"], "auth_required");
    assert_eq!(
        status.get("reason"),
        lineage.get("reason"),
        "run.status must report the pause reason session.read reports"
    );
    assert!(lineage.get("error").is_some(), "oracle error: {lineage}");
    assert_eq!(status.get("error"), lineage.get("error"));
}

/// A run whose terminal carries no typed error reports no `error` key at all: not `null`, and
/// not a synthesized placeholder. Covered for an `Error` terminal with no recorded error, a
/// clean completion, and a pause with no recorded error.
#[tokio::test]
async fn run_status_without_a_cause_omits_the_cause_keys() {
    let rig = Rig::new("status-no-cause", Arc::new(NoopExec));

    let errored = rig.key("errored-bare");
    append_seed(
        &rig.wal_store,
        &errored,
        &errored_run(&errored, "run-bare", None),
    );
    let (lineage, status) = read_and_status(&rig, &errored, "run-bare").await;
    assert_eq!(status["state"], "error");
    assert!(lineage.get("error").is_none(), "oracle: {lineage}");
    assert!(
        status.as_object().unwrap().get("error").is_none(),
        "an error terminal without a recorded error must omit the key: {status}"
    );

    let completed = rig.key("completed");
    append_seed(
        &rig.wal_store,
        &completed,
        &[
            started(&completed, "run-done", "hello"),
            assistant_text(0, "answer"),
            WalRecord::RunFinished {
                ts_ms: None,
                reason: RunFinishReason::Completed,
                usage: usage(),
                error: None,
                retries_used: 0,
            },
        ],
    );
    let (_, status) = read_and_status(&rig, &completed, "run-done").await;
    assert_eq!(status["state"], "completed");
    let fields = status.as_object().unwrap();
    assert!(fields.get("error").is_none(), "{status}");
    assert!(fields.get("reason").is_none(), "{status}");

    let paused = rig.key("paused-bare");
    append_seed(
        &rig.wal_store,
        &paused,
        &[
            started(&paused, "run-paused-bare", "auth"),
            WalRecord::RunPaused {
                ts_ms: None,
                run_id: RunId("run-paused-bare".into()),
                reason: PauseReason::AuthRequired,
                error: None,
            },
        ],
    );
    let (lineage, status) = read_and_status(&rig, &paused, "run-paused-bare").await;
    assert_eq!(status["state"], "paused");
    assert!(lineage.get("error").is_none(), "oracle: {lineage}");
    assert!(
        status.as_object().unwrap().get("error").is_none(),
        "a pause without a recorded error must omit the key: {status}"
    );
}

#[tokio::test]
async fn legacy_wal_without_lineage_id_remains_readable_and_absent() {
    let rig = Rig::new("legacy-lineage-id-absent", Arc::new(NoopExec));
    let key = rig.key("legacy-readable");
    write_legacy_seed(
        &rig.wal_store,
        &key,
        &[
            started(&key, "run-legacy", "legacy prompt"),
            assistant_text(0, "legacy answer"),
        ],
    );

    let page = rig
        .registry
        .read(read_request(&key, Some(0), Some(10)))
        .await
        .expect("read legacy WAL");
    assert_eq!(page.lineage_id, None);
    assert_eq!(page.messages.len(), 2, "legacy transcript remains readable");

    append_seed(
        &rig.wal_store,
        &key,
        &[assistant_text(1, "first post-upgrade write")],
    );
    let after_write = rig
        .registry
        .read(read_request(&key, Some(0), Some(10)))
        .await
        .expect("read legacy WAL after its next write");
    assert!(
        after_write.lineage_id.is_some(),
        "the next write backfills the durable identity"
    );
    assert_eq!(after_write.messages.len(), 3);
}

#[tokio::test]
async fn lineage_id_survives_writer_drop_and_store_reconstruction() {
    let rig = Rig::new("lineage-id-restart", Arc::new(NoopExec));
    let key = rig.key("restart");
    append_seed(
        &rig.wal_store,
        &key,
        &[started(&key, "run-restart", "persist me")],
    );
    let before = stored_lineage_id(&rig.wal_store, &key).expect("created WAL lineage id");

    let restarted_store = FileWalStore::new(rig.dir.join("wal"));
    let after = stored_lineage_id(&restarted_store, &key).expect("reloaded WAL lineage id");
    assert_eq!(after, before, "lineage id comes from durable WAL bytes");
}

#[tokio::test]
async fn moved_rekey_preserves_lineage_id() {
    let rig = Rig::new("lineage-id-rekey", Arc::new(NoopExec));
    let old_root = rig.dir.join("project-before-rename");
    let new_root = rig.dir.join("project-after-rename");
    std::fs::create_dir_all(&old_root).expect("create old project root");
    let old_key = BindIdentity::new(&old_root, "runner", "rekeyed");
    let new_key = BindIdentity::new(&new_root, "runner", "rekeyed");
    append_seed(
        &rig.wal_store,
        &old_key,
        &[started(&old_key, "run-rekey", "before rename")],
    );
    let before = stored_lineage_id(&rig.wal_store, &old_key).expect("old WAL lineage id");

    std::fs::rename(&old_root, &new_root).expect("rename project root");
    std::fs::rename(
        rig.wal_store.wal_path(&old_key),
        rig.wal_store.wal_path(&new_key),
    )
    .expect("move WAL to its re-keyed filename");

    let page = rig
        .registry
        .read(read_request(&new_key, Some(0), Some(10)))
        .await
        .expect("read moved WAL under re-keyed identity");
    assert_eq!(page.lineage_id.as_deref(), Some(before.as_str()));
    assert_eq!(
        page.messages.len(),
        1,
        "the moved transcript stays reachable"
    );
}

#[tokio::test]
async fn recreated_lineage_at_the_same_key_mints_a_different_id() {
    let rig = Rig::new("lineage-id-recreated", Arc::new(NoopExec));
    let key = rig.key("recreated");
    append_seed(
        &rig.wal_store,
        &key,
        &[started(&key, "run-old", "old lineage")],
    );
    let old_id = stored_lineage_id(&rig.wal_store, &key).expect("old lineage id");
    let wal_path = rig.wal_store.wal_path(&key);
    std::fs::rename(&wal_path, wal_path.with_extension("retired"))
        .expect("move old lineage away from the key");

    append_seed(
        &rig.wal_store,
        &key,
        &[started(&key, "run-new", "new lineage")],
    );
    let new_id = stored_lineage_id(&rig.wal_store, &key).expect("new lineage id");
    assert_ne!(new_id, old_id, "WAL creation is a new identity boundary");
}

#[tokio::test]
async fn legacy_session_read_does_not_backfill_or_mutate_the_wal() {
    let rig = Rig::new("lineage-id-no-read-backfill", Arc::new(NoopExec));
    let key = rig.key("legacy-no-write");
    write_legacy_seed(
        &rig.wal_store,
        &key,
        &[started(&key, "run-no-backfill", "read only")],
    );
    let wal_path = rig.wal_store.wal_path(&key);
    let before = std::fs::metadata(&wal_path).expect("stat legacy WAL before read");
    std::thread::sleep(Duration::from_millis(1100));

    let page = rig
        .registry
        .read(read_request(&key, Some(0), Some(10)))
        .await
        .expect("read legacy WAL without backfill");
    let after = std::fs::metadata(&wal_path).expect("stat legacy WAL after read");

    assert_eq!(page.lineage_id, None);
    assert_eq!(
        after.len(),
        before.len(),
        "session.read must not append metadata"
    );
    assert_eq!(
        after.modified().expect("legacy WAL post-read mtime"),
        before.modified().expect("legacy WAL pre-read mtime"),
        "session.read must not touch the WAL mtime"
    );
}

#[tokio::test]
async fn idle_read_flushes_unproven_tail_once_then_reuses_the_proof() {
    let root = broca_test_support::scratch_root().join(format!(
        "broca-session-read-flush-cache-{}-{}",
        std::process::id(),
        unique()
    ));
    std::fs::create_dir_all(root.join("proj")).expect("create test project");
    let key = BindIdentity::new(root.join("proj"), "runner", "idle-flush");
    let file_store = Arc::new(FileWalStore::new(root.join("wal")));
    let mut wal = FileWal::open(file_store.wal_path(&key)).expect("open direct test WAL");
    wal.append(1, &started(&key, "run", "unproven user"), false)
        .expect("append flushed-but-unsynced start");
    wal.append(1, &assistant_text(0, "unproven assistant"), false)
        .expect("append flushed-but-unsynced assistant");

    let flushes = Arc::new(AtomicUsize::new(0));
    let reader = Arc::new(FlushCountingWalStore {
        inner: file_store.clone(),
        flushes: flushes.clone(),
        fail_flush: false,
    });
    let opener = Arc::new(DefaultOpener::new(
        Arc::new(FileLeaseStore::new(root.join("leases"))),
        file_store,
        Arc::new(NoopExec),
    ));
    let registry = LineageRegistry::new_with_reader(opener, reader.clone(), None);

    // The proven-head cache is scoped by stable file identity. Platforms without one
    // (Windows) disable the cache by design: every idle page crossing unproven frames
    // takes its own conservative flush, so the expected counts shift by one per read.
    let cache_enabled = reader
        .inner
        .file_identity(&key)
        .expect("probe WAL file identity")
        .is_some();

    let first = registry
        .route_read(&key, None, None)
        .await
        .expect("read after durability flush");
    assert_eq!(first.messages.len(), 2);
    assert_eq!(
        flushes.load(Ordering::SeqCst),
        1,
        "the first idle page crossing the unknown tail takes one flush-only barrier"
    );

    let second = registry
        .route_read(&key, Some(1), None)
        .await
        .expect("read inside cached durable head");
    assert_eq!(second.messages.len(), 1);
    assert_eq!(
        flushes.load(Ordering::SeqCst),
        if cache_enabled { 1 } else { 2 },
        "with an identity-matched proof the covered page performs no extra fsync; without identity support every unproven page reflushes"
    );

    wal.append(1, &assistant_text(1, "new unproven assistant"), false)
        .expect("append a tail beyond the cached proof");
    let third = registry
        .route_read(&key, None, None)
        .await
        .expect("read after the WAL grows past the cached proof");
    assert_eq!(third.messages.len(), 3);
    assert_eq!(
        flushes.load(Ordering::SeqCst),
        if cache_enabled { 2 } else { 3 },
        "a page that reaches beyond the cached proof takes exactly one new flush"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn idle_read_flush_failure_hides_the_unproven_tail() {
    let root = broca_test_support::scratch_root().join(format!(
        "broca-session-read-flush-failure-{}-{}",
        std::process::id(),
        unique()
    ));
    std::fs::create_dir_all(root.join("proj")).expect("create test project");
    let key = BindIdentity::new(root.join("proj"), "runner", "idle-flush-failure");
    let file_store = Arc::new(FileWalStore::new(root.join("wal")));
    let mut wal = FileWal::open(file_store.wal_path(&key)).expect("open direct test WAL");
    wal.append(1, &started(&key, "run", "must stay hidden"), false)
        .expect("append flushed-but-unsynced start");

    let flushes = Arc::new(AtomicUsize::new(0));
    let reader = Arc::new(FlushCountingWalStore {
        inner: file_store.clone(),
        flushes: flushes.clone(),
        fail_flush: true,
    });
    let opener = Arc::new(DefaultOpener::new(
        Arc::new(FileLeaseStore::new(root.join("leases"))),
        file_store,
        Arc::new(NoopExec),
    ));
    let registry = LineageRegistry::new_with_reader(opener, reader, None);

    let page = registry
        .route_read(&key, None, None)
        .await
        .expect("a failed proof falls back to the empty materialized prefix");
    assert!(
        page.messages.is_empty(),
        "unproven WAL frames must not be served"
    );
    assert_eq!(flushes.load(Ordering::SeqCst), 1);
    let _ = std::fs::remove_dir_all(root);
}

fn min_config() -> RunConfig {
    RunConfig {
        model: ModelHandle::new("stub", "stub-model"),
        tools: vec![tool("lookup")].into(),
        tool_choice: ToolChoice::Auto,
        generation: Default::default(),
        response_format: Default::default(),
        provider_options: serde_json::Value::Null,
        cache_tiers: serde_json::Value::Null,
        context_epoch: 0,
        renderer_version: 1,
        frozen_render_config: serde_json::Value::Null,
        auth_selection: None,
        tools_were_omitted: false,
        budget: None,
        context_limit: None,
        work_class: None,
        variant: None,
        service_tier: None,
        stop_when: Vec::new(),
        context_management: None,
    }
}

fn started(key: &BindIdentity, run: &str, text: &str) -> WalRecord {
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
        ts_ms: None,
        run_id: RunId(run.into()),
        session: key.clone(),
        config: Box::new(min_config()),
        input: vec![Message::new(
            Role::User,
            vec![ContentBlock::Text { text: text.into() }],
        )]
        .into(),
        origin: broca_wal::RunOrigin::Fresh,
        submission_id: None,
    }
}

fn assistant_text(step: u64, text: &str) -> WalRecord {
    assistant_text_with_usage(step, text, usage())
}

fn assistant_text_with_usage(step: u64, text: &str, step_usage: Usage) -> WalRecord {
    WalRecord::ModelStepFinished {
        hooks: None,
        step_id: StepId(step),
        assistant_message: (AssistantMessage {
            message_id: MessageId(format!("m-{step}")),
            content: vec![ContentBlock::Text { text: text.into() }],
        })
        .into(),
        usage: step_usage,
        finish_reason: Some(FinishReason::Stop),
        retries_used: 0,
        service_tier_used: None,
        speed_used: None,
    }
}

fn read_request(
    key: &BindIdentity,
    from_ordinal: Option<u64>,
    limit: Option<u64>,
) -> SessionReadRequest {
    SessionReadRequest {
        session: key.clone(),
        late_after: None,
        from_ordinal,
        limit,
        include_tools: false,
        after_mid: None,
        lineage_id: None,
        max_bytes: None,
    }
}

fn read_request_with_tools(
    key: &BindIdentity,
    from_ordinal: Option<u64>,
    limit: Option<u64>,
) -> SessionReadRequest {
    SessionReadRequest {
        include_tools: true,
        ..read_request(key, from_ordinal, limit)
    }
}

#[tokio::test]
async fn terminal_index_metadata_avoids_full_wal_read() {
    let (rig, full_reads) = Rig::with_counting_wal("terminal-index-fast-path");
    let key = rig.key("indexed");
    rig.store
        .index_run_active(&RunIndexRow {
            withdrawals_owed: false,
            title: None,
            run_id: "run-indexed".into(),
            session: key.clone(),
            submission_id: None,
            request_identity_digest: None,
            state: RunIndexState::Active,
            usage: None,
            final_step_finish_reason: None,
            retries_used: 0,
        })
        .expect("index active run");
    rig.store
        .index_run_terminal(
            "run-indexed",
            RunIndexState::Completed,
            Some(usage()),
            Some(FinishReason::Stop),
            0,
        )
        .expect("index terminal metadata");
    rig.registry
        .route_status(&key)
        .await
        .expect("open actor before measuring status lookup");
    full_reads.store(0, Ordering::SeqCst);

    let state = rig
        .registry
        .route_run_state(&key, RunId("run-indexed".into()))
        .await
        .expect("resolve indexed terminal")
        .expect("indexed terminal exists");
    assert_eq!(
        full_reads.load(Ordering::SeqCst),
        0,
        "a metadata-bearing terminal index hit must not call WalStore::read_records"
    );
    assert_eq!(state.state, RunIndexState::Completed);
    assert_eq!(state.usage, Some(usage()));
    assert_eq!(state.final_step_finish_reason, Some(FinishReason::Stop));
}

#[tokio::test]
async fn terminal_lineage_usage_requires_a_completed_model_step() {
    let no_step_rig = Rig::new("terminal-usage-no-step", Arc::new(NoopExec));
    let no_step_key = no_step_rig.key("no-step");
    append_seed(
        no_step_rig.wal_store.as_ref(),
        &no_step_key,
        &[
            started(&no_step_key, "run-no-step", "hello"),
            WalRecord::RunFinished {
                ts_ms: None,
                reason: RunFinishReason::Error,
                usage: Usage::default(),
                error: Some(error_value("provider failed")),
                retries_used: 0,
            },
        ],
    );
    let no_step_page = no_step_rig
        .registry
        .read(read_request(&no_step_key, None, None))
        .await
        .expect("read terminal lineage");
    assert_eq!(no_step_page.lineage_state.usage, None);
    let no_step_status = no_step_rig
        .registry
        .route_run_state(&no_step_key, RunId("run-no-step".into()))
        .await
        .expect("resolve no-step run")
        .expect("run exists");
    assert_eq!(no_step_status.usage, None);

    let zero_step_rig = Rig::new("terminal-usage-zero-step", Arc::new(NoopExec));
    let zero_step_key = zero_step_rig.key("zero-step");
    append_seed(
        zero_step_rig.wal_store.as_ref(),
        &zero_step_key,
        &[
            started(&zero_step_key, "run-zero-step", "hello"),
            assistant_text_with_usage(1, "done", Usage::default()),
            WalRecord::RunFinished {
                ts_ms: None,
                reason: RunFinishReason::Completed,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            },
        ],
    );
    let zero_step_page = zero_step_rig
        .registry
        .read(read_request(&zero_step_key, None, None))
        .await
        .expect("read terminal lineage");
    assert_eq!(zero_step_page.lineage_state.usage, Some(Usage::default()));
    let zero_step_status = zero_step_rig
        .registry
        .route_run_state(&zero_step_key, RunId("run-zero-step".into()))
        .await
        .expect("resolve zero-step run")
        .expect("run exists");
    assert_eq!(zero_step_status.usage, Some(Usage::default()));
}

#[tokio::test]
async fn foreign_session_cannot_read_terminal_index_metadata() {
    let rig = Rig::new("terminal-index-foreign-session", Arc::new(NoopExec));
    let owner = rig.key("owner");
    let foreign = rig.key("foreign");
    append_seed(
        rig.wal_store.as_ref(),
        &owner,
        &[
            started(&owner, "run-owned", "hello"),
            assistant_text(0, "done"),
            WalRecord::RunFinished {
                ts_ms: None,
                reason: RunFinishReason::Completed,
                usage: usage(),
                error: None,
                retries_used: 0,
            },
        ],
    );
    rig.store
        .index_run_active(&RunIndexRow {
            withdrawals_owed: false,
            title: None,
            run_id: "run-owned".into(),
            session: owner.clone(),
            submission_id: None,
            request_identity_digest: None,
            state: RunIndexState::Active,
            usage: None,
            final_step_finish_reason: None,
            retries_used: 0,
        })
        .expect("index active run");
    rig.store
        .index_run_terminal(
            "run-owned",
            RunIndexState::Completed,
            Some(usage()),
            Some(FinishReason::Stop),
            0,
        )
        .expect("index terminal metadata");

    let state = rig
        .registry
        .route_run_state(&foreign, RunId("run-owned".into()))
        .await
        .expect("foreign-session lookup succeeds");
    assert_eq!(
        state, None,
        "a globally indexed terminal must remain private to its owning session"
    );
}

#[tokio::test]
async fn legacy_terminal_index_row_reconciles_metadata_from_wal() {
    let (rig, full_reads) = Rig::with_counting_wal("terminal-index-legacy-fallback");
    let key = rig.key("legacy");
    append_seed(
        rig.wal_store.as_ref(),
        &key,
        &[
            started(&key, "run-legacy", "hello"),
            assistant_text(0, "done"),
            WalRecord::RunFinished {
                ts_ms: None,
                reason: RunFinishReason::Completed,
                usage: usage(),
                error: None,
                retries_used: 0,
            },
        ],
    );
    rig.store
        .index_run_active(&RunIndexRow {
            withdrawals_owed: false,
            title: None,
            run_id: "run-legacy".into(),
            session: key.clone(),
            submission_id: None,
            request_identity_digest: None,
            state: RunIndexState::Active,
            usage: None,
            final_step_finish_reason: None,
            retries_used: 0,
        })
        .expect("index active run");
    rig.store
        .index_run_terminal("run-legacy", RunIndexState::Completed, None, None, 0)
        .expect("index legacy terminal");
    rig.registry
        .route_status(&key)
        .await
        .expect("open actor before measuring status lookup");
    full_reads.store(0, Ordering::SeqCst);

    let state = rig
        .registry
        .route_run_state(&key, RunId("run-legacy".into()))
        .await
        .expect("resolve legacy terminal")
        .expect("legacy terminal exists");
    assert_eq!(state.state, RunIndexState::Completed);
    assert_eq!(state.usage, Some(usage()));
    assert_eq!(state.final_step_finish_reason, Some(FinishReason::Stop));
    assert_eq!(
        full_reads.load(Ordering::SeqCst),
        0,
        "legacy terminal reconciliation reads the WAL through the actor's proven-head range"
    );
}

#[tokio::test]
async fn foreign_session_cannot_read_legacy_terminal_index_state() {
    let rig = Rig::new("legacy-terminal-index-foreign-session", Arc::new(NoopExec));
    let owner = rig.key("owner");
    let foreign = rig.key("foreign");
    append_seed(
        rig.wal_store.as_ref(),
        &owner,
        &[
            started(&owner, "run-legacy-owned", "hello"),
            assistant_text(0, "done"),
            WalRecord::RunFinished {
                ts_ms: None,
                reason: RunFinishReason::Completed,
                usage: usage(),
                error: None,
                retries_used: 0,
            },
        ],
    );
    rig.store
        .index_run_active(&RunIndexRow {
            withdrawals_owed: false,
            title: None,
            run_id: "run-legacy-owned".into(),
            session: owner,
            submission_id: None,
            request_identity_digest: None,
            state: RunIndexState::Active,
            usage: None,
            final_step_finish_reason: None,
            retries_used: 0,
        })
        .expect("index active run");
    rig.store
        .index_run_terminal("run-legacy-owned", RunIndexState::Completed, None, None, 0)
        .expect("index legacy terminal");

    let state = rig
        .registry
        .route_run_state(&foreign, RunId("run-legacy-owned".into()))
        .await
        .expect("foreign-session legacy lookup succeeds");
    assert_eq!(
        state, None,
        "a metadata-less terminal row must not leak its state across sessions"
    );
}

#[tokio::test]
async fn imported_session_is_projected_before_the_ack() {
    let rig = Rig::new("import-projection", Arc::new(NoopExec));
    let key = rig.key("imported");
    let messages = vec![
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "imported user".into(),
            }],
        ),
        Message::new(
            Role::Assistant,
            vec![ContentBlock::Text {
                text: "imported assistant".into(),
            }],
        ),
    ];

    let import_request = SessionImportRequest {
        project_id: None,
        principal: None,
        session: key.clone(),
        run_id: RunId("import-run".into()),
        messages: messages.clone(),
        source_ref: "fixture:module-import".into(),
    };
    let response = rig
        .registry
        .import(import_request.clone())
        .await
        .expect("import session");
    assert!(!response.already_imported);

    let snapshot = rig
        .store
        .read_page_with_watermark(&key, Some(0), 10, broca_core::context::ReadView::Raw)
        .expect("read imported store rows after acknowledgement");
    assert_eq!(snapshot.prefix_len, messages.len() as u64);
    assert_eq!(snapshot.rows.len(), messages.len());
    assert!(snapshot.watermark_seq > 0);

    let retry = rig
        .registry
        .import(import_request)
        .await
        .expect("retry imported session");
    assert!(retry.already_imported);
    assert_eq!(
        rig.store
            .read_page_with_watermark(&key, Some(0), 10, broca_core::context::ReadView::Raw)
            .expect("read imported store rows after retry"),
        snapshot
    );

    let page = rig
        .registry
        .read(read_request(&key, Some(0), Some(10)))
        .await
        .expect("read imported page");
    assert_eq!(
        page.messages
            .into_iter()
            .map(|entry| entry.message)
            .collect::<Vec<_>>(),
        snapshot
            .rows
            .into_iter()
            .map(|row| row.message)
            .collect::<Vec<_>>()
    );
}

fn projected_prompt(records: &[broca_wal::FramedRecord]) -> Vec<Message> {
    let mut out = Vec::new();
    let mut pending_tools: Vec<ToolResult> = Vec::new();
    for (_, unit) in project_control(records) {
        match unit {
            ControlUnit::AssistantMessage { message } => {
                if !pending_tools.is_empty() {
                    out.push(Message::new(
                        Role::Tool,
                        pending_tools
                            .drain(..)
                            .map(ContentBlock::ToolResult)
                            .collect(),
                    ));
                }
                out.push(Message::new(Role::Assistant, message.content.clone()));
            }
            ControlUnit::ToolResult { result } => pending_tools.push(result),
            _ => {}
        }
    }
    if !pending_tools.is_empty() {
        out.push(Message::new(
            Role::Tool,
            pending_tools
                .into_iter()
                .map(ContentBlock::ToolResult)
                .collect(),
        ));
    }
    out
}

#[tokio::test]
async fn read_pages_match_replay_and_subscribe_projection_for_multi_episode_error_lineage() {
    let terminal_error = error_value("typed terminal");
    let rig = Rig::with_exec_factory("differential", |wal_store, store| {
        Arc::new(ScriptedCoreExec {
            wal_store,
            store,
            scripts: Mutex::new(VecDeque::from([
                vec![tool_step(), text_step("episode one done")],
                vec![error_step(terminal_error.clone())],
            ])),
        })
    });
    let key = rig.key("s1");

    send(&rig, &key, "run-1", "first").await;
    wait_status(&rig, &key, "run-1", "completed").await;
    send(&rig, &key, "run-2", "second").await;
    wait_status(&rig, &key, "run-2", "error").await;

    let mut page_from = Some(0);
    let mut read_messages = Vec::new();
    loop {
        let page = rig
            .registry
            .read(read_request(&key, page_from, Some(2)))
            .await
            .expect("read page");
        read_messages.extend(page.messages.into_iter().map(|m| m.message));
        if page.next_from_ordinal.is_none() {
            assert_eq!(page.lineage_state.state, "error");
            assert_eq!(page.lineage_state.reason.as_deref(), Some("error"));
            assert_eq!(
                page.lineage_state.error,
                Some(terminal_error.clone().into())
            );
            break;
        }
        page_from = page.next_from_ordinal;
    }

    let framed = rig.wal_store.read_records(&key).expect("read wal");
    let replay_prompt = replay(&framed.iter().map(|f| f.record.clone()).collect::<Vec<_>>())
        .expect("replay")
        .prompt;
    assert_eq!(
        read_messages, replay_prompt,
        "session.read equals replay prompt"
    );

    let projected = projected_prompt(&framed);
    let replay_projected_roles_content: Vec<_> = replay_prompt
        .iter()
        .filter(|m| matches!(m.role, Role::Assistant | Role::Tool))
        .map(|m| (m.role, serde_json::to_vec(&m.content).unwrap()))
        .collect();
    let projected_roles_content: Vec<_> = projected
        .iter()
        .map(|m| (m.role, serde_json::to_vec(&m.content).unwrap()))
        .collect();
    assert_eq!(projected_roles_content, replay_projected_roles_content);
}

/// Frame `records` into `key`'s WAL through the real framing, inserting before `insert_at` a
/// record type this build has no variant for, as a newer build would write it.
fn write_seed_with_newer_build_record(
    wal_store: &FileWalStore,
    key: &BindIdentity,
    records: &[WalRecord],
    insert_at: usize,
) {
    let path = wal_store.wal_path(key);
    std::fs::create_dir_all(path.parent().expect("WAL path has a parent"))
        .expect("create WAL directory");
    let newer = br#"{"type":"attempt_digest_from_a_newer_build","step_id":1,"attempt":1,"status":200,"usage":{"output_tokens":999}}"#;
    let mut bytes = Vec::new();
    let mut seq = 1;
    for (index, record) in records.iter().enumerate() {
        if index == insert_at {
            bytes.extend(broca_wal::framing::encode_payload(seq, 1, newer).expect("frame"));
            seq += 1;
        }
        bytes.extend(broca_wal::framing::encode(seq, 1, record).expect("frame"));
        seq += 1;
    }
    std::fs::write(path, bytes).expect("write WAL bytes");
}

async fn read_all_pages(rig: &Rig, key: &BindIdentity) -> (Vec<u8>, Vec<u8>) {
    let mut from = Some(0);
    let mut messages = Vec::new();
    loop {
        let page = rig
            .registry
            .read(read_request(key, from, Some(2)))
            .await
            .expect("read page");
        messages.extend(page.messages);
        if page.next_from_ordinal.is_none() {
            return (
                serde_json::to_vec(&messages).unwrap(),
                serde_json::to_vec(&page.lineage_state).unwrap(),
            );
        }
        from = page.next_from_ordinal;
    }
}

/// A record type from a newer build, in the middle of a real two-episode session, adds no
/// message to `session.read` and no unit to the control stream: both read byte-identically
/// to the same WAL without it.
#[tokio::test]
async fn session_read_and_control_projection_skip_a_record_type_from_a_newer_build() {
    let rig = Rig::with_exec_factory("newer-build-record", |wal_store, store| {
        Arc::new(ScriptedCoreExec {
            wal_store,
            store,
            scripts: Mutex::new(VecDeque::from([
                vec![tool_step(), text_step("episode one done")],
                vec![text_step("episode two done")],
            ])),
        })
    });
    let source = rig.key("source");
    send(&rig, &source, "run-1", "first").await;
    wait_status(&rig, &source, "run-1", "completed").await;
    send(&rig, &source, "run-2", "second").await;
    wait_status(&rig, &source, "run-2", "completed").await;
    let records: Vec<WalRecord> = rig
        .wal_store
        .read_records(&source)
        .expect("read source WAL")
        .into_iter()
        .map(|framed| framed.record)
        .collect();
    assert!(records.len() >= 8, "a non-trivial session");

    let plain = rig.key("plain");
    write_legacy_seed(&rig.wal_store, &plain, &records);
    let (plain_messages, plain_state) = read_all_pages(&rig, &plain).await;
    let plain_units: Vec<ControlUnit> =
        project_control(&rig.wal_store.read_records(&plain).expect("read plain WAL"))
            .into_iter()
            .map(|(_, unit)| unit)
            .collect();

    assert!(plain_messages.len() > 2, "the plain session reads messages");
    // Inside the first episode's tool turn, and at the boundary between the two episodes.
    let between_episodes = records
        .iter()
        .rposition(|record| matches!(record, WalRecord::RunStarted { .. }))
        .expect("second episode");
    for insert_at in [3, between_episodes] {
        let key = rig.key(&format!("with-newer-record-{insert_at}"));
        write_seed_with_newer_build_record(&rig.wal_store, &key, &records, insert_at);
        let framed = rig.wal_store.read_records(&key).expect("read WAL");
        assert_eq!(framed.len(), records.len() + 1);
        assert_eq!(framed[insert_at].record, WalRecord::Unknown);

        let (messages, state) = read_all_pages(&rig, &key).await;
        assert_eq!(
            messages, plain_messages,
            "session.read messages at {insert_at}"
        );
        assert_eq!(
            state, plain_state,
            "session.read lineage state at {insert_at}"
        );

        let units: Vec<ControlUnit> = project_control(&framed)
            .into_iter()
            .map(|(_, unit)| unit)
            .collect();
        assert_eq!(units, plain_units, "control units at {insert_at}");
    }
}

/// Provider attempt records, placed where the live loop writes them (failed and successful
/// attempts before each `ModelStepFinished`), add no message to `session.read` and no unit to
/// the control stream: both read byte-identically to the same WAL without them.
#[tokio::test]
async fn session_read_and_control_projection_skip_provider_attempt_records() {
    let rig = Rig::with_exec_factory("attempt-records", |wal_store, store| {
        Arc::new(ScriptedCoreExec {
            wal_store,
            store,
            scripts: Mutex::new(VecDeque::from([
                vec![tool_step(), text_step("episode one done")],
                vec![text_step("episode two done")],
            ])),
        })
    });
    let source = rig.key("source");
    send(&rig, &source, "run-1", "first").await;
    wait_status(&rig, &source, "run-1", "completed").await;
    send(&rig, &source, "run-2", "second").await;
    wait_status(&rig, &source, "run-2", "completed").await;
    let records: Vec<WalRecord> = rig
        .wal_store
        .read_records(&source)
        .expect("read source WAL")
        .into_iter()
        .map(|framed| framed.record)
        .collect();

    let plain = rig.key("plain");
    write_legacy_seed(&rig.wal_store, &plain, &records);
    let (plain_messages, plain_state) = read_all_pages(&rig, &plain).await;
    let plain_units: Vec<ControlUnit> =
        project_control(&rig.wal_store.read_records(&plain).expect("read plain WAL"))
            .into_iter()
            .map(|(_, unit)| unit)
            .collect();
    assert!(plain_messages.len() > 2, "the plain session reads messages");

    let mut with_attempts = Vec::new();
    for record in &records {
        if let WalRecord::ModelStepFinished { step_id, .. } = record {
            for (attempt, outcome) in [
                (0, broca_protocol::AttemptOutcome::HttpError),
                (1, broca_protocol::AttemptOutcome::Complete),
            ] {
                with_attempts.push(WalRecord::ModelAttemptFinished {
                    content_progress: Default::default(),
                    step_id: *step_id,
                    attempt,
                    request_body_len: 10,
                    request_body_sha256: "ab".repeat(32),
                    http_status: Some(200),
                    content_type: Some("text/event-stream".into()),
                    response_body_len: 20,
                    response_body_sha256: "cd".repeat(32),
                    response_body_cap: None,
                    outcome,
                    ts_ms: 1,
                    sent_at_ms: None,
                    transport: None,
                    websocket_request: None,
                    websocket_close_code: None,
                    input_transformations: None,
                    input_transformations_unmeasured: false,
                    credential_refresh: None,
                    served_account_ref: None,
                });
            }
        }
        with_attempts.push(record.clone());
    }
    assert!(
        with_attempts.len() >= records.len() + 6,
        "three steps carry records"
    );

    let key = rig.key("with-attempt-records");
    write_legacy_seed(&rig.wal_store, &key, &with_attempts);
    let (messages, state) = read_all_pages(&rig, &key).await;
    assert_eq!(messages, plain_messages, "session.read messages");
    assert_eq!(state, plain_state, "session.read lineage state");
    let units: Vec<ControlUnit> =
        project_control(&rig.wal_store.read_records(&key).expect("read WAL"))
            .into_iter()
            .map(|(_, unit)| unit)
            .collect();
    assert_eq!(units, plain_units, "control units");
}

#[tokio::test]
async fn one_shot_run_skips_store_and_session_read_replays_the_wal() {
    let rig = Rig::with_exec_factory("one-shot-read", |wal_store, store| {
        Arc::new(ScriptedCoreExec {
            wal_store,
            store,
            scripts: Mutex::new(VecDeque::from([vec![text_step("one-shot answer")]])),
        })
    });
    let key = rig.key("one-shot");

    send_with_materialization(&rig, &key, "run-one-shot", "one-shot prompt", true).await;
    wait_status(&rig, &key, "run-one-shot", "completed").await;

    let prefix = rig
        .store
        .read_prefix_with_watermark(&key)
        .expect("inspect materialized store");
    assert_eq!(prefix.watermark_seq, 0, "one-shot run writes no store row");
    assert!(
        prefix.prefix.is_empty(),
        "one-shot transcript is not stored"
    );

    let page = rig
        .registry
        .read(read_request(&key, Some(0), Some(100)))
        .await
        .expect("session.read falls back to the WAL");
    let read_messages: Vec<_> = page
        .messages
        .into_iter()
        .map(|message| message.message)
        .collect();
    let framed = rig.wal_store.read_records(&key).expect("read WAL");
    let replayed = replay(
        &framed
            .iter()
            .map(|framed| framed.record.clone())
            .collect::<Vec<_>>(),
    )
    .expect("replay one-shot WAL")
    .prompt;
    assert_eq!(read_messages, replayed);
    assert_eq!(page.lineage_state.state, "completed");
}

/// `session.read` on a session whose WAL a newer build appended to fails with an error that
/// names NewerWriter, never with a page and never as corruption. The read goes through the
/// store's anchor, which the completed run left behind.
#[tokio::test]
async fn session_read_refuses_a_newer_writer_wal_by_name() {
    let rig = Rig::with_exec_factory("newer-writer-read", |wal_store, store| {
        Arc::new(ScriptedCoreExec {
            wal_store,
            store,
            scripts: Mutex::new(VecDeque::from([vec![text_step("answer")]])),
        })
    });
    let key = rig.key("newer-writer");
    send(&rig, &key, "run-1", "prompt").await;
    wait_status(&rig, &key, "run-1", "completed").await;
    rig.registry
        .read(read_request(&key, Some(0), Some(100)))
        .await
        .expect("the session reads before a newer build touches it");

    let path = rig.wal_store.wal_path(&key);
    let next = rig.wal_store.read_records(&key).expect("read WAL").len() as u64 + 1;
    let mut bytes = std::fs::read(&path).unwrap();
    bytes.extend(
        broca_wal::framing::encode_with_requires(
            next,
            1,
            &["from-the-future/v1"],
            &WalRecord::StepStarted {
                step_id: StepId(next),
            },
        )
        .unwrap(),
    );
    std::fs::write(&path, bytes).unwrap();

    let error = rig
        .registry
        .read(read_request(&key, Some(0), Some(100)))
        .await
        .expect_err("a WAL only a newer build can read must not produce a page");
    let message = error.to_string();
    assert!(message.contains("NewerWriter"), "{message}");
    assert!(message.contains("from-the-future/v1"), "{message}");
    assert!(!message.contains("corruption"), "{message}");
    // The refusal stays typed, so the wire adapter answers it with its own code instead of
    // re-parsing this message.
    match error {
        broca_module::serve::ServeError::WalNewerWriter { missing, .. } => {
            assert_eq!(missing, vec!["from-the-future/v1".to_string()]);
        }
        other => panic!("expected a typed newer-writer refusal, got {other:?}"),
    }
}

/// A session whose `RunStarted` records froze a scope (gated frames requiring `scope/v1`) is
/// read like any other: projection materializes its first episode, and `session.read` pages
/// both episodes through the store's anchor, never refusing the gated frames.
#[tokio::test]
async fn session_read_and_projection_decode_a_scoped_session() {
    check_scoped_session_reads(false).await;
}

#[tokio::test]
async fn agent_run_scope_reads_status_projection_and_refuses_unstamped_advancement() {
    check_scoped_session_reads(true).await;
}

async fn check_scoped_session_reads(agent_run: bool) {
    let rig = Rig::new(
        if agent_run {
            "agent-run-session-read"
        } else {
            "scoped-session-read"
        },
        Arc::new(NoopExec),
    );
    let key = rig.key("scoped");
    let scoped = |run: &str, text: &str| {
        let mut record = started(&key, run, text);
        if let WalRecord::RunStarted {
            scope, principal, ..
        } = &mut record
        {
            let owner = broca_protocol::BindPrincipal::Reserved {
                module_id: "prefrontal-core".into(),
            };
            let sender = agent_run.then(|| broca_protocol::BindPrincipal::Reserved {
                module_id: "basal".into(),
            });
            *principal = sender.clone().or_else(|| Some(owner.clone()));
            *scope = Some(broca_protocol::SessionScope {
                owner,
                scope_ref: "head-1".into(),
                scope_epoch: 3,
                flow_id: None,
                sender,
                agent_id: agent_run.then(|| "agent-1".into()),
                run_id: agent_run.then(|| "parent-run".into()),
            });
        }
        record
    };
    let finished = || WalRecord::RunFinished {
        ts_ms: None,
        reason: RunFinishReason::Completed,
        usage: usage(),
        error: None,
        retries_used: 0,
    };
    append_seed(
        &rig.wal_store,
        &key,
        &[
            scoped("first-run", "first question"),
            assistant_text(0, "first answer"),
            finished(),
        ],
    );
    let first_frames = rig
        .wal_store
        .read_records(&key)
        .expect("read the scoped first episode");
    rig.store
        .project(&key, &first_frames)
        .expect("materialize the scoped first episode");
    append_seed(
        &rig.wal_store,
        &key,
        &[
            scoped("second-run", "second question"),
            assistant_text(0, "second answer"),
            finished(),
        ],
    );

    let page = rig
        .registry
        .read(read_request(&key, Some(0), Some(100)))
        .await
        .expect("a scoped session reads");
    let texts: Vec<String> = page
        .messages
        .iter()
        .flat_map(|m| m.message.content.iter())
        .filter_map(|block| match block {
            ContentBlock::Text { text } => Some(text.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        texts,
        [
            "first question",
            "first answer",
            "second question",
            "second answer"
        ]
    );
    assert_eq!(page.lineage_state.state, "completed");
    let (_, status) = read_and_status(&rig, &key, "second-run").await;
    assert_eq!(status["state"], "completed");
    if agent_run {
        let checkpoint = broca_core::AdmissionCheckpointV1::from_frames(&first_frames).unwrap();
        let scope = checkpoint.ownership.unwrap().scope.unwrap();
        assert_eq!(scope.agent_id.as_deref(), Some("agent-1"));
        assert_eq!(scope.run_id.as_deref(), Some("parent-run"));
        let before = rig.wal_store.read_records(&key).unwrap();
        let request = req(&key, "next-run", "not written");
        let response = rig
            .registry
            .send(broca_module::serve::SendRequest {
                session_class: None,
                retention: None,
                stop_sequences_named: false,
                title: None,
                conversation_key: None,
                delivery: Default::default(),
                session: key.clone(),
                run_id: request.run_id.clone(),
                send_id: Some("not-written".into()),
                request: Box::new({
                    let mut submission = request.to_submission();
                    submission.scope = broca_protocol::SendScope::Unstamped;
                    submission
                }),
                skip_materialization: false,
                kind: RunKind::FreshOrResume,
                plan: None,
            })
            .await;
        assert!(
            matches!(
                response,
                Err(broca_module::serve::ServeError::ScopeRefused(
                    broca_core::ScopeRefusal::RunMismatch { .. }
                ))
            ),
            "{response:?}"
        );
        assert_eq!(rig.wal_store.read_records(&key).unwrap(), before);
    }
}

/// A session whose `RunStarted` records froze a plan manifest (gated frames requiring
/// `plan-manifest/v1`) is read like any other: projection materializes its first episode, and
/// `session.read` pages both episodes through the store's anchor, never refusing the gated
/// frames, and the WAL still holds the manifest afterwards.
#[tokio::test]
async fn session_read_and_projection_decode_a_manifest_session() {
    let rig = Rig::new("manifest-session-read", Arc::new(NoopExec));
    let key = rig.key("manifest");
    let fixture: WalRecord = serde_json::from_str(
        &include_str!("../../../broca-wal/tests/fixtures/plan_manifest_run_started.json")
            .trim_end()
            .replace("{RUN_ID}", "fixture"),
    )
    .unwrap();
    let WalRecord::RunStarted {
        manifest: frozen, ..
    } = fixture
    else {
        unreachable!()
    };
    let frozen = frozen.expect("the fixture carries a manifest");
    let with_manifest = |run: &str, text: &str| {
        let mut record = started(&key, run, text);
        if let WalRecord::RunStarted { manifest, .. } = &mut record {
            *manifest = Some(frozen.clone());
        }
        record
    };
    let finished = || WalRecord::RunFinished {
        ts_ms: None,
        reason: RunFinishReason::Completed,
        usage: usage(),
        error: None,
        retries_used: 0,
    };
    append_seed(
        &rig.wal_store,
        &key,
        &[
            with_manifest("first-run", "first question"),
            assistant_text(0, "first answer"),
            finished(),
        ],
    );
    let first_frames = rig
        .wal_store
        .read_records(&key)
        .expect("read the first episode");
    rig.store
        .project(&key, &first_frames)
        .expect("materialize the first episode");
    append_seed(
        &rig.wal_store,
        &key,
        &[
            with_manifest("second-run", "second question"),
            assistant_text(0, "second answer"),
            finished(),
        ],
    );

    let page = rig
        .registry
        .read(read_request(&key, Some(0), Some(100)))
        .await
        .expect("a manifest session reads");
    let texts: Vec<String> = page
        .messages
        .iter()
        .flat_map(|m| m.message.content.iter())
        .filter_map(|block| match block {
            ContentBlock::Text { text } => Some(text.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        texts,
        [
            "first question",
            "first answer",
            "second question",
            "second answer"
        ]
    );
    assert_eq!(page.lineage_state.state, "completed");
    let manifests: Vec<_> = rig
        .wal_store
        .read_records(&key)
        .unwrap()
        .into_iter()
        .filter_map(|framed| match framed.record {
            WalRecord::RunStarted { manifest, .. } => manifest,
            _ => None,
        })
        .collect();
    assert_eq!(manifests, vec![frozen.clone(), frozen]);
}

/// The records of one completed run whose single tool call was dispatched with an intent naming
/// its module and schema (a gated frame requiring `dispatch-module/v1`).
fn dispatch_module_episode(key: &BindIdentity, run: &str, call: &str) -> Vec<WalRecord> {
    let batch_id = BatchId(format!("b-{run}"));
    let call_id = ToolCallId(call.into());
    let intent: WalRecord = serde_json::from_value(serde_json::json!({
        "type": "tool_dispatch_intent",
        "batch_id": batch_id.0,
        "tool_call_id": call,
        "tool_name": "lookup",
        "args": {},
        "module_id": "aft",
        "schema_pin": {"schema_digest": "sha256:5f2c", "semantics": 2},
    }))
    .unwrap();
    vec![
        started(key, run, &format!("{run} question")),
        WalRecord::ModelStepFinished {
            hooks: None,
            step_id: StepId(0),
            assistant_message: (AssistantMessage {
                message_id: MessageId(format!("m-{run}")),
                content: vec![ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: call_id.clone(),
                    tool_name: "lookup".into(),
                    input: serde_json::json!({}),
                })],
            })
            .into(),
            usage: usage(),
            finish_reason: Some(FinishReason::ToolCalls),
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        },
        WalRecord::ToolBatchStarted {
            batch_id: batch_id.clone(),
            step_id: StepId(0),
            planned_calls: vec![call_id.clone()],
        },
        intent,
        WalRecord::ToolResult {
            hooks: None,
            batch_id: batch_id.clone(),
            result: ToolResult {
                tool_call_id: call_id,
                output: ToolOutput::Text { text: "ok".into() },
                is_error: false,
            }
            .into(),
            outcome: DispatchOutcome::Completed,
            reason: None,
            withdraw_elicitation: None,
            provider_held: None,
        },
        WalRecord::ToolBatchFinished { batch_id },
        WalRecord::TurnFinished { step_id: StepId(0) },
        assistant_text(1, &format!("{run} answer")),
        WalRecord::RunFinished {
            ts_ms: None,
            reason: RunFinishReason::Completed,
            usage: usage(),
            error: None,
            retries_used: 0,
        },
    ]
}

/// A session whose tool dispatch intents name their module (gated frames requiring
/// `dispatch-module/v1`) is read like any other: projection materializes its first episode, and
/// `session.read` pages both episodes through the store's anchor, never refusing the gated
/// frames, and the WAL still holds every module afterwards.
#[tokio::test]
async fn session_read_and_projection_decode_a_dispatch_module_session() {
    let rig = Rig::new("dispatch-module-session-read", Arc::new(NoopExec));
    let key = rig.key("dispatch-module");
    append_seed(
        &rig.wal_store,
        &key,
        &dispatch_module_episode(&key, "first", "call-1"),
    );
    let first_frames = rig
        .wal_store
        .read_records(&key)
        .expect("read the first episode");
    rig.store
        .project(&key, &first_frames)
        .expect("materialize the first episode");
    append_seed(
        &rig.wal_store,
        &key,
        &dispatch_module_episode(&key, "second", "call-2"),
    );

    let page = rig
        .registry
        .read(read_request(&key, Some(0), Some(100)))
        .await
        .expect("a dispatch-module session reads");
    let texts: Vec<String> = page
        .messages
        .iter()
        .flat_map(|m| m.message.content.iter())
        .filter_map(|block| match block {
            ContentBlock::Text { text } => Some(text.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        texts,
        [
            "first question",
            "first answer",
            "second question",
            "second answer"
        ]
    );
    assert_eq!(page.messages.len(), 8, "each episode reads four messages");
    assert_eq!(page.lineage_state.state, "completed");
    let modules: Vec<_> = rig
        .wal_store
        .read_records(&key)
        .unwrap()
        .into_iter()
        .filter_map(|framed| match framed.record {
            WalRecord::ToolDispatchIntent { module_id, .. } => module_id,
            _ => None,
        })
        .collect();
    assert_eq!(modules, ["aft", "aft"]);
}
#[tokio::test]
async fn read_falls_back_when_budget_config_is_only_in_materialized_prefix() {
    let rig = Rig::new("budget-tail-fallback", Arc::new(NoopExec));
    let key = rig.key("budget-tail");
    let mut config = min_config();
    config.budget = Some(BudgetConfig {
        max_input_tokens: None,
        nudges: vec![BudgetNudge {
            at_tool_calls: 1,
            text: "Please finish soon.".into(),
        }],
        finalize_at_tool_calls: 99,
        finalize_text: "Finish now.".into(),
    });
    let run_id = RunId("budget-run".into());
    let call_id = ToolCallId("call-1".into());
    let batch_id = BatchId("batch-1".into());
    let records = vec![
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
            ts_ms: None,
            run_id: run_id.clone(),
            session: key.clone(),
            config: Box::new(config),
            input: vec![Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "Use the tool.".into(),
                }],
            )]
            .into(),
            origin: broca_wal::RunOrigin::Fresh,
            submission_id: None,
        },
        WalRecord::ModelStepFinished {
            hooks: None,
            step_id: StepId(0),
            assistant_message: AssistantMessage {
                message_id: MessageId("m-budget-tool".into()),
                content: vec![ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: call_id.clone(),
                    tool_name: "lookup".into(),
                    input: serde_json::json!({"q":"weather"}),
                })],
            }
            .into(),
            usage: usage(),
            finish_reason: Some(FinishReason::ToolCalls),
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        },
        WalRecord::ToolBatchStarted {
            batch_id: batch_id.clone(),
            step_id: StepId(0),
            planned_calls: vec![call_id.clone()],
        },
        WalRecord::ToolResult {
            hooks: None,
            batch_id: batch_id.clone(),
            result: ToolResult {
                tool_call_id: call_id,
                output: ToolOutput::Text { text: "ok".into() },
                is_error: false,
            }
            .into(),
            outcome: DispatchOutcome::Completed,
            reason: None,
            withdraw_elicitation: None,
            provider_held: None,
        },
        WalRecord::ToolBatchFinished {
            batch_id: batch_id.clone(),
        },
        WalRecord::TurnFinished { step_id: StepId(0) },
        WalRecord::BudgetFired {
            run_id,
            kind: BudgetKind::Nudge,
            threshold: 1,
            ordinal: 3,
            caller_text: None,
        },
    ];
    append_seed(&rig.wal_store, &key, &records);
    let framed = rig.wal_store.read_records(&key).expect("read seeded WAL");
    let watermark = rig
        .store
        .project(&key, &framed)
        .expect("materialize through the turn boundary");
    assert_eq!(watermark, framed[5].seq);

    let mut from = Some(0);
    let mut paged = Vec::new();
    loop {
        let page = rig
            .registry
            .read(read_request(&key, from, Some(2)))
            .await
            .expect("read budget lineage page");
        paged.extend(page.messages.into_iter().map(|message| message.message));
        let Some(next) = page.next_from_ordinal else {
            break;
        };
        from = Some(next);
    }

    let replayed = replay(&records).expect("full replay").prompt;
    assert_eq!(
        serde_json::to_vec(&paged).unwrap(),
        serde_json::to_vec(&replayed).unwrap(),
        "paged session.read must be byte-equal to full replay"
    );
}

#[tokio::test]
async fn read_paging_defaults_and_no_actor_invariant() {
    let rig = Rig::new("no-actor", Arc::new(NoopExec));
    let key = rig.key("idle");
    let empty_page = rig
        .registry
        .read(read_request(&key, None, Some(1)))
        .await
        .expect("read empty lineage");
    assert_eq!(empty_page.lineage_state.state, "idle");
    assert_eq!(empty_page.head, None);
    let empty_json = serde_json::to_value(&empty_page).expect("serialize empty page");
    assert!(empty_json.get("head").is_none());
    assert!(
        empty_json["lineage_state"].get("usage").is_none(),
        "an empty lineage omits unknown usage"
    );

    append_seed(
        rig.wal_store.as_ref(),
        &key,
        &[
            started(&key, "run-complete", "hello"),
            assistant_text(0, "one"),
            WalRecord::RunFinished {
                ts_ms: None,
                reason: RunFinishReason::Completed,
                usage: usage(),
                error: None,
                retries_used: 0,
            },
        ],
    );

    let page = rig
        .registry
        .read(read_request(&key, None, Some(1)))
        .await
        .expect("read newest page");
    assert_eq!(page.messages.len(), 1);
    assert_eq!(page.messages[0].ordinal, 1);
    assert_eq!(page.messages[0].mid, "m1");
    assert_eq!(page.next_from_ordinal, None);
    assert_eq!(page.lineage_state.state, "completed");
    assert_eq!(page.lineage_state.reason.as_deref(), Some("completed"));
    assert_eq!(page.lineage_state.usage, Some(usage()));
    assert_eq!(
        serde_json::to_value(&page).expect("serialize completed page")["lineage_state"]["usage"],
        serde_json::json!({
            "input_tokens": 7,
            "output_tokens": 3
        })
    );
    assert_eq!(
        rig.registry.slot_state(&key),
        None,
        "read must not open an actor"
    );
    let lease_files = std::fs::read_dir(&rig.lease_dir)
        .ok()
        .map(|entries| entries.count())
        .unwrap_or(0);
    assert_eq!(lease_files, 0, "read must not create a session lease file");

    let run_status = rig
        .registry
        .run_status(&key, RunId("run-complete".into()))
        .await
        .expect("completed run status");
    assert_eq!(
        serde_json::to_value(run_status).expect("serialize run status")["usage"],
        serde_json::to_value(usage()).expect("serialize expected usage"),
        "run.status resolves the durable terminal's exact run-total usage"
    );
}

#[tokio::test]
async fn read_head_resumes_subscribe_without_gaps_or_duplicates() {
    let rig = Rig::new("head-handoff", Arc::new(NoopExec));
    let key = rig.key("head-handoff");
    append_seed(
        &rig.wal_store,
        &key,
        &[
            started(&key, "run-before-read", "before"),
            assistant_text(0, "before reply"),
            WalRecord::RunFinished {
                ts_ms: None,
                reason: RunFinishReason::Completed,
                usage: usage(),
                error: None,
                retries_used: 0,
            },
        ],
    );

    let page = rig
        .registry
        .read(read_request(&key, None, Some(10)))
        .await
        .expect("read handoff page");
    let head = page.head.expect("non-empty lineage has a control head");
    assert!(
        page.messages
            .iter()
            .all(|message| message.mid == format!("m{}", message.ordinal)),
        "wire message ids use the MC ordinal coordinate"
    );

    // The read implementation derives both the page and `head` from one store snapshot plus one
    // exact WAL-tail vector. Appending only after that read returns models the other side of the
    // atomic cut: these records must all be strictly after the returned head.
    append_seed(
        &rig.wal_store,
        &key,
        &[
            started(&key, "run-after-read", "after"),
            assistant_text(0, "after reply"),
            WalRecord::RunFinished {
                ts_ms: None,
                reason: RunFinishReason::Completed,
                usage: usage(),
                error: None,
                retries_used: 0,
            },
        ],
    );
    let framed = rig.wal_store.read_records(&key).expect("read complete WAL");
    let expected: Vec<_> = project_control(&framed)
        .into_iter()
        .filter(|(cursor, _)| *cursor > broca_core::Cursor::new(head.wal_seq, head.sub_index))
        .collect();
    assert!(!expected.is_empty(), "the post-read append projects units");

    let (sink, mut stream) = mpsc::channel(32);
    rig.registry
        .subscribe(
            &key,
            SubscribeFrom::After {
                wal_seq: head.wal_seq,
                sub_index: head.sub_index,
            },
            sink,
        )
        .await
        .expect("subscribe from read head");

    let mut actual = Vec::new();
    for _ in 0..expected.len() {
        match broca_test_support::event_timeout(
            "read_head_resumes_subscribe_without_gaps_or_duplicates",
            stream.recv(),
            || format!("remaining_units={}", expected.len()),
        )
        .await
        .expect("post-head unit arrives")
        .expect("stream stays open")
        {
            Delivery::Control(event) => actual.push(event.as_ref().clone()),
            Delivery::Display(_) => {}
            other => panic!("unexpected delivery after read head: {other:?}"),
        }
    }
    assert_eq!(actual, expected);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), stream.recv())
            .await
            .is_err(),
        "no historical duplicate follows the post-head units"
    );
}

#[tokio::test]
async fn read_lineage_state_paused_and_active_with_open_dispatch() {
    let rig = Rig::new("states", Arc::new(NoopExec));

    let paused = rig.key("paused");
    append_seed(
        rig.wal_store.as_ref(),
        &paused,
        &[
            started(&paused, "run-paused", "auth"),
            WalRecord::RunPaused {
                ts_ms: None,
                run_id: RunId("run-paused".into()),
                reason: PauseReason::AuthRequired,
                error: Some(error_value("reauth")),
            },
        ],
    );
    let paused_page = rig
        .registry
        .read(read_request(&paused, Some(0), Some(10)))
        .await
        .expect("read paused");
    assert_eq!(paused_page.lineage_state.state, "paused");
    assert_eq!(
        paused_page.lineage_state.reason.as_deref(),
        Some("auth_required")
    );
    assert!(paused_page.lineage_state.error.is_some());

    let active = rig.key("active");
    append_seed(
        rig.wal_store.as_ref(),
        &active,
        &[
            started(&active, "run-active", "open"),
            WalRecord::ModelStepFinished {
                hooks: None,
                step_id: StepId(0),
                assistant_message: (AssistantMessage {
                    message_id: MessageId("m-open".into()),
                    content: vec![ContentBlock::ToolCall(ToolCall {
                        thought_signature: None,
                        tool_call_id: ToolCallId("open-call".into()),
                        tool_name: "lookup".into(),
                        input: serde_json::json!({}),
                    })],
                })
                .into(),
                usage: usage(),
                finish_reason: Some(FinishReason::ToolCalls),
                retries_used: 0,
                service_tier_used: None,
                speed_used: None,
            },
            WalRecord::ToolBatchStarted {
                batch_id: broca_protocol::BatchId("b-open".into()),
                step_id: StepId(0),
                planned_calls: vec![ToolCallId("open-call".into())],
            },
            WalRecord::ToolDispatchIntent {
                hooks: None,
                batch_id: broca_protocol::BatchId("b-open".into()),
                tool_call_id: ToolCallId("open-call".into()),
                tool_name: Some("lookup".into()),
                args: serde_json::json!({}).into(),
                module_id: None,
                schema_pin: None,
                approval_digest: None,
                call_key: None,
            },
        ],
    );
    let active_page = rig
        .registry
        .read(read_request(&active, Some(0), Some(10)))
        .await
        .expect("read active");
    assert_eq!(active_page.lineage_state.state, "active");
    assert_eq!(
        active_page.messages.len(),
        2,
        "reads expose the durable open tool turn even before batch commitment"
    );
    assert_eq!(active_page.messages[0].message.role, Role::User);
    assert_eq!(active_page.messages[1].message.role, Role::Assistant);
    let calls = &active_page.messages[1].tool_calls;
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].tool_call_id, "open-call");
    assert!(calls[0].indeterminate);
    assert!(calls[0].call_key.is_some());
}

#[tokio::test]
async fn read_falls_back_for_a_budget_firing_in_a_later_unprojected_episode() {
    let rig = Rig::new("budget-later-episode", Arc::new(NoopExec));
    let key = rig.key("budget-later-episode");
    let first_episode = vec![
        started(&key, "first-run", "first question"),
        assistant_text(0, "first answer"),
        WalRecord::RunFinished {
            ts_ms: None,
            reason: RunFinishReason::Completed,
            usage: usage(),
            error: None,
            retries_used: 0,
        },
    ];
    append_seed(&rig.wal_store, &key, &first_episode);
    let first_frames = rig
        .wal_store
        .read_records(&key)
        .expect("read first episode");
    rig.store
        .project(&key, &first_frames)
        .expect("materialize first episode");

    let mut config = min_config();
    config.budget = Some(BudgetConfig {
        max_input_tokens: None,
        nudges: vec![BudgetNudge {
            at_tool_calls: 1,
            text: "Please finish soon.".into(),
        }],
        finalize_at_tool_calls: 99,
        finalize_text: "Finish now.".into(),
    });
    let run_id = RunId("second-run".into());
    let call_id = ToolCallId("later-call".into());
    let batch_id = BatchId("later-batch".into());
    append_seed(
        &rig.wal_store,
        &key,
        &[
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
                ts_ms: None,
                run_id: run_id.clone(),
                session: key.clone(),
                config: Box::new(config),
                input: vec![Message::new(
                    Role::User,
                    vec![ContentBlock::Text {
                        text: "second question".into(),
                    }],
                )]
                .into(),
                origin: broca_wal::RunOrigin::Fresh,
                submission_id: None,
            },
            WalRecord::ModelStepFinished {
                hooks: None,
                step_id: StepId(0),
                assistant_message: (AssistantMessage {
                    message_id: MessageId("later-tool-step".into()),
                    content: vec![ContentBlock::ToolCall(ToolCall {
                        thought_signature: None,
                        tool_call_id: call_id.clone(),
                        tool_name: "lookup".into(),
                        input: serde_json::json!({}),
                    })],
                })
                .into(),
                usage: usage(),
                finish_reason: Some(FinishReason::ToolCalls),
                retries_used: 0,
                service_tier_used: None,
                speed_used: None,
            },
            WalRecord::ToolBatchStarted {
                batch_id: batch_id.clone(),
                step_id: StepId(0),
                planned_calls: vec![call_id.clone()],
            },
            WalRecord::ToolResult {
                hooks: None,
                batch_id: batch_id.clone(),
                result: ToolResult {
                    tool_call_id: call_id,
                    output: ToolOutput::Text { text: "ok".into() },
                    is_error: false,
                }
                .into(),
                outcome: DispatchOutcome::Completed,
                reason: None,
                withdraw_elicitation: None,
                provider_held: None,
            },
            WalRecord::ToolBatchFinished { batch_id },
            WalRecord::TurnFinished { step_id: StepId(0) },
            WalRecord::BudgetFired {
                run_id,
                kind: BudgetKind::Nudge,
                threshold: 1,
                ordinal: 5,
                caller_text: None,
            },
        ],
    );

    let page = rig
        .registry
        .read(read_request(&key, Some(0), Some(20)))
        .await
        .expect("read must use full replay for global budget ordinals");
    let records: Vec<_> = rig
        .wal_store
        .read_records(&key)
        .expect("read full WAL")
        .into_iter()
        .map(|frame| frame.record)
        .collect();
    assert_eq!(
        page.messages
            .into_iter()
            .map(|entry| entry.message)
            .collect::<Vec<_>>(),
        replay(&records).expect("full replay").prompt
    );
}

// COSTS 30 SECONDS OF WALL CLOCK, deliberately, and the reason is worth knowing before anyone
// "fixes" it. The seeded WAL carries a dangling RunStarted, so the actor's startup recovery tries
// to resume it; NoopExec panics by design, the admission ack drops, and a dropped ack classifies
// as StartRunFailure::Infrastructure -- transient, because in production it usually is. Recovery
// then retries under backoff until STARTUP_RECOVERY_RETRY_BUDGET (30s) is spent, logs its HARD
// WARNING, and proceeds. That is the production path behaving correctly on an executor that can
// never succeed.
//
// DO NOT reach for #[tokio::test(start_paused = true)]. Measured: auto-advance grants every
// re-armed backoff instantly, so the bounded 30s wait becomes an unbounded spin -- the test ran
// ten minutes before being killed, which in CI is a job timeout rather than a slow test. The
// obvious fix is strictly worse than the cost.
//
// AND THE INJECTABLE-BUDGET FIX IS DECLINED, having done that round rather than leaving it open.
// Production has never taken this path: 8,520 broca lines in the daemon log carry ZERO
// "startup durable-work recovery" events, against a control proving the search works. So the
// retry loop is a backstop that has never been needed, and threading a budget through the actor's
// construction would add a production seam to speed up a path production does not walk. The 30s
// stays confined to the one test that deliberately provokes it.
//
// REVERSE THIS IF the daemon log starts carrying recovery-deferred lines, or if a second test
// needs to provoke the same path -- at that point the seam earns itself and the cost is no longer
// one test's.
#[tokio::test]
async fn torn_import_is_publicly_interrupted_for_read_and_run_status() {
    let rig = Rig::new("torn-import-state", Arc::new(NoopExec));
    let key = rig.key("torn-import-state");
    let run_id = RunId("torn-import".into());
    append_seed(
        &rig.wal_store,
        &key,
        &[WalRecord::RunStarted {
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
            ts_ms: None,
            run_id: run_id.clone(),
            session: key.clone(),
            config: Box::new(min_config()),
            input: vec![Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "ghost import".into(),
                }],
            )]
            .into(),
            origin: broca_wal::RunOrigin::SeededFrom {
                origin_kind: broca_wal::SeedOriginKind::Import,
                source_ref: "cc:torn".into(),
                origin_device_pseudonym: None,
                base_seq: None,
            },
            submission_id: None,
        }],
    );

    let page = rig
        .registry
        .read(read_request(&key, Some(0), Some(10)))
        .await
        .expect("read torn import");
    assert_eq!(page.lineage_state.state, "interrupted");
    assert_eq!(page.lineage_state.reason.as_deref(), Some("interrupted"));

    let state = rig
        .registry
        .route_run_state(&key, run_id)
        .await
        .expect("resolve run state")
        .expect("known import run");
    assert_eq!(state.state, broca_core::RunIndexState::Interrupted);
}

#[tokio::test]
async fn archived_session_read_matches_live_page_without_extracting() {
    let rig = Rig::new("archive-page", Arc::new(NoopExec));
    let key = rig.key("archived");
    append_seed(
        &rig.wal_store,
        &key,
        &[
            started(&key, "r", "archive prompt"),
            assistant_text(0, "archive answer"),
            WalRecord::RunFinished {
                ts_ms: None,
                reason: RunFinishReason::Completed,
                usage: usage(),
                error: None,
                retries_used: 0,
            },
        ],
    );
    let before = rig
        .registry
        .route_read(&key, Some(0), Some(10))
        .await
        .unwrap();
    assert!(!before.messages.is_empty());
    let range = rig
        .wal_store
        .read_records_range(&key, 0, None, None)
        .unwrap();
    Persister::project_range(rig.store.as_ref(), &key, &range).unwrap();
    let path = rig.wal_store.wal_path(&key);
    let dir = rig.dir.join("wal-archive");
    std::fs::create_dir_all(&dir).unwrap();
    let archive_path = dir.join("fold-1.ark");
    let mut writer = broca_wal::archive::ArchiveWriter::create(&archive_path)
        .unwrap()
        .without_feature_floor();
    writer
        .append(
            &broca_core::session_addr(&key),
            &std::fs::read(&path).unwrap(),
        )
        .unwrap();
    writer.seal().unwrap();
    let archived_bytes = std::fs::read(&archive_path).unwrap();
    std::fs::remove_file(&path).unwrap();
    let after = rig
        .registry
        .route_read(&key, Some(0), Some(10))
        .await
        .unwrap();
    assert_eq!(after, before);
    assert!(!path.exists());
    assert_eq!(std::fs::read(&archive_path).unwrap(), archived_bytes);
}

#[tokio::test]
async fn archive_lookup_miss_rejects_store_prefix_but_no_directory_preserves_legacy_read() {
    let rig = Rig::new("archive-miss", Arc::new(NoopExec));
    let key = rig.key("missing");
    append_seed(
        &rig.wal_store,
        &key,
        &[
            started(&key, "r", "projected prompt"),
            assistant_text(0, "projected answer"),
            WalRecord::RunFinished {
                ts_ms: None,
                reason: RunFinishReason::Completed,
                usage: usage(),
                error: None,
                retries_used: 0,
            },
        ],
    );
    let range = rig
        .wal_store
        .read_records_range(&key, 0, None, None)
        .unwrap();
    Persister::project_range(rig.store.as_ref(), &key, &range).unwrap();
    std::fs::remove_file(rig.wal_store.wal_path(&key)).unwrap();
    let legacy = rig
        .registry
        .route_read(&key, Some(0), Some(10))
        .await
        .unwrap();
    assert!(!legacy.messages.is_empty());
    let dir = rig.dir.join("wal-archive");
    std::fs::create_dir_all(&dir).unwrap();
    let mut writer = broca_wal::archive::ArchiveWriter::create(dir.join("fold-1.ark"))
        .unwrap()
        .without_feature_floor();
    writer.append("0000000000000000", &[]).unwrap();
    writer.seal().unwrap();
    assert_eq!(rig.wal_store.wal_present(&key), Some(false));
    assert!(matches!(
        rig.wal_store
            .read_records_range(&key, 0, None, None)
            .unwrap()
            .records(),
        broca_wal::WalRangeRecords::Absent
    ));
    let error = rig
        .registry
        .route_read(&key, Some(0), Some(10))
        .await
        .unwrap_err();
    // The message names the EVIDENCE, and deliberately shares no phrase with the pre-fix one:
    // consumers pattern-match this prose because no wire code separates "never existed" from
    // "existed and unreadable", so reusing the old words would hand every such matcher a
    // suppression rule for a real fault.
    let text = error.to_string();
    assert!(
        text.contains("materialized message(s) but its WAL is unreachable"),
        "a lineage the store vouches for must fail loudly: {text}"
    );
    assert!(
        !text.contains("neither a live WAL nor an archived member"),
        "the retained refusal must not reuse the wording a consumer may suppress: {text}"
    );
}

/// A BRAND-NEW SESSION ON A FOLDED TREE MUST READ AS EMPTY, NOT REFUSE.
///
/// This is the production defect the evidence check exists to fix. Before any fold there is no
/// `wal-archive/` directory, so nothing is consulted and a new session reads an empty page.
/// After one, a consulted miss is the NORMAL state of a session nobody has written -- so every
/// new session refused its first read, live from 2026-09-13, found by the one client that reads
/// before it writes.
///
/// The archive directory is the whole discriminator, which is why this test creates a sealed
/// container holding an unrelated member: without it the read falls through the legacy path and
/// passes for the wrong reason.
#[tokio::test]
async fn a_new_session_reads_empty_on_a_folded_tree_rather_than_refusing() {
    let rig = Rig::new("cold-start-folded", Arc::new(NoopExec));
    let key = rig.key("never-written");

    let dir = rig.dir.join("wal-archive");
    std::fs::create_dir_all(&dir).unwrap();
    let mut writer = broca_wal::archive::ArchiveWriter::create(dir.join("fold-1.ark"))
        .unwrap()
        .without_feature_floor();
    writer.append("0000000000000000", &[]).unwrap();
    writer.seal().unwrap();

    // The archive IS consulted for this session and misses -- the exact condition that used to
    // refuse. Asserted rather than assumed, so the test cannot pass because the lookup was
    // skipped.
    let range = rig
        .wal_store
        .read_records_range(&key, 0, None, None)
        .unwrap();
    assert!(
        range.archive_lookup_missed(),
        "the archive must be consulted and miss, or this test proves nothing"
    );
    assert_eq!(rig.wal_store.wal_present(&key), Some(false));

    let page = rig
        .registry
        .route_read(&key, Some(0), Some(10))
        .await
        .expect("a session with no durable history anywhere must read as empty");
    assert!(page.messages.is_empty());
}

#[tokio::test]
async fn folded_session_send_preserves_lineage_identity_and_archived_prefix() {
    let rig = Rig::with_exec_factory("archive-send", |wal_store, store| {
        Arc::new(ScriptedCoreExec {
            wal_store,
            store,
            scripts: Mutex::new(VecDeque::from([
                vec![text_step("first appended answer")],
                vec![text_step("second appended answer")],
            ])),
        })
    });
    let key = rig.key("archived-send");
    let mut start = started(&key, "seed", "original prompt");
    if let WalRecord::RunStarted { ts_ms, .. } = &mut start {
        *ts_ms = Some(1);
    }
    append_seed(
        &rig.wal_store,
        &key,
        &[
            start,
            assistant_text(0, "original answer"),
            WalRecord::RunFinished {
                ts_ms: Some(1),
                reason: RunFinishReason::Completed,
                usage: usage(),
                error: None,
                retries_used: 0,
            },
        ],
    );
    let before = rig
        .wal_store
        .read_records_range(&key, 0, None, None)
        .unwrap();
    let lineage = before.lineage_id.unwrap();
    let prefix = rig.wal_store.read_records(&key).unwrap();
    let archived_head = prefix.last().unwrap().seq;
    let report = broca_core::wal_fold::fold(
        &rig.dir,
        &broca_core::wal_fold::FoldOptions {
            write: true,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(report.removed, 1);
    let path = rig.wal_store.wal_path(&key);
    assert!(!path.exists());
    let archive = rig
        .dir
        .join("wal-archive")
        .join(report.journal.unwrap())
        .with_extension("ark");
    let container = std::fs::read(&archive).unwrap();
    send(&rig, &key, "appended-1", "first appended prompt").await;
    wait_status(&rig, &key, "appended-1", "completed").await;
    let after = rig
        .wal_store
        .read_records_range(&key, 0, None, None)
        .unwrap();
    assert_eq!(after.lineage_id, Some(lineage));
    let records = rig.wal_store.read_records(&key).unwrap();
    assert_eq!(&records[..prefix.len()], prefix.as_slice());
    assert_eq!(records[prefix.len()].seq, archived_head + 1);
    assert!(
        matches!(&records[prefix.len()].record, WalRecord::RunStarted { run_id, .. } if run_id.0 == "appended-1")
    );
    let identity = rig.wal_store.file_identity(&key).unwrap();
    send(&rig, &key, "appended-2", "second appended prompt").await;
    wait_status(&rig, &key, "appended-2", "completed").await;
    assert_eq!(rig.wal_store.file_identity(&key).unwrap(), identity);
    let second = rig.wal_store.read_records(&key).unwrap();
    assert_eq!(&second[..records.len()], records.as_slice());
    assert!(second.len() > records.len());
    assert_eq!(std::fs::read(archive).unwrap(), container);
    let page = rig
        .registry
        .route_read(&key, Some(0), Some(100))
        .await
        .unwrap();
    let rendered = serde_json::to_string(
        &page
            .messages
            .iter()
            .map(|row| &row.message)
            .collect::<Vec<_>>(),
    )
    .unwrap();
    for text in [
        "original answer",
        "first appended answer",
        "second appended answer",
    ] {
        assert!(rendered.contains(text), "{text}");
    }
}

fn finished_ok() -> WalRecord {
    WalRecord::RunFinished {
        ts_ms: None,
        reason: RunFinishReason::Completed,
        usage: usage(),
        error: None,
        retries_used: 0,
    }
}

/// `started`, but with the episode's frozen tool list chosen by the caller, so two episodes of
/// one session can be told apart by the tools they were admitted with.
fn started_with_tools(key: &BindIdentity, run: &str, text: &str, tools: Vec<Tool>) -> WalRecord {
    let mut record = started(key, run, text);
    if let WalRecord::RunStarted { config, .. } = &mut record {
        config.tools = tools.into();
    }
    record
}

/// Two finished episodes whose frozen tool lists differ: the first offers only `lookup`, the
/// second offers `lookup` plus a module-routed `web_search`. Written as a legacy (unidentified)
/// WAL so the read carries no random lineage id and its bytes are reproducible.
fn two_episode_tool_seed(key: &BindIdentity) -> Vec<WalRecord> {
    let mut web_search = tool("web_search");
    web_search.description = "search the web".into();
    web_search.input_schema = serde_json::json!({
        "type": "object",
        "properties": {"query": {"type": "string"}},
        "required": ["query"]
    });
    web_search.module = Some("plexus".into());
    vec![
        started_with_tools(key, "run-first", "first question", vec![tool("lookup")]),
        assistant_text(0, "first answer"),
        finished_ok(),
        started_with_tools(
            key,
            "run-latest",
            "second question",
            vec![tool("lookup"), web_search],
        ),
        assistant_text(0, "second answer"),
        finished_ok(),
    ]
}

fn two_episode_golden_path() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../broca-wire/tests/golden/session_read.two_episode.json")
}

/// The serve-path response without opt-in tools. Run attribution is always present when
/// recoverable, while asking for tools still adds only the two tool-list fields.
fn two_episode_pinned_bytes() -> String {
    std::fs::read_to_string(two_episode_golden_path())
        .unwrap()
        .trim_end_matches('\n')
        .to_owned()
}

/// Also the proof that `include_tools: false` (what `read_request` sends) leaves the response
/// byte-identical to the page whose tool-list fields are omitted.
#[tokio::test]
async fn two_episode_read_response_bytes_are_pinned() {
    let rig = Rig::new("two-episode-pin", Arc::new(NoopExec));
    let key = rig.key("pinned");
    write_legacy_seed(&rig.wal_store, &key, &two_episode_tool_seed(&key));

    let request = read_request(&key, Some(0), Some(10));
    assert!(!request.include_tools);
    let page = rig.registry.read(request).await.expect("session.read");
    let bytes = serde_json::to_string(&page).expect("serialize session.read response");
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(two_episode_golden_path(), format!("{bytes}\n")).unwrap();
    }
    assert_eq!(bytes, two_episode_pinned_bytes());
}

/// The pinned response with the latest episode's tools appended: the opt-in adds exactly the
/// two tool fields and changes nothing else about the page.
fn pinned_bytes_with_latest_tools() -> String {
    let pinned = two_episode_pinned_bytes();
    let body = pinned
        .strip_suffix('}')
        .expect("pinned response is a JSON object");
    format!(
        "{body},{}",
        concat!(
            r#""tools":["#,
            r#"{"name":"lookup","description":"test tool","input_schema":{"type":"object"}},"#,
            r#"{"name":"web_search","description":"search the web","#,
            r#""input_schema":{"properties":{"query":{"type":"string"}},"required":["query"],"type":"object"},"#,
            r#""module":"plexus"}],"#,
            r#""tools_run_id":"run-latest"}"#
        )
    )
}

/// Two episodes with different tool lists: the read reports the LATEST episode's list and the
/// run it came from. Checked twice, because the latest `RunStarted` can be found two ways: in
/// the WAL tail read for the page (nothing materialized), or in the materialized prefix, which
/// takes a separate WAL read bounded at the store's watermark.
#[tokio::test]
async fn include_tools_returns_the_latest_episodes_tools_and_run_id() {
    let rig = Rig::new("include-tools-latest", Arc::new(NoopExec));

    let from_tail = rig.key("from-tail");
    write_legacy_seed(
        &rig.wal_store,
        &from_tail,
        &two_episode_tool_seed(&from_tail),
    );

    let from_prefix = rig.key("from-prefix");
    write_legacy_seed(
        &rig.wal_store,
        &from_prefix,
        &two_episode_tool_seed(&from_prefix),
    );
    let framed = rig.wal_store.read_records(&from_prefix).expect("read WAL");
    rig.store
        .project(&from_prefix, &framed)
        .expect("materialize both episodes");
    let snapshot = rig
        .store
        .read_page_with_watermark(
            &from_prefix,
            Some(0),
            10,
            broca_core::context::ReadView::Raw,
        )
        .expect("read store snapshot");
    let latest_started_seq = framed
        .iter()
        .rev()
        .find(|framed| matches!(framed.record, WalRecord::RunStarted { .. }))
        .expect("seed has episodes")
        .seq;
    assert!(
        snapshot.watermark_seq >= latest_started_seq,
        "both RunStarted records must sit inside the materialized prefix, so the page's WAL \
         tail carries none (watermark {}, latest RunStarted {latest_started_seq})",
        snapshot.watermark_seq
    );

    for key in [&from_tail, &from_prefix] {
        let page = rig
            .registry
            .read(read_request_with_tools(key, Some(0), Some(10)))
            .await
            .expect("session.read with tools");
        assert_eq!(page.tools_run_id.as_deref(), Some("run-latest"), "{key:?}");
        let names: Vec<_> = page
            .tools
            .as_ref()
            .expect("a session with episodes reports tools")
            .iter()
            .map(|tool| tool.name.as_str())
            .collect();
        assert_eq!(names, ["lookup", "web_search"], "{key:?}");
        assert_eq!(
            serde_json::to_string(&page).unwrap(),
            pinned_bytes_with_latest_tools(),
            "{key:?}"
        );
    }
}

/// No episode means no tool list to report: `tools` is absent, not `[]`, so a consumer cannot
/// mistake "never ran" for "ran with no tools".
#[tokio::test]
async fn include_tools_on_a_session_with_no_episode_omits_tools() {
    let rig = Rig::new("include-tools-empty", Arc::new(NoopExec));
    let key = rig.key("never-ran");

    let page = rig
        .registry
        .read(read_request_with_tools(&key, None, None))
        .await
        .expect("session.read of an empty session");
    assert!(page.messages.is_empty());
    assert_eq!(page.tools, None);
    assert_eq!(page.tools_run_id, None);
    let bytes = serde_json::to_string(&page).unwrap();
    assert!(!bytes.contains("tools"), "{bytes}");
}

/// When the store holds a transcript but the WAL that recorded its episodes is gone, the tools
/// cannot be read. That is an error, not an absent `tools`, which would claim the session never
/// had an episode.
#[tokio::test]
async fn include_tools_refuses_when_the_store_has_history_but_the_wal_is_gone() {
    let rig = Rig::new("include-tools-wal-gone", Arc::new(NoopExec));
    let key = rig.key("wal-gone");
    write_legacy_seed(&rig.wal_store, &key, &two_episode_tool_seed(&key));
    let framed = rig.wal_store.read_records(&key).expect("read WAL");
    rig.store
        .project(&key, &framed)
        .expect("materialize both episodes");
    std::fs::remove_file(rig.wal_store.wal_path(&key)).expect("remove the WAL");

    // Without the opt-in the materialized prefix is still served, as before.
    rig.registry
        .read(read_request(&key, Some(0), Some(10)))
        .await
        .expect("the prefix alone still reads");
    let error = rig
        .registry
        .read(read_request_with_tools(&key, Some(0), Some(10)))
        .await
        .expect_err("tools cannot be reported without the WAL");
    assert!(
        error.to_string().contains("no readable RunStarted"),
        "{error}"
    );
}

/// A steered message reads back as a user message where the model saw it: written during the
/// first step, it is placed after that step's answer, at the next step boundary. The gated
/// frame raises the feature floor, the WAL-authoritative page and the materialized store agree,
/// and a second read after projection returns the same page.
#[tokio::test]
async fn session_read_renders_a_steered_message_at_its_wal_position() {
    let rig = Rig::new("steered-read", Arc::new(NoopExec));
    let key = rig.key("steered");
    let steered = WalRecord::Steered {
        retention: None,
        ts_ms: None,
        title: None,
        send_id: Some("steer-1".into()),
        request_identity_digest: None,
        delivery: Some(broca_protocol::Delivery::Steer),
        input: vec![Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "write up now".into(),
            }],
        )]
        .into(),
        hooks: None,
    };
    append_seed(
        &rig.wal_store,
        &key,
        &[
            started(&key, "run-steered", "hello"),
            WalRecord::StepStarted { step_id: StepId(1) },
            steered,
            assistant_text(1, "first answer"),
            WalRecord::StepStarted { step_id: StepId(2) },
            assistant_text(2, "written up"),
            WalRecord::RunFinished {
                ts_ms: None,
                reason: RunFinishReason::Completed,
                usage: usage(),
                error: None,
                retries_used: 0,
            },
        ],
    );
    assert_eq!(
        broca_wal::read_floor(&rig.wal_store.feature_floor_path()).unwrap(),
        std::collections::BTreeSet::from(["steer-queue/v1".to_owned()]),
        "the floor names the feature before its first frame"
    );
    let texts = |page: &broca_module::serve::SessionReadResponse| -> Vec<(Role, String)> {
        page.messages
            .iter()
            .map(|entry| {
                let text = match entry.message.content.first() {
                    Some(ContentBlock::Text { text }) => text.clone(),
                    _ => String::new(),
                };
                (entry.message.role, text)
            })
            .collect()
    };
    let expected = vec![
        (Role::User, "hello".to_owned()),
        (Role::Assistant, "first answer".to_owned()),
        (Role::User, "write up now".to_owned()),
        (Role::Assistant, "written up".to_owned()),
    ];
    let unprojected = rig
        .registry
        .read(read_request(&key, Some(0), Some(10)))
        .await
        .expect("session.read of a steered WAL");
    assert_eq!(texts(&unprojected), expected);

    let framed = rig.wal_store.read_records(&key).expect("read steered WAL");
    rig.store
        .project(&key, &framed)
        .expect("materialize the steered session");
    let projected = rig
        .registry
        .read(read_request(&key, Some(0), Some(10)))
        .await
        .expect("session.read after projection");
    assert_eq!(texts(&projected), expected);
    assert_eq!(
        projected
            .messages
            .iter()
            .map(|entry| entry.ordinal)
            .collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );
}

/// Materialize every frame of `key`'s WAL into the rig's store. A read's WAL tail then starts
/// past them, so any read from the WAL's first sequence number is a whole-WAL read.
fn materialize_all(rig: &Rig, key: &BindIdentity) {
    let frames = rig
        .wal_store
        .read_records(key)
        .expect("read the seeded WAL");
    rig.store
        .project(key, &frames)
        .expect("materialize the WAL");
}

/// The reply to the send that admitted a session carries its baseline from what the session's
/// actor already holds, so answering it reads no WAL: the baseline is answered for the first
/// episode's run, and for no later run.
#[tokio::test]
async fn admission_baseline_reads_nothing_and_answers_only_the_first_episode() {
    let (rig, whole_wal_reads) =
        Rig::with_counting_zero_ranges_and_exec("admission-baseline-cost", |wal_store, store| {
            Arc::new(ScriptedCoreExec {
                wal_store,
                store,
                scripts: Mutex::new(VecDeque::from([
                    vec![text_step("one")],
                    vec![text_step("two")],
                ])),
            })
        });
    let key = rig.key("baseline-cost");
    send(&rig, &key, "run-1", "first").await;
    wait_status(&rig, &key, "run-1", "completed").await;
    whole_wal_reads.store(0, Ordering::SeqCst);
    let baseline = rig
        .registry
        .admission_baseline(&key, &RunId("run-1".into()));
    assert_eq!(
        baseline,
        Some(broca_wire::BaselineWire::plan_less(vec!["lookup".into()]))
    );
    send(&rig, &key, "run-2", "second").await;
    wait_status(&rig, &key, "run-2", "completed").await;
    whole_wal_reads.store(0, Ordering::SeqCst);
    assert_eq!(
        rig.registry
            .admission_baseline(&key, &RunId("run-2".into())),
        None,
        "a later send did not admit the session"
    );
    assert_eq!(
        rig.registry
            .admission_baseline(&key, &RunId("run-1".into())),
        baseline,
        "a retry of the admitting send still answers it"
    );
    assert_eq!(whole_wal_reads.load(Ordering::SeqCst), 0);
}

/// `session.head` answers a materialized session from the store and the WAL past its watermark,
/// and the live WAL file's write time: it never reads the whole WAL, however long it is.
#[tokio::test]
async fn session_head_reads_no_whole_wal() {
    let (rig, whole_wal_reads) = Rig::with_counting_zero_ranges("head-cost");
    let key = rig.key("head-cost");
    for (run, call) in [
        ("first", "call-1"),
        ("second", "call-2"),
        ("third", "call-3"),
    ] {
        append_seed(
            &rig.wal_store,
            &key,
            &dispatch_module_episode(&key, run, call),
        );
    }
    materialize_all(&rig, &key);
    let page = rig
        .registry
        .read(read_request(&key, Some(0), Some(100)))
        .await
        .unwrap();
    whole_wal_reads.store(0, Ordering::SeqCst);
    let head = rig.registry.head(&key).await.expect("head answers");
    assert_eq!(whole_wal_reads.load(Ordering::SeqCst), 0, "{head:?}");
    assert_eq!(head.lineage_id, page.lineage_id);
    assert_eq!(head.last_ordinal, Some(page.messages.len() as u64 - 1));
    assert_eq!(
        head.last_run_state
            .as_ref()
            .map(|state| state.state.as_str()),
        Some("completed")
    );
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    assert!(
        head.updated_at
            .is_some_and(|at| at <= now_ms && at + 600_000 > now_ms),
        "the live WAL's write time: {head:?}"
    );
}

/// Every tool call a page reports, as `(ordinal, tool_call_id, call_key, dispatched_to)`, in
/// page order.
fn reported_calls(
    page: &broca_module::serve::SessionReadResponse,
) -> Vec<(u64, String, Option<String>, Option<String>)> {
    page.messages
        .iter()
        .flat_map(|message| {
            message.tool_calls.iter().map(|call| {
                (
                    message.ordinal,
                    call.tool_call_id.clone(),
                    call.call_key.clone(),
                    call.dispatched_to.clone(),
                )
            })
        })
        .collect()
}

/// Reporting a page's tool calls costs what the page costs. A call in the read's own WAL tail
/// (past the store's watermark) is reported from that tail. A call already materialized needs
/// one pass over the whole WAL the first time any read reports it, and never again. So
/// rereading tool-using pages reads no whole WAL.
#[tokio::test]
async fn tool_call_attribution_costs_follow_the_page_not_the_session() {
    let (rig, whole_wal_reads) = Rig::with_counting_zero_ranges("attribution-cost");
    let key = rig.key("attribution-cost");
    append_seed(
        &rig.wal_store,
        &key,
        &dispatch_module_episode(&key, "first", "call-1"),
    );
    materialize_all(&rig, &key);
    append_seed(
        &rig.wal_store,
        &key,
        &dispatch_module_episode(&key, "second", "call-2"),
    );
    whole_wal_reads.store(0, Ordering::SeqCst);

    // The second episode is in the WAL tail only: its call is reported from the tail.
    let tail_page = rig
        .registry
        .read(read_request(&key, Some(4), Some(100)))
        .await
        .unwrap();
    let tail_calls = reported_calls(&tail_page);
    assert_eq!(tail_calls.len(), 1);
    assert!(tail_calls[0].2.is_some(), "{tail_calls:?}");
    assert_eq!(whole_wal_reads.load(Ordering::SeqCst), 0, "a tail call");

    // The first episode's call is materialized, outside every read's WAL tail: the first read
    // reporting it passes over the whole WAL once, and later reads reuse what that pass found.
    let mut seen = None;
    let mut first_pass = None;
    for _ in 0..3 {
        let page = rig
            .registry
            .read(read_request(&key, Some(0), Some(100)))
            .await
            .unwrap();
        let calls = reported_calls(&page);
        assert_eq!(calls.len(), 2, "{calls:?}");
        for call in &calls {
            assert!(call.2.is_some(), "{calls:?}");
            assert_eq!(call.3.as_deref(), Some("aft"));
        }
        assert_eq!(calls[1], tail_calls[0], "the same facts from either source");
        if let Some(seen) = &seen {
            assert_eq!(&calls, seen);
        }
        seen = Some(calls);
        first_pass.get_or_insert(whole_wal_reads.load(Ordering::SeqCst));
    }
    // One pass over the whole WAL, paid by the first read and never again. A pass with no
    // actor holding the session costs one range read where the WAL's file identity lets the
    // reader reuse an earlier flush proof (unix), and two where it can't (a candidate read,
    // then the read bounded by a fresh flush), so only the unix count is exact.
    let first_pass = first_pass.unwrap();
    assert!(first_pass >= 1, "the first read paid for the pass");
    assert_eq!(
        whole_wal_reads.load(Ordering::SeqCst),
        first_pass,
        "later reads reuse what the pass found"
    );
    #[cfg(unix)]
    assert_eq!(first_pass, 1);
}

/// A message no recorded step produced (one an import brought in) is never given a real step's
/// dispatch facts, even when it holds exactly that step's calls: facts are tied to the step
/// that produced a message, never matched by content.
#[tokio::test]
async fn an_imported_message_never_takes_a_real_steps_attribution() {
    let (rig, _) = Rig::with_counting_zero_ranges("import-attribution");
    let key = rig.key("import-attribution");
    let mut import = started(&key, "import", "imported question");
    let WalRecord::RunStarted { origin, input, .. } = &mut import else {
        unreachable!()
    };
    *origin = broca_wal::RunOrigin::SeededFrom {
        origin_kind: broca_wal::SeedOriginKind::Import,
        source_ref: "fixture:import".into(),
        origin_device_pseudonym: None,
        base_seq: None,
    };
    // The imported message holds the same call (id, name and arguments) as the real step
    // appended after it.
    *input = vec![
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "imported question".into(),
            }],
        ),
        Message::new(
            Role::Assistant,
            vec![ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId("call-1".into()),
                tool_name: "lookup".into(),
                input: serde_json::json!({}),
            })],
        ),
        Message::new(
            Role::Tool,
            vec![ContentBlock::ToolResult(ToolResult {
                tool_call_id: ToolCallId("call-1".into()),
                output: ToolOutput::Text {
                    text: "imported result".into(),
                },
                is_error: false,
            })],
        ),
    ]
    .into();
    let mut records = vec![
        import,
        WalRecord::RunFinished {
            ts_ms: None,
            reason: RunFinishReason::Completed,
            usage: usage(),
            error: None,
            retries_used: 0,
        },
    ];
    records.extend(dispatch_module_episode(&key, "real", "call-1"));
    append_seed(&rig.wal_store, &key, &records);

    let expect = |page: &broca_module::serve::SessionReadResponse| {
        let calls = reported_calls(page);
        assert_eq!(calls.len(), 2, "{calls:?}");
        assert_eq!(
            (calls[0].0, calls[0].2.as_deref(), calls[0].3.as_deref()),
            (1, None, None),
            "the imported call: {calls:?}"
        );
        assert_eq!(
            (calls[1].0, calls[1].3.as_deref()),
            (4, Some("aft")),
            "{calls:?}"
        );
        assert!(calls[1].2.is_some(), "the real call has a key: {calls:?}");
    };
    // Read with every record in the WAL tail, then again with everything materialized, when the
    // facts come from a pass over the whole WAL instead.
    expect(
        &rig.registry
            .read(read_request(&key, Some(0), Some(100)))
            .await
            .unwrap(),
    );
    materialize_all(&rig, &key);
    expect(
        &rig.registry
            .read(read_request(&key, Some(0), Some(100)))
            .await
            .unwrap(),
    );
}

#[tokio::test]
async fn read_counting_store_forwards_authoritative_lineage_and_wal_presence() {
    let (rig, full_reads) = Rig::with_counting_wal("presence-forwarding");
    let key = rig.key("fresh");
    let counted = ReadCountingWalStore {
        inner: rig.wal_store.clone(),
        full_reads: full_reads.clone(),
    };
    assert_eq!(counted.wal_present(&key), Some(false));
    assert!(!counted.lineage_exists(&key, None).unwrap());
    let writer = counted.open(&key).unwrap();
    assert_eq!(counted.wal_present(&key), Some(true));
    assert!(counted.lineage_exists(&key, None).unwrap());
    assert_eq!(full_reads.load(Ordering::SeqCst), 0);
    drop(writer);
}

#[tokio::test]
async fn transcript_reply_body_budget_applies_to_tail_range_and_large_requests() {
    let rig = crate::common::Rig::new(
        "read-reply-budget",
        crate::common::FakeExecutor::auto(),
        Duration::from_secs(3600),
    );
    let key = rig.key("session");
    let cap = 3 * 1024 * 1024;
    // The message-only sum fits, but ordinals, mids, lineage and head push the reply over.
    let messages: Vec<_> = (0..2)
        .map(|_| {
            Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "x".repeat(cap / 2 - 64),
                }],
            )
        })
        .collect();
    assert!(
        messages
            .iter()
            .map(|message| serde_json::to_vec(message).unwrap().len())
            .sum::<usize>()
            < cap
    );
    rig.registry
        .import(SessionImportRequest {
            project_id: None,
            principal: None,
            session: key.clone(),
            run_id: RunId("import-budget".into()),
            messages: messages.clone(),
            source_ref: "fixture:read-budget".into(),
        })
        .await
        .unwrap();
    for tail in [false, true] {
        for requested_cap in [None, Some(64 * 1024 * 1024)] {
            let mut request =
                SessionReadRequest::new(key.clone(), if tail { None } else { Some(0) }, Some(100));
            request.max_bytes = requested_cap;
            let response = rig.registry.read(request).await.unwrap();
            assert!(serde_json::to_vec(&response).unwrap().len() < cap);
            assert_eq!(response.messages.len(), 1);
            let ordinal = u64::from(tail);
            assert_eq!(response.messages[0].ordinal, ordinal);
            assert_eq!(
                serde_json::to_value(&response.messages[0].message).unwrap(),
                serde_json::to_value(&messages[ordinal as usize]).unwrap()
            );
            assert_eq!(
                response.next_from_ordinal,
                if tail { None } else { Some(1) }
            );
        }
    }
}

#[tokio::test]
async fn model_view_pages_fit_the_serialized_wire_reply_and_continue_exactly() {
    let rig = crate::common::Rig::new(
        "model-read-reply-budget",
        crate::common::FakeExecutor::auto(),
        Duration::from_secs(3600),
    );
    let key = rig.key("session");
    let messages: Vec<_> = (0..3)
        .map(|_| {
            Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "x".repeat(1_250_000),
                }],
            )
        })
        .collect();
    rig.registry
        .import(SessionImportRequest {
            project_id: None,
            principal: None,
            session: key.clone(),
            run_id: RunId("import-model-budget".into()),
            messages: messages.clone(),
            source_ref: "fixture:model-read-budget".into(),
        })
        .await
        .unwrap();

    let first = rig
        .registry
        .read_model(SessionReadRequest::new(key.clone(), Some(0), Some(100)))
        .await
        .unwrap();
    assert_eq!(first.messages.len(), 2);
    assert_eq!(first.next_from_ordinal, Some(2));
    assert!(serde_json::to_vec(&first).unwrap().len() < 3 * 1024 * 1024);
    for (entry, ordinal) in first.messages.iter().zip(0..2) {
        assert_eq!(entry.source.from_ordinal(), ordinal);
        assert_eq!(
            serde_json::to_value(&entry.message).unwrap(),
            serde_json::to_value(&messages[ordinal as usize]).unwrap()
        );
    }

    let second = rig
        .registry
        .read_model(SessionReadRequest::new(
            key,
            first.next_from_ordinal,
            Some(100),
        ))
        .await
        .unwrap();
    assert_eq!(second.messages.len(), 1);
    assert_eq!(second.messages[0].source.from_ordinal(), 2);
    assert_eq!(
        serde_json::to_value(&second.messages[0].message).unwrap(),
        serde_json::to_value(&messages[2]).unwrap()
    );
    assert_eq!(second.next_from_ordinal, None);
    assert!(serde_json::to_vec(&second).unwrap().len() < 3 * 1024 * 1024);
    drop(rig);
}

#[tokio::test]
async fn imported_message_over_page_target_is_served_whole_when_frame_fits() {
    let rig = crate::common::Rig::new(
        "read-oversized-import",
        crate::common::FakeExecutor::auto(),
        Duration::from_secs(3600),
    );
    let key = rig.key("session");
    let message = Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: "x".repeat(3 * 1024 * 1024 + 128),
        }],
    );
    rig.registry
        .import(SessionImportRequest {
            project_id: None,
            principal: None,
            session: key.clone(),
            run_id: RunId("import-oversized".into()),
            messages: vec![message.clone()],
            source_ref: "fixture:oversized-message".into(),
        })
        .await
        .unwrap();
    let response = rig
        .registry
        .read(SessionReadRequest::new(key, Some(0), Some(100)))
        .await
        .unwrap();
    assert_eq!(response.messages.len(), 1);
    assert_eq!(
        serde_json::to_value(&response.messages[0].message).unwrap(),
        serde_json::to_value(&message).unwrap()
    );
    assert!(serde_json::to_vec(&response).unwrap().len() > 3 * 1024 * 1024);
    assert!(
        broca_wire::serialized_result_frame_bytes(&response) <= broca_wire::SUBC_MAX_FRAME_BYTES
    );
}

#[tokio::test]
async fn imported_message_over_frame_limit_is_refused_with_identity() {
    let rig = crate::common::Rig::new(
        "read-oversized-import-frame",
        crate::common::FakeExecutor::auto(),
        Duration::from_secs(3600),
    );
    let key = rig.key("session");
    let message = Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: "x".repeat(4 * 1024 * 1024),
        }],
    );
    rig.registry
        .import(SessionImportRequest {
            project_id: None,
            principal: None,
            session: key.clone(),
            run_id: RunId("import-over-frame".into()),
            messages: vec![message],
            source_ref: "fixture:oversized-frame-message".into(),
        })
        .await
        .unwrap();
    let error = rig
        .registry
        .read(SessionReadRequest::new(key.clone(), Some(0), Some(100)))
        .await
        .unwrap_err();
    let broca_module::serve::ServeError::ReplyTooLarge(detail) = error else {
        panic!("expected a frame-size refusal naming the message, got {error:?}");
    };
    assert!(detail.bytes > detail.limit);
    assert_eq!(detail.limit, broca_wire::SUBC_MAX_FRAME_BYTES);
    assert!(matches!(
        detail.subject,
        cortexkit_role_llm_runner::errors::ReplyTooLargeSubject::Message {
            session,
            ordinal: 0
        } if session == key.session
    ));
    drop(rig);
}
