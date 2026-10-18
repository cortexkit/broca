//! Integration proof of the agentic loop: drives `run_loop` with a scripted
//! provider + the stub tool plane + the file lease + an approval policy, and
//! asserts the design invariants (stop_when after every step, batch-bracketed
//! source-ordered dispatch, the tool_call_id fence, approval/capability gates,
//! the single-writer lease, display passthrough).

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use broca_core::{
    canonical_request_bytes, lease_key_for, replay, run_loop,
    run_loop_on_writer_admitted_with_prior_snapshot, run_loop_with_control, run_loop_with_store,
    AdmissionCheckpointV1, AllowAll, BatchCtx, DispatchResult, FileLeaseStore, FileWalStore,
    LeaseStore, MaterializedStore, OutputEvent, PendingSubmission, Persister, PriorWalSnapshot,
    RunControl, RunError, RunRequest, ScriptedApproval, StartMode, StorePrefix, StubEvent,
    StubProvider, StubReply, StubToolPlane, ToolManifest, ToolPlane, ToolPlaneError, WalHandle,
    WalStore,
};
use broca_protocol::BindIdentity;
use broca_protocol::{
    caller::{ControlUnit, RunFinishReason},
    content::{AssistantMessage, ContentBlock, Message, Role, ToolCall, ToolOutput},
    provider::{NormalizedEvent, ProviderControl},
    request::{
        BudgetConfig, BudgetKind, BudgetNudge, CacheRequest, CacheTail, ExecutionMode, ModelHandle,
        StopCondition, Tool, ToolChoice,
    },
    ApprovalResponse, DisplayEvent, FinishReason, GenerationConfig, MessageId, RunId, StepId,
    SubmissionId, ToolCallId, ToolResult, Usage,
};
use broca_wal::{
    DispatchOutcome, FileWal, FramedRecord, NoDispatchReason, RunConfig, Wal, WalError, WalRecord,
};
use serde_json::json;
use tokio::sync::mpsc;

// ───────────────────────── script helpers ─────────────────────────

fn ctrl(c: ProviderControl) -> NormalizedEvent {
    NormalizedEvent::Control(c)
}

fn usage(input: u64, output: u64) -> Usage {
    Usage {
        input_tokens: Some(input),
        output_tokens: Some(output),
        ..Usage::default()
    }
}

/// A step that emits text and no tool calls (loop will finish after it).
fn text_step(text: &str) -> Vec<NormalizedEvent> {
    text_step_with_usage(text, usage(10, 5))
}

fn text_step_with_usage(text: &str, step_usage: Usage) -> Vec<NormalizedEvent> {
    vec![
        ctrl(ProviderControl::StreamStart { warnings: vec![] }),
        NormalizedEvent::Display(DisplayEvent::TextDelta {
            message_id: MessageId("m".into()),
            tool_call_id: None,
            delta: text.into(),
        }),
        ctrl(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId("m".into()),
                content: vec![ContentBlock::Text { text: text.into() }],
            },
        }),
        ctrl(ProviderControl::StepFinished {
            step_id: StepId(0),
            usage: step_usage,
            finish_reason: FinishReason::Stop,
            service_tier_used: None,
            speed_used: None,
        }),
        ctrl(ProviderControl::Finished {
            usage: step_usage,
            finish_reason: FinishReason::Stop,
            retries_used: 0,
        }),
    ]
}

/// A tools-off step truncated by the provider's output-token ceiling.
fn length_step(text: &str) -> Vec<NormalizedEvent> {
    vec![
        ctrl(ProviderControl::StreamStart { warnings: vec![] }),
        ctrl(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId("m-length".into()),
                content: vec![ContentBlock::Text { text: text.into() }],
            },
        }),
        ctrl(ProviderControl::StepFinished {
            step_id: StepId(0),
            usage: usage(10, 4096),
            finish_reason: FinishReason::Length,
            service_tier_used: None,
            speed_used: None,
        }),
        ctrl(ProviderControl::Finished {
            usage: usage(10, 4096),
            finish_reason: FinishReason::Length,
            retries_used: 0,
        }),
    ]
}

/// A step whose assistant message calls the given tools (source order).
fn tool_step(calls: &[(&str, &str, serde_json::Value)]) -> Vec<NormalizedEvent> {
    let content = calls
        .iter()
        .map(|(id, name, input)| {
            ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId((*id).into()),
                tool_name: (*name).into(),
                input: input.clone(),
            })
        })
        .collect();
    vec![
        ctrl(ProviderControl::StreamStart { warnings: vec![] }),
        ctrl(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId("m".into()),
                content,
            },
        }),
        ctrl(ProviderControl::StepFinished {
            step_id: StepId(0),
            usage: usage(10, 8),
            finish_reason: FinishReason::ToolCalls,
            service_tier_used: None,
            speed_used: None,
        }),
        ctrl(ProviderControl::Finished {
            usage: usage(10, 8),
            finish_reason: FinishReason::ToolCalls,
            retries_used: 0,
        }),
    ]
}

/// A step whose provider stream terminates with a NON-auth error (e.g. a permanent
/// provider failure). The loop ends the run as `Error`.
fn error_step(class: broca_protocol::ErrorClass, message: &str) -> Vec<NormalizedEvent> {
    vec![
        ctrl(ProviderControl::StreamStart { warnings: vec![] }),
        ctrl(ProviderControl::Error {
            error: broca_protocol::error::ProviderError {
                provider_subcode: None,
                class,
                refusal: None,
                message: message.into(),
                status: Some(500),
                retry_after_secs: None,
                provider_code: Some("boom_code".into()),
                resets_at_ms: None,
                resets_at_source: None,
                limit_window: None,
                account_ref: None,
                credential_id: None,
                affordable_max_tokens: None,
            },
            retries_used: 0,
        }),
    ]
}

fn session(name: &str) -> BindIdentity {
    BindIdentity {
        project_root: "/tmp/proj".into(),
        harness: "test".into(),
        session: name.into(),
    }
}

fn tmp_lease_store(tag: &str) -> (FileLeaseStore, std::path::PathBuf) {
    let dir = broca_test_support::scratch_root().join(format!(
        "broca-runtest-{}-{}",
        std::process::id(),
        tag
    ));
    let _ = std::fs::remove_dir_all(&dir);
    (FileLeaseStore::new(&dir), dir)
}

#[derive(Default)]
struct FailingProjectionStore {
    calls: AtomicUsize,
}

impl MaterializedStore for FailingProjectionStore {
    fn read_prefix_with_watermark(&self, _session: &BindIdentity) -> Result<StorePrefix, String> {
        Ok(StorePrefix::empty())
    }
}

impl Persister for FailingProjectionStore {
    fn project(&self, _session: &BindIdentity, _records: &[FramedRecord]) -> Result<u64, String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err("injected projection failure".into())
    }
}

struct ReadCountingWalStore {
    inner: FileWalStore,
    reads: Arc<AtomicUsize>,
}

impl ReadCountingWalStore {
    fn new(root: impl Into<std::path::PathBuf>) -> Self {
        Self {
            inner: FileWalStore::new(root),
            reads: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn read_count(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }
}

impl WalStore for ReadCountingWalStore {
    fn open(
        &self,
        session: &BindIdentity,
    ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
        self.inner.open(session)
    }

    fn read_records(
        &self,
        session: &BindIdentity,
    ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.read_records(session)
    }
}

fn seed_idle_run(store: &impl WalStore, request: &RunRequest) {
    let mut wal = store.open(&request.session).expect("open WAL for seed");
    let config = RunConfig {
        model: request.model.clone(),
        tools: request.tools.clone().into(),
        tool_choice: request.tool_choice.clone(),
        generation: request.generation.clone(),
        response_format: Default::default(),
        provider_options: serde_json::Value::Null,
        cache_tiers: serde_json::Value::Null,
        context_epoch: 0,
        renderer_version: 1,
        frozen_render_config: serde_json::Value::Null,
        auth_selection: None,
        tools_were_omitted: false,
        budget: None,
        context_limit: request.context_limit,
        work_class: request.work_class.clone(),
        variant: request.variant.clone(),
        service_tier: None,
        stop_when: Vec::new(),
        context_management: None,
    };
    wal.append(
        1,
        &WalRecord::RunStarted {
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
            run_id: request.run_id.clone(),
            session: request.session.clone(),
            config: Box::new(config),
            input: request.input.clone().into(),
            origin: broca_wal::RunOrigin::Fresh,
            submission_id: None,
        },
        false,
    )
    .expect("seed RunStarted");
    wal.append(
        1,
        &WalRecord::RunFinished {
            ts_ms: None,
            reason: RunFinishReason::Completed,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        },
        true,
    )
    .expect("seed RunFinished");
}

fn resumable_frozen_config(request: &RunRequest, stop_when: Vec<StopCondition>) -> RunConfig {
    RunConfig {
        model: request.model.clone(),
        tools: request.tools.clone().into(),
        tool_choice: request.tool_choice.clone(),
        generation: request.generation.clone(),
        response_format: Default::default(),
        provider_options: serde_json::Value::Null,
        cache_tiers: serde_json::Value::Null,
        context_epoch: 0,
        renderer_version: 1,
        frozen_render_config: json!({ "render_config": "stateless" }),
        auth_selection: None,
        tools_were_omitted: false,
        budget: None,
        context_limit: request.context_limit,
        work_class: request.work_class.clone(),
        variant: request.variant.clone(),
        service_tier: None,
        stop_when,
        context_management: None,
    }
}

fn tool(name: &str, mode: ExecutionMode) -> Tool {
    Tool {
        name: name.into(),
        description: "t".into(),
        input_schema: json!({"type": "object"}),
        module: None,
        execution_mode: mode,
    }
}

fn req(name: &str, tools: Vec<Tool>, stop_when: Vec<StopCondition>) -> RunRequest {
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
        run_id: RunId(format!("run-{name}")),
        session: session(name),
        model: ModelHandle::new("stub", "stub-model"),
        input: vec![],
        tools,
        policy_allowed,
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        stop_when,
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

#[derive(Default)]
struct BorrowObservingPlane {
    manifest_addresses: Mutex<Vec<usize>>,
}

impl BorrowObservingPlane {
    fn manifest_addresses(&self) -> Vec<usize> {
        self.manifest_addresses.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl ToolPlane for BorrowObservingPlane {
    async fn batch_started(
        &self,
        _ctx: &BatchCtx,
        _calls: &[ToolCall],
        manifest: &ToolManifest,
    ) -> Result<(), ToolPlaneError> {
        assert!(manifest.get("tool-0").is_some());
        assert!(manifest.get("tool-63").is_some());
        self.manifest_addresses
            .lock()
            .unwrap()
            .push(manifest as *const ToolManifest as usize);
        Ok(())
    }

    async fn dispatch(&self, _ctx: &BatchCtx, call: &ToolCall) -> DispatchResult {
        DispatchResult::Completed(ToolResult {
            tool_call_id: call.tool_call_id.clone(),
            output: ToolOutput::Text {
                text: "borrowed manifest".into(),
            },
            is_error: false,
        })
    }

    async fn batch_finished(&self, _ctx: &BatchCtx) -> Result<(), ToolPlaneError> {
        Ok(())
    }
}

/// Run the loop and REPLAY the written WAL — for asserting the durable record set.
/// Returns (summary, wal_records).
async fn drive_wal<T, A>(
    request: RunRequest,
    provider: StubProvider,
    plane: &T,
    store: &FileLeaseStore,
    approval: &A,
) -> (Result<broca_core::RunSummary, RunError>, Vec<WalRecord>)
where
    T: broca_core::ToolPlane,
    A: broca_core::ApprovalPolicy,
{
    let wal_dir = broca_test_support::scratch_root().join(format!(
        "broca-runtest-walrec-{}-{}",
        std::process::id(),
        request.run_id.0
    ));
    let _ = std::fs::remove_dir_all(&wal_dir);
    let wal = FileWalStore::new(&wal_dir);
    let wal_path = wal.wal_path(&request.session);

    let (tx, mut rx) = mpsc::channel(4096);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let summary = run_loop(request, &provider, plane, store, approval, &wal, &tx).await;
    drop(tx);
    collector.await.unwrap();

    // Replay the on-disk WAL (the durable truth, not in-memory state).
    let records = FileWal::open(&wal_path)
        .unwrap()
        .replay()
        .unwrap()
        .into_iter()
        .map(|f| f.record)
        .collect();
    let _ = std::fs::remove_dir_all(&wal_dir);
    (summary, records)
}

/// Run the loop, collecting the full OutputStream.
async fn drive<T, A>(
    request: RunRequest,
    provider: StubProvider,
    plane: &T,
    store: &FileLeaseStore,
    approval: &A,
) -> (Result<broca_core::RunSummary, RunError>, Vec<OutputEvent>)
where
    T: broca_core::ToolPlane,
    A: broca_core::ApprovalPolicy,
{
    // WAL co-located with the lease store under a per-run temp dir keyed by run_id
    // (each test uses a distinct session/run, so a fresh dir is fine).
    let wal_dir = broca_test_support::scratch_root().join(format!(
        "broca-runtest-wal-{}-{}",
        std::process::id(),
        request.run_id.0
    ));
    let _ = std::fs::remove_dir_all(&wal_dir);
    let wal = FileWalStore::new(&wal_dir);

    let (tx, mut rx) = mpsc::channel(4096);
    let collector = tokio::spawn(async move {
        let mut v = Vec::new();
        while let Some(e) = rx.recv().await {
            v.push(e);
        }
        v
    });
    let summary = run_loop(request, &provider, plane, store, approval, &wal, &tx).await;
    drop(tx);
    let events = collector.await.unwrap();
    let _ = std::fs::remove_dir_all(&wal_dir);
    (summary, events)
}

/// Run with explicit controls and retain the durable records so a test can compare the live
/// output stream with the control projection subscribers consume.
async fn drive_with_control<T, A>(
    request: RunRequest,
    provider: StubProvider,
    plane: &T,
    store: &FileLeaseStore,
    approval: &A,
    control: RunControl,
) -> (
    Result<broca_core::RunSummary, RunError>,
    Vec<OutputEvent>,
    Vec<FramedRecord>,
)
where
    T: broca_core::ToolPlane,
    A: broca_core::ApprovalPolicy,
{
    let wal_dir = broca_test_support::scratch_root().join(format!(
        "broca-runtest-controlled-wal-{}-{}",
        std::process::id(),
        request.run_id.0
    ));
    let _ = std::fs::remove_dir_all(&wal_dir);
    let wal = FileWalStore::new(&wal_dir);
    let session = request.session.clone();

    let (tx, mut rx) = mpsc::channel(4096);
    let collector = tokio::spawn(async move {
        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(event);
        }
        events
    });
    let summary = run_loop_with_control(
        request, &provider, plane, store, approval, &wal, None, control, &tx,
    )
    .await;
    drop(tx);
    let events = collector.await.unwrap();
    let records = wal.read_records(&session).expect("read controlled-run WAL");
    let _ = std::fs::remove_dir_all(&wal_dir);
    (summary, events, records)
}

#[derive(Clone, Default)]
struct CountingWalState {
    records: Arc<Mutex<Vec<FramedRecord>>>,
    sync_calls: Arc<AtomicUsize>,
}

#[derive(Clone, Default)]
struct CountingWalStore {
    state: CountingWalState,
}

impl CountingWalStore {
    fn sync_calls(&self) -> usize {
        self.state.sync_calls.load(Ordering::SeqCst)
    }

    fn records(&self) -> Vec<WalRecord> {
        self.state
            .records
            .lock()
            .unwrap()
            .iter()
            .map(|f| f.record.clone())
            .collect()
    }
}

struct CountingWal {
    state: CountingWalState,
    next_seq: u64,
}

impl broca_core::WalStore for CountingWalStore {
    fn open(&self, _session: &BindIdentity) -> Result<Box<dyn Wal + Send>, WalError> {
        let next_seq = self
            .state
            .records
            .lock()
            .unwrap()
            .last()
            .map(|record| record.seq + 1)
            .unwrap_or(1);
        Ok(Box::new(CountingWal {
            state: self.state.clone(),
            next_seq,
        }))
    }

    fn read_records(&self, _session: &BindIdentity) -> Result<Vec<FramedRecord>, WalError> {
        Ok(self.state.records.lock().unwrap().clone())
    }
}

impl Wal for CountingWal {
    fn append(&mut self, fence: u64, record: &WalRecord, sync: bool) -> Result<u64, WalError> {
        let seq = self.next_seq;
        self.state.records.lock().unwrap().push(FramedRecord {
            seq,
            fence,
            record: record.clone(),
        });
        self.next_seq += 1;
        if sync {
            self.sync()?;
        }
        Ok(seq)
    }

    fn sync(&self) -> Result<(), WalError> {
        self.state.sync_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn replay(&self) -> Result<Vec<FramedRecord>, WalError> {
        Ok(self.state.records.lock().unwrap().clone())
    }

    fn tail_after_seq(&self, after_seq: u64) -> Result<Vec<FramedRecord>, WalError> {
        Ok(self
            .state
            .records
            .lock()
            .unwrap()
            .iter()
            .filter(|record| record.seq > after_seq)
            .cloned()
            .collect())
    }

    fn last_seq(&self) -> u64 {
        self.next_seq.saturating_sub(1)
    }
}

async fn drive_with_wal_store<T, A, W, P: broca_core::Provider>(
    request: RunRequest,
    provider: P,
    plane: &T,
    store: &FileLeaseStore,
    approval: &A,
    wal_store: &W,
) -> (Result<broca_core::RunSummary, RunError>, Vec<OutputEvent>)
where
    T: broca_core::ToolPlane,
    A: broca_core::ApprovalPolicy,
    W: broca_core::WalStore,
{
    let (tx, mut rx) = mpsc::channel(4096);
    let collector = tokio::spawn(async move {
        let mut v = Vec::new();
        while let Some(e) = rx.recv().await {
            v.push(e);
        }
        v
    });
    let summary = run_loop(request, &provider, plane, store, approval, wal_store, &tx).await;
    drop(tx);
    let events = collector.await.unwrap();
    (summary, events)
}

fn control_units(events: &[OutputEvent]) -> Vec<&ControlUnit> {
    events
        .iter()
        .filter_map(|e| match e {
            OutputEvent::Control(c) => Some(c),
            _ => None,
        })
        .collect()
}

// ───────────────────────── tests ─────────────────────────

#[tokio::test]
async fn single_text_step_finishes_completed_with_display_passthrough() {
    let (store, dir) = tmp_lease_store("text");
    let provider = StubProvider::new(vec![text_step("hello world")]);
    let plane = StubToolPlane::new();
    let (summary, events) = drive(
        req("text", vec![], vec![]),
        provider,
        &plane,
        &store,
        &AllowAll,
    )
    .await;

    let s = summary.expect("run ok");
    assert_eq!(s.reason, RunFinishReason::Completed);
    assert_eq!(s.steps, 1);

    // Display delta forwarded untouched.
    assert!(events.iter().any(|e| matches!(
        e,
        OutputEvent::Display(DisplayEvent::TextDelta { delta, .. }) if delta == "hello world"
    )));
    // Control lane bookends.
    let cu = control_units(&events);
    assert!(matches!(cu.first(), Some(ControlUnit::RunStarted { .. })));
    assert!(matches!(
        cu.last(),
        Some(ControlUnit::RunFinished {
            reason: RunFinishReason::Completed,
            ..
        })
    ));
    // No tool plane activity.
    assert!(plane.log().is_empty());
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn terminal_usage_presence_tracks_completed_model_steps() {
    let no_step_error = error_step(broca_protocol::ErrorClass::Permanent, "provider failed");
    let (no_step_store, no_step_dir) = tmp_lease_store("terminal-usage-no-step");
    let (no_step_summary, no_step_events, no_step_records) = drive_with_control(
        req("terminal-usage-no-step", vec![], vec![]),
        StubProvider::new(vec![no_step_error]),
        &StubToolPlane::new(),
        &no_step_store,
        &AllowAll,
        RunControl::default(),
    )
    .await;
    assert_eq!(no_step_summary.expect("run finishes").steps, 0);
    let no_step_live_usage = control_units(&no_step_events)
        .into_iter()
        .find_map(|unit| match unit {
            ControlUnit::RunFinished { usage, .. } => Some(*usage),
            _ => None,
        })
        .expect("live terminal is emitted");
    assert_eq!(no_step_live_usage, None);
    let no_step_projected_usage = broca_core::project_control(&no_step_records)
        .into_iter()
        .find_map(|(_, unit)| match unit {
            ControlUnit::RunFinished { usage, .. } => Some(usage),
            _ => None,
        })
        .expect("durable terminal is projected");
    assert_eq!(no_step_projected_usage, None);
    assert!(matches!(
        no_step_records.last().map(|record| &record.record),
        Some(WalRecord::RunFinished {
            usage,
            ..
        }) if *usage == Usage::default()
    ));
    let _ = std::fs::remove_dir_all(no_step_dir);

    let (zero_step_store, zero_step_dir) = tmp_lease_store("terminal-usage-zero-step");
    let (zero_step_summary, zero_step_events, zero_step_records) = drive_with_control(
        req("terminal-usage-zero-step", vec![], vec![]),
        StubProvider::new(vec![text_step_with_usage("zero", Usage::default())]),
        &StubToolPlane::new(),
        &zero_step_store,
        &AllowAll,
        RunControl::default(),
    )
    .await;
    assert_eq!(zero_step_summary.expect("run finishes").steps, 1);
    let zero_step_live_usage = control_units(&zero_step_events)
        .into_iter()
        .find_map(|unit| match unit {
            ControlUnit::RunFinished { usage, .. } => Some(*usage),
            _ => None,
        })
        .expect("live terminal is emitted");
    assert_eq!(zero_step_live_usage, Some(Usage::default()));
    let zero_step_projected_usage = broca_core::project_control(&zero_step_records)
        .into_iter()
        .find_map(|(_, unit)| match unit {
            ControlUnit::RunFinished { usage, .. } => Some(usage),
            _ => None,
        })
        .expect("durable terminal is projected");
    assert_eq!(zero_step_projected_usage, Some(Usage::default()));
    assert!(matches!(
        zero_step_records.last().map(|record| &record.record),
        Some(WalRecord::RunFinished {
            usage,
            ..
        }) if *usage == Usage::default()
    ));
    let _ = std::fs::remove_dir_all(zero_step_dir);
}

#[tokio::test]
async fn zero_step_count_fails_fresh_admission_before_provider_call() {
    let (store, dir) = tmp_lease_store("zero-step-count-fresh");
    let provider = StubProvider::new(vec![text_step("must not be billed")]);

    let (result, _) = drive(
        req(
            "zero-step-count-fresh",
            vec![],
            vec![StopCondition::StepCount { n: 0 }],
        ),
        provider.clone(),
        &StubToolPlane::new(),
        &store,
        &AllowAll,
    )
    .await;

    assert!(
        provider.calls().is_empty(),
        "invalid fresh admission must not reach the provider"
    );
    let error = result.expect_err("zero step_count must fail fresh admission");
    assert_eq!(
        error.admission_error_class(),
        broca_protocol::ErrorClass::Permanent
    );
    match error {
        RunError::Config(message) => assert_eq!(
            message,
            "stop_when[].n must be at least 1 for type 'step_count'"
        ),
        other => panic!("expected typed Config error, got {other:?}"),
    }
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn zero_context_limit_fails_fresh_admission_before_provider_call() {
    let (store, dir) = tmp_lease_store("zero-context-limit-fresh");
    let provider = StubProvider::new(vec![text_step("must not be billed")]);
    let mut request = req("zero-context-limit-fresh", vec![], vec![]);
    request.context_limit = Some(0);

    let (result, _) = drive(
        request,
        provider.clone(),
        &StubToolPlane::new(),
        &store,
        &AllowAll,
    )
    .await;

    assert!(
        provider.calls().is_empty(),
        "invalid fresh admission must not reach the provider"
    );
    let error = result.expect_err("an explicit zero context limit must fail fresh admission");
    assert_eq!(
        error.admission_error_class(),
        broca_protocol::ErrorClass::Permanent
    );
    match error {
        RunError::Config(message) => assert_eq!(
            message,
            "context_limit=0 is invalid; omit context_limit to use the catalog value, or supply a value of at least 1"
        ),
        other => panic!("expected typed Config error, got {other:?}"),
    }
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn nonzero_step_count_is_admitted() {
    let (store, dir) = tmp_lease_store("nonzero-step-count-fresh");
    let provider = StubProvider::new(vec![text_step("one allowed call")]);

    let (result, _) = drive(
        req(
            "nonzero-step-count-fresh",
            vec![],
            vec![StopCondition::StepCount { n: 1 }],
        ),
        provider.clone(),
        &StubToolPlane::new(),
        &store,
        &AllowAll,
    )
    .await;

    let summary = result.expect("nonzero step_count must admit");
    assert_eq!(summary.reason, RunFinishReason::MaxSteps);
    assert_eq!(summary.steps, 1);
    assert_eq!(
        provider.calls().len(),
        1,
        "the admitted run reaches the provider"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn zero_step_count_in_frozen_config_remains_resumable() {
    let (lease_store, lease_dir) = tmp_lease_store("zero-step-count-resume");
    let wal_dir = broca_test_support::scratch_root().join(format!(
        "broca-runtest-zero-step-resume-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&wal_dir);
    let wal_store = FileWalStore::new(&wal_dir);
    let request = req(
        "zero-step-count-resume",
        vec![],
        vec![StopCondition::StepCount { n: 0 }],
    );

    {
        let mut wal = wal_store
            .open(&request.session)
            .expect("open legacy WAL for seed");
        wal.append(
            1,
            &WalRecord::RunStarted {
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
                run_id: request.run_id.clone(),
                session: request.session.clone(),
                config: Box::new(resumable_frozen_config(
                    &request,
                    vec![StopCondition::StepCount { n: 0 }],
                )),
                input: request.input.clone().into(),
                origin: broca_wal::RunOrigin::Fresh,
                submission_id: None,
            },
            true,
        )
        .expect("seed legacy RunStarted with zero step_count");
    }

    let provider = StubProvider::new(vec![text_step("legacy resume")]);
    let (tx, mut rx) = mpsc::channel(64);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let result = run_loop(
        request,
        &provider,
        &StubToolPlane::new(),
        &lease_store,
        &AllowAll,
        &wal_store,
        &tx,
    )
    .await;
    drop(tx);
    collector.await.unwrap();

    let summary = result.expect("a frozen zero step_count must not wedge resume admission");
    assert_eq!(summary.reason, RunFinishReason::MaxSteps);
    assert_eq!(summary.steps, 1);
    assert_eq!(
        provider.calls().len(),
        1,
        "resume must use the previously admitted frozen config"
    );
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::remove_dir_all(lease_dir);
}

#[tokio::test]
async fn zero_context_limit_in_frozen_config_remains_resumable() {
    let (lease_store, lease_dir) = tmp_lease_store("zero-context-limit-resume");
    let wal_dir = broca_test_support::scratch_root().join(format!(
        "broca-runtest-zero-context-limit-resume-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&wal_dir);
    let wal_store = FileWalStore::new(&wal_dir);
    let mut request = req("zero-context-limit-resume", vec![], vec![]);
    request.context_limit = Some(0);

    {
        let mut wal = wal_store
            .open(&request.session)
            .expect("open legacy WAL for seed");
        wal.append(
            1,
            &WalRecord::RunStarted {
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
                run_id: request.run_id.clone(),
                session: request.session.clone(),
                config: Box::new(resumable_frozen_config(&request, Vec::new())),
                input: request.input.clone().into(),
                origin: broca_wal::RunOrigin::Fresh,
                submission_id: None,
            },
            true,
        )
        .expect("seed legacy RunStarted with zero context limit");
    }

    let provider = StubProvider::new(vec![text_step("legacy resume")]);
    let (tx, mut rx) = mpsc::channel(64);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let result = run_loop(
        request,
        &provider,
        &StubToolPlane::new(),
        &lease_store,
        &AllowAll,
        &wal_store,
        &tx,
    )
    .await;
    drop(tx);
    collector.await.unwrap();

    let summary = result.expect("a frozen zero context limit must not wedge resume admission");
    assert_eq!(summary.reason, RunFinishReason::Completed);
    assert_eq!(summary.steps, 1);
    assert_eq!(
        provider.calls().len(),
        1,
        "resume must use the previously admitted frozen config"
    );
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::remove_dir_all(lease_dir);
}

/// The caller declaring that its final message differs next request.
fn volatile_tail_cache() -> CacheRequest {
    CacheRequest {
        role: None,
        ttl: None,
        key: None,
        tail: Some(CacheTail::Volatile),
        mode: None,
    }
}

/// The declaration says "do not spend a breakpoint on my last message". On a run that can call
/// tools that breakpoint is the whole mechanism for step-to-step reuse, so honouring the
/// declaration would quietly degrade the one leg where marker caching already pays. Refuse the
/// combination instead of silently picking one of the two meanings.
#[tokio::test]
async fn volatile_cache_tail_with_tools_fails_fresh_admission_before_provider_call() {
    let (store, dir) = tmp_lease_store("volatile-tail-with-tools");
    let provider = StubProvider::new(vec![text_step("must not be billed")]);
    let mut request = req(
        "volatile-tail-with-tools",
        vec![tool("edit", ExecutionMode::Parallel)],
        vec![],
    );
    request.cache = Some(volatile_tail_cache());

    let (result, _) = drive(
        request,
        provider.clone(),
        &StubToolPlane::new(),
        &store,
        &AllowAll,
    )
    .await;

    assert!(
        provider.calls().is_empty(),
        "invalid fresh admission must not reach the provider"
    );
    let error = result.expect_err("a volatile tail declared alongside tools must fail admission");
    assert_eq!(
        error.admission_error_class(),
        broca_protocol::ErrorClass::Permanent
    );
    match error {
        RunError::Config(message) => {
            assert!(
                message.contains("cache.tail") && message.contains("tools"),
                "the message names the conflict: {message}"
            );
            assert!(
                message.contains("drop cache.tail or drop tools"),
                "the message names the action: {message}"
            );
        }
        other => panic!("expected typed Config error, got {other:?}"),
    }
    let _ = std::fs::remove_dir_all(dir);
}

/// The other half of the refusal's scope: the declaration is exactly what a tool-less run
/// wants, so it must still admit.
#[tokio::test]
async fn volatile_cache_tail_without_tools_is_admitted() {
    let (store, dir) = tmp_lease_store("volatile-tail-no-tools");
    let provider = StubProvider::new(vec![text_step("one call, stable prefix")]);
    let mut request = req("volatile-tail-no-tools", vec![], vec![]);
    request.cache = Some(volatile_tail_cache());

    let (result, _) = drive(
        request,
        provider.clone(),
        &StubToolPlane::new(),
        &store,
        &AllowAll,
    )
    .await;

    let summary = result.expect("a tool-less volatile tail declaration must admit");
    assert_eq!(summary.reason, RunFinishReason::Completed);
    assert_eq!(provider.calls().len(), 1);
    let _ = std::fs::remove_dir_all(dir);
}

/// The refusal is a FRESH-admission rule only. A lineage admitted before it existed (or one
/// whose tools were added by a caller that also sends the declaration) must still resume off
/// its frozen config -- scoping the check too broadly would wedge live runs that are already
/// mid-flight, which is a far worse failure than the one being prevented.
#[tokio::test]
async fn volatile_cache_tail_with_tools_in_frozen_config_remains_resumable() {
    let (lease_store, lease_dir) = tmp_lease_store("volatile-tail-resume");
    let wal_dir = broca_test_support::scratch_root().join(format!(
        "broca-runtest-volatile-tail-resume-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&wal_dir);
    let wal_store = FileWalStore::new(&wal_dir);
    let mut request = req(
        "volatile-tail-resume",
        vec![tool("edit", ExecutionMode::Parallel)],
        vec![],
    );
    request.cache = Some(volatile_tail_cache());

    {
        let mut wal = wal_store
            .open(&request.session)
            .expect("open legacy WAL for seed");
        wal.append(
            1,
            &WalRecord::RunStarted {
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
                run_id: request.run_id.clone(),
                session: request.session.clone(),
                config: Box::new(resumable_frozen_config(&request, Vec::new())),
                input: request.input.clone().into(),
                origin: broca_wal::RunOrigin::Fresh,
                submission_id: None,
            },
            true,
        )
        .expect("seed legacy RunStarted with tools and a volatile tail declaration");
    }

    let provider = StubProvider::new(vec![text_step("legacy resume")]);
    let (tx, mut rx) = mpsc::channel(64);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let result = run_loop(
        request,
        &provider,
        &StubToolPlane::new(),
        &lease_store,
        &AllowAll,
        &wal_store,
        &tx,
    )
    .await;
    drop(tx);
    collector.await.unwrap();

    let summary =
        result.expect("a frozen volatile tail alongside tools must not wedge resume admission");
    assert_eq!(summary.reason, RunFinishReason::Completed);
    assert_eq!(summary.steps, 1);
    assert_eq!(
        provider.calls().len(),
        1,
        "resume must use the previously admitted frozen config"
    );
    let _ = std::fs::remove_dir_all(wal_dir);
    let _ = std::fs::remove_dir_all(lease_dir);
}

#[tokio::test]
async fn required_tool_choice_without_tools_fails_before_provider_call() {
    let (store, dir) = tmp_lease_store("required-without-tools");
    let provider = StubProvider::new(vec![text_step("constraint was silently dropped")]);
    let mut request = req("required-without-tools", vec![], vec![]);
    request.tool_choice = ToolChoice::Required;

    let (result, _) = drive(
        request,
        provider.clone(),
        &StubToolPlane::new(),
        &store,
        &AllowAll,
    )
    .await;

    assert!(
        provider.calls().is_empty(),
        "an unsatisfiable required choice must not be downgraded into an unconstrained model call"
    );
    assert!(
        matches!(result, Err(RunError::Config(message)) if message.contains("tool_choice 'required'")),
        "an unsatisfiable required choice must fail admission"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn named_tool_choice_with_empty_filtered_list_fails_before_provider_call() {
    let (store, dir) = tmp_lease_store("named-empty-filtered");
    let provider = StubProvider::new(vec![text_step("constraint was silently dropped")]);
    let mut request = req("named-empty-filtered", vec![], vec![]);
    request.tool_choice = ToolChoice::Tool {
        name: "get_time".into(),
    };

    let (result, _) = drive(
        request,
        provider.clone(),
        &StubToolPlane::new(),
        &store,
        &AllowAll,
    )
    .await;

    assert!(
        provider.calls().is_empty(),
        "an unsatisfiable named choice must not be downgraded into an unconstrained model call"
    );
    assert!(
        matches!(
            &result,
            Err(RunError::Config(message))
                if message.contains("tool_choice names 'get_time'")
                    && message.contains("policy-allowed")
        ),
        "an unsatisfiable named choice must fail admission: {result:?}"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn named_tool_choice_absent_from_filtered_list_fails_before_provider_call() {
    let (store, dir) = tmp_lease_store("named-absent-filtered");
    let provider = StubProvider::new(vec![text_step("constraint was silently dropped")]);
    // Both tools are on the request, but policy keeps only get_time — so the filtered
    // list is non-empty and does not contain the named choice.
    let mut request = req(
        "named-absent-filtered",
        vec![
            tool("get_time", ExecutionMode::Parallel),
            tool("missing_tool", ExecutionMode::Parallel),
        ],
        vec![],
    );
    request.policy_allowed = ["get_time".to_string()].into_iter().collect();
    request.tool_choice = ToolChoice::Tool {
        name: "missing_tool".into(),
    };

    let (result, _) = drive(
        request,
        provider.clone(),
        &StubToolPlane::new(),
        &store,
        &AllowAll,
    )
    .await;

    assert!(
        provider.calls().is_empty(),
        "a named choice for a tool outside the filtered list must not reach the provider"
    );
    assert!(
        matches!(
            &result,
            Err(RunError::Config(message))
                if message.contains("tool_choice names 'missing_tool'")
                    && message.contains("policy-allowed")
        ),
        "a named choice for an absent tool must fail admission: {result:?}"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn named_tool_choice_present_in_filtered_list_is_admitted() {
    let (store, dir) = tmp_lease_store("named-present-filtered");
    let provider = StubProvider::new(vec![
        tool_step(&[("toolu_1", "get_time", json!({}))]),
        text_step("done"),
    ]);
    let plane = StubToolPlane::new().with_canned_text("get_time", "12:00");
    let mut request = req(
        "named-present-filtered",
        vec![tool("get_time", ExecutionMode::Parallel)],
        vec![],
    );
    request.tool_choice = ToolChoice::Tool {
        name: "get_time".into(),
    };

    let (result, _) = drive(request, provider.clone(), &plane, &store, &AllowAll).await;

    let summary = result.expect("named choice for a filtered tool must be admitted");
    assert_eq!(summary.reason, RunFinishReason::Completed);
    assert_eq!(summary.steps, 2);
    assert_eq!(
        provider.calls().len(),
        2,
        "run must proceed to the provider"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn length_truncated_step_is_surfaced_on_run_terminal() {
    let (store, dir) = tmp_lease_store("length-terminal");
    let provider = StubProvider::new(vec![length_step("{\"partial\":")]);
    let plane = StubToolPlane::new();
    let (summary, events) = drive(
        req("length-terminal", vec![], vec![]),
        provider,
        &plane,
        &store,
        &AllowAll,
    )
    .await;

    assert_eq!(summary.expect("run ok").reason, RunFinishReason::Completed);
    assert!(matches!(
        control_units(&events).last(),
        Some(ControlUnit::RunFinished {
            reason: RunFinishReason::Completed,
            final_step_finish_reason: Some(FinishReason::Length),
            ..
        })
    ));
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn tool_call_turn_dispatches_batch_bracketed_then_finishes() {
    let (store, dir) = tmp_lease_store("tool");
    // Step 1 calls get_time; step 2 is the model's text reply after the result.
    let provider = StubProvider::new(vec![
        tool_step(&[("toolu_1", "get_time", json!({"timezone": "Europe/Madrid"}))]),
        text_step("It is noon in Madrid."),
    ]);
    let plane = StubToolPlane::new().with_canned_text("get_time", "12:00");
    let (summary, events) = drive(
        req(
            "tool",
            vec![tool("get_time", ExecutionMode::Parallel)],
            vec![],
        ),
        provider,
        &plane,
        &store,
        &AllowAll,
    )
    .await;

    let s = summary.expect("run ok");
    assert_eq!(s.reason, RunFinishReason::Completed);
    assert_eq!(s.steps, 2);

    // Batch bracketing + single execution (fence).
    assert_eq!(
        plane.log(),
        vec![
            StubEvent::BatchStarted {
                call_ids: vec![ToolCallId("toolu_1".into())]
            },
            StubEvent::Dispatched(ToolCallId("toolu_1".into())),
            StubEvent::BatchFinished,
        ]
    );
    assert_eq!(plane.exec_count(&ToolCallId("toolu_1".into())), 1);

    // The tool result reached the caller with the canned output.
    let cu = control_units(&events);
    assert!(cu.iter().any(|c| matches!(
        c,
        ControlUnit::ToolResult { result } if matches!(&result.output, ToolOutput::Text { text } if text == "12:00")
    )));
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn repetition_guidance_reaches_the_model() {
    let (store, dir) = tmp_lease_store("repetition-guidance");
    let provider = StubProvider::new(vec![
        tool_step(&[("read-1", "read", json!({"filePath": "missing.rs"}))]),
        tool_step(&[("read-2", "read", json!({"filePath": "missing.rs"}))]),
        text_step("using another approach"),
    ]);
    let recorded = provider.clone();
    let plane =
        StubToolPlane::new().with_tool("read", |_| StubReply::ExecError("file not found".into()));

    drive(
        req(
            "repetition-guidance",
            vec![tool("read", ExecutionMode::Sequential)],
            vec![],
        ),
        provider,
        &plane,
        &store,
        &AllowAll,
    )
    .await
    .0
    .expect("run ok");

    let calls = recorded.calls();
    assert_eq!(calls.len(), 3);
    let rendered = String::from_utf8(canonical_request_bytes(&calls[2])).unwrap();
    assert!(
        rendered.contains(
            "read has now failed 2 times with identical arguments. Read the error and change the arguments or the approach; repeating this call unchanged will fail the same way."
        ),
        "the provider's next prompt must contain the repetition guidance"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn repetition_success_resets_failure_count() {
    let (store, dir) = tmp_lease_store("repetition-success-reset");
    let provider = StubProvider::new(vec![
        tool_step(&[("read-fail-1", "read", json!({"filePath": "maybe.rs"}))]),
        tool_step(&[("read-fail-2", "read", json!({"filePath": "maybe.rs"}))]),
        tool_step(&[("read-success", "read", json!({"filePath": "maybe.rs"}))]),
        tool_step(&[("read-fail-4", "read", json!({"filePath": "maybe.rs"}))]),
        text_step("done"),
    ]);
    let recorded = provider.clone();
    let plane = StubToolPlane::new().with_tool("read", |call| {
        if call.tool_call_id.0 == "read-success" {
            StubReply::Output(ToolOutput::Text {
                text: "contents".into(),
            })
        } else {
            StubReply::ExecError("file not found".into())
        }
    });

    drive(
        req(
            "repetition-success-reset",
            vec![tool("read", ExecutionMode::Sequential)],
            vec![],
        ),
        provider,
        &plane,
        &store,
        &AllowAll,
    )
    .await
    .0
    .expect("run ok");

    let calls = recorded.calls();
    assert_eq!(calls.len(), 5);
    let final_prompt = &calls[4].prompt;
    let rendered = String::from_utf8(canonical_request_bytes(&calls[4])).unwrap();
    assert!(
        rendered.contains("read has now failed 2 times with identical arguments"),
        "the second failure must prove the guard fired before the success"
    );
    assert!(
        !rendered.contains("failed 3 times"),
        "the failure after a success starts a new count"
    );
    let latest_result = final_prompt
        .iter()
        .rev()
        .find(|message| message.role == Role::Tool)
        .and_then(|message| {
            message.content.iter().find_map(|block| match block {
                ContentBlock::ToolResult(result) => Some(result),
                _ => None,
            })
        })
        .expect("fourth call has a model-visible result");
    assert!(matches!(
        &latest_result.output,
        ToolOutput::Error { message } if message == "file not found"
    ));
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn quiet_repetition_guard_leaves_rendered_prompt_unchanged() {
    let (store, dir) = tmp_lease_store("repetition-quiet");
    let provider = StubProvider::new(vec![
        tool_step(&[("read-once", "read", json!({"filePath": "missing.rs"}))]),
        text_step("try something else"),
    ]);
    let recorded = provider.clone();
    let plane =
        StubToolPlane::new().with_tool("read", |_| StubReply::ExecError("file not found".into()));

    drive(
        req(
            "repetition-quiet",
            vec![tool("read", ExecutionMode::Sequential)],
            vec![],
        ),
        provider,
        &plane,
        &store,
        &AllowAll,
    )
    .await
    .0
    .expect("run ok");

    let calls = recorded.calls();
    assert_eq!(calls.len(), 2);
    let actual = canonical_request_bytes(&calls[1]);
    let mut without_guard = calls[1].clone();
    let result = without_guard
        .prompt
        .iter_mut()
        .rev()
        .find(|message| message.role == Role::Tool)
        .and_then(|message| {
            message.content.iter_mut().find_map(|block| match block {
                ContentBlock::ToolResult(result) => Some(result),
                _ => None,
            })
        })
        .expect("the baseline contains the raw tool result");
    result.output = ToolOutput::Error {
        message: "file not found".into(),
    };
    assert_eq!(
        actual,
        canonical_request_bytes(&without_guard),
        "a quiet guard must render byte-identically to the raw, guard-absent result"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn multi_step_tool_batches_borrow_one_large_manifest() {
    let (store, dir) = tmp_lease_store("borrowed-tool-manifest");
    let tools = (0..64)
        .map(|index| Tool {
            name: format!("tool-{index}"),
            description: format!("tool {index}"),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "payload": {
                        "type": "string",
                        "description": "x".repeat(2048),
                    }
                }
            }),
            module: Some(format!("module-{index}")),
            execution_mode: ExecutionMode::Parallel,
        })
        .collect();
    let provider = StubProvider::new(vec![
        tool_step(&[("toolu_1", "tool-0", json!({}))]),
        tool_step(&[("toolu_2", "tool-0", json!({}))]),
        tool_step(&[("toolu_3", "tool-0", json!({}))]),
        text_step("done"),
    ]);
    let plane = BorrowObservingPlane::default();

    let (summary, _) = drive(
        req("borrowed-tool-manifest", tools, vec![]),
        provider,
        &plane,
        &store,
        &AllowAll,
    )
    .await;

    assert_eq!(summary.expect("run ok").steps, 4);
    let addresses = plane.manifest_addresses();
    assert_eq!(addresses.len(), 3, "one routing callback per tool batch");
    assert!(
        addresses.windows(2).all(|pair| pair[0] == pair[1]),
        "every batch must borrow the same run-owned manifest"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn module_gating_skips_live_payload_clones_but_preserves_display_and_projection() {
    let (store, dir) = tmp_lease_store("module-control-gating");
    let provider = StubProvider::new(vec![
        tool_step(&[("toolu_gate", "lookup", json!({"query": "broca"}))]),
        text_step("projected answer"),
    ]);
    let plane = StubToolPlane::new().with_canned_text("lookup", "projected result");
    let control = RunControl {
        emit_control_payloads: false,
        ..RunControl::default()
    };
    let (summary, events, records) = drive_with_control(
        req(
            "module-control-gating",
            vec![tool("lookup", ExecutionMode::Parallel)],
            vec![],
        ),
        provider,
        &plane,
        &store,
        &AllowAll,
        control,
    )
    .await;

    assert_eq!(summary.expect("module-style run succeeds").steps, 2);
    assert!(events.iter().any(|event| matches!(
        event,
        OutputEvent::Display(DisplayEvent::TextDelta { delta, .. })
            if delta == "projected answer"
    )));
    assert!(control_units(&events).iter().all(|unit| !matches!(
        unit,
        ControlUnit::AssistantMessage { .. }
            | ControlUnit::ToolCall { .. }
            | ControlUnit::ToolResult { .. }
    )));
    assert!(control_units(&events)
        .iter()
        .any(|unit| matches!(unit, ControlUnit::RunStarted { .. })));
    assert!(control_units(&events)
        .iter()
        .any(|unit| matches!(unit, ControlUnit::StepStarted { .. })));
    assert!(control_units(&events)
        .iter()
        .any(|unit| matches!(unit, ControlUnit::StepFinished { .. })));
    assert!(control_units(&events)
        .iter()
        .any(|unit| matches!(unit, ControlUnit::RunFinished { .. })));

    let projected: Vec<_> = broca_core::project_control(&records)
        .into_iter()
        .map(|(_, unit)| unit)
        .collect();
    assert!(projected
        .iter()
        .any(|unit| matches!(unit, ControlUnit::AssistantMessage { .. })));
    assert!(projected
        .iter()
        .any(|unit| matches!(unit, ControlUnit::ToolCall { .. })));
    assert!(projected
        .iter()
        .any(|unit| matches!(unit, ControlUnit::ToolResult { .. })));

    // Revert check: the default remains the direct-consumer behavior and emits all payload units.
    let provider = StubProvider::new(vec![
        tool_step(&[("toolu_direct", "lookup", json!({"query": "broca"}))]),
        text_step("direct answer"),
    ]);
    let plane = StubToolPlane::new().with_canned_text("lookup", "direct result");
    let (summary, events, _) = drive_with_control(
        req(
            "direct-control-default",
            vec![tool("lookup", ExecutionMode::Parallel)],
            vec![],
        ),
        provider,
        &plane,
        &store,
        &AllowAll,
        RunControl::default(),
    )
    .await;
    assert_eq!(summary.expect("direct-style run succeeds").steps, 2);
    let units = control_units(&events);
    assert!(units
        .iter()
        .any(|unit| matches!(unit, ControlUnit::AssistantMessage { .. })));
    assert!(units
        .iter()
        .any(|unit| matches!(unit, ControlUnit::ToolCall { .. })));
    assert!(units
        .iter()
        .any(|unit| matches!(unit, ControlUnit::ToolResult { .. })));

    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn multi_step_run_stamps_the_durable_prompt_without_changing_rendered_bytes() {
    let (store, dir) = tmp_lease_store("in-place-ordinals");
    let provider = StubProvider::new(vec![
        tool_step(&[("toolu_stamp", "lookup", json!({"query": "broca"}))]),
        text_step("done"),
    ]);
    let plane = StubToolPlane::new().with_canned_text("lookup", "result");
    let wal = CountingWalStore::default();
    let mut request = req(
        "in-place-ordinals",
        vec![tool("lookup", ExecutionMode::Parallel)],
        vec![],
    );
    request.input = vec![Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: "find broca".into(),
        }],
    )];

    let (summary, _) =
        drive_with_wal_store(request, provider.clone(), &plane, &store, &AllowAll, &wal).await;
    assert_eq!(summary.expect("run ok").steps, 2);

    let calls = provider.calls();
    assert_eq!(calls.len(), 2);
    assert_eq!(
        calls[1]
            .prompt
            .iter()
            .map(|message| message.ordinal)
            .collect::<Vec<_>>(),
        vec![Some(0), Some(1), Some(2)]
    );
    let mut full_stamp_reference = calls[1].prompt.clone();
    for message in &mut full_stamp_reference {
        message.ordinal = None;
    }
    broca_protocol::content::stamp_ordinals(&mut full_stamp_reference);
    assert_eq!(
        calls[1].prompt, full_stamp_reference,
        "the live incremental path must assign exactly the full-stamp ordinals"
    );

    let mut unstamped = calls[1].clone();
    for message in &mut unstamped.prompt {
        message.ordinal = None;
    }
    assert_eq!(
        canonical_request_bytes(&calls[1]),
        canonical_request_bytes(&unstamped),
        "serde-skipped ordinals must not alter the canonical provider request"
    );

    let durable = replay(&wal.records()).expect("replay durable run");
    assert!(
        durable
            .prompt
            .iter()
            .enumerate()
            .all(|(index, message)| message.ordinal == Some(index as u64)),
        "the durable prompt must retain positional ordinals after the run"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn projection_failure_is_non_fatal_to_the_run() {
    let (lease_store, dir) = tmp_lease_store("projection-failure");
    let wal_store = FileWalStore::new(dir.join("wal"));
    let projection_store = Arc::new(FailingProjectionStore::default());
    let provider = StubProvider::new(vec![
        tool_step(&[("toolu_1", "get_time", json!({"timezone": "UTC"}))]),
        text_step("done"),
    ]);
    let plane = StubToolPlane::new().with_canned_text("get_time", "12:00");
    let (tx, mut rx) = mpsc::channel(512);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    let summary = run_loop_with_store(
        req(
            "projection-failure",
            vec![tool("get_time", ExecutionMode::Parallel)],
            vec![],
        ),
        &provider,
        &plane,
        &lease_store,
        &AllowAll,
        &wal_store,
        Some(projection_store.clone() as Arc<dyn broca_core::SessionStore>),
        &tx,
    )
    .await
    .expect("projection failure must not fail a WAL-authoritative run");
    drop(tx);
    collector.await.unwrap();

    assert_eq!(summary.reason, RunFinishReason::Completed);
    assert_eq!(summary.steps, 2);
    assert!(
        projection_store.calls.load(Ordering::SeqCst) >= 1,
        "the async projector attempted the latest durable boundary"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[derive(Clone)]
struct AttemptRecordingStub(StubProvider);

#[async_trait::async_trait]
impl broca_core::Provider for AttemptRecordingStub {
    async fn step(
        &self,
        step_id: StepId,
        opts: &broca_protocol::CallOptions,
    ) -> mpsc::Receiver<NormalizedEvent> {
        let bytes = canonical_request_bytes(opts);
        let mut events = broca_core::Provider::step(&self.0, step_id, opts).await;
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            let attempt = broca_protocol::provider::ModelAttempt {
                content_progress: Default::default(),
                attempt: 0,
                request_body_len: bytes.len() as u64,
                request_body_sha256: broca_core::request_digest_hex(&bytes),
                http_status: Some(200),
                content_type: None,
                response_body_len: 0,
                response_body_sha256: broca_core::request_digest_hex(&[]),
                response_body_cap: None,
                outcome: broca_protocol::provider::AttemptOutcome::Complete,
                ts_ms: 0,
                sent_at_ms: None,
                transport: None,
                websocket_request: None,
                websocket_close_code: None,
                input_transformations: None,
                input_transformations_unmeasured: false,
                account_observation: Default::default(),
                credential_refresh: None,
                served_account_ref: None,
            };
            let _ = tx
                .send(ctrl(ProviderControl::AttemptFinished { attempt }))
                .await;
            while let Some(event) = events.recv().await {
                if tx.send(event).await.is_err() {
                    break;
                }
            }
        });
        rx
    }
}

#[derive(Clone)]
struct HeldAttemptProvider {
    outcome: broca_protocol::AttemptOutcome,
    held: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    wal: CountingWalStore,
    baseline_syncs: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl broca_core::Provider for HeldAttemptProvider {
    async fn step(
        &self,
        _step_id: StepId,
        opts: &broca_protocol::CallOptions,
    ) -> mpsc::Receiver<NormalizedEvent> {
        let bytes = canonical_request_bytes(opts);
        self.baseline_syncs
            .store(self.wal.sync_calls(), Ordering::SeqCst);
        let attempt = broca_protocol::ModelAttempt {
            content_progress: Default::default(),
            attempt: 0,
            request_body_len: bytes.len() as u64,
            request_body_sha256: broca_core::request_digest_hex(&bytes),
            http_status: Some(200),
            content_type: None,
            response_body_len: 0,
            response_body_sha256: broca_core::request_digest_hex(&[]),
            response_body_cap: None,
            outcome: self.outcome.clone(),
            ts_ms: 0,
            sent_at_ms: None,
            transport: None,
            websocket_request: None,
            websocket_close_code: None,
            input_transformations: None,
            input_transformations_unmeasured: false,
            account_observation: Default::default(),
            credential_refresh: None,
            served_account_ref: None,
        };
        let held = self.held.clone();
        let release = self.release.clone();
        let retry = self.outcome != broca_protocol::AttemptOutcome::Complete;
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            tx.send(ctrl(ProviderControl::AttemptFinished {
                attempt: attempt.clone(),
            }))
            .await
            .unwrap();
            // Chunk errors are retried by the HTTP transport. Model the second attempt
            // as held open by withholding its completion and the step's terminal events.
            // The test checks durability before releasing those events, so it cannot
            // accidentally rely on the run terminal's sync to persist the failed attempt.
            held.notify_one();
            release.notified().await;
            if retry {
                let mut complete = attempt;
                complete.attempt = 1;
                complete.outcome = broca_protocol::AttemptOutcome::Complete;
                tx.send(ctrl(ProviderControl::AttemptFinished { attempt: complete }))
                    .await
                    .unwrap();
            }
            for mut event in text_step("done") {
                if let NormalizedEvent::Control(ProviderControl::Finished {
                    retries_used, ..
                }) = &mut event
                {
                    *retries_used = u32::from(retry);
                }
                tx.send(event).await.unwrap();
            }
        });
        rx
    }
}

async fn check_held_attempt_durability(outcome: broca_protocol::AttemptOutcome, expect_sync: bool) {
    let wal = CountingWalStore::default();
    let request = req(
        if expect_sync {
            "held-retry"
        } else {
            "held-complete"
        },
        vec![],
        vec![],
    );
    let writer = WalHandle::spawn(wal.open(&request.session).unwrap(), 1).unwrap();
    let head = writer.durable_head();
    let provider = HeldAttemptProvider {
        outcome,
        held: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
        wal: wal.clone(),
        baseline_syncs: Arc::new(AtomicUsize::new(0)),
    };
    let plane = StubToolPlane::new();
    let (tx, mut rx) = mpsc::channel(512);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let run = broca_core::run_loop_on_writer_admitted(
        request,
        &writer,
        StartMode::FreshOrResume,
        &provider,
        &plane,
        &AllowAll,
        &wal,
        None,
        RunControl::default(),
        &tx,
        None,
    );
    let inspect = async {
        provider.held.notified().await;
        let seq = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if let Some(seq) = wal.state.records.lock().unwrap().iter().find_map(|frame| {
                    matches!(frame.record, WalRecord::ModelAttemptFinished { .. })
                        .then_some(frame.seq)
                }) {
                    break seq;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("attempt must be appended while provider is held");
        if expect_sync {
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                while head.load(Ordering::Acquire) < seq {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("failed attempt must be durable while retry remains held open");
            assert_eq!(
                wal.sync_calls(),
                provider.baseline_syncs.load(Ordering::SeqCst) + 1
            );
        } else {
            assert!(
                head.load(Ordering::Acquire) < seq,
                "complete attempt stays coalesced until step terminal"
            );
            assert_eq!(
                wal.sync_calls(),
                provider.baseline_syncs.load(Ordering::SeqCst)
            );
        }
        provider.release.notify_one();
    };
    let (result, ()) = tokio::join!(run, inspect);
    assert_eq!(result.unwrap().reason, RunFinishReason::Completed);
    drop(tx);
    collector.await.unwrap();
    let _ = writer.shutdown().await;
    assert_eq!(
        wal.sync_calls(),
        provider.baseline_syncs.load(Ordering::SeqCst) + 1 + usize::from(expect_sync),
        "only failed attempts add a barrier beyond the existing run terminal"
    );
}

#[tokio::test]
async fn failed_attempt_is_durable_while_retry_is_held_open() {
    check_held_attempt_durability(broca_protocol::AttemptOutcome::ChunkError, true).await;
}

#[tokio::test]
async fn complete_attempt_keeps_existing_sync_barriers() {
    check_held_attempt_durability(broca_protocol::AttemptOutcome::Complete, false).await;
}

#[tokio::test]
async fn duplicate_tool_call_ids_dispatch_once_and_receive_complete_results() {
    let (store, dir) = tmp_lease_store("duplicate-tool-id");
    let provider = StubProvider::new(vec![
        tool_step(&[
            ("toolu_dup", "edit", json!({"path": "first"})),
            ("toolu_dup", "edit", json!({"path": "duplicate"})),
        ]),
        text_step("done"),
    ]);
    let plane = StubToolPlane::new().with_canned_text("edit", "edited");
    let wal = CountingWalStore::default();

    let (summary, events) = drive_with_wal_store(
        req(
            "duplicate-tool-id",
            vec![tool("edit", ExecutionMode::Parallel)],
            vec![],
        ),
        AttemptRecordingStub(provider.clone()),
        &plane,
        &store,
        &AllowAll,
        &wal,
    )
    .await;

    assert!(summary.is_ok());
    assert_eq!(plane.exec_count(&ToolCallId("toolu_dup".into())), 1);
    assert_eq!(
        plane.log(),
        vec![
            StubEvent::BatchStarted {
                call_ids: vec![
                    ToolCallId("toolu_dup".into()),
                    ToolCallId("toolu_dup".into()),
                ]
            },
            StubEvent::Dispatched(ToolCallId("toolu_dup".into())),
            StubEvent::BatchFinished,
        ],
        "the duplicate must be rejected before a second dispatch reaches the tool plane"
    );

    let emitted_results: Vec<_> = control_units(&events)
        .into_iter()
        .filter_map(|control| match control {
            ControlUnit::ToolResult { result } => Some(result.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(emitted_results.len(), 2);
    assert!(emitted_results.iter().any(|result| {
        result.is_error
            && matches!(
                &result.output,
                ToolOutput::Error { message }
                    if message == "duplicate tool_call_id from provider"
            )
    }));

    let records = wal.records();
    let durable_results = records
        .iter()
        .filter(|record| matches!(record, WalRecord::ToolResult { .. }))
        .count();
    assert_eq!(
        durable_results, 2,
        "both tool_use instances have durable results"
    );
    let replayed = broca_core::replay(&records).expect("duplicate-id WAL remains replayable");
    let tool_uses = replayed
        .prompt
        .iter()
        .flat_map(|message| &message.content)
        .filter(|block| matches!(block, ContentBlock::ToolCall(_)))
        .count();
    let tool_results = replayed
        .prompt
        .iter()
        .flat_map(|message| &message.content)
        .filter(|block| matches!(block, ContentBlock::ToolResult(_)))
        .count();
    assert_eq!(tool_uses, 2);
    assert_eq!(
        tool_results, 2,
        "the replayed prompt remains provider-valid"
    );
    let live = provider.calls()[1].clone();
    let next_step = records
        .iter()
        .position(|record| {
            matches!(
                record,
                WalRecord::StepStarted {
                    step_id: StepId(2),
                    ..
                }
            )
        })
        .unwrap();
    let before_next = replay(&records[..next_step]).unwrap();
    assert_eq!(
        before_next.prompt, live.prompt,
        "replay must preserve each occurrence's live content"
    );
    let mut reconstructed = live.clone();
    reconstructed.prompt = before_next.prompt;
    let bytes = canonical_request_bytes(&reconstructed);
    let digest = broca_core::request_digest_hex(&bytes);
    let recorded_digest = records
        .iter()
        .find_map(|r| match r {
            WalRecord::ModelAttemptFinished {
                step_id: StepId(2),
                request_body_sha256,
                ..
            } => Some(request_body_sha256),
            _ => None,
        })
        .expect("provider recorded next request bytes");
    assert_eq!(&digest, recorded_digest);
    let batch_start = records
        .iter()
        .position(|r| matches!(r, WalRecord::ToolBatchStarted { .. }))
        .unwrap();
    let batch_end = records
        .iter()
        .position(|r| matches!(r, WalRecord::ToolBatchFinished { .. }))
        .unwrap();
    let terminals: Vec<_> = records[batch_start..=batch_end]
        .iter()
        .filter_map(|r| match r {
            WalRecord::ToolResult { result, .. } => Some(result.output.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        terminals,
        vec![
            ToolOutput::Text {
                text: "edited".into()
            },
            ToolOutput::Error {
                message: "duplicate tool_call_id from provider".into()
            }
        ],
        "WAL terminals follow occurrence order for repeated ids"
    );
    for cut in batch_start..=batch_end + 1 {
        let (cut_store, cut_dir) = tmp_lease_store(&format!("duplicate-cut-{cut}"));
        let cut_wal = CountingWalStore::default();
        *cut_wal.state.records.lock().unwrap() = wal.state.records.lock().unwrap()[..cut].to_vec();
        let cut_plane = StubToolPlane::new().with_canned_text("edit", "edited");
        let cut_provider = StubProvider::new(vec![text_step("done")]);
        let (result, _) = drive_with_wal_store(
            req(
                "duplicate-tool-id",
                vec![tool("edit", ExecutionMode::Parallel)],
                vec![],
            ),
            cut_provider.clone(),
            &cut_plane,
            &cut_store,
            &AllowAll,
            &cut_wal,
        )
        .await;
        assert!(result.is_ok(), "cut {cut}: {result:?}");
        let dispatched = records[..cut]
            .iter()
            .any(|r| matches!(r, WalRecord::ToolDispatchIntent { .. }));
        assert_eq!(
            cut_plane.exec_count(&ToolCallId("toolu_dup".into())),
            u32::from(!dispatched),
            "cut {cut}: only an unsent first copy may dispatch"
        );
        for sent in cut_provider.calls() {
            assert_eq!(sent.prompt, live.prompt, "cut {cut}: resumed prompt");
            assert_eq!(
                canonical_request_bytes(&sent),
                bytes,
                "cut {cut}: resumed bytes"
            );
        }
        let _ = std::fs::remove_dir_all(cut_dir);
    }
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn dispatched_turn_closes_on_the_result_barrier() {
    let (store, dir) = tmp_lease_store("tool-syncs");
    let provider = StubProvider::new(vec![
        tool_step(&[("toolu_1", "get_time", json!({"timezone": "Europe/Madrid"}))]),
        text_step("done"),
    ]);
    let plane = StubToolPlane::new().with_canned_text("get_time", "12:00");
    let wal = CountingWalStore::default();

    let (summary, _) = drive_with_wal_store(
        req(
            "tool-syncs",
            vec![tool("get_time", ExecutionMode::Parallel)],
            vec![],
        ),
        provider,
        &plane,
        &store,
        &AllowAll,
        &wal,
    )
    .await;

    assert!(summary.is_ok());
    assert_eq!(
        wal.sync_calls(),
        5,
        "the recovery proof plus run_started, dispatch intent, result+turn_finished, and run_finished are the only barriers"
    );

    let records = wal.records();
    let batch_finished = records
        .iter()
        .position(|record| matches!(record, WalRecord::ToolBatchFinished { .. }))
        .expect("a tool_batch_finished record");
    assert!(matches!(
        records.get(batch_finished + 1),
        Some(WalRecord::TurnFinished { step_id }) if *step_id == StepId(1)
    ));
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn no_dispatch_batch_uses_one_barrier_and_finishes_the_turn_in_order() {
    let (store, dir) = tmp_lease_store("no-dispatch-syncs");
    let provider = StubProvider::new(vec![
        tool_step(&[("toolu_no", "edit", json!({"path": "a"}))]),
        text_step("done"),
    ]);
    let plane = StubToolPlane::new().with_canned_text("edit", "ignored");
    let approval = ScriptedApproval::new(
        [(
            ToolCallId("toolu_no".into()),
            ApprovalResponse::Deny {
                tool_call_id: ToolCallId("toolu_no".into()),
                reason: "not allowed".into(),
            },
        )]
        .into_iter()
        .collect(),
    );
    let wal = CountingWalStore::default();

    let (summary, events) = drive_with_wal_store(
        req(
            "no-dispatch-syncs",
            vec![tool("edit", ExecutionMode::Parallel)],
            vec![],
        ),
        provider,
        &plane,
        &store,
        &approval,
        &wal,
    )
    .await;

    assert!(summary.is_ok());
    assert_eq!(
        wal.sync_calls(),
        4,
        "the recovery proof plus run_started, the collapsed no-dispatch batch barrier, and run_finished are the only barriers"
    );

    let records = wal.records();
    let batch_started = records
        .iter()
        .position(|record| matches!(record, WalRecord::ToolBatchStarted { .. }))
        .expect("a tool_batch_started record");
    assert!(matches!(
        records.get(batch_started + 1),
        Some(WalRecord::ToolNoDispatch { tool_call_id, .. }) if tool_call_id.0 == "toolu_no"
    ));
    assert!(matches!(
        records.get(batch_started + 2),
        Some(WalRecord::ToolBatchFinished { .. })
    ));
    assert!(matches!(
        records.get(batch_started + 3),
        Some(WalRecord::TurnFinished { step_id }) if *step_id == StepId(1)
    ));
    assert!(
        !records
            .iter()
            .any(|record| matches!(record, WalRecord::ToolDispatchIntent { .. })),
        "a no-dispatch batch must not write dispatch intents"
    );
    assert!(
        !records.iter().any(|record| matches!(
            record,
            WalRecord::ToolResult { result, .. } if result.tool_call_id.0 == "toolu_no"
        )),
        "the no-dispatch terminal is the ToolNoDispatch record, not a ToolResult"
    );

    let cu = control_units(&events);
    assert!(cu.iter().any(|control| matches!(
        control,
        ControlUnit::ToolResult { result }
            if matches!(&result.output, ToolOutput::Denied { .. })
    )));
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn empty_stop_when_still_bounds_the_loop() {
    // The default cap is the only thing between a caller who never thought about termination
    // and an unbounded loop, and nothing asserted it existed: deleting the defaulting outright
    // left the whole suite green, because every other test supplies its own stop_when.
    //
    // This pins the PROPERTY -- an empty stop_when still terminates -- rather than the number,
    // which is a tuning decision and would make this a change-detector. The script is longer
    // than any bound the loop would reasonably apply, and every step emits a tool call so the
    // loop always has a reason to continue; with no cap the run drains the script and dies on
    // the empty-stream protocol violation instead of stopping cleanly.
    let (store, dir) = tmp_lease_store("nocap");
    let scripts: Vec<Vec<NormalizedEvent>> = (0..500)
        .map(|i| {
            let id = format!("toolu_{i}");
            tool_step(&[(id.as_str(), "edit", json!({ "path": format!("f{i}") }))])
        })
        .collect();
    let provider = StubProvider::new(scripts);
    let plane = StubToolPlane::new().with_canned_text("edit", "edited");

    let (summary, _records) = drive_wal(
        req("nocap", vec![tool("edit", ExecutionMode::Parallel)], vec![]),
        provider,
        &plane,
        &store,
        &AllowAll,
    )
    .await;

    let s = summary.expect("run ok");
    assert_eq!(
        s.reason,
        RunFinishReason::MaxSteps,
        "an empty stop_when must still terminate on the default safety cap"
    );
    assert!(
        s.steps < 500,
        "the loop must stop on its own bound, not run until the provider script dries up"
    );
    let _ = std::fs::remove_dir_all(dir);
}

/// Each dispatch intent names the module the tool plane routes its call to, in a gated frame
/// requiring `dispatch-module/v1`; two sibling calls each get their own intent. A plane that
/// routes to no module (an in-process plane) leaves the intent an ordinary frame.
#[tokio::test]
async fn a_dispatched_intent_names_the_module_its_call_is_routed_to() {
    for (label, target) in [
        ("dispatch-module", Some("aft")),
        ("no-dispatch-module", None),
    ] {
        let (store, _dir) = tmp_lease_store(label);
        let provider = StubProvider::new(vec![
            tool_step(&[
                ("toolu_m", "edit", json!({"path": "a"})),
                ("toolu_n", "edit", json!({"path": "b"})),
            ]),
            text_step("done"),
        ]);
        let mut plane = StubToolPlane::new().with_canned_text("edit", "edited");
        if let Some(module) = target {
            plane = plane.with_dispatch_target(module);
        }
        let (summary, records) = drive_wal(
            req(label, vec![tool("edit", ExecutionMode::Parallel)], vec![]),
            provider,
            &plane,
            &store,
            &AllowAll,
        )
        .await;
        summary.expect("run ok");
        let intents: Vec<&WalRecord> = records
            .iter()
            .filter(|record| matches!(record, WalRecord::ToolDispatchIntent { .. }))
            .collect();
        assert_eq!(intents.len(), 2, "{label}: each sibling has its own intent");
        for intent in intents {
            let WalRecord::ToolDispatchIntent {
                module_id,
                schema_pin,
                ..
            } = intent
            else {
                unreachable!()
            };
            assert_eq!(module_id.as_deref(), target, "{label}");
            assert_eq!(schema_pin, &None, "{label}");
            let gated: &[&str] = if target.is_some() {
                &[broca_wal::DISPATCH_MODULE_FEATURE]
            } else {
                &[]
            };
            assert_eq!(intent.required_features(), gated, "{label}");
        }
    }
}

#[tokio::test]
async fn hard_stop_with_pending_calls_persists_provider_valid_turn() {
    // A stop firing on a tool-call step must NOT leave a dangling tool_use in the
    // WAL: every pending call gets a synthetic provider-valid tool_result tagged
    // MaxSteps, with NO intent (never dispatched), so resume sees a valid turn and
    // never re-dispatches.
    let (store, dir) = tmp_lease_store("c1a");
    // One step that emits tool calls; StepCount{1} fires right after it, BEFORE
    // dispatch → the pending calls must be synthesized.
    let provider = StubProvider::new(vec![tool_step(&[
        ("toolu_x", "edit", json!({"path": "a"})),
        ("toolu_y", "edit", json!({"path": "b"})),
    ])]);
    let plane = StubToolPlane::new().with_canned_text("edit", "edited");

    let (summary, records) = drive_wal(
        req(
            "c1a",
            vec![tool("edit", ExecutionMode::Parallel)],
            vec![StopCondition::StepCount { n: 1 }],
        ),
        provider,
        &plane,
        &store,
        &AllowAll,
    )
    .await;
    let s = summary.expect("run ok");
    assert_eq!(s.reason, RunFinishReason::MaxSteps);
    // The pending calls were NEVER dispatched → no intent.
    assert!(
        !records
            .iter()
            .any(|r| matches!(r, WalRecord::ToolDispatchIntent { .. })),
        "hard-stop calls were never dispatched, so NO intent records"
    );
    // Each pending call has a synthetic tool_result tagged MaxSteps.
    let maxsteps_results: Vec<&ToolCallId> = records
        .iter()
        .filter_map(|r| match r {
            WalRecord::ToolResult {
                result, outcome, ..
            } if *outcome == DispatchOutcome::MaxSteps => Some(&result.tool_call_id),
            _ => None,
        })
        .collect();
    assert_eq!(
        maxsteps_results.len(),
        2,
        "both pending calls have a MaxSteps terminal"
    );
    // The batch is complete + the turn is provider-valid (tool_use → tool_result).
    assert!(records
        .iter()
        .any(|r| matches!(r, WalRecord::ToolBatchFinished { .. })));
    // The plane never actually executed either tool (stop fired pre-dispatch).
    assert_eq!(plane.exec_count(&ToolCallId("toolu_x".into())), 0);
    assert_eq!(plane.exec_count(&ToolCallId("toolu_y".into())), 0);
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn durable_plan_records_intent_for_dispatched_and_no_dispatch_for_denied() {
    // The per-call plan is durable: a dispatched call → ToolDispatchIntent +
    // ToolResult; a DENIED call → ToolNoDispatch (NO intent, NO result) so resume
    // never re-dispatches it as "missing". Both are in ToolBatchStarted's
    // planned_calls.
    let (store, dir) = tmp_lease_store("durplan");
    let provider = StubProvider::new(vec![
        tool_step(&[
            ("toolu_ok", "edit", json!({"path": "a"})),
            ("toolu_no", "edit", json!({"path": "b"})),
        ]),
        text_step("done"),
    ]);
    let plane = StubToolPlane::new().with_canned_text("edit", "edited");
    // Deny exactly the second call by id.
    let approval = ScriptedApproval::new(
        [(
            ToolCallId("toolu_no".into()),
            ApprovalResponse::Deny {
                tool_call_id: ToolCallId("toolu_no".into()),
                reason: "not allowed".into(),
            },
        )]
        .into_iter()
        .collect(),
    );

    let (summary, records) = drive_wal(
        req(
            "durplan",
            vec![tool("edit", ExecutionMode::Parallel)],
            vec![],
        ),
        provider,
        &plane,
        &store,
        &approval,
    )
    .await;
    assert!(summary.is_ok());

    // Exactly one batch, with BOTH calls planned.
    let planned = records.iter().find_map(|r| match r {
        WalRecord::ToolBatchStarted { planned_calls, .. } => Some(planned_calls.clone()),
        _ => None,
    });
    let planned = planned.expect("a tool_batch_started");
    assert_eq!(planned.len(), 2, "both calls in the durable plan");

    // The dispatched call has an intent; the denied call has NONE.
    let has_intent = |id: &str| {
        records.iter().any(|r| {
            matches!(r,
            WalRecord::ToolDispatchIntent { tool_call_id, .. } if tool_call_id.0 == id)
        })
    };
    assert!(
        has_intent("toolu_ok"),
        "dispatched call has a durable intent"
    );
    assert!(
        !has_intent("toolu_no"),
        "denied call has NO intent (never dispatched)"
    );

    // The denied call has a no_dispatch terminal; the dispatched one does NOT.
    let has_no_dispatch = |id: &str| {
        records.iter().any(|r| {
            matches!(r,
            WalRecord::ToolNoDispatch { tool_call_id, .. } if tool_call_id.0 == id)
        })
    };
    assert!(
        has_no_dispatch("toolu_no"),
        "denied call has a durable tool_no_dispatch"
    );
    assert!(
        !has_no_dispatch("toolu_ok"),
        "dispatched call has no tool_no_dispatch"
    );

    // The dispatched call has a tool_result; the denied one does NOT (its terminal
    // is the no_dispatch).
    let has_result = |id: &str| {
        records.iter().any(|r| {
            matches!(r,
            WalRecord::ToolResult { result, .. } if result.tool_call_id.0 == id)
        })
    };
    assert!(has_result("toolu_ok"), "dispatched call has a tool_result");
    assert!(
        !has_result("toolu_no"),
        "denied call has NO tool_result (no_dispatch is its terminal)"
    );

    // The atomic run_started (config + input) + finalization bracket the run.
    assert!(
        matches!(records.first(), Some(WalRecord::RunStarted { config, .. }) if config.renderer_version >= 1),
        "starts with the atomic run_started carrying config"
    );
    assert!(
        matches!(records.last(), Some(WalRecord::RunFinished { .. })),
        "ends with run_finished"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn ambiguous_dispatch_interrupts_run_without_feeding_model() {
    // A "sent but no result" dispatch → the run finishes Interrupted with a
    // provider-valid synthetic unknown tool_result, and does NOT run a second model
    // step (that would feed the unknown outcome back to the model).
    let (store, dir) = tmp_lease_store("ambig");
    let provider = StubProvider::new(vec![
        tool_step(&[("toolu_1", "edit", json!({"path": "a"}))]),
        // A second step is scripted but must NEVER run — the interrupt stops first.
        text_step("should not be reached"),
    ]);
    let plane = StubToolPlane::new().with_tool("edit", |_| {
        StubReply::OutcomeUnknown("reset after send".into())
    });

    let (summary, events) = drive(
        req("ambig", vec![tool("edit", ExecutionMode::Parallel)], vec![]),
        provider,
        &plane,
        &store,
        &AllowAll,
    )
    .await;

    let s = summary.expect("run ok");
    assert_eq!(
        s.reason,
        RunFinishReason::Interrupted,
        "ambiguous outcome interrupts"
    );
    assert_eq!(s.steps, 1, "the second model step did NOT run");

    // The synthetic unknown result is present + provider-valid (is_error).
    let cu = control_units(&events);
    let tool_results: Vec<_> = cu
        .iter()
        .filter_map(|c| match c {
            ControlUnit::ToolResult { result } => Some(result),
            _ => None,
        })
        .collect();
    assert_eq!(tool_results.len(), 1);
    assert!(
        tool_results[0].is_error,
        "unknown result is is_error (valid conversation)"
    );
    // The edit tool never produced a real cached result (not Completed).
    assert_eq!(plane.exec_count(&ToolCallId("toolu_1".into())), 0);
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn tripped_sequential_batch_preserves_no_dispatch_terminal_and_projection() {
    let (store, dir) = tmp_lease_store("sequential-denial-trip");
    let provider = StubProvider::new(vec![
        tool_step(&[
            ("call_a", "edit", json!({"path": "a"})),
            ("call_b", "danger", json!({})),
        ]),
        text_step("should not be reached"),
    ]);
    let plane = StubToolPlane::new()
        .with_tool("edit", |_| {
            StubReply::OutcomeUnknown("reset after send".into())
        })
        .with_canned_text("danger", "SHOULD-NOT-RUN");
    let denial_reason = "operator denied dangerous call";
    let approval = ScriptedApproval::new(
        [(
            ToolCallId("call_b".into()),
            ApprovalResponse::Deny {
                tool_call_id: ToolCallId("call_b".into()),
                reason: denial_reason.into(),
            },
        )]
        .into_iter()
        .collect(),
    );

    let (summary, _events, framed) = drive_with_control(
        req(
            "sequential-denial-trip",
            vec![
                tool("edit", ExecutionMode::Sequential),
                tool("danger", ExecutionMode::Sequential),
            ],
            vec![],
        ),
        provider,
        &plane,
        &store,
        &approval,
        RunControl::default(),
    )
    .await;

    let summary = summary.expect("run ok");
    assert_eq!(summary.reason, RunFinishReason::Interrupted);
    assert_eq!(summary.steps, 1, "the ambiguous call must stop the run");
    assert_eq!(plane.exec_count(&ToolCallId("call_b".into())), 0);

    let records: Vec<WalRecord> = framed.iter().map(|frame| frame.record.clone()).collect();
    let b_terminals: Vec<&WalRecord> = records
        .iter()
        .filter(|record| match record {
            WalRecord::ToolNoDispatch { tool_call_id, .. } => tool_call_id.0 == "call_b",
            WalRecord::ToolResult { result, .. } => result.tool_call_id.0 == "call_b",
            _ => false,
        })
        .collect();
    assert_eq!(
        b_terminals.len(),
        1,
        "a refused call has exactly one durable terminal"
    );
    assert!(matches!(
        b_terminals.as_slice(),
        [WalRecord::ToolNoDispatch {
            reason: NoDispatchReason::Denied { reason },
            ..
        }] if reason == denial_reason
    ));
    assert!(!records.iter().any(|record| matches!(
        record,
        WalRecord::ToolResult { result, .. } if result.tool_call_id.0 == "call_b"
    )));

    let state = replay(&records).expect("replay tripped batch");
    let replayed_b = state
        .prompt
        .iter()
        .flat_map(|message| &message.content)
        .find_map(|block| match block {
            ContentBlock::ToolResult(result) if result.tool_call_id.0 == "call_b" => Some(result),
            _ => None,
        })
        .expect("replay reconstructs the denied result");
    let expected_b = replay::no_dispatch_result(
        &ToolCallId("call_b".into()),
        &NoDispatchReason::Denied {
            reason: denial_reason.into(),
        },
    );
    assert_eq!(replayed_b, &expected_b);

    let mut projected_result_ids: Vec<String> = broca_core::project_control(&framed)
        .into_iter()
        .filter_map(|(_, control)| match control {
            ControlUnit::ToolResult { result } => Some(result.tool_call_id.0),
            _ => None,
        })
        .collect();
    projected_result_ids.sort();
    assert_eq!(
        projected_result_ids,
        vec!["call_a", "call_b"],
        "the batch projection emits one result per call id"
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn tripped_sequential_batch_interrupts_remaining_dispatchable_call() {
    let (store, dir) = tmp_lease_store("sequential-dispatch-trip");
    let provider = StubProvider::new(vec![tool_step(&[
        ("call_a", "edit", json!({"path": "a"})),
        ("call_c", "edit", json!({"path": "c"})),
    ])]);
    let plane = StubToolPlane::new().with_tool("edit", |call| {
        if call.tool_call_id.0 == "call_a" {
            StubReply::OutcomeUnknown("reset after send".into())
        } else {
            StubReply::Output(ToolOutput::Text {
                text: "SHOULD-NOT-RUN".into(),
            })
        }
    });

    let (summary, _events, framed) = drive_with_control(
        req(
            "sequential-dispatch-trip",
            vec![tool("edit", ExecutionMode::Sequential)],
            vec![],
        ),
        provider,
        &plane,
        &store,
        &AllowAll,
        RunControl::default(),
    )
    .await;

    let summary = summary.expect("run ok");
    assert_eq!(summary.reason, RunFinishReason::Interrupted);
    assert_eq!(plane.exec_count(&ToolCallId("call_c".into())), 0);
    let interrupted_c: Vec<_> = framed
        .iter()
        .filter_map(|frame| match &frame.record {
            WalRecord::ToolResult {
                result,
                outcome: DispatchOutcome::Interrupted,
                ..
            } if result.tool_call_id.0 == "call_c" => Some(result),
            _ => None,
        })
        .collect();
    assert_eq!(interrupted_c.len(), 1);
    assert!(matches!(
        &interrupted_c[0].output,
        ToolOutput::Error { message } if message == "tool not run: batch interrupted"
    ));

    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn run_usage_accumulates_across_steps() {
    let (store, dir) = tmp_lease_store("usage");
    // Two billed steps with DISTINCT usages: a tool step (100in/10out) then the
    // text reply (7in/3out). Run total must be the SUM, not the last step.
    let step1 = {
        let mut s = tool_step(&[("toolu_1", "get_time", json!({}))]);
        // Replace the fixed usages with distinct ones for this test.
        s = s
            .into_iter()
            .map(|e| match e {
                NormalizedEvent::Control(ProviderControl::StepFinished {
                    step_id,
                    finish_reason,
                    ..
                }) => ctrl(ProviderControl::StepFinished {
                    step_id,
                    usage: usage(100, 10),
                    finish_reason,
                    service_tier_used: None,
                    speed_used: None,
                }),
                NormalizedEvent::Control(ProviderControl::Finished { finish_reason, .. }) => {
                    ctrl(ProviderControl::Finished {
                        usage: usage(100, 10),
                        finish_reason,
                        retries_used: 0,
                    })
                }
                other => other,
            })
            .collect();
        s
    };
    let step2 = {
        text_step("done")
            .into_iter()
            .map(|e| match e {
                NormalizedEvent::Control(ProviderControl::StepFinished {
                    step_id,
                    finish_reason,
                    ..
                }) => ctrl(ProviderControl::StepFinished {
                    step_id,
                    usage: usage(7, 3),
                    finish_reason,
                    service_tier_used: None,
                    speed_used: None,
                }),
                NormalizedEvent::Control(ProviderControl::Finished { finish_reason, .. }) => {
                    ctrl(ProviderControl::Finished {
                        usage: usage(7, 3),
                        finish_reason,
                        retries_used: 1,
                    })
                }
                other => other,
            })
            .collect::<Vec<_>>()
    };
    let provider = StubProvider::new(vec![step1, step2]);
    let plane = StubToolPlane::new().with_canned_text("get_time", "12:00");
    let (summary, events) = drive(
        req(
            "usage",
            vec![tool("get_time", ExecutionMode::Parallel)],
            vec![],
        ),
        provider,
        &plane,
        &store,
        &AllowAll,
    )
    .await;

    let s = summary.expect("run ok");
    assert_eq!(s.steps, 2);
    // SUM across the two billed steps, not just the last.
    assert_eq!(
        s.usage.input_tokens,
        Some(107),
        "input accumulates across steps"
    );
    assert_eq!(
        s.usage.output_tokens,
        Some(13),
        "output accumulates across steps"
    );
    assert_eq!(
        s.retries_used, 1,
        "run total includes the second step retry"
    );

    // RunFinished carries the same accumulated total.
    let cu = control_units(&events);
    match cu.last() {
        Some(ControlUnit::RunFinished {
            usage: Some(u),
            retries_used,
            ..
        }) => {
            assert_eq!(u.input_tokens, Some(107));
            assert_eq!(u.output_tokens, Some(13));
            assert_eq!(*retries_used, 1);
        }
        other => panic!("expected RunFinished with accumulated usage, got {other:?}"),
    }
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn stop_when_step_count_halts_before_tool_dispatch() {
    let (store, dir) = tmp_lease_store("stepcount");
    // Step 1 WOULD call a tool, but stop_when=StepCount{1} fires AFTER the step,
    // BEFORE dispatch.
    let provider = StubProvider::new(vec![tool_step(&[("toolu_1", "get_time", json!({}))])]);
    let plane = StubToolPlane::new().with_canned_text("get_time", "x");
    let (summary, records) = drive_wal(
        req(
            "stepcount",
            vec![tool("get_time", ExecutionMode::Parallel)],
            vec![StopCondition::StepCount { n: 1 }],
        ),
        provider,
        &plane,
        &store,
        &AllowAll,
    )
    .await;

    let s = summary.expect("run ok");
    assert_eq!(s.reason, RunFinishReason::MaxSteps);
    assert_eq!(s.steps, 1);
    let frozen_stop_when = records.iter().find_map(|record| match record {
        WalRecord::RunStarted { config, .. } => Some(config.stop_when.as_slice()),
        _ => None,
    });
    assert_eq!(
        frozen_stop_when,
        Some([StopCondition::StepCount { n: 1 }].as_slice()),
        "admission must persist the caller's stop boundary"
    );
    assert!(
        plane.log().is_empty(),
        "no dispatch after stop_when halts the run"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn denied_approval_yields_denied_result_without_dispatch() {
    let (store, dir) = tmp_lease_store("deny");
    let provider = StubProvider::new(vec![
        tool_step(&[("toolu_1", "danger", json!({}))]),
        text_step("ok, skipping."),
    ]);
    let plane = StubToolPlane::new().with_canned_text("danger", "SHOULD-NOT-RUN");
    let deny = ScriptedApproval::new(
        [(
            ToolCallId("toolu_1".into()),
            ApprovalResponse::Deny {
                tool_call_id: ToolCallId("toolu_1".into()),
                reason: "nope".into(),
            },
        )]
        .into_iter()
        .collect(),
    );
    let (summary, events) = drive(
        req(
            "deny",
            vec![tool("danger", ExecutionMode::Parallel)],
            vec![],
        ),
        provider,
        &plane,
        &store,
        &deny,
    )
    .await;

    summary.expect("run ok");
    // The tool NEVER executed.
    assert_eq!(plane.exec_count(&ToolCallId("toolu_1".into())), 0);
    // But a denied is_error result was fed back so the tool_use has a result.
    let cu = control_units(&events);
    assert!(cu.iter().any(|c| matches!(
        c,
        ControlUnit::ToolResult { result } if result.is_error && matches!(&result.output, ToolOutput::Denied { .. })
    )));
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn tool_outside_manifest_is_error_result_not_dispatched() {
    let (store, dir) = tmp_lease_store("cap");
    // The model calls a tool the harness policy never admitted.
    let provider = StubProvider::new(vec![
        tool_step(&[("toolu_1", "forbidden", json!({}))]),
        text_step("understood."),
    ]);
    let plane = StubToolPlane::new().with_canned_text("forbidden", "SHOULD-NOT-RUN");
    // Manifest has only get_time; forbidden is not in policy_allowed.
    let mut request = req(
        "cap",
        vec![tool("get_time", ExecutionMode::Parallel)],
        vec![],
    );
    request.policy_allowed = ["get_time".to_string()].into_iter().collect();
    let (summary, events) = drive(request, provider, &plane, &store, &AllowAll).await;

    summary.expect("run ok");
    assert_eq!(plane.exec_count(&ToolCallId("toolu_1".into())), 0);
    let cu = control_units(&events);
    assert!(cu.iter().any(|c| matches!(
        c,
        ControlUnit::ToolResult { result } if result.is_error
    )));
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn parallel_results_returned_in_source_order() {
    let (store, dir) = tmp_lease_store("order");
    let provider = StubProvider::new(vec![
        tool_step(&[
            ("call_a", "echo", json!({"v": "a"})),
            ("call_b", "echo", json!({"v": "b"})),
            ("call_c", "echo", json!({"v": "c"})),
        ]),
        text_step("done"),
    ]);
    // echo returns the call id so we can check ordering.
    let plane = StubToolPlane::new().with_tool("echo", |call| {
        StubReply::Output(ToolOutput::Text {
            text: call.tool_call_id.0.clone(),
        })
    });
    let (summary, events) = drive(
        req("order", vec![tool("echo", ExecutionMode::Parallel)], vec![]),
        provider,
        &plane,
        &store,
        &AllowAll,
    )
    .await;
    summary.expect("run ok");

    let order: Vec<String> = control_units(&events)
        .iter()
        .filter_map(|c| match c {
            ControlUnit::ToolResult { result } => match &result.output {
                ToolOutput::Text { text } => Some(text.clone()),
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert_eq!(
        order,
        vec!["call_a", "call_b", "call_c"],
        "results in assistant source order"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn sequential_execution_mode_still_source_ordered() {
    let (store, dir) = tmp_lease_store("seq");
    let provider = StubProvider::new(vec![
        tool_step(&[("call_a", "echo", json!({})), ("call_b", "echo", json!({}))]),
        text_step("done"),
    ]);
    let plane = StubToolPlane::new().with_tool("echo", |call| {
        StubReply::Output(ToolOutput::Text {
            text: call.tool_call_id.0.clone(),
        })
    });
    let (summary, events) = drive(
        req("seq", vec![tool("echo", ExecutionMode::Sequential)], vec![]),
        provider,
        &plane,
        &store,
        &AllowAll,
    )
    .await;
    summary.expect("run ok");
    let order: Vec<String> = control_units(&events)
        .iter()
        .filter_map(|c| match c {
            ControlUnit::ToolResult { result } => match &result.output {
                ToolOutput::Text { text } => Some(text.clone()),
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert_eq!(order, vec!["call_a", "call_b"]);
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn second_writer_on_held_session_is_rejected() {
    let (store, dir) = tmp_lease_store("lease");
    // Hold the lease for this session out-of-band, using the SAME identity→key mapping
    // the loop uses, so this genuinely collides with the loop's own acquire.
    let held = store
        .acquire(&lease_key_for(&session("lease")))
        .expect("hold");

    let provider = StubProvider::new(vec![text_step("hi")]);
    let plane = StubToolPlane::new();
    let (summary, events) = drive(
        req("lease", vec![], vec![]),
        provider,
        &plane,
        &store,
        &AllowAll,
    )
    .await;

    match summary {
        Err(RunError::LeaseHeld(s)) => assert_eq!(s, "lease"),
        other => panic!("expected LeaseHeld, got {other:?}"),
    }
    // No output before the lease failure.
    assert!(events.is_empty());
    drop(held);
    let _ = std::fs::remove_dir_all(dir);
}

/// An account-limit refusal names the run's frozen account and credential on every surface that
/// reports the terminal: the live control-lane `Error` unit, the durable `RunFinished`, and the
/// `Error` unit the WAL projection gives a subscriber. The provider cannot name either, so the
/// run stamps them. An ordinary rate limit names the credential (not the account) under the
/// typed `rate_limited` code, and an error unrelated to the credential stays exactly as the
/// provider reported it.
#[tokio::test]
async fn an_account_limit_terminal_names_the_frozen_account_on_every_surface() {
    let exhausted = broca_protocol::error::ProviderError {
        provider_subcode: None,
        refusal: None,
        class: broca_protocol::ErrorClass::Transient,
        message: "429: account limit".into(),
        status: Some(429),
        retry_after_secs: Some(352_835.0),
        provider_code: Some(broca_protocol::error::ACCOUNT_LIMIT_EXHAUSTED.into()),
        resets_at_ms: Some(1_790_740_800_000),
        resets_at_source: Some("header".into()),
        limit_window: Some("seven_day".into()),
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
    };
    let ordinary = broca_protocol::error::ProviderError {
        provider_code: Some("rate_limit_error".into()),
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        ..exhausted.clone()
    };
    let bad_request = broca_protocol::error::ProviderError {
        class: broca_protocol::ErrorClass::Permanent,
        message: "400: bad request".into(),
        status: Some(400),
        retry_after_secs: None,
        provider_code: Some("invalid_request_error".into()),
        ..ordinary.clone()
    };
    let credential = Some("oauth:anthropic:acct-a".to_owned());
    let cases = [
        (
            "acctlimit",
            exhausted.clone(),
            broca_protocol::error::ProviderError {
                account_ref: Some("d4bc9dbc".into()),
                credential_id: credential.clone(),
                ..exhausted
            },
        ),
        (
            "acctlimit-ordinary",
            ordinary.clone(),
            broca_protocol::error::ProviderError {
                provider_code: Some(broca_protocol::error::RATE_LIMITED.into()),
                credential_id: credential.clone(),
                ..ordinary
            },
        ),
        ("acctlimit-unrelated", bad_request.clone(), bad_request),
    ];
    for (name, provider_error, expected) in cases {
        let (store, dir) = tmp_lease_store(name);
        let provider = StubProvider::new(vec![vec![
            ctrl(ProviderControl::StreamStart { warnings: vec![] }),
            ctrl(ProviderControl::Error {
                error: provider_error.clone(),
                retries_used: 0,
            }),
        ]]);
        let control = RunControl {
            auth_selection: Some(broca_core::AuthSelection {
                credential_id: "oauth:anthropic:acct-a".into(),
                account_ref: Some("d4bc9dbc".into()),
                resolved_credential_id: None,
                vault_request: None,
                account_decision: None,
            }),
            ..RunControl::default()
        };
        let (summary, events, records) = drive_with_control(
            req(name, vec![], vec![]),
            provider,
            &StubToolPlane::new(),
            &store,
            &AllowAll,
            control,
        )
        .await;
        assert_eq!(summary.unwrap().reason, RunFinishReason::Error);

        let live = events
            .iter()
            .find_map(|event| match event {
                OutputEvent::Control(ControlUnit::Error { error }) => Some(error.as_inner()),
                _ => None,
            })
            .expect("live control-lane Error unit");
        assert_eq!(live, &expected, "{name}: live ControlUnit::Error");

        let durable = records
            .iter()
            .find_map(|framed| match &framed.record {
                WalRecord::RunFinished { error, .. } => error.clone(),
                _ => None,
            })
            .expect("RunFinished carries the typed error");
        assert_eq!(durable, expected, "{name}: WAL RunFinished.error");

        let projected = broca_core::project_control(&records)
            .into_iter()
            .find_map(|(_, unit)| match unit {
                ControlUnit::Error { error } => Some(error.into_inner()),
                _ => None,
            })
            .expect("projected ControlUnit::Error");
        assert_eq!(projected, expected, "{name}: projected ControlUnit::Error");
        let _ = std::fs::remove_dir_all(dir);
    }
}

/// A billing_exhausted refusal names the run's frozen account on every surface when one was
/// selected, and leaves it empty when none was selected. It names the frozen credential id
/// whenever the run froze a selection, account or not, and nothing when it froze none.
#[tokio::test]
async fn a_billing_exhausted_terminal_names_the_frozen_account_on_every_surface() {
    let billing = broca_protocol::error::ProviderError {
        provider_subcode: None,
        refusal: None,
        class: broca_protocol::ErrorClass::Permanent,
        message: "403: spending limit".into(),
        status: Some(403),
        retry_after_secs: None,
        provider_code: Some(broca_protocol::error::BILLING_EXHAUSTED.into()),
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
    };
    for (name, selection, expect_account, expect_credential) in [
        (
            "billing-with-acct",
            Some(Some("acct-x")),
            Some("acct-x"),
            Some("apikey:xai:default"),
        ),
        (
            "billing-without-acct",
            Some(None),
            None,
            Some("apikey:xai:default"),
        ),
        ("billing-without-selection", None, None, None),
    ] {
        let (store, dir) = tmp_lease_store(name);
        let provider = StubProvider::new(vec![vec![
            ctrl(ProviderControl::StreamStart { warnings: vec![] }),
            ctrl(ProviderControl::Error {
                error: billing.clone(),
                retries_used: 0,
            }),
        ]]);
        let control = RunControl {
            auth_selection: selection.map(|account: Option<&str>| broca_core::AuthSelection {
                credential_id: "apikey:xai:default".into(),
                account_ref: account.map(str::to_owned),
                resolved_credential_id: None,
                vault_request: None,
                account_decision: None,
            }),
            ..RunControl::default()
        };
        let (summary, events, records) = drive_with_control(
            req(name, vec![], vec![]),
            provider,
            &StubToolPlane::new(),
            &store,
            &AllowAll,
            control,
        )
        .await;
        assert_eq!(summary.unwrap().reason, RunFinishReason::Error);
        let expected = broca_protocol::error::ProviderError {
            account_ref: expect_account.map(str::to_owned),
            credential_id: expect_credential.map(str::to_owned),
            ..billing.clone()
        };

        // 1. Live control-lane Error unit:
        let live = events
            .iter()
            .find_map(|event| match event {
                OutputEvent::Control(ControlUnit::Error { error }) => Some(error.as_inner()),
                _ => None,
            })
            .expect("live control-lane Error unit");
        assert_eq!(live, &expected, "{name}: live ControlUnit::Error");

        // 2. Durable WAL RunFinished:
        let durable = records
            .iter()
            .find_map(|framed| match &framed.record {
                WalRecord::RunFinished { error, .. } => error.clone(),
                _ => None,
            })
            .expect("RunFinished carries the typed error");
        assert_eq!(durable, expected, "{name}: WAL RunFinished.error");

        // 3. Reconciled run state (powers run.status):
        let plain_records: Vec<_> = records.iter().map(|f| f.record.clone()).collect();
        let reconciled = broca_core::reconcile_run_state(&plain_records, &format!("run-{name}"))
            .expect("reconcile_run_state for run.status");
        assert_eq!(
            reconciled.error.as_ref(),
            Some(&expected),
            "{name}: reconciled state error (run.status)"
        );

        let _ = std::fs::remove_dir_all(dir);
    }
}

/// Drive a run with explicit controls against a caller-owned WAL (so a later resume sees the
/// same durable history) and return the summary plus every live output event.
async fn drive_on_wal(
    request: RunRequest,
    provider: StubProvider,
    store: &FileLeaseStore,
    wal: &FileWalStore,
    control: RunControl,
    resume: bool,
) -> (Result<broca_core::RunSummary, RunError>, Vec<OutputEvent>) {
    let (tx, mut rx) = mpsc::channel(4096);
    let collector = tokio::spawn(async move {
        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(event);
        }
        events
    });
    let plane = StubToolPlane::new();
    let summary = if resume {
        broca_core::run_loop_resume_after_auth_with_control(
            request, &provider, &plane, store, &AllowAll, wal, None, control, &tx,
        )
        .await
    } else {
        run_loop_with_control(
            request, &provider, &plane, store, &AllowAll, wal, None, control, &tx,
        )
        .await
    };
    drop(tx);
    (summary, collector.await.unwrap())
}

/// A resumed episode that supplies no auth selection still names the account and credential on
/// an account-limit refusal: they are the selection the run froze earlier, or — after a resume
/// that switched accounts wrote `RunAuthRefrozen` — the refrozen one, never the original. Each arm
/// admits a run with account `acct-A` that pauses for auth before any provider call; the
/// refreeze arm then resumes once with `acct-B` into a provider auth failure (which pauses
/// again); the final resume supplies no selection and its provider answers account-limit.
#[tokio::test]
async fn a_resumed_account_limit_terminal_names_the_frozen_or_refrozen_account() {
    let selection = |account: &str| broca_core::AuthSelection {
        credential_id: format!("oauth:anthropic:{account}"),
        account_ref: Some(account.into()),
        resolved_credential_id: None,
        vault_request: None,
        account_decision: None,
    };
    let auth_failure = broca_protocol::error::ProviderError {
        provider_subcode: None,
        refusal: None,
        class: broca_protocol::ErrorClass::Auth,
        message: "401: token revoked".into(),
        status: Some(401),
        retry_after_secs: None,
        provider_code: None,
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
    };
    let exhausted = broca_protocol::error::ProviderError {
        provider_subcode: None,
        refusal: None,
        class: broca_protocol::ErrorClass::Transient,
        message: "429: account limit".into(),
        status: Some(429),
        retry_after_secs: Some(352_835.0),
        provider_code: Some(broca_protocol::error::ACCOUNT_LIMIT_EXHAUSTED.into()),
        resets_at_ms: Some(1_790_740_800_000),
        resets_at_source: Some("header".into()),
        limit_window: Some("seven_day".into()),
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
    };
    let failing_step = |error: &broca_protocol::error::ProviderError| {
        vec![
            ctrl(ProviderControl::StreamStart { warnings: vec![] }),
            ctrl(ProviderControl::Error {
                error: error.clone(),
                retries_used: 0,
            }),
        ]
    };
    for (name, refreeze_to, expect_account) in [
        ("resume-acctlimit", None, "acct-A"),
        ("refrozen-acctlimit", Some("acct-B"), "acct-B"),
    ] {
        let (store, dir) = tmp_lease_store(name);
        let wal_dir = dir.with_extension("wal");
        let _ = std::fs::remove_dir_all(&wal_dir);
        let wal = FileWalStore::new(&wal_dir);

        // Admit with acct-A and pause before the first provider call.
        let (paused, _) = drive_on_wal(
            req(name, vec![], vec![]),
            StubProvider::new(vec![]),
            &store,
            &wal,
            RunControl {
                pause_for_auth_at_start: Some(auth_failure.clone()),
                auth_selection: Some(selection("acct-A")),
                ..RunControl::default()
            },
            false,
        )
        .await;
        assert_eq!(
            paused.unwrap().reason,
            RunFinishReason::Interrupted,
            "{name}: admitted then paused for auth"
        );

        if let Some(account) = refreeze_to {
            // Resume onto another account; the provider rejects the credential, so the run
            // pauses again with the new selection durably refrozen.
            let (repaused, _) = drive_on_wal(
                req(name, vec![], vec![]),
                StubProvider::new(vec![failing_step(&auth_failure)]),
                &store,
                &wal,
                RunControl {
                    auth_selection: Some(selection(account)),
                    ..RunControl::default()
                },
                true,
            )
            .await;
            assert_eq!(
                repaused.unwrap().reason,
                RunFinishReason::Interrupted,
                "{name}: the auth failure pauses the resumed run again"
            );
            let refrozen: Vec<_> = wal
                .read_records(&session(name))
                .expect("read refrozen WAL")
                .into_iter()
                .filter_map(|framed| match framed.record {
                    WalRecord::RunAuthRefrozen { auth_selection, .. } => Some(auth_selection),
                    _ => None,
                })
                .collect();
            assert_eq!(refrozen, vec![selection(account)], "{name}: refreeze");
        }

        // The final resume supplies NO selection; the provider answers account-limit.
        let (summary, events) = drive_on_wal(
            req(name, vec![], vec![]),
            StubProvider::new(vec![failing_step(&exhausted)]),
            &store,
            &wal,
            RunControl::default(),
            true,
        )
        .await;
        assert_eq!(summary.unwrap().reason, RunFinishReason::Error, "{name}");
        let expected = broca_protocol::error::ProviderError {
            account_ref: Some(expect_account.into()),
            credential_id: Some(format!("oauth:anthropic:{expect_account}")),
            ..exhausted.clone()
        };

        let live = events
            .iter()
            .find_map(|event| match event {
                OutputEvent::Control(ControlUnit::Error { error }) => Some(error.as_inner()),
                _ => None,
            })
            .expect("live control-lane Error unit");
        assert_eq!(live, &expected, "{name}: live ControlUnit::Error");

        let records = wal.read_records(&session(name)).expect("read resumed WAL");
        let durable = records
            .iter()
            .find_map(|framed| match &framed.record {
                WalRecord::RunFinished { error, .. } => error.clone(),
                _ => None,
            })
            .expect("RunFinished carries the typed error");
        assert_eq!(durable, expected, "{name}: WAL RunFinished.error");

        let projected = broca_core::project_control(&records)
            .into_iter()
            .find_map(|(_, unit)| match unit {
                ControlUnit::Error { error } => Some(error.into_inner()),
                _ => None,
            })
            .expect("projected ControlUnit::Error");
        assert_eq!(projected, expected, "{name}: projected ControlUnit::Error");
        let _ = std::fs::remove_dir_all(dir);
        let _ = std::fs::remove_dir_all(wal_dir);
    }
}
/// A non-auth provider error must persist its TYPED detail on the durable `RunFinished`
/// record — not just emit it live on `out`. Module subscribers source the control stream
/// from the WAL projection (the coordinator drops control events on the live `out`
/// channel), so an error whose detail isn't on the durable record is invisible to every
/// projection consumer: they'd see `RunFinished{Error}` with no cause. This asserts the
/// record carries the class/message/status/provider_code, which `project_control` then
/// surfaces as a `ControlUnit::Error` ahead of the terminal.
#[tokio::test]
async fn error_terminal_persists_typed_error_on_run_finished() {
    let (store, dir) = tmp_lease_store("errdetail");
    let provider = StubProvider::new(vec![error_step(
        broca_protocol::ErrorClass::Permanent,
        "model exploded",
    )
    .into_iter()
    .map(|event| match event {
        NormalizedEvent::Control(ProviderControl::Error { error, .. }) => {
            ctrl(ProviderControl::Error {
                error,
                retries_used: 2,
            })
        }
        other => other,
    })
    .collect()]);
    let plane = StubToolPlane::new();
    let (summary, records) = drive_wal(
        req("errdetail", vec![], vec![]),
        provider,
        &plane,
        &store,
        &AllowAll,
    )
    .await;

    // The run ends as Error and retains the retries spent by its failing final step.
    let summary = summary.unwrap();
    assert_eq!(summary.reason, RunFinishReason::Error);
    assert_eq!(summary.retries_used, 2);

    // The durable RunFinished carries the typed error detail (not None).
    let finished = records
        .iter()
        .find_map(|r| match r {
            WalRecord::RunFinished {
                reason,
                error,
                retries_used,
                ..
            } => Some((reason, error, retries_used)),
            _ => None,
        })
        .expect("a RunFinished record was written");
    assert_eq!(*finished.0, RunFinishReason::Error);
    assert_eq!(*finished.2, 2);
    let err = finished
        .1
        .as_ref()
        .expect("RunFinished on the Error path must carry the typed error, not None");
    assert_eq!(err.class, broca_protocol::ErrorClass::Permanent);
    assert_eq!(err.message, "model exploded");
    assert_eq!(err.status, Some(500));
    assert_eq!(err.provider_code.as_deref(), Some("boom_code"));

    // And the projection surfaces it as a ControlUnit::Error before the terminal — the
    // unit a module subscriber actually receives.
    let framed: Vec<broca_wal::FramedRecord> = records
        .into_iter()
        .enumerate()
        .map(|(i, record)| broca_wal::FramedRecord {
            seq: (i + 1) as u64,
            fence: 1,
            record,
        })
        .collect();
    let proj = broca_core::project_control(&framed);
    let err_pos = proj
        .iter()
        .position(|(_, u)| matches!(u, ControlUnit::Error { .. }))
        .expect("projection must emit a ControlUnit::Error for the error terminal");
    let fin_pos = proj
        .iter()
        .position(|(_, u)| matches!(u, ControlUnit::RunFinished { .. }))
        .expect("projection must emit RunFinished");
    assert!(
        err_pos < fin_pos,
        "the error detail must be projected BEFORE the terminal"
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn prior_snapshot_handoff_excludes_pending_payloads_and_shares_pending_storage() {
    const QUEUED: usize = 32;
    const PAYLOAD_BYTES: usize = 16 * 1024;

    let pending_fifo: Vec<_> = (0..QUEUED)
        .map(|index| {
            let mut request = req("snapshot-pending", vec![], vec![]).to_submission();
            request.input = vec![Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: format!("{index}:").repeat(PAYLOAD_BYTES / 2),
                }],
            )];
            PendingSubmission {
                hooks: None,
                title: None,
                conversation_key: None,
                delivery: None,
                send_id: None,
                submission_id: SubmissionId(format!("sub-{index}")),
                request,
                request_identity_digest: None,
                principal: None,
            }
        })
        .collect();
    let payload_ptr = match &pending_fifo[0].request.input[0].content[0] {
        ContentBlock::Text { text } => text.as_ptr(),
        block => panic!("expected queued text payload, got {block:?}"),
    };
    let mut snapshot = PriorWalSnapshot {
        base_watermark: 0,
        head_seq: 0,
        tail: Vec::new(),
        checkpoint_at_base: None,
        checkpoint_at_head: AdmissionCheckpointV1 {
            input_budget: None,
            session_class: None,
            lineage_retention: None,
            compaction: None,
            effort_history: None,
            ownership: Some(broca_core::SessionOwnershipV1::default()),
            at_seq: 0,
            last_episode: None,
            pending_fifo: Arc::new(pending_fifo),
        },
    };

    let shared_snapshot = snapshot.clone();
    assert!(Arc::ptr_eq(
        &snapshot.checkpoint_at_head.pending_fifo,
        &shared_snapshot.checkpoint_at_head.pending_fifo
    ));
    drop(shared_snapshot);

    let actor_queue = snapshot.take_pending_fifo();
    assert_eq!(actor_queue.len(), QUEUED);
    assert!(snapshot.checkpoint_at_head.pending_fifo.is_empty());
    let moved_payload_ptr = match &actor_queue[0].request.input[0].content[0] {
        ContentBlock::Text { text } => text.as_ptr(),
        block => panic!("expected queued text payload, got {block:?}"),
    };
    assert_eq!(
        moved_payload_ptr, payload_ptr,
        "queue payloads must be moved"
    );

    let executor_snapshot = snapshot.clone();
    let core_snapshot = executor_snapshot.clone();
    assert!(Arc::ptr_eq(
        &snapshot.checkpoint_at_head.pending_fifo,
        &executor_snapshot.checkpoint_at_head.pending_fifo
    ));
    assert!(Arc::ptr_eq(
        &executor_snapshot.checkpoint_at_head.pending_fifo,
        &core_snapshot.checkpoint_at_head.pending_fifo
    ));
}

#[tokio::test]
async fn prior_frames_snapshot_skips_reconstruct_read() {
    let dir = broca_test_support::scratch_root()
        .join(format!("broca-prior-snapshot-{}-reuse", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let store = ReadCountingWalStore::new(&dir);
    let initial = req("prior-reuse", vec![], vec![]);
    seed_idle_run(&store, &initial);

    let writer = WalHandle::spawn(store.open(&initial.session).expect("open writer"), 1)
        .expect("spawn writer");
    let prior = store
        .read_records(&initial.session)
        .expect("snapshot read succeeds");
    assert_eq!(store.read_count(), 1, "executor took one prior snapshot");

    let snapshot = PriorWalSnapshot {
        base_watermark: 0,
        head_seq: prior.last().map_or(0, |frame| frame.seq),
        checkpoint_at_head: AdmissionCheckpointV1::from_frames(&prior).expect("checkpoint"),
        tail: prior,
        checkpoint_at_base: None,
    };

    let mut followup = req("prior-reuse", vec![], vec![]);
    followup.run_id = RunId("run-prior-reuse-followup".into());
    let provider = StubProvider::new(vec![text_step("second turn")]);
    let plane = StubToolPlane::new();
    let (out, _rx) = mpsc::channel(32);

    let summary = run_loop_on_writer_admitted_with_prior_snapshot(
        followup,
        &writer,
        StartMode::FreshOrResume,
        &provider,
        &plane,
        &AllowAll,
        &store,
        None,
        RunControl::default(),
        &out,
        None,
        Some(&snapshot),
    )
    .await
    .expect("run succeeds using the shared snapshot");

    assert_eq!(summary.reason, RunFinishReason::Completed);
    assert_eq!(
        store.read_count(),
        1,
        "core reconstruction reused the executor snapshot instead of reading the WAL again"
    );
    let _ = writer.shutdown().await;
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn stale_prior_frames_fall_back_to_reconstruct_read() {
    let dir = broca_test_support::scratch_root()
        .join(format!("broca-prior-snapshot-{}-stale", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let store = ReadCountingWalStore::new(&dir);
    let initial = req("prior-stale", vec![], vec![]);
    seed_idle_run(&store, &initial);

    let writer = WalHandle::spawn(store.open(&initial.session).expect("open writer"), 1)
        .expect("spawn writer");
    let prior = store
        .read_records(&initial.session)
        .expect("snapshot read succeeds");
    let stale_prefix = prior[..prior.len().saturating_sub(1)].to_vec();
    let stale_snapshot = PriorWalSnapshot {
        base_watermark: 0,
        head_seq: stale_prefix.last().map_or(0, |frame| frame.seq),
        checkpoint_at_head: AdmissionCheckpointV1::from_frames(&stale_prefix)
            .expect("stale checkpoint"),
        tail: stale_prefix,
        checkpoint_at_base: None,
    };

    let mut followup = req("prior-stale", vec![], vec![]);
    followup.run_id = RunId("run-prior-stale-followup".into());
    let provider = StubProvider::new(vec![text_step("fresh read")]);
    let plane = StubToolPlane::new();
    let (out, _rx) = mpsc::channel(32);

    let summary = run_loop_on_writer_admitted_with_prior_snapshot(
        followup,
        &writer,
        StartMode::FreshOrResume,
        &provider,
        &plane,
        &AllowAll,
        &store,
        None,
        RunControl::default(),
        &out,
        None,
        Some(&stale_snapshot),
    )
    .await
    .expect("run succeeds after rejecting the stale snapshot");

    assert_eq!(summary.reason, RunFinishReason::Completed);
    assert_eq!(
        store.read_count(),
        2,
        "stale snapshot forced one fresh reconstruct read"
    );
    let _ = writer.shutdown().await;
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn parallel_tool_call_jump_fires_nudge_once_before_next_step() {
    let (store, dir) = tmp_lease_store("budget-nudge");
    let provider = StubProvider::new(vec![
        tool_step(&[
            ("n1", "lookup", json!({"value": 1})),
            ("n2", "lookup", json!({"value": 2})),
            ("n3", "lookup", json!({"value": 3})),
        ]),
        text_step("done"),
    ]);
    let plane = StubToolPlane::new().with_canned_text("lookup", "ok");
    let mut request = req(
        "budget-nudge",
        vec![tool("lookup", ExecutionMode::Parallel)],
        vec![],
    );
    request.budget = Some(BudgetConfig {
        max_input_tokens: None,
        nudges: vec![BudgetNudge {
            at_tool_calls: 2,
            text: "Use the remaining calls carefully.\nKeep going verbatim.".into(),
        }],
        finalize_at_tool_calls: 99,
        finalize_text: "finalize".into(),
    });

    let (summary, events) = drive(request, provider.clone(), &plane, &store, &AllowAll).await;
    assert_eq!(summary.unwrap().reason, RunFinishReason::Completed);
    let calls = provider.calls();
    assert_eq!(calls.len(), 2);
    let injected: Vec<&str> = calls[1]
        .prompt
        .iter()
        .filter(|message| message.role == broca_protocol::Role::User)
        .flat_map(|message| message.content.iter())
        .filter_map(|block| match block {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        injected,
        vec!["Use the remaining calls carefully.\nKeep going verbatim."]
    );
    assert_eq!(
        control_units(&events)
            .iter()
            .filter(|unit| matches!(unit, ControlUnit::BudgetFired { .. }))
            .count(),
        1
    );

    let records_provider = StubProvider::new(vec![
        tool_step(&[
            ("w1", "lookup", json!({"value": 1})),
            ("w2", "lookup", json!({"value": 2})),
            ("w3", "lookup", json!({"value": 3})),
        ]),
        text_step("done"),
    ]);
    let mut record_request = req(
        "budget-nudge-wal",
        vec![tool("lookup", ExecutionMode::Parallel)],
        vec![],
    );
    record_request.budget = Some(BudgetConfig {
        max_input_tokens: None,
        nudges: vec![BudgetNudge {
            at_tool_calls: 2,
            text: "Use the remaining calls carefully.\nKeep going verbatim.".into(),
        }],
        finalize_at_tool_calls: 99,
        finalize_text: "finalize".into(),
    });
    let (_, records) = drive_wal(record_request, records_provider, &plane, &store, &AllowAll).await;
    let firings: Vec<_> = records
        .iter()
        .filter_map(|record| match record {
            WalRecord::BudgetFired {
                kind: BudgetKind::Nudge,
                threshold: 2,
                caller_text,
                ..
            } => caller_text.as_deref(),
            _ => None,
        })
        .collect();
    assert_eq!(
        firings,
        vec!["Use the remaining calls carefully.\nKeep going verbatim."]
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(
                record,
                WalRecord::BudgetFired {
                    kind: BudgetKind::Nudge,
                    threshold: 2,
                    ..
                }
            ))
            .count(),
        1
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn finalize_turn_keeps_tools_disables_calls_and_preserves_final_text() {
    let (store, dir) = tmp_lease_store("budget-finalize");
    let provider = StubProvider::new(vec![
        tool_step(&[("f1", "lookup", json!({}))]),
        text_step("the final answer"),
    ]);
    let plane = StubToolPlane::new().with_canned_text("lookup", "ok");
    let mut request = req(
        "budget-finalize",
        vec![tool("lookup", ExecutionMode::Parallel)],
        vec![],
    );
    request.budget = Some(BudgetConfig {
        max_input_tokens: None,
        nudges: vec![],
        finalize_at_tool_calls: 1,
        finalize_text: "Answer now without using tools.".into(),
    });

    let (summary, _) = drive(request, provider.clone(), &plane, &store, &AllowAll).await;
    let summary = summary.unwrap();
    assert_eq!(summary.reason, RunFinishReason::Completed);
    assert_eq!(summary.steps, 2);
    let calls = provider.calls();
    assert_eq!(calls.len(), 2);
    // The declared toolset must ride the finalize turn byte-identical (dropping the
    // array would re-encode the request head and re-bill the whole prompt prefix);
    // only tool_choice disables calling.
    assert_eq!(calls[1].tools, calls[0].tools);
    assert!(!calls[1].tools.is_empty());
    assert_eq!(calls[1].tool_choice.as_ref(), &ToolChoice::None);
    assert_eq!(calls[0].tool_choice.as_ref(), &ToolChoice::Auto);
    assert!(calls[1].prompt.iter().any(|message| {
        message.role == broca_protocol::Role::User
            && message.content
                == vec![ContentBlock::Text {
                    text: "Answer now without using tools.".into(),
                }]
    }));
    assert_eq!(plane.exec_count(&ToolCallId("f1".into())), 1);
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn finalize_turn_tool_calls_are_not_dispatched() {
    let (store, dir) = tmp_lease_store("budget-finalize-tool-call");
    let provider = StubProvider::new(vec![
        tool_step(&[("before", "lookup", json!({}))]),
        tool_step(&[("ignored", "lookup", json!({}))]),
    ]);
    let plane = StubToolPlane::new().with_canned_text("lookup", "ok");
    let mut request = req(
        "budget-finalize-tool-call",
        vec![tool("lookup", ExecutionMode::Parallel)],
        vec![],
    );
    request.budget = Some(BudgetConfig {
        max_input_tokens: None,
        nudges: vec![],
        finalize_at_tool_calls: 1,
        finalize_text: "Finalize even if tempted to call a tool.".into(),
    });

    let (summary, events, records) = drive_with_control(
        request,
        provider.clone(),
        &plane,
        &store,
        &AllowAll,
        RunControl::default(),
    )
    .await;
    assert_eq!(summary.unwrap().reason, RunFinishReason::Completed);
    assert_eq!(provider.calls().len(), 2);
    assert_eq!(plane.exec_count(&ToolCallId("before".into())), 1);
    assert_eq!(plane.exec_count(&ToolCallId("ignored".into())), 0);

    let before_call = ToolCall {
        thought_signature: None,
        tool_call_id: ToolCallId("before".into()),
        tool_name: "lookup".into(),
        input: json!({}),
    };
    let model_steps: Vec<_> = records
        .iter()
        .filter_map(|framed| match &framed.record {
            WalRecord::ModelStepFinished {
                assistant_message,
                finish_reason,
                ..
            } => Some((assistant_message, *finish_reason)),
            _ => None,
        })
        .collect();
    assert_eq!(model_steps.len(), 2);
    // The ordinary tool turn remains byte-for-byte intact in the WAL; only the final turn is
    // filtered.
    assert_eq!(
        model_steps[0].0.tool_calls().cloned().collect::<Vec<_>>(),
        vec![before_call.clone()]
    );
    assert_eq!(model_steps[0].1, Some(FinishReason::ToolCalls));
    assert!(model_steps[1].0.tool_calls().next().is_none());
    assert_eq!(model_steps[1].1, Some(FinishReason::Stop));

    // Direct consumers receive the same finalized assistant message that the WAL stores.
    assert!(control_units(&events).iter().any(|unit| matches!(
        unit,
        ControlUnit::AssistantMessage { message }
            if message.tool_calls().any(|call| call == &before_call)
    )));
    assert!(!control_units(&events).iter().any(|unit| matches!(
        unit,
        ControlUnit::AssistantMessage { message }
            if message
                .tool_calls()
                .any(|call| call.tool_call_id == ToolCallId("ignored".into()))
    )));

    let projected = broca_core::project_control(&records);
    // Projection preserves ordinary calls, but never advertises the undeclared finalize call.
    assert!(projected.iter().any(|(_, unit)| matches!(
        unit,
        ControlUnit::ToolCall { call } if call == &before_call
    )));
    assert!(!projected.iter().any(|(_, unit)| matches!(
        unit,
        ControlUnit::ToolCall { call }
            if call.tool_call_id == ToolCallId("ignored".into())
    )));
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn all_denied_threshold_firing_is_durable_before_replay_sees_it() {
    let (store, dir) = tmp_lease_store("deny-budget");
    let provider = StubProvider::new(vec![
        tool_step(&[("toolu_budget", "danger", json!({}))]),
        text_step("ok, skipping."),
    ]);
    let plane = StubToolPlane::new().with_canned_text("danger", "SHOULD-NOT-RUN");
    let deny = ScriptedApproval::new(
        [(
            ToolCallId("toolu_budget".into()),
            ApprovalResponse::Deny {
                tool_call_id: ToolCallId("toolu_budget".into()),
                reason: "nope".into(),
            },
        )]
        .into_iter()
        .collect(),
    );
    let mut request = req(
        "deny-budget",
        vec![tool("danger", ExecutionMode::Parallel)],
        vec![],
    );
    request.budget = Some(BudgetConfig {
        max_input_tokens: None,
        nudges: vec![BudgetNudge {
            at_tool_calls: 1,
            text: "threshold reached".into(),
        }],
        finalize_at_tool_calls: 99,
        finalize_text: "finalize".into(),
    });
    let (summary, records) = drive_wal(request, provider, &plane, &store, &deny).await;
    summary.expect("run ok");
    let turn_finished = records
        .iter()
        .position(|record| matches!(record, WalRecord::TurnFinished { .. }))
        .expect("denied turn is committed");
    let budget_fired = records
        .iter()
        .position(|record| matches!(record, WalRecord::BudgetFired { .. }))
        .expect("threshold firing is durable");
    assert!(turn_finished < budget_fired);
    let replayed = broca_core::replay(&records).expect("durable budget lineage replays");
    assert!(replayed.prompt.iter().any(|message| {
        message.content.iter().any(
            |block| matches!(block, ContentBlock::Text { text } if text == "threshold reached"),
        )
    }));
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn budgeted_live_prompt_and_replay_preserve_literal_firing_order() {
    let (store, dir) = tmp_lease_store("budget-kernel-callers");
    let provider = StubProvider::new(vec![
        tool_step(&[("k1", "lookup", json!({})), ("k2", "lookup", json!({}))]),
        text_step("final answer"),
    ]);
    let plane = StubToolPlane::new().with_canned_text("lookup", "ok");
    let mut request = req(
        "budget-kernel-callers",
        vec![tool("lookup", ExecutionMode::Parallel)],
        vec![],
    );
    request.budget = Some(BudgetConfig {
        max_input_tokens: None,
        nudges: vec![
            BudgetNudge {
                at_tool_calls: 2,
                text: "second threshold".into(),
            },
            BudgetNudge {
                at_tool_calls: 1,
                text: "first threshold".into(),
            },
            BudgetNudge {
                at_tool_calls: 2,
                text: "tied nudge".into(),
            },
        ],
        finalize_at_tool_calls: 2,
        finalize_text: "finalize now".into(),
    });
    let (summary, records) = drive_wal(request, provider.clone(), &plane, &store, &AllowAll).await;
    assert_eq!(summary.unwrap().reason, RunFinishReason::Completed);
    let calls = provider.calls();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[1].tool_choice.as_ref(), &ToolChoice::None);
    let replayed = broca_core::replay(&records).expect("replay real budgeted episode");
    let user_texts = |prompt: &[broca_protocol::Message]| -> Vec<String> {
        prompt
            .iter()
            .filter(|message| message.role == broca_protocol::Role::User)
            .flat_map(|message| message.content.iter())
            .filter_map(|block| match block {
                ContentBlock::Text { text } => Some(text.clone()),
                _ => None,
            })
            .collect()
    };
    let expected = vec![
        "first threshold",
        "second threshold",
        "tied nudge",
        "finalize now",
    ];
    assert_eq!(user_texts(&calls[1].prompt), expected, "live firing order");
    assert_eq!(
        user_texts(&replayed.prompt),
        expected,
        "replay firing order"
    );
    assert_eq!(
        user_texts(&calls[1].prompt),
        user_texts(&replayed.prompt),
        "two callers preserve the same firing order"
    );
    let firings: Vec<_> = records
        .iter()
        .filter_map(|record| match record {
            WalRecord::BudgetFired {
                kind,
                threshold,
                ordinal,
                ..
            } => Some((*kind, *threshold, *ordinal)),
            _ => None,
        })
        .collect();
    assert_eq!(
        firings,
        vec![
            (BudgetKind::Nudge, 1, 2),
            (BudgetKind::Nudge, 2, 3),
            (BudgetKind::Nudge, 2, 4),
            (BudgetKind::Finalize, 2, 5)
        ]
    );
    assert_eq!(replayed.tool_call_count, 2);
    assert_eq!(replayed.budget_nudges_fired, vec![1, 0, 2]);
    assert!(replayed.budget_finalize_fired);
    assert!(!replayed.budget_finalize_pending);
    let _ = std::fs::remove_dir_all(dir);
}

/// Every `tool_result` record in `records`, as (call id, outcome, is_error, reason, text).
fn tool_results_in(
    records: &[WalRecord],
) -> Vec<(String, DispatchOutcome, bool, Option<String>, String)> {
    records
        .iter()
        .filter_map(|record| match record {
            WalRecord::ToolResult {
                result,
                outcome,
                reason,
                ..
            } => Some((
                result.tool_call_id.0.clone(),
                *outcome,
                result.is_error,
                reason.clone(),
                match &result.output {
                    ToolOutput::Error { message } => message.clone(),
                    other => format!("{other:?}"),
                },
            )),
            _ => None,
        })
        .collect()
}

fn run_finished_reason(records: &[WalRecord]) -> Option<RunFinishReason> {
    records.iter().rev().find_map(|record| match record {
        WalRecord::RunFinished { reason, .. } => Some(*reason),
        _ => None,
    })
}

/// A call a scope drain caught before it was sent: its intent is durable, its result is
/// `route_drained` naming the reason (outcome `completed`, `is_error`, typed reason), and the
/// run goes on to the model, which sees the refusal.
#[tokio::test]
async fn a_drain_before_send_records_route_drained_and_the_run_goes_on() {
    let (store, dir) = tmp_lease_store("drain-unsent");
    let provider = StubProvider::new(vec![
        tool_step(&[("toolu_1", "edit", json!({"path": "a"}))]),
        text_step("the route was drained"),
    ]);
    let plane = StubToolPlane::new().with_tool("edit", |_| StubReply::ScopeDrained {
        reason: "scope_delegation_changed".into(),
        sent: false,
    });

    let (summary, records) = drive_wal(
        req(
            "drain-unsent",
            vec![tool("edit", ExecutionMode::Parallel)],
            vec![],
        ),
        provider,
        &plane,
        &store,
        &AllowAll,
    )
    .await;

    let summary = summary.expect("run ok");
    assert_eq!(summary.reason, RunFinishReason::Completed);
    assert_eq!(summary.steps, 2, "the model saw the refusal and answered");
    assert!(records
        .iter()
        .any(|record| matches!(record, WalRecord::ToolDispatchIntent { .. })));
    let results = tool_results_in(&records);
    assert_eq!(results.len(), 1);
    let (_, outcome, is_error, reason, text) = &results[0];
    assert_eq!(*outcome, DispatchOutcome::Completed);
    assert!(is_error);
    assert_eq!(reason.as_deref(), Some("scope_delegation_changed"));
    assert!(text.starts_with("route_drained: "), "{text}");
    let _ = std::fs::remove_dir_all(dir);
}

/// A call a scope drain caught after it was sent: outcome `unknown` with the typed reason, and
/// the run stops Interrupted, never Cancelled, without feeding the unknown to the model.
#[tokio::test]
async fn a_drain_after_send_records_unknown_and_stops_interrupted_never_cancelled() {
    let (store, dir) = tmp_lease_store("drain-sent");
    let provider = StubProvider::new(vec![
        tool_step(&[("toolu_1", "edit", json!({"path": "a"}))]),
        text_step("should not be reached"),
    ]);
    let plane = StubToolPlane::new().with_tool("edit", |_| StubReply::ScopeDrained {
        reason: "scope_delegation_changed".into(),
        sent: true,
    });

    let (summary, records) = drive_wal(
        req(
            "drain-sent",
            vec![tool("edit", ExecutionMode::Parallel)],
            vec![],
        ),
        provider,
        &plane,
        &store,
        &AllowAll,
    )
    .await;

    let summary = summary.expect("run ok");
    assert_eq!(summary.reason, RunFinishReason::Interrupted);
    assert_eq!(summary.steps, 1, "no model step after the unknown outcome");
    assert_eq!(
        run_finished_reason(&records),
        Some(RunFinishReason::Interrupted)
    );
    let results = tool_results_in(&records);
    assert_eq!(results.len(), 1);
    let (_, outcome, is_error, reason, _) = &results[0];
    assert_eq!(*outcome, DispatchOutcome::Unknown);
    assert!(is_error);
    assert_eq!(reason.as_deref(), Some("scope_delegation_changed"));
    let _ = std::fs::remove_dir_all(dir);
}

/// A drain that ended the run's calls with nothing in flight is seen at the next batch's
/// start: every call of the batch gets `route_drained` naming the reason, none is dispatched
/// or given an intent, and the run stops Interrupted.
#[tokio::test]
async fn a_run_ending_drain_at_batch_start_drains_every_call_and_stops_interrupted() {
    let (store, dir) = tmp_lease_store("drain-idle");
    let provider = StubProvider::new(vec![
        tool_step(&[
            ("toolu_1", "edit", json!({"path": "a"})),
            ("toolu_2", "edit", json!({"path": "b"})),
        ]),
        text_step("should not be reached"),
    ]);
    let plane = StubToolPlane::new().with_canned_text("edit", "ran");
    plane.fail_next_batch_start(ToolPlaneError::ScopeDrained {
        reason: "scope_parent_ended".into(),
    });

    let (summary, records) = drive_wal(
        req(
            "drain-idle",
            vec![tool("edit", ExecutionMode::Parallel)],
            vec![],
        ),
        provider,
        &plane,
        &store,
        &AllowAll,
    )
    .await;

    let summary = summary.expect("run ok");
    assert_eq!(summary.reason, RunFinishReason::Interrupted);
    assert_eq!(summary.steps, 1);
    assert_eq!(
        run_finished_reason(&records),
        Some(RunFinishReason::Interrupted)
    );
    assert!(!records
        .iter()
        .any(|record| matches!(record, WalRecord::ToolDispatchIntent { .. })));
    let results = tool_results_in(&records);
    assert_eq!(results.len(), 2);
    for (id, outcome, is_error, reason, text) in &results {
        assert_eq!(*outcome, DispatchOutcome::Completed, "{id}");
        assert!(is_error, "{id}");
        assert_eq!(reason.as_deref(), Some("scope_parent_ended"), "{id}");
        assert!(text.starts_with("route_drained: "), "{id}: {text}");
    }
    assert_eq!(plane.exec_count(&ToolCallId("toolu_1".into())), 0);
    assert_eq!(plane.exec_count(&ToolCallId("toolu_2".into())), 0);
    let _ = std::fs::remove_dir_all(dir);
}

/// A scope refusal at batch start is a durable no-dispatch naming its code, and the run goes
/// on. (An ordinary tool-plane failure still records its old detail text, which sessions on
/// disk already hold.)
#[tokio::test]
async fn a_scope_refusal_at_batch_start_is_a_no_dispatch_naming_its_code() {
    let (store, dir) = tmp_lease_store("scope-refused");
    let provider = StubProvider::new(vec![
        tool_step(&[("toolu_1", "edit", json!({"path": "a"}))]),
        text_step("done"),
    ]);
    let plane = StubToolPlane::new().with_canned_text("edit", "ran");
    plane.fail_next_batch_start(ToolPlaneError::ScopeRefused {
        code: "scope_unverifiable".into(),
        detail: "held scope_not_synced past the bound".into(),
    });

    let (summary, records) = drive_wal(
        req(
            "scope-refused",
            vec![tool("edit", ExecutionMode::Parallel)],
            vec![],
        ),
        provider,
        &plane,
        &store,
        &AllowAll,
    )
    .await;

    assert_eq!(summary.expect("run ok").reason, RunFinishReason::Completed);
    let details: Vec<String> = records
        .iter()
        .filter_map(|record| match record {
            WalRecord::ToolNoDispatch {
                reason: NoDispatchReason::Unavailable { detail },
                ..
            } => Some(detail.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        details,
        vec!["scope_unverifiable: held scope_not_synced past the bound".to_owned()]
    );
    assert_eq!(plane.exec_count(&ToolCallId("toolu_1".into())), 0);
    let _ = std::fs::remove_dir_all(dir);
}

/// The tool plane sees the scope the run was admitted under on every batch it dispatches, so
/// it can open the run's provider routes under exactly that scope.
#[tokio::test]
async fn every_batch_carries_the_runs_frozen_scope() {
    let (store, dir) = tmp_lease_store("scoped-batches");
    let provider = StubProvider::new(vec![
        tool_step(&[("toolu_1", "edit", json!({"path": "a"}))]),
        tool_step(&[("toolu_2", "edit", json!({"path": "b"}))]),
        text_step("done"),
    ]);
    let plane = StubToolPlane::new().with_canned_text("edit", "ran");
    let scope = broca_protocol::SessionScope {
        owner: broca_protocol::BindPrincipal::Reserved {
            module_id: "prefrontal".into(),
        },
        scope_ref: "head-1".into(),
        scope_epoch: 4,
        flow_id: None,
        sender: None,
        agent_id: None,
        run_id: None,
    };
    let mut request = req(
        "scoped-batches",
        vec![tool("edit", ExecutionMode::Parallel)],
        vec![],
    );
    request.scope = Some(scope.clone());

    let (summary, _records) = drive_wal(request, provider, &plane, &store, &AllowAll).await;

    assert_eq!(summary.expect("run ok").reason, RunFinishReason::Completed);
    assert_eq!(plane.batch_scopes(), vec![Some(scope.clone()), Some(scope)]);
    let _ = std::fs::remove_dir_all(dir);
}

mod keep_warm_scheduler {
    use super::*;
    use broca_core::keep_warm::WarmCosts;
    use broca_core::keep_warm_wait::{KeepWarmDriver, WarmTick, WarmTiming};
    use broca_core::{CacheClock, CacheClockReading};
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;
    use tokio::sync::Notify;
    use tracing::instrument::WithSubscriber;

    struct HeldTool {
        started: Notify,
        release: Notify,
        clock: CacheClock,
        ttl_ms: u64,
    }
    #[async_trait::async_trait]
    impl ToolPlane for HeldTool {
        async fn batch_started(
            &self,
            _ctx: &BatchCtx,
            _calls: &[ToolCall],
            _manifest: &ToolManifest,
        ) -> Result<(), ToolPlaneError> {
            Ok(())
        }
        async fn batch_finished(&self, _ctx: &BatchCtx) -> Result<(), ToolPlaneError> {
            Ok(())
        }
        async fn dispatch(&self, _ctx: &BatchCtx, call: &ToolCall) -> DispatchResult {
            self.clock.note_sent(now(), Some(self.ttl_ms));
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

    struct Driver {
        timing: WarmTiming,
        writer: Arc<WalHandle>,
        clock: CacheClock,
        sends: AtomicUsize,
        skips: AtomicUsize,
        in_flight: AtomicBool,
        hanging: bool,
        costs: WarmCosts,
        started: Notify,
        readings: Mutex<Vec<CacheClockReading>>,
        responses_usage: Option<Usage>,
    }
    struct Active<'a>(&'a AtomicBool);
    impl Drop for Active<'_> {
        fn drop(&mut self) {
            self.0.store(false, Ordering::SeqCst);
        }
    }
    fn now() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    #[async_trait::async_trait]
    impl KeepWarmDriver for Driver {
        fn timing(&self, _config: &RunConfig) -> Result<WarmTiming, String> {
            Ok(self.timing)
        }
        fn last_sent_at_ms(&self) -> Option<u64> {
            self.clock.read().map(|r| r.last_sent_at_ms)
        }
        fn costs(&self, _config: &RunConfig) -> Result<WarmCosts, String> {
            Ok(self.costs)
        }
        async fn warm(&self, _config: &RunConfig, run_id: &RunId) -> WarmTick {
            if self.in_flight.swap(true, Ordering::SeqCst) {
                self.skips.fetch_add(1, Ordering::SeqCst);
                return WarmTick::Skipped;
            }
            let _active = Active(&self.in_flight);
            self.started.notify_one();
            if self.hanging {
                return std::future::pending().await;
            }
            let reading = self.clock.read().unwrap();
            assert!(
                now() < reading.expires_at_ms().unwrap(),
                "scheduler must fire before expiry"
            );
            self.readings.lock().unwrap().push(reading);
            let sent_at_ms = now();
            let usage = Usage {
                input_tokens: Some(164_384),
                cached_input_tokens: Some(15_616),
                ..Usage::default()
            };
            self.writer.record(WalRecord::CacheWarmed {
                trigger: Some("keep_warm".into()),
                run_id: run_id.clone(),
                step_id: StepId(1),
                request_body_sha256: "0".repeat(64),
                warm_body_sha256: "1".repeat(64),
                usage,
                served_account_ref: None,
                ratelimit_headers: Default::default(),
                sent_at_ms,
                ttl_ms: Some(self.timing.ttl_ms),
                expires_at_ms: Some(sent_at_ms + self.timing.ttl_ms),
            });
            self.clock.note_sent(sent_at_ms, Some(self.timing.ttl_ms));
            self.clock.note_usage(sent_at_ms, usage);
            self.sends.fetch_add(1, Ordering::SeqCst);
            self.writer.sync().await.unwrap();
            match self.responses_usage {
                Some(usage) => WarmTick::ResponsesSent {
                    usage,
                    cost: usage
                        .output_tokens
                        .map(|tokens| tokens as f64)
                        .unwrap_or(self.costs.warm),
                },
                None => WarmTick::Sent,
            }
        }
    }

    async fn wait_sends(driver: &Driver, wanted: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while driver.sends.load(Ordering::SeqCst) < wanted {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "warm sends observed: {}, wanted: {wanted}, skips: {}",
                driver.sends.load(Ordering::SeqCst),
                driver.skips.load(Ordering::SeqCst)
            )
        });
    }

    async fn wait_stops(stops: &AtomicUsize, wanted: usize, reason: &str) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while stops.load(Ordering::SeqCst) < wanted {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "keep_warm {reason} stops observed: {}, wanted: {wanted}",
                stops.load(Ordering::SeqCst)
            )
        });
    }

    async fn held_run(
        policy: Option<broca_protocol::request::KeepWarm>,
        costs: WarmCosts,
        hanging: bool,
        cancel: bool,
        manual: bool,
    ) -> (usize, Vec<WalRecord>) {
        held_run_with_usage(policy, costs, hanging, cancel, manual, None).await
    }

    async fn held_run_with_usage(
        policy: Option<broca_protocol::request::KeepWarm>,
        costs: WarmCosts,
        hanging: bool,
        cancel: bool,
        manual: bool,
        responses_usage: Option<Usage>,
    ) -> (usize, Vec<WalRecord>) {
        let wal = CountingWalStore::default();
        let mut request = req(
            "keep-warm-held",
            vec![tool("held", ExecutionMode::Parallel)],
            vec![],
        );
        request.keep_warm = policy;
        let writer = Arc::new(WalHandle::spawn(wal.open(&request.session).unwrap(), 1).unwrap());
        let timing = WarmTiming {
            // The scheduler reads SystemTime as well as Tokio's timer clock, so pausing
            // Tokio alone cannot virtualize expiry. Separate cache life from test cadence.
            ttl_ms: 60_000,
            lead_ms: 59_950,
            after_send_ms: None,
        };
        let clock = CacheClock::new();
        clock.note_sent(now(), Some(timing.ttl_ms));
        let driver = Arc::new(Driver {
            timing,
            writer: writer.clone(),
            clock: clock.clone(),
            sends: AtomicUsize::new(0),
            skips: AtomicUsize::new(0),
            in_flight: AtomicBool::new(manual),
            hanging,
            costs,
            started: Notify::new(),
            readings: Mutex::new(Vec::new()),
            responses_usage,
        });
        let plane = HeldTool {
            started: Notify::new(),
            release: Notify::new(),
            clock,
            ttl_ms: timing.ttl_ms,
        };
        let mut steps = vec![tool_step(&[("held-call", "held", json!({}))])];
        if responses_usage.is_some() {
            steps.push(tool_step(&[("held-again", "held", json!({}))]));
        }
        steps.push(text_step("finished"));
        let provider = StubProvider::new(steps);
        let cancellation = tokio_util::sync::CancellationToken::new();
        let control = RunControl {
            keep_warm: Some(driver.clone()),
            cancel: Some(cancellation.clone()),
            ..Default::default()
        };
        let (tx, mut rx) = mpsc::channel(512);
        let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let missed =
            responses_usage.is_some_and(|usage| usage.cached_input_tokens < usage.input_tokens);
        let budget_limited = costs.bust == 3.0;
        let stop_reason = if missed { "warm_missed" } else { "budget" };
        let stops = Arc::new(AtomicUsize::new(0));
        let run = broca_core::run_loop_on_writer_admitted(
            request,
            &writer,
            StartMode::FreshOrResume,
            &provider,
            &plane,
            &AllowAll,
            &wal,
            None,
            control,
            &tx,
            None,
        )
        .with_subscriber(super::unsupported_warm_logging::Stops(
            stops.clone(),
            stop_reason,
        ));
        let inspect = async {
            plane.started.notified().await;
            if hanging {
                tokio::time::timeout(Duration::from_secs(2), driver.started.notified())
                    .await
                    .unwrap();
                assert!(driver.in_flight.load(Ordering::SeqCst));
                if cancel {
                    cancellation.cancel();
                    tokio::time::timeout(Duration::from_secs(2), async {
                        while driver.in_flight.load(Ordering::SeqCst) {
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .unwrap();
                }
            } else if manual {
                tokio::time::timeout(Duration::from_secs(2), async {
                    while driver.skips.load(Ordering::SeqCst) == 0 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "expected a skipped manual flight; skips observed: {}",
                        driver.skips.load(Ordering::SeqCst)
                    )
                });
                assert_eq!(driver.sends.load(Ordering::SeqCst), 0);
                driver.in_flight.store(false, Ordering::SeqCst);
                wait_sends(&driver, 1).await;
            } else if policy == Some(broca_protocol::request::KeepWarm::Auto) {
                let target = match responses_usage {
                    Some(usage) if usage.cached_input_tokens < usage.input_tokens => 1,
                    Some(usage) if usage.output_tokens == Some(2) => 1,
                    _ if costs.bust == 3.0 => 2,
                    _ => 3,
                };
                wait_sends(&driver, target).await;
                if budget_limited || missed {
                    // A send count does not prove the scheduler has charged the returned
                    // cost and stopped. Wait for that decision before ending this tool wait.
                    wait_stops(&stops, 1, stop_reason).await;
                }
            } else {
                tokio::time::sleep(Duration::from_millis(550)).await;
            }
            // Capture the next wait's baseline before dispatch can send its first warm.
            // Reading it after started.notified() races the scheduler on a loaded runner.
            let next_wait_sends = driver.sends.load(Ordering::SeqCst) + 1;
            plane.release.notify_one();
            if responses_usage.is_some() {
                plane.started.notified().await;
                if missed {
                    tokio::time::sleep(Duration::from_millis(550)).await;
                } else {
                    if budget_limited {
                        // Let the second wait spend its budget even if the observer is late.
                        // Synchronizing on its stop must still see exactly one new warm.
                        wait_stops(&stops, 2, stop_reason).await;
                    }
                    wait_sends(&driver, next_wait_sends).await;
                }
                plane.release.notify_one();
            }
        };
        let (summary, ()) =
            tokio::time::timeout(Duration::from_secs(4), async { tokio::join!(run, inspect) })
                .await
                .unwrap();
        assert_eq!(
            summary.unwrap().reason,
            if cancel {
                RunFinishReason::Cancelled
            } else {
                RunFinishReason::Completed
            }
        );
        assert!(!driver.in_flight.load(Ordering::SeqCst));
        if missed {
            assert_eq!(
                stops.load(Ordering::SeqCst),
                1,
                "a miss stops the whole run"
            );
        }
        drop(tx);
        collector.await.unwrap();
        let sends = driver.sends.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            driver.sends.load(Ordering::SeqCst),
            sends,
            "tools completing must disarm maintenance"
        );
        let records = wal
            .state
            .records
            .lock()
            .unwrap()
            .iter()
            .map(|frame| frame.record.clone())
            .collect::<Vec<_>>();
        assert_eq!(records.iter().filter(|record| matches!(record, WalRecord::CacheWarmed { trigger: Some(trigger), .. } if trigger == "keep_warm")).count(), sends);
        let intent = records
            .iter()
            .position(|record| matches!(record, WalRecord::ToolDispatchIntent { .. }))
            .unwrap();
        for (index, record) in records.iter().enumerate() {
            if matches!(record, WalRecord::CacheWarmed { .. }) {
                assert!(index > intent);
            }
        }
        (sends, records)
    }

    #[tokio::test]
    async fn real_loop_partial_head_miss_stops_after_one_warm() {
        let (sends, _) = held_run_with_usage(
            Some(broca_protocol::request::KeepWarm::Auto),
            WarmCosts {
                warm: 1.0,
                bust: 1000.0,
            },
            false,
            false,
            false,
            Some(Usage {
                input_tokens: Some(164_384),
                cached_input_tokens: Some(15_616),
                ..Usage::default()
            }),
        )
        .await;
        assert_eq!(sends, 1);
    }

    #[tokio::test]
    async fn real_loop_non_responses_low_cached_usage_does_not_stop_warming() {
        let (sends, records) = held_run(
            Some(broca_protocol::request::KeepWarm::Auto),
            WarmCosts {
                warm: 1.0,
                bust: 1000.0,
            },
            false,
            false,
            false,
        )
        .await;
        assert!(sends >= 2);
        assert!(records.iter().any(|record| matches!(record, WalRecord::CacheWarmed { usage, .. } if usage.input_tokens == Some(164_384) && usage.cached_input_tokens == Some(15_616))));
    }

    #[tokio::test]
    async fn real_loop_responses_budget_uses_returned_cost_not_quote() {
        let (sends, _) = held_run_with_usage(
            Some(broca_protocol::request::KeepWarm::Auto),
            WarmCosts {
                warm: 1.0,
                bust: 3.0,
            },
            false,
            false,
            false,
            Some(Usage {
                input_tokens: Some(10_000),
                cached_input_tokens: Some(170_000),
                output_tokens: Some(2),
                ..Usage::default()
            }),
        )
        .await;
        assert_eq!(
            sends, 2,
            "one warm per tool wait at the actual cost, not two at the quote"
        );
    }

    #[tokio::test]
    async fn real_loop_responses_prefix_hit_keeps_cadence() {
        let (sends, _) = held_run_with_usage(
            Some(broca_protocol::request::KeepWarm::Auto),
            WarmCosts {
                warm: 1.0,
                bust: 1000.0,
            },
            false,
            false,
            false,
            Some(Usage {
                input_tokens: Some(10_000),
                cached_input_tokens: Some(170_000),
                ..Usage::default()
            }),
        )
        .await;
        assert!(sends >= 2);
    }

    #[tokio::test]
    async fn real_loop_keep_warm_refreshes_multiple_times_and_disarms_on_tool_result() {
        let (sends, _) = held_run(
            Some(broca_protocol::request::KeepWarm::Auto),
            WarmCosts {
                warm: 1.0,
                bust: 1000.0,
            },
            false,
            false,
            false,
        )
        .await;
        assert!(
            sends >= 3,
            "the real tool wait must cover several refreshes"
        );
    }
    #[tokio::test]
    async fn real_loop_keep_warm_gate_stops_at_the_computed_boundary() {
        let (sends, _) = held_run(
            Some(broca_protocol::request::KeepWarm::Auto),
            WarmCosts {
                warm: 1.0,
                bust: 3.0,
            },
            false,
            false,
            false,
        )
        .await;
        assert_eq!(sends, 2);
    }
    #[tokio::test]
    async fn real_loop_keep_warm_off_and_absent_send_nothing_on_the_same_hold() {
        for policy in [None, Some(broca_protocol::request::KeepWarm::Off)] {
            assert_eq!(
                held_run(
                    policy,
                    WarmCosts {
                        warm: 1.0,
                        bust: 1000.0
                    },
                    false,
                    false,
                    false
                )
                .await
                .0,
                0
            );
        }
    }
    #[tokio::test]
    async fn real_loop_keep_warm_drops_a_hanging_warm_before_advancing_or_cancelling() {
        for cancel in [false, true] {
            assert_eq!(
                held_run(
                    Some(broca_protocol::request::KeepWarm::Auto),
                    WarmCosts {
                        warm: 1.0,
                        bust: 1000.0
                    },
                    true,
                    cancel,
                    false
                )
                .await
                .0,
                0
            );
        }
    }
    #[tokio::test]
    async fn real_loop_keep_warm_skips_an_existing_manual_flight_and_reschedules() {
        assert_eq!(
            held_run(
                Some(broca_protocol::request::KeepWarm::Auto),
                WarmCosts {
                    warm: 1.0,
                    bust: 1000.0
                },
                false,
                false,
                true
            )
            .await
            .0,
            1
        );
    }
}

mod unsupported_warm_logging {
    use super::*;
    use broca_core::keep_warm_wait::{KeepWarmDriver, WarmTick, WarmTiming};

    struct Unsupported;
    #[async_trait::async_trait]
    impl KeepWarmDriver for Unsupported {
        fn timing(&self, _: &RunConfig) -> Result<WarmTiming, String> {
            Err("no cache hints".into())
        }
        fn last_sent_at_ms(&self) -> Option<u64> {
            None
        }
        fn costs(&self, _: &RunConfig) -> Result<broca_core::keep_warm::WarmCosts, String> {
            panic!("unsupported timing must stop before pricing")
        }
        async fn warm(&self, _: &RunConfig, _: &RunId) -> WarmTick {
            panic!("unsupported timing must never warm")
        }
    }

    pub(super) struct Stops(pub(super) Arc<AtomicUsize>, pub(super) &'static str);
    impl tracing::Subscriber for Stops {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            struct StopField(bool, &'static str);
            impl tracing::field::Visit for StopField {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    if field.name() == "stop" && format!("{value:?}") == format!("{:?}", self.1) {
                        self.0 = true;
                    }
                }
            }
            let mut stop = StopField(false, self.1);
            event.record(&mut stop);
            if event.metadata().target() == "cache_warm" && stop.0 {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    #[tokio::test]
    async fn unsupported_stop_is_logged_once_per_run_across_tool_waits() {
        let logs = broca_test_support::LogCapture::new();
        for run in 1..=2 {
            let wal = CountingWalStore::default();
            let mut request = req(
                "unsupported-log",
                vec![tool("work", ExecutionMode::Parallel)],
                vec![],
            );
            request.keep_warm = Some(broca_protocol::request::KeepWarm::Auto);
            let provider = StubProvider::new(vec![
                tool_step(&[("one", "work", json!({}))]),
                tool_step(&[("two", "work", json!({}))]),
                tool_step(&[("three", "work", json!({}))]),
                text_step("done"),
            ]);
            let writer =
                Arc::new(WalHandle::spawn(wal.open(&request.session).unwrap(), 1).unwrap());
            let (tx, _rx) = mpsc::channel(512);
            let summary = logs
                .scope_future(broca_core::run_loop_on_writer_admitted(
                    request,
                    &writer,
                    StartMode::FreshOrResume,
                    &provider,
                    &StubToolPlane::default(),
                    &AllowAll,
                    &wal,
                    None,
                    RunControl {
                        keep_warm: Some(Arc::new(Unsupported)),
                        ..Default::default()
                    },
                    &tx,
                    None,
                ))
                .await
                .unwrap();
            assert_eq!(summary.reason, RunFinishReason::Completed);
            assert_eq!(
                logs.events()
                    .iter()
                    .filter(|event| event.target == "cache_warm"
                        && event
                            .fields
                            .get("stop")
                            .is_some_and(|stop| stop == "unsupported"))
                    .count(),
                run,
                "one unsupported stop per run, not per wait"
            );
        }
    }
}

fn trailing_garbage_step(with_good_call: bool) -> Vec<NormalizedEvent> {
    let mut calls = vec![("bad", "lookup", json!({"key":"alpha"}))];
    if with_good_call {
        calls.insert(0, ("good", "lookup", json!({"key":"beta"})));
    }
    let mut events = tool_step(&calls);
    events.insert(
        1,
        ctrl(ProviderControl::ToolArgsTrailingGarbage {
            cut: broca_protocol::provider::ToolArgsTrailingGarbage {
                tool_call_id: ToolCallId("bad".into()),
                tool_name: "lookup".into(),
                json_bytes: 15,
                whitespace_bytes: 4097,
                omitted_tool_calls: 1,
            },
        }),
    );
    events.insert(1, ctrl(ProviderControl::AttemptFinished {
        attempt: serde_json::from_value(json!({
            "attempt":0, "request_body_len":0, "request_body_sha256":broca_core::request_digest_hex(&[]),
            "response_body_len":0, "response_body_sha256":broca_core::request_digest_hex(&[]),
            "http_status":200, "ts_ms":0, "outcome":"tool_args_trailing_garbage",
        })).unwrap(),
    }));
    events
}

#[tokio::test]
async fn trailing_garbage_step_commits_error_without_dispatch_and_model_retries() {
    for with_good_call in [false, true] {
        let name = if with_good_call {
            "trailing-garbage-mixed"
        } else {
            "trailing-garbage-only"
        };
        let (leases, dir) = tmp_lease_store(name);
        let provider = StubProvider::new(vec![
            trailing_garbage_step(with_good_call),
            text_step("retried"),
        ]);
        let observed = provider.clone();
        let plane = StubToolPlane::new().with_tool("lookup", |_| {
            StubReply::Output(ToolOutput::Text { text: "ok".into() })
        });
        let (summary, records) = drive_wal(
            req(name, vec![tool("lookup", ExecutionMode::Parallel)], vec![]),
            provider,
            &plane,
            &leases,
            &AllowAll,
        )
        .await;
        assert_eq!(summary.unwrap().steps, 2);
        assert!(records.iter().any(
            |r| matches!(r, WalRecord::ModelAttemptFinished { outcome, .. }
            if *outcome == broca_protocol::AttemptOutcome::ToolArgsTrailingGarbage)
        ));
        assert_eq!(
            records
                .iter()
                .filter(|r| matches!(r, WalRecord::ModelStepFinished { .. }))
                .count(),
            2
        );
        assert!(!records.iter().any(|r| matches!(r, WalRecord::ToolDispatchIntent { tool_call_id, .. } if tool_call_id.0 == "bad")));
        let error = records
            .iter()
            .find_map(|r| match r {
                WalRecord::ToolResult { result, reason, .. } if result.tool_call_id.0 == "bad" => {
                    assert_eq!(reason.as_deref(), Some("tool_args_trailing_garbage"));
                    Some(result)
                }
                _ => None,
            })
            .unwrap();
        assert!(error.is_error);
        let ToolOutput::Error { message } = &error.output else {
            panic!("expected malformed-call error");
        };
        assert!(message.starts_with("tool_args_trailing_garbage:"));
        assert!(message.contains("properly escaped"));
        assert!(message.contains("json_bytes=15, whitespace_bytes=4097"));
        assert!(message.contains("Omitted 1 other incomplete"));
        assert_eq!(observed.calls().len(), 2);
        assert!(observed.calls()[1]
            .prompt
            .iter()
            .flat_map(|m| &m.content)
            .any(|block| matches!(block,
            ContentBlock::ToolResult(result) if result == error.as_ref())));
        assert_eq!(
            records
                .iter()
                .filter(|r| matches!(r, WalRecord::ToolDispatchIntent { .. }))
                .count(),
            usize::from(with_good_call)
        );
        let _ = std::fs::remove_dir_all(dir);
    }
}

#[tokio::test]
async fn trailing_garbage_keeps_its_error_at_a_hard_stop() {
    let (leases, dir) = tmp_lease_store("trailing-garbage-stop");
    let provider = StubProvider::new(vec![trailing_garbage_step(false)]);
    let plane = StubToolPlane::new();
    let (summary, records) = drive_wal(
        req(
            "trailing-garbage-stop",
            vec![tool("lookup", ExecutionMode::Parallel)],
            vec![StopCondition::StepCount { n: 1 }],
        ),
        provider,
        &plane,
        &leases,
        &AllowAll,
    )
    .await;
    assert_eq!(summary.unwrap().steps, 1);
    assert!(!records
        .iter()
        .any(|r| matches!(r, WalRecord::ToolDispatchIntent { .. })));
    assert!(records.iter().any(|r| matches!(r, WalRecord::ToolResult { result, .. }
        if matches!(&result.output, ToolOutput::Error { message } if message.starts_with("tool_args_trailing_garbage:")))));
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn trailing_garbage_crash_prefixes_never_dispatch_the_rejected_call() {
    let (leases, baseline_dir) = tmp_lease_store("trailing-garbage-prefix-baseline");
    let request = req(
        "trailing-garbage-prefix",
        vec![tool("lookup", ExecutionMode::Parallel)],
        vec![],
    );
    let provider = StubProvider::new(vec![trailing_garbage_step(true), text_step("done")]);
    let plane = StubToolPlane::new().with_tool("lookup", |_| {
        StubReply::Output(ToolOutput::Text { text: "ok".into() })
    });
    let (summary, reference) =
        drive_wal(request.clone(), provider, &plane, &leases, &AllowAll).await;
    assert_eq!(summary.unwrap().steps, 2);
    for keep in 1..=reference.len() {
        let tag = format!("trailing-garbage-prefix-{keep}");
        let (leases, dir) = tmp_lease_store(&tag);
        let wal = FileWalStore::new(dir.join("wal"));
        let mut file = wal.open(&request.session).unwrap();
        for record in reference.iter().take(keep) {
            file.append(1, record, true).unwrap();
        }
        drop(file);
        let committed_step = reference[..keep].iter().any(|record| {
            matches!(record, WalRecord::ModelStepFinished { assistant_message, .. }
            if assistant_message.tool_calls().any(|call| call.tool_call_id.0 == "bad"))
        });
        let exact_result = reference[..keep].iter().any(|record| matches!(record, WalRecord::ToolResult { result, .. } if result.tool_call_id.0 == "bad"));
        let resumed = StubProvider::new(vec![text_step("after recovery")]);
        let plane = StubToolPlane::new().with_tool("lookup", |_| {
            StubReply::Output(ToolOutput::Text {
                text: "must not execute bad".into(),
            })
        });
        let (tx, mut rx) = mpsc::channel(4096);
        let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let summary = run_loop(
            request.clone(),
            &resumed,
            &plane,
            &leases,
            &AllowAll,
            &wal,
            &tx,
        )
        .await
        .unwrap();
        drop(tx);
        collector.await.unwrap();
        assert_eq!(
            plane.exec_count(&ToolCallId("bad".into())),
            0,
            "record boundary {keep}"
        );
        let frames = wal.read_records(&request.session).unwrap();
        let unresolved_intent = reference[..keep].iter().any(|record| matches!(record, WalRecord::ToolDispatchIntent { tool_call_id, .. } if tool_call_id.0 == "good"))
            && !reference[..keep].iter().any(|record| matches!(record, WalRecord::ToolResult { result, .. } if result.tool_call_id.0 == "good"));
        if unresolved_intent {
            assert_eq!(
                summary.reason,
                RunFinishReason::Interrupted,
                "record boundary {keep}"
            );
            assert_eq!(plane.exec_count(&ToolCallId("good".into())), 0);
            assert!(frames.iter().any(|frame| matches!(&frame.record, WalRecord::ToolResult { result, outcome: DispatchOutcome::Unknown, .. } if result.tool_call_id.0 == "good")));
        }
        assert!(!frames.iter().any(|frame| matches!(&frame.record, WalRecord::ToolDispatchIntent { tool_call_id, .. } if tool_call_id.0 == "bad")), "record boundary {keep}");
        if committed_step {
            let message = frames
                .iter()
                .find_map(|frame| match &frame.record {
                    WalRecord::ToolResult { result, .. } if result.tool_call_id.0 == "bad" => {
                        match &result.output {
                            ToolOutput::Error { message } => Some(message),
                            _ => None,
                        }
                    }
                    _ => None,
                })
                .unwrap_or_else(|| panic!("missing error at record boundary {keep}"));
            assert!(message.starts_with("tool_args_trailing_garbage:"));
            if exact_result {
                assert!(message.contains("json_bytes=15, whitespace_bytes=4097"));
            } else {
                assert!(message.contains("json_bytes=unavailable, whitespace_bytes=unavailable"));
            }
        }
        let _ = std::fs::remove_dir_all(dir);
    }
    let _ = std::fs::remove_dir_all(baseline_dir);
}

#[tokio::test]
async fn trailing_garbage_finalize_turn_preserves_call_and_exact_error() {
    let (leases, dir) = tmp_lease_store("trailing-garbage-finalize");
    let provider = StubProvider::new(vec![
        tool_step(&[("before", "lookup", json!({}))]),
        trailing_garbage_step(false),
    ]);
    let plane = StubToolPlane::new().with_canned_text("lookup", "ok");
    let mut request = req(
        "trailing-garbage-finalize",
        vec![tool("lookup", ExecutionMode::Parallel)],
        vec![],
    );
    request.budget = Some(BudgetConfig {
        max_input_tokens: None,
        nudges: vec![],
        finalize_at_tool_calls: 1,
        finalize_text: "Finish without tools".into(),
    });
    let (summary, records) = drive_wal(request, provider, &plane, &leases, &AllowAll).await;
    assert_eq!(summary.unwrap().steps, 2);
    assert_eq!(plane.exec_count(&ToolCallId("bad".into())), 0);
    assert!(records.iter().any(
        |record| matches!(record, WalRecord::ModelStepFinished { assistant_message, .. }
        if assistant_message.tool_calls().any(|call| call.tool_call_id.0 == "bad"))
    ));
    assert!(records.iter().any(|record| matches!(record, WalRecord::ToolResult { result, .. }
        if result.tool_call_id.0 == "bad" && matches!(&result.output, ToolOutput::Error { message }
            if message.starts_with("tool_args_trailing_garbage:") && message.contains("json_bytes=15, whitespace_bytes=4097")))));
    let _ = std::fs::remove_dir_all(dir);
}
