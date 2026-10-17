use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};
use std::sync::Arc;

use broca_core::{
    run_loop_under_lease, AllowAll, BatchCtx, DispatchResult, Provider, RunControl, RunRequest,
    StubToolPlane, ToolManifest, ToolPlane, ToolPlaneError, WalStore,
};
use broca_protocol::{
    BindIdentity, CallOptions, ContentBlock, ExecutionMode, FinishReason, GenerationConfig,
    Message, MessageId, ModelHandle, NormalizedEvent, ProviderControl, Role, RunId, StepId, Tool,
    ToolCall, ToolCallId, ToolChoice, ToolOutput, ToolResult, Usage,
};
use broca_wal::{FramedRecord, Wal, WalError, WalRecord};
use tokio::sync::{mpsc, Mutex, Notify};

static TRACK_ALLOCATIONS: AtomicBool = AtomicBool::new(false);
static TRACKED_NET_BYTES: AtomicIsize = AtomicIsize::new(0);
static TRACKED_NET_ALLOCATIONS: AtomicIsize = AtomicIsize::new(0);
static ALLOCATION_TEST_LOCK: Mutex<()> = Mutex::const_new(());

struct TrackingAllocator;

fn adjust_tracked_allocation(bytes: isize, allocations: isize) {
    if TRACK_ALLOCATIONS.load(Ordering::Relaxed) {
        TRACKED_NET_BYTES.fetch_add(bytes, Ordering::Relaxed);
        TRACKED_NET_ALLOCATIONS.fetch_add(allocations, Ordering::Relaxed);
    }
}

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        adjust_tracked_allocation(layout.size() as isize, 1);
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        adjust_tracked_allocation(layout.size() as isize, 1);
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        adjust_tracked_allocation(-(layout.size() as isize), -1);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        adjust_tracked_allocation(new_size as isize - layout.size() as isize, 0);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator;

fn start_tracking() {
    TRACKED_NET_BYTES.store(0, Ordering::Relaxed);
    TRACKED_NET_ALLOCATIONS.store(0, Ordering::Relaxed);
    assert!(
        TRACK_ALLOCATIONS
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok(),
        "allocation tracking is not nested"
    );
}

fn stop_tracking() {
    TRACK_ALLOCATIONS.store(false, Ordering::SeqCst);
}

fn tracked_net_bytes() -> isize {
    TRACKED_NET_BYTES.load(Ordering::Relaxed)
}

fn tracked_net_allocations() -> isize {
    TRACKED_NET_ALLOCATIONS.load(Ordering::Relaxed)
}

#[derive(Default)]
struct InputAllocationSnapshot {
    retained_bytes: AtomicIsize,
    retained_allocations: AtomicIsize,
    observed: AtomicBool,
}

struct InputSnapshotWal {
    last_seq: u64,
    snapshot: Arc<InputAllocationSnapshot>,
}

impl Wal for InputSnapshotWal {
    fn append(&mut self, _fence: u64, record: &WalRecord, _sync: bool) -> Result<u64, WalError> {
        self.last_seq += 1;
        if matches!(record, WalRecord::RunStarted { .. })
            && !self.snapshot.observed.swap(true, Ordering::SeqCst)
        {
            // FileWal serializes the record synchronously inside append, before the writer can drop it.
            self.snapshot
                .retained_bytes
                .store(tracked_net_bytes(), Ordering::SeqCst);
            self.snapshot
                .retained_allocations
                .store(tracked_net_allocations(), Ordering::SeqCst);
        }
        Ok(self.last_seq)
    }

    fn sync(&self) -> Result<(), WalError> {
        Ok(())
    }

    fn replay(&self) -> Result<Vec<FramedRecord>, WalError> {
        Ok(Vec::new())
    }

    fn tail_after_seq(&self, _after_seq: u64) -> Result<Vec<FramedRecord>, WalError> {
        Ok(Vec::new())
    }

    fn last_seq(&self) -> u64 {
        self.last_seq
    }
}

struct InputSnapshotWalStore {
    snapshot: Arc<InputAllocationSnapshot>,
}

impl WalStore for InputSnapshotWalStore {
    fn open(&self, _session: &BindIdentity) -> Result<Box<dyn Wal + Send>, WalError> {
        Ok(Box::new(InputSnapshotWal {
            last_seq: 0,
            snapshot: Arc::clone(&self.snapshot),
        }))
    }

    fn read_records(&self, _session: &BindIdentity) -> Result<Vec<FramedRecord>, WalError> {
        Ok(Vec::new())
    }
}

struct AllocationProbeProvider;

#[async_trait::async_trait]
impl Provider for AllocationProbeProvider {
    async fn step(&self, step_id: StepId, _opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        let (tx, rx) = mpsc::channel(3);
        tx.try_send(NormalizedEvent::Control(
            ProviderControl::AssistantMessage {
                message: broca_protocol::AssistantMessage {
                    message_id: MessageId("allocation-probe".into()),
                    content: vec![ContentBlock::Text {
                        text: "done".into(),
                    }],
                },
            },
        ))
        .expect("provider channel has room");
        tx.try_send(NormalizedEvent::Control(ProviderControl::StepFinished {
            step_id,
            usage: Usage::default(),
            finish_reason: FinishReason::Stop,
            service_tier_used: None,
            speed_used: None,
        }))
        .expect("provider channel has room");
        tx.try_send(NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: FinishReason::Stop,
            retries_used: 0,
        }))
        .expect("provider channel has room");
        rx
    }
}

const TERMINAL_ASSISTANT_TEXT_BYTES: usize = 512 * 1_024;

struct TerminalAssistantAllocationProbeProvider;

#[async_trait::async_trait]
impl Provider for TerminalAssistantAllocationProbeProvider {
    async fn step(&self, step_id: StepId, _opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        let (tx, rx) = mpsc::channel(3);
        tx.try_send(NormalizedEvent::Control(
            ProviderControl::AssistantMessage {
                message: broca_protocol::AssistantMessage {
                    message_id: MessageId("terminal-assistant-allocation-probe".into()),
                    content: vec![ContentBlock::Text {
                        text: "x".repeat(TERMINAL_ASSISTANT_TEXT_BYTES),
                    }],
                },
            },
        ))
        .expect("provider channel has room");
        tx.try_send(NormalizedEvent::Control(ProviderControl::StepFinished {
            step_id,
            usage: Usage::default(),
            finish_reason: FinishReason::Stop,
            service_tier_used: None,
            speed_used: None,
        }))
        .expect("provider channel has room");
        tx.try_send(NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: FinishReason::Stop,
            retries_used: 0,
        }))
        .expect("provider channel has room");
        rx
    }
}

#[derive(Default)]
struct TerminalAssistantAllocationSnapshot {
    retained_bytes: AtomicIsize,
    retained_allocations: AtomicIsize,
    observed: AtomicBool,
    appended: Notify,
}

struct TerminalAssistantSnapshotWal {
    last_seq: u64,
    snapshot: Arc<TerminalAssistantAllocationSnapshot>,
}

impl Wal for TerminalAssistantSnapshotWal {
    fn append(&mut self, _fence: u64, record: &WalRecord, _sync: bool) -> Result<u64, WalError> {
        self.last_seq += 1;
        if matches!(record, WalRecord::ModelStepFinished { .. })
            && !self.snapshot.observed.swap(true, Ordering::SeqCst)
        {
            self.snapshot
                .retained_bytes
                .store(tracked_net_bytes(), Ordering::SeqCst);
            self.snapshot
                .retained_allocations
                .store(tracked_net_allocations(), Ordering::SeqCst);
            self.snapshot.appended.notify_one();
        }
        Ok(self.last_seq)
    }

    fn sync(&self) -> Result<(), WalError> {
        Ok(())
    }

    fn replay(&self) -> Result<Vec<FramedRecord>, WalError> {
        Ok(Vec::new())
    }

    fn tail_after_seq(&self, _after_seq: u64) -> Result<Vec<FramedRecord>, WalError> {
        Ok(Vec::new())
    }

    fn last_seq(&self) -> u64 {
        self.last_seq
    }
}

struct TerminalAssistantSnapshotWalStore {
    snapshot: Arc<TerminalAssistantAllocationSnapshot>,
}

impl WalStore for TerminalAssistantSnapshotWalStore {
    fn open(&self, _session: &BindIdentity) -> Result<Box<dyn Wal + Send>, WalError> {
        Ok(Box::new(TerminalAssistantSnapshotWal {
            last_seq: 0,
            snapshot: Arc::clone(&self.snapshot),
        }))
    }

    fn read_records(&self, _session: &BindIdentity) -> Result<Vec<FramedRecord>, WalError> {
        Ok(Vec::new())
    }
}

const DISPATCH_ENTRY_COUNT: usize = 64;
const DISPATCH_ENTRY_BYTES: usize = 3 * 1_024;
const DISPATCH_INPUT_BYTES: usize = DISPATCH_ENTRY_COUNT * DISPATCH_ENTRY_BYTES;

struct DispatchAllocationProbeProvider;

#[async_trait::async_trait]
impl Provider for DispatchAllocationProbeProvider {
    async fn step(&self, step_id: StepId, _opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        let (content, finish_reason) = if step_id.0 == 1 {
            let entries: Vec<_> = (0..DISPATCH_ENTRY_COUNT)
                .map(|index| {
                    serde_json::json!({
                        "index": index,
                        "payload": "x".repeat(DISPATCH_ENTRY_BYTES),
                    })
                })
                .collect();
            (
                vec![ContentBlock::ToolCall(ToolCall {
                    tool_call_id: ToolCallId("dispatch-allocation-call".into()),
                    tool_name: "allocation_probe".into(),
                    input: serde_json::json!({ "entries": entries }),
                    thought_signature: None,
                })],
                FinishReason::ToolCalls,
            )
        } else {
            (
                vec![ContentBlock::Text {
                    text: "done".into(),
                }],
                FinishReason::Stop,
            )
        };

        let (tx, rx) = mpsc::channel(3);
        tx.try_send(NormalizedEvent::Control(
            ProviderControl::AssistantMessage {
                message: broca_protocol::AssistantMessage {
                    message_id: MessageId(format!("dispatch-allocation-{}", step_id.0)),
                    content,
                },
            },
        ))
        .expect("provider channel has room");
        tx.try_send(NormalizedEvent::Control(ProviderControl::StepFinished {
            step_id,
            usage: Usage::default(),
            finish_reason,
            service_tier_used: None,
            speed_used: None,
        }))
        .expect("provider channel has room");
        tx.try_send(NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason,
            retries_used: 0,
        }))
        .expect("provider channel has room");
        rx
    }
}

#[derive(Default)]
struct DispatchAllocationProbePlane {
    batch_started_bytes: AtomicIsize,
    batch_started_allocations: AtomicIsize,
    intent_retained_bytes: AtomicIsize,
    intent_retained_allocations: AtomicIsize,
    intent_observed: AtomicBool,
    dispatch_retained_bytes: AtomicIsize,
    dispatch_retained_allocations: AtomicIsize,
    dispatch_calls: AtomicUsize,
}

#[async_trait::async_trait]
impl ToolPlane for DispatchAllocationProbePlane {
    async fn batch_started(
        &self,
        _ctx: &BatchCtx,
        _calls: &[ToolCall],
        _manifest: &ToolManifest,
    ) -> Result<(), ToolPlaneError> {
        self.batch_started_bytes
            .store(tracked_net_bytes(), Ordering::SeqCst);
        self.batch_started_allocations
            .store(tracked_net_allocations(), Ordering::SeqCst);
        Ok(())
    }

    async fn dispatch(&self, _ctx: &BatchCtx, call: &ToolCall) -> DispatchResult {
        self.dispatch_calls.fetch_add(1, Ordering::SeqCst);
        self.dispatch_retained_bytes.store(
            tracked_net_bytes() - self.batch_started_bytes.load(Ordering::SeqCst),
            Ordering::SeqCst,
        );
        self.dispatch_retained_allocations.store(
            tracked_net_allocations() - self.batch_started_allocations.load(Ordering::SeqCst),
            Ordering::SeqCst,
        );
        DispatchResult::Completed(ToolResult {
            tool_call_id: call.tool_call_id.clone(),
            output: ToolOutput::Text { text: "ok".into() },
            is_error: false,
        })
    }

    async fn batch_finished(&self, _ctx: &BatchCtx) -> Result<(), ToolPlaneError> {
        Ok(())
    }
}

struct DispatchIntentSnapshotWal {
    last_seq: u64,
    plane: Arc<DispatchAllocationProbePlane>,
}

impl Wal for DispatchIntentSnapshotWal {
    fn append(&mut self, _fence: u64, record: &WalRecord, _sync: bool) -> Result<u64, WalError> {
        self.last_seq += 1;
        if matches!(record, WalRecord::ToolDispatchIntent { .. })
            && !self.plane.intent_observed.swap(true, Ordering::SeqCst)
        {
            self.plane.intent_retained_bytes.store(
                tracked_net_bytes() - self.plane.batch_started_bytes.load(Ordering::SeqCst),
                Ordering::SeqCst,
            );
            self.plane.intent_retained_allocations.store(
                tracked_net_allocations()
                    - self.plane.batch_started_allocations.load(Ordering::SeqCst),
                Ordering::SeqCst,
            );
        }
        Ok(self.last_seq)
    }

    fn sync(&self) -> Result<(), WalError> {
        Ok(())
    }

    fn replay(&self) -> Result<Vec<FramedRecord>, WalError> {
        Ok(Vec::new())
    }

    fn tail_after_seq(&self, _after_seq: u64) -> Result<Vec<FramedRecord>, WalError> {
        Ok(Vec::new())
    }

    fn last_seq(&self) -> u64 {
        self.last_seq
    }
}

struct DispatchIntentSnapshotWalStore {
    plane: Arc<DispatchAllocationProbePlane>,
}

impl WalStore for DispatchIntentSnapshotWalStore {
    fn open(&self, _session: &BindIdentity) -> Result<Box<dyn Wal + Send>, WalError> {
        Ok(Box::new(DispatchIntentSnapshotWal {
            last_seq: 0,
            plane: Arc::clone(&self.plane),
        }))
    }

    fn read_records(&self, _session: &BindIdentity) -> Result<Vec<FramedRecord>, WalError> {
        Ok(Vec::new())
    }
}

const TOOL_RESULT_TEXT_BYTES: usize = 2 * 1_024 * 1_024;

#[derive(Default)]
struct ToolResultAllocationSnapshot {
    dispatch_baseline_bytes: AtomicIsize,
    retained_bytes_at_wal_append: AtomicIsize,
    observed: AtomicBool,
}

struct ToolResultSnapshotWal {
    last_seq: u64,
    snapshot: Arc<ToolResultAllocationSnapshot>,
}

impl Wal for ToolResultSnapshotWal {
    fn append(&mut self, _fence: u64, record: &WalRecord, _sync: bool) -> Result<u64, WalError> {
        self.last_seq += 1;
        if matches!(record, WalRecord::ToolResult { .. })
            && !self.snapshot.observed.swap(true, Ordering::SeqCst)
        {
            let retained =
                tracked_net_bytes() - self.snapshot.dispatch_baseline_bytes.load(Ordering::SeqCst);
            self.snapshot
                .retained_bytes_at_wal_append
                .store(retained, Ordering::SeqCst);
        }
        Ok(self.last_seq)
    }

    fn sync(&self) -> Result<(), WalError> {
        Ok(())
    }

    fn replay(&self) -> Result<Vec<FramedRecord>, WalError> {
        Ok(Vec::new())
    }

    fn tail_after_seq(&self, _after_seq: u64) -> Result<Vec<FramedRecord>, WalError> {
        Ok(Vec::new())
    }

    fn last_seq(&self) -> u64 {
        self.last_seq
    }
}

struct ToolResultSnapshotWalStore {
    snapshot: Arc<ToolResultAllocationSnapshot>,
}

impl WalStore for ToolResultSnapshotWalStore {
    fn open(&self, _session: &BindIdentity) -> Result<Box<dyn Wal + Send>, WalError> {
        Ok(Box::new(ToolResultSnapshotWal {
            last_seq: 0,
            snapshot: Arc::clone(&self.snapshot),
        }))
    }

    fn read_records(&self, _session: &BindIdentity) -> Result<Vec<FramedRecord>, WalError> {
        Ok(Vec::new())
    }
}

struct ToolResultAllocationProbePlane {
    snapshot: Arc<ToolResultAllocationSnapshot>,
}

#[async_trait::async_trait]
impl ToolPlane for ToolResultAllocationProbePlane {
    async fn batch_started(
        &self,
        _ctx: &BatchCtx,
        _calls: &[ToolCall],
        _manifest: &ToolManifest,
    ) -> Result<(), ToolPlaneError> {
        Ok(())
    }

    async fn dispatch(&self, _ctx: &BatchCtx, call: &ToolCall) -> DispatchResult {
        self.snapshot
            .dispatch_baseline_bytes
            .store(tracked_net_bytes(), Ordering::SeqCst);
        DispatchResult::Completed(ToolResult {
            tool_call_id: call.tool_call_id.clone(),
            output: ToolOutput::Text {
                text: "r".repeat(TOOL_RESULT_TEXT_BYTES),
            },
            is_error: false,
        })
    }

    async fn batch_finished(&self, _ctx: &BatchCtx) -> Result<(), ToolPlaneError> {
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn terminal_no_tool_step_does_not_retain_a_second_assistant_message_copy() {
    let _allocation_test_lock = ALLOCATION_TEST_LOCK.lock().await;
    let request = RunRequest {
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
        run_id: RunId("terminal-assistant-allocation-probe".into()),
        session: BindIdentity {
            project_root: "/tmp/terminal-assistant-allocation-probe".into(),
            harness: "test".into(),
            session: "terminal-assistant-allocation-probe".into(),
        },
        model: ModelHandle::new("stub", "stub-model"),
        input: vec![Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "run the terminal allocation probe".into(),
            }],
        )],
        tools: Vec::new(),
        policy_allowed: HashSet::new(),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        budget: None,
        stop_when: Vec::new(),
        submission_id: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
    };
    let snapshot = Arc::new(TerminalAssistantAllocationSnapshot::default());
    let run_snapshot = Arc::clone(&snapshot);
    // RunStarted and StepStarted fill this channel. The runner then waits on StepFinished while the
    // WAL thread measures ModelStepFinished, keeping any second live message graph observable.
    let (out, events) = mpsc::channel(2);

    start_tracking();
    let run = tokio::spawn(async move {
        let provider = TerminalAssistantAllocationProbeProvider;
        let tool_plane = StubToolPlane::default();
        let wal_store = TerminalAssistantSnapshotWalStore {
            snapshot: run_snapshot,
        };
        run_loop_under_lease(
            request,
            1,
            &provider,
            &tool_plane,
            &AllowAll,
            &wal_store,
            None,
            RunControl {
                emit_control_payloads: false,
                ..RunControl::default()
            },
            &out,
        )
        .await
    });
    let observed = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        snapshot.appended.notified(),
    )
    .await;
    stop_tracking();
    drop(events);

    run.await
        .expect("allocation probe task does not panic")
        .expect("allocation probe run completes");
    observed.expect("terminal ModelStepFinished reaches the WAL");
    assert!(
        snapshot.observed.load(Ordering::SeqCst),
        "the allocation snapshot must observe a no-tool ModelStepFinished"
    );
    let retained_bytes = snapshot.retained_bytes.load(Ordering::SeqCst);
    let retained_allocations = snapshot.retained_allocations.load(Ordering::SeqCst);
    eprintln!(
        "retained at terminal ModelStepFinished: {retained_bytes} bytes in \
         {retained_allocations} allocations; assistant text: {TERMINAL_ASSISTANT_TEXT_BYTES} bytes"
    );
    // ONE-SIDED ON PURPOSE. Only retained growth can mean that ModelStepFinished kept a second
    // AssistantMessage graph; frees elsewhere in the run cannot violate that ownership property.
    assert!(
        retained_bytes
            < (TERMINAL_ASSISTANT_TEXT_BYTES + TERMINAL_ASSISTANT_TEXT_BYTES / 2) as isize,
        "terminal ModelStepFinished retained a deep AssistantMessage copy: {retained_bytes} \
         tracked bytes for {TERMINAL_ASSISTANT_TEXT_BYTES} assistant text bytes"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn tool_step_does_not_retain_a_second_assistant_message_copy() {
    let _allocation_test_lock = ALLOCATION_TEST_LOCK.lock().await;
    let request = RunRequest {
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
        run_id: RunId("tool-assistant-allocation-probe".into()),
        session: BindIdentity {
            project_root: "/tmp/tool-assistant-allocation-probe".into(),
            harness: "test".into(),
            session: "tool-assistant-allocation-probe".into(),
        },
        model: ModelHandle::new("stub", "stub-model"),
        input: vec![Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "run the tool-assistant allocation probe".into(),
            }],
        )],
        tools: vec![Tool {
            name: "allocation_probe".into(),
            description: "measure assistant-message ownership".into(),
            input_schema: serde_json::json!({ "type": "object" }),
            module: None,
            execution_mode: ExecutionMode::Sequential,
        }],
        policy_allowed: HashSet::from(["allocation_probe".into()]),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        budget: None,
        stop_when: Vec::new(),
        submission_id: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
    };
    let snapshot = Arc::new(TerminalAssistantAllocationSnapshot::default());
    let run_snapshot = Arc::clone(&snapshot);
    // RunStarted and StepStarted fill this channel, so call extraction cannot begin before the WAL
    // thread measures ModelStepFinished and observes every graph retained at the record seam.
    let (out, events) = mpsc::channel(2);

    start_tracking();
    let run = tokio::spawn(async move {
        let provider = DispatchAllocationProbeProvider;
        let tool_plane = DispatchAllocationProbePlane::default();
        let wal_store = TerminalAssistantSnapshotWalStore {
            snapshot: run_snapshot,
        };
        run_loop_under_lease(
            request,
            1,
            &provider,
            &tool_plane,
            &AllowAll,
            &wal_store,
            None,
            RunControl {
                emit_control_payloads: false,
                ..RunControl::default()
            },
            &out,
        )
        .await
    });
    let observed = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        snapshot.appended.notified(),
    )
    .await;
    stop_tracking();
    drop(events);

    run.await
        .expect("allocation probe task does not panic")
        .expect("allocation probe run completes");
    observed.expect("tool-bearing ModelStepFinished reaches the WAL");
    assert!(
        snapshot.observed.load(Ordering::SeqCst),
        "the allocation snapshot must observe a tool-bearing ModelStepFinished"
    );
    let retained_bytes = snapshot.retained_bytes.load(Ordering::SeqCst);
    let retained_allocations = snapshot.retained_allocations.load(Ordering::SeqCst);
    eprintln!(
        "retained at tool-bearing ModelStepFinished: {retained_bytes} bytes in \
         {retained_allocations} allocations; tool JSON strings: {DISPATCH_INPUT_BYTES} bytes"
    );
    // ONE-SIDED ON PURPOSE. Only retained growth can mean that the WAL record kept a second
    // AssistantMessage graph; frees elsewhere cannot violate that ownership property.
    assert!(
        retained_bytes < (DISPATCH_INPUT_BYTES + DISPATCH_INPUT_BYTES / 2) as isize,
        "tool-bearing ModelStepFinished retained a deep AssistantMessage copy: {retained_bytes} \
         tracked bytes for {DISPATCH_INPUT_BYTES} tool JSON string bytes"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn admitted_turn_does_not_retain_a_second_input_copy() {
    let _allocation_test_lock = ALLOCATION_TEST_LOCK.lock().await;
    const MESSAGE_COUNT: usize = 128;
    const TEXT_BYTES: usize = 4_096;
    const INPUT_TEXT_BYTES: usize = MESSAGE_COUNT * TEXT_BYTES;

    let source = (0..MESSAGE_COUNT)
        .map(|_| {
            Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "x".repeat(TEXT_BYTES),
                }],
            )
        })
        .collect::<Vec<_>>();
    let serialized = serde_json::to_vec(&source).expect("serialize production-shaped input");
    let input: Vec<Message> =
        serde_json::from_slice(&serialized).expect("deserialize production-shaped input");

    let request = RunRequest {
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
        run_id: RunId("allocation-probe".into()),
        session: BindIdentity {
            project_root: "/tmp/allocation-probe".into(),
            harness: "test".into(),
            session: "allocation-probe".into(),
        },
        model: ModelHandle::new("stub", "stub-model"),
        input,
        tools: Vec::new(),
        policy_allowed: HashSet::new(),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        budget: None,
        stop_when: Vec::new(),
        submission_id: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
    };
    let provider = AllocationProbeProvider;
    let tool_plane = StubToolPlane::default();
    let snapshot = Arc::new(InputAllocationSnapshot::default());
    let wal_store = InputSnapshotWalStore {
        snapshot: Arc::clone(&snapshot),
    };
    let (out, _events) = mpsc::channel(32);

    start_tracking();
    let result = run_loop_under_lease(
        request,
        1,
        &provider,
        &tool_plane,
        &AllowAll,
        &wal_store,
        None,
        RunControl::default(),
        &out,
    )
    .await;
    stop_tracking();

    result.expect("allocation probe run completes");
    assert!(
        snapshot.observed.load(Ordering::SeqCst),
        "the allocation snapshot must observe RunStarted at the WAL append seam"
    );
    let retained = snapshot.retained_bytes.load(Ordering::SeqCst);
    eprintln!(
        "retained tracked bytes while appending RunStarted: {retained}; input text bytes: {INPUT_TEXT_BYTES}"
    );
    // This check intentionally has only an upper bound. Enough positive retained growth to match
    // the input reveals a duplicate; a negative net value only means unrelated frees outweighed
    // allocations and cannot by itself reveal one.
    assert!(
        retained < (INPUT_TEXT_BYTES / 4) as isize,
        "RunStarted retained a deep input copy: {retained} tracked bytes for {INPUT_TEXT_BYTES} input text bytes"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn admitted_turn_does_not_retain_a_second_tool_copy() {
    let _allocation_test_lock = ALLOCATION_TEST_LOCK.lock().await;
    const TOOL_COUNT: usize = 32;
    const DESCRIPTION_BYTES: usize = 2_048;
    const SCHEMA_DESCRIPTION_BYTES: usize = 4_096;

    let tools = (0..TOOL_COUNT)
        .map(|index| Tool {
            name: format!("allocation_probe_{index}"),
            description: "d".repeat(DESCRIPTION_BYTES),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "payload": {
                        "type": "string",
                        "description": "s".repeat(SCHEMA_DESCRIPTION_BYTES),
                    },
                },
                "required": ["payload"],
                "additionalProperties": false,
            }),
            module: Some(format!("module-{index}")),
            execution_mode: if index % 2 == 0 {
                ExecutionMode::Parallel
            } else {
                ExecutionMode::Sequential
            },
        })
        .collect::<Vec<_>>();
    let serialized_tool_bytes = serde_json::to_vec(&tools)
        .expect("serialize production-shaped tools")
        .len();
    let policy_allowed = tools.iter().map(|tool| tool.name.clone()).collect();
    let request = RunRequest {
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
        run_id: RunId("tool-allocation-probe".into()),
        session: BindIdentity {
            project_root: "/tmp/tool-allocation-probe".into(),
            harness: "test".into(),
            session: "tool-allocation-probe".into(),
        },
        model: ModelHandle::new("stub", "stub-model"),
        input: vec![Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "run the probe".into(),
            }],
        )],
        tools,
        policy_allowed,
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        budget: None,
        stop_when: Vec::new(),
        submission_id: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
    };
    let provider = AllocationProbeProvider;
    let tool_plane = StubToolPlane::default();
    let snapshot = Arc::new(InputAllocationSnapshot::default());
    let wal_store = InputSnapshotWalStore {
        snapshot: Arc::clone(&snapshot),
    };
    let (out, _events) = mpsc::channel(32);

    start_tracking();
    let result = run_loop_under_lease(
        request,
        1,
        &provider,
        &tool_plane,
        &AllowAll,
        &wal_store,
        None,
        RunControl::default(),
        &out,
    )
    .await;
    stop_tracking();

    result.expect("allocation probe run completes");
    assert!(
        snapshot.observed.load(Ordering::SeqCst),
        "the allocation snapshot must observe RunStarted at the WAL append seam"
    );
    let retained_bytes = snapshot.retained_bytes.load(Ordering::SeqCst);
    let retained_allocations = snapshot.retained_allocations.load(Ordering::SeqCst);
    eprintln!(
        "retained while appending tool-bearing RunStarted: {retained_bytes} bytes in \
         {retained_allocations} allocations; serialized tools: {serialized_tool_bytes} bytes"
    );
    // ONE-SIDED ON PURPOSE. Only retained growth can reveal a second manifest graph; a net
    // decrease means unrelated frees outweighed allocations and cannot violate this ownership rule.
    assert!(
        retained_bytes < (serialized_tool_bytes / 4) as isize,
        "RunStarted retained a deep tool copy: {retained_bytes} tracked bytes for \
         {serialized_tool_bytes} serialized tool bytes"
    );
    assert!(
        retained_allocations < 200,
        "RunStarted retained a second tool allocation graph: {retained_allocations} live allocations"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn dispatch_plan_does_not_retain_a_second_tool_call_copy() {
    let _allocation_test_lock = ALLOCATION_TEST_LOCK.lock().await;
    let tool = Tool {
        name: "allocation_probe".into(),
        description: "measure dispatch ownership".into(),
        input_schema: serde_json::json!({ "type": "object" }),
        module: None,
        execution_mode: ExecutionMode::Sequential,
    };
    let request = RunRequest {
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
        run_id: RunId("dispatch-allocation-probe".into()),
        session: BindIdentity {
            project_root: "/tmp/dispatch-allocation-probe".into(),
            harness: "test".into(),
            session: "dispatch-allocation-probe".into(),
        },
        model: ModelHandle::new("stub", "stub-model"),
        input: vec![Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "run the dispatch probe".into(),
            }],
        )],
        tools: vec![tool],
        policy_allowed: HashSet::from(["allocation_probe".into()]),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        budget: None,
        stop_when: Vec::new(),
        submission_id: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
    };
    let provider = DispatchAllocationProbeProvider;
    let tool_plane = Arc::new(DispatchAllocationProbePlane::default());
    let wal_store = DispatchIntentSnapshotWalStore {
        plane: Arc::clone(&tool_plane),
    };
    let (out, _events) = mpsc::channel(32);
    let control = RunControl {
        emit_control_payloads: false,
        ..RunControl::default()
    };

    start_tracking();
    let result = run_loop_under_lease(
        request,
        1,
        &provider,
        tool_plane.as_ref(),
        &AllowAll,
        &wal_store,
        None,
        control,
        &out,
    )
    .await;
    stop_tracking();

    result.expect("dispatch allocation probe run completes");
    assert!(
        tool_plane.intent_observed.load(Ordering::SeqCst),
        "the allocation snapshot must observe ToolDispatchIntent at the WAL append seam"
    );
    let intent_retained_bytes = tool_plane.intent_retained_bytes.load(Ordering::SeqCst);
    let intent_retained_allocations = tool_plane
        .intent_retained_allocations
        .load(Ordering::SeqCst);
    eprintln!(
        "extra retained at ToolDispatchIntent append: {intent_retained_bytes} bytes in \
         {intent_retained_allocations} allocations; synthetic input strings: \
         {DISPATCH_INPUT_BYTES} bytes"
    );
    // ONE-SIDED ON PURPOSE. Only retained growth can reveal a second argument graph; a net
    // decrease means unrelated frees outweighed allocations and cannot violate this ownership rule.
    assert!(
        intent_retained_bytes < (DISPATCH_INPUT_BYTES / 4) as isize,
        "ToolDispatchIntent retained a deep argument copy: {intent_retained_bytes} tracked bytes \
         for {DISPATCH_INPUT_BYTES} input string bytes"
    );
    assert!(
        intent_retained_allocations < (DISPATCH_ENTRY_COUNT / 2) as isize,
        "ToolDispatchIntent retained a second argument allocation graph: \
         {intent_retained_allocations} live allocations for {DISPATCH_ENTRY_COUNT} entries"
    );
    assert_eq!(
        tool_plane.dispatch_calls.load(Ordering::SeqCst),
        1,
        "the allocation snapshot must be taken at a real dispatch"
    );
    let retained_bytes = tool_plane.dispatch_retained_bytes.load(Ordering::SeqCst);
    let retained_allocations = tool_plane
        .dispatch_retained_allocations
        .load(Ordering::SeqCst);
    eprintln!(
        "extra retained at first dispatch: {retained_bytes} bytes in {retained_allocations} \
         allocations; synthetic input strings: {DISPATCH_INPUT_BYTES} bytes"
    );
    // ONE-SIDED ON PURPOSE. The property is "planning does not retain a SECOND ToolCall graph",
    // which only a GROWTH in retained bytes can violate. An earlier version asserted on
    // `unsigned_abs()` and failed on Ubuntu at -141,417 bytes -- a large net FREE during the
    // window, which is the allocator behaving differently, not a duplicate appearing. macOS and
    // Windows passed the same code, so the two-sided bound was measuring platform allocator
    // policy under the name of an ownership invariant.
    //
    // A decrease cannot indicate a duplicate, so it is not a failure here. If this ever needs to
    // catch "we stopped retaining something we should", that is a different property and wants
    // its own assertion with its own name.
    assert!(
        retained_bytes < (DISPATCH_INPUT_BYTES / 4) as isize,
        "dispatch planning retained a deep ToolCall copy: {retained_bytes} tracked bytes for \
         {DISPATCH_INPUT_BYTES} input string bytes"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn dispatched_tool_result_does_not_retain_a_second_payload_copy() {
    let _allocation_test_lock = ALLOCATION_TEST_LOCK.lock().await;
    let request = RunRequest {
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
        run_id: RunId("tool-result-allocation-probe".into()),
        session: BindIdentity {
            project_root: "/tmp/tool-result-allocation-probe".into(),
            harness: "test".into(),
            session: "tool-result-allocation-probe".into(),
        },
        model: ModelHandle::new("stub", "stub-model"),
        input: vec![Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "run the tool-result allocation probe".into(),
            }],
        )],
        tools: vec![Tool {
            name: "allocation_probe".into(),
            description: "return a large synthetic result".into(),
            input_schema: serde_json::json!({ "type": "object" }),
            module: None,
            execution_mode: ExecutionMode::Parallel,
        }],
        policy_allowed: HashSet::from(["allocation_probe".into()]),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        budget: None,
        stop_when: Vec::new(),
        submission_id: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
    };
    let provider = DispatchAllocationProbeProvider;
    let snapshot = Arc::new(ToolResultAllocationSnapshot::default());
    let tool_plane = ToolResultAllocationProbePlane {
        snapshot: Arc::clone(&snapshot),
    };
    let wal_store = ToolResultSnapshotWalStore {
        snapshot: Arc::clone(&snapshot),
    };
    let (out, _events) = mpsc::channel(32);

    start_tracking();
    let result = run_loop_under_lease(
        request,
        1,
        &provider,
        &tool_plane,
        &AllowAll,
        &wal_store,
        None,
        RunControl {
            emit_control_payloads: false,
            ..RunControl::default()
        },
        &out,
    )
    .await;
    stop_tracking();

    result.expect("tool-result allocation probe run completes");
    assert!(
        snapshot.observed.load(Ordering::SeqCst),
        "the allocation snapshot must observe a dispatched ToolResult at the WAL append seam"
    );
    let retained_bytes = snapshot.retained_bytes_at_wal_append.load(Ordering::SeqCst);
    eprintln!(
        "retained from dispatch through ToolResult WAL append: {retained_bytes} bytes; \
         tool-result text: {TOOL_RESULT_TEXT_BYTES} bytes"
    );
    // ONE-SIDED ON PURPOSE. Only retained growth can reveal a second ToolResult payload; a net
    // decrease means unrelated frees outweighed allocations and cannot violate this ownership rule.
    assert!(
        retained_bytes < (TOOL_RESULT_TEXT_BYTES + TOOL_RESULT_TEXT_BYTES / 2) as isize,
        "ToolResult WAL append retained a second payload copy: {retained_bytes} tracked bytes for \
         {TOOL_RESULT_TEXT_BYTES} result text bytes"
    );
}
