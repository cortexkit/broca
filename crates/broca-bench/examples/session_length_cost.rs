// UNSAFE IS DELIBERATE AND SCOPED TO THIS BENCHMARK. `GlobalAlloc` is an unsafe trait, so a
// counting allocator cannot be written safely; there is no alternative shape. This example is a
// measurement tool and is never shipped in the daemon.
#![allow(unsafe_code)]

//! How a runner's cost scales with session length.
//!
//! Builds one synthetic session of about N messages (mostly short text episodes, one tool turn
//! every tenth episode), applies a compaction view that folds everything before the newest ~200
//! messages, and then measures broca's real code paths against it:
//!
//! - one step of a running session (the interval between two consecutive provider calls of a
//!   multi-step run: stream drain, WAL append, tool batch, store projection, steering, prompt
//!   assembly, view application and the run loop's own request renders for its size
//!   estimates; the probe provider's render of the request it receives is excluded);
//! - the heap the active run holds at its first provider call;
//! - resume of the session (from the call into the run loop to its first provider call), plus
//!   the store-prefix load, WAL snapshot and resume merge on their own;
//! - the step at which a newly published view is fetched and applied;
//! - `session.read` from the newest page, from the cut and from ordinal 0, the model view of
//!   `session.read` from the cut, and the store projection of one new turn;
//! - the step's view functions (`model_options`, `next_notice`, `decide`, ordinal stamping,
//!   request rendering) called once each over the resumed transcript.
//!
//! Every cost is the median of five measured samples after one warmup; fixture build time and
//! process RSS readings are single observations. The model provider
//! and the compaction provider are scripted in-process (no network); requests are rendered
//! with the shared canonical renderer.
//!
//! Run with
//! `cargo run --release -p broca-bench --example session_length_cost -- 1000 10000 50000 150000`.
//! Each size runs in a fresh process when given alone, which keeps peak-RSS figures separate.
//! Set `SESSION_LENGTH_COST_DIR` to choose where fixtures are written (default: the system
//! temporary directory).

use std::{
    alloc::{GlobalAlloc, Layout, System},
    collections::{HashSet, VecDeque},
    hint::black_box,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use broca_core::{
    compaction_provider::{
        lineage::MessageRef,
        notice::ViewReady,
        setup::{SetupAnswer, SetupRequest},
        view::{CompactionView, ViewCut, ViewGetAnswer, ViewGetRequest, ViewRange},
    },
    load_prior_wal_snapshot, read_session_page, replay, resume_state_from_store, AllowAll,
    CompactionPlane, CompactionUnavailable, CompactionViews, FileWalStore, MaterializedStore,
    Persister, Provider, RunControl, RunRequest, SessionStore, StartMode, StubToolPlane, WalHandle,
    WalStore,
};
use broca_protocol::{
    content::{AssistantMessage, ContentBlock, Message, Role, ToolCall},
    provider::{NormalizedEvent, ProviderControl},
    request::{ExecutionMode, ModelHandle, StopCondition, Tool, ToolChoice},
    BindIdentity, CallOptions, FinishReason, GenerationConfig, MessageId, RunId, StepId,
    ToolCallId, Usage,
};
use broca_store::SqliteStore;
use broca_wal::{OpaqueJson, PlanManifest, WalRangeRecords, WalRecord};
use serde_json::{json, Value};
use tokio::sync::mpsc;

/// Messages left outside the view's replace range.
const KEEP_TAIL: usize = 200;
const WARMUPS: usize = 1;
const SAMPLES: usize = 5;
/// Model steps in each measured run: five tool-call steps, then a closing text step.
const RUN_STEPS: usize = 6;
/// One tool episode in every this many episodes; the rest are one user and one assistant text.
const TOOL_EVERY: usize = 10;
const PAGE_LIMIT: u64 = 200;
const DEFAULT_SIZES: [usize; 4] = [1_000, 10_000, 50_000, 150_000];

// ── allocation and I/O accounting ────────────────────────────────────────────────────────────

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static LIVE_BYTES: AtomicI64 = AtomicI64::new(0);
static PEAK_LIVE_BYTES: AtomicI64 = AtomicI64::new(0);

struct CountingAllocator;

/// Allocations at least this large print up to 12 of their stack's `broca` frames, for at most
/// 40 allocations, while [`TRACING`] is set (only between provider calls of a measured run). 0 disables it; set with `SESSION_LENGTH_COST_TRACE_BYTES`.
static TRACE_BYTES: AtomicU64 = AtomicU64::new(0);
static TRACING: AtomicBool = AtomicBool::new(false);
/// Whether the current run is a plain measured run, the only kind whose steps are traced.
static TRACE_RUN: AtomicBool = AtomicBool::new(false);
static TRACES_LEFT: AtomicI64 = AtomicI64::new(40);

thread_local! {
    static IN_TRACE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Print the broca frames of the stack that asked for `bytes`. The backtrace and the print
/// allocate themselves, so a thread-local guard keeps them from tracing again.
fn trace(bytes: usize) {
    let threshold = TRACE_BYTES.load(Ordering::Relaxed);
    if threshold == 0 || (bytes as u64) < threshold || !TRACING.load(Ordering::Relaxed) {
        return;
    }
    if IN_TRACE.with(|flag| flag.replace(true)) {
        return;
    }
    if TRACES_LEFT.fetch_sub(1, Ordering::Relaxed) > 0 {
        let stack = std::backtrace::Backtrace::force_capture().to_string();
        let frames: Vec<&str> = stack
            .lines()
            .map(str::trim)
            .filter(|line| line.contains("broca") && !line.starts_with("at "))
            .filter(|line| !line.contains("session_length_cost::"))
            .take(12)
            .collect();
        eprintln!("ALLOC {bytes} bytes:\n  {}", frames.join("\n  "));
    }
    IN_TRACE.with(|flag| flag.set(false));
}

fn grow(bytes: usize) {
    trace(bytes);
    ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
    ALLOCATED_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
    let live = LIVE_BYTES.fetch_add(bytes as i64, Ordering::Relaxed) + bytes as i64;
    PEAK_LIVE_BYTES.fetch_max(live, Ordering::Relaxed);
}

fn shrink(bytes: usize) {
    LIVE_BYTES.fetch_sub(bytes as i64, Ordering::Relaxed);
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        grow(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        grow(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        shrink(layout.size());
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // Counted as one allocation of the new size and a release of the old one, so the
        // allocated-bytes figure is the bytes the code asked for, as a fresh allocation would be.
        grow(new_size);
        shrink(layout.size());
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// Counters at one instant. `read_bytes` is the process's `rchar` from `/proc/self/io` (bytes
/// returned by read-family syscalls, page cache included), or 0 if the file or counter cannot be read or parsed.
#[derive(Clone, Copy, Debug)]
struct Snap {
    at: Instant,
    allocations: u64,
    allocated_bytes: u64,
    live_bytes: i64,
    read_bytes: u64,
}

fn snap() -> Snap {
    // Read the I/O counter first: reading the file allocates and reads a few hundred bytes,
    // and taking the allocation counters afterwards keeps that out of the measured interval.
    let read_bytes = proc_counter("/proc/self/io", "rchar:");
    Snap {
        at: Instant::now(),
        allocations: ALLOCATIONS.load(Ordering::Relaxed),
        allocated_bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
        live_bytes: LIVE_BYTES.load(Ordering::Relaxed),
        read_bytes,
    }
}

fn proc_counter(path: &str, key: &str) -> u64 {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|text| {
            text.lines().find_map(|line| {
                line.strip_prefix(key)
                    .and_then(|rest| rest.split_whitespace().next())
                    .and_then(|value| value.parse().ok())
            })
        })
        .unwrap_or(0)
}

/// What happened between two snapshots.
#[derive(Clone, Copy, Debug, Default)]
struct Cost {
    time: Duration,
    allocations: u64,
    allocated_bytes: u64,
    read_bytes: u64,
}

impl Cost {
    fn between(from: &Snap, to: &Snap) -> Self {
        Self {
            time: to.at.duration_since(from.at),
            allocations: to.allocations - from.allocations,
            allocated_bytes: to.allocated_bytes - from.allocated_bytes,
            read_bytes: to.read_bytes.saturating_sub(from.read_bytes),
        }
    }

    fn mean(costs: &[Cost]) -> Self {
        let n = costs.len().max(1) as u64;
        Self {
            time: costs.iter().map(|c| c.time).sum::<Duration>() / n as u32,
            allocations: costs.iter().map(|c| c.allocations).sum::<u64>() / n,
            allocated_bytes: costs.iter().map(|c| c.allocated_bytes).sum::<u64>() / n,
            read_bytes: costs.iter().map(|c| c.read_bytes).sum::<u64>() / n,
        }
    }

    /// The median of each field on its own.
    fn median(costs: &[Cost]) -> Self {
        Self {
            time: median(costs.iter().map(|c| c.time).collect()),
            allocations: median(costs.iter().map(|c| c.allocations).collect()),
            allocated_bytes: median(costs.iter().map(|c| c.allocated_bytes).collect()),
            read_bytes: median(costs.iter().map(|c| c.read_bytes).collect()),
        }
    }
}

fn median<T: Ord + Copy>(mut values: Vec<T>) -> T {
    values.sort_unstable();
    values[values.len() / 2]
}

/// Time, allocations and bytes read for `operation`, median of [`SAMPLES`] after [`WARMUPS`].
fn measure<T>(mut operation: impl FnMut() -> T) -> (Cost, T) {
    for _ in 0..WARMUPS {
        black_box(operation());
    }
    let mut costs = Vec::with_capacity(SAMPLES);
    let mut last = None;
    for _ in 0..SAMPLES {
        let before = snap();
        let value = black_box(operation());
        costs.push(Cost::between(&before, &snap()));
        last = Some(value);
    }
    (Cost::median(&costs), last.expect("at least one sample"))
}

// ── the scripted model provider ──────────────────────────────────────────────────────────────

/// One provider call as the probe saw it: counters on entry and after it sized the request and
/// queued its scripted events (so a step interval runs from one call's `after` to the next
/// call's `before` and excludes that work; it still includes the few bytes the probe spends
/// recording a call), the prompt it was sent and that prompt's rendered request size.
#[derive(Clone, Copy, Debug)]
struct Call {
    before: Snap,
    /// Peak live heap at entry, since the run reset it.
    before_peak: i64,
    after: Snap,
    prompt_messages: usize,
    request_bytes: usize,
}

/// A scripted provider that keeps nothing of the requests it is sent except their sizes, so
/// the heap it adds does not grow with the session. Before the call at index `arm_at`, it
/// arms `views` to publish a newer view, which the run fetches at its next step boundary.
struct Probe {
    scripts: Mutex<VecDeque<Vec<NormalizedEvent>>>,
    calls: Mutex<Vec<Call>>,
    arm: Option<(usize, Arc<Views>)>,
}

impl Probe {
    fn new(scripts: Vec<Vec<NormalizedEvent>>) -> Self {
        Self {
            scripts: Mutex::new(scripts.into()),
            calls: Mutex::new(Vec::new()),
            arm: None,
        }
    }

    fn calls(&self) -> Vec<Call> {
        self.calls.lock().unwrap().clone()
    }
}

/// The boxed future `#[async_trait]` methods return. The traits are implemented by hand, in the
/// shape the macro expands to, so this example needs no dependency the crate does not have.
type Boxed<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

impl Provider for Probe {
    fn step<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _step_id: StepId,
        opts: &'life1 CallOptions,
    ) -> Boxed<'async_trait, mpsc::Receiver<NormalizedEvent>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move { self.step_now(opts) })
    }
}

impl Probe {
    fn step_now(&self, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        TRACING.store(false, Ordering::Relaxed);
        let before = snap();
        let before_peak = PEAK_LIVE_BYTES.load(Ordering::Relaxed);
        let request_bytes = broca_core::canonical_request_bytes(opts).len();
        let script = self.scripts.lock().unwrap().pop_front().unwrap_or_default();
        let index = {
            let calls = self.calls.lock().unwrap();
            calls.len()
        };
        if let Some((arm_at, views)) = &self.arm {
            if index + 1 == *arm_at {
                views.publish_next();
            }
        }
        let (tx, rx) = mpsc::channel(64);
        for event in script {
            tx.try_send(event).expect("script fits the channel");
        }
        let after = snap();
        self.calls.lock().unwrap().push(Call {
            before,
            before_peak,
            after,
            prompt_messages: opts.prompt.len(),
            request_bytes,
        });
        TRACING.store(TRACE_RUN.load(Ordering::Relaxed), Ordering::Relaxed);
        rx
    }
}

// ── the scripted compaction provider ─────────────────────────────────────────────────────────

/// A compaction provider holding one view: a single summary message in place of every
/// transcript message before `cut`, and the transcript from `cut` on kept as written.
/// `publish_next` announces a newer version of the same view.
struct Views {
    cut: AtomicU64,
    version: AtomicU64,
    lineage: Mutex<Option<String>>,
    fetches: AtomicU64,
    disabled: AtomicBool,
}

impl Views {
    fn new(cut: u64) -> Self {
        Self {
            cut: AtomicU64::new(cut),
            version: AtomicU64::new(2),
            lineage: Mutex::new(None),
            fetches: AtomicU64::new(0),
            disabled: AtomicBool::new(false),
        }
    }

    fn publish_next(&self) {
        self.version.fetch_add(1, Ordering::SeqCst);
    }

    fn view(&self, session_ref: &str, version: u64) -> CompactionView {
        let cut = self.cut.load(Ordering::SeqCst);
        let lineage = self.lineage.lock().unwrap().clone().expect("lineage known");
        let summary = json!([{"role":"user","content":[{"type":"text",
            "text":format!("summary of messages 0..{cut}")}]}]);
        CompactionView::new(
            session_ref,
            version,
            lineage,
            MessageRef {
                ordinal: cut - 1,
                mid: format!("m{}", cut - 1),
            },
            vec![ViewRange::replace(0, cut, summary.to_string())],
        )
        .with_cut(ViewCut::new("bench-cut", cut - 1))
    }
}

impl CompactionViews for Views {
    fn newest_ready(
        &self,
        session_ref: &str,
        lineage_id: &str,
        after: Option<u64>,
    ) -> Option<ViewReady> {
        if self.disabled.load(Ordering::SeqCst) {
            return None;
        }
        let version = self.version.load(Ordering::SeqCst);
        if after.is_some_and(|applied| applied >= version) {
            return None;
        }
        *self.lineage.lock().unwrap() = Some(lineage_id.into());
        Some(ViewReady::for_view(&self.view(session_ref, version)))
    }
}

impl CompactionPlane for Views {
    fn setup<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        _provider: &'life1 str,
        _bind: &'life2 BindIdentity,
        request: SetupRequest,
    ) -> Boxed<'async_trait, Result<SetupAnswer, CompactionUnavailable>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        // The head Setup inserts before the transcript; the first view replaces it.
        let answer = json!({"answer":"ready","request_id":request.request_id,
            "initial":{"compaction_id":"c","version":1,"range":{"from":0,"to":0},
            "replacement":[{"role":"user","content":[{"type":"text","text":"head"}]}]}});
        Box::pin(async move { serde_json::from_value(answer).map_err(|_| CompactionUnavailable) })
    }

    fn view_get<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        _provider: &'life1 str,
        _bind: &'life2 BindIdentity,
        request: ViewGetRequest,
    ) -> Boxed<'async_trait, Result<ViewGetAnswer, CompactionUnavailable>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        self.fetches.fetch_add(1, Ordering::SeqCst);
        let view = self.view(&request.session_ref, request.view_version);
        Box::pin(async move { Ok(ViewGetAnswer::view(view)) })
    }
}

fn manifest() -> Arc<PlanManifest> {
    Arc::new(PlanManifest {
        plan: OpaqueJson::from_json_text(
            json!({"compaction_item":{"provider":"provider","preset":"head","params":{}},
                "composition":{}})
            .to_string(),
        )
        .unwrap(),
        plan_identity: "id".into(),
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

fn compaction_control(views: &Arc<Views>) -> RunControl {
    RunControl {
        manifest: Some(manifest()),
        compaction_plane: Some(views.clone()),
        compaction_views: Some(views.clone()),
        ..Default::default()
    }
}

// ── requests and scripts ─────────────────────────────────────────────────────────────────────

fn text_message(role: Role, text: String) -> Message {
    Message {
        cache_prefix_blocks: None,
        origin: None,
        ordinal: None,
        tags: Default::default(),
        role,
        content: vec![ContentBlock::Text { text }],
    }
}

fn request(session: &BindIdentity, run: &str) -> RunRequest {
    RunRequest {
        directives: None,
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
        run_id: RunId(format!("run-{run}")),
        session: session.clone(),
        model: ModelHandle::new("stub", "bench"),
        input: vec![text_message(
            Role::User,
            format!("question {run}: please look this up"),
        )],
        tools: vec![Tool {
            name: "lookup".into(),
            description: "deterministic benchmark tool".into(),
            input_schema: json!({"type": "object"}),
            module: None,
            execution_mode: ExecutionMode::Parallel,
            reply: None,
        }],
        policy_allowed: HashSet::from(["lookup".to_owned()]),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        stop_when: vec![StopCondition::StepCount {
            n: RUN_STEPS as u64 + 4,
        }],
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

fn finished(reason: FinishReason) -> NormalizedEvent {
    NormalizedEvent::Control(ProviderControl::Finished {
        usage: Usage {
            input_tokens: Some(11),
            output_tokens: Some(7),
            ..Usage::default()
        },
        finish_reason: reason,
        retries_used: 0,
    })
}

fn text_step(run: &str) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(format!("msg-{run}-t")),
                content: vec![ContentBlock::Text {
                    text: format!("the answer for {run} is short"),
                }],
            },
        }),
        finished(FinishReason::Stop),
    ]
}

fn tool_step(run: &str, step: usize) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(format!("msg-{run}-c{step}")),
                content: vec![ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId(format!("call-{run}-{step}")),
                    tool_name: "lookup".into(),
                    input: json!({"query": "fixed"}),
                })],
            },
        }),
        finished(FinishReason::ToolCalls),
    ]
}

/// `tool_steps` tool-call steps, then a text answer.
fn scripts(run: &str, tool_steps: usize) -> Vec<Vec<NormalizedEvent>> {
    (0..tool_steps)
        .map(|step| tool_step(run, step))
        .chain(std::iter::once(text_step(run)))
        .collect()
}

async fn drive(
    fixture: &Fixture,
    run: &str,
    provider: &Probe,
    control: RunControl,
    store: bool,
) -> broca_core::RunSummary {
    let (tx, mut rx) = mpsc::channel(1024);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let session_store: Option<Arc<dyn SessionStore>> = store.then(|| {
        let store: Arc<dyn SessionStore> = fixture.store.clone();
        store
    });
    let summary = broca_core::run_loop_on_writer(
        request(&fixture.session, run),
        &fixture.writer,
        StartMode::AppendEpisode,
        provider,
        &fixture.plane,
        &AllowAll,
        &fixture.wal_store,
        session_store,
        control,
        &tx,
    )
    .await
    .expect("run completes");
    drop(tx);
    collector.await.expect("collector joins");
    summary
}

// ── the synthetic session ────────────────────────────────────────────────────────────────────

/// The marker every id and text of a template episode carries, replaced per copied episode.
fn marker(episode: usize) -> String {
    format!("EPX{episode:06}")
}

/// The records of one episode the production run loop wrote, as JSON, with the number of
/// transcript messages before it in the template session.
struct TemplateEpisode {
    records: Vec<Value>,
    messages_before: usize,
    messages: usize,
    marker: String,
}

struct Templates {
    first: Vec<WalRecord>,
    text: TemplateEpisode,
    tool: TemplateEpisode,
}

impl Templates {
    /// Runs three real episodes (text, text, tool) through the production run loop into a
    /// scratch WAL of the same session identity and keeps their records.
    async fn capture(root: &std::path::Path, session: &BindIdentity) -> Self {
        let wal_store = FileWalStore::new(root.join("template-wal"));
        let writer = WalHandle::spawn(wal_store.open(session).expect("open template WAL"), 1)
            .expect("spawn template writer");
        let plane = StubToolPlane::new().with_canned_text("lookup", "fixed tool result");
        // The session's compaction plan is frozen by its first episode, so the template
        // runs under it too; its Setup head is recorded in the first episode. No view is
        // published while templates are captured.
        let views = Arc::new(Views::new(1));
        views.disabled.store(true, Ordering::SeqCst);
        let (tx, mut rx) = mpsc::channel(256);
        let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        for episode in 0..3 {
            let run = marker(episode);
            let provider = broca_core::StubProvider::new(scripts(&run, usize::from(episode == 2)));
            let mode = if episode == 0 {
                StartMode::FreshOrResume
            } else {
                StartMode::AppendEpisode
            };
            broca_core::run_loop_on_writer(
                request(session, &run),
                &writer,
                mode,
                &provider,
                &plane,
                &AllowAll,
                &wal_store,
                None,
                compaction_control(&views),
                &tx,
            )
            .await
            .expect("template episode completes");
        }
        drop(tx);
        collector.await.expect("collector joins");
        writer.shutdown().await.expect("template writer shuts down");
        let frames = wal_store.read_records(session).expect("read template WAL");
        let mut episodes: Vec<Vec<WalRecord>> = Vec::new();
        for frame in frames {
            // Advance notices name the template WAL's lineage and count its own sequence, so
            // they are not copied; the fixture's own runs write their own notices, with the fixture's lineage and
            // sequence, from the next step on.
            if matches!(frame.record, WalRecord::CompactionNotice { .. }) {
                continue;
            }
            if matches!(frame.record, WalRecord::RunStarted { .. }) {
                episodes.push(Vec::new());
            }
            episodes
                .last_mut()
                .expect("WAL starts with RunStarted")
                .push(frame.record);
        }
        assert_eq!(episodes.len(), 3, "three template episodes");
        let json = |records: &[WalRecord]| -> Vec<Value> {
            records
                .iter()
                .map(|record| serde_json::to_value(record).expect("record JSON"))
                .collect()
        };
        Self {
            first: episodes[0].clone(),
            text: TemplateEpisode {
                records: json(&episodes[1]),
                messages_before: 2,
                messages: 2,
                marker: marker(1),
            },
            tool: TemplateEpisode {
                records: json(&episodes[2]),
                messages_before: 4,
                messages: 4,
                marker: marker(2),
            },
        }
    }

    /// Episode `episode`'s records, starting at transcript message `messages_before`: the
    /// template's ids renamed and its runner tags (numbered by transcript position) moved.
    fn episode(&self, episode: usize, messages_before: usize) -> (Vec<WalRecord>, usize) {
        let template = if episode % TOOL_EVERY == TOOL_EVERY - 1 {
            &self.tool
        } else {
            &self.text
        };
        let delta = messages_before as i64 - template.messages_before as i64;
        let name = marker(episode);
        let records = template
            .records
            .iter()
            .map(|value| {
                let text = value.to_string().replace(&template.marker, &name);
                let mut value: Value = serde_json::from_str(&text).expect("renamed JSON");
                shift_tags(&mut value, delta, false);
                serde_json::from_value(value).expect("renamed record decodes")
            })
            .collect();
        (records, template.messages)
    }
}

fn shift_tags(value: &mut Value, delta: i64, inside_tags: bool) {
    match value {
        Value::Number(number) if inside_tags => {
            let shifted = number.as_i64().expect("tag number") + delta;
            *value = Value::from(shifted);
        }
        Value::Array(items) => {
            for item in items {
                shift_tags(item, delta, inside_tags);
            }
        }
        Value::Object(map) => {
            for (key, child) in map {
                shift_tags(child, delta, inside_tags || key == "tags");
            }
        }
        _ => {}
    }
}

struct Fixture {
    root: PathBuf,
    session: BindIdentity,
    wal_store: FileWalStore,
    store: Arc<SqliteStore>,
    writer: WalHandle,
    plane: StubToolPlane,
    views: Arc<Views>,
    templates: Templates,
    next_episode: usize,
    messages: usize,
    cut: u64,
    build_time: Duration,
    setup_prompt_messages: usize,
}

impl Fixture {
    async fn build(target: usize) -> Self {
        let started = Instant::now();
        let base = std::env::var_os("SESSION_LENGTH_COST_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let root = base.join(format!(
            "broca-session-length-{}-{target}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("project")).expect("create fixture directory");
        let session = BindIdentity::new(root.join("project"), "bench", "cost");
        let templates = Templates::capture(&root, &session).await;

        let wal_store = FileWalStore::new(root.join("wal"));
        let writer =
            WalHandle::spawn(wal_store.open(&session).expect("open WAL"), 1).expect("spawn writer");
        let store = Arc::new(SqliteStore::open(root.join("store.db")).expect("open SQLite store"));

        for record in &templates.first {
            writer.record(record.clone());
        }
        let mut messages = 2;
        let mut episode = 1;
        let mut episode_starts = vec![0usize];
        // The episode that applies the view adds two more messages.
        while messages + 2 < target {
            let (records, added) = templates.episode(episode, messages);
            for record in records {
                writer.record(record);
            }
            episode_starts.push(messages);
            messages += added;
            episode += 1;
            if episode % 4096 == 0 {
                writer.sync().await.expect("sync fixture");
            }
        }
        writer.sync().await.expect("sync fixture");
        // The cut falls on an episode's first message, so no tool call is split from its result.
        let cut = episode_starts
            .iter()
            .copied()
            .filter(|start| *start + KEEP_TAIL <= messages)
            .max()
            .unwrap_or(0)
            .max(2) as u64;
        let views = Arc::new(Views::new(cut));
        let mut fixture = Self {
            root,
            session,
            wal_store,
            store,
            writer,
            plane: StubToolPlane::new().with_canned_text("lookup", "fixed tool result"),
            views,
            templates,
            next_episode: episode,
            messages,
            cut,
            build_time: Duration::ZERO,
            setup_prompt_messages: 0,
        };

        // One real episode that fetches and applies the view at its first step boundary. No
        // store yet, so this run replays the whole WAL once.
        let provider = Probe::new(vec![text_step("apply-view")]);
        drive(
            &fixture,
            "apply-view",
            &provider,
            compaction_control(&fixture.views),
            false,
        )
        .await;
        fixture.messages += 2;
        fixture.setup_prompt_messages = provider.calls()[0].prompt_messages;
        let head = fixture.writer.sync().await.expect("sync view episode");
        let frames = fixture
            .wal_store
            .read_records(&fixture.session)
            .expect("read fixture");
        if !frames
            .iter()
            .any(|frame| matches!(frame.record, WalRecord::CompactionViewApplied { .. }))
        {
            let start = frames
                .iter()
                .rposition(|frame| matches!(frame.record, WalRecord::RunStarted { .. }))
                .unwrap_or(0);
            for frame in &frames[start..] {
                let text = serde_json::to_string(&frame.record).unwrap_or_default();
                eprintln!("{} {}", frame.seq, &text[..text.len().min(600)]);
            }
            panic!("the fixture did not apply its view");
        }
        drop(frames);

        // Project the whole lineage into the store once, as the projector would have.
        let range = fixture
            .wal_store
            .read_records_range(&fixture.session, 0, Some(head), None)
            .expect("read whole WAL for the initial projection");
        Persister::project_range(fixture.store.as_ref(), &fixture.session, &range)
            .expect("initial projection");
        fixture.build_time = started.elapsed();
        fixture
    }
}

fn dir_bytes(path: &std::path::Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else {
        return std::fs::metadata(path).map_or(0, |m| m.len());
    };
    entries
        .flatten()
        .map(|entry| {
            let path = entry.path();
            if path.is_dir() {
                dir_bytes(&path)
            } else {
                entry.metadata().map_or(0, |m| m.len())
            }
        })
        .sum()
}

// ── measurements ─────────────────────────────────────────────────────────────────────────────

/// One measured multi-step run.
struct RunSample {
    resume: Cost,
    /// Peak live heap above the pre-run baseline, up to the first provider call.
    resume_peak: i64,
    /// Live heap above the pre-run baseline at the first provider call.
    held_first_call: i64,
    /// Live heap above the pre-run baseline at the last provider call.
    held_last_call: i64,
    /// The median step interval of the run, and their mean (the mean also carries the one
    /// step per run that grows the transcript's Vec past its capacity).
    step: Cost,
    step_mean: Cost,
    /// The step whose boundary fetched and applied a newly published view, when armed.
    view_step: Option<Cost>,
    sent_prompt_messages: usize,
    sent_request_bytes: usize,
}

async fn measured_run(fixture: &Fixture, run: &str, arm_view_at: Option<usize>) -> RunSample {
    let mut provider = Probe::new(scripts(run, RUN_STEPS - 1));
    provider.arm = arm_view_at.map(|at| (at, fixture.views.clone()));
    TRACE_RUN.store(arm_view_at.is_none(), Ordering::Relaxed);
    let base = snap();
    PEAK_LIVE_BYTES.store(base.live_bytes, Ordering::Relaxed);
    drive(
        fixture,
        run,
        &provider,
        compaction_control(&fixture.views),
        true,
    )
    .await;
    TRACING.store(false, Ordering::Relaxed);
    TRACE_RUN.store(false, Ordering::Relaxed);
    let calls = provider.calls();
    assert_eq!(calls.len(), RUN_STEPS, "every scripted step ran");
    let steps: Vec<Cost> = calls
        .windows(2)
        .enumerate()
        .filter(|(index, _)| arm_view_at != Some(index + 1))
        .map(|(_, pair)| Cost::between(&pair[0].after, &pair[1].before))
        .collect();
    let view_step = arm_view_at.map(|at| Cost::between(&calls[at - 1].after, &calls[at].before));
    RunSample {
        resume: Cost::between(&base, &calls[0].before),
        resume_peak: calls[0].before_peak - base.live_bytes,
        held_first_call: calls[0].before.live_bytes - base.live_bytes,
        held_last_call: calls[RUN_STEPS - 1].before.live_bytes - base.live_bytes,
        step: Cost::median(&steps),
        step_mean: Cost::mean(&steps),
        view_step,
        sent_prompt_messages: calls[1].prompt_messages,
        sent_request_bytes: calls[1].request_bytes,
    }
}

struct Report {
    n: usize,
    messages: usize,
    cut: u64,
    wal_bytes: u64,
    store_bytes: u64,
    build_time: Duration,
    setup_prompt_messages: usize,
    sent_prompt_messages: usize,
    sent_request_bytes: usize,
    step: Cost,
    step_mean: Cost,
    view_step: Cost,
    resume: Cost,
    resume_peak: i64,
    held_first_call: i64,
    held_last_call: i64,
    rss_kib: u64,
    hwm_kib: u64,
    prefix_load: Cost,
    prefix_load_cold: Cost,
    prefix_rows: usize,
    wal_snapshot: Cost,
    resume_merge: Cost,
    resume_prompt: usize,
    full_replay: Cost,
    read_newest: Cost,
    read_from_cut: Cost,
    read_from_zero: Cost,
    read_cut_messages: usize,
    model_read_from_cut: Cost,
    model_read_complete_wal: bool,
    project_one_turn: Cost,
    view_fetches: u64,
    parts: StepParts,
}

async fn measure_size(target: usize) -> Report {
    eprintln!("building a {target}-message session...");
    let mut fixture = Fixture::build(target).await;
    eprintln!(
        "built {} messages in {:.1}s; cut at ordinal {}",
        fixture.messages,
        fixture.build_time.as_secs_f64(),
        fixture.cut
    );

    // 1-2. Steps of a running session, and what the run holds.
    let mut samples = Vec::new();
    for sample in 0..(WARMUPS + SAMPLES) {
        let run = format!("steps-{sample}");
        let measured = measured_run(&fixture, &run, None).await;
        fixture.messages += 2 * RUN_STEPS;
        if sample >= WARMUPS {
            samples.push(measured);
        }
    }
    // A newly published view, fetched at the boundary before the fourth call.
    let mut view_steps = Vec::new();
    for sample in 0..(WARMUPS + SAMPLES) {
        let run = format!("view-{sample}");
        let measured = measured_run(&fixture, &run, Some(3)).await;
        fixture.messages += 2 * RUN_STEPS;
        if sample >= WARMUPS {
            view_steps.push(measured.view_step.expect("armed run"));
        }
    }
    let view_fetches = fixture.views.fetches.load(Ordering::SeqCst);
    fixture.views.disabled.store(true, Ordering::SeqCst);

    // 3. Resume components on their own.
    let head = fixture
        .writer
        .sync()
        .await
        .expect("sync before component reads");
    let session = fixture.session.clone();
    let (prefix_load, prefix) = measure(|| {
        MaterializedStore::read_prefix_with_watermark(fixture.store.as_ref(), &session)
            .expect("read materialized prefix")
    });
    // The same read on a store connection opened for it, as a reopened actor's first read is:
    // no page of the prefix is in SQLite's connection cache.
    let (prefix_load_cold, _) = measure(|| {
        let store = SqliteStore::open(fixture.root.join("store.db")).expect("reopen store");
        MaterializedStore::read_prefix_with_watermark(&store, &session)
            .expect("read materialized prefix")
            .prefix
            .len()
    });
    let (wal_snapshot, snapshot) = measure(|| {
        load_prior_wal_snapshot(
            &fixture.wal_store,
            &session,
            head,
            Some(fixture.store.as_ref()),
        )
        .expect("checkpoint plus WAL tail")
    });
    let tail: Vec<WalRecord> = snapshot
        .tail
        .iter()
        .filter(|frame| frame.seq > prefix.watermark_seq)
        .map(|frame| frame.record.clone())
        .collect();
    let episode = snapshot
        .checkpoint_at_head
        .last_episode
        .clone()
        .expect("fixture has an episode");
    // The run loop restores compaction state from the admission checkpoint, not the merge.
    let compaction = snapshot
        .checkpoint_at_head
        .compaction
        .as_ref()
        .and_then(|checkpoint| checkpoint.state().cloned())
        .expect("the checkpoint carries compaction state");
    drop(snapshot);
    let (resume_merge, state) = measure(|| {
        resume_state_from_store(
            &prefix,
            &tail,
            Some(episode.config.clone()),
            Some(episode.run_id.clone()),
            None,
        )
        .expect("resume merge")
    });
    let resume_prompt = state.prompt.len();
    let parts = step_parts(&fixture, &state, compaction);
    drop(state);
    let prefix_rows = prefix.prefix.len();
    drop(prefix);
    let (full_replay, _) = measure(|| {
        let records: Vec<WalRecord> = fixture
            .wal_store
            .read_records(&session)
            .expect("read whole WAL")
            .into_iter()
            .map(|frame| frame.record)
            .collect();
        replay(&records).expect("full replay").prompt.len()
    });

    // 4. session.read pages.
    let read = |from: Option<u64>| {
        let page = MaterializedStore::read_page_with_watermark(
            fixture.store.as_ref(),
            &session,
            from,
            PAGE_LIMIT,
            broca_core::context::ReadView::Raw,
        )
        .expect("read store page");
        let tail = fixture
            .wal_store
            .read_records_range(&session, page.watermark_seq, None, page.anchor.as_ref())
            .expect("read WAL tail");
        let records = match tail.records() {
            WalRangeRecords::Complete(records)
            | WalRangeRecords::Partial(records)
            | WalRangeRecords::Unknown(records) => records,
            WalRangeRecords::Absent => &[],
        };
        read_session_page(&page, records, from, Some(PAGE_LIMIT))
            .expect("assemble session page")
            .messages
            .len()
    };
    let (read_newest, _) = measure(|| read(None));
    let (read_from_cut, read_cut_messages) = measure(|| read(Some(fixture.cut)));
    let (read_from_zero, _) = measure(|| read(Some(0)));
    // The model view of session.read, as the module's registry serves it: the store page is
    // always read from ordinal 0 with the largest limit, and core may ask for the complete WAL.
    let complete_reads = AtomicU64::new(0);
    let (model_read_from_cut, _) = measure(|| {
        let page = MaterializedStore::read_page_with_watermark(
            fixture.store.as_ref(),
            &session,
            Some(0),
            broca_core::SESSION_READ_MAX_LIMIT,
            broca_core::context::ReadView::Model,
        )
        .expect("read store page");
        let tail = fixture
            .wal_store
            .read_records_range(&session, page.watermark_seq, None, page.anchor.as_ref())
            .expect("read WAL tail");
        let records = match tail.records() {
            WalRangeRecords::Complete(records)
            | WalRangeRecords::Partial(records)
            | WalRangeRecords::Unknown(records) => records,
            WalRangeRecords::Absent => &[],
        };
        broca_core::read_session_model_page(
            &page,
            records,
            &broca_core::ModelPageRequest {
                from_ordinal: Some(fixture.cut),
                after_mid: None,
                limit: Some(PAGE_LIMIT),
                max_bytes: None,
            },
            || {
                complete_reads.fetch_add(1, Ordering::Relaxed);
                fixture
                    .wal_store
                    .read_records(&session)
                    .map_err(|error| error.to_string())
            },
        )
        .expect("model page")
        .messages
        .len()
    });
    let model_read_complete_wal = complete_reads.load(Ordering::Relaxed) > 0;

    // Store projection of one new turn, as the projector does it after a step.
    let mut projections = Vec::new();
    for sample in 0..(WARMUPS + SAMPLES) {
        let (records, added) = fixture
            .templates
            .episode(fixture.next_episode, fixture.messages);
        // Keep the copied episode a text episode so it is one turn.
        let records = if added == 2 {
            records
        } else {
            fixture.next_episode += 1;
            fixture
                .templates
                .episode(fixture.next_episode, fixture.messages)
                .0
        };
        fixture.next_episode += 1;
        fixture.messages += 2;
        for record in records {
            fixture.writer.record(record);
        }
        let safe_h = fixture.writer.sync().await.expect("sync projection turn");
        let before = snap();
        let seed = Persister::projection_seed(fixture.store.as_ref(), &session)
            .expect("projection seed")
            .expect("complete projection metadata");
        let range = fixture
            .wal_store
            .read_records_range(
                &session,
                seed.watermark_seq,
                Some(safe_h),
                seed.anchor.as_ref(),
            )
            .expect("read one-turn range");
        black_box(
            Persister::project_range(fixture.store.as_ref(), &session, &range)
                .expect("project one turn"),
        );
        let cost = Cost::between(&before, &snap());
        if sample >= WARMUPS {
            projections.push(cost);
        }
    }

    let wal_bytes = dir_bytes(&fixture.root.join("wal"));
    let store_bytes =
        dir_bytes(&fixture.root.join("store.db")) + dir_bytes(&fixture.root.join("store.db-wal"));
    let sent_prompt_messages = samples[0].sent_prompt_messages;
    let sent_request_bytes = samples[0].sent_request_bytes;
    let report = Report {
        n: target,
        messages: fixture.messages,
        cut: fixture.cut,
        wal_bytes,
        store_bytes,
        build_time: fixture.build_time,
        setup_prompt_messages: fixture.setup_prompt_messages,
        sent_prompt_messages,
        sent_request_bytes,
        step: Cost::median(&samples.iter().map(|s| s.step).collect::<Vec<_>>()),
        step_mean: Cost::median(&samples.iter().map(|s| s.step_mean).collect::<Vec<_>>()),
        view_step: Cost::median(&view_steps),
        resume: Cost::median(&samples.iter().map(|s| s.resume).collect::<Vec<_>>()),
        resume_peak: median(samples.iter().map(|s| s.resume_peak).collect()),
        held_first_call: median(samples.iter().map(|s| s.held_first_call).collect()),
        held_last_call: median(samples.iter().map(|s| s.held_last_call).collect()),
        rss_kib: proc_counter("/proc/self/status", "VmRSS:"),
        hwm_kib: proc_counter("/proc/self/status", "VmHWM:"),
        prefix_load,
        prefix_load_cold,
        prefix_rows,
        wal_snapshot,
        resume_merge,
        resume_prompt,
        full_replay,
        read_newest,
        read_from_cut,
        read_from_zero,
        read_cut_messages,
        model_read_from_cut,
        model_read_complete_wal,
        project_one_turn: Cost::median(&projections),
        view_fetches,
        parts,
    };
    fixture.writer.shutdown().await.expect("writer shuts down");
    let _ = std::fs::remove_dir_all(&fixture.root);
    report
}

fn ms(cost: &Cost) -> String {
    format!("{:.3} ms", cost.time.as_secs_f64() * 1000.0)
}

fn mib(bytes: i64) -> String {
    format!("{:.2} MiB", bytes as f64 / (1024.0 * 1024.0))
}

fn cost_cells(cost: &Cost) -> String {
    format!(
        "{} | {} | {} | {}",
        ms(cost),
        cost.allocations,
        mib(cost.allocated_bytes as i64),
        mib(cost.read_bytes as i64)
    )
}

fn print_report(r: &Report) {
    println!("## N = {}", r.n);
    println!(
        "fixture: {} messages, cut at ordinal {}, WAL {}, store {}, built in {:.1}s; \
         first view-applied request sent {} messages",
        r.messages,
        r.cut,
        mib(r.wal_bytes as i64),
        mib(r.store_bytes as i64),
        r.build_time.as_secs_f64(),
        r.setup_prompt_messages
    );
    println!(
        "steady request: {} messages, {} bytes; view fetches {}",
        r.sent_prompt_messages, r.sent_request_bytes, r.view_fetches
    );
    println!("| path | time | allocations | allocated | bytes read |");
    println!("|---|---:|---:|---:|---:|");
    let rows = [
        ("one step (running session), median step", &r.step),
        ("one step, mean over the run's five steps", &r.step_mean),
        ("one step that applies a new view", &r.view_step),
        (
            "resume: run_loop_on_writer -> first provider call",
            &r.resume,
        ),
        (
            "store prefix load (read_prefix_with_watermark)",
            &r.prefix_load,
        ),
        (
            "store prefix load, freshly opened connection",
            &r.prefix_load_cold,
        ),
        ("load_prior_wal_snapshot", &r.wal_snapshot),
        ("resume_state_from_store merge", &r.resume_merge),
        ("full WAL read + replay", &r.full_replay),
        ("session.read newest page", &r.read_newest),
        ("session.read from the cut", &r.read_from_cut),
        ("session.read from ordinal 0", &r.read_from_zero),
        (
            "session.read model view from the cut",
            &r.model_read_from_cut,
        ),
        ("project one turn", &r.project_one_turn),
    ];
    for (name, cost) in rows {
        println!("| {name} | {} |", cost_cells(cost));
    }
    let p = &r.parts;
    println!("| step part (one call, over the resumed {} messages) | time | allocations | allocated | bytes read |", r.resume_prompt);
    println!("|---|---:|---:|---:|---:|");
    let rows = [
        (
            "stamp_ordinals over the whole transcript",
            &p.stamp_ordinals,
        ),
        ("compaction::model_options (view render)", &p.model_options),
        (
            "model_options without per-episode render inputs",
            &p.model_options_no_episodes,
        ),
        ("view::next_notice", &p.next_notice),
        (
            "canonical_request_bytes of the view-rendered request",
            &p.render_request,
        ),
        ("view::decide for a newly fetched view", &p.decide),
    ];
    for (name, cost) in rows {
        println!("| {name} | {} |", cost_cells(cost));
    }
    println!(
        "held parts: transcript Vec<Message> {}, per-episode render inputs {} ({} episodes), \
         message_runs {} entries ({})",
        mib(p.prompt_bytes),
        mib(p.episode_bytes),
        p.episodes,
        p.message_runs,
        mib(p.message_runs_bytes)
    );
    println!(
        "memory: held at first call {}, at last call {}, resume peak {}, VmRSS {} KiB, VmHWM {} KiB",
        mib(r.held_first_call),
        mib(r.held_last_call),
        mib(r.resume_peak),
        r.rss_kib,
        r.hwm_kib
    );
    println!(
        "rows: store prefix {} messages, resumed prompt {} messages, page from cut {} messages; \
         model view read the complete WAL: {}",
        r.prefix_rows, r.resume_prompt, r.read_cut_messages, r.model_read_complete_wal
    );
    println!(
        "CSV,{},{},{},{:.4},{},{},{:.4},{:.4},{},{},{},{},{:.4},{:.4},{:.4},{:.4},{:.4},{:.4},{:.4},{:.4},{},{}",
        r.n,
        r.messages,
        r.sent_prompt_messages,
        r.step.time.as_secs_f64() * 1000.0,
        r.step.allocations,
        r.step.allocated_bytes,
        r.view_step.time.as_secs_f64() * 1000.0,
        r.resume.time.as_secs_f64() * 1000.0,
        r.resume.read_bytes,
        r.held_first_call,
        r.resume_peak,
        r.hwm_kib,
        r.prefix_load.time.as_secs_f64() * 1000.0,
        r.wal_snapshot.time.as_secs_f64() * 1000.0,
        r.resume_merge.time.as_secs_f64() * 1000.0,
        r.full_replay.time.as_secs_f64() * 1000.0,
        r.read_newest.time.as_secs_f64() * 1000.0,
        r.read_from_cut.time.as_secs_f64() * 1000.0,
        r.read_from_zero.time.as_secs_f64() * 1000.0,
        r.project_one_turn.time.as_secs_f64() * 1000.0,
        r.wal_bytes,
        r.store_bytes
    );
}

#[tokio::main]
async fn main() {
    if let Some(bytes) = std::env::var("SESSION_LENGTH_COST_TRACE_BYTES")
        .ok()
        .and_then(|value| value.parse().ok())
    {
        TRACE_BYTES.store(bytes, Ordering::Relaxed);
    }
    let sizes: Vec<usize> = std::env::args()
        .skip(1)
        .map(|arg| arg.parse().expect("each argument is a message count"))
        .collect();
    let sizes = if sizes.is_empty() {
        DEFAULT_SIZES.to_vec()
    } else {
        sizes
    };
    for size in sizes {
        let report = measure_size(size).await;
        print_report(&report);
    }
}

// ── the step's parts on their own ────────────────────────────────────────────────────────────

/// The per-step functions of the run loop, each called once over the resumed transcript, and
/// the heap the transcript and its per-episode render inputs occupy.
struct StepParts {
    stamp_ordinals: Cost,
    model_options: Cost,
    model_options_no_episodes: Cost,
    next_notice: Cost,
    render_request: Cost,
    decide: Cost,
    prompt_bytes: i64,
    episode_bytes: i64,
    episodes: usize,
    message_runs: usize,
    message_runs_bytes: i64,
}

fn retained<T>(make: impl FnOnce() -> T) -> (i64, T) {
    let before = LIVE_BYTES.load(Ordering::Relaxed);
    let value = make();
    (LIVE_BYTES.load(Ordering::Relaxed) - before, value)
}

fn step_parts(
    fixture: &Fixture,
    state: &broca_core::ReplayState,
    compaction: broca_core::CompactionState,
) -> StepParts {
    use broca_core::compaction::view;
    use broca_protocol::request::RenderHistory;

    let (prompt_bytes, prompt) = retained(|| state.prompt.clone());
    let (episode_bytes, episodes) = retained(|| state.render_episodes.clone().unwrap_or_default());
    let episode_count = episodes.len();
    let mut opts = CallOptions::new(prompt);
    opts.render_episodes = Arc::new(RenderHistory {
        episodes,
        ..Default::default()
    });
    let mut without_episodes = CallOptions::new(opts.prompt.clone());
    without_episodes.render_episodes = Arc::new(RenderHistory::default());

    let mut raw = state.prompt.clone();
    let (stamp_ordinals, _) = measure(|| broca_protocol::content::stamp_ordinals(&mut raw));
    let (model_options, rendered) = measure(|| {
        broca_core::compaction::model_options(&opts, &compaction).expect("view renders")
    });
    let (model_options_no_episodes, _) = measure(|| {
        broca_core::compaction::model_options(&without_episodes, &compaction)
            .expect("view renders")
            .prompt
            .len()
    });
    let lineage = fixture
        .views
        .lineage
        .lock()
        .unwrap()
        .clone()
        .expect("lineage known");
    let (next_notice, _) = measure(|| {
        view::next_notice(
            &compaction,
            &raw,
            view::NoticeFacts {
                session_ref: "bench".into(),
                lineage_id: &lineage,
                est_tokens: 1,
                model: "bench",
                window: None,
                usage: None,
                events: Vec::new(),
                seq_floor: 0,
            },
        )
        .is_some()
    });
    let (render_request, _) = measure(|| broca_core::canonical_request_bytes(&rendered).len());
    let session_ref = broca_core::agent_session_ref(&fixture.session);
    let current = compaction
        .view
        .as_ref()
        .map_or(2, |applied| applied.view.view_version);
    let newer = fixture.views.view(&session_ref, current + 1);
    let ready = ViewReady::for_view(&newer);
    let (decide, decision) = measure(|| {
        view::decide(
            &newer,
            &ready,
            &compaction,
            &lineage,
            &opts.prompt,
            &raw,
            false,
        )
    });
    assert!(
        matches!(decision, view::Decision::Apply(_)),
        "the newer view applies: {decision:?}"
    );
    StepParts {
        stamp_ordinals,
        model_options,
        model_options_no_episodes,
        next_notice,
        render_request,
        decide,
        prompt_bytes,
        episode_bytes,
        episodes: episode_count,
        message_runs: state.message_runs.len(),
        message_runs_bytes: retained(|| state.message_runs.clone()).0,
    }
}
