// UNSAFE IS DELIBERATE AND SCOPED TO THIS BENCHMARK. `GlobalAlloc` is an unsafe trait, so a
// counting allocator cannot be written safely; there is no alternative shape. This example is a
// measurement tool and is never shipped in the daemon.
#![allow(unsafe_code)]

//! How a runner's cost scales with session length, and whether it stays flat.
//!
//! Builds one synthetic session of about N messages and applies a compaction view to it. The
//! view keeps ordinal 0, replaces ordinals 1 through `cut - 1` with one summary, and keeps the
//! newest 200 messages (the suffix, from ordinal `cut` on) as written. Only the discarded
//! history depends on N, and only through short episodes that are not guidance episodes (so it
//! has no keep range or held message). Everything a run holds is the same at every N: the
//! replacement size, the keep ranges, the number of applied views, the 200-message suffix, the
//! WAL tail and every guidance episode's length. The guidance episodes are the one reached
//! through the keep range, the 50-message episode the cut falls inside (whose repeated reads
//! carry repetition guidance on both sides of the cut), and the suffix's episodes.
//!
//! It then measures broca's real code paths against that session:
//!
//! - one step of a running session (the interval between two consecutive provider calls of a
//!   multi-step run: stream drain, WAL append, tool batch, store projection, steering, prompt
//!   assembly, view application and the run loop's own request renders for its size
//!   estimates; the probe provider's render of the request it receives is excluded);
//! - the heap the active run holds at its first and last provider calls;
//! - resume of the session (from the call into the run loop to its first provider call), plus
//!   the store-prefix load, WAL snapshot and resume merge on their own;
//! - the step at which a newly published view is fetched and applied;
//! - resume after a run is killed mid-episode, after a view applied in that episode;
//! - resume from a store written before the episode projection existed, once projection has
//!   filled it in;
//! - `session.read` from the newest page, from the cut and from ordinal 0, the model view of
//!   `session.read` from the cut, and the store projection of one new turn;
//! - the step's view functions (`model_options`, `next_notice`, `decide`, ordinal stamping,
//!   request rendering) called once each over the resumed transcript.
//!
//! Every cost is the median of ten measured samples after one warmup; fixture build time and
//! process RSS readings are single observations. The model provider
//! and the compaction provider are scripted in-process (no network); requests are rendered
//! with the shared canonical renderer. Each size runs twice: once as an Anthropic-style session
//! (a stateless render config) and once as a ChatGPT effort-update session (a frozen render
//! config asking for effort updates, with the effort changing between episodes).
//!
//! The flatness thresholds ([`THRESHOLDS`]) are fixed in this file, before anything is
//! measured. Every size is compared with the smallest size measured in the same process, so
//! run `1000 150000` (or all four defaults) in one process to get a verdict. Each resume must
//! also go through the store's held read with no whole-WAL read and no complete-prefix read.
//! The process exits 1 when any check fails. The `session.read` outputs on the store before
//! and after the episode projection is cleared must be byte-identical, or the run panics.
//!
//! Run with
//! `cargo run --release -p broca-bench --example session_length_cost -- 1000 10000 50000 150000`.
//! Set `SESSION_LENGTH_COST_DIR` to choose where fixtures are written (default: the system
//! temporary directory), and `SESSION_LENGTH_COST_FLAVORS` to `anthropic` or `chatgpt` to run
//! only one session flavor.

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
        view::{CompactionView, ViewGetAnswer, ViewGetRequest, ViewRange},
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
use tokio::sync::{mpsc, Notify};

/// Messages from the cut on when the view is first applied.
const SUFFIX: usize = 200;
const WARMUPS: usize = 1;
const SAMPLES: usize = 10;
/// Model steps in each measured run: five tool-call steps, then a closing text step.
const RUN_STEPS: usize = 6;
/// One tool episode in every this many discarded episodes; the rest are one user and one
/// assistant text.
const TOOL_EVERY: usize = 10;
const PAGE_LIMIT: u64 = 200;
const DEFAULT_SIZES: [usize; 4] = [1_000, 10_000, 50_000, 150_000];
/// Repeated reads in the episode the cut falls inside: one user, this many read call/result
/// pairs and a closing text make it 50 messages.
const LONG_READS: usize = 24;
const LONG_MESSAGES: usize = 2 + 2 * LONG_READS;
/// Messages of that episode before the cut: its user and twelve read pairs. The cut falls on a
/// read call, so no call is split from its result.
const LONG_BEFORE_CUT: usize = 1 + 2 * 12;
/// The provider call (by index) during which a crash-case run is killed. The view applies at
/// the boundary before call 1, so the run dies mid-episode after it, with its user message and
/// three tool call/result pairs durable and the fourth model step started but unfinished.
const CRASH_AT_CALL: usize = 3;
/// Messages a crash-case episode holds when it is killed.
const CRASH_MESSAGES_AT_KILL: usize = 1 + 2 * CRASH_AT_CALL;

// ── flatness thresholds, fixed before measurement ───────────────────────────────────────────

/// One flatness bound: the value at the largest size may be at most `ratio` times the value at
/// the smallest size, plus `slack`.
#[derive(Clone, Copy, Debug)]
struct Threshold {
    ratio: f64,
    slack: f64,
}

impl Threshold {
    /// The largest value allowed when the smallest size measured `base`. A negative base (a
    /// live-heap delta can be slightly negative) is treated as 0.
    fn limit(self, base: f64) -> f64 {
        self.ratio * base.max(0.0) + self.slack
    }

    fn holds(self, base: f64, value: f64) -> bool {
        value <= self.limit(base)
    }
}

const MIB: f64 = 1024.0 * 1024.0;

/// The bounds every measured case must meet at its largest size against its smallest.
struct Thresholds {
    /// Live heap above the pre-run snapshot at the first and at the last provider call, bytes.
    resident: Threshold,
    /// Bytes allocated from the call into the run loop to the first provider call.
    resume_allocated: Threshold,
    /// Wall time of the same interval, milliseconds.
    resume_ms: Threshold,
}

const THRESHOLDS: Thresholds = Thresholds {
    resident: Threshold {
        ratio: 1.10,
        slack: MIB,
    },
    resume_allocated: Threshold {
        ratio: 1.10,
        slack: MIB,
    },
    resume_ms: Threshold {
        ratio: 1.25,
        slack: 5.0,
    },
};

// ── allocation and I/O accounting ────────────────────────────────────────────────────────────

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static LIVE_BYTES: AtomicI64 = AtomicI64::new(0);
static PEAK_LIVE_BYTES: AtomicI64 = AtomicI64::new(0);

struct CountingAllocator;

/// Allocations at least this large print up to 12 of their stack's `broca` frames, for at most
/// 40 allocations, while [`TRACING`] is set (only during a plain measured run's resume and between its provider calls). 0 disables it; set with `SESSION_LENGTH_COST_TRACE_BYTES`.
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

// ── store and WAL read accounting ────────────────────────────────────────────────────────────

/// WAL reads that decode the log from its first byte: `read_records`, and range reads given no
/// byte anchor to start from. A resume that falls back to a full WAL replay makes one.
static WHOLE_WAL_READS: AtomicU64 = AtomicU64::new(0);
/// Reads of the store's complete materialized prefix (`read_prefix_with_watermark`).
static PREFIX_READS: AtomicU64 = AtomicU64::new(0);
/// Store reads that answered with only the held set of an applied view (`read_held_prefix`).
static HELD_READS: AtomicU64 = AtomicU64::new(0);

/// The read counters at one instant.
#[derive(Clone, Copy, Debug, Default)]
struct Reads {
    whole_wal: u64,
    prefix: u64,
    held: u64,
}

/// With `SESSION_LENGTH_COST_TRACE_READS` set, print the broca frames of the stack behind a
/// whole-WAL or complete-prefix read, for at most 20 reads, to show which path made it.
fn trace_read(kind: &str) {
    static LEFT: AtomicI64 = AtomicI64::new(20);
    if std::env::var_os("SESSION_LENGTH_COST_TRACE_READS").is_none()
        || LEFT.fetch_sub(1, Ordering::Relaxed) <= 0
    {
        return;
    }
    let stack = std::backtrace::Backtrace::force_capture().to_string();
    let frames: Vec<&str> = stack
        .lines()
        .map(str::trim)
        .filter(|line| line.contains("broca") && !line.starts_with("at "))
        .take(16)
        .collect();
    eprintln!("READ {kind}:\n  {}", frames.join("\n  "));
}

impl Reads {
    fn now() -> Self {
        Self {
            whole_wal: WHOLE_WAL_READS.load(Ordering::Relaxed),
            prefix: PREFIX_READS.load(Ordering::Relaxed),
            held: HELD_READS.load(Ordering::Relaxed),
        }
    }

    fn between(from: &Reads, to: &Reads) -> Self {
        Self {
            whole_wal: to.whole_wal - from.whole_wal,
            prefix: to.prefix - from.prefix,
            held: to.held - from.held,
        }
    }

    /// Whether a resume with these reads went through the store's held read, with no whole-WAL
    /// read and no complete-prefix read.
    fn held_only(&self) -> bool {
        self.held >= 1 && self.whole_wal == 0 && self.prefix == 0
    }
}

/// The fixture's WAL store, counting whole-log reads. Every method forwards to the file store,
/// including those with trait defaults, so the run loop sees exactly the file store's behavior.
struct CountingWal {
    inner: FileWalStore,
}

impl WalStore for CountingWal {
    fn uploads(&self) -> Option<broca_core::media_uploads::Uploads> {
        WalStore::uploads(&self.inner)
    }
    fn backup_classes(&self) -> Vec<String> {
        WalStore::backup_classes(&self.inner)
    }
    fn check_move(&self, session: &BindIdentity, write: bool) -> Result<(), broca_wal::WalError> {
        WalStore::check_move(&self.inner, session, write)
    }
    fn open(
        &self,
        session: &BindIdentity,
    ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
        WalStore::open(&self.inner, session)
    }
    fn open_with_retention(
        &self,
        session: &BindIdentity,
        retention: Option<u64>,
    ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
        WalStore::open_with_retention(&self.inner, session, retention)
    }
    fn open_with_class(
        &self,
        session: &BindIdentity,
        retention: Option<u64>,
        session_class: Option<&str>,
    ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
        WalStore::open_with_class(&self.inner, session, retention, session_class)
    }
    fn open_writer(
        &self,
        session: &BindIdentity,
        retention: Option<u64>,
        session_class: Option<&str>,
        lease: Box<dyn broca_core::LeaseHandle>,
    ) -> Result<broca_core::wal_writer::WriterAndLease, broca_wal::WalError> {
        WalStore::open_writer(&self.inner, session, retention, session_class, lease)
    }
    fn retired_tag_floor(&self, session: &BindIdentity) -> Result<u64, broca_wal::WalError> {
        WalStore::retired_tag_floor(&self.inner, session)
    }
    fn retired_notice_floor(&self, session: &BindIdentity) -> Result<u64, broca_wal::WalError> {
        WalStore::retired_notice_floor(&self.inner, session)
    }
    fn is_retention_lineage(&self, session: &BindIdentity) -> Result<bool, broca_wal::WalError> {
        WalStore::is_retention_lineage(&self.inner, session)
    }
    fn lineage_exists(
        &self,
        session: &BindIdentity,
        meta: Option<&broca_core::AdmissionStoreSnapshot>,
    ) -> Result<bool, broca_wal::WalError> {
        WalStore::lineage_exists(&self.inner, session, meta)
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
    fn live_wal_bytes(&self, session: &BindIdentity) -> Result<Option<u64>, broca_wal::WalError> {
        WalStore::live_wal_bytes(&self.inner, session)
    }
    fn read_records_tail_bounded(
        &self,
        session: &BindIdentity,
        tail_bytes: u64,
    ) -> Result<Option<broca_wal::WalRangeRead>, broca_wal::WalError> {
        WalStore::read_records_tail_bounded(&self.inner, session, tail_bytes)
    }
    fn first_user_message(
        &self,
        session: &BindIdentity,
    ) -> Result<(Option<Message>, u64), broca_wal::WalError> {
        WalStore::first_user_message(&self.inner, session)
    }
    fn read_records(
        &self,
        session: &BindIdentity,
    ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        WHOLE_WAL_READS.fetch_add(1, Ordering::Relaxed);
        trace_read("read_records");
        WalStore::read_records(&self.inner, session)
    }
    fn read_records_range(
        &self,
        session: &BindIdentity,
        after_seq: u64,
        through_seq: Option<u64>,
        anchor: Option<&broca_wal::WalByteAnchor>,
    ) -> Result<broca_wal::WalRangeRead, broca_wal::WalError> {
        if anchor.is_none() {
            WHOLE_WAL_READS.fetch_add(1, Ordering::Relaxed);
            trace_read("read_records_range without an anchor");
        }
        WalStore::read_records_range(&self.inner, session, after_seq, through_seq, anchor)
    }
    fn read_records_range_for_authorization(
        &self,
        session: &BindIdentity,
        after_seq: u64,
        through_seq: Option<u64>,
        anchor: Option<&broca_wal::WalByteAnchor>,
    ) -> Result<broca_wal::WalRangeRead, broca_wal::WalError> {
        if anchor.is_none() {
            WHOLE_WAL_READS.fetch_add(1, Ordering::Relaxed);
            trace_read("read_records_range without an anchor");
        }
        WalStore::read_records_range_for_authorization(
            &self.inner,
            session,
            after_seq,
            through_seq,
            anchor,
        )
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
    fn flush_for_authorization(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<u64>, broca_wal::WalError> {
        WalStore::flush_for_authorization(&self.inner, session)
    }
    fn file_identity_for_authorization(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<u64>, broca_wal::WalError> {
        WalStore::file_identity_for_authorization(&self.inner, session)
    }
}

/// The fixture's store as the run loop sees it, counting complete-prefix and held reads.
/// Every method forwards to the SQLite store, including those with trait defaults.
struct CountingStore {
    inner: Arc<SqliteStore>,
}

impl MaterializedStore for CountingStore {
    fn read_retained_head(
        &self,
        session: &BindIdentity,
        lineage_id: &str,
    ) -> Result<Option<broca_core::head::RetainedHeadCheckpoint>, String> {
        MaterializedStore::read_retained_head(self.inner.as_ref(), session, lineage_id)
    }
    fn checkpoint_retained_head(
        &self,
        session: &BindIdentity,
        checkpoint: &broca_core::head::RetainedHeadCheckpoint,
    ) -> Result<(), String> {
        MaterializedStore::checkpoint_retained_head(self.inner.as_ref(), session, checkpoint)
    }
    fn tag_location(
        &self,
        session: &BindIdentity,
        tag: u64,
    ) -> Result<Option<broca_core::TagLocation>, String> {
        MaterializedStore::tag_location(self.inner.as_ref(), session, tag)
    }
    fn materialized_message_count(&self, session: &BindIdentity) -> Result<Option<u64>, String> {
        MaterializedStore::materialized_message_count(self.inner.as_ref(), session)
    }
    fn read_run_event(&self, event_key: &str) -> Result<Option<(BindIdentity, Vec<u8>)>, String> {
        MaterializedStore::read_run_event(self.inner.as_ref(), event_key)
    }
    fn read_page_with_attribution(
        &self,
        session: &BindIdentity,
        from_ordinal: Option<u64>,
        limit: u64,
        view: broca_core::ReadView,
    ) -> Result<broca_core::StorePageSnapshot, broca_core::StorePageReadError> {
        MaterializedStore::read_page_with_attribution(
            self.inner.as_ref(),
            session,
            from_ordinal,
            limit,
            view,
        )
    }
    fn read_prefix_with_watermark(
        &self,
        session: &BindIdentity,
    ) -> Result<broca_core::StorePrefix, String> {
        PREFIX_READS.fetch_add(1, Ordering::Relaxed);
        trace_read("read_prefix_with_watermark");
        MaterializedStore::read_prefix_with_watermark(self.inner.as_ref(), session)
    }
    fn read_held_prefix(
        &self,
        session: &BindIdentity,
        held: &broca_core::HeldRowsRequest,
    ) -> Result<Option<broca_core::HeldPrefix>, String> {
        let read = MaterializedStore::read_held_prefix(self.inner.as_ref(), session, held);
        if matches!(read, Ok(Some(_))) {
            HELD_READS.fetch_add(1, Ordering::Relaxed);
        }
        read
    }
    fn read_episode_projection(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<Vec<broca_core::context::EpisodeRenderProjection>>, String> {
        MaterializedStore::read_episode_projection(self.inner.as_ref(), session)
    }
    fn read_episode_projection_async(
        &self,
        session: &BindIdentity,
    ) -> broca_core::context::EpisodeProjectionRead<'_> {
        MaterializedStore::read_episode_projection_async(self.inner.as_ref(), session)
    }
    fn read_admission_snapshot(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<broca_core::AdmissionStoreSnapshot>, String> {
        MaterializedStore::read_admission_snapshot(self.inner.as_ref(), session)
    }
    fn read_page_with_watermark(
        &self,
        session: &BindIdentity,
        from_ordinal: Option<u64>,
        limit: u64,
        view: broca_core::ReadView,
    ) -> Result<broca_core::StorePageSnapshot, String> {
        MaterializedStore::read_page_with_watermark(
            self.inner.as_ref(),
            session,
            from_ordinal,
            limit,
            view,
        )
    }
    fn reproject_if_lineage_mismatch(
        &self,
        session: &BindIdentity,
        stale_lineage_id: Option<&str>,
        complete_wal: &broca_wal::WalRangeRead,
    ) -> Result<(), String> {
        MaterializedStore::reproject_if_lineage_mismatch(
            self.inner.as_ref(),
            session,
            stale_lineage_id,
            complete_wal,
        )
    }
}

impl Persister for CountingStore {
    fn project_retained_head(
        &self,
        session: &BindIdentity,
        lineage_id: &str,
        records: &[broca_wal::FramedRecord],
    ) -> Result<(), String> {
        Persister::project_retained_head(self.inner.as_ref(), session, lineage_id, records)
    }
    fn projection_seed(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<broca_core::ProjectionSeed>, String> {
        Persister::projection_seed(self.inner.as_ref(), session)
    }
    fn project(
        &self,
        session: &BindIdentity,
        records: &[broca_wal::FramedRecord],
    ) -> Result<u64, String> {
        Persister::project(self.inner.as_ref(), session, records)
    }
    fn project_retention_metadata(
        &self,
        session: &BindIdentity,
        metadata: &broca_core::projection_plan::RetainedWithdrawalProjection,
    ) -> Result<(), String> {
        Persister::project_retention_metadata(self.inner.as_ref(), session, metadata)
    }
    fn project_range(
        &self,
        session: &BindIdentity,
        range: &broca_wal::WalRangeRead,
    ) -> Result<u64, String> {
        Persister::project_range(self.inner.as_ref(), session, range)
    }
    fn persist_wal_anchor(
        &self,
        session: &BindIdentity,
        anchor: Option<&broca_wal::WalByteAnchor>,
    ) -> Result<(), String> {
        Persister::persist_wal_anchor(self.inner.as_ref(), session, anchor)
    }
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
    /// The store and WAL read counters at entry.
    reads: Reads,
    after: Snap,
    prompt_messages: usize,
    request_bytes: usize,
}

/// A scripted provider that keeps nothing of the requests it is sent except their sizes, so
/// the heap it adds does not grow with the session. Before the call at index `arm_at`, it
/// arms `views` to publish a newer view, which the run fetches at its next step boundary. At
/// the call at index `hang_at` it never answers: it signals the notifier and keeps the
/// stream open, so the caller can drop the run mid-step as a kill would end it.
struct Probe {
    scripts: Mutex<VecDeque<Vec<NormalizedEvent>>>,
    calls: Mutex<Vec<Call>>,
    arm: Option<(usize, Arc<Views>)>,
    hang_at: Option<(usize, Arc<Notify>)>,
    /// Senders of hung calls, kept so their streams never close.
    hung: Mutex<Vec<mpsc::Sender<NormalizedEvent>>>,
    /// The frozen render config this provider declares; `None` is the stateless default.
    render: Option<Value>,
}

impl Probe {
    fn new(scripts: Vec<Vec<NormalizedEvent>>, render: Option<Value>) -> Self {
        Self {
            scripts: Mutex::new(scripts.into()),
            calls: Mutex::new(Vec::new()),
            arm: None,
            hang_at: None,
            hung: Mutex::new(Vec::new()),
            render,
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

    fn render_config_blob(&self) -> Value {
        self.render
            .clone()
            .unwrap_or_else(|| json!({ "render_config": "stateless" }))
    }
}

impl Probe {
    fn step_now(&self, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        TRACING.store(false, Ordering::Relaxed);
        let reads = Reads::now();
        let before = snap();
        let before_peak = PEAK_LIVE_BYTES.load(Ordering::Relaxed);
        let request_bytes = broca_core::canonical_request_bytes(opts).len();
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
        let hang = self
            .hang_at
            .as_ref()
            .filter(|(at, _)| *at == index)
            .map(|(_, notify)| notify.clone());
        if hang.is_none() {
            let script = self.scripts.lock().unwrap().pop_front().unwrap_or_default();
            for event in script {
                tx.try_send(event).expect("script fits the channel");
            }
        }
        let after = snap();
        self.calls.lock().unwrap().push(Call {
            before,
            before_peak,
            reads,
            after,
            prompt_messages: opts.prompt.len(),
            request_bytes,
        });
        if let Some(notify) = hang {
            self.hung.lock().unwrap().push(tx);
            notify.notify_one();
        }
        TRACING.store(TRACE_RUN.load(Ordering::Relaxed), Ordering::Relaxed);
        rx
    }
}

// ── the scripted compaction provider ─────────────────────────────────────────────────────────

/// A compaction provider holding one view: ordinal 0 kept, a single summary message in place of
/// ordinals 1 through `cut - 1`, and the transcript from ordinal `cut` on kept as written. `publish_next` announces a newer version of the same view.
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
        // The cut is printed at a fixed width, so the replacement is the same size at every N.
        let summary = json!([{"role":"user","content":[{"type":"text",
            "text":format!("summary of the discarded history before ordinal {cut:09}")}]}]);
        // No `with_cut`: broca refuses a view that starts a new lineage at a cut, so the view
        // only replaces history, as the views broca applies in production do.
        CompactionView::new(
            session_ref,
            version,
            lineage,
            MessageRef {
                ordinal: cut - 1,
                mid: format!("m{}", cut - 1),
            },
            vec![
                ViewRange::keep(0, 1),
                ViewRange::replace(1, cut, summary.to_string()),
            ],
        )
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
        rich_ui: None,
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

/// A run request: `inputs` user messages (the suffix has one two-user episode, which makes the
/// suffix an exact length), and the two deterministic tools. The step cap leaves room for the
/// 50-message episode's 25 model steps.
fn request(session: &BindIdentity, run: &str, inputs: usize) -> RunRequest {
    let tool = |name: &str| Tool {
        name: name.into(),
        description: "deterministic benchmark tool".into(),
        input_schema: json!({"type": "object"}),
        module: None,
        execution_mode: ExecutionMode::Parallel,
        reply: None,
    };
    RunRequest {
        input_written_at_ms: None,
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
        input: (0..inputs)
            .map(|index| {
                text_message(
                    Role::User,
                    format!("question {run}.{index}: please look this up"),
                )
            })
            .collect(),
        // `read` is a read-only tool, so its repeated identical results draw repetition
        // guidance; `lookup` is not, so it never does.
        tools: vec![tool("lookup"), tool("read")],
        policy_allowed: HashSet::from(["lookup".to_owned(), "read".to_owned()]),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        stop_when: vec![StopCondition::StepCount {
            n: LONG_READS as u64 + 8,
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

fn tool_step(run: &str, step: usize, tool: &str) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(format!("msg-{run}-c{step}")),
                content: vec![ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId(format!("call-{run}-{step}")),
                    tool_name: tool.into(),
                    input: json!({"query": "fixed"}),
                })],
            },
        }),
        finished(FinishReason::ToolCalls),
    ]
}

/// `tool_steps` steps calling `tool`, then a text answer.
fn scripts(run: &str, tool_steps: usize, tool: &str) -> Vec<Vec<NormalizedEvent>> {
    (0..tool_steps)
        .map(|step| tool_step(run, step, tool))
        .chain(std::iter::once(text_step(run)))
        .collect()
}

/// The counters just before the call into the run loop, the start of the resume interval.
struct RunStart {
    snap: Snap,
    reads: Reads,
}

/// Drive one run of `fixture`'s session. `started` receives the counters taken just before
/// the call into `run_loop_on_writer`, before the run's future is first polled, so a caller
/// that drops the run mid-way still has them.
#[allow(clippy::too_many_arguments)]
async fn drive(
    fixture: &Fixture,
    run: &str,
    inputs: usize,
    mode: StartMode,
    provider: &Probe,
    control: RunControl,
    store: bool,
    started: &Mutex<Option<RunStart>>,
) -> broca_core::RunSummary {
    let (tx, mut rx) = mpsc::channel(1024);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let session_store: Option<Arc<dyn SessionStore>> = store.then(|| {
        let store: Arc<dyn SessionStore> = fixture.counted.clone();
        store
    });
    let request = request(&fixture.session, run, inputs);
    let reads = Reads::now();
    *started.lock().unwrap() = Some(RunStart {
        snap: snap(),
        reads,
    });
    let summary = broca_core::run_loop_on_writer(
        request,
        &fixture.writer,
        mode,
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

/// The two session flavors the benchmark runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Flavor {
    /// A stateless render config, as an Anthropic-style provider records.
    Anthropic,
    /// A frozen render config asking for ChatGPT effort updates, with the reasoning effort
    /// changing between episode kinds, so the effort history has changed.
    ChatGpt,
}

impl Flavor {
    fn name(self) -> &'static str {
        match self {
            Self::Anthropic => "anthropic",
            Self::ChatGpt => "chatgpt",
        }
    }

    /// The render config a provider of this flavor declares for an episode at `effort`.
    fn render(self, effort: &str) -> Option<Value> {
        match self {
            Self::Anthropic => None,
            Self::ChatGpt => Some(json!({
                "chatgpt_effort_updates": true,
                "reasoning": {"effort": effort},
            })),
        }
    }
}

/// The kinds of episode the session is built from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    /// The lineage's first episode: one user and one assistant text.
    First,
    /// One user and one assistant text.
    Text,
    /// One user, one `lookup` call and result, and an assistant text.
    Tool,
    /// One user, [`LONG_READS`] identical `read` calls and results, and an assistant text.
    Long,
    /// Two users and one assistant text.
    Pair,
}

impl Kind {
    /// The order the templates are captured in.
    const TEMPLATES: [Kind; 5] = [Kind::First, Kind::Text, Kind::Tool, Kind::Long, Kind::Pair];

    fn messages(self) -> usize {
        match self {
            Self::First | Self::Text => 2,
            Self::Tool => 4,
            Self::Long => LONG_MESSAGES,
            Self::Pair => 3,
        }
    }

    fn inputs(self) -> usize {
        if self == Self::Pair {
            2
        } else {
            1
        }
    }

    fn tool_steps(self) -> usize {
        match self {
            Self::Tool => 1,
            Self::Long => LONG_READS,
            _ => 0,
        }
    }

    fn tool(self) -> &'static str {
        if self == Self::Long {
            "read"
        } else {
            "lookup"
        }
    }

    /// The reasoning effort of this kind's episodes in a ChatGPT session.
    fn effort(self) -> &'static str {
        match self {
            Self::First | Self::Text => "low",
            Self::Tool | Self::Long => "high",
            Self::Pair => "medium",
        }
    }
}

/// The episodes a fixture is built from, in lineage order, before the episode that applies the
/// view.
#[derive(Debug)]
struct SessionLayout {
    episodes: Vec<Kind>,
    /// Where the 50-message episode starts.
    long_start: usize,
    /// The view's cut: the first suffix message. Ordinals 1 to `cut - 1` are replaced.
    cut: usize,
    /// Transcript messages in all of `episodes`.
    messages: usize,
}

/// The layout of a session of about `target` messages. Discarded `Text` and `Tool` episodes
/// fill the space between the first episode and the 50-message episode; nothing else depends
/// on `target`. The cut falls inside the 50-message episode, which leaves 25 of its messages
/// before the cut and 25 after it, and the episodes after it make the suffix exactly
/// [`SUFFIX`] messages long.
fn session_layout(target: usize) -> SessionLayout {
    let suffix_rest = SUFFIX - (LONG_MESSAGES - LONG_BEFORE_CUT);
    // The 50-message episode, the rest of the suffix and the episode that applies the view.
    let fixed = LONG_MESSAGES + suffix_rest + Kind::Text.messages();
    let mut episodes = vec![Kind::First];
    let mut messages = Kind::First.messages();
    while messages + Kind::Tool.messages() + fixed <= target {
        let kind = if episodes.len() % TOOL_EVERY == TOOL_EVERY - 1 {
            Kind::Tool
        } else {
            Kind::Text
        };
        episodes.push(kind);
        messages += kind.messages();
    }
    let long_start = messages;
    episodes.push(Kind::Long);
    messages += LONG_MESSAGES;
    // The rest of the suffix is an odd count (the long episode leaves 25 messages after the
    // cut), so one three-message episode makes it reachable with two- and four-message ones.
    episodes.push(Kind::Pair);
    let mut rest = suffix_rest - Kind::Pair.messages();
    let mut index = 0;
    while rest > 0 {
        let kind = if rest >= Kind::Tool.messages() && index % 5 == 4 {
            Kind::Tool
        } else {
            Kind::Text
        };
        episodes.push(kind);
        rest -= kind.messages();
        index += 1;
    }
    messages += suffix_rest;
    SessionLayout {
        episodes,
        long_start,
        cut: long_start + LONG_BEFORE_CUT,
        messages,
    }
}

/// The records of one episode the production run loop wrote, as JSON, with the number of
/// transcript messages before it in the template session.
struct TemplateEpisode {
    records: Vec<Value>,
    messages_before: usize,
    marker: String,
}

struct Templates {
    first: Vec<WalRecord>,
    /// One template per kind other than `First`, in [`Kind::TEMPLATES`] order.
    kinds: Vec<(Kind, TemplateEpisode)>,
}

impl Templates {
    /// Runs one real episode of each kind through the production run loop into a scratch WAL
    /// of the same session identity and keeps their records.
    async fn capture(root: &std::path::Path, session: &BindIdentity, flavor: Flavor) -> Self {
        let wal_store = FileWalStore::new(root.join("template-wal"));
        let writer = WalHandle::spawn(wal_store.open(session).expect("open template WAL"), 1)
            .expect("spawn template writer");
        let plane = tool_plane();
        // The session's compaction plan is frozen by its first episode, so the template
        // runs under it too; its Setup head is recorded in the first episode. No view is
        // published while templates are captured.
        let views = Arc::new(Views::new(1));
        views.disabled.store(true, Ordering::SeqCst);
        let (tx, mut rx) = mpsc::channel(256);
        let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        for (episode, kind) in Kind::TEMPLATES.into_iter().enumerate() {
            let run = marker(episode);
            let provider = Probe::new(
                scripts(&run, kind.tool_steps(), kind.tool()),
                flavor.render(kind.effort()),
            );
            let mode = if episode == 0 {
                StartMode::FreshOrResume
            } else {
                StartMode::AppendEpisode
            };
            broca_core::run_loop_on_writer(
                request(session, &run, kind.inputs()),
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
        let replayed = replay(
            &frames
                .iter()
                .map(|frame| frame.record.clone())
                .collect::<Vec<_>>(),
        )
        .expect("replay template WAL")
        .prompt
        .len();
        let expected: usize = Kind::TEMPLATES.iter().map(|kind| kind.messages()).sum();
        assert_eq!(
            replayed, expected,
            "the template episodes hold the messages their kinds count"
        );
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
        assert_eq!(
            episodes.len(),
            Kind::TEMPLATES.len(),
            "one template episode per kind"
        );
        let json = |records: &[WalRecord]| -> Vec<Value> {
            records
                .iter()
                .map(|record| serde_json::to_value(record).expect("record JSON"))
                .collect()
        };
        let mut messages_before = Kind::First.messages();
        let mut kinds = Vec::new();
        for (episode, kind) in Kind::TEMPLATES.into_iter().enumerate().skip(1) {
            kinds.push((
                kind,
                TemplateEpisode {
                    records: json(&episodes[episode]),
                    messages_before,
                    marker: marker(episode),
                },
            ));
            messages_before += kind.messages();
        }
        Self {
            first: episodes[0].clone(),
            kinds,
        }
    }

    /// Episode `episode`'s records, a copy of the `kind` template starting at transcript message
    /// `messages_before`: the template's ids renamed and its runner tags (numbered by
    /// transcript position) moved.
    fn episode(&self, kind: Kind, episode: usize, messages_before: usize) -> Vec<WalRecord> {
        let template = &self
            .kinds
            .iter()
            .find(|(candidate, _)| *candidate == kind)
            .expect("a template of every kind but the first")
            .1;
        let delta = messages_before as i64 - template.messages_before as i64;
        let name = marker(episode);
        template
            .records
            .iter()
            .map(|value| {
                let text = value.to_string().replace(&template.marker, &name);
                let mut value: Value = serde_json::from_str(&text).expect("renamed JSON");
                shift_tags(&mut value, delta, false);
                serde_json::from_value(value).expect("renamed record decodes")
            })
            .collect()
    }
}

/// The tool plane every run uses: each tool answers its own fixed text.
fn tool_plane() -> StubToolPlane {
    StubToolPlane::new()
        .with_canned_text("lookup", "fixed tool result")
        .with_canned_text("read", "fixed file contents")
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
    flavor: Flavor,
    session: BindIdentity,
    wal_store: CountingWal,
    store: Arc<SqliteStore>,
    /// The same store, counting the reads the run loop makes.
    counted: Arc<CountingStore>,
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
    async fn build(target: usize, flavor: Flavor) -> Self {
        let started = Instant::now();
        let base = std::env::var_os("SESSION_LENGTH_COST_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let root = base.join(format!(
            "broca-session-length-{}-{}-{target}",
            std::process::id(),
            flavor.name()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("project")).expect("create fixture directory");
        let session = BindIdentity::new(root.join("project"), "bench", "cost");
        let templates = Templates::capture(&root, &session, flavor).await;

        let wal_store = CountingWal {
            inner: FileWalStore::new(root.join("wal")),
        };
        let writer =
            WalHandle::spawn(wal_store.open(&session).expect("open WAL"), 1).expect("spawn writer");
        let store = Arc::new(SqliteStore::open(root.join("store.db")).expect("open SQLite store"));

        let layout = session_layout(target);
        for record in &templates.first {
            writer.record(record.clone());
        }
        let mut messages = Kind::First.messages();
        for (episode, kind) in layout.episodes.iter().copied().enumerate().skip(1) {
            for record in templates.episode(kind, episode, messages) {
                writer.record(record);
            }
            messages += kind.messages();
            if episode % 4096 == 0 {
                writer.sync().await.expect("sync fixture");
            }
        }
        assert_eq!(messages, layout.messages, "the fixture follows its layout");
        eprintln!(
            "the cut at ordinal {} falls inside the {LONG_MESSAGES}-message episode at ordinal {}",
            layout.cut, layout.long_start
        );
        writer.sync().await.expect("sync fixture");
        let cut = layout.cut as u64;
        let views = Arc::new(Views::new(cut));
        let mut fixture = Self {
            root,
            flavor,
            session,
            wal_store,
            counted: Arc::new(CountingStore {
                inner: store.clone(),
            }),
            store,
            writer,
            plane: tool_plane(),
            views,
            templates,
            next_episode: layout.episodes.len(),
            messages,
            cut,
            build_time: Duration::ZERO,
            setup_prompt_messages: 0,
        };

        // One real episode that fetches and applies the view at its first step boundary. No
        // store yet, so this run replays the whole WAL once.
        let provider = Probe::new(vec![text_step("apply-view")], flavor.render("low"));
        drive(
            &fixture,
            "apply-view",
            1,
            StartMode::AppendEpisode,
            &provider,
            compaction_control(&fixture.views),
            false,
            &Mutex::new(None),
        )
        .await;
        fixture.messages += 2;
        fixture.setup_prompt_messages = provider.calls()[0].prompt_messages;
        let head = fixture.writer.sync().await.expect("sync view episode");
        let frames = fixture
            .wal_store
            .inner
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
            // Say why: judge the view the way the run loop does, over the whole transcript.
            let records: Vec<WalRecord> = frames.iter().map(|frame| frame.record.clone()).collect();
            let state = replay(&records).expect("replay fixture");
            let compaction = broca_core::AdmissionCheckpointV1::from_frames(&frames)
                .ok()
                .and_then(|checkpoint| checkpoint.compaction)
                .and_then(|checkpoint| checkpoint.state().cloned())
                .unwrap_or_default();
            let mut raw = state.prompt.clone();
            broca_protocol::content::stamp_ordinals(&mut raw);
            let lineage = fixture.views.lineage.lock().unwrap().clone();
            let decision = lineage.map(|lineage| {
                let session_ref = broca_core::agent_session_ref(&fixture.session);
                let view = fixture.views.view(&session_ref, 2);
                broca_core::compaction::view::decide(
                    &view,
                    &ViewReady::for_view(&view),
                    &compaction,
                    &lineage,
                    &raw,
                    &raw,
                    false,
                )
            });
            panic!("the fixture did not apply its view: {decision:?}");
        }
        drop(frames);

        // Project the whole lineage into the store once, as the projector would have.
        let range = fixture
            .wal_store
            .inner
            .read_records_range(&fixture.session, 0, Some(head), None)
            .expect("read whole WAL for the initial projection");
        Persister::project_range(fixture.store.as_ref(), &fixture.session, &range)
            .expect("initial projection");
        fixture.build_time = started.elapsed();
        fixture
    }

    /// Bring the store up to the WAL head, as the projector does after a run, so no projection
    /// is in flight and the next resume reads the same WAL tail at every N. With no usable
    /// projection seed (a store whose episode projection is absent), it projects the complete
    /// WAL, as the run loop's projector does.
    async fn quiesce(&self) {
        let head = self.writer.sync().await.expect("sync before projection");
        let seed = Persister::projection_seed(self.store.as_ref(), &self.session)
            .expect("projection seed");
        let (after, anchor) = seed.map_or((0, None), |seed| (seed.watermark_seq, seed.anchor));
        let range = self
            .wal_store
            .inner
            .read_records_range(&self.session, after, Some(head), anchor.as_ref())
            .expect("read WAL for projection");
        let watermark = Persister::project_range(self.store.as_ref(), &self.session, &range)
            .expect("project to the WAL head");
        // A projection that does not advance the watermark (the run's own projector already
        // materialized every complete turn) writes no byte anchor, which erases the one the
        // run's projector stored. Put it back, so this catch-up leaves the store as the run left
        // it and the next resume's WAL read still starts from the watermark's byte offset.
        if watermark == after {
            Persister::persist_wal_anchor(self.store.as_ref(), &self.session, anchor.as_ref())
                .expect("restore the WAL anchor");
        }
    }

    /// Make the store look like one written before the episode projection existed: such a
    /// build never wrote `meta.episode_metadata_json`, so the column is NULL. This is the only
    /// direct SQL the benchmark runs.
    fn clear_episode_projection(&self) {
        let connection =
            rusqlite::Connection::open(self.root.join("store.db")).expect("open store directly");
        connection
            .busy_timeout(Duration::from_secs(5))
            .expect("set busy timeout");
        connection
            .execute("UPDATE meta SET episode_metadata_json = NULL", [])
            .expect("clear the episode projection");
    }

    fn episode_projection_present(&self) -> bool {
        MaterializedStore::read_episode_projection(self.store.as_ref(), &self.session)
            .expect("read episode projection")
            .is_some()
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
    /// Store and WAL reads from the call into the run loop to the first provider call.
    resume_reads: Reads,
}

/// The sample of a run whose provider saw `calls`, started at `start`. The pre-run baseline
/// is the live heap just before the call into the run loop.
fn run_sample(calls: &[Call], start: &RunStart, arm_view_at: Option<usize>) -> RunSample {
    assert!(calls.len() >= 2, "a sampled run makes at least two calls");
    let steps: Vec<Cost> = calls
        .windows(2)
        .enumerate()
        .filter(|(index, _)| arm_view_at != Some(index + 1))
        .map(|(_, pair)| Cost::between(&pair[0].after, &pair[1].before))
        .collect();
    let view_step = arm_view_at.map(|at| Cost::between(&calls[at - 1].after, &calls[at].before));
    let base = &start.snap;
    RunSample {
        resume: Cost::between(base, &calls[0].before),
        resume_peak: calls[0].before_peak - base.live_bytes,
        held_first_call: calls[0].before.live_bytes - base.live_bytes,
        held_last_call: calls[calls.len() - 1].before.live_bytes - base.live_bytes,
        step: Cost::median(&steps),
        step_mean: Cost::mean(&steps),
        view_step,
        sent_prompt_messages: calls[1].prompt_messages,
        sent_request_bytes: calls[1].request_bytes,
        resume_reads: Reads::between(&start.reads, &calls[0].reads),
    }
}

/// The counters `drive` took at the start of a run.
fn take_start(started: &Mutex<Option<RunStart>>) -> RunStart {
    started.lock().unwrap().take().expect("the run started")
}

/// One appended episode of five tool steps and a closing text, then the store brought up to
/// the WAL head.
async fn measured_run(fixture: &Fixture, run: &str, arm_view_at: Option<usize>) -> RunSample {
    let mut provider = Probe::new(
        scripts(run, RUN_STEPS - 1, "lookup"),
        fixture.flavor.render("medium"),
    );
    provider.arm = arm_view_at.map(|at| (at, fixture.views.clone()));
    TRACE_RUN.store(arm_view_at.is_none(), Ordering::Relaxed);
    // Trace the resume too, from the call into the run loop to the first provider call.
    TRACING.store(arm_view_at.is_none(), Ordering::Relaxed);
    PEAK_LIVE_BYTES.store(LIVE_BYTES.load(Ordering::Relaxed), Ordering::Relaxed);
    let started = Mutex::new(None);
    drive(
        fixture,
        run,
        1,
        StartMode::AppendEpisode,
        &provider,
        compaction_control(&fixture.views),
        true,
        &started,
    )
    .await;
    TRACING.store(false, Ordering::Relaxed);
    TRACE_RUN.store(false, Ordering::Relaxed);
    let calls = provider.calls();
    assert_eq!(calls.len(), RUN_STEPS, "every scripted step ran");
    let sample = run_sample(&calls, &take_start(&started), arm_view_at);
    fixture.quiesce().await;
    sample
}

/// The crash case: an appended episode that applies a newly published view at its second
/// call's boundary is killed during its fourth provider call, and the killed episode is then
/// resumed. The sample is the resumed run's. The active episode holds the same messages at
/// every N when it is resumed.
async fn crash_sample(fixture: &Fixture, run: &str) -> RunSample {
    let render = fixture.flavor.render("high");
    let notify = Arc::new(Notify::new());
    let mut killed = Probe::new(scripts(run, RUN_STEPS - 1, "lookup"), render.clone());
    killed.arm = Some((1, fixture.views.clone()));
    killed.hang_at = Some((CRASH_AT_CALL, notify.clone()));
    let fetches = fixture.views.fetches.load(Ordering::SeqCst);
    let killed_start = Mutex::new(None);
    {
        let run_future = drive(
            fixture,
            run,
            1,
            StartMode::AppendEpisode,
            &killed,
            compaction_control(&fixture.views),
            true,
            &killed_start,
        );
        tokio::select! {
            _ = run_future => panic!("the crash-case run finished before it was killed"),
            () = notify.notified() => {}
        }
        // Leaving this block drops the run mid-step, as a kill ends it.
    }
    assert_eq!(
        killed.calls().len(),
        CRASH_AT_CALL + 1,
        "the killed run reached its hung call"
    );
    assert_eq!(
        fixture.views.fetches.load(Ordering::SeqCst),
        fetches + 1,
        "the killed run fetched the newer view before it died"
    );
    // Make what the killed run wrote durable and wait out any projection it had started (the
    // store's lock orders this projection after it), so the resume reads a quiet store.
    fixture.quiesce().await;

    let resumed = Probe::new(
        vec![tool_step(run, CRASH_AT_CALL, "lookup"), text_step(run)],
        render,
    );
    PEAK_LIVE_BYTES.store(LIVE_BYTES.load(Ordering::Relaxed), Ordering::Relaxed);
    let started = Mutex::new(None);
    drive(
        fixture,
        run,
        1,
        StartMode::FreshOrResume,
        &resumed,
        compaction_control(&fixture.views),
        true,
        &started,
    )
    .await;
    let calls = resumed.calls();
    assert_eq!(calls.len(), 2, "the resumed episode finished its script");
    let sample = run_sample(&calls, &take_start(&started), None);
    fixture.quiesce().await;
    sample
}

/// The medians of one case's samples, with how many of its resumes read only the held set.
#[derive(Clone, Copy, Debug)]
struct CaseMedians {
    resume: Cost,
    resume_peak: i64,
    held_first_call: i64,
    held_last_call: i64,
    runs: usize,
    held_only_runs: usize,
    /// The most reads of each kind any one resume of the case made.
    most_reads: Reads,
}

impl CaseMedians {
    fn of(samples: &[RunSample]) -> Self {
        Self {
            resume: Cost::median(&samples.iter().map(|s| s.resume).collect::<Vec<_>>()),
            resume_peak: median(samples.iter().map(|s| s.resume_peak).collect()),
            held_first_call: median(samples.iter().map(|s| s.held_first_call).collect()),
            held_last_call: median(samples.iter().map(|s| s.held_last_call).collect()),
            runs: samples.len(),
            held_only_runs: samples
                .iter()
                .filter(|s| s.resume_reads.held_only())
                .count(),
            most_reads: Reads {
                whole_wal: samples
                    .iter()
                    .map(|s| s.resume_reads.whole_wal)
                    .max()
                    .unwrap_or(0),
                prefix: samples
                    .iter()
                    .map(|s| s.resume_reads.prefix)
                    .max()
                    .unwrap_or(0),
                held: samples
                    .iter()
                    .map(|s| s.resume_reads.held)
                    .max()
                    .unwrap_or(0),
            },
        }
    }
}

/// The bytes of `session.read` on the fixture's session: the default (raw) view from the
/// newest page, from the cut and from ordinal 0, and the model view from the newest page and
/// from the cut. The pages' `Debug` forms carry every field they hold.
fn session_read_bytes(fixture: &Fixture) -> Vec<u8> {
    let session = &fixture.session;
    let tail_records = |page: &broca_core::StorePageSnapshot| {
        fixture
            .wal_store
            .inner
            .read_records_range(session, page.watermark_seq, None, page.anchor.as_ref())
            .expect("read WAL tail")
    };
    let mut out = Vec::new();
    for from in [None, Some(fixture.cut), Some(0)] {
        let page = MaterializedStore::read_page_with_watermark(
            fixture.store.as_ref(),
            session,
            from,
            PAGE_LIMIT,
            broca_core::context::ReadView::Raw,
        )
        .expect("read store page");
        let tail = tail_records(&page);
        let read = read_session_page(&page, range_records(&tail), from, Some(PAGE_LIMIT))
            .expect("assemble session page");
        out.extend(format!("raw {from:?}\n{read:?}\n").into_bytes());
    }
    for from in [None, Some(fixture.cut)] {
        let page = MaterializedStore::read_page_with_watermark(
            fixture.store.as_ref(),
            session,
            from,
            PAGE_LIMIT,
            broca_core::context::ReadView::Model,
        )
        .expect("read store page");
        let tail = tail_records(&page);
        let read = broca_core::read_session_model_page(
            &page,
            range_records(&tail),
            &broca_core::ModelPageRequest {
                from_ordinal: from,
                after_mid: None,
                limit: Some(PAGE_LIMIT),
                max_bytes: None,
            },
            || {
                fixture
                    .wal_store
                    .inner
                    .read_records(session)
                    .map_err(|error| error.to_string())
            },
        )
        .expect("model page");
        out.extend(format!("model {from:?}\n{read:?}\n").into_bytes());
    }
    out
}

fn range_records(range: &broca_wal::WalRangeRead) -> &[broca_wal::FramedRecord] {
    match range.records() {
        WalRangeRecords::Complete(records)
        | WalRangeRecords::Partial(records)
        | WalRangeRecords::Unknown(records) => records,
        WalRangeRecords::Absent => &[],
    }
}

struct Report {
    n: usize,
    flavor: Flavor,
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
    /// The plain measured runs' resumes and resident heap.
    steps_case: CaseMedians,
    /// Resumes of an episode killed mid-step after a view applied in it.
    crash_case: CaseMedians,
    /// Resumes from a store written before the episode projection existed, after projection
    /// filled it in.
    prechange_case: CaseMedians,
    /// Whole-WAL reads of the first resume on that store, before projection filled it in (a
    /// full replay, as such a store must take).
    prechange_first_whole_wal_reads: u64,
}

async fn measure_size(target: usize, flavor: Flavor) -> Report {
    eprintln!("building a {target}-message {} session...", flavor.name());
    let mut fixture = Fixture::build(target, flavor).await;
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
    // A run killed mid-episode after a view applied in it, and the killed episode resumed.
    let mut crash_samples = Vec::new();
    for sample in 0..(WARMUPS + SAMPLES) {
        let run = format!("crash-{sample}");
        let measured = crash_sample(&fixture, &run).await;
        // The killed run's user and three pairs, then the resumed pair and closing text.
        fixture.messages += CRASH_MESSAGES_AT_KILL + 3;
        if sample >= WARMUPS {
            crash_samples.push(measured);
        }
    }
    let view_fetches = fixture.views.fetches.load(Ordering::SeqCst);
    fixture.views.disabled.store(true, Ordering::SeqCst);

    // A store written before the episode projection existed. Its session.read output must be
    // the same bytes as this build's store gives. Its first resume replays the whole WAL; once
    // projection has filled the episode projection in, resumes must be as flat as above.
    let reads_current = session_read_bytes(&fixture);
    fixture.clear_episode_projection();
    assert!(
        !fixture.episode_projection_present(),
        "the store reads as one written before the episode projection"
    );
    let reads_prechange = session_read_bytes(&fixture);
    assert!(
        reads_prechange == reads_current,
        "session.read differs between a store written before the episode projection and this \
         build's store"
    );
    let first = measured_run(&fixture, "prechange-fill", None).await;
    fixture.messages += 2 * RUN_STEPS;
    assert!(
        fixture.episode_projection_present(),
        "projection filled the episode projection in"
    );
    let prechange_first_whole_wal_reads = first.resume_reads.whole_wal;
    let mut prechange_samples = Vec::new();
    for sample in 0..(WARMUPS + SAMPLES) {
        let run = format!("prechange-{sample}");
        let measured = measured_run(&fixture, &run, None).await;
        fixture.messages += 2 * RUN_STEPS;
        if sample >= WARMUPS {
            prechange_samples.push(measured);
        }
    }

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
    // Match the registry's page request. The store expands model reads to the episode history
    // needed for guidance; core still requests complete WAL replay for legacy projections.
    let complete_reads = AtomicU64::new(0);
    let (model_read_from_cut, _) = measure(|| {
        let page = MaterializedStore::read_page_with_watermark(
            fixture.store.as_ref(),
            &session,
            Some(fixture.cut),
            PAGE_LIMIT,
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
        // A text episode, so it is one turn.
        let records = fixture
            .templates
            .episode(Kind::Text, fixture.next_episode, fixture.messages);
        fixture.next_episode += 1;
        fixture.messages += Kind::Text.messages();
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
        flavor,
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
        steps_case: CaseMedians::of(&samples),
        crash_case: CaseMedians::of(&crash_samples),
        prechange_case: CaseMedians::of(&prechange_samples),
        prechange_first_whole_wal_reads,
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
    println!("## N = {}, {} session", r.n, r.flavor.name());
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
    println!("| resume case (median of {SAMPLES}) | time | allocations | allocated | bytes read | held at first call | held at last call | resume peak | resumes through the held read only | most whole-WAL / complete-prefix / held reads in one resume |");
    println!("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
    for (name, case) in cases(r) {
        println!(
            "| {name} | {} | {} | {} | {} | {}/{} | {} / {} / {} |",
            cost_cells(&case.resume),
            mib(case.held_first_call),
            mib(case.held_last_call),
            mib(case.resume_peak),
            case.held_only_runs,
            case.runs,
            case.most_reads.whole_wal,
            case.most_reads.prefix,
            case.most_reads.held
        );
    }
    println!(
        "pre-change store: its first resume made {} whole-WAL read(s) before projection filled \
         the episode projection in; session.read was byte-identical before and after",
        r.prechange_first_whole_wal_reads
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

fn cases(r: &Report) -> [(&'static str, CaseMedians); 3] {
    [
        ("appended episode", r.steps_case),
        ("killed episode resumed", r.crash_case),
        ("pre-change store, projection filled", r.prechange_case),
    ]
}

#[derive(Clone, Copy)]
enum Unit {
    Bytes,
    Ms,
}

fn show(unit: Unit, value: f64) -> String {
    match unit {
        Unit::Bytes => format!("{:.2} MiB", value / MIB),
        Unit::Ms => format!("{value:.3} ms"),
    }
}

/// One value the flatness thresholds bound.
struct Metric {
    name: &'static str,
    unit: Unit,
    threshold: Threshold,
    value: fn(&CaseMedians) -> f64,
}

const METRICS: [Metric; 4] = [
    Metric {
        name: "resident at first provider call",
        unit: Unit::Bytes,
        threshold: THRESHOLDS.resident,
        value: |case| case.held_first_call as f64,
    },
    Metric {
        name: "resident at last provider call",
        unit: Unit::Bytes,
        threshold: THRESHOLDS.resident,
        value: |case| case.held_last_call as f64,
    },
    Metric {
        name: "resume allocated",
        unit: Unit::Bytes,
        threshold: THRESHOLDS.resume_allocated,
        value: |case| case.resume.allocated_bytes as f64,
    },
    Metric {
        name: "resume wall time",
        unit: Unit::Ms,
        threshold: THRESHOLDS.resume_ms,
        value: |case| case.resume.time.as_secs_f64() * 1000.0,
    },
];

/// Whether every value is within `threshold` of `base`, the smallest size's value.
fn flat(threshold: Threshold, base: f64, values: &[f64]) -> bool {
    values.iter().all(|value| threshold.holds(base, *value))
}

/// The step and read numbers of one flavor's sizes, side by side with the bounded values.
fn print_summary(reports: &[&Report]) {
    let flavor = reports[0].flavor.name();
    println!("## Summary, {flavor} session (medians of {SAMPLES})");
    println!(
        "| N | messages | step | view step | read newest | read from cut | read from 0 | \
         model read from cut | project one turn | resume | resume allocated | held at first call | \
         held at last call |"
    );
    println!("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
    for r in reports {
        println!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
            r.n,
            r.messages,
            ms(&r.step),
            ms(&r.view_step),
            ms(&r.read_newest),
            ms(&r.read_from_cut),
            ms(&r.read_from_zero),
            ms(&r.model_read_from_cut),
            ms(&r.project_one_turn),
            ms(&r.steps_case.resume),
            mib(r.steps_case.resume.allocated_bytes as i64),
            mib(r.steps_case.held_first_call),
            mib(r.steps_case.held_last_call),
        );
    }
}

/// Print the verdict for one flavor's reports, in the order measured, and return whether every
/// check held. Each size is compared with the smallest size measured.
fn verdict(reports: &[&Report]) -> bool {
    let flavor = reports[0].flavor.name();
    let base = reports
        .iter()
        .min_by_key(|r| r.n)
        .expect("at least one report");
    let sizes: Vec<String> = reports.iter().map(|r| r.n.to_string()).collect();
    println!(
        "## Verdict, {flavor} session: each N against N = {} (bounds fixed before measurement)",
        base.n
    );
    println!(
        "| case | check | {} | bound at the largest N | holds |",
        sizes.join(" | ")
    );
    println!("|---|---|{}---:|---|", "---:|".repeat(reports.len()));
    let largest = reports.iter().max_by_key(|r| r.n).expect("a report");
    let mut all = true;
    for (index, (case_name, _)) in cases(base).into_iter().enumerate() {
        for metric in &METRICS {
            let value = |r: &Report| (metric.value)(&cases(r)[index].1);
            let values: Vec<f64> = reports.iter().map(|r| value(r)).collect();
            let holds = flat(metric.threshold, value(base), &values);
            all &= holds;
            let cells: Vec<String> = values.iter().map(|v| show(metric.unit, *v)).collect();
            println!(
                "| {case_name} | {} <= {:.2} x + {} | {} | {} (at N = {}) | {} |",
                metric.name,
                metric.threshold.ratio,
                show(metric.unit, metric.threshold.slack),
                cells.join(" | "),
                show(metric.unit, metric.threshold.limit(value(base))),
                largest.n,
                if holds { "yes" } else { "NO" }
            );
        }
        let held: Vec<String> = reports
            .iter()
            .map(|r| {
                let case = cases(r)[index].1;
                format!("{}/{}", case.held_only_runs, case.runs)
            })
            .collect();
        let holds = reports.iter().all(|r| {
            let case = cases(r)[index].1;
            case.held_only_runs == case.runs
        });
        all &= holds;
        println!(
            "| {case_name} | resumes through the held read, no whole-WAL or complete-prefix read | {} | all | {} |",
            held.join(" | "),
            if holds { "yes" } else { "NO" }
        );
    }
    let replayed: Vec<String> = reports
        .iter()
        .map(|r| r.prechange_first_whole_wal_reads.to_string())
        .collect();
    let holds = reports
        .iter()
        .all(|r| r.prechange_first_whole_wal_reads > 0);
    all &= holds;
    println!(
        "| pre-change store | its first resume replays the WAL (the store really lacks the projection) | {} | at least 1 | {} |",
        replayed.join(" | "),
        if holds { "yes" } else { "NO" }
    );
    if reports.len() < 2 {
        println!("Only one size was measured, so the flatness rows compare it with itself.");
    }
    all
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
    let flavors: Vec<Flavor> = match std::env::var("SESSION_LENGTH_COST_FLAVORS").as_deref() {
        Ok("anthropic") => vec![Flavor::Anthropic],
        Ok("chatgpt") => vec![Flavor::ChatGpt],
        Ok("") | Err(_) => vec![Flavor::Anthropic, Flavor::ChatGpt],
        Ok(other) => {
            panic!("SESSION_LENGTH_COST_FLAVORS must be anthropic or chatgpt, not {other}")
        }
    };
    let mut reports = Vec::new();
    for flavor in &flavors {
        for size in &sizes {
            let report = measure_size(*size, *flavor).await;
            print_report(&report);
            reports.push(report);
        }
    }
    let mut all = true;
    for flavor in &flavors {
        let mine: Vec<&Report> = reports.iter().filter(|r| r.flavor == *flavor).collect();
        print_summary(&mine);
        all &= verdict(&mine);
    }
    if !all {
        println!("FAILED: at least one check did not hold");
        std::process::exit(1);
    }
    println!("PASSED: every check held");
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
                history_edit_cost: None,
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Only the discarded episodes before the 50-message episode change with the session's
    /// length; the cut's place in that episode, the suffix and its episodes do not.
    #[test]
    fn only_the_discarded_episodes_change_with_session_length() {
        let layouts: Vec<SessionLayout> = [1_000, 10_000, 50_000, 150_000]
            .into_iter()
            .map(session_layout)
            .collect();
        let held_tail = |layout: &SessionLayout| -> Vec<Kind> {
            let long = layout
                .episodes
                .iter()
                .position(|kind| *kind == Kind::Long)
                .expect("a long episode");
            layout.episodes[long..].to_vec()
        };
        for (layout, target) in layouts.iter().zip([1_000, 10_000, 50_000, 150_000]) {
            assert_eq!(layout.messages - layout.cut, SUFFIX, "the suffix length");
            assert_eq!(
                layout.cut - layout.long_start,
                LONG_BEFORE_CUT,
                "the cut's place in the long episode"
            );
            assert_eq!(
                held_tail(layout),
                held_tail(&layouts[0]),
                "the held episodes"
            );
            let discarded = &layout.episodes[1..layout.episodes.len() - held_tail(layout).len()];
            assert!(
                discarded
                    .iter()
                    .all(|kind| matches!(kind, Kind::Text | Kind::Tool)),
                "only short episodes are discarded"
            );
            let total = layout.messages + Kind::Text.messages();
            assert!(
                total <= target && target - total < Kind::Tool.messages(),
                "a {target}-message session holds {total} messages"
            );
        }
        assert!(
            layouts[3].episodes.len() > layouts[0].episodes.len(),
            "longer sessions add discarded episodes"
        );
    }

    /// A bound allows exactly its ratio of the smallest size's value plus its slack.
    #[test]
    fn a_threshold_allows_its_ratio_and_slack_and_nothing_more() {
        let memory = THRESHOLDS.resident;
        let base = 10.0 * MIB;
        assert!(memory.holds(base, 12.0 * MIB), "1.10 x 10 MiB + 1 MiB");
        assert!(!memory.holds(base, 12.0 * MIB + 1.0));
        let time = THRESHOLDS.resume_ms;
        assert!(time.holds(100.0, 130.0), "1.25 x 100 ms + 5 ms");
        assert!(!time.holds(100.0, 130.001));
        assert_eq!(
            THRESHOLDS.resume_allocated.limit(-5.0),
            MIB,
            "a negative base is 0"
        );
        assert!(flat(time, 100.0, &[90.0, 125.0, 130.0]));
        assert!(!flat(time, 100.0, &[90.0, 131.0, 120.0]));
    }

    /// A resume counts as held only when it made a held read and no whole-WAL or
    /// complete-prefix read.
    #[test]
    fn a_resume_is_held_only_without_a_whole_wal_or_prefix_read() {
        let held = Reads {
            whole_wal: 0,
            prefix: 0,
            held: 1,
        };
        assert!(held.held_only());
        assert!(!Reads {
            whole_wal: 1,
            ..held
        }
        .held_only());
        assert!(!Reads { prefix: 1, ..held }.held_only());
        assert!(!Reads { held: 0, ..held }.held_only());
    }
}
