// UNSAFE IS DELIBERATE AND SCOPED TO THIS BENCHMARK. `GlobalAlloc` is an unsafe trait, so a
// counting allocator cannot be written safely; there is no alternative shape. This binary is a
// measurement tool and is never shipped in the daemon.
#![allow(unsafe_code)]

//! Allocation benchmark for incremental projection-tail replay.
//!
//! Fixtures use synthetic text at production-derived message sizes, but construct and decode the
//! actual WAL records through `FileWal`. Run with:
//! `cargo run --release -p broca-bench --bin projection_tail_alloc`.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    hint::black_box,
    path::PathBuf,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    time::Instant,
};

use broca_core::{replay_projection_tail, ProjectionSeed};
use broca_protocol::{
    content::{AssistantMessage, ContentBlock, Message, Role},
    request::ModelHandle,
    BindIdentity, MessageId, RunId, StepId, Usage,
};
use broca_wal::{
    replay_file, FileWal, FramedRecord, RunConfig, RunOrigin, Wal, WalRecord,
    DEFAULT_MAX_READ_BYTES,
};

const WARMUPS: usize = 5;
const SAMPLES: usize = 100;
const SHAPES: [Shape; 3] = [
    Shape::new("p50", 2_251),
    Shape::new("p95", 103_796),
    Shape::new("p99", 265_540),
];

static TRACK_ALLOCATIONS: AtomicBool = AtomicBool::new(false);
static ALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

struct CountingAllocator;

fn record_allocation(bytes: usize) {
    if TRACK_ALLOCATIONS.load(Ordering::Relaxed) {
        ALLOCATED_BYTES.fetch_add(bytes, Ordering::Relaxed);
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
    }
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_allocation(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if new_size > layout.size() {
            record_allocation(new_size - layout.size());
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

#[derive(Clone, Copy)]
struct Shape {
    label: &'static str,
    message_bytes: usize,
}

impl Shape {
    const fn new(label: &'static str, message_bytes: usize) -> Self {
        Self {
            label,
            message_bytes,
        }
    }
}

struct Fixture {
    path: PathBuf,
    frames: Vec<FramedRecord>,
    seed: ProjectionSeed,
}

impl Fixture {
    fn new(shape: Shape) -> Self {
        let path = std::env::temp_dir().join(format!(
            "broca-projection-tail-alloc-{}-{}.wal",
            std::process::id(),
            shape.label
        ));
        let _ = std::fs::remove_file(&path);
        let session = BindIdentity::new("/tmp/project", "projection-bench", shape.label);
        let mut wal = FileWal::open(&path).expect("open synthetic benchmark WAL");
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
                run_id: RunId(format!("run-{}", shape.label)),
                session,
                config: Box::new(RunConfig {
                    model: ModelHandle::new("stub", "projection-bench"),
                    tools: Vec::new().into(),
                    tool_choice: Default::default(),
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
                }),
                input: vec![Message::new(
                    Role::User,
                    vec![ContentBlock::Text {
                        text: "u".repeat(shape.message_bytes),
                    }],
                )]
                .into(),
                origin: RunOrigin::Fresh,
                submission_id: None,
            },
            false,
        )
        .expect("append synthetic run start");
        wal.append(1, &WalRecord::StepStarted { step_id: StepId(1) }, false)
            .expect("append synthetic step start");
        wal.append(
            1,
            &WalRecord::ModelStepFinished {
                hooks: None,
                step_id: StepId(1),
                assistant_message: (AssistantMessage {
                    message_id: MessageId(format!("message-{}", shape.label)),
                    content: vec![ContentBlock::Text {
                        text: "a".repeat(shape.message_bytes),
                    }],
                })
                .into(),
                usage: Usage::default(),
                finish_reason: None,
                retries_used: 0,
                service_tier_used: None,
                speed_used: None,
            },
            true,
        )
        .expect("append synthetic model step");
        drop(wal);

        let frames = replay_file(&path, DEFAULT_MAX_READ_BYTES)
            .expect("decode synthetic fixture through production WAL framing");
        assert_eq!(frames.len(), 3, "fixture has one projected model step");
        Self {
            path,
            frames,
            seed: ProjectionSeed::empty(),
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn start_tracking() {
    assert!(!TRACK_ALLOCATIONS.load(Ordering::SeqCst));
    ALLOCATED_BYTES.store(0, Ordering::SeqCst);
    ALLOCATIONS.store(0, Ordering::SeqCst);
    TRACK_ALLOCATIONS.store(true, Ordering::SeqCst);
}

fn stop_tracking() -> (usize, usize) {
    TRACK_ALLOCATIONS.store(false, Ordering::SeqCst);
    (
        ALLOCATED_BYTES.load(Ordering::SeqCst),
        ALLOCATIONS.load(Ordering::SeqCst),
    )
}

fn measure(fixture: &Fixture) -> (u128, usize, usize) {
    let mut best_ns = u128::MAX;
    let mut expected_allocation = None;
    for iteration in 0..WARMUPS + SAMPLES {
        if iteration < WARMUPS {
            black_box(
                replay_projection_tail(black_box(&fixture.seed), black_box(&fixture.frames))
                    .expect("replay synthetic projection tail"),
            );
            continue;
        }

        start_tracking();
        let started = Instant::now();
        let delta = replay_projection_tail(black_box(&fixture.seed), black_box(&fixture.frames))
            .expect("replay synthetic projection tail");
        let elapsed = started.elapsed().as_nanos();
        let allocation = stop_tracking();
        black_box(delta);
        best_ns = best_ns.min(elapsed);
        match expected_allocation {
            Some(expected) => assert_eq!(allocation, expected, "allocation count drift"),
            None => expected_allocation = Some(allocation),
        }
    }
    let (allocated_bytes, allocations) = expected_allocation.expect("measured samples");
    (best_ns, allocated_bytes, allocations)
}

fn main() {
    println!("shape\tbest_ns\tallocated_bytes\tallocations");
    for shape in SHAPES {
        let fixture = Fixture::new(shape);
        let (best_ns, allocated_bytes, allocations) = measure(&fixture);
        println!(
            "{}\t{}\t{}\t{}",
            shape.label, best_ns, allocated_bytes, allocations
        );
    }
}
