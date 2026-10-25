// UNSAFE IS DELIBERATE AND SCOPED TO THIS BENCHMARK. `GlobalAlloc` is an unsafe trait, so a
// counting allocator cannot be written safely; there is no alternative shape. Declared with an
// explicit allow rather than inherited by omission, because 20 of this workspace's 24 crate
// roots carry `forbid(unsafe_code)` and a `src/bin/` target is its OWN crate root that does not
// inherit the sibling `main.rs` attribute. Without this line the exemption would be silent --
// indistinguishable from nobody having thought about it.
//
// This binary is a measurement tool and is never shipped in the daemon.
#![allow(unsafe_code)]

//! Allocation benchmark for borrowed versus owned `session.read` page assembly.
//!
//! The fixtures use synthetic text but preserve the row counts and approximate payload sizes of
//! measured production page envelopes. Run with:
//! `cargo run --release -p broca-bench --bin read_page_alloc`.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    hint::black_box,
    path::PathBuf,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    time::Instant,
};

use broca_core::{read_session_page, read_session_page_owned, StorePageRow, StorePageSnapshot};
use broca_protocol::{caller::RunFinishReason, ContentBlock, Message, Role, Usage};
use broca_wal::{replay_file, FileWal, FramedRecord, Wal, WalRecord, DEFAULT_MAX_READ_BYTES};

const WARMUPS: usize = 5;
const SAMPLES: usize = 100;
const SHAPES: [Shape; 4] = [
    Shape::new("p50", 3, 92_000),
    Shape::new("p95", 3, 315_000),
    Shape::new("p99", 29, 453_000),
    Shape::new("max", 18, 2_400_000),
];

static TRACK_ALLOCATIONS: AtomicBool = AtomicBool::new(false);
static ALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

struct CountingAllocator;

struct TerminalTailFixture {
    path: PathBuf,
    frames: Vec<FramedRecord>,
}

impl TerminalTailFixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "broca-read-page-tail-alloc-{}.wal",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let mut wal = FileWal::open(&path).expect("open synthetic benchmark WAL");
        wal.append(
            1,
            &WalRecord::RunFinished {
                interrupt_reason: None,
                ts_ms: None,
                reason: RunFinishReason::Completed,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            },
            true,
        )
        .expect("append synthetic terminal record");
        drop(wal);
        let frames = replay_file(&path, DEFAULT_MAX_READ_BYTES)
            .expect("decode synthetic terminal WAL through read-only replay");
        assert_eq!(frames.len(), 1, "fixture has one decoded tail record");
        Self { path, frames }
    }
}

impl Drop for TerminalTailFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

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
    rows: usize,
    text_bytes: usize,
}

impl Shape {
    const fn new(label: &'static str, rows: usize, text_bytes: usize) -> Self {
        Self {
            label,
            rows,
            text_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Measurement {
    best_ns: u128,
    allocated_bytes: usize,
    allocations: usize,
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

fn snapshot(shape: Shape) -> StorePageSnapshot {
    let bytes_per_row = shape.text_bytes / shape.rows;
    let remainder = shape.text_bytes % shape.rows;
    let rows = (0..shape.rows)
        .map(|index| {
            let ordinal = index as u64;
            let text_len = bytes_per_row + usize::from(index < remainder);
            let mut message = Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "x".repeat(text_len),
                }],
            );
            message.ordinal = Some(ordinal);
            StorePageRow {
                written_at: None,
                ordinal,
                message,
                run: None,
            }
        })
        .collect();
    StorePageSnapshot {
        prefix_len: shape.rows as u64,
        completed_steps: 1,
        rows,
        ..StorePageSnapshot::empty()
    }
}

fn measure_borrowed(fixture: &StorePageSnapshot, limit: u64) -> Measurement {
    let mut best_ns = u128::MAX;
    let mut allocation_result = None;
    for iteration in 0..WARMUPS + SAMPLES {
        let input = fixture.clone();
        if iteration < WARMUPS {
            black_box(
                read_session_page(black_box(&input), &[], Some(0), Some(limit))
                    .expect("borrowed page assembly"),
            );
            continue;
        }

        start_tracking();
        let started = Instant::now();
        let page = read_session_page(black_box(&input), &[], Some(0), Some(limit))
            .expect("borrowed page assembly");
        let elapsed = started.elapsed().as_nanos();
        let allocation = stop_tracking();
        black_box(page);
        best_ns = best_ns.min(elapsed);
        match allocation_result {
            Some(expected) => assert_eq!(allocation, expected, "borrowed allocation drift"),
            None => allocation_result = Some(allocation),
        }
    }
    let (allocated_bytes, allocations) = allocation_result.expect("measured borrowed samples");
    Measurement {
        best_ns,
        allocated_bytes,
        allocations,
    }
}

fn measure_terminal_tail(fixture: &TerminalTailFixture) -> Measurement {
    let mut best_ns = u128::MAX;
    let mut allocation_result = None;
    for iteration in 0..WARMUPS + SAMPLES {
        let snapshot = StorePageSnapshot {
            completed_steps: 1,
            ..StorePageSnapshot::empty()
        };
        if iteration < WARMUPS {
            black_box(
                read_session_page_owned(snapshot, black_box(&fixture.frames), Some(0), Some(1))
                    .expect("terminal-tail page assembly"),
            );
            continue;
        }

        start_tracking();
        let started = Instant::now();
        let page = read_session_page_owned(
            black_box(snapshot),
            black_box(&fixture.frames),
            Some(0),
            Some(1),
        )
        .expect("terminal-tail page assembly");
        let elapsed = started.elapsed().as_nanos();
        let allocation = stop_tracking();
        black_box(page);
        best_ns = best_ns.min(elapsed);
        match allocation_result {
            Some(expected) => assert_eq!(allocation, expected, "terminal-tail allocation drift"),
            None => allocation_result = Some(allocation),
        }
    }
    let (allocated_bytes, allocations) = allocation_result.expect("measured terminal-tail samples");
    Measurement {
        best_ns,
        allocated_bytes,
        allocations,
    }
}

fn measure_owned(fixture: &StorePageSnapshot, limit: u64) -> Measurement {
    let mut best_ns = u128::MAX;
    let mut allocation_result = None;
    for iteration in 0..WARMUPS + SAMPLES {
        let input = fixture.clone();
        if iteration < WARMUPS {
            black_box(
                read_session_page_owned(black_box(input), &[], Some(0), Some(limit))
                    .expect("owned page assembly"),
            );
            continue;
        }

        start_tracking();
        let started = Instant::now();
        let page = read_session_page_owned(black_box(input), &[], Some(0), Some(limit))
            .expect("owned page assembly");
        let elapsed = started.elapsed().as_nanos();
        let allocation = stop_tracking();
        black_box(page);
        best_ns = best_ns.min(elapsed);
        match allocation_result {
            Some(expected) => assert_eq!(allocation, expected, "owned allocation drift"),
            None => allocation_result = Some(allocation),
        }
    }
    let (allocated_bytes, allocations) = allocation_result.expect("measured owned samples");
    Measurement {
        best_ns,
        allocated_bytes,
        allocations,
    }
}

fn main() {
    let terminal_fixture = TerminalTailFixture::new();
    let terminal = measure_terminal_tail(&terminal_fixture);
    println!(
        "terminal_tail {} ns {} bytes {} allocations",
        terminal.best_ns, terminal.allocated_bytes, terminal.allocations
    );
    assert_eq!(
        (terminal.allocated_bytes, terminal.allocations),
        (0, 0),
        "an ordinary terminal page must not allocate solely to replay its decoded WAL tail"
    );

    println!(
        "shape rows text_bytes borrowed_ns owned_ns borrowed_bytes owned_bytes removed_bytes borrowed_allocs owned_allocs"
    );
    for shape in SHAPES {
        let fixture = snapshot(shape);
        let borrowed_page = read_session_page(&fixture, &[], Some(0), Some(shape.rows as u64))
            .expect("borrowed identity page");
        let owned_page =
            read_session_page_owned(fixture.clone(), &[], Some(0), Some(shape.rows as u64))
                .expect("owned identity page");
        assert_eq!(borrowed_page, owned_page, "{} byte identity", shape.label);

        let borrowed = measure_borrowed(&fixture, shape.rows as u64);
        let owned = measure_owned(&fixture, shape.rows as u64);
        let removed = borrowed
            .allocated_bytes
            .checked_sub(owned.allocated_bytes)
            .expect("owned assembly must not allocate more bytes");
        assert!(
            removed >= shape.text_bytes * 9 / 10,
            "{} removed {removed} bytes for {} bytes of text",
            shape.label,
            shape.text_bytes
        );
        println!(
            "{} {} {} {} {} {} {} {} {} {}",
            shape.label,
            shape.rows,
            shape.text_bytes,
            borrowed.best_ns,
            owned.best_ns,
            borrowed.allocated_bytes,
            owned.allocated_bytes,
            removed,
            borrowed.allocations,
            owned.allocations,
        );
    }
}
