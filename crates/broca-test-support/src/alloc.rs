//! A counting allocator for memory budget tests.
//!
//! A test binary installs [`CountingAlloc`] as its global allocator under `cfg(test)` only:
//!
//! ```ignore
//! #[cfg(test)]
//! #[global_allocator]
//! static ALLOCATOR: broca_test_support::alloc::CountingAlloc =
//!     broca_test_support::alloc::CountingAlloc;
//! ```
//!
//! Counting is per thread, and off until a test switches it on with [`Counting::start`]: only
//! the threads in the measuring test's [`Group`] add to its numbers, so they are exact whether
//! tests run one per process (nextest) or as threads of one process (`cargo test`). The test's
//! own thread is in its group; so are the threads of a runtime the test builds with
//! [`Group::joiner`] as its `on_thread_start`, which is how a current-thread runtime's blocking
//! pool counts too. Work on any other thread, such as a shared runtime's, is not counted.
//!
//! What a measurement reports is what happened in its group while it was on: a block allocated
//! before it started and freed during it lowers `held`, as it should for "how much more does the
//! group hold now".
//!
//! This module is only ever a dev-dependency: it must never be the allocator of a shipped
//! binary.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicU64, Ordering::Relaxed};

/// The size range of one parsed JSON object's block, as `serde_json::Map` allocates it: a map
/// node of about 640 bytes on a 64-bit target.
pub const JSON_OBJECT_BLOCK: std::ops::RangeInclusive<usize> = 600..=700;

/// The system allocator, counting what the threads of a [`Group`] allocate and free while its
/// counting is switched on.
pub struct CountingAlloc;

/// The counters one measurement adds to, shared by the threads that joined it.
#[derive(Debug, Default)]
pub struct Group {
    on: AtomicBool,
    live: AtomicIsize,
    peak: AtomicIsize,
    allocated: AtomicU64,
    allocations: AtomicU64,
    object_blocks: AtomicIsize,
}

thread_local! {
    // A `const` initialiser without a destructor: reading it never allocates, so the allocator
    // can use it.
    static MEMBER: Cell<Option<&'static Group>> = const { Cell::new(None) };
}

/// The calling thread's group while its counting is on.
fn counting() -> Option<&'static Group> {
    MEMBER
        .try_with(Cell::get)
        .ok()
        .flatten()
        .filter(|group| group.on.load(Relaxed))
}

fn grew(bytes: usize) {
    let Some(group) = counting() else {
        return;
    };
    let now = group.live.fetch_add(bytes as isize, Relaxed) + bytes as isize;
    group.peak.fetch_max(now, Relaxed);
    group.allocated.fetch_add(bytes as u64, Relaxed);
    group.allocations.fetch_add(1, Relaxed);
    if JSON_OBJECT_BLOCK.contains(&bytes) {
        group.object_blocks.fetch_add(1, Relaxed);
    }
}

fn shrank(bytes: usize) {
    let Some(group) = counting() else {
        return;
    };
    group.live.fetch_sub(bytes as isize, Relaxed);
    if JSON_OBJECT_BLOCK.contains(&bytes) {
        group.object_blocks.fetch_sub(1, Relaxed);
    }
}

// SAFETY: every call is forwarded to `System` with the same arguments; the counting touches only
// a thread-local cell and atomics, and never allocates.
#[allow(unsafe_code)]
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            grew(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            grew(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        shrank(layout.size());
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new = unsafe { System.realloc(ptr, layout, new_size) };
        if !new.is_null() {
            // A growing vector's realloc is a new block as far as the budget is concerned: it
            // counts as one allocation of its new size.
            shrank(layout.size());
            grew(new_size);
        }
        new
    }
}

impl Group {
    /// A new group, which no thread is in yet. A group lives as long as the process: a test makes
    /// one or a few.
    pub fn new() -> &'static Group {
        Box::leak(Box::default())
    }

    /// Puts the calling thread in this group: what it allocates and frees while the group's
    /// counting is on counts here. A thread is in one group at a time.
    pub fn join(&'static self) {
        MEMBER.with(|member| member.set(Some(self)));
    }

    /// [`Group::join`] as a closure, for the `on_thread_start` of a runtime the test builds, so
    /// the threads it starts (a current-thread runtime's blocking pool) count with the test.
    pub fn joiner(&'static self) -> impl Fn() + Send + Sync + 'static {
        move || self.join()
    }

    /// Puts the calling thread in this group and switches the group's counting on, measuring
    /// from here.
    pub fn start(&'static self) -> Counting {
        self.join();
        let was_on = self.on.swap(true, Relaxed);
        let live = self.live.load(Relaxed);
        let outer_peak = self.peak.swap(live, Relaxed);
        Counting {
            group: self,
            was_on,
            live,
            allocated: self.allocated.load(Relaxed),
            allocations: self.allocations.load(Relaxed),
            object_blocks: self.object_blocks.load(Relaxed),
            outer_peak,
        }
    }
}

/// What a group did while a [`Counting`] was on.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counts {
    /// Bytes allocated and not freed: what the group holds now beyond what it held at the start.
    /// Negative when it freed more than it allocated.
    pub held: isize,
    /// The most `held` has been at once.
    pub peak: isize,
    /// Bytes allocated in total, freed or not. A realloc counts its new size.
    pub allocated: u64,
    /// Allocations made, reallocs included.
    pub allocations: u64,
    /// Blocks of [`JSON_OBJECT_BLOCK`] bytes allocated and not freed: about one per parsed JSON
    /// object held.
    pub object_blocks: isize,
}

/// Counting switched on for a group until dropped. Nested countings each see what happened
/// since they started; dropping an inner one leaves the outer one's numbers whole.
#[must_use = "counting stops when this is dropped"]
pub struct Counting {
    group: &'static Group,
    was_on: bool,
    live: isize,
    allocated: u64,
    allocations: u64,
    object_blocks: isize,
    outer_peak: isize,
}

impl Counting {
    /// Switches counting on for the calling thread's group, or for a new group of this thread
    /// alone when it is in none, and starts measuring from here.
    pub fn start() -> Self {
        match MEMBER.with(Cell::get) {
            Some(group) => group.start(),
            None => Group::new().start(),
        }
    }

    /// The group this counting counts in, for a runtime whose threads should join it.
    pub fn group(&self) -> &'static Group {
        self.group
    }

    /// What the group did since this counting started.
    pub fn read(&self) -> Counts {
        let group = self.group;
        Counts {
            held: group.live.load(Relaxed) - self.live,
            peak: group.peak.load(Relaxed) - self.live,
            allocated: group.allocated.load(Relaxed) - self.allocated,
            allocations: group.allocations.load(Relaxed) - self.allocations,
            object_blocks: group.object_blocks.load(Relaxed) - self.object_blocks,
        }
    }
}

impl Drop for Counting {
    fn drop(&mut self) {
        // The outer measurement's peak is the larger of what it saw before this one started
        // and what this one saw.
        self.group.peak.fetch_max(self.outer_peak, Relaxed);
        self.group.on.store(self.was_on, Relaxed);
    }
}

/// Runs `work` on this thread with counting on and returns what it returned with what it did.
/// `held` counts what the returned value (and anything else `work` left behind) holds.
pub fn measure<T>(work: impl FnOnce() -> T) -> (T, Counts) {
    let counting = Counting::start();
    let out = work();
    let counts = counting.read();
    (out, counts)
}

/// Whether [`CountingAlloc`] is this binary's global allocator. A budget test asserts it first,
/// so a crate that forgot to install it fails instead of measuring zero.
pub fn installed() -> bool {
    let (boxed, counts) = measure(|| std::hint::black_box(Box::new([0u8; 64])));
    drop(boxed);
    counts.allocations > 0
}

/// A memory budget: what each holder measured against its limit, shown in multiples of a unit
/// (such as `W`, the window's messages as compact JSON). [`Budget::check`] fails naming every
/// holder over its limit, with the whole table.
#[derive(Debug)]
pub struct Budget {
    what: String,
    unit_name: &'static str,
    unit: f64,
    rows: Vec<(String, u64, u64)>,
}

impl Budget {
    /// A budget for `what`, shown in multiples of `unit` bytes, named `unit_name`.
    pub fn new(what: impl Into<String>, unit_name: &'static str, unit: usize) -> Self {
        Self {
            what: what.into(),
            unit_name,
            unit: unit.max(1) as f64,
            rows: Vec::new(),
        }
    }

    /// `holder` measured `measured` bytes (or allocations) and may measure up to `limit`.
    pub fn holder(mut self, holder: impl Into<String>, measured: u64, limit: u64) -> Self {
        self.rows.push((holder.into(), measured, limit));
        self
    }

    /// The table of every holder: measured, limit, both as multiples of the unit.
    pub fn table(&self) -> String {
        let mut out = String::new();
        for (holder, measured, limit) in &self.rows {
            out.push_str(&format!(
                "\n  {holder:<28} measured {measured:>12} ({:>6.3} x {unit}), limit {limit:>12} ({:>6.3} x {unit}){}",
                *measured as f64 / self.unit,
                *limit as f64 / self.unit,
                if measured > limit { "  OVER" } else { "" },
                unit = self.unit_name,
            ));
        }
        out
    }

    /// Fails when any holder measured more than its limit, naming each such holder.
    pub fn check(&self) {
        let over: Vec<&str> = self
            .rows
            .iter()
            .filter(|(_, measured, limit)| measured > limit)
            .map(|(holder, ..)| holder.as_str())
            .collect();
        assert!(
            over.is_empty(),
            "{}: over budget: {} ({} is {} bytes){}",
            self.what,
            over.join(", "),
            self.unit_name,
            self.unit,
            self.table()
        );
    }
}

/// `milli` thousandths of `unit`, for limits written as multiples of `W`.
pub fn thousandths(unit: usize, milli: u64) -> u64 {
    (unit as u64).saturating_mul(milli) / 1_000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_test_binary_counts_with_it() {
        assert!(installed());
    }

    #[test]
    fn a_measurement_counts_what_its_thread_holds_and_allocated() {
        let (kept, counts) = measure(|| {
            let kept = std::hint::black_box(vec![0u8; 10_000]);
            drop(std::hint::black_box(vec![0u8; 5_000]));
            kept
        });
        assert_eq!(counts.held, 10_000);
        assert_eq!(counts.peak, 15_000);
        assert_eq!(counts.allocated, 15_000);
        assert_eq!(counts.allocations, 2);
        drop(kept);
    }

    #[test]
    fn parsed_json_objects_are_counted_as_blocks() {
        let (value, counts) = measure(|| object_sized_blocks(3));
        assert_eq!(counts.object_blocks, 3);
        drop(value);
    }

    /// Blocks the size of one parsed JSON object, standing in for `serde_json` objects without
    /// depending on it. Each box is one such block on the heap, which is the point.
    #[allow(clippy::vec_box)]
    fn object_sized_blocks(n: usize) -> Vec<Box<[u8; 640]>> {
        (0..n)
            .map(|_| std::hint::black_box(Box::new([0u8; 640])))
            .collect()
    }

    #[test]
    fn a_thread_that_joins_the_group_counts_with_it() {
        let counting = Counting::start();
        let group = counting.group();
        std::thread::spawn(move || {
            group.join();
            drop(std::hint::black_box(vec![0u8; 100_000]));
        })
        .join()
        .unwrap();
        assert!(counting.read().allocated >= 100_000);
    }

    #[test]
    fn another_thread_adds_nothing() {
        let counting = Counting::start();
        std::thread::spawn(|| drop(std::hint::black_box(vec![0u8; 100_000])))
            .join()
            .unwrap();
        let counts = counting.read();
        // The spawn itself allocates a little on this thread; the other thread's vector isn't
        // counted.
        assert!(counts.allocated < 100_000, "{counts:?}");
    }

    #[test]
    fn an_inner_measurement_leaves_the_outer_one_whole() {
        let outer = Counting::start();
        drop(std::hint::black_box(vec![0u8; 8_000]));
        let (kept, inner) = measure(|| std::hint::black_box(vec![0u8; 1_000]));
        assert_eq!(inner.peak, 1_000);
        let counts = outer.read();
        assert_eq!(counts.peak, 8_000);
        assert_eq!(counts.held, 1_000);
        drop(outer);
        drop(kept);
        // Off again: an allocation now changes nothing.
        let group = MEMBER.with(Cell::get).unwrap();
        let before = group.live.load(Relaxed);
        let unseen = std::hint::black_box(vec![0u8; 2_000]);
        assert_eq!(group.live.load(Relaxed), before);
        drop(unseen);
    }
}
