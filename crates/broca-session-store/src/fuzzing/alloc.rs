//! An allocator that counts what each thread holds, so a check can bound the memory one decode
//! takes. Every fuzz target installs it as the global allocator; in any other binary the counts
//! stay at zero and `bounded` checks nothing. The crate's own tests use the workspace's test
//! allocator instead.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

/// The system allocator, counting the bytes each thread has live and the most it has had live
/// since a [`Counting`] started.
pub struct CountingAlloc;

thread_local! {
    // `const` initialisers without destructors: reading them never allocates, so the allocator
    // can use them.
    static LIVE: Cell<isize> = const { Cell::new(0) };
    static PEAK: Cell<isize> = const { Cell::new(0) };
}

fn grew(bytes: usize) {
    let _ = LIVE.try_with(|live| {
        let now = live.get().saturating_add(bytes as isize);
        live.set(now);
        let _ = PEAK.try_with(|peak| peak.set(peak.get().max(now)));
    });
}

fn shrank(bytes: usize) {
    let _ = LIVE.try_with(|live| live.set(live.get().saturating_sub(bytes as isize)));
}

// SAFETY: every call is forwarded to `System` with the same arguments; the counting touches only
// thread-local cells and never allocates.
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
            grew(new_size);
            shrank(layout.size());
        }
        new
    }
}

/// Bytes this thread has live now.
fn live() -> isize {
    LIVE.with(Cell::get)
}

/// The most this thread has had live since its peak was last reset.
fn peak() -> isize {
    PEAK.with(Cell::get)
}

/// What a [`Counting`] saw.
pub struct Counts {
    /// The most this thread held at once beyond what it held when counting started.
    pub peak: isize,
}

/// One measurement of what this thread holds, the same interface the crate's tests measure
/// through (`broca_test_support::alloc::Counting`). This allocator counts always, so starting a
/// measurement only starts a new peak; dropping it raises the peak back to what an outer
/// measurement saw before, so a nested one doesn't hide it.
pub struct Counting {
    start: isize,
    outer: isize,
}

impl Counting {
    pub fn start() -> Self {
        let outer = peak();
        let start = live();
        PEAK.with(|peak| peak.set(start));
        Self { start, outer }
    }

    pub fn read(&self) -> Counts {
        Counts {
            peak: peak() - self.start,
        }
    }
}

impl Drop for Counting {
    fn drop(&mut self) {
        PEAK.with(|peak| peak.set(peak.get().max(self.outer)));
    }
}
