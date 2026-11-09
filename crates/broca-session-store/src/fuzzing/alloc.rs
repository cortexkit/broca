//! An allocator that counts what each thread holds, so a check can bound the memory one decode
//! takes. The crate's test binary and every fuzz target install it as the global allocator; in
//! any other binary the counts stay at zero and `bounded` checks nothing.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

/// The system allocator, counting the bytes each thread has live and the most it has had live
/// since the last [`reset_peak`].
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
pub fn live() -> isize {
    LIVE.with(Cell::get)
}

/// Starts a new peak at what this thread has live now.
pub fn reset_peak() {
    let now = live();
    PEAK.with(|peak| peak.set(now));
}

/// The most this thread has had live since the last [`reset_peak`].
pub fn peak() -> isize {
    PEAK.with(Cell::get)
}

/// Raises the peak to at least `to`, so a measurement nested inside another doesn't hide what
/// the outer one saw before it.
pub fn raise_peak(to: isize) {
    PEAK.with(|peak| peak.set(peak.get().max(to)));
}
