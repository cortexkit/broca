//! The storage port must not box a future for each publisher poll.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::future::Future;
use std::task::{Context, Poll, Waker};

use broca_store::bus_events::{
    drain_bus_event, BusPublisher, DrainResult, Publication, RunEventStore,
};
use broca_store::SqliteStore;
use cortexkit_bus_trait::PublishReceipt;

thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

struct CountAllocations;

fn allocated() {
    if TRACK.try_with(Cell::get).unwrap_or(false) {
        let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
    }
}

// The wrapper delegates every allocation and deallocation to the same system allocator.
unsafe impl GlobalAlloc for CountAllocations {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        allocated();
        // SAFETY: the caller's layout is forwarded unchanged to System.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        allocated();
        // SAFETY: the caller's layout is forwarded unchanged to System.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        allocated();
        // SAFETY: ptr came from System, and its layout and requested size are unchanged.
        unsafe { System.realloc(ptr, layout, size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: ptr came from System with the supplied layout.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountAllocations = CountAllocations;

struct NeverPublish;

#[async_trait::async_trait]
impl BusPublisher for NeverPublish {
    async fn publish(&self, _: &Publication) -> Result<PublishReceipt, String> {
        panic!("an empty outbox must not publish")
    }
}

#[test]
fn legacy_port_drain_future_construction_is_allocation_free() {
    let store = SqliteStore::open_in_memory().unwrap();
    let port: &dyn RunEventStore = &store;
    // Initialize thread-local bookkeeping before counting the production call shape.
    ALLOCATIONS.with(|count| count.set(0));
    TRACK.with(|active| active.set(true));
    let future = std::hint::black_box(drain_bus_event(port, &NeverPublish, 1));
    TRACK.with(|active| active.set(false));
    assert_eq!(
        ALLOCATIONS.with(Cell::get),
        0,
        "constructing an empty drain poll allocated a boxed future"
    );

    let mut future = std::pin::pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    assert!(matches!(
        future.as_mut().poll(&mut context),
        Poll::Ready(Ok(DrainResult::Empty))
    ));
}
