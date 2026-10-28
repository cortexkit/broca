use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
};

use broca_protocol::{ContentBlock, Message, ModelHandle, TextProviderFormat, TextProviderState};
use broca_wire::SessionReadMessage;

// Count total allocated bytes, including temporary copies freed before serialization returns.
// Thread-local accounting excludes allocations made by the test harness on other threads.
thread_local! {
    static ALLOCATED: Cell<Option<usize>> = const { Cell::new(None) };
}

fn record(size: usize) {
    let _ = ALLOCATED.try_with(|total| {
        if let Some(bytes) = total.get() {
            total.set(Some(bytes + size));
        }
    });
}

struct TrackingAllocator;

// Every allocation is delegated to System with its original layout and pointer.
unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record(new_size);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator;

#[test]
fn stateful_caller_message_serializer_uses_borrowed_content() {
    let response = SessionReadMessage {
        rich_ui: None,
        written_at: None,
        ordinal: 0,
        mid: "m0".into(),
        message: Message::assistant_from(
            vec![ContentBlock::TextWithState {
                text: "x".repeat(1024 * 1024),
                provider_state: TextProviderState {
                    format: TextProviderFormat::OpenAiResponsesChatgpt,
                    data: "opaque".repeat(256 * 1024),
                },
            }],
            ModelHandle::new("openai", "gpt-test"),
        ),
        run: None,
        tool_calls: Vec::new(),
        tags: Vec::new(),
        delivered_as: None,
        digest: None,
        part_digests: None,
        described: None,
    };
    // A sink excludes output-buffer growth: only the caller conversion's extra allocations count.
    ALLOCATED.with(|total| total.set(Some(0)));
    let result = serde_json::to_writer(std::io::sink(), &response);
    let allocated = ALLOCATED.with(|total| total.replace(None).unwrap());
    result.unwrap();
    assert!(
        allocated < 64 * 1024,
        "serialization must borrow large text and opaque state, allocated {allocated} bytes"
    );
}
