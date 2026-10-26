use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
};

use broca_protocol::{MediaKind, MediaRef, ToolOutput, ToolOutputPart, ToolResult, Usage};
use broca_wal::{Wal, WalError, WalRecord};
use serde_json::json;

use broca_core::{compaction::CapturedMedia, CompactionCheckpoint, WalHandle};

// Unlike retained-size probes, count allocation calls even when the allocation is freed
// before the measured operation returns. Track only this thread, not the writer or other tests.
thread_local! {
    static ALLOCATIONS: Cell<Option<usize>> = const { Cell::new(None) };
}

fn allocated() {
    let _ = ALLOCATIONS.try_with(|count| {
        if let Some(value) = count.get() {
            count.set(Some(value + 1));
        }
    });
}

struct TrackingAllocator;

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        allocated();
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        allocated();
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        allocated();
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator;

fn allocations(operation: impl FnOnce()) -> usize {
    ALLOCATIONS.with(|count| assert!(count.replace(Some(0)).is_none()));
    operation();
    ALLOCATIONS.with(|count| count.replace(None).unwrap())
}

#[derive(Default)]
struct MemoryWal(u64);

impl Wal for MemoryWal {
    fn append(&mut self, _: u64, _: &WalRecord, _: bool) -> Result<u64, WalError> {
        self.0 += 1;
        Ok(self.0)
    }

    fn sync(&self) -> Result<(), WalError> {
        Ok(())
    }

    fn replay(&self) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
        Ok(Vec::new())
    }

    fn tail_after_seq(&self, _: u64) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
        Ok(Vec::new())
    }

    fn last_seq(&self) -> u64 {
        self.0
    }
}

fn result(output: ToolOutput) -> WalRecord {
    serde_json::from_value(json!({
        "type": "tool_result", "batch_id": "b", "outcome": "completed",
        "result": ToolResult {
            tool_call_id: broca_protocol::ToolCallId("call".into()),
            output,
            is_error: false,
        },
    }))
    .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn media_free_records_do_not_allocate_capture_indexes() {
    // A net-live counter would miss the empty index being allocated and immediately freed.
    assert!(allocations(|| drop(std::hint::black_box(CapturedMedia::default()))) > 0);
    let records: [(&str, WalRecord); 4] = [
        (
            "RunStarted",
            serde_json::from_value(json!({
                "type": "run_started", "run_id": "run",
                "session": {"project_root":"/project", "harness":"test", "session":"captures"},
                "config": {
                    "model": {"provider_module_id":"stub", "model_id":"model"},
                    "tools": [], "tool_choice": {"type":"auto"}, "generation": {},
                    "response_format": {"type":"text"}, "context_epoch":0, "renderer_version":1,
                },
                "input": [{"role":"user", "content":[{"type":"text", "text":"hello"}]}],
            }))
            .unwrap(),
        ),
        (
            "ToolResult",
            result(ToolOutput::Text {
                text: "done".into(),
            }),
        ),
        (
            "marker-only Parts",
            result(ToolOutput::Parts {
                parts: vec![ToolOutputPart::Text {
                    text: "[image stripped]".into(),
                }],
            }),
        ),
        (
            "ModelStepFinished",
            serde_json::from_value(json!({
                "type":"model_step_finished", "step_id":1, "usage":Usage::default(),
                "assistant_message":{"message_id":"m", "content":[{"type":"text", "text":"done"}]},
            }))
            .unwrap(),
        ),
    ];
    let writer = WalHandle::spawn(Box::<MemoryWal>::default(), 1).unwrap();
    for (kind, record) in &records {
        let mut checkpoint = CompactionCheckpoint::None;
        assert_eq!(
            allocations(|| checkpoint.fold(record)),
            0,
            "{kind}: checkpoint captures allocated"
        );
        assert_eq!(checkpoint, CompactionCheckpoint::None);
        // Measure the exact collection function used by enqueue_record_with, excluding the
        // command's Box and channel storage, which are ordinary WAL costs rather than captures.
        // Only this writer probe needs a debug seam; checkpoint counts run in every profile.
        #[cfg(debug_assertions)]
        assert_eq!(
            allocations(|| writer.capture_for_append_for_test(1, record)),
            0,
            "{kind}: append captures allocated"
        );
    }

    let reference = MediaRef {
        kind: MediaKind::Image,
        mime: "image/png".into(),
        byte_size: 68,
        sha256: "a".repeat(64),
        width: Some(1),
        height: Some(1),
    };
    let media = result(ToolOutput::Parts {
        parts: vec![ToolOutputPart::Media {
            media: reference.clone(),
        }],
    });
    let mut checkpoint = CompactionCheckpoint::None;
    checkpoint.fold(&media);
    let CompactionCheckpoint::Captures(captured) = checkpoint else {
        panic!("media-bearing result must create a capture index");
    };
    assert_eq!(
        serde_json::to_value(captured).unwrap()[&reference.sha256],
        json!([reference])
    );
    // The real append path still collects media-bearing results and waits for durability.
    writer.record(media).into_result().unwrap();
    writer.sync().await.unwrap();
    #[cfg(debug_assertions)]
    assert!(writer.has_pending_media_capture_for_test(&reference));
    writer.shutdown().await.unwrap();
}
