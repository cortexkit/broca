#![cfg(feature = "test-clock")]

use broca_core::{lease::WalLeaseDomain, FileWalStore, ShutdownSeal, WalHandle, WalStore};
use broca_module::{warm_setup_for_allocation_test, WarmBase, WarmJob};
use broca_protocol::{BindIdentity, CallOptions, RunId, StepId};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    sync::Arc,
};

thread_local! {
    static ACTIVE: Cell<bool> = const { Cell::new(false) };
    static CALLS: Cell<usize> = const { Cell::new(0) };
}

struct CountingAllocator;
fn note() {
    let _ = ACTIVE.try_with(|active| {
        if active.get() {
            CALLS.with(|calls| calls.set(calls.get() + 1));
        }
    });
}
// The allocator forwards unchanged; the counter includes temporary allocations freed in setup.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note();
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note();
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        note();
        unsafe { System.realloc(ptr, layout, size) }
    }
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn count<T>(work: impl FnOnce() -> T) -> (T, usize) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            ACTIVE.with(|active| active.set(false));
        }
    }
    CALLS.with(|calls| calls.set(0));
    ACTIVE.with(|active| active.set(true));
    let reset = Reset;
    let value = work();
    drop(reset);
    (value, CALLS.with(Cell::get))
}

#[tokio::test]
async fn legacy_manual_and_automatic_hook_setup_allocate_nothing() {
    for overhaul in [false, true] {
        let root = broca_test_support::scratch_root().join(format!(
            "warm-setup-{}",
            broca_core::attempt_id::mint_attempt_id().unwrap().as_str()
        ));
        let wal_config = if overhaul {
            broca_wal::WalConfig::OVERHAUL
        } else {
            broca_wal::WalConfig::default()
        };
        let store = Arc::new(FileWalStore::with_config(root.join("wal"), wal_config));
        let key = BindIdentity::new("/p", "runner", "warm-setup");
        let tenure = WalLeaseDomain::new(root.join("leases"))
            .acquire(&key)
            .unwrap();
        let writer = Arc::new(
            WalHandle::open_and_spawn_owned(store.clone(), key.clone(), tenure, None)
                .await
                .unwrap(),
        );
        let broca_wal::WalRecord::RunStarted { config, .. } = serde_json::from_str(include_str!(
            "../../broca-wal/tests/golden/run_started.keep_warm.json"
        ))
        .unwrap() else {
            unreachable!()
        };
        let job = WarmJob {
            session: key.clone(),
            config: *config.clone(),
            attempt_hook: None,
            base: WarmBase::Recorded(Box::new(broca_core::RecordedRequest {
                run_id: RunId("r".into()),
                step_id: StepId(1),
                config: *config,
                opts: CallOptions::new(vec![]),
                request_body_sha256: "base".into(),
                route: None,
            })),
        };
        let run = RunId("r".into());
        let seal = ShutdownSeal::new();
        let before = store.read_records(&key).unwrap().len();
        for automatic in [false, true] {
            let (hook_installed, allocations) =
                count(|| warm_setup_for_allocation_test(&writer, &seal, &job, &run, automatic));
            assert_eq!(hook_installed, overhaul);
            if overhaul {
                assert!(
                    allocations > 0,
                    "the positive control must observe journal and seal/trigger construction"
                );
            } else {
                assert_eq!(allocations, 0, "legacy automatic={automatic}: neither the trigger string nor idle seal may be constructed");
            }
        }
        assert_eq!(
            store.read_records(&key).unwrap().len(),
            before,
            "setup alone grants no send permission"
        );
        Arc::try_unwrap(writer)
            .ok()
            .unwrap()
            .shutdown()
            .await
            .unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}
