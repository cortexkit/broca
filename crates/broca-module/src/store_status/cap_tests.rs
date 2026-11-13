//! The settled run facts' byte cap, measured with the counting allocator.

use broca_core::RunIndexState;
use broca_test_support::alloc::{installed, measure};

use super::{fresh_state, RunFacts, SettledRuns};

/// The facts of a completed run whose final message holds `text`.
fn completed(n: u64, text: String) -> RunFacts {
    RunFacts {
        wait: Default::default(),
        state: fresh_state(RunIndexState::Completed),
        interrupt_reason: None,
        wake_directive: false,
        final_message: Some(Ok(broca_wire::FinalMessage {
            ordinal: n,
            mid: format!("g0.{n}"),
            text,
        })),
    }
}

/// Settled facts filled with several times their byte cap of final messages hold no more than
/// the cap, as the allocator measures them, and the cache counts what it holds.
#[test]
fn settled_run_facts_hold_no_more_than_their_byte_cap() {
    assert!(
        installed(),
        "the crate's tests count with the counting allocator"
    );
    const CAP: usize = 1024 * 1024;
    const MESSAGE: usize = 64 * 1024;
    const RUNS: usize = 80;
    const _: () = assert!(RUNS * MESSAGE > 4 * CAP);
    let settled = SettledRuns::with_bytes(SettledRuns::CAPACITY, CAP);
    let key = |n: usize| {
        (
            "b0d9e7b0d9e7b0d9".to_owned(),
            [1; 16],
            0,
            format!("run-{n}"),
        )
    };
    for n in 0..RUNS {
        settled.put(key(n), completed(n as u64, "x".repeat(MESSAGE)));
    }
    let memory = settled.memory();
    // The newest run's facts are kept, and the oldest are gone.
    assert!(settled.get(&key(RUNS - 1)).is_some());
    assert!(settled.get(&key(0)).is_none());
    // What the cache holds is what dropping it frees.
    let ((), counts) = measure(|| drop(settled));
    let held = counts.held.unsigned_abs();
    eprintln!(
        "filled {}, held {held}, counted {:?}, entries {}",
        RUNS * MESSAGE,
        memory.counted_bytes,
        memory.entries
    );
    assert!(
        held <= CAP,
        "the settled facts hold {held} bytes, over their cap of {CAP}"
    );
    assert!(memory.counted_bytes.unwrap() <= CAP as u64);
    assert!(memory.entries > 1 && (memory.entries as usize) < RUNS);
}
