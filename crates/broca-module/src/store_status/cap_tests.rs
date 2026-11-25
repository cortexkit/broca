//! The settled run facts' byte cap, measured with the counting allocator.

use broca_core::RunIndexState;
use broca_test_support::alloc::{installed, measure};

use super::{fresh_state, RunFacts, SettledRuns};

/// The facts of a completed run whose final message holds `text`.
fn completed(n: u64, text: String) -> RunFacts {
    RunFacts {
        admission_error_detail: None,
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

/// The facts of a run that ended in error, whose error message and undelivered steer ids hold
/// `text`.
fn errored(text: String) -> RunFacts {
    let mut state = fresh_state(RunIndexState::Error);
    state.error = Some(
        serde_json::from_value(serde_json::json!({"class": "permanent", "message": text})).unwrap(),
    );
    state.undelivered_steers = Some(vec![text]);
    RunFacts {
        admission_error_detail: None,
        wait: Default::default(),
        state,
        interrupt_reason: None,
        wake_directive: false,
        final_message: None,
    }
}

/// Settled facts of runs that ended in error, whose detail lives in their state rather than a
/// final message, hold no more than the byte cap either: the cache counts the state's detail.
#[test]
fn errored_run_facts_hold_no_more_than_their_byte_cap() {
    assert!(
        installed(),
        "the crate's tests count with the counting allocator"
    );
    const CAP: usize = 1024 * 1024;
    const MESSAGE: usize = 32 * 1024;
    const RUNS: usize = 80;
    const _: () = assert!(RUNS * 2 * MESSAGE > 4 * CAP);
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
        settled.put(key(n), errored("x".repeat(MESSAGE)));
    }
    let memory = settled.memory();
    let ((), counts) = measure(|| drop(settled));
    let held = counts.held.unsigned_abs();
    eprintln!(
        "filled {}, held {held}, counted {:?}, entries {}",
        RUNS * 2 * MESSAGE,
        memory.counted_bytes,
        memory.entries
    );
    assert!(
        held <= CAP,
        "the settled facts hold {held} bytes, over their cap of {CAP}"
    );
    assert!(memory.entries > 1 && (memory.entries as usize) < RUNS);
}

/// A run's admission is kept under its session's incarnation and history version, so a revert
/// or a session created again under the same id asks again, and only the newest
/// [`SettledRuns::CAPACITY`] runs' admissions are kept.
#[test]
fn kept_admissions_answer_their_own_key_and_drop_the_oldest() {
    let settled = SettledRuns::new(2);
    let answer = |run: &str| broca_wire::RunAdmissionResponse {
        run_id: run.to_owned(),
        session_class: Some("head".into()),
        started_at_ms: 1,
        storage: broca_wire::RunAdmissionStorage::Store,
        fetches: Vec::new(),
        scope: None,
    };
    let key = |version: u64, run: &str| {
        (
            "b0d9e7b0d9e7b0d9".to_owned(),
            [1; 16],
            version,
            run.to_owned(),
        )
    };
    settled.keep_admission(key(0, "a"), answer("a"));
    assert_eq!(
        settled
            .admission(&key(0, "a"))
            .map(|kept| kept.run_id.clone()),
        Some("a".to_owned())
    );
    assert!(
        settled.admission(&key(1, "a")).is_none(),
        "a moved history version asks again"
    );
    settled.keep_admission(key(0, "b"), answer("b"));
    settled.keep_admission(key(0, "c"), answer("c"));
    assert!(
        settled.admission(&key(0, "a")).is_none(),
        "the oldest is dropped"
    );
    assert!(settled.admission(&key(0, "c")).is_some());
}
