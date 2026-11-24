use super::*;

fn estimated() -> u64 {
    state()
        .lock()
        .unwrap()
        .sites
        .iter()
        .map(|site| site.bytes)
        .sum()
}

/// The sites' estimates follow what the heap holds, and frees take them back out: the test
/// binary's own allocations go through the profiler. Read under nextest, where each test has the
/// process to itself.
#[test]
fn the_estimate_follows_what_the_heap_holds() {
    let before = estimated();
    let held: Vec<Vec<u8>> = (0..4096).map(|_| vec![7u8; 4096]).collect();
    let real = 4096 * 4096u64;
    let during = estimated().saturating_sub(before);
    assert!(
        during > real / 2 && during < real * 2,
        "estimated {during} for {real} held"
    );
    drop(held);
    let after = estimated().saturating_sub(before);
    assert!(
        after < real / 8,
        "frees took the estimate back out: {after} left"
    );
}

/// A resized buffer is weighed by the size it has now, so a vector grown from small to large
/// counts as large.
#[test]
fn a_grown_buffer_counts_at_its_new_size() {
    let before = estimated();
    let mut grown: Vec<Vec<u8>> = (0..2048).map(|_| Vec::with_capacity(16)).collect();
    for buffer in &mut grown {
        buffer.reserve_exact(8192);
    }
    let real = 2048 * 8192u64;
    let during = estimated().saturating_sub(before);
    assert!(
        during > real / 2 && during < real * 2,
        "estimated {during} for {real} held"
    );
}

#[test]
fn an_estimate_never_undercounts_the_block() {
    for size in [
        1,
        64,
        4096,
        SAMPLE_BYTES as usize,
        10 * SAMPLE_BYTES as usize,
    ] {
        assert!(estimate(size) >= size);
    }
    // A block far larger than the mean gap is all but certain to be sampled, and stands for
    // about its own size.
    let big = 64 * SAMPLE_BYTES as usize;
    assert!(estimate(big) < big + big / 100);
}

/// A guard already held on this thread stays held however many times something on the thread
/// tries to enter again. The snapshot thread holds one for its whole life; when a failed entry
/// cleared it, the next allocation took the state lock the snapshot already held, and the
/// daemon stopped answering.
#[test]
fn a_held_guard_stays_held_through_nested_entries() {
    let outer = Guard::enter();
    assert!(outer.is_some());
    assert!(Guard::enter().is_none());
    assert!(
        Guard::enter().is_none(),
        "a refused entry let go of the outer guard"
    );
    drop(outer);
    assert!(Guard::enter().is_some());
}

/// A snapshot taken the way the snapshot thread takes one, under its guard, finishes, and its
/// JSON names the sampled sites.
#[test]
fn a_snapshot_taken_under_the_profilers_guard_finishes() {
    // Blocks several times the mean sampling gap are all but certain to be sampled.
    let held: Vec<Vec<u8>> = (0..4)
        .map(|_| vec![1u8; 4 * SAMPLE_BYTES as usize])
        .collect();
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _guard = Guard::enter();
        // Sample the first allocation that gets past the guard, so a guard that lets go while
        // the snapshot holds the lock is caught every time, not one sample in many.
        UNTIL_SAMPLE.with(|left| left.set(1));
        let _ = done.send(snapshot());
    });
    let bytes = finished
        .recv_timeout(std::time::Duration::from_secs(30))
        .expect("the snapshot never finished");
    let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(!parsed["sites"].as_array().unwrap().is_empty(), "{parsed}");
    drop(held);
}
