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
