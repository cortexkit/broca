//! The sweep must NOT remove a sibling whose scratch is recent.
//!
//! A sweep proven only to remove things would also remove a concurrently running test
//! binary's scratch, and that failure surfaces as some unrelated test flaking. This is the
//! arm that guards against it.
//!
//! This file holds exactly one test so it runs in its own process under both
//! `cargo test` and nextest: the sweep runs once per process, on the first call to
//! `scratch_root()`, so the plant must happen before anything else in the process calls it.

use std::time::{Duration, SystemTime};

mod age;

use broca_test_support::{scratch_root, ROOT_DIR_NAME, SWEEP_AGE};

#[test]
fn sweep_spares_a_sibling_younger_than_the_threshold() {
    // A numeric name that is no real pid on any platform (pids stay below 2^22), offset
    // by our own pid so concurrent runs of this test never share a plant.
    let fake_pid = 4_100_000_000u32 + std::process::id();
    let parent = std::env::temp_dir().join(ROOT_DIR_NAME);
    let planted = parent.join(broca_test_support::scratch_dir_name(fake_pid, 0));
    std::fs::create_dir_all(planted.join("nested")).expect("plant recent sibling");
    std::fs::write(planted.join("nested").join("in-use"), b"live").expect("plant file");

    // Recent but not brand new: an hour old, still well inside the threshold, so the test
    // covers a long-running neighbour and not only one created a moment ago.
    let recent = SystemTime::now() - Duration::from_secs(60 * 60);
    assert!(Duration::from_secs(60 * 60) < SWEEP_AGE);
    age::set_dir_mtime(&planted, recent);

    let root = scratch_root();
    let survived = planted.join("nested").join("in-use").is_file();

    // This test's own plant is not evidence of anything once asserted; remove it so the
    // test does not leave litter of its own.
    let _ = std::fs::remove_dir_all(&planted);

    assert!(root.is_dir());
    assert!(
        survived,
        "a recent sibling scratch directory must survive the sweep: {}",
        planted.display()
    );
}
