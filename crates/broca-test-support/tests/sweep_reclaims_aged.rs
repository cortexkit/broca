//! The sweep removes a sibling process's scratch once it has aged past the threshold.
//!
//! This file holds exactly one test so it runs in its own process under both
//! `cargo test` and nextest: the sweep runs once per process, on the first call to
//! `scratch_root()`, so the plant must happen before anything else in the process calls it.

use std::time::{Duration, SystemTime};

mod age;

use broca_test_support::{scratch_root, ROOT_DIR_NAME, SWEEP_AGE};

#[test]
fn sweep_reclaims_a_sibling_older_than_the_threshold() {
    // A numeric name that is no real pid on any platform (pids stay below 2^22), offset
    // by our own pid so concurrent runs of this test never share a plant.
    let fake_pid = 4_000_000_000u32 + std::process::id();
    let parent = std::env::temp_dir().join(ROOT_DIR_NAME);
    let planted = parent.join(broca_test_support::scratch_dir_name(fake_pid, 0));
    std::fs::create_dir_all(planted.join("nested")).expect("plant aged sibling");
    std::fs::write(planted.join("nested").join("left-behind"), b"orphan").expect("plant file");

    let aged = SystemTime::now() - SWEEP_AGE - Duration::from_secs(60 * 60);
    age::set_dir_mtime(&planted, aged);
    assert!(planted.is_dir(), "the plant is in place before the sweep");

    let root = scratch_root();

    assert!(root.is_dir());
    assert!(
        !planted.exists(),
        "an aged sibling scratch directory must be reclaimed: {}",
        planted.display()
    );
}
