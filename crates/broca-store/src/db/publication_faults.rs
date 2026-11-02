//! Non-shipping process-kill seam for the synced-WAL publication boundary.
//!
//! Only a child explicitly given a marker path stops here. The production build
//! has neither this module nor the call on the publication path.

/// Announce that a WAL publication synced on the writer's descriptor has reached the database,
/// then wait for the crash harness to kill this process before the commit.
pub(super) fn before_commit(sequence: u64) {
    std::hint::black_box("BROCA_TEST_PUBLICATION_FAULT_V1");
    let Some(marker) = std::env::var_os("BROCA_TEST_PUBLICATION_CUT") else {
        return;
    };
    let minimum = std::env::var("BROCA_TEST_PUBLICATION_CUT_MIN_SEQ")
        .ok()
        .map(|value| value.parse::<u64>().expect("publication cut sequence"))
        .unwrap_or(0);
    if sequence < minimum {
        return;
    }
    std::fs::write(marker, b"synced WAL; database publication not committed\n")
        .expect("announce publication crash cut");
    loop {
        std::thread::park();
    }
}
