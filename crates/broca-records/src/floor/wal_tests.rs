use broca_records::floor::*;
use std::collections::BTreeSet;
use std::path::PathBuf;

mod tests {
    use super::*;

    pub(super) fn scratch(tag: &str) -> PathBuf {
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-floor-{tag}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }
}

#[path = "io_tests.rs"]
mod io_tests;
