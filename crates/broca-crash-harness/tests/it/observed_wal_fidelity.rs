//! The crash harness's observed WAL wrappers take every path production's WAL types take.

use std::collections::BTreeSet;

/// The names of the methods defined directly inside `header`'s impl block.
fn impl_methods(source: &str, header: &str) -> BTreeSet<String> {
    let start = source
        .find(header)
        .unwrap_or_else(|| panic!("no `{header}` block"));
    let body = &source[start + header.len()..];
    let end = body.find("\n}\n").expect("unterminated impl block");
    body[..end]
        .lines()
        .filter_map(|line| line.strip_prefix("    fn "))
        .map(|rest| rest.split(['(', '<']).next().unwrap().to_owned())
        .collect()
}

/// A trait method the observed wrappers leave to its default sends the reference run down
/// a path production never takes, and nothing else would notice.
#[test]
fn observed_wrappers_forward_every_production_override() {
    let here = include_str!("../../src/wal.rs");
    for (production, production_header, observed_header) in [
        (
            include_str!("../../../broca-core/src/wal_writer.rs"),
            "impl WalStore for FileWalStore {",
            "impl WalStore for ObservedWalStore {",
        ),
        (
            include_str!("../../../broca-wal/src/file_wal.rs"),
            "impl Wal for FileWal {",
            "impl Wal for ObservedWal {",
        ),
    ] {
        let wanted = impl_methods(production, production_header);
        // An empty or tiny set would mean the block was not found as expected.
        assert!(wanted.len() > 5, "{production_header} parsed to {wanted:?}");
        let forwarded = impl_methods(here, observed_header);
        let missing: Vec<_> = wanted.difference(&forwarded).collect();
        assert!(
            missing.is_empty(),
            "{observed_header} does not forward {missing:?}"
        );
    }
}
