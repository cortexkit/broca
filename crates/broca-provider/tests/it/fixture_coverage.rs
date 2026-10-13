//! Every fixture in this crate's fixture directory is read by some test in this crate.
//!
//! A fixture that nothing loads is not a failing test, it is a silence: it lands, no test
//! references it, and every suite stays green forever. There is no deletion for review to
//! catch and no moment at which anything changed, so the gap does not announce itself the
//! way an eroded test does. The cost is worst for the fixtures that encode absence,
//! refusal, or malformed input, because those exist precisely to pin behaviour nobody
//! exercises by accident.
//!
//! This is a set predicate rather than a count. Adding a fixture together with the test
//! that reads it keeps it true, so it does not need updating on every legitimate change --
//! whereas pinning the number of fixtures would fail for an addition that is entirely
//! correct. It is assertable here only because this crate owns both sides: it supplies the
//! fixtures and it supplies their readers. A directory owned by someone else could only be
//! reported on, since they are free to add vectors this side has no obligation to consume.

use std::collections::BTreeSet;
use std::path::Path;

/// A fixture counts as read when its file name appears in a string literal, which is how
/// every reader here refers to one. Matching the name with its closing quote keeps
/// `x.json` distinct from `x.json.version`: both exist as separate fixtures, and a bare
/// substring search would report the first as read whenever only the second is.
fn is_referenced(name: &str, sources: &[String]) -> bool {
    let quoted = format!("{name}\"");
    sources.iter().any(|src| src.contains(&quoted))
}

fn read_sources(tests_dir: &Path) -> Vec<String> {
    let mut sources = Vec::new();
    let mut stack = vec![tests_dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).expect("read tests directory") {
            let path = entry.expect("read directory entry").path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                sources.push(std::fs::read_to_string(&path).expect("read test source"));
            }
        }
    }
    sources
}

#[test]
fn every_fixture_is_read_by_some_test() {
    let tests_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let fixtures_dir = tests_dir.join("fixtures");

    let fixtures: BTreeSet<String> = std::fs::read_dir(&fixtures_dir)
        .expect("read fixtures directory")
        .map(|entry| {
            entry
                .expect("read fixture entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();

    let sources = read_sources(&tests_dir);

    // Both sides have to be non-empty before an empty difference means anything. A wrong
    // directory, a rename, or a reader that silently returns nothing all produce a clean
    // result that is indistinguishable from full coverage.
    assert!(
        !fixtures.is_empty(),
        "no fixtures found under {} -- the scan is not reading what it should",
        fixtures_dir.display()
    );
    assert!(
        !sources.is_empty(),
        "no test sources found under {} -- the scan is not reading what it should",
        tests_dir.display()
    );

    let unread: Vec<&String> = fixtures
        .iter()
        .filter(|name| !is_referenced(name, &sources))
        .collect();

    assert!(
        unread.is_empty(),
        "{} fixture(s) are not read by any test in this crate:\n{}\n\n\
         Either add the test that consumes them, or delete them. A fixture nothing loads \
         pins nothing, while still reading like coverage to anyone who lists the directory.",
        unread.len(),
        unread
            .iter()
            .map(|name| format!("  {name}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

/// The check above can only report an unread fixture if its notion of "read" is capable of
/// returning false. Everything it examines is genuinely referenced, so on this tree the
/// negative arm is never exercised -- and a predicate that always returns true would pass
/// the whole suite while proving nothing.
#[test]
fn the_reference_test_can_return_false() {
    let sources = vec![String::from(
        "include_bytes!(\"fixtures/real_fixture.json\")",
    )];

    assert!(
        is_referenced("real_fixture.json", &sources),
        "a fixture named in a source literal must count as read"
    );
    assert!(
        !is_referenced("absent_fixture.json", &sources),
        "a fixture named nowhere must count as unread"
    );
    assert!(
        !is_referenced("real_fixture.json.version", &sources),
        "a sibling sharing a prefix must not inherit the other's reference"
    );
}
