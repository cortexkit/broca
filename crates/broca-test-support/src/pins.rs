//! Expectations a test keeps as text in a file beside its crate, instead of in its source.
//!
//! A test calls [`expect`] with each value it checks, in order. The values are written as their
//! pretty `Debug` form to `<crate>/pins/<test name>.txt`, one entry per call under its label. A
//! value longer than [`TEXT_LIMIT`] bytes is kept as the SHA-256 of that form, so whole provider
//! requests don't fill the repository; a mismatch then prints the value the test saw.
//! With `BROCA_UPDATE_GOLDEN` set, the file is written from what the test sees; otherwise each
//! value must equal the entry the file holds for that position, and a missing file or entry
//! fails. Regenerate only for an intended change, and read the diff.
//!
//! The test is named by its thread: libtest runs each test on a thread named after it, and a
//! `#[tokio::test]` runs its body on that thread.

use std::collections::HashMap;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

/// One test's entries, in the order its checks run.
struct Pinned {
    file: PathBuf,
    expected: Option<Vec<(String, String)>>,
    seen: Vec<(String, String)>,
}

fn tests() -> &'static Mutex<HashMap<String, Pinned>> {
    static TESTS: OnceLock<Mutex<HashMap<String, Pinned>>> = OnceLock::new();
    TESTS.get_or_init(Mutex::default)
}

/// The longest value kept as text; a longer one is kept as its digest.
pub const TEXT_LIMIT: usize = 2048;

/// A value whose pinned form is `text` itself, such as pretty JSON, rather than its `Debug`
/// quoting.
pub struct Text(pub String);

impl Debug for Text {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// `text`, or its digest when it is longer than [`TEXT_LIMIT`].
fn kept(text: &str) -> String {
    if text.len() <= TEXT_LIMIT {
        return text.to_owned();
    }
    use sha2::Digest as _;
    let digest = sha2::Sha256::digest(text.as_bytes());
    let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    format!("sha256:{hex} ({} bytes)", text.len())
}

fn updating() -> bool {
    std::env::var_os("BROCA_UPDATE_GOLDEN").is_some()
}

fn test_name() -> String {
    let thread = std::thread::current();
    match thread.name() {
        Some(name) if name != "main" => name.to_owned(),
        _ => panic!(
            "pins: the test runs on a thread with no test name; call pins::expect from the \
             test's own thread"
        ),
    }
}

fn parse(text: &str) -> Vec<(String, String)> {
    let mut entries = Vec::new();
    for block in text
        .split("\n== ")
        .map(|block| block.trim_start_matches("== "))
    {
        if block.trim().is_empty() {
            continue;
        }
        let (label, value) = block.split_once('\n').unwrap_or((block, ""));
        entries.push((label.to_owned(), value.trim_end_matches('\n').to_owned()));
    }
    entries
}

fn render(entries: &[(String, String)]) -> String {
    let mut text = String::new();
    for (label, value) in entries {
        text.push_str("== ");
        text.push_str(label);
        text.push('\n');
        text.push_str(value);
        text.push('\n');
    }
    text
}

/// Checks `value` against this test's next pinned entry, labelled `label`. `crate_dir` is the
/// calling crate's `CARGO_MANIFEST_DIR`; [`crate::expect_pinned`] passes it.
pub fn expect(crate_dir: &str, label: &str, value: &dyn Debug) {
    let name = test_name();
    let full = format!("{value:#?}");
    let observed = kept(&full);
    let mut tests = tests().lock().unwrap_or_else(|poison| poison.into_inner());
    let pinned = tests.entry(name.clone()).or_insert_with(|| {
        let file = Path::new(crate_dir)
            .join("pins")
            .join(format!("{}.txt", name.replace("::", ".")));
        let expected = (!updating()).then(|| {
            let text = std::fs::read_to_string(&file).unwrap_or_else(|error| {
                panic!(
                    "pins: {} can't be read ({error}); write it with BROCA_UPDATE_GOLDEN=1",
                    file.display()
                )
            });
            parse(&text)
        });
        Pinned {
            file,
            expected,
            seen: Vec::new(),
        }
    });
    let position = pinned.seen.len();
    pinned.seen.push((label.to_owned(), observed.clone()));
    match &pinned.expected {
        Some(expected) => {
            let Some((want_label, want)) = expected.get(position) else {
                panic!(
                    "pins: {} holds {} entries, and the test checks a value at {position} \
                     ({label})",
                    pinned.file.display(),
                    expected.len()
                );
            };
            // Entries match by position. A label can name a value minted per run, such as a
            // message id, so it only helps a reader find the entry.
            if &observed != want {
                let shown: String = full.chars().take(8 * 1024).collect();
                panic!(
                    "pins: {label} differs from entry {position} of {} (pinned as \
                     {want_label})\n  pinned: {want}\n  seen:   {observed}\nthe value seen:\n\
                     {shown}",
                    pinned.file.display()
                );
            }
        }
        None => {
            if let Some(parent) = pinned.file.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::write(&pinned.file, render(&pinned.seen)).unwrap();
        }
    }
}

/// Fails when the test checked fewer values than its file pins, so a check that silently
/// stopped running is caught. A test ends with it.
pub fn all_checked(crate_dir: &str) {
    let name = test_name();
    let tests = tests().lock().unwrap_or_else(|poison| poison.into_inner());
    let Some(pinned) = tests.get(&name) else {
        let file = Path::new(crate_dir)
            .join("pins")
            .join(format!("{}.txt", name.replace("::", ".")));
        assert!(
            updating() || !file.exists(),
            "pins: the test checked nothing, and {} pins values",
            file.display()
        );
        return;
    };
    if let Some(expected) = &pinned.expected {
        assert_eq!(
            pinned.seen.len(),
            expected.len(),
            "pins: the test checked {} values, and {} pins {}",
            pinned.seen.len(),
            pinned.file.display(),
            expected.len()
        );
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn entries_read_back_as_written() {
        let entries = vec![
            ("request 0".to_owned(), "Call {\n    a: 1,\n}".to_owned()),
            ("record 3".to_owned(), "\"text\"".to_owned()),
            ("empty".to_owned(), String::new()),
        ];
        assert_eq!(super::parse(&super::render(&entries)), entries);
    }

    #[test]
    fn a_long_value_is_kept_as_its_digest() {
        let short = "x".repeat(super::TEXT_LIMIT);
        assert_eq!(super::kept(&short), short);
        let long = "x".repeat(super::TEXT_LIMIT + 1);
        let kept = super::kept(&long);
        assert!(kept.starts_with("sha256:"), "{kept}");
        assert!(
            kept.ends_with(&format!("({} bytes)", super::TEXT_LIMIT + 1)),
            "{kept}"
        );
    }
}
