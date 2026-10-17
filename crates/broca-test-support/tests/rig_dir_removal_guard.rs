//! A test file that runs the real daemon must not remove a rig directory and discard the error.
//!
//! The daemon's modules keep writing into the rig directory (broca creates its state root,
//! run index and stall logs there while it starts). A rig that kills only the daemon and then
//! removes the directory with `let _ = std::fs::remove_dir_all(..)` can lose the race to a
//! module that is still filling the directory, and the error that would say so is discarded.
//! The directory then survives the test, or comes back after it, and nothing reports it.
//!
//! Rigs end the daemon with `broca_test_support::kill_and_reap_tree`, which also ends its
//! modules, and remove the directory with `broca_test_support::remove_rig_dir`, which reports a
//! failed removal. This guard fails on the swallowed shapes (`let _ = ..remove_dir_all(..)` and
//! `..remove_dir_all(..).ok()`) in every file that runs the real daemon.

use std::path::{Path, PathBuf};

/// What a byte of Rust source belongs to.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Class {
    Code,
    /// A string or character literal, including its quotes.
    Literal,
    Comment,
}

/// Classify every byte of `source` as code, literal or comment.
///
/// Handles line and nested block comments, strings with escapes, raw strings (`r"…"`,
/// `r#"…"#`), and character literals, telling them apart from lifetimes (`'a`).
fn classify(source: &str) -> Vec<Class> {
    let bytes = source.as_bytes();
    let mut class = vec![Class::Code; bytes.len()];
    let mut index = 0;
    while index < bytes.len() {
        let previous_is_ident =
            index > 0 && (bytes[index - 1].is_ascii_alphanumeric() || bytes[index - 1] == b'_');
        let end = if bytes[index..].starts_with(b"//") {
            let line_end = source[index..]
                .find('\n')
                .map_or(bytes.len(), |n| index + n);
            class[index..line_end].fill(Class::Comment);
            line_end
        } else if bytes[index..].starts_with(b"/*") {
            let mut depth = 0usize;
            let mut cursor = index;
            while cursor < bytes.len() {
                if bytes[cursor..].starts_with(b"/*") {
                    depth += 1;
                    cursor += 2;
                } else if bytes[cursor..].starts_with(b"*/") {
                    depth -= 1;
                    cursor += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    cursor += 1;
                }
            }
            class[index..cursor].fill(Class::Comment);
            cursor
        } else if bytes[index] == b'r' && !previous_is_ident {
            let mut quote = index + 1;
            while bytes.get(quote) == Some(&b'#') {
                quote += 1;
            }
            if bytes.get(quote) == Some(&b'"') {
                let terminator = format!("\"{}", "#".repeat(quote - index - 1));
                let literal_end = source[quote + 1..]
                    .find(&terminator)
                    .map_or(bytes.len(), |n| quote + 1 + n + terminator.len());
                class[index..literal_end].fill(Class::Literal);
                literal_end
            } else {
                index + 1
            }
        } else if bytes[index] == b'"' {
            let mut cursor = index + 1;
            while cursor < bytes.len() && bytes[cursor] != b'"' {
                cursor += if bytes[cursor] == b'\\' { 2 } else { 1 };
            }
            let literal_end = (cursor + 1).min(bytes.len());
            class[index..literal_end].fill(Class::Literal);
            literal_end
        } else if bytes[index] == b'\'' {
            // A character literal is `'x'` or an escape such as `'\''`; anything else is a
            // lifetime or label, which is code.
            let literal_end = if bytes.get(index + 1) == Some(&b'\\') {
                // The escaped character itself may be a quote (`'\''`), so the closing quote
                // is searched for after it.
                source
                    .get(index + 3..)
                    .and_then(|rest| rest.find('\''))
                    .map(|n| index + 3 + n + 1)
            } else {
                source[index + 1..]
                    .chars()
                    .next()
                    .map(|c| index + 1 + c.len_utf8())
                    .filter(|&close| bytes.get(close) == Some(&b'\''))
                    .map(|close| close + 1)
            };
            match literal_end {
                Some(literal_end) => {
                    class[index..literal_end].fill(Class::Literal);
                    literal_end
                }
                None => index + 1,
            }
        } else {
            index + 1
        };
        index = end;
    }
    class
}

/// The binary name rigs resolve the real daemon by, as a string literal in source. Assembled
/// here so this guard's own source does not read as a file that runs the daemon.
const DAEMON_LITERAL: &str = concat!("\"ck-", "subc\"");

/// Whether `source` names the real daemon binary outside a comment.
fn names_the_real_daemon(source: &str) -> bool {
    let class = classify(source);
    source
        .match_indices(DAEMON_LITERAL)
        .any(|(position, _)| class[position] == Class::Literal)
}

fn is_ident(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

/// Every `remove_dir_all(..)` call in `source` whose result is discarded, as
/// `line N: <statement>`.
fn swallowed_removals(source: &str) -> Vec<String> {
    let class = classify(source);
    let bytes = source.as_bytes();
    let is_code = |position: usize| class.get(position) == Some(&Class::Code);
    let mut found = Vec::new();
    for (start, name) in source.match_indices("remove_dir_all") {
        let end = start + name.len();
        if !is_code(start)
            || (start > 0 && is_ident(bytes[start - 1]))
            || bytes.get(end).copied().is_some_and(is_ident)
        {
            continue;
        }
        let Some(open) = (end..bytes.len()).find(|&n| !bytes[n].is_ascii_whitespace()) else {
            continue;
        };
        if bytes[open] != b'(' || !is_code(open) {
            continue;
        }
        let mut depth = 0usize;
        let Some(close) = (open..bytes.len()).find(|&n| {
            if is_code(n) {
                match bytes[n] {
                    b'(' => depth += 1,
                    b')' => depth -= 1,
                    _ => {}
                }
            }
            depth == 0
        }) else {
            continue;
        };
        let statement_start = (0..start)
            .rev()
            .find(|&n| is_code(n) && matches!(bytes[n], b';' | b'{' | b'}'))
            .map_or(0, |n| n + 1);
        // `let _ = std::fs::remove_dir_all(..)`: the text before the path is exactly `let _ =`.
        let path_start = (statement_start..start)
            .rev()
            .find(|&n| !(is_ident(bytes[n]) || bytes[n] == b':'))
            .map_or(statement_start, |n| n + 1);
        let before: String = source[statement_start..path_start]
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();
        let discarded_by_let = before == "let_=";
        let after: String = source[close + 1..]
            .chars()
            .take_while(|&c| c != ';' && c != '}')
            .filter(|c| !c.is_whitespace())
            .collect();
        let discarded_by_ok = after.starts_with(".ok()");
        if discarded_by_let || discarded_by_ok {
            let line = source[..start].matches('\n').count() + 1;
            let statement_end = source[close..]
                .find(';')
                .map_or(close + 1, |n| close + n + 1);
            found.push(format!(
                "line {line}: {}",
                source[statement_start..statement_end].trim()
            ));
        }
    }
    found
}

fn rust_files(directory: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(directory).expect("reading the crates source tree") {
        let path = entry.expect("reading a crates source entry").path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            out.push(path);
        }
    }
}

/// The files that run the real daemon: those naming its binary, plus every file under the
/// module directory of a test target whose root file names it (for a root `tests/foo.rs`, the
/// files under `tests/foo/`), since those modules start the daemon through helpers in the root.
fn daemon_files(files: &[PathBuf]) -> Vec<(PathBuf, String)> {
    let sources: Vec<(PathBuf, String)> = files
        .iter()
        .map(|file| {
            let source = std::fs::read_to_string(file)
                .unwrap_or_else(|error| panic!("reading {} failed: {error}", file.display()));
            (file.clone(), source)
        })
        .collect();
    let target_roots: Vec<PathBuf> = sources
        .iter()
        .filter(|(file, source)| {
            names_the_real_daemon(source)
                && file
                    .parent()
                    .and_then(Path::file_name)
                    .is_some_and(|parent| parent == "tests")
        })
        .map(|(file, _)| file.with_extension(""))
        .collect();
    sources
        .into_iter()
        .filter(|(file, source)| {
            names_the_real_daemon(source) || target_roots.iter().any(|root| file.starts_with(root))
        })
        .collect()
}

#[test]
fn real_daemon_rigs_report_rig_directory_removal_failures() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut files = Vec::new();
    rust_files(&workspace.join("crates"), &mut files);
    let daemon_files = daemon_files(&files);
    let relative = |file: &Path| {
        // Join components with `/` so the expected paths below match on Windows too, where
        // `display()` would separate them with `\`.
        file.strip_prefix(&workspace)
            .unwrap_or(file)
            .components()
            .map(|part| part.as_os_str().to_string_lossy())
            .collect::<Vec<_>>()
            .join("/")
    };
    let scanned: Vec<String> = daemon_files
        .iter()
        .map(|(file, _)| relative(file))
        .collect();
    // Positive controls: a rig that names the daemon directly, and a module file reached only
    // through its test target's root. Without them an empty scan would pass.
    for expected in [
        "crates/broca-module-serve/tests/it/approval_gate.rs",
        "crates/broca-module-serve/tests/llm_runner_conformance/attribution.rs",
    ] {
        assert!(
            scanned.iter().any(|file| file == expected),
            "the scan must cover {expected}; it covered:\n{}",
            scanned.join("\n")
        );
    }
    let offenders: Vec<String> = daemon_files
        .iter()
        .filter_map(|(file, source)| {
            let sites = swallowed_removals(source);
            (!sites.is_empty()).then(|| format!("{}:\n{}", relative(file), sites.join("\n")))
        })
        .collect();
    assert!(
        offenders.is_empty(),
        "files that run the real daemon must end it with broca_test_support::kill_and_reap_tree \
         and remove rig directories with broca_test_support::remove_rig_dir, which reports a \
         failed removal instead of discarding it:\n{}",
        offenders.join("\n")
    );
}

mod scanner_tests {
    use super::{names_the_real_daemon, swallowed_removals, DAEMON_LITERAL};

    #[test]
    fn detects_each_swallowed_removal_shape() {
        let source = r#"
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.dir);
                if !std::thread::panicking() { let _ = fs::remove_dir_all(dir.join("a")); }
                std::fs::remove_dir_all(&root).ok();
                let _ = remove_dir_all(root);
            }
        "#;
        let found = swallowed_removals(source);
        assert_eq!(found.len(), 4, "{found:?}");
        assert!(found[0].starts_with("line 3: let _ = std::fs::remove_dir_all"));
    }

    #[test]
    fn accepts_reported_removals_and_text_that_is_not_a_call() {
        let source = r#"
            fn drop(&mut self) {
                broca_test_support::remove_rig_dir(&self.dir);
                std::fs::remove_dir_all(&root).unwrap();
                let removed = std::fs::remove_dir_all(&root);
                // let _ = std::fs::remove_dir_all(&self.dir);
                /* let _ = std::fs::remove_dir_all(&self.dir); */
                let text = "let _ = std::fs::remove_dir_all(&self.dir);";
                let quote = '"'; let escaped = '\''; let _ = make_remove_dir_all(&dir);
            }
        "#;
        assert_eq!(swallowed_removals(source), Vec::<String>::new());
    }

    /// A quote inside a character literal must not open a string that hides the code after it.
    #[test]
    fn character_literals_do_not_hide_a_later_removal() {
        let source = r#"
            fn drop(&mut self) {
                let quote = '"'; let escaped = '\''; let lifetime: &'static str = "";
                let _ = std::fs::remove_dir_all(&self.dir);
            }
        "#;
        assert_eq!(swallowed_removals(source).len(), 1);
    }

    #[test]
    fn a_file_names_the_daemon_only_in_code() {
        let call = format!("let bin = subc_bin({DAEMON_LITERAL}, \"SUBC_CORE_BIN\");");
        assert!(names_the_real_daemon(&call));
        let comment = format!("// resolved like subc_bin({DAEMON_LITERAL}, ..)\nfn f() {{}}");
        assert!(!names_the_real_daemon(&comment));
        assert!(!names_the_real_daemon(
            "let bin = subc_bin(\"fake-aft-stub\", \"X\");"
        ));
    }
}
