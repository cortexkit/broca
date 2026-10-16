//! Constructor-detector controls for reader-only records. Compaction's reader-only guard
//! now runs at real admission in broca-module-serve: compaction plans must still be refused
//! before a WAL opens or Setup runs until the complete writer release ships.

use std::path::{Path, PathBuf};

const VARIANTS: &[&str] = &[
    "CompactionRequest",
    "CompactionMessage",
    "CompactionAnswer",
    "CompactionWait",
];

const DEFERRED_VARIANTS: &[&str] = &[
    "ToolCallDeferred",
    "ApprovalRecorded",
    "AwaitingApproval",
    "LateApprovalRecorded",
    "LateDispatchIntent",
    "LateResult",
];

pub(super) fn sources(dir: &Path, result: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            sources(&path, result);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            result.push(path);
        }
    }
}

/// Whether an out-of-line module is declared under `#[cfg(test)]`, directly or through
/// another test-only module. Follow declarations rather than filenames: a test module's
/// children inherit its configuration even when their own declarations have only `#[path]`.
pub(super) fn is_cfg_test_module_file(path: &Path) -> bool {
    cfg_test_module_file(path, &mut std::collections::HashSet::new())
}

fn cfg_test_module_file(path: &Path, seen: &mut std::collections::HashSet<PathBuf>) -> bool {
    if !seen.insert(path.to_path_buf()) {
        return false;
    }
    let (Some(dir), Some(name), Some(file_name)) = (
        path.parent(),
        path.file_stem().and_then(|s| s.to_str()),
        path.file_name().and_then(|s| s.to_str()),
    ) else {
        return false;
    };
    let parents = [
        dir.with_extension("rs"),
        dir.join("mod.rs"),
        dir.join("lib.rs"),
        dir.join("main.rs"),
    ];
    // The attributes directly above a declaration belong to it; any other line ends them.
    let attributes = |lines: &[&str], at: usize| -> Vec<String> {
        lines[..at]
            .iter()
            .rev()
            .take_while(|above| above.starts_with("#["))
            .map(|above| above.to_string())
            .collect()
    };
    let is_module = |line: &str| {
        let declaration = line
            .trim_start_matches("pub(crate) ")
            .trim_start_matches("pub(super) ")
            .trim_start_matches("pub ");
        declaration.starts_with("mod ") && declaration.ends_with(';')
    };
    let by_parent = parents
        .iter()
        .filter(|parent| parent.as_path() != path)
        .filter_map(|parent| {
            std::fs::read_to_string(parent)
                .ok()
                .map(|source| (parent, source))
        })
        .any(|(parent, source)| {
            let lines: Vec<&str> = source.lines().map(str::trim).collect();
            lines.iter().enumerate().any(|(at, line)| {
                is_module(line)
                    && line.ends_with(&format!("mod {name};"))
                    && (attributes(&lines, at).iter().any(|a| a == "#[cfg(test)]")
                        || cfg_test_module_file(parent, seen))
            })
        });
    if by_parent {
        return true;
    }
    // A sibling can declare a `#[path]` module under any name; its test-only
    // configuration also applies to the file it names.
    let path_attribute = format!("#[path = \"{file_name}\"]");
    std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|sibling| sibling != path && sibling.extension().is_some_and(|ext| ext == "rs"))
        .filter_map(|sibling| {
            std::fs::read_to_string(&sibling)
                .ok()
                .map(|source| (sibling, source))
        })
        .any(|(sibling, source)| {
            let lines: Vec<&str> = source.lines().map(str::trim).collect();
            lines.iter().enumerate().any(|(at, line)| {
                let attrs = attributes(&lines, at);
                is_module(line)
                    && attrs.contains(&path_attribute)
                    && (attrs.iter().any(|a| a == "#[cfg(test)]")
                        || cfg_test_module_file(&sibling, seen))
            })
        })
}

/// Small lexer for source fencing, not a Rust parser. Comments and literals cannot create
/// constructor matches. Keeping delimiters lets cfg(test) items be skipped individually,
/// rather than cutting a file at its first test-only import and losing later production.
pub(super) fn tokens(source: &str) -> Vec<&str> {
    let bytes = source.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i].is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if bytes[i..].starts_with(b"//") {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if bytes[i..].starts_with(b"/*") {
            i += 2;
            let mut depth = 1;
            while i < bytes.len() && depth > 0 {
                if bytes[i..].starts_with(b"/*") {
                    depth += 1;
                    i += 2;
                } else if bytes[i..].starts_with(b"*/") {
                    depth -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
            continue;
        }
        let start = i;
        // A character literal such as '"' must not open a string. A lifetime has no
        // closing quote at the character boundary, so it remains an ordinary token.
        if bytes[i] == b'\'' && i + 1 < bytes.len() {
            let at = i + 1;
            let end = if bytes[at] == b'\\' {
                (at + 2..bytes.len().min(at + 16)).find(|at| bytes[*at] == b'\'')
            } else {
                let end = at + source[at..].chars().next().unwrap().len_utf8();
                (bytes.get(end) == Some(&b'\'')).then_some(end)
            };
            if let Some(end) = end {
                i = end + 1;
                continue;
            }
        }
        // Raw and raw byte strings may contain quotes, braces and source-looking text.
        let raw = if bytes[i..].starts_with(b"br") {
            Some(i + 2)
        } else if bytes[i] == b'r' {
            Some(i + 1)
        } else {
            None
        };
        if let Some(mut at) = raw {
            let hashes = at;
            while at < bytes.len() && bytes[at] == b'#' {
                at += 1;
            }
            if at < bytes.len() && bytes[at] == b'"' {
                let count = at - hashes;
                i = at + 1;
                while i < bytes.len() {
                    if bytes[i] == b'"'
                        && bytes
                            .get(i + 1..i + 1 + count)
                            .is_some_and(|tail| tail.iter().all(|b| *b == b'#'))
                    {
                        i += count + 1;
                        break;
                    }
                    i += 1;
                }
                continue;
            }
        }
        if bytes[i] == b'"' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\\' {
                    i = (i + 2).min(bytes.len());
                } else if bytes[i] == b'"' {
                    i += 1;
                    break;
                } else {
                    i += 1;
                }
            }
            continue;
        }
        if bytes[i].is_ascii_alphabetic() || bytes[i] == b'_' {
            i += 1;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
        } else {
            // Consume a complete UTF-8 character even though all searched tokens are ASCII.
            i += source[i..].chars().next().unwrap().len_utf8();
        }
        out.push(&source[start..i]);
    }
    out
}

pub(super) fn close_brace(tokens: &[&str], start: usize) -> usize {
    let mut depth = 0;
    for (at, token) in tokens.iter().enumerate().skip(start) {
        if *token == "{" {
            depth += 1;
        }
        if *token == "}" {
            depth -= 1;
            if depth == 0 {
                return at;
            }
        }
    }
    panic!("unclosed source item");
}

fn constructors(source: &str) -> Vec<String> {
    scan_constructors(source, VARIANTS, false)
}

fn scan_constructors(source: &str, variants: &[&str], deferred_fields: bool) -> Vec<String> {
    let tokens = tokens(source);
    let mut found = Vec::new();
    let mut i = 0;
    while i < tokens.len() {
        if tokens[i..].starts_with(&["#", "[", "cfg", "(", "test", ")", "]"]) {
            i += 7;
            while i < tokens.len() && tokens[i] != ";" && tokens[i] != "{" {
                i += 1;
            }
            if i < tokens.len() && tokens[i] == "{" {
                i = close_brace(&tokens, i);
            }
            i += 1;
            continue;
        }
        if tokens[i] == "enum" || tokens[i] == "struct" {
            while i < tokens.len() && tokens[i] != ";" && tokens[i] != "{" {
                i += 1;
            }
            if i < tokens.len() && tokens[i] == "{" {
                i = close_brace(&tokens, i);
            }
            i += 1;
            continue;
        }
        if deferred_fields && tokens[i] == "WithdrawElicitation" && tokens.get(i + 1) == Some(&"(")
        {
            // The seal helper accepts a caller-selected close kind. Reader production callers
            // must never construct its writer arm; pattern arms in the helper do not write.
            let mut end = i + 1;
            let mut depth = 0;
            while end < tokens.len() {
                if tokens[end] == "(" {
                    depth += 1;
                }
                if tokens[end] == ")" {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                end += 1;
            }
            if !tokens[end + 1..].starts_with(&["=", ">"]) {
                found.push("WithdrawElicitation".into());
            }
            i = end;
        }
        if deferred_fields
            && ["ToolDispatchIntent", "ToolResult"].contains(&tokens[i])
            && tokens.get(i + 1) == Some(&"{")
        {
            let end = close_brace(&tokens, i + 1);
            let next = &tokens[end + 1..];
            let pattern = next.starts_with(&["=", ">"])
                || tokens[i + 2..end].windows(2).any(|pair| pair == [".", "."])
                || next
                    .first()
                    .is_some_and(|token| ["|", "if", "="].contains(token));
            if !pattern {
                let mut at = i + 2;
                while at < end {
                    if [
                        "approval_digest",
                        "call_key",
                        "withdraw_elicitation",
                        "provider_held",
                    ]
                    .contains(&tokens[at])
                    {
                        if tokens.get(at + 1) == Some(&":") && tokens.get(at + 2) != Some(&"None") {
                            found.push(format!("{}.{}", tokens[i], tokens[at]));
                        } else if tokens
                            .get(at + 1)
                            .is_some_and(|token| [",", "}"].contains(token))
                        {
                            // This shorthand is confined to the seal helper; the writer close-kind
                            // constructor guard above proves no reader caller can populate it.
                            if tokens[at] != "withdraw_elicitation"
                                || !(0..i)
                                    .rev()
                                    .find(|at| tokens[*at] == "fn")
                                    .is_some_and(|at| {
                                        tokens.get(at + 1) == Some(&"close_out_open_batch")
                                    })
                            {
                                found.push(format!("{}.{}", tokens[i], tokens[at]));
                            }
                        }
                    }
                    if ["{", "("].contains(&tokens[at]) {
                        let open = tokens[at];
                        let close = if open == "{" { "}" } else { ")" };
                        let mut depth = 1;
                        at += 1;
                        while at < end && depth > 0 {
                            if tokens[at] == open {
                                depth += 1;
                            }
                            if tokens[at] == close {
                                depth -= 1;
                            }
                            at += 1;
                        }
                    } else {
                        at += 1;
                    }
                }
            }
            i = end;
        }
        // Match qualified constructors, including a variant imported without WalRecord.
        if variants.contains(&tokens[i]) && tokens.get(i + 1) == Some(&"{") {
            let end = close_brace(&tokens, i + 1);
            let next = &tokens[end + 1..];
            let pattern = next.starts_with(&["=", ">"])
                || tokens[i + 2..end].windows(2).any(|pair| pair == [".", "."])
                || next.first() == Some(&"|")
                || next.first() == Some(&"if")
                || next.first() == Some(&"=");
            if !pattern {
                found.push(tokens[i].to_owned());
            }
            i = end;
        }
        i += 1;
    }
    found
}

#[test]
fn constructor_detector_recognizes_compaction_variants() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");

    // Plant each real constructor into a copy of a production file, not a proxy predicate.
    let live = std::fs::read_to_string(root.join("crates/broca-wal/src/lib.rs")).unwrap();
    for variant in VARIANTS {
        let planted = format!("{live}\nfn planted() {{ let record = WalRecord::{variant} {{ provider: p, request_id: id }}; }}");
        assert_eq!(
            constructors(&planted),
            [*variant],
            "planted {variant} must be caught"
        );
        let shorthand = format!(
            "fn planted() {{ let record = WalRecord::{variant} {{ provider, request_id }}; }}"
        );
        assert_eq!(constructors(&shorthand), [*variant]);
        let patterns = format!("match record {{ WalRecord::{variant} {{ provider, .. }} => {{}} }}\n#[cfg(test)] mod tests {{ fn example() {{ WalRecord::{variant} {{ provider: p }}; }} }}\nfn later() {{ WalRecord::{variant} {{ provider: p }}; }}");
        assert_eq!(
            constructors(&patterns),
            [*variant],
            "test exclusion must not hide later production"
        );
    }
}

#[test]
fn only_cfg_test_module_files_are_excluded_from_source_scans() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    // These out-of-line test modules include direct, #[path], and inherited declarations.
    for test_file in [
        "crates/broca-core/src/wal_fold/tests.rs",
        "crates/broca-core/src/retention/tests.rs",
        "crates/broca-core/src/run/hook_tests.rs",
        "crates/broca-module/src/approval_tests.rs",
        "crates/broca-module/src/retention_tests.rs",
        "crates/broca-core/src/run/consent_fourth_audit_tests.rs",
        "crates/broca-core/src/run/consent_runtime_tests.rs",
        "crates/broca-core/src/run/consent_audit_tests.rs",
    ] {
        assert!(
            is_cfg_test_module_file(&root.join(test_file)),
            "{test_file} must be recognised as a cfg(test) module"
        );
    }
    // Production modules, including ones whose parent also declares test modules.
    for production in [
        "crates/broca-core/src/run.rs",
        "crates/broca-core/src/consent.rs",
        "crates/broca-core/src/replay.rs",
        "crates/broca-core/src/retention.rs",
        "crates/broca-core/src/wal_writer/extraction.rs",
    ] {
        assert!(
            !is_cfg_test_module_file(&root.join(production)),
            "{production} must stay in the production scan"
        );
    }
    // A planted module declared without cfg(test), and one with cfg(test) on another item.
    let scratch =
        broca_test_support::scratch_root().join(format!("cfg-test-scan-{}", std::process::id()));
    std::fs::create_dir_all(scratch.join("parent")).unwrap();
    std::fs::write(
        scratch.join("parent.rs"),
        "mod shipped;\n#[cfg(test)]\nfn helper() {}\nmod also_shipped;\n#[cfg(test)]\npub(crate) mod tests;\n",
    )
    .unwrap();
    for file in ["shipped.rs", "also_shipped.rs", "tests.rs"] {
        std::fs::write(scratch.join("parent").join(file), "").unwrap();
    }
    assert!(!is_cfg_test_module_file(&scratch.join("parent/shipped.rs")));
    assert!(!is_cfg_test_module_file(
        &scratch.join("parent/also_shipped.rs")
    ));
    assert!(is_cfg_test_module_file(&scratch.join("parent/tests.rs")));
    // Sibling `#[path]` declarations: only one carrying cfg(test) excludes the named file.
    std::fs::create_dir_all(scratch.join("flat")).unwrap();
    std::fs::write(
        scratch.join("flat/owner.rs"),
        "#[path = \"shipped_impl.rs\"]\nmod shipped;\n#[cfg(test)]\n#[path = \"owner_tests.rs\"]\nmod tests;\n",
    )
    .unwrap();
    for file in ["shipped_impl.rs", "owner_tests.rs"] {
        std::fs::write(scratch.join("flat").join(file), "").unwrap();
    }
    assert!(!is_cfg_test_module_file(
        &scratch.join("flat/shipped_impl.rs")
    ));
    assert!(is_cfg_test_module_file(
        &scratch.join("flat/owner_tests.rs")
    ));
    // Test configuration propagates to both ordinary and #[path] children, not to
    // an otherwise identical production module's children.
    for parent in ["tests", "shipped"] {
        std::fs::create_dir_all(scratch.join("parent").join(parent)).unwrap();
        std::fs::write(
            scratch.join(format!("parent/{parent}.rs")),
            format!("mod child;\n#[path = \"{parent}_flat_child.rs\"]\nmod renamed;\n"),
        )
        .unwrap();
        for child in [
            format!("parent/{parent}/child.rs"),
            format!("parent/{parent}_flat_child.rs"),
        ] {
            let path = scratch.join(child);
            std::fs::write(&path, "").unwrap();
            assert_eq!(
                is_cfg_test_module_file(&path),
                parent == "tests",
                "{}",
                path.display()
            );
        }
    }
    std::fs::remove_dir_all(&scratch).unwrap();
}

// Directory enumeration may resolve `..` while the manifest-derived root still contains it.
// Normalize separators lexically so the same allowances also work with Windows paths.
fn repo_relative_path(root: &Path, path: &Path) -> String {
    let normalize = |path: &Path| {
        let path = path.to_string_lossy().replace('\\', "/");
        let mut parts = Vec::new();
        for part in path.split('/') {
            match part {
                "" | "." => {}
                ".." => {
                    parts.pop().expect("source path must not escape its root");
                }
                _ => parts.push(part),
            }
        }
        parts.join("/")
    };
    let root = format!("{}/", normalize(root));
    normalize(path)
        .strip_prefix(&root)
        .expect("source path must be inside the repository")
        .to_owned()
}

#[test]
fn deferred_writer_paths_are_normalized_relative_to_repo() {
    for (root, path) in [
        (
            r"D:\a\broca\broca\broca\crates\broca-wal\../..",
            r"D:\a\broca\broca\broca\crates\broca-core\src\consent.rs",
        ),
        (
            r"D:\a\broca\broca\broca",
            r"D:\a\broca\broca\broca\crates\broca-wal\..\..\crates\broca-core\src\consent.rs",
        ),
        (
            "/work/broca/crates/broca-wal/../..",
            "/work/broca/crates/broca-core/./src/consent.rs",
        ),
    ] {
        assert_eq!(
            repo_relative_path(Path::new(root), Path::new(path)),
            "crates/broca-core/src/consent.rs"
        );
    }
}

#[test]
fn no_production_path_writes_deferred_call_records() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(root.join("crates")).unwrap() {
        let src = entry.unwrap().path().join("src");
        if src.is_dir() {
            sources(&src, &mut paths);
        }
    }
    assert!(paths.len() > 100, "scan must reach real production sources");
    // This fixture library is reachable only from cfg(test) or the explicit test-conformance
    // feature, never from a shipped reader. Verify that boundary before omitting its helpers.
    let store_lib = std::fs::read_to_string(root.join("crates/broca-store/src/lib.rs")).unwrap();
    assert!(store_lib
        .contains("#[cfg(any(test, feature = \"test-conformance\"))]\npub mod conformance;"));
    // Core's run loop writes the deferred-call records. Other production files may
    // construct only the specific helper records admitted below.
    let writer = "crates/broca-core/src/run.rs";
    // Consent's authorization helper builds the approval ID records the run loop writes.
    let consent = "crates/broca-core/src/consent.rs";
    // The session actor's restart seal decides how core closes an open Deferred call that still
    // owes a withdrawal: it returns `DeferredCloseKind::WithdrawElicitation(..)` to core, and
    // core writes the record through the session's single writer. Choosing that close kind is
    // the only deferred-call construction the actor may make.
    let actor = "crates/broca-module/src/actor.rs";
    let writes: Vec<_> = paths
        .iter()
        .filter(|path| !is_cfg_test_module_file(path))
        .map(|path| (path, repo_relative_path(&root, path)))
        .filter(|(_, relative)| {
            !relative.starts_with("crates/broca-store/src/conformance/") && relative != writer
        })
        .filter_map(|(path, relative)| {
            let source = std::fs::read_to_string(path).unwrap();
            let mut found = scan_constructors(&source, DEFERRED_VARIANTS, true);
            if relative == consent {
                found.retain(|variant| variant != "AwaitingApproval");
            }
            if relative == actor {
                found.retain(|variant| *variant != "WithdrawElicitation");
            }
            (!found.is_empty()).then(|| format!("{relative}: {found:?}"))
        })
        .collect();
    assert!(
        writes.is_empty(),
        "deferred-call records written outside the core run loop:\n{}",
        writes.join("\n")
    );

    let live = std::fs::read_to_string(root.join("crates/broca-wal/src/lib.rs")).unwrap();
    for variant in DEFERRED_VARIANTS {
        let planted =
            format!("{live}\nfn planted() {{ WalRecord::{variant} {{ call_key: key }}; }}");
        assert_eq!(
            scan_constructors(&planted, DEFERRED_VARIANTS, true),
            [*variant]
        );
    }
    for (variant, field) in [
        ("ToolDispatchIntent", "approval_digest"),
        ("ToolDispatchIntent", "call_key"),
        ("ToolResult", "withdraw_elicitation"),
        ("ToolResult", "provider_held"),
    ] {
        let planted =
            format!("{live}\nfn planted() {{ WalRecord::{variant} {{ {field}: Some(value) }}; }}");
        assert_eq!(
            scan_constructors(&planted, DEFERRED_VARIANTS, true),
            [format!("{variant}.{field}")]
        );
        let shorthand = format!("fn planted() {{ WalRecord::{variant} {{ {field}, }}; }}");
        assert_eq!(
            scan_constructors(&shorthand, DEFERRED_VARIANTS, true),
            [format!("{variant}.{field}")]
        );
        let absent = format!("fn reader() {{ WalRecord::{variant} {{ {field}: None }}; }}");
        assert!(scan_constructors(&absent, DEFERRED_VARIANTS, true).is_empty());
    }
    assert_eq!(
        scan_constructors(
            "fn writer() { DeferredCloseKind::WithdrawElicitation(w); }",
            DEFERRED_VARIANTS,
            true
        ),
        ["WithdrawElicitation"]
    );
    assert!(scan_constructors(
        "match kind { DeferredCloseKind::WithdrawElicitation(w) => Some(w) }",
        DEFERRED_VARIANTS,
        true
    )
    .is_empty());
}
