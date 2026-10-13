//! Scope stamps carry both automation identity and provider delegation attributes. Broca may
//! compare an agent identity for safe tool rebind, or freeze it with a stamped agent-run ID;
//! it must never use delegation to drive an ordinary owner-agent session.
//!
//! Attribute reads are confined to two adapters: `same_agent` compares `agent_id` on a tool
//! rebind, and `to_bind_scope` copies `flow_id`, `run_id`, and `agent_id` (only with `run_id`).
//! Frozen `SessionScope.agent_id` is not a stamp attribute: WAL readers and exports need it.
//! Any other stamp-attribute access, including wholesale attribute cloning, and every read
//! of `delegates` stays forbidden. Behavioral scope tests also pin that an ordinary stamp's
//! agent/delegation attributes do not grant ownership.

use std::path::{Path, PathBuf};

fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

/// The code on a line, without a trailing `//` comment. A `//` inside a string literal also
/// cuts the line, which can only hide text, never invent a match.
fn code_of(line: &str) -> &str {
    line.split_once("//").map_or(line, |(code, _)| code)
}

/// Whether `code` names `word` as a whole identifier.
fn names(code: &str, word: &str) -> bool {
    code.match_indices(word).any(|(at, _)| {
        let before = code[..at].chars().next_back();
        let after = code[at + word.len()..].chars().next();
        let ident = |c: Option<char>| c.is_some_and(|c| c.is_alphanumeric() || c == '_');
        !ident(before) && !ident(after)
    })
}

#[test]
fn broca_reads_stamp_identity_only_in_scope_adapters() {
    let crates = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let mut files = Vec::new();
    for entry in std::fs::read_dir(&crates).unwrap().flatten() {
        // broca-owner-only depends only on windows-sys, so it cannot reach a scope stamp.
        // Its `.attributes()` calls build Win32 SECURITY_ATTRIBUTES, not stamp attributes.
        if entry.file_name() == "broca-owner-only" {
            let manifest = std::fs::read_to_string(entry.path().join("Cargo.toml")).unwrap();
            // A dependency line starts with the crate name; `name = "broca-owner-only"` does not.
            assert!(
                !manifest
                    .lines()
                    .map(str::trim_start)
                    .any(|line| { line.starts_with("subc") || line.starts_with("broca-") }),
                "broca-owner-only gained a dependency that can carry scope stamps; scan it again"
            );
            continue;
        }
        rust_sources(&entry.path().join("src"), &mut files);
    }
    assert!(
        files.len() > 100,
        "the scan must cover the workspace's sources, found {}",
        files.len()
    );
    let mut offenders = Vec::new();
    let mut allowed_reads = 0;
    for file in &files {
        let text = std::fs::read_to_string(file).unwrap();
        let allowed = allowed_lines(file, &text);
        let flow_allowed = function_lines(
            file,
            &text,
            "broca-module-serve/src/serve.rs",
            "to_bind_scope",
        );
        for (number, line) in text.lines().enumerate() {
            let mut code = code_of(line).to_owned();
            if allowed.contains(&number) && code.contains(ALLOWED_READ) {
                // The one permitted field access; anything else on the line is still checked.
                code = code.replace(ALLOWED_READ, "");
                allowed_reads += 1;
            }
            if flow_allowed.contains(&number) {
                code = code
                    .replace(".attributes.flow_id", "")
                    .replace(".attributes.run_id", "")
                    .replace(".attributes.agent_id", "");
            }
            let hit = names(&code, "delegates") || code.contains(".attributes");
            if hit {
                offenders.push(format!(
                    "{}:{}: {}",
                    file.display(),
                    number + 1,
                    line.trim()
                ));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "broca must not read stamp delegation or attributes outside the identity adapters \
         `{ALLOWED_FUNCTION}` and `to_bind_scope`:\n{}",
        offenders.join("\n")
    );
    assert!(
        allowed_reads > 0,
        "the allowlisted `{ALLOWED_FUNCTION}` no longer reads `{ALLOWED_READ}`; if the rebind \
         check moved, move the allowlist with it"
    );
}

/// The allowlist: the one function permitted to read the scope stamp's `agent_id`, the file it
/// lives in, and the exact field access it may make.
const ALLOWED_FILE: &str = "broca-subc/src/subc_plane.rs";
const ALLOWED_FUNCTION: &str = "same_agent";
const ALLOWED_READ: &str = ".attributes.agent_id";

/// The 0-based line numbers of `ALLOWED_FUNCTION`'s signature and body in `file`, empty for any
/// other file. The function is a free function, so its body ends at the first line that is a
/// lone closing brace in the first column.
fn allowed_lines(file: &Path, text: &str) -> std::collections::HashSet<usize> {
    function_lines(file, text, ALLOWED_FILE, ALLOWED_FUNCTION)
}

fn function_lines(
    file: &Path,
    text: &str,
    allowed_file: &str,
    allowed_function: &str,
) -> std::collections::HashSet<usize> {
    let mut lines = std::collections::HashSet::new();
    if !file
        .to_string_lossy()
        .replace('\\', "/")
        .ends_with(allowed_file)
    {
        return lines;
    }
    let signature = format!("fn {allowed_function}(");
    let mut inside = false;
    for (number, line) in text.lines().enumerate() {
        if !inside && code_of(line).trim_start().starts_with(&signature) {
            inside = true;
        }
        if inside {
            lines.insert(number);
            if line == "}" {
                break;
            }
        }
    }
    lines
}

#[test]
fn the_source_rule_sees_a_read_and_ignores_comments() {
    assert!(names(
        code_of("let id = stamp.attributes.agent_id;"),
        "agent_id"
    ));
    assert!(names(code_of("if attrs.delegates {"), "delegates"));
    assert!(!names(code_of("// broca never reads agent_id"), "agent_id"));
    assert!(!names(code_of("let agent_identity = 1;"), "agent_id"));
}

/// The allowlist permits `.attributes.agent_id` only inside `same_agent`. The same read
/// anywhere else in the file fails the check, and so does reading `delegates`, or any other
/// part of `.attributes`, inside the function.
#[test]
fn the_allowlist_admits_only_the_agent_equality_read() {
    let file = Path::new("crates").join(ALLOWED_FILE);
    let text = "fn other() {\n    let x = a.attributes.agent_id;\n}\n\
                fn same_agent(a: &S, b: &S) -> bool {\n\
                \x20   a.attributes.agent_id == b.attributes.agent_id && a.attributes.delegates\n\
                }\n";
    let allowed = allowed_lines(&file, text);
    assert_eq!(allowed, [3, 4, 5].into_iter().collect());
    let body = code_of(text.lines().nth(4).unwrap()).replace(ALLOWED_READ, "");
    assert!(
        names(&body, "delegates"),
        "a delegates read inside the function is still caught"
    );
    assert!(
        !allowed_lines(Path::new("crates/broca-core/src/run.rs"), text).contains(&4),
        "the allowlist names one file"
    );
}
