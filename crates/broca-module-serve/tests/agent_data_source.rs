//! A returning Mac's old session is hidden behind its moved tombstone. Ordinary
//! readers must refuse that copy; only the move participant may inspect it.
use std::path::{Path, PathBuf};

fn rust_files(dir: &Path, files: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            rust_files(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
}

#[test]
fn only_agent_data_can_call_the_move_address_reader() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let allowed = root.join("crates/broca-module-serve/src/agent_data.rs");
    let mut files = Vec::new();
    rust_files(&root.join("crates"), &mut files);
    let mut violations = Vec::new();
    for file in files {
        if file == allowed {
            continue;
        }
        let text = std::fs::read_to_string(&file).unwrap();
        for line in bypass_identifier_lines(
            &text,
            file == root.join("crates/broca-core/src/wal_writer.rs"),
        ) {
            violations.push(format!(
                "{}:{line}: move-only WAL reader identifier",
                file.strip_prefix(&root).unwrap().display()
            ));
        }
    }
    assert!(
        violations.is_empty(),
        "move-only WAL reader called outside agent-data:\n{}",
        violations.join("\n")
    );
}

fn bypass_identifier_lines(source: &str, definition: bool) -> Vec<usize> {
    let identifier = concat!("read_address_", "records_for_move");
    let word = |c: char| c.is_alphanumeric() || c == '_';
    let mut violations = Vec::new();
    // Do not require call punctuation: imports, aliases and function items can
    // all grant access. Conservatively reserve this identifier's text too.
    for (index, line) in source.lines().enumerate() {
        for (offset, _) in line.match_indices(identifier) {
            let prefix = &line[..offset];
            let suffix = &line[offset + identifier.len()..];
            if prefix.chars().next_back().is_some_and(word)
                || suffix.chars().next().is_some_and(word)
            {
                continue;
            }
            if definition && prefix.split_whitespace().last() == Some("fn") {
                continue;
            }
            violations.push(index + 1);
        }
    }
    violations
}

#[test]
fn bypass_fence_catches_function_items_imports_and_method_references() {
    let api = concat!("read_address_", "records_for_move");
    for source in [
        format!("let reader = broca_core::FileWalStore::{api};\nlet _ = reader(wal, \"0123456789abcdef\");"),
        format!("use crate::elsewhere::{api} as reader;"),
        format!("let reference = FileWalStore::{api};"),
        format!("wal.{api}(\"0123456789abcdef\");"),
    ] {
        assert_eq!(bypass_identifier_lines(&source,false),vec![1],"missed identifier use: {source}");
    }
    assert!(bypass_identifier_lines(&format!("let {api}_other = 1;"), false).is_empty());
}
