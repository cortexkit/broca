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
    let call = concat!("read_address_", "records_for_move", "(");
    let mut violations = Vec::new();
    for file in files {
        if file == allowed {
            continue;
        }
        let text = std::fs::read_to_string(&file).unwrap();
        let mut compact = String::new();
        let mut lines = Vec::new();
        for (index, line) in text.lines().enumerate() {
            if line.trim().starts_with("//") {
                continue;
            }
            for character in line.chars().filter(|c| !c.is_whitespace()) {
                compact.push(character);
                lines.extend(std::iter::repeat_n(index + 1, character.len_utf8()));
            }
        }
        for (offset, _) in compact.match_indices(call) {
            if file == root.join("crates/broca-core/src/wal_writer.rs")
                && compact[..offset].ends_with("fn")
            {
                continue;
            }
            violations.push(format!(
                "{}:{}: move-only WAL reader call",
                file.strip_prefix(&root).unwrap().display(),
                lines[offset]
            ));
        }
    }
    assert!(
        violations.is_empty(),
        "move-only WAL reader called outside agent-data:\n{}",
        violations.join("\n")
    );
}
