//! A credential-store regression must fail without opening anybody's real store.

use std::path::{Path, PathBuf};

fn source_files(dir: &Path, files: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("read source directory") {
        let entry = entry.expect("read source entry");
        let path = entry.path();
        let kind = entry.file_type().expect("read source entry type");
        if kind.is_dir() {
            if !matches!(
                entry.file_name().to_str(),
                Some("target" | "node_modules" | ".git")
            ) {
                source_files(&path, files);
            }
        } else if kind.is_file() {
            files.push(path);
        }
    }
}

#[test]
fn shipped_sources_never_name_another_apps_credential_store() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut files = Vec::new();
    for dir in ["crates", "scripts"] {
        source_files(&workspace.join(dir), &mut files);
    }
    // Split the needle so the scanner checks its own source too, without an exception.
    let forbidden = ["opencode", "/auth.json"].concat();
    let mut offenders = Vec::new();
    for file in &files {
        let bytes = std::fs::read(file).expect("read source file");
        if bytes
            .windows(forbidden.len())
            .any(|window| window == forbidden.as_bytes())
        {
            offenders.push(file.strip_prefix(&workspace).unwrap().display().to_string());
        }
    }
    for expected in [
        "crates/broca-catalog/src/live.rs",
        "crates/broca-provider/tests/live_common/mod.rs",
        "crates/broca-run/src/main.rs",
        "scripts/quickcheck.sh",
    ] {
        assert!(
            files.iter().any(|file| file == &workspace.join(expected)),
            "scan missed {expected}"
        );
    }
    assert!(
        files.len() > 100,
        "credential source scan reached too few files"
    );
    assert!(
        offenders.is_empty(),
        "forbidden credential-store path in: {}",
        offenders.join(", ")
    );
}
