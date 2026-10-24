//! A credential-store regression must fail without opening anybody's real store.

use std::path::{Path, PathBuf};

#[test]
fn daemon_serve_path_never_resolves_a_vault_handles_file() {
    // Read only named Rust sources, never a credential file. Startup and serving must use
    // grant-backed credential IDs; the developer tools retain their separate handle loader.
    for (path, source) in [
        ("main.rs", include_str!("../../src/main.rs")),
        ("serve.rs", include_str!("../../src/serve.rs")),
        ("config_home.rs", include_str!("../../src/config_home.rs")),
    ] {
        for forbidden in ["BROCA_VAULT_HANDLES_PATH", "vault-handles.json"] {
            assert!(
                !source.contains(forbidden),
                "daemon serve path references legacy vault handles: {path}: {forbidden}"
            );
        }
    }
    // The shared vault module documents the retired file and tests that dev-tools ignore its
    // override. Check production string literals so those explanations are not mistaken for IO.
    let vault_config = include_str!("../../src/vault_config.rs")
        .split_once("\n#[cfg(test)]")
        .expect("vault configuration unit-test boundary")
        .0;
    for forbidden in ["\"BROCA_VAULT_HANDLES_PATH\"", "\"vault-handles.json\""] {
        assert!(
            !vault_config.contains(forbidden),
            "daemon serve path references legacy vault handles: vault_config.rs: {forbidden}"
        );
    }
}

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
            // Repository paths use `/` on every OS, so the message reads the same on Windows.
            let relative = file.strip_prefix(&workspace).unwrap();
            let parts: Vec<String> = relative
                .components()
                .map(|part| part.as_os_str().to_string_lossy().into_owned())
                .collect();
            offenders.push(parts.join("/"));
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
