use std::path::PathBuf;
use std::process::Command;

#[test]
fn offline_maintenance_liveness() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let python = if cfg!(windows) { "python" } else { "python3" };
    let output = Command::new(python)
        .args(["-B", "scripts/lib/test_liveness.py"])
        .current_dir(&root)
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .env(
            "STORE_MAINTENANCE_BIN",
            env!("CARGO_BIN_EXE_store-maintenance"),
        )
        .output()
        .expect("Python is required for the shared maintenance liveness suite");
    assert!(
        output.status.success(),
        "shared liveness suite failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("Ran 16 tests"));
}

#[test]
fn manifest_cli_unknown_is_distinct_from_input_errors_and_does_not_initialise_sqlite() {
    let root = broca_test_support::scratch_root()
        .join(format!("manifest liveness cli {}", std::process::id()));
    std::fs::create_dir_all(root.join("wal")).unwrap();
    let database = root.join("run-index.db");
    let manifest = root.join("wal-manifest");
    std::fs::write(&database, []).unwrap();
    std::fs::write(&manifest, b"existing mapping\n").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_store-maintenance"))
        .arg("wal-manifest")
        .arg(&root)
        .arg("--write")
        .env("PATH", "")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(3));
    assert!(String::from_utf8_lossy(&output.stderr)
        .contains("liveness unknown; refusing offline mutation:"));
    assert_eq!(std::fs::read(&database).unwrap(), b"");
    assert_eq!(std::fs::read(&manifest).unwrap(), b"existing mapping\n");
    assert!(!root.join("run-index.db-wal").exists());
    assert!(!root.join("run-index.db-shm").exists());
    std::fs::remove_file(&database).unwrap();
    let invalid = Command::new(env!("CARGO_BIN_EXE_store-maintenance"))
        .arg("wal-manifest")
        .arg(&root)
        .arg("--write")
        .env("PATH", "")
        .output()
        .unwrap();
    assert_eq!(invalid.status.code(), Some(2));
    assert!(!String::from_utf8_lossy(&invalid.stderr).contains("liveness unknown;"));
    std::fs::remove_dir_all(root).unwrap();
}
