use std::process::Command;

#[test]
fn repair_help_requires_stopped_supervisor_and_describes_preview() {
    let output = Command::new(env!("CARGO_BIN_EXE_store-maintenance"))
        .args(["repair-session-root", "--help"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stderr).unwrap();
    assert!(help.contains("Preview by default"));
    assert!(help.contains("broca AND its supervisor STOPPED"));
    assert!(help.contains("both session leases"));
    assert!(help.contains("No log bytes are changed"));
}

#[test]
fn repair_write_unknown_liveness_refuses_before_sqlite_open() {
    let root = broca_test_support::scratch_root()
        .join(format!("repair-cli-liveness-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let database = root.join("run-index.db");
    std::fs::write(&database, []).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_store-maintenance"))
        .arg("repair-session-root")
        .arg(&root)
        .args([
            "--harness",
            "broca",
            "--session",
            "cktui-dogfood-1",
            "--write",
        ])
        .env("PATH", "")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(3));
    assert!(String::from_utf8_lossy(&output.stderr)
        .contains("liveness unknown; refusing offline mutation"));
    assert_eq!(std::fs::read(database).unwrap(), b"");
    assert!(!root.join("run-index.db-wal").exists());
    assert!(!root.join("leases").exists());
}

#[test]
fn repair_cli_defaults_to_read_only_preview_even_without_python() {
    use broca_core::SessionRefIndex;
    let root = broca_test_support::scratch_root()
        .join(format!("repair-cli-preview-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let database = root.join("run-index.db");
    let store = broca_store::SqliteStore::open(&database).unwrap();
    let identity = broca_protocol::BindIdentity::new("/root", "broca", "session");
    store
        .record_session(&broca_core::session_addr(&identity), &identity)
        .unwrap();
    broca_store::session_refs::migrate(&database, &broca_core::ReadOnlyWalReader::new(&root))
        .unwrap();
    drop(store);
    let bytes = std::fs::read(&database).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_store-maintenance"))
        .arg("repair-session-root")
        .arg(&root)
        .args(["--harness", "broca", "--session", "session"])
        .env("PATH", "")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["mode"], "preview");
    assert_eq!(
        report["plan"]["surviving_identity"]["project_root"],
        "/root"
    );
    assert_eq!(report["plan"]["stale_identity"], serde_json::Value::Null);
    assert_eq!(std::fs::read(database).unwrap(), bytes);
    assert!(!root.join("leases").exists());
}
