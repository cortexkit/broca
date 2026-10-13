//! The operator path must retain its 14-day candidate set after identity loading moves.
use broca_core::{session_addr, FileWalStore, WalStore};
use broca_protocol::{BindIdentity, RunFinishReason};

#[test]
fn operator_cli_default_remains_fourteen_days_with_store_identities() {
    let root = broca_test_support::scratch_root()
        .join(format!("broca-cli-default-fold-{}", std::process::id()));
    std::fs::create_dir_all(root.join("wal")).unwrap();
    let db = root.join("run-index.db");
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute_batch("CREATE TABLE meta (session TEXT);")
        .unwrap();
    let wals = FileWalStore::new(root.join("wal"));
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let mut cold = Vec::new();
    for days in [13, 15, 29, 31] {
        let session = BindIdentity::new("/cli-fold", "runner", days.to_string());
        conn.execute(
            "INSERT INTO meta VALUES (?1)",
            [format!("/cli-fold\u{1f}runner\u{1f}{days}")],
        )
        .unwrap();
        let mut wal = wals.open(&session).unwrap();
        wal.append(
            1,
            &broca_wal::WalRecord::RunFinished {
                reason: RunFinishReason::Completed,
                ts_ms: Some(now - days * 86_400_000),
                usage: Default::default(),
                error: None,
                retries_used: 0,
            },
            true,
        )
        .unwrap();
        if days >= 14 {
            cold.push(session_addr(&session));
        }
    }
    // Invalid rows remain ignored just as in the original private CLI loader.
    conn.execute("INSERT INTO meta VALUES ('malformed')", [])
        .unwrap();
    drop(conn);
    let before = std::fs::read(&db).unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_store-maintenance"))
        .args(["wal-fold", root.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    cold.sort();
    assert_eq!(report["candidates"], serde_json::json!(cold));
    assert_eq!(report["resolved_from_store"], report["candidates"]);
    assert_eq!(report["removed"], 0);
    assert_eq!(
        std::fs::read(&db).unwrap(),
        before,
        "preview must not migrate or write the database"
    );
    assert_eq!(std::fs::read_dir(root.join("wal")).unwrap().count(), 4);
    assert!(!root.join("wal-archive").exists());
    std::fs::remove_dir_all(root).unwrap();
}
