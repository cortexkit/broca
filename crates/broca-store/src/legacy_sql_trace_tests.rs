use super::*;
use std::cell::RefCell;

thread_local! {
    static STATEMENTS: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

fn record(statement: &str) {
    STATEMENTS.with(|statements| statements.borrow_mut().push(statement.to_owned()));
}

fn trace(store: &SqliteStore, work: impl FnOnce()) -> Vec<String> {
    STATEMENTS.with(|statements| statements.borrow_mut().clear());
    store.conn.lock().unwrap().trace(Some(record));
    work();
    store.conn.lock().unwrap().trace(None);
    STATEMENTS.with(|statements| statements.borrow_mut().drain(..).collect())
}

#[test]
fn legacy_summary_and_projection_sql_match_pre_recovery_master() {
    let root = broca_test_support::scratch_root().join("legacy-sql-statements");
    std::fs::create_dir_all(&root).unwrap();
    let store = SqliteStore::open(root.join("run-index.db"))
        .unwrap()
        .with_wal_config(broca_wal::WalConfig::LEGACY);
    let session = BindIdentity::new("/legacy-project", "harness", "legacy-sql");
    store
        .conn
        .lock()
        .unwrap()
        .execute(
            "INSERT INTO run_index(run_id, session, state) VALUES('legacy-run', ?1, 'active')",
            [serde_json::to_string(&session).unwrap()],
        )
        .unwrap();
    let diagnostics = trace(&store, || {
        store.diagnostic_evidence(10, 20).unwrap();
    });
    let lookup = trace(&store, || {
        assert!(store.lookup_run("legacy-run").unwrap().is_some());
    });
    let projection = trace(&store, || {
        store
            .projection_snapshot(&SqliteStore::key(&session))
            .unwrap();
    });
    let actual =
        serde_json::json!({"diagnostics":diagnostics,"lookup":lookup,"projection":projection});
    println!("LEGACY_SQL_CAPTURE={actual}");
    let pinned: serde_json::Value =
        serde_json::from_str(include_str!("../tests/fixtures/legacy-master-sql.json")).unwrap();
    assert_eq!(
        actual, pinned["statements"],
        "legacy SQL must equal the independently measured pre-recovery master queries"
    );
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}
