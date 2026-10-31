use super::*;
use broca_core::SessionRefIndex;
use std::sync::{Arc, Barrier};

fn complete(store: &SqliteStore) {
    store.conn.lock().unwrap().execute_batch(
        "INSERT INTO agent_data_projection VALUES(1,1); INSERT INTO agent_data_census_done VALUES(1)",
    ).unwrap();
}

fn bind(root: &str) -> BindIdentity {
    BindIdentity::new(root, "athena", "seat-1")
}

#[test]
fn existing_unscoped_pair_keeps_legacy_root_after_rebind_and_scope_adoption() {
    let store = SqliteStore::open_in_memory().unwrap();
    complete(&store);
    let legacy = bind("/legacy/checkout");
    store
        .record_session(&broca_core::session_addr(&legacy), &legacy)
        .unwrap();
    for workdir in [None, Some(Path::new("/new/worktree"))] {
        let resolved = store
            .resolve_session_address(&bind("/different/route"), workdir)
            .unwrap();
        assert_eq!(resolved, legacy);
        assert_eq!(
            serde_json::to_string(&resolved).unwrap(),
            r#"{"project_root":"/legacy/checkout","harness":"athena","session":"seat-1"}"#
        );
    }
}

#[test]
fn new_scoped_pair_uses_workdir_but_new_unscoped_pair_retains_route_root() {
    for (workdir, expected) in [
        (Some(Path::new("/stamped/worktree")), "/stamped/worktree"),
        (None, "/route/checkout"),
    ] {
        let store = SqliteStore::open_in_memory().unwrap();
        complete(&store);
        assert_eq!(
            store
                .resolve_session_address(&bind("/route/checkout"), workdir)
                .unwrap(),
            bind(expected)
        );
        assert_eq!(
            store
                .resolve_session_address(&bind("/later/route"), Some(Path::new("/later/stamp")))
                .unwrap(),
            bind(expected)
        );
    }
}

#[test]
fn ambiguous_pair_refuses_without_inventory_writes() {
    let store = SqliteStore::open_in_memory().unwrap();
    complete(&store);
    for root in ["/old/one", "/old/two"] {
        let identity = bind(root);
        store
            .record_session(&broca_core::session_addr(&identity), &identity)
            .unwrap();
    }
    let before = store.conn.lock().unwrap().total_changes();
    let error = store
        .resolve_session_address(&bind("/old/one"), Some(Path::new("/old/one")))
        .unwrap_err();
    assert_eq!(error, SessionAddressError::Ambiguous);
    assert_eq!(error.code(), "session_address_ambiguous");
    assert_eq!(store.conn.lock().unwrap().total_changes(), before);
}

#[test]
fn incomplete_inventory_never_proves_absence() {
    let store = SqliteStore::open_in_memory().unwrap();
    let before = store.conn.lock().unwrap().total_changes();
    let error = store
        .resolve_session_address(&bind("/new/route"), None)
        .unwrap_err();
    assert!(matches!(error, SessionAddressError::Unavailable(_)));
    assert_eq!(error.code(), "session_address_unavailable");
    assert_eq!(store.conn.lock().unwrap().total_changes(), before);
}

#[test]
fn recorded_pair_resolves_with_incomplete_inventory_and_a_gap_marker() {
    let root = broca_test_support::scratch_root()
        .join(format!("address-known-gap-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let store = SqliteStore::open(root.join("run-index.db")).unwrap();
    let recorded = bind("/recorded/checkout");
    store
        .record_session(&broca_core::session_addr(&recorded), &recorded)
        .unwrap();
    assert_eq!(
        store
            .resolve_session_address(&bind("/later/route"), None)
            .unwrap(),
        recorded
    );
    std::fs::write(
        root.join(broca_core::wal_writer::SESSION_PROJECTION_INCOMPLETE),
        b"",
    )
    .unwrap();
    assert_eq!(
        store
            .resolve_session_address(&bind("/later/route"), None)
            .unwrap(),
        recorded
    );
    let other = bind("/other/checkout");
    store
        .record_session(&broca_core::session_addr(&other), &other)
        .unwrap();
    assert_eq!(
        store.resolve_session_address(&bind("/later/route"), None),
        Err(SessionAddressError::Ambiguous)
    );
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn a_filesystem_projection_gap_refuses_even_with_complete_sqlite_markers() {
    let root =
        broca_test_support::scratch_root().join(format!("address-gap-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let store = SqliteStore::open(root.join("run-index.db")).unwrap();
    complete(&store);
    std::fs::write(
        root.join(broca_core::wal_writer::SESSION_PROJECTION_INCOMPLETE),
        b"",
    )
    .unwrap();
    let before = store.conn.lock().unwrap().total_changes();
    assert!(matches!(
        store.resolve_session_address(&bind("/new/route"), None),
        Err(SessionAddressError::Unavailable(_))
    ));
    assert_eq!(store.conn.lock().unwrap().total_changes(), before);
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn only_the_resolved_root_is_validated_as_a_storage_key() {
    let store = SqliteStore::open_in_memory().unwrap();
    complete(&store);
    let invalid_claim = bind("/route\u{1f}root");
    assert_eq!(
        store
            .resolve_session_address(&invalid_claim, Some(Path::new("/worktree")))
            .unwrap(),
        bind("/worktree")
    );
    let invalid_unscoped = BindIdentity::new("/route\u{1f}root", "athena", "invalid-unscoped");
    let before = store.conn.lock().unwrap().total_changes();
    assert!(matches!(
        store.resolve_session_address(&invalid_unscoped, None),
        Err(SessionAddressError::Invalid(_))
    ));
    assert_eq!(store.conn.lock().unwrap().total_changes(), before);
}

#[test]
fn first_bind_reservation_survives_restart_without_a_wal_append() {
    let root = broca_test_support::scratch_root()
        .join(format!("address-reservation-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let db = root.join("run-index.db");
    {
        let store = SqliteStore::open(&db).unwrap();
        complete(&store);
        assert_eq!(
            store
                .resolve_session_address(&bind("/first/route"), None)
                .unwrap(),
            bind("/first/route")
        );
    }
    let store = SqliteStore::open(&db).unwrap();
    assert_eq!(
        store
            .resolve_session_address(&bind("/later/route"), None)
            .unwrap(),
        bind("/first/route")
    );
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn concurrent_first_binds_reserve_one_root_across_sqlite_connections() {
    let root = broca_test_support::scratch_root()
        .join(format!("address-concurrent-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let db = root.join("run-index.db");
    let first = SqliteStore::open(&db).unwrap();
    complete(&first);
    let second = SqliteStore::open(&db).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let threads: Vec<_> = [first, second]
        .into_iter()
        .enumerate()
        .map(|(index, store)| {
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                store
                    .resolve_session_address(
                        &bind("/route"),
                        Some(Path::new(if index == 0 {
                            "/worktree/one"
                        } else {
                            "/worktree/two"
                        })),
                    )
                    .unwrap()
            })
        })
        .collect();
    let resolved: Vec<_> = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect();
    assert_eq!(resolved[0], resolved[1]);
    let store = SqliteStore::open(&db).unwrap();
    assert_eq!(
        store
            .session_refs_named("seat-1", 3)
            .unwrap()
            .sessions
            .len(),
        1
    );
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn existing_pair_bind_succeeds_while_another_connection_holds_the_write_lock() {
    let root = broca_test_support::scratch_root()
        .join(format!("address-read-only-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let database = root.join("run-index.db");
    let store = SqliteStore::open(&database).unwrap();
    complete(&store);
    let legacy = bind("/original/checkout");
    store
        .record_session(&broca_core::session_addr(&legacy), &legacy)
        .unwrap();
    // Any attempt to acquire a write transaction fails immediately, rather than
    // turning a write-lock regression into a slow timing-dependent test.
    store
        .conn
        .lock()
        .unwrap()
        .busy_timeout(std::time::Duration::ZERO)
        .unwrap();
    let mut writer = rusqlite::Connection::open(&database).unwrap();
    let held = writer
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .unwrap();
    held.execute(
        "UPDATE agent_data_projection SET backfilled=backfilled WHERE id=1",
        [],
    )
    .unwrap();
    let before = store.conn.lock().unwrap().total_changes();
    assert_eq!(
        store
            .resolve_session_address(&bind("/later/route"), None)
            .unwrap(),
        legacy
    );
    assert_eq!(store.conn.lock().unwrap().total_changes(), before);
    let absent = BindIdentity::new("/new/checkout", "athena", "new-seat");
    assert!(matches!(
        store.resolve_session_address(&absent, None),
        Err(SessionAddressError::Unavailable(
            InventoryUnavailable::StoreError(_)
        ))
    ));
    drop(held);
    drop(writer);
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}
