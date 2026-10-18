use super::*;

fn fixture(tag: &str) -> (PathBuf, BindIdentity, WalRecord) {
    let root =
        broca_test_support::scratch_root().join(format!("class-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let session = BindIdentity::new("/class-test", "runner", tag);
    let mut record: serde_json::Value = serde_json::from_str(include_str!(
        "../../../broca-wal/tests/golden/pre_step_transforms/run_started.json"
    ))
    .unwrap();
    record["session"] = serde_json::to_value(&session).unwrap();
    record["session_class"] = serde_json::json!("head");
    (root, session, serde_json::from_value(record).unwrap())
}

#[test]
fn retention_beats_durable_class() {
    let (root, session, _) = fixture("retention-wins");
    let store = FileWalStore::new(root.join("wal"));
    let wal = store
        .open_with_class(&session, Some(3600), Some("head"))
        .unwrap();
    assert!(store.ephemeral_wal_path(&session).is_file());
    assert!(!store.durable_wal_path(&session).exists());
    assert!(!store.wal_path(&session).exists());
    drop(wal);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn first_send_places_a_preopened_unstarted_wal() {
    let (root, session, record) = fixture("preopened");
    let store = FileWalStore::new(root.join("wal"));
    let mut wal = store.open(&session).unwrap();
    assert!(store.wal_path(&session).is_file());
    wal.append(1, &record, true).unwrap();
    assert!(store.durable_wal_path(&session).is_file());
    assert!(!store.wal_path(&session).exists());
    assert_eq!(store.read_records(&session).unwrap()[0].record, record);
    drop(wal);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn backup_classes_change_never_moves_existing_live_sessions() {
    let (root, durable, record) = fixture("placement-frozen");
    let ordinary = BindIdentity::new("/class-test", "runner", "unclassed");
    let nonbackup = BindIdentity::new("/class-test", "runner", "worker");
    let store = FileWalStore::new(root.join("wal"));
    store
        .open_with_class(&durable, None, Some("head"))
        .unwrap()
        .append(1, &record, true)
        .unwrap();
    drop(store.open_with_class(&ordinary, None, None).unwrap());
    drop(
        store
            .open_with_class(&nonbackup, None, Some("worker"))
            .unwrap(),
    );
    let changed = FileWalStore::new(root.join("wal")).with_backup_classes(vec!["worker".into()]);
    drop(
        changed
            .open_with_class(&durable, None, Some("head"))
            .unwrap(),
    );
    drop(changed.open_with_class(&ordinary, None, None).unwrap());
    drop(
        changed
            .open_with_class(&nonbackup, None, Some("worker"))
            .unwrap(),
    );
    assert!(store.durable_wal_path(&durable).is_file());
    assert!(!store.wal_path(&durable).exists());
    assert!(store.wal_path(&ordinary).is_file());
    assert!(store.wal_path(&nonbackup).is_file());
    assert!(!store.durable_wal_path(&nonbackup).exists());
    let fresh = BindIdentity::new("/class-test", "runner", "new-worker");
    drop(
        changed
            .open_with_class(&fresh, None, Some("worker"))
            .unwrap(),
    );
    assert!(store.durable_wal_path(&fresh).is_file());
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn durable_live_readers_find_session() {
    let (root, session, record) = fixture("durable-reader");
    let store = FileWalStore::new(root.join("wal"));
    store
        .open_with_class(&session, None, Some("head"))
        .unwrap()
        .append(1, &record, true)
        .unwrap();
    let store = Arc::new(FileWalStore::new(root.join("wal")).with_backup_classes(vec![]));
    let address = session_addr(&session);
    assert_eq!(
        store.resolve_live_path(&session).unwrap(),
        Some(store.durable_wal_path(&session))
    );
    assert_eq!(store.read_records(&session).unwrap()[0].record, record);
    assert_eq!(
        store.read_address_records(&address).unwrap()[0].record,
        record
    );
    assert_eq!(store.wal_present(&session), Some(true));
    // File identity exists only where the platform reports one (Unix); off Unix it is absent
    // for every WAL, so a durable WAL must match that, not claim an identity.
    assert_eq!(store.file_identity(&session).unwrap().is_some(), cfg!(unix));
    assert!(store.lineage_exists(&session, None).unwrap());
    assert!(store.addresses().unwrap().contains(&address));
    assert!(store.move_addresses().unwrap().0.contains(&address));
    assert!(store.bytes_for_move(&session).unwrap().is_some());
    let range = store
        .read_records_range_for_authorization(&session, 0, None, None)
        .unwrap();
    assert!(
        matches!(range.records(), broca_wal::WalRangeRecords::Complete(records) if records[0].record == record)
    );
    std::fs::remove_dir_all(root).unwrap();
}
