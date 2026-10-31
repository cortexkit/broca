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
    // Durable WALs expose the same native file identity as other live WALs.
    assert_eq!(
        store.file_identity(&session).unwrap().is_some(),
        cfg!(any(unix, windows))
    );
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

#[cfg(any(unix, windows))]
#[test]
fn native_file_identity_distinguishes_aliases_from_equal_bytes() {
    let (root, _, _) = fixture("native-alias-identity");
    let original = root.join("original.wal");
    let alias = root.join("alias.wal");
    let copy = root.join("copy.wal");
    std::fs::write(&original, b"identical bytes").unwrap();
    std::fs::hard_link(&original, &alias).unwrap();
    std::fs::copy(&original, &copy).unwrap();
    assert!(broca_wal::file_identity(&original).unwrap().is_some());
    assert!(same_wal_file(&original, &alias).unwrap());
    assert!(!same_wal_file(&original, &copy).unwrap());
    assert!(matches!(
        same_wal_file(&original, &root.join("missing.wal")),
        Err(WalError::Io(error)) if error.kind() == io::ErrorKind::NotFound
    ));
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn missing_native_identity_never_establishes_aliasing() {
    assert!(!same_native_file_identity(None, None));
    assert!(!same_native_file_identity(Some((1, 2)), None));
    assert!(!same_native_file_identity(None, Some((1, 2))));
    assert!(!same_native_file_identity(Some((1, 2)), Some((2, 2))));
    assert!(!same_native_file_identity(Some((1, 2)), Some((1, 3))));
    assert!(same_native_file_identity(Some((1, 2)), Some((1, 2))));
}

#[cfg(any(unix, windows))]
fn linked_overhaul_wal(tag: &str) -> (PathBuf, BindIdentity, FileWalStore, Vec<u8>) {
    let (root, session, _) = fixture(tag);
    std::fs::write(
        root.join(broca_wal::state_format::MARKER_FILENAME),
        b"broca-state/2\n",
    )
    .unwrap();
    let store = FileWalStore::with_config(root.join("wal"), broca_wal::WalConfig::OVERHAUL);
    drop(store.open(&session).unwrap());
    let source = store.wal_path(&session);
    let alias = store.ephemeral_wal_path(&session);
    std::fs::create_dir_all(alias.parent().unwrap()).unwrap();
    std::fs::hard_link(&source, &alias).unwrap();
    let payload = std::fs::read(source).unwrap();
    (root, session, store, payload)
}

#[cfg(any(unix, windows))]
#[test]
fn overhaul_alias_discovery_is_transient_and_preserves_both_names() {
    let (root, session, store, payload) = linked_overhaul_wal("alias-discovery");
    let source = store.wal_path(&session);
    let alias = store.ephemeral_wal_path(&session);
    assert!(matches!(
        store.resolve_live_path(&session),
        Err(WalError::RelocationIncomplete { paths, cause: None })
            if paths == vec![source.clone(), alias.clone()]
    ));
    assert_eq!(std::fs::read(source).unwrap(), payload);
    assert_eq!(std::fs::read(alias).unwrap(), payload);
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(any(unix, windows))]
#[test]
fn owned_open_recovers_cut_link_unlink_placement_without_changing_bytes() {
    use crate::lease::{lease_key_for, FileLeaseStore, LeaseStore};

    for (tag, retention, class) in [
        ("alias-repair-ordinary", None, None),
        ("alias-repair-retained", Some(3600), None),
        ("alias-repair-durable", None, Some("head")),
        ("alias-repair-retention-wins", Some(3600), Some("head")),
    ] {
        let (root, session, store, payload) = linked_overhaul_wal(tag);
        let source = store.wal_path(&session);
        let ephemeral = store.ephemeral_wal_path(&session);
        let durable = store.durable_wal_path(&session);
        let destination = if retention.is_some() {
            &ephemeral
        } else if class.is_some() {
            &durable
        } else {
            &source
        };
        let leases = FileLeaseStore::new(root.join("leases"));
        let own = leases.acquire(&lease_key_for(&session)).unwrap();
        let epoch = own.epoch();
        let (wal, lease) = store
            .open_owned_writer(&session, retention, class, own)
            .unwrap();
        assert_eq!(lease.epoch(), epoch);
        assert_eq!(wal.last_seq(), 0);
        assert_eq!(
            store.resolve_live_path(&session).unwrap().as_ref(),
            Some(destination)
        );
        assert_eq!(std::fs::read(destination).unwrap(), payload);
        for path in [&source, &ephemeral, &durable] {
            assert_eq!(path.exists(), path == destination);
        }
        drop(wal);
        drop(lease);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(any(unix, windows))]
#[test]
fn retry_live_identity_rejects_disappearance_after_the_metadata_check() {
    let (root, session, _) = fixture("retry-identity-disappears");
    let store = FileWalStore::new(root.join("wal"));
    drop(store.open(&session).unwrap());
    let path = store.wal_path(&session);
    assert!(store.file_identity(&session).unwrap().is_some());
    let removed = path.clone();
    AFTER_RETRY_LIVE_STAT.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || std::fs::remove_file(removed).unwrap()));
    });
    let result = store.retry_live_identity(&session);
    assert!(!path.exists(), "the disappearance interleaving must run");
    assert!(matches!(
        result,
        Err(WalError::Io(error)) if error.kind() == io::ErrorKind::NotFound
    ));
    std::fs::remove_dir_all(root).unwrap();
}
