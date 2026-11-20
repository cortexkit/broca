use super::*;

struct Root {
    path: PathBuf,
    store: FileWalStore,
    session: BindIdentity,
}

impl Root {
    fn new(overhaul: bool) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = broca_test_support::scratch_root().join(format!(
            "writer-id-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let config = if overhaul {
            broca_wal::WalConfig::OVERHAUL
        } else {
            broca_wal::WalConfig::LEGACY
        };
        Self {
            store: FileWalStore::with_config(path.join("wal"), config),
            path,
            session: BindIdentity::new("/writer-id", "runner", "session"),
        }
    }

    fn spawn(&self) -> WalHandle {
        let wal = self.store.open(&self.session).unwrap();
        WalHandle::spawn_in_store(wal, &self.store, &self.session, 1, None).unwrap()
    }

    fn records(&self) -> Vec<broca_wal::FramedRecord> {
        broca_wal::replay_file(self.store.wal_path(&self.session), u64::MAX).unwrap()
    }

    fn markers(&self) -> Vec<PathBuf> {
        match std::fs::read_dir(self.path.join("open-writers")) {
            Ok(entries) => entries.map(|entry| entry.unwrap().path()).collect(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => panic!("{error}"),
        }
    }
}

impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn ordinary() -> WalRecord {
    WalRecord::TurnFinished {
        step_id: broca_protocol::StepId(1),
    }
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn lifecycle_only_ephemeral_overhaul_is_not_retention_evidence() {
    let mut root = Root::new(true);
    root.store.max_read_bytes = 64_000;
    std::fs::write(root.path.join("test-retained-wal-cap"), "8000").unwrap();
    let wal = root
        .store
        .open_with_retention(&root.session, Some(60))
        .unwrap();
    WalHandle::spawn_in_store(wal, &root.store, &root.session, 1, None)
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    let wal = root.store.open(&root.session).unwrap();
    assert_eq!(wal.append_budget_scope(), retained_size::SizeScope::Session);
    assert_eq!(wal.append_budget().unwrap().0, 64_000);
}

#[tokio::test]
async fn overhaul_relocation_refuses_a_writer_record_plus_an_ordinary_record() {
    let root = Root::new(true);
    let handle = root.spawn();
    handle.record(ordinary()).into_result().unwrap();
    handle.shutdown().await.unwrap();
    let mut wal = FileWal::open_with_recovery(
        root.store.wal_path(&root.session),
        broca_wal::WalConfig::OVERHAUL,
        root.store.feature_floor_path(),
    )
    .unwrap()
    .with_feature_floor(root.store.feature_floor_path())
    .with_wal_config(broca_wal::WalConfig::OVERHAUL)
    .unwrap();
    let destination = root.store.ephemeral_wal_path(&root.session);
    assert!(wal
        .relocate_unstarted(&destination)
        .unwrap_err()
        .to_string()
        .contains("started session"));
    assert!(!destination.exists());
    assert_eq!(root.records().len(), 3);
}

#[tokio::test]
async fn clean_close_appends_writer_closed_and_unlinks_marker() {
    let root = Root::new(true);
    let handle = root.spawn();
    assert_eq!(root.markers().len(), 1);
    handle.record(ordinary()).into_result().unwrap();
    handle.shutdown().await.unwrap();
    let records = root.records();
    assert_eq!(records.len(), 3);
    assert!(
        matches!((&records[0].record, &records[2].record), (WalRecord::WriterOpened { writer_id: opened, .. }, WalRecord::WriterClosed { writer_id: closed }) if opened == closed)
    );
    assert!(root.markers().is_empty());
}

#[test]
fn disconnect_closes_like_shutdown() {
    let root = Root::new(true);
    let mut handle = root.spawn();
    handle.record(ordinary()).into_result().unwrap();
    let (replacement, _) = std_mpsc::channel();
    drop(std::mem::replace(&mut handle.tx, replacement));
    handle.join.take().unwrap().join().unwrap();
    let records = root.records();
    assert_eq!(records.len(), 3);
    assert!(matches!(records[2].record, WalRecord::WriterClosed { .. }));
    assert!(root.markers().is_empty());
}

#[test]
fn overhaul_spawn_without_marker_context_refuses_by_name() {
    let root = Root::new(true);
    let error = WalHandle::spawn(root.store.open(&root.session).unwrap(), 1)
        .err()
        .unwrap();
    assert!(error.to_string().contains("writer-id/v1"));
    assert!(root.records().is_empty());
    assert!(root.markers().is_empty());
}

#[test]
fn marker_creation_failure_never_appends_writer_opened() {
    let root = Root::new(true);
    std::fs::write(root.path.join("open-writers"), b"not a directory").unwrap();
    let wal = root.store.open(&root.session).unwrap();
    assert!(WalHandle::spawn_in_store(wal, &root.store, &root.session, 1, None).is_err());
    assert!(root.records().is_empty());
}

#[tokio::test]
async fn legacy_writer_has_no_writer_records_or_markers() {
    let root = Root::new(false);
    let handle = root.spawn();
    assert_eq!(handle.record(ordinary()).into_result().unwrap(), 1);
    handle.shutdown().await.unwrap();
    assert_eq!(root.records().len(), 1);
    assert!(root.markers().is_empty());
}

#[test]
fn writer_records_require_writer_id_and_round_trip() {
    for record in [
        WalRecord::WriterOpened {
            writer_id: [1; 16],
            previous_writer_id: None,
            recovered_through_seq: 0,
            cause: "new".into(),
        },
        WalRecord::WriterClosed { writer_id: [1; 16] },
    ] {
        assert_eq!(record.required_features(), ["writer-id/v1"]);
        assert!(record.is_informational());
        let bytes = broca_wal::framing::encode(1, 1, &record).unwrap();
        assert!(matches!(
            broca_wal::framing::decode_next(&bytes),
            broca_wal::framing::DecodeStep::Record { .. }
        ));
    }
}

#[tokio::test]
async fn published_successor_close_retires_only_verified_predecessor_markers() {
    let root = Root::new(true);
    let writer = root.spawn();
    let previous = root.markers().pop().unwrap();
    let bytes = std::fs::read(&previous).unwrap();
    writer.shutdown().await.unwrap();
    std::fs::write(&previous, bytes).unwrap();
    let invalid = root.path.join("open-writers").join(format!(
        "{}.{}",
        session_addr(&root.session),
        "f".repeat(32)
    ));
    std::fs::write(&invalid, b"{malformed identity").unwrap();
    let next = root.spawn();
    assert!(
        previous.exists(),
        "opening alone must not retire predecessor markers"
    );
    next.shutdown().await.unwrap();
    assert!(
        !previous.exists(),
        "a successfully published newer writer settles its predecessor"
    );
    assert_eq!(
        root.markers(),
        [invalid],
        "untrusted marker contents must remain for diagnosis"
    );
    assert_eq!(root.records().len(), 4);
}
