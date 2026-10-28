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

fn first_send(root: &Root, retention_value: Option<u64>) -> WalRecord {
    let mut record = crate::replay::projection_boundary_tests::run_started(1, "run").record;
    if let WalRecord::RunStarted {
        session, retention, ..
    } = &mut record
    {
        *session = root.session.clone();
        *retention = retention_value;
    }
    record
}

#[tokio::test]
async fn legacy_first_send_places_a_preopened_unstarted_wal_unchanged() {
    let root = Root::new(false);
    root.spawn().shutdown().await.unwrap();
    let handle = root.spawn();
    assert_eq!(
        handle
            .record(first_send(&root, Some(60)))
            .into_result()
            .unwrap(),
        1
    );
    handle.shutdown().await.unwrap();
    assert!(!root.store.wal_path(&root.session).exists());
    let records =
        broca_wal::replay_file(root.store.ephemeral_wal_path(&root.session), u64::MAX).unwrap();
    assert_eq!(records.len(), 1);
    assert!(matches!(records[0].record, WalRecord::RunStarted { .. }));
    assert!(root.markers().is_empty());
}

#[tokio::test]
async fn overhaul_first_send_places_a_warm_opened_writer_only_log() {
    let root = Root::new(true);
    root.spawn().shutdown().await.unwrap();
    let handle = root.spawn();
    handle
        .record(first_send(&root, Some(60)))
        .into_result()
        .unwrap();
    handle.shutdown().await.unwrap();
    assert!(!root.store.wal_path(&root.session).exists());
    let records =
        broca_wal::replay_file(root.store.ephemeral_wal_path(&root.session), u64::MAX).unwrap();
    assert_eq!(records.len(), 5);
    assert!(matches!(records[3].record, WalRecord::RunStarted { .. }));
    assert!(root.markers().is_empty());
}

#[tokio::test]
async fn overhaul_relocation_refuses_a_writer_record_plus_an_ordinary_record() {
    let root = Root::new(true);
    let handle = root.spawn();
    handle.record(ordinary()).into_result().unwrap();
    handle.shutdown().await.unwrap();
    let mut wal = FileWal::open(root.store.wal_path(&root.session))
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
async fn writer_opened_precedes_every_append() {
    let root = Root::new(true);
    for tenure in 0..2 {
        let handle = root.spawn();
        let records = root.records();
        let opened = records.last().unwrap();
        let WalRecord::WriterOpened {
            writer_id,
            previous_writer_id,
            recovered_through_seq,
            ..
        } = opened.record
        else {
            panic!("first barrier must open the writer")
        };
        assert_eq!(recovered_through_seq, opened.seq - 1);
        assert_eq!(previous_writer_id.is_some(), tenure > 0);
        let expected_id: String = writer_id.iter().map(|byte| format!("{byte:02x}")).collect();
        assert_eq!(
            root.markers(),
            [root
                .path
                .join("open-writers")
                .join(format!("{}.{expected_id}", session_addr(&root.session)))]
        );
        for _ in 0..3 {
            assert!(handle.record(ordinary()).into_result().unwrap() > opened.seq);
        }
        handle.sync().await.unwrap();
        assert_eq!(root.records().len(), records.len() + 3);
        let bytes = std::fs::read(root.store.wal_path(&root.session)).unwrap();
        let mut offset = 0;
        while offset < bytes.len() {
            if bytes[offset + 4] == broca_wal::framing::V4_FRAMING_VERSION {
                let header = broca_wal::framing::v4::decode_header(&bytes[offset..])
                    .unwrap()
                    .unwrap();
                if header.metadata.seq >= opened.seq {
                    assert_eq!(header.metadata.writer_id, writer_id);
                }
                offset += header.frame_len();
            } else {
                offset += broca_wal::framing::LINEAGE_FRAME_LEN;
            }
        }
        handle.shutdown().await.unwrap();
    }
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
async fn lifecycle_observation_follows_durability_and_precedes_marker_unlink() {
    let root = Root::new(true);
    let wal = root.store.open(&root.session).unwrap();
    let (wal, seq) = WalHandle::prove_recovery(wal).unwrap();
    let (tx, _) = broadcast::channel(8);
    let healthy = Arc::new(AtomicBool::new(true));
    let seen = Arc::new(AtomicU64::new(0));
    let observer = RecordObserver::new(tx, Arc::new(AtomicUsize::new(0))).with_durable_tap(
        {
            let path = root.path.clone();
            let log = root.store.wal_path(&root.session);
            let seen = seen.clone();
            Arc::new(move |seq, _, _, record| {
                assert_eq!(
                    std::fs::read_dir(path.join("open-writers"))
                        .unwrap()
                        .count(),
                    1
                );
                let replay = broca_wal::replay_file(&log, u64::MAX).unwrap();
                assert_eq!(&replay.last().unwrap().record, record);
                assert_eq!(replay.last().unwrap().seq, seq);
                seen.fetch_add(1, Ordering::Relaxed);
            })
        },
        healthy.clone(),
    );
    let handle = WalHandle::spawn_proven_with_clock(
        wal,
        seq,
        1,
        Some(observer),
        None,
        (
            StampClock::wall(),
            Some(open_writers::WriterContext {
                root: root.path.clone(),
                address: session_addr(&root.session),
            }),
        ),
    )
    .unwrap();
    assert_eq!(seen.load(Ordering::Relaxed), 1);
    handle.shutdown().await.unwrap();
    assert!(healthy.load(Ordering::Acquire));
    assert_eq!(seen.load(Ordering::Relaxed), 2);
    assert!(root.markers().is_empty());
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
