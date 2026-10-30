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

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn warm_ephemeral_overhaul_first_send_without_retention_agrees_on_session_cap() {
    let mut root = Root::new(true);
    root.store.max_read_bytes = 64_000;
    std::fs::write(root.path.join("test-retained-wal-cap"), "8000").unwrap();
    let wal = root
        .store
        .open_with_retention(&root.session, Some(60))
        .unwrap();
    let handle = WalHandle::spawn_in_store(wal, &root.store, &root.session, 1, None).unwrap();
    assert!(root.store.ephemeral_wal_path(&root.session).exists());
    let mut send = first_send(&root, None);
    if let WalRecord::RunStarted { title, .. } = &mut send {
        *title = Some("x".repeat(16_000));
    }
    handle.record(send).into_result().unwrap();
    handle
        .sync()
        .await
        .expect("first send must not poison the writer under the retained cap");
    let (tx, rx) = std_mpsc::channel();
    handle.tx.send(WalCommand::BudgetSnapshot(tx)).unwrap();
    let file_cap = rx.recv().unwrap().unwrap().0;
    let handle_cap = handle
        .append_budget
        .as_ref()
        .unwrap()
        .lock()
        .unwrap()
        .max_bytes;
    assert_eq!(file_cap, 64_000);
    assert_eq!(handle_cap, file_cap);
    handle.shutdown().await.unwrap();
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

fn fill_ordinary_budget(root: &Root, handle: &WalHandle) {
    handle.record(first_send(root, None)).into_result().unwrap();
    for _ in 0..1000 {
        match handle.record(ordinary()) {
            RecordOutcome::Accepted(_) => {}
            RecordOutcome::Refused(WalWriteError::SessionTooLarge { max_bytes: 16_000 }) => return,
            other => panic!("unexpected near-cap append outcome: {other:?}"),
        }
    }
    panic!("bounded session never reached its append limit");
}

#[tokio::test]
async fn near_cap_writer_closes_and_reopens_without_paying_another_terminal_reserve() {
    let mut root = Root::new(true);
    root.store.max_read_bytes = 16_000;
    let handle = root.spawn();
    fill_ordinary_budget(&root, &handle);
    assert!(
        !handle.is_poisoned(),
        "enqueue size refusal is not a writer failure"
    );
    handle
        .shutdown()
        .await
        .expect("held close bytes must fit even at the ordinary limit");
    assert!(root.markers().is_empty());
    let handle = root.spawn();
    handle
        .shutdown()
        .await
        .expect("reopened writer retains room for its close");
    assert!(root.markers().is_empty());
    assert!(
        std::fs::metadata(root.store.wal_path(&root.session))
            .unwrap()
            .len()
            <= 16_000
    );
}

#[cfg(feature = "test-clock")]
fn maximum_width_terminal() -> WalRecord {
    let mut terminal = retained_size::terminal(
        u64::MAX,
        Some(u64::MAX),
        broca_protocol::Usage {
            input_tokens: Some(u64::MAX),
            cached_input_tokens: Some(u64::MAX),
            cache_write_tokens: Some(u64::MAX),
            output_tokens: Some(u64::MAX),
            reasoning_tokens: Some(u64::MAX),
        },
        u32::MAX,
    );
    if let WalRecord::RunFinished { reason, .. } = &mut terminal {
        *reason = broca_protocol::RunFinishReason::Interrupted;
    }
    terminal
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn maximum_width_writer_reopens_at_cap_after_the_terminal() {
    let mut root = Root::new(true);
    root.store.max_read_bytes = 16_000;
    let spawn = || broca_wal::file_wal::with_writer_id_for_test([u8::MAX; 16], || root.spawn());
    let handle = spawn();
    fill_ordinary_budget(&root, &handle);
    handle.record_terminal(maximum_width_terminal()).unwrap();
    handle.shutdown().await.unwrap();
    assert!(root.markers().is_empty());
    assert!(
        std::fs::metadata(root.store.wal_path(&root.session))
            .unwrap()
            .len()
            <= 16_000
    );

    spawn().shutdown().await.unwrap();
    assert!(root.markers().is_empty());
    assert!(
        std::fs::metadata(root.store.wal_path(&root.session))
            .unwrap()
            .len()
            <= 16_000
    );
    let records = root.records();
    assert_eq!(records[records.len() - 4].record, maximum_width_terminal());
    assert!(matches!(
        records[records.len() - 3].record,
        WalRecord::WriterClosed { .. }
    ));
    assert!(matches!(
        records[records.len() - 2].record,
        WalRecord::WriterOpened { writer_id, previous_writer_id, .. }
            if writer_id == [u8::MAX; 16] && previous_writer_id == Some([u8::MAX; 16])
    ));
    assert!(matches!(
        records.last().unwrap().record,
        WalRecord::WriterClosed { .. }
    ));
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn maximum_width_reclaim_at_cap_settles_a_resurrected_marker() {
    struct MaximumIdStore(FileWalStore);
    impl WalStore for MaximumIdStore {
        fn wal_config(&self) -> broca_wal::WalConfig {
            self.0.wal_config()
        }
        fn writer_marker_root(&self) -> Option<PathBuf> {
            self.0.writer_marker_root()
        }
        fn open(&self, session: &BindIdentity) -> Result<Box<dyn Wal + Send>, WalError> {
            broca_wal::file_wal::with_writer_id_for_test([u8::MAX; 16], || self.0.open(session))
        }
        fn read_records(
            &self,
            session: &BindIdentity,
        ) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
            self.0.read_records(session)
        }
        fn open_owned_writer(
            &self,
            session: &BindIdentity,
            retention: Option<u64>,
            session_class: Option<&str>,
            lease: Box<dyn LeaseHandle>,
        ) -> Result<WriterAndLease, WalError> {
            broca_wal::file_wal::with_writer_id_for_test([u8::MAX; 16], || {
                self.0
                    .open_owned_writer(session, retention, session_class, lease)
            })
        }
    }

    let mut root = Root::new(true);
    root.store.max_read_bytes = 16_000;
    let handle = broca_wal::file_wal::with_writer_id_for_test([254; 16], || root.spawn());
    let previous_marker = root.markers().pop().unwrap();
    let marker_bytes = std::fs::read(&previous_marker).unwrap();
    fill_ordinary_budget(&root, &handle);
    handle.record_terminal(maximum_width_terminal()).unwrap();
    handle.shutdown().await.unwrap();
    // A crash can preserve the durable close while losing the later marker unlink.
    std::fs::write(&previous_marker, marker_bytes).unwrap();

    let mut store =
        FileWalStore::with_config(root.path.join("wal"), broca_wal::WalConfig::OVERHAUL);
    store.max_read_bytes = 16_000;
    let domain = crate::lease::WalLeaseDomain::new(root.path.join("leases"));
    let successor = WalHandle::open_and_spawn_owned_for_reclaim(
        Arc::new(MaximumIdStore(store)),
        root.session.clone(),
        domain.acquire(&root.session).unwrap(),
    )
    .await
    .expect("the successor's worst-width lifecycle must fit after a terminal and close");
    assert!(
        previous_marker.exists(),
        "opening alone cannot retire a predecessor"
    );
    successor.shutdown().await.unwrap();
    assert!(root.markers().is_empty());
    assert!(
        std::fs::metadata(root.store.wal_path(&root.session))
            .unwrap()
            .len()
            <= 16_000
    );
    let records = root.records();
    assert!(matches!(
        &records[records.len() - 2].record,
        WalRecord::WriterOpened { writer_id, previous_writer_id, cause, .. }
            if *writer_id == [u8::MAX; 16]
                && *previous_writer_id == Some([254; 16])
                && cause == "reclaim"
    ));
    assert!(matches!(
        records.last().unwrap().record,
        WalRecord::WriterClosed { .. }
    ));
    drop(
        domain
            .acquire(&root.session)
            .expect("reclaim must release its tenure after close"),
    );
}

#[tokio::test]
async fn run_terminal_cannot_spend_the_outstanding_writer_close_reserve() {
    let mut root = Root::new(true);
    root.store.max_read_bytes = 16_000;
    let handle = root.spawn();
    fill_ordinary_budget(&root, &handle);
    let terminal = serde_json::from_str(
        r#"{"type":"run_finished","reason":"completed","usage":{},"retries_used":0}"#,
    )
    .unwrap();
    handle.record_terminal(terminal).unwrap();
    handle.sync().await.unwrap();
    handle
        .shutdown()
        .await
        .expect("RunFinished must preserve the writer's close reserve");
    assert!(root.markers().is_empty());
    let records = root.records();
    assert!(matches!(
        records[records.len() - 2].record,
        WalRecord::RunFinished { .. }
    ));
    assert!(matches!(
        records.last().unwrap().record,
        WalRecord::WriterClosed { .. }
    ));
    assert!(
        std::fs::metadata(root.store.wal_path(&root.session))
            .unwrap()
            .len()
            <= 16_000
    );
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
        assert_eq!(
            decode_writer_marker(&std::fs::read(&root.markers()[0]).unwrap()).unwrap(),
            root.session,
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
                identity: root.session.clone(),
                cause: None,
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

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn owned_writer_ready_barrier_does_not_block_the_runtime() {
    use broca_wal::fs_ops::{FileOps, FsOps, OpenOptions, ReadDir, RealFs};
    use std::fs::Metadata;
    use std::path::Path;
    use std::time::Duration;

    struct PauseMarkerRename {
        gate: Mutex<Option<(tokio::sync::oneshot::Sender<()>, std_mpsc::Receiver<()>)>>,
    }
    impl FsOps for PauseMarkerRename {
        fn open(&self, path: &Path, options: &OpenOptions) -> io::Result<Box<dyn FileOps>> {
            RealFs.open(path, options)
        }
        fn metadata(&self, path: &Path) -> io::Result<Metadata> {
            RealFs.metadata(path)
        }
        fn create_dir_all(&self, path: &Path) -> io::Result<()> {
            RealFs.create_dir_all(path)
        }
        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            if to
                .parent()
                .is_some_and(|parent| parent.ends_with("open-writers"))
            {
                if let Some((reached, release)) = self.gate.lock().unwrap().take() {
                    let _ = reached.send(());
                    release
                        .recv_timeout(Duration::from_secs(5))
                        .map_err(|error| {
                            io::Error::other(format!(
                                "runtime could not release the writer ready barrier: {error}"
                            ))
                        })?;
                }
            }
            RealFs.rename(from, to)
        }
        fn remove_file(&self, path: &Path) -> io::Result<()> {
            RealFs.remove_file(path)
        }
        fn sync_dir(&self, path: &Path) -> io::Result<()> {
            RealFs.sync_dir(path)
        }
        fn create_dir(&self, path: &Path) -> io::Result<()> {
            RealFs.create_dir(path)
        }
        fn hard_link(&self, from: &Path, to: &Path) -> io::Result<()> {
            RealFs.hard_link(from, to)
        }
        fn canonicalize(&self, path: &Path) -> io::Result<PathBuf> {
            RealFs.canonicalize(path)
        }
        fn read_dir(&self, path: &Path) -> io::Result<ReadDir> {
            RealFs.read_dir(path)
        }
        fn validate_if_exists(&self, path: &Path) -> io::Result<()> {
            RealFs.validate_if_exists(path)
        }
    }

    let root = Root::new(true);
    let (reached_tx, reached) = tokio::sync::oneshot::channel();
    let (release, release_rx) = std_mpsc::channel();
    let fs = Arc::new(PauseMarkerRename {
        gate: Mutex::new(Some((reached_tx, release_rx))),
    });
    let tenure = crate::lease::WalLeaseDomain::new(root.path.join("leases"))
        .acquire(&root.session)
        .unwrap();
    let opening = WalHandle::open_and_spawn_owned_for_reclaim_with_fs_for_test(
        Arc::new(FileWalStore::with_config(
            root.path.join("wal"),
            broca_wal::WalConfig::OVERHAUL,
        )),
        root.session.clone(),
        tenure,
        fs,
    );
    let release_on_runtime = async {
        reached.await.unwrap();
        let _ = release.send(());
    };
    let (opened, ()) = tokio::join!(opening, release_on_runtime);
    opened
        .expect("owned startup must leave the runtime free to release its barrier")
        .shutdown()
        .await
        .unwrap();
}

#[tokio::test]
async fn owned_overhaul_writer_keeps_lease_until_the_last_arc_closes() {
    let root = Root::new(true);
    let domain = crate::lease::WalLeaseDomain::new(root.path.join("leases"));
    let store = Arc::new(FileWalStore::with_config(
        root.path.join("wal"),
        broca_wal::WalConfig::OVERHAUL,
    ));
    let owner = Arc::new(
        WalHandle::open_and_spawn_owned(
            store,
            root.session.clone(),
            domain.acquire(&root.session).unwrap(),
            None,
        )
        .await
        .unwrap(),
    );
    let survivor = owner.clone();
    drop(owner);
    assert!(matches!(
        domain.acquire(&root.session),
        Err(crate::lease::LeaseError::Held { .. })
    ));
    survivor.record(ordinary()).into_result().unwrap();
    survivor.sync().await.unwrap();
    assert!(
        matches!(
            domain.acquire(&root.session),
            Err(crate::lease::LeaseError::Held { .. })
        ),
        "a surviving writable Arc must exclude reclaim"
    );
    Arc::try_unwrap(survivor)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    drop(
        domain
            .acquire(&root.session)
            .expect("only the joined writer may release the lease"),
    );
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

#[tokio::test]
async fn writer_open_counter_keeps_recovery_and_startup_replays_in_one_bucket() {
    use broca_wal::access_counter::{ReadClass, WalAccessCounter};
    for (prefix, retention) in [(1, None), (20, None), (20, Some(60))] {
        let root = Root::new(true);
        let wal = root
            .store
            .open_with_retention(&root.session, retention)
            .unwrap();
        let handle = WalHandle::spawn_in_store(wal, &root.store, &root.session, 1, None).unwrap();
        handle
            .record(first_send(&root, retention))
            .into_result()
            .unwrap();
        for _ in 0..prefix {
            handle.record(ordinary()).into_result().unwrap();
        }
        handle.shutdown().await.unwrap();
        let path = root
            .store
            .resolve_live_path(&root.session)
            .unwrap()
            .unwrap();
        let length = std::fs::metadata(&path).unwrap().len();
        let frame_count = broca_wal::replay_file(&path, u64::MAX).unwrap().len() as u64;
        let domain = crate::lease::WalLeaseDomain::new(root.path.join("leases"));
        let counter = WalAccessCounter::default();
        let owner = counter
            .operation("writer reopen")
            .run_future(WalHandle::open_and_spawn_owned_with_retention(
                Arc::new(FileWalStore::with_config(
                    root.path.join("wal"),
                    broca_wal::WalConfig::OVERHAUL,
                )),
                root.session.clone(),
                domain.acquire(&root.session).unwrap(),
                None,
                retention,
            ))
            .await
            .unwrap();
        let snapshot = counter.snapshot();
        let measured = &snapshot[0];
        assert_eq!(measured.writer_opens.len(), 1);
        let open = &measured.writer_opens[0];
        assert_eq!(open.opens, 1);
        assert_eq!(open.session_length, length);
        // Recovery, lifecycle-only classification and previous-writer lookup
        // scan the log; retained logs also replay to recover retention policy.
        let scans = if retention.is_some() { 4 } else { 3 };
        assert_eq!(open.bytes, length * scans);
        // One initial lineage and one trailing lineage per record in each scan.
        assert_eq!(open.frames_decoded, (frame_count * 2 + 1) * scans);
        assert_eq!(measured.frames_decoded, open.frames_decoded);
        assert!(measured
            .reads
            .iter()
            .all(|read| read.class == ReadClass::WriterOpen));
        owner.shutdown().await.unwrap();
    }
}
