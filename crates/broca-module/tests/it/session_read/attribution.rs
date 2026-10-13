use super::*;
use broca_core::LeaseStore;
use broca_store::conformance::PageUsageConformance;

fn seed(rig: &Rig, key: &BindIdentity) {
    append_seed(
        &rig.wal_store,
        key,
        &[
            started(key, "old", "old input"),
            assistant_text(1, "old answer"),
            WalRecord::RunFinished {
                ts_ms: None,
                reason: RunFinishReason::Completed,
                usage: usage(),
                error: None,
                retries_used: 0,
            },
        ],
    );
    let range = rig
        .wal_store
        .read_records_range(key, 0, None, None)
        .unwrap();
    Persister::project_range(rig.store.as_ref(), key, &range).unwrap();
    rig.store
        .erase_attribution_for_conformance(key, false)
        .unwrap();
}

async fn backfilled(rig: &Rig, key: &BindIdentity) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if rig
                .store
                .read_page_with_watermark(key, Some(0), 10, broca_core::ReadView::Raw)
                .is_ok()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("lazy attribution backfill completes");
}

struct CountingPages {
    inner: Arc<broca_store::SqliteStore>,
    ready_reads: AtomicUsize,
    stale_reads: AtomicUsize,
}

impl MaterializedStore for CountingPages {
    fn read_prefix_with_watermark(
        &self,
        key: &BindIdentity,
    ) -> Result<broca_core::StorePrefix, String> {
        self.inner.read_prefix_with_watermark(key)
    }
    fn read_page_with_attribution(
        &self,
        key: &BindIdentity,
        from: Option<u64>,
        limit: u64,
        view: broca_core::ReadView,
    ) -> Result<broca_core::StorePageSnapshot, broca_core::StorePageReadError> {
        let result = self
            .inner
            .read_page_with_attribution(key, from, limit, view);
        match &result {
            Ok(_) => {
                self.ready_reads.fetch_add(1, Ordering::SeqCst);
            }
            Err(broca_core::StorePageReadError::AttributionStale) => {
                self.stale_reads.fetch_add(1, Ordering::SeqCst);
            }
            Err(_) => {}
        }
        result
    }
}

#[tokio::test]
async fn archived_old_format_rows_backfill_without_wal_writes_and_later_reads_use_store() {
    let mut rig = Rig::new("attribution-archive", Arc::new(NoopExec));
    let key = rig.key("session");
    seed(&rig, &key);
    let path = rig.wal_store.wal_path(&key);
    let dir = rig.dir.join("wal-archive");
    std::fs::create_dir_all(&dir).unwrap();
    let archive = dir.join("fold-1.ark");
    let mut writer = broca_wal::archive::ArchiveWriter::create(&archive)
        .unwrap()
        .without_feature_floor();
    writer
        .append(
            &broca_core::session_addr(&key),
            &std::fs::read(&path).unwrap(),
        )
        .unwrap();
    writer.seal().unwrap();
    let bytes = std::fs::read(&archive).unwrap();
    std::fs::remove_file(&path).unwrap();
    let zero_reads = Arc::new(AtomicUsize::new(0));
    let counted = Arc::new(ZeroRangeCountingWalStore {
        inner: rig.wal_store.clone(),
        zero_range_reads: zero_reads.clone(),
    });
    let opener = Arc::new(
        DefaultOpener::new(
            Arc::new(FileLeaseStore::new(&rig.lease_dir)),
            rig.wal_store.clone(),
            Arc::new(NoopExec),
        )
        .with_materialized_store(rig.store.clone()),
    );
    let pages = Arc::new(CountingPages {
        inner: rig.store.clone(),
        ready_reads: AtomicUsize::new(0),
        stale_reads: AtomicUsize::new(0),
    });
    rig.registry = LineageRegistry::new_with_reader(opener, counted, Some(pages.clone()));
    let first = rig
        .registry
        .route_read(&key, Some(0), Some(10))
        .await
        .unwrap();
    assert_eq!(first.messages.len(), 2);
    assert_eq!(first.messages[0].run.as_ref().unwrap().run_id, "old");
    assert!(first.messages[1].run.as_ref().unwrap().is_final);
    assert_eq!(
        pages.stale_reads.load(Ordering::SeqCst),
        1,
        "the first page must encounter a typed stale projection"
    );
    backfilled(&rig, &key).await;
    assert_eq!(rig.registry.slot_counts().open, 0);
    let zero_before = zero_reads.load(Ordering::SeqCst);
    assert!(
        zero_before > 0,
        "first read must actually replay the archived WAL"
    );
    assert_eq!(
        rig.registry
            .route_read(&key, Some(0), Some(10))
            .await
            .unwrap(),
        first
    );
    assert_eq!(
        zero_reads.load(Ordering::SeqCst),
        zero_before,
        "later page reads must not replay from zero"
    );
    assert_eq!(
        pages.ready_reads.load(Ordering::SeqCst),
        1,
        "the later registry read must consume an actual store snapshot"
    );
    assert!(!path.exists(), "backfill must not extract an archived WAL");
    assert_eq!(
        std::fs::read(archive).unwrap(),
        bytes,
        "backfill must not alter archived bytes"
    );
}

struct HeldProjection {
    inner: Arc<broca_store::SqliteStore>,
    entered: tokio::sync::Notify,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}
impl MaterializedStore for HeldProjection {
    fn read_page_with_attribution(
        &self,
        key: &BindIdentity,
        from: Option<u64>,
        limit: u64,
        view: broca_core::ReadView,
    ) -> Result<broca_core::StorePageSnapshot, broca_core::StorePageReadError> {
        self.inner
            .read_page_with_attribution(key, from, limit, view)
    }
    fn read_prefix_with_watermark(
        &self,
        key: &BindIdentity,
    ) -> Result<broca_core::StorePrefix, String> {
        self.inner.read_prefix_with_watermark(key)
    }
    fn read_admission_snapshot(
        &self,
        key: &BindIdentity,
    ) -> Result<Option<broca_core::AdmissionStoreSnapshot>, String> {
        self.inner.read_admission_snapshot(key)
    }
    fn read_page_with_watermark(
        &self,
        key: &BindIdentity,
        from: Option<u64>,
        limit: u64,
        view: broca_core::ReadView,
    ) -> Result<broca_core::StorePageSnapshot, String> {
        self.inner.read_page_with_watermark(key, from, limit, view)
    }
}
impl Persister for HeldProjection {
    fn project(
        &self,
        key: &BindIdentity,
        frames: &[broca_wal::FramedRecord],
    ) -> Result<u64, String> {
        Persister::project(self.inner.as_ref(), key, frames)
    }
    fn project_range(
        &self,
        key: &BindIdentity,
        range: &broca_wal::WalRangeRead,
    ) -> Result<u64, String> {
        self.entered.notify_one();
        self.release
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(5))
            .map_err(|e| e.to_string())?;
        Persister::project_range(self.inner.as_ref(), key, range)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_send_waits_for_attribution_repair_instead_of_receiving_a_lease_refusal() {
    let mut rig = Rig::new("attribution-send", Arc::new(NoopExec));
    let key = rig.key("session");
    seed(&rig, &key);
    let bytes = std::fs::read(rig.wal_store.wal_path(&key)).unwrap();
    let (release, rx) = std::sync::mpsc::channel();
    let held = Arc::new(HeldProjection {
        inner: rig.store.clone(),
        entered: Default::default(),
        release: std::sync::Mutex::new(rx),
    });
    let exec = Arc::new(ScriptedCoreExec {
        wal_store: rig.wal_store.clone(),
        store: rig.store.clone(),
        scripts: Mutex::new(VecDeque::from([vec![text_step("new answer")]])),
    });
    let opener = Arc::new(
        DefaultOpener::new(
            Arc::new(FileLeaseStore::new(&rig.lease_dir)),
            rig.wal_store.clone(),
            exec,
        )
        .with_materialized_store(held.clone()),
    );
    rig.registry =
        LineageRegistry::new_with_reader(opener, rig.wal_store.clone(), Some(held.clone()));
    rig.registry
        .route_read(&key, Some(0), Some(10))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), held.entered.notified())
        .await
        .unwrap();
    assert!(
        matches!(
            FileLeaseStore::new(&rig.lease_dir).acquire(&broca_core::lease_key_for(&key)),
            Err(broca_core::LeaseError::Held { .. })
        ),
        "the real repair must hold the session lease"
    );
    assert_eq!(std::fs::read(rig.wal_store.wal_path(&key)).unwrap(), bytes);
    let pending = send(&rig, &key, "new", "new input");
    tokio::pin!(pending);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut pending)
            .await
            .is_err(),
        "send must wait, not return an error while repair holds the lease"
    );
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), &mut pending)
        .await
        .expect("send is admitted after repair");
    wait_status(&rig, &key, "new", "completed").await;
    let frames = rig.wal_store.read_records(&key).unwrap();
    assert_eq!(frames.iter().filter(|frame| matches!(&frame.record, WalRecord::RunStarted { run_id, .. } if run_id.0 == "new")).count(), 1, "the waiting send is admitted exactly once");
}

struct FaultingPages;

impl MaterializedStore for FaultingPages {
    fn read_prefix_with_watermark(
        &self,
        _key: &BindIdentity,
    ) -> Result<broca_core::StorePrefix, String> {
        Err("injected store I/O failure".into())
    }
    fn read_page_with_watermark(
        &self,
        _key: &BindIdentity,
        _from: Option<u64>,
        _limit: u64,
        _view: broca_core::ReadView,
    ) -> Result<broca_core::StorePageSnapshot, String> {
        // Diagnostic text resembling a migration notice must not become a repair class.
        Err("injected store I/O failure: run attribution requires WAL backfill".into())
    }
}

#[tokio::test]
async fn stale_attribution_logs_info_once_and_store_faults_remain_warn() {
    let rig = Rig::new("attribution-log-level", Arc::new(NoopExec));
    let key = rig.key("session");
    seed(&rig, &key);
    // Keep the projection stale across both reads, as a live actor's lease would.
    let _lease = FileLeaseStore::new(&rig.lease_dir)
        .acquire(&broca_core::lease_key_for(&key))
        .unwrap();
    let capture = broca_test_support::LogCapture::new();
    capture
        .scope_future(async {
            for _ in 0..2 {
                let page = rig
                    .registry
                    .route_read(&key, Some(0), Some(10))
                    .await
                    .unwrap();
                assert_eq!(page.messages.len(), 2);
            }
            let opener = Arc::new(
                DefaultOpener::new(
                    Arc::new(FileLeaseStore::new(&rig.lease_dir)),
                    rig.wal_store.clone(),
                    Arc::new(NoopExec),
                )
                .with_materialized_store(rig.store.clone()),
            );
            let faulting = LineageRegistry::new_with_reader(
                opener,
                rig.wal_store.clone(),
                Some(Arc::new(FaultingPages)),
            );
            assert_eq!(
                faulting
                    .route_read(&key, Some(0), Some(10))
                    .await
                    .unwrap()
                    .messages
                    .len(),
                2
            );
        })
        .await;
    let events = capture.events();
    let migration: Vec<_> = events
        .iter()
        .filter(|event| {
            event.target == "session"
                && event
                    .message()
                    .starts_with("run attribution not yet stored; reading from WAL and backfilling")
        })
        .collect();
    assert_eq!(
        migration.len(),
        1,
        "expected migration is announced only once per session"
    );
    assert_eq!(migration[0].level, tracing::Level::INFO);
    let faults: Vec<_> = events
        .iter()
        .filter(|event| {
            event.target == "session" && event.message().contains("store page read failed")
        })
        .collect();
    assert_eq!(
        faults.len(),
        1,
        "migration reads must never use the failure warning"
    );
    assert_eq!(faults[0].level, tracing::Level::WARN);
    assert!(faults[0].message().contains("injected store I/O failure"));
}
