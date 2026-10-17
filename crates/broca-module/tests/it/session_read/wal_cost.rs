use super::*;
use broca_wal::{FramedRecord, WalByteAnchor, WalError, WalRangeRead};
use std::time::{Instant, SystemTime};

// Forward the file store's cheap probes and durability barriers: using the trait defaults
// would measure fallback behavior that the production file store never takes. Count
// sequence-zero reads reaching the seeded tail separately from bounded prefix reads.
struct CountingStore {
    inner: Arc<FileWalStore>,
    full: AtomicUsize,
    from_zero: AtomicUsize,
    full_history: AtomicUsize,
    tail_seq: u64,
}

impl CountingStore {
    fn reset(&self) {
        self.full.store(0, Ordering::SeqCst);
        self.from_zero.store(0, Ordering::SeqCst);
        self.full_history.store(0, Ordering::SeqCst);
    }

    fn count_range(&self, after: u64, read: &WalRangeRead) {
        if after == 0 {
            self.from_zero.fetch_add(1, Ordering::SeqCst);
            let records = match read.records() {
                broca_wal::WalRangeRecords::Complete(records)
                | broca_wal::WalRangeRecords::Partial(records)
                | broca_wal::WalRangeRecords::Unknown(records) => records,
                broca_wal::WalRangeRecords::Absent => &[],
            };
            if records
                .last()
                .is_some_and(|frame| frame.seq >= self.tail_seq)
            {
                self.full_history.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    fn report(&self, label: &str, start: Instant) {
        println!(
            "WAL_COST {label}: read_records={} zero_ranges={} full_history={} ms={:.3}",
            self.full.load(Ordering::SeqCst),
            self.from_zero.load(Ordering::SeqCst),
            self.full_history.load(Ordering::SeqCst),
            start.elapsed().as_secs_f64() * 1000.0,
        );
    }
}

impl WalStore for CountingStore {
    fn check_move(&self, key: &BindIdentity, write: bool) -> Result<(), WalError> {
        self.inner.check_move(key, write)
    }

    fn open(&self, key: &BindIdentity) -> Result<Box<dyn Wal + Send>, WalError> {
        self.inner.open(key)
    }

    fn open_with_retention(
        &self,
        key: &BindIdentity,
        retention: Option<u64>,
    ) -> Result<Box<dyn Wal + Send>, WalError> {
        self.inner.open_with_retention(key, retention)
    }

    fn is_retention_lineage(&self, key: &BindIdentity) -> Result<bool, WalError> {
        self.inner.is_retention_lineage(key)
    }

    fn lineage_exists(
        &self,
        key: &BindIdentity,
        meta: Option<&broca_core::AdmissionStoreSnapshot>,
    ) -> Result<bool, WalError> {
        self.inner.lineage_exists(key, meta)
    }

    fn wal_present(&self, key: &BindIdentity) -> Option<bool> {
        self.inner.wal_present(key)
    }

    fn live_wal_last_write(&self, key: &BindIdentity) -> Result<Option<SystemTime>, WalError> {
        self.inner.live_wal_last_write(key)
    }

    fn read_records(&self, key: &BindIdentity) -> Result<Vec<FramedRecord>, WalError> {
        self.full.fetch_add(1, Ordering::SeqCst);
        self.full_history.fetch_add(1, Ordering::SeqCst);
        self.inner.read_records(key)
    }

    fn read_records_range(
        &self,
        key: &BindIdentity,
        after: u64,
        through: Option<u64>,
        anchor: Option<&WalByteAnchor>,
    ) -> Result<WalRangeRead, WalError> {
        let read = self.inner.read_records_range(key, after, through, anchor)?;
        self.count_range(after, &read);
        Ok(read)
    }

    fn read_records_range_for_authorization(
        &self,
        key: &BindIdentity,
        after: u64,
        through: Option<u64>,
        anchor: Option<&WalByteAnchor>,
    ) -> Result<WalRangeRead, WalError> {
        let read = self
            .inner
            .read_records_range_for_authorization(key, after, through, anchor)?;
        self.count_range(after, &read);
        Ok(read)
    }

    fn read_records_range_from_valid_anchor(
        &self,
        key: &BindIdentity,
        after: u64,
        through: Option<u64>,
        anchor: &WalByteAnchor,
    ) -> Result<Option<WalRangeRead>, WalError> {
        let read = self
            .inner
            .read_records_range_from_valid_anchor(key, after, through, anchor)?;
        if let Some(read) = &read {
            self.count_range(after, read);
        }
        Ok(read)
    }

    fn flush_for_read(&self, key: &BindIdentity) -> Result<Option<u64>, WalError> {
        self.inner.flush_for_read(key)
    }

    fn file_identity(&self, key: &BindIdentity) -> Result<Option<u64>, WalError> {
        self.inner.file_identity(key)
    }

    fn flush_for_authorization(&self, key: &BindIdentity) -> Result<Option<u64>, WalError> {
        self.inner.flush_for_authorization(key)
    }

    fn file_identity_for_authorization(&self, key: &BindIdentity) -> Result<Option<u64>, WalError> {
        self.inner.file_identity_for_authorization(key)
    }
}

fn registry(rig: &Rig, counting: Arc<CountingStore>) -> LineageRegistry {
    let opener = Arc::new(
        DefaultOpener::new(
            Arc::new(FileLeaseStore::new(&rig.lease_dir)),
            counting.clone(),
            Arc::new(NoopExec),
        )
        .with_run_index(rig.store.clone())
        .with_materialized_store(rig.store.clone()),
    );
    LineageRegistry::new_with_reader(opener, counting, Some(rig.store.clone()))
}

fn seed(rig: &Rig, key: &BindIdentity) {
    let mut wal = rig.wal_store.open(key).unwrap();
    let text = "x".repeat(30_000);
    for episode in 0..500 {
        for record in [
            started(key, &format!("run-{episode}"), "prompt"),
            assistant_text(0, &text),
            finished_ok(),
        ] {
            wal.append(1, &record, false).unwrap();
        }
    }
    wal.sync().unwrap();
    println!(
        "WAL_COST seed: episodes=500 bytes={}",
        std::fs::metadata(rig.wal_store.wal_path(key))
            .unwrap()
            .len()
    );
}

fn counter(rig: &Rig, key: &BindIdentity) -> Arc<CountingStore> {
    Arc::new(CountingStore {
        inner: rig.wal_store.clone(),
        full: AtomicUsize::new(0),
        from_zero: AtomicUsize::new(0),
        full_history: AtomicUsize::new(0),
        tail_seq: rig
            .wal_store
            .read_records(key)
            .unwrap()
            .last()
            .map_or(u64::MAX, |frame| frame.seq),
    })
}

fn two_episodes(rig: &Rig, key: &BindIdentity) {
    append_seed(
        &rig.wal_store,
        key,
        &[
            started(key, "first", "first prompt"),
            assistant_text(0, "first answer"),
            finished_ok(),
            started(key, "second", "second prompt"),
            assistant_text(0, "second answer"),
            finished_ok(),
        ],
    );
}

#[tokio::test]
async fn cold_run_result_reuses_status_snapshot() {
    let rig = Rig::new("result-snapshot-count", Arc::new(NoopExec));
    let key = rig.key("result");
    two_episodes(&rig, &key);
    let counting = counter(&rig, &key);
    let registry = registry(&rig, counting.clone());
    registry
        .run_result(&key, RunId("first".into()))
        .await
        .unwrap();
    assert_eq!(counting.full.load(Ordering::SeqCst), 0);
    assert_eq!(
        counting.full_history.load(Ordering::SeqCst),
        2,
        "run.result must reuse the status WAL snapshot"
    );
    assert_eq!(
        counting.from_zero.load(Ordering::SeqCst),
        2,
        "only the candidate and fsync-bounded proof reads, not another result read"
    );
    counting.reset();
    registry
        .run_result(&key, RunId("second".into()))
        .await
        .unwrap();
    // A repeat reuses the cached read proof, which is keyed on file identity. Live WALs have a
    // file identity only on Unix, so off Unix the repeat pays the proof read again.
    let repeat_reads = if cfg!(unix) { 1 } else { 2 };
    assert_eq!(counting.full.load(Ordering::SeqCst), 0);
    assert_eq!(counting.from_zero.load(Ordering::SeqCst), repeat_reads);
    assert_eq!(counting.full_history.load(Ordering::SeqCst), repeat_reads);
}

#[tokio::test]
async fn run_result_snapshot_matches_full_replay_after_crash_and_archive() {
    use std::io::Write;

    let rig = Rig::new("result-snapshot-replay", Arc::new(NoopExec));
    let key = rig.key("result");
    two_episodes(&rig, &key);
    materialize_all(&rig, &key);
    let path = rig.wal_store.wal_path(&key);
    // A process can die between appending a frame's header and its body. Readers must
    // keep serving completed episodes without treating the incomplete suffix as absence.
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"torn")
        .unwrap();
    for phase in ["crashed", "recovered", "archived"] {
        if phase == "recovered" {
            drop(FileWal::open(&path).unwrap());
        } else if phase == "archived" {
            let archive_dir = rig.dir.join("wal-archive");
            std::fs::create_dir_all(&archive_dir).unwrap();
            let mut writer =
                broca_wal::archive::ArchiveWriter::create(archive_dir.join("fold-1.ark"))
                    .unwrap()
                    .without_feature_floor();
            writer
                .append(
                    &broca_core::session_addr(&key),
                    &std::fs::read(&path).unwrap(),
                )
                .unwrap();
            writer.seal().unwrap();
            std::fs::remove_file(&path).unwrap();
        }
        let full = rig.wal_store.read_records(&key).unwrap();
        let transcript = broca_core::replay_transcript(&full).unwrap();
        assert_eq!(transcript.len(), 4, "{phase}");
        let registry = registry(&rig, counter(&rig, &key));
        for (run, ordinal, text) in [("first", 1, "first answer"), ("second", 3, "second answer")] {
            let response = registry.run_result(&key, RunId(run.into())).await.unwrap();
            let expected =
                broca_core::reconcile_run_state(full.iter().map(|frame| &frame.record), run)
                    .unwrap();
            assert_eq!(expected.state, RunIndexState::Completed, "{phase}");
            assert_eq!(response.state, "completed", "{phase}");
            assert!(response.reason.is_none(), "{phase}");
            assert_eq!(response.error, expected.error.map(Into::into), "{phase}");
            let message = response.final_message.unwrap();
            assert_eq!(message.ordinal, ordinal, "{phase}");
            assert_eq!(message.mid, format!("m{ordinal}"), "{phase}");
            assert_eq!(message.text, text, "{phase}");
            assert_eq!(
                transcript[ordinal as usize].content,
                vec![ContentBlock::Text { text: message.text }],
                "{phase}"
            );
        }
    }
    assert!(!path.exists(), "archive reads must not extract a live WAL");
}

#[tokio::test]
#[ignore = "release-build measurement of a 15 MB WAL; run with --ignored --nocapture"]
async fn large_wal_read_costs() {
    let rig = Rig::new("wal-cost", Arc::new(NoopExec));
    let key = rig.key("cost");
    seed(&rig, &key);
    let counting = counter(&rig, &key);
    for materialized in [false, true] {
        if materialized {
            materialize_all(&rig, &key);
        }
        for op in [
            "status",
            "result",
            "head",
            "baseline",
            "bind_owner",
            "owner",
            "session_status",
        ] {
            let registry = registry(&rig, counting.clone());
            for attempt in 0..3 {
                counting.reset();
                let start = Instant::now();
                match op {
                    "status" => {
                        registry
                            .run_status(&key, RunId("run-499".into()))
                            .await
                            .unwrap();
                    }
                    "result" => {
                        registry
                            .run_result(&key, RunId("run-499".into()))
                            .await
                            .unwrap();
                    }
                    "head" => {
                        registry.head(&key).await.unwrap();
                    }
                    "baseline" => {
                        registry
                            .baseline(&key, None, &Default::default())
                            .await
                            .unwrap();
                    }
                    "bind_owner" => {
                        registry.session_ownership_for_bind(&key).await.unwrap();
                    }
                    "owner" => {
                        registry
                            .authorize_session_owner(&key, None, &Default::default())
                            .await
                            .unwrap();
                    }
                    "session_status" => {
                        registry.status(&key).await.unwrap();
                    }
                    _ => unreachable!(),
                }
                counting.report(
                    &format!("{op} materialized={materialized} attempt={attempt}"),
                    start,
                );
            }
            assert!(registry.stop_and_seal().await);
        }
    }
    for episode in 0..500 {
        let run_id = format!("run-{episode}");
        rig.store
            .index_run_active(&RunIndexRow {
                withdrawals_owed: false,
                title: None,
                run_id: run_id.clone(),
                session: key.clone(),
                submission_id: None,
                request_identity_digest: None,
                state: RunIndexState::Active,
                usage: None,
                final_step_finish_reason: None,
                retries_used: 0,
            })
            .unwrap();
        rig.store
            .index_run_terminal(
                &run_id,
                RunIndexState::Completed,
                Some(usage()),
                Some(FinishReason::Stop),
                0,
            )
            .unwrap();
    }
    for attempt in 0..2 {
        counting.reset();
        let start = Instant::now();
        let emitted = broca_core::catch_up_usage_exports(
            rig.store.as_ref(),
            counting.as_ref(),
            &UnknownCharge,
        )
        .unwrap();
        counting.report(&format!("export runs={emitted} attempt={attempt}"), start);
    }
}

struct UnknownCharge;

impl broca_core::ChargeBasisResolver for UnknownCharge {
    fn resolve_charge_basis(&self, _: &str, _: &str) -> (broca_core::ChargeBasis, u32) {
        (broca_core::ChargeBasis::Unknown, 1)
    }

    fn charge_basis_overrides(&self) -> String {
        broca_core::NO_CHARGE_BASIS_OVERRIDES.into()
    }
}
