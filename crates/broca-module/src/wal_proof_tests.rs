use super::tests::{min_config, text_req};
use super::*;
use broca_core::{FileLeaseStore, FileWalStore, LeaseStore};
use std::sync::atomic::{AtomicBool, AtomicUsize};

struct FaultWal {
    inner: FileWalStore,
    mode: AtomicUsize,
    fail_snapshot: AtomicBool,
    snapshot_reads: AtomicUsize,
}

impl WalStore for FaultWal {
    fn open(
        &self,
        key: &LineageKey,
    ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
        self.inner.open(key)
    }

    fn read_records(
        &self,
        key: &LineageKey,
    ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        if self.fail_snapshot.load(Ordering::Relaxed)
            && self.snapshot_reads.fetch_add(1, Ordering::Relaxed) == 0
        {
            // Fail the snapshot load once so load_checkpoint_at_head must reread the
            // history through the writer's durable head instead of trusting a short prefix.
            return Err(broca_wal::WalError::Io(std::io::Error::other(
                "injected primary snapshot failure",
            )));
        }
        let mut records = self.inner.read_records(key)?;
        if self.mode.load(Ordering::Relaxed) == 2 {
            records.truncate(1);
        }
        Ok(records)
    }

    fn read_records_range(
        &self,
        key: &LineageKey,
        after: u64,
        through: Option<u64>,
        anchor: Option<&broca_wal::WalByteAnchor>,
    ) -> Result<broca_wal::WalRangeRead, broca_wal::WalError> {
        let range = self.inner.read_records_range(key, after, through, anchor)?;
        let broca_wal::WalRangeRecords::Complete(records) = range.records() else {
            return Ok(range);
        };
        let records = records.to_vec();
        match self.mode.load(Ordering::Relaxed) {
            1 => Ok(broca_wal::WalRangeRead::without_anchors(Vec::new())),
            2 => self.inner.read_records_range(key, after, Some(1), anchor),
            3 => self.inner.read_records_range(key, after, Some(0), anchor),
            4 => self
                .inner
                .read_records_range(key, after.max(2), through, anchor),
            5 => Ok(broca_wal::WalRangeRead::absent_after_archive_lookup()),
            6 => Ok(broca_wal::WalRangeRead::without_anchors(records)),
            7 => Ok(broca_wal::WalRangeRead::without_anchors(
                records.into_iter().take(1).collect(),
            )),
            _ => Ok(range),
        }
    }
}

struct CountingExecutor(AtomicUsize);

#[async_trait::async_trait]
impl RunExecutor for CountingExecutor {
    async fn execute(
        &self,
        job: crate::executor::RunJob,
        _: mpsc::Sender<broca_core::OutputEvent>,
    ) -> Result<broca_core::RunSummary, broca_core::RunError> {
        self.0.fetch_add(1, Ordering::Relaxed);
        if let Some(ack) = job.on_admitted {
            let _ = ack.send(Ok(job.req.run_id));
        }
        Ok(broca_core::RunSummary {
            reason: broca_protocol::RunFinishReason::Completed,
            paused: None,
            steps: 0,
            usage: Default::default(),
            retries_used: 0,
            final_step_finish_reason: None,
        })
    }
}

struct Fixture {
    root: std::path::PathBuf,
    actor: LineageActor<FaultWal>,
    executor: Arc<CountingExecutor>,
    request: SubmissionRequest,
}

impl Fixture {
    async fn new() -> Self {
        Self::with_history(true).await
    }

    async fn with_history(history: bool) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "broca-wal-proof-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        let key = LineageKey::new(root.join("project"), "agent", "session");
        let wal_store = Arc::new(FaultWal {
            inner: FileWalStore::new(root.join("wal")),
            mode: AtomicUsize::new(0),
            fail_snapshot: AtomicBool::new(false),
            snapshot_reads: AtomicUsize::new(0),
        });
        let request = submission_from_config(min_config(), text_req(&key, "old").input);
        let digest = send_identity_digest(&request, broca_protocol::Delivery::Queue, None);
        let mut wal = wal_store.open(&key).unwrap();
        for run in ["old", "latest"].into_iter().filter(|_| history) {
            wal.append(
                1,
                &WalRecord::RunStarted {
                    tags: None,
                    session_class: None,
                    retention: None,
                    hooks: None,
                    title: None,
                    on_restart: None,
                    keep_warm: None,
                    marker_order_version: None,
                    manifest: None,
                    scope: None,
                    send_id: Some(run.into()),
                    request_identity_digest: Some(digest.clone()),
                    project_id: None,
                    principal: None,
                    ts_ms: None,
                    run_id: RunId(run.into()),
                    session: key.clone(),
                    config: Box::new(min_config()),
                    input: request.input.clone().into(),
                    origin: broca_wal::RunOrigin::Fresh,
                    submission_id: None,
                },
                false,
            )
            .unwrap();
            wal.append(
                1,
                &WalRecord::RunFinished {
                    reason: broca_protocol::RunFinishReason::Completed,
                    ts_ms: None,
                    usage: Default::default(),
                    error: None,
                    retries_used: 0,
                },
                true,
            )
            .unwrap();
        }
        let writer = Arc::new(WalHandle::spawn(wal, 1).unwrap());
        let lease = FileLeaseStore::new(root.join("leases"))
            .acquire(&broca_core::lease_key_for(&key))
            .unwrap();
        let executor = Arc::new(CountingExecutor(AtomicUsize::new(0)));
        let registry = crate::registry::LineageRegistry::new(Arc::new(super::tests::SeamOpener {
            lease_store: Arc::new(FileLeaseStore::new(root.join("unused"))),
            wal_store: Arc::new(FileWalStore::new(root.join("unused-wal"))),
            idle_ttl: Duration::from_secs(60),
            executor: executor.clone(),
        }));
        let (fanout, _) = crate::fanout::Fanout::new(wal_store.clone(), key.clone());
        let approvals = fanout.approval_state();
        let (self_tx, rx) = mpsc::channel(64);
        let (terminal_tx, terminal_rx) = mpsc::unbounded_channel();
        let mut actor = LineageActor {
            admitting_session_class: None,
            agent_run_scope: None,
            #[cfg(all(test, feature = "test-clock"))]
            agent_run_writer_for_test: AGENT_RUN_WRITER_FOR_TEST.get(),
            retention_lineage: false,
            retention_deadline: None,
            admitting_retention: None,
            warm_task: None,
            withdrawal_task: None,
            withdrawal_index_lock: Arc::default(),
            approvals,
            approval_seeded: false,
            approval_debt: None,
            actor_jobs: Arc::default(),
            key,
            gen: 1,
            lease: Some(lease),
            writer: Some(writer),
            executor: executor.clone(),
            compaction_inbox: Arc::default(),
            wal_store,
            registry: registry.handle(),
            rx,
            terminal_rx,
            self_tx,
            terminal_tx,
            writer_poisoned: false,
            run_in_flight: Arc::new(AtomicU64::new(0)),
            state: RunState::Idle,
            next_run_attempt: 1,
            admitted_request_identities: HashMap::new(),
            idle_ttl: Duration::from_secs(60),
            fanout,
            run_index: None,
            materialized_store: None,
            export_index: None,
            charge_basis_resolver: None,
            cap_store: None,
            pending_capped_export: None,
            startup_recovery_pending: false,
            prior_snapshot: None,
            pending_fifo: Vec::new(),
            send_reply: None,
            skip_materialization_submissions: HashSet::new(),
            skip_materialization_runs: HashSet::new(),
            close_after_one_shot: false,
            pending_cancels: HashMap::new(),
            retracted_submissions: HashSet::new(),
            recorded_send_keys: HashMap::new(),
            recorded_steers: HashMap::new(),
            terminal_status_cache: HashMap::new(),
            started_run_titles: HashMap::new(),
            wal_scan_complete: false,
            crash_orphan_candidates: None,
            first_episode: Arc::default(),
            admitting_manifest: None,
            restart_admitting: false,
            restart_waiters: Vec::new(),
            warm_in_flight: Default::default(),
        };
        actor.refresh_admission_view().await.unwrap();
        Self {
            root,
            actor,
            executor,
            request,
        }
    }

    async fn send_old(&mut self) -> SendOutcome {
        tokio::time::timeout(
            Duration::from_secs(5),
            self.actor.on_send(
                RunId("old".into()),
                self.request.clone(),
                RunKind::FreshOrResume,
                SendKey::SendId("old".into()),
                false,
                broca_protocol::Delivery::Queue,
                None,
                None,
                None,
                None,
                None,
                false,
            ),
        )
        .await
        .expect("send must terminate")
    }

    async fn close(mut self) {
        self.actor.shutdown_writer().await;
        drop(self.actor.lease.take());
        std::fs::remove_dir_all(self.root).unwrap();
    }
}

#[tokio::test]
async fn old_keyed_send_requires_proven_full_scan_without_run_index() {
    for mode in [1, 2, 3, 4, 5, 6, 7] {
        let mut fixture = Fixture::new().await;
        fixture.actor.wal_store.mode.store(mode, Ordering::Relaxed);
        let before =
            std::fs::read(fixture.actor.wal_store.inner.wal_path(&fixture.actor.key)).unwrap();
        let outcome = fixture.send_old().await;
        assert_eq!(
            std::fs::read(fixture.actor.wal_store.inner.wal_path(&fixture.actor.key)).unwrap(),
            before
        );
        assert_eq!(
            fixture.executor.0.load(Ordering::Relaxed),
            0,
            "degraded scan must never execute an old send: {outcome:?}"
        );
        assert!(
            matches!(outcome, SendOutcome::Error(_) | SendOutcome::Transient(_)),
            "mode {mode}: {outcome:?}"
        );
        assert!(!fixture.actor.wal_scan_complete);
        fixture.actor.wal_store.mode.store(0, Ordering::Relaxed);
        assert!(matches!(
            fixture.send_old().await,
            SendOutcome::Finished { .. }
        ));
        assert_eq!(fixture.executor.0.load(Ordering::Relaxed), 0);
        fixture.close().await;
    }
    let fixture = Fixture::with_history(false).await;
    assert!(fixture
        .actor
        .read_wal_framed_records()
        .await
        .unwrap()
        .is_empty());
    fixture.actor.wal_store.mode.store(5, Ordering::Relaxed);
    assert!(fixture
        .actor
        .read_wal_framed_records()
        .await
        .unwrap()
        .is_empty());
    fixture.actor.wal_store.mode.store(1, Ordering::Relaxed);
    assert!(fixture.actor.read_wal_framed_records().await.is_err());
    fixture.close().await;
}

#[tokio::test]
async fn degraded_retract_preserves_good_scan_sets_and_refuses_negative_answers() {
    let mut fixture = Fixture::new().await;
    fixture.actor.ensure_wal_scan_loaded().await.unwrap();
    let before = fixture.actor.recorded_send_keys.clone();
    fixture.actor.wal_store.mode.store(1, Ordering::Relaxed);
    assert!(matches!(
        fixture
            .actor
            .on_retract(broca_protocol::SubmissionId("missing".into()))
            .await,
        RetractOutcome::Error(_)
    ));
    assert_eq!(fixture.actor.recorded_send_keys, before);
    assert!(fixture
        .actor
        .reconcile_from_wal(&RunId("old".into()))
        .await
        .is_err());
    let outcome = fixture
        .actor
        .on_send(
            RunId("new".into()),
            fixture.request.clone(),
            RunKind::FreshOrResume,
            SendKey::Minted,
            false,
            broca_protocol::Delivery::Queue,
            None,
            None,
            None,
            Some(60),
            None,
            false,
        )
        .await;
    assert!(matches!(outcome, SendOutcome::Error(_)));
    fixture.actor.retention_deadline = Some(1);
    assert!(!fixture.actor.expire_actor(2).await);
    assert!(fixture.actor.retention_deadline.is_some());
    fixture.actor.wal_store.mode.store(0, Ordering::Relaxed);
    assert!(matches!(
        fixture
            .actor
            .on_retract(broca_protocol::SubmissionId("missing".into()))
            .await,
        RetractOutcome::NotPending
    ));
    fixture.close().await;
}

#[tokio::test]
async fn checkpoint_fallback_requires_coverage_and_keeps_primary_cause() {
    let fixture = Fixture::new().await;
    fixture
        .actor
        .wal_store
        .fail_snapshot
        .store(true, Ordering::Relaxed);
    fixture.actor.wal_store.mode.store(2, Ordering::Relaxed);
    let error = fixture
        .actor
        .load_checkpoint_at_head()
        .await
        .expect_err("short fallback cannot establish ownership");
    assert!(
        error.contains("injected primary snapshot failure"),
        "{error}"
    );
    assert!(error.contains("unproven"), "{error}");
    fixture.actor.wal_store.mode.store(0, Ordering::Relaxed);
    fixture
        .actor
        .wal_store
        .snapshot_reads
        .store(0, Ordering::Relaxed);
    use std::io::Write;
    let mut tail = std::fs::OpenOptions::new()
        .append(true)
        .open(fixture.actor.wal_store.inner.wal_path(&fixture.actor.key))
        .unwrap();
    tail.write_all(b"corrupt unsynced tail\n").unwrap();
    let checkpoint = fixture.actor.load_checkpoint_at_head().await.unwrap();
    assert_eq!(checkpoint.at_seq, 4);
    assert_eq!(checkpoint.ownership.unwrap().episodes, 2);
    fixture.close().await;
}
