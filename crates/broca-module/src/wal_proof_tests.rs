use super::tests::{min_config, text_req};
use super::*;
use broca_core::{FileLeaseStore, FileWalStore, LeaseStore};
use std::sync::atomic::{AtomicBool, AtomicUsize};

struct FaultWal {
    inner: Arc<FileWalStore>,
    mode: AtomicUsize,
    fail_snapshot: AtomicBool,
    snapshot_reads: AtomicUsize,
    range_reads: AtomicUsize,
    tail_reads: AtomicUsize,
    tail_limit: AtomicUsize,
    after_read: std::sync::Mutex<Option<std::sync::Weak<WalHandle>>>,
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
        self.range_reads.fetch_add(1, Ordering::Relaxed);
        let range = self.inner.read_records_range(key, after, through, anchor)?;
        if let Some(writer) = self.after_read.lock().unwrap().take() {
            writer
                .upgrade()
                .unwrap()
                .record_terminal(refusal_terminal())
                .unwrap();
        }
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

    fn read_records_tail_bounded(
        &self,
        key: &LineageKey,
        bytes: u64,
    ) -> Result<Option<broca_wal::WalRangeRead>, broca_wal::WalError> {
        self.tail_reads.fetch_add(1, Ordering::Relaxed);
        self.tail_limit.fetch_max(bytes as usize, Ordering::Relaxed);
        self.inner.read_records_tail_bounded(key, bytes)
    }
}

struct CountingExecutor(AtomicUsize);

#[async_trait::async_trait]
impl RunExecutor for CountingExecutor {
    fn compaction_views(&self) -> Option<Arc<dyn broca_core::CompactionViews>> {
        Some(Arc::new(broca_core::ViewReadyInbox::default()))
    }
    async fn execute(
        &self,
        job: crate::executor::RunJob,
        _: mpsc::Sender<broca_core::OutputEvent>,
    ) -> Result<broca_core::RunSummary, broca_core::RunError> {
        assert!(
            job.control.compaction_views.is_some(),
            "actor omitted the executor's ready inbox"
        );
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

#[tokio::test]
async fn actor_installs_the_executors_compaction_inbox_on_run_control() {
    let mut fixture = Fixture::with_ceiling(100).await;
    let outcome = fixture.send_new(broca_protocol::Delivery::Queue).await;
    assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
    assert_eq!(fixture.executor.0.load(Ordering::Relaxed), 1);
    fixture.close().await;
}

struct Fixture {
    root: std::path::PathBuf,
    actor: LineageActor<FaultWal>,
    executor: Arc<CountingExecutor>,
    request: SubmissionRequest,
}

fn queued_record(
    request: SubmissionRequest,
    id: &str,
    retention: Option<u64>,
    ts_ms: Option<u64>,
) -> WalRecord {
    WalRecord::Queued {
        written_at_ms: None,
        retention,
        ts_ms,
        hooks: None,
        keep_warm: None,
        conversation_key: None,
        delivery: None,
        send_id: Some(id.into()),
        title: None,
        submission_id: broca_protocol::SubmissionId(format!("sub-{id}")),
        request: Box::new(request),
        request_identity_digest: None,
        principal: None,
    }
}

// Inject a real enqueue-budget refusal. The selected sync enqueues a reserved RunFinished
// record, which bypasses and clears the earlier byte-budget refusal. No timing race is needed.
struct RefusingWal {
    inner: Box<dyn broca_wal::Wal + Send>,
    writer: Arc<std::sync::Mutex<std::sync::Weak<WalHandle>>>,
    sync_count: AtomicUsize,
    clear_at: usize,
}

const REFUSAL_MAX_BYTES: u64 = 1_000_000;

fn refusal_terminal() -> WalRecord {
    WalRecord::RunFinished {
        interrupt_reason: None,
        reason: broca_protocol::RunFinishReason::Completed,
        ts_ms: None,
        usage: Default::default(),
        error: None,
        retries_used: 0,
    }
}

impl broca_wal::Wal for RefusingWal {
    fn append(
        &mut self,
        fence: u64,
        record: &WalRecord,
        sync: bool,
    ) -> Result<u64, broca_wal::WalError> {
        self.inner.append(fence, record, sync)
    }
    fn sync(&self) -> Result<(), broca_wal::WalError> {
        self.inner.sync()?;
        let Some(writer) = self.writer.lock().unwrap().upgrade() else {
            return Ok(());
        };
        if self.sync_count.fetch_add(1, Ordering::Relaxed) + 1 == self.clear_at {
            writer.record_terminal(refusal_terminal()).unwrap();
        }
        Ok(())
    }
    fn replay(&self) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        self.inner.replay()
    }
    fn tail_after_seq(
        &self,
        after: u64,
    ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        self.inner.tail_after_seq(after)
    }
    fn last_seq(&self) -> u64 {
        self.inner.last_seq()
    }
    fn lineage_id(&self) -> Option<broca_wal::WalLineageId> {
        self.inner.lineage_id()
    }
    fn append_budget(&self) -> Option<(u64, u64, u64)> {
        Some((
            REFUSAL_MAX_BYTES,
            REFUSAL_MAX_BYTES - broca_wal::size_limit::reserve_bytes(0),
            0,
        ))
    }
}

impl Fixture {
    async fn refuse_next_append(&mut self, clear_at_sync: usize) {
        self.actor.shutdown_writer().await;
        let writer = Arc::new(std::sync::Mutex::new(std::sync::Weak::new()));
        let handle = Arc::new(
            WalHandle::spawn(
                Box::new(RefusingWal {
                    inner: self.actor.wal_store.open(&self.actor.key).unwrap(),
                    writer: writer.clone(),
                    sync_count: AtomicUsize::new(0),
                    clear_at: clear_at_sync,
                }),
                1,
            )
            .unwrap(),
        );
        *writer.lock().unwrap() = Arc::downgrade(&handle);
        self.actor.writer = Some(handle);
    }
    async fn new() -> Self {
        Self::with_history(true).await
    }

    async fn with_history(history: bool) -> Self {
        Self::with_retention(history, false).await
    }

    async fn with_retention(history: bool, retained: bool) -> Self {
        Self::with_placement(history, retained, retained).await
    }

    /// `service` installs the retention service; `retained` also places the WAL under the
    /// ephemeral root, as a retained first send does. A service without a retained WAL
    /// models a fresh state root whose first session is not (yet) retained.
    async fn with_placement(history: bool, service: bool, retained: bool) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "broca-wal-proof-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        let key = LineageKey::new(root.join("project"), "agent", "session");
        let wal_store = Arc::new(FaultWal {
            inner: Arc::new(FileWalStore::new(root.join("wal"))),
            mode: AtomicUsize::new(0),
            fail_snapshot: AtomicBool::new(false),
            snapshot_reads: AtomicUsize::new(0),
            range_reads: AtomicUsize::new(0),
            tail_reads: AtomicUsize::new(0),
            tail_limit: AtomicUsize::new(0),
            after_read: Default::default(),
        });
        let request = submission_from_config(min_config(), text_req(&key, "old").input);
        let digest = send_identity_digest(&request, broca_protocol::Delivery::Queue, None);
        let mut wal = wal_store
            .inner
            .open_with_retention(&key, retained.then_some(3600))
            .unwrap();
        for run in ["old", "latest"].into_iter().filter(|_| history) {
            wal.append(
                1,
                &WalRecord::RunStarted {
                    written_at_ms: None,
                    tags: None,
                    session_class: None,
                    retention: retained.then_some(3600),
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
                    interrupt_reason: None,
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
        if service {
            let store = Arc::new(broca_store::SqliteStore::open_in_memory().unwrap());
            registry.set_retention_service(Arc::new(
                crate::retention::RetentionService::new(
                    wal_store.inner.clone(),
                    store.clone(),
                    Arc::new(FileLeaseStore::new(root.join("leases"))),
                    store,
                    Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::MeteredApi, 1)),
                )
                .unwrap(),
            ));
        }
        let (fanout, _) = crate::fanout::Fanout::new(wal_store.clone(), key.clone());
        let approvals = fanout.approval_state();
        let (self_tx, rx) = mpsc::channel(64);
        let (terminal_tx, terminal_rx) = mpsc::unbounded_channel();
        let mut actor = LineageActor {
            ceiling_drain_held: false,
            compaction_watch: Default::default(),
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
            admitting_order: None,
            admitting_sent_by: None,
            immediate_ordered_drain: None,
            ordered_submissions: Default::default(),
            ended_submissions: Default::default(),
            delivered_submissions: Default::default(),
            restart_admitting: false,
            restart_waiters: Vec::new(),
            restart_recovery_owns_waiters: false,
            resuming_for_late_result: false,
            nested_exit_requested: false,
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
async fn refused_queue_and_retract_leave_memory_unchanged() {
    let mut failures = Vec::new();
    let mut queue = Fixture::new().await;
    queue.actor.ensure_wal_scan_loaded().await.unwrap();
    let send_keys = queue.actor.recorded_send_keys.clone();
    queue.refuse_next_append(1).await;
    // Keep a real append awaiting sync so the physical barrier runs the injected
    // terminal-clear race even though the Queued append itself will be refused.
    queue
        .actor
        .writer
        .as_ref()
        .unwrap()
        .record_terminal(refusal_terminal())
        .unwrap();
    let outcome = queue
        .actor
        .queue_prepared_submission(
            RunId("refused".into()),
            queue.request.clone(),
            SendIdentity {
                conversation_key: None,
                title: None,
                send_id: Some("refused".into()),
                request_identity_digest: Some("digest".into()),
            },
            true,
            broca_protocol::Delivery::Queue,
            Some(pre_user::PreparedInput {
                input: queue.request.input.clone(),
                hooks: None,
                refused: None,
            }),
        )
        .await;
    if outcome
        != (SendOutcome::SessionTooLarge {
            max_bytes: REFUSAL_MAX_BYTES,
        })
        || !queue.actor.pending_fifo.is_empty()
        || queue.actor.recorded_send_keys != send_keys
        || !queue.actor.skip_materialization_submissions.is_empty()
    {
        failures.push(format!(
            "refused queue changed memory or lost its type: {outcome:?}"
        ));
    }
    // A successful later barrier cannot retroactively accept the refused append.
    queue.actor.writer.as_ref().unwrap().sync().await.unwrap();
    assert!(!queue
        .actor
        .wal_store
        .read_records(&queue.actor.key)
        .unwrap()
        .iter()
        .any(|f| matches!(f.record, WalRecord::Queued { .. })));
    queue.close().await;

    let mut retract = Fixture::new().await;
    let id = broca_protocol::SubmissionId("sub-queued".into());
    let queued = retract
        .actor
        .queue_prepared_submission(
            RunId("queued".into()),
            retract.request.clone(),
            SendIdentity {
                conversation_key: None,
                title: None,
                send_id: Some("queued".into()),
                request_identity_digest: None,
            },
            true,
            broca_protocol::Delivery::Queue,
            Some(pre_user::PreparedInput {
                input: retract.request.input.clone(),
                hooks: None,
                refused: None,
            }),
        )
        .await;
    assert_eq!(
        queued,
        SendOutcome::Pending {
            submission_id: id.clone()
        }
    );
    retract.actor.ensure_wal_scan_loaded().await.unwrap();
    let steer = broca_core::SteerInbox::new();
    steer.seed_ordered(vec![broca_core::delivery_order::OrderedSend {
        admitted_at_ms: None,
        submission_id: id.clone(),
        principal: None,
        input: retract.request.input.clone(),
        order: broca_protocol::DeliveryOrder {
            modes: vec![broca_protocol::DeliveryMode::Steer],
            dedupe_key: None,
            expires_at_ms: None,
            template: None,
        },
    }]);
    retract.actor.set_active(RunState::Active {
        run_id: RunId("latest".into()),
        task_run_id: RunId("latest".into()),
        attempt_id: 1,
        skip_materialization: false,
        cancel: CancellationToken::new(),
        steer: steer.clone(),
        shutdown_seal: Default::default(),
        progress: Arc::default(),
        step_progress: Default::default(),
        handle: tokio::spawn(async {}),
        durable_finish: Arc::default(),
    });
    retract.refuse_next_append(1).await;
    // The run appends after the actor captures its read head, leaving work for the
    // retract barrier without changing the queued submission's pending fate.
    *retract.actor.wal_store.after_read.lock().unwrap() =
        Some(Arc::downgrade(retract.actor.writer.as_ref().unwrap()));
    let outcome = retract.actor.on_retract(id.clone()).await;
    if outcome
        != (RetractOutcome::SessionTooLarge {
            max_bytes: REFUSAL_MAX_BYTES,
        })
        || retract.actor.pending_fifo.len() != 1
        || !retract.actor.skip_materialization_submissions.contains(&id)
        || retract.actor.retracted_submissions.contains(&id)
        || retract.actor.prior_snapshot.is_none()
    {
        failures.push(format!(
            "refused retract changed memory or lost its type: {outcome:?}"
        ));
    }
    let live = steer
        .try_take_at_step_ordered(&RunId("latest".into()), 0, |_| Vec::new())
        .unwrap();
    if live.len() != 1 {
        failures.push("refused retract removed its live inbox copy".into());
    }
    retract.actor.writer.as_ref().unwrap().sync().await.unwrap();
    assert!(!retract
        .actor
        .wal_store
        .read_records(&retract.actor.key)
        .unwrap()
        .iter()
        .any(|f| matches!(f.record, WalRecord::Retracted { .. })));
    retract.actor.set_idle();
    retract.close().await;
    assert!(
        failures.is_empty(),
        "refused writes must not take effect: {failures:?}"
    );
}

#[tokio::test]
async fn long_retention_sync_is_bounded_and_repair_survives_restart() {
    let mut fixture = Fixture::with_retention(true, true).await;
    let service = fixture.actor.registry.retention_service().unwrap();
    let writer = fixture.actor.writer.as_ref().unwrap().clone();
    let mut large = fixture.request.clone();
    large.input[0].content = vec![broca_protocol::ContentBlock::Text {
        text: "x".repeat(1_000_000),
    }];
    writer
        .record(queued_record(large, "large", Some(3600), Some(1000)))
        .into_result()
        .unwrap();
    writer
        .record(queued_record(
            fixture.request.clone(),
            "tail",
            Some(1),
            Some(2000),
        ))
        .into_result()
        .unwrap();
    writer.sync().await.unwrap();
    fixture
        .actor
        .wal_store
        .range_reads
        .store(0, Ordering::Relaxed);
    for _ in 0..3 {
        fixture.actor.sync_retention_index().await.unwrap();
    }
    assert_eq!(
        fixture.actor.wal_store.range_reads.load(Ordering::Relaxed),
        0,
        "retention sync must not decode full history"
    );
    assert_eq!(
        fixture.actor.wal_store.tail_reads.load(Ordering::Relaxed),
        3
    );
    assert_eq!(
        fixture.actor.wal_store.tail_limit.load(Ordering::Relaxed),
        256 * 1024
    );
    assert!(fixture.actor.retention_deadline.is_none());
    assert!(service.index.live(&fixture.actor.key).unwrap().is_none());
    // The actor's lease defers enrollment repair; repeated syncs enqueue one retry path.
    assert_eq!(service.enroll_changes().unwrap(), 0);
    drop(writer);
    fixture.actor.shutdown_writer().await;
    drop(fixture.actor.lease.take());
    let restarted = crate::retention::RetentionService::new(
        service.wal.clone(),
        service.index.clone(),
        Arc::new(FileLeaseStore::new(fixture.root.join("leases"))),
        Arc::new(broca_store::SqliteStore::open_in_memory().unwrap()),
        Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::MeteredApi, 1)),
    )
    .unwrap();
    assert_eq!(restarted.recover_enrollment().unwrap(), 1);
    let row = service.index.live(&fixture.actor.key).unwrap().unwrap();
    assert_eq!(row.retention_s, 1);
    assert_eq!(row.expires_at_ms, Some(3000));
    assert_eq!(service.enroll_changes().unwrap(), 0);
    assert_eq!(
        row.wal_bytes,
        std::fs::metadata(service.wal.ephemeral_wal_path(&fixture.actor.key))
            .unwrap()
            .len()
    );
    fixture.close().await;
}

#[tokio::test]
async fn retention_sync_before_the_first_run_is_a_quiet_no_op_on_a_fresh_state_root() {
    // An actor syncs the retention index when it starts, before its first send has
    // appended RunStarted. On a fresh state root the ephemeral directory does not exist
    // yet, and an unstarted lineage has no retention policy to project, so the sync must
    // succeed without touching the ephemeral root or enrolling repair.
    let mut fixture = Fixture::with_placement(false, true, false).await;
    let service = fixture.actor.registry.retention_service().unwrap();
    let ephemeral = service.wal.ephemeral_root();
    assert!(!ephemeral.exists(), "fixture must model a fresh state root");
    for _ in 0..2 {
        let result = fixture.actor.sync_retention_index().await;
        assert_eq!(result, Ok(()), "unstarted lineage sync must not fail");
    }
    assert!(fixture.actor.retention_deadline.is_none());
    assert!(
        !ephemeral.exists(),
        "an unstarted lineage must not be remembered as a retained session"
    );
    assert!(service.index.live(&fixture.actor.key).unwrap().is_none());
    assert_eq!(
        fixture.actor.registry.retention_repairs_pending(),
        0,
        "nothing is unprojected, so nothing needs repair"
    );
    fixture.close().await;
}

/// The actor-start sync no longer writes a session sidecar before the first RunStarted, so a
/// retained session's sidecar and index row are first written by the sync that follows
/// admission. A crash between the retained RunStarted append and that sync leaves a retained
/// WAL with neither, and the writer's in-memory change hints die with the process. Boot
/// recovery must still find the WAL by walking the ephemeral root, rediscover its session from
/// the WAL itself, and give it its deadline once its interrupted run is sealed.
#[tokio::test]
async fn retained_first_send_cut_before_its_index_sync_is_recovered_at_boot() {
    let mut fixture = Fixture::with_placement(false, true, false).await;
    let service = fixture.actor.registry.retention_service().unwrap();
    let key = fixture.actor.key.clone();
    let sidecar = service
        .wal
        .ephemeral_wal_path(&key)
        .with_extension("session");
    // Production state roots already hold retained logs, so the ephemeral root exists and the
    // removed actor-start sidecar write would have succeeded there.
    std::fs::create_dir_all(service.wal.ephemeral_root()).unwrap();
    fixture.actor.sync_retention_index().await.unwrap();
    assert!(!sidecar.exists(), "actor start writes no sidecar");

    // Admission's durable step: the retained first RunStarted, which moves the WAL under
    // the ephemeral root. The process then dies before the post-admission index sync.
    let request = fixture.request.clone();
    let writer = fixture.actor.writer.as_ref().unwrap().clone();
    let digest = send_identity_digest(&request, broca_protocol::Delivery::Queue, None);
    writer
        .record(WalRecord::RunStarted {
            written_at_ms: None,
            tags: None,
            session_class: None,
            retention: Some(3600),
            hooks: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            marker_order_version: None,
            manifest: None,
            scope: None,
            send_id: Some("first".into()),
            request_identity_digest: Some(digest),
            project_id: None,
            principal: None,
            ts_ms: Some(1000),
            run_id: RunId("first".into()),
            session: key.clone(),
            config: Box::new(min_config()),
            input: request.input.clone().into(),
            origin: broca_wal::RunOrigin::Fresh,
            submission_id: None,
        })
        .into_result()
        .unwrap();
    writer.sync().await.unwrap();
    drop(writer);
    fixture.actor.shutdown_writer().await;
    drop(fixture.actor.lease.take());
    assert!(
        service.wal.ephemeral_wal_path(&key).exists(),
        "the WAL is retained"
    );
    assert!(!sidecar.exists(), "the cut leaves no sidecar");
    assert!(
        service.index.live(&key).unwrap().is_none(),
        "the cut leaves no index row"
    );

    // A restarted process: same durable state, but a new WAL store, so no writer hints.
    let restart = || {
        crate::retention::RetentionService::new(
            Arc::new(FileWalStore::new(fixture.root.join("wal"))),
            service.index.clone(),
            Arc::new(FileLeaseStore::new(fixture.root.join("leases"))),
            Arc::new(broca_store::SqliteStore::open_in_memory().unwrap()),
            Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::MeteredApi, 1)),
        )
        .unwrap()
    };
    let booted = restart();
    assert_eq!(booted.wal.take_retention_changes(), (vec![], false));
    assert_eq!(booted.recover_enrollment().unwrap(), 1);
    let row = service
        .index
        .live(&key)
        .unwrap()
        .expect("boot recovery finds the WAL");
    assert_eq!(row.retention_s, 3600);
    assert_eq!(
        row.expires_at_ms, None,
        "an unfinished run has no deadline yet"
    );
    assert!(sidecar.exists(), "recovery rewrites the sidecar");

    // Boot's crash-orphan seal finishes the interrupted run. It is appended through yet
    // another store so it nominates nothing to the next boot's service either.
    let mut sealed = FileWalStore::new(fixture.root.join("wal"))
        .open(&key)
        .unwrap();
    sealed
        .append(
            1,
            &WalRecord::RunFinished {
                interrupt_reason: None,
                reason: broca_protocol::RunFinishReason::Interrupted,
                ts_ms: Some(2000),
                usage: Default::default(),
                error: None,
                retries_used: 0,
            },
            true,
        )
        .unwrap();
    drop(sealed);
    let rebooted = restart();
    assert_eq!(rebooted.recover_enrollment().unwrap(), 1);
    let row = service.index.live(&key).unwrap().unwrap();
    assert_eq!(row.expires_at_ms, Some(2000 + 3600 * 1000));
    fixture.close().await;
}

#[test]
fn retention_warning_errors_keep_their_reason_and_drop_paths() {
    assert_eq!(
        content_free_error("No such file or directory (os error 2)"),
        "No such file or directory (os error 2)"
    );
    assert_eq!(
        content_free_error("conflicting live WALs: /home/u/.local/wal/ab.wal and C:\\s\\ab.wal"),
        "conflicting live WALs: <path> and <path>"
    );
}

#[tokio::test]
async fn stale_retention_summary_never_schedules_early_deletion() {
    for seconds in [1, 7200] {
        let mut fixture = Fixture::with_retention(true, true).await;
        let service = fixture.actor.registry.retention_service().unwrap();
        service.rederive(&fixture.actor.key).unwrap();
        let old = service.index.live(&fixture.actor.key).unwrap().unwrap();
        let writer = fixture.actor.writer.as_ref().unwrap().clone();
        let mut large = fixture.request.clone();
        large.input[0].content = vec![broca_protocol::ContentBlock::Text {
            text: "x".repeat(1_000_000),
        }];
        writer
            .record(queued_record(large, "large", Some(seconds), Some(2000)))
            .into_result()
            .unwrap();
        writer.sync().await.unwrap();
        fixture.actor.retention_deadline = old.expires_at_ms;
        fixture.actor.sync_retention_index().await.unwrap();
        assert!(
            fixture.actor.retention_deadline.is_none(),
            "stale summary must not schedule deletion"
        );
        assert_eq!(
            service.index.live(&fixture.actor.key).unwrap().unwrap(),
            old
        );
        drop(writer);
        fixture.actor.shutdown_writer().await;
        drop(fixture.actor.lease.take());
        assert_eq!(service.enroll_changes().unwrap(), 1);
        let repaired = service.index.live(&fixture.actor.key).unwrap().unwrap();
        assert_eq!(repaired.expires_at_ms, Some(2000 + seconds * 1000));
        fixture.close().await;
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

impl Fixture {
    async fn with_ceiling(size: u64) -> Self {
        let mut fixture = Self::with_history(false).await;
        let manifest = broca_wal::PlanManifest {
            plan: broca_wal::OpaqueJson::from_json_text(
                r#"{"compaction_item":{"provider":"provider","params":{}},"composition":{}}"#,
            )
            .unwrap(),
            plan_identity: "compaction".into(),
            composition_digest: "digest".into(),
            tools: Arc::new(vec![]),
            system_text: String::new(),
            system_digest: String::new(),
            join_version: 1,
            role_versions: Default::default(),
            fetches: vec![],
            declarations: vec![],
        };
        let writer = fixture.actor.writer.as_ref().unwrap();
        let mut config = min_config();
        config.context_limit = Some(10_000);
        let started: WalRecord = serde_json::from_value(serde_json::json!({
            "type":"run_started", "run_id":"prior", "session":fixture.actor.key,
            "config":config, "input":fixture.request.input, "origin":broca_wal::RunOrigin::Fresh,
            "manifest":manifest,
        }))
        .unwrap();
        writer.record(started);
        writer.record(serde_json::from_value(serde_json::json!({
            "type":"compaction_message", "provider":"provider", "kind":"setup", "compaction_id":"head", "version":1,
            "range":{"from":0,"to":0}, "replacement":[], "sha256":broca_wal::compaction_replacement_sha256(&[]).unwrap(),
            "ceiling":r#"{"at_share":0.8,"wait_ms":50}"#
        })).unwrap());
        writer.record(WalRecord::RunFinished {
            interrupt_reason: None,
            reason: broca_protocol::RunFinishReason::Completed,
            ts_ms: None,
            error: None,
            usage: Default::default(),
            retries_used: 0,
        });
        writer.sync().await.unwrap();
        fixture.record_ceiling_notice(size, 1).await;
        fixture.actor.refresh_admission_view().await.unwrap();
        fixture
    }
    async fn record_ceiling_notice(&mut self, size: u64, seq: u64) {
        let writer = self.actor.writer.as_ref().unwrap();
        let notice = broca_core::compaction_provider::notice::AdvanceNotice::new(
            broca_core::agent_session_ref(&self.actor.key),
            seq,
            writer.lineage_id().unwrap().to_string(),
            0,
            "a".repeat(64),
            size,
            "m",
        )
        .with_window(10_000);
        writer.record(WalRecord::CompactionNotice {
            seq,
            headers: notice.to_headers(),
        });
        writer.sync().await.unwrap();
    }
    fn durable_count(&self) -> usize {
        self.actor
            .wal_store
            .read_records(&self.actor.key)
            .unwrap()
            .len()
    }
    async fn send_new(&mut self, delivery: broca_protocol::Delivery) -> SendOutcome {
        let mut request = self.request.clone();
        request.input = text_req(&self.actor.key, "new").input;
        // The session runs under a fetch plan, which owns its tools; a later send that names
        // tools is refused before the ceiling check this fixture exists to reach.
        request.tools = None;
        self.actor
            .on_send(
                RunId("new".into()),
                request,
                RunKind::AppendEpisode,
                SendKey::SendId("new".into()),
                false,
                delivery,
                None,
                None,
                None,
                None,
                None,
                false,
            )
            .await
    }
}

#[tokio::test]
async fn context_ceiling_receipt_refuses_idle_start_and_joining_steer_without_writes() {
    for delivery in [
        broca_protocol::Delivery::Queue,
        broca_protocol::Delivery::Steer,
        broca_protocol::Delivery::Interrupt,
    ] {
        let mut fixture = Fixture::with_ceiling(8_000).await;
        let before = fixture.durable_count();
        let outcome = fixture.send_new(delivery).await;
        assert!(
            matches!(outcome, SendOutcome::ContextCeiling(ref d) if d.window == 10_000 && d.size >= 8_000),
            "{outcome:?}"
        );
        assert_eq!(fixture.durable_count(), before);
        assert_eq!(fixture.executor.0.load(Ordering::Relaxed), 0);
        fixture.close().await;
    }
    let mut fixture = Fixture::with_ceiling(100).await;
    let mut started = fixture
        .actor
        .wal_store
        .read_records(&fixture.actor.key)
        .unwrap()[0]
        .record
        .clone();
    if let WalRecord::RunStarted { run_id, .. } = &mut started {
        *run_id = RunId("running".into());
    }
    fixture.actor.writer.as_ref().unwrap().record(started);
    fixture.actor.writer.as_ref().unwrap().sync().await.unwrap();
    fixture.actor.prior_snapshot = None;
    let run = fixture
        .actor
        .start_run(
            RunId("running".into()),
            fixture.request.clone(),
            RunKind::FreshOrResume,
            None,
            SendIdentity::default(),
            false,
        )
        .await;
    assert!(run.is_ok());
    fixture.record_ceiling_notice(8_000, 2).await;
    let before = fixture.durable_count();
    let outcome = fixture.send_new(broca_protocol::Delivery::Steer).await;
    assert!(
        matches!(outcome, SendOutcome::ContextCeiling(_)),
        "{outcome:?}"
    );
    assert_eq!(fixture.durable_count(), before);
    fixture.close().await;
}

#[tokio::test]
async fn a_ceiling_held_queue_head_stays_pending_without_writes_and_wakes_after_a_view() {
    let mut fixture = Fixture::with_ceiling(8_000).await;
    let outcome = fixture
        .actor
        .queue_submission(
            RunId("queued".into()),
            fixture.request.clone(),
            SendIdentity::default(),
            false,
            broca_protocol::Delivery::Queue,
        )
        .await;
    assert!(
        matches!(outcome, SendOutcome::Pending { .. }),
        "{outcome:?}"
    );
    let before = fixture.durable_count();
    fixture.actor.drain_queue_one().await.unwrap();
    assert_eq!(fixture.durable_count(), before);
    assert_eq!(fixture.actor.pending_fifo.len(), 1);
    let records = fixture
        .actor
        .wal_store
        .read_records(&fixture.actor.key)
        .unwrap();
    assert_eq!(
        broca_core::delivery_order::queued_send_fate(records.iter().map(|r| &r.record), "queued"),
        Some(broca_core::delivery_order::QueuedSendFate::Pending)
    );
    assert!(fixture.actor.ceiling_drain_held);
    assert_eq!(fixture.executor.0.load(Ordering::Relaxed), 0);
    let writer = fixture.actor.writer.as_ref().unwrap();
    let view = broca_core::compaction_provider::view::CompactionView::new(
        broca_core::agent_session_ref(&fixture.actor.key),
        2,
        writer.lineage_id().unwrap().to_string(),
        broca_core::compaction_provider::lineage::MessageRef {
            ordinal: 0,
            mid: "m0".into(),
        },
        vec![broca_core::compaction_provider::view::ViewRange::replace(
            0,
            1,
            serde_json::to_string(&fixture.request.input).unwrap(),
        )],
    );
    writer.record(WalRecord::CompactionViewApplied {
        provider: "provider".into(),
        view_version: 2,
        view: broca_wal::OpaqueJson::from_json_text(serde_json::to_string(&view).unwrap()).unwrap(),
        choice: broca_wal::CompactionViewChoice::Applied,
    });
    writer.sync().await.unwrap();
    fixture.record_ceiling_notice(100, 2).await;
    let (reply, complete) = tokio::sync::oneshot::channel();
    fixture
        .actor
        .handle_cmd(LineageCmd::CompactionChanged { reply })
        .await;
    complete.await.unwrap().unwrap();
    assert_eq!(
        fixture.executor.0.load(Ordering::Relaxed),
        1,
        "view wake must re-attempt admission"
    );
    assert!(!fixture.actor.ceiling_drain_held);
    fixture.close().await;
}

#[tokio::test]
async fn live_actor_status_reads_the_ceiling_wait_and_clears_it_after_the_decision() {
    let mut fixture = Fixture::with_ceiling(100).await;
    let mut started = fixture
        .actor
        .wal_store
        .read_records(&fixture.actor.key)
        .unwrap()[0]
        .record
        .clone();
    if let WalRecord::RunStarted { run_id, .. } = &mut started {
        *run_id = RunId("running".into());
    }
    let writer = fixture.actor.writer.as_ref().unwrap().clone();
    writer.record(started);
    writer.sync().await.unwrap();
    fixture.actor.prior_snapshot = None;
    assert!(fixture
        .actor
        .start_run(
            RunId("running".into()),
            fixture.request.clone(),
            RunKind::FreshOrResume,
            None,
            SendIdentity::default(),
            false
        )
        .await
        .is_ok());
    writer.record(WalRecord::ContextCeilingWait {
        step_id: broca_protocol::StepId(1),
        deadline_ms: 987_654,
        unavailable: None,
    });
    writer.sync().await.unwrap();
    for frame in fixture
        .actor
        .wal_store
        .read_records(&fixture.actor.key)
        .unwrap()
    {
        fixture
            .actor
            .approvals
            .observe(frame.seq, frame.fence, writer.lineage_id(), &frame.record);
    }
    let state = tokio::time::timeout(
        Duration::from_secs(5),
        fixture.actor.resolve_run_state(&RunId("running".into())),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(state.state, RunIndexState::Active);
    assert_eq!(state.ceiling_deadline_ms, Some(987_654));
    writer.record(WalRecord::ContextCeilingDecided {
        step_id: broca_protocol::StepId(1),
        detail: None,
    });
    writer.sync().await.unwrap();
    for frame in fixture
        .actor
        .wal_store
        .read_records(&fixture.actor.key)
        .unwrap()
    {
        fixture
            .actor
            .approvals
            .observe(frame.seq, frame.fence, writer.lineage_id(), &frame.record);
    }
    let state = tokio::time::timeout(
        Duration::from_secs(5),
        fixture.actor.resolve_run_state(&RunId("running".into())),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(state.ceiling_deadline_ms, None);
    drop(writer);
    fixture.close().await;
}
