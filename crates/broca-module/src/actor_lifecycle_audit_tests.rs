//! Lifecycle edges of the session actor that only show up under specific interleavings: a run
//! terminal published after the idle tick was chosen, a close that loses its registry slot, a
//! command that reaches an expired session, a move fence that cannot be read, an import retry
//! preflight on an active session, and commands served inside a restart admission.
//!
//! Each test builds the actor directly so it can hold the actor at the exact point it checks.

use super::tests::{min_config, text_req};
use super::*;
use broca_core::{FileLeaseStore, FileWalStore, LeaseStore};
use broca_protocol::BindIdentity;
use std::sync::atomic::{AtomicBool, AtomicUsize};

/// File-backed WAL store with the two failures these tests inject.
struct ProbeWal {
    inner: Arc<FileWalStore>,
    /// Make every move-fence check fail with an I/O error rather than a move refusal.
    move_check_fails: AtomicBool,
    /// When armed, the next full read first waits for a signal that another task on the test's
    /// runtime sends. A read made on the runtime thread itself keeps that task from running,
    /// so the wait times out and `read_starved_runtime` records it.
    read_gate: std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    read_starved_runtime: AtomicBool,
}

impl WalStore for ProbeWal {
    fn check_move(&self, session: &BindIdentity, write: bool) -> Result<(), broca_wal::WalError> {
        if self.move_check_fails.load(Ordering::SeqCst) {
            return Err(broca_wal::WalError::Io(std::io::Error::other(
                "injected move-fence read failure",
            )));
        }
        self.inner.check_move(session, write)
    }

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
        let gate = self.read_gate.lock().unwrap().take();
        if let Some(gate) = gate {
            if gate.recv_timeout(Duration::from_secs(2)).is_err() {
                self.read_starved_runtime.store(true, Ordering::SeqCst);
            }
        }
        self.inner.read_records(key)
    }

    fn read_records_range(
        &self,
        key: &LineageKey,
        after: u64,
        through: Option<u64>,
        anchor: Option<&broca_wal::WalByteAnchor>,
    ) -> Result<broca_wal::WalRangeRead, broca_wal::WalError> {
        self.inner.read_records_range(key, after, through, anchor)
    }

    fn read_records_tail_bounded(
        &self,
        key: &LineageKey,
        bytes: u64,
    ) -> Result<Option<broca_wal::WalRangeRead>, broca_wal::WalError> {
        self.inner.read_records_tail_bounded(key, bytes)
    }
}

/// The session's log, with a switch that makes every later append fail. A failed append
/// poisons the session writer exactly as a real disk failure does.
struct FailableWal {
    inner: Box<dyn broca_wal::Wal + Send>,
    fail_appends: Arc<AtomicBool>,
}

impl broca_wal::Wal for FailableWal {
    fn append(
        &mut self,
        fence: u64,
        record: &WalRecord,
        sync: bool,
    ) -> Result<u64, broca_wal::WalError> {
        if self.fail_appends.load(Ordering::SeqCst) {
            return Err(broca_wal::WalError::Io(std::io::Error::other(
                "injected append failure",
            )));
        }
        self.inner.append(fence, record, sync)
    }
    fn recovery_byte_cap(&self) -> Option<u64> {
        self.inner.recovery_byte_cap()
    }
    fn append_budget_scope(&self) -> broca_wal::size_limit::SizeScope {
        self.inner.append_budget_scope()
    }
    fn sync(&self) -> Result<(), broca_wal::WalError> {
        self.inner.sync()
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
}

/// Completes runs immediately, or holds every run before its admission until the test releases
/// it and then refuses that admission, like a tool route the run depends on never coming up.
struct ProbeExecutor {
    hold_admission: bool,
    release: tokio::sync::Notify,
    session_closes: AtomicUsize,
}

#[async_trait::async_trait]
impl RunExecutor for ProbeExecutor {
    async fn execute(
        &self,
        job: crate::executor::RunJob,
        _: mpsc::Sender<broca_core::OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        if self.hold_admission {
            self.release.notified().await;
            return Err(RunError::Config("dependency refused admission".into()));
        }
        if let Some(ack) = job.on_admitted {
            let _ = ack.send(Ok(job.req.run_id));
        }
        Ok(completed())
    }

    async fn on_session_close(&self, _session: &BindIdentity) {
        self.session_closes.fetch_add(1, Ordering::SeqCst);
    }
}

fn completed() -> RunSummary {
    RunSummary {
        reason: broca_protocol::RunFinishReason::Completed,
        paused: None,
        steps: 1,
        usage: Default::default(),
        retries_used: 0,
        final_step_finish_reason: None,
    }
}

/// What the session's WAL holds before the actor opens it.
#[derive(Clone, Copy, PartialEq)]
enum Seed {
    /// One finished episode.
    Finished,
    /// One finished episode on a lineage retained for an hour. Its records carry no
    /// timestamps, so its retention deadline is one hour after the epoch.
    Retained,
    /// One episode paused by a daemon restart, waiting to resume.
    RestartPaused,
}

/// Retention deadline of a [`Seed::Retained`] lineage, and a clock reading past it.
const RETAINED_DEADLINE_MS: u64 = 3_600_000;
const AFTER_RETAINED_DEADLINE_MS: u64 = RETAINED_DEADLINE_MS + 1;

struct Fixture {
    root: std::path::PathBuf,
    actor: LineageActor<ProbeWal>,
    executor: Arc<ProbeExecutor>,
    wal_store: Arc<ProbeWal>,
    fail_appends: Arc<AtomicBool>,
}

impl Fixture {
    async fn new(seed: Seed, hold_admission: bool) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "broca-actor-lifecycle-audit-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let key = LineageKey::new(root.join("project"), "agent", "session");
        let wal_store = Arc::new(ProbeWal {
            inner: Arc::new(FileWalStore::new(root.join("wal"))),
            move_check_fails: AtomicBool::new(false),
            read_gate: std::sync::Mutex::new(None),
            read_starved_runtime: AtomicBool::new(false),
        });
        let retained = seed == Seed::Retained;
        let mut wal = wal_store
            .inner
            .open_with_retention(&key, retained.then_some(3600))
            .unwrap();
        wal.append(
            1,
            &WalRecord::RunStarted {
                written_at_ms: None,
                tags: None,
                session_class: None,
                retention: retained.then_some(3600),
                hooks: None,
                title: None,
                on_restart: (seed == Seed::RestartPaused)
                    .then_some(broca_protocol::request::OnRestart::Resume),
                keep_warm: None,
                marker_order_version: None,
                manifest: None,
                scope: None,
                send_id: None,
                request_identity_digest: None,
                project_id: None,
                principal: None,
                ts_ms: None,
                run_id: RunId("prior".into()),
                session: key.clone(),
                config: Box::new(min_config()),
                input: text_req(&key, "prior").input.into(),
                origin: broca_wal::RunOrigin::Fresh,
                submission_id: None,
            },
            false,
        )
        .unwrap();
        let end = if seed == Seed::RestartPaused {
            WalRecord::RunPaused {
                run_id: RunId("prior".into()),
                ts_ms: None,
                reason: broca_protocol::PauseReason::Restart,
                error: None,
            }
        } else {
            WalRecord::RunFinished {
                interrupt_reason: None,
                reason: broca_protocol::RunFinishReason::Completed,
                ts_ms: None,
                usage: Default::default(),
                error: None,
                retries_used: 0,
            }
        };
        wal.append(1, &end, true).unwrap();
        let fail_appends = Arc::new(AtomicBool::new(false));
        let writer = Arc::new(
            WalHandle::spawn(
                Box::new(FailableWal {
                    inner: wal,
                    fail_appends: fail_appends.clone(),
                }),
                1,
            )
            .unwrap(),
        );
        let lease = FileLeaseStore::new(root.join("leases"))
            .acquire(&broca_core::lease_key_for(&key))
            .unwrap();
        let executor = Arc::new(ProbeExecutor {
            hold_admission,
            release: tokio::sync::Notify::new(),
            session_closes: AtomicUsize::new(0),
        });
        let registry = crate::registry::LineageRegistry::new(Arc::new(super::tests::SeamOpener {
            lease_store: Arc::new(FileLeaseStore::new(root.join("unused-leases"))),
            wal_store: Arc::new(FileWalStore::new(root.join("unused-wal"))),
            idle_ttl: Duration::from_secs(60),
            executor: executor.clone(),
        }));
        if retained {
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
            retention_lineage: retained,
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
            lease: Some(ActorTenure::Lease(lease)),
            writer: Some(writer),
            executor: executor.clone(),
            wal_store: wal_store.clone(),
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
            session_change: Default::default(),
            session_change_recorded: false,
            session_change_rebuild: false,
            session_change_flushed: false,
            session_change_last_model: None,
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
            held_resume_owed: false,
            nested_exit_requested: false,
            warm_in_flight: Default::default(),
        };
        actor.refresh_admission_view().await.unwrap();
        Self {
            root,
            actor,
            executor,
            wal_store,
            fail_appends,
        }
    }

    /// Make this actor generation the registry's open slot for its session, so its close can
    /// be claimed and committed.
    fn publish_open_slot(&self) {
        let actor = &self.actor;
        actor.registry.publish_open_slot_for_test(
            &actor.key,
            ActorHandle {
                gen: actor.gen,
                tx: actor.self_tx.clone(),
                durable_head: actor.writer.as_ref().unwrap().durable_head(),
                run_in_flight: actor.run_in_flight.clone(),
                first_episode: actor.first_episode.clone(),
                approvals: actor.approvals.clone(),
            },
        );
    }

    /// Mark the actor Active with a run task whose join handle is `handle`.
    fn activate(
        &mut self,
        task_run_id: &str,
        attempt_id: u64,
        handle: tokio::task::JoinHandle<()>,
    ) {
        let shutdown_seal = broca_core::ShutdownSeal::with_cache_clock(
            self.actor.registry.cache_clock(&self.actor.key),
        );
        self.actor.set_active(RunState::Active {
            run_id: RunId(task_run_id.into()),
            task_run_id: RunId(task_run_id.into()),
            attempt_id,
            skip_materialization: false,
            cancel: CancellationToken::new(),
            steer: broca_core::SteerInbox::new(),
            shutdown_seal,
            progress: Arc::new(AtomicU64::new(0)),
            step_progress: Arc::new(std::sync::Mutex::new(None)),
            handle,
            durable_finish: Arc::default(),
            // This fixture deliberately takes the durable cancellation path when held state is unknown.
            provider_held: None,
        });
    }

    async fn close(mut self) {
        self.actor.stop_active_run().await;
        self.actor.emergency_teardown().await;
        let _ = std::fs::remove_dir_all(self.root);
    }
}

/// The idle tick can be chosen while the terminal lane is still empty, and the run can publish
/// its real terminal and finish right after. The backstop must deliver that terminal instead
/// of declaring it lost and tearing the actor down.
#[tokio::test]
async fn idle_tick_backstop_delivers_a_terminal_published_after_the_tick_was_chosen() {
    let mut fixture = Fixture::new(Seed::Finished, false).await;
    let handle = tokio::spawn(async {});
    while !handle.is_finished() {
        tokio::task::yield_now().await;
    }
    fixture.activate("late-terminal", 9, handle);
    // The run's guard published the real outcome before its task finished.
    deliver_terminal(
        fixture.actor.terminal_tx.clone(),
        RunId("late-terminal".into()),
        9,
        Box::new(Ok(completed())),
    );

    let must_exit = fixture.actor.idle_tick_backstop().await;

    assert!(
        !must_exit,
        "a delivered terminal must not be replaced by a synthetic Panicked and an emergency exit"
    );
    assert!(matches!(fixture.actor.state, RunState::Idle));
    fixture.close().await;
}

/// A run task that finished without publishing anything still gets the synthetic terminal.
#[tokio::test]
async fn idle_tick_backstop_still_recovers_a_genuinely_lost_terminal() {
    let mut fixture = Fixture::new(Seed::Finished, false).await;
    let handle = tokio::spawn(async {});
    while !handle.is_finished() {
        tokio::task::yield_now().await;
    }
    fixture.activate("lost-terminal", 3, handle);

    assert!(
        fixture.actor.idle_tick_backstop().await,
        "a finished run with no terminal ends this generation for WAL recovery"
    );
    fixture.close().await;
}

/// When a command finds the session expired, the expiry close already told the executor the
/// session closed and released the lease, after which a new generation may open. The teardown
/// the actor loop runs next must not repeat that close for the new generation's routes.
#[tokio::test]
async fn expiry_found_by_a_command_closes_the_session_once() {
    let mut fixture = Fixture::new(Seed::Retained, false).await;
    fixture.publish_open_slot();

    assert!(
        fixture.actor.expire_actor(AFTER_RETAINED_DEADLINE_MS).await,
        "the expired lineage closes"
    );
    // `run()` follows a command that returned exit with the emergency teardown.
    fixture.actor.emergency_teardown().await;

    assert_eq!(
        fixture.executor.session_closes.load(Ordering::SeqCst),
        1,
        "the executor's session close runs once per generation"
    );
    let _ = std::fs::remove_dir_all(&fixture.root);
}

/// A close that finds its registry slot already gone still owns the subscribers, writer and
/// lease of this generation, and must shut the writer down before the lease is released.
#[tokio::test]
async fn idle_close_without_its_registry_slot_still_shuts_down_writer_and_lease() {
    let mut fixture = Fixture::new(Seed::Finished, false).await;

    assert!(fixture.actor.try_close().await, "the actor exits");

    assert!(
        fixture.actor.writer.is_none(),
        "the session writer is shut down before the actor exits"
    );
    assert!(fixture.actor.lease.is_none(), "the lease is released");
    let _ = std::fs::remove_dir_all(&fixture.root);
}

/// An expiry that cannot claim the registry slot must not leave its elapsed deadline armed,
/// which would wake the actor loop at once and reread the whole WAL without pause.
#[tokio::test]
async fn expiry_without_its_registry_slot_backs_off() {
    let mut fixture = Fixture::new(Seed::Retained, false).await;

    assert!(!fixture.actor.expire_actor(AFTER_RETAINED_DEADLINE_MS).await);

    assert!(
        fixture
            .actor
            .retention_deadline
            .is_some_and(|deadline| deadline > AFTER_RETAINED_DEADLINE_MS),
        "the retry is scheduled in the future, got {:?}",
        fixture.actor.retention_deadline
    );
    fixture.close().await;
}

/// An unreadable move fence says nothing about whether durable work is waiting, so startup
/// recovery must fail into its back-off instead of reporting a successful empty recovery.
#[tokio::test]
async fn recovery_does_not_treat_an_unreadable_move_fence_as_nothing_to_recover() {
    let mut fixture = Fixture::new(Seed::Finished, false).await;
    fixture
        .wal_store
        .move_check_fails
        .store(true, Ordering::SeqCst);

    let recovered = fixture.actor.recover_durable_work().await;

    assert!(recovered.is_err(), "got {recovered:?}");
    fixture
        .wal_store
        .move_check_fails
        .store(false, Ordering::SeqCst);
    fixture.close().await;
}

/// The import retry preflight on an active session reads the whole WAL. It must run on the
/// blocking pool: this runtime has one thread, and the read waits for a task on that thread.
#[tokio::test]
async fn active_import_retry_preflight_reads_off_the_runtime_thread() {
    let mut fixture = Fixture::new(Seed::Finished, false).await;
    fixture.activate("active", 1, tokio::spawn(std::future::pending::<()>()));
    let (signal, gate) = std::sync::mpsc::channel();
    *fixture.wal_store.read_gate.lock().unwrap() = Some(gate);
    tokio::spawn(async move {
        let _ = signal.send(());
    });

    let request = broca_core::ImportRequest {
        project_id: None,
        principal: None,
        run_id: RunId("import".into()),
        session: fixture.actor.key.clone(),
        messages: text_req(&fixture.actor.key, "import").input,
        origin: broca_core::ImportOrigin {
            source_ref: "audit".into(),
        },
    };
    let outcome = fixture.actor.on_import(request).await;

    assert!(
        !fixture
            .wal_store
            .read_starved_runtime
            .load(Ordering::SeqCst),
        "the preflight read blocked the runtime thread (outcome {outcome:?})"
    );
    assert!(matches!(outcome, ImportOutcome::Busy), "{outcome:?}");
    fixture.close().await;
}

/// Start the restart-paused episode again outside startup restart recovery, as a cancel
/// finalizer or a queued drain does. Its admission waits on the executor.
async fn restart_admission(actor: &mut LineageActor<ProbeWal>) -> Result<RunId, StartRunFailure> {
    let request = submission_from_config(min_config(), text_req(&actor.key, "prior").input);
    actor
        .start_run(
            RunId("prior".into()),
            request,
            RunKind::FreshOrResume,
            None,
            SendIdentity {
                title: None,
                conversation_key: None,
                send_id: None,
                request_identity_digest: None,
            },
            false,
        )
        .await
}

/// Queue `cmd` and then a status probe, and return the probe's reply. Commands are served in
/// order, so the reply proves `cmd` was dispatched.
async fn queue_with_probe(
    tx: &mpsc::Sender<LineageCmd>,
    cmd: LineageCmd,
) -> oneshot::Receiver<LineageStatus> {
    tx.send(cmd).await.unwrap();
    let (reply, probe) = oneshot::channel();
    tx.send(LineageCmd::Status { reply }).await.unwrap();
    probe
}

/// A `ResumeRestart` that arrives while some other path is admitting the restart-paused
/// episode is parked. It must be answered when that admission ends, or the boot resume loop,
/// which awaits each session's reply in turn, stalls behind this session.
#[tokio::test]
async fn resume_restart_parked_during_another_restart_admission_is_answered() {
    let mut fixture = Fixture::new(Seed::RestartPaused, true).await;
    let (reply, waiter) = oneshot::channel();
    let dispatched = queue_with_probe(
        &fixture.actor.self_tx.clone(),
        LineageCmd::ResumeRestart { reply },
    )
    .await;
    let executor = fixture.executor.clone();
    let release = async move {
        dispatched.await.unwrap();
        // The resume request is parked; now let the admission end.
        executor.release.notify_one();
    };

    let (admission, ()) = broca_test_support::event_timeout(
        "the restart admission ends",
        async { tokio::join!(restart_admission(&mut fixture.actor), release) },
        || "the restart admission is still waiting".into(),
    )
    .await
    .expect("the restart admission ends once the executor refuses it");
    assert!(admission.is_err(), "the executor refused admission");

    let mut waiter = waiter;
    let answer = waiter
        .try_recv()
        .expect("the parked resume request is answered when the admission ends");
    assert!(
        answer.is_err(),
        "the refused admission is reported: {answer:?}"
    );
    fixture.close().await;
}

/// A command served inside a restart admission can observe that the session writer has been
/// poisoned. The admission must stop there rather than keep waiting to admit work into a
/// generation that is about to be torn down.
#[tokio::test]
async fn restart_admission_stops_when_a_nested_command_finds_the_writer_poisoned() {
    let mut fixture = Fixture::new(Seed::RestartPaused, true).await;
    let tx = fixture.actor.self_tx.clone();
    let writer = fixture.actor.writer.clone().unwrap();
    let fail_appends = fixture.fail_appends.clone();
    let poison = async move {
        let (reply, first) = oneshot::channel();
        tx.send(LineageCmd::Status { reply }).await.unwrap();
        first.await.unwrap();
        // The admission is now serving commands. Fail the disk under it.
        fail_appends.store(true, Ordering::SeqCst);
        let _ = writer.record(WalRecord::RunFinished {
            interrupt_reason: None,
            reason: broca_protocol::RunFinishReason::Completed,
            ts_ms: None,
            usage: Default::default(),
            error: None,
            retries_used: 0,
        });
        assert!(
            writer.sync().await.is_err(),
            "the failed append poisons the writer"
        );
        let (reply, _second) = oneshot::channel();
        tx.send(LineageCmd::Status { reply }).await.unwrap();
    };

    let (admission, ()) = broca_test_support::event_timeout(
        "the restart admission stops",
        async { tokio::join!(restart_admission(&mut fixture.actor), poison) },
        || "the restart admission is still waiting to admit".into(),
    )
    .await
    .expect("the restart admission stops once a served command sees the poisoned writer");
    assert!(admission.is_err(), "the admission ends with an error");
    assert!(fixture.actor.writer_poisoned);
    fixture.executor.release.notify_one();
    fixture.actor.stop_active_run().await;
    drop(fixture.actor.lease.take());
    let _ = std::fs::remove_dir_all(&fixture.root);
}
