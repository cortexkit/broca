//! The startup crash-orphan pass: a run a crashed process left with a `RunStarted` and no
//! terminal is sealed `Interrupted` at the next boot, through the shutdown seal's own code.
//!
//! Every fixture here is a real file WAL, a real file lease store, and a real SQLite run index,
//! seeded the way a SIGKILLed process leaves them, with the "process start" instant taken after
//! the seed so the WAL predates the boot the pass is run for.

use super::tests::min_config;
use super::*;
use crate::opener::DefaultOpener;
use crate::registry::{CrashOrphanSweep, LineageRegistry};
use broca_core::{FileLeaseStore, FileWalStore, LeaseStore};
use broca_protocol::content::{AssistantMessage, ContentBlock, ToolCall};
use broca_protocol::{BatchId, BindIdentity, MessageId, RunFinishReason, ToolCallId};

const ORPHAN: &str = "orphan-run";
const CALL: &str = "orphan-call";
const BATCH: &str = "orphan-batch";

#[tokio::test]
async fn crash_orphan_seal_projects_ended_and_claim_check_is_session_owned() {
    use crate::serve::ServeHandler;
    let durable = Arc::new(std::sync::Mutex::new(None));
    let seen = durable.clone();
    let fixture = Fixture::configured(
        |index| index,
        Some(Arc::new(move |_, seq, epoch| {
            *seen.lock().unwrap() = Some((seq, epoch));
        })),
    );
    fixture.index.enable_bus_events("box_test").unwrap();
    fixture.seed(vec![run_started(&fixture.key)]);
    let marked = fixture.sweep(Fixture::boot_after_seed()).await;
    assert_eq!(marked.marked, 1);
    let wal = Arc::new(fixture.wal_store());
    let (head, observed_epoch) = durable
        .lock()
        .unwrap()
        .expect("orphan writer must notify durable event projection");
    let range = wal
        .read_records_range(&fixture.key, 0, Some(head), None)
        .unwrap();
    let epoch = range.lineage_id.unwrap().to_string();
    assert_eq!(observed_epoch.unwrap().to_string(), epoch);
    let broca_wal::WalRangeRecords::Complete(records) = range.into_records() else {
        panic!("sealed WAL absent")
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    fixture
        .index
        .project_bus_events(&fixture.key, &epoch, &records, now)
        .unwrap();
    let started = fixture.index.pending_bus_event().unwrap().unwrap();
    fixture
        .index
        .acknowledge_bus_event(
            &started,
            cortexkit_bus_trait::PublishReceipt::new(1, false),
            now,
        )
        .unwrap();
    let ended = fixture
        .index
        .pending_bus_event()
        .unwrap()
        .expect("crash orphan must publish run.ended");
    assert!(ended.subject.ends_with(".run.ended.v1"));
    let body: serde_json::Value = serde_json::from_slice(&ended.body).unwrap();
    assert_eq!(body["terminal"], "interrupted");
    assert_eq!(body["seq"], records.last().unwrap().seq);
    assert!(body.get("error").is_none());
    fixture.stop().await;
    let opener = DefaultOpener::new(
        Arc::new(FileLeaseStore::new(fixture.root.join("leases"))),
        wal.clone(),
        fixture.executor.clone(),
    );
    let registry =
        LineageRegistry::new_with_reader(Arc::new(opener), wal, Some(fixture.index.clone()));
    let found = registry
        .run_event_get(&fixture.key, &ended.event_key)
        .await
        .unwrap();
    assert_eq!(
        found,
        broca_wire::RunEventGetResponse::Found {
            event_key: ended.event_key.clone(),
            jcs: String::from_utf8(ended.body).unwrap()
        }
    );
    let other = BindIdentity::new(fixture.root.join("project"), "runner", "foreign-session");
    assert_eq!(
        registry
            .run_event_get(&other, &ended.event_key)
            .await
            .unwrap(),
        broca_wire::RunEventGetResponse::Absent
    );
    assert_eq!(
        registry
            .run_event_get(&fixture.key, &"0".repeat(64))
            .await
            .unwrap(),
        broca_wire::RunEventGetResponse::Absent
    );
}

/// Records every run it is asked to execute and completes it at once. A crash orphan the pass
/// fails to seal is resumed by open-time recovery, which shows up here as its run id.
#[derive(Default)]
struct Recording {
    executed: std::sync::Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl RunExecutor for Recording {
    async fn execute(
        &self,
        job: RunJob,
        _out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, broca_core::RunError> {
        self.executed.lock().unwrap().push(job.req.run_id.0.clone());
        job.writer.record(WalRecord::RunStarted {
            written_at_ms: None,
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
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            ts_ms: None,
            run_id: job.req.run_id.clone(),
            session: job.req.session.clone(),
            config: Box::new(min_config()),
            input: job.req.input.into(),
            origin: broca_wal::RunOrigin::Fresh,
            submission_id: None,
        });
        job.writer.record(WalRecord::RunFinished {
            interrupt_reason: None,
            reason: RunFinishReason::Completed,
            ts_ms: None,
            usage: broca_protocol::Usage::default(),
            error: None,
            retries_used: 0,
        });
        job.writer
            .sync()
            .await
            .map_err(|e| broca_core::RunError::Wal(e.to_string()))?;
        if let Some(admitted) = job.on_admitted {
            let _ = admitted.send(Ok(job.req.run_id.clone()));
        }
        Ok(RunSummary {
            reason: RunFinishReason::Completed,
            paused: None,
            steps: 0,
            usage: broca_protocol::Usage::default(),
            retries_used: 0,
            final_step_finish_reason: None,
        })
    }
}

/// Runs each send through broca's real run loop with a scripted provider, the way the daemon's
/// executor does, so the provider sees the exact request the loop renders from the WAL.
struct CoreLoop {
    wal_store: Arc<FileWalStore>,
    provider: broca_core::StubProvider,
}

#[async_trait::async_trait]
impl RunExecutor for CoreLoop {
    async fn execute(
        &self,
        job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, broca_core::RunError> {
        let mode = match job.kind {
            RunKind::FreshOrResume => broca_core::StartMode::FreshOrResume,
            RunKind::AppendEpisode => broca_core::StartMode::AppendEpisode,
            RunKind::ResumeAfterAuth => broca_core::StartMode::ResumeAfterAuth,
        };
        broca_core::run_loop_on_writer_admitted_with_prior_snapshot(
            job.req,
            job.writer.as_ref(),
            mode,
            &self.provider,
            &broca_core::StubToolPlane::new(),
            &broca_core::AllowAll,
            self.wal_store.as_ref(),
            None,
            job.control,
            &out,
            job.on_admitted,
            Some(&job.prior_snapshot),
        )
        .await
    }
}

struct Fixture {
    root: std::path::PathBuf,
    key: BindIdentity,
    index: Arc<broca_store::SqliteStore>,
    executor: Arc<Recording>,
    registry: LineageRegistry,
}

/// The fixture's SQLite run index with one switch: while `fail_terminal` is set, every terminal
/// write fails, the way a locked or full store.db fails it. Everything else reaches SQLite.
struct FlakyIndex {
    inner: Arc<broca_store::SqliteStore>,
    fail_terminal: std::sync::atomic::AtomicBool,
}

impl RunIndex for FlakyIndex {
    fn index_run_active(&self, row: &broca_core::RunIndexRow) -> Result<(), String> {
        self.inner.index_run_active(row)
    }

    fn index_run_terminal(
        &self,
        run_id: &str,
        state: RunIndexState,
        usage: Option<broca_protocol::Usage>,
        final_step_finish_reason: Option<broca_protocol::FinishReason>,
        retries_used: u32,
    ) -> Result<(), String> {
        if self.fail_terminal.load(Ordering::Acquire) {
            return Err("injected run-index write failure".into());
        }
        self.inner
            .index_run_terminal(run_id, state, usage, final_step_finish_reason, retries_used)
    }

    fn lookup_run(&self, run_id: &str) -> Result<Option<broca_core::RunIndexRow>, String> {
        self.inner.lookup_run(run_id)
    }
}

impl Fixture {
    fn new() -> Self {
        Self::with_opener_index(|index| index)
    }

    /// A fixture whose actors write the run index through `wrap(index)`; the fixture's own
    /// seeding and assertions still read the SQLite index directly.
    fn with_opener_index(
        wrap: impl FnOnce(Arc<broca_store::SqliteStore>) -> Arc<dyn RunIndex>,
    ) -> Self {
        Self::configured(wrap, None)
    }

    fn configured(
        wrap: impl FnOnce(Arc<broca_store::SqliteStore>) -> Arc<dyn RunIndex>,
        observer: Option<Arc<broca_core::wal_writer::DurableWalObserver>>,
    ) -> Self {
        Self::build(wrap, observer, None)
    }

    /// A fixture whose sends run broca's real run loop against `provider`, so a test can see
    /// the model request a send after the seal renders.
    fn with_core_loop(provider: broca_core::StubProvider) -> Self {
        Self::build(|index| index, None, Some(provider))
    }

    fn build(
        wrap: impl FnOnce(Arc<broca_store::SqliteStore>) -> Arc<dyn RunIndex>,
        observer: Option<Arc<broca_core::wal_writer::DurableWalObserver>>,
        provider: Option<broca_core::StubProvider>,
    ) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "broca-crash-orphan-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(root.join("project")).unwrap();
        let key = BindIdentity::new(root.join("project"), "runner", "session");
        let index = Arc::new(broca_store::SqliteStore::open(root.join("run-index.db")).unwrap());
        let executor = Arc::new(Recording::default());
        let mut wal = FileWalStore::new(root.join("wal"));
        if let Some(observer) = observer {
            wal = wal.with_durable_observer(observer);
        }
        let wal = Arc::new(wal);
        let runs: Arc<dyn RunExecutor> = match provider {
            Some(provider) => Arc::new(CoreLoop {
                wal_store: wal.clone(),
                provider,
            }),
            None => executor.clone(),
        };
        let opener = DefaultOpener::new(
            Arc::new(FileLeaseStore::new(root.join("leases"))),
            wal.clone(),
            runs,
        )
        .with_run_index(wrap(index.clone()));
        let registry = LineageRegistry::new_with_reader(Arc::new(opener), wal, None);
        Self {
            root,
            key,
            index,
            executor,
            registry,
        }
    }

    fn wal_store(&self) -> FileWalStore {
        FileWalStore::new(self.root.join("wal"))
    }

    /// Leave the WAL and index exactly as a process killed mid-run leaves them: the records
    /// on disk, and the admission-time `active` row.
    fn seed(&self, records: Vec<WalRecord>) {
        let mut wal = self.wal_store().open(&self.key).unwrap();
        for record in records {
            wal.append(1, &record, true).unwrap();
        }
        drop(wal);
        self.index
            .index_run_active(&broca_core::RunIndexRow {
                withdrawals_owed: false,
                title: None,
                run_id: ORPHAN.into(),
                session: self.key.clone(),
                submission_id: None,
                request_identity_digest: None,
                state: RunIndexState::Active,
                usage: None,
                final_step_finish_reason: None,
                retries_used: 0,
            })
            .unwrap();
    }

    /// A boot that starts strictly after every write seeded so far.
    fn boot_after_seed() -> std::time::SystemTime {
        std::thread::sleep(Duration::from_millis(20));
        std::time::SystemTime::now()
    }

    /// Both halves of the pass, as `main` runs them, for a boot at `process_started`.
    async fn sweep(&self, process_started: std::time::SystemTime) -> CrashOrphanSweep {
        let marked = self.mark(process_started);
        self.registry.seal_marked_crash_orphans(marked).await
    }

    fn mark(&self, process_started: std::time::SystemTime) -> CrashOrphanSweep {
        let candidates = self
            .index
            .active_runs()
            .unwrap()
            .into_iter()
            .map(|(run_id, key)| (RunId(run_id), key))
            .collect();
        self.registry
            .mark_crash_orphans(&self.wal_store(), candidates, process_started)
    }

    fn records(&self) -> Vec<WalRecord> {
        self.wal_store()
            .read_records(&self.key)
            .unwrap()
            .into_iter()
            .map(|f| f.record)
            .collect()
    }

    fn terminals(&self) -> Vec<RunFinishReason> {
        self.records()
            .into_iter()
            .filter_map(|record| match record {
                WalRecord::RunFinished { reason, .. } => Some(reason),
                _ => None,
            })
            .collect()
    }

    fn index_state(&self) -> RunIndexState {
        self.index.lookup_run(ORPHAN).unwrap().unwrap().state
    }

    async fn stop(&self) {
        assert!(self.registry.stop_and_seal().await);
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn run_started(key: &BindIdentity) -> WalRecord {
    WalRecord::RunStarted {
        written_at_ms: None,
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
        send_id: None,
        request_identity_digest: None,
        project_id: None,
        principal: None,
        ts_ms: None,
        run_id: RunId(ORPHAN.into()),
        session: key.clone(),
        config: Box::new(min_config()),
        input: super::tests::text_req(key, ORPHAN).input.into(),
        origin: broca_wal::RunOrigin::Fresh,
        submission_id: None,
    }
}

/// A completed model step asking for one tool call: the step is durable (and billed), the run
/// has not ended. Replay resumes such a run by dispatching the call.
fn model_step_with_tool_call() -> WalRecord {
    WalRecord::ModelStepFinished {
        fallback: Default::default(),
        written_at_ms: None,
        tags: None,
        hooks: None,
        step_id: broca_protocol::StepId(1),
        assistant_message: AssistantMessage {
            message_id: MessageId("orphan-message".into()),
            content: vec![ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId(CALL.into()),
                tool_name: "edit".into(),
                input: serde_json::json!({}),
            })],
        }
        .into(),
        usage: broca_protocol::Usage::default(),
        finish_reason: Some(broca_protocol::FinishReason::ToolCalls),
        retries_used: 3,
        service_tier_used: None,
        speed_used: None,
    }
}

fn batch_started() -> WalRecord {
    WalRecord::ToolBatchStarted {
        batch_id: BatchId(BATCH.into()),
        step_id: broca_protocol::StepId(1),
        planned_calls: vec![ToolCallId(CALL.into())],
    }
}

fn dispatch_intent() -> WalRecord {
    WalRecord::ToolDispatchIntent {
        dispatched_at_ms: None,
        hooks: None,
        batch_id: BatchId(BATCH.into()),
        tool_call_id: ToolCallId(CALL.into()),
        tool_name: Some("edit".into()),
        args: serde_json::json!({}).into(),
        module_id: None,
        schema_pin: None,
        approval_digest: None,
        call_key: None,
    }
}

#[tokio::test]
async fn crash_orphan_is_sealed_interrupted_once_and_its_index_row_follows() {
    let f = Fixture::new();
    f.seed(vec![run_started(&f.key), model_step_with_tool_call()]);
    let sweep = f.sweep(Fixture::boot_after_seed()).await;
    assert_eq!(sweep.marked, 1, "{sweep:?}");
    assert_eq!(sweep.sealed, 1, "{sweep:?}");
    assert_eq!(
        f.terminals(),
        vec![RunFinishReason::Interrupted],
        "a run a crash left mid-run gets exactly one terminal, Interrupted, never Cancelled \
         (Cancelled is reserved for a caller's request)"
    );
    assert!(
        matches!(
            f.records().last(),
            Some(WalRecord::RunFinished {
                ts_ms: Some(_),
                retries_used: 3,
                ..
            })
        ),
        "the seal's terminal is the shutdown seal's: stamped, and carrying the run's retries"
    );
    assert_eq!(f.index_state(), RunIndexState::Interrupted);
    assert!(
        f.executor.executed.lock().unwrap().is_empty(),
        "the orphan must be sealed, not resumed"
    );
    f.stop().await;
}

#[tokio::test]
async fn crash_orphan_seal_carries_previous_process_cause_without_becoming_error() {
    let f = Fixture::new();
    f.seed(vec![run_started(&f.key), model_step_with_tool_call()]);
    assert_eq!(f.sweep(Fixture::boot_after_seed()).await.sealed, 1);
    let records = f.records();
    let Some(WalRecord::RunFinished {
        reason,
        interrupt_reason,
        error,
        ..
    }) = records.last()
    else {
        panic!("the crash orphan has no durable terminal");
    };
    assert_eq!(interrupt_reason.as_deref(), Some("runner_crash"));
    assert_eq!(*reason, RunFinishReason::Interrupted);
    let error = error.as_ref().expect("crash orphan must report its cause");
    assert_eq!(error.class, broca_protocol::ErrorClass::Transient);
    assert_eq!(
        error.provider_code.as_deref(),
        Some("previous_process_ended_unsealed")
    );
    assert_eq!(error.message, "the previous broca process ended without sealing this run: host crash, kill, or power loss");
    assert_eq!(f.index_state(), RunIndexState::Interrupted);
    f.stop().await;
}

#[tokio::test]
async fn crash_orphan_tool_intent_without_result_is_indeterminate() {
    let f = Fixture::new();
    f.seed(vec![
        run_started(&f.key),
        model_step_with_tool_call(),
        batch_started(),
        dispatch_intent(),
    ]);
    let sweep = f.sweep(Fixture::boot_after_seed()).await;
    assert_eq!(sweep.sealed, 1, "{sweep:?}");
    let status = f
        .registry
        .route_run_state(&f.key, RunId(ORPHAN.into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.state, RunIndexState::Interrupted);
    assert_eq!(
        status.indeterminate_tool_calls,
        Some(vec![ToolCallId(CALL.into())]),
        "a call that left the process with no result may have applied; the caller must hear so"
    );
    f.stop().await;
}

/// The crash-orphan pass resumes the episode the way a run-loop resume does: its resume record
/// first, then an `outcome_unknown` result that pairs the call left without one, with the batch
/// committed, all synced before the Interrupted terminal. The next send therefore renders the
/// call paired with that result instead of a call with nothing after it.
#[tokio::test]
async fn crash_orphan_seal_pairs_an_indeterminate_call_with_outcome_unknown_before_its_terminal() {
    let f = Fixture::new();
    let seeded = vec![
        run_started(&f.key),
        model_step_with_tool_call(),
        batch_started(),
        dispatch_intent(),
    ];
    let seeded_len = seeded.len();
    f.seed(seeded);
    let sweep = f.sweep(Fixture::boot_after_seed()).await;
    assert_eq!(sweep.sealed, 1, "{sweep:?}");
    let records = f.records();
    let appended = &records[seeded_len..];
    assert!(
        matches!(
            appended,
            [
                WalRecord::ResumeStarted {
                    resumed_from_seq: 4,
                    indeterminate,
                    ..
                },
                WalRecord::ToolResult {
                    result,
                    outcome: broca_wal::DispatchOutcome::Unknown,
                    ..
                },
                WalRecord::ToolBatchFinished { .. },
                WalRecord::TurnFinished { .. },
                WalRecord::RunFinished {
                    reason: RunFinishReason::Interrupted,
                    ..
                },
            ] if indeterminate == &vec![ToolCallId(CALL.into())]
                && **result == broca_core::outcome_unknown_result(&ToolCallId(CALL.into()))
        ),
        "{appended:#?}"
    );
    let state = broca_core::replay(&records).unwrap();
    assert!(
        state
            .prompt
            .iter()
            .any(|message| message.content.iter().any(
                |block| matches!(block, ContentBlock::ToolResult(r) if r.tool_call_id.0 == CALL)
            )),
        "the sealed conversation pairs the call with its result"
    );
    f.stop().await;
}

/// A crash after the model step that asked for a tool, before that call's dispatch intent: the
/// call never left the process. The seal keeps the written step and pairs the call with a
/// never-executed result, committed in the same barrier as the Interrupted terminal, so the
/// transcript only grows and the lineage stays the same.
#[tokio::test]
async fn crash_orphan_seal_keeps_a_never_sent_call_with_a_never_executed_result() {
    let barriers = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
    let observed = barriers.clone();
    let f = Fixture::configured(
        |index| index,
        Some(Arc::new(move |_, seq, _| {
            observed.lock().unwrap().push(seq)
        })),
    );
    let seeded = vec![run_started(&f.key), model_step_with_tool_call()];
    let seeded_len = seeded.len();
    f.seed(seeded);
    let wal_path = f.wal_store().wal_path(&f.key);
    let bytes_before = std::fs::read(&wal_path).unwrap();
    // What a reader saw before the restart; the read goes around the actor, so it neither
    // opens the session nor seals the run.
    let read_before = f
        .registry
        .route_read_page(&f.key, None, None, false)
        .await
        .unwrap();
    let messages_before: Vec<Vec<u8>> = read_before
        .page
        .messages
        .iter()
        .map(|message| serde_json::to_vec(&message.message).unwrap())
        .collect();
    assert!(
        !read_before.page.messages.is_empty(),
        "premise: the cut run's input is readable before the restart"
    );

    let sweep = f.sweep(Fixture::boot_after_seed()).await;
    assert_eq!(sweep.sealed, 1, "{sweep:?}");
    assert_eq!(f.terminals(), vec![RunFinishReason::Interrupted]);
    assert!(
        f.executor.executed.lock().unwrap().is_empty(),
        "the orphan must be sealed, not resumed"
    );

    let call = ToolCallId(CALL.into());
    let records = f.records();
    let appended = &records[seeded_len..];
    assert!(
        !records
            .iter()
            .any(|record| matches!(record, WalRecord::ToolDispatchIntent { .. })),
        "the never-sent call is never dispatched: {appended:#?}"
    );
    let [WalRecord::ResumeStarted { indeterminate, .. }, WalRecord::ToolBatchStarted {
        batch_id: opened,
        planned_calls,
        ..
    }, WalRecord::ToolResult {
        batch_id,
        result,
        outcome: broca_wal::DispatchOutcome::Interrupted,
        ..
    }, WalRecord::ToolBatchFinished { batch_id: closed }, WalRecord::TurnFinished { .. }, WalRecord::RunFinished {
        reason: RunFinishReason::Interrupted,
        ..
    }] = appended
    else {
        panic!("the seal must keep the call with a never-executed result: {appended:#?}");
    };
    assert!(
        indeterminate.is_empty(),
        "a call never sent is not indeterminate"
    );
    assert_eq!(planned_calls, &vec![call.clone()]);
    assert_eq!(batch_id, opened);
    assert_eq!(closed, opened);
    assert_eq!(**result, broca_core::not_dispatched_result(&call));

    // One commit: no durability barrier fell between the result and the terminal.
    let frames = f.wal_store().read_records(&f.key).unwrap();
    let first_kept = frames[seeded_len + 1].seq;
    let terminal = frames.last().unwrap().seq;
    let barriers = barriers.lock().unwrap().clone();
    assert!(
        barriers.contains(&terminal),
        "premise: the terminal's barrier was observed: {barriers:?}"
    );
    assert!(
        !barriers
            .iter()
            .any(|head| (first_kept..terminal).contains(head)),
        "the kept turn and the terminal must be durable together, but a barrier fell at \
         {barriers:?} between seq {first_kept} and the terminal at {terminal}"
    );

    // The transcript only grows, under the same lineage.
    let bytes_after = std::fs::read(&wal_path).unwrap();
    assert!(
        bytes_after.starts_with(&bytes_before),
        "the seal rewrote durable WAL bytes"
    );
    let read_after = f
        .registry
        .route_read_page(&f.key, None, None, false)
        .await
        .unwrap();
    assert_eq!(read_after.page.lineage_id, read_before.page.lineage_id);
    let kept = read_before.page.messages.len();
    assert!(
        read_after.page.messages.len() > kept,
        "the kept turn is readable"
    );
    assert_eq!(
        read_after.page.messages[..kept],
        read_before.page.messages[..],
        "messages read before the restart read back unchanged"
    );
    let messages_after: Vec<Vec<u8>> = read_after.page.messages[..kept]
        .iter()
        .map(|message| serde_json::to_vec(&message.message).unwrap())
        .collect();
    assert_eq!(
        messages_after, messages_before,
        "messages read before the restart serialize byte-identically"
    );
    let dispatches: Vec<_> = read_after.dispatches.values().flatten().collect();
    assert!(
        matches!(dispatches.as_slice(), [entry] if entry.tool_call_id == CALL
            && !entry.indeterminate
            && entry.intent_seq.is_none()
            && entry.dispatched_to.is_none()),
        "session.read reports the call as never dispatched and not indeterminate: {dispatches:?}"
    );
    let status = f
        .registry
        .route_run_state(&f.key, RunId(ORPHAN.into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.state, RunIndexState::Interrupted);
    assert!(
        status
            .indeterminate_tool_calls
            .as_ref()
            .is_none_or(|calls| calls.is_empty()),
        "{:?}",
        status.indeterminate_tool_calls
    );

    // The next send renders the call followed directly by its result.
    let prompt = broca_core::replay(&records).unwrap().prompt;
    let call_at = prompt
        .iter()
        .position(|message| {
            message
                .content
                .iter()
                .any(|block| matches!(block, ContentBlock::ToolCall(c) if c.tool_call_id == call))
        })
        .expect("the kept step is in the history the next send renders");
    assert!(
        matches!(
            prompt.get(call_at + 1),
            Some(message) if message.role == broca_protocol::content::Role::Tool
                && matches!(message.content.as_slice(),
                    [ContentBlock::ToolResult(r)] if *r == broca_core::not_dispatched_result(&call))
        ),
        "{prompt:#?}"
    );
    f.stop().await;
}

/// The send after such a seal reaches the model, and its request carries the kept step as
/// written: the call, its never-executed result right after it, then the new user turn. The
/// model's answer is written as the next assistant turn.
#[tokio::test]
async fn next_send_after_a_kept_never_sent_call_renders_call_result_and_new_turn() {
    use broca_protocol::content::Role;
    use broca_protocol::provider::{NormalizedEvent, ProviderControl};
    let provider = broca_core::StubProvider::new(vec![vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId("after-the-seal".into()),
                content: vec![ContentBlock::Text {
                    text: "after the seal".into(),
                }],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: broca_protocol::Usage::default(),
            finish_reason: broca_protocol::FinishReason::Stop,
            retries_used: 0,
        }),
    ]]);
    let f = Fixture::with_core_loop(provider.clone());
    f.seed(vec![run_started(&f.key), model_step_with_tool_call()]);
    assert_eq!(f.sweep(Fixture::boot_after_seed()).await.sealed, 1);

    let outcome = f
        .registry
        .route_send(
            &f.key,
            super::tests::text_req(&f.key, "next-run"),
            RunKind::FreshOrResume,
        )
        .await
        .unwrap();
    assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
    let terminals = broca_test_support::wait_until("the next send's run to end", || async {
        let terminals = f.terminals();
        if terminals.len() == 2 {
            Ok(terminals)
        } else {
            Err(format!("terminals so far: {terminals:?}"))
        }
    })
    .await;
    assert_eq!(
        terminals,
        vec![RunFinishReason::Interrupted, RunFinishReason::Completed],
        "the next send completes instead of stalling or failing"
    );

    let calls = provider.calls();
    let [request] = calls.as_slice() else {
        panic!("the next send makes exactly one model request: {calls:#?}");
    };
    let call = ToolCallId(CALL.into());
    let tail: Vec<(Role, &[ContentBlock])> = request
        .prompt
        .iter()
        .rev()
        .take(3)
        .rev()
        .map(|message| (message.role, message.content.as_slice()))
        .collect();
    assert!(
        matches!(
            tail.as_slice(),
            [
                (Role::Assistant, [ContentBlock::ToolCall(c)]),
                (Role::Tool, [ContentBlock::ToolResult(r)]),
                (Role::User, [ContentBlock::Text { text }]),
            ] if c.tool_call_id == call
                && *r == broca_core::not_dispatched_result(&call)
                && text == "hi"
        ),
        "{:#?}",
        request.prompt
    );
    let prompt = broca_core::replay(&f.records()).unwrap().prompt;
    assert!(
        matches!(prompt.last(), Some(message) if message.role == Role::Assistant
            && matches!(message.content.as_slice(), [ContentBlock::Text { text }] if text == "after the seal")),
        "{prompt:#?}"
    );
    f.stop().await;
}

#[tokio::test]
async fn crash_orphan_pass_leaves_a_paused_run_alone() {
    let f = Fixture::new();
    // The index row still says active: the pause's own index write was lost with the process.
    // The WAL is the authority, and it says the run is waiting for re-auth.
    f.seed(vec![
        run_started(&f.key),
        model_step_with_tool_call(),
        WalRecord::RunPaused {
            run_id: RunId(ORPHAN.into()),
            ts_ms: None,
            reason: broca_protocol::PauseReason::AuthRequired,
            error: None,
        },
    ]);
    let before = f.records();
    let sweep = f.sweep(Fixture::boot_after_seed()).await;
    assert_eq!(
        sweep.marked, 1,
        "premise: the pass considered it: {sweep:?}"
    );
    assert_eq!(sweep.sealed, 0, "{sweep:?}");
    assert_eq!(
        f.records(),
        before,
        "a paused run is not an orphan; its caller resumes it"
    );
    f.stop().await;
}

#[tokio::test]
async fn crash_orphan_restart_policy_does_not_resume_a_torn_pause() {
    use std::io::Write;

    let f = Fixture::new();
    let mut started = run_started(&f.key);
    if let WalRecord::RunStarted { on_restart, .. } = &mut started {
        *on_restart = Some(broca_protocol::request::OnRestart::Resume);
    }
    f.seed(vec![started]);
    let store = f.wal_store();
    let seq = store.read_records(&f.key).unwrap().last().unwrap().seq + 1;
    let pause = broca_wal::framing::encode_with_requires(
        seq,
        1,
        &[],
        &WalRecord::RunPaused {
            ts_ms: None,
            run_id: RunId(ORPHAN.into()),
            reason: broca_protocol::PauseReason::Restart,
            error: None,
        },
    )
    .unwrap();
    // A killed append leaves only part of the frame, never a valid restart authorization.
    std::fs::OpenOptions::new()
        .append(true)
        .open(store.wal_path(&f.key))
        .unwrap()
        .write_all(&pause[..pause.len() / 2])
        .unwrap();
    let sweep = f.sweep(Fixture::boot_after_seed()).await;
    assert_eq!(sweep.sealed, 1);
    assert_eq!(f.terminals(), vec![RunFinishReason::Interrupted]);
    assert_eq!(f.index_state(), RunIndexState::Interrupted);
    assert!(f.executor.executed.lock().unwrap().is_empty());
    assert!(!f
        .records()
        .iter()
        .any(|record| matches!(record, WalRecord::RunPaused { .. })));
    assert_eq!(
        f.registry
            .resume_restart_pauses(Arc::new(store), vec![(ORPHAN.into(), f.key.clone())],)
            .await,
        (0, 0)
    );
    f.stop().await;
}

#[tokio::test]
async fn crash_orphan_pass_leaves_an_already_sealed_run_alone() {
    let f = Fixture::new();
    // The WAL terminal is durable; only its index write was lost.
    f.seed(vec![
        run_started(&f.key),
        model_step_with_tool_call(),
        WalRecord::RunFinished {
            interrupt_reason: None,
            reason: RunFinishReason::Completed,
            ts_ms: None,
            usage: broca_protocol::Usage::default(),
            error: None,
            retries_used: 3,
        },
    ]);
    let sweep = f.sweep(Fixture::boot_after_seed()).await;
    assert_eq!(sweep.sealed, 0, "{sweep:?}");
    assert_eq!(f.terminals(), vec![RunFinishReason::Completed]);
    // The lost index write is made good from the WAL's own terminal, not from a seal.
    assert_eq!(sweep.index_repaired, 1, "{sweep:?}");
    assert_eq!(f.index_state(), RunIndexState::Completed);
    f.stop().await;
}

#[tokio::test]
async fn a_seal_whose_index_write_fails_still_counts_and_clears_its_mark() {
    // The actor is spawned by the registry, so identify its warning by run and error
    // rather than assuming that the actor inherits the test task's tracing span.
    let logs = broca_test_support::LogCapture::observe(|event| {
        event.level == tracing::Level::WARN
            && event.target == "lifecycle"
            && event.fields.values().any(|value| value.contains(ORPHAN))
            && event
                .fields
                .values()
                .any(|value| value.contains("injected run-index write failure"))
    });
    let flaky = Arc::new(std::sync::OnceLock::<Arc<FlakyIndex>>::new());
    let f = Fixture::with_opener_index(|index| {
        let wrapped = Arc::new(FlakyIndex {
            inner: index,
            fail_terminal: std::sync::atomic::AtomicBool::new(true),
        });
        let _ = flaky.set(wrapped.clone());
        wrapped
    });
    f.seed(vec![run_started(&f.key), model_step_with_tool_call()]);
    let sweep = f.sweep(Fixture::boot_after_seed()).await;
    assert_eq!(
        sweep.marked, 1,
        "premise: the pass considered it: {sweep:?}"
    );
    assert_eq!(
        sweep.sealed, 1,
        "the WAL terminal is durable, so the seal happened even though the index write \
         after it failed: {sweep:?}"
    );
    assert_eq!(sweep.open_failed, 0, "{sweep:?}");
    assert_eq!(f.terminals(), vec![RunFinishReason::Interrupted]);
    assert_eq!(
        f.index_state(),
        RunIndexState::Active,
        "premise: the injected failure kept the row from moving"
    );
    assert!(
        f.executor.executed.lock().unwrap().is_empty(),
        "the orphan must be sealed, not resumed"
    );
    // The session opened and took its mark; a later status read finds it idle, not marked.
    assert_eq!(
        f.registry.route_status(&f.key).await.unwrap(),
        LineageStatus::Idle
    );
    let warned = logs.events();
    assert!(
        warned
            .iter()
            .any(|event| event.level == tracing::Level::WARN
                && event.target == "lifecycle"
                && event.fields.values().any(|value| value.contains(ORPHAN))
                && event
                    .fields
                    .values()
                    .any(|value| value.contains("injected run-index write failure"))),
        "the failed index write is logged at WARN, naming the run and the error: {warned:?}"
    );
    flaky
        .get()
        .unwrap()
        .fail_terminal
        .store(false, Ordering::Release);
    f.stop().await;
}

#[tokio::test]
async fn a_wal_terminal_whose_index_write_was_lost_is_repaired_not_resealed() {
    let f = Fixture::new();
    // A crash between the seal's WAL sync and its index write: the terminal is durable, the
    // row still reads active.
    let usage = broca_protocol::Usage {
        input_tokens: Some(120),
        output_tokens: Some(7),
        ..broca_protocol::Usage::default()
    };
    f.seed(vec![
        run_started(&f.key),
        model_step_with_tool_call(),
        WalRecord::RunFinished {
            interrupt_reason: None,
            reason: RunFinishReason::Interrupted,
            ts_ms: Some(1),
            usage,
            error: None,
            retries_used: 3,
        },
    ]);
    let sweep = f.sweep(Fixture::boot_after_seed()).await;
    assert_eq!(
        sweep.marked, 1,
        "premise: the pass considered it: {sweep:?}"
    );
    assert_eq!(sweep.index_repaired, 1, "{sweep:?}");
    assert_eq!(sweep.sealed, 0, "a repair is not a seal: {sweep:?}");
    assert_eq!(
        f.terminals(),
        vec![RunFinishReason::Interrupted],
        "the WAL already ends the run; no second terminal is appended"
    );
    let row = f.index.lookup_run(ORPHAN).unwrap().unwrap();
    assert_eq!(row.state, RunIndexState::Interrupted);
    assert_eq!(row.usage, Some(usage), "usage comes from the WAL terminal");
    assert_eq!(row.retries_used, 3);
    assert_eq!(
        row.final_step_finish_reason,
        Some(broca_protocol::FinishReason::ToolCalls),
        "the last step's provider finish reason, as the loop's own terminal write records it"
    );
    assert!(f.executor.executed.lock().unwrap().is_empty());
    f.stop().await;
}

#[tokio::test]
async fn crash_orphan_pass_skips_a_wal_written_since_boot() {
    let f = Fixture::new();
    let process_started = std::time::SystemTime::now();
    std::thread::sleep(Duration::from_millis(20));
    f.seed(vec![run_started(&f.key), model_step_with_tool_call()]);
    let sweep = f.sweep(process_started).await;
    assert_eq!(sweep.written_since_boot, 1, "{sweep:?}");
    assert_eq!(sweep.marked, 0, "{sweep:?}");
    assert!(f.terminals().is_empty());
    assert_eq!(f.index_state(), RunIndexState::Active);
}

#[tokio::test]
async fn crash_orphan_pass_skips_a_session_another_process_holds() {
    let f = Fixture::new();
    f.seed(vec![run_started(&f.key), model_step_with_tool_call()]);
    let process_started = Fixture::boot_after_seed();
    // A second live process owns this session: its lease is held for the whole pass.
    let other_process = FileLeaseStore::new(f.root.join("leases"))
        .acquire(&broca_core::lease_key_for(&f.key))
        .unwrap();
    let sweep = f.sweep(process_started).await;
    assert_eq!(sweep.lease_held, 1, "{sweep:?}");
    assert_eq!(sweep.sealed, 0, "{sweep:?}");
    assert!(
        f.terminals().is_empty(),
        "the holder's run is its own business; the pass must not write into its WAL"
    );
    assert_eq!(f.index_state(), RunIndexState::Active);
    drop(other_process);
    // The mark is dropped with the skip, so a later open does not act on a stale decision.
    f.registry.actor_for(&f.key).await.unwrap();
    // Opening can acknowledge admission before the actor handles the recovered run's terminal.
    broca_test_support::wait_until(
        "recovered run's durable terminal and completion",
        || async {
            let status = f
                .registry
                .route_status(&f.key)
                .await
                .map_err(|error| format!("recovered run status unavailable: {error:?}"))?;
            if f.terminals().is_empty() {
                return Err(format!(
                    "no durable RunFinished for {ORPHAN}; status: {status:?}"
                ));
            }
            if status != LineageStatus::Idle {
                return Err(format!(
                    "durable RunFinished for {ORPHAN}; still {status:?}"
                ));
            }
            Ok(())
        },
    )
    .await;
    assert_eq!(
        f.registry.route_status(&f.key).await.unwrap(),
        LineageStatus::Idle
    );
    f.stop().await;
}

#[tokio::test]
async fn a_send_that_reaches_a_marked_session_first_cannot_resume_the_orphan() {
    let f = Fixture::new();
    f.seed(vec![run_started(&f.key), model_step_with_tool_call()]);
    let marked = f.mark(Fixture::boot_after_seed());
    assert_eq!(marked.marked, 1, "{marked:?}");
    // Serving has begun and a caller's send wins the race to the marked session. Its open
    // takes the mark, so the orphan is sealed inside the actor before the send is handled.
    let request = super::tests::text_req(&f.key, "caller-run");
    let outcome = f
        .registry
        .route_send(&f.key, request, RunKind::FreshOrResume)
        .await
        .unwrap();
    assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
    let sweep = f.registry.seal_marked_crash_orphans(marked).await;
    assert_eq!(sweep.sealed, 1, "{sweep:?}");
    assert_eq!(
        f.executor.executed.lock().unwrap().as_slice(),
        ["caller-run".to_string()],
        "only the caller's run executes; the orphan was sealed, never re-driven"
    );
    let order: Vec<String> = f
        .records()
        .into_iter()
        .filter_map(|record| match record {
            WalRecord::RunStarted { run_id, .. } => Some(format!("start {}", run_id.0)),
            WalRecord::RunFinished { reason, .. } => Some(format!("finish {reason:?}")),
            _ => None,
        })
        .collect();
    assert_eq!(
        order,
        [
            format!("start {ORPHAN}"),
            "finish Interrupted".to_string(),
            "start caller-run".to_string(),
            "finish Completed".to_string(),
        ],
        "the seal and the send serialize through the session's actor"
    );
    f.stop().await;
}

#[tokio::test]
async fn keyed_resend_of_crash_orphan_is_finished_without_wal_writes() {
    let f = Fixture::new();
    f.seed(vec![run_started(&f.key), model_step_with_tool_call()]);
    let request = super::tests::text_req(&f.key, ORPHAN);
    let mut row = f.index.lookup_run(ORPHAN).unwrap().unwrap();
    row.request_identity_digest = Some(broca_core::submission_identity_digest(
        &request.to_submission(),
    ));
    f.index.index_run_active(&row).unwrap();
    let sweep = f.sweep(Fixture::boot_after_seed()).await;
    assert_eq!(sweep.sealed, 1);
    let before = f.wal_store().read_records(&f.key).unwrap();
    let outcome = f
        .registry
        .route_send(&f.key, request, RunKind::FreshOrResume)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        SendOutcome::Finished {
            run_id: RunId(ORPHAN.into()),
            reason: RunFinishReason::Interrupted,
            delivered: None,
        }
    );
    assert_eq!(f.wal_store().read_records(&f.key).unwrap(), before);
    assert!(f.executor.executed.lock().unwrap().is_empty());
    f.stop().await;
}

#[tokio::test]
async fn keyed_resend_reconciles_stale_index_to_finished() {
    let f = Fixture::new();
    f.seed(vec![
        run_started(&f.key),
        WalRecord::RunFinished {
            interrupt_reason: None,
            reason: RunFinishReason::Completed,
            ts_ms: None,
            usage: broca_protocol::Usage::default(),
            error: None,
            retries_used: 0,
        },
    ]);
    let request = super::tests::text_req(&f.key, ORPHAN);
    let mut row = f.index.lookup_run(ORPHAN).unwrap().unwrap();
    row.request_identity_digest = Some(broca_core::submission_identity_digest(
        &request.to_submission(),
    ));
    f.index.index_run_active(&row).unwrap();
    assert_eq!(f.index_state(), RunIndexState::Active);
    let before = f.wal_store().read_records(&f.key).unwrap();
    let outcome = f
        .registry
        .route_send(&f.key, request, RunKind::FreshOrResume)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        SendOutcome::Finished {
            run_id: RunId(ORPHAN.into()),
            reason: RunFinishReason::Completed,
            delivered: None,
        }
    );
    assert_eq!(f.wal_store().read_records(&f.key).unwrap(), before);
    assert!(f.executor.executed.lock().unwrap().is_empty());
    f.stop().await;
}
