use super::tests::{min_config, text_req};
use super::*;
use crate::opener::DefaultOpener;
use crate::registry::LineageRegistry;
use broca_core::{FileLeaseStore, FileWalStore};
use broca_protocol::content::{AssistantMessage, ContentBlock, ToolCall, ToolOutput, ToolResult};
use broca_protocol::{BatchId, BindIdentity, MessageId, RunFinishReason, ToolCallId};

struct DispatchHeld {
    resolved: bool,
    finish: Option<Arc<tokio::sync::Notify>>,
    /// Plan a second call in the same batch that never gets a dispatch intent: it had not left
    /// the process when the run was stopped.
    unsent_sibling: bool,
    poll_gate: Option<Arc<HoldGate>>,
}

#[async_trait::async_trait]
impl RunExecutor for DispatchHeld {
    async fn execute(
        &self,
        job: RunJob,
        _out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, broca_core::RunError> {
        let call = ToolCallId("shutdown-call".into());
        let batch = BatchId("shutdown-batch".into());
        job.writer.record(WalRecord::RunStarted {
            written_at_ms: None,
            tags: None,
            session_class: None,
            retention: None,
            hooks: None,
            title: None,
            on_restart: job.req.on_restart,
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
        job.writer.record(WalRecord::ModelStepFinished {
            written_at_ms: None,
            tags: None,
            hooks: None,
            step_id: broca_protocol::StepId(1),
            assistant_message: AssistantMessage {
                message_id: MessageId("shutdown-message".into()),
                content: vec![ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: call.clone(),
                    tool_name: "edit".into(),
                    input: serde_json::json!({}),
                })],
            }
            .into(),
            usage: broca_protocol::Usage::default(),
            finish_reason: Some(broca_protocol::FinishReason::ToolCalls),
            retries_used: 2,
            service_tier_used: None,
            speed_used: None,
        });
        let mut planned_calls = vec![call.clone()];
        if self.unsent_sibling {
            planned_calls.push(ToolCallId("shutdown-sibling".into()));
        }
        job.writer.record(WalRecord::ToolBatchStarted {
            batch_id: batch.clone(),
            step_id: broca_protocol::StepId(1),
            planned_calls,
        });
        job.writer.record(WalRecord::ToolDispatchIntent {
            dispatched_at_ms: None,
            hooks: None,
            batch_id: batch.clone(),
            tool_call_id: call.clone(),
            tool_name: Some("edit".into()),
            args: serde_json::json!({}).into(),
            module_id: None,
            schema_pin: None,
            approval_digest: None,
            call_key: None,
        });
        if self.resolved {
            job.writer.record(WalRecord::ToolResult {
                finished_at_ms: None,
                tags: None,
                hooks: None,
                batch_id: batch,
                result: ToolResult {
                    tool_call_id: call,
                    output: ToolOutput::Text {
                        text: "done".into(),
                    },
                    is_error: false,
                }
                .into(),
                outcome: broca_wal::DispatchOutcome::Completed,
                reason: None,
                withdraw_elicitation: None,
                provider_held: None,
            });
        }
        job.writer
            .sync()
            .await
            .map_err(|e| broca_core::RunError::Wal(e.to_string()))?;
        job.on_admitted.unwrap().send(Ok(job.req.run_id)).unwrap();
        if let Some(gate) = &self.poll_gate {
            // Let the actor finish admission before parking a runtime worker. Otherwise the
            // actor's wakeup can remain in that worker's private scheduling slot.
            gate.poll_requested.notified().await;
            gate.wait();
        }
        if let Some(finish) = &self.finish {
            finish.notified().await;
            job.writer.record(WalRecord::RunFinished {
                interrupt_reason: None,
                reason: RunFinishReason::Completed,
                ts_ms: None,
                usage: broca_protocol::Usage::default(),
                error: None,
                retries_used: 2,
            });
            job.writer
                .sync()
                .await
                .map_err(|e| broca_core::RunError::Wal(e.to_string()))?;
            return Ok(RunSummary {
                reason: RunFinishReason::Completed,
                paused: None,
                steps: 1,
                usage: broca_protocol::Usage::default(),
                retries_used: 2,
                final_step_finish_reason: Some(broca_protocol::FinishReason::ToolCalls),
            });
        }
        std::future::pending().await
    }
}

struct Fixture {
    registry: LineageRegistry,
    key: BindIdentity,
    root: std::path::PathBuf,
    wal: Arc<GatedReadWalStore>,
    index: Option<Arc<broca_store::SqliteStore>>,
}

impl Fixture {
    fn new(resolved: bool) -> Self {
        Self::with_finish(resolved, None)
    }

    fn with_finish(resolved: bool, finish: Option<Arc<tokio::sync::Notify>>) -> Self {
        Self::build(resolved, finish, false, false, None, true)
    }

    fn for_admission() -> Self {
        // Admission assertions inspect records, not crash durability.
        Self::build(false, None, false, false, None, false)
    }

    fn for_seal_records(with_index: bool) -> Self {
        // These assertions check terminal metadata and index updates, not survival across a crash.
        Self::build(false, None, with_index, false, None, false)
    }

    /// A run stopped inside a batch of two planned calls: the first was dispatched (its intent
    /// is durable, its result is not), the second never left the process.
    fn with_unsent_sibling() -> Self {
        Self::build(false, None, false, true, None, true)
    }

    /// The production shape: a durable run index beside the WAL. The other constructors omit
    /// it because most seal arms are about the WAL, and an index-free registry is a legal
    /// deployment (`broca-run`).
    fn with_run_index(resolved: bool) -> Self {
        Self::build(resolved, None, true, false, None, true)
    }

    fn build(
        resolved: bool,
        finish: Option<Arc<tokio::sync::Notify>>,
        with_index: bool,
        unsent_sibling: bool,
        poll_gate: Option<Arc<HoldGate>>,
        fsync: bool,
    ) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "broca-shutdown-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(root.join("project")).unwrap();
        let key = BindIdentity::new(root.join("project"), "runner", "session");
        let wal = Arc::new(GatedReadWalStore {
            inner: FileWalStore::new(root.join("wal")),
            blocked: std::sync::Mutex::new(false),
            released: std::sync::Condvar::new(),
            entered: tokio::sync::Notify::new(),
            sync_gate: Arc::new(HoldGate::default()),
            replay_gate: Arc::new(HoldGate::default()),
            fsync,
        });
        let mut opener = DefaultOpener::new(
            Arc::new(FileLeaseStore::new(root.join("leases"))),
            wal.clone(),
            Arc::new(DispatchHeld {
                resolved,
                finish,
                unsent_sibling,
                poll_gate,
            }),
        );
        let index = with_index.then(|| {
            Arc::new(if fsync {
                broca_store::SqliteStore::open(root.join("run-index.db")).unwrap()
            } else {
                broca_store::SqliteStore::open_in_memory().unwrap()
            })
        });
        if let Some(index) = &index {
            opener = opener.with_run_index(index.clone());
        }
        Self {
            registry: LineageRegistry::new(Arc::new(opener)),
            key,
            root,
            wal,
            index,
        }
    }

    async fn start(&self) {
        assert!(matches!(
            self.registry
                .route_send(
                    &self.key,
                    text_req(&self.key, "shutdown-run"),
                    RunKind::FreshOrResume
                )
                .await
                .unwrap(),
            SendOutcome::Active { .. }
        ));
        assert_eq!(self.registry.slot_counts().runs_in_flight, 1);
    }

    async fn stop(&self) {
        let clean = broca_test_support::wait_for(
            "shutdown seal completes",
            self.registry
                .stop_and_seal_with_budget(Duration::ZERO, broca_test_support::EVENT_HANG_GUARD),
            || {
                format!(
                    "slots={:?}, report={:?}",
                    self.registry.slot_counts(),
                    self.registry.shutdown_report()
                )
            },
        )
        .await;
        assert!(clean, "{:?}", self.registry.shutdown_report());
        assert_eq!(self.registry.slot_counts().open, 0);
    }

    fn records(&self) -> Vec<WalRecord> {
        // Read from a new file store after the actor's WAL writer is gone, rather
        // than relying on records cached by that writer.
        FileWalStore::new(self.root.join("wal"))
            .read_records(&self.key)
            .unwrap()
            .into_iter()
            .map(|f| f.record)
            .collect()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[tokio::test]
async fn shutdown_seals_inflight_durably() {
    let f = Fixture::new(false);
    f.start().await;
    f.stop().await;
    assert_eq!(
        f.records()
            .iter()
            .filter(|r| matches!(r, WalRecord::RunFinished { .. }))
            .count(),
        1,
        "shutdown must persist a terminal, not merely rely on inferred interruption"
    );
}

#[tokio::test]
async fn shutdown_seal_moves_the_index_row_off_active() {
    // The WAL is the authority and the query path reconciles a stale Active row on read, so
    // leaving the row alone was never wrong on `run.status`. It was wrong for every reader
    // that consumes run_index DIRECTLY: on 2026-09-18 four Active rows stood for runs that
    // were not, one of them the run the seal had just terminated, and a placement gate
    // counting active runs from the index would never have seen zero on that host again.
    // The seal now touches the index after the durable terminal, so the index is at worst
    // behind the WAL, never ahead of it, and never stale forever.
    let f = Fixture::for_seal_records(true);
    f.start().await;
    let _held_physical_sync = f.wal.sync_gate.arm();
    let index = f.index.clone().expect("this fixture carries a run index");
    let before = index
        .lookup_run("shutdown-run")
        .unwrap()
        .expect("admission indexed the run");
    assert_eq!(
        before.state,
        RunIndexState::Active,
        "premise: the row is Active while the run is in flight"
    );
    f.stop().await;
    let after = index
        .lookup_run("shutdown-run")
        .unwrap()
        .expect("the row must survive the seal");
    assert_eq!(
        after.state,
        RunIndexState::Interrupted,
        "the seal must move the index row off Active; a direct reader of run_index would \
         otherwise count this run as live forever (the WAL says Interrupted)"
    );
    assert_eq!(
        after.retries_used, 2,
        "the seal carries the run's metadata to the row"
    );
}

#[tokio::test]
async fn shutdown_seal_terminal_carries_a_timestamp() {
    // The seal's RunFinished feeds the export lane like any other terminal. A None here
    // exports `occurred_at_ms: NULL`, which no period total includes, so the sealed run's
    // spend disappears from the window it occurred in. Found on the first live seal with a
    // session in flight; this arm keeps it found.
    let f = Fixture::for_seal_records(false);
    f.start().await;
    let _held_physical_sync = f.wal.sync_gate.arm();
    f.stop().await;
    let ts_ms = f.records().into_iter().find_map(|r| match r {
        WalRecord::RunFinished { ts_ms, .. } => Some(ts_ms),
        _ => None,
    });
    assert!(
        matches!(ts_ms, Some(Some(_))),
        "the seal must stamp ts_ms so the sealed run stays inside period totals; got {ts_ms:?}"
    );
}

#[tokio::test]
async fn shutdown_reason_is_interrupted_never_cancelled() {
    let f = Fixture::new(false);
    f.start().await;
    f.stop().await;
    let reason = f.records().into_iter().find_map(|r| match r {
        WalRecord::RunFinished { reason, .. } => Some(reason),
        _ => None,
    });
    assert_eq!(reason, Some(RunFinishReason::Interrupted));
}

#[tokio::test]
async fn shutdown_seal_does_not_claim_previous_process_ended_unsealed() {
    let f = Fixture::new(false);
    f.start().await;
    f.stop().await;
    let records = f.records();
    assert!(
        matches!(records.last(), Some(WalRecord::RunFinished { interrupt_reason: Some(reason), .. }) if reason == "runner_shutdown")
    );
    assert!(
        matches!(
            records.last(),
            Some(WalRecord::RunFinished {
                reason: RunFinishReason::Interrupted,
                error: None,
                ..
            })
        ),
        "graceful shutdown must not acquire a crash-orphan cause: {:?}",
        records.last()
    );
}

#[tokio::test]
async fn shutdown_indeterminate_status_distinguishes_missing_and_matching_result() {
    for resolved in [false, true] {
        let f = Fixture::new(resolved);
        f.start().await;
        f.stop().await;
        // The caller's run-status reconciliation is the consumer of the persisted seal and
        // preceding tool records. An intent without a result must report its call id; adding
        // a result for that same call must instead report an empty indeterminate-call set.
        let reopened = LineageRegistry::new(Arc::new(DefaultOpener::new(
            Arc::new(FileLeaseStore::new(f.root.join("leases"))),
            Arc::new(FileWalStore::new(f.root.join("wal"))),
            Arc::new(DispatchHeld {
                resolved,
                finish: None,
                unsent_sibling: false,
                poll_gate: None,
            }),
        )));
        reopened.actor_for(&f.key).await.unwrap();
        let status = reopened
            .route_run_state(&f.key, RunId("shutdown-run".into()))
            .await
            .unwrap()
            .unwrap();
        let expected = if resolved {
            vec![]
        } else {
            vec![ToolCallId("shutdown-call".into())]
        };
        assert_eq!(status.indeterminate_tool_calls, Some(expected));
        assert_eq!(status.retries_used, 2);
        assert!(reopened.stop_and_seal().await);
    }
}

/// A graceful stop inside a tool dispatch closes the open batch before sealing, the way a
/// resume does: an `outcome_unknown` result for the dispatched call, a not-dispatched result
/// for the call that never left the process, the batch and turn commits, then one Interrupted
/// terminal. It writes no resume record, because it stops a run this process was running
/// rather than resuming one. The extra barrier must still fit inside the seal's time bound.
#[tokio::test]
async fn shutdown_seal_closes_the_open_batch_before_its_interrupted_terminal() {
    let f = Fixture::with_unsent_sibling();
    f.start().await;
    let seeded = f.records().len();
    let started = std::time::Instant::now();
    assert!(
        f.registry
            .stop_and_seal_with_budget(Duration::ZERO, SHUTDOWN_SEAL_BOUND)
            .await,
        "the seal must report success, which it does only inside its time bound"
    );
    assert!(
        started.elapsed() < SHUTDOWN_SEAL_BOUND,
        "the close-out's extra barrier pushed the seal past its bound: {:?}",
        started.elapsed()
    );
    assert_eq!(f.registry.slot_counts().open, 0);

    let records = f.records();
    let appended = &records[seeded..];
    let dispatched = ToolCallId("shutdown-call".into());
    let sibling = ToolCallId("shutdown-sibling".into());
    assert!(
        matches!(
            appended,
            [
                WalRecord::ToolResult {
                    result: unknown,
                    outcome: broca_wal::DispatchOutcome::Unknown,
                    ..
                },
                WalRecord::ToolResult {
                    result: not_sent,
                    outcome: broca_wal::DispatchOutcome::Interrupted,
                    ..
                },
                WalRecord::ToolBatchFinished { .. },
                WalRecord::TurnFinished { .. },
                WalRecord::RunFinished {
                    reason: RunFinishReason::Interrupted,
                    ..
                },
            ] if **unknown == broca_core::outcome_unknown_result(&dispatched)
                && not_sent.tool_call_id == sibling
                && not_sent.is_error
                && matches!(&not_sent.output, ToolOutput::Error { message }
                    if message == broca_core::resume::NOT_DISPATCHED_TEXT)
        ),
        "{appended:#?}"
    );
    assert!(!records
        .iter()
        .any(|r| matches!(r, WalRecord::ResumeStarted { .. })));
    let terminals: Vec<RunFinishReason> = records
        .iter()
        .filter_map(|r| match r {
            WalRecord::RunFinished { reason, .. } => Some(*reason),
            _ => None,
        })
        .collect();
    assert_eq!(
        terminals,
        vec![RunFinishReason::Interrupted],
        "one terminal, never Cancelled"
    );
}

#[tokio::test]
async fn shutdown_idle_exits_promptly() {
    let f = Fixture::new(false);
    f.registry.actor_for(&f.key).await.unwrap();
    assert_eq!(f.registry.slot_counts().runs_in_flight, 0);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), f.registry.stop_and_seal())
            .await
            .expect("idle shutdown must skip the ten-second grace")
    );
}

#[tokio::test]
async fn shutdown_admits_queued_sends_before_stopping_and_stops_cleanly() {
    let f = Fixture::for_admission();
    f.start().await;
    let handle = f.registry.actor_for(&f.key).await.unwrap();
    let signal = f.registry.stopping_signal();
    let registry = f.registry.clone();
    // Keep the stop task before its first poll of the registry stop future, so both
    // sends below are admitted before the registry requests stopping.
    let (release, held) = oneshot::channel();
    let stopping = tokio::spawn(async move {
        held.await.unwrap();
        registry
            .stop_and_seal_with_budget(Duration::from_millis(200), Duration::from_secs(2))
            .await
    });
    let (reply, rx) = oneshot::channel();
    handle
        .send(LineageCmd::Send {
            session_class: None,
            retention: None,
            stop_sequences_named: false,
            title: None,
            conversation_key: None,
            run_id: RunId("late-run".into()),
            request: Box::new(text_req(&f.key, "late-run").to_submission()),
            kind: RunKind::FreshOrResume,
            key: crate::actor::SendKey::Minted,
            skip_materialization: false,
            delivery: broca_protocol::Delivery::Queue,
            plan: None,
            order: None,
            reply,
        })
        .await
        .ok()
        .unwrap();
    let result = rx.await.unwrap();
    assert!(!signal.is_cancelled());
    assert!(matches!(result, SendOutcome::Pending { .. }), "{result:?}");
    let route_result = f
        .registry
        .route_send(
            &f.key,
            text_req(&f.key, "late-route"),
            RunKind::FreshOrResume,
        )
        .await;
    assert!(
        matches!(route_result, Ok(SendOutcome::Pending { .. })),
        "{route_result:?}"
    );
    release.send(()).unwrap();
    let clean = stopping.await.unwrap();
    assert!(clean, "{}", f.registry.shutdown_report().unwrap());
}

#[tokio::test]
async fn shutdown_slow_seal_sync_expires_admission_test_budget() {
    let f = Fixture::new(false);
    f.start().await;
    let release = f.wal.sync_gate.arm();
    let registry = f.registry.clone();
    let total = Duration::from_secs(2);
    let started = tokio::time::Instant::now();
    let stopping = tokio::spawn(async move {
        registry
            .stop_and_seal_with_budget(Duration::from_millis(200), total)
            .await
    });
    f.wal.sync_gate.entered.notified().await;
    // Hold the seal's sync beyond the registry budget without relying on disk latency.
    // The actor's own five-second write bound has not expired when the registry returns.
    tokio::time::pause();
    tokio::time::advance(total).await;
    let sealed = stopping.await.unwrap();
    assert_total_deadline(&f, sealed, started.elapsed(), total);
    let report = f.registry.shutdown_report().unwrap();
    assert!(report.current_failures.is_empty(), "{report}");
    assert!(report.recovery_debt.is_empty(), "{report}");
    tokio::time::resume();
    drop(release);
    wait_for_closed(&f).await;
}

#[tokio::test]
async fn shutdown_rejects_new_admission() {
    let f = Fixture::for_admission();
    f.start().await;
    let handle = f.registry.actor_for(&f.key).await.unwrap();
    let signal = f.registry.stopping_signal();
    let registry = f.registry.clone();
    let stopping = tokio::spawn(async move {
        registry
            .stop_and_seal_with_budget(Duration::from_millis(200), Duration::from_secs(2))
            .await
    });
    // A scheduler yield does not prove the spawned task has requested stopping.
    signal.cancelled().await;
    let (reply, rx) = oneshot::channel();
    handle
        .send(LineageCmd::Send {
            session_class: None,
            retention: None,
            stop_sequences_named: false,
            title: None,
            conversation_key: None,
            run_id: RunId("late-run".into()),
            request: Box::new(text_req(&f.key, "late-run").to_submission()),
            kind: RunKind::FreshOrResume,
            key: crate::actor::SendKey::Minted,
            skip_materialization: false,
            delivery: broca_protocol::Delivery::Queue,
            plan: None,
            order: None,
            reply,
        })
        .await
        .ok()
        .unwrap();
    let result = rx.await.unwrap();
    let route_result = f
        .registry
        .route_send(
            &f.key,
            text_req(&f.key, "late-route"),
            RunKind::FreshOrResume,
        )
        .await;
    assert!(
        stopping.await.unwrap(),
        "{}",
        f.registry.shutdown_report().unwrap()
    );
    assert!(
        matches!(result, SendOutcome::Transient(_)),
        "buffered Send admitted while stopping: {result:?}"
    );
    assert!(route_result.is_err(), "registry admitted while stopping");
    assert!(!f
        .records()
        .iter()
        .any(|r| matches!(r, WalRecord::Queued { .. })));
}

struct GatedReadWalStore {
    inner: FileWalStore,
    blocked: std::sync::Mutex<bool>,
    released: std::sync::Condvar,
    entered: tokio::sync::Notify,
    sync_gate: Arc<HoldGate>,
    replay_gate: Arc<HoldGate>,
    fsync: bool,
}

impl WalStore for GatedReadWalStore {
    fn open(
        &self,
        key: &BindIdentity,
    ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
        Ok(Box::new(GatedSyncWal {
            inner: self.inner.open(key)?,
            gate: self.sync_gate.clone(),
            fsync: self.fsync,
        }))
    }

    fn read_records(
        &self,
        key: &BindIdentity,
    ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        self.replay_gate.wait();
        self.inner.read_records(key)
    }

    fn read_records_range(
        &self,
        key: &BindIdentity,
        after: u64,
        through: Option<u64>,
        anchor: Option<&broca_wal::WalByteAnchor>,
    ) -> Result<broca_wal::WalRangeRead, broca_wal::WalError> {
        let mut blocked = self.blocked.lock().unwrap();
        if *blocked {
            self.entered.notify_one();
        }
        while *blocked {
            blocked = self.released.wait(blocked).unwrap();
        }
        drop(blocked);
        self.inner.read_records_range(key, after, through, anchor)
    }
}

struct ReleaseRead(Arc<GatedReadWalStore>);
impl Drop for ReleaseRead {
    fn drop(&mut self) {
        *self.0.blocked.lock().unwrap() = false;
        self.0.released.notify_all();
    }
}

/// Parks a blocking operation without consuming CPU. The release guard also unblocks it
/// during unwinding, so a failed assertion cannot hang the test runtime's worker shutdown.
#[derive(Default)]
struct HoldGate {
    blocked: std::sync::Mutex<bool>,
    released: std::sync::Condvar,
    entered: tokio::sync::Notify,
    poll_requested: tokio::sync::Notify,
}

impl HoldGate {
    fn arm(self: &Arc<Self>) -> ReleaseGate {
        *self.blocked.lock().unwrap() = true;
        ReleaseGate(self.clone())
    }

    fn wait(&self) {
        let mut blocked = self.blocked.lock().unwrap();
        if *blocked {
            self.entered.notify_one();
        }
        while *blocked {
            let (next, timeout) = self
                .released
                .wait_timeout(blocked, Duration::from_secs(30))
                .unwrap();
            blocked = next;
            if timeout.timed_out() {
                drop(blocked);
                panic!("test did not release the held operation within 30s");
            }
        }
    }
}

struct ReleaseGate(Arc<HoldGate>);
impl Drop for ReleaseGate {
    fn drop(&mut self) {
        *self.0.blocked.lock().unwrap() = false;
        self.0.released.notify_all();
    }
}

struct GatedSyncWal {
    inner: Box<dyn broca_wal::Wal + Send>,
    gate: Arc<HoldGate>,
    fsync: bool,
}

impl broca_wal::Wal for GatedSyncWal {
    fn append(
        &mut self,
        fence: u64,
        record: &WalRecord,
        sync: bool,
    ) -> Result<u64, broca_wal::WalError> {
        self.inner.append(fence, record, sync && self.fsync)
    }

    fn sync(&self) -> Result<(), broca_wal::WalError> {
        if self.fsync {
            self.gate.wait();
            self.inner.sync()
        } else {
            Ok(())
        }
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

async fn wait_for_closed(f: &Fixture) {
    broca_test_support::wait_until("released operation lets teardown close slots", || async {
        let open = f.registry.slot_counts().open;
        (open == 0)
            .then_some(())
            .ok_or_else(|| format!("open_slots={open}"))
    })
    .await;
}

fn assert_total_deadline(f: &Fixture, sealed: bool, elapsed: Duration, total: Duration) -> String {
    let diagnostic = f
        .registry
        .shutdown_report()
        .expect("stop records its outcome")
        .to_string();
    assert!(!sealed, "{diagnostic}");
    assert!(elapsed >= total, "{elapsed:?}");
    assert!(
        diagnostic.contains("registry total shutdown deadline expired"),
        "{diagnostic}"
    );
    assert!(
        diagnostic.contains(&format!("budget={total:?}")),
        "{diagnostic}"
    );
    assert!(
        diagnostic.contains(&format!("{:?}", f.key.session)),
        "{diagnostic}"
    );
    assert!(diagnostic.contains("\"open\""), "{diagnostic}");
    assert_eq!(f.registry.slot_counts().open, 1);
    // The slot's survival must also preserve the physical single-writer lease.
    use broca_core::LeaseStore;
    assert!(FileLeaseStore::new(f.root.join("leases"))
        .acquire(&broca_core::lease_key_for(&f.key))
        .is_err());
    println!("{diagnostic}");
    diagnostic
}

#[tokio::test]
async fn shutdown_writer_join_retains_slot_and_lease_past_write_deadline() {
    let f = Fixture::with_run_index(false);
    let mut request = text_req(&f.key, "shutdown-run");
    request.on_restart = Some(broca_protocol::request::OnRestart::Resume);
    assert!(matches!(
        f.registry
            .route_send(&f.key, request, RunKind::FreshOrResume)
            .await
            .unwrap(),
        SendOutcome::Active { .. }
    ));
    let release = f.wal.sync_gate.arm();
    let registry = f.registry.clone();
    let started = tokio::time::Instant::now();
    let stopping = tokio::spawn(async move {
        registry
            .stop_and_seal_with_budget(Duration::ZERO, Duration::from_secs(20))
            .await
    });
    f.wal.sync_gate.entered.notified().await;
    // The writer is now inside the close-out barrier. Clock advancement cannot release it.
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(6)).await;
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(14)).await;
    let sealed = stopping.await.unwrap();
    let diagnostic = assert_total_deadline(&f, sealed, started.elapsed(), Duration::from_secs(20));
    assert!(
        diagnostic.contains("bound=5s; result=Err(Elapsed(()))"),
        "{diagnostic}"
    );
    assert!(!f.records().iter().any(|record| matches!(
        record,
        WalRecord::RunFinished { .. } | WalRecord::RunPaused { .. }
    )));
    tokio::time::resume();
    drop(release);
    wait_for_closed(&f).await;

    // Model process exit only after the held writer has drained and released its lease.
    // The next boot must seal this resume-opted but never-paused run, not auto-resume it.
    let store = Arc::new(FileWalStore::new(f.root.join("wal")));
    let next = LineageRegistry::new(Arc::new(
        DefaultOpener::new(
            Arc::new(FileLeaseStore::new(f.root.join("leases"))),
            store.clone(),
            Arc::new(DispatchHeld {
                resolved: false,
                finish: None,
                unsent_sibling: false,
                poll_gate: None,
            }),
        )
        .with_run_index(f.index.as_ref().unwrap().clone()),
    ));
    let sweep = next.mark_crash_orphans(
        store.as_ref(),
        vec![(RunId("shutdown-run".into()), f.key.clone())],
        std::time::SystemTime::now(),
    );
    assert_eq!(sweep.marked, 1);
    let sweep = next.seal_marked_crash_orphans(sweep).await;
    assert_eq!(sweep.sealed, 1);
    assert!(matches!(
        f.records().last(),
        Some(WalRecord::RunFinished {
            reason: RunFinishReason::Interrupted,
            ..
        })
    ));
    assert_eq!(
        next.resume_restart_pauses(store, vec![("shutdown-run".into(), f.key.clone())])
            .await,
        (0, 0)
    );
    assert!(next.stop_and_seal().await);
}

#[tokio::test]
async fn shutdown_subscriber_replay_join_retains_slot_past_write_deadline() {
    let f = Fixture::new(false);
    f.start().await;
    let release = f.wal.replay_gate.arm();
    let (sink, _receiver) = mpsc::channel(8);
    assert!(matches!(
        f.registry
            .route_subscribe(&f.key, crate::fanout::FromCursor::Start, sink)
            .await
            .unwrap(),
        SubscribeOutcome::Attached
    ));
    f.wal.replay_gate.entered.notified().await;
    // Let the write itself complete with real IO before advancing the clock over teardown.
    let registry = f.registry.clone();
    let started = tokio::time::Instant::now();
    let stopping = tokio::spawn(async move {
        registry
            .stop_and_seal_with_budget(Duration::ZERO, Duration::from_secs(20))
            .await
    });
    tokio::time::timeout(Duration::from_secs(10), async {
        while !f
            .records()
            .iter()
            .any(|record| matches!(record, WalRecord::RunFinished { .. }))
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(20)).await;
    let sealed = stopping.await.unwrap();
    let diagnostic = assert_total_deadline(&f, sealed, started.elapsed(), Duration::from_secs(20));
    assert!(diagnostic.contains("current_failures=[]"), "{diagnostic}");
    tokio::time::resume();
    drop(release);
    wait_for_closed(&f).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_aborted_run_join_retains_slot_while_poll_is_blocked() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let gate = Arc::new(HoldGate::default());
        let release = gate.arm();
        let f = Fixture::build(false, None, false, false, Some(gate.clone()), true);
        f.start().await;
        gate.poll_requested.notify_one();
        gate.entered.notified().await;
        // One runtime worker is parked in non-async run code; the other drives the stop.
        // Abort cannot drop a future while another worker is still polling it.
        let started = tokio::time::Instant::now();
        let sealed = f
            .registry
            .stop_and_seal_with_budget(Duration::from_millis(200), Duration::from_secs(1))
            .await;
        let diagnostic =
            assert_total_deadline(&f, sealed, started.elapsed(), Duration::from_secs(1));
        assert!(diagnostic.contains("current_failures=[]"), "{diagnostic}");
        assert!(!f
            .records()
            .iter()
            .any(|record| matches!(record, WalRecord::RunFinished { .. })));
        drop(release);
        wait_for_closed(&f).await;
    })
    .await
    .expect("held run proof must finish within 10s");
}

#[tokio::test]
async fn shutdown_budget_bounds_stalled_replay_and_leaves_unsealed_wal() {
    let f = Fixture::new(false);
    f.start().await;
    *f.wal.blocked.lock().unwrap() = true;
    let release = ReleaseRead(f.wal.clone());
    let registry = f.registry.clone();
    let stopping = tokio::spawn(async move {
        registry
            .stop_and_seal_with_budget(Duration::ZERO, Duration::from_millis(50))
            .await
    });
    broca_test_support::event_timeout(
        "shutdown reaches held real WAL read",
        f.wal.entered.notified(),
        || format!("open_slots={}", f.registry.slot_counts().open),
    )
    .await
    .expect("shutdown must reach the real WAL read");
    assert!(!tokio::time::timeout(Duration::from_secs(1), stopping)
        .await
        .expect("stalled replay cannot hold exit hostage")
        .unwrap());
    assert!(!f
        .records()
        .iter()
        .any(|r| matches!(r, WalRecord::RunFinished { .. })));
    // Production exits here. Release the blocked reader only to let the test runtime clean up.
    drop(release);
    wait_for_closed(&f).await;
}

#[tokio::test]
async fn shutdown_allows_natural_completion_without_rewriting_reason() {
    let finish = Arc::new(tokio::sync::Notify::new());
    let f = Fixture::with_finish(true, Some(finish.clone()));
    f.start().await;
    let signal = f.registry.stopping_signal();
    let registry = f.registry.clone();
    let stopping = tokio::spawn(async move { registry.stop_and_seal().await });
    signal.cancelled().await;
    finish.notify_one();
    assert!(tokio::time::timeout(Duration::from_secs(1), stopping)
        .await
        .unwrap()
        .unwrap());
    let terminals: Vec<_> = f
        .records()
        .into_iter()
        .filter_map(|r| match r {
            WalRecord::RunFinished { reason, .. } => Some(reason),
            _ => None,
        })
        .collect();
    assert_eq!(terminals, vec![RunFinishReason::Completed]);
}
