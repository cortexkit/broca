//! Attach-barrier conformance — the commit-2 centerpiece (the I1/I2 gap-and-dup-free
//! proof, the equivalent of commit-1's panic test).
//!
//! A step-paced executor emits a deterministic record sequence (RunStarted → StepStarted →
//! ModelStepFinished → ToolResult → RunFinished), pausing between records at the test's
//! command. At EVERY pause point a fresh subscriber attaches `from: Start`; the test then
//! lets the run finish and asserts the subscriber received the COMPLETE control stream with
//! NO gap and NO duplicate — proving the attach barrier (register-(R) → sync-(H) → replay
//! seq≤H → drain skip ≤H) is correct at every interleaving of subscribe-vs-run-progress.

use std::sync::Arc;

use async_trait::async_trait;
use broca_core::{FileLeaseStore, FileWalStore, LeaseStore, OutputEvent, RunError, RunSummary};
use broca_core::{RunRequest, WalHandle, WalStore};
use broca_module::actor::{LineageActor, LineageStatus, SubscribeOutcome};
use broca_module::executor::{RunExecutor, RunJob, RunKind};
use broca_module::fanout::{Delivery, Fanout, FromCursor};
use broca_module::registry::{
    ActorHandle, LineageKey, LineageOpener, LineageRegistry, OpenError, RegistryHandle,
};
use broca_module::serve::{ServeHandler, SubscribeFrom};
use broca_protocol::caller::{ControlUnit, RunFinishReason};
use broca_protocol::content::{
    AssistantMessage, ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult,
};
use broca_protocol::{BatchId, BindIdentity, MessageId, RunId, StepId, ToolCallId, Usage};
use broca_wal::{DispatchOutcome, RunConfig, RunOrigin, WalRecord};
use tokio::sync::{mpsc, Notify};

/// The fixed record sequence both executors emit (each projects to ≥1 control unit).
fn record_sequence(run: &RunId, session: &BindIdentity) -> Vec<WalRecord> {
    vec![
        WalRecord::RunStarted {
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
            run_id: run.clone(),
            session: session.clone(),
            config: Box::new(min_config()),
            input: vec![Message {
                cache_prefix_blocks: None,
                origin: None,
                ordinal: None,
                role: Role::User,
                content: vec![ContentBlock::Text { text: "q".into() }],
            }]
            .into(),
            origin: RunOrigin::Fresh,
            submission_id: None,
        },
        WalRecord::StepStarted { step_id: StepId(1) },
        WalRecord::ModelStepFinished {
            hooks: None,
            step_id: StepId(1),
            assistant_message: assistant_one_call().into(),
            usage: Usage::default(),
            finish_reason: Some(broca_protocol::content::FinishReason::ToolCalls),
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        },
        WalRecord::ToolResult {
            hooks: None,
            batch_id: BatchId("b1".into()),
            result: ToolResult {
                tool_call_id: ToolCallId("c1".into()),
                output: ToolOutput::Text { text: "did".into() },
                is_error: false,
            }
            .into(),
            outcome: DispatchOutcome::Completed,
            reason: None,
            withdraw_elicitation: None,
            provider_held: None,
        },
        WalRecord::RunFinished {
            ts_ms: None,
            reason: RunFinishReason::Completed,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        },
    ]
}

/// A run that emits a fixed record sequence, pausing between each so a test can attach a
/// subscriber at every record boundary. `advance` releases the next record.
struct SteppedRun {
    advance: Arc<Notify>,
    emitted: Arc<Notify>,
}

/// A run that emits the record sequence FREE-RUNNING (no pause), each with a tiny yield —
/// so a subscriber attaching concurrently is LIKELY to hit the overlap window where a
/// record is published to the ring AFTER the subscriber registered (R) but was already
/// fsynced (seq ≤ H). That overlap is exactly what the dedup (skip cursor.wal_seq ≤ H)
/// exists to handle; the stepped run (quiescent at attach) never exercises it.
struct FreeRun;

#[async_trait]
impl RunExecutor for FreeRun {
    async fn execute(
        &self,
        mut job: RunJob,
        _out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        // Admit immediately so the actor's start_run unblocks (the test drives emission
        // after route_send returns).
        if let Some(ack) = job.on_admitted.take() {
            let _ = ack.send(Ok(job.req.run_id.clone()));
        }
        for rec in record_sequence(&job.req.run_id, &job.req.session) {
            job.writer.record(rec);
            job.writer.sync().await.expect("sync");
            tokio::task::yield_now().await; // widen the overlap window
        }
        Ok(RunSummary {
            reason: RunFinishReason::Completed,
            paused: None,
            steps: 1,
            usage: Usage::default(),
            retries_used: 0,
            final_step_finish_reason: None,
        })
    }
}

#[async_trait]
impl RunExecutor for SteppedRun {
    async fn execute(
        &self,
        mut job: RunJob,
        _out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        // Admit immediately so the actor's start_run unblocks BEFORE the test drives the
        // per-record advance loop (else route_send would deadlock awaiting admission).
        if let Some(ack) = job.on_admitted.take() {
            let _ = ack.send(Ok(job.req.run_id.clone()));
        }
        let w = &job.writer;
        for rec in record_sequence(&job.req.run_id, &job.req.session) {
            // Pause BEFORE each record so the test can attach a subscriber at this boundary.
            self.advance.notified().await;
            w.record(rec);
            w.sync().await.expect("sync");
            self.emitted.notify_one();
        }
        Ok(RunSummary {
            reason: RunFinishReason::Completed,
            paused: None,
            steps: 1,
            usage: Usage::default(),
            retries_used: 0,
            final_step_finish_reason: None,
        })
    }
}

fn assistant_one_call() -> AssistantMessage {
    AssistantMessage {
        message_id: MessageId("m1".into()),
        content: vec![ContentBlock::ToolCall(ToolCall {
            thought_signature: None,
            tool_call_id: ToolCallId("c1".into()),
            tool_name: "alpha".into(),
            input: serde_json::json!({"x": 1}),
        })],
    }
}

fn min_config() -> RunConfig {
    RunConfig {
        model: broca_protocol::request::ModelHandle::new("fake", "fake"),
        tools: vec![].into(),
        tool_choice: Default::default(),
        generation: Default::default(),
        response_format: Default::default(),
        provider_options: serde_json::Value::Null,
        cache_tiers: serde_json::Value::Null,
        context_epoch: 0,
        renderer_version: 1,
        frozen_render_config: serde_json::Value::Null,
        auth_selection: None,
        tools_were_omitted: false,
        budget: None,
        context_limit: None,
        work_class: None,
        variant: None,
        service_tier: None,
        stop_when: Vec::new(),
        context_management: None,
    }
}

/// An opener wiring the SteppedRun executor with real file stores + the fan-out.
struct SteppedOpener {
    lease_store: Arc<FileLeaseStore>,
    wal_store: Arc<FileWalStore>,
    advance: Arc<Notify>,
    emitted: Arc<Notify>,
}

#[async_trait]
impl LineageOpener for SteppedOpener {
    async fn open(
        &self,
        key: &LineageKey,
        gen: u64,
        registry: RegistryHandle,
    ) -> Result<ActorHandle, OpenError> {
        let lease = self
            .lease_store
            .acquire(&broca_core::lease_key_for(key))
            .map_err(|e| OpenError::Open(e.to_string()))?;
        let (fanout, observer) = Fanout::new(self.wal_store.clone(), key.clone());
        let wal = self
            .wal_store
            .open(key)
            .map_err(|e| OpenError::Open(e.to_string()))?;
        let writer = Arc::new(
            WalHandle::spawn_observed(wal, lease.epoch(), Some(observer)).expect("spawn writer"),
        );
        Ok(LineageActor::spawn(
            key.clone(),
            gen,
            lease,
            writer,
            Arc::new(SteppedRun {
                advance: self.advance.clone(),
                emitted: self.emitted.clone(),
            }),
            self.wal_store.clone(),
            registry,
            std::time::Duration::from_secs(3600),
            fanout,
            None,
            None,
        ))
    }
}

/// An opener wiring the FREE-running executor (for the concurrent-attach stress).
struct FreeOpener {
    lease_store: Arc<FileLeaseStore>,
    wal_store: Arc<FileWalStore>,
}

#[async_trait]
impl LineageOpener for FreeOpener {
    async fn open(
        &self,
        key: &LineageKey,
        gen: u64,
        registry: RegistryHandle,
    ) -> Result<ActorHandle, OpenError> {
        let lease = self
            .lease_store
            .acquire(&broca_core::lease_key_for(key))
            .map_err(|e| OpenError::Open(e.to_string()))?;
        let (fanout, observer) = Fanout::new(self.wal_store.clone(), key.clone());
        let wal = self
            .wal_store
            .open(key)
            .map_err(|e| OpenError::Open(e.to_string()))?;
        let writer = Arc::new(
            WalHandle::spawn_observed(wal, lease.epoch(), Some(observer)).expect("spawn writer"),
        );
        Ok(LineageActor::spawn(
            key.clone(),
            gen,
            lease,
            writer,
            Arc::new(FreeRun),
            self.wal_store.clone(),
            registry,
            std::time::Duration::from_secs(3600),
            fanout,
            None,
            None,
        ))
    }
}

fn text_req(key: &BindIdentity, run: &str) -> RunRequest {
    RunRequest {
        retention: None,
        title: None,
        on_restart: None,
        keep_warm: None,
        scope: None,
        send_id: None,
        request_identity_digest: None,
        project_id: None,
        principal: None,
        run_id: RunId(run.into()),
        session: key.clone(),
        model: broca_protocol::request::ModelHandle::new("fake", "fake"),
        input: vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            role: Role::User,
            content: vec![ContentBlock::Text { text: "hi".into() }],
        }],
        tools: vec![],
        policy_allowed: Default::default(),
        tool_choice: Default::default(),
        generation: Default::default(),
        cache: None,
        stop_when: vec![],
        submission_id: None,
        budget: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
    }
}

/// Collect a subscriber's delivered control units (until it sees a terminal), asserting NO
/// `Detached` and that cursors are STRICTLY increasing (no gap-as-dup, no out-of-order).
async fn collect_until_terminal(rx: &mut mpsc::Receiver<Delivery>) -> Vec<ControlUnit> {
    let mut units = Vec::new();
    let mut last_cursor: Option<broca_core::Cursor> = None;
    while let Ok(Some(d)) =
        broca_test_support::event_timeout("collect_until_terminal", rx.recv(), || {
            format!("collected_units={}", units.len())
        })
        .await
    {
        match d {
            Delivery::Control(event) => {
                let (cursor, unit) = event.as_ref();
                if let Some(prev) = last_cursor {
                    assert!(
                        *cursor > prev,
                        "cursors must be strictly increasing (no dup/gap): {prev:?} then {cursor:?}"
                    );
                }
                last_cursor = Some(*cursor);
                let terminal = matches!(
                    unit,
                    ControlUnit::RunFinished { .. } | ControlUnit::Paused { .. }
                );
                units.push(unit.clone());
                if terminal {
                    break;
                }
            }
            // This harness's runs emit no display; ignore any display delivery.
            Delivery::Display(_) => {}
            Delivery::Error(error) => panic!("subscriber replay failed unexpectedly: {error}"),
            Delivery::Expired { .. } => panic!("forever subscriber expired"),
            Delivery::Detached => panic!("subscriber detached unexpectedly (a gap occurred)"),
        }
    }
    units
}

/// The full expected control stream the run produces (5 records → these units in order).
fn expected_units() -> Vec<&'static str> {
    vec![
        "RunStarted",
        "StepStarted",
        "AssistantMessage",
        "ToolCall",
        "StepFinished",
        "ToolResult",
        "RunFinished",
    ]
}

fn tag(u: &ControlUnit) -> &'static str {
    match u {
        ControlUnit::RunStarted { .. } => "RunStarted",
        ControlUnit::StepStarted { .. } => "StepStarted",
        ControlUnit::BudgetFired { .. } => "BudgetFired",
        ControlUnit::AssistantMessage { .. } => "AssistantMessage",
        ControlUnit::ToolCall { .. } => "ToolCall",
        ControlUnit::ToolResult { .. } => "ToolResult",
        ControlUnit::StepFinished { .. } => "StepFinished",
        ControlUnit::RunFinished { .. } => "RunFinished",
        ControlUnit::Paused { .. } => "Paused",
        ControlUnit::Error { .. } => "Error",
        ControlUnit::DisplayReset => "DisplayReset",
        ControlUnit::ApprovalRequest { .. } => "ApprovalRequest",
    }
}

/// THE BARRIER-RACE PROOF: attach a subscriber at EVERY record boundary (0..=5 records
/// emitted before the subscribe), and assert it receives the COMPLETE control stream with
/// no gap/dup. `attach_after` = how many records are durable when the subscriber attaches —
/// so the barrier's replay (seq≤H) covers `attach_after` records and the live ring covers
/// the rest; the subscriber must see ALL of them exactly once at every split.
#[tokio::test]
async fn attach_barrier_no_gap_no_dup_at_every_boundary() {
    for attach_after in 0..=5usize {
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-barrier-{}-{}",
            std::process::id(),
            attach_after
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        let key = BindIdentity::new(dir.join("proj"), "runner", "s1");

        let advance = Arc::new(Notify::new());
        let emitted = Arc::new(Notify::new());
        let opener = Arc::new(SteppedOpener {
            lease_store: Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal_store: Arc::new(FileWalStore::new(dir.join("wal"))),
            advance: advance.clone(),
            emitted: emitted.clone(),
        });
        let registry = LineageRegistry::new(opener);

        // Start the run (it pauses before the FIRST record).
        registry
            .route_send(&key, text_req(&key, "r1"), RunKind::FreshOrResume)
            .await
            .unwrap();

        // Emit `attach_after` records, awaiting each emission so they're durable.
        for _ in 0..attach_after {
            advance.notify_one();
            emitted.notified().await;
        }

        // Attach a subscriber NOW — at this exact boundary (attach_after records durable).
        let (sink, mut rx) = mpsc::channel::<Delivery>(64);
        let outcome = registry
            .route_subscribe(&key, FromCursor::Start, sink)
            .await
            .unwrap();
        assert_eq!(outcome, SubscribeOutcome::Attached, "attach must succeed");

        // Release the rest of the records concurrently with the subscriber draining.
        let advance2 = advance.clone();
        let emitted2 = emitted.clone();
        let pump = tokio::spawn(async move {
            for _ in attach_after..5 {
                advance2.notify_one();
                emitted2.notified().await;
            }
        });

        // The subscriber must receive the COMPLETE stream — exactly the 7 units, in order,
        // no gap (would panic in collect), no dup (strictly-increasing cursors).
        let got = collect_until_terminal(&mut rx).await;
        let got_tags: Vec<&str> = got.iter().map(tag).collect();
        assert_eq!(
            got_tags,
            expected_units(),
            "attach_after={attach_after}: subscriber must see the COMPLETE control stream \
             (replay seq≤H ⊕ live >H) with no gap/dup"
        );

        let _ = pump.await;
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// Attach while records are still being emitted and check two things: records that appear
/// in both WAL replay and the live stream are delivered only once, and records emitted
/// before the attach are still seen because the WAL replay covers them. Across many
/// interleavings the subscriber must end up with the full ordered stream, with no gap and no
/// duplicate.
#[tokio::test]
async fn attach_during_free_running_emission_no_gap_no_dup() {
    for round in 0..30 {
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-barrier-free-{}-{}",
            std::process::id(),
            round
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        let key = BindIdentity::new(dir.join("proj"), "runner", "s1");

        let opener = Arc::new(FreeOpener {
            lease_store: Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal_store: Arc::new(FileWalStore::new(dir.join("wal"))),
        });
        let registry = LineageRegistry::new(opener);

        // Start the run with ZERO subscribers attached: the observer gate is initially off,
        // so any prefix emitted before the attach must come back via the WAL replay.
        registry
            .route_send(&key, text_req(&key, "r1"), RunKind::FreshOrResume)
            .await
            .unwrap();

        // Attach at a varying moment relative to the free-running emission. Different
        // rounds cover both cases this barrier must handle: some records are already durable
        // before the attach starts, and some records overlap between the replayed durable
        // prefix and the newly enabled live stream.
        for _ in 0..(round % 5) {
            tokio::task::yield_now().await;
        }

        let (sink, mut rx) = mpsc::channel::<Delivery>(64);
        let outcome = registry
            .route_subscribe(&key, FromCursor::Start, sink)
            .await
            .unwrap();
        assert_eq!(outcome, SubscribeOutcome::Attached);

        // The subscriber must see the COMPLETE stream, each unit EXACTLY once (the
        // collect asserts strictly-increasing cursors = no dup, and no Detached = no gap).
        let got = collect_until_terminal(&mut rx).await;
        let got_tags: Vec<&str> = got.iter().map(tag).collect();
        assert_eq!(
            got_tags,
            expected_units(),
            "round {round}: subscriber attaching during free-running emission must see the \
             COMPLETE stream exactly once (gate-off prefix via replay, overlap via the seq≤H \
             dedup)"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[tokio::test]
async fn live_attach_skips_idle_history_and_receives_the_next_run() {
    let dir = broca_test_support::scratch_root().join(format!(
        "broca-live-attach-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let key = BindIdentity::new(dir.join("proj"), "runner", "s1");
    let advance = Arc::new(Notify::new());
    let emitted = Arc::new(Notify::new());
    let opener = Arc::new(SteppedOpener {
        lease_store: Arc::new(FileLeaseStore::new(dir.join("leases"))),
        wal_store: Arc::new(FileWalStore::new(dir.join("wal"))),
        advance: advance.clone(),
        emitted: emitted.clone(),
    });
    let registry = LineageRegistry::new(opener);

    registry
        .route_send(&key, text_req(&key, "history"), RunKind::FreshOrResume)
        .await
        .unwrap();
    for _ in 0..5 {
        advance.notify_one();
        emitted.notified().await;
    }
    broca_test_support::wait_for(
        "history run goes idle",
        async {
            loop {
                if matches!(
                    registry.route_status(&key).await.unwrap(),
                    LineageStatus::Idle
                ) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        },
        || format!("slot={:?}", registry.slot_state(&key)),
    )
    .await;
    assert!(
        matches!(
            registry.route_status(&key).await.unwrap(),
            LineageStatus::Idle
        ),
        "history run reaches an idle lineage before live attach"
    );

    let (sink, mut rx) = mpsc::channel::<Delivery>(64);
    registry
        .subscribe(&key, SubscribeFrom::Live, sink)
        .await
        .expect("live attach succeeds");
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv())
            .await
            .is_err(),
        "live attach remains quiet instead of replaying the completed history"
    );

    registry
        .route_send(&key, text_req(&key, "future"), RunKind::FreshOrResume)
        .await
        .unwrap();
    for _ in 0..5 {
        advance.notify_one();
        emitted.notified().await;
    }
    let got = collect_until_terminal(&mut rx).await;
    assert_eq!(got.iter().map(tag).collect::<Vec<_>>(), expected_units());
    assert!(matches!(
        got.first(),
        Some(ControlUnit::RunStarted {
            run_id,
            submission_id,
        }) if run_id == &RunId("future".into()) && submission_id.is_none()
    ));

    let _ = std::fs::remove_dir_all(&dir);
}
