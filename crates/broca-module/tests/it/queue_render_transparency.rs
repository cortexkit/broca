//! Queue render-transparency — the C7-equivalent gate for the durable queue.
//!
//! A queued-then-drained submission must run the SAME turn it would have if sent when idle:
//! the queue is a pure DEFERRAL, never a config-mutating path. Two proofs:
//!  1. ROUND-TRIP (mechanism): RunRequest → to_submission → from_submission reconstructs an
//!     identical RunRequest (every byte-affecting field survives — caught structurally by
//!     full struct equality, not a sampled subset).
//!  2. END-TO-END (behavior): with ambient PINNED (same stores/config, no drift), the
//!     second turn's first-provider-call RENDERED BYTES are identical whether that turn was
//!     sent while idle (append-episode) or queued-while-active then drained. Holding
//!     resolution constant, the deferral is the only variable, so equal bytes prove the
//!     queue path doesn't mutate rendering.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use broca_core::{
    lease_key_for, run_loop_on_writer_admitted_with_prior_snapshot, AllowAll, FileLeaseStore,
    FileWalStore, LeaseStore, OutputEvent, Provider, RunError, RunRequest, RunSummary, StartMode,
    StubToolPlane, WalHandle, WalStore,
};
use broca_module::actor::{LineageActor, SendOutcome};
use broca_module::executor::{RunExecutor, RunJob, RunKind};
use broca_module::fanout::{Delivery, Fanout, FromCursor};
use broca_module::registry::{
    ActorHandle, LineageKey, LineageOpener, LineageRegistry, OpenError, RegistryHandle,
};
use broca_protocol::caller::ControlUnit;
use broca_protocol::content::{AssistantMessage, ContentBlock, Message, Role, ToolCall};
use broca_protocol::request::{
    BudgetConfig, BudgetNudge, CacheRequest, CacheRole, CacheTail, ExecutionMode, GenerationConfig,
    ModelHandle, StopCondition, Tool, ToolChoice,
};
use broca_protocol::{
    BindIdentity, CallOptions, FinishReason, MessageId, NormalizedEvent, ProviderControl, RunId,
    StepId, SubmissionId, ToolCallId, Usage,
};
use tokio::sync::{mpsc, Notify};

// ── 1. ROUND-TRIP (mechanism) ──────────────────────────────────────────────

fn full_request(run: &str, session: &BindIdentity) -> RunRequest {
    RunRequest {
        session_class: None,
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
        session: session.clone(),
        model: ModelHandle::new("anthropic", "claude-opus-4-8"),
        input: vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: "do the thing".into(),
            }],
        }],
        tools: vec![Tool {
            name: "edit".into(),
            description: "edit a file".into(),
            input_schema: serde_json::json!({"type": "object"}),
            module: Some("aft".into()),
            execution_mode: ExecutionMode::Parallel,
        }],
        policy_allowed: ["edit".to_string()].into_iter().collect(),
        tool_choice: ToolChoice::Required,
        generation: GenerationConfig {
            max_output_tokens: Some(1024),
            temperature: Some(0.7),
            top_p: Some(0.9),
            stop_sequences: vec!["STOP".into()],
        },
        cache: Some(CacheRequest {
            role: Some(CacheRole::Ephemeral),
            ttl: Some("5m".into()),
            key: Some("alfonso-classify-v1".into()),
            // Populated so the round-trip below actually carries the tail declaration; this
            // fixture is never admitted, so the tools beside it are not a conflict here.
            tail: Some(CacheTail::Volatile),
            mode: None,
        }),
        stop_when: vec![StopCondition::StepCount { n: 7 }],
        submission_id: None,
        budget: Some(BudgetConfig {
            nudges: vec![BudgetNudge {
                at_tool_calls: 20,
                text: "budget nudge".into(),
            }],
            finalize_at_tool_calls: 30,
            finalize_text: "finalize".into(),
        }),
        context_limit: Some(200_000),
        work_class: Some("mason".into()),
        context_management: None,
        variant: Some("high".into()),
        service_tier: None,
        inherit_frozen_cache: false,
    }
}

/// to_submission → from_submission reconstructs an IDENTICAL RunRequest (modulo the
/// coordinator-assigned identity it re-supplies). Full struct equality, so a DROPPED
/// byte-affecting field is caught structurally — not a sampled subset.
#[test]
fn submission_round_trip_preserves_every_field() {
    let session = BindIdentity::new("/tmp/p", "runner", "s1");
    let original = full_request("r1", &session);

    let submission = original.to_submission();
    let rebuilt = RunRequest::from_submission(
        submission,
        original.run_id.clone(),
        original.session.clone(),
        SubmissionId("sub-r1".into()),
    );

    // Equal in every field EXCEPT submission_id (the consume-link the drain adds).
    let mut expected = original.clone();
    expected.submission_id = Some(SubmissionId("sub-r1".into()));
    assert_eq!(
        rebuilt, expected,
        "the submission round-trip must preserve every byte-affecting field; only \
         submission_id differs (the consume-link the drain adds)"
    );
}

#[test]
fn queued_project_observation_survives_serialization_without_changing_send_identity() {
    let session = BindIdentity::new("/tmp/p", "runner", "s1");
    let original = full_request("r1", &session);
    let legacy = original.to_submission();
    let mut observed = original.clone();
    observed.project_id = Some("pj-queued".into());
    let submitted = observed.to_submission();
    assert_eq!(broca_core::submission_difference(&legacy, &submitted), None);
    assert_eq!(
        broca_core::submission_identity_digest(&legacy),
        broca_core::submission_identity_digest(&submitted)
    );
    let serialized = serde_json::to_vec(&submitted).unwrap();
    let rebuilt = RunRequest::from_submission(
        serde_json::from_slice(&serialized).unwrap(),
        observed.run_id.clone(),
        session,
        SubmissionId("queued".into()),
    );
    assert_eq!(rebuilt.project_id.as_deref(), Some("pj-queued"));
}

// ── 2. END-TO-END (behavior): idle-append vs queued-drain render bytes ───────

/// Records the rendered bytes of each run's FIRST step (the C7 hash domain), keyed by
/// run_id, then replies with a one-line answer so the run ends.
#[derive(Clone)]
struct RecordingProvider {
    run_id: String,
    sent_first: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

#[async_trait]
impl Provider for RecordingProvider {
    fn render_request(&self, opts: &CallOptions) -> Vec<u8> {
        // Model an identity-embedding wire family such as Google Code Assist. The canonical
        // renderer deliberately skips StepContext, so the explicit field keeps this test from
        // masking a run-id change during queue drain.
        serde_json::to_vec(&serde_json::json!({
            "request": serde_json::to_value(opts).unwrap(),
            "run_id": opts.step_context.as_ref().map(|context| &context.run_id),
        }))
        .unwrap()
    }

    async fn step(&self, step_id: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        if step_id.0 == 1 {
            let bytes = self.render_request(opts);
            self.sent_first
                .lock()
                .unwrap()
                .entry(self.run_id.clone())
                .or_insert(bytes);
        }
        let (tx, rx) = mpsc::channel(16);
        let tool_turn = self.run_id == "recover-stop" && step_id.0 == 1;
        tokio::spawn(async move {
            let content = if tool_turn {
                vec![ContentBlock::ToolCall(ToolCall {
                    tool_call_id: ToolCallId("recover-call".into()),
                    tool_name: "edit".into(),
                    input: serde_json::json!({"path": "should-not-run"}),
                    thought_signature: None,
                })]
            } else {
                vec![ContentBlock::Text {
                    text: "done".into(),
                }]
            };
            let _ = tx
                .send(NormalizedEvent::Control(
                    ProviderControl::AssistantMessage {
                        message: AssistantMessage {
                            message_id: MessageId("m".into()),
                            content,
                        },
                    },
                ))
                .await;
            let _ = tx
                .send(NormalizedEvent::Control(ProviderControl::Finished {
                    usage: Usage::default(),
                    finish_reason: if tool_turn {
                        FinishReason::ToolCalls
                    } else {
                        FinishReason::Stop
                    },
                    retries_used: 0,
                }))
                .await;
        });
        rx
    }
}

/// Drives the REAL run_loop_on_writer with a RecordingProvider — the genuine render path, so
/// the captured bytes are exactly what a live run would send. The run whose id == `gate_run`
/// BLOCKS after recording (so a test can queue a submission while it's "active").
struct RecExecutor {
    sent_first: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    skip_materialization: Arc<Mutex<HashMap<String, bool>>>,
    wal_store: Arc<FileWalStore>,
    gate: Arc<Notify>,
    gate_run: Option<String>,
    tool_plane: Arc<StubToolPlane>,
}

#[async_trait]
impl RunExecutor for RecExecutor {
    async fn execute(
        &self,
        mut job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        let is_gated = self.gate_run.as_deref() == Some(job.req.run_id.0.as_str());
        self.skip_materialization
            .lock()
            .unwrap()
            .insert(job.req.run_id.0.clone(), job.skip_materialization);
        let provider = RecordingProvider {
            run_id: job.req.run_id.0.clone(),
            sent_first: self.sent_first.clone(),
        };
        let mode = match job.kind {
            RunKind::AppendEpisode => StartMode::AppendEpisode,
            RunKind::ResumeAfterAuth => StartMode::ResumeAfterAuth,
            RunKind::FreshOrResume => StartMode::FreshOrResume,
        };
        // Forward the admission-ack into the real loop, which fires it at the RunStarted
        // barrier — so the actor's start_run unblocks and replies Active.
        let on_admitted = job.on_admitted.take();
        let prior_snapshot = job.prior_snapshot;
        let summary = run_loop_on_writer_admitted_with_prior_snapshot(
            job.req,
            &job.writer,
            mode,
            &provider,
            self.tool_plane.as_ref(),
            &AllowAll,
            self.wal_store.as_ref(),
            None,
            job.control,
            &out,
            on_admitted,
            Some(&prior_snapshot),
        )
        .await;
        if is_gated {
            // Hold the run "active" until released, so a concurrent send durably queues.
            self.gate.notified().await;
        }
        summary
    }
}

struct RecOpener {
    lease_store: Arc<FileLeaseStore>,
    wal_store: Arc<FileWalStore>,
    sent_first: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    skip_materialization: Arc<Mutex<HashMap<String, bool>>>,
    gate: Arc<Notify>,
    gate_run: Option<String>,
    tool_plane: Arc<StubToolPlane>,
}

#[async_trait]
impl LineageOpener for RecOpener {
    async fn open(
        &self,
        key: &LineageKey,
        gen: u64,
        registry: RegistryHandle,
    ) -> Result<ActorHandle, OpenError> {
        let lease = self
            .lease_store
            .acquire(&lease_key_for(key))
            .map_err(|e| OpenError::Open(e.to_string()))?;
        let (fanout, observer) = Fanout::new(self.wal_store.clone(), key.clone());
        let wal = self
            .wal_store
            .open(key)
            .map_err(|e| OpenError::Open(e.to_string()))?;
        let writer = Arc::new(
            WalHandle::spawn_observed(wal, lease.epoch(), Some(observer)).expect("spawn writer"),
        );
        let exec = Arc::new(RecExecutor {
            sent_first: self.sent_first.clone(),
            skip_materialization: self.skip_materialization.clone(),
            wal_store: self.wal_store.clone(),
            gate: self.gate.clone(),
            gate_run: self.gate_run.clone(),
            tool_plane: self.tool_plane.clone(),
        });
        Ok(LineageActor::spawn(
            key.clone(),
            gen,
            lease,
            writer,
            exec,
            self.wal_store.clone(),
            registry,
            Duration::from_secs(3600),
            fanout,
            None,
            None,
        ))
    }
}

fn text(run: &str, msg: &str, session: &BindIdentity) -> RunRequest {
    RunRequest {
        session_class: None,
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
        session: session.clone(),
        model: ModelHandle::new("anthropic", "claude-opus-4-8"),
        input: vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            role: Role::User,
            content: vec![ContentBlock::Text { text: msg.into() }],
        }],
        tools: vec![],
        policy_allowed: Default::default(),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: Some(CacheRequest {
            role: Some(CacheRole::Ephemeral),
            ttl: Some("5m".into()),
            key: Some("alfonso-classify-v1".into()),
            tail: None,
            mode: None,
        }),
        stop_when: vec![StopCondition::StepCount { n: 2 }],
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

fn omitted_text_submission(
    run: &str,
    msg: &str,
    session: &BindIdentity,
) -> broca_protocol::request::SubmissionRequest {
    let mut request = text(run, msg, session).to_submission();
    request.tools = None;
    request
}

/// Render-bytes slot keyed by run_id.
type SentMap = Arc<Mutex<HashMap<String, Vec<u8>>>>;

struct Rig {
    dir: std::path::PathBuf,
    registry: LineageRegistry,
    sent_first: SentMap,
    skip_materialization: Arc<Mutex<HashMap<String, bool>>>,
    gate: Arc<Notify>,
    wal_store: Arc<FileWalStore>,
    tool_plane: Arc<StubToolPlane>,
}

fn rig(tag: &str, gate_run: Option<&str>) -> Rig {
    let dir = broca_test_support::scratch_root().join(format!(
        "broca-qrt-{}-{}",
        std::process::id(),
        tag
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let sent_first: SentMap = Arc::new(Mutex::new(HashMap::new()));
    let skip_materialization = Arc::new(Mutex::new(HashMap::new()));
    let gate = Arc::new(Notify::new());
    let wal_store = Arc::new(FileWalStore::new(dir.join("wal")));
    let tool_plane = Arc::new(StubToolPlane::new().with_canned_text("edit", "edited"));
    let opener = Arc::new(RecOpener {
        lease_store: Arc::new(FileLeaseStore::new(dir.join("leases"))),
        wal_store: wal_store.clone(),
        sent_first: sent_first.clone(),
        skip_materialization: skip_materialization.clone(),
        gate: gate.clone(),
        gate_run: gate_run.map(|s| s.to_string()),
        tool_plane: tool_plane.clone(),
    });
    Rig {
        dir,
        registry: LineageRegistry::new(opener),
        sent_first,
        skip_materialization,
        gate,
        wal_store,
        tool_plane,
    }
}

async fn wait_for_bytes(slot: &SentMap, run: &str) -> Vec<u8> {
    broca_test_support::wait_for(
        "first-step render bytes",
        async {
            loop {
                if let Some(b) = slot.lock().unwrap().get(run) {
                    return b.clone();
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        },
        || {
            format!(
                "recorded runs={:?}, wanted={run}",
                slot.lock().unwrap().keys().collect::<Vec<_>>()
            )
        },
    )
    .await
}

async fn run_started_submission_id(
    rx: &mut mpsc::Receiver<Delivery>,
    expected_run_id: &str,
) -> Option<SubmissionId> {
    loop {
        let delivery =
            broca_test_support::wait_for("control stream announces RunStarted", rx.recv(), || {
                format!("expected_run_id={expected_run_id}")
            })
            .await
            .expect("the control stream remains open");
        match delivery {
            Delivery::Control(event) => {
                let (_, unit) = event.as_ref();
                if let ControlUnit::RunStarted {
                    run_id,
                    submission_id,
                } = unit
                {
                    if run_id.0 == expected_run_id {
                        return submission_id.clone();
                    }
                }
            }
            Delivery::Display(_) => {}
            Delivery::Error(error) => panic!("control-stream replay failed: {error}"),
            Delivery::Expired { .. } => panic!("forever subscriber expired"),
            Delivery::Detached => panic!("control stream detached before RunStarted"),
        }
    }
}

/// A direct run announces no submission, while the queued run drained after it announces the
/// exact `submission_id` returned while it was pending. The stream is attached from Start before
/// drain, so this exercises the replayed direct unit and the live drained unit through the same
/// durable projection.
#[tokio::test]
async fn control_stream_links_drained_run_to_pending_submission() {
    let rig = rig("submission-link", Some("r1"));
    let key = BindIdentity::new(rig.dir.join("proj"), "runner", "s1");

    rig.registry
        .route_send(&key, text("r1", "first turn", &key), RunKind::FreshOrResume)
        .await
        .unwrap();
    let _ = wait_for_bytes(&rig.sent_first, "r1").await;

    let (sink, mut stream) = mpsc::channel(64);
    rig.registry
        .route_subscribe(&key, FromCursor::Start, sink)
        .await
        .unwrap();
    assert_eq!(
        run_started_submission_id(&mut stream, "r1").await,
        None,
        "a directly sent run must not claim a queued submission"
    );

    let pending = rig
        .registry
        .route_submission(
            &key,
            RunId("r2".into()),
            omitted_text_submission("r2", "queued turn", &key),
            RunKind::FreshOrResume,
            true,
        )
        .await
        .unwrap();
    let submission_id = match pending {
        SendOutcome::Pending { submission_id } => submission_id,
        other => panic!("second turn must queue while first is active, got {other:?}"),
    };

    rig.gate.notify_one();
    assert_eq!(
        run_started_submission_id(&mut stream, "r2").await,
        Some(submission_id),
        "the drained run must announce the exact pending submission it consumed"
    );

    let _ = std::fs::remove_dir_all(&rig.dir);
}

/// THE GATE: turn-2's first-step render bytes are IDENTICAL whether turn-2 was sent while
/// idle (append-episode after turn-1 finished) or queued-while-turn-1-active then drained —
/// holding turn-1's history + all ambient config constant. So the queue deferral is
/// render-transparent (the C7-equivalent for the queue).
#[tokio::test]
async fn drained_run_renders_identical_bytes_to_idle_send() {
    // ── PATH A: idle. turn-1 finishes, THEN turn-2 sent (append-episode over turn-1). ──
    let a = rig("idle", None);
    let (reg_a, sent_a) = (&a.registry, &a.sent_first);
    let key_a = BindIdentity::new(a.dir.join("proj"), "runner", "s1");
    reg_a
        .route_send(
            &key_a,
            text("r1", "first turn", &key_a),
            RunKind::FreshOrResume,
        )
        .await
        .unwrap();
    // turn-1 auto-finishes (no gate); wait for it to record + go idle.
    let _ = wait_for_bytes(sent_a, "r1").await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    reg_a
        .route_submission(
            &key_a,
            RunId("r2".into()),
            omitted_text_submission("r2", "second turn", &key_a),
            RunKind::AppendEpisode,
            true,
        )
        .await
        .unwrap();
    let bytes_idle = wait_for_bytes(sent_a, "r2").await;

    // ── PATH B: queued. turn-1 HELD active, turn-2 queued, turn-1 released → drain. ──
    let b = rig("queued", Some("r1"));
    let (reg_b, sent_b, gate_b) = (&b.registry, &b.sent_first, &b.gate);
    let key_b = BindIdentity::new(b.dir.join("proj"), "runner", "s1");
    reg_b
        .route_send(
            &key_b,
            text("r1", "first turn", &key_b),
            RunKind::FreshOrResume,
        )
        .await
        .unwrap();
    // turn-1 records then HOLDS (gated). Wait for its render, then queue turn-2.
    let _ = wait_for_bytes(sent_b, "r1").await;
    let o2 = reg_b
        .route_submission_with_materialization(
            &key_b,
            RunId("r2".into()),
            omitted_text_submission("r2", "second turn", &key_b),
            RunKind::FreshOrResume,
            broca_module::actor::SendKey::RunId,
            true,
        )
        .await
        .unwrap();
    assert!(
        matches!(o2, SendOutcome::Pending { .. }),
        "turn-2 queues while turn-1 is active"
    );
    // Release turn-1 → it finishes → the actor drains turn-2 (consume → append-episode).
    gate_b.notify_one();
    // Queue drain preserves the exact run identity assigned to the original send.
    let bytes_queued = wait_for_bytes(sent_b, "r2").await;

    assert_eq!(
        a.skip_materialization.lock().unwrap().get("r2"),
        Some(&false),
        "an ordinary idle send keeps materialization"
    );
    assert_eq!(
        b.skip_materialization.lock().unwrap().get("r2"),
        Some(&true),
        "queue drain preserves the one-shot execution hint"
    );

    // The only policy difference is store projection, so the queued one-shot send must render
    // exactly like the idle materialized send.
    assert_eq!(
        bytes_idle, bytes_queued,
        "a queued-then-drained turn must render byte-identical to the same turn sent idle \
         (the queue is a pure deferral, not a config-mutating path)"
    );

    let _ = std::fs::remove_dir_all(&a.dir);
    let _ = std::fs::remove_dir_all(&b.dir);
}

fn recovered_stop_config() -> broca_wal::RunConfig {
    broca_wal::RunConfig {
        model: ModelHandle::new("anthropic", "claude-opus-4-8"),
        tools: vec![Tool {
            name: "edit".into(),
            description: "edit a file".into(),
            input_schema: serde_json::json!({"type": "object"}),
            module: Some("aft".into()),
            execution_mode: ExecutionMode::Parallel,
        }]
        .into(),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        response_format: Default::default(),
        provider_options: serde_json::Value::Null,
        cache_tiers: serde_json::Value::Null,
        context_epoch: 0,
        renderer_version: 1,
        frozen_render_config: serde_json::json!({"render_config": "stateless"}),
        auth_selection: None,
        tools_were_omitted: false,
        budget: None,
        context_limit: None,
        work_class: None,
        variant: None,
        service_tier: None,
        stop_when: vec![StopCondition::StepCount { n: 1 }],
        context_management: None,
    }
}

#[tokio::test]
async fn crash_recovery_preserves_stop_before_tool_dispatch() {
    let rig = rig("recover-stop", None);
    let key = BindIdentity::new(rig.dir.join("proj"), "runner", "s1");
    let mut wal = rig
        .wal_store
        .open(&key)
        .expect("open WAL to seed the admitted run");
    wal.append(
        1,
        &broca_wal::WalRecord::RunStarted {
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
            run_id: RunId("recover-stop".into()),
            session: key.clone(),
            config: Box::new(recovered_stop_config()),
            input: text("recover-stop", "edit after recovery", &key)
                .input
                .into(),
            origin: broca_wal::RunOrigin::Fresh,
            submission_id: None,
        },
        true,
    )
    .expect("seed the crash-cut WAL");
    drop(wal);

    // Opening the actor first recovers the unfinished durable episode before serving status.
    let _ = rig.registry.route_status(&key).await.unwrap();

    let mut terminal_reason = None;
    broca_test_support::wait_for(
        "recovered crash-cut terminal",
        async {
            loop {
                let records = rig.wal_store.read_records(&key).unwrap();
                terminal_reason = records.iter().find_map(|frame| match &frame.record {
                    broca_wal::WalRecord::RunFinished { reason, .. } => Some(*reason),
                    _ => None,
                });
                if terminal_reason.is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        },
        || {
            broca_test_support::record_kinds(
                rig.wal_store
                    .read_records(&key)
                    .unwrap()
                    .iter()
                    .map(|f| &f.record),
            )
        },
    )
    .await;

    assert_eq!(
        terminal_reason,
        Some(broca_protocol::RunFinishReason::MaxSteps),
        "the recovered run must reach its original stop boundary"
    );
    assert!(
        rig.tool_plane.log().is_empty(),
        "the recovered StepCount(1) boundary must stop before the tool plane"
    );
    let _ = std::fs::remove_dir_all(&rig.dir);
}
