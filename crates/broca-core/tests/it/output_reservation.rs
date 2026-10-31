//! A step's output reservation stays inside the run's context limit: the run loop lowers a
//! step's output cap when the estimated input plus the frozen cap would pass the limit, records
//! the lowered cap before sending, and ends the run `context_overflow` without sending when not
//! even the provider's floor fits. A recorded cap is rendered again by a resume and a cache warm.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use broca_core::output_reservation::{margin, OutputLimits};
use broca_core::{
    canonical_request_bytes, provider::Provider, request_digest_hex,
    run_loop_resume_after_auth_with_control, run_loop_with_control, AdmissionStoreSnapshot,
    AllowAll, FileLeaseStore, FileWalStore, MaterializedStore, Persister, RunControl, RunRequest,
    SessionStore, StorePrefix, StubReply, StubToolPlane, WalStore,
};
use broca_protocol::{
    caller::RunFinishReason,
    content::{AssistantMessage, ContentBlock, Message, Role, ToolCall, ToolOutput},
    provider::{AttemptOutcome, ModelAttempt, NormalizedEvent, ProviderControl},
    request::{CallOptions, ExecutionMode, ModelHandle, StopCondition, Tool, ToolChoice},
    ErrorClass, FinishReason, GenerationConfig, MessageId, RunId, StepId, ToolCallId, Usage,
};
use broca_wal::{FileWal, FramedRecord, Wal, WalRecord};
use serde_json::json;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// A 1M-token window and a catalog output limit of 131,072 tokens.
const WINDOW: u64 = 1_048_576;
const CAP: u32 = 131_072;

/// One request the provider was asked to send.
#[derive(Clone)]
struct Sent {
    step: u64,
    opts: CallOptions,
    bytes: Vec<u8>,
}

impl Sent {
    fn cap(&self) -> Option<u32> {
        self.opts
            .step_context
            .as_ref()
            .and_then(|step| step.max_output_tokens)
    }
}

/// A provider whose rendered bytes carry the output cap the way a wire body does, that reports
/// each request's body length in an attempt record, and that declares the window model's cap.
#[derive(Clone)]
struct WindowProvider {
    /// Step 1's reported input tokens in the default script; later default steps report a small count.
    first_input: u64,
    limits: Option<OutputLimits>,
    sent: Arc<Mutex<Vec<Sent>>>,
    scripts: BTreeMap<u64, Vec<NormalizedEvent>>,
}

impl WindowProvider {
    fn new(first_input: u64) -> Self {
        Self {
            first_input,
            limits: Some(OutputLimits::new(CAP, 0)),
            sent: Arc::default(),
            scripts: BTreeMap::new(),
        }
    }

    fn without_limits(mut self) -> Self {
        self.limits = None;
        self
    }

    fn sent(&self) -> Vec<Sent> {
        self.sent.lock().unwrap().clone()
    }

    fn script(&self, step: u64) -> Vec<NormalizedEvent> {
        if let Some(events) = self.scripts.get(&step) {
            return events.clone();
        }
        let (content, input, finish) = if step == 1 {
            (
                vec![ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId("t1".into()),
                    tool_name: "read".into(),
                    input: json!({"path": "src/lib.rs"}),
                })],
                self.first_input,
                FinishReason::ToolCalls,
            )
        } else {
            (
                vec![ContentBlock::Text {
                    text: "done".into(),
                }],
                50,
                FinishReason::Stop,
            )
        };
        vec![
            NormalizedEvent::Control(ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id: MessageId(format!("m{step}")),
                    content,
                },
            }),
            NormalizedEvent::Control(ProviderControl::Finished {
                usage: Usage {
                    input_tokens: Some(input),
                    output_tokens: Some(10),
                    ..Default::default()
                },
                finish_reason: finish,
                retries_used: 0,
            }),
        ]
    }
}

fn attempt(bytes: &[u8]) -> ModelAttempt {
    ModelAttempt {
        content_progress: Default::default(),
        attempt: 0,
        request_body_len: bytes.len() as u64,
        request_body_sha256: request_digest_hex(bytes),
        http_status: Some(200),
        content_type: None,
        response_body_len: 1,
        response_body_sha256: request_digest_hex(&[]),
        response_body_cap: None,
        outcome: AttemptOutcome::Complete,
        ts_ms: 0,
        sent_at_ms: None,
        transport: None,
        websocket_request: None,
        cache_evidence: None,
        websocket_close_code: None,
        input_transformations: None,
        input_transformations_unmeasured: false,
        account_observation: Default::default(),
        credential_refresh: None,
        served_account_ref: None,
    }
}

#[async_trait]
impl Provider for WindowProvider {
    async fn step(&self, step_id: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        let bytes = self.render_request(opts);
        self.sent.lock().unwrap().push(Sent {
            step: step_id.0,
            opts: opts.clone(),
            bytes: bytes.clone(),
        });
        let mut events = vec![NormalizedEvent::Control(ProviderControl::AttemptFinished {
            attempt: attempt(&bytes),
        })];
        events.extend(self.script(step_id.0));
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            for event in events {
                let _ = tx.send(event).await;
            }
        });
        rx
    }

    /// The canonical bytes plus the cap the request carries, as a wire renderer would emit it.
    fn render_request(&self, opts: &CallOptions) -> Vec<u8> {
        let cap = opts
            .step_context
            .as_ref()
            .and_then(|step| step.max_output_tokens)
            .unwrap_or(CAP);
        let mut bytes = canonical_request_bytes(opts);
        bytes.extend_from_slice(format!("|max_output_tokens={cap}").as_bytes());
        bytes
    }

    fn output_limits(&self) -> Option<OutputLimits> {
        self.limits
    }
}

fn request(dir: &std::path::Path) -> RunRequest {
    RunRequest {
        input_written_at_ms: None,
        directives: None,
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
        run_id: RunId("r".into()),
        session: broca_protocol::BindIdentity::new(dir, "window", "s1"),
        model: ModelHandle::new("stub", "m"),
        input: vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: "read the file".into(),
            }],
        }],
        tools: vec![Tool {
            name: "read".into(),
            description: "read a file".into(),
            input_schema: json!({"type": "object"}),
            module: None,
            execution_mode: ExecutionMode::Parallel,
            reply: None,
        }],
        policy_allowed: ["read".to_string()].into_iter().collect(),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        stop_when: vec![StopCondition::StepCount { n: 5 }],
        submission_id: None,
        budget: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
        structured_output: None,
    }
}

fn scratch(tag: &str) -> std::path::PathBuf {
    let dir = broca_test_support::scratch_root()
        .join(format!("output-reservation-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// A tool result of about 5 KB.
fn plane() -> StubToolPlane {
    StubToolPlane::new().with_tool("read", |_| {
        StubReply::Output(ToolOutput::Text {
            text: "fn main() {}\n".repeat(400),
        })
    })
}

struct BoundaryPlane {
    inner: StubToolPlane,
    stop: Option<CancellationToken>,
}

#[async_trait]
impl broca_core::ToolPlane for BoundaryPlane {
    async fn batch_started(
        &self,
        ctx: &broca_core::BatchCtx,
        calls: &[ToolCall],
        manifest: &broca_core::ToolManifest,
    ) -> Result<(), broca_core::ToolPlaneError> {
        self.inner.batch_started(ctx, calls, manifest).await
    }

    async fn batch_finished(
        &self,
        ctx: &broca_core::BatchCtx,
    ) -> Result<(), broca_core::ToolPlaneError> {
        self.inner.batch_finished(ctx).await
    }

    async fn dispatch(
        &self,
        ctx: &broca_core::BatchCtx,
        call: &ToolCall,
    ) -> broca_core::DispatchResult {
        let result = self.inner.dispatch(ctx, call).await;
        // Shutdown after the second tool has completed, so the loop can pause at a clean
        // turn boundary instead of refusing a tool call that has not been sent.
        if call.tool_call_id.0 == "t2" {
            if let Some(stop) = &self.stop {
                stop.cancel();
            }
        }
        result
    }
}

async fn run(dir: &std::path::Path, provider: &impl Provider) -> RunFinishReason {
    run_with(
        dir,
        provider,
        request(dir),
        None,
        RunControl {
            admitted_context_limit: Some(WINDOW),
            ..Default::default()
        },
        false,
    )
    .await
}

async fn run_with(
    dir: &std::path::Path,
    provider: &impl Provider,
    req: RunRequest,
    store: Option<Arc<dyn SessionStore>>,
    control: RunControl,
    after_auth: bool,
) -> RunFinishReason {
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let (tx, mut rx) = mpsc::channel(512);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let tool_plane = BoundaryPlane {
        inner: plane(),
        stop: control.stop_requested.clone(),
    };
    let summary = if after_auth {
        run_loop_resume_after_auth_with_control(
            req,
            provider,
            &tool_plane,
            &lease,
            &AllowAll,
            &wal,
            store,
            control,
            &tx,
        )
        .await
    } else {
        run_loop_with_control(
            req,
            provider,
            &tool_plane,
            &lease,
            &AllowAll,
            &wal,
            store,
            control,
            &tx,
        )
        .await
    }
    .expect("run ok");
    drop(tx);
    collector.await.unwrap();
    summary.reason
}

fn records(dir: &std::path::Path) -> Vec<WalRecord> {
    FileWalStore::new(dir.join("wal"))
        .read_records(&request(dir).session)
        .unwrap()
        .into_iter()
        .map(|frame| frame.record)
        .collect()
}

/// Byte offset where the frame with `seq` ends, found by decoding the WAL file itself.
///
/// Walking the frames finds the end offset independently of the optional native file
/// identity used by `WalRangeRead::anchor_at`.
fn frame_byte_end(path: &std::path::Path, seq: u64) -> u64 {
    let bytes = std::fs::read(path).unwrap();
    let mut offset = 0usize;
    while offset < bytes.len() {
        match broca_wal::framing::decode_next(&bytes[offset..]) {
            broca_wal::framing::DecodeStep::Record { framed, consumed } => {
                offset += consumed;
                if framed.seq == seq {
                    return offset as u64;
                }
            }
            broca_wal::framing::DecodeStep::Lineage { consumed, .. } => {
                offset += consumed;
            }
            broca_wal::framing::DecodeStep::NewerWriter { ver, .. } => {
                panic!("newer WAL frame v{ver} at offset {offset}");
            }
            broca_wal::framing::DecodeStep::Corrupt(error) => {
                panic!("WAL corruption at offset {offset}: {error}");
            }
            broca_wal::framing::DecodeStep::TornTail => {
                panic!("torn WAL tail before seq {seq}");
            }
        }
    }
    panic!("seq {seq} not found in the WAL");
}

/// Rewrite the WAL to the records `keep` accepts among its first `upto`, as a crash after
/// those records would leave it.
fn rewrite_wal(dir: &std::path::Path, upto: usize, keep: impl Fn(&WalRecord) -> bool) {
    let wal_store = FileWalStore::new(dir.join("wal"));
    let path = wal_store.wal_path(&request(dir).session);
    let framed = FileWal::open(&path).unwrap().replay().unwrap();
    let _ = std::fs::remove_file(&path);
    let mut wal = FileWal::open(&path)
        .unwrap()
        .with_feature_floor(wal_store.feature_floor_path());
    for frame in framed.into_iter().take(upto) {
        if keep(&frame.record) {
            wal.append(frame.fence, &frame.record, true).unwrap();
        }
    }
}

/// Rewrite the WAL to its first `upto` records followed by `extra`, as a crash after a build
/// had written `extra` would leave it.
fn rewrite_wal_with(dir: &std::path::Path, upto: usize, extra: Option<WalRecord>) {
    let wal_store = FileWalStore::new(dir.join("wal"));
    let path = wal_store.wal_path(&request(dir).session);
    let framed = FileWal::open(&path).unwrap().replay().unwrap();
    let fence = framed[upto - 1].fence;
    let _ = std::fs::remove_file(&path);
    let mut wal = FileWal::open(&path)
        .unwrap()
        .with_feature_floor(wal_store.feature_floor_path());
    for frame in framed.into_iter().take(upto) {
        wal.append(frame.fence, &frame.record, true).unwrap();
    }
    if let Some(record) = extra {
        wal.append(fence, &record, true).unwrap();
    }
}

/// Step 2's input as the run loop estimates it: step 1's measured input plus the growth in
/// rendered bytes since step 1, at two bytes per token.
fn estimate(first_input: u64, step1: &Sent, step2: &Sent, provider: &WindowProvider) -> u64 {
    let mut unclamped = step2.opts.clone();
    unclamped.step_context.as_mut().unwrap().max_output_tokens = None;
    let grown = provider.render_request(&unclamped).len() - step1.bytes.len();
    first_input + (grown as u64).div_ceil(2)
}

const LAST_INPUT: u64 = 1_005_937 + 1_105;

fn tool_step(step: u64, usage: Usage) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(format!("m{step}")),
                content: vec![ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId(format!("t{step}")),
                    tool_name: "read".into(),
                    input: json!({"path": "src/lib.rs"}),
                })],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage,
            finish_reason: FinishReason::ToolCalls,
            retries_used: 0,
        }),
    ]
}

fn continuing_provider() -> WindowProvider {
    let mut provider = WindowProvider::new(916_594);
    provider.scripts.insert(
        2,
        tool_step(
            2,
            Usage {
                input_tokens: Some(1_105),
                cached_input_tokens: Some(1_005_937),
                output_tokens: Some(10),
                ..Default::default()
            },
        ),
    );
    provider
}

fn resume_control(stop: Option<CancellationToken>) -> RunControl {
    RunControl {
        admitted_context_limit: Some(WINDOW),
        stop_requested: stop,
        ..Default::default()
    }
}

fn resumable_request(dir: &std::path::Path) -> RunRequest {
    let mut req = request(dir);
    req.on_restart = Some(broca_protocol::request::OnRestart::Resume);
    req
}

fn assert_next_clamp(
    wal: &[WalRecord],
    measured: &Sent,
    next: &Sent,
    provider: &WindowProvider,
) -> WalRecord {
    let start = wal
        .iter()
        .rposition(|record| matches!(record, WalRecord::RunStarted { .. }))
        .unwrap();
    let wal = &wal[start..];
    let expected_input = estimate(LAST_INPUT, measured, next, provider);
    let expected_cap = WINDOW - expected_input - margin(WINDOW);
    let expected = WalRecord::OutputReservationClamped {
        step_id: StepId(next.step),
        max_output_tokens: expected_cap as u32,
        estimated_input_tokens: expected_input,
        context_limit: WINDOW,
    };
    assert!(
        wal.contains(&expected),
        "next step must record {expected:?}"
    );
    assert_eq!(next.cap(), Some(expected_cap as u32));
    let clamp_at = wal.iter().position(|record| *record == expected).unwrap();
    let attempt_at = wal
        .iter()
        .position(|record| {
            matches!(record,
                WalRecord::ModelAttemptFinished { step_id, .. } if step_id.0 == next.step
            )
        })
        .unwrap();
    assert!(
        clamp_at < attempt_at,
        "clamp must be durable before sending"
    );
    expected
}

/// A frozen materialized prefix keeps the measured turns out of the resume's WAL tail.
/// Projection deliberately lags while the resumed loop writes its new turns.
struct MeasuredPrefixStore {
    prefix: StorePrefix,
    admission: AdmissionStoreSnapshot,
    reads: std::sync::atomic::AtomicUsize,
}

impl MeasuredPrefixStore {
    fn from_wal(dir: &std::path::Path) -> Self {
        let wal = FileWalStore::new(dir.join("wal"));
        let range = wal
            .read_records_range(&request(dir).session, 0, None, None)
            .unwrap();
        let frames = wal.read_records(&request(dir).session).unwrap();
        let watermark = broca_core::latest_projection_boundary(&frames, 0).unwrap();
        let prefix_frames: Vec<_> = frames
            .into_iter()
            .filter(|frame| frame.seq <= watermark)
            .collect();
        let state = broca_core::replay(
            &prefix_frames
                .iter()
                .map(|frame| frame.record.clone())
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let lineage_id = range.lineage_id.map(|id| id.to_string());
        Self {
            prefix: StorePrefix {
                lineage_id: lineage_id.clone(),
                prefix: state.prompt,
                completed_steps: state.completed_steps,
                usage: state.usage,
                retries_used: state.retries_used,
                last_step_usage: state.last_step_usage,
                last_step_finish_reason: state.last_step_finish_reason,
                watermark_seq: watermark,
                context_epoch: 0,
            },
            admission: AdmissionStoreSnapshot {
                lineage_id,
                watermark_seq: watermark,
                anchor: range.anchor_at(watermark),
                checkpoint: Some(
                    serde_json::from_slice(
                        &serde_json::to_vec(
                            &broca_core::AdmissionCheckpointV1::from_frames(&prefix_frames)
                                .unwrap(),
                        )
                        .unwrap(),
                    )
                    .unwrap(),
                ),
            },
            reads: Default::default(),
        }
    }
}

impl MaterializedStore for MeasuredPrefixStore {
    fn read_prefix_with_watermark(
        &self,
        _: &broca_protocol::BindIdentity,
    ) -> Result<StorePrefix, String> {
        self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(self.prefix.clone())
    }

    fn read_admission_snapshot(
        &self,
        _: &broca_protocol::BindIdentity,
    ) -> Result<Option<AdmissionStoreSnapshot>, String> {
        Ok(Some(self.admission.clone()))
    }
}

impl Persister for MeasuredPrefixStore {
    fn project(&self, _: &broca_protocol::BindIdentity, _: &[FramedRecord]) -> Result<u64, String> {
        Ok(self.prefix.watermark_seq)
    }
}

async fn compare_full_and_sparse_continuation(
    dir: &std::path::Path,
    store: Arc<MeasuredPrefixStore>,
    req: RunRequest,
    measured: &Sent,
    after_auth: bool,
    provider: impl Fn() -> WindowProvider,
) {
    let wal = FileWalStore::new(dir.join("wal"));
    let frames = wal.read_records(&req.session).unwrap();
    let head = frames.last().unwrap().seq;
    let snapshot =
        broca_core::load_prior_wal_snapshot(&wal, &req.session, head, Some(store.as_ref()))
            .unwrap();
    assert_eq!(
        snapshot.base_watermark, store.prefix.watermark_seq,
        "resume must use the checkpoint and tail, not fall back to full replay"
    );
    let full_state = broca_core::replay(
        &frames
            .iter()
            .map(|frame| frame.record.clone())
            .collect::<Vec<_>>(),
    )
    .unwrap();
    assert_eq!(
        snapshot.checkpoint_at_head.output_reservation.as_ref(),
        Some(&full_state.output_reservation)
    );
    let wal_path = wal.wal_path(&req.session);
    let initial_bytes = std::fs::read(&wal_path).unwrap();

    // Restore the identical log, including its lineage header, between the two continuations.
    let full = provider();
    assert_eq!(
        run_with(
            dir,
            &full,
            req.clone(),
            None,
            resume_control(None),
            after_auth
        )
        .await,
        RunFinishReason::Completed
    );
    let expected = assert_next_clamp(&records(dir), measured, full.sent().last().unwrap(), &full);
    std::fs::write(&wal_path, initial_bytes).unwrap();

    let resumed = provider();
    assert_eq!(
        run_with(
            dir,
            &resumed,
            req,
            Some(store.clone()),
            resume_control(None),
            after_auth
        )
        .await,
        RunFinishReason::Completed
    );
    assert_eq!(
        assert_next_clamp(
            &records(dir),
            measured,
            resumed.sent().last().unwrap(),
            &resumed
        ),
        expected
    );
    let (actual, expected) = (resumed.sent(), full.sent());
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.iter().zip(&expected) {
        assert_eq!(actual.step, expected.step);
        assert_eq!(actual.bytes, expected.bytes);
    }
    assert!(store.reads.load(std::sync::atomic::Ordering::SeqCst) > 0);
}

#[tokio::test]
async fn restart_resume_restores_the_last_measurement_from_a_materialized_prefix() {
    let dir = scratch("restart-measurement");
    let stop = CancellationToken::new();
    let before = continuing_provider();
    assert_eq!(
        run_with(
            &dir,
            &before,
            resumable_request(&dir),
            None,
            resume_control(Some(stop)),
            false
        )
        .await,
        RunFinishReason::Interrupted
    );
    let initial = records(&dir);
    assert!(initial.iter().any(|record| matches!(
        record,
        WalRecord::RunPaused {
            reason: broca_protocol::PauseReason::Restart,
            ..
        }
    )));
    assert!(
        before.sent()[1].cap().is_some(),
        "the step before restart was clamped"
    );
    let store = Arc::new(MeasuredPrefixStore::from_wal(&dir));
    assert_eq!(store.prefix.completed_steps, 2);
    compare_full_and_sparse_continuation(
        &dir,
        store,
        resumable_request(&dir),
        &before.sent()[1],
        false,
        continuing_provider,
    )
    .await;
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn crash_continuation_restores_the_last_measurement_from_a_materialized_prefix() {
    let dir = scratch("crash-measurement");
    let before = continuing_provider();
    assert_eq!(
        run_with(
            &dir,
            &before,
            resumable_request(&dir),
            None,
            resume_control(Some(CancellationToken::new())),
            false
        )
        .await,
        RunFinishReason::Interrupted
    );
    let wal = FileWalStore::new(dir.join("wal"));
    let frames = wal.read_records(&request(&dir).session).unwrap();
    assert!(matches!(
        frames.last().unwrap().record,
        WalRecord::RunPaused { .. }
    ));
    let target = frames[frames.len() - 2].seq;
    let end = frame_byte_end(&wal.wal_path(&request(&dir).session), target);
    std::fs::OpenOptions::new()
        .write(true)
        .open(wal.wal_path(&request(&dir).session))
        .unwrap()
        .set_len(end)
        .unwrap();
    assert!(!records(&dir).iter().any(|record| matches!(
        record,
        WalRecord::RunPaused { .. } | WalRecord::RunFinished { .. }
    )));
    let store = Arc::new(MeasuredPrefixStore::from_wal(&dir));
    compare_full_and_sparse_continuation(
        &dir,
        store,
        resumable_request(&dir),
        &before.sent()[1],
        false,
        continuing_provider,
    )
    .await;
    let _ = std::fs::remove_dir_all(dir);
}

fn auth_error() -> NormalizedEvent {
    NormalizedEvent::Control(ProviderControl::Error {
        error: broca_protocol::ProviderError {
            provider_details: None,
            provider_subcode: None,
            refusal: None,
            class: ErrorClass::Auth,
            message: "credential expired".into(),
            status: Some(401),
            retry_after_secs: None,
            provider_code: None,
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
        },
        retries_used: 0,
    })
}

#[tokio::test]
async fn auth_resume_preserves_the_prefix_measurement_past_an_unmeasured_step() {
    let dir = scratch("auth-measurement");
    let mut before = continuing_provider();
    before.scripts.insert(3, vec![auth_error()]);
    assert_eq!(
        run_with(
            &dir,
            &before,
            request(&dir),
            None,
            resume_control(None),
            false
        )
        .await,
        RunFinishReason::Interrupted
    );
    assert!(records(&dir).iter().any(|record| matches!(
        record,
        WalRecord::RunPaused {
            reason: broca_protocol::PauseReason::AuthRequired,
            ..
        }
    )));
    let store = Arc::new(MeasuredPrefixStore::from_wal(&dir));
    assert_eq!(store.prefix.completed_steps, 2);
    compare_full_and_sparse_continuation(
        &dir,
        store,
        request(&dir),
        &before.sent()[1],
        true,
        || {
            let mut provider = continuing_provider();
            // The retried step reuses its recorded cap but reports no input usage. The following
            // step still needs the last measured request, not the auth error's attempt length.
            provider.scripts.insert(3, tool_step(3, Usage::default()));
            provider
        },
    )
    .await;
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn a_send_after_an_ended_run_uses_the_same_model_measurement() {
    let dir = scratch("append-measurement");
    let mut before = continuing_provider();
    let mut final_step = WindowProvider::new(0).script(2);
    for event in &mut final_step {
        if let NormalizedEvent::Control(ProviderControl::Finished { usage, .. }) = event {
            *usage = Usage {
                input_tokens: Some(1_105),
                cached_input_tokens: Some(1_005_937),
                ..Default::default()
            };
        }
    }
    before.scripts.insert(2, final_step);
    assert_eq!(
        run_with(
            &dir,
            &before,
            request(&dir),
            None,
            resume_control(None),
            false
        )
        .await,
        RunFinishReason::Completed
    );
    let store = Arc::new(MeasuredPrefixStore::from_wal(&dir));
    let mut req = request(&dir);
    req.run_id = RunId("next-run".into());
    compare_full_and_sparse_continuation(&dir, store, req, &before.sent()[1], false, || {
        let mut provider = WindowProvider::new(0);
        provider.scripts.insert(1, provider.script(2));
        provider
    })
    .await;
    assert_eq!(
        records(&dir)
            .iter()
            .filter(|record| matches!(record, WalRecord::RunStarted { .. }))
            .count(),
        2
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn materialized_resume_without_a_measurement_or_nearby_pressure_keeps_request_bytes() {
    for legacy in [false, true] {
        let dir = scratch(if legacy {
            "legacy-measurement"
        } else {
            "far-resume"
        });
        let mut before = WindowProvider::new(1_000);
        before.scripts.insert(
            2,
            tool_step(
                2,
                Usage {
                    input_tokens: Some(if legacy { LAST_INPUT } else { 2_000 }),
                    ..Default::default()
                },
            ),
        );
        assert_eq!(
            run_with(
                &dir,
                &before,
                resumable_request(&dir),
                None,
                resume_control(Some(CancellationToken::new())),
                false
            )
            .await,
            RunFinishReason::Interrupted
        );
        if legacy {
            rewrite_wal(&dir, records(&dir).len(), |record| {
                !matches!(record, WalRecord::ModelAttemptFinished { .. })
            });
        }
        let store = Arc::new(MeasuredPrefixStore::from_wal(&dir));
        let wal = FileWalStore::new(dir.join("wal"));
        let path = wal.wal_path(&request(&dir).session);
        let original = std::fs::read(&path).unwrap();
        let baseline = WindowProvider::new(0).without_limits();
        assert_eq!(
            run_with(
                &dir,
                &baseline,
                resumable_request(&dir),
                None,
                resume_control(None),
                false
            )
            .await,
            RunFinishReason::Completed
        );
        std::fs::write(&path, original).unwrap();
        let resumed = WindowProvider::new(0);
        assert_eq!(
            run_with(
                &dir,
                &resumed,
                resumable_request(&dir),
                Some(store),
                resume_control(None),
                false
            )
            .await,
            RunFinishReason::Completed
        );
        assert_eq!(resumed.sent().len(), 1);
        assert_eq!(
            resumed.sent()[0].bytes,
            baseline.sent()[0].bytes,
            "legacy={legacy}"
        );
        assert!(resumed.sent()[0].cap().is_none());
        assert!(!records(&dir).iter().any(|record| matches!(
            record,
            WalRecord::OutputReservationClamped {
                step_id: StepId(3),
                ..
            }
        )));
        let _ = std::fs::remove_dir_all(dir);
    }
}

#[tokio::test]
async fn a_checkpoint_without_reservation_history_recovers_it_from_the_complete_log() {
    let dir = scratch("old-reservation-checkpoint");
    let before = continuing_provider();
    assert_eq!(
        run_with(
            &dir,
            &before,
            resumable_request(&dir),
            None,
            resume_control(Some(CancellationToken::new())),
            false
        )
        .await,
        RunFinishReason::Interrupted
    );
    let mut store = MeasuredPrefixStore::from_wal(&dir);
    let mut old = serde_json::to_value(store.admission.checkpoint.take().unwrap()).unwrap();
    old.as_object_mut().unwrap().remove("output_reservation");
    store.admission.checkpoint = Some(serde_json::from_value(old).unwrap());
    let wal = FileWalStore::new(dir.join("wal"));
    let frames = wal.read_records(&request(&dir).session).unwrap();
    let head = frames.last().unwrap().seq;
    let mut unknown = store.admission.checkpoint.clone().unwrap();
    unknown
        .fold_frames(
            &frames
                .iter()
                .filter(|frame| frame.seq > store.prefix.watermark_seq)
                .cloned()
                .collect::<Vec<_>>(),
        )
        .unwrap();
    assert!(
        unknown.output_reservation.is_none(),
        "folding a tail cannot repair unknown prefix history"
    );
    assert!(
        broca_core::AdmissionCheckpointV1::from_frames(&frames[1..])
            .unwrap()
            .output_reservation
            .is_none(),
        "a suffix must not claim complete reservation history"
    );
    let snapshot =
        broca_core::load_prior_wal_snapshot(&wal, &request(&dir).session, head, Some(&store))
            .unwrap();
    assert_eq!(
        snapshot.base_watermark, 0,
        "unknown prefix history requires complete replay"
    );
    assert!(snapshot
        .checkpoint_at_head
        .output_reservation
        .as_ref()
        .unwrap()
        .has_measurement(&request(&dir).model));
    let resumed = continuing_provider();
    assert_eq!(
        run_with(
            &dir,
            &resumed,
            resumable_request(&dir),
            Some(Arc::new(store)),
            resume_control(None),
            false
        )
        .await,
        RunFinishReason::Completed
    );
    assert_next_clamp(
        &records(&dir),
        &before.sent()[1],
        &resumed.sent()[0],
        &resumed,
    );
    let _ = std::fs::remove_dir_all(dir);
}

/// A step with 916,594 measured input tokens on the step before it,
/// against a 1,048,576-token window and a 131,072-token cap. The next step adds a tool result,
/// so the frozen cap would carry the request past the window; it is sent with a cap that fits.
#[tokio::test]
async fn a_request_whose_input_and_default_reservation_cross_the_window_is_clamped() {
    let dir = scratch("clamp");
    let provider = WindowProvider::new(916_594);
    assert_eq!(run(&dir, &provider).await, RunFinishReason::Completed);

    let sent = provider.sent();
    assert_eq!(sent.len(), 2);
    assert_eq!(
        sent[0].cap(),
        None,
        "the first request has nothing measured"
    );
    let estimate = estimate(916_594, &sent[0], &sent[1], &provider);
    assert!(
        estimate + u64::from(CAP) > WINDOW,
        "the frozen cap crosses the window"
    );
    let expected = WINDOW - estimate - margin(WINDOW);
    assert_eq!(sent[1].cap(), Some(expected as u32));
    assert!(sent[1]
        .bytes
        .ends_with(format!("|max_output_tokens={expected}").as_bytes()));

    // The cap is durable before the request: its record precedes step 2's attempt.
    let wal = records(&dir);
    let clamp_at = wal
        .iter()
        .position(|record| {
            *record
                == WalRecord::OutputReservationClamped {
                    step_id: StepId(2),
                    max_output_tokens: expected as u32,
                    estimated_input_tokens: estimate,
                    context_limit: WINDOW,
                }
        })
        .expect("the lowered cap is recorded");
    let attempt_at = wal
        .iter()
        .position(|record| {
            matches!(record, WalRecord::ModelAttemptFinished { step_id, .. } if *step_id == StepId(2))
        })
        .unwrap();
    assert!(clamp_at < attempt_at);

    // A cache warm rebuilds the request from the WAL with the same cap, so its digest matches.
    let recorded = broca_core::cache_warm::last_recorded_request(&wal).unwrap();
    assert_eq!(
        recorded
            .opts
            .step_context
            .as_ref()
            .unwrap()
            .max_output_tokens,
        Some(expected as u32)
    );
    assert_eq!(
        request_digest_hex(&provider.render_request(&recorded.opts)),
        recorded.request_body_sha256
    );
    let _ = std::fs::remove_dir_all(dir);
}

/// When even the floor does not fit, the run ends `context_overflow` and the request is never
/// sent, so the caller compacts instead of retrying into the provider's refusal.
#[tokio::test]
async fn a_request_that_cannot_fit_the_minimum_reservation_ends_context_overflow_unsent() {
    let dir = scratch("overflow");
    let provider = WindowProvider::new(WINDOW - 3_000);
    assert_eq!(run(&dir, &provider).await, RunFinishReason::Error);

    assert_eq!(provider.sent().len(), 1, "step 2 is never sent");
    let wal = records(&dir);
    assert!(!wal.iter().any(|record| matches!(
        record,
        WalRecord::ModelAttemptFinished { step_id, .. } if *step_id == StepId(2)
    )));
    assert!(!wal
        .iter()
        .any(|record| matches!(record, WalRecord::OutputReservationClamped { .. })));
    let Some(WalRecord::RunFinished {
        reason: RunFinishReason::Error,
        error: Some(error),
        ..
    }) = wal.last()
    else {
        panic!("expected an error terminal, got {:?}", wal.last());
    };
    assert_eq!(error.class, ErrorClass::ContextOverflow);
    assert_eq!(
        error.provider_code.as_deref(),
        Some(broca_core::run::OUTPUT_RESERVATION_OVERFLOW_CODE)
    );
    let _ = std::fs::remove_dir_all(dir);
}

/// A request far from the window keeps the frozen cap: nothing is recorded and its bytes equal
/// those of a provider that declares no cap at all.
#[tokio::test]
async fn a_request_far_from_the_window_renders_unchanged() {
    let dir = scratch("far");
    let provider = WindowProvider::new(1_000);
    assert_eq!(run(&dir, &provider).await, RunFinishReason::Completed);
    let baseline_dir = scratch("far-baseline");
    let baseline = WindowProvider::new(1_000).without_limits();
    assert_eq!(
        run(&baseline_dir, &baseline).await,
        RunFinishReason::Completed
    );

    let (sent, expected) = (provider.sent(), baseline.sent());
    assert_eq!(sent.len(), 2);
    assert!(sent.iter().all(|sent| sent.cap().is_none()));
    for (sent, expected) in sent.iter().zip(&expected) {
        assert_eq!(sent.step, expected.step);
        assert_eq!(sent.bytes, expected.bytes);
    }
    assert!(!records(&dir)
        .iter()
        .any(|record| matches!(record, WalRecord::OutputReservationClamped { .. })));
    let _ = std::fs::remove_dir_all(dir);
    let _ = std::fs::remove_dir_all(baseline_dir);
}

/// A crash after the lowered cap was recorded resumes step 2 with the same bytes: from the
/// recomputed estimate, and from the recorded cap alone when the estimate cannot be made (here,
/// with the attempt records it is measured from removed).
#[tokio::test]
async fn a_resume_reproduces_the_clamped_request_bytes() {
    for drop_attempts in [false, true] {
        let dir = scratch(if drop_attempts {
            "resume-recorded"
        } else {
            "resume"
        });
        let provider = WindowProvider::new(916_594);
        assert_eq!(run(&dir, &provider).await, RunFinishReason::Completed);
        let original = provider.sent()[1].clone();
        assert!(original.cap().is_some());

        let clamp_at = records(&dir)
            .iter()
            .position(|record| matches!(record, WalRecord::OutputReservationClamped { .. }))
            .unwrap();
        rewrite_wal(&dir, clamp_at + 1, |record| {
            !(drop_attempts && matches!(record, WalRecord::ModelAttemptFinished { .. }))
        });

        let resumed = WindowProvider::new(916_594);
        assert_eq!(run(&dir, &resumed).await, RunFinishReason::Completed);
        let sent = resumed.sent();
        assert_eq!(sent.len(), 1, "only step 2 is re-sent");
        assert_eq!(sent[0].step, 2);
        assert_eq!(
            sent[0].bytes, original.bytes,
            "drop_attempts={drop_attempts}"
        );
        // The resume reuses the record rather than writing another.
        assert_eq!(
            records(&dir)
                .iter()
                .filter(|record| matches!(record, WalRecord::OutputReservationClamped { .. }))
                .count(),
            1
        );
        let _ = std::fs::remove_dir_all(dir);
    }
}

/// A crash after step 2's lowered cap was recorded, before its request was answered: the resume
/// sends step 2 with the recorded cap, not the frozen one, and a cache warm rebuilds that request
/// with the same cap, so its digest matches the recorded attempt. The provider here declares no
/// cap of its own, so the cap can only come from the record.
#[tokio::test]
async fn a_resume_renders_the_recorded_output_cap() {
    let dir = scratch("resume-reads-record");
    let provider = WindowProvider::new(1_000).without_limits();
    assert_eq!(run(&dir, &provider).await, RunFinishReason::Completed);
    let original = provider.sent()[1].clone();
    assert_eq!(original.step, 2);
    assert_eq!(original.cap(), None, "nothing recorded, nothing lowered");

    let cap = CAP - 7;
    let attempt_at = records(&dir)
        .iter()
        .position(|record| {
            matches!(record, WalRecord::ModelAttemptFinished { step_id, .. } if *step_id == StepId(2))
        })
        .unwrap();
    rewrite_wal_with(
        &dir,
        attempt_at,
        Some(WalRecord::OutputReservationClamped {
            step_id: StepId(2),
            max_output_tokens: cap,
            estimated_input_tokens: WINDOW - u64::from(cap) - 4_096,
            context_limit: WINDOW,
        }),
    );

    let resumed = WindowProvider::new(1_000).without_limits();
    assert_eq!(run(&dir, &resumed).await, RunFinishReason::Completed);
    let sent = resumed.sent();
    assert_eq!(sent.len(), 1, "only step 2 is re-sent");
    assert_eq!(sent[0].step, 2);
    assert_eq!(sent[0].cap(), Some(cap));
    let mut expected = original.opts.clone();
    expected.step_context.as_mut().unwrap().max_output_tokens = Some(cap);
    assert_eq!(sent[0].bytes, resumed.render_request(&expected));
    assert!(sent[0]
        .bytes
        .ends_with(format!("|max_output_tokens={cap}").as_bytes()));

    let wal = records(&dir);
    let recorded = broca_core::cache_warm::last_recorded_request(&wal).unwrap();
    assert_eq!(
        recorded
            .opts
            .step_context
            .as_ref()
            .unwrap()
            .max_output_tokens,
        Some(cap)
    );
    assert_eq!(
        request_digest_hex(&resumed.render_request(&recorded.opts)),
        recorded.request_body_sha256
    );
    let _ = std::fs::remove_dir_all(dir);
}

const FALLBACK_WINDOW: u64 = 1_000_000;
const FALLBACK_CAP: u32 = 100_000;

/// Refuse on the source, then serve a tool step and a final answer on a model with different
/// context and output limits. Both routes report their actual rendered attempt lengths.
struct FallbackWindowProvider {
    inner: WindowProvider,
    route: Mutex<broca_core::refusal_fallback::StepRoute>,
    context_limit: Option<u64>,
    refuse_step: u64,
}

impl FallbackWindowProvider {
    fn new() -> Self {
        Self {
            inner: WindowProvider::new(916_594),
            route: Mutex::new(broca_core::refusal_fallback::StepRoute::Source),
            context_limit: Some(FALLBACK_WINDOW),
            refuse_step: 1,
        }
    }

    fn fallback() -> ModelHandle {
        ModelHandle::new("stub", "fallback")
    }

    fn is_fallback(&self) -> bool {
        *self.route.lock().unwrap() == broca_core::refusal_fallback::StepRoute::Fallback
    }
}

#[async_trait]
impl Provider for FallbackWindowProvider {
    fn refusal_fallback_model(&self) -> Option<ModelHandle> {
        Some(Self::fallback())
    }

    fn refusal_fallback_context_limit(&self) -> Option<u64> {
        self.context_limit
    }

    fn set_step_route(&self, route: broca_core::refusal_fallback::StepRoute) {
        *self.route.lock().unwrap() = route;
    }

    fn output_limits(&self) -> Option<OutputLimits> {
        Some(OutputLimits::new(
            if self.is_fallback() {
                FALLBACK_CAP
            } else {
                CAP
            },
            0,
        ))
    }

    fn render_request(&self, opts: &CallOptions) -> Vec<u8> {
        let max = if self.is_fallback() {
            FALLBACK_CAP
        } else {
            CAP
        };
        let cap = opts
            .step_context
            .as_ref()
            .and_then(|step| step.max_output_tokens)
            .map_or(max, |cap| cap.min(max));
        let mut bytes = canonical_request_bytes(opts);
        bytes.extend_from_slice(format!("|max_output_tokens={cap}").as_bytes());
        bytes
    }

    async fn step(&self, step_id: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        if self.is_fallback() {
            assert_eq!(opts.target_model, Some(Self::fallback()));
        }
        let bytes = self.render_request(opts);
        self.inner.sent.lock().unwrap().push(Sent {
            step: step_id.0,
            opts: opts.clone(),
            bytes: bytes.clone(),
        });
        let mut events = vec![NormalizedEvent::Control(ProviderControl::AttemptFinished {
            attempt: attempt(&bytes),
        })];
        if self.is_fallback() || step_id.0 < self.refuse_step {
            events.extend(self.inner.script(step_id.0));
        } else {
            events.push(NormalizedEvent::Control(
                ProviderControl::AssistantMessage {
                    message: AssistantMessage {
                        message_id: MessageId("refused".into()),
                        content: vec![ContentBlock::Text {
                            text: "refused".into(),
                        }],
                    },
                },
            ));
            events.push(NormalizedEvent::Control(ProviderControl::Finished {
                usage: Usage {
                    input_tokens: Some(10),
                    ..Default::default()
                },
                finish_reason: FinishReason::ContentFilter,
                retries_used: 0,
            }));
        }
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            for event in events {
                let _ = tx.send(event).await;
            }
        });
        rx
    }
}

#[tokio::test]
async fn a_fallback_step_is_clamped_by_its_own_window_and_output_limits() {
    let dir = scratch("fallback-window");
    let provider = FallbackWindowProvider::new();
    assert_eq!(run(&dir, &provider).await, RunFinishReason::Completed);
    let sent = provider.inner.sent();
    assert_eq!(sent.len(), 3, "one source refusal and two fallback calls");
    assert_eq!(
        sent[1].cap(),
        None,
        "a source tokenizer cannot estimate the first fallback"
    );
    let clamp = records(&dir)
        .into_iter()
        .find_map(|record| match record {
            WalRecord::OutputReservationClamped {
                step_id,
                max_output_tokens,
                estimated_input_tokens,
                context_limit,
            } => Some((
                step_id,
                max_output_tokens,
                estimated_input_tokens,
                context_limit,
            )),
            _ => None,
        })
        .expect("the second fallback call needs a clamp");
    assert_eq!(clamp.0, StepId(2));
    assert_eq!(clamp.3, FALLBACK_WINDOW);
    assert!(clamp.2 + u64::from(FALLBACK_CAP) + margin(FALLBACK_WINDOW) > FALLBACK_WINDOW);
    assert!(
        clamp.2 + u64::from(FALLBACK_CAP) + margin(WINDOW) < WINDOW,
        "the source window would wrongly leave the fallback cap unchanged"
    );
    assert_eq!(
        u64::from(clamp.1),
        FALLBACK_WINDOW - clamp.2 - margin(FALLBACK_WINDOW)
    );
    assert_eq!(sent[2].cap(), Some(clamp.1));
    assert!(sent[2]
        .bytes
        .ends_with(format!("|max_output_tokens={}", clamp.1).as_bytes()));
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn a_resume_of_a_fallback_step_reuses_its_recorded_clamp() {
    for drop_attempts in [false, true] {
        let dir = scratch(if drop_attempts {
            "fallback-recorded"
        } else {
            "fallback-reestimate"
        });
        let provider = FallbackWindowProvider::new();
        assert_eq!(run(&dir, &provider).await, RunFinishReason::Completed);
        let original = provider.inner.sent()[2].clone();
        assert!(original.cap().is_some());
        let clamp_at = records(&dir)
            .iter()
            .position(|record| matches!(record, WalRecord::OutputReservationClamped { .. }))
            .unwrap();
        rewrite_wal(&dir, clamp_at + 1, |record| {
            !(drop_attempts && matches!(record, WalRecord::ModelAttemptFinished { .. }))
        });
        let resumed = FallbackWindowProvider::new();
        assert_eq!(run(&dir, &resumed).await, RunFinishReason::Completed);
        let sent = resumed.inner.sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].step, 2);
        assert_eq!(
            sent[0].opts.target_model,
            Some(FallbackWindowProvider::fallback())
        );
        assert_eq!(sent[0].cap(), original.cap());
        assert_eq!(
            sent[0].bytes, original.bytes,
            "drop_attempts={drop_attempts}"
        );
        assert_eq!(
            records(&dir)
                .iter()
                .filter(|record| matches!(record, WalRecord::OutputReservationClamped { .. }))
                .count(),
            1
        );
        let _ = std::fs::remove_dir_all(dir);
    }
}

#[tokio::test]
async fn a_fallback_without_a_known_window_does_not_use_the_source_window() {
    let dir = scratch("fallback-unknown-window");
    let mut provider = FallbackWindowProvider::new();
    provider.context_limit = None;
    provider.inner.first_input = WINDOW - 10_000;
    assert_eq!(run(&dir, &provider).await, RunFinishReason::Completed);
    assert_eq!(provider.inner.sent().len(), 3);
    assert!(provider
        .inner
        .sent()
        .iter()
        .all(|sent| sent.cap().is_none()));
    assert!(!records(&dir)
        .iter()
        .any(|record| matches!(record, WalRecord::OutputReservationClamped { .. })));
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn a_source_clamp_survives_a_refusal_resend_and_resume() {
    let dir = scratch("source-clamp-refusal");
    let mut provider = FallbackWindowProvider::new();
    provider.refuse_step = 2;
    provider.inner.first_input = 970_000;
    assert_eq!(run(&dir, &provider).await, RunFinishReason::Completed);
    let sent = provider.inner.sent();
    assert_eq!(sent.len(), 3);
    let original = sent[2].clone();
    let cap = sent[1].cap().expect("the source step was clamped");
    assert!(cap < FALLBACK_CAP);
    assert_eq!(
        original.cap(),
        Some(cap),
        "a resend never raises a step's recorded cap"
    );
    let refused_at = records(&dir)
        .iter()
        .position(|record| matches!(record, WalRecord::ModelStepRefused { .. }))
        .unwrap();
    rewrite_wal(&dir, refused_at + 1, |_| true);
    let mut resumed = FallbackWindowProvider::new();
    resumed.refuse_step = 2;
    resumed.inner.first_input = 970_000;
    assert_eq!(run(&dir, &resumed).await, RunFinishReason::Completed);
    let resent = resumed.inner.sent();
    assert_eq!(
        resent.len(),
        1,
        "the durable refusal routes straight to the fallback"
    );
    assert_eq!(resent[0].bytes, original.bytes);
    assert_eq!(resent[0].cap(), Some(cap));
    let _ = std::fs::remove_dir_all(dir);
}
