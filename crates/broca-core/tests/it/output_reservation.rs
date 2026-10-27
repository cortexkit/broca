//! A step's output cap recorded in the WAL (`output_reservation_clamped`) is part of the request
//! the step sent: a resume of the step renders it again, and a cache warm rebuilds the request
//! with it.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broca_core::{
    canonical_request_bytes, provider::Provider, request_digest_hex, run_loop_with_control,
    AllowAll, FileLeaseStore, FileWalStore, RunControl, RunRequest, StubReply, StubToolPlane,
    WalStore,
};
use broca_protocol::{
    caller::RunFinishReason,
    content::{AssistantMessage, ContentBlock, Message, Role, ToolCall, ToolOutput},
    provider::{AttemptOutcome, ModelAttempt, NormalizedEvent, ProviderControl},
    request::{CallOptions, ExecutionMode, ModelHandle, StopCondition, Tool, ToolChoice},
    FinishReason, GenerationConfig, MessageId, RunId, StepId, ToolCallId, Usage,
};
use broca_wal::{FileWal, Wal, WalRecord};
use serde_json::json;
use tokio::sync::mpsc;

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

/// A provider whose rendered bytes carry the output cap the way a wire body does, and that
/// reports each request's body length in an attempt record.
#[derive(Clone)]
struct WindowProvider {
    /// Step 1's reported input tokens; step 2 reports a small count.
    first_input: u64,
    sent: Arc<Mutex<Vec<Sent>>>,
}

impl WindowProvider {
    fn new(first_input: u64) -> Self {
        Self {
            first_input,
            sent: Arc::default(),
        }
    }

    fn sent(&self) -> Vec<Sent> {
        self.sent.lock().unwrap().clone()
    }

    fn script(&self, step: u64) -> Vec<NormalizedEvent> {
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

async fn run(dir: &std::path::Path, provider: &WindowProvider) -> RunFinishReason {
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let (tx, mut rx) = mpsc::channel(512);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let summary = run_loop_with_control(
        request(dir),
        provider,
        &plane(),
        &lease,
        &AllowAll,
        &wal,
        None,
        RunControl {
            admitted_context_limit: Some(WINDOW),
            ..Default::default()
        },
        &tx,
    )
    .await
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

/// A crash after step 2's lowered cap was recorded, before its request was answered: the resume
/// sends step 2 with the recorded cap, not the frozen one, and a cache warm rebuilds that request
/// with the same cap, so its digest matches the recorded attempt. The provider here declares no
/// cap of its own, so the cap can only come from the record.
#[tokio::test]
async fn a_resume_renders_the_recorded_output_cap() {
    let dir = scratch("resume-reads-record");
    let provider = WindowProvider::new(1_000);
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

    let resumed = WindowProvider::new(1_000);
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
