//! The rendered conversation must be APPEND-ONLY across every request of a session, including
//! across episodes: each request's system, tools and shared messages byte-identical to the
//! previous request's. Providers bind signed thinking to everything sent before it and reject a
//! request whose earlier content changed; every provider's prompt cache misses on the same edits.
//!
//! These tests drive real `run_loop` / `run_loop_append_episode` calls against a file WAL and
//! record every request in the order it was sent, so an addition the run loop makes to one
//! request and forgets on a later one shows up as a changed earlier message.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broca_core::{
    provider::Provider, run_loop, run_loop_append_episode, AllowAll, FileLeaseStore, FileWalStore,
    RunRequest, StubReply, StubToolPlane,
};
use broca_protocol::{
    canonical_json_bytes,
    content::{AssistantMessage, ContentBlock, Message, Role, ToolCall},
    provider::{NormalizedEvent, ProviderControl},
    request::{CallOptions, ExecutionMode, ModelHandle, StopCondition, Tool, ToolChoice},
    FinishReason, GenerationConfig, MessageId, RunId, StepId, ToolCallId, Usage,
};
use serde_json::{json, Value};
use tokio::sync::mpsc;

/// Serves scripted steps in call order across every episode and records each request it was
/// asked to send.
#[derive(Clone)]
struct SequenceProvider {
    scripts: Arc<Mutex<std::collections::VecDeque<Vec<NormalizedEvent>>>>,
    sent: Arc<Mutex<Vec<CallOptions>>>,
}

impl SequenceProvider {
    fn new(scripts: Vec<Vec<NormalizedEvent>>) -> Self {
        Self {
            scripts: Arc::new(Mutex::new(scripts.into())),
            sent: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn sent(&self) -> Vec<CallOptions> {
        self.sent.lock().unwrap().clone()
    }
}

#[async_trait]
impl Provider for SequenceProvider {
    async fn step(&self, _step_id: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        self.sent.lock().unwrap().push(opts.clone());
        let script = self.scripts.lock().unwrap().pop_front().unwrap_or_default();
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            for event in script {
                let _ = tx.send(event).await;
            }
        });
        rx
    }
}

fn tool_step(id: &str, name: &str, input: Value) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(id.into()),
                content: vec![ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId(id.into()),
                    tool_name: name.into(),
                    input,
                })],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: FinishReason::ToolCalls,
            retries_used: 0,
        }),
    ]
}

fn text_step(id: &str, text: &str) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(id.into()),
                content: vec![ContentBlock::Text { text: text.into() }],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: FinishReason::Stop,
            retries_used: 0,
        }),
    ]
}

fn request(run: &str, dir: &std::path::Path, text: &str) -> RunRequest {
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
        session: broca_protocol::BindIdentity::new(dir, "append-only", "s1"),
        model: ModelHandle::new("stub", "m"),
        input: vec![
            Message::new(
                Role::System,
                vec![ContentBlock::Text {
                    text: "You are a careful agent.".into(),
                }],
            ),
            Message::new(Role::User, vec![ContentBlock::Text { text: text.into() }]),
        ],
        tools: vec![Tool {
            name: "read".into(),
            description: "read a file".into(),
            input_schema: json!({"type": "object"}),
            module: None,
            execution_mode: ExecutionMode::Sequential,
        }],
        policy_allowed: ["read".to_string()].into_iter().collect(),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        stop_when: vec![StopCondition::StepCount { n: 8 }],
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

fn failing_read_plane() -> StubToolPlane {
    StubToolPlane::new().with_tool("read", |_| StubReply::ExecError("file not found".into()))
}

async fn drive(
    dir: &std::path::Path,
    req: RunRequest,
    provider: &SequenceProvider,
    plane: &StubToolPlane,
    append: bool,
) {
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let (tx, mut rx) = mpsc::channel(512);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    if append {
        run_loop_append_episode(req, provider, plane, &lease, &AllowAll, &wal, None, &tx)
            .await
            .expect("appended episode runs");
    } else {
        run_loop(req, provider, plane, &lease, &AllowAll, &wal, &tx)
            .await
            .expect("first episode runs");
    }
    drop(tx);
    collector.await.unwrap();
}

/// Canonical bytes of one message with its cache marker removed: moving a cache breakpoint is a
/// valid edit, so it must not count as a change.
fn message_bytes(message: &Message) -> Vec<u8> {
    let mut message = message.clone();
    message.cache_prefix_blocks = None;
    canonical_json_bytes(&serde_json::to_value(&message).unwrap())
}

/// Assert that `next` extends `prev` without editing anything `prev` sent: identical tools, and
/// every message of `prev` byte-identical at the same position of `next`.
fn assert_append_only(prev: &CallOptions, next: &CallOptions, label: &str) {
    assert_eq!(
        canonical_json_bytes(&serde_json::to_value(&*prev.tools).unwrap()),
        canonical_json_bytes(&serde_json::to_value(&*next.tools).unwrap()),
        "{label}: tools changed between requests"
    );
    assert!(
        next.prompt.len() >= prev.prompt.len(),
        "{label}: the later request dropped messages ({} -> {})",
        prev.prompt.len(),
        next.prompt.len()
    );
    for (index, (before, after)) in prev.prompt.iter().zip(&next.prompt).enumerate() {
        let before_bytes = message_bytes(before);
        let after_bytes = message_bytes(after);
        assert!(
            before_bytes == after_bytes,
            "{label}: message {index} ({:?}) was edited between requests\n before: {}\n  after: {}",
            before.role,
            String::from_utf8_lossy(&before_bytes),
            String::from_utf8_lossy(&after_bytes),
        );
    }
}

fn scratch(name: &str) -> std::path::PathBuf {
    let dir = broca_test_support::scratch_root().join(format!("{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Episode 1 trips the repetition guard (the same read fails twice), so its second tool result is
/// SENT with guidance appended. The first request of episode 2 must render that result exactly as
/// it was sent; the WAL keeps only the tool's own output, so the guidance has to be reproduced for
/// the earlier episode too, not only for the episode being resumed.
#[tokio::test]
async fn repetition_guidance_survives_into_the_next_episode() {
    let dir = scratch("broca-append-only-repetition");
    let provider = SequenceProvider::new(vec![
        tool_step("r1", "read", json!({"filePath": "missing.rs"})),
        tool_step("r2", "read", json!({"filePath": "missing.rs"})),
        text_step("done-1", "trying something else"),
        text_step("done-2", "second answer"),
    ]);
    let plane = failing_read_plane();

    drive(
        &dir,
        request("episode-1", &dir, "read missing.rs"),
        &provider,
        &plane,
        false,
    )
    .await;
    drive(
        &dir,
        request("episode-2", &dir, "and now?"),
        &provider,
        &plane,
        true,
    )
    .await;

    let sent = provider.sent();
    assert_eq!(
        sent.len(),
        4,
        "three requests in episode 1, one in episode 2"
    );
    let last_of_first = &sent[2];
    assert!(
        String::from_utf8_lossy(&canonical_json_bytes(
            &serde_json::to_value(&last_of_first.prompt).unwrap()
        ))
        .contains("read has now failed 2 times with identical arguments"),
        "episode 1 must actually trip the guard, or this test proves nothing"
    );
    for (index, pair) in sent.windows(2).enumerate() {
        assert_append_only(
            &pair[0],
            &pair[1],
            &format!("request {index} -> {}", index + 1),
        );
    }

    let _ = std::fs::remove_dir_all(dir);
}

/// A provider that reports, on its attempt evidence, that it dropped a thinking block.
struct TransformingProvider;

#[async_trait]
impl Provider for TransformingProvider {
    async fn step(&self, _step_id: StepId, _opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        let (tx, rx) = mpsc::channel(8);
        tokio::spawn(async move {
            let attempt = broca_protocol::ModelAttempt {
                content_progress: Default::default(),
                attempt: 0,
                request_body_len: 1,
                request_body_sha256: "aa".into(),
                http_status: Some(200),
                content_type: None,
                response_body_len: 1,
                response_body_sha256: "bb".into(),
                response_body_cap: None,
                outcome: broca_protocol::AttemptOutcome::Complete,
                ts_ms: 0,
                sent_at_ms: None,
                transport: None,
                websocket_request: None,
                websocket_close_code: None,
                input_transformations: Some(vec![broca_protocol::InputTransformation {
                    kind: Some("thinking_dropped".into()),
                    path: Some("messages.1.content.0".into()),
                    reason: Some("prefix_binding_mismatch".into()),
                }]),
                credential_refresh: None,
                served_account_ref: None,
                account_observation: Default::default(),
            };
            let _ = tx
                .send(NormalizedEvent::Control(ProviderControl::AttemptFinished {
                    attempt,
                }))
                .await;
            for event in text_step("t1", "ok") {
                let _ = tx.send(event).await;
            }
        });
        rx
    }
}

/// What a provider reports it changed in the request reaches the step's durable attempt record
/// and the process-wide counter, so a dropped thinking block is visible after the fact.
#[tokio::test]
async fn reported_input_transformations_are_recorded_and_counted() {
    let dir = scratch("broca-input-transformations");
    let before = broca_core::input_transformations_reported();
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let (tx, mut rx) = mpsc::channel(512);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let req = request("transform", &dir, "hi");
    let session = req.session.clone();
    run_loop(
        req,
        &TransformingProvider,
        &failing_read_plane(),
        &lease,
        &AllowAll,
        &wal,
        &tx,
    )
    .await
    .expect("run ok");
    drop(tx);
    collector.await.unwrap();

    assert!(broca_core::input_transformations_reported() > before);
    use broca_wal::Wal as _;
    let records = broca_wal::FileWal::open(wal.wal_path(&session))
        .unwrap()
        .replay()
        .unwrap();
    let recorded = records
        .iter()
        .find_map(|frame| match &frame.record {
            broca_wal::WalRecord::ModelAttemptFinished {
                input_transformations,
                ..
            } => input_transformations.clone(),
            _ => None,
        })
        .expect("the attempt record carries the report");
    assert_eq!(recorded.len(), 1);
    assert_eq!(
        recorded[0].reason.as_deref(),
        Some("prefix_binding_mismatch")
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn new_session_freezes_tail_first_order_on_every_episode() {
    use broca_core::WalStore;
    let dir =
        broca_test_support::scratch_root().join(format!("marker-admit-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let provider = SequenceProvider::new(vec![text_step("m1", "first"), text_step("m2", "second")]);
    let plane = StubToolPlane::new();
    let first = request("r1", &dir, "first");
    let session = first.session.clone();
    drive(&dir, first, &provider, &plane, false).await;
    drive(&dir, request("r2", &dir, "second"), &provider, &plane, true).await;
    let sent = provider.sent();
    assert_eq!(sent.len(), 2);
    assert!(sent.iter().all(|opts| opts.marker_order_version == Some(1)));
    let records = FileWalStore::new(dir.join("wal"))
        .read_records(&session)
        .unwrap();
    let versions: Vec<_> = records
        .iter()
        .filter_map(|f| match &f.record {
            broca_wal::WalRecord::RunStarted {
                marker_order_version,
                ..
            } => Some(*marker_order_version),
            _ => None,
        })
        .collect();
    assert_eq!(versions, vec![Some(1), Some(1)]);
}
