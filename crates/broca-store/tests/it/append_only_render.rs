//! The store-path counterpart of `broca-core/tests/it/append_only_render.rs`: when the prior
//! conversation is read from a materialized store prefix instead of a full WAL replay, the next
//! episode's first request must still render every earlier message exactly as it was sent.
//!
//! The store prefix does not record where episodes began, so an earlier episode's repetition
//! guidance cannot be reproduced from it alone. Admission must notice that and fall back to a
//! full replay; this test fails if it silently renders the materialized results raw instead.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broca_core::{
    provider::Provider, run_loop_append_episode, run_loop_with_store, AllowAll, FileLeaseStore,
    FileWalStore, MaterializedStore, Persister, RunRequest, StubReply, StubToolPlane,
};
use broca_protocol::{
    canonical_json_bytes,
    content::{AssistantMessage, ContentBlock, Message, Role, ToolCall},
    provider::{NormalizedEvent, ProviderControl},
    request::{CallOptions, ExecutionMode, ModelHandle, StopCondition, Tool, ToolChoice},
    FinishReason, GenerationConfig, MessageId, RunId, StepId, ToolCallId, Usage,
};
use broca_store::SqliteStore;
use broca_wal::{FileWal, Wal};
use serde_json::{json, Value};
use tokio::sync::mpsc;

#[derive(Clone)]
struct SequenceProvider {
    scripts: Arc<Mutex<VecDeque<Vec<NormalizedEvent>>>>,
    sent: Arc<Mutex<Vec<CallOptions>>>,
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

fn step(id: &str, block: ContentBlock, finish_reason: FinishReason) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(id.into()),
                content: vec![block],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason,
            retries_used: 0,
        }),
    ]
}

fn read_step(id: &str, input: Value) -> Vec<NormalizedEvent> {
    step(
        id,
        ContentBlock::ToolCall(ToolCall {
            thought_signature: None,
            tool_call_id: ToolCallId(id.into()),
            tool_name: "read".into(),
            input,
        }),
        FinishReason::ToolCalls,
    )
}

fn text_step(id: &str, text: &str) -> Vec<NormalizedEvent> {
    step(
        id,
        ContentBlock::Text { text: text.into() },
        FinishReason::Stop,
    )
}

fn request(run: &str, dir: &std::path::Path, text: &str) -> RunRequest {
    RunRequest {
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
        run_id: RunId(run.into()),
        session: broca_protocol::BindIdentity::new(dir, "append-only-store", "s1"),
        model: ModelHandle::new("stub", "m"),
        input: vec![Message::new(
            Role::User,
            vec![ContentBlock::Text { text: text.into() }],
        )],
        tools: vec![Tool {
            name: "read".into(),
            description: "read a file".into(),
            input_schema: json!({"type": "object"}),
            module: None,
            execution_mode: ExecutionMode::Sequential,
            reply: None,
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
        structured_output: None,
    }
}

fn message_bytes(message: &Message) -> Vec<u8> {
    let mut message = message.clone();
    message.cache_prefix_blocks = None;
    canonical_json_bytes(&serde_json::to_value(&message).unwrap())
}

#[tokio::test]
async fn store_prefix_resume_keeps_earlier_episode_guidance() {
    let dir = broca_test_support::scratch_root()
        .join(format!("broca-append-only-store-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let provider = SequenceProvider {
        scripts: Arc::new(Mutex::new(VecDeque::from(vec![
            read_step("r1", json!({"filePath": "missing.rs"})),
            read_step("r2", json!({"filePath": "missing.rs"})),
            text_step("done-1", "trying something else"),
            text_step("done-2", "second answer"),
        ]))),
        sent: Arc::new(Mutex::new(Vec::new())),
    };
    let plane =
        StubToolPlane::new().with_tool("read", |_| StubReply::ExecError("file not found".into()));
    let store = Arc::new(SqliteStore::open(dir.join("store.db")).unwrap());
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));

    let (tx, mut rx) = mpsc::channel(512);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    run_loop_with_store(
        request("episode-1", &dir, "read missing.rs"),
        &provider,
        &plane,
        &lease,
        &AllowAll,
        &wal,
        Some(store.clone() as Arc<dyn broca_core::SessionStore>),
        &tx,
    )
    .await
    .expect("first episode runs");
    drop(tx);
    collector.await.unwrap();

    // Materialize the whole first episode so the next admission reads the store prefix.
    let session = request("episode-1", &dir, "").session;
    let frames = FileWal::open(wal.wal_path(&session))
        .unwrap()
        .replay()
        .unwrap();
    Persister::project(store.as_ref(), &session, &frames).expect("project episode 1");
    let prefix = store.read_prefix_with_watermark(&session).unwrap();
    assert!(
        prefix.prefix.len() >= 5,
        "the first episode must be materialized, or the store path is not exercised"
    );

    let (tx, mut rx) = mpsc::channel(512);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    run_loop_append_episode(
        request("episode-2", &dir, "and now?"),
        &provider,
        &plane,
        &lease,
        &AllowAll,
        &wal,
        Some(store.clone() as Arc<dyn broca_core::SessionStore>),
        &tx,
    )
    .await
    .expect("second episode runs");
    drop(tx);
    collector.await.unwrap();

    let sent = provider.sent.lock().unwrap().clone();
    assert_eq!(sent.len(), 4);
    assert!(
        String::from_utf8_lossy(&canonical_json_bytes(
            &serde_json::to_value(&sent[2].prompt).unwrap()
        ))
        .contains("read has now failed 2 times with identical arguments"),
        "episode 1 must trip the repetition guard"
    );
    for (index, pair) in sent.windows(2).enumerate() {
        let (prev, next) = (&pair[0], &pair[1]);
        assert!(next.prompt.len() >= prev.prompt.len());
        for (position, (before, after)) in prev.prompt.iter().zip(&next.prompt).enumerate() {
            assert!(
                message_bytes(before) == message_bytes(after),
                "request {index} -> {}: message {position} was edited\n before: {}\n  after: {}",
                index + 1,
                String::from_utf8_lossy(&message_bytes(before)),
                String::from_utf8_lossy(&message_bytes(after)),
            );
        }
    }

    let _ = std::fs::remove_dir_all(dir);
}
