//! A session without compaction sends the request bytes it sent before asynchronous compaction
//! existed. The golden file holds the canonical bytes of every request of a two-episode session
//! (a system prompt, failing tool calls that draw loop guidance, an appended episode), as the
//! build before compaction (`122fe3a7`) rendered them; this build must render the same.
//! The ignored `generate_non_compaction_request_bytes` test writes the golden explicitly;
//! run it only on the reference build, never as part of verification.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broca_core::{
    provider::Provider, run_loop, run_loop_append_episode, AllowAll, FileLeaseStore, FileWalStore,
    RunRequest, StubReply, StubToolPlane,
};
use broca_protocol::{
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

const GOLDEN: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/non_compaction_request_bytes.json"
);

async fn session_request_bytes() -> Vec<String> {
    let dir = broca_test_support::scratch_root().join(format!(
        "non-compaction-request-bytes-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let provider = SequenceProvider::new(vec![
        tool_step("c1", "read", json!({"path": "a"})),
        tool_step("c2", "read", json!({"path": "a"})),
        tool_step("c3", "read", json!({"path": "a"})),
        text_step("a1", "done"),
        tool_step("c4", "read", json!({"path": "b"})),
        text_step("a2", "done again"),
    ]);
    let plane = failing_read_plane();
    drive(
        &dir,
        request("r1", &dir, "first task"),
        &provider,
        &plane,
        false,
    )
    .await;
    drive(
        &dir,
        request("r2", &dir, "second task"),
        &provider,
        &plane,
        true,
    )
    .await;
    let sent: Vec<String> = provider
        .sent()
        .iter()
        .map(|opts| String::from_utf8(broca_core::render::canonical_request_bytes(opts)).unwrap())
        .collect();
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(sent.len(), 6);
    sent
}

#[tokio::test]
#[ignore = "writes the byte oracle; invoke explicitly on the reference build only"]
async fn generate_non_compaction_request_bytes() {
    let sent = session_request_bytes().await;
    std::fs::write(GOLDEN, serde_json::to_string_pretty(&sent).unwrap() + "\n").unwrap();
}

#[tokio::test]
async fn a_session_without_compaction_renders_the_bytes_it_rendered_before_compaction() {
    let sent = session_request_bytes().await;
    let golden: Vec<String> =
        serde_json::from_str(&std::fs::read_to_string(GOLDEN).unwrap()).unwrap();
    assert_eq!(sent.len(), golden.len());
    for (index, (now, before)) in sent.iter().zip(&golden).enumerate() {
        assert_eq!(
            now, before,
            "request {index} changed for a session without compaction"
        );
    }
}
