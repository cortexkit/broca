//! A run whose send names no stop condition has no step limit: it goes on until the model
//! stops calling tools, however many steps that takes.

use std::collections::VecDeque;
use std::sync::Mutex;

use async_trait::async_trait;
use broca_core::stub_plane::StubToolPlane;
use broca_protocol::content::{AssistantMessage, ContentBlock, FinishReason, ToolCall};
use broca_protocol::request::CallOptions;
use broca_protocol::{
    BindIdentity, MessageId, NormalizedEvent, ProviderControl, RunFinishReason, RunId, StepId,
    SubmissionRequest, ToolCallId, Usage,
};
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::{Cipher, DeploymentKey};
use broca_session_store::session::Session;
use tokio::sync::mpsc;

use crate::parts::MemoryParts;
use crate::run::Seams;
use crate::writer::Writer;

/// More steps than any built-in limit broca ever had (100).
const TOOL_STEPS: usize = 150;

fn finished(message: AssistantMessage, finish_reason: FinishReason) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage { message }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(10),
                output_tokens: Some(1),
                ..Usage::default()
            },
            finish_reason,
            retries_used: 0,
        }),
    ]
}

fn read_step(n: usize) -> Vec<NormalizedEvent> {
    finished(
        AssistantMessage {
            message_id: MessageId(format!("m-{n}")),
            content: vec![ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId(format!("c{n}")),
                tool_name: "read".into(),
                input: serde_json::json!({"path": format!("src/{n}.rs")}).into(),
            })],
        },
        FinishReason::ToolCalls,
    )
}

/// Answers each model call with the next scripted step and counts the calls.
struct Scripted {
    steps: Mutex<VecDeque<Vec<NormalizedEvent>>>,
    calls: Mutex<usize>,
}

#[async_trait]
impl broca_core::provider::Provider for Scripted {
    async fn step(&self, _step_id: StepId, _opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        *self.calls.lock().unwrap() += 1;
        let step = self.steps.lock().unwrap().pop_front().unwrap_or_default();
        let (tx, rx) = mpsc::channel(8);
        for event in step {
            tx.try_send(event).unwrap();
        }
        rx
    }
}

#[tokio::test]
async fn a_run_without_a_stop_condition_has_no_step_limit() {
    let backend = MemoryBackend::default();
    let triple = SessionTriple {
        project_root: "/synthetic".into(),
        harness: "limit".into(),
        session: "s1".into(),
    };
    let bind = BindIdentity::new("/synthetic", "limit", "s1");
    let deployment = DeploymentKey::new(1, &[7; 32]).creating(Cipher::None);
    let mut session = Session::create(
        &backend,
        SessionAddr("5e55105e55105e55".into()),
        triple,
        &deployment,
        false,
    )
    .await
    .unwrap();
    let mut steps: VecDeque<_> = (0..TOOL_STEPS).map(read_step).collect();
    steps.push_back(finished(
        AssistantMessage {
            message_id: MessageId("m-answer".into()),
            content: vec![ContentBlock::Text {
                text: "Done.".into(),
            }],
        },
        FinishReason::Stop,
    ));
    let provider = Scripted {
        steps: Mutex::new(steps),
        calls: Mutex::new(0),
    };
    let request: SubmissionRequest = serde_json::from_value(serde_json::json!({
        "model": {"provider_module_id": "stub", "model_id": "m"},
        "input": [{"role": "user", "content": [{"type": "text", "text": "read every file"}]}],
    }))
    .unwrap();
    let mut request = broca_core::RunRequest::from_submission(
        request,
        RunId("unlimited".into()),
        bind.clone(),
        broca_protocol::SubmissionId(String::new()),
    );
    let control = broca_core::RunControl {
        cancel: Some(Default::default()),
        ..Default::default()
    };
    let now = || 1_800_000_000_000;
    let admission = crate::admit::admission(
        &mut request,
        &control,
        &provider,
        Writer::operations(&session),
        Some(now()),
    )
    .unwrap();
    assert!(
        admission.stop_when.is_empty(),
        "the send names no stop condition"
    );
    let tools = StubToolPlane::new().with_canned_text("read", "fn main() {}");
    let seams = Seams {
        person_steers: None,
        provider: &provider,
        tools: &tools,
        bind,
        out: None,
        cancel: Default::default(),
        stop: Default::default(),
        held: None,
        approval: &broca_core::capability::AllowAll,
        hook_plane: None,
        now_ms: &now,
        session_address: None,
        compaction_plane: None,
        compaction_session: None,
        compaction_changes: false,
        keep_warm: None,
        account_reports: None,
        max_same_account_wait: Default::default(),
        auth_selection: None,
        step_progress: None,
        model_call_ready: None,
        pause_for_auth_at_start: None,
        account_continue: None,
    };
    let finished = crate::run::run(&mut session, &mut MemoryParts::default(), admission, &seams)
        .await
        .unwrap();
    assert_eq!(finished.reason, RunFinishReason::Completed);
    assert_eq!(
        *provider.calls.lock().unwrap(),
        TOOL_STEPS + 1,
        "every tool step and the answer were asked for"
    );
}
