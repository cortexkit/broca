//! A call's result is written and synced as soon as the call answers, while the batch's other
//! calls still run: a `say` that answers at once is readable beside a `bash` that runs on, and
//! survives a power loss while `bash` runs. The next request still lists the batch's results in
//! the order the calls were planned.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use broca_core::stub_plane::StubToolPlane;
use broca_core::tool_plane::{BatchCtx, DispatchResult, ToolPlane, ToolPlaneError};
use broca_core::ToolManifest;
use broca_protocol::content::{AssistantMessage, ContentBlock, FinishReason, Role, ToolCall};
use broca_protocol::request::CallOptions;
use broca_protocol::{
    BindIdentity, MessageId, NormalizedEvent, ProviderControl, RunId, StepId, SubmissionRequest,
    ToolCallId, Usage,
};
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::{Cipher, DeploymentKey};
use broca_session_store::record::Record;
use broca_session_store::session::Session;
use tokio::sync::mpsc;

use crate::parts::MemoryParts;
use crate::run::Seams;
use crate::writer::Writer;

const ADDR: &str = "e417e417e417e417";

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

fn call(id: &str, tool: &str) -> ContentBlock {
    ContentBlock::ToolCall(ToolCall {
        thought_signature: None,
        provider_state: None,
        tool_call_id: ToolCallId(id.into()),
        tool_name: tool.into(),
        input: serde_json::json!({}).into(),
    })
}

/// A step that calls `bash` and then `say`, and a final answer.
fn steps() -> VecDeque<Vec<NormalizedEvent>> {
    VecDeque::from([
        finished(
            AssistantMessage {
                message_id: MessageId("m-calls".into()),
                content: vec![call("bash-1", "bash"), call("say-1", "say")],
            },
            FinishReason::ToolCalls,
        ),
        finished(
            AssistantMessage {
                message_id: MessageId("m-answer".into()),
                content: vec![ContentBlock::Text {
                    text: "Done.".into(),
                }],
            },
            FinishReason::Stop,
        ),
    ])
}

struct Scripted {
    steps: Mutex<VecDeque<Vec<NormalizedEvent>>>,
}

#[async_trait]
impl broca_core::provider::Provider for Scripted {
    async fn step(&self, _step_id: StepId, _opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        let step = self.steps.lock().unwrap().pop_front().unwrap_or_default();
        let (tx, rx) = mpsc::channel(8);
        for event in step {
            tx.try_send(event).unwrap();
        }
        rx
    }
}

/// Answers `say` at once. `bash` answers after a pause, first stopping every later write to the
/// backend when `crash` is set, as a process that dies while `bash` runs does.
struct Tools {
    inner: StubToolPlane,
    backend: &'static MemoryBackend,
    crash: bool,
}

#[async_trait]
impl ToolPlane for Tools {
    async fn batch_started(
        &self,
        ctx: &BatchCtx,
        calls: &[ToolCall],
        manifest: &ToolManifest,
    ) -> Result<(), ToolPlaneError> {
        self.inner.batch_started(ctx, calls, manifest).await
    }

    async fn dispatch(&self, ctx: &BatchCtx, call: &ToolCall) -> DispatchResult {
        if call.tool_name == "bash" {
            tokio::time::sleep(Duration::from_millis(300)).await;
            if self.crash {
                self.backend.crash_after(Some(0));
            }
        }
        self.inner.dispatch(ctx, call).await
    }

    async fn batch_finished(&self, ctx: &BatchCtx) -> Result<(), ToolPlaneError> {
        self.inner.batch_finished(ctx).await
    }
}

fn triple() -> SessionTriple {
    SessionTriple {
        project_root: "/synthetic".into(),
        harness: "early".into(),
        session: "s1".into(),
    }
}

fn deployment() -> broca_session_store::keys::DeploymentKey {
    DeploymentKey::new(1, &[7; 32])
}

/// Runs the two-call step to its end, or to the crash, and returns the backend.
async fn run_batch(crash: bool) -> &'static MemoryBackend {
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let bind = BindIdentity::new("/synthetic", "early", "s1");
    let mut session = Session::create(
        backend,
        SessionAddr(ADDR.into()),
        triple(),
        &deployment().creating(Cipher::None),
        false,
    )
    .await
    .unwrap();
    let provider = Scripted {
        steps: Mutex::new(steps()),
    };
    let request: SubmissionRequest = serde_json::from_value(serde_json::json!({
        "model": {"provider_module_id": "stub", "model_id": "m"},
        "input": [{"role": "user", "content": [{"type": "text", "text": "run it and tell me"}]}],
        "tools": [
            {"name": "bash", "description": "runs a command", "input_schema": {"type": "object"}},
            {"name": "say", "description": "tells the person", "input_schema": {"type": "object"}},
        ],
    }))
    .unwrap();
    let mut request = broca_core::RunRequest::from_submission(
        request,
        RunId("early".into()),
        bind.clone(),
        broca_protocol::SubmissionId(String::new()),
    );
    request.policy_allowed = ["bash".to_owned(), "say".to_owned()].into();
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
    let tools = Tools {
        inner: StubToolPlane::new()
            .with_canned_text("bash", "built")
            .with_canned_text("say", "sent"),
        backend,
        crash,
    };
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
    let ended = crate::run::run(&mut session, &mut MemoryParts::default(), admission, &seams).await;
    assert_eq!(ended.is_err(), crash, "{ended:?}");
    backend
}

async fn reload(backend: &'static MemoryBackend) -> Session<&'static MemoryBackend> {
    backend.crash_after(None);
    Session::load(backend, SessionAddr(ADDR.into()), &triple(), &deployment())
        .await
        .unwrap()
}

/// The ids of the calls whose results the session holds, in the order they were written.
async fn answered(session: &Session<&'static MemoryBackend>) -> Vec<String> {
    session
        .open_log_records()
        .await
        .unwrap()
        .into_iter()
        .filter_map(|(_, record)| match record {
            Record::ToolOutcome(outcome) => Some(outcome.tool_call_id.0),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn a_calls_result_is_written_while_the_batchs_other_call_still_runs() {
    // Nothing is written after `bash` answers: whatever the session holds was written before.
    let backend = run_batch(true).await;
    let session = reload(backend).await;
    assert_eq!(answered(&session).await, ["say-1"]);
}

#[tokio::test]
async fn a_result_a_reader_could_see_survives_a_power_loss_before_the_batch_ends() {
    // The power loss keeps only what was synced: the `say` result a reader could already see
    // must be part of it, or resume would find `say` without an outcome and stop the run.
    let backend = run_batch(true).await;
    let lost: &'static MemoryBackend = Box::leak(Box::new(backend.power_loss()));
    let session = reload(lost).await;
    assert_eq!(answered(&session).await, ["say-1"]);
}

#[tokio::test]
async fn the_next_request_lists_the_results_in_the_calls_order() {
    let backend = run_batch(false).await;
    let session = reload(backend).await;
    assert_eq!(
        answered(&session).await,
        ["say-1", "bash-1"],
        "written as they answered"
    );
    let results: Vec<String> = Writer::conversation(&session)
        .held_messages_of(Role::Tool)
        .unwrap()
        .iter()
        .flat_map(|message| message.content.iter())
        .filter_map(|block| match block {
            ContentBlock::ToolResult(result) => Some(result.tool_call_id.0.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(results, ["bash-1", "say-1"], "listed in the calls' order");
}
