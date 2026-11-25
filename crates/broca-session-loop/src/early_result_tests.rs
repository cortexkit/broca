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
        steer_yields: None,
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

/// Each call's outcome in the order they were written, as `id:Outcome`.
async fn outcomes(session: &Session<&'static MemoryBackend>) -> Vec<String> {
    session
        .open_log_records()
        .await
        .unwrap()
        .into_iter()
        .filter_map(|(_, record)| match record {
            Record::ToolOutcome(outcome) => {
                Some(format!("{}:{:?}", outcome.tool_call_id.0, outcome.outcome))
            }
            _ => None,
        })
        .collect()
}

/// A person's steer the run hasn't placed yet: the run's end places it.
fn steer() -> Record {
    Record::Steer(broca_session_store::record::Steer {
        named_speaker: None,
        kept_notice: None,
        media: Vec::new(),
        input: vec![broca_protocol::content::Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "also check the logs".into(),
            }],
        )],
        written_at_ms: Some(1_800_000_000_001),
        tags_v1: None,
        receipt: None,
        message_marks: None,
    })
}

/// The error the session's owner ends a failed run with.
fn loop_failed() -> broca_protocol::ProviderError {
    broca_protocol::ProviderError {
        class: broca_protocol::ErrorClass::Permanent,
        message: "storing the call's media failed".into(),
        provider_code: Some("runner_error".into()),
        provider_details: None,
        cause: None,
        provider_subcode: None,
        refusal: None,
        status: None,
        retry_after_secs: None,
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
        account_continue: None,
    }
}

/// A run whose loop failed with its tool batch open, `say` answered and `bash` sent with no
/// result, as when storing a call's media fails while the log stays writable, and a person
/// steered in meanwhile. The owner's end closes the batch before it places the steer and ends
/// the run: `bash` gets an unknown outcome, the tool turn closes, and the session takes the next
/// append.
#[tokio::test]
async fn ending_a_failed_run_closes_its_open_batch_before_placing_a_steer() {
    let backend = run_batch(true).await;
    let mut session = reload(backend).await;
    session.append(&[steer()], true).await.unwrap();
    let ended = crate::run::end_run_with_error(
        &mut session,
        "early",
        broca_protocol::RunFinishReason::Error,
        &loop_failed(),
        1_800_000_000_002,
        None,
    )
    .await;
    assert!(ended.is_ok(), "end_run_with_error: {ended:?}");
    assert!(
        Writer::conversation(&session).open_tool_turn().is_none(),
        "the tool turn closes with the run"
    );
    assert_eq!(
        outcomes(&session).await,
        ["say-1:Completed", "bash-1:Unknown"]
    );
    let later = session.append(&[steer()], true).await;
    assert!(later.is_ok(), "a later append: {later:?}");
}

/// The held calls' lifecycle, logging each withdrawal.
#[derive(Default)]
struct Withdrawals(Mutex<Vec<String>>);

#[async_trait]
impl broca_core::tool_plane::ProviderHeldLifecycle for Withdrawals {
    async fn wait(
        &self,
        _ctx: &BatchCtx,
        _call: &ToolCall,
        _reply: &broca_core::tool_plane::ProviderHeldReply,
    ) -> DispatchResult {
        std::future::pending().await
    }

    async fn withdraw_held(
        &self,
        call: &broca_core::tool_plane::ProviderHeldReply,
    ) -> Result<(), String> {
        self.0.lock().unwrap().push(call.tool_call_id.0.clone());
        Ok(())
    }
}

/// A failed run's end with a call its provider still holds: the call gets the held close
/// result, and its provider is asked to withdraw it, since nobody will collect its late result.
#[tokio::test]
async fn ending_a_failed_run_withdraws_a_call_its_provider_holds() {
    let backend = run_batch(true).await;
    let mut session = reload(backend).await;
    let batch = Writer::operations(&session)
        .run
        .as_ref()
        .and_then(|run| run.open_batch.clone())
        .expect("the batch is open");
    let position = (0..batch.calls.len())
        .find(|position| batch.id(*position).0 == "bash-1")
        .expect("bash is planned");
    session
        .append(
            &[Record::ToolHeld(broca_session_store::record::ToolHeld {
                batch_id: batch.batch_id.clone(),
                tool_call_id: ToolCallId("bash-1".into()),
                call_key: batch.key(position).expect("bash was sent").to_owned(),
                module: "provider".into(),
                reason: "running".into(),
                detail_digest: None,
                held_at_ms: 1_800_000_000_001,
                sequential: false,
            })],
            true,
        )
        .await
        .unwrap();
    let withdrawals = Withdrawals::default();
    let ended = crate::run::end_run_with_error(
        &mut session,
        "early",
        broca_protocol::RunFinishReason::Error,
        &loop_failed(),
        1_800_000_000_002,
        Some(&withdrawals),
    )
    .await;
    assert!(ended.is_ok(), "end_run_with_error: {ended:?}");
    assert!(
        Writer::conversation(&session).open_tool_turn().is_none(),
        "the tool turn closes with the run"
    );
    assert_eq!(
        outcomes(&session).await,
        ["say-1:Completed", "bash-1:Interrupted"]
    );
    let held_close = session
        .open_log_records()
        .await
        .unwrap()
        .into_iter()
        .any(|(_, record)| {
            matches!(&record, Record::ToolOutcome(outcome)
                if outcome.tool_call_id.0 == "bash-1"
                    && outcome.reason.as_deref() == Some("provider_held_interrupted"))
        });
    assert!(held_close, "bash-1 gets the held close result");
    assert_eq!(*withdrawals.0.lock().unwrap(), ["bash-1"]);
}
