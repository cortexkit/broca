use std::sync::Arc;

use async_trait::async_trait;
use broca_core::capability::AllowAll;
use broca_core::provider::StubProvider;
use broca_core::stub_plane::StubToolPlane;
use broca_core::{OutputEvent, RunError, RunSummary};
use broca_protocol::content::{AssistantMessage, ContentBlock, FinishReason};
use broca_protocol::request::StopCondition;
use broca_protocol::{
    BindIdentity, MessageId, NormalizedEvent, ProviderControl, RunId, SubmissionRequest, Usage,
};
use broca_session_loop::parts::MemoryParts;
use broca_session_loop::run::{run, Seams};
use broca_session_loop::writer::Writer;
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::DeploymentKey;
use broca_session_store::record::Record;
use broca_session_store::session::Session;
use tokio::sync::{mpsc, oneshot, Notify};

use super::StoreActor;
use crate::actor::{CancelOutcome, LineageCmd, LineageStatus, SendKey, SendOutcome};
use crate::executor::{RunExecutor, RunJob, RunKind, StoreRunEnd, StoreRunError, StoreRunJob};

fn text_step(id: &str, text: &str) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(id.into()),
                content: vec![ContentBlock::Text { text: text.into() }],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(100),
                output_tokens: Some(20),
                ..Usage::default()
            },
            finish_reason: FinishReason::Stop,
            retries_used: 0,
        }),
    ]
}

/// A step that calls the `read` tool and asks for its result.
fn read_step(id: &str) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(id.into()),
                content: vec![ContentBlock::ToolCall(broca_protocol::content::ToolCall {
                    thought_signature: None,
                    tool_call_id: broca_protocol::ToolCallId("c1".into()),
                    tool_name: "read".into(),
                    input: serde_json::json!({}),
                })],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(100),
                output_tokens: Some(20),
                ..Usage::default()
            },
            finish_reason: FinishReason::ToolCalls,
            retries_used: 0,
        }),
    ]
}

fn bind() -> BindIdentity {
    BindIdentity::new("/project", "store-actor", "s1")
}

/// A scripted provider whose first step waits for `gate`, so a test can act while a run's start
/// is durable and its first model call is in flight.
struct Gated {
    inner: StubProvider,
    gate: Option<Arc<Notify>>,
}

#[async_trait]
impl broca_core::provider::Provider for Gated {
    async fn step(
        &self,
        step_id: broca_protocol::StepId,
        opts: &broca_protocol::request::CallOptions,
    ) -> mpsc::Receiver<NormalizedEvent> {
        if let Some(gate) = &self.gate {
            gate.notified().await;
        }
        self.inner.step(step_id, opts).await
    }
}

/// Runs each store job through the new loop's admission and run, over a scripted provider.
struct LoopExecutor {
    provider: Gated,
}

fn executor(steps: Vec<Vec<NormalizedEvent>>, gate: Option<Arc<Notify>>) -> LoopExecutor {
    LoopExecutor {
        provider: Gated {
            inner: StubProvider::new(steps),
            gate,
        },
    }
}

#[async_trait]
impl RunExecutor for LoopExecutor {
    async fn execute(
        &self,
        _job: RunJob,
        _out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        unreachable!("a store session's runs come through execute_on_store")
    }

    async fn execute_on_store(
        &self,
        job: StoreRunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> StoreRunEnd {
        let StoreRunJob {
            mut req,
            control,
            mut writer,
            mut parts,
            ..
        } = job;
        let result = async {
            let admission = broca_session_loop::admit::admission(
                &mut req,
                &control,
                &self.provider,
                writer.operations(),
                Some(1),
            )
            .map_err(StoreRunError::Admission)?;
            let tools = StubToolPlane::new().with_canned_text("read", "file contents");
            let now = || 1;
            let seams = Seams {
                provider: &self.provider,
                tools: &tools,
                bind: req.session.clone(),
                out: Some(&out),
                cancel: control.cancel.clone().unwrap_or_default(),
                stop: Default::default(),
                held: None,
                approval: &AllowAll,
                hook_plane: None,
                now_ms: &now,
                session_address: None,
                compaction_plane: None,
                compaction_session: None,
                keep_warm: None,
                account_reports: None,
                max_same_account_wait: Default::default(),
                auth_selection: None,
            };
            run(&mut writer, parts.as_mut(), admission, &seams)
                .await
                .map_err(StoreRunError::Loop)
        }
        .await;
        StoreRunEnd { parts, result }
    }
}

/// A store actor over a new session in memory, serving on its own task, and the backend it
/// writes to.
async fn actor(
    executor: LoopExecutor,
) -> (
    mpsc::UnboundedSender<LineageCmd>,
    tokio::task::JoinHandle<Session<&'static MemoryBackend>>,
    &'static MemoryBackend,
) {
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let session = Session::create(
        backend,
        SessionAddr("0123456789abcdef".into()),
        SessionTriple {
            project_root: "/project".into(),
            harness: "store-actor".into(),
            session: "s1".into(),
        },
        &DeploymentKey::new(1, &[7; 32]),
        false,
    )
    .await
    .unwrap();
    let actor = StoreActor::new(
        bind(),
        session,
        Arc::new(executor),
        Box::new(MemoryParts::default()),
    );
    let (commands, receiver) = mpsc::unbounded_channel();
    let served = tokio::spawn(actor.serve(receiver));
    (commands, served, backend)
}

fn request(text: &str) -> SubmissionRequest {
    serde_json::from_value(serde_json::json!({
        "model": {"provider_module_id": "stub", "model_id": "m"},
        "input": [{"role": "user", "content": [{"type": "text", "text": text}]}],
        "tools": [{
            "name": "read",
            "description": "reads a file",
            "input_schema": {"type": "object"},
        }],
        "policy_allowed": ["read"],
        "stop_when": [{"type": "step_count", "n": 3}],
    }))
    .unwrap()
}

async fn send(
    commands: &mpsc::UnboundedSender<LineageCmd>,
    run_id: &str,
    text: &str,
) -> SendOutcome {
    let (reply, answer) = oneshot::channel();
    commands
        .send(LineageCmd::Send {
            session_class: None,
            retention: None,
            stop_sequences_named: false,
            conversation_key: None,
            title: None,
            run_id: RunId(run_id.into()),
            request: Box::new(request(text)),
            kind: RunKind::FreshOrResume,
            key: SendKey::Minted,
            skip_materialization: false,
            delivery: broca_protocol::Delivery::Queue,
            plan: None,
            order: None,
            reply,
        })
        .unwrap();
    answer.await.unwrap()
}

async fn status(commands: &mpsc::UnboundedSender<LineageCmd>) -> LineageStatus {
    let (reply, answer) = oneshot::channel();
    commands.send(LineageCmd::Status { reply }).unwrap();
    answer.await.unwrap()
}

/// Waits until the session is idle again.
async fn idle(commands: &mpsc::UnboundedSender<LineageCmd>) {
    for _ in 0..500 {
        if status(commands).await == LineageStatus::Idle {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    panic!("the session never went idle");
}

fn texts(session: &Session<&'static MemoryBackend>) -> Vec<String> {
    session
        .conversation()
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|block| match block {
            ContentBlock::Text { text } => Some(text.clone()),
            _ => None,
        })
        .collect()
}

/// A send on an idle session is answered `active` once its run's start is in the session, and
/// the run, written through the actor, finishes there. A second send then runs on the same
/// session, after the first.
#[tokio::test]
async fn a_send_on_an_idle_session_runs_through_the_actor() {
    let (commands, served, _) = actor(executor(
        vec![text_step("a1", "hello"), text_step("a2", "again")],
        None,
    ))
    .await;
    assert_eq!(
        send(&commands, "r1", "first").await,
        SendOutcome::Active {
            run_id: RunId("r1".into()),
            delivered: None
        }
    );
    idle(&commands).await;
    assert!(matches!(
        send(&commands, "r2", "second").await,
        SendOutcome::Active { .. }
    ));
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    assert_eq!(texts(&session), ["first", "hello", "second", "again"]);
    let run = session.operations().run.as_ref().expect("the session ran");
    assert_eq!(run.start.as_ref().unwrap().run_id, "r2");
    assert_eq!(
        run.finished,
        Some(broca_protocol::RunFinishReason::Completed)
    );
}

/// A send the shared admission rules refuse is answered with the refusal, and nothing is
/// written: a zero step limit is refused before the run's start.
#[tokio::test]
async fn a_refused_send_writes_nothing() {
    let (commands, served, _) = actor(executor(vec![], None)).await;
    let (reply, answer) = oneshot::channel();
    let mut refused = request("first");
    refused.stop_when = vec![StopCondition::StepCount { n: 0 }];
    commands
        .send(LineageCmd::Send {
            session_class: None,
            retention: None,
            stop_sequences_named: false,
            conversation_key: None,
            title: None,
            run_id: RunId("r1".into()),
            request: Box::new(refused),
            kind: RunKind::FreshOrResume,
            key: SendKey::Minted,
            skip_materialization: false,
            delivery: broca_protocol::Delivery::Queue,
            plan: None,
            order: None,
            reply,
        })
        .unwrap();
    match answer.await.unwrap() {
        SendOutcome::Error(message) => assert!(message.contains("step_count"), "{message}"),
        other => panic!("expected the refusal: {other:?}"),
    }
    assert_eq!(status(&commands).await, LineageStatus::Idle);
    drop(commands);
    let session = served.await.unwrap();
    assert!(session.operations().run.is_none(), "nothing was written");
    assert!(session.conversation().messages.is_empty());
}

/// A send needing a rule this actor doesn't serve yet is refused by name, and the session stays
/// as it was.
#[tokio::test]
async fn an_unserved_send_is_refused_by_name() {
    let (commands, served, _) = actor(executor(vec![], None)).await;
    let (reply, answer) = oneshot::channel();
    let mut keyed = request("first");
    keyed.keep_warm = Some(broca_protocol::request::KeepWarm::Auto);
    commands
        .send(LineageCmd::Send {
            session_class: None,
            retention: None,
            stop_sequences_named: false,
            conversation_key: None,
            title: None,
            run_id: RunId("r1".into()),
            request: Box::new(keyed),
            kind: RunKind::FreshOrResume,
            key: SendKey::Minted,
            skip_materialization: false,
            delivery: broca_protocol::Delivery::Queue,
            plan: None,
            order: None,
            reply,
        })
        .unwrap();
    assert_eq!(
        answer.await.unwrap(),
        SendOutcome::Error(
            "keep-warm is not served yet for a session kept in the new storage".into()
        )
    );
    drop(commands);
    assert!(served.await.unwrap().operations().run.is_none());
}

/// A cancel at the step boundary is durable before its reply. The run, whose first model call is
/// in flight, finishes that step and its tool call, and then ends cancelled at the boundary
/// instead of sending its next step.
#[tokio::test]
async fn a_boundary_cancel_is_durable_and_ends_the_run_at_its_boundary() {
    let gate = Arc::new(Notify::new());
    let (commands, served, _) = actor(executor(
        vec![read_step("a1"), text_step("a2", "never sent")],
        Some(Arc::clone(&gate)),
    ))
    .await;
    assert!(matches!(
        send(&commands, "r1", "first").await,
        SendOutcome::Active { .. }
    ));
    let (reply, answer) = oneshot::channel();
    commands
        .send(LineageCmd::Cancel {
            run_id: RunId("r1".into()),
            at: broca_wal::record::RunCancelAt::StepBoundary,
            reply,
        })
        .unwrap();
    assert_eq!(answer.await.unwrap(), CancelOutcome::Ack);
    gate.notify_one();
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    let run = session.operations().run.as_ref().unwrap();
    assert_eq!(
        run.finished,
        Some(broca_protocol::RunFinishReason::Cancelled)
    );
    assert_eq!(run.completed_steps, 1, "the step in flight finished");
    // The input, the step's tool call and its result: the next step was never sent.
    assert_eq!(session.conversation().messages.len(), 3);
    let records = session.open_log_records().await.unwrap();
    assert!(records
        .iter()
        .any(|(_, record)| matches!(record, Record::CancelRequest(_))));
}

/// A cancel sent right behind the send that starts a run waits until the run's start is durable,
/// so the cancel request lands after the start it names, never before it.
#[tokio::test]
async fn a_cancel_behind_a_send_lands_after_the_runs_start() {
    let gate = Arc::new(Notify::new());
    let (commands, served, _) = actor(executor(
        vec![read_step("a1"), text_step("a2", "never sent")],
        Some(Arc::clone(&gate)),
    ))
    .await;
    let (reply, sent) = oneshot::channel();
    commands
        .send(LineageCmd::Send {
            session_class: None,
            retention: None,
            stop_sequences_named: false,
            conversation_key: None,
            title: None,
            run_id: RunId("r1".into()),
            request: Box::new(request("first")),
            kind: RunKind::FreshOrResume,
            key: SendKey::Minted,
            skip_materialization: false,
            delivery: broca_protocol::Delivery::Queue,
            plan: None,
            order: None,
            reply,
        })
        .unwrap();
    let (reply, cancelled) = oneshot::channel();
    commands
        .send(LineageCmd::Cancel {
            run_id: RunId("r1".into()),
            at: broca_wal::record::RunCancelAt::StepBoundary,
            reply,
        })
        .unwrap();
    assert!(matches!(sent.await.unwrap(), SendOutcome::Active { .. }));
    assert_eq!(cancelled.await.unwrap(), CancelOutcome::Ack);
    gate.notify_one();
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    let records = session.open_log_records().await.unwrap();
    let seq = |wanted: fn(&Record) -> bool| {
        records
            .iter()
            .find(|(_, record)| wanted(record))
            .map(|(seq, _)| *seq)
            .expect("the record is in the log")
    };
    let start = seq(|record| matches!(record, Record::RunStart(_)));
    let cancel = seq(|record| matches!(record, Record::CancelRequest(_)));
    assert!(
        cancel > start,
        "the cancel ({cancel}) follows the start ({start})"
    );
}

/// A cancel for a run the session isn't running is answered `not active`, writing nothing.
#[tokio::test]
async fn a_cancel_with_no_active_run_is_not_active() {
    let (commands, served, _) = actor(executor(vec![], None)).await;
    let (reply, answer) = oneshot::channel();
    commands
        .send(LineageCmd::Cancel {
            run_id: RunId("r1".into()),
            at: broca_wal::record::RunCancelAt::Now,
            reply,
        })
        .unwrap();
    assert_eq!(answer.await.unwrap(), CancelOutcome::NotActive);
    drop(commands);
    let session = served.await.unwrap();
    assert!(!session
        .open_log_records()
        .await
        .unwrap()
        .iter()
        .any(|(_, record)| matches!(record, Record::CancelRequest(_))));
}

/// Commands the actor doesn't serve yet are refused by name.
#[tokio::test]
async fn an_unserved_command_is_refused_by_name() {
    let (commands, _served, _) = actor(executor(vec![], None)).await;
    let (reply, answer) = oneshot::channel();
    commands
        .send(LineageCmd::Retract {
            submission_id: broca_protocol::SubmissionId("sub-1".into()),
            reply,
        })
        .unwrap();
    assert_eq!(
        answer.await.unwrap(),
        crate::actor::RetractOutcome::Error(
            "a retraction is not served yet for a session kept in the new storage".into()
        )
    );
}
